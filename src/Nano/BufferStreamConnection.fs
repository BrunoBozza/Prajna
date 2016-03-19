namespace Prajna.Nano

open System
open System.Threading
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent

open System.Net.Sockets

open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.Network

type BufferQueue = BlockingCollection<MemoryStreamB>

open Prajna.Tools.Utils

type BufferStreamConnection() =

    do BufferListStream<byte>.InitSharedPool()

    let readQueue  = new BufferQueue(50)
    let writeQueue = new BufferQueue(50)

    let asyncReceive (socket: Socket) (buffer: byte[]) (offset: int) (count: int) =
        Async.FromBeginEnd((fun (asyncCallback, state) -> socket.BeginReceive(buffer, offset, count, SocketFlags.None, asyncCallback, state)),
                            (fun asyncResult -> socket.EndReceive(asyncResult)))

    let asyncSend (socket: Socket) (buffer: byte[]) (offset: int) (count: int) =
        Async.FromBeginEnd((fun (asyncCallback, state) -> socket.BeginSend(buffer, offset, count, SocketFlags.None, asyncCallback, state)),
                            (fun asyncResult -> socket.EndSend(asyncResult)))

    let asyncSendAll (socket: Socket) (buffer: byte[]) (offset: int) (count: int) =
        async {
            let mutable curSent = 0
            while curSent < count do
                let! sentThisIter = asyncSend socket buffer (offset + curSent) (count - curSent)
                curSent <- curSent + sentThisIter
        }

    let masyncSend (socket: Socket) (buffer: byte[]) (offset: int) (count: int) : MAsync<int> =
        let ar = socket.BeginSend(buffer, offset, count, SocketFlags.None, null, null)
        if ar.IsCompleted then
            Sync (socket.EndSend(ar))
        else
            AAsync(
                async {
                    return socket.EndSend(ar)
                })

    let rec masyncSendAll (socket: Socket) (buffer: byte[]) (offset: int) (count: int) : MAsync<unit> =
        let rec masyncSendAll' (socket: Socket) (buffer: byte[]) (offset: int) (count: int) : MAsync<int> =
            if count = 0 then
                Sync 0
            else
                masyncSend socket buffer offset count |> bind (fun sent -> 
                    masyncSendAll' socket buffer (offset + sent) (count - sent)
                )
        masyncSendAll' socket buffer offset count |> bind (fun _ -> Sync(()) )
                
    let receiveBufferPart : Socket -> Async<RBufPart<byte>> = 
        let bufferSource = new MemoryStreamB()
        let mutable buffer = bufferSource.GetStackElem()

        let mutable bufferPos = int buffer.Elem.Offset
        let mutable bufferLeft = int buffer.Elem.Length

        let incBufferPosLen (amt: int) =
            bufferPos <- bufferPos + amt
            bufferLeft <- bufferLeft - amt

        let moveToNextBuffer() = 
            (buffer :> IDisposable).Dispose()
            buffer <- bufferSource.GetStackElem()
            bufferPos <- buffer.Elem.Offset
            bufferLeft <- int buffer.Elem.Length

        fun (socket: Socket) ->
            async {
                if bufferLeft = 0 then
                    moveToNextBuffer()
                let! bytesRead = asyncReceive socket buffer.Elem.Buffer bufferPos bufferLeft
                let retPart = new RBufPart<byte>(buffer, bufferPos, int64 bytesRead)
                incBufferPosLen bytesRead
                return retPart
            }

    let readMessage : Socket -> MAsync<MemoryStreamB> =
        let mutable part : RBufPart<byte> = null
        let mutable partPos = 0
        let mutable partLen = 0

        let incPartPosLen (amt: int) =
            partPos <- partPos + amt
            partLen <- partLen - amt

        let moveToNextPart (socket: Socket) =
            async {
                let! tmpBufferPart = receiveBufferPart socket
                part <- tmpBufferPart
                partPos <- part.Offset
                partLen <- int part.Count
            }

        let readTo (socket: Socket) (memStream: MemoryStreamB) (count: int64) : MAsync<unit> =
            let finalPosition = memStream.Position + count
            let addCurPart() =
                let countInCurPart = min (finalPosition - memStream.Position) (int64 partLen)
                let newPart = new RBufPart<byte>(part, partPos, countInCurPart)
                memStream.WriteRBufNoCopy(newPart)
                incPartPosLen (int countInCurPart)
            if count > int64 partLen then
                AAsync(async {
                        do addCurPart()
                        while memStream.Position < finalPosition do
                            do! moveToNextPart socket
                            let countInCurPart = min (finalPosition - memStream.Position) (int64 partLen)
                            let newPart = new RBufPart<byte>(part, partPos, countInCurPart)
                            memStream.WriteRBufNoCopy(newPart)
                            incPartPosLen (int countInCurPart)
                    })
            else
                do addCurPart()
                Sync ()

        fun (socket: Socket) ->
            // can this be Async<unit> option?
            let memStream = new MemoryStreamB()
            let moveToBuffer =
                if partLen = 0 then
                    AAsync(moveToNextPart socket)
                else 
                    Sync ()
            let doIt = 
                moveToBuffer |> bind (fun _ ->
                readTo socket memStream 8L |> bind (fun _ -> 
                memStream.Seek(0L, SeekOrigin.Begin) |> ignore
                let payloadLen = memStream.ReadInt64()
                memStream.Seek(8L, SeekOrigin.Begin) |> ignore
                readTo socket memStream payloadLen |> bind (fun _ ->
                    memStream.Seek(0L, SeekOrigin.Begin) |> ignore
                    Sync ()
                )))
            match doIt with
            | Sync _ -> Sync(memStream)
            | AAsync au -> AAsync(async.Bind(au, fun _ -> async.Return(memStream)))

    let receiveBuffers(socket: Socket) =
        async {
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "BufferStreamConnection: starting to read")
            try
                while true do
                    match readMessage socket with
                    | Sync(ms) -> readQueue.Add ms
                    | AAsync(asyncMemStream) ->
                        let! ms = asyncMemStream
                        readQueue.Add ms
            with
                | :? IOException | :? SocketException -> readQueue.CompleteAdding()
        }

    let onNewBuffer (socket: Socket) =
        let semaphore = new SemaphoreSlim(1) 
        fun (bufferToSend: MemoryStreamB) ->
            async {
                try
                    Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Sending %d bytes." bufferToSend.Length)
                    do! semaphore.WaitAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
                    let (_,markPos,_) = bufferToSend.GetBufferPosLength()

                    match bufferToSend.MAsyncReadToWriter(masyncSendAll socket, bufferToSend.Length) with
                    | Sync() -> ()
                    | AAsync(au) -> do! au

                    Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "%d bytes written asynchronously." (int bufferToSend.Position - markPos))
                    bufferToSend.Dispose()
                    semaphore.Release() |> ignore
                with
                    | :? IOException | :? SystemException | _ -> 
                        semaphore.Release() |> ignore
                        writeQueue.CompleteAdding()
            } 
            |> Async.StartImmediate

    let sendBuffers(socket: Socket) = 
        QueueMultiplexer<MemoryStreamB>.AddQueue(writeQueue, onNewBuffer socket)

    interface IConn with 

        member val Socket = null with get, set

        member this.Init(socket: Socket, state: obj) = 
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "New connection created (%A)." socket.LocalEndPoint)
            socket.NoDelay <- true
            socket.UseOnlyOverlappedIO <- true
            socket.SendBufferSize <- 1 <<< 20
            socket.ReceiveBufferSize <- 1 <<< 20
            (this :> IConn).Socket <- socket
            let onConnect : BufferQueue -> BufferQueue -> unit = downcast state
            onConnect readQueue writeQueue
            Async.Start(receiveBuffers socket)
            sendBuffers socket

        member this.Close() = 
            (this :> IConn).Socket.Shutdown(SocketShutdown.Both)
            readQueue.CompleteAdding()
            writeQueue.CompleteAdding()
              
