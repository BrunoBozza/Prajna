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

module MaybeAsync = 
    type 'a MaybeAsync = Choice<unit -> 'a, Async<'a>>

    let RunSynchronously (value: MaybeAsync<'a>) =
        match value with 
        | Choice1Of2(x) -> x()
        | Choice2Of2(asyncX) -> asyncX |> Async.RunSynchronously

    type MaybeAsyncBuilder() =

        member this.Bind(prev: 'a MaybeAsync, binder: 'a -> 'b MaybeAsync) : 'b MaybeAsync  =
            match prev with
            | Choice1Of2(a) -> binder (a())
            | Choice2Of2(asyncA) ->
                Choice2Of2(
                    async {
                        let! a = asyncA
                        match binder a with
                        | Choice1Of2(b) -> return (b())
                        | Choice2Of2(asyncB) -> return! asyncB
                    })
        
        member this.Run(ma: 'a MaybeAsync) =
            ma 

        member this.Return(a: 'a) = Choice1Of2(fun _ -> a)

        member this.Zero() = this.Return(())
//        member this.Run(ma: 'a MaybeAsync) = ma |> RunSynchronously

        member this.Combine(prev: unit MaybeAsync, next: 'a MaybeAsync) = this.Bind(prev, fun _ -> next)
        member this.Delay(gen: unit -> 'a MaybeAsync) = this.Bind(this.Return(()), gen)

        member this.While(test: unit -> bool, action: unit MaybeAsync) =
            let f =
                match action with
                | Choice1Of2(f) -> f
                | Choice2Of2(aF) -> fun _ -> aF |> Async.RunSynchronously
            
            if test() 
            then 
                this.Bind(Choice1Of2(f), fun _ -> this.While(test, Choice1Of2(f)))
//                match action with
//                | Choice1Of2(f) -> 
//                    do f()
//                    this.Bind(action, fun _ -> this.While(test, action))
//                | c2 -> this.Bind(c2, fun _ -> this.While(test, action))
            else 
                this.Zero()
        
    let masync = new MaybeAsyncBuilder()

//    let mx = masync { return 3 }
//    let my = masync { return 5 }
//    let mz = masync {
//        let! x = mx
//        let! y = my
//        return x + y
//    }
//
//    mz |> RunSynchronously

    let countToZero() = 
        masync {
            let x = ref 10
            while !x > 0 do
                printfn "%d" !x
                x := !x - 1
            return ()
        } |> RunSynchronously

open MaybeAsync


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

    let receiveBufferPart = 
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

    let readMessage : Socket -> Choice<MemoryStreamB, Async<MemoryStreamB>> =
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

        let readTo (socket: Socket) (memStream: MemoryStreamB) (count: int64) : Async<unit> option =
            let finalPosition = memStream.Position + count
            let addCurPart() =
                let countInCurPart = min (finalPosition - memStream.Position) (int64 partLen)
                let newPart = new RBufPart<byte>(part, partPos, countInCurPart)
                memStream.WriteRBufNoCopy(newPart)
                incPartPosLen (int countInCurPart)
            if count > int64 partLen then
                Some(async {
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
                None

        let bind (binder: unit -> Async<unit> option) (ao: Async<unit> option) : Async<unit> option =
            match ao with
            | None -> binder()
            | Some au -> 
                Some(async { 
                    do! au
                    match binder() with
                    | Some au2 -> return! au2
                    | None -> ()
                    return ()
                })

        fun (socket: Socket) ->
            // can this be Async<unit> option?
            let memStream = new MemoryStreamB()
            let moveToBuffer =
                if partLen = 0 then
                    Some(moveToNextPart socket)
                else 
                    None
            let doIt : Async<unit> option = 
                moveToBuffer |> bind (fun _ ->
                readTo socket memStream 8L |> bind (fun _ -> 
                memStream.Seek(0L, SeekOrigin.Begin) |> ignore
                let payloadLen = memStream.ReadInt64()
                memStream.Seek(8L, SeekOrigin.Begin) |> ignore
                readTo socket memStream payloadLen |> bind (fun _ ->
                    memStream.Seek(0L, SeekOrigin.Begin) |> ignore
                    None
                )))
            match doIt with
            | None -> Choice1Of2(memStream)
            | Some au -> Choice2Of2(async.Bind(au, fun _ -> async.Return(memStream)))

    let receiveBuffers(socket: Socket) =
        async {
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "BufferStreamConnection: starting to read")
            try
                while true do
                    match readMessage socket with
                    | Choice1Of2(ms) -> readQueue.Add ms
                    | Choice2Of2(asyncMemStream) ->
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
//                    let sendResult = asyncSendMS socket bufferToSend 0L (bufferToSend.Length)
                    let sendResult = Some <| bufferToSend.AsyncReadToWriter(asyncSendAll socket, bufferToSend.Length)
                    match sendResult with
                    | None -> 
                        Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "%d bytes written synchronously." (int bufferToSend.Position - markPos))
                    | Some(asyncCompletion) -> 
                        do! asyncCompletion
                        Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "%d bytes written asynchronously." (int bufferToSend.Position - markPos))
                    bufferToSend.Dispose()
                    semaphore.Release() |> ignore
                with
                    | :? IOException | :? SystemException | _ -> 
                        semaphore.Release() |> ignore
                        writeQueue.CompleteAdding()
            } 
            |> Async.Start


    let sendBuffers(socket: Socket) = 
        QueueMultiplexer<MemoryStreamB>.AddQueue(writeQueue, onNewBuffer socket)

    interface IConn with 

        member val Socket = null with get, set

        member this.Init(socket: Socket, state: obj) = 
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "New connection created (%A)." socket.LocalEndPoint)
            socket.NoDelay <- true
            socket.UseOnlyOverlappedIO <- true
            (this :> IConn).Socket <- socket
            let onConnect : BufferQueue -> BufferQueue -> unit = downcast state
            onConnect readQueue writeQueue
            Async.Start(receiveBuffers socket)
            sendBuffers socket

        member this.Close() = 
            (this :> IConn).Socket.Shutdown(SocketShutdown.Both)
            readQueue.CompleteAdding()
            writeQueue.CompleteAdding()
              
