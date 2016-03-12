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

type BufferStreamConnection() =

    do BufferListStream<byte>.InitSharedPool()

    let readQueue  = new BufferQueue(50)
    let writeQueue = new BufferQueue(50)

    let asyncReceive (socket: Socket) (buffer: byte[]) (offset: int) (count: int) =
        Async.FromBeginEnd((fun (asyncCallback, state) -> socket.BeginReceive(buffer, offset, count, SocketFlags.None, asyncCallback, state)),
                            (fun asyncResult -> socket.EndReceive(asyncResult)))

    let asyncReceiveBuffers (socket: Socket) (segments: IList<ArraySegment<byte>>) =
        Async.FromBeginEnd((fun (asyncCallback, state) -> socket.BeginReceive(segments, SocketFlags.None, asyncCallback, state)),
                            (fun asyncResult -> socket.EndReceive(asyncResult)))

    let asyncReceiveAllBuffers (socket: Socket) (segments: List<ArraySegment<byte>>) =
        async {
            let total = segments |> Seq.sumBy (fun s -> s.Count)
            let mutable received = 0
            while received < total do
                let! receivedThisIter = asyncReceiveBuffers socket segments
                let mutable receivedThisIterToGo = receivedThisIter
                let mutable i = 0
                while receivedThisIterToGo > 0 do
                    let curSegment = segments.[i]
                    let toRemoveFromCurSegment = min curSegment.Count receivedThisIterToGo
                    segments.[i] <- ArraySegment<byte>(curSegment.Array, curSegment.Offset + toRemoveFromCurSegment, curSegment.Count - toRemoveFromCurSegment)
                    receivedThisIterToGo <- receivedThisIterToGo - toRemoveFromCurSegment
                    i <- i + 1
                received <- received + receivedThisIter
            return ()
        }

    let asyncSend (socket: Socket) (buffer: byte[]) (offset: int) (count: int) =
        Async.FromBeginEnd((fun (asyncCallback, state) -> socket.BeginSend(buffer, offset, count, SocketFlags.None, asyncCallback, state)),
                            (fun asyncResult -> socket.EndSend(asyncResult)))

    let getSegments (ms: MemoryStreamB) (offset: int64) (count: int64) : List<ArraySegment<byte>> =
        let ret = new List<ArraySegment<byte>>()
        let reader = new StreamReader<byte>(ms, offset, count)
        reader.ApplyFnToBuffers(fun (buf,pos,len) -> ret.Add(ArraySegment(buf, pos, len)))
        ret

    let asyncSendSegments (socket: Socket) (bufList: ArraySegment<byte>[]) (offset: int64) (count: int64) =
        let sea = new SocketAsyncEventArgs()
//        let bufList = getSegments buffer offset count//  new List<ArraySegment<byte>>()
//        do
//            let reader = new StreamReader<byte>(buffer, offset, count)
//            reader.ApplyFnToBuffers(fun (_bytes, _offset, _count) -> bufList.Add(ArraySegment<byte>(_bytes, _offset, _count)))
        if bufList.Length > 1 then
            sea.BufferList <- bufList
        else
            let onlySegment = bufList.[0]
            sea.SetBuffer(onlySegment.Array, onlySegment.Offset, onlySegment.Count)

        let evt = new ManualResetEventSlim(false)
        sea.Completed.Add(fun _ -> 
            sea.Dispose()
            evt.Set() |> ignore
        )
        let socketRet = socket.SendAsync(sea)
        if sea.SocketError <> SocketError.Success then
            raise <| new SocketException(int sea.SocketError)
        else if socketRet then
            let eventAsync = 
                async { 
                    if not evt.IsSet then
                        do! Async.AwaitWaitHandle evt.WaitHandle |> Async.Ignore
                    evt.Dispose()
                }
            Some eventAsync
        else 
            sea.Dispose()
            evt.Dispose()
            None

    let zeros = Array.zeroCreate<byte> (1 <<< 20)

    let asyncReceiveMS (socket: Socket) (ms: MemoryStreamB) (count: int64) : Option<Async<unit>> = // (buffer: MemoryStreamB) (offset: int64) (count: int64) =
        let sea = new SocketAsyncEventArgs()
        do
            let initialPosition = ms.Position
            while ms.Position - initialPosition < count do
                let toWrite = count - (ms.Position - initialPosition)
                let bp = ms.GetStackElem()
                let bpCount = min toWrite (max count (int64 zeros.Length))
                ms.AppendNoCopy(bp, 0L, bpCount)
            ms.Position <- initialPosition
        let segments = getSegments ms ms.Position count
        if segments.Count > 1 then
            sea.BufferList <- segments
        else
            let onlySegment = segments.[0]
            sea.SetBuffer(onlySegment.Array, onlySegment.Offset, onlySegment.Count)
        let evt = new ManualResetEventSlim(false)
        sea.Completed.Add(fun _ -> 
            evt.Set() |> ignore
        )
        let eventAsync = 
            async { 
                if not evt.IsSet then
                    do! Async.AwaitWaitHandle evt.WaitHandle |> Async.Ignore
                ms.Position <- ms.Position + count
                sea.Dispose()
                evt.Dispose()
            }
        let socketRet = socket.ReceiveAsync(sea)
        if sea.SocketError <> SocketError.Success then
            raise <| new SocketException(int sea.SocketError)
        else if socketRet then
            Some eventAsync
        else 
            ms.Position <- ms.Position + count
            sea.Dispose()
            evt.Dispose()
            None

    let asyncSendAll (socket: Socket) (buffer: byte[]) (offset: int) (count: int) =
        async {
            let mutable curSent = 0
            while curSent < count do
                let! sentThisIter = asyncSend socket buffer (offset + curSent) (count - curSent)
                curSent <- curSent + sentThisIter
        }

    let receiveBuffers(socket: Socket) =
        async {
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "BufferStreamConnection: starting to read")
            try
//                let mutable memStream = new MemoryStreamB()
//                let mutable buffer : RBufPart<byte> = memStream.GetStackElem()
//                let mutable bufferPos = int buffer.Elem.Offset
//                let mutable bufferLeft = int buffer.Elem.Length
//
//                let incBufferPosLen (amt: int) =
//                    bufferPos <- bufferPos + amt
//                    bufferLeft <- bufferLeft - amt
//
//                let moveToNextBuffer() = 
//                    (buffer :> IDisposable).Dispose()
//                    buffer <- memStream.GetStackElem()
//                    bufferPos <- buffer.Elem.Offset
//                    bufferLeft <- int buffer.Elem.Length
//
//                while true do
//                    memStream <- new MemoryStreamB()
//                    let mutable countReadCurMsg = 0
//                    let mutable payloadStartCurBuffer = bufferPos + 8
//                    while countReadCurMsg < 8 do
//                        if bufferLeft = 0 then
//                            // we need a new buffer
//                            payloadStartCurBuffer <- 8 - countReadCurMsg
//                            moveToNextBuffer()
//                        // In order to decrease the total number of I/O calls,
//                        // always try to read until the end of the current buffer
//                        let! bytesReadThisIter = asyncReceive socket buffer.Elem.Buffer bufferPos bufferLeft
//                        let headerBytesCurBuffer = min (8 - countReadCurMsg) bytesReadThisIter
//                        let newPart = new RBufPart<byte>(buffer, bufferPos, int64 headerBytesCurBuffer)
//                        memStream.WriteRBufNoCopy(newPart)
//                        countReadCurMsg <- countReadCurMsg + bytesReadThisIter
//                        incBufferPosLen bytesReadThisIter
//
//                    memStream.Seek(0L, SeekOrigin.Begin) |> ignore
//                    let payloadLength = memStream.ReadInt64() 
//                    if int64(bufferPos - payloadStartCurBuffer) >= payloadLength then
//                        // We read beyond the end of the message. 
//                        // Copy our part and continue
//                        memStream.AppendNoCopy(buffer, int64 payloadStartCurBuffer, payloadLength)
//                    else
//                        // We did not read the entire message
//                        // Copy the part we already have and fire receive for others
//                        let payloadFirstReadLength = int64(bufferPos - payloadStartCurBuffer)
//                        memStream.AppendNoCopy(buffer, int64 payloadStartCurBuffer, payloadFirstReadLength)
//                        let mutable payloadToGo = payloadLength - payloadFirstReadLength
//                        let remainingParts = new List<RBufPart<byte>>()
//                        while payloadToGo > 0L do
//                            if bufferLeft = 0 then
//                                // we need a new buffer
//                                moveToNextBuffer()
//                            if payloadToGo < int64 bufferLeft then
//                                remainingParts.Add(new RBufPart<byte>(buffer, bufferPos, payloadToGo))
//                                incBufferPosLen (int payloadToGo)
//                                payloadToGo <- 0L
//                            else
//                                remainingParts.Add(new RBufPart<byte>(buffer, bufferPos, int64 bufferLeft))
//                                payloadToGo <- payloadToGo - int64 bufferLeft
//                                moveToNextBuffer()
//                        let segments = new List<ArraySegment<byte>>(remainingParts.Count)
//                        for part in remainingParts do
//                            segments.Add(ArraySegment<byte>(part.Elem.Buffer, part.Offset, int part.Count))
//                        
//                        do! asyncReceiveAllBuffers socket segments
//
//                        for part in remainingParts do
//                            memStream.AppendNoCopy(part, int64 part.Elem.Offset, part.Count)
//
//                    memStream.Seek(0L, SeekOrigin.Begin) |> ignore
//                    readQueue.Add memStream

//                let count = BitConverter.ToInt64(countBuffer, 0)
//                Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Read count: %d." count)
//                memoryStream.WriteInt64 count
//                do! memoryStream.AsyncWriteFromReader(asyncReceive socket, int64 count)
//                memoryStream.Seek(0L, SeekOrigin.Begin) |> ignore
//                readQueue.Add memoryStream


                while true do
                    let countBuffer = Array.zeroCreate<byte> 8
                    let mutable countRead = 0
                    while countRead < 8 do
                        let! bytesReadThisIter = asyncReceive socket countBuffer countRead (8 - countRead) 
                        countRead <- countRead + bytesReadThisIter
                    let count = BitConverter.ToInt64(countBuffer, 0)
                    Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Read count: %d." count)
                    let memoryStream = new MemoryStreamB()
                    memoryStream.WriteInt64 count
                    do! memoryStream.AsyncWriteFromReader(asyncReceive socket, int64 count)
                    memoryStream.Seek(0L, SeekOrigin.Begin) |> ignore
                    readQueue.Add memoryStream

//                    let ms = new MemoryStreamB()
//                    match asyncReceiveMS socket ms 8L with
//                    | Some asyncCompletion -> do! asyncCompletion
//                    | None -> ()
//                    ms.Seek(0L, SeekOrigin.Begin) |> ignore
//                    let count = ms.ReadInt64()
//                    match asyncReceiveMS socket ms count with
//                    | Some otherAsyncCompletion -> do! otherAsyncCompletion
//                    | None -> ()
//                    ms.Seek(0L, SeekOrigin.Begin) |> ignore
//                    readQueue.Add ms
            with
                | :? IOException | :? SocketException -> readQueue.CompleteAdding()
        }

    let onNewBuffer (socket: Socket) =
        let semaphore = new SemaphoreSlim(1) 
        fun (bufferToSend: MemoryStreamB) ->
            async {
                try
                    Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Responding with %d bytes." bufferToSend.Length)
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
              
