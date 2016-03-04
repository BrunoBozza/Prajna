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

    let readQueue  = new BufferQueue(50)
    let writeQueue = new BufferQueue(50)

//    let matchOrThrow (choice: Choice<'T,exn>) =
//        match choice with 
//        | Choice1Of2(t) -> t
//        | Choice2Of2(exc) -> raise exc

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

    let receiveBuffers(socket: Socket) =
        async {
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "BufferStreamConnection: starting to read")
            try
                let countBuffer = Array.zeroCreate<byte> 4
                while true do
                    let mutable countRead = 0
                    while countRead < 4 do
                        let! bytesReadThisIter = asyncReceive socket countBuffer countRead (4 - countRead) 
                        countRead <- countRead + bytesReadThisIter
                    let count = BitConverter.ToInt32(countBuffer, 0)
                    Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Read count: %d." count)
                    let memoryStream = new MemoryStreamB()
                    do! memoryStream.AsyncWriteFromReader(asyncReceive socket, int64 count)
                    memoryStream.Seek(0L, SeekOrigin.Begin) |> ignore
                    readQueue.Add memoryStream
            with
                | :? IOException | :? SocketException -> readQueue.CompleteAdding()
        }

    let onNewBuffer (socket: Socket) =
        let semaphore = new SemaphoreSlim(1) 
        fun (bufferToSend: MemoryStreamB) ->
            async {
                try
                    Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Responding with %d bytes." bufferToSend.Length)
                    let countBytes = BitConverter.GetBytes(int bufferToSend.Length)
                    do! semaphore.WaitAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
                    do! asyncSendAll socket countBytes 0 4
                    let mark = bufferToSend.GetBufferPosLength()
                    do! bufferToSend.AsyncReadToWriter(asyncSendAll socket, bufferToSend.Length)
                    let getPosition (x,y,z) = y
                    Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "%d bytes written." (int bufferToSend.Position - getPosition mark))
                    bufferToSend.Dispose()
                    semaphore.Release() |> ignore
                with
                    | :? IOException | :? SystemException -> 
                        semaphore.Release() |> ignore
                        writeQueue.CompleteAdding()
            } 
            |> Async.Start


    let sendBuffers(socket: Socket) = 
        socket.NoDelay <- true
        QueueMultiplexer<MemoryStreamB>.AddQueue(writeQueue, onNewBuffer socket)

    interface IConn with 

        member val Socket = null with get, set

        member this.Init(socket: Socket, state: obj) = 
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "New connection created (%A)." socket.LocalEndPoint)
            (this :> IConn).Socket <- socket
            let onConnect : BufferQueue -> BufferQueue -> unit = downcast state
            onConnect readQueue writeQueue
            Async.Start(receiveBuffers socket)
            sendBuffers socket

        member this.Close() = 
            (this :> IConn).Socket.Shutdown(SocketShutdown.Both)
            readQueue.CompleteAdding()
            writeQueue.CompleteAdding()
              
