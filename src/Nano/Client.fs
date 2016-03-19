namespace Prajna.Nano

open System
open System.Diagnostics
open System.Collections.Concurrent
open System.Threading.Tasks
open System.Runtime.Serialization
open System.Net
open System.Collections.Generic
open System.Threading
open System.Collections.Concurrent
open System.IO

open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.Network

open BaseADTs

type ServerInfo = {Address: IPAddress; Port: int}

type ClientNode(address: IPAddress, port: int) as self =

    static let network = new ConcreteNetwork()

    let mutable writeQueues = Array.zeroCreate<BufferQueue> 10
    let callbacks = new ConcurrentDictionary<int64, Response -> unit>()

    // TODO: avoid wraparound in case we have more thatn int64.MaxValue calls
    let nextQueue = 
        let mutable curQueue = -1L
        let numQueues = int64 writeQueues.Length
        fun() ->
            let next = Interlocked.Increment(&curQueue)
            writeQueues.[int(next % numQueues)]

    let onNewBuffer (responseBytes: MemoryStreamB) =
        let sw = Stopwatch.StartNew()
        let (Numbered(number,response)) = Serializer.Deserialize(responseBytes) :?> Numbered<Response>
        Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Client Handler: Deserialized %1.3fMB response." ((float responseBytes.Length) / 1000000.0))
        responseBytes.Dispose()
        callbacks.[number] response
        match callbacks.TryRemove(number) with
        | true, _ -> ()
        | false, _ -> raise <| Exception("Cound not remove callback.")
        responseBytes.Dispose()
    
    let onConnect (qi: int) readQueue wq =
        writeQueues.[qi] <- wq
        QueueMultiplexer<MemoryStreamB>.AddQueue(readQueue, onNewBuffer)

    let getRemote (response: Response) =
        match response with
        | RunDelegateResponse(Choice1Of2(pos)) -> Remote<'T>(pos, self)
        | RunDelegateResponse(Choice2Of2(e)) -> raise e
        | _ -> raise <| Exception("Unexpected response to RunDelegate request.")

    do
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting client node")
        for i = 0 to writeQueues.Length - 1 do
            network.Connect<BufferStreamConnection>(address, port, onConnect i) |> ignore

    interface IRequestHandler with
        member this.AsyncHandleRequest(request: Request) : Async<Response> =
            let (Numbered(number,_)) as numberedRequest = newNumbered request
            let sw = Stopwatch.StartNew()
            let memStream = Serializer.Serialize(numberedRequest).Bytes
            sw.Stop()
            Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Client Handler: Serialized %d bytes (%fms)" memStream.Length sw.Elapsed.TotalMilliseconds)
            let responseHolder : Response option ref = ref None
            let semaphore = new SemaphoreSlim(0) 
            let callback (response: Response) = 
                responseHolder := Some(response)
                lock responseHolder (fun _ ->
                    semaphore.Release() |> ignore
                )
            callbacks.AddOrUpdate(number, callback, Func<_,_,_>(fun _ _ -> raise <| Exception("Unexpected pre-existing request number."))) |> ignore
            lock responseHolder (fun _ ->
                nextQueue().Add memStream
                async {
                    do! Async.AwaitIAsyncResult(semaphore.WaitAsync(), 200) |> Async.Ignore
                    while !responseHolder = None do
                        do! Async.Sleep 200
                    return responseHolder.Value.Value
                }
            )
        member this.HandleRequest(request: Request) = 
            (this :> IRequestHandler).AsyncHandleRequest(request)
            |> Async.RunSynchronously

        member this.Address = address
        member this.Port = port


    member this.NewRemote(func: Func<'T>) : Async<Remote<'T>> =
        async {
            let handler = (* defaultArg (ServerNode.TryGetServer((address,port))) *) (this :> IRequestHandler)
            let! response = handler.AsyncHandleRequest(RunDelegate(-1, func))
            match response with
            | RunDelegateResponse(Choice1Of2(pos)) -> return new Remote<'T>(pos, this)
            | RunDelegateResponse(Choice2Of2(e)) -> return (raise e)
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }

    member this.NewRemoteAsync(func: Func<'T>) = this.NewRemote(func) |> Async.StartAsTask

    member this.NewRemote(func: Serialized<Func<'T>>) : Async<Remote<'T>> =
        async {
            let handler = (* defaultArg (ServerNode.TryGetServer((address,port))) *) (this :> IRequestHandler)
            let! response = handler.AsyncHandleRequest(RunDelegateSerialized(-1, func.Bytes))
            return getRemote response
        }

    member this.NewRemoteAsync(func: Serialized<Func<'T>>) = this.NewRemote(func) |> Async.StartAsTask

    interface IDisposable with
        member __.Dispose() = 
            writeQueues |> Array.iter (fun wq -> wq.CompleteAdding())

and RemoteModule() =

    static member val ClientByEndpoint = ConcurrentDictionary<IPAddress * int, ClientNode>()

and Remote<'T> =
    
    [<NonSerialized>]
    val mutable handler: IRequestHandler 
    val pos: int
    val mutable serverInfo : ServerInfo

    internal new(pos: int, handler: IRequestHandler) = 
        {handler = handler (*Unchecked.defaultof<IRequestHandler>*); pos = pos; serverInfo = {Address = handler.Address; Port = handler.Port }}

    member private this.ReinitHandler() = 
        if Object.ReferenceEquals(this.handler, null) then
            match ServerNode.TryGetServer((this.serverInfo.Address, this.serverInfo.Port)) with
            | Some(server) -> this.handler <- server
            | None -> 
                let endPoint = (this.serverInfo.Address, this.serverInfo.Port)
                this.handler <- 
                    match RemoteModule.ClientByEndpoint.TryGetValue(endPoint) with
                    | true, client -> client :> IRequestHandler
                    | _ -> 
                        let client = new ClientNode(this.serverInfo.Address, this.serverInfo.Port)
                        RemoteModule.ClientByEndpoint.AddOrUpdate(
                            endPoint, 
                            Func<IPAddress*int,ClientNode>(fun ep -> 
                                Logger.LogF(LogLevel.Info, fun _ -> "Creating new client")
                                new ClientNode(fst ep, snd ep)), 
                            Func<IPAddress*int,ClientNode,ClientNode>(fun _ c -> c)) |> ignore
                        client :> IRequestHandler
            
    /// Apply<Func>
    member this.Apply(func: Func<'T, 'U>) =
        this.ReinitHandler()
        async {
            let! response = this.handler.AsyncHandleRequest( RunDelegate(this.pos, func) )
            match response with
            | RunDelegateResponse(Choice1Of2(pos)) -> return new Remote<'U>(pos, this.handler)
            | RunDelegateResponse(Choice2Of2(e)) -> return (raise e)
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }
    member this.ApplyTask(func: Func<'T, 'U>) = this.Apply(func) |> Async.StartAsTask

    member this.ApplyAsync(func: Func<'T, Async<'U>>) =
        this.ReinitHandler()
        async {
            let! response = this.handler.AsyncHandleRequest( RunDelegateAsync(this.pos, func) )
            match response with
            | RunDelegateResponse(Choice1Of2(pos)) -> return new Remote<'U>(pos, this.handler)
            | RunDelegateResponse(Choice2Of2(e)) -> return (raise e)
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }
//    member this.ApplyTask(func: Func<'T, 'U>) = this.Apply(func) |> Async.StartAsTask

    /// ApplyAndGetValue<Func>
    member this.ApplyAndGetValue(func: Func<'T, 'U>) : Async<'U> =
        this.ReinitHandler()
        async {
            let! response = this.handler.AsyncHandleRequest( RunDelegateAndGetValue(this.pos, func) )
            match response with
            | GetValueResponse(obj) -> return obj :?> 'U
            | RunDelegateResponse(Choice2Of2(e)) -> return (raise e)
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }

    /// ApplyAndAsyncGetValue<Func>
    member this.ApplyTaskAndGetValueTask(func: Func<'T, Task<'U>>) : Task<'U> =
        this.ReinitHandler()
        async {
            let! response = this.handler.AsyncHandleRequest( RunDelegateAsyncAndGetValue(this.pos, func) )
            match response with
            | GetValueResponse(obj) -> return obj :?> 'U
            | RunDelegateResponse(Choice2Of2(e)) -> return (raise e)
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        } |> Async.StartAsTask

    member this.ApplyAsyncAndGetValue(func: Func<'T, Async<'U>>) : Async<'U> =
        this.ReinitHandler()
        async {
            let! response = this.handler.AsyncHandleRequest( RunDelegateAsyncAndGetValue(this.pos, func) )
            match response with
            | GetValueResponse(obj) -> return obj :?> 'U
            | RunDelegateResponse(Choice2Of2(e)) -> return (raise e)
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }

    /// Apply<SerializedFunc>
    member this.ApplyTask(func: Serialized<Func<'T, 'U>>) = this.Apply(func) |> Async.StartAsTask
    member this.Apply(func: Serialized<Func<'T, 'U>>) =
        this.ReinitHandler()
        async {
            let! response = this.handler.AsyncHandleRequest( RunDelegateSerialized(this.pos, func.Bytes) )
            match response with
            | RunDelegateResponse(Choice1Of2(pos)) -> return new Remote<'U>(pos, this.handler)
            | RunDelegateResponse(Choice2Of2(e)) -> return (raise e)
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }

    // GetValue
    member this.GetValue() =
        this.ReinitHandler()
        async {
            let! response = this.handler.AsyncHandleRequest( GetValue(this.pos) )
            match response with
            | GetValueResponse(obj) -> return obj :?> 'T
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }
    member this.GetValueTask() = this.GetValue() |> Async.StartAsTask

        
