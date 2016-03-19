namespace Prajna.Nano

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open System.Net
open System.Net.NetworkInformation
open System.Net.Sockets
open System.IO
open System.Collections.Generic

open Prajna.Tools
open Prajna.Tools.FSharp

open BaseADTs

type IRequestHandler =
    abstract member AsyncHandleRequest : Request -> Async<Response>
    abstract member HandleRequest : Request -> Response
    abstract member Address : IPAddress
    abstract member Port : int

type ServerBufferHandler(readQueue: BufferQueue, writeQueue: BufferQueue, handler: IRequestHandler) =
    
    let onNewBuffer (requestBytes: MemoryStreamB) =
        async {
            let numberedRequest : Numbered<Request> = downcast Serializer.Deserialize(requestBytes)
            let (Numbered(number,request)) : Numbered<Request> = numberedRequest
            Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Deserialized request: %d bytes." requestBytes.Length)
            requestBytes.Dispose()
            let! response = handler.AsyncHandleRequest request
            let numberedResponse = Numbered(number, response)
            let responseStream = Serializer.Serialize(numberedResponse).Bytes
            writeQueue.Add responseStream
        }
        |> Async.Start

    let processRequests() = 
        async {
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting to consume request bytes")
            QueueMultiplexer<MemoryStreamB>.AddQueue(readQueue, onNewBuffer, fun _ -> writeQueue.CompleteAdding())
        }

    member this.Start() =
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting request handler")
        processRequests()  |> Async.Start

    member this.Shutdown() =
        readQueue.CompleteAdding()

type ServerNode(port: int) as self =

    let network = new ConcreteNetwork()
    let objects = new List<obj>()
    let handlers = new List<ServerBufferHandler>()

    let address = ServerNode.GetDefaultIP()

    let applyDelegate (pos: int) (func: Delegate) : obj =
        let argument : obj[] = if pos = -1 then null else (Array.init 1 (fun _ -> objects.[pos]))
        let ret = func.DynamicInvoke(argument)
        Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Ran method")
        ret

    let addObjectAndGetResponse (x: obj) (objType: Type) : Response = 
        let retPos = lock objects (fun _ -> objects.Add x; objects.Count - 1)
        if objType <> typeof<Void> then
            let retPos = lock objects (fun _ -> objects.Add x; objects.Count - 1)
            RunDelegateResponse(Choice1Of2(retPos))
        else
            RunDelegateResponse(Choice1Of2(-1))

    let concurrentMemo (f: 'a -> 'b) =
        let cache = ConcurrentDictionary<'a, 'b>()
        fun x -> cache.GetOrAdd(x, f)

    let handleDelegateFunc (pos: int) (func: Delegate) : Response =
        try
            let ret = applyDelegate pos func
            addObjectAndGetResponse ret func.Method.ReturnType
        with
            | e -> RunDelegateResponse(Choice2Of2(e.InnerException))

    let dynamicReturnValue =
        concurrentMemo (fun (tType: Type) ->
            typeof<ServerNode>.GetMethod("GetValueResponse").MakeGenericMethod(tType).Invoke(null, null) :?> Delegate)

    let dynamicReturnRemote =
        concurrentMemo (fun (tType: Type) ->
            typeof<ServerNode>.GetMethod("GetRemoteResponse").MakeGenericMethod(tType).Invoke(self, null) :?> Delegate)

    let handleAsyncCall (remotePos: int) (userFunc: Delegate) (asyncResponseDelegate: Type -> Delegate) =
        async {
            let asyncUOrException = 
                try
                    Choice1Of2(applyDelegate remotePos userFunc)
                with
                    | e -> Choice2Of2(e)
            match asyncUOrException with
            | Choice1Of2(asyncU) ->
                try
                    let uType = userFunc.Method.ReturnType.GetGenericArguments().[0] 
                    let dynamicReturnResponse = asyncResponseDelegate uType
                    let asyncResponse = dynamicReturnResponse.DynamicInvoke(asyncU)
                    return! (asyncResponse :?> Async<Response>)
                with 
                    | e -> return RunDelegateResponse(Choice2Of2(e))
            | Choice2Of2(e) -> return RunDelegateResponse(Choice2Of2(e.InnerException))
        }

    let handleRequest(request: Request) : Async<Response> =
        match request with
        | RunDelegate(pos,func) ->
            async.Return(handleDelegateFunc pos func)
        | RunDelegateAndGetValue(pos,func) ->
            try
                let ret = applyDelegate pos func
                async.Return(GetValueResponse(ret))
            with
                | e -> async.Return(RunDelegateResponse(Choice2Of2(e.InnerException)))
        | RunDelegateAsync(pos,func) ->
            handleAsyncCall pos func dynamicReturnRemote
        | RunDelegateAsyncAndGetValue(pos,func) ->
            handleAsyncCall pos func dynamicReturnValue
        | RunDelegateSerialized(pos, bytes) ->
            let func = Serializer.Deserialize(bytes) :?> Delegate
            bytes.Dispose()
            async.Return(handleDelegateFunc pos func)
        | GetValue(pos) -> 
            Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Returning GetValue response")
            async.Return(GetValueResponse(objects.[pos]))

    let onConnect readQueue writeQueue =
        let handler = ServerBufferHandler(readQueue, writeQueue, self)
        lock handlers (fun _ ->
            handlers.Add(handler)
            handler.Start()
        )

    static let instances = new Dictionary<IPAddress * int, ServerNode>()

    do
        lock instances (fun _ -> instances.Add( (address,port)  , self))
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting server node")
        //BUGBUG: the server key is "address,port", with address being the first IP address returned by Dns.GetHostAddresses("")
        // But there's no guarantee that this is what we'll be listening at.
        network.Listen<BufferStreamConnection>(port, (*address.ToString(),*) onConnect)


    member this.GetRemoteResponse<'T>() = 
        Func<Async<'T>, Async<Response>>(fun (at: Async<'T>) -> 
            async {
                let! t = at
                return addObjectAndGetResponse t (typeof<'T>)
            })

    static member GetValueResponse<'T>() = 
        Func<Async<'T>, Async<Response>>(fun (at: Async<'T>) -> 
            async {
                let! t = at
                return GetValueResponse(t)
            })

//    static member GetReturnRemote<'T>() = 
//        Func<Async<'T>, Async<Response>>(fun (at: Async<'T>) -> 
//            async {
//                let! t = at
//                return GetValueResponse(t)
//            })

//    static member GetReturn<'T>() = Func<'T,Async<'T>>(fun x -> async.Return(x))

//    static member GetBind<'T,'U>() =
//        Func<Async<'T>, Func<'T, Async<'U>>, Async<'U>>(fun at f -> async.Bind(at, fun x -> f.Invoke(x)))
        
    static member GetDefaultIP() =
        let firstIP =
            Dns.GetHostAddresses("")
            |> Seq.tryFind (fun a -> a.AddressFamily = AddressFamily.InterNetwork)
        match firstIP with
        | Some ip -> ip
        | None -> failwith "Could not find Internet IP"

    static member TryGetServer(ip: IPAddress, port: int) = 
        match instances.TryGetValue ((ip,port)) with
        | true, server -> Some (server :> IRequestHandler)
        | _ -> None

    interface IRequestHandler with
        member __.Address = address
        member x.Port = port
        
        member __.AsyncHandleRequest(req: Request) = handleRequest req

        member __.HandleRequest(req: Request) = handleRequest req |> Async.RunSynchronously

    interface IDisposable with
        
        member __.Dispose() = 
            instances.Remove((address,port)) |> ignore
            network.StopListen()
            for handler in handlers do
                handler.Shutdown()
