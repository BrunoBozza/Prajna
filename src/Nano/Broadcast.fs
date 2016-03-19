namespace Prajna.Nano

open System
open System.Threading.Tasks
open System.Diagnostics
open System.Collections.Concurrent
open System.IO

open Prajna.Tools
open Prajna.Tools.FSharp

type Distributed<'T>(remotes: Remote<'T>[]) =

    member this.Remotes = remotes    

    member this.Apply(func: Func<'T, 'U>) : Async<Distributed<'U>> = 
        async {
            let! us = 
                remotes
                |> Array.map (fun r -> r.Apply(func))
                |> Async.Parallel
            return Distributed<'U>(us)
        }

    member this.GetValues() : Async<'T[]> =
        remotes 
        |> Array.map (fun r -> r.GetValue())
        |> Async.Parallel


type internal RemoteStack = Remote<ConcurrentStack<int*MemoryStreamB>>


type DistTree<'T> = DistTree of Remote<'T * DistTree<'T>[]> 


type Broadcaster(clients: ClientNode[]) =

    static let makeTree (rs: Remote<'T>[]) (degree: int) =
        let rec makeTree' (i: int) : Async<DistTree<'T>> =
            async {
                let root = rs.[i]
                let! children = 
                    [| for c in (i*degree+1)..(min (i*degree+degree) (rs.Length-1)) -> makeTree' c |]
                    |> Async.Parallel
                let! pair = root.Apply(fun rData -> rData, children )
                return DistTree pair
            }
        makeTree' 0

    static let rec iter (f: 'T -> unit) (DistTree(tree): DistTree<'T>)  : Async<unit> =
        tree.ApplyAsync(fun (x, children) -> 
            async {
                do f x
                do! children |> Array.map (fun c -> iter f c) |> Async.Parallel |> Async.Ignore
            }
        )
        |> Async.Ignore


    let newRemoteStacks() =
        async {
            let newStack = Serializer.Serialize(Func<_>(fun _ -> new ConcurrentStack<int*MemoryStreamB>()))
            let! ret =
                clients 
                |> Array.map (fun c -> c.NewRemote newStack)
                |> Async.Parallel 
            newStack.Bytes.Dispose()
            return ret
        }

//    let newRemoteStackTree (remoteStacks: Remote<'T>[]) (degree: int) =
//        async {
//            let! stacks = remoteStacks
//            return! makeTree stacks degree
//        }
//
    let newForwarder2 (stacks: RemoteStack[]) : Async<Remote<int * MemoryStreamB -> Async<unit>>>  =
        let forwardOne (rs: RemoteStack) (nextFuncOption: Option<Async<Remote<int * MemoryStreamB -> Async<unit> >>>) : Option<Async<Remote<int * MemoryStreamB -> Async<unit>>>> =
            match nextFuncOption with 
            | None -> Some(rs.Apply(Func<_,_>(fun (stack: ConcurrentStack<_>) -> (fun bytes -> async { return stack.Push bytes }))))
            | Some(asyncNextFunc) -> 
                Some(async {
                        let! nextFunc = asyncNextFunc
                        return! rs.Apply(
                                    Func<_,_>(fun (stack: ConcurrentStack<_>) -> 
                                        fun posBytesPair ->
                                            stack.Push posBytesPair
                                            nextFunc.ApplyAsyncAndGetValue(fun f -> f posBytesPair)
                                    )
                                )
                        })
        Array.foldBack forwardOne stacks None |> Option.get

    let getChunks (stream: MemoryStreamB) : MemoryStreamB[] =
        let chunkSize = (stream.Length / 300L) + 1L
        let ret = 
            [|while stream.Position < stream.Length do
                let curChunkSize = min (int64 chunkSize) (stream.Length - stream.Position)
                let ret = new MemoryStreamB(stream, stream.Position, curChunkSize)
                ret.Seek(0L, SeekOrigin.Begin) |> ignore
                stream.Position <- stream.Position + curChunkSize
                yield ret|]
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Broadcast: chunk size: %d. NumChunks: %d." chunkSize ret.Length)
        ret

    let transpose (arr: 'T[][]) : 'T[][] =
        let nRows = arr.Length
        let nCols = arr.[0].Length
        Array.init nCols (fun j -> Array.init nRows (fun i -> arr.[i].[j]))

    member internal this.BroadcastTree<'T>(serFunc: MemoryStreamB, treeDegree: int) : Async<Distributed<'T>> =
        async {
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: starting")
            let chunks = getChunks serFunc
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: got chunks")
            let! remoteStacks = newRemoteStacks ()
            let! stackTree = makeTree remoteStacks treeDegree
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: got remote stacks")
            do!
                chunks
//                |> Array.mapi (fun i chunk -> forwarder.ApplyAsyncAndGetValue(fun addToStackAndForward -> addToStackAndForward (i,chunk)))
                |> Array.mapi (fun i chunk -> stackTree |> iter (fun stack -> stack.Push((i,chunk)) ))
                |> Async.Parallel
                |> Async.Ignore
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: all chunks sent and received")
            let! remotes = 
                remoteStacks
                |> Array.map (fun remoteStack ->
                    remoteStack.Apply(fun stack ->
                        let newStream = new MemoryStreamB()
                        let orderedChunks = 
                            seq {while stack.Count > 0 do match stack.TryPop() with | true,v -> yield v | _ -> failwith "Failed to pop"}
                            |> Seq.sortBy fst
                            |> Seq.map snd
                            |> Seq.toArray
                        for chunk in orderedChunks do
                            newStream.AppendNoCopy(chunk, 0L, chunk.Length)
                            //chunk.Dispose()
                        newStream.Seek(0L, SeekOrigin.Begin) |> ignore
                        let f = Serializer.Deserialize(newStream) :?> Func<'T>
                        let ret = f.Invoke()
                        newStream.Dispose()
                        ret)
                    )
                |> Async.Parallel
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: chunks assembled on remotes")
            chunks |> Array.iter (fun c -> c.Dispose())
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: chunks disposed")
            return Distributed(remotes)
        }

    member internal this.BroadcastChained2<'T>(serFunc: MemoryStreamB) : Async<Distributed<'T>> =
        async {
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: starting")
            let chunks = getChunks serFunc
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: got chunks")
            let! remoteStacks = newRemoteStacks()
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: got remote stacks")
            let! forwarder = newForwarder2 remoteStacks
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: got forwarder")
            do!
                chunks
                |> Array.mapi (fun i chunk -> forwarder.ApplyAsyncAndGetValue(fun addToStackAndForward -> addToStackAndForward (i,chunk)))
                |> Async.Parallel
                |> Async.Ignore
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: all chunks sent and received")
            let! remotes = 
                remoteStacks
                |> Array.map (fun remoteStack ->
                    remoteStack.Apply(fun stack ->
                        let newStream = new MemoryStreamB()
                        let orderedChunks = 
                            seq {while stack.Count > 0 do match stack.TryPop() with | true,v -> yield v | _ -> failwith "Failed to pop"}
                            |> Seq.sortBy fst
                            |> Seq.map snd
                            |> Seq.toArray
                        for chunk in orderedChunks do
                            newStream.AppendNoCopy(chunk, 0L, chunk.Length)
                            //chunk.Dispose()
                        newStream.Seek(0L, SeekOrigin.Begin) |> ignore
                        let f = Serializer.Deserialize(newStream) :?> Func<'T>
                        let ret = f.Invoke()
                        newStream.Dispose()
                        ret)
                    )
                |> Async.Parallel
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: chunks assembled on remotes")
            chunks |> Array.iter (fun c -> c.Dispose())
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: chunks disposed")
            return Distributed(remotes)
        }

    member this.BroadcastChained(func: Serialized<Func<'T>>) : Async<Distributed<'T>> =
        this.BroadcastChained2<'T>(func.Bytes)

    member this.BroadcastTree(func: Func<'T>, treeDegree: int) : Async<Distributed<'T>> =
        let serFunc = (Serializer.Serialize func).Bytes
        async {
            let! dist = this.BroadcastTree<'T>(serFunc, treeDegree)
            serFunc.Dispose()
            return dist
        }

    member this.BroadcastChained(func: Func<'T>) : Async<Distributed<'T>> =
        let serFunc = (Serializer.Serialize func).Bytes
        async {
            let! dist = this.BroadcastChained2<'T>(serFunc)
            serFunc.Dispose()
            return dist
        }

    member this.BroadcastParallel(func: Func<'T>) = 
        async {
            let serFunc = Serializer.Serialize(func)
            let! rs = 
                clients
                |> Array.map (fun c -> c.NewRemote serFunc)
                |> Async.Parallel
            return new Distributed<'T>(rs)
        }
