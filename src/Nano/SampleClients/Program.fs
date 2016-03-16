// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

open System
open System.Threading
open System.Net
open System.Net.Sockets
open System.Diagnostics
open Prajna.Nano

open Prajna.Tools


let hello() =
    use client = new ClientNode(ServerNode.GetDefaultIP(), 1500)
    client.AsyncNewRemote(fun _ -> printfn "Hello") |> Async.RunSynchronously |> ignore

let helloParallel() =
    let clients = [1500..1503] |> List.map (fun p -> new ClientNode(ServerNode.GetDefaultIP(), p)) |> List.toArray
    try
        clients 
        |> Array.mapi (fun i c -> c.AsyncNewRemote(fun _ -> printfn "Hello at Machine----: %d!" i; i))
        |> Async.Parallel
        |> Async.RunSynchronously
        |> ignore
        ()
    finally
        clients |> Array.iter (fun c -> (c :> IDisposable).Dispose())

let broadcast() =
    let clients = [1500..1503] |> List.map (fun p -> new ClientNode(ServerNode.GetDefaultIP(), p)) |> List.toArray
    try
        async {
            let broadcaster = new Broadcaster(clients)
            let data = [|1L..80000000L|]
            let sw = Stopwatch.StartNew()
            let! distributed = broadcaster.BroadcastParallel(fun _ -> 
                let pid = Process.GetCurrentProcess().Id
                printfn "PID: %d" pid
                data.[0] <- int64 pid
                data)
            printfn "Broadcasting took: %A" sw.Elapsed
            do! distributed.Apply(fun data -> printfn "First element is: %d" data.[0]) |> Async.Ignore

        }
        |> Async.RunSynchronously
    finally
        clients |> Array.iter (fun c -> (c :> IDisposable).Dispose())

let getOneNetClusterIPs(machines: int list) =
    machines 
    |> List.map(fun m -> 
        Dns.GetHostAddresses("OneNet" + m.ToString()) 
        |> Seq.filter(fun ad -> ad.AddressFamily.ToString() = System.Net.Sockets.ProtocolFamily.InterNetwork.ToString())
        |> Seq.nth 0)
    |> Seq.toList

let resetTiming, time =
    let sw = Stopwatch()
    (fun () -> sw.Restart()), (fun (msg: string) -> printfn "%s: %A" msg sw.Elapsed; sw.Restart())


let broadcastCluster() =
    do Prajna.Tools.Logger.ParseArgs([|"-verbose"; "info"; "-con"|])
    
    Prajna.Tools.BufferListStream<byte>.BufferSizeDefault <- 1 <<< 20

    resetTiming()
    let ips = getOneNetClusterIPs [21..35]
    time "Getting IPs"

    let clients = 
        ips 
        |> List.toArray 
        |> Array.map(fun ip -> async{ return new ClientNode(ip, 1500) })
        |> Async.Parallel
        |> Async.RunSynchronously
    time "Connecting"

    let broadcaster = Broadcaster(clients)
    time "Starting broadcaster"

    let d = 
        broadcaster.BroadcastChained(fun _ ->
            let m = Environment.MachineName
            printfn "Hello from %s!" m
            m)
        |> Async.RunSynchronously
    printfn "Machine names: %A" (d.Remotes |> Array.map (fun r -> r.AsyncGetValue()) |> Async.Parallel |> Async.RunSynchronously )
    time "Broadcasting machine name fetch"

    resetTiming()
    let longs = Array.init 4 (fun _ -> 
                    Array.init 125000000 (fun i -> i)
                ) 
    time "Initializing arrays"

    resetTiming()
    let mbs = [for arr in longs -> 
                (float(arr.Length * 8) / 1000000.0 
              )] |> List.sum
    printfn "Broadcasting %2.2fMB" mbs
    let arrs = broadcaster.BroadcastChained(fun _ -> printfn "Received longs"; longs) |> Async.RunSynchronously
    time (sprintf "Broadcast %2.2fMB" mbs)

let latency (argv: string[]) =

    let numTrips = Int32.Parse(argv.[0])
    let numBytes = Int32.Parse(argv.[1])

    printfn "Starting"
    let client = new ClientNode( getOneNetClusterIPs [21] |> Seq.nth 0, 1500 )
    let r = 
        client.AsyncNewRemote(fun _ -> Array.init numBytes (fun i -> byte (i % 256))) 
        |> Async.RunSynchronously
    time "Connected and created"

    do r.AsyncGetValue() |> Async.RunSynchronously |> ignore
    time "First get"

    resetTiming()

    let sw = Stopwatch.StartNew()
    let vals = Array.init numTrips (fun _ -> r.AsyncGetValue() |> Async.RunSynchronously)
    let elapsed = sw.Elapsed
    printf "%d round trips: %A. (avg. round trip time: %Ams)" numTrips elapsed (elapsed.TotalMilliseconds / float numTrips)

let latencyParallel (argv: string[]) =

    let numTrips = Int32.Parse(argv.[0])
    let numBytes = Int32.Parse(argv.[1])
    let numAsyncs = Int32.Parse(argv.[2])

    printfn "Starting"
    let client = new ClientNode( getOneNetClusterIPs [21] |> Seq.nth 0, 1500 )
    let r = 
        client.AsyncNewRemote(fun _ -> let rnd = new Random() in Array.init numBytes (fun _ -> rnd.Next(256) |> byte)) 
        |> Async.RunSynchronously
    time "Connected and created"

    do r.AsyncGetValue() |> Async.RunSynchronously |> ignore
    time "First get"

    resetTiming()
    let sw = Stopwatch.StartNew()
    let vals = 
        Array.init numAsyncs (fun i -> 
            async { 
                for j in i..numAsyncs..numTrips do 
                    do! r.AsyncGetValue() |> Async.Ignore  
            })
        |> Async.Parallel
        |> Async.RunSynchronously
//        Array.init numTrips (fun _ -> r.AsyncGetValue() |> Async.RunSynchronously)
    let elapsed = sw.Elapsed
    printf "%d round trips: %A. (avg. round trip time: %Ams)" numTrips elapsed (elapsed.TotalMilliseconds / float numTrips)

let inline receiveAll (socket: Socket) (bytes: byte[]) (offset: int) (count: int) =
    let mutable cur = 0
    while cur < count do
        cur <- cur + socket.Receive(bytes, offset + cur, count - cur, SocketFlags.None)

let inline sendAll (socket: Socket) (bytes: byte[]) (offset: int) (count: int) =
    let mutable cur = 0
    while cur < count do
        cur <- cur + socket.Send(bytes, offset + cur, count - cur, SocketFlags.None)

let roundTrip (socket: Socket) (bytes: byte[]) (count: int) =
    sendAll socket bytes 0 count
    receiveAll socket bytes 0 count

let roundTripMemoryStream (socket: Socket) (bytes: MemoryStreamB) =
    let serStream = new StreamReader<_>(bytes, 0L, bytes.Length)
    serStream.ApplyFnToParts(fun part -> sendAll socket part.Elem.Buffer part.Offset (int part.Count))
    let len = int bytes.Length
    let retBytes = Array.zeroCreate<byte> len
    receiveAll socket retBytes 0 len
    new MemoryStreamB(retBytes)

let roundTrip2Calls (socket: Socket) (bytes: byte[]) (count: int) =
    socket.Send(bytes, 0, 4, SocketFlags.None) |> ignore
    socket.Send(bytes, 4, count - 4, SocketFlags.None) |> ignore
    socket.Receive(bytes, 0, 4, SocketFlags.None) |> ignore
    let retCount = BitConverter.ToInt32(bytes, 0)
//    printfn "retCount = %d" retCount
    socket.Receive(bytes, 4, retCount, SocketFlags.None) |> ignore

let rawLatency (args: string[]) =
    printfn "Starting"
    let machine = Int32.Parse(args.[0])
    let port = Int32.Parse(args.[1])
    let numTrips = Int32.Parse(args.[2])
    let count = Int32.Parse(args.[3])
    let client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
    client.NoDelay <- true
    client.Connect(getOneNetClusterIPs [machine] |> Seq.nth 0, port)
    printfn "Connected"
    let bytes = 
        let r = new Random()
        Array.init<byte> 10000000 (fun _ -> r.Next(256) |> byte)
    Array.Copy(BitConverter.GetBytes(count), bytes, 4)
    let sw = Stopwatch.StartNew()
    roundTrip client bytes count
    printfn "First round trip: %A" sw.Elapsed
    sw.Restart()
    for i = 1 to numTrips do
        roundTrip client bytes count
    let elapsed = sw.Elapsed
    printf "%d round trips: %A. (avg. round trip time: %Ams)" numTrips elapsed (elapsed.TotalMilliseconds / float numTrips)

let rawLatencyMemoryStream (args: string[]) =
    printfn "Starting"
    let machine = Int32.Parse(args.[0])
    let port = Int32.Parse(args.[1])
    let numTrips = Int32.Parse(args.[2])
    let count = Int32.Parse(args.[3])
    let client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
    client.NoDelay <- true
    client.Connect(getOneNetClusterIPs [machine] |> Seq.nth 0, port)
    printfn "Connected"
    let bytes = 
        let r = new Random()
        Array.init<byte> count (fun _ -> r.Next(256) |> byte)
    let countBuf = BitConverter.GetBytes(int64 (count - 8))
    Array.Copy(countBuf, bytes, 8)
    let sw = Stopwatch.StartNew()
    roundTrip client bytes count
    printfn "First round trip: %A" sw.Elapsed
    sw.Restart()
    for i = 1 to numTrips do
        let ms = new MemoryStreamB(bytes)
        let ms2 = roundTripMemoryStream client ms
        if ms.Length <> ms2.Length then
            raise <| Exception("Different lengths")
    let elapsed = sw.Elapsed
    printf "%d round trips: %A. (avg. round trip time: %Ams)" numTrips elapsed (elapsed.TotalMilliseconds / float numTrips)

let rawLatencyWithSer (args: string[]) =
    printfn "Starting"
    let machine = Int32.Parse(args.[0])
    let port = Int32.Parse(args.[1])
    let numTrips = Int32.Parse(args.[2])
    let count = Int32.Parse(args.[3])
    let client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
    client.NoDelay <- true
    client.Connect(getOneNetClusterIPs [machine] |> Seq.nth 0, port)
    printfn "Connected"

    let mutable x = 10
    let closure() = x <- x + 1; x
    let sw = Stopwatch.StartNew()
    let closureMS = (Serializer.Serialize(closure)).Bytes
    let echoClosure = roundTripMemoryStream client closureMS |> Serializer.Deserialize :?> (unit -> int)
    printfn "First round trip: %A" sw.Elapsed
    printfn "Closure size: %d" closureMS.Length
    printfn "Local closure result: %d" (closure())
    printfn "Round tripped closure result: %d" (echoClosure())
    sw.Restart()

    for i = 1 to numTrips do
        let serClosure = Serializer.Serialize(closure)
        let ms2 = roundTripMemoryStream client serClosure.Bytes
        let deserClosureObj = Serializer.Deserialize(ms2)
        let deserCloser = deserClosureObj :?> (unit -> int)
        ()

    let elapsed = sw.Elapsed
    printf "%d round trips: %A. (avg. round trip time: %Ams)" numTrips elapsed (elapsed.TotalMilliseconds / float numTrips)

let rawLatencyUdp (args: string[]) =
    printfn "Starting"
    let machine = Int32.Parse(args.[0])
    let port = Int32.Parse(args.[1])
    let numTrips = Int32.Parse(args.[2])
    let client = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp)
    client.Bind(IPEndPoint(IPAddress.Any, 1500))
    let mutable remote = IPEndPoint(getOneNetClusterIPs [machine] |> Seq.nth 0, 1500) :> EndPoint
    printfn "Bound"
    let bytes = Array.zeroCreate 1
    let sw = Stopwatch.StartNew()
    client.SendTo(bytes, 0, 1, SocketFlags.None, remote) |> ignore
    client.ReceiveFrom(bytes, 1, SocketFlags.None, &remote) |> ignore
    printfn "First round trip: %A" sw.Elapsed
    sw.Restart()
    for i = 1 to numTrips do            
        client.SendTo(bytes, 0, 1, SocketFlags.None, remote) |> ignore
        client.ReceiveFrom(bytes, 1, SocketFlags.None, &remote) |> ignore
    let elapsed = sw.Elapsed
    printf "%d round trips: %A. (avg. round trip time: %Ams)" numTrips elapsed (elapsed.TotalMilliseconds / float numTrips)

[<EntryPoint>]
let main argv = 
//    BufferListStream<byte>.BufferSizeDefault <- 1 <<< 16
    // do Prajna.Tools.Logger.ParseArgs([|"-verbose"; "info"; "-con"|])
    BufferListStream<byte>.InitSharedPool()
    rawLatencyWithSer argv
    //latency argv
    // broadcastCluster()
    0 // return an integer exit code
