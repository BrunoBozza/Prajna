// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

open System
open System.Net
open System.Net.Sockets
open System.Threading
open Prajna.Tools
open Prajna.Nano

let printUsage() = printfn "Usage: NanoServer.exe <portToLisen>"

let startNanoServer (argv: string[]) =
    if argv.Length <> 1 then
        printUsage()
        -1
    else
        match UInt16.TryParse argv.[0] with
        | true, port ->
            BufferListStream<byte>.BufferSizeDefault <- 1 <<< 18
            BufferListStream<byte>.InitSharedPool()
            let thisAsm = System.Reflection.Assembly.GetExecutingAssembly()
            printfn "Starting NanoServer on port %d" port
            printfn "CodeBase is: %s" thisAsm.CodeBase
            use server = new ServerNode(int port)
            lock argv (fun _ -> System.Threading.Monitor.Wait argv |> ignore; 0)
        | _ -> 
            printfn "Error: Invalid port number"
            -2

let inline receiveAll (socket: Socket) (bytes: byte[]) (offset: int) (count: int) =
    let mutable cur = 0
    while cur < count do
        cur <- cur + socket.Receive(bytes, offset + cur, count - cur, SocketFlags.None)

let inline sendAll (socket: Socket) (bytes: byte[]) (offset: int) (count: int) =
    let mutable cur = 0
    while cur < count do
        cur <- cur + socket.Send(bytes, offset + cur, count - cur, SocketFlags.None)

let echo (socket: Socket) (bytes: byte[]) (count: int) =
    receiveAll socket bytes 0 count
    sendAll socket bytes 0 count 

let echo2Calls (socket: Socket) (bytes: byte[]) (count: int) =
    receiveAll socket bytes 0 4
    receiveAll socket bytes 4 (count - 4)
    sendAll socket bytes 0 4
    sendAll socket bytes 4 (count - 4)

let startEchoThread (port: int) (count: int) = 
    printfn "Creating TcpListener"
    let server = new TcpListener( IPAddress.Any, port)
    server.Start()
    printfn "TcpListener started"
    let serverThread = 
        new Thread(new ThreadStart(fun _ -> 
            printfn "TcpListener thread start"
            while true do 
                try 
                    let socket = server.AcceptSocket()
                    socket.NoDelay <- true
                    let buffer = Array.zeroCreate<byte> 10000000
                    printfn "Echo thread running on port %d" port
                    while true do
                        echo2Calls socket buffer count
                with  
                    | :? SocketException -> ()))
    serverThread.Start()

let startEchoThreadUdp (port: int) = 
    printfn "Creating TcpListener"
    printfn "TcpListener started"
    let serverThread = 
        new Thread(new ThreadStart(fun _ -> 
            printfn "TcpListener thread start"
            while true do 
                try 
                    use socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp)
                    socket.Bind(IPEndPoint(IPAddress.Parse("10.196.45.2"), 1500))
                    let mutable remote = IPEndPoint(IPAddress.Any, 1500) :> EndPoint
                    let buffer = Array.zeroCreate<byte> 1
                    printfn "Echo thread running on port %d" port
                    while true do
                        socket.ReceiveFrom(buffer, 1, SocketFlags.None, &remote) |> ignore
                        socket.SendTo(buffer, 1, SocketFlags.None, remote) |> ignore
                with  
                    | :? SocketException -> ()))
    serverThread.Start()


[<EntryPoint>]
let main argv = 

//    do Prajna.Tools.Logger.ParseArgs([|"-verbose"; "err" (*; "-con"*)|])
    startNanoServer argv    

//    startEchoThread (Int32.Parse(argv.[0])) (Int32.Parse(argv.[1]))
//    0

