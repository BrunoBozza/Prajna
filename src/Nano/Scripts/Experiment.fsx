#I __SOURCE_DIRECTORY__

#r @"..\..\Tools\Tools\bin\Debugx64\Prajna.Tools.dll"

#load "load-project-debug.fsx"

open Prajna.Tools
open Prajna.Nano



do BufferListStream<byte>.BufferSizeDefault <- 1 <<< 10
do BufferListStream<byte>.InitSharedPool()


let bytes = Array.init 1500 (fun i -> byte i)
let ms = new MemoryStreamB()
ms.Capacity64 <- 2500L

//ms.WriteArr(bytes)

ms.Position <- 5L

let reader = new StreamReader<byte>(ms, 500L, 600L)
let ret = reader.ApplyFnToBuffers( fun bpl -> printfn "%A" bpl)

ms.Capacity64 <- 20L


let remoteNextClients : Remote<ClientNode>[] = null



do Prajna.Tools.Logger.ParseArgs([|"-con"|])

let serverNode = new ServerNode(1500)

let cn = new ClientNode(ServerNode.GetDefaultIP(), 1500)
let cn2 = new ClientNode(ServerNode.GetDefaultIP(), 1500)

async {
    let acc = ref 0
    for i = 1 to 1 do
        let! r1 = cn.AsyncNewRemote(fun _ -> "Test2")
        let! r2 = r1.AsyncApply(fun str -> str.Length)

        let! r3 = cn2.AsyncNewRemote(fun _ -> "Test33")
        let! r4 = r3.AsyncApply(fun str -> str.Length)

        acc := !acc + r2.GetValue() + r4.GetValue()

    printfn "%d" !acc
}
|> Async.RunSynchronously
printfn ""

