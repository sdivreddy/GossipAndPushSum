#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration

// Configuration
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }")

let system = ActorSystem.Create("Gossip", configuration)
type Information = 
    | GossipObj of (list<IActorRef>*IActorRef)
    | GossipObjSelf of (list<IActorRef>*IActorRef)
    | PushsumObj of (float*float*list<IActorRef>*IActorRef)
    | PushsumObjSelf of (list<IActorRef>*IActorRef)
    | Initialize of (list<IActorRef>*string*IActorRef)
    | Terminate of (IActorRef*IActorRef)

type BossMessage = 
    | Start of (string)
    | Received of (string)

// Round off to proper squares for 2D and imperfect 2D
let timer = System.Diagnostics.Stopwatch()
let roundOffNodes (numNode:int) =
    let mutable sqrtVal = numNode |> float |> sqrt |> int
    if sqrtVal*sqrtVal <> numNode then
        sqrtVal <- sqrtVal + 1
    sqrtVal*sqrtVal

// Input from Command Line
let mutable nodes = fsi.CommandLineArgs.[1] |> int
let topology = fsi.CommandLineArgs.[2]
let algo = fsi.CommandLineArgs.[3]
let removeNodes = fsi.CommandLineArgs.[4] |> int

let rand = Random(nodes)
if topology = "imp2D" || topology = "2D" then
    nodes <- roundOffNodes nodes
let mutable presentNodesCnt = nodes - removeNodes

// Build the remaining nodes in the network after removing failure number of nodes randomly.
let hasIdInRemList checkId list = List.exists (fun elem -> elem = checkId) list

let mutable rem = 0
let avlNodes = [1..nodes]
let mutable presentNodes = [1..nodes]
let mutable removeNodesList = []
let remRand = Random()
while rem < removeNodes do
    let num = avlNodes.[remRand.Next(avlNodes.Length)]
    let numAlreadyPresent = hasIdInRemList num removeNodesList
    if not numAlreadyPresent then
        removeNodesList <- num :: removeNodesList
        rem <- rem + 1
        presentNodes <- presentNodes |> List.indexed |> List.filter (fun (i, v) -> v <> num) |> List.map snd

// Builds the neighbours according to the respective network from the remaining nodes
let form2DNeighbours (actorName:string) (mypool:Information) = 
    match mypool with
    | Initialize(pool, topo, boss) ->
        let myId = (actorName.Split '_').[1] |> int
        let mutable neighbourList = []
        let size = sqrt (nodes |> float) |> int
        if myId%size <> 1 then
            let remNodelistHas = hasIdInRemList (myId-1) removeNodesList
            if not remNodelistHas then
                neighbourList <- pool.[myId-2] :: neighbourList
        if myId%size <> 0 then 
            let remNodelistHas = hasIdInRemList (myId+1) removeNodesList
            if not remNodelistHas then
                neighbourList <- pool.[myId] :: neighbourList
        if myId > size then
            let remNodelistHas = hasIdInRemList (myId-size) removeNodesList
            if not remNodelistHas then
                neighbourList <- pool.[myId-size-1] :: neighbourList
        if myId <= (nodes-size) then
            let remNodelistHas = hasIdInRemList (myId+size) removeNodesList
            if not remNodelistHas then
                neighbourList <- pool.[myId+size-1] :: neighbourList
        if topo = "imp2D" then
            let mutable tp = presentNodes.[remRand.Next(presentNodes.Length)]
            let mutable temp = pool.[tp-1]
            while temp.Path.Name = actorName do
                tp <- presentNodes.[remRand.Next(presentNodes.Length)]
                temp <- pool.[tp-1]
            neighbourList <- temp :: neighbourList

        neighbourList

// Build Line Topology - select it's Left and Right neighbours
let formLineNeighbours (actorName:string) (pool:list<IActorRef>) = 
    let mutable neighbourList = []
    let myId = (actorName.Split '_').[1] |> int
    if myId = 1 then
        let remNodelistHas = hasIdInRemList (myId+1) removeNodesList
        if not remNodelistHas then
            neighbourList <- pool.[myId] :: neighbourList
    else if myId = nodes then
        let remNodelistHas1 = hasIdInRemList (myId-1) removeNodesList
        if not remNodelistHas1 then
            neighbourList <- pool.[myId-2] :: neighbourList
    else
        let remNodelistHas2 = hasIdInRemList (myId-1) removeNodesList
        if not remNodelistHas2 then
            neighbourList <- pool.[myId-2] :: neighbourList
        let remNodelistHas3 = hasIdInRemList (myId+1) removeNodesList
        if not remNodelistHas3 then
            neighbourList <- pool.[myId] :: neighbourList
    neighbourList

// Build Full Topology - select all other remaining nodes except for self 
let formFullNeighbours (actorName:string) (pool:list<IActorRef>) = 
    let myId = (actorName.Split '_').[1] |> int
    let neighbourList = pool |> List.indexed |> List.filter (fun (i, _) -> i <> myId-1 && not (hasIdInRemList (i+1) removeNodesList)) |> List.map snd
    neighbourList

// Gossip Actor - receives rumors and sends rumours to one of it's neighbours and to self
// periodically since it first receives until it terminates
let GossipActors (mailbox:Actor<_>) = 
    //let mutable first = true
    let mutable exhausted = 100
    let mutable neighbourList = []
    let mutable neighbourCount = -1
    let mutable gossipTopo = ""
    let mutable refBoss = mailbox.Self
    let mutable donerumor = false
    let rec loop () = actor {
        let! message = mailbox.Receive()
        let mutable gossipmsg : Information = message
        let mutable actorpool = []
        let mutable init = false
        match gossipmsg with
        | GossipObj(pool, bossRef) ->
            exhausted <- exhausted - 1
            actorpool <- pool
        | GossipObjSelf(pool,bossRef) ->
            gossipmsg <- GossipObj(pool,bossRef)
            actorpool <- pool
        | Initialize(pool, topo, boss) ->
            init <- true
            gossipTopo <- topo
            if topo = "2D" || topo = "imp2D" then
                neighbourList <- form2DNeighbours mailbox.Self.Path.Name gossipmsg
            else if topo = "line" then
                neighbourList <- formLineNeighbours mailbox.Self.Path.Name pool
            else 
                neighbourList <- formFullNeighbours mailbox.Self.Path.Name pool  
            neighbourCount <- neighbourList.Length
            refBoss <- boss
            if neighbourCount = 0 then  
                refBoss <! Received("Terminated")
                exhausted <- 0
            return! loop()
        | Terminate(killedActorRef, bossRef) ->
            neighbourCount <- neighbourCount - 1
            if neighbourCount = 0 then
                donerumor <- true
                exhausted <- -1
                bossRef <! Received("Terminated")
        | _ -> ignore()

        if exhausted >= 0 then
            let randGossip = System.Random()
            if exhausted = 0 then
                exhausted <- -1
            if neighbourList.Length > 0 then
                let neighbour = neighbourList.[randGossip.Next(neighbourList.Length)]
                neighbour <! gossipmsg
            system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0), mailbox.Self, GossipObjSelf(actorpool, refBoss))
        else if not donerumor then
            donerumor <- true
            exhausted <- -1
            refBoss <! Received("Terminated")
            neighbourList |> List.iter (fun item -> 
                item <! Terminate(mailbox.Self, refBoss))
        else
            exhausted <- -1

        return! loop()
    }
    loop()

// Boss creates actors pool and picks a available random Node to start with and
// Terminates when all the remaining nodes receive rumour 100 number of times 
let BossActor (mailbox:Actor<_>) = 
    let mutable reached = 0
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with 
        | Start(_) -> 
            if algo = "gossip" then
                let actorsPool = 
                    [1 .. nodes]
                    |> List.map(fun id -> spawn system (sprintf "Actor_%d" id) GossipActors)
                
                actorsPool |> List.iter (fun item -> 
                    item <! Initialize(actorsPool, topology, mailbox.Self))
                timer.Start() 
                let rnd = Random()
                let pick = presentNodes.[rnd.Next(presentNodes.Length)]
                actorsPool.[pick-1] <! GossipObj(actorsPool, mailbox.Self)                
        | Received(_) -> 
            reached <- reached + 1
            if reached = presentNodesCnt then
                printfn "Time = %i\n" timer.ElapsedMilliseconds
                mailbox.Context.Stop(mailbox.Self)
                mailbox.Context.System.Terminate() |> ignore
        | _ -> ()

        return! loop()
    }
    loop()

// Start - Call the delegator Boss
let boss = spawn system "boss" BossActor
boss <! Start("start")
system.WhenTerminated.Wait()