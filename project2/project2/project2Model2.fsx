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
            stdout-loglevel : DEBUG
            loglevel : ERROR
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
let rand = Random(nodes)
if topology = "imp2D" || topology = "2D" then
    nodes <- roundOffNodes nodes
 
 // Builds the neighbours according to the respective network from the nodes
let form2DNeighbours (actorName:string) (mypool:Information) = 
    match mypool with
    | Initialize(pool, topo, boss) ->
        let myId = (actorName.Split '_').[1] |> int
        let mutable neighbourList = []
        let mutable pickId = 0
        let size = sqrt (nodes |> float) |> int
        if myId%size <> 1 then
            neighbourList <- pool.[myId-2] :: neighbourList
        if myId%size <> 0 then 
            neighbourList <- pool.[myId] :: neighbourList
        if myId > size then
            neighbourList <- pool.[myId-size-1] :: neighbourList
        if myId <= (nodes-size) then
            neighbourList <- pool.[myId+size-1] :: neighbourList
        if topo = "imp2D" then
            let mutable temp = pool.[rand.Next()%nodes]
            while temp.Path.Name = actorName do
                temp <- pool.[rand.Next()%nodes]
            neighbourList <- temp :: neighbourList

        neighbourList

// Build Line Topology - select it's Left and Right neighbours
let formLineNeighbours (actorName:string) (pool:list<IActorRef>) = 
    let mutable neighbourList = []
    let myId = (actorName.Split '_').[1] |> int
    if myId = 1 then
       neighbourList <- pool.[myId] :: neighbourList 
    else if myId = nodes then
       neighbourList <- pool.[myId-2] :: neighbourList 
    else
       neighbourList <- pool.[myId-2] :: neighbourList
       neighbourList <- pool.[myId] :: neighbourList
    neighbourList

// Build Full Topology - select all other nodes except for self 
let formFullNeighbours (actorName:string) (pool:list<IActorRef>) = 
    let myId = (actorName.Split '_').[1] |> int
    let neighbourList = pool |> List.indexed |> List.filter (fun (i, _) -> i <> myId-1) |> List.map snd
    neighbourList

// Push-sum: for aggregation calculation. State pair (s, w) is used to calculate the 
// convergence. The s/w ratio is added when recceived and halfed when sent to external neighbor from its
// list but remains same when for self message
let PushSumActors (mailbox:Actor<_>) =
    let mutable neighbourList = []
    let mutable s = 0.0
    let mutable w = 1.0
    let mutable endThis = 0
    let mutable prevValue = s/w
    let mutable pushSumTopo = ""
    let mutable bossRef = mailbox.Self
    let rec loop () = actor {
        let! message = mailbox.Receive()
        let mutable pushmsg : Information = message
        let mutable actorPool = []
        match pushmsg with
        | PushsumObjSelf(pool, bRef) ->
            actorPool <- pool
        | PushsumObj(is,iw,pool,boss) ->
            actorPool <- pool
            if endThis < 3 then
                s <- is + s
                w <- iw + w
                if abs ((s/w) - prevValue) <= (pown 10.0 -10) then
                    endThis <- endThis + 1
                    if endThis = 3 then
                        bossRef <! Received("Terminated")
                        neighbourList |> List.iter (fun item -> 
                            item <! Terminate(mailbox.Self, bossRef))
                else
                    endThis <- 0
        | Initialize(pool, topo, boss) ->
            pushSumTopo <- topo
            bossRef <- boss
            s <- (mailbox.Self.Path.Name.Split '_').[1] |> float
            prevValue <- s/w
            if topo = "2D" || topo = "imp2D" then
                neighbourList <- form2DNeighbours mailbox.Self.Path.Name pushmsg
            else if topo = "line" then 
                neighbourList <- formLineNeighbours mailbox.Self.Path.Name pool
            else
                neighbourList <- formFullNeighbours mailbox.Self.Path.Name pool
            return! loop()
        | Terminate(killedActorRef, boss) ->
            let myId = (killedActorRef.Path.Name.Split '_').[1] |> int
            let before = neighbourList.Length
            neighbourList <- neighbourList |> List.indexed |> List.filter (fun (i, v) -> ((v.Path.Name.Split '_').[1] |> int) <> myId) |> List.map snd

            if neighbourList.Length = 0 then
                endThis <- 100
                bossRef <! Received("Terminated")
        | _ -> ignore()
    
        if endThis <= 3 then
            prevValue <- s/w
            s <- s/2.0
            w <- w/2.0
            if endThis = 3 then
                endThis <- 100
            let randPushSum = System.Random()
            if neighbourList.Length > 0 then
                let neighbour = neighbourList.[randPushSum.Next(neighbourList.Length)]
                neighbour <! PushsumObj(s,w,actorPool,bossRef)
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0), mailbox.Self, PushsumObjSelf(actorPool, bossRef))

        return! loop()
    }
    loop()

// Gossip Actor - receives rumors and sends rumours to one of it's neighbours and to self
// periodically since it first receives until it terminates
let GossipActors (mailbox:Actor<_>) = 
    let mutable first = true
    let mutable exhausted = 100
    let mutable neighbourList = []
    let mutable gossipTopo = ""
    let rec loop () = actor {
        let! message = mailbox.Receive()
        let mutable gossipmsg : Information = message
        let mutable actorpool = []
        let mutable refBoss = mailbox.Self
        let mutable init = false
        match gossipmsg with
        | GossipObj(pool, bossRef) ->
            if first then
                first <- false
                bossRef <! Received("Received")
            exhausted <- exhausted - 1
            actorpool <- pool
            refBoss <- bossRef
        | GossipObjSelf(pool,bossRef) ->
            gossipmsg <- GossipObj(pool,bossRef)
            actorpool <- pool
            refBoss <- bossRef
        | Initialize(pool, topo, boss) ->
            init <- true
            gossipTopo <- topo
            if topo = "2D" || topo = "imp2D" then
                neighbourList <- form2DNeighbours mailbox.Self.Path.Name gossipmsg
            else if topo = "line" then
                neighbourList <- formLineNeighbours mailbox.Self.Path.Name pool
            else 
                neighbourList <- formFullNeighbours mailbox.Self.Path.Name pool  
            return! loop()
        | _ -> ignore()

        if not init then
            if exhausted >= 0 then
                let randImp2D = System.Random()
                if exhausted = 0 then
                    exhausted <- -1
                if neighbourList.Length > 0 then
                    let neighbour = neighbourList.[randImp2D.Next(neighbourList.Length)]
                    neighbour <! gossipmsg
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0), mailbox.Self, GossipObjSelf(actorpool, refBoss))
            else
                exhausted <- -1
                mailbox.Context.Stop(mailbox.Self)

        return! loop()
    }
    loop()

// Initializes the pool of actors, builds the topology and randomly select one actor to start with
// Terminates when all the actors receive rumour 100 times for gossip
// For push-sum when s/w ratio is approx same for 3 rounds
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
                actorsPool.[(rand.Next()) % nodes] <! GossipObj(actorsPool, mailbox.Self)                
            else if algo = "push-sum" then
                let actorsPool = 
                    [1 .. nodes]
                    |> List.map(fun id -> spawn system (sprintf "Actor_%d" id) PushSumActors)
                actorsPool |> List.iter (fun item -> 
                    item <! Initialize(actorsPool, topology, mailbox.Self))
                timer.Start()
                actorsPool.[(rand.Next()) % nodes] <! PushsumObjSelf(actorsPool, mailbox.Self)
        | Received(_) ->
            reached <- reached + 1
            if reached = nodes then
                printfn "Time taken = %i\n" timer.ElapsedMilliseconds
                mailbox.Context.Stop(mailbox.Self)
                mailbox.Context.System.Terminate() |> ignore
        | _ -> ()

        return! loop()
    }
    loop()

// Start of the algorithm - spawn Boss, the delgator
let boss = spawn system "boss" BossActor
boss <! Start("start")
// Wait until all the actors has finished processing
system.WhenTerminated.Wait()