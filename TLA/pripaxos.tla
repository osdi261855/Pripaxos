
--------------------------- MODULE pripaxos -----------------------------
(*
    This is a specification of the EPaxos protocol (deps-only abstraction).
    - Removed explicit reach tracking - use message queries instead
    - Removed Paxos legacy variables (decision, maxBallot, maxVBallot, maxValue)
    - committed uses set style (like epaxos.tla) to detect inconsistency
    - deps uses set style SUBSET Instances like epaxos.tla
    - Instances == Replicas × (1..Cardinality(Commands)) like epaxos.tla
    - cmdLog uses sparse set-of-records style like epaxos.tla (space efficient)
*)

EXTENDS TLC, Naturals, FiniteSets, Integers
          
CONSTANTS Commands, Replicas, Quorums, none

\* none is a model value provided by the .cfg (must be distinct from Commands)

VARIABLES messages \* Set of all messages sent.
VARIABLES crtInstance \* Highest local instance number used so far by each replica: [Replicas -> Int]
\* cmdLog[observer] = set of records, each record has {inst: <<cmdLeader, instNum>>, deps: ...}
\* Sparse storage - only stores instances that have been seen
VARIABLES cmdLog
\* committed[inst] = set of <<cmd, deps>> tuples (inst = <<cmdLeader, instNum>>)
\* If Cardinality > 1, different replicas committed different commands = BUG!
VARIABLES committed
\* proposed = commands that have been proposed (like epaxos.tla)
\* Each command can only be proposed once
VARIABLES proposed

\* Instance is a tuple <<cmdLeader, instNum>> like epaxos.tla
Instances == Replicas \X (1..Cardinality(Commands))

\* Deps is a set of <<replica, instNum>> pairs
DepsType == SUBSET Instances

\* Type for committed: just deps (simplified, cmd is implicit from instance)
CommittedValue == DepsType

\* cmdLog record type
CmdLogRecord == [inst: Instances, deps: DepsType]

\* Set of all possible messages.

P1Message == [type : {"P1"},
             replica : Replicas,
             instance : Nat,
             cmd : Commands \cup {none},
             deps : DepsType]

\* P2 models the (broadcast) PreAcceptReply path (simplified):
P2Message == [type : {"P2"},
             replica : Replicas,      \* cmdLeader in Go code (pareply.Replica)
             instance : Nat,
             sender : Replicas,       \* who sent this reply
             deps : DepsType]

Message == P1Message \union P2Message

ASSUME EPaxosAssume ==
    /\ IsFiniteSet(Replicas)
    /\ IsFiniteSet(Commands)
    /\ none \notin Commands
    /\ \A q \in Quorums : q \subseteq Replicas
    /\ \A q \in Quorums : Cardinality(Replicas) \div 2 < Cardinality(q)
    /\ \A q, r \in Quorums : q \intersect r # {}

Max(a, b) == IF a >= b THEN a ELSE b

\* Get the max instance number for a replica from a deps set
\* Returns -1 if no dependency on that replica
MaxDepFor(deps, r) ==
    LET nums == {i \in 1..Cardinality(Commands) : <<r, i>> \in deps}
    IN IF nums = {} THEN -1
       ELSE CHOOSE max \in nums : \A i \in nums : i <= max

\* Merge two dependency sets by union
MergeDeps(deps1, deps2) == deps1 \union deps2

\* ============================================================================
\* cmdLog access helpers (sparse storage)
\* ============================================================================
 
\* Get deps for an instance from cmdLog, returns {} if not found
GetDeps(obs, ldr, i) ==
    LET recs == {rec \in cmdLog[obs] : rec.inst = <<ldr, i>>}
    IN IF recs = {} THEN {}
       ELSE (CHOOSE rec \in recs : TRUE).deps

\* Update or add a record in cmdLog
UpdateCmdLog(log, inst, newDeps) ==
    LET oldRecs == {rec \in log : rec.inst = inst}
    IN (log \ oldRecs) \union {[inst |-> inst, deps |-> newDeps]}

\* ============================================================================
\* Priority-related helpers
\* ============================================================================

\* Check if an instance can be safely added to sender's deps
\* Conditions:
\*   1. inst must not depend on sender's instance (avoid cycle!)
\*   2. inst must not depend on lower-priority instances not in sender's deps
CanAdopt(me, inst, sender, senderInst, senderDeps) ==
    LET instDeps == GetDeps(me, me, inst)
    IN \* Condition 1: <<me, inst>> must NOT depend on <<sender, senderInst>>
       <<sender, senderInst>> \notin instDeps
       \* Condition 2: original check
       /\ \A <<k, j>> \in instDeps :
           k <= sender \/ <<k, j>> \in senderDeps

\* UpdatePriority: Add my new instances to sender's deps
\* "New" = instances I have that sender doesn't know about (not in senderDeps)
\*
\* For each new instance, check CanAdopt before adding
UpdatePriority(me, sender, senderInst, senderDeps, currentDeps) ==
    \* Precondition (enforced by caller): sender > me
    LET \* My instances that sender doesn't know about
        myKnownMax == crtInstance[me]
        senderKnowsMax == MaxDepFor(senderDeps, me)
        \* New instances: (senderKnowsMax, myKnownMax]
        newInsts == {i \in 1..myKnownMax : i > senderKnowsMax}
        \* Filter: only add instances that satisfy CanAdopt
        safeToAdd == {i \in newInsts : CanAdopt(me, i, sender, senderInst, senderDeps)}
    IN currentDeps \union {<<me, i>> : i \in safeToAdd}
 
\* ============================================================================
\* Message-based quorum checking
\* ============================================================================

P2Replies(cmdLeader, inst) ==
    {m \in messages : m.type = "P2" /\ m.replica = cmdLeader /\ m.instance = inst}

QuorumReached(cmdLeader, inst) ==
    LET repliedReplicas == {m.sender : m \in P2Replies(cmdLeader, inst)} \union {cmdLeader}
    IN Cardinality(repliedReplicas) > Cardinality(Replicas) \div 2

PriorityOK(cmdLeader, inst) ==
    LET repliedReplicas == {m.sender : m \in P2Replies(cmdLeader, inst)} \union {cmdLeader}
    IN \A r \in Replicas : (r < cmdLeader) => r \in repliedReplicas

IsCommitted(inst) ==
    committed[inst] # {}

SendMessage(m) == messages' = messages \union {m}

\* ============================================================================
\* EPaxos Actions
\* ============================================================================

\* EPaxos P1: Propose / start new instance (like epaxos.tla Propose)
\* Each command can only be proposed once (C \in Commands \ proposed)
EPaxosP1 ==
    /\ UNCHANGED<<committed>>
    /\ \E replica \in Replicas, C \in (Commands \ proposed) :
        LET instance == crtInstance[replica] + 1
            inst == <<replica, instance>>
            \* All known instances + their deps (transitive closure)
            deps == {rec.inst : rec \in cmdLog[replica]} 
                    \union UNION {rec.deps : rec \in cmdLog[replica]}
        IN /\ instance \in 1..Cardinality(Commands)
           /\ proposed' = proposed \cup {C}
           /\ crtInstance' = [crtInstance EXCEPT ![replica] = instance]
           /\ cmdLog' = [cmdLog EXCEPT ![replica] = UpdateCmdLog(@, inst, deps)]
           /\ SendMessage([type |-> "P1",
                           replica |-> replica,
                           instance |-> instance,
                           cmd |-> C,
                           deps |-> deps])

\* When a replica receives a P1 message (like handlePreAcceptReply with Command)
EPaxosReceiveP1 ==
    /\ UNCHANGED<<crtInstance, committed, proposed>>
    /\ \E receiver \in Replicas, m \in {msg \in messages : msg.type = "P1"} :
        LET inst == <<m.replica, m.instance>>
            oldDeps == GetDeps(receiver, m.replica, m.instance)
            \* Use m.deps if first time, otherwise keep existing (P1 has minimal deps)
            baseDeps == IF oldDeps = {} THEN m.deps ELSE oldDeps
            \* Add receiver's instances to deps if sender has lower priority
            newDeps == IF m.replica > receiver
                       THEN UpdatePriority(receiver, m.replica, m.instance, m.deps, baseDeps)
                       ELSE baseDeps
        IN /\ receiver # m.replica
           /\ cmdLog' = [cmdLog EXCEPT ![receiver] = UpdateCmdLog(@, inst, newDeps)]
           /\ SendMessage([type |-> "P2",
                           replica |-> m.replica,
                           instance |-> m.instance,
                           sender |-> receiver,
                           deps |-> newDeps])

\* When a replica receives a P2 message (forwarded PreAcceptReply)
\* Always merge deps for consistency
EPaxosReceiveP2 ==
    /\ UNCHANGED<<messages, crtInstance, committed, proposed>>
    /\ \E receiver \in Replicas, m \in {msg \in messages : msg.type = "P2"} :
        LET inst == <<m.replica, m.instance>>
            old == GetDeps(receiver, m.replica, m.instance)
            updated == MergeDeps(old, m.deps)
        IN cmdLog' = [cmdLog EXCEPT ![receiver] = UpdateCmdLog(@, inst, updated)]

\* EPaxos commit (like epaxos.tla Phase1Fast/Phase2Finalize commit part)
EPaxosCommit ==
    /\ UNCHANGED<<messages, crtInstance, cmdLog, proposed>>
    /\ \E observer \in Replicas, cmdLeader \in Replicas, instNum \in 1..Cardinality(Commands) :
        LET inst == <<cmdLeader, instNum>>
            deps == GetDeps(observer, cmdLeader, instNum)
        IN /\ ~IsCommitted(inst)  \* Not yet committed
           /\ deps # {}  \* Instance must exist
           /\ QuorumReached(cmdLeader, instNum)
           /\ PriorityOK(cmdLeader, instNum)
           /\ committed' = [committed EXCEPT ![inst] = @ \union {deps}]
 
\* ============================================================================
\* Specification
\* ============================================================================

vars == <<messages, crtInstance, cmdLog, committed, proposed>>

TypeOK == /\ messages \subseteq Message
          /\ crtInstance \in [Replicas -> Int]
          /\ Replicas \subseteq Int
          /\ cmdLog \in [Replicas -> SUBSET CmdLogRecord]
          /\ committed \in [Instances -> SUBSET CommittedValue]
          /\ proposed \in SUBSET Commands

Init == /\ messages = {} 
        /\ crtInstance = [r \in Replicas |-> 0]
        /\ cmdLog = [r \in Replicas |-> {}]  \* Empty sets - sparse!
        /\ committed = [inst \in Instances |-> {}]
        /\ proposed = {}  \* No commands proposed yet

Next == \/ EPaxosP1
        \/ EPaxosReceiveP1
        \/ EPaxosReceiveP2
        \/ EPaxosCommit

Spec == Init /\ [][Next]_vars

\* ============================================================================
\* Safety Properties
\* ============================================================================

\* EPaxos Consistency: At most one deps set can be committed per instance
Consistency ==
    \A inst \in Instances :
        Cardinality(committed[inst]) <= 1

\* Build dependency graph from all committed instances
\* Edge <<i, j>> means instance i depends on instance j
DependencyGraph ==
    UNION {UNION {{<<i, j>> : j \in deps} : deps \in committed[i]} : i \in Instances}

\* Compose two relations: {<<a,c>> : exists b s.t. <<a,b>> in R1 and <<b,c>> in R2}
Compose(R1, R2) ==
    LET Pairs(a) == {<<a, c>> : c \in {y \in Instances : \E b \in Instances : <<a, b>> \in R1 /\ <<b, y>> \in R2}}
    IN UNION {Pairs(a) : a \in Instances}

\* Compute transitive closure iteratively (fixed point)
\* TC contains all pairs <<a, b>> where there's a path from a to b
TC == LET Step1 == DependencyGraph
           Step2 == Step1 \union Compose(Step1, Step1)
           Step3 == Step2 \union Compose(Step2, Step2)
           \* For small models (9 instances), 4 iterations is enough for transitive closure
       IN Step3

\* NoCycles: No instance depends on itself (directly or transitively)
\* If <<i, i>> \in TC, there's a cycle involving i
NoCycles ==
    \A i \in Instances : <<i, i>> \notin TC

\* Define symmetry for faster computations.
\* Note: Replicas are integers in this model, so we only permute Commands (model values).
Symmetry == Permutations(Commands)

===============================================================
