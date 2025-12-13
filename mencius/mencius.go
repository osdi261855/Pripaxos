package mencius

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/imdea-software/swiftpaxos/config"
	"github.com/imdea-software/swiftpaxos/dlog"
	"github.com/imdea-software/swiftpaxos/replica"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
	"github.com/imdea-software/swiftpaxos/state"
)

const TRUE = uint8(1)
const FALSE = uint8(0)

type InstanceStatus int

const (
	PREPARING InstanceStatus = iota
	ACCEPTED
	READY
	COMMITTED
	EXECUTED
)

type Instance struct {
	skipped       bool
	nbInstSkipped int
	command       *state.Command
	ballot        int32
	status        InstanceStatus
	lb            *LeaderBookkeeping
}

type LeaderBookkeeping struct {
	clientProposal *defs.GPropose
	maxRecvBallot  int32
	prepareOKs     int
	acceptOKs      int
	nacks          int
}

type Replica struct {
	*replica.Replica
	skipChan            chan fastrpc.Serializable
	prepareChan         chan fastrpc.Serializable
	acceptChan          chan fastrpc.Serializable
	commitChan          chan fastrpc.Serializable
	prepareReplyChan    chan fastrpc.Serializable
	acceptReplyChan     chan fastrpc.Serializable
	delayedSkipChan     chan *DelayedSkip
	skipRPC             uint8
	prepareRPC          uint8
	acceptRPC           uint8
	commitRPC           uint8
	prepareReplyRPC     uint8
	acceptReplyRPC      uint8
	clockChan           chan bool
	instanceSpace       []*Instance
	crtInstance         int32
	latestInstReady     int32
	latestInstCommitted int32
	blockingInstance    int32
	Shutdown            bool
}

type DelayedSkip struct{ skipEnd int32 }

func New(alias string, id int, peerAddrList []string, exec bool, durable bool, f int, conf *config.Config, logger *dlog.Logger) *Replica {
	r := &Replica{
		Replica:             replica.New(alias, id, f, peerAddrList, conf.Thrifty, exec, false, conf, logger),
		skipChan:            make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		prepareChan:         make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		acceptChan:          make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		commitChan:          make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		prepareReplyChan:    make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		acceptReplyChan:     make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		delayedSkipChan:     make(chan *DelayedSkip, defs.CHAN_BUFFER_SIZE),
		clockChan:           make(chan bool, 10),
		instanceSpace:       make([]*Instance, 10*1024*1024),
		crtInstance:         int32(id),
		latestInstReady:     int32(-1),
		latestInstCommitted: int32(0),
		blockingInstance:    int32(0),
		Shutdown:            false,
	}
	r.Durable = durable

	r.skipRPC = r.RPC.Register(new(Skip), r.skipChan)
	r.prepareRPC = r.RPC.Register(new(Prepare), r.prepareChan)
	r.acceptRPC = r.RPC.Register(new(Accept), r.acceptChan)
	r.commitRPC = r.RPC.Register(new(Commit), r.commitChan)
	r.prepareReplyRPC = r.RPC.Register(new(PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RPC.Register(new(AcceptReply), r.acceptReplyChan)

	if logger != nil {
		logger.Printf("mencius.New(alias=%s, id=%d) peers=%d exec=%v durable=%v thrifty=%v", alias, id, len(peerAddrList), exec, durable, conf.Thrifty)
	}
	go r.run()
	return r
}

func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}
	var b [10]byte
	if inst.skipped {
		b[0] = 1
	} else {
		b[0] = 0
	}
	binary.LittleEndian.PutUint32(b[1:5], uint32(inst.nbInstSkipped))
	binary.LittleEndian.PutUint32(b[5:9], uint32(inst.ballot))
	b[9] = byte(inst.status)
	r.StableStore.Write(b[:])
}

func (r *Replica) recordCommand(cmd *state.Command) {
	if !r.Durable || cmd == nil {
		return
	}
	cmd.Marshal(io.Writer(r.StableStore))
}

func (r *Replica) sync() {
	if r.Durable {
		r.StableStore.Sync()
	}
}

func (r *Replica) run() {
	r.Printf("mencius[%d] starting peer connections", r.Id)
	r.ConnectToPeers()
	r.Printf("mencius[%d] peers connected", r.Id)
	r.ComputeClosestPeers()
	r.Printf("mencius[%d] computed closest peers", r.Id)
	if r.Exec {
		r.Printf("mencius[%d] exec enabled, starting executor", r.Id)
		go r.executeCommands()
	}
	r.Printf("mencius[%d] starting clock/client listeners", r.Id)
	go r.clock()
	go r.WaitForClientConnections()

	r.Printf("mencius[%d] entering select loop", r.Id)
	for !r.Shutdown {
		select {
		case skipS := <-r.skipChan:
			r.PrintDebug("mencius skip event", "instance", skipS.(*Skip).StartInstance)
			r.handleSkip(skipS.(*Skip))
		case prepareS := <-r.prepareChan:
			r.PrintDebug("mencius prepare event", "instance", prepareS.(*Prepare).Instance)
			r.handlePrepare(prepareS.(*Prepare))
		case acceptS := <-r.acceptChan:
			r.PrintDebug("mencius accept event", "instance", acceptS.(*Accept).Instance)
			r.handleAccept(acceptS.(*Accept))
		case commitS := <-r.commitChan:
			r.PrintDebug("mencius commit event", "instance", commitS.(*Commit).Instance)
			r.handleCommit(commitS.(*Commit))
		case preplyS := <-r.prepareReplyChan:
			r.PrintDebug("mencius prepareReply event", "instance", preplyS.(*PrepareReply).Instance)
			r.handlePrepareReply(preplyS.(*PrepareReply))
		case areplyS := <-r.acceptReplyChan:
			r.PrintDebug("mencius acceptReply event", "instance", areplyS.(*AcceptReply).Instance)
			r.handleAcceptReply(areplyS.(*AcceptReply))
		case <-r.clockChan:
			// placeholder for periodic actions
		case propose := <-r.ProposeChan:
			r.PrintDebug("mencius propose event", "clientId", propose.ClientId, "cmdId", propose.CommandId)
			r.handlePropose(propose)
		}
	}
}

func (r *Replica) clock() {
	for !r.Shutdown {
		time.Sleep(100 * time.Millisecond)
		r.clockChan <- true
	}
}

func (r *Replica) replyPrepare(replicaId int32, reply *PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}
func (r *Replica) replyAccept(replicaId int32, reply *AcceptReply) {
	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

func (r *Replica) handlePropose(propose *defs.GPropose) {
	instNo := r.crtInstance
	r.PrintDebug("mencius handlePropose", "instance", instNo, "clientId", propose.ClientId, "cmdId", propose.CommandId)
	r.crtInstance += int32(r.N)
	r.instanceSpace[instNo] = &Instance{false, 0, &propose.Command, replica.NextBallotOf(r.Id, 0, r.N), ACCEPTED, &LeaderBookkeeping{clientProposal: propose, maxRecvBallot: 0, prepareOKs: 0, acceptOKs: 0, nacks: 0}}
	r.recordInstanceMetadata(r.instanceSpace[instNo])
	r.recordCommand(&propose.Command)
	r.sync()
	r.bcastAccept(instNo, r.instanceSpace[instNo].ballot, FALSE, 0, propose.Command)
}

func (r *Replica) bcastAccept(instance int32, ballot int32, skip uint8, nbInstToSkip int32, command state.Command) {
	args := &Accept{LeaderId: r.Id, Instance: instance, Ballot: ballot, Skip: skip, NbInstancesToSkip: nbInstToSkip, Command: command}
	for q := int32(0); q < int32(r.N); q++ {
		if q == r.Id || !r.Alive[q] {
			continue
		}
		r.SendMsg(q, r.acceptRPC, args)
	}
}

func (r *Replica) bcastCommit(commit *Commit) {
	for q := int32(0); q < int32(r.N); q++ {
		if q == r.Id || !r.Alive[q] {
			continue
		}
		r.SendMsg(q, r.commitRPC, commit)
	}
}

func (r *Replica) finalizeCommit(instance int32, inst *Instance) {
	if inst == nil || inst.status == COMMITTED || inst.status == EXECUTED {
		return
	}
	inst.status = COMMITTED
	r.recordInstanceMetadata(inst)

	commit := &Commit{
		LeaderId:          r.Id,
		Instance:          instance,
		Skip:              FALSE,
		NbInstancesToSkip: int32(inst.nbInstSkipped),
	}
	if inst.skipped {
		commit.Skip = TRUE
	}
	r.bcastCommit(commit)
}

func (r *Replica) handleSkip(skip *Skip) {
	r.instanceSpace[skip.StartInstance] = &Instance{true, int(skip.EndInstance-skip.StartInstance)/r.N + 1, nil, 0, COMMITTED, nil}
}

func (r *Replica) handlePrepare(prepare *Prepare) {
	inst := r.instanceSpace[prepare.Instance]
	if inst == nil {
		r.replyPrepare(prepare.LeaderId, &PrepareReply{Instance: prepare.Instance, OK: TRUE, Ballot: -1, Skip: FALSE, NbInstancesToSkip: 0, Command: state.Command{Op: state.NONE, K: 0, V: state.NIL()}})
		r.instanceSpace[prepare.Instance] = &Instance{false, 0, nil, prepare.Ballot, PREPARING, nil}
		return
	}
	ok := TRUE
	if prepare.Ballot < inst.ballot {
		ok = FALSE
	}
	if inst.command == nil {
		inst.command = &state.Command{Op: state.NONE, K: 0, V: state.NIL()}
	}
	skipped := FALSE
	if inst.skipped {
		skipped = TRUE
	}
	r.replyPrepare(prepare.LeaderId, &PrepareReply{Instance: prepare.Instance, OK: ok, Ballot: inst.ballot, Skip: skipped, NbInstancesToSkip: int32(inst.nbInstSkipped), Command: *inst.command})
}

func (r *Replica) handleAccept(accept *Accept) {
	inst := r.instanceSpace[accept.Instance]
	if inst != nil && inst.ballot > accept.Ballot {
		r.replyAccept(accept.LeaderId, &AcceptReply{Instance: accept.Instance, OK: FALSE, Ballot: inst.ballot, SkippedStartInstance: -1, SkippedEndInstance: -1})
		return
	}
	if inst == nil {
		skip := accept.Skip == TRUE
		r.instanceSpace[accept.Instance] = &Instance{skipped: skip, nbInstSkipped: int(accept.NbInstancesToSkip), command: &accept.Command, ballot: accept.Ballot, status: ACCEPTED, lb: nil}
		r.recordInstanceMetadata(r.instanceSpace[accept.Instance])
		r.recordCommand(&accept.Command)
		r.sync()
		r.replyAccept(accept.LeaderId, &AcceptReply{Instance: accept.Instance, OK: TRUE, Ballot: -1, SkippedStartInstance: -1, SkippedEndInstance: -1})
		return
	}
	if inst.status == COMMITTED || inst.status == EXECUTED {
		if inst.command == nil {
			inst.command = &accept.Command
		}
	} else {
		inst.command = &accept.Command
		inst.ballot = accept.Ballot
		inst.status = ACCEPTED
		inst.skipped = accept.Skip == TRUE
		inst.nbInstSkipped = int(accept.NbInstancesToSkip)
		r.recordInstanceMetadata(inst)
		r.replyAccept(accept.LeaderId, &AcceptReply{Instance: accept.Instance, OK: TRUE, Ballot: inst.ballot, SkippedStartInstance: -1, SkippedEndInstance: -1})
	}
}

func (r *Replica) handleCommit(commit *Commit) {
	inst := r.instanceSpace[commit.Instance]
	if inst == nil {
		r.instanceSpace[commit.Instance] = &Instance{skipped: commit.Skip == TRUE, nbInstSkipped: int(commit.NbInstancesToSkip), command: nil, ballot: 0, status: COMMITTED, lb: nil}
	} else {
		inst.status = COMMITTED
		inst.skipped = commit.Skip == TRUE
		inst.nbInstSkipped = int(commit.NbInstancesToSkip)
		if inst.lb != nil && inst.lb.clientProposal != nil {
			r.ProposeChan <- inst.lb.clientProposal
			inst.lb.clientProposal = nil
		}
	}
	r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
}

func (r *Replica) handlePrepareReply(preply *PrepareReply) {
	inst := r.instanceSpace[preply.Instance]
	if inst == nil || inst.status != PREPARING {
		return
	}
	if preply.OK == TRUE {
		inst.lb.prepareOKs++
		if preply.Ballot > inst.lb.maxRecvBallot {
			inst.command = &preply.Command
			inst.skipped = preply.Skip == TRUE
			inst.nbInstSkipped = int(preply.NbInstancesToSkip)
			inst.lb.maxRecvBallot = preply.Ballot
		}
		if inst.lb.prepareOKs+1 > r.N>>1 {
			inst.status = ACCEPTED
			inst.lb.nacks = 0
			skip := FALSE
			if inst.skipped {
				skip = TRUE
			}
			r.bcastAccept(preply.Instance, inst.ballot, skip, int32(inst.nbInstSkipped), *inst.command)
		}
	} else {
		inst.lb.nacks++
		if preply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = preply.Ballot
		}
		if inst.lb.nacks >= r.N>>1 && inst.lb != nil {
			inst.ballot = replica.NextBallotOf(r.Id, inst.lb.maxRecvBallot, r.N)
			// broadcast prepare
			args := &Prepare{LeaderId: r.Id, Instance: preply.Instance, Ballot: inst.ballot}
			for q := int32(0); q < int32(r.N); q++ {
				if q == r.Id || !r.Alive[q] {
					continue
				}
				r.SendMsg(q, r.prepareRPC, args)
			}
		}
	}
}

func (r *Replica) handleAcceptReply(areply *AcceptReply) {
	inst := r.instanceSpace[areply.Instance]
	if inst == nil || inst.lb == nil {
		return
	}
	if areply.OK == TRUE {
		inst.lb.acceptOKs++
		if inst.status != COMMITTED && inst.status != EXECUTED && inst.lb.acceptOKs+1 > r.N>>1 {
			r.PrintDebug("mencius quorum reached", "instance", areply.Instance, "acceptOKs", inst.lb.acceptOKs+1)
			r.finalizeCommit(areply.Instance, inst)
		}
	} else {
		inst.lb.nacks++
		if areply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = areply.Ballot
		}
		if inst.lb.nacks >= r.N>>1 {
			if inst.lb.clientProposal != nil {
				inst.ballot = replica.NextBallotOf(r.Id, inst.lb.maxRecvBallot, r.N)
			}
		}
	}
}

func (r *Replica) executeCommands() {
	execedUpTo := int32(-1)
	for !r.Shutdown {
		executed := false
		for i := execedUpTo + 1; i < r.crtInstance; i++ {
			inst := r.instanceSpace[i]
			if inst == nil || inst.status != COMMITTED || inst.skipped {
				break
			}
			if inst.command == nil {
				time.Sleep(time.Millisecond)
				continue
			}
			if r.Dreply && inst.lb != nil && inst.lb.clientProposal != nil {
				val := inst.command.Execute(r.State)
				r.ReplyProposeTS(&defs.ProposeReplyTS{OK: defs.TRUE, CommandId: inst.lb.clientProposal.CommandId, Value: val, Timestamp: inst.lb.clientProposal.Timestamp}, inst.lb.clientProposal.Reply, inst.lb.clientProposal.Mutex)
			} else if inst.command.Op == state.PUT {
				inst.command.Execute(r.State)
			}
			inst.status = EXECUTED
			executed = true
			execedUpTo = i
		}
		if !executed {
			time.Sleep(time.Millisecond)
		}
	}
}
