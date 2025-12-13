package mencius

import (
	"io"
	"sync"

	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
	"github.com/imdea-software/swiftpaxos/state"
)

// Protocol message types (ported from epaxos/src/menciusproto)

type Skip struct {
	LeaderId      int32
	StartInstance int32
	EndInstance   int32
}

type Prepare struct {
	LeaderId int32
	Instance int32
	Ballot   int32
}

type PrepareReply struct {
	Instance          int32
	OK                uint8
	Ballot            int32
	Skip              uint8
	NbInstancesToSkip int32
	Command           state.Command
}

type Accept struct {
	LeaderId          int32
	Instance          int32
	Ballot            int32
	Skip              uint8
	NbInstancesToSkip int32
	Command           state.Command
}

type AcceptReply struct {
	Instance             int32
	OK                   uint8
	Ballot               int32
	SkippedStartInstance int32
	SkippedEndInstance   int32
}

type Commit struct {
	LeaderId          int32
	Instance          int32
	Skip              uint8
	NbInstancesToSkip int32
}

// Implement rpc.Serializable interfaces and marshalers

func (t *Skip) New() fastrpc.Serializable         { return new(Skip) }
func (t *Prepare) New() fastrpc.Serializable      { return new(Prepare) }
func (t *PrepareReply) New() fastrpc.Serializable { return new(PrepareReply) }
func (t *Accept) New() fastrpc.Serializable       { return new(Accept) }
func (t *AcceptReply) New() fastrpc.Serializable  { return new(AcceptReply) }
func (t *Commit) New() fastrpc.Serializable       { return new(Commit) }

// Caches (copied from generated code)
type SkipCache struct {
	mu    sync.Mutex
	cache []*Skip
}
type PrepareCache struct {
	mu    sync.Mutex
	cache []*Prepare
}
type PrepareReplyCache struct {
	mu    sync.Mutex
	cache []*PrepareReply
}
type AcceptCache struct {
	mu    sync.Mutex
	cache []*Accept
}
type AcceptReplyCache struct {
	mu    sync.Mutex
	cache []*AcceptReply
}
type CommitCache struct {
	mu    sync.Mutex
	cache []*Commit
}

func NewSkipCache() *SkipCache                 { return &SkipCache{} }
func NewPrepareCache() *PrepareCache           { return &PrepareCache{} }
func NewPrepareReplyCache() *PrepareReplyCache { return &PrepareReplyCache{} }
func NewAcceptCache() *AcceptCache             { return &AcceptCache{} }
func NewAcceptReplyCache() *AcceptReplyCache   { return &AcceptReplyCache{} }
func NewCommitCache() *CommitCache             { return &CommitCache{} }

func (p *SkipCache) Get() *Skip {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.cache); n > 0 {
		t := p.cache[n-1]
		p.cache = p.cache[:n-1]
		return t
	}
	return &Skip{}
}
func (p *SkipCache) Put(t *Skip) { p.mu.Lock(); p.cache = append(p.cache, t); p.mu.Unlock() }
func (p *PrepareCache) Get() *Prepare {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.cache); n > 0 {
		t := p.cache[n-1]
		p.cache = p.cache[:n-1]
		return t
	}
	return &Prepare{}
}
func (p *PrepareCache) Put(t *Prepare) { p.mu.Lock(); p.cache = append(p.cache, t); p.mu.Unlock() }
func (p *PrepareReplyCache) Get() *PrepareReply {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.cache); n > 0 {
		t := p.cache[n-1]
		p.cache = p.cache[:n-1]
		return t
	}
	return &PrepareReply{}
}
func (p *PrepareReplyCache) Put(t *PrepareReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (p *AcceptCache) Get() *Accept {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.cache); n > 0 {
		t := p.cache[n-1]
		p.cache = p.cache[:n-1]
		return t
	}
	return &Accept{}
}
func (p *AcceptCache) Put(t *Accept) { p.mu.Lock(); p.cache = append(p.cache, t); p.mu.Unlock() }
func (p *AcceptReplyCache) Get() *AcceptReply {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.cache); n > 0 {
		t := p.cache[n-1]
		p.cache = p.cache[:n-1]
		return t
	}
	return &AcceptReply{}
}
func (p *AcceptReplyCache) Put(t *AcceptReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (p *CommitCache) Get() *Commit {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.cache); n > 0 {
		t := p.cache[n-1]
		p.cache = p.cache[:n-1]
		return t
	}
	return &Commit{}
}
func (p *CommitCache) Put(t *Commit) { p.mu.Lock(); p.cache = append(p.cache, t); p.mu.Unlock() }

// Marshal/Unmarshal methods (ported from generated code)
func (t *Skip) Marshal(wire io.Writer) {
	var b [12]byte
	bs := b[:12]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.StartInstance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.EndInstance
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	wire.Write(bs)
}
func (t *Skip) Unmarshal(wire io.Reader) error {
	var b [12]byte
	bs := b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.LeaderId = int32(uint32(bs[0]) | uint32(bs[1])<<8 | uint32(bs[2])<<16 | uint32(bs[3])<<24)
	t.StartInstance = int32(uint32(bs[4]) | uint32(bs[5])<<8 | uint32(bs[6])<<16 | uint32(bs[7])<<24)
	t.EndInstance = int32(uint32(bs[8]) | uint32(bs[9])<<8 | uint32(bs[10])<<16 | uint32(bs[11])<<24)
	return nil
}

func (t *Prepare) Marshal(wire io.Writer) {
	var b [12]byte
	bs := b[:12]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Instance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	wire.Write(bs)
}
func (t *Prepare) Unmarshal(wire io.Reader) error {
	var b [12]byte
	bs := b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.LeaderId = int32(uint32(bs[0]) | uint32(bs[1])<<8 | uint32(bs[2])<<16 | uint32(bs[3])<<24)
	t.Instance = int32(uint32(bs[4]) | uint32(bs[5])<<8 | uint32(bs[6])<<16 | uint32(bs[7])<<24)
	t.Ballot = int32(uint32(bs[8]) | uint32(bs[9])<<8 | uint32(bs[10])<<16 | uint32(bs[11])<<24)
	return nil
}

func (t *PrepareReply) Marshal(wire io.Writer) {
	var b [14]byte
	bs := b[:14]
	tmp32 := t.Instance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	bs[4] = byte(t.OK)
	tmp32 = t.Ballot
	bs[5] = byte(tmp32)
	bs[6] = byte(tmp32 >> 8)
	bs[7] = byte(tmp32 >> 16)
	bs[8] = byte(tmp32 >> 24)
	bs[9] = byte(t.Skip)
	tmp32 = t.NbInstancesToSkip
	bs[10] = byte(tmp32)
	bs[11] = byte(tmp32 >> 8)
	bs[12] = byte(tmp32 >> 16)
	bs[13] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Command.Marshal(wire)
}
func (t *PrepareReply) Unmarshal(wire io.Reader) error {
	var b [14]byte
	bs := b[:14]
	if _, err := io.ReadAtLeast(wire, bs, 14); err != nil {
		return err
	}
	t.Instance = int32(uint32(bs[0]) | uint32(bs[1])<<8 | uint32(bs[2])<<16 | uint32(bs[3])<<24)
	t.OK = uint8(bs[4])
	t.Ballot = int32(uint32(bs[5]) | uint32(bs[6])<<8 | uint32(bs[7])<<16 | uint32(bs[8])<<24)
	t.Skip = uint8(bs[9])
	t.NbInstancesToSkip = int32(uint32(bs[10]) | uint32(bs[11])<<8 | uint32(bs[12])<<16 | uint32(bs[13])<<24)
	return t.Command.Unmarshal(wire)
}

func (t *Accept) Marshal(wire io.Writer) {
	var b [17]byte
	bs := b[:17]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Instance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	bs[12] = byte(t.Skip)
	tmp32 = t.NbInstancesToSkip
	bs[13] = byte(tmp32)
	bs[14] = byte(tmp32 >> 8)
	bs[15] = byte(tmp32 >> 16)
	bs[16] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Command.Marshal(wire)
}
func (t *Accept) Unmarshal(wire io.Reader) error {
	var b [17]byte
	bs := b[:17]
	if _, err := io.ReadAtLeast(wire, bs, 17); err != nil {
		return err
	}
	t.LeaderId = int32(uint32(bs[0]) | uint32(bs[1])<<8 | uint32(bs[2])<<16 | uint32(bs[3])<<24)
	t.Instance = int32(uint32(bs[4]) | uint32(bs[5])<<8 | uint32(bs[6])<<16 | uint32(bs[7])<<24)
	t.Ballot = int32(uint32(bs[8]) | uint32(bs[9])<<8 | uint32(bs[10])<<16 | uint32(bs[11])<<24)
	t.Skip = uint8(bs[12])
	t.NbInstancesToSkip = int32(uint32(bs[13]) | uint32(bs[14])<<8 | uint32(bs[15])<<16 | uint32(bs[16])<<24)
	return t.Command.Unmarshal(wire)
}

func (t *AcceptReply) Marshal(wire io.Writer) {
	var b [17]byte
	bs := b[:17]
	tmp32 := t.Instance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	bs[4] = byte(t.OK)
	tmp32 = t.Ballot
	bs[5] = byte(tmp32)
	bs[6] = byte(tmp32 >> 8)
	bs[7] = byte(tmp32 >> 16)
	bs[8] = byte(tmp32 >> 24)
	tmp32 = t.SkippedStartInstance
	bs[9] = byte(tmp32)
	bs[10] = byte(tmp32 >> 8)
	bs[11] = byte(tmp32 >> 16)
	bs[12] = byte(tmp32 >> 24)
	tmp32 = t.SkippedEndInstance
	bs[13] = byte(tmp32)
	bs[14] = byte(tmp32 >> 8)
	bs[15] = byte(tmp32 >> 16)
	bs[16] = byte(tmp32 >> 24)
	wire.Write(bs)
}
func (t *AcceptReply) Unmarshal(wire io.Reader) error {
	var b [17]byte
	bs := b[:17]
	if _, err := io.ReadAtLeast(wire, bs, 17); err != nil {
		return err
	}
	t.Instance = int32(uint32(bs[0]) | uint32(bs[1])<<8 | uint32(bs[2])<<16 | uint32(bs[3])<<24)
	t.OK = uint8(bs[4])
	t.Ballot = int32(uint32(bs[5]) | uint32(bs[6])<<8 | uint32(bs[7])<<16 | uint32(bs[8])<<24)
	t.SkippedStartInstance = int32(uint32(bs[9]) | uint32(bs[10])<<8 | uint32(bs[11])<<16 | uint32(bs[12])<<24)
	t.SkippedEndInstance = int32(uint32(bs[13]) | uint32(bs[14])<<8 | uint32(bs[15])<<16 | uint32(bs[16])<<24)
	return nil
}

func (t *Commit) Marshal(wire io.Writer) {
	var b [13]byte
	bs := b[:13]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Instance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	bs[8] = byte(t.Skip)
	tmp32 = t.NbInstancesToSkip
	bs[9] = byte(tmp32)
	bs[10] = byte(tmp32 >> 8)
	bs[11] = byte(tmp32 >> 16)
	bs[12] = byte(tmp32 >> 24)
	wire.Write(bs)
}
func (t *Commit) Unmarshal(wire io.Reader) error {
	var b [13]byte
	bs := b[:13]
	if _, err := io.ReadAtLeast(wire, bs, 13); err != nil {
		return err
	}
	t.LeaderId = int32(uint32(bs[0]) | uint32(bs[1])<<8 | uint32(bs[2])<<16 | uint32(bs[3])<<24)
	t.Instance = int32(uint32(bs[4]) | uint32(bs[5])<<8 | uint32(bs[6])<<16 | uint32(bs[7])<<24)
	t.Skip = uint8(bs[8])
	t.NbInstancesToSkip = int32(uint32(bs[9]) | uint32(bs[10])<<8 | uint32(bs[11])<<16 | uint32(bs[12])<<24)
	return nil
}
