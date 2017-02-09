package health

import (
	"encoding/json"
	"os"
	"time"

	"github.com/nats-io/go-nats"
)

// AgentLoc conveys to interested parties
// the Id and location of one gnatsd
// server in the cluster.
type AgentLoc struct {
	ID   string `json:"serverId"`
	Host string `json:"host"`
	Port int    `json:"port"`

	// Are we the leader?
	IsLeader bool `json:"leader"`

	// LeaseExpires is zero for any
	// non-leader. For the leader,
	// LeaseExpires tells you when
	// the leaders lease expires.
	LeaseExpires time.Time `json:"leaseExpires"`

	// lower rank is leader until lease
	// expires. Ties are broken by ID.
	// Rank should be assignable on the
	// gnatsd command line with -rank to
	// let the operator prioritize
	// leadership for certain hosts.
	Rank int `json:"rank"`

	// Pid or process id is the only
	// way to tell apart two processes
	// sometimes, if they share the
	// same nats server.
	//
	// Pid is the one difference between
	// a nats.ServerLoc and a health.AgentLoc.
	//
	Pid int `json:"pid"`
}

func (s *AgentLoc) String() string {
	by, err := json.Marshal(s)
	panicOn(err)
	return string(by)
}

func (s *AgentLoc) fromBytes(by []byte) error {
	return json.Unmarshal(by, s)
}

func alocEqual(a, b *AgentLoc) bool {
	aless := AgentLocLessThan(a, b)
	bless := AgentLocLessThan(b, a)
	return !aless && !bless
}

func slocEqualIgnoreLease(a, b *AgentLoc) bool {
	a0 := *a
	b0 := *b
	a0.LeaseExpires = time.Time{}
	a0.IsLeader = false
	b0.LeaseExpires = time.Time{}
	b0.IsLeader = false

	aless := AgentLocLessThan(&a0, &b0)
	bless := AgentLocLessThan(&b0, &a0)
	return !aless && !bless
}

// the 2 types should be kept in sync.
// We return a brand new &AgentLoc{}
// with contents filled from loc.
func natsLocConvert(loc *nats.ServerLoc) *AgentLoc {
	return &AgentLoc{
		ID:           loc.ID,
		Host:         loc.Host,
		Port:         loc.Port,
		IsLeader:     loc.IsLeader,
		LeaseExpires: loc.LeaseExpires,
		Rank:         loc.Rank,
		Pid:          os.Getpid(),
	}
}
