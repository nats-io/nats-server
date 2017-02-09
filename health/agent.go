package health

import (
	"fmt"
	"net"
	"time"

	"github.com/nats-io/gnatsd/server"
)

// Agent implements the InternalClient interface.
// It provides health status checks and
// leader election from among the candidate
// gnatsd instances in a cluster.
type Agent struct {
	opts  *server.Options
	mship *Membership
}

// NewAgent makes a new Agent.
func NewAgent(opts *server.Options) *Agent {
	return &Agent{
		opts: opts,
	}
}

// Name should identify the internal client for logging.
func (h *Agent) Name() string {
	return "health-agent"
}

// Start makes an internal
// entirely in-process client that monitors
// cluster health and manages group
// membership functions.
//
func (h *Agent) Start(
	info server.Info,
	opts server.Options,
	logger server.Logger,

) (net.Conn, error) {

	cli, srv, err := NewInternalClientPair()
	if err != nil {
		return nil, fmt.Errorf("NewInternalClientPair() returned error: %s", err)
	}

	rank := opts.HealthRank
	beat := opts.HealthBeat
	lease := opts.HealthLease

	cfg := &MembershipCfg{
		MaxClockSkew: time.Second,
		BeatDur:      beat,
		LeaseTime:    lease,
		MyRank:       rank,
		CliConn:      cli,
		Log:          logger,
	}
	h.mship = NewMembership(cfg)
	go h.mship.Start()
	return srv, nil
}

// Stop halts the background goroutine.
func (h *Agent) Stop() {
	h.mship.Stop()
}
