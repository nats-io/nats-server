package health

import (
	"log"
	"net"
	"time"

	"github.com/nats-io/gnatsd/logger"
	"github.com/nats-io/gnatsd/server"
)

// deafTrue means the node will won't ping or pong.
const deafTrue = 1

// deafFalse means there is no simulated network
// parition, and pings/pings proceed normally.
const deafFalse = 0

// MembershipCfg configures the
// Membership service, which is
// the health-agent implementation.
type MembershipCfg struct {

	// max we allow for clocks to be out of sync.
	// default to 1 second if not set.
	MaxClockSkew time.Duration

	// how often we heartbeat. defaults to 100msec
	// if not set.
	BeatDur time.Duration

	// NatsURL example "nats://127.0.0.1:4222"
	NatsURL string

	// defaults to "_nats.cluster.members."
	SysMemberPrefix string

	// LeaseTime is the minimum time the
	// leader is elected for. Defaults to 10 sec.
	LeaseTime time.Duration

	// provide a default until the server gives us rank
	MyRank int

	// optional, if provided we will use this connection on
	// the client side.
	CliConn net.Conn

	// where we log stuff.
	Log server.Logger

	// for testing under network partition
	deaf int64

	// how much history to save
	historyCount int
}

// SetDefaults fills in default values.
func (cfg *MembershipCfg) SetDefaults() {
	if cfg.LeaseTime == 0 {
		cfg.LeaseTime = time.Second * 12
	}
	if cfg.SysMemberPrefix == "" {
		cfg.SysMemberPrefix = "_nats.cluster.members."
	}
	if cfg.BeatDur == 0 {
		cfg.BeatDur = 3000 * time.Millisecond
	}
	if cfg.MaxClockSkew == 0 {
		cfg.MaxClockSkew = time.Second
	}
	if cfg.NatsURL == "" {
		cfg.NatsURL = "nats://127.0.0.1:4222"
	}
	if cfg.Log == nil {
		// stderr
		cfg.Log = logger.NewStdLogger(micros, debug, trace, colors, pid, log.LUTC)
	}
}

// constants controlling log levels:

const colors = false
const micros, pid = true, true
const trace = false

//const debug = true
const debug = false

// Dial allows us to replace a client's dial of
// an external TCP socket with an already established
// internal TCP connection.
func (cfg *MembershipCfg) Dial(network, address string) (net.Conn, error) {
	return cfg.CliConn, nil
}
