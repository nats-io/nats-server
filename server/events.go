// Copyright 2018-2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server/pse"
)

const (
	accLookupReqTokens = 6
	accLookupReqSubj   = "$SYS.REQ.ACCOUNT.%s.CLAIMS.LOOKUP"
	accPackReqSubj     = "$SYS.REQ.CLAIMS.PACK"
	accListReqSubj     = "$SYS.REQ.CLAIMS.LIST"
	accClaimsReqSubj   = "$SYS.REQ.CLAIMS.UPDATE"
	accDeleteReqSubj   = "$SYS.REQ.CLAIMS.DELETE"

	connectEventSubj    = "$SYS.ACCOUNT.%s.CONNECT"
	disconnectEventSubj = "$SYS.ACCOUNT.%s.DISCONNECT"
	accReqSubj          = "$SYS.REQ.ACCOUNT.%s.%s"
	// kept for backward compatibility when using http resolver
	// this overlaps with the names for events but you'd have to have the operator private key in order to succeed.
	accUpdateEventSubjOld    = "$SYS.ACCOUNT.%s.CLAIMS.UPDATE"
	accUpdateEventSubjNew    = "$SYS.REQ.ACCOUNT.%s.CLAIMS.UPDATE"
	connsRespSubj            = "$SYS._INBOX_.%s"
	accConnsEventSubjNew     = "$SYS.ACCOUNT.%s.SERVER.CONNS"
	accConnsEventSubjOld     = "$SYS.SERVER.ACCOUNT.%s.CONNS" // kept for backward compatibility
	shutdownEventSubj        = "$SYS.SERVER.%s.SHUTDOWN"
	authErrorEventSubj       = "$SYS.SERVER.%s.CLIENT.AUTH.ERR"
	serverStatsSubj          = "$SYS.SERVER.%s.STATSZ"
	serverDirectReqSubj      = "$SYS.REQ.SERVER.%s.%s"
	serverPingReqSubj        = "$SYS.REQ.SERVER.PING.%s"
	serverStatsPingReqSubj   = "$SYS.REQ.SERVER.PING" // use $SYS.REQ.SERVER.PING.STATSZ instead
	leafNodeConnectEventSubj = "$SYS.ACCOUNT.%s.LEAFNODE.CONNECT"
	remoteLatencyEventSubj   = "$SYS.LATENCY.M2.%s"
	inboxRespSubj            = "$SYS._INBOX.%s.%s"

	// FIXME(dlc) - Should account scope, even with wc for now, but later on
	// we can then shard as needed.
	accNumSubsReqSubj = "$SYS.REQ.ACCOUNT.NSUBS"

	// These are for exported debug services. These are local to this server only.
	accSubsSubj = "$SYS.DEBUG.SUBSCRIBERS"

	shutdownEventTokens = 4
	serverSubjectIndex  = 2
	accUpdateTokensNew  = 6
	accUpdateTokensOld  = 5
	accUpdateAccIdxOld  = 2

	accReqTokens   = 5
	accReqAccIndex = 3
)

// FIXME(dlc) - make configurable.
var eventsHBInterval = 30 * time.Second

// Used to send and receive messages from inside the server.
type internal struct {
	account  *Account
	client   *client
	seq      uint64
	sid      int
	servers  map[string]*serverUpdate
	sweeper  *time.Timer
	stmr     *time.Timer
	replies  map[string]msgHandler
	sendq    chan *pubMsg
	resetCh  chan struct{}
	wg       sync.WaitGroup
	sq       *sendq
	orphMax  time.Duration
	chkOrph  time.Duration
	statsz   time.Duration
	cstatsz  time.Duration
	shash    string
	inboxPre string
}

// ServerStatsMsg is sent periodically with stats updates.
type ServerStatsMsg struct {
	Server ServerInfo  `json:"server"`
	Stats  ServerStats `json:"statsz"`
}

// ConnectEventMsg is sent when a new connection is made that is part of an account.
type ConnectEventMsg struct {
	TypedEvent
	Server ServerInfo `json:"server"`
	Client ClientInfo `json:"client"`
}

// ConnectEventMsgType is the schema type for ConnectEventMsg
const ConnectEventMsgType = "io.nats.server.advisory.v1.client_connect"

// DisconnectEventMsg is sent when a new connection previously defined from a
// ConnectEventMsg is closed.
type DisconnectEventMsg struct {
	TypedEvent
	Server   ServerInfo `json:"server"`
	Client   ClientInfo `json:"client"`
	Sent     DataStats  `json:"sent"`
	Received DataStats  `json:"received"`
	Reason   string     `json:"reason"`
}

// DisconnectEventMsgType is the schema type for DisconnectEventMsg
const DisconnectEventMsgType = "io.nats.server.advisory.v1.client_disconnect"

// AccountNumConns is an event that will be sent from a server that is tracking
// a given account when the number of connections changes. It will also HB
// updates in the absence of any changes.
type AccountNumConns struct {
	TypedEvent
	Server     ServerInfo `json:"server"`
	Account    string     `json:"acc"`
	Conns      int        `json:"conns"`
	LeafNodes  int        `json:"leafnodes"`
	TotalConns int        `json:"total_conns"`
}

const AccountNumConnsMsgType = "io.nats.server.advisory.v1.account_connections"

// accNumConnsReq is sent when we are starting to track an account for the first
// time. We will request others send info to us about their local state.
type accNumConnsReq struct {
	Server  ServerInfo `json:"server"`
	Account string     `json:"acc"`
}

// ServerInfo identifies remote servers.
type ServerInfo struct {
	Name      string    `json:"name"`
	Host      string    `json:"host"`
	ID        string    `json:"id"`
	Cluster   string    `json:"cluster,omitempty"`
	Version   string    `json:"ver"`
	Seq       uint64    `json:"seq"`
	JetStream bool      `json:"jetstream"`
	Time      time.Time `json:"time"`
}

// ClientInfo is detailed information about the client forming a connection.
type ClientInfo struct {
	Start     *time.Time    `json:"start,omitempty"`
	Host      string        `json:"host,omitempty"`
	ID        uint64        `json:"id,omitempty"`
	Account   string        `json:"acc"`
	Service   string        `json:"svc,omitempty"`
	User      string        `json:"user,omitempty"`
	Name      string        `json:"name,omitempty"`
	Lang      string        `json:"lang,omitempty"`
	Version   string        `json:"ver,omitempty"`
	RTT       time.Duration `json:"rtt,omitempty"`
	Server    string        `json:"server,omitempty"`
	Cluster   string        `json:"cluster,omitempty"`
	Stop      *time.Time    `json:"stop,omitempty"`
	Jwt       string        `json:"jwt,omitempty"`
	IssuerKey string        `json:"issuer_key,omitempty"`
	NameTag   string        `json:"name_tag,omitempty"`
	Tags      jwt.TagList   `json:"tags,omitempty"`
}

// ServerStats hold various statistics that we will periodically send out.
type ServerStats struct {
	Start            time.Time      `json:"start"`
	Mem              int64          `json:"mem"`
	Cores            int            `json:"cores"`
	CPU              float64        `json:"cpu"`
	Connections      int            `json:"connections"`
	TotalConnections uint64         `json:"total_connections"`
	ActiveAccounts   int            `json:"active_accounts"`
	NumSubs          uint32         `json:"subscriptions"`
	Sent             DataStats      `json:"sent"`
	Received         DataStats      `json:"received"`
	SlowConsumers    int64          `json:"slow_consumers"`
	Routes           []*RouteStat   `json:"routes,omitempty"`
	Gateways         []*GatewayStat `json:"gateways,omitempty"`
}

// RouteStat holds route statistics.
type RouteStat struct {
	ID       uint64    `json:"rid"`
	Name     string    `json:"name,omitempty"`
	Sent     DataStats `json:"sent"`
	Received DataStats `json:"received"`
	Pending  int       `json:"pending"`
}

// GatewayStat holds gateway statistics.
type GatewayStat struct {
	ID         uint64    `json:"gwid"`
	Name       string    `json:"name"`
	Sent       DataStats `json:"sent"`
	Received   DataStats `json:"received"`
	NumInbound int       `json:"inbound_connections"`
}

// DataStats reports how may msg and bytes. Applicable for both sent and received.
type DataStats struct {
	Msgs  int64 `json:"msgs"`
	Bytes int64 `json:"bytes"`
}

// Used for internally queueing up messages that the server wants to send.
type pubMsg struct {
	c    *client
	sub  string
	rply string
	si   *ServerInfo
	msg  interface{}
	last bool
}

// Used to track server updates.
type serverUpdate struct {
	seq   uint64
	ltime time.Time
}

// TypedEvent is a event or advisory sent by the server that has nats type hints
// typically used for events that might be consumed by 3rd party event systems
type TypedEvent struct {
	Type string    `json:"type"`
	ID   string    `json:"id"`
	Time time.Time `json:"timestamp"`
}

// internalSendLoop will be responsible for serializing all messages that
// a server wants to send.
func (s *Server) internalSendLoop(wg *sync.WaitGroup) {
	defer wg.Done()

RESET:
	s.mu.Lock()
	if s.sys == nil || s.sys.sendq == nil {
		s.mu.Unlock()
		return
	}
	sysc := s.sys.client
	resetCh := s.sys.resetCh
	sendq := s.sys.sendq
	id := s.info.ID
	host := s.info.Host
	servername := s.info.Name
	seqp := &s.sys.seq
	js := s.info.JetStream
	cluster := s.info.Cluster
	if s.gateway.enabled {
		cluster = s.getGatewayName()
	}
	s.mu.Unlock()

	// Warn when internal send queue is backed up past 75%
	warnThresh := 3 * internalSendQLen / 4
	warnFreq := time.Second
	last := time.Now().Add(-warnFreq)

	for s.eventsRunning() {
		// Setup information for next message
		if len(sendq) > warnThresh && time.Since(last) >= warnFreq {
			s.Warnf("Internal system send queue > 75%%")
			last = time.Now()
		}

		select {
		case pm := <-sendq:
			if pm.si != nil {
				pm.si.Name = servername
				pm.si.Host = host
				pm.si.Cluster = cluster
				pm.si.ID = id
				pm.si.Seq = atomic.AddUint64(seqp, 1)
				pm.si.Version = VERSION
				pm.si.Time = time.Now().UTC()
				pm.si.JetStream = js
			}
			var b []byte
			if pm.msg != nil {
				switch v := pm.msg.(type) {
				case string:
					b = []byte(v)
				case []byte:
					b = v
				default:
					b, _ = json.Marshal(pm.msg)
				}
			}

			// Setup our client. If the user wants to use a non-system account use our internal
			// account scoped here so that we are not changing out accounts for the system client.
			var c *client
			if pm.c != nil {
				c = pm.c
			} else {
				c = sysc
			}

			// Grab client lock.
			c.mu.Lock()

			// Prep internal structures needed to send message.
			c.pa.subject = []byte(pm.sub)
			c.pa.size = len(b)
			c.pa.szb = []byte(strconv.FormatInt(int64(len(b)), 10))
			c.pa.reply = []byte(pm.rply)
			trace := c.trace
			c.mu.Unlock()

			// Add in NL
			b = append(b, _CRLF_...)

			if trace {
				c.traceInOp(fmt.Sprintf("PUB %s %s %d", c.pa.subject, c.pa.reply, c.pa.size), nil)
				c.traceMsg(b)
			}

			// Process like a normal inbound msg.
			c.processInboundClientMsg(b)

			// See if we are doing graceful shutdown.
			if !pm.last {
				c.flushClients(0) // Never spend time in place.
			} else {
				// For the Shutdown event, we need to send in place otherwise
				// there is a chance that the process will exit before the
				// writeLoop has a chance to send it.
				c.flushClients(time.Second)
				return
			}
		case <-resetCh:
			goto RESET
		case <-s.quitCh:
			return
		}
	}
}

// Will send a shutdown message.
func (s *Server) sendShutdownEvent() {
	s.mu.Lock()
	if s.sys == nil || s.sys.sendq == nil {
		s.mu.Unlock()
		return
	}
	subj := fmt.Sprintf(shutdownEventSubj, s.info.ID)
	sendq := s.sys.sendq
	// Stop any more messages from queueing up.
	s.sys.sendq = nil
	// Unhook all msgHandlers. Normal client cleanup will deal with subs, etc.
	s.sys.replies = nil
	s.mu.Unlock()
	// Send to the internal queue and mark as last.
	si := &ServerInfo{}
	sendq <- &pubMsg{nil, subj, _EMPTY_, si, si, true}
}

// Used to send an internal message to an arbitrary account.
func (s *Server) sendInternalAccountMsg(a *Account, subject string, msg interface{}) error {
	s.mu.Lock()
	if s.sys == nil || s.sys.sendq == nil {
		s.mu.Unlock()
		return ErrNoSysAccount
	}
	sendq := s.sys.sendq
	// Don't hold lock while placing on the channel.
	c := s.sys.client
	s.mu.Unlock()

	// Replace our client with the account's internal client.
	if a != nil {
		a.mu.Lock()
		c = a.internalClient()
		a.mu.Unlock()
	}

	sendq <- &pubMsg{c, subject, _EMPTY_, nil, msg, false}
	return nil
}

// This will queue up a message to be sent.
// Lock should not be held.
func (s *Server) sendInternalMsgLocked(sub, rply string, si *ServerInfo, msg interface{}) {
	s.mu.Lock()
	s.sendInternalMsg(sub, rply, si, msg)
	s.mu.Unlock()
}

// This will queue up a message to be sent.
// Assumes lock is held on entry.
func (s *Server) sendInternalMsg(sub, rply string, si *ServerInfo, msg interface{}) {
	if s.sys == nil || s.sys.sendq == nil {
		return
	}
	sendq := s.sys.sendq
	// Don't hold lock while placing on the channel.
	s.mu.Unlock()
	sendq <- &pubMsg{nil, sub, rply, si, msg, false}
	s.mu.Lock()
}

// Used to send internal messages from other system clients to avoid no echo issues.
func (c *client) sendInternalMsg(sub, rply string, si *ServerInfo, msg interface{}) {
	if c == nil {
		return
	}
	s := c.srv
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.sys == nil || s.sys.sendq == nil {
		return
	}
	sendq := s.sys.sendq
	// Don't hold lock while placing on the channel.
	s.mu.Unlock()

	sendq <- &pubMsg{c, sub, rply, si, msg, false}
}

// Locked version of checking if events system running. Also checks server.
func (s *Server) eventsRunning() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	er := s.running && s.eventsEnabled()
	s.mu.Unlock()
	return er
}

// EventsEnabled will report if the server has internal events enabled via
// a defined system account.
func (s *Server) EventsEnabled() bool {
	s.mu.Lock()
	ee := s.eventsEnabled()
	s.mu.Unlock()
	return ee
}

// eventsEnabled will report if events are enabled.
// Lock should be held.
func (s *Server) eventsEnabled() bool {
	return s.sys != nil && s.sys.client != nil && s.sys.account != nil
}

// TrackedRemoteServers returns how many remote servers we are tracking
// from a system events perspective.
func (s *Server) TrackedRemoteServers() int {
	s.mu.Lock()
	if !s.running || !s.eventsEnabled() {
		return -1
	}
	ns := len(s.sys.servers)
	s.mu.Unlock()
	return ns
}

// Check for orphan servers who may have gone away without notification.
// This should be wrapChk() to setup common locking.
func (s *Server) checkRemoteServers() {
	now := time.Now()
	for sid, su := range s.sys.servers {
		if now.Sub(su.ltime) > s.sys.orphMax {
			s.Debugf("Detected orphan remote server: %q", sid)
			// Simulate it going away.
			s.processRemoteServerShutdown(sid)
			delete(s.sys.servers, sid)
		}
	}
	if s.sys.sweeper != nil {
		s.sys.sweeper.Reset(s.sys.chkOrph)
	}
}

// Grab RSS and PCPU
// Server lock will be held but released.
func (s *Server) updateServerUsage(v *ServerStats) {
	s.mu.Unlock()
	defer s.mu.Lock()
	var vss int64
	pse.ProcUsage(&v.CPU, &v.Mem, &vss)
	v.Cores = numCores
}

// Generate a route stat for our statz update.
func routeStat(r *client) *RouteStat {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	rs := &RouteStat{
		ID: r.cid,
		Sent: DataStats{
			Msgs:  atomic.LoadInt64(&r.outMsgs),
			Bytes: atomic.LoadInt64(&r.outBytes),
		},
		Received: DataStats{
			Msgs:  atomic.LoadInt64(&r.inMsgs),
			Bytes: atomic.LoadInt64(&r.inBytes),
		},
		Pending: int(r.out.pb),
	}
	if r.route != nil {
		rs.Name = r.route.remoteName
	}
	r.mu.Unlock()
	return rs
}

// Actual send method for statz updates.
// Lock should be held.
func (s *Server) sendStatsz(subj string) {
	m := ServerStatsMsg{}
	s.updateServerUsage(&m.Stats)
	m.Stats.Start = s.start
	m.Stats.Connections = len(s.clients)
	m.Stats.TotalConnections = s.totalClients
	m.Stats.ActiveAccounts = int(atomic.LoadInt32(&s.activeAccounts))
	m.Stats.Received.Msgs = atomic.LoadInt64(&s.inMsgs)
	m.Stats.Received.Bytes = atomic.LoadInt64(&s.inBytes)
	m.Stats.Sent.Msgs = atomic.LoadInt64(&s.outMsgs)
	m.Stats.Sent.Bytes = atomic.LoadInt64(&s.outBytes)
	m.Stats.SlowConsumers = atomic.LoadInt64(&s.slowConsumers)
	m.Stats.NumSubs = s.numSubscriptions()

	for _, r := range s.routes {
		m.Stats.Routes = append(m.Stats.Routes, routeStat(r))
	}
	if s.gateway.enabled {
		gw := s.gateway
		gw.RLock()
		for name, c := range gw.out {
			gs := &GatewayStat{Name: name}
			c.mu.Lock()
			gs.ID = c.cid
			gs.Sent = DataStats{
				Msgs:  atomic.LoadInt64(&c.outMsgs),
				Bytes: atomic.LoadInt64(&c.outBytes),
			}
			c.mu.Unlock()
			// Gather matching inbound connections
			gs.Received = DataStats{}
			for _, c := range gw.in {
				c.mu.Lock()
				if c.gw.name == name {
					gs.Received.Msgs += atomic.LoadInt64(&c.inMsgs)
					gs.Received.Bytes += atomic.LoadInt64(&c.inBytes)
					gs.NumInbound++
				}
				c.mu.Unlock()
			}
			m.Stats.Gateways = append(m.Stats.Gateways, gs)
		}
		gw.RUnlock()
	}
	s.sendInternalMsg(subj, _EMPTY_, &m.Server, &m)
}

// Send out our statz update.
// This should be wrapChk() to setup common locking.
func (s *Server) heartbeatStatsz() {
	if s.sys.stmr != nil {
		// Increase after startup to our max.
		s.sys.cstatsz *= 4
		if s.sys.cstatsz > s.sys.statsz {
			s.sys.cstatsz = s.sys.statsz
		}
		s.sys.stmr.Reset(s.sys.cstatsz)
	}
	s.sendStatsz(fmt.Sprintf(serverStatsSubj, s.info.ID))
}

// This should be wrapChk() to setup common locking.
func (s *Server) startStatszTimer() {
	// We will start by sending out more of these and trail off to the statsz being the max.
	s.sys.cstatsz = time.Second
	// Send out the first one only after a second.
	s.sys.stmr = time.AfterFunc(s.sys.cstatsz, s.wrapChk(s.heartbeatStatsz))
}

// Start a ticker that will fire periodically and check for orphaned servers.
// This should be wrapChk() to setup common locking.
func (s *Server) startRemoteServerSweepTimer() {
	s.sys.sweeper = time.AfterFunc(s.sys.chkOrph, s.wrapChk(s.checkRemoteServers))
}

// Length of our system hash used for server targeted messages.
const sysHashLen = 8

// Computes a hash of 8 characters for the name.
func getHash(name string) []byte {
	return getHashSize(name, sysHashLen)
}

// Returns the node name for this server which is a hash of the server name.
func (s *Server) Node() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sys != nil {
		return s.sys.shash
	}
	return _EMPTY_
}

// This will setup our system wide tracking subs.
// For now we will setup one wildcard subscription to
// monitor all accounts for changes in number of connections.
// We can make this on a per account tracking basis if needed.
// Tradeoff is subscription and interest graph events vs connect and
// disconnect events, etc.
func (s *Server) initEventTracking() {
	if !s.EventsEnabled() {
		return
	}
	// Create a system hash which we use for other servers to target us specifically.
	s.sys.shash = string(getHash(s.info.Name))

	// This will be for all inbox responses.
	subject := fmt.Sprintf(inboxRespSubj, s.sys.shash, "*")
	if _, err := s.sysSubscribe(subject, s.inboxReply); err != nil {
		s.Errorf("Error setting up internal tracking: %v", err)
	}
	s.sys.inboxPre = subject
	// This is for remote updates for connection accounting.
	subject = fmt.Sprintf(accConnsEventSubjOld, "*")
	if _, err := s.sysSubscribe(subject, s.remoteConnsUpdate); err != nil {
		s.Errorf("Error setting up internal tracking for %s: %v", subject, err)
	}
	// This will be for responses for account info that we send out.
	subject = fmt.Sprintf(connsRespSubj, s.info.ID)
	if _, err := s.sysSubscribe(subject, s.remoteConnsUpdate); err != nil {
		s.Errorf("Error setting up internal tracking: %v", err)
	}
	// Listen for broad requests to respond with number of subscriptions for a given subject.
	if _, err := s.sysSubscribe(accNumSubsReqSubj, s.nsubsRequest); err != nil {
		s.Errorf("Error setting up internal tracking: %v", err)
	}
	// Listen for statsz from others.
	subject = fmt.Sprintf(serverStatsSubj, "*")
	if _, err := s.sysSubscribe(subject, s.remoteServerUpdate); err != nil {
		s.Errorf("Error setting up internal tracking: %v", err)
	}
	// Listen for all server shutdowns.
	subject = fmt.Sprintf(shutdownEventSubj, "*")
	if _, err := s.sysSubscribe(subject, s.remoteServerShutdown); err != nil {
		s.Errorf("Error setting up internal tracking: %v", err)
	}
	// Listen for account claims updates.
	subscribeToUpdate := true
	if s.accResolver != nil {
		subscribeToUpdate = !s.accResolver.IsTrackingUpdate()
	}
	if subscribeToUpdate {
		for _, sub := range []string{accUpdateEventSubjOld, accUpdateEventSubjNew} {
			if _, err := s.sysSubscribe(fmt.Sprintf(sub, "*"), s.accountClaimUpdate); err != nil {
				s.Errorf("Error setting up internal tracking: %v", err)
			}
		}
	}
	// Listen for ping messages that will be sent to all servers for statsz.
	// This subscription is kept for backwards compatibility. Got replaced by ...PING.STATZ from below
	if _, err := s.sysSubscribe(serverStatsPingReqSubj, s.statszReq); err != nil {
		s.Errorf("Error setting up internal tracking: %v", err)
	}
	monSrvc := map[string]msgHandler{
		"STATSZ": s.statszReq,
		"VARZ": func(sub *subscription, _ *client, subject, reply string, msg []byte) {
			optz := &VarzEventOptions{}
			s.zReq(reply, msg, &optz.EventFilterOptions, optz, func() (interface{}, error) { return s.Varz(&optz.VarzOptions) })
		},
		"SUBSZ": func(sub *subscription, _ *client, subject, reply string, msg []byte) {
			optz := &SubszEventOptions{}
			s.zReq(reply, msg, &optz.EventFilterOptions, optz, func() (interface{}, error) { return s.Subsz(&optz.SubszOptions) })
		},
		"CONNZ": func(sub *subscription, _ *client, subject, reply string, msg []byte) {
			optz := &ConnzEventOptions{}
			s.zReq(reply, msg, &optz.EventFilterOptions, optz, func() (interface{}, error) { return s.Connz(&optz.ConnzOptions) })
		},
		"ROUTEZ": func(sub *subscription, _ *client, subject, reply string, msg []byte) {
			optz := &RoutezEventOptions{}
			s.zReq(reply, msg, &optz.EventFilterOptions, optz, func() (interface{}, error) { return s.Routez(&optz.RoutezOptions) })
		},
		"GATEWAYZ": func(sub *subscription, _ *client, subject, reply string, msg []byte) {
			optz := &GatewayzEventOptions{}
			s.zReq(reply, msg, &optz.EventFilterOptions, optz, func() (interface{}, error) { return s.Gatewayz(&optz.GatewayzOptions) })
		},
		"LEAFZ": func(sub *subscription, _ *client, subject, reply string, msg []byte) {
			optz := &LeafzEventOptions{}
			s.zReq(reply, msg, &optz.EventFilterOptions, optz, func() (interface{}, error) { return s.Leafz(&optz.LeafzOptions) })
		},
		"ACCOUNTZ": func(sub *subscription, _ *client, subject, reply string, msg []byte) {
			optz := &AccountzEventOptions{}
			s.zReq(reply, msg, &optz.EventFilterOptions, optz, func() (interface{}, error) { return s.Accountz(&optz.AccountzOptions) })
		},
		"JSZ": func(sub *subscription, _ *client, subject, reply string, msg []byte) {
			optz := &JszEventOptions{}
			s.zReq(reply, msg, &optz.EventFilterOptions, optz, func() (interface{}, error) { return s.Jsz(&optz.JSzOptions) })
		},
	}
	for name, req := range monSrvc {
		subject = fmt.Sprintf(serverDirectReqSubj, s.info.ID, name)
		if _, err := s.sysSubscribe(subject, req); err != nil {
			s.Errorf("Error setting up internal tracking: %v", err)
		}
		subject = fmt.Sprintf(serverPingReqSubj, name)
		if _, err := s.sysSubscribe(subject, req); err != nil {
			s.Errorf("Error setting up internal tracking: %v", err)
		}
	}
	extractAccount := func(subject string) (string, error) {
		if tk := strings.Split(subject, tsep); len(tk) != accReqTokens {
			return "", fmt.Errorf("subject %q is malformed", subject)
		} else {
			return tk[accReqAccIndex], nil
		}
	}
	monAccSrvc := map[string]msgHandler{
		"SUBSZ": func(sub *subscription, _ *client, subject, reply string, msg []byte) {
			optz := &SubszEventOptions{}
			s.zReq(reply, msg, &optz.EventFilterOptions, optz, func() (interface{}, error) {
				if acc, err := extractAccount(subject); err != nil {
					return nil, err
				} else {
					optz.SubszOptions.Subscriptions = true
					optz.SubszOptions.Account = acc
					return s.Subsz(&optz.SubszOptions)
				}
			})
		},
		"CONNZ": func(sub *subscription, _ *client, subject, reply string, msg []byte) {
			optz := &ConnzEventOptions{}
			s.zReq(reply, msg, &optz.EventFilterOptions, optz, func() (interface{}, error) {
				if acc, err := extractAccount(subject); err != nil {
					return nil, err
				} else {
					optz.ConnzOptions.Account = acc
					return s.Connz(&optz.ConnzOptions)
				}
			})
		},
		"LEAFZ": func(sub *subscription, _ *client, subject, reply string, msg []byte) {
			optz := &LeafzEventOptions{}
			s.zReq(reply, msg, &optz.EventFilterOptions, optz, func() (interface{}, error) {
				if acc, err := extractAccount(subject); err != nil {
					return nil, err
				} else {
					optz.LeafzOptions.Account = acc
					return s.Leafz(&optz.LeafzOptions)
				}
			})
		},
		"JSZ": func(sub *subscription, _ *client, subject, reply string, msg []byte) {
			optz := &JszEventOptions{}
			s.zReq(reply, msg, &optz.EventFilterOptions, optz, func() (interface{}, error) {
				if acc, err := extractAccount(subject); err != nil {
					return nil, err
				} else {
					optz.Account = acc
					return s.JszAccount(&optz.JSzOptions)
				}
			})
		},
		"INFO": func(sub *subscription, _ *client, subject, reply string, msg []byte) {
			optz := &AccInfoEventOptions{}
			s.zReq(reply, msg, &optz.EventFilterOptions, optz, func() (interface{}, error) {
				if acc, err := extractAccount(subject); err != nil {
					return nil, err
				} else {
					return s.accountInfo(acc)
				}
			})
		},
		"CONNS": s.connsRequest,
	}
	for name, req := range monAccSrvc {
		if _, err := s.sysSubscribe(fmt.Sprintf(accReqSubj, "*", name), req); err != nil {
			s.Errorf("Error setting up internal tracking: %v", err)
		}
	}

	// Listen for updates when leaf nodes connect for a given account. This will
	// force any gateway connections to move to `modeInterestOnly`
	subject = fmt.Sprintf(leafNodeConnectEventSubj, "*")
	if _, err := s.sysSubscribe(subject, s.leafNodeConnected); err != nil {
		s.Errorf("Error setting up internal tracking: %v", err)
	}
	// For tracking remote latency measurements.
	subject = fmt.Sprintf(remoteLatencyEventSubj, s.sys.shash)
	if _, err := s.sysSubscribe(subject, s.remoteLatencyUpdate); err != nil {
		s.Errorf("Error setting up internal latency tracking: %v", err)
	}
	// This is for simple debugging of number of subscribers that exist in the system.
	if _, err := s.sysSubscribeInternal(accSubsSubj, s.debugSubscribers); err != nil {
		s.Errorf("Error setting up internal debug service for subscribers: %v", err)
	}
}

// add all exports a system account will need
func (s *Server) addSystemAccountExports(sacc *Account) {
	if !s.EventsEnabled() {
		return
	}
	if err := sacc.AddServiceExport(accSubsSubj, nil); err != nil {
		s.Errorf("Error adding system service export for %q: %v", accSubsSubj, err)
	}
	if s.JetStreamEnabled() {
		s.checkJetStreamExports()
	}
}

// accountClaimUpdate will receive claim updates for accounts.
func (s *Server) accountClaimUpdate(sub *subscription, _ *client, subject, resp string, msg []byte) {
	if !s.EventsEnabled() {
		return
	}
	pubKey := ""
	toks := strings.Split(subject, tsep)
	if len(toks) == accUpdateTokensNew {
		pubKey = toks[accReqAccIndex]
	} else if len(toks) == accUpdateTokensOld {
		pubKey = toks[accUpdateAccIdxOld]
	} else {
		s.Debugf("Received account claims update on bad subject %q", subject)
		return
	}
	if claim, err := jwt.DecodeAccountClaims(string(msg)); err != nil {
		respondToUpdate(s, resp, pubKey, "jwt update resulted in error", err)
	} else if claim.Subject != pubKey {
		err := errors.New("subject does not match jwt content")
		respondToUpdate(s, resp, pubKey, "jwt update resulted in error", err)
	} else if v, ok := s.accounts.Load(pubKey); !ok {
		respondToUpdate(s, resp, pubKey, "jwt update skipped", nil)
	} else if err := s.updateAccountWithClaimJWT(v.(*Account), string(msg)); err != nil {
		respondToUpdate(s, resp, pubKey, "jwt update resulted in error", err)
	} else {
		respondToUpdate(s, resp, pubKey, "jwt updated", nil)
	}
}

// processRemoteServerShutdown will update any affected accounts.
// Will update the remote count for clients.
// Lock assume held.
func (s *Server) processRemoteServerShutdown(sid string) {
	s.accounts.Range(func(k, v interface{}) bool {
		v.(*Account).removeRemoteServer(sid)
		return true
	})
	// Update any state in nodeInfo.
	s.nodeToInfo.Range(func(k, v interface{}) bool {
		si := v.(nodeInfo)
		if si.id == sid {
			si.offline = true
			s.nodeToInfo.Store(k, si)
			return false
		}
		return true
	})
}

// remoteServerShutdownEvent is called when we get an event from another server shutting down.
func (s *Server) remoteServerShutdown(sub *subscription, _ *client, subject, reply string, msg []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.eventsEnabled() {
		return
	}
	toks := strings.Split(subject, tsep)
	if len(toks) < shutdownEventTokens {
		s.Debugf("Received remote server shutdown on bad subject %q", subject)
		return
	}

	sid := toks[serverSubjectIndex]
	if su := s.sys.servers[sid]; su != nil {
		s.processRemoteServerShutdown(sid)
	}

	if len(msg) == 0 {
		return
	}

	// We have an optional serverInfo here, remove from nodeToX lookups.
	var si ServerInfo
	if err := json.Unmarshal(msg, &si); err != nil {
		s.Debugf("Received bad server info for remote server shutdown")
		return
	}
	// Additional processing here.
	node := string(getHash(si.Name))
	s.nodeToInfo.Store(node, nodeInfo{si.Name, si.Cluster, si.ID, true, true})
}

// remoteServerUpdate listens for statsz updates from other servers.
func (s *Server) remoteServerUpdate(sub *subscription, _ *client, subject, reply string, msg []byte) {
	var ssm ServerStatsMsg
	if err := json.Unmarshal(msg, &ssm); err != nil {
		s.Debugf("Received bad server info for remote server update")
		return
	}
	si := ssm.Server
	node := string(getHash(si.Name))
	if _, ok := s.nodeToInfo.Load(node); !ok {
		// Since we have not seen this one they probably have not seen us so send out our update.
		s.mu.Lock()
		s.sendStatsz(fmt.Sprintf(serverStatsSubj, s.info.ID))
		s.mu.Unlock()
	}
	s.nodeToInfo.Store(node, nodeInfo{si.Name, si.Cluster, si.ID, false, si.JetStream})
}

// updateRemoteServer is called when we have an update from a remote server.
// This allows us to track remote servers, respond to shutdown messages properly,
// make sure that messages are ordered, and allow us to prune dead servers.
// Lock should be held upon entry.
func (s *Server) updateRemoteServer(ms *ServerInfo) {
	su := s.sys.servers[ms.ID]
	if su == nil {
		s.sys.servers[ms.ID] = &serverUpdate{ms.Seq, time.Now()}
		s.processNewServer(ms)
	} else {
		// Should always be going up.
		if ms.Seq <= su.seq {
			s.Errorf("Received out of order remote server update from: %q", ms.ID)
			return
		}
		su.seq = ms.Seq
		su.ltime = time.Now()
	}
}

// processNewServer will hold any logic we want to use when we discover a new server.
// Lock should be held upon entry.
func (s *Server) processNewServer(ms *ServerInfo) {
	// Right now we only check if we have leafnode servers and if so send another
	// connect update to make sure they switch this account to interest only mode.
	s.ensureGWsInterestOnlyForLeafNodes()
	// Add to our nodeToName
	node := string(getHash(ms.Name))
	s.nodeToInfo.Store(node, nodeInfo{ms.Name, ms.Cluster, ms.ID, false, ms.JetStream})
}

// If GW is enabled on this server and there are any leaf node connections,
// this function will send a LeafNode connect system event to the super cluster
// to ensure that the GWs are in interest-only mode for this account.
// Lock should be held upon entry.
// TODO(dlc) - this will cause this account to be loaded on all servers. Need a better
// way with GW2.
func (s *Server) ensureGWsInterestOnlyForLeafNodes() {
	if !s.gateway.enabled || len(s.leafs) == 0 {
		return
	}
	sent := make(map[*Account]bool, len(s.leafs))
	for _, c := range s.leafs {
		if !sent[c.acc] {
			s.sendLeafNodeConnectMsg(c.acc.Name)
			sent[c.acc] = true
		}
	}
}

// shutdownEventing will clean up all eventing state.
func (s *Server) shutdownEventing() {
	if !s.eventsRunning() {
		return
	}

	s.mu.Lock()
	clearTimer(&s.sys.sweeper)
	clearTimer(&s.sys.stmr)
	sys := s.sys
	s.mu.Unlock()

	// We will queue up a shutdown event and wait for the
	// internal send loop to exit.
	s.sendShutdownEvent()
	sys.wg.Wait()
	close(sys.resetCh)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Whip through all accounts.
	s.accounts.Range(func(k, v interface{}) bool {
		v.(*Account).clearEventing()
		return true
	})
	// Turn everything off here.
	s.sys = nil
}

// Request for our local connection count.
func (s *Server) connsRequest(sub *subscription, _ *client, subject, reply string, msg []byte) {
	if !s.eventsRunning() {
		return
	}
	tk := strings.Split(subject, tsep)
	if len(tk) != accReqTokens {
		s.sys.client.Errorf("Bad subject account connections request message")
		return
	}
	a := tk[accReqAccIndex]
	m := accNumConnsReq{Account: a}
	if err := json.Unmarshal(msg, &m); err != nil {
		s.sys.client.Errorf("Error unmarshalling account connections request message: %v", err)
		return
	}
	if m.Account != a {
		s.sys.client.Errorf("Error unmarshalled account does not match subject")
		return
	}
	// Here we really only want to lookup the account if its local. We do not want to fetch this
	// account if we have no interest in it.
	var acc *Account
	if v, ok := s.accounts.Load(m.Account); ok {
		acc = v.(*Account)
	}
	if acc == nil {
		return
	}
	// We know this is a local connection.
	if nlc := acc.NumLocalConnections(); nlc > 0 {
		s.mu.Lock()
		s.sendAccConnsUpdate(acc, reply)
		s.mu.Unlock()
	}
}

// leafNodeConnected is an event we will receive when a leaf node for a given account connects.
func (s *Server) leafNodeConnected(sub *subscription, _ *client, subject, reply string, msg []byte) {
	m := accNumConnsReq{}
	if err := json.Unmarshal(msg, &m); err != nil {
		s.sys.client.Errorf("Error unmarshalling account connections request message: %v", err)
		return
	}

	s.mu.Lock()
	na := m.Account == "" || !s.eventsEnabled() || !s.gateway.enabled
	s.mu.Unlock()

	if na {
		return
	}

	if acc, _ := s.lookupAccount(m.Account); acc != nil {
		s.switchAccountToInterestMode(acc.Name)
	}
}

// Common filter options for system requests STATSZ VARZ SUBSZ CONNZ ROUTEZ GATEWAYZ LEAFZ
type EventFilterOptions struct {
	Name    string   `json:"server_name,omitempty"` // filter by server name
	Cluster string   `json:"cluster,omitempty"`     // filter by cluster name
	Host    string   `json:"host,omitempty"`        // filter by host name
	Tags    []string `json:"tags,omitempty"`        // filter by tags (must match all tags)
}

// StatszEventOptions are options passed to Statsz
type StatszEventOptions struct {
	// No actual options yet
	EventFilterOptions
}

// Options for account Info
type AccInfoEventOptions struct {
	// No actual options yet
	EventFilterOptions
}

// In the context of system events, ConnzEventOptions are options passed to Connz
type ConnzEventOptions struct {
	ConnzOptions
	EventFilterOptions
}

// In the context of system events, RoutezEventOptions are options passed to Routez
type RoutezEventOptions struct {
	RoutezOptions
	EventFilterOptions
}

// In the context of system events, SubzEventOptions are options passed to Subz
type SubszEventOptions struct {
	SubszOptions
	EventFilterOptions
}

// In the context of system events, VarzEventOptions are options passed to Varz
type VarzEventOptions struct {
	VarzOptions
	EventFilterOptions
}

// In the context of system events, GatewayzEventOptions are options passed to Gatewayz
type GatewayzEventOptions struct {
	GatewayzOptions
	EventFilterOptions
}

// In the context of system events, LeafzEventOptions are options passed to Leafz
type LeafzEventOptions struct {
	LeafzOptions
	EventFilterOptions
}

// In the context of system events, AccountzEventOptions are options passed to Accountz
type AccountzEventOptions struct {
	AccountzOptions
	EventFilterOptions
}

// In the context of system events, JszEventOptions are options passed to Jsz
type JszEventOptions struct {
	JSzOptions
	EventFilterOptions
}

// returns true if the request does NOT apply to this server and can be ignored.
// DO NOT hold the server lock when
func (s *Server) filterRequest(fOpts *EventFilterOptions) bool {
	if fOpts.Name != "" && !strings.Contains(s.info.Name, fOpts.Name) {
		return true
	}
	if fOpts.Host != "" && !strings.Contains(s.info.Host, fOpts.Host) {
		return true
	}
	if fOpts.Cluster != "" {
		s.mu.Lock()
		cluster := s.info.Cluster
		s.mu.Unlock()
		if !strings.Contains(cluster, fOpts.Cluster) {
			return true
		}
	}
	if len(fOpts.Tags) > 0 {
		opts := s.getOpts()
		for _, t := range fOpts.Tags {
			if !opts.Tags.Contains(t) {
				return true
			}
		}
	}
	return false
}

// statszReq is a request for us to respond with current statsz.
func (s *Server) statszReq(sub *subscription, _ *client, subject, reply string, msg []byte) {
	if !s.EventsEnabled() || reply == _EMPTY_ {
		return
	}
	opts := StatszEventOptions{}
	if len(msg) != 0 {
		if err := json.Unmarshal(msg, &opts); err != nil {
			server := &ServerInfo{}
			response := map[string]interface{}{"server": server}
			response["error"] = map[string]interface{}{
				"code":        http.StatusBadRequest,
				"description": err.Error(),
			}
			s.sendInternalMsgLocked(reply, _EMPTY_, server, response)
			return
		} else if ignore := s.filterRequest(&opts.EventFilterOptions); ignore {
			return
		}
	}
	s.mu.Lock()
	s.sendStatsz(reply)
	s.mu.Unlock()
}

var errSkipZreq = errors.New("filtered response")

func (s *Server) zReq(reply string, msg []byte, fOpts *EventFilterOptions, optz interface{}, respf func() (interface{}, error)) {
	if !s.EventsEnabled() || reply == _EMPTY_ {
		return
	}
	server := &ServerInfo{}
	response := map[string]interface{}{"server": server}
	var err error
	status := 0
	if len(msg) != 0 {
		if err = json.Unmarshal(msg, optz); err != nil {
			status = http.StatusBadRequest // status is only included on error, so record how far execution got
		} else if s.filterRequest(fOpts) {
			return
		}
	}
	if err == nil {
		response["data"], err = respf()
		if errors.Is(err, errSkipZreq) {
			return
		}
		status = http.StatusInternalServerError
	}
	if err != nil {
		response["error"] = map[string]interface{}{
			"code":        status,
			"description": err.Error(),
		}
	}
	s.sendInternalMsgLocked(reply, _EMPTY_, server, response)
}

// remoteConnsUpdate gets called when we receive a remote update from another server.
func (s *Server) remoteConnsUpdate(sub *subscription, _ *client, subject, reply string, msg []byte) {
	if !s.eventsRunning() {
		return
	}
	m := AccountNumConns{}
	if err := json.Unmarshal(msg, &m); err != nil {
		s.sys.client.Errorf("Error unmarshalling account connection event message: %v", err)
		return
	}

	// See if we have the account registered, if not drop it.
	// Make sure this does not force us to load this account here.
	var acc *Account
	if v, ok := s.accounts.Load(m.Account); ok {
		acc = v.(*Account)
	}
	// Silently ignore these if we do not have local interest in the account.
	if acc == nil {
		return
	}

	s.mu.Lock()

	// check again here if we have been shutdown.
	if !s.running || !s.eventsEnabled() {
		s.mu.Unlock()
		return
	}
	// Double check that this is not us, should never happen, so error if it does.
	if m.Server.ID == s.info.ID {
		s.sys.client.Errorf("Processing our own account connection event message: ignored")
		s.mu.Unlock()
		return
	}
	// If we are here we have interest in tracking this account. Update our accounting.
	clients := acc.updateRemoteServer(&m)
	s.updateRemoteServer(&m.Server)
	s.mu.Unlock()
	// Need to close clients outside of server lock
	for _, c := range clients {
		c.maxAccountConnExceeded()
	}
}

// Setup tracking for this account. This allows us to track global account activity.
// Lock should be held on entry.
func (s *Server) enableAccountTracking(a *Account) {
	if a == nil || !s.eventsEnabled() {
		return
	}

	// TODO(ik): Generate payload although message may not be sent.
	// May need to ensure we do so only if there is a known interest.
	// This can get complicated with gateways.

	subj := fmt.Sprintf(accReqSubj, a.Name, "CONNS")
	reply := fmt.Sprintf(connsRespSubj, s.info.ID)
	m := accNumConnsReq{Account: a.Name}
	s.sendInternalMsg(subj, reply, &m.Server, &m)
}

// Event on leaf node connect.
// Lock should NOT be held on entry.
func (s *Server) sendLeafNodeConnect(a *Account) {
	s.mu.Lock()
	// If we are not in operator mode, or do not have any gateways defined, this should also be a no-op.
	if a == nil || !s.eventsEnabled() || !s.gateway.enabled {
		s.mu.Unlock()
		return
	}
	s.sendLeafNodeConnectMsg(a.Name)
	s.mu.Unlock()

	s.switchAccountToInterestMode(a.Name)
}

// Send the leafnode connect message.
// Lock should be held.
func (s *Server) sendLeafNodeConnectMsg(accName string) {
	subj := fmt.Sprintf(leafNodeConnectEventSubj, accName)
	m := accNumConnsReq{Account: accName}
	s.sendInternalMsg(subj, "", &m.Server, &m)
}

// sendAccConnsUpdate is called to send out our information on the
// account's local connections.
// Lock should be held on entry.
func (s *Server) sendAccConnsUpdate(a *Account, subj ...string) {
	if !s.eventsEnabled() || a == nil {
		return
	}
	sendQ := s.sys.sendq
	if sendQ == nil {
		return
	}
	// Build event with account name and number of local clients and leafnodes.
	eid := s.nextEventID()
	a.mu.Lock()
	s.mu.Unlock()
	localConns := a.numLocalConnections()
	m := &AccountNumConns{
		TypedEvent: TypedEvent{
			Type: AccountNumConnsMsgType,
			ID:   eid,
			Time: time.Now().UTC(),
		},
		Account:    a.Name,
		Conns:      localConns,
		LeafNodes:  a.numLocalLeafNodes(),
		TotalConns: localConns + a.numLocalLeafNodes(),
	}
	// Set timer to fire again unless we are at zero.
	if localConns == 0 {
		clearTimer(&a.ctmr)
	} else {
		// Check to see if we have an HB running and update.
		if a.ctmr == nil {
			a.ctmr = time.AfterFunc(eventsHBInterval, func() { s.accConnsUpdate(a) })
		} else {
			a.ctmr.Reset(eventsHBInterval)
		}
	}
	for _, sub := range subj {
		msg := &pubMsg{nil, sub, _EMPTY_, &m.Server, &m, false}
		select {
		case sendQ <- msg:
		default:
			a.mu.Unlock()
			sendQ <- msg
			a.mu.Lock()
		}
	}
	a.mu.Unlock()
	s.mu.Lock()
}

// accConnsUpdate is called whenever there is a change to the account's
// number of active connections, or during a heartbeat.
func (s *Server) accConnsUpdate(a *Account) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.eventsEnabled() || a == nil {
		return
	}
	s.sendAccConnsUpdate(a, fmt.Sprintf(accConnsEventSubjOld, a.Name), fmt.Sprintf(accConnsEventSubjNew, a.Name))
}

// server lock should be held
func (s *Server) nextEventID() string {
	return s.eventIds.Next()
}

// accountConnectEvent will send an account client connect event if there is interest.
// This is a billing event.
func (s *Server) accountConnectEvent(c *client) {
	s.mu.Lock()
	if !s.eventsEnabled() {
		s.mu.Unlock()
		return
	}
	gacc := s.gacc
	eid := s.nextEventID()
	s.mu.Unlock()

	c.mu.Lock()
	// Ignore global account activity
	if c.acc == nil || c.acc == gacc {
		c.mu.Unlock()
		return
	}

	m := ConnectEventMsg{
		TypedEvent: TypedEvent{
			Type: ConnectEventMsgType,
			ID:   eid,
			Time: time.Now().UTC(),
		},
		Client: ClientInfo{
			Start:     &c.start,
			Host:      c.host,
			ID:        c.cid,
			Account:   accForClient(c),
			User:      c.getRawAuthUser(),
			Name:      c.opts.Name,
			Lang:      c.opts.Lang,
			Version:   c.opts.Version,
			Jwt:       c.opts.JWT,
			IssuerKey: issuerForClient(c),
			Tags:      c.tags,
			NameTag:   c.nameTag,
		},
	}
	c.mu.Unlock()

	subj := fmt.Sprintf(connectEventSubj, c.acc.Name)
	s.sendInternalMsgLocked(subj, _EMPTY_, &m.Server, &m)
}

// accountDisconnectEvent will send an account client disconnect event if there is interest.
// This is a billing event.
func (s *Server) accountDisconnectEvent(c *client, now time.Time, reason string) {
	s.mu.Lock()
	if !s.eventsEnabled() {
		s.mu.Unlock()
		return
	}
	gacc := s.gacc
	eid := s.nextEventID()
	s.mu.Unlock()

	c.mu.Lock()

	// Ignore global account activity
	if c.acc == nil || c.acc == gacc {
		c.mu.Unlock()
		return
	}

	m := DisconnectEventMsg{
		TypedEvent: TypedEvent{
			Type: DisconnectEventMsgType,
			ID:   eid,
			Time: now,
		},
		Client: ClientInfo{
			Start:     &c.start,
			Stop:      &now,
			Host:      c.host,
			ID:        c.cid,
			Account:   accForClient(c),
			User:      c.getRawAuthUser(),
			Name:      c.opts.Name,
			Lang:      c.opts.Lang,
			Version:   c.opts.Version,
			RTT:       c.getRTT(),
			Jwt:       c.opts.JWT,
			IssuerKey: issuerForClient(c),
			Tags:      c.tags,
			NameTag:   c.nameTag,
		},
		Sent: DataStats{
			Msgs:  atomic.LoadInt64(&c.inMsgs),
			Bytes: atomic.LoadInt64(&c.inBytes),
		},
		Received: DataStats{
			Msgs:  c.outMsgs,
			Bytes: c.outBytes,
		},
		Reason: reason,
	}
	accName := c.acc.Name
	c.mu.Unlock()

	subj := fmt.Sprintf(disconnectEventSubj, accName)
	s.sendInternalMsgLocked(subj, _EMPTY_, &m.Server, &m)
}

func (s *Server) sendAuthErrorEvent(c *client) {
	s.mu.Lock()
	if !s.eventsEnabled() {
		s.mu.Unlock()
		return
	}
	eid := s.nextEventID()
	s.mu.Unlock()
	now := time.Now().UTC()
	c.mu.Lock()
	m := DisconnectEventMsg{
		TypedEvent: TypedEvent{
			Type: DisconnectEventMsgType,
			ID:   eid,
			Time: now,
		},
		Client: ClientInfo{
			Start:     &c.start,
			Stop:      &now,
			Host:      c.host,
			ID:        c.cid,
			Account:   accForClient(c),
			User:      c.getRawAuthUser(),
			Name:      c.opts.Name,
			Lang:      c.opts.Lang,
			Version:   c.opts.Version,
			RTT:       c.getRTT(),
			Jwt:       c.opts.JWT,
			IssuerKey: issuerForClient(c),
			Tags:      c.tags,
			NameTag:   c.nameTag,
		},
		Sent: DataStats{
			Msgs:  c.inMsgs,
			Bytes: c.inBytes,
		},
		Received: DataStats{
			Msgs:  c.outMsgs,
			Bytes: c.outBytes,
		},
		Reason: AuthenticationViolation.String(),
	}
	c.mu.Unlock()

	s.mu.Lock()
	subj := fmt.Sprintf(authErrorEventSubj, s.info.ID)
	s.sendInternalMsg(subj, _EMPTY_, &m.Server, &m)
	s.mu.Unlock()
}

// Internal message callback. If the msg is needed past the callback it is
// required to be copied.
type msgHandler func(sub *subscription, client *client, subject, reply string, msg []byte)

// Create an internal subscription. sysSubscribeQ for queue groups.
func (s *Server) sysSubscribe(subject string, cb msgHandler) (*subscription, error) {
	return s.systemSubscribe(subject, _EMPTY_, false, nil, cb)
}

// Create an internal subscription with queue
func (s *Server) sysSubscribeQ(subject, queue string, cb msgHandler) (*subscription, error) {
	return s.systemSubscribe(subject, queue, false, nil, cb)
}

// Create an internal subscription but do not forward interest.
func (s *Server) sysSubscribeInternal(subject string, cb msgHandler) (*subscription, error) {
	return s.systemSubscribe(subject, _EMPTY_, true, nil, cb)
}

func (s *Server) systemSubscribe(subject, queue string, internalOnly bool, c *client, cb msgHandler) (*subscription, error) {
	s.mu.Lock()
	if !s.eventsEnabled() {
		s.mu.Unlock()
		return nil, ErrNoSysAccount
	}
	if cb == nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("undefined message handler")
	}
	if c == nil {
		c = s.sys.client
	}
	trace := c.trace
	s.sys.sid++
	sid := strconv.Itoa(s.sys.sid)
	s.mu.Unlock()

	// Now create the subscription
	if trace {
		c.traceInOp("SUB", []byte(subject+" "+queue+" "+sid))
	}

	var q []byte
	if queue != "" {
		q = []byte(queue)
	}
	// Now create the subscription
	return c.processSub([]byte(subject), q, []byte(sid), cb, internalOnly)
}

func (s *Server) sysUnsubscribe(sub *subscription) {
	if sub == nil {
		return
	}
	s.mu.Lock()
	if !s.eventsEnabled() {
		s.mu.Unlock()
		return
	}
	c := sub.client
	s.mu.Unlock()

	if c != nil {
		c.processUnsub(sub.sid)
	}
}

// This will generate the tracking subject for remote latency from the response subject.
func remoteLatencySubjectForResponse(subject []byte) string {
	if !isTrackedReply(subject) {
		return ""
	}
	toks := bytes.Split(subject, []byte(tsep))
	// FIXME(dlc) - Sprintf may become a performance concern at some point.
	return fmt.Sprintf(remoteLatencyEventSubj, toks[len(toks)-2])
}

// remoteLatencyUpdate is used to track remote latency measurements for tracking on exported services.
func (s *Server) remoteLatencyUpdate(sub *subscription, _ *client, subject, _ string, msg []byte) {
	if !s.eventsRunning() {
		return
	}
	rl := remoteLatency{}
	if err := json.Unmarshal(msg, &rl); err != nil {
		s.Errorf("Error unmarshalling remote latency measurement: %v", err)
		return
	}
	// Now we need to look up the responseServiceImport associated with this measurement.
	acc, err := s.LookupAccount(rl.Account)
	if err != nil {
		s.Warnf("Could not lookup account %q for latency measurement", rl.Account)
		return
	}
	// Now get the request id / reply. We need to see if we have a GW prefix and if so strip that off.
	reply := rl.ReqId
	if gwPrefix, old := isGWRoutedSubjectAndIsOldPrefix([]byte(reply)); gwPrefix {
		reply = string(getSubjectFromGWRoutedReply([]byte(reply), old))
	}
	acc.mu.RLock()
	si := acc.exports.responses[reply]
	if si == nil {
		acc.mu.RUnlock()
		return
	}
	m1 := si.m1
	m2 := rl.M2

	lsub := si.latency.subject
	acc.mu.RUnlock()

	// So we have not processed the response tracking measurement yet.
	if m1 == nil {
		si.acc.mu.Lock()
		// Double check since could have slipped in.
		m1 = si.m1
		if m1 == nil {
			// Store our value there for them to pick up.
			si.m1 = &m2
		}
		si.acc.mu.Unlock()
		if m1 == nil {
			return
		}
	}

	// Calculate the correct latencies given M1 and M2.
	m1.merge(&m2)

	// Clear the requesting client since we send the result here.
	acc.mu.Lock()
	si.rc = nil
	acc.mu.Unlock()

	// Make sure we remove the entry here.
	acc.removeServiceImport(si.from)
	// Send the metrics
	s.sendInternalAccountMsg(acc, lsub, m1)
}

// This is used for all inbox replies so that we do not send supercluster wide interest
// updates for every request. Same trick used in modern NATS clients.
func (s *Server) inboxReply(sub *subscription, c *client, subject, reply string, msg []byte) {
	s.mu.Lock()
	if !s.eventsEnabled() || s.sys.replies == nil {
		s.mu.Unlock()
		return
	}
	cb, ok := s.sys.replies[subject]
	s.mu.Unlock()

	if ok && cb != nil {
		cb(sub, c, subject, reply, msg)
	}
}

// Copied from go client.
// We could use serviceReply here instead to save some code.
// I prefer these semantics for the moment, when tracing you know what this is.
const (
	InboxPrefix        = "$SYS._INBOX."
	inboxPrefixLen     = len(InboxPrefix)
	respInboxPrefixLen = inboxPrefixLen + sysHashLen + 1
	replySuffixLen     = 8 // Gives us 62^8
)

// Creates an internal inbox used for replies that will be processed by the global wc handler.
func (s *Server) newRespInbox() string {
	var b [respInboxPrefixLen + replySuffixLen]byte
	pres := b[:respInboxPrefixLen]
	copy(pres, s.sys.inboxPre)
	rn := rand.Int63()
	for i, l := respInboxPrefixLen, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	return string(b[:])
}

// accNumSubsReq is sent when we need to gather remote info on subs.
type accNumSubsReq struct {
	Account string `json:"acc"`
	Subject string `json:"subject"`
	Queue   []byte `json:"queue,omitempty"`
}

// helper function to total information from results to count subs.
func totalSubs(rr *SublistResult, qg []byte) (nsubs int32) {
	if rr == nil {
		return
	}
	checkSub := func(sub *subscription) {
		// TODO(dlc) - This could be smarter.
		if qg != nil && !bytes.Equal(qg, sub.queue) {
			return
		}
		if sub.client.kind == CLIENT || sub.client.isHubLeafNode() {
			nsubs++
		}
	}
	if qg == nil {
		for _, sub := range rr.psubs {
			checkSub(sub)
		}
	}
	for _, qsub := range rr.qsubs {
		for _, sub := range qsub {
			checkSub(sub)
		}
	}
	return
}

// Allows users of large systems to debug active subscribers for a given subject.
// Payload should be the subject of interest.
func (s *Server) debugSubscribers(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	// Even though this is an internal only subscription, meaning interest was not forwarded, we could
	// get one here from a GW in optimistic mode. Ignore for now.
	// FIXME(dlc) - Should we send no interest here back to the GW?
	if c.kind != CLIENT {
		return
	}

	_, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		return
	}

	// We could have a single subject or we could have a subject and a wildcard separated by whitespace.
	args := strings.Split(strings.TrimSpace(string(msg)), " ")
	if len(args) == 0 {
		s.sendInternalAccountMsg(acc, reply, 0)
		return
	}

	tsubj := args[0]
	var qgroup []byte
	if len(args) > 1 {
		qgroup = []byte(args[1])
	}

	var nsubs int32

	if subjectIsLiteral(tsubj) {
		// We will look up subscribers locally first then determine if we need to solicit other servers.
		rr := acc.sl.Match(tsubj)
		nsubs = totalSubs(rr, qgroup)
	} else {
		// We have a wildcard, so this is a bit slower path.
		var _subs [32]*subscription
		subs := _subs[:0]
		acc.sl.All(&subs)
		for _, sub := range subs {
			if subjectIsSubsetMatch(string(sub.subject), tsubj) {
				if qgroup != nil && !bytes.Equal(qgroup, sub.queue) {
					continue
				}
				if sub.client.kind == CLIENT || sub.client.isHubLeafNode() {
					nsubs++
				}
			}
		}
	}

	// We should have an idea of how many responses to expect from remote servers.
	var expected = acc.expectedRemoteResponses()

	// If we are only local, go ahead and return.
	if expected == 0 {
		s.sendInternalAccountMsg(nil, reply, nsubs)
		return
	}

	// We need to solicit from others.
	// To track status.
	responses := int32(0)
	done := make(chan (bool))

	s.mu.Lock()
	// Create direct reply inbox that we multiplex under the WC replies.
	replySubj := s.newRespInbox()
	// Store our handler.
	s.sys.replies[replySubj] = func(sub *subscription, _ *client, subject, _ string, msg []byte) {
		if n, err := strconv.Atoi(string(msg)); err == nil {
			atomic.AddInt32(&nsubs, int32(n))
		}
		if atomic.AddInt32(&responses, 1) >= expected {
			select {
			case done <- true:
			default:
			}
		}
	}
	// Send the request to the other servers.
	request := &accNumSubsReq{
		Account: acc.Name,
		Subject: tsubj,
		Queue:   qgroup,
	}
	s.sendInternalMsg(accNumSubsReqSubj, replySubj, nil, request)
	s.mu.Unlock()

	// FIXME(dlc) - We should rate limit here instead of blind Go routine.
	go func() {
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
		}
		// Cleanup the WC entry.
		var sendResponse bool
		s.mu.Lock()
		if s.sys != nil && s.sys.replies != nil {
			delete(s.sys.replies, replySubj)
			sendResponse = true
		}
		s.mu.Unlock()
		if sendResponse {
			// Send the response.
			s.sendInternalAccountMsg(nil, reply, atomic.LoadInt32(&nsubs))
		}
	}()
}

// Request for our local subscription count. This will come from a remote origin server
// that received the initial request.
func (s *Server) nsubsRequest(sub *subscription, _ *client, subject, reply string, msg []byte) {
	if !s.eventsRunning() {
		return
	}
	m := accNumSubsReq{}
	if err := json.Unmarshal(msg, &m); err != nil {
		s.sys.client.Errorf("Error unmarshalling account nsubs request message: %v", err)
		return
	}
	// Grab account.
	acc, _ := s.lookupAccount(m.Account)
	if acc == nil || acc.numLocalAndLeafConnections() == 0 {
		return
	}
	// We will look up subscribers locally first then determine if we need to solicit other servers.
	var nsubs int32
	if subjectIsLiteral(m.Subject) {
		rr := acc.sl.Match(m.Subject)
		nsubs = totalSubs(rr, m.Queue)
	} else {
		// We have a wildcard, so this is a bit slower path.
		var _subs [32]*subscription
		subs := _subs[:0]
		acc.sl.All(&subs)
		for _, sub := range subs {
			if (sub.client.kind == CLIENT || sub.client.isHubLeafNode()) && subjectIsSubsetMatch(string(sub.subject), m.Subject) {
				if m.Queue != nil && !bytes.Equal(m.Queue, sub.queue) {
					continue
				}
				nsubs++
			}
		}
	}
	s.sendInternalMsgLocked(reply, _EMPTY_, nil, nsubs)
}

// Helper to grab account name for a client.
func accForClient(c *client) string {
	if c.acc != nil {
		return c.acc.Name
	}
	return "N/A"
}

// Helper to grab issuer for a client.
func issuerForClient(c *client) (issuerKey string) {
	if c == nil || c.user == nil {
		return
	}
	issuerKey = c.user.SigningKey
	if issuerKey == "" && c.user.Account != nil {
		issuerKey = c.user.Account.Name
	}
	return
}

// Helper to clear timers.
func clearTimer(tp **time.Timer) {
	if t := *tp; t != nil {
		t.Stop()
		*tp = nil
	}
}

// Helper function to wrap functions with common test
// to lock server and return if events not enabled.
func (s *Server) wrapChk(f func()) func() {
	return func() {
		s.mu.Lock()
		if !s.eventsEnabled() {
			s.mu.Unlock()
			return
		}
		f()
		s.mu.Unlock()
	}
}
