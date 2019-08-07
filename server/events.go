// Copyright 2018-2019 The NATS Authors
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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/jwt"
	"github.com/nats-io/nats-server/v2/server/pse"
)

const (
	connectEventSubj         = "$SYS.ACCOUNT.%s.CONNECT"
	disconnectEventSubj      = "$SYS.ACCOUNT.%s.DISCONNECT"
	accConnsReqSubj          = "$SYS.REQ.ACCOUNT.%s.CONNS"
	accUpdateEventSubj       = "$SYS.ACCOUNT.%s.CLAIMS.UPDATE"
	connsRespSubj            = "$SYS._INBOX_.%s"
	accConnsEventSubj        = "$SYS.SERVER.ACCOUNT.%s.CONNS"
	shutdownEventSubj        = "$SYS.SERVER.%s.SHUTDOWN"
	authErrorEventSubj       = "$SYS.SERVER.%s.CLIENT.AUTH.ERR"
	serverStatsSubj          = "$SYS.SERVER.%s.STATSZ"
	serverStatsReqSubj       = "$SYS.REQ.SERVER.%s.STATSZ"
	serverStatsPingReqSubj   = "$SYS.REQ.SERVER.PING"
	leafNodeConnectEventSubj = "$SYS.ACCOUNT.%s.LEAFNODE.CONNECT"

	shutdownEventTokens = 4
	serverSubjectIndex  = 2
	accUpdateTokens     = 5
	accUpdateAccIndex   = 2
	defaultEventsHBItvl = 30 * time.Second
)

// FIXME(dlc) - make configurable.
var eventsHBInterval = defaultEventsHBItvl

// Used to send and receive messages from inside the server.
type internal struct {
	account *Account
	client  *client
	seq     uint64
	sid     uint64
	servers map[string]*serverUpdate
	sweeper *time.Timer
	stmr    *time.Timer
	subs    map[string]msgHandler
	sendq   chan *pubMsg
	wg      sync.WaitGroup
	orphMax time.Duration
	chkOrph time.Duration
	statsz  time.Duration
}

// ServerStatsMsg is sent periodically with stats updates.
type ServerStatsMsg struct {
	Server ServerInfo  `json:"server"`
	Stats  ServerStats `json:"statsz"`
}

// ConnectEventMsg is sent when a new connection is made that is part of an account.
type ConnectEventMsg struct {
	Server ServerInfo `json:"server"`
	Client ClientInfo `json:"client"`
}

// DisconnectEventMsg is sent when a new connection previously defined from a
// ConnectEventMsg is closed.
type DisconnectEventMsg struct {
	Server   ServerInfo `json:"server"`
	Client   ClientInfo `json:"client"`
	Sent     DataStats  `json:"sent"`
	Received DataStats  `json:"received"`
	Reason   string     `json:"reason"`
}

// AccountNumConns is an event that will be sent from a server that is tracking
// a given account when the number of connections changes. It will also HB
// updates in the absence of any changes.
type AccountNumConns struct {
	Server     ServerInfo `json:"server"`
	Account    string     `json:"acc"`
	Conns      int        `json:"conns"`
	LeafNodes  int        `json:"leafnodes"`
	TotalConns int        `json:"total_conns"`
}

// accNumConnsReq is sent when we are starting to track an account for the first
// time. We will request others send info to us about their local state.
type accNumConnsReq struct {
	Server  ServerInfo `json:"server"`
	Account string     `json:"acc"`
}

// ServerInfo identifies remote servers.
type ServerInfo struct {
	Host    string    `json:"host"`
	ID      string    `json:"id"`
	Cluster string    `json:"cluster,omitempty"`
	Version string    `json:"ver"`
	Seq     uint64    `json:"seq"`
	Time    time.Time `json:"time"`
}

// ClientInfo is detailed information about the client forming a connection.
type ClientInfo struct {
	Start   time.Time  `json:"start,omitempty"`
	Host    string     `json:"host,omitempty"`
	ID      uint64     `json:"id"`
	Account string     `json:"acc"`
	User    string     `json:"user,omitempty"`
	Name    string     `json:"name,omitempty"`
	Lang    string     `json:"lang,omitempty"`
	Version string     `json:"ver,omitempty"`
	RTT     string     `json:"rtt,omitempty"`
	Stop    *time.Time `json:"stop,omitempty"`
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

// internalSendLoop will be responsible for serializing all messages that
// a server wants to send.
func (s *Server) internalSendLoop(wg *sync.WaitGroup) {
	defer wg.Done()

	s.mu.Lock()
	if s.sys == nil || s.sys.sendq == nil {
		s.mu.Unlock()
		return
	}
	c := s.sys.client
	sendq := s.sys.sendq
	id := s.info.ID
	host := s.info.Host
	seqp := &s.sys.seq
	var cluster string
	if s.gateway.enabled {
		cluster = s.getGatewayName()
	}
	s.mu.Unlock()

	for s.eventsRunning() {
		// Setup information for next message
		seq := atomic.AddUint64(seqp, 1)
		select {
		case pm := <-sendq:
			if pm.si != nil {
				pm.si.Host = host
				pm.si.Cluster = cluster
				pm.si.ID = id
				pm.si.Seq = seq
				pm.si.Version = VERSION
				pm.si.Time = time.Now()
			}
			var b []byte
			if pm.msg != nil {
				b, _ = json.MarshalIndent(pm.msg, _EMPTY_, "  ")
			}
			// Prep internal structures needed to send message.
			c.pa.subject = []byte(pm.sub)
			c.pa.size = len(b)
			c.pa.szb = []byte(strconv.FormatInt(int64(len(b)), 10))
			c.pa.reply = []byte(pm.rply)
			// Add in NL
			b = append(b, _CRLF_...)
			c.processInboundClientMsg(b)
			c.flushClients(0) // Never spend time in place.
			// See if we are doing graceful shutdown.
			if pm.last {
				return
			}
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
	s.sys.subs = nil
	s.mu.Unlock()
	// Send to the internal queue and mark as last.
	sendq <- &pubMsg{subj, _EMPTY_, nil, nil, true}
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
	sendq <- &pubMsg{sub, rply, si, msg, false}
	s.mu.Lock()
}

// Locked version of checking if events system running. Also checks server.
func (s *Server) eventsRunning() bool {
	s.mu.Lock()
	er := s.running && s.eventsEnabled()
	s.mu.Unlock()
	return er
}

// EventsEnabled will report if the server has internal events enabled via
// a defined system account.
func (s *Server) EventsEnabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.eventsEnabled()
}

// eventsEnabled will report if events are enabled.
// Lock should be held.
func (s *Server) eventsEnabled() bool {
	return s.sys != nil && s.sys.client != nil && s.sys.account != nil
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
func updateServerUsage(v *ServerStats) {
	var rss, vss int64
	var pcpu float64
	pse.ProcUsage(&pcpu, &rss, &vss)
	v.Mem = rss
	v.CPU = pcpu
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
	r.mu.Unlock()
	return rs
}

// Actual send method for statz updates.
// Lock should be held.
func (s *Server) sendStatsz(subj string) {
	m := ServerStatsMsg{}
	updateServerUsage(&m.Stats)
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
		s.sys.stmr.Reset(s.sys.statsz)
	}
	s.sendStatsz(fmt.Sprintf(serverStatsSubj, s.info.ID))
}

// This should be wrapChk() to setup common locking.
func (s *Server) startStatszTimer() {
	s.sys.stmr = time.AfterFunc(s.sys.statsz, s.wrapChk(s.heartbeatStatsz))
}

// Start a ticker that will fire periodically and check for orphaned servers.
// This should be wrapChk() to setup common locking.
func (s *Server) startRemoteServerSweepTimer() {
	s.sys.sweeper = time.AfterFunc(s.sys.chkOrph, s.wrapChk(s.checkRemoteServers))
}

// This will setup our system wide tracking subs.
// For now we will setup one wildcard subscription to
// monitor all accounts for changes in number of connections.
// We can make this on a per account tracking basis if needed.
// Tradeoff is subscription and interest graph events vs connect and
// disconnect events, etc.
func (s *Server) initEventTracking() {
	if !s.eventsEnabled() {
		return
	}
	subject := fmt.Sprintf(accConnsEventSubj, "*")
	if _, err := s.sysSubscribe(subject, s.remoteConnsUpdate); err != nil {
		s.Errorf("Error setting up internal tracking: %v", err)
	}
	// This will be for responses for account info that we send out.
	subject = fmt.Sprintf(connsRespSubj, s.info.ID)
	if _, err := s.sysSubscribe(subject, s.remoteConnsUpdate); err != nil {
		s.Errorf("Error setting up internal tracking: %v", err)
	}
	// Listen for broad requests to respond with account info.
	subject = fmt.Sprintf(accConnsReqSubj, "*")
	if _, err := s.sysSubscribe(subject, s.connsRequest); err != nil {
		s.Errorf("Error setting up internal tracking: %v", err)
	}
	// Listen for all server shutdowns.
	subject = fmt.Sprintf(shutdownEventSubj, "*")
	if _, err := s.sysSubscribe(subject, s.remoteServerShutdown); err != nil {
		s.Errorf("Error setting up internal tracking: %v", err)
	}
	// Listen for account claims updates.
	subject = fmt.Sprintf(accUpdateEventSubj, "*")
	if _, err := s.sysSubscribe(subject, s.accountClaimUpdate); err != nil {
		s.Errorf("Error setting up internal tracking: %v", err)
	}
	// Listen for requests for our statsz.
	subject = fmt.Sprintf(serverStatsReqSubj, s.info.ID)
	if _, err := s.sysSubscribe(subject, s.statszReq); err != nil {
		s.Errorf("Error setting up internal tracking: %v", err)
	}
	// Listen for ping messages that will be sent to all servers for statsz.
	if _, err := s.sysSubscribe(serverStatsPingReqSubj, s.statszReq); err != nil {
		s.Errorf("Error setting up internal tracking: %v", err)
	}
	// Listen for updates when leaf nodes connect for a given account. This will
	// force any gateway connections to move to `modeInterestOnly`
	subject = fmt.Sprintf(leafNodeConnectEventSubj, "*")
	if _, err := s.sysSubscribe(subject, s.leafNodeConnected); err != nil {
		s.Errorf("Error setting up internal tracking: %v", err)
	}
}

// accountClaimUpdate will receive claim updates for accounts.
func (s *Server) accountClaimUpdate(sub *subscription, subject, reply string, msg []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.eventsEnabled() {
		return
	}
	toks := strings.Split(subject, tsep)
	if len(toks) < accUpdateTokens {
		s.Debugf("Received account claims update on bad subject %q", subject)
		return
	}
	if v, ok := s.accounts.Load(toks[accUpdateAccIndex]); ok {
		s.updateAccountWithClaimJWT(v.(*Account), string(msg))
	}
}

// processRemoteServerShutdown will update any affected accounts.
// Will update the remote count for clients.
// Lock assume held.
func (s *Server) processRemoteServerShutdown(sid string) {
	s.accounts.Range(func(k, v interface{}) bool {
		a := v.(*Account)
		a.mu.Lock()
		prev := a.strack[sid]
		delete(a.strack, sid)
		a.nrclients -= prev.conns
		a.nrleafs -= prev.leafs
		a.mu.Unlock()
		return true
	})
}

// remoteServerShutdownEvent is called when we get an event from another server shutting down.
func (s *Server) remoteServerShutdown(sub *subscription, subject, reply string, msg []byte) {
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
	su := s.sys.servers[sid]
	if su != nil {
		s.processRemoteServerShutdown(sid)
	}
}

// updateRemoteServer is called when we have an update from a remote server.
// This allows us to track remote servers, respond to shutdown messages properly,
// make sure that messages are ordered, and allow us to prune dead servers.
// Lock should be held upon entry.
func (s *Server) updateRemoteServer(ms *ServerInfo) {
	su := s.sys.servers[ms.ID]
	if su == nil {
		s.sys.servers[ms.ID] = &serverUpdate{ms.Seq, time.Now()}
	} else {
		// Should alwqys be going up.
		if ms.Seq <= su.seq {
			s.Errorf("Received out of order remote server update from: %q", ms.ID)
			return
		}
		su.seq = ms.Seq
		su.ltime = time.Now()
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
	s.mu.Unlock()

	// We will queue up a shutdown event and wait for the
	// internal send loop to exit.
	s.sendShutdownEvent()
	s.sys.wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Whip through all accounts.
	s.accounts.Range(func(k, v interface{}) bool {
		a := v.(*Account)
		a.mu.Lock()
		a.nrclients = 0
		// Now clear state
		clearTimer(&a.etmr)
		clearTimer(&a.ctmr)
		a.clients = nil
		a.strack = nil
		a.mu.Unlock()
		return true
	})
	// Turn everything off here.
	s.sys = nil
}

// Request for our local connection count.
func (s *Server) connsRequest(sub *subscription, subject, reply string, msg []byte) {
	if !s.eventsRunning() {
		return
	}
	m := accNumConnsReq{}
	if err := json.Unmarshal(msg, &m); err != nil {
		s.sys.client.Errorf("Error unmarshalling account connections request message: %v", err)
		return
	}
	acc, _ := s.lookupAccount(m.Account)
	if acc == nil {
		return
	}
	if nlc := acc.NumLocalConnections(); nlc > 0 {
		s.mu.Lock()
		s.sendAccConnsUpdate(acc, reply)
		s.mu.Unlock()
	}
}

// leafNodeConnected is an event we will receive when a leaf node for a given account
// connects.
func (s *Server) leafNodeConnected(sub *subscription, subject, reply string, msg []byte) {
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

// statszReq is a request for us to respond with current statz.
func (s *Server) statszReq(sub *subscription, subject, reply string, msg []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.eventsEnabled() || reply == _EMPTY_ {
		return
	}
	s.sendStatsz(reply)
}

// remoteConnsUpdate gets called when we receive a remote update from another server.
func (s *Server) remoteConnsUpdate(sub *subscription, subject, reply string, msg []byte) {
	if !s.eventsRunning() {
		return
	}
	m := AccountNumConns{}
	if err := json.Unmarshal(msg, &m); err != nil {
		s.sys.client.Errorf("Error unmarshalling account connection event message: %v", err)
		return
	}

	// See if we have the account registered, if not drop it.
	acc, _ := s.lookupAccount(m.Account)

	s.mu.Lock()
	defer s.mu.Unlock()

	// check again here if we have been shutdown.
	if !s.running || !s.eventsEnabled() {
		return
	}

	// Double check that this is not us, should never happen, so error if it does.
	if m.Server.ID == s.info.ID {
		s.sys.client.Errorf("Processing our own account connection event message: ignored")
		return
	}
	if acc == nil {
		s.sys.client.Debugf("Received account connection event for unknown account: %s", m.Account)
		return
	}
	// If we are here we have interest in tracking this account. Update our accounting.
	acc.mu.Lock()
	if acc.strack == nil {
		acc.strack = make(map[string]sconns)
	}
	// This does not depend on receiving all updates since each one is idempotent.
	prev := acc.strack[m.Server.ID]
	acc.strack[m.Server.ID] = sconns{conns: int32(m.Conns), leafs: int32(m.LeafNodes)}
	acc.nrclients += int32(m.Conns) - prev.conns
	acc.nrleafs += int32(m.LeafNodes) - prev.leafs
	acc.mu.Unlock()

	s.updateRemoteServer(&m.Server)
}

// Setup tracking for this account. This allows us to track globally
// account activity.
// Lock should be held on entry.
func (s *Server) enableAccountTracking(a *Account) {
	if a == nil || !s.eventsEnabled() {
		return
	}

	// TODO(ik): Generate payload although message may not be sent.
	// May need to ensure we do so only if there is a known interest.
	// This can get complicated with gateways.

	subj := fmt.Sprintf(accConnsReqSubj, a.Name)
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
	subj := fmt.Sprintf(leafNodeConnectEventSubj, a.Name)
	m := accNumConnsReq{Account: a.Name}
	s.sendInternalMsg(subj, "", &m.Server, &m)
	s.mu.Unlock()

	s.switchAccountToInterestMode(a.Name)
}

// sendAccConnsUpdate is called to send out our information on the
// account's local connections.
// Lock should be held on entry.
func (s *Server) sendAccConnsUpdate(a *Account, subj string) {
	if !s.eventsEnabled() || a == nil || a == s.gacc {
		return
	}
	a.mu.RLock()

	// If no limits set, don't update, no need to.
	if a.mconns == jwt.NoLimit && a.mleafs == jwt.NoLimit {
		a.mu.RUnlock()
		return
	}

	// Build event with account name and number of local clients and leafnodes.
	m := AccountNumConns{
		Account:    a.Name,
		Conns:      a.numLocalConnections(),
		LeafNodes:  a.numLocalLeafNodes(),
		TotalConns: a.numLocalConnections() + a.numLocalLeafNodes(),
	}
	a.mu.RUnlock()

	s.sendInternalMsg(subj, _EMPTY_, &m.Server, &m)

	// Set timer to fire again unless we are at zero.
	a.mu.Lock()
	if a.numLocalConnections() == 0 {
		clearTimer(&a.ctmr)
	} else {
		// Check to see if we have an HB running and update.
		if a.ctmr == nil {
			a.ctmr = time.AfterFunc(eventsHBInterval, func() { s.accConnsUpdate(a) })
		} else {
			a.ctmr.Reset(eventsHBInterval)
		}
	}
	a.mu.Unlock()
}

// accConnsUpdate is called whenever there is a change to the account's
// number of active connections, or during a heartbeat.
func (s *Server) accConnsUpdate(a *Account) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.eventsEnabled() || a == nil {
		return
	}
	subj := fmt.Sprintf(accConnsEventSubj, a.Name)
	s.sendAccConnsUpdate(a, subj)
}

// accountConnectEvent will send an account client connect event if there is interest.
// This is a billing event.
func (s *Server) accountConnectEvent(c *client) {
	s.mu.Lock()
	if !s.eventsEnabled() {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	subj := fmt.Sprintf(connectEventSubj, c.acc.Name)

	c.mu.Lock()
	m := ConnectEventMsg{
		Client: ClientInfo{
			Start:   c.start,
			Host:    c.host,
			ID:      c.cid,
			Account: accForClient(c),
			User:    nameForClient(c),
			Name:    c.opts.Name,
			Lang:    c.opts.Lang,
			Version: c.opts.Version,
		},
	}
	c.mu.Unlock()

	s.mu.Lock()
	s.sendInternalMsg(subj, _EMPTY_, &m.Server, &m)
	s.mu.Unlock()
}

// accountDisconnectEvent will send an account client disconnect event if there is interest.
// This is a billing event.
func (s *Server) accountDisconnectEvent(c *client, now time.Time, reason string) {
	s.mu.Lock()
	gacc := s.gacc
	if !s.eventsEnabled() {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	c.mu.Lock()

	// Ignore global account activity
	if c.acc == nil || c.acc == gacc {
		c.mu.Unlock()
		return
	}

	m := DisconnectEventMsg{
		Client: ClientInfo{
			Start:   c.start,
			Stop:    &now,
			Host:    c.host,
			ID:      c.cid,
			Account: accForClient(c),
			User:    nameForClient(c),
			Name:    c.opts.Name,
			Lang:    c.opts.Lang,
			Version: c.opts.Version,
			RTT:     c.getRTT(),
		},
		Sent: DataStats{
			Msgs:  c.inMsgs,
			Bytes: c.inBytes,
		},
		Received: DataStats{
			Msgs:  c.outMsgs,
			Bytes: c.outBytes,
		},
		Reason: reason,
	}
	c.mu.Unlock()

	subj := fmt.Sprintf(disconnectEventSubj, c.acc.Name)

	s.mu.Lock()
	s.sendInternalMsg(subj, _EMPTY_, &m.Server, &m)
	s.mu.Unlock()
}

func (s *Server) sendAuthErrorEvent(c *client) {
	s.mu.Lock()
	if !s.eventsEnabled() {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	now := time.Now()
	c.mu.Lock()
	m := DisconnectEventMsg{
		Client: ClientInfo{
			Start:   c.start,
			Stop:    &now,
			Host:    c.host,
			ID:      c.cid,
			Account: accForClient(c),
			User:    nameForClient(c),
			Name:    c.opts.Name,
			Lang:    c.opts.Lang,
			Version: c.opts.Version,
			RTT:     c.getRTT(),
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
type msgHandler func(sub *subscription, subject, reply string, msg []byte)

func (s *Server) deliverInternalMsg(sub *subscription, subject, reply, msg []byte) {
	s.mu.Lock()
	if !s.eventsEnabled() || s.sys.subs == nil {
		s.mu.Unlock()
		return
	}
	cb := s.sys.subs[string(sub.sid)]
	s.mu.Unlock()
	if cb != nil {
		cb(sub, string(subject), string(reply), msg)
	}
}

// Create an internal subscription. No support for queue groups atm.
func (s *Server) sysSubscribe(subject string, cb msgHandler) (*subscription, error) {
	if !s.eventsEnabled() {
		return nil, ErrNoSysAccount
	}
	if cb == nil {
		return nil, fmt.Errorf("undefined message handler")
	}
	s.mu.Lock()
	sid := strconv.FormatInt(int64(s.sys.sid), 10)
	s.sys.subs[sid] = cb
	s.sys.sid++
	c := s.sys.client
	s.mu.Unlock()

	// Now create the subscription
	if err := c.processSub([]byte(subject + " " + sid)); err != nil {
		return nil, err
	}
	c.mu.Lock()
	sub := c.subs[sid]
	c.mu.Unlock()
	return sub, nil
}

func (s *Server) sysUnsubscribe(sub *subscription) {
	if sub == nil || !s.eventsEnabled() {
		return
	}
	s.mu.Lock()
	acc := s.sys.account
	c := s.sys.client
	delete(s.sys.subs, string(sub.sid))
	s.mu.Unlock()
	c.unsubscribe(acc, sub, true, true)
}

// Helper to grab name for a client.
func nameForClient(c *client) string {
	if c.user != nil {
		return c.user.Nkey
	}
	return "N/A"
}

// Helper to grab account name for a client.
func accForClient(c *client) string {
	if c.acc != nil {
		return c.acc.Name
	}
	return "N/A"
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
		defer s.mu.Unlock()
		if !s.eventsEnabled() {
			return
		}
		f()
	}
}
