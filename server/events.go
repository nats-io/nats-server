// Copyright 2018 The NATS Authors
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
)

const (
	connectEventSubj    = "$SYS.ACCOUNT.%s.CONNECT"
	disconnectEventSubj = "$SYS.ACCOUNT.%s.DISCONNECT"
	accConnsEventSubj   = "$SYS.SERVER.ACCOUNT.%s.CONNS"
	accConnsReqSubj     = "$SYS.REQ.ACCOUNT.%s.CONNS"
	accUpdateEventSubj  = "$SYS.ACCOUNT.%s.CLAIMS.UPDATE"
	connsRespSubj       = "$SYS._INBOX_.%s"
	shutdownEventSubj   = "$SYS.SERVER.%s.SHUTDOWN"
	shutdownEventTokens = 4
	serverSubjectIndex  = 2
	accUpdateTokens     = 5
	accUpdateAccIndex   = 2
)

// Used to send and receive messages from inside the server.
type internal struct {
	account *Account
	client  *client
	seq     uint64
	sid     uint64
	servers map[string]*serverUpdate
	sweeper *time.Timer
	subs    map[string]msgHandler
	sendq   chan *pubMsg
	wg      sync.WaitGroup
	orphMax time.Duration
	chkOrph time.Duration
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

// accNumConns is an event that will be sent from a server that is tracking
// a given account when the number of connections changes. It will also HB
// updates in the absence of any changes.
type accNumConns struct {
	Server  ServerInfo `json:"server"`
	Account string     `json:"acc"`
	Conns   int        `json:"conns"`
}

// accNumConnsReq is sent when we are starting to track an account for the first
// time. We will request others send info to us about their local state.
type accNumConnsReq struct {
	Server  ServerInfo `json:"server"`
	Account string     `json:"acc"`
}

type ServerInfo struct {
	Host string `json:"host"`
	ID   string `json:"id"`
	Seq  uint64 `json:"seq"`
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
	Stop    *time.Time `json:"stop,omitempty"`
}

// DataStats reports how may msg and bytes. Applicable for both sent and received.
type DataStats struct {
	Msgs  int64 `json:"msgs"`
	Bytes int64 `json:"bytes"`
}

// Used for internally queueing up messages that the server wants to send.
type pubMsg struct {
	r    *SublistResult
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
	acc := s.sys.account
	sendq := s.sys.sendq
	id := s.info.ID
	host := s.info.Host
	seqp := &s.sys.seq
	s.mu.Unlock()

	for s.eventsRunning() {
		// Setup information for next message
		seq := atomic.AddUint64(seqp, 1)
		select {
		case pm := <-sendq:
			if pm.si != nil {
				pm.si.Host = host
				pm.si.ID = id
				pm.si.Seq = seq
			}
			var b []byte
			if pm.msg != nil {
				b, _ = json.MarshalIndent(pm.msg, _EMPTY_, "  ")
			}
			// Prep internal structures needed to send message.
			c.pa.subject = []byte(pm.sub)
			c.pa.size = len(b)
			c.pa.szb = []byte(strconv.FormatInt(int64(len(b)), 10))
			// Add in NL
			b = append(b, _CRLF_...)
			// Check to see if we need to map/route to another account.
			if acc.imports.services != nil {
				c.checkForImportServices(acc, b)
			}
			c.processMsgResults(acc, pm.r, b, c.pa.subject, []byte(pm.rply), nil)
			c.flushClients()
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
	r := s.sys.account.sl.Match(subj)
	sendq := s.sys.sendq
	// Stop any more messages from queueing up.
	s.sys.sendq = nil
	// Unhook all msgHandlers. Normal client cleanup will deal with subs, etc.
	s.sys.subs = nil
	s.mu.Unlock()
	// Send to the internal queue and mark as last.
	sendq <- &pubMsg{r, subj, _EMPTY_, nil, nil, true}
}

// This will queue up a message to be sent.
// Assumes lock is held on entry.
func (s *Server) sendInternalMsg(r *SublistResult, sub, rply string, si *ServerInfo, msg interface{}) {
	if s.sys == nil || s.sys.sendq == nil {
		return
	}
	sendq := s.sys.sendq
	// Don't hold lock while placing on the channel.
	s.mu.Unlock()
	sendq <- &pubMsg{r, sub, rply, si, msg, false}
	s.mu.Lock()
}

// Locked version of checking if events system running. Also checks server.
func (s *Server) eventsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running && s.eventsEnabled()
}

func (s *Server) eventsEnabled() bool {
	return s.sys != nil && s.sys.client != nil && s.sys.account != nil
}

// Check for orphan servers who may have gone away without notification.
func (s *Server) checkRemoteServers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.eventsEnabled() {
		return
	}
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

// Start a ticker that will fire periodically and check for orphaned servers.
func (s *Server) startRemoteServerSweepTimer() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.eventsEnabled() {
		return
	}
	s.sys.sweeper = time.AfterFunc(s.sys.chkOrph, s.checkRemoteServers)
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
	accName := toks[accUpdateAccIndex]
	s.updateAccountWithClaimJWT(s.accounts[accName], string(msg))
}

// processRemoteServerShutdown will update any affected accounts.
// Will upidate the remote count for clients.
// Lock assume held.
func (s *Server) processRemoteServerShutdown(sid string) {
	for _, a := range s.accounts {
		a.mu.Lock()
		prev := a.strack[sid]
		delete(a.strack, sid)
		a.nrclients -= prev
		a.mu.Unlock()
	}
}

// serverShutdownEvent is called when we get an event from another server shutting down.
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
		if ms.Seq <= su.seq {
			s.Errorf("Received out of order remote server update from: %q", ms.ID)
			return
		}
		if ms.Seq != su.seq+1 {
			s.Errorf("Missed [%d] remote server updates from: %q", ms.Seq-su.seq+1, ms.ID)
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
	if s.sys.sweeper != nil {
		s.sys.sweeper.Stop()
		s.sys.sweeper = nil
	}
	s.mu.Unlock()

	// We will queue up a shutdown event and wait for the
	// internal send loop to exit.
	s.sendShutdownEvent()
	s.sys.wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Whip through all accounts.
	for _, a := range s.accounts {
		a.mu.Lock()
		a.nrclients = 0
		// Now clear state
		if a.etmr != nil {
			a.etmr.Stop()
			a.etmr = nil
		}
		if a.ctmr != nil {
			a.ctmr.Stop()
			a.ctmr = nil
		}
		a.clients = nil
		a.strack = nil
		a.mu.Unlock()
	}
	// Turn everything off here.
	s.sys = nil
}

func (s *Server) connsRequest(sub *subscription, subject, reply string, msg []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.eventsEnabled() {
		return
	}

	m := accNumConnsReq{}
	if err := json.Unmarshal(msg, &m); err != nil {
		s.sys.client.Errorf("Error unmarshalling account connections request message: %v", err)
		return
	}
	acc := s.lookupAccount(m.Account)
	if acc == nil {
		return
	}
	if nlc := acc.NumLocalClients(); nlc > 0 {
		s.sendAccConnsUpdate(acc, reply)
	}
}

// remoteConnsUpdate gets called when we receive a remote update from another server.
func (s *Server) remoteConnsUpdate(sub *subscription, subject, reply string, msg []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.eventsEnabled() {
		return
	}

	m := accNumConns{}
	if err := json.Unmarshal(msg, &m); err != nil {
		s.sys.client.Errorf("Error unmarshalling account connection event message: %v", err)
		return
	}
	// Double check that this is not us, should never happen, so error if it does.
	if m.Server.ID == s.info.ID {
		s.sys.client.Errorf("Processing our own account connection event message: ignored")
		return
	}
	// See if we have the account registered, if not drop it.
	acc := s.lookupAccount(m.Account)
	if acc == nil {
		s.sys.client.Debugf("Received account connection event for unknown account: %s", m.Account)
		return
	}
	// If we are here we have interest in tracking this account. Update our accounting.
	acc.mu.Lock()
	if acc.strack == nil {
		acc.strack = make(map[string]int)
	}
	// This does not depend on receiving all updates since each one is idempotent.
	prev := acc.strack[m.Server.ID]
	acc.strack[m.Server.ID] = m.Conns
	acc.nrclients += (m.Conns - prev)
	acc.mu.Unlock()

	s.updateRemoteServer(&m.Server)
}

// Setup tracking for this account. This allows us to track globally
// account activity.
// Lock should be held on entry.
func (s *Server) enableAccountTracking(a *Account) {
	if a == nil || !s.eventsEnabled() || a == s.sys.account {
		return
	}
	acc := s.sys.account
	sc := s.sys.client

	subj := fmt.Sprintf(accConnsReqSubj, a.Name)
	r := acc.sl.Match(subj)
	if noOutSideInterest(sc, r) {
		return
	}
	reply := fmt.Sprintf(connsRespSubj, s.info.ID)
	m := accNumConnsReq{Account: a.Name}
	s.sendInternalMsg(r, subj, reply, &m.Server, &m)
}

// FIXME(dlc) - make configurable.
const AccountConnHBInterval = 30 * time.Second

// sendAccConnsUpdate is called to send out our information on the
// account's local connections.
// Lock should be held on entry.
func (s *Server) sendAccConnsUpdate(a *Account, subj string) {
	if !s.eventsEnabled() || a == nil || a == s.sys.account || a == s.gacc {
		return
	}
	acc := s.sys.account
	sc := s.sys.client

	r := acc.sl.Match(subj)
	if noOutSideInterest(sc, r) {
		return
	}

	a.mu.Lock()
	// If no limits set, don't update, no need to.
	if a.mconns == 0 {
		a.mu.Unlock()
		return
	}
	// Build event with account name and number of local clients.
	m := accNumConns{
		Account: a.Name,
		Conns:   len(a.clients),
	}
	// Check to see if we have an HB running and update.
	if a.ctmr == nil {
		a.etmr = time.AfterFunc(AccountConnHBInterval, func() { s.accConnsUpdate(a) })
	} else {
		a.etmr.Reset(AccountConnHBInterval)
	}
	a.mu.Unlock()

	s.sendInternalMsg(r, subj, "", &m.Server, &m)
}

// accConnsUpdate is called whenever there is a change to the account's
// number of active connections, or during a heartbeat.
func (s *Server) accConnsUpdate(a *Account) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.eventsEnabled() || a == nil || a == s.sys.account {
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
	acc := s.sys.account
	sc := s.sys.client
	s.mu.Unlock()

	subj := fmt.Sprintf(connectEventSubj, c.acc.Name)
	r := acc.sl.Match(subj)
	if noOutSideInterest(sc, r) {
		return
	}

	c.mu.Lock()
	m := ConnectEventMsg{
		Client: ClientInfo{
			Start:   c.start,
			Host:    c.host,
			ID:      c.cid,
			Account: c.acc.Name,
			User:    nameForClient(c),
			Name:    c.opts.Name,
			Lang:    c.opts.Lang,
			Version: c.opts.Version,
		},
	}
	c.mu.Unlock()

	s.mu.Lock()
	s.sendInternalMsg(r, subj, "", &m.Server, &m)
	s.mu.Unlock()
}

// accountDisconnectEvent will send an account client disconnect event if there is interest.
// This is a billing event.
func (s *Server) accountDisconnectEvent(c *client, now time.Time, reason string) {
	s.mu.Lock()
	if !s.eventsEnabled() {
		s.mu.Unlock()
		return
	}
	acc := s.sys.account
	sc := s.sys.client
	s.mu.Unlock()

	subj := fmt.Sprintf(disconnectEventSubj, c.acc.Name)
	r := acc.sl.Match(subj)
	if noOutSideInterest(sc, r) {
		return
	}

	c.mu.Lock()
	m := DisconnectEventMsg{
		Client: ClientInfo{
			Start:   c.start,
			Stop:    &now,
			Host:    c.host,
			ID:      c.cid,
			Account: c.acc.Name,
			User:    nameForClient(c),
			Name:    c.opts.Name,
			Lang:    c.opts.Lang,
			Version: c.opts.Version,
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

	s.mu.Lock()
	s.sendInternalMsg(r, subj, "", &m.Server, &m)
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
		return nil, fmt.Errorf("Undefined message handler")
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
	c.unsubscribe(acc, sub, true)
}

func noOutSideInterest(sc *client, r *SublistResult) bool {
	if sc == nil || r == nil {
		return true
	}
	nsubs := len(r.psubs) + len(r.qsubs)
	if nsubs == 0 {
		return true
	}
	// We will always be no-echo but will determine that on delivery.
	// Here we try to avoid generating the payload if there is only us.
	// We only check normal subs. If we introduce queue subs into the
	// internal subscribers we should add in the check.
	for _, sub := range r.psubs {
		if sub.client != sc {
			return false
		}
	}
	return true
}

func (c *client) flushClients() {
	last := time.Now()
	for cp := range c.pcd {
		// Queue up a flush for those in the set
		cp.mu.Lock()
		// Update last activity for message delivery
		cp.last = last
		cp.out.fsp--
		cp.flushSignal()
		cp.mu.Unlock()
		delete(c.pcd, cp)
	}
}

func nameForClient(c *client) string {
	if c.user != nil {
		return c.user.Nkey
	}
	return "N/A"
}
