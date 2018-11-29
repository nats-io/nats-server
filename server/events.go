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
	"time"
)

const (
	connectEventSubj    = "$SYS.%s.CLIENT.CONNECT"
	disconnectEventSubj = "$SYS.%s.CLIENT.DISCONNECT"
)

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

// This will send a message.
// TODO(dlc) - Note we want the sequence numbers to be serialized but right now they may not be.
func (s *Server) sendInternalMsg(r *SublistResult, subj string, msg []byte) {
	if s.sys == nil {
		return
	}
	c := s.sys.client
	acc := s.sys.account

	// Prep internl structures needed to send message.
	c.pa.subject = []byte(subj)
	c.pa.size = len(msg)
	c.pa.szb = []byte(strconv.FormatInt(int64(len(msg)), 10))
	// Add in NL
	msg = append(msg, _CRLF_...)

	// Check to see if we need to map/route to another account.
	if acc.imports.services != nil {
		c.checkForImportServices(acc, msg)
	}
	c.processMsgResults(acc, r, msg, []byte(subj), nil, nil)
	c.flushClients()
}

// accountConnectEvent will send an account client connect event if there is interest.
func (s *Server) accountConnectEvent(c *client) {
	if s.sys == nil || s.sys.client == nil || s.sys.account == nil {
		return
	}
	acc := s.sys.account

	subj := fmt.Sprintf(connectEventSubj, c.acc.Name)
	r := acc.sl.Match(subj)
	if s.noOutSideInterest(r) {
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

	s.stampServerInfo(&m.Server)
	msg, _ := json.MarshalIndent(m, "", "  ")
	s.sendInternalMsg(r, subj, msg)
}

// accountDisconnectEvent will send an account client disconnect event if there is interest.
func (s *Server) accountDisconnectEvent(c *client, now time.Time, reason string) {
	if s.sys == nil || s.sys.client == nil || s.sys.account == nil {
		return
	}
	acc := s.sys.account

	subj := fmt.Sprintf(disconnectEventSubj, c.acc.Name)
	r := acc.sl.Match(subj)
	if s.noOutSideInterest(r) {
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

	s.stampServerInfo(&m.Server)
	msg, _ := json.MarshalIndent(m, "", "  ")
	s.sendInternalMsg(r, subj, msg)
}

// Internal message callback. If the msg is needed past the callback it is
// required to be copied.
type msgHandler func(sub *subscription, subject, reply string, msg []byte)

func (s *Server) deliverInternalMsg(sub *subscription, subject, reply, msg []byte) {
	if s.sys == nil {
		return
	}
	s.mu.Lock()
	cb := s.sys.subs[string(sub.sid)]
	s.mu.Unlock()
	if cb != nil {
		cb(sub, string(subject), string(reply), msg)
	}
}

// Create an internal subscription. No support for queue groups atm.
func (s *Server) sysSubscribe(subject string, cb msgHandler) (*subscription, error) {
	if s.sys == nil {
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
	if sub == nil || s.sys == nil {
		return
	}
	s.mu.Lock()
	acc := s.sys.account
	c := s.sys.client
	delete(s.sys.subs, string(sub.sid))
	s.mu.Unlock()
	c.unsubscribe(acc, sub, true)
}

func (s *Server) noOutSideInterest(r *SublistResult) bool {
	sc := s.sys.client
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
	return false
}

func (s *Server) stampServerInfo(si *ServerInfo) {
	s.mu.Lock()
	si.ID = s.info.ID
	si.Seq = s.sys.seq
	si.Host = s.info.Host
	s.sys.seq++
	s.mu.Unlock()
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
