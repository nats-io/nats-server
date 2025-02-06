// Copyright 2020-2024 The NATS Authors
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
	"strconv"
	"sync"
)

type outMsg struct {
	subj string
	rply string
	hdr  []byte
	msg  []byte
}

type sendq struct {
	mu sync.Mutex
	q  *ipQueue[*outMsg]
	s  *Server
	a  *Account
}

func (s *Server) newSendQ(acc *Account) *sendq {
	sq := &sendq{s: s, q: newIPQueue[*outMsg](s, "SendQ"), a: acc}
	s.startGoRoutine(sq.internalLoop)
	return sq
}

func (sq *sendq) internalLoop() {
	sq.mu.Lock()
	s, q := sq.s, sq.q
	sq.mu.Unlock()

	defer s.grWG.Done()

	c := s.createInternalSystemClient()
	c.registerWithAccount(sq.a)
	c.noIcb = true

	defer c.closeConnection(ClientClosed)

	// To optimize for not converting a string to a []byte slice.
	var (
		subj [256]byte
		rply [256]byte
		szb  [10]byte
		hdb  [10]byte
		_msg [4096]byte
		msg  = _msg[:0]
	)

	for s.isRunning() {
		select {
		case <-s.quitCh:
			return
		case <-q.ch:
			pms := q.pop()
			for _, pm := range pms {
				c.pa.subject = append(subj[:0], pm.subj...)
				c.pa.size = len(pm.msg) + len(pm.hdr)
				c.pa.szb = append(szb[:0], strconv.Itoa(c.pa.size)...)
				if len(pm.rply) > 0 {
					c.pa.reply = append(rply[:0], pm.rply...)
				} else {
					c.pa.reply = nil
				}
				msg = msg[:0]
				if len(pm.hdr) > 0 {
					c.pa.hdr = len(pm.hdr)
					c.pa.hdb = append(hdb[:0], strconv.Itoa(c.pa.hdr)...)
					msg = append(msg, pm.hdr...)
					msg = append(msg, pm.msg...)
					msg = append(msg, _CRLF_...)
				} else {
					c.pa.hdr = -1
					c.pa.hdb = nil
					msg = append(msg, pm.msg...)
					msg = append(msg, _CRLF_...)
				}
				c.processInboundClientMsg(msg)
				c.pa.szb = nil
				outMsgPool.Put(pm)
			}
			// TODO: should this be in the for-loop instead?
			c.flushClients(0)
			q.recycle(&pms)
		}
	}
}

var outMsgPool = sync.Pool{
	New: func() any {
		return &outMsg{}
	},
}

func (sq *sendq) send(subj, rply string, hdr, msg []byte) {
	if sq == nil {
		return
	}
	out := outMsgPool.Get().(*outMsg)
	out.subj, out.rply = subj, rply
	out.hdr = append(out.hdr[:0], hdr...)
	out.msg = append(out.msg[:0], msg...)
	sq.q.push(out)
}
