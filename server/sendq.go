// Copyright 2020-2021 The NATS Authors
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
	"time"
)

type outMsg struct {
	subj string
	rply string
	hdr  []byte
	msg  []byte
	next *outMsg
}

type sendq struct {
	mu   sync.Mutex
	mch  chan struct{}
	head *outMsg
	tail *outMsg
	s    *Server
}

func (s *Server) newSendQ() *sendq {
	sq := &sendq{s: s, mch: make(chan struct{}, 1)}
	s.startGoRoutine(sq.internalLoop)
	return sq
}

func (sq *sendq) internalLoop() {
	sq.mu.Lock()
	s, mch := sq.s, sq.mch
	sq.mu.Unlock()

	defer s.grWG.Done()

	c := s.createInternalSystemClient()
	c.registerWithAccount(s.SystemAccount())
	c.noIcb = true

	defer c.closeConnection(ClientClosed)

	for s.isRunning() {
		select {
		case <-s.quitCh:
			return
		case <-mch:
			for pm := sq.pending(); pm != nil; {
				c.pa.subject = []byte(pm.subj)
				c.pa.size = len(pm.msg) + len(pm.hdr)
				c.pa.szb = []byte(strconv.Itoa(c.pa.size))
				c.pa.reply = []byte(pm.rply)
				var msg []byte
				if len(pm.hdr) > 0 {
					c.pa.hdr = len(pm.hdr)
					c.pa.hdb = []byte(strconv.Itoa(c.pa.hdr))
					msg = append(pm.hdr, pm.msg...)
					msg = append(msg, _CRLF_...)
				} else {
					c.pa.hdr = -1
					c.pa.hdb = nil
					msg = append(pm.msg, _CRLF_...)
				}
				c.processInboundClientMsg(msg)
				c.pa.szb = nil
				// Do this here to nil out below vs up in for loop.
				next := pm.next
				pm.next, pm.hdr, pm.msg = nil, nil, nil
				if pm = next; pm == nil {
					pm = sq.pending()
				}
			}
			c.flushClients(10 * time.Millisecond)
		}
	}
}

func (sq *sendq) pending() *outMsg {
	sq.mu.Lock()
	head := sq.head
	sq.head, sq.tail = nil, nil
	sq.mu.Unlock()
	return head
}

func (sq *sendq) send(subj, rply string, hdr, msg []byte) {
	out := &outMsg{subj, rply, nil, nil, nil}
	// We will copy these for now.
	if len(hdr) > 0 {
		hdr = append(hdr[:0:0], hdr...)
		out.hdr = hdr
	}
	if len(msg) > 0 {
		msg = append(msg[:0:0], msg...)
		out.msg = msg
	}

	sq.mu.Lock()
	var notify bool
	if sq.head == nil {
		sq.head = out
		notify = true
	} else {
		sq.tail.next = out
	}
	sq.tail = out
	sq.mu.Unlock()

	if notify {
		select {
		case sq.mch <- struct{}{}:
		default:
		}
	}
}
