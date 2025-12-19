// Copyright 2025 The NATS Authors
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
	"sync"
)

type msgHook func(subject, reply string, msg []byte)

type raftTransportHub struct {
	mu         sync.Mutex
	transports map[string]*mockTransport
	partitions map[string]int
	afterMsg   msgHook
}

func (h *raftTransportHub) register(t *mockTransport) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.transports[t.Node().ID()] = t
}

func (h *raftTransportHub) unregister(t *mockTransport) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.transports, t.Node().ID())
}

func (h *raftTransportHub) partition(nodeID string, partitionID int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.partitions[nodeID] = partitionID
}

func (h *raftTransportHub) heal(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.partitions, nodeID)
}

func (h *raftTransportHub) healPartitions() {
	h.mu.Lock()
	defer h.mu.Unlock()
	clear(h.partitions)
}

func (h *raftTransportHub) setAfterMsgHook(hook msgHook) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.afterMsg = hook
}

func (h *raftTransportHub) publish(t *mockTransport, subject, reply string, msg []byte) {
	h.mu.Lock()
	afterMsgHook := h.afterMsg
	sender := t.Node().ID()
	partition := h.partitions[sender]

	for id, transport := range h.transports {
		if sender == id {
			continue
		}

		if partition != h.partitions[id] {
			continue
		}

		res := transport.sub.Match(subject)
		for _, sub := range res.psubs {
			sub.icb(sub, nil, transport.acc, subject, reply, msg)
		}
	}
	h.mu.Unlock()

	if afterMsgHook != nil {
		afterMsgHook(subject, reply, msg)
	}
}

type mockTransport struct {
	server *Server
	node   RaftNode
	hub    *raftTransportHub
	sub    *Sublist
	acc    *Account
}

func mockTransportFactory() (*raftTransportHub, raftTransportFactory) {
	h := &raftTransportHub{
		partitions: make(map[string]int),
		transports: make(map[string]*mockTransport),
		afterMsg:   nil,
	}
	return h, func(s *Server, n RaftNode) raftTransport {
		return &mockTransport{
			server: s,
			node:   n,
			hub:    h,
		}
	}
}

func (t *mockTransport) Reset(acc *Account) {
	t.Close()
	t.acc = acc
	t.sub = NewSublist(false)
	t.hub.register(t)
}

func (t *mockTransport) Close() {
	t.hub.unregister(t)
	t.sub = nil
}

func (t *mockTransport) Node() RaftNode {
	return t.node
}

func (t *mockTransport) Account() *Account {
	return t.acc
}

func (t *mockTransport) Publish(subject string, reply string, msg []byte) {
	t.hub.publish(t, subject, reply, msg)
}

func (t *mockTransport) Subscribe(subject string, cb msgHandler) (*subscription, error) {
	if t.sub == nil {
		return nil, errNoInternalClient
	}
	sub := &subscription{subject: []byte(subject), sid: nil, icb: cb}
	if err := t.sub.Insert(sub); err != nil {
		return nil, err
	}
	return sub, nil
}

func (t *mockTransport) Unsubscribe(sub *subscription) {
	if t.sub != nil {
		t.sub.Remove(sub)
	}
}
