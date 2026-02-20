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

// raftTransport is an interface that defines the communication
// mechanism for Raft nodes.
type raftTransport interface {
	// Node returns the RaftNode associated with this transport.
	Node() RaftNode

	// Account returns the NATS Account this transport operates within.
	Account() *Account

	// Reset reconfigures the transport for a new account.
	// This involves tearing down existing client resources and
	// setting up new ones for the provided account.
	Reset(acc *Account)

	// Close shuts down the transport, releasing any associated resources
	// like internal clients and subscriptions.
	Close()

	// Publish sends a message to the specified subject.
	Publish(subject string, reply string, msg []byte)

	// Subscribe creates a subscription for to the specified subject.
	Subscribe(subject string, cb msgHandler) (*subscription, error)

	// Unsubscribe removes a previously established subscription.
	Unsubscribe(sub *subscription)
}

type raftTransportFactory func(*Server, RaftNode) raftTransport

// defaultTransport is the default implementation of the raftTransport interface.
// It uses an internal NATS client to allow communication between Raft nodes.
type defaultTransport struct {
	n   RaftNode
	s   *Server
	c   *client
	sq  *sendq
	acc *Account
}

func defaultRaftTransport(server *Server, raft RaftNode) raftTransport {
	return &defaultTransport{s: server, n: raft}
}

// Node returns the RaftNode associated with this transport.
func (t *defaultTransport) Node() RaftNode {
	return t.n
}

func (t *defaultTransport) Account() *Account {
	return t.acc
}

func (t *defaultTransport) Reset(acc *Account) {
	t.Close()

	t.c = t.s.createInternalSystemClient()
	t.c.registerWithAccount(acc)
	if acc.sq == nil {
		acc.sq = t.s.newSendQ(acc)
	}
	t.sq = acc.sq
	t.acc = acc
}

func (t *defaultTransport) Close() {
	if c := t.c; c != nil {
		c.mu.Lock()
		subs := make([]*subscription, 0, len(c.subs))
		for _, sub := range c.subs {
			subs = append(subs, sub)
		}
		c.mu.Unlock()
		for _, sub := range subs {
			t.Unsubscribe(sub)
		}
		c.closeConnection(InternalClient)
		t.c = nil
	}
}

func (t *defaultTransport) Publish(subject string, reply string, msg []byte) {
	t.sq.send(subject, reply, nil, msg)
}

func (t *defaultTransport) Subscribe(subject string, cb msgHandler) (*subscription, error) {
	if t.c == nil {
		return nil, errNoInternalClient
	}
	return t.s.systemSubscribe(subject, _EMPTY_, false, t.c, cb)
}

func (t *defaultTransport) Unsubscribe(sub *subscription) {
	if t.c != nil && sub != nil {
		t.c.processUnsub(sub.sid)
	}
}
