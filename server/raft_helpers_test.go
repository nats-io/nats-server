// Copyright 2023 The NATS Authors
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

// Do not exlude this file with the !skip_js_tests since those helpers
// are also used by MQTT.

package server

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type stateMachine interface {
	server() *Server
	node() RaftNode
	// This will call forward as needed so can be called on any node.
	propose(data []byte)
	// When entries have been committed and can be applied.
	applyEntry(ce *CommittedEntry)
	// When a leader change happens.
	leaderChange(isLeader bool)
	// Stop the raft group.
	stop()
	// Restart
	restart()
}

// Factory function needed for constructor.
type smFactory func(s *Server, cfg *RaftConfig, node RaftNode) stateMachine

type smGroup []stateMachine

// Leader of the group.
func (sg smGroup) leader() stateMachine {
	for _, sm := range sg {
		if sm.node().Leader() {
			return sm
		}
	}
	return nil
}

// Wait on a leader to be elected.
func (sg smGroup) waitOnLeader() {
	expires := time.Now().Add(10 * time.Second)
	for time.Now().Before(expires) {
		for _, sm := range sg {
			if sm.node().Leader() {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Pick a random member.
func (sg smGroup) randomMember() stateMachine {
	return sg[rand.Intn(len(sg))]
}

// Return a non-leader
func (sg smGroup) nonLeader() stateMachine {
	for _, sm := range sg {
		if !sm.node().Leader() {
			return sm
		}
	}
	return nil
}

// Create a raft group and place on numMembers servers at random.
func (c *cluster) createRaftGroup(name string, numMembers int, smf smFactory) smGroup {
	c.t.Helper()
	if numMembers > len(c.servers) {
		c.t.Fatalf("Members > Peers: %d vs  %d", numMembers, len(c.servers))
	}
	servers := append([]*Server{}, c.servers...)
	rand.Shuffle(len(servers), func(i, j int) { servers[i], servers[j] = servers[j], servers[i] })
	return c.createRaftGroupWithPeers(name, servers[:numMembers], smf)
}

func (c *cluster) createRaftGroupWithPeers(name string, servers []*Server, smf smFactory) smGroup {
	c.t.Helper()

	var sg smGroup
	var peers []string

	for _, s := range servers {
		// generate peer names.
		s.mu.RLock()
		peers = append(peers, s.sys.shash)
		s.mu.RUnlock()
	}

	for _, s := range servers {
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: c.t.TempDir(), BlockSize: defaultMediumBlockSize, AsyncFlush: false, SyncInterval: 5 * time.Minute},
			StreamConfig{Name: name, Storage: FileStorage},
		)
		require_NoError(c.t, err)
		cfg := &RaftConfig{Name: name, Store: c.t.TempDir(), Log: fs}
		s.bootstrapRaftNode(cfg, peers, true)
		n, err := s.startRaftNode(globalAccountName, cfg, pprofLabels{})
		require_NoError(c.t, err)
		sm := smf(s, cfg, n)
		sg = append(sg, sm)
		go smLoop(sm)
	}
	return sg
}

// Driver program for the state machine.
// Should be run in its own go routine.
func smLoop(sm stateMachine) {
	s, n := sm.server(), sm.node()
	qch, lch, aq := n.QuitC(), n.LeadChangeC(), n.ApplyQ()

	for {
		select {
		case <-s.quitCh:
			return
		case <-qch:
			return
		case <-aq.ch:
			ces := aq.pop()
			for _, ce := range ces {
				sm.applyEntry(ce)
			}
			aq.recycle(&ces)

		case isLeader := <-lch:
			sm.leaderChange(isLeader)
		}
	}
}

// Simple implementation of a replicated state.
// The adder state just sums up int64 values.
type stateAdder struct {
	sync.Mutex
	s   *Server
	n   RaftNode
	cfg *RaftConfig
	sum int64
	lch chan bool
}

// Simple getters for server and the raft node.
func (a *stateAdder) server() *Server {
	a.Lock()
	defer a.Unlock()
	return a.s
}
func (a *stateAdder) node() RaftNode {
	a.Lock()
	defer a.Unlock()
	return a.n
}

func (a *stateAdder) propose(data []byte) {
	a.Lock()
	defer a.Unlock()
	a.n.ForwardProposal(data)
}

func (a *stateAdder) applyEntry(ce *CommittedEntry) {
	a.Lock()
	defer a.Unlock()
	if ce == nil {
		// This means initial state is done/replayed.
		return
	}
	for _, e := range ce.Entries {
		if e.Type == EntryNormal {
			delta, _ := binary.Varint(e.Data)
			a.sum += delta
		} else if e.Type == EntrySnapshot {
			a.sum, _ = binary.Varint(e.Data)
		}
	}
	// Update applied.
	a.n.Applied(ce.Index)
}

func (a *stateAdder) leaderChange(isLeader bool) {
	select {
	case a.lch <- isLeader:
	default:
	}
}

// Adder specific to change the total.
func (a *stateAdder) proposeDelta(delta int64) {
	data := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(data, int64(delta))
	a.propose(data[:n])
}

// Stop the group.
func (a *stateAdder) stop() {
	a.Lock()
	defer a.Unlock()
	a.n.Stop()
}

// Restart the group
func (a *stateAdder) restart() {
	a.Lock()
	defer a.Unlock()

	if a.n.State() != Closed {
		return
	}

	// The filestore is stopped as well, so need to extract the parts to recreate it.
	rn := a.n.(*raft)
	fs := rn.wal.(*fileStore)

	var err error
	a.cfg.Log, err = newFileStore(fs.fcfg, fs.cfg.StreamConfig)
	if err != nil {
		panic(err)
	}
	a.n, err = a.s.startRaftNode(globalAccountName, a.cfg, pprofLabels{})
	if err != nil {
		panic(err)
	}
	// Finally restart the driver.
	go smLoop(a)
}

// Total for the adder state machine.
func (a *stateAdder) total() int64 {
	a.Lock()
	defer a.Unlock()
	return a.sum
}

// Install a snapshot.
func (a *stateAdder) snapshot(t *testing.T) {
	a.Lock()
	defer a.Unlock()
	data := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(data, a.sum)
	snap := data[:n]
	require_NoError(t, a.n.InstallSnapshot(snap))
}

// Helper to wait for a certain state.
func (rg smGroup) waitOnTotal(t *testing.T, expected int64) {
	t.Helper()
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		for _, sm := range rg {
			asm := sm.(*stateAdder)
			if total := asm.total(); total != expected {
				return fmt.Errorf("Adder on %v has wrong total: %d vs %d",
					asm.server(), total, expected)
			}
		}
		return nil
	})
}

// Factory function.
func newStateAdder(s *Server, cfg *RaftConfig, n RaftNode) stateMachine {
	return &stateAdder{s: s, n: n, cfg: cfg, lch: make(chan bool, 1)}
}
