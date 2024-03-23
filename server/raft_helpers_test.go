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
	"encoding"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"math/rand"
	"path/filepath"
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
		baseDir := filepath.Join(c.t.TempDir(), fmt.Sprintf("%s-%s", name, s.Name()))
		logDir := filepath.Join(baseDir, "log")
		storeDir := filepath.Join(baseDir, "store")
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: logDir, BlockSize: defaultMediumBlockSize, AsyncFlush: false, SyncInterval: 5 * time.Minute},
			StreamConfig{Name: name, Storage: FileStorage},
		)
		require_NoError(c.t, err)
		cfg := &RaftConfig{Name: name, Store: storeDir, Log: fs}
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

func (a *stateAdder) leaderChange(isLeader bool) {}

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
	return &stateAdder{s: s, n: n, cfg: cfg}
}

var RaftChainOptions = struct {
	verbose      bool
	maxBlockSize int
}{
	false,
	25,
}

// Simple implementation of a replicated state machine on top of RAFT.
// Hash each value delivered on top of existing hash
// All replicas should go through the same sequence of block hashes
type raftChainStateMachine struct {
	sync.Mutex
	s                          *Server
	n                          RaftNode
	cfg                        *RaftConfig
	leader                     bool
	proposalSequence           uint64
	rng                        *rand.Rand
	hash                       hash.Hash
	blocksApplied              uint64
	blocksAppliedSinceSnapshot uint64
	stopped                    bool
	ready                      bool
	safeSnapshots              bool
}

// Block is just a random array of bytes, but contains a little extra metadata to track its source
type ChainBlock struct {
	Proposer         string
	ProposerSequence uint64
	Data             []byte
}

func (sm *raftChainStateMachine) logDebug(format string, args ...any) {
	if RaftChainOptions.verbose {
		fmt.Printf("["+sm.s.Name()+" ("+sm.n.ID()+")] "+format+"\n", args...)
	}
}

func (sm *raftChainStateMachine) server() *Server {
	sm.Lock()
	defer sm.Unlock()
	return sm.s
}

func (sm *raftChainStateMachine) node() RaftNode {
	sm.Lock()
	defer sm.Unlock()
	return sm.n
}

func (sm *raftChainStateMachine) propose(data []byte) {
	sm.Lock()
	defer sm.Unlock()
	if !sm.ready {
		sm.logDebug("Refusing to propose during recovery")
	}
	err := sm.n.ForwardProposal(data)
	if err != nil {
		sm.logDebug("block proposal error: %s", err)
	}
}

func (sm *raftChainStateMachine) applyEntry(ce *CommittedEntry) {
	sm.Lock()
	defer sm.Unlock()
	if ce == nil {
		// A nil CE is a signal the previous recovery backlog is over
		sm.logDebug("Recovery complete")
		sm.ready = true
		return
	}
	sm.logDebug("Apply entries #%d (%d entries)", ce.Index, len(ce.Entries))
	for _, entry := range ce.Entries {
		if entry.Type == EntryNormal {
			sm.applyBlock(entry.Data)
		} else if entry.Type == EntrySnapshot {
			sm.loadSnapshot(entry.Data)
		} else {
			panic(fmt.Sprintf("[%s] unknown entry type: %s", sm.s.Name(), entry.Type))
		}
	}
	sm.n.Applied(ce.Index)
}

func (sm *raftChainStateMachine) leaderChange(isLeader bool) {
	if sm.leader && !isLeader {
		sm.logDebug("Leader change: no longer leader")
	} else if sm.leader && isLeader {
		sm.logDebug("Elected leader while already leader")
	} else if !sm.leader && isLeader {
		sm.logDebug("Leader change: i am leader")
	} else {
		sm.logDebug("Leader change")
	}
	sm.leader = isLeader
	if isLeader != sm.node().Leader() {
		sm.logDebug("⚠️ Leader state out of sync with underlying node")
	}
}

func (sm *raftChainStateMachine) stop() {
	sm.Lock()
	defer sm.Unlock()
	sm.n.Stop()

	// Clear state, on restart it will be recovered from snapshot or peers
	sm.stopped = true
	sm.blocksApplied = 0
	sm.hash.Reset()
	sm.leader = false
	sm.logDebug("Stopped")
}

func (sm *raftChainStateMachine) restart() {
	sm.Lock()
	defer sm.Unlock()

	sm.logDebug("Restarting")

	sm.stopped = false
	sm.ready = false
	if sm.n.State() != Closed {
		return
	}

	// The filestore is stopped as well, so need to extract the parts to recreate it.
	rn := sm.n.(*raft)
	fs := rn.wal.(*fileStore)

	var err error
	sm.cfg.Log, err = newFileStore(fs.fcfg, fs.cfg.StreamConfig)
	if err != nil {
		panic(err)
	}
	sm.n, err = sm.s.startRaftNode(globalAccountName, sm.cfg, pprofLabels{})
	if err != nil {
		panic(err)
	}
	// Finally restart the driver.
	go smLoop(sm)
}

func (sm *raftChainStateMachine) proposeBlock() {
	// Track how many blocks this replica proposed
	sm.proposalSequence += 1
	// Create a block
	block := ChainBlock{
		Proposer:         sm.s.Name(),
		ProposerSequence: sm.proposalSequence,
		Data:             make([]byte, sm.rng.Intn(20)+1),
	}
	// Data is random bytes
	sm.rng.Read(block.Data)
	// Serialize as JSON
	blockData, err := json.Marshal(block)
	if err != nil {
		panic(fmt.Sprintf("serialization error: %s", err))
	}
	sm.logDebug(
		"Proposing block <%s, %d, [%dB]>",
		block.Proposer,
		block.ProposerSequence,
		len(block.Data),
	)

	// Propose (may silently fail if this replica is not leader, or other reasons)
	sm.propose(blockData)
}

func (sm *raftChainStateMachine) applyBlock(data []byte) {
	// Deserialize block received in JSON format
	var block ChainBlock
	err := json.Unmarshal(data, &block)
	if err != nil {
		panic(fmt.Sprintf("deserialization error: %s", err))
	}
	sm.logDebug("Applying block <%s, %d>", block.Proposer, block.ProposerSequence)

	// Hash the data on top of the existing running hash
	n, err := sm.hash.Write(block.Data)
	if n != len(block.Data) {
		panic(fmt.Sprintf("unexpected checksum written %d data block size: %d", n, len(block.Data)))
	} else if err != nil {
		panic(fmt.Sprintf("checksum error: %s", err))
	}

	// Track block number
	sm.blocksApplied += 1
	sm.blocksAppliedSinceSnapshot += 1

	sm.logDebug("Hash after %d blocks: %X ", sm.blocksApplied, sm.hash.Sum(nil))
}

func (sm *raftChainStateMachine) getCurrentHash() (bool, uint64, string) {
	sm.Lock()
	defer sm.Unlock()

	// Return running, the number of blocks applied and the current running hash
	return !sm.stopped, sm.blocksApplied, fmt.Sprintf("%X", sm.hash.Sum(nil))
}

type chainHashSnapshot struct {
	SourceNode  string
	HashData    []byte
	BlocksCount uint64
}

func (sm *raftChainStateMachine) snapshot() {
	sm.Lock()
	defer sm.Unlock()

	if sm.blocksAppliedSinceSnapshot == 0 {
		sm.logDebug("Skip snapshot, no new entries")
		return
	}

	if sm.safeSnapshots && !sm.ready {
		sm.logDebug("Skip snapshot, still recovering")
		return
	}

	sm.logDebug(
		"Snapshot (with %d blocks applied, %d since last snapshot)",
		sm.blocksApplied,
		sm.blocksAppliedSinceSnapshot,
	)

	// Serialize the internal state of the hash block
	serializedHash, err := sm.hash.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		panic(fmt.Sprintf("failed to marshal hash: %s", err))
	}

	// Create snapshot
	snapshot := chainHashSnapshot{
		SourceNode:  fmt.Sprintf("%s (%s)", sm.s.Name(), sm.n.ID()),
		HashData:    serializedHash,
		BlocksCount: sm.blocksApplied,
	}

	// Serialize snapshot as JSON
	snapshotData, err := json.Marshal(snapshot)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal snapshot: %s", err))
	}

	// Install it as byte array
	err = sm.n.InstallSnapshot(snapshotData)
	if err != nil {
		panic(fmt.Sprintf("failed to snapshot: %s", err))
	}

	// Reset counter since last snapshot
	sm.blocksAppliedSinceSnapshot = 0
}

func (sm *raftChainStateMachine) loadSnapshot(data []byte) {
	// Deserialize snapshot from JSON
	var snapshot chainHashSnapshot
	err := json.Unmarshal(data, &snapshot)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal snapshot: %s", err))
	}

	sm.logDebug(
		"Applying snapshot (created by %s) taken after %d blocks",
		snapshot.SourceNode,
		snapshot.BlocksCount,
	)

	// Load internal hash block state
	err = sm.hash.(encoding.BinaryUnmarshaler).UnmarshalBinary(snapshot.HashData)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal hash data: %s", err))
	}

	// Load block counter
	sm.blocksApplied = snapshot.BlocksCount
	sm.blocksAppliedSinceSnapshot = 0

	sm.logDebug("Hash after snapshot with %d blocks: %X ", sm.blocksApplied, sm.hash.Sum(nil))
}

// Factory function to create RaftChainStateMachine on top of the given server/node
func newRaftChainStateMachine(s *Server, cfg *RaftConfig, n RaftNode) stateMachine {
	// Create RNG seed based on server name and node id
	var seed int64
	for _, c := range []byte(s.Name()) {
		seed += int64(c)
	}
	for _, c := range []byte(n.ID()) {
		seed += int64(c)
	}
	rng := rand.New(rand.NewSource(seed))

	// Initialize empty hash block
	hashBlock := crc32.NewIEEE()

	// Set to true to make RCSM ignore snapshot requests during 'recovery'.
	// i.e. after a restart and before a nil commit entry is consumed from the transitions queue.
	var safeSnapshots bool

	return &raftChainStateMachine{s: s, n: n, cfg: cfg, rng: rng, hash: hashBlock, safeSnapshots: safeSnapshots}
}
