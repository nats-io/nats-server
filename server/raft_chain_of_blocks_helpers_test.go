// Copyright 2024-2025 The NATS Authors
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

// Raft Chain of Blocks (RCOB) is a replicated state machine on top of Raft used to test the latter.
// Each value ("block") proposed to raft is a simple array of bytes (stateless, can be small or large).
// Each replica state consists of a hash of all blocks delivered so far.
// This makes it easy to check consistency over time.
// If all replicas deliver the same blocks in the same order (the whole point of Raft!), then the N-th hash on each
// replica should be identical. This invariant is easy to verify in test settings, because if at any point a replica
// delivered (hashed) a different value, then all subsequent hashes will diverge from the rest.

package server

import (
	"encoding"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"math/rand"
	"sync"
)

// Static options (but may be useful to tweak when debugging)
var RCOBOptions = struct {
	verbose bool
	// Proposed block [data] size is random between 1 and maxBlockSize
	maxBlockSize int
	// If set to true, the state machine will not take snapshots while in recovery
	// (corresponding to the 'ready' state machine member variable)
	safeSnapshots bool
}{
	false,
	10240,
	true,
}

// Simple state machine on top of Raft.
// Each value delivered is hashed, N-th value hash should match on all replicas or something went wrong.
// If even a single value in the chain of hash differs on one replica, hashes will diverge.
type RCOBStateMachine struct {
	sync.Mutex
	s                          *Server
	n                          RaftNode
	cfg                        *RaftConfig
	wg                         sync.WaitGroup
	leader                     bool
	proposalSequence           uint64
	rng                        *rand.Rand
	hash                       hash.Hash
	blocksApplied              uint64
	blocksAppliedSinceSnapshot uint64
	stopped                    bool
	ready                      bool
}

func (sm *RCOBStateMachine) waitGroup() *sync.WaitGroup {
	sm.Lock()
	defer sm.Unlock()
	return &sm.wg
}

// RCOBBlock is the structure of values replicated through Raft.
// Block data is a random array of bytes.
// Additional proposer metadata is present for debugging and for further ordering invariant checks (but not included
// in the hash).
type RCOBBlock struct {
	Proposer         string
	ProposerSequence uint64
	Data             []byte
}

// logDebug is for fine-grained event logging, useful for debugging but default off
func (sm *RCOBStateMachine) logDebug(format string, args ...any) {
	if RCOBOptions.verbose {
		fmt.Printf("["+sm.s.Name()+" ("+sm.n.ID()+")] "+format+"\n", args...)
	}
}

func (sm *RCOBStateMachine) server() *Server {
	sm.Lock()
	defer sm.Unlock()
	return sm.s
}

func (sm *RCOBStateMachine) node() RaftNode {
	sm.Lock()
	defer sm.Unlock()
	return sm.n
}

func (sm *RCOBStateMachine) propose(data []byte) {
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

func (sm *RCOBStateMachine) applyEntry(ce *CommittedEntry) {
	sm.Lock()
	defer sm.Unlock()
	if ce == nil {
		// A nil entry signals that the previous recovery backlog is over
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
	// Signal to the node that entries were applied
	sm.n.Applied(ce.Index)
}

func (sm *RCOBStateMachine) leaderChange(isLeader bool) {
	sm.Lock()
	defer sm.Unlock()
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
	if isLeader != sm.n.Leader() {
		sm.logDebug("⚠️ Leader state out of sync with underlying node")
	}
}

func (sm *RCOBStateMachine) stop() {
	n, wg := sm.node(), sm.waitGroup()
	n.Stop()
	n.WaitForStop()
	wg.Wait()

	// Clear state, on restart it will be recovered from snapshot or peers
	sm.Lock()
	defer sm.Unlock()

	sm.stopped = true
	sm.blocksApplied = 0
	sm.hash.Reset()
	sm.leader = false
	sm.logDebug("Stopped")
}

func (sm *RCOBStateMachine) restart() {
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

func (sm *RCOBStateMachine) proposeBlock() {
	// Keep track how many blocks this replica proposed
	sm.proposalSequence += 1

	// Create a block
	blockSize := 1 + sm.rng.Intn(RCOBOptions.maxBlockSize)
	block := RCOBBlock{
		Proposer:         sm.s.Name(),
		ProposerSequence: sm.proposalSequence,
		Data:             make([]byte, blockSize),
	}

	// Data is random bytes
	sm.rng.Read(block.Data)

	// Serialize block to JSON
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

	// Propose block (may fail, and that's ok)
	sm.propose(blockData)
}

func (sm *RCOBStateMachine) applyBlock(data []byte) {
	// Deserialize block
	var block RCOBBlock
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

	// Track numbers of blocks applied
	sm.blocksApplied += 1
	sm.blocksAppliedSinceSnapshot += 1

	sm.logDebug("Hash after %d blocks: %X ", sm.blocksApplied, sm.hash.Sum(nil))
}

func (sm *RCOBStateMachine) getCurrentHash() (bool, uint64, string) {
	sm.Lock()
	defer sm.Unlock()

	// Return running, the number of blocks applied and the current running hash
	return !sm.stopped, sm.blocksApplied, fmt.Sprintf("%X", sm.hash.Sum(nil))
}

// RCOBSnapshot structure for RCOB snapshots
// Consists of the hash (32b) after BlocksCount blocks were hashed.
// TODO: Could start storing last N blocks to make snapshot larger and exercise more code paths.
// (a big chunk of random data would also do the trick)
type RCOBSnapshot struct {
	SourceNode  string
	HashData    []byte
	BlocksCount uint64
}

// createSnapshot asks the state machine to create a snapshot.
// The request may be ignored if no blocks were applied since the last snapshot.
func (sm *RCOBStateMachine) createSnapshot() {
	sm.Lock()
	defer sm.Unlock()

	if sm.blocksAppliedSinceSnapshot == 0 {
		sm.logDebug("Skip snapshot, no new entries")
		return
	}

	if RCOBOptions.safeSnapshots && !sm.ready {
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
	snapshot := RCOBSnapshot{
		SourceNode:  fmt.Sprintf("%s (%s)", sm.s.Name(), sm.n.ID()),
		HashData:    serializedHash,
		BlocksCount: sm.blocksApplied,
	}

	// Serialize snapshot as JSON
	snapshotData, err := json.Marshal(snapshot)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal snapshot: %s", err))
	}

	// InstallSnapshot is actually "save the snapshot", which is an operation delegated to the node
	err = sm.n.InstallSnapshot(snapshotData)
	if err != nil {
		sm.logDebug("failed to snapshot: %s", err)
		return
	}

	// Reset counter since last snapshot
	sm.blocksAppliedSinceSnapshot = 0
}

func (sm *RCOBStateMachine) loadSnapshot(data []byte) {
	// Deserialize snapshot from JSON
	var snapshot RCOBSnapshot
	err := json.Unmarshal(data, &snapshot)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal snapshot: %s", err))
	}

	sm.logDebug(
		"Applying snapshot (created by %s) taken after %d blocks",
		snapshot.SourceNode,
		snapshot.BlocksCount,
	)

	// Load internal hash state
	err = sm.hash.(encoding.BinaryUnmarshaler).UnmarshalBinary(snapshot.HashData)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal hash data: %s", err))
	}

	// Load block counter
	sm.blocksApplied = snapshot.BlocksCount

	// Reset number of blocks applied since snapshot
	sm.blocksAppliedSinceSnapshot = 0

	sm.logDebug("Hash after snapshot with %d blocks: %X ", sm.blocksApplied, sm.hash.Sum(nil))
}

// Factory function to create state machine on top of the given server/node
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

	return &RCOBStateMachine{s: s, n: n, cfg: cfg, rng: rng, hash: hashBlock}
}
