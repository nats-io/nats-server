// Copyright 2021-2023 The NATS Authors
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
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNRGSimple(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()
	// Do several state transitions.
	rg.randomMember().(*stateAdder).proposeDelta(11)
	rg.randomMember().(*stateAdder).proposeDelta(22)
	rg.randomMember().(*stateAdder).proposeDelta(33)
	// Wait for all members to have the correct state.
	rg.waitOnTotal(t, 66)
}

func TestNRGSnapshotAndRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	var expectedTotal int64

	leader := rg.leader().(*stateAdder)
	sm := rg.nonLeader().(*stateAdder)

	for i := 0; i < 1000; i++ {
		delta := rand.Int63n(222)
		expectedTotal += delta
		leader.proposeDelta(delta)

		if i == 250 {
			// Let some things catchup.
			time.Sleep(50 * time.Millisecond)
			// Snapshot leader and stop and snapshot a member.
			leader.snapshot(t)
			sm.snapshot(t)
			sm.stop()
		}
	}
	// Restart.
	sm.restart()
	// Wait for all members to have the correct state.
	rg.waitOnTotal(t, expectedTotal)
}

func TestNRGAppendEntryEncode(t *testing.T) {
	ae := &appendEntry{
		term:   1,
		pindex: 0,
	}

	// Test leader should be _EMPTY_ or exactly idLen long
	ae.leader = "foo_bar_baz"
	_, err := ae.encode(nil)
	require_Error(t, err, errLeaderLen)

	// Empty ok (noLeader)
	ae.leader = noLeader // _EMPTY_
	_, err = ae.encode(nil)
	require_NoError(t, err)

	ae.leader = "DEREK123"
	_, err = ae.encode(nil)
	require_NoError(t, err)

	// Buffer reuse
	var rawSmall [32]byte
	var rawBigger [64]byte

	b := rawSmall[:]
	ae.encode(b)
	if b[0] != 0 {
		t.Fatalf("Expected arg buffer to not be used")
	}
	b = rawBigger[:]
	ae.encode(b)
	if b[0] == 0 {
		t.Fatalf("Expected arg buffer to be used")
	}

	// Test max number of entries.
	for i := 0; i < math.MaxUint16+1; i++ {
		ae.entries = append(ae.entries, &Entry{EntryNormal, nil})
	}
	_, err = ae.encode(b)
	require_Error(t, err, errTooManyEntries)
}

func TestNRGAppendEntryDecode(t *testing.T) {
	ae := &appendEntry{
		leader: "12345678",
		term:   1,
		pindex: 0,
	}
	for i := 0; i < math.MaxUint16; i++ {
		ae.entries = append(ae.entries, &Entry{EntryNormal, nil})
	}
	buf, err := ae.encode(nil)
	require_NoError(t, err)

	// Truncate buffer first.
	var node *raft
	short := buf[0 : len(buf)-1024]
	_, err = node.decodeAppendEntry(short, nil, _EMPTY_)
	require_Error(t, err, errBadAppendEntry)

	for i := 0; i < 100; i++ {
		b := copyBytes(buf)
		// modifying the header (idx < 42) will not result in an error by decodeAppendEntry
		bi := 42 + rand.Intn(len(b)-42)
		if b[bi] != 0 && bi != 40 {
			b[bi] = 0
			_, err = node.decodeAppendEntry(b, nil, _EMPTY_)
			require_Error(t, err, errBadAppendEntry)
		}
	}
}

func TestNRGRecoverFromFollowingNoLeader(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// Find out what term we are on.
	term := rg.leader().node().Term()

	// Start by pausing all of the nodes. This will stop them from
	// processing new entries.
	for _, n := range rg {
		n.node().PauseApply()
	}

	// Now drain all of the ApplyQ entries from them, which will stop
	// them from automatically trying to follow a previous leader if
	// they happened to have received an apply entry from one. Then
	// we're going to force them into a state where they are all
	// followers but they don't have a leader.
	for _, n := range rg {
		rn := n.node().(*raft)
		rn.ApplyQ().drain()
		rn.switchToFollower("")
	}

	// Resume the nodes.
	for _, n := range rg {
		n.node().ResumeApply()
	}

	// Wait a while. The nodes should notice that they haven't heard
	// from a leader lately and will switch to voting. After an
	// election we should find a new leader and be on a new term.
	rg.waitOnLeader()
	require_True(t, rg.leader() != nil)
	require_NotEqual(t, rg.leader().node().Term(), term)
}

func TestNRGObserverMode(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// Put all of the followers into observer mode. In this state
	// they will not participate in an election but they will continue
	// to apply incoming commits.
	for _, n := range rg {
		if n.node().Leader() {
			continue
		}
		n.node().SetObserver(true)
	}

	// Propose a change from the leader.
	adder := rg.leader().(*stateAdder)
	adder.proposeDelta(1)
	adder.proposeDelta(2)
	adder.proposeDelta(3)

	// Wait for the followers to apply it.
	rg.waitOnTotal(t, 6)

	// Confirm the followers are still just observers and weren't
	// reset out of that state for some reason.
	for _, n := range rg {
		if n.node().Leader() {
			continue
		}
		require_True(t, n.node().IsObserver())
	}
}

// TestNRGSimpleElection tests that a simple election succeeds. It is
// simple because the group hasn't processed any entries and hasn't
// suffered any interruptions of any kind, therefore there should be
// no way that the conditions for granting the votes can fail.
func TestNRGSimpleElection(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 9)
	defer c.shutdown()
	c.waitOnLeader()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 9, newStateAdder)
	rg.waitOnLeader()

	voteReqs := make(chan *nats.Msg, 1)
	voteResps := make(chan *nats.Msg, len(rg)-1)

	// Keep a record of the term when we started.
	leader := rg.leader().node().(*raft)
	startTerm := leader.term

	// Subscribe to the vote request subject, this should be the
	// same across all nodes in the group.
	_, err := nc.ChanSubscribe(leader.vsubj, voteReqs)
	require_NoError(t, err)

	// Subscribe to all of the vote response inboxes for all nodes
	// in the Raft group, as they can differ.
	for _, n := range rg {
		rn := n.node().(*raft)
		_, err = nc.ChanSubscribe(rn.vreply, voteResps)
		require_NoError(t, err)
	}

	// Step down, this will start a new voting session.
	require_NoError(t, rg.leader().node().StepDown())

	// Wait for a vote request to come in.
	msg := require_ChanRead(t, voteReqs, time.Second)
	vr := decodeVoteRequest(msg.Data, msg.Reply)
	require_True(t, vr != nil)
	require_NotEqual(t, vr.candidate, "")

	// The leader should have bumped their term in order to start
	// an election.
	require_Equal(t, vr.term, startTerm+1)
	require_Equal(t, vr.lastTerm, startTerm)

	// Wait for all of the vote responses to come in. There should
	// be as many vote responses as there are followers.
	for i := 0; i < len(rg)-1; i++ {
		msg := require_ChanRead(t, voteResps, time.Second)
		re := decodeVoteResponse(msg.Data)
		require_True(t, re != nil)

		// The vote should have been granted.
		require_Equal(t, re.granted, true)

		// The node granted the vote, therefore the term in the vote
		// response should have advanced as well.
		require_Equal(t, re.term, vr.term)
		require_Equal(t, re.term, startTerm+1)
	}

	// Everyone in the group should have voted for our candidate
	// and arrived at the term from the vote request.
	for _, n := range rg {
		rn := n.node().(*raft)
		require_Equal(t, rn.term, vr.term)
		require_Equal(t, rn.term, startTerm+1)
		require_Equal(t, rn.vote, vr.candidate)
	}
}

func TestRaftChainOneBlockInLockstep(t *testing.T) {
	const iterations = 50
	const timeout = 15 * time.Second
	//RaftChainOptions.verbose = true

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newRaftChainStateMachine)
	rg.waitOnLeader()

	for iteration := uint64(1); iteration <= iterations; iteration++ {
		rg.leader().(*raftChainStateMachine).proposeBlock()

		// Wait on participants to converge
		var previousNodeName string
		var previousNodeHash string

		for _, sm := range rg {
			stateMachine := sm.(*raftChainStateMachine)
			nodeName := fmt.Sprintf(
				"%s/%s",
				stateMachine.server().Name(),
				stateMachine.node().ID(),
			)
			checkFor(t, timeout, 500*time.Millisecond, func() error {
				running, blocksCount, currentHash := stateMachine.getCurrentHash()
				// All nodes always running
				if !running {
					return fmt.Errorf(
						"node %s is not running",
						nodeName,
					)
				}
				// Node is behind
				if blocksCount != iteration {
					return fmt.Errorf(
						"node %s applied %d blocks out of %d expected",
						nodeName,
						blocksCount,
						iteration,
					)
				}
				// Make sure hash is not empty
				if currentHash == "" {
					return fmt.Errorf(
						"node %s has empty hash after applying %d blocks",
						nodeName,
						blocksCount,
					)
				}
				// Check against previous node hash, unless this is the first node to be checked
				if previousNodeHash != "" && previousNodeHash != currentHash {
					return fmt.Errorf(
						"hash mismatch after %d blocks: %s hash: %s != %s hash: %s",
						iteration,
						nodeName,
						currentHash,
						previousNodeName,
						previousNodeHash,
					)
				}
				// Set node name and hash for next node to compare against
				previousNodeName, previousNodeHash = nodeName, currentHash
				// All is well
				return nil
			})
		}
		t.Logf(
			"Verified chain hash %s for %d/%d nodes after %d/%d iterations",
			previousNodeHash,
			len(rg),
			len(rg),
			iteration,
			iterations,
		)
	}
}

func TestRaftChainStopAndCatchUp(t *testing.T) {
	const iterations = 50
	const blocksPerIteration = 3
	const timeout = 15 * time.Second
	//RaftChainOptions.verbose = true

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newRaftChainStateMachine)
	rg.waitOnLeader()

	for iteration := uint64(1); iteration <= iterations; iteration++ {

		// Stop a (non-leader) node
		stoppedNode := rg.nonLeader()
		stoppedNode.stop()

		t.Logf(
			"Iteration %d/%d: stopping node: %s/%s",
			iteration,
			iterations,
			stoppedNode.server().Name(),
			stoppedNode.node().ID(),
		)

		// Propose some new blocks
		for i := 0; i < blocksPerIteration; i++ {
			rg.leader().(*raftChainStateMachine).proposeBlock()
		}

		// Restart the stopped node
		stoppedNode.restart()

		// Wait on participants to converge
		var previousNodeName string
		var previousNodeHash string
		expectedBlocks := iteration * blocksPerIteration
		for _, sm := range rg {
			stateMachine := sm.(*raftChainStateMachine)
			nodeName := fmt.Sprintf(
				"%s/%s",
				stateMachine.server().Name(),
				stateMachine.node().ID(),
			)
			checkFor(t, timeout, 500*time.Millisecond, func() error {
				running, blocksCount, currentHash := stateMachine.getCurrentHash()
				// All nodes should be running
				if !running {
					return fmt.Errorf(
						"node %s not running",
						nodeName,
					)
				}
				// Node is behind
				if blocksCount != expectedBlocks {
					return fmt.Errorf(
						"node %s applied %d blocks out of %d expected",
						nodeName,
						blocksCount,
						expectedBlocks,
					)
				}
				// Make sure hash is not empty
				if currentHash == "" {
					return fmt.Errorf(
						"node %s has empty hash after applying %d blocks",
						nodeName,
						blocksCount,
					)
				}
				// Check against previous node hash, unless this is the first node to be checked
				if previousNodeHash != "" && previousNodeHash != currentHash {
					return fmt.Errorf(
						"hash mismatch after %d blocks: %s hash: %s != %s hash: %s",
						expectedBlocks,
						nodeName,
						currentHash,
						previousNodeName,
						previousNodeHash,
					)
				}
				// Set node name and hash for next node to compare against
				previousNodeName, previousNodeHash = nodeName, currentHash
				// All is well
				return nil
			})
		}
		t.Logf(
			"Verified chain hash %s for %d/%d nodes after %d blocks, %d/%d iterations",
			previousNodeHash,
			len(rg),
			len(rg),
			expectedBlocks,
			iteration,
			iterations,
		)
	}
}

func FuzzRaftChain(f *testing.F) {
	const (
		groupName               = "FUZZ_TEST_RAFT_CHAIN"
		numPeers                = 3
		checkConvergenceTimeout = 30 * time.Second
	)

	RaftChainOptions.verbose = true

	// Cases to run when executed as unit test:
	//f.Add(100, int64(123456))
	f.Add(1000, int64(123456))

	// Run in Fuzz mode to repeat maximizing coverage and looking for failing cases
	// notice that this test execution is not perfectly deterministic!
	// The same seed may not fail on retry.
	f.Fuzz(
		func(t *testing.T, iterations int, rngSeed int64) {
			rng := rand.New(rand.NewSource(rngSeed))

			c := createJetStreamClusterExplicit(t, "R3S", numPeers)
			defer c.shutdown()

			rg := c.createRaftGroup(groupName, numPeers, newRaftChainStateMachine)
			rg.waitOnLeader()

			// Manually track active and stopped nodes
			activeNodes := make([]stateMachine, 0, numPeers)
			stoppedNodes := make([]stateMachine, 0, numPeers)

			// Initially all are active
			activeNodes = append(activeNodes, rg...)

			// Available operations
			type RaftFuzzTestOperation string

			const (
				StopOne       RaftFuzzTestOperation = "Stop one active node"
				StopAll                             = "Stop all active nodes"
				RestartOne                          = "Restart one stopped node"
				RestartAll                          = "Restart all stopped nodes"
				Snapshot                            = "Snapshot one active node"
				Propose                             = "Propose a value via one active node"
				ProposeLeader                       = "Propose a value via leader"
				Pause                               = "Let things run undisturbed for a while"
				Check                               = "Wait for nodes to converge"
			)

			// Weighted distribution of operations, one is randomly chosen from this vector in each iteration
			opsWeighted := []RaftFuzzTestOperation{
				StopOne,
				StopAll,
				RestartOne,
				RestartOne,
				RestartAll,
				RestartAll,
				RestartAll,
				Snapshot,
				Snapshot,
				Propose,
				Propose,
				Propose,
				Propose,
				Propose,
				Propose,
				ProposeLeader,
				ProposeLeader,
				ProposeLeader,
				ProposeLeader,
				ProposeLeader,
				ProposeLeader,
				Pause,
				Pause,
				Pause,
				Pause,
				Pause,
				Pause,
				Check,
				Check,
				Check,
				Check,
			}

			pickRandomNode := func(nodes []stateMachine) ([]stateMachine, stateMachine) {
				if len(nodes) == 0 {
					// Input list is empty
					return nodes, nil
				}
				// Pick random node
				i := rng.Intn(len(nodes))
				node := nodes[i]
				// Move last element in its place
				nodes[i] = nodes[len(nodes)-1]
				// Return slice excluding last element
				return nodes[:len(nodes)-1], node
			}

			chainStatusString := func() string {
				b := strings.Builder{}
				for _, sm := range rg {
					csm := sm.(*raftChainStateMachine)
					running, blocksCount, blockHash := csm.getCurrentHash()
					if running {
						b.WriteString(
							fmt.Sprintf(
								" [%s (%s): %d blocks, hash=%s],",
								csm.server().Name(),
								csm.node().ID(),
								blocksCount,
								blockHash,
							),
						)
					} else {
						b.WriteString(
							fmt.Sprintf(
								" [%s (%s): STOPPED],",
								csm.server().Name(),
								csm.node().ID(),
							),
						)

					}
				}
				return b.String()
			}

			// Track the highest number of blocks applied by any of the replicas
			highestBlocksCount := uint64(0)

			for iteration := 1; iteration <= iterations; iteration++ {
				nextOperation := opsWeighted[rng.Intn(len(opsWeighted))]
				t.Logf("State: %s", chainStatusString())
				t.Logf("Iteration %d/%d: %s", iteration, iterations, nextOperation)

				switch nextOperation {

				case StopOne:
					// Stop an active node (if any are left active)
					var n stateMachine
					activeNodes, n = pickRandomNode(activeNodes)
					if n != nil {
						n.stop()
						stoppedNodes = append(stoppedNodes, n)
					}

				case StopAll:
					// Stop any node which is active
					for _, node := range activeNodes {
						node.stop()
					}
					stoppedNodes = append(stoppedNodes, activeNodes...)
					activeNodes = make([]stateMachine, 0, numPeers)

				case RestartOne:
					// Restart a stopped node (if any are stopped)
					var n stateMachine
					stoppedNodes, n = pickRandomNode(stoppedNodes)
					if n != nil {
						n.restart()
						activeNodes = append(activeNodes, n)
					}

				case RestartAll:
					// Restart any node which is stopped
					for _, node := range stoppedNodes {
						node.restart()
					}
					activeNodes = append(activeNodes, stoppedNodes...)
					stoppedNodes = make([]stateMachine, 0, numPeers)

				case Snapshot:
					// Make an active node take a snapshot (if any nodes are active)
					if len(activeNodes) > 0 {
						n := activeNodes[rng.Intn(len(activeNodes))]
						n.(*raftChainStateMachine).snapshot()
					}

				case Propose:
					// Make an active node propose the next block (if any nodes are active)
					if len(activeNodes) > 0 {
						n := activeNodes[rng.Intn(len(activeNodes))]
						n.(*raftChainStateMachine).proposeBlock()
					}

				case ProposeLeader:
					// Make the leader propose the next block (if a leader is active)
					leader := rg.leader()
					if leader != nil {
						leader.(*raftChainStateMachine).proposeBlock()
					}

				case Pause:
					// Just sit for a while and let things happen
					time.Sleep(time.Duration(rng.Intn(250)) * time.Millisecond)

				case Check:
					// Restart any stopped node
					for _, node := range stoppedNodes {
						node.restart()
					}
					activeNodes = append(activeNodes, stoppedNodes...)
					stoppedNodes = make([]stateMachine, 0, numPeers)

					// Ensure all nodes (eventually) converge
					checkFor(
						t,
						checkConvergenceTimeout,
						1*time.Second,
						func() error {
							referenceRunning, referenceBlocksCount, referenceHash := rg[0].(*raftChainStateMachine).getCurrentHash()
							if !referenceRunning {
								return fmt.Errorf(
									"reference node not running",
								)
							}
							for _, n := range rg {
								sm := n.(*raftChainStateMachine)
								running, blocksCount, blockHash := sm.getCurrentHash()
								if !running {
									return fmt.Errorf(
										"node not running: %s (%s)",
										sm.server().Name(),
										sm.node().ID(),
									)
								}
								// Track the highest block seen
								if blocksCount > highestBlocksCount {
									t.Logf(
										"New highest blocks count: %d (%s (%s))",
										blocksCount,
										sm.s.Name(),
										sm.n.ID(),
									)
									highestBlocksCount = blocksCount
								}
								// Each replica must match the reference node (given enough time)
								if blocksCount != referenceBlocksCount || blockHash != referenceHash {
									return fmt.Errorf(
										"nodes not converged: %s",
										chainStatusString(),
									)
								}
							}
							// Replicas are in sync, but missing some blocks that was previously seen
							if referenceBlocksCount < highestBlocksCount {
								return fmt.Errorf(
									"nodes converged below highest known block count: %d: %s",
									highestBlocksCount,
									chainStatusString(),
								)
							}
							// All nodes reached the same state, check passed
							return nil
						},
					)
				}
			}
		},
	)
}
