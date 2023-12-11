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
	"math"
	"math/rand"
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
	rg.randomMember().(*stateAdder).proposeDelta(11)
	rg.randomMember().(*stateAdder).proposeDelta(-22)
	// Wait for all members to have the correct state.
	rg.waitOnTotal(t, 0)
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

		// The new term hasn't started yet, so the vote responses
		// should contain the term from before the election. It is
		// possible that candidates are listening to this to work
		// out if they are in previous terms.
		require_Equal(t, re.term, vr.lastTerm)
		require_Equal(t, re.term, startTerm)

		// The vote should have been granted.
		require_Equal(t, re.granted, true)
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
