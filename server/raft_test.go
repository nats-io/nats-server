// Copyright 2021-2025 The NATS Authors
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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
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
	rg.randomMember().(*stateAdder).proposeDelta(22)
	rg.randomMember().(*stateAdder).proposeDelta(-11)
	rg.randomMember().(*stateAdder).proposeDelta(-10)
	// Wait for all members to have the correct state.
	rg.waitOnTotal(t, 1)
}

func TestNRGSnapshotAndRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	lsm := rg.waitOnLeader()
	require_NotNil(t, lsm)
	leader := lsm.(*stateAdder)

	var expectedTotal int64

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
		rn.switchToFollower(noLeader)
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

func TestNRGInlineStepdown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// When StepDown() completes, we should not be the leader. Before,
	// this would not be guaranteed as the stepdown could be processed
	// some time later.
	n := rg.leader().node().(*raft)
	require_NoError(t, n.StepDown())
	require_NotEqual(t, n.State(), Leader)
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

func TestNRGAEFromOldLeader(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// Listen out for catchup requests.
	ch := make(chan *nats.Msg, 16)
	_, err := nc.ChanSubscribe(fmt.Sprintf(raftCatchupReply, ">"), ch)
	require_NoError(t, err)

	// Start next term so that we can reuse term 1 in the next step.
	leader := rg.leader().node().(*raft)
	leader.StepDown()
	time.Sleep(time.Millisecond * 100)
	rg.waitOnLeader()
	require_Equal(t, leader.Term(), 2)
	leader = rg.leader().node().(*raft)

	// Send an append entry with an outdated term. Beforehand, doing
	// so would have caused a WAL reset and then would have triggered
	// a Raft-level catchup.
	ae := &appendEntry{
		term:   1,
		pindex: 0,
		leader: leader.id,
		reply:  nc.NewRespInbox(),
	}
	payload, err := ae.encode(nil)
	require_NoError(t, err)
	resp, err := nc.Request(leader.asubj, payload, time.Second)
	require_NoError(t, err)

	// Wait for the response, the server should have rejected it.
	ar := leader.decodeAppendEntryResponse(resp.Data)
	require_NotNil(t, ar)
	require_Equal(t, ar.success, false)

	// No catchup should happen at this point because no reset should
	// have happened.
	require_NoChanRead(t, ch, time.Second*2)
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
	require_NotEqual(t, vr.candidate, _EMPTY_)

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

		// Ignore old vote responses that could be in-flight.
		if re.term < vr.term {
			i--
			continue
		}

		// The vote should have been granted.
		require_Equal(t, re.granted, true)

		// The node granted the vote, therefore the term in the vote
		// response should have advanced as well.
		require_Equal(t, re.term, vr.term)
		require_Equal(t, re.term, startTerm+1)
	}

	// Everyone in the group should have voted for our candidate
	// and arrived at the term from the vote request.
	rg.lockAll()
	defer rg.unlockAll()
	for _, n := range rg {
		rn := n.node().(*raft)
		require_Equal(t, rn.term, vr.term)
		require_Equal(t, rn.term, startTerm+1)
		require_Equal(t, rn.vote, vr.candidate)
	}
}

func TestNRGSwitchStateClearsQueues(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	s := c.servers[0] // RunBasicJetStreamServer not available

	n := &raft{
		prop:  newIPQueue[*proposedEntry](s, "prop"),
		resp:  newIPQueue[*appendEntryResponse](s, "resp"),
		leadc: make(chan bool, 1), // for switchState
	}
	n.state.Store(int32(Leader))
	require_Equal(t, n.prop.len(), 0)
	require_Equal(t, n.resp.len(), 0)

	n.prop.push(&proposedEntry{&Entry{}, _EMPTY_})
	n.resp.push(&appendEntryResponse{})
	require_Equal(t, n.prop.len(), 1)
	require_Equal(t, n.resp.len(), 1)

	n.switchState(Follower)
	require_Equal(t, n.prop.len(), 0)
	require_Equal(t, n.resp.len(), 0)
}

func TestNRGStepDownOnSameTermDoesntClearVote(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	c.waitOnLeader()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	lsm := rg.leader().(*stateAdder)
	leader := lsm.node().(*raft)
	follower := rg.nonLeader().node().(*raft)

	// Make sure we handle the leader change notification from above.
	require_ChanRead(t, lsm.lch, time.Second)

	// Subscribe to the append entry subject.
	sub, err := nc.SubscribeSync(leader.asubj)
	require_NoError(t, err)

	// Get the first append entry that we receive.
	msg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_NoError(t, sub.Unsubscribe())

	// We're going to modify the append entry that we received so that
	// we can send it again with modifications.
	ae, err := leader.decodeAppendEntry(msg.Data, nil, msg.Reply)
	require_NoError(t, err)

	// First of all we're going to try sending an append entry that
	// has an old term and a fake leader.
	msg.Reply = follower.areply
	ae.leader = follower.id
	ae.term = leader.term - 1
	msg.Data, err = ae.encode(msg.Data[:0])
	require_NoError(t, err)
	require_NoError(t, nc.PublishMsg(msg))

	// Because the term was old, the fake leader shouldn't matter as
	// the current leader should ignore it.
	require_NoChanRead(t, lsm.lch, time.Second)

	// Now we're going to send it on the same term that the current leader
	// is on. What we expect to happen is that the leader will step down
	// but it *shouldn't* clear the vote.
	ae.term = leader.term
	msg.Data, err = ae.encode(msg.Data[:0])
	require_NoError(t, err)
	require_NoError(t, nc.PublishMsg(msg))

	// Wait for the leader transition and ensure that the vote wasn't
	// cleared.
	require_ChanRead(t, lsm.lch, time.Second)
	require_NotEqual(t, leader.vote, noVote)
}

func TestNRGUnsuccessfulVoteRequestDoesntResetElectionTimer(t *testing.T) {
	// This test relies on nodes not hitting their election timer too often,
	// otherwise the step later where we capture the election time before and
	// after the failed vote request will flake.
	origMinTimeout, origMaxTimeout, origHBInterval := minElectionTimeout, maxElectionTimeout, hbInterval
	minElectionTimeout, maxElectionTimeout, hbInterval = time.Second*5, time.Second*10, time.Second*10
	defer func() {
		minElectionTimeout, maxElectionTimeout, hbInterval = origMinTimeout, origMaxTimeout, origHBInterval
	}()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	c.waitOnLeader()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)

	// Because the election timer is quite high, we want to kick a node into
	// campaigning before it naturally needs to, otherwise the test takes a
	// long time just to pick a leader.
	for _, n := range rg {
		n.node().Campaign()
		break
	}
	rg.waitOnLeader()
	rgLeader := rg.leader()
	leader := rgLeader.node().(*raft)
	follower := rg.nonLeader().node().(*raft)

	// Send one message to ensure heartbeats are not sent during the remainder of this test.
	rgLeader.(*stateAdder).proposeDelta(1)
	rg.waitOnTotal(t, 1)

	// Set up a new inbox for the vote responses to go to.
	vsubj, vreply := leader.vsubj, nc.NewInbox()
	ch := make(chan *nats.Msg, 3)
	_, err := nc.ChanSubscribe(vreply, ch)
	require_NoError(t, err)

	// Keep a track of the last time the election timer was reset before this.
	// Also build up a vote request that's obviously behind so that the other
	// nodes should not do anything with it. All locks are taken at the same
	// time so that it guarantees that both the leader and the follower aren't
	// operating at the time we take the etlr snapshots.
	rg.lockAll()
	leader.resetElect(maxElectionTimeout)
	follower.resetElect(maxElectionTimeout)
	leaderOriginal := leader.etlr
	followerOriginal := follower.etlr
	vr := &voteRequest{
		term:      follower.term - 1,
		lastTerm:  follower.term - 1,
		lastIndex: 0,
		candidate: follower.id,
	}
	rg.unlockAll()

	// Now send a vote request that's obviously behind.
	require_NoError(t, nc.PublishMsg(&nats.Msg{
		Subject: vsubj,
		Reply:   vreply,
		Data:    vr.encode(),
	}))

	// Wait for everyone to respond.
	require_ChanRead(t, ch, time.Second)
	require_ChanRead(t, ch, time.Second)
	require_ChanRead(t, ch, time.Second)

	// Neither the leader nor our chosen follower should have updated their
	// election timer as a result of this.
	rg.lockAll()
	leaderEqual := leaderOriginal.Equal(leader.etlr)
	followerEqual := followerOriginal.Equal(follower.etlr)
	rg.unlockAll()
	require_True(t, leaderEqual)
	require_True(t, followerEqual)
}

func TestNRGUnsuccessfulVoteRequestCampaignEarly(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats0 := "S1Nunr6R" // "nats-0"
	n.etlr = time.Time{}

	// Simple case: we are follower and vote for a candidate.
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 1, lastTerm: 0, lastIndex: 0, candidate: nats0}))
	require_Equal(t, n.term, 1)
	require_Equal(t, n.vote, nats0)
	require_NotEqual(t, n.etlr, time.Time{}) // Resets election timer as it voted.
	n.etlr = time.Time{}

	// We are follower and deny vote for outdated candidate.
	n.pterm, n.pindex = 1, 100
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 2, lastTerm: 1, lastIndex: 2, candidate: nats0}))
	require_Equal(t, n.term, 2)
	require_Equal(t, n.vote, noVote)
	require_NotEqual(t, n.etlr, time.Time{}) // Resets election timer as it starts campaigning.
	n.etlr = time.Time{}

	// Switch to candidate.
	n.pterm, n.pindex = 2, 200
	n.switchToCandidate()
	require_Equal(t, n.term, 3)
	require_Equal(t, n.State(), Candidate)
	require_NotEqual(t, n.etlr, time.Time{}) // Resets election timer as part of switching state.
	n.etlr = time.Time{}

	// We are candidate and deny vote for outdated candidate. But they were on a more recent term, restart campaign.
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 4, lastTerm: 1, lastIndex: 2, candidate: nats0}))
	require_Equal(t, n.term, 4)
	require_Equal(t, n.vote, noVote)
	require_NotEqual(t, n.etlr, time.Time{}) // Resets election timer as it restarts campaigning.
	n.etlr = time.Time{}

	// Switch to candidate.
	n.pterm, n.pindex = 4, 400
	n.switchToCandidate()
	require_Equal(t, n.term, 5)
	require_Equal(t, n.State(), Candidate)
	require_NotEqual(t, n.etlr, time.Time{}) // Resets election timer as part of switching state.
	n.etlr = time.Time{}

	// We are candidate and deny vote for outdated candidate. Don't start campaigning early.
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 5, lastTerm: 1, lastIndex: 2, candidate: nats0}))
	require_Equal(t, n.term, 5)
	require_Equal(t, n.vote, noVote)
	// Election timer must NOT be updated as that would mean another candidate that we don't vote
	// for can short-circuit us by making us restart elections, denying us the ability to become leader.
	require_Equal(t, n.etlr, time.Time{})
}

func TestNRGInvalidTAVDoesntPanic(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	c.waitOnLeader()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	leader := rg.waitOnLeader()
	require_NotNil(t, leader)

	// Mangle the TAV file to a short length (less than uint64).
	tav := filepath.Join(leader.node().(*raft).sd, termVoteFile)
	require_NoError(t, os.WriteFile(tav, []byte{1, 2, 3, 4}, 0644))

	// Restart the node.
	leader.stop()
	leader.restart()

	// Before the fix, a crash would have happened before this point.
	c.waitOnAllCurrent()
}

func TestNRGAssumeHighTermAfterCandidateIsolation(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	c.waitOnLeader()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// Bump the term up on one of the follower nodes by a considerable
	// amount and force it into the candidate state. This is what happens
	// after a period of time in isolation.
	follower := rg.nonLeader().node().(*raft)
	follower.Lock()
	follower.term += 100
	follower.switchState(Candidate)
	follower.Unlock()

	follower.requestVote()
	time.Sleep(time.Millisecond * 100)

	// The candidate will shortly send a vote request. When that happens,
	// the rest of the nodes in the cluster should move up to that term,
	// even though they will not grant the vote.
	nterm := follower.term
	for _, n := range rg {
		require_Equal(t, n.node().Term(), nterm)
	}

	// Have the leader send out a proposal, which will force the candidate
	// back into follower state.
	rg.waitOnLeader()
	rg.leader().(*stateAdder).proposeDelta(1)
	rg.waitOnTotal(t, 1)

	// The candidate should have switched to a follower on a term equal to
	// or newer than the candidate had.
	for _, n := range rg {
		require_NotEqual(t, n.node().State(), Candidate)
		require_True(t, n.node().Term() >= nterm)
	}
}

// Test to make sure this does not cause us to truncate our wal or enter catchup state.
func TestNRGHeartbeatOnLeaderChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	for i := 0; i < 10; i++ {
		// Restart the leader.
		leader := rg.leader().(*stateAdder)
		leader.proposeDelta(22)
		leader.proposeDelta(-11)
		leader.proposeDelta(-10)
		// Must observe forward progress, so each iteration will check +1 total.
		rg.waitOnTotal(t, int64(i+1))
		leader.stop()
		leader.restart()
		rg.waitOnLeader()
	}
}

func TestNRGElectionTimerAfterObserver(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	for _, n := range rg {
		n.node().SetObserver(true)
	}

	time.Sleep(maxElectionTimeout)
	before := time.Now()

	for _, n := range rg {
		n.node().SetObserver(false)
	}

	time.Sleep(maxCampaignTimeout)

	for _, n := range rg {
		rn := n.node().(*raft)
		rn.RLock()
		etlr := rn.etlr
		rn.RUnlock()
		require_True(t, etlr.After(before))
	}
}

func TestNRGSystemClientCleanupFromAccount(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()
	sacc := s.SystemAccount()

	numClients := func() int {
		sacc.mu.RLock()
		defer sacc.mu.RUnlock()
		return len(sacc.clients)
	}

	start := numClients()

	var all []smGroup
	for i := 0; i < 5; i++ {
		rgName := fmt.Sprintf("TEST-%d", i)
		rg := c.createRaftGroup(rgName, 3, newStateAdder)
		all = append(all, rg)
		rg.waitOnLeader()
	}
	for _, rg := range all {
		for _, sm := range rg {
			sm.node().Stop()
		}
		for _, sm := range rg {
			sm.node().WaitForStop()
		}
	}
	finish := numClients()
	require_Equal(t, start, finish)
}

func TestNRGLeavesObserverAfterPause(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	n := rg.nonLeader().node().(*raft)

	checkState := func(observer, pobserver bool) {
		t.Helper()
		n.RLock()
		defer n.RUnlock()
		require_Equal(t, n.observer, observer)
		require_Equal(t, n.pobserver, pobserver)
	}

	// Assume this has happened because of jetstream_cluster_migrate
	// or similar.
	n.SetObserver(true)
	checkState(true, false)

	// Now something like a catchup has started, but since we were
	// already in observer mode, pobserver is set to true.
	n.PauseApply()
	checkState(true, true)

	// Now jetstream_cluster_migrate is happy that the leafnodes are
	// back up so it tries to leave observer mode, but the catchup
	// hasn't finished yet. This will instead set pobserver to false.
	n.SetObserver(false)
	checkState(true, false)

	// The catchup finishes, so we should correctly leave the observer
	// state by setting observer to the pobserver value.
	n.ResumeApply()
	checkState(false, false)
}

func TestNRGCandidateDoesntRevertTermAfterOldAE(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// Bump the term up a few times.
	for i := 0; i < 3; i++ {
		rg.leader().node().StepDown()
		time.Sleep(time.Millisecond * 50) // Needed because stepdowns not synchronous
		rg.waitOnLeader()
	}

	leader := rg.leader().node().(*raft)
	follower := rg.nonLeader().node().(*raft)

	// Sanity check that we are where we expect to be.
	require_Equal(t, leader.term, 4)
	require_Equal(t, follower.term, 4)

	// At this point the active term is 4 and pterm is 4, force the
	// term up to 9. This won't bump the pterm.
	rg.lockAll()
	for _, n := range rg {
		n.node().(*raft).term += 5
	}
	rg.unlockAll()

	// Build an AE that has a term newer than the pterm but older than
	// the term. Give it to the follower in candidate state.
	ae := newAppendEntry(leader.id, 6, leader.commit, leader.pterm, leader.pindex, nil)
	follower.switchToCandidate()
	follower.processAppendEntry(ae, nil)

	// The candidate must not have reverted back to term 6.
	require_NotEqual(t, follower.term, 6)
}

func TestNRGTermDoesntRollBackToPtermOnCatchup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// Propose some entries so that we have entries in the log that have pterm 1.
	lsm := rg.leader().(*stateAdder)
	for i := 0; i < 5; i++ {
		lsm.proposeDelta(1)
		rg.waitOnTotal(t, int64(i)+1)
	}

	// Check that everyone is where they are supposed to be.
	rg.lockAll()
	for _, n := range rg {
		rn := n.node().(*raft)
		require_Equal(t, rn.term, 1)
		require_Equal(t, rn.pterm, 1)
		require_Equal(t, rn.pindex, 6)
	}
	rg.unlockAll()

	// Force a stepdown so that we move up to term 2.
	rg.leader().node().(*raft).switchToFollower(noLeader)
	rg.waitOnLeader()
	leader := rg.leader().node().(*raft)

	// Now make sure everyone has moved up to term 2. Additionally we're
	// going to prune back the follower logs to term 1 as this is what will
	// create the right conditions for the catchup.
	rg.lockAll()
	for _, n := range rg {
		rn := n.node().(*raft)
		require_Equal(t, rn.term, 2)

		if !rn.Leader() {
			rn.truncateWAL(1, 6)
			require_Equal(t, rn.term, 2) // rn.term must stay the same
			require_Equal(t, rn.pterm, 1)
			require_Equal(t, rn.pindex, 6)
		}
	}
	// This will make followers run a catchup.
	ae := newAppendEntry(leader.id, leader.term, leader.commit, leader.pterm, leader.pindex, nil)
	rg.unlockAll()

	arInbox := nc.NewRespInbox()
	arCh := make(chan *nats.Msg, 2)
	_, err := nc.ChanSubscribe(arInbox, arCh)
	require_NoError(t, err)

	// Ensure the subscription is known by the server we're connected to,
	// but also by the other servers in the cluster.
	require_NoError(t, nc.Flush())
	time.Sleep(100 * time.Millisecond)

	// In order to trip this condition, we need to send an append entry that
	// will trick the followers into running a catchup. In the process they
	// were setting the term back to pterm which is incorrect.
	b, err := ae.encode(nil)
	require_NoError(t, err)
	require_NoError(t, nc.PublishMsg(&nats.Msg{
		Subject: fmt.Sprintf(raftAppendSubj, "TEST"),
		Reply:   arInbox,
		Data:    b,
	}))

	// Wait for both followers to respond to the append entry and then verify
	// that none of the nodes should have reverted back to term 1.
	require_ChanRead(t, arCh, time.Second*5) // First follower
	require_ChanRead(t, arCh, time.Second*5) // Second follower
	for _, n := range rg {
		require_NotEqual(t, n.node().Term(), 1)
	}
}

func TestNRGNoResetOnAppendEntryResponse(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()
	c.waitOnAllCurrent()

	leader := rg.leader().node().(*raft)
	follower := rg.nonLeader().node().(*raft)
	lsm := rg.leader().(*stateAdder)

	// Subscribe for append entries that aren't heartbeats and respond to
	// each of them as though it's a non-success and with a higher term.
	// The higher term in this case is what would cause the leader previously
	// to reset the entire log which it shouldn't do.
	_, err := nc.Subscribe(fmt.Sprintf(raftAppendSubj, "TEST"), func(msg *nats.Msg) {
		if ae, err := follower.decodeAppendEntry(msg.Data, nil, msg.Reply); err == nil && len(ae.entries) > 0 {
			ar := newAppendEntryResponse(ae.term+1, ae.commit, follower.id, false)
			require_NoError(t, msg.Respond(ar.encode(nil)))
		}
	})
	require_NoError(t, err)

	// Generate an append entry that the subscriber above can respond to.
	c.waitOnAllCurrent()
	lsm.proposeDelta(5)
	rg.waitOnTotal(t, 5)

	// The was-leader should now have stepped down, make sure that it
	// didn't blow away its log in the process.
	rg.lockAll()
	defer rg.unlockAll()
	require_Equal(t, leader.State(), Follower)
	require_NotEqual(t, leader.pterm, 0)
	require_NotEqual(t, leader.pindex, 0)
}

func TestNRGCandidateDontStepdownDueToLeaderOfPreviousTerm(t *testing.T) {
	// This test relies on nodes not hitting their election timer too often.
	origMinTimeout, origMaxTimeout, origHBInterval := minElectionTimeout, maxElectionTimeout, hbInterval
	minElectionTimeout, maxElectionTimeout, hbInterval = time.Second*5, time.Second*10, time.Second
	defer func() {
		minElectionTimeout, maxElectionTimeout, hbInterval = origMinTimeout, origMaxTimeout, origHBInterval
	}()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	c.waitOnLeader()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	var (
		candidatePterm  uint64 = 50
		candidatePindex uint64 = 70
		candidateTerm   uint64 = 100
	)

	// Create a candidate that has received entries while they were a follower in a previous term
	candidate := rg.nonLeader().node().(*raft)
	candidate.Lock()
	candidate.switchState(Candidate)
	candidate.pterm = candidatePterm
	candidate.pindex = candidatePindex
	candidate.term = candidateTerm
	candidate.Unlock()

	// Leader term is behind candidate
	leader := rg.leader().node().(*raft)
	leader.Lock()
	leader.term = candidatePterm
	leader.pterm = candidatePterm
	leader.pindex = candidatePindex
	leader.Unlock()

	// Subscribe to the append entry subject.
	sub, err := nc.SubscribeSync(leader.asubj)
	require_NoError(t, err)

	// Get the first append entry that we receive, should be heartbeat from leader of prev term
	msg, err := sub.NextMsg(5 * time.Second)
	require_NoError(t, err)

	// Stop nodes from progressing so we can check state
	rg.lockAll()
	defer rg.unlockAll()

	// Decode the append entry
	ae, err := leader.decodeAppendEntry(msg.Data, nil, msg.Reply)
	require_NoError(t, err)

	// Check that the append entry is from the leader
	require_Equal(t, ae.leader, leader.id)
	// Check that it came from the leader before it updated its term with the response from the candidate
	require_Equal(t, ae.term, candidatePterm)

	// Check that the candidate hasn't stepped down
	require_Equal(t, candidate.State(), Candidate)
	// Check that the candidate's term is still ahead of the leader's term
	require_True(t, candidate.term > ae.term)
}

func TestNRGRemoveLeaderPeerDeadlockBug(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	n := rg.leader().node().(*raft)
	leader := n.ID()

	// Propose to remove the leader as a peer. Will lead to a deadlock with bug.
	require_NoError(t, n.ProposeRemovePeer(leader))
	rg.waitOnLeader()

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		nl := n.GroupLeader()
		if nl != leader {
			return nil
		}
		return errors.New("Leader has not moved")
	})
}

func TestNRGWALEntryWithoutQuorumMustTruncate(t *testing.T) {
	tests := []struct {
		title  string
		modify func(rg smGroup)
	}{
		{
			// state equals, only need to remove the entry
			title:  "equal",
			modify: func(rg smGroup) {},
		},
		{
			// state diverged, need to replace the entry
			title: "diverged",
			modify: func(rg smGroup) {
				rg.leader().(*stateAdder).proposeDelta(11)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			rg := c.createRaftGroup("TEST", 3, newStateAdder)
			rg.waitOnLeader()

			var err error
			var scratch [1024]byte

			// Simulate leader storing an AppendEntry in WAL but being hard killed before it can propose to its peers.
			n := rg.leader().node().(*raft)
			esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
			entries := []*Entry{newEntry(EntryNormal, esm)}
			n.Lock()
			ae := n.buildAppendEntry(entries)
			ae.buf, err = ae.encode(scratch[:])
			require_NoError(t, err)
			err = n.storeToWAL(ae)
			n.Unlock()
			require_NoError(t, err)

			// Stop the leader so it moves to another one.
			n.shutdown()

			// Wait for another leader to be picked
			rg.waitOnLeader()

			// Make a modification, specific to this test.
			test.modify(rg)

			// Restart the previous leader that contains the stored AppendEntry without quorum.
			for _, a := range rg {
				if a.node().ID() == n.ID() {
					sa := a.(*stateAdder)
					sa.restart()
					break
				}
			}

			// The previous leader's WAL should truncate to remove the AppendEntry only it has.
			// Eventually all WALs for all peers must match.
			checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
				var expected [][]byte
				for _, a := range rg {
					an := a.node().(*raft)
					var state StreamState
					an.wal.FastState(&state)
					if len(expected) > 0 && int(state.LastSeq-state.FirstSeq+1) != len(expected) {
						return fmt.Errorf("WAL is different: too many entries")
					}
					// Loop over all entries in the WAL, checking if the contents for all RAFT nodes are equal.
					for index := state.FirstSeq; index <= state.LastSeq; index++ {
						ae, err := an.loadEntry(index)
						if err != nil {
							return err
						}
						seq := int(index)
						if len(expected) < seq {
							expected = append(expected, ae.buf)
						} else if !bytes.Equal(expected[seq-1], ae.buf) {
							return fmt.Errorf("WAL is different: stored bytes differ")
						}
					}
				}
				return nil
			})
		})
	}
}

func TestNRGTermNoDecreaseAfterWALReset(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	l := rg.leader().node().(*raft)
	l.Lock()
	l.term = 20
	l.Unlock()

	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}
	l.Lock()
	ae := l.buildAppendEntry(entries)
	l.Unlock()

	for _, f := range rg {
		if f.node().ID() != l.ID() {
			fn := f.node().(*raft)
			fn.processAppendEntry(ae, fn.aesub)
			require_Equal(t, fn.term, 20) // Follower's term gets upped as expected.
		}
	}

	// Lower the term, simulating the followers receiving a message from an old term/leader.
	ae.term = 3
	for _, f := range rg {
		if f.node().ID() != l.ID() {
			fn := f.node().(*raft)
			fn.processAppendEntry(ae, fn.aesub)
			require_Equal(t, fn.term, 20) // Follower should reject and the term stays the same.

			fn.Lock()
			fn.resetWAL()
			fn.Unlock()
			fn.processAppendEntry(ae, fn.aesub)
			require_Equal(t, fn.term, 20) // Follower should reject again, even after reset, term stays the same.
		}
	}
}

func TestNRGPendingAppendEntryCacheInvalidation(t *testing.T) {
	for _, test := range []struct {
		title   string
		entries int
	}{
		{title: "empty", entries: 1},
		{title: "at limit", entries: paeDropThreshold},
		{title: "full", entries: paeDropThreshold + 1},
	} {
		t.Run(test.title, func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
			rg.waitOnLeader()
			l := rg.leader()

			l.(*stateAdder).proposeDelta(1)
			rg.waitOnTotal(t, 1)

			// Fill up the cache with N entries.
			// The contents don't matter as they should never be applied.
			rg.lockAll()
			for _, s := range rg {
				n := s.node().(*raft)
				for i := 0; i < test.entries; i++ {
					n.pae[n.pindex+uint64(1+i)] = newAppendEntry("", 0, 0, 0, 0, nil)
				}
			}
			rg.unlockAll()

			l.(*stateAdder).proposeDelta(1)
			rg.waitOnTotal(t, 2)
		})
	}
}

func TestNRGCatchupDoesNotTruncateUncommittedEntriesWithQuorum(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"

	// Timeline, for first leader
	aeInitial := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})
	aeUncommitted := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeNoQuorum := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 2, entries: entries})

	// Timeline, after leader change
	aeMissed := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 1, pindex: 2, entries: entries})
	aeCatchupTrigger := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 2, pindex: 3, entries: entries})
	aeHeartbeat2 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 2, pterm: 2, pindex: 4, entries: nil})
	aeHeartbeat3 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 4, pterm: 2, pindex: 4, entries: nil})

	// Initial case is simple, just store the entry.
	n.processAppendEntry(aeInitial, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Heartbeat, makes sure commit moves up.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 1)

	// We get one entry that has quorum (but we don't know that yet), so it stays uncommitted for a bit.
	n.processAppendEntry(aeUncommitted, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 2)
	entry, err = n.loadEntry(2)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// We get one entry that has NO quorum (but we don't know that yet).
	n.processAppendEntry(aeNoQuorum, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 3)
	entry, err = n.loadEntry(3)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// We've just had a leader election, and we missed one message from the previous leader.
	// We should truncate the last message.
	n.processAppendEntry(aeCatchupTrigger, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 2)
	require_True(t, n.catchup == nil)

	// We get a heartbeat that prompts us to catchup.
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 2)
	require_Equal(t, n.commit, 1) // Commit should not change, as we missed an item.
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 1)  // n.pterm
	require_Equal(t, n.catchup.pindex, 2) // n.pindex

	// We now notice the leader indicated a different entry at the (no quorum) index, should save that.
	n.processAppendEntry(aeMissed, n.catchup.sub)
	require_Equal(t, n.wal.State().Msgs, 3)
	require_True(t, n.catchup != nil)

	// We now get the entry that initially triggered us to catchup, it should be added.
	n.processAppendEntry(aeCatchupTrigger, n.catchup.sub)
	require_Equal(t, n.wal.State().Msgs, 4)
	require_True(t, n.catchup != nil)
	entry, err = n.loadEntry(4)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats1)

	// Heartbeat, makes sure we commit (and reset catchup, as we're now up-to-date).
	n.processAppendEntry(aeHeartbeat3, n.aesub)
	require_Equal(t, n.commit, 4)
	require_True(t, n.catchup == nil)
}

func TestNRGCatchupCanTruncateMultipleEntriesWithoutQuorum(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"

	// Timeline, for first leader
	aeInitial := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})
	aeNoQuorum1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeNoQuorum2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 2, entries: entries})

	// Timeline, after leader change
	aeMissed1 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeMissed2 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 2, pindex: 2, entries: entries})
	aeCatchupTrigger := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 2, pindex: 3, entries: entries})
	aeHeartbeat2 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 2, pterm: 2, pindex: 4, entries: nil})
	aeHeartbeat3 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 4, pterm: 2, pindex: 4, entries: nil})

	// Initial case is simple, just store the entry.
	n.processAppendEntry(aeInitial, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Heartbeat, makes sure commit moves up.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 1)

	// We get one entry that has NO quorum (but we don't know that yet).
	n.processAppendEntry(aeNoQuorum1, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 2)
	entry, err = n.loadEntry(2)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// We get another entry that has NO quorum (but we don't know that yet).
	n.processAppendEntry(aeNoQuorum2, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 3)
	entry, err = n.loadEntry(3)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// We've just had a leader election, and we missed messages from the previous leader.
	// We should truncate the last message.
	n.processAppendEntry(aeCatchupTrigger, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 2)
	require_True(t, n.catchup == nil)

	// We get a heartbeat that prompts us to catchup.
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 2)
	require_Equal(t, n.commit, 1) // Commit should not change, as we missed an item.
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 1)  // n.pterm
	require_Equal(t, n.catchup.pindex, 2) // n.pindex

	// We now notice the leader indicated a different entry at the (no quorum) index. We should truncate again.
	n.processAppendEntry(aeMissed2, n.catchup.sub)
	require_Equal(t, n.wal.State().Msgs, 1)
	require_True(t, n.catchup == nil)

	// We get a heartbeat that prompts us to catchup.
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 1)
	require_Equal(t, n.commit, 1) // Commit should not change, as we missed an item.
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 1)  // n.pterm
	require_Equal(t, n.catchup.pindex, 1) // n.pindex

	// We now get caught up with the missed messages.
	n.processAppendEntry(aeMissed1, n.catchup.sub)
	require_Equal(t, n.wal.State().Msgs, 2)
	require_True(t, n.catchup != nil)

	n.processAppendEntry(aeMissed2, n.catchup.sub)
	require_Equal(t, n.wal.State().Msgs, 3)
	require_True(t, n.catchup != nil)

	// We now get the entry that initially triggered us to catchup, it should be added.
	n.processAppendEntry(aeCatchupTrigger, n.catchup.sub)
	require_Equal(t, n.wal.State().Msgs, 4)
	require_True(t, n.catchup != nil)
	entry, err = n.loadEntry(4)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats1)

	// Heartbeat, makes sure we commit (and reset catchup, as we're now up-to-date).
	n.processAppendEntry(aeHeartbeat3, n.aesub)
	require_Equal(t, n.commit, 4)
	require_True(t, n.catchup == nil)
}

func TestNRGCatchupDoesNotTruncateCommittedEntriesDuringRedelivery(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline.
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeHeartbeat1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: nil})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 2, entries: entries})
	aeHeartbeat2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 3, pterm: 1, pindex: 3, entries: nil})

	// Initial case is simple, just store the entry.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Deliver a message.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 2)
	entry, err = n.loadEntry(2)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Heartbeat, makes sure commit moves up.
	n.processAppendEntry(aeHeartbeat1, n.aesub)
	require_Equal(t, n.commit, 2)

	// Deliver another message.
	n.processAppendEntry(aeMsg3, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 3)
	entry, err = n.loadEntry(3)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Simulate receiving an old entry as a redelivery. We should not truncate as that lowers our commit.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.commit, 2)

	// Heartbeat, makes sure we commit.
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_Equal(t, n.commit, 3)
}

func TestNRGCatchupFromNewLeaderWithIncorrectPterm(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline.
	aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 0, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})

	// Heartbeat, triggers catchup.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 0) // Commit should not change, as we missed an item.
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 0)  // n.pterm
	require_Equal(t, n.catchup.pindex, 0) // n.pindex

	// First catchup message has the incorrect pterm, stop catchup and re-trigger later with the correct pterm.
	n.processAppendEntry(aeMsg, n.catchup.sub)
	require_True(t, n.catchup == nil)
	require_Equal(t, n.pterm, 1)
	require_Equal(t, n.pindex, 0)

	// Heartbeat, triggers catchup.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 0) // Commit should not change, as we missed an item.
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 1)  // n.pterm
	require_Equal(t, n.catchup.pindex, 0) // n.pindex

	// Now we get the message again and can continue to store it.
	n.processAppendEntry(aeMsg, n.catchup.sub)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Now heartbeat is able to commit the entry.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 1)
}

func TestNRGDontRemoveSnapshotIfTruncateToApplied(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline.
	aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})

	// Initial case is simple, just store the entry.
	n.processAppendEntry(aeMsg, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Heartbeat, makes sure commit moves up.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.pterm, 1)

	// Simulate upper layer calling down to apply.
	n.Applied(1)

	// Install snapshot and check it exists.
	err = n.InstallSnapshot(nil)
	require_NoError(t, err)

	snapshots := path.Join(n.sd, snapshotsDir)
	files, err := os.ReadDir(snapshots)
	require_NoError(t, err)
	require_Equal(t, len(files), 1)

	// Truncate and check snapshot is kept.
	n.truncateWAL(n.pterm, n.applied)

	files, err = os.ReadDir(snapshots)
	require_NoError(t, err)
	require_Equal(t, len(files), 1)
}

func TestNRGSnapshotAndTruncateToApplied(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats1 := "yrzKKRBu" // "nats-1"
	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline, other leader
	aeMsg1 := encode(t, &appendEntry{leader: nats1, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats1, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})

	// Timeline, we temporarily became leader
	aeHeartbeat1 := encode(t, &appendEntry{leader: nats0, term: 2, commit: 2, pterm: 1, pindex: 2, entries: nil})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 2, commit: 2, pterm: 1, pindex: 2, entries: entries})

	// Timeline, old leader is back.
	aeHeartbeat2 := encode(t, &appendEntry{leader: nats1, term: 3, commit: 2, pterm: 1, pindex: 2, entries: nil})

	// Simply receive first message.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.commit, 0)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats1)

	// Receive second message, which commits the first message.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.wal.State().Msgs, 2)
	entry, err = n.loadEntry(2)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats1)

	// Simulate upper layer calling down to apply.
	n.Applied(1)

	// Send heartbeat, which commits the second message.
	n.processAppendEntryResponse(&appendEntryResponse{
		term:    aeHeartbeat1.term,
		index:   aeHeartbeat1.pindex,
		peer:    nats1,
		reply:   _EMPTY_,
		success: true,
	})
	require_Equal(t, n.commit, 2)

	// Simulate upper layer calling down to apply.
	n.Applied(2)

	// Install snapshot and check it exists.
	err = n.InstallSnapshot(nil)
	require_NoError(t, err)

	snapshots := path.Join(n.sd, snapshotsDir)
	files, err := os.ReadDir(snapshots)
	require_NoError(t, err)
	require_Equal(t, len(files), 1)
	require_Equal(t, n.wal.State().Msgs, 0)

	// Store a third message, it stays uncommitted.
	require_NoError(t, n.storeToWAL(aeMsg3))
	require_Equal(t, n.commit, 2)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err = n.loadEntry(3)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Receive heartbeat from new leader, should not lose commits.
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 0)
	require_Equal(t, n.commit, 2)
	require_Equal(t, n.applied, 2)
}

func TestNRGIgnoreDoubleSnapshot(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 2, commit: 1, pterm: 1, pindex: 1, entries: entries})

	// Simply receive first message.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.commit, 0)

	// Heartbeat moves commit up.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 1)

	// Manually call back down to applied.
	n.Applied(1)

	// Second message just for upping the pterm.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.pindex, 2)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.pterm, 2)
	require_Equal(t, n.term, 2)

	// Snapshot, and confirm state.
	err := n.InstallSnapshot(nil)
	require_NoError(t, err)
	snap, err := n.loadLastSnapshot()
	require_NoError(t, err)
	require_Equal(t, snap.lastTerm, 1)
	require_Equal(t, snap.lastIndex, 1)

	// Snapshot again, should not overwrite previous snapshot.
	err = n.InstallSnapshot(nil)
	require_Error(t, err, errNoSnapAvailable)
	snap, err = n.loadLastSnapshot()
	require_NoError(t, err)
	require_Equal(t, snap.lastTerm, 1)
	require_Equal(t, snap.lastIndex, 1)
}

func TestNRGDontSwitchToCandidateWithInflightSnapshot(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample snapshot entry, the content doesn't matter.
	snapshotEntries := []*Entry{
		newEntry(EntrySnapshot, nil),
		newEntry(EntryPeerState, encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt})),
	}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline.
	aeTriggerCatchup := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})
	aeCatchupSnapshot := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: snapshotEntries})

	// Switch follower into catchup.
	n.processAppendEntry(aeTriggerCatchup, n.aesub)
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 0)  // n.pterm
	require_Equal(t, n.catchup.pindex, 0) // n.pindex

	// Follower receives a snapshot, marking a snapshot as inflight as the apply queue is async.
	n.processAppendEntry(aeCatchupSnapshot, n.catchup.sub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.commit, 1)

	// Try to switch to candidate, it should be blocked since the snapshot is not processed yet.
	n.switchToCandidate()
	require_Equal(t, n.State(), Follower)

	// Simulate snapshot being processed by the upper layer.
	n.Applied(1)

	// Retry becoming candidate, snapshot is processed so can now do so.
	n.switchToCandidate()
	require_Equal(t, n.State(), Candidate)
}

func TestNRGDontSwitchToCandidateWithMultipleInflightSnapshots(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample snapshot entry, the content doesn't matter.
	snapshotEntries := []*Entry{
		newEntry(EntrySnapshot, nil),
		newEntry(EntryPeerState, encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt})),
	}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline.
	aeSnapshot1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: snapshotEntries})
	aeSnapshot2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: snapshotEntries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: nil})

	// Simulate snapshots being sent to us.
	n.processAppendEntry(aeSnapshot1, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.commit, 0)
	require_Equal(t, n.applied, 0)

	n.processAppendEntry(aeSnapshot2, n.aesub)
	require_Equal(t, n.pindex, 2)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.applied, 0)

	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.pindex, 2)
	require_Equal(t, n.commit, 2)
	require_Equal(t, n.applied, 0)

	for i := uint64(1); i <= 2; i++ {
		// Try to switch to candidate, it should be blocked since the snapshot is not processed yet.
		n.switchToCandidate()
		require_Equal(t, n.State(), Follower)

		// Simulate snapshot being processed by the upper layer.
		n.Applied(i)
	}

	// Retry becoming candidate, all snapshots processed so can now do so.
	n.switchToCandidate()
	require_Equal(t, n.State(), Candidate)
}

func TestNRGRecoverPindexPtermOnlyIfLogNotEmpty(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	gn := rg[0].(*stateAdder)
	rn := rg[0].node().(*raft)

	// Delete the msgs and snapshots, leaving the only remaining trace
	// of the term in the TAV file.
	store := filepath.Join(gn.cfg.Store)
	require_NoError(t, rn.wal.Truncate(0))
	require_NoError(t, os.RemoveAll(filepath.Join(store, "msgs")))
	require_NoError(t, os.RemoveAll(filepath.Join(store, "snapshots")))

	for _, gn := range rg {
		gn.stop()
	}
	rg[0].restart()
	rn = rg[0].node().(*raft)

	// Both should be zero as, without any snapshots or log entries,
	// the log is considered empty and therefore we account as such.
	require_Equal(t, rn.pterm, 0)
	require_Equal(t, rn.pindex, 0)
}

func TestNRGCancelCatchupWhenDetectingHigherTermDuringVoteRequest(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline.
	aeCatchupTrigger := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})

	// Truncate to simulate we missed one message and need to catchup.
	n.processAppendEntry(aeCatchupTrigger, n.aesub)
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 0)  // n.pterm
	require_Equal(t, n.catchup.pindex, 0) // n.pindex

	// Process first message as part of the catchup.
	catchupSub := n.catchup.sub
	n.processAppendEntry(aeMsg1, catchupSub)
	require_True(t, n.catchup != nil)

	// Receiving a vote request should cancel our catchup.
	// Otherwise, we could receive catchup messages after this that provides the previous leader with quorum.
	// If the new leader doesn't have these entries, the previous leader would desync since it would commit them.
	err := n.processVoteRequest(&voteRequest{2, 1, 1, nats0, "reply"})
	require_NoError(t, err)
	require_True(t, n.catchup == nil)
}

func TestNRGMultipleStopsDontPanic(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	defer func() {
		p := recover()
		require_True(t, p == nil)
	}()

	for i := 0; i < 10; i++ {
		n.Stop()
	}
}

func TestNRGTruncateDownToCommitted(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"

	// Timeline, we are leader
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})

	// Timeline, after leader change
	aeMsg3 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats1, term: 2, commit: 2, pterm: 2, pindex: 2, entries: nil})

	// Simply receive first message.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.commit, 0)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Receive second message, which commits the first message.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.wal.State().Msgs, 2)
	entry, err = n.loadEntry(2)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// We receive an entry from another leader, should truncate down to commit / remove the second message.
	// After doing so, we should also be able to immediately store the message after.
	n.processAppendEntry(aeMsg3, n.aesub)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.wal.State().Msgs, 2)
	entry, err = n.loadEntry(2)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats1)

	// Heartbeat moves commit up.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 2)
}

type mockWALTruncateAlwaysFails struct {
	WAL
}

func (m mockWALTruncateAlwaysFails) Truncate(seq uint64) error {
	return errors.New("test: truncate always fails")
}

func TestNRGTruncateDownToCommittedWhenTruncateFails(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	n.Lock()
	n.wal = mockWALTruncateAlwaysFails{n.wal}
	n.Unlock()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"

	// Timeline, we are leader
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})

	// Timeline, after leader change
	aeMsg3 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 1, pindex: 1, entries: entries})

	// Simply receive first message.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.commit, 0)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Receive second message, which commits the first message.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.wal.State().Msgs, 2)
	entry, err = n.loadEntry(2)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// We receive an entry from another leader, should truncate down to commit / remove the second message.
	// But, truncation fails so should register that and not change pindex/pterm.
	bindex, bterm := n.pindex, n.pterm
	n.processAppendEntry(aeMsg3, n.aesub)
	require_Error(t, n.werr, errors.New("test: truncate always fails"))
	require_Equal(t, bindex, n.pindex)
	require_Equal(t, bterm, n.pterm)
}

func TestNRGForwardProposalResponse(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	n := rg.nonLeader().node().(*raft)
	psubj := n.psubj

	data := make([]byte, binary.MaxVarintLen64)
	dn := binary.PutVarint(data, int64(123))

	_, err := nc.Request(psubj, data[:dn], time.Second*5)
	require_NoError(t, err)

	rg.waitOnTotal(t, 123)
}

func TestNRGMemoryWALEmptiesSnapshotsDir(t *testing.T) {
	n, c := initSingleMemRaftNodeWithCluster(t)
	defer c.shutdown()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})

	// Simply receive first message.
	n.processAppendEntry(aeMsg, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.commit, 0)

	// Heartbeat moves commit up.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 1)

	// Manually call back down to applied, and then snapshot.
	n.Applied(1)
	err := n.InstallSnapshot(nil)
	require_NoError(t, err)

	// Stop current node and restart it.
	n.Stop()
	n.WaitForStop()

	s := c.servers[0]
	ms, err := newMemStore(&StreamConfig{Name: "TEST", Storage: MemoryStorage})
	require_NoError(t, err)
	cfg := &RaftConfig{Name: "TEST", Store: n.sd, Log: ms}
	n, err = s.initRaftNode(globalAccountName, cfg, pprofLabels{})
	require_NoError(t, err)

	// Since the WAL is in-memory, the snapshots dir should've been emptied upon restart.
	files, err := os.ReadDir(filepath.Join(n.sd, snapshotsDir))
	require_NoError(t, err)
	require_Len(t, len(files), 0)
}

func TestNRGHealthCheckWaitForCatchup(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 3, pterm: 1, pindex: 3, entries: nil})

	// Switch follower into catchup.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 0)  // n.pterm
	require_Equal(t, n.catchup.pindex, 0) // n.pindex
	require_Equal(t, n.catchup.cterm, aeHeartbeat.term)
	require_Equal(t, n.catchup.cindex, aeHeartbeat.pindex)

	// Catchup first message.
	n.processAppendEntry(aeMsg1, n.catchup.sub)
	require_Equal(t, n.pindex, 1)
	require_False(t, n.Healthy())

	// Catchup second message.
	n.processAppendEntry(aeMsg2, n.catchup.sub)
	require_Equal(t, n.pindex, 2)
	require_Equal(t, n.commit, 1)
	require_False(t, n.Healthy())

	// If we apply the entry sooner than we receive the next catchup message,
	// should not mark as healthy since we're still in catchup.
	n.Applied(1)
	require_False(t, n.Healthy())

	// Catchup third message.
	n.processAppendEntry(aeMsg3, n.catchup.sub)
	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.commit, 2)
	n.Applied(2)
	require_False(t, n.Healthy())

	// Heartbeat stops catchup.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_True(t, n.catchup == nil)
	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.commit, 3)
	require_False(t, n.Healthy())

	// Still need to wait for the last entry to be applied.
	n.Applied(3)
	require_True(t, n.Healthy())
}

func TestNRGHealthCheckWaitForDoubleCatchup(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeHeartbeat1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: nil})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: entries})
	aeHeartbeat2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 3, pterm: 1, pindex: 3, entries: nil})

	// Switch follower into catchup.
	n.processAppendEntry(aeHeartbeat1, n.aesub)
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 0)  // n.pterm
	require_Equal(t, n.catchup.pindex, 0) // n.pindex
	require_Equal(t, n.catchup.cterm, aeHeartbeat1.term)
	require_Equal(t, n.catchup.cindex, aeHeartbeat1.pindex)

	// Catchup first message.
	n.processAppendEntry(aeMsg1, n.catchup.sub)
	require_Equal(t, n.pindex, 1)
	require_False(t, n.Healthy())

	// We miss this message, since we're catching up.
	n.processAppendEntry(aeMsg3, n.aesub)
	require_True(t, n.catchup != nil)
	require_Equal(t, n.pindex, 1)
	require_False(t, n.Healthy())

	// We also miss the heartbeat, since we're catching up.
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_True(t, n.catchup != nil)
	require_Equal(t, n.pindex, 1)
	require_False(t, n.Healthy())

	// Catchup second message, this will stop catchup.
	n.processAppendEntry(aeMsg2, n.catchup.sub)
	require_Equal(t, n.pindex, 2)
	require_Equal(t, n.commit, 1)
	n.Applied(1)
	require_False(t, n.Healthy())

	// We expect to still be in catchup, waiting for a heartbeat or new append entry to reset.
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.cterm, aeHeartbeat1.term)
	require_Equal(t, n.catchup.cindex, aeHeartbeat1.pindex)

	// We now get a 'future' heartbeat, should restart catchup.
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 1)  // n.pterm
	require_Equal(t, n.catchup.pindex, 2) // n.pindex
	require_Equal(t, n.catchup.cterm, aeHeartbeat2.term)
	require_Equal(t, n.catchup.cindex, aeHeartbeat2.pindex)
	require_False(t, n.Healthy())

	// Catchup third message.
	n.processAppendEntry(aeMsg3, n.catchup.sub)
	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.commit, 2)
	n.Applied(2)
	require_False(t, n.Healthy())

	// Heartbeat stops catchup.
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_True(t, n.catchup == nil)
	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.commit, 3)
	require_False(t, n.Healthy())

	// Still need to wait for the last entry to be applied.
	n.Applied(3)
	require_True(t, n.Healthy())
}

func TestNRGHealthCheckWaitForPendingCommitsWhenPaused(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: nil})

	// Process first message.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_False(t, n.Healthy())

	// Process second message, moves commit up.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.pindex, 2)
	require_False(t, n.Healthy())

	// We're healthy once we've applied the first message.
	n.Applied(1)
	require_True(t, n.Healthy())

	// If we're paused we still are healthy if there are no pending commits.
	err := n.PauseApply()
	require_NoError(t, err)
	require_True(t, n.Healthy())

	// Heartbeat marks second message to be committed.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.pindex, 2)
	require_False(t, n.Healthy())

	// Resuming apply commits the message.
	n.ResumeApply()
	require_NoError(t, err)
	require_False(t, n.Healthy())

	// But still waiting for it to be applied before marking healthy.
	n.Applied(2)
	require_True(t, n.Healthy())
}

func TestNRGHeartbeatCanEstablishQuorumAfterLeaderChange(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeHeartbeatResponse := &appendEntryResponse{term: 1, index: 1, peer: nats0, success: true}

	// Process first message.
	n.processAppendEntry(aeMsg, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.aflr, 0)

	// Simulate becoming leader, and not knowing if the stored entry has quorum and can be committed.
	// Switching to leader should send a heartbeat.
	n.switchToLeader()
	require_Equal(t, n.aflr, 1)
	require_Equal(t, n.commit, 0)

	// We simulate receiving the successful heartbeat response here. It should move the commit up.
	n.processAppendEntryResponse(aeHeartbeatResponse)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.aflr, 1)

	// Once the entry is applied, it should reset the applied floor.
	n.Applied(1)
	require_Equal(t, n.aflr, 0)
}

func TestNRGQuorumAccounting(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats1 := "yrzKKRBu" // "nats-1"
	nats2 := "cnrtt3eg" // "nats-2"

	// Timeline
	aeHeartbeat1Response := &appendEntryResponse{term: 1, index: 1, peer: nats1, success: true}
	aeHeartbeat2Response := &appendEntryResponse{term: 1, index: 1, peer: nats2, success: true}

	// Adjust cluster size, so we need at least 2 responses from other servers to establish quorum.
	require_NoError(t, n.AdjustBootClusterSize(5))
	require_Equal(t, n.csz, 5)
	require_Equal(t, n.qn, 3)

	// Switch this node to leader, and send an entry.
	n.switchToLeader()
	require_Equal(t, n.pindex, 0)
	n.sendAppendEntry(entries)
	require_Equal(t, n.pindex, 1)

	// The first response MUST NOT indicate quorum has been reached.
	n.processAppendEntryResponse(aeHeartbeat1Response)
	require_Equal(t, n.commit, 0)

	// The second response means we have reached quorum and can move commit up.
	n.processAppendEntryResponse(aeHeartbeat2Response)
	require_Equal(t, n.commit, 1)
}

func TestNRGRevalidateQuorumAfterLeaderChange(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats1 := "yrzKKRBu" // "nats-1"
	nats2 := "cnrtt3eg" // "nats-2"

	// Timeline
	aeHeartbeat1Response := &appendEntryResponse{term: 1, index: 1, peer: nats1, success: true}
	aeHeartbeat2Response := &appendEntryResponse{term: 1, index: 1, peer: nats2, success: true}

	// Adjust cluster size, so we need at least 2 responses from other servers to establish quorum.
	require_NoError(t, n.AdjustBootClusterSize(5))
	require_Equal(t, n.csz, 5)
	require_Equal(t, n.qn, 3)

	// Switch this node to leader, and send an entry.
	n.term++
	n.switchToLeader()
	require_Equal(t, n.term, 1)
	require_Equal(t, n.pindex, 0)
	n.sendAppendEntry(entries)
	require_Equal(t, n.pindex, 1)

	// We have one server that signals the message was stored. The leader will add 1 to the acks count.
	n.processAppendEntryResponse(aeHeartbeat1Response)
	require_Equal(t, n.commit, 0)
	require_Len(t, len(n.acks), 1)

	// We stepdown now and don't know if we will have quorum on the first entry.
	n.stepdown(noLeader)

	// Let's assume there are a bunch of leader elections now, data being added to the log, being truncated, etc.
	// We don't know what happened, maybe we were partitioned, but we can't know for sure if the first entry has quorum.

	// We now become leader again.
	n.term = 6
	n.switchToLeader()
	require_Equal(t, n.term, 6)

	// We now receive a successful response from another server saying they have stored it.
	// Anything can have happened to the replica that said success before, we can't assume that's still valid.
	// So our commit must stay the same and we restart counting for quorum.
	n.processAppendEntryResponse(aeHeartbeat2Response)
	require_Equal(t, n.commit, 0)
	require_Len(t, len(n.acks), 1)
}

func TestNRGSignalLeadChangeFalseIfCampaignImmediately(t *testing.T) {
	tests := []struct {
		title      string
		switchNode func(n *raft)
	}{
		{
			title: "Follower",
		},
		{
			title: "Candidate",
			switchNode: func(n *raft) {
				n.switchToCandidate()
			},
		},
		{
			title: "Leader",
			switchNode: func(n *raft) {
				n.switchToCandidate()
				n.switchToLeader()
				select {
				case isLeader := <-n.LeadChangeC():
					require_True(t, isLeader)
				default:
					t.Error("Expected leadChange signal")
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			n, cleanup := initSingleMemRaftNode(t)
			defer cleanup()

			// Create a sample entry, the content doesn't matter, just that it's stored.
			esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
			entries := []*Entry{newEntry(EntryNormal, esm)}

			nats0 := "S1Nunr6R" // "nats-0"
			aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})

			// Campaigning immediately signals we're the preferred leader.
			require_NoError(t, n.CampaignImmediately())
			if test.switchNode != nil {
				test.switchNode(n)
			}

			n.processAppendEntry(aeMsg1, n.aesub)

			select {
			case isLeader := <-n.LeadChangeC():
				require_False(t, isLeader)
			default:
				t.Error("Expected leadChange signal")
			}
			require_Equal(t, n.State(), Follower)
			require_Equal(t, n.leader, nats0)
			require_Equal(t, n.term, 1)
		})
	}
}

// This is a RaftChainOfBlocks test where a block is proposed and then we wait for all replicas to apply it before
// proposing the next one.
// The test may fail if:
//   - Replicas hash diverge
//   - One replica never applies the N-th block applied by the rest
//   - The given number of blocks cannot be applied within some amount of time
func TestNRGChainOfBlocksRunInLockstep(t *testing.T) {
	const iterations = 50
	const applyTimeout = 3 * time.Second
	const testTimeout = iterations * time.Second

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newRaftChainStateMachine)
	rg.waitOnLeader()

	testTimer := time.NewTimer(testTimeout)

	for iteration := uint64(1); iteration <= iterations; iteration++ {
		select {
		case <-testTimer.C:
			t.Fatalf("Timeout, completed %d/%d iterations", iteration-1, iterations)
		default:
			// Continue
		}

		// Propose the next block (this test assumes the proposal always goes through)
		rg.randomMember().(*RCOBStateMachine).proposeBlock()

		var currentHash string

		// Wait on participants to converge
		checkFor(t, applyTimeout, 500*time.Millisecond, func() error {
			var previousNodeName string
			var previousNodeHash string
			for _, sm := range rg {
				stateMachine := sm.(*RCOBStateMachine)
				nodeName := fmt.Sprintf(
					"%s/%s",
					stateMachine.server().Name(),
					stateMachine.node().ID(),
				)

				running, blocksCount, hash := stateMachine.getCurrentHash()
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
				if hash == "" {
					return fmt.Errorf(
						"node %s has empty hash after applying %d blocks",
						nodeName,
						blocksCount,
					)
				}
				// Check against previous node hash, unless this is the first node and we don't have anyone to compare
				if previousNodeHash != "" && previousNodeHash != hash {
					return fmt.Errorf(
						"hash mismatch after %d blocks: %s hash: %s != %s hash: %s",
						iteration,
						nodeName,
						hash,
						previousNodeName,
						previousNodeHash,
					)
				}
				// Set node name and hash for next node to compare against
				previousNodeName, previousNodeHash = nodeName, hash

			}
			// All replicas applied the last block and their hashes match
			currentHash = previousNodeHash
			return nil
		})
		if RCOBOptions.verbose {
			t.Logf(
				"Verified chain hash %s for %d/%d nodes after %d/%d iterations",
				currentHash,
				len(rg),
				len(rg),
				iteration,
				iterations,
			)
		}
	}
}

// This is a RaftChainOfBlocks test where one of the replicas is stopped before proposing a short burst of blocks.
// Upon resuming the replica, we check it is able to catch up to the rest.
// The test may fail if:
//   - Replicas hash diverge
//   - One replica never applies the N-th block applied by the rest
//   - The given number of blocks cannot be applied within some amount of time
func TestNRGChainOfBlocksStopAndCatchUp(t *testing.T) {
	const iterations = 50
	const blocksPerIteration = 3
	const applyTimeout = 3 * time.Second
	const testTimeout = 2 * iterations * time.Second

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newRaftChainStateMachine)
	rg.waitOnLeader()

	testTimer := time.NewTimer(testTimeout)
	highestBlockSeen := uint64(0)

	for iteration := uint64(1); iteration <= iterations; iteration++ {

		select {
		case <-testTimer.C:
			t.Fatalf("Timeout, completed %d/%d iterations", iteration-1, iterations)
		default:
			// Continue
		}

		// Stop a random node
		stoppedNode := rg.randomMember()
		leader := stoppedNode.node().Leader()
		stoppedNode.stop()

		// Snapshot a random (non-stopped) node
		snapshotNode := rg.randomMember()
		for snapshotNode == stoppedNode {
			snapshotNode = rg.randomMember()
		}
		snapshotNode.(*RCOBStateMachine).createSnapshot()

		if RCOBOptions.verbose {
			t.Logf(
				"Iteration %d/%d: stopping node: %s/%s (leader: %v)",
				iteration,
				iterations,
				stoppedNode.server().Name(),
				stoppedNode.node().ID(),
				leader,
			)
		}

		// Propose some new blocks
		rg.waitOnLeader()
		for i := 0; i < blocksPerIteration; i++ {
			proposer := rg.randomMember()
			// Pick again if we randomly chose the stopped node
			for proposer == stoppedNode {
				proposer = rg.randomMember()
			}

			proposer.(*RCOBStateMachine).proposeBlock()
		}

		// Restart the stopped node
		stoppedNode.restart()

		// Wait on participants to converge
		expectedBlocks := iteration * blocksPerIteration
		var currentHash string
		checkFor(t, applyTimeout, 250*time.Millisecond, func() error {
			var previousNodeName string
			var previousNodeHash string
			for _, sm := range rg {
				stateMachine := sm.(*RCOBStateMachine)
				nodeName := fmt.Sprintf(
					"%s/%s",
					stateMachine.server().Name(),
					stateMachine.node().ID(),
				)
				running, blocksCount, currentHash := stateMachine.getCurrentHash()
				// Track the highest block seen by any replica
				if blocksCount > highestBlockSeen {
					highestBlockSeen = blocksCount
					// Must check all replicas again
					return fmt.Errorf("updated highest block to %d (%s)", highestBlockSeen, nodeName)
				}
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
			}
			// All is well
			currentHash = previousNodeHash
			return nil
		})

		if RCOBOptions.verbose {
			t.Logf(
				"Verified chain hash %s for %d/%d nodes after %d blocks, %d/%d iterations (%d lost proposals)",
				currentHash,
				len(rg),
				len(rg),
				highestBlockSeen,
				iteration,
				iterations,
				expectedBlocks-highestBlockSeen,
			)
		}
	}
}
