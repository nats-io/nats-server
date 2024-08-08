// Copyright 2021-2024 The NATS Authors
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
	"os"
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

func TestNRGSwitchStateClearsQueues(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	s := c.servers[0] // RunBasicJetStreamServer not available

	n := &raft{
		prop:  newIPQueue[*Entry](s, "prop"),
		resp:  newIPQueue[*appendEntryResponse](s, "resp"),
		leadc: make(chan bool, 1), // for switchState
	}
	n.state.Store(int32(Leader))
	require_Equal(t, n.prop.len(), 0)
	require_Equal(t, n.resp.len(), 0)

	n.prop.push(&Entry{})
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
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	c.waitOnLeader()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()
	leader := rg.leader().node().(*raft)
	follower := rg.nonLeader().node().(*raft)

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
	defer rg.unlockAll()
	require_True(t, leaderOriginal.Equal(leader.etlr))
	require_True(t, followerOriginal.Equal(follower.etlr))
}

func TestNRGInvalidTAVDoesntPanic(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	c.waitOnLeader()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// Mangle the TAV file to a short length (less than uint64).
	leader := rg.leader()
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
		leader.proposeDelta(-11)
		rg.waitOnTotal(t, 0)
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
			rn.truncateWAL(1, 6) // This will overwrite rn.term, so...
			rn.term = 2          // ... we'll set it back manually.
			require_Equal(t, rn.pterm, 1)
			require_Equal(t, rn.pindex, 6)
		}
	}
	rg.unlockAll()

	arInbox := nc.NewRespInbox()
	arCh := make(chan *nats.Msg, 2)
	_, err := nc.ChanSubscribe(arInbox, arCh)
	require_NoError(t, err)

	// In order to trip this condition, we need to send an append entry that
	// will trick the followers into running a catchup. In the process they
	// were setting the term back to pterm which is incorrect.
	ae := newAppendEntry(leader.id, leader.term, leader.commit, leader.pterm, leader.pindex, nil)
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
