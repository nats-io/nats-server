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

func TestNRGCandidateStepsDownAfterAE(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	c.waitOnLeader()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// Pick a random follower node. Bump the term up by a considerable
	// amount and force it into the candidate state. This is what happens
	// after a period of time in isolation.
	n := rg.nonLeader().node().(*raft)
	n.Lock()
	n.term += 100
	n.switchState(Candidate)
	n.Unlock()

	// Have the leader push through something on the current term just
	// for good measure, although the heartbeats probably work too.
	rg.leader().(*stateAdder).proposeDelta(1)

	// Wait for the leader to receive the next append entry from the
	// current leader. What should happen is that the node steps down
	// and starts following the leader, as nothing in the log of the
	// follower is newer than the term of the leader.
	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		if n.State() == Candidate {
			return fmt.Errorf("shouldn't still be candidate state")
		}
		if nterm, lterm := n.Term(), rg.leader().node().Term(); nterm != lterm {
			return fmt.Errorf("follower term %d should match leader term %d", nterm, lterm)
		}
		return nil
	})
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
