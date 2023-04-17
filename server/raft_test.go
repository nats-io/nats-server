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

func TestNRGPIndex(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	/*
		for _, s := range c.servers {
			log := srvlog.NewStdLogger(true, true, false, true, true)
			s.SetLogger(log, true, false)
			s.reloadDebugRaftNodes(true)
		}
	*/

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()
	rg.waitOnAllCurrent()

	randomAdder := func() *stateAdder {
		return rg.randomMember().(*stateAdder)
	}

	type step struct {
		total  int64  // The expected value of the adder
		term   uint64 // The expected term value
		pterm  uint64 // The expected pterm value
		pindex uint64 // The expected pindex value
		commit uint64 // The expected number of commits
		action func() // The action this step should perform
	}

	for i, s := range []step{
		// First, try just proposing some changes. Use a random node
		// each time to check that we are remaining consistent.
		{1, 1, 1, 2, 2, func() { randomAdder().proposeDelta(1) }},
		{3, 1, 1, 3, 3, func() { randomAdder().proposeDelta(2) }},
		{6, 1, 1, 4, 4, func() { randomAdder().proposeDelta(3) }},
		{10, 1, 1, 5, 5, func() { randomAdder().proposeDelta(4) }},

		// Now ask the leader to step down. At this point we expect
		// a new term to start but the adder value shouldn't change.
		{10, 2, 2, 7, 7, func() { rg.leader().node().StepDown() }},

		// Propose a new addition now that we have a new leader.
		{11, 2, 2, 8, 8, func() { randomAdder().proposeDelta(1) }},

		// Now we're going to kill and restart the leader. There
		// should be no value change here, but we do expect a new
		// term to start, since this should result in an election.
		{11, 3, 3, 9, 9, func() {
			leader := rg.leader()
			t.Log("Restarting", leader.node().ID())
			leader.stop()
			leader.restart()
		}},

		// Now propose some more changes.
		{12, 3, 3, 10, 10, func() { randomAdder().proposeDelta(1) }},
		{14, 3, 3, 11, 11, func() { randomAdder().proposeDelta(2) }},
		{17, 3, 3, 12, 12, func() { randomAdder().proposeDelta(3) }},
	} {
		t.Logf("Step %d", i+1)

		// Perform the action and then wait for all nodes in the
		// cluster to become current.
		s.action()
		rg.waitOnAllCurrent()

		rg.lockAndInspect(t, func() {
			// Now perform sanity checks.
			for _, sm := range rg {
				a := sm.(*stateAdder)
				r := sm.node().(*raft)
				t.Logf(
					" - [%s] %s, %v, total %d, term %d, pterm %d, pindex %d, commit %d",
					r.id, r.state, true /*r.Current()*/, a.total(), r.term, r.pterm, r.pindex, r.commit,
				)
			}

			for _, sm := range rg {
				a := sm.(*stateAdder)
				r := sm.node().(*raft)
				switch {
				case r.commit != s.commit:
					t.Fatalf("%s state should be commit %d, got %d", r.id, s.commit, r.commit)
				case r.commit != r.applied:
					t.Fatalf("%s state should have applied %d commits but has only applied %d", r.id, r.commit, r.applied)
				case a.total() != s.total:
					t.Fatalf("%s total should be %d, got %d", r.id, s.total, a.total())
				case r.term != s.term:
					t.Fatalf("%s state should be term %d, got %d", r.id, s.term, r.term)
				case r.pterm != s.pterm:
					t.Fatalf("%s state should be pterm %d, got %d", r.id, s.pterm, r.pterm)
				case r.pindex != s.pindex:
					t.Fatalf("%s state should be pindex %d, got %d", r.id, s.pindex, r.pindex)
				}
			}
		})
	}
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
