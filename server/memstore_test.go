// Copyright 2019-2023 The NATS Authors
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
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestMemStoreBasics(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}

	subj, msg := "foo", []byte("Hello World")
	now := time.Now().UnixNano()
	if seq, ts, err := ms.StoreMsg(subj, nil, msg); err != nil {
		t.Fatalf("Error storing msg: %v", err)
	} else if seq != 1 {
		t.Fatalf("Expected sequence to be 1, got %d", seq)
	} else if ts < now || ts > now+int64(time.Millisecond) {
		t.Fatalf("Expected timestamp to be current, got %v", ts-now)
	}

	state := ms.State()
	if state.Msgs != 1 {
		t.Fatalf("Expected 1 msg, got %d", state.Msgs)
	}
	expectedSize := memStoreMsgSize(subj, nil, msg)
	if state.Bytes != expectedSize {
		t.Fatalf("Expected %d bytes, got %d", expectedSize, state.Bytes)
	}
	sm, err := ms.LoadMsg(1, nil)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if sm.subj != subj {
		t.Fatalf("Subjects don't match, original %q vs %q", subj, sm.subj)
	}
	if !bytes.Equal(sm.msg, msg) {
		t.Fatalf("Msgs don't match, original %q vs %q", msg, sm.msg)
	}
}

func TestMemStoreMsgLimit(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage, MaxMsgs: 10})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}
	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		ms.StoreMsg(subj, nil, msg)
	}
	state := ms.State()
	if state.Msgs != 10 {
		t.Fatalf("Expected %d msgs, got %d", 10, state.Msgs)
	}
	if _, _, err := ms.StoreMsg(subj, nil, msg); err != nil {
		t.Fatalf("Error storing msg: %v", err)
	}
	state = ms.State()
	if state.Msgs != 10 {
		t.Fatalf("Expected %d msgs, got %d", 10, state.Msgs)
	}
	if state.LastSeq != 11 {
		t.Fatalf("Expected the last sequence to be 11 now, but got %d", state.LastSeq)
	}
	if state.FirstSeq != 2 {
		t.Fatalf("Expected the first sequence to be 2 now, but got %d", state.FirstSeq)
	}
	// Make sure we can not lookup seq 1.
	if _, err := ms.LoadMsg(1, nil); err == nil {
		t.Fatalf("Expected error looking up seq 1 but got none")
	}
}

func TestMemStoreBytesLimit(t *testing.T) {
	subj, msg := "foo", make([]byte, 512)
	storedMsgSize := memStoreMsgSize(subj, nil, msg)

	toStore := uint64(1024)
	maxBytes := storedMsgSize * toStore

	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage, MaxBytes: int64(maxBytes)})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}

	for i := uint64(0); i < toStore; i++ {
		ms.StoreMsg(subj, nil, msg)
	}
	state := ms.State()
	if state.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	if state.Bytes != storedMsgSize*toStore {
		t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
	}

	// Now send 10 more and check that bytes limit enforced.
	for i := 0; i < 10; i++ {
		if _, _, err := ms.StoreMsg(subj, nil, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		}
	}
	state = ms.State()
	if state.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	if state.Bytes != storedMsgSize*toStore {
		t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
	}
	if state.FirstSeq != 11 {
		t.Fatalf("Expected first sequence to be 11, got %d", state.FirstSeq)
	}
	if state.LastSeq != toStore+10 {
		t.Fatalf("Expected last sequence to be %d, got %d", toStore+10, state.LastSeq)
	}
}

func TestMemStoreAgeLimit(t *testing.T) {
	maxAge := 10 * time.Millisecond
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage, MaxAge: maxAge})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}
	// Store some messages. Does not really matter how many.
	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		ms.StoreMsg(subj, nil, msg)
	}
	state := ms.State()
	if state.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	checkExpired := func(t *testing.T) {
		t.Helper()
		checkFor(t, time.Second, maxAge, func() error {
			state = ms.State()
			if state.Msgs != 0 {
				return fmt.Errorf("Expected no msgs, got %d", state.Msgs)
			}
			if state.Bytes != 0 {
				return fmt.Errorf("Expected no bytes, got %d", state.Bytes)
			}
			return nil
		})
	}
	// Let them expire
	checkExpired(t)
	// Now add some more and make sure that timer will fire again.
	for i := 0; i < toStore; i++ {
		ms.StoreMsg(subj, nil, msg)
	}
	state = ms.State()
	if state.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	checkExpired(t)
}

func TestMemStoreTimeStamps(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}
	last := time.Now().UnixNano()
	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		time.Sleep(5 * time.Microsecond)
		ms.StoreMsg(subj, nil, msg)
	}
	var smv StoreMsg
	for seq := uint64(1); seq <= 10; seq++ {
		sm, err := ms.LoadMsg(seq, &smv)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
		// These should be different
		if sm.ts <= last {
			t.Fatalf("Expected different timestamps, got %v", sm.ts)
		}
		last = sm.ts
	}
}

func TestMemStorePurge(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}

	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		ms.StoreMsg(subj, nil, msg)
	}
	if state := ms.State(); state.Msgs != 10 {
		t.Fatalf("Expected 10 msgs, got %d", state.Msgs)
	}
	ms.Purge()
	if state := ms.State(); state.Msgs != 0 {
		t.Fatalf("Expected no msgs, got %d", state.Msgs)
	}
}

func TestMemStoreCompact(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}

	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		ms.StoreMsg(subj, nil, msg)
	}
	if state := ms.State(); state.Msgs != 10 {
		t.Fatalf("Expected 10 msgs, got %d", state.Msgs)
	}
	n, err := ms.Compact(6)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n != 5 {
		t.Fatalf("Expected to have purged 5 msgs, got %d", n)
	}
	state := ms.State()
	if state.Msgs != 5 {
		t.Fatalf("Expected 5 msgs, got %d", state.Msgs)
	}
	if state.FirstSeq != 6 {
		t.Fatalf("Expected first seq of 6, got %d", state.FirstSeq)
	}
	// Now test that compact will also reset first if seq > last
	n, err = ms.Compact(100)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n != 5 {
		t.Fatalf("Expected to have purged 5 msgs, got %d", n)
	}
	if state = ms.State(); state.FirstSeq != 100 {
		t.Fatalf("Expected first seq of 100, got %d", state.FirstSeq)
	}
}

func TestMemStoreEraseMsg(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}
	subj, msg := "foo", []byte("Hello World")
	ms.StoreMsg(subj, nil, msg)
	sm, err := ms.LoadMsg(1, nil)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if !bytes.Equal(msg, sm.msg) {
		t.Fatalf("Expected same msg, got %q vs %q", sm.msg, msg)
	}
	if removed, _ := ms.EraseMsg(1); !removed {
		t.Fatalf("Expected erase msg to return success")
	}
}

func TestMemStoreMsgHeaders(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}
	subj, hdr, msg := "foo", []byte("name:derek"), []byte("Hello World")
	if sz := int(memStoreMsgSize(subj, hdr, msg)); sz != (len(subj) + len(hdr) + len(msg) + 16) {
		t.Fatalf("Wrong size for stored msg with header")
	}
	ms.StoreMsg(subj, hdr, msg)
	sm, err := ms.LoadMsg(1, nil)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if !bytes.Equal(msg, sm.msg) {
		t.Fatalf("Expected same msg, got %q vs %q", sm.msg, msg)
	}
	if !bytes.Equal(hdr, sm.hdr) {
		t.Fatalf("Expected same hdr, got %q vs %q", sm.hdr, hdr)
	}
	if removed, _ := ms.EraseMsg(1); !removed {
		t.Fatalf("Expected erase msg to return success")
	}
}

func TestMemStoreStreamStateDeleted(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}

	subj, toStore := "foo", uint64(10)
	for i := uint64(1); i <= toStore; i++ {
		msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
		if _, _, err := ms.StoreMsg(subj, nil, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		}
	}
	state := ms.State()
	if len(state.Deleted) != 0 {
		t.Fatalf("Expected deleted to be empty")
	}
	// Now remove some interior messages.
	var expected []uint64
	for seq := uint64(2); seq < toStore; seq += 2 {
		ms.RemoveMsg(seq)
		expected = append(expected, seq)
	}
	state = ms.State()
	if !reflect.DeepEqual(state.Deleted, expected) {
		t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
	}
	// Now fill the gap by deleting 1 and 3
	ms.RemoveMsg(1)
	ms.RemoveMsg(3)
	expected = expected[2:]
	state = ms.State()
	if !reflect.DeepEqual(state.Deleted, expected) {
		t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
	}
	if state.FirstSeq != 5 {
		t.Fatalf("Expected first seq to be 5, got %d", state.FirstSeq)
	}
	ms.Purge()
	if state = ms.State(); len(state.Deleted) != 0 {
		t.Fatalf("Expected no deleted after purge, got %+v\n", state.Deleted)
	}
}

func TestMemStoreStreamTruncate(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}

	tseq := uint64(50)

	subj, toStore := "foo", uint64(100)
	for i := uint64(1); i < tseq; i++ {
		_, _, err := ms.StoreMsg(subj, nil, []byte("ok"))
		require_NoError(t, err)
	}
	subj = "bar"
	for i := tseq; i <= toStore; i++ {
		_, _, err := ms.StoreMsg(subj, nil, []byte("ok"))
		require_NoError(t, err)
	}

	if state := ms.State(); state.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}

	// Check that sequence has to be interior.
	if err := ms.Truncate(toStore + 1); err != ErrInvalidSequence {
		t.Fatalf("Expected err of '%v', got '%v'", ErrInvalidSequence, err)
	}

	if err := ms.Truncate(tseq); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if state := ms.State(); state.Msgs != tseq {
		t.Fatalf("Expected %d msgs, got %d", tseq, state.Msgs)
	}

	// Now make sure we report properly if we have some deleted interior messages.
	ms.RemoveMsg(10)
	ms.RemoveMsg(20)
	ms.RemoveMsg(30)
	ms.RemoveMsg(40)

	tseq = uint64(25)
	if err := ms.Truncate(tseq); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	state := ms.State()
	if state.Msgs != tseq-2 {
		t.Fatalf("Expected %d msgs, got %d", tseq-2, state.Msgs)
	}
	if state.NumSubjects != 1 {
		t.Fatalf("Expected only 1 subject, got %d", state.NumSubjects)
	}
	expected := []uint64{10, 20}
	if !reflect.DeepEqual(state.Deleted, expected) {
		t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
	}
}

func TestMemStorePurgeExWithSubject(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		_, _, err = ms.StoreMsg("foo", nil, nil)
		require_NoError(t, err)
	}

	// This should purge all.
	ms.PurgeEx("foo", 1, 0)
	require_True(t, ms.State().Msgs == 0)
}

func TestMemStoreUpdateMaxMsgsPerSubject(t *testing.T) {
	cfg := &StreamConfig{
		Name:       "TEST",
		Storage:    MemoryStorage,
		Subjects:   []string{"foo"},
		MaxMsgsPer: 10,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)

	// Make sure this is honored on an update.
	cfg.MaxMsgsPer = 50
	err = ms.UpdateConfig(cfg)
	require_NoError(t, err)

	numStored := 22
	for i := 0; i < numStored; i++ {
		_, _, err = ms.StoreMsg("foo", nil, nil)
		require_NoError(t, err)
	}

	ss := ms.SubjectsState("foo")["foo"]
	if ss.Msgs != uint64(numStored) {
		t.Fatalf("Expected to have %d stored, got %d", numStored, ss.Msgs)
	}

	// Now make sure we trunk if setting to lower value.
	cfg.MaxMsgsPer = 10
	err = ms.UpdateConfig(cfg)
	require_NoError(t, err)

	ss = ms.SubjectsState("foo")["foo"]
	if ss.Msgs != 10 {
		t.Fatalf("Expected to have %d stored, got %d", 10, ss.Msgs)
	}
}

func TestMemStoreStreamTruncateReset(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "TEST",
		Storage:  MemoryStorage,
		Subjects: []string{"foo"},
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)

	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 1000; i++ {
		_, _, err := ms.StoreMsg(subj, nil, msg)
		require_NoError(t, err)
	}

	// Reset everything
	require_NoError(t, ms.Truncate(0))

	state := ms.State()
	require_True(t, state.Msgs == 0)
	require_True(t, state.Bytes == 0)
	require_True(t, state.FirstSeq == 0)
	require_True(t, state.LastSeq == 0)
	require_True(t, state.NumSubjects == 0)
	require_True(t, state.NumDeleted == 0)

	for i := 0; i < 1000; i++ {
		_, _, err := ms.StoreMsg(subj, nil, msg)
		require_NoError(t, err)
	}

	state = ms.State()
	require_True(t, state.Msgs == 1000)
	require_True(t, state.Bytes == 30000)
	require_True(t, state.FirstSeq == 1)
	require_True(t, state.LastSeq == 1000)
	require_True(t, state.NumSubjects == 1)
	require_True(t, state.NumDeleted == 0)
}

func TestMemStoreStreamCompactMultiBlockSubjectInfo(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "TEST",
		Storage:  MemoryStorage,
		Subjects: []string{"foo.*"},
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)

	for i := 0; i < 1000; i++ {
		subj := fmt.Sprintf("foo.%d", i)
		_, _, err := ms.StoreMsg(subj, nil, []byte("Hello World"))
		require_NoError(t, err)
	}

	// Compact such that we know we throw blocks away from the beginning.
	deleted, err := ms.Compact(501)
	require_NoError(t, err)
	require_True(t, deleted == 500)

	// Make sure we adjusted for subjects etc.
	state := ms.State()
	require_True(t, state.NumSubjects == 500)
}

func TestMemStoreSubjectsTotals(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "TEST",
		Storage:  MemoryStorage,
		Subjects: []string{"*.*"},
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)

	fmap := make(map[int]int)
	bmap := make(map[int]int)

	var m map[int]int
	var ft string

	for i := 0; i < 10_000; i++ {
		// Flip coin for prefix
		if rand.Intn(2) == 0 {
			ft, m = "foo", fmap
		} else {
			ft, m = "bar", bmap
		}
		dt := rand.Intn(100)
		subj := fmt.Sprintf("%s.%d", ft, dt)
		m[dt]++

		_, _, err := ms.StoreMsg(subj, nil, []byte("Hello World"))
		require_NoError(t, err)
	}

	// Now test SubjectsTotal
	for dt, total := range fmap {
		subj := fmt.Sprintf("foo.%d", dt)
		m := ms.SubjectsTotals(subj)
		if m[subj] != uint64(total) {
			t.Fatalf("Expected %q to have %d total, got %d", subj, total, m[subj])
		}
	}

	// Check fmap.
	if st := ms.SubjectsTotals("foo.*"); len(st) != len(fmap) {
		t.Fatalf("Expected %d subjects for %q, got %d", len(fmap), "foo.*", len(st))
	} else {
		expected := 0
		for _, n := range fmap {
			expected += n
		}
		received := uint64(0)
		for _, n := range st {
			received += n
		}
		if received != uint64(expected) {
			t.Fatalf("Expected %d total but got %d", expected, received)
		}
	}

	// Check bmap.
	if st := ms.SubjectsTotals("bar.*"); len(st) != len(bmap) {
		t.Fatalf("Expected %d subjects for %q, got %d", len(bmap), "bar.*", len(st))
	} else {
		expected := 0
		for _, n := range bmap {
			expected += n
		}
		received := uint64(0)
		for _, n := range st {
			received += n
		}
		if received != uint64(expected) {
			t.Fatalf("Expected %d total but got %d", expected, received)
		}
	}

	// All with pwc match.
	if st, expected := ms.SubjectsTotals("*.*"), len(bmap)+len(fmap); len(st) != expected {
		t.Fatalf("Expected %d subjects for %q, got %d", expected, "*.*", len(st))
	}
}

func TestMemStoreNumPending(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "TEST",
		Storage:  MemoryStorage,
		Subjects: []string{"*.*.*.*"},
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)

	tokens := []string{"foo", "bar", "baz"}
	genSubj := func() string {
		return fmt.Sprintf("%s.%s.%s.%s",
			tokens[rand.Intn(len(tokens))],
			tokens[rand.Intn(len(tokens))],
			tokens[rand.Intn(len(tokens))],
			tokens[rand.Intn(len(tokens))],
		)
	}

	for i := 0; i < 50_000; i++ {
		subj := genSubj()
		_, _, err := ms.StoreMsg(subj, nil, []byte("Hello World"))
		require_NoError(t, err)
	}

	state := ms.State()

	// Scan one by one for sanity check against other calculations.
	sanityCheck := func(sseq uint64, filter string) SimpleState {
		t.Helper()
		var ss SimpleState
		var smv StoreMsg
		// For here we know 0 is invalid, set to 1.
		if sseq == 0 {
			sseq = 1
		}
		for seq := sseq; seq <= state.LastSeq; seq++ {
			sm, err := ms.LoadMsg(seq, &smv)
			if err != nil {
				t.Logf("Encountered error %v loading sequence: %d", err, seq)
				continue
			}
			if subjectIsSubsetMatch(sm.subj, filter) {
				ss.Msgs++
				ss.Last = seq
				if ss.First == 0 || seq < ss.First {
					ss.First = seq
				}
			}
		}
		return ss
	}

	check := func(sseq uint64, filter string) {
		t.Helper()
		np, lvs := ms.NumPending(sseq, filter, false)
		ss := ms.FilteredState(sseq, filter)
		sss := sanityCheck(sseq, filter)
		if lvs != state.LastSeq {
			t.Fatalf("Expected NumPending to return valid through last of %d but got %d", state.LastSeq, lvs)
		}
		if ss.Msgs != np {
			t.Fatalf("NumPending of %d did not match ss.Msgs of %d", np, ss.Msgs)
		}
		if ss != sss {
			t.Fatalf("Failed sanity check, expected %+v got %+v", sss, ss)
		}
	}

	sanityCheckLastOnly := func(sseq uint64, filter string) SimpleState {
		t.Helper()
		var ss SimpleState
		var smv StoreMsg
		// For here we know 0 is invalid, set to 1.
		if sseq == 0 {
			sseq = 1
		}
		seen := make(map[string]bool)
		for seq := state.LastSeq; seq >= sseq; seq-- {
			sm, err := ms.LoadMsg(seq, &smv)
			if err != nil {
				t.Logf("Encountered error %v loading sequence: %d", err, seq)
				continue
			}
			if !seen[sm.subj] && subjectIsSubsetMatch(sm.subj, filter) {
				ss.Msgs++
				if ss.Last == 0 {
					ss.Last = seq
				}
				if ss.First == 0 || seq < ss.First {
					ss.First = seq
				}
				seen[sm.subj] = true
			}
		}
		return ss
	}

	checkLastOnly := func(sseq uint64, filter string) {
		t.Helper()
		np, lvs := ms.NumPending(sseq, filter, true)
		ss := sanityCheckLastOnly(sseq, filter)
		if lvs != state.LastSeq {
			t.Fatalf("Expected NumPending to return valid through last of %d but got %d", state.LastSeq, lvs)
		}
		if ss.Msgs != np {
			t.Fatalf("NumPending of %d did not match ss.Msgs of %d", np, ss.Msgs)
		}
	}

	startSeqs := []uint64{0, 1, 2, 200, 444, 555, 2222, 8888, 12_345, 28_222, 33_456, 44_400, 49_999}
	checkSubs := []string{"foo.>", "*.bar.>", "foo.bar.*.baz", "*.bar.>", "*.foo.bar.*", "foo.foo.bar.baz"}

	for _, filter := range checkSubs {
		for _, start := range startSeqs {
			check(start, filter)
			checkLastOnly(start, filter)
		}
	}
}

func TestMemStoreInitialFirstSeq(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Storage:  MemoryStorage,
		FirstSeq: 1000,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)

	seq, _, err := ms.StoreMsg("A", nil, []byte("OK"))
	require_NoError(t, err)
	if seq != 1000 {
		t.Fatalf("Message should have been sequence 1000 but was %d", seq)
	}

	seq, _, err = ms.StoreMsg("B", nil, []byte("OK"))
	require_NoError(t, err)
	if seq != 1001 {
		t.Fatalf("Message should have been sequence 1001 but was %d", seq)
	}

	var state StreamState
	ms.FastState(&state)
	switch {
	case state.Msgs != 2:
		t.Fatalf("Expected 2 messages, got %d", state.Msgs)
	case state.FirstSeq != 1000:
		t.Fatalf("Expected first seq 1000, got %d", state.FirstSeq)
	case state.LastSeq != 1001:
		t.Fatalf("Expected last seq 1001, got %d", state.LastSeq)
	}
}

func TestMemStoreDeleteBlocks(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{"*"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)

	// Put in 10_000 msgs.
	total := 10_000
	for i := 0; i < total; i++ {
		_, _, err := ms.StoreMsg("A", nil, []byte("OK"))
		require_NoError(t, err)
	}

	// Now pick 5k random sequences.
	delete := 5000
	deleteMap := make(map[int]struct{}, delete)
	for len(deleteMap) < delete {
		deleteMap[rand.Intn(total)+1] = struct{}{}
	}
	// Now remove?
	for seq := range deleteMap {
		ms.RemoveMsg(uint64(seq))
	}

	var state StreamState
	ms.FastState(&state)

	// For now we just track via one dmap.
	ms.mu.RLock()
	dmap := ms.dmap.Clone()
	ms.mu.RUnlock()

	require_True(t, dmap.Size() == state.NumDeleted)
}
