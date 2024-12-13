// Copyright 2012-2024 The NATS Authors
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

//go:build !skip_store_tests
// +build !skip_store_tests

package server

import (
	"fmt"
	"testing"
)

func testAllStoreAllPermutations(t *testing.T, compressionAndEncryption bool, cfg StreamConfig, fn func(t *testing.T, fs StreamStore)) {
	t.Run("Memory", func(t *testing.T) {
		cfg.Storage = MemoryStorage
		fs, err := newMemStore(&cfg)
		require_NoError(t, err)
		defer fs.Stop()
		fn(t, fs)
	})
	t.Run("File", func(t *testing.T) {
		cfg.Storage = FileStorage
		if compressionAndEncryption {
			testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
				fs, err := newFileStore(fcfg, cfg)
				require_NoError(t, err)
				defer fs.Stop()
				fn(t, fs)
			})
		} else {
			fs, err := newFileStore(FileStoreConfig{
				StoreDir: t.TempDir(),
			}, cfg)
			require_NoError(t, err)
			defer fs.Stop()
			fn(t, fs)
		}
	})
}

func TestStoreMsgLoadNextMsgMulti(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}},
		func(t *testing.T, fs StreamStore) {
			// Put 1k msgs in
			for i := 0; i < 1000; i++ {
				subj := fmt.Sprintf("foo.%d", i)
				fs.StoreMsg(subj, nil, []byte("ZZZ"))
			}

			var smv StoreMsg
			// Do multi load next with 1 wc entry.
			sl := NewSublistWithCache()
			sl.Insert(&subscription{subject: []byte("foo.>")})
			for i, seq := 0, uint64(1); i < 1000; i++ {
				sm, nseq, err := fs.LoadNextMsgMulti(sl, seq, &smv)
				require_NoError(t, err)
				require_Equal(t, sm.subj, fmt.Sprintf("foo.%d", i))
				require_Equal(t, nseq, seq)
				seq++
			}

			// Now do multi load next with 1000 literal subjects.
			sl = NewSublistWithCache()
			for i := 0; i < 1000; i++ {
				subj := fmt.Sprintf("foo.%d", i)
				sl.Insert(&subscription{subject: []byte(subj)})
			}
			for i, seq := 0, uint64(1); i < 1000; i++ {
				sm, nseq, err := fs.LoadNextMsgMulti(sl, seq, &smv)
				require_NoError(t, err)
				require_Equal(t, sm.subj, fmt.Sprintf("foo.%d", i))
				require_Equal(t, nseq, seq)
				seq++
			}

			// Check that we can pull out 3 individuals.
			sl = NewSublistWithCache()
			sl.Insert(&subscription{subject: []byte("foo.2")})
			sl.Insert(&subscription{subject: []byte("foo.222")})
			sl.Insert(&subscription{subject: []byte("foo.999")})
			sm, seq, err := fs.LoadNextMsgMulti(sl, 1, &smv)
			require_NoError(t, err)
			require_Equal(t, sm.subj, "foo.2")
			require_Equal(t, seq, 3)
			sm, seq, err = fs.LoadNextMsgMulti(sl, seq+1, &smv)
			require_NoError(t, err)
			require_Equal(t, sm.subj, "foo.222")
			require_Equal(t, seq, 223)
			sm, seq, err = fs.LoadNextMsgMulti(sl, seq+1, &smv)
			require_NoError(t, err)
			require_Equal(t, sm.subj, "foo.999")
			require_Equal(t, seq, 1000)
			_, seq, err = fs.LoadNextMsgMulti(sl, seq+1, &smv)
			require_Error(t, err)
			require_Equal(t, seq, 1000)
		},
	)
}

func TestStoreDeleteSlice(t *testing.T) {
	ds := DeleteSlice{2}
	var deletes []uint64
	ds.Range(func(seq uint64) bool {
		deletes = append(deletes, seq)
		return true
	})
	require_Len(t, len(deletes), 1)
	require_Equal(t, deletes[0], 2)

	first, last, num := ds.State()
	require_Equal(t, first, 2)
	require_Equal(t, last, 2)
	require_Equal(t, num, 1)
}

func TestStoreDeleteRange(t *testing.T) {
	dr := DeleteRange{First: 2, Num: 1}
	var deletes []uint64
	dr.Range(func(seq uint64) bool {
		deletes = append(deletes, seq)
		return true
	})
	require_Len(t, len(deletes), 1)
	require_Equal(t, deletes[0], 2)

	first, last, num := dr.State()
	require_Equal(t, first, 2)
	require_Equal(t, last, 2)
	require_Equal(t, num, 1)
}

func TestStoreSubjectStateConsistency(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "TEST", Subjects: []string{"foo"}},
		func(t *testing.T, fs StreamStore) {
			getSubjectState := func() SimpleState {
				t.Helper()
				ss := fs.SubjectsState("foo")
				return ss["foo"]
			}
			var smp StoreMsg
			expectFirstSeq := func(eseq uint64) {
				t.Helper()
				sm, _, err := fs.LoadNextMsg("foo", false, 0, &smp)
				require_NoError(t, err)
				require_Equal(t, sm.seq, eseq)
			}
			expectLastSeq := func(eseq uint64) {
				t.Helper()
				sm, err := fs.LoadLastMsg("foo", &smp)
				require_NoError(t, err)
				require_Equal(t, sm.seq, eseq)
			}

			// Publish an initial batch of messages.
			for i := 0; i < 4; i++ {
				_, _, err := fs.StoreMsg("foo", nil, nil)
				require_NoError(t, err)
			}

			// Expect 4 msgs, with first=1, last=4.
			ss := getSubjectState()
			require_Equal(t, ss.Msgs, 4)
			require_Equal(t, ss.First, 1)
			expectFirstSeq(1)
			require_Equal(t, ss.Last, 4)
			expectLastSeq(4)

			// Remove first message, ss.First is lazy so will only mark ss.firstNeedsUpdate.
			removed, err := fs.RemoveMsg(1)
			require_NoError(t, err)
			require_True(t, removed)

			// Will update first, so corrects to seq 2.
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 3)
			require_Equal(t, ss.First, 2)
			expectFirstSeq(2)
			require_Equal(t, ss.Last, 4)
			expectLastSeq(4)

			// Remove last message, ss.Last is lazy so will only mark ss.lastNeedsUpdate.
			removed, err = fs.RemoveMsg(4)
			require_NoError(t, err)
			require_True(t, removed)

			// Will update last, so corrects to 3.
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 2)
			require_Equal(t, ss.First, 2)
			expectFirstSeq(2)
			require_Equal(t, ss.Last, 3)
			expectLastSeq(3)

			// Remove first message again.
			removed, err = fs.RemoveMsg(2)
			require_NoError(t, err)
			require_True(t, removed)

			// Since we only have one message left, must update ss.First and ensure ss.Last equals.
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 1)
			require_Equal(t, ss.First, 3)
			expectFirstSeq(3)
			require_Equal(t, ss.Last, 3)
			expectLastSeq(3)

			// Publish some more messages so we can test another scenario.
			for i := 0; i < 3; i++ {
				_, _, err := fs.StoreMsg("foo", nil, nil)
				require_NoError(t, err)
			}

			// Just check the state is complete again.
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 4)
			require_Equal(t, ss.First, 3)
			expectFirstSeq(3)
			require_Equal(t, ss.Last, 7)
			expectLastSeq(7)

			// Remove last sequence, ss.Last is lazy so doesn't get updated.
			removed, err = fs.RemoveMsg(7)
			require_NoError(t, err)
			require_True(t, removed)

			// Remove first sequence, ss.First is lazy so doesn't get updated.
			removed, err = fs.RemoveMsg(3)
			require_NoError(t, err)
			require_True(t, removed)

			// Remove (now) first sequence. Both ss.First and ss.Last are lazy and both need to be recalculated later.
			removed, err = fs.RemoveMsg(5)
			require_NoError(t, err)
			require_True(t, removed)

			// ss.First and ss.Last should both be recalculated and equal each other.
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 1)
			require_Equal(t, ss.First, 6)
			expectFirstSeq(6)
			require_Equal(t, ss.Last, 6)
			expectLastSeq(6)
		},
	)
}

func TestStoreMaxMsgsPerUpdateBug(t *testing.T) {
	config := func() StreamConfig {
		return StreamConfig{Name: "TEST", Subjects: []string{"foo"}, MaxMsgsPer: 0}
	}
	testAllStoreAllPermutations(
		t, false, config(),
		func(t *testing.T, fs StreamStore) {
			for i := 0; i < 5; i++ {
				_, _, err := fs.StoreMsg("foo", nil, nil)
				require_NoError(t, err)
			}

			ss := fs.State()
			require_Equal(t, ss.Msgs, 5)
			require_Equal(t, ss.FirstSeq, 1)
			require_Equal(t, ss.LastSeq, 5)

			// Update max messages per-subject from 0 (infinite) to 1.
			// Since the per-subject limit was not specified before, messages should be removed upon config update.
			cfg := config()
			if _, ok := fs.(*fileStore); ok {
				cfg.Storage = FileStorage
			} else {
				cfg.Storage = MemoryStorage
			}
			cfg.MaxMsgsPer = 1
			err := fs.UpdateConfig(&cfg)
			require_NoError(t, err)

			// Only one message should remain.
			ss = fs.State()
			require_Equal(t, ss.Msgs, 1)
			require_Equal(t, ss.FirstSeq, 5)
			require_Equal(t, ss.LastSeq, 5)

			// Update max messages per-subject from 0 (infinite) to an invalid value (< -1).
			cfg.MaxMsgsPer = -2
			err = fs.UpdateConfig(&cfg)
			require_NoError(t, err)
			require_Equal(t, cfg.MaxMsgsPer, -1)
		},
	)
}
