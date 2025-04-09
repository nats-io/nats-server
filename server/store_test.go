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
				fs.StoreMsg(subj, nil, []byte("ZZZ"), 0)
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
				_, _, err := fs.StoreMsg("foo", nil, nil, 0)
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
				_, _, err := fs.StoreMsg("foo", nil, nil, 0)
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

			// We store a new message for ss.Last and remove it after, which marks it to be recalculated.
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
			removed, err = fs.RemoveMsg(8)
			require_NoError(t, err)
			require_True(t, removed)
			// This will be the new ss.Last message, so reset ss.lastNeedsUpdate
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)

			// ss.First should remain the same, but ss.Last should equal the last message.
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 2)
			require_Equal(t, ss.First, 6)
			expectFirstSeq(6)
			require_Equal(t, ss.Last, 9)
			expectLastSeq(9)
		},
	)
}

func TestStoreSubjectStateConsistencyOptimization(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "TEST", Subjects: []string{"foo"}},
		func(t *testing.T, fs StreamStore) {
			fillMsgs := func(c int) {
				t.Helper()
				for i := 0; i < c; i++ {
					_, _, err := fs.StoreMsg("foo", nil, nil, 0)
					require_NoError(t, err)
				}
			}
			removeMsgs := func(seqs ...uint64) {
				t.Helper()
				for _, seq := range seqs {
					removed, err := fs.RemoveMsg(seq)
					require_NoError(t, err)
					require_True(t, removed)
				}
			}
			getSubjectState := func() (ss *SimpleState) {
				t.Helper()
				if f, ok := fs.(*fileStore); ok {
					ss, ok = f.lmb.fss.Find([]byte("foo"))
					require_True(t, ok)
				} else if ms, ok := fs.(*memStore); ok {
					ss, ok = ms.fss.Find([]byte("foo"))
					require_True(t, ok)
				} else {
					t.Fatal("Store not supported")
				}
				return ss
			}
			var smp StoreMsg
			expectSeq := func(seq uint64) {
				t.Helper()
				sm, _, err := fs.LoadNextMsg("foo", false, 0, &smp)
				require_NoError(t, err)
				require_Equal(t, sm.seq, seq)
				sm, err = fs.LoadLastMsg("foo", &smp)
				require_NoError(t, err)
				require_Equal(t, sm.seq, seq)
			}

			// results in ss.Last, ss.First is marked lazy (when we hit ss.Msgs-1==1).
			fillMsgs(3)
			removeMsgs(2, 1)
			ss := getSubjectState()
			require_Equal(t, ss.Msgs, 1)
			require_Equal(t, ss.First, 3)
			require_Equal(t, ss.Last, 3)
			require_False(t, ss.firstNeedsUpdate)
			require_False(t, ss.lastNeedsUpdate)
			expectSeq(3)

			// ss.First is marked lazy first, then ss.Last is marked lazy (when we hit ss.Msgs-1==1).
			fillMsgs(2)
			removeMsgs(3, 5)
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 1)
			require_Equal(t, ss.First, 3)
			require_Equal(t, ss.Last, 5)
			require_True(t, ss.firstNeedsUpdate)
			require_True(t, ss.lastNeedsUpdate)
			expectSeq(4)

			// ss.Last is marked lazy first, then ss.First is marked lazy (when we hit ss.Msgs-1==1).
			fillMsgs(2)
			removeMsgs(7, 4)
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 1)
			require_Equal(t, ss.First, 4)
			require_Equal(t, ss.Last, 7)
			require_True(t, ss.firstNeedsUpdate)
			require_True(t, ss.lastNeedsUpdate)
			expectSeq(6)

			// ss.Msgs=1, results in ss.First, ss.Last is marked lazy (when we hit ss.Msgs-1==1).
			fillMsgs(2)
			removeMsgs(9, 8)
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 1)
			require_Equal(t, ss.First, 6)
			require_Equal(t, ss.Last, 6)
			require_False(t, ss.firstNeedsUpdate)
			require_False(t, ss.lastNeedsUpdate)
			expectSeq(6)
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
				_, _, err := fs.StoreMsg("foo", nil, nil, 0)
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

func TestStoreCompactCleansUpDmap(t *testing.T) {
	config := func() StreamConfig {
		return StreamConfig{Name: "TEST", Subjects: []string{"foo"}, MaxMsgsPer: 0}
	}
	for cseq := uint64(2); cseq <= 4; cseq++ {
		t.Run(fmt.Sprintf("Compact(%d)", cseq), func(t *testing.T) {
			testAllStoreAllPermutations(
				t, false, config(),
				func(t *testing.T, fs StreamStore) {
					dmapEntries := func() int {
						if fss, ok := fs.(*fileStore); ok {
							return fss.dmapEntries()
						} else if mss, ok := fs.(*memStore); ok {
							mss.mu.RLock()
							defer mss.mu.RUnlock()
							return mss.dmap.Size()
						} else {
							return 0
						}
					}

					// Publish messages, should have no interior deletes.
					for i := 0; i < 3; i++ {
						_, _, err := fs.StoreMsg("foo", nil, nil, 0)
						require_NoError(t, err)
					}
					require_Len(t, dmapEntries(), 0)

					// Removing one message in the middle should be an interior delete.
					_, err := fs.RemoveMsg(2)
					require_NoError(t, err)
					require_Len(t, dmapEntries(), 1)

					// Compacting must always clean up the interior delete.
					_, err = fs.Compact(cseq)
					require_NoError(t, err)
					require_Len(t, dmapEntries(), 0)

					// Validate first/last sequence.
					state := fs.State()
					fseq := uint64(3)
					if fseq < cseq {
						fseq = cseq
					}
					require_Equal(t, state.FirstSeq, fseq)
					require_Equal(t, state.LastSeq, 3)
				})
		})
	}
}

func TestStoreTruncateCleansUpDmap(t *testing.T) {
	config := func() StreamConfig {
		return StreamConfig{Name: "TEST", Subjects: []string{"foo"}, MaxMsgsPer: 0}
	}
	for tseq := uint64(0); tseq <= 1; tseq++ {
		t.Run(fmt.Sprintf("Truncate(%d)", tseq), func(t *testing.T) {
			testAllStoreAllPermutations(
				t, false, config(),
				func(t *testing.T, fs StreamStore) {
					dmapEntries := func() int {
						if fss, ok := fs.(*fileStore); ok {
							return fss.dmapEntries()
						} else if mss, ok := fs.(*memStore); ok {
							mss.mu.RLock()
							defer mss.mu.RUnlock()
							return mss.dmap.Size()
						} else {
							return 0
						}
					}

					// Publish messages, should have no interior deletes.
					for i := 0; i < 3; i++ {
						_, _, err := fs.StoreMsg("foo", nil, nil, 0)
						require_NoError(t, err)
					}
					require_Len(t, dmapEntries(), 0)

					// Removing one message in the middle should be an interior delete.
					_, err := fs.RemoveMsg(2)
					require_NoError(t, err)
					require_Len(t, dmapEntries(), 1)

					// Truncating must always clean up the interior delete.
					err = fs.Truncate(tseq)
					require_NoError(t, err)
					require_Len(t, dmapEntries(), 0)

					// Validate first/last sequence.
					state := fs.State()
					fseq := uint64(1)
					if fseq > tseq {
						fseq = tseq
					}
					require_Equal(t, state.FirstSeq, fseq)
					require_Equal(t, state.LastSeq, tseq)
				})
		})
	}
}

// https://github.com/nats-io/nats-server/issues/6709
func TestStorePurgeExZero(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "TEST", Subjects: []string{"foo"}},
		func(t *testing.T, fs StreamStore) {
			// Simple purge all.
			_, err := fs.Purge()
			require_NoError(t, err)
			ss := fs.State()
			require_Equal(t, ss.FirstSeq, 1)
			require_Equal(t, ss.LastSeq, 0)

			// PurgeEx(seq=0) must be equal.
			_, err = fs.PurgeEx(_EMPTY_, 0, 0)
			require_NoError(t, err)
			ss = fs.State()
			require_Equal(t, ss.FirstSeq, 1)
			require_Equal(t, ss.LastSeq, 0)
		},
	)
}
