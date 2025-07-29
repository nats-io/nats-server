// Copyright 2012-2025 The NATS Authors
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
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server/gsl"
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
			sl := gsl.NewSublist[struct{}]()
			sl.Insert("foo.>", struct{}{})
			for i, seq := 0, uint64(1); i < 1000; i++ {
				sm, nseq, err := fs.LoadNextMsgMulti(sl, seq, &smv)
				require_NoError(t, err)
				require_Equal(t, sm.subj, fmt.Sprintf("foo.%d", i))
				require_Equal(t, nseq, seq)
				seq++
			}

			// Now do multi load next with 1000 literal subjects.
			sl = gsl.NewSublist[struct{}]()
			for i := 0; i < 1000; i++ {
				subj := fmt.Sprintf("foo.%d", i)
				sl.Insert(subj, struct{}{})
			}
			for i, seq := 0, uint64(1); i < 1000; i++ {
				sm, nseq, err := fs.LoadNextMsgMulti(sl, seq, &smv)
				require_NoError(t, err)
				require_Equal(t, sm.subj, fmt.Sprintf("foo.%d", i))
				require_Equal(t, nseq, seq)
				seq++
			}

			// Check that we can pull out 3 individuals.
			sl = gsl.NewSublist[struct{}]()
			sl.Insert("foo.2", struct{}{})
			sl.Insert("foo.222", struct{}{})
			sl.Insert("foo.999", struct{}{})
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

func TestStoreUpdateConfigTTLState(t *testing.T) {
	config := func() StreamConfig {
		return StreamConfig{Name: "TEST", Subjects: []string{"foo"}}
	}
	testAllStoreAllPermutations(
		t, false, config(),
		func(t *testing.T, fs StreamStore) {
			cfg := config()
			switch fs.(type) {
			case *fileStore:
				cfg.Storage = FileStorage
			case *memStore:
				cfg.Storage = MemoryStorage
			}

			// TTLs disabled at this point so this message should survive.
			seq, _, err := fs.StoreMsg("foo", nil, nil, 1)
			require_NoError(t, err)
			time.Sleep(2 * time.Second)
			_, err = fs.LoadMsg(seq, nil)
			require_NoError(t, err)

			// Now enable TTLs.
			cfg.AllowMsgTTL = true
			require_NoError(t, fs.UpdateConfig(&cfg))

			// TTLs enabled at this point so this message should be cleaned up.
			seq, _, err = fs.StoreMsg("foo", nil, nil, 1)
			require_NoError(t, err)
			time.Sleep(2 * time.Second)
			_, err = fs.LoadMsg(seq, nil)
			require_Error(t, err)

			// Now disable TTLs again.
			cfg.AllowMsgTTL = false
			require_NoError(t, fs.UpdateConfig(&cfg))

			// TTLs disabled again so this message should survive.
			seq, _, err = fs.StoreMsg("foo", nil, nil, 1)
			require_NoError(t, err)
			time.Sleep(2 * time.Second)
			_, err = fs.LoadMsg(seq, nil)
			require_NoError(t, err)
		},
	)
}

func TestStoreStreamInteriorDeleteAccounting(t *testing.T) {
	tests := []struct {
		title  string
		action func(s StreamStore, lseq uint64)
	}{
		{
			title: "TruncateWithRemove",
			action: func(s StreamStore, lseq uint64) {
				seq, _, err := s.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
				require_Equal(t, seq, lseq)
				removed, err := s.RemoveMsg(lseq)
				require_NoError(t, err)
				require_True(t, removed)
				require_NoError(t, s.Truncate(lseq))
			},
		},
		{
			title: "TruncateWithErase",
			action: func(s StreamStore, lseq uint64) {
				seq, _, err := s.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
				require_Equal(t, seq, lseq)
				removed, err := s.EraseMsg(lseq)
				require_NoError(t, err)
				require_True(t, removed)
				require_NoError(t, s.Truncate(lseq))
			},
		},
		{
			title: "TruncateWithTombstone",
			action: func(s StreamStore, lseq uint64) {
				seq, _, err := s.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
				require_Equal(t, seq, lseq)
				if fs, ok := s.(*fileStore); ok {
					removed, err := fs.removeMsg(lseq, false, false, true)
					require_NoError(t, err)
					require_True(t, removed)
				} else {
					removed, err := s.RemoveMsg(lseq)
					require_NoError(t, err)
					require_True(t, removed)
				}
				require_NoError(t, s.Truncate(lseq))
			},
		},
		{
			title: "SkipMsg",
			action: func(s StreamStore, lseq uint64) {
				s.SkipMsg()
			},
		},
		{
			title: "SkipMsgs",
			action: func(s StreamStore, lseq uint64) {
				require_NoError(t, s.SkipMsgs(lseq, 1))
			},
		},
	}
	for _, empty := range []bool{false, true} {
		for _, test := range tests {
			t.Run(fmt.Sprintf("Empty=%v/%s", empty, test.title), func(t *testing.T) {
				cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}}
				testAllStoreAllPermutations(t, true, cfg, func(t *testing.T, s StreamStore) {
					var err error
					var lseq uint64
					if !empty {
						lseq, _, err = s.StoreMsg("foo", nil, nil, 0)
						require_NoError(t, err)
						require_Equal(t, lseq, 1)
					}
					lseq++

					test.action(s, lseq)

					// Confirm state as baseline.
					before := s.State()
					if empty {
						require_Equal(t, before.Msgs, 0)
						require_Equal(t, before.FirstSeq, 2)
						require_Equal(t, before.LastSeq, 1)
					} else {
						require_Equal(t, before.Msgs, 1)
						require_Equal(t, before.FirstSeq, 1)
						require_Equal(t, before.LastSeq, 2)
					}

					var fs *fileStore
					var ok bool
					if fs, ok = s.(*fileStore); !ok {
						return
					}
					cfg.Storage = FileStorage
					fcfg := fs.fcfg
					created := time.Time{}

					// Restart should equal state.
					require_NoError(t, fs.Stop())
					fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
					require_NoError(t, err)
					defer fs.Stop()

					if state := fs.State(); !reflect.DeepEqual(state, before) {
						t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
					}

					// Stop and remove stream state file.
					require_NoError(t, fs.Stop())
					require_NoError(t, os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)))

					// Recovering based on blocks should result in the same state.
					fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
					require_NoError(t, err)
					defer fs.Stop()

					if state := fs.State(); !reflect.DeepEqual(state, before) {
						t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
					}

					// Rebuilding state must also result in the same state.
					fs.rebuildState(nil)
					if state := fs.State(); !reflect.DeepEqual(state, before) {
						t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
					}
				})
			})
		}
	}
}
