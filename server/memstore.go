// Copyright 2019 The NATS Authors
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
	"sort"
	"sync"
	"time"
)

// TODO(dlc) - This is a fairly simplistic approach but should do for now.
type memStore struct {
	mu     sync.RWMutex
	stats  MsgSetStats
	msgs   map[uint64]*storedMsg
	ageChk *time.Timer
	config MsgSetConfig
}

type storedMsg struct {
	subj string
	msg  []byte
	seq  uint64
	ts   int64 // nanoseconds
}

func newMemStore(cfg *MsgSetConfig) (*memStore, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config required for MsgSetStore")
	}
	if cfg.Storage != MemoryStorage {
		return nil, fmt.Errorf("memStore requires memory storage type in cfg")
	}
	ms := &memStore{msgs: make(map[uint64]*storedMsg), config: *cfg}
	// This only happens once, so ok to call here.
	return ms, nil
}

// Store stores a message.
// both arguments should be copied.
func (ms *memStore) StoreMsg(subj string, msg []byte) (uint64, error) {
	ms.mu.Lock()
	seq := ms.stats.LastSeq + 1
	if ms.stats.FirstSeq == 0 {
		ms.stats.FirstSeq = seq
	}

	// Make copies - https://github.com/go101/go101/wiki
	// TODO(dlc) - Maybe be smarter here.
	msg = append(msg[:0:0], msg...)

	ms.msgs[seq] = &storedMsg{subj, msg, seq, time.Now().UnixNano()}
	ms.stats.Msgs++
	ms.stats.Bytes += memStoreMsgSize(subj, msg)
	ms.stats.LastSeq = seq

	// Limits checks and enforcement.
	ms.enforceMsgLimit()
	ms.enforceBytesLimit()

	// Check it we have and need age expiration timer running.
	if ms.ageChk == nil && ms.config.MaxAge != 0 {
		ms.startAgeChk()
	}
	ms.mu.Unlock()

	return seq, nil
}

// GetSeqFromTime looks for the first sequence number that has the message
// with >= timestamp.
func (ms *memStore) GetSeqFromTime(t time.Time) uint64 {
	ts := t.UnixNano()
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if len(ms.msgs) == 0 {
		return ms.stats.LastSeq + 1
	}
	if ts <= ms.msgs[ms.stats.FirstSeq].ts {
		return ms.stats.FirstSeq
	}
	last := ms.msgs[ms.stats.LastSeq].ts
	if ts == last {
		return ms.stats.LastSeq
	}
	if ts > last {
		return ms.stats.LastSeq + 1
	}
	index := sort.Search(len(ms.msgs), func(i int) bool {
		return ms.msgs[uint64(i)+ms.stats.FirstSeq].ts >= ts
	})
	return uint64(index) + ms.stats.FirstSeq
}

// Will check the msg limit and drop firstSeq msg if needed.
// Lock should be held.
func (ms *memStore) enforceMsgLimit() {
	if ms.config.MaxMsgs == 0 || ms.stats.Msgs <= ms.config.MaxMsgs {
		return
	}
	ms.deleteFirstMsgOrPanic()
}

// Will check the bytes limit and drop msgs if needed.
// Lock should be held.
func (ms *memStore) enforceBytesLimit() {
	if ms.config.MaxBytes == 0 || ms.stats.Bytes <= ms.config.MaxBytes {
		return
	}
	for bs := ms.stats.Bytes; bs > ms.config.MaxBytes; bs = ms.stats.Bytes {
		ms.deleteFirstMsgOrPanic()
	}
}

// Will start the age check timer.
// Lock should be held.
func (ms *memStore) startAgeChk() {
	if ms.ageChk == nil && ms.config.MaxAge != 0 {
		ms.ageChk = time.AfterFunc(ms.config.MaxAge, ms.expireMsgs)
	}
}

// Will expire msgs that are too old.
func (ms *memStore) expireMsgs() {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	now := time.Now().UnixNano()
	minAge := now - int64(ms.config.MaxAge)
	for {
		if sm, ok := ms.msgs[ms.stats.FirstSeq]; ok && sm.ts <= minAge {
			ms.deleteFirstMsgOrPanic()
		} else {
			if !ok {
				ms.ageChk.Stop()
				ms.ageChk = nil
			} else {
				fireIn := time.Duration(sm.ts-now) + ms.config.MaxAge
				ms.ageChk.Reset(fireIn)
			}
			return
		}
	}
}

func (ms *memStore) deleteFirstMsgOrPanic() {
	if !ms.deleteFirstMsg() {
		panic("jetstream memstore has inconsistent state, can't find firstSeq msg")
	}
}

func (ms *memStore) deleteFirstMsg() bool {
	sm, ok := ms.msgs[ms.stats.FirstSeq]
	if !ok || sm == nil {
		return false
	}
	delete(ms.msgs, ms.stats.FirstSeq)
	ms.stats.FirstSeq++
	ms.stats.Msgs--
	ms.stats.Bytes -= memStoreMsgSize(sm.subj, sm.msg)
	return true
}

func (ms *memStore) Lookup(seq uint64) (string, []byte, int64, error) {
	ms.mu.RLock()
	sm, ok := ms.msgs[seq]
	ms.mu.RUnlock()

	if !ok || sm == nil {
		return "", nil, 0, ErrStoreMsgNotFound
	}
	return sm.subj, sm.msg, sm.ts, nil
}

func (ms *memStore) Stats() MsgSetStats {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.stats
}

func memStoreMsgSize(subj string, msg []byte) uint64 {
	return uint64(len(subj) + len(msg) + 16) // 8*2 for seq + age
}
