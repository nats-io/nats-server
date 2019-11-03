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
	"math/rand"
	"sort"
	"sync"
	"time"
)

// TODO(dlc) - This is a fairly simplistic approach but should do for now.
type memStore struct {
	mu     sync.RWMutex
	stats  MsgSetStats
	msgs   map[uint64]*storedMsg
	scb    func(int64)
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
		return nil, fmt.Errorf("config required")
	}
	if cfg.Storage != MemoryStorage {
		return nil, fmt.Errorf("memStore requires memory storage type in config")
	}
	ms := &memStore{msgs: make(map[uint64]*storedMsg), config: *cfg}
	// This only happens once, so ok to call here.
	return ms, nil
}

// Store stores a message.
func (ms *memStore) StoreMsg(subj string, msg []byte) (uint64, error) {
	ms.mu.Lock()
	seq := ms.stats.LastSeq + 1
	if ms.stats.FirstSeq == 0 {
		ms.stats.FirstSeq = seq
	}

	// Make copies - https://github.com/go101/go101/wiki
	// TODO(dlc) - Maybe be smarter here.
	if len(msg) > 0 {
		msg = append(msg[:0:0], msg...)
	}

	startBytes := int64(ms.stats.Bytes)

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
	cb := ms.scb
	stopBytes := int64(ms.stats.Bytes)
	ms.mu.Unlock()

	if cb != nil {
		cb(stopBytes - startBytes)
	}

	return seq, nil
}

// StorageBytesUpdate registers an async callback for updates to storage changes.
func (ms *memStore) StorageBytesUpdate(cb func(int64)) {
	ms.mu.Lock()
	ms.scb = cb
	ms.mu.Unlock()
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
	if ms.config.MaxMsgs <= 0 || ms.stats.Msgs <= uint64(ms.config.MaxMsgs) {
		return
	}
	ms.deleteFirstMsgOrPanic()
}

// Will check the bytes limit and drop msgs if needed.
// Lock should be held.
func (ms *memStore) enforceBytesLimit() {
	if ms.config.MaxBytes <= 0 || ms.stats.Bytes <= uint64(ms.config.MaxBytes) {
		return
	}
	for bs := ms.stats.Bytes; bs > uint64(ms.config.MaxBytes); bs = ms.stats.Bytes {
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

// Purge will remove all messages from this store.
// Will return the number of purged messages.
func (ms *memStore) Purge() uint64 {
	ms.mu.Lock()
	purged := uint64(len(ms.msgs))
	cb := ms.scb
	bytes := int64(ms.stats.Bytes)
	ms.stats.FirstSeq = ms.stats.LastSeq + 1
	ms.stats.Bytes = 0
	ms.stats.Msgs = 0
	ms.msgs = make(map[uint64]*storedMsg)
	ms.mu.Unlock()

	if cb != nil {
		cb(-bytes)
	}

	return purged
}

func (ms *memStore) deleteFirstMsgOrPanic() {
	if !ms.deleteFirstMsg() {
		panic("jetstream memstore has inconsistent state, can't find first seq msg")
	}
}

func (ms *memStore) deleteFirstMsg() bool {
	return ms.removeMsg(ms.stats.FirstSeq, false)
}

// LoadMsg will lookup the message by sequence number and return it if found.
func (ms *memStore) LoadMsg(seq uint64) (string, []byte, int64, error) {
	ms.mu.RLock()
	sm, ok := ms.msgs[seq]
	ms.mu.RUnlock()

	if !ok || sm == nil {
		return "", nil, 0, ErrStoreMsgNotFound
	}
	return sm.subj, sm.msg, sm.ts, nil
}

// RemoveMsg will remove the message from this store.
// Will return the number of bytes removed.
func (ms *memStore) RemoveMsg(seq uint64) bool {
	ms.mu.Lock()
	removed := ms.removeMsg(seq, false)
	ms.mu.Unlock()
	return removed
}

// EraseMsg will remove the message and rewrite its contents.
func (ms *memStore) EraseMsg(seq uint64) bool {
	ms.mu.Lock()
	removed := ms.removeMsg(seq, true)
	ms.mu.Unlock()
	return removed
}

// Removes the message referenced by seq.
func (ms *memStore) removeMsg(seq uint64, secure bool) bool {
	var ss uint64
	sm, ok := ms.msgs[seq]
	if ok {
		delete(ms.msgs, seq)
		ms.stats.Msgs--
		ss = memStoreMsgSize(sm.subj, sm.msg)
		ms.stats.Bytes -= ss
		if seq == ms.stats.FirstSeq {
			ms.stats.FirstSeq++
		}
		if secure {
			rand.Read(sm.msg)
			sm.seq = 0
		}
	}
	if ms.scb != nil {
		delta := int64(ss)
		ms.scb(-delta)
	}
	return ok
}

func (ms *memStore) Stats() MsgSetStats {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.stats
}

func memStoreMsgSize(subj string, msg []byte) uint64 {
	return uint64(len(subj) + len(msg) + 16) // 8*2 for seq + age
}

// Delete is same as Stop for memory store.
func (ms *memStore) Delete() {
	ms.Purge()
	ms.Stop()
}

func (ms *memStore) Stop() {
	ms.mu.Lock()
	if ms.ageChk != nil {
		ms.ageChk.Stop()
		ms.ageChk = nil
	}
	ms.msgs = nil
	ms.mu.Unlock()
}

type observableMemStore struct{}

func (ms *memStore) ObservableStore(_ string, _ *ObservableConfig) (ObservableStore, error) {
	return &observableMemStore{}, nil
}

// No-ops.
func (os *observableMemStore) Update(_ *ObservableState) error {
	return nil
}
func (os *observableMemStore) Stop()                            {}
func (os *observableMemStore) State() (*ObservableState, error) { return nil, nil }
