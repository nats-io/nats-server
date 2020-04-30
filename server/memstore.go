// Copyright 2019-2020 The NATS Authors
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
	"io"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// TODO(dlc) - This is a fairly simplistic approach but should do for now.
type memStore struct {
	mu        sync.RWMutex
	cfg       StreamConfig
	state     StreamState
	msgs      map[uint64]*storedMsg
	scb       func(int64)
	ageChk    *time.Timer
	consumers int
}

type storedMsg struct {
	subj string
	msg  []byte
	seq  uint64
	ts   int64 // nanoseconds
}

func newMemStore(cfg *StreamConfig) (*memStore, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config required")
	}
	if cfg.Storage != MemoryStorage {
		return nil, fmt.Errorf("memStore requires memory storage type in config")
	}
	return &memStore{msgs: make(map[uint64]*storedMsg), cfg: *cfg}, nil
}

func (ms *memStore) UpdateConfig(cfg *StreamConfig) error {
	if cfg == nil {
		return fmt.Errorf("config required")
	}
	if cfg.Storage != MemoryStorage {
		return fmt.Errorf("memStore requires memory storage type in config")
	}

	ms.mu.Lock()
	ms.cfg = *cfg
	// Limits checks and enforcement.
	ms.enforceMsgLimit()
	ms.enforceBytesLimit()
	// Do age timers.
	if ms.ageChk == nil && ms.cfg.MaxAge != 0 {
		ms.startAgeChk()
	}
	if ms.ageChk != nil && ms.cfg.MaxAge == 0 {
		ms.ageChk.Stop()
		ms.ageChk = nil
	}
	ms.mu.Unlock()

	if cfg.MaxAge != 0 {
		ms.expireMsgs()
	}
	return nil
}

// Store stores a message.
func (ms *memStore) StoreMsg(subj string, msg []byte) (uint64, int64, error) {
	ms.mu.Lock()

	// Check if we are discarding new messages when we reach the limit.
	if ms.cfg.Discard == DiscardNew {
		if ms.cfg.MaxMsgs > 0 && ms.state.Msgs >= uint64(ms.cfg.MaxMsgs) {
			ms.mu.Unlock()
			return 0, 0, ErrMaxMsgs
		}
		if ms.cfg.MaxBytes > 0 && ms.state.Bytes+uint64(len(msg)) >= uint64(ms.cfg.MaxBytes) {
			ms.mu.Unlock()
			return 0, 0, ErrMaxBytes
		}
	}

	seq := ms.state.LastSeq + 1
	if ms.state.FirstSeq == 0 {
		ms.state.FirstSeq = seq
	}

	// Make copies - https://github.com/go101/go101/wiki
	// TODO(dlc) - Maybe be smarter here.
	if len(msg) > 0 {
		msg = append(msg[:0:0], msg...)
	}

	startBytes := int64(ms.state.Bytes)
	ts := time.Now().UnixNano()
	ms.msgs[seq] = &storedMsg{subj, msg, seq, ts}
	ms.state.Msgs++
	ms.state.Bytes += memStoreMsgSize(subj, msg)
	ms.state.LastSeq = seq

	// Limits checks and enforcement.
	ms.enforceMsgLimit()
	ms.enforceBytesLimit()

	// Check if we have and need the age expiration timer running.
	if ms.ageChk == nil && ms.cfg.MaxAge != 0 {
		ms.startAgeChk()
	}
	cb := ms.scb
	stopBytes := int64(ms.state.Bytes)
	ms.mu.Unlock()

	if cb != nil {
		cb(stopBytes - startBytes)
	}

	return seq, ts, nil
}

// StorageBytesUpdate registers an async callback for updates to storage changes.
func (ms *memStore) StorageBytesUpdate(cb func(int64)) {
	ms.mu.Lock()
	ms.scb = cb
	ms.mu.Unlock()
}

// GetSeqFromTime looks for the first sequence number that has the message
// with >= timestamp.
// FIXME(dlc) - inefficient.
func (ms *memStore) GetSeqFromTime(t time.Time) uint64 {
	ts := t.UnixNano()
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if len(ms.msgs) == 0 {
		return ms.state.LastSeq + 1
	}
	if ts <= ms.msgs[ms.state.FirstSeq].ts {
		return ms.state.FirstSeq
	}
	last := ms.msgs[ms.state.LastSeq].ts
	if ts == last {
		return ms.state.LastSeq
	}
	if ts > last {
		return ms.state.LastSeq + 1
	}
	index := sort.Search(len(ms.msgs), func(i int) bool {
		return ms.msgs[uint64(i)+ms.state.FirstSeq].ts >= ts
	})
	return uint64(index) + ms.state.FirstSeq
}

// Will check the msg limit and drop firstSeq msg if needed.
// Lock should be held.
func (ms *memStore) enforceMsgLimit() {
	if ms.cfg.MaxMsgs <= 0 || ms.state.Msgs <= uint64(ms.cfg.MaxMsgs) {
		return
	}
	for nmsgs := ms.state.Msgs; nmsgs > uint64(ms.cfg.MaxMsgs); nmsgs = ms.state.Msgs {
		ms.deleteFirstMsgOrPanic()
	}
}

// Will check the bytes limit and drop msgs if needed.
// Lock should be held.
func (ms *memStore) enforceBytesLimit() {
	if ms.cfg.MaxBytes <= 0 || ms.state.Bytes <= uint64(ms.cfg.MaxBytes) {
		return
	}
	for bs := ms.state.Bytes; bs > uint64(ms.cfg.MaxBytes); bs = ms.state.Bytes {
		ms.deleteFirstMsgOrPanic()
	}
}

// Will start the age check timer.
// Lock should be held.
func (ms *memStore) startAgeChk() {
	if ms.ageChk == nil && ms.cfg.MaxAge != 0 {
		ms.ageChk = time.AfterFunc(ms.cfg.MaxAge, ms.expireMsgs)
	}
}

// Will expire msgs that are too old.
func (ms *memStore) expireMsgs() {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	now := time.Now().UnixNano()
	minAge := now - int64(ms.cfg.MaxAge)
	for {
		if sm, ok := ms.msgs[ms.state.FirstSeq]; ok && sm.ts <= minAge {
			ms.deleteFirstMsgOrPanic()
		} else {
			if !ok {
				ms.ageChk.Stop()
				ms.ageChk = nil
			} else {
				fireIn := time.Duration(sm.ts-now) + ms.cfg.MaxAge
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
	bytes := int64(ms.state.Bytes)
	ms.state.FirstSeq = ms.state.LastSeq + 1
	ms.state.Bytes = 0
	ms.state.Msgs = 0
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
	return ms.removeMsg(ms.state.FirstSeq, false)
}

// LoadMsg will lookup the message by sequence number and return it if found.
func (ms *memStore) LoadMsg(seq uint64) (string, []byte, int64, error) {
	ms.mu.RLock()
	sm, ok := ms.msgs[seq]
	last := ms.state.LastSeq
	ms.mu.RUnlock()

	if !ok || sm == nil {
		var err = ErrStoreEOF
		if seq <= last {
			err = ErrStoreMsgNotFound
		}
		return "", nil, 0, err
	}
	return sm.subj, sm.msg, sm.ts, nil
}

// RemoveMsg will remove the message from this store.
// Will return the number of bytes removed.
func (ms *memStore) RemoveMsg(seq uint64) (bool, error) {
	ms.mu.Lock()
	removed := ms.removeMsg(seq, false)
	ms.mu.Unlock()
	return removed, nil
}

// EraseMsg will remove the message and rewrite its contents.
func (ms *memStore) EraseMsg(seq uint64) (bool, error) {
	ms.mu.Lock()
	removed := ms.removeMsg(seq, true)
	ms.mu.Unlock()
	return removed, nil
}

// Removes the message referenced by seq.
func (ms *memStore) removeMsg(seq uint64, secure bool) bool {
	var ss uint64
	sm, ok := ms.msgs[seq]
	if ok {
		delete(ms.msgs, seq)
		ms.state.Msgs--
		ss = memStoreMsgSize(sm.subj, sm.msg)
		ms.state.Bytes -= ss
		if seq == ms.state.FirstSeq {
			var nseq uint64
			for nseq = ms.state.FirstSeq + 1; nseq < ms.state.LastSeq; nseq++ {
				if _, ok := ms.msgs[nseq]; ok {
					break
				}
			}
			ms.state.FirstSeq = nseq
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

func (ms *memStore) State() StreamState {
	ms.mu.RLock()
	state := ms.state
	state.Consumers = ms.consumers
	ms.mu.RUnlock()
	return state
}

func memStoreMsgSize(subj string, msg []byte) uint64 {
	return uint64(len(subj) + len(msg) + 16) // 8*2 for seq + age
}

// Delete is same as Stop for memory store.
func (ms *memStore) Delete() error {
	ms.Purge()
	return ms.Stop()
}

func (ms *memStore) Stop() error {
	ms.mu.Lock()
	if ms.ageChk != nil {
		ms.ageChk.Stop()
		ms.ageChk = nil
	}
	ms.msgs = nil
	ms.mu.Unlock()
	return nil
}

func (ms *memStore) incConsumers() {
	ms.mu.Lock()
	ms.consumers++
	ms.mu.Unlock()
}

func (ms *memStore) decConsumers() {
	ms.mu.Lock()
	if ms.consumers > 0 {
		ms.consumers--
	}
	ms.mu.Unlock()
}

type consumerMemStore struct {
	ms *memStore
}

func (ms *memStore) ConsumerStore(_ string, _ *ConsumerConfig) (ConsumerStore, error) {
	ms.incConsumers()
	return &consumerMemStore{ms}, nil
}

func (ms *memStore) Snapshot() (io.ReadCloser, error) {
	return nil, fmt.Errorf("no impl")
}

// No-ops.
func (os *consumerMemStore) Update(_ *ConsumerState) error {
	return nil
}
func (os *consumerMemStore) Stop() error {
	os.ms.decConsumers()
	return nil
}

func (os *consumerMemStore) Delete() error {
	return os.Stop()
}

func (os *consumerMemStore) State() (*ConsumerState, error) { return nil, nil }

// Templates
type templateMemStore struct{}

func newTemplateMemStore() *templateMemStore {
	return &templateMemStore{}
}

// No-ops for memstore.
func (ts *templateMemStore) Store(t *StreamTemplate) error  { return nil }
func (ts *templateMemStore) Delete(t *StreamTemplate) error { return nil }
