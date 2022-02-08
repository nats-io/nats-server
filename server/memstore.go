// Copyright 2019-2021 The NATS Authors
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
	mu        sync.RWMutex
	cfg       StreamConfig
	state     StreamState
	msgs      map[uint64]*storedMsg
	fss       map[string]*SimpleState
	maxp      int64
	scb       StorageUpdateHandler
	ageChk    *time.Timer
	consumers int
}

type storedMsg struct {
	subj string
	hdr  []byte
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
	ms := &memStore{
		msgs: make(map[uint64]*storedMsg),
		fss:  make(map[string]*SimpleState),
		maxp: cfg.MaxMsgsPer,
		cfg:  *cfg,
	}

	return ms, nil
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

// Stores a raw message with expected sequence number and timestamp.
// Lock should be held.
func (ms *memStore) storeRawMsg(subj string, hdr, msg []byte, seq uint64, ts int64) error {
	if ms.msgs == nil {
		return ErrStoreClosed
	}

	// Tracking by subject.
	var ss *SimpleState
	var asl bool
	if len(subj) > 0 {
		if ss = ms.fss[subj]; ss != nil {
			asl = ms.maxp > 0 && ss.Msgs >= uint64(ms.maxp)
		}
	}

	// Check if we are discarding new messages when we reach the limit.
	if ms.cfg.Discard == DiscardNew {
		if ms.cfg.MaxMsgs > 0 && ms.state.Msgs >= uint64(ms.cfg.MaxMsgs) {
			// If we are tracking max messages per subject and are at the limit we will replace, so this is ok.
			if !asl {
				return ErrMaxMsgs
			}
		}
		if ms.cfg.MaxBytes > 0 && ms.state.Bytes+uint64(len(msg)+len(hdr)) >= uint64(ms.cfg.MaxBytes) {
			if !asl {
				return ErrMaxBytes
			}
			// If we are here we are at a subject maximum, need to determine if dropping last message gives us enough room.
			sm, ok := ms.msgs[ss.First]
			if !ok || memStoreMsgSize(sm.subj, sm.hdr, sm.msg) < uint64(len(msg)+len(hdr)) {
				return ErrMaxBytes
			}
		}
	}

	if seq != ms.state.LastSeq+1 {
		if seq > 0 {
			return ErrSequenceMismatch
		}
		seq = ms.state.LastSeq + 1
	}

	// Adjust first if needed.
	now := time.Unix(0, ts).UTC()
	if ms.state.Msgs == 0 {
		ms.state.FirstSeq = seq
		ms.state.FirstTime = now
	}

	// Make copies
	// TODO(dlc) - Maybe be smarter here.
	if len(msg) > 0 {
		msg = copyBytes(msg)
	}
	if len(hdr) > 0 {
		hdr = copyBytes(hdr)
	}

	ms.msgs[seq] = &storedMsg{subj, hdr, msg, seq, ts}
	ms.state.Msgs++
	ms.state.Bytes += memStoreMsgSize(subj, hdr, msg)
	ms.state.LastSeq = seq
	ms.state.LastTime = now

	// Track per subject.
	if len(subj) > 0 {
		if ss != nil {
			ss.Msgs++
			ss.Last = seq
			// Check per subject limits.
			if ms.maxp > 0 && ss.Msgs > uint64(ms.maxp) {
				ms.enforcePerSubjectLimit(ss)
			}
		} else {
			ms.fss[subj] = &SimpleState{Msgs: 1, First: seq, Last: seq}
		}
	}

	// Limits checks and enforcement.
	ms.enforceMsgLimit()
	ms.enforceBytesLimit()

	// Check if we have and need the age expiration timer running.
	if ms.ageChk == nil && ms.cfg.MaxAge != 0 {
		ms.startAgeChk()
	}

	return nil
}

// StoreRawMsg stores a raw message with expected sequence number and timestamp.
func (ms *memStore) StoreRawMsg(subj string, hdr, msg []byte, seq uint64, ts int64) error {
	ms.mu.Lock()
	err := ms.storeRawMsg(subj, hdr, msg, seq, ts)
	cb := ms.scb
	ms.mu.Unlock()

	if err == nil && cb != nil {
		cb(1, int64(memStoreMsgSize(subj, hdr, msg)), seq, subj)
	}

	return err
}

// Store stores a message.
func (ms *memStore) StoreMsg(subj string, hdr, msg []byte) (uint64, int64, error) {
	ms.mu.Lock()
	seq, ts := ms.state.LastSeq+1, time.Now().UnixNano()
	err := ms.storeRawMsg(subj, hdr, msg, seq, ts)
	cb := ms.scb
	ms.mu.Unlock()

	if err != nil {
		seq, ts = 0, 0
	} else if cb != nil {
		cb(1, int64(memStoreMsgSize(subj, hdr, msg)), seq, subj)
	}

	return seq, ts, err
}

// SkipMsg will use the next sequence number but not store anything.
func (ms *memStore) SkipMsg() uint64 {
	// Grab time.
	now := time.Now().UTC()

	ms.mu.Lock()
	seq := ms.state.LastSeq + 1
	ms.state.LastSeq = seq
	ms.state.LastTime = now
	if ms.state.Msgs == 0 {
		ms.state.FirstSeq = seq
		ms.state.FirstTime = now
	}
	ms.updateFirstSeq(seq)
	ms.mu.Unlock()
	return seq
}

// RegisterStorageUpdates registers a callback for updates to storage changes.
// It will present number of messages and bytes as a signed integer and an
// optional sequence number of the message if a single.
func (ms *memStore) RegisterStorageUpdates(cb StorageUpdateHandler) {
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

// FilteredState will return the SimpleState associated with the filtered subject and a proposed starting sequence.
func (ms *memStore) FilteredState(sseq uint64, subj string) SimpleState {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.filteredStateLocked(sseq, subj)
}

func (ms *memStore) filteredStateLocked(sseq uint64, subj string) SimpleState {
	var ss SimpleState

	if sseq < ms.state.FirstSeq {
		sseq = ms.state.FirstSeq
	}

	// If past the end no results.
	if sseq > ms.state.LastSeq {
		return ss
	}

	// Empty same as everything.
	if subj == _EMPTY_ {
		subj = fwcs
	}

	wc := subjectHasWildcard(subj)
	subs := []string{subj}
	if wc {
		subs = subs[:0]
		for fsubj := range ms.fss {
			if subjectIsSubsetMatch(fsubj, subj) {
				subs = append(subs, fsubj)
			}
		}
	}
	fseq, lseq := ms.state.LastSeq, uint64(0)
	for _, subj := range subs {
		ss := ms.fss[subj]
		if ss == nil {
			continue
		}
		if ss.First < fseq {
			fseq = ss.First
		}
		if ss.Last > lseq {
			lseq = ss.Last
		}
	}
	if fseq < sseq {
		fseq = sseq
	}

	// FIXME(dlc) - Optimize better like filestore.
	eq := compareFn(subj)
	for seq := fseq; seq <= lseq; seq++ {
		if sm, ok := ms.msgs[seq]; ok && eq(sm.subj, subj) {
			ss.Msgs++
			if ss.First == 0 {
				ss.First = seq
			}
			ss.Last = seq
		}
	}
	return ss
}

// SubjectsState returns a map of SimpleState for all matching subjects.
func (ms *memStore) SubjectsState(subject string) map[string]SimpleState {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.fss) == 0 {
		return nil
	}

	fss := make(map[string]SimpleState)
	for subj, ss := range ms.fss {
		if subject == _EMPTY_ || subject == fwcs || subjectIsSubsetMatch(subj, subject) {
			oss := fss[subj]
			if oss.First == 0 { // New
				fss[subj] = *ss
			} else {
				// Merge here.
				oss.Last, oss.Msgs = ss.Last, oss.Msgs+ss.Msgs
				fss[subj] = oss
			}
		}
	}
	return fss
}

// Will check the msg limit for this tracked subject.
// Lock should be held.
func (ms *memStore) enforcePerSubjectLimit(ss *SimpleState) {
	if ms.maxp <= 0 {
		return
	}
	for nmsgs := ss.Msgs; nmsgs > uint64(ms.maxp); nmsgs = ss.Msgs {
		if !ms.removeMsg(ss.First, false) {
			break
		}
	}
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
				if ms.ageChk != nil {
					ms.ageChk.Stop()
					ms.ageChk = nil
				}
			} else {
				fireIn := time.Duration(sm.ts-now) + ms.cfg.MaxAge
				if ms.ageChk != nil {
					ms.ageChk.Reset(fireIn)
				} else {
					ms.ageChk = time.AfterFunc(fireIn, ms.expireMsgs)
				}
			}
			return
		}
	}
}

// PurgeEx will remove messages based on subject filters, sequence and number of messages to keep.
// Will return the number of purged messages.
func (ms *memStore) PurgeEx(subject string, sequence, keep uint64) (purged uint64, err error) {
	if subject == _EMPTY_ || subject == fwcs {
		if keep == 0 && (sequence == 0 || sequence == 1) {
			return ms.Purge()
		}
		if sequence > 1 {
			return ms.Compact(sequence)
		} else if keep > 0 {
			ms.mu.RLock()
			msgs, lseq := ms.state.Msgs, ms.state.LastSeq
			ms.mu.RUnlock()
			if keep >= msgs {
				return 0, nil
			}
			return ms.Compact(lseq - keep + 1)
		}
		return 0, nil

	}
	eq := compareFn(subject)
	if ss := ms.FilteredState(1, subject); ss.Msgs > 0 {
		if keep > 0 {
			if keep >= ss.Msgs {
				return 0, nil
			}
			ss.Msgs -= keep
		}
		last := ss.Last
		if sequence > 0 {
			last = sequence - 1
		}
		ms.mu.Lock()
		for seq := ss.First; seq <= last; seq++ {
			if sm, ok := ms.msgs[seq]; ok && eq(sm.subj, subject) {
				if ok := ms.removeMsg(sm.seq, false); ok {
					purged++
					if purged >= ss.Msgs {
						break
					}
				}
			}
		}
		ms.mu.Unlock()
	}
	return purged, nil
}

// Purge will remove all messages from this store.
// Will return the number of purged messages.
func (ms *memStore) Purge() (uint64, error) {
	ms.mu.Lock()
	purged := uint64(len(ms.msgs))
	cb := ms.scb
	bytes := int64(ms.state.Bytes)
	ms.state.FirstSeq = ms.state.LastSeq + 1
	ms.state.FirstTime = time.Time{}
	ms.state.Bytes = 0
	ms.state.Msgs = 0
	ms.msgs = make(map[uint64]*storedMsg)
	ms.fss = make(map[string]*SimpleState)
	ms.mu.Unlock()

	if cb != nil {
		cb(-int64(purged), -bytes, 0, _EMPTY_)
	}

	return purged, nil
}

// Compact will remove all messages from this store up to
// but not including the seq parameter.
// Will return the number of purged messages.
func (ms *memStore) Compact(seq uint64) (uint64, error) {
	if seq == 0 {
		return ms.Purge()
	}

	var purged, bytes uint64

	ms.mu.Lock()
	cb := ms.scb
	if seq <= ms.state.LastSeq {
		sm, ok := ms.msgs[seq]
		if !ok {
			ms.mu.Unlock()
			return 0, ErrStoreMsgNotFound
		}
		ms.state.FirstSeq = seq
		ms.state.FirstTime = time.Unix(0, sm.ts).UTC()

		for seq := seq - 1; seq > 0; seq-- {
			if sm := ms.msgs[seq]; sm != nil {
				bytes += memStoreMsgSize(sm.subj, sm.hdr, sm.msg)
				purged++
				delete(ms.msgs, seq)
			}
		}
		ms.state.Msgs -= purged
		ms.state.Bytes -= bytes
	} else {
		// We are compacting past the end of our range. Do purge and set sequences correctly
		// such that the next message placed will have seq.
		purged = uint64(len(ms.msgs))
		bytes = ms.state.Bytes
		ms.state.Bytes = 0
		ms.state.Msgs = 0
		ms.state.FirstSeq = seq
		ms.state.FirstTime = time.Time{}
		ms.state.LastSeq = seq - 1
		ms.msgs = make(map[uint64]*storedMsg)
	}
	ms.mu.Unlock()

	if cb != nil {
		cb(-int64(purged), -int64(bytes), 0, _EMPTY_)
	}

	return purged, nil
}

// Truncate will truncate a stream store up to and including seq. Sequence needs to be valid.
func (ms *memStore) Truncate(seq uint64) error {
	var purged, bytes uint64

	ms.mu.Lock()
	lsm, ok := ms.msgs[seq]
	if !ok {
		ms.mu.Unlock()
		return ErrInvalidSequence
	}

	for i := ms.state.LastSeq; i > seq; i-- {
		if sm := ms.msgs[i]; sm != nil {
			purged++
			bytes += memStoreMsgSize(sm.subj, sm.hdr, sm.msg)
			delete(ms.msgs, seq)
		}
	}
	// Reset last.
	ms.state.LastSeq = lsm.seq
	ms.state.LastTime = time.Unix(0, lsm.ts).UTC()
	// Update msgs and bytes.
	ms.state.Msgs -= purged
	ms.state.Bytes -= bytes
	cb := ms.scb
	ms.mu.Unlock()

	if cb != nil {
		cb(-int64(purged), -int64(bytes), 0, _EMPTY_)
	}

	return nil
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
func (ms *memStore) LoadMsg(seq uint64) (string, []byte, []byte, int64, error) {
	ms.mu.RLock()
	sm, ok := ms.msgs[seq]
	last := ms.state.LastSeq
	ms.mu.RUnlock()

	if !ok || sm == nil {
		var err = ErrStoreEOF
		if seq <= last {
			err = ErrStoreMsgNotFound
		}
		return _EMPTY_, nil, nil, 0, err
	}
	return sm.subj, sm.hdr, sm.msg, sm.ts, nil
}

// LoadLastMsg will return the last message we have that matches a given subject.
// The subject can be a wildcard.
func (ms *memStore) LoadLastMsg(subject string) (subj string, seq uint64, hdr, msg []byte, ts int64, err error) {
	var sm *storedMsg
	var ok bool

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if subject == _EMPTY_ || subject == fwcs {
		sm, ok = ms.msgs[ms.state.LastSeq]
	} else if ss := ms.filteredStateLocked(1, subject); ss.Msgs > 0 {
		sm, ok = ms.msgs[ss.Last]
	}
	if !ok || sm == nil {
		return _EMPTY_, 0, nil, nil, 0, ErrStoreMsgNotFound
	}
	return sm.subj, sm.seq, sm.hdr, sm.msg, sm.ts, nil
}

// LoadNextMsg will find the next message matching the filter subject starting at the start sequence.
// The filter subject can be a wildcard.
func (ms *memStore) LoadNextMsg(filter string, wc bool, start uint64) (subj string, seq uint64, hdr, msg []byte, ts int64, err error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if start < ms.state.FirstSeq {
		start = ms.state.FirstSeq
	}

	// If past the end no results.
	if start > ms.state.LastSeq {
		return _EMPTY_, ms.state.LastSeq, nil, nil, 0, ErrStoreEOF
	}

	isAll := filter == _EMPTY_ || filter == fwcs
	subs := []string{filter}
	if wc || isAll {
		subs = subs[:0]
		for fsubj := range ms.fss {
			if isAll || subjectIsSubsetMatch(fsubj, filter) {
				subs = append(subs, fsubj)
			}
		}
	}
	fseq, lseq := ms.state.LastSeq, uint64(0)
	for _, subj := range subs {
		ss := ms.fss[subj]
		if ss == nil {
			continue
		}
		if ss.First < fseq {
			fseq = ss.First
		}
		if ss.Last > lseq {
			lseq = ss.Last
		}
	}
	if fseq < start {
		fseq = start
	}

	eq := subjectsEqual
	if wc {
		eq = subjectIsSubsetMatch
	}

	for nseq := fseq; nseq <= lseq; nseq++ {
		if sm, ok := ms.msgs[nseq]; ok && (isAll || eq(sm.subj, filter)) {
			return sm.subj, nseq, sm.hdr, sm.msg, sm.ts, nil
		}
	}
	return _EMPTY_, ms.state.LastSeq, nil, nil, 0, ErrStoreEOF
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

// Performs logic to update first sequence number.
// Lock should be held.
func (ms *memStore) updateFirstSeq(seq uint64) {
	if seq != ms.state.FirstSeq {
		// Interior delete.
		return
	}
	var nsm *storedMsg
	var ok bool
	for nseq := ms.state.FirstSeq + 1; nseq <= ms.state.LastSeq; nseq++ {
		if nsm, ok = ms.msgs[nseq]; ok {
			break
		}
	}
	if nsm != nil {
		ms.state.FirstSeq = nsm.seq
		ms.state.FirstTime = time.Unix(0, nsm.ts).UTC()
	} else {
		// Like purge.
		ms.state.FirstSeq = ms.state.LastSeq + 1
		ms.state.FirstTime = time.Time{}
	}
}

// Remove a seq from the fss and select new first.
// Lock should be held.
func (ms *memStore) removeSeqPerSubject(subj string, seq uint64) {
	ss := ms.fss[subj]
	if ss == nil {
		return
	}
	if ss.Msgs == 1 {
		delete(ms.fss, subj)
		return
	}
	ss.Msgs--
	if seq != ss.First {
		return
	}
	// TODO(dlc) - Might want to optimize this.
	for tseq := seq + 1; tseq <= ss.Last; tseq++ {
		if sm := ms.msgs[tseq]; sm != nil && sm.subj == subj {
			ss.First = tseq
			break
		}
	}
}

// Removes the message referenced by seq.
// Lock should he held.
func (ms *memStore) removeMsg(seq uint64, secure bool) bool {
	var ss uint64
	sm, ok := ms.msgs[seq]
	if !ok {
		return false
	}

	ss = memStoreMsgSize(sm.subj, sm.hdr, sm.msg)

	delete(ms.msgs, seq)
	ms.state.Msgs--
	ms.state.Bytes -= ss
	ms.updateFirstSeq(seq)

	if secure {
		if len(sm.hdr) > 0 {
			sm.hdr = make([]byte, len(sm.hdr))
			rand.Read(sm.hdr)
		}
		if len(sm.msg) > 0 {
			sm.msg = make([]byte, len(sm.msg))
			rand.Read(sm.msg)
		}
		sm.seq, sm.ts = 0, 0
	}

	// Remove any per subject tracking.
	ms.removeSeqPerSubject(sm.subj, seq)

	if ms.scb != nil {
		// We do not want to hold any locks here.
		ms.mu.Unlock()
		delta := int64(ss)
		ms.scb(-1, -delta, seq, sm.subj)
		ms.mu.Lock()
	}

	return ok
}

// Type returns the type of the underlying store.
func (ms *memStore) Type() StorageType {
	return MemoryStorage
}

// FastState will fill in state with only the following.
// Msgs, Bytes, First and Last Sequence and Time and NumDeleted.
func (ms *memStore) FastState(state *StreamState) {
	ms.mu.RLock()
	state.Msgs = ms.state.Msgs
	state.Bytes = ms.state.Bytes
	state.FirstSeq = ms.state.FirstSeq
	state.FirstTime = ms.state.FirstTime
	state.LastSeq = ms.state.LastSeq
	state.LastTime = ms.state.LastTime
	if state.LastSeq > state.FirstSeq {
		state.NumDeleted = int((state.LastSeq - state.FirstSeq) - state.Msgs + 1)
	}
	state.Consumers = ms.consumers
	state.NumSubjects = len(ms.fss)
	ms.mu.RUnlock()
}

func (ms *memStore) State() StreamState {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	state := ms.state
	state.Consumers = ms.consumers
	state.NumSubjects = len(ms.fss)
	state.Deleted = nil

	// Calculate interior delete details.
	if state.LastSeq > state.FirstSeq {
		state.NumDeleted = int((state.LastSeq - state.FirstSeq) - state.Msgs + 1)
		if state.NumDeleted > 0 {
			state.Deleted = make([]uint64, 0, state.NumDeleted)
			// TODO(dlc) - Too Simplistic, once state is updated to allow runs etc redo.
			for seq := state.FirstSeq + 1; seq < ms.state.LastSeq; seq++ {
				if _, ok := ms.msgs[seq]; !ok {
					state.Deleted = append(state.Deleted, seq)
				}
			}
		}
	}

	return state
}

func (ms *memStore) Utilization() (total, reported uint64, err error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.state.Bytes, ms.state.Bytes, nil
}

func memStoreMsgSize(subj string, hdr, msg []byte) uint64 {
	return uint64(len(subj) + len(hdr) + len(msg) + 16) // 8*2 for seq + age
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

func (ms *memStore) Snapshot(_ time.Duration, _, _ bool) (*SnapshotResult, error) {
	return nil, fmt.Errorf("no impl")
}

// No-ops.
func (os *consumerMemStore) Update(_ *ConsumerState) error                 { return nil }
func (os *consumerMemStore) UpdateDelivered(_, _, _ uint64, _ int64) error { return nil }
func (os *consumerMemStore) UpdateAcks(_, _ uint64) error                  { return nil }
func (os *consumerMemStore) UpdateConfig(_ *ConsumerConfig) error          { return nil }

func (os *consumerMemStore) Stop() error {
	os.ms.decConsumers()
	return nil
}

func (os *consumerMemStore) Delete() error {
	return os.Stop()
}
func (os *consumerMemStore) StreamDelete() error {
	return os.Stop()
}

func (os *consumerMemStore) State() (*ConsumerState, error) { return nil, nil }

// Type returns the type of the underlying store.
func (os *consumerMemStore) Type() StorageType { return MemoryStorage }

// Templates
type templateMemStore struct{}

func newTemplateMemStore() *templateMemStore {
	return &templateMemStore{}
}

// No-ops for memstore.
func (ts *templateMemStore) Store(t *streamTemplate) error  { return nil }
func (ts *templateMemStore) Delete(t *streamTemplate) error { return nil }
