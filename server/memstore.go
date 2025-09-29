// Copyright 2019-2025 The NATS Authors
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
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server/ats"
	"github.com/nats-io/nats-server/v2/server/avl"
	"github.com/nats-io/nats-server/v2/server/gsl"
	"github.com/nats-io/nats-server/v2/server/stree"
	"github.com/nats-io/nats-server/v2/server/thw"
)

// TODO(dlc) - This is a fairly simplistic approach but should do for now.
type memStore struct {
	mu          sync.RWMutex
	cfg         StreamConfig
	state       StreamState
	msgs        map[uint64]*StoreMsg
	fss         *stree.SubjectTree[SimpleState]
	dmap        avl.SequenceSet
	maxp        int64
	scb         StorageUpdateHandler
	rmcb        StorageRemoveMsgHandler
	pmsgcb      ProcessJetStreamMsgHandler
	ageChk      *time.Timer // Timer to expire messages.
	ageChkRun   bool        // Whether message expiration is currently running.
	ageChkTime  int64       // When the message expiration is scheduled to run.
	consumers   int
	receivedAny bool
	ttls        *thw.HashWheel
	scheduling  *MsgScheduling
	sdm         *SDMMeta
}

func newMemStore(cfg *StreamConfig) (*memStore, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config required")
	}
	if cfg.Storage != MemoryStorage {
		return nil, fmt.Errorf("memStore requires memory storage type in config")
	}
	ms := &memStore{
		msgs: make(map[uint64]*StoreMsg),
		fss:  stree.NewSubjectTree[SimpleState](),
		maxp: cfg.MaxMsgsPer,
		cfg:  *cfg,
	}
	// Only create a THW if we're going to allow TTLs.
	if cfg.AllowMsgTTL {
		ms.ttls = thw.NewHashWheel()
	}
	if cfg.AllowMsgSchedules {
		ms.scheduling = newMsgScheduling(ms.runMsgScheduling)
	}
	if cfg.FirstSeq > 0 {
		if _, err := ms.purge(cfg.FirstSeq); err != nil {
			return nil, err
		}
	}

	// Register with access time service.
	ats.Register()

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
	// Create or delete the THW if needed.
	if cfg.AllowMsgTTL && ms.ttls == nil {
		ms.recoverTTLState()
	} else if !cfg.AllowMsgTTL && ms.ttls != nil {
		ms.ttls = nil
	}
	if cfg.AllowMsgSchedules && ms.scheduling == nil {
		ms.recoverMsgSchedulingState()
	} else if !cfg.AllowMsgSchedules && ms.scheduling != nil {
		ms.scheduling = nil
	}
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
		ms.ageChkTime = 0
	}
	// Make sure to update MaxMsgsPer
	if cfg.MaxMsgsPer < -1 {
		cfg.MaxMsgsPer = -1
	}
	maxp := ms.maxp
	ms.maxp = cfg.MaxMsgsPer
	// If the value is smaller, or was unset before, we need to enforce that.
	if ms.maxp > 0 && (maxp == 0 || ms.maxp < maxp) {
		lm := uint64(ms.maxp)
		ms.fss.IterFast(func(subj []byte, ss *SimpleState) bool {
			if ss.Msgs > lm {
				ms.enforcePerSubjectLimit(bytesToString(subj), ss)
			}
			return true
		})
	}
	ms.mu.Unlock()

	if cfg.MaxAge != 0 || cfg.AllowMsgTTL {
		ms.expireMsgs()
	}
	if cfg.AllowMsgSchedules {
		ms.runMsgScheduling()
	}
	return nil
}

// Lock should be held.
func (ms *memStore) recoverTTLState() {
	ms.ttls = thw.NewHashWheel()
	if ms.state.Msgs == 0 {
		return
	}

	var (
		seq uint64
		smv StoreMsg
		sm  *StoreMsg
	)
	defer ms.resetAgeChk(0)
	for sm, seq, _ = ms.loadNextMsgLocked(fwcs, true, 0, &smv); sm != nil; sm, seq, _ = ms.loadNextMsgLocked(fwcs, true, seq+1, &smv) {
		if len(sm.hdr) == 0 {
			continue
		}
		if ttl, _ := getMessageTTL(sm.hdr); ttl > 0 {
			expires := time.Duration(sm.ts) + (time.Second * time.Duration(ttl))
			ms.ttls.Add(seq, int64(expires))
		}
	}
}

// Lock should be held.
func (ms *memStore) recoverMsgSchedulingState() {
	ms.scheduling = newMsgScheduling(ms.runMsgScheduling)
	if ms.state.Msgs == 0 {
		return
	}

	var (
		seq uint64
		smv StoreMsg
		sm  *StoreMsg
	)
	defer ms.scheduling.resetTimer()
	for sm, seq, _ = ms.loadNextMsgLocked(fwcs, true, 0, &smv); sm != nil; sm, seq, _ = ms.loadNextMsgLocked(fwcs, true, seq+1, &smv) {
		if len(sm.hdr) == 0 {
			continue
		}
		if schedule, ok := getMessageSchedule(sm.hdr); ok && !schedule.IsZero() {
			ms.scheduling.init(seq, sm.subj, schedule.UnixNano())
		}
	}
}

// Stores a raw message with expected sequence number and timestamp.
// Lock should be held.
func (ms *memStore) storeRawMsg(subj string, hdr, msg []byte, seq uint64, ts, ttl int64) error {
	if ms.msgs == nil {
		return ErrStoreClosed
	}

	// Tracking by subject.
	var ss *SimpleState
	var asl bool
	if len(subj) > 0 {
		var ok bool
		if ss, ok = ms.fss.Find(stringToBytes(subj)); ok {
			asl = ms.maxp > 0 && ss.Msgs >= uint64(ms.maxp)
		}
	}

	// Check if we are discarding new messages when we reach the limit.
	if ms.cfg.Discard == DiscardNew {
		if asl && ms.cfg.DiscardNewPer {
			return ErrMaxMsgsPerSubject
		}
		// If we are discard new and limits policy and clustered, we do the enforcement
		// above and should not disqualify the message here since it could cause replicas to drift.
		if ms.cfg.Retention == LimitsPolicy || ms.cfg.Replicas == 1 {
			if ms.cfg.MaxMsgs > 0 && ms.state.Msgs >= uint64(ms.cfg.MaxMsgs) {
				// If we are tracking max messages per subject and are at the limit we will replace, so this is ok.
				if !asl {
					return ErrMaxMsgs
				}
			}
			if ms.cfg.MaxBytes > 0 && ms.state.Bytes+memStoreMsgSize(subj, hdr, msg) >= uint64(ms.cfg.MaxBytes) {
				if !asl {
					return ErrMaxBytes
				}
				// If we are here we are at a subject maximum, need to determine if dropping last message gives us enough room.
				if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
					ms.recalculateForSubj(subj, ss)
				}
				sm, ok := ms.msgs[ss.First]
				if !ok || memStoreMsgSize(sm.subj, sm.hdr, sm.msg) < memStoreMsgSize(subj, hdr, msg) {
					return ErrMaxBytes
				}
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

	// FIXME(dlc) - Could pool at this level?
	sm := &StoreMsg{subj, nil, nil, make([]byte, 0, len(hdr)+len(msg)), seq, ts}
	sm.buf = append(sm.buf, hdr...)
	sm.buf = append(sm.buf, msg...)
	if len(hdr) > 0 {
		sm.hdr = sm.buf[:len(hdr)]
	}
	sm.msg = sm.buf[len(hdr):]
	ms.msgs[seq] = sm
	ms.state.Msgs++
	ms.state.Bytes += memStoreMsgSize(subj, hdr, msg)
	ms.state.LastSeq = seq
	ms.state.LastTime = now

	// Track per subject.
	if len(subj) > 0 {
		if ss != nil {
			ss.Msgs++
			ss.Last = seq
			ss.lastNeedsUpdate = false
			// Check per subject limits.
			if ms.maxp > 0 && ss.Msgs > uint64(ms.maxp) {
				ms.enforcePerSubjectLimit(subj, ss)
			}
		} else {
			ms.fss.Insert([]byte(subj), SimpleState{Msgs: 1, First: seq, Last: seq})
		}
	}

	// Limits checks and enforcement.
	ms.enforceMsgLimit()
	ms.enforceBytesLimit()

	// Per-message TTL.
	if ms.ttls != nil && ttl > 0 {
		expires := time.Duration(ts) + (time.Second * time.Duration(ttl))
		ms.ttls.Add(seq, int64(expires))
	}

	// Check if we have and need the age expiration timer running.
	switch {
	case ms.ttls != nil && ttl > 0:
		ms.resetAgeChk(0)
	case ms.ageChk == nil && (ms.cfg.MaxAge > 0 || ms.ttls != nil):
		ms.startAgeChk()
	}

	// Message scheduling.
	if ms.scheduling != nil {
		if schedule, ok := getMessageSchedule(hdr); ok && !schedule.IsZero() {
			ms.scheduling.add(seq, subj, schedule.UnixNano())
		}
	}

	return nil
}

// StoreRawMsg stores a raw message with expected sequence number and timestamp.
func (ms *memStore) StoreRawMsg(subj string, hdr, msg []byte, seq uint64, ts, ttl int64) error {
	ms.mu.Lock()
	err := ms.storeRawMsg(subj, hdr, msg, seq, ts, ttl)
	cb := ms.scb
	// Check if first message timestamp requires expiry
	// sooner than initial replica expiry timer set to MaxAge when initializing.
	if !ms.receivedAny && ms.cfg.MaxAge != 0 && ts > 0 {
		ms.receivedAny = true
		// Calculate duration when the next expireMsgs should be called.
		ms.resetAgeChk(int64(time.Millisecond) * 50)
	}
	ms.mu.Unlock()

	if err == nil && cb != nil {
		cb(1, int64(memStoreMsgSize(subj, hdr, msg)), seq, subj)
	}

	return err
}

// Store stores a message.
func (ms *memStore) StoreMsg(subj string, hdr, msg []byte, ttl int64) (uint64, int64, error) {
	ms.mu.Lock()
	seq, ts := ms.state.LastSeq+1, time.Now().UnixNano()
	err := ms.storeRawMsg(subj, hdr, msg, seq, ts, ttl)
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
	now := time.Unix(0, ats.AccessTime()).UTC()

	ms.mu.Lock()
	seq := ms.state.LastSeq + 1
	ms.state.LastSeq = seq
	ms.state.LastTime = now
	if ms.state.Msgs == 0 {
		ms.state.FirstSeq = seq + 1
		ms.state.FirstTime = time.Time{}
	} else {
		ms.dmap.Insert(seq)
	}
	ms.mu.Unlock()
	return seq
}

// Skip multiple msgs.
func (ms *memStore) SkipMsgs(seq uint64, num uint64) error {
	// Grab time.
	now := time.Unix(0, ats.AccessTime()).UTC()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Check sequence matches our last sequence.
	if seq != ms.state.LastSeq+1 {
		if seq > 0 {
			return ErrSequenceMismatch
		}
		seq = ms.state.LastSeq + 1
	}
	lseq := seq + num - 1

	ms.state.LastSeq = lseq
	ms.state.LastTime = now
	if ms.state.Msgs == 0 {
		ms.state.FirstSeq, ms.state.FirstTime = lseq+1, time.Time{}
	} else {
		for ; seq <= lseq; seq++ {
			ms.dmap.Insert(seq)
		}
	}
	return nil
}

// FlushAllPending flushes all data that was still pending to be written.
func (ms *memStore) FlushAllPending() {
	// Noop, in-memory store doesn't use async applying.
}

// RegisterStorageUpdates registers a callback for updates to storage changes.
// It will present number of messages and bytes as a signed integer and an
// optional sequence number of the message if a single.
func (ms *memStore) RegisterStorageUpdates(cb StorageUpdateHandler) {
	ms.mu.Lock()
	ms.scb = cb
	ms.mu.Unlock()
}

// RegisterStorageRemoveMsg registers a callback to remove messages.
// Replicated streams should propose removals, R1 can remove inline.
func (ms *memStore) RegisterStorageRemoveMsg(cb StorageRemoveMsgHandler) {
	ms.mu.Lock()
	ms.rmcb = cb
	ms.mu.Unlock()
}

// RegisterProcessJetStreamMsg registers a callback to process new JetStream messages.
func (ms *memStore) RegisterProcessJetStreamMsg(cb ProcessJetStreamMsgHandler) {
	ms.mu.Lock()
	ms.pmsgcb = cb
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
	// LastSeq is not guaranteed to be present since last does not go backwards.
	var lmsg *StoreMsg
	for lseq := ms.state.LastSeq; lseq > ms.state.FirstSeq; lseq-- {
		if lmsg = ms.msgs[lseq]; lmsg != nil {
			break
		}
	}
	if lmsg == nil {
		return ms.state.LastSeq + 1
	}

	last := lmsg.ts
	if ts == last {
		return ms.state.LastSeq
	}
	if ts > last {
		return ms.state.LastSeq + 1
	}
	index := sort.Search(len(ms.msgs), func(i int) bool {
		if msg := ms.msgs[ms.state.FirstSeq+uint64(i)]; msg != nil {
			return msg.ts >= ts
		}
		return false
	})
	return uint64(index) + ms.state.FirstSeq
}

// FilteredState will return the SimpleState associated with the filtered subject and a proposed starting sequence.
func (ms *memStore) FilteredState(sseq uint64, subj string) SimpleState {
	// This needs to be a write lock, as filteredStateLocked can
	// mutate the per-subject state.
	ms.mu.Lock()
	defer ms.mu.Unlock()

	return ms.filteredStateLocked(sseq, subj, false)
}

func (ms *memStore) filteredStateLocked(sseq uint64, filter string, lastPerSubject bool) SimpleState {
	if sseq < ms.state.FirstSeq {
		sseq = ms.state.FirstSeq
	}

	// If past the end no results.
	if sseq > ms.state.LastSeq {
		return SimpleState{}
	}

	if filter == _EMPTY_ {
		filter = fwcs
	}
	isAll := filter == fwcs

	// First check if we can optimize this part.
	// This means we want all and the starting sequence was before this block.
	if isAll && sseq <= ms.state.FirstSeq {
		total := ms.state.Msgs
		if lastPerSubject {
			total = uint64(ms.fss.Size())
		}
		return SimpleState{
			Msgs:  total,
			First: ms.state.FirstSeq,
			Last:  ms.state.LastSeq,
		}
	}

	_tsa, _fsa := [32]string{}, [32]string{}
	tsa, fsa := _tsa[:0], _fsa[:0]
	wc := subjectHasWildcard(filter)
	if wc {
		fsa = tokenizeSubjectIntoSlice(fsa[:0], filter)
	}
	// 1. See if we match any subs from fss.
	// 2. If we match and the sseq is past ss.Last then we can use meta only.
	// 3. If we match we need to do a partial, break and clear any totals and do a full scan like num pending.

	isMatch := func(subj string) bool {
		if isAll {
			return true
		}
		if !wc {
			return subj == filter
		}
		tsa = tokenizeSubjectIntoSlice(tsa[:0], subj)
		return isSubsetMatchTokenized(tsa, fsa)
	}

	var ss SimpleState
	update := func(fss *SimpleState) {
		msgs, first, last := fss.Msgs, fss.First, fss.Last
		if lastPerSubject {
			msgs, first = 1, last
		}
		ss.Msgs += msgs
		if ss.First == 0 || first < ss.First {
			ss.First = first
		}
		if last > ss.Last {
			ss.Last = last
		}
	}

	var havePartial bool
	var totalSkipped uint64
	// We will track start and end sequences as we go.
	ms.fss.Match(stringToBytes(filter), func(subj []byte, fss *SimpleState) {
		if fss.firstNeedsUpdate || fss.lastNeedsUpdate {
			ms.recalculateForSubj(bytesToString(subj), fss)
		}
		if sseq <= fss.First {
			update(fss)
		} else if sseq <= fss.Last {
			// We matched but it is a partial.
			havePartial = true
			// Don't break here, we will update to keep tracking last.
			update(fss)
		} else {
			totalSkipped += fss.Msgs
		}
	})

	// If we did not encounter any partials we can return here.
	if !havePartial {
		return ss
	}

	// If we are here we need to scan the msgs.
	// Capture first and last sequences for scan and then clear what we had.
	first, last := ss.First, ss.Last
	// To track if we decide to exclude we need to calculate first.
	var needScanFirst bool
	if first < sseq {
		first = sseq
		needScanFirst = true
	}

	// Now we want to check if it is better to scan inclusive and recalculate that way
	// or leave and scan exclusive and adjust our totals.
	// ss.Last is always correct here.
	toScan, toExclude := last-first, first-ms.state.FirstSeq+ms.state.LastSeq-ss.Last
	var seen map[string]bool
	if lastPerSubject {
		seen = make(map[string]bool)
	}
	if toScan < toExclude {
		ss.Msgs, ss.First = 0, 0

		update := func(sm *StoreMsg) {
			ss.Msgs++
			if ss.First == 0 {
				ss.First = sm.seq
			}
			if seen != nil {
				seen[sm.subj] = true
			}
		}
		// Check if easier to just scan msgs vs the sequence range.
		// This can happen with lots of interior deletes.
		if last-first > uint64(len(ms.msgs)) {
			for _, sm := range ms.msgs {
				if sm.seq >= first && sm.seq <= last && !seen[sm.subj] && isMatch(sm.subj) {
					update(sm)
				}
			}
		} else {
			for seq := first; seq <= last; seq++ {
				if sm, ok := ms.msgs[seq]; ok && !seen[sm.subj] && isMatch(sm.subj) {
					update(sm)
				}
			}
		}
	} else {
		// We will adjust from the totals above by scanning what we need to exclude.
		ss.First = first
		ss.Msgs += totalSkipped
		var adjust uint64
		var tss *SimpleState

		update := func(sm *StoreMsg) {
			if lastPerSubject {
				tss, _ = ms.fss.Find(stringToBytes(sm.subj))
			}
			// If we are last per subject, make sure to only adjust if all messages are before our first.
			if tss == nil || tss.Last < first {
				adjust++
			}
			if seen != nil {
				seen[sm.subj] = true
			}
		}
		// Check if easier to just scan msgs vs the sequence range.
		if first-ms.state.FirstSeq > uint64(len(ms.msgs)) {
			for _, sm := range ms.msgs {
				if sm.seq < first && !seen[sm.subj] && isMatch(sm.subj) {
					update(sm)
				}
			}
		} else {
			for seq := ms.state.FirstSeq; seq < first; seq++ {
				if sm, ok := ms.msgs[seq]; ok && !seen[sm.subj] && isMatch(sm.subj) {
					update(sm)
				}
			}
		}
		// Now do range at end.
		for seq := last + 1; seq < ms.state.LastSeq; seq++ {
			if sm, ok := ms.msgs[seq]; ok && !seen[sm.subj] && isMatch(sm.subj) {
				adjust++
				if seen != nil {
					seen[sm.subj] = true
				}
			}
		}
		ss.Msgs -= adjust
		if needScanFirst {
			// Check if easier to just scan msgs vs the sequence range.
			// Since we will need to scan all of the msgs vs below where we break on the first match,
			// we will only do so if a few orders of magnitude lower.
			if last-first > 100*uint64(len(ms.msgs)) {
				low := ms.state.LastSeq
				for _, sm := range ms.msgs {
					if sm.seq >= first && sm.seq < last && isMatch(sm.subj) {
						if sm.seq < low {
							low = sm.seq
						}
					}
				}
				if low < ms.state.LastSeq {
					ss.First = low
				}
			} else {
				for seq := first; seq < last; seq++ {
					if sm, ok := ms.msgs[seq]; ok && isMatch(sm.subj) {
						ss.First = seq
						break
					}
				}
			}
		}
	}

	return ss
}

// SubjectsState returns a map of SimpleState for all matching subjects.
func (ms *memStore) SubjectsState(subject string) map[string]SimpleState {
	// This needs to be a write lock, as we can mutate the per-subject state.
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.fss.Size() == 0 {
		return nil
	}

	if subject == _EMPTY_ {
		subject = fwcs
	}

	fss := make(map[string]SimpleState)
	ms.fss.Match(stringToBytes(subject), func(subj []byte, ss *SimpleState) {
		subjs := string(subj)
		if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
			ms.recalculateForSubj(subjs, ss)
		}
		oss := fss[subjs]
		if oss.First == 0 { // New
			fss[subjs] = *ss
		} else {
			// Merge here.
			oss.Last, oss.Msgs = ss.Last, oss.Msgs+ss.Msgs
			fss[subjs] = oss
		}
	})
	return fss
}

// AllLastSeqs will return a sorted list of last sequences for all subjects.
func (ms *memStore) AllLastSeqs() ([]uint64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.allLastSeqsLocked()
}

// allLastSeqsLocked will return a sorted list of last sequences for all
// subjects, but won't take the lock to do it, to avoid the issue of compounding
// read locks causing a deadlock with a write lock.
func (ms *memStore) allLastSeqsLocked() ([]uint64, error) {
	if len(ms.msgs) == 0 {
		return nil, nil
	}

	seqs := make([]uint64, 0, ms.fss.Size())
	ms.fss.IterFast(func(subj []byte, ss *SimpleState) bool {
		seqs = append(seqs, ss.Last)
		return true
	})

	slices.Sort(seqs)
	return seqs, nil
}

// Helper to determine if the filter(s) represent all the subjects.
// Most clients send in subjects even if they match the stream's ingest subjects.
// Lock should be held.
func (ms *memStore) filterIsAll(filters []string) bool {
	if len(filters) != len(ms.cfg.Subjects) {
		return false
	}
	// Sort so we can compare.
	slices.Sort(filters)
	for i, subj := range filters {
		if !subjectIsSubsetMatch(ms.cfg.Subjects[i], subj) {
			return false
		}
	}
	return true
}

// MultiLastSeqs will return a sorted list of sequences that match all subjects presented in filters.
// We will not exceed the maxSeq, which if 0 becomes the store's last sequence.
func (ms *memStore) MultiLastSeqs(filters []string, maxSeq uint64, maxAllowed int) ([]uint64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.msgs) == 0 {
		return nil, nil
	}

	// See if we can short circuit if we think they are asking for all last sequences and have no maxSeq or maxAllowed set.
	if maxSeq == 0 && maxAllowed <= 0 && ms.filterIsAll(filters) {
		return ms.allLastSeqsLocked()
	}

	// Implied last sequence.
	if maxSeq == 0 {
		maxSeq = ms.state.LastSeq
	}

	seqs := make([]uint64, 0, 64)
	seen := make(map[uint64]struct{})

	addIfNotDupe := func(seq uint64) {
		if _, ok := seen[seq]; !ok {
			seqs = append(seqs, seq)
			seen[seq] = struct{}{}
		}
	}

	for _, filter := range filters {
		ms.fss.Match(stringToBytes(filter), func(subj []byte, ss *SimpleState) {
			if ss.Last <= maxSeq {
				addIfNotDupe(ss.Last)
			} else if ss.Msgs > 1 {
				// The last is greater than maxSeq.
				s := bytesToString(subj)
				for seq := maxSeq; seq > 0; seq-- {
					if sm, ok := ms.msgs[seq]; ok && sm.subj == s {
						addIfNotDupe(seq)
						break
					}
				}
			}
		})
		// If maxAllowed was sepcified check that we will not exceed that.
		if maxAllowed > 0 && len(seqs) > maxAllowed {
			return nil, ErrTooManyResults
		}
	}
	slices.Sort(seqs)
	return seqs, nil
}

// SubjectsTotals return message totals per subject.
func (ms *memStore) SubjectsTotals(filterSubject string) map[string]uint64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.subjectsTotalsLocked(filterSubject)
}

// Lock should be held.
func (ms *memStore) subjectsTotalsLocked(filterSubject string) map[string]uint64 {
	if ms.fss.Size() == 0 {
		return nil
	}

	_tsa, _fsa := [32]string{}, [32]string{}
	tsa, fsa := _tsa[:0], _fsa[:0]
	fsa = tokenizeSubjectIntoSlice(fsa[:0], filterSubject)
	isAll := filterSubject == _EMPTY_ || filterSubject == fwcs

	fst := make(map[string]uint64)
	ms.fss.Match(stringToBytes(filterSubject), func(subj []byte, ss *SimpleState) {
		subjs := string(subj)
		if isAll {
			fst[subjs] = ss.Msgs
		} else {
			if tsa = tokenizeSubjectIntoSlice(tsa[:0], subjs); isSubsetMatchTokenized(tsa, fsa) {
				fst[subjs] = ss.Msgs
			}
		}
	})
	return fst
}

// NumPending will return the number of pending messages matching the filter subject starting at sequence.
func (ms *memStore) NumPending(sseq uint64, filter string, lastPerSubject bool) (total, validThrough uint64) {
	// This needs to be a write lock, as filteredStateLocked can mutate the per-subject state.
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ss := ms.filteredStateLocked(sseq, filter, lastPerSubject)
	return ss.Msgs, ms.state.LastSeq
}

// NumPending will return the number of pending messages matching any subject in the sublist starting at sequence.
func (ms *memStore) NumPendingMulti(sseq uint64, sl *gsl.SimpleSublist, lastPerSubject bool) (total, validThrough uint64) {
	if sl == nil {
		return ms.NumPending(sseq, fwcs, lastPerSubject)
	}

	// This needs to be a write lock, as we can mutate the per-subject state.
	ms.mu.Lock()
	defer ms.mu.Unlock()

	var ss SimpleState
	if sseq < ms.state.FirstSeq {
		sseq = ms.state.FirstSeq
	}
	// If past the end no results.
	if sseq > ms.state.LastSeq {
		return 0, ms.state.LastSeq
	}

	update := func(fss *SimpleState) {
		msgs, first, last := fss.Msgs, fss.First, fss.Last
		if lastPerSubject {
			msgs, first = 1, last
		}
		ss.Msgs += msgs
		if ss.First == 0 || first < ss.First {
			ss.First = first
		}
		if last > ss.Last {
			ss.Last = last
		}
	}

	var havePartial bool
	var totalSkipped uint64
	// We will track start and end sequences as we go.
	gsl.IntersectStree[SimpleState](ms.fss, sl, func(subj []byte, fss *SimpleState) {
		if fss.firstNeedsUpdate || fss.lastNeedsUpdate {
			ms.recalculateForSubj(bytesToString(subj), fss)
		}
		if sseq <= fss.First {
			update(fss)
		} else if sseq <= fss.Last {
			// We matched but it is a partial.
			havePartial = true
			// Don't break here, we will update to keep tracking last.
			update(fss)
		} else {
			totalSkipped += fss.Msgs
		}
	})

	// If we did not encounter any partials we can return here.
	if !havePartial {
		return ss.Msgs, ms.state.LastSeq
	}

	// If we are here we need to scan the msgs.
	// Capture first and last sequences for scan and then clear what we had.
	first, last := ss.First, ss.Last
	// To track if we decide to exclude we need to calculate first.
	if first < sseq {
		first = sseq
	}

	// Now we want to check if it is better to scan inclusive and recalculate that way
	// or leave and scan exclusive and adjust our totals.
	// ss.Last is always correct here.
	toScan, toExclude := last-first, first-ms.state.FirstSeq+ms.state.LastSeq-ss.Last
	var seen map[string]bool
	if lastPerSubject {
		seen = make(map[string]bool)
	}
	if toScan < toExclude {
		ss.Msgs, ss.First = 0, 0

		update := func(sm *StoreMsg) {
			ss.Msgs++
			if ss.First == 0 {
				ss.First = sm.seq
			}
			if seen != nil {
				seen[sm.subj] = true
			}
		}
		// Check if easier to just scan msgs vs the sequence range.
		// This can happen with lots of interior deletes.
		if last-first > uint64(len(ms.msgs)) {
			for _, sm := range ms.msgs {
				if sm.seq >= first && sm.seq <= last && !seen[sm.subj] && sl.HasInterest(sm.subj) {
					update(sm)
				}
			}
		} else {
			for seq := first; seq <= last; seq++ {
				if sm, ok := ms.msgs[seq]; ok && !seen[sm.subj] && sl.HasInterest(sm.subj) {
					update(sm)
				}
			}
		}
	} else {
		// We will adjust from the totals above by scanning what we need to exclude.
		ss.First = first
		ss.Msgs += totalSkipped
		var adjust uint64
		var tss *SimpleState

		update := func(sm *StoreMsg) {
			if lastPerSubject {
				tss, _ = ms.fss.Find(stringToBytes(sm.subj))
			}
			// If we are last per subject, make sure to only adjust if all messages are before our first.
			if tss == nil || tss.Last < first {
				adjust++
			}
			if seen != nil {
				seen[sm.subj] = true
			}
		}
		// Check if easier to just scan msgs vs the sequence range.
		if first-ms.state.FirstSeq > uint64(len(ms.msgs)) {
			for _, sm := range ms.msgs {
				if sm.seq < first && !seen[sm.subj] && sl.HasInterest(sm.subj) {
					update(sm)
				}
			}
		} else {
			for seq := ms.state.FirstSeq; seq < first; seq++ {
				if sm, ok := ms.msgs[seq]; ok && !seen[sm.subj] && sl.HasInterest(sm.subj) {
					update(sm)
				}
			}
		}
		// Now do range at end.
		for seq := last + 1; seq < ms.state.LastSeq; seq++ {
			if sm, ok := ms.msgs[seq]; ok && !seen[sm.subj] && sl.HasInterest(sm.subj) {
				adjust++
				if seen != nil {
					seen[sm.subj] = true
				}
			}
		}
		ss.Msgs -= adjust
	}

	return ss.Msgs, ms.state.LastSeq
}

// Will check the msg limit for this tracked subject.
// Lock should be held.
func (ms *memStore) enforcePerSubjectLimit(subj string, ss *SimpleState) {
	if ms.maxp <= 0 {
		return
	}
	for nmsgs := ss.Msgs; nmsgs > uint64(ms.maxp); nmsgs = ss.Msgs {
		if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
			ms.recalculateForSubj(subj, ss)
		}
		if !ms.removeMsg(ss.First, false) {
			break
		}
	}
}

// Will check the msg limit and drop firstSeq msg if needed.
// Lock should be held.
func (ms *memStore) enforceMsgLimit() {
	if ms.cfg.Discard != DiscardOld {
		return
	}
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
	if ms.cfg.Discard != DiscardOld {
		return
	}
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
	if ms.ageChk != nil {
		return
	}
	if ms.cfg.MaxAge != 0 || ms.ttls != nil {
		ms.ageChk = time.AfterFunc(ms.cfg.MaxAge, ms.expireMsgs)
	}
}

// Lock should be held.
func (ms *memStore) resetAgeChk(delta int64) {
	// If we're already expiring messages, it will make sure to reset.
	// Don't trigger again, as that could result in many expire goroutines.
	if ms.ageChkRun {
		return
	}

	var next int64 = math.MaxInt64
	if ms.ttls != nil {
		next = ms.ttls.GetNextExpiration(next)
	}

	// If there's no MaxAge and there's nothing waiting to be expired then
	// don't bother continuing. The next storeRawMsg() will wake us up if
	// needs be.
	if ms.cfg.MaxAge <= 0 && next == math.MaxInt64 {
		clearTimer(&ms.ageChk)
		return
	}

	// Check to see if we should be firing sooner than MaxAge for an expiring TTL.
	fireIn := ms.cfg.MaxAge

	// If delta for next-to-expire message is unset, but we still have messages to remove.
	// Assume messages are removed through proposals, and we need to speed up subsequent age check.
	if delta == 0 && ms.state.Msgs > 0 {
		if until := 2 * time.Second; until < fireIn {
			fireIn = until
		}
	}

	if next < math.MaxInt64 {
		// Looks like there's a next expiration, use it either if there's no
		// MaxAge set or if it looks to be sooner than MaxAge is.
		if until := time.Until(time.Unix(0, next)); fireIn == 0 || until < fireIn {
			fireIn = until
		}
	}

	// If not then look at the delta provided (usually gap to next age expiry).
	if delta > 0 {
		if fireIn == 0 || time.Duration(delta) < fireIn {
			fireIn = time.Duration(delta)
		}
	}

	// Make sure we aren't firing too often either way, otherwise we can
	// negatively impact stream ingest performance.
	if fireIn < 250*time.Millisecond {
		fireIn = 250 * time.Millisecond
	}

	// If we want to kick the timer to run later than what was assigned before, don't reset it.
	// Otherwise, we could get in a situation where the timer is continuously reset, and it never runs.
	expires := ats.AccessTime() + fireIn.Nanoseconds()
	if ms.ageChkTime > 0 && expires > ms.ageChkTime {
		return
	}

	ms.ageChkTime = expires
	if ms.ageChk != nil {
		ms.ageChk.Reset(fireIn)
	} else {
		ms.ageChk = time.AfterFunc(fireIn, ms.expireMsgs)
	}
}

// Lock should be held.
func (ms *memStore) cancelAgeChk() {
	if ms.ageChk != nil {
		ms.ageChk.Stop()
		ms.ageChk = nil
		ms.ageChkTime = 0
	}
}

// Will expire msgs that are too old.
func (ms *memStore) expireMsgs() {
	var smv StoreMsg
	var sm *StoreMsg
	ms.mu.Lock()
	maxAge := int64(ms.cfg.MaxAge)
	minAge := time.Now().UnixNano() - maxAge
	rmcb := ms.rmcb
	pmsgcb := ms.pmsgcb
	sdmTTL := int64(ms.cfg.SubjectDeleteMarkerTTL.Seconds())
	sdmEnabled := sdmTTL > 0

	// If SDM is enabled, but handlers aren't set up yet. Try again later.
	if sdmEnabled && (rmcb == nil || pmsgcb == nil) {
		ms.resetAgeChk(0)
		ms.mu.Unlock()
		return
	}
	ms.ageChkRun = true
	ms.mu.Unlock()

	if maxAge > 0 {
		var seq uint64
		for sm, seq, _ = ms.LoadNextMsg(fwcs, true, 0, &smv); sm != nil && sm.ts <= minAge; sm, seq, _ = ms.LoadNextMsg(fwcs, true, seq+1, &smv) {
			if len(sm.hdr) > 0 {
				if ttl, err := getMessageTTL(sm.hdr); err == nil && ttl < 0 {
					// The message has a negative TTL, therefore it must "never expire".
					minAge = time.Now().UnixNano() - maxAge
					continue
				}
			}
			if sdmEnabled {
				if last, ok := ms.shouldProcessSdm(seq, sm.subj); ok {
					sdm := last && !isSubjectDeleteMarker(sm.hdr)
					ms.handleRemovalOrSdm(seq, sm.subj, sdm, sdmTTL)
				}
			} else {
				ms.mu.Lock()
				ms.removeMsg(seq, false)
				ms.mu.Unlock()
			}
			// Recalculate in case we are expiring a bunch.
			minAge = time.Now().UnixNano() - maxAge
		}
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	// TODO: Not great that we're holding the lock here, but the timed hash wheel isn't thread-safe.
	nextTTL := int64(math.MaxInt64)
	var rmSeqs []thw.HashWheelEntry
	if ms.ttls != nil {
		ms.ttls.ExpireTasks(func(seq uint64, ts int64) bool {
			rmSeqs = append(rmSeqs, thw.HashWheelEntry{Seq: seq, Expires: ts})
			// We might need to remove messages out of band, those can fail, and we can be shutdown halfway
			// through so don't remove from THW just yet.
			return false
		})
		if maxAge > 0 {
			// Only check if we're expiring something in the next MaxAge interval, saves us a bit
			// of work if MaxAge will beat us to the next expiry anyway.
			nextTTL = ms.ttls.GetNextExpiration(time.Now().Add(time.Duration(maxAge)).UnixNano())
		} else {
			nextTTL = ms.ttls.GetNextExpiration(math.MaxInt64)
		}
	}

	// Remove messages collected by THW.
	if !sdmEnabled {
		for _, rm := range rmSeqs {
			ms.removeMsg(rm.Seq, false)
		}
	} else {
		// THW is unordered, so must sort by sequence and must not be holding the lock.
		ms.mu.Unlock()
		slices.SortFunc(rmSeqs, func(a, b thw.HashWheelEntry) int {
			if a.Seq == b.Seq {
				return 0
			} else if a.Seq < b.Seq {
				return -1
			} else {
				return 1
			}
		})
		for _, rm := range rmSeqs {
			// Need to grab subject for the specified sequence if for SDM, and check
			// if the message hasn't been removed in the meantime.
			// We need to grab the message and check if we should process SDM while holding the lock,
			// otherwise we can race if a deletion of this message is in progress.
			ms.mu.Lock()
			sm, _ = ms.loadMsgLocked(rm.Seq, &smv, false)
			if sm == nil {
				ms.ttls.Remove(rm.Seq, rm.Expires)
				ms.mu.Unlock()
				continue
			}
			last, ok := ms.shouldProcessSdmLocked(rm.Seq, sm.subj)
			ms.mu.Unlock()
			if ok {
				sdm := last && !isSubjectDeleteMarker(sm.hdr)
				ms.handleRemovalOrSdm(rm.Seq, sm.subj, sdm, sdmTTL)
			}
		}
		ms.mu.Lock()
	}

	// Only cancel if no message left, not on potential lookup error that would result in sm == nil.
	ms.ageChkRun, ms.ageChkTime = false, 0
	if ms.state.Msgs == 0 && nextTTL == math.MaxInt64 {
		ms.cancelAgeChk()
	} else {
		if sm == nil {
			ms.resetAgeChk(0)
		} else {
			ms.resetAgeChk(sm.ts - minAge)
		}
	}
}

func (ms *memStore) shouldProcessSdm(seq uint64, subj string) (bool, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.shouldProcessSdmLocked(seq, subj)
}

// Lock should be held.
func (ms *memStore) shouldProcessSdmLocked(seq uint64, subj string) (bool, bool) {
	if ms.sdm == nil {
		ms.sdm = newSDMMeta()
	}

	if p, ok := ms.sdm.pending[seq]; ok {
		// Don't allow more proposals for the same sequence if we already did recently.
		if time.Since(time.Unix(0, p.ts)) < 2*time.Second {
			return p.last, false
		}
		// If we're about to use the cached value, and we knew it was last before,
		// quickly check that we don't have more remaining messages for the subject now.
		// Which means we are not the last anymore and must reset to not remove later data.
		if p.last {
			msgs := ms.subjectsTotalsLocked(subj)[subj]
			numPending := ms.sdm.totals[subj]
			if remaining := msgs - numPending; remaining > 0 {
				p.last = false
			}
		}
		ms.sdm.pending[seq] = SDMBySeq{p.last, time.Now().UnixNano()}
		return p.last, true
	}

	msgs := ms.subjectsTotalsLocked(subj)[subj]
	if msgs == 0 {
		return false, true
	}
	numPending := ms.sdm.totals[subj]
	remaining := msgs - numPending
	return ms.sdm.trackPending(seq, subj, remaining == 1), true
}

func (ms *memStore) handleRemovalOrSdm(seq uint64, subj string, sdm bool, sdmTTL int64) {
	if sdm {
		var _hdr [128]byte
		hdr := fmt.Appendf(
			_hdr[:0],
			"NATS/1.0\r\n%s: %s\r\n%s: %s\r\n%s: %s\r\n\r\n",
			JSMarkerReason, JSMarkerReasonMaxAge,
			JSMessageTTL, time.Duration(sdmTTL)*time.Second,
			JSMsgRollup, JSMsgRollupSubject,
		)
		msg := &inMsg{
			subj: subj,
			hdr:  hdr,
		}
		ms.pmsgcb(msg)
	} else {
		ms.rmcb(seq)
	}
}

// Will run through scheduled messages.
func (ms *memStore) runMsgScheduling() {
	// TODO: Not great that we're holding the lock here, but the timed hash wheel and message scheduling isn't thread-safe.
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// If scheduling is enabled, but handler isn't set up yet. Try again later.
	if ms.scheduling == nil {
		return
	}
	if ms.pmsgcb == nil {
		ms.scheduling.resetTimer()
		return
	}
	ms.scheduling.running = true

	scheduledMsgs := ms.scheduling.getScheduledMessages(func(seq uint64, smv *StoreMsg) *StoreMsg {
		sm, _ := ms.loadMsgLocked(seq, smv, false)
		return sm
	})
	if len(scheduledMsgs) > 0 {
		ms.mu.Unlock()
		for _, msg := range scheduledMsgs {
			ms.pmsgcb(msg)
		}
		ms.mu.Lock()
	}

	ms.scheduling.running, ms.scheduling.deadline = false, 0
	ms.scheduling.resetTimer()
}

// PurgeEx will remove messages based on subject filters, sequence and number of messages to keep.
// Will return the number of purged messages.
func (ms *memStore) PurgeEx(subject string, sequence, keep uint64) (purged uint64, err error) {
	if subject == _EMPTY_ || subject == fwcs {
		if keep == 0 && sequence == 0 {
			return ms.purge(0)
		}
		if sequence > 1 {
			return ms.compact(sequence)
		} else if keep > 0 {
			ms.mu.RLock()
			msgs, lseq := ms.state.Msgs, ms.state.LastSeq
			ms.mu.RUnlock()
			if keep >= msgs {
				return 0, nil
			}
			return ms.compact(lseq - keep + 1)
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
		if sequence > 1 {
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
	return ms.purge(0)
}

func (ms *memStore) purge(fseq uint64) (uint64, error) {
	ms.mu.Lock()
	purged := uint64(len(ms.msgs))
	cb := ms.scb
	bytes := int64(ms.state.Bytes)
	if fseq == 0 {
		fseq = ms.state.LastSeq + 1
	} else if fseq < ms.state.LastSeq {
		ms.mu.Unlock()
		return 0, fmt.Errorf("partial purges not supported on memory store")
	}
	ms.state.FirstSeq = fseq
	ms.state.LastSeq = fseq - 1
	ms.state.FirstTime = time.Time{}
	ms.state.Bytes = 0
	ms.state.Msgs = 0
	if ms.msgs != nil {
		ms.msgs = make(map[uint64]*StoreMsg)
	}
	ms.fss = stree.NewSubjectTree[SimpleState]()
	ms.dmap.Empty()
	ms.sdm.empty()
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
	return ms.compact(seq)
}

func (ms *memStore) compact(seq uint64) (uint64, error) {
	if seq == 0 {
		return ms.Purge()
	}

	var purged, bytes uint64

	ms.mu.Lock()
	cb := ms.scb
	if seq <= ms.state.LastSeq {
		fseq := ms.state.FirstSeq
		// Determine new first sequence.
		for ; seq <= ms.state.LastSeq; seq++ {
			if sm, ok := ms.msgs[seq]; ok {
				ms.state.FirstSeq = seq
				ms.state.FirstTime = time.Unix(0, sm.ts).UTC()
				break
			}
		}
		for seq := seq - 1; seq >= fseq; seq-- {
			if sm := ms.msgs[seq]; sm != nil {
				bytes += memStoreMsgSize(sm.subj, sm.hdr, sm.msg)
				purged++
				ms.removeSeqPerSubject(sm.subj, seq)
				// Must delete message after updating per-subject info, to be consistent with file store.
				delete(ms.msgs, seq)
			} else if !ms.dmap.IsEmpty() {
				ms.dmap.Delete(seq)
			}
		}
		if purged > ms.state.Msgs {
			purged = ms.state.Msgs
		}
		ms.state.Msgs -= purged
		if bytes > ms.state.Bytes {
			bytes = ms.state.Bytes
		}
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
		// Reset msgs, fss and dmap.
		ms.msgs = make(map[uint64]*StoreMsg)
		ms.fss = stree.NewSubjectTree[SimpleState]()
		ms.dmap.Empty()
		ms.sdm.empty()
	}
	ms.mu.Unlock()

	if cb != nil {
		cb(-int64(purged), -int64(bytes), 0, _EMPTY_)
	}

	return purged, nil
}

// Will completely reset our store.
func (ms *memStore) reset() error {
	ms.mu.Lock()
	var purged, bytes uint64
	cb := ms.scb
	if cb != nil {
		for _, sm := range ms.msgs {
			purged++
			bytes += memStoreMsgSize(sm.subj, sm.hdr, sm.msg)
		}
	}

	// Reset
	ms.state.FirstSeq = 0
	ms.state.FirstTime = time.Time{}
	ms.state.LastSeq = 0
	ms.state.LastTime = time.Now().UTC()
	// Update msgs and bytes.
	ms.state.Msgs = 0
	ms.state.Bytes = 0
	// Reset msgs, fss and dmap.
	ms.msgs = make(map[uint64]*StoreMsg)
	ms.fss = stree.NewSubjectTree[SimpleState]()
	ms.dmap.Empty()
	ms.sdm.empty()

	ms.mu.Unlock()

	if cb != nil {
		cb(-int64(purged), -int64(bytes), 0, _EMPTY_)
	}

	return nil
}

// Truncate will truncate a stream store up to seq. Sequence needs to be valid.
func (ms *memStore) Truncate(seq uint64) error {
	// Check for request to reset.
	if seq == 0 {
		return ms.reset()
	}

	var purged, bytes uint64

	ms.mu.Lock()
	lsm, ok := ms.msgs[seq]
	lastTime := ms.state.LastTime
	if ok && lsm != nil {
		lastTime = time.Unix(0, lsm.ts).UTC()
	}

	for i := ms.state.LastSeq; i > seq; i-- {
		if sm := ms.msgs[i]; sm != nil {
			purged++
			bytes += memStoreMsgSize(sm.subj, sm.hdr, sm.msg)
			ms.removeSeqPerSubject(sm.subj, i)
			// Must delete message after updating per-subject info, to be consistent with file store.
			delete(ms.msgs, i)
		} else if !ms.dmap.IsEmpty() {
			ms.dmap.Delete(i)
		}
	}
	// Reset last.
	ms.state.LastSeq = seq
	ms.state.LastTime = lastTime
	// Update msgs and bytes.
	if purged > ms.state.Msgs {
		purged = ms.state.Msgs
	}
	ms.state.Msgs -= purged
	if bytes > ms.state.Bytes {
		bytes = ms.state.Bytes
	}
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

// SubjectForSeq will return what the subject is for this sequence if found.
func (ms *memStore) SubjectForSeq(seq uint64) (string, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if seq < ms.state.FirstSeq {
		return _EMPTY_, ErrStoreMsgNotFound
	}
	if sm, ok := ms.msgs[seq]; ok {
		return sm.subj, nil
	}
	return _EMPTY_, ErrStoreMsgNotFound
}

// LoadMsg will lookup the message by sequence number and return it if found.
func (ms *memStore) LoadMsg(seq uint64, smp *StoreMsg) (*StoreMsg, error) {
	return ms.loadMsgLocked(seq, smp, true)
}

// loadMsgLocked will lookup the message by sequence number and return it if found.
func (ms *memStore) loadMsgLocked(seq uint64, smp *StoreMsg, needMSLock bool) (*StoreMsg, error) {
	if needMSLock {
		ms.mu.RLock()
	}
	sm, ok := ms.msgs[seq]
	last := ms.state.LastSeq
	if needMSLock {
		ms.mu.RUnlock()
	}

	if !ok || sm == nil {
		var err = ErrStoreEOF
		if seq <= last {
			err = ErrStoreMsgNotFound
		}
		return nil, err
	}

	if smp == nil {
		smp = new(StoreMsg)
	}
	sm.copy(smp)
	return smp, nil
}

// LoadLastMsg will return the last message we have that matches a given subject.
// The subject can be a wildcard.
func (ms *memStore) LoadLastMsg(subject string, smp *StoreMsg) (*StoreMsg, error) {
	var sm *StoreMsg
	var ok bool

	// This needs to be a write lock, as filteredStateLocked can
	// mutate the per-subject state.
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if subject == _EMPTY_ || subject == fwcs {
		sm, ok = ms.msgs[ms.state.LastSeq]
	} else if subjectIsLiteral(subject) {
		var ss *SimpleState
		if ss, ok = ms.fss.Find(stringToBytes(subject)); ok && ss.Msgs > 0 {
			sm, ok = ms.msgs[ss.Last]
		}
	} else if ss := ms.filteredStateLocked(1, subject, true); ss.Msgs > 0 {
		sm, ok = ms.msgs[ss.Last]
	}
	if !ok || sm == nil {
		return nil, ErrStoreMsgNotFound
	}

	if smp == nil {
		smp = new(StoreMsg)
	}
	sm.copy(smp)
	return smp, nil
}

// LoadNextMsgMulti will find the next message matching any entry in the sublist.
func (ms *memStore) LoadNextMsgMulti(sl *gsl.SimpleSublist, start uint64, smp *StoreMsg) (sm *StoreMsg, skip uint64, err error) {
	// TODO(dlc) - for now simple linear walk to get started.
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if start < ms.state.FirstSeq {
		start = ms.state.FirstSeq
	}

	// If past the end no results.
	if start > ms.state.LastSeq || ms.state.Msgs == 0 {
		return nil, ms.state.LastSeq, ErrStoreEOF
	}

	// Initial setup.
	fseq, lseq := start, ms.state.LastSeq

	for nseq := fseq; nseq <= lseq; nseq++ {
		sm, ok := ms.msgs[nseq]
		if !ok {
			continue
		}
		if sl.HasInterest(sm.subj) {
			if smp == nil {
				smp = new(StoreMsg)
			}
			sm.copy(smp)
			return smp, nseq, nil
		}
	}
	return nil, ms.state.LastSeq, ErrStoreEOF
}

// LoadNextMsg will find the next message matching the filter subject starting at the start sequence.
// The filter subject can be a wildcard.
func (ms *memStore) LoadNextMsg(filter string, wc bool, start uint64, smp *StoreMsg) (*StoreMsg, uint64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.loadNextMsgLocked(filter, wc, start, smp)
}

// Lock should be held.
func (ms *memStore) loadNextMsgLocked(filter string, wc bool, start uint64, smp *StoreMsg) (*StoreMsg, uint64, error) {
	if start < ms.state.FirstSeq {
		start = ms.state.FirstSeq
	}

	// If past the end no results.
	if start > ms.state.LastSeq || ms.state.Msgs == 0 {
		return nil, ms.state.LastSeq, ErrStoreEOF
	}

	if filter == _EMPTY_ {
		filter = fwcs
	}
	isAll := filter == fwcs

	// Skip scan of ms.fss if number of messages in the block are less than
	// 1/2 the number of subjects in ms.fss. Or we have a wc and lots of fss entries.
	const linearScanMaxFSS = 256
	doLinearScan := isAll || 2*int(ms.state.LastSeq-start) < ms.fss.Size() || (wc && ms.fss.Size() > linearScanMaxFSS)

	// Initial setup.
	fseq, lseq := start, ms.state.LastSeq

	if !doLinearScan {
		subs := []string{filter}
		if wc || isAll {
			subs = subs[:0]
			ms.fss.Match(stringToBytes(filter), func(subj []byte, val *SimpleState) {
				subs = append(subs, string(subj))
			})
		}
		fseq, lseq = ms.state.LastSeq, uint64(0)
		for _, subj := range subs {
			ss, ok := ms.fss.Find(stringToBytes(subj))
			if !ok {
				continue
			}
			if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
				ms.recalculateForSubj(subj, ss)
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
	}

	eq := subjectsEqual
	if wc {
		eq = subjectIsSubsetMatch
	}

	for nseq := fseq; nseq <= lseq; nseq++ {
		if sm, ok := ms.msgs[nseq]; ok && (isAll || eq(sm.subj, filter)) {
			if smp == nil {
				smp = new(StoreMsg)
			}
			sm.copy(smp)
			return smp, nseq, nil
		}
	}
	return nil, ms.state.LastSeq, ErrStoreEOF
}

// Will load the next non-deleted msg starting at the start sequence and walking backwards.
func (ms *memStore) LoadPrevMsg(start uint64, smp *StoreMsg) (sm *StoreMsg, err error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if ms.msgs == nil {
		return nil, ErrStoreClosed
	}
	if ms.state.Msgs == 0 || start < ms.state.FirstSeq {
		return nil, ErrStoreEOF
	}
	if start > ms.state.LastSeq {
		start = ms.state.LastSeq
	}

	for seq := start; seq >= ms.state.FirstSeq; seq-- {
		if sm, ok := ms.msgs[seq]; ok {
			if smp == nil {
				smp = new(StoreMsg)
			}
			sm.copy(smp)
			return smp, nil
		}
	}
	return nil, ErrStoreEOF
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
	var nsm *StoreMsg
	var ok bool
	for nseq := ms.state.FirstSeq + 1; nseq <= ms.state.LastSeq; nseq++ {
		if nsm, ok = ms.msgs[nseq]; ok {
			break
		}
	}
	oldFirst := ms.state.FirstSeq
	if nsm != nil {
		ms.state.FirstSeq = nsm.seq
		ms.state.FirstTime = time.Unix(0, nsm.ts).UTC()
	} else {
		// Like purge.
		ms.state.FirstSeq = ms.state.LastSeq + 1
		ms.state.FirstTime = time.Time{}
	}

	if oldFirst == ms.state.FirstSeq-1 {
		ms.dmap.Delete(oldFirst)
	} else {
		for seq := oldFirst; seq < ms.state.FirstSeq; seq++ {
			ms.dmap.Delete(seq)
		}
	}
}

// Remove a seq from the fss and select new first.
// Lock should be held.
func (ms *memStore) removeSeqPerSubject(subj string, seq uint64) {
	ss, ok := ms.fss.Find(stringToBytes(subj))
	if !ok {
		return
	}
	ms.sdm.removeSeqAndSubject(seq, subj)
	if ss.Msgs == 1 {
		ms.fss.Delete(stringToBytes(subj))
		return
	}
	ss.Msgs--

	// Only one left
	if ss.Msgs == 1 {
		if !ss.lastNeedsUpdate && seq != ss.Last {
			ss.First = ss.Last
			ss.firstNeedsUpdate = false
			return
		}
		if !ss.firstNeedsUpdate && seq != ss.First {
			ss.Last = ss.First
			ss.lastNeedsUpdate = false
			return
		}
	}

	// We can lazily calculate the first/last sequence when needed.
	ss.firstNeedsUpdate = seq == ss.First || ss.firstNeedsUpdate
	ss.lastNeedsUpdate = seq == ss.Last || ss.lastNeedsUpdate
}

// Will recalculate the first and/or last sequence for this subject.
// Lock should be held.
func (ms *memStore) recalculateForSubj(subj string, ss *SimpleState) {
	if ss.firstNeedsUpdate {
		tseq := ss.First + 1
		if tseq < ms.state.FirstSeq {
			tseq = ms.state.FirstSeq
		}
		for ; tseq <= ss.Last; tseq++ {
			if sm := ms.msgs[tseq]; sm != nil && sm.subj == subj {
				ss.First = tseq
				ss.firstNeedsUpdate = false
				if ss.Msgs == 1 {
					ss.Last = tseq
					ss.lastNeedsUpdate = false
					return
				}
				break
			}
		}
	}
	if ss.lastNeedsUpdate {
		tseq := ss.Last - 1
		if tseq > ms.state.LastSeq {
			tseq = ms.state.LastSeq
		}
		for ; tseq >= ss.First; tseq-- {
			if sm := ms.msgs[tseq]; sm != nil && sm.subj == subj {
				ss.Last = tseq
				ss.lastNeedsUpdate = false
				if ss.Msgs == 1 {
					ss.First = tseq
					ss.firstNeedsUpdate = false
				}
				return
			}
		}
	}
}

// Removes the message referenced by seq.
// Lock should be held.
func (ms *memStore) removeMsg(seq uint64, secure bool) bool {
	var ss uint64
	sm, ok := ms.msgs[seq]
	if !ok {
		return false
	}

	ss = memStoreMsgSize(sm.subj, sm.hdr, sm.msg)

	if ms.state.Msgs > 0 {
		ms.state.Msgs--
		if ss > ms.state.Bytes {
			ss = ms.state.Bytes
		}
		ms.state.Bytes -= ss
	}
	ms.dmap.Insert(seq)
	ms.updateFirstSeq(seq)

	// Remove any per subject tracking.
	ms.removeSeqPerSubject(sm.subj, seq)
	if ms.ttls != nil {
		if ttl, err := getMessageTTL(sm.hdr); err == nil {
			expires := time.Duration(sm.ts) + (time.Second * time.Duration(ttl))
			ms.ttls.Remove(seq, int64(expires))
		}
	}

	if secure {
		if len(sm.hdr) > 0 {
			sm.hdr = make([]byte, len(sm.hdr))
			crand.Read(sm.hdr)
		}
		if len(sm.msg) > 0 {
			sm.msg = make([]byte, len(sm.msg))
			crand.Read(sm.msg)
		}
		sm.seq, sm.ts = 0, 0
	}

	// Must delete message after updating per-subject info, to be consistent with file store.
	delete(ms.msgs, seq)

	if ms.scb != nil {
		// We do not want to hold any locks here.
		ms.mu.Unlock()
		if ms.scb != nil {
			delta := int64(ss)
			ms.scb(-1, -delta, seq, sm.subj)
		}
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
		state.NumDeleted = int((state.LastSeq - state.FirstSeq + 1) - state.Msgs)
		if state.NumDeleted < 0 {
			state.NumDeleted = 0
		}
	}
	state.Consumers = ms.consumers
	state.NumSubjects = ms.fss.Size()
	ms.mu.RUnlock()
}

func (ms *memStore) State() StreamState {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	state := ms.state
	state.Consumers = ms.consumers
	state.NumSubjects = ms.fss.Size()
	state.Deleted = nil

	// Calculate interior delete details.
	if numDeleted := int((state.LastSeq - state.FirstSeq + 1) - state.Msgs); numDeleted > 0 {
		state.Deleted = make([]uint64, 0, numDeleted)
		fseq, lseq := state.FirstSeq, state.LastSeq
		ms.dmap.Range(func(seq uint64) bool {
			if seq < fseq || seq > lseq {
				ms.dmap.Delete(seq)
			} else {
				state.Deleted = append(state.Deleted, seq)
			}
			return true
		})
	}
	if len(state.Deleted) > 0 {
		state.NumDeleted = len(state.Deleted)
	}

	return state
}

func (ms *memStore) Utilization() (total, reported uint64, err error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.state.Bytes, ms.state.Bytes, nil
}

func memStoreMsgSizeRaw(slen, hlen, mlen int) uint64 {
	return uint64(slen + hlen + mlen + 16) // 8*2 for seq + age
}

func memStoreMsgSize(subj string, hdr, msg []byte) uint64 {
	return memStoreMsgSizeRaw(len(subj), len(hdr), len(msg))
}

// ResetState resets any state that's temporary. For example when changing leaders.
func (ms *memStore) ResetState() {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.scheduling != nil {
		ms.scheduling.clearInflight()
	}
}

// Delete is same as Stop for memory store.
func (ms *memStore) Delete(_ bool) error {
	return ms.Stop()
}

func (ms *memStore) Stop() error {
	ms.mu.Lock()
	if ms.msgs == nil {
		ms.mu.Unlock()
		return nil
	}
	if ms.ageChk != nil {
		ms.ageChk.Stop()
		ms.ageChk = nil
		ms.ageChkTime = 0
	}
	ms.msgs = nil
	ms.mu.Unlock()

	// These can't come back, so stop is same as Delete.
	ms.Purge()

	// Unregister from the access time service.
	ats.Unregister()

	return nil
}

func (ms *memStore) isClosed() bool {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.msgs == nil
}

type consumerMemStore struct {
	mu     sync.Mutex
	ms     StreamStore
	cfg    ConsumerConfig
	state  ConsumerState
	closed bool
}

func (ms *memStore) ConsumerStore(name string, cfg *ConsumerConfig) (ConsumerStore, error) {
	if ms == nil {
		return nil, fmt.Errorf("memstore is nil")
	}
	if ms.isClosed() {
		return nil, ErrStoreClosed
	}
	if cfg == nil || name == _EMPTY_ {
		return nil, fmt.Errorf("bad consumer config")
	}
	o := &consumerMemStore{ms: ms, cfg: *cfg}
	ms.AddConsumer(o)
	return o, nil
}

func (ms *memStore) AddConsumer(o ConsumerStore) error {
	ms.mu.Lock()
	ms.consumers++
	ms.mu.Unlock()
	return nil
}

func (ms *memStore) RemoveConsumer(o ConsumerStore) error {
	ms.mu.Lock()
	if ms.consumers > 0 {
		ms.consumers--
	}
	ms.mu.Unlock()
	return nil
}

func (ms *memStore) Snapshot(_ time.Duration, _, _ bool) (*SnapshotResult, error) {
	return nil, fmt.Errorf("no impl")
}

// Binary encoded state snapshot, >= v2.10 server.
func (ms *memStore) EncodedStreamState(failed uint64) ([]byte, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	// Quick calculate num deleted.
	numDeleted := int((ms.state.LastSeq - ms.state.FirstSeq + 1) - ms.state.Msgs)
	if numDeleted < 0 {
		numDeleted = 0
	}

	// Encoded is Msgs, Bytes, FirstSeq, LastSeq, Failed, NumDeleted and optional DeletedBlocks
	var buf [1024]byte
	buf[0], buf[1] = streamStateMagic, streamStateVersion
	n := hdrLen
	n += binary.PutUvarint(buf[n:], ms.state.Msgs)
	n += binary.PutUvarint(buf[n:], ms.state.Bytes)
	n += binary.PutUvarint(buf[n:], ms.state.FirstSeq)
	n += binary.PutUvarint(buf[n:], ms.state.LastSeq)
	n += binary.PutUvarint(buf[n:], failed)
	n += binary.PutUvarint(buf[n:], uint64(numDeleted))

	b := buf[0:n]

	if numDeleted > 0 {
		buf, err := ms.dmap.Encode(nil)
		if err != nil {
			return nil, err
		}
		b = append(b, buf...)
	}

	return b, nil
}

// SyncDeleted will make sure this stream has same deleted state as dbs.
func (ms *memStore) SyncDeleted(dbs DeleteBlocks) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// For now we share one dmap, so if we have one entry here check if states are the same.
	// Note this will work for any DeleteBlock type, but we expect this to be a dmap too.
	if len(dbs) == 1 {
		min, max, num := ms.dmap.State()
		if pmin, pmax, pnum := dbs[0].State(); pmin == min && pmax == max && pnum == num {
			return
		}
	}
	lseq := ms.state.LastSeq
	for _, db := range dbs {
		// Skip if beyond our current state.
		if first, _, _ := db.State(); first > lseq {
			continue
		}
		db.Range(func(seq uint64) bool {
			ms.removeMsg(seq, false)
			return true
		})
	}
}

func (o *consumerMemStore) Update(state *ConsumerState) error {
	// Sanity checks.
	if state.AckFloor.Consumer > state.Delivered.Consumer {
		return fmt.Errorf("bad ack floor for consumer")
	}
	if state.AckFloor.Stream > state.Delivered.Stream {
		return fmt.Errorf("bad ack floor for stream")
	}

	// Copy to our state.
	var pending map[uint64]*Pending
	var redelivered map[uint64]uint64
	if len(state.Pending) > 0 {
		pending = make(map[uint64]*Pending, len(state.Pending))
		for seq, p := range state.Pending {
			pending[seq] = &Pending{p.Sequence, p.Timestamp}
			if seq <= state.AckFloor.Stream || seq > state.Delivered.Stream {
				return fmt.Errorf("bad pending entry, sequence [%d] out of range", seq)
			}
		}
	}
	if len(state.Redelivered) > 0 {
		redelivered = make(map[uint64]uint64, len(state.Redelivered))
		for seq, dc := range state.Redelivered {
			redelivered[seq] = dc
		}
	}

	// Replace our state.
	o.mu.Lock()
	defer o.mu.Unlock()

	// Check to see if this is an outdated update.
	if state.Delivered.Consumer < o.state.Delivered.Consumer || state.AckFloor.Stream < o.state.AckFloor.Stream {
		return fmt.Errorf("old update ignored")
	}

	o.state.Delivered = state.Delivered
	o.state.AckFloor = state.AckFloor
	o.state.Pending = pending
	o.state.Redelivered = redelivered

	return nil
}

// SetStarting sets our starting stream sequence.
func (o *consumerMemStore) SetStarting(sseq uint64) error {
	o.mu.Lock()
	o.state.Delivered.Stream = sseq
	o.mu.Unlock()
	return nil
}

// UpdateStarting updates our starting stream sequence.
func (o *consumerMemStore) UpdateStarting(sseq uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if sseq > o.state.Delivered.Stream {
		o.state.Delivered.Stream = sseq
		// For AckNone just update delivered and ackfloor at the same time.
		if o.cfg.AckPolicy == AckNone {
			o.state.AckFloor.Stream = sseq
		}
	}
}

// HasState returns if this store has a recorded state.
func (o *consumerMemStore) HasState() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	// We have a running state.
	return o.state.Delivered.Consumer != 0 || o.state.Delivered.Stream != 0
}

func (o *consumerMemStore) UpdateDelivered(dseq, sseq, dc uint64, ts int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if dc != 1 && o.cfg.AckPolicy == AckNone {
		return ErrNoAckPolicy
	}

	// On restarts the old leader may get a replay from the raft logs that are old.
	if dseq <= o.state.AckFloor.Consumer {
		return nil
	}

	// See if we expect an ack for this.
	if o.cfg.AckPolicy != AckNone {
		// Need to create pending records here.
		if o.state.Pending == nil {
			o.state.Pending = make(map[uint64]*Pending)
		}
		var p *Pending
		// Check for an update to a message already delivered.
		if sseq <= o.state.Delivered.Stream {
			if p = o.state.Pending[sseq]; p != nil {
				// Do not update p.Sequence, that should be the original delivery sequence.
				p.Timestamp = ts
			}
		} else {
			// Add to pending.
			o.state.Pending[sseq] = &Pending{dseq, ts}
		}
		// Update delivered as needed.
		if dseq > o.state.Delivered.Consumer {
			o.state.Delivered.Consumer = dseq
		}
		if sseq > o.state.Delivered.Stream {
			o.state.Delivered.Stream = sseq
		}

		if dc > 1 {
			if maxdc := uint64(o.cfg.MaxDeliver); maxdc > 0 && dc > maxdc {
				// Make sure to remove from pending.
				delete(o.state.Pending, sseq)
			}
			if o.state.Redelivered == nil {
				o.state.Redelivered = make(map[uint64]uint64)
			}
			// Only update if greater than what we already have.
			if o.state.Redelivered[sseq] < dc-1 {
				o.state.Redelivered[sseq] = dc - 1
			}
		}
	} else {
		// For AckNone just update delivered and ackfloor at the same time.
		if dseq > o.state.Delivered.Consumer {
			o.state.Delivered.Consumer = dseq
			o.state.AckFloor.Consumer = dseq
		}
		if sseq > o.state.Delivered.Stream {
			o.state.Delivered.Stream = sseq
			o.state.AckFloor.Stream = sseq
		}
	}

	return nil
}

func (o *consumerMemStore) UpdateAcks(dseq, sseq uint64) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.cfg.AckPolicy == AckNone {
		return ErrNoAckPolicy
	}

	// On restarts the old leader may get a replay from the raft logs that are old.
	if dseq <= o.state.AckFloor.Consumer {
		return nil
	}

	if len(o.state.Pending) == 0 || o.state.Pending[sseq] == nil {
		delete(o.state.Redelivered, sseq)
		return ErrStoreMsgNotFound
	}

	// Check for AckAll here.
	if o.cfg.AckPolicy == AckAll {
		sgap := sseq - o.state.AckFloor.Stream
		o.state.AckFloor.Consumer = dseq
		o.state.AckFloor.Stream = sseq
		if sgap > uint64(len(o.state.Pending)) {
			for seq := range o.state.Pending {
				if seq <= sseq {
					delete(o.state.Pending, seq)
					delete(o.state.Redelivered, seq)
				}
			}
		} else {
			for seq := sseq; seq > sseq-sgap && len(o.state.Pending) > 0; seq-- {
				delete(o.state.Pending, seq)
				delete(o.state.Redelivered, seq)
			}
		}
		return nil
	}

	// AckExplicit

	// First delete from our pending state.
	if p, ok := o.state.Pending[sseq]; ok {
		delete(o.state.Pending, sseq)
		if dseq > p.Sequence && p.Sequence > 0 {
			dseq = p.Sequence // Use the original.
		}
	}

	if len(o.state.Pending) == 0 {
		o.state.AckFloor.Consumer = o.state.Delivered.Consumer
		o.state.AckFloor.Stream = o.state.Delivered.Stream
	} else if dseq == o.state.AckFloor.Consumer+1 {
		o.state.AckFloor.Consumer = dseq
		o.state.AckFloor.Stream = sseq

		if o.state.Delivered.Consumer > dseq {
			for ss := sseq + 1; ss <= o.state.Delivered.Stream; ss++ {
				if p, ok := o.state.Pending[ss]; ok {
					if p.Sequence > 0 {
						o.state.AckFloor.Consumer = p.Sequence - 1
						o.state.AckFloor.Stream = ss - 1
					}
					break
				}
			}
		}
	}
	// We do these regardless.
	delete(o.state.Redelivered, sseq)

	return nil
}

func (o *consumerMemStore) UpdateConfig(cfg *ConsumerConfig) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// This is mostly unchecked here. We are assuming the upper layers have done sanity checking.
	o.cfg = *cfg
	return nil
}

func (o *consumerMemStore) Stop() error {
	o.mu.Lock()
	o.closed = true
	ms := o.ms
	o.mu.Unlock()
	ms.RemoveConsumer(o)
	return nil
}

func (o *consumerMemStore) Delete() error {
	return o.Stop()
}

func (o *consumerMemStore) StreamDelete() error {
	return o.Stop()
}

func (o *consumerMemStore) State() (*ConsumerState, error) {
	return o.stateWithCopy(true)
}

// This will not copy pending or redelivered, so should only be done under the
// consumer owner's lock.
func (o *consumerMemStore) BorrowState() (*ConsumerState, error) {
	return o.stateWithCopy(false)
}

func (o *consumerMemStore) stateWithCopy(doCopy bool) (*ConsumerState, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil, ErrStoreClosed
	}

	state := &ConsumerState{}

	state.Delivered = o.state.Delivered
	state.AckFloor = o.state.AckFloor
	if len(o.state.Pending) > 0 {
		if doCopy {
			state.Pending = o.copyPending()
		} else {
			state.Pending = o.state.Pending
		}
	}
	if len(o.state.Redelivered) > 0 {
		if doCopy {
			state.Redelivered = o.copyRedelivered()
		} else {
			state.Redelivered = o.state.Redelivered
		}
	}
	return state, nil
}

// EncodedState for this consumer store.
func (o *consumerMemStore) EncodedState() ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil, ErrStoreClosed
	}

	return encodeConsumerState(&o.state), nil
}

func (o *consumerMemStore) copyPending() map[uint64]*Pending {
	pending := make(map[uint64]*Pending, len(o.state.Pending))
	for seq, p := range o.state.Pending {
		pending[seq] = &Pending{p.Sequence, p.Timestamp}
	}
	return pending
}

func (o *consumerMemStore) copyRedelivered() map[uint64]uint64 {
	redelivered := make(map[uint64]uint64, len(o.state.Redelivered))
	for seq, dc := range o.state.Redelivered {
		redelivered[seq] = dc
	}
	return redelivered
}

// Type returns the type of the underlying store.
func (o *consumerMemStore) Type() StorageType { return MemoryStorage }

// Templates
type templateMemStore struct{}

func newTemplateMemStore() *templateMemStore {
	return &templateMemStore{}
}

// No-ops for memstore.
func (ts *templateMemStore) Store(t *streamTemplate) error  { return nil }
func (ts *templateMemStore) Delete(t *streamTemplate) error { return nil }
