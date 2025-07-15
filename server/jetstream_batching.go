// Copyright 2025 The NATS Authors
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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// Tracks the total inflight batches, across all streams and accounts that enable batching.
	globalInflightBatches atomic.Int32
)

type batching struct {
	mu    sync.Mutex
	group map[string]*batchGroup
}

type batchGroup struct {
	lseq  uint64
	store StreamStore
	timer *time.Timer
}

// Lock should be held.
func (batches *batching) newBatchGroup(mset *stream, batchId string) (*batchGroup, error) {
	store, err := newBatchStore(mset, batchId)
	if err != nil {
		return nil, err
	}
	b := &batchGroup{store: store}

	// Create a timer to clean up after timeout.
	timeout := streamMaxBatchTimeout
	if maxBatchTimeout := mset.srv.getOpts().JetStreamLimits.MaxBatchTimeout; maxBatchTimeout > 0 {
		timeout = maxBatchTimeout
	}
	b.timer = time.AfterFunc(timeout, func() {
		b.cleanup(batchId, batches)
	})
	return b, nil
}

func getBatchStoreDir(mset *stream, batchId string) (string, string) {
	mset.mu.RLock()
	jsa, name := mset.jsa, mset.cfg.Name
	mset.mu.RUnlock()

	jsa.mu.RLock()
	sd := jsa.storeDir
	jsa.mu.RUnlock()

	bname := getHash(batchId)
	return bname, filepath.Join(sd, streamsDir, name, batchesDir, bname)
}

func newBatchStore(mset *stream, batchId string) (StreamStore, error) {
	mset.mu.RLock()
	replicas, storage := mset.cfg.Replicas, mset.cfg.Storage
	mset.mu.RUnlock()

	if replicas == 1 && storage == FileStorage {
		bname, storeDir := getBatchStoreDir(mset, batchId)
		fcfg := FileStoreConfig{AsyncFlush: true, BlockSize: defaultLargeBlockSize, StoreDir: storeDir}
		s := mset.srv
		prf := s.jsKeyGen(s.getOpts().JetStreamKey, mset.acc.Name)
		if prf != nil {
			// We are encrypted here, fill in correct cipher selection.
			fcfg.Cipher = s.getOpts().JetStreamCipher
		}
		oldprf := s.jsKeyGen(s.getOpts().JetStreamOldKey, mset.acc.Name)
		cfg := StreamConfig{Name: bname, Storage: FileStorage}
		return newFileStoreWithCreated(fcfg, cfg, time.Time{}, prf, oldprf)
	}
	return newMemStore(&StreamConfig{Name: _EMPTY_, Storage: MemoryStorage})
}

// readyForCommit indicates the batch is ready to be committed.
// If the timer has already cleaned up the batch, we can't commit.
// Otherwise, we ensure the timer does not clean up the batch in the meantime.
// Lock should be held.
func (b *batchGroup) readyForCommit() bool {
	if !b.timer.Stop() {
		return false
	}
	b.store.FlushAllPending()
	return true
}

// cleanup deletes underlying resources associated with the batch and unregisters it from the stream's batches.
func (b *batchGroup) cleanup(batchId string, batches *batching) {
	batches.mu.Lock()
	defer batches.mu.Unlock()
	b.cleanupLocked(batchId, batches)
}

// Lock should be held.
func (b *batchGroup) cleanupLocked(batchId string, batches *batching) {
	globalInflightBatches.Add(-1)
	b.timer.Stop()
	b.store.Delete(true)
	delete(batches.group, batchId)
}

// Lock should be held.
func (b *batchGroup) stopLocked() {
	globalInflightBatches.Add(-1)
	b.timer.Stop()
	b.store.Stop()
}

// batchStagedDiff stages all changes for consistency checks until commit.
type batchStagedDiff struct {
	msgIds             map[string]struct{}
	counter            map[string]*msgCounterRunningTotal
	inflight           map[string]*inflightSubjectRunningTotal
	expectedPerSubject map[string]*batchExpectedPerSubject
}

type batchExpectedPerSubject struct {
	sseq  uint64 // Stream sequence.
	clseq uint64 // Clustered proposal sequence.
}

func (diff *batchStagedDiff) commit(mset *stream) {
	if len(diff.msgIds) > 0 {
		ts := time.Now().UnixNano()
		mset.ddMu.Lock()
		for msgId := range diff.msgIds {
			// We stage with zero, and will update in processJetStreamMsg once we know the sequence.
			mset.storeMsgIdLocked(&ddentry{msgId, 0, ts})
		}
		mset.ddMu.Unlock()
	}

	// Store running totals for counters, we could have multiple counter increments proposed, but not applied yet.
	if len(diff.counter) > 0 {
		if mset.clusteredCounterTotal == nil {
			mset.clusteredCounterTotal = make(map[string]*msgCounterRunningTotal, len(diff.counter))
		}
		for k, c := range diff.counter {
			mset.clusteredCounterTotal[k] = c
		}
	}

	// Track inflight.
	if len(diff.inflight) > 0 {
		if mset.inflight == nil {
			mset.inflight = make(map[string]*inflightSubjectRunningTotal, len(diff.inflight))
		}
		for subj, i := range diff.inflight {
			if c, ok := mset.inflight[subj]; ok {
				c.bytes += i.bytes
				c.ops += i.ops
			} else {
				mset.inflight[subj] = i
			}
		}
	}

	// Track sequence and subject.
	if len(diff.expectedPerSubject) > 0 {
		if mset.expectedPerSubjectSequence == nil {
			mset.expectedPerSubjectSequence = make(map[uint64]string, len(diff.expectedPerSubject))
		}
		if mset.expectedPerSubjectInProcess == nil {
			mset.expectedPerSubjectInProcess = make(map[string]struct{}, len(diff.expectedPerSubject))
		}
		for subj, e := range diff.expectedPerSubject {
			mset.expectedPerSubjectSequence[e.clseq] = subj
			mset.expectedPerSubjectInProcess[subj] = struct{}{}
		}
	}
}

type batchApply struct {
	mu         sync.Mutex
	id         string            // ID of the current batch.
	count      uint64            // Number of entries in the batch, for consistency checks.
	entries    []*CommittedEntry // Previous entries that are part of this batch.
	entryStart int               // The index into an entry indicating the first message of the batch.
	maxApplied uint64            // Applied value before the entry containing the first message of the batch.
}

// clearBatchStateLocked clears in-memory apply-batch-related state.
// batch.mu lock should be held.
func (batch *batchApply) clearBatchStateLocked() {
	batch.id = _EMPTY_
	batch.count = 0
	batch.entries = nil
	batch.entryStart = 0
	batch.maxApplied = 0
}

// rejectBatchStateLocked rejects the batch and clears in-memory apply-batch-related state.
// Corrects mset.clfs to take the failed batch into account.
// batch.mu lock should be held.
func (batch *batchApply) rejectBatchStateLocked(mset *stream) {
	mset.clMu.Lock()
	mset.clfs += batch.count
	mset.clMu.Unlock()
	// We're rejecting the batch, so all entries need to be returned to the pool.
	for _, bce := range batch.entries {
		bce.ReturnToPool()
	}
	batch.clearBatchStateLocked()
}

func (batch *batchApply) rejectBatchState(mset *stream) {
	batch.mu.Lock()
	defer batch.mu.Unlock()
	batch.rejectBatchStateLocked(mset)
}

// checkMsgHeadersPreClusteredProposal checks the message for expected/consistency headers.
// mset.mu lock must NOT be held or used.
// mset.clMu lock must be held.
func checkMsgHeadersPreClusteredProposal(
	diff *batchStagedDiff, mset *stream, subject string, hdr []byte, msg []byte, sourced bool, name string,
	jsa *jsAccount, allowRollup, denyPurge, allowTTL, allowMsgCounter, allowMsgSchedules bool,
	discard DiscardPolicy, discardNewPer bool, maxMsgSize int, maxMsgs int64, maxMsgsPer int64, maxBytes int64,
) ([]byte, []byte, uint64, *ApiError, error) {
	var incr *big.Int

	// Some header checks must be checked pre proposal.
	if len(hdr) > 0 {
		// Since we encode header len as u16 make sure we do not exceed.
		// Again this works if it goes through but better to be pre-emptive.
		if len(hdr) > math.MaxUint16 {
			err := fmt.Errorf("JetStream header size exceeds limits for '%s > %s'", jsa.acc().Name, mset.cfg.Name)
			return hdr, msg, 0, NewJSStreamHeaderExceedsMaximumError(), err
		}
		// Counter increments.
		// Only supported on counter streams, and payload must be empty (if not coming from a source).
		var ok bool
		if incr, ok = getMessageIncr(hdr); !ok {
			apiErr := NewJSMessageIncrInvalidError()
			return hdr, msg, 0, apiErr, apiErr
		} else if incr != nil && !sourced {
			// Only do checks if the message isn't sourced. Otherwise, we need to store verbatim.
			if !allowMsgCounter {
				apiErr := NewJSMessageIncrDisabledError()
				return hdr, msg, 0, apiErr, apiErr
			} else if len(msg) > 0 {
				apiErr := NewJSMessageIncrPayloadError()
				return hdr, msg, 0, apiErr, apiErr
			} else {
				// Check for incompatible headers.
				var doErr bool
				if getRollup(hdr) != _EMPTY_ ||
					getExpectedStream(hdr) != _EMPTY_ ||
					getExpectedLastMsgId(hdr) != _EMPTY_ ||
					getExpectedLastSeqPerSubjectForSubject(hdr) != _EMPTY_ {
					doErr = true
				} else if _, ok = getExpectedLastSeq(hdr); ok {
					doErr = true
				} else if _, ok = getExpectedLastSeqPerSubject(hdr); ok {
					doErr = true
				}

				if doErr {
					apiErr := NewJSMessageIncrInvalidError()
					return hdr, msg, 0, apiErr, apiErr
				}
			}
		}
		// Expected stream name can also be pre-checked.
		if sname := getExpectedStream(hdr); sname != _EMPTY_ && sname != name {
			return hdr, msg, 0, NewJSStreamNotMatchError(), errStreamMismatch
		}
		// TTL'd messages are rejected entirely if TTLs are not enabled on the stream, or if the TTL is invalid.
		if ttl, err := getMessageTTL(hdr); !sourced && (ttl != 0 || err != nil) {
			if !allowTTL {
				return hdr, msg, 0, NewJSMessageTTLDisabledError(), errMsgTTLDisabled
			} else if err != nil {
				return hdr, msg, 0, NewJSMessageTTLInvalidError(), err
			}
		}
		// Check for MsgIds here at the cluster level to avoid excessive CLFS accounting.
		// Will help during restarts.
		if msgId := getMsgId(hdr); msgId != _EMPTY_ {
			// Dedupe if staged.
			if _, ok = diff.msgIds[msgId]; ok {
				return hdr, msg, 0, nil, errMsgIdDuplicate
			}
			mset.ddMu.Lock()
			if dde := mset.checkMsgId(msgId); dde != nil {
				seq := dde.seq
				mset.ddMu.Unlock()
				// Should not return an invalid sequence, in that case error.
				if seq > 0 {
					return hdr, msg, seq, nil, errMsgIdDuplicate
				} else {
					return hdr, msg, 0, NewJSStreamDuplicateMessageConflictError(), errMsgIdDuplicate
				}
			}
			if diff.msgIds == nil {
				diff.msgIds = map[string]struct{}{msgId: {}}
			} else {
				diff.msgIds[msgId] = struct{}{}
			}
			mset.ddMu.Unlock()
		}
	}

	// Apply increment for counter.
	// But only if it's allowed for this stream. This can happen when we store verbatim for a sourced stream.
	if incr == nil && allowMsgCounter {
		apiErr := NewJSMessageIncrMissingError()
		return hdr, msg, 0, apiErr, apiErr
	}
	if incr != nil && allowMsgCounter {
		var initial big.Int
		var sources CounterSources

		// If we've got a running total, update that, since we have inflight proposals updating the same counter.
		var ok bool
		var counter *msgCounterRunningTotal
		if counter, ok = diff.counter[subject]; ok {
			initial = *counter.total
			sources = counter.sources
		} else if counter, ok = mset.clusteredCounterTotal[subject]; ok {
			initial = *counter.total
			sources = counter.sources
			// Make an explicit copy to separate the staged data from what's committed.
			// Don't need to initialize all values, they'll be overwritten later.
			counter = &msgCounterRunningTotal{ops: counter.ops}
		} else {
			// Load last message, and store as inflight running total.
			var smv StoreMsg
			sm, err := mset.store.LoadLastMsg(subject, &smv)
			if err == nil && sm != nil {
				var val CounterValue
				// Return an error if the counter is broken somehow.
				if json.Unmarshal(sm.msg, &val) != nil {
					apiErr := NewJSMessageCounterBrokenError()
					return hdr, msg, 0, apiErr, apiErr
				}
				if ncs := sliceHeader(JSMessageCounterSources, sm.hdr); len(ncs) > 0 {
					if err := json.Unmarshal(ncs, &sources); err != nil {
						apiErr := NewJSMessageCounterBrokenError()
						return hdr, msg, 0, apiErr, apiErr
					}
				}
				initial.SetString(val.Value, 10)
			}
		}
		srchdr := sliceHeader(JSStreamSource, hdr)
		if len(srchdr) > 0 {
			// This is a sourced message, so we can't apply Nats-Incr but
			// instead should just update the source count header.
			fields := strings.Split(string(srchdr), " ")
			origStream := fields[0]
			origSubj := subject
			if len(fields) >= 5 {
				origSubj = fields[4]
			}
			var val CounterValue
			if json.Unmarshal(msg, &val) != nil {
				apiErr := NewJSMessageCounterBrokenError()
				return hdr, msg, 0, apiErr, apiErr
			}
			var sourced big.Int
			sourced.SetString(val.Value, 10)
			if sources == nil {
				sources = map[string]map[string]string{}
			}
			if _, ok = sources[origStream]; !ok {
				sources[origStream] = map[string]string{}
			}
			prevVal := sources[origStream][origSubj]
			sources[origStream][origSubj] = sourced.String()
			// We will also replace the Nats-Incr header with the diff
			// between our last value from this source and this one, so
			// that the arithmetic is always correct.
			var previous big.Int
			previous.SetString(prevVal, 10)
			incr.Sub(&sourced, &previous)
			hdr = setHeader(JSMessageIncr, incr.String(), hdr)
		}
		// Now make the change.
		initial.Add(&initial, incr)
		// Generate the new payload.
		var _msg [128]byte
		msg = fmt.Appendf(_msg[:0], "{%q:%q}", "val", initial.String())
		// Write the updated source count headers.
		if len(sources) > 0 {
			nhdr, err := json.Marshal(sources)
			if err != nil {
				return hdr, msg, 0, NewJSMessageCounterBrokenError(), err
			}
			hdr = setHeader(JSMessageCounterSources, string(nhdr), hdr)
		}

		// Check to see if we are over the max msg size.
		maxSize := int64(mset.srv.getOpts().MaxPayload)
		if maxMsgSize >= 0 && int64(maxMsgSize) < maxSize {
			maxSize = int64(maxMsgSize)
		}
		hdrLen, msgLen := int64(len(hdr)), int64(len(msg))
		// Subtract to prevent against overflows.
		if hdrLen > maxSize || msgLen > maxSize-hdrLen {
			return hdr, msg, 0, NewJSStreamMessageExceedsMaximumError(), ErrMaxPayload
		}

		// Keep the in-memory counters up-to-date.
		if counter == nil {
			counter = &msgCounterRunningTotal{}
		}
		counter.total = &initial
		counter.sources = sources
		counter.ops++
		if diff.counter == nil {
			diff.counter = map[string]*msgCounterRunningTotal{subject: counter}
		} else {
			diff.counter[subject] = counter
		}
	}

	if len(hdr) > 0 {
		// Expected last sequence.
		if seq, exists := getExpectedLastSeq(hdr); exists && seq != mset.clseq-mset.clfs {
			mlseq := mset.clseq - mset.clfs
			err := fmt.Errorf("last sequence mismatch: %d vs %d", seq, mlseq)
			return hdr, msg, 0, NewJSStreamWrongLastSequenceError(mlseq), err
		}

		// Expected last sequence per subject.
		if seq, exists := getExpectedLastSeqPerSubject(hdr); exists {
			// Allow override of the subject used for the check.
			seqSubj := subject
			if optSubj := getExpectedLastSeqPerSubjectForSubject(hdr); optSubj != _EMPTY_ {
				seqSubj = optSubj
			}

			// The subject is already written to in this batch, we can't allow
			// expected checks since they would be incorrect.
			if _, ok := diff.inflight[seqSubj]; ok {
				err := errors.New("last sequence by subject mismatch")
				return hdr, msg, 0, NewJSStreamWrongLastSequenceConstantError(), err
			}

			// If the subject is already in process, block as otherwise we could have
			// multiple messages inflight with the same subject.
			if _, found := mset.expectedPerSubjectInProcess[seqSubj]; found {
				err := errors.New("last sequence by subject mismatch")
				return hdr, msg, 0, NewJSStreamWrongLastSequenceConstantError(), err
			}

			// If the subject is already in process but without expected headers, block as we would have
			// multiple messages inflight with the same subject.
			if _, ok := mset.inflight[seqSubj]; ok {
				err := errors.New("last sequence by subject mismatch")
				return hdr, msg, 0, NewJSStreamWrongLastSequenceConstantError(), err
			}

			// If we've already done an expected-check on this subject, use the cached result.
			if e, ok := diff.expectedPerSubject[seqSubj]; ok {
				if e.sseq != seq {
					err := fmt.Errorf("last sequence by subject mismatch: %d vs %d", seq, e.sseq)
					return hdr, msg, 0, NewJSStreamWrongLastSequenceError(e.sseq), err
				}
				e.clseq = mset.clseq
			} else {
				var smv StoreMsg
				var fseq uint64
				sm, err := mset.store.LoadLastMsg(seqSubj, &smv)
				if sm != nil {
					fseq = sm.seq
				}
				if err == ErrStoreMsgNotFound && seq == 0 {
					fseq, err = 0, nil
				}
				if err != nil || fseq != seq {
					err = fmt.Errorf("last sequence by subject mismatch: %d vs %d", seq, fseq)
					return hdr, msg, 0, NewJSStreamWrongLastSequenceError(fseq), err
				}

				e = &batchExpectedPerSubject{sseq: fseq, clseq: mset.clseq}
				if diff.expectedPerSubject == nil {
					diff.expectedPerSubject = map[string]*batchExpectedPerSubject{seqSubj: e}
				} else {
					diff.expectedPerSubject[seqSubj] = e
				}
			}
		} else if getExpectedLastSeqPerSubjectForSubject(hdr) != _EMPTY_ {
			apiErr := NewJSStreamExpectedLastSeqPerSubjectInvalidError()
			return hdr, msg, 0, apiErr, apiErr
		}

		// Message scheduling.
		if schedule, ok := getMessageSchedule(hdr); !ok {
			apiErr := NewJSMessageSchedulesPatternInvalidError()
			if !allowMsgSchedules {
				apiErr = NewJSMessageSchedulesDisabledError()
			}
			return hdr, msg, 0, apiErr, apiErr
		} else if !schedule.IsZero() {
			if !allowMsgSchedules {
				apiErr := NewJSMessageSchedulesDisabledError()
				return hdr, msg, 0, apiErr, apiErr
			} else if scheduleTtl, ok := getMessageScheduleTTL(hdr); !ok {
				apiErr := NewJSMessageSchedulesTTLInvalidError()
				return hdr, msg, 0, apiErr, apiErr
			} else if scheduleTtl != _EMPTY_ && !allowTTL {
				return hdr, msg, 0, NewJSMessageTTLDisabledError(), errMsgTTLDisabled
			} else if scheduleTarget := getMessageScheduleTarget(hdr); scheduleTarget == _EMPTY_ ||
				!IsValidPublishSubject(scheduleTarget) || SubjectsCollide(scheduleTarget, subject) {
				apiErr := NewJSMessageSchedulesTargetInvalidError()
				return hdr, msg, 0, apiErr, apiErr
			} else {
				mset.cfgMu.RLock()
				match := slices.ContainsFunc(mset.cfg.Subjects, func(subj string) bool {
					return SubjectsCollide(subj, scheduleTarget)
				})
				mset.cfgMu.RUnlock()
				if !match {
					apiErr := NewJSMessageSchedulesTargetInvalidError()
					return hdr, msg, 0, apiErr, apiErr
				}

				// Add a rollup sub header if it doesn't already exist.
				// Otherwise, it must exist already as a rollup on the subject.
				if rollup := getRollup(hdr); rollup == _EMPTY_ {
					hdr = genHeader(hdr, JSMsgRollup, JSMsgRollupSubject)
				} else if rollup != JSMsgRollupSubject {
					apiErr := NewJSMessageSchedulesRollupInvalidError()
					return hdr, msg, 0, apiErr, apiErr
				}
			}
		}

		// Check for any rollups.
		if rollup := getRollup(hdr); rollup != _EMPTY_ {
			if !allowRollup || denyPurge {
				err := errors.New("rollup not permitted")
				return hdr, msg, 0, NewJSStreamRollupFailedError(err), err
			}
			switch rollup {
			case JSMsgRollupSubject:
				// Rolling up the subject is only allowed if the first occurrence of this subject in the batch.
				if _, ok := diff.inflight[subject]; ok {
					err := errors.New("batch rollup sub invalid")
					return hdr, msg, 0, NewJSStreamRollupFailedError(err), err
				}
			case JSMsgRollupAll:
				// Rolling up the whole stream is only allowed if this is the first message of the batch.
				if len(diff.inflight) > 0 {
					err := errors.New("batch rollup all invalid")
					return hdr, msg, 0, NewJSStreamRollupFailedError(err), err
				}
			default:
				err := fmt.Errorf("rollup value invalid: %q", rollup)
				return hdr, msg, 0, NewJSStreamRollupFailedError(err), err
			}
		}
	}

	// Track inflight.
	// Store the subject to ensure other messages in this batch using
	// an expected check or rollup on the same subject fail.
	if diff.inflight == nil {
		diff.inflight = make(map[string]*inflightSubjectRunningTotal, 1)
	}
	var sz uint64
	if mset.store.Type() == FileStorage {
		sz = fileStoreMsgSizeRaw(len(subject), len(hdr), len(msg))
	} else {
		sz = memStoreMsgSizeRaw(len(subject), len(hdr), len(msg))
	}
	var (
		i   *inflightSubjectRunningTotal
		ok  bool
		err error
	)
	if i, ok = diff.inflight[subject]; ok {
		i.bytes += sz
		i.ops++
	} else {
		i = &inflightSubjectRunningTotal{bytes: sz, ops: 1}
		diff.inflight[subject] = i
	}

	// Check if we have discard new with max msgs or bytes.
	// We need to deny here otherwise we'd need to bump CLFS, and it could succeed on some
	// peers and not others depending on consumer ack state (if interest policy).
	// So we deny here, if we allow that means we know it would succeed on every peer.
	if discard == DiscardNew && (maxMsgs > 0 || maxBytes > 0) {
		// Error if over DiscardNew per subject threshold.
		if discardNewPer {
			totalMsgsForSubject := i.ops
			if i, ok = mset.inflight[subject]; ok {
				totalMsgsForSubject += i.ops
			}
			if maxMsgsPer > 0 && totalMsgsForSubject > uint64(maxMsgsPer) {
				err = ErrMaxMsgsPerSubject
				return hdr, msg, 0, NewJSStreamStoreFailedError(err, Unless(err)), err
			}
		}

		// Track usual max msgs/bytes thresholds for DiscardNew.
		var state StreamState
		mset.store.FastState(&state)

		totalMsgs := state.Msgs
		totalBytes := state.Bytes
		for _, i = range mset.inflight {
			totalMsgs += i.ops
			totalBytes += i.bytes
		}
		for _, i = range diff.inflight {
			totalMsgs += i.ops
			totalBytes += i.bytes
		}

		if maxMsgs > 0 && totalMsgs > uint64(maxMsgs) {
			err = ErrMaxMsgs
		} else if maxBytes > 0 && totalBytes > uint64(maxBytes) {
			err = ErrMaxBytes
		}
		if err != nil {
			return hdr, msg, 0, NewJSStreamStoreFailedError(err, Unless(err)), err
		}
	}

	return hdr, msg, 0, nil, nil
}
