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
	"fmt"
	"math"
	"sync"
	"time"
)

type batching struct {
	mu    sync.Mutex
	group map[string]*batchGroup
}

type batchGroup struct {
	lseq  uint64
	store StreamStore
}

// checkMsgHeadersPreClusteredProposal checks the message for expected/consistency headers.
// mset.mu lock must NOT be held or used.
// mset.clMu lock must be held.
func checkMsgHeadersPreClusteredProposal(
	mset *stream, subject string, hdr []byte, mlen int, sourced bool, name string,
	jsa *jsAccount, allowTTL bool, stype StorageType, store StreamStore,
	interestPolicy bool, discard DiscardPolicy, maxMsgs int64, maxBytes int64,
) (uint64, *ApiError, error) {
	// Some header checks must be checked pre proposal.
	if len(hdr) > 0 {
		// Since we encode header len as u16 make sure we do not exceed.
		// Again this works if it goes through but better to be pre-emptive.
		if len(hdr) > math.MaxUint16 {
			err := fmt.Errorf("JetStream header size exceeds limits for '%s > %s'", jsa.acc().Name, mset.cfg.Name)
			return 0, NewJSStreamHeaderExceedsMaximumError(), err
		}
		// Expected stream name can also be pre-checked.
		if sname := getExpectedStream(hdr); sname != _EMPTY_ && sname != name {
			return 0, NewJSStreamNotMatchError(), errStreamMismatch
		}
		// TTL'd messages are rejected entirely if TTLs are not enabled on the stream, or if the TTL is invalid.
		if ttl, err := getMessageTTL(hdr); !sourced && (ttl != 0 || err != nil) {
			if !allowTTL {
				return 0, NewJSMessageTTLDisabledError(), errMsgTTLDisabled
			} else if err != nil {
				return 0, NewJSMessageTTLInvalidError(), err
			}
		}
		// Check for MsgIds here at the cluster level to avoid excessive CLFS accounting.
		// Will help during restarts.
		if msgId := getMsgId(hdr); msgId != _EMPTY_ {
			mset.ddMu.Lock()
			if dde := mset.checkMsgId(msgId); dde != nil {
				seq := dde.seq
				mset.ddMu.Unlock()
				// Should not return an invalid sequence, in that case error.
				if seq > 0 {
					return seq, nil, errMsgIdDuplicate
				} else {
					return 0, NewJSStreamDuplicateMessageConflictError(), errMsgIdDuplicate
				}
			}
			// We stage with zero, and will update in processJetStreamMsg once we know the sequence.
			mset.storeMsgIdLocked(&ddentry{msgId, 0, time.Now().UnixNano()})
			mset.ddMu.Unlock()
		}
	}

	// Check if we have an interest policy and discard new with max msgs or bytes.
	// We need to deny here otherwise it could succeed on some peers and not others
	// depending on consumer ack state. So we deny here, if we allow that means we know
	// it would succeed on every peer.
	if interestPolicy && discard == DiscardNew && (maxMsgs > 0 || maxBytes > 0) {
		// Track inflight.
		if mset.inflight == nil {
			mset.inflight = make(map[uint64]uint64)
		}
		if stype == FileStorage {
			mset.inflight[mset.clseq] = fileStoreMsgSizeRaw(len(subject), len(hdr), mlen)
		} else {
			mset.inflight[mset.clseq] = memStoreMsgSizeRaw(len(subject), len(hdr), mlen)
		}

		var state StreamState
		mset.store.FastState(&state)

		var err error
		if maxMsgs > 0 && state.Msgs+uint64(len(mset.inflight)) > uint64(maxMsgs) {
			err = ErrMaxMsgs
		} else if maxBytes > 0 {
			// TODO(dlc) - Could track this rollup independently.
			var bytesPending uint64
			for _, nb := range mset.inflight {
				bytesPending += nb
			}
			if state.Bytes+bytesPending > uint64(maxBytes) {
				err = ErrMaxBytes
			}
		}
		if err != nil {
			delete(mset.inflight, mset.clseq)
			return 0, NewJSStreamStoreFailedError(err, Unless(err)), err
		}
	}

	if len(hdr) > 0 {
		// Expected last sequence per subject.
		if seq, exists := getExpectedLastSeqPerSubject(hdr); exists && store != nil {
			// Allow override of the subject used for the check.
			seqSubj := subject
			if optSubj := getExpectedLastSeqPerSubjectForSubject(hdr); optSubj != _EMPTY_ {
				seqSubj = optSubj
			}

			// If subject is already in process, block as otherwise we could have multiple messages inflight with same subject.
			if _, found := mset.expectedPerSubjectInProcess[seqSubj]; found {
				// Could have set inflight above, cleanup here.
				delete(mset.inflight, mset.clseq)
				err := fmt.Errorf("last sequence by subject mismatch")
				return 0, NewJSStreamWrongLastSequenceConstantError(), err
			}

			var smv StoreMsg
			var fseq uint64
			sm, err := store.LoadLastMsg(seqSubj, &smv)
			if sm != nil {
				fseq = sm.seq
			}
			if err == ErrStoreMsgNotFound && seq == 0 {
				fseq, err = 0, nil
			}
			if err != nil || fseq != seq {
				// Could have set inflight above, cleanup here.
				delete(mset.inflight, mset.clseq)
				err = fmt.Errorf("last sequence by subject mismatch: %d vs %d", seq, fseq)
				return 0, NewJSStreamWrongLastSequenceError(fseq), err
			}

			// Track sequence and subject.
			if mset.expectedPerSubjectSequence == nil {
				mset.expectedPerSubjectSequence = make(map[uint64]string)
			}
			if mset.expectedPerSubjectInProcess == nil {
				mset.expectedPerSubjectInProcess = make(map[string]struct{})
			}
			mset.expectedPerSubjectSequence[mset.clseq] = seqSubj
			mset.expectedPerSubjectInProcess[seqSubj] = struct{}{}
		}
	}

	return 0, nil, nil
}
