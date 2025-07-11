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

//go:build !skip_js_tests

package server

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// FIXME(mvv): don't allow sourced batch API messages to be handled as a batch, or test how it works generally if the field is set

func TestJetStreamBatchingStageAndCommit(t *testing.T) {
	type BatchItem struct {
		subject string
		header  nats.Header
		err     error
	}

	type BatchTest struct {
		title              string
		allowTTL           bool
		allowMsgCounter    bool
		interestDiscardNew bool
		init               func(mset *stream)
		batch              []BatchItem
		validate           func(mset *stream, commit bool)
	}

	tests := []BatchTest{
		{
			title: "dedupe-distinct",
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgId: {"foo"}}},
				{subject: "bar", header: nats.Header{JSMsgId: {"bar"}}},
			},
			validate: func(mset *stream, commit bool) {
				require_Equal(t, mset.checkMsgId("foo") != nil, commit)
				require_Equal(t, mset.checkMsgId("bar") != nil, commit)
			},
		},
		{
			title: "dedupe",
			init: func(mset *stream) {
				mset.storeMsgId(&ddentry{id: "foo", seq: 0, ts: time.Now().UnixNano()})
			},
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgId: {"foo"}}, err: errMsgIdDuplicate},
			},
			validate: func(mset *stream, commit bool) {
				require_NotNil(t, mset.checkMsgId("foo"))
			},
		},
		{
			title: "dedupe-staged",
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgId: {"foo"}}},
				{subject: "foo", header: nats.Header{JSMsgId: {"foo"}}, err: errMsgIdDuplicate},
			},
			validate: func(mset *stream, commit bool) {
				require_Equal(t, mset.checkMsgId("foo") != nil, commit)
			},
		},
		{
			title:           "counter-single",
			allowMsgCounter: true,
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMessageIncr: {"1"}}},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_True(t, mset.clusteredCounterTotal["foo"] == nil)
				} else {
					counter := mset.clusteredCounterTotal["foo"]
					require_NotNil(t, counter)
					require_Equal(t, counter.ops, 1)
					require_Equal(t, counter.total.String(), "1")
				}
			},
		},
		{
			title:           "counter-multiple",
			allowMsgCounter: true,
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMessageIncr: {"1"}}},
				{subject: "foo", header: nats.Header{JSMessageIncr: {"2"}}},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_True(t, mset.clusteredCounterTotal["foo"] == nil)
				} else {
					counter := mset.clusteredCounterTotal["foo"]
					require_NotNil(t, counter)
					require_Equal(t, counter.ops, 2)
					require_Equal(t, counter.total.String(), "3")
				}
			},
		},
		{
			title:           "counter-pre-init",
			allowMsgCounter: true,
			init: func(mset *stream) {
				mset.clusteredCounterTotal = map[string]*msgCounterRunningTotal{
					"foo": {total: big.NewInt(1), ops: 1},
				}
			},
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMessageIncr: {"2"}}},
			},
			validate: func(mset *stream, commit bool) {
				counter := mset.clusteredCounterTotal["foo"]
				require_NotNil(t, counter)
				if !commit {
					require_Equal(t, counter.ops, 1)
					require_Equal(t, counter.total.String(), "1")
				} else {
					require_Equal(t, counter.ops, 2)
					require_Equal(t, counter.total.String(), "3")
				}
			},
		},
		{
			title:              "interest-discard-new",
			interestDiscardNew: true,
			batch: []BatchItem{
				{subject: "foo"},
				{subject: "foo_2"},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.inflight), 0)
				} else {
					require_Len(t, len(mset.inflight), 2)
					require_Equal(t, mset.inflight[0], 19)
					require_Equal(t, mset.inflight[1], 21)
				}
			},
		},
		// TODO(mvv): expected-last-sequence header
		{
			title: "expect-per-subj-simple",
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSExpectedLastSubjSeq: {"0"}}},
				{subject: "bar", header: nats.Header{JSExpectedLastSubjSeq: {"0"}}},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.expectedPerSubjectSequence), 0)
					require_Len(t, len(mset.expectedPerSubjectInProcess), 0)
				} else {
					require_Len(t, len(mset.expectedPerSubjectSequence), 2)
					require_Len(t, len(mset.expectedPerSubjectInProcess), 2)
					require_Equal(t, mset.expectedPerSubjectSequence[0], "foo")
					require_Equal(t, mset.expectedPerSubjectSequence[1], "bar")
				}
			},
		},
		{
			title: "expect-per-subj-single-batch",
			batch: []BatchItem{
				// Would normally fail the batch, just drops this.
				{
					subject: "foo",
					header:  nats.Header{JSExpectedLastSubjSeq: {"1"}},
					err:     errors.New("last sequence by subject mismatch: 1 vs 0"),
				},
				{subject: "foo", header: nats.Header{JSExpectedLastSubjSeq: {"0"}}},
				// Would normally fail the batch, recognize in-process.
				{
					subject: "foo",
					header:  nats.Header{JSExpectedLastSubjSeq: {"1"}},
					err:     errors.New("last sequence by subject mismatch: 1 vs 0"),
				},
				// Seems invalid, but is actually a redundant expected check that matches the state prior to the batch.
				// It's accepted as normal, just like the first message.
				{subject: "foo", header: nats.Header{JSExpectedLastSubjSeq: {"0"}}},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.expectedPerSubjectSequence), 0)
					require_Len(t, len(mset.expectedPerSubjectInProcess), 0)
				} else {
					require_Len(t, len(mset.expectedPerSubjectSequence), 1)
					require_Len(t, len(mset.expectedPerSubjectInProcess), 1)
					require_Equal(t, mset.expectedPerSubjectSequence[3], "foo")
				}
			},
		},
		{
			title: "expect-per-subj-change",
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSExpectedLastSubjSeq: {"0"}, JSExpectedLastSubjSeqSubj: {"baz"}}},
				{subject: "bar", header: nats.Header{JSExpectedLastSubjSeq: {"0"}, JSExpectedLastSubjSeqSubj: {"baz"}}},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.expectedPerSubjectSequence), 0)
					require_Len(t, len(mset.expectedPerSubjectInProcess), 0)
				} else {
					require_Len(t, len(mset.expectedPerSubjectSequence), 1)
					require_Len(t, len(mset.expectedPerSubjectInProcess), 1)
					require_Equal(t, mset.expectedPerSubjectSequence[1], "baz")
				}
			},
		},
		{
			title: "expect-per-subj-in-process",
			init: func(mset *stream) {
				mset.expectedPerSubjectInProcess = map[string]struct{}{"foo": {}}
			},
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSExpectedLastSubjSeq: {"10"}}, err: errors.New("last sequence by subject mismatch")},
			},
			validate: func(mset *stream, commit bool) {
				require_Len(t, len(mset.expectedPerSubjectSequence), 0)
				require_Len(t, len(mset.expectedPerSubjectInProcess), 1)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:               "TEST",
				Storage:            MemoryStorage,
				AllowAtomicPublish: true,
			})
			require_NoError(t, err)

			mset, err := s.globalAccount().lookupStream("TEST")
			require_NoError(t, err)
			store := mset.store
			require_NotNil(t, store)

			if test.init != nil {
				test.init(mset)
			}

			var (
				interestPolicy bool
				discard        DiscardPolicy
				maxMsgs        int64
				maxBytes       int64
			)
			if test.interestDiscardNew {
				interestPolicy, discard = true, DiscardNew
				maxMsgs, maxBytes = 10, 1024
			}

			diff := &batchStagedDiff{}
			for _, m := range test.batch {
				var hdr []byte
				for key, values := range m.header {
					for _, value := range values {
						hdr = genHeader(hdr, key, value)
					}
				}
				_, _, _, _, err = checkMsgHeadersPreClusteredProposal(diff, mset, m.subject, hdr, nil, false, "TEST", nil, test.allowTTL, test.allowMsgCounter, MemoryStorage, store, interestPolicy, discard, maxMsgs, maxBytes)
				if m.err != nil {
					require_Error(t, err, m.err)
				} else {
					require_True(t, err == nil)
				}
				test.validate(mset, false)
				mset.clseq++
			}
			diff.commit(mset)
			test.validate(mset, true)
		})
	}
}
