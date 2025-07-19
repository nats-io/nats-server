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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"

	"github.com/nats-io/nats.go"
)

func TestJetStreamAtomicBatchPublish(t *testing.T) {
	test := func(
		t *testing.T,
		nc *nats.Conn,
		js nats.JetStreamContext,
		storage StorageType,
		retention RetentionPolicy,
		replicas int,
	) {
		var pubAck JSPubAckResponse

		cfg := &StreamConfig{
			Name:      "TEST",
			Subjects:  []string{"foo.*"},
			Storage:   storage,
			Retention: retention,
			Replicas:  replicas,
		}

		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		m := nats.NewMsg("foo.0")
		m.Data = []byte("foo.0")
		m.Header.Set("Nats-Batch-Id", "uuid")

		// Publish with atomic publish disabled.
		rmsg, err := nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_NotNil(t, pubAck.Error)
		require_Error(t, pubAck.Error, NewJSAtomicPublishDisabledError())

		// Enable atomic publish.
		cfg.AllowAtomicPublish = true
		_, err = jsStreamUpdate(t, nc, cfg)
		require_NoError(t, err)

		// Publish without batch sequence errors.
		rmsg, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSAtomicPublishMissingSeqError())

		// Publish a batch, misses start.
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "2")
		rmsg, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSAtomicPublishIncompleteBatchError())

		// Publish a "batch" which immediately commits.
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		m.Header.Set("Nats-Batch-Commit", "1")
		rmsg, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Equal(t, pubAck.Sequence, 1)
		require_Equal(t, pubAck.BatchId, "uuid")
		require_Equal(t, pubAck.BatchSize, 1)

		// Reset commit.
		m.Header.Del("Nats-Batch-Commit")

		// Publish a "batch" which has gaps.
		require_NoError(t, nc.PublishMsg(m))
		m.Header.Set("Nats-Batch-Sequence", "3")
		rmsg, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSAtomicPublishIncompleteBatchError())

		// Publish a batch of N messages.
		m.Header.Del("Nats-Batch-Commit")
		for seq, batch := uint64(1), uint64(5); seq <= batch; seq++ {
			m.Subject = fmt.Sprintf("foo.%d", seq)
			m.Data = []byte(m.Subject)
			m.Header.Set("Nats-Batch-Sequence", strconv.FormatUint(seq, 10))
			// If not commit.
			if seq != batch {
				require_NoError(t, nc.PublishMsg(m))
				continue
			}

			m.Header.Set("Nats-Batch-Commit", "1")
			rmsg, err = nc.RequestMsg(m, time.Second)
			require_NoError(t, err)

			pubAck = JSPubAckResponse{}
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			require_Equal(t, pubAck.Sequence, 6)
			require_Equal(t, pubAck.BatchId, "uuid")
			require_Equal(t, pubAck.BatchSize, 5)
		}

		// Validate stream contents.
		if retention != InterestPolicy {
			for i := 0; i < 6; i++ {
				rsm, err := js.GetMsg("TEST", uint64(i+1))
				require_NoError(t, err)
				subj := fmt.Sprintf("foo.%d", i)
				require_Equal(t, rsm.Subject, subj)
				require_Equal(t, string(rsm.Data), subj)
			}
		}

		// TODO(mvv): implement timeout
	}

	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		for _, retention := range []RetentionPolicy{LimitsPolicy, InterestPolicy, WorkQueuePolicy} {
			replicas := 3
			t.Run(fmt.Sprintf("%s/%s/R%d", storage, retention, replicas), func(t *testing.T) {
				c := createJetStreamClusterExplicit(t, "R3S", 3)
				defer c.shutdown()

				nc, js := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				test(t, nc, js, storage, retention, replicas)
			})
		}
	}
}

func TestJetStreamAtomicBatchPublishDedupeNotAllowed(t *testing.T) {
	test := func(
		t *testing.T,
		nc *nats.Conn,
		js nats.JetStreamContext,
		storage StorageType,
		retention RetentionPolicy,
		replicas int,
	) {
		cfg := &StreamConfig{
			Name:               "TEST",
			Retention:          retention,
			Subjects:           []string{"foo"},
			Replicas:           3,
			Storage:            FileStorage,
			AllowAtomicPublish: true,
		}

		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		m.Header.Set("Nats-Msg-Id", "msgId1")
		require_NoError(t, nc.PublishMsg(m))
		m.Header.Set("Nats-Batch-Sequence", "2")
		m.Header.Set("Nats-Msg-Id", "pre-existing")
		require_NoError(t, nc.PublishMsg(m))

		var pubAck JSPubAckResponse
		m.Header.Set("Nats-Batch-Sequence", "3")
		m.Header.Set("Nats-Msg-Id", "msgId2")
		m.Header.Set("Nats-Batch-Commit", "1")
		rmsg, err := nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_NotNil(t, pubAck.Error)
		require_Error(t, pubAck.Error, NewJSAtomicPublishDuplicateError())
	}

	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		for _, retention := range []RetentionPolicy{LimitsPolicy, InterestPolicy, WorkQueuePolicy} {
			replicas := 3
			t.Run(fmt.Sprintf("%s/%s/R%d", storage, retention, replicas), func(t *testing.T) {
				c := createJetStreamClusterExplicit(t, "R3S", 3)
				defer c.shutdown()

				nc, js := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				test(t, nc, js, storage, retention, replicas)
			})
		}
	}
}

func TestJetStreamAtomicBatchPublishSourceAndMirror(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			AllowAtomicPublish: true,
			Replicas:           replicas,
		})
		require_NoError(t, err)

		for seq := uint64(1); seq <= 3; seq++ {
			m := nats.NewMsg("foo")
			m.Header.Set("Nats-Batch-Id", "uuid")
			m.Header.Set("Nats-Batch-Sequence", strconv.FormatUint(seq, 10))
			commit := seq == 3
			if !commit {
				require_NoError(t, nc.PublishMsg(m))
				continue
			}
			m.Header.Set("Nats-Batch-Commit", "1")

			rmsg, err := nc.RequestMsg(m, time.Second)
			require_NoError(t, err)
			var pubAck JSPubAckResponse
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			require_Equal(t, pubAck.Sequence, 3)
			require_Equal(t, pubAck.BatchId, "uuid")
			require_Equal(t, pubAck.BatchSize, 3)
		}

		require_NoError(t, js.DeleteMsg("TEST", 2))
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "M",
			Mirror:   &nats.StreamSource{Name: "TEST"},
			Replicas: replicas,
		})
		require_NoError(t, err)

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "S",
			Sources:  []*nats.StreamSource{{Name: "TEST"}},
			Replicas: replicas,
		})
		require_NoError(t, err)

		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			for _, name := range []string{"M", "S"} {
				if si, err := js.StreamInfo(name); err != nil {
					return err
				} else if si.State.Msgs != 2 {
					return fmt.Errorf("expected 2 messages for stream %q, got %d", name, si.State.Msgs)
				}
			}
			return nil
		})

		// Ensure the batching headers were removed when ingested into the source/mirror.
		rsm, err := js.GetMsg("M", 1)
		require_NoError(t, err)
		require_Len(t, len(rsm.Header), 0)

		rsm, err = js.GetMsg("M", 3)
		require_NoError(t, err)
		require_Len(t, len(rsm.Header), 0)

		rsm, err = js.GetMsg("S", 1)
		require_NoError(t, err)
		require_Len(t, len(rsm.Header), 1)
		require_Equal(t, rsm.Header.Get(JSStreamSource), "TEST 1 > >")

		rsm, err = js.GetMsg("S", 2)
		require_NoError(t, err)
		require_Len(t, len(rsm.Header), 1)
		require_Equal(t, rsm.Header.Get(JSStreamSource), "TEST 3 > >")
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamAtomicBatchPublishEncode(t *testing.T) {
	test := func(t *testing.T, commit bool, compress bool) {
		ts := time.Now().UnixNano()
		hdr := genHeader(nil, "Nats-Batch-Id", "uuid")
		hdr = genHeader(hdr, "Nats-Batch-Sequence", "1")
		msg := []byte("A")
		if compress {
			msg = bytes.Repeat(msg, compressThreshold)
		}
		esm := encodeStreamMsgAllowCompressAndBatch("foo", "reply", hdr, msg, 1, ts, false, "uuid", 1, commit)

		buf, op := esm[1:], entryOp(esm[0])
		if commit {
			require_Equal(t, op, batchCommitMsgOp)
		} else {
			require_Equal(t, op, batchMsgOp)
		}

		batchId, batchSeq, op, mbuf, err := decodeBatchMsg(buf)
		require_NoError(t, err)
		require_Equal(t, batchId, "uuid")
		require_Equal(t, batchSeq, 1)
		if compress {
			require_Equal(t, op, compressedStreamMsgOp)
			mbuf, err = s2.Decode(nil, mbuf)
			require_NoError(t, err)
		} else {
			require_Equal(t, op, streamMsgOp)
		}

		subject, reply, dhdr, dmsg, lseq, dts, sourced, err := decodeStreamMsg(mbuf)
		require_NoError(t, err)
		require_Equal(t, subject, "foo")
		require_Equal(t, reply, "reply")
		require_True(t, bytes.Equal(dhdr, hdr))
		require_True(t, bytes.Equal(dmsg, msg))
		require_Equal(t, lseq, 1)
		require_Equal(t, dts, ts)
		require_False(t, sourced)
	}

	t.Run("normal", func(t *testing.T) { test(t, false, false) })
	t.Run("normal-compress", func(t *testing.T) { test(t, false, true) })
	t.Run("commit", func(t *testing.T) { test(t, true, false) })
	t.Run("commit-compress", func(t *testing.T) { test(t, true, true) })
}

// Test a batch within a single proposal, optionally combined with messages unrelated
// to the batch but within the same proposal.
func TestJetStreamAtomicBatchPublishProposeOne(t *testing.T) {
	test := func(t *testing.T, combined bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			Replicas:           3,
			AllowAtomicPublish: true,
		})
		require_NoError(t, err)

		sl := c.streamLeader(globalAccountName, "TEST")
		mset, err := sl.globalAccount().lookupStream("TEST")
		require_NoError(t, err)

		pubAck, err := js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		var entries []*Entry

		mset.clMu.Lock()
		if combined {
			esm := encodeStreamMsg("foo", _EMPTY_, nil, nil, mset.clseq, 0, false)
			entries = append(entries, newEntry(EntryNormal, esm))
			mset.clseq++
		}

		msg := []byte("hello")
		hdr := genHeader(nil, "Nats-Batch-Id", "uuid")
		hdr = setHeader("Nats-Batch-Sequence", "1", hdr)
		esm := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, "uuid", 1, false)
		entries = append(entries, newEntry(EntryNormal, esm))
		mset.clseq++

		hdr = setHeader("Nats-Batch-Sequence", "2", hdr)
		hdr = setHeader("Nats-Batch-Commit", "1", hdr)
		esm = encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, "uuid", 2, true)
		entries = append(entries, newEntry(EntryNormal, esm))
		mset.clseq++

		if combined {
			esm = encodeStreamMsg("foo", _EMPTY_, nil, nil, mset.clseq, 0, false)
			entries = append(entries, newEntry(EntryNormal, esm))
			mset.clseq++
		}
		mset.clMu.Unlock()
		n := mset.raftNode().(*raft)
		n.sendAppendEntry(entries)

		pubAck, err = js.Publish("foo", nil)
		require_NoError(t, err)
		if combined {
			require_Equal(t, pubAck.Sequence, 6)
		} else {
			require_Equal(t, pubAck.Sequence, 4)
		}
	}

	t.Run("single", func(t *testing.T) { test(t, false) })
	t.Run("combined", func(t *testing.T) { test(t, true) })
}

// Test a batch spanning multiple proposals, optionally combined with messages unrelated
// to the batch but within the same first/last proposal.
func TestJetStreamAtomicBatchPublishProposeMultiple(t *testing.T) {
	test := func(t *testing.T, partial bool, combined bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			Replicas:           3,
			AllowAtomicPublish: true,
		})
		require_NoError(t, err)

		sl := c.streamLeader(globalAccountName, "TEST")
		mset, err := sl.globalAccount().lookupStream("TEST")
		require_NoError(t, err)

		pubAck, err := js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		var entries []*Entry
		mset.clMu.Lock()
		hdr := genHeader(nil, "Nats-Batch-Id", "uuid")
		hdr = genHeader(hdr, "Nats-Batch-Sequence", "1")
		msg := []byte("hello")
		if combined {
			esm := encodeStreamMsg("foo", _EMPTY_, nil, nil, mset.clseq, 0, false)
			entries = append(entries, newEntry(EntryNormal, esm))
			mset.clseq++
		}
		esm := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, "uuid", 1, false)
		entries = append(entries, newEntry(EntryNormal, esm))
		mset.clseq++
		mset.clMu.Unlock()
		n := mset.raftNode().(*raft)
		n.sendAppendEntry(entries)

		mset.clMu.Lock()
		hdr = setHeader("Nats-Batch-Sequence", "2", hdr)
		esm = encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, "uuid", 2, false)
		mset.clseq++
		mset.clMu.Unlock()
		n.sendAppendEntry([]*Entry{newEntry(EntryNormal, esm)})

		entries = nil
		mset.clMu.Lock()
		if !partial {
			hdr = setHeader("Nats-Batch-Sequence", "3", hdr)
			hdr = genHeader(hdr, "Nats-Batch-Commit", "1")
			esm = encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, "uuid", 3, true)
			entries = append(entries, newEntry(EntryNormal, esm))
			mset.clseq++
		}
		if combined {
			esm = encodeStreamMsg("foo", _EMPTY_, nil, nil, mset.clseq, 0, false)
			entries = append(entries, newEntry(EntryNormal, esm))
			mset.clseq++
		}
		mset.clMu.Unlock()
		n.sendAppendEntry(entries)

		pubAck, err = js.Publish("foo", nil)
		require_NoError(t, err)
		expectedSeq := uint64(2)
		if !partial {
			expectedSeq += 3
		}
		if combined {
			expectedSeq += 2
		}
		require_Equal(t, pubAck.Sequence, expectedSeq)
	}

	t.Run("partial", func(t *testing.T) { test(t, true, false) })
	t.Run("partial-combined", func(t *testing.T) { test(t, true, true) })
	t.Run("full", func(t *testing.T) { test(t, false, false) })
	t.Run("full-combined", func(t *testing.T) { test(t, false, true) })
}

// Test a batch that was only partially proposed.
// This should not happen, but guard against it anyhow.
func TestJetStreamAtomicBatchPublishProposeOnePartialBatch(t *testing.T) {
	maxEntries := 3
	for i := range maxEntries + 1 {
		t.Run(fmt.Sprintf("I-%d", i), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:               "TEST",
				Subjects:           []string{"foo"},
				Storage:            FileStorage,
				Replicas:           3,
				AllowAtomicPublish: true,
			})
			require_NoError(t, err)

			sl := c.streamLeader(globalAccountName, "TEST")
			mset, err := sl.globalAccount().lookupStream("TEST")
			require_NoError(t, err)

			pubAck, err := js.Publish("foo", nil)
			require_NoError(t, err)
			require_Equal(t, pubAck.Sequence, 1)

			var entries []*Entry
			mset.clMu.Lock()
			msg := []byte("hello")
			hdr := genHeader(nil, "Nats-Batch-Id", "uuid")
			for j := range 3 {
				// Skip if indices equal.
				if i == j {
					continue
				}
				bseq := uint64(j + 1)
				hdr = setHeader("Nats-Batch-Sequence", strconv.FormatUint(bseq, 10), hdr)
				commit := bseq == uint64(maxEntries)
				if commit {
					hdr = setHeader("Nats-Batch-Commit", "1", hdr)
				}
				esm := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, "uuid", bseq, commit)
				entries = append(entries, newEntry(EntryNormal, esm))
				mset.clseq++
			}
			mset.clMu.Unlock()
			n := mset.raftNode().(*raft)
			n.sendAppendEntry(entries)

			pubAck, err = js.Publish("foo", nil)
			require_NoError(t, err)
			expectedSeq := uint64(2)
			if i >= maxEntries {
				expectedSeq += uint64(maxEntries)
			}
			require_Equal(t, pubAck.Sequence, expectedSeq)
		})
	}
}

// Test multiple sequential batches, the first batch is partially proposed.
// This should not happen, but guard against it anyhow.
func TestJetStreamAtomicBatchPublishProposeMultiplePartialBatches(t *testing.T) {
	for i := range 2 {
		batchSize := i + 1
		t.Run(fmt.Sprintf("B-%d", batchSize), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:               "TEST",
				Subjects:           []string{"foo"},
				Storage:            FileStorage,
				Replicas:           3,
				AllowAtomicPublish: true,
			})
			require_NoError(t, err)

			sl := c.streamLeader(globalAccountName, "TEST")
			mset, err := sl.globalAccount().lookupStream("TEST")
			require_NoError(t, err)

			pubAck, err := js.Publish("foo", nil)
			require_NoError(t, err)
			require_Equal(t, pubAck.Sequence, 1)

			var entries []*Entry
			mset.clMu.Lock()
			msg := []byte("hello")
			hdr := genHeader(nil, "Nats-Batch-Id", "ID_1")
			hdr = setHeader("Nats-Batch-Sequence", "1", hdr)
			esm := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, "ID_1", 1, false)
			entries = append(entries, newEntry(EntryNormal, esm))
			mset.clseq++

			for j := range batchSize {
				bseq := uint64(j + 1)
				hdr = genHeader(nil, "Nats-Batch-Id", "ID_2")
				hdr = setHeader("Nats-Batch-Sequence", strconv.FormatUint(bseq, 10), hdr)
				commit := bseq == uint64(batchSize)
				if commit {
					hdr = setHeader("Nats-Batch-Commit", "1", hdr)
				}
				esm = encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, "ID_2", bseq, commit)
				entries = append(entries, newEntry(EntryNormal, esm))
				mset.clseq++
			}
			mset.clMu.Unlock()
			n := mset.raftNode().(*raft)
			n.sendAppendEntry(entries)

			pubAck, err = js.Publish("foo", nil)
			require_NoError(t, err)
			require_Equal(t, pubAck.Sequence, uint64(2+batchSize))
		})
	}
}

// Test a continuous flow of batches spanning multiple append entries can still move applied up.
// Also, test a server can become leader if the previous leader left it with a partial batch.
func TestJetStreamAtomicBatchPublishContinuousBatchesStillMoveAppliedUp(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"foo"},
		Storage:            FileStorage,
		Replicas:           3,
		AllowAtomicPublish: true,
	})
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	pubAck, err := js.Publish("foo", nil)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)

	n := mset.raftNode().(*raft)
	index, commit, applied := n.Progress()

	// The first batch spans two append entries, but commits.
	mset.clMu.Lock()
	hdr := genHeader(nil, "Nats-Batch-Id", "ID_1")
	hdr = setHeader("Nats-Batch-Sequence", "1", hdr)
	esm := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, nil, mset.clseq, 0, false, "ID_1", 1, false)
	mset.clseq++
	mset.clMu.Unlock()
	n.sendAppendEntry([]*Entry{newEntry(EntryNormal, esm)})

	var entries []*Entry
	mset.clMu.Lock()
	hdr = genHeader(nil, "Nats-Batch-Id", "ID_1")
	hdr = setHeader("Nats-Batch-Sequence", "2", hdr)
	hdr = setHeader("Nats-Batch-Commit", "1", hdr)
	esm = encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, nil, mset.clseq, 0, false, "ID_1", 2, true)
	mset.clseq++

	// The second batch doesn't commit.
	entries = append(entries, newEntry(EntryNormal, esm))
	hdr = genHeader(nil, "Nats-Batch-Id", "ID_2")
	hdr = setHeader("Nats-Batch-Sequence", "1", hdr)
	esm = encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, nil, mset.clseq, 0, false, "ID_2", 1, false)
	mset.clseq++
	entries = append(entries, newEntry(EntryNormal, esm))
	mset.clMu.Unlock()
	n.sendAppendEntry(entries)

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		n.RLock()
		nindex, ncommit, processed, napplied := n.pindex, n.commit, n.processed, n.applied
		n.RUnlock()
		if nindex == index {
			return errors.New("index not updated")
		} else if ncommit == commit {
			return errors.New("commit not updated")
		} else if napplied == applied {
			return errors.New("applied not updated")
		} else if napplied == ncommit {
			return errors.New("applied should not be able to equal commit yet")
		} else if processed != ncommit {
			return errors.New("must have processed all commits")
		}
		return checkState(t, c, globalAccountName, "TEST")
	})

	// Followers are now stranded with a partial batch, one needs to become leader
	// and have the batch be rejected since it was partially proposed.
	sl.Shutdown()
	c.waitOnStreamLeader(globalAccountName, "TEST")
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	// Confirm the last batch gets rejected, and we are still able to publish with quorum.
	pubAck, err = js.Publish("foo", nil)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 4)

	c.restartServer(sl)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	// Publish again, now with all servers online.
	pubAck, err = js.Publish("foo", nil)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 5)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})
}
