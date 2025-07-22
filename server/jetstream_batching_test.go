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
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

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
		require_Error(t, pubAck.Error, NewJSAtomicPublishUnsupportedHeaderBatchError("Nats-Msg-Id"))
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
		require_Equal(t, rsm.Header.Get(JSStreamSource), "TEST 1 > > foo")

		rsm, err = js.GetMsg("S", 2)
		require_NoError(t, err)
		require_Len(t, len(rsm.Header), 1)
		require_Equal(t, rsm.Header.Get(JSStreamSource), "TEST 3 > > foo")
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamAtomicBatchPublishCleanup(t *testing.T) {
	const (
		Disable = iota
		StepDown
		Delete
	)

	test := func(t *testing.T, mode int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		cfg := &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			AllowAtomicPublish: false,
			Replicas:           3,
		}
		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		sl := c.streamLeader(globalAccountName, "TEST")
		mset, err := sl.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		mset.mu.RLock()
		batches := mset.batches
		mset.mu.RUnlock()
		require_True(t, batches == nil)

		// Enabling doesn't need to populate the batching state.
		cfg.AllowAtomicPublish = true
		_, err = jsStreamUpdate(t, nc, cfg)
		require_NoError(t, err)
		mset.mu.RLock()
		batches = mset.batches
		mset.mu.RUnlock()
		require_True(t, batches == nil)

		// Publish a partial batch that needs to be cleaned up.
		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		require_NoError(t, nc.PublishMsg(m))

		// Publish another batch that commits, state should be cleaned up by default.
		m.Header.Set("Nats-Batch-Id", "commit")
		m.Header.Set("Nats-Batch-Commit", "1")
		_, err = js.PublishMsg(m)
		require_NoError(t, err)

		mset.mu.RLock()
		batches = mset.batches
		mset.mu.RUnlock()
		require_NotNil(t, batches)
		batches.mu.Lock()
		groups := len(batches.group)
		b := batches.group["uuid"]
		batches.mu.Unlock()
		require_Len(t, groups, 1)
		require_NotNil(t, b)
		store := b.store
		require_Equal(t, store.State().Msgs, 1)

		// Should fully clean up the in-progress batch.
		switch mode {
		case Disable:
			cfg.AllowAtomicPublish = false
			_, err = jsStreamUpdate(t, nc, cfg)
			require_NoError(t, err)
		case StepDown:
			require_NoError(t, sl.JetStreamStepdownStream(globalAccountName, "TEST"))
		case Delete:
			require_NoError(t, mset.delete())
		}
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			mset.mu.RLock()
			batches = mset.batches
			mset.mu.RUnlock()
			if batches != nil {
				return fmt.Errorf("expected no batches")
			}
			if msgs := store.State().Msgs; msgs != 0 {
				return fmt.Errorf("expected 0 messages, got %d", msgs)
			}
			return nil
		})
	}

	t.Run("Disable", func(t *testing.T) { test(t, Disable) })
	t.Run("StepDown", func(t *testing.T) { test(t, StepDown) })
	t.Run("Delete", func(t *testing.T) { test(t, Delete) })
}

func TestJetStreamAtomicBatchPublishDenyHeaders(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		cfg := &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			AllowAtomicPublish: true,
			Replicas:           replicas,
		}
		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		// We might support these headers later on, but for now error.
		for key, value := range map[string]string{
			"Nats-Msg-Id":                 "msgId",
			"Nats-Expected-Last-Sequence": "0",
			"Nats-Expected-Last-Msg-Id":   "msgId",
		} {
			t.Run(key, func(t *testing.T) {
				m := nats.NewMsg("foo")
				m.Header.Set("Nats-Batch-Id", "uuid")
				m.Header.Set("Nats-Batch-Sequence", "1")
				m.Header.Set("Nats-Batch-Commit", "1")
				m.Header.Set(key, value)
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSAtomicPublishUnsupportedHeaderBatchError(key))
			})
		}
	}

	t.Run("R3", func(t *testing.T) { test(t, 3) })
}
