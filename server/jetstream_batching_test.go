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
	"math/big"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats.go"
)

func TestJetStreamAtomicBatchPublish(t *testing.T) {
	test := func(
		t *testing.T,
		storage StorageType,
		retention RetentionPolicy,
		replicas int,
	) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

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
	}

	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		for _, retention := range []RetentionPolicy{LimitsPolicy, InterestPolicy, WorkQueuePolicy} {
			for _, replicas := range []int{1, 3} {
				t.Run(fmt.Sprintf("%s/%s/R%d", storage, retention, replicas), func(t *testing.T) {
					test(t, storage, retention, replicas)
				})
			}
		}
	}
}

func TestJetStreamAtomicBatchPublishLimits(t *testing.T) {
	streamMaxBatchInflightPerStream = 1
	streamMaxBatchInflightTotal = 1
	streamMaxBatchSize = 2
	streamMaxBatchTimeout = 500 * time.Millisecond
	defer func() {
		streamMaxBatchInflightPerStream = streamDefaultMaxBatchInflightPerStream
		streamMaxBatchInflightTotal = streamDefaultMaxBatchInflightTotal
		streamMaxBatchSize = streamDefaultMaxBatchSize
		streamMaxBatchTimeout = streamDefaultMaxBatchTimeout
	}()

	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc := clientConnectToServer(t, c.randomServer())
		defer nc.Close()

		var pubAck JSPubAckResponse

		cfg := &StreamConfig{
			Name:               "FOO",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			Retention:          LimitsPolicy,
			Replicas:           replicas,
			AllowAtomicPublish: true,
		}
		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		// For testing total server-wide maximum inflight batches.
		cfg = &StreamConfig{
			Name:               "BAR",
			Subjects:           []string{"bar"},
			Storage:            FileStorage,
			Retention:          LimitsPolicy,
			Replicas:           replicas,
			AllowAtomicPublish: true,
		}
		_, err = jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		// A batch ID must not exceed the maximum length.
		for _, length := range []int{64, 65} {
			longBatchId := strings.Repeat("A", length)
			m := nats.NewMsg("foo")
			m.Header.Set("Nats-Batch-Id", longBatchId)
			m.Header.Set("Nats-Batch-Sequence", "1")
			m.Header.Set("Nats-Batch-Commit", "1")
			rmsg, err := nc.RequestMsg(m, time.Second)
			require_NoError(t, err)
			pubAck = JSPubAckResponse{}
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			if length <= 64 {
				require_True(t, pubAck.Error == nil)
			} else {
				require_NotNil(t, pubAck.Error)
				require_Error(t, pubAck.Error, NewJSAtomicPublishInvalidBatchIDError())
			}
		}

		// One batch is inflight.
		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		require_NoError(t, nc.PublishMsg(m))

		// Another batch moves over the threshold and batch is denied.
		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "exceeds_threshold")
		m.Header.Set("Nats-Batch-Sequence", "1")
		m.Header.Set("Nats-Batch-Commit", "1")
		rmsg, err := nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSAtomicPublishIncompleteBatchError())

		// Another batch on a different stream moves over the server-wide threshold and batch is denied.
		m = nats.NewMsg("bar")
		m.Header.Set("Nats-Batch-Id", "bar")
		m.Header.Set("Nats-Batch-Sequence", "1")
		m.Header.Set("Nats-Batch-Commit", "1")
		rmsg, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSAtomicPublishIncompleteBatchError())

		// The first batch should now time out.
		sl := c.streamLeader(globalAccountName, "FOO")
		mset, err := sl.globalAccount().lookupStream("FOO")
		require_NoError(t, err)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			mset.mu.RLock()
			batches := mset.batches
			mset.mu.RUnlock()
			if batches == nil {
				return errors.New("batches not found")
			}
			batches.mu.Lock()
			groups := len(batches.group)
			batches.mu.Unlock()
			if groups != 0 {
				return fmt.Errorf("expected 0 groups, got %d", groups)
			}
			return nil
		})

		// Publishing to the batch should also error since it timed out.
		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "2")
		m.Header.Set("Nats-Batch-Commit", "1")
		rmsg, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSAtomicPublishIncompleteBatchError())

		// Batch should be rejected if going over the max batch size.
		for _, size := range []int{streamMaxBatchSize, streamMaxBatchSize + 1} {
			for i := range size {
				seq := i + 1
				m = nats.NewMsg("foo")
				m.Header.Set("Nats-Batch-Id", "exceeds_threshold")
				m.Header.Set("Nats-Batch-Sequence", strconv.Itoa(seq))
				commit := seq == size
				if !commit {
					require_NoError(t, nc.PublishMsg(m))
					continue
				}

				m.Header.Set("Nats-Batch-Commit", "1")
				rmsg, err = nc.RequestMsg(m, time.Second)
				require_NoError(t, err)
				pubAck = JSPubAckResponse{}
				require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
				if size <= streamMaxBatchSize {
					require_True(t, pubAck.Error == nil)
					require_Equal(t, pubAck.BatchSize, size)
				} else {
					require_Error(t, pubAck.Error, NewJSAtomicPublishIncompleteBatchError())
				}
			}
		}
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
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
			Replicas:           replicas,
			Storage:            storage,
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
			for _, replicas := range []int{1, 3} {
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

		// Mirror can source batched messages but can't do atomic batching itself.
		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:               "M-no-batch",
			Storage:            FileStorage,
			Mirror:             &StreamSource{Name: "TEST"},
			Replicas:           replicas,
			AllowAtomicPublish: true,
		})
		require_Error(t, err, NewJSMirrorWithAtomicPublishError())

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
		Commit
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
		batch := mset.batchApply
		mset.mu.RUnlock()
		require_True(t, batches == nil)
		require_True(t, batch == nil)

		// Enabling doesn't need to populate the batching state.
		cfg.AllowAtomicPublish = true
		_, err = jsStreamUpdate(t, nc, cfg)
		require_NoError(t, err)
		mset.mu.RLock()
		batches = mset.batches
		batch = mset.batchApply
		mset.mu.RUnlock()
		require_True(t, batches == nil)
		require_True(t, batch == nil)

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
		batch = mset.batchApply
		mset.mu.RUnlock()
		require_NotNil(t, batches)
		require_NotNil(t, batch)
		batches.mu.Lock()
		groups := len(batches.group)
		b := batches.group["uuid"]
		batches.mu.Unlock()
		require_Len(t, groups, 1)
		require_NotNil(t, b)
		store := b.store
		require_Equal(t, store.State().Msgs, 1)
		clfs := mset.getCLFS()

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
		case Commit:
			m = nats.NewMsg("foo")
			m.Header.Set("Nats-Batch-Id", "uuid")
			m.Header.Set("Nats-Batch-Sequence", "2")
			m.Header.Set("Nats-Batch-Commit", "1")
			_, err = js.PublishMsg(m)
			require_NoError(t, err)
		}
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			mset.mu.RLock()
			batches = mset.batches
			mset.mu.RUnlock()
			if batches != nil {
				if mode != Commit {
					return fmt.Errorf("expected no batches")
				}
				batches.mu.Lock()
				groups = len(batches.group)
				batches.mu.Unlock()
				if groups > 0 {
					return fmt.Errorf("expected 0 groups, got %d", groups)
				}
			}
			if msgs := store.State().Msgs; msgs != 0 {
				return fmt.Errorf("expected 0 messages, got %d", msgs)
			}
			return nil
		})
		// Should clean up the batch apply state.
		if mode == Disable || mode == Delete {
			mset.mu.RLock()
			batch = mset.batchApply
			mset.mu.RUnlock()
			nclfs := mset.getCLFS()
			require_True(t, batch == nil)
			require_Equal(t, clfs, nclfs)
		}
	}

	t.Run("Disable", func(t *testing.T) { test(t, Disable) })
	t.Run("StepDown", func(t *testing.T) { test(t, StepDown) })
	t.Run("Delete", func(t *testing.T) { test(t, Delete) })
	t.Run("Commit", func(t *testing.T) { test(t, Commit) })
}

func TestJetStreamAtomicBatchPublishConfigOpts(t *testing.T) {
	// Defaults.
	require_Equal(t, streamMaxBatchInflightPerStream, 50)
	require_Equal(t, streamMaxBatchInflightTotal, 1000)
	require_Equal(t, streamMaxBatchSize, 1000)
	require_Equal(t, streamMaxBatchTimeout, 10*time.Second)

	batchingConf := `
	listen: 127.0.0.1:-1
	jetstream {
		store_dir: %q
		limits {
			batch {
				max_inflight_per_stream: %d
				max_inflight_total: %d
				max_msgs: %d
				timeout: %s
			}
		}
	}`

	cf := createConfFile(t, []byte(fmt.Sprintf(batchingConf, t.TempDir(), 10, 20, 100, "5s")))
	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	opts := s.getOpts()
	require_Equal(t, opts.JetStreamLimits.MaxBatchInflightPerStream, 10)
	require_Equal(t, opts.JetStreamLimits.MaxBatchInflightTotal, 20)
	require_Equal(t, opts.JetStreamLimits.MaxBatchSize, 100)
	require_Equal(t, opts.JetStreamLimits.MaxBatchTimeout, 5*time.Second)

	// Reloading is not supported, that would potentially mean dropping random batches when lowering limits.
	changeCurrentConfigContentWithNewContent(t, cf, []byte(fmt.Sprintf(batchingConf, t.TempDir(), 20, 40, 200, "10s")))
	require_Error(t, s.Reload(), fmt.Errorf("config reload not supported for JetStreamLimits"))
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
			"Nats-Msg-Id":               "msgId",
			"Nats-Expected-Last-Msg-Id": "msgId",
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

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamAtomicBatchPublishStageAndCommit(t *testing.T) {
	type BatchItem struct {
		subject string
		header  nats.Header
		msg     []byte
		err     error
	}

	type BatchTest struct {
		title             string
		allowRollup       bool
		denyPurge         bool
		allowTTL          bool
		allowMsgCounter   bool
		allowMsgSchedules bool
		discardNew        bool
		discardNewPerSubj bool
		init              func(mset *stream)
		batch             []BatchItem
		validate          func(mset *stream, commit bool)
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
			title:             "msg-schedules-disabled",
			allowMsgSchedules: false,
			batch: []BatchItem{
				{
					subject: "foo",
					header:  nats.Header{JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"}},
					err:     NewJSMessageSchedulesDisabledError(),
				},
				{
					subject: "foo",
					header:  nats.Header{JSSchedulePattern: {"disabled"}},
					err:     NewJSMessageSchedulesDisabledError(),
				},
			},
		},
		{
			title:             "msg-schedules-ttl-disabled",
			allowMsgSchedules: true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTTL:     {"1s"},
					},
					err: errMsgTTLDisabled,
				},
			},
		},
		{
			title:             "msg-schedules-ttl-invalid",
			allowMsgSchedules: true,
			allowTTL:          true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTTL:     {"invalid"},
					},
					err: NewJSMessageSchedulesTTLInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-invalid-schedule",
			allowMsgSchedules: true,
			batch: []BatchItem{
				{
					subject: "foo",
					header:  nats.Header{JSSchedulePattern: {"invalid"}},
					err:     NewJSMessageSchedulesPatternInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-target-mismatch",
			allowMsgSchedules: true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTarget:  {"not.matching"},
					},
					err: NewJSMessageSchedulesTargetInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-target-must-be-literal",
			allowMsgSchedules: true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTarget:  {"foo.*"},
					},
					err: NewJSMessageSchedulesTargetInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-target-must-be-unique",
			allowMsgSchedules: true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTarget:  {"foo"},
					},
					err: NewJSMessageSchedulesTargetInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-rollup-disabled",
			allowMsgSchedules: true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTarget:  {"bar"},
						JSMsgRollup:       {JSMsgRollupSubject},
					},
					err: errors.New("rollup not permitted"),
				},
			},
		},
		{
			title:             "msg-schedules",
			allowMsgSchedules: true,
			allowRollup:       true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTarget:  {"baz"},
					},
				},
				{
					subject: "bar",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTarget:  {"baz"},
						JSMsgRollup:       {JSMsgRollupSubject},
					},
				},
			},
		},
		{
			title:      "discard-new",
			discardNew: true,
			batch: []BatchItem{
				{subject: "foo1"},
				{subject: "foo2"},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.inflight), 0)
				} else {
					require_Len(t, len(mset.inflight), 2)
					require_Equal(t, *mset.inflight["foo1"], inflightSubjectRunningTotal{bytes: 20, ops: 1})
					require_Equal(t, *mset.inflight["foo2"], inflightSubjectRunningTotal{bytes: 20, ops: 1})
				}
			},
		},
		{
			title:      "discard-new-max-msgs",
			discardNew: true,
			batch: []BatchItem{
				{subject: "foo"},
				{subject: "foo"},
				{subject: "foo", err: ErrMaxMsgs},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.inflight), 0)
				} else {
					require_Len(t, len(mset.inflight), 1)
					require_Equal(t, *mset.inflight["foo"], inflightSubjectRunningTotal{bytes: 19 * 3, ops: 3})
				}
			},
		},
		{
			title:      "discard-new-max-bytes",
			discardNew: true,
			batch: []BatchItem{
				{subject: "foo1", msg: bytes.Repeat([]byte("A"), 10)},
				{subject: "foo2", msg: bytes.Repeat([]byte("A"), 11), err: ErrMaxBytes},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.inflight), 0)
				} else {
					require_Len(t, len(mset.inflight), 2)
					require_Equal(t, *mset.inflight["foo1"], inflightSubjectRunningTotal{bytes: 30, ops: 1})
					require_Equal(t, *mset.inflight["foo2"], inflightSubjectRunningTotal{bytes: 31, ops: 1})
				}
			},
		},
		{
			title:             "discard-new-max-msgs-per-subj",
			discardNew:        true,
			discardNewPerSubj: true,
			batch: []BatchItem{
				{subject: "foo"},
				{subject: "foo", err: ErrMaxMsgsPerSubject},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.inflight), 0)
				} else {
					require_Len(t, len(mset.inflight), 1)
					require_Equal(t, *mset.inflight["foo"], inflightSubjectRunningTotal{bytes: 19 * 2, ops: 2})
				}
			},
		},
		{
			title: "expect-last-seq",
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSExpectedLastSeq: {"0"}}},
				{subject: "bar", header: nats.Header{JSExpectedLastSeq: {"1"}}},
			},
		},
		{
			title: "expect-last-seq-invalid-first",
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSExpectedLastSeq: {"1"}}, err: errors.New("last sequence mismatch: 1 vs 0")},
			},
		},
		{
			title: "expect-last-seq-invalid",
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSExpectedLastSeq: {"0"}}},
				{subject: "bar", header: nats.Header{JSExpectedLastSeq: {"0"}}, err: errors.New("last sequence mismatch: 0 vs 1")},
			},
		},
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
			title: "expect-per-subj-redundant-in-batch",
			batch: []BatchItem{
				{
					subject: "foo",
					header:  nats.Header{JSExpectedLastSubjSeq: {"0"}},
				},
				// Would normally fail the batch, recognize in-process.
				{
					subject: "foo",
					header:  nats.Header{JSExpectedLastSubjSeq: {"1"}},
					err:     errors.New("last sequence by subject mismatch: 1 vs 0"),
				},
				// Redundant expected check results in an error. The subject 'foo' is also updated in the batch.
				{
					subject: "foo",
					header:  nats.Header{JSExpectedLastSubjSeq: {"0"}},
					err:     errors.New("last sequence by subject mismatch"),
				},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.expectedPerSubjectSequence), 0)
					require_Len(t, len(mset.expectedPerSubjectInProcess), 0)
				} else {
					require_Len(t, len(mset.expectedPerSubjectSequence), 1)
					require_Len(t, len(mset.expectedPerSubjectInProcess), 1)
					require_Equal(t, mset.expectedPerSubjectSequence[0], "foo")
				}
			},
		},
		{
			title: "expect-per-subj-dupe-in-change",
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
			title: "expect-per-subj-not-first",
			batch: []BatchItem{
				{subject: "foo"},
				// Mismatch because only the first 'foo' can have the expected check.
				{
					subject: "foo",
					header:  nats.Header{JSExpectedLastSubjSeq: {"0"}},
					err:     errors.New("last sequence by subject mismatch"),
				},
				// Mismatch because only the first 'foo' can have the expected check.
				{
					subject: "bar",
					header:  nats.Header{JSExpectedLastSubjSeq: {"0"}, JSExpectedLastSubjSeqSubj: {"foo"}},
					err:     errors.New("last sequence by subject mismatch"),
				},
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
		{
			title: "expect-per-subj-inflight",
			init: func(mset *stream) {
				mset.inflight = map[string]*inflightSubjectRunningTotal{"bar": {bytes: 33, ops: 1}}
			},
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSExpectedLastSubjSeq: {"10"}, JSExpectedLastSubjSeqSubj: {"bar"}}, err: errors.New("last sequence by subject mismatch")},
			},
			validate: func(mset *stream, commit bool) {
				require_Len(t, len(mset.expectedPerSubjectSequence), 0)
				require_Len(t, len(mset.expectedPerSubjectInProcess), 0)
				require_Len(t, len(mset.inflight), 1)
			},
		},
		{
			title:       "rollup-deny-purge",
			allowRollup: true,
			denyPurge:   true,
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgRollup: {JSMsgRollupSubject}}, err: errors.New("rollup not permitted")},
			},
		},
		{
			title:       "rollup-invalid",
			allowRollup: true,
			denyPurge:   false,
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgRollup: {"invalid"}}, err: fmt.Errorf("rollup value invalid: %q", "invalid")},
			},
		},
		{
			title:       "rollup-all-first",
			allowRollup: true,
			denyPurge:   false,
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgRollup: {JSMsgRollupAll}}},
			},
		},
		{
			title:       "rollup-all-not-first",
			allowRollup: true,
			denyPurge:   false,
			batch: []BatchItem{
				{subject: "foo"},
				{subject: "bar", header: nats.Header{JSMsgRollup: {JSMsgRollupAll}}, err: errors.New("batch rollup all invalid")},
			},
		},
		{
			title:       "rollup-sub-unique",
			allowRollup: true,
			denyPurge:   false,
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgRollup: {JSMsgRollupSubject}}},
				{subject: "bar", header: nats.Header{JSMsgRollup: {JSMsgRollupSubject}}},
			},
		},
		{
			title:       "rollup-sub-overlap",
			allowRollup: true,
			denyPurge:   false,
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgRollup: {JSMsgRollupSubject}}},
				{subject: "foo", header: nats.Header{JSMsgRollup: {JSMsgRollupSubject}}, err: errors.New("batch rollup sub invalid")},
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
				Subjects:           []string{"foo", "bar", "baz"},
			})
			require_NoError(t, err)

			mset, err := s.globalAccount().lookupStream("TEST")
			require_NoError(t, err)

			if test.init != nil {
				test.init(mset)
			}

			var (
				discard       DiscardPolicy
				discardNewPer bool
				maxMsgs       int64 = -1
				maxMsgsPer    int64 = -1
				maxBytes      int64 = -1
			)
			if test.discardNew {
				discard, maxMsgs, maxBytes = DiscardNew, 2, 60
			}
			if test.discardNewPerSubj {
				require_True(t, test.discardNew)
				discardNewPer, maxMsgs, maxMsgsPer = true, -1, 1
			}

			diff := &batchStagedDiff{}
			mset.clMu.Lock()
			defer mset.clMu.Unlock()
			for _, m := range test.batch {
				var hdr []byte
				for key, values := range m.header {
					for _, value := range values {
						hdr = genHeader(hdr, key, value)
					}
				}
				_, _, _, _, err = checkMsgHeadersPreClusteredProposal(diff, mset, m.subject, hdr, m.msg, false, "TEST", nil, test.allowRollup, test.denyPurge, test.allowTTL, test.allowMsgCounter, test.allowMsgSchedules, discard, discardNewPer, -1, maxMsgs, maxMsgsPer, maxBytes)
				if m.err != nil {
					require_Error(t, err, m.err)
				} else if err != nil {
					require_NoError(t, err)
				}
				if test.validate != nil {
					test.validate(mset, false)
				}
				mset.clseq++
			}
			if test.validate != nil {
				diff.commit(mset)
				test.validate(mset, true)
			}
		})
	}
}

func TestJetStreamAtomicBatchPublishHighLevelRollback(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Storage:   FileStorage,
		Retention: InterestPolicy,
		MaxMsgs:   1,
	})
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	requireEmpty := func() {
		mset.ddMu.Lock()
		ddarr, ddmap := len(mset.ddarr), len(mset.ddmap)
		mset.ddMu.Unlock()
		require_Len(t, ddarr, 0)
		require_Len(t, ddmap, 0)

		mset.clMu.Lock()
		inflight, subjSeq, subjInProcess := len(mset.inflight), len(mset.expectedPerSubjectSequence), len(mset.expectedPerSubjectInProcess)
		mset.clMu.Unlock()
		require_Len(t, inflight, 0)
		require_Len(t, subjSeq, 0)
		require_Len(t, subjInProcess, 0)
	}
	requireEmpty()

	m := nats.NewMsg("foo")
	m.Header.Set("Nats-Msg-Id", "dedupe")
	m.Header.Set("Nats-Expected-Last-Subject-Sequence", "1")
	_, err = js.PublishMsg(m)
	require_Error(t, err, NewJSStreamWrongLastSequenceError(0))
	requireEmpty()
}

func TestJetStreamAtomicBatchPublishExpectedPerSubject(t *testing.T) {
	type TestKind int
	const (
		OnlyFirst TestKind = iota
		Redundant
		NotFirst
	)

	test := func(t *testing.T, kind TestKind) {
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

		_, err = js.Publish("foo", nil)
		require_NoError(t, err)

		m := nats.NewMsg("foo")
		if kind != NotFirst {
			m.Header.Set("Nats-Expected-Last-Subject-Sequence", "1")
		}
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		require_NoError(t, nc.PublishMsg(m))

		var pubAck JSPubAckResponse

		// Redundant expected headers are okay, as long as they reflect the state prior to the batch.
		m = nats.NewMsg("foo")
		if kind == Redundant || kind == NotFirst {
			m.Header.Set("Nats-Expected-Last-Subject-Sequence", "1")
		}
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "2")
		m.Header.Set("Nats-Batch-Commit", "1")
		rmsg, err := nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		if kind == Redundant || kind == NotFirst {
			require_NotNil(t, pubAck.Error)
			require_Error(t, pubAck.Error, NewJSStreamWrongLastSequenceConstantError())
			return
		}
		require_True(t, pubAck.Error == nil)
		require_Equal(t, pubAck.Sequence, 3)
		require_Equal(t, pubAck.BatchSize, 2)

		// The first message still contains the expected headers.
		rsm, err := js.GetMsg("TEST", 2)
		require_NoError(t, err)
		require_Equal(t, rsm.Header.Get("Nats-Batch-Id"), "uuid")
		require_Equal(t, rsm.Header.Get("Nats-Batch-Sequence"), "1")
		require_Equal(t, rsm.Header.Get("Nats-Expected-Last-Subject-Sequence"), "1")

		// The second message doesn't have the expected headers, as the condition was already checked
		// and seems inconsistent when getting the message afterward.
		rsm, err = js.GetMsg("TEST", 3)
		require_NoError(t, err)
		require_Equal(t, rsm.Header.Get("Nats-Batch-Id"), "uuid")
		require_Equal(t, rsm.Header.Get("Nats-Batch-Sequence"), "2")
		require_Equal(t, rsm.Header.Get("Nats-Expected-Last-Subject-Sequence"), _EMPTY_)
	}

	t.Run("single", func(t *testing.T) { test(t, OnlyFirst) })
	t.Run("redundant", func(t *testing.T) { test(t, Redundant) })
	t.Run("not-first", func(t *testing.T) { test(t, NotFirst) })
}

func TestJetStreamAtomicBatchPublishSingleServerRecovery(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	cfg := &StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"foo"},
		Storage:            FileStorage,
		Retention:          LimitsPolicy,
		Replicas:           1,
		AllowAtomicPublish: true,
	}
	_, err := jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)

	// Manually construct and store a batch, so it doesn't immediately commit.
	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	mset.mu.Lock()
	batches := &batching{}
	mset.batches = batches
	mset.mu.Unlock()
	batches.mu.Lock()
	b, err := batches.newBatchGroup(mset, "uuid")
	if err != nil {
		batches.mu.Unlock()
		require_NoError(t, err)
	}
	hdr1 := genHeader(nil, "Nats-Batch-Id", "uuid")
	hdr1 = genHeader(hdr1, "Nats-Batch-Sequence", "1")
	_, _, err = b.store.StoreMsg("foo", hdr1, nil, 0)
	if err != nil {
		batches.mu.Unlock()
		require_NoError(t, err)
	}

	hdr2 := genHeader(nil, "Nats-Batch-Id", "uuid")
	hdr2 = genHeader(hdr2, "Nats-Batch-Sequence", "2")
	hdr2 = genHeader(hdr2, "Nats-Batch-Commit", "1")
	_, _, err = b.store.StoreMsg("foo", hdr2, nil, 0)
	if err != nil {
		batches.mu.Unlock()
		require_NoError(t, err)
	}
	commitReady := b.readyForCommit()
	batches.mu.Unlock()
	require_True(t, commitReady)

	// Simulate the first message of the batch is committed.
	err = mset.processJetStreamMsg("foo", _EMPTY_, hdr1, nil, 0, 0, nil, false, true)
	require_NoError(t, err)

	// Simulate a hard kill, upon recovery the rest of the batch should be applied.
	port := s.opts.Port
	sd := s.StoreDir()
	nc.Close()
	s.Shutdown()
	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()

	mset, err = s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	state := mset.state()
	require_Equal(t, state.Msgs, 2)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 2)
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

func TestJetStreamRollupIsolatedRead(t *testing.T) {
	const (
		DirectGet = iota
		DirectBatchGet
		DirectMultiGet
		Consumer
	)

	test := func(t *testing.T, mode int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:        "TEST",
			Subjects:    []string{"foo.*"},
			AllowRollup: true,
			Replicas:    3,
		})
		require_NoError(t, err)

		rs := c.randomNonStreamLeader(globalAccountName, "TEST")
		mset, err := rs.globalAccount().lookupStream("TEST")
		require_NoError(t, err)

		// Reconnect to the selected server.
		nc.Close()
		nc, js = jsClientConnect(t, rs)
		defer nc.Close()

		m := nats.NewMsg("foo.0")
		pubAck, err := js.PublishMsg(m)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		// mset.processJetStreamMsg will first store the new message, then update dedupe state, and then do the rollup.
		// We block the replica such that it can store the new message but can't do the rollup yet.
		// A read should wait for the rollup to complete.
		mset.ddMu.Lock()
		m.Subject = "foo.1"
		m.Header.Set(JSMsgRollup, JSMsgRollupAll)
		m.Header.Set(JSMsgId, "msgId")
		_, _ = js.PublishMsg(m)
		err = checkForErr(2*time.Second, 200*time.Millisecond, func() error {
			var state StreamState
			mset.store.FastState(&state)
			if state.LastSeq != 2 {
				return fmt.Errorf("expected last seq 2, got: %d", state.LastSeq)
			}
			return nil
		})
		if err != nil {
			mset.ddMu.Unlock()
			require_NoError(t, err)
		}

		// We're now subscribing and going to do a request, while the write is still halfway.
		sub, err := nc.SubscribeSync("reply")
		if err != nil {
			mset.ddMu.Unlock()
			require_NoError(t, err)
		}
		defer sub.Drain()
		if err = nc.Flush(); err != nil {
			mset.ddMu.Unlock()
			require_NoError(t, err)
		}

		// Run two goroutines, one will unblock the first write after a short sleep,
		// the second will do the read/consumer request.
		var (
			ready   sync.WaitGroup
			run     sync.WaitGroup
			cleanup sync.WaitGroup
		)
		ready.Add(2)
		run.Add(1)
		cleanup.Add(1)
		go func() {
			ready.Done()
			defer cleanup.Done()
			run.Wait()
			time.Sleep(250 * time.Millisecond)
			mset.ddMu.Unlock()
		}()
		go func() {
			ready.Done()
			run.Wait()
			switch mode {
			case DirectGet:
				mset.getDirectRequest(&JSApiMsgGetRequest{LastFor: "foo.0"}, "reply")
			case DirectBatchGet:
				mset.getDirectRequest(&JSApiMsgGetRequest{NextFor: "foo.*", Batch: 2}, "reply")
			case DirectMultiGet:
				mset.getDirectRequest(&JSApiMsgGetRequest{MultiLastFor: []string{"foo.*"}, Batch: 2}, "reply")
			case Consumer:
				mset.addConsumer(&ConsumerConfig{DeliverSubject: "reply", Direct: true})
			}
		}()

		// Wait for both to be ready, then run both of them.
		ready.Wait()
		run.Done()

		msg, err := sub.NextMsg(time.Second)
		// Make sure cleanup has happened before validating.
		cleanup.Wait()
		require_NoError(t, err)
		if mode == DirectGet {
			require_Equal(t, msg.Header.Get("Status"), "404")
		} else if mode != Consumer {
			require_Equal(t, msg.Header.Get(JSNumPending), "0")
			require_Equal(t, msg.Header.Get(JSSequence), "2")
		} else {
			// $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>
			require_True(t, strings.HasPrefix(msg.Reply, jsAckPre))
			tokens := strings.Split(msg.Reply, ".")
			require_Len(t, len(tokens), 9)
			require_Equal(t, tokens[8], "0") // pending
			require_Equal(t, tokens[5], "2") // sseq
		}
	}

	t.Run("DirectGet", func(t *testing.T) { test(t, DirectGet) })
	t.Run("DirectBatchGet", func(t *testing.T) { test(t, DirectBatchGet) })
	t.Run("DirectMultiGet", func(t *testing.T) { test(t, DirectMultiGet) })
	t.Run("Consumer", func(t *testing.T) { test(t, Consumer) })
}

func TestJetStreamAtomicBatchPublishAdvisories(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc := clientConnectToServer(t, c.randomServer())
		defer nc.Close()

		cfg := &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Replicas:           replicas,
			Storage:            FileStorage,
			AllowAtomicPublish: true,
		}

		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		sub, err := nc.SubscribeSync(fmt.Sprintf("%s.*", JSAdvisoryStreamBatchAbandonedPre))
		require_NoError(t, err)
		defer sub.Drain()
		require_NoError(t, nc.Flush())

		// Should receive an advisory if gaps are detected.
		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		require_NoError(t, nc.PublishMsg(m))
		m.Header.Set("Nats-Batch-Sequence", "3")
		require_NoError(t, nc.PublishMsg(m))

		msg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		var advisory JSStreamBatchAbandonedAdvisory
		require_NoError(t, json.Unmarshal(msg.Data, &advisory))
		require_Equal(t, advisory.BatchId, "uuid")
		require_Equal(t, advisory.Reason, BatchIncomplete)

		count := 1002
		for seq := 1; seq <= count; seq++ {
			m = nats.NewMsg("foo")
			m.Header.Set("Nats-Batch-Id", "uuid")
			m.Header.Set("Nats-Batch-Sequence", strconv.Itoa(seq))
			if seq != count {
				require_NoError(t, nc.PublishMsg(m))
				continue
			}
			var pubAck JSPubAckResponse
			m.Header.Set("Nats-Batch-Commit", "1")
			rmsg, err := nc.RequestMsg(m, time.Second)
			require_NoError(t, err)
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			require_NotNil(t, pubAck.Error)
			require_Error(t, pubAck.Error, NewJSAtomicPublishTooLargeBatchError())
		}

		msg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		advisory = JSStreamBatchAbandonedAdvisory{}
		require_NoError(t, json.Unmarshal(msg.Data, &advisory))
		require_Equal(t, advisory.BatchId, "uuid")
		require_Equal(t, advisory.Reason, BatchTimeout)
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			test(t, replicas)
		})
	}
}
