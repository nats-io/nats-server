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
	"testing"
	"time"

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
			replicas := 3
			t.Run(fmt.Sprintf("%s/%s/R%d", storage, retention, replicas), func(t *testing.T) {
				test(t, storage, retention, replicas)
			})
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
				_, _, _, _, err = checkMsgHeadersPreClusteredProposal(diff, mset, m.subject, hdr, m.msg, false, "TEST", nil, test.allowRollup, test.denyPurge, test.allowTTL, test.allowMsgCounter, discard, discardNewPer, -1, maxMsgs, maxMsgsPer, maxBytes)
				if m.err != nil {
					require_Error(t, err, m.err)
				} else {
					require_True(t, err == nil)
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
