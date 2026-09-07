// Copyright 2026 The NATS Authors
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

//go:build !skip_js_tests && !skip_js_cluster_tests

package server

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamClusterAtomicBatchScaleUpWaitingCommit(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()
	cfg := &StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Storage: FileStorage, Replicas: 1, AllowAtomicPublish: true}
	_, err := jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)
	s := c.streamLeader(globalAccountName, "TEST")
	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	pub, _ := jsClientConnect(t, s)
	defer pub.Close()
	m := nats.NewMsg("foo")
	m.Data = []byte("first")
	m.Header.Set(JSBatchId, "handoff")
	m.Header.Set(JSBatchSeq, "1")
	_, err = pub.RequestMsg(m, time.Second)
	require_NoError(t, err)

	// A reader can hold isolation while the commit starts and chooses R1.
	mset.isolateMu.RLock()
	locked := true
	defer func() {
		if locked {
			mset.isolateMu.RUnlock()
		}
	}()
	m.Data = []byte("second")
	m.Header.Set(JSBatchSeq, "2")
	m.Header.Set(JSBatchCommit, "1")
	ack := natsSubSync(t, pub, nats.NewInbox())
	m.Reply = ack.Subject
	require_NoError(t, pub.PublishMsg(m))
	require_NoError(t, pub.Flush())
	// Our read lock prevents the writer from entering. Wait until it queues,
	// so the commit has captured the standalone topology before the update.
	checkFor(t, time.Second, time.Millisecond, func() error {
		if mset.isolateMu.TryRLock() {
			mset.isolateMu.RUnlock()
			return fmt.Errorf("commit has not entered isolation")
		}
		return nil
	})

	cfg.Replicas = 3
	request, err := json.Marshal(cfg)
	require_NoError(t, err)
	updated := natsSubSync(t, nc, nats.NewInbox())
	require_NoError(t, nc.PublishRequest(fmt.Sprintf(JSApiStreamUpdateT, "TEST"), updated.Subject, request))
	response, updateErr := updated.NextMsg(time.Second)
	if updateErr != nil {
		require_Error(t, updateErr, nats.ErrTimeout)
	} else {
		// Without the handoff fence, the update can finish while the commit
		// is waiting. Let the initial state reach the new peers first.
		c.waitOnStreamLeader(globalAccountName, "TEST")
		for _, server := range c.servers {
			c.waitOnStreamCurrent(server, globalAccountName, "TEST")
		}
	}
	mset.isolateMu.RUnlock()
	locked = false
	if response == nil {
		response, err = updated.NextMsg(5 * time.Second)
		require_NoError(t, err)
	}
	var update JSApiStreamUpdateResponse
	require_NoError(t, json.Unmarshal(response.Data, &update))
	require_True(t, update.Error == nil)
	response, err = ack.NextMsg(5 * time.Second)
	require_NoError(t, err)
	var pa JSPubAckResponse
	require_NoError(t, json.Unmarshal(response.Data, &pa))
	require_True(t, pa.Error == nil)
	require_Equal(t, pa.Sequence, 2)
	require_Equal(t, pa.BatchId, "handoff")
	require_Equal(t, pa.BatchSize, 2)

	c.waitOnStreamLeader(globalAccountName, "TEST")
	for _, server := range c.servers {
		checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
			stream, err := server.GlobalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			for seq, data := range []string{"first", "second"} {
				sm, err := stream.store.LoadMsg(uint64(seq+1), nil)
				if err != nil {
					return fmt.Errorf("%s sequence %d: %w", server.Name(), seq+1, err)
				}
				if string(sm.msg) != data {
					return fmt.Errorf("%s sequence %d: %q", server.Name(), seq+1, sm.msg)
				}
			}
			return nil
		})
	}
	// The acknowledged data must remain available after changing leaders.
	c.stepDownStreamLeader(nc, globalAccountName, "TEST", c.randomNonStreamLeader(globalAccountName, "TEST"))
	reader, js := jsClientConnect(t, c.streamLeader(globalAccountName, "TEST"))
	defer reader.Close()
	for seq, data := range []string{"first", "second"} {
		msg, err := js.GetMsg("TEST", uint64(seq+1))
		require_NoError(t, err)
		require_Equal(t, string(msg.Data), data)
	}
}
