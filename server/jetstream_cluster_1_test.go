// Copyright 2020-2025 The NATS Authors
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
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
)

func TestJetStreamClusterConfig(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 16GB, max_file_store: 10TB, store_dir: '%s'}
		cluster { listen: 127.0.0.1:-1 }
	`))

	check := func(errStr string) {
		t.Helper()
		opts, err := ProcessConfigFile(conf)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := NewServer(opts); err == nil || !strings.Contains(err.Error(), errStr) {
			t.Fatalf("Expected an error of `%s`, got `%v`", errStr, err)
		}
	}

	check("requires `server_name`")

	conf = createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: "TEST"
		jetstream: {max_mem_store: 16GB, max_file_store: 10TB, store_dir: '%s'}
		cluster { listen: 127.0.0.1:-1 }
	`))

	check("requires `cluster.name`")
}

func TestJetStreamClusterLeader(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	// Kill our current leader and force an election.
	c.leader().Shutdown()
	c.waitOnLeader()

	// Now killing our current leader should leave us leaderless.
	c.leader().Shutdown()
	c.expectNoLeader()
}

func TestJetStreamClusterExpand(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 2)
	defer c.shutdown()

	c.addInNewServer()
	c.waitOnPeerCount(3)
}

func TestJetStreamClusterAccountInfo(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc := clientConnectToServer(t, c.randomServer())
	defer nc.Close()

	reply := nats.NewInbox()
	sub, _ := nc.SubscribeSync(reply)

	if err := nc.PublishRequest(JSApiAccountInfo, reply, nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, sub, 1)
	resp, _ := sub.NextMsg(0)

	var info JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.JetStreamAccountStats == nil || info.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", info.Error)
	}
	// Make sure we only got 1 response.
	// Technically this will always work since its a singelton service export.
	if nmsgs, _, _ := sub.Pending(); nmsgs > 0 {
		t.Fatalf("Expected only a single response, got %d more", nmsgs)
	}
}

func TestJetStreamClusterStreamLimitWithAccountDefaults(t *testing.T) {
	// 2MB memory, 8MB disk
	c := createJetStreamClusterWithTemplate(t, jsClusterLimitsTempl, "R3L", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 2,
		MaxBytes: 4 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST2",
		Replicas: 2,
		MaxBytes: 15 * 1024 * 1024,
	})
	require_Contains(t, err.Error(), "no suitable peers for placement", "insufficient storage")
}

func TestJetStreamClusterInfoRaftGroup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R1S", 3)
	defer c.shutdown()

	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	acc := s.GlobalAccount()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Storage:  nats.FileStorage,
		Replicas: 3,
	})
	require_NoError(t, err)

	nfoResp, err := nc.Request("$JS.API.STREAM.INFO.TEST", nil, time.Second)
	require_NoError(t, err)

	var si StreamInfo
	err = json.Unmarshal(nfoResp.Data, &si)
	require_NoError(t, err)

	if si.Cluster == nil {
		t.Fatalf("Expected cluster info, got none")
	}

	stream, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	if si.Cluster.RaftGroup != stream.raftGroup().Name {
		t.Fatalf("Expected raft group %q to equal %q", si.Cluster.RaftGroup, stream.raftGroup().Name)
	}

	var sscfg StreamConfig
	rCfgData, err := os.ReadFile(filepath.Join(s.opts.StoreDir, "jetstream", "$SYS", "_js_", stream.raftGroup().Name, "meta.inf"))
	require_NoError(t, err)
	err = json.Unmarshal(rCfgData, &sscfg)
	require_NoError(t, err)
	if !reflect.DeepEqual(sscfg.Metadata, map[string]string{"account": "$G", "stream": "TEST", "type": "stream"}) {
		t.Fatalf("Invalid raft stream metadata: %v", sscfg.Metadata)
	}

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "DURABLE", Replicas: 3})
	require_NoError(t, err)

	consumer := stream.lookupConsumer("DURABLE")

	var ci ConsumerInfo
	nfoResp, err = nc.Request("$JS.API.CONSUMER.INFO.TEST.DURABLE", nil, time.Second)
	require_NoError(t, err)

	var cscfg ConsumerConfig
	rCfgData, err = os.ReadFile(filepath.Join(s.opts.StoreDir, "jetstream", "$SYS", "_js_", consumer.raftGroup().Name, "meta.inf"))
	require_NoError(t, err)
	err = json.Unmarshal(rCfgData, &cscfg)
	require_NoError(t, err)
	if !reflect.DeepEqual(cscfg.Metadata, map[string]string{"account": "$G", "consumer": "DURABLE", "stream": "TEST", "type": "consumer"}) {
		t.Fatalf("Invalid raft stream metadata: %v", cscfg.Metadata)
	}

	err = json.Unmarshal(nfoResp.Data, &ci)
	require_NoError(t, err)

	if ci.Cluster.RaftGroup != consumer.raftGroup().Name {
		t.Fatalf("Expected raft group %q to equal %q", ci.Cluster.RaftGroup, consumer.raftGroup().Name)
	}
}

func TestJetStreamClusterSingleReplicaStreams(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R1S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected si to have cluster info")
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	// Now grab info for this stream.
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}
	// Now create a consumer. This should be pinned to same server that our stream was allocated to.
	// First do a normal sub.
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, toSend)

	// Now create a consumer as well.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci == nil || ci.Name != "dlc" || ci.Stream != "TEST" {
		t.Fatalf("ConsumerInfo is not correct %+v", ci)
	}

	// Now make sure that if we kill and restart the server that this stream and consumer return.
	sl := c.streamLeader("$G", "TEST")
	sl.Shutdown()
	c.restartServer(sl)

	c.waitOnStreamLeader("$G", "TEST")
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	// Now durable consumer.
	c.waitOnConsumerLeader("$G", "TEST", "dlc")
	if _, err = js.ConsumerInfo("TEST", "dlc"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterMultiReplicaStreams(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "RNS", 5)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Now grab info for this stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}
	// Now create a consumer. This should be affinitize to the same set of servers as the stream.
	// First do a normal sub.
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, toSend)

	// Now create a consumer as well.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci == nil || ci.Name != "dlc" || ci.Stream != "TEST" || ci.NumPending != uint64(toSend) {
		t.Fatalf("ConsumerInfo is not correct %+v", ci)
	}
}

func TestJetStreamClusterMultiReplicaStreamsDefaultFileMem(t *testing.T) {
	const testConfig = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}
`
	c := createJetStreamClusterWithTemplate(t, testConfig, "RNS", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
		MaxBytes: 1024,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Now grab info for this stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}
	// Now create a consumer. This should be affinitize to the same set of servers as the stream.
	// First do a normal sub.
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, toSend)

	// Now create a consumer as well.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci == nil || ci.Name != "dlc" || ci.Stream != "TEST" || ci.NumPending != uint64(toSend) {
		t.Fatalf("ConsumerInfo is not correct %+v", ci)
	}
}

func TestJetStreamClusterMemoryStore(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3M", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
		Storage:  nats.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send in 100 messages.
	msg, toSend := []byte("Hello MemoryStore"), 100
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	// Now grab info for this stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	if si.Cluster == nil || len(si.Cluster.Replicas) != 2 {
		t.Fatalf("Cluster info is incorrect: %+v", si.Cluster)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}
	// Do a normal sub.
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, toSend)
}

func TestJetStreamClusterDelete(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "RNS", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "C22",
		Subjects: []string{"foo", "bar", "baz"},
		Replicas: 2,
		Storage:  nats.FileStorage,
		MaxMsgs:  100,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	// Now create a consumer.
	if _, err := js.AddConsumer("C22", &nats.ConsumerConfig{
		Durable:   "dlc",
		AckPolicy: nats.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("Error adding consumer: %v", err)
	}

	// Now delete the consumer.
	if err := js.DeleteConsumer("C22", "dlc"); err != nil {
		t.Fatalf("Error deleting consumer: %v", err)
	}

	// Now delete the stream.
	if err := js.DeleteStream("C22"); err != nil {
		t.Fatalf("Error deleting stream: %v", err)
	}

	// This will get the current information about usage and limits for this account.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		info, err := js.AccountInfo()
		if err != nil {
			return err
		}
		if info.Streams != 0 {
			return fmt.Errorf("Expected no remaining streams, got %d", info.Streams)
		}
		return nil
	})
}

func TestJetStreamClusterStreamPurge(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msg, toSend := []byte("Hello JS Clustering"), 100
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Now grab info for this stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}

	// Now purge the stream.
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if si.State.Msgs != 0 || si.State.FirstSeq != uint64(toSend+1) {
		t.Fatalf("Expected no msgs, got: %+v", si.State)
	}
}

func TestJetStreamClusterStreamUpdateSubjects(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	}

	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we can update subjects.
	cfg.Subjects = []string{"bar", "baz"}

	si, err := js.UpdateStream(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil {
		t.Fatalf("Expected a stream info, got none")
	}
	if !reflect.DeepEqual(si.Config.Subjects, cfg.Subjects) {
		t.Fatalf("Expected subjects to be updated: got %+v", si.Config.Subjects)
	}
	// Make sure it registered
	js2, err := nc.JetStream(nats.MaxWait(250 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js2.Publish("foo", nil); err == nil {
		t.Fatalf("Expected this to fail")
	}
	if _, err = js2.Publish("baz", nil); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
}

func TestJetStreamClusterBadStreamUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	}

	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	msg, toSend := []byte("Keep Me"), 50
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Make sure a bad update will not remove our stream.
	cfg.Subjects = []string{"foo..bar"}
	if _, err := js.UpdateStream(cfg); err == nil || err == nats.ErrTimeout {
		t.Fatalf("Expected error but got none or timeout")
	}

	// Make sure we did not delete our original stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(si.Config.Subjects, []string{"foo", "bar"}) {
		t.Fatalf("Expected subjects to be original ones, got %+v", si.Config.Subjects)
	}
}

func TestJetStreamClusterConsumerRedeliveredInfo(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{Name: "TEST"}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.Publish("TEST", []byte("CI")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	sub, _ := nc.SubscribeSync("R")
	sub.AutoUnsubscribe(2)

	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverSubject: "R",
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, 2)
	sub.Unsubscribe()

	ci, err = js.ConsumerInfo("TEST", ci.Name)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.NumRedelivered != 1 {
		t.Fatalf("Expected 1 redelivered, got %d", ci.NumRedelivered)
	}
}

func TestJetStreamClusterConsumerState(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 5)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Make sure we are not connected to any of the stream servers so that we do not do client reconnect
	// when we take out the consumer leader.
	if s.JetStreamIsStreamAssigned("$G", "TEST") {
		nc.Close()
		for _, ns := range c.servers {
			if !ns.JetStreamIsStreamAssigned("$G", "TEST") {
				s = ns
				nc, js = jsClientConnect(t, s)
				defer nc.Close()
				break
			}
		}
	}

	sub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Pull 5 messages and ack.
	for _, m := range fetchMsgs(t, sub, 5, 5*time.Second) {
		m.AckSync()
	}

	// Let state propagate for exact comparison below.
	time.Sleep(200 * time.Millisecond)

	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}
	if ci.AckFloor.Consumer != 5 {
		t.Fatalf("Expected ack floor of %d, got %d", 5, ci.AckFloor.Consumer)
	}

	c.consumerLeader("$G", "TEST", "dlc").Shutdown()
	c.waitOnConsumerLeader("$G", "TEST", "dlc")

	nci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}
	// nil out timestamp for better comparison
	nci.Delivered.Last, ci.Delivered.Last = nil, nil
	if nci.Delivered != ci.Delivered {
		t.Fatalf("Consumer delivered did not match after leader switch, wanted %+v, got %+v", ci.Delivered, nci.Delivered)
	}
	nci.AckFloor.Last, ci.AckFloor.Last = nil, nil
	if nci.AckFloor != ci.AckFloor {
		t.Fatalf("Consumer ackfloor did not match after leader switch, wanted %+v, got %+v", ci.AckFloor, nci.AckFloor)
	}

	// Now make sure we can receive new messages.
	// Pull last 5.
	for _, m := range fetchMsgs(t, sub, 5, 5*time.Second) {
		m.AckSync()
	}

	nci, _ = sub.ConsumerInfo()
	if nci.Delivered.Consumer != 10 || nci.Delivered.Stream != 10 {
		t.Fatalf("Received bad delivered: %+v", nci.Delivered)
	}
	if nci.AckFloor.Consumer != 10 || nci.AckFloor.Stream != 10 {
		t.Fatalf("Received bad ackfloor: %+v", nci.AckFloor)
	}
	if nci.NumAckPending != 0 {
		t.Fatalf("Received bad ackpending: %+v", nci.NumAckPending)
	}
}

func TestJetStreamClusterFullConsumerState(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fetchMsgs(t, sub, 1, 5*time.Second)

	// Now purge the stream.
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
}

func TestJetStreamClusterMetaSnapshotsAndCatchup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Shut one down.
	rs := c.randomServer()
	rs.Shutdown()

	c.waitOnLeader()
	s := c.leader()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	numStreams := 4
	// Create 4 streams
	// FIXME(dlc) - R2 make sure we place properly.
	for i := 0; i < numStreams; i++ {
		sn := fmt.Sprintf("T-%d", i+1)
		_, err := js.AddStream(&nats.StreamConfig{Name: sn})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	c.leader().JetStreamSnapshotMeta()

	rs = c.restartServer(rs)
	c.checkClusterFormed()
	c.waitOnServerCurrent(rs)

	rs.Shutdown()
	c.waitOnLeader()

	for i := 0; i < numStreams; i++ {
		sn := fmt.Sprintf("T-%d", i+1)
		err := js.DeleteStream(sn)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	rs = c.restartServer(rs)
	c.checkClusterFormed()
	c.waitOnServerCurrent(rs)
}

func TestJetStreamClusterMetaSnapshotsMultiChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 2)
	defer c.shutdown()

	s := c.leader()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Add in 2 streams with 1 consumer each.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "S1"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.waitOnStreamLeader(globalAccountName, "S1")
	_, err := js.AddConsumer("S1", &nats.ConsumerConfig{Durable: "S1C1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.waitOnConsumerLeader(globalAccountName, "S1", "S1C1")

	if _, err = js.AddStream(&nats.StreamConfig{Name: "S2"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.waitOnStreamLeader(globalAccountName, "S2")
	_, err = js.AddConsumer("S2", &nats.ConsumerConfig{Durable: "S2C1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.waitOnConsumerLeader(globalAccountName, "S2", "S2C1")

	// Add in a new server to the group. This way we know we can delete the original streams and consumers.
	rs := c.addInNewServer()
	c.waitOnServerCurrent(rs)
	rsn := rs.Name()

	// Shut it down.
	rs.Shutdown()

	// Wait for the peer to be removed.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		for _, p := range s.JetStreamClusterPeers() {
			if p == rsn {
				return fmt.Errorf("Old server still in peer set")
			}
		}
		return nil
	})

	// We want to make changes here that test each delta scenario for the meta snapshots.
	// Add new stream and consumer.
	if _, err = js.AddStream(&nats.StreamConfig{Name: "S3"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.waitOnStreamLeader(globalAccountName, "S3")
	_, err = js.AddConsumer("S3", &nats.ConsumerConfig{Durable: "S3C1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.waitOnConsumerLeader(globalAccountName, "S3", "S3C1")
	// Delete stream S2
	resp, _ := nc.Request(fmt.Sprintf(JSApiStreamDeleteT, "S2"), nil, time.Second)
	var dResp JSApiStreamDeleteResponse
	if err := json.Unmarshal(resp.Data, &dResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !dResp.Success || dResp.Error != nil {
		t.Fatalf("Got a bad response %+v", dResp.Error)
	}
	// Delete the consumer on S1 but add another.
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerDeleteT, "S1", "S1C1"), nil, time.Second)
	require_NoError(t, err)
	require_NotNil(t, resp)
	var cdResp JSApiConsumerDeleteResponse
	if err = json.Unmarshal(resp.Data, &cdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !cdResp.Success || cdResp.Error != nil {
		t.Fatalf("Got a bad response %+v", cdResp)
	}
	// Add new consumer on S1
	_, err = js.AddConsumer("S1", &nats.ConsumerConfig{Durable: "S1C2", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.waitOnConsumerLeader(globalAccountName, "S1", "S1C2")

	cl := c.leader()
	cl.JetStreamSnapshotMeta()
	c.waitOnServerCurrent(cl)

	rs = c.restartServer(rs)
	c.checkClusterFormed()
	c.waitOnServerCurrent(rs)
}

func TestJetStreamClusterStreamSynchedTimeStamps(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Storage: nats.MemoryStorage, Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js.Publish("foo", []byte("TSS")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	// Grab the message and timestamp from our current leader
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	m, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	meta, _ := m.Metadata()

	sub.Unsubscribe()

	sl := c.streamLeader("$G", "foo")

	sl.Shutdown()

	c.waitOnLeader()
	c.waitOnStreamLeader("$G", "foo")

	nc, js = jsClientConnect(t, c.leader())
	defer nc.Close()

	sm, err := js.GetMsg("foo", 1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !sm.Time.Equal(meta.Timestamp) {
		t.Fatalf("Expected same timestamps, got %v vs %v", sm.Time, meta.Timestamp)
	}
}

// Test to mimic what R.I. was seeing.
func TestJetStreamClusterRestoreSingleConsumer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js.Publish("foo", []byte("TSS")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if m, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	} else {
		m.AckSync()
	}

	c.stopAll()
	c.restartAll()
	c.waitOnLeader()
	c.waitOnStreamLeader("$G", "foo")

	s = c.randomServer()
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	var names []string
	for name := range js.StreamNames() {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(names))
	}

	// Now do detailed version.
	var infos []*nats.StreamInfo
	for info := range js.StreamsInfo() {
		infos = append(infos, info)
	}
	if len(infos) != 1 {
		t.Fatalf("Expected 1 stream but got %d", len(infos))
	}
	si, err := js.StreamInfo("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "foo" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}

	// Now check for consumer.
	names = names[:0]
	for name := range js.ConsumerNames("foo") {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected 1 consumer but got %d", len(names))
	}
}

func TestJetStreamClusterMaxBytesForStream(t *testing.T) {
	// Has max_file_store of 2GB
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	info, err := js.AccountInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Make sure we still are dynamic.
	if info.Limits.MaxStore != -1 || info.Limits.MaxMemory != -1 {
		t.Fatalf("Expected dynamic limits for the account, got %+v\n", info.Limits)
	}
	// Stream config.
	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Replicas: 2,
		MaxBytes: 2 * 1024 * 1024 * 1024, // 2GB
	}
	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	// Make sure going over the single server limit though is enforced (for now).
	cfg.Name = "TEST2"
	cfg.MaxBytes *= 2
	_, err = js.AddStream(cfg)
	require_Contains(t, err.Error(), "no suitable peers for placement")
}

func TestJetStreamClusterStreamPublishWithActiveConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js.Publish("foo", []byte("TSS")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// FIXME(dlc) - Need to track this down.
	c.waitOnConsumerLeader("$G", "foo", "dlc")

	if m, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	} else {
		m.AckSync()
	}

	// Send 10 messages.
	for i := 1; i <= 10; i++ {
		payload := []byte(fmt.Sprintf("MSG-%d", i))
		if _, err = js.Publish("foo", payload); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	checkSubsPending(t, sub, 10)
	// Sanity check for duplicate deliveries..
	if nmsgs, _, _ := sub.Pending(); nmsgs > 10 {
		t.Fatalf("Expected only %d responses, got %d more", 10, nmsgs)
	}
	for i := 1; i <= 10; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		payload := []byte(fmt.Sprintf("MSG-%d", i))
		if !bytes.Equal(m.Data, payload) {
			t.Fatalf("Did not get expected msg, expected %q, got %q", payload, m.Data)
		}
	}

	var ci *nats.ConsumerInfo
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		ci, err = sub.ConsumerInfo()
		if err != nil {
			return fmt.Errorf("Unexpected error getting consumer info: %v", err)
		}
		if ci.Delivered.Stream != 11 {
			return fmt.Errorf("expected delivered stream sequence to be %d, got %d", 11, ci.Delivered.Stream)
		}
		return nil
	})

	c.consumerLeader("$G", "foo", "dlc").Shutdown()
	c.waitOnConsumerLeader("$G", "foo", "dlc")

	ci2, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}

	ci.Cluster = nil
	ci2.Cluster = nil

	// nil out timestamp for better comparison
	ci.Delivered.Last, ci2.Delivered.Last = nil, nil
	ci.AckFloor.Last, ci2.AckFloor.Last = nil, nil
	if !reflect.DeepEqual(ci, ci2) {
		t.Fatalf("Consumer info did not match: %+v vs %+v", ci, ci2)
	}

	// In case the server above was also stream leader.
	c.waitOnStreamLeader("$G", "foo")

	// Now send more..
	// Send 10 more messages.
	for i := 11; i <= 20; i++ {
		payload := []byte(fmt.Sprintf("MSG-%d", i))
		if _, err = js.Publish("foo", payload); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	checkSubsPending(t, sub, 10)
	// Sanity check for duplicate deliveries..
	if nmsgs, _, _ := sub.Pending(); nmsgs > 10 {
		t.Fatalf("Expected only %d responses, got %d more", 10, nmsgs)
	}

	for i := 11; i <= 20; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		payload := []byte(fmt.Sprintf("MSG-%d", i))
		if !bytes.Equal(m.Data, payload) {
			t.Fatalf("Did not get expected msg, expected %q, got %q", payload, m.Data)
		}
	}
}

func TestJetStreamClusterStreamOverlapSubjects(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST2", Subjects: []string{"foo"}}); err == nil || err == nats.ErrTimeout {
		t.Fatalf("Expected error but got none or timeout: %v", err)
	}

	// Now grab list of streams and make sure the second is not there.
	var names []string
	for name := range js.StreamNames() {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(names))
	}

	// Now do a detailed version.
	var infos []*nats.StreamInfo
	for info := range js.StreamsInfo() {
		infos = append(infos, info)
	}
	if len(infos) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(infos))
	}
}

func TestJetStreamClusterStreamInfoList(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	createStream := func(name string) {
		t.Helper()
		if _, err := js.AddStream(&nats.StreamConfig{Name: name}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	createStream("foo")
	createStream("bar")
	createStream("baz")

	sendBatch := func(subject string, n int) {
		t.Helper()
		// Send a batch to a given subject.
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	sendBatch("foo", 10)
	sendBatch("bar", 22)
	sendBatch("baz", 33)

	// Now get the stream list info.
	var infos []*nats.StreamInfo
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		infos = infos[:0]
		for info := range js.StreamsInfo() {
			infos = append(infos, info)
		}
		if len(infos) != 3 {
			return fmt.Errorf("StreamInfo expected 3 results, got %d", len(infos))
		}
		return nil
	})

	for _, si := range infos {
		switch si.Config.Name {
		case "foo":
			if si.State.Msgs != 10 {
				t.Fatalf("Expected %d msgs but got %d", 10, si.State.Msgs)
			}
		case "bar":
			if si.State.Msgs != 22 {
				t.Fatalf("Expected %d msgs but got %d", 22, si.State.Msgs)
			}
		case "baz":
			if si.State.Msgs != 33 {
				t.Fatalf("Expected %d msgs but got %d", 33, si.State.Msgs)
			}
		}
	}
}

func TestJetStreamClusterConsumerInfoList(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Place messages so we can generate consumer state.
	for i := 0; i < 10; i++ {
		if _, err := js.Publish("TEST", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	createConsumer := func(name string) *nats.Subscription {
		t.Helper()
		sub, err := js.PullSubscribe("TEST", name)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return sub
	}

	subFoo := createConsumer("foo")
	subBar := createConsumer("bar")
	subBaz := createConsumer("baz")

	// Place consumers in various states.
	for _, ss := range []struct {
		sub   *nats.Subscription
		fetch int
		ack   int
	}{
		{subFoo, 4, 2},
		{subBar, 2, 0},
		{subBaz, 8, 6},
	} {
		msgs := fetchMsgs(t, ss.sub, ss.fetch, 5*time.Second)
		for i := 0; i < ss.ack; i++ {
			msgs[i].AckSync()
		}
	}

	// Now get the consumer list info.
	var infos []*nats.ConsumerInfo
	for info := range js.ConsumersInfo("TEST") {
		infos = append(infos, info)
	}
	if len(infos) != 3 {
		t.Fatalf("ConsumerInfo expected 3 results, got %d", len(infos))
	}
	for _, ci := range infos {
		switch ci.Name {
		case "foo":
			if ci.Delivered.Consumer != 4 {
				t.Fatalf("Expected %d delivered but got %d", 4, ci.Delivered.Consumer)
			}
			if ci.AckFloor.Consumer != 2 {
				t.Fatalf("Expected %d for ack floor but got %d", 2, ci.AckFloor.Consumer)
			}
		case "bar":
			if ci.Delivered.Consumer != 2 {
				t.Fatalf("Expected %d delivered but got %d", 2, ci.Delivered.Consumer)
			}
			if ci.AckFloor.Consumer != 0 {
				t.Fatalf("Expected %d for ack floor but got %d", 0, ci.AckFloor.Consumer)
			}
		case "baz":
			if ci.Delivered.Consumer != 8 {
				t.Fatalf("Expected %d delivered but got %d", 8, ci.Delivered.Consumer)
			}
			if ci.AckFloor.Consumer != 6 {
				t.Fatalf("Expected %d for ack floor but got %d", 6, ci.AckFloor.Consumer)
			}
		}
	}
}

func TestJetStreamClusterStreamUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sc := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
		MaxMsgs:  10,
		Discard:  DiscardNew,
	}

	if _, err := js.AddStream(sc); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 1; i <= int(sc.MaxMsgs); i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err := js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Expect error here.
	if _, err := js.Publish("foo", []byte("fail")); err == nil {
		t.Fatalf("Expected publish to fail")
	}

	// Now update MaxMsgs, select non-leader
	s = c.randomNonStreamLeader("$G", "TEST")
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	sc.MaxMsgs = 20
	si, err := js.UpdateStream(sc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Config.MaxMsgs != 20 {
		t.Fatalf("Expected to have config updated with max msgs of %d, got %d", 20, si.Config.MaxMsgs)
	}

	// Do one that will fail. Wait and make sure we only are getting one response.
	sc.Name = "TEST22"

	rsub, _ := nc.SubscribeSync(nats.NewInbox())
	defer rsub.Unsubscribe()
	nc.Flush()

	req, _ := json.Marshal(sc)
	if err := nc.PublishRequest(fmt.Sprintf(JSApiStreamUpdateT, "TEST"), rsub.Subject, req); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Wait incase more than one reply sent.
	time.Sleep(250 * time.Millisecond)

	if nmsgs, _, _ := rsub.Pending(); err != nil || nmsgs != 1 {
		t.Fatalf("Expected only one response, got %d", nmsgs)
	}

	m, err := rsub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}

	var scResp JSApiStreamCreateResponse
	if err := json.Unmarshal(m.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo != nil || scResp.Error == nil {
		t.Fatalf("Did not receive correct response: %+v", scResp)
	}
}

func TestJetStreamClusterStreamExtendedUpdates(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	updateStream := func() *nats.StreamInfo {
		si, err := js.UpdateStream(cfg)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return si
	}

	// Subjects can be updated
	cfg.Subjects = []string{"bar", "baz"}
	if si := updateStream(); !reflect.DeepEqual(si.Config.Subjects, cfg.Subjects) {
		t.Fatalf("Did not get expected stream info: %+v", si)
	}
	// Mirror changes are not supported for now
	cfg.Subjects = nil
	cfg.Mirror = &nats.StreamSource{Name: "ORDERS"}
	_, err := js.UpdateStream(cfg)
	require_Error(t, err, NewJSStreamMirrorNotUpdatableError())
}

func TestJetStreamClusterDoubleAdd(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R32", 2)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Streams should allow double add.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check Consumers.
	cfg := &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}
	if _, err := js.AddConsumer("TEST", cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Check double add ok.
	if _, err := js.AddConsumer("TEST", cfg); err != nil {
		t.Fatalf("Expected no error but got: %v", err)
	}
}

func TestJetStreamClusterDefaultMaxAckPending(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R32", 2)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Do Consumers too.
	cfg := &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}
	ci, err := js.AddConsumer("TEST", cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check that we have a default set now for the max ack pending.
	if ci.Config.MaxAckPending != JsDefaultMaxAckPending {
		t.Fatalf("Expected a default for max ack pending of %d, got %d", JsDefaultMaxAckPending, ci.Config.MaxAckPending)
	}
}

func TestJetStreamClusterStreamNormalCatchup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10
	for i := 1; i <= toSend; i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sl := c.streamLeader("$G", "TEST")
	sl.Shutdown()
	c.waitOnStreamLeader("$G", "TEST")

	// Send 10 more while one replica offline.
	for i := toSend; i <= toSend*2; i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Delete the first from the second batch.
	dreq := JSApiMsgDeleteRequest{Seq: uint64(toSend)}
	dreqj, err := json.Marshal(dreq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, _ := nc.Request(fmt.Sprintf(JSApiMsgDeleteT, "TEST"), dreqj, time.Second)
	var delMsgResp JSApiMsgDeleteResponse
	if err = json.Unmarshal(resp.Data, &delMsgResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !delMsgResp.Success || delMsgResp.Error != nil {
		t.Fatalf("Got a bad response %+v", delMsgResp.Error)
	}

	sl = c.restartServer(sl)
	c.checkClusterFormed()

	c.waitOnServerCurrent(sl)
	c.waitOnStreamCurrent(sl, "$G", "TEST")
}

func TestJetStreamClusterStreamSnapshotCatchup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	pseq := uint64(1)
	sendBatch := func(n int) {
		t.Helper()
		// Send a batch.
		for i := 0; i < n; i++ {
			msg := []byte(fmt.Sprintf("HELLO JSC-%d", pseq))
			if _, err = js.Publish("foo", msg); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
			pseq++
		}
	}

	sendBatch(2)

	sl := c.streamLeader("$G", "TEST")
	sl.Shutdown()
	c.waitOnStreamLeader("$G", "TEST")

	sendBatch(100)

	deleteMsg := func(seq uint64) {
		if err := js.DeleteMsg("TEST", seq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Delete the first from the second batch.
	deleteMsg(pseq / 2)
	// Delete the next one too.
	deleteMsg(pseq/2 + 1)

	nsl := c.streamLeader("$G", "TEST")
	nsl.JetStreamSnapshotStream("$G", "TEST")

	// Do some activity post snapshot as well.
	// Delete next to last.
	deleteMsg(pseq - 2)
	// Send another batch.
	sendBatch(100)

	mset, err := nsl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	ostate := mset.stateWithDetail(true)

	sl = c.restartServer(sl)
	c.checkClusterFormed()

	c.waitOnServerCurrent(sl)
	c.waitOnStreamCurrent(sl, "$G", "TEST")

	mset, err = sl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if nstate := mset.stateWithDetail(true); !reflect.DeepEqual(ostate, nstate) {
			return fmt.Errorf("States do not match after recovery: %+v vs %+v", ostate, nstate)
		}
		return nil
	})
}

func TestJetStreamClusterDeleteMsg(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// R=1 make sure delete works.
	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10
	for i := 1; i <= toSend; i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	deleteMsg := func(seq uint64) {
		if err := js.DeleteMsg("TEST", seq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	deleteMsg(1)

	// Also make sure purge of R=1 works too.
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
}

func TestJetStreamClusterDeleteMsgAndRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// R=1 make sure delete works.
	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10
	for i := 1; i <= toSend; i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	deleteMsg := func(seq uint64) {
		if err := js.DeleteMsg("TEST", seq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	deleteMsg(1)

	c.stopAll()
	c.restartAll()

	c.waitOnStreamLeader("$G", "TEST")
}

func TestJetStreamClusterStreamSnapshotCatchupWithPurge(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sl := c.streamLeader("$G", "TEST")

	sl.Shutdown()
	c.waitOnStreamLeader("$G", "TEST")

	toSend := 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	nsl := c.streamLeader("$G", "TEST")
	if err := nsl.JetStreamSnapshotStream("$G", "TEST"); err != nil {
		t.Fatalf("Error snapshotting stream: %v", err)
	}
	time.Sleep(250 * time.Millisecond)

	sl = c.restartServer(sl)
	c.checkClusterFormed()

	// Now purge the stream while we are recovering.
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}

	c.waitOnServerCurrent(sl)
	c.waitOnStreamCurrent(sl, "$G", "TEST")

	nsl.Shutdown()
	c.waitOnStreamLeader("$G", "TEST")

	if _, err := js.StreamInfo("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterExtendedStreamInfo(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 50
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	leader := c.streamLeader("$G", "TEST").Name()

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected cluster info")
	}
	if si.Cluster.Name != c.name {
		t.Fatalf("Expected cluster name of %q, got %q", c.name, si.Cluster.Name)
	}

	if si.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, si.Cluster.Leader)
	}
	if len(si.Cluster.Replicas) != 2 {
		t.Fatalf("Expected %d replicas, got %d", 2, len(si.Cluster.Replicas))
	}

	// Make sure that returned array is ordered
	for i := 0; i < 50; i++ {
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		require_True(t, len(si.Cluster.Replicas) == 2)
		s1 := si.Cluster.Replicas[0].Name
		s2 := si.Cluster.Replicas[1].Name
		if s1 > s2 {
			t.Fatalf("Expected replicas to be ordered, got %s then %s", s1, s2)
		}
	}

	// We may need to wait a bit for peers to catch up.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, "$G", "TEST")
	})

	// Shutdown the leader.
	oldLeader := c.streamLeader("$G", "TEST")
	oldLeader.Shutdown()

	c.waitOnStreamLeader("$G", "TEST")

	// Re-request.
	leader = c.streamLeader("$G", "TEST").Name()
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected cluster info")
	}
	if si.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, si.Cluster.Leader)
	}
	if len(si.Cluster.Replicas) != 2 {
		t.Fatalf("Expected %d replicas, got %d", 2, len(si.Cluster.Replicas))
	}
	for _, peer := range si.Cluster.Replicas {
		if peer.Name == oldLeader.Name() {
			if peer.Current {
				t.Fatalf("Expected old leader to be reported as not current: %+v", peer)
			}
		} else if !peer.Current {
			t.Fatalf("Expected replica to be current: %+v", peer)
		}
	}

	// Now send a few more messages then restart the oldLeader.
	for i := 0; i < 10; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	oldLeader = c.restartServer(oldLeader)
	c.checkClusterFormed()

	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnStreamCurrent(oldLeader, "$G", "TEST")

	// Re-request.
	leader = c.streamLeader("$G", "TEST").Name()
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected cluster info")
	}
	if si.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, si.Cluster.Leader)
	}
	if len(si.Cluster.Replicas) != 2 {
		t.Fatalf("Expected %d replicas, got %d", 2, len(si.Cluster.Replicas))
	}

	// We may need to wait a bit for peers to catch up.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, "$G", "TEST")
	})

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Now do consumer.
	sub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	fetchMsgs(t, sub, 10, 5*time.Second)

	leader = c.consumerLeader("$G", "TEST", "dlc").Name()
	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}

	if ci.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, ci.Cluster.Leader)
	}
	if len(ci.Cluster.Replicas) != 2 {
		t.Fatalf("Expected %d replicas, got %d", 2, len(ci.Cluster.Replicas))
	}
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, "$G", "TEST")
	})
}

func TestJetStreamClusterExtendedStreamInfoSingleReplica(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 50
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	leader := c.streamLeader("$G", "TEST").Name()

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected cluster info")
	}
	if si.Cluster.Name != c.name {
		t.Fatalf("Expected cluster name of %q, got %q", c.name, si.Cluster.Name)
	}
	if si.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, si.Cluster.Leader)
	}
	if len(si.Cluster.Replicas) != 0 {
		t.Fatalf("Expected no replicas but got %d", len(si.Cluster.Replicas))
	}

	// Make sure we can grab consumer lists from any
	var infos []*nats.ConsumerInfo
	for info := range js.ConsumersInfo("TEST") {
		infos = append(infos, info)
	}
	if len(infos) != 0 {
		t.Fatalf("ConsumerInfo expected no paged results, got %d", len(infos))
	}

	// Now add in a consumer.
	cfg := &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}
	if _, err := js.AddConsumer("TEST", cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	infos = infos[:0]
	for info := range js.ConsumersInfo("TEST") {
		infos = append(infos, info)
	}
	if len(infos) != 1 {
		t.Fatalf("ConsumerInfo expected 1 result, got %d", len(infos))
	}

	// Now do direct names list as well.
	var names []string
	for name := range js.ConsumerNames("TEST") {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 consumer but got %d", len(names))
	}
}

func TestJetStreamClusterInterestRetention(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Retention: nats.InterestPolicy, Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sl := c.streamLeader("$G", "foo")
	cl := c.consumerLeader("$G", "foo", "dlc")
	if sl == cl {
		_, err := nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "foo"), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c.waitOnStreamLeader("$G", "foo")
	}

	if _, err = js.Publish("foo", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	m, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error getting msg: %v", err)
	}
	m.AckSync()

	waitForZero := func() {
		checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
			si, err := js.StreamInfo("foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si.State.Msgs != 0 {
				return fmt.Errorf("Expected 0 msgs, got state: %+v", si.State)
			}
			return nil
		})
	}

	waitForZero()

	// Add in 50 messages.
	for i := 0; i < 50; i++ {
		if _, err = js.Publish("foo", []byte("more")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	checkSubsPending(t, sub, 50)

	// Now delete the consumer and make sure the stream goes to zero.
	if err := js.DeleteConsumer("foo", "dlc"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	waitForZero()
}

// https://github.com/nats-io/nats-server/issues/2243
func TestJetStreamClusterWorkQueueRetention(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "FOO",
		Subjects:  []string{"foo.*"},
		Replicas:  2,
		Retention: nats.WorkQueuePolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := js.PullSubscribe("foo.test", "test")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js.Publish("foo.test", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	si, err := js.StreamInfo("FOO")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != 1 {
		t.Fatalf("Expected 1 msg, got state: %+v", si.State)
	}

	// Fetch from our pull consumer and ack.
	for _, m := range fetchMsgs(t, sub, 1, 5*time.Second) {
		m.AckSync()
	}

	// Make sure the messages are removed.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("FOO")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 0 {
			return fmt.Errorf("Expected 0 msgs, got state: %+v", si.State)
		}
		return nil
	})

}

func TestJetStreamClusterMirrorAndSourceWorkQueues(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "WQ", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "WQ22",
		Subjects:  []string{"foo"},
		Replicas:  2,
		Retention: nats.WorkQueuePolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Replicas: 2,
		Mirror:   &nats.StreamSource{Name: "WQ22"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "S",
		Replicas: 2,
		Sources:  []*nats.StreamSource{{Name: "WQ22"}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Allow direct sync consumers to connect.
	time.Sleep(1500 * time.Millisecond)

	if _, err = js.Publish("foo", []byte("ok")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		if si, _ := js.StreamInfo("WQ22"); si.State.Msgs != 0 {
			return fmt.Errorf("Expected no msgs for %q, got %d", "WQ22", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("M"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for %q, got %d", "M", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("S"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for %q, got %d", "S", si.State.Msgs)
		}
		return nil
	})

}

func TestJetStreamClusterMirrorAndSourceInterestPolicyStream(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "WQ", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "IP22",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Retention: nats.InterestPolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Replicas: 2,
		Mirror:   &nats.StreamSource{Name: "IP22"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "S",
		Replicas: 2,
		Sources:  []*nats.StreamSource{{Name: "IP22"}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Allow sync consumers to connect.
	time.Sleep(1500 * time.Millisecond)

	if _, err = js.Publish("foo", []byte("ok")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		// This one will be 0 since no other interest exists.
		if si, _ := js.StreamInfo("IP22"); si.State.Msgs != 0 {
			return fmt.Errorf("Expected no msgs for %q, got %d", "IP22", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("M"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for %q, got %d", "M", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("S"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for %q, got %d", "S", si.State.Msgs)
		}
		return nil
	})

	// Now create other interest on IP22.
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	// Allow consumer state to propagate.
	time.Sleep(500 * time.Millisecond)

	if _, err = js.Publish("foo", []byte("ok")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		// This one will be 0 since no other interest exists.
		if si, _ := js.StreamInfo("IP22"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for %q, got %d", "IP22", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("M"); si.State.Msgs != 2 {
			return fmt.Errorf("Expected 2 msgs for %q, got %d", "M", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("S"); si.State.Msgs != 2 {
			return fmt.Errorf("Expected 2 msgs for %q, got %d", "S", si.State.Msgs)
		}
		return nil
	})
}

func TestJetStreamClusterInterestRetentionWithFilteredConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"*"}, Retention: nats.InterestPolicy, Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fsub, err := js.SubscribeSync("foo", nats.Durable("d1"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fsub.Unsubscribe()

	bsub, err := js.SubscribeSync("bar", nats.Durable("d2"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer bsub.Unsubscribe()

	msg := []byte("FILTERED")
	sendMsg := func(subj string) {
		t.Helper()
		if _, err = js.Publish(subj, msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	getAndAck := func(sub *nats.Subscription) {
		t.Helper()
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error getting msg: %v", err)
		}
		m.AckSync()
	}

	jsq, err := nc.JetStream(nats.MaxWait(250 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkState := func(expected uint64) {
		t.Helper()
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			t.Helper()
			si, err := jsq.StreamInfo("TEST")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si.State.Msgs != expected {
				return fmt.Errorf("Expected %d msgs, got %d", expected, si.State.Msgs)
			}
			return nil
		})
	}

	sendMsg("foo")
	checkState(1)
	getAndAck(fsub)
	checkState(0)
	sendMsg("bar")
	sendMsg("foo")
	checkState(2)
	getAndAck(bsub)
	checkState(1)
	getAndAck(fsub)
	checkState(0)

	// Now send a bunch of messages and then delete the consumer.
	for i := 0; i < 10; i++ {
		sendMsg("foo")
		sendMsg("bar")
	}
	checkState(20)

	if err := js.DeleteConsumer("TEST", "d1"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := js.DeleteConsumer("TEST", "d2"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkState(0)

	// Now make sure pull based consumers work same.
	if _, err := js.PullSubscribe("foo", "dlc"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now send a bunch of messages and then delete the consumer.
	for i := 0; i < 10; i++ {
		sendMsg("foo")
		sendMsg("bar")
	}
	checkState(10)

	if err := js.DeleteConsumer("TEST", "dlc"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkState(0)
}

func TestJetStreamClusterEphemeralConsumerNoImmediateInterest(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// We want to relax the strict interest requirement.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{DeliverSubject: "r"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	cl := c.consumerLeader("$G", "TEST", ci.Name)
	mset, err := cl.GlobalAccount().lookupStream("TEST")
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", "TEST")
	}
	o := mset.lookupConsumer(ci.Name)
	if o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	o.setInActiveDeleteThreshold(500 * time.Millisecond)

	// Make sure the consumer goes away though eventually.
	// Should be 5 seconds wait.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if _, err := js.ConsumerInfo("TEST", ci.Name); err != nil {
			return nil
		}
		return fmt.Errorf("Consumer still present")
	})
}

func TestJetStreamClusterEphemeralConsumerCleanup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 2})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub, err := js.Subscribe("foo", func(m *nats.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ci, _ := sub.ConsumerInfo()
	if ci == nil {
		t.Fatalf("Unexpected error: no consumer info")
	}

	// We will look up by hand this consumer to set inactive threshold lower for this test.
	cl := c.consumerLeader("$G", "foo", ci.Name)
	if cl == nil {
		t.Fatalf("Could not find consumer leader")
	}
	mset, err := cl.GlobalAccount().lookupStream("foo")
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", "foo")
	}
	o := mset.lookupConsumer(ci.Name)
	if o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	o.setInActiveDeleteThreshold(10 * time.Millisecond)

	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	getConsumers := func() []string {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var names []string
		for name := range js.ConsumerNames("foo", nats.Context(ctx)) {
			names = append(names, name)
		}
		return names
	}

	checkConsumer := func(expected int) {
		consumers := getConsumers()
		if len(consumers) != expected {
			t.Fatalf("Expected %d consumers but got %d", expected, len(consumers))
		}
	}

	checkConsumer(1)

	// Now Unsubscribe, since this is ephemeral this will make this go away.
	sub.Unsubscribe()

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if consumers := getConsumers(); len(consumers) == 0 {
			return nil
		} else {
			return fmt.Errorf("Still %d consumers remaining", len(consumers))
		}
	})
}

func TestJetStreamClusterEphemeralConsumersNotReplicated(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ci, _ := sub.ConsumerInfo()
	if ci == nil {
		t.Fatalf("Unexpected error: no consumer info")
	}

	if _, err = js.Publish("foo", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	checkSubsPending(t, sub, 1)
	sub.NextMsg(0)

	if ci.Cluster == nil || len(ci.Cluster.Replicas) != 0 {
		t.Fatalf("Expected ephemeral to be R=1, got %+v", ci.Cluster)
	}
	scl := c.serverByName(ci.Cluster.Leader)
	if scl == nil {
		t.Fatalf("Could not select server where ephemeral consumer is running")
	}

	// Test migrations. If we are also metadata leader will not work so skip.
	if scl == c.leader() {
		return
	}

	scl.Shutdown()
	c.waitOnStreamLeader("$G", "foo")

	if _, err = js.Publish("foo", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	if _, err := sub.NextMsg(500 * time.Millisecond); err != nil {
		t.Logf("Expected to see another message, but behavior is optimistic so can fail")
	}
}

func TestJetStreamClusterUserSnapshotAndRestore(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 200

	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Create consumer with no state.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "rip", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create another consumer as well and give it a non-simplistic state.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy, AckWait: 10 * time.Second})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	jsub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Ack first 50.
	for _, m := range fetchMsgs(t, jsub, 50, 5*time.Second) {
		require_NoError(t, m.AckSync())
	}
	// Now ack every third message for next 50.
	for i, m := range fetchMsgs(t, jsub, 50, 5*time.Second) {
		if i%3 == 0 {
			require_NoError(t, m.AckSync())
		}
	}

	// Snapshot consumer info.
	ci, err := jsub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}

	sreq := &JSApiStreamSnapshotRequest{
		DeliverSubject: nats.NewInbox(),
		ChunkSize:      512,
	}

	req, _ := json.Marshal(sreq)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "TEST"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}

	var resp JSApiStreamSnapshotResponse
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error != nil {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}

	// Grab state for comparison.
	state := *resp.State
	config := *resp.Config

	var snapshot []byte
	done := make(chan bool)

	sub, _ := nc.Subscribe(sreq.DeliverSubject, func(m *nats.Msg) {
		// EOF
		if len(m.Data) == 0 {
			done <- true
			return
		}
		// Could be writing to a file here too.
		snapshot = append(snapshot, m.Data...)
		// Flow ack
		m.Respond(nil)
	})
	defer sub.Unsubscribe()

	// Wait to receive the snapshot.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive our snapshot in time")
	}

	var rresp JSApiStreamRestoreResponse
	rreq := &JSApiStreamRestoreRequest{
		Config: config,
		State:  state,
	}
	req, _ = json.Marshal(rreq)

	// Make sure a restore to an existing stream fails.
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "TEST"), req, 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	json.Unmarshal(rmsg.Data, &rresp)
	if !IsNatsErr(rresp.Error, JSStreamNameExistRestoreFailedErr) {
		t.Fatalf("Did not get correct error response: %+v", rresp.Error)
	}

	if _, err := js.StreamInfo("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now make sure a restore will work.
	// Delete our stream first.
	if err := js.DeleteStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.StreamInfo("TEST"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Expected not found error: %v", err)
	}

	// This should work properly.
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "TEST"), req, 5*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
	}
	if rresp.DeliverSubject == _EMPTY_ {
		t.Fatalf("No deliver subject set on response: %+v", rresp)
	}
	// Send our snapshot back in to restore the stream.
	// Can be any size message.
	var chunk [1024]byte
	for r := bytes.NewReader(snapshot); ; {
		n, err := r.Read(chunk[:])
		if err != nil {
			break
		}
		nc.Request(rresp.DeliverSubject, chunk[:n], time.Second)
	}
	rmsg, err = nc.Request(rresp.DeliverSubject, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
	}

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" || si.State.Msgs != uint64(toSend) {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}

	// Make sure the replicas become current eventually. They will be doing catchup.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, "$G", "TEST")
	})

	// Wait on the system to elect a leader for the restored consumer.
	c.waitOnConsumerLeader("$G", "TEST", "dlc")

	// Now check for the consumer being recreated.
	nci, err := js.ConsumerInfo("TEST", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// nil out timestamp for better comparison
	nci.Delivered.Last, ci.Delivered.Last = nil, nil
	if nci.Delivered != ci.Delivered {
		t.Fatalf("Delivered states do not match %+v vs %+v", nci.Delivered, ci.Delivered)
	}
	nci.AckFloor.Last, ci.AckFloor.Last = nil, nil
	if nci.AckFloor != ci.AckFloor {
		t.Fatalf("Ack floors did not match %+v vs %+v", nci.AckFloor, ci.AckFloor)
	}

	// Make sure consumer works.
	// It should pick up with the next delivery spot, so check for that as first message.
	// We should have all the messages for first delivery delivered.
	wantSeq := 101
	for _, m := range fetchMsgs(t, jsub, 100, 5*time.Second) {
		meta, err := m.Metadata()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if meta.Sequence.Stream != uint64(wantSeq) {
			t.Fatalf("Expected stream sequence of %d, but got %d", wantSeq, meta.Sequence.Stream)
		}
		require_NoError(t, m.AckSync())
		wantSeq++
	}

	// Check that redelivered come in now..
	redelivered := 50/3 + 1
	fetchMsgs(t, jsub, redelivered, 15*time.Second)

	// Now make sure the other server was properly caughtup.
	// Need to call this by hand for now.
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var sdResp JSApiStreamLeaderStepDownResponse
	if err := json.Unmarshal(rmsg.Data, &sdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if sdResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", sdResp.Error)
	}

	c.waitOnStreamLeader("$G", "TEST")
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Unexpected stream info: %+v", si)
	}

	// Check idle consumer
	c.waitOnConsumerLeader("$G", "TEST", "rip")

	// Now check for the consumer being recreated.
	if _, err := js.ConsumerInfo("TEST", "rip"); err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}
}

func TestJetStreamClusterUserSnapshotAndRestoreConfigChanges(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// FIXME(dlc) - Do case with R=1
	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	}

	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	getSnapshot := func() ([]byte, *StreamState) {
		t.Helper()
		sreq := &JSApiStreamSnapshotRequest{
			DeliverSubject: nats.NewInbox(),
			ChunkSize:      1024,
		}

		req, _ := json.Marshal(sreq)
		rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "TEST"), req, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error on snapshot request: %v", err)
		}

		var resp JSApiStreamSnapshotResponse
		json.Unmarshal(rmsg.Data, &resp)
		if resp.Error != nil {
			t.Fatalf("Did not get correct error response: %+v", resp.Error)
		}

		var snapshot []byte
		done := make(chan bool)

		sub, _ := nc.Subscribe(sreq.DeliverSubject, func(m *nats.Msg) {
			// EOF
			if len(m.Data) == 0 {
				done <- true
				return
			}
			// Could be writing to a file here too.
			snapshot = append(snapshot, m.Data...)
			// Flow ack
			m.Respond(nil)
		})
		defer sub.Unsubscribe()

		// Wait to receive the snapshot.
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive our snapshot in time")
		}
		return snapshot, resp.State
	}

	restore := func(cfg *StreamConfig, state *StreamState, snap []byte) *nats.StreamInfo {
		rreq := &JSApiStreamRestoreRequest{
			Config: *cfg,
			State:  *state,
		}
		req, err := json.Marshal(rreq)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamRestoreT, cfg.Name), req, 5*time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var rresp JSApiStreamRestoreResponse
		json.Unmarshal(rmsg.Data, &rresp)
		if rresp.Error != nil {
			t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
		}
		if rresp.DeliverSubject == _EMPTY_ {
			t.Fatalf("No deliver subject set on response: %+v", rresp)
		}
		// Send our snapshot back in to restore the stream.
		// Can be any size message.
		var chunk [1024]byte
		for r := bytes.NewReader(snap); ; {
			n, err := r.Read(chunk[:])
			if err != nil {
				break
			}
			nc.Request(rresp.DeliverSubject, chunk[:n], time.Second)
		}
		rmsg, err = nc.Request(rresp.DeliverSubject, nil, 2*time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		rresp.Error = nil
		json.Unmarshal(rmsg.Data, &rresp)
		if rresp.Error != nil {
			t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
		}
		si, err := js.StreamInfo(cfg.Name)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return si
	}

	snap, state := getSnapshot()

	if err := js.DeleteStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now change subjects.
	ncfg := &StreamConfig{
		Name:     "TEST",
		Subjects: []string{"bar", "baz"},
		Storage:  FileStorage,
		Replicas: 2,
	}
	if si := restore(ncfg, state, snap); !reflect.DeepEqual(si.Config.Subjects, ncfg.Subjects) {
		t.Fatalf("Did not get expected stream info: %+v", si)
	}
	if err := js.DeleteStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Storage
	ncfg.Storage = MemoryStorage
	if si := restore(ncfg, state, snap); !reflect.DeepEqual(si.Config.Subjects, ncfg.Subjects) {
		t.Fatalf("Did not get expected stream info: %+v", si)
	}
	if err := js.DeleteStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Now replicas
	ncfg.Replicas = 3
	if si := restore(ncfg, state, snap); !reflect.DeepEqual(si.Config.Subjects, ncfg.Subjects) {
		t.Fatalf("Did not get expected stream info: %+v", si)
	}
}

func TestJetStreamClusterAccountInfoAndLimits(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	// Adjust our limits.
	c.updateLimits("$G", map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxMemory:    1024,
			MaxStore:     8000,
			MaxStreams:   3,
			MaxConsumers: 2,
		},
	})

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 1}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "bar", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "baz", Replicas: 3}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create with same config is idempotent, and must not exceed max streams as it already exists.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "baz", Replicas: 3}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sendBatch := func(subject string, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("JSC-OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	sendBatch("foo", 25)
	sendBatch("bar", 75)
	sendBatch("baz", 10)

	accountStats := func() *nats.AccountInfo {
		t.Helper()

		info, err := js.AccountInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return info
	}

	// If subject is not 3 letters or payload not 2 this needs to change.
	const msgSize = uint64(22 + 3 + 6 + 8)

	stats := accountStats()
	if stats.Streams != 3 {
		t.Fatalf("Should have been tracking 3 streams, found %d", stats.Streams)
	}
	expectedSize := 25*msgSize + 75*msgSize*2 + 10*msgSize*3
	// This may lag.
	checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
		if stats.Store != expectedSize {
			err := fmt.Errorf("Expected store size to be %d, got %+v\n", expectedSize, stats)
			stats = accountStats()
			return err

		}
		return nil
	})

	// Check limit enforcement.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "fail", Replicas: 3}); err == nil {
		t.Fatalf("Expected an error but got none")
	}

	// We should be at 7995 at the moment with a limit of 8000, so any message will go over.
	if _, err := js.Publish("baz", []byte("JSC-NOT-OK")); err == nil {
		t.Fatalf("Expected publish error but got none")
	}

	// Check consumers
	_, err := js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create (with explicit create API) for the same consumer must be idempotent, and not trigger limit.
	obsReq := CreateConsumerRequest{
		Stream: "foo",
		Config: ConsumerConfig{Durable: "bar"},
		Action: ActionCreate,
	}
	req, err := json.Marshal(obsReq)
	require_NoError(t, err)

	msg, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "foo", "bar"), req, time.Second)
	require_NoError(t, err)
	var resp JSApiConsumerInfoResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %v", resp.Error)
	}

	msg, err = nc.Request(fmt.Sprintf(JSApiDurableCreateT, "foo", "bar"), req, time.Second)
	require_NoError(t, err)
	var resp2 JSApiConsumerInfoResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp2))
	if resp2.Error != nil {
		t.Fatalf("Unexpected error: %v", resp2.Error)
	}

	// This should fail.
	_, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc22", AckPolicy: nats.AckExplicitPolicy})
	if err == nil {
		t.Fatalf("Expected error but got none")
	}
}

func TestJetStreamClusterMaxStreamsReached(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomNonLeader())
	defer nc.Close()

	// Adjust our limits.
	c.updateLimits("$G", map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxMemory:  1024,
			MaxStore:   1024,
			MaxStreams: 1,
		},
	})

	// Many stream creations in parallel for the same stream should not result in
	// maximum number of streams reached error. All should have a successful response.
	var wg sync.WaitGroup
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Replicas: 1,
			})
			require_NoError(t, err)
		}()
	}
	wg.Wait()
	require_NoError(t, js.DeleteStream("TEST"))

	// Adjust our limits.
	c.updateLimits("$G", map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxMemory:  1024,
			MaxStore:   1024,
			MaxStreams: 2,
		},
	})

	// Setup streams beforehand.
	for d := 0; d < 2; d++ {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     fmt.Sprintf("TEST-%d", d),
			Subjects: []string{fmt.Sprintf("foo.%d", d)},
			Replicas: 1,
		})
		require_NoError(t, err)
	}

	// Many stream creations in parallel for streams that already exist should not result in
	// maximum number of streams reached error. All should have a successful response.
	for i := 0; i < 15; i++ {
		wg.Add(1)
		d := i % 2
		go func() {
			defer wg.Done()
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     fmt.Sprintf("TEST-%d", d),
				Subjects: []string{fmt.Sprintf("foo.%d", d)},
				Replicas: 1,
			})
			require_NoError(t, err)
		}()
	}
	wg.Wait()
}

func TestJetStreamClusterStreamLimits(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Check that large R will fail.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 5}); err == nil {
		t.Fatalf("Expected error but got none")
	}

	maxMsgs := 5

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       "foo",
		Replicas:   3,
		Retention:  nats.LimitsPolicy,
		Discard:    DiscardNew,
		MaxMsgSize: 11,
		MaxMsgs:    int64(maxMsgs),
		MaxAge:     250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Large message should fail.
	if _, err := js.Publish("foo", []byte("0123456789ZZZ")); err == nil {
		t.Fatalf("Expected publish to fail")
	}

	for i := 0; i < maxMsgs; i++ {
		if _, err := js.Publish("foo", []byte("JSC-OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// These should fail.
	if _, err := js.Publish("foo", []byte("JSC-OK")); err == nil {
		t.Fatalf("Expected publish to fail")
	}

	// Make sure when space frees up we can send more.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("foo")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 0 {
			return fmt.Errorf("Expected 0 msgs, got state: %+v", si.State)
		}
		return nil
	})

	if _, err := js.Publish("foo", []byte("ROUND2")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterStreamInterestOnlyPolicy(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "foo",
		Replicas:  3,
		Retention: nats.InterestPolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10

	// With no interest these should be no-ops.
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("foo", []byte("JSC-OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	si, err := js.StreamInfo("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != 0 {
		t.Fatalf("Expected no messages with no interest, got %d", si.State.Msgs)
	}

	// Now create a consumer.
	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("foo", []byte("JSC-OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	checkSubsPending(t, sub, toSend)

	si, err = js.StreamInfo("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages with interest, got %d", toSend, si.State.Msgs)
	}
	if si.State.FirstSeq != uint64(toSend+1) {
		t.Fatalf("Expected first sequence of %d, got %d", toSend+1, si.State.FirstSeq)
	}

	// Now delete the consumer.
	sub.Unsubscribe()
	// That should make it go away.
	if _, err := js.ConsumerInfo("foo", "dlc"); err == nil {
		t.Fatalf("Expected not found error, got none")
	}

	// Wait for the messages to be purged.
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		si, err := js.StreamInfo("foo")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs == 0 {
			return nil
		}
		return fmt.Errorf("Wanted 0 messages, got %d", si.State.Msgs)
	})
}

// These are disabled for now.
// Deprecated: stream templates are deprecated and will be removed in a future version.
func TestJetStreamClusterStreamTemplates(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	// List API
	var tListResp JSApiStreamTemplateNamesResponse
	resp, err := nc.Request(JSApiTemplates, nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := json.Unmarshal(resp.Data, &tListResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if tListResp.Error == nil {
		t.Fatalf("Expected an unsupported error, got none")
	}
	if !strings.Contains(tListResp.Error.Description, "not currently supported in clustered mode") {
		t.Fatalf("Did not get correct error response: %+v", tListResp.Error)
	}

	// Create
	// Now do templates.
	mcfg := &StreamConfig{
		Subjects: []string{"kv.*"},
		Storage:  MemoryStorage,
	}
	template := &StreamTemplateConfig{
		Name:       "kv",
		Config:     mcfg,
		MaxStreams: 4,
	}
	req, err := json.Marshal(template)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var stResp JSApiStreamTemplateCreateResponse
	resp, err = nc.Request(fmt.Sprintf(JSApiTemplateCreateT, template.Name), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err = json.Unmarshal(resp.Data, &stResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if stResp.Error == nil {
		t.Fatalf("Expected an unsupported error, got none")
	}
	if !strings.Contains(stResp.Error.Description, "not currently supported in clustered mode") {
		t.Fatalf("Did not get correct error response: %+v", stResp.Error)
	}
}

func TestJetStreamClusterExtendedAccountInfo(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sendBatch := func(subject string, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("JSC-OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	// Add in some streams with msgs and consumers.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-1", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-1"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-1", 25)

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-2", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-2"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-2", 50)

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-3", Replicas: 3, Storage: nats.MemoryStorage}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-3"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-3", 100)

	// Go client will lag so use direct for now.
	getAccountInfo := func() *nats.AccountInfo {
		t.Helper()

		info, err := js.AccountInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return info
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		ai := getAccountInfo()
		if ai.Streams != 3 || ai.Consumers != 3 {
			return fmt.Errorf("AccountInfo not correct: %+v", ai)
		}
		if ai.API.Total < 7 {
			return fmt.Errorf("Expected at least 7 total API calls, got %d", ai.API.Total)
		}
		return nil
	})

	// Now do a failure to make sure we track API errors.
	js.StreamInfo("NO-STREAM")
	js.ConsumerInfo("TEST-1", "NO-CONSUMER")
	js.ConsumerInfo("TEST-2", "NO-CONSUMER")
	js.ConsumerInfo("TEST-3", "NO-CONSUMER")

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		ai := getAccountInfo()
		if ai.API.Errors != 4 {
			return fmt.Errorf("Expected 4 API calls to be errors, got %d", ai.API.Errors)
		}
		return nil
	})
}

func TestJetStreamClusterPeerRemovalAPI(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	// Client based API
	ml := c.leader()
	nc, err := nats.Connect(ml.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer nc.Close()

	// Expect error if unknown peer
	req := &JSApiMetaServerRemoveRequest{Server: "S-9"}
	jsreq, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rmsg, err := nc.Request(JSApiRemoveServer, jsreq, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var resp JSApiMetaServerRemoveResponse
	if err := json.Unmarshal(rmsg.Data, &resp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp.Error == nil {
		t.Fatalf("Expected an error, got none")
	}

	sub, err := nc.SubscribeSync(JSAdvisoryServerRemoved)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	rs := c.randomNonLeader()
	req = &JSApiMetaServerRemoveRequest{Server: rs.Name()}
	jsreq, err = json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rmsg, err = nc.Request(JSApiRemoveServer, jsreq, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp.Error = nil
	if err := json.Unmarshal(rmsg.Data, &resp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %+v", resp.Error)
	}
	c.waitOnLeader()
	ml = c.leader()

	checkSubsPending(t, sub, 1)
	madv, _ := sub.NextMsg(0)
	var adv JSServerRemovedAdvisory
	if err := json.Unmarshal(madv.Data, &adv); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if adv.Server != rs.Name() {
		t.Fatalf("Expected advisory about %s being removed, got %+v", rs.Name(), adv)
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		for _, s := range ml.JetStreamClusterPeers() {
			if s == rs.Name() {
				return fmt.Errorf("Still in the peer list")
			}
		}
		return nil
	})
}

func TestJetStreamClusterPeerRemovalAndStreamReassignment(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	// Client based API
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Admin based API
	ml := c.leader()
	nc, err = nats.Connect(ml.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer nc.Close()

	// Select the non-leader server for the stream to remove.
	if len(si.Cluster.Replicas) < 2 {
		t.Fatalf("Not enough replicas found: %+v", si.Cluster)
	}
	toRemove, cl := si.Cluster.Replicas[0].Name, c.leader()
	if toRemove == cl.Name() {
		toRemove = si.Cluster.Replicas[1].Name
	}

	req := &JSApiMetaServerRemoveRequest{Server: toRemove}
	jsreq, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rmsg, err := nc.Request(JSApiRemoveServer, jsreq, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var resp JSApiMetaServerRemoveResponse
	if err := json.Unmarshal(rmsg.Data, &resp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %+v", resp.Error)
	}
	// In case that server was also meta-leader.
	c.waitOnLeader()

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		for _, s := range ml.JetStreamClusterPeers() {
			if s == toRemove {
				return fmt.Errorf("Server still in the peer list")
			}
		}
		return nil
	})

	// Now wait until the stream is now current.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST", nats.MaxWait(time.Second))
		if err != nil {
			return fmt.Errorf("Could not fetch stream info: %v", err)
		}
		// We should not see the old server at all.
		for _, p := range si.Cluster.Replicas {
			if p.Name == toRemove {
				t.Fatalf("Peer not removed yet: %+v", toRemove)
			}
			if !p.Current {
				return fmt.Errorf("Expected replica to be current: %+v", p)
			}
		}
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(si.Cluster.Replicas))
		}
		return nil
	})
}

func TestJetStreamClusterPeerRemovalAndStreamReassignmentWithoutSpace(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Admin based API
	ml := c.leader()
	nc, err = nats.Connect(ml.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer nc.Close()

	// Select the non-leader server for the stream to remove.
	if len(si.Cluster.Replicas) < 2 {
		t.Fatalf("Not enough replicas found: %+v", si.Cluster)
	}
	toRemove, cl := si.Cluster.Replicas[0].Name, c.leader()
	if toRemove == cl.Name() {
		toRemove = si.Cluster.Replicas[1].Name
	}

	req := &JSApiMetaServerRemoveRequest{Server: toRemove}
	jsreq, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rmsg, err := nc.Request(JSApiRemoveServer, jsreq, 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var resp JSApiMetaServerRemoveResponse
	if err := json.Unmarshal(rmsg.Data, &resp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %+v", resp.Error)
	}
	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		for _, s := range ml.JetStreamClusterPeers() {
			if s == toRemove {
				return fmt.Errorf("Server still in the peer list")
			}
		}
		return nil
	})
	// Make sure only 2 peers at this point.
	c.waitOnPeerCount(2)

	// Now wait until the stream is now current.
	streamCurrent := func(nr int) {
		checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST", nats.MaxWait(time.Second))
			if err != nil {
				return fmt.Errorf("Could not fetch stream info: %v", err)
			}
			// We should not see the old server at all.
			for _, p := range si.Cluster.Replicas {
				if p.Name == toRemove {
					return fmt.Errorf("Peer not removed yet: %+v", toRemove)
				}
				if !p.Current {
					return fmt.Errorf("Expected replica to be current: %+v", p)
				}
			}
			if len(si.Cluster.Replicas) != nr {
				return fmt.Errorf("Expected %d replicas, got %d", nr, len(si.Cluster.Replicas))
			}
			return nil
		})
	}

	// Make sure the peer was removed from the stream and that we did not fill the new spot.
	streamCurrent(1)

	// Now add in a new server and make sure it gets added to our stream.
	c.addInNewServer()
	c.waitOnPeerCount(3)

	streamCurrent(2)
}

func TestJetStreamClusterPeerRemovalAndServerBroughtBack(t *testing.T) {
	// Speed up for this test
	peerRemoveTimeout = 2 * time.Second
	defer func() {
		peerRemoveTimeout = peerRemoveTimeoutDefault
	}()

	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	// Client based API
	ml := c.leader()
	nc, err := nats.Connect(ml.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer nc.Close()

	getPeersCount := func() int {
		js := ml.getJetStream()
		if js == nil {
			return 0
		}
		js.mu.RLock()
		defer js.mu.RUnlock()

		cc := js.cluster
		if !cc.isLeader() || cc.meta == nil {
			return 0
		}
		return len(cc.meta.Peers())
	}

	checkFor(t, 2*time.Second, 250*time.Millisecond, func() error {
		if l := getPeersCount(); l != 5 {
			return fmt.Errorf("expected 5 peers, got %d", l)
		}
		return nil
	})

	// Shutdown server first.
	rs := c.randomNonLeader()
	rs.Shutdown()

	// Peers should still remain the same, even if one server is shut down.
	checkFor(t, 2*time.Second, 250*time.Millisecond, func() error {
		if l := getPeersCount(); l != 5 {
			return fmt.Errorf("expected 5 peers, got %d", l)
		}
		return nil
	})

	// Peer-remove after shutdown.
	req := &JSApiMetaServerRemoveRequest{Server: rs.Name()}
	jsreq, err := json.Marshal(req)
	require_NoError(t, err)
	rmsg, err := nc.Request(JSApiRemoveServer, jsreq, time.Second)
	require_NoError(t, err)

	var resp JSApiMetaServerRemoveResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %+v", resp.Error)
	}

	// Peer should be removed.
	checkFor(t, 2*time.Second, 250*time.Millisecond, func() error {
		if l := getPeersCount(); l != 4 {
			return fmt.Errorf("expected 4 peers, got %d", l)
		}
		return nil
	})

	// Bringing back the server should re-add to peers after peer-remove timeout.
	c.restartServer(rs)
	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		if l := getPeersCount(); l != 5 {
			return fmt.Errorf("expected 5 peers, got %d", l)
		}
		return nil
	})
}

func TestJetStreamClusterPeerExclusionTag(t *testing.T) {
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "C", 3,
		func(serverName, clusterName, storeDir, conf string) string {
			switch serverName {
			case "S-1":
				return fmt.Sprintf("%s\nserver_tags: [server:%s, intersect, %s]", conf, serverName, jsExcludePlacement)
			case "S-2":
				return fmt.Sprintf("%s\nserver_tags: [server:%s, intersect]", conf, serverName)
			default:
				return fmt.Sprintf("%s\nserver_tags: [server:%s]", conf, serverName)
			}
		})
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	for i, c := range []nats.StreamConfig{
		{Replicas: 1, Placement: &nats.Placement{Tags: []string{"server:S-1"}}},
		{Replicas: 2, Placement: &nats.Placement{Tags: []string{"intersect"}}},
		{Replicas: 3}, // not enough server without !jetstream
	} {
		c.Name = fmt.Sprintf("TEST%d", i)
		c.Subjects = []string{c.Name}
		_, err := js.AddStream(&c)
		require_Error(t, err)
		require_Contains(t, err.Error(), "no suitable peers for placement", "exclude tag set")
	}

	// Test update failure
	cfg := &nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 2}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_Error(t, err)
	require_Contains(t, err.Error(), "no suitable peers for placement", "exclude tag set")
	// Test tag reload removing !jetstream tag, and allowing placement again

	srv := c.serverByName("S-1")

	v, err := srv.Varz(nil)
	require_NoError(t, err)
	require_True(t, v.Tags.Contains(jsExcludePlacement))
	content, err := os.ReadFile(srv.configFile)
	require_NoError(t, err)
	newContent := strings.ReplaceAll(string(content), fmt.Sprintf(", %s]", jsExcludePlacement), "]")
	changeCurrentConfigContentWithNewContent(t, srv.configFile, []byte(newContent))

	require_NoError(t, srv.Reload())
	srv.sendStatszUpdate()

	v, err = srv.Varz(nil)
	require_NoError(t, err)
	require_True(t, !v.Tags.Contains(jsExcludePlacement))

	checkFor(t, 20*time.Second, 500*time.Millisecond, func() error {
		for _, s := range c.servers {
			var err error
			s.nodeToInfo.Range(func(sn any, value any) bool {
				ni := value.(nodeInfo)
				if ni.name != "S-1" {
					return true
				}
				if ni.tags.Contains("!jetstream") {
					err = fmt.Errorf("%s still has jetstream exclude tag", sn)
					return false
				}
				return true
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
}

func TestJetStreamClusterAccountPurge(t *testing.T) {
	sysKp, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
	sysCreds := newUser(t, sysKp)
	accKp, accpub := createKey(t)
	accClaim := jwt.NewAccountClaims(accpub)
	accClaim.Limits.JetStreamLimits.DiskStorage = 1024 * 1024 * 5
	accClaim.Limits.JetStreamLimits.MemoryStorage = 1024 * 1024 * 5
	accJwt := encodeClaim(t, accClaim, accpub)
	accCreds := newUser(t, accKp)

	tmlp := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		leaf {
			listen: 127.0.0.1:-1
		}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
	`
	c := createJetStreamClusterWithTemplateAndModHook(t, tmlp, "cluster", 3,
		func(serverName, clustername, storeDir, conf string) string {
			return conf + fmt.Sprintf(`
				operator: %s
				system_account: %s
				resolver: {
					type: full
					dir: '%s/jwt'
					timeout: "10ms"
				}`, ojwt, syspub, storeDir)
		})
	defer c.shutdown()

	c.waitOnLeader()

	updateJwt(t, c.randomServer().ClientURL(), sysCreds, sysJwt, 3)
	updateJwt(t, c.randomServer().ClientURL(), sysCreds, accJwt, 3)

	c.waitOnAccount(accpub)

	createTestData := func(t *testing.T) {
		nc, js := jsClientConnect(t, c.randomNonLeader(), nats.UserCredentials(accCreds))
		defer nc.Close()

		// Prevent 'nats: JetStream not enabled for account' when creating the first stream.
		c.waitOnAccount(accpub)

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST1",
			Subjects: []string{"foo"},
			Replicas: 3,
		})
		require_NoError(t, err)
		c.waitOnStreamLeader(accpub, "TEST1")

		ci, err := js.AddConsumer("TEST1",
			&nats.ConsumerConfig{Durable: "DUR1",
				AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
		require_True(t, ci.Config.Replicas == 0)

		ci, err = js.AddConsumer("TEST1",
			&nats.ConsumerConfig{Durable: "DUR2",
				AckPolicy: nats.AckExplicitPolicy,
				Replicas:  1})
		require_NoError(t, err)
		require_True(t, ci.Config.Replicas == 1)

		toSend := uint64(1_000)
		for i := uint64(0); i < toSend; i++ {
			_, err = js.Publish("foo", nil)
			require_NoError(t, err)
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "TEST2",
			Subjects: []string{"bar"},
			Replicas: 1,
		})
		require_NoError(t, err)

		ci, err = js.AddConsumer("TEST2",
			&nats.ConsumerConfig{Durable: "DUR1",
				AckPolicy: nats.AckExplicitPolicy,
				Replicas:  0})
		require_NoError(t, err)
		require_True(t, ci.Config.Replicas == 0)

		for i := uint64(0); i < toSend; i++ {
			_, err = js.Publish("bar", nil)
			require_NoError(t, err)
		}
	}

	inspectDirs := func(t *testing.T, sysTotal, accTotal int) error {
		t.Helper()
		sysDirs := 0
		accDirs := 0
		for _, s := range c.servers {
			files, err := os.ReadDir(filepath.Join(s.getOpts().StoreDir, "jetstream", syspub, "_js_"))
			require_NoError(t, err)
			sysDirs += len(files) - 1 // sub 1 for _meta_
			files, err = os.ReadDir(filepath.Join(s.getOpts().StoreDir, "jetstream", accpub, "streams"))
			if err == nil || err.(*os.PathError).Error() == "no such file or directory" {
				accDirs += len(files)
			}
		}
		if sysDirs != sysTotal || accDirs != accTotal {
			return fmt.Errorf("expected directory count does not match %d == %d, %d == %d",
				sysDirs, sysTotal, accDirs, accTotal)
		}
		return nil
	}

	checkForDirs := func(t *testing.T, sysTotal, accTotal int) {
		t.Helper()
		checkFor(t, 20*time.Second, 250*time.Millisecond, func() error {
			return inspectDirs(t, sysTotal, accTotal)
		})
	}

	purge := func(t *testing.T) {
		t.Helper()
		ncsys, err := nats.Connect(c.randomServer().ClientURL(), nats.UserCredentials(sysCreds))
		require_NoError(t, err)
		defer ncsys.Close()

		request := func() error {
			var resp JSApiAccountPurgeResponse
			m, err := ncsys.Request(fmt.Sprintf(JSApiAccountPurgeT, accpub), nil, time.Second)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(m.Data, &resp); err != nil {
				return err
			}
			if !resp.Initiated {
				return fmt.Errorf("not started")
			}
			return nil
		}
		checkFor(t, 30*time.Second, 250*time.Millisecond, request)
	}

	t.Run("startup-cleanup", func(t *testing.T) {
		_, newCleanupAcc1 := createKey(t)
		_, newCleanupAcc2 := createKey(t)
		for _, s := range c.servers {
			os.MkdirAll(filepath.Join(s.getOpts().StoreDir, JetStreamStoreDir, newCleanupAcc1, streamsDir), defaultDirPerms)
			os.MkdirAll(filepath.Join(s.getOpts().StoreDir, JetStreamStoreDir, newCleanupAcc2), defaultDirPerms)
		}
		createTestData(t)
		checkForDirs(t, 6, 4)
		c.stopAll()
		c.restartAll()
		for _, s := range c.servers {
			accDir := filepath.Join(s.getOpts().StoreDir, JetStreamStoreDir, newCleanupAcc1)
			_, e := os.Stat(filepath.Join(accDir, streamsDir))
			require_Error(t, e)
			require_True(t, os.IsNotExist(e))
			_, e = os.Stat(accDir)
			require_Error(t, e)
			require_True(t, os.IsNotExist(e))
			_, e = os.Stat(filepath.Join(s.getOpts().StoreDir, JetStreamStoreDir, newCleanupAcc2))
			require_Error(t, e)
			require_True(t, os.IsNotExist(e))
		}
		checkForDirs(t, 6, 4)
		// Make sure we have a leader for all assets before moving to the next test
		c.waitOnStreamLeader(accpub, "TEST1")
		c.waitOnConsumerLeader(accpub, "TEST1", "DUR1")
		c.waitOnConsumerLeader(accpub, "TEST1", "DUR2")
		c.waitOnStreamLeader(accpub, "TEST2")
		c.waitOnConsumerLeader(accpub, "TEST2", "DUR1")
	})

	t.Run("purge-with-restart", func(t *testing.T) {
		createTestData(t)
		checkForDirs(t, 6, 4)
		purge(t)
		checkForDirs(t, 0, 0)
		c.stopAll()
		c.restartAll()
		checkForDirs(t, 0, 0)
	})

	t.Run("purge-with-reuse", func(t *testing.T) {
		createTestData(t)
		checkForDirs(t, 6, 4)
		purge(t)
		checkForDirs(t, 0, 0)
		createTestData(t)
		checkForDirs(t, 6, 4)
		purge(t)
		checkForDirs(t, 0, 0)
	})

	t.Run("purge-deleted-account", func(t *testing.T) {
		createTestData(t)
		checkForDirs(t, 6, 4)
		c.stopAll()
		for _, s := range c.servers {
			require_NoError(t, os.Remove(s.getOpts().StoreDir+"/jwt/"+accpub+".jwt"))
		}
		c.restartAll()
		checkForDirs(t, 6, 4)
		purge(t)
		checkForDirs(t, 0, 0)
		c.stopAll()
		c.restartAll()
		checkForDirs(t, 0, 0)
	})
}

func TestJetStreamClusterScaleConsumer(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterTempl, "C", 3)
	defer c.shutdown()

	srv := c.randomNonLeader()
	nc, js := jsClientConnect(t, srv)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	durCfg := &nats.ConsumerConfig{Durable: "DUR", AckPolicy: nats.AckExplicitPolicy}
	ci, err := js.AddConsumer("TEST", durCfg)
	require_NoError(t, err)
	require_True(t, ci.Config.Replicas == 0)

	toSend := uint64(1_000)
	for i := uint64(0); i < toSend; i++ {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	s, err := js.PullSubscribe("foo", "DUR")
	require_NoError(t, err)

	consumeOne := func(expSeq uint64) error {
		if ci, err := js.ConsumerInfo("TEST", "DUR"); err != nil {
			return err
		} else if ci.Delivered.Stream != expSeq {
			return fmt.Errorf("pre: not expected delivered stream %d, got %d", expSeq, ci.Delivered.Stream)
		} else if ci.Delivered.Consumer != expSeq {
			return fmt.Errorf("pre: not expected delivered consumer %d, got %d", expSeq, ci.Delivered.Consumer)
		} else if ci.AckFloor.Stream != expSeq {
			return fmt.Errorf("pre: not expected ack stream %d, got %d", expSeq, ci.AckFloor.Stream)
		} else if ci.AckFloor.Consumer != expSeq {
			return fmt.Errorf("pre: not expected ack consumer %d, got %d", expSeq, ci.AckFloor.Consumer)
		}
		if m, err := s.Fetch(1); err != nil {
			return err
		} else if err := m[0].AckSync(); err != nil {
			return err
		}
		expSeq = expSeq + 1
		if ci, err := js.ConsumerInfo("TEST", "DUR"); err != nil {
			return err
		} else if ci.Delivered.Stream != expSeq {
			return fmt.Errorf("post: not expected delivered stream %d, got %d", expSeq, ci.Delivered.Stream)
		} else if ci.Delivered.Consumer != expSeq {
			return fmt.Errorf("post: not expected delivered consumer %d, got %d", expSeq, ci.Delivered.Consumer)
		} else if ci.AckFloor.Stream != expSeq {
			return fmt.Errorf("post: not expected ack stream %d, got %d", expSeq, ci.AckFloor.Stream)
		} else if ci.AckFloor.Consumer != expSeq {
			return fmt.Errorf("post: not expected ack consumer %d, got %d", expSeq, ci.AckFloor.Consumer)
		}
		return nil
	}

	require_NoError(t, consumeOne(0))

	// scale down, up, down and up to default == 3 again
	for i, r := range []int{1, 3, 1, 0} {
		durCfg.Replicas = r
		if r == 0 {
			r = si.Config.Replicas
		}
		js.UpdateConsumer("TEST", durCfg)

		checkFor(t, time.Second*30, time.Millisecond*250, func() error {
			if ci, err = js.ConsumerInfo("TEST", "DUR"); err != nil {
				return err
			} else if ci.Cluster == nil {
				return errors.New("no cluster info")
			} else if ci.Cluster.Leader == _EMPTY_ {
				return errors.New("no leader")
			} else if len(ci.Cluster.Replicas) != r-1 {
				return fmt.Errorf("not enough replica, got %d wanted %d", len(ci.Cluster.Replicas), r-1)
			} else {
				for _, r := range ci.Cluster.Replicas {
					if !r.Current || r.Offline || r.Lag != 0 {
						return fmt.Errorf("replica %s not current %t offline %t lag %d", r.Name, r.Current, r.Offline, r.Lag)
					}
				}
			}
			return nil
		})

		require_NoError(t, consumeOne(uint64(i+1)))
	}
}

func TestJetStreamClusterConsumerScaleUp(t *testing.T) {
	c := createJetStreamCluster(t, jsClusterTempl, "HUB", _EMPTY_, 3, 22020, true)
	defer c.shutdown()

	// Client based API
	srv := c.randomNonLeader()
	nc, js := jsClientConnect(t, srv)
	defer nc.Close()

	scfg := nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	}
	_, err := js.AddStream(&scfg)
	require_NoError(t, err)
	defer js.DeleteStream("TEST")

	dcfg := nats.ConsumerConfig{
		Durable:   "DUR",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  0}
	_, err = js.AddConsumer("TEST", &dcfg)
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	scfg.Replicas = 2
	_, err = js.UpdateStream(&scfg)
	require_NoError(t, err)

	// The scale up issue shows itself as permanent loss of consumer leadership
	// So give it some time for the change to propagate to new consumer peers and the quorum to disrupt
	// 2 seconds is a value arrived by experimentally, no sleep or a sleep of 1sec always had the test pass a lot.
	time.Sleep(2 * time.Second)

	c.waitOnStreamLeader("$G", "TEST")

	// There is also a timing component to the issue triggering.
	c.waitOnConsumerLeader("$G", "TEST", "DUR")
}

func TestJetStreamClusterPeerOffline(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	ml := c.leader()
	rs := c.randomNonLeader()

	checkPeer := func(ml, rs *Server, shouldBeOffline bool) {
		t.Helper()

		checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
			var found bool
			for _, s := range ml.JetStreamClusterPeers() {
				if s == rs.Name() {
					found = true
					break
				}
			}
			if !shouldBeOffline && !found {
				return fmt.Errorf("Server %q not in the peers list", rs.Name())
			} else if shouldBeOffline && found {
				return fmt.Errorf("Server %q should not be in the peers list", rs.Name())
			}

			var ok bool
			ml.nodeToInfo.Range(func(k, v any) bool {
				if si := v.(nodeInfo); si.name == rs.Name() {
					if shouldBeOffline && si.offline || !shouldBeOffline && !si.offline {
						ok = true
						return false
					}
				}
				return true
			})
			if !ok {
				if shouldBeOffline {
					return fmt.Errorf("Server %q should be marked as online", rs.Name())
				}
				return fmt.Errorf("Server %q is still marked as online", rs.Name())
			}
			return nil
		})
	}

	// Shutdown the server and make sure that it is now showing as offline.
	rs.Shutdown()
	checkPeer(ml, rs, true)

	// Now restart that server and check that is no longer offline.
	oldrs := rs
	rs, _ = RunServerWithConfig(rs.getOpts().ConfigFile)
	defer rs.Shutdown()

	// Replaced old with new server
	for i := 0; i < len(c.servers); i++ {
		if c.servers[i] == oldrs {
			c.servers[i] = rs
		}
	}

	// Wait for cluster to be formed
	checkClusterFormed(t, c.servers...)

	// Make sure that we have a leader (there can always be a re-election)
	c.waitOnLeader()
	ml = c.leader()

	// Now check that rs is not offline
	checkPeer(ml, rs, false)
}

func TestJetStreamClusterNoQuorumStepdown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Setup subscription for leader elected.
	lesub, err := nc.SubscribeSync(JSAdvisoryStreamLeaderElectedPre + ".*")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{Name: "NO-Q", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we received our leader elected advisory.
	leadv, _ := lesub.NextMsg(0)
	if leadv == nil {
		t.Fatalf("Expected to receive a leader elected advisory")
	}
	var le JSStreamLeaderElectedAdvisory
	if err := json.Unmarshal(leadv.Data, &le); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ln := c.streamLeader("$G", "NO-Q").Name(); le.Leader != ln {
		t.Fatalf("Expected to have leader %q in elect advisory, got %q", ln, le.Leader)
	}

	payload := []byte("Hello JSC")
	for i := 0; i < 10; i++ {
		if _, err := js.Publish("NO-Q", payload); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Setup subscription for leader elected.
	clesub, err := nc.SubscribeSync(JSAdvisoryConsumerLeaderElectedPre + ".*.*")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make durable to have R match Stream.
	sub, err := js.SubscribeSync("NO-Q", nats.Durable("rr"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ci, err := sub.ConsumerInfo()
	if err != nil || ci == nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we received our consumer leader elected advisory.
	leadv, _ = clesub.NextMsg(0)
	if leadv == nil {
		t.Fatalf("Expected to receive a consumer leader elected advisory")
	}

	// Shutdown the non-leader.
	c.randomNonStreamLeader("$G", "NO-Q").Shutdown()

	// This should eventually have us stepdown as leader since we would have lost quorum with R=2.
	checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
		if sl := c.streamLeader("$G", "NO-Q"); sl == nil {
			return nil
		}
		return fmt.Errorf("Still have leader for stream")
	})

	notAvailableErr := func(err error) bool {
		return err != nil && (strings.Contains(err.Error(), "unavailable") || err == context.DeadlineExceeded)
	}

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if cl := c.consumerLeader("$G", "NO-Q", ci.Name); cl == nil {
			return nil
		}
		return fmt.Errorf("Still have leader for consumer")
	})

	if _, err = js.ConsumerInfo("NO-Q", ci.Name); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if _, err := sub.ConsumerInfo(); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}

	// Now let's take out the other non meta-leader
	// We should get same error for general API calls.
	c.randomNonLeader().Shutdown()
	c.expectNoLeader()

	// Now make sure the general JS API responds with system unavailable.
	if _, err = js.AccountInfo(); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "NO-Q33", Replicas: 2}); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if _, err := js.UpdateStream(&nats.StreamConfig{Name: "NO-Q33", Replicas: 2}); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if err := js.DeleteStream("NO-Q"); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if err := js.PurgeStream("NO-Q"); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if err := js.DeleteMsg("NO-Q", 1); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	// Consumer
	if _, err := js.AddConsumer("NO-Q", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if err := js.DeleteConsumer("NO-Q", "dlc"); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if _, err := js.ConsumerInfo("NO-Q", "dlc"); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	// Listers
	for info := range js.StreamsInfo() {
		t.Fatalf("Unexpected stream info, got %v", info)
	}
	for info := range js.ConsumersInfo("NO-Q") {
		t.Fatalf("Unexpected consumer info, got %v", info)
	}
}

func TestJetStreamClusterCreateResponseAdvisoriesHaveSubject(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.API")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST", nats.Durable("DLC")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := js.DeleteStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, 6)

	for m, err := sub.NextMsg(0); err == nil; m, err = sub.NextMsg(0) {
		var audit JSAPIAudit
		if err := json.Unmarshal(m.Data, &audit); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if audit.Subject == _EMPTY_ {
			t.Fatalf("Expected subject, got nothing")
		}
	}
}

func TestJetStreamClusterRestartAndRemoveAdvisories(t *testing.T) {
	// FIXME(dlc) - Flaky on Travis, skip for now.
	skip(t)

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.API")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	csub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.*.CREATED.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer csub.Unsubscribe()
	nc.Flush()

	sendBatch := func(subject string, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("JSC-OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	// Add in some streams with msgs and consumers.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-1", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-1", nats.Durable("DC")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-1", 25)

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-2", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-2", nats.Durable("DC")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-2", 50)

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-3", Replicas: 3, Storage: nats.MemoryStorage}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-3", nats.Durable("DC")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-3", 100)

	drainSub := func(sub *nats.Subscription) {
		for _, err := sub.NextMsg(0); err == nil; _, err = sub.NextMsg(0) {
		}
	}

	// Wait for the advisories for all streams and consumers.
	checkSubsPending(t, sub, 12) // 3 streams, 3*2 consumers, 3 stream names lookups for creating consumers.
	drainSub(sub)

	// Created audit events.
	checkSubsPending(t, csub, 6)
	drainSub(csub)

	usub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.*.UPDATED.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer usub.Unsubscribe()
	nc.Flush()

	checkSubsPending(t, csub, 0)
	checkSubsPending(t, sub, 0)
	checkSubsPending(t, usub, 0)

	// Now restart the other two servers we are not connected to.
	for _, cs := range c.servers {
		if cs != s {
			cs.Shutdown()
			c.restartServer(cs)
		}
	}
	c.waitOnAllCurrent()

	checkSubsPending(t, csub, 0)
	checkSubsPending(t, sub, 0)
	checkSubsPending(t, usub, 0)

	dsub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.*.DELETED.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer dsub.Unsubscribe()
	nc.Flush()

	c.waitOnConsumerLeader("$G", "TEST-1", "DC")
	c.waitOnLeader()

	// Now check delete advisories as well.
	if err := js.DeleteConsumer("TEST-1", "DC"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, csub, 0)
	checkSubsPending(t, dsub, 1)
	checkSubsPending(t, sub, 1)
	checkSubsPending(t, usub, 0)
	drainSub(dsub)

	if err := js.DeleteStream("TEST-3"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, dsub, 2) // Stream and the consumer underneath.
	checkSubsPending(t, sub, 2)
}

func TestJetStreamClusterNoDuplicateOnNodeRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "ND", 2)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sl := c.streamLeader("$G", "TEST")
	if s == sl {
		nc.Close()
		nc, js = jsClientConnect(t, s)
		defer nc.Close()
	}

	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js.Publish("foo", []byte("msg1"))
	if m, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	} else {
		m.AckSync()
	}

	sl.Shutdown()
	c.restartServer(sl)
	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnConsumerLeader("$G", "TEST", "dlc")

	// Send second msg
	js.Publish("foo", []byte("msg2"))
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	if string(msg.Data) != "msg2" {
		t.Fatalf("Unexpected message: %s", msg.Data)
	}
	msg.AckSync()

	// Make sure we don't get a duplicate.
	msg, err = sub.NextMsg(250 * time.Millisecond)
	if err == nil {
		t.Fatalf("Should have gotten an error, got %s", msg.Data)
	}
}

func TestJetStreamClusterNoDupePeerSelection(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "NDP", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Create 10 streams. Make sure none of them have a replica
	// that is the same as the leader.
	for i := 1; i <= 10; i++ {
		si, err := js.AddStream(&nats.StreamConfig{
			Name:     fmt.Sprintf("TEST-%d", i),
			Replicas: 3,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.Cluster == nil || si.Cluster.Leader == "" || len(si.Cluster.Replicas) != 2 {
			t.Fatalf("Unexpected cluster state for stream info: %+v\n", si.Cluster)
		}
		// Make sure that the replicas are not same as the leader.
		for _, pi := range si.Cluster.Replicas {
			if pi.Name == si.Cluster.Leader {
				t.Fatalf("Found replica that is same as leader, meaning 2 nodes placed on same server")
			}
		}
		// Now do a consumer and check same thing.
		sub, err := js.SubscribeSync(si.Config.Name)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ci, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatalf("Unexpected error getting consumer info: %v", err)
		}
		for _, pi := range ci.Cluster.Replicas {
			if pi.Name == ci.Cluster.Leader {
				t.Fatalf("Found replica that is same as leader, meaning 2 nodes placed on same server")
			}
		}
	}
}

func TestJetStreamClusterStreamRemovePeer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "RNS", 5)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sub, err := js.SubscribeSync("TEST", nats.Durable("cat"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, sub, toSend)

	// Do ephemeral too.
	esub, err := js.SubscribeSync("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, esub, toSend)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "pull", AckPolicy: nats.AckNonePolicy})
	require_NoError(t, err)

	pullSub, err := js.PullSubscribe("TEST", "pull")
	require_NoError(t, err)

	// First fetch the messages that are already there.
	msgs, err := pullSub.Fetch(toSend, nats.MaxWait(500*time.Millisecond))
	require_NoError(t, err)
	require_Equal(t, toSend, len(msgs))

	// Now prepare a check to see if we get unwated `Consumer Deleted` error on peer remove.
	pullResults := make(chan error, 1)
	go func() {
		_, err := pullSub.Fetch(1, nats.MaxWait(30*time.Second))
		// Let's check if we get unwted `Consumer Deleted` error on peer remove.
		// Everything else is fine (Leader Changed, Timeout, etc.)
		if err != nats.ErrConsumerDeleted {
			close(pullResults)
		} else {
			pullResults <- err
		}
	}()

	ci, err := esub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Could not fetch consumer info: %v", err)
	}
	// Capture ephemeral's server and name.
	es, en := ci.Cluster.Leader, ci.Name

	// Grab stream info.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	peers := []string{si.Cluster.Leader}
	for _, p := range si.Cluster.Replicas {
		peers = append(peers, p.Name)
	}
	// Pick a truly random server to remove.
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	toRemove := peers[0]

	// First test bad peer.
	req := &JSApiStreamRemovePeerRequest{Peer: "NOT VALID"}
	jsreq, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Need to call this by hand for now.
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), jsreq, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var rpResp JSApiStreamRemovePeerResponse
	if err := json.Unmarshal(resp.Data, &rpResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rpResp.Error == nil || !strings.Contains(rpResp.Error.Description, "peer not a member") {
		t.Fatalf("Expected error for bad peer, got %+v", rpResp.Error)
	}
	rpResp.Error = nil

	req = &JSApiStreamRemovePeerRequest{Peer: toRemove}
	jsreq, err = json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	resp, err = nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), jsreq, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := json.Unmarshal(resp.Data, &rpResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rpResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", rpResp.Error)
	}

	c.waitOnStreamLeader("$G", "TEST")

	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST", nats.MaxWait(time.Second))
		if err != nil {
			return fmt.Errorf("Could not fetch stream info: %v", err)
		}
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(si.Cluster.Replicas))
		}
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		if si.Cluster.Leader == toRemove {
			return fmt.Errorf("Peer not removed yet: %+v", toRemove)
		}
		for _, p := range si.Cluster.Replicas {
			if p.Name == toRemove {
				return fmt.Errorf("Peer not removed yet: %+v", toRemove)
			}
		}
		return nil
	})

	c.waitOnConsumerLeader("$G", "TEST", "cat")
	c.waitOnConsumerLeader("$G", "TEST", "pull")

	// Now check consumer info as well.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "cat", nats.MaxWait(time.Second))
		if err != nil {
			return fmt.Errorf("Could not fetch consumer info: %v", err)
		}
		if len(ci.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(ci.Cluster.Replicas))
		}
		for _, peer := range ci.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		if ci.Cluster.Leader == toRemove {
			return fmt.Errorf("Peer not removed yet: %+v", toRemove)
		}
		for _, p := range ci.Cluster.Replicas {
			if p.Name == toRemove {
				return fmt.Errorf("Peer not removed yet: %+v", toRemove)
			}
		}
		return nil
	})

	// Check if we got the `Consumer Deleted` error on the pull consumer.
	select {
	case err := <-pullResults:
		if err != nil {
			t.Fatalf("Expected timeout error or nil, got %v", err)
		}
	default:
	}

	// Now check ephemeral consumer info.
	// Make sure we did not stamp same new group into the ephemeral where R=1.
	ci, err = esub.ConsumerInfo()
	// If the leader was same as what we just removed, this should fail.
	if es == toRemove {
		if err != nats.ErrConsumerNotFound {
			t.Fatalf("Expected a not found error, got %v", err)
		}
		// Also make sure this was removed all together.
		// We may proactively move things in the future.
		for cn := range js.ConsumerNames("TEST") {
			if cn == en {
				t.Fatalf("Expected ephemeral consumer to be deleted since we removed its only peer")
			}
		}
	} else {
		if err != nil {
			t.Fatalf("Could not fetch consumer info: %v", err)
		}
		if len(ci.Cluster.Replicas) != 0 {
			t.Fatalf("Expected no replicas for ephemeral, got %d", len(ci.Cluster.Replicas))
		}
	}
}

func TestJetStreamClusterStreamLeaderStepDown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "RNS", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sub, err := js.SubscribeSync("TEST", nats.Durable("cat"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	oldLeader := c.streamLeader("$G", "TEST").Name()

	// Need to call this by hand for now.
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var sdResp JSApiStreamLeaderStepDownResponse
	if err := json.Unmarshal(resp.Data, &sdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if sdResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", sdResp.Error)
	}

	// Grab shorter timeout jetstream context.
	js, err = nc.JetStream(nats.MaxWait(250 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return fmt.Errorf("Could not fetch stream info: %v", err)
		}
		if si.Cluster.Leader == oldLeader {
			return fmt.Errorf("Still have old leader")
		}
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(si.Cluster.Replicas))
		}
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
	})

	// Now do consumer.
	oldLeader = c.consumerLeader("$G", "TEST", "cat").Name()

	// Need to call this by hand for now.
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "cat"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var cdResp JSApiConsumerLeaderStepDownResponse
	if err := json.Unmarshal(resp.Data, &cdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if cdResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", cdResp.Error)
	}

	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "cat")
		if err != nil {
			return fmt.Errorf("Could not fetch consumer info: %v", err)
		}
		if ci.Cluster.Leader == oldLeader {
			return fmt.Errorf("Still have old leader")
		}
		if len(ci.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(ci.Cluster.Replicas))
		}
		for _, peer := range ci.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
	})
}

func TestJetStreamClusterRemoveServer(t *testing.T) {
	skip(t)

	c := createJetStreamClusterExplicit(t, "RNS", 5)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	sub, err := js.SubscribeSync("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, sub, toSend)
	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	cname := ci.Name

	sl := c.streamLeader("$G", "TEST")
	c.removeJetStream(sl)

	c.waitOnLeader()
	c.waitOnStreamLeader("$G", "TEST")

	// Faster timeout since we loop below checking for condition.
	js, err = nc.JetStream(nats.MaxWait(250 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check the stream info is eventually correct.
	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return fmt.Errorf("Could not fetch stream info: %v", err)
		}
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(si.Cluster.Replicas))
		}
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
	})

	// Now do consumer.
	c.waitOnConsumerLeader("$G", "TEST", cname)
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", cname)
		if err != nil {
			return fmt.Errorf("Could not fetch consumer info: %v", err)
		}
		if len(ci.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(ci.Cluster.Replicas))
		}
		for _, peer := range ci.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
	})
}

func TestJetStreamClusterPurgeReplayAfterRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "P3F", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sendBatch := func(n int) {
		t.Helper()
		// Send a batch to a given subject.
		for i := 0; i < n; i++ {
			if _, err := js.Publish("TEST", []byte("OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	sendBatch(10)
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
	sendBatch(10)

	c.stopAll()
	c.restartAll()

	c.waitOnStreamLeader("$G", "TEST")

	s = c.randomServer()
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != 10 {
		t.Fatalf("Expected 10 msgs after restart, got %d", si.State.Msgs)
	}
}

func TestJetStreamClusterStreamGetMsg(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3F", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.Publish("TEST", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	mreq := &JSApiMsgGetRequest{Seq: 1}
	req, err := json.Marshal(mreq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rmsg, err := nc.Request(fmt.Sprintf(JSApiMsgGetT, "TEST"), req, time.Second)
	if err != nil {
		t.Fatalf("Could not retrieve stream message: %v", err)
	}
	if err != nil {
		t.Fatalf("Could not retrieve stream message: %v", err)
	}

	var resp JSApiMsgGetResponse
	err = json.Unmarshal(rmsg.Data, &resp)
	if err != nil {
		t.Fatalf("Could not parse stream message: %v", err)
	}
	if resp.Message == nil || resp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", resp.Error)
	}
}

func TestJetStreamClusterStreamDirectGetMsg(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3F", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo"},
		Storage:     MemoryStorage,
		Replicas:    3,
		MaxMsgsPer:  1,
		AllowDirect: true,
	}
	addStream(t, nc, cfg)
	sendStreamMsg(t, nc, "foo", "bar")

	getSubj := fmt.Sprintf(JSDirectMsgGetT, "TEST")
	getMsg := func(req *JSApiMsgGetRequest) *nats.Msg {
		var b []byte
		var err error
		if req != nil {
			b, err = json.Marshal(req)
			require_NoError(t, err)
		}
		m, err := nc.Request(getSubj, b, time.Second)
		require_NoError(t, err)
		return m
	}

	m := getMsg(&JSApiMsgGetRequest{LastFor: "foo"})
	require_True(t, string(m.Data) == "bar")
	require_True(t, m.Header.Get(JSStream) == "TEST")
	require_True(t, m.Header.Get(JSSequence) == "1")
	require_True(t, m.Header.Get(JSSubject) == "foo")
	require_True(t, m.Subject != "foo")
	require_True(t, m.Header.Get(JSTimeStamp) != _EMPTY_)
}

func TestJetStreamClusterStreamPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	skip(t)

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	numConnections := 4
	var conns []nats.JetStream
	for i := 0; i < numConnections; i++ {
		s := c.randomServer()
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		conns = append(conns, js)
	}

	toSend := 100000
	numProducers := 8

	payload := []byte("Hello JSC")

	startCh := make(chan bool)
	var wg sync.WaitGroup

	for n := 0; n < numProducers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			js := conns[rand.Intn(numConnections)]
			<-startCh
			for i := 0; i < int(toSend)/numProducers; i++ {
				if _, err = js.Publish("foo", payload); err != nil {
					t.Errorf("Unexpected publish error: %v", err)
				}
			}
		}()
	}

	// Wait for Go routines.
	time.Sleep(250 * time.Millisecond)

	start := time.Now()
	close(startCh)
	wg.Wait()

	tt := time.Since(start)
	fmt.Printf("Took %v to send %d msgs with %d producers and R=3!\n", tt, toSend, numProducers)
	fmt.Printf("%.0f msgs/sec\n\n", float64(toSend)/tt.Seconds())
}

func TestJetStreamClusterConsumerPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	skip(t)

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	toSend := 500000
	msg := make([]byte, 64)
	crand.Read(msg)

	for i := 0; i < toSend; i++ {
		nc.Publish("TEST", msg)
	}
	nc.Flush()

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return fmt.Errorf("Unexpected error: %v", err)
		}
		if si.State.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected to have %d messages, got %d", toSend, si.State.Msgs)
		}
		return nil
	})

	received := int32(0)
	deliverTo := "r"
	done := make(chan bool)
	total := int32(toSend)
	var start time.Time

	nc.Subscribe(deliverTo, func(m *nats.Msg) {
		if r := atomic.AddInt32(&received, 1); r >= total {
			done <- true
		} else if r == 1 {
			start = time.Now()
		}
	})

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{DeliverSubject: deliverTo, Durable: "gf"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("Timed out?")
	}
	tt := time.Since(start)
	fmt.Printf("Took %v to receive %d msgs\n", tt, toSend)
	fmt.Printf("%.0f msgs/sec\n\n", float64(toSend)/tt.Seconds())
}

// This test creates a queue consumer for the delivery subject,
// and make sure it connects to the server that is not the leader
// of the stream. A bug was not stripping the $JS.ACK reply subject
// correctly, which means that ack sent on the reply subject was
// dropped by the route.
func TestJetStreamClusterQueueSubConsumer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R2S", 2)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>"},
		Replicas: 1,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	inbox := nats.NewInbox()
	obsReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			Durable:        "ivan",
			DeliverSubject: inbox,
			DeliverGroup:   "queue",
			AckPolicy:      AckExplicit,
			AckWait:        100 * time.Millisecond,
		},
	}
	req, err := json.Marshal(obsReq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", "ivan"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var ccResp JSApiConsumerCreateResponse
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ccResp.Error != nil {
		t.Fatalf("Unexpected error, got %+v", ccResp.Error)
	}

	ci, err := js.ConsumerInfo("TEST", "ivan")
	if err != nil {
		t.Fatalf("Error getting consumer info: %v", err)
	}

	// Now create a client that does NOT connect to the stream leader.
	// Start with url from first server in the cluster.
	u := c.servers[0].ClientURL()
	// If leader is "S-1", then use S-2 to connect to, which is at servers[1].
	if ci.Cluster.Leader == "S-1" {
		u = c.servers[1].ClientURL()
	}
	qsubnc, err := nats.Connect(u)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer qsubnc.Close()

	ch := make(chan struct{}, 2)
	if _, err := qsubnc.QueueSubscribe(inbox, "queue", func(m *nats.Msg) {
		m.Respond(nil)
		ch <- struct{}{}
	}); err != nil {
		t.Fatalf("Error creating sub: %v", err)
	}

	// Use the other connection to publish a message
	if _, err := js.Publish("foo.bar", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}

	// Wait that we receive the message first.
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("Did not receive message")
	}

	// Message should be ack'ed and not redelivered.
	select {
	case <-ch:
		t.Fatal("Message redelivered!!!")
	case <-time.After(250 * time.Millisecond):
		// OK
	}
}

func TestJetStreamClusterLeaderStepdown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	c.waitOnLeader()
	cl := c.leader()
	// Now ask the system account to have the leader stepdown.
	s := c.randomNonLeader()
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer nc.Close()

	resp, err := nc.Request(JSApiLeaderStepDown, nil, time.Second)
	if err != nil {
		t.Fatalf("Error on stepdown request: %v", err)
	}
	var sdr JSApiLeaderStepDownResponse
	if err := json.Unmarshal(resp.Data, &sdr); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if sdr.Error != nil || !sdr.Success {
		t.Fatalf("Unexpected error for leader stepdown: %+v", sdr.Error)
	}

	c.waitOnLeader()
	if cl == c.leader() {
		t.Fatalf("Expected a new metaleader, got same")
	}
}

func TestJetStreamClusterSourcesFilteringAndUpdating(t *testing.T) {
	owt := srcConsumerWaitTime
	srcConsumerWaitTime = 2 * time.Second
	defer func() { srcConsumerWaitTime = owt }()

	c := createJetStreamClusterExplicit(t, "MSR", 5)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	sendBatch := func(subject string, n int) {
		t.Helper()
		// Send a batch to a given subject.
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	checkSync := func(msgsTest, msgsM uint64) {
		t.Helper()
		checkFor(t, 30*time.Second, time.Second, func() error {
			if tsi, err := js.StreamInfo("TEST"); err != nil {
				return err
			} else if msi, err := js.StreamInfo("M"); err != nil {
				return err
			} else if tsi.State.Msgs != msgsTest {
				return fmt.Errorf("received %d msgs from TEST, expected %d", tsi.State.Msgs, msgsTest)
			} else if msi.State.Msgs != msgsM {
				return fmt.Errorf("received %d msgs from M, expected %d", msi.State.Msgs, msgsM)
			}
			return nil
		})
	}

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 2,
	})
	require_NoError(t, err)
	defer js.DeleteStream("TEST")

	// Create M stream with a single source on "foo"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Sources:  []*nats.StreamSource{{Name: "TEST", FilterSubject: "foo"}},
		Replicas: 2,
	})
	require_NoError(t, err)
	defer js.DeleteStream("M")

	// check a message on "bar" doesn't get sourced
	sendBatch("bar", 100)
	checkSync(100, 0)
	// check a message on "foo" does get sourced
	sendBatch("foo", 100)
	checkSync(200, 100)

	// change remove the source on "foo" and add a new source on "bar"
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "M",
		Sources:  []*nats.StreamSource{{Name: "TEST", FilterSubject: "bar"}},
		Replicas: 2,
	})
	require_NoError(t, err)

	// As it is a new source (never been sourced before) it starts sourcing at the start of TEST
	// and therefore sources the message on "bar" that is in TEST
	checkSync(200, 200)

	// new messages on "foo" are being filtered as it's not being currently sourced
	sendBatch("foo", 100)
	checkSync(300, 200)
	// new messages on "bar" are being sourced
	sendBatch("bar", 100)
	checkSync(400, 300)

	// re-add the source for "foo" keep the source on "bar"
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "M",
		Sources:  []*nats.StreamSource{{Name: "TEST", FilterSubject: "bar"}, {Name: "TEST", FilterSubject: "foo"}},
		Replicas: 2,
	})
	require_NoError(t, err)

	// check the 'backfill' of messages on "foo" that were published while the source was inactive
	checkSync(400, 400)

	// causes startingSequenceForSources() to be called
	nc.Close()
	c.stopAll()
	c.restartAll()
	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnStreamLeader("$G", "M")

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// check that it restarted the sources' consumers at the right place
	checkSync(400, 400)

	// check both sources are still active
	sendBatch("bar", 100)
	checkSync(500, 500)
	sendBatch("foo", 100)
	checkSync(600, 600)

	// Check that purging the stream and does not cause the re-sourcing of the messages
	js.PurgeStream("M")
	checkSync(600, 0)

	// Even after a leader change or restart
	nc.Close()
	c.stopAll()
	c.restartAll()
	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnStreamLeader("$G", "M")

	// Allow direct sync consumers to connect.
	// This test could pass if we do not hook up and try to deliver the messages when we should not.
	time.Sleep(1500 * time.Millisecond)

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	checkSync(600, 0)
}

func TestJetStreamClusterSourcesUpdateOriginError(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "MSR", 5)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	sendBatch := func(subject string, n int) {
		t.Helper()
		// Send a batch to a given subject.
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	checkSync := func(msgsTest, msgsM uint64) {
		t.Helper()
		checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
			if tsi, err := js.StreamInfo("TEST"); err != nil {
				return err
			} else if msi, err := js.StreamInfo("M"); err != nil {
				return err
			} else if tsi.State.Msgs != msgsTest {
				return fmt.Errorf("received %d msgs from TEST, expected %d", tsi.State.Msgs, msgsTest)
			} else if msi.State.Msgs != msgsM {
				return fmt.Errorf("received %d msgs from M, expected %d", msi.State.Msgs, msgsM)
			}
			return nil
		})
	}

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Sources:  []*nats.StreamSource{{Name: "TEST", FilterSubject: "foo"}},
		Replicas: 2,
	})

	require_NoError(t, err)

	// Send 100 msgs.
	sendBatch("foo", 100)
	checkSync(100, 100)

	// update makes source invalid
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"bar"},
		Replicas: 2,
	})
	require_NoError(t, err)

	// TODO check for downstream error propagation

	_, err = js.Publish("foo", nil)
	require_Error(t, err)

	sendBatch("bar", 100)
	// The source stream remains at 100 msgs as it still uses foo as it's filter
	checkSync(200, 100)

	nc.Close()
	c.stopAll()
	c.restartAll()
	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnStreamLeader("$G", "M")

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	checkSync(200, 100)

	_, err = js.Publish("foo", nil)
	require_Error(t, err)
	require_Equal(t, err.Error(), "nats: no response from stream")

	sendBatch("bar", 100)
	// The source stream remains at 100 msgs as it still uses foo as it's filter
	checkSync(300, 100)
}

func TestJetStreamClusterMirrorAndSourcesClusterRestart(t *testing.T) {
	owt := srcConsumerWaitTime
	srcConsumerWaitTime = 2 * time.Second
	defer func() { srcConsumerWaitTime = owt }()

	test := func(t *testing.T, mirror bool, filter bool) {
		c := createJetStreamClusterExplicit(t, "MSR", 5)
		defer c.shutdown()

		// Client for API requests.
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		// Origin
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo", "bar", "baz.*"},
			Replicas: 2,
		})
		require_NoError(t, err)

		filterSubj := _EMPTY_
		if filter {
			filterSubj = "foo"
		}

		// Create Mirror/Source now.
		if mirror {
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     "M",
				Mirror:   &nats.StreamSource{Name: "TEST", FilterSubject: filterSubj},
				Replicas: 2,
			})
		} else {
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     "M",
				Sources:  []*nats.StreamSource{{Name: "TEST", FilterSubject: filterSubj}},
				Replicas: 2,
			})
		}
		require_NoError(t, err)

		expectedMsgCount := uint64(0)

		sendBatch := func(subject string, n int) {
			t.Helper()
			if subject == "foo" || !filter {
				expectedMsgCount += uint64(n)
			}
			// Send a batch to a given subject.
			for i := 0; i < n; i++ {
				if _, err := js.Publish(subject, []byte("OK")); err != nil {
					t.Fatalf("Unexpected publish error: %v", err)
				}
			}
		}

		checkSync := func(msgsTest, msgsM uint64) {
			t.Helper()
			checkFor(t, 40*time.Second, time.Second, func() error {
				if tsi, err := js.StreamInfo("TEST"); err != nil {
					return err
				} else if msi, err := js.StreamInfo("M"); err != nil {
					return err
				} else if tsi.State.Msgs != msgsTest {
					return fmt.Errorf("received %d msgs from TEST, expected %d", tsi.State.Msgs, msgsTest)
				} else if msi.State.Msgs != msgsM {
					return fmt.Errorf("received %d msgs from M, expected %d", msi.State.Msgs, msgsM)
				}
				return nil
			})
		}

		sendBatch("foo", 100)
		checkSync(100, expectedMsgCount)
		sendBatch("bar", 100)
		checkSync(200, expectedMsgCount)

		nc.Close()
		c.stopAll()
		c.restartAll()
		c.waitOnStreamLeader("$G", "TEST")
		c.waitOnStreamLeader("$G", "M")

		nc, js = jsClientConnect(t, c.randomServer())
		defer nc.Close()

		checkSync(200, expectedMsgCount)
		sendBatch("foo", 100)
		checkSync(300, expectedMsgCount)
		sendBatch("bar", 100)
		checkSync(400, expectedMsgCount)
	}
	t.Run("mirror-filter", func(t *testing.T) {
		test(t, true, true)
	})
	t.Run("mirror-nofilter", func(t *testing.T) {
		test(t, true, false)
	})
	t.Run("source-filter", func(t *testing.T) {
		test(t, false, true)
	})
	t.Run("source-nofilter", func(t *testing.T) {
		test(t, false, false)
	})
}

func TestJetStreamClusterMirrorAndSourcesFilteredConsumers(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterMirrorSourceImportsTempl, "MS5", 5)
	defer c.shutdown()

	// Client for API requests.
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Create Mirror now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:   "M",
		Mirror: &nats.StreamSource{Name: "TEST"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	dsubj := nats.NewInbox()
	nc.SubscribeSync(dsubj)
	nc.Flush()

	createConsumer := func(sn, fs string) {
		t.Helper()
		_, err = js.AddConsumer(sn, &nats.ConsumerConfig{DeliverSubject: dsubj, FilterSubject: fs})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	expectFail := func(sn, fs string) {
		t.Helper()
		_, err = js.AddConsumer(sn, &nats.ConsumerConfig{DeliverSubject: dsubj, FilterSubject: fs})
		if err == nil {
			t.Fatalf("Expected error but got none")
		}
	}

	createConsumer("M", "foo")
	createConsumer("M", "bar")
	createConsumer("M", "baz.foo")
	expectFail("M", ".")
	expectFail("M", ">.foo")

	// Make sure wider scoped subjects work as well.
	createConsumer("M", "*")
	createConsumer("M", ">")

	// Now do some sources.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "O1", Subjects: []string{"foo.*"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "O2", Subjects: []string{"bar.*"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create downstream now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:    "S",
		Sources: []*nats.StreamSource{{Name: "O1"}, {Name: "O2"}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	createConsumer("S", "foo.1")
	createConsumer("S", "bar.1")

	// Now cross account stuff.
	nc2, js2 := jsClientConnect(t, s, nats.UserInfo("rip", "pass"))
	defer nc2.Close()

	if _, err := js2.AddStream(&nats.StreamConfig{Name: "ORIGIN", Subjects: []string{"foo.*"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	cfg := StreamConfig{
		Name:    "SCA",
		Storage: FileStorage,
		Sources: []*StreamSource{{
			Name: "ORIGIN",
			External: &ExternalStream{
				ApiPrefix:     "RI.JS.API",
				DeliverPrefix: "RI.DELIVER.SYNC.SOURCES",
			},
		}},
	}
	req, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var scResp JSApiStreamCreateResponse
	if err := json.Unmarshal(resp.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo == nil || scResp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", scResp.Error)
	}

	// Externals skip the checks for now.
	createConsumer("SCA", "foo.1")
	createConsumer("SCA", "bar.1")
	createConsumer("SCA", "baz")
}

func TestJetStreamClusterCrossAccountMirrorsAndSources(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterMirrorSourceImportsTempl, "C1", 3)
	defer c.shutdown()

	// Create source stream under RI account.
	s := c.randomServer()
	nc, js := jsClientConnect(t, s, nats.UserInfo("rip", "pass"))
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// use large number to tease out FC issues
	toSend := 3000
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("TEST", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	nc2, js2 := jsClientConnect(t, s)
	defer nc2.Close()

	// Have to do this direct until we get Go client support.
	// Need to match jsClusterMirrorSourceImportsTempl imports.
	_, err := js2.AddStream(&nats.StreamConfig{
		Name: "MY_MIRROR_TEST",
		Mirror: &nats.StreamSource{
			Name: "TEST",
			External: &nats.ExternalStream{
				APIPrefix:     "RI.JS.API",
				DeliverPrefix: "RI.DELIVER.SYNC.MIRRORS",
			},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkFor(t, 20*time.Second, 500*time.Millisecond, func() error {
		si, err := js2.StreamInfo("MY_MIRROR_TEST")
		if err != nil {
			t.Fatalf("Could not retrieve stream info: %s", err)
		}
		if si.State.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected %d msgs, got state: %+v", toSend, si.State)
		}
		return nil
	})

	// Now do sources as well.
	_, err = js2.AddStream(&nats.StreamConfig{
		Name: "MY_SOURCE_TEST",
		Sources: []*nats.StreamSource{
			{
				Name: "TEST",
				External: &nats.ExternalStream{
					APIPrefix:     "RI.JS.API",
					DeliverPrefix: "RI.DELIVER.SYNC.SOURCES",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("MY_SOURCE_TEST")
		if err != nil {
			t.Fatalf("Could not retrieve stream info")
		}
		if si.State.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected %d msgs, got state: %+v", toSend, si.State)
		}
		return nil
	})

}

func TestJetStreamClusterFailMirrorsAndSources(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterMirrorSourceImportsTempl, "C1", 3)
	defer c.shutdown()

	// Create source stream under RI account.
	s := c.randomServer()
	nc, js := jsClientConnect(t, s, nats.UserInfo("rip", "pass"))
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2, Subjects: []string{"test.>"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	nc2, _ := jsClientConnect(t, s, nats.UserInfo("rip", "pass"))
	defer nc2.Close()

	testPrefix := func(testName string, id ErrorIdentifier, cfg StreamConfig) {
		t.Run(testName, func(t *testing.T) {
			req, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			resp, err := nc2.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var scResp JSApiStreamCreateResponse
			if err := json.Unmarshal(resp.Data, &scResp); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if scResp.Error == nil {
				t.Fatalf("Did expect an error but got none")
			} else if !IsNatsErr(scResp.Error, id) {
				t.Fatalf("Expected different error: %s", scResp.Error.Description)
			}
		})
	}

	testPrefix("mirror-bad-apiprefix", JSStreamExternalApiOverlapErrF, StreamConfig{
		Name:    "MY_MIRROR_TEST",
		Storage: FileStorage,
		Mirror: &StreamSource{
			Name: "TEST",
			External: &ExternalStream{
				ApiPrefix:     "$JS.API",
				DeliverPrefix: "here",
			},
		},
	})
	testPrefix("source-bad-apiprefix", JSStreamExternalApiOverlapErrF, StreamConfig{
		Name:    "MY_SOURCE_TEST",
		Storage: FileStorage,
		Sources: []*StreamSource{{
			Name: "TEST",
			External: &ExternalStream{
				ApiPrefix:     "$JS.API",
				DeliverPrefix: "here",
			},
		},
		},
	})
}

func TestJetStreamClusterConsumerDeliveredSyncReporting(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Retention: nats.WorkQueuePolicy,
		Subjects:  []string{"foo.*"},
		Replicas:  3,
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo.bar", "mc")
	require_NoError(t, err)

	// Make us match first, but not next 10.
	_, err = js.Publish("foo.bar", nil)
	require_NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err = js.Publish("foo.baz", nil)
		require_NoError(t, err)
	}

	opts := &JSzOptions{Accounts: true, Streams: true, Consumer: true}
	for _, s := range c.servers {
		jsz, err := s.Jsz(opts)
		require_NoError(t, err)
		ci := jsz.AccountDetails[0].Streams[0].Consumer[0]
		require_Equal(t, ci.Delivered.Consumer, 0)
		require_Equal(t, ci.Delivered.Stream, 0)
	}

	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 1)
	meta, err := msgs[0].Metadata()
	require_NoError(t, err)
	require_Equal(t, meta.Sequence.Consumer, 1)
	require_Equal(t, meta.Sequence.Stream, 1)

	// Allow some time for the state to propagate.
	maxWait := 200 * time.Millisecond
	time.Sleep(maxWait)

	for _, s := range c.servers {
		jsz, err := s.Jsz(opts)
		require_NoError(t, err)
		ci := jsz.AccountDetails[0].Streams[0].Consumer[0]
		require_Equal(t, ci.Delivered.Consumer, 1)
		require_Equal(t, ci.Delivered.Stream, 1)
	}

	// Now we want to make sure that jsz reporting will show the same
	// state, including delivered, which will have skipped to the end.
	// The skip can happen on several factors, but for here we just send
	// another pull request which we will let fail.
	_, err = sub.Fetch(1, nats.MaxWait(maxWait))
	require_Error(t, err)

	for _, s := range c.servers {
		jsz, err := s.Jsz(opts)
		require_NoError(t, err)
		ci := jsz.AccountDetails[0].Streams[0].Consumer[0]
		require_Equal(t, ci.Delivered.Consumer, 1)
		require_Equal(t, ci.Delivered.Stream, 11)
	}
}

// This is to test follower ack fill logic when pending not empty.
// There was a bug that would update p.Sequence in the stores (mem & file)
// that would cause the logic to fail. Redeliveries were required to trigger.
func TestJetStreamClusterConsumerAckSyncReporting(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Retention: nats.WorkQueuePolicy,
		Subjects:  []string{"foo.*"},
		Replicas:  3,
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo.bar", "mc", nats.AckWait(250*time.Millisecond))
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = js.Publish("foo.bar", nil)
		require_NoError(t, err)
	}

	// Wait for all servers to converge on the same state. Stream and consumer leader could be different.
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	msgs, err := sub.Fetch(10)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)
	// Let redeliveries kick in.
	time.Sleep(time.Second)
	msgs, err = sub.Fetch(10)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)

	// Randomize
	rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })

	dontAck := uint64(7)
	var skipped, last *nats.Msg
	for _, m := range msgs {
		meta, err := m.Metadata()
		require_NoError(t, err)
		if meta.Sequence.Stream == dontAck {
			skipped = m
			continue
		}
		if meta.Sequence.Stream == 10 {
			last = m
			continue
		}
		err = m.AckSync()
		require_NoError(t, err)
	}
	require_NotNil(t, skipped)
	require_NotNil(t, last)

	checkAckFloor := func(consumer, stream uint64) {
		opts := &JSzOptions{Accounts: true, Streams: true, Consumer: true}
		checkFor(t, 3*time.Second, 200*time.Millisecond, func() error {
			for _, s := range c.servers {
				jsz, err := s.Jsz(opts)
				if err != nil {
					return err
				}
				ci := jsz.AccountDetails[0].Streams[0].Consumer[0]
				if ci.AckFloor.Consumer != consumer {
					return fmt.Errorf("AckFloor.Consumer is not %d: %v", consumer, ci.AckFloor.Consumer)
				}
				if ci.AckFloor.Stream != stream {
					return fmt.Errorf("AckFloor.Stream is not %d: %v", stream, ci.AckFloor.Stream)
				}
			}
			return nil
		})
	}

	// Now we want to make sure that jsz reporting will show the same
	// state for ack floor.
	checkAckFloor(dontAck-1, dontAck-1)

	// Now ack the skipped message
	err = skipped.AckSync()
	require_NoError(t, err)
	checkAckFloor(9, 9)

	// Now ack the last message
	err = last.AckSync()
	require_NoError(t, err)
	checkAckFloor(20, 10)
}

func TestJetStreamClusterConsumerDeleteInterestPolicyMultipleConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Retention: nats.InterestPolicy,
		Subjects:  []string{"foo.*"},
		Replicas:  3,
	})
	require_NoError(t, err)

	// Make the first sequence high. We already protect against it but for extra sanity.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 100_000_000})
	require_NoError(t, err)

	// Create 2 consumers.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C1",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C2",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		js.PublishAsync("foo.bar", []byte("ok"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	expectedStreamMsgs := func(msgs uint64) {
		t.Helper()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST")
			if err != nil {
				return err
			}
			if si.State.Msgs != msgs {
				return fmt.Errorf("require uint64 equal, but got: %d != %d", si.State.Msgs, msgs)
			}
			return nil
		})
	}
	expectedStreamMsgs(100)

	sub, err := js.PullSubscribe("foo.bar", "C1")
	require_NoError(t, err)
	msgs, err := sub.Fetch(50)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 50)
	for _, m := range msgs {
		require_NoError(t, m.AckSync())
	}

	// Now delete second one and make sure accounting correct.
	err = js.DeleteConsumer("TEST", "C2")
	require_NoError(t, err)

	expectedStreamMsgs(50)
}

func TestJetStreamClusterConsumerAckNoneInterestPolicyShouldNotRetainAfterDelivery(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Retention: nats.InterestPolicy,
		Subjects:  []string{"foo.*"},
		Replicas:  3,
	})
	require_NoError(t, err)

	// Make the first sequence high. We already protect against it but for extra sanity.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 100_000_000})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C1",
		AckPolicy: nats.AckNonePolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		js.PublishAsync("foo.bar", []byte("ok"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 100)

	sub, err := js.PullSubscribe("foo.bar", "C1")
	require_NoError(t, err)
	msgs, err := sub.Fetch(100)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 100)

	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		si, err = js.StreamInfo("TEST")
		if err != nil {
			return err
		}
		if si.State.Msgs != 0 {
			return fmt.Errorf("require uint64 equal, but got: %d != 0", si.State.Msgs)
		}
		return nil
	})
}

func TestJetStreamClusterConsumerDeleteAckNoneInterestPolicyWithOthers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Retention: nats.InterestPolicy,
		Subjects:  []string{"foo.*"},
		Replicas:  3,
	})
	require_NoError(t, err)

	// Make the first sequence high. We already protect against it but for extra sanity.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 100_000_000})
	require_NoError(t, err)

	// Create 2 consumers.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C1",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C2",
		AckPolicy: nats.AckNonePolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		js.PublishAsync("foo.bar", []byte("ok"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	expectedStreamMsgs := func(msgs uint64) {
		t.Helper()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST")
			if err != nil {
				return err
			}
			if si.State.Msgs != msgs {
				return fmt.Errorf("require uint64 equal, but got: %d != %d", si.State.Msgs, msgs)
			}
			return nil
		})
	}
	expectedStreamMsgs(100)

	sub, err := js.PullSubscribe("foo.bar", "C1")
	require_NoError(t, err)
	msgs, err := sub.Fetch(100)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 100)
	for _, m := range msgs {
		require_NoError(t, m.AckSync())
	}
	// AckNone will hold.
	expectedStreamMsgs(100)

	// Now delete second one and make sure accounting correct.
	err = js.DeleteConsumer("TEST", "C2")
	require_NoError(t, err)

	expectedStreamMsgs(0)
}

// Make sure to not allow non-system accounts to move meta leader.
func TestJetStreamClusterMetaStepdownFromNonSysAccount(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	ml := c.leader()

	_, err := nc.Request(JSApiLeaderStepDown, nil, time.Second)
	require_Error(t, err, nats.ErrTimeout)

	// Make sure we did not move.
	c.waitOnLeader()
	require_Equal(t, ml, c.leader())

	// System user can move it.
	snc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	resp, err := snc.Request(JSApiLeaderStepDown, nil, time.Second)
	require_NoError(t, err)

	var sdr JSApiLeaderStepDownResponse
	require_NoError(t, json.Unmarshal(resp.Data, &sdr))
	require_True(t, sdr.Success)
	require_Equal(t, sdr.Error, nil)

	// Make sure we did move this time.
	c.waitOnLeader()
	require_NotEqual(t, ml, c.leader())
}

func TestJetStreamClusterMaxDeliveriesOnInterestStreams(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.*"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	sub1, err := js.PullSubscribe("foo.*", "c1", nats.AckWait(10*time.Millisecond), nats.MaxDeliver(1))
	require_NoError(t, err)

	sub2, err := js.PullSubscribe("foo.*", "c2", nats.AckWait(10*time.Millisecond), nats.MaxDeliver(1))
	require_NoError(t, err)

	_, err = js.Publish("foo.bar", []byte("HELLO"))
	require_NoError(t, err)

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 1)

	msgs, err := sub1.Fetch(1)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 1)

	msgs, err = sub2.Fetch(1)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 1)

	// Wait for redelivery to both consumers which will do nothing.
	time.Sleep(250 * time.Millisecond)

	// Now check that stream and consumer infos are correct.
	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	// Messages that are skipped due to max deliveries should NOT remove messages.
	require_Equal(t, si.State.Msgs, 1)
	require_Equal(t, si.State.Consumers, 2)

	for _, cname := range []string{"c1", "c2"} {
		ci, err := js.ConsumerInfo("TEST", cname)
		require_NoError(t, err)
		require_Equal(t, ci.Delivered.Consumer, 1)
		require_Equal(t, ci.Delivered.Stream, 1)
		require_Equal(t, ci.AckFloor.Consumer, 0)
		require_Equal(t, ci.AckFloor.Stream, 0)
		require_Equal(t, ci.NumAckPending, 0)
		require_Equal(t, ci.NumRedelivered, 0)
		require_Equal(t, ci.NumPending, 0)
	}
}

func TestJetStreamClusterMetaRecoveryUpdatesDeletesConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	js := c.leader().getJetStream()

	create := []*Entry{
		{EntryNormal, encodeAddStreamAssignment(&streamAssignment{
			Config: &StreamConfig{Name: "TEST", Storage: FileStorage},
		})},
		{EntryNormal, encodeAddConsumerAssignment(&consumerAssignment{
			Stream: "TEST",
			Config: &ConsumerConfig{Name: "consumer"},
		})},
	}

	delete := []*Entry{
		{EntryNormal, encodeDeleteStreamAssignment(&streamAssignment{
			Config: &StreamConfig{Name: "TEST", Storage: FileStorage},
		})},
	}

	// Need to be recovering so that we accumulate recoveryUpdates.
	js.setMetaRecovering()
	ru := &recoveryUpdates{
		removeStreams:   make(map[string]*streamAssignment),
		removeConsumers: make(map[string]map[string]*consumerAssignment),
		addStreams:      make(map[string]*streamAssignment),
		updateStreams:   make(map[string]*streamAssignment),
		updateConsumers: make(map[string]map[string]*consumerAssignment),
	}

	// Push recovery entries that create the stream & consumer.
	_, _, _, err := js.applyMetaEntries(create, ru)
	require_NoError(t, err)
	require_Len(t, len(ru.updateConsumers), 1)

	// Now push another recovery entry that deletes the stream. The
	// entry that creates the consumer should now be gone.
	_, _, _, err = js.applyMetaEntries(delete, ru)
	require_NoError(t, err)
	require_Len(t, len(ru.removeStreams), 1)
	require_Len(t, len(ru.updateConsumers), 0)
}

func TestJetStreamClusterMetaRecoveryRecreateFileStreamAsMemory(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	js := c.leader().getJetStream()

	createFileStream := []*Entry{
		{EntryNormal, encodeAddStreamAssignment(&streamAssignment{
			Config: &StreamConfig{Name: "TEST", Storage: FileStorage},
		})},
	}

	deleteFileStream := []*Entry{
		{EntryNormal, encodeDeleteStreamAssignment(&streamAssignment{
			Config: &StreamConfig{Name: "TEST", Storage: FileStorage},
		})},
	}

	createMemoryStream := []*Entry{
		{EntryNormal, encodeAddStreamAssignment(&streamAssignment{
			Config: &StreamConfig{Name: "TEST", Storage: FileStorage},
		})},
	}

	createConsumer := []*Entry{
		{EntryNormal, encodeAddConsumerAssignment(&consumerAssignment{
			Stream: "TEST",
			Config: &ConsumerConfig{Name: "consumer"},
		})},
	}

	// Need to be recovering so that we accumulate recoveryUpdates.
	js.setMetaRecovering()
	ru := &recoveryUpdates{
		removeStreams:   make(map[string]*streamAssignment),
		removeConsumers: make(map[string]map[string]*consumerAssignment),
		addStreams:      make(map[string]*streamAssignment),
		updateStreams:   make(map[string]*streamAssignment),
		updateConsumers: make(map[string]map[string]*consumerAssignment),
	}

	// We created a file-based stream first, but deleted it shortly after.
	_, _, _, err := js.applyMetaEntries(createFileStream, ru)
	require_NoError(t, err)
	require_Len(t, len(ru.addStreams), 1)
	require_Len(t, len(ru.removeStreams), 0)

	// Now push another recovery entry that deletes the stream.
	// The file-based stream should not have been created.
	_, _, _, err = js.applyMetaEntries(deleteFileStream, ru)
	require_NoError(t, err)
	require_Len(t, len(ru.addStreams), 0)
	require_Len(t, len(ru.removeStreams), 1)

	// Now stage a memory-based stream to be created.
	_, _, _, err = js.applyMetaEntries(createMemoryStream, ru)
	require_NoError(t, err)
	require_Len(t, len(ru.addStreams), 1)
	require_Len(t, len(ru.removeStreams), 0)
	require_Len(t, len(ru.updateConsumers), 0)

	// Also create a consumer on that memory-based stream.
	_, _, _, err = js.applyMetaEntries(createConsumer, ru)
	require_NoError(t, err)
	require_Len(t, len(ru.addStreams), 1)
	require_Len(t, len(ru.removeStreams), 0)
	require_Len(t, len(ru.updateConsumers), 1)
}

func TestJetStreamClusterMetaRecoveryConsumerCreateAndRemove(t *testing.T) {
	tests := []struct {
		title                       string
		encodeAddConsumerAssignment func(ca *consumerAssignment) []byte
	}{
		{title: "simple", encodeAddConsumerAssignment: encodeAddConsumerAssignment},
		{title: "compressed", encodeAddConsumerAssignment: encodeAddConsumerAssignmentCompressed},
	}
	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			js := c.leader().getJetStream()

			ca := &consumerAssignment{Stream: "TEST", Name: "consumer"}
			createConsumer := []*Entry{{EntryNormal, test.encodeAddConsumerAssignment(ca)}}
			deleteConsumer := []*Entry{{EntryNormal, encodeDeleteConsumerAssignment(ca)}}

			// Need to be recovering so that we accumulate recoveryUpdates.
			js.setMetaRecovering()
			ru := &recoveryUpdates{
				removeStreams:   make(map[string]*streamAssignment),
				removeConsumers: make(map[string]map[string]*consumerAssignment),
				addStreams:      make(map[string]*streamAssignment),
				updateStreams:   make(map[string]*streamAssignment),
				updateConsumers: make(map[string]map[string]*consumerAssignment),
			}

			// Creating the consumer should append to update consumers list.
			_, _, _, err := js.applyMetaEntries(createConsumer, ru)
			require_NoError(t, err)
			require_Len(t, len(ru.updateConsumers[":TEST"]), 1)
			require_Len(t, len(ru.removeConsumers), 0)

			// Deleting the consumer should append to remove consumers list and remove from update list.
			_, _, _, err = js.applyMetaEntries(deleteConsumer, ru)
			require_NoError(t, err)
			require_Len(t, len(ru.removeConsumers[":TEST"]), 1)
			require_Len(t, len(ru.updateConsumers[":TEST"]), 0)

			// When re-creating the consumer, add to update list and remove from remove list.
			_, _, _, err = js.applyMetaEntries(createConsumer, ru)
			require_NoError(t, err)
			require_Len(t, len(ru.updateConsumers[":TEST"]), 1)
			require_Len(t, len(ru.removeConsumers[":TEST"]), 0)
		})
	}
}

// Make sure if we received acks that are out of bounds, meaning past our
// last sequence or before our first that they are ignored and errored if applicable.
func TestJetStreamClusterConsumerAckOutOfBounds(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.*"},
		Retention: nats.WorkQueuePolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = js.Publish("foo.bar", []byte("OK"))
		require_NoError(t, err)
	}

	sub, err := js.PullSubscribe("foo.*", "C")
	require_NoError(t, err)

	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 1)
	msgs[0].AckSync()

	// Now ack way past the last sequence.
	_, err = nc.Request("$JS.ACK.TEST.C.1.10000000000.0.0.0", nil, 250*time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)

	// Make sure that now changes happened to our state.
	ci, err := js.ConsumerInfo("TEST", "C")
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Consumer, 1)
	require_Equal(t, ci.Delivered.Stream, 1)
	require_Equal(t, ci.AckFloor.Consumer, 1)
	require_Equal(t, ci.AckFloor.Stream, 1)

	s := c.consumerLeader("$G", "TEST", "C")
	s.Shutdown()
	s.WaitForShutdown()
	c.restartServer(s)
	c.waitOnConsumerLeader(globalAccountName, "TEST", "C")

	// Confirm new leader has same state for delivered and ack floor.
	ci, err = js.ConsumerInfo("TEST", "C")
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Consumer, 1)
	require_Equal(t, ci.Delivered.Stream, 1)
	require_Equal(t, ci.AckFloor.Consumer, 1)
	require_Equal(t, ci.AckFloor.Stream, 1)
}

func TestJetStreamClusterCatchupLoadNextMsgTooManyDeletes(t *testing.T) {
	tests := []struct {
		title          string
		catchupRequest *streamSyncRequest
		setup          func(js nats.JetStreamContext)
		assert         func(sub *nats.Subscription)
	}{
		{
			title: "within-delete-gap",
			setup: func(js nats.JetStreamContext) {},
		},
		{
			title: "EOF",
			setup: func(js nats.JetStreamContext) {
				err := js.DeleteMsg("TEST", 100)
				require_NoError(t, err)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo", "bar"},
				Replicas: 3,
			})
			require_NoError(t, err)

			// Starts and ends with subject "foo", we'll purge so there's a large gap of deletes in the middle.
			// This should force runCatchup to use LoadNextMsg instead of LoadMsg.
			for i := 0; i < 100; i++ {
				subject := "bar"
				if i == 0 || i == 99 {
					subject = "foo"
				}
				_, err = js.Publish(subject, nil)
				require_NoError(t, err)
			}
			err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Subject: "bar"})
			require_NoError(t, err)

			// Optionally run some extra setup.
			test.setup(js)

			// Reconnect to stream leader.
			l := c.streamLeader(globalAccountName, "TEST")
			nc.Close()
			nc, _ = jsClientConnect(t, l, nats.UserInfo("admin", "s3cr3t!"))
			defer nc.Close()

			// Setup wiretap and grab stream.
			sendSubject := "test-wiretap"
			sub, err := nc.SubscribeSync(sendSubject)
			require_NoError(t, err)
			err = nc.Flush() // Must flush, otherwise our subscription could be too late.
			require_NoError(t, err)
			acc, err := l.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)

			// Run custom catchup request and the test's asserts.
			sreq := &streamSyncRequest{Peer: "peer", FirstSeq: 5, LastSeq: 5, DeleteRangesOk: true}
			require_True(t, mset.srv.startGoRoutine(func() { mset.runCatchup(sendSubject, sreq) }))

			// Our first message should be a skip msg.
			msg, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
			require_Equal(t, entryOp(msg.Data[0]), streamMsgOp)
			subj, _, _, _, seq, ts, _, err := decodeStreamMsg(msg.Data[1:])
			require_NoError(t, err)
			require_Equal(t, seq, 5)
			require_Equal(t, subj, _EMPTY_)
			require_Equal(t, ts, 0)

			// And end with EOF.
			msg, err = sub.NextMsg(time.Second)
			require_NoError(t, err)
			require_Len(t, len(msg.Data), 0)
		})
	}
}

func TestJetStreamClusterCatchupMustStallWhenBehindOnApplies(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	// Reconnect to stream leader.
	l := c.streamLeader(globalAccountName, "TEST")
	nc.Close()
	nc, _ = jsClientConnect(t, l, nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	// Setup wiretap and grab stream.
	sendSubject := "test-wiretap"
	sub, err := nc.SubscribeSync(sendSubject)
	require_NoError(t, err)
	err = nc.Flush() // Must flush, otherwise our subscription could be too late.
	require_NoError(t, err)
	acc, err := l.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	// We have a message at sequence 1, so expect a successful catchup.
	sreq1 := &streamSyncRequest{Peer: "peer", FirstSeq: 1, LastSeq: 1, DeleteRangesOk: true}
	require_True(t, mset.srv.startGoRoutine(func() { mset.runCatchup(sendSubject, sreq1) }))
	// Expect the message at sequence 1.
	msg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_Equal(t, entryOp(msg.Data[0]), streamMsgOp)
	subj, _, _, _, seq, _, _, err := decodeStreamMsg(msg.Data[1:])
	require_NoError(t, err)
	require_Equal(t, seq, 1)
	require_Equal(t, subj, "foo")
	// And end with EOF.
	msg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_Len(t, len(msg.Data), 0)

	// Add one additional entry into the log that's not applied yet.
	n := mset.node.(*raft)
	n.Lock()
	ae := n.buildAppendEntry(nil)
	err = n.storeToWAL(ae)
	n.Unlock()
	index, commit, applied := n.Progress()
	require_NoError(t, err)
	require_LessThan(t, applied, index)
	require_Equal(t, commit, applied)
	// We have a message at sequence 1, but we haven't applied as many append entries.
	// We can't fulfill the request right now as we don't know yet if
	// that message will be deleted as part of upcoming append entries.
	sreq2 := &streamSyncRequest{Peer: "peer", FirstSeq: 1, LastSeq: 1, DeleteRangesOk: true, MinApplied: index}
	require_True(t, mset.srv.startGoRoutine(func() { mset.runCatchup(sendSubject, sreq2) }))
	_, err = sub.NextMsg(time.Second)
	require_Error(t, err, nats.ErrTimeout)

	// We have a message at sequence 1, but we haven't applied as many append entries.
	// Also, we seem to have a log that doesn't contain enough entries, even though we became leader.
	// Something has already gone wrong and got the logs to desync.
	// Value availability here and just fulfill the request.
	sreq3 := &streamSyncRequest{Peer: "peer", FirstSeq: 1, LastSeq: 1, DeleteRangesOk: true, MinApplied: 100}
	require_True(t, mset.srv.startGoRoutine(func() { mset.runCatchup(sendSubject, sreq3) }))
	// Expect the message at sequence 1.
	msg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_Equal(t, entryOp(msg.Data[0]), streamMsgOp)
	subj, _, _, _, seq, _, _, err = decodeStreamMsg(msg.Data[1:])
	require_NoError(t, err)
	require_Equal(t, seq, 1)
	require_Equal(t, subj, "foo")
	// And end with EOF.
	msg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_Len(t, len(msg.Data), 0)
}

func TestJetStreamClusterConsumerInfoAfterCreate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nl := c.randomNonLeader()
	nc, js := jsClientConnect(t, nl)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	}
	si, err := js.AddStream(cfg)
	require_NoError(t, err)

	// We want to ensure the consumer can be created and be applied.
	// On the non-meta-leader server we'll pause applies, so need to make
	// sure the consumer is not created on that server.
	if si.Cluster.Leader == nl.Name() {
		ml := c.leader()
		jreq, err := json.Marshal(&JSApiLeaderStepdownRequest{Placement: &Placement{Preferred: ml.Name()}})
		require_NoError(t, err)
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), jreq, time.Second)
		require_NoError(t, err)
		var sdr JSApiLeaderStepDownResponse
		require_NoError(t, json.Unmarshal(resp.Data, &sdr))
	}

	// Wait some time for the leader to settle.
	time.Sleep(500 * time.Millisecond)

	// Scale down to ensure the consumer gets created on this server.
	cfg.Replicas = 1
	si, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	require_NotEqual(t, si.Cluster.Leader, nl.Name())

	// We pause applies for the server we're connected to.
	// This is fine for the RAFT log and allowing the consumer to be created,
	// but we will not be able to apply the consumer assignment for some time.
	mjs := nl.getJetStream()
	require_NotNil(t, js)
	mg := mjs.getMetaGroup()
	require_NotNil(t, mg)
	err = mg.(*raft).PauseApply()
	require_NoError(t, err)

	// Add consumer.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)

	// Consumer info should not fail, this server should not short-circuit because
	// it was not able to apply the consumer assignment.
	_, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)

	// Resume applies.
	mg.(*raft).ResumeApply()

	// Check consumer info still works.
	_, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
}

func TestJetStreamClusterStreamUpscalePeersAfterDownscale(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	checkPeerSet := func() {
		t.Helper()
		checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
			for _, s := range c.servers {
				acc, err := s.lookupAccount(globalAccountName)
				if err != nil {
					return err
				}
				mset, err := acc.lookupStream("TEST")
				if err != nil {
					return err
				}
				peers := mset.raftNode().Peers()
				if len(peers) != 5 {
					return fmt.Errorf("expected 5 peers, got %d", len(peers))
				}
			}
			return nil
		})
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 5,
	})
	require_NoError(t, err)

	checkPeerSet()

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 5,
	})
	require_NoError(t, err)

	checkPeerSet()
}

func TestJetStreamClusterClearAllPreAcksOnRemoveMsg(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	// Wait for all servers to converge on the same state.
	checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	// Register pre-acks on all servers.
	// Normally this can't happen as the stream leader will have the message that's acked available, just for testing.
	for _, s := range c.servers {
		acc, err := s.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		o := mset.lookupConsumer("CONSUMER")
		require_NotNil(t, o)

		// Register pre-acks for the 3 messages.
		mset.registerPreAckLock(o, 1)
		mset.registerPreAckLock(o, 2)
		mset.registerPreAckLock(o, 3)
	}

	// Check there's an expected amount of pre-acks, and there are no pre-acks for the given sequence.
	checkPreAcks := func(seq uint64, expected int) {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				acc, err := s.lookupAccount(globalAccountName)
				if err != nil {
					return err
				}
				mset, err := acc.lookupStream("TEST")
				if err != nil {
					return err
				}
				mset.mu.RLock()
				numPreAcks := len(mset.preAcks)
				numSeqPreAcks := len(mset.preAcks[seq])
				mset.mu.RUnlock()
				if numPreAcks != expected {
					return fmt.Errorf("expected %d pre-acks, got %d", expected, numPreAcks)
				}
				if seq > 0 && numSeqPreAcks != 0 {
					return fmt.Errorf("expected 0 pre-acks for seq %d, got %d", seq, numSeqPreAcks)
				}
			}
			return nil
		})
	}
	// Check all pre-acks were registered.
	checkPreAcks(0, 3)

	// Deleting the message should clear the pre-ack.
	err = js.DeleteMsg("TEST", 1)
	require_NoError(t, err)
	checkPreAcks(1, 2)

	// Erasing the message should clear the pre-ack.
	err = js.SecureDeleteMsg("TEST", 2)
	require_NoError(t, err)
	checkPreAcks(2, 1)

	// Purging should clear all pre-acks below the purged floor.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 4})
	require_NoError(t, err)
	checkPreAcks(3, 0)
}

func TestJetStreamClusterStreamHealthCheckMustNotRecreate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	waitForStreamAssignments := func() {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				js := s.getJetStream()
				js.mu.RLock()
				sa := js.streamAssignment(globalAccountName, "TEST")
				js.mu.RUnlock()
				if sa == nil {
					return fmt.Errorf("stream assignment not found on %s", s.Name())
				}
			}
			return nil
		})
	}
	waitForNoStreamAssignments := func() {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				js := s.getJetStream()
				js.mu.RLock()
				sa := js.streamAssignment(globalAccountName, "TEST")
				js.mu.RUnlock()
				if sa != nil {
					return fmt.Errorf("stream assignment still available on %s", s.Name())
				}
			}
			return nil
		})
	}
	getStreamAssignment := func(rs *Server) (*jetStream, *Account, *streamAssignment, *stream) {
		acc, err := rs.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NotNil(t, err)

		sjs := rs.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()

		sas := sjs.cluster.streams[globalAccountName]
		require_True(t, sas != nil)
		sa := sas["TEST"]
		require_True(t, sa != nil)
		sa.Created = time.Time{}
		return sjs, acc, sa, mset
	}
	checkNodeIsClosed := func(sa *streamAssignment) {
		t.Helper()
		require_True(t, sa.Group != nil)
		rg := sa.Group
		require_True(t, rg.node != nil)
		n := rg.node
		require_Equal(t, n.State(), Closed)
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	waitForStreamAssignments()

	// We manually stop the RAFT node and ensure it doesn't get restarted.
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	sjs, acc, sa, mset := getStreamAssignment(rs)
	require_True(t, sa.Group != nil)
	rg := sa.Group
	require_True(t, rg.node != nil)
	n := rg.node
	n.Stop()
	n.WaitForStop()

	// We wait for the monitor to exit, so we can set the flag back manually.
	checkFor(t, 5*time.Second, time.Second, func() error {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
		if mset.inMonitor {
			return errors.New("waiting for monitor to stop")
		}
		return nil
	})
	mset.mu.Lock()
	mset.inMonitor = true
	mset.mu.Unlock()

	// The RAFT node should be closed. Checking health must not change that.
	// Simulates a race condition where we're shutting down, but we're still in the stream monitor.
	checkNodeIsClosed(sa)
	sjs.isStreamHealthy(acc, sa)
	checkNodeIsClosed(sa)

	err = js.DeleteStream("TEST")
	require_NoError(t, err)
	waitForNoStreamAssignments()

	// Underlying layer would be aware the health check made a copy.
	// So we sneakily set these values back, which simulates a race condition where
	// the health check is called while the deletion is in progress. This could happen
	// depending on how the locks are used.
	sjs.mu.Lock()
	sjs.cluster.streams = make(map[string]map[string]*streamAssignment)
	sjs.cluster.streams[globalAccountName] = make(map[string]*streamAssignment)
	sjs.cluster.streams[globalAccountName]["TEST"] = sa
	sa.Group.node = n
	sjs.mu.Unlock()

	// The underlying stream has been deleted. Checking health must not recreate the stream.
	checkNodeIsClosed(sa)
	sjs.isStreamHealthy(acc, sa)
	checkNodeIsClosed(sa)
}

func TestJetStreamClusterStreamHealthCheckMustNotDeleteEarly(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	waitForStreamAssignments := func() {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				js := s.getJetStream()
				js.mu.RLock()
				sa := js.streamAssignment(globalAccountName, "TEST")
				js.mu.RUnlock()
				if sa == nil {
					return fmt.Errorf("stream assignment not found on %s", s.Name())
				}
			}
			return nil
		})
	}
	getStreamAssignment := func(rs *Server) (*jetStream, *Account, *streamAssignment, *stream) {
		acc, err := rs.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NotNil(t, err)

		sjs := rs.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()

		sas := sjs.cluster.streams[globalAccountName]
		require_True(t, sas != nil)
		sa := sas["TEST"]
		require_True(t, sa != nil)
		sa.Created = time.Time{}
		return sjs, acc, sa, mset
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	waitForStreamAssignments()

	// We manually clear the node on the stream.
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	sjs, acc, sa, mset := getStreamAssignment(rs)
	mset.mu.Lock()
	mset.node = nil
	mset.mu.Unlock()
	sjs.mu.Lock()
	group := sa.Group
	if group == nil {
		sjs.mu.Unlock()
		t.Fatal("sa.Group not initialized")
	}
	node := group.node
	if node == nil {
		sjs.mu.Unlock()
		t.Fatal("sa.Group.node not initialized")
	}
	sjs.mu.Unlock()

	// The health check gets the Raft node of the assignment and checks it against the
	// Raft node of the stream. We simulate a race condition where the stream's Raft node
	// is not yet initialized. The health check MUST NOT delete the node.
	sjs.isStreamHealthy(acc, sa)
	require_Equal(t, node.State(), Follower)
}

func TestJetStreamClusterStreamHealthCheckOnlyReportsSkew(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	waitForStreamAssignments := func() {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				js := s.getJetStream()
				js.mu.RLock()
				sa := js.streamAssignment(globalAccountName, "TEST")
				js.mu.RUnlock()
				if sa == nil {
					return fmt.Errorf("stream assignment not found on %s", s.Name())
				}
			}
			return nil
		})
	}
	getStreamAssignment := func(rs *Server) (*jetStream, *Account, *streamAssignment, *stream) {
		acc, err := rs.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NotNil(t, err)

		sjs := rs.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()

		sas := sjs.cluster.streams[globalAccountName]
		require_True(t, sas != nil)
		sa := sas["TEST"]
		require_True(t, sa != nil)
		sa.Created = time.Time{}
		return sjs, acc, sa, mset
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	waitForStreamAssignments()

	// Confirm the stream and assignment Raft nodes are equal.
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	sjs, acc, sa, mset := getStreamAssignment(rs)
	mset.mu.RLock()
	msetNode := mset.node
	mset.mu.RUnlock()
	sjs.mu.Lock()
	group := sa.Group
	if group == nil {
		sjs.mu.Unlock()
		t.Fatal("sa.Group not initialized")
	}
	node := group.node
	if node == nil {
		sjs.mu.Unlock()
		t.Fatal("sa.Group.node not initialized")
	}
	sjs.mu.Unlock()
	require_Equal(t, msetNode, node)

	// Simulate stopping and restarting a new instance.
	node.Stop()
	node.WaitForStop()
	node, err = sjs.createRaftGroup(globalAccountName, group, false, FileStorage, pprofLabels{})
	require_NoError(t, err)
	require_NotEqual(t, node.State(), Closed)

	// The health check gets the Raft node of the assignment and checks it against the
	// Raft node of the stream. We simulate a race condition where the assignment's Raft node
	// is re-newed, but the stream's node is still the old instance.
	// The health check MUST NOT delete the node.
	require_Error(t, sjs.isStreamHealthy(acc, sa), errors.New("cluster node skew detected"))
	require_NotEqual(t, node.State(), Closed)
}

func TestJetStreamClusterConsumerHealthCheckMustNotRecreate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	waitForConsumerAssignments := func() {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				if s.getJetStream().consumerAssignment(globalAccountName, "TEST", "CONSUMER") == nil {
					return fmt.Errorf("stream assignment not found on %s", s.Name())
				}
			}
			return nil
		})
	}
	waitForNoConsumerAssignments := func() {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				if s.getJetStream().consumerAssignment(globalAccountName, "TEST", "CONSUMER") != nil {
					return fmt.Errorf("stream assignment still available on %s", s.Name())
				}
			}
			return nil
		})
	}
	getConsumerAssignment := func(rs *Server) (*jetStream, *streamAssignment, *consumerAssignment, *stream) {
		acc, err := rs.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NotNil(t, err)

		sjs := rs.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()

		sas := sjs.cluster.streams[globalAccountName]
		require_True(t, sas != nil)
		sa := sas["TEST"]
		require_True(t, sa != nil)
		ca := sa.consumers["CONSUMER"]
		require_True(t, ca != nil)
		ca.Created = time.Time{}
		return sjs, sa, ca, mset
	}
	checkNodeIsClosed := func(ca *consumerAssignment) {
		t.Helper()
		require_True(t, ca.Group != nil)
		rg := ca.Group
		require_True(t, rg.node != nil)
		n := rg.node
		require_Equal(t, n.State(), Closed)
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Retention: nats.InterestPolicy, // Replicated consumers by default
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)
	waitForConsumerAssignments()

	// We manually stop the RAFT node and ensure it doesn't get restarted.
	rs := c.randomNonConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	sjs, sa, ca, mset := getConsumerAssignment(rs)
	require_True(t, ca.Group != nil)
	rg := ca.Group
	require_True(t, rg.node != nil)
	n := rg.node
	n.Stop()
	n.WaitForStop()

	// The RAFT node should be closed. Checking health must not change that.
	// Simulates a race condition where we're shutting down.
	checkNodeIsClosed(ca)
	sjs.isConsumerHealthy(mset, "CONSUMER", ca)
	checkNodeIsClosed(ca)

	// We create a new RAFT group, the health check should detect this skew and restart.
	_, err = sjs.createRaftGroup(globalAccountName, ca.Group, false, MemoryStorage, pprofLabels{})
	require_NoError(t, err)
	sjs.mu.Lock()
	// We set creating to now, since previously it would delete all data but NOT restart if created within <10s.
	ca.Created = time.Now()
	// Setting ca.pending, since a side effect of js.processConsumerAssignment is that it resets it.
	ca.pending = true
	sjs.mu.Unlock()
	sjs.isConsumerHealthy(mset, "CONSUMER", ca)
	require_True(t, ca.pending)

	err = js.DeleteConsumer("TEST", "CONSUMER")
	require_NoError(t, err)
	waitForNoConsumerAssignments()

	// Underlying layer would be aware the health check made a copy.
	// So we sneakily set these values back, which simulates a race condition where
	// the health check is called while the deletion is in progress. This could happen
	// depending on how the locks are used.
	sjs.mu.Lock()
	sjs.cluster.streams = make(map[string]map[string]*streamAssignment)
	sjs.cluster.streams[globalAccountName] = make(map[string]*streamAssignment)
	sjs.cluster.streams[globalAccountName]["TEST"] = sa
	ca.Created = time.Time{}
	ca.Group.node = n
	ca.deleted = false
	sjs.mu.Unlock()

	// The underlying consumer has been deleted. Checking health must not recreate the consumer.
	checkNodeIsClosed(ca)
	sjs.isConsumerHealthy(mset, "CONSUMER", ca)
	checkNodeIsClosed(ca)
}

func TestJetStreamClusterConsumerHealthCheckMustNotDeleteEarly(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	waitForConsumerAssignments := func() {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				if s.getJetStream().consumerAssignment(globalAccountName, "TEST", "CONSUMER") == nil {
					return fmt.Errorf("stream assignment not found on %s", s.Name())
				}
			}
			return nil
		})
	}
	getConsumerAssignment := func(rs *Server) (*jetStream, *consumerAssignment, *stream, *consumer) {
		acc, err := rs.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NotNil(t, err)
		o := mset.lookupConsumer("CONSUMER")

		sjs := rs.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()

		sas := sjs.cluster.streams[globalAccountName]
		require_True(t, sas != nil)
		sa := sas["TEST"]
		require_True(t, sa != nil)
		ca := sa.consumers["CONSUMER"]
		require_True(t, ca != nil)
		ca.Created = time.Time{}
		return sjs, ca, mset, o
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Retention: nats.InterestPolicy, // Replicated consumers by default
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)
	waitForConsumerAssignments()

	// We manually clear the node on the consumer.
	rs := c.randomNonConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	sjs, ca, mset, o := getConsumerAssignment(rs)
	o.mu.Lock()
	o.node = nil
	o.mu.Unlock()
	sjs.mu.Lock()
	group := ca.Group
	if group == nil {
		sjs.mu.Unlock()
		t.Fatal("ca.Group not initialized")
	}
	node := group.node
	if node == nil {
		sjs.mu.Unlock()
		t.Fatal("ca.Group.node not initialized")
	}
	sjs.mu.Unlock()

	// The health check gets the Raft node of the assignment and checks it against the
	// Raft node of the consumer. We simulate a race condition where the consumer's Raft node
	// is not yet initialized. The health check MUST NOT delete the node.
	sjs.isConsumerHealthy(mset, "CONSUMER", ca)
	require_Equal(t, node.State(), Follower)
}

func TestJetStreamClusterConsumerHealthCheckOnlyReportsSkew(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	waitForConsumerAssignments := func() {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				if s.getJetStream().consumerAssignment(globalAccountName, "TEST", "CONSUMER") == nil {
					return fmt.Errorf("stream assignment not found on %s", s.Name())
				}
			}
			return nil
		})
	}
	getConsumerAssignment := func(rs *Server) (*jetStream, *consumerAssignment, *stream, *consumer) {
		acc, err := rs.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NotNil(t, err)
		o := mset.lookupConsumer("CONSUMER")

		sjs := rs.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()

		sas := sjs.cluster.streams[globalAccountName]
		require_True(t, sas != nil)
		sa := sas["TEST"]
		require_True(t, sa != nil)
		ca := sa.consumers["CONSUMER"]
		require_True(t, ca != nil)
		ca.Created = time.Time{}
		return sjs, ca, mset, o
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Retention: nats.InterestPolicy, // Replicated consumers by default
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)
	waitForConsumerAssignments()

	// Confirm the consumer and assignment Raft nodes are equal.
	rs := c.randomNonConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	sjs, ca, mset, o := getConsumerAssignment(rs)
	o.mu.RLock()
	oNode := o.node
	o.mu.RUnlock()
	sjs.mu.Lock()
	group := ca.Group
	if group == nil {
		sjs.mu.Unlock()
		t.Fatal("ca.Group not initialized")
	}
	node := group.node
	if node == nil {
		sjs.mu.Unlock()
		t.Fatal("ca.Group.node not initialized")
	}
	sjs.mu.Unlock()
	require_Equal(t, oNode, node)

	// Simulate stopping and restarting a new instance.
	node.Stop()
	node.WaitForStop()
	node, err = sjs.createRaftGroup(globalAccountName, group, false, FileStorage, pprofLabels{})
	require_NoError(t, err)
	require_NotEqual(t, node.State(), Closed)

	// The health check gets the Raft node of the assignment and checks it against the
	// Raft node of the consumer. We simulate a race condition where the assignment's Raft node
	// is re-newed, but the consumer's node is still the old instance.
	// The health check MUST NOT delete the node.
	require_Error(t, sjs.isConsumerHealthy(mset, "CONSUMER", ca), errors.New("cluster node skew detected"))
	require_NotEqual(t, node.State(), Closed)
}

// https://github.com/nats-io/nats-server/issues/7149
func TestJetStreamClusterConsumerHealthCheckDeleted(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)

	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, cl)
	mset, err := cl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	sjs := cl.getJetStream()
	sjs.mu.Lock()
	ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	if ca == nil {
		sjs.mu.Unlock()
		t.Fatal("ca not found")
	}
	// Reset created time, simulating the consumer existed already for a while.
	ca.Created = time.Time{}
	sjs.mu.Unlock()

	// The health check gathers all assignments and does checking after.
	// If the consumer was deleted in the meantime, it should not report an error.
	require_NoError(t, js.DeleteConsumer("TEST", "CONSUMER"))
	require_NoError(t, sjs.isConsumerHealthy(mset, "CONSUMER", ca))

	// The health check could run earlier than we're able to create the consumer.
	// In that case, wait before erroring.
	sjs.mu.Lock()
	if !ca.deleted {
		sjs.mu.Unlock()
		t.Fatal("ca.deleted not set")
	}
	ca.deleted = false
	ca.Created = time.Now()
	sjs.mu.Unlock()
	require_NoError(t, sjs.isConsumerHealthy(mset, "CONSUMER", ca))
}

func TestJetStreamClusterRespectConsumerStartSeq(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create replicated stream.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// We could have published messages into the stream that have not yet been applied on the follower.
	// If we create a consumer with a starting sequence in the future, we must respect it.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverPolicy: nats.DeliverByStartSequencePolicy,
		OptStartSeq:   20,
	})
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Stream, 19)

	// Same thing if the first sequence is not 0.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 10})
	require_NoError(t, err)

	// Ensure all servers are up-to-date.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	ci, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverPolicy: nats.DeliverByStartSequencePolicy,
		OptStartSeq:   20,
	})
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Stream, 19)

	// Only if we're requested to start at a sequence that's not available anymore
	// can we safely move it up. That data is gone already, so can't do anything else.
	ci, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverPolicy: nats.DeliverByStartSequencePolicy,
		OptStartSeq:   5,
	})
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Stream, 9)
}

func TestJetStreamClusterPeerRemoveStreamConsumerDesync(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Must have at least one message in the stream.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	acc, err := sl.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	peer := rs.Name()
	rn := mset.raftNode()
	mset.mu.Lock()
	esm := encodeStreamMsgAllowCompress("foo", _EMPTY_, nil, nil, mset.clseq, time.Now().UnixNano(), false)
	mset.clseq++
	mset.mu.Unlock()
	// Propose both remove peer and a normal entry within the same append entry.
	err = rn.ProposeMulti([]*Entry{
		newEntry(EntryRemovePeer, []byte(peer)),
		newEntry(EntryNormal, esm),
	})
	require_NoError(t, err)

	// If the previous normal entry was skipped, we'd get a seq mismatch error here.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	// Now check the same but for a consumer.
	_, err = js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)

	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	acc, err = cl.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err = acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)

	updateDeliveredBuffer := func() []byte {
		var b [4*binary.MaxVarintLen64 + 1]byte
		b[0] = byte(updateDeliveredOp)
		n := 1
		n += binary.PutUvarint(b[n:], 100)
		n += binary.PutUvarint(b[n:], 100)
		n += binary.PutUvarint(b[n:], 1)
		n += binary.PutVarint(b[n:], time.Now().UnixNano())
		return b[:n]
	}

	rs = c.randomNonConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	peer = rs.Name()
	rn = o.raftNode()
	// Propose both remove peer and a normal entry within the same append entry.
	err = rn.ProposeMulti([]*Entry{
		newEntry(EntryRemovePeer, []byte(peer)),
		newEntry(EntryNormal, updateDeliveredBuffer()),
	})
	require_NoError(t, err)

	// Check the normal entry was applied.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		o.mu.Lock()
		defer o.mu.Unlock()
		cs, err := o.store.State()
		if err != nil {
			return err
		}
		if cs.Delivered.Consumer != 100 || cs.Delivered.Stream != 100 {
			return fmt.Errorf("expected sequence 100, got: %v", cs.Delivered)
		}
		return nil
	})
}

func TestJetStreamClusterStuckConsumerAfterLeaderChangeWithUnknownDeliveries(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Publish some messages into the stream.
	for i := 0; i < 3; i++ {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	// Ensure all servers are up-to-date.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	sub, err := js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)
	defer sub.Unsubscribe()

	// We only fetch 1 message here, since the condition is hard to trigger otherwise.
	// But, we're simulating fetching 3 messages and the consumer leader changing while
	// deliveries are happening. This will result in the new consumer leader not knowing
	// that the last two messages were also delivered (since we don't wait for quorum before delivering).
	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)

	// The client could send an acknowledgement, while the new consumer leader doesn't know about it
	// ever being delivered. It must NOT adjust any state and ignore the request to remain consistent.
	_, err = nc.Request("$JS.ACK.TEST.CONSUMER.1.3.3.0.0", nil, time.Second)
	require_Error(t, err, nats.ErrTimeout)

	// Acknowledging a message that is known to be delivered is accepted still.
	_, err = nc.Request("$JS.ACK.TEST.CONSUMER.1.1.1.0.0", nil, time.Second)
	require_NoError(t, err)

	// Check for consistent consumer info.
	ci, err := js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Consumer, 1)
	require_Equal(t, ci.Delivered.Stream, 1)
	require_Equal(t, ci.AckFloor.Consumer, 1)
	require_Equal(t, ci.AckFloor.Stream, 1)

	// Fetching for new messages MUST return the two messages the new consumer leader didn't
	// know were delivered before. If we wouldn't deliver these we'd have a stuck consumer.
	msgs, err = sub.Fetch(2)
	require_NoError(t, err)
	require_Len(t, len(msgs), 2)
	for _, msg := range msgs {
		require_NoError(t, msg.AckSync())
	}

	// Check for consistent consumer info.
	ci, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Consumer, 3)
	require_Equal(t, ci.Delivered.Stream, 3)
	require_Equal(t, ci.AckFloor.Consumer, 3)
	require_Equal(t, ci.AckFloor.Stream, 3)
}

// This is for when we are still using $SYS for NRG replication but we want to make sure
// we track this in something visible to the end user.
func TestJetStreamClusterAccountStatsForReplicatedStreams(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 5,
	})
	require_NoError(t, err)

	// Let's connect to stream leader to make sent messages predictable, otherwise we get those to come up based on routing to stream leader.
	s := c.streamLeader(globalAccountName, "TEST")
	require_True(t, s != nil)

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// NRG traffic can be compressed, so make this unique so we can check stats correctly.
	msg := make([]byte, 1024*1024)
	crand.Read(msg)

	// Publish some messages into the stream.
	for i := 0; i < 10; i++ {
		_, err = js.Publish("foo", msg)
		require_NoError(t, err)
	}
	time.Sleep(250 * time.Millisecond)

	// Now grab the account stats for us and make sure we account for the replicated messages.

	// Opts to grab our account.
	opts := &AccountStatzOptions{
		Accounts: []string{globalAccountName},
	}
	as, err := s.AccountStatz(opts)
	require_NoError(t, err)
	require_Equal(t, len(as.Accounts), 1)

	accStats := as.Accounts[0]

	// We need to account for possibility that the stream create was also on this server, hence the >= vs strict ==.
	require_True(t, accStats.Received.Msgs >= 10)
	require_True(t, accStats.Received.Bytes >= 1024*1024)
	// For sent, we will have 10 pub acks, and then should have 40 extra messages that are sent and accounted for
	// during the nrg propsal to the R5 peers.
	require_True(t, accStats.Sent.Msgs >= 50)
	require_True(t, accStats.Sent.Bytes >= accStats.Received.Bytes*4)
}

func TestJetStreamClusterRecreateConsumerFromMetaSnapshot(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Initial setup.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)

	// Wait for all servers to be fully up-to-date.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		if err = checkState(t, c, globalAccountName, "TEST"); err != nil {
			return err
		}
		for _, s := range c.servers {
			if acc, err := s.lookupAccount(globalAccountName); err != nil {
				return err
			} else if mset, err := acc.lookupStream("TEST"); err != nil {
				return err
			} else if o := mset.lookupConsumer("CONSUMER"); o == nil {
				return errors.New("consumer doesn't exist")
			}
		}
		return nil
	})

	// Shutdown a random server.
	rs := c.randomServer()
	rs.Shutdown()
	rs.WaitForShutdown()

	// Recreate connection, since we could have shutdown the server we were connected to.
	nc.Close()
	c.waitOnLeader()
	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Recreate consumer.
	require_NoError(t, js.DeleteConsumer("TEST", "CONSUMER"))
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)

	// Wait for all servers (except for the one that's down) to have recreated the consumer.
	var consumerRg string
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		consumerRg = _EMPTY_
		for _, s := range c.servers {
			if s == rs {
				continue
			}
			if acc, err := s.lookupAccount(globalAccountName); err != nil {
				return err
			} else if mset, err := acc.lookupStream("TEST"); err != nil {
				return err
			} else if o := mset.lookupConsumer("CONSUMER"); o == nil {
				return errors.New("consumer doesn't exist")
			} else if ccrg := o.raftNode().Group(); consumerRg == _EMPTY_ {
				consumerRg = ccrg
			} else if consumerRg != ccrg {
				return errors.New("consumer raft groups don't match")
			}
		}
		return nil
	})

	// Install snapshots on all remaining servers to "hide" the intermediate consumer recreate requests.
	for _, s := range c.servers {
		if s != rs {
			sjs := s.getJetStream()
			require_NotNil(t, sjs)
			snap, err := sjs.metaSnapshot()
			require_NoError(t, err)
			sjs.mu.RLock()
			meta := sjs.cluster.meta
			sjs.mu.RUnlock()
			require_NoError(t, meta.InstallSnapshot(snap))
		}
	}

	// Restart the server, it should receive a meta snapshot and recognize the consumer recreation.
	rs = c.restartServer(rs)
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		consumerRg = _EMPTY_
		for _, s := range c.servers {
			if acc, err := s.lookupAccount(globalAccountName); err != nil {
				return err
			} else if mset, err := acc.lookupStream("TEST"); err != nil {
				return err
			} else if o := mset.lookupConsumer("CONSUMER"); o == nil {
				return errors.New("consumer doesn't exist")
			} else if rn := o.raftNode(); rn == nil {
				return errors.New("consumer raft node doesn't exist")
			} else if ccrg := rn.Group(); ccrg == _EMPTY_ {
				return errors.New("consumer raft group doesn't exist")
			} else if consumerRg == _EMPTY_ {
				consumerRg = ccrg
			} else if consumerRg != ccrg {
				return errors.New("consumer raft groups don't match")
			}
		}
		return nil
	})
}

func TestJetStreamClusterUpgradeStreamVersioning(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	ml := c.leader()
	sjs := ml.getJetStream()
	rn := sjs.getMetaGroup()
	acc, err := ml.lookupAccount(globalAccountName)
	require_NoError(t, err)

	// Create stream config.
	cfg, apiErr := ml.checkStreamCfg(&StreamConfig{Name: "TEST", Subjects: []string{"foo"}}, acc, false)
	require_True(t, apiErr == nil)

	// Create and propose stream assignment.
	ci := &ClientInfo{Cluster: "R3S", Account: globalAccountName}
	rg, perr := sjs.createGroupForStream(ci, &cfg)
	require_True(t, perr == nil)
	sa := &streamAssignment{Group: rg, Sync: syncSubjForStream(), Config: &cfg, Client: ci, Created: time.Now().UTC()}
	require_NoError(t, rn.Propose(encodeAddStreamAssignment(sa)))

	// Wait for the stream assignment to have gone through.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		if sjs.streamAssignment(globalAccountName, "TEST") == nil {
			return errors.New("stream assignment does not exist yet")
		}
		return nil
	})

	for _, create := range []bool{true, false} {
		title := "create"
		if !create {
			title = "update"
		}
		t.Run(title, func(t *testing.T) {
			if create {
				// Create on 2.11+ should be idempotent with previous create on 2.10-.
				mcfg := &StreamConfig{}
				setStaticStreamMetadata(mcfg)
				si, err := js.AddStream(&nats.StreamConfig{
					Name:     "TEST",
					Subjects: []string{"foo"},
					Metadata: setDynamicStreamMetadata(mcfg).Metadata,
				})
				require_NoError(t, err)
				deleteDynamicMetadata(si.Config.Metadata)
				require_Len(t, len(si.Config.Metadata), 0)
			} else {
				// Update populates the versioning metadata.
				si, err := js.UpdateStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
				require_NoError(t, err)
				require_Len(t, len(si.Config.Metadata), 3)
			}
		})
	}
}

func TestJetStreamClusterUpgradeConsumerVersioning(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	ml := c.leader()
	sjs := ml.getJetStream()

	// Wait for the stream assignment to have gone through.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		if sjs.streamAssignment(globalAccountName, "TEST") == nil {
			return errors.New("stream assignment does not exist yet")
		}
		return nil
	})

	rn := sjs.getMetaGroup()
	sa := sjs.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, sa)
	acc, err := ml.lookupAccount(globalAccountName)
	require_NoError(t, err)

	// Create consumer config.
	cfg := &ConsumerConfig{Durable: "CONSUMER"}
	streamCfg, ok := sjs.clusterStreamConfig(acc.Name, "TEST")
	if !ok {
		require_NoError(t, NewJSStreamNotFoundError())
	}
	selectedLimits, _, _, apiErr := acc.selectLimits(cfg.replicas(&streamCfg))
	if apiErr != nil {
		require_NoError(t, apiErr)
	}
	srvLim := &ml.getOpts().JetStreamLimits
	apiErr = setConsumerConfigDefaults(cfg, &streamCfg, srvLim, selectedLimits, false)
	if apiErr != nil {
		require_NoError(t, apiErr)
	}

	// Create and propose consumer assignment.
	ci := &ClientInfo{Cluster: "R3S", Account: globalAccountName}
	rg := sjs.cluster.createGroupForConsumer(cfg, sa)
	ca := &consumerAssignment{Group: rg, Stream: "TEST", Name: "CONSUMER", Config: cfg, Client: ci, Created: time.Now().UTC()}
	require_NoError(t, rn.Propose(encodeAddConsumerAssignment(ca)))

	// Wait for the consumer assignment to have gone through.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		if sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER") == nil {
			return errors.New("consumer assignment does not exist yet")
		}
		return nil
	})

	for _, create := range []bool{true, false} {
		title := "create"
		if !create {
			title = "update"
		}
		t.Run(title, func(t *testing.T) {
			createConsumerRequest := func(obsReq CreateConsumerRequest) (*JSApiConsumerInfoResponse, error) {
				req, err := json.Marshal(obsReq)
				if err != nil {
					return nil, err
				}
				msg, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", "CONSUMER"), req, time.Second)
				if err != nil {
					return nil, err
				}
				var resp JSApiConsumerInfoResponse
				require_NoError(t, json.Unmarshal(msg.Data, &resp))
				if resp.Error != nil {
					return nil, resp.Error
				}
				return &resp, nil
			}

			if create {
				// Create on 2.11+ should be idempotent with previous create on 2.10-.
				ncfg := &ConsumerConfig{Durable: "CONSUMER"}
				setStaticConsumerMetadata(ncfg)
				obsReq := CreateConsumerRequest{
					Stream: "TEST",
					Config: *setDynamicConsumerMetadata(ncfg),
					Action: ActionCreate,
				}
				resp, err := createConsumerRequest(obsReq)
				require_NoError(t, err)
				deleteDynamicMetadata(resp.Config.Metadata)
				require_Len(t, len(resp.Config.Metadata), 0)
			} else {
				// Update populates the versioning metadata.
				obsReq := CreateConsumerRequest{
					Stream: "TEST",
					Config: ConsumerConfig{Durable: "CONSUMER"},
					Action: ActionUpdate,
				}
				resp, err := createConsumerRequest(obsReq)
				require_NoError(t, err)
				require_Len(t, len(resp.Config.Metadata), 3)
			}
		})
	}
}

func TestJetStreamClusterInterestPolicyAckAll(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Retention: nats.InterestPolicy,
		Subjects:  []string{"foo"},
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		AckPolicy: nats.AckAllPolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		_, err = js.Publish("foo", []byte("ok"))
		require_NoError(t, err)
	}

	expectedStreamMsgs := func(msgs uint64) {
		t.Helper()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST")
			if err != nil {
				return err
			}
			if si.State.Msgs != msgs {
				return fmt.Errorf("require uint64 equal, but got: %d != %d", si.State.Msgs, msgs)
			}
			return nil
		})
	}
	expectedStreamMsgs(100)

	for _, s := range c.servers {
		acc, err := s.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		o := mset.lookupConsumer("CONSUMER")
		require_NotNil(t, o)
		o.mu.Lock()
		// Ensure o.checkStateForInterestStream can't hide that the issue happened.
		o.chkflr = 1000
		o.mu.Unlock()
	}

	sub, err := js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)
	msgs, err := sub.Fetch(50)
	require_NoError(t, err)
	require_True(t, len(msgs) == 50)
	require_NoError(t, msgs[49].AckSync())

	expectedStreamMsgs(50)
}

func TestJetStreamClusterPreserveRedeliveredWithLaggingStream(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "CONSUMER", nats.AckWait(500*time.Millisecond))
	require_NoError(t, err)
	defer sub.Unsubscribe()

	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	require_NoError(t, msgs[0].AckSync())

	rsf := c.randomNonStreamLeader(globalAccountName, "TEST")
	acc, err := rsf.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	require_NoError(t, mset.raftNode().PauseApply())

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	// Move consumer leader to equal stream leader.
	sl := c.streamLeader(globalAccountName, "TEST")
	req := JSApiLeaderStepdownRequest{Placement: &Placement{Preferred: sl.Name()}}
	data, err := json.Marshal(req)
	require_NoError(t, err)
	_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "CONSUMER"), data, time.Second)
	require_NoError(t, err)
	c.waitOnConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_Equal(t, c.consumerLeader(globalAccountName, "TEST", "CONSUMER").Name(), sl.Name())

	// Get the message to be stored in redelivered state.
	msgs, err = sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	meta, err := msgs[0].Metadata()
	require_NoError(t, err)
	require_Equal(t, meta.NumDelivered, 1)

	msgs, err = sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	meta, err = msgs[0].Metadata()
	require_NoError(t, err)
	require_Equal(t, meta.NumDelivered, 2)

	// Move consumer leader to a different server.
	req = JSApiLeaderStepdownRequest{Placement: &Placement{Preferred: rsf.Name()}}
	data, err = json.Marshal(req)
	require_NoError(t, err)
	_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "CONSUMER"), data, time.Second)
	require_NoError(t, err)
	c.waitOnConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_Equal(t, c.consumerLeader(globalAccountName, "TEST", "CONSUMER").Name(), rsf.Name())

	// Confirming the stream contents of the consumer leader don't contain the message yet.
	msgs, err = sub.Fetch(1, nats.MaxWait(time.Second))
	require_Error(t, err, nats.ErrTimeout)
	require_Len(t, len(msgs), 0)

	// Now let it apply the missing message.
	mset.raftNode().ResumeApply()

	// Should preserve delivered state properly, otherwise the client would observe NumDelivered rolled back.
	msgs, err = sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	meta, err = msgs[0].Metadata()
	require_NoError(t, err)
	require_Equal(t, meta.NumDelivered, 3)
}

func TestJetStreamClusterInvalidJSACKOverRoute(t *testing.T) {
	test := func(t *testing.T, stream, consumer, subject string) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:      stream,
			Subjects:  []string{"foo.>"},
			Retention: nats.LimitsPolicy,
			Replicas:  3,
		})
		require_NoError(t, err)

		_, err = js.AddConsumer(stream, &nats.ConsumerConfig{
			Durable:   consumer,
			AckPolicy: nats.AckExplicitPolicy,
		})
		require_NoError(t, err)

		// Reconnect to consumer follower.
		nc.Close()
		rs := c.randomNonConsumerLeader(globalAccountName, stream, consumer)
		nc, js = jsClientConnect(t, rs)
		defer nc.Close()

		_, err = js.Publish(subject, nil)
		require_NoError(t, err)

		checkFor(t, time.Second, 100*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, stream)
		})

		sub, err := js.PullSubscribe("foo.>", consumer)
		require_NoError(t, err)
		defer sub.Unsubscribe()

		// The message went over a route and should have the correct subject, and working ACK.
		msgs, err := sub.Fetch(1)
		require_NoError(t, err)
		require_Len(t, len(msgs), 1)
		require_Equal(t, msgs[0].Subject, subject)
		require_NoError(t, msgs[0].AckSync())
	}

	for _, s := range []string{"StreamNoAt", "StreamWith@At"} {
		for _, c := range []string{"ConsumerNoAt", "ConsumerWith@At"} {
			for _, subject := range []string{"foo.no.at", "foo.with@at"} {
				t.Run(fmt.Sprintf("%s/%s/%s", s, c, subject), func(t *testing.T) {
					test(t, s, c, subject)
				})
			}
		}
	}
}

func TestJetStreamClusterConsumerOnlyDeliverMsgAfterQuorum(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  3,
		AckWait:   2 * time.Second,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	acc, err := cl.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)
	rn := o.raftNode().(*raft)

	// Force the leader to not be able to make proposals.
	rn.Lock()
	rn.werr = errors.New("block proposals")
	rn.Unlock()

	sub, err := js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)
	defer sub.Unsubscribe()

	// We must only receive a message AFTER quorum was met for updating delivered state.
	// This should time out since proposals are blocked.
	msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
	require_Error(t, err, nats.ErrTimeout)
	require_Len(t, len(msgs), 0)

	// Allow proposals to be made again.
	rn.Lock()
	rn.werr = nil
	rn.Unlock()

	// Now it should pass.
	msgs, err = sub.Fetch(1, nats.MaxWait(2*time.Second))
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	require_NoError(t, msgs[0].AckSync())
}

func TestJetStreamClusterConsumerResetPendingDeliveriesOnMaxAckPendingUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	cfg := &nats.ConsumerConfig{
		Durable:       "CONSUMER",
		AckPolicy:     nats.AckExplicitPolicy,
		Replicas:      3,
		AckWait:       2 * time.Second,
		MaxAckPending: 100,
	}
	_, err = js.AddConsumer("TEST", cfg)
	require_NoError(t, err)

	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	acc, err := cl.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)

	o.mu.Lock()
	o.pendingDeliveries = map[uint64]*jsPubMsg{0: getJSPubMsgFromPool()}
	o.mu.Unlock()

	// Increasing does not reset pending deliveries.
	cfg.MaxAckPending++
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)

	o.mu.Lock()
	l := len(o.pendingDeliveries)
	o.mu.Unlock()
	require_Equal(t, l, 1)

	// Decreasing does reset pending deliveries, so we can shrink the map.
	cfg.MaxAckPending--
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)

	o.mu.Lock()
	l = len(o.pendingDeliveries)
	o.mu.Unlock()
	require_Equal(t, l, 0)
}

func TestJetStreamClusterConsumerActiveAfterDidNotDeliverOverRoute(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	pubAck, err := js.Publish("foo", nil)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "CONSUMER",
		Replicas:       3,
		DeliverSubject: "deliver_subject",
	})
	require_NoError(t, err)

	ci, err := js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_False(t, ci.PushBound)

	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	rs := c.randomNonConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	ncRs, jsRs := jsClientConnect(t, rs)
	defer ncRs.Close()

	// Just noop callback.
	cb := func(msg *nats.Msg) {}
	_, err = jsRs.Subscribe("foo", cb, nats.Bind("TEST", "CONSUMER"))
	require_NoError(t, err)

	// Eventually the sub should be known to the leader, and report active/bound.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		ci, err = js.ConsumerInfo("TEST", "CONSUMER")
		if err != nil {
			return err
		}
		if !ci.PushBound {
			return errors.New("push not bound")
		}
		return nil
	})

	// Simulate failed delivery over route connection.
	acc, err := cl.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)
	o.didNotDeliver(1, "foo")

	// Consumer should still be active/bound.
	ci, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_True(t, ci.PushBound)
}

func TestJetStreamClusterOfflineR1StreamDenyUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	// Stop current R1 stream leader.
	sl := c.streamLeader(globalAccountName, "TEST")
	sl.Shutdown()
	nc.Close()
	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Wait for meta leader, so we can send an update.
	c.waitOnLeader()

	_, err = js.StreamInfo("TEST")
	require_Error(t, err, NewJSStreamOfflineError())

	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_Error(t, err, NewJSStreamOfflineError())
}

func TestJetStreamClusterOfflineR1ConsumerDenyUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	cfg := &nats.ConsumerConfig{
		Durable:  "CONSUMER",
		Replicas: 1,
	}
	_, err = js.AddConsumer("TEST", cfg)
	require_NoError(t, err)

	// Stop current R1 consumer leader.
	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	cl.Shutdown()
	nc.Close()
	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Wait for meta leader, so we can send an update.
	c.waitOnLeader()

	_, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_Error(t, err, NewJSConsumerOfflineError())

	cfg.Replicas = 3
	_, err = js.UpdateConsumer("TEST", cfg)
	require_Error(t, err, NewJSConsumerOfflineError())
}

func TestJetStreamClusterSnapshotStreamAssetOnShutdown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	var sds []string
	for _, s := range c.servers {
		sds = append(sds, s.StoreDir())
	}

	for _, sd := range sds {
		matches, err := filepath.Glob(filepath.Join(sd, "$SYS", "_js_", "*", snapshotsDir, "*"))
		require_NoError(t, err)
		require_True(t, len(matches) > 0)
		for _, match := range matches {
			require_NoError(t, os.RemoveAll(match))
		}
	}

	// Publish, so we have something new to snapshot.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	// Shutdown servers, and check if all made stream snapshots.
	for _, s := range c.servers {
		s.Shutdown()
	}
	for _, sd := range sds {
		matches, err := filepath.Glob(filepath.Join(sd, "$SYS", "_js_", "*", snapshotsDir))
		require_NoError(t, err)
		// Matches _meta_ and stream raft groups.
		require_Len(t, len(matches), 2)
		var foundStream bool
		for _, match := range matches {
			if !strings.Contains(match, "S-R3F") {
				continue
			}
			foundStream = true
			dirs, err := os.ReadDir(match)
			require_NoError(t, err)
			if len(dirs) != 1 {
				t.Errorf("Missing snapshot for %s", match)
			}
		}
		require_True(t, foundStream)
	}
}

func TestJetStreamClusterDontReviveRemovedStream(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	// Wait for stream to be assigned on all servers.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			if !s.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
				return fmt.Errorf("stream not yet assigned on %s", s.Name())
			}
		}
		return nil
	})

	sl := c.streamLeader(globalAccountName, "TEST")
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	mset, err := rs.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// Simulate the stream being scaled down, quickly after it was scaled up.
	cfg.Replicas = 1
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	// Wait for the stream to not be assigned on the servers (except the leader).
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			if s == sl {
				continue
			}
			if s.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
				return fmt.Errorf("stream still assigned on %s", s.Name())
			}
		}
		if !mset.closed.Load() {
			return fmt.Errorf("stream not closed yet")
		}
		return nil
	})

	// Simulating the stream was catching up and is resetting after timing out.
	require_True(t, mset.resetClusteredState(nil))

	// Allow for some time here, mset.resetClusteredState might
	// spin up a goroutine if it's resetting the stream.
	// Confirm the stream is not revived.
	time.Sleep(200 * time.Millisecond)
	_, err = rs.globalAccount().lookupStream("TEST")
	require_Error(t, err, ErrJetStreamStreamNotFound)
}

func TestJetStreamClusterCreateR3StreamWithOfflineNodes(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "FOO", Replicas: 3})
	require_NoError(t, err)

	// Shutdown one server, we'll still have quorum.
	rs1 := c.randomNonLeader()
	rs1.Shutdown()
	_, err = js.AddStream(&nats.StreamConfig{Name: "BAR", Replicas: 3})
	require_NoError(t, err)

	// While we still have quorum, publish a few messages to the streams.
	// The offline server will need to both assign the stream and catchup when it comes back.
	for _, subj := range []string{"FOO", "BAR"} {
		_, err = js.Publish(subj, nil)
		require_NoError(t, err)
	}

	// Shutdown one more server, only one server left that can't reach quorum on its own.
	rs2 := c.randomNonLeader()
	rs2.Shutdown()
	_, err = js.AddStream(&nats.StreamConfig{Name: "BAZ", Replicas: 3})
	require_Contains(t, err.Error(), "no suitable peers for placement", "peer offline")

	// Restart offline servers and ensure all comes back up.
	c.restartServer(rs1)
	c.restartServer(rs2)
	c.waitOnLeader()
	c.waitOnStreamLeader(globalAccountName, "FOO")
	c.waitOnStreamLeader(globalAccountName, "BAR")
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if err := checkState(t, c, globalAccountName, "FOO"); err != nil {
			return err
		}
		if err := checkState(t, c, globalAccountName, "BAR"); err != nil {
			return err
		}
		return nil
	})
}

func TestJetStreamClusterCreateEphemeralConsumerWithOfflineNodes(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()
	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	require_NoError(t, err)

	// Shutdown a random server.
	c.randomNonLeader().Shutdown()

	for range 10 {
		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{})
		require_NoError(t, err)
	}
}

func TestJetStreamClusterSetPreferredToOnlineNode(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Shutdown one random server.
	ml := c.leader()
	rs := c.randomNonLeader()
	js := rs.getJetStream()
	cc := js.cluster
	require_NotNil(t, cc)
	meta := cc.meta
	require_NotNil(t, meta)
	rsid := meta.ID()
	rs.Shutdown()

	cfg := &StreamConfig{Storage: FileStorage, Replicas: 3}
	js = ml.getJetStream()
	cc = js.cluster
	require_NotNil(t, cc)
	peers, err := cc.selectPeerGroup(3, "R3S", cfg, nil, 0, nil)
	// Need explicit nil-check.
	if err != nil {
		require_NoError(t, err)
	}
	require_Len(t, len(peers), 3)

	// Should prefer online peers when selecting preferred.
	rg := &raftGroup{Name: groupNameForStream(peers, cfg.Storage), Storage: cfg.Storage, Peers: peers, Cluster: "R3S"}
	rg.setPreferred(ml)
	require_NotEqual(t, rg.Preferred, rsid)
}

func TestJetStreamClusterAsyncFlushBasics(t *testing.T) {
	test := func(t *testing.T, syncAlways bool) {
		supportsAsync := !syncAlways

		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		for _, s := range c.servers {
			s.optsMu.Lock()
			s.opts.SyncAlways = syncAlways
			s.optsMu.Unlock()
		}

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		var s *Server
		checkStoreIsAsync := func(expectAsync bool) {
			t.Helper()
			mset, err := s.globalAccount().lookupStream("TEST")
			require_NoError(t, err)
			fs := mset.Store().(*fileStore)
			fs.mu.RLock()
			lmb := fs.lmb
			asyncFlush := fs.fcfg.AsyncFlush
			fip := fs.fip
			fs.mu.RUnlock()
			require_Equal(t, asyncFlush, expectAsync)
			require_Equal(t, fip, !expectAsync)
			require_NotNil(t, lmb)
			lmb.mu.RLock()
			flusher := lmb.flusher
			lmb.mu.RUnlock()
			if expectAsync {
				if !flusher {
					t.Fatal("flusher not initialized")
				} else if !asyncFlush {
					t.Fatal("async flush config not set")
				}
			} else {
				if flusher {
					t.Fatal("flusher still initialized")
				} else if asyncFlush {
					t.Fatal("async flush config not reset")
				}
			}
		}

		cfg := &StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Storage:  FileStorage,
			Replicas: 1,
		}
		// Test disabled async flush on create.
		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)
		s = c.streamLeader(globalAccountName, "TEST")
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		checkStoreIsAsync(false)

		// Enabling async flush.
		cfg.Replicas = 3
		_, err = jsStreamUpdate(t, nc, cfg)
		require_NoError(t, err)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		checkStoreIsAsync(supportsAsync)

		// Disabling async flush.
		cfg.Replicas = 1
		_, err = jsStreamUpdate(t, nc, cfg)
		require_NoError(t, err)
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		checkStoreIsAsync(false)

		// Test async flush on create.
		require_NoError(t, js.DeleteStream("TEST"))
		cfg.Replicas = 3
		_, err = jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)
		s = c.streamLeader(globalAccountName, "TEST")
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		checkStoreIsAsync(supportsAsync)
	}

	t.Run("Default", func(t *testing.T) { test(t, false) })
	t.Run("SyncAlways", func(t *testing.T) { test(t, true) })
}

func TestJetStreamClusterAsyncFlushFileStoreFlushOnSnapshot(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  FileStorage,
		Replicas: 3,
	})
	require_NoError(t, err)

	// Now shutdown flusher and wait for it to be closed,
	// we'll not flush published messages automatically anymore.
	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	fs := mset.Store().(*fileStore)
	fs.mu.RLock()
	lmb := fs.lmb
	fs.mu.RUnlock()
	require_NotNil(t, lmb)

	lmb.mu.Lock()
	if lmb.qch != nil {
		close(lmb.qch)
		lmb.qch = nil
	}
	lmb.mu.Unlock()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		lmb.mu.RLock()
		defer lmb.mu.RUnlock()
		if lmb.flusher {
			return errors.New("flusher still active")
		}
		return nil
	})

	// Get the highest applied count from snapshot before below publish.
	n := mset.raftNode().(*raft)
	_, _, previousApplied := n.Progress()

	// Confirm no pending writes.
	require_Equal(t, lmb.pendingWriteSize(), 0)

	// Publishing a message will still work, because writes are async.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	// Confirm above write is pending.
	require_Equal(t, lmb.pendingWriteSize(), 33)

	// Stop stream monitor routine, which will install a snapshot on shutdown.
	mset.mu.Lock()
	if mset.mqch != nil {
		close(mset.mqch)
		mset.mqch = nil
	}
	mset.mu.Unlock()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		n.Lock()
		snap, err := n.loadLastSnapshot()
		n.Unlock()
		if err != nil {
			return err
		}
		if snap.lastIndex <= previousApplied {
			return fmt.Errorf("snapshot still at lastIndex=%d, expected=%d", snap.lastIndex, previousApplied)
		}
		return nil
	})

	// Confirm the write is flushed as a result of the snapshot.
	require_Equal(t, lmb.pendingWriteSize(), 0)
}

func TestJetStreamClusterScheduledDelayedMessage(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, storage := range []StorageType{FileStorage, MemoryStorage} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, storage), func(t *testing.T) {
				c := createJetStreamClusterExplicit(t, "R3S", 3)
				defer c.shutdown()

				nc, js := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				_, err := jsStreamCreate(t, nc, &StreamConfig{
					Name:              "SchedulesDisabled",
					Subjects:          []string{"disabled"},
					Storage:           storage,
					Replicas:          replicas,
					AllowMsgSchedules: false,
				})
				require_NoError(t, err)

				schedulePattern := "@at 1970-01-01T00:00:00Z"
				m := nats.NewMsg("disabled")
				m.Header.Set("Nats-Schedule", schedulePattern) // Needs to be valid, but is not used.
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesDisabledError())

				m = nats.NewMsg("disabled")
				m.Header.Set("Nats-Schedule", "disabled")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesDisabledError())

				_, err = jsStreamCreate(t, nc, &StreamConfig{
					Name:              "SchedulesEnabledNoTtl",
					Subjects:          []string{"disabled.ttl"},
					Storage:           storage,
					Replicas:          replicas,
					AllowMsgSchedules: true,
				})
				require_NoError(t, err)

				m = nats.NewMsg("disabled.ttl")
				m.Header.Set("Nats-Schedule", schedulePattern) // Needs to be valid, but is not used.
				m.Header.Set("Nats-Schedule-TTL", "1s")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageTTLDisabledError())

				cfg := &StreamConfig{
					Name:              "SchedulesEnabled",
					Subjects:          []string{"foo.*"},
					Storage:           storage,
					Replicas:          replicas,
					AllowMsgSchedules: true,
					AllowMsgTTL:       true,
				}
				_, err = jsStreamCreate(t, nc, cfg)
				require_NoError(t, err)

				m = nats.NewMsg("foo.invalid")
				m.Header.Set("Nats-Schedule", "invalid")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesPatternInvalidError())

				m = nats.NewMsg("foo.invalid")
				m.Header.Set("Nats-Schedule", schedulePattern)
				m.Header.Set("Nats-Schedule-Target", "not.matching")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesTargetInvalidError())

				m = nats.NewMsg("foo.invalid")
				m.Header.Set("Nats-Schedule", schedulePattern)
				m.Header.Set("Nats-Schedule-Target", "foo.*") // Must be literal.
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesTargetInvalidError())

				m = nats.NewMsg("foo.invalid")
				m.Header.Set("Nats-Schedule", schedulePattern)
				m.Header.Set("Nats-Schedule-Target", "foo.invalid") // Can't equal the publish subject.
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesTargetInvalidError())

				m = nats.NewMsg("foo.invalid")
				m.Header.Set("Nats-Schedule", schedulePattern)
				m.Header.Set("Nats-Schedule-Target", "foo.publish")
				m.Header.Set("Nats-Schedule-TTL", "invalid")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesTTLInvalidError())

				m = nats.NewMsg("foo.invalid")
				m.Header.Set("Nats-Schedule", schedulePattern)
				m.Header.Set("Nats-Schedule-Target", "foo.publish")
				m.Header.Set("Nats-Schedule-TTL", "1s")
				m.Header.Set("Nats-Rollup", "all")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesRollupInvalidError())

				// Schedule with a delay that takes a very long time.
				schedule := time.Now().Add(time.Hour).Format(time.RFC3339Nano)
				schedulePattern = fmt.Sprintf("@at %s", schedule)
				m = nats.NewMsg("foo.schedule")
				m.Header.Set("Nats-Schedule", schedulePattern)
				m.Header.Set("Nats-Schedule-Target", "foo.publish")
				m.Header.Set("Nats-Schedule-TTL", "1s")
				m.Header.Set("Nats-Rollup", "sub")
				pubAck, err := js.PublishMsg(m)
				require_NoError(t, err)
				require_Equal(t, pubAck.Sequence, 1)

				// Schedule where the rollup is missing, automatically gets the rollup added.
				// Also, will rollup the previous message for this schedule.
				schedule = time.Now().Add(time.Second).Format(time.RFC3339Nano)
				schedulePattern = fmt.Sprintf("@at %s", schedule)
				m = nats.NewMsg("foo.schedule")
				m.Data = []byte("hello")
				m.Header.Set("Nats-Schedule", schedulePattern)
				m.Header.Set("Nats-Schedule-Target", "foo.publish")
				m.Header.Set("Nats-Schedule-TTL", "1s")
				// Add a bunch of headers that will be stripped on the scheduled message.
				m.Header.Set("Nats-Expected-Stream", "SchedulesEnabled")
				m.Header.Set("Nats-Expected-Last-Sequence", "1")
				m.Header.Set("Nats-Expected-Last-Subject-Sequence", "1")
				m.Header.Set("Nats-Msg-Id", "X")
				m.Header.Set("Nats-TTL", "60s")
				m.Header.Set("Header", "Value")
				pubAck, err = js.PublishMsg(m)
				require_NoError(t, err)
				require_Equal(t, pubAck.Sequence, 2)

				sl := c.streamLeader(globalAccountName, "SchedulesEnabled")
				mset, err := sl.globalAccount().lookupStream("SchedulesEnabled")
				require_NoError(t, err)

				// Only one schedule exists.
				state := mset.state()
				require_Equal(t, state.LastSeq, 2)
				require_Equal(t, state.Msgs, 1)

				// Waiting for the delayed message to be published.
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					state = mset.state()
					if state.LastSeq != 3 {
						return fmt.Errorf("expected last seq 3, got %d", state.LastSeq)
					} else if state.Msgs != 1 {
						return fmt.Errorf("expected 1 msg, got %d", state.Msgs)
					}
					return nil
				})

				// Confirm the scheduled message has the correct data.
				rsm, err := js.GetLastMsg("SchedulesEnabled", "foo.publish")
				require_NoError(t, err)
				require_Equal(t, rsm.Sequence, 3)
				require_True(t, bytes.Equal(rsm.Data, []byte("hello")))
				require_Len(t, len(rsm.Header), 4)
				require_Equal(t, rsm.Header.Get("Nats-Scheduler"), "foo.schedule")
				require_Equal(t, rsm.Header.Get("Nats-Schedule-Next"), "purge")
				require_Equal(t, rsm.Header.Get("Nats-TTL"), "1s")
				require_Equal(t, rsm.Header.Get("Header"), "Value")

				// Waiting for the delayed message to age out due to its TTL.
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					state = mset.state()
					if state.FirstSeq != 4 {
						return fmt.Errorf("expected first seq 4, got %d", state.FirstSeq)
					} else if state.LastSeq != 3 {
						return fmt.Errorf("expected last seq 3, got %d", state.LastSeq)
					} else if state.Msgs != 0 {
						return fmt.Errorf("expected no messages, got %d", state.Msgs)
					}
					return nil
				})

				// Servers should be synced.
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					return checkState(t, c, globalAccountName, "SchedulesEnabled")
				})

				cfg.AllowMsgSchedules = false
				_, err = jsStreamUpdate(t, nc, cfg)
				require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("message schedules can not be disabled")))
			})
		}
	}
}

func TestJetStreamClusterScheduledDelayedMessageReversedHeaderOrder(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, storage := range []StorageType{FileStorage, MemoryStorage} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, storage), func(t *testing.T) {
				c := createJetStreamClusterExplicit(t, "R3S", 3)
				defer c.shutdown()

				nc, _ := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				cfg := &StreamConfig{
					Name:              "TEST",
					Subjects:          []string{"foo.*"},
					Storage:           storage,
					Replicas:          replicas,
					AllowMsgSchedules: true,
					AllowMsgTTL:       true,
				}
				_, err := jsStreamCreate(t, nc, cfg)
				require_NoError(t, err)

				sl := c.streamLeader(globalAccountName, "TEST")
				gacc := sl.globalAccount()
				// The Nats-Schedule headers share a common prefix, so make sure if these are ordered differently
				// we can still properly schedule a message.
				hdr := genHeader(nil, "Nats-Schedule-Target", "foo.publish")
				hdr = genHeader(hdr, "Nats-Schedule", "@at 1970-01-01T00:00:00Z")
				require_NoError(t, sl.sendInternalAccountMsgWithReply(gacc, "foo.schedule", _EMPTY_, hdr, nil, false))

				mset, err := gacc.lookupStream("TEST")
				require_NoError(t, err)

				// Waiting for the delayed message to be published.
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					state := mset.state()
					if state.LastSeq != 2 {
						return fmt.Errorf("expected last seq 2, got %d", state.LastSeq)
					} else if state.Msgs != 1 {
						return fmt.Errorf("expected 1 msg, got %d", state.Msgs)
					}
					return nil
				})
			})
		}
	}
}

func TestJetStreamClusterOfflineStreamAndConsumerAfterAssetCreateOrUpdate(t *testing.T) {
	clusterName := "R3S"
	c := createJetStreamClusterExplicit(t, clusterName, 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	ml := c.leader()
	require_NotNil(t, ml)

	sjs := ml.getJetStream()
	require_NotNil(t, sjs)
	sjs.mu.Lock()
	cc := sjs.cluster
	if cc == nil || cc.meta == nil {
		sjs.mu.Unlock()
		t.Fatalf("Expected cluster to be initialized")
	}

	restart := func() {
		t.Helper()
		for _, s := range c.servers {
			sjs = s.getJetStream()
			snap, err := sjs.metaSnapshot()
			require_NoError(t, err)
			meta := sjs.getMetaGroup()
			meta.InstallSnapshot(snap)
		}

		c.stopAll()
		c.restartAllSamePorts()
		c.waitOnLeader()
		ml = c.leader()
		require_NotNil(t, ml)
		require_NoError(t, nc.ForceReconnect())

		sjs = ml.getJetStream()
		require_NotNil(t, sjs)
		sjs.mu.Lock()
		cc = sjs.cluster
		if cc == nil || cc.meta == nil {
			sjs.mu.Unlock()
			t.Fatalf("Expected cluster to be initialized")
		}
		sjs.mu.Unlock()
	}

	getValidMetaSnapshot := func() (wsas []writeableStreamAssignment) {
		t.Helper()
		snap, err := sjs.metaSnapshot()
		require_NoError(t, err)
		require_True(t, len(snap) > 0)
		dec, err := s2.Decode(nil, snap)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(dec, &wsas))
		return wsas
	}

	// Create a stream that's unsupported.
	ci := &ClientInfo{
		Account: globalAccountName,
		Cluster: clusterName,
	}
	scfg := &StreamConfig{
		Name:     "DowngradeStreamTest",
		Storage:  FileStorage,
		Replicas: 3,
		Metadata: map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt - 1)},
	}
	rg, perr := sjs.createGroupForStream(ci, scfg)
	if perr != nil {
		sjs.mu.Unlock()
		require_NoError(t, perr)
	}
	sa := &streamAssignment{
		Config:  scfg,
		Group:   rg,
		Created: time.Now().UTC(),
		Client:  ci,
	}
	err := cc.meta.Propose(encodeAddStreamAssignment(sa))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnAllCurrent()

	unsupported := func(requiredApiLevel int) string {
		return fmt.Sprintf("unsupported - required API level: %d, current API level: %d", requiredApiLevel, JSApiLevel)
	}
	expectStreamInfo := func(offlineReason, streamName string) {
		var msg *nats.Msg
		checkFor(t, 3*time.Second, 200*time.Millisecond, func() error {
			msg, err = nc.Request(fmt.Sprintf(JSApiStreamInfoT, streamName), nil, time.Second)
			return err
		})
		var si JSApiStreamInfoResponse
		require_NoError(t, json.Unmarshal(msg.Data, &si))
		require_NotNil(t, si.Error)
		require_Error(t, si.Error, NewJSStreamOfflineReasonError(errors.New(offlineReason)))

		var sn JSApiStreamNamesResponse
		msg, err = nc.Request(JSApiStreams, nil, time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(msg.Data, &sn))
		require_Len(t, len(sn.Streams), 1)
		require_Equal(t, sn.Streams[0], streamName)

		var sl JSApiStreamListResponse
		msg, err = nc.Request(JSApiStreamList, nil, 2*time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(msg.Data, &sl))
		require_Len(t, len(sl.Streams), 0)
		require_Len(t, len(sl.Missing), 1)
		require_Equal(t, sl.Missing[0], streamName)
		require_Len(t, len(sl.Offline), 1)
		require_Equal(t, sl.Offline[streamName], offlineReason)
	}

	// Stream should be reported as offline, but healthz should report healthy to not block downgrades.
	expectStreamInfo(unsupported(math.MaxInt-1), "DowngradeStreamTest")
	health := ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)
	restart()
	expectStreamInfo(unsupported(math.MaxInt-1), "DowngradeStreamTest")
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)

	wsas := getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	require_Equal(t, wsas[0].Config.Name, "DowngradeStreamTest")
	require_Equal(t, wsas[0].Config.Metadata["_nats.req.level"], strconv.Itoa(math.MaxInt-1))

	// Update a stream that's unsupported.
	sjs.mu.Lock()
	scfg.Metadata = map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt)}
	err = cc.meta.Propose(encodeUpdateStreamAssignment(sa))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnAllCurrent()

	// Stream should be reported as offline, but healthz should report healthy to not block downgrades.
	expectStreamInfo(unsupported(math.MaxInt), "DowngradeStreamTest")
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)
	restart()
	expectStreamInfo(unsupported(math.MaxInt), "DowngradeStreamTest")
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)

	wsas = getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	require_Equal(t, wsas[0].Config.Name, "DowngradeStreamTest")
	require_Equal(t, wsas[0].Config.Metadata["_nats.req.level"], strconv.Itoa(math.MaxInt))

	// Deleting a stream should always work, even if it is unsupported.
	require_NoError(t, js.DeleteStream("DowngradeStreamTest"))
	snap, err := sjs.metaSnapshot()
	require_NoError(t, err)
	require_True(t, snap == nil)

	// Create a supported stream and consumer.
	_, err = js.AddStream(&nats.StreamConfig{Name: "DowngradeConsumerTest", Replicas: 3})
	require_NoError(t, err)
	_, err = js.AddConsumer("DowngradeConsumerTest", &nats.ConsumerConfig{Name: "consumer"})
	require_NoError(t, err)

	// Create a consumer that's unsupported.
	sjs.mu.Lock()
	ccfg := &ConsumerConfig{
		Name:     "DowngradeConsumerTest",
		Replicas: 3,
		Metadata: map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt - 1)},
	}
	rg = cc.createGroupForConsumer(ccfg, sa)
	ca := &consumerAssignment{
		Config:  ccfg,
		Group:   rg,
		Stream:  "DowngradeConsumerTest",
		Name:    "DowngradeConsumerTest",
		Created: time.Now().UTC(),
		Client:  ci,
	}
	err = cc.meta.Propose(encodeAddConsumerAssignment(ca))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnAllCurrent()

	expectConsumerInfo := func(offlineReason string) {
		var msg *nats.Msg
		checkFor(t, 3*time.Second, 200*time.Millisecond, func() error {
			msg, err = nc.Request(fmt.Sprintf(JSApiConsumerInfoT, "DowngradeConsumerTest", "DowngradeConsumerTest"), nil, time.Second)
			return err
		})
		var ci JSApiConsumerInfoResponse
		require_NoError(t, json.Unmarshal(msg.Data, &ci))
		require_NotNil(t, ci.Error)
		require_Error(t, ci.Error, NewJSConsumerOfflineReasonError(errors.New(offlineReason)))

		var cn JSApiConsumerNamesResponse
		msg, err = nc.Request(fmt.Sprintf(JSApiConsumersT, "DowngradeConsumerTest"), nil, time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(msg.Data, &cn))
		require_Len(t, len(cn.Consumers), 2)
		require_Equal(t, cn.Consumers[0], "DowngradeConsumerTest")
		for _, name := range cn.Consumers {
			require_True(t, name == "consumer" || name == "DowngradeConsumerTest")
		}

		var cl JSApiConsumerListResponse
		msg, err = nc.Request(fmt.Sprintf(JSApiConsumerListT, "DowngradeConsumerTest"), nil, 5*time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(msg.Data, &cl))
		require_Len(t, len(cl.Consumers), 0)
		require_Len(t, len(cl.Missing), 2)
		for _, name := range cl.Missing {
			require_True(t, name == "consumer" || name == "DowngradeConsumerTest")
		}
		require_Len(t, len(cl.Offline), 1)
		require_Equal(t, cl.Offline["DowngradeConsumerTest"], offlineReason)

		// Stream should also be reported as offline.
		// Specifically, as "stopped" because it's still supported, but can't run due to the unsupported consumer.
		expectStreamInfo("stopped", "DowngradeConsumerTest")
	}

	// Consumer should be reported as offline, but healthz should report healthy to not block downgrades.
	expectConsumerInfo(unsupported(math.MaxInt - 1))
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)
	restart()
	expectConsumerInfo(unsupported(math.MaxInt - 1))
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)

	wsas = getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	require_Equal(t, wsas[0].Config.Name, "DowngradeConsumerTest")
	require_Equal(t, wsas[0].Config.Metadata["_nats.req.level"], "0")
	require_Len(t, len(wsas[0].Consumers), 2)
	for _, wca := range wsas[0].Consumers {
		if wca.Config.Name == "DowngradeConsumerTest" {
			require_Equal(t, wca.Config.Metadata["_nats.req.level"], strconv.Itoa(math.MaxInt-1))
		} else {
			require_Equal(t, wca.Config.Name, "consumer")
		}
	}

	// Update a consumer (with compressed data) that's unsupported.
	ccfg.Metadata = map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt)}
	sjs.mu.Lock()
	err = cc.meta.Propose(encodeAddConsumerAssignmentCompressed(ca))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnAllCurrent()

	// Consumer should be reported as offline, but healthz should report healthy to not block downgrades.
	expectConsumerInfo(unsupported(math.MaxInt))
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)
	restart()
	expectConsumerInfo(unsupported(math.MaxInt))
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)

	wsas = getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	require_Equal(t, wsas[0].Config.Name, "DowngradeConsumerTest")
	require_Equal(t, wsas[0].Config.Metadata["_nats.req.level"], "0")
	require_Len(t, len(wsas[0].Consumers), 2)
	for _, wca := range wsas[0].Consumers {
		if wca.Config.Name == "DowngradeConsumerTest" {
			require_Equal(t, wca.Config.Metadata["_nats.req.level"], strconv.Itoa(math.MaxInt))
		} else {
			require_Equal(t, wca.Config.Name, "consumer")
		}
	}

	// Deleting a consumer should always work, even if it is unsupported.
	require_NoError(t, js.DeleteConsumer("DowngradeConsumerTest", "DowngradeConsumerTest"))
	c.waitOnAllCurrent()

	wsas = getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	require_Equal(t, wsas[0].Config.Name, "DowngradeConsumerTest")
	require_Equal(t, wsas[0].Config.Metadata["_nats.req.level"], "0")
	require_Len(t, len(wsas[0].Consumers), 1)
	require_Equal(t, wsas[0].Consumers[0].Config.Name, "consumer")
}

func TestJetStreamClusterOfflineStreamAndConsumerAfterDowngrade(t *testing.T) {
	clusterName := "R3S"
	c := createJetStreamClusterExplicit(t, clusterName, 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	ml := c.leader()
	require_NotNil(t, ml)

	sjs := ml.getJetStream()
	require_NotNil(t, sjs)
	sjs.mu.Lock()
	cc := sjs.cluster
	if cc == nil || cc.meta == nil {
		sjs.mu.Unlock()
		t.Fatalf("Expected cluster to be initialized")
	}

	restart := func() {
		t.Helper()
		for _, s := range c.servers {
			sjs = s.getJetStream()
			snap, err := sjs.metaSnapshot()
			require_NoError(t, err)
			meta := sjs.getMetaGroup()
			meta.InstallSnapshot(snap)
		}

		c.stopAll()
		c.restartAllSamePorts()
		c.waitOnLeader()
		ml = c.leader()
		require_NotNil(t, ml)
		require_NoError(t, nc.ForceReconnect())

		sjs = ml.getJetStream()
		require_NotNil(t, sjs)
		sjs.mu.Lock()
		cc = sjs.cluster
		if cc == nil || cc.meta == nil {
			sjs.mu.Unlock()
			t.Fatalf("Expected cluster to be initialized")
		}
		sjs.mu.Unlock()
	}

	getValidMetaSnapshot := func() (wsas []writeableStreamAssignment) {
		t.Helper()
		snap, err := sjs.metaSnapshot()
		require_NoError(t, err)
		require_True(t, len(snap) > 0)
		dec, err := s2.Decode(nil, snap)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(dec, &wsas))
		return wsas
	}

	// Create a stream that's unsupported.
	ci := &ClientInfo{
		Account: globalAccountName,
		Cluster: clusterName,
	}
	scfg := &StreamConfig{
		Name:     "DowngradeStreamTest",
		Storage:  FileStorage,
		Replicas: 3,
	}
	rg, perr := sjs.createGroupForStream(ci, scfg)
	if perr != nil {
		sjs.mu.Unlock()
		require_NoError(t, perr)
	}
	sa := &streamAssignment{
		Config:  scfg,
		Group:   rg,
		Created: time.Now().UTC(),
		Client:  ci,
	}
	err := cc.meta.Propose(encodeAddStreamAssignment(sa))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "DowngradeStreamTest")

	expectStreamInfo := func(offline bool) {
		if !offline {
			c.waitOnStreamLeader(globalAccountName, "DowngradeStreamTest")
		}
		var msg *nats.Msg
		checkFor(t, 3*time.Second, 200*time.Millisecond, func() error {
			msg, err = nc.Request(fmt.Sprintf(JSApiStreamInfoT, "DowngradeStreamTest"), nil, time.Second)
			return err
		})
		var si JSApiStreamInfoResponse
		require_NoError(t, json.Unmarshal(msg.Data, &si))
		if !offline {
			require_True(t, si.Error == nil)
		} else {
			require_NotNil(t, si.Error)
			require_Contains(t, si.Error.Error(), "stream is offline", "unsupported", "required API level")
		}
	}

	// Stream is still supported, so it should be available and healthz should report healthy.
	expectStreamInfo(false)
	health := ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)
	restart()
	expectStreamInfo(false)
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)

	wsas := getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	require_Equal(t, wsas[0].Config.Name, "DowngradeStreamTest")

	// Update a stream to be unsupported.
	sjs.mu.Lock()
	scfg.Metadata = map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt)}
	err = cc.meta.Propose(encodeUpdateStreamAssignment(sa))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnAllCurrent()

	// Stream should be reported as offline, but healthz should report healthy to not block downgrades.
	expectStreamInfo(true)
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)
	restart()
	expectStreamInfo(true)
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)

	wsas = getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	require_Equal(t, wsas[0].Config.Name, "DowngradeStreamTest")

	// Deleting a stream should always work, even if it is unsupported.
	require_NoError(t, js.DeleteStream("DowngradeStreamTest"))
	snap, err := sjs.metaSnapshot()
	require_NoError(t, err)
	require_True(t, snap == nil)

	// Create a supported stream and consumer.
	_, err = js.AddStream(&nats.StreamConfig{Name: "DowngradeConsumerTest", Replicas: 3})
	require_NoError(t, err)

	sjs.mu.Lock()
	ccfg := &ConsumerConfig{
		Name:     "DowngradeConsumerTest",
		Replicas: 3,
	}
	rg = cc.createGroupForConsumer(ccfg, sa)
	ca := &consumerAssignment{
		Config:  ccfg,
		Group:   rg,
		Stream:  "DowngradeConsumerTest",
		Name:    "DowngradeConsumerTest",
		Created: time.Now().UTC(),
		Client:  ci,
	}
	err = cc.meta.Propose(encodeAddConsumerAssignment(ca))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnConsumerLeader(globalAccountName, "DowngradeConsumerTest", "DowngradeConsumerTest")

	expectConsumerInfo := func(offline bool) {
		if !offline {
			c.waitOnConsumerLeader(globalAccountName, "DowngradeConsumerTest", "DowngradeConsumerTest")
		}
		var msg *nats.Msg
		checkFor(t, 3*time.Second, 200*time.Millisecond, func() error {
			msg, err = nc.Request(fmt.Sprintf(JSApiConsumerInfoT, "DowngradeConsumerTest", "DowngradeConsumerTest"), nil, 2*time.Second)
			return err
		})
		var ci JSApiConsumerInfoResponse
		require_NoError(t, json.Unmarshal(msg.Data, &ci))
		if !offline {
			require_True(t, ci.Error == nil)
		} else {
			require_NotNil(t, ci.Error)
			require_Contains(t, ci.Error.Error(), "consumer is offline", "unsupported", "required API level")
		}
	}

	// Consumer is still supported, so it should be available and healthz should report healthy.
	expectConsumerInfo(false)
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)
	restart()
	expectConsumerInfo(false)
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)

	wsas = getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	require_Equal(t, wsas[0].Config.Name, "DowngradeConsumerTest")
	require_Len(t, len(wsas[0].Consumers), 1)

	// Update a consumer to be unsupported.
	ccfg.Metadata = map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt)}
	sjs.mu.Lock()
	err = cc.meta.Propose(encodeAddConsumerAssignment(ca))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnAllCurrent()

	// Consumer should be reported as offline, but healthz should report healthy to not block downgrades.
	expectConsumerInfo(true)
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)
	restart()
	expectConsumerInfo(true)
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)

	wsas = getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	require_Equal(t, wsas[0].Config.Name, "DowngradeConsumerTest")
	require_Equal(t, wsas[0].Config.Metadata["_nats.req.level"], "0")
	require_Len(t, len(wsas[0].Consumers), 1)
	require_Equal(t, wsas[0].Consumers[0].Config.Metadata["_nats.req.level"], strconv.Itoa(math.MaxInt))
}

func TestJetStreamClusterOfflineStreamAndConsumerStrictDecoding(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	unsupportedJson := []byte("{\"unknown\": true}")

	sa, err := decodeStreamAssignment(s, unsupportedJson)
	require_NoError(t, err)
	require_True(t, bytes.Equal(sa.unsupported.json, unsupportedJson))

	ca, err := decodeConsumerAssignment(unsupportedJson)
	require_NoError(t, err)
	require_True(t, bytes.Equal(ca.unsupported.json, unsupportedJson))

	var bb bytes.Buffer
	s2e := s2.NewWriter(&bb)
	_, err = s2e.Write(unsupportedJson)
	require_NoError(t, err)
	require_NoError(t, s2e.Close())
	ca, err = decodeConsumerAssignmentCompressed(bb.Bytes())
	require_NoError(t, err)
	require_True(t, bytes.Equal(ca.unsupported.json, unsupportedJson))

	var wsa writeableStreamAssignment
	require_NoError(t, wsa.UnmarshalJSON(unsupportedJson))
	require_True(t, bytes.Equal(wsa.unsupportedJson, unsupportedJson))

	var wca writeableConsumerAssignment
	require_NoError(t, wca.UnmarshalJSON(unsupportedJson))
	require_True(t, bytes.Equal(wca.unsupportedJson, unsupportedJson))
}

func TestJetStreamClusterStreamMonitorShutdownWithoutRaftNode(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			if !s.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
				return fmt.Errorf("stream not assigned on %s", s.Name())
			}
		}
		return nil
	})

	var nodes []RaftNode
	for _, s := range c.servers {
		mset, err := s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		// Manually nil-out the node. This shouldn't happen normally,
		// but tests we can shut down purely with the monitor goroutine quit channel.
		mset.mu.Lock()
		n := mset.node
		mset.node = nil
		mset.mu.Unlock()
		require_NotNil(t, n)
		nodes = append(nodes, n)
	}
	for _, n := range nodes {
		require_NotEqual(t, n.State(), Closed)
	}

	require_NoError(t, js.DeleteStream("TEST"))
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, n := range nodes {
			if state := n.State(); state != Closed {
				return fmt.Errorf("node not closed on %s: %s", n.ID(), state.String())
			}
		}
		return nil
	})
}

func TestJetStreamClusterConsumerMonitorShutdownWithoutRaftNode(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:  "DURABLE",
		Replicas: 3,
	})
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			_, cc := s.getJetStreamCluster()
			if !cc.isConsumerAssigned(s.globalAccount(), "TEST", "DURABLE") {
				return fmt.Errorf("consumer not assigned on %s", s.Name())
			}
		}
		return nil
	})

	var nodes []RaftNode
	for _, s := range c.servers {
		mset, err := s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		o := mset.lookupConsumer("DURABLE")
		require_NotNil(t, o)
		// Manually nil-out the node. This shouldn't happen normally,
		// but tests we can shut down purely with the monitor goroutine quit channel.
		o.mu.Lock()
		n := o.node
		o.node = nil
		o.mu.Unlock()
		require_NotNil(t, n)
		nodes = append(nodes, n)
	}
	for _, n := range nodes {
		require_NotEqual(t, n.State(), Closed)
	}

	require_NoError(t, js.DeleteStream("TEST"))
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, n := range nodes {
			if state := n.State(); state != Closed {
				return fmt.Errorf("node not closed on %s: %s", n.ID(), state.String())
			}
		}
		return nil
	})
}

func TestJetStreamClusterUnsetEmptyPlacement(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Placement: &nats.Placement{},
	}
	si, err := js.AddStream(cfg)
	require_NoError(t, err)
	require_True(t, si.Config.Placement == nil)

	si, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	require_True(t, si.Config.Placement == nil)

	// Set a placement level
	cfg.Placement = &nats.Placement{Cluster: "R3S"}
	si, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	require_True(t, si.Config.Placement != nil)
	require_Equal(t, si.Config.Placement.Cluster, "R3S")

	// And ensure it can be reset.
	cfg.Placement = &nats.Placement{}
	si, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	require_True(t, si.Config.Placement == nil)
}

func TestJetStreamClusterPersistModeAsync(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc := clientConnectToServer(t, c.randomServer())
	defer nc.Close()

	cfg := &StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo"},
		Storage:     FileStorage,
		Replicas:    3,
		PersistMode: AsyncPersistMode,
	}
	_, err := jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("async persist mode is not supported on replicated streams")))
}

func TestJetStreamClusterDeleteMsgEOF(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Replicas: replicas,
			})
			require_NoError(t, err)

			_, err = js.Publish("foo", nil)
			require_NoError(t, err)

			require_Error(t, js.DeleteMsg("TEST", 0), NewJSNoMessageFoundError())
			require_NoError(t, js.DeleteMsg("TEST", 1))
			require_Error(t, js.DeleteMsg("TEST", 1), NewJSNoMessageFoundError())
			require_Error(t, js.DeleteMsg("TEST", 2), NewJSStreamMsgDeleteFailedError(ErrStoreEOF))
		})
	}
}

//
// DO NOT ADD NEW TESTS IN THIS FILE (unless to balance test times)
// Add at the end of jetstream_cluster_<n>_test.go, with <n> being the highest value.
//
