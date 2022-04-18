// Copyright 2020-2022 The NATS Authors
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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamClusterConfig(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 16GB, max_file_store: 10TB, store_dir: '%s'}
		cluster { listen: 127.0.0.1:-1 }
	`))
	defer removeFile(t, conf)

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
	defer removeFile(t, conf)

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

func TestJetStreamExpandCluster(t *testing.T) {
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
	require_Error(t, err, NewJSInsufficientResourcesError(), NewJSStorageResourcesExceededError())
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
	_, err := js.AddConsumer("S1", &nats.ConsumerConfig{Durable: "S1C1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js.AddStream(&nats.StreamConfig{Name: "S2"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = js.AddConsumer("S2", &nats.ConsumerConfig{Durable: "S2C1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

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
	_, err = js.AddConsumer("S3", &nats.ConsumerConfig{Durable: "S3C1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	resp, _ = nc.Request(fmt.Sprintf(JSApiConsumerDeleteT, "S1", "S1C1"), nil, time.Second)
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
	require_Error(t, err, NewJSInsufficientResourcesError(), NewJSStorageResourcesExceededError())
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

	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}

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

	expectError := func() {
		if _, err := js.UpdateStream(cfg); err == nil {
			t.Fatalf("Expected error and got none")
		}
	}

	// Subjects
	cfg.Subjects = []string{"bar", "baz"}
	if si := updateStream(); !reflect.DeepEqual(si.Config.Subjects, cfg.Subjects) {
		t.Fatalf("Did not get expected stream info: %+v", si)
	}
	// Mirror changes
	cfg.Replicas = 3
	cfg.Mirror = &nats.StreamSource{Name: "ORDERS"}
	expectError()
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

	sl = c.restartServer(sl)
	c.checkClusterFormed()

	c.waitOnServerCurrent(sl)
	c.waitOnStreamCurrent(sl, "$G", "TEST")
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

	// Faster timeout since we loop below checking for condition.
	js2, err := nc.JetStream(nats.MaxWait(250 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// We may need to wait a bit for peers to catch up.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				if si, err = js2.StreamInfo("TEST"); err != nil {
					t.Fatalf("Could not retrieve stream info")
				}
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
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
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				if si, err = js2.StreamInfo("TEST"); err != nil {
					t.Fatalf("Could not retrieve stream info")
				}
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
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
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
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
	time.Sleep(500 * time.Millisecond)

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
	time.Sleep(500 * time.Millisecond)

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
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy, AckWait: 7500 * time.Millisecond})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	jsub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Ack first 50.
	for _, m := range fetchMsgs(t, jsub, 50, 5*time.Second) {
		m.AckSync()
	}
	// Now ack every third message for next 50.
	for i, m := range fetchMsgs(t, jsub, 50, 5*time.Second) {
		if i%3 == 0 {
			m.AckSync()
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
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "TEST"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	json.Unmarshal(rmsg.Data, &rresp)
	if !IsNatsErr(rresp.Error, JSStreamNameExistErr) {
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
	rmsg, err = nc.Request(rresp.DeliverSubject, nil, time.Second)
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
		si, _ := js.StreamInfo("TEST")
		if si == nil || si.Cluster == nil {
			t.Fatalf("Did not get stream info")
		}
		for _, pi := range si.Cluster.Replicas {
			if !pi.Current {
				return fmt.Errorf("Peer not current: %+v", pi)
			}
		}
		return nil
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
		m.AckSync()
		wantSeq++
	}

	// Check that redelivered come in now..
	redelivered := 50/3 + 1
	fetchMsgs(t, jsub, redelivered, 10*time.Second)

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
		rmsg, err = nc.Request(rresp.DeliverSubject, nil, time.Second)
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
			MaxConsumers: 1,
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

	// This should fail.
	_, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc22", AckPolicy: nats.AckExplicitPolicy})
	if err == nil {
		t.Fatalf("Expected error but got none")
	}
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

	// Wait to accumulate.
	time.Sleep(500 * time.Millisecond)

	ai := getAccountInfo()
	if ai.Streams != 3 || ai.Consumers != 3 {
		t.Fatalf("AccountInfo not correct: %+v", ai)
	}
	if ai.API.Total < 7 {
		t.Fatalf("Expected at least 7 total API calls, got %d", ai.API.Total)
	}

	// Now do a failure to make sure we track API errors.
	js.StreamInfo("NO-STREAM")
	js.ConsumerInfo("TEST-1", "NO-CONSUMER")
	js.ConsumerInfo("TEST-2", "NO-CONSUMER")
	js.ConsumerInfo("TEST-3", "NO-CONSUMER")

	ai = getAccountInfo()
	if ai.API.Errors != 4 {
		t.Fatalf("Expected 4 API calls to be errors, got %d", ai.API.Errors)
	}
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

	checkFor(t, 2*time.Second, 250*time.Millisecond, func() error {
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
			ml.nodeToInfo.Range(func(k, v interface{}) bool {
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
	// Make this shorter for the test.
	oldInterval := lostQuorumInterval
	lostQuorumInterval = hbIntervalDefault * 3
	oldCheck := lostQuorumCheck
	lostQuorumCheck = hbIntervalDefault * 2
	defer func() {
		lostQuorumInterval = oldInterval
		lostQuorumCheck = oldCheck
	}()

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
		if audit.Subject == "" {
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

	sl := c.streamLeader("$G", "TEST")
	sl.Shutdown()
	c.restartServer(sl)
	c.waitOnStreamLeader("$G", "TEST")

	// Send second msg
	js.Publish("foo", []byte("msg2"))
	msg, err := sub.NextMsg(time.Second)
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

func TestJetStreamClusterSuperClusterMetaPlacement(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// We want to influence where the meta leader will place itself when we ask the
	// current leader to stepdown.
	ml := sc.leader()
	cn := ml.ClusterName()
	var pcn string
	for _, c := range sc.clusters {
		if c.name != cn {
			pcn = c.name
			break
		}
	}

	// Client based API
	s := sc.randomServer()
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer nc.Close()

	stepdown := func(cn string) *JSApiLeaderStepDownResponse {
		req := &JSApiLeaderStepdownRequest{Placement: &Placement{Cluster: cn}}
		jreq, err := json.Marshal(req)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		resp, err := nc.Request(JSApiLeaderStepDown, jreq, time.Second)
		if err != nil {
			t.Fatalf("Error on stepdown request: %v", err)
		}
		var sdr JSApiLeaderStepDownResponse
		if err := json.Unmarshal(resp.Data, &sdr); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return &sdr
	}

	// Make sure we get correct errors for tags and bad or unavailable cluster placement.
	sdr := stepdown("C22")
	if sdr.Error == nil || !strings.Contains(sdr.Error.Description, "no suitable peers") {
		t.Fatalf("Got incorrect error result: %+v", sdr.Error)
	}
	// Should work.
	sdr = stepdown(pcn)
	if sdr.Error != nil {
		t.Fatalf("Got an error on stepdown: %+v", sdr.Error)
	}

	sc.waitOnLeader()
	ml = sc.leader()
	cn = ml.ClusterName()

	if cn != pcn {
		t.Fatalf("Expected new metaleader to be in cluster %q, got %q", pcn, cn)
	}
}

func TestJetStreamUniquePlacementTag(t *testing.T) {
	tmlp := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s', unique_tag: az}
		leaf {listen: 127.0.0.1:-1}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	s := createJetStreamSuperClusterWithTemplateAndModHook(t, tmlp, 5, 2,
		func(serverName, clustername, storeDir, conf string) string {
			azTag := map[string]string{
				"C1-S1": "az:same",
				"C1-S2": "az:same",
				"C1-S3": "az:same",
				"C1-S4": "az:same",
				"C1-S5": "az:same",
				"C2-S1": "az:1",
				"C2-S2": "az:2",
				"C2-S3": "az:1",
				"C2-S4": "az:2",
				"C2-S5": "az:1",
			}
			return conf + fmt.Sprintf("\nserver_tags: [cloud:%s-tag, %s]\n", clustername, azTag[serverName])
		})
	defer s.shutdown()

	nc := natsConnect(t, s.randomServer().ClientURL())
	defer nc.Close()

	js, err := nc.JetStream()
	require_NoError(t, err)

	for i, test := range []struct {
		placement *nats.Placement
		replicas  int
		fail      bool
		cluster   string
	}{
		// these pass because replica count is 1
		{&nats.Placement{Tags: []string{"az:same"}}, 1, false, "C1"},
		{&nats.Placement{Tags: []string{"cloud:C1-tag", "az:same"}}, 1, false, "C1"},
		{&nats.Placement{Tags: []string{"cloud:C1-tag"}}, 1, false, "C1"},
		// pass because az is set, which disables the filter
		{&nats.Placement{Tags: []string{"az:same"}}, 2, false, "C1"},
		{&nats.Placement{Tags: []string{"cloud:C1-tag", "az:same"}}, 2, false, "C1"},
		// fails because this cluster only has the same az
		{&nats.Placement{Tags: []string{"cloud:C1-tag"}}, 2, true, ""},
		// fails because no 3 unique tags exist
		{&nats.Placement{Tags: []string{"cloud:C2-tag"}}, 3, true, ""},
		{nil, 3, true, ""},
		// pass because replica count is low enough
		{nil, 2, false, "C2"},
		{&nats.Placement{Tags: []string{"cloud:C2-tag"}}, 2, false, "C2"},
		// pass because az is provided
		{&nats.Placement{Tags: []string{"az:1"}}, 3, false, "C2"},
		{&nats.Placement{Tags: []string{"az:2"}}, 2, false, "C2"},
	} {
		name := fmt.Sprintf("test-%d", i)
		t.Run(name, func(t *testing.T) {
			ci, err := js.AddStream(&nats.StreamConfig{Name: name, Replicas: test.replicas, Placement: test.placement})
			if test.fail {
				require_Error(t, err)
				require_Equal(t, err.Error(), "insufficient resources")
				return
			}
			require_NoError(t, err)
			if test.cluster != _EMPTY_ {
				require_Equal(t, ci.Cluster.Name, test.cluster)
			}
		})
	}
}

func TestJetStreamClusterSuperClusterBasics(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// Client based API
	s := sc.randomServer()
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
	// Check request origin placement.
	if si.Cluster.Name != s.ClusterName() {
		t.Fatalf("Expected stream to be placed in %q, but got %q", s.ClusterName(), si.Cluster.Name)
	}

	// Check consumers.
	sub, err := js.SubscribeSync("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, sub, toSend)
	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.Delivered.Consumer != uint64(toSend) || ci.NumAckPending != toSend {
		t.Fatalf("ConsumerInfo is not correct: %+v", ci)
	}

	// Now check we can place a stream.
	pcn := "C3"
	scResp, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST2",
		Placement: &nats.Placement{Cluster: pcn},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if scResp.Cluster.Name != pcn {
		t.Fatalf("Expected the stream to be placed in %q, got %q", pcn, scResp.Cluster.Name)
	}
}

// Test that consumer interest across gateways and superclusters is properly identitifed in a remote cluster.
func TestJetStreamClusterSuperClusterCrossClusterConsumerInterest(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// Since we need all of the peers accounted for to add the stream wait for all to be present.
	sc.waitOnPeerCount(9)

	// Client based API - Connect to Cluster C1. Stream and consumer will live in C2.
	s := sc.clusterForName("C1").randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	pcn := "C2"
	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 3, Placement: &nats.Placement{Cluster: pcn}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Pull based first.
	sub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send a message.
	if _, err = js.Publish("foo", []byte("CCI")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	fetchMsgs(t, sub, 1, 5*time.Second)

	// Now check push based delivery.
	sub, err = js.SubscribeSync("foo", nats.Durable("rip"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, sub, 1)

	// Send another message.
	if _, err = js.Publish("foo", []byte("CCI")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	checkSubsPending(t, sub, 2)
}

func TestJetStreamClusterSuperClusterPeerReassign(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// Client based API
	s := sc.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	pcn := "C2"

	// Create a stream in C2 that sources TEST
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Placement: &nats.Placement{Cluster: pcn},
		Replicas:  3,
	})
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
	// Check request origin placement.
	if si.Cluster.Name != pcn {
		t.Fatalf("Expected stream to be placed in %q, but got %q", s.ClusterName(), si.Cluster.Name)
	}

	// Now remove a peer that is assigned to the stream.
	rc := sc.clusterForName(pcn)
	rs := rc.randomNonStreamLeader("$G", "TEST")
	rc.removeJetStream(rs)

	// Check the stream info is eventually correct.
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
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
			if !strings.HasPrefix(peer.Name, pcn) {
				t.Fatalf("Stream peer reassigned to wrong cluster: %q", peer.Name)
			}
		}
		return nil
	})
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
		_, js := jsClientConnect(t, s)
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
	rand.Read(msg)

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

func TestJetStreamClusterMirrorAndSourcesClusterRestart(t *testing.T) {
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Create Mirror now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Mirror:   &nats.StreamSource{Name: "TEST"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sendBatch := func(subject string, n int) {
		t.Helper()
		// Send a batch to a given subject.
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	checkSync := func() {
		t.Helper()
		checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
			tsi, err := js.StreamInfo("TEST")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msi, err := js.StreamInfo("M")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if tsi.State.Msgs != msi.State.Msgs {
				return fmt.Errorf("Total messages not the same: TEST %d vs M %d", tsi.State.Msgs, msi.State.Msgs)
			}
			return nil
		})
	}

	// Send 100 msgs.
	sendBatch("foo", 100)
	checkSync()

	c.stopAll()
	c.restartAll()
	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnStreamLeader("$G", "M")

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	sendBatch("bar", 100)
	checkSync()
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
	expectFail("M", "baz")
	expectFail("M", "baz.1.2")
	expectFail("M", "apple")

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
	expectFail("S", "baz")
	expectFail("S", "baz.1")
	expectFail("S", "apple")

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

func TestJetStreamCrossAccountMirrorsAndSources(t *testing.T) {
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

	testPrefix("mirror-bad-deliverprefix", JSStreamExternalDelPrefixOverlapsErrF, StreamConfig{
		Name:    "MY_MIRROR_TEST",
		Storage: FileStorage,
		Mirror: &StreamSource{
			Name: "TEST",
			External: &ExternalStream{
				ApiPrefix: "RI.JS.API",
				// this will result in test.test.> which test.> would match
				DeliverPrefix: "test",
			},
		},
	})
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
	testPrefix("source-bad-deliverprefix", JSStreamExternalDelPrefixOverlapsErrF, StreamConfig{
		Name:    "MY_SOURCE_TEST",
		Storage: FileStorage,
		Sources: []*StreamSource{{
			Name: "TEST",
			External: &ExternalStream{
				ApiPrefix:     "RI.JS.API",
				DeliverPrefix: "test",
			},
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

func TestJetStreamClusterJSAPIImport(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterImportsTempl, "C1", 3)
	defer c.shutdown()

	// Client based API - This will connect to the non-js account which imports JS.
	// Connect below does an AccountInfo call.
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Note if this was ephemeral we would need to setup export/import for that subject.
	sub, err := js.SubscribeSync("TEST", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Make sure we can look up both.
	if _, err := js.StreamInfo("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := sub.ConsumerInfo(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Names list..
	var names []string
	for name := range js.StreamNames() {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(names))
	}

	// Now send to stream.
	if _, err := js.Publish("TEST", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	sub, err = js.PullSubscribe("TEST", "tr")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msgs := fetchMsgs(t, sub, 1, 5*time.Second)

	m := msgs[0]
	if m.Subject != "TEST" {
		t.Fatalf("Expected subject of %q, got %q", "TEST", m.Subject)
	}
	if m.Header != nil {
		t.Fatalf("Expected no header on the message, got: %v", m.Header)
	}
	meta, err := m.Metadata()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if meta.Sequence.Consumer != 1 || meta.Sequence.Stream != 1 || meta.NumDelivered != 1 || meta.NumPending != 0 {
		t.Fatalf("Bad meta: %+v", meta)
	}

	js.Publish("TEST", []byte("Second"))
	js.Publish("TEST", []byte("Third"))

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "tr")
		if err != nil {
			return fmt.Errorf("Error getting consumer info: %v", err)
		}
		if ci.NumPending != 2 {
			return fmt.Errorf("NumPending still not 1: %v", ci.NumPending)
		}
		return nil
	})

	// Ack across accounts.
	m, err = nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.tr", []byte("+NXT"), 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	meta, err = m.Metadata()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if meta.Sequence.Consumer != 2 || meta.Sequence.Stream != 2 || meta.NumDelivered != 1 || meta.NumPending != 1 {
		t.Fatalf("Bad meta: %+v", meta)
	}

	// AckNext
	_, err = nc.Request(m.Reply, []byte("+NXT"), 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterSuperClusterInterestOnlyMode(t *testing.T) {
	template := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		accounts {
			one {
				jetstream: enabled
				users [{user: one, password: password}]
			}
			two {
				%s
				users [{user: two, password: password}]
			}
		}
		cluster {
			listen: 127.0.0.1:%d
			name: %s
			routes = ["nats://127.0.0.1:%d"]
		}
		gateway {
			name: %s
			listen: 127.0.0.1:%d
			gateways = [{name: %s, urls: ["nats://127.0.0.1:%d"]}]
		}
	`
	storeDir1 := createDir(t, JetStreamStoreDir)
	conf1 := createConfFile(t, []byte(fmt.Sprintf(template,
		"S1", storeDir1, "", 23222, "A", 23222, "A", 11222, "B", 11223)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	storeDir2 := createDir(t, JetStreamStoreDir)
	conf2 := createConfFile(t, []byte(fmt.Sprintf(template,
		"S2", storeDir2, "", 23223, "B", 23223, "B", 11223, "A", 11222)))
	s2, o2 := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	waitForInboundGateways(t, s1, 1, 2*time.Second)
	waitForInboundGateways(t, s2, 1, 2*time.Second)
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)

	nc1 := natsConnect(t, fmt.Sprintf("nats://two:password@127.0.0.1:%d", o1.Port))
	defer nc1.Close()
	nc1.Publish("foo", []byte("some message"))
	nc1.Flush()

	nc2 := natsConnect(t, fmt.Sprintf("nats://two:password@127.0.0.1:%d", o2.Port))
	defer nc2.Close()
	nc2.Publish("bar", []byte("some message"))
	nc2.Flush()

	checkMode := func(accName string, expectedMode GatewayInterestMode) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			servers := []*Server{s1, s2}
			for _, s := range servers {
				var gws []*client
				s.getInboundGatewayConnections(&gws)
				for _, gw := range gws {
					var mode GatewayInterestMode
					gw.mu.Lock()
					ie := gw.gw.insim[accName]
					if ie != nil {
						mode = ie.mode
					}
					gw.mu.Unlock()
					if ie == nil {
						return fmt.Errorf("Account %q not in map", accName)
					}
					if mode != expectedMode {
						return fmt.Errorf("Expected account %q mode to be %v, got: %v", accName, expectedMode, mode)
					}
				}
			}
			return nil
		})
	}

	checkMode("one", InterestOnly)
	checkMode("two", Optimistic)

	// Now change account "two" to enable JS
	changeCurrentConfigContentWithNewContent(t, conf1, []byte(fmt.Sprintf(template,
		"S1", storeDir1, "jetstream: enabled", 23222, "A", 23222, "A", 11222, "B", 11223)))
	changeCurrentConfigContentWithNewContent(t, conf2, []byte(fmt.Sprintf(template,
		"S2", storeDir2, "jetstream: enabled", 23223, "B", 23223, "B", 11223, "A", 11222)))

	if err := s1.Reload(); err != nil {
		t.Fatalf("Error on s1 reload: %v", err)
	}
	if err := s2.Reload(); err != nil {
		t.Fatalf("Error on s2 reload: %v", err)
	}

	checkMode("one", InterestOnly)
	checkMode("two", InterestOnly)
}

func TestJetStreamClusterSuperClusterEphemeralCleanup(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	// Create a stream in cluster 0
	s := sc.clusters[0].randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for _, test := range []struct {
		name            string
		sourceInCluster int
		streamName      string
		sourceName      string
	}{
		{"local", 0, "TEST1", "S1"},
		{"remote", 1, "TEST2", "S2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := js.AddStream(&nats.StreamConfig{Name: test.streamName, Replicas: 3}); err != nil {
				t.Fatalf("Error adding %q stream: %v", test.streamName, err)
			}
			if _, err := js.Publish(test.streamName, []byte("hello")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}

			// Now create a source for that stream, either in same or remote cluster.
			s2 := sc.clusters[test.sourceInCluster].randomServer()
			nc2, js2 := jsClientConnect(t, s2)
			defer nc2.Close()

			if _, err := js2.AddStream(&nats.StreamConfig{
				Name:     test.sourceName,
				Storage:  nats.FileStorage,
				Sources:  []*nats.StreamSource{{Name: test.streamName}},
				Replicas: 1,
			}); err != nil {
				t.Fatalf("Error adding source stream: %v", err)
			}

			// Check that TEST(n) has 1 consumer and that S(n) is created and has 1 message.
			checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
				si, err := js2.StreamInfo(test.sourceName)
				if err != nil {
					return fmt.Errorf("Could not get stream info: %v", err)
				}
				if si.State.Msgs != 1 {
					return fmt.Errorf("Expected 1 msg, got state: %+v", si.State)
				}
				return nil
			})

			// Get the consumer because we will want to artificially reduce
			// the delete threshold.
			leader := sc.clusters[0].streamLeader("$G", test.streamName)
			mset, err := leader.GlobalAccount().lookupStream(test.streamName)
			if err != nil {
				t.Fatalf("Expected to find a stream for %q, got %v", test.streamName, err)
			}
			cons := mset.getConsumers()[0]
			cons.mu.Lock()
			cons.dthresh = 1250 * time.Millisecond
			active := cons.active
			dtimerSet := cons.dtmr != nil
			deliver := cons.cfg.DeliverSubject
			cons.mu.Unlock()

			if !active || dtimerSet {
				t.Fatalf("Invalid values for active=%v dtimerSet=%v", active, dtimerSet)
			}
			// To add to the mix, let's create a local interest on the delivery subject
			// and stop it. This is to ensure that this does not stop timers that should
			// still be running and monitor the GW interest.
			sub := natsSubSync(t, nc, deliver)
			natsFlush(t, nc)
			natsUnsub(t, sub)
			natsFlush(t, nc)

			// Now remove the "S(n)" stream...
			if err := js2.DeleteStream(test.sourceName); err != nil {
				t.Fatalf("Error deleting stream: %v", err)
			}

			// Now check that the stream S(n) is really removed and that
			// the consumer is gone for stream TEST(n).
			checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
				// First, make sure that stream S(n) has disappeared.
				if _, err := js2.StreamInfo(test.sourceName); err == nil {
					return fmt.Errorf("Stream %q should no longer exist", test.sourceName)
				}
				if ndc := mset.numDirectConsumers(); ndc != 0 {
					return fmt.Errorf("Expected %q stream to have 0 consumers, got %v", test.streamName, ndc)
				}
				return nil
			})
		})
	}
}

func TestJetStreamSuperClusterConnectionCount(t *testing.T) {
	sc := createJetStreamSuperClusterWithTemplate(t, jsClusterAccountsTempl, 3, 2)
	defer sc.shutdown()

	sysNc := natsConnect(t, sc.randomServer().ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	defer sysNc.Close()
	_, err := sysNc.Request(fmt.Sprintf(accReqSubj, "ONE", "CONNS"), nil, 100*time.Millisecond)
	// this is a timeout as the server only responds when it has connections....
	// not convinced this should be that way, but also not the issue to investigate.
	require_True(t, err == nats.ErrTimeout)

	for i := 1; i <= 2; i++ {
		func() {
			nc := natsConnect(t, sc.clusterForName(fmt.Sprintf("C%d", i)).randomServer().ClientURL())
			defer nc.Close()
			js, err := nc.JetStream()
			require_NoError(t, err)
			name := fmt.Sprintf("foo%d", 1)
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     name,
				Subjects: []string{name},
				Replicas: 3})
			require_NoError(t, err)
		}()
	}
	func() {
		nc := natsConnect(t, sc.clusterForName("C1").randomServer().ClientURL())
		defer nc.Close()
		js, err := nc.JetStream()
		require_NoError(t, err)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "src",
			Sources:  []*nats.StreamSource{{Name: "foo.1"}, {Name: "foo.2"}},
			Replicas: 3})
		require_NoError(t, err)
	}()
	func() {
		nc := natsConnect(t, sc.clusterForName("C2").randomServer().ClientURL())
		defer nc.Close()
		js, err := nc.JetStream()
		require_NoError(t, err)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "mir",
			Mirror:   &nats.StreamSource{Name: "foo.2"},
			Replicas: 3})
		require_NoError(t, err)
	}()

	// There should be no active NATS CLIENT connections, but we still need
	// to wait a little bit...
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		_, err := sysNc.Request(fmt.Sprintf(accReqSubj, "ONE", "CONNS"), nil, 100*time.Millisecond)
		if err != nats.ErrTimeout {
			return fmt.Errorf("Expected timeout, got %v", err)
		}
		return nil
	})
	sysNc.Close()

	s := sc.randomServer()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		acc, err := s.lookupAccount("ONE")
		if err != nil {
			t.Fatalf("Could not look up account: %v", err)
		}
		if n := acc.NumConnections(); n != 0 {
			return fmt.Errorf("Expected no connections, got %d", n)
		}
		return nil
	})
}

func TestJetStreamSuperClusterDirectConsumersBrokenGateways(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 1, 2)
	defer sc.shutdown()

	// Client based API
	s := sc.clusterForName("C1").randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// This will be in C1.
	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create a stream in C2 that sources TEST
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      "S",
		Placement: &nats.Placement{Cluster: "C2"},
		Sources:   []*nats.StreamSource{{Name: "TEST"}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Wait for direct consumer to get registered and detect interest across GW.
	time.Sleep(time.Second)

	// Send 100 msgs over 100ms in separate Go routine.
	msg, toSend, done := []byte("Hello"), 100, make(chan bool)
	go func() {
		// Send in 10 messages.
		for i := 0; i < toSend; i++ {
			if _, err = js.Publish("TEST", msg); err != nil {
				t.Errorf("Unexpected publish error: %v", err)
			}
			time.Sleep(500 * time.Microsecond)
		}
		done <- true
	}()

	breakGW := func() {
		s.gateway.Lock()
		gw := s.gateway.out["C2"]
		s.gateway.Unlock()
		if gw != nil {
			gw.closeConnection(ClientClosed)
		}
	}

	// Wait til about half way through.
	time.Sleep(20 * time.Millisecond)
	// Now break GW connection.
	breakGW()

	// Wait for GW to reform.
	for _, c := range sc.clusters {
		for _, s := range c.servers {
			waitForOutboundGateways(t, s, 1, 2*time.Second)
		}
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not complete sending first batch of messages")
	}

	// Make sure we can deal with data loss at the end.
	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("S")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 100 {
			return fmt.Errorf("Expected to have %d messages, got %d", 100, si.State.Msgs)
		}
		return nil
	})

	// Now send 100 more. Will aos break here in the middle.
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
		if i == 50 {
			breakGW()
		}
	}

	// Wait for GW to reform.
	for _, c := range sc.clusters {
		for _, s := range c.servers {
			waitForOutboundGateways(t, s, 1, 2*time.Second)
		}
	}

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != 200 {
		t.Fatalf("Expected to have %d messages, got %d", 200, si.State.Msgs)
	}

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("S")
		if err != nil {
			return fmt.Errorf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 200 {
			return fmt.Errorf("Expected to have %d messages, got %d", 200, si.State.Msgs)
		}
		return nil
	})
}

func TestJetStreamClusterMultiRestartBug(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send in 10000 messages.
	msg, toSend := make([]byte, 4*1024), 10000
	rand.Read(msg)

	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

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

	// For this bug, we will stop and remove the complete state from one server.
	s := c.randomServer()
	opts := s.getOpts()
	s.Shutdown()
	removeDir(t, opts.StoreDir)

	// Then restart it.
	c.restartAll()
	c.waitOnAllCurrent()
	c.waitOnStreamLeader("$G", "TEST")

	s = c.serverByName(s.Name())
	c.waitOnStreamCurrent(s, "$G", "TEST")

	// Now restart them all..
	c.stopAll()
	c.restartAll()
	c.waitOnLeader()
	c.waitOnStreamLeader("$G", "TEST")

	// Create new client.
	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Make sure the replicas are current.
	js2, err := nc.JetStream(nats.MaxWait(250 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		si, _ := js2.StreamInfo("TEST")
		if si == nil || si.Cluster == nil {
			t.Fatalf("Did not get stream info")
		}
		for _, pi := range si.Cluster.Replicas {
			if !pi.Current {
				return fmt.Errorf("Peer not current: %+v", pi)
			}
		}
		return nil
	})
}

func TestJetStreamClusterServerLimits(t *testing.T) {
	// 2MB memory, 8MB disk
	c := createJetStreamClusterWithTemplate(t, jsClusterLimitsTempl, "R3L", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	msg, toSend := make([]byte, 4*1024), 5000
	rand.Read(msg)

	// Memory first.
	max_mem := uint64(2*1024*1024) + uint64(len(msg))

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TM",
		Replicas: 3,
		Storage:  nats.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TM", msg); err != nil {
			break
		}
	}
	if err == nil || !strings.HasPrefix(err.Error(), "nats: insufficient resources") {
		t.Fatalf("Expected a ErrJetStreamResourcesExceeded error, got %v", err)
	}

	si, err := js.StreamInfo("TM")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Bytes > max_mem {
		t.Fatalf("Expected bytes of %v to not be greater then %v",
			friendlyBytes(int64(si.State.Bytes)),
			friendlyBytes(int64(max_mem)),
		)
	}

	c.waitOnLeader()

	// Now disk.
	max_disk := uint64(8*1024*1024) + uint64(len(msg))

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TF",
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TF", msg); err != nil {
			break
		}
	}
	if err == nil || !strings.HasPrefix(err.Error(), "nats: insufficient resources") {
		t.Fatalf("Expected a ErrJetStreamResourcesExceeded error, got %v", err)
	}

	si, err = js.StreamInfo("TF")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Bytes > max_disk {
		t.Fatalf("Expected bytes of %v to not be greater then %v",
			friendlyBytes(int64(si.State.Bytes)),
			friendlyBytes(int64(max_disk)),
		)
	}
}

func TestJetStreamClusterAccountLoadFailure(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterLimitsTempl, "R3L", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.leader())
	defer nc.Close()

	// Remove the "ONE" account from non-leader
	s := c.randomNonLeader()
	s.mu.Lock()
	s.accounts.Delete("ONE")
	s.mu.Unlock()

	_, err := js.AddStream(&nats.StreamConfig{Name: "F", Replicas: 3})
	if err == nil || !strings.Contains(err.Error(), "account not found") {
		t.Fatalf("Expected an 'account not found' error but got %v", err)
	}
}

func TestJetStreamClusterAckPendingWithExpired(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
		MaxAge:   500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send in 100 messages.
	msg, toSend := make([]byte, 256), 100
	rand.Read(msg)

	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, toSend)
	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.NumAckPending != toSend {
		t.Fatalf("Expected %d to be pending, got %d", toSend, ci.NumAckPending)
	}

	// Wait for messages to expire.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 0 {
			return fmt.Errorf("Expected 0 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// Once expired these messages can not be redelivered so should not be considered ack pending at this point.
	// Now ack..
	ci, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.NumAckPending != 0 {
		t.Fatalf("Expected nothing to be ack pending, got %d", ci.NumAckPending)
	}
}

func TestJetStreamClusterAckPendingWithMaxRedelivered(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send in 100 messages.
	msg, toSend := []byte("HELLO"), 100

	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sub, err := js.SubscribeSync("foo",
		nats.MaxDeliver(2),
		nats.Durable("dlc"),
		nats.AckWait(10*time.Millisecond),
		nats.MaxAckPending(50),
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, toSend*2)

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		ci, err := sub.ConsumerInfo()
		if err != nil {
			return err
		}
		if ci.NumAckPending != 0 {
			return fmt.Errorf("Expected nothing to be ack pending, got %d", ci.NumAckPending)
		}
		return nil
	})
}

func TestJetStreamClusterMixedMode(t *testing.T) {
	for _, test := range []struct {
		name string
		tmpl string
	}{
		{"multi-account", jsClusterLimitsTempl},
		{"global-account", jsMixedModeGlobalAccountTempl},
	} {
		t.Run(test.name, func(t *testing.T) {

			c := createMixedModeCluster(t, test.tmpl, "MM5", _EMPTY_, 3, 2, true)
			defer c.shutdown()

			// Client based API - Non-JS server.
			nc, js := jsClientConnect(t, c.serverByName("S-5"))
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo", "bar"},
				Replicas: 3,
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ml := c.leader()
			if ml == nil {
				t.Fatalf("No metaleader")
			}

			// Make sure we are tracking only the JS peers.
			checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
				peers := ml.JetStreamClusterPeers()
				if len(peers) == 3 {
					return nil
				}
				return fmt.Errorf("Not correct number of peers, expected %d, got %d", 3, len(peers))
			})

			// Grab the underlying raft structure and make sure the system adjusts its cluster set size.
			meta := ml.getJetStream().getMetaGroup().(*raft)
			checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
				ps := meta.currentPeerState()
				if len(ps.knownPeers) != 3 {
					return fmt.Errorf("Expected known peers to be 3, but got %+v", ps.knownPeers)
				}
				if ps.clusterSize < 3 {
					return fmt.Errorf("Expected cluster size to be 3, but got %+v", ps)
				}
				return nil
			})
		})
	}
}

func TestJetStreamClusterLeafnodeSpokes(t *testing.T) {
	c := createJetStreamCluster(t, jsClusterTempl, "HUB", _EMPTY_, 3, 22020, false)
	defer c.shutdown()

	lnc1 := c.createLeafNodesWithStartPortAndDomain("R1", 3, 22110, _EMPTY_)
	defer lnc1.shutdown()

	lnc2 := c.createLeafNodesWithStartPortAndDomain("R2", 3, 22120, _EMPTY_)
	defer lnc2.shutdown()

	lnc3 := c.createLeafNodesWithStartPortAndDomain("R3", 3, 22130, _EMPTY_)
	defer lnc3.shutdown()

	// Wait on all peers.
	c.waitOnPeerCount(12)

	// Make sure shrinking works.
	lnc3.shutdown()
	c.waitOnPeerCount(9)

	lnc3 = c.createLeafNodesWithStartPortAndDomain("LNC3", 3, 22130, _EMPTY_)
	defer lnc3.shutdown()

	c.waitOnPeerCount(12)
}

func TestJetStreamClusterSuperClusterAndLeafNodesWithSharedSystemAccountAndSameDomain(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	lnc := sc.createLeafNodes("LNC", 2)
	defer lnc.shutdown()

	// We want to make sure there is only one leader and its always in the supercluster.
	sc.waitOnLeader()

	if ml := lnc.leader(); ml != nil {
		t.Fatalf("Detected a meta-leader in the leafnode cluster: %s", ml)
	}

	// leafnodes should have been added into the overall peer count.
	sc.waitOnPeerCount(8)

	// Check here that we auto detect sharing system account as well and auto place the correct
	// deny imports and exports.
	ls := lnc.randomServer()
	if ls == nil {
		t.Fatalf("Expected a leafnode server, got none")
	}
	gacc := ls.globalAccount().GetName()

	ls.mu.Lock()
	var hasDE, hasDI bool
	for _, ln := range ls.leafs {
		ln.mu.Lock()
		if ln.leaf.remote.RemoteLeafOpts.LocalAccount == gacc {
			re := ln.perms.pub.deny.Match(jsAllAPI)
			hasDE = len(re.psubs)+len(re.qsubs) > 0
			rs := ln.perms.sub.deny.Match(jsAllAPI)
			hasDI = len(rs.psubs)+len(rs.qsubs) > 0
		}
		ln.mu.Unlock()
	}
	ls.mu.Unlock()

	if !hasDE {
		t.Fatalf("No deny export on global account")
	}
	if !hasDI {
		t.Fatalf("No deny import on global account")
	}

	// Make a stream by connecting to the leafnode cluster. Make sure placement is correct.
	// Client based API
	nc, js := jsClientConnect(t, lnc.randomServer())
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster.Name != "LNC" {
		t.Fatalf("Expected default placement to be %q, got %q", "LNC", si.Cluster.Name)
	}

	// Now make sure placement also works if we want to place in a cluster in the supercluster.
	pcn := "C2"
	si, err = js.AddStream(&nats.StreamConfig{
		Name:      "TEST2",
		Subjects:  []string{"baz"},
		Replicas:  2,
		Placement: &nats.Placement{Cluster: pcn},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster.Name != pcn {
		t.Fatalf("Expected default placement to be %q, got %q", pcn, si.Cluster.Name)
	}
}

func TestJetStreamClusterSuperClusterAndLeafNodesWithSharedSystemAccountAndDifferentDomain(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	lnc := sc.createLeafNodesWithDomain("LNC", 2, "LEAFDOMAIN")
	defer lnc.shutdown()

	// We want to make sure there is only one leader and its always in the supercluster.
	sc.waitOnLeader()
	lnc.waitOnLeader()

	// even though system account is shared, because domains differ,
	sc.waitOnPeerCount(6)
	lnc.waitOnPeerCount(2)

	// Check here that we auto detect sharing system account as well and auto place the correct
	// deny imports and exports.
	ls := lnc.randomServer()
	if ls == nil {
		t.Fatalf("Expected a leafnode server, got none")
	}
	gacc := ls.globalAccount().GetName()

	ls.mu.Lock()
	var hasDE, hasDI bool
	for _, ln := range ls.leafs {
		ln.mu.Lock()
		if ln.leaf.remote.RemoteLeafOpts.LocalAccount == gacc {
			re := ln.perms.pub.deny.Match(jsAllAPI)
			hasDE = len(re.psubs)+len(re.qsubs) > 0
			rs := ln.perms.sub.deny.Match(jsAllAPI)
			hasDI = len(rs.psubs)+len(rs.qsubs) > 0
		}
		ln.mu.Unlock()
	}
	ls.mu.Unlock()

	if !hasDE {
		t.Fatalf("No deny export on global account")
	}
	if !hasDI {
		t.Fatalf("No deny import on global account")
	}

	// Make a stream by connecting to the leafnode cluster. Make sure placement is correct.
	// Client based API
	nc, js := jsClientConnect(t, lnc.randomServer())
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster.Name != "LNC" {
		t.Fatalf("Expected default placement to be %q, got %q", "LNC", si.Cluster.Name)
	}

	// Now make sure placement does not works for cluster in different domain
	pcn := "C2"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      "TEST2",
		Subjects:  []string{"baz"},
		Replicas:  2,
		Placement: &nats.Placement{Cluster: pcn},
	})
	if err == nil || !strings.Contains(err.Error(), "insufficient resources") {
		t.Fatalf("Expected insufficient resources, got: %v", err)
	}
}

func TestJetStreamClusterSuperClusterAndSingleLeafNodeWithSharedSystemAccount(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	ln := sc.createSingleLeafNode(true)
	defer ln.Shutdown()

	// We want to make sure there is only one leader and its always in the supercluster.
	sc.waitOnLeader()

	// leafnodes should have been added into the overall peer count.
	sc.waitOnPeerCount(7)

	// Now make sure we can place a stream in the leaf node.
	// First connect to the leafnode server itself.
	nc, js := jsClientConnect(t, ln)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST1",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster.Name != "LNS" {
		t.Fatalf("Expected to be placed in leafnode with %q as cluster name, got %q", "LNS", si.Cluster.Name)
	}
	// Now check we can place on here as well but connect to the hub.
	nc, js = jsClientConnect(t, sc.randomServer())
	defer nc.Close()

	si, err = js.AddStream(&nats.StreamConfig{
		Name:      "TEST2",
		Subjects:  []string{"bar"},
		Placement: &nats.Placement{Cluster: "LNS"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster.Name != "LNS" {
		t.Fatalf("Expected to be placed in leafnode with %q as cluster name, got %q", "LNS", si.Cluster.Name)
	}
}

func TestJetStreamClusterLeafNodeDenyNoDupe(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: CORE, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "CORE", _EMPTY_, 3, 18033, true)
	defer c.shutdown()

	tmpl = strings.Replace(jsClusterTemplWithSingleLeafNode, "store_dir:", "domain: SPOKE, store_dir:", 1)
	ln := c.createLeafNodeWithTemplate("LN-SPOKE", tmpl)
	defer ln.Shutdown()

	checkLeafNodeConnectedCount(t, ln, 2)

	// Now disconnect our leafnode connections by restarting the server we are connected to..
	for _, s := range c.servers {
		if s.ClusterName() != c.name {
			continue
		}
		if nln := s.NumLeafNodes(); nln > 0 {
			s.Shutdown()
			c.restartServer(s)
		}
	}
	// Make sure we are back connected.
	checkLeafNodeConnectedCount(t, ln, 2)

	// Now grab leaf varz and make sure we have no dupe deny clauses.
	vz, err := ln.Varz(nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Grab the correct remote.
	for _, remote := range vz.LeafNode.Remotes {
		if remote.LocalAccount == ln.SystemAccount().Name {
			if remote.Deny != nil && len(remote.Deny.Exports) > 3 { // denyAll := []string{jscAllSubj, raftAllSubj, jsAllAPI}
				t.Fatalf("Dupe entries found: %+v", remote.Deny)
			}
			break
		}
	}
}

// Multiple JS domains.
func TestJetStreamClusterSingleLeafNodeWithoutSharedSystemAccount(t *testing.T) {
	c := createJetStreamCluster(t, strings.Replace(jsClusterAccountsTempl, "store_dir", "domain: CORE, store_dir", 1), "HUB", _EMPTY_, 3, 14333, true)
	defer c.shutdown()

	ln := c.createSingleLeafNodeNoSystemAccount()
	defer ln.Shutdown()

	// The setup here has a single leafnode server with two accounts. One has JS, the other does not.
	// We want to to test the following.
	// 1. For the account without JS, we simply will pass through to the HUB. Meaning since our local account
	//    does not have it, we simply inherit the hub's by default.
	// 2. For the JS enabled account, we are isolated and use our local one only.

	// Check behavior of the account without JS.
	// Normally this should fail since our local account is not enabled. However, since we are bridging
	// via the leafnode we expect this to work here.
	nc, js := jsClientConnectEx(t, ln, "CORE", nats.UserInfo("n", "p"))
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil || si.Cluster.Name != "HUB" {
		t.Fatalf("Expected stream to be placed in %q", "HUB")
	}
	// Do some other API calls.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	seen := 0
	for name := range js.StreamNames() {
		seen++
		if name != "TEST" {
			t.Fatalf("Expected only %q but got %q", "TEST", name)
		}
	}
	if seen != 1 {
		t.Fatalf("Expected only 1 stream, got %d", seen)
	}
	if _, err := js.StreamInfo("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
	if err := js.DeleteConsumer("TEST", "C1"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.UpdateStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"bar"}, Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := js.DeleteStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now check the enabled account.
	// Check the enabled account only talks to its local JS domain by default.
	nc, js = jsClientConnect(t, ln, nats.UserInfo("y", "p"))
	defer nc.Close()

	sub, err := nc.SubscribeSync(JSAdvisoryStreamCreatedPre + ".>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	si, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster != nil {
		t.Fatalf("Expected no cluster designation for stream since created on single LN server")
	}

	// Wait for a bit and make sure we only get one of these.
	// The HUB domain should be cut off by default.
	time.Sleep(250 * time.Millisecond)
	checkSubsPending(t, sub, 1)
	// Drain.
	for _, err := sub.NextMsg(0); err == nil; _, err = sub.NextMsg(0) {
	}

	// Now try to talk to the HUB JS domain through a new context that uses a different mapped subject.
	// This is similar to how we let users cross JS domains between accounts as well.
	js, err = nc.JetStream(nats.APIPrefix("$JS.HUB.API"))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	// This should fail here with jetstream not enabled.
	if _, err := js.AccountInfo(); err != nats.ErrJetStreamNotEnabled {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now add in a mapping to the connected account in the HUB.
	// This aligns with the APIPrefix context above and works across leafnodes.
	// TODO(dlc) - Should we have a mapping section for leafnode solicit?
	c.addSubjectMapping("ONE", "$JS.HUB.API.>", "$JS.API.>")

	// Now it should work.
	if _, err := js.AccountInfo(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we can add a stream, etc.
	si, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST22",
		Subjects: []string{"bar"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil || si.Cluster.Name != "HUB" {
		t.Fatalf("Expected stream to be placed in %q", "HUB")
	}

	jsLocal, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}

	// Create a mirror on the local leafnode for stream TEST22.
	_, err = jsLocal.AddStream(&nats.StreamConfig{
		Name: "M",
		Mirror: &nats.StreamSource{
			Name:     "TEST22",
			External: &nats.ExternalStream{APIPrefix: "$JS.HUB.API"},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Publish a message to the HUB's TEST22 stream.
	if _, err := js.Publish("bar", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	// Make sure the message arrives in our mirror.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		si, err := jsLocal.StreamInfo("M")
		if err != nil {
			return fmt.Errorf("Could not get stream info: %v", err)
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg, got state: %+v", si.State)
		}
		return nil
	})

	// Now do the reverse and create a sourced stream in the HUB from our local stream on leafnode.
	// Inside the HUB we need to be able to find our local leafnode JetStream assets, so we need
	// a mapping in the LN server to allow this to work. Normally this will just be in server config.
	acc, err := ln.LookupAccount("JSY")
	if err != nil {
		c.t.Fatalf("Unexpected error on %v: %v", ln, err)
	}
	if err := acc.AddMapping("$JS.LN.API.>", "$JS.API.>"); err != nil {
		c.t.Fatalf("Error adding mapping: %v", err)
	}

	// js is the HUB JetStream context here.
	_, err = js.AddStream(&nats.StreamConfig{
		Name: "S",
		Sources: []*nats.StreamSource{{
			Name:     "M",
			External: &nats.ExternalStream{APIPrefix: "$JS.LN.API"},
		}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure the message arrives in our sourced stream.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("S")
		if err != nil {
			return fmt.Errorf("Could not get stream info: %v", err)
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg, got state: %+v", si.State)
		}
		return nil
	})
}

// JetStream Domains
func TestJetStreamClusterDomains(t *testing.T) {
	// This adds in domain config option to template.
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: CORE, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "CORE", _EMPTY_, 3, 12232, true)
	defer c.shutdown()

	// This leafnode is a single server with no domain but sharing the system account.
	// This extends the CORE domain through this leafnode.
	ln := c.createLeafNodeWithTemplate("LN-SYS",
		strings.ReplaceAll(jsClusterTemplWithSingleLeafNode, "store_dir:", "extension_hint: will_extend, domain: CORE, store_dir:"))
	defer ln.Shutdown()

	// This shows we have extended this system.
	c.waitOnPeerCount(4)
	if ml := c.leader(); ml == ln {
		t.Fatalf("Detected a meta-leader in the leafnode: %s", ml)
	}

	// Now create another LN but with a domain defined.
	tmpl = strings.Replace(jsClusterTemplWithSingleLeafNode, "store_dir:", "domain: SPOKE, store_dir:", 1)
	spoke := c.createLeafNodeWithTemplate("LN-SPOKE", tmpl)
	defer spoke.Shutdown()

	// Should be the same, should not extend the CORE domain.
	c.waitOnPeerCount(4)

	// The domain signals to the system that we are our own JetStream domain and should not extend CORE.
	// We want to check to make sure we have all the deny properly setup.
	spoke.mu.Lock()
	// var hasDE, hasDI bool
	for _, ln := range spoke.leafs {
		ln.mu.Lock()
		remote := ln.leaf.remote
		ln.mu.Unlock()
		remote.RLock()
		if remote.RemoteLeafOpts.LocalAccount == "$SYS" {
			for _, s := range denyAllJs {
				if r := ln.perms.pub.deny.Match(s); len(r.psubs) != 1 {
					t.Fatalf("Expected to have deny permission for %s", s)
				}
				if r := ln.perms.sub.deny.Match(s); len(r.psubs) != 1 {
					t.Fatalf("Expected to have deny permission for %s", s)
				}
			}
		}
		remote.RUnlock()
	}
	spoke.mu.Unlock()

	// Now do some operations.
	// Check the enabled account only talks to its local JS domain by default.
	nc, js := jsClientConnect(t, spoke)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster != nil {
		t.Fatalf("Expected no cluster designation for stream since created on single LN server")
	}

	// Now try to talk to the CORE JS domain through a new context that uses a different mapped subject.
	jsCore, err := nc.JetStream(nats.APIPrefix("$JS.CORE.API"))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	if _, err := jsCore.AccountInfo(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Make sure we can add a stream, etc.
	si, err = jsCore.AddStream(&nats.StreamConfig{
		Name:     "TEST22",
		Subjects: []string{"bar"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil || si.Cluster.Name != "CORE" {
		t.Fatalf("Expected stream to be placed in %q, got %q", "CORE", si.Cluster.Name)
	}

	jsLocal, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}

	// Create a mirror on our local leafnode for stream TEST22.
	_, err = jsLocal.AddStream(&nats.StreamConfig{
		Name: "M",
		Mirror: &nats.StreamSource{
			Name:     "TEST22",
			External: &nats.ExternalStream{APIPrefix: "$JS.CORE.API"},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Publish a message to the CORE's TEST22 stream.
	if _, err := jsCore.Publish("bar", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	// Make sure the message arrives in our mirror.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := jsLocal.StreamInfo("M")
		if err != nil {
			return fmt.Errorf("Could not get stream info: %v", err)
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg, got state: %+v", si.State)
		}
		return nil
	})

	// jsCore is the CORE JetStream domain.
	// Create a sourced stream in the CORE that is sourced from our mirror stream in our leafnode.
	_, err = jsCore.AddStream(&nats.StreamConfig{
		Name: "S",
		Sources: []*nats.StreamSource{{
			Name:     "M",
			External: &nats.ExternalStream{APIPrefix: "$JS.SPOKE.API"},
		}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure the message arrives in our sourced stream.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := jsCore.StreamInfo("S")
		if err != nil {
			return fmt.Errorf("Could not get stream info: %v", err)
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg, got state: %+v", si.State)
		}
		return nil
	})

	// Now connect directly to the CORE cluster and make sure we can operate there.
	nc, jsLocal = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create the js contexts again.
	jsSpoke, err := nc.JetStream(nats.APIPrefix("$JS.SPOKE.API"))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}

	// Publish a message to the CORE's TEST22 stream.
	if _, err := jsLocal.Publish("bar", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	// Make sure the message arrives in our mirror.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := jsSpoke.StreamInfo("M")
		if err != nil {
			return fmt.Errorf("Could not get stream info: %v", err)
		}
		if si.State.Msgs != 2 {
			return fmt.Errorf("Expected 2 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// Make sure the message arrives in our sourced stream.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := jsLocal.StreamInfo("S")
		if err != nil {
			return fmt.Errorf("Could not get stream info: %v", err)
		}
		if si.State.Msgs != 2 {
			return fmt.Errorf("Expected 2 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// We are connected to the CORE domain/system. Create a JetStream context referencing ourselves.
	jsCore, err = nc.JetStream(nats.APIPrefix("$JS.CORE.API"))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}

	si, err = jsCore.StreamInfo("S")
	if err != nil {
		t.Fatalf("Could not get stream info: %v", err)
	}
	if si.State.Msgs != 2 {
		t.Fatalf("Expected 2 msgs, got state: %+v", si.State)
	}
}

func TestJetStreamClusterDomainsWithNoJSHub(t *testing.T) {
	// Create our hub cluster with no JetStream defined.
	c := createMixedModeCluster(t, jsClusterAccountsTempl, "NOJS5", _EMPTY_, 0, 5, false)
	defer c.shutdown()

	ln := c.createSingleLeafNodeNoSystemAccountAndEnablesJetStream()
	defer ln.Shutdown()

	lnd := c.createSingleLeafNodeNoSystemAccountAndEnablesJetStreamWithDomain("SPOKE", "nojs")
	defer lnd.Shutdown()

	// Client based API - Connected to the core cluster with no JS but account has JS.
	s := c.randomServer()
	// Make sure the JS interest from the LNs has made it to this server.
	checkSubInterest(t, s, "NOJS", "$JS.SPOKE.API.INFO", time.Second)
	nc, _ := jsClientConnect(t, s, nats.UserInfo("nojs", "p"))
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	}
	req, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Do by hand to make sure we only get one response.
	sis := fmt.Sprintf(strings.ReplaceAll(JSApiStreamCreateT, JSApiPrefix, "$JS.SPOKE.API"), "TEST")
	rs := nats.NewInbox()
	sub, _ := nc.SubscribeSync(rs)
	nc.PublishRequest(sis, rs, req)
	// Wait for response.
	checkSubsPending(t, sub, 1)
	// Double check to make sure we only have 1.
	if nr, _, err := sub.Pending(); err != nil || nr != 1 {
		t.Fatalf("Expected 1 response, got %d and %v", nr, err)
	}
	resp, err := sub.NextMsg(time.Second)
	require_NoError(t, err)

	// This StreamInfo should *not* have a domain set.
	// Do by hand until this makes it to the Go client.
	var si StreamInfo
	if err = json.Unmarshal(resp.Data, &si); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Domain != _EMPTY_ {
		t.Fatalf("Expected to have NO domain set but got %q", si.Domain)
	}

	// Now let's create a stream specifically on the SPOKE domain.
	js, err := nc.JetStream(nats.Domain("SPOKE"))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST22",
		Subjects: []string{"bar"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now lookup by hand to check domain.
	resp, err = nc.Request("$JS.SPOKE.API.STREAM.INFO.TEST22", nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err = json.Unmarshal(resp.Data, &si); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Domain != "SPOKE" {
		t.Fatalf("Expected to have domain set to %q but got %q", "SPOKE", si.Domain)
	}
}

// Issue #2205
func TestJetStreamClusterDomainsAndAPIResponses(t *testing.T) {
	// This adds in domain config option to template.
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: CORE, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "CORE", _EMPTY_, 3, 12232, true)
	defer c.shutdown()

	// Now create spoke LN cluster.
	tmpl = strings.Replace(jsClusterTemplWithLeafNode, "store_dir:", "domain: SPOKE, store_dir:", 1)
	lnc := c.createLeafNodesWithTemplateAndStartPort(tmpl, "SPOKE", 5, 23913)
	defer lnc.shutdown()

	lnc.waitOnClusterReady()

	// Make the physical connection to the CORE.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now create JS domain context and try to do same in LN cluster.
	// The issue referenced above details a bug where we can not receive a positive response.
	nc, _ = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	jsSpoke, err := nc.JetStream(nats.APIPrefix("$JS.SPOKE.API"))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}

	si, err := jsSpoke.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster.Name != "SPOKE" {
		t.Fatalf("Expected %q as the cluster, got %q", "SPOKE", si.Cluster.Name)
	}
}

// Issue #2202
func TestJetStreamClusterDomainsAndSameNameSources(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: CORE, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "CORE", _EMPTY_, 3, 9323, true)
	defer c.shutdown()

	tmpl = strings.Replace(jsClusterTemplWithSingleLeafNode, "store_dir:", "domain: SPOKE-1, store_dir:", 1)
	spoke1 := c.createLeafNodeWithTemplate("LN-SPOKE-1", tmpl)
	defer spoke1.Shutdown()

	tmpl = strings.Replace(jsClusterTemplWithSingleLeafNode, "store_dir:", "domain: SPOKE-2, store_dir:", 1)
	spoke2 := c.createLeafNodeWithTemplate("LN-SPOKE-2", tmpl)
	defer spoke2.Shutdown()

	checkLeafNodeConnectedCount(t, spoke1, 2)
	checkLeafNodeConnectedCount(t, spoke2, 2)

	subjFor := func(s *Server) string {
		switch s {
		case spoke1:
			return "foo"
		case spoke2:
			return "bar"
		}
		return "TEST"
	}

	// Create the same name stream in both spoke domains.
	for _, s := range []*Server{spoke1, spoke2} {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{subjFor(s)},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		nc.Close()
	}

	// Now connect to the hub and create a sourced stream from both leafnode streams.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name: "S",
		Sources: []*nats.StreamSource{
			{
				Name:     "TEST",
				External: &nats.ExternalStream{APIPrefix: "$JS.SPOKE-1.API"},
			},
			{
				Name:     "TEST",
				External: &nats.ExternalStream{APIPrefix: "$JS.SPOKE-2.API"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Publish a message to each spoke stream and we will check that our sourced stream gets both.
	for _, s := range []*Server{spoke1, spoke2} {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		js.Publish(subjFor(s), []byte("DOUBLE TROUBLE"))
		si, err := js.StreamInfo("TEST")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 1 {
			t.Fatalf("Expected 1 msg, got %d", si.State.Msgs)
		}
		nc.Close()
	}

	// Now make sure we have 2 msgs in our sourced stream.
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		si, err := js.StreamInfo("S")
		require_NoError(t, err)
		if si.State.Msgs != 2 {
			return fmt.Errorf("Expected 2 msgs, got %d", si.State.Msgs)
		}
		return nil
	})

	// Make sure we can see our external information.
	// This not in the Go client yet so manual for now.
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "S"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var ssi StreamInfo
	if err = json.Unmarshal(resp.Data, &ssi); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(ssi.Sources) != 2 {
		t.Fatalf("Expected 2 source streams, got %d", len(ssi.Sources))
	}
	if ssi.Sources[0].External == nil {
		t.Fatalf("Expected a non-nil external designation")
	}
	pre := ssi.Sources[0].External.ApiPrefix
	if pre != "$JS.SPOKE-1.API" && pre != "$JS.SPOKE-2.API" {
		t.Fatalf("Expected external api of %q, got %q", "$JS.SPOKE-[1|2].API", ssi.Sources[0].External.ApiPrefix)
	}

	// Also create a mirror.
	_, err = js.AddStream(&nats.StreamConfig{
		Name: "M",
		Mirror: &nats.StreamSource{
			Name:     "TEST",
			External: &nats.ExternalStream{APIPrefix: "$JS.SPOKE-1.API"},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err = nc.Request(fmt.Sprintf(JSApiStreamInfoT, "M"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err = json.Unmarshal(resp.Data, &ssi); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ssi.Mirror == nil || ssi.Mirror.External == nil {
		t.Fatalf("Expected a non-nil external designation for our mirror")
	}
	if ssi.Mirror.External.ApiPrefix != "$JS.SPOKE-1.API" {
		t.Fatalf("Expected external api of %q, got %q", "$JS.SPOKE-1.API", ssi.Sources[0].External.ApiPrefix)
	}
}

// When a leafnode enables JS on an account that is not enabled on the remote cluster account this should fail
// Accessing a jet stream in a different availability domain requires the client provide a damain name, or
// the server having set up appropriate defaults (default_js_domain. tested in leafnode_test.go)
func TestJetStreamClusterSingleLeafNodeEnablingJetStream(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: HUB, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "HUB", _EMPTY_, 3, 11322, true)
	defer c.shutdown()

	ln := c.createSingleLeafNodeNoSystemAccountAndEnablesJetStream()
	defer ln.Shutdown()

	// Check that we have JS in the $G account on the leafnode.
	nc, js := jsClientConnect(t, ln)
	defer nc.Close()

	if _, err := js.AccountInfo(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Connect our client to the "nojs" account in the cluster but make sure JS works since its enabled via the leafnode.
	s := c.randomServer()
	nc, js = jsClientConnect(t, s, nats.UserInfo("nojs", "p"))
	defer nc.Close()
	_, err := js.AccountInfo()
	// error is context deadline exceeded as the local account has no js and can't reach the remote one
	require_True(t, err == context.DeadlineExceeded)
}

func TestJetStreamClusterLeafNodesWithoutJS(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: HUB, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "HUB", _EMPTY_, 3, 11233, true)
	defer c.shutdown()

	testJS := func(s *Server, domain string, doDomainAPI bool) {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		if doDomainAPI {
			var err error
			apiPre := fmt.Sprintf("$JS.%s.API", domain)
			if js, err = nc.JetStream(nats.APIPrefix(apiPre)); err != nil {
				t.Fatalf("Unexpected error getting JetStream context: %v", err)
			}
		}
		ai, err := js.AccountInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ai.Domain != domain {
			t.Fatalf("Expected domain of %q, got %q", domain, ai.Domain)
		}
	}

	ln := c.createLeafNodeWithTemplate("LN-SYS-S-NOJS", jsClusterTemplWithSingleLeafNodeNoJS)
	defer ln.Shutdown()

	// Check that we can access JS in the $G account on the cluster through the leafnode.
	testJS(ln, "HUB", true)
	ln.Shutdown()

	// Now create a leafnode cluster with No JS and make sure that works.
	lnc := c.createLeafNodesNoJS("LN-SYS-C-NOJS", 3)
	defer lnc.shutdown()

	testJS(lnc.randomServer(), "HUB", true)
	lnc.shutdown()

	// Do mixed mode but with a JS config block that specifies domain and just sets it to disabled.
	// This is the preferred method for mixed mode, always define JS server config block just disable
	// in those you do not want it running.
	// e.g. jetstream: {domain: "SPOKE", enabled: false}
	tmpl = strings.Replace(jsClusterTemplWithLeafNode, "store_dir:", "domain: SPOKE, store_dir:", 1)
	lncm := c.createLeafNodesWithTemplateMixedMode(tmpl, "SPOKE", 3, 2, true)
	defer lncm.shutdown()

	// Now grab a non-JS server, last two are non-JS.
	sl := lncm.servers[0]
	testJS(sl, "SPOKE", false)

	// Test that mappings work as well and we can access the hub.
	testJS(sl, "HUB", true)
}

func TestJetStreamClusterLeafNodesWithSameDomainNames(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: HUB, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "HUB", _EMPTY_, 3, 11233, true)
	defer c.shutdown()

	tmpl = strings.Replace(jsClusterTemplWithLeafNode, "store_dir:", "domain: HUB, store_dir:", 1)
	lnc := c.createLeafNodesWithTemplateAndStartPort(tmpl, "SPOKE", 3, 11311)
	defer lnc.shutdown()

	c.waitOnPeerCount(6)
}

// Issue reported with superclusters and leafnodes where first few get next requests for pull subscribers
// have the wrong subject.
func TestJetStreamClusterSuperClusterGetNextRewrite(t *testing.T) {
	sc := createJetStreamSuperClusterWithTemplate(t, jsClusterAccountsTempl, 2, 2)
	defer sc.shutdown()

	// Will connect the leafnode to cluster C1. We will then connect the "client" to cluster C2 to cross gateways.
	ln := sc.clusterForName("C1").createSingleLeafNodeNoSystemAccountAndEnablesJetStreamWithDomain("C", "nojs")
	defer ln.Shutdown()

	c2 := sc.clusterForName("C2")
	nc, js := jsClientConnectEx(t, c2.randomServer(), "C", nats.UserInfo("nojs", "p"))
	defer nc.Close()

	// Create a stream and add messages.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "foo"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := js.Publish("foo", []byte("ok")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Pull messages and make sure subject rewrite works.
	sub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, m := range fetchMsgs(t, sub, 5, time.Second) {
		if m.Subject != "foo" {
			t.Fatalf("Expected %q as subject but got %q", "foo", m.Subject)
		}
	}
}

func TestJetStreamClusterSuperClusterGetNextSubRace(t *testing.T) {
	sc := createJetStreamSuperClusterWithTemplate(t, jsClusterAccountsTempl, 2, 2)
	defer sc.shutdown()

	// Will connect the leafnode to cluster C1. We will then connect the "client" to cluster C2 to cross gateways.
	ln := sc.clusterForName("C1").createSingleLeafNodeNoSystemAccountAndEnablesJetStreamWithDomain("C", "nojs")
	defer ln.Shutdown()

	// Shutdown 1 of the server from C1, (the one LN is not connected to)
	for _, s := range sc.clusterForName("C1").servers {
		s.mu.Lock()
		if len(s.leafs) == 0 {
			s.mu.Unlock()
			s.Shutdown()
			break
		}
		s.mu.Unlock()
	}

	// Wait on meta leader in case shutdown of server above caused an election.
	sc.waitOnLeader()

	var c2Srv *Server
	// Take the server from C2 that has no inbound from C1.
	c2 := sc.clusterForName("C2")
	for _, s := range c2.servers {
		var gwsa [2]*client
		gws := gwsa[:0]
		s.getInboundGatewayConnections(&gws)
		if len(gws) == 0 {
			c2Srv = s
			break
		}
	}
	if c2Srv == nil {
		t.Fatalf("Both servers in C2 had an inbound GW connection!")
	}

	nc, js := jsClientConnectEx(t, c2Srv, "C", nats.UserInfo("nojs", "p"))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
	require_NoError(t, err)

	_, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dur", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		sendStreamMsg(t, nc, "foo", "ok")
	}

	// Wait for all messages to appear in the consumer
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("foo", "dur")
		if err != nil {
			return err
		}
		if n := ci.NumPending; n != 100 {
			return fmt.Errorf("Expected 100 msgs, got %v", n)
		}
		return nil
	})

	req := &JSApiConsumerGetNextRequest{Batch: 1, Expires: 5 * time.Second}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)
	// Create this by hand here to make sure we create the subscription
	// on the reply subject for every single request
	nextSubj := fmt.Sprintf(JSApiRequestNextT, "foo", "dur")
	nextSubj = "$JS.C.API" + strings.TrimPrefix(nextSubj, "$JS.API")
	for i := 0; i < 100; i++ {
		inbox := nats.NewInbox()
		sub := natsSubSync(t, nc, inbox)
		natsPubReq(t, nc, nextSubj, inbox, jreq)
		msg := natsNexMsg(t, sub, time.Second)
		if len(msg.Header) != 0 && string(msg.Data) != "ok" {
			t.Fatalf("Unexpected message: header=%+v data=%s", msg.Header, msg.Data)
		}
		sub.Unsubscribe()
	}
}

func TestJetStreamClusterSuperClusterPullConsumerAndHeaders(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	c1 := sc.clusterForName("C1")
	c2 := sc.clusterForName("C2")

	nc, js := jsClientConnect(t, c1.randomServer())
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "ORIGIN"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	toSend := 50
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("ORIGIN", []byte("ok")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	nc2, js2 := jsClientConnect(t, c2.randomServer())
	defer nc2.Close()

	_, err := js2.AddStream(&nats.StreamConfig{
		Name:    "S",
		Sources: []*nats.StreamSource{{Name: "ORIGIN"}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Wait for them to be in the sourced stream.
	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		if si, _ := js2.StreamInfo("S"); si.State.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected %d msgs for %q, got %d", toSend, "S", si.State.Msgs)
		}
		return nil
	})

	// Now create a pull consumer for the sourced stream.
	_, err = js2.AddConsumer("S", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now we will connect and request the next message from each server in C1 cluster and check that headers remain in place.
	for _, s := range c1.servers {
		nc, err := nats.Connect(s.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()
		m, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.S.dlc", nil, 2*time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(m.Header) != 1 {
			t.Fatalf("Expected 1 header element, got %+v", m.Header)
		}
	}
}

func TestJetStreamClusterLeafDifferentAccounts(t *testing.T) {
	c := createJetStreamCluster(t, jsClusterAccountsTempl, "HUB", _EMPTY_, 2, 23133, false)
	defer c.shutdown()

	ln := c.createLeafNodesWithStartPortAndDomain("LN", 2, 22110, _EMPTY_)
	defer ln.shutdown()

	// Wait on all peers.
	c.waitOnPeerCount(4)

	nc, js := jsClientConnect(t, ln.randomServer())
	defer nc.Close()

	// Make sure we can properly indentify the right account when the leader received the request.
	// We need to map the client info header to the new account once received by the hub.
	if _, err := js.AccountInfo(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterStreamInfoDeletedDetails(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R2", 2)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send in 10 messages.
	msg, toSend := []byte("HELLO"), 10

	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Now remove some messages.
	deleteMsg := func(seq uint64) {
		if err := js.DeleteMsg("TEST", seq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	deleteMsg(2)
	deleteMsg(4)
	deleteMsg(6)

	// Need to do these via direct server request for now.
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var si StreamInfo
	if err = json.Unmarshal(resp.Data, &si); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.NumDeleted != 3 {
		t.Fatalf("Expected %d deleted, got %d", 3, si.State.NumDeleted)
	}
	if len(si.State.Deleted) != 0 {
		t.Fatalf("Expected not deleted details, but got %+v", si.State.Deleted)
	}

	// Now request deleted details.
	req := JSApiStreamInfoRequest{DeletedDetails: true}
	b, _ := json.Marshal(req)

	resp, err = nc.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), b, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err = json.Unmarshal(resp.Data, &si); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if si.State.NumDeleted != 3 {
		t.Fatalf("Expected %d deleted, got %d", 3, si.State.NumDeleted)
	}
	if len(si.State.Deleted) != 3 {
		t.Fatalf("Expected deleted details, but got %+v", si.State.Deleted)
	}
}

func TestJetStreamClusterMirrorAndSourceExpiration(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "MSE", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Origin
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	bi := 1
	sendBatch := func(n int) {
		t.Helper()
		// Send a batch to a given subject.
		for i := 0; i < n; i++ {
			msg := fmt.Sprintf("ID: %d", bi)
			bi++
			if _, err := js.PublishAsync("TEST", []byte(msg)); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	checkStream := func(stream string, num uint64) {
		t.Helper()
		checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
			si, err := js.StreamInfo(stream)
			if err != nil {
				return err
			}
			if si.State.Msgs != num {
				return fmt.Errorf("Expected %d msgs, got %d", num, si.State.Msgs)
			}
			return nil
		})
	}

	checkSource := func(num uint64) { t.Helper(); checkStream("S", num) }
	checkMirror := func(num uint64) { t.Helper(); checkStream("M", num) }
	checkTest := func(num uint64) { t.Helper(); checkStream("TEST", num) }

	var err error

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Mirror:   &nats.StreamSource{Name: "TEST"},
		Replicas: 2,
		MaxAge:   500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// We want this to not be same as TEST leader for this test.
	sl := c.streamLeader("$G", "TEST")
	for ss := sl; ss == sl; {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "S",
			Sources:  []*nats.StreamSource{{Name: "TEST"}},
			Replicas: 2,
			MaxAge:   500 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ss = c.streamLeader("$G", "S"); ss == sl {
			// Delete and retry.
			js.DeleteStream("S")
		}
	}

	sendBatch(100)
	checkTest(100)
	checkMirror(100)
	checkSource(100)

	// Make sure they expire.
	checkMirror(0)
	checkSource(0)

	// Now stop the server housing the leader of the source stream.
	sl.Shutdown()
	c.restartServer(sl)
	checkClusterFormed(t, c.servers...)
	c.waitOnStreamLeader("$G", "S")
	c.waitOnStreamLeader("$G", "M")

	// Make sure can process correctly after we have expired all of the messages.
	sendBatch(100)
	// Need to check both in parallel.
	scheck, mcheck := uint64(0), uint64(0)
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if scheck != 100 {
			if si, _ := js.StreamInfo("S"); si != nil {
				scheck = si.State.Msgs
			}
		}
		if mcheck != 100 {
			if si, _ := js.StreamInfo("M"); si != nil {
				mcheck = si.State.Msgs
			}
		}
		if scheck == 100 && mcheck == 100 {
			return nil
		}
		return fmt.Errorf("Both not at 100 yet, S=%d, M=%d", scheck, mcheck)
	})

	checkTest(200)
}

func TestJetStreamClusterMirrorAndSourceSubLeaks(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "MSL", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	startSubs := c.stableTotalSubs()

	var ss []*nats.StreamSource

	// Create 10 origin streams
	for i := 0; i < 10; i++ {
		sn := fmt.Sprintf("ORDERS-%d", i+1)
		ss = append(ss, &nats.StreamSource{Name: sn})
		if _, err := js.AddStream(&nats.StreamConfig{Name: sn}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Create mux'd stream that sources all of the origin streams.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "MUX",
		Replicas: 2,
		Sources:  ss,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now create a mirror of the mux stream.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "MIRROR",
		Replicas: 2,
		Mirror:   &nats.StreamSource{Name: "MUX"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Get stable subs count.
	afterSubs := c.stableTotalSubs()

	js.DeleteStream("MIRROR")
	js.DeleteStream("MUX")

	for _, si := range ss {
		js.DeleteStream(si.Name)
	}

	// Some subs take longer to settle out so we give ourselves a small buffer.
	// There will be 1 sub for client on each server (such as _INBOX.IvVJ2DOXUotn4RUSZZCFvp.*)
	// and 2 or 3 subs such as `_R_.xxxxx.>` on each server, so a total of 12 subs.
	if deleteSubs := c.stableTotalSubs(); deleteSubs > startSubs+12 {
		t.Fatalf("Expected subs to return to %d from a high of %d, but got %d", startSubs, afterSubs, deleteSubs)
	}
}

func TestJetStreamClusterCreateConcurrentDurableConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "MSL", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create origin stream, must be R > 1
	if _, err := js.AddStream(&nats.StreamConfig{Name: "ORDERS", Replicas: 3}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.QueueSubscribeSync("ORDERS", "wq", nats.Durable("shared")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now try to create durables concurrently.
	start := make(chan struct{})
	var wg sync.WaitGroup
	created := uint32(0)
	errs := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start
			_, err := js.QueueSubscribeSync("ORDERS", "wq", nats.Durable("shared"))
			if err == nil {
				atomic.AddUint32(&created, 1)
			} else if !strings.Contains(err.Error(), "consumer name already") {
				errs <- err
			}
		}()
	}

	close(start)
	wg.Wait()

	if lc := atomic.LoadUint32(&created); lc != 10 {
		t.Fatalf("Expected all 10 to be created, got %d", lc)
	}
	if len(errs) > 0 {
		t.Fatalf("Failed to create some sub: %v", <-errs)
	}
}

// https://github.com/nats-io/nats-server/issues/2144
func TestJetStreamClusterUpdateStreamToExisting(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "MSL", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "ORDERS1",
		Replicas: 3,
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "ORDERS2",
		Replicas: 3,
		Subjects: []string{"bar"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "ORDERS2",
		Replicas: 3,
		Subjects: []string{"foo"},
	})
	if err == nil {
		t.Fatalf("Expected an error but got none")
	}
}

func TestJetStreamClusterCrossAccountInterop(t *testing.T) {
	template := `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, domain: HUB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {
		JS {
			jetstream: enabled
			users = [ { user: "rip", pass: "pass" } ]
			exports [
				{ service: "$JS.API.CONSUMER.INFO.>" }
				{ service: "$JS.HUB.API.CONSUMER.>", response: stream }
				{ stream: "M.SYNC.>" } # For the mirror
			]
		}
		IA {
			jetstream: enabled
			users = [ { user: "dlc", pass: "pass" } ]
			imports [
				{ service: { account: JS, subject: "$JS.API.CONSUMER.INFO.TEST.DLC"}, to: "FROM.DLC" }
				{ service: { account: JS, subject: "$JS.HUB.API.CONSUMER.>"}, to: "js.xacc.API.CONSUMER.>" }
				{ stream: { account: JS, subject: "M.SYNC.>"} }
			]
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
	`

	c := createJetStreamClusterWithTemplate(t, template, "HUB", 3)
	defer c.shutdown()

	// Create the stream and the consumer under the JS/rip user.
	s := c.randomServer()
	nc, js := jsClientConnect(t, s, nats.UserInfo("rip", "pass"))
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err := js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "DLC", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Also create a stream via the domain qualified API.
	js, err = nc.JetStream(nats.APIPrefix("$JS.HUB.API"))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "ORDERS", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now we want to access the consumer info from IA/dlc.
	nc2, js2 := jsClientConnect(t, c.randomServer(), nats.UserInfo("dlc", "pass"))
	defer nc2.Close()

	if _, err := nc2.Request("FROM.DLC", nil, time.Second); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure domain mappings etc work across accounts.
	// Setup a mirror.
	_, err = js2.AddStream(&nats.StreamConfig{
		Name: "MIRROR",
		Mirror: &nats.StreamSource{
			Name: "ORDERS",
			External: &nats.ExternalStream{
				APIPrefix:     "js.xacc.API",
				DeliverPrefix: "M.SYNC",
			},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send 10 messages..
	msg, toSend := []byte("Hello mapped domains"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("ORDERS", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("MIRROR")
		if err != nil {
			return fmt.Errorf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 10 {
			return fmt.Errorf("Expected 10 msgs, got state: %+v", si.State)
		}
		return nil
	})
}

// https://github.com/nats-io/nats-server/issues/2242
func TestJetStreamClusterMsgIdDuplicateBug(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "MSL", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sendMsgID := func(id string) (*nats.PubAck, error) {
		t.Helper()
		m := nats.NewMsg("foo")
		m.Header.Add(JSMsgId, id)
		m.Data = []byte("HELLO WORLD")
		return js.PublishMsg(m)
	}

	if _, err := sendMsgID("1"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// This should fail with duplicate detected.
	if pa, _ := sendMsgID("1"); pa == nil || !pa.Duplicate {
		t.Fatalf("Expected duplicate but got none: %+v", pa)
	}
	// This should be fine.
	if _, err := sendMsgID("2"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterNilMsgWithHeaderThroughSourcedStream(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: HUB, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "HUB", _EMPTY_, 3, 12232, true)
	defer c.shutdown()

	tmpl = strings.Replace(jsClusterTemplWithSingleLeafNode, "store_dir:", "domain: SPOKE, store_dir:", 1)
	spoke := c.createLeafNodeWithTemplate("SPOKE", tmpl)
	defer spoke.Shutdown()

	checkLeafNodeConnectedCount(t, spoke, 2)

	// Client for API requests.
	nc, js := jsClientConnect(t, spoke)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	jsHub, err := nc.JetStream(nats.APIPrefix("$JS.HUB.API"))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}

	_, err = jsHub.AddStream(&nats.StreamConfig{
		Name:     "S",
		Replicas: 2,
		Sources: []*nats.StreamSource{{
			Name:     "TEST",
			External: &nats.ExternalStream{APIPrefix: "$JS.SPOKE.API"},
		}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now send a message to the origin stream with nil body and a header.
	m := nats.NewMsg("foo")
	m.Header.Add("X-Request-ID", "e9a639b4-cecb-4fbe-8376-1ef511ae1f8d")
	m.Data = []byte("HELLO WORLD")

	if _, err = jsHub.PublishMsg(m); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := jsHub.SubscribeSync("foo", nats.BindStream("S"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if string(msg.Data) != "HELLO WORLD" {
		t.Fatalf("Message corrupt? Expecting %q got %q", "HELLO WORLD", msg.Data)
	}
}

// Make sure varz reports the server usage not replicated usage etc.
func TestJetStreamClusterVarzReporting(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// ~100k per message.
	msg := []byte(strings.Repeat("A", 99_960))
	msz := fileStoreMsgSize("TEST", nil, msg)
	total := msz * 10

	for i := 0; i < 10; i++ {
		if _, err := js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	// To show the bug we need this to allow remote usage to replicate.
	time.Sleep(2 * usageTick)

	v, err := s.Varz(nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if v.JetStream.Stats.Store > total {
		t.Fatalf("Single server varz JetStream store usage should be <= %d, got %d", total, v.JetStream.Stats.Store)
	}

	info, err := js.AccountInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Store < total*3 {
		t.Fatalf("Expected account information to show usage ~%d, got %d", total*3, info.Store)
	}
}

func TestJetStreamClusterStatszActiveServers(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 2, 2)
	defer sc.shutdown()

	checkActive := func(expected int) {
		t.Helper()
		checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
			s := sc.randomServer()
			nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
			if err != nil {
				t.Fatalf("Failed to create system client: %v", err)
			}
			defer nc.Close()

			resp, err := nc.Request(serverStatsPingReqSubj, nil, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var ssm ServerStatsMsg
			if err := json.Unmarshal(resp.Data, &ssm); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ssm.Stats.ActiveServers != expected {
				return fmt.Errorf("Wanted %d, got %d", expected, ssm.Stats.ActiveServers)
			}
			return nil
		})
	}

	checkActive(4)
	c := sc.randomCluster()
	ss := c.randomServer()
	ss.Shutdown()
	checkActive(3)
	c.restartServer(ss)
	checkActive(4)
}

func TestJetStreamClusterSourceAndMirrorConsumersLeaderChange(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	c1 := sc.clusterForName("C1")
	c2 := sc.clusterForName("C2")

	nc, js := jsClientConnect(t, c1.randomServer())
	defer nc.Close()

	var sources []*nats.StreamSource
	numStreams := 10

	for i := 1; i <= numStreams; i++ {
		name := fmt.Sprintf("O%d", i)
		sources = append(sources, &nats.StreamSource{Name: name})
		if _, err := js.AddStream(&nats.StreamConfig{Name: name}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Place our new stream that will source all the others in different cluster.
	nc, js = jsClientConnect(t, c2.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "S",
		Replicas: 2,
		Sources:  sources,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Force leader change twice.
	nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "S"), nil, time.Second)
	c2.waitOnStreamLeader("$G", "S")
	nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "S"), nil, time.Second)
	c2.waitOnStreamLeader("$G", "S")

	// Now make sure we only have a single direct consumer on our origin streams.
	// Pick one at random.
	name := fmt.Sprintf("O%d", rand.Intn(numStreams-1)+1)
	c1.waitOnStreamLeader("$G", name)
	s := c1.streamLeader("$G", name)
	a, err := s.lookupAccount("$G")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	mset, err := a.lookupStream(name)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		if ndc := mset.numDirectConsumers(); ndc != 1 {
			return fmt.Errorf("Stream %q wanted 1 direct consumer, got %d", name, ndc)
		}
		return nil
	})

	// Now create a mirror of selected from above. Will test same scenario.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Replicas: 2,
		Mirror:   &nats.StreamSource{Name: name},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Force leader change twice.
	nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "M"), nil, time.Second)
	c2.waitOnStreamLeader("$G", "M")
	nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "M"), nil, time.Second)
	c2.waitOnStreamLeader("$G", "M")

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		if ndc := mset.numDirectConsumers(); ndc != 2 {
			return fmt.Errorf("Stream %q wanted 2 direct consumers, got %d", name, ndc)
		}
		return nil
	})
}

func TestJetStreamClusterPurgeBySequence(t *testing.T) {
	for _, st := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(st.String(), func(t *testing.T) {

			c := createJetStreamClusterExplicit(t, "JSC", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			cfg := StreamConfig{
				Name:       "KV",
				Subjects:   []string{"kv.*.*"},
				Storage:    st,
				Replicas:   2,
				MaxMsgsPer: 5,
			}
			req, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			for i := 0; i < 20; i++ {
				if _, err = js.Publish("kv.myapp.username", []byte(fmt.Sprintf("value %d", i))); err != nil {
					t.Fatalf("request failed: %s", err)
				}
			}
			for i := 0; i < 20; i++ {
				if _, err = js.Publish("kv.myapp.password", []byte(fmt.Sprintf("value %d", i))); err != nil {
					t.Fatalf("request failed: %s", err)
				}
			}
			expectSequences := func(t *testing.T, subject string, seq ...int) {
				sub, err := js.SubscribeSync(subject)
				if err != nil {
					t.Fatalf("sub failed: %s", err)
				}
				defer sub.Unsubscribe()
				for _, i := range seq {
					msg, err := sub.NextMsg(time.Second)
					if err != nil {
						t.Fatalf("didn't get message: %s", err)
					}
					meta, err := msg.Metadata()
					if err != nil {
						t.Fatalf("didn't get metadata: %s", err)
					}
					if meta.Sequence.Stream != uint64(i) {
						t.Fatalf("expected sequence %d got %d", i, meta.Sequence.Stream)
					}
				}
			}
			expectSequences(t, "kv.myapp.username", 16, 17, 18, 19, 20)
			expectSequences(t, "kv.myapp.password", 36, 37, 38, 39, 40)

			// delete up to but not including 18 of username...
			jr, _ := json.Marshal(&JSApiStreamPurgeRequest{Subject: "kv.myapp.username", Sequence: 18})
			_, err = nc.Request(fmt.Sprintf(JSApiStreamPurgeT, "KV"), jr, time.Second)
			if err != nil {
				t.Fatalf("request failed: %s", err)
			}
			// 18 should still be there
			expectSequences(t, "kv.myapp.username", 18, 19, 20)
		})
	}
}

func TestJetStreamClusterMaxConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:         "MAXC",
		Storage:      nats.MemoryStorage,
		Subjects:     []string{"in.maxc.>"},
		MaxConsumers: 1,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	si, err := js.StreamInfo("MAXC")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Config.MaxConsumers != 1 {
		t.Fatalf("Expected max of 1, got %d", si.Config.MaxConsumers)
	}
	// Make sure we get the right error.
	// This should succeed.
	if _, err := js.SubscribeSync("in.maxc.foo"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("in.maxc.bar"); err == nil {
		t.Fatalf("Eexpected error but got none")
	}
}

func TestJetStreamClusterMaxConsumersMultipleConcurrentRequests(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:         "MAXCC",
		Storage:      nats.MemoryStorage,
		Subjects:     []string{"in.maxcc.>"},
		MaxConsumers: 1,
		Replicas:     3,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	si, err := js.StreamInfo("MAXCC")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Config.MaxConsumers != 1 {
		t.Fatalf("Expected max of 1, got %d", si.Config.MaxConsumers)
	}

	startCh := make(chan bool)
	var wg sync.WaitGroup

	for n := 0; n < 10; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()
			<-startCh
			js.SubscribeSync("in.maxcc.foo")
		}()
	}
	// Wait for Go routines.
	time.Sleep(250 * time.Millisecond)

	close(startCh)
	wg.Wait()

	var names []string
	for n := range js.ConsumerNames("MAXCC") {
		names = append(names, n)
	}
	if nc := len(names); nc > 1 {
		t.Fatalf("Expected only 1 consumer, got %d", nc)
	}
}

func TestJetStreamClusterPanicDecodingConsumerState(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	rch := make(chan struct{}, 2)
	nc, js := jsClientConnect(t, c.randomServer(),
		nats.ReconnectWait(50*time.Millisecond),
		nats.MaxReconnects(-1),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			rch <- struct{}{}
		}),
	)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"ORDERS.*"},
		Storage:   nats.FileStorage,
		Replicas:  3,
		Retention: nats.WorkQueuePolicy,
		Discard:   nats.DiscardNew,
		MaxMsgs:   -1,
		MaxAge:    time.Hour * 24 * 365,
	}); err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}

	sub, err := js.PullSubscribe("ORDERS.created", "durable", nats.MaxAckPending(1000))

	if err != nil {
		t.Fatalf("Error creating pull subscriber: %v", err)
	}

	sendMsg := func(subject string) {
		t.Helper()
		if _, err := js.Publish(subject, []byte("msg")); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		sendMsg("ORDERS.something")
		sendMsg("ORDERS.created")
	}

	for total := 0; total != 100; {
		msgs, err := sub.Fetch(100-total, nats.MaxWait(2*time.Second))
		if err != nil {
			t.Fatalf("Failed to fetch message: %v", err)
		}
		for _, m := range msgs {
			m.AckSync()
			total++
		}
	}

	c.stopAll()
	c.restartAllSamePorts()
	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnConsumerLeader("$G", "TEST", "durable")

	select {
	case <-rch:
	case <-time.After(2 * time.Second):
		t.Fatal("Did not reconnect")
	}

	for i := 0; i < 100; i++ {
		sendMsg("ORDERS.something")
		sendMsg("ORDERS.created")
	}

	for total := 0; total != 100; {
		msgs, err := sub.Fetch(100-total, nats.MaxWait(2*time.Second))
		if err != nil {
			t.Fatalf("Error on fetch: %v", err)
		}
		for _, m := range msgs {
			m.AckSync()
			total++
		}
	}
}

// Had a report of leaked subs with pull subscribers.
func TestJetStreamClusterPullConsumerLeakedSubs(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"Domains.*"},
		Replicas:  1,
		Retention: nats.InterestPolicy,
	}); err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}

	sub, err := js.PullSubscribe("Domains.Domain", "Domains-Api", nats.MaxAckPending(20_000))
	if err != nil {
		t.Fatalf("Error creating pull subscriber: %v", err)
	}
	defer sub.Unsubscribe()

	// Load up a bunch of requests.
	numRequests := 20
	for i := 0; i < numRequests; i++ {
		js.PublishAsync("Domains.Domain", []byte("QUESTION"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
	numSubs := c.stableTotalSubs()

	// With batch of 1 we do not see any issues, so set to 10.
	// Currently Go client uses auto unsub based on the batch size.
	for i := 0; i < numRequests/10; i++ {
		msgs, err := sub.Fetch(10)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		for _, m := range msgs {
			m.AckSync()
		}
	}

	// Make sure the stream is empty..
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != 0 {
		t.Fatalf("Stream should be empty, got %+v", si)
	}

	// Make sure we did not leak any subs.
	if numSubsAfter := c.stableTotalSubs(); numSubsAfter != numSubs {
		t.Fatalf("Subs leaked: %d before, %d after", numSubs, numSubsAfter)
	}
}

func TestJetStreamClusterPushConsumerQueueGroup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	}); err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}

	js.Publish("foo", []byte("QG"))

	// Do consumer by hand for now.
	inbox := nats.NewInbox()
	obsReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			Durable:        "dlc",
			DeliverSubject: inbox,
			DeliverGroup:   "22",
			AckPolicy:      AckNone,
		},
	}
	req, err := json.Marshal(obsReq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", "dlc"), req, time.Second)
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

	sub, _ := nc.SubscribeSync(inbox)
	if _, err := sub.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatalf("Expected a timeout, we should not get messages here")
	}
	qsub, _ := nc.QueueSubscribeSync(inbox, "22")
	checkSubsPending(t, qsub, 1)

	// Test deleting the plain sub has not affect.
	sub.Unsubscribe()
	js.Publish("foo", []byte("QG"))
	checkSubsPending(t, qsub, 2)

	qsub.Unsubscribe()
	qsub2, _ := nc.QueueSubscribeSync(inbox, "22")
	js.Publish("foo", []byte("QG"))
	checkSubsPending(t, qsub2, 1)

	// Catch all sub.
	sub, _ = nc.SubscribeSync(inbox)
	qsub2.Unsubscribe() // Should be no more interest.
	// Send another, make sure we do not see the message flow here.
	js.Publish("foo", []byte("QG"))
	if _, err := sub.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatalf("Expected a timeout, we should not get messages here")
	}
}

func TestJetStreamClusterConsumerLastActiveReporting(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{Name: "foo", Replicas: 2}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sendMsg := func() {
		t.Helper()
		if _, err := js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// TODO(dlc) - Do by hand for now until Go client has this.
	consumerInfo := func(name string) *ConsumerInfo {
		t.Helper()
		resp, err := nc.Request(fmt.Sprintf(JSApiConsumerInfoT, "foo", name), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var cinfo JSApiConsumerInfoResponse
		if err := json.Unmarshal(resp.Data, &cinfo); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if cinfo.ConsumerInfo == nil || cinfo.Error != nil {
			t.Fatalf("Got a bad response %+v", cinfo)
		}
		return cinfo.ConsumerInfo
	}

	if ci := consumerInfo("dlc"); ci.Delivered.Last != nil || ci.AckFloor.Last != nil {
		t.Fatalf("Expected last to be nil by default, got %+v", ci)
	}

	checkTimeDiff := func(t1, t2 *time.Time) {
		t.Helper()
		// Compare on a seconds level
		rt1, rt2 := t1.UTC().Round(time.Second), t2.UTC().Round(time.Second)
		if rt1 != rt2 {
			d := rt1.Sub(rt2)
			if d > time.Second || d < -time.Second {
				t.Fatalf("Times differ too much, expected %v got %v", rt1, rt2)
			}
		}
	}

	checkDelivered := func(name string) {
		t.Helper()
		now := time.Now()
		ci := consumerInfo(name)
		if ci.Delivered.Last == nil {
			t.Fatalf("Expected delivered last to not be nil after activity, got %+v", ci.Delivered)
		}
		checkTimeDiff(&now, ci.Delivered.Last)
	}

	checkLastAck := func(name string, m *nats.Msg) {
		t.Helper()
		now := time.Now()
		if err := m.AckSync(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ci := consumerInfo(name)
		if ci.AckFloor.Last == nil {
			t.Fatalf("Expected ack floor last to not be nil after ack, got %+v", ci.AckFloor)
		}
		// Compare on a seconds level
		checkTimeDiff(&now, ci.AckFloor.Last)
	}

	checkAck := func(name string) {
		t.Helper()
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		checkLastAck(name, m)
	}

	// Push
	sendMsg()
	checkSubsPending(t, sub, 1)
	checkDelivered("dlc")
	checkAck("dlc")

	// Check pull.
	sub, err = js.PullSubscribe("foo", "rip")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendMsg()
	// Should still be nil since pull.
	if ci := consumerInfo("rip"); ci.Delivered.Last != nil || ci.AckFloor.Last != nil {
		t.Fatalf("Expected last to be nil by default, got %+v", ci)
	}
	msgs, err := sub.Fetch(1)
	if err != nil || len(msgs) == 0 {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkDelivered("rip")
	checkLastAck("rip", msgs[0])

	// Now test to make sure this state is held correctly across a cluster.
	ci := consumerInfo("rip")
	nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "foo", "rip"), nil, time.Second)
	c.waitOnConsumerLeader("$G", "foo", "rip")
	nci := consumerInfo("rip")
	if nci.Delivered.Last == nil {
		t.Fatalf("Expected delivered last to not be nil, got %+v", nci.Delivered)
	}
	if nci.AckFloor.Last == nil {
		t.Fatalf("Expected ack floor last to not be nil, got %+v", nci.AckFloor)
	}

	checkTimeDiff(ci.Delivered.Last, nci.Delivered.Last)
	checkTimeDiff(ci.AckFloor.Last, nci.AckFloor.Last)
}

func TestJetStreamRaceOnRAFTCreate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	srv := c.servers[0]
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	}); err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}

	js, err = nc.JetStream(nats.MaxWait(2 * time.Second))
	if err != nil {
		t.Fatal(err)
	}

	size := 10
	wg := sync.WaitGroup{}
	wg.Add(size)
	for i := 0; i < size; i++ {
		go func(i int) {
			defer wg.Done()
			if _, err := js.PullSubscribe("foo", "shared"); err != nil {
				t.Errorf("Unexpected error on %v: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
}

func TestJetStreamClusterDeadlockOnVarz(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	srv := c.servers[0]
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	size := 10
	wg := sync.WaitGroup{}
	wg.Add(size)
	ch := make(chan struct{})
	for i := 0; i < size; i++ {
		go func(i int) {
			defer wg.Done()
			<-ch
			js.AddStream(&nats.StreamConfig{
				Name:     fmt.Sprintf("TEST%d", i),
				Subjects: []string{"foo"},
				Replicas: 3,
			})
		}(i)
	}

	close(ch)
	for i := 0; i < 10; i++ {
		srv.Varz(nil)
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
}

// Issue #2397
func TestJetStreamClusterStreamCatchupNoState(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R2S", 2)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Hold onto servers.
	sl := c.streamLeader("$G", "TEST")
	if sl == nil {
		t.Fatalf("Did not get a server")
	}
	nsl := c.randomNonStreamLeader("$G", "TEST")
	if nsl == nil {
		t.Fatalf("Did not get a server")
	}
	// Grab low level stream and raft node.
	mset, err := nsl.GlobalAccount().lookupStream("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	node := mset.raftNode()
	if node == nil {
		t.Fatalf("Could not get stream group name")
	}
	gname := node.Group()

	numRequests := 100
	for i := 0; i < numRequests; i++ {
		// This will force a snapshot which will prune the normal log.
		// We will remove the snapshot to simulate the error condition.
		if i == 10 {
			if err := node.InstallSnapshot(mset.stateSnapshot()); err != nil {
				t.Fatalf("Error installing snapshot: %v", err)
			}
		}
		js.Publish("foo.created", []byte("REQ"))
	}

	config := nsl.JetStreamConfig()
	if config == nil {
		t.Fatalf("No config")
	}
	lconfig := sl.JetStreamConfig()
	if lconfig == nil {
		t.Fatalf("No config")
	}

	nc.Close()
	c.stopAll()
	// Remove all state by truncating for the non-leader.
	for _, fn := range []string{"1.blk", "1.idx", "1.fss"} {
		fname := filepath.Join(config.StoreDir, "$G", "streams", "TEST", "msgs", fn)
		fd, err := os.OpenFile(fname, os.O_RDWR, defaultFilePerms)
		if err != nil {
			continue
		}
		fd.Truncate(0)
		fd.Close()
	}
	// For both make sure we have no raft snapshots.
	snapDir := filepath.Join(lconfig.StoreDir, "$SYS", "_js_", gname, "snapshots")
	os.RemoveAll(snapDir)
	snapDir = filepath.Join(config.StoreDir, "$SYS", "_js_", gname, "snapshots")
	os.RemoveAll(snapDir)

	// Now restart.
	c.restartAll()
	for _, cs := range c.servers {
		c.waitOnStreamCurrent(cs, "$G", "TEST")
	}

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	if _, err := js.Publish("foo.created", []byte("REQ")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.LastSeq != 101 {
		t.Fatalf("bad state after restart: %+v", si.State)
	}
}

// Issue #2525
func TestJetStreamClusterLargeHeaders(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("add stream failed: %s", err)
	}

	// We use u16 to encode msg header len. Make sure we do the right thing when > 65k.
	data := make([]byte, 8*1024)
	rand.Read(data)
	val := hex.EncodeToString(data)[:8*1024]
	m := nats.NewMsg("foo")
	for i := 1; i <= 10; i++ {
		m.Header.Add(fmt.Sprintf("LargeHeader-%d", i), val)
	}
	m.Data = []byte("Hello Large Headers!")
	if _, err = js.PublishMsg(m); err == nil {
		t.Fatalf("Expected an error but got none")
	}
}

func TestJetStreamClusterFlowControlRequiresHeartbeats(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "dlc",
		DeliverSubject: nats.NewInbox(),
		FlowControl:    true,
	}); err == nil || IsNatsErr(err, JSConsumerWithFlowControlNeedsHeartbeats) {
		t.Fatalf("Unexpected error: %v", err)
	}
}

var jsClusterAccountLimitsTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: js

	accounts {
		$JS { users = [ { user: "js", pass: "p" } ]; jetstream: {max_store: 1MB, max_mem: 0} }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

func TestJetStreamClusterMixedModeColdStartPrune(t *testing.T) {
	// Purposely make this unbalanced. Without changes this will never form a quorum to elect the meta-leader.
	c := createMixedModeCluster(t, jsMixedModeGlobalAccountTempl, "MMCS5", _EMPTY_, 3, 4, false)
	defer c.shutdown()

	// Make sure we report cluster size.
	checkClusterSize := func(s *Server) {
		t.Helper()
		jsi, err := s.Jsz(nil)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if jsi.Meta == nil {
			t.Fatalf("Expected a cluster info")
		}
		if jsi.Meta.Size != 3 {
			t.Fatalf("Expected cluster size to be adjusted to %d, but got %d", 3, jsi.Meta.Size)
		}
	}

	checkClusterSize(c.leader())
	checkClusterSize(c.randomNonLeader())
}

func TestJetStreamClusterMirrorAndSourceCrossNonNeighboringDomain(t *testing.T) {
	storeDir1 := createDir(t, JetStreamStoreDir)
	conf1 := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 256MB, max_file_store: 256MB, domain: domain1, store_dir: '%s'}
		accounts {
			A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
			SYS:{ users:[ {user:s1,password:s1}]},
		}
		system_account = SYS
		no_auth_user: a1
		leafnodes: {
			listen: 127.0.0.1:-1
		}
	`, storeDir1)))
	s1, _ := RunServerWithConfig(conf1)
	defer s1.Shutdown()
	storeDir2 := createDir(t, JetStreamStoreDir)
	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 256MB, max_file_store: 256MB, domain: domain2, store_dir: '%s'}
		accounts {
			A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
			SYS:{ users:[ {user:s1,password:s1}]},
		}
		system_account = SYS
		no_auth_user: a1
		leafnodes:{
			remotes:[{ url:nats://a1:a1@127.0.0.1:%d, account: A},
					 { url:nats://s1:s1@127.0.0.1:%d, account: SYS}]
		}
	`, storeDir2, s1.opts.LeafNode.Port, s1.opts.LeafNode.Port)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()
	storeDir3 := createDir(t, JetStreamStoreDir)
	conf3 := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 256MB, max_file_store: 256MB, domain: domain3, store_dir: '%s'}
		accounts {
			A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
			SYS:{ users:[ {user:s1,password:s1}]},
		}
		system_account = SYS
		no_auth_user: a1
		leafnodes:{
			remotes:[{ url:nats://a1:a1@127.0.0.1:%d, account: A},
					 { url:nats://s1:s1@127.0.0.1:%d, account: SYS}]
		}
	`, storeDir3, s1.opts.LeafNode.Port, s1.opts.LeafNode.Port)))
	s3, _ := RunServerWithConfig(conf3)
	defer s3.Shutdown()

	checkLeafNodeConnectedCount(t, s1, 4)
	checkLeafNodeConnectedCount(t, s2, 2)
	checkLeafNodeConnectedCount(t, s3, 2)

	c2 := natsConnect(t, s2.ClientURL())
	defer c2.Close()
	js2, err := c2.JetStream(nats.Domain("domain2"))
	require_NoError(t, err)
	ai2, err := js2.AccountInfo()
	require_NoError(t, err)
	require_Equal(t, ai2.Domain, "domain2")
	_, err = js2.AddStream(&nats.StreamConfig{
		Name:     "disk",
		Storage:  nats.FileStorage,
		Subjects: []string{"disk"},
	})
	require_NoError(t, err)
	_, err = js2.Publish("disk", nil)
	require_NoError(t, err)
	si, err := js2.StreamInfo("disk")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 1)

	c3 := natsConnect(t, s3.ClientURL())
	defer c3.Close()
	js3, err := c3.JetStream(nats.Domain("domain3"))
	require_NoError(t, err)
	ai3, err := js3.AccountInfo()
	require_NoError(t, err)
	require_Equal(t, ai3.Domain, "domain3")

	_, err = js3.AddStream(&nats.StreamConfig{
		Name:    "stream-mirror",
		Storage: nats.FileStorage,
		Mirror: &nats.StreamSource{
			Name:     "disk",
			External: &nats.ExternalStream{APIPrefix: "$JS.domain2.API"},
		},
	})
	require_NoError(t, err)

	_, err = js3.AddStream(&nats.StreamConfig{
		Name:    "stream-source",
		Storage: nats.FileStorage,
		Sources: []*nats.StreamSource{{
			Name:     "disk",
			External: &nats.ExternalStream{APIPrefix: "$JS.domain2.API"},
		}},
	})
	require_NoError(t, err)
	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		if si, _ := js3.StreamInfo("stream-mirror"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for mirror, got %d", si.State.Msgs)
		}
		if si, _ := js3.StreamInfo("stream-source"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for source, got %d", si.State.Msgs)
		}
		return nil
	})
}

func TestJetStreamSeal(t *testing.T) {
	s := RunBasicJetStreamServer()
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	// Need to be done by hand until makes its way to Go client.
	createStream := func(t *testing.T, nc *nats.Conn, cfg *StreamConfig) *JSApiStreamCreateResponse {
		t.Helper()
		req, err := json.Marshal(cfg)
		require_NoError(t, err)
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
		require_NoError(t, err)
		var scResp JSApiStreamCreateResponse
		err = json.Unmarshal(resp.Data, &scResp)
		require_NoError(t, err)
		return &scResp
	}

	updateStream := func(t *testing.T, nc *nats.Conn, cfg *StreamConfig) *JSApiStreamUpdateResponse {
		t.Helper()
		req, err := json.Marshal(cfg)
		require_NoError(t, err)
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamUpdateT, cfg.Name), req, time.Second)
		require_NoError(t, err)
		var scResp JSApiStreamUpdateResponse
		err = json.Unmarshal(resp.Data, &scResp)
		require_NoError(t, err)
		return &scResp
	}

	testSeal := func(t *testing.T, s *Server, replicas int) {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		// Should not be able to create a stream that starts sealed.
		scr := createStream(t, nc, &StreamConfig{Name: "SEALED", Replicas: replicas, Storage: MemoryStorage, Sealed: true})
		if scr.Error == nil {
			t.Fatalf("Expected an error but got none")
		}
		// Create our stream.
		scr = createStream(t, nc, &StreamConfig{Name: "SEALED", Replicas: replicas, MaxAge: time.Minute, Storage: MemoryStorage})
		if scr.Error != nil {
			t.Fatalf("Unexpected error: %v", scr.Error)
		}
		for i := 0; i < 100; i++ {
			js.Publish("SEALED", []byte("OK"))
		}
		// Update to sealed.
		sur := updateStream(t, nc, &StreamConfig{Name: "SEALED", Replicas: replicas, MaxAge: time.Minute, Storage: MemoryStorage, Sealed: true})
		if sur.Error != nil {
			t.Fatalf("Unexpected error: %v", sur.Error)
		}

		// Grab stream info and make sure its reflected as sealed.
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "SEALED"), nil, time.Second)
		require_NoError(t, err)
		var sir JSApiStreamInfoResponse
		err = json.Unmarshal(resp.Data, &sir)
		require_NoError(t, err)
		if sir.Error != nil {
			t.Fatalf("Unexpected error: %v", sir.Error)
		}
		si := sir.StreamInfo
		if !si.Config.Sealed {
			t.Fatalf("Expected the stream to be marked sealed, got %+v\n", si.Config)
		}
		// Make sure we also updated any max age and moved to discard new.
		if si.Config.MaxAge != 0 {
			t.Fatalf("Expected MaxAge to be cleared, got %v", si.Config.MaxAge)
		}
		if si.Config.Discard != DiscardNew {
			t.Fatalf("Expected DiscardPolicy to be set to new, got %v", si.Config.Discard)
		}
		// Also make sure we set denyDelete and denyPurge.
		if !si.Config.DenyDelete {
			t.Fatalf("Expected the stream to be marked as DenyDelete, got %+v\n", si.Config)
		}
		if !si.Config.DenyPurge {
			t.Fatalf("Expected the stream to be marked as DenyPurge, got %+v\n", si.Config)
		}
		if si.Config.AllowRollup {
			t.Fatalf("Expected the stream to be marked as not AllowRollup, got %+v\n", si.Config)
		}

		// Sealing is not reversible, so make sure we get an error trying to undo.
		sur = updateStream(t, nc, &StreamConfig{Name: "SEALED", Replicas: replicas, Storage: MemoryStorage, Sealed: false})
		if sur.Error == nil {
			t.Fatalf("Expected an error but got none")
		}

		// Now test operations like publish a new msg, delete, purge etc all fail.
		if _, err := js.Publish("SEALED", []byte("OK")); err == nil {
			t.Fatalf("Expected a publish to fail")
		}
		if err := js.DeleteMsg("SEALED", 1); err == nil {
			t.Fatalf("Expected a delete to fail")
		}
		if err := js.PurgeStream("SEALED"); err == nil {
			t.Fatalf("Expected a purge to fail")
		}
		if err := js.DeleteStream("SEALED"); err != nil {
			t.Fatalf("Expected a delete to succeed, got %v", err)
		}
	}

	t.Run("Single", func(t *testing.T) { testSeal(t, s, 1) })
	t.Run("Clustered", func(t *testing.T) { testSeal(t, c.randomServer(), 3) })
}

func addStream(t *testing.T, nc *nats.Conn, cfg *StreamConfig) *StreamInfo {
	t.Helper()
	req, err := json.Marshal(cfg)
	require_NoError(t, err)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
	require_NoError(t, err)
	var resp JSApiStreamCreateResponse
	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	if resp.Type != JSApiStreamCreateResponseType {
		t.Fatalf("Invalid response type %s expected %s", resp.Type, JSApiStreamCreateResponseType)
	}
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %+v", resp.Error)
	}
	return resp.StreamInfo
}

// Issue #2568
func TestJetStreamClusteredStreamCreateIdempotent(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &StreamConfig{
		Name:       "AUDIT",
		Storage:    MemoryStorage,
		Subjects:   []string{"foo"},
		Replicas:   3,
		DenyDelete: true,
		DenyPurge:  true,
	}
	addStream(t, nc, cfg)
	addStream(t, nc, cfg)
}

func TestJetStreamRollupsRequirePurge(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &StreamConfig{
		Name:        "SENSORS",
		Storage:     FileStorage,
		Subjects:    []string{"sensor.*.temp"},
		MaxMsgsPer:  10,
		AllowRollup: true,
		DenyPurge:   true,
		Replicas:    2,
	}

	j, err := json.Marshal(cfg)
	require_NoError(t, err)
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), j, time.Second)
	require_NoError(t, err)

	var cr JSApiStreamCreateResponse
	err = json.Unmarshal(resp.Data, &cr)
	require_NoError(t, err)
	if cr.Error == nil || cr.Error.Description != "roll-ups require the purge permission" {
		t.Fatalf("unexpected error: %v", cr.Error)
	}
}

func TestJetStreamRollups(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &StreamConfig{
		Name:        "SENSORS",
		Storage:     FileStorage,
		Subjects:    []string{"sensor.*.temp"},
		MaxMsgsPer:  10,
		AllowRollup: true,
		Replicas:    2,
	}
	addStream(t, nc, cfg)

	var bt [16]byte
	var le = binary.LittleEndian

	// Generate 1000 random measurements for 10 sensors
	for i := 0; i < 1000; i++ {
		id, temp := strconv.Itoa(rand.Intn(9)+1), rand.Int31n(42)+60 // 60-102 degrees.
		le.PutUint16(bt[0:], uint16(temp))
		js.PublishAsync(fmt.Sprintf("sensor.%v.temp", id), bt[:])
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Grab random sensor and do a rollup by averaging etc.
	sensor := fmt.Sprintf("sensor.%v.temp", strconv.Itoa(rand.Intn(9)+1))
	sub, err := js.SubscribeSync(sensor)
	require_NoError(t, err)

	var total, samples int
	for m, err := sub.NextMsg(time.Second); err == nil; m, err = sub.NextMsg(time.Second) {
		total += int(le.Uint16(m.Data))
		samples++
	}
	sub.Unsubscribe()
	avg := uint16(total / samples)
	le.PutUint16(bt[0:], avg)

	rollup := nats.NewMsg(sensor)
	rollup.Data = bt[:]
	rollup.Header.Set(JSMsgRollup, JSMsgRollupSubject)
	_, err = js.PublishMsg(rollup)
	require_NoError(t, err)
	sub, err = js.SubscribeSync(sensor)
	require_NoError(t, err)
	// Make sure only 1 left.
	checkSubsPending(t, sub, 1)
	sub.Unsubscribe()

	// Now do all.
	rollup.Header.Set(JSMsgRollup, JSMsgRollupAll)
	_, err = js.PublishMsg(rollup)
	require_NoError(t, err)
	// Same thing as above should hold true.
	sub, err = js.SubscribeSync(sensor)
	require_NoError(t, err)
	// Make sure only 1 left.
	checkSubsPending(t, sub, 1)
	sub.Unsubscribe()

	// Also should only be 1 msgs in total stream left with JSMsgRollupAll
	si, err := js.StreamInfo("SENSORS")
	require_NoError(t, err)
	if si.State.Msgs != 1 {
		t.Fatalf("Expected only 1 msg left after rollup all, got %+v", si.State)
	}
}

func TestJetStreamRollupSubjectAndWatchers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &StreamConfig{
		Name:        "KVW",
		Storage:     FileStorage,
		Subjects:    []string{"kv.*"},
		MaxMsgsPer:  10,
		AllowRollup: true,
		Replicas:    2,
	}
	addStream(t, nc, cfg)

	sub, err := js.SubscribeSync("kv.*")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	send := func(key, value string) {
		t.Helper()
		_, err := js.Publish("kv."+key, []byte(value))
		require_NoError(t, err)
	}

	rollup := func(key, value string) {
		t.Helper()
		m := nats.NewMsg("kv." + key)
		m.Data = []byte(value)
		m.Header.Set(JSMsgRollup, JSMsgRollupSubject)
		_, err := js.PublishMsg(m)
		require_NoError(t, err)
	}

	expectUpdate := func(key, value string, seq uint64) {
		t.Helper()
		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		if m.Subject != "kv."+key {
			t.Fatalf("Keys don't match: %q vs %q", m.Subject[3:], key)
		}
		if string(m.Data) != value {
			t.Fatalf("Values don't match: %q vs %q", m.Data, value)
		}
		meta, err := m.Metadata()
		require_NoError(t, err)
		if meta.Sequence.Consumer != seq {
			t.Fatalf("Sequences don't match: %v vs %v", meta.Sequence.Consumer, value)
		}
	}

	rollup("name", "derek")
	expectUpdate("name", "derek", 1)
	rollup("age", "22")
	expectUpdate("age", "22", 2)

	send("name", "derek")
	expectUpdate("name", "derek", 3)
	send("age", "22")
	expectUpdate("age", "22", 4)
	send("age", "33")
	expectUpdate("age", "33", 5)
	send("name", "ivan")
	expectUpdate("name", "ivan", 6)
	send("name", "rip")
	expectUpdate("name", "rip", 7)
	rollup("age", "50")
	expectUpdate("age", "50", 8)
}

func TestJetStreamClusterAppendOnly(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &StreamConfig{
		Name:       "AUDIT",
		Storage:    MemoryStorage,
		Subjects:   []string{"foo"},
		Replicas:   3,
		DenyDelete: true,
		DenyPurge:  true,
	}
	si := addStream(t, nc, cfg)
	if !si.Config.DenyDelete || !si.Config.DenyPurge {
		t.Fatalf("Expected DenyDelete and DenyPurge to be set, got %+v", si.Config)
	}
	for i := 0; i < 10; i++ {
		js.Publish("foo", []byte("ok"))
	}
	// Delete should not be allowed.
	if err := js.DeleteMsg("AUDIT", 1); err == nil {
		t.Fatalf("Expected an error for delete but got none")
	}
	if err := js.PurgeStream("AUDIT"); err == nil {
		t.Fatalf("Expected an error for purge but got none")
	}

	cfg.DenyDelete = false
	cfg.DenyPurge = false

	req, err := json.Marshal(cfg)
	require_NoError(t, err)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamUpdateT, cfg.Name), req, time.Second)
	require_NoError(t, err)
	var resp JSApiStreamCreateResponse
	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	if resp.Error == nil {
		t.Fatalf("Expected an error")
	}
}

// Related to #2642
func TestJetStreamClusterStreamUpdateSyncBug(t *testing.T) {
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

	msg, toSend := []byte("OK"), 100
	for i := 0; i < toSend; i++ {
		if _, err := js.PublishAsync("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	cfg.Subjects = []string{"foo", "bar", "baz"}
	if _, err := js.UpdateStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Shutdown a server. The bug is that the update wiped the sync subject used to cacthup a stream that has the RAFT layer snapshotted.
	nsl := c.randomNonStreamLeader("$G", "TEST")
	nsl.Shutdown()
	// make sure a leader exists
	c.waitOnStreamLeader("$G", "TEST")

	for i := 0; i < toSend*4; i++ {
		if _, err := js.PublishAsync("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Throw in deletes as well.
	for seq := uint64(200); seq < uint64(300); seq += 4 {
		if err := js.DeleteMsg("TEST", seq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// We need to snapshot to force upper layer catchup vs RAFT layer.
	mset, err := c.streamLeader("$G", "TEST").GlobalAccount().lookupStream("TEST")
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", "TEST")
	}
	if err := mset.raftNode().InstallSnapshot(mset.stateSnapshot()); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	nsl = c.restartServer(nsl)
	c.waitOnStreamCurrent(nsl, "$G", "TEST")

	mset, _ = nsl.GlobalAccount().lookupStream("TEST")
	cloneState := mset.state()

	mset, _ = c.streamLeader("$G", "TEST").GlobalAccount().lookupStream("TEST")
	leaderState := mset.state()

	if !reflect.DeepEqual(cloneState, leaderState) {
		t.Fatalf("States do not match: %+v vs %+v", cloneState, leaderState)
	}
}

func TestJetStreamClusterStreamUpdateMissingBeginning(t *testing.T) {
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

	msg, toSend := []byte("OK"), 100
	for i := 0; i < toSend; i++ {
		if _, err := js.PublishAsync("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	nsl := c.randomNonStreamLeader("$G", "TEST")
	// Delete the first 50 messages manually from only this server.
	mset, _ := nsl.GlobalAccount().lookupStream("TEST")
	if _, err := mset.purge(&JSApiStreamPurgeRequest{Sequence: 50}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now shutdown.
	nsl.Shutdown()
	// make sure a leader exists
	c.waitOnStreamLeader("$G", "TEST")

	for i := 0; i < toSend; i++ {
		if _, err := js.PublishAsync("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// We need to snapshot to force upper layer catchup vs RAFT layer.
	mset, err := c.streamLeader("$G", "TEST").GlobalAccount().lookupStream("TEST")
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", "TEST")
	}
	if err := mset.raftNode().InstallSnapshot(mset.stateSnapshot()); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	nsl = c.restartServer(nsl)
	c.waitOnStreamCurrent(nsl, "$G", "TEST")

	mset, _ = nsl.GlobalAccount().lookupStream("TEST")
	cloneState := mset.state()

	mset, _ = c.streamLeader("$G", "TEST").GlobalAccount().lookupStream("TEST")
	leaderState := mset.state()

	if !reflect.DeepEqual(cloneState, leaderState) {
		t.Fatalf("States do not match: %+v vs %+v", cloneState, leaderState)
	}
}

// Issue #2666
func TestJetStreamClusterKVMultipleConcurrentCreate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TEST", History: 1, TTL: 150 * time.Millisecond, Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	startCh := make(chan bool)
	var wg sync.WaitGroup

	for n := 0; n < 5; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh
			if r, err := kv.Create("name", []byte("dlc")); err == nil {
				if _, err = kv.Update("name", []byte("rip"), r); err != nil {
					t.Log("Unexpected Update error: ", err)
				}
			}
		}()
	}
	// Wait for Go routines to start.
	time.Sleep(100 * time.Millisecond)
	close(startCh)
	wg.Wait()
	// Just make sure its there and picks up the phone.
	if _, err := js.StreamInfo("KV_TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now make sure we do ok when servers are restarted and we need to deal with dangling clfs state.
	// First non-leader.
	rs := c.randomNonStreamLeader("$G", "KV_TEST")
	rs.Shutdown()
	rs = c.restartServer(rs)
	c.waitOnStreamCurrent(rs, "$G", "KV_TEST")

	if _, err := kv.Put("name", []byte("ik")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now the actual leader.
	sl := c.streamLeader("$G", "KV_TEST")
	sl.Shutdown()
	sl = c.restartServer(sl)
	c.waitOnStreamLeader("$G", "KV_TEST")
	c.waitOnStreamCurrent(sl, "$G", "KV_TEST")

	if _, err := kv.Put("name", []byte("mh")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	time.Sleep(time.Second)
}

func TestJetStreamClusterAccountInfoForSystemAccount(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	if _, err := js.AccountInfo(); err != nats.ErrJetStreamNotEnabled {
		t.Fatalf("Expected a not enabled error for system account, got %v", err)
	}
}

func TestJetStreamListFilter(t *testing.T) {
	s := RunBasicJetStreamServer()
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	testList := func(t *testing.T, srv *Server, r int) {
		nc, js := jsClientConnect(t, srv)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "ONE",
			Subjects: []string{"one.>"},
			Replicas: r,
		})
		require_NoError(t, err)

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "TWO",
			Subjects: []string{"two.>"},
			Replicas: r,
		})
		require_NoError(t, err)

		resp, err := nc.Request(JSApiStreamList, []byte("{}"), time.Second)
		require_NoError(t, err)

		list := &JSApiStreamListResponse{}
		err = json.Unmarshal(resp.Data, list)
		require_NoError(t, err)

		if len(list.Streams) != 2 {
			t.Fatalf("Expected 2 responses got %d", len(list.Streams))
		}

		resp, err = nc.Request(JSApiStreamList, []byte(`{"subject":"two.x"}`), time.Second)
		require_NoError(t, err)
		list = &JSApiStreamListResponse{}
		err = json.Unmarshal(resp.Data, list)
		require_NoError(t, err)
		if len(list.Streams) != 1 {
			t.Fatalf("Expected 1 response got %d", len(list.Streams))
		}
		if list.Streams[0].Config.Name != "TWO" {
			t.Fatalf("Expected stream TWO in result got %#v", list.Streams[0])
		}
	}

	t.Run("Single", func(t *testing.T) { testList(t, s, 1) })
	t.Run("Clustered", func(t *testing.T) { testList(t, c.randomServer(), 3) })
}

func TestJetStreamConsumerUpdates(t *testing.T) {
	s := RunBasicJetStreamServer()
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "JSC", 5)
	defer c.shutdown()

	testConsumerUpdate := func(t *testing.T, s *Server, replicas int) {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		// Create a stream.
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo", "bar"},
			Replicas: replicas,
		})
		require_NoError(t, err)

		for i := 0; i < 100; i++ {
			js.PublishAsync("foo", []byte("OK"))
		}

		cfg := &nats.ConsumerConfig{
			Durable:        "dlc",
			Description:    "Update TEST",
			FilterSubject:  "foo",
			DeliverSubject: "d.foo",
			AckPolicy:      nats.AckExplicitPolicy,
			AckWait:        time.Minute,
			MaxDeliver:     5,
			MaxAckPending:  50,
		}

		_, err = js.AddConsumer("TEST", cfg)
		require_NoError(t, err)

		// Update delivery subject, which worked before, but upon review had issues unless replica count == clustered size.
		cfg.DeliverSubject = "d.bar"
		_, err = js.AddConsumer("TEST", cfg)
		require_NoError(t, err)

		// Bind deliver subject.
		sub, err := nc.SubscribeSync("d.bar")
		require_NoError(t, err)
		defer sub.Unsubscribe()

		ncfg := *cfg
		ncfg.DeliverSubject = "d.baz"

		// Should fail.
		_, err = js.AddConsumer("TEST", &ncfg)
		require_Error(t, err)

		// Description
		cfg.Description = "New Description"
		_, err = js.AddConsumer("TEST", cfg)
		require_NoError(t, err)

		// MaxAckPending
		checkSubsPending(t, sub, 50)
		cfg.MaxAckPending = 75
		_, err = js.AddConsumer("TEST", cfg)
		require_NoError(t, err)
		checkSubsPending(t, sub, 75)

		// Drain sub, do not ack first ten though so we can test shortening AckWait.
		for i := 0; i < 100; i++ {
			m, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
			if i >= 10 {
				m.AckSync()
			}
		}

		// AckWait
		checkSubsPending(t, sub, 0)
		cfg.AckWait = 200 * time.Millisecond
		_, err = js.AddConsumer("TEST", cfg)
		require_NoError(t, err)
		checkSubsPending(t, sub, 10)

		// Rate Limit
		cfg.RateLimit = 8 * 1024
		_, err = js.AddConsumer("TEST", cfg)
		require_NoError(t, err)

		cfg.RateLimit = 0
		_, err = js.AddConsumer("TEST", cfg)
		require_NoError(t, err)

		// These all should fail.
		ncfg = *cfg
		ncfg.FilterSubject = "bar"
		_, err = js.AddConsumer("TEST", &ncfg)
		require_Error(t, err)

		ncfg = *cfg
		ncfg.DeliverPolicy = nats.DeliverLastPolicy
		_, err = js.AddConsumer("TEST", &ncfg)
		require_Error(t, err)

		ncfg = *cfg
		ncfg.OptStartSeq = 22
		_, err = js.AddConsumer("TEST", &ncfg)
		require_Error(t, err)

		ncfg = *cfg
		now := time.Now()
		ncfg.OptStartTime = &now
		_, err = js.AddConsumer("TEST", &ncfg)
		require_Error(t, err)

		ncfg = *cfg
		ncfg.AckPolicy = nats.AckAllPolicy
		_, err = js.AddConsumer("TEST", &ncfg)
		require_Error(t, err)

		ncfg = *cfg
		ncfg.ReplayPolicy = nats.ReplayOriginalPolicy
		_, err = js.AddConsumer("TEST", &ncfg)
		require_Error(t, err)

		ncfg = *cfg
		ncfg.Heartbeat = time.Second
		_, err = js.AddConsumer("TEST", &ncfg)
		require_Error(t, err)

		ncfg = *cfg
		ncfg.FlowControl = true
		_, err = js.AddConsumer("TEST", &ncfg)
		require_Error(t, err)

	}

	t.Run("Single", func(t *testing.T) { testConsumerUpdate(t, s, 1) })
	t.Run("Clustered", func(t *testing.T) { testConsumerUpdate(t, c.randomServer(), 2) })
}

func TestJetStreamSuperClusterPushConsumerInterest(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	for _, test := range []struct {
		name  string
		queue string
	}{
		{"non queue", _EMPTY_},
		{"queue", "queue"},
	} {
		t.Run(test.name, func(t *testing.T) {
			testInterest := func(s *Server) {
				t.Helper()
				nc, js := jsClientConnect(t, s)
				defer nc.Close()

				_, err := js.AddStream(&nats.StreamConfig{
					Name:     "TEST",
					Subjects: []string{"foo"},
					Replicas: 3,
				})
				require_NoError(t, err)

				var sub *nats.Subscription
				if test.queue != _EMPTY_ {
					sub, err = js.QueueSubscribeSync("foo", test.queue)
				} else {
					sub, err = js.SubscribeSync("foo", nats.Durable("dur"))
				}
				require_NoError(t, err)

				js.Publish("foo", []byte("msg1"))
				// Since the GW watcher is checking every 1sec, make sure we are
				// giving it enough time for the delivery to start.
				_, err = sub.NextMsg(2 * time.Second)
				require_NoError(t, err)
			}

			// Create the durable push consumer from cluster "0"
			testInterest(sc.clusters[0].servers[0])

			// Now "move" to a server in cluster "1"
			testInterest(sc.clusters[1].servers[0])
		})
	}
}

func TestJetStreamClusterAccountReservations(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterMaxBytesAccountLimitTempl, "C1", 3)
	defer c.shutdown()

	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	accMax := 3

	test := func(t *testing.T, replica int) {
		mb := int64((1+accMax)-replica) * 1024 * 1024 * 1024 // GB, corrected for replication factor
		_, err := js.AddStream(&nats.StreamConfig{Name: "S1", Subjects: []string{"s1"}, MaxBytes: mb, Replicas: replica})
		require_NoError(t, err)

		_, err = js.AddStream(&nats.StreamConfig{Name: "S2", Subjects: []string{"s2"}, MaxBytes: 1024, Replicas: replica})
		require_Error(t, err)
		require_Equal(t, err.Error(), "insufficient storage resources available")

		_, err = js.UpdateStream(&nats.StreamConfig{Name: "S1", Subjects: []string{"s1"}, MaxBytes: mb / 2, Replicas: replica})
		require_NoError(t, err)

		_, err = js.AddStream(&nats.StreamConfig{Name: "S2", Subjects: []string{"s2"}, MaxBytes: mb / 2, Replicas: replica})
		require_NoError(t, err)

		_, err = js.AddStream(&nats.StreamConfig{Name: "S3", Subjects: []string{"s3"}, MaxBytes: 1024, Replicas: replica})
		require_Error(t, err)
		require_Equal(t, err.Error(), "insufficient storage resources available")

		_, err = js.UpdateStream(&nats.StreamConfig{Name: "S2", Subjects: []string{"s2"}, MaxBytes: mb/2 + 1, Replicas: replica})
		require_Error(t, err)
		require_Equal(t, err.Error(), "insufficient storage resources available")

		require_NoError(t, js.DeleteStream("S1"))
		require_NoError(t, js.DeleteStream("S2"))
	}
	test(t, 3)
	test(t, 1)
}

func TestJetStreamClusterOverflowPlacement(t *testing.T) {
	sc := createJetStreamSuperClusterWithTemplate(t, jsClusterMaxBytesTempl, 3, 3)
	defer sc.shutdown()

	pcn := "C2"
	s := sc.clusterForName(pcn).randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// With this setup, we opted in for requiring MaxBytes, so this should error.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Replicas: 3,
	})
	require_Error(t, err, NewJSStreamMaxBytesRequiredError())

	// R=2 on purpose to leave one server empty.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Replicas: 2,
		MaxBytes: 2 * 1024 * 1024 * 1024,
	})
	require_NoError(t, err)

	// Now try to add another that will overflow the current cluster's reservation.
	// Since we asked explicitly for the same cluster this should fail.
	// Note this will not be testing the peer picker since the update has probably not made it to the meta leader.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      "bar",
		Replicas:  3,
		MaxBytes:  2 * 1024 * 1024 * 1024,
		Placement: &nats.Placement{Cluster: pcn},
	})
	require_Error(t, err, NewJSInsufficientResourcesError(), NewJSStorageResourcesExceededError())

	// Now test actual overflow placement. So try again with no placement designation.
	// This will test the peer picker's logic since they are updated at this point and the meta leader
	// knows it can not place it in C2.
	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "bar",
		Replicas: 3,
		MaxBytes: 2 * 1024 * 1024 * 1024,
	})
	require_NoError(t, err)

	// Make sure we did not get place into C2.
	falt := si.Cluster.Name
	if falt == pcn {
		t.Fatalf("Expected to be placed in another cluster besides %q, but got %q", pcn, falt)
	}

	// One more time that should spill over again to our last cluster.
	si, err = js.AddStream(&nats.StreamConfig{
		Name:     "baz",
		Replicas: 3,
		MaxBytes: 2 * 1024 * 1024 * 1024,
	})
	require_NoError(t, err)

	// Make sure we did not get place into C2.
	if salt := si.Cluster.Name; salt == pcn || salt == falt {
		t.Fatalf("Expected to be placed in last cluster besides %q or %q, but got %q", pcn, falt, salt)
	}

	// Now place a stream of R1 into C2 which should have space.
	si, err = js.AddStream(&nats.StreamConfig{
		Name:     "dlc",
		MaxBytes: 2 * 1024 * 1024 * 1024,
	})
	require_NoError(t, err)

	if si.Cluster.Name != pcn {
		t.Fatalf("Expected to be placed in our origin cluster %q, but got %q", pcn, si.Cluster.Name)
	}
}

func TestJetStreamClusterConcurrentAccountLimits(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterMaxBytesAccountLimitTempl, "cluster", 3)
	defer c.shutdown()

	startCh := make(chan bool)
	var wg sync.WaitGroup
	var swg sync.WaitGroup
	failCount := int32(0)

	start := func(name string) {
		wg.Add(1)
		defer wg.Done()

		s := c.randomServer()
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		swg.Done()
		<-startCh

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     name,
			Replicas: 3,
			MaxBytes: 1024 * 1024 * 1024,
		})
		if err != nil {
			atomic.AddInt32(&failCount, 1)
			require_Equal(t, err.Error(), "insufficient storage resources available")
		}
	}

	swg.Add(2)
	go start("foo")
	go start("bar")
	swg.Wait()
	// Now start both at same time.
	close(startCh)
	wg.Wait()
	require_True(t, failCount == 1)
}

func TestJetStreamClusterConcurrentOverflow(t *testing.T) {
	sc := createJetStreamSuperClusterWithTemplate(t, jsClusterMaxBytesTempl, 3, 3)
	defer sc.shutdown()

	pcn := "C2"

	startCh := make(chan bool)
	var wg sync.WaitGroup
	var swg sync.WaitGroup

	start := func(name string) {
		defer wg.Done()

		s := sc.clusterForName(pcn).randomServer()
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		swg.Done()
		<-startCh

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     name,
			Replicas: 3,
			MaxBytes: 2 * 1024 * 1024 * 1024,
		})
		require_NoError(t, err)
	}
	wg.Add(2)
	swg.Add(2)
	go start("foo")
	go start("bar")
	swg.Wait()
	// Now start both at same time.
	close(startCh)
	wg.Wait()
}

func TestJetStreamClusterBalancedPlacement(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterMaxBytesTempl, "CB", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// We have 10GB (2GB X 5) available.
	// Use MaxBytes for ease of test (used works too) and place 5 1GB streams with R=2.
	for i := 1; i <= 5; i++ {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     fmt.Sprintf("S-%d", i),
			Replicas: 2,
			MaxBytes: 1 * 1024 * 1024 * 1024,
		})
		require_NoError(t, err)
	}
	// Make sure the next one fails properly.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "FAIL",
		Replicas: 2,
		MaxBytes: 1 * 1024 * 1024 * 1024,
	})
	require_Error(t, err, NewJSInsufficientResourcesError(), NewJSStorageResourcesExceededError())
}

func TestJetStreamClusterConsumerPendingBug(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	nc2, js2 := jsClientConnect(t, c.randomServer())
	defer nc2.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 3})
	require_NoError(t, err)

	startCh, doneCh := make(chan bool), make(chan error)
	go func() {
		<-startCh
		_, err := js2.AddConsumer("foo", &nats.ConsumerConfig{
			Durable:        "dlc",
			FilterSubject:  "foo",
			DeliverSubject: "x",
		})
		doneCh <- err
	}()

	n := 10_000
	for i := 0; i < n; i++ {
		nc.Publish("foo", []byte("ok"))
		if i == 222 {
			startCh <- true
		}
	}
	// Wait for them to all be there.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("foo")
		require_NoError(t, err)
		if si.State.Msgs != uint64(n) {
			return fmt.Errorf("Not received all messages")
		}
		return nil
	})

	select {
	case err := <-doneCh:
		if err != nil {
			t.Fatalf("Error creating consumer: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Timed out?")
	}
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("foo", "dlc")
		require_NoError(t, err)
		if ci.NumPending != uint64(n) {
			return fmt.Errorf("Expected NumPending to be %d, got %d", n, ci.NumPending)
		}
		return nil
	})
}

func TestJetStreamClusterPullPerf(t *testing.T) {
	skip(t)

	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	js.AddStream(&nats.StreamConfig{Name: "f22"})
	defer js.DeleteStream("f22")

	n, msg := 1_000_000, []byte(strings.Repeat("A", 1000))
	for i := 0; i < n; i++ {
		js.PublishAsync("f22", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(10 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	si, err := js.StreamInfo("f22")
	require_NoError(t, err)

	fmt.Printf("msgs: %d, total_bytes: %v\n", si.State.Msgs, friendlyBytes(int64(si.State.Bytes)))

	// OrderedConsumer - fastest push based.
	start := time.Now()
	received, done := 0, make(chan bool)
	_, err = js.Subscribe("f22", func(m *nats.Msg) {
		received++
		if received >= n {
			done <- true
		}
	}, nats.OrderedConsumer())
	require_NoError(t, err)

	// Wait to receive all messages.
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("Did not receive all of our messages")
	}

	tt := time.Since(start)
	fmt.Printf("Took %v to receive %d msgs\n", tt, n)
	fmt.Printf("%.0f msgs/s\n", float64(n)/tt.Seconds())
	fmt.Printf("%.0f mb/s\n\n", float64(si.State.Bytes/(1024*1024))/tt.Seconds())

	// Now do pull based, this is custom for now.
	// Current nats.PullSubscribe maxes at about 1/2 the performance even with large batches.
	_, err = js.AddConsumer("f22", &nats.ConsumerConfig{
		Durable:       "dlc",
		AckPolicy:     nats.AckAllPolicy,
		MaxAckPending: 1000,
	})
	require_NoError(t, err)

	r := 0
	_, err = nc.Subscribe("xx", func(m *nats.Msg) {
		r++
		if r >= n {
			done <- true
		}
		if r%750 == 0 {
			m.AckSync()
		}
	})
	require_NoError(t, err)

	// Simulate an non-ending request.
	req := &JSApiConsumerGetNextRequest{Batch: n, Expires: 60 * time.Second}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)

	start = time.Now()
	rsubj := fmt.Sprintf(JSApiRequestNextT, "f22", "dlc")
	err = nc.PublishRequest(rsubj, "xx", jreq)
	require_NoError(t, err)

	// Wait to receive all messages.
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatalf("Did not receive all of our messages")
	}

	tt = time.Since(start)
	fmt.Printf("Took %v to receive %d msgs\n", tt, n)
	fmt.Printf("%.0f msgs/s\n", float64(n)/tt.Seconds())
	fmt.Printf("%.0f mb/s\n\n", float64(si.State.Bytes/(1024*1024))/tt.Seconds())
}

// Test that we get the right signaling when a consumer leader change occurs for any pending requests.
func TestJetStreamClusterPullConsumerLeaderChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 3,
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "dlc",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "foo",
	})
	require_NoError(t, err)

	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", "dlc")
	sub, err := nc.SubscribeSync("reply")
	require_NoError(t, err)
	defer sub.Unsubscribe()

	drainSub := func() {
		t.Helper()
		for _, err := sub.NextMsg(0); err == nil; _, err = sub.NextMsg(0) {
		}
		checkSubsPending(t, sub, 0)
	}

	// Queue up a request that can live for a bit.
	req := &JSApiConsumerGetNextRequest{Expires: 2 * time.Second}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)
	err = nc.PublishRequest(rsubj, "reply", jreq)
	require_NoError(t, err)
	// Make sure request is recorded and replicated.
	time.Sleep(100 * time.Millisecond)
	checkSubsPending(t, sub, 0)

	// Now have consumer leader change and make sure we get signaled that our request is not valid.
	_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "dlc"), nil, time.Second)
	require_NoError(t, err)
	c.waitOnConsumerLeader("$G", "TEST", "dlc")
	checkSubsPending(t, sub, 1)
	m, err := sub.NextMsg(0)
	require_NoError(t, err)
	// Make sure this is an alert that tells us our request is no longer valid.
	if m.Header.Get("Status") != "409" {
		t.Fatalf("Expected a 409 status code, got %q", m.Header.Get("Status"))
	}
	checkSubsPending(t, sub, 0)

	// Add a few messages to the stream to fulfill a request.
	for i := 0; i < 10; i++ {
		_, err := js.Publish("foo", []byte("HELLO"))
		require_NoError(t, err)
	}
	req = &JSApiConsumerGetNextRequest{Batch: 10, Expires: 10 * time.Second}
	jreq, err = json.Marshal(req)
	require_NoError(t, err)
	err = nc.PublishRequest(rsubj, "reply", jreq)
	require_NoError(t, err)
	checkSubsPending(t, sub, 10)
	drainSub()

	// Now do a leader change again, make sure we do not get anything about that request.
	_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "dlc"), nil, time.Second)
	require_NoError(t, err)
	c.waitOnConsumerLeader("$G", "TEST", "dlc")
	time.Sleep(100 * time.Millisecond)
	checkSubsPending(t, sub, 0)

	// Make sure we do not get anything if we expire, etc.
	req = &JSApiConsumerGetNextRequest{Batch: 10, Expires: 250 * time.Millisecond}
	jreq, err = json.Marshal(req)
	require_NoError(t, err)
	err = nc.PublishRequest(rsubj, "reply", jreq)
	require_NoError(t, err)
	// Let it expire.
	time.Sleep(350 * time.Millisecond)
	checkSubsPending(t, sub, 1)

	// Now do a leader change again, make sure we do not get anything about that request.
	_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "dlc"), nil, time.Second)
	require_NoError(t, err)
	c.waitOnConsumerLeader("$G", "TEST", "dlc")
	checkSubsPending(t, sub, 1)
}

func TestJetStreamClusterEphemeralPullConsumerServerShutdown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 2,
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "foo",
	})
	require_NoError(t, err)

	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", ci.Name)
	sub, err := nc.SubscribeSync("reply")
	require_NoError(t, err)
	defer sub.Unsubscribe()

	// Queue up a request that can live for a bit.
	req := &JSApiConsumerGetNextRequest{Expires: 2 * time.Second}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)
	err = nc.PublishRequest(rsubj, "reply", jreq)
	require_NoError(t, err)
	// Make sure request is recorded and replicated.
	time.Sleep(100 * time.Millisecond)
	checkSubsPending(t, sub, 0)

	// Now shutdown the server where this ephemeral lives.
	c.consumerLeader("$G", "TEST", ci.Name).Shutdown()
	checkSubsPending(t, sub, 1)
	m, err := sub.NextMsg(0)
	require_NoError(t, err)
	// Make sure this is an alert that tells us our request is no longer valid.
	if m.Header.Get("Status") != "409" {
		t.Fatalf("Expected a 409 status code, got %q", m.Header.Get("Status"))
	}
}

func TestJetStreamClusterNAKBackoffs(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 2,
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", []byte("NAK"))
	require_NoError(t, err)

	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"), nats.AckWait(5*time.Second), nats.ManualAck())
	require_NoError(t, err)
	defer sub.Unsubscribe()

	checkSubsPending(t, sub, 1)
	m, err := sub.NextMsg(0)
	require_NoError(t, err)

	// Default nak will redeliver almost immediately.
	// We can now add a parse duration string after whitespace to the NAK proto.
	start := time.Now()
	dnak := []byte(fmt.Sprintf("%s 200ms", AckNak))
	m.Respond(dnak)
	checkSubsPending(t, sub, 1)
	elapsed := time.Since(start)
	if elapsed < 200*time.Millisecond {
		t.Fatalf("Took too short to redeliver, expected ~200ms but got %v", elapsed)
	}
	if elapsed > time.Second {
		t.Fatalf("Took too long to redeliver, expected ~200ms but got %v", elapsed)
	}

	// Now let's delay and make sure that is honored when a new consumer leader takes over.
	m, err = sub.NextMsg(0)
	require_NoError(t, err)
	dnak = []byte(fmt.Sprintf("%s 1s", AckNak))
	start = time.Now()
	m.Respond(dnak)
	// Wait for NAK state to propagate.
	time.Sleep(100 * time.Millisecond)
	// Ask leader to stepdown.
	_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "dlc"), nil, time.Second)
	require_NoError(t, err)
	c.waitOnConsumerLeader("$G", "TEST", "dlc")
	checkSubsPending(t, sub, 1)
	elapsed = time.Since(start)
	if elapsed < time.Second {
		t.Fatalf("Took too short to redeliver, expected ~1s but got %v", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("Took too long to redeliver, expected ~1s but got %v", elapsed)
	}

	// Test json version.
	delay, err := json.Marshal(&ConsumerNakOptions{Delay: 20 * time.Millisecond})
	require_NoError(t, err)
	dnak = []byte(fmt.Sprintf("%s  %s", AckNak, delay))
	m, err = sub.NextMsg(0)
	require_NoError(t, err)
	start = time.Now()
	m.Respond(dnak)
	checkSubsPending(t, sub, 1)
	elapsed = time.Since(start)
	if elapsed < 20*time.Millisecond {
		t.Fatalf("Took too short to redeliver, expected ~20ms but got %v", elapsed)
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("Took too long to redeliver, expected ~20ms but got %v", elapsed)
	}
}

func TestJetStreamClusterRedeliverBackoffs(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 2,
		Subjects: []string{"foo", "bar"},
	})
	require_NoError(t, err)

	// Produce some messages on bar so that when we create the consumer
	// on "foo", we don't have a 1:1 between consumer/stream sequence.
	for i := 0; i < 10; i++ {
		js.Publish("bar", []byte("msg"))
	}

	// Test when BackOff is configured and AckWait and MaxDeliver are as well.
	// Currently the BackOff will override AckWait, but we want MaxDeliver to be set to be at least len(BackOff)+1.
	ccReq := &CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			Durable:        "dlc",
			FilterSubject:  "foo",
			DeliverSubject: "x",
			AckPolicy:      AckExplicit,
			AckWait:        30 * time.Second,
			MaxDeliver:     2,
			BackOff:        []time.Duration{25 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond},
		},
	}
	req, err := json.Marshal(ccReq)
	require_NoError(t, err)
	resp, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", "dlc"), req, time.Second)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	if ccResp.Error == nil || ccResp.Error.ErrCode != 10116 {
		t.Fatalf("Expected an error when MaxDeliver is <= len(BackOff), got %+v", ccResp.Error)
	}

	// Set MaxDeliver to 6.
	ccReq.Config.MaxDeliver = 6
	req, err = json.Marshal(ccReq)
	require_NoError(t, err)
	resp, err = nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", "dlc"), req, time.Second)
	require_NoError(t, err)
	ccResp.Error = nil
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	if ccResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", ccResp.Error)
	}
	if cfg := ccResp.ConsumerInfo.Config; cfg.AckWait != 25*time.Millisecond || cfg.MaxDeliver != 6 {
		t.Fatalf("Expected AckWait to be first BackOff (25ms) and MaxDeliver set to 6, got %+v", cfg)
	}

	var received []time.Time
	var mu sync.Mutex

	sub, err := nc.Subscribe("x", func(m *nats.Msg) {
		mu.Lock()
		received = append(received, time.Now())
		mu.Unlock()
	})
	require_NoError(t, err)

	// Send a message.
	start := time.Now()
	_, err = js.Publish("foo", []byte("m22"))
	require_NoError(t, err)

	checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
		mu.Lock()
		nr := len(received)
		mu.Unlock()
		if nr >= 6 {
			return nil
		}
		return fmt.Errorf("Only seen %d of 6", nr)
	})
	sub.Unsubscribe()

	expected := ccReq.Config.BackOff
	// We expect the MaxDeliver to go until 6, so fill in two additional ones.
	expected = append(expected, 250*time.Millisecond, 250*time.Millisecond)
	for i, tr := range received[1:] {
		d := tr.Sub(start)
		// Adjust start for next calcs.
		start = start.Add(d)
		if d < expected[i] || d > expected[i]*2 {
			t.Fatalf("Timing is off for %d, expected ~%v, but got %v", i, expected[i], d)
		}
	}
}

func TestJetStreamConsumerUpgrade(t *testing.T) {
	s := RunBasicJetStreamServer()
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	testUpdate := func(t *testing.T, s *Server) {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		_, err := js.AddStream(&nats.StreamConfig{Name: "X"})
		require_NoError(t, err)
		_, err = js.Publish("X", []byte("OK"))
		require_NoError(t, err)
		// First create a consumer that is push based.
		_, err = js.AddConsumer("X", &nats.ConsumerConfig{Durable: "dlc", DeliverSubject: "Y"})
		require_NoError(t, err)
		// Now do same name but pull. This should be an error.
		_, err = js.AddConsumer("X", &nats.ConsumerConfig{Durable: "dlc"})
		require_Error(t, err)
	}

	t.Run("Single", func(t *testing.T) { testUpdate(t, s) })
	t.Run("Clustered", func(t *testing.T) { testUpdate(t, c.randomServer()) })
}

func TestJetStreamClusterAddConsumerWithInfo(t *testing.T) {
	s := RunBasicJetStreamServer()
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	testConsInfo := func(t *testing.T, s *Server) {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
		})
		require_NoError(t, err)

		for i := 0; i < 10; i++ {
			_, err = js.Publish("foo", []byte("msg"))
			require_NoError(t, err)
		}

		for i := 0; i < 100; i++ {
			inbox := nats.NewInbox()
			sub := natsSubSync(t, nc, inbox)

			ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
				DeliverSubject: inbox,
				DeliverPolicy:  nats.DeliverAllPolicy,
				FilterSubject:  "foo",
				AckPolicy:      nats.AckExplicitPolicy,
			})
			require_NoError(t, err)

			if ci.NumPending != 10 {
				t.Fatalf("Iter=%v - expected 10 messages pending on create, got %v", i+1, ci.NumPending)
			}
			js.DeleteConsumer("TEST", ci.Name)
			sub.Unsubscribe()
		}
	}

	t.Run("Single", func(t *testing.T) { testConsInfo(t, s) })
	t.Run("Clustered", func(t *testing.T) { testConsInfo(t, c.randomServer()) })
}

func TestJetStreamClusterStreamReplicaUpdates(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R7S", 7)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Start out at R1
	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	numMsgs := 1000
	for i := 0; i < numMsgs; i++ {
		js.PublishAsync("foo", []byte("HELLO WORLD"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	updateReplicas := func(r int) {
		t.Helper()
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		leader := si.Cluster.Leader

		cfg.Replicas = r
		_, err = js.UpdateStream(cfg)
		require_NoError(t, err)
		c.waitOnStreamLeader("$G", "TEST")

		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			si, err = js.StreamInfo("TEST")
			require_NoError(t, err)
			if len(si.Cluster.Replicas) != r-1 {
				return fmt.Errorf("Expected %d replicas, got %d", r-1, len(si.Cluster.Replicas))
			}
			return nil
		})

		// Make sure we kept same leader.
		if si.Cluster.Leader != leader {
			t.Fatalf("Leader changed, expected %q got %q", leader, si.Cluster.Leader)
		}
		// Make sure all are current.
		for _, r := range si.Cluster.Replicas {
			c.waitOnStreamCurrent(c.serverByName(r.Name), "$G", "TEST")
		}
		// Check msgs.
		if si.State.Msgs != uint64(numMsgs) {
			t.Fatalf("Expected %d msgs, got %d", numMsgs, si.State.Msgs)
		}
		// Make sure we have the right number of HA Assets running on the leader.
		s := c.serverByName(leader)
		jsi, err := s.Jsz(nil)
		require_NoError(t, err)
		nha := 1 // meta always present.
		if len(si.Cluster.Replicas) > 0 {
			nha++
		}
		if nha != jsi.HAAssets {
			t.Fatalf("Expected %d HA asset(s), but got %d", nha, jsi.HAAssets)
		}
	}

	// Update from 1-3
	updateReplicas(3)
	// Update from 3-5
	updateReplicas(5)
	// Update from 5-3
	updateReplicas(3)
	// Update from 3-1
	updateReplicas(1)
}

func TestJetStreamClusterStreamAndConsumerScaleUpAndDown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Start out at R3
	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	sub, err := js.SubscribeSync("foo", nats.Durable("cat"))
	require_NoError(t, err)

	numMsgs := 10
	for i := 0; i < numMsgs; i++ {
		_, err := js.Publish("foo", []byte("HELLO WORLD"))
		require_NoError(t, err)
	}
	checkSubsPending(t, sub, numMsgs)

	// Now ask leader to stepdown.
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, time.Second)
	require_NoError(t, err)

	var sdResp JSApiStreamLeaderStepDownResponse
	err = json.Unmarshal(rmsg.Data, &sdResp)
	require_NoError(t, err)

	if sdResp.Error != nil || !sdResp.Success {
		t.Fatalf("Unexpected error: %+v", sdResp.Error)
	}

	c.waitOnStreamLeader("$G", "TEST")

	updateReplicas := func(r int) {
		t.Helper()
		cfg.Replicas = r
		_, err := js.UpdateStream(cfg)
		require_NoError(t, err)
		c.waitOnStreamLeader("$G", "TEST")
		c.waitOnConsumerLeader("$G", "TEST", "cat")
		ci, err := js.ConsumerInfo("TEST", "cat")
		require_NoError(t, err)
		if ci.Cluster.Leader == _EMPTY_ {
			t.Fatalf("Expected a consumer leader but got none in consumer info")
		}
		if len(ci.Cluster.Replicas)+1 != r {
			t.Fatalf("Expected consumer info to have %d peers, got %d", r, len(ci.Cluster.Replicas)+1)
		}
	}

	// Capture leader, we want to make sure when we scale down this does not change.
	sl := c.streamLeader("$G", "TEST")

	// Scale down to 1.
	updateReplicas(1)

	if sl != c.streamLeader("$G", "TEST") {
		t.Fatalf("Expected same leader, but it changed")
	}

	// Make sure we can still send to the stream.
	for i := 0; i < numMsgs; i++ {
		_, err := js.Publish("foo", []byte("HELLO WORLD"))
		require_NoError(t, err)
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	if si.State.Msgs != uint64(2*numMsgs) {
		t.Fatalf("Expected %d msgs, got %d", 3*numMsgs, si.State.Msgs)
	}

	checkSubsPending(t, sub, 2*numMsgs)

	// Now back up.
	updateReplicas(3)

	// Send more.
	for i := 0; i < numMsgs; i++ {
		_, err := js.Publish("foo", []byte("HELLO WORLD"))
		require_NoError(t, err)
	}

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	if si.State.Msgs != uint64(3*numMsgs) {
		t.Fatalf("Expected %d msgs, got %d", 3*numMsgs, si.State.Msgs)
	}

	checkSubsPending(t, sub, 3*numMsgs)

	// Make sure cluster replicas are current.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		si, err = js.StreamInfo("TEST")
		require_NoError(t, err)
		for _, r := range si.Cluster.Replicas {
			if !r.Current {
				return fmt.Errorf("Expected replica to be current: %+v", r)
			}
		}
		return nil
	})

	checkState := func(s *Server) {
		t.Helper()
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			mset, err := s.GlobalAccount().lookupStream("TEST")
			require_NoError(t, err)
			state := mset.state()
			if state.Msgs != uint64(3*numMsgs) || state.FirstSeq != 1 || state.LastSeq != 30 || state.Bytes != 1320 {
				return fmt.Errorf("Wrong state: %+v for server: %v", state, s)
			}
			return nil
		})
	}

	// Now check each indidvidual stream on each server to make sure replication occurred.
	for _, s := range c.servers {
		checkState(s)
	}
}

func TestJetStreamClusterStreamTagPlacement(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()

	placeOK := func(connectCluster string, tags []string, expectedCluster string) {
		t.Helper()
		nc, js := jsClientConnect(t, sc.clusterForName(connectCluster).randomServer())
		defer nc.Close()
		si, err := js.AddStream(&nats.StreamConfig{
			Name:      "TEST",
			Subjects:  []string{"foo"},
			Placement: &nats.Placement{Tags: tags},
		})
		require_NoError(t, err)
		if si.Cluster.Name != expectedCluster {
			t.Fatalf("Failed to place properly in %q, got %q", expectedCluster, si.Cluster.Name)
		}
		js.DeleteStream("TEST")
	}

	placeOK("C2", []string{"cloud:aws"}, "C1")
	placeOK("C2", []string{"country:jp"}, "C3")
	placeOK("C1", []string{"cloud:gcp", "country:uk"}, "C2")

	// Case shoud not matter.
	placeOK("C1", []string{"cloud:GCP", "country:UK"}, "C2")
	placeOK("C2", []string{"Cloud:AwS", "Country:uS"}, "C1")

	placeErr := func(connectCluster string, tags []string) {
		t.Helper()
		nc, js := jsClientConnect(t, sc.clusterForName(connectCluster).randomServer())
		defer nc.Close()
		_, err := js.AddStream(&nats.StreamConfig{
			Name:      "TEST",
			Subjects:  []string{"foo"},
			Placement: &nats.Placement{Tags: tags},
		})
		require_Error(t, err, NewJSInsufficientResourcesError())
	}

	placeErr("C1", []string{"cloud:GCP", "country:US"})
	placeErr("C1", []string{"country:DN"})
	placeErr("C1", []string{"cloud:DO"})
}

func TestJetStreamClusterInterestRetentionWithFilteredConsumersExtra(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	subjectNameZero := "foo.bar"
	subjectNameOne := "foo.baz"

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo.*"}, Retention: nats.InterestPolicy, Replicas: 3})
	require_NoError(t, err)

	checkState := func(expected uint64) {
		t.Helper()
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			if si.State.Msgs != expected {
				return fmt.Errorf("Expected %d msgs, got %d", expected, si.State.Msgs)
			}
			return nil
		})
	}

	subZero, err := js.PullSubscribe(subjectNameZero, "dlc-0")
	require_NoError(t, err)

	subOne, err := js.PullSubscribe(subjectNameOne, "dlc-1")
	require_NoError(t, err)

	msg := []byte("FILTERED")
	// Now send a bunch of messages
	for i := 0; i < 1000; i++ {
		_, err = js.PublishAsync(subjectNameZero, msg)
		require_NoError(t, err)
		_, err = js.PublishAsync(subjectNameOne, msg)
		require_NoError(t, err)
	}

	// should be 2000 in total
	checkState(2000)

	// fetch and acknowledge, count records to ensure no errors acknowledging
	getAndAckBatch := func(sub *nats.Subscription) {
		t.Helper()
		successCounter := 0
		msgs, err := sub.Fetch(1000)
		require_NoError(t, err)

		for _, m := range msgs {
			err = m.AckSync()
			require_NoError(t, err)
			successCounter++
		}
		if successCounter != 1000 {
			t.Fatalf("Unexpected number of acknowledges %d for subscription %v", successCounter, sub)
		}
	}

	// fetch records subscription zero
	getAndAckBatch(subZero)
	// fetch records for subscription one
	getAndAckBatch(subOne)
	// Make sure stream is zero.
	checkState(0)
}

func TestJetStreamClusterStreamConsumersCount(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	sname := "TEST_STREAM_CONS_COUNT"
	_, err := js.AddStream(&nats.StreamConfig{Name: sname, Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)

	// Create some R1 consumers
	for i := 0; i < 10; i++ {
		inbox := nats.NewInbox()
		natsSubSync(t, nc, inbox)
		_, err = js.AddConsumer(sname, &nats.ConsumerConfig{DeliverSubject: inbox})
		require_NoError(t, err)
	}

	// Now check that the consumer count in stream info/list is 10
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		// Check stream info
		si, err := js.StreamInfo(sname)
		if err != nil {
			return fmt.Errorf("Error getting stream info: %v", err)
		}
		if n := si.State.Consumers; n != 10 {
			return fmt.Errorf("From StreamInfo, expecting 10 consumers, got %v", n)
		}

		// Now from stream list
		for si := range js.StreamsInfo() {
			if n := si.State.Consumers; n != 10 {
				return fmt.Errorf("From StreamsInfo, expecting 10 consumers, got %v", n)
			}
		}
		return nil
	})
}

func TestJetStreamClusterFilteredAndIdleConsumerNRGGrowth(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	sname := "TEST"
	_, err := js.AddStream(&nats.StreamConfig{Name: sname, Subjects: []string{"foo.*"}, Replicas: 3})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("foo.baz", nats.Durable("dlc"))
	require_NoError(t, err)

	for i := 0; i < 10_000; i++ {
		js.PublishAsync("foo.bar", []byte("ok"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkSubsPending(t, sub, 0)

	// Grab consumer's underlying info and make sure NRG log not running away do to no-op skips on filtered consumer.
	// Need a non-leader for the consumer, they are only ones getting skip ops to keep delivered updated.
	cl := c.consumerLeader("$G", "TEST", "dlc")
	var s *Server
	for _, s = range c.servers {
		if s != cl {
			break
		}
	}

	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("dlc")
	if o == nil {
		t.Fatalf("Error looking up consumer %q", "dlc")
	}

	// compactNumMin from monitorConsumer is 8192 atm.
	const compactNumMin = 8192
	if entries, _ := o.raftNode().Size(); entries > compactNumMin {
		t.Fatalf("Expected <= %d entries, got %d", compactNumMin, entries)
	}

	// Now make the consumer leader stepdown and make sure we have the proper snapshot.
	resp, err := nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "dlc"), nil, time.Second)
	require_NoError(t, err)

	var cdResp JSApiConsumerLeaderStepDownResponse
	if err := json.Unmarshal(resp.Data, &cdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if cdResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", cdResp.Error)
	}

	c.waitOnConsumerLeader("$G", "TEST", "dlc")
}

func TestJetStreamClusterMirrorOrSourceNotActiveReporting(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)

	si, err := js.AddStream(&nats.StreamConfig{
		Name:   "M",
		Mirror: &nats.StreamSource{Name: "TEST"},
	})
	require_NoError(t, err)

	// We would previous calculate a large number if we actually never heard from the peer yet.
	// We want to make sure if we have never heard from the other side report -1 as Active.
	// It is possible if testing infra is slow that this could be legit, but should be pretty small.
	if si.Mirror.Active != -1 && si.Mirror.Active > 10*time.Millisecond {
		t.Fatalf("Expected an Active of -1, but got %v", si.Mirror.Active)
	}
}

func TestJetStreamStreamAdvisories(t *testing.T) {
	s := RunBasicJetStreamServer()
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	checkAdv := func(t *testing.T, sub *nats.Subscription, expectedPrefixes ...string) {
		t.Helper()
		seen := make([]bool, len(expectedPrefixes))
		for i := 0; i < len(expectedPrefixes); i++ {
			msg := natsNexMsg(t, sub, time.Second)
			var gotOne bool
			for j, pfx := range expectedPrefixes {
				if !seen[j] && strings.HasPrefix(msg.Subject, pfx) {
					seen[j] = true
					gotOne = true
					break
				}
			}
			if !gotOne {
				t.Fatalf("Expected one of prefixes %q, got %q", expectedPrefixes, msg.Subject)
			}
		}
	}

	// Used to keep stream names pseudo unique. t.Name() has slashes in it which caused problems.
	var testN int

	checkAdvisories := func(t *testing.T, s *Server, replicas int) {

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		testN++
		streamName := "TEST_ADVISORIES_" + fmt.Sprintf("%d", testN)

		sub := natsSubSync(t, nc, "$JS.EVENT.ADVISORY.STREAM.*."+streamName)

		si, err := js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Storage:  nats.FileStorage,
			Replicas: replicas,
		})
		require_NoError(t, err)
		advisories := []string{JSAdvisoryStreamCreatedPre}
		if replicas > 1 {
			advisories = append(advisories, JSAdvisoryStreamLeaderElectedPre)
		}
		checkAdv(t, sub, advisories...)

		si.Config.MaxMsgs = 1000
		_, err = js.UpdateStream(&si.Config)
		require_NoError(t, err)
		checkAdv(t, sub, JSAdvisoryStreamUpdatedPre)

		snapreq := &JSApiStreamSnapshotRequest{
			DeliverSubject: nats.NewInbox(),
			ChunkSize:      512,
		}
		var snapshot []byte
		done := make(chan bool)
		nc.Subscribe(snapreq.DeliverSubject, func(m *nats.Msg) {
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

		req, _ := json.Marshal(snapreq)
		rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, streamName), req, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error on snapshot request: %v", err)
		}

		var snapresp JSApiStreamSnapshotResponse
		json.Unmarshal(rmsg.Data, &snapresp)
		if snapresp.Error != nil {
			t.Fatalf("Did not get correct error response: %+v", snapresp.Error)
		}

		// Wait to receive the snapshot.
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive our snapshot in time")
		}

		checkAdv(t, sub, JSAdvisoryStreamSnapshotCreatePre)
		checkAdv(t, sub, JSAdvisoryStreamSnapshotCompletePre)

		err = js.DeleteStream(streamName)
		require_NoError(t, err)
		checkAdv(t, sub, JSAdvisoryStreamDeletedPre)

		state := *snapresp.State
		config := *snapresp.Config
		resreq := &JSApiStreamRestoreRequest{
			Config: config,
			State:  state,
		}
		req, _ = json.Marshal(resreq)
		rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, streamName), req, 5*time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var resresp JSApiStreamRestoreResponse
		json.Unmarshal(rmsg.Data, &resresp)
		if resresp.Error != nil {
			t.Fatalf("Got an unexpected error response: %+v", resresp.Error)
		}

		// Send our snapshot back in to restore the stream.
		// Can be any size message.
		var chunk [1024]byte
		for r := bytes.NewReader(snapshot); ; {
			n, err := r.Read(chunk[:])
			if err != nil {
				break
			}
			nc.Request(resresp.DeliverSubject, chunk[:n], time.Second)
		}
		rmsg, err = nc.Request(resresp.DeliverSubject, nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		resresp.Error = nil
		json.Unmarshal(rmsg.Data, &resresp)
		if resresp.Error != nil {
			t.Fatalf("Got an unexpected error response: %+v", resresp.Error)
		}

		checkAdv(t, sub, JSAdvisoryStreamRestoreCreatePre)
		// At this point, the stream_created advisory may be sent before
		// or after the restore_complete advisory because they are sent
		// using different "send queues". That is, the restore uses the
		// server's event queue while the stream_created is sent from
		// the stream's own send queue.
		advisories = append(advisories, JSAdvisoryStreamRestoreCompletePre)
		checkAdv(t, sub, advisories...)
	}

	t.Run("Single", func(t *testing.T) { checkAdvisories(t, s, 1) })
	t.Run("Clustered_R1", func(t *testing.T) { checkAdvisories(t, c.randomServer(), 1) })
	t.Run("Clustered_R3", func(t *testing.T) { checkAdvisories(t, c.randomServer(), 3) })
}

func TestJetStreamRemovedPeersAndStreamsListAndDelete(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	pcn := "C2"
	sc.waitOnLeader()
	ml := sc.leader()
	if ml.ClusterName() == pcn {
		pcn = "C1"
	}

	// Client based API
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "GONE",
		Replicas:  3,
		Placement: &nats.Placement{Cluster: pcn},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("GONE", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Replicas:  3,
		Placement: &nats.Placement{Cluster: ml.ClusterName()},
	})
	require_NoError(t, err)

	// Put messages in..
	num := 100
	for i := 0; i < num; i++ {
		js.PublishAsync("GONE", []byte("SLS"))
		js.PublishAsync("TEST", []byte("SLS"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	c := sc.clusterForName(pcn)
	c.shutdown()

	// Grab Stream List..
	start := time.Now()
	resp, err := nc.Request(JSApiStreamList, nil, 2*time.Second)
	require_NoError(t, err)
	if delta := time.Since(start); delta > 100*time.Millisecond {
		t.Fatalf("Stream list call took too long to return: %v", delta)
	}
	var list JSApiStreamListResponse
	err = json.Unmarshal(resp.Data, &list)
	require_NoError(t, err)

	if len(list.Missing) != 1 || list.Missing[0] != "GONE" {
		t.Fatalf("Wrong Missing: %+v", list)
	}

	// Check behavior of stream info as well. We want it to return the stream is offline and not just timeout.
	_, err = js.StreamInfo("GONE")
	// FIXME(dlc) - Go client not putting nats: prefix on for stream but does for consumer.
	require_Error(t, err, NewJSStreamOfflineError(), errors.New("nats: stream is offline"))

	// Same for Consumer
	start = time.Now()
	resp, err = nc.Request("$JS.API.CONSUMER.LIST.GONE", nil, 2*time.Second)
	require_NoError(t, err)
	if delta := time.Since(start); delta > 100*time.Millisecond {
		t.Fatalf("Consumer list call took too long to return: %v", delta)
	}
	var clist JSApiConsumerListResponse
	err = json.Unmarshal(resp.Data, &clist)
	require_NoError(t, err)

	if len(clist.Missing) != 1 || clist.Missing[0] != "dlc" {
		t.Fatalf("Wrong Missing: %+v", clist)
	}

	_, err = js.ConsumerInfo("GONE", "dlc")
	require_Error(t, err, NewJSConsumerOfflineError(), errors.New("nats: consumer is offline"))

	// Make sure delete works.
	err = js.DeleteConsumer("GONE", "dlc")
	require_NoError(t, err)

	err = js.DeleteStream("GONE")
	require_NoError(t, err)

	// Test it is really gone.
	_, err = js.StreamInfo("GONE")
	require_Error(t, err, nats.ErrStreamNotFound)
}

func TestJetStreamConsumerDeliverNewBug(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	pcn := "C2"
	sc.waitOnLeader()
	ml := sc.leader()
	if ml.ClusterName() == pcn {
		pcn = "C1"
	}

	// Client based API
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "T",
		Replicas:  3,
		Placement: &nats.Placement{Cluster: pcn},
	})
	require_NoError(t, err)

	// Put messages in..
	num := 200
	for i := 0; i < num; i++ {
		js.PublishAsync("T", []byte("OK"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	ci, err := js.AddConsumer("T", &nats.ConsumerConfig{
		Durable:       "d",
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverNewPolicy,
	})
	require_NoError(t, err)

	if ci.Delivered.Consumer != 0 || ci.Delivered.Stream != 200 {
		t.Fatalf("Incorrect consumer delivered info: %+v", ci.Delivered)
	}

	c := sc.clusterForName(pcn)
	for _, s := range c.servers {
		sd := s.JetStreamConfig().StoreDir
		s.Shutdown()
		removeDir(t, sd)
		s = c.restartServer(s)
		c.waitOnServerHealthz(s)
	}

	c.waitOnConsumerLeader("$G", "T", "d")
	ci, err = js.ConsumerInfo("T", "d")
	require_NoError(t, err)

	if ci.Delivered.Consumer != 0 || ci.Delivered.Stream != 200 {
		t.Fatalf("Incorrect consumer delivered info: %+v", ci.Delivered)
	}
	if ci.NumPending != 0 {
		t.Fatalf("Did not expect NumPending, got %d", ci.NumPending)
	}
}

// If the config files have duplicate routes this can have the metagroup estimate a size for the system
// which prevents reaching quorum and electing a meta-leader.
func TestJetStreamClusterDuplicateRoutesDisruptJetStreamMetaGroup(t *testing.T) {
	tmpl := `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
		name: RR
		listen: 127.0.0.1:%d
		routes = [
			nats-route://127.0.0.1:%d
			nats-route://127.0.0.1:%d
			nats-route://127.0.0.1:%d
			# These will be dupes
			nats-route://127.0.0.1:%d
			nats-route://127.0.0.1:%d
			nats-route://127.0.0.1:%d
		]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

	c := &cluster{servers: make([]*Server, 0, 3), opts: make([]*Options, 0, 3), name: "RR", t: t}

	rports := []int{22208, 22209, 22210}
	for i, p := range rports {
		sname, sd := fmt.Sprintf("S%d", i+1), createDir(t, JetStreamStoreDir)
		cf := fmt.Sprintf(tmpl, sname, sd, p, rports[0], rports[1], rports[2], rports[0], rports[1], rports[2])
		s, o := RunServerWithConfig(createConfFile(t, []byte(cf)))
		c.servers, c.opts = append(c.servers, s), append(c.opts, o)
	}
	defer c.shutdown()

	checkClusterFormed(t, c.servers...)
	c.waitOnClusterReady()
}

func TestJetStreamClusterDuplicateMsgIdsOnCatchupAndLeaderTakeover(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 3,
	})
	require_NoError(t, err)

	// Shutdown a non-leader.
	nc.Close()
	sr := c.randomNonStreamLeader("$G", "TEST")
	sr.Shutdown()

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	m := nats.NewMsg("TEST")
	m.Data = []byte("OK")

	n := 10
	for i := 0; i < n; i++ {
		m.Header.Set(JSMsgId, strconv.Itoa(i))
		_, err := js.PublishMsg(m)
		require_NoError(t, err)
	}

	m.Header.Set(JSMsgId, "8")
	pa, err := js.PublishMsg(m)
	require_NoError(t, err)
	if !pa.Duplicate {
		t.Fatalf("Expected msg to be a duplicate")
	}

	// Now force a snapshot, want to test catchup above RAFT layer.
	sl := c.streamLeader("$G", "TEST")
	mset, err := sl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	if node := mset.raftNode(); node == nil {
		t.Fatalf("Could not get stream group name")
	} else if err := node.InstallSnapshot(mset.stateSnapshot()); err != nil {
		t.Fatalf("Error installing snapshot: %v", err)
	}

	// Now restart
	sr = c.restartServer(sr)
	c.waitOnStreamCurrent(sr, "$G", "TEST")

	// Now make them the leader.
	for sr != c.streamLeader("$G", "TEST") {
		_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, time.Second)
		require_NoError(t, err)
		c.waitOnStreamLeader("$G", "TEST")
	}

	// Make sure this gets rejected.
	pa, err = js.PublishMsg(m)
	require_NoError(t, err)
	if !pa.Duplicate {
		t.Fatalf("Expected msg to be a duplicate")
	}
}

func TestJetStreamClusterConsumerLeaderChangeDeadlock(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create a stream and durable with ack explicit
	_, err := js.AddStream(&nats.StreamConfig{Name: "test", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	_, err = js.AddConsumer("test", &nats.ConsumerConfig{
		Durable:        "test",
		DeliverSubject: "bar",
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        250 * time.Millisecond,
	})
	require_NoError(t, err)

	// Wait for a leader
	c.waitOnConsumerLeader("$G", "test", "test")
	cl := c.consumerLeader("$G", "test", "test")

	// Publish a message
	_, err = js.Publish("foo", []byte("msg"))
	require_NoError(t, err)

	// Create nats consumer on "bar" and don't ack it
	sub := natsSubSync(t, nc, "bar")
	natsNexMsg(t, sub, time.Second)
	// Wait for redeliveries, to make sure it is in the redelivery map
	natsNexMsg(t, sub, time.Second)
	natsNexMsg(t, sub, time.Second)

	mset, err := cl.GlobalAccount().lookupStream("test")
	require_NoError(t, err)
	require_True(t, mset != nil)

	// There are parts in the code (for instance when signaling to consumers
	// that there are new messages) where we get the mset lock and iterate
	// over the consumers and get consumer lock. We are going to do that
	// in a go routine while we send a consumer step down request from
	// another go routine. We will watch for possible deadlock and if
	// found report it.
	ch := make(chan struct{})
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		for {
			mset.mu.Lock()
			for _, o := range mset.consumers {
				o.mu.Lock()
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
				o.mu.Unlock()
			}
			mset.mu.Unlock()
			select {
			case <-ch:
				return
			default:
			}
		}
	}()

	// Now cause a leader changes
	for i := 0; i < 5; i++ {
		m, err := nc.Request("$JS.API.CONSUMER.LEADER.STEPDOWN.test.test", nil, 2*time.Second)
		// Ignore error here and check for deadlock below
		if err != nil {
			break
		}
		// if there is a message, check that it is success
		var resp JSApiConsumerLeaderStepDownResponse
		err = json.Unmarshal(m.Data, &resp)
		require_NoError(t, err)
		require_True(t, resp.Success)
		c.waitOnConsumerLeader("$G", "test", "test")
	}

	close(ch)
	select {
	case <-doneCh:
		// OK!
	case <-time.After(2 * time.Second):
		buf := make([]byte, 1000000)
		n := runtime.Stack(buf, true)
		t.Fatalf("Suspected deadlock, printing current stack. The test suite may timeout and will also dump the stack\n%s\n", buf[:n])
	}
}

// We were compacting to keep the raft log manageable but not snapshotting, which meant that restarted
// servers could complain about no snapshot and could not sync after that condition.
// Changes also address https://github.com/nats-io/nats-server/issues/2936
func TestJetStreamClusterMemoryConsumerCompactVsSnapshot(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create a stream and durable with ack explicit
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "test",
		Storage:  nats.MemoryStorage,
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("test", &nats.ConsumerConfig{
		Durable:   "d",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// Bring a non-leader down.
	s := c.randomNonConsumerLeader("$G", "test", "d")
	s.Shutdown()

	// In case that was also mete or stream leader.
	c.waitOnLeader()
	c.waitOnStreamLeader("$G", "test")
	// In case we were connected there.
	nc.Close()
	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Generate some state.
	for i := 0; i < 2000; i++ {
		_, err := js.PublishAsync("test", nil)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	sub, err := js.PullSubscribe("test", "d")
	require_NoError(t, err)

	for i := 0; i < 2; i++ {
		for _, m := range fetchMsgs(t, sub, 1000, 5*time.Second) {
			m.AckSync()
		}
	}

	// Restart our downed server.
	s = c.restartServer(s)
	c.checkClusterFormed()
	c.waitOnServerCurrent(s)

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("test", "d")
		require_NoError(t, err)
		for _, r := range ci.Cluster.Replicas {
			if !r.Current || r.Lag != 0 {
				return fmt.Errorf("Replica not current: %+v", r)
			}
		}
		return nil
	})
}

// This will test our ability to move streams and consumers between clusters.
func TestJetStreamClusterMovingStreamsAndConsumers(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()

	nc, js := jsClientConnect(t, sc.randomServer())
	defer nc.Close()

	for _, test := range []struct {
		name     string
		replicas int
	}{
		{"R1", 1},
		{"R3", 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			replicas := test.replicas

			si, err := js.AddStream(&nats.StreamConfig{
				Name:      "MOVE",
				Replicas:  replicas,
				Placement: &nats.Placement{Tags: []string{"cloud:aws"}},
			})
			require_NoError(t, err)
			defer js.DeleteStream("MOVE")

			if si.Cluster.Name != "C1" {
				t.Fatalf("Failed to place properly in %q, got %q", "C1", si.Cluster.Name)
			}

			for i := 0; i < 1000; i++ {
				_, err := js.PublishAsync("MOVE", []byte("Moving on up"))
				require_NoError(t, err)
			}
			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}

			// Durable Push Consumer, so same R.
			dpushSub, err := js.SubscribeSync("MOVE", nats.Durable("dlc"))
			require_NoError(t, err)
			defer dpushSub.Unsubscribe()

			// Ephemeral Push Consumer, R1.
			epushSub, err := js.SubscribeSync("MOVE")
			require_NoError(t, err)
			defer epushSub.Unsubscribe()

			// Durable Pull Consumer, so same R.
			dpullSub, err := js.PullSubscribe("MOVE", "dlc-pull")
			require_NoError(t, err)
			defer dpullSub.Unsubscribe()

			// TODO(dlc) - Server supports ephemeral pulls but Go client does not yet.

			si, err = js.StreamInfo("MOVE")
			require_NoError(t, err)
			if si.State.Consumers != 3 {
				t.Fatalf("Expected 3 attached consumers, got %d", si.State.Consumers)
			}

			initialState := si.State

			checkSubsPending(t, dpushSub, int(initialState.Msgs))
			checkSubsPending(t, epushSub, int(initialState.Msgs))

			// Ack 100
			toAck := 100
			for i := 0; i < toAck; i++ {
				m, err := dpushSub.NextMsg(time.Second)
				require_NoError(t, err)
				m.AckSync()
				// Ephemeral
				m, err = epushSub.NextMsg(time.Second)
				require_NoError(t, err)
				m.AckSync()
			}

			// Do same with pull subscriber.
			for _, m := range fetchMsgs(t, dpullSub, toAck, 5*time.Second) {
				m.AckSync()
			}

			// First make sure we disallow move and replica changes in same update.
			_, err = js.UpdateStream(&nats.StreamConfig{
				Name:      "MOVE",
				Placement: &nats.Placement{Tags: []string{"cloud:gcp"}},
				Replicas:  replicas + 1,
			})
			require_Error(t, err, NewJSStreamMoveAndScaleError())

			// Now move to new cluster.
			si, err = js.UpdateStream(&nats.StreamConfig{
				Name:      "MOVE",
				Replicas:  replicas,
				Placement: &nats.Placement{Tags: []string{"cloud:gcp"}},
			})
			require_NoError(t, err)

			if si.Cluster.Name != "C1" {
				t.Fatalf("Expected cluster of %q but got %q", "C1", si.Cluster.Name)
			}

			// Make sure we can not move an inflight stream and consumers, should error.
			_, err = js.UpdateStream(&nats.StreamConfig{
				Name:      "MOVE",
				Replicas:  replicas,
				Placement: &nats.Placement{Tags: []string{"cloud:aws"}},
			})
			require_Error(t, err, NewJSStreamMoveInProgressError())

			checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
				si, err := js.StreamInfo("MOVE")
				if err != nil {
					return err
				}
				// We should see 2X peers.
				numPeers := len(si.Cluster.Replicas)
				if si.Cluster.Leader != _EMPTY_ {
					numPeers++
				}
				if numPeers != 2*replicas {
					return fmt.Errorf("Expected to see %d replicas, got %d", 2*replicas, numPeers)
				}
				return nil
			})

			// Expect a new leader to emerge and replicas to drop as a leader is elected.
			// We have to check fast or it might complete and we will not see intermediate steps.
			sc.waitOnStreamLeader("$G", "MOVE")
			checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
				si, err := js.StreamInfo("MOVE")
				if err != nil {
					return err
				}
				if len(si.Cluster.Replicas) >= 2*replicas {
					return fmt.Errorf("Expected <%d replicas, got %d", 2*replicas, len(si.Cluster.Replicas))
				}
				return nil
			})

			// Should see the cluster designation and leader switch to C2.
			// We should also shrink back down to original replica count.
			checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
				si, err := js.StreamInfo("MOVE")
				if err != nil {
					return err
				}
				if si.Cluster.Name != "C2" {
					return fmt.Errorf("Wrong cluster: %q", si.Cluster.Name)
				}
				if si.Cluster.Leader == _EMPTY_ {
					return fmt.Errorf("No leader yet")
				} else if !strings.HasPrefix(si.Cluster.Leader, "C2-") {
					return fmt.Errorf("Wrong leader: %q", si.Cluster.Leader)
				}
				// Now we want to see that we shrink back to original.
				if len(si.Cluster.Replicas) != replicas-1 {
					return fmt.Errorf("Expected %d replicas, got %d", replicas-1, len(si.Cluster.Replicas))
				}
				return nil
			})

			// Check moved state is same as initial state.
			si, err = js.StreamInfo("MOVE")
			require_NoError(t, err)

			if si.State != initialState {
				t.Fatalf("States do not match after migration:\n%+v\nvs\n%+v", si.State, initialState)
			}

			// Make sure we can still send messages.
			addN := toAck
			for i := 0; i < addN; i++ {
				_, err := js.Publish("MOVE", []byte("Done Moved"))
				require_NoError(t, err)
			}

			si, err = js.StreamInfo("MOVE")
			require_NoError(t, err)

			expectedPushMsgs := initialState.Msgs + uint64(addN)
			expectedPullMsgs := uint64(addN)

			if si.State.Msgs != expectedPushMsgs {
				t.Fatalf("Expected to be able to send new messages")
			}

			// Now check consumers, make sure the state is correct and that they transferred state and reflect the new messages.
			// We Ack'd 100 and sent another 100, so should be same.
			checkConsumer := func(sub *nats.Subscription, isPull bool) {
				t.Helper()
				checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
					ci, err := sub.ConsumerInfo()
					if err != nil {
						return err
					}
					var expectedMsgs uint64
					if isPull {
						expectedMsgs = expectedPullMsgs
					} else {
						expectedMsgs = expectedPushMsgs
					}

					if ci.Delivered.Consumer != expectedMsgs || ci.Delivered.Stream != expectedMsgs {
						return fmt.Errorf("Delivered for %q is not correct: %+v", ci.Name, ci.Delivered)
					}
					if ci.AckFloor.Consumer != uint64(toAck) || ci.AckFloor.Stream != uint64(toAck) {
						return fmt.Errorf("AckFloor for %q is not correct: %+v", ci.Name, ci.AckFloor)
					}
					if isPull && ci.NumAckPending != 0 {
						return fmt.Errorf("NumAckPending for %q is not correct: %v", ci.Name, ci.NumAckPending)
					} else if !isPull && ci.NumAckPending != int(initialState.Msgs) {
						return fmt.Errorf("NumAckPending for %q is not correct: %v", ci.Name, ci.NumAckPending)
					}
					// Make sure the replicas etc are back to what is expected.
					si, err := js.StreamInfo("MOVE")
					if err != nil {
						return err
					}
					numExpected := si.Config.Replicas
					if ci.Config.Durable == _EMPTY_ {
						numExpected = 1
					}
					numPeers := len(ci.Cluster.Replicas)
					if ci.Cluster.Leader != _EMPTY_ {
						numPeers++
					}
					if numPeers != numExpected {
						return fmt.Errorf("Expected %d peers, got %d", numExpected, numPeers)
					}
					// If we are push check sub pending.
					if !isPull {
						checkSubsPending(t, sub, int(expectedPushMsgs)-toAck)
					}
					return nil
				})
			}

			checkPushConsumer := func(sub *nats.Subscription) {
				t.Helper()
				checkConsumer(sub, false)
			}
			checkPullConsumer := func(sub *nats.Subscription) {
				t.Helper()
				checkConsumer(sub, true)
			}

			checkPushConsumer(dpushSub)
			checkPushConsumer(epushSub)
			checkPullConsumer(dpullSub)

			// Cleanup
			err = js.DeleteStream("MOVE")
			require_NoError(t, err)
		})
	}
}

func TestJetStreamClusterMovingStreamsWithMirror(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()

	nc, js := jsClientConnect(t, sc.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "SOURCE",
		Subjects:  []string{"foo", "bar"},
		Replicas:  3,
		Placement: &nats.Placement{Tags: []string{"cloud:aws"}},
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:      "MIRROR",
		Replicas:  1,
		Mirror:    &nats.StreamSource{Name: "SOURCE"},
		Placement: &nats.Placement{Tags: []string{"cloud:gcp"}},
	})
	require_NoError(t, err)

	done := make(chan struct{})
	exited := make(chan struct{})
	errors := make(chan error, 1)

	numNoResp := uint64(0)

	// We will run a separate routine and send at 100hz
	go func() {
		nc, js := jsClientConnect(t, sc.randomServer())
		defer nc.Close()

		defer close(exited)

		for {
			select {
			case <-done:
				return
			case <-time.After(10 * time.Millisecond):
				_, err := js.Publish("foo", []byte("100HZ"))
				if err == nil {
				} else if err == nats.ErrNoStreamResponse {
					atomic.AddUint64(&numNoResp, 1)
					continue
				}
				if err != nil {
					errors <- err
					return
				}
			}
		}
	}()

	// Let it get going.
	time.Sleep(500 * time.Millisecond)

	// Now move the source to a new cluster.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "SOURCE",
		Subjects:  []string{"foo", "bar"},
		Replicas:  3,
		Placement: &nats.Placement{Tags: []string{"cloud:gcp"}},
	})
	require_NoError(t, err)

	checkFor(t, 30*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("SOURCE")
		if err != nil {
			return err
		}
		if si.Cluster.Name != "C2" {
			return fmt.Errorf("Wrong cluster: %q", si.Cluster.Name)
		}
		if si.Cluster.Leader == _EMPTY_ {
			return fmt.Errorf("No leader yet")
		} else if !strings.HasPrefix(si.Cluster.Leader, "C2-") {
			return fmt.Errorf("Wrong leader: %q", si.Cluster.Leader)
		}
		// Now we want to see that we shrink back to original.
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected %d replicas, got %d", 2, len(si.Cluster.Replicas))
		}
		// Let's get to 50+ msgs.
		if si.State.Msgs < 50 {
			return fmt.Errorf("Only see %d msgs", si.State.Msgs)
		}
		return nil
	})

	close(done)
	<-exited

	if nnr := atomic.LoadUint64(&numNoResp); nnr > 0 {
		if nnr > 5 {
			t.Fatalf("Expected no or very few failed message publishes, got %d", nnr)
		} else {
			t.Logf("Got a few failed publishes: %d", nnr)
		}
	}

	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("SOURCE")
		require_NoError(t, err)
		mi, err := js.StreamInfo("MIRROR")
		require_NoError(t, err)

		if si.State != mi.State {
			return fmt.Errorf("Expected mirror to be the same, got %+v vs %+v", mi.State, si.State)
		}
		return nil
	})

}

func TestJetStreamClusterMovingStreamAndMoveBack(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()

	nc, js := jsClientConnect(t, sc.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Replicas:  3,
		Placement: &nats.Placement{Tags: []string{"cloud:aws"}},
	})
	require_NoError(t, err)

	for i := 0; i < 1000; i++ {
		_, err := js.PublishAsync("TEST", []byte("HELLO WORLD"))
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Replicas:  3,
		Placement: &nats.Placement{Tags: []string{"cloud:gcp"}},
	})
	require_NoError(t, err)

	checkMove := func(cluster string) {
		t.Helper()
		checkFor(t, 30*time.Second, 100*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST")
			if err != nil {
				return err
			}
			if si.Cluster.Name != cluster {
				return fmt.Errorf("Wrong cluster: %q", si.Cluster.Name)
			}
			if si.Cluster.Leader == _EMPTY_ {
				return fmt.Errorf("No leader yet")
			} else if !strings.HasPrefix(si.Cluster.Leader, cluster) {
				return fmt.Errorf("Wrong leader: %q", si.Cluster.Leader)
			}
			// Now we want to see that we shrink back to original.
			if len(si.Cluster.Replicas) != 2 {
				return fmt.Errorf("Expected %d replicas, got %d", 2, len(si.Cluster.Replicas))
			}
			if si.State.Msgs != 1000 {
				return fmt.Errorf("Only see %d msgs", si.State.Msgs)
			}
			return nil
		})
	}

	checkMove("C2")

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Replicas:  3,
		Placement: &nats.Placement{Tags: []string{"cloud:aws"}},
	})
	require_NoError(t, err)

	checkMove("C1")
}

func TestJetStreamClusterMemoryConsumerInterestRetention(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "test",
		Storage:   nats.MemoryStorage,
		Retention: nats.InterestPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("test", nats.Durable("dlc"))
	require_NoError(t, err)

	for i := 0; i < 1000; i++ {
		_, err := js.PublishAsync("test", nil)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	toAck := 100
	for i := 0; i < toAck; i++ {
		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		m.AckSync()
	}

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		si, err := js.StreamInfo("test")
		if err != nil {
			return err
		}
		if n := si.State.Msgs; n != 900 {
			return fmt.Errorf("Waiting for msgs count to be 900, got %v", n)
		}
		return nil
	})

	si, err := js.StreamInfo("test")
	require_NoError(t, err)

	ci, err := sub.ConsumerInfo()
	require_NoError(t, err)

	// Make sure acks are not only replicated but processed in a way to remove messages from the replica streams.
	_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "test"), nil, time.Second)
	require_NoError(t, err)
	c.waitOnStreamLeader("$G", "test")

	_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "test", "dlc"), nil, time.Second)
	require_NoError(t, err)
	c.waitOnConsumerLeader("$G", "test", "dlc")

	nsi, err := js.StreamInfo("test")
	require_NoError(t, err)
	if nsi.State != si.State {
		t.Fatalf("Stream states do not match: %+v vs %+v", si.State, nsi.State)
	}

	nci, err := sub.ConsumerInfo()
	require_NoError(t, err)

	// Last may be skewed a very small amount.
	ci.AckFloor.Last, nci.AckFloor.Last = nil, nil
	if nci.AckFloor != ci.AckFloor {
		t.Fatalf("Consumer AckFloors are not the same: %+v vs %+v", ci.AckFloor, nci.AckFloor)
	}
}

func TestJetStreamClusterImportConsumerStreamSubjectRemap(t *testing.T) {
	template := `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, domain: HUB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts: {
		JS: {
			jetstream: enabled
			users: [ {user: js, password: pwd} ]
			exports [
				# This is streaming to a delivery subject for a push based consumer.
				{ stream: "deliver.ORDERS.*" }
				# This is to ack received messages. This is a service to support sync ack.
				{ service: "$JS.ACK.ORDERS.*.>" }
				# To support ordered consumers, flow control.
				{ service: "$JS.FC.>" }
			]
		},
		IM: {
			users: [ {user: im, password: pwd} ]
			imports [
				{ stream:  { account: JS, subject: "deliver.ORDERS.*" }}
				{ service: {account: JS, subject: "$JS.FC.>" }}
			]
		},
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] },
	}
	leaf {
		listen: 127.0.0.1:-1
	}`

	c := createJetStreamSuperClusterWithTemplate(t, template, 3, 2)
	defer c.shutdown()

	s := c.randomServer()
	nc, js := jsClientConnect(t, s, nats.UserInfo("js", "pwd"))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "ORDERS",
		Subjects:  []string{"foo"}, // The JS subject.
		Replicas:  3,
		Placement: &nats.Placement{Cluster: "C1"},
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", []byte("OK"))
	require_NoError(t, err)

	for dur, deliver := range map[string]string{
		"dur-route":   "deliver.ORDERS.route",
		"dur-gateway": "deliver.ORDERS.gateway",
		"dur-leaf-1":  "deliver.ORDERS.leaf1",
		"dur-leaf-2":  "deliver.ORDERS.leaf2",
	} {
		_, err = js.AddConsumer("ORDERS", &nats.ConsumerConfig{
			Durable:        dur,
			DeliverSubject: deliver,
			AckPolicy:      nats.AckExplicitPolicy,
		})
		require_NoError(t, err)
	}

	test := func(t *testing.T, s *Server, dSubj string) {
		nc2, err := nats.Connect(s.ClientURL(), nats.UserInfo("im", "pwd"))
		require_NoError(t, err)
		defer nc2.Close()

		sub, err := nc2.SubscribeSync(dSubj)
		require_NoError(t, err)

		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)

		if m.Subject != "foo" {
			t.Fatalf("Subject not mapped correctly across account boundary, expected %q got %q", "foo", m.Subject)
		}
		require_False(t, strings.Contains(m.Reply, "@"))
	}

	t.Run("route", func(t *testing.T) {
		// pick random non consumer leader so we receive via route
		s := c.clusterForName("C1").randomNonConsumerLeader("JS", "ORDERS", "dur-route")
		test(t, s, "deliver.ORDERS.route")
	})
	t.Run("gateway", func(t *testing.T) {
		// pick server with inbound gateway from consumer leader, so we receive from gateway and have no route in between
		scl := c.clusterForName("C1").consumerLeader("JS", "ORDERS", "dur-gateway")
		var sfound *Server
		for _, s := range c.clusterForName("C2").servers {
			s.mu.Lock()
			for _, c := range s.gateway.in {
				if c.GetName() == scl.info.ID {
					sfound = s
					break
				}
			}
			s.mu.Unlock()
			if sfound != nil {
				break
			}
		}
		test(t, sfound, "deliver.ORDERS.gateway")
	})
	t.Run("leaf-post-export", func(t *testing.T) {
		// create leaf node server connected post export/import
		scl := c.clusterForName("C1").consumerLeader("JS", "ORDERS", "dur-leaf-1")
		cf := createConfFile(t, []byte(fmt.Sprintf(`
			port: -1
			leafnodes {
				remotes [ { url: "nats://im:pwd@127.0.0.1:%d" } ]
			}
			authorization: {
				user: im,
				password: pwd
			}
		`, scl.getOpts().LeafNode.Port)))
		defer removeFile(t, cf)
		s, _ := RunServerWithConfig(cf)
		defer s.Shutdown()
		checkLeafNodeConnected(t, scl)
		test(t, s, "deliver.ORDERS.leaf1")
	})
	t.Run("leaf-pre-export", func(t *testing.T) {
		// create leaf node server connected pre export, perform export/import on leaf node server
		scl := c.clusterForName("C1").consumerLeader("JS", "ORDERS", "dur-leaf-2")
		cf := createConfFile(t, []byte(fmt.Sprintf(`
			port: -1
			leafnodes {
				remotes [ { url: "nats://im:pwd@127.0.0.1:%d", account: JS2 } ]
			}
			accounts: {
				JS2: {
					users: [ {user: js, password: pwd} ]
					exports [
						# This is streaming to a delivery subject for a push based consumer.
						{ stream: "deliver.ORDERS.*" }
						# This is to ack received messages. This is a service to support sync ack.
						{ service: "$JS.ACK.ORDERS.*.>" }
						# To support ordered consumers, flow control.
						{ service: "$JS.FC.>" }
					]
				},
				IM2: {
					users: [ {user: im, password: pwd} ]
					imports [
						{ stream:  { account: JS2, subject: "deliver.ORDERS.*" }}
						{ service: {account: JS2, subject: "$JS.FC.>" }}
					]
				},
			}
		`, scl.getOpts().LeafNode.Port)))
		defer removeFile(t, cf)
		s, _ := RunServerWithConfig(cf)
		defer s.Shutdown()
		checkLeafNodeConnected(t, scl)
		test(t, s, "deliver.ORDERS.leaf2")
	})
}

func TestJetStreamMaxHaAssets(t *testing.T) {
	sc := createJetStreamSuperClusterWithTemplateAndModHook(t, `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s', limits: {max_ha_assets: 2}}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`, 3, 2,
		func(serverName, clusterName, storeDir, conf string) string {
			return conf
		})
	defer sc.shutdown()

	// speed up statsz reporting
	for _, c := range sc.clusters {
		for _, s := range c.servers {
			s.mu.Lock()
			s.sys.statsz = 10 * time.Millisecond
			s.sys.cstatsz = s.sys.statsz
			s.sys.stmr.Reset(s.sys.statsz)
			s.mu.Unlock()
		}
	}

	nc, js := jsClientConnect(t, sc.randomServer())
	defer nc.Close()

	ncSys := natsConnect(t, sc.randomServer().ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	statszSub, err := ncSys.SubscribeSync(fmt.Sprintf(serverStatsSubj, "*"))
	require_NoError(t, err)
	require_NoError(t, ncSys.Flush())

	waitStatsz := func(peers, haassets int) {
		t.Helper()
		for peersWithExactHaAssets := 0; peersWithExactHaAssets < peers; {
			m, err := statszSub.NextMsg(time.Second)
			require_NoError(t, err)
			var statsz ServerStatsMsg
			err = json.Unmarshal(m.Data, &statsz)
			require_NoError(t, err)
			if statsz.Stats.JetStream == nil {
				continue
			}
			if haassets == statsz.Stats.JetStream.Stats.HAAssets {
				peersWithExactHaAssets++
			}
		}
	}
	waitStatsz(6, 1) // counts _meta_
	_, err = js.AddStream(&nats.StreamConfig{Name: "S0", Replicas: 1, Placement: &nats.Placement{Cluster: "C1"}})
	require_NoError(t, err)
	waitStatsz(6, 1)
	_, err = js.AddStream(&nats.StreamConfig{Name: "S1", Replicas: 3, Placement: &nats.Placement{Cluster: "C1"}})
	require_NoError(t, err)
	waitStatsz(3, 2)
	waitStatsz(3, 1)
	_, err = js.AddStream(&nats.StreamConfig{Name: "S2", Replicas: 3, Placement: &nats.Placement{Cluster: "C1"}})
	require_NoError(t, err)
	waitStatsz(3, 3)
	waitStatsz(3, 1)
	_, err = js.AddStream(&nats.StreamConfig{Name: "S3", Replicas: 3, Placement: &nats.Placement{Cluster: "C1"}})
	require_Error(t, err)
	require_Equal(t, err.Error(), "insufficient resources")
	require_NoError(t, js.DeleteStream("S1"))
	waitStatsz(3, 2)
	waitStatsz(3, 1)
	_, err = js.AddConsumer("S2", &nats.ConsumerConfig{Durable: "DUR1", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	waitStatsz(3, 3)
	waitStatsz(3, 1)
	_, err = js.AddConsumer("S2", &nats.ConsumerConfig{Durable: "DUR2", AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err)
	require_Equal(t, err.Error(), "insufficient resources")
	_, err = js.AddConsumer("S2", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	waitStatsz(3, 3)
	waitStatsz(3, 1)
	_, err = js.UpdateStream(&nats.StreamConfig{Name: "S2", Replicas: 3, Description: "foobar"})
	require_NoError(t, err)
	waitStatsz(3, 3)
	waitStatsz(3, 1)
	si, err := js.AddStream(&nats.StreamConfig{Name: "S4", Replicas: 3})
	require_NoError(t, err)
	require_Equal(t, si.Cluster.Name, "C2")
	waitStatsz(3, 3)
	waitStatsz(3, 2)
	si, err = js.AddStream(&nats.StreamConfig{Name: "S5", Replicas: 3})
	require_NoError(t, err)
	require_Equal(t, si.Cluster.Name, "C2")
	waitStatsz(6, 3)
	_, err = js.AddConsumer("S4", &nats.ConsumerConfig{Durable: "DUR2", AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err)
	require_Equal(t, err.Error(), "insufficient resources")
	_, err = js.UpdateStream(&nats.StreamConfig{Name: "S2", Replicas: 3, Placement: &nats.Placement{Cluster: "C2"}})
	require_Error(t, err)
	require_Equal(t, err.Error(), "insufficient resources")
}

func TestJetStreamClusterStreamAlternates(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()

	nc, js := jsClientConnect(t, sc.randomServer())
	defer nc.Close()

	// C1
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "SOURCE",
		Subjects:  []string{"foo", "bar", "baz"},
		Replicas:  3,
		Placement: &nats.Placement{Tags: []string{"cloud:aws", "country:us"}},
	})
	require_NoError(t, err)

	// C2
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      "MIRROR-1",
		Replicas:  1,
		Mirror:    &nats.StreamSource{Name: "SOURCE"},
		Placement: &nats.Placement{Tags: []string{"cloud:gcp", "country:uk"}},
	})
	require_NoError(t, err)

	// C3
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      "MIRROR-2",
		Replicas:  2,
		Mirror:    &nats.StreamSource{Name: "SOURCE"},
		Placement: &nats.Placement{Tags: []string{"cloud:az", "country:jp"}},
	})
	require_NoError(t, err)

	// No client support yet, so do by hand.
	getStreamInfo := func(nc *nats.Conn, expected string) {
		t.Helper()
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "SOURCE"), nil, time.Second)
		require_NoError(t, err)
		var si StreamInfo
		err = json.Unmarshal(resp.Data, &si)
		require_NoError(t, err)
		require_True(t, len(si.Alternates) == 3)
		require_True(t, si.Alternates[0].Cluster == expected)
		seen := make(map[string]struct{})
		for _, alt := range si.Alternates {
			seen[alt.Cluster] = struct{}{}
		}
		require_True(t, len(seen) == 3)
	}

	// Connect to different clusters to check ordering.
	nc, _ = jsClientConnect(t, sc.clusterForName("C1").randomServer())
	getStreamInfo(nc, "C1")
	nc, _ = jsClientConnect(t, sc.clusterForName("C2").randomServer())
	getStreamInfo(nc, "C2")
	nc, _ = jsClientConnect(t, sc.clusterForName("C3").randomServer())
	getStreamInfo(nc, "C3")
}

func TestJetStreamClusterDeleteAndRestoreAndRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST"})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := js.Publish("TEST", []byte("OK"))
		require_NoError(t, err)
	}
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	require_NoError(t, js.DeleteConsumer("TEST", "dlc"))
	require_NoError(t, js.DeleteStream("TEST"))

	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST"})
	require_NoError(t, err)

	for i := 0; i < 22; i++ {
		_, err := js.Publish("TEST", []byte("OK"))
		require_NoError(t, err)
	}

	sub, err := js.SubscribeSync("TEST", nats.Durable("dlc"), nats.Description("SECOND"))
	require_NoError(t, err)

	for i := 0; i < 5; i++ {
		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		m.AckSync()
	}

	// Now restart.
	sl := c.streamLeader("$G", "TEST")
	sl.Shutdown()
	sl = c.restartServer(sl)
	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnConsumerLeader("$G", "TEST", "dlc")

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	if si.State.Msgs != 22 {
		t.Fatalf("State is not correct after restart")
	}

	ci, err := js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)

	if ci.AckFloor.Consumer != 5 {
		t.Fatalf("Bad ack floor: %+v", ci.AckFloor)
	}

	// Now delete and make sure consumer does not come back.
	// First add a delete something else.
	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST2"})
	require_NoError(t, err)
	require_NoError(t, js.DeleteStream("TEST2"))
	// Now the consumer.
	require_NoError(t, js.DeleteConsumer("TEST", "dlc"))

	sl.Shutdown()
	c.restartServer(sl)
	c.waitOnStreamLeader("$G", "TEST")

	// In rare circumstances this could be recovered and then quickly deleted.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if _, err := js.ConsumerInfo("TEST", "dlc"); err == nil {
			return fmt.Errorf("Not cleaned up yet")
		}
		return nil
	})
}

// Support functions

// Used to setup superclusters for tests.
type supercluster struct {
	t        *testing.T
	clusters []*cluster
}

func (sc *supercluster) shutdown() {
	if sc == nil {
		return
	}
	for _, c := range sc.clusters {
		shutdownCluster(c)
	}
}

func (sc *supercluster) randomServer() *Server {
	return sc.randomCluster().randomServer()
}

func (sc *supercluster) serverByName(sname string) *Server {
	for _, c := range sc.clusters {
		if s := c.serverByName(sname); s != nil {
			return s
		}
	}
	return nil
}

func (sc *supercluster) waitOnStreamLeader(account, stream string) {
	sc.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		for _, c := range sc.clusters {
			if leader := c.streamLeader(account, stream); leader != nil {
				time.Sleep(200 * time.Millisecond)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	sc.t.Fatalf("Expected a stream leader for %q %q, got none", account, stream)
}

var jsClusterAccountsTempl = `
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

	no_auth_user: one

	accounts {
		ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
		TWO { users = [ { user: "two", pass: "p" } ]; jetstream: enabled }
		NOJS { users = [ { user: "nojs", pass: "p" } ] }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsClusterTempl = `
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

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterMaxBytesTempl = `
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

	no_auth_user: u

	accounts {
		$U {
			users = [ { user: "u", pass: "p" } ]
			jetstream: {
				max_mem:   128MB
				max_file:  18GB
				max_bytes: true // Forces streams to indicate max_bytes.
			}
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsClusterMaxBytesAccountLimitTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 4GB, store_dir: '%s'}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: u

	accounts {
		$U {
			users = [ { user: "u", pass: "p" } ]
			jetstream: {
				max_mem:   128MB
				max_file:  3GB
				max_bytes: true // Forces streams to indicate max_bytes.
			}
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsSuperClusterTempl = `
	%s
	gateway {
		name: %s
		listen: 127.0.0.1:%d
		gateways = [%s
		]
	}

	system_account: "$SYS"
`

var jsClusterLimitsTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2MB, max_file_store: 8MB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: u

	accounts {
		ONE {
			users = [ { user: "u", pass: "s3cr3t!" } ]
			jetstream: enabled
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsMixedModeGlobalAccountTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2MB, max_file_store: 8MB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsGWTempl = `%s{name: %s, urls: [%s]}`

func createJetStreamTaggedSuperCluster(t *testing.T) *supercluster {
	sc := createJetStreamSuperCluster(t, 3, 3)
	sc.waitOnPeerCount(9)

	reset := func(s *Server) {
		s.mu.Lock()
		rch := s.sys.resetCh
		s.mu.Unlock()
		if rch != nil {
			rch <- struct{}{}
		}
		s.sendStatszUpdate()
	}

	// Make first cluster AWS, US country code.
	for _, s := range sc.clusterForName("C1").servers {
		s.optsMu.Lock()
		s.opts.Tags.Add("cloud:aws")
		s.opts.Tags.Add("country:us")
		s.optsMu.Unlock()
		reset(s)
	}
	// Make second cluster GCP, UK country code.
	for _, s := range sc.clusterForName("C2").servers {
		s.optsMu.Lock()
		s.opts.Tags.Add("cloud:gcp")
		s.opts.Tags.Add("country:uk")
		s.optsMu.Unlock()
		reset(s)
	}
	// Make third cluster AZ, JP country code.
	for _, s := range sc.clusterForName("C3").servers {
		s.optsMu.Lock()
		s.opts.Tags.Add("cloud:az")
		s.opts.Tags.Add("country:jp")
		s.optsMu.Unlock()
		reset(s)
	}

	ml := sc.leader()
	js := ml.getJetStream()
	require_True(t, js != nil)
	js.mu.RLock()
	defer js.mu.RUnlock()
	cc := js.cluster
	require_True(t, cc != nil)

	// Walk and make sure all tags are registered.
	expires := time.Now().Add(10 * time.Second)
	for time.Now().Before(expires) {
		allOK := true
		for _, p := range cc.meta.Peers() {
			si, ok := ml.nodeToInfo.Load(p.ID)
			require_True(t, ok)
			ni := si.(nodeInfo)
			if len(ni.tags) == 0 {
				allOK = false
				reset(sc.serverByName(ni.name))
			}
		}
		if allOK {
			break
		}
	}

	return sc
}

func createJetStreamSuperCluster(t *testing.T, numServersPer, numClusters int) *supercluster {
	return createJetStreamSuperClusterWithTemplate(t, jsClusterTempl, numServersPer, numClusters)
}

func createJetStreamSuperClusterWithTemplate(t *testing.T, tmpl string, numServersPer, numClusters int) *supercluster {
	return createJetStreamSuperClusterWithTemplateAndModHook(t, tmpl, numServersPer, numClusters, nil)
}

func createJetStreamSuperClusterWithTemplateAndModHook(t *testing.T, tmpl string, numServersPer, numClusters int, modify modifyCb) *supercluster {
	t.Helper()
	if numServersPer < 1 {
		t.Fatalf("Number of servers must be >= 1")
	}
	if numClusters <= 1 {
		t.Fatalf("Number of clusters must be > 1")
	}

	startClusterPorts := []int{20_022, 22_022, 24_022}
	startGatewayPorts := []int{20_122, 22_122, 24_122}
	startClusterPort := startClusterPorts[rand.Intn(len(startClusterPorts))]
	startGWPort := startGatewayPorts[rand.Intn(len(startGatewayPorts))]

	// Make the GWs form faster for the tests.
	SetGatewaysSolicitDelay(10 * time.Millisecond)
	defer ResetGatewaysSolicitDelay()

	cp, gp := startClusterPort, startGWPort
	var clusters []*cluster

	var gws []string
	// Build GWs first, will be same for all servers.
	for i, port := 1, gp; i <= numClusters; i++ {
		cn := fmt.Sprintf("C%d", i)
		var urls []string
		for n := 0; n < numServersPer; n++ {
			urls = append(urls, fmt.Sprintf("nats-route://127.0.0.1:%d", port))
			port++
		}
		gws = append(gws, fmt.Sprintf(jsGWTempl, "\n\t\t\t", cn, strings.Join(urls, ",")))
	}
	gwconf := strings.Join(gws, "")

	for i := 1; i <= numClusters; i++ {
		cn := fmt.Sprintf("C%d", i)
		// Go ahead and build configurations.
		c := &cluster{servers: make([]*Server, 0, numServersPer), opts: make([]*Options, 0, numServersPer), name: cn}

		// Build out the routes that will be shared with all configs.
		var routes []string
		for port := cp; port < cp+numServersPer; port++ {
			routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", port))
		}
		routeConfig := strings.Join(routes, ",")

		for si := 0; si < numServersPer; si++ {
			storeDir := createDir(t, JetStreamStoreDir)
			sn := fmt.Sprintf("%s-S%d", cn, si+1)
			bconf := fmt.Sprintf(tmpl, sn, storeDir, cn, cp+si, routeConfig)
			conf := fmt.Sprintf(jsSuperClusterTempl, bconf, cn, gp, gwconf)
			gp++
			if modify != nil {
				conf = modify(sn, cn, storeDir, conf)
			}
			s, o := RunServerWithConfig(createConfFile(t, []byte(conf)))
			c.servers = append(c.servers, s)
			c.opts = append(c.opts, o)
		}
		checkClusterFormed(t, c.servers...)
		clusters = append(clusters, c)
		cp += numServersPer
		c.t = t
	}

	// Wait for the supercluster to be formed.
	egws := numClusters - 1
	for _, c := range clusters {
		for _, s := range c.servers {
			waitForOutboundGateways(t, s, egws, 2*time.Second)
		}
	}

	sc := &supercluster{t, clusters}
	sc.waitOnLeader()
	sc.waitOnAllCurrent()

	// Wait for all the peer nodes to be registered.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		var peers []string
		if ml := sc.leader(); ml != nil {
			peers = ml.ActivePeers()
			if len(peers) == numClusters*numServersPer {
				return nil
			}
		}
		return fmt.Errorf("Not correct number of peers, expected %d, got %d", numClusters*numServersPer, len(peers))
	})

	if sc.leader() == nil {
		sc.t.Fatalf("Expected a cluster leader, got none")
	}

	return sc
}

func (sc *supercluster) createLeafNodes(clusterName string, numServers int) *cluster {
	// Create our leafnode cluster template first.
	return sc.createLeafNodesWithDomain(clusterName, numServers, "")
}

func (sc *supercluster) createLeafNodesWithDomain(clusterName string, numServers int, domain string) *cluster {
	// Create our leafnode cluster template first.
	return sc.randomCluster().createLeafNodes(clusterName, numServers, domain)
}

func (sc *supercluster) createSingleLeafNode(extend bool) *Server {
	return sc.randomCluster().createLeafNode(extend)
}

func (sc *supercluster) leader() *Server {
	for _, c := range sc.clusters {
		if leader := c.leader(); leader != nil {
			return leader
		}
	}
	return nil
}

func (sc *supercluster) waitOnLeader() {
	sc.t.Helper()
	expires := time.Now().Add(5 * time.Second)
	for time.Now().Before(expires) {
		for _, c := range sc.clusters {
			if leader := c.leader(); leader != nil {
				time.Sleep(250 * time.Millisecond)
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	sc.t.Fatalf("Expected a cluster leader, got none")
}

func (sc *supercluster) waitOnAllCurrent() {
	for _, c := range sc.clusters {
		c.waitOnAllCurrent()
	}
}

func (sc *supercluster) clusterForName(name string) *cluster {
	for _, c := range sc.clusters {
		if c.name == name {
			return c
		}
	}
	return nil
}

func (sc *supercluster) randomCluster() *cluster {
	clusters := append(sc.clusters[:0:0], sc.clusters...)
	rand.Shuffle(len(clusters), func(i, j int) { clusters[i], clusters[j] = clusters[j], clusters[i] })
	return clusters[0]
}

func (sc *supercluster) waitOnPeerCount(n int) {
	sc.t.Helper()
	sc.waitOnLeader()
	leader := sc.leader()
	expires := time.Now().Add(20 * time.Second)
	for time.Now().Before(expires) {
		peers := leader.JetStreamClusterPeers()
		if len(peers) == n {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	sc.t.Fatalf("Expected a super cluster peer count of %d, got %d", n, len(leader.JetStreamClusterPeers()))
}

var jsClusterMirrorSourceImportsTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: dlc

	accounts {
		JS {
			jetstream: enabled
			users = [ { user: "rip", pass: "pass" } ]
			exports [
				{ service: "$JS.API.CONSUMER.>" } # To create internal consumers to mirror/source.
				{ stream: "RI.DELIVER.SYNC.>" }   # For the mirror/source consumers sending to IA via delivery subject.
				{ service: "$JS.FC.>" }
			]
		}
		IA {
			jetstream: enabled
			users = [ { user: "dlc", pass: "pass" } ]
			imports [
				{ service: { account: JS, subject: "$JS.API.CONSUMER.>"}, to: "RI.JS.API.CONSUMER.>" }
				{ stream: { account: JS, subject: "RI.DELIVER.SYNC.>"} }
				{ service: {account: JS, subject: "$JS.FC.>" }}
			]
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsClusterImportsTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: dlc

	accounts {
		JS {
			jetstream: enabled
			users = [ { user: "rip", pass: "pass" } ]
			exports [
				{ service: "$JS.API.>", response: stream }
				{ service: "TEST" } # For publishing to the stream.
				{ service: "$JS.ACK.TEST.*.>" }
			]
		}
		IA {
			users = [ { user: "dlc", pass: "pass" } ]
			imports [
				{ service: { subject: "$JS.API.>", account: JS }}
				{ service: { subject: "TEST", account: JS }}
				{ service: { subject: "$JS.ACK.TEST.*.>", account: JS }}
			]
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

func createMixedModeCluster(t *testing.T, tmpl string, clusterName, snPre string, numJsServers, numNonServers int, doJSConfig bool) *cluster {
	t.Helper()

	if clusterName == _EMPTY_ || numJsServers < 0 || numNonServers < 1 {
		t.Fatalf("Bad params")
	}

	numServers := numJsServers + numNonServers
	const startClusterPort = 23232

	// Build out the routes that will be shared with all configs.
	var routes []string
	for cp := startClusterPort; cp < startClusterPort+numServers; cp++ {
		routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", cp))
	}
	routeConfig := strings.Join(routes, ",")

	// Go ahead and build configurations and start servers.
	c := &cluster{servers: make([]*Server, 0, numServers), opts: make([]*Options, 0, numServers), name: clusterName}

	for cp := startClusterPort; cp < startClusterPort+numServers; cp++ {
		storeDir := createDir(t, JetStreamStoreDir)

		sn := fmt.Sprintf("%sS-%d", snPre, cp-startClusterPort+1)
		conf := fmt.Sprintf(tmpl, sn, storeDir, clusterName, cp, routeConfig)

		// Disable JS here.
		if cp-startClusterPort >= numJsServers {
			// We can disable by commmenting it out, meaning no JS config, or can set the config up and just set disabled.
			// e.g. jetstream: {domain: "SPOKE", enabled: false}
			if doJSConfig {
				conf = strings.Replace(conf, "jetstream: {", "jetstream: { enabled: false, ", 1)
			} else {
				conf = strings.Replace(conf, "jetstream: ", "# jetstream: ", 1)
			}
		}

		s, o := RunServerWithConfig(createConfFile(t, []byte(conf)))
		c.servers = append(c.servers, s)
		c.opts = append(c.opts, o)
	}
	c.t = t

	// Wait til we are formed and have a leader.
	c.checkClusterFormed()
	if numJsServers > 0 {
		c.waitOnPeerCount(numJsServers)
	}

	return c
}

// This will create a cluster that is explicitly configured for the routes, etc.
// and also has a defined clustername. All configs for routes and cluster name will be the same.
func createJetStreamClusterExplicit(t *testing.T, clusterName string, numServers int) *cluster {
	return createJetStreamClusterWithTemplate(t, jsClusterTempl, clusterName, numServers)
}

func createJetStreamClusterWithTemplate(t *testing.T, tmpl string, clusterName string, numServers int) *cluster {
	return createJetStreamClusterWithTemplateAndModHook(t, tmpl, clusterName, numServers, nil)
}

func createJetStreamClusterWithTemplateAndModHook(t *testing.T, tmpl string, clusterName string, numServers int, modify modifyCb) *cluster {
	startPorts := []int{7_022, 9_022, 11_022, 15_022}
	port := startPorts[rand.Intn(len(startPorts))]
	return createJetStreamClusterAndModHook(t, tmpl, clusterName, _EMPTY_, numServers, port, true, modify)
}

func createJetStreamCluster(t *testing.T, tmpl string, clusterName, snPre string, numServers int, portStart int, waitOnReady bool) *cluster {
	return createJetStreamClusterAndModHook(t, tmpl, clusterName, snPre, numServers, portStart, waitOnReady, nil)
}

type modifyCb func(serverName, clusterName, storeDir, conf string) string

func createJetStreamClusterAndModHook(t *testing.T, tmpl string, clusterName, snPre string, numServers int, portStart int, waitOnReady bool, modify modifyCb) *cluster {
	t.Helper()
	if clusterName == _EMPTY_ || numServers < 1 {
		t.Fatalf("Bad params")
	}

	// Flaky test prevention:
	// Binding a socket to IP stack port 0 will bind an ephemeral port from an OS-specific range.
	// If someone passes in to us a port spec which would cover that range, the test would be flaky.
	// Adjust these ports to be the most inclusive across the port runner OSes.
	// Linux: /proc/sys/net/ipv4/ip_local_port_range : 32768:60999
	// <https://dataplane.org/ephemeralports.html> is useful, and shows there's no safe available range without OS-specific tuning.
	// Our tests are usually run on Linux.  Folks who care about other OSes: if you can't tune your test-runner OS to match, please
	// propose a viable alternative.
	const prohibitedPortFirst = 32768
	const prohibitedPortLast = 60999
	if (portStart >= prohibitedPortFirst && portStart <= prohibitedPortLast) ||
		(portStart+numServers-1 >= prohibitedPortFirst && portStart+numServers-1 <= prohibitedPortLast) {
		t.Fatalf("test setup failure: may not specify a cluster port range which falls within %d:%d", prohibitedPortFirst, prohibitedPortLast)
	}

	// Build out the routes that will be shared with all configs.
	var routes []string
	for cp := portStart; cp < portStart+numServers; cp++ {
		routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", cp))
	}
	routeConfig := strings.Join(routes, ",")

	// Go ahead and build configurations and start servers.
	c := &cluster{servers: make([]*Server, 0, numServers), opts: make([]*Options, 0, numServers), name: clusterName}

	for cp := portStart; cp < portStart+numServers; cp++ {
		storeDir := createDir(t, JetStreamStoreDir)
		sn := fmt.Sprintf("%sS-%d", snPre, cp-portStart+1)
		conf := fmt.Sprintf(tmpl, sn, storeDir, clusterName, cp, routeConfig)
		if modify != nil {
			conf = modify(sn, clusterName, storeDir, conf)
		}
		s, o := RunServerWithConfig(createConfFile(t, []byte(conf)))
		c.servers = append(c.servers, s)
		c.opts = append(c.opts, o)
	}
	c.t = t

	// Wait til we are formed and have a leader.
	c.checkClusterFormed()
	if waitOnReady {
		c.waitOnClusterReady()
	}

	return c
}

func (c *cluster) addInNewServer() *Server {
	c.t.Helper()
	sn := fmt.Sprintf("S-%d", len(c.servers)+1)
	storeDir, _ := ioutil.TempDir(tempRoot, JetStreamStoreDir)
	seedRoute := fmt.Sprintf("nats-route://127.0.0.1:%d", c.opts[0].Cluster.Port)
	conf := fmt.Sprintf(jsClusterTempl, sn, storeDir, c.name, -1, seedRoute)
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)
	c.checkClusterFormed()
	return s
}

// This is tied to jsClusterAccountsTempl, so changes there to users needs to be reflected here.
func (c *cluster) createSingleLeafNodeNoSystemAccount() *Server {
	as := c.randomServer()
	lno := as.getOpts().LeafNode
	ln1 := fmt.Sprintf("nats://one:p@%s:%d", lno.Host, lno.Port)
	ln2 := fmt.Sprintf("nats://two:p@%s:%d", lno.Host, lno.Port)
	conf := fmt.Sprintf(jsClusterSingleLeafNodeTempl, createDir(c.t, JetStreamStoreDir), ln1, ln2)
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)

	checkLeafNodeConnectedCount(c.t, as, 2)

	return s
}

// This is tied to jsClusterAccountsTempl, so changes there to users needs to be reflected here.
func (c *cluster) createSingleLeafNodeNoSystemAccountAndEnablesJetStream() *Server {
	return c.createSingleLeafNodeNoSystemAccountAndEnablesJetStreamWithDomain(_EMPTY_, "nojs")
}

func (c *cluster) createSingleLeafNodeNoSystemAccountAndEnablesJetStreamWithDomain(domain, user string) *Server {
	tmpl := jsClusterSingleLeafNodeLikeNGSTempl
	if domain != _EMPTY_ {
		nsc := fmt.Sprintf("domain: %s, store_dir:", domain)
		tmpl = strings.Replace(jsClusterSingleLeafNodeLikeNGSTempl, "store_dir:", nsc, 1)
	}
	as := c.randomServer()
	lno := as.getOpts().LeafNode
	ln := fmt.Sprintf("nats://%s:p@%s:%d", user, lno.Host, lno.Port)
	conf := fmt.Sprintf(tmpl, createDir(c.t, JetStreamStoreDir), ln)
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)

	checkLeafNodeConnectedCount(c.t, as, 1)

	return s
}

var jsClusterSingleLeafNodeLikeNGSTempl = `
	listen: 127.0.0.1:-1
	server_name: LNJS
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	leaf { remotes [ { urls: [ %s ] } ] }
`

var jsClusterSingleLeafNodeTempl = `
	listen: 127.0.0.1:-1
	server_name: LNJS
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	leaf { remotes [
		{ urls: [ %s ], account: "JSY" }
		{ urls: [ %s ], account: "JSN" } ]
	}

	accounts {
		JSY { users = [ { user: "y", pass: "p" } ]; jetstream: true }
		JSN { users = [ { user: "n", pass: "p" } ] }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsClusterTemplWithLeafNode = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	{{leaf}}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterTemplWithLeafNodeNoJS = `
	listen: 127.0.0.1:-1
	server_name: %s

	# Need to keep below since it fills in the store dir by default so just comment out.
	# jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	{{leaf}}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterTemplWithSingleLeafNode = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	{{leaf}}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterTemplWithSingleLeafNodeNoJS = `
	listen: 127.0.0.1:-1
	server_name: %s

	# jetstream: {store_dir: '%s'}

	{{leaf}}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsLeafFrag = `
	leaf {
		remotes [
			{ urls: [ %s ] }
			{ urls: [ %s ], account: "$SYS" }
		]
	}
`

func (c *cluster) createLeafNodes(clusterName string, numServers int, domain string) *cluster {
	return c.createLeafNodesWithStartPortAndDomain(clusterName, numServers, 22111, domain)
}

func (c *cluster) createLeafNodesNoJS(clusterName string, numServers int) *cluster {
	return c.createLeafNodesWithTemplateAndStartPort(jsClusterTemplWithLeafNodeNoJS, clusterName, numServers, 21333)
}

func (c *cluster) createLeafNodesWithStartPortAndDomain(clusterName string, numServers int, portStart int, domain string) *cluster {
	if domain == _EMPTY_ {
		return c.createLeafNodesWithTemplateAndStartPort(jsClusterTemplWithLeafNode, clusterName, numServers, portStart)
	}
	tmpl := strings.Replace(jsClusterTemplWithLeafNode, "store_dir:", fmt.Sprintf(`domain: "%s", store_dir:`, domain), 1)
	return c.createLeafNodesWithTemplateAndStartPort(tmpl, clusterName, numServers, portStart)
}

func (c *cluster) createLeafNode(extend bool) *Server {
	if extend {
		return c.createLeafNodeWithTemplate("LNS",
			strings.ReplaceAll(jsClusterTemplWithSingleLeafNode, "store_dir:", " extension_hint: will_extend, store_dir:"))
	} else {
		return c.createLeafNodeWithTemplate("LNS", jsClusterTemplWithSingleLeafNode)
	}
}

func (c *cluster) createLeafNodeWithTemplate(name, template string) *Server {
	tmpl := c.createLeafSolicit(template)
	conf := fmt.Sprintf(tmpl, name, createDir(c.t, JetStreamStoreDir))
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)
	return s
}

// Helper to generate the leaf solicit configs.
func (c *cluster) createLeafSolicit(tmpl string) string {
	// Create our leafnode cluster template first.
	var lns, lnss []string
	for _, s := range c.servers {
		if s.ClusterName() != c.name {
			continue
		}
		ln := s.getOpts().LeafNode
		lns = append(lns, fmt.Sprintf("nats://%s:%d", ln.Host, ln.Port))
		lnss = append(lnss, fmt.Sprintf("nats://admin:s3cr3t!@%s:%d", ln.Host, ln.Port))
	}
	lnc := strings.Join(lns, ", ")
	lnsc := strings.Join(lnss, ", ")
	lconf := fmt.Sprintf(jsLeafFrag, lnc, lnsc)
	return strings.Replace(tmpl, "{{leaf}}", lconf, 1)
}

func (c *cluster) createLeafNodesWithTemplateMixedMode(template, clusterName string, numJsServers, numNonServers int, doJSConfig bool) *cluster {
	// Create our leafnode cluster template first.
	tmpl := c.createLeafSolicit(template)
	pre := clusterName + "-"

	lc := createMixedModeCluster(c.t, tmpl, clusterName, pre, numJsServers, numNonServers, doJSConfig)
	for _, s := range lc.servers {
		checkLeafNodeConnectedCount(c.t, s, 2)
	}
	lc.waitOnClusterReadyWithNumPeers(numJsServers)

	return lc
}

func (c *cluster) createLeafNodesWithTemplateAndStartPort(template, clusterName string, numServers int, portStart int) *cluster {
	// Create our leafnode cluster template first.
	tmpl := c.createLeafSolicit(template)
	pre := clusterName + "-"
	lc := createJetStreamCluster(c.t, tmpl, clusterName, pre, numServers, portStart, false)
	for _, s := range lc.servers {
		checkLeafNodeConnectedCount(c.t, s, 2)
	}
	return lc
}

// Will add in the mapping for the account to each server.
func (c *cluster) addSubjectMapping(account, src, dest string) {
	for _, s := range c.servers {
		if s.ClusterName() != c.name {
			continue
		}
		acc, err := s.LookupAccount(account)
		if err != nil {
			c.t.Fatalf("Unexpected error on %v: %v", s, err)
		}
		if err := acc.AddMapping(src, dest); err != nil {
			c.t.Fatalf("Error adding mapping: %v", err)
		}
	}
	// Make sure interest propagates.
	time.Sleep(200 * time.Millisecond)
}

// Adjust limits for the given account.
func (c *cluster) updateLimits(account string, newLimits map[string]JetStreamAccountLimits) {
	c.t.Helper()
	for _, s := range c.servers {
		acc, err := s.LookupAccount(account)
		if err != nil {
			c.t.Fatalf("Unexpected error: %v", err)
		}
		if err := acc.UpdateJetStreamLimits(newLimits); err != nil {
			c.t.Fatalf("Unexpected error: %v", err)
		}
	}
}

// Hack for staticcheck
var skip = func(t *testing.T) {
	t.SkipNow()
}

func jsClientConnect(t *testing.T, s *Server, opts ...nats.Option) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func jsClientConnectEx(t *testing.T, s *Server, domain string, opts ...nats.Option) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	js, err := nc.JetStream(nats.MaxWait(10*time.Second), nats.Domain(domain))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func checkSubsPending(t *testing.T, sub *nats.Subscription, numExpected int) {
	t.Helper()
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if nmsgs, _, err := sub.Pending(); err != nil || nmsgs != numExpected {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
		}
		return nil
	})
}

func fetchMsgs(t *testing.T, sub *nats.Subscription, numExpected int, totalWait time.Duration) []*nats.Msg {
	t.Helper()
	result := make([]*nats.Msg, 0, numExpected)
	for start, count, wait := time.Now(), numExpected, totalWait; len(result) != numExpected; {
		msgs, err := sub.Fetch(count, nats.MaxWait(wait))
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, msgs...)
		count -= len(msgs)
		if wait = totalWait - time.Since(start); wait < 0 {
			break
		}
	}
	if len(result) != numExpected {
		t.Fatalf("Unexpected msg count, got %d, want %d", len(result), numExpected)
	}
	return result
}

func (c *cluster) restartServer(rs *Server) *Server {
	c.t.Helper()
	index := -1
	var opts *Options
	for i, s := range c.servers {
		if s == rs {
			index = i
			break
		}
	}
	if index < 0 {
		c.t.Fatalf("Could not find server %v to restart", rs)
	}
	opts = c.opts[index]
	s, o := RunServerWithConfig(opts.ConfigFile)
	c.servers[index] = s
	c.opts[index] = o
	return s
}

func (c *cluster) checkClusterFormed() {
	c.t.Helper()
	checkClusterFormed(c.t, c.servers...)
}

func (c *cluster) waitOnPeerCount(n int) {
	c.t.Helper()
	c.waitOnLeader()
	leader := c.leader()
	for leader == nil {
		c.waitOnLeader()
		leader = c.leader()
	}
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		if peers := leader.JetStreamClusterPeers(); len(peers) == n {
			return
		}
		time.Sleep(100 * time.Millisecond)
		leader = c.leader()
		for leader == nil {
			c.waitOnLeader()
			leader = c.leader()
		}
	}
	c.t.Fatalf("Expected a cluster peer count of %d, got %d", n, len(leader.JetStreamClusterPeers()))
}

func (c *cluster) waitOnConsumerLeader(account, stream, consumer string) {
	c.t.Helper()
	expires := time.Now().Add(20 * time.Second)
	for time.Now().Before(expires) {
		if leader := c.consumerLeader(account, stream, consumer); leader != nil {
			time.Sleep(200 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("Expected a consumer leader for %q %q %q, got none", account, stream, consumer)
}

func (c *cluster) consumerLeader(account, stream, consumer string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if s.JetStreamIsConsumerLeader(account, stream, consumer) {
			return s
		}
	}
	return nil
}

func (c *cluster) randomNonConsumerLeader(account, stream, consumer string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if !s.JetStreamIsConsumerLeader(account, stream, consumer) {
			return s
		}
	}
	return nil
}

func (c *cluster) waitOnStreamLeader(account, stream string) {
	c.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		if leader := c.streamLeader(account, stream); leader != nil {
			time.Sleep(200 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("Expected a stream leader for %q %q, got none", account, stream)
}

func (c *cluster) randomNonStreamLeader(account, stream string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if s.JetStreamIsStreamAssigned(account, stream) && !s.JetStreamIsStreamLeader(account, stream) {
			return s
		}
	}
	return nil
}

func (c *cluster) randomStreamNotAssigned(account, stream string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if !s.JetStreamIsStreamAssigned(account, stream) {
			return s
		}
	}
	return nil
}

func (c *cluster) streamLeader(account, stream string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if s.JetStreamIsStreamLeader(account, stream) {
			return s
		}
	}
	return nil
}

func (c *cluster) waitOnStreamCurrent(s *Server, account, stream string) {
	c.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		if s.JetStreamIsStreamCurrent(account, stream) {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("Expected server %q to eventually be current for stream %q", s, stream)
}

func (c *cluster) waitOnServerHealthz(s *Server) {
	c.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		hs := s.healthz()
		if hs.Status == "ok" && hs.Error == _EMPTY_ {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("Expected server %q to eventually return healthz 'ok', but got %q", s, s.healthz().Error)
}

func (c *cluster) waitOnServerCurrent(s *Server) {
	c.t.Helper()
	expires := time.Now().Add(20 * time.Second)
	for time.Now().Before(expires) {
		time.Sleep(100 * time.Millisecond)
		if !s.JetStreamEnabled() || s.JetStreamIsCurrent() {
			return
		}
	}
	c.t.Fatalf("Expected server %q to eventually be current", s)
}

func (c *cluster) waitOnAllCurrent() {
	for _, cs := range c.servers {
		c.waitOnServerCurrent(cs)
	}
}

func (c *cluster) serverByName(sname string) *Server {
	for _, s := range c.servers {
		if s.Name() == sname {
			return s
		}
	}
	return nil
}

func (c *cluster) randomNonLeader() *Server {
	// range should randomize.. but..
	for _, s := range c.servers {
		if s.Running() && !s.JetStreamIsLeader() {
			return s
		}
	}
	return nil
}

func (c *cluster) leader() *Server {
	for _, s := range c.servers {
		if s.JetStreamIsLeader() {
			return s
		}
	}
	return nil
}

func (c *cluster) expectNoLeader() {
	c.t.Helper()
	expires := time.Now().Add(maxElectionTimeout)
	for time.Now().Before(expires) {
		if c.leader() == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("Expected no leader but have one")
}

func (c *cluster) waitOnLeader() {
	c.t.Helper()
	expires := time.Now().Add(40 * time.Second)
	for time.Now().Before(expires) {
		if leader := c.leader(); leader != nil {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	c.t.Fatalf("Expected a cluster leader, got none")
}

// Helper function to check that a cluster is formed
func (c *cluster) waitOnClusterReady() {
	c.t.Helper()
	c.waitOnClusterReadyWithNumPeers(len(c.servers))
}

func (c *cluster) waitOnClusterReadyWithNumPeers(numPeersExpected int) {
	c.t.Helper()
	var leader *Server
	expires := time.Now().Add(40 * time.Second)
	for time.Now().Before(expires) {
		if leader = c.leader(); leader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Now make sure we have all peers.
	for leader != nil && time.Now().Before(expires) {
		if len(leader.JetStreamClusterPeers()) == numPeersExpected {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	if leader == nil {
		c.t.Fatalf("Failed to elect a meta-leader")
	}

	peersSeen := len(leader.JetStreamClusterPeers())
	c.shutdown()
	if leader == nil {
		c.t.Fatalf("Expected a cluster leader and fully formed cluster, no leader")
	} else {
		c.t.Fatalf("Expected a fully formed cluster, only %d of %d peers seen", peersSeen, numPeersExpected)
	}
}

// Helper function to remove JetStream from a server.
func (c *cluster) removeJetStream(s *Server) {
	c.t.Helper()
	index := -1
	for i, cs := range c.servers {
		if cs == s {
			index = i
			break
		}
	}
	cf := c.opts[index].ConfigFile
	cb, _ := ioutil.ReadFile(cf)
	var sb strings.Builder
	for _, l := range strings.Split(string(cb), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "jetstream") {
			sb.WriteString(l + "\n")
		}
	}
	if err := ioutil.WriteFile(cf, []byte(sb.String()), 0644); err != nil {
		c.t.Fatalf("Error writing updated config file: %v", err)
	}
	if err := s.Reload(); err != nil {
		c.t.Fatalf("Error on server reload: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
}

func (c *cluster) stopAll() {
	c.t.Helper()
	for _, s := range c.servers {
		s.Shutdown()
	}
}

func (c *cluster) restartAll() {
	c.t.Helper()
	for i, s := range c.servers {
		if !s.Running() {
			opts := c.opts[i]
			s, o := RunServerWithConfig(opts.ConfigFile)
			c.servers[i] = s
			c.opts[i] = o
		}
	}
	c.waitOnClusterReady()
}

func (c *cluster) restartAllSamePorts() {
	c.t.Helper()
	for i, s := range c.servers {
		if !s.Running() {
			opts := c.opts[i]
			s := RunServer(opts)
			c.servers[i] = s
		}
	}
	c.waitOnClusterReady()
}

func (c *cluster) totalSubs() (total int) {
	c.t.Helper()
	for _, s := range c.servers {
		total += int(s.NumSubscriptions())
	}
	return total
}

func (c *cluster) stableTotalSubs() (total int) {
	nsubs := -1
	checkFor(c.t, 2*time.Second, 250*time.Millisecond, func() error {
		subs := c.totalSubs()
		if subs == nsubs {
			return nil
		}
		nsubs = subs
		return fmt.Errorf("Still stabilizing")
	})
	return nsubs

}

func TestJetStreamMirrorSourceLoop(t *testing.T) {
	test := func(t *testing.T, s *Server, replicas int) {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		// Create a source/mirror loop
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "1",
			Subjects: []string{"foo", "bar"},
			Replicas: replicas,
			Sources:  []*nats.StreamSource{{Name: "DECOY"}, {Name: "2"}},
		})
		require_NoError(t, err)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "DECOY",
			Subjects: []string{"baz"},
			Replicas: replicas,
			Sources:  []*nats.StreamSource{{Name: "NOTTHERE"}},
		})
		require_NoError(t, err)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "2",
			Replicas: replicas,
			Sources:  []*nats.StreamSource{{Name: "3"}},
		})
		require_NoError(t, err)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "3",
			Replicas: replicas,
			Sources:  []*nats.StreamSource{{Name: "1"}},
		})
		require_Error(t, err)
		require_Equal(t, err.Error(), "detected cycle")
	}

	t.Run("Single", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		if config := s.JetStreamConfig(); config != nil {
			defer removeDir(t, config.StoreDir)
		}
		defer s.Shutdown()
		test(t, s, 1)
	})
	t.Run("Clustered", func(t *testing.T) {
		c := createJetStreamClusterExplicit(t, "JSC", 5)
		defer c.shutdown()
		test(t, c.randomServer(), 2)
	})
}
