// Copyright 2020-2021 The NATS Authors
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

package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestJetStreamClusterConfig(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 16GB, max_file_store: 10TB, store_dir: "%s"}
		cluster { listen: 127.0.0.1:-1 }
	`))
	defer os.Remove(conf)

	check := func(errStr string) {
		t.Helper()
		opts, err := server.ProcessConfigFile(conf)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := server.NewServer(opts); err == nil || !strings.Contains(err.Error(), errStr) {
			t.Fatalf("Expected an error of `%s`, got `%v`", errStr, err)
		}
	}

	check("requires `server_name`")

	conf = createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: "TEST"
		jetstream: {max_mem_store: 16GB, max_file_store: 10TB, store_dir: "%s"}
		cluster { listen: 127.0.0.1:-1 }
	`))
	defer os.Remove(conf)

	check("requires `cluster.name`")
}

func TestJetStreamClusterLeader(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	// Kill our current leader and force an election.
	c.leader().Shutdown()
	c.waitOnClusterReady()

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

	if err := nc.PublishRequest(server.JSApiAccountInfo, reply, nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, sub, 1)
	resp, _ := sub.NextMsg(0)

	var info server.JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.JetStreamAccountStats == nil || info.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", info.Error)
	}
	// Make sure we only got 1 response.
	// Technicall this will always work since its a singelton service export.
	if nmsgs, _, _ := sub.Pending(); nmsgs > 0 {
		t.Fatalf("Expected only a single response, got %d more", nmsgs)
	}
}

func TestJetStreamClusterSingleReplicaStreams(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R1S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
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
}

func TestJetStreamClusterMultiReplicaStreams(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "RNS", 5)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// FIXME(dlc) - This should be default.
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

func TestJetStreamClusterCompaction(t *testing.T) {
	// This test takes a long time to observe compactions.
	// Once moved to server we can adjust and re-enable.
	skip(t)

	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 1000
	payload := []byte("Hello JSC")
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("TEST", payload); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	nc.Flush()

	if _, err := js.StreamInfo("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub, err := js.SubscribeSync("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	checkSubsPending(t, sub, toSend)

	for i := 0; i < toSend; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error getting msg %d: %v", i+1, err)
		}
		m.Ack()
	}
}

func TestJetStreamClusterDelete(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "RNS", 3)
	defer c.shutdown()

	s := c.randomServer()
	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	cfg := server.StreamConfig{
		Name:     "C22",
		Subjects: []string{"foo", "bar", "baz"},
		Replicas: 2,
		Storage:  server.FileStorage,
		MaxMsgs:  100,
	}
	req, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err := nc.Request(fmt.Sprintf(server.JSApiStreamCreateT, cfg.Name), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var scResp server.JSApiStreamCreateResponse
	if err := json.Unmarshal(resp.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo == nil || scResp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", scResp.Error)
	}
	// Now create a consumer.
	obsReq := server.CreateConsumerRequest{
		Stream: cfg.Name,
		Config: server.ConsumerConfig{Durable: "dlc", AckPolicy: server.AckExplicit},
	}
	req, err = json.Marshal(obsReq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err = nc.Request(fmt.Sprintf(server.JSApiDurableCreateT, cfg.Name, "dlc"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var ccResp server.JSApiConsumerCreateResponse
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ccResp.ConsumerInfo == nil || ccResp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", ccResp.Error)
	}

	// Now delete the consumer.
	resp, _ = nc.Request(fmt.Sprintf(server.JSApiConsumerDeleteT, cfg.Name, "dlc"), nil, time.Second)
	var cdResp server.JSApiConsumerDeleteResponse
	if err = json.Unmarshal(resp.Data, &cdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !cdResp.Success || cdResp.Error != nil {
		t.Fatalf("Got a bad response %+v", cdResp)
	}

	// Now delete the stream.
	resp, _ = nc.Request(fmt.Sprintf(server.JSApiStreamDeleteT, cfg.Name), nil, time.Second)
	var dResp server.JSApiStreamDeleteResponse
	if err = json.Unmarshal(resp.Data, &dResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !dResp.Success || dResp.Error != nil {
		t.Fatalf("Got a bad response %+v", dResp.Error)
	}

	// This will get the current information about usage and limits for this account.
	resp, err = nc.Request(server.JSApiAccountInfo, nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var info server.JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Streams != 0 {
		t.Fatalf("Expected no remaining streams, got %d", info.Streams)
	}
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

func TestJetStreamClusterConsumerState(t *testing.T) {
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

	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"), nats.Pull(1))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, 1)

	// Pull 5 messages and ack.
	for i := 0; i < 5; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error getting msg %d: %v", i+1, err)
		}
		m.Ack()
	}

	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}
	if ci.AckFloor.Consumer != 5 {
		t.Fatalf("Expected ack floor of %d, got %d", 5, ci.AckFloor.Consumer)
	}

	c.consumerLeader("$G", "TEST", "dlc").Shutdown()
	c.waitOnNewConsumerLeader("$G", "TEST", "dlc")

	nci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}

	if nci.Delivered != ci.Delivered {
		t.Fatalf("Consumer delivered did not match after leader switch, wanted %+v, got %+v", ci.Delivered, nci.Delivered)
	}
	if nci.AckFloor != ci.AckFloor {
		t.Fatalf("Consumer ackfloor did not match after leader switch, wanted %+v, got %+v", ci.AckFloor, nci.AckFloor)
	}

	// Now make sure we can receive new messages.
	// Pull last 5.
	for i := 0; i < 5; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error getting msg %d: %v", i+1, err)
		}
		m.Ack()
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

	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"), nats.Pull(1))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, 1)

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
	time.Sleep(250 * time.Millisecond)

	rs = c.restartServer(rs)
	c.checkClusterFormed()
	c.waitOnServerCurrent(rs)

	rs.Shutdown()
	c.waitOnLeader()

	for i := 0; i < numStreams; i++ {
		sn := fmt.Sprintf("T-%d", i+1)
		resp, _ := nc.Request(fmt.Sprintf(server.JSApiStreamDeleteT, sn), nil, time.Second)
		var dResp server.JSApiStreamDeleteResponse
		if err := json.Unmarshal(resp.Data, &dResp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !dResp.Success || dResp.Error != nil {
			t.Fatalf("Got a bad response %+v", dResp.Error)
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

	// Shut it down.
	rs.Shutdown()
	time.Sleep(250 * time.Millisecond)

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
	resp, _ := nc.Request(fmt.Sprintf(server.JSApiStreamDeleteT, "S2"), nil, time.Second)
	var dResp server.JSApiStreamDeleteResponse
	if err := json.Unmarshal(resp.Data, &dResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !dResp.Success || dResp.Error != nil {
		t.Fatalf("Got a bad response %+v", dResp.Error)
	}
	// Delete the consumer on S1 but add another.
	resp, _ = nc.Request(fmt.Sprintf(server.JSApiConsumerDeleteT, "S1", "S1C1"), nil, time.Second)
	var cdResp server.JSApiConsumerDeleteResponse
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

	c.leader().JetStreamSnapshotMeta()
	time.Sleep(250 * time.Millisecond)

	rs = c.restartServer(rs)
	c.checkClusterFormed()
	c.waitOnServerCurrent(rs)
}

func TestJetStreamClusterStreamSynchedTimeStamps(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 5)
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

	// Grab the message and timestamp from our current leader
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	m, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	meta, _ := m.MetaData()

	sub.Unsubscribe()

	sl := c.streamLeader("$G", "foo")

	sl.Shutdown()
	c.waitOnNewStreamLeader("$G", "foo")

	sub, err = js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	m, err = sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	meta2, _ := m.MetaData()
	if meta.Timestamp != meta2.Timestamp {
		t.Fatalf("Expected same timestamps, got %v vs %v", meta.Timestamp, meta2.Timestamp)
	}
}

func TestJetStreamClusterStreamOverlapSubjects(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R32", 2)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST2", Subjects: []string{"foo"}, Replicas: 2}); err == nil || err == nats.ErrTimeout {
		t.Fatalf("Expected error but got none or timeout")
	}

	// Now grab list of streams and make sure the second is not there.
	resp, err := nc.Request(server.JSApiStreams, nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var streams server.JSApiStreamNamesResponse
	if err = json.Unmarshal(resp.Data, &streams); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(streams.Streams) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(streams.Streams))
	}

	// Now do detailed version.
	resp, err = nc.Request(server.JSApiStreamList, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var listResponse server.JSApiStreamListResponse
	if err = json.Unmarshal(resp.Data, &listResponse); err != nil {
		t.Fatalf("Unexpected error: %v", err)
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
	sl := js.NewStreamLister()
	if !sl.Next() {
		t.Fatalf("Unexpected error: %v", sl.Err())
	}
	p := sl.Page()
	if len(p) != 3 {
		t.Fatalf("StreamInfo expected 3 results, got %d", len(p))
	}
	for _, si := range p {
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
		sub, err := js.SubscribeSync("TEST", nats.Durable(name), nats.Pull(2))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		checkSubsPending(t, sub, 2)
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
		for i := 0; i < ss.fetch; i++ {
			if m, err := ss.sub.NextMsg(time.Second); err != nil {
				t.Fatalf("Unexpected error getting message %d: %v", i, err)
			} else if i < ss.ack {
				m.Ack()
			}
		}
	}

	// Now get the consumer list info.
	cl := js.NewConsumerLister("TEST")
	if !cl.Next() {
		t.Fatalf("Unexpected error: %v", cl.Err())
	}
	p := cl.Page()
	if len(p) != 3 {
		t.Fatalf("ConsumerInfo expected 3 results, got %d", len(p))
	}
	for _, ci := range p {
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
		Discard:  server.DiscardNew,
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

	// Now update MaxMsgs, select non-leader server.
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
	if err := nc.PublishRequest(fmt.Sprintf(server.JSApiStreamUpdateT, "TEST"), rsub.Subject, req); err != nil {
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

	var scResp server.JSApiStreamCreateResponse
	if err := json.Unmarshal(m.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo != nil || scResp.Error == nil {
		t.Fatalf("Did not receive correct response: %+v", scResp)
	}
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
	// Check double add fails.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err == nil || err == nats.ErrTimeout {
		t.Fatalf("Expected error but got none or timeout")
	}

	// Do Consumers too.
	cfg := &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}
	if _, err := js.AddConsumer("TEST", cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Check double add fails.
	if _, err := js.AddConsumer("TEST", cfg); err == nil || err == nats.ErrTimeout {
		t.Fatalf("Expected error but got none or timeout")
	}
}

func TestJetStreamClusterStreamNormalCatchup(t *testing.T) {
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

	toSend := 10
	for i := 1; i <= toSend; i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sl := c.streamLeader("$G", "TEST")
	sl.Shutdown()
	c.waitOnNewStreamLeader("$G", "TEST")

	// Send 10 more while one replica offline.
	for i := toSend; i <= toSend*2; i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Delete the first from the second batch.
	dreq := server.JSApiMsgDeleteRequest{Seq: uint64(toSend)}
	dreqj, err := json.Marshal(dreq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, _ := nc.Request(fmt.Sprintf(server.JSApiMsgDeleteT, "TEST"), dreqj, time.Second)
	var delMsgResp server.JSApiMsgDeleteResponse
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
	c.waitOnNewStreamLeader("$G", "TEST")

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
	c.waitOnNewStreamLeader("$G", "TEST")

	toSend := 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	nsl := c.streamLeader("$G", "TEST")
	nsl.JetStreamSnapshotStream("$G", "TEST")

	time.Sleep(200 * time.Millisecond)

	sl = c.restartServer(sl)
	c.checkClusterFormed()

	// Now purge the stream while we are recovering.
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}

	c.waitOnServerCurrent(sl)
	c.waitOnStreamCurrent(sl, "$G", "TEST")

	nsl.Shutdown()
	c.waitOnNewStreamLeader("$G", "TEST")

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

	// TODO(dlc) - Change over to Go client version once it is updated.
	resp, err := nc.Request(fmt.Sprintf(server.JSApiStreamInfoT, "TEST"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var si server.StreamInfo
	if err = json.Unmarshal(resp.Data, &si); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected cluster info")
	}

	if si.Cluster.Name != c.name {
		t.Fatalf("Expected cluster name of %q, got %q", c.name, si.Cluster.Name)
	}

	leader := c.streamLeader("$G", "TEST").Name()
	if si.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, si.Cluster.Leader)
	}
	if len(si.Cluster.Replicas) != 2 {
		t.Fatalf("Expected %d replicas, got %d", 2, len(si.Cluster.Replicas))
	}
	for _, peer := range si.Cluster.Replicas {
		if !peer.Current {
			t.Fatalf("Expected replica to be current: %+v", peer)
		}
	}

	// Shutdown the leader.
	oldLeader := c.streamLeader("$G", "TEST")
	oldLeader.Shutdown()

	c.waitOnNewStreamLeader("$G", "TEST")

	// Re-request.
	resp, err = nc.Request(fmt.Sprintf(server.JSApiStreamInfoT, "TEST"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err = json.Unmarshal(resp.Data, &si); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected cluster info")
	}

	leader = c.streamLeader("$G", "TEST").Name()
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
	c.waitOnStreamCurrent(oldLeader, "$G", "TEST")

	// Re-request.
	resp, err = nc.Request(fmt.Sprintf(server.JSApiStreamInfoT, "TEST"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err = json.Unmarshal(resp.Data, &si); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected cluster info")
	}

	leader = c.streamLeader("$G", "TEST").Name()
	if si.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, si.Cluster.Leader)
	}
	if len(si.Cluster.Replicas) != 2 {
		t.Fatalf("Expected %d replicas, got %d", 2, len(si.Cluster.Replicas))
	}
	for _, peer := range si.Cluster.Replicas {
		if !peer.Current {
			t.Fatalf("Expected replica to be current: %+v", peer)
		}
	}

	// Now do consumer.
	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"), nats.Pull(10))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	checkSubsPending(t, sub, 10)

	resp, err = nc.Request(fmt.Sprintf(server.JSApiConsumerInfoT, "TEST", "dlc"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var ci server.ConsumerInfo
	if err = json.Unmarshal(resp.Data, &ci); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	leader = c.consumerLeader("$G", "TEST", "dlc").Name()
	if ci.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, ci.Cluster.Leader)
	}
	if len(ci.Cluster.Replicas) != 2 {
		t.Fatalf("Expected %d replicas, got %d", 2, len(ci.Cluster.Replicas))
	}
	for _, peer := range ci.Cluster.Replicas {
		if !peer.Current {
			t.Fatalf("Expected replica to be current: %+v", peer)
		}
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
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 500
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sreq := &server.JSApiStreamSnapshotRequest{
		DeliverSubject: nats.NewInbox(),
		ChunkSize:      512,
	}

	req, _ := json.Marshal(sreq)
	rmsg, err := nc.Request(fmt.Sprintf(server.JSApiStreamSnapshotT, "TEST"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}

	var resp server.JSApiStreamSnapshotResponse
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

	var rresp server.JSApiStreamRestoreResponse
	rreq := &server.JSApiStreamRestoreRequest{
		Config: config,
		State:  state,
	}
	req, _ = json.Marshal(rreq)

	// Make sure a restore to an existing stream fails.
	rmsg, err = nc.Request(fmt.Sprintf(server.JSApiStreamRestoreT, "TEST"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error == nil || rresp.Error.Code != 500 || !strings.Contains(rresp.Error.Description, "already in use") {
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

	// This should work properly.
	rmsg, err = nc.Request(fmt.Sprintf(server.JSApiStreamRestoreT, "TEST"), req, 5*time.Second)
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
	var chunk [512]byte
	for r := bytes.NewReader(snapshot); ; {
		n, err := r.Read(chunk[:])
		if err != nil {
			break
		}
		nc.Request(rresp.DeliverSubject, chunk[:n], time.Second)
	}
	nc.Request(rresp.DeliverSubject, nil, time.Second)

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" || si.State.Msgs != uint64(toSend) {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}

	getExtendedStreamInfo := func() *server.StreamInfo {
		t.Helper()
		resp, err := nc.Request(fmt.Sprintf(server.JSApiStreamInfoT, "TEST"), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var si server.StreamInfo
		if err = json.Unmarshal(resp.Data, &si); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.Cluster == nil {
			t.Fatalf("Expected cluster info")
		}
		return &si
	}

	// Make sure the replicas become current eventually. They will be doing catchup.
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		si := getExtendedStreamInfo()
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

	numConnections := 2
	var conns []nats.JetStream
	for i := 0; i < numConnections; i++ {
		s := c.randomServer()
		_, js := jsClientConnect(t, s)
		conns = append(conns, js)
	}

	toSend := 100000
	numProducers := 10

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
	time.Sleep(200 * time.Millisecond)

	start := time.Now()
	close(startCh)
	wg.Wait()

	tt := time.Since(start)
	fmt.Printf("Took %v to send %d msgs with %d producers and R=3!\n", tt, toSend, numProducers)
	fmt.Printf("%.0f msgs/sec\n\n", float64(toSend)/tt.Seconds())
}

// Support functions

var jsClusterTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 16GB, max_file_store: 10TB, store_dir: "%s"}
	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}
`

// This will create a cluster that is explicitly configured for the routes, etc.
// and also has a defined clustername. All configs for routes and cluster name will be the same.
func createJetStreamClusterExplicit(t *testing.T, clusterName string, numServers int) *cluster {
	t.Helper()
	if clusterName == "" || numServers < 1 {
		t.Fatalf("Bad params")
	}
	const startClusterPort = 22332

	// Build out the routes that will be shared with all configs.
	var routes []string
	for cp := startClusterPort; cp < startClusterPort+numServers; cp++ {
		routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", cp))
	}
	routeConfig := strings.Join(routes, ",")

	// Go ahead and build configurations and start servers.
	c := &cluster{servers: make([]*server.Server, 0, numServers), opts: make([]*server.Options, 0, numServers), name: clusterName}

	for cp := startClusterPort; cp < startClusterPort+numServers; cp++ {
		storeDir, _ := ioutil.TempDir("", server.JetStreamStoreDir)
		sn := fmt.Sprintf("S-%d", cp-startClusterPort+1)
		conf := fmt.Sprintf(jsClusterTempl, sn, storeDir, clusterName, cp, routeConfig)
		s, o := RunServerWithConfig(createConfFile(t, []byte(conf)))
		c.servers = append(c.servers, s)
		c.opts = append(c.opts, o)
	}
	c.t = t

	// Wait til we are formed and have a leader.
	c.checkClusterFormed()
	c.waitOnClusterReady()

	return c
}

func (c *cluster) addInNewServer() *server.Server {
	c.t.Helper()
	sn := fmt.Sprintf("S-%d", len(c.servers)+1)
	storeDir, _ := ioutil.TempDir("", server.JetStreamStoreDir)
	seedRoute := fmt.Sprintf("nats-route://127.0.0.1:%d", c.opts[0].Cluster.Port)
	conf := fmt.Sprintf(jsClusterTempl, sn, storeDir, c.name, -1, seedRoute)
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)
	c.checkClusterFormed()
	return s
}

// Hack for staticcheck
var skip = func(t *testing.T) {
	t.SkipNow()
}

func jsClientConnect(t *testing.T, s *server.Server) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	js, err := nc.JetStream(nats.MaxWait(5 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func checkSubsPending(t *testing.T, sub *nats.Subscription, numExpected int) {
	t.Helper()
	checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, err := sub.Pending(); err != nil || nmsgs != numExpected {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
		}
		return nil
	})
}

func (c *cluster) restartServer(rs *server.Server) *server.Server {
	c.t.Helper()
	index := -1
	var opts *server.Options
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
	expires := time.Now().Add(5 * time.Second)
	for time.Now().Before(expires) {
		peers := leader.JetStreamClusterPeers()
		if len(peers) == n {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("Expected a cluster peer count of %d, got %d", n, len(leader.JetStreamClusterPeers()))
}

func (c *cluster) waitOnNewConsumerLeader(account, stream, consumer string) {
	c.t.Helper()
	expires := time.Now().Add(5 * time.Second)
	for time.Now().Before(expires) {
		if leader := c.consumerLeader(account, stream, consumer); leader != nil {
			time.Sleep(25 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("Expected a consumer leader for %q %q %q, got none", account, stream, consumer)
}

func (c *cluster) consumerLeader(account, stream, consumer string) *server.Server {
	c.t.Helper()
	for _, s := range c.servers {
		if s.JetStreamIsConsumerLeader(account, stream, consumer) {
			return s
		}
	}
	return nil
}

func (c *cluster) waitOnNewStreamLeader(account, stream string) {
	c.t.Helper()
	expires := time.Now().Add(5 * time.Second)
	for time.Now().Before(expires) {
		if leader := c.streamLeader(account, stream); leader != nil {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	c.t.Fatalf("Expected a stream leader for %q %q, got none", account, stream)
}

func (c *cluster) randomNonStreamLeader(account, stream string) *server.Server {
	c.t.Helper()
	for _, s := range c.servers {
		if s.JetStreamIsStreamAssigned(account, stream) && !s.JetStreamIsStreamLeader(account, stream) {
			return s
		}
	}
	return nil
}

func (c *cluster) streamLeader(account, stream string) *server.Server {
	c.t.Helper()
	for _, s := range c.servers {
		if s.JetStreamIsStreamLeader(account, stream) {
			return s
		}
	}
	return nil
}

func (c *cluster) waitOnStreamCurrent(s *server.Server, account, stream string) {
	c.t.Helper()
	expires := time.Now().Add(10 * time.Second)
	for time.Now().Before(expires) {
		if s.JetStreamIsStreamCurrent(account, stream) {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	c.t.Fatalf("Expected server %q to eventually be current for stream %q", s, stream)
}

func (c *cluster) waitOnServerCurrent(s *server.Server) {
	c.t.Helper()
	expires := time.Now().Add(5 * time.Second)
	for time.Now().Before(expires) {
		if s.JetStreamIsCurrent() {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	c.t.Fatalf("Expected server %q to eventually be current", s)
}

func (c *cluster) randomNonLeader() *server.Server {
	// range should randomize.. but..
	for _, s := range c.servers {
		if !s.JetStreamIsLeader() {
			return s
		}
	}
	return nil
}

func (c *cluster) leader() *server.Server {
	for _, s := range c.servers {
		if s.JetStreamIsLeader() {
			return s
		}
	}
	return nil
}

// This needs to match raft.go:minElectionTimeout*2
const maxElectionTimeout = 550 * time.Millisecond

func (c *cluster) expectNoLeader() {
	c.t.Helper()
	expires := time.Now().Add(maxElectionTimeout)
	for time.Now().Before(expires) {
		if c.leader() != nil {
			c.t.Fatalf("Expected no leader but have one")
		}
	}
}

func (c *cluster) waitOnLeader() {
	c.t.Helper()
	expires := time.Now().Add(5 * time.Second)
	for time.Now().Before(expires) {
		if leader := c.leader(); leader != nil {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	c.t.Fatalf("Expected a cluster leader, got none")
}

// Helper function to check that a cluster is formed
func (c *cluster) waitOnClusterReady() {
	c.t.Helper()
	var leader *server.Server
	expires := time.Now().Add(10 * time.Second)
	for time.Now().Before(expires) {
		if leader = c.leader(); leader != nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	// Now make sure we have all peers.
	for leader != nil && time.Now().Before(expires) {
		if len(leader.JetStreamClusterPeers()) == len(c.servers) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("Expected a cluster leader and fully formed cluster")
}
