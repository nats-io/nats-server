// Copyright 2022 The NATS Authors
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

//go:build !skip_js_tests && !skip_js_cluster_tests_3
// +build !skip_js_tests,!skip_js_cluster_tests_3

package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamClusterRemovePeerByID(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Wait for a leader
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Get the name of the one that is not restarted
	srvName := c.opts[2].ServerName
	// And its node ID
	peerID := c.servers[2].Node()

	nc.Close()
	// Now stop the whole cluster
	c.stopAll()
	// Restart all but one
	for i := 0; i < 2; i++ {
		opts := c.opts[i]
		s, o := RunServerWithConfig(opts.ConfigFile)
		c.servers[i] = s
		c.opts[i] = o
	}

	c.waitOnClusterReadyWithNumPeers(2)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Now attempt to remove by name, this should fail because the cluster
	// was restarted and names are not persisted.
	ml := c.leader()
	nc, err = nats.Connect(ml.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	req := &JSApiMetaServerRemoveRequest{Server: srvName}
	jsreq, err := json.Marshal(req)
	require_NoError(t, err)
	rmsg, err := nc.Request(JSApiRemoveServer, jsreq, 2*time.Second)
	require_NoError(t, err)

	var resp JSApiMetaServerRemoveResponse
	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	require_True(t, resp.Error != nil)
	require_True(t, IsNatsErr(resp.Error, JSClusterServerNotMemberErr))

	// Now try by ID, but first with an ID that does not match any peerID
	req.Peer = "some_bad_id"
	jsreq, err = json.Marshal(req)
	require_NoError(t, err)
	rmsg, err = nc.Request(JSApiRemoveServer, jsreq, 2*time.Second)
	require_NoError(t, err)

	resp = JSApiMetaServerRemoveResponse{}
	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	require_True(t, resp.Error != nil)
	require_True(t, IsNatsErr(resp.Error, JSClusterServerNotMemberErr))

	// Now with the proper peer ID
	req.Peer = peerID
	jsreq, err = json.Marshal(req)
	require_NoError(t, err)
	rmsg, err = nc.Request(JSApiRemoveServer, jsreq, 2*time.Second)
	require_NoError(t, err)

	resp = JSApiMetaServerRemoveResponse{}
	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	require_True(t, resp.Error == nil)
	require_True(t, resp.Success)
}

func TestJetStreamClusterDiscardNewAndMaxMsgsPerSubject(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client for API requests.
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for _, test := range []struct {
		name     string
		storage  StorageType
		replicas int
	}{
		{"MEM-R1", MemoryStorage, 1},
		{"FILE-R1", FileStorage, 1},
		{"MEM-R3", MemoryStorage, 3},
		{"FILE-R3", FileStorage, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			js.DeleteStream("KV")
			// Make sure setting new without DiscardPolicy also being new is error.
			cfg := &StreamConfig{
				Name:          "KV",
				Subjects:      []string{"KV.>"},
				Storage:       test.storage,
				AllowDirect:   true,
				DiscardNewPer: true,
				MaxMsgs:       10,
				Replicas:      test.replicas,
			}
			if _, apiErr := addStreamWithError(t, nc, cfg); apiErr == nil {
				t.Fatalf("Expected API error but got none")
			} else if apiErr.ErrCode != 10052 || !strings.Contains(apiErr.Description, "discard new per subject requires discard new policy") {
				t.Fatalf("Got wrong error: %+v", apiErr)
			}

			// Set broad discard new policy to engage DiscardNewPer
			cfg.Discard = DiscardNew
			// We should also error here since we have not setup max msgs per subject.
			if _, apiErr := addStreamWithError(t, nc, cfg); apiErr == nil {
				t.Fatalf("Expected API error but got none")
			} else if apiErr.ErrCode != 10052 || !strings.Contains(apiErr.Description, "discard new per subject requires max msgs per subject > 0") {
				t.Fatalf("Got wrong error: %+v", apiErr)
			}

			cfg.MaxMsgsPer = 1
			addStream(t, nc, cfg)

			// We want to test that we reject new messages on a per subject basis if the
			// max msgs per subject limit has been hit, even if other limits have not.
			_, err := js.Publish("KV.foo", nil)
			require_NoError(t, err)

			_, err = js.Publish("KV.foo", nil)
			// Go client does not have const for this one.
			require_Error(t, err, errors.New("nats: maximum messages per subject exceeded"))
		})
	}
}

func TestJetStreamClusterCreateConsumerWithReplicaOneGetsResponse(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	c.waitOnStreamLeader(globalAccountName, "TEST")

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C3",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	c.waitOnConsumerLeader(globalAccountName, "TEST", "C3")

	// Update to scale down to R1, that should work (get a response)
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C3",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	c.waitOnConsumerLeader(globalAccountName, "TEST", "C3")

	ci, err := js.ConsumerInfo("TEST", "C3")
	require_NoError(t, err)
	require_True(t, ci.Config.Replicas == 1)
	require_True(t, len(ci.Cluster.Replicas) == 0)
}

func TestJetStreamClusterConsumerListPaging(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	cfg := &nats.ConsumerConfig{
		Replicas:      1,
		MemoryStorage: true,
		AckPolicy:     nats.AckExplicitPolicy,
	}

	// create 3000 consumers.
	numConsumers := 3000
	for i := 1; i <= numConsumers; i++ {
		cfg.Durable = fmt.Sprintf("d-%.4d", i)
		_, err := js.AddConsumer("TEST", cfg)
		require_NoError(t, err)
	}

	// Test both names and list operations.

	// Names
	reqSubj := fmt.Sprintf(JSApiConsumersT, "TEST")
	grabConsumerNames := func(offset int) []string {
		req := fmt.Sprintf(`{"offset":%d}`, offset)
		respMsg, err := nc.Request(reqSubj, []byte(req), time.Second)
		require_NoError(t, err)
		var resp JSApiConsumerNamesResponse
		err = json.Unmarshal(respMsg.Data, &resp)
		require_NoError(t, err)
		// Sanity check that we are actually paging properly around limits.
		if resp.Limit < len(resp.Consumers) {
			t.Fatalf("Expected total limited to %d but got %d", resp.Limit, len(resp.Consumers))
		}
		return resp.Consumers
	}

	results := make(map[string]bool)

	for offset := 0; len(results) < numConsumers; {
		consumers := grabConsumerNames(offset)
		offset += len(consumers)
		for _, name := range consumers {
			if results[name] {
				t.Fatalf("Found duplicate %q", name)
			}
			results[name] = true
		}
	}

	// List
	reqSubj = fmt.Sprintf(JSApiConsumerListT, "TEST")
	grabConsumerList := func(offset int) []*ConsumerInfo {
		req := fmt.Sprintf(`{"offset":%d}`, offset)
		respMsg, err := nc.Request(reqSubj, []byte(req), time.Second)
		require_NoError(t, err)
		var resp JSApiConsumerListResponse
		err = json.Unmarshal(respMsg.Data, &resp)
		require_NoError(t, err)
		// Sanity check that we are actually paging properly around limits.
		if resp.Limit < len(resp.Consumers) {
			t.Fatalf("Expected total limited to %d but got %d", resp.Limit, len(resp.Consumers))
		}
		return resp.Consumers
	}

	results = make(map[string]bool)

	for offset := 0; len(results) < numConsumers; {
		consumers := grabConsumerList(offset)
		offset += len(consumers)
		for _, ci := range consumers {
			name := ci.Config.Durable
			if results[name] {
				t.Fatalf("Found duplicate %q", name)
			}
			results[name] = true
		}
	}
}

func TestJetStreamClusterMetaRecoveryLogic(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 1,
	})
	require_NoError(t, err)

	err = js.DeleteStream("TEST")
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	err = js.DeleteStream("TEST")
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"baz"},
		Replicas: 1,
	})
	require_NoError(t, err)

	osi, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	c.stopAll()
	c.restartAll()
	c.waitOnLeader()
	c.waitOnStreamLeader("$G", "TEST")

	s = c.randomNonLeader()
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	if !reflect.DeepEqual(si.Config, osi.Config) {
		t.Fatalf("Expected %+v, but got %+v", osi.Config, si.Config)
	}
}

func TestJetStreamClusterDeleteConsumerWhileServerDown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomNonLeader())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "DC",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	s := c.randomNonConsumerLeader("$G", "TEST", "DC")
	s.Shutdown()

	c.waitOnLeader()                                 // In case that was metaleader.
	nc, js = jsClientConnect(t, c.randomNonLeader()) // In case we were connected there.
	defer nc.Close()

	err = js.DeleteConsumer("TEST", "DC")
	require_NoError(t, err)

	// Restart.
	s = c.restartServer(s)
	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		hs := s.healthz(&HealthzOptions{
			JSEnabled:    true,
			JSServerOnly: false,
		})
		if hs.Error != _EMPTY_ {
			return errors.New(hs.Error)
		}
		return nil
	})

	// Make sure we can not see it on the server that was down at the time of delete.
	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	if o := mset.lookupConsumer("DC"); o != nil {
		t.Fatalf("Expected to not find consumer, but did")
	}

	// Now repeat but force a meta snapshot.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "DC",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	s = c.randomNonConsumerLeader("$G", "TEST", "DC")
	s.Shutdown()

	c.waitOnLeader()                                 // In case that was metaleader.
	nc, js = jsClientConnect(t, c.randomNonLeader()) // In case we were connected there.
	defer nc.Close()

	err = js.DeleteConsumer("TEST", "DC")
	require_NoError(t, err)

	err = c.leader().JetStreamSnapshotMeta()
	require_NoError(t, err)

	// Restart.
	s = c.restartServer(s)
	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		hs := s.healthz(&HealthzOptions{
			JSEnabled:    true,
			JSServerOnly: false,
		})
		if hs.Error != _EMPTY_ {
			return errors.New(hs.Error)
		}
		return nil
	})

	// Make sure we can not see it on the server that was down at the time of delete.
	mset, err = s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	if o := mset.lookupConsumer("DC"); o != nil {
		t.Fatalf("Expected to not find consumer, but did")
	}
}

func TestJetStreamClusterNegativeReplicas(t *testing.T) {
	s := RunBasicJetStreamServer()
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	testBadReplicas := func(t *testing.T, s *Server, name string) {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     name,
			Replicas: -1,
		})
		require_Error(t, err, NewJSReplicasCountCannotBeNegativeError())

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     name,
			Replicas: 1,
		})
		require_NoError(t, err)

		// Check upadte now.
		_, err = js.UpdateStream(&nats.StreamConfig{
			Name:     name,
			Replicas: -11,
		})
		require_Error(t, err, NewJSReplicasCountCannotBeNegativeError())

		// Now same for consumers
		durName := fmt.Sprintf("%s_dur", name)
		_, err = js.AddConsumer(name, &nats.ConsumerConfig{
			Durable:  durName,
			Replicas: -1,
		})
		require_Error(t, err, NewJSReplicasCountCannotBeNegativeError())

		_, err = js.AddConsumer(name, &nats.ConsumerConfig{
			Durable:  durName,
			Replicas: 1,
		})
		require_NoError(t, err)

		// Check update now
		_, err = js.UpdateConsumer(name, &nats.ConsumerConfig{
			Durable:  durName,
			Replicas: -11,
		})
		require_Error(t, err, NewJSReplicasCountCannotBeNegativeError())
	}

	t.Run("Standalone", func(t *testing.T) { testBadReplicas(t, s, "TEST1") })
	t.Run("Clustered", func(t *testing.T) { testBadReplicas(t, c.randomServer(), "TEST2") })
}

func TestJetStreamClusterUserGivenConsName(t *testing.T) {
	s := RunBasicJetStreamServer()
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	test := func(t *testing.T, s *Server, stream string, replicas int, cons string) {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     stream,
			Replicas: replicas,
		})
		require_NoError(t, err)

		cc := &CreateConsumerRequest{
			Stream: stream,
			Config: ConsumerConfig{
				Name:              cons,
				FilterSubject:     stream,
				InactiveThreshold: 10 * time.Second,
			},
		}
		subj := fmt.Sprintf(JSApiConsumerCreateExT, stream, cons, stream)
		req, err := json.Marshal(cc)
		require_NoError(t, err)

		reply, err := nc.Request(subj, req, 2*time.Second)
		require_NoError(t, err)

		var cresp JSApiConsumerCreateResponse
		json.Unmarshal(reply.Data, &cresp)
		if cresp.Error != nil {
			t.Fatalf("Unexpected error: %v", cresp.Error)
		}
		require_Equal(t, cresp.Name, cons)
		require_Equal(t, cresp.Config.Name, cons)

		// Resend the add request but before change something that the server
		// should reject since the consumer already exist and we don't support
		// the update of the consumer that way.
		cc.Config.DeliverPolicy = DeliverNew
		req, err = json.Marshal(cc)
		require_NoError(t, err)
		reply, err = nc.Request(subj, req, 2*time.Second)
		require_NoError(t, err)

		cresp = JSApiConsumerCreateResponse{}
		json.Unmarshal(reply.Data, &cresp)
		require_Error(t, cresp.Error, NewJSConsumerCreateError(errors.New("deliver policy can not be updated")))
	}

	t.Run("Standalone", func(t *testing.T) { test(t, s, "TEST", 1, "cons") })
	t.Run("Clustered R1", func(t *testing.T) { test(t, c.randomServer(), "TEST2", 1, "cons2") })
	t.Run("Clustered R3", func(t *testing.T) { test(t, c.randomServer(), "TEST3", 3, "cons3") })
}

func TestJetStreamClusterUserGivenConsNameWithLeaderChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	c.waitOnStreamLeader(globalAccountName, "TEST")
	for i := 0; i < 100; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	consName := "myephemeral"
	cc := &CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			Name:              consName,
			FilterSubject:     "foo",
			InactiveThreshold: time.Hour,
			Replicas:          3,
		},
	}
	subj := fmt.Sprintf(JSApiConsumerCreateExT, "TEST", consName, "foo")
	req, err := json.Marshal(cc)
	require_NoError(t, err)

	reply, err := nc.Request(subj, req, 2*time.Second)
	require_NoError(t, err)

	var cresp JSApiConsumerCreateResponse
	json.Unmarshal(reply.Data, &cresp)
	if cresp.Error != nil {
		t.Fatalf("Unexpected error: %v", cresp.Error)
	}
	require_Equal(t, cresp.Name, consName)
	require_Equal(t, cresp.Config.Name, consName)

	// Consumer leader name
	clname := cresp.ConsumerInfo.Cluster.Leader

	nreq := &JSApiConsumerGetNextRequest{Batch: 1, Expires: time.Second}
	req, err = json.Marshal(nreq)
	require_NoError(t, err)

	sub := natsSubSync(t, nc, "xxx")
	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", consName)
	err = nc.PublishRequest(rsubj, "xxx", req)
	require_NoError(t, err)

	msg := natsNexMsg(t, sub, time.Second)
	require_Equal(t, string(msg.Data), "msg")

	// Shutdown the consumer leader
	cl := c.serverByName(clname)
	cl.Shutdown()

	// Wait for a bit to be sure that we lost leadership
	time.Sleep(250 * time.Millisecond)

	// Wait for new leader
	c.waitOnStreamLeader(globalAccountName, "TEST")
	c.waitOnConsumerLeader(globalAccountName, "TEST", consName)

	// Make sure we can still consume.
	for i := 0; i < 2; i++ {
		err = nc.PublishRequest(rsubj, "xxx", req)
		require_NoError(t, err)

		msg = natsNexMsg(t, sub, time.Second)
		if len(msg.Data) == 0 {
			continue
		}
		require_Equal(t, string(msg.Data), "msg")
		return
	}
	t.Fatal("Did not receive message")
}

func TestJetStreamClusterMirrorCrossDomainOnLeadnodeNoSystemShare(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: HUB, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "CORE", _EMPTY_, 3, 18033, true)
	defer c.shutdown()

	tmpl = strings.Replace(jsClusterTemplWithSingleLeafNode, "store_dir:", "domain: SPOKE, store_dir:", 1)
	ln := c.createLeafNodeWithTemplateNoSystem("LN-SPOKE", tmpl)
	defer ln.Shutdown()

	checkLeafNodeConnectedCount(t, ln, 1)

	// Create origin stream in hub.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo"},
		MaxMsgsPerSubject: 10,
		AllowDirect:       true,
	})
	require_NoError(t, err)

	// Now create the mirror on the leafnode.
	lnc, ljs := jsClientConnect(t, ln)
	defer lnc.Close()

	_, err = ljs.AddStream(&nats.StreamConfig{
		Name:              "M",
		MaxMsgsPerSubject: 10,
		AllowDirect:       true,
		MirrorDirect:      true,
		Mirror: &nats.StreamSource{
			Name: "TEST",
			External: &nats.ExternalStream{
				APIPrefix: "$JS.HUB.API",
			},
		},
	})
	require_NoError(t, err)

	// Publish to the hub stream and make sure the mirror gets those messages.
	for i := 0; i < 20; i++ {
		js.Publish("foo", nil)
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 10)

	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		si, err := ljs.StreamInfo("M")
		require_NoError(t, err)
		if si.State.Msgs == 10 {
			return nil
		}
		return fmt.Errorf("State not current: %+v", si.State)
	})
}

func TestJetStreamClusterFirstSeqMismatch(t *testing.T) {
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "C", 3,
		func(serverName, clusterName, storeDir, conf string) string {
			tf := createFile(t, "")
			logName := tf.Name()
			tf.Close()
			return fmt.Sprintf("%s\nlogfile: '%s'", conf, logName)
		})
	defer c.shutdown()

	rs := c.randomServer()
	nc, js := jsClientConnect(t, rs)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
		MaxAge:   2 * time.Second,
	})
	require_NoError(t, err)

	c.waitOnStreamLeader(globalAccountName, "TEST")

	mset, err := c.streamLeader(globalAccountName, "TEST").GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	node := mset.raftNode()

	nl := c.randomNonStreamLeader(globalAccountName, "TEST")
	if rs == nl {
		nc.Close()
		for _, s := range c.servers {
			if s != nl {
				nc, _ = jsClientConnect(t, s)
				defer nc.Close()
				break
			}
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	ch := make(chan struct{})
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			sendStreamMsg(t, nc, "foo", "msg")
			select {
			case <-ch:
				return
			default:
			}
		}
	}()

	time.Sleep(2500 * time.Millisecond)
	nl.Shutdown()

	time.Sleep(500 * time.Millisecond)
	node.InstallSnapshot(mset.stateSnapshot())
	time.Sleep(3500 * time.Millisecond)

	c.restartServer(nl)
	c.waitOnAllCurrent()

	close(ch)
	wg.Wait()

	log := nl.getOpts().LogFile
	nl.Shutdown()

	content, err := os.ReadFile(log)
	require_NoError(t, err)
	if bytes.Contains(content, []byte(errFirstSequenceMismatch.Error())) {
		t.Fatalf("First sequence mismatch occurred!")
	}
}

func TestJetStreamClusterConsumerInactiveThreshold(t *testing.T) {
	// Create a standalone, a cluster, and a super cluster

	s := RunBasicJetStreamServer()
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	test := func(t *testing.T, c *cluster, s *Server, replicas int) {
		if c != nil {
			s = c.randomServer()
		}
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		sname := fmt.Sprintf("TEST%d", replicas)
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     sname,
			Subjects: []string{sname},
			Replicas: replicas,
		})
		require_NoError(t, err)

		if c != nil {
			c.waitOnStreamLeader(globalAccountName, sname)
		}

		for i := 0; i < 10; i++ {
			js.PublishAsync(sname, []byte("ok"))
		}
		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive completion signal")
		}

		waitOnCleanup := func(ci *nats.ConsumerInfo) {
			t.Helper()
			checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
				_, err := js.ConsumerInfo(ci.Stream, ci.Name)
				if err == nil {
					return fmt.Errorf("Consumer still present")
				}
				return nil
			})
		}

		// Test to make sure inactive threshold is enforced for all types.
		// Ephemeral and Durable, both push and pull.

		// Ephemeral Push (no bind to deliver subject)
		ci, err := js.AddConsumer(sname, &nats.ConsumerConfig{
			DeliverSubject:    "_no_bind_",
			InactiveThreshold: 50 * time.Millisecond,
		})
		require_NoError(t, err)
		waitOnCleanup(ci)

		// Ephemeral Pull
		ci, err = js.AddConsumer(sname, &nats.ConsumerConfig{
			AckPolicy:         nats.AckExplicitPolicy,
			InactiveThreshold: 50 * time.Millisecond,
		})
		require_NoError(t, err)
		waitOnCleanup(ci)

		// Support InactiveThresholds for Durables as well.

		// Durable Push (no bind to deliver subject)
		ci, err = js.AddConsumer(sname, &nats.ConsumerConfig{
			Durable:           "d1",
			DeliverSubject:    "_no_bind_",
			InactiveThreshold: 50 * time.Millisecond,
		})
		require_NoError(t, err)
		waitOnCleanup(ci)

		// Durable Push (no bind to deliver subject) with an activity
		// threshold set after creation
		ci, err = js.AddConsumer(sname, &nats.ConsumerConfig{
			Durable:        "d2",
			DeliverSubject: "_no_bind_",
		})
		require_NoError(t, err)
		if c != nil {
			c.waitOnConsumerLeader(globalAccountName, sname, "d2")
		}
		_, err = js.UpdateConsumer(sname, &nats.ConsumerConfig{
			Durable:           "d2",
			DeliverSubject:    "_no_bind_",
			InactiveThreshold: 50 * time.Millisecond,
		})
		require_NoError(t, err)
		waitOnCleanup(ci)

		// Durable Pull
		ci, err = js.AddConsumer(sname, &nats.ConsumerConfig{
			Durable:           "d3",
			AckPolicy:         nats.AckExplicitPolicy,
			InactiveThreshold: 50 * time.Millisecond,
		})
		require_NoError(t, err)
		waitOnCleanup(ci)

		// Durable Pull with an inactivity threshold set after creation
		ci, err = js.AddConsumer(sname, &nats.ConsumerConfig{
			Durable:   "d4",
			AckPolicy: nats.AckExplicitPolicy,
		})
		require_NoError(t, err)
		if c != nil {
			c.waitOnConsumerLeader(globalAccountName, sname, "d4")
		}
		_, err = js.UpdateConsumer(sname, &nats.ConsumerConfig{
			Durable:           "d4",
			AckPolicy:         nats.AckExplicitPolicy,
			InactiveThreshold: 50 * time.Millisecond,
		})
		require_NoError(t, err)
		waitOnCleanup(ci)
	}

	t.Run("standalone", func(t *testing.T) { test(t, nil, s, 1) })
	t.Run("cluster-r1", func(t *testing.T) { test(t, c, nil, 1) })
	t.Run("cluster-r3", func(t *testing.T) { test(t, c, nil, 3) })
	t.Run("super-cluster-r1", func(t *testing.T) { test(t, sc.randomCluster(), nil, 1) })
	t.Run("super-cluster-r3", func(t *testing.T) { test(t, sc.randomCluster(), nil, 3) })
}

// To capture our false warnings for clustered stream lag.
type testStreamLagWarnLogger struct {
	DummyLogger
	ch chan string
}

func (l *testStreamLagWarnLogger) Warnf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "has high message lag") {
		select {
		case l.ch <- msg:
		default:
		}
	}
}

// False triggering warnings on stream lag because not offsetting by failures.
func TestJetStreamClusterStreamLagWarning(t *testing.T) {
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

	sl := c.streamLeader("$G", "TEST")

	l := &testStreamLagWarnLogger{ch: make(chan string, 10)}
	sl.SetLogger(l, false, false)

	// We only need to trigger post RAFT propose failures that increment mset.clfs.
	// Dedupe with msgIDs is one, so we will use that.
	m := nats.NewMsg("foo")
	m.Data = []byte("OK")
	m.Header.Set(JSMsgId, "zz")

	// Make sure we know we will trip the warning threshold.
	for i := 0; i < 2*streamLagWarnThreshold; i++ {
		js.PublishMsgAsync(m)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	select {
	case msg := <-l.ch:
		t.Fatalf("Unexpected msg lag warning seen: %s", msg)
	case <-time.After(100 * time.Millisecond):
		// OK
	}
}

// https://github.com/nats-io/nats-server/issues/3603
func TestJetStreamClusterSignalPullConsumersOnDelete(t *testing.T) {
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

	// Create 2 pull consumers.
	sub1, err := js.PullSubscribe("foo", "d1")
	require_NoError(t, err)

	sub2, err := js.PullSubscribe("foo", "d2")
	require_NoError(t, err)

	// We want to make sure we get kicked out prior to the timeout
	// when consumers are being deleted or the parent stream is being deleted.
	// Note this should be lower case, Go client needs to be updated.
	expectedErr := errors.New("nats: Consumer Deleted")

	// Queue up the delete for sub1
	time.AfterFunc(250*time.Millisecond, func() { js.DeleteConsumer("TEST", "d1") })
	start := time.Now()
	_, err = sub1.Fetch(1, nats.MaxWait(10*time.Second))
	require_Error(t, err, expectedErr)

	// Check that we bailed early.
	if time.Since(start) > time.Second {
		t.Fatalf("Took to long to bail out on consumer delete")
	}

	time.AfterFunc(250*time.Millisecond, func() { js.DeleteStream("TEST") })
	start = time.Now()
	_, err = sub2.Fetch(1, nats.MaxWait(10*time.Second))
	require_Error(t, err, expectedErr)
	if time.Since(start) > time.Second {
		t.Fatalf("Took to long to bail out on stream delete")
	}
}

// https://github.com/nats-io/nats-server/issues/3559
func TestJetStreamClusterSourceWithOptStartTime(t *testing.T) {
	s := RunBasicJetStreamServer()
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	test := func(t *testing.T, c *cluster, s *Server) {

		replicas := 1
		if c != nil {
			s = c.randomServer()
			replicas = 3
		}
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: replicas,
		})
		require_NoError(t, err)

		yesterday := time.Now().Add(-24 * time.Hour)

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "SOURCE",
			Replicas: replicas,
			Sources: []*nats.StreamSource{&nats.StreamSource{
				Name:         "TEST",
				OptStartTime: &yesterday,
			}},
		})
		require_NoError(t, err)

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "MIRROR",
			Replicas: replicas,
			Mirror: &nats.StreamSource{
				Name:         "TEST",
				OptStartTime: &yesterday,
			},
		})
		require_NoError(t, err)

		total := 10
		for i := 0; i < total; i++ {
			sendStreamMsg(t, nc, "foo", "hello")
		}

		checkCount := func(sname string, expected int) {
			t.Helper()
			checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
				si, err := js.StreamInfo(sname)
				if err != nil {
					return err
				}
				if n := si.State.Msgs; n != uint64(expected) {
					return fmt.Errorf("Expected stream %q to have %v messages, got %v", sname, expected, n)
				}
				return nil
			})
		}

		checkCount("TEST", 10)
		checkCount("SOURCE", 10)
		checkCount("MIRROR", 10)

		err = js.PurgeStream("SOURCE")
		require_NoError(t, err)
		err = js.PurgeStream("MIRROR")
		require_NoError(t, err)

		checkCount("TEST", 10)
		checkCount("SOURCE", 0)
		checkCount("MIRROR", 0)

		nc.Close()
		if c != nil {
			c.stopAll()
			c.restartAll()

			c.waitOnStreamLeader(globalAccountName, "TEST")
			c.waitOnStreamLeader(globalAccountName, "SOURCE")
			c.waitOnStreamLeader(globalAccountName, "MIRROR")

			s = c.randomServer()
		} else {
			sd := s.JetStreamConfig().StoreDir
			s.Shutdown()
			s = RunJetStreamServerOnPort(-1, sd)
		}

		// Wait a bit before checking because sync'ing (even with the defect)
		// would not happen right away. I tried with 1 sec and test would pass,
		// so need to be at least that much.
		time.Sleep(2 * time.Second)

		nc, js = jsClientConnect(t, s)
		defer nc.Close()
		checkCount("TEST", 10)
		checkCount("SOURCE", 0)
		checkCount("MIRROR", 0)
	}

	t.Run("standalone", func(t *testing.T) { test(t, nil, s) })
	t.Run("cluster", func(t *testing.T) { test(t, c, nil) })
}

type networkCableUnplugged struct {
	net.Conn
	sync.Mutex
	unplugged bool
	wb        bytes.Buffer
	wg        sync.WaitGroup
}

func (c *networkCableUnplugged) Write(b []byte) (int, error) {
	c.Lock()
	if c.unplugged {
		c.wb.Write(b)
		c.Unlock()
		return len(b), nil
	} else if c.wb.Len() > 0 {
		c.wb.Write(b)
		buf := c.wb.Bytes()
		c.wb.Reset()
		c.Unlock()
		if _, err := c.Conn.Write(buf); err != nil {
			return 0, err
		}
		return len(b), nil
	}
	c.Unlock()
	return c.Conn.Write(b)
}

func (c *networkCableUnplugged) Read(b []byte) (int, error) {
	c.Lock()
	wait := c.unplugged
	c.Unlock()
	if wait {
		c.wg.Wait()
	}
	return c.Conn.Read(b)
}

func TestJetStreamClusterScaleDownWhileNoQuorum(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	require_NoError(t, err)

	for i := 0; i < 1000; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	// Let's have a server from this R2 stream be network partitionned.
	// We will take the leader, but doesn't have to be.
	// To simulate partition, we will replace all its routes with a
	// special connection that drops messages.
	sl := c.serverByName(si.Cluster.Leader)
	if s == sl {
		nc.Close()
		for s = c.randomServer(); s != sl; s = c.randomServer() {
		}
		nc, js = jsClientConnect(t, s)
		defer nc.Close()
	}

	sl.mu.Lock()
	for _, r := range sl.routes {
		r.mu.Lock()
		ncu := &networkCableUnplugged{Conn: r.nc, unplugged: true}
		ncu.wg.Add(1)
		r.nc = ncu
		r.mu.Unlock()
	}
	sl.mu.Unlock()

	// Wait for the stream info to fail
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST", nats.MaxWait(time.Second))
		if err != nil {
			return err
		}
		if si.Cluster.Leader == _EMPTY_ {
			return nil
		}
		return fmt.Errorf("stream still has a leader")
	})

	// Now try to edit the stream by making it an R1. In some case we get
	// a context deadline error, in some no error. So don't check the returned error.
	js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	}, nats.MaxWait(5*time.Second))

	sl.mu.Lock()
	for _, r := range sl.routes {
		r.mu.Lock()
		ncu := r.nc.(*networkCableUnplugged)
		ncu.Lock()
		ncu.unplugged = false
		ncu.wg.Done()
		ncu.Unlock()
		r.mu.Unlock()
	}
	sl.mu.Unlock()

	checkClusterFormed(t, c.servers...)
	c.waitOnStreamLeader(globalAccountName, "TEST")
}

// We noticed that ha_assets enforcement seemed to not be upheld when assets created in a rapid fashion.
func TestJetStreamClusterHAssetsEnforcement(t *testing.T) {
	tmpl := strings.Replace(jsClusterTempl, "store_dir:", "limits: {max_ha_assets: 2}, store_dir:", 1)
	c := createJetStreamClusterWithTemplateAndModHook(t, tmpl, "R3S", 3, nil)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST-1",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST-2",
		Subjects: []string{"bar"},
		Replicas: 3,
	})
	require_NoError(t, err)

	exceededErrs := []error{errors.New("system limit reached"), errors.New("no suitable peers")}

	// Should fail.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST-3",
		Subjects: []string{"baz"},
		Replicas: 3,
	})
	require_Error(t, err, exceededErrs...)
}

func TestJetStreamClusterInterestStreamConsumer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	var subs []*nats.Subscription
	ns := 5

	for i := 0; i < ns; i++ {
		dn := fmt.Sprintf("d%d", i)
		sub, err := js.PullSubscribe("foo", dn)
		require_NoError(t, err)
		subs = append(subs, sub)
	}

	// Send 10 msgs
	n := 10
	for i := 0; i < n; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	// Collect all the messages.
	var msgs []*nats.Msg
	for _, sub := range subs {
		lmsgs := fetchMsgs(t, sub, n, time.Second)
		if len(lmsgs) != n {
			t.Fatalf("Did not receive all msgs: %d vs %d", len(lmsgs), n)
		}
		msgs = append(msgs, lmsgs...)
	}

	// Shuffle
	rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
	for _, m := range msgs {
		m.AckSync()
	}
	// Make sure replicated acks are processed.
	time.Sleep(250 * time.Millisecond)

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	if si.State.Msgs != 0 {
		t.Fatalf("Should not have any messages left: %d of %d", si.State.Msgs, n)
	}
}

func TestJetStreamClusterNoPanicOnStreamInfoWhenNoLeaderYet(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc := natsConnect(t, c.randomServer().ClientURL())
	defer nc.Close()

	js, _ := nc.JetStream(nats.MaxWait(500 * time.Millisecond))

	wg := sync.WaitGroup{}
	wg.Add(1)
	ch := make(chan struct{})
	go func() {
		defer wg.Done()

		for {
			js.StreamInfo("TEST")
			select {
			case <-ch:
				return
			case <-time.After(15 * time.Millisecond):
			}
		}
	}()

	time.Sleep(250 * time.Millisecond)

	// Don't care if this succeeds or not (could get a context deadline
	// due to the low MaxWait() when creating the context).
	js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})

	close(ch)
	wg.Wait()
}

// Issue https://github.com/nats-io/nats-server/issues/3630
func TestJetStreamClusterPullConsumerAcksExtendInactivityThreshold(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})

	n := 10
	for i := 0; i < n; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	// Pull Consumer
	sub, err := js.PullSubscribe("foo", "d", nats.InactiveThreshold(time.Second))
	require_NoError(t, err)

	fetchMsgs(t, sub, n/2, time.Second)
	// Will wait for .5s.
	time.Sleep(500 * time.Millisecond)
	msgs := fetchMsgs(t, sub, n/2, time.Second)
	if len(msgs) != n/2 {
		t.Fatalf("Did not receive msgs: %d vs %d", len(msgs), n/2)
	}

	// Wait for .5s.
	time.Sleep(500 * time.Millisecond)
	msgs[0].Ack() // Ack
	// Wait another .5s.
	time.Sleep(500 * time.Millisecond)
	msgs[1].Nak() // Nak
	// Wait another .5s.
	time.Sleep(500 * time.Millisecond)
	msgs[2].Term() // Term
	time.Sleep(500 * time.Millisecond)
	msgs[3].InProgress() // WIP

	// The above should have kept the consumer alive.
	_, err = js.ConsumerInfo("TEST", "d")
	require_NoError(t, err)

	// Make sure it gets cleaned up.
	time.Sleep(2 * time.Second)
	_, err = js.ConsumerInfo("TEST", "d")
	require_Error(t, err, nats.ErrConsumerNotFound)
}

// https://github.com/nats-io/nats-server/issues/3677
func TestJetStreamParallelStreamCreation(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	np := 20

	startCh := make(chan bool)
	errCh := make(chan error, np)

	wg := sync.WaitGroup{}
	wg.Add(np)
	for i := 0; i < np; i++ {
		go func() {
			defer wg.Done()

			// Individual connection
			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			// Make them all fire at once.
			<-startCh

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"common.*.*"},
				Replicas: 3,
			})
			if err != nil {
				errCh <- err
			}
		}()
	}

	close(startCh)
	wg.Wait()

	if len(errCh) > 0 {
		t.Fatalf("Expected no errors, got %d", len(errCh))
	}
}

// In addition to test above, if streams were attempted to be created in parallel
// it could be that multiple raft groups would be created for the same asset.
func TestJetStreamParallelStreamCreationDupeRaftGroups(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	np := 20

	startCh := make(chan bool)
	wg := sync.WaitGroup{}
	wg.Add(np)
	for i := 0; i < np; i++ {
		go func() {
			defer wg.Done()

			// Individual connection
			nc, _ := jsClientConnect(t, c.randomServer())
			js, _ := nc.JetStream(nats.MaxWait(time.Second))
			defer nc.Close()

			// Make them all fire at once.
			<-startCh

			// Ignore errors in this test, care about raft group and metastate.
			js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"common.*.*"},
				Replicas: 3,
			})
		}()
	}

	close(startCh)
	wg.Wait()

	// Restart a server too.
	s := c.randomServer()
	s.Shutdown()
	s = c.restartServer(s)
	c.waitOnLeader()
	c.waitOnStreamLeader(globalAccountName, "TEST")
	// Check that this server has only two active raft nodes after restart.
	if nrn := s.numRaftNodes(); nrn != 2 {
		t.Fatalf("Expected only two active raft nodes, got %d", nrn)
	}

	// Make sure we only have 2 unique raft groups for all servers.
	// One for meta, one for stream.
	expected := 2
	rg := make(map[string]struct{})
	for _, s := range c.servers {
		s.mu.RLock()
		for _, ni := range s.raftNodes {
			n := ni.(*raft)
			rg[n.Group()] = struct{}{}
		}
		s.mu.RUnlock()
	}
	if len(rg) != expected {
		t.Fatalf("Expected only %d distinct raft groups for all servers, go %d", expected, len(rg))
	}
}

func TestJetStreamParallelConsumerCreation(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"common.*.*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	np := 20

	startCh := make(chan bool)
	errCh := make(chan error, np)

	wg := sync.WaitGroup{}
	wg.Add(np)
	for i := 0; i < np; i++ {
		go func() {
			defer wg.Done()

			// Individual connection
			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			// Make them all fire at once.
			<-startCh

			_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
				Durable:  "dlc",
				Replicas: 3,
			})
			if err != nil {
				errCh <- err
			}
		}()
	}

	close(startCh)
	wg.Wait()

	if len(errCh) > 0 {
		t.Fatalf("Expected no errors, got %d", len(errCh))
	}

	// Make sure we only have 3 unique raft groups for all servers.
	// One for meta, one for stream, one for consumer.
	expected := 3
	rg := make(map[string]struct{})
	for _, s := range c.servers {
		s.mu.RLock()
		for _, ni := range s.raftNodes {
			n := ni.(*raft)
			rg[n.Group()] = struct{}{}
		}
		s.mu.RUnlock()
	}
	if len(rg) != expected {
		t.Fatalf("Expected only %d distinct raft groups for all servers, go %d", expected, len(rg))
	}

}

func TestJetStreamClusterReplacementPolicyAfterPeerRemove(t *testing.T) {
	// R3 scenario where there is a redundant node in each unique cloud so removing a peer should result in
	// an immediate replacement also preserving cloud uniqueness.

	sc := createJetStreamClusterExplicit(t, "PR9", 9)
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

	tags := []string{"cloud:aws", "cloud:aws", "cloud:aws", "cloud:gcp", "cloud:gcp", "cloud:gcp", "cloud:az", "cloud:az", "cloud:az"}

	var serverUTags = make(map[string]string)

	for i, s := range sc.servers {
		s.optsMu.Lock()
		serverUTags[s.Name()] = tags[i]
		s.opts.Tags.Add(tags[i])
		s.opts.JetStreamUniqueTag = "cloud"
		s.optsMu.Unlock()
		reset(s)
	}

	ml := sc.leader()
	js := ml.getJetStream()
	require_True(t, js != nil)
	js.mu.RLock()
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
	js.mu.RUnlock()
	defer sc.shutdown()

	sc.waitOnClusterReadyWithNumPeers(9)

	s := sc.leader()
	nc, jsc := jsClientConnect(t, s)
	defer nc.Close()

	_, err := jsc.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sc.waitOnStreamLeader(globalAccountName, "TEST")

	osi, err := jsc.StreamInfo("TEST")
	require_NoError(t, err)

	// Double check original placement honors unique_tag
	var uTags = make(map[string]struct{})

	uTags[serverUTags[osi.Cluster.Leader]] = struct{}{}
	for _, replica := range osi.Cluster.Replicas {
		evalTag := serverUTags[replica.Name]
		if _, exists := uTags[evalTag]; !exists {
			uTags[evalTag] = struct{}{}
			continue
		} else {
			t.Fatalf("expected initial placement to honor unique_tag")
		}
	}

	// Remove a peer and select replacement 5 times to avoid false good
	for i := 0; i < 5; i++ {
		// Remove 1 peer replica (this will be random cloud region as initial placement was randomized ordering)
		// After each successful iteration, osi will reflect the current RG peers

		toRemove := osi.Cluster.Replicas[0].Name
		_, err = nc.Request("$JS.API.STREAM.PEER.REMOVE.TEST", []byte(`{"peer":"`+toRemove+`"}`), time.Second*10)
		require_NoError(t, err)

		sc.waitOnStreamLeader(globalAccountName, "TEST")

		checkFor(t, time.Second, 200*time.Millisecond, func() error {
			osi, err = jsc.StreamInfo("TEST")
			require_NoError(t, err)
			if len(osi.Cluster.Replicas) != 2 {
				return fmt.Errorf("expected R3, got R%d", len(osi.Cluster.Replicas)+1)
			}
			// STREAM.PEER.REMOVE is asynchronous command; make sure remove has occurred by retrying
			for _, replica := range osi.Cluster.Replicas {
				if replica.Name == toRemove {
					return fmt.Errorf("expected replaced replica, old replica still present")
				}
			}
			return nil
		})

		// Validate that replacement with new peer still honors
		uTags = make(map[string]struct{}) //reset

		uTags[serverUTags[osi.Cluster.Leader]] = struct{}{}
		for _, replica := range osi.Cluster.Replicas {
			evalTag := serverUTags[replica.Name]
			if _, exists := uTags[evalTag]; !exists {
				uTags[evalTag] = struct{}{}
				continue
			} else {
				t.Fatalf("expected new peer and revised placement to honor unique_tag")
			}
		}
	}
}

func TestJetStreamClusterReplacementPolicyAfterPeerRemoveNoPlace(t *testing.T) {
	// R3 scenario where there are exactly three unique cloud nodes, so removing a peer should NOT
	// result in a new peer

	sc := createJetStreamClusterExplicit(t, "threeup", 3)
	sc.waitOnPeerCount(3)

	reset := func(s *Server) {
		s.mu.Lock()
		rch := s.sys.resetCh
		s.mu.Unlock()
		if rch != nil {
			rch <- struct{}{}
		}
		s.sendStatszUpdate()
	}

	tags := []string{"cloud:aws", "cloud:gcp", "cloud:az"}

	var serverUTags = make(map[string]string)

	for i, s := range sc.servers {
		s.optsMu.Lock()
		serverUTags[s.Name()] = tags[i]
		s.opts.Tags.Add(tags[i])
		s.opts.JetStreamUniqueTag = "cloud"
		s.optsMu.Unlock()
		reset(s)
	}

	ml := sc.leader()
	js := ml.getJetStream()
	require_True(t, js != nil)
	js.mu.RLock()
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
	js.mu.RUnlock()
	defer sc.shutdown()

	sc.waitOnClusterReadyWithNumPeers(3)

	s := sc.leader()
	nc, jsc := jsClientConnect(t, s)
	defer nc.Close()

	_, err := jsc.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sc.waitOnStreamLeader(globalAccountName, "TEST")

	osi, err := jsc.StreamInfo("TEST")
	require_NoError(t, err)

	// Double check original placement honors unique_tag
	var uTags = make(map[string]struct{})

	uTags[serverUTags[osi.Cluster.Leader]] = struct{}{}
	for _, replica := range osi.Cluster.Replicas {
		evalTag := serverUTags[replica.Name]
		if _, exists := uTags[evalTag]; !exists {
			uTags[evalTag] = struct{}{}
			continue
		} else {
			t.Fatalf("expected initial placement to honor unique_tag")
		}
	}

	// Remove 1 peer replica (this will be random cloud region as initial placement was randomized ordering)
	_, err = nc.Request("$JS.API.STREAM.PEER.REMOVE.TEST", []byte(`{"peer":"`+osi.Cluster.Replicas[0].Name+`"}`), time.Second*10)
	require_NoError(t, err)

	sc.waitOnStreamLeader(globalAccountName, "TEST")

	// Verify R2 since no eligible peer can replace the removed peer without braking unique constraint
	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		osi, err = jsc.StreamInfo("TEST")
		require_NoError(t, err)
		if len(osi.Cluster.Replicas) != 1 {
			return fmt.Errorf("expected R2, got R%d", len(osi.Cluster.Replicas)+1)
		}
		return nil
	})

	// Validate that remaining members still honor unique tags
	uTags = make(map[string]struct{}) //reset

	uTags[serverUTags[osi.Cluster.Leader]] = struct{}{}
	for _, replica := range osi.Cluster.Replicas {
		evalTag := serverUTags[replica.Name]
		if _, exists := uTags[evalTag]; !exists {
			uTags[evalTag] = struct{}{}
			continue
		} else {
			t.Fatalf("expected revised placement to honor unique_tag")
		}
	}
}
