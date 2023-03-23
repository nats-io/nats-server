// Copyright 2022-2023 The NATS Authors
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
			JSEnabledOnly: false,
			JSServerOnly:  false,
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
			JSEnabledOnly: false,
			JSServerOnly:  false,
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
	s := RunBasicJetStreamServer(t)
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
	s := RunBasicJetStreamServer(t)
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
			tf := createTempFile(t, "")
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

	s := RunBasicJetStreamServer(t)
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
	expectedErr := errors.New("nats: consumer deleted")

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
	s := RunBasicJetStreamServer(t)
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
			Sources: []*nats.StreamSource{{
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

	// Make sure if meta leader was on same server as stream leader we make sure
	// it elects new leader to receive update request.
	c.waitOnLeader()

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

	np := 50

	startCh := make(chan bool)
	errCh := make(chan error, np)

	cfg := &nats.ConsumerConfig{
		Durable:  "dlc",
		Replicas: 3,
	}

	wg := sync.WaitGroup{}
	swg := sync.WaitGroup{}
	wg.Add(np)
	swg.Add(np)

	for i := 0; i < np; i++ {
		go func() {
			defer wg.Done()

			// Individual connection
			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			swg.Done()

			// Make them all fire at once.
			<-startCh

			if _, err := js.AddConsumer("TEST", cfg); err != nil {
				errCh <- err
			}
		}()
	}

	swg.Wait()
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

func TestJetStreamGhostEphemeralsAfterRestart(t *testing.T) {
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

	// Add in 100 memory based ephemerals.
	for i := 0; i < 100; i++ {
		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
			Replicas:          1,
			InactiveThreshold: time.Second,
			MemoryStorage:     true,
		})
		require_NoError(t, err)
	}

	// Grab random server.
	rs := c.randomServer()
	// Now shutdown cluster.
	c.stopAll()

	// Let the consumers all expire.
	time.Sleep(2 * time.Second)

	// Restart first and wait so that we know it will try cleanup without a metaleader.
	c.restartServer(rs)
	time.Sleep(time.Second)

	c.restartAll()
	c.waitOnLeader()
	c.waitOnStreamLeader(globalAccountName, "TEST")

	nc, _ = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	subj := fmt.Sprintf(JSApiConsumerListT, "TEST")
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		m, err := nc.Request(subj, nil, time.Second)
		if err != nil {
			return err
		}
		var resp JSApiConsumerListResponse
		err = json.Unmarshal(m.Data, &resp)
		require_NoError(t, err)
		if len(resp.Consumers) != 0 {
			return fmt.Errorf("Still have %d consumers", len(resp.Consumers))
		}
		if len(resp.Missing) != 0 {
			return fmt.Errorf("Still have %d missing consumers", len(resp.Missing))
		}

		return nil
	})
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
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), []byte(`{"peer":"`+toRemove+`"}`), time.Second)
		require_NoError(t, err)
		var rpResp JSApiStreamRemovePeerResponse
		err = json.Unmarshal(resp.Data, &rpResp)
		require_NoError(t, err)
		require_True(t, rpResp.Success)

		sc.waitOnStreamLeader(globalAccountName, "TEST")

		checkFor(t, time.Second, 200*time.Millisecond, func() error {
			osi, err = jsc.StreamInfo("TEST")
			require_NoError(t, err)
			if len(osi.Cluster.Replicas) != 2 {
				return fmt.Errorf("expected R3, got R%d", len(osi.Cluster.Replicas)+1)
			}
			// STREAM.PEER.REMOVE is asynchronous command; make sure remove has occurred by
			// checking that the toRemove peer is gone.
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

// https://github.com/nats-io/nats-server/issues/3191
func TestJetStreamClusterLeafnodeDuplicateConsumerMessages(t *testing.T) {
	// Cluster B
	c := createJetStreamCluster(t, jsClusterTempl, "B", _EMPTY_, 2, 22020, false)
	defer c.shutdown()

	// Cluster A
	// Domain is "A'
	lc := c.createLeafNodesWithStartPortAndDomain("A", 2, 22110, "A")
	defer lc.shutdown()

	lc.waitOnClusterReady()

	// We want A-S-1 connected to B-S-1 and A-S-2 connected to B-S-2
	// So adjust if needed.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for i, ls := range lc.servers {
			ls.mu.RLock()
			var remoteServer string
			for _, rc := range ls.leafs {
				rc.mu.Lock()
				remoteServer = rc.leaf.remoteServer
				rc.mu.Unlock()
				break
			}
			ls.mu.RUnlock()

			wantedRemote := fmt.Sprintf("S-%d", i+1)
			if remoteServer != wantedRemote {
				ls.Shutdown()
				lc.restartServer(ls)
				return fmt.Errorf("Leafnode server %d not connected to %q", i+1, wantedRemote)
			}
		}
		return nil
	})

	// Wait on ready again.
	lc.waitOnClusterReady()

	// Create a stream and a durable pull consumer on cluster A.
	lnc, ljs := jsClientConnect(t, lc.randomServer())
	defer lnc.Close()

	_, err := ljs.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	require_NoError(t, err)

	// Make sure stream leader is on S-1
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		si, err := ljs.StreamInfo("TEST")
		require_NoError(t, err)
		if si.Cluster.Leader == "A-S-1" {
			return nil
		}
		_, err = lnc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, time.Second)
		require_NoError(t, err)
		return fmt.Errorf("Stream leader not placed on A-S-1")
	})

	_, err = ljs.StreamInfo("TEST")
	require_NoError(t, err)

	_, err = ljs.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:    "dlc",
		Replicas:   2,
		MaxDeliver: 1,
		AckPolicy:  nats.AckNonePolicy,
	})
	require_NoError(t, err)

	// Make sure consumer leader is on S-2
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		ci, err := ljs.ConsumerInfo("TEST", "dlc")
		require_NoError(t, err)
		if ci.Cluster.Leader == "A-S-2" {
			return nil
		}
		_, err = lnc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "dlc"), nil, time.Second)
		require_NoError(t, err)
		return fmt.Errorf("Stream leader not placed on A-S-1")
	})

	_, err = ljs.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)

	// Send 2 messages.
	sendStreamMsg(t, lnc, "foo", "M-1")
	sendStreamMsg(t, lnc, "foo", "M-2")

	// Now bind apps to cluster B servers and bind to pull consumer.
	nc1, _ := jsClientConnect(t, c.servers[0])
	defer nc1.Close()
	js1, err := nc1.JetStream(nats.Domain("A"))
	require_NoError(t, err)

	sub1, err := js1.PullSubscribe("foo", "dlc", nats.BindStream("TEST"))
	require_NoError(t, err)
	defer sub1.Unsubscribe()

	nc2, _ := jsClientConnect(t, c.servers[1])
	defer nc2.Close()
	js2, err := nc2.JetStream(nats.Domain("A"))
	require_NoError(t, err)

	sub2, err := js2.PullSubscribe("foo", "dlc", nats.BindStream("TEST"))
	require_NoError(t, err)
	defer sub2.Unsubscribe()

	// Make sure we can properly get messages.
	msgs, err := sub1.Fetch(1)
	require_NoError(t, err)
	require_True(t, len(msgs) == 1)
	require_True(t, string(msgs[0].Data) == "M-1")

	msgs, err = sub2.Fetch(1)
	require_NoError(t, err)
	require_True(t, len(msgs) == 1)
	require_True(t, string(msgs[0].Data) == "M-2")

	// Make sure delivered state makes it to other server to not accidentally send M-2 again
	// and fail the test below.
	time.Sleep(250 * time.Millisecond)

	// Now let's introduce and event, where A-S-2 will now reconnect after a restart to B-S-2
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		ls := lc.servers[1]
		wantedRemote := "S-1"
		var remoteServer string

		ls.mu.RLock()
		for _, rc := range ls.leafs {
			rc.mu.Lock()
			remoteServer = rc.leaf.remoteServer
			rc.mu.Unlock()
			break
		}
		ls.mu.RUnlock()

		if remoteServer != wantedRemote {
			ls.Shutdown()
			lc.restartServer(ls)
			return fmt.Errorf("Leafnode server not connected to %q", wantedRemote)
		}
		return nil
	})

	// Wait on ready again.
	lc.waitOnClusterReady()
	lc.waitOnStreamLeader(globalAccountName, "TEST")
	lc.waitOnConsumerLeader(globalAccountName, "TEST", "dlc")

	// Send 2 more messages.
	sendStreamMsg(t, lnc, "foo", "M-3")
	sendStreamMsg(t, lnc, "foo", "M-4")

	msgs, err = sub1.Fetch(2)
	require_NoError(t, err)
	require_True(t, len(msgs) == 2)
	require_True(t, string(msgs[0].Data) == "M-3")
	require_True(t, string(msgs[1].Data) == "M-4")

	// Send 2 more messages.
	sendStreamMsg(t, lnc, "foo", "M-5")
	sendStreamMsg(t, lnc, "foo", "M-6")

	msgs, err = sub2.Fetch(2)
	require_NoError(t, err)
	require_True(t, len(msgs) == 2)
	require_True(t, string(msgs[0].Data) == "M-5")
	require_True(t, string(msgs[1].Data) == "M-6")
}

func snapRGSet(pFlag bool, banner string, osi *nats.StreamInfo) *map[string]struct{} {
	var snapSet = make(map[string]struct{})
	if pFlag {
		fmt.Println(banner)
	}
	if osi == nil {
		if pFlag {
			fmt.Printf("bonkers!\n")
		}
		return nil
	}

	snapSet[osi.Cluster.Leader] = struct{}{}
	if pFlag {
		fmt.Printf("Leader: %s\n", osi.Cluster.Leader)
	}
	for _, replica := range osi.Cluster.Replicas {
		snapSet[replica.Name] = struct{}{}
		if pFlag {
			fmt.Printf("Replica: %s\n", replica.Name)
		}
	}

	return &snapSet
}

func TestJetStreamClusterAfterPeerRemoveZeroState(t *testing.T) {
	// R3 scenario (w/messages) in a 4-node cluster. Peer remove from RG and add back to same RG later.
	// Validate that original peer brought no memory or issues from its previous RG tour of duty, specifically
	// that the restored peer has the correct filestore usage bytes for the asset.
	var err error

	sc := createJetStreamClusterExplicit(t, "cl4", 4)
	defer sc.shutdown()

	sc.waitOnClusterReadyWithNumPeers(4)

	s := sc.leader()
	nc, jsc := jsClientConnect(t, s)
	defer nc.Close()

	_, err = jsc.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sc.waitOnStreamLeader(globalAccountName, "foo")

	osi, err := jsc.StreamInfo("foo")
	require_NoError(t, err)

	// make sure 0 msgs
	require_True(t, osi.State.Msgs == 0)

	// load up messages
	toSend := 10000
	// storage bytes with JS message overhead
	assetStoreBytesExpected := uint64(460000)

	for i := 1; i <= toSend; i++ {
		msg := []byte("Hello World")
		if _, err = jsc.Publish("foo.a", msg); err != nil {
			t.Fatalf("unexpected publish error: %v", err)
		}
	}

	osi, err = jsc.StreamInfo("foo")
	require_NoError(t, err)

	// make sure 10000 msgs
	require_True(t, osi.State.Msgs == uint64(toSend))

	origSet := *snapRGSet(false, "== Orig RG Set ==", osi)

	// remove 1 peer replica (1 of 2 non-leaders)
	origPeer := osi.Cluster.Replicas[0].Name
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "foo"), []byte(`{"peer":"`+origPeer+`"}`), time.Second)
	require_NoError(t, err)
	var rpResp JSApiStreamRemovePeerResponse
	err = json.Unmarshal(resp.Data, &rpResp)
	require_NoError(t, err)
	require_True(t, rpResp.Success)

	// validate the origPeer is removed with a replacement newPeer
	sc.waitOnStreamLeader(globalAccountName, "foo")
	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		osi, err = jsc.StreamInfo("foo")
		require_NoError(t, err)
		if len(osi.Cluster.Replicas) != 2 {
			return fmt.Errorf("expected R3, got R%d", len(osi.Cluster.Replicas)+1)
		}
		// STREAM.PEER.REMOVE is asynchronous command; make sure remove has occurred
		for _, replica := range osi.Cluster.Replicas {
			if replica.Name == origPeer {
				return fmt.Errorf("expected replaced replica, old replica still present")
			}
		}
		return nil
	})

	// identify the new peer
	var newPeer string
	osi, err = jsc.StreamInfo("foo")
	require_NoError(t, err)
	newSet := *snapRGSet(false, "== New RG Set ==", osi)
	for peer := range newSet {
		_, ok := origSet[peer]
		if !ok {
			newPeer = peer
			break
		}
	}
	require_True(t, newPeer != "")

	// kick out newPeer which will cause origPeer to be assigned to the RG again
	resp, err = nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "foo"), []byte(`{"peer":"`+newPeer+`"}`), time.Second)
	require_NoError(t, err)
	err = json.Unmarshal(resp.Data, &rpResp)
	require_NoError(t, err)
	require_True(t, rpResp.Success)

	// validate the newPeer is removed and R3 has reformed (with origPeer)
	sc.waitOnStreamLeader(globalAccountName, "foo")
	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		osi, err = jsc.StreamInfo("foo")
		require_NoError(t, err)
		if len(osi.Cluster.Replicas) != 2 {
			return fmt.Errorf("expected R3, got R%d", len(osi.Cluster.Replicas)+1)
		}
		// STREAM.PEER.REMOVE is asynchronous command; make sure remove has occurred
		for _, replica := range osi.Cluster.Replicas {
			if replica.Name == newPeer {
				return fmt.Errorf("expected replaced replica, old replica still present")
			}
		}
		return nil
	})

	osi, err = jsc.StreamInfo("foo")
	require_NoError(t, err)

	// make sure all msgs reported in stream at this point with original leader
	require_True(t, osi.State.Msgs == uint64(toSend))

	snapRGSet(false, "== RG Set w/origPeer Back ==", osi)

	// get a handle to original peer server
	var origServer *Server = sc.serverByName(origPeer)
	if origServer == nil {
		t.Fatalf("expected to get a handle to original peer server by name")
	}

	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		jszResult, err := origServer.Jsz(nil)
		require_NoError(t, err)
		if jszResult.Store != assetStoreBytesExpected {
			return fmt.Errorf("expected %d storage on orig peer, got %d", assetStoreBytesExpected, jszResult.Store)
		}
		return nil
	})
}

func TestJetStreamClusterMemLeaderRestart(t *testing.T) {
	// Test if R3 clustered mem store asset leader server restarted, that asset remains stable with final quorum
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, jsc := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := jsc.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Storage:  nats.MemoryStorage,
		Subjects: []string{"foo.*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// load up messages
	toSend := 10000
	for i := 1; i <= toSend; i++ {
		msg := []byte("Hello World")
		if _, err = jsc.Publish("foo.a", msg); err != nil {
			t.Fatalf("unexpected publish error: %v", err)
		}
	}

	osi, err := jsc.StreamInfo("foo")
	require_NoError(t, err)
	// make sure 10000 msgs
	require_True(t, osi.State.Msgs == uint64(toSend))

	// Shutdown the stream leader server
	rs := c.serverByName(osi.Cluster.Leader)
	rs.Shutdown()

	// Make sure that we have a META leader (there can always be a re-election)
	c.waitOnLeader()

	// Should still have quorum and a new leader
	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		osi, err = jsc.StreamInfo("foo")
		if err != nil {
			return fmt.Errorf("expected healthy stream asset, got %s", err.Error())
		}
		if osi.Cluster.Leader == "" {
			return fmt.Errorf("expected healthy stream asset with new leader")
		}
		if osi.State.Msgs != uint64(toSend) {
			return fmt.Errorf("expected healthy stream asset %d messages, got %d messages", toSend, osi.State.Msgs)
		}
		return nil
	})

	// Now restart the old leader peer (old stream state)
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

	// Can we get stream info after return
	osi, err = jsc.StreamInfo("foo")
	if err != nil {
		t.Fatalf("expected stream asset info return, got %s", err.Error())
	}

	// When asset leader came back did we re-form with quorum
	if osi.Cluster.Leader == "" {
		t.Fatalf("expected a current leader after old leader restarted")
	}
}

// Customer reported R1 consumers that seemed to be ghosted after server restart.
func TestJetStreamClusterLostConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "GHOST", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"events.>"},
		Replicas: 3,
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			_, err := js.Publish(fmt.Sprintf("events.%d.%d", i, j), []byte("test"))
			require_NoError(t, err)
		}
	}

	s := c.randomServer()
	s.Shutdown()

	c.waitOnLeader()
	c.waitOnStreamLeader(globalAccountName, "TEST")

	nc, _ = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cc := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			AckPolicy: AckExplicit,
		},
	}
	req, err := json.Marshal(cc)
	require_NoError(t, err)

	reqSubj := fmt.Sprintf(JSApiConsumerCreateT, "TEST")

	// Now create 50 consumers. We do not wait for the answer.
	for i := 0; i < 50; i++ {
		nc.Publish(reqSubj, req)
	}
	nc.Flush()

	// Grab the meta leader.
	ml := c.leader()
	require_NoError(t, ml.JetStreamSnapshotMeta())

	numConsumerAssignments := func(s *Server) int {
		t.Helper()
		js := s.getJetStream()
		js.mu.RLock()
		defer js.mu.RUnlock()
		cc := js.cluster
		for _, asa := range cc.streams {
			for _, sa := range asa {
				return len(sa.consumers)
			}
		}
		return 0
	}

	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		num := numConsumerAssignments(ml)
		if num == 50 {
			return nil
		}
		return fmt.Errorf("Consumers is only %d", num)
	})

	// Restart the server we shutdown. We snapshotted to the snapshot
	// has to fill in the new consumers.
	// The bug would fail to add them to the meta state since the stream
	// existed.
	s = c.restartServer(s)

	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		num := numConsumerAssignments(s)
		if num == 50 {
			return nil
		}
		return fmt.Errorf("Consumers is only %d", num)
	})
}

// https://github.com/nats-io/nats-server/issues/3636
func TestJetStreamClusterScaleDownDuringServerOffline(t *testing.T) {
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

	for i := 0; i < 100; i++ {
		sendStreamMsg(t, nc, "foo", "hello")
	}

	s := c.randomNonStreamLeader(globalAccountName, "TEST")
	s.Shutdown()

	c.waitOnLeader()

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	})
	require_NoError(t, err)

	s = c.restartServer(s)
	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		hs := s.healthz(nil)
		if hs.Error != _EMPTY_ {
			return errors.New(hs.Error)
		}
		return nil
	})
}

// Reported by a customer manually upgrading their streams to support direct gets.
// Worked if single replica but not in clustered mode.
func TestJetStreamClusterDirectGetStreamUpgrade(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "KV_TEST",
		Subjects:          []string{"$KV.TEST.>"},
		Discard:           nats.DiscardNew,
		MaxMsgsPerSubject: 1,
		DenyDelete:        true,
		Replicas:          3,
	})
	require_NoError(t, err)

	kv, err := js.KeyValue("TEST")
	require_NoError(t, err)

	_, err = kv.PutString("name", "derek")
	require_NoError(t, err)

	entry, err := kv.Get("name")
	require_NoError(t, err)
	require_True(t, string(entry.Value()) == "derek")

	// Now simulate a update to the stream to support direct gets.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:              "KV_TEST",
		Subjects:          []string{"$KV.TEST.>"},
		Discard:           nats.DiscardNew,
		MaxMsgsPerSubject: 1,
		DenyDelete:        true,
		AllowDirect:       true,
		Replicas:          3,
	})
	require_NoError(t, err)

	// Rebind to KV to make sure we DIRECT version of Get().
	kv, err = js.KeyValue("TEST")
	require_NoError(t, err)

	// Make sure direct get works.
	entry, err = kv.Get("name")
	require_NoError(t, err)
	require_True(t, string(entry.Value()) == "derek")
}

// For interest (or workqueue) based streams its important to match the replication factor.
// This was the case but now that more control over consumer creation is allowed its possible
// to create a consumer where the replication factor does not match. This could cause
// instability in the state between servers and cause problems on leader switches.
func TestJetStreamClusterInterestPolicyStreamForConsumersToMatchRFactor(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
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

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "XX",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})

	require_Error(t, err, NewJSConsumerReplicasShouldMatchStreamError())
}

// https://github.com/nats-io/nats-server/issues/3791
func TestJetStreamClusterKVWatchersWithServerDown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:   "TEST",
		Replicas: 3,
	})
	require_NoError(t, err)

	kv.PutString("foo", "bar")
	kv.PutString("foo", "baz")

	// Shutdown a follower.
	s := c.randomNonStreamLeader(globalAccountName, "KV_TEST")
	s.Shutdown()
	c.waitOnLeader()

	nc, _ = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	js, err = nc.JetStream(nats.MaxWait(2 * time.Second))
	require_NoError(t, err)

	kv, err = js.KeyValue("TEST")
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		w, err := kv.Watch("foo")
		require_NoError(t, err)
		w.Stop()
	}
}

// TestJetStreamClusterCurrentVsHealth is designed to show the
// difference between "current" and "healthy" when async publishes
// outpace the rate at which they can be applied.
func TestJetStreamClusterCurrentVsHealth(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	c.waitOnLeader()
	server := c.randomNonLeader()

	nc, js := jsClientConnect(t, server)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	server = c.randomNonStreamLeader(globalAccountName, "TEST")
	stream, err := server.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	raft, ok := stream.raftGroup().node.(*raft)
	require_True(t, ok)

	for i := 0; i < 1000; i++ {
		_, err := js.PublishAsync("foo", []byte("bar"))
		require_NoError(t, err)

		raft.RLock()
		commit := raft.commit
		applied := raft.applied
		raft.RUnlock()

		current := raft.Current()
		healthy := raft.Healthy()

		if !current || !healthy || commit != applied {
			t.Logf(
				"%d | Current %v, healthy %v, commit %d, applied %d, pending %d",
				i, current, healthy, commit, applied, commit-applied,
			)
		}
	}
}

// Several users and customers use this setup, but many times across leafnodes.
// This should be allowed in same account since we are really protecting against
// multiple pub acks with cycle detection.
func TestJetStreamClusterActiveActiveSourcedStreams(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "A",
		Subjects: []string{"A.>"},
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "B",
		Subjects: []string{"B.>"},
	})
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "A",
		Subjects: []string{"A.>"},
		Sources: []*nats.StreamSource{{
			Name:          "B",
			FilterSubject: "B.>",
		}},
	})
	require_NoError(t, err)

	// Before this would fail.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "B",
		Subjects: []string{"B.>"},
		Sources: []*nats.StreamSource{{
			Name:          "A",
			FilterSubject: "A.>",
		}},
	})
	require_NoError(t, err)
}

func TestJetStreamClusterUpdateConsumerShouldNotForceDeleteOnRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R7S", 7)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	require_NoError(t, err)

	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "D",
		DeliverSubject: "_no_bind_",
	})
	require_NoError(t, err)

	// Shutdown a consumer follower.
	nc.Close()
	s := c.serverByName(ci.Cluster.Replicas[0].Name)
	s.Shutdown()

	c.waitOnLeader()

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Change delivery subject.
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "D",
		DeliverSubject: "_d_",
	})
	require_NoError(t, err)

	// Create interest in new and old deliver subject.
	_, err = nc.SubscribeSync("_d_")
	require_NoError(t, err)
	_, err = nc.SubscribeSync("_no_bind_")
	require_NoError(t, err)
	nc.Flush()

	c.restartServer(s)
	c.waitOnAllCurrent()

	// Wait on bad error that would cleanup consumer.
	time.Sleep(time.Second)

	_, err = js.ConsumerInfo("TEST", "D")
	require_NoError(t, err)
}

func TestJetStreamClusterInterestPolicyEphemeral(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	for _, test := range []struct {
		testName string
		stream   string
		subject  string
		durable  string
		name     string
	}{
		{testName: "InterestWithDurable", durable: "eph", subject: "intdur", stream: "INT_DUR"},
		{testName: "InterestWithName", name: "eph", subject: "inteph", stream: "INT_EPH"},
	} {
		t.Run(test.testName, func(t *testing.T) {
			var err error

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err = js.AddStream(&nats.StreamConfig{
				Name:      test.stream,
				Subjects:  []string{test.subject},
				Retention: nats.LimitsPolicy,
				Replicas:  3,
			})
			require_NoError(t, err)

			const inactiveThreshold = time.Second

			_, err = js.AddConsumer(test.stream, &nats.ConsumerConfig{
				DeliverSubject:    nats.NewInbox(),
				AckPolicy:         nats.AckExplicitPolicy,
				InactiveThreshold: inactiveThreshold,
				Durable:           test.durable,
				Name:              test.name,
			})
			require_NoError(t, err)

			name := test.durable
			if test.durable == _EMPTY_ {
				name = test.name
			}

			const msgs = 5_000
			done, count := make(chan bool), 0

			sub, err := js.Subscribe(_EMPTY_, func(msg *nats.Msg) {
				require_NoError(t, msg.Ack())
				count++
				if count >= msgs {
					done <- true
				}
			}, nats.Bind(test.stream, name), nats.ManualAck())
			require_NoError(t, err)

			// This happens only if we start publishing messages after consumer was created.
			pubDone := make(chan struct{})
			go func(subject string) {
				for i := 0; i < msgs; i++ {
					js.Publish(subject, []byte("DATA"))
				}
				close(pubDone)
			}(test.subject)

			// Wait for inactive threshold to expire and all messages to be published and received
			// Bug is we clean up active consumers when we should not.
			time.Sleep(3 * inactiveThreshold / 2)

			select {
			case <-pubDone:
			case <-time.After(10 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}

			info, err := js.ConsumerInfo(test.stream, name)
			if err != nil {
				t.Fatalf("Expected to be able to retrieve consumer: %v", err)
			}
			require_True(t, info.Delivered.Stream == msgs)

			// Stop the subscription and remove the interest.
			err = sub.Unsubscribe()
			require_NoError(t, err)

			// Now wait for interest inactivity threshold to kick in.
			time.Sleep(3 * inactiveThreshold / 2)

			// Check if the consumer has been removed.
			_, err = js.ConsumerInfo(test.stream, name)
			require_Error(t, err, nats.ErrConsumerNotFound)
		})
	}
}

// TestJetStreamClusterWALBuildupOnNoOpPull tests whether or not the consumer
// RAFT log is being compacted when the stream is idle but we are performing
// lots of fetches. Otherwise the disk usage just spirals out of control if
// there are no other state changes to trigger a compaction.
func TestJetStreamClusterWALBuildupOnNoOpPull(t *testing.T) {
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

	sub, err := js.PullSubscribe(
		"foo",
		"durable",
		nats.ConsumerReplicas(3),
	)
	require_NoError(t, err)

	for i := 0; i < 10000; i++ {
		_, _ = sub.Fetch(1, nats.MaxWait(time.Microsecond))
	}

	// Needs to be at least 10 seconds, otherwise we won't hit the
	// minSnapDelta that prevents us from snapshotting too often
	time.Sleep(time.Second * 11)

	for i := 0; i < 1024; i++ {
		_, _ = sub.Fetch(1, nats.MaxWait(time.Microsecond))
	}

	time.Sleep(time.Second)

	server := c.randomNonConsumerLeader(globalAccountName, "TEST", "durable")

	stream, err := server.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	consumer := stream.lookupConsumer("durable")
	require_NotNil(t, consumer)

	entries, bytes := consumer.raftNode().Size()
	t.Log("new entries:", entries)
	t.Log("new bytes:", bytes)

	if max := uint64(1024); entries > max {
		t.Fatalf("got %d entries, expected less than %d entries", entries, max)
	}
}

// Found in https://github.com/nats-io/nats-server/issues/3848
// When Max Age was specified and stream was scaled up, new replicas
// were expiring messages much later than the leader.
func TestJetStreamClusterStreamMaxAgeScaleUp(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	for _, test := range []struct {
		name    string
		storage nats.StorageType
		stream  string
		purge   bool
	}{
		{name: "file", storage: nats.FileStorage, stream: "A", purge: false},
		{name: "memory", storage: nats.MemoryStorage, stream: "B", purge: false},
		{name: "file with purge", storage: nats.FileStorage, stream: "C", purge: true},
		{name: "memory with purge", storage: nats.MemoryStorage, stream: "D", purge: true},
	} {

		t.Run(test.name, func(t *testing.T) {
			ttl := time.Second * 5
			// Add stream with one replica and short MaxAge.
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     test.stream,
				Replicas: 1,
				Subjects: []string{test.stream},
				MaxAge:   ttl,
				Storage:  test.storage,
			})
			require_NoError(t, err)

			// Add some messages.
			for i := 0; i < 10; i++ {
				sendStreamMsg(t, nc, test.stream, "HELLO")
			}
			// We need to also test if we properly set expiry
			// if first sequence is not 1.
			if test.purge {
				err = js.PurgeStream(test.stream)
				require_NoError(t, err)
				// Add some messages.
				for i := 0; i < 10; i++ {
					sendStreamMsg(t, nc, test.stream, "HELLO")
				}
			}
			// Mark the time when all messages were published.
			start := time.Now()

			// Sleep for half of the MaxAge time.
			time.Sleep(ttl / 2)

			// Scale up the Stream to 3 replicas.
			_, err = js.UpdateStream(&nats.StreamConfig{
				Name:     test.stream,
				Replicas: 3,
				Subjects: []string{test.stream},
				MaxAge:   ttl,
				Storage:  test.storage,
			})
			require_NoError(t, err)

			// All messages should still be there.
			info, err := js.StreamInfo(test.stream)
			require_NoError(t, err)
			require_True(t, info.State.Msgs == 10)

			// Wait until MaxAge is reached.
			time.Sleep(ttl - time.Since(start) + (10 * time.Millisecond))

			// Check if all messages are expired.
			info, err = js.StreamInfo(test.stream)
			require_NoError(t, err)
			require_True(t, info.State.Msgs == 0)

			// Now switch leader to one of replicas
			_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, test.stream), nil, time.Second)
			require_NoError(t, err)
			c.waitOnStreamLeader("$G", test.stream)

			// and make sure that it also expired all messages
			info, err = js.StreamInfo(test.stream)
			require_NoError(t, err)
			require_True(t, info.State.Msgs == 0)
		})
	}
}

func TestJetStreamClusterWorkQueueConsumerReplicatedAfterScaleUp(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Replicas:  1,
		Subjects:  []string{"WQ"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	// Create an ephemeral consumer.
	sub, err := js.SubscribeSync("WQ")
	require_NoError(t, err)

	// Scale up to R3.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Replicas:  3,
		Subjects:  []string{"WQ"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	ci, err := sub.ConsumerInfo()
	require_NoError(t, err)

	require_True(t, ci.Config.Replicas == 0 || ci.Config.Replicas == 3)

	c.waitOnConsumerLeader(globalAccountName, "TEST", ci.Name)
	s := c.consumerLeader(globalAccountName, "TEST", ci.Name)
	require_NotNil(t, s)

	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	o := mset.lookupConsumer(ci.Name)
	require_NotNil(t, o)
	require_NotNil(t, o.raftNode())
}

// https://github.com/nats-io/nats-server/issues/3953
func TestJetStreamClusterWorkQueueAfterScaleUp(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Replicas:  1,
		Subjects:  []string{"WQ"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "d1",
		DeliverSubject: "d1",
		AckPolicy:      nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	wch := make(chan bool, 1)
	_, err = nc.Subscribe("d1", func(msg *nats.Msg) {
		msg.AckSync()
		wch <- true
	})
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Replicas:  3,
		Subjects:  []string{"WQ"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	sendStreamMsg(t, nc, "WQ", "SOME WORK")
	<-wch

	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		if si.State.Msgs == 0 {
			return nil
		}
		return fmt.Errorf("Still have %d msgs left", si.State.Msgs)
	})
}

func TestJetStreamClusterInterestBasedStreamAndConsumerSnapshots(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Replicas:  3,
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("foo", nats.Durable("d22"))
	require_NoError(t, err)

	num := 200
	for i := 0; i < num; i++ {
		js.PublishAsync("foo", []byte("ok"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkSubsPending(t, sub, num)

	// Shutdown one server.
	s := c.randomServer()
	s.Shutdown()

	c.waitOnStreamLeader(globalAccountName, "TEST")

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Now ack all messages while the other server is down.
	for i := 0; i < num; i++ {
		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		m.AckSync()
	}

	// Wait for all message acks to be processed and all messages to be removed.
	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		if si.State.Msgs == 0 {
			return nil
		}
		return fmt.Errorf("Still have %d msgs left", si.State.Msgs)
	})

	// Force a snapshot on the consumer leader before restarting the downed server.
	cl := c.consumerLeader(globalAccountName, "TEST", "d22")
	require_NotNil(t, cl)

	mset, err := cl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	o := mset.lookupConsumer("d22")
	require_NotNil(t, o)

	snap, err := o.store.EncodedState()
	require_NoError(t, err)

	n := o.raftNode()
	require_NotNil(t, n)
	require_NoError(t, n.InstallSnapshot(snap))

	// Now restart the downed server.
	s = c.restartServer(s)

	// Make the restarted server the eventual leader.
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnStreamLeader(globalAccountName, "TEST")
		if sl := c.streamLeader(globalAccountName, "TEST"); sl != s {
			sl.JetStreamStepdownStream(globalAccountName, "TEST")
			return fmt.Errorf("Server %s is not leader yet", s)
		}
		return nil
	})

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 0)
}

func TestJetStreamClusterConsumerFollowerStoreStateAckFloorBug(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 3,
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe(_EMPTY_, "C", nats.BindStream("TEST"), nats.ManualAck())
	require_NoError(t, err)

	num := 100
	for i := 0; i < num; i++ {
		sendStreamMsg(t, nc, "foo", "data")
	}

	// This one prevents the state for pending from reaching 0 and resetting, which would not show the bug.
	sendStreamMsg(t, nc, "foo", "data")

	// Ack all but one and out of order and make sure all consumers have the same stored state.
	msgs, err := sub.Fetch(num, nats.MaxWait(time.Second))
	require_NoError(t, err)
	require_True(t, len(msgs) == num)

	_, err = sub.Fetch(1, nats.MaxWait(time.Second))
	require_NoError(t, err)

	rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
	for _, m := range msgs {
		m.AckSync()
	}

	checkConsumerState := func(delivered, ackFloor nats.SequenceInfo, numAckPending int) {
		expectedDelivered := uint64(num) + 1
		if delivered.Stream != expectedDelivered || delivered.Consumer != expectedDelivered {
			t.Fatalf("Wrong delivered, expected %d got %+v", expectedDelivered, delivered)
		}
		expectedAck := uint64(num)
		if ackFloor.Stream != expectedAck || ackFloor.Consumer != expectedAck {
			t.Fatalf("Wrong ackFloor, expected %d got %+v", expectedAck, ackFloor)
		}
		require_True(t, numAckPending == 1)
	}

	ci, err := js.ConsumerInfo("TEST", "C")
	require_NoError(t, err)
	checkConsumerState(ci.Delivered, ci.AckFloor, ci.NumAckPending)

	// Check each consumer on each server for it's store state and make sure it matches as well.
	for _, s := range c.servers {
		mset, err := s.GlobalAccount().lookupStream("TEST")
		require_NoError(t, err)
		require_NotNil(t, mset)
		o := mset.lookupConsumer("C")
		require_NotNil(t, o)

		state, err := o.store.State()
		require_NoError(t, err)

		delivered := nats.SequenceInfo{Stream: state.Delivered.Stream, Consumer: state.Delivered.Consumer}
		ackFloor := nats.SequenceInfo{Stream: state.AckFloor.Stream, Consumer: state.AckFloor.Consumer}
		checkConsumerState(delivered, ackFloor, len(state.Pending))
	}

	// Now stepdown the consumer and move its leader and check the state after transition.
	// Make the restarted server the eventual leader.
	seen := make(map[*Server]bool)
	cl := c.consumerLeader(globalAccountName, "TEST", "C")
	require_NotNil(t, cl)
	seen[cl] = true

	allSeen := func() bool {
		for _, s := range c.servers {
			if !seen[s] {
				return false
			}
		}
		return true
	}

	checkAllLeaders := func() {
		t.Helper()
		checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
			c.waitOnConsumerLeader(globalAccountName, "TEST", "C")
			if allSeen() {
				return nil
			}
			cl := c.consumerLeader(globalAccountName, "TEST", "C")
			seen[cl] = true
			ci, err := js.ConsumerInfo("TEST", "C")
			require_NoError(t, err)
			checkConsumerState(ci.Delivered, ci.AckFloor, ci.NumAckPending)
			cl.JetStreamStepdownConsumer(globalAccountName, "TEST", "C")
			return fmt.Errorf("Not all servers have been consumer leader yet")
		})
	}

	checkAllLeaders()

	// No restart all servers and check again.
	c.stopAll()
	c.restartAll()
	c.waitOnLeader()

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	seen = make(map[*Server]bool)
	checkAllLeaders()
}

func TestNoRaceJetStreamClusterDifferentRTTInterestBasedStreamPreAck(t *testing.T) {
	tmpl := `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
		name: "F3"
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
	`

	//  Route Ports
	//	"S1": 14622,
	//	"S2": 15622,
	//	"S3": 16622,

	// S2 (stream leader) will have a slow path to S1 (via proxy) and S3 (consumer leader) will have a fast path.

	// Do these in order, S1, S2 (proxy) then S3.
	c := &cluster{t: t, servers: make([]*Server, 3), opts: make([]*Options, 3), name: "F3"}

	// S1
	conf := fmt.Sprintf(tmpl, "S1", t.TempDir(), 14622, "route://127.0.0.1:15622, route://127.0.0.1:16622")
	c.servers[0], c.opts[0] = RunServerWithConfig(createConfFile(t, []byte(conf)))

	// S2
	// Create the proxy first. Connect this to S1. Make it slow, e.g. 5ms RTT.
	np := createNetProxy(1*time.Millisecond, 1024*1024*1024, 1024*1024*1024, "route://127.0.0.1:14622", true)
	routes := fmt.Sprintf("%s, route://127.0.0.1:16622", np.routeURL())
	conf = fmt.Sprintf(tmpl, "S2", t.TempDir(), 15622, routes)
	c.servers[1], c.opts[1] = RunServerWithConfig(createConfFile(t, []byte(conf)))

	// S3
	conf = fmt.Sprintf(tmpl, "S3", t.TempDir(), 16622, "route://127.0.0.1:14622, route://127.0.0.1:15622")
	c.servers[2], c.opts[2] = RunServerWithConfig(createConfFile(t, []byte(conf)))

	c.checkClusterFormed()
	c.waitOnClusterReady()
	defer c.shutdown()
	defer np.stop()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Now create the stream.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "EVENTS",
		Subjects:  []string{"EV.>"},
		Replicas:  3,
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	// Make sure it's leader is on S2.
	sl := c.servers[1]
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnStreamLeader(globalAccountName, "EVENTS")
		if s := c.streamLeader(globalAccountName, "EVENTS"); s != sl {
			s.JetStreamStepdownStream(globalAccountName, "EVENTS")
			return fmt.Errorf("Server %s is not stream leader yet", sl)
		}
		return nil
	})

	// Now create the consumer.
	_, err = js.AddConsumer("EVENTS", &nats.ConsumerConfig{
		Durable:        "C",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: "dx",
	})
	require_NoError(t, err)

	// Make sure the consumer leader is on S3.
	cl := c.servers[2]
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnConsumerLeader(globalAccountName, "EVENTS", "C")
		if s := c.consumerLeader(globalAccountName, "EVENTS", "C"); s != cl {
			s.JetStreamStepdownConsumer(globalAccountName, "EVENTS", "C")
			return fmt.Errorf("Server %s is not consumer leader yet", sl)
		}
		return nil
	})

	// Create the real consumer on the consumer leader to make it efficient.
	nc, js = jsClientConnect(t, cl)
	defer nc.Close()

	_, err = js.Subscribe(_EMPTY_, func(msg *nats.Msg) {
		msg.Ack()
	}, nats.BindStream("EVENTS"), nats.Durable("C"), nats.ManualAck())
	require_NoError(t, err)

	for i := 0; i < 1_000; i++ {
		_, err := js.PublishAsync("EVENTS.PAID", []byte("ok"))
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	slow := c.servers[0]
	mset, err := slow.GlobalAccount().lookupStream("EVENTS")
	require_NoError(t, err)

	// Make sure preAck is non-nil, so we know the logic has kicked in.
	mset.mu.RLock()
	preAcks := mset.preAcks
	mset.mu.RUnlock()
	require_NotNil(t, preAcks)

	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		state := mset.state()
		if state.Msgs == 0 {
			mset.mu.RLock()
			lp := len(mset.preAcks)
			mset.mu.RUnlock()
			if lp == 0 {
				return nil
			} else {
				t.Fatalf("Expected no preAcks with no msgs, but got %d", lp)
			}
		}
		return fmt.Errorf("Still have %d msgs left", state.Msgs)
	})

}

func TestJetStreamInterestLeakOnDisableJetStream(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.leader())
	defer nc.Close()

	for i := 1; i <= 5; i++ {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     fmt.Sprintf("test_%d", i),
			Subjects: []string{fmt.Sprintf("test_%d", i)},
			Replicas: 3,
		})
		require_NoError(t, err)
	}

	c.waitOnAllCurrent()

	server := c.randomNonLeader()
	account := server.SystemAccount()

	server.DisableJetStream()

	var sublist []*subscription
	account.sl.localSubs(&sublist, false)

	var danglingJSC, danglingRaft int
	for _, sub := range sublist {
		if strings.HasPrefix(string(sub.subject), "$JSC.") {
			danglingJSC++
		} else if strings.HasPrefix(string(sub.subject), "$NRG.") {
			danglingRaft++
		}
	}
	if danglingJSC > 0 || danglingRaft > 0 {
		t.Fatalf("unexpected dangling interests for JetStream assets after shutdown (%d $JSC, %d $NRG)", danglingJSC, danglingRaft)
	}
}
