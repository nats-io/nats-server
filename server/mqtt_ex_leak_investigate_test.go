// Copyright 2024 The NATS Authors
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

//go:build !skip_mqtt_tests
// +build !skip_mqtt_tests

package server

import (
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

func TestJetstreamConsumerLeak(t *testing.T) {

	QOS := byte(2)
	NSubscribers := 1000
	NConcurrentSubscribers := 100

	clusterConf := `
	listen: 127.0.0.1:-1

	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	leafnodes {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {
		ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`
	cl := createJetStreamClusterWithTemplate(t, clusterConf, "Leak-test", 3)
	defer cl.shutdown()

	cl.waitOnLeader()

	s := cl.randomNonLeader()
	testMQTTInitializeStreams(t, s)

	// Write the memory profile before starting the test
	w, _ := os.Create("before.pprof")
	pprof.WriteHeapProfile(w)
	w.Close()

	before := &runtime.MemStats{}
	runtime.GC()
	runtime.ReadMemStats(before)

	testMQTTConnSubReceiveDiscConcurrent(t, s, QOS, NSubscribers, NConcurrentSubscribers, testMQTTConnSubDiscJS)

	// Sleep for a few seconds to see if some timers kick in and help cleanup?
	time.Sleep(10 * time.Second)

	runtime.GC()
	w, _ = os.Create("after.pprof")
	pprof.WriteHeapProfile(w)
	w.Close()

	after := &runtime.MemStats{}
	runtime.GC()
	runtime.ReadMemStats(after)

	limit := before.HeapInuse + 100*1024*1024 // 100MB
	if after.HeapInuse > limit {
		t.Fatalf("Memory usage too high: %v", after.HeapInuse)
	}
}

func testMQTTInitializeStreams(t *testing.T, server *Server) {
	nc, js := jsClientConnect(t, server, nats.UserInfo("one", "p"))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      mqttStreamName,
		Subjects:  []string{mqttStreamSubjectPrefix + ">"},
		Storage:   nats.FileStorage,
		Retention: nats.InterestPolicy,
		Replicas:  3,
	})
	if err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      mqttOutStreamName,
		Subjects:  []string{mqttOutSubjectPrefix + ">"},
		Storage:   nats.FileStorage,
		Retention: nats.InterestPolicy,
		Replicas:  3,
	})
	if err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}
}

func testMQTTConnSubDiscJS(t *testing.T, server *Server, QOS byte, iSub int) {
	nc, js := jsClientConnect(t, server, nats.UserInfo("one", "p"))
	defer nc.Close()

	// make sure the MQTT streams are accessible to us
	_, err := js.StreamInfo(mqttStreamName)
	if err != nil {
		t.Fatalf("Error on JetStream stream info: %v", err)
	}
	_, err = js.StreamInfo(mqttOutStreamName)
	if err != nil {
		t.Fatalf("Error on JetStream stream info: %v", err)
	}

	start := time.Now()
	pubrelConsumerName := mqttPubRelConsumerDurablePrefix + nuid.Next()
	_, err = js.AddConsumer(mqttOutStreamName, &nats.ConsumerConfig{
		DeliverSubject: "pubrel-delivery_" + nuid.Next(),
		Durable:        pubrelConsumerName,
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverPolicy:  nats.DeliverNewPolicy,
		FilterSubject:  mqttPubRelSubjectPrefix + nuid.Next(),
		AckWait:        mqttDefaultAckWait,
		MaxAckPending:  mqttDefaultMaxAckPending,
		MemoryStorage:  false,
	})
	if err != nil {
		t.Fatalf("Error on JetStream consumer creation: %v", err)
	}

	subConsumerName := "sessid_" + nuid.Next()
	_, err = js.AddConsumer(mqttStreamName, &nats.ConsumerConfig{
		DeliverSubject: "inbox",
		Durable:        subConsumerName,
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverPolicy:  nats.DeliverNewPolicy,
		FilterSubject:  mqttStreamSubjectPrefix + "subject",
		AckWait:        mqttDefaultAckWait,
		MaxAckPending:  mqttDefaultMaxAckPending,
		MemoryStorage:  false,
	})
	if err != nil {
		t.Fatalf("Error on JetStream consumer creation: %v", err)
	}
	t.Logf("<>/<> SUB %v: Now %v, created 2 consumers", iSub, time.Since(start))

	err = js.DeleteConsumer(mqttOutStreamName, pubrelConsumerName)
	if err != nil {
		t.Fatalf("Error on JetStream consumer deletion: %v", err)
	}
	err = js.DeleteConsumer(mqttStreamName, subConsumerName)
	if err != nil {
		t.Fatalf("Error on JetStream consumer deletion: %v", err)
	}
	t.Logf("SUB %v: Now %v, deleted 2 consumers", iSub, time.Since(start))
}

func testMQTTConnSubReceiveDiscConcurrent(
	t *testing.T, server *Server, QOS byte, NSubscribers int, NConcurrentSubscribers int,
	subf func(t *testing.T, server *Server, QOS byte, n int),
) {
	ConcurrentSubscribers := make(chan struct{}, NConcurrentSubscribers)
	for i := 0; i < NConcurrentSubscribers; i++ {
		ConcurrentSubscribers <- struct{}{}
	}

	wg := sync.WaitGroup{}
	wg.Add(NSubscribers)
	// Start concurrent subscribers. Each will receive 1 to 3 messages, then quit.
	go func() {
		for iSub := 0; iSub < NSubscribers; {
			// wait for a slot to open up
			<-ConcurrentSubscribers
			iSub++
			go func(c int) {
				defer func() {
					ConcurrentSubscribers <- struct{}{}
					wg.Done()
				}()
				subf(t, server, QOS, c)
			}(iSub)
		}
	}()
	wg.Wait()
}
