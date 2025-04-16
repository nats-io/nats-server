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

package server

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

var serverConfig1 = `
	server_name: server1
	listen: 127.0.0.1:4222
	http: 8222

	prof_port = 18222

	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
  		name: my_cluster
  		listen: 127.0.0.1:4248
  		routes: [nats://127.0.0.1:4249,nats://127.0.0.1:4250]
	}

	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }}
`

var serverConfig2 = `
	server_name: server2
	listen: 127.0.0.1:5222
	http: 8223

	prof_port = 18223

	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
  		name: my_cluster
  		listen: 127.0.0.1:4249
  		routes: [nats://127.0.0.1:4248,nats://127.0.0.1:4250]
	}

	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }}
`

var serverConfig3 = `
	server_name: server3
	listen: 127.0.0.1:6222
	http: 8224

	prof_port = 18224

	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
  		name: my_cluster
  		listen: 127.0.0.1:4250
  		routes: [nats://127.0.0.1:4248,nats://127.0.0.1:4249]
	}

	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }}
`

var connectURL = "nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224"

func createMyLocalCluster(t *testing.T) *cluster {
	c := &cluster{servers: make([]*Server, 0, 3), opts: make([]*Options, 0, 3), name: "C3"}

	storeDir1 := t.TempDir()
	s1, o := RunServerWithConfig(createConfFile(t, []byte(fmt.Sprintf(serverConfig1, storeDir1))))
	c.servers = append(c.servers, s1)
	c.opts = append(c.opts, o)

	storeDir2 := t.TempDir()
	s2, o := RunServerWithConfig(createConfFile(t, []byte(fmt.Sprintf(serverConfig2, storeDir2))))
	c.servers = append(c.servers, s2)
	c.opts = append(c.opts, o)

	storeDir3 := t.TempDir()
	s3, o := RunServerWithConfig(createConfFile(t, []byte(fmt.Sprintf(serverConfig3, storeDir3))))
	c.servers = append(c.servers, s3)
	c.opts = append(c.opts, o)

	c.t = t

	// Wait til we are formed and have a leader.
	c.checkClusterFormed()
	c.waitOnClusterReady()

	return c
}

// This test is being skipped by CI as it takes too long to run and is meant to test the scalability of sourcing
// rather than being a unit test.
func TestStreamSourcingScalingSourcingManyBenchmark(t *testing.T) {
	t.Skip()

	var numSourced = 1000
	var numMsgPerSource = 10_000
	var batchSize = 200
	var retries int

	var err error

	c := createMyLocalCluster(t)
	defer c.shutdown()

	nc, err := nats.Connect(connectURL)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	defer nc.Close()

	// create n streams to source from
	for i := 0; i < numSourced; i++ {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:                 fmt.Sprintf("sourced-%d", i),
			Subjects:             []string{strconv.Itoa(i)},
			Retention:            nats.LimitsPolicy,
			Storage:              nats.FileStorage,
			Discard:              nats.DiscardOld,
			Replicas:             1,
			AllowDirect:          true,
			MirrorDirect:         false,
			DiscardNewPerSubject: false,
		})
		require_NoError(t, err)
	}

	fmt.Printf("Streams created\n")

	// publish n messages for each sourced stream
	for j := 0; j < numMsgPerSource; j++ {
		start := time.Now()
		var pafs = make([]nats.PubAckFuture, numSourced)
		for i := 0; i < numSourced; i++ {
			var err error

			for {
				pafs[i], err = js.PublishAsync(strconv.Itoa(i), []byte(strconv.Itoa(j)))
				if err != nil {
					fmt.Printf("Error async publishing: %v, retrying\n", err)
					retries++
					time.Sleep(10 * time.Millisecond)
				} else {
					break
				}
			}

			if i != 0 && i%batchSize == 0 {
				<-js.PublishAsyncComplete()
			}

		}

		<-js.PublishAsyncComplete()

		for i := 0; i < numSourced; i++ {
			select {
			case <-pafs[i].Ok():
			case psae := <-pafs[i].Err():
				fmt.Printf("Error on PubAckFuture: %v, retrying sync...\n", psae)
				retries++
				_, err = js.Publish(strconv.Itoa(i), []byte(strconv.Itoa(j)))
				require_NoError(t, err)
			}
		}

		end := time.Now()
		if j%1000 == 0 {
			fmt.Printf("Published round %d, avg pub latency %v\n", j, end.Sub(start)/time.Duration(numSourced))
		}
	}

	fmt.Printf("Messages published\n")

	// create the StreamSources
	streamSources := make([]*nats.StreamSource, numSourced)

	for i := 0; i < numSourced; i++ {
		streamSources[i] = &nats.StreamSource{Name: fmt.Sprintf("sourced-%d", i), FilterSubject: strconv.Itoa(i)}
	}

	// create a stream that sources from them
	_, err = js.AddStream(&nats.StreamConfig{
		Name:                 "sourcing",
		Subjects:             []string{"foo"},
		Sources:              streamSources,
		Retention:            nats.LimitsPolicy,
		Storage:              nats.FileStorage,
		Discard:              nats.DiscardOld,
		Replicas:             3,
		AllowDirect:          true,
		MirrorDirect:         false,
		DiscardNewPerSubject: false,
	})
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "sourcing")
	sl := c.streamLeader(globalAccountName, "sourcing")
	fmt.Printf("Sourcing stream created leader is *** %s ***\n", sl.Name())

	expectedSeq := make([]int, numSourced)

	start := time.Now()

	var lastMsgs uint64
	mset, err := sl.GlobalAccount().lookupStream("sourcing")
	require_NoError(t, err)

	checkFor(t, 5*time.Minute, 1000*time.Millisecond, func() error {
		mset.mu.RLock()
		var state StreamState
		mset.store.FastState(&state)
		mset.mu.RUnlock()

		if state.Msgs == uint64(numMsgPerSource*numSourced) {
			fmt.Printf("👍 Test passed: expected %d messages, got %d and took %v\n", uint64(numMsgPerSource*numSourced), state.Msgs, time.Since(start))
			return nil
		} else if state.Msgs < uint64(numMsgPerSource*numSourced) {
			fmt.Printf("Current Rate %d per second - Received %d\n", state.Msgs-lastMsgs, state.Msgs)
			lastMsgs = state.Msgs
			return fmt.Errorf("Expected %d messages, got %d", uint64(numMsgPerSource*numSourced), state.Msgs)
		} else {
			fmt.Printf("Too many messages! expected %d (retries=%d), got %d\n", uint64(numMsgPerSource*numSourced), retries, state.Msgs)
			return fmt.Errorf("Too many messages: expected %d (retries=%d), got %d", uint64(numMsgPerSource*numSourced), retries, state.Msgs)
		}
	})

	// Check that all the messages sourced in the stream are correct
	// Note: expects to see exactly increasing matching sequence numbers, so could theoretically fail if some messages
	// get recorded 'out of order' (according to the payload value) because asynchronous JS publication is used to
	// publish the messages.
	// However, that should not happen if the publish 'batch size' is not more than the number of streams being sourced.

	// create a consumer on sourcing
	_, err = js.AddConsumer("sourcing", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	syncSub, err := js.SubscribeSync("", nats.BindStream("sourcing"))
	require_NoError(t, err)

	start = time.Now()

	print("Checking the messages\n")
	for i := 0; i < numSourced*numMsgPerSource; i++ {
		msg, err := syncSub.NextMsg(30 * time.Second)
		require_NoError(t, err)
		sId, err := strconv.Atoi(msg.Subject)
		require_NoError(t, err)
		seq, err := strconv.Atoi(string(msg.Data))
		require_NoError(t, err)
		if expectedSeq[sId] == seq {
			expectedSeq[sId]++
		} else {
			t.Fatalf("Expected seq number %d got %d for source %d\n", expectedSeq[sId], seq, sId)
		}
		msg.Ack()
		if i%100_000 == 0 {
			now := time.Now()
			fmt.Printf("[%v] Checked %d messages: %f msgs/sec \n", now, i, 100_000/now.Sub(start).Seconds())
			start = now
		}
	}
	print("👍 Done. \n")
}
