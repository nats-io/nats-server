// Copyright 2019-2024 The NATS Authors
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
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"testing"
	"time"
)

// This test is being skipped by CI as it takes too long to run and is meant to test the scalability of sourcing
// rather than being a unit test.
func TestStreamSourcingScalingManySourcing(t *testing.T) {
	t.Skip()

	var numSourcing = 10000 // fails at 10000
	var numMessages = uint64(50)

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	s := c.randomServer()
	urls := s.clientConnectURLs
	fmt.Printf("Connected to server %+v\n", urls)

	nc, js := jsClientConnect(t, s, nats.Timeout(20*time.Second))
	defer nc.Close()

	// create a stream to source from
	_, err := js.AddStream(&nats.StreamConfig{
		Name:                 "source",
		Subjects:             []string{"foo.>"},
		Retention:            nats.LimitsPolicy,
		Storage:              nats.FileStorage,
		Discard:              nats.DiscardOld,
		Replicas:             1,
		AllowDirect:          true,
		MirrorDirect:         false,
		DiscardNewPerSubject: false,
		MaxAge:               time.Duration(60 * time.Minute),
	})
	require_NoError(t, err)

	// create n streams that source from it
	for i := 0; i < numSourcing; i++ {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:                 fmt.Sprintf("sourcing-%d", i),
			Sources:              []*nats.StreamSource{{Name: "source", FilterSubject: fmt.Sprintf("foo.%d", i)}},
			Retention:            nats.LimitsPolicy,
			Storage:              nats.FileStorage,
			Discard:              nats.DiscardOld,
			Replicas:             1,
			AllowDirect:          true,
			MirrorDirect:         false,
			DiscardNewPerSubject: false,
			MaxAge:               time.Duration(60 * time.Minute),
		})
		require_NoError(t, err)
	}

	fmt.Printf("Streams created\n")

	// publish n messages for each sourcer in the source stream
	for j := uint64(0); j < numMessages; j++ {
		start := time.Now()
		for i := 0; i < numSourcing; i++ {
			_, err = js.Publish(fmt.Sprintf("foo.%d", i), []byte("hello"))
			require_NoError(t, err)
		}
		end := time.Now()
		fmt.Printf("[%v] Published round %d, avg pub latency %v\n", time.Now(), j, end.Sub(start)/time.Duration(numSourcing))
	}

	fmt.Printf("Messages published\n")
	time.Sleep(100 * time.Millisecond)

	// cause a leader stepdown
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "source"), nil, time.Second)
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

	fmt.Printf("Leader stepdown done\n")

	for i := 0; i < numSourcing; i++ {
		// publish one message for each sourcing stream into the source stream
		_, err = js.Publish(fmt.Sprintf("foo.%d", i), []byte("hello"))
		require_NoError(t, err)
	}

	time.Sleep(500 * time.Millisecond)

	for i := 0; i < numSourcing; i++ {
		// check that each sourcer has the message
		state, err := js.StreamInfo(fmt.Sprintf("sourcing-%d", i))
		require_NoError(t, err)
		if state.State.Msgs != numMessages+1 {
			t.Fatalf("Expected %d messages, got %d", numMessages, state.State.Msgs)
		}
		fmt.Printf("Stream sourcing-%d has %d messages\n", i, state.State.Msgs)
	}

}

// This test is being skipped by CI as it takes too long to run and is meant to test the scalability of sourcing
// rather than being a unit test.
func TestStreamSourcingScalingSourcingMany(t *testing.T) {
	t.Skip()

	var numSourced = 10000
	var numMsgPerSource = uint64(1000)
	var batchSize = 1000
	var retries int

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	s := c.randomServer()
	urls := s.clientConnectURLs
	fmt.Printf("Connected to server %+v\n", urls)

	nc, js := jsClientConnect(t, s, nats.Timeout(20*time.Second))
	defer nc.Close()

	// create n streams to source from
	for i := 0; i < numSourced; i++ {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:                 fmt.Sprintf("sourced-%d", i),
			Subjects:             []string{fmt.Sprintf("foo.%d", i)},
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

	// create the StreamSources
	streamSources := make([]*nats.StreamSource, numSourced)

	for i := 0; i < numSourced; i++ {
		streamSources[i] = &nats.StreamSource{Name: fmt.Sprintf("sourced-%d", i), FilterSubject: fmt.Sprintf("foo.%d", i)}
	}

	// create a stream that sources from them
	_, err := js.AddStream(&nats.StreamConfig{
		Name:                 "sourcing",
		Sources:              streamSources,
		Retention:            nats.LimitsPolicy,
		Storage:              nats.FileStorage,
		Discard:              nats.DiscardOld,
		Replicas:             1,
		AllowDirect:          true,
		MirrorDirect:         false,
		DiscardNewPerSubject: false,
	})
	require_NoError(t, err)

	fmt.Printf("Streams created\n")

	// publish n messages for each sourced stream
	for j := uint64(0); j < numMsgPerSource; j++ {
		start := time.Now()
		var pafs = make([]nats.PubAckFuture, numSourced)
		for i := 0; i < numSourced; i++ {
			var err error

			for {
				pafs[i], err = js.PublishAsync(fmt.Sprintf("foo.%d", i), []byte("hello"))
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
				_, err = js.Publish(fmt.Sprintf("foo.%d", i), []byte("hello"))
				require_NoError(t, err)
			}
		}

		end := time.Now()
		fmt.Printf("[%v] Published round %d, avg pub latency %v\n", time.Now(), j, end.Sub(start)/time.Duration(numSourced))
	}

	fmt.Printf("Messages published\n")

	checkFor(t, 10*time.Minute, 1*time.Second, func() error {
		state, err := js.StreamInfo("sourcing")
		if err != nil {
			return err
		}
		if state.State.Msgs == numMsgPerSource*uint64(numSourced) {
			fmt.Printf("👍 Test passed: expected %d messages, got %d\n", numMsgPerSource*uint64(numSourced), state.State.Msgs)
			return nil
		} else if state.State.Msgs < numMsgPerSource*uint64(numSourced) {
			fmt.Printf("Expected %d messages, got %d\n", numMsgPerSource*uint64(numSourced), state.State.Msgs)
			return fmt.Errorf("Expected %d messages, got %d", numMsgPerSource*uint64(numSourced), state.State.Msgs)
		} else {
			fmt.Printf("\nToo many messages! expected %d (retries=%d), got %d", numMsgPerSource*uint64(numSourced), retries, state.State.Msgs)
			return fmt.Errorf("Too many messages: expected %d (retries=%d), got %d", numMsgPerSource*uint64(numSourced), retries, state.State.Msgs)
		}
	})
}

// This test is being skipped by CI as it takes too long to run and is meant to test the scalability of sourcing
// rather than being a unit test.
func TestStreamSourcingScalingSourcingManyBenchmark(t *testing.T) {
	t.Skip()

	var numSourced = 500
	var numMsgPerSource = uint64(10000)
	var batchSize = 500
	var retries int

	var err error

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	s := c.randomServer()
	urls := s.clientConnectURLs
	fmt.Printf("Connected to server %+v\n", urls)

	nc, js := jsClientConnect(t, s, nats.Timeout(20*time.Second))
	defer nc.Close()

	// create n streams to source from
	for i := 0; i < numSourced; i++ {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:                 fmt.Sprintf("sourced-%d", i),
			Subjects:             []string{fmt.Sprintf("foo.%d", i)},
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
	for j := uint64(0); j < numMsgPerSource; j++ {
		start := time.Now()
		var pafs = make([]nats.PubAckFuture, numSourced)
		for i := 0; i < numSourced; i++ {
			var err error

			for {
				pafs[i], err = js.PublishAsync(fmt.Sprintf("foo.%d", i), []byte("hello"))
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
				_, err = js.Publish(fmt.Sprintf("foo.%d", i), []byte("hello"))
				require_NoError(t, err)
			}
		}

		end := time.Now()
		fmt.Printf("[%v] Published round %d, avg pub latency %v\n", time.Now(), j, end.Sub(start)/time.Duration(numSourced))
	}

	fmt.Printf("Messages published\n")

	// create the StreamSources
	streamSources := make([]*nats.StreamSource, numSourced)

	for i := 0; i < numSourced; i++ {
		streamSources[i] = &nats.StreamSource{Name: fmt.Sprintf("sourced-%d", i), FilterSubject: fmt.Sprintf("foo.%d", i)}
	}

	// create a stream that sources from them
	_, err = js.AddStream(&nats.StreamConfig{
		Name:                 "sourcing",
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

	start := time.Now()

	fmt.Printf("[%v] Sourcing stream created\n", start)

	checkFor(t, 10*time.Minute, 1*time.Second, func() error {
		state, err := js.StreamInfo("sourcing")
		if err != nil {
			return err
		}
		if state.State.Msgs == numMsgPerSource*uint64(numSourced) {
			fmt.Printf("[%v] 👍 Test passed: expected %d messages, got %d and took %v\n", time.Now(), numMsgPerSource*uint64(numSourced), state.State.Msgs, time.Since(start))
			return nil
		} else if state.State.Msgs < numMsgPerSource*uint64(numSourced) {
			fmt.Printf("[%v] Expected %d messages, got %d\n", time.Now(), numMsgPerSource*uint64(numSourced), state.State.Msgs)
			return fmt.Errorf("Expected %d messages, got %d", numMsgPerSource*uint64(numSourced), state.State.Msgs)
		} else {
			fmt.Printf("[%v] Too many messages! expected %d (retries=%d), got %d\n", time.Now(), numMsgPerSource*uint64(numSourced), retries, state.State.Msgs)
			return fmt.Errorf("Too many messages: expected %d (retries=%d), got %d", numMsgPerSource*uint64(numSourced), retries, state.State.Msgs)
		}
	})
}
