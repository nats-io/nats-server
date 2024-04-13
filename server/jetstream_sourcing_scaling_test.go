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

func TestStreamSourcingScalingManySourcing(t *testing.T) {
	var numSourcing = 10000
	var numMessages = uint64(1000)

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
		Replicas:             3,
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
		fmt.Printf("Published round %d, avg pub latency %v\n", j, end.Sub(start)/time.Duration(numSourcing))
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
		// publish one message for each sourcer into the source stream
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

func TestStreamSourcingScalingSourcingMany(t *testing.T) {
	var numSourced = 10000
	var numMessages = uint64(50)

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
			MaxAge:               time.Duration(60 * time.Minute),
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
		Replicas:             3,
		AllowDirect:          true,
		MirrorDirect:         false,
		DiscardNewPerSubject: false,
		MaxAge:               time.Duration(60 * time.Minute),
	})
	require_NoError(t, err)

	fmt.Printf("Streams created\n")

	// publish n messages for each sourced stream
	for j := uint64(0); j < numMessages; j++ {
		start := time.Now()
		for i := 0; i < numSourced; i++ {
			_, err = js.Publish(fmt.Sprintf("foo.%d", i), []byte("hello"))
			require_NoError(t, err)
		}
		end := time.Now()
		fmt.Printf("Published round %d, avg pub latency %v\n", j, end.Sub(start)/time.Duration(numSourced))
	}

	fmt.Printf("Messages published\n")
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		state, err := js.StreamInfo("sourcing")
		if err != nil {
			return err
		}
		if state.State.Msgs == numMessages*uint64(numSourced) {
			fmt.Printf("Lucky match Expected %d messages, got %d", numMessages*uint64(numSourced), state.State.Msgs)
			return nil
		} else if state.State.Msgs < numMessages*uint64(numSourced) {
			fmt.Printf("Expected %d messages, got %d", numMessages*uint64(numSourced), state.State.Msgs)
			return fmt.Errorf("Expected %d messages, got %d", numMessages*uint64(numSourced), state.State.Msgs)
		} else {
			println("Too many messages!")
			return fmt.Errorf("Too many messages, expected %d, got %d", numMessages*uint64(numSourced), state.State.Msgs)
		}
	})
}
