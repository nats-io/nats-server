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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestWorkQueue(t *testing.T) {

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	s := c.randomServer()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:                 "QUEUE",
		Subjects:             []string{"Q.>"},
		Retention:            nats.WorkQueuePolicy,
		MaxMsgs:              3000,
		Storage:              nats.FileStorage,
		Discard:              nats.DiscardNew,
		Replicas:             3,
		Duplicates:           time.Duration(2 * time.Minute),
		AllowDirect:          true,
		MirrorDirect:         false,
		DiscardNewPerSubject: false,
	})
	require_NoError(t, err)

	pubSignal := make(chan struct{})
	// Two publishers
	go func() {
		s := c.randomServer()
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

	publish:
		for {
			select {
			case <-pubSignal:
				fmt.Println("@@@@@@@@@@@@@@ stop publishing Q.edr.new @@@@@@@@@@@@@@")
				break publish
			default:
			}
			_, _ = js.Publish("Q.edr.new", []byte("hello"))
		}
	}()
	go func() {
		s := c.randomServer()
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

	publish:
		for {
			select {
			case <-pubSignal:
				fmt.Println("@@@@@@@@@@@@@@ stop publishing Q.old @@@@@@@@@@@@@@")
				break publish
			default:
			}
			time.Sleep(20 * time.Millisecond)
			_, _ = js.Publish("Q.old", []byte("hello"))
		}
	}()
	go func() {
		s := c.randomServer()
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

	publish:
		for {
			select {
			case <-pubSignal:
				fmt.Println("@@@@@@@@@@@@@@ stop publishing Q.edr.foo @@@@@@@@@@@@@@")
				break publish
			default:
			}

			time.Sleep(time.Duration(rand.Intn(100)+1) * time.Millisecond)
			_, _ = js.Publish("Q.edr.foo", []byte("hello"))
		}
	}()

	cleanupSignal := make(chan struct{})

	// First client
	go func() {
		s := c.randomServer()
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		// Create a consumer
		_, err := js.AddConsumer("QUEUE", &nats.ConsumerConfig{
			Durable:         "edr:new",
			AckPolicy:       nats.AckExplicitPolicy,
			AckWait:         30 * time.Second,
			DeliverPolicy:   nats.DeliverAllPolicy,
			FilterSubject:   "Q.edr.new",
			MaxWaiting:      512,
			ReplayPolicy:    nats.ReplayInstantPolicy,
			SampleFrequency: "100%",
			Replicas:        3,
		})
		require_NoError(t, err)

		sub, err := js.PullSubscribe("Q.edr.new", "edr:new")
		require_NoError(t, err)
		fmt.Println("CONSUMER 1 CREATED")

		batchSize := 1
		i := 0
		chaos := true
		for {
			select {
			case <-cleanupSignal:
				chaos = false
			default:

			}
			var missed = make(map[uint64]struct{})

			batchSize = rand.Intn(100) + 1
			msgs, _ := sub.Fetch(batchSize)
			for _, m := range msgs {
				mm, err := m.Metadata()
				require_NoError(t, err)
				mseq := mm.Sequence.Stream
				i++
				if i%13 != 0 || !chaos {
					m.Ack()
				} else {
					if _, ok := missed[mseq]; !ok {
						missed[mseq] = struct{}{}
						fmt.Println("Missed 1")
					} else {
						m.Ack()
					}
				}
			}
		}

	}()

	// Second client
	go func() {
		s := c.randomServer()
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		// Create a consumer
		_, err := js.AddConsumer("QUEUE", &nats.ConsumerConfig{
			Durable:         "old",
			AckPolicy:       nats.AckExplicitPolicy,
			AckWait:         30 * time.Second,
			DeliverPolicy:   nats.DeliverAllPolicy,
			FilterSubject:   "Q.old",
			MaxWaiting:      512,
			ReplayPolicy:    nats.ReplayInstantPolicy,
			SampleFrequency: "100%",
			Replicas:        3,
		})
		require_NoError(t, err)

		sub, err := js.PullSubscribe("Q.old", "old")
		require_NoError(t, err)
		fmt.Println("CONSUMER 2 CREATED")

		batchSize := 1
		i := 0
		chaos := true
		for {
			select {
			case <-cleanupSignal:
				chaos = false
			default:

			}
			var missed = make(map[uint64]struct{})

			batchSize = rand.Intn(100) + 1
			msgs, _ := sub.Fetch(batchSize)
			for _, m := range msgs {
				mm, err := m.Metadata()
				require_NoError(t, err)
				mseq := mm.Sequence.Stream
				i++
				if i%13 != 0 || !chaos {
					m.Ack()
				} else {
					if _, ok := missed[mseq]; !ok {
						missed[mseq] = struct{}{}
						fmt.Println("Missed 2")
					} else {
						m.Ack()
					}
				}
			}
		}
	}()

	// Third client
	go func() {
		s := c.randomServer()
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		// Create a consumer
		_, err := js.AddConsumer("QUEUE", &nats.ConsumerConfig{
			Durable:         "foo",
			AckPolicy:       nats.AckExplicitPolicy,
			AckWait:         30 * time.Second,
			DeliverPolicy:   nats.DeliverAllPolicy,
			FilterSubject:   "Q.edr.foo",
			MaxWaiting:      512,
			ReplayPolicy:    nats.ReplayInstantPolicy,
			SampleFrequency: "100%",
			Replicas:        3,
		})
		require_NoError(t, err)

		sub, err := js.PullSubscribe("Q.edr.foo", "foo")
		require_NoError(t, err)
		fmt.Println("CONSUMER 3 CREATED")

		batchSize := 1
		i := 0
		chaos := true
		for {
			select {
			case <-cleanupSignal:
				chaos = false
			default:

			}
			var missed = make(map[uint64]struct{})

			batchSize = rand.Intn(100) + 1
			msgs, _ := sub.Fetch(batchSize)
			for _, m := range msgs {
				mm, err := m.Metadata()
				require_NoError(t, err)
				mseq := mm.Sequence.Stream
				i++
				if i%13 != 0 || !chaos {
					m.Ack()
				} else {
					if _, ok := missed[mseq]; !ok {
						missed[mseq] = struct{}{}
						fmt.Println("Missed 3")
					} else {
						m.Ack()
					}
				}
			}
		}
	}()

	pubTimer := time.NewTimer(1 * time.Minute)
	finallTimer := time.NewTimer(2 * time.Minute)

	for {
		select {
		case <-pubTimer.C:
			close(pubSignal)
			close(cleanupSignal)
		case <-finallTimer.C:
			fmt.Println("STOPPING")
			fmt.Println("LAST STATUS")
			info, err := js.StreamInfo("QUEUE", &nats.StreamInfoRequest{SubjectsFilter: "Q.>"})
			require_NoError(t, err)
			fmt.Printf("!!!!!!QUEUE:\n %+v\n", info)

			cinfo, err := js.ConsumerInfo("QUEUE", "edr:new")
			require_NoError(t, err)

			fmt.Printf("!!!!!!edr:new:\n %+v\n", cinfo)

			cinfo, err = js.ConsumerInfo("QUEUE", "old")
			require_NoError(t, err)

			fmt.Printf("!!!!!old:\n %+v\n", cinfo)

			cinfo, err = js.ConsumerInfo("QUEUE", "foo")
			require_NoError(t, err)

			fmt.Printf("!!!!!foo:\n %+v\n", cinfo)
			require_True(t, info.State.Msgs == 0)
			return
		default:
		}

		time.Sleep(5 * time.Second)
		info, err := js.StreamInfo("QUEUE")
		require_NoError(t, err)
		fmt.Printf("!!!!!!QUEUE:\n %+v\n", info)

		cinfo, err := js.ConsumerInfo("QUEUE", "edr:new")
		require_NoError(t, err)

		fmt.Printf("!!!!!!edr:new:\n %+v\n", cinfo)

		cinfo, err = js.ConsumerInfo("QUEUE", "old")
		require_NoError(t, err)

		fmt.Printf("!!!!!old:\n %+v\n", cinfo)

		cinfo, err = js.ConsumerInfo("QUEUE", "foo")
		require_NoError(t, err)

		fmt.Printf("!!!!!foo:\n %+v\n", cinfo)
	}
}
