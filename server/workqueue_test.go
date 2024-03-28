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
	"sync"
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
		Storage:              nats.FileStorage,
		Discard:              nats.DiscardOld,
		Replicas:             1,
		Duplicates:           time.Duration(2 * time.Minute),
		AllowDirect:          true,
		MirrorDirect:         false,
		DiscardNewPerSubject: false,
		MaxAge:               time.Duration(50 * time.Minute),
	})
	require_NoError(t, err)

	pubSignal := make(chan struct{})

	publisher := func(subject string, sleepMin, sleepMax int, pubSignal chan struct{}) {
		s := c.randomServer()
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

	publish:
		for {
			select {
			case <-pubSignal:
				fmt.Printf("@@@@@@@@@@@@@@ stop publishing %v @@@@@@@@@@@@@@", subject)
				break publish
			default:
			}
			if sleepMax != 0 {
				time.Sleep(time.Duration(rand.Intn(sleepMax-sleepMin)+sleepMin) * time.Millisecond)
			}
			_, _ = js.Publish(subject, []byte("hello"))
		}
	}

	go publisher("Q.edr.new", 0, 0, pubSignal)
	go publisher("Q.old", 0, 0, pubSignal)
	go publisher("Q.bla", 0, 0, pubSignal)
	go publisher("Q.edr.foo", 1, 3, pubSignal)

	sequences := make(map[uint64]struct{})
	mu := &sync.Mutex{}

	consumer := func(config *nats.ConsumerConfig, consumers int, batchMin, batchMax int, cleanupSignal chan struct{}) {
		go func() {
			s := c.randomServer()
			nc, js := jsClientConnect(t, s)

			// Create a consumer
			_, err := js.AddConsumer("QUEUE", config)
			require_NoError(t, err)

			batchSize := 1
			i := 0
			// chaos := true
			for c := 0; c < consumers; c++ {
				var sub *nats.Subscription
				var err error
				if len(config.FilterSubjects) > 0 {
					sub, err = js.PullSubscribe("", config.Durable, nats.Bind("QUEUE", config.Durable))
				} else {
					sub, err = js.PullSubscribe(config.FilterSubject, config.Durable)
				}
				require_NoError(t, err)
				fmt.Printf("CONSUMER %v CREATED\n", config.Durable)
				go func() {
					defer nc.Close()
					for {
						select {
						case <-cleanupSignal:
							// chaos = false
						default:

						}
						var _ = make(map[uint64]struct{})

						if batchMax == 1 {
							batchSize = 1
						} else {
							batchSize = rand.Intn(batchMax-batchMin) + batchMin
						}
						msgs, _ := sub.Fetch(batchSize)
						for _, m := range msgs {
							mm, err := m.Metadata()
							mu.Lock()
							sequences[mm.Sequence.Stream] = struct{}{}
							mu.Unlock()
							require_NoError(t, err)
							_ = mm.Sequence.Stream
							i++
							if true {
								// if i%(rand.Intn(50)+1) != 0 || !chaos {
								m.Ack()
							} else {
								// if _, ok := missed[mseq]; !ok {
								// 	missed[mseq] = struct{}{}
								// fmt.Printf("Missed %v\n", config.Durable)
								// } else {
								// m.Ack()
								// }
							}
						}
					}

				}()
			}
		}()
	}

	cleanupSignal := make(chan struct{})

	consumersCount := 10
	go consumer(&nats.ConsumerConfig{
		Durable:         "edr:new",
		AckPolicy:       nats.AckExplicitPolicy,
		AckWait:         3 * time.Second,
		DeliverPolicy:   nats.DeliverAllPolicy,
		FilterSubject:   "Q.edr.new",
		MaxWaiting:      512,
		ReplayPolicy:    nats.ReplayInstantPolicy,
		SampleFrequency: "100%",
		Replicas:        1,
	}, consumersCount, 1, 1, cleanupSignal)

	go consumer(&nats.ConsumerConfig{
		Durable:         "edr:foo",
		AckPolicy:       nats.AckExplicitPolicy,
		AckWait:         3 * time.Second,
		DeliverPolicy:   nats.DeliverAllPolicy,
		FilterSubject:   "Q.edr.foo",
		MaxWaiting:      512,
		ReplayPolicy:    nats.ReplayInstantPolicy,
		SampleFrequency: "100%",
		Replicas:        1,
	}, consumersCount, 1, 1, cleanupSignal)

	go consumer(&nats.ConsumerConfig{
		Durable:         "old",
		AckPolicy:       nats.AckExplicitPolicy,
		AckWait:         3 * time.Second,
		DeliverPolicy:   nats.DeliverAllPolicy,
		FilterSubjects:  []string{"Q.old", "Q.bla"},
		MaxWaiting:      512,
		ReplayPolicy:    nats.ReplayInstantPolicy,
		SampleFrequency: "100%",
		Replicas:        1,
	}, consumersCount, 1, 1, cleanupSignal)

	pubTimer := time.NewTimer(3 * time.Minute)
	finallTimer := time.NewTimer(5 * time.Minute)

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

			cinfo, err = js.ConsumerInfo("QUEUE", "edr:foo")
			require_NoError(t, err)

			fmt.Printf("!!!!!foo:\n %+v\n", cinfo)

			fmt.Printf("SEQUENCES: %v\n", len(sequences))
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

		cinfo, err = js.ConsumerInfo("QUEUE", "edr:foo")
		require_NoError(t, err)

		fmt.Printf("!!!!!foo:\n %+v\n", cinfo)
	}
}
