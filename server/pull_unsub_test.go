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
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestPullConsumerMessageRedeliveryAfterUnsub(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	nc, err := nats.Connect(s.ClientURL())
	// nc, err := nats.Connect("localhost:4222")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	lock := sync.Mutex{}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer js.DeleteStream("TEST")

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "cons",
		AckPolicy: nats.AckExplicitPolicy,
		AckWait:   250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer js.DeleteConsumer("TEST", "cons")
	sub, err := js.PullSubscribe("foo", "", nats.Bind("TEST", "cons"))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	statusChanged := sub.StatusChanged(nats.SubscriptionClosed)
	stopPub := make(chan struct{})
	ackedMsgs := make(map[int]*nats.MsgMetadata)
	var maxSeen atomic.Uint32

	// subscriber
	go func() {
		for {
			msgs, err := sub.FetchBatch(300)
			if err != nil {
				if errors.Is(err, nats.ErrSubscriptionClosed) {
					// fetchingDone <- struct{}{}
					return
				}
				continue
			}
			for msg := range msgs.Messages() {
				acked, err := strconv.Atoi(string(msg.Data))
				if err != nil {
					fmt.Println("Error converting message to int: ", err)
					continue
				}
				if err := msg.Ack(); err != nil {
					fmt.Println("Error acking message: ", err)
				}
				meta, err := msg.Metadata()
				if err != nil {
					fmt.Println("Error getting metadata: ", err)
				}
				lock.Lock()
				ackedMsgs[acked] = meta
				lock.Unlock()
				if acked > int(maxSeen.Load()) {
					maxSeen.Store(uint32(acked))
				}
			}
		}
	}()

	// publisher
	go func() {
		var i int
		for {
			select {
			case <-stopPub:
				fmt.Println("Published messages: ", i)
				return
			default:
				i++
				if _, err := js.Publish("foo", []byte(fmt.Sprintf("%d", i))); err != nil {
					fmt.Println("Error publishing message: ", err)
					continue
				}
			}
		}
	}()
	time.Sleep(100 * time.Millisecond)

	// drain the subscription
	// sends `UNSUB` to the server, but subscriber should be able to process the
	// messages already received by the client
	sub.Drain()
	select {
	case status := <-statusChanged:
		if status != nats.SubscriptionClosed {
			t.Fatalf("Expected subscription to be closed, got %v", status)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Expected subscription to be closed, got %v", nats.SubscriptionClosed)
	}
	stopPub <- struct{}{}

	// resubscribe
	sub, err = js.PullSubscribe("foo", "", nats.Bind("TEST", "cons"))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// fetch remaining messages until all published messages are processed
	for {
		msgs, err := sub.FetchBatch(300, nats.MaxWait(500*time.Millisecond))
		if err != nil {
			if !errors.Is(err, nats.ErrTimeout) {
				t.Fatalf("Error fetching messages: %v", err)
			}
		}
		var i int
		for msg := range msgs.Messages() {
			i++
			num, err := strconv.Atoi(string(msg.Data))
			if err != nil {
				t.Fatalf("Error converting message to int: %v", err)
			}
			meta, err := msg.Metadata()
			if err != nil {
				t.Fatalf("Error getting metadata: %v", err)
			}
			// check if messages are out of order
			if num <= int(maxSeen.Load()) {
				t.Errorf("Messages out of order; current: %d; previous max: %d", num, maxSeen.Load())
			}

			if meta.NumDelivered != 1 {
				lock.Lock()
				// check if the message was previously delivered to the client
				if meta, ok := ackedMsgs[num]; ok {
					t.Errorf("Duplicate message: %d, %+v", num, meta)
				}
				lock.Unlock()
				info, err := js.ConsumerInfo("TEST", "cons")
				fmt.Printf("consumer info: %+v\n", info)
				if err != nil {
					t.Fatalf("Error getting consumer info: %v", err)
				}
				fmt.Printf("Metadata: %+v\n", meta)
				t.Errorf("Expected 1 delivered, got %d\n", meta.NumDelivered)
			}
			if err := msg.Ack(); err != nil {
				t.Fatalf("Error acking message: %v", err)
			}
			if num > int(maxSeen.Load()) {
				maxSeen.Store(uint32(num))
			}
		}
		if i == 0 {
			break
		}
	}
}
