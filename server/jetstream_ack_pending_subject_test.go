// Copyright 2023-2025 The NATS Authors
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

//go:build !skip_js_tests

package server

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamMaxAckPendingPerSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Create stream
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "MAPPS",
		Subjects: []string{"test.*"},
	})
	if err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	// Create consumer with limit 1
	req, _ := json.Marshal(&CreateConsumerRequest{
		Stream: "MAPPS",
		Config: ConsumerConfig{
			Durable:                 "CONS",
			AckPolicy:               AckExplicit,
			MaxAckPendingPerSubject: 1,
		},
	})
	resp, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "MAPPS", "CONS"), req, time.Second)
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}
	var ccResp JSApiConsumerCreateResponse
	json.Unmarshal(resp.Data, &ccResp)
	if ccResp.Error != nil {
		t.Fatalf("Consumer create error: %v", ccResp.Error)
	}

	// Publish 2 messages to same subject
	js.Publish("test.1", []byte("msg 1"))
	js.Publish("test.1", []byte("msg 2"))

	sub, err := js.PullSubscribe("", "CONS", nats.BindStream("MAPPS"))
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}

	// 1. Get first message
	msgs1, err := sub.Fetch(1, nats.MaxWait(time.Second))
	if err != nil || len(msgs1) != 1 {
		t.Fatalf("Expected 1 message, got %v (err: %v)", len(msgs1), err)
	}

	// 2. Try to get second message - should timeout because of limit 1
	msgs2, err := sub.Fetch(1, nats.MaxWait(200*time.Millisecond))
	if err == nil && len(msgs2) > 0 {
		t.Fatalf("Expected timeout because of limit, but got a message")
	}

	// 3. Ack first message
	msgs1[0].Ack()

	// 4. Try to get second message - should work now
	msgs3, err := sub.Fetch(1, nats.MaxWait(time.Second))
	if err != nil || len(msgs3) != 1 {
		t.Fatalf("Expected second message after ack, but got %v (err: %v)", len(msgs3), err)
	}
}

func TestJetStreamMaxAckPendingPerSubjectReactive(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Create stream
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "REACTIVE",
		Subjects: []string{"test.*"},
	})
	if err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	// Create consumer with limit 1
	req, _ := json.Marshal(&CreateConsumerRequest{
		Stream: "REACTIVE",
		Config: ConsumerConfig{
			Durable:                 "CONS",
			AckPolicy:               AckExplicit,
			MaxAckPendingPerSubject: 1,
		},
	})
	nc.Request(fmt.Sprintf(JSApiDurableCreateT, "REACTIVE", "CONS"), req, time.Second)

	// Publish to different subjects
	js.Publish("test.1", []byte("a1"))
	js.Publish("test.2", []byte("b1"))

	sub, err := js.PullSubscribe("", "CONS", nats.BindStream("REACTIVE"))
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}

	// Get a1
	msgs, _ := sub.Fetch(1, nats.MaxWait(time.Second))
	if len(msgs) != 1 || string(msgs[0].Data) != "a1" {
		t.Fatalf("Expected a1")
	}

	// Get b1 - should work because different subject
	msgs, err = sub.Fetch(1, nats.MaxWait(time.Second))
	if err != nil || len(msgs) != 1 || string(msgs[0].Data) != "b1" {
		t.Fatalf("Expected b1 (err: %v)", err)
	}
}

func BenchmarkMaxAckPendingPerSubjectFetch(b *testing.B) {
	s := RunBasicJetStreamServer(b)
	defer s.Shutdown()

	nc, js := jsClientConnect(b, s)
	defer nc.Close()

	js.AddStream(&nats.StreamConfig{
		Name:     "BENCH_FETCH",
		Subjects: []string{"test.*"},
	})

	req, _ := json.Marshal(&CreateConsumerRequest{
		Stream: "BENCH_FETCH",
		Config: ConsumerConfig{
			Durable:                 "CONS",
			AckPolicy:               AckExplicit,
			MaxAckPendingPerSubject: 50,
		},
	})
	nc.Request(fmt.Sprintf(JSApiDurableCreateT, "BENCH_FETCH", "CONS"), req, time.Second)

	// Pre-publish more to avoid constant restarts
	for i := 0; i < 5000; i++ {
		js.Publish("test.1", []byte("data"))
	}

	sub, _ := js.PullSubscribe("", "CONS", nats.BindStream("BENCH_FETCH"))

	groups := []string{"test.group_1", "test.group_2", "test.group_3"}
	unacked := make(map[string]int)
	maxUnacked := make(map[string]int)
	for _, g := range groups {
		unacked[g] = 0
		maxUnacked[g] = 0
	}

	b.ResetTimer()
	for i := 0; i < b.N; {
		batch := 100
		if i+batch > b.N {
			batch = b.N - i
		}
		msgs, err := sub.Fetch(batch, nats.MaxWait(500*time.Millisecond))
		if err == nil && len(msgs) > 0 {
			for _, m := range msgs {
				subj := m.Subject
				unacked[subj]++
				if unacked[subj] > maxUnacked[subj] {
					maxUnacked[subj] = unacked[subj]
				}
			}
			for _, m := range msgs {
				m.Ack()
				unacked[m.Subject]--
			}
			i += len(msgs)
		} else {
			i++ // avoid infinite loop if no msgs
		}

		if i > 0 && i%5000 == 0 {
			fmt.Printf("Fetch progress: %d/%d (Max Pending: group_1=%d, group_2=%d, group_3=%d)\n",
				i, b.N, maxUnacked["test.group_1"], maxUnacked["test.group_2"], maxUnacked["test.group_3"])
			b.StopTimer()
			for _, g := range groups {
				for j := 0; j < 2000; j++ {
					js.Publish(g, []byte("data"))
				}
			}
			b.StartTimer()
		}
	}
}

func BenchmarkMaxAckPendingPerSubjectCallback(b *testing.B) {
	s := RunBasicJetStreamServer(b)
	defer s.Shutdown()

	nc, js := jsClientConnect(b, s)
	defer nc.Close()

	js.AddStream(&nats.StreamConfig{
		Name:     "BENCH_CB",
		Subjects: []string{"test.*"},
	})

	// Use a Push consumer for callback benchmark
	req, _ := json.Marshal(&CreateConsumerRequest{
		Stream: "BENCH_CB",
		Config: ConsumerConfig{
			Durable:                 "CONS",
			DeliverSubject:          "ds",
			AckPolicy:               AckExplicit,
			MaxAckPendingPerSubject: 50,
		},
	})
	nc.Request(fmt.Sprintf(JSApiDurableCreateT, "BENCH_CB", "CONS"), req, time.Second)

	done := make(chan struct{})
	var count int32
	groups := []string{"test.group_1", "test.group_2", "test.group_3"}
	var unacked sync.Map    // string -> *int32
	var maxUnacked sync.Map // string -> *int32

	for _, g := range groups {
		var u int32 = 0
		var m int32 = 0
		unacked.Store(g, &u)
		maxUnacked.Store(g, &m)
	}

	// Use js.Subscribe which will bind to the push consumer
	sub, _ := js.Subscribe("test.*", func(m *nats.Msg) {
		subj := m.Subject
		uPtr, _ := unacked.Load(subj)
		mPtr, _ := maxUnacked.Load(subj)
		u := uPtr.(*int32)
		mx := mPtr.(*int32)

		atomic.AddInt32(u, 1)
		currU := atomic.LoadInt32(u)
		for {
			prevM := atomic.LoadInt32(mx)
			if currU <= prevM || atomic.CompareAndSwapInt32(mx, prevM, currU) {
				break
			}
		}

		// Simulate minor processing delay to let unacked build up
		if currU < 50 {
			time.Sleep(1 * time.Microsecond)
		}

		m.Ack()
		atomic.AddInt32(u, -1)

		curr := atomic.AddInt32(&count, 1)
		if int(curr)%5000 == 0 {
			m1, _ := maxUnacked.Load("test.group_1")
			m2, _ := maxUnacked.Load("test.group_2")
			m3, _ := maxUnacked.Load("test.group_3")
			fmt.Printf("Callback progress: %d/%d (Max Pending: group_1=%d, group_2=%d, group_3=%d)\n",
				curr, b.N, atomic.LoadInt32(m1.(*int32)), atomic.LoadInt32(m2.(*int32)), atomic.LoadInt32(m3.(*int32)))
		}
		if int(curr) >= b.N {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	}, nats.BindStream("BENCH_CB"), nats.Durable("CONS"), nats.ManualAck())

	b.ResetTimer()
	// Publish in a separate goroutine
	go func() {
		for i := 0; i < b.N; i++ {
			subj := groups[i%len(groups)]
			js.Publish(subj, []byte("data"))
			if (i+1)%5000 == 0 {
				fmt.Printf("Publish progress: %d/%d\n", i+1, b.N)
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		b.Fatalf("Benchmark timed out at %d/%d", atomic.LoadInt32(&count), b.N)
	}
	sub.Unsubscribe()
}

func TestJetStreamMaxAckPendingPerSubjectVisual(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	js.AddStream(&nats.StreamConfig{
		Name:     "VISUAL",
		Subjects: []string{"test.*"},
	})

	// Set limit to 50
	req, _ := json.Marshal(&CreateConsumerRequest{
		Stream: "VISUAL",
		Config: ConsumerConfig{
			Durable:                 "CONS",
			AckPolicy:               AckExplicit,
			MaxAckPendingPerSubject: 50,
		},
	})
	nc.Request(fmt.Sprintf(JSApiDurableCreateT, "VISUAL", "CONS"), req, time.Second)

	// Publish 100 per group, interleaved
	groups := []string{"test.group_1", "test.group_2", "test.group_3"}
	for i := 0; i < 100; i++ {
		for _, g := range groups {
			js.Publish(g, []byte("data"))
		}
	}

	sub, _ := js.PullSubscribe("", "CONS", nats.BindStream("VISUAL"))

	// Fetch as many as possible
	fmt.Printf("\n--- Visual Proof of Per-Subject Limits (Limit: 50) ---\n")
	counts := make(map[string]int)
	for _, g := range groups {
		counts[g] = 0
	}

	// Try to fetch 300 messages total (100 per group available)
	msgs, _ := sub.Fetch(300, nats.MaxWait(500*time.Millisecond))
	for _, m := range msgs {
		counts[m.Subject]++
	}

	for _, g := range groups {
		fmt.Printf("Subject: %s | Received: %d | Limit: 50\n", g, counts[g])
		if counts[g] > 50 {
			t.Errorf("Subject %s exceeded limit! Got %d", g, counts[g])
		}
	}
	fmt.Printf("---------------------------------------------------\n")
}
