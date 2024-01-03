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

//go:build !skip_js_tests
// +build !skip_js_tests

package server

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

func TestJetStreamConsumerMultipleFiltersRemoveFilters(t *testing.T) {

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Retention: LimitsPolicy,
		Subjects:  []string{"one", "two", "three"},
		MaxAge:    time.Second * 90,
	})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "consumer",
		FilterSubjects: []string{"one", "two"},
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "one", "data")
	sendStreamMsg(t, nc, "two", "data")
	sendStreamMsg(t, nc, "three", "data")

	consumer, err := js.PullSubscribe("", "consumer", nats.Bind("TEST", "consumer"))
	require_NoError(t, err)

	msgs, err := consumer.Fetch(1)
	require_NoError(t, err)
	require_True(t, len(msgs) == 1)

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "consumer",
		FilterSubjects: []string{},
	})
	require_NoError(t, err)

	msgs, err = consumer.Fetch(1)
	require_NoError(t, err)
	require_True(t, len(msgs) == 1)

	msgs, err = consumer.Fetch(1)
	require_NoError(t, err)
	require_True(t, len(msgs) == 1)

}

func TestJetStreamConsumerMultipleFiltersRace(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Retention: LimitsPolicy,
		Subjects:  []string{"one", "two", "three", "four"},
		MaxAge:    time.Second * 90,
	})
	require_NoError(t, err)

	var seqs []uint64
	var mu sync.Mutex

	for i := 0; i < 10_000; i++ {
		sendStreamMsg(t, nc, "one", "data")
		sendStreamMsg(t, nc, "two", "data")
		sendStreamMsg(t, nc, "three", "data")
		sendStreamMsg(t, nc, "four", "data")
	}

	mset.addConsumer(&ConsumerConfig{
		Durable:        "consumer",
		FilterSubjects: []string{"one", "two", "three"},
		AckPolicy:      AckExplicit,
	})

	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func(t *testing.T) {

			c, err := js.PullSubscribe("", "consumer", nats.Bind("TEST", "consumer"))
			require_NoError(t, err)

			for {
				select {
				case <-done:
					return
				default:
				}
				msgs, err := c.Fetch(10)
				// We don't want to stop before at expected number of messages, as we want
				// to also test against getting to many messages.
				// Because of that, we ignore timeout and connection closed errors.
				if err != nil && err != nats.ErrTimeout && err != nats.ErrConnectionClosed {
					t.Errorf("error while fetching messages: %v", err)
				}

				for _, msg := range msgs {
					info, err := msg.Metadata()
					require_NoError(t, err)
					mu.Lock()
					seqs = append(seqs, info.Sequence.Consumer)
					mu.Unlock()
					msg.Ack()
				}
			}
		}(t)
	}

	checkFor(t, time.Second*30, time.Second*1, func() error {
		mu.Lock()
		defer mu.Unlock()
		if len(seqs) != 30_000 {
			return fmt.Errorf("found %d messages instead of %d", len(seqs), 30_000)
		}
		sort.Slice(seqs, func(i, j int) bool {
			return seqs[i] < seqs[j]
		})
		for i := 1; i < len(seqs); i++ {
			if seqs[i] != seqs[i-1]+1 {
				fmt.Printf("seqs: %+v\n", seqs)
				return fmt.Errorf("sequence mismatch at %v", i)
			}
		}

		return nil
	})

	close(done)

}

func TestJetStreamConsumerMultipleConsumersSingleFilter(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	// Setup few subjects with varying messages count.
	subjects := []struct {
		subject  string
		messages int
		wc       bool
	}{
		{subject: "one", messages: 5000},
		{subject: "two", messages: 7500},
		{subject: "three", messages: 2500},
		{subject: "four", messages: 1000},
		{subject: "five.>", messages: 3000, wc: true},
	}

	totalMsgs := 0
	for _, subject := range subjects {
		totalMsgs += subject.messages
	}

	// Setup consumers, filtering some of the messages from the stream.
	consumers := []*struct {
		name         string
		subjects     []string
		expectedMsgs int
		delivered    atomic.Int32
	}{
		{name: "C1", subjects: []string{"one"}, expectedMsgs: 5000},
		{name: "C2", subjects: []string{"two"}, expectedMsgs: 7500},
		{name: "C3", subjects: []string{"one"}, expectedMsgs: 5000},
		{name: "C4", subjects: []string{"one"}, expectedMsgs: 5000},
	}

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Retention: LimitsPolicy,
		Subjects:  []string{"one", "two", "three", "four", "five.>"},
		MaxAge:    time.Second * 90,
	})
	require_NoError(t, err)

	for c, consumer := range consumers {
		_, err := mset.addConsumer(&ConsumerConfig{
			Durable:        consumer.name,
			FilterSubjects: consumer.subjects,
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverAll,
			AckWait:        time.Second * 30,
			DeliverSubject: nc.NewInbox(),
		})
		require_NoError(t, err)
		go func(c int, name string) {
			_, err = js.Subscribe("", func(m *nats.Msg) {
				require_NoError(t, m.Ack())
				require_NoError(t, err)
				consumers[c].delivered.Add(1)

			}, nats.Bind("TEST", name))
			require_NoError(t, err)
		}(c, consumer.name)
	}

	// Publish with random intervals, while consumers are active.
	var wg sync.WaitGroup
	for _, subject := range subjects {
		wg.Add(subject.messages)
		go func(subject string, messages int, wc bool) {
			nc, js := jsClientConnect(t, s)
			defer nc.Close()
			time.Sleep(time.Duration(rand.Int63n(1000)+1) * time.Millisecond)
			for i := 0; i < messages; i++ {
				time.Sleep(time.Duration(rand.Int63n(1000)+1) * time.Microsecond)
				// If subject has wildcard, add random last subject token.
				pubSubject := subject
				if wc {
					pubSubject = fmt.Sprintf("%v.%v", subject, rand.Int63n(10))
				}
				_, err := js.PublishAsync(pubSubject, []byte("data"))
				require_NoError(t, err)
				wg.Done()
			}
		}(subject.subject, subject.messages, subject.wc)
	}
	wg.Wait()

	checkFor(t, time.Second*10, time.Millisecond*500, func() error {
		for _, consumer := range consumers {
			info, err := js.ConsumerInfo("TEST", consumer.name)
			require_NoError(t, err)
			if info.Delivered.Consumer != uint64(consumer.expectedMsgs) {
				return fmt.Errorf("%v:expected consumer delivered seq %v, got %v. actually delivered: %v", consumer.name, consumer.expectedMsgs, info.Delivered.Consumer, consumer.delivered.Load())
			}
			if info.AckFloor.Consumer != uint64(consumer.expectedMsgs) {
				return fmt.Errorf("%v: expected consumer ack floor %v, got %v", consumer.name, totalMsgs, info.AckFloor.Consumer)
			}
			if consumer.delivered.Load() != int32(consumer.expectedMsgs) {

				return fmt.Errorf("%v: expected %v, got %v", consumer.name, consumer.expectedMsgs, consumer.delivered.Load())
			}
		}
		return nil
	})

}

func TestJetStreamConsumerMultipleConsumersMultipleFilters(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	// Setup few subjects with varying messages count.
	subjects := []struct {
		subject  string
		messages int
		wc       bool
	}{
		{subject: "one", messages: 50},
		{subject: "two", messages: 75},
		{subject: "three", messages: 250},
		{subject: "four", messages: 10},
		{subject: "five.>", messages: 300, wc: true},
	}

	totalMsgs := 0
	for _, subject := range subjects {
		totalMsgs += subject.messages
	}

	// Setup consumers, filtering some of the messages from the stream.
	consumers := []*struct {
		name         string
		subjects     []string
		expectedMsgs int
		delivered    atomic.Int32
	}{
		{name: "C1", subjects: []string{"one", "two"}, expectedMsgs: 125},
		{name: "C2", subjects: []string{"two", "three"}, expectedMsgs: 325},
		{name: "C3", subjects: []string{"one", "three"}, expectedMsgs: 300},
		{name: "C4", subjects: []string{"one", "five.>"}, expectedMsgs: 350},
	}

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Retention: LimitsPolicy,
		Subjects:  []string{"one", "two", "three", "four", "five.>"},
		MaxAge:    time.Second * 90,
	})
	require_NoError(t, err)

	for c, consumer := range consumers {
		_, err := mset.addConsumer(&ConsumerConfig{
			Durable:        consumer.name,
			FilterSubjects: consumer.subjects,
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverAll,
			AckWait:        time.Second * 30,
			DeliverSubject: nc.NewInbox(),
		})
		require_NoError(t, err)
		go func(c int, name string) {
			_, err = js.Subscribe("", func(m *nats.Msg) {
				require_NoError(t, m.Ack())
				require_NoError(t, err)
				consumers[c].delivered.Add(1)

			}, nats.Bind("TEST", name))
			require_NoError(t, err)
		}(c, consumer.name)
	}

	// Publish with random intervals, while consumers are active.
	var wg sync.WaitGroup
	for _, subject := range subjects {
		wg.Add(subject.messages)
		go func(subject string, messages int, wc bool) {
			nc, js := jsClientConnect(t, s)
			defer nc.Close()
			time.Sleep(time.Duration(rand.Int63n(1000)+1) * time.Millisecond)
			for i := 0; i < messages; i++ {
				time.Sleep(time.Duration(rand.Int63n(1000)+1) * time.Microsecond)
				// If subject has wildcard, add random last subject token.
				pubSubject := subject
				if wc {
					pubSubject = fmt.Sprintf("%v.%v", subject, rand.Int63n(10))
				}
				ack, err := js.PublishAsync(pubSubject, []byte("data"))
				require_NoError(t, err)
				go func() {
					ack.Ok()
					wg.Done()
				}()
			}
		}(subject.subject, subject.messages, subject.wc)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-time.After(time.Second * 15):
		t.Fatalf("Timed out waiting for acks")
	case <-done:
	}
	wg.Wait()

	checkFor(t, time.Second*15, time.Second*1, func() error {
		for _, consumer := range consumers {
			info, err := js.ConsumerInfo("TEST", consumer.name)
			require_NoError(t, err)
			if info.Delivered.Consumer != uint64(consumer.expectedMsgs) {
				return fmt.Errorf("%v:expected consumer delivered seq %v, got %v. actually delivered: %v", consumer.name, consumer.expectedMsgs, info.Delivered.Consumer, consumer.delivered.Load())
			}
			if info.AckFloor.Consumer != uint64(consumer.expectedMsgs) {
				return fmt.Errorf("%v: expected consumer ack floor %v, got %v", consumer.name, totalMsgs, info.AckFloor.Consumer)
			}
			if consumer.delivered.Load() != int32(consumer.expectedMsgs) {

				return fmt.Errorf("%v: expected %v, got %v", consumer.name, consumer.expectedMsgs, consumer.delivered.Load())
			}
		}
		return nil
	})

}

func TestJetStreamConsumerMultipleFiltersSequence(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Retention: LimitsPolicy,
		Subjects:  []string{"one", "two", "three", "four", "five.>"},
		MaxAge:    time.Second * 90,
	})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "DUR",
		FilterSubjects: []string{"one", "two"},
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverAll,
		AckWait:        time.Second * 30,
		DeliverSubject: nc.NewInbox(),
	})
	require_NoError(t, err)

	for i := 0; i < 20; i++ {
		sendStreamMsg(t, nc, "one", fmt.Sprintf("%d", i))
	}
	for i := 20; i < 40; i++ {
		sendStreamMsg(t, nc, "two", fmt.Sprintf("%d", i))
	}
	for i := 40; i < 60; i++ {
		sendStreamMsg(t, nc, "one", fmt.Sprintf("%d", i))
	}

	sub, err := js.SubscribeSync("", nats.Bind("TEST", "DUR"))
	require_NoError(t, err)

	for i := 0; i < 60; i++ {
		msg, err := sub.NextMsg(time.Second * 1)
		require_NoError(t, err)
		require_True(t, string(msg.Data) == fmt.Sprintf("%d", i))
	}
}

func TestJetStreamConsumerActions(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Retention: LimitsPolicy,
		Subjects:  []string{"one", "two", "three", "four", "five.>"},
		MaxAge:    time.Second * 90,
	})
	require_NoError(t, err)

	// Create Consumer. No consumers existed before, so should be fine.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "DUR",
		FilterSubjects: []string{"one", "two"},
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverAll,
		AckWait:        time.Second * 30,
	}, ActionCreate)
	require_NoError(t, err)
	// Create consumer again. Should be ok if action is CREATE but config is exactly the same.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "DUR",
		FilterSubjects: []string{"one", "two"},
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverAll,
		AckWait:        time.Second * 30,
	}, ActionCreate)
	require_NoError(t, err)
	// Create consumer again. Should error if action is CREATE.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "DUR",
		FilterSubjects: []string{"one"},
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverAll,
		AckWait:        time.Second * 30,
	}, ActionCreate)
	require_Error(t, err)

	// Update existing consumer. Should be fine, as consumer exists.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "DUR",
		FilterSubjects: []string{"one"},
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverAll,
		AckWait:        time.Second * 30,
	}, ActionUpdate)
	require_NoError(t, err)

	// Update consumer. Should error, as this consumer does not exist.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "NEW",
		FilterSubjects: []string{"one"},
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverAll,
		AckWait:        time.Second * 30,
	}, ActionUpdate)
	require_Error(t, err)

	// Create new ephemeral. Should be fine as the consumer doesn't exist already
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Name:           "EPH",
		FilterSubjects: []string{"one"},
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverAll,
		AckWait:        time.Second * 30,
	}, ActionCreate)
	require_NoError(t, err)

	// Trying to create it again right away. Should error as it already exists (and hasn't been cleaned up yet)
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Name:           "EPH",
		FilterSubjects: []string{"one"},
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverAll,
		AckWait:        time.Second * 30,
	}, ActionCreate)
	require_Error(t, err)
}

func TestJetStreamConsumerActionsOnWorkQueuePolicyStream(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Retention: WorkQueuePolicy,
		Subjects:  []string{"one", "two", "three", "four", "five.>"},
	})
	require_NoError(t, err)

	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "C1",
		FilterSubjects: []string{"one", "two"},
		AckPolicy:      AckExplicit,
	}, ActionCreate)
	require_NoError(t, err)

	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "C2",
		FilterSubjects: []string{"three", "four"},
		AckPolicy:      AckExplicit,
	}, ActionCreate)
	require_NoError(t, err)

	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "C3",
		FilterSubjects: []string{"five.*"},
		AckPolicy:      AckExplicit,
	}, ActionCreate)
	require_NoError(t, err)

	// Updating a consumer by removing a previous subject filter.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "C1",
		FilterSubjects: []string{"one"}, // Remove a subject.
		AckPolicy:      AckExplicit,
	}, ActionUpdate)
	require_NoError(t, err)

	// Updating a consumer without overlapping subjects.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "C2",
		FilterSubjects: []string{"three", "four", "two"}, // Add previously removed subject.
		AckPolicy:      AckExplicit,
	}, ActionUpdate)
	require_NoError(t, err)

	// Creating a consumer with overlapping subjects should return an error.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "C4",
		FilterSubjects: []string{"one", "two", "three", "four"},
		AckPolicy:      AckExplicit,
	}, ActionCreate)
	require_Error(t, err)
	if !IsNatsErr(err, JSConsumerWQConsumerNotUniqueErr) {
		t.Errorf("want error %q, got %q", ApiErrors[JSConsumerWQConsumerNotUniqueErr], err)
	}

	// Updating a consumer with overlapping subjects should return an error.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "C3",
		FilterSubjects: []string{"one", "two", "three", "four"},
		AckPolicy:      AckExplicit,
	}, ActionUpdate)
	require_Error(t, err)
	if !IsNatsErr(err, JSConsumerWQConsumerNotUniqueErr) {
		t.Errorf("want error %q, got %q", ApiErrors[JSConsumerWQConsumerNotUniqueErr], err)
	}
}

func TestJetStreamConsumerActionsViaAPI(t *testing.T) {

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	_, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Retention: LimitsPolicy,
		Subjects:  []string{"one"},
		MaxAge:    time.Second * 90,
	})
	require_NoError(t, err)

	// Update non-existing consumer, which should fail.
	request, err := json.Marshal(&CreateConsumerRequest{
		Action: ActionUpdate,
		Config: ConsumerConfig{
			Durable: "hello",
		},
		Stream: "TEST",
	})
	require_NoError(t, err)

	resp, err := nc.Request("$JS.API.CONSUMER.DURABLE.CREATE.TEST.hello", []byte(request), time.Second*6)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	require_Error(t, ccResp.Error)

	// create non existing consumer - which should be fine.
	ccResp.Error = nil
	request, err = json.Marshal(&CreateConsumerRequest{
		Action: ActionCreate,
		Config: ConsumerConfig{
			Durable: "hello",
		},
		Stream: "TEST",
	})
	require_NoError(t, err)

	resp, err = nc.Request("$JS.API.CONSUMER.DURABLE.CREATE.TEST.hello", []byte(request), time.Second*6)
	require_NoError(t, err)
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	if ccResp.Error != nil {
		t.Fatalf("expected nil, got %v", ccResp.Error)
	}

	// re-create existing consumer - which should be an error.
	ccResp.Error = nil
	request, err = json.Marshal(&CreateConsumerRequest{
		Action: ActionCreate,
		Config: ConsumerConfig{
			Durable:       "hello",
			FilterSubject: "one",
		},
		Stream: "TEST",
	})
	require_NoError(t, err)
	resp, err = nc.Request("$JS.API.CONSUMER.DURABLE.CREATE.TEST.hello", []byte(request), time.Second*6)
	require_NoError(t, err)
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	if ccResp.Error == nil {
		t.Fatalf("expected err, got nil")
	}

	// create a named ephemeral consumer
	ccResp.Error = nil
	request, err = json.Marshal(&CreateConsumerRequest{
		Action: ActionCreate,
		Config: ConsumerConfig{
			Name:          "ephemeral",
			FilterSubject: "one",
		},
		Stream: "TEST",
	})
	require_NoError(t, err)
	resp, err = nc.Request("$JS.API.CONSUMER.CREATE.TEST.ephemeral", []byte(request), time.Second*6)
	require_NoError(t, err)
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)

	// re-create existing consumer - which should be an error.
	ccResp.Error = nil
	request, err = json.Marshal(&CreateConsumerRequest{
		Action: ActionCreate,
		Config: ConsumerConfig{
			Name:          "ephemeral",
			FilterSubject: "one",
		},
		Stream: "TEST",
	})
	require_NoError(t, err)
	resp, err = nc.Request("$JS.API.CONSUMER.CREATE.TEST.ephemeral", []byte(request), time.Second*6)
	require_NoError(t, err)
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	if ccResp.Error == nil {
		t.Fatalf("expected err, got nil")
	}
}

func TestJetStreamConsumerActionsUnmarshal(t *testing.T) {
	tests := []struct {
		name      string
		given     []byte
		expected  ConsumerAction
		expectErr bool
	}{
		{name: "action create", given: []byte(`{"action": "create"}`), expected: ActionCreate},
		{name: "action update", given: []byte(`{"action": "update"}`), expected: ActionUpdate},
		{name: "no action", given: []byte("{}"), expected: ActionCreateOrUpdate},
		{name: "unknown", given: []byte(`{"action": "unknown"}`), expected: ActionCreateOrUpdate, expectErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			var request CreateConsumerRequest
			err := json.Unmarshal(test.given, &request)
			fmt.Printf("given: %v, expected: %v\n", test.expectErr, err)
			if !test.expectErr {
				require_NoError(t, err)
			} else {
				require_Error(t, err)
			}
			require_True(t, test.expected == request.Action)
		})
	}
}

func TestJetStreamConsumerMultipleFiltersLastPerSubject(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, error := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"one", "two"},
		Replicas: 3,
	})
	require_NoError(t, error)

	sendStreamMsg(t, nc, "one", "1")
	sendStreamMsg(t, nc, "one", "2")
	sendStreamMsg(t, nc, "one", "3")
	sendStreamMsg(t, nc, "two", "1")
	sendStreamMsg(t, nc, "two", "2")
	sendStreamMsg(t, nc, "two", "3")

	_, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name:           "C",
		FilterSubjects: []string{"one", "two"},
		DeliverPolicy:  nats.DeliverLastPerSubjectPolicy,
		Replicas:       3,
		DeliverSubject: "deliver",
	})
	require_NoError(t, err)

	consumer, err := js.SubscribeSync("", nats.Bind("TEST", "C"))
	require_NoError(t, err)

	// expect last message for subject "one"
	msg, err := consumer.NextMsg(time.Second)
	require_NoError(t, err)
	require_Equal(t, "3", string(msg.Data))
	require_Equal(t, "one", msg.Subject)

	// expect last message for subject "two"
	msg, err = consumer.NextMsg(time.Second)
	require_NoError(t, err)
	require_Equal(t, "3", string(msg.Data))
	require_Equal(t, "two", msg.Subject)

}

func consumerWithFilterSubjects(filterSubjects []string) *consumer {
	c := consumer{}
	for _, filter := range filterSubjects {
		sub := &subjectFilter{
			subject:          filter,
			hasWildcard:      subjectHasWildcard(filter),
			tokenizedSubject: tokenizeSubjectIntoSlice(nil, filter),
		}
		c.subjf = append(c.subjf, sub)
	}

	return &c
}

func filterSubjects(n int) []string {
	fs := make([]string, 0, n)
	for {
		literals := []string{"foo", "bar", nuid.Next(), "xyz", "abcdef"}
		fs = append(fs, strings.Join(literals, "."))
		if len(fs) == n {
			return fs
		}
		// Create more filterSubjects by going through the literals and replacing one with the '*' wildcard.
		l := len(literals)
		for i := 0; i < l; i++ {
			e := make([]string, l)
			for j := 0; j < l; j++ {
				if j == i {
					e[j] = "*"
				} else {
					e[j] = literals[j]
				}
			}
			fs = append(fs, strings.Join(e, "."))
			if len(fs) == n {
				return fs
			}
		}
	}
}

func TestJetStreamConsumerIsFilteredMatch(t *testing.T) {
	for _, test := range []struct {
		name           string
		filterSubjects []string
		subject        string
		result         bool
	}{
		{"no filter", []string{}, "foo.bar", true},
		{"literal match", []string{"foo.baz", "foo.bar"}, "foo.bar", true},
		{"literal mismatch", []string{"foo.baz", "foo.bar"}, "foo.ban", false},
		{"wildcard > match", []string{"bar.>", "foo.>"}, "foo.bar", true},
		{"wildcard > match", []string{"bar.>", "foo.>"}, "bar.foo", true},
		{"wildcard > mismatch", []string{"bar.>", "foo.>"}, "baz.foo", false},
		{"wildcard * match", []string{"bar.*", "foo.*"}, "foo.bar", true},
		{"wildcard * match", []string{"bar.*", "foo.*"}, "bar.foo", true},
		{"wildcard * mismatch", []string{"bar.*", "foo.*"}, "baz.foo", false},
		{"wildcard * match", []string{"foo.*.x", "foo.*.y"}, "foo.bar.x", true},
		{"wildcard * match", []string{"foo.*.x", "foo.*.y", "foo.*.z"}, "foo.bar.z", true},
		{"many mismatch", filterSubjects(100), "foo.bar.do.not.match.any.filter.subject", false},
		{"many match", filterSubjects(100), "foo.bar.12345.xyz.abcdef", true}, // will be matched by "foo.bar.*.xyz.abcdef"
	} {
		test := test

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			c := consumerWithFilterSubjects(test.filterSubjects)
			if res := c.isFilteredMatch(test.subject); res != test.result {
				t.Fatalf("Subject %q filtered match of %v, should be %v, got %v",
					test.subject, test.filterSubjects, test.result, res)
			}
		})
	}
}

func TestJetStreamConsumerIsEqualOrSubsetMatch(t *testing.T) {
	for _, test := range []struct {
		name           string
		filterSubjects []string
		subject        string
		result         bool
	}{
		{"no filter", []string{}, "foo.bar", false},
		{"literal match", []string{"foo.baz", "foo.bar"}, "foo.bar", true},
		{"literal mismatch", []string{"foo.baz", "foo.bar"}, "foo.ban", false},
		{"literal match", []string{"bar.>", "foo.>"}, "foo.>", true},
		{"subset match", []string{"bar.foo.>", "foo.bar.>"}, "bar.>", true},
		{"subset mismatch", []string{"bar.>", "foo.>"}, "baz.foo.>", false},
		{"literal match", filterSubjects(100), "foo.bar.*.xyz.abcdef", true},
		{"subset match", filterSubjects(100), "foo.bar.>", true},
	} {
		test := test

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			c := consumerWithFilterSubjects(test.filterSubjects)
			if res := c.isEqualOrSubsetMatch(test.subject); res != test.result {
				t.Fatalf("Subject %q subset match of %v, should be %v, got %v",
					test.subject, test.filterSubjects, test.result, res)
			}
		})
	}
}

func Benchmark____JetStreamConsumerIsFilteredMatch(b *testing.B) {
	subject := "foo.bar.do.not.match.any.filter.subject"
	for n := 1; n <= 1024; n *= 2 {
		name := fmt.Sprintf("%d filter subjects", int(n))
		c := consumerWithFilterSubjects(filterSubjects(int(n)))
		b.Run(name, func(b *testing.B) {
			c.isFilteredMatch(subject)
		})
	}
}
