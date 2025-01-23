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
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strconv"
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

	total := 10_000
	var wg sync.WaitGroup

	send := func(subj string) {
		defer wg.Done()
		for i := 0; i < total; i++ {
			sendStreamMsg(t, nc, subj, "data")
		}
	}
	wg.Add(4)
	go send("one")
	go send("two")
	go send("three")
	go send("four")
	wg.Wait()

	mset.addConsumer(&ConsumerConfig{
		Durable:        "consumer",
		FilterSubjects: []string{"one", "two", "three"},
		AckPolicy:      AckExplicit,
	})

	done := make(chan struct{})
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(t *testing.T) {
			defer wg.Done()

			c, err := js.PullSubscribe(_EMPTY_, "consumer", nats.Bind("TEST", "consumer"))
			require_NoError(t, err)

			for {
				select {
				case <-done:
					return
				default:
				}
				msgs, err := c.Fetch(10, nats.MaxWait(2*time.Second))
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

	checkFor(t, 30*time.Second, 100*time.Millisecond, func() error {
		mu.Lock()
		defer mu.Unlock()
		if len(seqs) != 3*total {
			return fmt.Errorf("found %d messages instead of %d", len(seqs), 3*total)
		}
		slices.Sort(seqs)
		for i := 1; i < len(seqs); i++ {
			if seqs[i] != seqs[i-1]+1 {
				fmt.Printf("seqs: %+v\n", seqs)
				return fmt.Errorf("sequence mismatch at %v", i)
			}
		}
		return nil
	})
	close(done)
	wg.Wait()
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
				return fmt.Errorf("%v:expected consumer delivered seq %v, got %v. actually delivered: %v",
					consumer.name, consumer.expectedMsgs, info.Delivered.Consumer, consumer.delivered.Load())
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
				return fmt.Errorf("%v:expected consumer delivered seq %v, got %v. actually delivered: %v",
					consumer.name, consumer.expectedMsgs, info.Delivered.Consumer, consumer.delivered.Load())
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

func TestJetStreamConsumerWorkQueuePolicyOverlap(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.*.*"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "ConsumerA",
		FilterSubject: "foo.bar.*",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "ConsumerB",
		FilterSubject: "foo.*.bar",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_Error(t, err)
	require_True(t, strings.Contains(err.Error(), "unique"))
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

func TestJetStreamConsumerBackOff(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for _, test := range []struct {
		name      string
		config    nats.ConsumerConfig
		shouldErr bool
	}{
		{
			name: "backoff_with_max_deliver",
			config: nats.ConsumerConfig{
				MaxDeliver: 3,
				BackOff:    []time.Duration{time.Second, time.Minute},
			},
			shouldErr: false,
		},
		{
			name: "backoff_with_max_deliver_equal",
			config: nats.ConsumerConfig{
				MaxDeliver: 3,
				BackOff:    []time.Duration{time.Second, time.Minute, time.Hour},
			},
			shouldErr: false,
		},
		{
			name: "backoff_with_max_deliver_equal_to_zero",
			config: nats.ConsumerConfig{
				MaxDeliver: 0,
				BackOff:    []time.Duration{},
			},
			shouldErr: false,
		},
		{
			name: "backoff_with_max_deliver_smaller",
			config: nats.ConsumerConfig{
				MaxDeliver: 2,
				BackOff:    []time.Duration{time.Second, time.Minute, time.Hour},
			},
			shouldErr: true,
		},
		{
			name: "backoff_with_default_max_deliver",
			config: nats.ConsumerConfig{
				BackOff: []time.Duration{time.Second, time.Minute, time.Hour},
			},
			shouldErr: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     test.name,
				Subjects: []string{test.name},
			})
			require_NoError(t, err)

			_, err = js.AddConsumer(test.name, &test.config)
			require_True(t, test.shouldErr == (err != nil))
			if test.shouldErr {
				require_True(t, strings.Contains(err.Error(), "max deliver"))
			}

			// test if updating consumers works too.
			test.config.Durable = "consumer"
			_, err = js.AddConsumer(test.name, &nats.ConsumerConfig{
				Durable: test.config.Durable,
			})
			require_NoError(t, err)

			test.config.Description = "Updated"
			_, err = js.UpdateConsumer(test.name, &test.config)
			require_True(t, test.shouldErr == (err != nil))
			if test.shouldErr {
				require_True(t, strings.Contains(err.Error(), "max deliver"))
			}
		})

	}
}

func TestJetStreamConsumerDelete(t *testing.T) {
	tests := []struct {
		name     string
		replicas int
	}{
		{"single server", 1},
		{"clustered", 3},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var s *Server
			if test.replicas == 1 {
				s = RunBasicJetStreamServer(t)
				defer s.Shutdown()
			} else {
				c := createJetStreamClusterExplicit(t, "R3S", test.replicas)
				defer c.shutdown()
				s = c.randomServer()
			}

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"events.>"},
				MaxAge:   time.Second * 90,
				Replicas: test.replicas,
			})
			require_NoError(t, err)

			_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
				Durable:       "consumer",
				FilterSubject: "events.>",
				Replicas:      test.replicas,
			})
			require_NoError(t, err)

			js.Publish("events.1", []byte("hello"))

			cr := JSApiConsumerGetNextRequest{
				Batch:   10,
				Expires: time.Second * 30,
			}
			crBytes, err := json.Marshal(cr)
			require_NoError(t, err)

			inbox := nats.NewInbox()
			consumerSub, err := nc.SubscribeSync(inbox)
			require_NoError(t, err)

			err = nc.PublishRequest(fmt.Sprintf(JSApiRequestNextT, "TEST", "consumer"), inbox, crBytes)
			require_NoError(t, err)

			msg, err := consumerSub.NextMsg(time.Second * 30)
			require_NoError(t, err)
			require_Equal(t, "hello", string(msg.Data))

			js.DeleteConsumer("TEST", "consumer")

			msg, err = consumerSub.NextMsg(time.Second * 30)
			require_NoError(t, err)

			if !strings.Contains(string(msg.Header.Get("Description")), "Consumer Deleted") {
				t.Fatalf("Expected exclusive consumer error, got %q", msg.Header.Get("Description"))
			}
		})

	}
}

func TestJetStreamConsumerFetchWithDrain(t *testing.T) {
	t.Skip()

	test := func(t *testing.T, cc *nats.ConsumerConfig) {
		s := RunBasicJetStreamServer(t)
		defer s.Shutdown()

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:      "TEST",
			Subjects:  []string{"foo"},
			Retention: nats.LimitsPolicy,
		})
		require_NoError(t, err)

		_, err = js.AddConsumer("TEST", cc)
		require_NoError(t, err)

		const messages = 10_000

		for i := 0; i < messages; i++ {
			sendStreamMsg(t, nc, "foo", fmt.Sprintf("%d", i+1))
		}

		cr := JSApiConsumerGetNextRequest{
			Batch:   100_000,
			Expires: 10 * time.Second,
		}
		crBytes, err := json.Marshal(cr)
		require_NoError(t, err)

		msgs := make(map[int]int)

		processMsg := func(t *testing.T, sub *nats.Subscription, msgs map[int]int) bool {
			msg, err := sub.NextMsg(time.Second)
			if err != nil {
				return false
			}
			metadata, err := msg.Metadata()
			require_NoError(t, err)
			require_NoError(t, msg.Ack())

			v, err := strconv.Atoi(string(msg.Data))
			require_NoError(t, err)
			require_Equal(t, uint64(v), metadata.Sequence.Stream)

			if _, ok := msgs[int(metadata.Sequence.Stream-1)]; !ok && len(msgs) > 0 {
				t.Logf("Stream Sequence gap detected: current %d", metadata.Sequence.Stream)
			}
			if _, ok := msgs[int(metadata.Sequence.Stream)]; ok {
				t.Fatalf("Message for seq %d has been seen before", metadata.Sequence.Stream)
			}
			// We do not expect official redeliveries here so this should always be 1.
			if metadata.NumDelivered != 1 {
				t.Errorf("Expected NumDelivered of 1, got %d for seq %d",
					metadata.NumDelivered, metadata.Sequence.Stream)
			}
			msgs[int(metadata.Sequence.Stream)] = int(metadata.NumDelivered)
			return true
		}

		for {
			inbox := nats.NewInbox()
			sub, err := nc.SubscribeSync(inbox)
			require_NoError(t, err)

			err = nc.PublishRequest(fmt.Sprintf(JSApiRequestNextT, "TEST", "C"), inbox, crBytes)
			require_NoError(t, err)

			// Drain after first message processed.
			processMsg(t, sub, msgs)
			sub.Drain()

			for {
				if !processMsg(t, sub, msgs) {
					if len(msgs) == messages {
						return
					}
					break
				}
			}
		}
	}

	t.Run("no-backoff", func(t *testing.T) {
		test(t, &nats.ConsumerConfig{
			Durable:   "C",
			AckPolicy: nats.AckExplicitPolicy,
			AckWait:   20 * time.Second,
		})
	})
	t.Run("with-backoff", func(t *testing.T) {
		test(t, &nats.ConsumerConfig{
			Durable:   "C",
			AckPolicy: nats.AckExplicitPolicy,
			AckWait:   20 * time.Second,
			BackOff:   []time.Duration{25 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond},
		})
	})
}

func TestJetStreamConsumerLongSubjectHang(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	readSubj := "a1."
	purgeSubj := "a2."
	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{readSubj + ">", purgeSubj + ">"},
		AllowRollup: true,
	})
	require_NoError(t, err)

	prefix := strings.Repeat("a", 22)
	for i := 0; i < 2; i++ {
		subj := readSubj + prefix + fmt.Sprintf("%d", i)
		_, err = js.Publish(subj, []byte("hello"))
		require_NoError(t, err)
		chunkSubj := purgeSubj + fmt.Sprintf("%d", i)
		_, err = js.Publish(chunkSubj, []byte("contents"))
		require_NoError(t, err)
	}
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Subject: purgeSubj + ">"})
	require_NoError(t, err)

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	// we should have 2 msgs left after purge
	require_Equal(t, si.State.Msgs, 2)

	sub, err := js.SubscribeSync(readSubj+">", nats.OrderedConsumer(), nats.DeliverLastPerSubject())
	require_NoError(t, err)
	defer sub.Unsubscribe()

	for i := 0; i < 2; i++ {
		m, err := sub.NextMsg(500 * time.Millisecond)
		require_NoError(t, err)
		require_True(t, string(m.Data) == "hello")
	}
}

func TestJetStreamConsumerStuckAckPending(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	type ActiveWorkItem struct {
		ID     int
		Expiry time.Time
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "TEST_ACTIVE_WORK_ITEMS",
		Discard:           nats.DiscardOld,
		MaxMsgsPerSubject: 1,
		Subjects:          []string{"TEST_ACTIVE_WORK_ITEMS.>"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST_ACTIVE_WORK_ITEMS", &nats.ConsumerConfig{
		Durable:       "testactiveworkitemsconsumer",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxAckPending: -1,
		MaxWaiting:    20000,
		AckWait:       15 * time.Second,
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("TEST_ACTIVE_WORK_ITEMS.>", "testactiveworkitemsconsumer", nats.BindStream("TEST_ACTIVE_WORK_ITEMS"))
	require_NoError(t, err)

	errs := make(chan error)
	go func() {
		for {
			msgs, err := sub.Fetch(200)
			if err != nil {
				// test is done. stop the loop.
				if errors.Is(err, nats.ErrSubscriptionClosed) || errors.Is(err, nats.ErrConnectionClosed) {
					return
				}
				if !errors.Is(err, nats.ErrTimeout) {
					errs <- err
					return
				}
				continue
			}
			for _, msg := range msgs {
				msg := msg
				var workItem ActiveWorkItem
				if err := json.Unmarshal(msg.Data, &workItem); err != nil {
					errs <- err
					return
				}

				now := time.Now()
				// If the work item has not expired, nak it with the respective delay.
				if workItem.Expiry.After(now) {
					msg.NakWithDelay(workItem.Expiry.Sub(now))
				} else {
					msg.Ack()
				}
			}
		}
	}()

	for i := 0; i < 25_000; i++ {
		// Publish item to TEST_ACTIVE_WORK_ITEMS stream with an expiry time.
		workItem := ActiveWorkItem{ID: i, Expiry: time.Now().Add(30 * time.Second)}
		data, err := json.Marshal(workItem)
		require_NoError(t, err)

		_, err = js.Publish(fmt.Sprintf("TEST_ACTIVE_WORK_ITEMS.%d", i), data)
		require_NoError(t, err)

		// Update expiry time and republish item to TEST_ACTIVE_WORK_ITEMS stream.
		workItem.Expiry = time.Now().Add(3 * time.Second)
		data, err = json.Marshal(workItem)
		require_NoError(t, err)
		_, err = js.Publish(fmt.Sprintf("TEST_ACTIVE_WORK_ITEMS.%d", i), data)
		require_NoError(t, err)
	}
	noChange := false
	lastNumAckPending := 0
	checkFor(t, 60*time.Second, 3*time.Second, func() error {
		select {
		case err := <-errs:
			t.Fatalf("consumer goroutine failed: %v", err)
		default:
		}
		ci, err := js.ConsumerInfo("TEST_ACTIVE_WORK_ITEMS", "testactiveworkitemsconsumer")
		require_NoError(t, err)

		if lastNumAckPending != 0 && lastNumAckPending == ci.NumAckPending {
			noChange = true
		}
		lastNumAckPending = ci.NumAckPending

		// If we have no change since last check, we can fail the test before `totalWait` timeout.
		if ci.NumAckPending > 0 && ci.NumPending == 0 {
			if noChange {
				_, err := sub.Fetch(1)
				if err != nil && errors.Is(err, nats.ErrTimeout) {

					t.Fatalf("num ack pending: %d\t num pending: %v\n", ci.NumAckPending, ci.NumPending)
				}
			}
			return fmt.Errorf("num ack pending: %d\t num pending: %v\n", ci.NumAckPending, ci.NumPending)
		}
		return nil
	})
}

func TestJetStreamConsumerMultipleFitersWithStartDate(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	past := time.Now().Add(-90 * time.Second)

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"events.>"},
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "events.foo", "msg-1")
	sendStreamMsg(t, nc, "events.bar", "msg-2")
	sendStreamMsg(t, nc, "events.baz", "msg-3")
	sendStreamMsg(t, nc, "events.biz", "msg-4")
	sendStreamMsg(t, nc, "events.faz", "msg-5")
	sendStreamMsg(t, nc, "events.foo", "msg-6")
	sendStreamMsg(t, nc, "events.biz", "msg-7")

	for _, test := range []struct {
		name                   string
		filterSubjects         []string
		startTime              time.Time
		expectedMessages       uint64
		expectedStreamSequence uint64
	}{
		{"Single-Filter-first-sequence", []string{"events.foo"}, past, 2, 0},
		{"Multiple-Filter-first-sequence", []string{"events.foo", "events.bar", "events.baz"}, past, 4, 0},
		{"Multiple-Filters-second-subject", []string{"events.bar", "events.baz"}, past, 2, 1},
		{"Multiple-Filters-first-last-subject", []string{"events.foo", "events.biz"}, past, 4, 0},
		{"Multiple-Filters-in-future", []string{"events.foo", "events.biz"}, time.Now().Add(1 * time.Minute), 0, 7},
	} {
		t.Run(test.name, func(t *testing.T) {
			info, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
				Durable:        test.name,
				FilterSubjects: test.filterSubjects,
				DeliverPolicy:  nats.DeliverByStartTimePolicy,
				OptStartTime:   &test.startTime,
			})
			require_NoError(t, err)
			require_Equal(t, test.expectedStreamSequence, info.Delivered.Stream)
			require_Equal(t, test.expectedMessages, info.NumPending)
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

// https://github.com/nats-io/nats-server/issues/6085
func TestJetStreamConsumerBackoffNotRespectedWithMultipleInflightRedeliveries(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"events.>"},
	})
	require_NoError(t, err)

	maxDeliver := 3
	backoff := []time.Duration{2 * time.Second, 4 * time.Second}
	sub, err := js.SubscribeSync(
		"events.>",
		nats.MaxDeliver(maxDeliver),
		nats.BackOff(backoff),
		nats.AckExplicit(),
	)
	require_NoError(t, err)

	calculateExpectedBackoff := func(numDelivered int) time.Duration {
		expectedBackoff := 500 * time.Millisecond
		for i := 0; i < numDelivered-1 && i < len(backoff); i++ {
			expectedBackoff += backoff[i]
		}
		return expectedBackoff
	}

	// We get one message to be redelivered using the final backoff duration.
	firstMsgSent := time.Now()
	sendStreamMsg(t, nc, "events.first", "msg-1")
	_, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_LessThan(t, time.Since(firstMsgSent), calculateExpectedBackoff(1))
	_, err = sub.NextMsg(5 * time.Second)
	require_NoError(t, err)
	require_LessThan(t, time.Since(firstMsgSent), calculateExpectedBackoff(2))
	// This message will be redelivered with the final/highest backoff below.

	// If we now send a new message, the pending timer should be reset to the first backoff.
	// Otherwise, if it remains at the final backoff duration we'll get this message redelivered too late.
	sendStreamMsg(t, nc, "events.second", "msg-2")

	for {
		msg, err := sub.NextMsg(5 * time.Second)
		require_NoError(t, err)
		if msg.Subject == "events.first" {
			require_LessThan(t, time.Since(firstMsgSent), calculateExpectedBackoff(3))
			continue
		}

		// We expect the second message to be redelivered using the specified backoff strategy.
		// Before, the first redelivery of the second message would be sent after the highest backoff duration.
		metadata, err := msg.Metadata()
		require_NoError(t, err)
		numDelivered := int(metadata.NumDelivered)
		expectedBackoff := calculateExpectedBackoff(numDelivered)
		require_LessThan(t, time.Since(metadata.Timestamp), expectedBackoff)

		// We've received all message, test passed.
		if numDelivered >= maxDeliver {
			break
		}
	}
}

func TestJetStreamConsumerBackoffWhenBackoffLengthIsEqualToMaxDeliverConfig(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"events.>"},
	})
	require_NoError(t, err)

	maxDeliver := 3
	backoff := []time.Duration{time.Second, 2 * time.Second, 3 * time.Second}
	sub, err := js.SubscribeSync(
		"events.>",
		nats.MaxDeliver(maxDeliver),
		nats.BackOff(backoff),
		nats.AckExplicit(),
	)
	require_NoError(t, err)

	calculateExpectedBackoff := func(numDelivered int) time.Duration {
		return backoff[numDelivered-1] + 50*time.Millisecond // 50ms of margin to system overhead
	}

	// message to be redelivered using backoff duration.
	firstMsgSent := time.Now()
	sendStreamMsg(t, nc, "events.first", "msg-1")
	_, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_LessThan(t, time.Since(firstMsgSent), calculateExpectedBackoff(1))
	_, err = sub.NextMsg(2 * time.Second)
	require_NoError(t, err)
	require_LessThan(t, time.Since(firstMsgSent), calculateExpectedBackoff(2))
	_, err = sub.NextMsg(3 * time.Second)
	require_NoError(t, err)
	require_LessThan(t, time.Since(firstMsgSent), calculateExpectedBackoff(3))
}

func TestJetStreamConsumerRetryAckAfterTimeout(t *testing.T) {
	for _, ack := range []struct {
		title  string
		policy nats.SubOpt
	}{
		{title: "AckExplicit", policy: nats.AckExplicit()},
		{title: "AckAll", policy: nats.AckAll()},
	} {
		t.Run(ack.title, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
			})
			require_NoError(t, err)

			_, err = js.Publish("foo", nil)
			require_NoError(t, err)

			sub, err := js.PullSubscribe("foo", "CONSUMER", ack.policy)
			require_NoError(t, err)

			msgs, err := sub.Fetch(1)
			require_NoError(t, err)
			require_Len(t, len(msgs), 1)

			msg := msgs[0]
			// Send core request so the client is unaware of the ack being sent.
			_, err = nc.Request(msg.Reply, nil, time.Second)
			require_NoError(t, err)

			// It could be we have already acked this specific message, but we haven't received the success response.
			// Retrying the ack should not time out and still signal success.
			err = msg.AckSync()
			require_NoError(t, err)
		})
	}
}

func TestJetStreamConsumerSwitchLeaderDuringInflightAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	for i := 0; i < 2_000; i++ {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	sub, err := js.PullSubscribe(
		"foo",
		"CONSUMER",
		nats.MaxAckPending(2_000),
		nats.ManualAck(),
		nats.AckExplicit(),
		nats.AckWait(2*time.Second),
	)
	require_NoError(t, err)

	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)

	msgs, err := sub.Fetch(2_000)
	require_NoError(t, err)
	require_Len(t, len(msgs), 2_000)

	// Simulate an ack being pushed, and o.setLeader(false) being called before the ack is processed and resets o.awl
	atomic.AddInt64(&o.awl, 1)
	o.setLeader(false)
	o.setLeader(true)

	msgs, err = sub.Fetch(1, nats.MaxWait(5*time.Second))
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
}

func TestJetStreamConsumerMessageDeletedDuringRedelivery(t *testing.T) {
	storageTypes := []nats.StorageType{nats.MemoryStorage, nats.FileStorage}
	for _, storageType := range storageTypes {
		t.Run(storageType.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Storage:  storageType,
			})
			require_NoError(t, err)

			for i := 0; i < 3; i++ {
				_, err = js.Publish("foo", nil)
				require_NoError(t, err)
			}

			sub, err := js.PullSubscribe(
				"foo",
				"CONSUMER",
				nats.ManualAck(),
				nats.AckExplicit(),
				nats.AckWait(time.Second),
			)
			require_NoError(t, err)

			acc, err := s.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)
			o := mset.lookupConsumer("CONSUMER")
			require_NotNil(t, o)

			msgs, err := sub.Fetch(3)
			require_NoError(t, err)
			require_Len(t, len(msgs), 3)

			err = js.DeleteMsg("TEST", 2)
			require_NoError(t, err)

			// Wait for mset.storeUpdates to call into o.decStreamPending which runs
			// the o.processTerm goroutine, removing one message from pending.
			checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
				o.mu.RLock()
				defer o.mu.RUnlock()
				if len(o.pending) != 2 {
					return fmt.Errorf("expected 2 pending, but got %d", len(o.pending))
				}
				return nil
			})

			// Now empty the redelivery queue and reset the pending state.
			o.mu.Lock()
			for _, seq := range o.rdq {
				o.removeFromRedeliverQueue(seq)
			}
			o.pending = make(map[uint64]*Pending)
			o.pending[2] = &Pending{}
			o.addToRedeliverQueue(2)

			// Also reset delivery/ack floors to confirm they get corrected.
			o.adflr, o.asflr = 0, 0
			o.dseq, o.sseq = 11, 11

			// Getting the next message should skip seq 2, as that's deleted, but must not touch state.
			_, _, err = o.getNextMsg()
			o.mu.Unlock()
			require_Error(t, err, ErrStoreEOF)
			require_Len(t, len(o.pending), 1)

			// Simulate the o.processTerm goroutine running after a call to o.getNextMsg.
			// Pending state and delivery/ack floors should be corrected.
			o.processTerm(2, 2, 1, ackTermUnackedLimitsReason, _EMPTY_)

			o.mu.RLock()
			defer o.mu.RUnlock()
			require_Len(t, len(o.pending), 0)
			require_Equal(t, o.adflr, 10)
			require_Equal(t, o.asflr, 10)
		})
	}
}
