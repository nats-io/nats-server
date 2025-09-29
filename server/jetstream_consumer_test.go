// Copyright 2022-2025 The NATS Authors
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

//go:build !skip_js_tests && !skip_js_consumer_tests

package server

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

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
	}, ActionCreate, false)
	require_NoError(t, err)
	// Create consumer again. Should be ok if action is CREATE but config is exactly the same.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "DUR",
		FilterSubjects: []string{"one", "two"},
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverAll,
		AckWait:        time.Second * 30,
	}, ActionCreate, false)
	require_NoError(t, err)
	// Create consumer again. Should error if action is CREATE.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "DUR",
		FilterSubjects: []string{"one"},
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverAll,
		AckWait:        time.Second * 30,
	}, ActionCreate, false)
	require_Error(t, err)

	// Update existing consumer. Should be fine, as consumer exists.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "DUR",
		FilterSubjects: []string{"one"},
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverAll,
		AckWait:        time.Second * 30,
	}, ActionUpdate, false)
	require_NoError(t, err)

	// Update consumer. Should error, as this consumer does not exist.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "NEW",
		FilterSubjects: []string{"one"},
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverAll,
		AckWait:        time.Second * 30,
	}, ActionUpdate, false)
	require_Error(t, err)

	// Create new ephemeral. Should be fine as the consumer doesn't exist already
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Name:           "EPH",
		FilterSubjects: []string{"one"},
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverAll,
		AckWait:        time.Second * 30,
	}, ActionCreate, false)
	require_NoError(t, err)

	// Trying to create it again right away. Should error as it already exists (and hasn't been cleaned up yet)
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Name:           "EPH",
		FilterSubjects: []string{"one"},
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverAll,
		AckWait:        time.Second * 30,
	}, ActionCreate, false)
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
	}, ActionCreate, false)
	require_NoError(t, err)

	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "C2",
		FilterSubjects: []string{"three", "four"},
		AckPolicy:      AckExplicit,
	}, ActionCreate, false)
	require_NoError(t, err)

	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "C3",
		FilterSubjects: []string{"five.*"},
		AckPolicy:      AckExplicit,
	}, ActionCreate, false)
	require_NoError(t, err)

	// Updating a consumer by removing a previous subject filter.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "C1",
		FilterSubjects: []string{"one"}, // Remove a subject.
		AckPolicy:      AckExplicit,
	}, ActionUpdate, false)
	require_NoError(t, err)

	// Updating a consumer without overlapping subjects.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "C2",
		FilterSubjects: []string{"three", "four", "two"}, // Add previously removed subject.
		AckPolicy:      AckExplicit,
	}, ActionUpdate, false)
	require_NoError(t, err)

	// Creating a consumer with overlapping subjects should return an error.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "C4",
		FilterSubjects: []string{"one", "two", "three", "four"},
		AckPolicy:      AckExplicit,
	}, ActionCreate, false)
	require_Error(t, err)
	if !IsNatsErr(err, JSConsumerWQConsumerNotUniqueErr) {
		t.Errorf("want error %q, got %q", ApiErrors[JSConsumerWQConsumerNotUniqueErr], err)
	}

	// Updating a consumer with overlapping subjects should return an error.
	_, err = mset.addConsumerWithAction(&ConsumerConfig{
		Durable:        "C3",
		FilterSubjects: []string{"one", "two", "three", "four"},
		AckPolicy:      AckExplicit,
	}, ActionUpdate, false)
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

func TestJetStreamConsumerPedanticMode(t *testing.T) {

	singleServerTemplate := `
			listen: 127.0.0.1:-1
			jetstream: {
				max_mem_store: 2MB,
				max_file_store: 8MB,
				store_dir: '%s',
				limits: {max_request_batch: 250}
			}
			no_auth_user: u
			accounts {
				ONE {
					users = [ { user: "u", pass: "s3cr3t!" } ]
					jetstream: enabled
				}
				$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
			}`

	clusterTemplate := `
			listen: 127.0.0.1:-1
			server_name: %s
			jetstream: {
				max_mem_store: 2MB,
				max_file_store: 8MB,
				store_dir: '%s',
				limits: {max_request_batch: 250}
			}
			cluster {
				name: %s
				listen: 127.0.0.1:%d
				routes = [%s]
			}
			no_auth_user: u
			accounts {
				ONE {
					users = [ { user: "u", pass: "s3cr3t!" } ]
					jetstream: enabled
				}
				$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
			}`

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	tests := []struct {
		name                  string
		givenConfig           ConsumerConfig
		givenLimits           nats.StreamConsumerLimits
		serverTemplateSingle  string
		serverTemplateCluster string
		shouldError           bool
		pedantic              bool
		replicas              int
	}{
		{
			name: "default_non_pedantic",
			givenConfig: ConsumerConfig{
				Durable: "durable",
			},
			givenLimits: nats.StreamConsumerLimits{
				InactiveThreshold: time.Minute,
				MaxAckPending:     100,
			},
			shouldError: false,
			pedantic:    false,
		},
		{
			name: "default_pedantic_inactive_threshold",
			givenConfig: ConsumerConfig{
				Durable: "durable",
			},
			givenLimits: nats.StreamConsumerLimits{
				InactiveThreshold: time.Minute,
			},
			shouldError: true,
			pedantic:    true,
		},
		{
			name: "default_pedantic_max_ack_pending",
			givenConfig: ConsumerConfig{
				Durable: "durable",
			},
			givenLimits: nats.StreamConsumerLimits{
				MaxAckPending: 100,
			},
			shouldError: true,
			pedantic:    true,
		},
		{
			name: "pedantic_backoff_no_ack_wait",
			givenConfig: ConsumerConfig{
				Durable: "durable",
				BackOff: []time.Duration{time.Second, time.Minute},
			},
			pedantic:    true,
			shouldError: true,
		},
		{
			name: "backoff_no_ack_wait",
			givenConfig: ConsumerConfig{
				Durable: "durable",
				BackOff: []time.Duration{time.Second, time.Minute},
			},
			pedantic:    false,
			shouldError: false,
		},
		{
			name: "max_batch_requests",
			givenConfig: ConsumerConfig{
				Durable: "durable",
			},
			serverTemplateSingle:  singleServerTemplate,
			serverTemplateCluster: clusterTemplate,
			pedantic:              false,
			shouldError:           false,
		},
		{
			name: "pedantic_max_batch_requests",
			givenConfig: ConsumerConfig{
				Durable: "durable",
			},
			serverTemplateSingle:  singleServerTemplate,
			serverTemplateCluster: clusterTemplate,
			pedantic:              true,
			shouldError:           true,
		},
	}

	for _, test := range tests {
		for _, mode := range []string{"clustered", "single"} {
			t.Run(fmt.Sprintf("%v_%v", mode, test.name), func(t *testing.T) {

				var s *Server
				if mode == "single" {
					s = RunBasicJetStreamServer(t)
					defer s.Shutdown()
				} else {
					c := createJetStreamClusterExplicit(t, "R3S", 3)
					defer c.shutdown()
					s = c.randomServer()
				}

				replicas := 1
				if mode == "clustered" {
					replicas = 3
				}

				nc, js := jsClientConnect(t, s)
				defer nc.Close()

				js.AddStream(&nats.StreamConfig{
					Name:     test.name,
					Subjects: []string{"foo"},
					Replicas: replicas,
					ConsumerLimits: nats.StreamConsumerLimits{
						InactiveThreshold: time.Minute,
						MaxAckPending:     100,
					},
				})

				_, err := addConsumerWithError(t, nc, &CreateConsumerRequest{
					Stream:   test.name,
					Config:   test.givenConfig,
					Action:   ActionCreateOrUpdate,
					Pedantic: test.pedantic,
				})
				require_True(t, (err != nil) == test.shouldError)
				if err != nil {
					require_True(t, strings.Contains(err.Error(), "pedantic"))
				}
			})
		}
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

func TestJetStreamConsumerPinned(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.>", "bar", "baz"},
		Retention: LimitsPolicy,
	})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "C",
		FilterSubject:  "foo.>",
		PriorityGroups: []string{"A"},
		PriorityPolicy: PriorityPinnedClient,
		AckPolicy:      AckExplicit,
		PinnedTTL:      10 * time.Second,
	})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		msg := nats.NewMsg(fmt.Sprintf("foo.%d", i))
		msg.Data = []byte(fmt.Sprintf("msg-%d", i))
		// Add headers to check if we properly serialize Nats-Pin-Id with and without headers.
		if i%2 == 0 {
			msg.Header.Add("Some-Header", "Value")
		}
		js.PublishMsg(msg)
	}

	req := JSApiConsumerGetNextRequest{Batch: 3, Expires: 5 * time.Second, PriorityGroup: PriorityGroup{
		Group: "A",
	}}
	reqb, _ := json.Marshal(req)
	reply := "ONE"
	replies, err := nc.SubscribeSync(reply)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", reply, reqb)
	require_NoError(t, err)

	reply2 := "TWO"
	replies2, err := nc.SubscribeSync(reply2)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", reply2, reqb)
	require_NoError(t, err)

	// This is the first Pull Request, so it should become the pinned one.
	msg, err := replies.NextMsg(time.Second)
	require_NoError(t, err)
	require_NotNil(t, msg)
	// Check if we are really pinned.
	pinned := msg.Header.Get("Nats-Pin-Id")
	if pinned == "" {
		t.Fatalf("Expected pinned message, got none")
	}

	// Here, we should have pull request that just idles, as it is not pinned.
	_, err = replies2.NextMsg(time.Second)
	require_Error(t, err)

	// While the pinned one continues to get messages.
	msg, err = replies.NextMsg(time.Second)
	require_NoError(t, err)
	require_NotNil(t, msg)

	// Just making sure that the other one does not get round-robined message.
	_, err = replies2.NextMsg(time.Second)
	require_Error(t, err)

	// Now let's send a request with wrong pinned id.
	req = JSApiConsumerGetNextRequest{Batch: 3, Expires: 250 * time.Millisecond, PriorityGroup: PriorityGroup{
		Id:    "WRONG",
		Group: "A",
	}}
	reqBad, err := json.Marshal(req)
	require_NoError(t, err)
	replies3, err := nc.SubscribeSync(reply)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", reply, reqBad)
	require_NoError(t, err)

	// and make sure we got error telling us it's wrong ID.
	msg, err = replies3.NextMsg(time.Second)
	require_NoError(t, err)
	if msg.Header.Get("Status") != "423" {
		t.Fatalf("Expected 423, got %v", msg.Header.Get("Status"))
	}
	// Send a new request with a good pinned ID.
	req = JSApiConsumerGetNextRequest{Batch: 3, Expires: 250 * time.Millisecond, PriorityGroup: PriorityGroup{
		Id:    pinned,
		Group: "A",
	}}
	reqb, _ = json.Marshal(req)
	reply = "FOUR"
	replies4, err := nc.SubscribeSync(reply)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", reply, reqb)
	require_NoError(t, err)

	// and check that we got a message.
	msg, err = replies4.NextMsg(time.Second)
	require_NoError(t, err)
	require_NotNil(t, msg)

	advisories, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.CONSUMER.*.TEST.C")
	require_NoError(t, err)

	// Send a new request without pin ID, which should work after the TTL.
	req = JSApiConsumerGetNextRequest{Batch: 3, Expires: 50 * time.Second, PriorityGroup: PriorityGroup{
		Group: "A",
	}}
	reqb, _ = json.Marshal(req)
	reply = "FIVE"
	replies5, err := nc.SubscribeSync(reply)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", reply, reqb)

	checkFor(t, 20*time.Second, 1*time.Second, func() error {
		_, err = replies5.NextMsg(500 * time.Millisecond)
		if err == nil {
			return nil
		}
		return err
	})

	advisory, err := advisories.NextMsg(time.Second)
	require_NoError(t, err)
	require_Equal(t, fmt.Sprintf("%s.TEST.C", JSAdvisoryConsumerUnpinnedPre), advisory.Subject)
	advisory, err = advisories.NextMsg(time.Second)
	require_NoError(t, err)
	require_Equal(t, fmt.Sprintf("%s.TEST.C", JSAdvisoryConsumerPinnedPre), advisory.Subject)

	// Manually unpin the current fetch request.
	request := JSApiConsumerUnpinRequest{Group: "A"}
	requestData, err := json.Marshal(request)
	require_NoError(t, err)
	msg, err = nc.Request("$JS.API.CONSUMER.UNPIN.TEST.C", requestData, time.Second*1)
	require_NoError(t, err)

	var response JSApiConsumerUnpinResponse
	err = json.Unmarshal(msg.Data, &response)
	require_NoError(t, err)
	require_True(t, response.Error == nil)

	// check if we got proper advisories.
	advisory, err = advisories.NextMsg(time.Second)
	require_NoError(t, err)
	require_Equal(t, fmt.Sprintf("%s.TEST.C", JSAdvisoryConsumerUnpinnedPre), advisory.Subject)

	replies6, err := nc.SubscribeSync(reply)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", reply, reqBad)
	require_NoError(t, err)

	_, err = replies6.NextMsg(time.Second * 5)
	require_NoError(t, err)
}

func TestJetStreamConsumerPinnedUnsubscribeOnPinned(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.>", "bar", "baz"},
		Retention: LimitsPolicy,
	})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "C",
		FilterSubject:  "foo.>",
		PriorityGroups: []string{"A"},
		PriorityPolicy: PriorityPinnedClient,
		AckPolicy:      AckExplicit,
		PinnedTTL:      time.Second,
	})
	require_NoError(t, err)
	_, err = js.Publish("foo.1", []byte("hello"))
	require_NoError(t, err)

	req := JSApiConsumerGetNextRequest{Batch: 3, Expires: 5 * time.Second, Heartbeat: 250 * time.Millisecond, PriorityGroup: PriorityGroup{
		Group: "A",
	}}
	reqb, _ := json.Marshal(req)
	reply := "ONE"
	replies, err := nc.SubscribeSync(reply)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", reply, reqb)
	require_NoError(t, err)

	reply2 := "TWO"
	replies2, err := nc.SubscribeSync(reply2)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", reply2, reqb)
	require_NoError(t, err)
	defer replies2.Unsubscribe()

	// This is the first Pull Request, so it should become the pinned one.
	msg, err := replies.NextMsg(time.Second)
	require_NoError(t, err)
	require_NotNil(t, msg)
	// Check if we are really pinned.
	pinned := msg.Header.Get("Nats-Pin-Id")
	if pinned == "" {
		t.Fatalf("Expected pinned message, got none")
	}

	err = replies.Unsubscribe()
	require_NoError(t, err)
	_, err = js.Publish("foo.1", []byte("hello"))
	require_NoError(t, err)

	// Here, we should receive a heartbeat message.
	msg, err = replies2.NextMsg(time.Second)
	require_NoError(t, err)
	require_NotNil(t, msg)
	require_Equal(t, msg.Header.Get("Status"), "100")

	// receive heartbeats until pinned ttl expires
	// and we should get a new message with new pin id
	for {
		msg, err = replies2.NextMsg(time.Second)
		require_NoError(t, err)
		require_NotNil(t, msg)
		if msg.Header.Get("Status") == "100" {
			continue
		}
		if msg.Header.Get("Nats-Pin-Id") == "" {
			t.Fatalf("Expected pinned message, got none")
		}
		break
	}
}

// This tests if Unpin works correctly when there are no pending messages.
// It checks if the next pinned client will be different than the first one
// after new messages is published.
func TestJetStreamConsumerUnpinNoMessages(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: LimitsPolicy,
	})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "C",
		FilterSubject:  "foo",
		PriorityGroups: []string{"A"},
		PriorityPolicy: PriorityPinnedClient,
		AckPolicy:      AckExplicit,
		PinnedTTL:      30 * time.Second,
	})
	require_NoError(t, err)

	req := JSApiConsumerGetNextRequest{Batch: 30, Expires: 60 * time.Second, PriorityGroup: PriorityGroup{
		Group: "A",
	}}
	reqb, _ := json.Marshal(req)
	reply := "ONE"
	replies, err := nc.SubscribeSync(reply)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", reply, reqb)
	require_NoError(t, err)

	reply2 := "TWO"
	replies2, err := nc.SubscribeSync(reply2)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", reply2, reqb)
	require_NoError(t, err)

	sendStreamMsg(t, nc, "foo", "data")
	sendStreamMsg(t, nc, "foo", "data")

	msg, err := replies.NextMsg(1 * time.Second)
	pinId := msg.Header.Get("Nats-Pin-Id")
	require_NotEqual(t, pinId, "")
	require_NoError(t, err)
	_, err = replies.NextMsg(1 * time.Second)
	require_NoError(t, err)

	_, err = replies2.NextMsg(1 * time.Second)
	require_Error(t, err)

	unpinRequest := func(t *testing.T, nc *nats.Conn, stream, consumer, group string) *ApiError {
		var response JSApiConsumerUnpinResponse
		request := JSApiConsumerUnpinRequest{Group: group}
		requestData, err := json.Marshal(request)
		require_NoError(t, err)
		msg, err := nc.Request(fmt.Sprintf("$JS.API.CONSUMER.UNPIN.%s.%s", stream, consumer), requestData, time.Second*1)
		require_NoError(t, err)
		err = json.Unmarshal(msg.Data, &response)
		require_NoError(t, err)
		return response.Error
	}

	unpinError := unpinRequest(t, nc, "TEST", "C", "A")
	require_True(t, unpinError == nil)

	sendStreamMsg(t, nc, "foo", "data")
	sendStreamMsg(t, nc, "foo", "data")

	// Old pinned client should get info that it is no longer pinned.
	msg, err = replies.NextMsg(1 * time.Second)
	require_NoError(t, err)
	require_Equal(t, msg.Header.Get("Status"), "423")

	// While the new one should get the message and new pin.
	msg, err = replies2.NextMsg(1 * time.Second)
	require_NoError(t, err)
	require_Equal(t, string(msg.Data), "data")
	require_NotEqual(t, msg.Header.Get("Nats-Pin-Id"), pinId)
}

// In some scenarios, if the next waiting request is the same as the old pinned, it could be picked as a new pin.
// This test replicates that behavior and checks if the new pin is different than the old one.
func TestJetStreamConsumerUnpinPickDifferentRequest(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: LimitsPolicy,
	})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "C",
		FilterSubject:  "foo",
		PriorityGroups: []string{"A"},
		PriorityPolicy: PriorityPinnedClient,
		AckPolicy:      AckExplicit,
		PinnedTTL:      30 * time.Second,
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "foo", "data")

	req := JSApiConsumerGetNextRequest{Batch: 5, Expires: 15 * time.Second, PriorityGroup: PriorityGroup{
		Group: "A",
	}}

	reqBytes, err := json.Marshal(req)
	require_NoError(t, err)

	firstInbox := "FIRST"
	firstReplies, err := nc.SubscribeSync(firstInbox)
	require_NoError(t, err)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", firstInbox, reqBytes)

	msg, err := firstReplies.NextMsg(1 * time.Second)
	require_NoError(t, err)
	pinId := msg.Header.Get("Nats-Pin-Id")
	require_NotEqual(t, pinId, "")

	reqPinned := JSApiConsumerGetNextRequest{Batch: 5, Expires: 15 * time.Second, PriorityGroup: PriorityGroup{
		Group: "A",
		Id:    pinId,
	}}
	_, err = json.Marshal(reqPinned)
	require_NoError(t, err)

	secondInbox := "SECOND"
	secondReplies, err := nc.SubscribeSync(secondInbox)
	require_NoError(t, err)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", secondInbox, reqBytes)

	_, err = secondReplies.NextMsg(1 * time.Second)
	require_Error(t, err)

	unpinRequest := func(t *testing.T, nc *nats.Conn, stream, consumer, group string) *ApiError {
		var response JSApiConsumerUnpinResponse
		request := JSApiConsumerUnpinRequest{Group: group}
		requestData, err := json.Marshal(request)
		require_NoError(t, err)
		msg, err := nc.Request(fmt.Sprintf("$JS.API.CONSUMER.UNPIN.%s.%s", stream, consumer), requestData, time.Second*1)
		require_NoError(t, err)
		err = json.Unmarshal(msg.Data, &response)
		require_NoError(t, err)
		return response.Error
	}

	unpinRequest(t, nc, "TEST", "C", "A")
	_, err = firstReplies.NextMsg(1 * time.Second)
	// If there are no messages in the stream, do not expect unpin message to arrive.
	// Advisory will be sent immediately, but messages with headers - only when there is anything to be sent.
	require_Error(t, err)
	// Send a new message to the stream.
	sendStreamMsg(t, nc, "foo", "data")
	// Check if the old pinned will get the information about bad pin.
	msg, err = firstReplies.NextMsg(1 * time.Second)
	require_NoError(t, err)
	require_Equal(t, msg.Header.Get("Status"), "423")
	// Make sure that the old pin is cleared.
	require_Equal(t, msg.Header.Get("Nats-Pin-Id"), "")

	// Try different wr.
	msg, err = secondReplies.NextMsg(1 * time.Second)
	require_NoError(t, err)
	// Make sure that its pin is different than the old one and not empty.
	require_NotEqual(t, msg.Header.Get("Nats-Pin-Id"), pinId)
	require_NotEqual(t, msg.Header.Get("Nats-Pin-Id"), "")
}

func TestJetStreamConsumerPinnedTTL(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: LimitsPolicy,
	})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "C",
		FilterSubject:  "foo",
		PriorityGroups: []string{"A"},
		PriorityPolicy: PriorityPinnedClient,
		AckPolicy:      AckExplicit,
		PinnedTTL:      3 * time.Second,
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "foo", "data")
	}

	req := JSApiConsumerGetNextRequest{Batch: 1, Expires: 10 * time.Second, PriorityGroup: PriorityGroup{
		Group: "A",
	}}

	reqBytes, err := json.Marshal(req)
	require_NoError(t, err)

	firstInbox := "FIRST"
	firstReplies, err := nc.SubscribeSync(firstInbox)
	require_NoError(t, err)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", firstInbox, reqBytes)

	msg, err := firstReplies.NextMsg(1 * time.Second)
	require_NoError(t, err)
	pinId := msg.Header.Get("Nats-Pin-Id")
	require_NotEqual(t, pinId, "")

	secondInbox := "SECOND"
	secondReplies, err := nc.SubscribeSync(secondInbox)
	require_NoError(t, err)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", secondInbox, reqBytes)

	// Expect error, as first request should be still pinned.
	_, err = secondReplies.NextMsg(1 * time.Second)
	require_Error(t, err)

	// During the 5 second window, the first Pin should time out and this request
	// should become the pinned one and get the message.
	msg, err = secondReplies.NextMsg(5 * time.Second)
	require_NoError(t, err)
	newPinId := msg.Header.Get("Nats-Pin-Id")
	require_NotEqual(t, newPinId, pinId)
	require_NotEqual(t, newPinId, "")

	thirdInbox := "THIRD"
	thirdReplies, err := nc.SubscribeSync(thirdInbox)
	require_NoError(t, err)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", thirdInbox, reqBytes)

	// The same process as above, but tests different codepath - one where Pin
	// is set on existing waiting request.
	msg, err = thirdReplies.NextMsg(5 * time.Second)
	require_NoError(t, err)
	require_NotEqual(t, msg.Header.Get("Nats-Pin-Id"), pinId)
	require_NotEqual(t, msg.Header.Get("Nats-Pin-Id"), newPinId)
	require_NotEqual(t, newPinId, "")

}

func TestJetStreamConsumerUnpin(t *testing.T) {
	single := RunBasicJetStreamServer(t)
	defer single.Shutdown()
	nc, js := jsClientConnect(t, single)
	defer nc.Close()

	cluster := createJetStreamClusterExplicit(t, "R3S", 3)
	defer cluster.shutdown()
	cnc, cjs := jsClientConnect(t, cluster.randomServer())
	defer cnc.Close()

	// Create a stream and consumer for both single server and clustered mode.
	for _, server := range []struct {
		replicas int
		js       nats.JetStreamContext
		nc       *nats.Conn
	}{
		{1, js, nc},
		{3, cjs, cnc},
	} {

		_, err := server.js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo.>", "bar", "baz"},
			Replicas: server.replicas,
		})
		require_NoError(t, err)

		consumerConfig := CreateConsumerRequest{
			Stream: "TEST",
			Action: ActionCreate,
			Config: ConsumerConfig{
				Durable:        "C",
				FilterSubject:  "foo.>",
				PriorityGroups: []string{"A"},
				PriorityPolicy: PriorityPinnedClient,
				AckPolicy:      AckExplicit,
				PinnedTTL:      10 * time.Second,
			},
		}
		req, err := json.Marshal(consumerConfig)
		require_NoError(t, err)
		rmsg, err := server.nc.Request(fmt.Sprintf(JSApiDurableCreateT, consumerConfig.Stream, consumerConfig.Config.Durable), req, 5*time.Second)
		require_NoError(t, err)

		var resp JSApiConsumerCreateResponse
		err = json.Unmarshal(rmsg.Data, &resp)
		require_NoError(t, err)
		require_True(t, resp.Error == nil)

	}
	cluster.waitOnStreamLeader("$G", "TEST")
	cluster.waitOnConsumerLeader("$G", "TEST", "C")

	unpinRequest := func(t *testing.T, nc *nats.Conn, stream, consumer, group string) *ApiError {
		var response JSApiConsumerUnpinResponse
		request := JSApiConsumerUnpinRequest{Group: group}
		requestData, err := json.Marshal(request)
		require_NoError(t, err)
		msg, err := nc.Request(fmt.Sprintf("$JS.API.CONSUMER.UNPIN.%s.%s", stream, consumer), requestData, time.Second*1)
		require_NoError(t, err)
		err = json.Unmarshal(msg.Data, &response)
		require_NoError(t, err)
		return response.Error
	}

	for _, test := range []struct {
		name     string
		nc       *nats.Conn
		stream   string
		consumer string
		group    string
		err      *ApiError
	}{
		{"unpin non-existing group", nc, "TEST", "C", "B", &ApiError{ErrCode: uint16(JSConsumerInvalidPriorityGroupErr)}},
		{"unpin on missing stream", nc, "NOT_EXIST", "C", "A", &ApiError{ErrCode: uint16(JSStreamNotFoundErr)}},
		{"unpin on missing consumer", nc, "TEST", "NOT_EXIST", "A", &ApiError{ErrCode: uint16(JSConsumerNotFoundErr)}},
		{"unpin missing group", nc, "TEST", "C", "", &ApiError{ErrCode: uint16(JSInvalidJSONErr)}},
		{"unpin bad group name", nc, "TEST", "C", "group    name\r\n", &ApiError{ErrCode: uint16(JSConsumerInvalidGroupNameErr)}},
		{"ok unpin", nc, "TEST", "C", "A", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := unpinRequest(t, nc, test.stream, test.consumer, test.group)
			if test.err != nil {
				require_True(t, err.ErrCode == test.err.ErrCode)
			} else {
				require_True(t, err == nil)
			}
		})
		t.Run(fmt.Sprintf("%s clustered", test.name), func(t *testing.T) {
			err := unpinRequest(t, cnc, test.stream, test.consumer, test.group)
			if test.err != nil {
				require_True(t, err.ErrCode == test.err.ErrCode)
			} else {
				require_True(t, err == nil)
			}
		})
	}
}

func TestJetStreamConsumerWithPriorityGroups(t *testing.T) {
	single := RunBasicJetStreamServer(t)
	defer single.Shutdown()
	nc, js := jsClientConnect(t, single)
	defer nc.Close()

	cluster := createJetStreamClusterExplicit(t, "R3S", 3)
	defer cluster.shutdown()
	cnc, cjs := jsClientConnect(t, cluster.randomServer())
	defer cnc.Close()

	// Create a stream and consumer for both single server and clustered mode.
	for _, server := range []struct {
		replicas int
		js       nats.JetStreamContext
	}{
		{1, js},
		{3, cjs},
	} {

		_, err := server.js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo.>", "bar", "baz"},
			Replicas: server.replicas,
		})
		require_NoError(t, err)
	}
	cluster.waitOnStreamLeader("$G", "TEST")

	for _, test := range []struct {
		name           string
		nc             *nats.Conn
		stream         string
		consumer       string
		groups         []string
		mode           PriorityPolicy
		deliverSubject string
		err            *ApiError
	}{
		{"Pinned Consumer with Priority Group", nc, "TEST", "PINNED", []string{"A"}, PriorityPinnedClient, "", nil},
		{"Pinned Consumer with Priority Group, clustered", cnc, "TEST", "PINNED", []string{"A"}, PriorityPinnedClient, "", nil},
		{"Overflow Consumer with Priority Group", nc, "TEST", "OVERFLOW", []string{"A"}, PriorityOverflow, "", nil},
		{"Overflow Consumer with Priority Group, clustered", cnc, "TEST", "OVERFLOW", []string{"A"}, PriorityOverflow, "", nil},
		{"Pinned Consumer without Priority Group", nc, "TEST", "PINNED_NO_GROUP", nil, PriorityPinnedClient, "", &ApiError{ErrCode: uint16(JSConsumerPriorityPolicyWithoutGroup)}},
		{"Pinned Consumer without Priority Group, clustered", cnc, "TEST", "PINNED_NO_GROUP", nil, PriorityPinnedClient, "", &ApiError{ErrCode: uint16(JSConsumerPriorityPolicyWithoutGroup)}},
		{"Overflow Consumer without Priority Group", nc, "TEST", "PINNED_NO_GROUP", nil, PriorityOverflow, "", &ApiError{ErrCode: uint16(JSConsumerPriorityPolicyWithoutGroup)}},
		{"Overflow Consumer without Priority Group, clustered", cnc, "TEST", "PINNED_NO_GROUP", nil, PriorityOverflow, "", &ApiError{ErrCode: uint16(JSConsumerPriorityPolicyWithoutGroup)}},
		{"Pinned Consumer with empty Priority Group", nc, "TEST", "PINNED_NO_GROUP", []string{""}, PriorityPinnedClient, "", &ApiError{ErrCode: uint16(JSConsumerEmptyGroupName)}},
		{"Pinned Consumer with empty Priority Group, clustered", cnc, "TEST", "PINNED_NO_GROUP", []string{""}, PriorityPinnedClient, "", &ApiError{ErrCode: uint16(JSConsumerEmptyGroupName)}},
		{"Pinned Consumer with empty Priority Group", nc, "TEST", "PINNED_NO_GROUP", []string{""}, PriorityOverflow, "", &ApiError{ErrCode: uint16(JSConsumerEmptyGroupName)}},
		{"Pinned Consumer with empty Priority Group, clustered", cnc, "TEST", "PINNED_NO_GROUP", []string{""}, PriorityOverflow, "", &ApiError{ErrCode: uint16(JSConsumerEmptyGroupName)}},
		{"Consumer with `none` policy priority and no pinned TTL set", nc, "TEST", "NONE", []string{}, PriorityNone, "", &ApiError{ErrCode: uint16(JSConsumerPinnedTTLWithoutPriorityPolicyNone)}},
		{"Consumer with `none` policy priority and Priority Group set", nc, "TEST", "NONE_WITH_GROUPS", []string{"A"}, PriorityNone, "", &ApiError{ErrCode: uint16(JSConsumerPriorityGroupWithPolicyNone)}},
		{"Push consumer with Priority Group", nc, "TEST", "PUSH_WITH_POLICY", []string{"A"}, PriorityOverflow, "subject", &ApiError{ErrCode: uint16(JSConsumerPushWithPriorityGroupErr)}},
	} {
		t.Run(test.name, func(t *testing.T) {

			consumerConfig := CreateConsumerRequest{
				Stream: "TEST",
				Action: ActionCreate,
				Config: ConsumerConfig{
					Durable:        test.consumer,
					FilterSubject:  "foo.>",
					PriorityGroups: test.groups,
					PriorityPolicy: test.mode,
					AckPolicy:      AckExplicit,
					DeliverSubject: test.deliverSubject,
					PinnedTTL:      10 * time.Second,
				},
			}
			req, err := json.Marshal(consumerConfig)
			require_NoError(t, err)
			rmsg, err := test.nc.Request(fmt.Sprintf(JSApiDurableCreateT, consumerConfig.Stream, consumerConfig.Config.Durable), req, 5*time.Second)
			require_NoError(t, err)

			var resp JSApiConsumerCreateResponse
			err = json.Unmarshal(rmsg.Data, &resp)
			require_NoError(t, err)

			if test.err != nil {
				require_True(t, resp.Error.ErrCode == test.err.ErrCode)
			} else {
				require_True(t, resp.Error == nil)
			}
		})
	}
}

func TestJetStreamConsumerPriorityPullRequests(t *testing.T) {
	single := RunBasicJetStreamServer(t)
	defer single.Shutdown()
	nc, js := jsClientConnect(t, single)
	defer nc.Close()

	cluster := createJetStreamClusterExplicit(t, "R3S", 3)
	defer cluster.shutdown()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"pinned.>", "overflow.>"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable: "STANDARD",
	})
	require_NoError(t, err)

	consumerConfig := CreateConsumerRequest{
		Stream: "TEST",
		Action: ActionCreate,
		Config: ConsumerConfig{
			Durable:        "PINNED",
			FilterSubject:  "pinned.>",
			PriorityGroups: []string{"A"},
			PriorityPolicy: PriorityPinnedClient,
			AckPolicy:      AckExplicit,
			PinnedTTL:      10 * time.Second,
		},
	}
	req, err := json.Marshal(consumerConfig)
	require_NoError(t, err)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, consumerConfig.Stream, consumerConfig.Config.Durable), req, 5*time.Second)
	require_NoError(t, err)

	var resp JSApiConsumerCreateResponse
	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	require_True(t, resp.Error == nil)

	consumerConfig = CreateConsumerRequest{
		Stream: "TEST",
		Action: ActionCreate,
		Config: ConsumerConfig{
			Durable:        "OVERFLOW",
			FilterSubject:  "overflow.>",
			PriorityGroups: []string{"A"},
			PriorityPolicy: PriorityOverflow,
			AckPolicy:      AckExplicit,
			PinnedTTL:      5 * time.Second,
		},
	}
	req, err = json.Marshal(consumerConfig)
	require_NoError(t, err)
	rmsg, err = nc.Request(fmt.Sprintf(JSApiDurableCreateT, consumerConfig.Stream, consumerConfig.Config.Durable), req, 5*time.Second)
	require_NoError(t, err)

	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	require_True(t, resp.Error == nil)

	for i := 0; i < 50; i++ {
		sendStreamMsg(t, nc, "pinned.1", fmt.Sprintf("msg-%d", i))
		sendStreamMsg(t, nc, "overflow.1", fmt.Sprintf("msg-%d", i))
	}

	for _, test := range []struct {
		name        string
		nc          *nats.Conn
		consumer    string
		request     JSApiConsumerGetNextRequest
		description string
	}{
		{"Pinned Pull Request", nc, "PINNED", JSApiConsumerGetNextRequest{Batch: 1, Expires: 5 * time.Second, PriorityGroup: PriorityGroup{Group: "A"}}, ""},
		{"Pinned Pull Request, no group", nc, "PINNED", JSApiConsumerGetNextRequest{Batch: 1, Expires: 5 * time.Second, PriorityGroup: PriorityGroup{}}, "Bad Request - Priority Group missing"},
		{"Pinned Pull Request, bad group", nc, "PINNED", JSApiConsumerGetNextRequest{Batch: 1, Expires: 5 * time.Second, PriorityGroup: PriorityGroup{Group: "Bad"}}, "Bad Request - Invalid Priority Group"},
		{"Pinned Pull Request, against Overflow", nc, "OVERFLOW", JSApiConsumerGetNextRequest{Batch: 1, Expires: 5 * time.Second, PriorityGroup: PriorityGroup{Group: "A", Id: "PINNED-ID"}}, "Bad Request - Not a Pinned Client Priority consumer"},
		{"Pinned Pull Request, against standard consumer", nc, "STANDARD", JSApiConsumerGetNextRequest{Batch: 1, Expires: 5 * time.Second, PriorityGroup: PriorityGroup{Group: "A", Id: "PINNED-ID"}}, "Bad Request - Not a Pinned Client Priority consumer"},
		{"Overflow Pull Request, overflow below threshold", nc, "OVERFLOW", JSApiConsumerGetNextRequest{Batch: 1, Expires: 5 * time.Second, PriorityGroup: PriorityGroup{Group: "A", MinPending: 1000}}, "Request Timeout"},
		{"Overflow Pull Request, overflow above threshold", nc, "OVERFLOW", JSApiConsumerGetNextRequest{Batch: 1, Expires: 5 * time.Second, PriorityGroup: PriorityGroup{Group: "A", MinPending: 10}}, ""},
		{"Overflow Pull Request, against pinned", nc, "PINNED", JSApiConsumerGetNextRequest{Batch: 1, Expires: 5 * time.Second, PriorityGroup: PriorityGroup{Group: "A", MinPending: 10}}, "Bad Request - Not a Overflow Priority consumer"},
		{"Overflow Pull Request, against standard consumer", nc, "STANDARD", JSApiConsumerGetNextRequest{Batch: 1, Expires: 5 * time.Second, PriorityGroup: PriorityGroup{Group: "A", MinPending: 10}}, "Bad Request - Not a Overflow Priority consumer"},
	} {
		t.Run(test.name, func(t *testing.T) {
			inbox := nats.NewInbox()
			replies, err := test.nc.SubscribeSync(inbox)
			reqb, _ := json.Marshal(test.request)
			require_NoError(t, err)
			nc.PublishRequest(fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.TEST.%s", test.consumer), inbox, reqb)
			require_NoError(t, err)
			msg, err := replies.NextMsg(10 * time.Second)
			require_NoError(t, err)
			require_Equal(t, test.description, msg.Header.Get("Description"))
		})
	}
}

func TestJetStreamConsumerOverflow(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.>", "bar", "baz"},
		Retention: LimitsPolicy,
		Storage:   FileStorage,
	})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "C",
		FilterSubject:  "foo.>",
		PriorityGroups: []string{"A"},
		PriorityPolicy: PriorityOverflow,
		AckPolicy:      AckExplicit,
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "foo.1", "msg-1")

	// nothing unacked, so should return nothing.
	req := JSApiConsumerGetNextRequest{Batch: 1, Expires: 90 * time.Second, PriorityGroup: PriorityGroup{
		MinAckPending: 1,
		Group:         "A",
	}}
	ackPending1 := sendRequest(t, nc, "ackPending", req)
	_, err = ackPending1.NextMsg(time.Second)
	require_Error(t, err)

	// one pending message, so should return it.
	req = JSApiConsumerGetNextRequest{Batch: 1, Expires: 90 * time.Second, PriorityGroup: PriorityGroup{
		MinPending: 1,
		Group:      "A",
	}}
	numPending1 := sendRequest(t, nc, "singleOverflow", req)
	msg, err := numPending1.NextMsg(time.Second)
	require_NoError(t, err)
	require_NotNil(t, msg)

	sendStreamMsg(t, nc, "foo.1", "msg-2")
	sendStreamMsg(t, nc, "foo.1", "msg-3")

	// overflow set to 10, so we should not get any messages, as there are only few pending.
	req = JSApiConsumerGetNextRequest{Batch: 1, Expires: 90 * time.Second, PriorityGroup: PriorityGroup{
		MinPending: 10,
		Group:      "A",
	}}
	numPending10 := sendRequest(t, nc, "overflow", req)
	_, err = numPending10.NextMsg(time.Second)
	require_Error(t, err)

	// without overflow, we should get messages.
	req = JSApiConsumerGetNextRequest{Batch: 1, Expires: 90 * time.Second}
	fetchNoOverflow := sendRequest(t, nc, "without_overflow", req)
	noOverflowMsg, err := fetchNoOverflow.NextMsg(time.Second)
	require_NoError(t, err)
	require_NotNil(t, noOverflowMsg)

	// Now add more messages.
	for i := 0; i < 100; i++ {
		sendStreamMsg(t, nc, "foo.1", "msg-1")
	}

	// and previous batch should receive messages now.
	msg, err = numPending10.NextMsg(time.Second * 5)
	require_NoError(t, err)
	require_NotNil(t, msg)

	// But one with max ack pending should get nothing.
	req = JSApiConsumerGetNextRequest{Batch: 1, Expires: 90 * time.Second, PriorityGroup: PriorityGroup{
		MinAckPending: 50,
		Group:         "A",
	}}
	maxAckPending50 := sendRequest(t, nc, "maxAckPending", req)
	_, err = maxAckPending50.NextMsg(time.Second)
	require_Error(t, err)

	// However, when we miss a lot of acks, we should get messages on overflow with max ack pending.
	req = JSApiConsumerGetNextRequest{Batch: 200, Expires: 90 * time.Second, PriorityGroup: PriorityGroup{
		Group: "A",
	}}
	fetchNoOverflow = sendRequest(t, nc, "without_overflow", req)
	noOverflowMsg, err = fetchNoOverflow.NextMsg(time.Second)
	require_NoError(t, err)
	require_NotNil(t, noOverflowMsg)

	msg, err = maxAckPending50.NextMsg(time.Second)
	require_NoError(t, err)
	require_NotNil(t, msg)
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

func TestPriorityGroupNameRegex(t *testing.T) {
	for _, test := range []struct {
		name  string
		group string
		valid bool
	}{
		{"valid-short", "A", true},
		{"valid-with-accepted-special-chars", "group/consumer=A", true},
		{"empty", "", false},
		{"with-space", "A B", false},
		{"with-tab", "A   B", false},
		{"too-long-name", "group-name-that-is-too-long", false},
		{"line-termination", "\r\n", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			require_Equal(t, test.valid, validGroupName.MatchString(test.group))
		})
	}
}

func sendRequest(t *testing.T, nc *nats.Conn, reply string, req JSApiConsumerGetNextRequest) *nats.Subscription {
	reqb, _ := json.Marshal(req)
	replies, err := nc.SubscribeSync(reply)
	nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.C", reply, reqb)
	require_NoError(t, err)
	return replies
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

func TestJetStreamConsumerDeliveryCount(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	for i := 0; i < 2; i++ {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	sub, err := js.PullSubscribe(
		"foo",
		"CONSUMER",
		nats.ManualAck(),
		nats.AckExplicit(),
		nats.AckWait(time.Second),
		nats.MaxDeliver(1),
	)
	require_NoError(t, err)

	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)

	msgs, err := sub.Fetch(2)
	require_NoError(t, err)
	require_Len(t, len(msgs), 2)
	require_NoError(t, msgs[1].Nak())

	require_Equal(t, o.deliveryCount(1), 1)
	require_Equal(t, o.deliveryCount(2), 1)

	// max deliver 1 so this will fail
	_, err = sub.Fetch(1, nats.MaxWait(250*time.Millisecond))
	require_Error(t, err)

	// This would previously report delivery count 0, because o.rdc!=nil
	require_Equal(t, o.deliveryCount(1), 1)
	require_Equal(t, o.deliveryCount(2), 1)
}

func TestJetStreamConsumerCreate(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "foo", Storage: MemoryStorage, Subjects: []string{"foo", "bar"}, Retention: WorkQueuePolicy}},
		{"FileStore", &StreamConfig{Name: "foo", Storage: FileStorage, Subjects: []string{"foo", "bar"}, Retention: WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// Check for basic errors.
			if _, err := mset.addConsumer(nil); err == nil {
				t.Fatalf("Expected an error for no config")
			}

			// No deliver subject, meaning its in pull mode, work queue mode means it is required to
			// do explicit ack.
			if _, err := mset.addConsumer(&ConsumerConfig{}); err == nil {
				t.Fatalf("Expected an error on work queue / pull mode without explicit ack mode")
			}

			// Check for delivery subject errors.

			// Literal delivery subject required.
			if _, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: "foo.*"}); err == nil {
				t.Fatalf("Expected an error on bad delivery subject")
			}
			// Check for cycles
			if _, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: "foo"}); err == nil {
				t.Fatalf("Expected an error on delivery subject that forms a cycle")
			}
			if _, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: "bar"}); err == nil {
				t.Fatalf("Expected an error on delivery subject that forms a cycle")
			}
			if _, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: "*"}); err == nil {
				t.Fatalf("Expected an error on delivery subject that forms a cycle")
			}

			// StartPosition conflicts
			now := time.Now().UTC()
			if _, err := mset.addConsumer(&ConsumerConfig{
				DeliverSubject: "A",
				OptStartSeq:    1,
				OptStartTime:   &now,
			}); err == nil {
				t.Fatalf("Expected an error on start position conflicts")
			}
			if _, err := mset.addConsumer(&ConsumerConfig{
				DeliverSubject: "A",
				OptStartTime:   &now,
			}); err == nil {
				t.Fatalf("Expected an error on start position conflicts")
			}

			// Non-Durables need to have subscription to delivery subject.
			delivery := nats.NewInbox()
			nc := clientConnectToServer(t, s)
			defer nc.Close()
			sub, _ := nc.SubscribeSync(delivery)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery, AckPolicy: AckExplicit})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}

			if err := mset.deleteConsumer(o); err != nil {
				t.Fatalf("Expected no error on delete, got %v", err)
			}

			// Now let's check that durables can be created and a duplicate call to add will be ok.
			dcfg := &ConsumerConfig{
				Durable:        "ddd",
				DeliverSubject: delivery,
				AckPolicy:      AckExplicit,
			}
			if _, err = mset.addConsumer(dcfg); err != nil {
				t.Fatalf("Unexpected error creating consumer: %v", err)
			}
			if _, err = mset.addConsumer(dcfg); err != nil {
				t.Fatalf("Unexpected error creating second identical consumer: %v", err)
			}
			// Not test that we can change the delivery subject if that is only thing that has not
			// changed and we are not active.
			sub.Unsubscribe()
			sub, _ = nc.SubscribeSync("d.d.d")
			nc.Flush()
			defer sub.Unsubscribe()
			dcfg.DeliverSubject = "d.d.d"
			if _, err = mset.addConsumer(dcfg); err != nil {
				t.Fatalf("Unexpected error creating third consumer with just deliver subject changed: %v", err)
			}
		})
	}
}

func TestJetStreamConsumerAndStreamDescriptions(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	descr := "foo asset"
	acc := s.GlobalAccount()

	// Check stream's first.
	mset, err := acc.addStream(&StreamConfig{Name: "foo", Description: descr})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	if cfg := mset.config(); cfg.Description != descr {
		t.Fatalf("Expected a description of %q, got %q", descr, cfg.Description)
	}

	// Now consumer
	edescr := "analytics"
	o, err := mset.addConsumer(&ConsumerConfig{
		Description:    edescr,
		DeliverSubject: "to",
		AckPolicy:      AckNone})
	if err != nil {
		t.Fatalf("Unexpected error adding consumer: %v", err)
	}
	if cfg := o.config(); cfg.Description != edescr {
		t.Fatalf("Expected a description of %q, got %q", edescr, cfg.Description)
	}

	// Test max.
	data := make([]byte, JSMaxDescriptionLen+1)
	crand.Read(data)
	bigDescr := base64.StdEncoding.EncodeToString(data)

	_, err = acc.addStream(&StreamConfig{Name: "bar", Description: bigDescr})
	if err == nil || !strings.Contains(err.Error(), "description is too long") {
		t.Fatalf("Expected an error but got none")
	}

	_, err = mset.addConsumer(&ConsumerConfig{
		Description:    bigDescr,
		DeliverSubject: "to",
		AckPolicy:      AckNone})
	if err == nil || !strings.Contains(err.Error(), "description is too long") {
		t.Fatalf("Expected an error but got none")
	}
}
func TestJetStreamConsumerWithNameAndDurable(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	descr := "foo asset"
	name := "name"
	durable := "durable"
	acc := s.GlobalAccount()

	// Check stream's first.
	mset, err := acc.addStream(&StreamConfig{Name: "foo", Description: descr})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	if cfg := mset.config(); cfg.Description != descr {
		t.Fatalf("Expected a description of %q, got %q", descr, cfg.Description)
	}

	// it's ok to specify  both durable and name, but they have to be the same.
	_, err = mset.addConsumer(&ConsumerConfig{
		DeliverSubject: "to",
		Durable:        "consumer",
		Name:           "consumer",
		AckPolicy:      AckNone})
	if err != nil {
		t.Fatalf("Unexpected error adding consumer: %v", err)
	}

	// if they're not the same, expect error
	_, err = mset.addConsumer(&ConsumerConfig{
		DeliverSubject: "to",
		Durable:        durable,
		Name:           name,
		AckPolicy:      AckNone})

	if !strings.Contains(err.Error(), "Consumer Durable and Name have to be equal") {
		t.Fatalf("Wrong error while adding consumer with not matching Name and Durable: %v", err)
	}
}

func TestJetStreamConsumerWithStartTime(t *testing.T) {
	subj := "my_stream"
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: subj, Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: subj, Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			fsCfg := &FileStoreConfig{BlockSize: 100}
			mset, err := s.GlobalAccount().addStreamWithStore(c.mconfig, fsCfg)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 250
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subj, fmt.Sprintf("MSG: %d", i+1))
			}

			time.Sleep(10 * time.Millisecond)
			startTime := time.Now().UTC()

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subj, fmt.Sprintf("MSG: %d", i+1))
			}

			if msgs := mset.state().Msgs; msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, msgs)
			}

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:       "d",
				DeliverPolicy: DeliverByStartTime,
				OptStartTime:  &startTime,
				AckPolicy:     AckExplicit,
			})
			require_NoError(t, err)
			defer o.delete()

			msg, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
			require_NoError(t, err)
			sseq, dseq, _, _, _ := replyInfo(msg.Reply)
			if dseq != 1 {
				t.Fatalf("Expected delivered seq of 1, got %d", dseq)
			}
			if sseq != uint64(toSend+1) {
				t.Fatalf("Expected to get store seq of %d, got %d", toSend+1, sseq)
			}
		})
	}
}

// Test for https://github.com/nats-io/jetstream/issues/143
func TestJetStreamConsumerWithMultipleStartOptions(t *testing.T) {
	subj := "my_stream"
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: subj, Subjects: []string{"foo.>"}, Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: subj, Subjects: []string{"foo.>"}, Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			obsReq := CreateConsumerRequest{
				Stream: subj,
				Config: ConsumerConfig{
					Durable:       "d",
					DeliverPolicy: DeliverLast,
					FilterSubject: "foo.22",
					AckPolicy:     AckExplicit,
				},
			}
			req, err := json.Marshal(obsReq)
			require_NoError(t, err)
			_, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, subj), req, time.Second)
			require_NoError(t, err)
			nc.Close()
			s.Shutdown()
		})
	}
}

func TestJetStreamConsumerMaxDeliveries(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_WQ", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "MY_WQ", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Queue up our work item.
			sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			maxDeliver := 5
			ackWait := 10 * time.Millisecond

			o, err := mset.addConsumer(&ConsumerConfig{
				DeliverSubject: sub.Subject,
				AckPolicy:      AckExplicit,
				AckWait:        ackWait,
				MaxDeliver:     maxDeliver,
			})
			require_NoError(t, err)
			defer o.delete()

			// Wait for redeliveries to pile up.
			checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, err := sub.Pending(); err != nil || nmsgs != maxDeliver {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, maxDeliver)
				}
				return nil
			})

			// Now wait a bit longer and make sure we do not have more than maxDeliveries.
			time.Sleep(2 * ackWait)
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxDeliver {
				t.Fatalf("Did not receive correct number of messages: %d vs %d", nmsgs, maxDeliver)
			}
		})
	}
}

func TestJetStreamConsumerSingleTokenSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	filterSubject := "foo"
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{filterSubject},
	})
	require_NoError(t, err)

	req, err := json.Marshal(&CreateConsumerRequest{Stream: "TEST", Config: ConsumerConfig{
		FilterSubject: filterSubject,
		Name:          "name",
	}})

	if err != nil {
		t.Fatalf("failed to marshal consumer create request: %v", err)
	}

	resp, err := nc.Request(fmt.Sprintf("$JS.API.CONSUMER.CREATE.%s.%s.%s", "TEST", "name", "not_filter_subject"), req, time.Second*10)

	var apiResp ApiResponse
	json.Unmarshal(resp.Data, &apiResp)
	if err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if apiResp.Error == nil {
		t.Fatalf("expected error, got nil")
	}
	if apiResp.Error.ErrCode != 10131 {
		t.Fatalf("expected error 10131, got %v", apiResp.Error)
	}
}

func TestJetStreamConsumerPullDelayedFirstPullWithReplayOriginal(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_WQ", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "MY_WQ", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Queue up our work item.
			sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:      "d",
				AckPolicy:    AckExplicit,
				ReplayPolicy: ReplayOriginal,
			})
			require_NoError(t, err)
			defer o.delete()

			// Force delay here which triggers the bug.
			time.Sleep(250 * time.Millisecond)

			if _, err = nc.Request(o.requestNextMsgSubject(), nil, time.Second); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestJetStreamConsumerAckFloorFill(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MQ", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "MQ", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			for i := 1; i <= 4; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, fmt.Sprintf("msg-%d", i))
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "d",
				DeliverSubject: sub.Subject,
				AckPolicy:      AckExplicit,
			})
			require_NoError(t, err)
			defer o.delete()

			var first *nats.Msg

			for i := 1; i <= 3; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error receiving message %d: %v", i, err)
				}
				// Don't ack 1 or 4.
				if i == 1 {
					first = m
				} else if i == 2 || i == 3 {
					m.Respond(nil)
				}
			}
			nc.Flush()
			if info := o.info(); info.AckFloor.Consumer != 0 {
				t.Fatalf("Expected the ack floor to be 0, got %d", info.AckFloor.Consumer)
			}
			// Now ack first, should move ack floor to 3.
			first.Respond(nil)
			nc.Flush()

			checkFor(t, time.Second, 50*time.Millisecond, func() error {
				if info := o.info(); info.AckFloor.Consumer != 3 {
					return fmt.Errorf("Expected the ack floor to be 3, got %d", info.AckFloor.Consumer)
				}
				return nil
			})
		})
	}
}

func TestJetStreamConsumerAckAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "ACK-ACK"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	o, err := mset.addConsumer(&ConsumerConfig{Durable: "worker", AckPolicy: AckExplicit})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()
	rqn := o.requestNextMsgSubject()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// 4 for number of ack protocols to test them all.
	for i := 0; i < 4; i++ {
		sendStreamMsg(t, nc, mname, "Hello World!")
	}

	testAck := func(ackType []byte) {
		m, err := nc.Request(rqn, nil, 10*time.Millisecond)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Send a request for the ack and make sure the server "ack's" the ack.
		if _, err := nc.Request(m.Reply, ackType, 10*time.Millisecond); err != nil {
			t.Fatalf("Unexpected error on ack/ack: %v", err)
		}
	}

	testAck(AckAck)
	testAck(AckNak)
	testAck(AckProgress)
	testAck(AckTerm)
}

func TestJetStreamConsumerRateLimit(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "RATELIMIT"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	msgSize := 128 * 1024
	msg := make([]byte, msgSize)
	crand.Read(msg)

	// 10MB
	totalSize := 10 * 1024 * 1024
	toSend := totalSize / msgSize
	for i := 0; i < toSend; i++ {
		nc.Publish(mname, msg)
	}
	nc.Flush()

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		state := mset.state()
		if state.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected %d messages, got %d", toSend, state.Msgs)
		}
		return nil
	})

	// 100Mbit
	rateLimit := uint64(100 * 1024 * 1024)
	// Make sure if you set a rate with a pull based consumer it errors.
	_, err = mset.addConsumer(&ConsumerConfig{Durable: "to", AckPolicy: AckExplicit, RateLimit: rateLimit})
	if err == nil {
		t.Fatalf("Expected an error, got none")
	}

	// Now create one and measure the rate delivered.
	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:        "rate",
		DeliverSubject: "to",
		RateLimit:      rateLimit,
		AckPolicy:      AckNone})
	require_NoError(t, err)
	defer o.delete()

	var received int
	done := make(chan bool)

	start := time.Now()

	nc.Subscribe("to", func(m *nats.Msg) {
		received++
		if received >= toSend {
			done <- true
		}
	})
	nc.Flush()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive all the messages in time")
	}

	tt := time.Since(start)
	rate := float64(8*toSend*msgSize) / tt.Seconds()
	if rate > float64(rateLimit)*1.25 {
		t.Fatalf("Exceeded desired rate of %d mbps, got %0.f mbps", rateLimit/(1024*1024), rate/(1024*1024))
	}
}

func TestJetStreamConsumerEphemeralRecoveryAfterServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "MYS"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sub, _ := nc.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()
	nc.Flush()

	o, err := mset.addConsumer(&ConsumerConfig{
		DeliverSubject: sub.Subject,
		AckPolicy:      AckExplicit,
	})
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}
	defer o.delete()

	// Snapshot our name.
	oname := o.String()

	// Send 100 messages
	for i := 0; i < 100; i++ {
		sendStreamMsg(t, nc, mname, "Hello World!")
	}
	if state := mset.state(); state.Msgs != 100 {
		t.Fatalf("Expected %d messages, got %d", 100, state.Msgs)
	}

	// Read 6 messages
	for i := 0; i <= 6; i++ {
		if m, err := sub.NextMsg(time.Second); err == nil {
			m.Respond(nil)
		} else {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())

	restartServer := func() {
		t.Helper()
		// Stop current
		sd := s.JetStreamConfig().StoreDir
		s.Shutdown()
		// Restart.
		s = RunJetStreamServerOnPort(port, sd)
	}

	// Do twice
	for i := 0; i < 2; i++ {
		// Restart.
		restartServer()
		defer s.Shutdown()

		mset, err = s.GlobalAccount().lookupStream(mname)
		if err != nil {
			t.Fatalf("Expected to find a stream for %q", mname)
		}
		o = mset.lookupConsumer(oname)
		if o == nil {
			t.Fatalf("Error looking up consumer %q", oname)
		}
		// Make sure config does not have durable.
		if cfg := o.config(); cfg.Durable != _EMPTY_ {
			t.Fatalf("Expected no durable to be set")
		}
		// Wait for it to become active
		checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
			if !o.isActive() {
				return fmt.Errorf("Consumer not active")
			}
			return nil
		})
	}

	// Now close the connection. Make sure this acts like an ephemeral and goes away.
	o.setInActiveDeleteThreshold(10 * time.Millisecond)
	nc.Close()

	checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
		if o := mset.lookupConsumer(oname); o != nil {
			return fmt.Errorf("Consumer still active")
		}
		return nil
	})
}

func TestJetStreamConsumerMaxDeliveryAndServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "MYS"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	streamCreated := mset.createdTime()

	dsubj := "D.TO"
	max := 3

	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:        "TO",
		DeliverSubject: dsubj,
		AckPolicy:      AckExplicit,
		AckWait:        100 * time.Millisecond,
		MaxDeliver:     max,
	})
	defer o.delete()

	consumerCreated := o.createdTime()
	// For calculation of consumer created times below.
	time.Sleep(5 * time.Millisecond)

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sub, _ := nc.SubscribeSync(dsubj)
	nc.Flush()
	defer sub.Unsubscribe()

	// Send one message.
	sendStreamMsg(t, nc, mname, "order-1")

	checkSubPending := func(numExpected int) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if nmsgs, _, _ := sub.Pending(); nmsgs != numExpected {
				return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
			}
			return nil
		})
	}

	checkNumMsgs := func(numExpected uint64) {
		t.Helper()
		mset, err = s.GlobalAccount().lookupStream(mname)
		if err != nil {
			t.Fatalf("Expected to find a stream for %q", mname)
		}
		state := mset.state()
		if state.Msgs != numExpected {
			t.Fatalf("Expected %d msgs, got %d", numExpected, state.Msgs)
		}
	}

	// Wait til we know we have max queued up.
	checkSubPending(max)

	// Once here we have gone over the limit for the 1st message for max deliveries.
	// Send second
	sendStreamMsg(t, nc, mname, "order-2")

	// Just wait for first delivery + one redelivery.
	checkSubPending(max + 2)

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())

	restartServer := func() {
		t.Helper()
		sd := s.JetStreamConfig().StoreDir
		// Stop current
		s.Shutdown()
		// Restart.
		s = RunJetStreamServerOnPort(port, sd)
	}

	waitForClientReconnect := func() {
		t.Helper()
		require_NoError(t, nc.ForceReconnect())
		checkFor(t, 2500*time.Millisecond, 5*time.Millisecond, func() error {
			if !nc.IsConnected() {
				return fmt.Errorf("Not connected")
			}
			return nil
		})
	}

	// Restart.
	restartServer()
	defer s.Shutdown()

	checkNumMsgs(2)

	// Wait for client to be reconnected.
	waitForClientReconnect()

	// Once we are here send third order.
	sendStreamMsg(t, nc, mname, "order-3")
	checkNumMsgs(3)

	// Restart.
	restartServer()
	defer s.Shutdown()

	checkNumMsgs(3)

	// Wait for client to be reconnected.
	waitForClientReconnect()

	// Now we should have max times three on our sub.
	checkSubPending(max * 3)

	// Now do some checks on created timestamps.
	mset, err = s.GlobalAccount().lookupStream(mname)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", mname)
	}
	if mset.createdTime() != streamCreated {
		t.Fatalf("Stream creation time not restored, wanted %v, got %v", streamCreated, mset.createdTime())
	}
	o = mset.lookupConsumer("TO")
	if o == nil {
		t.Fatalf("Error looking up consumer: %v", err)
	}
	// Consumer created times can have a very small skew.
	delta := o.createdTime().Sub(consumerCreated)
	if delta > 5*time.Millisecond {
		t.Fatalf("Consumer creation time not restored, wanted %v, got %v", consumerCreated, o.createdTime())
	}
}

func TestJetStreamConsumerDeleteAndServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	sendSubj := "MYQ"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: sendSubj, Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	// Create basic work queue mode consumer.
	oname := "WQ"
	o, err := mset.addConsumer(workerModeConfig(oname))
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}

	// Now delete and then we will restart the
	o.delete()

	if numo := mset.numConsumers(); numo != 0 {
		t.Fatalf("Expected to have zero consumers, got %d", numo)
	}

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())
	sd := s.JetStreamConfig().StoreDir

	// Stop current
	s.Shutdown()

	// Restart.
	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()

	mset, err = s.GlobalAccount().lookupStream(sendSubj)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", sendSubj)
	}

	if numo := mset.numConsumers(); numo != 0 {
		t.Fatalf("Expected to have zero consumers, got %d", numo)
	}
}

func TestJetStreamConsumerDurableReconnectWithOnlyPending(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DT", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "DT", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			dname := "d22"
			subj1 := nats.NewInbox()

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj1,
				AckPolicy:      AckExplicit,
				AckWait:        25 * time.Millisecond})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			sendMsg := func(payload string) {
				t.Helper()
				if err := nc.Publish("foo.22", []byte(payload)); err != nil {
					return
				}
			}

			sendMsg("1")

			sub, _ := nc.SubscribeSync(subj1)
			defer sub.Unsubscribe()

			checkFor(t, 500*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 1 {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, 1)
				}
				return nil
			})

			// Now unsubscribe and wait to become inactive
			sub.Unsubscribe()
			checkFor(t, 250*time.Millisecond, 50*time.Millisecond, func() error {
				if o.isActive() {
					return fmt.Errorf("Consumer is still active")
				}
				return nil
			})

			// Send the second message while delivery subscriber is not running
			sendMsg("2")

			// Now we should be able to replace the delivery subject.
			subj2 := nats.NewInbox()
			o, err = mset.addConsumer(&ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj2,
				AckPolicy:      AckExplicit,
				AckWait:        25 * time.Millisecond})
			if err != nil {
				t.Fatalf("Unexpected error trying to add a new durable consumer: %v", err)
			}
			sub, _ = nc.SubscribeSync(subj2)
			defer sub.Unsubscribe()
			nc.Flush()

			// We should get msg "1" and "2" delivered. They will be reversed.
			for i := 0; i < 2; i++ {
				msg, err := sub.NextMsg(500 * time.Millisecond)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				sseq, _, dc, _, _ := replyInfo(msg.Reply)
				if sseq == 1 && dc == 1 {
					t.Fatalf("Expected a redelivery count greater then 1 for sseq 1, got %d", dc)
				}
				if sseq != 1 && sseq != 2 {
					t.Fatalf("Expected stream sequence of 1 or 2 but got %d", sseq)
				}
			}
		})
	}
}

func TestJetStreamConsumerDurableFilteredSubjectReconnect(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DT", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "DT", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			sendMsgs := func(toSend int) {
				for i := 0; i < toSend; i++ {
					var subj string
					if i%2 == 0 {
						subj = "foo.AA"
					} else {
						subj = "foo.ZZ"
					}
					_, err := js.Publish(subj, []byte("OK!"))
					require_NoError(t, err)
				}
			}

			// Send 50 msgs
			toSend := 50
			sendMsgs(toSend)

			dname := "d33"
			dsubj := nats.NewInbox()

			// Now create an consumer for foo.AA, only requesting the last one.
			_, err = mset.addConsumer(&ConsumerConfig{
				Durable:        dname,
				DeliverSubject: dsubj,
				FilterSubject:  "foo.AA",
				DeliverPolicy:  DeliverLast,
				AckPolicy:      AckExplicit,
				AckWait:        100 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			sub, _ := nc.SubscribeSync(dsubj)
			defer sub.Unsubscribe()

			// Used to calculate difference between store seq and delivery seq.
			storeBaseOff := 47

			getMsg := func(seq int) *nats.Msg {
				t.Helper()
				sseq := 2*seq + storeBaseOff
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, roseq, dcount, _, _ := replyInfo(m.Reply)
				if roseq != uint64(seq) {
					t.Fatalf("Expected consumer sequence of %d , got %d", seq, roseq)
				}
				if rsseq != uint64(sseq) {
					t.Fatalf("Expected stream sequence of %d , got %d", sseq, rsseq)
				}
				if dcount != 1 {
					t.Fatalf("Expected message to not be marked as redelivered")
				}
				return m
			}

			getRedeliveredMsg := func(seq int) *nats.Msg {
				t.Helper()
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				_, roseq, dcount, _, _ := replyInfo(m.Reply)
				if roseq != uint64(seq) {
					t.Fatalf("Expected consumer sequence of %d , got %d", seq, roseq)
				}
				if dcount < 2 {
					t.Fatalf("Expected message to be marked as redelivered")
				}
				// Ack this message.
				m.Respond(nil)
				return m
			}

			// All consumers start at 1 and always have increasing sequence numbers.
			m := getMsg(1)
			m.Respond(nil)

			// Now send 50 more, so 100 total, 26 (last + 50/2) for this consumer.
			sendMsgs(toSend)

			state := mset.state()
			if state.Msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, state.Msgs)
			}

			// For tracking next expected.
			nextSeq := 2
			noAcks := 0
			for i := 0; i < toSend/2; i++ {
				m := getMsg(nextSeq)
				if i%2 == 0 {
					m.Respond(nil) // Ack evens.
				} else {
					noAcks++
				}
				nextSeq++
			}

			// We should now get those redelivered.
			for i := 0; i < noAcks; i++ {
				getRedeliveredMsg(nextSeq)
				nextSeq++
			}

			// Now send 50 more.
			sendMsgs(toSend)

			storeBaseOff -= noAcks * 2

			for i := 0; i < toSend/2; i++ {
				m := getMsg(nextSeq)
				m.Respond(nil)
				nextSeq++
			}
		})
	}
}

func TestJetStreamConsumerInactiveNoDeadlock(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			// Send lots of msgs and have them queued up.
			for i := 0; i < 10000; i++ {
				js.Publish("DC", []byte("OK!"))
			}

			if state := mset.state(); state.Msgs != 10000 {
				t.Fatalf("Expected %d messages, got %d", 10000, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			sub.SetPendingLimits(-1, -1)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()

			for i := 0; i < 10; i++ {
				if _, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			// Force us to become inactive but we want to make sure we do not lock up
			// the internal sendq.
			sub.Unsubscribe()
			nc.Flush()
		})
	}
}

func TestJetStreamConsumerReconnect(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "ET", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "ET", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			// Capture the subscription.
			delivery := sub.Subject

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery, AckPolicy: AckExplicit})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !o.isActive() {
				t.Fatalf("Expected the consumer to be considered active")
			}
			if numo := mset.numConsumers(); numo != 1 {
				t.Fatalf("Expected number of consumers to be 1, got %d", numo)
			}

			// We will simulate reconnect by unsubscribing on one connection and forming
			// the same on another. Once we have cluster tests we will do more testing on
			// reconnect scenarios.
			getMsg := func(seqno int) *nats.Msg {
				t.Helper()
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error for %d: %v", seqno, err)
				}
				if seq := o.seqFromReply(m.Reply); seq != uint64(seqno) {
					t.Fatalf("Expected sequence of %d , got %d", seqno, seq)
				}
				m.Respond(nil)
				return m
			}

			sendMsg := func() {
				t.Helper()
				if err := nc.Publish("foo.22", []byte("OK!")); err != nil {
					return
				}
			}

			checkForInActive := func() {
				checkFor(t, 250*time.Millisecond, 50*time.Millisecond, func() error {
					if o.isActive() {
						return fmt.Errorf("Consumer is still active")
					}
					return nil
				})
			}

			// Send and Pull first message.
			sendMsg() // 1
			getMsg(1)
			// Cancel first one.
			sub.Unsubscribe()
			// Re-establish new sub on same subject.
			sub, _ = nc.SubscribeSync(delivery)
			nc.Flush()

			// We should be getting 2 here.
			sendMsg() // 2
			getMsg(2)

			sub.Unsubscribe()
			checkForInActive()

			// send 3-10
			for i := 0; i <= 7; i++ {
				sendMsg()
			}
			// Make sure they are all queued up with no interest.
			nc.Flush()

			// Restablish again.
			sub, _ = nc.SubscribeSync(delivery)
			nc.Flush()

			// We should be getting 3-10 here.
			for i := 3; i <= 10; i++ {
				getMsg(i)
			}
		})
	}
}

func TestJetStreamConsumerDurableReconnect(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DT", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "DT", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			dname := "d22"
			subj1 := nats.NewInbox()

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj1,
				AckPolicy:      AckExplicit,
				AckWait:        50 * time.Millisecond})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			sendMsg := func() {
				t.Helper()
				if err := nc.Publish("foo.22", []byte("OK!")); err != nil {
					return
				}
			}

			// Send 10 msgs
			toSend := 10
			for i := 0; i < toSend; i++ {
				sendMsg()
			}

			sub, _ := nc.SubscribeSync(subj1)
			defer sub.Unsubscribe()

			checkFor(t, 500*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
				}
				return nil
			})

			getMsg := func(seqno int) *nats.Msg {
				t.Helper()
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if seq := o.streamSeqFromReply(m.Reply); seq != uint64(seqno) {
					t.Fatalf("Expected sequence of %d , got %d", seqno, seq)
				}
				m.Respond(nil)
				return m
			}

			// Ack first half
			for i := 1; i <= toSend/2; i++ {
				m := getMsg(i)
				m.Respond(nil)
			}

			// Now unsubscribe and wait to become inactive
			sub.Unsubscribe()
			checkFor(t, 250*time.Millisecond, 50*time.Millisecond, func() error {
				if o.isActive() {
					return fmt.Errorf("Consumer is still active")
				}
				return nil
			})

			// Now we should be able to replace the delivery subject.
			subj2 := nats.NewInbox()
			sub, _ = nc.SubscribeSync(subj2)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err = mset.addConsumer(&ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj2,
				AckPolicy:      AckExplicit,
				AckWait:        50 * time.Millisecond})
			if err != nil {
				t.Fatalf("Unexpected error trying to add a new durable consumer: %v", err)
			}

			// We should get the remaining messages here.
			for i := toSend/2 + 1; i <= toSend; i++ {
				m := getMsg(i)
				m.Respond(nil)
			}
		})
	}
}

func TestJetStreamConsumerReplayRate(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			totalMsgs := 10

			var gaps []time.Duration
			lst := time.Now()

			for i := 0; i < totalMsgs; i++ {
				gaps = append(gaps, time.Since(lst))
				lst = time.Now()
				nc.Publish("DC", []byte("OK!"))
				// Calculate a gap between messages.
				gap := 10*time.Millisecond + time.Duration(rand.Intn(20))*time.Millisecond
				time.Sleep(gap)
			}

			if state := mset.state(); state.Msgs != uint64(totalMsgs) {
				t.Fatalf("Expected %d messages, got %d", totalMsgs, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()

			// Firehose/instant which is default.
			last := time.Now()
			for i := 0; i < totalMsgs; i++ {
				if _, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				now := time.Now()
				// Delivery from addConsumer starts in a go routine, so be
				// more tolerant for the first message.
				limit := 5 * time.Millisecond
				if i == 0 {
					limit = 10 * time.Millisecond
				}
				if now.Sub(last) > limit {
					t.Fatalf("Expected firehose/instant delivery, got message gap of %v", now.Sub(last))
				}
				last = now
			}

			// Now do replay rate to match original.
			o, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject, ReplayPolicy: ReplayOriginal})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()

			// Original rate messsages were received for push based consumer.
			for i := 0; i < totalMsgs; i++ {
				start := time.Now()
				if _, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				gap := time.Since(start)
				// 15ms is high but on macs time.Sleep(delay) does not sleep only delay.
				// Also on travis if things get bogged down this could be delayed.
				gl, gh := gaps[i]-10*time.Millisecond, gaps[i]+15*time.Millisecond
				if gap < gl || gap > gh {
					t.Fatalf("Gap is off for %d, expected %v got %v", i, gaps[i], gap)
				}
			}

			// Now create pull based.
			oc := workerModeConfig("PM")
			oc.ReplayPolicy = ReplayOriginal
			o, err = mset.addConsumer(oc)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()

			for i := 0; i < totalMsgs; i++ {
				start := time.Now()
				if _, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				gap := time.Since(start)
				// 10ms is high but on macs time.Sleep(delay) does not sleep only delay.
				gl, gh := gaps[i]-5*time.Millisecond, gaps[i]+10*time.Millisecond
				if gap < gl || gap > gh {
					t.Fatalf("Gap is incorrect for %d, expected %v got %v", i, gaps[i], gap)
				}
			}
		})
	}
}

func TestJetStreamConsumerReplayRateNoAck(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			totalMsgs := 10
			for i := 0; i < totalMsgs; i++ {
				nc.Request("DC", []byte("Hello World"), time.Second)
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			}
			if state := mset.state(); state.Msgs != uint64(totalMsgs) {
				t.Fatalf("Expected %d messages, got %d", totalMsgs, state.Msgs)
			}
			subj := "d.dc"
			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "derek",
				DeliverSubject: subj,
				AckPolicy:      AckNone,
				ReplayPolicy:   ReplayOriginal,
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()
			// Sleep a random amount of time.
			time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)

			sub, _ := nc.SubscribeSync(subj)
			nc.Flush()

			checkFor(t, time.Second, 25*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != totalMsgs {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, totalMsgs)
				}
				return nil
			})
		})
	}
}

func TestJetStreamConsumerReplayQuit(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 2 msgs
			nc.Request("DC", []byte("OK!"), time.Second)
			time.Sleep(100 * time.Millisecond)
			nc.Request("DC", []byte("OK!"), time.Second)

			if state := mset.state(); state.Msgs != 2 {
				t.Fatalf("Expected %d messages, got %d", 2, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			// Now do replay rate to match original.
			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject, ReplayPolicy: ReplayOriginal})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Allow loop and deliver / replay go routine to spin up
			time.Sleep(50 * time.Millisecond)
			base := runtime.NumGoroutine()
			o.delete()

			checkFor(t, 100*time.Millisecond, 10*time.Millisecond, func() error {
				if runtime.NumGoroutine() >= base {
					return fmt.Errorf("Consumer go routines still running")
				}
				return nil
			})
		})
	}
}

func TestJetStreamConsumerPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	msetConfig := StreamConfig{
		Name:     "sr22",
		Storage:  MemoryStorage,
		Subjects: []string{"foo"},
	}

	mset, err := acc.addStream(&msetConfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	payload := []byte("Hello World")

	toStore := 2000000
	for i := 0; i < toStore; i++ {
		nc.Publish("foo", payload)
	}
	nc.Flush()

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "d",
		DeliverSubject: "d",
		AckPolicy:      AckNone,
	})
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}

	var received int
	done := make(chan bool)

	nc.Subscribe("d", func(m *nats.Msg) {
		received++
		if received >= toStore {
			done <- true
		}
	})
	start := time.Now()
	nc.Flush()

	<-done
	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
}

func TestJetStreamConsumerAckFileStorePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	msetConfig := StreamConfig{
		Name:     "sr22",
		Storage:  FileStorage,
		Subjects: []string{"foo"},
	}

	mset, err := acc.addStream(&msetConfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	payload := []byte("Hello World")

	toStore := uint64(200000)
	for i := uint64(0); i < toStore; i++ {
		nc.Publish("foo", payload)
	}
	nc.Flush()

	if msgs := mset.state().Msgs; msgs != uint64(toStore) {
		t.Fatalf("Expected %d messages, got %d", toStore, msgs)
	}

	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:        "d",
		DeliverSubject: "d",
		AckPolicy:      AckExplicit,
		AckWait:        10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}
	defer o.stop()

	var received uint64
	done := make(chan bool)

	sub, _ := nc.Subscribe("d", func(m *nats.Msg) {
		m.Respond(nil) // Ack
		received++
		if received >= toStore {
			done <- true
		}
	})
	sub.SetPendingLimits(-1, -1)

	start := time.Now()
	nc.Flush()

	<-done
	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
}

// This test is in support fo clients that want to match on subject, they
// can set the filter subject always. We always store the subject so that
// should the stream later be edited to expand into more subjects the consumer
// still gets what was actually requested
func TestJetStreamConsumerFilterSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	sc := &StreamConfig{Name: "MY_STREAM", Subjects: []string{"foo"}}
	mset, err := s.GlobalAccount().addStream(sc)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	cfg := &ConsumerConfig{
		Durable:        "d",
		DeliverSubject: "A",
		AckPolicy:      AckExplicit,
		FilterSubject:  "foo",
	}

	o, err := mset.addConsumer(cfg)
	if err != nil {
		t.Fatalf("Unexpected error adding consumer: %v", err)
	}
	defer o.delete()

	if o.info().Config.FilterSubject != "foo" {
		t.Fatalf("Expected the filter to be stored")
	}

	// Now use the original cfg with updated delivery subject and make sure that works ok.
	cfg = &ConsumerConfig{
		Durable:        "d",
		DeliverSubject: "B",
		AckPolicy:      AckExplicit,
		FilterSubject:  "foo",
	}

	o, err = mset.addConsumer(cfg)
	if err != nil {
		t.Fatalf("Unexpected error adding consumer: %v", err)
	}
	defer o.delete()
}

func TestJetStreamConsumerUpdateRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{
			Name:      "MY_STREAM",
			Storage:   MemoryStorage,
			Subjects:  []string{"foo.>"},
			Retention: InterestPolicy,
		}},
		{"FileStore", &StreamConfig{
			Name:      "MY_STREAM",
			Storage:   FileStorage,
			Subjects:  []string{"foo.>"},
			Retention: InterestPolicy,
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Create a durable consumer.
			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "dur22",
				DeliverSubject: sub.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      AckExplicit,
				AckWait:        100 * time.Millisecond,
				MaxDeliver:     3,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o.delete()

			// Send 20 messages
			toSend := 20
			for i := 1; i <= toSend; i++ {
				sendStreamMsg(t, nc, "foo.bar", fmt.Sprintf("msg-%v", i))
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %v messages, got %d", toSend, state.Msgs)
			}

			// Receive the messages and ack only every 4th
			for i := 0; i < toSend; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error getting message: %v", err)
				}
				seq, _, _, _, _ := replyInfo(m.Reply)
				// 4, 8, 12, 16, 20
				if seq%4 == 0 {
					m.Respond(nil)
				}
			}

			// Now close the sub and open a new one and update the consumer.
			sub.Unsubscribe()

			// Wait for it to become inactive
			checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
				if o.isActive() {
					return fmt.Errorf("Consumer still active")
				}
				return nil
			})

			// Send 20 more messages.
			for i := toSend; i < toSend*2; i++ {
				sendStreamMsg(t, nc, "foo.bar", fmt.Sprintf("msg-%v", i))
			}

			// Create new subscription.
			sub, _ = nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err = mset.addConsumer(&ConsumerConfig{
				Durable:        "dur22",
				DeliverSubject: sub.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      AckExplicit,
				AckWait:        100 * time.Millisecond,
				MaxDeliver:     3,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o.delete()

			expect := toSend + toSend - 5 // mod 4 acks
			checkFor(t, time.Second, 5*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != expect {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, expect)
				}
				return nil
			})

			for i, eseq := 0, uint64(1); i < expect; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error getting message: %v", err)
				}
				// Skip the ones we ack'd from above. We should not get them back here.
				if eseq <= uint64(toSend) && eseq%4 == 0 {
					eseq++
				}
				seq, _, dc, _, _ := replyInfo(m.Reply)
				if seq != eseq {
					t.Fatalf("Expected stream sequence of %d, got %d", eseq, seq)
				}
				if seq <= uint64(toSend) && dc != 2 {
					t.Fatalf("Expected delivery count of 2 for sequence of %d, got %d", seq, dc)
				}
				if seq > uint64(toSend) && dc != 1 {
					t.Fatalf("Expected delivery count of 1 for sequence of %d, got %d", seq, dc)
				}
				if seq > uint64(toSend) {
					m.Respond(nil) // Ack
				}
				eseq++
			}

			// We should get the second half back since we did not ack those from above.
			expect = toSend - 5
			checkFor(t, time.Second, 5*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != expect {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, expect)
				}
				return nil
			})

			for i, eseq := 0, uint64(1); i < expect; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error getting message: %v", err)
				}
				// Skip the ones we ack'd from above. We should not get them back here.
				if eseq <= uint64(toSend) && eseq%4 == 0 {
					eseq++
				}
				seq, _, dc, _, _ := replyInfo(m.Reply)
				if seq != eseq {
					t.Fatalf("Expected stream sequence of %d, got %d", eseq, seq)
				}
				if dc != 3 {
					t.Fatalf("Expected delivery count of 3 for sequence of %d, got %d", seq, dc)
				}
				eseq++
			}
		})
	}
}

func TestJetStreamConsumerMaxAckPending(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  MemoryStorage,
			Subjects: []string{"foo.*"},
		}},
		{"FileStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  FileStorage,
			Subjects: []string{"foo.*"},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Do error scenarios.
			_, err = mset.addConsumer(&ConsumerConfig{
				Durable:        "d22",
				DeliverSubject: nats.NewInbox(),
				AckPolicy:      AckNone,
				MaxAckPending:  1,
			})
			if err == nil {
				t.Fatalf("Expected error, MaxAckPending only applicable to ack != AckNone")
			}

			// Queue up 100 messages.
			toSend := 100
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.bar", fmt.Sprintf("MSG: %d", i+1))
			}

			// Limit to 33
			maxAckPending := 33

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "d22",
				DeliverSubject: nats.NewInbox(),
				AckPolicy:      AckExplicit,
				MaxAckPending:  maxAckPending,
			})
			require_NoError(t, err)

			defer o.delete()

			sub, _ := nc.SubscribeSync(o.info().Config.DeliverSubject)
			defer sub.Unsubscribe()

			checkSubPending := func(numExpected int) {
				t.Helper()
				checkFor(t, time.Second, 20*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != numExpected {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
					}
					return nil
				})
			}

			checkSubPending(maxAckPending)
			// We hit the limit, double check we stayed there.
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxAckPending {
				t.Fatalf("Too many messages received: %d vs %d", nmsgs, maxAckPending)
			}

			// Now ack them all.
			for i := 0; i < maxAckPending; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error receiving message %d: %v", i, err)
				}
				m.Respond(nil)
			}
			checkSubPending(maxAckPending)

			o.stop()
			mset.purge(nil)

			// Now test a consumer that is live while we publish messages to the stream.
			o, err = mset.addConsumer(&ConsumerConfig{
				Durable:        "d33",
				DeliverSubject: nats.NewInbox(),
				AckPolicy:      AckExplicit,
				MaxAckPending:  maxAckPending,
			})
			require_NoError(t, err)

			defer o.delete()

			sub, _ = nc.SubscribeSync(o.info().Config.DeliverSubject)
			defer sub.Unsubscribe()
			nc.Flush()

			checkSubPending(0)

			// Now stream more then maxAckPending.
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.baz", fmt.Sprintf("MSG: %d", i+1))
			}
			checkSubPending(maxAckPending)
			// We hit the limit, double check we stayed there.
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxAckPending {
				t.Fatalf("Too many messages received: %d vs %d", nmsgs, maxAckPending)
			}
		})
	}
}

func TestJetStreamConsumerPullMaxAckPending(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  MemoryStorage,
			Subjects: []string{"foo.*"},
		}},
		{"FileStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  FileStorage,
			Subjects: []string{"foo.*"},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Queue up 100 messages.
			toSend := 100
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.bar", fmt.Sprintf("MSG: %d", i+1))
			}

			// Limit to 33
			maxAckPending := 33

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:       "d22",
				AckPolicy:     AckExplicit,
				MaxAckPending: maxAckPending,
			})
			require_NoError(t, err)

			defer o.delete()

			getSubj := o.requestNextMsgSubject()

			var toAck []*nats.Msg

			for i := 0; i < maxAckPending; i++ {
				if m, err := nc.Request(getSubj, nil, time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				} else {
					toAck = append(toAck, m)
				}
			}

			// Now ack them all.
			for _, m := range toAck {
				m.Respond(nil)
			}

			// Now do batch above the max.
			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			checkSubPending := func(numExpected int) {
				t.Helper()
				checkFor(t, time.Second, 10*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != numExpected {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
					}
					return nil
				})
			}

			req := &JSApiConsumerGetNextRequest{Batch: maxAckPending}
			jreq, _ := json.Marshal(req)
			nc.PublishRequest(getSubj, sub.Subject, jreq)

			checkSubPending(maxAckPending)
			// We hit the limit, double check we stayed there.
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxAckPending {
				t.Fatalf("Too many messages received: %d vs %d", nmsgs, maxAckPending)
			}
		})
	}
}

func TestJetStreamConsumerPullMaxAckPendingRedeliveries(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  MemoryStorage,
			Subjects: []string{"foo.*"},
		}},
		{"FileStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  FileStorage,
			Subjects: []string{"foo.*"},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Queue up 10 messages.
			toSend := 10
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.bar", fmt.Sprintf("MSG: %d", i+1))
			}

			// Limit to 1
			maxAckPending := 1
			ackWait := 20 * time.Millisecond
			expSeq := uint64(4)

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:       "d22",
				DeliverPolicy: DeliverByStartSequence,
				OptStartSeq:   expSeq,
				AckPolicy:     AckExplicit,
				AckWait:       ackWait,
				MaxAckPending: maxAckPending,
			})
			require_NoError(t, err)

			defer o.delete()

			getSubj := o.requestNextMsgSubject()
			delivery := uint64(1)

			getNext := func() {
				t.Helper()
				m, err := nc.Request(getSubj, nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				sseq, dseq, dcount, _, pending := replyInfo(m.Reply)
				if sseq != expSeq {
					t.Fatalf("Expected stream sequence of %d, got %d", expSeq, sseq)
				}
				if dseq != delivery {
					t.Fatalf("Expected consumer sequence of %d, got %d", delivery, dseq)
				}
				if dcount != delivery {
					t.Fatalf("Expected delivery count of %d, got %d", delivery, dcount)
				}
				if pending != uint64(toSend)-expSeq {
					t.Fatalf("Expected pending to be %d, got %d", uint64(toSend)-expSeq, pending)
				}
				delivery++
			}

			getNext()
			getNext()
			getNext()
			getNext()
			getNext()
		})
	}
}

// User report of bug.
func TestJetStreamConsumerBadNumPending(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.*"},
	})
	require_NoError(t, err)

	newOrders := func(n int) {
		// Queue up new orders.
		for i := 0; i < n; i++ {
			js.Publish("orders.created", []byte("NEW"))
		}
	}

	newOrders(10)

	// Create to subscribers.
	process := func(m *nats.Msg) {
		js.Publish("orders.approved", []byte("APPROVED"))
	}

	op, err := js.Subscribe("orders.created", process)
	require_NoError(t, err)

	defer op.Unsubscribe()

	mon, err := js.SubscribeSync("orders.*")
	require_NoError(t, err)

	defer mon.Unsubscribe()

	waitForMsgs := func(n uint64) {
		t.Helper()
		checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
			si, err := js.StreamInfo("ORDERS")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si.State.Msgs != n {
				return fmt.Errorf("Expected %d msgs, got state: %+v", n, si.State)
			}
			return nil
		})
	}

	checkForNoPending := func(sub *nats.Subscription) {
		t.Helper()
		if ci, err := sub.ConsumerInfo(); err != nil || ci == nil || ci.NumPending != 0 {
			if ci != nil && ci.NumPending != 0 {
				t.Fatalf("Bad consumer NumPending, expected 0 but got %d", ci.NumPending)
			} else {
				t.Fatalf("Bad consumer info: %+v", ci)
			}
		}
	}

	waitForMsgs(20)
	checkForNoPending(op)
	checkForNoPending(mon)

	newOrders(10)

	waitForMsgs(40)
	checkForNoPending(op)
	checkForNoPending(mon)
}

// We had a report of a consumer delete crashing the server when in interest retention mode.
// This I believe is only really possible in clustered mode, but we will force the issue here.
func TestJetStreamConsumerCleanupWithRetentionPolicy(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "ORDERS",
		Subjects:  []string{"orders.*"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("orders.*")
	require_NoError(t, err)

	payload := []byte("Hello World")
	for i := 0; i < 10; i++ {
		subj := fmt.Sprintf("orders.%d", i+1)
		js.Publish(subj, payload)
	}

	checkSubsPending(t, sub, 10)

	for i := 0; i < 10; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		m.AckSync()
	}

	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}

	acc := s.GlobalAccount()
	mset, err := acc.lookupStream("ORDERS")
	require_NoError(t, err)

	o := mset.lookupConsumer(ci.Name)
	if o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	lseq := mset.lastSeq()
	o.mu.Lock()
	// Force boundary condition here.
	o.asflr = lseq + 2
	o.mu.Unlock()
	sub.Unsubscribe()

	// Make sure server still available.
	if _, err := js.StreamInfo("ORDERS"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamConsumerPendingBugWithKV(t *testing.T) {
	msc := StreamConfig{
		Name:       "KV",
		Subjects:   []string{"kv.>"},
		Storage:    MemoryStorage,
		MaxMsgsPer: 1,
	}
	fsc := msc
	fsc.Storage = FileStorage

	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &msc},
		{"FileStore", &fsc},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			// Client based API
			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			// Not in Go client under server yet.
			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			js.Publish("kv.1", []byte("1"))
			js.Publish("kv.2", []byte("2"))
			js.Publish("kv.3", []byte("3"))
			js.Publish("kv.1", []byte("4"))

			si, err := js.StreamInfo("KV")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si.State.Msgs != 3 {
				t.Fatalf("Expected 3 total msgs, got %d", si.State.Msgs)
			}

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "dlc",
				DeliverSubject: "xxx",
				DeliverPolicy:  DeliverLastPerSubject,
				FilterSubject:  ">",
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ci := o.info(); ci.NumPending != 3 {
				t.Fatalf("Expected pending of 3, got %d", ci.NumPending)
			}
		})
	}
}

// Issue #2423
func TestJetStreamConsumerBadCreateErr(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*"},
		Storage:  nats.MemoryStorage,
	})
	require_NoError(t, err)

	// When adding a consumer with both deliver subject and max wait (push vs pull),
	// we got the wrong err about deliver subject having a wildcard.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "nowcerr",
		DeliverSubject: "X",
		MaxWaiting:     100,
	})
	if err == nil {
		t.Fatalf("Expected an error but got none")
	}
	if !strings.Contains(err.Error(), "push mode can not set max waiting") {
		t.Fatalf("Incorrect error returned: %v", err)
	}
}

func TestJetStreamConsumerPushBound(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Storage:  nats.MemoryStorage,
		Subjects: []string{"foo"},
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// We want to test extended consumer info for push based consumers.
	// We need to do these by hand for now.
	createConsumer := func(name, deliver string) {
		t.Helper()
		creq := CreateConsumerRequest{
			Stream: "TEST",
			Config: ConsumerConfig{
				Durable:        name,
				DeliverSubject: deliver,
				AckPolicy:      AckExplicit,
			},
		}
		req, err := json.Marshal(creq)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		resp, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", name), req, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var ccResp JSApiConsumerCreateResponse
		if err := json.Unmarshal(resp.Data, &ccResp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ccResp.ConsumerInfo == nil || ccResp.Error != nil {
			t.Fatalf("Got a bad response %+v", ccResp)
		}
	}

	consumerInfo := func(name string) *ConsumerInfo {
		t.Helper()
		resp, err := nc.Request(fmt.Sprintf(JSApiConsumerInfoT, "TEST", name), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var cinfo JSApiConsumerInfoResponse
		if err := json.Unmarshal(resp.Data, &cinfo); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if cinfo.ConsumerInfo == nil || cinfo.Error != nil {
			t.Fatalf("Got a bad response %+v", cinfo)
		}
		return cinfo.ConsumerInfo
	}

	// First create a durable push and make sure we show now active status.
	createConsumer("dlc", "d.X")
	if ci := consumerInfo("dlc"); ci.PushBound {
		t.Fatalf("Expected push bound to be false")
	}
	// Now bind the deliver subject.
	sub, _ := nc.SubscribeSync("d.X")
	nc.Flush() // Make sure it registers.
	// Check that its reported.
	if ci := consumerInfo("dlc"); !ci.PushBound {
		t.Fatalf("Expected push bound to be set")
	}
	sub.Unsubscribe()
	nc.Flush() // Make sure it registers.
	if ci := consumerInfo("dlc"); ci.PushBound {
		t.Fatalf("Expected push bound to be false")
	}

	// Now make sure we have queue groups indictated as needed.
	createConsumer("ik", "d.Z")
	// Now bind the deliver subject with a queue group.
	sub, _ = nc.QueueSubscribeSync("d.Z", "g22")
	defer sub.Unsubscribe()
	nc.Flush() // Make sure it registers.
	// Check that queue group is not reported.
	if ci := consumerInfo("ik"); ci.PushBound {
		t.Fatalf("Expected push bound to be false")
	}
	sub.Unsubscribe()
	nc.Flush() // Make sure it registers.
	if ci := consumerInfo("ik"); ci.PushBound {
		t.Fatalf("Expected push bound to be false")
	}

	// Make sure pull consumers report PushBound as false by default.
	createConsumer("rip", _EMPTY_)
	if ci := consumerInfo("rip"); ci.PushBound {
		t.Fatalf("Expected push bound to be false")
	}
}

// Got a report of memory leaking, tracked it to internal clients for consumers.
func TestJetStreamConsumerInternalClientLeak(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:    "TEST",
		Storage: nats.MemoryStorage,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ga, sa := s.GlobalAccount(), s.SystemAccount()
	ncb, nscb := ga.NumConnections(), sa.NumConnections()

	// Create 10 consumers
	for i := 0; i < 10; i++ {
		ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{DeliverSubject: "x"})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Accelerate ephemeral cleanup.
		mset, err := ga.lookupStream("TEST")
		if err != nil {
			t.Fatalf("Expected to find a stream for %q", "TEST")
		}
		o := mset.lookupConsumer(ci.Name)
		if o == nil {
			t.Fatalf("Error looking up consumer %q", ci.Name)
		}
		o.setInActiveDeleteThreshold(500 * time.Millisecond)
	}

	// Wait for them to all go away.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Consumers == 0 {
			return nil
		}
		return fmt.Errorf("Consumers still present")
	})
	// Make sure we are not leaking clients/connections.
	// Server does not see these so need to look at account.
	if nca := ga.NumConnections(); nca != ncb {
		t.Fatalf("Leaked clients in global account: %d vs %d", ncb, nca)
	}
	if nsca := sa.NumConnections(); nsca != nscb {
		t.Fatalf("Leaked clients in system account: %d vs %d", nscb, nsca)
	}
}

func TestJetStreamConsumerEventingRaceOnShutdown(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s, nats.NoReconnect())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  nats.MemoryStorage,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			if _, err := js.SubscribeSync("foo", nats.BindStream("TEST")); err != nil {
				return
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	s.Shutdown()

	wg.Wait()
}

func TestJetStreamConsumerNoMsgPayload(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "S"})
	require_NoError(t, err)

	msg := nats.NewMsg("S")
	msg.Header.Set("name", "derek")
	msg.Data = bytes.Repeat([]byte("A"), 128)
	for i := 0; i < 10; i++ {
		msg.Reply = _EMPTY_ // Fixed in Go client but not in embdedded on yet.
		_, err = js.PublishMsgAsync(msg)
		require_NoError(t, err)
	}

	mset, err := s.GlobalAccount().lookupStream("S")
	require_NoError(t, err)

	// Now create our consumer with no payload option.
	_, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: "_d_", Durable: "d22", HeadersOnly: true})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("S", nats.Durable("d22"))
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		if len(m.Data) > 0 {
			t.Fatalf("Expected no payload")
		}
		if ms := m.Header.Get(JSMsgSize); ms != "128" {
			t.Fatalf("Expected a header with msg size, got %q", ms)
		}
	}
}

func TestJetStreamConsumerUpdateSurvival(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "X"})
	require_NoError(t, err)

	// First create a consumer with max ack pending.
	_, err = js.AddConsumer("X", &nats.ConsumerConfig{
		Durable:       "dlc",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxAckPending: 1024,
	})
	require_NoError(t, err)

	// Now do same name but pull. This will update the MaxAcKPending
	ci, err := js.UpdateConsumer("X", &nats.ConsumerConfig{
		Durable:       "dlc",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxAckPending: 22,
	})
	require_NoError(t, err)

	if ci.Config.MaxAckPending != 22 {
		t.Fatalf("Expected MaxAckPending to be 22, got %d", ci.Config.MaxAckPending)
	}

	// Make sure this survives across a restart.
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	ci, err = js.ConsumerInfo("X", "dlc")
	require_NoError(t, err)

	if ci.Config.MaxAckPending != 22 {
		t.Fatalf("Expected MaxAckPending to be 22, got %d", ci.Config.MaxAckPending)
	}
}

func TestJetStreamConsumerPendingCountWithRedeliveries(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:    "test",
		AckPolicy:  nats.AckExplicitPolicy,
		AckWait:    50 * time.Millisecond,
		MaxDeliver: 1,
	})
	require_NoError(t, err)

	// Publish 1st message
	_, err = js.Publish("foo", []byte("msg1"))
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "test")
	require_NoError(t, err)
	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	for _, m := range msgs {
		require_Equal(t, string(m.Data), "msg1")
		// Do not ack the message
	}
	// Check consumer info, pending should be 0
	ci, err := js.ConsumerInfo("TEST", "test")
	require_NoError(t, err)
	if ci.NumPending != 0 {
		t.Fatalf("Expected consumer info pending count to be 0, got %v", ci.NumPending)
	}

	// Wait for more than expiration
	time.Sleep(100 * time.Millisecond)

	// Publish 2nd message
	_, err = js.Publish("foo", []byte("msg2"))
	require_NoError(t, err)

	msgs, err = sub.Fetch(1)
	require_NoError(t, err)
	for _, m := range msgs {
		require_Equal(t, string(m.Data), "msg2")
		// Its deliver count should be 1
		meta, err := m.Metadata()
		require_NoError(t, err)
		if meta.NumDelivered != 1 {
			t.Fatalf("Expected message's deliver count to be 1, got %v", meta.NumDelivered)
		}
	}
	// Check consumer info, pending should be 0
	ci, err = js.ConsumerInfo("TEST", "test")
	require_NoError(t, err)
	if ci.NumPending != 0 {
		t.Fatalf("Expected consumer info pending count to be 0, got %v", ci.NumPending)
	}
}

func TestJetStreamConsumerPullHeartBeats(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "T", Storage: nats.MemoryStorage})
	require_NoError(t, err)

	_, err = js.AddConsumer("T", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	rsubj := fmt.Sprintf(JSApiRequestNextT, "T", "dlc")

	type tsMsg struct {
		received time.Time
		msg      *nats.Msg
	}

	doReq := func(batch int, hb, expires time.Duration, expected int) []*tsMsg {
		t.Helper()
		req := &JSApiConsumerGetNextRequest{Batch: batch, Expires: expires, Heartbeat: hb}
		jreq, err := json.Marshal(req)
		require_NoError(t, err)
		reply := nats.NewInbox()
		var msgs []*tsMsg
		var mu sync.Mutex

		sub, err := nc.Subscribe(reply, func(m *nats.Msg) {
			mu.Lock()
			msgs = append(msgs, &tsMsg{time.Now(), m})
			mu.Unlock()
		})
		require_NoError(t, err)

		err = nc.PublishRequest(rsubj, reply, jreq)
		require_NoError(t, err)
		checkFor(t, time.Second, 50*time.Millisecond, func() error {
			mu.Lock()
			nr := len(msgs)
			mu.Unlock()
			if nr >= expected {
				return nil
			}
			return fmt.Errorf("Only have seen %d of %d responses", nr, expected)
		})
		sub.Unsubscribe()
		return msgs
	}

	reqBad := nats.Header{"Status": []string{"400"}, "Description": []string{"Bad Request - heartbeat value too large"}}
	expectErr := func(msgs []*tsMsg) {
		t.Helper()
		if len(msgs) != 1 {
			t.Fatalf("Expected 1 msg, got %d", len(msgs))
		}
		if !reflect.DeepEqual(msgs[0].msg.Header, reqBad) {
			t.Fatalf("Expected %+v hdr, got %+v", reqBad, msgs[0].msg.Header)
		}
	}

	// Test errors first.
	// Setting HB with no expires.
	expectErr(doReq(1, 100*time.Millisecond, 0, 1))
	// If HB larger than 50% of expires..
	expectErr(doReq(1, 75*time.Millisecond, 100*time.Millisecond, 1))

	expectHBs := func(start time.Time, msgs []*tsMsg, expected int, hbi time.Duration) {
		t.Helper()
		if len(msgs) != expected {
			t.Fatalf("Expected %d but got %d", expected, len(msgs))
		}
		// expected -1 should be all HBs.
		for i, ts := 0, start; i < expected-1; i++ {
			tr, m := msgs[i].received, msgs[i].msg
			if m.Header.Get("Status") != "100" {
				t.Fatalf("Expected a 100 status code, got %q", m.Header.Get("Status"))
			}
			if m.Header.Get("Description") != "Idle Heartbeat" {
				t.Fatalf("Wrong description, got %q", m.Header.Get("Description"))
			}
			ts = ts.Add(hbi)
			if tr.Before(ts) {
				t.Fatalf("Received at wrong time: %v vs %v", tr, ts)
			}
		}
		// Last msg should be timeout.
		lm := msgs[len(msgs)-1].msg
		if key := lm.Header.Get("Status"); key != "408" {
			t.Fatalf("Expected 408 Request Timeout, got %s", key)
		}
	}

	// These should work. Test idle first.
	start, msgs := time.Now(), doReq(1, 50*time.Millisecond, 250*time.Millisecond, 5)
	expectHBs(start, msgs, 5, 50*time.Millisecond)

	// Now test that we do not send heartbeats while we receive traffic.
	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(50 * time.Millisecond)
			js.Publish("T", nil)
		}
	}()

	msgs = doReq(10, 75*time.Millisecond, 350*time.Millisecond, 6)
	// The first 5 should be msgs, no HBs.
	for i := 0; i < 5; i++ {
		if m := msgs[i].msg; len(m.Header) > 0 {
			t.Fatalf("Got a potential heartbeat msg when we should not have: %+v", m.Header)
		}
	}
	// Last should be timeout.
	lm := msgs[len(msgs)-1].msg
	if key := lm.Header.Get("Status"); key != "408" {
		t.Fatalf("Expected 408 Request Timeout, got %s", key)
	}
}

func TestJetStreamConsumerAckSampling(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:         "dlc",
		AckPolicy:       nats.AckExplicitPolicy,
		FilterSubject:   "foo",
		SampleFrequency: "100%",
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "dlc")
	require_NoError(t, err)

	_, err = js.Publish("foo", []byte("Hello"))
	require_NoError(t, err)

	msub, err := nc.SubscribeSync("$JS.EVENT.METRIC.>")
	require_NoError(t, err)

	for _, m := range fetchMsgs(t, sub, 1, time.Second) {
		err = m.AckSync()
		require_NoError(t, err)
	}

	m, err := msub.NextMsg(time.Second)
	require_NoError(t, err)

	var am JSConsumerAckMetric
	err = json.Unmarshal(m.Data, &am)
	require_NoError(t, err)

	if am.Stream != "TEST" || am.Consumer != "dlc" || am.ConsumerSeq != 1 {
		t.Fatalf("Not a proper ack metric: %+v", am)
	}

	// Do less than 100%
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:         "alps",
		AckPolicy:       nats.AckExplicitPolicy,
		FilterSubject:   "foo",
		SampleFrequency: "50%",
	})
	require_NoError(t, err)

	asub, err := js.PullSubscribe("foo", "alps")
	require_NoError(t, err)

	total := 500
	for i := 0; i < total; i++ {
		_, err = js.Publish("foo", []byte("Hello"))
		require_NoError(t, err)
	}

	mp := 0
	for _, m := range fetchMsgs(t, asub, total, time.Second) {
		err = m.AckSync()
		require_NoError(t, err)
		mp++
	}
	nc.Flush()

	if mp != total {
		t.Fatalf("Got only %d msgs out of %d", mp, total)
	}

	nmsgs, _, err := msub.Pending()
	require_NoError(t, err)

	// Should be ~250
	if nmsgs < 200 || nmsgs > 300 {
		t.Fatalf("Expected about 250, got %d", nmsgs)
	}
}

func TestJetStreamConsumerAckSamplingSpecifiedUsingUpdateConsumer(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "dlc",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "foo",
	})
	require_NoError(t, err)

	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:         "dlc",
		AckPolicy:       nats.AckExplicitPolicy,
		FilterSubject:   "foo",
		SampleFrequency: "100%",
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "dlc")
	require_NoError(t, err)

	_, err = js.Publish("foo", []byte("Hello"))
	require_NoError(t, err)

	msub, err := nc.SubscribeSync("$JS.EVENT.METRIC.>")
	require_NoError(t, err)

	for _, m := range fetchMsgs(t, sub, 1, time.Second) {
		err = m.AckSync()
		require_NoError(t, err)
	}

	m, err := msub.NextMsg(time.Second)
	require_NoError(t, err)

	var am JSConsumerAckMetric
	err = json.Unmarshal(m.Data, &am)
	require_NoError(t, err)

	if am.Stream != "TEST" || am.Consumer != "dlc" || am.ConsumerSeq != 1 {
		t.Fatalf("Not a proper ack metric: %+v", am)
	}
}

func TestJetStreamConsumerMaxDeliverUpdate(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	maxDeliver := 2
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "ard",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "foo",
		MaxDeliver:    maxDeliver,
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "ard")
	require_NoError(t, err)

	checkMaxDeliver := func() {
		t.Helper()
		for i := 0; i <= maxDeliver; i++ {
			msgs, err := sub.Fetch(2, nats.MaxWait(100*time.Millisecond))
			if i < maxDeliver {
				require_NoError(t, err)
				require_Len(t, 1, len(msgs))
				_ = msgs[0].Nak()
			} else {
				require_Error(t, err, nats.ErrTimeout)
			}
		}
	}

	_, err = js.Publish("foo", []byte("Hello"))
	require_NoError(t, err)
	checkMaxDeliver()

	// update maxDeliver
	maxDeliver++
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "ard",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "foo",
		MaxDeliver:    maxDeliver,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", []byte("Hello"))
	require_NoError(t, err)
	checkMaxDeliver()
}

func TestJetStreamConsumerStreamUpdate(t *testing.T) {
	test := func(t *testing.T, s *Server, replica int) {
		nc := natsConnect(t, s.ClientURL())
		defer nc.Close()
		js, err := nc.JetStream()
		require_NoError(t, err)
		_, err = js.AddStream(&nats.StreamConfig{Name: "foo", Duplicates: 1 * time.Minute, Replicas: replica})
		defer js.DeleteStream("foo")
		require_NoError(t, err)
		// Update with no change
		_, err = js.UpdateStream(&nats.StreamConfig{Name: "foo", Duplicates: 1 * time.Minute, Replicas: replica})
		require_NoError(t, err)
		// Update with change
		_, err = js.UpdateStream(&nats.StreamConfig{Description: "stream", Name: "foo", Duplicates: 1 * time.Minute, Replicas: replica})
		require_NoError(t, err)
		_, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dur1", AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
		// Update with no change
		_, err = js.UpdateConsumer("foo", &nats.ConsumerConfig{Durable: "dur1", AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
		// Update with change
		_, err = js.UpdateConsumer("foo", &nats.ConsumerConfig{Description: "consumer", Durable: "dur1", AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
	}
	t.Run("clustered", func(t *testing.T) {
		c := createJetStreamClusterWithTemplate(t, `
			listen: 127.0.0.1:-1
			server_name: %s
			jetstream: {
				max_mem_store: 2MB,
				max_file_store: 8MB,
				store_dir: '%s',
			}
			cluster {
				name: %s
				listen: 127.0.0.1:%d
				routes = [%s]
			}
			no_auth_user: u
			accounts {
				ONE {
					users = [ { user: "u", pass: "s3cr3t!" } ]
					jetstream: enabled
				}
				$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
			}`, "clust", 3)
		defer c.shutdown()
		s := c.randomServer()
		t.Run("r3", func(t *testing.T) {
			test(t, s, 3)
		})
		t.Run("r1", func(t *testing.T) {
			test(t, s, 1)
		})
	})
	t.Run("single", func(t *testing.T) {
		storeDir := t.TempDir()
		conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 2MB, max_file_store: 8MB, store_dir: '%s'}`,
			storeDir)))
		s, _ := RunServerWithConfig(conf)
		defer s.Shutdown()
		test(t, s, 1)
	})
}

func TestJetStreamConsumerDeliverNewNotConsumingBeforeRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	inbox := nats.NewInbox()
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverSubject: inbox,
		Durable:        "dur",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverPolicy:  nats.DeliverNewPolicy,
		FilterSubject:  "foo",
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	checkCount := func(expected int) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			ci, err := js.ConsumerInfo("TEST", "dur")
			if err != nil {
				return err
			}
			if n := int(ci.NumPending); n != expected {
				return fmt.Errorf("Expected %v pending, got %v", expected, n)
			}
			return nil
		})
	}
	checkCount(10)

	time.Sleep(300 * time.Millisecond)

	// Check server restart
	nc.Close()
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	checkCount(10)

	// Make sure messages can be consumed
	sub := natsSubSync(t, nc, inbox)
	for i := 0; i < 10; i++ {
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("i=%v next msg error: %v", i, err)
		}
		msg.AckSync()
	}
	checkCount(0)
}

func TestJetStreamConsumerNumPendingWithMaxPerSubjectGreaterThanOne(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	test := func(t *testing.T, st nats.StorageType) {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"KV.*.*"},
			MaxMsgsPerSubject: 2,
			Storage:           st,
		})
		require_NoError(t, err)

		// If we allow more than one msg per subject, consumer's num pending can be off (bug in store level).
		// This requires a filtered state, simple states work ok.
		// Since we now rely on stream's filtered state when asked directly for consumer info in >=2.8.3.
		js.PublishAsync("KV.plans.foo", []byte("OK"))
		js.PublishAsync("KV.plans.bar", []byte("OK"))
		js.PublishAsync("KV.plans.baz", []byte("OK"))
		// These are required, the consumer needs to filter these out to see the bug.
		js.PublishAsync("KV.config.foo", []byte("OK"))
		js.PublishAsync("KV.config.bar", []byte("OK"))
		js.PublishAsync("KV.config.baz", []byte("OK"))

		// Double up some now.
		js.PublishAsync("KV.plans.bar", []byte("OK"))
		js.PublishAsync("KV.plans.baz", []byte("OK"))

		ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
			Durable:       "d",
			AckPolicy:     nats.AckExplicitPolicy,
			DeliverPolicy: nats.DeliverLastPerSubjectPolicy,
			FilterSubject: "KV.plans.*",
		})
		require_NoError(t, err)

		err = js.DeleteStream("TEST")
		require_NoError(t, err)

		if ci.NumPending != 3 {
			t.Fatalf("Expected 3 NumPending, but got %d", ci.NumPending)
		}
	}

	t.Run("MemoryStore", func(t *testing.T) { test(t, nats.MemoryStorage) })
	t.Run("FileStore", func(t *testing.T) { test(t, nats.FileStorage) })
}

func TestJetStreamConsumerAndStreamNamesWithPathSeparators(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "usr/bin"})
	require_Error(t, err, NewJSStreamNameContainsPathSeparatorsError(), nats.ErrInvalidStreamName)
	_, err = js.AddStream(&nats.StreamConfig{Name: `Documents\readme.txt`})
	require_Error(t, err, NewJSStreamNameContainsPathSeparatorsError(), nats.ErrInvalidStreamName)

	// Now consumers.
	_, err = js.AddStream(&nats.StreamConfig{Name: "T"})
	require_NoError(t, err)

	_, err = js.AddConsumer("T", &nats.ConsumerConfig{Durable: "a/b", AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err, NewJSConsumerNameContainsPathSeparatorsError(), nats.ErrInvalidConsumerName)

	_, err = js.AddConsumer("T", &nats.ConsumerConfig{Durable: `a\b`, AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err, NewJSConsumerNameContainsPathSeparatorsError(), nats.ErrInvalidConsumerName)
}

func TestJetStreamConsumerUpdateFilterSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "T", Subjects: []string{"foo", "bar", "baz"}})
	require_NoError(t, err)

	// 10 foo
	for i := 0; i < 10; i++ {
		js.PublishAsync("foo", []byte("OK"))
	}
	// 20 bar
	for i := 0; i < 20; i++ {
		js.PublishAsync("bar", []byte("OK"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	sub, err := js.PullSubscribe("foo", "d")
	require_NoError(t, err)

	// Consume 5 msgs
	msgs, err := sub.Fetch(5)
	require_NoError(t, err)
	require_True(t, len(msgs) == 5)

	// Now update to different filter subject.
	_, err = js.UpdateConsumer("T", &nats.ConsumerConfig{
		Durable:       "d",
		FilterSubject: "bar",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	sub, err = js.PullSubscribe("bar", "d")
	require_NoError(t, err)

	msgs, err = sub.Fetch(1)
	require_NoError(t, err)

	// Make sure meta and pending etc are all correct.
	m := msgs[0]
	meta, err := m.Metadata()
	require_NoError(t, err)

	if meta.Sequence.Consumer != 6 || meta.Sequence.Stream != 11 {
		t.Fatalf("Sequence incorrect %+v", meta.Sequence)
	}
	if meta.NumDelivered != 1 {
		t.Fatalf("Expected NumDelivered to be 1, got %d", meta.NumDelivered)
	}
	if meta.NumPending != 19 {
		t.Fatalf("Expected NumPending to be 19, got %d", meta.NumPending)
	}
}

// Originally pull consumers were FIFO with respect to the request, not delivery of messages.
// We have changed to have the behavior be FIFO but on an individual message basis.
// So after a message is delivered, the request, if still outstanding, effectively
// goes to the end of the queue of requests pending.
func TestJetStreamConsumerPullConsumerFIFO(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "T"})
	require_NoError(t, err)

	// Create pull consumer.
	_, err = js.AddConsumer("T", &nats.ConsumerConfig{Durable: "d", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// Simulate 10 pull requests each asking for 10 messages.
	var subs []*nats.Subscription
	for i := 0; i < 10; i++ {
		inbox := nats.NewInbox()
		sub := natsSubSync(t, nc, inbox)
		subs = append(subs, sub)
		req := &JSApiConsumerGetNextRequest{Batch: 10, Expires: 60 * time.Second}
		jreq, err := json.Marshal(req)
		require_NoError(t, err)
		err = nc.PublishRequest(fmt.Sprintf(JSApiRequestNextT, "T", "d"), inbox, jreq)
		require_NoError(t, err)
	}

	// Not all core publishes could have made it to the leader yet, wait for some time.
	time.Sleep(100 * time.Millisecond)

	// Now send 100 messages.
	for i := 0; i < 100; i++ {
		_, err = js.Publish("T", []byte("FIFO FTW!"))
		require_NoError(t, err)
	}

	// Wait for messages
	for _, sub := range subs {
		checkSubsPending(t, sub, 10)
	}

	// Confirm FIFO
	for index, sub := range subs {
		for i := 0; i < 10; i++ {
			m, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
			meta, err := m.Metadata()
			require_NoError(t, err)
			// We expect these to be FIFO per message. E.g. sub #1 = [1, 11, 21, 31, ..]
			if sseq := meta.Sequence.Stream; sseq != uint64(index+1+(10*i)) {
				t.Fatalf("Expected message #%d for sub #%d to be %d, but got %d", i+1, index+1, index+1+(10*i), sseq)
			}
		}
	}
}

// Make sure that when we reach an ack limit that we follow one shot semantics.
func TestJetStreamConsumerPullConsumerOneShotOnMaxAckLimit(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "T"})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		js.Publish("T", []byte("OK"))
	}

	sub, err := js.PullSubscribe("T", "d", nats.MaxAckPending(5))
	require_NoError(t, err)

	start := time.Now()
	msgs, err := sub.Fetch(10, nats.MaxWait(2*time.Second))
	require_NoError(t, err)

	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("Took too long, not one shot behavior: %v", elapsed)
	}

	if len(msgs) != 5 {
		t.Fatalf("Expected 5 msgs, got %d", len(msgs))
	}
}

func TestJetStreamConsumerDeliverNewMaxRedeliveriesAndServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*"},
	})
	require_NoError(t, err)

	inbox := nats.NewInbox()
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverSubject: inbox,
		Durable:        "dur",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverPolicy:  nats.DeliverNewPolicy,
		MaxDeliver:     3,
		AckWait:        250 * time.Millisecond,
		FilterSubject:  "foo.bar",
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "foo.bar", "msg")

	sub := natsSubSync(t, nc, inbox)
	for i := 0; i < 3; i++ {
		natsNexMsg(t, sub, time.Second)
	}
	// Now check that there is no more redeliveries
	if msg, err := sub.NextMsg(300 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Expected timeout, got msg=%+v err=%v", msg, err)
	}

	// Give a chance to things to be persisted
	time.Sleep(300 * time.Millisecond)

	// Check server restart
	nc.Close()
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, _ = jsClientConnect(t, s)
	defer nc.Close()

	sub = natsSubSync(t, nc, inbox)
	// We should not have messages being redelivered.
	if msg, err := sub.NextMsg(300 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Expected timeout, got msg=%+v err=%v", msg, err)
	}
}

func TestJetStreamConsumerPendingLowerThanStreamFirstSeq(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	inbox := nats.NewInbox()
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverSubject: inbox,
		Durable:        "dur",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverPolicy:  nats.DeliverAllPolicy,
	})
	require_NoError(t, err)

	sub := natsSubSync(t, nc, inbox)
	for i := 0; i < 10; i++ {
		msg := natsNexMsg(t, sub, time.Second)
		require_NoError(t, msg.AckSync())
	}

	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("dur")
	require_True(t, o != nil)
	o.stop()
	mset.store.Compact(1_000_000)
	nc.Close()

	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.FirstSeq == 1_000_000)
	require_True(t, si.State.LastSeq == 999_999)

	acc, err = s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err = acc.lookupStream("TEST")
	require_NoError(t, err)
	o = mset.lookupConsumer("dur")
	require_True(t, o != nil)

	natsSubSync(t, nc, inbox)
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "dur")
		if err != nil {
			return err
		}
		if ci.NumAckPending != 0 {
			return fmt.Errorf("NumAckPending should be 0, got %v", ci.NumAckPending)
		}
		// Delivered stays the same because it reflects what was actually delivered.
		// And must not be influenced by purges/compacts.
		if ci.Delivered.Stream != 10 {
			return fmt.Errorf("Delivered.Stream should be 10, got %v", ci.Delivered.Stream)
		}

		// Starting sequence should be skipped ahead, respecting the compact.
		o.mu.RLock()
		sseq := o.sseq
		o.mu.RUnlock()
		if sseq != 1_000_000 {
			return fmt.Errorf("o.sseq should be 1,000,000, got %v", sseq)
		}
		return nil
	})
}

// Bug when stream's consumer config does not force filestore to track per subject information.
func TestJetStreamConsumerEOFBugNewFileStore(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.bar.*"},
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:   "M",
		Mirror: &nats.StreamSource{Name: "TEST"},
	})
	require_NoError(t, err)

	dsubj := nats.NewInbox()
	sub, err := nc.SubscribeSync(dsubj)
	require_NoError(t, err)
	nc.Flush()

	// Filter needs to be a wildcard. Need to bind to the
	_, err = js.AddConsumer("M", &nats.ConsumerConfig{DeliverSubject: dsubj, FilterSubject: "foo.>"})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		_, err := js.PublishAsync("foo.bar.baz", []byte("OK"))
		require_NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		m.Respond(nil)
	}

	// Now force an expiration.
	mset, err := s.GlobalAccount().lookupStream("M")
	require_NoError(t, err)
	mset.mu.RLock()
	store := mset.store.(*fileStore)
	mset.mu.RUnlock()
	store.mu.RLock()
	mb := store.blks[0]
	store.mu.RUnlock()
	mb.mu.Lock()
	mb.fss = nil
	mb.mu.Unlock()

	// Now send another message.
	_, err = js.PublishAsync("foo.bar.baz", []byte("OK"))
	require_NoError(t, err)

	// This will fail with the bug.
	_, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
}

func TestJetStreamConsumerMultipleSubjectsLast(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	durable := "durable"
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Subjects: []string{"events", "data", "other"},
		Name:     "name",
	})
	if err != nil {
		t.Fatalf("error while creating stream")
	}

	sendStreamMsg(t, nc, "events", "1")
	sendStreamMsg(t, nc, "data", "2")
	sendStreamMsg(t, nc, "other", "3")
	sendStreamMsg(t, nc, "events", "4")
	sendStreamMsg(t, nc, "data", "5")
	sendStreamMsg(t, nc, "data", "6")
	sendStreamMsg(t, nc, "other", "7")
	sendStreamMsg(t, nc, "other", "8")

	// if they're not the same, expect error
	_, err = mset.addConsumer(&ConsumerConfig{
		DeliverPolicy:  DeliverLast,
		AckPolicy:      AckExplicit,
		DeliverSubject: "deliver",
		FilterSubjects: []string{"events", "data"},
		Durable:        durable,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync(_EMPTY_, nats.Bind("name", durable))
	require_NoError(t, err)

	msg, err := sub.NextMsg(time.Millisecond * 500)
	require_NoError(t, err)

	j, err := strconv.Atoi(string(msg.Data))
	require_NoError(t, err)
	expectedStreamSeq := 6
	if j != expectedStreamSeq {
		t.Fatalf("wrong sequence, expected %v got %v", expectedStreamSeq, j)
	}

	require_NoError(t, msg.AckSync())

	// check if we don't get more than we wanted
	msg, err = sub.NextMsg(time.Millisecond * 500)
	if msg != nil || err == nil {
		t.Fatalf("should not get more messages")
	}

	info, err := js.ConsumerInfo("name", durable)
	require_NoError(t, err)

	require_Equal(t, info.NumAckPending, 0)
	require_Equal(t, info.AckFloor.Stream, 6)
	require_Equal(t, info.AckFloor.Consumer, 1)
	require_Equal(t, info.NumPending, 0)
}

func TestJetStreamConsumerMultipleSubjectsLastPerSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	durable := "durable"
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Subjects: []string{"events.*", "data.>", "other"},
		Name:     "name",
	})
	if err != nil {
		t.Fatalf("error while creating stream")
	}

	sendStreamMsg(t, nc, "events.1", "bad")
	sendStreamMsg(t, nc, "events.1", "events.1")

	sendStreamMsg(t, nc, "data.1", "bad")
	sendStreamMsg(t, nc, "data.1", "bad")
	sendStreamMsg(t, nc, "data.1", "bad")
	sendStreamMsg(t, nc, "data.1", "bad")
	sendStreamMsg(t, nc, "data.1", "data.1")

	sendStreamMsg(t, nc, "events.2", "bad")
	sendStreamMsg(t, nc, "events.2", "bad")
	// this is last proper sequence,
	sendStreamMsg(t, nc, "events.2", "events.2")

	sendStreamMsg(t, nc, "other", "bad")
	sendStreamMsg(t, nc, "other", "bad")

	// if they're not the same, expect error
	_, err = mset.addConsumer(&ConsumerConfig{
		DeliverPolicy:  DeliverLastPerSubject,
		AckPolicy:      AckExplicit,
		DeliverSubject: "deliver",
		FilterSubjects: []string{"events.*", "data.>"},
		Durable:        durable,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("", nats.Bind("name", durable))
	require_NoError(t, err)

	checkMessage := func(t *testing.T, subject string, payload string, ack bool) {
		msg, err := sub.NextMsg(time.Millisecond * 500)
		require_NoError(t, err)

		if string(msg.Data) != payload {
			t.Fatalf("expected %v paylaod, got %v", payload, string(msg.Data))
		}
		if subject != msg.Subject {
			t.Fatalf("expected %v subject, got %v", subject, msg.Subject)
		}
		if ack {
			msg.AckSync()
		}
	}

	checkMessage(t, "events.1", "events.1", true)
	checkMessage(t, "data.1", "data.1", true)
	checkMessage(t, "events.2", "events.2", false)

	info, err := js.ConsumerInfo("name", durable)
	require_NoError(t, err)

	require_Equal(t, info.AckFloor.Consumer, 2)
	require_Equal(t, info.AckFloor.Stream, 9)
	require_Equal(t, info.Delivered.Stream, 10)
	require_Equal(t, info.Delivered.Consumer, 3)

	require_NoError(t, err)

}
func TestJetStreamConsumerMultipleSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	durable := "durable"
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{
		Subjects: []string{"events.>", "data.>"},
		Name:     "name",
	})
	require_NoError(t, err)

	for i := 0; i < 20; i += 2 {
		sendStreamMsg(t, nc, "events.created", fmt.Sprintf("created %v", i))
		sendStreamMsg(t, nc, "data.processed", fmt.Sprintf("processed %v", i+1))
	}

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        durable,
		DeliverSubject: "deliver",
		FilterSubjects: []string{"events.created", "data.processed"},
		AckPolicy:      AckExplicit,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("", nats.Bind("name", durable))
	require_NoError(t, err)

	for i := 0; i < 20; i++ {
		msg, err := sub.NextMsg(time.Millisecond * 500)
		require_NoError(t, err)
		require_NoError(t, msg.AckSync())
	}
	info, err := js.ConsumerInfo("name", durable)
	require_NoError(t, err)
	require_True(t, info.NumAckPending == 0)
	require_True(t, info.NumPending == 0)
	require_True(t, info.AckFloor.Consumer == 20)
	require_True(t, info.AckFloor.Stream == 20)

}

func TestJetStreamConsumerMultipleSubjectsWithEmpty(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	durable := "durable"
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Subjects: []string{"events.>"},
		Name:     "name",
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "events.created", fmt.Sprintf("%v", i))
	}

	// if they're not the same, expect error
	_, err = js.AddConsumer("name", &nats.ConsumerConfig{
		DeliverSubject: "deliver",
		FilterSubject:  "",
		Durable:        durable,
		AckPolicy:      nats.AckExplicitPolicy})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("", nats.Bind("name", durable))
	require_NoError(t, err)

	for i := 0; i < 9; i++ {
		msg, err := sub.NextMsg(time.Millisecond * 500)
		require_NoError(t, err)
		j, err := strconv.Atoi(string(msg.Data))
		require_NoError(t, err)
		if j != i {
			t.Fatalf("wrong sequence, expected %v got %v", i, j)
		}
		require_NoError(t, msg.AckSync())
	}

	info, err := js.ConsumerInfo("name", durable)
	require_NoError(t, err)
	require_True(t, info.Delivered.Stream == 10)
	require_True(t, info.Delivered.Consumer == 10)
	require_True(t, info.AckFloor.Stream == 9)
	require_True(t, info.AckFloor.Consumer == 9)
	require_True(t, info.NumAckPending == 1)

	resp := createConsumer(t, nc, "name", ConsumerConfig{
		FilterSubjects: []string{""},
		DeliverSubject: "multiple",
		Durable:        "multiple",
		AckPolicy:      AckExplicit,
	})
	require_True(t, resp.Error.ErrCode == 10139)
}

func TestJetStreamConsumerOverlappingSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	_, err := acc.addStream(&StreamConfig{
		Subjects: []string{"events.>"},
		Name:     "deliver",
	})
	require_NoError(t, err)

	resp := createConsumer(t, nc, "deliver", ConsumerConfig{
		FilterSubjects: []string{"events.one", "events.*"},
		Durable:        "name",
	})

	if resp.Error.ErrCode != 10138 {
		t.Fatalf("this should error as we have overlapping subjects, got %+v", resp.Error)
	}
}

func TestJetStreamConsumerMultipleSubjectsAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Subjects: []string{"events", "data", "other"},
		Name:     "deliver",
	})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		FilterSubjects: []string{"events", "data"},
		Durable:        "name",
		AckPolicy:      AckExplicit,
		Replicas:       1,
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "events", "1")
	sendStreamMsg(t, nc, "data", "2")
	sendStreamMsg(t, nc, "data", "3")
	sendStreamMsg(t, nc, "data", "4")
	sendStreamMsg(t, nc, "events", "5")
	sendStreamMsg(t, nc, "data", "6")
	sendStreamMsg(t, nc, "data", "7")

	consumer, err := js.PullSubscribe("", "name", nats.Bind("deliver", "name"))
	require_NoError(t, err)

	msg, err := consumer.Fetch(3)
	require_NoError(t, err)

	require_Len(t, len(msg), 3)

	require_NoError(t, msg[0].AckSync())
	require_NoError(t, msg[1].AckSync())

	// Due to consumer signaling it might not immediately be reflected.
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		info, err := js.ConsumerInfo("deliver", "name")
		if err != nil {
			return err
		}
		if info.AckFloor.Consumer != 2 {
			return fmt.Errorf("bad consumer sequence. expected %v, got %v", 2, info.AckFloor.Consumer)
		}
		if info.AckFloor.Stream != 2 {
			return fmt.Errorf("bad stream sequence. expected %v, got %v", 2, info.AckFloor.Stream)
		}
		if info.NumPending != 4 {
			return fmt.Errorf("bad num pending. Expected %v, got %v", 4, info.NumPending)
		}
		return nil
	})
}

func TestJetStreamConsumerMultipleSubjectAndNewAPI(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	_, err := acc.addStream(&StreamConfig{
		Subjects: []string{"data", "events"},
		Name:     "deliver",
	})
	if err != nil {
		t.Fatalf("error while creating stream")
	}

	req, err := json.Marshal(&CreateConsumerRequest{Stream: "deliver", Config: ConsumerConfig{
		FilterSubjects: []string{"events", "data"},
		Name:           "name",
		Durable:        "name",
	}})
	require_NoError(t, err)

	resp, err := nc.Request(fmt.Sprintf("$JS.API.CONSUMER.CREATE.%s.%s.%s", "deliver", "name", "data.>"), req, time.Second*10)

	var apiResp JSApiConsumerCreateResponse
	json.Unmarshal(resp.Data, &apiResp)
	require_NoError(t, err)

	if apiResp.Error.ErrCode != 10137 {
		t.Fatal("this should error as multiple subject filters is incompatible with new API and didn't")
	}

}

func TestJetStreamConsumerMultipleSubjectsWithAddedMessages(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	durable := "durable"
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Subjects: []string{"events.>"},
		Name:     "deliver",
	})
	require_NoError(t, err)

	// if they're not the same, expect error
	_, err = mset.addConsumer(&ConsumerConfig{
		DeliverSubject: "deliver",
		FilterSubjects: []string{"events.created", "events.processed"},
		Durable:        durable,
		AckPolicy:      AckExplicit,
	})

	require_NoError(t, err)

	sendStreamMsg(t, nc, "events.created", "0")
	sendStreamMsg(t, nc, "events.created", "1")
	sendStreamMsg(t, nc, "events.created", "2")
	sendStreamMsg(t, nc, "events.created", "3")
	sendStreamMsg(t, nc, "events.other", "BAD")
	sendStreamMsg(t, nc, "events.processed", "4")
	sendStreamMsg(t, nc, "events.processed", "5")
	sendStreamMsg(t, nc, "events.processed", "6")
	sendStreamMsg(t, nc, "events.other", "BAD")
	sendStreamMsg(t, nc, "events.processed", "7")
	sendStreamMsg(t, nc, "events.processed", "8")

	sub, err := js.SubscribeSync("", nats.Bind("deliver", durable))
	if err != nil {
		t.Fatalf("error while subscribing to Consumer: %v", err)
	}

	for i := 0; i < 10; i++ {
		if i == 5 {
			sendStreamMsg(t, nc, "events.created", "9")
		}
		if i == 9 {
			sendStreamMsg(t, nc, "events.other", "BAD")
			sendStreamMsg(t, nc, "events.created", "11")
		}
		if i == 7 {
			sendStreamMsg(t, nc, "events.processed", "10")
		}

		msg, err := sub.NextMsg(time.Second * 1)
		require_NoError(t, err)
		j, err := strconv.Atoi(string(msg.Data))
		require_NoError(t, err)
		if j != i {
			t.Fatalf("wrong sequence, expected %v got %v", i, j)
		}
		if err := msg.AckSync(); err != nil {
			t.Fatalf("error while acking the message :%v", err)
		}

	}

	info, err := js.ConsumerInfo("deliver", durable)
	require_NoError(t, err)

	require_True(t, info.Delivered.Consumer == 12)
	require_True(t, info.Delivered.Stream == 15)
	require_True(t, info.AckFloor.Stream == 12)
	require_True(t, info.AckFloor.Consumer == 10)
}

func TestJetStreamConsumerThreeFilters(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"events", "data", "other", "ignored"},
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "ignored", "100")
	sendStreamMsg(t, nc, "events", "0")
	sendStreamMsg(t, nc, "events", "1")

	sendStreamMsg(t, nc, "data", "2")
	sendStreamMsg(t, nc, "ignored", "100")
	sendStreamMsg(t, nc, "data", "3")

	sendStreamMsg(t, nc, "other", "4")
	sendStreamMsg(t, nc, "data", "5")
	sendStreamMsg(t, nc, "other", "6")
	sendStreamMsg(t, nc, "data", "7")
	sendStreamMsg(t, nc, "ignored", "100")

	mset.addConsumer(&ConsumerConfig{
		FilterSubjects: []string{"events", "data", "other"},
		Durable:        "multi",
		AckPolicy:      AckExplicit,
	})

	consumer, err := js.PullSubscribe("", "multi", nats.Bind("TEST", "multi"))
	require_NoError(t, err)

	msgs, err := consumer.Fetch(6)
	require_NoError(t, err)
	for i, msg := range msgs {
		require_Equal(t, string(msg.Data), fmt.Sprintf("%d", i))
		require_NoError(t, msg.AckSync())
	}

	info, err := js.ConsumerInfo("TEST", "multi")
	require_NoError(t, err)
	require_True(t, info.Delivered.Stream == 8)
	require_True(t, info.Delivered.Consumer == 6)
	require_True(t, info.NumPending == 2)
	require_True(t, info.NumAckPending == 0)
	require_True(t, info.AckFloor.Consumer == 6)
	require_True(t, info.AckFloor.Stream == 8)
}

func TestJetStreamConsumerUpdateFilterSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"events", "data", "other"},
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "other", "100")
	sendStreamMsg(t, nc, "events", "0")
	sendStreamMsg(t, nc, "events", "1")
	sendStreamMsg(t, nc, "data", "2")
	sendStreamMsg(t, nc, "data", "3")
	sendStreamMsg(t, nc, "other", "4")
	sendStreamMsg(t, nc, "data", "5")

	_, err = mset.addConsumer(&ConsumerConfig{
		FilterSubjects: []string{"events", "data"},
		Durable:        "multi",
		AckPolicy:      AckExplicit,
	})
	require_NoError(t, err)

	consumer, err := js.PullSubscribe("", "multi", nats.Bind("TEST", "multi"))
	require_NoError(t, err)

	msgs, err := consumer.Fetch(3)
	require_NoError(t, err)
	for i, msg := range msgs {
		require_Equal(t, string(msg.Data), fmt.Sprintf("%d", i))
		require_NoError(t, msg.AckSync())
	}

	_, err = mset.addConsumer(&ConsumerConfig{
		FilterSubjects: []string{"events", "data", "other"},
		Durable:        "multi",
		AckPolicy:      AckExplicit,
	})
	require_NoError(t, err)

	updatedConsumer, err := js.PullSubscribe("", "multi", nats.Bind("TEST", "multi"))
	require_NoError(t, err)

	msgs, err = updatedConsumer.Fetch(3)
	require_NoError(t, err)
	for i, msg := range msgs {
		require_Equal(t, string(msg.Data), fmt.Sprintf("%d", i+3))
		require_NoError(t, msg.AckSync())
	}
}

func TestJetStreamConsumerAndStreamMetadata(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	metadata := map[string]string{"key": "value", "_nats_created_version": "2.9.11"}
	acc := s.GlobalAccount()

	// Check stream's first.
	mset, err := acc.addStream(&StreamConfig{Name: "foo", Metadata: metadata})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	if cfg := mset.config(); !reflect.DeepEqual(metadata, cfg.Metadata) {
		t.Fatalf("Expected a metadata of %q, got %q", metadata, cfg.Metadata)
	}

	// Now consumer
	o, err := mset.addConsumer(&ConsumerConfig{
		Metadata:       metadata,
		DeliverSubject: "to",
		AckPolicy:      AckNone})
	if err != nil {
		t.Fatalf("Unexpected error adding consumer: %v", err)
	}
	if cfg := o.config(); !reflect.DeepEqual(metadata, cfg.Metadata) {
		t.Fatalf("Expected a metadata of %q, got %q", metadata, cfg.Metadata)
	}

	// Test max.
	data := make([]byte, JSMaxMetadataLen/100)
	crand.Read(data)
	bigValue := base64.StdEncoding.EncodeToString(data)

	bigMetadata := make(map[string]string, 101)
	for i := 0; i < 101; i++ {
		bigMetadata[fmt.Sprintf("key%d", i)] = bigValue
	}

	_, err = acc.addStream(&StreamConfig{Name: "bar", Metadata: bigMetadata})
	if err == nil || !strings.Contains(err.Error(), "stream metadata exceeds") {
		t.Fatalf("Expected an error but got none")
	}

	_, err = mset.addConsumer(&ConsumerConfig{
		Metadata:       bigMetadata,
		DeliverSubject: "to",
		AckPolicy:      AckNone})
	if err == nil || !strings.Contains(err.Error(), "consumer metadata exceeds") {
		t.Fatalf("Expected an error but got none")
	}
}

func TestJetStreamConsumerPurge(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "test.1", "hello")
	sendStreamMsg(t, nc, "test.2", "hello")

	sub, err := js.PullSubscribe("test.>", "consumer")
	require_NoError(t, err)

	// Purge one of the subjects.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Subject: "test.2"})
	require_NoError(t, err)

	info, err := js.ConsumerInfo("TEST", "consumer")
	require_NoError(t, err)
	require_True(t, info.NumPending == 1)

	// Expect to get message from not purged subject.
	_, err = sub.Fetch(1)
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "OTHER",
		Subjects: []string{"other.>"},
	})
	require_NoError(t, err)

	// Publish two items to two subjects.
	sendStreamMsg(t, nc, "other.1", "hello")
	sendStreamMsg(t, nc, "other.2", "hello")

	sub, err = js.PullSubscribe("other.>", "other_consumer")
	require_NoError(t, err)

	// Purge whole stream.
	err = js.PurgeStream("OTHER", &nats.StreamPurgeRequest{})
	require_NoError(t, err)

	info, err = js.ConsumerInfo("OTHER", "other_consumer")
	require_NoError(t, err)
	require_True(t, info.NumPending == 0)

	// This time expect error, as we purged whole stream,
	_, err = sub.Fetch(1)
	require_Error(t, err)

}

func TestJetStreamConsumerFilterUpdate(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>", "bar.>"},
	})
	require_NoError(t, err)

	for i := 0; i < 3; i++ {
		sendStreamMsg(t, nc, "foo.data", "OK")
	}

	sub, err := nc.SubscribeSync("deliver")
	require_NoError(t, err)

	js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "consumer",
		DeliverSubject: "deliver",
		FilterSubject:  "foo.data",
	})

	_, err = sub.NextMsg(time.Second * 1)
	require_NoError(t, err)
	_, err = sub.NextMsg(time.Second * 1)
	require_NoError(t, err)
	_, err = sub.NextMsg(time.Second * 1)
	require_NoError(t, err)

	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "consumer",
		DeliverSubject: "deliver",
		FilterSubject:  "foo.>",
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "foo.other", "data")

	// This will timeout if filters were not properly updated.
	_, err = sub.NextMsg(time.Second * 1)
	require_NoError(t, err)

	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	checkNumFilter := func(expected int) {
		t.Helper()
		mset.mu.RLock()
		nf := mset.numFilter
		mset.mu.RUnlock()
		if nf != expected {
			t.Fatalf("Expected stream's numFilter to be %d, got %d", expected, nf)
		}
	}

	checkNumFilter(1)

	// Update consumer once again, now not having any filters
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "consumer",
		DeliverSubject: "deliver",
		FilterSubject:  _EMPTY_,
	})
	require_NoError(t, err)

	// and expect that numFilter reports correctly.
	checkNumFilter(0)
}

func TestJetStreamConsumerAckFloorWithExpired(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		MaxAge:   2 * time.Second,
	})
	require_NoError(t, err)

	nmsgs := 100
	for i := 0; i < nmsgs; i++ {
		sendStreamMsg(t, nc, "foo", "OK")
	}
	sub, err := js.PullSubscribe("foo", "dlc", nats.AckWait(time.Second))
	require_NoError(t, err)

	// Queue up all for ack pending.
	msgs, err := sub.Fetch(nmsgs)
	require_NoError(t, err)
	require_True(t, len(msgs) == nmsgs)

	// Let all messages expire.
	time.Sleep(3 * time.Second)

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 0)

	ci, err := js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)

	require_True(t, ci.Delivered.Consumer == uint64(nmsgs))
	require_True(t, ci.Delivered.Stream == uint64(nmsgs))
	require_True(t, ci.AckFloor.Consumer == uint64(nmsgs))
	require_True(t, ci.AckFloor.Stream == uint64(nmsgs))
	require_True(t, ci.NumAckPending == 0)
	require_True(t, ci.NumPending == 0)
	require_True(t, ci.NumRedelivered == 0)
}

func TestJetStreamConsumerIsFiltered(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	acc := s.GlobalAccount()

	tests := []struct {
		name           string
		streamSubjects []string
		filters        []string
		isFiltered     bool
	}{
		{
			name:           "single_subject",
			streamSubjects: []string{"one"},
			filters:        []string{"one"},
			isFiltered:     false,
		},
		{
			name:           "single_subject_filtered",
			streamSubjects: []string{"one.>"},
			filters:        []string{"one.filter"},
			isFiltered:     true,
		},
		{
			name:           "multi_subject_non_filtered",
			streamSubjects: []string{"multi", "foo", "bar.>"},
			filters:        []string{"multi", "bar.>", "foo"},
			isFiltered:     false,
		},
		{
			name:           "multi_subject_filtered_wc",
			streamSubjects: []string{"events", "data"},
			filters:        []string{"data"},
			isFiltered:     true,
		},
		{
			name:           "multi_subject_filtered",
			streamSubjects: []string{"machines", "floors"},
			filters:        []string{"machines"},
			isFiltered:     true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mset, err := acc.addStream(&StreamConfig{
				Name:     test.name,
				Subjects: test.streamSubjects,
			})
			require_NoError(t, err)

			o, err := mset.addConsumer(&ConsumerConfig{
				FilterSubjects: test.filters,
				Durable:        test.name,
			})
			require_NoError(t, err)

			require_True(t, o.isFiltered() == test.isFiltered)
		})
	}
}

func TestJetStreamConsumerWithFormattingSymbol(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "Test%123",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "foo", "OK")
	}

	_, err = js.AddConsumer("Test%123", &nats.ConsumerConfig{
		Durable:        "Test%123",
		FilterSubject:  "foo",
		DeliverSubject: "bar",
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("foo", nats.Bind("Test%123", "Test%123"))
	require_NoError(t, err)

	_, err = sub.NextMsg(time.Second * 5)
	require_NoError(t, err)
}

func TestJetStreamConsumerDefaultsFromStream(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()
	if _, err := acc.addStream(&StreamConfig{
		Name:     "test",
		Subjects: []string{"test.*"},
		ConsumerLimits: StreamConsumerLimits{
			MaxAckPending:     15,
			InactiveThreshold: time.Second,
		},
	}); err != nil {
		t.Fatalf("Failed to add stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Failed to connect to JetStream: %v", err)
	}

	t.Run("InheritDefaultsFromStream", func(t *testing.T) {
		ci, err := js.AddConsumer("test", &nats.ConsumerConfig{
			Name:      "InheritDefaultsFromStream",
			AckPolicy: nats.AckExplicitPolicy,
		})
		require_NoError(t, err)

		switch {
		case ci.Config.InactiveThreshold != time.Second:
			t.Fatalf("InactiveThreshold should be 1s, got %s", ci.Config.InactiveThreshold)
		case ci.Config.MaxAckPending != 15:
			t.Fatalf("MaxAckPending should be 15, got %d", ci.Config.MaxAckPending)
		}
	})

	t.Run("CreateConsumerErrorOnExceedMaxAckPending", func(t *testing.T) {
		_, err := js.AddConsumer("test", &nats.ConsumerConfig{
			Name:          "CreateConsumerErrorOnExceedMaxAckPending",
			MaxAckPending: 30,
		})
		switch e := err.(type) {
		case *nats.APIError:
			if ErrorIdentifier(e.ErrorCode) != JSConsumerMaxPendingAckExcessErrF {
				t.Fatalf("invalid error code, got %d, wanted %d", e.ErrorCode, JSConsumerMaxPendingAckExcessErrF)
			}
		default:
			t.Fatalf("should have returned API error, got %T", e)
		}
	})

	t.Run("CreateConsumerErrorOnExceedInactiveThreshold", func(t *testing.T) {
		_, err := js.AddConsumer("test", &nats.ConsumerConfig{
			Name:              "CreateConsumerErrorOnExceedInactiveThreshold",
			InactiveThreshold: time.Second * 2,
		})
		switch e := err.(type) {
		case *nats.APIError:
			if ErrorIdentifier(e.ErrorCode) != JSConsumerInactiveThresholdExcess {
				t.Fatalf("invalid error code, got %d, wanted %d", e.ErrorCode, JSConsumerInactiveThresholdExcess)
			}
		default:
			t.Fatalf("should have returned API error, got %T", e)
		}
	})

	t.Run("UpdateStreamErrorOnViolateConsumerMaxAckPending", func(t *testing.T) {
		_, err := js.AddConsumer("test", &nats.ConsumerConfig{
			Name:          "UpdateStreamErrorOnViolateConsumerMaxAckPending",
			MaxAckPending: 15,
		})
		require_NoError(t, err)

		stream, err := acc.lookupStream("test")
		require_NoError(t, err)

		err = stream.update(&StreamConfig{
			Name:     "test",
			Subjects: []string{"test.*"},
			ConsumerLimits: StreamConsumerLimits{
				MaxAckPending: 10,
			},
		})
		if err == nil {
			t.Fatalf("stream update should have errored but didn't")
		}
	})

	t.Run("UpdateStreamErrorOnViolateConsumerInactiveThreshold", func(t *testing.T) {
		_, err := js.AddConsumer("test", &nats.ConsumerConfig{
			Name:              "UpdateStreamErrorOnViolateConsumerInactiveThreshold",
			InactiveThreshold: time.Second,
		})
		require_NoError(t, err)

		stream, err := acc.lookupStream("test")
		require_NoError(t, err)

		err = stream.update(&StreamConfig{
			Name:     "test",
			Subjects: []string{"test.*"},
			ConsumerLimits: StreamConsumerLimits{
				InactiveThreshold: time.Second / 2,
			},
		})
		if err == nil {
			t.Fatalf("stream update should have errored but didn't")
		}
	})
}

// Server issue 4685
func TestJetStreamConsumerPendingForKV(t *testing.T) {
	for _, st := range []nats.StorageType{nats.FileStorage, nats.MemoryStorage} {
		t.Run(st.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:              "TEST",
				Subjects:          []string{"test.>"},
				Storage:           st,
				MaxMsgsPerSubject: 10,
				Discard:           nats.DiscardNew,
			})
			require_NoError(t, err)

			_, err = js.Publish("test.1", []byte("x"))
			require_NoError(t, err)

			var msg *nats.Msg

			// this is the detail that triggers the off by one, remove this code and all tests pass
			msg = nats.NewMsg("test.1")
			msg.Data = []byte("y")
			msg.Header.Add(nats.ExpectedLastSeqHdr, "1")
			_, err = js.PublishMsg(msg)
			require_NoError(t, err)

			_, err = js.Publish("test.2", []byte("x"))
			require_NoError(t, err)
			_, err = js.Publish("test.3", []byte("x"))
			require_NoError(t, err)
			_, err = js.Publish("test.4", []byte("x"))
			require_NoError(t, err)
			_, err = js.Publish("test.5", []byte("x"))
			require_NoError(t, err)

			nfo, err := js.StreamInfo("TEST", &nats.StreamInfoRequest{SubjectsFilter: ">"})
			require_NoError(t, err)

			require_Equal(t, len(nfo.State.Subjects), 5)

			sub, err := js.SubscribeSync("test.>", nats.DeliverLastPerSubject())
			require_NoError(t, err)

			msg, err = sub.NextMsg(time.Second)
			require_NoError(t, err)
			meta, err := msg.Metadata()
			require_NoError(t, err)
			require_Equal(t, meta.NumPending, 4)
		})
	}
}

func TestJetStreamConsumerNakThenAckFloorMove(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	for i := 0; i < 11; i++ {
		js.Publish("foo", []byte("OK"))
	}

	sub, err := js.PullSubscribe("foo", "dlc", nats.AckWait(100*time.Millisecond))
	require_NoError(t, err)

	msgs, err := sub.Fetch(11)
	require_NoError(t, err)

	// Nak first one
	msgs[0].Nak()

	// Ack 2-10
	for i := 1; i < 10; i++ {
		msgs[i].AckSync()
	}
	// Hold onto last.
	lastMsg := msgs[10]

	ci, err := sub.ConsumerInfo()
	require_NoError(t, err)

	require_Equal(t, ci.AckFloor.Consumer, 0)
	require_Equal(t, ci.AckFloor.Stream, 0)
	require_Equal(t, ci.NumAckPending, 2)

	// Grab first messsage again and ack this time.
	msgs, err = sub.Fetch(1)
	require_NoError(t, err)
	msgs[0].AckSync()

	ci, err = sub.ConsumerInfo()
	require_NoError(t, err)

	require_Equal(t, ci.Delivered.Consumer, 12)
	require_Equal(t, ci.Delivered.Stream, 11)
	require_Equal(t, ci.AckFloor.Consumer, 10)
	require_Equal(t, ci.AckFloor.Stream, 10)
	require_Equal(t, ci.NumAckPending, 1)

	// Make sure when we ack last one we collapse the AckFloor.Consumer
	// with the higher delivered due to re-deliveries.
	lastMsg.AckSync()
	ci, err = sub.ConsumerInfo()
	require_NoError(t, err)

	require_Equal(t, ci.Delivered.Consumer, 12)
	require_Equal(t, ci.Delivered.Stream, 11)
	require_Equal(t, ci.AckFloor.Consumer, 12)
	require_Equal(t, ci.AckFloor.Stream, 11)
	require_Equal(t, ci.NumAckPending, 0)
}

func TestJetStreamConsumerPauseViaConfig(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	t.Run("CreateShouldSucceed", func(t *testing.T) {
		deadline := time.Now().Add(time.Hour)
		ci := jsTestPause_CreateOrUpdateConsumer(t, nc, ActionCreate, "TEST", ConsumerConfig{
			Name:       "my_consumer_1",
			PauseUntil: &deadline,
		})
		require_True(t, ci != nil)
		require_True(t, ci.Config != nil)
		require_True(t, ci.Config.PauseUntil != nil)
		require_True(t, ci.Config.PauseUntil.Equal(deadline))
	})

	t.Run("UpdateShouldFail", func(t *testing.T) {
		deadline := time.Now().Add(time.Hour)
		ci := jsTestPause_CreateOrUpdateConsumer(t, nc, ActionCreate, "TEST", ConsumerConfig{
			Name: "my_consumer_2",
		})
		require_True(t, ci != nil)
		require_True(t, ci.Config != nil)
		require_True(t, ci.Config.PauseUntil == nil || ci.Config.PauseUntil.IsZero())

		var cc ConsumerConfig
		j, err := json.Marshal(ci.Config)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(j, &cc))

		pauseUntil := time.Now().Add(time.Hour)
		cc.PauseUntil = &pauseUntil
		ci2 := jsTestPause_CreateOrUpdateConsumer(t, nc, ActionUpdate, "TEST", cc)
		require_False(t, ci2.Config.PauseUntil != nil && ci2.Config.PauseUntil.Equal(deadline))
		require_True(t, ci2.Config.PauseUntil == nil || ci2.Config.PauseUntil.Equal(time.Time{}))
	})
}

func TestJetStreamConsumerPauseViaEndpoint(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"push", "pull"},
	})
	require_NoError(t, err)

	t.Run("PullConsumer", func(t *testing.T) {
		_, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
			Name: "pull_consumer",
		})
		require_NoError(t, err)

		sub, err := js.PullSubscribe("pull", "", nats.Bind("TEST", "pull_consumer"))
		require_NoError(t, err)

		// This should succeed as there's no pause, so it definitely
		// shouldn't take more than a second.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("pull", []byte("OK"))
			require_NoError(t, err)
		}
		msgs, err := sub.Fetch(10, nats.MaxWait(time.Second))
		require_NoError(t, err)
		require_Equal(t, len(msgs), 10)

		// Now we'll pause the consumer for 3 seconds.
		deadline := time.Now().Add(time.Second * 3)
		require_True(t, jsTestPause_PauseConsumer(t, nc, "TEST", "pull_consumer", deadline).Equal(deadline))

		// This should fail as we'll wait for only half of the deadline.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("pull", []byte("OK"))
			require_NoError(t, err)
		}
		_, err = sub.Fetch(10, nats.MaxWait(time.Until(deadline)/2))
		require_Error(t, err, nats.ErrTimeout)

		// This should succeed after a short wait, and when we're done,
		// we should be after the deadline.
		msgs, err = sub.Fetch(10)
		require_NoError(t, err)
		require_Equal(t, len(msgs), 10)
		require_True(t, time.Now().After(deadline))

		// This should succeed as there's no pause, so it definitely
		// shouldn't take more than a second.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("pull", []byte("OK"))
			require_NoError(t, err)
		}
		msgs, err = sub.Fetch(10, nats.MaxWait(time.Second))
		require_NoError(t, err)
		require_Equal(t, len(msgs), 10)

		require_True(t, jsTestPause_PauseConsumer(t, nc, "TEST", "pull_consumer", time.Time{}).Equal(time.Time{}))

		// This should succeed as there's no pause, so it definitely
		// shouldn't take more than a second.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("pull", []byte("OK"))
			require_NoError(t, err)
		}
		msgs, err = sub.Fetch(10, nats.MaxWait(time.Second))
		require_NoError(t, err)
		require_Equal(t, len(msgs), 10)
	})

	t.Run("PushConsumer", func(t *testing.T) {
		ch := make(chan *nats.Msg, 100)
		_, err = js.ChanSubscribe("push", ch, nats.BindStream("TEST"), nats.ConsumerName("push_consumer"))
		require_NoError(t, err)

		// This should succeed as there's no pause, so it definitely
		// shouldn't take more than a second.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("push", []byte("OK"))
			require_NoError(t, err)
		}
		for i := 0; i < 10; i++ {
			msg := require_ChanRead(t, ch, time.Second)
			require_NotEqual(t, msg, nil)
		}

		// Now we'll pause the consumer for 3 seconds.
		deadline := time.Now().Add(time.Second * 3)
		require_True(t, jsTestPause_PauseConsumer(t, nc, "TEST", "push_consumer", deadline).Equal(deadline))

		// This should succeed after a short wait, and when we're done,
		// we should be after the deadline.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("push", []byte("OK"))
			require_NoError(t, err)
		}
		for i := 0; i < 10; i++ {
			msg := require_ChanRead(t, ch, time.Second*5)
			require_NotEqual(t, msg, nil)
			require_True(t, time.Now().After(deadline))
		}

		// This should succeed as there's no pause, so it definitely
		// shouldn't take more than a second.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("push", []byte("OK"))
			require_NoError(t, err)
		}
		for i := 0; i < 10; i++ {
			msg := require_ChanRead(t, ch, time.Second)
			require_NotEqual(t, msg, nil)
		}

		require_True(t, jsTestPause_PauseConsumer(t, nc, "TEST", "push_consumer", time.Time{}).Equal(time.Time{}))

		// This should succeed as there's no pause, so it definitely
		// shouldn't take more than a second.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("push", []byte("OK"))
			require_NoError(t, err)
		}
		for i := 0; i < 10; i++ {
			msg := require_ChanRead(t, ch, time.Second)
			require_NotEqual(t, msg, nil)
		}
	})
}

func TestJetStreamConsumerPauseResumeViaEndpoint(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"TEST"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name: "CONSUMER",
	})
	require_NoError(t, err)

	getConsumerInfo := func() ConsumerInfo {
		var ci ConsumerInfo
		infoResp, err := nc.Request("$JS.API.CONSUMER.INFO.TEST.CONSUMER", nil, time.Second)
		require_NoError(t, err)
		err = json.Unmarshal(infoResp.Data, &ci)
		require_NoError(t, err)
		return ci
	}

	// Ensure we are not paused
	require_False(t, getConsumerInfo().Paused)

	// Now we'll pause the consumer for 30 seconds.
	deadline := time.Now().Add(time.Second * 30)
	require_True(t, jsTestPause_PauseConsumer(t, nc, "TEST", "CONSUMER", deadline).Equal(deadline))

	// Ensure the consumer reflects being paused
	require_True(t, getConsumerInfo().Paused)

	subj := fmt.Sprintf("$JS.API.CONSUMER.PAUSE.%s.%s", "TEST", "CONSUMER")
	_, err = nc.Request(subj, nil, time.Second)
	require_NoError(t, err)

	// Ensure the consumer reflects being resumed
	require_False(t, getConsumerInfo().Paused)
}

func TestJetStreamConsumerPauseHeartbeats(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	deadline := time.Now().Add(time.Hour)
	dsubj := "deliver_subj"

	ci := jsTestPause_CreateOrUpdateConsumer(t, nc, ActionCreate, "TEST", ConsumerConfig{
		Name:           "my_consumer",
		PauseUntil:     &deadline,
		Heartbeat:      time.Millisecond * 100,
		DeliverSubject: dsubj,
	})
	require_True(t, ci.Config.PauseUntil.Equal(deadline))

	ch := make(chan *nats.Msg, 10)
	_, err = nc.ChanSubscribe(dsubj, ch)
	require_NoError(t, err)

	for i := 0; i < 20; i++ {
		msg := require_ChanRead(t, ch, time.Millisecond*200)
		require_Equal(t, msg.Header.Get("Status"), "100")
		require_Equal(t, msg.Header.Get("Description"), "Idle Heartbeat")
	}
}

func TestJetStreamConsumerPauseAdvisories(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	checkAdvisory := func(msg *nats.Msg, shouldBePaused bool, deadline time.Time) {
		t.Helper()
		var advisory JSConsumerPauseAdvisory
		require_NoError(t, json.Unmarshal(msg.Data, &advisory))
		require_Equal(t, advisory.Stream, "TEST")
		require_Equal(t, advisory.Consumer, "my_consumer")
		require_Equal(t, advisory.Paused, shouldBePaused)
		require_True(t, advisory.PauseUntil.Equal(deadline))
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	ch := make(chan *nats.Msg, 10)
	_, err = nc.ChanSubscribe(JSAdvisoryConsumerPausePre+".TEST.my_consumer", ch)
	require_NoError(t, err)

	deadline := time.Now().Add(time.Second)
	jsTestPause_CreateOrUpdateConsumer(t, nc, ActionCreate, "TEST", ConsumerConfig{
		Name:       "my_consumer",
		PauseUntil: &deadline,
	})

	// First advisory should tell us that the consumer was paused
	// on creation.
	msg := require_ChanRead(t, ch, time.Second*2)
	checkAdvisory(msg, true, deadline)

	// The second one for the unpause.
	msg = require_ChanRead(t, ch, time.Second*2)
	checkAdvisory(msg, false, deadline)

	// Now we'll pause the consumer using the API.
	deadline = time.Now().Add(time.Second)
	require_True(t, jsTestPause_PauseConsumer(t, nc, "TEST", "my_consumer", deadline).Equal(deadline))

	// Third advisory should tell us about the pause via the API.
	msg = require_ChanRead(t, ch, time.Second*2)
	checkAdvisory(msg, true, deadline)

	// Finally that should unpause.
	msg = require_ChanRead(t, ch, time.Second*2)
	checkAdvisory(msg, false, deadline)
}

func TestJetStreamConsumerSurvivesRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	deadline := time.Now().Add(time.Hour)
	jsTestPause_CreateOrUpdateConsumer(t, nc, ActionCreate, "TEST", ConsumerConfig{
		Name:       "my_consumer",
		PauseUntil: &deadline,
	})

	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	stream, err := s.gacc.lookupStream("TEST")
	require_NoError(t, err)

	consumer := stream.lookupConsumer("my_consumer")
	require_NotEqual(t, consumer, nil)

	consumer.mu.RLock()
	timer := consumer.uptmr
	consumer.mu.RUnlock()
	require_True(t, timer != nil)
}

func TestJetStreamConsumerInfoNumPending(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "LIMITS",
		Subjects: []string{"js.in.limits"},
		MaxMsgs:  100,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("LIMITS", &nats.ConsumerConfig{
		Name:      "PULL",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 1000; i++ {
		js.Publish("js.in.limits", []byte("x"))
	}

	ci, err := js.ConsumerInfo("LIMITS", "PULL")
	require_NoError(t, err)
	require_Equal(t, ci.NumPending, 100)

	// Now restart the server.
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	ci, err = js.ConsumerInfo("LIMITS", "PULL")
	require_NoError(t, err)
	require_Equal(t, ci.NumPending, 100)
}

func TestJetStreamConsumerDontDecrementPendingCountOnSkippedMsg(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)

	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")

	requireExpected := func(expected int64) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			o.mu.RLock()
			npc := o.npc
			o.mu.RUnlock()
			if npc != expected {
				return fmt.Errorf("expected npc=%d, got %d", expected, npc)
			}
			return nil
		})
	}

	// Should initially report no messages available.
	requireExpected(0)

	// A new message is available, should report in pending.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	requireExpected(1)

	// Pending count should decrease when the message is deleted.
	err = js.DeleteMsg("TEST", 1)
	require_NoError(t, err)
	requireExpected(0)

	// Make more messages available, should report in pending.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	requireExpected(2)

	// Simulate getNextMsg being called and the starting sequence to skip over a deleted message.
	// Also simulate one pending message.
	o.mu.Lock()
	o.sseq = 100
	o.npc--
	o.pending = make(map[uint64]*Pending)
	o.pending[2] = &Pending{}
	o.mu.Unlock()

	// Since this message is pending we should not decrement pending count as we've done so already.
	o.decStreamPending(2, "foo")
	requireExpected(1)

	// This is the deleted message that was skipped, we've hit the race condition and are not able to
	// fix it at this point. If we decrement then we could have decremented it twice if the message
	// was removed as a result of an Ack with Interest or WorkQueue retention, instead of due to contention.
	o.decStreamPending(3, "foo")
	requireExpected(1)
}

func TestJetStreamConsumerPendingCountAfterMsgAckAboveFloor(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 2; i++ {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	sub, err := js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)

	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")

	requireExpected := func(expected int64) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			o.mu.RLock()
			npc := o.npc
			o.mu.RUnlock()
			if npc != expected {
				return fmt.Errorf("expected npc=%d, got %d", expected, npc)
			}
			return nil
		})
	}

	// Expect 2 messages pending.
	requireExpected(2)

	// Fetch 2 messages and ack the last.
	msgs, err := sub.Fetch(2)
	require_NoError(t, err)
	require_Len(t, len(msgs), 2)
	msg := msgs[1]
	err = msg.AckSync()
	require_NoError(t, err)

	// We've fetched 2 message so should report 0 pending.
	requireExpected(0)
}

func TestJetStreamConsumerPullRemoveInterest(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "MYS-PULL"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	wcfg := &ConsumerConfig{Durable: "worker", AckPolicy: AckExplicit}
	o, err := mset.addConsumer(wcfg)
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	rqn := o.requestNextMsgSubject()
	defer o.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Ask for a message even though one is not there. This will queue us up for waiting.
	if _, err := nc.Request(rqn, nil, 10*time.Millisecond); err == nil {
		t.Fatalf("Expected an error, got none")
	}

	// This is using new style request mechanism. so drop the connection itself to get rid of interest.
	nc.Close()

	// Wait for client cleanup
	checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
		if n := s.NumClients(); err != nil || n != 0 {
			return fmt.Errorf("Still have %d clients", n)
		}
		return nil
	})

	nc = clientConnectToServer(t, s)
	defer nc.Close()

	// Send a message
	sendStreamMsg(t, nc, mname, "Hello World!")

	msg, err := nc.Request(rqn, nil, time.Second)
	require_NoError(t, err)
	_, dseq, dc, _, _ := replyInfo(msg.Reply)
	if dseq != 1 {
		t.Fatalf("Expected consumer sequence of 1, got %d", dseq)
	}
	if dc != 1 {
		t.Fatalf("Expected delivery count of 1, got %d", dc)
	}

	// Now do old school request style and more than one waiting.
	nc = clientConnectWithOldRequest(t, s)
	defer nc.Close()

	// Now queue up 10 waiting via failed requests.
	for i := 0; i < 10; i++ {
		if _, err := nc.Request(rqn, nil, 1*time.Millisecond); err == nil {
			t.Fatalf("Expected an error, got none")
		}
	}

	// Send a second message
	sendStreamMsg(t, nc, mname, "Hello World!")

	msg, err = nc.Request(rqn, nil, time.Second)
	require_NoError(t, err)
	_, dseq, dc, _, _ = replyInfo(msg.Reply)
	if dseq != 2 {
		t.Fatalf("Expected consumer sequence of 2, got %d", dseq)
	}
	if dc != 1 {
		t.Fatalf("Expected delivery count of 1, got %d", dc)
	}
}

func TestJetStreamConsumerPullMaxWaitingOfOne(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"TEST.A"}})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:    "dur",
		MaxWaiting: 1,
		AckPolicy:  nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// First check that a request can timeout (we had an issue where this was
	// not the case for MaxWaiting of 1).
	req := JSApiConsumerGetNextRequest{Batch: 1, Expires: 250 * time.Millisecond}
	reqb, _ := json.Marshal(req)
	msg, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.dur", reqb, 13000*time.Millisecond)
	require_NoError(t, err)
	if v := msg.Header.Get("Status"); v != "408" {
		t.Fatalf("Expected 408, got: %s", v)
	}

	// Now have a request waiting...
	req = JSApiConsumerGetNextRequest{Batch: 1}
	reqb, _ = json.Marshal(req)
	// Send the request, but do not block since we want then to send an extra
	// request that should be rejected.
	sub := natsSubSync(t, nc, nats.NewInbox())
	err = nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.dur", sub.Subject, reqb)
	require_NoError(t, err)

	// Send a new request, this should be rejected as a 409.
	req = JSApiConsumerGetNextRequest{Batch: 1, Expires: 250 * time.Millisecond}
	reqb, _ = json.Marshal(req)
	msg, err = nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.dur", reqb, 300*time.Millisecond)
	require_NoError(t, err)
	if v := msg.Header.Get("Status"); v != "409" {
		t.Fatalf("Expected 409, got: %s", v)
	}
	if v := msg.Header.Get("Description"); v != "Exceeded MaxWaiting" {
		t.Fatalf("Expected error about exceeded max waiting, got: %s", v)
	}
}

func TestJetStreamConsumerPullMaxWaitingOfOneWithHeartbeatInterval(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"TEST.A"}})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:    "dur",
		MaxWaiting: 1,
		AckPolicy:  nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// First check that a request can timeout (we had an issue where this was
	// not the case for MaxWaiting of 1).
	req := JSApiConsumerGetNextRequest{Batch: 1, Expires: 250 * time.Millisecond}
	reqb, _ := json.Marshal(req)
	msg, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.dur", reqb, 13000*time.Millisecond)
	require_NoError(t, err)
	if v := msg.Header.Get("Status"); v != "408" {
		t.Fatalf("Expected 408, got: %s", v)
	}

	// Now have a request waiting...
	req = JSApiConsumerGetNextRequest{Batch: 1}
	reqb, _ = json.Marshal(req)
	// Send the request, but do not block since we want then to send an extra
	// request that should be rejected.
	sub := natsSubSync(t, nc, nats.NewInbox())
	err = nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.dur", sub.Subject, reqb)
	require_NoError(t, err)

	// Send a new request, this should not respond since we specified an idle heartbeat,
	// therefore the client will just expect to miss those instead.
	req = JSApiConsumerGetNextRequest{Batch: 1, Expires: 500 * time.Millisecond, Heartbeat: 250 * time.Millisecond}
	reqb, _ = json.Marshal(req)
	_, err = nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.dur", reqb, 300*time.Millisecond)
	require_Error(t, err)
}

func TestJetStreamConsumerPullMaxWaiting(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"test.*"}})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:    "dur",
		AckPolicy:  nats.AckExplicitPolicy,
		MaxWaiting: 10,
	})
	require_NoError(t, err)

	// Cannot be updated.
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:    "dur",
		AckPolicy:  nats.AckExplicitPolicy,
		MaxWaiting: 1,
	})
	if !strings.Contains(err.Error(), "can not be updated") {
		t.Fatalf(`expected "cannot be updated" error, got %s`, err)
	}
}

func TestJetStreamConsumerPullRequestCleanup(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "T", Storage: nats.MemoryStorage})
	require_NoError(t, err)

	_, err = js.AddConsumer("T", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	req := &JSApiConsumerGetNextRequest{Batch: 10, Expires: 100 * time.Millisecond}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)

	// Need interest otherwise the requests will be recycled based on that.
	_, err = nc.SubscribeSync("xx")
	require_NoError(t, err)

	// Queue up 100 requests.
	rsubj := fmt.Sprintf(JSApiRequestNextT, "T", "dlc")
	for i := 0; i < 100; i++ {
		err = nc.PublishRequest(rsubj, "xx", jreq)
		require_NoError(t, err)
	}
	// Wait to expire
	time.Sleep(200 * time.Millisecond)

	ci, err := js.ConsumerInfo("T", "dlc")
	require_NoError(t, err)

	if ci.NumWaiting != 0 {
		t.Fatalf("Expected to see no waiting requests, got %d", ci.NumWaiting)
	}
}

func TestJetStreamConsumerPullRequestMaximums(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	// Need to do this via server for now.
	acc := s.GlobalAccount()
	mset, err := acc.addStream(&StreamConfig{Name: "TEST", Storage: MemoryStorage})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:            "dlc",
		MaxRequestBatch:    10,
		MaxRequestMaxBytes: 10_000,
		MaxRequestExpires:  time.Second,
		AckPolicy:          AckExplicit,
	})
	require_NoError(t, err)

	genReq := func(b, mb int, e time.Duration) []byte {
		req := &JSApiConsumerGetNextRequest{Batch: b, Expires: e, MaxBytes: mb}
		jreq, err := json.Marshal(req)
		require_NoError(t, err)
		return jreq
	}

	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", "dlc")

	// Exceeds max batch size.
	resp, err := nc.Request(rsubj, genReq(11, 0, 100*time.Millisecond), time.Second)
	require_NoError(t, err)
	if status := resp.Header.Get("Status"); status != "409" {
		t.Fatalf("Expected a 409 status code, got %q", status)
	}

	// Exceeds max expires.
	resp, err = nc.Request(rsubj, genReq(1, 0, 10*time.Minute), time.Second)
	require_NoError(t, err)
	if status := resp.Header.Get("Status"); status != "409" {
		t.Fatalf("Expected a 409 status code, got %q", status)
	}

	// Exceeds max bytes.
	resp, err = nc.Request(rsubj, genReq(10, 10_000*2, 10*time.Minute), time.Second)
	require_NoError(t, err)
	if status := resp.Header.Get("Status"); status != "409" {
		t.Fatalf("Expected a 409 status code, got %q", status)
	}
}

func TestJetStreamConsumerPullCrossAccountExpires(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 4GB, max_file_store: 1TB, store_dir: %q}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [ { service: "$JS.API.CONSUMER.MSG.NEXT.>", response: stream } ]
			},
			IU: {
				users: [ {user: mh, password: bar} ]
				imports [ { service: { subject: "$JS.API.CONSUMER.MSG.NEXT.*.*", account: JS } }]
				# Re-export for dasiy chain test.
				exports [ { service: "$JS.API.CONSUMER.MSG.NEXT.>", response: stream } ]
			},
			IU2: {
				users: [ {user: ik, password: bar} ]
				imports [ { service: { subject: "$JS.API.CONSUMER.MSG.NEXT.*.*", account: IU } } ]
			},
		}
	`, t.TempDir())))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Connect to JS account and create stream, put some messages into it.
	nc, js := jsClientConnect(t, s, nats.UserInfo("dlc", "foo"))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "PC", Subjects: []string{"foo"}})
	require_NoError(t, err)

	toSend := 50
	for i := 0; i < toSend; i++ {
		_, err := js.Publish("foo", []byte("OK"))
		require_NoError(t, err)
	}

	// Now create pull consumer.
	_, err = js.AddConsumer("PC", &nats.ConsumerConfig{Durable: "PC", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// Now access from the importing account.
	nc2, _ := jsClientConnect(t, s, nats.UserInfo("mh", "bar"))
	defer nc2.Close()

	// Make sure batch request works properly with stream response.
	req := &JSApiConsumerGetNextRequest{Batch: 10}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)
	rsubj := fmt.Sprintf(JSApiRequestNextT, "PC", "PC")
	// Make sure we can get a batch correctly etc.
	// This requires response stream above in the export definition.
	sub, err := nc2.SubscribeSync("xx")
	require_NoError(t, err)
	err = nc2.PublishRequest(rsubj, "xx", jreq)
	require_NoError(t, err)
	checkSubsPending(t, sub, 10)

	// Now let's queue up a bunch of requests and then delete interest to make sure the system
	// removes those requests.

	// Purge stream
	err = js.PurgeStream("PC")
	require_NoError(t, err)

	// Queue up 10 requests
	for i := 0; i < 10; i++ {
		err = nc2.PublishRequest(rsubj, "xx", jreq)
		require_NoError(t, err)
	}
	// Since using different connection, flush to make sure processed.
	nc2.Flush()

	ci, err := js.ConsumerInfo("PC", "PC")
	require_NoError(t, err)
	if ci.NumWaiting != 10 {
		t.Fatalf("Expected to see 10 waiting requests, got %d", ci.NumWaiting)
	}

	// Now remove interest and make sure requests are removed.
	sub.Unsubscribe()
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("PC", "PC")
		require_NoError(t, err)
		if ci.NumWaiting != 0 {
			return fmt.Errorf("Requests still present")
		}
		return nil
	})

	// Now let's test that ephemerals will go away as well when interest etc is no longer around.
	ci, err = js.AddConsumer("PC", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// Set the inactivity threshold by hand for now.
	jsacc, err := s.LookupAccount("JS")
	require_NoError(t, err)
	mset, err := jsacc.lookupStream("PC")
	require_NoError(t, err)
	o := mset.lookupConsumer(ci.Name)
	if o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	err = o.setInActiveDeleteThreshold(50 * time.Millisecond)
	require_NoError(t, err)

	rsubj = fmt.Sprintf(JSApiRequestNextT, "PC", ci.Name)
	sub, err = nc2.SubscribeSync("zz")
	require_NoError(t, err)
	err = nc2.PublishRequest(rsubj, "zz", jreq)
	require_NoError(t, err)

	// Wait past inactive threshold.
	time.Sleep(100 * time.Millisecond)
	// Make sure it is still there..
	ci, err = js.ConsumerInfo("PC", ci.Name)
	require_NoError(t, err)
	if ci.NumWaiting != 1 {
		t.Fatalf("Expected to see 1 waiting request, got %d", ci.NumWaiting)
	}

	// Now release interest.
	sub.Unsubscribe()
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		_, err := js.ConsumerInfo("PC", ci.Name)
		if err == nil {
			return fmt.Errorf("Consumer still present")
		}
		return nil
	})

	// Now test daisy chained.
	toSend = 10
	for i := 0; i < toSend; i++ {
		_, err := js.Publish("foo", []byte("OK"))
		require_NoError(t, err)
	}

	ci, err = js.AddConsumer("PC", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// Set the inactivity threshold by hand for now.
	o = mset.lookupConsumer(ci.Name)
	if o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	// Make this one longer so we test request purge and ephemerals in same test.
	err = o.setInActiveDeleteThreshold(500 * time.Millisecond)
	require_NoError(t, err)

	// Now access from the importing account.
	nc3, _ := jsClientConnect(t, s, nats.UserInfo("ik", "bar"))
	defer nc3.Close()

	sub, err = nc3.SubscribeSync("yy")
	require_NoError(t, err)

	rsubj = fmt.Sprintf(JSApiRequestNextT, "PC", ci.Name)
	err = nc3.PublishRequest(rsubj, "yy", jreq)
	require_NoError(t, err)
	checkSubsPending(t, sub, 10)

	// Purge stream
	err = js.PurgeStream("PC")
	require_NoError(t, err)

	// Queue up 10 requests
	for i := 0; i < 10; i++ {
		err = nc3.PublishRequest(rsubj, "yy", jreq)
		require_NoError(t, err)
	}
	// Since using different connection, flush to make sure processed.
	nc3.Flush()

	ci, err = js.ConsumerInfo("PC", ci.Name)
	require_NoError(t, err)
	if ci.NumWaiting != 10 {
		t.Fatalf("Expected to see 10 waiting requests, got %d", ci.NumWaiting)
	}

	// Now remove interest and make sure requests are removed.
	sub.Unsubscribe()
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("PC", ci.Name)
		require_NoError(t, err)
		if ci.NumWaiting != 0 {
			return fmt.Errorf("Requests still present")
		}
		return nil
	})
	// Now make sure the ephemeral goes away too.
	// Ephemerals have jitter by default of up to 1s.
	checkFor(t, 6*time.Second, 10*time.Millisecond, func() error {
		_, err := js.ConsumerInfo("PC", ci.Name)
		if err == nil {
			return fmt.Errorf("Consumer still present")
		}
		return nil
	})
}

func TestJetStreamConsumerPullCrossAccountExpiresNoDataRace(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 4GB, max_file_store: 1TB, store_dir: %q}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [ { service: "$JS.API.CONSUMER.MSG.NEXT.>", response: stream } ]
			},
			IU: {
				jetstream: enabled
				users: [ {user: ik, password: bar} ]
				imports [ { service: { subject: "$JS.API.CONSUMER.MSG.NEXT.*.*", account: JS } }]
			},
		}
	`, t.TempDir())))

	test := func() {
		s, _ := RunServerWithConfig(conf)
		defer s.Shutdown()

		// Connect to JS account and create stream, put some messages into it.
		nc, js := jsClientConnect(t, s, nats.UserInfo("dlc", "foo"))
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{Name: "PC", Subjects: []string{"foo"}})
		require_NoError(t, err)

		toSend := 100
		for i := 0; i < toSend; i++ {
			_, err := js.Publish("foo", []byte("OK"))
			require_NoError(t, err)
		}

		// Create pull consumer.
		_, err = js.AddConsumer("PC", &nats.ConsumerConfig{Durable: "PC", AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)

		// Now access from the importing account.
		nc2, _ := jsClientConnect(t, s, nats.UserInfo("ik", "bar"))
		defer nc2.Close()

		req := &JSApiConsumerGetNextRequest{Batch: 1}
		jreq, err := json.Marshal(req)
		require_NoError(t, err)
		rsubj := fmt.Sprintf(JSApiRequestNextT, "PC", "PC")
		sub, err := nc2.SubscribeSync("xx")
		require_NoError(t, err)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			time.Sleep(5 * time.Millisecond)
			sub.Unsubscribe()
			wg.Done()
		}()
		for i := 0; i < toSend; i++ {
			nc2.PublishRequest(rsubj, "xx", jreq)
		}
		wg.Wait()
	}
	// Need to rerun this test several times to get the race (which then would possible be panic
	// such as: "fatal error: concurrent map read and map write"
	for iter := 0; iter < 10; iter++ {
		test()
	}
}

// This tests account export/import replies across a LN connection with account import/export
// on both sides of the LN.
func TestJetStreamConsumerPullCrossAccountsAndLeafNodes(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: SJS
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 4GB, max_file_store: 1TB, domain: JSD, store_dir: %q }
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [ { service: "$JS.API.CONSUMER.MSG.NEXT.>", response: stream } ]
			},
			IU: {
				users: [ {user: mh, password: bar} ]
				imports [ { service: { subject: "$JS.API.CONSUMER.MSG.NEXT.*.*", account: JS } }]
			},
		}
		leaf { listen: "127.0.0.1:-1" }
	`, t.TempDir())))

	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: SLN
		listen: 127.0.0.1:-1
		accounts: {
			A: {
				users: [ {user: l, password: p} ]
				exports [ { service: "$JS.JSD.API.CONSUMER.MSG.NEXT.>", response: stream } ]
			},
			B: {
				users: [ {user: m, password: p} ]
				imports [ { service: { subject: "$JS.JSD.API.CONSUMER.MSG.NEXT.*.*", account: A } }]
			},
		}
		# bind local A to IU account on other side of LN.
		leaf { remotes [ { url: nats://mh:bar@127.0.0.1:%d; account: A } ] }
	`, o.LeafNode.Port)))

	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnectedCount(t, s, 1)

	// Connect to JS account, create stream and consumer and put in some messages.
	nc, js := jsClientConnect(t, s, nats.UserInfo("dlc", "foo"))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "PC", Subjects: []string{"foo"}})
	require_NoError(t, err)

	toSend := 10
	for i := 0; i < toSend; i++ {
		_, err := js.Publish("foo", []byte("OK"))
		require_NoError(t, err)
	}

	// Now create durable pull consumer.
	_, err = js.AddConsumer("PC", &nats.ConsumerConfig{Durable: "PC", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// Now access from the account on the leafnode, so importing on both sides and crossing a leafnode connection.
	nc2, _ := jsClientConnect(t, s2, nats.UserInfo("m", "p"))
	defer nc2.Close()

	req := &JSApiConsumerGetNextRequest{Batch: toSend, Expires: 500 * time.Millisecond}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)

	// Make sure we can get a batch correctly etc.
	// This requires response stream above in the export definition.
	sub, err := nc2.SubscribeSync("xx")
	require_NoError(t, err)

	rsubj := "$JS.JSD.API.CONSUMER.MSG.NEXT.PC.PC"
	err = nc2.PublishRequest(rsubj, "xx", jreq)
	require_NoError(t, err)
	checkSubsPending(t, sub, 10)

	// Queue up a bunch of requests.
	for i := 0; i < 10; i++ {
		err = nc2.PublishRequest(rsubj, "xx", jreq)
		require_NoError(t, err)
	}
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("PC", "PC")
		require_NoError(t, err)
		if ci.NumWaiting != 10 {
			return fmt.Errorf("Expected to see 10 waiting requests, got %d", ci.NumWaiting)
		}
		return nil
	})

	// Remove interest.
	sub.Unsubscribe()
	// Make sure requests go away eventually after they expire.
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("PC", "PC")
		require_NoError(t, err)
		if ci.NumWaiting != 0 {
			return fmt.Errorf("Expected to see no waiting requests, got %d", ci.NumWaiting)
		}
		return nil
	})
}

// This test is to explicitly test for all combinations of pull consumer behavior.
//  1. Long poll, will be used to emulate push. A request is only invalidated when batch is filled, it expires, or we lose interest.
//  2. Batch 1, will return no messages or a message. Works today.
//  3. Conditional wait, or one shot. This is what the clients do when the do a fetch().
//     They expect to wait up to a given time for any messages but will return once they have any to deliver, so parital fills.
//  4. Try, which never waits at all ever.
func TestJetStreamConsumerPullOneShotBehavior(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "dlc",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "foo",
	})
	require_NoError(t, err)

	// We will do low level requests by hand for this test as to not depend on any client impl.
	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", "dlc")

	getNext := func(batch int, expires time.Duration, noWait bool) (numMsgs int, elapsed time.Duration, hdr *nats.Header) {
		t.Helper()
		req := &JSApiConsumerGetNextRequest{Batch: batch, Expires: expires, NoWait: noWait}
		jreq, err := json.Marshal(req)
		require_NoError(t, err)
		// Create listener.
		reply, msgs := nats.NewInbox(), make(chan *nats.Msg, batch)
		sub, err := nc.ChanSubscribe(reply, msgs)
		require_NoError(t, err)
		defer sub.Unsubscribe()

		// Send request.
		start := time.Now()
		err = nc.PublishRequest(rsubj, reply, jreq)
		require_NoError(t, err)

		for {
			select {
			case m := <-msgs:
				if len(m.Data) == 0 && m.Header != nil {
					return numMsgs, time.Since(start), &m.Header
				}
				numMsgs++
				if numMsgs >= batch {
					return numMsgs, time.Since(start), nil
				}
			case <-time.After(expires + 250*time.Millisecond):
				t.Fatalf("Did not receive all the msgs in time")
			}
		}
	}

	expect := func(batch int, expires time.Duration, noWait bool, ne int, he *nats.Header, lt time.Duration, gt time.Duration) {
		t.Helper()
		n, e, h := getNext(batch, expires, noWait)
		if n != ne {
			t.Fatalf("Expected %d msgs, got %d", ne, n)
		}
		if !reflect.DeepEqual(h, he) {
			t.Fatalf("Expected %+v hdr, got %+v", he, h)
		}
		if lt > 0 && e > lt {
			t.Fatalf("Expected elapsed of %v to be less than %v", e, lt)
		}
		if gt > 0 && e < gt {
			t.Fatalf("Expected elapsed of %v to be greater than %v", e, gt)
		}
	}
	expectAfter := func(batch int, expires time.Duration, noWait bool, ne int, he *nats.Header, gt time.Duration) {
		t.Helper()
		expect(batch, expires, noWait, ne, he, 0, gt)
	}
	expectInstant := func(batch int, expires time.Duration, noWait bool, ne int, he *nats.Header) {
		t.Helper()
		expect(batch, expires, noWait, ne, he, 5*time.Millisecond, 0)
	}
	expectOK := func(batch int, expires time.Duration, noWait bool, ne int) {
		t.Helper()
		expectInstant(batch, expires, noWait, ne, nil)
	}

	noMsgs := &nats.Header{"Status": []string{"404"}, "Description": []string{"No Messages"}}
	reqTimeout := &nats.Header{"Status": []string{"408"}, "Description": []string{"Request Timeout"}, "Nats-Pending-Bytes": []string{"0"}, "Nats-Pending-Messages": []string{"1"}}

	// We are empty here, meaning no messages available.
	// Do not wait, should get noMsgs.
	expectInstant(1, 0, true, 0, noMsgs)
	// We should wait here for the full second.
	expectAfter(1, 250*time.Millisecond, false, 0, reqTimeout, 250*time.Millisecond)
	// This should also wait since no messages are available. This is the one shot scenario, or wait for at least a message if none are there.
	expectAfter(1, 500*time.Millisecond, true, 0, reqTimeout, 500*time.Millisecond)

	// Now let's put some messages into the system.
	for i := 0; i < 20; i++ {
		_, err := js.Publish("foo", []byte("HELLO"))
		require_NoError(t, err)
	}

	// Now run same 3 scenarios.
	expectOK(1, 0, true, 1)
	expectOK(5, 500*time.Millisecond, false, 5)
	expectOK(5, 500*time.Millisecond, true, 5)
}

func TestJetStreamConsumerPullMultipleRequestsExpireOutOfOrder(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "dlc",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "foo",
	})
	require_NoError(t, err)

	// We will now queue up 4 requests. All should expire but they will do so out of order.
	// We want to make sure we get them in correct order.
	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", "dlc")
	sub, err := nc.SubscribeSync("i.*")
	require_NoError(t, err)
	defer sub.Unsubscribe()

	for _, expires := range []time.Duration{200, 100, 25, 75} {
		reply := fmt.Sprintf("i.%d", expires)
		req := &JSApiConsumerGetNextRequest{Expires: expires * time.Millisecond}
		jreq, err := json.Marshal(req)
		require_NoError(t, err)
		err = nc.PublishRequest(rsubj, reply, jreq)
		require_NoError(t, err)
	}
	start := time.Now()
	checkSubsPending(t, sub, 4)
	elapsed := time.Since(start)

	if elapsed < 200*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("Expected elapsed to be close to %v, but got %v", 200*time.Millisecond, elapsed)
	}

	var rs []string
	for i := 0; i < 4; i++ {
		m, err := sub.NextMsg(0)
		require_NoError(t, err)
		rs = append(rs, m.Subject)
	}
	if expected := []string{"i.25", "i.75", "i.100", "i.200"}; !reflect.DeepEqual(rs, expected) {
		t.Fatalf("Received in wrong order, wanted %+v, got %+v", expected, rs)
	}
}

func TestJetStreamConsumerPullNoAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"ORDERS.*"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "dlc",
		AckPolicy: nats.AckNonePolicy,
	})
	require_NoError(t, err)
}

func TestJetStreamConsumerPullLastPerSubjectRedeliveries(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>"},
	})
	require_NoError(t, err)

	for i := 0; i < 20; i++ {
		sendStreamMsg(t, nc, fmt.Sprintf("foo.%v", i), "msg")
	}

	// Create a pull sub with a maxackpending that is <= of the number of
	// messages in the stream and as much as we are going to Fetch() below.
	sub, err := js.PullSubscribe(">", "dur",
		nats.AckExplicit(),
		nats.BindStream("TEST"),
		nats.DeliverLastPerSubject(),
		nats.MaxAckPending(10),
		nats.MaxRequestBatch(10),
		nats.AckWait(250*time.Millisecond))
	require_NoError(t, err)

	// Fetch the max number of message we can get, and don't ack them.
	_, err = sub.Fetch(10, nats.MaxWait(time.Second))
	require_NoError(t, err)

	// Wait for more than redelivery time.
	time.Sleep(500 * time.Millisecond)

	// Fetch again, make sure we can get those 10 messages.
	msgs, err := sub.Fetch(10, nats.MaxWait(time.Second))
	require_NoError(t, err)
	require_True(t, len(msgs) == 10)
	// Make sure those were the first 10 messages
	for i, m := range msgs {
		if m.Subject != fmt.Sprintf("foo.%v", i) {
			t.Fatalf("Expected message for subject foo.%v, got %v", i, m.Subject)
		}
		m.Ack()
	}
}

func TestJetStreamConsumerPullTimeoutHeaders(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "dlc",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	nc.Publish("foo.foo", []byte("foo"))
	nc.Publish("foo.bar", []byte("bar"))
	nc.Publish("foo.else", []byte("baz"))
	nc.Flush()

	// We will do low level requests by hand for this test as to not depend on any client impl.
	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", "dlc")

	maxBytes := 1024
	batch := 50
	req := &JSApiConsumerGetNextRequest{Batch: batch, Expires: 100 * time.Millisecond, NoWait: false, MaxBytes: maxBytes}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)
	// Create listener.
	reply, msgs := nats.NewInbox(), make(chan *nats.Msg, batch)
	sub, err := nc.ChanSubscribe(reply, msgs)
	require_NoError(t, err)
	defer sub.Unsubscribe()

	// Send request.
	err = nc.PublishRequest(rsubj, reply, jreq)
	require_NoError(t, err)

	bytesReceived := 0
	messagesReceived := 0

	checkHeaders := func(expectedStatus, expectedDesc string, m *nats.Msg) {
		t.Helper()
		if value := m.Header.Get("Status"); value != expectedStatus {
			t.Fatalf("Expected status %q, got %q", expectedStatus, value)
		}
		if value := m.Header.Get("Description"); value != expectedDesc {
			t.Fatalf("Expected description %q, got %q", expectedDesc, value)
		}
		if value := m.Header.Get(JSPullRequestPendingMsgs); value != fmt.Sprint(batch-messagesReceived) {
			t.Fatalf("Expected %d messages, got %s", batch-messagesReceived, value)
		}
		if value := m.Header.Get(JSPullRequestPendingBytes); value != fmt.Sprint(maxBytes-bytesReceived) {
			t.Fatalf("Expected %d bytes, got %s", maxBytes-bytesReceived, value)
		}
	}

	for done := false; !done; {
		select {
		case m := <-msgs:
			if len(m.Data) == 0 && m.Header != nil {
				checkHeaders("408", "Request Timeout", m)
				done = true
			} else {
				messagesReceived += 1
				bytesReceived += (len(m.Data) + len(m.Header) + len(m.Reply) + len(m.Subject))
			}
		case <-time.After(100 + 250*time.Millisecond):
			t.Fatalf("Did not receive all the msgs in time")
		}
	}

	// Now resend the request but then shutdown the server and
	// make sure we have the same info.
	err = nc.PublishRequest(rsubj, reply, jreq)
	require_NoError(t, err)
	natsFlush(t, nc)

	s.Shutdown()

	// It is possible that the client did not receive, so let's not fail
	// on that. But if the 409 indicating the server is shutdown
	// is received, then it should have the new headers.
	messagesReceived, bytesReceived = 0, 0
	select {
	case m := <-msgs:
		checkHeaders("409", "Server Shutdown", m)
	case <-time.After(500 * time.Millisecond):
		// we can't fail for that.
		t.Logf("Subscription did not receive the pull request response on server shutdown")
	}
}

func TestJetStreamConsumerPullBatchCompleted(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	msgSize := 128
	msg := make([]byte, msgSize)
	crand.Read(msg)

	for i := 0; i < 10; i++ {
		_, err := js.Publish("foo", msg)
		require_NoError(t, err)
	}

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "dur",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	req := JSApiConsumerGetNextRequest{Batch: 0, MaxBytes: 1024, Expires: 250 * time.Millisecond}

	reqb, _ := json.Marshal(req)
	sub := natsSubSync(t, nc, nats.NewInbox())
	err = nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.dur", sub.Subject, reqb)
	require_NoError(t, err)

	// Expect first message to arrive normally.
	_, err = sub.NextMsg(time.Second * 1)
	require_NoError(t, err)

	// Second message should be info that batch is complete, but there were pending bytes.
	pullMsg, err := sub.NextMsg(time.Second * 1)
	require_NoError(t, err)

	if v := pullMsg.Header.Get("Status"); v != "409" {
		t.Fatalf("Expected 409, got: %s", v)
	}
	if v := pullMsg.Header.Get("Description"); v != "Batch Completed" {
		t.Fatalf("Expected Batch Completed, got: %s", v)
	}
}

func TestJetStreamConsumerPullLargeBatchExpired(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("add stream failed: %s", err)
	}

	sub, err := js.PullSubscribe("foo", "dlc", nats.PullMaxWaiting(10), nats.MaxAckPending(10*50_000_000))
	if err != nil {
		t.Fatalf("Error creating pull subscriber: %v", err)
	}

	// Queue up 10 batch requests with timeout.
	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", "dlc")
	req := &JSApiConsumerGetNextRequest{Batch: 50_000_000, Expires: 100 * time.Millisecond}
	jreq, _ := json.Marshal(req)
	for i := 0; i < 10; i++ {
		nc.PublishRequest(rsubj, "bar", jreq)
	}
	nc.Flush()

	// Let them all expire.
	time.Sleep(150 * time.Millisecond)

	// Now do another and measure how long to timeout and shutdown the server.
	start := time.Now()
	sub.Fetch(1, nats.MaxWait(100*time.Millisecond))
	s.Shutdown()

	if delta := time.Since(start); delta > 200*time.Millisecond {
		t.Fatalf("Took too long to expire: %v", delta)
	}
}

func TestJetStreamConsumerPullTimeout(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name: "TEST",
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "pr",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	const numMessages = 1000
	// Send messages in small intervals.
	go func() {
		for i := 0; i < numMessages; i++ {
			time.Sleep(time.Millisecond * 10)
			sendStreamMsg(t, nc, "TEST", "data")
		}
	}()

	// Prepare manual Pull Request.
	req := &JSApiConsumerGetNextRequest{Batch: 200, NoWait: false, Expires: time.Millisecond * 100}
	jreq, _ := json.Marshal(req)

	subj := fmt.Sprintf(JSApiRequestNextT, "TEST", "pr")
	reply := "_pr_"
	var got atomic.Int32
	nc.PublishRequest(subj, reply, jreq)

	// Manually subscribe to inbox subject and send new request only if we get `408 Request Timeout`.
	sub, _ := nc.Subscribe(reply, func(msg *nats.Msg) {
		if msg.Header.Get("Status") == "408" && msg.Header.Get("Description") == "Request Timeout" {
			nc.PublishRequest(subj, reply, jreq)
			nc.Flush()
		} else {
			got.Add(1)
			msg.Ack()
		}
	})
	defer sub.Unsubscribe()

	// Check if we're not stuck.
	checkFor(t, time.Second*30, time.Second*1, func() error {
		if got.Load() < int32(numMessages) {
			return fmt.Errorf("expected %d messages", numMessages)
		}
		return nil
	})
}

func TestJetStreamConsumerPullMaxBytes(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name: "TEST",
	})
	require_NoError(t, err)

	// Put in ~2MB, each ~100k
	msz, dsz := 100_000, 99_950
	total, msg := 20, []byte(strings.Repeat("Z", dsz))

	for i := 0; i < total; i++ {
		if _, err := js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "pr",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	req := &JSApiConsumerGetNextRequest{MaxBytes: 100, NoWait: true}
	jreq, _ := json.Marshal(req)

	subj := fmt.Sprintf(JSApiRequestNextT, "TEST", "pr")
	reply := "_pr_"
	sub, _ := nc.SubscribeSync(reply)
	defer sub.Unsubscribe()

	checkHeader := func(m *nats.Msg, expected *nats.Header) {
		t.Helper()
		if len(m.Data) != 0 {
			t.Fatalf("Did not expect data, got %d bytes", len(m.Data))
		}
		expectedStatus, givenStatus := expected.Get("Status"), m.Header.Get("Status")
		expectedDesc, givenDesc := expected.Get("Description"), m.Header.Get("Description")
		if expectedStatus != givenStatus || expectedDesc != givenDesc {
			t.Fatalf("expected  %s %s, got %s %s", expectedStatus, expectedDesc, givenStatus, givenDesc)
		}
	}

	// If we ask for less MaxBytes then a single message make sure we get an error.
	badReq := &nats.Header{"Status": []string{"409"}, "Description": []string{"Message Size Exceeds MaxBytes"}}

	nc.PublishRequest(subj, reply, jreq)
	m, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	checkSubsPending(t, sub, 0)
	checkHeader(m, badReq)

	// If we request a ton of max bytes make sure batch size overrides.
	req = &JSApiConsumerGetNextRequest{Batch: 1, MaxBytes: 10_000_000, NoWait: true}
	jreq, _ = json.Marshal(req)
	nc.PublishRequest(subj, reply, jreq)
	// we expect two messages, as the second one should be `Batch Completed` status.
	checkSubsPending(t, sub, 2)

	// first one is message from the stream.
	m, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_True(t, len(m.Data) == dsz)
	require_True(t, len(m.Header) == 0)
	// second one is the status.
	m, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	if v := m.Header.Get("Description"); v != "Batch Completed" {
		t.Fatalf("Expected Batch Completed, got: %s", v)
	}
	checkSubsPending(t, sub, 0)

	// Same but with batch > 1
	req = &JSApiConsumerGetNextRequest{Batch: 5, MaxBytes: 10_000_000, NoWait: true}
	jreq, _ = json.Marshal(req)
	nc.PublishRequest(subj, reply, jreq)
	// 6, not 5, as 6th is the status.
	checkSubsPending(t, sub, 6)
	for i := 0; i < 5; i++ {
		m, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		require_True(t, len(m.Data) == dsz)
		require_True(t, len(m.Header) == 0)
	}
	m, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	if v := m.Header.Get("Description"); v != "Batch Completed" {
		t.Fatalf("Expected Batch Completed, got: %s", v)
	}
	checkSubsPending(t, sub, 0)

	// Now ask for large batch but make sure we are limited by batch size.
	req = &JSApiConsumerGetNextRequest{Batch: 1_000, MaxBytes: msz * 4, NoWait: true}
	jreq, _ = json.Marshal(req)
	nc.PublishRequest(subj, reply, jreq)
	// Receive 4 messages + the 409
	checkSubsPending(t, sub, 5)
	for i := 0; i < 4; i++ {
		m, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		require_True(t, len(m.Data) == dsz)
		require_True(t, len(m.Header) == 0)
	}
	m, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	checkHeader(m, badReq)
	checkSubsPending(t, sub, 0)

	req = &JSApiConsumerGetNextRequest{Batch: 1_000, MaxBytes: msz, NoWait: true}
	jreq, _ = json.Marshal(req)
	nc.PublishRequest(subj, reply, jreq)
	// Receive 1 message + 409
	checkSubsPending(t, sub, 2)
	m, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_True(t, len(m.Data) == dsz)
	require_True(t, len(m.Header) == 0)
	m, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	checkHeader(m, badReq)
	checkSubsPending(t, sub, 0)
}

// https://github.com/nats-io/nats-server/issues/6824
func TestJetStreamConsumerDeliverAllOverlappingFilterSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnectNewAPI(t, s)
	defer nc.Close()

	ctx := context.Background()
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"stream.>"},
	})
	require_NoError(t, err)

	publishMessageCount := 10
	for i := 0; i < publishMessageCount; i++ {
		_, err = js.Publish(ctx, "stream.A", nil)
		require_NoError(t, err)
	}

	// Create consumer
	consumer, err := js.CreateOrUpdateConsumer(ctx, "TEST", jetstream.ConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubjects: []string{
			"stream.A",
			"stream.A.>",
		},
	})
	require_NoError(t, err)

	messages := make(chan jetstream.Msg)
	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		messages <- msg
		msg.Ack()
	})
	require_NoError(t, err)
	defer cc.Drain()

	var count = 0
	for {
		if count == publishMessageCount {
			// All messages received.
			return
		}
		select {
		case <-messages:
			count++
		case <-time.After(2 * time.Second):
			t.Errorf("Timeout reached, %d messages received. Exiting.", count)
			return
		}
	}
}

// https://github.com/nats-io/nats-server/issues/6844
func TestJetStreamConsumerDeliverAllNonOverlappingFilterSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnectNewAPI(t, s)
	defer nc.Close()

	ctx := context.Background()
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"stream.>"},
	})
	require_NoError(t, err)

	publishMessageCount := 10
	for i := 0; i < publishMessageCount; i++ {
		_, err = js.Publish(ctx, "stream.subject", nil)
		require_NoError(t, err)
	}

	// Create consumer
	consumer, err := js.CreateOrUpdateConsumer(ctx, "TEST", jetstream.ConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubjects: []string{
			"stream.subject.A",
			"stream.subject.A.>",
		},
	})
	require_NoError(t, err)

	i, err := consumer.Info(ctx)
	require_NoError(t, err)
	require_Equal(t, i.NumPending, 0)
}

// https://github.com/nats-io/nats-server/issues/7336
func TestJetStreamConsumerDeliverPartialOverlappingFilterSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnectNewAPI(t, s)
	defer nc.Close()

	ctx := context.Background()
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"stream.>"},
	})
	require_NoError(t, err)

	publishMessageCount := 10
	for i := 0; i < publishMessageCount; i++ {
		_, err = js.Publish(ctx, "stream.A", nil)
		require_NoError(t, err)
	}

	// Create consumer
	consumer, err := js.CreateOrUpdateConsumer(ctx, "TEST", jetstream.ConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubjects: []string{
			"stream.A",
			"stream.*.A",
		},
	})
	require_NoError(t, err)

	messages := make(chan jetstream.Msg)
	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		messages <- msg
		msg.Ack()
	})
	require_NoError(t, err)
	defer cc.Drain()

	var count = 0
	for {
		if count == publishMessageCount {
			// All messages received.
			return
		}
		select {
		case <-messages:
			count++
		case <-time.After(2 * time.Second):
			t.Errorf("Timeout reached, %d messages received. Exiting.", count)
			return
		}
	}
}

func TestJetStreamConsumerStateAlwaysFromStore(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "CONSUMER",
		AckWait:       2 * time.Second,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "foo.bar",
	})
	require_NoError(t, err)

	// Publish two messages, one the consumer is interested in.
	_, err = js.Publish("foo.bar", nil)
	require_NoError(t, err)
	_, err = js.Publish("foo.other", nil)
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo.bar", "CONSUMER")
	require_NoError(t, err)
	defer sub.Drain()

	// Consumer info should start empty.
	ci, err := js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Stream, 0)
	require_Equal(t, ci.AckFloor.Stream, 0)

	// Fetch more messages than match our filter.
	msgs, err := sub.Fetch(2, nats.MaxWait(time.Second))
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)

	// We have received, but not acknowledged, consumer info must reflect that.
	ci, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Stream, 1)
	require_Equal(t, ci.AckFloor.Stream, 0)

	// Now we acknowledge the message and expect our delivered/ackfloor to be correct.
	require_NoError(t, msgs[0].AckSync())

	ci, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Stream, 1)
	require_Equal(t, ci.AckFloor.Stream, 1)
}

func TestJetStreamConsumerPullNoWaitBatchLargerThanPending(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: replicas,
		})
		require_NoError(t, err)

		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
			Durable:       "C",
			AckPolicy:     nats.AckExplicitPolicy,
			FilterSubject: "foo",
			Replicas:      replicas,
		})
		require_NoError(t, err)

		req := JSApiConsumerGetNextRequest{Batch: 10, NoWait: true}

		for range 5 {
			_, err := js.Publish("foo", []byte("OK"))
			require_NoError(t, err)
		}

		sub := sendRequest(t, nc, "rply", req)
		defer sub.Unsubscribe()

		// Should get all 5 messages.
		for range 5 {
			msg, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
			if len(msg.Data) == 0 && msg.Header != nil {
				t.Fatalf("Expected data, got: %s", msg.Header.Get("Description"))
			}
		}
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamConsumerNotInactiveDuringAckWait(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: replicas,
		})
		require_NoError(t, err)

		_, err = js.Publish("foo", nil)
		require_NoError(t, err)

		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
			Durable:           "CONSUMER",
			AckPolicy:         nats.AckExplicitPolicy,
			Replicas:          replicas,
			InactiveThreshold: 500 * time.Millisecond, // Pull mode adds up to 1 second randomly.
			AckWait:           time.Minute,
		})
		require_NoError(t, err)

		_, err = js.ConsumerInfo("TEST", "CONSUMER")
		require_NoError(t, err)

		sub, err := js.PullSubscribe(_EMPTY_, "CONSUMER", nats.BindStream("TEST"))
		require_NoError(t, err)
		defer sub.Drain()

		msgs, err := sub.Fetch(1)
		require_NoError(t, err)
		require_Len(t, len(msgs), 1)

		_, err = js.ConsumerInfo("TEST", "CONSUMER")
		require_NoError(t, err)

		// AckWait is still active, so must not delete the consumer while waiting for an ack.
		time.Sleep(1750 * time.Millisecond)

		_, err = js.ConsumerInfo("TEST", "CONSUMER")
		require_NoError(t, err)
		require_NoError(t, msgs[0].AckSync())

		// Not waiting on AckWait anymore, consumer is deleted after the inactivity threshold.
		time.Sleep(1750 * time.Millisecond)
		_, err = js.ConsumerInfo("TEST", "CONSUMER")
		require_Error(t, err, nats.ErrConsumerNotFound)
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamConsumerNotInactiveDuringAckWaitBackoff(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: replicas,
		})
		require_NoError(t, err)

		_, err = js.Publish("foo", nil)
		require_NoError(t, err)

		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
			Durable:           "CONSUMER",
			AckPolicy:         nats.AckExplicitPolicy,
			Replicas:          replicas,
			InactiveThreshold: 500 * time.Millisecond, // Pull mode adds up to 1 second randomly.
			BackOff: []time.Duration{
				2 * time.Second,
				4 * time.Second,
			},
		})
		require_NoError(t, err)

		_, err = js.ConsumerInfo("TEST", "CONSUMER")
		require_NoError(t, err)

		sub, err := js.PullSubscribe(_EMPTY_, "CONSUMER", nats.BindStream("TEST"))
		require_NoError(t, err)
		defer sub.Drain()

		msgs, err := sub.Fetch(1)
		require_NoError(t, err)
		require_Len(t, len(msgs), 1)

		_, err = js.ConsumerInfo("TEST", "CONSUMER")
		require_NoError(t, err)

		// AckWait is still active, so must not delete the consumer while waiting for an ack.
		time.Sleep(1750 * time.Millisecond)

		_, err = js.ConsumerInfo("TEST", "CONSUMER")
		require_NoError(t, err)
		require_NoError(t, msgs[0].Nak())

		msgs, err = sub.Fetch(1)
		require_NoError(t, err)
		require_Len(t, len(msgs), 1)

		// AckWait is still active, now based on backoff, so must not delete the consumer while waiting for an ack.
		// We've confirmed can wait 2s AckWait + InactiveThreshold, now check we can also wait for the backoff.
		time.Sleep(3750 * time.Millisecond)

		_, err = js.ConsumerInfo("TEST", "CONSUMER")
		require_NoError(t, err)
		require_NoError(t, msgs[0].AckSync())

		// Not waiting on AckWait anymore, consumer is deleted after the inactivity threshold.
		time.Sleep(1750 * time.Millisecond)
		_, err = js.ConsumerInfo("TEST", "CONSUMER")
		require_Error(t, err, nats.ErrConsumerNotFound)
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestSortingConsumerPullRequests(t *testing.T) {

	for _, test := range []struct {
		name     string
		requests []waitingRequest
		expected []waitingRequest
	}{
		{
			name: "sort",
			requests: []waitingRequest{
				{priorityGroup: &PriorityGroup{Priority: 1}},
				{priorityGroup: &PriorityGroup{Priority: 2}},
				{priorityGroup: &PriorityGroup{Priority: 1}},
				{priorityGroup: &PriorityGroup{Priority: 3}},
			},
			expected: []waitingRequest{
				{priorityGroup: &PriorityGroup{Priority: 1}},
				{priorityGroup: &PriorityGroup{Priority: 1}},
				{priorityGroup: &PriorityGroup{Priority: 2}},
				{priorityGroup: &PriorityGroup{Priority: 3}},
			},
		},
		{
			name: "test if sort is stable",
			requests: []waitingRequest{
				{priorityGroup: &PriorityGroup{Priority: 1}, reply: "1a"},
				{priorityGroup: &PriorityGroup{Priority: 2}, reply: "2a"},
				{priorityGroup: &PriorityGroup{Priority: 1}, reply: "1b"},
				{priorityGroup: &PriorityGroup{Priority: 2}, reply: "2b"},
				{priorityGroup: &PriorityGroup{Priority: 1}, reply: "1c"},
				{priorityGroup: &PriorityGroup{Priority: 3}, reply: "3a"},
				{priorityGroup: &PriorityGroup{Priority: 2}, reply: "2c"},
			},
			expected: []waitingRequest{
				{priorityGroup: &PriorityGroup{Priority: 1}, reply: "1a"},
				{priorityGroup: &PriorityGroup{Priority: 1}, reply: "1b"},
				{priorityGroup: &PriorityGroup{Priority: 1}, reply: "1c"},
				{priorityGroup: &PriorityGroup{Priority: 2}, reply: "2a"},
				{priorityGroup: &PriorityGroup{Priority: 2}, reply: "2b"},
				{priorityGroup: &PriorityGroup{Priority: 2}, reply: "2c"},
				{priorityGroup: &PriorityGroup{Priority: 3}, reply: "3a"},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Helper()

			wq := newWaitQueue(100)

			for _, r := range test.requests {
				err := wq.addPrioritized(&r)
				require_NoError(t, err)
			}

			if wq.n != len(test.expected) {
				t.Fatalf("Expected %d requests, got %d", len(test.expected), wq.n)
			}

			// Verify order
			for i, expected := range test.expected {
				current := wq.peek()
				if current == nil {
					t.Fatalf("Expected request at position %d, but queue was empty", i)
				}
				if current.priorityGroup.Priority != expected.priorityGroup.Priority {
					t.Fatalf("At position %d: expected priority %d, got %d",
						i, expected.priorityGroup.Priority, current.priorityGroup.Priority)
				}
				if current.reply != expected.reply {
					t.Fatalf("At position %d: expected reply %q, got %q", i, expected.reply, current.reply)
				}
				wq.removeCurrent()
			}

		})
	}
}

func TestWaitQueuePopAndRequeue(t *testing.T) {
	t.Run("basic requeue with batches", func(t *testing.T) {
		wq := newWaitQueue(100)

		// Add elements with different priorities
		reqs := []waitingRequest{
			{priorityGroup: &PriorityGroup{Priority: 1}, reply: "1a", n: 3},
			{priorityGroup: &PriorityGroup{Priority: 1}, reply: "1b", n: 3},
			{priorityGroup: &PriorityGroup{Priority: 1}, reply: "1c", n: 3},
			{priorityGroup: &PriorityGroup{Priority: 2}, reply: "2a", n: 3},
			{priorityGroup: &PriorityGroup{Priority: 2}, reply: "2b", n: 3},
			{priorityGroup: &PriorityGroup{Priority: 2}, reply: "2c", n: 3},
			{priorityGroup: &PriorityGroup{Priority: 3}, reply: "3a", n: 3},
			{priorityGroup: &PriorityGroup{Priority: 3}, reply: "3b", n: 3},
			{priorityGroup: &PriorityGroup{Priority: 3}, reply: "3c", n: 3},
		}

		for i := range reqs {
			err := wq.addPrioritized(&reqs[i])
			require_NoError(t, err)
		}

		i := 0
		j := 0
		for {
			wr := wq.popAndRequeue()
			require_Equal(t, wr.reply, fmt.Sprintf("%da", j+1))
			wr = wq.popAndRequeue()
			require_Equal(t, wr.reply, fmt.Sprintf("%db", j+1))
			wr = wq.popAndRequeue()
			require_Equal(t, wr.reply, fmt.Sprintf("%dc", j+1))

			i++
			if i%3 == 0 {
				j++
			}
			require_Equal(t, wq.n, 9-(j*3))
			if j == 2 {
				break
			}
		}
	})

	t.Run("request removal when fully served", func(t *testing.T) {
		wq := newWaitQueue(100)

		reqs := []waitingRequest{
			{priorityGroup: &PriorityGroup{Priority: 1}, reply: "1a", n: 2}, // Will be removed after 2 pops
			{priorityGroup: &PriorityGroup{Priority: 1}, reply: "1b", n: 1}, // Will be removed after 1 pop
			{priorityGroup: &PriorityGroup{Priority: 2}, reply: "2a", n: 3}, // Will remain
		}

		for i := range reqs {
			err := wq.addPrioritized(&reqs[i])
			require_NoError(t, err)
		}
		initialCount := wq.n

		// Pop 1a first time (n=2 -> n=1, should requeue)
		wr := wq.popAndRequeue()
		if wr == nil || wr.reply != "1a" || wr.n != 1 {
			t.Fatalf("Expected 1a with n=1, got %v with n=%d", wr, wr.n)
		}
		if wq.n != initialCount {
			t.Fatalf("Queue size should remain %d, got %d", initialCount, wq.n)
		}

		// Pop 1b (n=1 -> n=0, should be removed)
		wr = wq.popAndRequeue()
		if wr == nil || wr.reply != "1b" || wr.n != 0 {
			t.Fatalf("Expected 1b with n=0, got %v with n=%d", wr, wr.n)
		}
		if wq.n != initialCount-1 {
			t.Fatalf("Queue size should be %d after 1b removal, got %d", initialCount-1, wq.n)
		}

		// Pop 1a second time (n=1 -> n=0, should be removed)
		wr = wq.popAndRequeue()
		if wr == nil || wr.reply != "1a" || wr.n != 0 {
			t.Fatalf("Expected 1a with n=0, got %v with n=%d", wr, wr.n)
		}
		if wq.n != initialCount-2 {
			t.Fatalf("Queue size should be %d after 1a removal, got %d", initialCount-2, wq.n)
		}

		// Only 2a should remain
		next := wq.peek()
		if next == nil || next.reply != "2a" || next.n != 3 {
			t.Fatalf("Expected only 2a with n=3 to remain, got %v with n=%d", next, next.n)
		}
	})

	t.Run("single element lifecycle", func(t *testing.T) {
		wq := newWaitQueue(10)
		req := waitingRequest{priorityGroup: &PriorityGroup{Priority: 1}, reply: "single", n: 2}
		wq.add(&req)

		// First pop (n=2 -> n=1, should stay)
		result := wq.popAndRequeue()
		if result == nil || result.reply != "single" || result.n != 1 {
			t.Fatalf("Expected single with n=1, got %v with n=%d", result, result.n)
		}
		if wq.n != 1 {
			t.Fatalf("Queue should still have 1 element, got %d", wq.n)
		}

		// Second pop (n=1 -> n=0, should be removed)
		result = wq.popAndRequeue()
		if result == nil || result.reply != "single" || result.n != 0 {
			t.Fatalf("Expected single with n=0, got %v with n=%d", result, result.n)
		}
		if wq.n != 0 {
			t.Fatalf("Queue should be empty after removal, got %d elements", wq.n)
		}
		if wq.head != nil {
			t.Fatalf("Queue head should be nil after removal, got %v", wq.head)
		}
	})
}

func TestJetStreamConsumerPrioritized(t *testing.T) {

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "CONSUMER",
		AckPolicy:      AckNone,
		FilterSubject:  "foo",
		PriorityGroups: []string{"A"},
		PriorityPolicy: PriorityPrioritized,
	})
	require_NoError(t, err)

	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)

	// Helper function to send pull request and get response
	sendPullRequest := func(t *testing.T, inbox string, priority int, batch int) *nats.Subscription {
		sub, err := nc.SubscribeSync(inbox)
		require_NoError(t, err)

		req := JSApiConsumerGetNextRequest{
			Batch:   batch,
			Expires: 10 * time.Second,
			PriorityGroup: PriorityGroup{
				Group:    "A",
				Priority: priority,
			},
		}
		reqb, _ := json.Marshal(req)
		err = nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.CONSUMER", inbox, reqb)
		require_NoError(t, err)

		return sub
	}

	t.Run("invalid priority number", func(t *testing.T) {

		sub := sendPullRequest(t, "invalid_priority", 10, 1)

		msg, err := sub.NextMsg(2 * time.Second)
		require_NoError(t, err)
		require_Equal(t, msg.Header.Get("Status"), "400")

		sub2 := sendPullRequest(t, "invalid_priority", -5, 1)
		require_Equal(t, msg.Header.Get("Status"), "400")

		msg, err = sub2.NextMsg(2 * time.Second)
		require_NoError(t, err)
		require_Equal(t, msg.Header.Get("Status"), "400")

	})

	t.Run("priority ordering", func(t *testing.T) {
		priority3 := sendPullRequest(t, "priority3", 3, 1) // Priority 3
		priority1 := sendPullRequest(t, "priority1", 1, 1) // Priority 1 (should be served first)
		priority2 := sendPullRequest(t, "priority2", 2, 2) // Priority 2

		// Small delay to ensure requests are processed
		time.Sleep(50 * time.Millisecond)

		_, err = js.Publish("foo", fmt.Appendf(nil, "message"))
		require_NoError(t, err)

		// Priority 3 should time out
		_, err := priority3.NextMsg(200 * time.Millisecond)
		require_Error(t, err, nats.ErrTimeout)

		// Priority 2 should time out
		_, err = priority2.NextMsg(200 * time.Millisecond)
		require_Error(t, err, nats.ErrTimeout)

		// Priority 1 should get message first
		msg, err := priority1.NextMsg(2 * time.Second)
		require_NoError(t, err)
		require_NotNil(t, msg)

		_, err = js.Publish("foo", fmt.Appendf(nil, "message"))
		require_NoError(t, err)

		// Priority 3 should time out.
		_, err = priority3.NextMsg(200 * time.Millisecond)
		require_Error(t, err, nats.ErrTimeout)

		// Priority 2 should get message next
		msg, err = priority2.NextMsg(2 * time.Second)
		require_NoError(t, err)
		require_NotNil(t, msg)

		_, err = js.Publish("foo", fmt.Appendf(nil, "message"))
		require_NoError(t, err)

		// Priority 2 should get message next
		msg, err = priority2.NextMsg(2 * time.Second)
		require_NoError(t, err)
		require_NotNil(t, msg)

		// Priority 3 should time out
		_, err = priority3.NextMsg(2 * time.Second)
		require_Error(t, err, nats.ErrTimeout)

		_, err = js.Publish("foo", fmt.Appendf(nil, "message"))
		require_NoError(t, err)

		// Priority 3 should get message next
		msg, err = priority3.NextMsg(2 * time.Second)
		require_NoError(t, err)
		require_NotNil(t, msg)
	})

	t.Run("dynamic priority interruption", func(t *testing.T) {

		inbox3 := nats.NewInbox()
		sub3 := sendPullRequest(t, inbox3, 3, 3)

		_, err = js.Publish("foo", fmt.Appendf(nil, "msg"))
		require_NoError(t, err)

		msg, err := sub3.NextMsg(2 * time.Second)
		require_NoError(t, err)
		require_NotNil(t, msg)

		// Now, despite priority 3 still having pending messages, a new pull request
		// with a lower priority should be able to take over the delivery.
		inbox2 := nats.NewInbox()
		sub2 := sendPullRequest(t, inbox2, 2, 2)

		_, err = js.Publish("foo", fmt.Appendf(nil, "msg"))
		require_NoError(t, err)

		msg, err = sub2.NextMsg(2 * time.Second)
		require_NoError(t, err)
		require_NotNil(t, msg)

		// The same should happen with priority 1.
		inbox1 := nats.NewInbox()
		sub1 := sendPullRequest(t, inbox1, 1, 1) // Priority 1, batch 3

		_, err = js.Publish("foo", fmt.Appendf(nil, "msg"))
		require_NoError(t, err)
		_, err = js.Publish("foo", fmt.Appendf(nil, "msg"))
		require_NoError(t, err)
		_, err = js.Publish("foo", fmt.Appendf(nil, "msg"))
		require_NoError(t, err)

		msg, err = sub1.NextMsg(2 * time.Second)
		require_NoError(t, err)
		require_NotNil(t, msg)

		// Now priority 2 should take over for last message.
		msg, err = sub2.NextMsg(2 * time.Second)
		require_NoError(t, err)
		require_NotNil(t, msg)

		// Finally, priority 3 should get its last message.
		msg, err = sub3.NextMsg(2 * time.Second)
		require_NoError(t, err)
		require_NotNil(t, msg)
	})
}

func TestJetStreamConsumerMaxDeliverUnderflow(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	cfg := &nats.ConsumerConfig{Durable: "CONSUMER", MaxDeliver: -1}
	_, err = js.AddConsumer("TEST", cfg)
	require_NoError(t, err)

	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)

	// Infinite MaxDeliver should be zero.
	o.mu.RLock()
	maxdc := o.maxdc
	o.mu.RUnlock()
	require_Equal(t, maxdc, 0)

	// Finite MaxDeliver should be reported the same.
	cfg.MaxDeliver = 1
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)
	o.mu.RLock()
	maxdc = o.maxdc
	o.mu.RUnlock()
	require_Equal(t, maxdc, 1)

	// Infinite MaxDeliver should be zero.
	cfg.MaxDeliver = -1
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)
	o.mu.RLock()
	maxdc = o.maxdc
	o.mu.RUnlock()
	require_Equal(t, maxdc, 0)
}
