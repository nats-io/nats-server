// Copyright 2022-2024 The NATS Authors
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

//go:build !skip_js_tests && !skip_js_cluster_tests_5
// +build !skip_js_tests,!skip_js_cluster_tests_5

package server

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestDurableConsumerExhaustMaxDeliver(t *testing.T) {

	// Inspired by https://github.com/nats-io/nats-server/issues/6374:

	const (
		NumServers               = 3
		StreamReplicas           = NumServers
		ConsumerReplicas         = NumServers
		ClusterName              = "Test-Cluster"
		StreamName               = "Test-Stream"
		ConsumerName             = "Test-Consumer"
		TestSequenceHeader       = "test-sequence"
		SubjectPrefix            = "S."
		NumMessages              = 10000
		MessageSize              = 32
		RngSeed                  = 12345
		DistinctStreamSubjects   = 25
		DistinctConsumerSubjects = 12
		Verbose                  = false

		// Consumer does not acknowledge any message, so make the timeout/retry short
		AckWait = 100 * time.Millisecond
		// How many times each message gets delivered (given the lack of ACKs)
		DeliveryAttempts = 5
		// Max number of messages passed to consumer before previous ones are acknowledged
		MaxOutstandingAcks = min(100, NumMessages)
		// How long after last message delivered consumer stops waiting for more messages
		InactivityInterval = 10 * time.Second
		// Fetch timeout (make sure it's significantly smaller than inactivity interval
		FetchTimeout = InactivityInterval / 10
	)

	debugf := func(format string, v ...any) {
		if Verbose {
			t.Logf(format, v...)
		}
	}

	publishSubjects := make([]string, DistinctStreamSubjects)
	for i := 0; i < DistinctStreamSubjects; i++ {
		publishSubjects[i] = SubjectPrefix + fmt.Sprintf("%d", i)
	}
	debugf("Stream/Publish subjects (%d): %v", DistinctStreamSubjects, publishSubjects)

	consumerSubjects := make([]string, DistinctConsumerSubjects)
	for i := 0; i < DistinctConsumerSubjects; i++ {
		consumerSubjects[i] = SubjectPrefix + fmt.Sprintf("%d", i)
	}
	debugf("Consumer filter subjects (%d): %v", DistinctConsumerSubjects, consumerSubjects)

	streamConfig := nats.StreamConfig{
		Name:     StreamName,
		Replicas: StreamReplicas,
		Storage:  nats.FileStorage,
		Subjects: []string{SubjectPrefix + ">"},
	}

	consumerConfig := nats.ConsumerConfig{
		Name:              ConsumerName,
		Replicas:          ConsumerReplicas,
		FilterSubjects:    consumerSubjects,
		AckWait:           AckWait,
		MaxDeliver:        DeliveryAttempts,
		MaxAckPending:     MaxOutstandingAcks,
		AckPolicy:         nats.AckExplicitPolicy,
		InactiveThreshold: 48 * time.Hour,
	}

	// Map MsgId => Delivery count
	// Counts how many times each msgId was delivered by consumer
	deliveryCounts := make(map[string]uint64, NumMessages)

	rng := rand.New(rand.NewSource(RngSeed))

	// Setup cluster
	cluster := createJetStreamClusterExplicit(t, ClusterName, NumServers)
	defer cluster.shutdown()

	// Connect (shared by producer and later consumer)
	nc, js := jsClientConnect(t, cluster.randomServer())
	defer nc.Close()

	var err error

	// Create stream
	_, err = js.AddStream(&streamConfig)
	require_NoError(t, err)

	// Create durable consumer
	_, err = js.AddConsumer(StreamName, &consumerConfig)
	require_NoError(t, err)

	messageDataBuffer := make([]byte, MessageSize)

	// Publish fixed number of messages (to a random subject)
	t.Logf("Publishing %d messages", NumMessages)
	for i := 1; i <= NumMessages; i++ {
		// Choose a random subject
		subjectRandomIndex := rand.Intn(DistinctStreamSubjects)
		subject := publishSubjects[subjectRandomIndex]

		// Create unique message id
		msgId := fmt.Sprintf("%s-%d", subject, i)

		// Create message
		msg := nats.NewMsg(subject)
		rng.Read(messageDataBuffer)
		msg.Data = messageDataBuffer
		msg.Header.Add(nats.MsgIdHdr, msgId)
		msg.Header.Add(TestSequenceHeader, fmt.Sprintf("%d", i))

		// Publish message
		pubAck, err := js.PublishMsg(msg)
		require_NoError(t, err)

		// Is the consumer expected to deliver this message?
		expectedDelivery := subjectRandomIndex < DistinctConsumerSubjects
		if expectedDelivery {
			// If so, add the message id to the tracking table
			deliveryCounts[msgId] = 0
		}

		debugf("Published message: %s to subject %s (stream seq: %d) expected delivery? %v\n", msgId, subject, pubAck.Sequence, expectedDelivery)
	}

	debugf("Published %d messages, expecting consumer to deliver %d of them (%d times each)", NumMessages, len(deliveryCounts), DeliveryAttempts)

	// Subscribe
	sub, err := js.PullSubscribe("", "", nats.Bind(StreamName, ConsumerName))
	require_NoError(t, err)

	deliveriesTimer := time.NewTimer(InactivityInterval)

	t.Logf("Consuming %d messages", len(deliveryCounts))

ConsumeLoop:
	for {

		// Break out of ConsumeLoop if a given amount of time passes without any message delivered
		select {
		case <-deliveriesTimer.C:
			t.Logf("No delivery for %s, moving on", InactivityInterval)
			break ConsumeLoop
		default:
			// Continue consuming
		}

		// Fetch some messages
		messagesBatch, err := sub.Fetch(100, nats.MaxWait(FetchTimeout))
		if errors.Is(err, nats.ErrTimeout) {
			// Fetch timeout, keep trying as long as there is time
			debugf("Fetch timeout")
			continue ConsumeLoop
		}
		require_NoError(t, err)

		// If anything was received, reset the stall timer
		if len(messagesBatch) > 0 {
			deliveriesTimer.Reset(InactivityInterval)
		}

		debugf("Received a batch of %d messages", len(messagesBatch))
		for _, msg := range messagesBatch {
			msgMeta, err := msg.Metadata()
			require_NoError(t, err)

			msgId := msg.Header.Get(nats.MsgIdHdr)
			require_NotEqual(t, msgId, "")
			debugf("Received message: %s (seq: <%d,%d>)\n", msgId, msgMeta.Sequence.Stream, msgMeta.Sequence.Consumer)

			// Increment number of deliveries for this message id
			count, ok := deliveryCounts[msgId]
			require_True(t, ok)
			deliveryCounts[msgId] = count + 1
		}
	}

	// Timeout, no deliveries for a certain amount of time
	// Now check how many times each message was delivered
	neverDelivered, overDelivered, underDelivered, correct := 0, 0, 0, 0
	for msgId, count := range deliveryCounts {
		if count == 0 {
			neverDelivered++
		} else if count > DeliveryAttempts {
			debugf("Message %s was delivered more times than expected: %d", msgId, count)
			overDelivered++
		} else if count < DeliveryAttempts {
			debugf("Message %s was delivered fewer times than expected: %d", msgId, count)
			underDelivered++
		} else {
			correct++
		}
	}

	if neverDelivered > 0 || overDelivered > 0 || underDelivered > 0 {
		t.Fatalf(
			"Of %d expected messages: %d never delivered, %d over-delivered, %d under-delivered, %d delivered as expected",
			len(deliveryCounts),
			neverDelivered,
			overDelivered,
			underDelivered,
			correct,
		)
	} else {
		t.Logf("%d messages delivered %d times each", len(deliveryCounts), DeliveryAttempts)
	}
}
