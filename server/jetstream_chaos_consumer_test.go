// Copyright 2022 The NATS Authors
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

//go:build js_chaos_tests
// +build js_chaos_tests

package server

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	chaosConsumerTestsClusterName = "CONSUMERS_CHAOS_TEST"
	chaosConsumerTestsStreamName  = "CONSUMER_CHAOS_TEST_STREAM"
	chaosConsumerTestsSubject     = "foo"
	chaosConsumerTestsDebug       = false
)

// Creates stream and fills it with the given number of messages.
// Each message is the string representation of the stream sequence number,
// e.g. the first message (seqno: 1) contains data "1".
// This allows consumers to verify the content of each message without tracking additional state
func createStreamForConsumerChaosTest(t *testing.T, c *cluster, replicas, numMessages int) {
	t.Helper()

	const publishBatchSize = 1_000

	pubNc, pubJs := jsClientConnectCluster(t, c)
	defer pubNc.Close()

	_, err := pubJs.AddStream(&nats.StreamConfig{
		Name:     chaosConsumerTestsStreamName,
		Subjects: []string{chaosConsumerTestsSubject},
		Replicas: replicas,
	})
	if err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}

	ackFutures := make([]nats.PubAckFuture, 0, publishBatchSize)

	for i := 1; i <= numMessages; i++ {
		message := []byte(fmt.Sprintf("%d", i))
		pubAckFuture, err := pubJs.PublishAsync(chaosConsumerTestsSubject, message, nats.ExpectLastSequence(uint64(i-1)))
		if err != nil {
			t.Fatalf("Publish error: %s", err)
		}
		ackFutures = append(ackFutures, pubAckFuture)

		if (i > 0 && i%publishBatchSize == 0) || i == numMessages {
			select {
			case <-pubJs.PublishAsyncComplete():
				for _, pubAckFuture := range ackFutures {
					select {
					case <-pubAckFuture.Ok():
						// Noop
					case pubAckErr := <-pubAckFuture.Err():
						t.Fatalf("Error publishing: %s", pubAckErr)
					case <-time.After(30 * time.Second):
						t.Fatalf("Timeout verifying pubAck for message: %s", pubAckFuture.Msg().Data)
					}
				}
				ackFutures = make([]nats.PubAckFuture, 0, publishBatchSize)
				t.Logf("Published %d/%d messages", i, numMessages)

			case <-time.After(30 * time.Second):
				t.Fatalf("Publish timed out")
			}
		}
	}
}

// Verify ordered delivery despite cluster-wide outages
func TestJetStreamChaosConsumerOrdered(t *testing.T) {

	const numMessages = 30_000
	const maxRetries = 100
	const retryDelay = 500 * time.Millisecond
	const fetchTimeout = 250 * time.Millisecond
	const clusterSize = 3
	const replicas = 3

	c := createJetStreamClusterExplicit(t, chaosConsumerTestsClusterName, clusterSize)
	defer c.shutdown()

	createStreamForConsumerChaosTest(t, c, replicas, numMessages)

	chaos := createClusterChaosMonkeyController(
		t,
		c,
		&clusterBouncerChaosMonkey{
			minDowntime:    0 * time.Second,
			maxDowntime:    2 * time.Second,
			minDownServers: clusterSize, // Whole cluster outage
			maxDownServers: clusterSize,
			pause:          1 * time.Second,
		},
	)

	subNc, subJs := jsClientConnectCluster(t, c)
	defer subNc.Close()

	sub, err := subJs.SubscribeSync(
		chaosConsumerTestsSubject,
		nats.OrderedConsumer(),
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	if chaosConsumerTestsDebug {
		t.Logf("Initial subscription: %s", toIndentedJsonString(sub))
	}

	chaos.start()
	defer chaos.stop()

	for i := 1; i <= numMessages; i++ {
		var msg *nats.Msg
		var nextMsgErr error
		var expectedMsgData = []byte(fmt.Sprintf("%d", i))

	nextMsgRetryLoop:
		for r := 0; r <= maxRetries; r++ {
			msg, nextMsgErr = sub.NextMsg(fetchTimeout)
			if nextMsgErr == nil {
				break nextMsgRetryLoop
			} else if r == maxRetries {
				t.Fatalf("Exceeded max retries for NextMsg")
			} else if nextMsgErr == nats.ErrBadSubscription {
				t.Fatalf("Subscription is invalid: %s", toIndentedJsonString(sub))
			} else {
				time.Sleep(retryDelay)
			}
		}

		metadata, err := msg.Metadata()
		if err != nil {
			t.Fatalf("Failed to get message metadata: %v", err)
		}

		if metadata.Sequence.Stream != uint64(i) {
			t.Fatalf("Expecting stream sequence %d, got %d instead", i, metadata.Sequence.Stream)
		}

		if !bytes.Equal(msg.Data, expectedMsgData) {
			t.Fatalf("Expecting message %s, got %s instead", expectedMsgData, msg.Data)
		}

		// Simulate application processing (and gives the monkey some time to brew chaos)
		time.Sleep(10 * time.Millisecond)

		if i%1000 == 0 {
			t.Logf("Consumed %d/%d", i, numMessages)
		}
	}
}

// Verify ordered delivery despite cluster-wide outages
func TestJetStreamChaosConsumerAsync(t *testing.T) {

	const numMessages = 30_000
	const timeout = 30 * time.Second // No (new) messages for 30s => terminate
	const maxRetries = 25
	const retryDelay = 500 * time.Millisecond
	const clusterSize = 3
	const replicas = 3

	c := createJetStreamClusterExplicit(t, chaosConsumerTestsClusterName, clusterSize)
	defer c.shutdown()

	createStreamForConsumerChaosTest(t, c, replicas, numMessages)

	chaos := createClusterChaosMonkeyController(
		t,
		c,
		&clusterBouncerChaosMonkey{
			minDowntime:    0 * time.Second,
			maxDowntime:    2 * time.Second,
			minDownServers: clusterSize,
			maxDownServers: clusterSize,
			pause:          2 * time.Second,
		},
	)

	subNc, subJs := jsClientConnectCluster(t, c)
	defer subNc.Close()

	timeoutTimer := time.NewTimer(timeout)
	deliveryCount := uint64(0)
	received := NewBitset(numMessages)

	handleMsg := func(msg *nats.Msg) {
		deliveryCount += 1

		metadata, err := msg.Metadata()
		if err != nil {
			t.Fatalf("Failed to get message metadata: %v", err)
		}
		seq := metadata.Sequence.Stream

		var expectedMsgData = []byte(fmt.Sprintf("%d", seq))
		if !bytes.Equal(msg.Data, expectedMsgData) {
			t.Fatalf("Expecting message content '%s', got '%s' instead", expectedMsgData, msg.Data)
		}

		isDupe := received.get(seq - 1)

		if isDupe {
			if chaosConsumerTestsDebug {
				t.Logf("Duplicate message delivery, seq: %d", seq)
			}
			return
		}

		// Mark this sequence as received
		received.set(seq-1, true)
		if received.count() < numMessages {
			// Reset timeout
			timeoutTimer.Reset(timeout)
		} else {
			// All received, speed up the shutdown
			timeoutTimer.Reset(1 * time.Second)
		}

		if received.count()%1000 == 0 {
			t.Logf("Consumed %d/%d", received.count(), numMessages)
		}

		// Simulate application processing (and gives the monkey some time to brew chaos)
		time.Sleep(10 * time.Millisecond)

	ackRetryLoop:
		for i := 0; i <= maxRetries; i++ {
			ackErr := msg.Ack()
			if ackErr == nil {
				break ackRetryLoop
			} else if i == maxRetries {
				t.Fatalf("Failed to ACK message %d (retried %d times)", seq, maxRetries)
			} else {
				time.Sleep(retryDelay)
			}
		}
	}

	subOpts := []nats.SubOpt{}
	sub, err := subJs.Subscribe(chaosConsumerTestsSubject, handleMsg, subOpts...)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	chaos.start()
	defer chaos.stop()

	// Wait for long enough silence.
	// Either a stall, or all messages received
	<-timeoutTimer.C

	// Shut down consumer
	sub.Unsubscribe()

	uniqueDeliveredCount := received.count()

	t.Logf(
		"Delivered %d/%d messages %d duplicate deliveries",
		uniqueDeliveredCount,
		numMessages,
		deliveryCount-uniqueDeliveredCount,
	)

	if uniqueDeliveredCount != numMessages {
		t.Fatalf("No new message delivered in the last %s, %d/%d messages never delivered", timeout, numMessages-uniqueDeliveredCount, numMessages)
	}
}

// Verify durable consumer retains state despite cluster-wide outages
// The consumer connection is also periodically closed, and the consumer 'resumes' on a different one
func TestJetStreamChaosConsumerDurable(t *testing.T) {

	const numMessages = 30_000
	const timeout = 30 * time.Second // No (new) messages for 60s => terminate
	const clusterSize = 3
	const replicas = 3
	const maxRetries = 25
	const retryDelay = 500 * time.Millisecond
	const durableConsumerName = "durable"

	c := createJetStreamClusterExplicit(t, chaosConsumerTestsClusterName, clusterSize)
	defer c.shutdown()

	createStreamForConsumerChaosTest(t, c, replicas, numMessages)

	chaos := createClusterChaosMonkeyController(
		t,
		c,
		&clusterBouncerChaosMonkey{
			minDowntime:    0 * time.Second,
			maxDowntime:    2 * time.Second,
			minDownServers: 1,
			maxDownServers: clusterSize,
			pause:          3 * time.Second,
		},
	)

	var nc *nats.Conn
	var sub *nats.Subscription
	var subLock sync.Mutex

	var handleMsgFun func(msg *nats.Msg)
	var natsURL string

	{
		var sb strings.Builder
		for _, s := range c.servers {
			sb.WriteString(s.ClientURL())
			sb.WriteString(",")
		}
		natsURL = sb.String()
	}

	resetDurableConsumer := func() {
		subLock.Lock()
		defer subLock.Unlock()

		if nc != nil {
			nc.Close()
		}

		var newNc *nats.Conn
	connectRetryLoop:
		for r := 0; r <= maxRetries; r++ {
			var connErr error
			newNc, connErr = nats.Connect(natsURL)
			if connErr == nil {
				break connectRetryLoop
			} else if r == maxRetries {
				t.Fatalf("Failed to connect, exceeded max retries, last error: %s", connErr)
			} else {
				time.Sleep(retryDelay)
			}
		}

		var newJs nats.JetStreamContext
	jsRetryLoop:
		for r := 0; r <= maxRetries; r++ {
			var jsErr error
			newJs, jsErr = newNc.JetStream(nats.MaxWait(10 * time.Second))
			if jsErr == nil {
				break jsRetryLoop
			} else if r == maxRetries {
				t.Fatalf("Failed to get JS, exceeded max retries, last error: %s", jsErr)
			} else {
				time.Sleep(retryDelay)
			}
		}

		subOpts := []nats.SubOpt{
			nats.Durable(durableConsumerName),
		}

		var newSub *nats.Subscription
	subscribeRetryLoop:
		for i := 0; i <= maxRetries; i++ {
			var subErr error
			newSub, subErr = newJs.Subscribe(chaosConsumerTestsSubject, handleMsgFun, subOpts...)
			if subErr == nil {
				ci, err := newJs.ConsumerInfo(chaosConsumerTestsStreamName, durableConsumerName)
				if err == nil {
					if chaosConsumerTestsDebug {
						t.Logf("Consumer info:\n %s", toIndentedJsonString(ci))
					}
				} else {
					t.Logf("Failed to retrieve consumer info: %s", err)
				}

				break subscribeRetryLoop
			} else if i == maxRetries {
				t.Fatalf("Exceeded max retries creating subscription: %v", subErr)
			} else {
				time.Sleep(retryDelay)
			}
		}

		nc, sub = newNc, newSub
	}

	timeoutTimer := time.NewTimer(timeout)
	deliveryCount := uint64(0)
	received := NewBitset(numMessages)

	handleMsgFun = func(msg *nats.Msg) {

		subLock.Lock()
		if msg.Sub != sub {
			// Message from a previous instance of durable consumer, drop
			defer subLock.Unlock()
			return
		}
		subLock.Unlock()

		deliveryCount += 1

		metadata, err := msg.Metadata()
		if err != nil {
			t.Fatalf("Failed to get message metadata: %v", err)
		}
		seq := metadata.Sequence.Stream

		var expectedMsgData = []byte(fmt.Sprintf("%d", seq))
		if !bytes.Equal(msg.Data, expectedMsgData) {
			t.Fatalf("Expecting message content '%s', got '%s' instead", expectedMsgData, msg.Data)
		}

		isDupe := received.get(seq - 1)

		if isDupe {
			if chaosConsumerTestsDebug {
				t.Logf("Duplicate message delivery, seq: %d", seq)
			}
			return
		}

		// Mark this sequence as received
		received.set(seq-1, true)
		if received.count() < numMessages {
			// Reset timeout
			timeoutTimer.Reset(timeout)
		} else {
			// All received, speed up the shutdown
			timeoutTimer.Reset(1 * time.Second)
		}

		// Simulate application processing (and gives the monkey some time to brew chaos)
		time.Sleep(10 * time.Millisecond)

	ackRetryLoop:
		for i := 0; i <= maxRetries; i++ {
			ackErr := msg.Ack()
			if ackErr == nil {
				break ackRetryLoop
			} else if i == maxRetries {
				t.Fatalf("Failed to ACK message %d (retried %d times)", seq, maxRetries)
			} else {
				time.Sleep(retryDelay)
			}
		}

		if received.count()%1000 == 0 {
			t.Logf("Consumed %d/%d, duplicate deliveries: %d", received.count(), numMessages, deliveryCount-received.count())
			// Close connection and resume consuming on a different one
			resetDurableConsumer()
		}
	}

	resetDurableConsumer()

	chaos.start()
	defer chaos.stop()

	// Wait for long enough silence.
	// Either a stall, or all messages received
	<-timeoutTimer.C

	// Shut down consumer
	if sub != nil {
		sub.Unsubscribe()
	}

	uniqueDeliveredCount := received.count()

	t.Logf(
		"Delivered %d/%d messages %d duplicate deliveries",
		uniqueDeliveredCount,
		numMessages,
		deliveryCount-uniqueDeliveredCount,
	)

	if uniqueDeliveredCount != numMessages {
		t.Fatalf("No new message delivered in the last %s, %d/%d messages never delivered", timeout, numMessages-uniqueDeliveredCount, numMessages)
	}
}

func TestJetStreamChaosConsumerPull(t *testing.T) {

	const numMessages = 10_000
	const maxRetries = 100
	const retryDelay = 500 * time.Millisecond
	const fetchTimeout = 250 * time.Millisecond
	const fetchBatchSize = 100
	const clusterSize = 3
	const replicas = 3
	const durableConsumerName = "durable"

	c := createJetStreamClusterExplicit(t, chaosConsumerTestsClusterName, clusterSize)
	defer c.shutdown()

	createStreamForConsumerChaosTest(t, c, replicas, numMessages)

	chaos := createClusterChaosMonkeyController(
		t,
		c,
		&clusterBouncerChaosMonkey{
			minDowntime:    0 * time.Second,
			maxDowntime:    2 * time.Second,
			minDownServers: clusterSize, // Whole cluster outage
			maxDownServers: clusterSize,
			pause:          1 * time.Second,
		},
	)

	subNc, subJs := jsClientConnectCluster(t, c)
	defer subNc.Close()

	sub, err := subJs.PullSubscribe(
		chaosConsumerTestsSubject,
		durableConsumerName,
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	if chaosConsumerTestsDebug {
		t.Logf("Initial subscription: %s", toIndentedJsonString(sub))
	}

	chaos.start()
	defer chaos.stop()

	fetchMaxWait := nats.MaxWait(fetchTimeout)
	received := NewBitset(numMessages)
	deliveredCount := uint64(0)

	for received.count() < numMessages {

		var msgs []*nats.Msg
		var fetchErr error

	fetchRetryLoop:
		for r := 0; r <= maxRetries; r++ {
			msgs, fetchErr = sub.Fetch(fetchBatchSize, fetchMaxWait)
			if fetchErr == nil {
				break fetchRetryLoop
			} else if r == maxRetries {
				t.Fatalf("Exceeded max retries for Fetch, last error: %s", fetchErr)
			} else if fetchErr == nats.ErrBadSubscription {
				t.Fatalf("Subscription is invalid: %s", toIndentedJsonString(sub))
			} else {
				// t.Logf("Fetch error: %v", fetchErr)
				time.Sleep(retryDelay)
			}
		}

		for _, msg := range msgs {

			deliveredCount += 1

			metadata, err := msg.Metadata()
			if err != nil {
				t.Fatalf("Failed to get message metadata: %v", err)
			}

			streamSeq := metadata.Sequence.Stream

			expectedMsgData := []byte(fmt.Sprintf("%d", streamSeq))

			if !bytes.Equal(msg.Data, expectedMsgData) {
				t.Fatalf("Expecting message %s, got %s instead", expectedMsgData, msg.Data)
			}

			isDupe := received.get(streamSeq - 1)

			received.set(streamSeq-1, true)

			// Simulate application processing (and gives the monkey some time to brew chaos)
			time.Sleep(10 * time.Millisecond)

		ackRetryLoop:
			for r := 0; r <= maxRetries; r++ {
				ackErr := msg.Ack()
				if ackErr == nil {
					break ackRetryLoop
				} else if r == maxRetries {
					t.Fatalf("Failed to ACK message %d, last error: %s", streamSeq, ackErr)
				} else {
					time.Sleep(retryDelay)
				}
			}

			if !isDupe && received.count()%1000 == 0 {
				t.Logf("Consumed %d/%d (duplicates: %d)", received.count(), numMessages, deliveredCount-received.count())
			}
		}
	}
}
