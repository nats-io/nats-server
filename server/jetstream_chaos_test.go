// Copyright 2023 The NATS Authors
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
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Support functions for "chaos" testing (random injected failures)

type ChaosMonkeyController interface {
	// Launch the monkey as background routine and return
	start()
	// Stop a monkey that was previously started
	stop()
	// Run the monkey synchronously, until it is manually stopped via stopCh
	run()
}

type ClusterChaosMonkey interface {
	// Set defaults and validates the monkey parameters
	validate(t *testing.T, c *cluster)
	// Run the monkey synchronously, until it is manually stopped via stopCh
	run(t *testing.T, c *cluster, stopCh <-chan bool)
}

// Chaos Monkey Controller that acts on a cluster
type clusterChaosMonkeyController struct {
	t       *testing.T
	cluster *cluster
	wg      sync.WaitGroup
	stopCh  chan bool
	ccm     ClusterChaosMonkey
}

func createClusterChaosMonkeyController(t *testing.T, c *cluster, ccm ClusterChaosMonkey) ChaosMonkeyController {
	ccm.validate(t, c)
	return &clusterChaosMonkeyController{
		t:       t,
		cluster: c,
		stopCh:  make(chan bool, 3),
		ccm:     ccm,
	}
}

func (m *clusterChaosMonkeyController) start() {
	m.t.Logf("🐵 Starting monkey")
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.run()
	}()
}

func (m *clusterChaosMonkeyController) stop() {
	m.t.Logf("🐵 Stopping monkey")
	m.stopCh <- true
	m.wg.Wait()
	m.t.Logf("🐵 Monkey stopped")
}

func (m *clusterChaosMonkeyController) run() {
	m.ccm.run(m.t, m.cluster, m.stopCh)
}

// Cluster Chaos Monkey that selects a random subset of the nodes in a cluster (according to min/max provided),
// shuts them down for a given duration (according to min/max provided), then brings them back up.
// Then sleeps for a given time, and does it again until stopped.
type clusterBouncerChaosMonkey struct {
	minDowntime    time.Duration
	maxDowntime    time.Duration
	minDownServers int
	maxDownServers int
	pause          time.Duration
}

func (m *clusterBouncerChaosMonkey) validate(t *testing.T, c *cluster) {
	if m.minDowntime > m.maxDowntime {
		t.Fatalf("Min downtime %v cannot be larger than max downtime %v", m.minDowntime, m.maxDowntime)
	}

	if m.minDownServers > m.maxDownServers {
		t.Fatalf("Min down servers %v cannot be larger than max down servers %v", m.minDownServers, m.maxDownServers)
	}
}

func (m *clusterBouncerChaosMonkey) run(t *testing.T, c *cluster, stopCh <-chan bool) {
	for {
		// Pause between actions
		select {
		case <-stopCh:
			return
		case <-time.After(m.pause):
		}

		// Pick a random subset of servers
		numServersDown := rand.Intn(1+m.maxDownServers-m.minDownServers) + m.minDownServers
		servers := c.selectRandomServers(numServersDown)
		serverNames := []string{}
		for _, s := range servers {
			serverNames = append(serverNames, s.info.Name)
		}

		// Pick a random outage interval
		minOutageNanos := m.minDowntime.Nanoseconds()
		maxOutageNanos := m.maxDowntime.Nanoseconds()
		outageDurationNanos := rand.Int63n(1+maxOutageNanos-minOutageNanos) + minOutageNanos
		outageDuration := time.Duration(outageDurationNanos)

		// Take down selected servers
		t.Logf("🐵 Taking down %d/%d servers for %v (%v)", numServersDown, len(c.servers), outageDuration, serverNames)
		c.stopSubset(servers)

		// Wait for the "outage" duration
		select {
		case <-stopCh:
			return
		case <-time.After(outageDuration):
		}

		// Restart servers and wait for cluster to be healthy
		t.Logf("🐵 Restoring cluster")
		c.restartAllSamePorts()
		c.waitOnClusterHealthz()

		c.waitOnClusterReady()
		c.waitOnAllCurrent()
		c.waitOnLeader()
	}
}

// Additional cluster methods for chaos testing

func (c *cluster) waitOnClusterHealthz() {
	c.t.Helper()
	for _, cs := range c.servers {
		c.waitOnServerHealthz(cs)
	}
}

func (c *cluster) stopSubset(toStop []*Server) {
	c.t.Helper()
	for _, s := range toStop {
		s.Shutdown()
	}
}

func (c *cluster) selectRandomServers(numServers int) []*Server {
	c.t.Helper()
	if numServers > len(c.servers) {
		panic(fmt.Sprintf("Can't select %d servers in a cluster of %d", numServers, len(c.servers)))
	}
	var selectedServers []*Server
	selectedServers = append(selectedServers, c.servers...)
	rand.Shuffle(len(selectedServers), func(x, y int) {
		selectedServers[x], selectedServers[y] = selectedServers[y], selectedServers[x]
	})
	return selectedServers[0:numServers]
}

// Other helpers

func jsClientConnectCluster(t testing.TB, c *cluster) (*nats.Conn, nats.JetStreamContext) {
	serverConnectURLs := make([]string, len(c.servers))
	for i, server := range c.servers {
		serverConnectURLs[i] = server.ClientURL()
	}
	connectURL := strings.Join(serverConnectURLs, ",")

	nc, err := nats.Connect(connectURL)
	if err != nil {
		t.Fatalf("Failed to connect: %s", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Failed to init JetStream context: %s", err)
	}

	return nc, js
}

func toIndentedJsonString(v any) string {
	s, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(s)
}

// Bounces the entire set of nodes, then brings them back up.
// Fail if some nodes don't come back online.
func TestJetStreamChaosClusterBounce(t *testing.T) {

	const duration = 60 * time.Second
	const clusterSize = 3

	c := createJetStreamClusterExplicit(t, "R3", clusterSize)
	defer c.shutdown()

	chaos := createClusterChaosMonkeyController(
		t,
		c,
		&clusterBouncerChaosMonkey{
			minDowntime:    0 * time.Second,
			maxDowntime:    2 * time.Second,
			minDownServers: clusterSize,
			maxDownServers: clusterSize,
			pause:          3 * time.Second,
		},
	)
	chaos.start()
	defer chaos.stop()

	<-time.After(duration)
}

// Bounces a subset of the nodes, then brings them back up.
// Fails if some nodes don't come back online.
func TestJetStreamChaosClusterBounceSubset(t *testing.T) {

	const duration = 60 * time.Second
	const clusterSize = 3

	c := createJetStreamClusterExplicit(t, "R3", clusterSize)
	defer c.shutdown()

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
	chaos.start()
	defer chaos.stop()

	<-time.After(duration)
}

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

	const numMessages = 5_000
	const numBatch = 500
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

		if i%numBatch == 0 {
			t.Logf("Consumed %d/%d", i, numMessages)
		}
	}
}

// Verify ordered delivery despite cluster-wide outages
func TestJetStreamChaosConsumerAsync(t *testing.T) {

	const numMessages = 5_000
	const numBatch = 500
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

		if received.count()%numBatch == 0 {
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

	const numMessages = 5_000
	const numBatch = 500
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

		if received.count()%numBatch == 0 {
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

	const numMessages = 5_000
	const numBatch = 500
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

			if !isDupe && received.count()%numBatch == 0 {
				t.Logf("Consumed %d/%d (duplicates: %d)", received.count(), numMessages, deliveredCount-received.count())
			}
		}
	}
}

const (
	chaosKvTestsClusterName = "KV_CHAOS_TEST"
	chaosKvTestsBucketName  = "KV_CHAOS_TEST_BUCKET"
	chaosKvTestsSubject     = "foo"
	chaosKvTestsDebug       = false
)

// Creates KV store (a.k.a. bucket).
func createBucketForKvChaosTest(t *testing.T, c *cluster, replicas int) {
	t.Helper()

	pubNc, pubJs := jsClientConnectCluster(t, c)
	defer pubNc.Close()

	config := nats.KeyValueConfig{
		Bucket:      chaosKvTestsBucketName,
		Replicas:    replicas,
		Description: "Test bucket",
	}

	kvs, err := pubJs.CreateKeyValue(&config)
	if err != nil {
		t.Fatalf("Error creating bucket: %v", err)
	}

	status, err := kvs.Status()
	if err != nil {
		t.Fatalf("Error retrieving bucket status: %v", err)
	}
	t.Logf("Bucket created: %s", status.Bucket())
}

// Single client performs a set of PUT on a single key.
// If PUT is successful, perform a GET on the same key.
// If GET is successful, ensure key revision and value match the most recent successful write.
func TestJetStreamChaosKvPutGet(t *testing.T) {

	const numOps = 100_000
	const clusterSize = 3
	const replicas = 3
	const key = "key"
	const staleReadsOk = true // Set to false to check for violations of 'read committed' consistency

	c := createJetStreamClusterExplicit(t, chaosKvTestsClusterName, clusterSize)
	defer c.shutdown()

	createBucketForKvChaosTest(t, c, replicas)

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

	nc, js := jsClientConnectCluster(t, c)
	defer nc.Close()

	// Create KV bucket
	kv, err := js.KeyValue(chaosKvTestsBucketName)
	if err != nil {
		t.Fatalf("Failed to get KV store: %v", err)
	}

	// Initialize the only key
	firstRevision, err := kv.Create(key, []byte("INITIAL VALUE"))
	if err != nil {
		t.Fatalf("Failed to create key: %v", err)
	} else if firstRevision != 1 {
		t.Fatalf("Unexpected revision: %d", firstRevision)
	}

	// Start chaos
	chaos.start()
	defer chaos.stop()

	staleReadsCount := uint64(0)
	successCount := uint64(0)

	previousRevision := firstRevision

putGetLoop:
	for i := 1; i <= numOps; i++ {

		if i%1000 == 0 {
			t.Logf("Completed %d/%d PUT+GET operations", i, numOps)
		}

		// PUT a value
		putValue := fmt.Sprintf("value-%d", i)
		putRevision, err := kv.Put(key, []byte(putValue))
		if err != nil {
			t.Logf("PUT error: %v", err)
			continue putGetLoop
		}

		// Check revision is monotonically increasing
		if putRevision <= previousRevision {
			t.Fatalf("PUT produced revision %d which is not greater than the previous successful PUT revision: %d", putRevision, previousRevision)
		}

		previousRevision = putRevision

		// If PUT was successful, GET the same
		kve, err := kv.Get(key)
		if err == nats.ErrKeyNotFound {
			t.Fatalf("GET key not found, but key does exists (last PUT revision: %d)", putRevision)
		} else if err != nil {
			t.Logf("GET error: %v", err)
			continue putGetLoop
		}

		getValue := string(kve.Value())
		getRevision := kve.Revision()

		if putRevision > getRevision {
			// Stale read, violates 'read committed' consistency criteria
			if !staleReadsOk {
				t.Fatalf("PUT value %s (rev: %d) then read value %s (rev: %d)", putValue, putRevision, getValue, getRevision)
			} else {
				staleReadsCount += 1
			}
		} else if putRevision < getRevision {
			// Returned revision is higher than any ever written, this should never happen
			t.Fatalf("GET returned revision %d, but most recent expected revision is %d", getRevision, putRevision)
		} else if putValue != getValue {
			// Returned revision matches latest, but values do not, this should never happen
			t.Fatalf("GET returned revision %d with value %s, but value %s was just committed for that same revision", getRevision, getValue, putValue)
		} else {
			// Get returned the latest revision/value
			successCount += 1
			if chaosKvTestsDebug {
				t.Logf("PUT+GET %s=%s (rev: %d)", key, putValue, putRevision)
			}
		}
	}

	t.Logf("Completed %d PUT+GET cycles of which %d successful, %d GETs returned a stale value", numOps, successCount, staleReadsCount)
}

// A variant TestJetStreamChaosKvPutGet where PUT is retried until successful, and GET is retried until it returns the latest known key revision.
// This validates than a confirmed PUT value is never lost, and becomes eventually visible.
func TestJetStreamChaosKvPutGetWithRetries(t *testing.T) {

	const numOps = 10_000
	const maxRetries = 20
	const retryDelay = 100 * time.Millisecond
	const clusterSize = 3
	const replicas = 3
	const key = "key"

	c := createJetStreamClusterExplicit(t, chaosKvTestsClusterName, clusterSize)
	defer c.shutdown()

	createBucketForKvChaosTest(t, c, replicas)

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

	nc, js := jsClientConnectCluster(t, c)
	defer nc.Close()

	kv, err := js.KeyValue(chaosKvTestsBucketName)
	if err != nil {
		t.Fatalf("Failed to get KV store: %v", err)
	}

	// Initialize key value
	firstRevision, err := kv.Create(key, []byte("INITIAL VALUE"))
	if err != nil {
		t.Fatalf("Failed to create key: %v", err)
	} else if firstRevision != 1 {
		t.Fatalf("Unexpected revision: %d", firstRevision)
	}

	// Start chaos
	chaos.start()
	defer chaos.stop()

	staleReadCount := 0
	previousRevision := firstRevision

putGetLoop:
	for i := 1; i <= numOps; i++ {

		if i%1000 == 0 {
			t.Logf("Completed %d/%d PUT+GET operations", i, numOps)
		}

		putValue := fmt.Sprintf("value-%d", i)
		putRevision := uint64(0)

		// Put new value for key, retry until successful or out of retries
	putRetryLoop:
		for r := 0; r <= maxRetries; r++ {
			var putErr error
			putRevision, putErr = kv.Put(key, []byte(putValue))
			if putErr == nil {
				break putRetryLoop
			} else if r == maxRetries {
				t.Fatalf("Failed to PUT (retried %d times): %v", maxRetries, putErr)
			} else {
				if chaosKvTestsDebug {
					t.Logf("PUT error: %v", putErr)
				}
				time.Sleep(retryDelay)
			}
		}

		// Ensure key version is monotonically increasing
		if putRevision <= previousRevision {
			t.Fatalf("Latest PUT created revision %d which is not greater than the previous revision: %d", putRevision, previousRevision)
		}
		previousRevision = putRevision

		// Read value for key, retry until successful, and validate corresponding version and value
	getRetryLoop:
		for r := 0; r <= maxRetries; r++ {
			var getErr error
			kve, getErr := kv.Get(key)
			if getErr != nil && r == maxRetries {
				t.Fatalf("Failed to GET (retried %d times): %v", maxRetries, getErr)
			} else if getErr != nil {
				if chaosKvTestsDebug {
					t.Logf("GET error: %v", getErr)
				}
				time.Sleep(retryDelay)
				continue getRetryLoop
			}

			// GET successful, check revision and value
			getValue := string(kve.Value())
			getRevision := kve.Revision()

			if putRevision == getRevision {
				if putValue != getValue {
					t.Fatalf("Unexpected value %s for revision %d, expected: %s", getValue, getRevision, putValue)
				}
				if chaosKvTestsDebug {
					t.Logf("PUT+GET %s=%s (rev: %d) (retry: %d)", key, putValue, putRevision, r)
				}
				continue putGetLoop
			} else if getRevision > putRevision {
				t.Fatalf("GET returned version that should not exist yet: %d, last created: %d", getRevision, putRevision)
			} else { // get revision < put revision
				staleReadCount += 1
				if chaosKvTestsDebug {
					t.Logf("GET got stale value: %v (rev: %d, latest: %d)", getValue, getRevision, putRevision)
				}
				time.Sleep(retryDelay)
				continue getRetryLoop
			}
		}
	}

	t.Logf("Client completed %d PUT+GET cycles, %d GET returned a stale value", numOps, staleReadCount)
}

// Multiple clients updating a finite set of keys with CAS semantics.
// TODO check that revision is never lower than last one seen
// TODO check that KeyNotFound is never returned, as keys are initialized beforehand
func TestJetStreamChaosKvCAS(t *testing.T) {
	const numOps = 10_000
	const maxRetries = 50
	const retryDelay = 300 * time.Millisecond
	const clusterSize = 3
	const replicas = 3
	const numKeys = 15
	const numClients = 5

	c := createJetStreamClusterExplicit(t, chaosKvTestsClusterName, clusterSize)
	defer c.shutdown()

	createBucketForKvChaosTest(t, c, replicas)

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

	nc, js := jsClientConnectCluster(t, c)
	defer nc.Close()

	// Create bucket
	kv, err := js.KeyValue(chaosKvTestsBucketName)
	if err != nil {
		t.Fatalf("Failed to get KV store: %v", err)
	}

	// Create set of keys and initialize them with dummy value
	keys := make([]string, numKeys)
	for k := 0; k < numKeys; k++ {
		key := fmt.Sprintf("key-%d", k)
		keys[k] = key

		_, err := kv.Create(key, []byte("Initial value"))
		if err != nil {
			t.Fatalf("Failed to create key: %v", err)
		}
	}

	wgStart := sync.WaitGroup{}
	wgComplete := sync.WaitGroup{}

	// Client routine
	client := func(clientId int, kv nats.KeyValue) {
		defer wgComplete.Done()

		rng := rand.New(rand.NewSource(int64(clientId)))
		successfulUpdates := 0
		casRejectUpdates := 0
		otherUpdateErrors := 0

		// Map to track last known revision for each of the keys
		knownRevisions := map[string]uint64{}
		for _, key := range keys {
			knownRevisions[key] = 0
		}

		// Wait for all clients to reach this point before proceeding
		wgStart.Done()
		wgStart.Wait()

		for i := 1; i <= numOps; i++ {

			if i%1000 == 0 {
				t.Logf("Client %d completed %d/%d updates", clientId, i, numOps)
			}

			// Pick random key from the set
			key := keys[rng.Intn(numKeys)]

			// Prepare unique value to be written
			value := fmt.Sprintf("client: %d operation %d", clientId, i)

			// Try to update a key with CAS
			newRevision, updateErr := kv.Update(key, []byte(value), knownRevisions[key])
			if updateErr == nil {
				// Update successful
				knownRevisions[key] = newRevision
				successfulUpdates += 1
				if chaosKvTestsDebug {
					t.Logf("Client %d updated key %s, new revision: %d", clientId, key, newRevision)
				}
			} else if updateErr != nil && strings.Contains(fmt.Sprint(updateErr), "wrong last sequence") {
				// CAS rejected update, learn current revision for this key
				casRejectUpdates += 1

				for r := 0; r <= maxRetries; r++ {
					kve, getErr := kv.Get(key)
					if getErr == nil {
						currentRevision := kve.Revision()
						if currentRevision < knownRevisions[key] {
							// Revision number moved backward, this should never happen
							t.Fatalf("Current revision for key %s is %d, which is lower than the last known revision %d", key, currentRevision, knownRevisions[key])

						}

						knownRevisions[key] = currentRevision
						if chaosKvTestsDebug {
							t.Logf("Client %d learn key %s revision: %d", clientId, key, currentRevision)
						}
						break
					} else if r == maxRetries {
						t.Fatalf("Failed to GET (retried %d times): %v", maxRetries, getErr)
					} else {
						time.Sleep(retryDelay)
					}
				}
			} else {
				// Other update error
				otherUpdateErrors += 1
				if chaosKvTestsDebug {
					t.Logf("Client %d update error for key %s: %v", clientId, key, updateErr)
				}
				time.Sleep(retryDelay)
			}
		}
		t.Logf("Client %d done, %d kv updates, %d CAS rejected, %d other errors", clientId, successfulUpdates, casRejectUpdates, otherUpdateErrors)
	}

	// Launch all clients
	for i := 1; i <= numClients; i++ {
		cNc, cJs := jsClientConnectCluster(t, c)
		defer cNc.Close()

		cKv, err := cJs.KeyValue(chaosKvTestsBucketName)
		if err != nil {
			t.Fatalf("Failed to get KV store: %v", err)
		}

		wgStart.Add(1)
		wgComplete.Add(1)
		go client(i, cKv)
	}

	// Wait for clients to be connected and ready
	wgStart.Wait()

	// Start failures
	chaos.start()
	defer chaos.stop()

	// Wait for all clients to be done
	wgComplete.Wait()
}
