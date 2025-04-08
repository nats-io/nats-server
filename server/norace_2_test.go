// Copyright 2018-2025 The NATS Authors
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

//go:build !race && !skip_no_race_tests && !skip_no_race_2_tests

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	crand "crypto/rand"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

// IMPORTANT: Tests in this file are not executed when running with the -race flag.
//            The test name should be prefixed with TestNoRace so we can run only
//            those tests: go test -run=TestNoRace ...

// ConsumerInfo seems to being called quite a bit more than we had anticipated.
// Under certain circumstances, since we reset num pending, this can be very costly.
// We will use the fast path to alleviate that performance bottleneck but also make
// sure we are still being accurate.
func TestNoRaceJetStreamClusterConsumerInfoSpeed(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	c.waitOnLeader()
	server := c.randomNonLeader()

	nc, js := jsClientConnect(t, server)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"events.>"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// The issue is compounded when we have lots of different subjects captured
	// by a terminal fwc. The consumer will have a terminal pwc.
	// Here make all subjects unique.

	sub, err := js.PullSubscribe("events.*", "DLC")
	require_NoError(t, err)

	toSend := 250_000
	for i := 0; i < toSend; i++ {
		subj := fmt.Sprintf("events.%d", i+1)
		js.PublishAsync(subj, []byte("ok"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkNumPending := func(expected int) {
		t.Helper()
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			start := time.Now()
			ci, err := js.ConsumerInfo("TEST", "DLC")
			if err != nil {
				return err
			}
			// Make sure these are fast now.
			if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
				return fmt.Errorf("ConsumerInfo took too long: %v", elapsed)
			}
			// Make sure pending == expected.
			if ci.NumPending != uint64(expected) {
				return fmt.Errorf("Expected %d NumPending, got %d", expected, ci.NumPending)
			}
			return nil
		})
	}
	// Make sure in simple case it is correct.
	checkNumPending(toSend)

	// Do a few acks.
	toAck := 25
	for _, m := range fetchMsgs(t, sub, 25, time.Second) {
		err = m.AckSync()
		require_NoError(t, err)
	}
	checkNumPending(toSend - toAck)

	// Now do a purge such that we only keep so many.
	// We want to make sure we do the right thing here and have correct calculations.
	toKeep := 100_000
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Keep: uint64(toKeep)})
	require_NoError(t, err)

	checkNumPending(toKeep)
}

func TestNoRaceJetStreamKVAccountWithServerRestarts(t *testing.T) {
	// Uncomment to run. Needs fast machine to not time out on KeyValue lookup.
	skip(t)

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:   "TEST",
		Replicas: 3,
	})
	require_NoError(t, err)

	npubs := 10_000
	par := 8
	iter := 2
	nsubjs := 250

	wg := sync.WaitGroup{}
	putKeys := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()
			kv, err := js.KeyValue("TEST")
			require_NoError(t, err)

			for i := 0; i < npubs; i++ {
				subj := fmt.Sprintf("KEY-%d", rand.Intn(nsubjs))
				if _, err := kv.PutString(subj, "hello"); err != nil {
					nc, js := jsClientConnect(t, c.randomServer())
					defer nc.Close()
					kv, err = js.KeyValue("TEST")
					require_NoError(t, err)
				}
			}
		}()
	}

	restartServers := func() {
		time.Sleep(2 * time.Second)
		// Rotate through and restart the servers.
		for _, server := range c.servers {
			server.Shutdown()
			restarted := c.restartServer(server)
			checkFor(t, time.Second, 200*time.Millisecond, func() error {
				hs := restarted.healthz(&HealthzOptions{
					JSMetaOnly: true,
				})
				if hs.Error != _EMPTY_ {
					return errors.New(hs.Error)
				}
				return nil
			})
		}
		c.waitOnLeader()
		c.waitOnStreamLeader(globalAccountName, "KV_TEST")
	}

	for n := 0; n < iter; n++ {
		for i := 0; i < par; i++ {
			putKeys()
		}
		restartServers()
	}
	wg.Wait()

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	si, err := js.StreamInfo("KV_TEST")
	require_NoError(t, err)
	require_True(t, si.State.NumSubjects == uint64(nsubjs))
}

// Test for consumer create when the subject cardinality is high and the
// consumer is filtered with a wildcard that forces linear scans.
// We have an optimization to use in memory structures in filestore to speed up.
// Only if asking to scan all (DeliverAll).
func TestNoRaceJetStreamConsumerCreateTimeNumPending(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"events.>"},
	})
	require_NoError(t, err)

	n := 500_000
	msg := bytes.Repeat([]byte("X"), 8*1024)

	for i := 0; i < n; i++ {
		subj := fmt.Sprintf("events.%d", rand.Intn(100_000))
		js.PublishAsync(subj, msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
	}

	// Should stay under 5ms now, but for Travis variability say 50ms.
	threshold := 50 * time.Millisecond

	start := time.Now()
	_, err = js.PullSubscribe("events.*", "dlc")
	require_NoError(t, err)
	if elapsed := time.Since(start); elapsed > threshold {
		t.Skipf("Consumer create took longer than expected, %v vs %v", elapsed, threshold)
	}

	start = time.Now()
	_, err = js.PullSubscribe("events.99999", "xxx")
	require_NoError(t, err)
	if elapsed := time.Since(start); elapsed > threshold {
		t.Skipf("Consumer create took longer than expected, %v vs %v", elapsed, threshold)
	}

	start = time.Now()
	_, err = js.PullSubscribe(">", "zzz")
	require_NoError(t, err)
	if elapsed := time.Since(start); elapsed > threshold {
		t.Skipf("Consumer create took longer than expected, %v vs %v", elapsed, threshold)
	}
}

func TestNoRaceJetStreamClusterGhostConsumers(t *testing.T) {
	consumerNotActiveStartInterval = time.Second
	consumerNotActiveMaxInterval = time.Second
	defer func() {
		consumerNotActiveStartInterval = defaultConsumerNotActiveStartInterval
		consumerNotActiveMaxInterval = defaultConsumerNotActiveMaxInterval
	}()

	c := createJetStreamClusterExplicit(t, "GHOST", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"events.>"},
		Replicas: 3,
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			require_NoError(t, nc.Publish(fmt.Sprintf("events.%d.%d", i, j), []byte(`test`)))
		}
	}

	fetch := func(id int) {
		subject := fmt.Sprintf("events.%d.*", id)
		subscription, err := js.PullSubscribe(subject,
			_EMPTY_, // ephemeral consumer
			nats.DeliverAll(),
			nats.ReplayInstant(),
			nats.BindStream("TEST"),
			nats.ConsumerReplicas(1),
			nats.ConsumerMemoryStorage(),
		)
		if err != nil {
			return
		}
		defer subscription.Unsubscribe()

		info, err := subscription.ConsumerInfo()
		if err != nil {
			return
		}

		subscription.Fetch(int(info.NumPending))
	}

	replay := func(ctx context.Context, id int) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				fetch(id)
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	go replay(ctx, 0)
	go replay(ctx, 1)
	go replay(ctx, 2)
	go replay(ctx, 3)
	go replay(ctx, 4)
	go replay(ctx, 5)
	go replay(ctx, 6)
	go replay(ctx, 7)
	go replay(ctx, 8)
	go replay(ctx, 9)

	time.Sleep(5 * time.Second)

	for _, server := range c.servers {
		server.Shutdown()
		restarted := c.restartServer(server)
		checkFor(t, time.Second, 200*time.Millisecond, func() error {
			hs := restarted.healthz(&HealthzOptions{
				JSMetaOnly: true,
			})
			if hs.Error != _EMPTY_ {
				return errors.New(hs.Error)
			}
			return nil
		})
		c.waitOnStreamLeader(globalAccountName, "TEST")
		time.Sleep(time.Second * 2)
		go replay(ctx, 5)
		go replay(ctx, 6)
		go replay(ctx, 7)
		go replay(ctx, 8)
		go replay(ctx, 9)
	}

	time.Sleep(5 * time.Second)
	cancel()

	// Check we don't report missing consumers.
	subj := fmt.Sprintf(JSApiConsumerListT, "TEST")
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		// Request will take at most 4 seconds if some consumers can't be found.
		m, err := nc.Request(subj, nil, 5*time.Second)
		if err != nil {
			return err
		}
		var resp JSApiConsumerListResponse
		require_NoError(t, json.Unmarshal(m.Data, &resp))
		if len(resp.Missing) == 0 {
			return nil
		}
		return fmt.Errorf("Still have missing: %+v", resp.Missing)
	})

	// Also check all servers agree on the available consumer assignments.
	// It could be the above check passes, i.e. our meta leader thinks all is okay, but other servers actually drifted.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		var previousConsumers []string
		for _, s := range c.servers {
			sjs := s.getJetStream()
			sjs.mu.Lock()
			cc := sjs.cluster
			sa := cc.streams[globalAccountName]["TEST"]
			var consumers []string
			for cName := range sa.consumers {
				consumers = append(consumers, cName)
			}
			sjs.mu.Unlock()
			slices.Sort(consumers)
			if previousConsumers != nil && !slices.Equal(previousConsumers, consumers) {
				return fmt.Errorf("Consumer mismatch:\n- previous: %v\n- actual  : %v\n", previousConsumers, consumers)
			}
			previousConsumers = consumers
		}
		return nil
	})
}

// This is to test a publish slowdown and general instability experienced in a setup similar to this.
// We have feeder streams that are all sourced to an aggregate stream. All streams are interest retention.
// We want to monitor the avg publish time for the sync publishers to the feeder streams, the ingest rate to
// the aggregate stream, and general health of the consumers on the aggregate stream.
// Target publish rate is ~2k/s with publish time being ~40-60ms but remaining stable.
// We can also simulate max redeliveries that create interior deletes in streams.
func TestNoRaceJetStreamClusterF3Setup(t *testing.T) {
	// Uncomment to run. Needs to be on a pretty big machine. Do not want as part of Travis tests atm.
	skip(t)

	// These and the settings below achieve ~60ms pub time on avg and ~2k msgs per sec inbound to the aggregate stream.
	// On my machine though.
	np := clusterProxy{
		rtt:  2 * time.Millisecond,
		up:   1 * 1024 * 1024 * 1024, // 1gbit
		down: 1 * 1024 * 1024 * 1024, // 1gbit
	}

	// Test params.
	numSourceStreams := 20
	numConsumersPerSource := 1
	numPullersPerConsumer := 50
	numPublishers := 100
	setHighStartSequence := false
	simulateMaxRedeliveries := false
	maxBadPubTimes := uint32(20)
	badPubThresh := 500 * time.Millisecond
	testTime := 5 * time.Minute // make sure to do --timeout=65m

	t.Logf("Starting Test: Total Test Time %v", testTime)

	c := createJetStreamClusterWithNetProxy(t, "R3S", 3, &np)
	defer c.shutdown()

	// Do some quick sanity checking for latency stuff.
	{
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:      "TEST",
			Replicas:  3,
			Subjects:  []string{"foo"},
			Retention: nats.InterestPolicy,
		})
		require_NoError(t, err)
		defer js.DeleteStream("TEST")

		sl := c.streamLeader(globalAccountName, "TEST")
		nc, js = jsClientConnect(t, sl)
		defer nc.Close()
		start := time.Now()
		_, err = js.Publish("foo", []byte("hello"))
		require_NoError(t, err)
		// This is best case, and with client connection being close to free, this should be at least > rtt
		if elapsed := time.Since(start); elapsed < np.rtt {
			t.Fatalf("Expected publish time to be > %v, got %v", np.rtt, elapsed)
		}

		nl := c.randomNonStreamLeader(globalAccountName, "TEST")
		nc, js = jsClientConnect(t, nl)
		defer nc.Close()
		start = time.Now()
		_, err = js.Publish("foo", []byte("hello"))
		require_NoError(t, err)
		// This is worst case, meaning message has to travel to leader, then to fastest replica, then back.
		// So should be at 3x rtt, so check at least > 2x rtt.
		if elapsed := time.Since(start); elapsed < 2*np.rtt {
			t.Fatalf("Expected publish time to be > %v, got %v", 2*np.rtt, elapsed)
		}
	}

	// Setup source streams.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	t.Logf("Creating %d Source Streams", numSourceStreams)

	var sources []string
	wg := sync.WaitGroup{}
	for i := 0; i < numSourceStreams; i++ {
		sname := fmt.Sprintf("EVENT-%s", nuid.Next())
		sources = append(sources, sname)
		wg.Add(1)
		go func(stream string) {
			defer wg.Done()
			t.Logf("  %q", stream)
			subj := fmt.Sprintf("%s.>", stream)
			_, err := js.AddStream(&nats.StreamConfig{
				Name:      stream,
				Subjects:  []string{subj},
				Replicas:  3,
				Retention: nats.InterestPolicy,
			})
			require_NoError(t, err)
			for j := 0; j < numConsumersPerSource; j++ {
				consumer := fmt.Sprintf("C%d", j)
				_, err := js.Subscribe(_EMPTY_, func(msg *nats.Msg) {
					msg.Ack()
				}, nats.BindStream(stream), nats.Durable(consumer), nats.ManualAck())
				require_NoError(t, err)
			}
		}(sname)
	}
	wg.Wait()

	var streamSources []*nats.StreamSource
	for _, src := range sources {
		streamSources = append(streamSources, &nats.StreamSource{Name: src})

	}

	t.Log("Creating Aggregate Stream")

	// Now create the aggregate stream.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "EVENTS",
		Replicas:  3,
		Retention: nats.InterestPolicy,
		Sources:   streamSources,
	})
	require_NoError(t, err)

	// Set first sequence to a high number.
	if setHighStartSequence {
		require_NoError(t, js.PurgeStream("EVENTS", &nats.StreamPurgeRequest{Sequence: 32_000_001}))
	}

	// Now create 2 pull consumers.
	_, err = js.PullSubscribe(_EMPTY_, "C1",
		nats.BindStream("EVENTS"),
		nats.MaxDeliver(1),
		nats.AckWait(10*time.Second),
		nats.ManualAck(),
	)
	require_NoError(t, err)

	_, err = js.PullSubscribe(_EMPTY_, "C2",
		nats.BindStream("EVENTS"),
		nats.MaxDeliver(1),
		nats.AckWait(10*time.Second),
		nats.ManualAck(),
	)
	require_NoError(t, err)

	t.Logf("Creating %d x 2 Pull Subscribers", numPullersPerConsumer)

	// Now create the pullers.
	for _, subName := range []string{"C1", "C2"} {
		for i := 0; i < numPullersPerConsumer; i++ {
			go func(subName string) {
				nc, js := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				sub, err := js.PullSubscribe(_EMPTY_, subName,
					nats.BindStream("EVENTS"),
					nats.MaxDeliver(1),
					nats.AckWait(10*time.Second),
					nats.ManualAck(),
				)
				require_NoError(t, err)

				for {
					msgs, err := sub.Fetch(25, nats.MaxWait(2*time.Second))
					if err != nil && err != nats.ErrTimeout {
						t.Logf("Exiting pull subscriber %q: %v", subName, err)
						return
					}
					// Shuffle
					rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })

					// Wait for a random interval up to 100ms.
					time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

					for _, m := range msgs {
						// If we want to simulate max redeliveries being hit, since not acking
						// once will cause it due to subscriber setup.
						// 100_000 == 0.01%
						if simulateMaxRedeliveries && rand.Intn(100_000) == 0 {
							md, err := m.Metadata()
							require_NoError(t, err)
							t.Logf("** Skipping Ack: %d **", md.Sequence.Stream)
						} else {
							m.Ack()
						}
					}
				}
			}(subName)
		}
	}

	// Now create feeder publishers.
	eventTypes := []string{"PAYMENT", "SUBMISSION", "CANCEL"}

	msg := make([]byte, 2*1024) // 2k payload
	crand.Read(msg)

	// For tracking pub times.
	var pubs int
	var totalPubTime time.Duration
	var pmu sync.Mutex
	last := time.Now()

	updatePubStats := func(elapsed time.Duration) {
		pmu.Lock()
		defer pmu.Unlock()
		// Reset every 5s
		if time.Since(last) > 5*time.Second {
			pubs = 0
			totalPubTime = 0
			last = time.Now()
		}
		pubs++
		totalPubTime += elapsed
	}
	avgPubTime := func() time.Duration {
		pmu.Lock()
		np := pubs
		tpt := totalPubTime
		pmu.Unlock()
		return tpt / time.Duration(np)
	}

	t.Logf("Creating %d Publishers", numPublishers)

	var numLimitsExceeded atomic.Uint32
	errCh := make(chan error, 100)

	for i := 0; i < numPublishers; i++ {
		go func() {
			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			for {
				// Grab a random source stream
				stream := sources[rand.Intn(len(sources))]
				// Grab random event type.
				evt := eventTypes[rand.Intn(len(eventTypes))]
				subj := fmt.Sprintf("%s.%s", stream, evt)
				start := time.Now()
				_, err := js.Publish(subj, msg)
				if err != nil {
					t.Logf("Exiting publisher: %v", err)
					return
				}
				elapsed := time.Since(start)
				if elapsed > badPubThresh {
					t.Logf("Publish time took more than expected: %v", elapsed)
					numLimitsExceeded.Add(1)
					if ne := numLimitsExceeded.Load(); ne > maxBadPubTimes {
						errCh <- fmt.Errorf("Too many exceeded times on publish: %d", ne)
						return
					}
				}
				updatePubStats(elapsed)
			}
		}()
	}

	t.Log("Creating Monitoring Routine - Data in ~10s")

	// Create monitoring routine.
	go func() {
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		fseq, lseq := uint64(0), uint64(0)
		for {
			// Grab consumers
			var minAckFloor uint64 = math.MaxUint64
			for _, consumer := range []string{"C1", "C2"} {
				ci, err := js.ConsumerInfo("EVENTS", consumer)
				if err != nil {
					t.Logf("Exiting Monitor: %v", err)
					return
				}
				if lseq > 0 {
					t.Logf("%s:\n  Delivered:\t%d\n  AckFloor:\t%d\n  AckPending:\t%d\n  NumPending:\t%d",
						consumer, ci.Delivered.Stream, ci.AckFloor.Stream, ci.NumAckPending, ci.NumPending)
				}
				if ci.AckFloor.Stream < minAckFloor {
					minAckFloor = ci.AckFloor.Stream
				}
			}
			// Now grab aggregate stream state.
			si, err := js.StreamInfo("EVENTS")
			if err != nil {
				t.Logf("Exiting Monitor: %v", err)
				return
			}
			state := si.State
			if lseq != 0 {
				t.Logf("Stream:\n  Msgs: \t%d\n  First:\t%d\n  Last: \t%d\n  Deletes:\t%d\n",
					state.Msgs, state.FirstSeq, state.LastSeq, state.NumDeleted)
				t.Logf("Publish Stats:\n  Msgs/s:\t%0.2f\n  Avg Pub:\t%v\n\n", float64(si.State.LastSeq-lseq)/5.0, avgPubTime())
				if si.State.FirstSeq < minAckFloor && si.State.FirstSeq == fseq {
					t.Log("Stream first seq < minimum ack floor")
				}
			}
			fseq, lseq = si.State.FirstSeq, si.State.LastSeq
			time.Sleep(5 * time.Second)
		}

	}()

	select {
	case e := <-errCh:
		t.Fatal(e)
	case <-time.After(testTime):
		t.Fatalf("Did not receive completion signal")
	}
}

// Unbalanced stretch cluster.
// S2 (stream leader) will have a slow path to S1 (via proxy) and S3 (consumer leader) will have a fast path.
//
//	 Route Ports
//		"S1": 14622
//		"S2": 15622
//		"S3": 16622
func createStretchUnbalancedCluster(t testing.TB) (c *cluster, np *netProxy) {
	t.Helper()

	tmpl := `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
		name: "F3"
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
	`
	// Do these in order, S1, S2 (proxy) then S3.
	c = &cluster{t: t, servers: make([]*Server, 3), opts: make([]*Options, 3), name: "F3"}

	// S1
	conf := fmt.Sprintf(tmpl, "S1", t.TempDir(), 14622, "route://127.0.0.1:15622, route://127.0.0.1:16622")
	c.servers[0], c.opts[0] = RunServerWithConfig(createConfFile(t, []byte(conf)))

	// S2
	// Create the proxy first. Connect this to S1. Make it slow, e.g. 5ms RTT.
	np = createNetProxy(1*time.Millisecond, 1024*1024*1024, 1024*1024*1024, "route://127.0.0.1:14622", true)
	routes := fmt.Sprintf("%s, route://127.0.0.1:16622", np.routeURL())
	conf = fmt.Sprintf(tmpl, "S2", t.TempDir(), 15622, routes)
	c.servers[1], c.opts[1] = RunServerWithConfig(createConfFile(t, []byte(conf)))

	// S3
	conf = fmt.Sprintf(tmpl, "S3", t.TempDir(), 16622, "route://127.0.0.1:14622, route://127.0.0.1:15622")
	c.servers[2], c.opts[2] = RunServerWithConfig(createConfFile(t, []byte(conf)))

	c.checkClusterFormed()
	c.waitOnClusterReady()

	return c, np
}

// We test an interest based stream that has a cluster with a node with asymmetric paths from
// the stream leader and the consumer leader such that the consumer leader path is fast and
// replicated acks arrive sooner then the actual message. This path was considered, but also
// categorized as very rare and was expensive as it tried to forward a new stream msg delete
// proposal to the original stream leader. It now will deal with the issue locally and not
// slow down the ingest rate to the stream's publishers.
func TestNoRaceJetStreamClusterDifferentRTTInterestBasedStreamSetup(t *testing.T) {
	// Uncomment to run. Do not want as part of Travis tests atm.
	skip(t)

	c, np := createStretchUnbalancedCluster(t)
	defer c.shutdown()
	defer np.stop()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Now create the stream.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "EVENTS",
		Subjects:  []string{"EV.>"},
		Replicas:  3,
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	// Make sure it's leader is on S2.
	sl := c.servers[1]
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnStreamLeader(globalAccountName, "EVENTS")
		if s := c.streamLeader(globalAccountName, "EVENTS"); s != sl {
			s.JetStreamStepdownStream(globalAccountName, "EVENTS")
			return fmt.Errorf("Server %s is not stream leader yet", sl)
		}
		return nil
	})

	// Now create the consumer.
	_, err = js.PullSubscribe(_EMPTY_, "C", nats.BindStream("EVENTS"), nats.ManualAck())
	require_NoError(t, err)

	// Make sure the consumer leader is on S3.
	cl := c.servers[2]
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnConsumerLeader(globalAccountName, "EVENTS", "C")
		if s := c.consumerLeader(globalAccountName, "EVENTS", "C"); s != cl {
			s.JetStreamStepdownConsumer(globalAccountName, "EVENTS", "C")
			return fmt.Errorf("Server %s is not consumer leader yet", cl)
		}
		return nil
	})

	go func(js nats.JetStream) {
		sub, err := js.PullSubscribe(_EMPTY_, "C", nats.BindStream("EVENTS"), nats.ManualAck())
		require_NoError(t, err)

		for {
			msgs, err := sub.Fetch(100, nats.MaxWait(2*time.Second))
			if err != nil && err != nats.ErrTimeout {
				return
			}
			// Shuffle
			rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
			for _, m := range msgs {
				m.Ack()
			}
		}
	}(js)

	numPublishers := 25
	pubThresh := 2 * time.Second
	var maxExceeded atomic.Int64
	errCh := make(chan error, numPublishers)
	wg := sync.WaitGroup{}

	msg := make([]byte, 2*1024) // 2k payload
	crand.Read(msg)

	// Publishers.
	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(iter int) {
			defer wg.Done()

			// Connect to random, the slow ones will be connected to the slow node.
			// But if you connect them all there it will pass.
			s := c.randomServer()
			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			for i := 0; i < 1_000; i++ {
				start := time.Now()
				_, err := js.Publish("EV.PAID", msg)
				if err != nil {
					errCh <- fmt.Errorf("Publish error: %v", err)
					return
				}
				if elapsed := time.Since(start); elapsed > pubThresh {
					errCh <- fmt.Errorf("Publish time exceeded")
					if int64(elapsed) > maxExceeded.Load() {
						maxExceeded.Store(int64(elapsed))
					}
					return
				}
			}
		}(i)
	}

	wg.Wait()

	select {
	case e := <-errCh:
		t.Fatalf("%v: threshold is %v, maximum seen: %v", e, pubThresh, time.Duration(maxExceeded.Load()))
	default:
	}
}

func TestNoRaceJetStreamInterestStreamCheckInterestRaceBug(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Retention: nats.InterestPolicy,
		Storage:   nats.MemoryStorage,
	})
	require_NoError(t, err)

	numConsumers := 10
	for i := 0; i < numConsumers; i++ {
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err = js.Subscribe("foo", func(m *nats.Msg) {
			m.Ack()
		}, nats.Durable(fmt.Sprintf("C%d", i)), nats.ManualAck())
		require_NoError(t, err)
	}

	numToSend := 10_000
	for i := 0; i < numToSend; i++ {
		_, err := js.PublishAsync("foo", nil, nats.StallWait(800*time.Millisecond))
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(20 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Put this into a function so that the defer lets go of that server's stream lock
	// when we have finished checking, otherwise the loop in checkFor() ends up holding
	// all of the stream locks across all servers, wedging things.
	checkForServer := func(s *Server) error {
		mset, err := s.GlobalAccount().lookupStream("TEST")
		require_NoError(t, err)
		require_Equal(t, mset.numConsumers(), numConsumers)

		mset.mu.RLock()
		defer mset.mu.RUnlock()

		if mset.lseq < uint64(numToSend) {
			return fmt.Errorf("waiting for %d messages in stream (%d so far)", numToSend, mset.lseq)
		}

		for _, o := range mset.consumers {
			state, err := o.store.State()
			require_NoError(t, err)
			if state.AckFloor.Stream != uint64(numToSend) {
				return fmt.Errorf("Ackfloor not correct yet (%d != %d)", state.AckFloor.Stream, numToSend)
			}
		}

		state := mset.state()
		if state.Msgs != 0 {
			return fmt.Errorf("too many messages: %d", state.Msgs)
		} else if state.FirstSeq != uint64(numToSend+1) {
			return fmt.Errorf("wrong FirstSeq: %d, expected: %d", state.FirstSeq, numToSend+1)
		}

		return nil
	}

	// Wait til ackfloor is correct for all consumers.
	checkFor(t, 30*time.Second, 250*time.Millisecond, func() error {
		for _, s := range c.servers {
			if err := checkForServer(s); err != nil {
				return err
			}
		}
		return nil
	})
}

func TestNoRaceJetStreamClusterInterestStreamConsistencyAfterRollingRestart(t *testing.T) {
	// Uncomment to run. Needs to be on a big machine. Do not want as part of Travis tests atm.
	skip(t)

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	numStreams := 200
	numConsumersPer := 5
	numPublishers := 10

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	qch := make(chan bool)

	var mm sync.Mutex
	ackMap := make(map[string]map[uint64][]string)

	addAckTracking := func(seq uint64, stream, consumer string) {
		mm.Lock()
		defer mm.Unlock()
		sam := ackMap[stream]
		if sam == nil {
			sam = make(map[uint64][]string)
			ackMap[stream] = sam
		}
		sam[seq] = append(sam[seq], consumer)
	}

	doPullSubscriber := func(stream, consumer, filter string) {
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		var err error
		var sub *nats.Subscription
		timeout := time.Now().Add(5 * time.Second)
		for time.Now().Before(timeout) {
			sub, err = js.PullSubscribe(filter, consumer, nats.BindStream(stream), nats.ManualAck())
			if err == nil {
				break
			}
		}
		if err != nil {
			t.Logf("Error on pull subscriber: %v", err)
			return
		}

		for {
			select {
			case <-time.After(500 * time.Millisecond):
				msgs, err := sub.Fetch(100, nats.MaxWait(time.Second))
				if err != nil {
					continue
				}
				// Shuffle
				rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
				for _, m := range msgs {
					meta, err := m.Metadata()
					require_NoError(t, err)
					m.Ack()
					addAckTracking(meta.Sequence.Stream, stream, consumer)
					if meta.NumDelivered > 1 {
						t.Logf("Got a msg redelivered %d for sequence %d on %q %q\n", meta.NumDelivered, meta.Sequence.Stream, stream, consumer)
					}
				}
			case <-qch:
				nc.Flush()
				return
			}
		}
	}

	// Setup
	wg := sync.WaitGroup{}
	for i := 0; i < numStreams; i++ {
		wg.Add(1)
		go func(stream string) {
			defer wg.Done()
			subj := fmt.Sprintf("%s.>", stream)
			_, err := js.AddStream(&nats.StreamConfig{
				Name:      stream,
				Subjects:  []string{subj},
				Replicas:  3,
				Retention: nats.InterestPolicy,
			})
			require_NoError(t, err)
			for i := 0; i < numConsumersPer; i++ {
				consumer := fmt.Sprintf("C%d", i)
				filter := fmt.Sprintf("%s.%d", stream, i)
				_, err = js.AddConsumer(stream, &nats.ConsumerConfig{
					Durable:       consumer,
					FilterSubject: filter,
					AckPolicy:     nats.AckExplicitPolicy,
					AckWait:       2 * time.Second,
				})
				require_NoError(t, err)
				c.waitOnConsumerLeader(globalAccountName, stream, consumer)
				go doPullSubscriber(stream, consumer, filter)
			}
		}(fmt.Sprintf("A-%d", i))
	}
	wg.Wait()

	msg := make([]byte, 2*1024) // 2k payload
	crand.Read(msg)

	// Controls if publishing is on or off.
	var pubActive atomic.Bool

	doPublish := func() {
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		for {
			select {
			case <-time.After(100 * time.Millisecond):
				if pubActive.Load() {
					for i := 0; i < numStreams; i++ {
						for j := 0; j < numConsumersPer; j++ {
							subj := fmt.Sprintf("A-%d.%d", i, j)
							// Don't care about errors here for this test.
							js.Publish(subj, msg)
						}
					}
				}
			case <-qch:
				return
			}
		}
	}

	pubActive.Store(true)

	for i := 0; i < numPublishers; i++ {
		go doPublish()
	}

	// Let run for a bit.
	time.Sleep(20 * time.Second)

	// Do a rolling restart.
	for _, s := range c.servers {
		t.Logf("Shutdown %v\n", s)
		s.Shutdown()
		s.WaitForShutdown()
		time.Sleep(20 * time.Second)
		t.Logf("Restarting %v\n", s)
		s = c.restartServer(s)
		c.waitOnServerHealthz(s)
	}

	// Let run for a bit longer.
	time.Sleep(10 * time.Second)

	// Stop pubs.
	pubActive.Store(false)

	// Let settle.
	time.Sleep(10 * time.Second)
	close(qch)
	time.Sleep(20 * time.Second)

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	minAckFloor := func(stream string) (uint64, string) {
		var maf uint64 = math.MaxUint64
		var consumer string
		for i := 0; i < numConsumersPer; i++ {
			cname := fmt.Sprintf("C%d", i)
			ci, err := js.ConsumerInfo(stream, cname)
			require_NoError(t, err)
			if ci.AckFloor.Stream < maf {
				maf = ci.AckFloor.Stream
				consumer = cname
			}
		}
		return maf, consumer
	}

	checkStreamAcks := func(stream string) {
		mm.Lock()
		defer mm.Unlock()
		if sam := ackMap[stream]; sam != nil {
			for seq := 1; ; seq++ {
				acks := sam[uint64(seq)]
				if acks == nil {
					if sam[uint64(seq+1)] != nil {
						t.Logf("Missing an ack on stream %q for sequence %d\n", stream, seq)
					} else {
						break
					}
				}
				if len(acks) > 1 {
					t.Logf("Multiple acks for %d which is not expected: %+v", seq, acks)
				}
			}
		}
	}

	// Now check all streams such that their first sequence is equal to the minimum of all consumers.
	for i := 0; i < numStreams; i++ {
		stream := fmt.Sprintf("A-%d", i)
		si, err := js.StreamInfo(stream)
		require_NoError(t, err)

		if maf, consumer := minAckFloor(stream); maf > si.State.FirstSeq {
			t.Logf("\nBAD STATE DETECTED FOR %q, CHECKING OTHER SERVERS! ACK %d vs %+v LEADER %v, CL FOR %q %v\n",
				stream, maf, si.State, c.streamLeader(globalAccountName, stream), consumer, c.consumerLeader(globalAccountName, stream, consumer))

			t.Logf("TEST ACKS %+v\n", ackMap)

			checkStreamAcks(stream)

			for _, s := range c.servers {
				mset, err := s.GlobalAccount().lookupStream(stream)
				require_NoError(t, err)
				state := mset.state()
				t.Logf("Server %v Stream STATE %+v\n", s, state)

				var smv StoreMsg
				if sm, err := mset.store.LoadMsg(state.FirstSeq, &smv); err == nil {
					t.Logf("Subject for msg %d is %q", state.FirstSeq, sm.subj)
				} else {
					t.Logf("Could not retrieve msg for %d: %v", state.FirstSeq, err)
				}

				if len(mset.preAcks) > 0 {
					t.Logf("%v preAcks %+v\n", s, mset.preAcks)
				}

				for _, o := range mset.consumers {
					ostate, err := o.store.State()
					require_NoError(t, err)
					t.Logf("Consumer STATE for %q is %+v\n", o.name, ostate)
				}
			}
			t.Fatalf("BAD STATE: ACKFLOOR > FIRST %d vs %d\n", maf, si.State.FirstSeq)
		}
	}
}

func TestNoRaceFileStoreNumPending(t *testing.T) {
	// No need for all permutations here.
	storeDir := t.TempDir()
	fcfg := FileStoreConfig{
		StoreDir:  storeDir,
		BlockSize: 2 * 1024, // Create many blocks on purpose.
	}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"*.*.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	tokens := []string{"foo", "bar", "baz"}
	genSubj := func() string {
		return fmt.Sprintf("%s.%s.%s.%s",
			tokens[rand.Intn(len(tokens))],
			tokens[rand.Intn(len(tokens))],
			tokens[rand.Intn(len(tokens))],
			tokens[rand.Intn(len(tokens))],
		)
	}

	for i := 0; i < 50_000; i++ {
		subj := genSubj()
		_, _, err := fs.StoreMsg(subj, nil, []byte("Hello World"), 0)
		require_NoError(t, err)
	}

	state := fs.State()

	// Scan one by one for sanity check against other calculations.
	sanityCheck := func(sseq uint64, filter string) SimpleState {
		t.Helper()
		var ss SimpleState
		var smv StoreMsg
		// For here we know 0 is invalid, set to 1.
		if sseq == 0 {
			sseq = 1
		}
		for seq := sseq; seq <= state.LastSeq; seq++ {
			sm, err := fs.LoadMsg(seq, &smv)
			if err != nil {
				t.Logf("Encountered error %v loading sequence: %d", err, seq)
				continue
			}
			if subjectIsSubsetMatch(sm.subj, filter) {
				ss.Msgs++
				ss.Last = seq
				if ss.First == 0 || seq < ss.First {
					ss.First = seq
				}
			}
		}
		return ss
	}

	check := func(sseq uint64, filter string) {
		t.Helper()
		np, lvs := fs.NumPending(sseq, filter, false)
		ss := fs.FilteredState(sseq, filter)
		sss := sanityCheck(sseq, filter)
		if lvs != state.LastSeq {
			t.Fatalf("Expected NumPending to return valid through last of %d but got %d", state.LastSeq, lvs)
		}
		if ss.Msgs != np {
			t.Fatalf("NumPending of %d did not match ss.Msgs of %d", np, ss.Msgs)
		}
		if ss != sss {
			t.Fatalf("Failed sanity check, expected %+v got %+v", sss, ss)
		}
	}

	sanityCheckLastOnly := func(sseq uint64, filter string) SimpleState {
		t.Helper()
		var ss SimpleState
		var smv StoreMsg
		// For here we know 0 is invalid, set to 1.
		if sseq == 0 {
			sseq = 1
		}
		seen := make(map[string]bool)
		for seq := state.LastSeq; seq >= sseq; seq-- {
			sm, err := fs.LoadMsg(seq, &smv)
			if err != nil {
				t.Logf("Encountered error %v loading sequence: %d", err, seq)
				continue
			}
			if !seen[sm.subj] && subjectIsSubsetMatch(sm.subj, filter) {
				ss.Msgs++
				if ss.Last == 0 {
					ss.Last = seq
				}
				if ss.First == 0 || seq < ss.First {
					ss.First = seq
				}
				seen[sm.subj] = true
			}
		}
		return ss
	}

	checkLastOnly := func(sseq uint64, filter string) {
		t.Helper()
		np, lvs := fs.NumPending(sseq, filter, true)
		ss := sanityCheckLastOnly(sseq, filter)
		if lvs != state.LastSeq {
			t.Fatalf("Expected NumPending to return valid through last of %d but got %d", state.LastSeq, lvs)
		}
		if ss.Msgs != np {
			t.Fatalf("NumPending of %d did not match ss.Msgs of %d", np, ss.Msgs)
		}
	}

	startSeqs := []uint64{0, 1, 2, 200, 444, 555, 2222, 8888, 12_345, 28_222, 33_456, 44_400, 49_999}
	checkSubs := []string{"foo.>", "*.bar.>", "foo.bar.*.baz", "*.bar.>", "*.foo.bar.*", "foo.foo.bar.baz"}

	for _, filter := range checkSubs {
		for _, start := range startSeqs {
			check(start, filter)
			checkLastOnly(start, filter)
		}
	}
}

func TestNoRaceJetStreamClusterUnbalancedInterestMultipleConsumers(t *testing.T) {
	c, np := createStretchUnbalancedCluster(t)
	defer c.shutdown()
	defer np.stop()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Now create the stream.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "EVENTS",
		Subjects:  []string{"EV.>"},
		Replicas:  3,
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	// Make sure it's leader is on S2.
	sl := c.servers[1]
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnStreamLeader(globalAccountName, "EVENTS")
		if s := c.streamLeader(globalAccountName, "EVENTS"); s != sl {
			s.JetStreamStepdownStream(globalAccountName, "EVENTS")
			return fmt.Errorf("Server %s is not stream leader yet", sl)
		}
		return nil
	})

	// Create a fast ack consumer.
	_, err = js.Subscribe("EV.NEW", func(m *nats.Msg) {
		m.Ack()
	}, nats.Durable("C"), nats.ManualAck())
	require_NoError(t, err)

	// Make sure the consumer leader is on S3.
	cl := c.servers[2]
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnConsumerLeader(globalAccountName, "EVENTS", "C")
		if s := c.consumerLeader(globalAccountName, "EVENTS", "C"); s != cl {
			s.JetStreamStepdownConsumer(globalAccountName, "EVENTS", "C")
			return fmt.Errorf("Server %s is not consumer leader yet", cl)
		}
		return nil
	})

	// Connect a client directly to the stream leader.
	nc, js = jsClientConnect(t, sl)
	defer nc.Close()

	// Now create a pull subscriber.
	sub, err := js.PullSubscribe("EV.NEW", "D", nats.ManualAck())
	require_NoError(t, err)

	// Make sure this consumer leader is on S1.
	cl = c.servers[0]
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnConsumerLeader(globalAccountName, "EVENTS", "D")
		if s := c.consumerLeader(globalAccountName, "EVENTS", "D"); s != cl {
			s.JetStreamStepdownConsumer(globalAccountName, "EVENTS", "D")
			return fmt.Errorf("Server %s is not consumer leader yet", cl)
		}
		return nil
	})

	numToSend := 1000
	for i := 0; i < numToSend; i++ {
		_, err := js.PublishAsync("EV.NEW", nil)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(20 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Now make sure we can pull messages since we have not acked.
	// The bug is that the acks arrive on S1 faster then the messages but we want to
	// make sure we do not remove prematurely.
	msgs, err := sub.Fetch(100, nats.MaxWait(time.Second))
	require_NoError(t, err)
	require_Len(t, len(msgs), 100)
	for _, m := range msgs {
		m.AckSync()
	}

	ci, err := js.ConsumerInfo("EVENTS", "D")
	require_NoError(t, err)
	require_Equal(t, ci.NumPending, uint64(numToSend-100))
	require_Equal(t, ci.NumAckPending, 0)
	require_Equal(t, ci.Delivered.Stream, 100)
	require_Equal(t, ci.AckFloor.Stream, 100)

	// Check stream state on all servers.
	// Since acks result in messages to be removed through proposals,
	// it could take some time to be reflected in the stream state.
	checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
		for _, s := range c.servers {
			mset, err := s.GlobalAccount().lookupStream("EVENTS")
			if err != nil {
				return err
			}
			state := mset.state()
			if state.Msgs != 900 {
				return fmt.Errorf("expected state.Msgs=900, got %d", state.Msgs)
			}
			if state.FirstSeq != 101 {
				return fmt.Errorf("expected state.FirstSeq=101, got %d", state.FirstSeq)
			}
			if state.LastSeq != 1000 {
				return fmt.Errorf("expected state.LastSeq=1000, got %d", state.LastSeq)
			}
			if state.Consumers != 2 {
				return fmt.Errorf("expected state.Consumers=2, got %d", state.Consumers)
			}
		}
		return nil
	})

	msgs, err = sub.Fetch(900, nats.MaxWait(time.Second))
	require_NoError(t, err)
	require_Len(t, len(msgs), 900)
	for _, m := range msgs {
		m.AckSync()
	}

	// Let acks propagate.
	time.Sleep(250 * time.Millisecond)

	// Check final stream state on all servers.
	for _, s := range c.servers {
		mset, err := s.GlobalAccount().lookupStream("EVENTS")
		require_NoError(t, err)
		state := mset.state()
		require_Equal(t, state.Msgs, 0)
		require_Equal(t, state.FirstSeq, 1001)
		require_Equal(t, state.LastSeq, 1000)
		require_Equal(t, state.Consumers, 2)
		// Now check preAcks
		mset.mu.RLock()
		numPreAcks := len(mset.preAcks)
		mset.mu.RUnlock()
		require_Len(t, numPreAcks, 0)
	}
}

func TestNoRaceJetStreamClusterUnbalancedInterestMultipleFilteredConsumers(t *testing.T) {
	c, np := createStretchUnbalancedCluster(t)
	defer c.shutdown()
	defer np.stop()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Now create the stream.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "EVENTS",
		Subjects:  []string{"EV.>"},
		Replicas:  3,
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	// Make sure it's leader is on S2.
	sl := c.servers[1]
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnStreamLeader(globalAccountName, "EVENTS")
		if s := c.streamLeader(globalAccountName, "EVENTS"); s != sl {
			s.JetStreamStepdownStream(globalAccountName, "EVENTS")
			return fmt.Errorf("Server %s is not stream leader yet", sl)
		}
		return nil
	})

	// Create a fast ack consumer.
	_, err = js.Subscribe("EV.NEW", func(m *nats.Msg) {
		m.Ack()
	}, nats.Durable("C"), nats.ManualAck())
	require_NoError(t, err)

	// Make sure the consumer leader is on S3.
	cl := c.servers[2]
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnConsumerLeader(globalAccountName, "EVENTS", "C")
		if s := c.consumerLeader(globalAccountName, "EVENTS", "C"); s != cl {
			s.JetStreamStepdownConsumer(globalAccountName, "EVENTS", "C")
			return fmt.Errorf("Server %s is not consumer leader yet", cl)
		}
		return nil
	})

	// Connect a client directly to the stream leader.
	nc, js = jsClientConnect(t, sl)
	defer nc.Close()

	// Now create another fast ack consumer.
	_, err = js.Subscribe("EV.UPDATED", func(m *nats.Msg) {
		m.Ack()
	}, nats.Durable("D"), nats.ManualAck())
	require_NoError(t, err)

	// Make sure this consumer leader is on S1.
	cl = c.servers[0]
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnConsumerLeader(globalAccountName, "EVENTS", "D")
		if s := c.consumerLeader(globalAccountName, "EVENTS", "D"); s != cl {
			s.JetStreamStepdownConsumer(globalAccountName, "EVENTS", "D")
			return fmt.Errorf("Server %s is not consumer leader yet", cl)
		}
		return nil
	})

	numToSend := 500
	for i := 0; i < numToSend; i++ {
		_, err := js.PublishAsync("EV.NEW", nil)
		require_NoError(t, err)
		_, err = js.PublishAsync("EV.UPDATED", nil)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(20 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Let acks propagate.
	time.Sleep(250 * time.Millisecond)

	ci, err := js.ConsumerInfo("EVENTS", "D")
	require_NoError(t, err)
	require_Equal(t, ci.NumPending, 0)
	require_Equal(t, ci.NumAckPending, 0)
	require_Equal(t, ci.Delivered.Consumer, 500)
	require_Equal(t, ci.Delivered.Stream, 1000)
	require_Equal(t, ci.AckFloor.Consumer, 500)
	require_Equal(t, ci.AckFloor.Stream, 1000)

	// Check final stream state on all servers.
	for _, s := range c.servers {
		mset, err := s.GlobalAccount().lookupStream("EVENTS")
		require_NoError(t, err)
		state := mset.state()
		require_Equal(t, state.Msgs, 0)
		require_Equal(t, state.FirstSeq, 1001)
		require_Equal(t, state.LastSeq, 1000)
		require_Equal(t, state.Consumers, 2)
		// Now check preAcks
		mset.mu.RLock()
		numPreAcks := len(mset.preAcks)
		mset.mu.RUnlock()
		require_Len(t, numPreAcks, 0)
	}
}

func TestNoRaceParallelStreamAndConsumerCreation(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// stream config.
	scfg := &StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		MaxMsgs:  10,
		Storage:  FileStorage,
		Replicas: 1,
	}

	// Will do these direct against the low level API to really make
	// sure parallel creation ok.
	np := 1000
	startCh := make(chan bool)
	errCh := make(chan error, np)
	wg := sync.WaitGroup{}
	wg.Add(np)

	var streams sync.Map

	for i := 0; i < np; i++ {
		go func() {
			defer wg.Done()

			// Make them all fire at once.
			<-startCh

			if mset, err := s.GlobalAccount().addStream(scfg); err != nil {
				t.Logf("Stream create got an error: %v", err)
				errCh <- err
			} else {
				streams.Store(mset, true)
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)
	close(startCh)
	wg.Wait()

	// Check for no errors.
	if len(errCh) > 0 {
		t.Fatalf("Expected no errors, got %d: %v", len(errCh), <-errCh)
	}

	// Now make sure we really only created one stream.
	var numStreams int
	streams.Range(func(k, v any) bool {
		numStreams++
		return true
	})
	if numStreams > 1 {
		t.Fatalf("Expected only one stream to be really created, got %d out of %d attempts", numStreams, np)
	}

	// Also make sure we cleanup the inflight entries for streams.
	gacc := s.GlobalAccount()
	_, jsa, err := gacc.checkForJetStream()
	require_NoError(t, err)
	var numEntries int
	jsa.inflight.Range(func(k, v any) bool {
		numEntries++
		return true
	})
	if numEntries > 0 {
		t.Fatalf("Expected no inflight entries to be left over, got %d", numEntries)
	}

	// Now do consumers.
	mset, err := gacc.lookupStream("TEST")
	require_NoError(t, err)

	cfg := &ConsumerConfig{
		DeliverSubject: "to",
		Name:           "DLC",
		AckPolicy:      AckExplicit,
	}

	startCh = make(chan bool)
	errCh = make(chan error, np)
	wg.Add(np)

	var consumers sync.Map

	for i := 0; i < np; i++ {
		go func() {
			defer wg.Done()

			// Make them all fire at once.
			<-startCh

			if _, err = mset.addConsumer(cfg); err != nil {
				t.Logf("Consumer create got an error: %v", err)
				errCh <- err
			} else {
				consumers.Store(mset, true)
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)
	close(startCh)
	wg.Wait()

	// Check for no errors.
	if len(errCh) > 0 {
		t.Fatalf("Expected no errors, got %d: %v", len(errCh), <-errCh)
	}

	// Now make sure we really only created one stream.
	var numConsumers int
	consumers.Range(func(k, v any) bool {
		numConsumers++
		return true
	})
	if numConsumers > 1 {
		t.Fatalf("Expected only one consumer to be really created, got %d out of %d attempts", numConsumers, np)
	}
}

func TestNoRaceRoutePool(t *testing.T) {
	var dur1 time.Duration
	var dur2 time.Duration

	total := 1_000_000

	for _, test := range []struct {
		name     string
		poolSize int
	}{
		{"no pooling", 0},
		{"pooling", 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			tmpl := `
			port: -1
			accounts {
				A { users: [{user: "A", password: "A"}] }
				B { users: [{user: "B", password: "B"}] }
				C { users: [{user: "C", password: "C"}] }
				D { users: [{user: "D", password: "D"}] }
				E { users: [{user: "E", password: "E"}] }
			}
			cluster {
				port: -1
				name: "local"
				%s
				pool_size: %d
			}
		`
			conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, _EMPTY_, test.poolSize)))
			s1, o1 := RunServerWithConfig(conf1)
			defer s1.Shutdown()

			conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl,
				fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port),
				test.poolSize)))
			s2, _ := RunServerWithConfig(conf2)
			defer s2.Shutdown()

			checkClusterFormed(t, s1, s2)

			wg := sync.WaitGroup{}
			wg.Add(5)

			sendAndRecv := func(acc string) (*nats.Conn, *nats.Conn) {
				t.Helper()

				s2nc := natsConnect(t, s2.ClientURL(), nats.UserInfo(acc, acc))
				count := 0
				natsSub(t, s2nc, "foo", func(_ *nats.Msg) {
					if count++; count == total {
						wg.Done()
					}
				})
				natsFlush(t, s2nc)

				s1nc := natsConnect(t, s1.ClientURL(), nats.UserInfo(acc, acc))

				checkSubInterest(t, s1, acc, "foo", time.Second)
				return s2nc, s1nc
			}

			var rcv = [5]*nats.Conn{}
			var snd = [5]*nats.Conn{}
			accs := []string{"A", "B", "C", "D", "E"}

			for i := 0; i < 5; i++ {
				rcv[i], snd[i] = sendAndRecv(accs[i])
				defer rcv[i].Close()
				defer snd[i].Close()
			}

			payload := []byte("some message")
			start := time.Now()
			for i := 0; i < 5; i++ {
				go func(idx int) {
					for i := 0; i < total; i++ {
						snd[idx].Publish("foo", payload)
					}
				}(i)
			}

			wg.Wait()
			dur := time.Since(start)
			if test.poolSize == 0 {
				dur1 = dur
			} else {
				dur2 = dur
			}
		})
	}
	perf1 := float64(total*5) / dur1.Seconds()
	t.Logf("No pooling: %.0f msgs/sec", perf1)
	perf2 := float64(total*5) / dur2.Seconds()
	t.Logf("Pooling   : %.0f msgs/sec", perf2)
	t.Logf("Gain      : %.2fx", perf2/perf1)
}

func testNoRaceRoutePerAccount(t *testing.T, useWildCard bool) {
	var dur1 time.Duration
	var dur2 time.Duration

	accounts := make([]string, 5)
	for i := 0; i < 5; i++ {
		akp, _ := nkeys.CreateAccount()
		pub, _ := akp.PublicKey()
		accounts[i] = pub
	}
	routeAccs := fmt.Sprintf("accounts: [\"%s\", \"%s\", \"%s\", \"%s\", \"%s\"]",
		accounts[0], accounts[1], accounts[2], accounts[3], accounts[4])

	total := 1_000_000

	for _, test := range []struct {
		name      string
		dedicated bool
	}{
		{"route for all accounts", false},
		{"route per account", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			tmpl := `
			server_name: "%s"
			port: -1
			accounts {
				%s { users: [{user: "0", password: "0"}] }
				%s { users: [{user: "1", password: "1"}] }
				%s { users: [{user: "2", password: "2"}] }
				%s { users: [{user: "3", password: "3"}] }
				%s { users: [{user: "4", password: "4"}] }
			}
			cluster {
				port: -1
				name: "local"
				%s
				%s
			}
		`
			var racc string
			if test.dedicated {
				racc = routeAccs
			} else {
				racc = _EMPTY_
			}
			conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "A",
				accounts[0], accounts[1], accounts[2], accounts[3],
				accounts[4], _EMPTY_, racc)))
			s1, o1 := RunServerWithConfig(conf1)
			defer s1.Shutdown()

			conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "B",
				accounts[0], accounts[1], accounts[2], accounts[3], accounts[4],
				fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port),
				racc)))
			s2, _ := RunServerWithConfig(conf2)
			defer s2.Shutdown()

			checkClusterFormed(t, s1, s2)

			wg := sync.WaitGroup{}
			wg.Add(5)

			sendAndRecv := func(acc string, user string) (*nats.Conn, *nats.Conn) {
				t.Helper()

				s2nc := natsConnect(t, s2.ClientURL(), nats.UserInfo(user, user))
				count := 0
				var subj string
				var checkSubj string
				if useWildCard {
					subj, checkSubj = "foo.*", "foo.0"
				} else {
					subj, checkSubj = "foo", "foo"
				}
				natsSub(t, s2nc, subj, func(_ *nats.Msg) {
					if count++; count == total {
						wg.Done()
					}
				})
				natsFlush(t, s2nc)

				s1nc := natsConnect(t, s1.ClientURL(), nats.UserInfo(user, user))

				checkSubInterest(t, s1, acc, checkSubj, time.Second)
				return s2nc, s1nc
			}

			var rcv = [5]*nats.Conn{}
			var snd = [5]*nats.Conn{}
			users := []string{"0", "1", "2", "3", "4"}

			for i := 0; i < 5; i++ {
				rcv[i], snd[i] = sendAndRecv(accounts[i], users[i])
				defer rcv[i].Close()
				defer snd[i].Close()
			}

			payload := []byte("some message")
			start := time.Now()
			for i := 0; i < 5; i++ {
				go func(idx int) {
					for i := 0; i < total; i++ {
						var subj string
						if useWildCard {
							subj = fmt.Sprintf("foo.%d", i)
						} else {
							subj = "foo"
						}
						snd[idx].Publish(subj, payload)
					}
				}(i)
			}

			wg.Wait()
			dur := time.Since(start)
			if !test.dedicated {
				dur1 = dur
			} else {
				dur2 = dur
			}
		})
	}
	perf1 := float64(total*5) / dur1.Seconds()
	t.Logf("Route for all accounts: %.0f msgs/sec", perf1)
	perf2 := float64(total*5) / dur2.Seconds()
	t.Logf("Route per account     : %.0f msgs/sec", perf2)
	t.Logf("Gain                  : %.2fx", perf2/perf1)
}

func TestNoRaceRoutePerAccount(t *testing.T) {
	testNoRaceRoutePerAccount(t, false)
}

func TestNoRaceRoutePerAccountSubWithWildcard(t *testing.T) {
	testNoRaceRoutePerAccount(t, true)
}

// This test, which checks that messages are not duplicated when pooling or
// per-account routes are reloaded, would cause a DATA RACE that is not
// specific to the changes for pooling/per_account. For this reason, this
// test is located in the norace_test.go file.
func TestNoRaceRoutePoolAndPerAccountConfigReload(t *testing.T) {
	for _, test := range []struct {
		name           string
		poolSizeBefore string
		poolSizeAfter  string
		accountsBefore string
		accountsAfter  string
	}{
		{"from no pool to pool", _EMPTY_, "pool_size: 2", _EMPTY_, _EMPTY_},
		{"increase pool size", "pool_size: 2", "pool_size: 5", _EMPTY_, _EMPTY_},
		{"decrease pool size", "pool_size: 5", "pool_size: 2", _EMPTY_, _EMPTY_},
		{"from pool to no pool", "pool_size: 5", _EMPTY_, _EMPTY_, _EMPTY_},
		{"from no account to account", _EMPTY_, _EMPTY_, _EMPTY_, "accounts: [\"A\"]"},
		{"add account", _EMPTY_, _EMPTY_, "accounts: [\"B\"]", "accounts: [\"A\",\"B\"]"},
		{"remove account", _EMPTY_, _EMPTY_, "accounts: [\"A\",\"B\"]", "accounts: [\"B\"]"},
		{"from account to no account", _EMPTY_, _EMPTY_, "accounts: [\"A\"]", _EMPTY_},
		{"increase pool size and add account", "pool_size: 2", "pool_size: 3", "accounts: [\"B\"]", "accounts: [\"B\",\"A\"]"},
		{"decrease pool size and remove account", "pool_size: 3", "pool_size: 2", "accounts: [\"A\",\"B\"]", "accounts: [\"B\"]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tmplA := `
				port: -1
				server_name: "A"
				accounts {
					A { users: [{user: a, password: pwd}] }
					B { users: [{user: b, password: pwd}] }
				}
				cluster: {
					port: -1
					name: "local"
					%s
					%s
				}
			`
			confA := createConfFile(t, []byte(fmt.Sprintf(tmplA, test.poolSizeBefore, test.accountsBefore)))
			srva, optsA := RunServerWithConfig(confA)
			defer srva.Shutdown()

			tmplB := `
				port: -1
				server_name: "B"
				accounts {
					A { users: [{user: a, password: pwd}] }
					B { users: [{user: b, password: pwd}] }
				}
				cluster: {
					port: -1
					name: "local"
					%s
					%s
					routes: ["nats://127.0.0.1:%d"]
				}
			`
			confB := createConfFile(t, []byte(fmt.Sprintf(tmplB, test.poolSizeBefore, test.accountsBefore, optsA.Cluster.Port)))
			srvb, _ := RunServerWithConfig(confB)
			defer srvb.Shutdown()

			checkClusterFormed(t, srva, srvb)

			ncA := natsConnect(t, srva.ClientURL(), nats.UserInfo("a", "pwd"))
			defer ncA.Close()

			sub := natsSubSync(t, ncA, "foo")
			sub.SetPendingLimits(-1, -1)
			checkSubInterest(t, srvb, "A", "foo", time.Second)

			ncB := natsConnect(t, srvb.ClientURL(), nats.UserInfo("a", "pwd"))
			defer ncB.Close()

			wg := sync.WaitGroup{}
			wg.Add(1)
			ch := make(chan struct{})
			go func() {
				defer wg.Done()

				for i := 0; ; i++ {
					ncB.Publish("foo", []byte(fmt.Sprintf("%d", i)))
					select {
					case <-ch:
						return
					default:
					}
					if i%300 == 0 {
						time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
					}
				}
			}()

			var l *captureErrorLogger
			if test.accountsBefore != _EMPTY_ && test.accountsAfter == _EMPTY_ {
				l = &captureErrorLogger{errCh: make(chan string, 100)}
				srva.SetLogger(l, false, false)
			}

			time.Sleep(250 * time.Millisecond)
			reloadUpdateConfig(t, srva, confA, fmt.Sprintf(tmplA, test.poolSizeAfter, test.accountsAfter))
			time.Sleep(125 * time.Millisecond)
			reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(tmplB, test.poolSizeAfter, test.accountsAfter, optsA.Cluster.Port))

			checkClusterFormed(t, srva, srvb)
			checkSubInterest(t, srvb, "A", "foo", time.Second)

			if l != nil {
				// Errors regarding "No route for account" should stop
				var ok bool
				for numErrs := 0; !ok && numErrs < 10; {
					select {
					case e := <-l.errCh:
						if strings.Contains(e, "No route for account") {
							numErrs++
						}
					case <-time.After(DEFAULT_ROUTE_RECONNECT + 250*time.Millisecond):
						ok = true
					}
				}
				if !ok {
					t.Fatalf("Still report of no route for account")
				}
			}

			close(ch)
			wg.Wait()

			for prev := -1; ; {
				msg, err := sub.NextMsg(50 * time.Millisecond)
				if err != nil {
					break
				}
				cur, _ := strconv.Atoi(string(msg.Data))
				if cur <= prev {
					t.Fatalf("Previous was %d, got %d", prev, cur)
				}
				prev = cur
			}
		})
	}
}

// This test ensures that outbound queues don't cause a run on
// memory when sending something to lots of clients.
func TestNoRaceClientOutboundQueueMemory(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	var before runtime.MemStats
	var after runtime.MemStats

	var err error
	clients := make([]*nats.Conn, 50000)
	wait := &sync.WaitGroup{}
	wait.Add(len(clients))

	for i := 0; i < len(clients); i++ {
		clients[i], err = nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port), nats.InProcessServer(s))
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer clients[i].Close()

		clients[i].Subscribe("test", func(m *nats.Msg) {
			wait.Done()
		})
	}

	runtime.GC()
	runtime.ReadMemStats(&before)

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port), nats.InProcessServer(s))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	var m [48000]byte
	if err = nc.Publish("test", m[:]); err != nil {
		t.Fatal(err)
	}

	wait.Wait()

	runtime.GC()
	runtime.ReadMemStats(&after)

	hb, ha := float64(before.HeapAlloc), float64(after.HeapAlloc)
	ms := float64(len(m))
	diff := float64(ha) - float64(hb)
	inc := (diff / float64(hb)) * 100

	if inc > 10 {
		t.Logf("Message size:       %.1fKB\n", ms/1024)
		t.Logf("Subscribed clients: %d\n", len(clients))
		t.Logf("Heap allocs before: %.1fMB\n", hb/1024/1024)
		t.Logf("Heap allocs after:  %.1fMB\n", ha/1024/1024)
		t.Logf("Heap allocs delta:  %.1f%%\n", inc)

		t.Fatalf("memory increase was %.1f%% (should be <= 10%%)", inc)
	}
}

func TestNoRaceJetStreamClusterLeafnodeConnectPerf(t *testing.T) {
	// Uncomment to run. Needs to be on a big machine. Do not want as part of Travis tests atm.
	skip(t)

	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: cloud, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "CLOUD", _EMPTY_, 3, 18033, true)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "STATE",
		Subjects: []string{"STATE.GLOBAL.CELL1.*.>"},
		Replicas: 3,
	})
	require_NoError(t, err)

	tmpl = strings.Replace(jsClusterTemplWithSingleFleetLeafNode, "store_dir:", "domain: vehicle, store_dir:", 1)

	var vinSerial int
	genVIN := func() string {
		vinSerial++
		return fmt.Sprintf("7PDSGAALXNN%06d", vinSerial)
	}

	numVehicles := 500
	for i := 0; i < numVehicles; i++ {
		start := time.Now()
		vin := genVIN()
		ln := c.createLeafNodeWithTemplateNoSystemWithProto(vin, tmpl, "ws")
		nc, js := jsClientConnect(t, ln)
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "VEHICLE",
			Subjects: []string{"STATE.GLOBAL.LOCAL.>"},
			Sources: []*nats.StreamSource{{
				Name:          "STATE",
				FilterSubject: fmt.Sprintf("STATE.GLOBAL.CELL1.%s.>", vin),
				External: &nats.ExternalStream{
					APIPrefix:     "$JS.cloud.API",
					DeliverPrefix: fmt.Sprintf("DELIVER.STATE.GLOBAL.CELL1.%s", vin),
				},
			}},
		})
		require_NoError(t, err)
		// Create the sourced stream.
		checkLeafNodeConnectedCount(t, ln, 1)
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("Took too long to create leafnode %d connection: %v", i+1, elapsed)
		}
		nc.Close()
	}
}

func TestNoRaceJetStreamClusterDifferentRTTInterestBasedStreamPreAck(t *testing.T) {
	tmpl := `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
		name: "F3"
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
	`

	//  Route Ports
	//	"S1": 14622,
	//	"S2": 15622,
	//	"S3": 16622,

	// S2 (stream leader) will have a slow path to S1 (via proxy) and S3 (consumer leader) will have a fast path.

	// Do these in order, S1, S2 (proxy) then S3.
	c := &cluster{t: t, servers: make([]*Server, 3), opts: make([]*Options, 3), name: "F3"}

	// S1
	// The route connection to S2 must be through a slow proxy
	np12 := createNetProxy(10*time.Millisecond, 1024*1024*1024, 1024*1024*1024, "route://127.0.0.1:15622", true)
	routes := fmt.Sprintf("%s, route://127.0.0.1:16622", np12.routeURL())
	conf := fmt.Sprintf(tmpl, "S1", t.TempDir(), 14622, routes)
	c.servers[0], c.opts[0] = RunServerWithConfig(createConfFile(t, []byte(conf)))

	// S2
	// The route connection to S1 must be through a slow proxy
	np21 := createNetProxy(10*time.Millisecond, 1024*1024*1024, 1024*1024*1024, "route://127.0.0.1:14622", true)
	routes = fmt.Sprintf("%s, route://127.0.0.1:16622", np21.routeURL())
	conf = fmt.Sprintf(tmpl, "S2", t.TempDir(), 15622, routes)
	c.servers[1], c.opts[1] = RunServerWithConfig(createConfFile(t, []byte(conf)))

	// S3
	conf = fmt.Sprintf(tmpl, "S3", t.TempDir(), 16622, "route://127.0.0.1:14622, route://127.0.0.1:15622")
	c.servers[2], c.opts[2] = RunServerWithConfig(createConfFile(t, []byte(conf)))

	c.checkClusterFormed()
	c.waitOnClusterReady()
	defer c.shutdown()
	defer np12.stop()
	defer np21.stop()

	slow := c.servers[0] // Expecting pre-acks here.
	sl := c.servers[1]   // Stream leader, will publish here.
	cl := c.servers[2]   // Consumer leader, will consume & ack here.

	snc, sjs := jsClientConnect(t, sl)
	defer snc.Close()

	cnc, cjs := jsClientConnect(t, cl)
	defer cnc.Close()

	// Now create the stream.
	_, err := sjs.AddStream(&nats.StreamConfig{
		Name:      "EVENTS",
		Subjects:  []string{"EV.>"},
		Replicas:  3,
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	// Make sure it's leader is on S2.
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnStreamLeader(globalAccountName, "EVENTS")
		if s := c.streamLeader(globalAccountName, "EVENTS"); s != sl {
			s.JetStreamStepdownStream(globalAccountName, "EVENTS")
			return fmt.Errorf("Server %s is not stream leader yet", sl)
		}
		return nil
	})

	// Now create the consumer.
	_, err = sjs.AddConsumer("EVENTS", &nats.ConsumerConfig{
		Durable:        "C",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: "dx",
	})
	require_NoError(t, err)

	// Make sure the consumer leader is on S3.
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnConsumerLeader(globalAccountName, "EVENTS", "C")
		if s := c.consumerLeader(globalAccountName, "EVENTS", "C"); s != cl {
			s.JetStreamStepdownConsumer(globalAccountName, "EVENTS", "C")
			return fmt.Errorf("Server %s is not consumer leader yet", sl)
		}
		return nil
	})

	_, err = cjs.Subscribe(_EMPTY_, func(msg *nats.Msg) {
		msg.Ack()
	}, nats.BindStream("EVENTS"), nats.Durable("C"), nats.ManualAck())
	require_NoError(t, err)

	// Publish directly on the stream leader to make it efficient.
	for i := 0; i < 1_000; i++ {
		_, err := sjs.PublishAsync("EV.PAID", []byte("ok"))
		require_NoError(t, err)
	}
	select {
	case <-sjs.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	mset, err := slow.GlobalAccount().lookupStream("EVENTS")
	require_NoError(t, err)

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		state := mset.state()
		if state.LastSeq != 1000 {
			return fmt.Errorf("Haven't received all messages yet (last seq %d)", state.LastSeq)
		}
		mset.mu.RLock()
		preAcks := mset.preAcks
		mset.mu.RUnlock()
		if preAcks == nil {
			return fmt.Errorf("Expected to have preAcks by now")
		}
		if state.Msgs == 0 {
			mset.mu.RLock()
			lp := len(mset.preAcks)
			mset.mu.RUnlock()
			if lp == 0 {
				return nil
			} else {
				t.Fatalf("Expected no preAcks with no msgs, but got %d", lp)
			}
		}
		return fmt.Errorf("Still have %d msgs left", state.Msgs)
	})

}

func TestNoRaceCheckAckFloorWithVeryLargeFirstSeqAndNewConsumers(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	// Make sure to time bound here for the acksync call below.
	js, err := nc.JetStream(nats.MaxWait(200 * time.Millisecond))
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"wq-req"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	largeFirstSeq := uint64(1_200_000_000)
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: largeFirstSeq})
	require_NoError(t, err)
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.FirstSeq == largeFirstSeq)

	// Add a simple request to the stream.
	sendStreamMsg(t, nc, "wq-req", "HELP")

	sub, err := js.PullSubscribe("wq-req", "dlc")
	require_NoError(t, err)

	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_True(t, len(msgs) == 1)

	// The bug is around the checkAckFloor walking the sequences from current ackfloor
	// to the first sequence of the stream. We time bound the max wait with the js context
	// to 200ms. Since checkAckFloor is spinning and holding up processing of acks this will fail.
	// We will short circuit new consumers to fix this one.
	require_NoError(t, msgs[0].AckSync())

	// Now do again so we move past the new consumer with no ack floor situation.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 2 * largeFirstSeq})
	require_NoError(t, err)
	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.FirstSeq == 2*largeFirstSeq)

	sendStreamMsg(t, nc, "wq-req", "MORE HELP")

	// We check this one directly for this use case.
	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("dlc")
	require_True(t, o != nil)

	// Purge will move the stream floor by default, so force into the situation where it is back to largeFirstSeq.
	// This will not trigger the new consumer logic, but will trigger a walk of the sequence space.
	// Fix will be to walk the lesser of the two linear spaces.
	o.mu.Lock()
	o.asflr = largeFirstSeq
	o.mu.Unlock()

	done := make(chan bool)
	go func() {
		o.checkAckFloor()
		done <- true
	}()

	select {
	case <-done:
		return
	case <-time.After(time.Second):
		t.Fatalf("Check ack floor taking too long!")
	}
}

func TestNoRaceReplicatedMirrorWithLargeStartingSequenceOverLeafnode(t *testing.T) {
	// Cluster B
	tmpl := strings.Replace(jsClusterTempl, "store_dir:", "domain: B, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "B", _EMPTY_, 3, 22020, true)
	defer c.shutdown()

	// Cluster A
	// Domain is "A'
	lc := c.createLeafNodesWithStartPortAndDomain("A", 3, 22110, "A")
	defer lc.shutdown()

	lc.waitOnClusterReady()

	// Create a stream on B (HUB/CLOUD) and set its starting sequence very high.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 1_000_000_000})
	require_NoError(t, err)

	// Send in a small amount of messages.
	for i := 0; i < 1000; i++ {
		sendStreamMsg(t, nc, "foo", "Hello")
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.FirstSeq == 1_000_000_000)

	// Now try to create a replicated mirror on the leaf cluster.
	lnc, ljs := jsClientConnect(t, lc.randomServer())
	defer lnc.Close()

	_, err = ljs.AddStream(&nats.StreamConfig{
		Name: "TEST",
		Mirror: &nats.StreamSource{
			Name:   "TEST",
			Domain: "B",
		},
	})
	require_NoError(t, err)

	// Make sure we sync quickly.
	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		si, err = ljs.StreamInfo("TEST")
		require_NoError(t, err)
		if si.State.Msgs == 1000 && si.State.FirstSeq == 1_000_000_000 {
			return nil
		}
		return fmt.Errorf("Mirror state not correct: %+v", si.State)
	})
}

func TestNoRaceBinaryStreamSnapshotEncodingBasic(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"*"},
		MaxMsgsPerSubject: 1,
	})
	require_NoError(t, err)

	// Set first key
	sendStreamMsg(t, nc, "key:1", "hello")

	// Set Second key but keep updating it, causing a laggard pattern.
	value := bytes.Repeat([]byte("Z"), 8*1024)

	for i := 0; i <= 1000; i++ {
		_, err := js.PublishAsync("key:2", value)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Now do more of swiss cheese style.
	for i := 3; i <= 1000; i++ {
		key := fmt.Sprintf("key:%d", i)
		_, err := js.PublishAsync(key, value)
		require_NoError(t, err)
		// Send it twice to create hole right behind it, like swiss cheese.
		_, err = js.PublishAsync(key, value)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Make for round numbers for stream state.
	sendStreamMsg(t, nc, "key:2", "hello")
	sendStreamMsg(t, nc, "key:2", "world")

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.FirstSeq == 1)
	require_True(t, si.State.LastSeq == 3000)
	require_True(t, si.State.Msgs == 1000)
	require_True(t, si.State.NumDeleted == 2000)

	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	snap, err := mset.store.EncodedStreamState(0)
	require_NoError(t, err)

	// Now decode the snapshot.
	ss, err := DecodeStreamState(snap)
	require_NoError(t, err)

	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 3000)
	require_Equal(t, ss.Msgs, 1000)
	require_Equal(t, ss.Deleted.NumDeleted(), 2000)
}

func TestNoRaceFilestoreBinaryStreamSnapshotEncodingLargeGaps(t *testing.T) {
	storeDir := t.TempDir()
	fcfg := FileStoreConfig{
		StoreDir:  storeDir,
		BlockSize: 512, // Small on purpose to create alot of blks.
	}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"zzz"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "zzz", bytes.Repeat([]byte("X"), 128)
	numMsgs := 20_000

	fs.StoreMsg(subj, nil, msg, 0)
	for i := 2; i < numMsgs; i++ {
		seq, _, err := fs.StoreMsg(subj, nil, nil, 0)
		require_NoError(t, err)
		fs.RemoveMsg(seq)
	}
	fs.StoreMsg(subj, nil, msg, 0)

	snap, err := fs.EncodedStreamState(0)
	require_NoError(t, err)
	require_True(t, len(snap) < 512)

	// Now decode the snapshot.
	ss, err := DecodeStreamState(snap)
	require_NoError(t, err)

	require_True(t, ss.FirstSeq == 1)
	require_True(t, ss.LastSeq == 20_000)
	require_True(t, ss.Msgs == 2)
	require_True(t, len(ss.Deleted) <= 2)
	require_True(t, ss.Deleted.NumDeleted() == 19_998)
}

func TestNoRaceJetStreamClusterStreamSnapshotCatchup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"*"},
		MaxMsgsPerSubject: 1,
		Replicas:          3,
	})
	require_NoError(t, err)

	msg := []byte("Hello World")
	_, err = js.Publish("foo", msg)
	require_NoError(t, err)

	for i := 1; i < 1000; i++ {
		_, err := js.PublishAsync("bar", msg)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	sr := c.randomNonStreamLeader(globalAccountName, "TEST")
	sr.Shutdown()

	// In case we were connected to sr.
	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Now create a large gap.
	for i := 0; i < 50_000; i++ {
		_, err := js.PublishAsync("bar", msg)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(10 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	sl := c.streamLeader(globalAccountName, "TEST")
	sl.JetStreamSnapshotStream(globalAccountName, "TEST")

	sr = c.restartServer(sr)
	c.checkClusterFormed()
	c.waitOnServerCurrent(sr)
	c.waitOnStreamCurrent(sr, globalAccountName, "TEST")

	mset, err := sr.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// Make sure it's caught up
	var state StreamState
	mset.store.FastState(&state)
	require_Equal(t, state.Msgs, 2)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 51_000)
	require_Equal(t, state.NumDeleted, 51_000-2)

	sr.Shutdown()

	_, err = js.Publish("baz", msg)
	require_NoError(t, err)

	sl.JetStreamSnapshotStream(globalAccountName, "TEST")

	sr = c.restartServer(sr)
	c.checkClusterFormed()
	c.waitOnServerCurrent(sr)
	c.waitOnStreamCurrent(sr, globalAccountName, "TEST")

	mset, err = sr.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	mset.store.FastState(&state)

	require_Equal(t, state.Msgs, 3)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 51_001)
	require_Equal(t, state.NumDeleted, 51_001-3)
}

func TestNoRaceStoreStreamEncoderDecoder(t *testing.T) {
	cfg := &StreamConfig{
		Name:       "zzz",
		Subjects:   []string{"*"},
		MaxMsgsPer: 1,
		Storage:    MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, MaxMsgsPer: 1, Storage: FileStorage},
	)
	require_NoError(t, err)
	defer fs.Stop()

	const seed = 2222222
	msg := bytes.Repeat([]byte("ABC"), 33) // ~100bytes

	maxEncodeTime := 2 * time.Second
	maxEncodeSize := 700 * 1024

	test := func(t *testing.T, gs StreamStore) {
		t.Parallel()
		prand := rand.New(rand.NewSource(seed))
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		done := time.NewTimer(10 * time.Second)

		for running := true; running; {
			select {
			case <-tick.C:
				var state StreamState
				gs.FastState(&state)
				if state.NumDeleted == 0 {
					continue
				}
				start := time.Now()
				snap, err := gs.EncodedStreamState(0)
				require_NoError(t, err)
				elapsed := time.Since(start)
				// Should take <1ms without race but if CI/CD is slow we will give it a bit of room.
				if elapsed > maxEncodeTime {
					t.Logf("Encode took longer then expected: %v", elapsed)
				}
				if len(snap) > maxEncodeSize {
					t.Fatalf("Expected snapshot size < %v got %v", friendlyBytes(maxEncodeSize), friendlyBytes(len(snap)))
				}
				ss, err := DecodeStreamState(snap)
				require_True(t, len(ss.Deleted) > 0)
				require_NoError(t, err)
			case <-done.C:
				running = false
			default:
				key := strconv.Itoa(prand.Intn(256_000))
				gs.StoreMsg(key, nil, msg, 0)
			}
		}
	}

	for _, gs := range []StreamStore{ms, fs} {
		switch gs.(type) {
		case *memStore:
			t.Run("MemStore", func(t *testing.T) {
				test(t, gs)
			})
		case *fileStore:
			t.Run("FileStore", func(t *testing.T) {
				test(t, gs)
			})
		}
	}
}

func TestNoRaceJetStreamClusterKVWithServerKill(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Setup the KV bucket and use for making assertions.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()
	_, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:   "TEST",
		Replicas: 3,
		History:  10,
	})
	require_NoError(t, err)

	// Total number of keys to range over.
	numKeys := 50

	// ID is the server id to explicitly connect to.
	work := func(ctx context.Context, wg *sync.WaitGroup, id int) {
		defer wg.Done()

		nc, js := jsClientConnectEx(t, c.servers[id], []nats.JSOpt{nats.Context(ctx)})
		defer nc.Close()

		kv, err := js.KeyValue("TEST")
		require_NoError(t, err)

		// 100 messages a second for each single client.
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-tk.C:
				// Pick a random key within the range.
				k := fmt.Sprintf("key.%d", rand.Intn(numKeys))
				// Attempt to get a key.
				e, err := kv.Get(k)
				// If found, attempt to update or delete.
				if err == nil {
					if rand.Intn(10) < 3 {
						kv.Delete(k, nats.LastRevision(e.Revision()))
					} else {
						kv.Update(k, nil, e.Revision())
					}
				} else if errors.Is(err, nats.ErrKeyNotFound) {
					kv.Create(k, nil)
				}
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(3)

	go work(ctx, &wg, 0)
	go work(ctx, &wg, 1)
	go work(ctx, &wg, 2)

	time.Sleep(time.Second)

	// Simulate server stop and restart.
	for i := 0; i < 7; i++ {
		s := c.randomServer()
		s.Shutdown()
		c.waitOnLeader()
		c.waitOnStreamLeader(globalAccountName, "KV_TEST")

		// Wait for a bit and then start the server again.
		time.Sleep(time.Duration(rand.Intn(1250)) * time.Millisecond)
		s = c.restartServer(s)
		c.waitOnServerCurrent(s)
		c.waitOnLeader()
		c.waitOnStreamLeader(globalAccountName, "KV_TEST")
		c.waitOnPeerCount(3)
	}

	// Stop the workload.
	cancel()
	wg.Wait()

	type fullState struct {
		state StreamState
		lseq  uint64
		clfs  uint64
	}

	grabState := func(mset *stream) *fullState {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
		var state StreamState
		mset.store.FastState(&state)
		return &fullState{state, mset.lseq, mset.clfs}
	}

	grabStore := func(mset *stream) map[string][]uint64 {
		mset.mu.RLock()
		store := mset.store
		mset.mu.RUnlock()
		var state StreamState
		store.FastState(&state)
		storeMap := make(map[string][]uint64)
		for seq := state.FirstSeq; seq <= state.LastSeq; seq++ {
			if sm, err := store.LoadMsg(seq, nil); err == nil {
				storeMap[sm.subj] = append(storeMap[sm.subj], sm.seq)
			}
		}
		return storeMap
	}

	checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
		// Current stream leader.
		sl := c.streamLeader(globalAccountName, "KV_TEST")
		mset, err := sl.GlobalAccount().lookupStream("KV_TEST")
		require_NoError(t, err)
		lstate := grabState(mset)
		golden := grabStore(mset)

		// Report messages per server.
		for _, s := range c.servers {
			if s == sl {
				continue
			}
			mset, err := s.GlobalAccount().lookupStream("KV_TEST")
			require_NoError(t, err)
			state := grabState(mset)
			if !reflect.DeepEqual(state, lstate) {
				return fmt.Errorf("Expected follower state\n%+v\nto match leader's\n %+v", state, lstate)
			}
			sm := grabStore(mset)
			if !reflect.DeepEqual(sm, golden) {
				t.Fatalf("Expected follower store for %v\n%+v\nto match leader's %v\n %+v", s, sm, sl, golden)
			}
		}
		return nil
	})
}

func TestNoRaceFileStoreLargeMsgsAndFirstMatching(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 8 * 1024 * 1024},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	for i := 0; i < 150_000; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.bar.%d", i), nil, nil, 0)
	}
	for i := 0; i < 150_000; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.baz.%d", i), nil, nil, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 2)
	fs.mu.RLock()
	mb := fs.blks[1]
	fs.mu.RUnlock()
	fseq := atomic.LoadUint64(&mb.first.seq)
	// The -40 leaves enough mb.fss entries to kick in linear scan.
	for seq := fseq; seq < 300_000-40; seq++ {
		fs.RemoveMsg(uint64(seq))
	}
	start := time.Now()
	fs.LoadNextMsg("*.baz.*", true, fseq, nil)
	require_True(t, time.Since(start) < 200*time.Microsecond)
	// Now remove more to kick into non-linear logic.
	for seq := 300_000 - 40; seq < 300_000; seq++ {
		fs.RemoveMsg(uint64(seq))
	}
	start = time.Now()
	fs.LoadNextMsg("*.baz.*", true, fseq, nil)
	require_True(t, time.Since(start) < 200*time.Microsecond)
}

func TestNoRaceWSNoCorruptionWithFrameSizeLimit(t *testing.T) {
	testWSNoCorruptionWithFrameSizeLimit(t, 50000)
}

func TestNoRaceJetStreamAPIDispatchQueuePending(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Setup the KV bucket and use for making assertions.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*.*"},
	})
	require_NoError(t, err)

	// Queue up 500k messages all with different subjects.
	// We want to make num pending for a consumer expensive, so a large subject
	// space and wildcards for now does the trick.
	toks := []string{"foo", "bar", "baz"} // for second token.
	for i := 1; i <= 500_000; i++ {
		subj := fmt.Sprintf("foo.%s.%d", toks[rand.Intn(len(toks))], i)
		_, err := js.PublishAsync(subj, nil, nats.StallWait(time.Second))
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(20 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// To back up our pending queue we will create lots of filtered, with wildcards, R1 consumers
	// from a different server then the one hosting the stream.
	// ok to share this connection here.
	sldr := c.streamLeader(globalAccountName, "TEST")
	for _, s := range c.servers {
		if s != sldr {
			nc, js = jsClientConnect(t, s)
			defer nc.Close()
			break
		}
	}

	ngr, ncons := 100, 10
	startCh, errCh := make(chan bool), make(chan error, ngr)
	var wg, swg sync.WaitGroup
	wg.Add(ngr)
	swg.Add(ngr)

	// The wildcard in the filter subject is the key.
	cfg := &nats.ConsumerConfig{FilterSubject: "foo.*.22"}
	var tt atomic.Int64

	for i := 0; i < ngr; i++ {
		go func() {
			defer wg.Done()
			swg.Done()
			// Make them all fire at once.
			<-startCh

			for i := 0; i < ncons; i++ {
				start := time.Now()
				if _, err := js.AddConsumer("TEST", cfg); err != nil {
					errCh <- err
					t.Logf("Got err creating consumer: %v", err)
				}
				elapsed := time.Since(start)
				tt.Add(int64(elapsed))
			}
		}()
	}
	swg.Wait()
	close(startCh)
	time.Sleep(time.Millisecond)
	jsz, _ := sldr.Jsz(nil)
	// This could be 0 legit, so just log, don't fail.
	if jsz.JetStreamStats.API.Inflight == 0 {
		t.Log("Expected a non-zero inflight")
	}
	wg.Wait()

	if len(errCh) > 0 {
		t.Fatalf("Expected no errors, got %d: %v", len(errCh), <-errCh)
	}
}

func TestNoRaceJetStreamMirrorAndSourceConsumerFailBackoff(t *testing.T) {
	owt := srcConsumerWaitTime
	srcConsumerWaitTime = 2 * time.Second
	defer func() { srcConsumerWaitTime = owt }()

	// Check calculations first.
	for i := 1; i <= 20; i++ {
		backoff := calculateRetryBackoff(i)
		if i < 12 {
			require_Equal(t, backoff, time.Duration(i)*10*time.Second)
		} else {
			require_Equal(t, backoff, retryMaximum)
		}
	}

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*.*"},
	})
	require_NoError(t, err)
	sl := c.streamLeader(globalAccountName, "TEST")

	// Create a mirror.
	ml := sl
	// Make sure not on the same server. Should not happened in general but possible.
	for ml == sl {
		js.DeleteStream("MIRROR")
		_, err = js.AddStream(&nats.StreamConfig{
			Name:   "MIRROR",
			Mirror: &nats.StreamSource{Name: "TEST"},
		})
		require_NoError(t, err)
		ml = c.streamLeader(globalAccountName, "MIRROR")
	}
	// Create a source.
	srcl := sl
	for srcl == sl {
		js.DeleteStream("SOURCE")
		_, err = js.AddStream(&nats.StreamConfig{
			Name:    "SOURCE",
			Sources: []*nats.StreamSource{{Name: "TEST"}},
		})
		require_NoError(t, err)
		srcl = c.streamLeader(globalAccountName, "MIRROR")
	}

	// Create sub to watch for the consumer create requests.
	nc, _ = jsClientConnect(t, ml)
	defer nc.Close()
	sub := natsSubSync(t, nc, "$JS.API.CONSUMER.CREATE.>")

	// Kill the server where the source is..
	sldr := c.streamLeader(globalAccountName, "TEST")
	sldr.Shutdown()

	// Wait for just greater than 5s. We should only see 1 request during this time.
	time.Sleep(6 * time.Second)
	// There should have been 2 requests, one for mirror, one for source
	n, _, _ := sub.Pending()
	require_Equal(t, n, 2)
	var mreq, sreq int
	for i := 0; i < 2; i++ {
		msg := natsNexMsg(t, sub, time.Second)
		if bytes.Contains(msg.Data, []byte("$JS.M.")) {
			mreq++
		} else if bytes.Contains(msg.Data, []byte("$JS.S.")) {
			sreq++
		}
	}
	if mreq != 1 || sreq != 1 {
		t.Fatalf("Consumer create captures invalid: mreq=%v sreq=%v", mreq, sreq)
	}

	// Now make sure that the fails is set properly.
	mset, err := c.streamLeader(globalAccountName, "MIRROR").GlobalAccount().lookupStream("MIRROR")
	require_NoError(t, err)
	mset.mu.RLock()
	fails := mset.mirror.fails
	mset.mu.RUnlock()
	require_Equal(t, fails, 1)

	mset, err = c.streamLeader(globalAccountName, "SOURCE").GlobalAccount().lookupStream("SOURCE")
	require_NoError(t, err)
	mset.mu.RLock()
	si := mset.sources["TEST > >"]
	mset.mu.RUnlock()
	require_True(t, si != nil)
	require_Equal(t, si.fails, 1)
}

func TestNoRaceJetStreamClusterStreamCatchupLargeInteriorDeletes(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo.*"},
		MaxMsgsPerSubject: 100,
		Replicas:          1,
	}

	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	msg := bytes.Repeat([]byte("Z"), 2*1024)
	// We will create lots of interior deletes on our R1 then scale up.
	_, err = js.Publish("foo.0", msg)
	require_NoError(t, err)

	// Create 50k messages randomly from 1-100
	for i := 0; i < 50_000; i++ {
		subj := fmt.Sprintf("foo.%d", rand.Intn(100)+1)
		js.PublishAsync(subj, msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
	// Now create a large gap.
	for i := 0; i < 100_000; i++ {
		js.PublishAsync("foo.2", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
	// Do 50k random again at end.
	for i := 0; i < 50_000; i++ {
		subj := fmt.Sprintf("foo.%d", rand.Intn(100)+1)
		js.PublishAsync(subj, msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	cfg.Replicas = 2
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	// Let catchup start.
	c.waitOnStreamLeader(globalAccountName, "TEST")

	nl := c.randomNonStreamLeader(globalAccountName, "TEST")
	require_True(t, nl != nil)
	mset, err := nl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
		state := mset.state()
		if state.Msgs == si.State.Msgs {
			return nil
		}
		return fmt.Errorf("Msgs not equal %d vs %d", state.Msgs, si.State.Msgs)
	})
}

func TestNoRaceJetStreamClusterBadRestartsWithHealthzPolling(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>"},
		Replicas: 3,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	// We will poll healthz at a decent clip and make sure any restart logic works
	// correctly with assets coming and going.
	ch := make(chan struct{})
	defer close(ch)

	go func() {
		for {
			select {
			case <-ch:
				return
			case <-time.After(50 * time.Millisecond):
				for _, s := range c.servers {
					s.healthz(nil)
				}
			}
		}
	}()

	numConsumers := 500
	consumers := make([]string, 0, numConsumers)

	var wg sync.WaitGroup

	for i := 0; i < numConsumers; i++ {
		cname := fmt.Sprintf("CONS-%d", i+1)
		consumers = append(consumers, cname)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := js.PullSubscribe("foo.>", cname, nats.BindStream("TEST"))
			require_NoError(t, err)
		}()
	}
	wg.Wait()

	// Make sure all are reported.
	c.waitOnAllCurrent()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			jsz, _ := s.Jsz(nil)
			if jsz.Consumers != numConsumers {
				return fmt.Errorf("%v wrong number of consumers: %d vs %d", s, jsz.Consumers, numConsumers)
			}
		}
		return nil
	})

	// Now do same for streams.
	numStreams := 200
	streams := make([]string, 0, numStreams)

	for i := 0; i < numStreams; i++ {
		sname := fmt.Sprintf("TEST-%d", i+1)
		streams = append(streams, sname)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := js.AddStream(&nats.StreamConfig{Name: sname, Replicas: 3})
			require_NoError(t, err)
		}()
	}
	wg.Wait()

	// Make sure all are reported.
	c.waitOnAllCurrent()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			jsz, _ := s.Jsz(nil)
			if jsz.Streams != numStreams+1 {
				return fmt.Errorf("%v wrong number of streams: %d vs %d", s, jsz.Streams, numStreams+1)
			}
		}
		return nil
	})

	// Delete consumers.
	for _, cname := range consumers {
		err := js.DeleteConsumer("TEST", cname)
		require_NoError(t, err)
	}
	// Make sure reporting goes to zero.
	c.waitOnAllCurrent()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			jsz, _ := s.Jsz(nil)
			if jsz.Consumers != 0 {
				return fmt.Errorf("%v still has %d consumers", s, jsz.Consumers)
			}
		}
		return nil
	})

	// Delete streams
	for _, sname := range streams {
		err := js.DeleteStream(sname)
		require_NoError(t, err)
	}
	err = js.DeleteStream("TEST")
	require_NoError(t, err)

	// Make sure reporting goes to zero.
	c.waitOnAllCurrent()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			jsz, _ := s.Jsz(nil)
			if jsz.Streams != 0 {
				return fmt.Errorf("%v still has %d streams", s, jsz.Streams)
			}
		}
		return nil
	})
}

func TestNoRaceJetStreamKVReplaceWithServerRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()
	// Shorten wait time for disconnects.
	js, err := nc.JetStream(nats.MaxWait(time.Second))
	require_NoError(t, err)

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:   "TEST",
		Replicas: 3,
	})
	require_NoError(t, err)

	createData := func(n int) []byte {
		const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		b := make([]byte, n)
		for i := range b {
			b[i] = letterBytes[rand.Intn(len(letterBytes))]
		}
		return b
	}

	_, err = kv.Create("foo", createData(160))
	require_NoError(t, err)

	ch := make(chan struct{})
	wg := sync.WaitGroup{}

	// For counting errors that should not happen.
	errCh := make(chan error, 1024)

	wg.Add(1)
	go func() {
		defer wg.Done()

		var lastData []byte
		var revision uint64

		for {
			select {
			case <-ch:
				return
			default:
				k, err := kv.Get("foo")
				if err == nats.ErrKeyNotFound {
					errCh <- err
				} else if k != nil {
					if lastData != nil && k.Revision() == revision && !bytes.Equal(lastData, k.Value()) {
						errCh <- fmt.Errorf("data loss [%s][rev:%d] expected:[%q] is:[%q]\n", "foo", revision, lastData, k.Value())
					}
					newData := createData(160)
					if revision, err = kv.Update("foo", newData, k.Revision()); err == nil {
						lastData = newData
					}
				}
			}
		}
	}()

	// Wait a short bit.
	time.Sleep(2 * time.Second)
	for _, s := range c.servers {
		s.Shutdown()
		// Need to leave servers down for awhile to trigger bug properly.
		time.Sleep(5 * time.Second)
		s = c.restartServer(s)
		c.waitOnServerHealthz(s)
	}

	// Shutdown the go routine above.
	close(ch)
	// Wait for it to finish.
	wg.Wait()

	if len(errCh) != 0 {
		for err := range errCh {
			t.Logf("Received err %v during test", err)
		}
		t.Fatalf("Encountered errors")
	}
}

func TestNoRaceMemStoreCompactPerformance(t *testing.T) {
	//Load MemStore so that it is full
	subj, msg := "foo", make([]byte, 1000)
	storedMsgSize := memStoreMsgSize(subj, nil, msg)

	toStore := uint64(10_000)
	toStoreOnTop := uint64(1_000)
	setSeqNo := uint64(10_000_000_000)

	expectedPurge := toStore - 1
	maxBytes := storedMsgSize * toStore

	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage, MaxBytes: int64(maxBytes)})
	require_NoError(t, err)
	defer ms.Stop()

	for i := uint64(0); i < toStore; i++ {
		ms.StoreMsg(subj, nil, msg, 0)
	}
	state := ms.State()
	require_Equal(t, toStore, state.Msgs)
	require_Equal(t, state.Bytes, storedMsgSize*toStore)

	//1st run: Load additional messages then compact
	for i := uint64(0); i < toStoreOnTop; i++ {
		ms.StoreMsg(subj, nil, msg, 0)
	}
	startFirstRun := time.Now()
	purgedFirstRun, _ := ms.Compact(toStore + toStoreOnTop)
	elapsedFirstRun := time.Since(startFirstRun)
	require_Equal(t, expectedPurge, purgedFirstRun)

	//set the seq number to a very high value by compacting with a too high seq number
	purgedFull, _ := ms.Compact(setSeqNo)
	require_Equal(t, 1, purgedFull)

	//2nd run: Compact again
	for i := uint64(0); i < toStore; i++ {
		ms.StoreMsg(subj, nil, msg, 0)
	}
	startSecondRun := time.Now()
	purgedSecondRun, _ := ms.Compact(setSeqNo + toStore - 1)
	elapsedSecondRun := time.Since(startSecondRun)
	require_Equal(t, expectedPurge, purgedSecondRun)

	//Calculate delta between runs and fail if it is too high
	require_LessThan(t, elapsedSecondRun-elapsedFirstRun, time.Duration(1)*time.Second)
}

func TestNoRaceJetStreamSnapshotsWithSlowAckDontSlowConsumer(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	ech := make(chan error)
	ecb := func(_ *nats.Conn, _ *nats.Subscription, err error) {
		if err != nil {
			ech <- err
		}
	}
	nc, js := jsClientConnect(t, s, nats.ErrorHandler(ecb))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	// Put in over 64MB.
	msg, toSend := make([]byte, 1024*1024), 80
	crand.Read(msg)

	for i := 0; i < toSend; i++ {
		_, err := js.Publish("foo", msg)
		require_NoError(t, err)
	}

	sreq := &JSApiStreamSnapshotRequest{
		DeliverSubject: nats.NewInbox(),
		ChunkSize:      1024 * 1024,
	}
	req, _ := json.Marshal(sreq)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "TEST"), req, time.Second)
	require_NoError(t, err)

	var resp JSApiStreamSnapshotResponse
	json.Unmarshal(rmsg.Data, &resp)
	require_True(t, resp.Error == nil)

	done := make(chan *nats.Msg)
	sub, _ := nc.Subscribe(sreq.DeliverSubject, func(m *nats.Msg) {
		// EOF
		if len(m.Data) == 0 {
			done <- m
			return
		}
	})
	defer sub.Unsubscribe()

	// Check that we do not get disconnected due to slow consumer.
	select {
	case msg := <-done:
		require_Equal(t, msg.Header.Get("Status"), "408")
		require_Equal(t, msg.Header.Get("Description"), "No Flow Response")
	case <-ech:
		t.Fatalf("Got disconnected: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("Should have received EOF with error status")
	}
}

func TestNoRaceJetStreamWQSkippedMsgsOnScaleUp(t *testing.T) {
	checkInterestStateT = 4 * time.Second
	checkInterestStateJ = 1
	defer func() {
		checkInterestStateT = defaultCheckInterestStateT
		checkInterestStateJ = defaultCheckInterestStateJ
	}()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	const pre = "CORE_ENT_DR_OTP_22."
	wcSubj := pre + ">"

	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{wcSubj},
		Retention:   nats.WorkQueuePolicy,
		AllowDirect: true,
		Replicas:    3,
	})
	require_NoError(t, err)

	cfg := &nats.ConsumerConfig{
		Durable:           "dlc",
		FilterSubject:     wcSubj,
		DeliverPolicy:     nats.DeliverAllPolicy,
		AckPolicy:         nats.AckExplicitPolicy,
		MaxAckPending:     10_000,
		AckWait:           500 * time.Millisecond,
		MaxWaiting:        100,
		MaxRequestExpires: 1050 * time.Millisecond,
	}
	_, err = js.AddConsumer("TEST", cfg)
	require_NoError(t, err)

	pdone := make(chan bool)
	cdone := make(chan bool)

	// We will have 51 consumer apps and a producer app. Make sure to wait for
	// all go routines to end at the end of the test.
	wg := sync.WaitGroup{}
	wg.Add(52)

	// Publish routine
	go func() {
		defer wg.Done()

		publishSubjects := []string{
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.918886682066",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.918886682067",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.916596543211",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.916596543212",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.916596543213",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.916596543214",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.916596543215",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.916596543216",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.916596543217",
		}
		// ~1.7kb
		msg := bytes.Repeat([]byte("Z"), 1750)

		// 200 msgs/s
		st := time.NewTicker(5 * time.Millisecond)
		defer st.Stop()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		for {
			select {
			case <-st.C:
				subj := publishSubjects[rand.Intn(len(publishSubjects))]
				_, err = js.Publish(subj, msg)
				require_NoError(t, err)
			case <-pdone:
				return
			}
		}
	}()

	consumerApp := func() {
		defer wg.Done()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := js.ConsumerInfo("TEST", "dlc")
		require_NoError(t, err)
		_, err = js.UpdateConsumer("TEST", cfg)
		require_NoError(t, err)

		sub, err := js.PullSubscribe(wcSubj, "dlc")
		require_NoError(t, err)

		st := time.NewTicker(100 * time.Millisecond)
		defer st.Stop()

		for {
			select {
			case <-st.C:
				msgs, err := sub.Fetch(1, nats.MaxWait(100*time.Millisecond))
				if err != nil {
					continue
				}
				require_Equal(t, len(msgs), 1)
				m := msgs[0]
				if rand.Intn(10) == 1 {
					m.Nak()
				} else {
					// Wait up to 20ms to ack.
					time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
					// This could fail and that is ok, system should recover due to low ack wait.
					m.Ack()
				}
			case <-cdone:
				return
			}
		}
	}

	// Now consumer side single.
	go consumerApp()

	// Wait for 2s
	time.Sleep(2 * time.Second)

	// Now spin up 50 more.
	for i := 1; i <= 50; i++ {
		if i%5 == 0 {
			time.Sleep(200 * time.Millisecond)
		}
		go consumerApp()
	}

	timeout := time.Now().Add(8 * time.Second)
	for time.Now().Before(timeout) {
		time.Sleep(750 * time.Millisecond)
		if s := c.consumerLeader(globalAccountName, "TEST", "dlc"); s != nil {
			s.JetStreamStepdownConsumer(globalAccountName, "TEST", "dlc")
		}
	}

	// Close publishers and defer closing consumers.
	close(pdone)
	defer func() {
		close(cdone)
		wg.Wait()
	}()

	checkFor(t, 30*time.Second, 50*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		if si.State.NumDeleted > 0 || si.State.Msgs > 0 {
			return fmt.Errorf("State not correct: %+v", si.State)
		}
		return nil
	})
}

func TestNoRaceConnectionObjectReleased(t *testing.T) {
	ob1Conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		server_name: "B1"
		accounts {
			A { users: [{user: a, password: pwd}] }
			SYS { users: [{user: sys, password: pwd}] }
		}
		cluster {
			name: "B"
			listen: "127.0.0.1:-1"
		}
		gateway {
			name: "B"
			listen: "127.0.0.1:-1"
		}
		leaf {
			listen: "127.0.0.1:-1"
		}
		system_account: "SYS"
	`))
	sb1, ob1 := RunServerWithConfig(ob1Conf)
	defer sb1.Shutdown()

	oaConf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		server_name: "A"
		accounts {
			A { users: [{user: a, password: pwd}] }
			SYS { users: [{user: sys, password: pwd}] }
		}
		gateway {
			name: "A"
			listen: "127.0.0.1:-1"
			gateways [
				{
					name: "B"
					url: "nats://a:pwd@127.0.0.1:%d"
				}
			]
		}
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
		system_account: "SYS"
	`, ob1.Gateway.Port)))
	sa, oa := RunServerWithConfig(oaConf)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb1, 1, 2*time.Second)

	ob2Conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		server_name: "B2"
		accounts {
			A { users: [{user: a, password: pwd}] }
			SYS { users: [{user: sys, password: pwd}] }
		}
		cluster {
			name: "B"
			listen: "127.0.0.1:-1"
			routes: ["nats://127.0.0.1:%d"]
		}
		gateway {
			name: "B"
			listen: "127.0.0.1:-1"
		}
		system_account: "SYS"
	`, ob1.Cluster.Port)))
	sb2, _ := RunServerWithConfig(ob2Conf)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)
	waitForOutboundGateways(t, sb2, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 2, 2*time.Second)

	leafConf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		server_name: "C"
		accounts {
			A { users: [{user: a, password: pwd}] }
			SYS { users: [{user: sys, password: pwd}] }
		}
		leafnodes {
			remotes [
				{ url: "nats://a:pwd@127.0.0.1:%d" }
			]
		}
		system_account: "SYS"
	`, ob1.LeafNode.Port)))
	leaf, _ := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, leaf)

	// Start an independent MQTT server to check MQTT client connection.
	mo := testMQTTDefaultOptions()
	mo.ServerName = "MQTTServer"
	sm := testMQTTRunServer(t, mo)
	defer testMQTTShutdownServer(sm)

	mc, mr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, mo.MQTT.Host, mo.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, mr, mqttConnAckRCConnectionAccepted, false)

	nc := natsConnect(t, sb1.ClientURL(), nats.UserInfo("a", "pwd"))
	defer nc.Close()
	cid, err := nc.GetClientID()
	require_NoError(t, err)
	natsSubSync(t, nc, "foo")
	natsFlush(t, nc)

	ncWS := natsConnect(t, fmt.Sprintf("ws://a:pwd@127.0.0.1:%d", oa.Websocket.Port))
	defer ncWS.Close()
	cidWS, err := ncWS.GetClientID()
	require_NoError(t, err)

	var conns []net.Conn
	var total int
	var ch chan string

	track := func(c *client) {
		total++
		c.mu.Lock()
		conns = append(conns, c.nc)
		c.mu.Unlock()
		runtime.SetFinalizer(c, func(c *client) {
			ch <- fmt.Sprintf("Server=%s - Kind=%s - Conn=%v", c.srv, c.kindString(), c)
		})
	}
	// Track the connection for the MQTT client
	sm.mu.RLock()
	for _, c := range sm.clients {
		track(c)
	}
	sm.mu.RUnlock()

	// Track the connection from the NATS client
	track(sb1.getClient(cid))
	// The outbound connection to GW "A"
	track(sb1.getOutboundGatewayConnection("A"))
	// The inbound connection from GW "A"
	var inGW []*client
	sb1.getInboundGatewayConnections(&inGW)
	track(inGW[0])
	// The routes from sb2
	sb1.forEachRoute(func(r *client) {
		track(r)
	})
	// The leaf form "LEAF"
	sb1.mu.RLock()
	for _, l := range sb1.leafs {
		track(l)
	}
	sb1.mu.RUnlock()

	// Now from sb2, the routes to sb1
	sb2.forEachRoute(func(r *client) {
		track(r)
	})
	// The outbound connection to GW "A"
	track(sb2.getOutboundGatewayConnection("A"))

	// From server "A", track the outbound GW
	track(sa.getOutboundGatewayConnection("B"))
	inGW = inGW[:0]
	// Track the inbound GW connections
	sa.getInboundGatewayConnections(&inGW)
	for _, ig := range inGW {
		track(ig)
	}
	// Track the websocket client
	track(sa.getClient(cidWS))

	// From the LEAF server, the connection to sb1
	leaf.mu.RLock()
	for _, l := range leaf.leafs {
		track(l)
	}
	leaf.mu.RUnlock()

	// Now close all connections and wait to see if all connections
	// with the finalizer set is invoked.
	ch = make(chan string, total)
	// Close the clients and then all other connections to create a disconnect.
	nc.Close()
	mc.Close()
	ncWS.Close()
	for _, conn := range conns {
		conn.Close()
	}
	// Wait and see if we get them all.
	tm := time.NewTimer(10 * time.Second)
	defer tm.Stop()
	tk := time.NewTicker(10 * time.Millisecond)
	for clients := make([]string, 0, total); len(clients) < total; {
		select {
		case <-tk.C:
			runtime.GC()
		case cs := <-ch:
			clients = append(clients, cs)
		case <-tm.C:
			// Don't fail the test since there is no guarantee that
			// finalizers are invoked.
			t.Logf("Got %v out of %v finalizers", len(clients), total)
			slices.Sort(clients)
			for _, cs := range clients {
				t.Logf("  => %s", cs)
			}
			return
		}
	}
}

func TestNoRaceFileStoreMsgLoadNextMsgMultiPerf(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Put 1k msgs in
	for i := 0; i < 1000; i++ {
		subj := fmt.Sprintf("foo.%d", i)
		_, _, err = fs.StoreMsg(subj, nil, []byte("ZZZ"), 0)
		require_NoError(t, err)
	}

	var smv StoreMsg

	// Now do normal load next with no filter.
	// This is baseline.
	start := time.Now()
	for i, seq := 0, uint64(1); i < 1000; i++ {
		sm, nseq, err := fs.LoadNextMsg(_EMPTY_, false, seq, &smv)
		require_NoError(t, err)
		require_Equal(t, sm.subj, fmt.Sprintf("foo.%d", i))
		require_Equal(t, nseq, seq)
		seq++
	}
	baseline := time.Since(start)
	t.Logf("Single - No filter %v", baseline)

	// Allow some additional skew.
	baseline += time.Millisecond

	// Now do normal load next with wc filter.
	start = time.Now()
	for i, seq := 0, uint64(1); i < 1000; i++ {
		sm, nseq, err := fs.LoadNextMsg("foo.>", true, seq, &smv)
		require_NoError(t, err)
		require_Equal(t, sm.subj, fmt.Sprintf("foo.%d", i))
		require_Equal(t, nseq, seq)
		seq++
	}
	elapsed := time.Since(start)
	t.Logf("Single - WC filter %v", elapsed)
	require_LessThan(t, elapsed, 2*baseline)

	// Now do multi load next with 1 wc entry.
	sl := NewSublistWithCache()
	require_NoError(t, sl.Insert(&subscription{subject: []byte("foo.>")}))
	start = time.Now()
	for i, seq := 0, uint64(1); i < 1000; i++ {
		sm, nseq, err := fs.LoadNextMsgMulti(sl, seq, &smv)
		require_NoError(t, err)
		require_Equal(t, sm.subj, fmt.Sprintf("foo.%d", i))
		require_Equal(t, nseq, seq)
		seq++
	}
	elapsed = time.Since(start)
	t.Logf("Multi - Single WC filter %v", elapsed)
	require_LessThan(t, elapsed, 2*baseline)

	// Now do multi load next with 1000 literal subjects.
	sl = NewSublistWithCache()
	for i := 0; i < 1000; i++ {
		subj := fmt.Sprintf("foo.%d", i)
		require_NoError(t, sl.Insert(&subscription{subject: []byte(subj)}))
	}
	start = time.Now()
	for i, seq := 0, uint64(1); i < 1000; i++ {
		sm, nseq, err := fs.LoadNextMsgMulti(sl, seq, &smv)
		require_NoError(t, err)
		require_Equal(t, sm.subj, fmt.Sprintf("foo.%d", i))
		require_Equal(t, nseq, seq)
		seq++
	}
	elapsed = time.Since(start)
	t.Logf("Multi - 1000 filters %v", elapsed)
	require_LessThan(t, elapsed, 3*baseline)
}

func TestNoRaceWQAndMultiSubjectFilters(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"Z.>"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	stopPubs := make(chan bool)

	publish := func(subject string) {
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		for {
			select {
			case <-stopPubs:
				return
			default:
				_, _ = js.Publish(subject, []byte("hello"))
			}
		}
	}

	go publish("Z.foo")
	go publish("Z.bar")
	go publish("Z.baz")

	// Cancel pubs after 10s.
	time.AfterFunc(10*time.Second, func() { close(stopPubs) })

	// Create a consumer
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "zzz",
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        5 * time.Second,
		FilterSubjects: []string{"Z.foo", "Z.bar", "Z.baz"},
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe(_EMPTY_, "zzz", nats.Bind("TEST", "zzz"))
	require_NoError(t, err)

	received := make([]uint64, 0, 256_000)
	batchSize := 10

	for running := true; running; {
		msgs, err := sub.Fetch(batchSize, nats.MaxWait(2*time.Second))
		if err == nats.ErrTimeout {
			running = false
		}
		for _, m := range msgs {
			meta, err := m.Metadata()
			require_NoError(t, err)
			received = append(received, meta.Sequence.Stream)
			m.Ack()
		}
	}

	slices.Sort(received)

	var pseq, gaps uint64
	for _, seq := range received {
		if pseq != 0 && pseq != seq-1 {
			gaps += seq - pseq + 1
		}
		pseq = seq
	}
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	if si.State.Msgs != 0 || gaps > 0 {
		t.Fatalf("Orphaned msgs %d with %d gaps detected", si.State.Msgs, gaps)
	}
}

// https://github.com/nats-io/nats-server/issues/4957
func TestNoRaceWQAndMultiSubjectFiltersRace(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"Z.>"},
		Retention: nats.WorkQueuePolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	// The bug would happen when the stream was on same server as meta-leader.
	// So make that so.
	// Make sure stream leader is on S-1
	sl := c.streamLeader(globalAccountName, "TEST")
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if sl == c.leader() {
			return nil
		}
		// Move meta-leader since stream can be R1.
		snc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
		defer snc.Close()
		if _, err := snc.Request(JSApiLeaderStepDown, nil, time.Second); err != nil {
			return err
		}
		return fmt.Errorf("stream leader on meta-leader")
	})

	start := make(chan struct{})
	var done, ready sync.WaitGroup

	// Create num go routines who will all race to create a consumer with the same filter subject but a different name.
	num := 10
	ready.Add(num)
	done.Add(num)

	for i := 0; i < num; i++ {
		go func(n int) {
			// Connect directly to the meta leader but with our own connection.
			s := c.leader()
			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			ready.Done()
			defer done.Done()
			<-start

			js.AddConsumer("TEST", &nats.ConsumerConfig{
				Name:          fmt.Sprintf("C-%d", n),
				FilterSubject: "Z.foo",
				AckPolicy:     nats.AckExplicitPolicy,
			})
		}(i)
	}

	// Wait for requestors to be ready
	ready.Wait()
	close(start)
	done.Wait()

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		if si.State.Consumers != 1 {
			return fmt.Errorf("Consumer count not correct: %d vs 1", si.State.Consumers)
		}
		return nil
	})
}

func TestNoRaceFileStoreWriteFullStateUniqueSubjects(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	fs, err := newFileStore(fcfg,
		StreamConfig{Name: "zzz", Subjects: []string{"records.>"}, Storage: FileStorage, MaxMsgsPer: 1, MaxBytes: 15 * 1024 * 1024 * 1024})
	require_NoError(t, err)
	defer fs.Stop()

	qch := make(chan struct{})
	defer close(qch)

	go func() {
		const numThreshold = 1_000_000
		tick := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-qch:
				return
			case <-tick.C:
				err := fs.writeFullState()
				var state StreamState
				fs.FastState(&state)
				if state.Msgs > numThreshold && err != nil {
					require_Error(t, err, errStateTooBig)
				}
			}
		}
	}()

	labels := []string{"AAAAA", "BBBB", "CCCC", "DD", "EEEEE"}
	msg := []byte(strings.Repeat("Z", 128))

	for i := 0; i < 100; i++ {
		partA := nuid.Next()
		for j := 0; j < 100; j++ {
			partB := nuid.Next()
			for k := 0; k < 500; k++ {
				partC := nuid.Next()
				partD := labels[rand.Intn(len(labels)-1)]
				subject := fmt.Sprintf("records.%s.%s.%s.%s.%s", partA, partB, partC, partD, nuid.Next())
				start := time.Now()
				fs.StoreMsg(subject, nil, msg, 0)
				elapsed := time.Since(start)
				if elapsed > 500*time.Millisecond {
					t.Fatalf("Slow store for %q: %v\n", subject, elapsed)
				}
			}
		}
	}
	// Make sure we do write the full state on stop.
	fs.Stop()
	fi, err := os.Stat(filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile))
	require_NoError(t, err)
	// ~500MB, could change if we tweak encodings..
	require_True(t, fi.Size() > 500*1024*1024)
}

// When a catchup takes a long time and the ingest rate is high enough to cause us
// to drop append entries and move our first past out catchup window due to max bytes or max msgs, etc.
func TestNoRaceLargeStreamCatchups(t *testing.T) {
	// This usually takes too long on Travis.
	t.Skip()

	var jsMaxOutTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_file_store: 22GB, store_dir: '%s', max_outstanding_catchup: 128KB}
	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}
	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

	c := createJetStreamClusterWithTemplate(t, jsMaxOutTempl, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(64 * 1024))
	require_NoError(t, err)

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"Z.>"},
		MaxBytes: 4 * 1024 * 1024 * 1024,
		Replicas: 1, // Start at R1
	}

	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	// Load up to a decent size first.
	num, msg := 25_000, bytes.Repeat([]byte("Z"), 256*1024)
	for i := 0; i < num; i++ {
		_, err := js.PublishAsync("Z.Z.Z", msg)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(20 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	var sl *Server
	time.AfterFunc(time.Second, func() {
		cfg.Replicas = 3
		_, err = js.UpdateStream(cfg)
		require_NoError(t, err)
		c.waitOnStreamLeader(globalAccountName, "TEST")
		sl = c.streamLeader(globalAccountName, "TEST")
	})

	// Run for 60 seconds sending new messages at a high rate.
	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		for i := 0; i < 5_000; i++ {
			// Not worried about each message, so not checking err here.
			// Just generating high load of new traffic while trying to catch up.
			js.PublishAsync("Z.Z.Z", msg)
		}
		// This will gate us waiting on a response.
		js.Publish("Z.Z.Z", msg)
	}

	// Make sure the leader has not changed.
	require_Equal(t, sl, c.streamLeader(globalAccountName, "TEST"))

	// Grab the leader and its state.
	mset, err := sl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	expected := mset.state()

	checkFor(t, 45*time.Second, time.Second, func() error {
		for _, s := range c.servers {
			if s == sl {
				continue
			}
			mset, err := s.GlobalAccount().lookupStream("TEST")
			require_NoError(t, err)
			state := mset.state()
			if !reflect.DeepEqual(expected, state) {
				return fmt.Errorf("Follower %v state does not match: %+v vs %+v", s, state, expected)
			}
		}
		return nil
	})
}

func TestNoRaceLargeNumDeletesStreamCatchups(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(16 * 1024))
	require_NoError(t, err)

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1, // Start at R1
	}
	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	// We will manipulate the stream at the lower level to achieve large number of interior deletes.
	// We will store only 2 msgs, but have 100M deletes in between.
	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	mset.mu.Lock()
	mset.store.StoreMsg("foo", nil, []byte("ok"), 0)
	mset.store.SkipMsgs(2, 1_000_000_000)
	mset.store.StoreMsg("foo", nil, []byte("ok"), 0)
	mset.store.SkipMsgs(1_000_000_003, 1_000_000_000)
	var state StreamState
	mset.store.FastState(&state)
	mset.lseq = state.LastSeq
	mset.mu.Unlock()

	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")
	mset, err = sl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	expected := mset.state()

	// This should happen fast and not spin on all interior deletes.
	checkFor(t, 250*time.Millisecond, 50*time.Millisecond, func() error {
		for _, s := range c.servers {
			if s == sl {
				continue
			}
			mset, err := s.GlobalAccount().lookupStream("TEST")
			require_NoError(t, err)
			state := mset.state()
			// Ignore LastTime for this test since we send delete range at end.
			state.LastTime = expected.LastTime
			if !reflect.DeepEqual(expected, state) {
				return fmt.Errorf("Follower %v state does not match: %+v vs %+v", s, state, expected)
			}
		}
		return nil
	})
}

func TestNoRaceJetStreamClusterMemoryStreamLastSequenceResetAfterRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	numStreams := 250
	var wg sync.WaitGroup
	wg.Add(numStreams)

	for i := 1; i <= numStreams; i++ {
		go func(n int) {
			defer wg.Done()
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     fmt.Sprintf("TEST:%d", n),
				Storage:  nats.MemoryStorage,
				Subjects: []string{fmt.Sprintf("foo.%d.*", n)},
				Replicas: 3,
			}, nats.MaxWait(30*time.Second))
			require_NoError(t, err)
			subj := fmt.Sprintf("foo.%d.bar", n)
			for i := 0; i < 222; i++ {
				js.Publish(subj, nil)
			}
		}(i)
	}
	wg.Wait()

	// Make sure all streams have a snapshot in place to stress the snapshot logic for memory based streams.
	for _, s := range c.servers {
		for i := 1; i <= numStreams; i++ {
			stream := fmt.Sprintf("TEST:%d", i)
			mset, err := s.GlobalAccount().lookupStream(stream)
			require_NoError(t, err)
			node := mset.raftNode()
			require_NotNil(t, node)
			node.InstallSnapshot(mset.stateSnapshot())
		}
	}

	// Do 5 rolling restarts waiting on healthz in between.
	for i := 0; i < 5; i++ {
		// Walk the servers and shut each down, and wipe the storage directory.
		for _, s := range c.servers {
			s.Shutdown()
			s.WaitForShutdown()
			s = c.restartServer(s)
			c.waitOnServerHealthz(s)
			c.waitOnAllCurrent()
			// Make sure all streams are current after healthz returns ok.
			for i := 1; i <= numStreams; i++ {
				stream := fmt.Sprintf("TEST:%d", i)
				mset, err := s.GlobalAccount().lookupStream(stream)
				require_NoError(t, err)
				var state StreamState
				checkFor(t, 30*time.Second, time.Second, func() error {
					mset.store.FastState(&state)
					if state.LastSeq != 222 {
						return fmt.Errorf("%v Wrong last sequence %d for %q - State  %+v", s, state.LastSeq, stream, state)
					}
					return nil
				})
			}
		}
	}
}

func TestNoRaceJetStreamClusterMemoryWorkQueueLastSequenceResetAfterRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	numStreams := 50
	var wg sync.WaitGroup
	wg.Add(numStreams)

	for i := 1; i <= numStreams; i++ {
		go func(n int) {
			defer wg.Done()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			checkFor(t, 5*time.Second, time.Second, func() error {
				_, err := js.AddStream(&nats.StreamConfig{
					Name:      fmt.Sprintf("TEST:%d", n),
					Storage:   nats.MemoryStorage,
					Retention: nats.WorkQueuePolicy,
					Subjects:  []string{fmt.Sprintf("foo.%d.*", n)},
					Replicas:  3,
				}, nats.MaxWait(time.Second))
				return err
			})

			subj := fmt.Sprintf("foo.%d.bar", n)
			for i := 0; i < 22; i++ {
				checkFor(t, 5*time.Second, time.Second, func() error {
					_, err := js.Publish(subj, nil)
					return err
				})
			}
			// Now consume them all as well.
			var err error
			var sub *nats.Subscription
			checkFor(t, 5*time.Second, time.Second, func() error {
				sub, err = js.PullSubscribe(subj, "wq")
				return err
			})

			var msgs []*nats.Msg
			checkFor(t, 5*time.Second, time.Second, func() error {
				msgs, err = sub.Fetch(22, nats.MaxWait(time.Second))
				return err
			})
			require_Equal(t, len(msgs), 22)
			for _, m := range msgs {
				checkFor(t, 5*time.Second, time.Second, func() error {
					return m.AckSync()
				})
			}
		}(i)
	}
	wg.Wait()

	// Do 2 rolling restarts waiting on healthz in between.
	for i := 0; i < 2; i++ {
		// Walk the servers and shut each down, and wipe the storage directory.
		for _, s := range c.servers {
			s.Shutdown()
			s.WaitForShutdown()
			s = c.restartServer(s)
			c.waitOnServerHealthz(s)
			c.waitOnAllCurrent()
			// Make sure all streams are current after healthz returns ok.
			for i := 1; i <= numStreams; i++ {
				stream := fmt.Sprintf("TEST:%d", i)
				mset, err := s.GlobalAccount().lookupStream(stream)
				require_NoError(t, err)
				var state StreamState
				checkFor(t, 20*time.Second, time.Second, func() error {
					mset.store.FastState(&state)
					if state.LastSeq != 22 {
						return fmt.Errorf("%v Wrong last sequence %d for %q - State  %+v", s, state.LastSeq, stream, state)
					}
					if state.FirstSeq != 23 {
						return fmt.Errorf("%v Wrong first sequence %d for %q - State  %+v", s, state.FirstSeq, stream, state)
					}
					return nil
				})
			}
		}
	}
}

func TestNoRaceJetStreamClusterMirrorSkipSequencingBug(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "ORIGIN",
		Subjects:          []string{"foo.*"},
		MaxMsgsPerSubject: 1,
		Replicas:          1,
		Storage:           nats.MemoryStorage,
	})
	require_NoError(t, err)

	// Create a mirror with R5 such that it will be much slower than the ORIGIN.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Replicas: 5,
		Mirror:   &nats.StreamSource{Name: "ORIGIN"},
	})
	require_NoError(t, err)

	// Connect new directly to ORIGIN
	s := c.streamLeader(globalAccountName, "ORIGIN")
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// We are going to send at a high rate and also delete some along the way
	// via the max msgs per limit.
	for i := 0; i < 500_000; i++ {
		subj := fmt.Sprintf("foo.%d", i)
		js.PublishAsync(subj, nil)
		// Create sequence holes every 100k.
		if i%100_000 == 0 {
			js.PublishAsync(subj, nil)
		}
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkFor(t, 20*time.Second, time.Second, func() error {
		si, err := js.StreamInfo("M")
		require_NoError(t, err)
		if si.State.Msgs != 500_000 {
			return fmt.Errorf("Expected 1M msgs, got state: %+v", si.State)
		}
		return nil
	})
}

func TestNoRaceJetStreamStandaloneDontReplyToAckBeforeProcessingIt(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:                 "WQ",
		Discard:              nats.DiscardNew,
		MaxMsgsPerSubject:    1,
		DiscardNewPerSubject: true,
		Retention:            nats.WorkQueuePolicy,
		Subjects:             []string{"queue.>"},
	})
	require_NoError(t, err)

	// Keep this low since we are going to run as many go routines
	// to consume, ack and republish the message.
	total := 10000
	// Populate the queue, one message per subject.
	for i := 0; i < total; i++ {
		js.Publish(fmt.Sprintf("queue.%d", i), []byte("hello"))
	}

	_, err = js.AddConsumer("WQ", &nats.ConsumerConfig{
		Durable:       "cons",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxWaiting:    20000,
		MaxAckPending: -1,
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("queue.>", "cons", nats.BindStream("WQ"))
	require_NoError(t, err)

	errCh := make(chan error, total)

	var wg sync.WaitGroup
	for iter := 0; iter < 3; iter++ {
		wg.Add(total)
		for i := 0; i < total; i++ {
			go func() {
				defer wg.Done()
				for {
					msgs, err := sub.Fetch(1)
					if err != nil {
						time.Sleep(5 * time.Millisecond)
						continue
					}
					msg := msgs[0]
					err = msg.AckSync()
					if err != nil {
						time.Sleep(5 * time.Millisecond)
						continue
					}
					_, err = js.Publish(msg.Subject, []byte("hello"))
					if err != nil {
						errCh <- err
						return
					}
					break
				}
			}()
		}
		wg.Wait()
		select {
		case err := <-errCh:
			t.Fatalf("Test failed, first error was: %v", err)
		default:
			// OK!
		}
	}
}

// Under certain scenarios an old index.db with a stream that has msg limits set will not restore properly
// due to an old index.db and compaction after the index.db took place which could lose per subject information.
func TestNoRaceFileStoreMsgLimitsAndOldRecoverState(t *testing.T) {
	for _, test := range []struct {
		name             string
		expectedFirstSeq uint64
		expectedLastSeq  uint64
		expectedMsgs     uint64
		transform        func(StreamConfig) StreamConfig
	}{
		{
			name:             "MaxMsgsPer",
			expectedFirstSeq: 10_001,
			expectedLastSeq:  1_010_001,
			expectedMsgs:     1_000_001,
			transform: func(config StreamConfig) StreamConfig {
				config.MaxMsgsPer = 1
				return config
			},
		},
		{
			name:             "MaxMsgs",
			expectedFirstSeq: 10_001,
			expectedLastSeq:  1_010_001,
			expectedMsgs:     1_000_001,
			transform: func(config StreamConfig) StreamConfig {
				config.MaxMsgs = 1_000_001
				return config
			},
		},
		{
			name:             "MaxBytes",
			expectedFirstSeq: 8_624,
			expectedLastSeq:  1_010_001,
			expectedMsgs:     1_001_378,
			transform: func(config StreamConfig) StreamConfig {
				config.MaxBytes = 1_065_353_216
				return config
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sd := t.TempDir()
			fs, err := newFileStore(
				FileStoreConfig{StoreDir: sd},
				test.transform(StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage}),
			)
			require_NoError(t, err)
			defer fs.Stop()

			msg := make([]byte, 1024)

			for i := 0; i < 10_000; i++ {
				subj := fmt.Sprintf("foo.%d", i)
				fs.StoreMsg(subj, nil, msg, 0)
			}

			// This will write the index.db file. We will capture this and use it to replace a new one.
			sfile := filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)
			fs.Stop()
			_, err = os.Stat(sfile)
			require_NoError(t, err)

			// Read it in and make sure len > 0.
			buf, err := os.ReadFile(sfile)
			require_NoError(t, err)
			require_True(t, len(buf) > 0)

			// Restart
			fs, err = newFileStore(
				FileStoreConfig{StoreDir: sd},
				test.transform(StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage}),
			)
			require_NoError(t, err)
			defer fs.Stop()

			// Put in more messages with wider range. This will compact a bunch of the previous blocks.
			for i := 0; i < 1_000_001; i++ {
				subj := fmt.Sprintf("foo.%d", i)
				fs.StoreMsg(subj, nil, msg, 0)
			}

			var ss StreamState
			fs.FastState(&ss)
			require_Equal(t, ss.FirstSeq, test.expectedFirstSeq)
			require_Equal(t, ss.LastSeq, test.expectedLastSeq)
			require_Equal(t, ss.Msgs, test.expectedMsgs)

			// Now stop again, but replace index.db with old one.
			fs.Stop()
			// Put back old stream state.
			require_NoError(t, os.WriteFile(sfile, buf, defaultFilePerms))

			// Restart
			fs, err = newFileStore(
				FileStoreConfig{StoreDir: sd},
				test.transform(StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage}),
			)
			require_NoError(t, err)
			defer fs.Stop()

			fs.FastState(&ss)
			require_Equal(t, ss.FirstSeq, test.expectedFirstSeq)
			require_Equal(t, ss.LastSeq, test.expectedLastSeq)
			require_Equal(t, ss.Msgs, test.expectedMsgs)
		})
	}
}

func TestNoRaceJetStreamClusterCheckInterestStatePerformanceWQ(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3F", 3)
	defer c.shutdown()

	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.*"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	// Load up a bunch of messages for three different subjects.
	msg := bytes.Repeat([]byte("Z"), 4096)
	for i := 0; i < 100_000; i++ {
		js.PublishAsync("foo.foo", msg)
	}
	for i := 0; i < 5_000; i++ {
		js.PublishAsync("foo.bar", msg)
		js.PublishAsync("foo.baz", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// We will not process this one and leave it as "offline".
	_, err = js.PullSubscribe("foo.foo", "A")
	require_NoError(t, err)
	subB, err := js.PullSubscribe("foo.bar", "B")
	require_NoError(t, err)
	subC, err := js.PullSubscribe("foo.baz", "C")
	require_NoError(t, err)

	// Now catch up both B and C but let A simulate being offline of very behind.
	for i := 0; i < 5; i++ {
		for _, sub := range []*nats.Subscription{subB, subC} {
			msgs, err := sub.Fetch(1000)
			require_NoError(t, err)
			require_Equal(t, len(msgs), 1000)
			for _, m := range msgs {
				m.Ack()
			}
		}
	}
	// Let acks process.
	nc.Flush()
	time.Sleep(200 * time.Millisecond)

	// Now test the check checkInterestState() on the stream.
	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	expireAllBlks := func() {
		mset.mu.RLock()
		fs := mset.store.(*fileStore)
		mset.mu.RUnlock()
		fs.mu.RLock()
		for _, mb := range fs.blks {
			mb.tryForceExpireCache()
		}
		fs.mu.RUnlock()
	}

	// First expire all the blocks.
	expireAllBlks()

	start := time.Now()
	mset.checkInterestState()
	elapsed := time.Since(start)
	// This is actually ~300 microseconds but due to travis and race flags etc.
	// Was > 30 ms before fix for comparison, M2 macbook air.
	require_LessThan(t, elapsed, 5*time.Millisecond)

	// Make sure we set the chkflr correctly. The chkflr should be equal to asflr+1.
	// Otherwise, if chkflr would be set higher a subsequent call to checkInterestState will be ineffective.
	requireFloorsEqual := func(o *consumer) {
		t.Helper()
		require_True(t, o != nil)
		o.mu.RLock()
		defer o.mu.RUnlock()
		require_Equal(t, o.chkflr, o.asflr+1)
	}

	requireFloorsEqual(mset.lookupConsumer("A"))
	requireFloorsEqual(mset.lookupConsumer("B"))
	requireFloorsEqual(mset.lookupConsumer("C"))

	// Expire all the blocks again.
	expireAllBlks()

	// This checks the chkflr state.
	start = time.Now()
	mset.checkInterestState()
	elapsed = time.Since(start)
	require_LessThan(t, elapsed, 5*time.Millisecond)
}

func TestNoRaceJetStreamClusterCheckInterestStatePerformanceInterest(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3F", 3)
	defer c.shutdown()

	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.*"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	// We will not process this one and leave it as "offline".
	_, err = js.PullSubscribe("foo.foo", "A")
	require_NoError(t, err)
	_, err = js.PullSubscribe("foo.*", "B")
	require_NoError(t, err)
	// Make subC multi-subject.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "C",
		FilterSubjects: []string{"foo.foo", "foo.bar", "foo.baz"},
		AckPolicy:      nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// Load up a bunch of messages for three different subjects.
	msg := bytes.Repeat([]byte("Z"), 4096)
	for i := 0; i < 90_000; i++ {
		js.PublishAsync("foo.foo", msg)
	}
	for i := 0; i < 5_000; i++ {
		js.PublishAsync("foo.bar", msg)
		js.PublishAsync("foo.baz", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
	// This is so we do not asynchronously update our consumer state after we set the state due to notifications
	// from new messages for the stream.
	time.Sleep(250 * time.Millisecond)

	// Now catch up both B and C but let A simulate being offline of very behind.
	// Will do this manually here to speed up tests.
	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	for _, cname := range []string{"B", "C"} {
		o := mset.lookupConsumer(cname)
		o.mu.Lock()
		o.setStoreState(&ConsumerState{
			Delivered: SequencePair{100_000, 100_000},
			AckFloor:  SequencePair{100_000, 100_000},
		})
		o.mu.Unlock()
	}

	// Now test the check checkInterestState() on the stream.
	start := time.Now()
	mset.checkInterestState()
	elapsed := time.Since(start)

	// Make sure we set the chkflr correctly.
	checkFloor := func(o *consumer) uint64 {
		require_True(t, o != nil)
		o.mu.RLock()
		defer o.mu.RUnlock()
		return o.chkflr
	}

	require_Equal(t, checkFloor(mset.lookupConsumer("A")), 1)
	require_Equal(t, checkFloor(mset.lookupConsumer("B")), 90_001)
	require_Equal(t, checkFloor(mset.lookupConsumer("C")), 100_001)

	// This checks the chkflr state. For this test this should be much faster,
	// two orders of magnitude then the first time.
	start = time.Now()
	mset.checkInterestState()
	require_True(t, time.Since(start) < elapsed/100)
}

func TestNoRaceJetStreamClusterLargeMetaSnapshotTiming(t *testing.T) {
	// This test was to show improvements in speed for marshaling the meta layer with lots of assets.
	// Move to S2.Encode vs EncodeBetter which is 2x faster and actually better compression.
	// Also moved to goccy json which is faster then the default and in my tests now always matches
	// the default encoder byte for byte which last time I checked it did not.
	t.Skip()

	c := createJetStreamClusterExplicit(t, "R3F", 3)
	defer c.shutdown()

	// Create 200 streams, each with 500 consumers.
	numStreams := 200
	numConsumers := 500
	wg := sync.WaitGroup{}
	wg.Add(numStreams)
	for i := 0; i < numStreams; i++ {
		go func() {
			defer wg.Done()
			s := c.randomServer()
			nc, js := jsClientConnect(t, s)
			defer nc.Close()
			sname := fmt.Sprintf("TEST-SNAPSHOT-%d", i)
			subj := fmt.Sprintf("foo.%d", i)
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     sname,
				Subjects: []string{subj},
				Replicas: 3,
			})
			require_NoError(t, err)

			// Now consumers.
			for c := 0; c < numConsumers; c++ {
				_, err = js.AddConsumer(sname, &nats.ConsumerConfig{
					Durable:       fmt.Sprintf("C-%d", c),
					FilterSubject: subj,
					AckPolicy:     nats.AckExplicitPolicy,
					Replicas:      1,
				})
				require_NoError(t, err)
			}
		}()
	}
	wg.Wait()

	s := c.leader()
	js := s.getJetStream()
	n := js.getMetaGroup()
	// Now let's see how long it takes to create a meta snapshot and how big it is.
	start := time.Now()
	snap, err := js.metaSnapshot()
	require_NoError(t, err)
	require_NoError(t, n.InstallSnapshot(snap))
	t.Logf("Took %v to snap meta with size of %v\n", time.Since(start), friendlyBytes(len(snap)))
}

func TestNoRaceStoreReverseWalkWithDeletesPerf(t *testing.T) {
	cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage}

	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	cfg.Storage = MemoryStorage
	ms, err := newMemStore(&cfg)
	require_NoError(t, err)
	defer ms.Stop()

	msg := []byte("Hello")

	for _, store := range []StreamStore{fs, ms} {
		store.StoreMsg("foo.A", nil, msg, 0)
		for i := 0; i < 1_000_000; i++ {
			store.StoreMsg("foo.B", nil, msg, 0)
		}
		store.StoreMsg("foo.C", nil, msg, 0)

		var ss StreamState
		store.FastState(&ss)
		require_Equal(t, ss.Msgs, 1_000_002)

		// Create a bunch of interior deletes.
		p, err := store.PurgeEx("foo.B", 1, 0, true)
		require_NoError(t, err)
		require_Equal(t, p, 1_000_000)

		// Now simulate a walk backwards as we currently do when searching for starting sequence numbers in sourced streams.
		start := time.Now()
		var smv StoreMsg
		for seq := ss.LastSeq; seq > 0; seq-- {
			_, err := store.LoadMsg(seq, &smv)
			if err == errDeletedMsg || err == ErrStoreMsgNotFound {
				continue
			}
			require_NoError(t, err)
		}
		elapsed := time.Since(start)

		// Now use the optimized load prev.
		seq, seen := ss.LastSeq, 0
		start = time.Now()
		for {
			sm, err := store.LoadPrevMsg(seq, &smv)
			if err == ErrStoreEOF {
				break
			}
			require_NoError(t, err)
			seq = sm.seq - 1
			seen++
		}
		elapsedNew := time.Since(start)
		require_Equal(t, seen, 2)

		switch store.(type) {
		case *memStore:
			require_True(t, elapsedNew < elapsed)
		case *fileStore:
			// Bigger gains for filestore, 10x
			require_True(t, elapsedNew*10 < elapsed)
		}
	}
}

type fastProdLogger struct {
	DummyLogger
	gotIt chan struct{}
}

func (l *fastProdLogger) Debugf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, "fast producer") {
		select {
		case l.gotIt <- struct{}{}:
		default:
		}
	}
}

func TestNoRaceNoFastProducerStall(t *testing.T) {
	tmpl := `
		listen: "127.0.0.1:-1"
		no_fast_producer_stall: %s
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, "true")))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	l := &fastProdLogger{gotIt: make(chan struct{}, 1)}
	s.SetLogger(l, true, false)

	ncSlow := natsConnect(t, s.ClientURL())
	defer ncSlow.Close()
	natsSub(t, ncSlow, "foo", func(_ *nats.Msg) {})
	natsFlush(t, ncSlow)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	traceSub := natsSubSync(t, nc, "my.trace.subj")
	natsFlush(t, nc)

	ncProd := natsConnect(t, s.ClientURL())
	defer ncProd.Close()

	payload := make([]byte, 256)

	cid, err := ncSlow.GetClientID()
	require_NoError(t, err)
	c := s.GetClient(cid)
	require_True(t, c != nil)

	wg := sync.WaitGroup{}
	pub := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()

			msg := nats.NewMsg("foo")
			msg.Header.Set(MsgTraceDest, traceSub.Subject)
			msg.Data = payload
			ncProd.PublishMsg(msg)
		}()
	}

	checkTraceMsg := func(err string) {
		t.Helper()
		var e MsgTraceEvent
		traceMsg := natsNexMsg(t, traceSub, time.Second)
		json.Unmarshal(traceMsg.Data, &e)
		egresses := e.Egresses()
		require_Equal(t, len(egresses), 1)
		eg := egresses[0]
		require_Equal(t, eg.CID, cid)
		if err != _EMPTY_ {
			require_Contains(t, eg.Error, err)
		} else {
			require_Equal(t, eg.Error, _EMPTY_)
		}
	}

	// Artificially set a stall channel.
	c.mu.Lock()
	c.out.stc = make(chan struct{})
	c.mu.Unlock()

	// Publish a message, it should not stall the producer.
	pub()
	// Now  make sure we did not get any fast producer debug statements.
	select {
	case <-l.gotIt:
		t.Fatal("Got debug logs about fast producer")
	case <-time.After(250 * time.Millisecond):
		// OK!
	}
	wg.Wait()

	checkTraceMsg(errMsgTraceFastProdNoStall)

	// Now we will conf reload to enable fast producer stalling.
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, "false"))

	// Publish, this time the prod should be stalled.
	pub()
	select {
	case <-l.gotIt:
		// OK!
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timed-out waiting for a warning")
	}
	wg.Wait()

	// Should have been delivered to the trace subscription.
	checkTraceMsg(_EMPTY_)
}

func TestNoRaceProducerStallLimits(t *testing.T) {
	tmpl := `
		listen: "127.0.0.1:-1"
	`
	conf := createConfFile(t, []byte(tmpl))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	ncSlow := natsConnect(t, s.ClientURL())
	defer ncSlow.Close()
	natsSub(t, ncSlow, "foo", func(m *nats.Msg) { m.Respond([]byte("42")) })
	natsFlush(t, ncSlow)

	ncProd := natsConnect(t, s.ClientURL())
	defer ncProd.Close()

	cid, err := ncSlow.GetClientID()
	require_NoError(t, err)
	c := s.GetClient(cid)
	require_True(t, c != nil)

	// Artificially set a stall channel on the subscriber.
	c.mu.Lock()
	c.out.stc = make(chan struct{})
	c.mu.Unlock()

	start := time.Now()
	_, err = ncProd.Request("foo", []byte("HELLO"), time.Second)
	elapsed := time.Since(start)
	require_NoError(t, err)

	// This should have not cleared on its own but should have between min and max pause.
	require_True(t, elapsed >= stallClientMinDuration)
	require_LessThan(t, elapsed, stallClientMaxDuration+5*time.Millisecond)

	// Now test total maximum by loading up a bunch of requests and measuring the last one.
	// Artificially set a stall channel again on the subscriber.
	c.mu.Lock()
	c.out.stc = make(chan struct{})
	// This will prevent us from clearing the stc.
	c.out.pb = c.out.mp/4*3 + 100
	c.mu.Unlock()

	for i := 0; i < 10; i++ {
		err = ncProd.PublishRequest("foo", "bar", []byte("HELLO"))
		require_NoError(t, err)
	}
	start = time.Now()
	_, err = ncProd.Request("foo", []byte("HELLO"), time.Second)
	elapsed = time.Since(start)
	require_NoError(t, err)

	require_True(t, elapsed >= stallTotalAllowed)
	// Should always be close to totalAllowed (e.g. 10ms), but if you run a lot of them in one go can bump up
	// just past it, hence the Max setting below to avoid a flapper.
	require_LessThan(t, elapsed, stallTotalAllowed+20*time.Millisecond)
}

func TestNoRaceJetStreamClusterConsumerDeleteInterestPolicyPerf(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Retention: nats.InterestPolicy,
		Subjects:  []string{"foo.*"},
		Replicas:  3,
	})
	require_NoError(t, err)

	// Make the first sequence high. We already protect against it but for extra sanity.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 100_000_000})
	require_NoError(t, err)

	// Create 3 consumers. 1 Ack explicit, 1 AckAll and 1 AckNone
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C1",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C2",
		AckPolicy: nats.AckAllPolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C3",
		AckPolicy: nats.AckNonePolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 500_000; i++ {
		js.PublishAsync("foo.bar", []byte("ok"))

		// Confirm batch.
		if i%1000 == 0 {
			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}
		}
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	expectedStreamMsgs := func(msgs uint64) {
		t.Helper()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST")
			if err != nil {
				return err
			}
			if si.State.Msgs != msgs {
				return fmt.Errorf("require uint64 equal, but got: %d != %d", si.State.Msgs, msgs)
			}
			return nil
		})
	}
	expectedStreamMsgs(500_000)

	// For C1 grab 100 and ack evens.
	sub, err := js.PullSubscribe("foo.bar", "C1")
	require_NoError(t, err)
	msgs, err := sub.Fetch(100)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 100)
	for _, m := range msgs {
		meta, _ := m.Metadata()
		if meta.Sequence.Stream%2 == 0 {
			require_NoError(t, m.AckSync())
		}
	}

	// For C2 grab 500 and ack 100.
	sub, err = js.PullSubscribe("foo.bar", "C2")
	require_NoError(t, err)
	msgs, err = sub.Fetch(500)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 500)
	require_NoError(t, msgs[99].AckSync())

	// Simulate stream viewer, get first 10 from C3
	sub, err = js.PullSubscribe("foo.bar", "C3")
	require_NoError(t, err)
	msgs, err = sub.Fetch(10)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)

	expectedStreamMsgs(499_995)

	// This test would flake depending on if mset.checkInterestState already ran or not.
	// Manually call it here, because consumers don't retry removing messages below their ack floor.
	for _, s := range c.servers {
		acc, err := s.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		mset.checkInterestState()
	}

	// Before fix this was in the seconds. All the while the stream is locked.
	// This should be short now, but messages might not be cleaned up.
	start := time.Now()
	err = js.DeleteConsumer("TEST", "C3")
	require_NoError(t, err)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Deleting AckNone consumer took too long: %v", elapsed)
	}

	expectedStreamMsgs(499_995)

	// Now do AckAll
	start = time.Now()
	err = js.DeleteConsumer("TEST", "C2")
	require_NoError(t, err)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Deleting AckAll consumer took too long: %v", elapsed)
	}

	expectedStreamMsgs(499_995)

	// Now do AckExplicit
	start = time.Now()
	err = js.DeleteConsumer("TEST", "C1")
	require_NoError(t, err)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Deleting AckExplicit consumer took too long: %v", elapsed)
	}

	expectedStreamMsgs(0)
}

func TestNoRaceJetStreamClusterConsumerDeleteInterestPolicyUniqueFiltersPerf(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Retention: nats.InterestPolicy,
		Subjects:  []string{"foo.*"},
		Replicas:  3,
	})
	require_NoError(t, err)

	// Create 2 consumers. 1 Ack explicit, 1 AckNone
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "C0",
		AckPolicy:      nats.AckExplicitPolicy,
		FilterSubjects: []string{"foo.0"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "C1",
		AckPolicy:      nats.AckNonePolicy,
		FilterSubjects: []string{"foo.1"},
	})
	require_NoError(t, err)

	for i := 0; i < 500_000; i++ {
		subject := fmt.Sprintf("foo.%d", i%2)
		js.PublishAsync(subject, []byte("ok"))

		// Confirm batch.
		if i%1000 == 0 {
			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}
		}
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	expectedStreamMsgs := func(msgs uint64) {
		t.Helper()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST")
			if err != nil {
				return err
			}
			if si.State.Msgs != msgs {
				return fmt.Errorf("require uint64 equal, but got: %d != %d", si.State.Msgs, msgs)
			}
			return nil
		})
	}

	expectedStreamMsgs(500_000)

	// For C0 grab 100 and ack them.
	sub, err := js.PullSubscribe("foo.0", "C0")
	require_NoError(t, err)
	msgs, err := sub.Fetch(100)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 100)
	for _, msg := range msgs {
		require_NoError(t, msg.AckSync())
	}

	expectedStreamMsgs(499_900)

	start := time.Now()
	err = js.DeleteConsumer("TEST", "C1")
	require_NoError(t, err)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Deleting AckNone consumer took too long: %v", elapsed)
	}

	expectedStreamMsgs(499_900)

	start = time.Now()
	err = js.DeleteConsumer("TEST", "C0")
	require_NoError(t, err)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Deleting AckExplicit consumer took too long: %v", elapsed)
	}
	expectedStreamMsgs(0)
}
