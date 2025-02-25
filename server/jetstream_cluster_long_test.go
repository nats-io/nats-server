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

// This test file is skipped by default to avoid accidentally running (e.g. `go test ./server`)
//go:build !skip_js_tests && include_js_long_tests

package server

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// TestLongKVPutWithServerRestarts overwrites values in a replicated KV bucket for a fixed amount of time.
// Also restarts a random server at fixed interval.
// The test fails if updates fail for a continuous interval of time, or if a server fails to restart and catch up.
func TestLongKVPutWithServerRestarts(t *testing.T) {
	// RNG Seed
	const Seed = 123456
	// Number of keys in bucket
	const NumKeys = 1000
	// Size of (random) values
	const ValueSize = 1024
	// Test duration
	const Duration = 3 * time.Minute
	// If no updates successful for this interval, then fail the test
	const MaxRetry = 5 * time.Second
	// Minimum time between put operations
	const UpdatesInterval = 1 * time.Millisecond
	// Time between progress reports to console
	const ProgressInterval = 10 * time.Second
	// Time between server restarts
	const ServerRestartInterval = 5 * time.Second

	type Parameters struct {
		numServers int
		replicas   int
		storage    nats.StorageType
	}

	test := func(t *testing.T, p Parameters) {
		rng := rand.New(rand.NewSource(Seed))

		// Create cluster
		clusterName := fmt.Sprintf("C_%d-%s", p.numServers, p.storage)
		cluster := createJetStreamClusterExplicit(t, clusterName, p.numServers)
		defer cluster.shutdown()

		// Connect to a random server but client will discover others too.
		nc, js := jsClientConnect(t, cluster.randomServer())
		defer nc.Close()

		// Create bucket
		kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:   "TEST",
			Replicas: p.replicas,
			Storage:  p.storage,
		})
		require_NoError(t, err)

		// Initialize list of keys
		keys := make([]string, NumKeys)
		for i := 0; i < NumKeys; i++ {
			keys[i] = fmt.Sprintf("key-%d", i)
		}

		// Initialize keys in bucket with an empty value
		for _, key := range keys {
			_, err := kv.Put(key, []byte{})
			require_NoError(t, err)
		}

		// Track statistics
		var stats = struct {
			start                time.Time
			lastSuccessfulUpdate time.Time
			updateOk             uint64
			updateErr            uint64
			restarts             uint64
			restartsMap          map[string]int
		}{
			start:                time.Now(),
			lastSuccessfulUpdate: time.Now(),
			restartsMap:          make(map[string]int, p.numServers),
		}
		for _, server := range cluster.servers {
			stats.restartsMap[server.Name()] = 0
		}

		// Print statistics
		printProgress := func() {

			t.Logf(
				"[%s] %d updates %d errors, %d server restarts (%v)",
				time.Since(stats.start).Round(time.Second),
				stats.updateOk,
				stats.updateErr,
				stats.restarts,
				stats.restartsMap,
			)
		}

		// Print update on completion
		defer printProgress()

		// Pick a random key and update it with a random value
		valueBuffer := make([]byte, ValueSize)
		updateRandomKey := func() error {
			key := keys[rand.Intn(NumKeys)]
			_, err := rng.Read(valueBuffer)
			require_NoError(t, err)
			_, err = kv.Put(key, valueBuffer)
			return err
		}

		// Set up timers and tickers
		endTestTimer := time.After(Duration)
		nextUpdateTicker := time.NewTicker(UpdatesInterval)
		progressTicker := time.NewTicker(ProgressInterval)
		restartServerTicker := time.NewTicker(ServerRestartInterval)

	runLoop:
		for {
			select {

			case <-endTestTimer:
				break runLoop

			case <-nextUpdateTicker.C:
				err := updateRandomKey()
				if err == nil {
					stats.updateOk++
					stats.lastSuccessfulUpdate = time.Now()
				} else {
					stats.updateErr++
					if time.Since(stats.lastSuccessfulUpdate) > MaxRetry {
						t.Fatalf("Could not successfully update for over %s", MaxRetry)
					}
				}

			case <-restartServerTicker.C:
				randomServer := cluster.servers[rng.Intn(len(cluster.servers))]
				randomServer.Shutdown()
				randomServer.WaitForShutdown()
				restartedServer := cluster.restartServer(randomServer)
				cluster.waitOnClusterReady()
				cluster.waitOnServerHealthz(restartedServer)
				cluster.waitOnAllCurrent()
				stats.restarts++
				stats.restartsMap[randomServer.Name()]++

			case <-progressTicker.C:
				printProgress()
			}
		}
	}

	testCases := []Parameters{
		{numServers: 5, replicas: 3, storage: nats.MemoryStorage},
		{numServers: 5, replicas: 3, storage: nats.FileStorage},
	}

	for _, testCase := range testCases {
		name := fmt.Sprintf("N:%d,R:%d,%s", testCase.numServers, testCase.replicas, testCase.storage)
		t.Run(
			name,
			func(t *testing.T) { test(t, testCase) },
		)
	}
}

func TestLongClusterWorkQueueMessagesNotSkipped(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	iterations := 500_000
	stream := "s1"
	subjf := "subj.>"
	consumers := map[string]string{
		"c1": "subj.c1",
		"c2": "subj.c2.*",
		"c3": "subj.c3",
	}

	sig := make(chan *nats.Msg, 900)

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       stream,
		Storage:    nats.FileStorage,
		Subjects:   []string{subjf},
		Retention:  nats.WorkQueuePolicy,
		Duplicates: time.Minute * 2,
		Replicas:   3,
		MaxAge:     time.Hour,
	})
	require_NoError(t, err)

	for name, subjf := range consumers {
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err = js.AddConsumer(stream, &nats.ConsumerConfig{
			Name:          name,
			FilterSubject: subjf,
			Replicas:      3,
			AckPolicy:     nats.AckExplicitPolicy,
			DeliverPolicy: nats.DeliverAllPolicy,
		})
		require_NoError(t, err)

		ps, err := js.PullSubscribe(subjf, "", nats.Bind(stream, name))
		require_NoError(t, err)

		go func() {
			for {
				msgs, err := ps.FetchBatch(300)
				if errors.Is(err, nats.ErrTimeout) {
					continue
				}
				if errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrSubscriptionClosed) {
					return // ... for when the test finishes
				}
				require_NoError(t, err)
				for msg := range msgs.Messages() {
					go func() {
						time.Sleep(time.Millisecond * time.Duration(rand.Int31n(100)))
						require_NoError(t, msg.Ack())
						sig <- msg
					}()
				}
			}
		}()
	}

	go func() {
		hdrs := nats.Header{}
		for i := 1; i <= iterations; i++ {
			// Pick a random consumer to hit this time (map iteration order is
			// non-deterministic, but break to do it just once).
			for _, subj := range consumers {
				hdrs.Set("Nats-Msg-Id", fmt.Sprintf("msg-%d", i))
				if strings.HasPrefix(subj, "*") {
					subj = strings.Replace(subj, "*", fmt.Sprintf("%d", i), 1)
				}
				_, err := js.PublishMsg(&nats.Msg{
					Subject: subj,
					Header:  hdrs,
				})
				require_NoError(t, err)
				break
			}
		}
	}()

	for i := 1; i <= iterations; i++ {
		if i%10000 == 0 {
			t.Logf("%d messages out of %d", i, iterations)
		}

		select {
		case <-sig:
		case <-time.After(time.Second * 2):
			si, err := js.StreamInfo(stream)
			require_NoError(t, err)
			t.Logf("Stream info: %+v", si.State)

			for name := range consumers {
				ci, err := js.ConsumerInfo(stream, name)
				require_NoError(t, err)
				t.Logf("Consumer %q info: %+v, %+v", name, ci.AckFloor, ci.Delivered)
			}

			t.Fatalf("Didn't receive message %d", i)
		}
	}
}

func TestLongClusterStreamOrphanMsgsAndReplicasDrifting(t *testing.T) {
	type testParams struct {
		restartAny       bool
		restartLeader    bool
		rolloutRestart   bool
		ldmRestart       bool
		restarts         int
		checkHealthz     bool
		reconnectRoutes  bool
		reconnectClients bool
	}
	test := func(t *testing.T, params *testParams, sc *nats.StreamConfig) {
		conf := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {
			store_dir: '%s',
		}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
		server_tags: ["test"]
		system_account: sys
		no_auth_user: js
		accounts {
			sys { users = [ { user: sys, pass: sys } ] }
			js {
				jetstream = enabled
				users = [ { user: js, pass: js } ]
		    }
		}`
		c := createJetStreamClusterWithTemplate(t, conf, sc.Name, 3)
		defer c.shutdown()

		// Update lame duck duration for all servers.
		for _, s := range c.servers {
			s.optsMu.Lock()
			s.opts.LameDuckDuration = 5 * time.Second
			s.opts.LameDuckGracePeriod = -5 * time.Second
			s.optsMu.Unlock()
		}

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		cnc, cjs := jsClientConnect(t, c.randomServer())
		defer cnc.Close()

		_, err := js.AddStream(sc)
		require_NoError(t, err)

		pctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Start producers
		var wg sync.WaitGroup

		// First call is just to create the pull subscribers.
		mp := nats.MaxAckPending(10000)
		mw := nats.PullMaxWaiting(1000)
		aw := nats.AckWait(5 * time.Second)

		for i := 0; i < 10; i++ {
			for _, partition := range []string{"EEEEE"} {
				subject := fmt.Sprintf("MSGS.%s.*.H.100XY.*.*.WQ.00000000000%d", partition, i)
				consumer := fmt.Sprintf("consumer:%s:%d", partition, i)
				_, err := cjs.PullSubscribe(subject, consumer, mp, mw, aw)
				require_NoError(t, err)
			}
		}

		// Create a single consumer that does no activity.
		// Make sure we still calculate low ack properly and cleanup etc.
		_, err = cjs.PullSubscribe("MSGS.ZZ.>", "consumer:ZZ:0", mp, mw, aw)
		require_NoError(t, err)

		subjects := []string{
			"MSGS.EEEEE.P.H.100XY.1.100Z.WQ.000000000000",
			"MSGS.EEEEE.P.H.100XY.1.100Z.WQ.000000000001",
			"MSGS.EEEEE.P.H.100XY.1.100Z.WQ.000000000002",
			"MSGS.EEEEE.P.H.100XY.1.100Z.WQ.000000000003",
			"MSGS.EEEEE.P.H.100XY.1.100Z.WQ.000000000004",
			"MSGS.EEEEE.P.H.100XY.1.100Z.WQ.000000000005",
			"MSGS.EEEEE.P.H.100XY.1.100Z.WQ.000000000006",
			"MSGS.EEEEE.P.H.100XY.1.100Z.WQ.000000000007",
			"MSGS.EEEEE.P.H.100XY.1.100Z.WQ.000000000008",
			"MSGS.EEEEE.P.H.100XY.1.100Z.WQ.000000000009",
		}
		payload := []byte(strings.Repeat("A", 1024))

		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				pnc, pjs := jsClientConnect(t, c.randomServer())
				defer pnc.Close()

				for i := 1; i < 200_000; i++ {
					select {
					case <-pctx.Done():
						wg.Done()
						return
					default:
					}
					for _, subject := range subjects {
						// Send each message a few times.
						msgID := nats.MsgId(nuid.Next())
						pjs.PublishAsync(subject, payload, msgID)
						pjs.Publish(subject, payload, msgID, nats.AckWait(250*time.Millisecond))
						pjs.Publish(subject, payload, msgID, nats.AckWait(250*time.Millisecond))
					}
				}
			}()
		}

		// Rogue publisher that sends the same msg ID everytime.
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				pnc, pjs := jsClientConnect(t, c.randomServer())
				defer pnc.Close()

				msgID := nats.MsgId("1234567890")
				for i := 1; ; i++ {
					select {
					case <-pctx.Done():
						wg.Done()
						return
					default:
					}
					for _, subject := range subjects {
						// Send each message a few times.
						pjs.PublishAsync(subject, payload, msgID, nats.RetryAttempts(0), nats.RetryWait(0))
						pjs.Publish(subject, payload, msgID, nats.AckWait(1*time.Millisecond), nats.RetryAttempts(0), nats.RetryWait(0))
						pjs.Publish(subject, payload, msgID, nats.AckWait(1*time.Millisecond), nats.RetryAttempts(0), nats.RetryWait(0))
					}
				}
			}()
		}

		// Let enough messages into the stream then start consumers.
		time.Sleep(15 * time.Second)

		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		for i := 0; i < 10; i++ {
			subject := fmt.Sprintf("MSGS.EEEEE.*.H.100XY.*.*.WQ.00000000000%d", i)
			consumer := fmt.Sprintf("consumer:EEEEE:%d", i)
			for n := 0; n < 5; n++ {
				cpnc, cpjs := jsClientConnect(t, c.randomServer())
				defer cpnc.Close()

				psub, err := cpjs.PullSubscribe(subject, consumer, mp, mw, aw)
				require_NoError(t, err)

				time.AfterFunc(15*time.Second, func() {
					cpnc.Close()
				})

				wg.Add(1)
				go func() {
					tick := time.NewTicker(1 * time.Millisecond)
					for {
						if cpnc.IsClosed() {
							wg.Done()
							return
						}
						select {
						case <-ctx.Done():
							wg.Done()
							return
						case <-tick.C:
							// Fetch 1 first, then if no errors Fetch 100.
							msgs, err := psub.Fetch(1, nats.MaxWait(200*time.Millisecond))
							if err != nil {
								continue
							}
							for _, msg := range msgs {
								msg.Ack()
							}
							msgs, err = psub.Fetch(100, nats.MaxWait(200*time.Millisecond))
							if err != nil {
								continue
							}
							for _, msg := range msgs {
								msg.Ack()
							}
							msgs, err = psub.Fetch(1000, nats.MaxWait(200*time.Millisecond))
							if err != nil {
								continue
							}
							for _, msg := range msgs {
								msg.Ack()
							}
						}
					}
				}()
			}
		}

		for i := 0; i < 10; i++ {
			subject := fmt.Sprintf("MSGS.EEEEE.*.H.100XY.*.*.WQ.00000000000%d", i)
			consumer := fmt.Sprintf("consumer:EEEEE:%d", i)
			for n := 0; n < 10; n++ {
				cpnc, cpjs := jsClientConnect(t, c.randomServer())
				defer cpnc.Close()

				psub, err := cpjs.PullSubscribe(subject, consumer, mp, mw, aw)
				if err != nil {
					t.Logf("ERROR: %v", err)
					continue
				}

				wg.Add(1)
				go func() {
					tick := time.NewTicker(1 * time.Millisecond)
					for {
						select {
						case <-ctx.Done():
							wg.Done()
							return
						case <-tick.C:
							// Fetch 1 first, then if no errors Fetch 100.
							msgs, err := psub.Fetch(1, nats.MaxWait(200*time.Millisecond))
							if err != nil {
								continue
							}
							for _, msg := range msgs {
								msg.Ack()
							}
							msgs, err = psub.Fetch(100, nats.MaxWait(200*time.Millisecond))
							if err != nil {
								continue
							}
							for _, msg := range msgs {
								msg.Ack()
							}

							msgs, err = psub.Fetch(1000, nats.MaxWait(200*time.Millisecond))
							if err != nil {
								continue
							}
							for _, msg := range msgs {
								msg.Ack()
							}
						}
					}
				}()
			}
		}

		// Periodically disconnect routes from one of the servers.
		if params.reconnectRoutes {
			wg.Add(1)
			go func() {
				for range time.NewTicker(10 * time.Second).C {
					select {
					case <-ctx.Done():
						wg.Done()
						return
					default:
					}

					// Force disconnecting routes from one of the servers.
					s := c.servers[rand.Intn(3)]
					var routes []*client
					t.Logf("Disconnecting routes from %v", s.Name())
					s.mu.Lock()
					for _, conns := range s.routes {
						routes = append(routes, conns...)
					}
					s.mu.Unlock()
					for _, r := range routes {
						r.closeConnection(ClientClosed)
					}
				}
			}()
		}

		// Periodically reconnect clients.
		if params.reconnectClients {
			reconnectClients := func(s *Server) {
				for _, client := range s.clients {
					client.closeConnection(Kicked)
				}
			}

			wg.Add(1)
			go func() {
				for range time.NewTicker(10 * time.Second).C {
					select {
					case <-ctx.Done():
						wg.Done()
						return
					default:
					}
					// Force reconnect clients from one of the servers.
					s := c.servers[rand.Intn(len(c.servers))]
					reconnectClients(s)
				}
			}()
		}

		// Restarts
		time.AfterFunc(10*time.Second, func() {
			for i := 0; i < params.restarts; i++ {
				switch {
				case params.restartLeader:
					// Find server leader of the stream and restart it.
					s := c.streamLeader("js", sc.Name)
					if params.ldmRestart {
						s.lameDuckMode()
					} else {
						s.Shutdown()
					}
					s.WaitForShutdown()
					c.restartServer(s)
				case params.restartAny:
					s := c.servers[rand.Intn(len(c.servers))]
					if params.ldmRestart {
						s.lameDuckMode()
					} else {
						s.Shutdown()
					}
					s.WaitForShutdown()
					c.restartServer(s)
				case params.rolloutRestart:
					for _, s := range c.servers {
						if params.ldmRestart {
							s.lameDuckMode()
						} else {
							s.Shutdown()
						}
						s.WaitForShutdown()
						c.restartServer(s)

						if params.checkHealthz {
							hctx, hcancel := context.WithTimeout(ctx, 15*time.Second)
							defer hcancel()

							for range time.NewTicker(2 * time.Second).C {
								select {
								case <-hctx.Done():
								default:
								}

								status := s.healthz(nil)
								if status.StatusCode == 200 {
									break
								}
							}
						}
					}
				}
				c.waitOnClusterReady()
			}
		})

		// Wait until context is done then check state.
		<-ctx.Done()

		getStreamDetails := func(t *testing.T, srv *Server) *StreamDetail {
			t.Helper()
			jsz, err := srv.Jsz(&JSzOptions{Accounts: true, Streams: true, Consumer: true})
			require_NoError(t, err)
			if len(jsz.AccountDetails) > 0 && len(jsz.AccountDetails[0].Streams) > 0 {
				stream := jsz.AccountDetails[0].Streams[0]
				return &stream
			}
			t.Error("Could not find account details")
			return nil
		}

		checkState := func(t *testing.T) error {
			t.Helper()

			leaderSrv := c.streamLeader("js", sc.Name)
			if leaderSrv == nil {
				return fmt.Errorf("no leader found for stream")
			}
			streamLeader := getStreamDetails(t, leaderSrv)
			var errs []error
			for _, srv := range c.servers {
				if srv == leaderSrv {
					// Skip self
					continue
				}
				stream := getStreamDetails(t, srv)
				if stream == nil {
					return fmt.Errorf("stream not found")
				}

				if stream.State.Msgs != streamLeader.State.Msgs {
					err := fmt.Errorf("Leader %v has %d messages, Follower %v has %d messages",
						stream.Cluster.Leader, streamLeader.State.Msgs,
						srv, stream.State.Msgs,
					)
					errs = append(errs, err)
				}
				if stream.State.FirstSeq != streamLeader.State.FirstSeq {
					err := fmt.Errorf("Leader %v FirstSeq is %d, Follower %v is at %d",
						stream.Cluster.Leader, streamLeader.State.FirstSeq,
						srv, stream.State.FirstSeq,
					)
					errs = append(errs, err)
				}
				if stream.State.LastSeq != streamLeader.State.LastSeq {
					err := fmt.Errorf("Leader %v LastSeq is %d, Follower %v is at %d",
						stream.Cluster.Leader, streamLeader.State.LastSeq,
						srv, stream.State.LastSeq,
					)
					errs = append(errs, err)
				}
			}
			if len(errs) > 0 {
				return errors.Join(errs...)
			}
			return nil
		}

		checkMsgsEqual := func(t *testing.T) {
			// These have already been checked to be the same for all streams.
			state := getStreamDetails(t, c.streamLeader("js", sc.Name)).State
			// Gather the stream mset from each replica.
			var msets []*stream
			for _, s := range c.servers {
				acc, err := s.LookupAccount("js")
				require_NoError(t, err)
				mset, err := acc.lookupStream(sc.Name)
				require_NoError(t, err)
				msets = append(msets, mset)
			}
			for seq := state.FirstSeq; seq <= state.LastSeq; seq++ {
				var expectedErr error
				var msgId string
				var smv StoreMsg
				for _, mset := range msets {
					mset.mu.RLock()
					sm, err := mset.store.LoadMsg(seq, &smv)
					mset.mu.RUnlock()
					if err != nil || expectedErr != nil {
						// If one of the msets reports an error for LoadMsg for this
						// particular sequence, then the same error should be reported
						// by all msets for that seq to prove consistency across replicas.
						// If one of the msets either returns no error or doesn't return
						// the same error, then that replica has drifted.
						if expectedErr == nil {
							expectedErr = err
						} else {
							require_Error(t, err, expectedErr)
						}
						continue
					}
					if msgId == _EMPTY_ {
						msgId = string(sm.hdr)
					} else if msgId != string(sm.hdr) {
						t.Fatalf("MsgIds do not match for seq %d: %q vs %q", seq, msgId, sm.hdr)
					}
				}
			}
		}

		// Wait for test to finish before checking state.
		wg.Wait()

		// If clustered, check whether leader and followers have drifted.
		if sc.Replicas > 1 {
			// If we have drifted do not have to wait too long, usually it's stuck for good.
			checkFor(t, 5*time.Minute, time.Second, func() error {
				return checkState(t)
			})
			// If we succeeded now let's check that all messages are also the same.
			// We may have no messages but for tests that do we make sure each msg is the same
			// across all replicas.
			checkMsgsEqual(t)
		}

		err = checkForErr(2*time.Minute, time.Second, func() error {
			var consumerPending int
			consumers := make(map[string]int)
			for i := 0; i < 10; i++ {
				consumerName := fmt.Sprintf("consumer:EEEEE:%d", i)
				ci, err := js.ConsumerInfo(sc.Name, consumerName)
				if err != nil {
					return err
				}
				pending := int(ci.NumPending)
				consumers[consumerName] = pending
				consumerPending += pending
			}

			// Only check if there are any pending messages.
			if consumerPending > 0 {
				// Check state of streams and consumers.
				si, err := js.StreamInfo(sc.Name, &nats.StreamInfoRequest{SubjectsFilter: ">"})
				if err != nil {
					return err
				}
				streamPending := int(si.State.Msgs)
				// FIXME: Num pending can be out of sync from the number of stream messages in the subject.
				if streamPending != consumerPending {
					return fmt.Errorf("Unexpected number of pending messages, stream=%d, consumers=%d \n subjects: %+v\nconsumers: %+v",
						streamPending, consumerPending, si.State.Subjects, consumers)
				}
			}
			return nil
		})
		if err != nil {
			t.Errorf("WRN: %v", err)
		}
	}

	// Setting up test variations below:
	//
	// File based with single replica and discard old policy.
	t.Run("R1F", func(t *testing.T) {
		params := &testParams{
			restartAny:     true,
			ldmRestart:     false,
			rolloutRestart: false,
			restarts:       1,
		}
		test(t, params, &nats.StreamConfig{
			Name:        "OWQTEST_R1F",
			Subjects:    []string{"MSGS.>"},
			Replicas:    1,
			MaxAge:      30 * time.Minute,
			Duplicates:  5 * time.Minute,
			Retention:   nats.WorkQueuePolicy,
			Discard:     nats.DiscardOld,
			AllowRollup: true,
			Placement: &nats.Placement{
				Tags: []string{"test"},
			},
		})
	})

	// Clustered memory based with discard new policy and max msgs limit.
	t.Run("R3M", func(t *testing.T) {
		params := &testParams{
			restartAny:     true,
			ldmRestart:     true,
			rolloutRestart: false,
			restarts:       1,
			checkHealthz:   true,
		}
		test(t, params, &nats.StreamConfig{
			Name:        "OWQTEST_R3M",
			Subjects:    []string{"MSGS.>"},
			Replicas:    3,
			MaxAge:      30 * time.Minute,
			MaxMsgs:     100_000,
			Duplicates:  5 * time.Minute,
			Retention:   nats.WorkQueuePolicy,
			Discard:     nats.DiscardNew,
			AllowRollup: true,
			Storage:     nats.MemoryStorage,
			Placement: &nats.Placement{
				Tags: []string{"test"},
			},
		})
	})

	// Clustered file based with discard new policy and max msgs limit.
	t.Run("R3F_DN", func(t *testing.T) {
		params := &testParams{
			restartAny:     true,
			ldmRestart:     true,
			rolloutRestart: false,
			restarts:       1,
			checkHealthz:   true,
		}
		test(t, params, &nats.StreamConfig{
			Name:        "OWQTEST_R3F_DN",
			Subjects:    []string{"MSGS.>"},
			Replicas:    3,
			MaxAge:      30 * time.Minute,
			MaxMsgs:     100_000,
			Duplicates:  5 * time.Minute,
			Retention:   nats.WorkQueuePolicy,
			Discard:     nats.DiscardNew,
			AllowRollup: true,
			Placement: &nats.Placement{
				Tags: []string{"test"},
			},
		})
	})

	// Clustered file based with discard old policy and max msgs limit.
	t.Run("R3F_DO", func(t *testing.T) {
		params := &testParams{
			restartAny:     true,
			ldmRestart:     true,
			rolloutRestart: false,
			restarts:       1,
			checkHealthz:   true,
		}
		test(t, params, &nats.StreamConfig{
			Name:        "OWQTEST_R3F_DO",
			Subjects:    []string{"MSGS.>"},
			Replicas:    3,
			MaxAge:      30 * time.Minute,
			MaxMsgs:     100_000,
			Duplicates:  5 * time.Minute,
			Retention:   nats.WorkQueuePolicy,
			Discard:     nats.DiscardOld,
			AllowRollup: true,
			Placement: &nats.Placement{
				Tags: []string{"test"},
			},
		})
	})

	// Clustered file based with discard old policy and no limits.
	t.Run("R3F_DO_NOLIMIT", func(t *testing.T) {
		params := &testParams{
			restartAny:     true,
			ldmRestart:     true,
			rolloutRestart: false,
			restarts:       1,
			checkHealthz:   true,
		}
		test(t, params, &nats.StreamConfig{
			Name:       "OWQTEST_R3F_DO_NOLIMIT",
			Subjects:   []string{"MSGS.>"},
			Replicas:   3,
			Duplicates: 30 * time.Second,
			Discard:    nats.DiscardOld,
			Placement: &nats.Placement{
				Tags: []string{"test"},
			},
		})
	})
}

func TestLongClusterCLFSOnDuplicates(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	nc2, js2 := jsClientConnect(t, c.randomServer())
	defer nc2.Close()

	streamName := "TESTW"
	_, err := js.AddStream(&nats.StreamConfig{
		Name:       streamName,
		Subjects:   []string{"foo"},
		Replicas:   3,
		Storage:    nats.FileStorage,
		MaxAge:     3 * time.Minute,
		Duplicates: 2 * time.Minute,
	})
	require_NoError(t, err)

	c.waitOnStreamLeader(globalAccountName, streamName)

	var wg sync.WaitGroup

	// The test will be successful if it runs for this long without dup issues.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	go func() {
		tick := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			case <-tick.C:
				c.streamLeader(globalAccountName, streamName).JetStreamStepdownStream(globalAccountName, streamName)
			}
		}
	}()
	wg.Add(1)

	for i := 0; i < 5; i++ {
		go func(i int) {
			var err error
			sub, err := js2.PullSubscribe("foo", fmt.Sprintf("A:%d", i))
			require_NoError(t, err)

			for {
				select {
				case <-ctx.Done():
					wg.Done()
					return
				default:
				}

				msgs, err := sub.Fetch(100, nats.MaxWait(200*time.Millisecond))
				if err != nil {
					continue
				}
				for _, msg := range msgs {
					msg.Ack()
				}
			}
		}(i)
		wg.Add(1)
	}

	// Sync producer that only does a couple of duplicates, cancel the test
	// if we get too many errors without responses.
	errCh := make(chan error, 10)
	go func() {
		// Try sync publishes normally in this state and see if it times out.
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			default:
			}

			var succeeded bool
			var failures int
			for n := 0; n < 10; n++ {
				_, err := js.Publish(
					"foo", []byte("test"),
					nats.MsgId(fmt.Sprintf("sync:checking:%d", i)),
					nats.RetryAttempts(30),
					nats.AckWait(500*time.Millisecond),
				)
				if err != nil {
					failures++
					continue
				}
				succeeded = true
			}
			if !succeeded {
				errCh <- fmt.Errorf("Too many publishes failed with timeout: failures=%d, i=%d", failures, i)
			}
		}
	}()
	wg.Add(1)

Loop:
	for n := uint64(0); true; n++ {
		select {
		case <-ctx.Done():
			break Loop
		case e := <-errCh:
			t.Error(e)
			break Loop
		default:
		}
		// Cause a lot of duplicates very fast until producer stalls.
		for i := 0; i < 128; i++ {
			msgID := nats.MsgId(fmt.Sprintf("id.%d.%d", n, i))
			js.PublishAsync("foo", []byte("test"), msgID, nats.RetryAttempts(10))
		}
	}
	cancel()
	wg.Wait()
}

func TestLongClusterJetStreamRestartThenScaleStreamReplicas(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	nc2, producer := jsClientConnect(t, s)
	defer nc2.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	end := time.Now().Add(2 * time.Second)
	for time.Now().Before(end) {
		producer.Publish("foo", []byte(strings.Repeat("A", 128)))
		time.Sleep(time.Millisecond)
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		sub, err := js.PullSubscribe("foo", fmt.Sprintf("C-%d", i))
		require_NoError(t, err)

		wg.Add(1)
		go func() {
			defer wg.Done()
			for range time.NewTicker(10 * time.Millisecond).C {
				select {
				case <-ctx.Done():
					return
				default:
				}

				msgs, err := sub.Fetch(1)
				if err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, nats.ErrConnectionClosed) {
					t.Logf("Pull Error: %v", err)
				}
				for _, msg := range msgs {
					msg.Ack()
				}
			}
		}()
	}
	c.lameDuckRestartAll()
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Swap the logger to try to detect the condition after the restart.
	loggers := make([]*captureDebugLogger, 3)
	for i, srv := range c.servers {
		l := &captureDebugLogger{dbgCh: make(chan string, 10)}
		loggers[i] = l
		srv.SetLogger(l, true, false)
	}
	condition := `Direct proposal ignored, not leader (state: CLOSED)`
	errCh := make(chan error, 10)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case dl := <-loggers[0].dbgCh:
				if strings.Contains(dl, condition) {
					errCh <- errors.New(condition)
				}
			case dl := <-loggers[1].dbgCh:
				if strings.Contains(dl, condition) {
					errCh <- errors.New(condition)
				}
			case dl := <-loggers[2].dbgCh:
				if strings.Contains(dl, condition) {
					errCh <- errors.New(condition)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start publishing again for a while.
	end = time.Now().Add(2 * time.Second)
	for time.Now().Before(end) {
		producer.Publish("foo", []byte(strings.Repeat("A", 128)))
		time.Sleep(time.Millisecond)
	}

	// Try to do a stream edit back to R=1 after doing all the upgrade.
	info, _ := js.StreamInfo("TEST")
	sconfig := info.Config
	sconfig.Replicas = 1
	_, err = js.UpdateStream(&sconfig)
	require_NoError(t, err)

	// Leave running for some time after the update.
	time.Sleep(2 * time.Second)

	info, _ = js.StreamInfo("TEST")
	sconfig = info.Config
	sconfig.Replicas = 3
	_, err = js.UpdateStream(&sconfig)
	require_NoError(t, err)

	select {
	case e := <-errCh:
		t.Fatalf("Bad condition on raft node: %v", e)
	case <-time.After(2 * time.Second):
		// Done
	}

	// Stop goroutines and wait for them to exit.
	cancel()
	wg.Wait()
}

func TestLongClusterJetStreamBusyStreams(t *testing.T) {
	type streamSetup struct {
		config    *nats.StreamConfig
		consumers []*nats.ConsumerConfig
		subjects  []string
	}
	type job func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext, c *cluster)
	type testParams struct {
		cluster         string
		streams         []*streamSetup
		producers       int
		consumers       int
		restartAny      bool
		restartWait     time.Duration
		ldmRestart      bool
		rolloutRestart  bool
		restarts        int
		checkHealthz    bool
		jobs            []job
		expect          job
		duration        time.Duration
		producerMsgs    int
		producerMsgSize int
	}
	test := func(t *testing.T, test *testParams) {
		conf := `
                listen: 127.0.0.1:-1
                http: 127.0.0.1:-1
                server_name: %s
                jetstream: {
                        domain: "cloud"
                        store_dir: '%s',
                }
                cluster {
                        name: %s
                        listen: 127.0.0.1:%d
                        routes = [%s]
                }
                server_tags: ["test"]
                system_account: sys

                no_auth_user: js
                accounts {
                        sys { users = [ { user: sys, pass: sys } ] }

                        js  { jetstream = enabled
                              users = [ { user: js, pass: js } ]
                        }
                }`
		c := createJetStreamClusterWithTemplate(t, conf, test.cluster, 3)
		defer c.shutdown()
		for _, s := range c.servers {
			s.optsMu.Lock()
			s.opts.LameDuckDuration = 15 * time.Second
			s.opts.LameDuckGracePeriod = -15 * time.Second
			s.optsMu.Unlock()
		}

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		var wg sync.WaitGroup
		for _, stream := range test.streams {
			stream := stream
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := js.AddStream(stream.config)
				require_NoError(t, err)

				for _, consumer := range stream.consumers {
					_, err := js.AddConsumer(stream.config.Name, consumer)
					require_NoError(t, err)
					_, err = js.PullSubscribe(consumer.FilterSubject, "", nats.Bind(stream.config.Name, consumer.Name))
					if err != nil {
						t.Logf("WRN: Failed binding to pull subscriber: %v - %v - %v - %v",
							consumer.FilterSubject, stream.config.Name, consumer.Name, err)
					}
				}
			}()
		}
		wg.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), test.duration)
		defer cancel()
		for _, stream := range test.streams {
			payload := []byte(strings.Repeat("A", test.producerMsgSize))
			stream := stream
			subjects := stream.subjects

			// Create publishers on different connections that sends messages
			// to all the consumers subjects.
			var n atomic.Uint64
			for i := 0; i < test.producers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					nc, js := jsClientConnect(t, c.randomServer())
					defer nc.Close()

					for range time.NewTicker(1 * time.Millisecond).C {
						select {
						case <-ctx.Done():
							return
						default:
						}

						for _, subject := range subjects {
							_, err := js.Publish(subject, payload, nats.AckWait(200*time.Millisecond))
							if err == nil {
								if nn := n.Add(1); int(nn) >= test.producerMsgs {
									return
								}
							}
						}
					}
				}()
			}

			// Create multiple parallel pull subscribers per consumer config.
			for i := 0; i < test.consumers; i++ {
				for _, consumer := range stream.consumers {
					wg.Add(1)

					consumer := consumer
					go func() {
						defer wg.Done()

						for attempts := 0; attempts < 60; attempts++ {
							_, err := js.ConsumerInfo(stream.config.Name, consumer.Name)
							if err != nil && err != context.DeadlineExceeded {
								t.Logf("WRN: Failed calling consumer info: %v - %v - %v - %v - attempts:%v - %v",
									consumer.FilterSubject, stream.config.Name, consumer.Name, err, attempts, nc.ConnectedServerName())
							}
						}
						sub, err := js.PullSubscribe(consumer.FilterSubject, "", nats.Bind(stream.config.Name, consumer.Name))
						if err != nil {
							t.Logf("WRN: Failed creating pull subscriber: %v - %v - %v - %v",
								consumer.FilterSubject, stream.config.Name, consumer.Name, err)
							return
						}
						require_NoError(t, err)

						for range time.NewTicker(100 * time.Millisecond).C {
							select {
							case <-ctx.Done():
								return
							default:
							}

							msgs, err := sub.Fetch(1, nats.MaxWait(200*time.Millisecond))
							if err != nil {
								continue
							}
							for _, msg := range msgs {
								msg.Ack()
							}

							msgs, err = sub.Fetch(100, nats.MaxWait(200*time.Millisecond))
							if err != nil {
								continue
							}
							for _, msg := range msgs {
								msg.Ack()
							}
						}
					}()
				}
			}
		}

		for _, job := range test.jobs {
			go job(t, nc, js, c)
		}
		if test.restarts > 0 {
			wg.Add(1)
			time.AfterFunc(test.restartWait, func() {
				defer wg.Done()
				for i := 0; i < test.restarts; i++ {
					t.Logf("Restarting: %v time", i+1)
					switch {
					case test.restartAny:
						s := c.servers[rand.Intn(len(c.servers))]
						if test.ldmRestart {
							s.lameDuckMode()
						} else {
							s.Shutdown()
						}
						s.WaitForShutdown()
						c.restartServer(s)
					case test.rolloutRestart:
						for _, s := range c.servers {
							if test.ldmRestart {
								s.lameDuckMode()
							} else {
								s.Shutdown()
							}
							s.WaitForShutdown()
							s = c.restartServer(s)

							if test.checkHealthz {
								hctx, hcancel := context.WithTimeout(ctx, 15*time.Second)
								defer hcancel()

							Healthz:
								for range time.NewTicker(2 * time.Second).C {
									select {
									case <-hctx.Done():
										break Healthz
									default:
									}

									status := s.healthz(nil)
									if status.StatusCode == 200 {
										break Healthz
									}
								}
							}
						}
					}
					c.waitOnClusterReady()
				}
			})
		}
		test.expect(t, nc, js, c)
		cancel()
		wg.Wait()
	}
	stepDown := func(nc *nats.Conn, streamName string) {
		nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, streamName), nil, time.Second)
	}
	getStreamDetails := func(t *testing.T, c *cluster, accountName, streamName string) *StreamDetail {
		t.Helper()
		srv := c.streamLeader(accountName, streamName)
		jsz, err := srv.Jsz(&JSzOptions{Accounts: true, Streams: true, Consumer: true})
		require_NoError(t, err)
		for _, acc := range jsz.AccountDetails {
			if acc.Name == accountName {
				for _, stream := range acc.Streams {
					if stream.Name == streamName {
						return &stream
					}
				}
			}
		}
		t.Error("Could not find account details")
		return nil
	}
	checkMsgsEqual := func(t *testing.T, c *cluster, accountName, streamName string) {
		state := getStreamDetails(t, c, accountName, streamName).State
		var msets []*stream
		for _, s := range c.servers {
			acc, err := s.LookupAccount(accountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream(streamName)
			require_NoError(t, err)
			msets = append(msets, mset)
		}
		for seq := state.FirstSeq; seq <= state.LastSeq; seq++ {
			var msgId string
			var smv StoreMsg
			for _, mset := range msets {
				mset.mu.RLock()
				sm, err := mset.store.LoadMsg(seq, &smv)
				mset.mu.RUnlock()
				require_NoError(t, err)
				if msgId == _EMPTY_ {
					msgId = string(sm.hdr)
				} else if msgId != string(sm.hdr) {
					t.Fatalf("MsgIds do not match for seq %d: %q vs %q", seq, msgId, sm.hdr)
				}
			}
		}
	}
	checkConsumer := func(t *testing.T, c *cluster, accountName, streamName, consumerName string) {
		t.Helper()
		var leader string
		for _, s := range c.servers {
			jsz, err := s.Jsz(&JSzOptions{Accounts: true, Streams: true, Consumer: true})
			require_NoError(t, err)
			for _, acc := range jsz.AccountDetails {
				if acc.Name == accountName {
					for _, stream := range acc.Streams {
						if stream.Name == streamName {
							for _, consumer := range stream.Consumer {
								if leader == "" {
									leader = consumer.Cluster.Leader
								} else if leader != consumer.Cluster.Leader {
									t.Errorf("There are two leaders for %s/%s: %s vs %s",
										stream.Name, consumer.Name, leader, consumer.Cluster.Leader)
								}
							}
						}
					}
				}
			}
		}
	}

	t.Run("R1F/rescale/R3F/sources:10/limits", func(t *testing.T) {
		testDuration := 3 * time.Minute
		totalStreams := 10
		streams := make([]*streamSetup, totalStreams)
		sources := make([]*nats.StreamSource, totalStreams)
		for i := 0; i < totalStreams; i++ {
			name := fmt.Sprintf("test:%d", i)
			st := &streamSetup{
				config: &nats.StreamConfig{
					Name:      name,
					Subjects:  []string{fmt.Sprintf("test.%d.*", i)},
					Replicas:  1,
					Retention: nats.LimitsPolicy,
				},
			}
			st.subjects = append(st.subjects, fmt.Sprintf("test.%d.0", i))
			sources[i] = &nats.StreamSource{Name: name}
			streams[i] = st
		}

		// Create Source consumer.
		sourceSetup := &streamSetup{
			config: &nats.StreamConfig{
				Name:      "source-test",
				Replicas:  1,
				Retention: nats.LimitsPolicy,
				Sources:   sources,
			},
			consumers: make([]*nats.ConsumerConfig, 0),
		}
		cc := &nats.ConsumerConfig{
			Name:          "A",
			Durable:       "A",
			FilterSubject: "test.>",
			AckPolicy:     nats.AckExplicitPolicy,
		}
		sourceSetup.consumers = append(sourceSetup.consumers, cc)
		streams = append(streams, sourceSetup)

		scale := func(replicas int, wait time.Duration) job {
			return func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext, c *cluster) {
				config := sourceSetup.config
				time.AfterFunc(wait, func() {
					config.Replicas = replicas
					for i := 0; i < 10; i++ {
						_, err := js.UpdateStream(config)
						if err == nil {
							return
						}
						time.Sleep(1 * time.Second)
					}
				})
			}
		}

		expect := func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext, c *cluster) {
			// The source stream should not be stuck or be different from the other streams.
			time.Sleep(testDuration + 1*time.Minute)
			accName := "js"
			streamName := "source-test"

			// Check a few times to see if there are no changes in the number of messages.
			var changed bool
			var prevMsgs uint64
			for i := 0; i < 10; i++ {
				sinfo, err := js.StreamInfo(streamName)
				if err != nil {
					t.Logf("Error: %v", err)
					time.Sleep(2 * time.Second)
					continue
				}
				prevMsgs = sinfo.State.Msgs
			}
			for i := 0; i < 10; i++ {
				sinfo, err := js.StreamInfo(streamName)
				if err != nil {
					t.Logf("Error: %v", err)
					time.Sleep(2 * time.Second)
					continue
				}
				changed = prevMsgs != sinfo.State.Msgs
				prevMsgs = sinfo.State.Msgs
				time.Sleep(2 * time.Second)
			}
			if !changed {
				// Doing a leader step down should not cause the messages to change.
				stepDown(nc, streamName)

				for i := 0; i < 10; i++ {
					sinfo, err := js.StreamInfo(streamName)
					if err != nil {
						t.Logf("Error: %v", err)
						time.Sleep(2 * time.Second)
						continue
					}
					changed = prevMsgs != sinfo.State.Msgs
					prevMsgs = sinfo.State.Msgs
					time.Sleep(2 * time.Second)
				}
				if changed {
					t.Error("Stream msgs changed after the step down")
				}
			}

			/////////////////////////////////////////////////////////////////////////////////////////
			//                                                                                     //
			//  The number of messages sourced should match the count from all the other streams.  //
			//                                                                                     //
			/////////////////////////////////////////////////////////////////////////////////////////
			var expectedMsgs uint64
			for i := 0; i < totalStreams; i++ {
				name := fmt.Sprintf("test:%d", i)
				sinfo, err := js.StreamInfo(name)
				require_NoError(t, err)
				expectedMsgs += sinfo.State.Msgs
			}
			sinfo, err := js.StreamInfo(streamName)
			require_NoError(t, err)

			gotMsgs := sinfo.State.Msgs
			if gotMsgs != expectedMsgs {
				t.Errorf("stream with sources has %v messages, but total sourced messages should be %v", gotMsgs, expectedMsgs)
			}
			checkConsumer(t, c, accName, streamName, "A")
			checkMsgsEqual(t, c, accName, streamName)
		}
		test(t, &testParams{
			cluster:        t.Name(),
			streams:        streams,
			producers:      10,
			consumers:      10,
			restarts:       1,
			rolloutRestart: true,
			ldmRestart:     true,
			checkHealthz:   true,
			// TODO(dlc) - If this overlaps with the scale jobs this test will fail.
			// Leaders will be elected with partial state.
			restartWait: 65 * time.Second,
			jobs: []job{
				scale(3, 15*time.Second),
				scale(1, 30*time.Second),
				scale(3, 60*time.Second),
			},
			expect:          expect,
			duration:        testDuration,
			producerMsgSize: 1024,
			producerMsgs:    100_000,
		})
	})

	t.Run("R3F/streams:30/limits", func(t *testing.T) {
		testDuration := 3 * time.Minute
		totalStreams := 30
		consumersPerStream := 5
		streams := make([]*streamSetup, totalStreams)
		for i := 0; i < totalStreams; i++ {
			name := fmt.Sprintf("test:%d", i)
			st := &streamSetup{
				config: &nats.StreamConfig{
					Name:      name,
					Subjects:  []string{fmt.Sprintf("test.%d.*", i)},
					Replicas:  3,
					Retention: nats.LimitsPolicy,
				},
				consumers: make([]*nats.ConsumerConfig, 0),
			}
			for j := 0; j < consumersPerStream; j++ {
				subject := fmt.Sprintf("test.%d.%d", i, j)
				name := fmt.Sprintf("A:%d:%d", i, j)
				cc := &nats.ConsumerConfig{
					Name:          name,
					Durable:       name,
					FilterSubject: subject,
					AckPolicy:     nats.AckExplicitPolicy,
				}
				st.consumers = append(st.consumers, cc)
				st.subjects = append(st.subjects, subject)
			}
			streams[i] = st
		}
		expect := func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext, c *cluster) {
			time.Sleep(testDuration + 1*time.Minute)
			accName := "js"
			for i := 0; i < totalStreams; i++ {
				streamName := fmt.Sprintf("test:%d", i)
				checkMsgsEqual(t, c, accName, streamName)
			}
		}
		test(t, &testParams{
			cluster:         t.Name(),
			streams:         streams,
			producers:       10,
			consumers:       10,
			restarts:        1,
			rolloutRestart:  true,
			ldmRestart:      true,
			checkHealthz:    true,
			restartWait:     45 * time.Second,
			expect:          expect,
			duration:        testDuration,
			producerMsgSize: 1024,
			producerMsgs:    100_000,
		})
	})

	t.Run("R3M/streams:30/limits", func(t *testing.T) {
		testDuration := 3 * time.Minute
		totalStreams := 30
		consumersPerStream := 5
		streams := make([]*streamSetup, totalStreams)
		for i := 0; i < totalStreams; i++ {
			name := fmt.Sprintf("test:%d", i)
			st := &streamSetup{
				config: &nats.StreamConfig{
					Name:      name,
					Subjects:  []string{fmt.Sprintf("test.%d.*", i)},
					Replicas:  3,
					Retention: nats.LimitsPolicy,
					Storage:   nats.MemoryStorage,
				},
				consumers: make([]*nats.ConsumerConfig, 0),
			}
			for j := 0; j < consumersPerStream; j++ {
				subject := fmt.Sprintf("test.%d.%d", i, j)
				name := fmt.Sprintf("A:%d:%d", i, j)
				cc := &nats.ConsumerConfig{
					Name:          name,
					Durable:       name,
					FilterSubject: subject,
					AckPolicy:     nats.AckExplicitPolicy,
				}
				st.consumers = append(st.consumers, cc)
				st.subjects = append(st.subjects, subject)
			}
			streams[i] = st
		}
		expect := func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext, c *cluster) {
			time.Sleep(testDuration + time.Minute)
			accName := "js"
			for i := 0; i < totalStreams; i++ {
				streamName := fmt.Sprintf("test:%d", i)
				checkMsgsEqual(t, c, accName, streamName)
			}
		}
		test(t, &testParams{
			cluster:         t.Name(),
			streams:         streams,
			producers:       10,
			consumers:       10,
			restarts:        1,
			rolloutRestart:  true,
			ldmRestart:      true,
			checkHealthz:    true,
			restartWait:     45 * time.Second,
			expect:          expect,
			duration:        testDuration,
			producerMsgSize: 512,
			producerMsgs:    5_000,
		})
	})
}

func TestLongClusterJetStreamBusyStreamsVariations(t *testing.T) {
	type streamSetup struct {
		config    *nats.StreamConfig
		consumers []*nats.ConsumerConfig
		subjects  []string
	}
	type job func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext, c *cluster)
	type testParams struct {
		cluster            string
		streams            []*streamSetup
		producers          int
		consumers          int
		restartAny         bool
		restartFirst       bool
		restartWait        time.Duration
		ldmRestart         bool
		rolloutRestart     bool
		restarts           int
		checkHealthz       bool
		jobs               []job
		expect             job
		duration           time.Duration
		producerMsgs       int
		producerMsgSize    int
		producerRate       time.Duration
		unfiltered         bool
		maxAckPending      int
		totalStreams       int
		consumersPerStream int
		outOfOrderAcks     bool
		delayedAcks        bool
		skipSequence       uint64
	}
	test := func(t *testing.T, test *testParams) {
		if test.producerRate == 0 {
			test.producerRate = 1 * time.Millisecond
		}
		conf := `
                listen: 127.0.0.1:-1
                http: 127.0.0.1:-1
                server_name: %s
                jetstream: {
                        domain: "cloud"
                        store_dir: '%s',
                }
                cluster {
                        name: %s
                        listen: 127.0.0.1:%d
                        routes = [%s]
                }
                server_tags: ["test"]
                system_account: sys

                no_auth_user: js
                accounts {
                        sys { users = [ { user: sys, pass: sys } ] }

                        js  { jetstream = enabled
                              users = [ { user: js, pass: js } ]
                        }
                }`
		c := createJetStreamClusterWithTemplate(t, conf, test.cluster, 3)
		defer c.shutdown()
		for _, s := range c.servers {
			s.optsMu.Lock()
			s.opts.LameDuckDuration = 15 * time.Second
			s.opts.LameDuckGracePeriod = -15 * time.Second
			s.optsMu.Unlock()
		}

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		var wg sync.WaitGroup
		for _, stream := range test.streams {
			stream := stream
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := js.AddStream(stream.config)
				require_NoError(t, err)

				for _, consumer := range stream.consumers {
					_, err := js.AddConsumer(stream.config.Name, consumer)
					require_NoError(t, err)
				}
			}()
		}
		wg.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), test.duration)
		defer cancel()
		for _, stream := range test.streams {
			payload := []byte(strings.Repeat("A", test.producerMsgSize))
			stream := stream
			subjects := stream.subjects

			// Create publishers on different connections that sends messages
			// to all the consumers subjects.
			var n atomic.Uint64
			for i := 0; i < test.producers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					nc, js := jsClientConnect(t, c.randomServer())
					defer nc.Close()

					for range time.NewTicker(test.producerRate).C {
						select {
						case <-ctx.Done():
							return
						default:
						}

						for _, subject := range subjects {
							msgID := nats.MsgId(fmt.Sprintf("n:%d", n.Load()))
							_, err := js.Publish(subject, payload, nats.AckWait(200*time.Millisecond), msgID)
							if err == nil {
								if nn := n.Add(1); int(nn) >= test.producerMsgs {
									return
								}
							}
						}
					}
				}()
			}

			// Create multiple parallel pull subscribers per consumer config.
			for i := 0; i < test.consumers; i++ {
				for _, consumer := range stream.consumers {
					wg.Add(1)

					consumer := consumer
					outOfOrderAcks := test.outOfOrderAcks
					delayedAcks := test.delayedAcks
					skipSequence := test.skipSequence
					go func() {
						defer wg.Done()

						for attempts := 0; attempts < 60; attempts++ {
							_, err := js.ConsumerInfo(stream.config.Name, consumer.Name)
							if err != nil {
								t.Logf("WRN: Failed creating pull subscriber: %v - %v - %v - %v",
									consumer.FilterSubject, stream.config.Name, consumer.Name, err)
							}
						}
						sub, err := js.PullSubscribe(consumer.FilterSubject, "", nats.Bind(stream.config.Name, consumer.Name))
						if err != nil {
							t.Logf("WRN: Failed creating pull subscriber: %v - %v - %v - %v",
								consumer.FilterSubject, stream.config.Name, consumer.Name, err)
							return
						}
						require_NoError(t, err)

						for range time.NewTicker(100 * time.Millisecond).C {
							select {
							case <-ctx.Done():
								return
							default:
							}

							msgs, err := sub.Fetch(1, nats.MaxWait(200*time.Millisecond))
							if err != nil {
								continue
							}

							for _, msg := range msgs {
								meta, _ := msg.Metadata()
								if skipSequence > 0 && meta.Sequence.Stream == skipSequence {
									// Always skip this sequence one to keep ack pending
									// from reaching 0 in a test.
								} else {
									msg.Ack()
								}
							}

							if outOfOrderAcks {
								rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
							}
							msgs, err = sub.Fetch(100, nats.MaxWait(200*time.Millisecond))
							if err != nil {
								continue
							}
						NextMsg:
							for i, msg := range msgs {
								if skipSequence > 0 {
									meta, _ := msg.Metadata()
									// Always skip this sequence one to keep ack pending
									// from reaching 0 in a test.
									if meta.Sequence.Stream == skipSequence {
										continue NextMsg
									}
								}
								if delayedAcks && i%5 == 0 {
									time.AfterFunc(15*time.Second, func() {
										msg.Ack()
									})
								} else {
									msg.Ack()
								}
							}
						}
					}()
				}
			}
		}

		for _, job := range test.jobs {
			go job(t, nc, js, c)
		}
		if test.restarts > 0 {
			wg.Add(1)
			time.AfterFunc(test.restartWait, func() {
				defer wg.Done()
				for i := 0; i < test.restarts; i++ {
					switch {
					case test.restartFirst:
						s := c.servers[0]
						if test.ldmRestart {
							s.lameDuckMode()
						} else {
							s.Shutdown()
						}
						s.WaitForShutdown()
						c.restartServer(s)
					case test.restartAny:
						s := c.servers[rand.Intn(len(c.servers))]
						if test.ldmRestart {
							s.lameDuckMode()
						} else {
							s.Shutdown()
						}
						s.WaitForShutdown()
						c.restartServer(s)
					case test.rolloutRestart:
						for _, s := range c.servers {
							if test.ldmRestart {
								s.lameDuckMode()
							} else {
								s.Shutdown()
							}
							s.WaitForShutdown()
							s = c.restartServer(s)

							if test.checkHealthz {
								hctx, hcancel := context.WithTimeout(ctx, 15*time.Second)
								defer hcancel()

							Healthz:
								for range time.NewTicker(2 * time.Second).C {
									select {
									case <-hctx.Done():
										break Healthz
									default:
									}

									status := s.healthz(nil)
									if status.StatusCode == 200 {
										break Healthz
									}
								}
							}
						}
					}
					c.waitOnClusterReady()
				}
			})
		}
		test.expect(t, nc, js, c)
		cancel()
		wg.Wait()
	}
	// stepDown := func(nc *nats.Conn, streamName string) {
	// 	nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, streamName), nil, time.Second)
	// }
	getStreamDetails := func(t *testing.T, c *cluster, accountName, streamName string) *StreamDetail {
		t.Helper()
		srv := c.streamLeader(accountName, streamName)
		if srv == nil {
			return nil
		}
		jsz, err := srv.Jsz(&JSzOptions{Accounts: true, Streams: true, Consumer: true})
		require_NoError(t, err)
		for _, acc := range jsz.AccountDetails {
			if acc.Name == accountName {
				for _, stream := range acc.Streams {
					if stream.Name == streamName {
						return &stream
					}
				}
			}
		}
		t.Error("Could not find account details")
		return nil
	}
	checkMsgsEqual := func(t *testing.T, c *cluster, accountName, streamName string) {
		details := getStreamDetails(t, c, accountName, streamName)
		if details == nil {
			// Nothing to do, can happen with deleted streams.
			return
		}
		state := details.State

		msets := make(map[*Server]*stream)
		for _, s := range c.servers {
			acc, err := s.LookupAccount(accountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream(streamName)
			require_NoError(t, err)
			msets[s] = mset
		}
		for seq := state.FirstSeq; seq <= state.LastSeq; seq++ {
			var msgId string
			var smv StoreMsg
			for replica, mset := range msets {
				mset.mu.RLock()
				sm, err := mset.store.LoadMsg(seq, &smv)
				mset.mu.RUnlock()
				if err != nil {
					t.Logf("STATE: %+v", state)
					if err == ErrStoreMsgNotFound {
						t.Logf("WRN: Unexpected error loading message (seq=%d) from stream %q on replica %q: %v", seq, streamName, replica, err)
					} else {
						t.Errorf("Unexpected error loading message (seq=%d) from stream %q on replica %q: %v", seq, streamName, replica, err)
					}
					continue
				}
				if msgId == _EMPTY_ {
					msgId = string(sm.hdr)
				} else if msgId != string(sm.hdr) {
					t.Errorf("MsgIds do not match for seq %d: %q vs %q", seq, msgId, sm.hdr)
				}
			}
		}
	}
	checkConsumer := func(t *testing.T, c *cluster, accountName, streamName, consumerName string) {
		t.Helper()
		md := make(map[*Server]*ConsumerInfo)
		for _, s := range c.servers {
			if s == nil {
				// State no longer available to check.
				continue
			}
			jsz, err := s.Jsz(&JSzOptions{Accounts: true, Streams: true, Consumer: true})
			require_NoError(t, err)
			for _, acc := range jsz.AccountDetails {
				if acc.Name == accountName {
					for _, stream := range acc.Streams {
						if stream.Name == streamName {
							for _, consumer := range stream.Consumer {
								if consumer.Name == consumerName {
									if stream.State.LastSeq < consumer.Delivered.Stream {
										t.Errorf("Stream last sequence is behind consumer sequence, s:%d vs c:%d (stream: %+v, consumer: %+v)",
											stream.State.LastSeq, consumer.Delivered.Stream, stream.Name, consumer.Name)
									}
									md[s] = consumer
								}
							}
						}
					}
				}
			}
		}
		for sA, ciA := range md {
			for sB, ciB := range md {
				if sA == sB {
					continue
				}
				if ciA.Delivered.Stream != ciB.Delivered.Stream {
					t.Errorf("Consumer last delivery stream sequence is different on consumer %q (%d vs %d)",
						ciA.Name, ciA.Delivered.Stream, ciB.Delivered.Stream)
				}
				if ciA.AckFloor.Stream != ciB.AckFloor.Stream {
					t.Errorf("Consumer ack floor stream sequence is different on consumer %q (%d vs %d)",
						ciA.Name, ciA.AckFloor.Stream, ciB.AckFloor.Stream)
				}
			}
		}
	}

	shared := func(t *testing.T, sc *nats.StreamConfig, tp *testParams) func(t *testing.T) {
		return func(t *testing.T) {
			testDuration := 3 * time.Minute
			totalStreams := 30
			consumersPerStream := 5

			if tp.totalStreams > 0 {
				totalStreams = tp.totalStreams
			}
			if tp.consumersPerStream > 0 {
				consumersPerStream = tp.consumersPerStream
			}
			streams := make([]*streamSetup, totalStreams)
			for i := 0; i < totalStreams; i++ {
				name := fmt.Sprintf("test:%d", i)
				st := &streamSetup{
					config: &nats.StreamConfig{
						Name:      name,
						Subjects:  []string{fmt.Sprintf("test.%d.*", i)},
						Replicas:  3,
						Discard:   sc.Discard,
						Retention: sc.Retention,
						Storage:   sc.Storage,
						MaxMsgs:   sc.MaxMsgs,
						MaxBytes:  sc.MaxBytes,
						MaxAge:    sc.MaxAge,
					},
					consumers: make([]*nats.ConsumerConfig, 0),
				}
				for j := 0; j < consumersPerStream; j++ {
					subject := fmt.Sprintf("test.%d.%d", i, j)
					name := fmt.Sprintf("A:%d:%d", i, j)
					cc := &nats.ConsumerConfig{
						Name:      name,
						Durable:   name,
						AckPolicy: nats.AckExplicitPolicy,
					}
					if !tp.unfiltered {
						cc.FilterSubject = subject
					}
					if tp.maxAckPending > 0 {
						cc.MaxAckPending = tp.maxAckPending
					}
					st.consumers = append(st.consumers, cc)
					st.subjects = append(st.subjects, subject)
				}
				streams[i] = st
			}
			expect := func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext, c *cluster) {
				time.Sleep(testDuration + 1*time.Minute)
				accName := "js"
				for i := 0; i < totalStreams; i++ {
					streamName := fmt.Sprintf("test:%d", i)
					for j := 0; j < consumersPerStream; j++ {
						consumerName := fmt.Sprintf("A:%d:%d", i, j)
						checkConsumer(t, c, accName, streamName, consumerName)
					}
					checkMsgsEqual(t, c, accName, streamName)
				}
			}
			test(t, &testParams{
				cluster:         t.Name(),
				streams:         streams,
				producers:       tp.producers,
				consumers:       tp.consumers,
				restartFirst:    tp.restartFirst,
				restartAny:      tp.restartAny,
				restarts:        tp.restarts,
				rolloutRestart:  tp.rolloutRestart,
				ldmRestart:      tp.ldmRestart,
				checkHealthz:    tp.checkHealthz,
				restartWait:     tp.restartWait,
				expect:          expect,
				jobs:            tp.jobs,
				duration:        testDuration,
				producerMsgSize: tp.producerMsgSize,
				producerMsgs:    tp.producerMsgs,
				producerRate:    tp.producerRate,
				unfiltered:      tp.unfiltered,
				maxAckPending:   tp.maxAckPending,
				outOfOrderAcks:  tp.outOfOrderAcks,
				delayedAcks:     tp.delayedAcks,
				skipSequence:    tp.skipSequence,
			})
		}
	}

	t.Run("rollouts", func(t *testing.T) {
		for prefix, st := range map[string]nats.StorageType{"R3F": nats.FileStorage, "R3M": nats.MemoryStorage} {
			t.Run(prefix, func(t *testing.T) {
				for rolloutType, params := range map[string]*testParams{
					// Rollouts using graceful restarts and checking healthz.
					"ldm": {
						restarts:        1,
						rolloutRestart:  true,
						ldmRestart:      true,
						checkHealthz:    true,
						restartWait:     45 * time.Second,
						producers:       10,
						consumers:       10,
						producerMsgSize: 1024,
						producerMsgs:    100_000,
					},
					// Non graceful restarts calling Shutdown, but using healthz on startup.
					"term": {
						restarts:        1,
						rolloutRestart:  true,
						ldmRestart:      false,
						checkHealthz:    true,
						restartWait:     45 * time.Second,
						producers:       10,
						consumers:       10,
						producerMsgSize: 1024,
						producerMsgs:    100_000,
					},
					// Do a few of non graceful restarts to first node.
					"term:2": {
						restarts:        2,
						restartFirst:    true,
						restartWait:     45 * time.Second,
						producers:       10,
						consumers:       10,
						producerMsgSize: 1024,
						producerMsgs:    100_000,
					},
					// Do a few of non graceful restarts to any nodes.
					"term:3": {
						restarts:        3,
						restartAny:      true,
						restartWait:     45 * time.Second,
						producers:       10,
						consumers:       10,
						producerMsgSize: 1024,
						producerMsgs:    100_000,
					},
				} {
					t.Run(rolloutType, func(t *testing.T) {
						t.Run("limits", shared(t, &nats.StreamConfig{
							Retention: nats.LimitsPolicy,
							Storage:   st,
						}, params))
						t.Run("wq", shared(t, &nats.StreamConfig{
							Retention: nats.WorkQueuePolicy,
							Storage:   st,
						}, params))
						t.Run("interest", shared(t, &nats.StreamConfig{
							Retention: nats.InterestPolicy,
							Storage:   st,
						}, params))
						t.Run("limits:dn:max-per-subject", shared(t, &nats.StreamConfig{
							Retention:         nats.LimitsPolicy,
							Storage:           st,
							MaxMsgsPerSubject: 1,
							Discard:           nats.DiscardNew,
						}, params))
						t.Run("wq:dn:max-msgs", shared(t, &nats.StreamConfig{
							Retention: nats.WorkQueuePolicy,
							Storage:   st,
							MaxMsgs:   10_000,
							Discard:   nats.DiscardNew,
						}, params))
						t.Run("wq:dn-per-subject:max-msgs", shared(t, &nats.StreamConfig{
							Retention:            nats.WorkQueuePolicy,
							Storage:              st,
							MaxMsgs:              10_000,
							MaxMsgsPerSubject:    100,
							Discard:              nats.DiscardNew,
							DiscardNewPerSubject: true,
						}, params))
					})
				}
			})
		}
	})

	t.Run("short-streams", func(t *testing.T) {
		for prefix, st := range map[string]nats.StorageType{"R3F": nats.FileStorage, "R3M": nats.MemoryStorage} {
			t.Run(prefix, func(t *testing.T) {
				t.Run("multi-restart", func(t *testing.T) {
					// Also do LDM but without healthz.
					params := &testParams{
						restarts:        3,
						rolloutRestart:  true,
						ldmRestart:      true,
						checkHealthz:    false,
						restartWait:     45 * time.Second,
						producers:       10,
						consumers:       10,
						producerMsgSize: 1024,
						producerMsgs:    90,
					}
					t.Run("wq:dn:max-msgs", shared(t, &nats.StreamConfig{
						Retention:  nats.WorkQueuePolicy,
						Storage:    st,
						MaxMsgs:    2_000,
						Discard:    nats.DiscardNew,
						Duplicates: 5 * time.Second,
					}, params))
					t.Run("limits:dn:max-msgs", shared(t, &nats.StreamConfig{
						Retention:  nats.LimitsPolicy,
						Storage:    st,
						MaxMsgs:    2_000,
						Discard:    nats.DiscardNew,
						Duplicates: 5 * time.Second,
					}, params))
					t.Run("limits:do:max-msgs", shared(t, &nats.StreamConfig{
						Retention:  nats.LimitsPolicy,
						Storage:    st,
						MaxMsgs:    2_000,
						Discard:    nats.DiscardOld,
						Duplicates: 5 * time.Second,
					}, params))
				})

				t.Run("rollout", func(t *testing.T) {
					// Regular rollout with healthz, also very low rate of publishing.
					params := &testParams{
						restarts:        1,
						rolloutRestart:  true,
						ldmRestart:      true,
						checkHealthz:    true,
						restartWait:     45 * time.Second,
						producers:       2,
						consumers:       10,
						producerMsgSize: 1024,
						producerMsgs:    90,
						producerRate:    1 * time.Second,
					}
					t.Run("wq:dn:max-msgs", shared(t, &nats.StreamConfig{
						Retention:  nats.WorkQueuePolicy,
						Storage:    st,
						MaxMsgs:    2_000,
						Discard:    nats.DiscardNew,
						Duplicates: 5 * time.Second,
					}, params))
					t.Run("limits:dn:max-msgs", shared(t, &nats.StreamConfig{
						Retention:  nats.LimitsPolicy,
						Storage:    st,
						MaxMsgs:    2_000,
						Discard:    nats.DiscardNew,
						Duplicates: 5 * time.Second,
					}, params))
					t.Run("limits:do:max-msgs", shared(t, &nats.StreamConfig{
						Retention:  nats.LimitsPolicy,
						Storage:    st,
						MaxMsgs:    2_000,
						Discard:    nats.DiscardOld,
						Duplicates: 5 * time.Second,
					}, params))

					t.Run("unfiltered:wq:dn:max-msgs", shared(t, &nats.StreamConfig{
						Retention:  nats.WorkQueuePolicy,
						Storage:    st,
						MaxMsgs:    2_000,
						Discard:    nats.DiscardNew,
						Duplicates: 5 * time.Second,
					}, &testParams{
						restarts:           1,
						rolloutRestart:     true,
						ldmRestart:         true,
						checkHealthz:       true,
						restartWait:        45 * time.Second,
						producers:          2,
						consumers:          10,
						producerMsgSize:    1024,
						producerMsgs:       2_000,
						producerRate:       1 * time.Millisecond,
						maxAckPending:      2_000,
						unfiltered:         true,
						totalStreams:       30,
						consumersPerStream: 1,
						outOfOrderAcks:     true,
						delayedAcks:        true,
						skipSequence:       5,
					}))
				})
			})
		}
	})
}
