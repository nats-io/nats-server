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

// This test file is skipped by default (unless the include_js_cluster_long_running_tests tag is set).
// This is a measure to avoid accidentally running tests here (e.g. `go test ./server`)
//go:build !skip_js_tests && include_js_cluster_long_running_tests
// +build !skip_js_tests,include_js_cluster_long_running_tests

package server

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestLongDummy(t *testing.T) {
	// Dummy test to verify tests set of tests are running as expected

	t.Run("Passing sub-test", func(t *testing.T) {
		t.Logf("Pass!")
	})

	t.Run("Failing sub-test", func(t *testing.T) {
		t.Fatalf("Fail!")
	})
}

// TestLongJetStreamKVUpdates a client overwrites entries in a KV bucket for a fixed amount of time
func TestLongJetStreamKVUpdates(t *testing.T) {

	// RNG Seed
	const Seed = 123456
	// Number of keys in bucket
	const NumKeys = 1000
	// Size of (random) values
	const ValueSize = 1024
	// Test duration
	const Duration = 1 * time.Minute
	// If no updates successfull for this interval, fail the test
	const MaxRetry = 20 * time.Second
	// Minimum time before updates
	const UpdatesInterval = 1 * time.Millisecond
	// Time between progress reports to console
	const ProgressInterval = 10 * time.Second

	rng := rand.New(rand.NewSource(Seed))

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Connect to a random server but client will discover others too.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:   "TEST",
		Replicas: 3,
	})
	require_NoError(t, err)

	// Initialize list of keys
	keys := make([]string, NumKeys)
	for i := 0; i < NumKeys; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	// Initialize keys in bucket with an empty value
	// (code below can assume all keys exist)
	for _, key := range keys {
		_, err := kv.Put(key, []byte{})
		require_NoError(t, err)
	}

	// Track some statistics
	var stats = struct {
		start                time.Time
		lastSuccessfulUpdate time.Time
		updateOk             uint64
		updateErr            uint64
	}{
		start:                time.Now(),
		lastSuccessfulUpdate: time.Now(),
	}

	// Pick a random key and update it with a random value
	valueBuffer := make([]byte, ValueSize)
	updateRandomKey := func() error {
		key := keys[rand.Intn(NumKeys)]
		_, err := rng.Read(valueBuffer)
		require_NoError(t, err)
		_, err = kv.Put(key, valueBuffer)
		return err
	}

	printProgress := func() {
		t.Logf(
			"[%s] %d updates %d errors",
			time.Since(stats.start).Round(time.Second),
			stats.updateOk,
			stats.updateErr,
		)
	}

	// Print update on completion
	defer printProgress()

	// Set up timers and tickers for the run loop
	endTestTimer := time.After(Duration)
	nextUpdateTicker := time.NewTicker(UpdatesInterval)
	progressTicker := time.NewTicker(ProgressInterval)
	restartServerTicker := time.NewTicker(3 * time.Second)

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
			randomServer := c.servers[rng.Intn(len(c.servers))]
			restartedServer := c.restartServer(randomServer)
			c.waitOnServerHealthz(restartedServer)
			c.waitOnAllCurrent()

		case <-progressTicker.C:
			printProgress()
		}
	}
}

func TestLongJetStreamClusterRestartThenScaleStreamReplicas(t *testing.T) {
	t.Skip("This test fails with NPE")
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

func TestLongJetStreamClusterBusyStreams(t *testing.T) {
	t.Skip("This test produces tons of output and seems to get stuck")
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
							//t.Logf("WRN: Failed creating pull subscriber: %v - %v - %v - %v",
							//	consumer.FilterSubject, stream.config.Name, consumer.Name, err)
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

	t.Run("rollouts", func(t *testing.T) {
		shared := func(t *testing.T, sc *nats.StreamConfig, tp *testParams) func(t *testing.T) {
			return func(t *testing.T) {
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
					restarts:        tp.restarts,
					rolloutRestart:  tp.rolloutRestart,
					ldmRestart:      tp.ldmRestart,
					checkHealthz:    tp.checkHealthz,
					restartWait:     tp.restartWait,
					expect:          expect,
					duration:        testDuration,
					producerMsgSize: 1024,
					producerMsgs:    100_000,
				})
			}
		}
		for prefix, st := range map[string]nats.StorageType{"R3F": nats.FileStorage, "R3M": nats.MemoryStorage} {
			t.Run(prefix, func(t *testing.T) {
				for rolloutType, params := range map[string]*testParams{
					// Rollouts using graceful restarts and checking healthz.
					"ldm": {
						restarts:       1,
						rolloutRestart: true,
						ldmRestart:     true,
						checkHealthz:   true,
						restartWait:    45 * time.Second,
					},
					// Non graceful restarts calling Shutdown, but using healthz on startup.
					"term": {
						restarts:       1,
						rolloutRestart: true,
						ldmRestart:     false,
						checkHealthz:   true,
						restartWait:    45 * time.Second,
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
}

func TestLongJetStreamClusterKeyValueSync(t *testing.T) {
	t.Skip("Too much output, needs to be cleaned up")
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	for _, s := range c.servers {
		s.optsMu.Lock()
		s.opts.LameDuckDuration = 15 * time.Second
		s.opts.LameDuckGracePeriod = -15 * time.Second
		s.optsMu.Unlock()
	}
	s := c.randomNonLeader()
	connect := func(t *testing.T) (*nats.Conn, nats.JetStreamContext) {
		return jsClientConnect(t, s)
	}

	const accountName = "$G"
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	createData := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = letterBytes[rand.Intn(len(letterBytes))]
		}
		return b
	}
	getOrCreateKvStore := func(kvname string) (nats.KeyValue, error) {
		_, js := connect(t)
		kvExists := false
		existingKvnames := js.KeyValueStoreNames()
		for existingKvname := range existingKvnames {
			if existingKvname == kvname {
				kvExists = true
				break
			}
		}
		if !kvExists {
			return js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:   kvname,
				Replicas: 3,
				Storage:  nats.FileStorage,
			})
		} else {
			return js.KeyValue(kvname)
		}
	}
	abs := func(x int64) int64 {
		if x < 0 {
			return -x
		}
		return x
	}
	var counter int64
	var errorCounter int64

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
	checkState := func(t *testing.T, c *cluster, accountName, streamName string) error {
		t.Helper()

		leaderSrv := c.streamLeader(accountName, streamName)
		if leaderSrv == nil {
			return fmt.Errorf("no leader server found for stream %q", streamName)
		}
		streamLeader := getStreamDetails(t, c, accountName, streamName)
		if streamLeader == nil {
			return fmt.Errorf("no leader found for stream %q", streamName)
		}
		var errs []error
		for _, srv := range c.servers {
			if srv == leaderSrv {
				// Skip self
				continue
			}
			acc, err := srv.LookupAccount(accountName)
			require_NoError(t, err)
			stream, err := acc.lookupStream(streamName)
			require_NoError(t, err)
			state := stream.state()

			if state.Msgs != streamLeader.State.Msgs {
				err := fmt.Errorf("[%s] Leader %v has %d messages, Follower %v has %d messages",
					streamName, leaderSrv, streamLeader.State.Msgs,
					srv, state.Msgs,
				)
				errs = append(errs, err)
			}
			if state.FirstSeq != streamLeader.State.FirstSeq {
				err := fmt.Errorf("[%s] Leader %v FirstSeq is %d, Follower %v is at %d",
					streamName, leaderSrv, streamLeader.State.FirstSeq,
					srv, state.FirstSeq,
				)
				errs = append(errs, err)
			}
			if state.LastSeq != streamLeader.State.LastSeq {
				err := fmt.Errorf("[%s] Leader %v LastSeq is %d, Follower %v is at %d",
					streamName, leaderSrv, streamLeader.State.LastSeq,
					srv, state.LastSeq,
				)
				errs = append(errs, err)
			}
			if state.NumDeleted != streamLeader.State.NumDeleted {
				err := fmt.Errorf("[%s] Leader %v NumDeleted is %d, Follower %v is at %d\nSTATE_A: %+v\nSTATE_B: %+v\n",
					streamName, leaderSrv, streamLeader.State.NumDeleted,
					srv, state.NumDeleted, streamLeader.State, state,
				)
				errs = append(errs, err)
			}
		}
		if len(errs) > 0 {
			return errors.Join(errs...)
		}
		return nil
	}

	checkMsgsEqual := func(t *testing.T, accountName, streamName string) error {
		// Gather all the streams replicas and compare contents.
		msets := make(map[*Server]*stream)
		for _, s := range c.servers {
			acc, err := s.LookupAccount(accountName)
			if err != nil {
				return err
			}
			mset, err := acc.lookupStream(streamName)
			if err != nil {
				return err
			}
			msets[s] = mset
		}

		str := getStreamDetails(t, c, accountName, streamName)
		if str == nil {
			return fmt.Errorf("could not get stream leader state")
		}
		state := str.State
		for seq := state.FirstSeq; seq <= state.LastSeq; seq++ {
			var msgId string
			var smv StoreMsg
			for replica, mset := range msets {
				mset.mu.RLock()
				sm, err := mset.store.LoadMsg(seq, &smv)
				mset.mu.RUnlock()
				if err != nil {
					if err == ErrStoreMsgNotFound || err == errDeletedMsg {
						// Skip these.
					} else {
						t.Logf("WRN: Error loading message (seq=%d) from stream %q on replica %q: %v", seq, streamName, replica, err)
					}
					continue
				}
				if msgId == _EMPTY_ {
					msgId = string(sm.hdr)
				} else if msgId != string(sm.hdr) {
					t.Errorf("MsgIds do not match for seq %d on stream %q: %q vs %q", seq, streamName, msgId, sm.hdr)
				}
			}
		}
		return nil
	}

	keyUpdater := func(ctx context.Context, cancel context.CancelFunc, kvname string, numKeys int) {
		kv, err := getOrCreateKvStore(kvname)
		if err != nil {
			t.Fatalf("[%s]:%v", kvname, err)
		}
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key-%d", i)
			kv.Create(key, createData(160))
		}
		lastData := make(map[string][]byte)
		revisions := make(map[string]uint64)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			r := rand.Intn(numKeys)
			key := fmt.Sprintf("key-%d", r)

			for i := 0; i < 5; i++ {
				_, err := kv.Get(key)
				if err != nil {
					atomic.AddInt64(&errorCounter, 1)
					if err == nats.ErrKeyNotFound {
						t.Logf("WRN: Key not found! [%s/%s] - [%s]", kvname, key, err)
						cancel()
					}
				}
			}

			k, err := kv.Get(key)
			if err != nil {
				atomic.AddInt64(&errorCounter, 1)
			} else {
				if revisions[key] != 0 && abs(int64(k.Revision())-int64(revisions[key])) < 2 {
					lastDataVal, ok := lastData[key]
					if ok && k.Revision() == revisions[key] && slices.Compare(lastDataVal, k.Value()) != 0 {
						t.Logf("data loss [%s/%s][rev:%d] expected:[%v] is:[%v]", kvname, key, revisions[key], string(lastDataVal), string(k.Value()))
					}
				}
				newData := createData(160)
				revisions[key], err = kv.Update(key, newData, k.Revision())
				if err != nil && err != nats.ErrTimeout {
					atomic.AddInt64(&errorCounter, 1)
				} else {
					lastData[key] = newData
				}
				atomic.AddInt64(&counter, 1)
			}
		}
	}

	streamCount := 50
	keysCount := 100
	streamPrefix := "IKV"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// The keyUpdaters will run for less time.
	kctx, kcancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer kcancel()

	var wg sync.WaitGroup
	var streams []string
	for i := 0; i < streamCount; i++ {
		streamName := fmt.Sprintf("%s-%d", streamPrefix, i)
		streams = append(streams, "KV_"+streamName)

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			keyUpdater(kctx, cancel, streamName, keysCount)
		}(i)
	}

	debug := false
	nc2, _ := jsClientConnect(t, s)
	if debug {
		go func() {
			for range time.NewTicker(5 * time.Second).C {
				select {
				case <-ctx.Done():
					return
				default:
				}
				for _, str := range streams {
					leaderSrv := c.streamLeader(accountName, str)
					if leaderSrv == nil {
						continue
					}
					streamLeader := getStreamDetails(t, c, accountName, str)
					if streamLeader == nil {
						continue
					}
					t.Logf("|------------------------------------------------------------------------------------------------------------------------|")
					lstate := streamLeader.State
					t.Logf("| %-10s | %-10s | msgs:%-10d | bytes:%-10d | deleted:%-10d | first:%-10d | last:%-10d |",
						str, leaderSrv.String()+"*", lstate.Msgs, lstate.Bytes, lstate.NumDeleted, lstate.FirstSeq, lstate.LastSeq,
					)
					for _, srv := range c.servers {
						if srv == leaderSrv {
							continue
						}
						acc, err := srv.LookupAccount(accountName)
						if err != nil {
							continue
						}
						stream, err := acc.lookupStream(str)
						if err != nil {
							t.Logf("Error looking up stream %s on %s replica", str, srv)
							continue
						}
						state := stream.state()

						unsynced := lstate.Msgs != state.Msgs || lstate.Bytes != state.Bytes ||
							lstate.NumDeleted != state.NumDeleted || lstate.FirstSeq != state.FirstSeq || lstate.LastSeq != state.LastSeq

						var result string
						if unsynced {
							result = "UNSYNCED"
						}
						t.Logf("| %-10s | %-10s | msgs:%-10d | bytes:%-10d | deleted:%-10d | first:%-10d | last:%-10d | %s",
							str, srv, state.Msgs, state.Bytes, state.NumDeleted, state.FirstSeq, state.LastSeq, result,
						)
					}
				}
				t.Logf("|------------------------------------------------------------------------------------------------------------------------| %v", nc2.ConnectedUrl())
			}
		}()
	}

	checkStreams := func(t *testing.T) {
		for _, str := range streams {
			checkFor(t, time.Minute, 500*time.Millisecond, func() error {
				return checkState(t, c, accountName, str)
			})
			checkFor(t, time.Minute, 500*time.Millisecond, func() error {
				return checkMsgsEqual(t, accountName, str)
			})
		}
	}

Loop:
	for range time.NewTicker(30 * time.Second).C {
		select {
		case <-ctx.Done():
			break Loop
		default:
		}
		rollout := func(t *testing.T) {
			for _, s := range c.servers {
				// For graceful mode
				s.lameDuckMode()
				s.WaitForShutdown()
				s = c.restartServer(s)

				hctx, hcancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer hcancel()

			Healthz:
				for range time.NewTicker(2 * time.Second).C {
					select {
					case <-hctx.Done():
					default:
					}

					status := s.healthz(nil)
					if status.StatusCode == 200 {
						break Healthz
					}
				}
				c.waitOnClusterReady()
				checkStreams(t)
			}
		}
		rollout(t)
		checkStreams(t)
	}
	wg.Wait()
	checkStreams(t)
}

func TestLongJetStreamConsumerFetchWithDrain(t *testing.T) {
	t.Skip("Fails")
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

func TestLongStreamSourcingScalingSourcingManyBenchmark(t *testing.T) {
	t.Skip("Test gets stuck and times out -- needs to be reviewed and updated")
	var numSourced = 1000
	var numMsgPerSource = 10_000
	var batchSize = 200
	var retries int

	var err error

	c := createMyLocalCluster(t)
	defer c.shutdown()

	nc, err := nats.Connect(connectURL)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	defer nc.Close()

	// create n streams to source from
	for i := 0; i < numSourced; i++ {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:                 fmt.Sprintf("sourced-%d", i),
			Subjects:             []string{strconv.Itoa(i)},
			Retention:            nats.LimitsPolicy,
			Storage:              nats.FileStorage,
			Discard:              nats.DiscardOld,
			Replicas:             1,
			AllowDirect:          true,
			MirrorDirect:         false,
			DiscardNewPerSubject: false,
		})
		require_NoError(t, err)
	}

	fmt.Printf("Streams created\n")

	// publish n messages for each sourced stream
	for j := 0; j < numMsgPerSource; j++ {
		start := time.Now()
		var pafs = make([]nats.PubAckFuture, numSourced)
		for i := 0; i < numSourced; i++ {
			var err error

			for {
				pafs[i], err = js.PublishAsync(strconv.Itoa(i), []byte(strconv.Itoa(j)))
				if err != nil {
					fmt.Printf("Error async publishing: %v, retrying\n", err)
					retries++
					time.Sleep(10 * time.Millisecond)
				} else {
					break
				}
			}

			if i != 0 && i%batchSize == 0 {
				<-js.PublishAsyncComplete()
			}

		}

		<-js.PublishAsyncComplete()

		for i := 0; i < numSourced; i++ {
			select {
			case <-pafs[i].Ok():
			case psae := <-pafs[i].Err():
				fmt.Printf("Error on PubAckFuture: %v, retrying sync...\n", psae)
				retries++
				_, err = js.Publish(strconv.Itoa(i), []byte(strconv.Itoa(j)))
				require_NoError(t, err)
			}
		}

		end := time.Now()
		if j%1000 == 0 {
			fmt.Printf("Published round %d, avg pub latency %v\n", j, end.Sub(start)/time.Duration(numSourced))
		}
	}

	fmt.Printf("Messages published\n")

	// create the StreamSources
	streamSources := make([]*nats.StreamSource, numSourced)

	for i := 0; i < numSourced; i++ {
		streamSources[i] = &nats.StreamSource{Name: fmt.Sprintf("sourced-%d", i), FilterSubject: strconv.Itoa(i)}
	}

	// create a stream that sources from them
	_, err = js.AddStream(&nats.StreamConfig{
		Name:                 "sourcing",
		Subjects:             []string{"foo"},
		Sources:              streamSources,
		Retention:            nats.LimitsPolicy,
		Storage:              nats.FileStorage,
		Discard:              nats.DiscardOld,
		Replicas:             3,
		AllowDirect:          true,
		MirrorDirect:         false,
		DiscardNewPerSubject: false,
	})
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "sourcing")
	sl := c.streamLeader(globalAccountName, "sourcing")
	fmt.Printf("Sourcing stream created leader is *** %s ***\n", sl.Name())

	expectedSeq := make([]int, numSourced)

	start := time.Now()

	var lastMsgs uint64
	mset, err := sl.GlobalAccount().lookupStream("sourcing")
	require_NoError(t, err)

	checkFor(t, 5*time.Minute, 1000*time.Millisecond, func() error {
		mset.mu.RLock()
		var state StreamState
		mset.store.FastState(&state)
		mset.mu.RUnlock()

		if state.Msgs == uint64(numMsgPerSource*numSourced) {
			fmt.Printf("👍 Test passed: expected %d messages, got %d and took %v\n", uint64(numMsgPerSource*numSourced), state.Msgs, time.Since(start))
			return nil
		} else if state.Msgs < uint64(numMsgPerSource*numSourced) {
			fmt.Printf("Current Rate %d per second - Received %d\n", state.Msgs-lastMsgs, state.Msgs)
			lastMsgs = state.Msgs
			return fmt.Errorf("Expected %d messages, got %d", uint64(numMsgPerSource*numSourced), state.Msgs)
		} else {
			fmt.Printf("Too many messages! expected %d (retries=%d), got %d\n", uint64(numMsgPerSource*numSourced), retries, state.Msgs)
			return fmt.Errorf("Too many messages: expected %d (retries=%d), got %d", uint64(numMsgPerSource*numSourced), retries, state.Msgs)
		}
	})

	// Check that all the messages sourced in the stream are correct
	// Note: expects to see exactly increasing matching sequence numbers, so could theoretically fail if some messages
	// get recorded 'out of order' (according to the payload value) because asynchronous JS publication is used to
	// publish the messages.
	// However, that should not happen if the publish 'batch size' is not more than the number of streams being sourced.

	// create a consumer on sourcing
	_, err = js.AddConsumer("sourcing", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	syncSub, err := js.SubscribeSync("", nats.BindStream("sourcing"))
	require_NoError(t, err)

	start = time.Now()

	print("Checking the messages\n")
	for i := 0; i < numSourced*numMsgPerSource; i++ {
		msg, err := syncSub.NextMsg(30 * time.Second)
		require_NoError(t, err)
		sId, err := strconv.Atoi(msg.Subject)
		require_NoError(t, err)
		seq, err := strconv.Atoi(string(msg.Data))
		require_NoError(t, err)
		if expectedSeq[sId] == seq {
			expectedSeq[sId]++
		} else {
			t.Fatalf("Expected seq number %d got %d for source %d\n", expectedSeq[sId], seq, sId)
		}
		msg.Ack()
		if i%100_000 == 0 {
			now := time.Now()
			fmt.Printf("[%v] Checked %d messages: %f msgs/sec \n", now, i, 100_000/now.Sub(start).Seconds())
			start = now
		}
	}
	print("👍 Done. \n")
}

func TestLongNoRaceJetStreamClusterInterestStreamConsistencyAfterRollingRestart(t *testing.T) {
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

// This test was from Ivan K. and showed a bug in the filestore implementation.
func TestLongNoRaceJetStreamOrderedConsumerMissingMsg(t *testing.T) {

	t.Skip("Fails")
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     "benchstream",
		Subjects: []string{"testsubject"},
		Replicas: 1,
	}); err != nil {
		t.Fatalf("add stream failed: %s", err)
	}

	total := 1_000_000

	numSubs := 10
	ch := make(chan struct{}, numSubs)
	wg := sync.WaitGroup{}
	wg.Add(numSubs)
	errCh := make(chan error, 1)
	for i := 0; i < numSubs; i++ {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		go func(nc *nats.Conn, js nats.JetStreamContext) {
			defer wg.Done()
			received := 0
			_, err := js.Subscribe("testsubject", func(m *nats.Msg) {
				meta, _ := m.Metadata()
				if meta.Sequence.Consumer != meta.Sequence.Stream {
					nc.Close()
					errCh <- fmt.Errorf("Bad meta: %+v", meta)
				}
				received++
				if received == total {
					ch <- struct{}{}
				}
			}, nats.OrderedConsumer())
			if err != nil {
				select {
				case errCh <- fmt.Errorf("Error creating sub: %v", err):
				default:
				}

			}
		}(nc, js)
	}
	wg.Wait()
	select {
	case e := <-errCh:
		t.Fatal(e)
	default:
	}

	payload := make([]byte, 500)
	for i := 1; i <= total; i++ {
		js.PublishAsync("testsubject", payload)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(10 * time.Second):
		t.Fatalf("Did not send all messages")
	}

	// Now wait for consumers to be done:
	for i := 0; i < numSubs; i++ {
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatal("Did not receive all messages for all consumers in time")
		}
	}
}
