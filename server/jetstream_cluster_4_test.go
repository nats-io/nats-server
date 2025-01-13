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

//go:build !skip_js_tests && !skip_js_cluster_tests_4
// +build !skip_js_tests,!skip_js_cluster_tests_4

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
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

func TestJetStreamClusterWorkQueueStreamDiscardNewDesync(t *testing.T) {
	t.Run("max msgs", func(t *testing.T) {
		testJetStreamClusterWorkQueueStreamDiscardNewDesync(t, &nats.StreamConfig{
			Name:      "WQTEST_MM",
			Subjects:  []string{"messages.*"},
			Replicas:  3,
			MaxAge:    10 * time.Minute,
			MaxMsgs:   100,
			Retention: nats.WorkQueuePolicy,
			Discard:   nats.DiscardNew,
		})
	})
	t.Run("max bytes", func(t *testing.T) {
		testJetStreamClusterWorkQueueStreamDiscardNewDesync(t, &nats.StreamConfig{
			Name:      "WQTEST_MB",
			Subjects:  []string{"messages.*"},
			Replicas:  3,
			MaxAge:    10 * time.Minute,
			MaxBytes:  1 * 1024 * 1024,
			Retention: nats.WorkQueuePolicy,
			Discard:   nats.DiscardNew,
		})
	})
}

func testJetStreamClusterWorkQueueStreamDiscardNewDesync(t *testing.T, sc *nats.StreamConfig) {
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
        system_account: sys
        no_auth_user: js
	accounts {
	  sys {
	    users = [
	      { user: sys, pass: sys }
	    ]
	  }
	  js {
	    jetstream = enabled
	    users = [
	      { user: js, pass: js }
	    ]
	  }
	}`
	c := createJetStreamClusterWithTemplate(t, conf, sc.Name, 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cnc, cjs := jsClientConnect(t, c.randomServer())
	defer cnc.Close()

	_, err := js.AddStream(sc)
	require_NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	psub, err := cjs.PullSubscribe("messages.*", "consumer")
	require_NoError(t, err)

	stepDown := func() {
		_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, sc.Name), nil, time.Second)
	}

	// Messages will be produced and consumed in parallel, then once there are
	// enough errors a leader election will be triggered.
	var (
		wg          sync.WaitGroup
		received    uint64
		errCh       = make(chan error, 100_000)
		receivedMap = make(map[string]*nats.Msg)
	)
	wg.Add(1)
	go func() {
		tick := time.NewTicker(20 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			case <-tick.C:
				msgs, err := psub.Fetch(10, nats.MaxWait(200*time.Millisecond))
				if err != nil {
					// The consumer will continue to timeout here eventually.
					continue
				}
				for _, msg := range msgs {
					received++
					receivedMap[msg.Subject] = msg
					msg.Ack()
				}
			}
		}
	}()

	shouldDrop := make(map[string]error)
	wg.Add(1)
	go func() {
		payload := []byte(strings.Repeat("A", 1024))
		tick := time.NewTicker(1 * time.Millisecond)
		for i := 1; ; i++ {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			case <-tick.C:
				subject := fmt.Sprintf("messages.%d", i)
				_, err := js.Publish(subject, payload, nats.RetryAttempts(0))
				if err != nil {
					errCh <- err
				}
				// Capture the messages that have failed.
				if err != nil {
					shouldDrop[subject] = err
				}
			}
		}
	}()

	// Collect enough errors to cause things to get out of sync.
	var errCount int
Setup:
	for {
		select {
		case err = <-errCh:
			errCount++
			if errCount%500 == 0 {
				stepDown()
			} else if errCount >= 2000 {
				// Stop both producing and consuming.
				cancel()
				break Setup
			}
		case <-time.After(5 * time.Second):
			// Unblock the test and continue.
			cancel()
			break Setup
		}
	}

	// Both goroutines should be exiting now..
	wg.Wait()

	// Let acks propagate for stream checks.
	time.Sleep(250 * time.Millisecond)

	// Check messages that ought to have been dropped.
	for subject := range receivedMap {
		found, ok := shouldDrop[subject]
		if ok {
			t.Errorf("Should have dropped message published on %q since got error: %v", subject, found)
		}
	}
}

// https://github.com/nats-io/nats-server/issues/5071
func TestJetStreamClusterStreamPlacementDistribution(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 5)
	defer c.shutdown()

	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for i := 1; i <= 10; i++ {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     fmt.Sprintf("TEST:%d", i),
			Subjects: []string{fmt.Sprintf("foo.%d.*", i)},
			Replicas: 3,
		})
		require_NoError(t, err)
	}

	// 10 streams, 3 replicas div 5 servers.
	expectedStreams := 10 * 3 / 5
	for _, s := range c.servers {
		jsz, err := s.Jsz(nil)
		require_NoError(t, err)
		require_Equal(t, jsz.Streams, expectedStreams)
	}
}

func TestJetStreamClusterSourceWorkingQueueWithLimit(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "WQ3", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "test", Subjects: []string{"test"}, Replicas: 3})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{Name: "wq", MaxMsgs: 100, Discard: nats.DiscardNew, Retention: nats.WorkQueuePolicy,
		Sources: []*nats.StreamSource{{Name: "test"}}, Replicas: 3})
	require_NoError(t, err)

	sendBatch := func(subject string, n int) {
		for i := 0; i < n; i++ {
			_, err = js.Publish(subject, []byte(strconv.Itoa(i)))
			require_NoError(t, err)
		}
	}
	// Populate each one.
	sendBatch("test", 300)

	checkFor(t, 3*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("wq")
		require_NoError(t, err)
		if si.State.Msgs != 100 {
			return fmt.Errorf("Expected 100 msgs, got state: %+v", si.State)
		}
		return nil
	})

	_, err = js.AddConsumer("wq", &nats.ConsumerConfig{Durable: "wqc", FilterSubject: "test", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	ss, err := js.PullSubscribe("test", "wqc", nats.Bind("wq", "wqc"))
	require_NoError(t, err)
	// we must have at least one message on the transformed subject name (ie no timeout)
	f := func(done chan bool) {
		for i := 0; i < 300; i++ {
			m, err := ss.Fetch(1, nats.MaxWait(3*time.Second))
			require_NoError(t, err)
			p, err := strconv.Atoi(string(m[0].Data))
			require_NoError(t, err)
			require_Equal(t, p, i)
			time.Sleep(11 * time.Millisecond)
			err = m[0].Ack()
			require_NoError(t, err)
		}
		done <- true
	}

	var doneChan = make(chan bool)
	go f(doneChan)

	checkFor(t, 6*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("wq")
		require_NoError(t, err)
		if si.State.Msgs > 0 && si.State.Msgs <= 100 {
			return fmt.Errorf("Expected 0 msgs, got: %d", si.State.Msgs)
		} else if si.State.Msgs > 100 {
			t.Fatalf("Got more than our 100 message limit: %+v", si.State)
		}
		return nil
	})

	select {
	case <-doneChan:
		ss.Drain()
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
}

func TestJetStreamClusterStreamOrphanMsgsAndReplicasDrifting(t *testing.T) {
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
				for i, mset := range msets {
					mset.mu.RLock()
					sm, err := mset.store.LoadMsg(seq, &smv)
					mset.mu.RUnlock()
					if err != nil || expectedErr != nil {
						// If one of the msets reports an error for LoadMsg for this
						// particular sequence, then the same error should be reported
						// by all msets for that seq to prove consistency across replicas.
						// If one of the msets either returns no error or doesn't return
						// the same error, then that replica has drifted.
						if msgId != _EMPTY_ {
							t.Fatalf("Expected MsgId %q for seq %d, but got error: %v", msgId, seq, err)
						} else if expectedErr == nil {
							expectedErr = err
						} else {
							require_Error(t, err, expectedErr)
						}
						continue
					}
					// Only set expected msg ID if it's for the very first time.
					if msgId == _EMPTY_ && i == 0 {
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
			t.Logf("WRN: %v", err)
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

func TestJetStreamClusterConsumerNRGCleanup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Storage:   nats.MemoryStorage,
		Retention: nats.WorkQueuePolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	// First call is just to create the pull subscribers.
	_, err = js.PullSubscribe("foo", "dlc")
	require_NoError(t, err)

	require_NoError(t, js.DeleteConsumer("TEST", "dlc"))

	// Now delete the stream.
	require_NoError(t, js.DeleteStream("TEST"))

	// Now make sure we cleaned up the NRG directories for the stream and consumer.
	var numConsumers, numStreams int
	for _, s := range c.servers {
		sd := s.JetStreamConfig().StoreDir
		nd := filepath.Join(sd, "$SYS", "_js_")
		f, err := os.Open(nd)
		require_NoError(t, err)
		dirs, err := f.ReadDir(-1)
		require_NoError(t, err)
		for _, fi := range dirs {
			if strings.HasPrefix(fi.Name(), "C-") {
				numConsumers++
			} else if strings.HasPrefix(fi.Name(), "S-") {
				numStreams++
			}
		}
		f.Close()
	}
	require_Equal(t, numConsumers, 0)
	require_Equal(t, numStreams, 0)
}

// https://github.com/nats-io/nats-server/issues/4878
func TestClusteredInterestConsumerFilterEdit(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomNonLeader()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "INTEREST",
		Retention: nats.InterestPolicy,
		Subjects:  []string{"interest.>"},
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("INTEREST", &nats.ConsumerConfig{
		Durable:       "C0",
		FilterSubject: "interest.>",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = js.Publish(fmt.Sprintf("interest.%d", i), []byte(strconv.Itoa(i)))
		require_NoError(t, err)
	}

	// we check we got 10 messages
	nfo, err := js.StreamInfo("INTEREST")
	require_NoError(t, err)
	if nfo.State.Msgs != 10 {
		t.Fatalf("expected 10 messages got %d", nfo.State.Msgs)
	}

	// now we lower the consumer interest from all subjects to 1,
	// then check the stream state and check if interest behavior still works
	_, err = js.UpdateConsumer("INTEREST", &nats.ConsumerConfig{
		Durable:       "C0",
		FilterSubject: "interest.1",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// we should now have only one message left
	nfo, err = js.StreamInfo("INTEREST")
	require_NoError(t, err)
	if nfo.State.Msgs != 1 {
		t.Fatalf("expected 1 message got %d", nfo.State.Msgs)
	}
}

func TestJetStreamClusterDoubleAckRedelivery(t *testing.T) {
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
	c := createJetStreamClusterWithTemplate(t, conf, "R3F", 3)
	defer c.shutdown()
	for _, s := range c.servers {
		s.optsMu.Lock()
		s.opts.LameDuckDuration = 15 * time.Second
		s.opts.LameDuckGracePeriod = -15 * time.Second
		s.optsMu.Unlock()
	}
	s := c.randomNonLeader()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sc, err := js.AddStream(&nats.StreamConfig{
		Name:     "LIMITS",
		Subjects: []string{"foo.>"},
		Replicas: 3,
		Storage:  nats.FileStorage,
	})
	require_NoError(t, err)

	stepDown := func() {
		_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, sc.Config.Name), nil, time.Second)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	producer := func(name string) {
		wg.Add(1)
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		defer wg.Done()

		i := 0
		payload := []byte(strings.Repeat("Z", 1024))
		for range time.NewTicker(1 * time.Millisecond).C {
			select {
			case <-ctx.Done():
				return
			default:
			}
			msgID := nats.MsgId(fmt.Sprintf("%s:%d", name, i))
			js.PublishAsync("foo.bar", payload, msgID, nats.RetryAttempts(10))
			i++
		}
	}
	go producer("A")
	go producer("B")
	go producer("C")

	sub, err := js.PullSubscribe("foo.bar", "ABC", nats.AckWait(5*time.Second), nats.MaxAckPending(1000), nats.PullMaxWaiting(1000))
	if err != nil {
		t.Fatal(err)
	}

	type ackResult struct {
		ack         *nats.Msg
		original    *nats.Msg
		redelivered *nats.Msg
	}
	received := make(map[string]int64)
	acked := make(map[string]*ackResult)
	errors := make(map[string]error)
	extraRedeliveries := 0

	wg.Add(1)
	go func() {
		nc, js = jsClientConnect(t, s)
		defer nc.Close()
		defer wg.Done()

		fetch := func(t *testing.T, batchSize int) {
			msgs, err := sub.Fetch(batchSize, nats.MaxWait(500*time.Millisecond))
			if err != nil {
				return
			}

			for _, msg := range msgs {
				meta, err := msg.Metadata()
				if err != nil {
					t.Error(err)
					continue
				}

				msgID := msg.Header.Get(nats.MsgIdHdr)
				if err, ok := errors[msgID]; ok {
					t.Logf("Redelivery (num_delivered: %v) after failed Ack Sync: %+v - %+v - error: %v", meta.NumDelivered, msg.Reply, msg.Header, err)
				}
				if resp, ok := acked[msgID]; ok {
					t.Errorf("Redelivery (num_delivered: %v) after successful Ack Sync: msgID:%v - redelivered:%v - original:%+v - ack:%+v",
						meta.NumDelivered, msgID, msg.Reply, resp.original.Reply, resp.ack)
					resp.redelivered = msg
					extraRedeliveries++
				}
				received[msgID]++

				// Retry quickly a few times after there is a failed ack.
			Retries:
				for i := 0; i < 10; i++ {
					resp, err := nc.Request(msg.Reply, []byte("+ACK"), 500*time.Millisecond)
					if err != nil {
						t.Logf("Error: %v %v", msgID, err)
						errors[msgID] = err
					} else {
						acked[msgID] = &ackResult{resp, msg, nil}
						break Retries
					}
				}
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			fetch(t, 1)
			fetch(t, 50)
		}
	}()

	// Cause a couple of step downs before the restarts as well.
	time.AfterFunc(5*time.Second, func() { stepDown() })
	time.AfterFunc(10*time.Second, func() { stepDown() })

	// Let messages be produced, and then restart the servers.
	<-time.After(15 * time.Second)

NextServer:
	for _, s := range c.servers {
		s.lameDuckMode()
		s.WaitForShutdown()
		s = c.restartServer(s)

		hctx, hcancel := context.WithTimeout(ctx, 60*time.Second)
		defer hcancel()
		for range time.NewTicker(2 * time.Second).C {
			select {
			case <-hctx.Done():
				t.Logf("WRN: Timed out waiting for healthz from %s", s)
				continue NextServer
			default:
			}

			status := s.healthz(nil)
			if status.StatusCode == 200 {
				continue NextServer
			}
		}
		// Pause in-between server restarts.
		time.Sleep(10 * time.Second)
	}

	// Stop all producer and consumer goroutines to check results.
	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
	}
	wg.Wait()
	if extraRedeliveries > 0 {
		t.Fatalf("Received %v redeliveries after a successful ack", extraRedeliveries)
	}
}

func TestJetStreamClusterBusyStreams(t *testing.T) {
	t.Skip("Too long for CI at the moment")
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
}

// https://github.com/nats-io/nats-server/issues/5488
func TestJetStreamClusterSingleMaxConsumerUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:         "TEST",
		MaxConsumers: 1,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name:          "test_consumer",
		MaxAckPending: 1000,
	})
	require_NoError(t, err)

	// This would previously return a "nats: maximum consumers limit
	// reached" (10026) error.
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Name:          "test_consumer",
		MaxAckPending: 1001,
	})
	require_NoError(t, err)
}

func TestJetStreamClusterStreamLastSequenceResetAfterStorageWipe(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// After bug was found, number of streams and wiping store directory really did not affect.
	numStreams := 50
	var wg sync.WaitGroup
	wg.Add(numStreams)

	for i := 1; i <= numStreams; i++ {
		go func(n int) {
			defer wg.Done()
			_, err := js.AddStream(&nats.StreamConfig{
				Name:      fmt.Sprintf("TEST:%d", n),
				Retention: nats.InterestPolicy,
				Subjects:  []string{fmt.Sprintf("foo.%d.*", n)},
				Replicas:  3,
			}, nats.MaxWait(30*time.Second))
			require_NoError(t, err)
			subj := fmt.Sprintf("foo.%d.bar", n)
			for i := 0; i < 222; i++ {
				js.Publish(subj, nil)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < 5; i++ {
		// Walk the servers and shut each down, and wipe the storage directory.
		for _, s := range c.servers {
			sd := s.JetStreamConfig().StoreDir
			s.Shutdown()
			s.WaitForShutdown()
			os.RemoveAll(sd)
			s = c.restartServer(s)
			checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
				hs := s.healthz(nil)
				if hs.Error != _EMPTY_ {
					return errors.New(hs.Error)
				}
				return nil
			})
		}

		for _, s := range c.servers {
			for i := 1; i <= numStreams; i++ {
				stream := fmt.Sprintf("TEST:%d", i)
				mset, err := s.GlobalAccount().lookupStream(stream)
				require_NoError(t, err)
				var state StreamState
				checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
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

func TestJetStreamClusterAckFloorBetweenLeaderAndFollowers(t *testing.T) {
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

	sub, err := js.PullSubscribe("foo.*", "consumer")
	require_NoError(t, err)

	// Do 25 rounds.
	for i := 1; i <= 25; i++ {
		// Send 50 msgs.
		for x := 0; x < 50; x++ {
			_, err := js.Publish("foo.bar", nil)
			require_NoError(t, err)
		}
		msgs, err := sub.Fetch(50, nats.MaxWait(10*time.Second))
		require_NoError(t, err)
		require_Equal(t, len(msgs), 50)
		// Randomize
		rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
		for _, m := range msgs {
			m.AckSync()
		}

		time.Sleep(100 * time.Millisecond)
		for _, s := range c.servers {
			mset, err := s.GlobalAccount().lookupStream("TEST")
			require_NoError(t, err)
			consumer := mset.lookupConsumer("consumer")
			require_NotEqual(t, consumer, nil)
			info := consumer.info()
			require_Equal(t, info.NumAckPending, 0)
			require_Equal(t, info.AckFloor.Consumer, uint64(i*50))
			require_Equal(t, info.AckFloor.Stream, uint64(i*50))
		}
	}
}

// https://github.com/nats-io/nats-server/pull/5600
func TestJetStreamClusterConsumerLeak(t *testing.T) {
	N := 2000 // runs in under 10s, but significant enough to see the difference.
	NConcurrent := 100

	clusterConf := `
	listen: 127.0.0.1:-1

	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	leafnodes {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {
		ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

	cl := createJetStreamClusterWithTemplate(t, clusterConf, "Leak-test", 3)
	defer cl.shutdown()
	cl.waitOnLeader()

	s := cl.randomNonLeader()

	// Create the test stream.
	streamName := "LEAK_TEST_STREAM"
	nc, js := jsClientConnect(t, s, nats.UserInfo("one", "p"))
	defer nc.Close()
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      streamName,
		Subjects:  []string{"$SOMETHING.>"},
		Storage:   nats.FileStorage,
		Retention: nats.InterestPolicy,
		Replicas:  3,
	})
	if err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}

	concurrent := make(chan struct{}, NConcurrent)
	for i := 0; i < NConcurrent; i++ {
		concurrent <- struct{}{}
	}
	errors := make(chan error, N)

	wg := sync.WaitGroup{}
	wg.Add(N)

	// Gather the stats for comparison.
	before := &runtime.MemStats{}
	runtime.GC()
	runtime.ReadMemStats(before)

	for i := 0; i < N; {
		// wait for a slot to open up
		<-concurrent
		i++
		go func() {
			defer func() {
				concurrent <- struct{}{}
				wg.Done()
			}()

			nc, js := jsClientConnect(t, s, nats.UserInfo("one", "p"))
			defer nc.Close()

			consumerName := "sessid_" + nuid.Next()
			_, err := js.AddConsumer(streamName, &nats.ConsumerConfig{
				DeliverSubject: "inbox",
				Durable:        consumerName,
				AckPolicy:      nats.AckExplicitPolicy,
				DeliverPolicy:  nats.DeliverNewPolicy,
				FilterSubject:  "$SOMETHING.ELSE.subject",
				AckWait:        30 * time.Second,
				MaxAckPending:  1024,
			})
			if err != nil {
				errors <- fmt.Errorf("Error on JetStream consumer creation: %v", err)
				return
			}

			err = js.DeleteConsumer(streamName, consumerName)
			if err != nil {
				errors <- fmt.Errorf("Error on JetStream consumer deletion: %v", err)
			}
		}()
	}

	wg.Wait()
	if len(errors) > 0 {
		for err := range errors {
			t.Fatalf("%v", err)
		}
	}

	after := &runtime.MemStats{}
	runtime.GC()
	runtime.ReadMemStats(after)

	// Before https://github.com/nats-io/nats-server/pull/5600 this test was
	// adding 180Mb+ to HeapInuse. Now it's under 40Mb (ran locally on a Mac)
	limit := before.HeapInuse + 100*1024*1024 // 100MB
	if after.HeapInuse > before.HeapInuse+limit {
		t.Fatalf("Extra memory usage too high: %v", after.HeapInuse-before.HeapInuse)
	}
}

func TestJetStreamClusterWQRoundRobinSubjectRetention(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "wq_stream",
		Subjects:  []string{"something.>"},
		Storage:   nats.FileStorage,
		Retention: nats.WorkQueuePolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		n := (i % 5) + 1
		_, err := js.Publish(fmt.Sprintf("something.%d", n), nil)
		require_NoError(t, err)
	}

	sub, err := js.PullSubscribe(
		"something.5",
		"wq_consumer_5",
		nats.BindStream("wq_stream"),
		nats.ConsumerReplicas(3),
	)
	require_NoError(t, err)

	for {
		msgs, _ := sub.Fetch(5)
		if len(msgs) == 0 {
			break
		}
		for _, msg := range msgs {
			require_NoError(t, msg.AckSync())
		}
	}

	si, err := js.StreamInfo("wq_stream")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 80)
	require_Equal(t, si.State.NumDeleted, 20)
	require_Equal(t, si.State.NumSubjects, 4)
}

type captureLeafClusterSpacesLogger struct {
	DummyLogger
	ch     chan string
	warnCh chan string
}

func (l *captureLeafClusterSpacesLogger) Warnf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, `Server name has spaces and used as the cluster name, leaf remotes may not connect properly`) {
		select {
		case l.warnCh <- msg:
		default:
		}
	}
}

func (l *captureLeafClusterSpacesLogger) Errorf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, `Leafnode Error 'cluster name cannot contain spaces or new lines'`) {
		select {
		case l.ch <- msg:
		default:
		}
	}
}

func TestJetStreamClusterAndNamesWithSpaces(t *testing.T) {
	gwConf := `
		listen: 127.0.0.1:-1
                http: 127.0.0.1:-1
		server_name: 'SRV %s'
		jetstream: {
			store_dir: '%s',
		}
		cluster {
			name: '%s'
			listen: 127.0.0.1:%d
			routes = [%s]
		}
		server_tags: ["test"]
		system_account: sys
		no_auth_user: js

		leafnodes {
			host: "127.0.0.1"
			port: -1
                }

		accounts {
			sys {
                          users = [ { user: sys, pass: sys } ] }
			js {
                          jetstream: enabled
  			  users = [ { user: js, pass: js } ]
		    }
		}
        `
	c := createJetStreamClusterAndModHook(t, gwConf, "S P A C E 1", "GW_1_", 3, 15022, false,
		func(serverName, clusterName, storeDir, conf string) string {
			conf += `
			   gateway {
				  name: "S P A C E 1"
				  listen: 127.0.0.1:-1
			  }
                        `
			return conf
		})
	defer c.shutdown()

	c2 := createJetStreamClusterAndModHook(t, gwConf, "S P A C E 2", "GW_2_", 3, 16022, false,
		func(serverName, clusterName, storeDir, conf string) string {
			conf += fmt.Sprintf(`
			   gateway {
				  name: "S P A C E 2"
				  listen: 127.0.0.1:-1
				  gateways: [{
					  name: "S P A C E 1"
					  url: "nats://127.0.0.1:%d"
				  }]
			  }
                `, c.servers[0].opts.Gateway.Port)
			return conf
		})
	defer c2.shutdown()

	c3 := createJetStreamClusterAndModHook(t, gwConf, "S P A C E 3", "GW_3_", 3, 17022, false,
		func(serverName, clusterName, storeDir, conf string) string {
			conf += fmt.Sprintf(`
			   gateway {
				  name: "S P A C E 3"
				  listen: 127.0.0.1:-1
				  gateways: [{
					  name: "S P A C E 1"
					  url: "nats://127.0.0.1:%d"
				  }]
			  }
                `, c.servers[0].opts.Gateway.Port)
			return conf
		})
	defer c3.shutdown()

	for _, s := range c2.servers {
		waitForOutboundGateways(t, s, 2, 2*time.Second)
	}
	for _, s := range c3.servers {
		waitForOutboundGateways(t, s, 2, 2*time.Second)
	}

	// Leaf with spaces in name which becomes its cluster name as well.
	leafConfA := `
		host: "127.0.0.1"
		port: -1

		server_name: "L E A F S P A C E"

		leafnodes {
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"
			remotes: [ {
				url: "nats://127.0.0.1:%d"
			} ]
		}
	`
	leafConfA = fmt.Sprintf(leafConfA, c.servers[0].opts.LeafNode.Port)
	sconfA := createConfFile(t, []byte(leafConfA))
	oA := LoadConfig(sconfA)
	leafA, err := NewServer(oA)
	require_NoError(t, err)
	lA := &captureLeafClusterSpacesLogger{ch: make(chan string, 10), warnCh: make(chan string, 10)}
	leafA.SetLogger(lA, false, false)
	leafA.Start()
	defer leafA.Shutdown()

	// Leaf with spaces in name but with a valid cluster name is able to connect.
	leafConfB := `
		host: "127.0.0.1"
		port: -1
                http: 127.0.0.1:-1

		server_name: "L E A F 2"
                cluster { name: "LEAF" }

		leafnodes {
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"
			remotes: [ {
				url: "nats://127.0.0.1:%d"
			} ]
		}
	`
	leafConfB = fmt.Sprintf(leafConfB, c.servers[0].opts.LeafNode.Port)
	sconfB := createConfFile(t, []byte(leafConfB))
	leafB, _ := RunServerWithConfig(sconfB)
	lB := &captureLeafClusterSpacesLogger{ch: make(chan string, 10)}
	leafB.SetLogger(lB, false, false)
	defer leafB.Shutdown()

	// Leaf with valid server name but cluster name with spaces.
	leafConfC := `
		host: "127.0.0.1"
		port: -1
                http: 127.0.0.1:-1

		server_name: "LEAF3"
                cluster { name: "L E A F 3" }

		leafnodes {
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"
			remotes: [ {
				url: "nats://127.0.0.1:%d"
			} ]
		}
	`
	leafConfC = fmt.Sprintf(leafConfC, c.servers[0].opts.LeafNode.Port)
	sconfC := createConfFile(t, []byte(leafConfC))
	leafC, _ := RunServerWithConfig(sconfC)
	lC := &captureLeafClusterSpacesLogger{ch: make(chan string, 10)}
	leafC.SetLogger(lC, false, false)
	defer leafC.Shutdown()

	// Leafs with valid server name but using protocol special characters in cluster name.
	leafConfD := `
		host: "127.0.0.1"
		port: -1
                http: 127.0.0.1:-1

		server_name: "LEAF4"
                cluster { name: "LEAF
4" }

		leafnodes {
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"
			remotes: [ {
				url: "nats://127.0.0.1:%d"
			} ]
		}
	`
	leafConfD = fmt.Sprintf(leafConfD, c.servers[0].opts.LeafNode.Port)
	sconfD := createConfFile(t, []byte(leafConfD))
	leafD, _ := RunServerWithConfig(sconfD)
	lD := &captureLeafClusterSpacesLogger{ch: make(chan string, 10)}
	leafD.SetLogger(lD, false, false)
	defer leafD.Shutdown()

	leafConfD2 := `
		host: "127.0.0.1"
		port: -1
                http: 127.0.0.1:-1

		server_name: "LEAF42"
                cluster { name: "LEAF4\r2" }

		leafnodes {
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"
			remotes: [ {
				url: "nats://127.0.0.1:%d"
			} ]
		}
	`
	leafConfD2 = fmt.Sprintf(leafConfD2, c.servers[0].opts.LeafNode.Port)
	sconfD2 := createConfFile(t, []byte(leafConfD2))
	leafD2, _ := RunServerWithConfig(sconfD2)
	lD2 := &captureLeafClusterSpacesLogger{ch: make(chan string, 10)}
	leafD2.SetLogger(lD2, false, false)
	defer leafD2.Shutdown()

	leafConfD3 := `
		host: "127.0.0.1"
		port: -1
                http: 127.0.0.1:-1

		server_name: "LEAF43"
                cluster { name: "LEAF4\t3" }

		leafnodes {
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"
			remotes: [ {
				url: "nats://127.0.0.1:%d"
			} ]
		}
	`
	leafConfD3 = fmt.Sprintf(leafConfD3, c.servers[0].opts.LeafNode.Port)
	sconfD3 := createConfFile(t, []byte(leafConfD3))
	leafD3, _ := RunServerWithConfig(sconfD3)
	lD3 := &captureLeafClusterSpacesLogger{ch: make(chan string, 10)}
	leafD3.SetLogger(lD3, false, false)
	defer leafD3.Shutdown()

	// Leaf with valid configuration should be able to connect to GW cluster with spaces in names.
	leafConfE := `
		host: "127.0.0.1"
		port: -1
                http: 127.0.0.1:-1

		server_name: "LEAF5"
                cluster { name: "LEAF5" }

		leafnodes {
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"
			remotes: [ {
				url: "nats://127.0.0.1:%d"
			} ]
		}
	`
	leafConfE = fmt.Sprintf(leafConfE, c.servers[0].opts.LeafNode.Port)
	sconfE := createConfFile(t, []byte(leafConfE))
	leafE, _ := RunServerWithConfig(sconfE)
	lE := &captureLeafClusterSpacesLogger{ch: make(chan string, 10)}
	leafE.SetLogger(lE, false, false)
	defer leafE.Shutdown()

	// Finally do a smoke test of connectivity among gateways and that JS is working
	// when using clusters with spaces still.
	nc1, js1 := jsClientConnect(t, c.servers[1])
	defer nc1.Close()

	_, err = js1.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)
	c.waitOnStreamLeader("js", "foo")

	sub1, err := nc1.SubscribeSync("foo")
	require_NoError(t, err)
	nc1.Flush()

	// Check that invalid configs got the errors.
	select {
	case <-lA.ch:
	case <-time.After(5 * time.Second):
		t.Errorf("Timed out waiting for error")
	}
	select {
	case <-lC.ch:
	case <-time.After(5 * time.Second):
		t.Errorf("Timed out waiting for error")
	}
	select {
	case <-lD.ch:
	case <-time.After(5 * time.Second):
		t.Errorf("Timed out waiting for error")
	}
	select {
	case <-lD2.ch:
	case <-time.After(5 * time.Second):
		t.Errorf("Timed out waiting for error")
	}
	select {
	case <-lD3.ch:
	case <-time.After(5 * time.Second):
		t.Errorf("Timed out waiting for error")
	}

	// Check that we got a warning about the server name being reused
	// for the cluster name.
	select {
	case <-lA.warnCh:
	case <-time.After(5 * time.Second):
		t.Errorf("Timed out waiting for warning")
	}

	// Check that valid configs were ok still.
	select {
	case <-lB.ch:
		t.Errorf("Unexpected error from valid leafnode config")
	case <-lE.ch:
		t.Errorf("Unexpected error from valid leafnode config")
	case <-time.After(2 * time.Second):
	}

	nc2, js2 := jsClientConnect(t, c2.servers[1])
	defer nc2.Close()
	nc2.Publish("foo", []byte("test"))
	nc2.Flush()
	time.Sleep(250 * time.Millisecond)

	msg, err := sub1.NextMsg(1 * time.Second)
	require_NoError(t, err)
	require_Equal(t, "test", string(msg.Data))
	sinfo, err := js2.StreamInfo("foo")
	require_NoError(t, err)
	require_Equal(t, sinfo.State.Msgs, 1)
}

func TestJetStreamClusterMetaSyncOrphanCleanup(t *testing.T) {
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "R3S", 3,
		func(serverName, clusterName, storeDir, conf string) string {
			return fmt.Sprintf("%s\nserver_tags: [server:%s]", conf, serverName)
		})
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create a bunch of streams on S1
	for i := 0; i < 100; i++ {
		stream := fmt.Sprintf("TEST-%d", i)
		subject := fmt.Sprintf("TEST.%d", i)
		_, err := js.AddStream(&nats.StreamConfig{
			Name:      stream,
			Subjects:  []string{subject},
			Storage:   nats.FileStorage,
			Placement: &nats.Placement{Tags: []string{"server:S-1"}},
		})
		require_NoError(t, err)
		// Put in 10 msgs to each
		for j := 0; j < 10; j++ {
			_, err := js.Publish(subject, nil)
			require_NoError(t, err)
		}
	}

	// Now we will shutdown S1 and remove all of its meta-data to trip the condition.
	s := c.serverByName("S-1")
	require_True(t, s != nil)

	sd := s.JetStreamConfig().StoreDir
	nd := filepath.Join(sd, "$SYS", "_js_", "_meta_")
	s.Shutdown()
	s.WaitForShutdown()
	os.RemoveAll(nd)
	s = c.restartServer(s)
	c.waitOnServerCurrent(s)
	jsz, err := s.Jsz(nil)
	require_NoError(t, err)
	require_Equal(t, jsz.Streams, 100)

	// These will be recreated by the meta layer, but if the orphan detection deleted them they will be empty,
	// so check all streams to make sure they still have data.
	acc := s.GlobalAccount()
	var state StreamState
	for i := 0; i < 100; i++ {
		mset, err := acc.lookupStream(fmt.Sprintf("TEST-%d", i))
		require_NoError(t, err)
		mset.store.FastState(&state)
		require_Equal(t, state.Msgs, 10)
	}
}

func TestJetStreamClusterKeyValueDesyncAfterHardKill(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3F", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.serverByName("S-1"))
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:   "inconsistency",
		Replicas: 3,
	})
	require_NoError(t, err)

	// First create should succeed.
	revision, err := kv.Create("key.exists", []byte("1"))
	require_NoError(t, err)
	require_Equal(t, revision, 1)

	// Second create will be rejected but bump CLFS.
	_, err = kv.Create("key.exists", []byte("2"))
	require_Error(t, err)

	// Insert a new message, should only be applied once, even if we hard kill and replay afterward.
	revision, err = kv.Put("key.put", []byte("3"))
	require_NoError(t, err)
	require_Equal(t, revision, 2)

	// Restart a server
	s3 := c.serverByName("S-3")
	// We will remove the index.db file after we shutdown.
	mset, err := s3.GlobalAccount().lookupStream("KV_inconsistency")
	require_NoError(t, err)
	fs := mset.store.(*fileStore)
	ifile := filepath.Join(fs.fcfg.StoreDir, msgDir, "index.db")

	s3.Shutdown()
	s3.WaitForShutdown()
	// Remove the index.db file to simulate a hard kill where server can not write out the index.db file.
	require_NoError(t, os.Remove(ifile))

	c.restartServer(s3)
	c.waitOnClusterReady()
	c.waitOnAllCurrent()

	err = checkState(t, c, "$G", "KV_inconsistency")
	require_NoError(t, err)
}

func TestJetStreamClusterKeyValueSync(t *testing.T) {
	t.Skip("Too long for CI at the moment")
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

func TestJetStreamClusterKeyValueLastSeqMismatch(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	for _, r := range []int{1, 3} {
		t.Run(fmt.Sprintf("R=%d", r), func(t *testing.T) {
			kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:   fmt.Sprintf("mismatch_%v", r),
				Replicas: r,
			})
			require_NoError(t, err)

			revision, err := kv.Create("foo", []byte("1"))
			require_NoError(t, err)
			require_Equal(t, revision, 1)

			revision, err = kv.Create("bar", []byte("2"))
			require_NoError(t, err)
			require_Equal(t, revision, 2)

			// Now say we want to update baz but iff last was revision 1.
			_, err = kv.Update("baz", []byte("3"), uint64(1))
			require_Error(t, err)
			require_Equal(t, err.Error(), `nats: wrong last sequence: 0`)
		})
	}
}

func TestJetStreamClusterPubAckSequenceDupe(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "TEST_CLUSTER", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	type client struct {
		nc *nats.Conn
		js nats.JetStreamContext
	}

	clients := make([]client, len(c.servers))
	for i, server := range c.servers {
		clients[i].nc, clients[i].js = jsClientConnect(t, server)
		defer clients[i].nc.Close()
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       "TEST_STREAM",
		Subjects:   []string{"TEST_SUBJECT.*"},
		Replicas:   3,
		Duplicates: 1 * time.Minute,
	})
	require_NoError(t, err)

	msgData := []byte("...")

	for seq := uint64(1); seq < 10; seq++ {

		if seq%3 == 0 {
			c.restartAll()
		}

		msgSubject := "TEST_SUBJECT." + strconv.FormatUint(seq, 10)
		msgIdOpt := nats.MsgId(nuid.Next())

		firstPublisherClient := &clients[rand.Intn(len(clients))]
		secondPublisherClient := &clients[rand.Intn(len(clients))]

		pubAck1, err := firstPublisherClient.js.Publish(msgSubject, msgData, msgIdOpt)
		require_NoError(t, err)
		require_Equal(t, seq, pubAck1.Sequence)
		require_False(t, pubAck1.Duplicate)

		pubAck2, err := secondPublisherClient.js.Publish(msgSubject, msgData, msgIdOpt)
		require_NoError(t, err)
		require_Equal(t, seq, pubAck2.Sequence)
		require_True(t, pubAck2.Duplicate)

	}

}

func TestJetStreamClusterConsumeWithStartSequence(t *testing.T) {

	const (
		NumMessages         = 10
		ChosenSeq           = 5
		StreamName          = "TEST"
		StreamSubject       = "ORDERS.*"
		StreamSubjectPrefix = "ORDERS."
	)

	for _, ClusterSize := range []int{
		1, // Single server
		3, // 3-node cluster
	} {
		R := ClusterSize
		t.Run(
			fmt.Sprintf("Nodes:%d,Replicas:%d", ClusterSize, R),
			func(t *testing.T) {
				// This is the success condition for all sub-tests below
				var ExpectedMsgId = ""
				checkMessage := func(t *testing.T, msg *nats.Msg) {
					t.Helper()

					msgMeta, err := msg.Metadata()
					require_NoError(t, err)

					// Check sequence number
					require_Equal(t, msgMeta.Sequence.Stream, ChosenSeq)

					// Check message id
					require_NotEqual(t, ExpectedMsgId, "")
					require_Equal(t, msg.Header.Get(nats.MsgIdHdr), ExpectedMsgId)
				}

				checkRawMessage := func(t *testing.T, msg *nats.RawStreamMsg) {
					t.Helper()

					// Check sequence number
					require_Equal(t, msg.Sequence, ChosenSeq)

					// Check message id
					require_NotEqual(t, ExpectedMsgId, "")
					require_Equal(t, msg.Header.Get(nats.MsgIdHdr), ExpectedMsgId)
				}

				// Setup: start server or cluster, connect client
				var server *Server
				if ClusterSize == 1 {
					server = RunBasicJetStreamServer(t)
					defer server.Shutdown()
				} else {
					c := createJetStreamCluster(t, jsClusterTempl, "HUB", _EMPTY_, ClusterSize, 22020, true)
					defer c.shutdown()
					server = c.randomServer()
				}

				// Setup: connect
				var nc *nats.Conn
				var js nats.JetStreamContext
				nc, js = jsClientConnect(t, server)
				defer nc.Close()

				// Setup: create stream
				_, err := js.AddStream(&nats.StreamConfig{
					Replicas: R,
					Name:     StreamName,
					Subjects: []string{StreamSubject},
				})
				require_NoError(t, err)

				// Setup: populate stream
				buf := make([]byte, 100)
				for i := uint64(1); i <= NumMessages; i++ {
					msgId := nuid.Next()
					pubAck, err := js.Publish(StreamSubjectPrefix+strconv.Itoa(int(i)), buf, nats.MsgId(msgId))
					require_NoError(t, err)

					// Verify assumption made in tests below
					require_Equal(t, pubAck.Sequence, i)

					if i == ChosenSeq {
						// Save the expected message id for the chosen message
						ExpectedMsgId = msgId
					}
				}

				// Setup: create subscriptions, needs to be after stream creation or OptStartSeq could be clipped
				var preCreatedSub, preCreatedSubDurable *nats.Subscription
				{
					preCreatedSub, err = js.PullSubscribe(
						StreamSubject,
						"",
						nats.StartSequence(ChosenSeq),
					)
					require_NoError(t, err)
					defer func() {
						require_NoError(t, preCreatedSub.Unsubscribe())
					}()

					const Durable = "dlc_pre_created"
					c, err := js.AddConsumer(StreamName, &nats.ConsumerConfig{
						Durable:       Durable,
						DeliverPolicy: nats.DeliverByStartSequencePolicy,
						OptStartSeq:   ChosenSeq,
						Replicas:      R,
					})
					require_NoError(t, err)
					defer func() {
						require_NoError(t, js.DeleteConsumer(c.Stream, c.Name))
					}()

					preCreatedSubDurable, err = js.PullSubscribe(
						"",
						"",
						nats.Bind(StreamName, Durable),
					)
					require_NoError(t, err)
					defer func() {
						require_NoError(t, preCreatedSubDurable.Unsubscribe())
					}()
				}

				// Tests various ways to consume the stream starting at the ChosenSeq sequence

				t.Run(
					"DurableConsumer",
					func(t *testing.T) {
						const Durable = "dlc"
						c, err := js.AddConsumer(StreamName, &nats.ConsumerConfig{
							Durable:       Durable,
							DeliverPolicy: nats.DeliverByStartSequencePolicy,
							OptStartSeq:   ChosenSeq,
							Replicas:      R,
						})
						require_NoError(t, err)
						defer func() {
							require_NoError(t, js.DeleteConsumer(c.Stream, c.Name))
						}()

						sub, err := js.PullSubscribe(
							StreamSubject,
							Durable,
						)
						require_NoError(t, err)
						defer func() {
							require_NoError(t, sub.Unsubscribe())
						}()

						msgs, err := sub.Fetch(1)
						require_NoError(t, err)
						require_Equal(t, len(msgs), 1)

						checkMessage(t, msgs[0])
					},
				)

				t.Run(
					"DurableConsumerWithBind",
					func(t *testing.T) {
						const Durable = "dlc_bind"
						c, err := js.AddConsumer(StreamName, &nats.ConsumerConfig{
							Durable:       Durable,
							DeliverPolicy: nats.DeliverByStartSequencePolicy,
							OptStartSeq:   ChosenSeq,
							Replicas:      R,
						})
						require_NoError(t, err)
						defer func() {
							require_NoError(t, js.DeleteConsumer(c.Stream, c.Name))
						}()

						sub, err := js.PullSubscribe(
							"",
							"",
							nats.Bind(StreamName, Durable),
						)
						require_NoError(t, err)
						defer func() {
							require_NoError(t, sub.Unsubscribe())
						}()

						msgs, err := sub.Fetch(1)
						require_NoError(t, err)
						require_Equal(t, len(msgs), 1)

						checkMessage(t, msgs[0])
					},
				)

				t.Run(
					"PreCreatedDurableConsumerWithBind",
					func(t *testing.T) {
						msgs, err := preCreatedSubDurable.Fetch(1)
						require_NoError(t, err)
						require_Equal(t, len(msgs), 1)

						checkMessage(t, msgs[0])
					},
				)

				t.Run(
					"PullConsumer",
					func(t *testing.T) {
						sub, err := js.PullSubscribe(
							StreamSubject,
							"",
							nats.StartSequence(ChosenSeq),
						)
						require_NoError(t, err)
						defer func() {
							require_NoError(t, sub.Unsubscribe())
						}()

						msgs, err := sub.Fetch(1)
						require_NoError(t, err)
						require_Equal(t, len(msgs), 1)

						checkMessage(t, msgs[0])
					},
				)

				t.Run(
					"PreCreatedPullConsumer",
					func(t *testing.T) {
						msgs, err := preCreatedSub.Fetch(1)
						require_NoError(t, err)
						require_Equal(t, len(msgs), 1)

						checkMessage(t, msgs[0])
					},
				)

				t.Run(
					"SynchronousConsumer",
					func(t *testing.T) {
						sub, err := js.SubscribeSync(
							StreamSubject,
							nats.StartSequence(ChosenSeq),
						)
						if err != nil {
							return
						}
						require_NoError(t, err)
						defer func() {
							require_NoError(t, sub.Unsubscribe())
						}()

						msg, err := sub.NextMsg(1 * time.Second)
						require_NoError(t, err)
						checkMessage(t, msg)
					},
				)

				t.Run(
					"CallbackSubscribe",
					func(t *testing.T) {
						var waitGroup sync.WaitGroup
						waitGroup.Add(1)
						// To be populated by callback
						var receivedMsg *nats.Msg

						sub, err := js.Subscribe(
							StreamSubject,
							func(msg *nats.Msg) {
								// Save first message received
								if receivedMsg == nil {
									receivedMsg = msg
									waitGroup.Done()
								}
							},
							nats.StartSequence(ChosenSeq),
						)
						require_NoError(t, err)
						defer func() {
							require_NoError(t, sub.Unsubscribe())
						}()

						waitGroup.Wait()
						require_NotNil(t, receivedMsg)
						checkMessage(t, receivedMsg)
					},
				)

				t.Run(
					"ChannelSubscribe",
					func(t *testing.T) {
						msgChannel := make(chan *nats.Msg, 1)
						sub, err := js.ChanSubscribe(
							StreamSubject,
							msgChannel,
							nats.StartSequence(ChosenSeq),
						)
						require_NoError(t, err)
						defer func() {
							require_NoError(t, sub.Unsubscribe())
						}()

						msg := <-msgChannel
						checkMessage(t, msg)
					},
				)

				t.Run(
					"GetRawStreamMessage",
					func(t *testing.T) {
						rawMsg, err := js.GetMsg(StreamName, ChosenSeq)
						require_NoError(t, err)
						checkRawMessage(t, rawMsg)
					},
				)

				t.Run(
					"GetLastMessageBySubject",
					func(t *testing.T) {
						rawMsg, err := js.GetLastMsg(
							StreamName,
							fmt.Sprintf("ORDERS.%d", ChosenSeq),
						)
						require_NoError(t, err)
						checkRawMessage(t, rawMsg)
					},
				)
			},
		)
	}
}

func TestJetStreamClusterAckDeleted(t *testing.T) {

	const (
		NumMessages         = 10
		StreamName          = "TEST"
		StreamSubject       = "ORDERS.*"
		StreamSubjectPrefix = "ORDERS."
	)

	for _, ClusterSize := range []int{
		1, // Single server
		3, // 3-node cluster
	} {
		R := ClusterSize
		t.Run(
			fmt.Sprintf("Nodes:%d,Replicas:%d", ClusterSize, R),
			func(t *testing.T) {
				// Setup: start server or cluster, connect client
				var server *Server
				if ClusterSize == 1 {
					server = RunBasicJetStreamServer(t)
					defer server.Shutdown()
				} else {
					c := createJetStreamCluster(t, jsClusterTempl, "HUB", _EMPTY_, ClusterSize, 22020, true)
					defer c.shutdown()
					server = c.randomServer()
				}

				// Setup: connect
				var nc *nats.Conn
				var js nats.JetStreamContext
				nc, js = jsClientConnect(t, server)
				defer nc.Close()

				// Setup: create stream
				_, err := js.AddStream(&nats.StreamConfig{
					Replicas:  R,
					Name:      StreamName,
					Subjects:  []string{StreamSubject},
					Retention: nats.LimitsPolicy,
					Discard:   nats.DiscardOld,
					MaxMsgs:   1, // Only keep the latest message
				})
				require_NoError(t, err)

				// Setup: create durable consumer and subscription
				const Durable = "dlc"
				c, err := js.AddConsumer(StreamName, &nats.ConsumerConfig{
					Durable:       Durable,
					Replicas:      R,
					AckPolicy:     nats.AckExplicitPolicy,
					MaxAckPending: NumMessages,
				})
				require_NoError(t, err)
				defer func() {
					require_NoError(t, js.DeleteConsumer(c.Stream, c.Name))
				}()

				// Setup: create durable consumer subscription
				sub, err := js.PullSubscribe(
					"",
					"",
					nats.Bind(StreamName, Durable),
				)
				require_NoError(t, err)
				defer func() {
					require_NoError(t, sub.Unsubscribe())
				}()

				// Collect received and non-ACKed messages
				receivedMessages := make([]*nats.Msg, 0, NumMessages)

				buf := make([]byte, 100)
				for i := uint64(1); i <= NumMessages; i++ {
					// Publish one message
					msgId := nuid.Next()
					pubAck, err := js.Publish(
						StreamSubjectPrefix+strconv.Itoa(int(i)),
						buf,
						nats.MsgId(msgId),
					)
					require_NoError(t, err)
					require_Equal(t, pubAck.Sequence, i)

					// Consume message
					msgs, err := sub.Fetch(1)
					require_NoError(t, err)
					require_Equal(t, len(msgs), 1)

					// Validate message
					msg := msgs[0]
					require_Equal(t, msgs[0].Header.Get(nats.MsgIdHdr), msgId)

					// Validate message metadata
					msgMeta, err := msg.Metadata()
					require_NoError(t, err)
					// Check sequence number
					require_Equal(t, msgMeta.Sequence.Stream, i)

					// Save for ACK later
					receivedMessages = append(receivedMessages, msg)
				}

				// Verify stream state, expecting a single message due to limits
				streamInfo, err := js.StreamInfo(StreamName)
				require_NoError(t, err)
				require_Equal(t, streamInfo.State.Msgs, 1)

				// Verify consumer state, expecting ack floor corresponding to messages dropped
				consumerInfo, err := js.ConsumerInfo(StreamName, Durable)
				require_NoError(t, err)
				require_Equal(t, consumerInfo.NumAckPending, 1)
				require_Equal(t, consumerInfo.AckFloor.Stream, 9)
				require_Equal(t, consumerInfo.AckFloor.Consumer, 9)

				// ACK all messages (all except last have been dropped from the stream)
				for _, message := range receivedMessages {
					err := message.AckSync()
					require_NoError(t, err)
				}

				// Verify consumer state, all messages ACKed
				consumerInfo, err = js.ConsumerInfo(StreamName, Durable)
				require_NoError(t, err)
				require_Equal(t, consumerInfo.NumAckPending, 0)
				require_Equal(t, consumerInfo.AckFloor.Stream, 10)
				require_Equal(t, consumerInfo.AckFloor.Consumer, 10)
			},
		)
	}
}

func TestJetStreamClusterAPILimitDefault(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	for _, s := range c.servers {
		s.optsMu.RLock()
		lim := s.opts.JetStreamRequestQueueLimit
		s.optsMu.RUnlock()

		require_Equal(t, lim, JSDefaultRequestQueueLimit)
		require_Equal(t, atomic.LoadInt64(&s.getJetStream().queueLimit), JSDefaultRequestQueueLimit)
	}
}

func TestJetStreamClusterAPILimitAdvisory(t *testing.T) {
	// Hit the limit straight away.
	const queueLimit = 1

	config := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {
			max_mem_store: 256MB
			max_file_store: 2GB
			store_dir: '%s'
			request_queue_limit: ` + fmt.Sprintf("%d", queueLimit) + `
		}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
    `
	c := createJetStreamClusterWithTemplate(t, config, "R3S", 3)
	defer c.shutdown()

	c.waitOnLeader()
	s := c.randomNonLeader()

	for _, s := range c.servers {
		lim := atomic.LoadInt64(&s.getJetStream().queueLimit)
		require_Equal(t, lim, queueLimit)
	}

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	snc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	sub, err := snc.SubscribeSync(JSAdvisoryAPILimitReached)
	require_NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	require_NoError(t, nc.PublishMsg(&nats.Msg{
		Subject: fmt.Sprintf(JSApiConsumerListT, "TEST"),
		Reply:   nc.NewInbox(),
	}))

	// Wait for the advisory to come in.
	msg, err := sub.NextMsgWithContext(ctx)
	require_NoError(t, err)
	var advisory JSAPILimitReachedAdvisory
	require_NoError(t, json.Unmarshal(msg.Data, &advisory))
	require_Equal(t, advisory.Domain, _EMPTY_)     // No JetStream domain was set.
	require_Equal(t, advisory.Dropped, queueLimit) // Configured queue limit.
}

func TestJetStreamPendingRequestsInJsz(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	c.waitOnLeader()
	metaleader := c.leader()

	sjs := metaleader.getJetStream()
	sjs.mu.Lock()
	sub := &subscription{
		subject: []byte("$JS.API.VERY_SLOW"),
		icb: func(sub *subscription, client *client, acc *Account, subject, reply string, rmsg []byte) {
			select {
			case <-client.srv.quitCh:
			case <-time.After(time.Second * 3):
			}
		},
	}
	err := metaleader.getJetStream().apiSubs.Insert(sub)
	sjs.mu.Unlock()

	require_NoError(t, err)

	nc, _ := jsClientConnect(t, c.randomNonLeader())
	defer nc.Close()

	inbox := nc.NewRespInbox()
	msg := &nats.Msg{
		Subject: "$JS.API.VERY_SLOW",
		Reply:   inbox,
	}

	// Fall short of hitting the API limit by a little bit,
	// otherwise the requests get drained away.
	for i := 0; i < JSDefaultRequestQueueLimit-10; i++ {
		require_NoError(t, nc.PublishMsg(msg))
	}

	jsz, err := metaleader.Jsz(nil)
	require_NoError(t, err)
	require_True(t, jsz.Meta != nil)
	require_NotEqual(t, jsz.Meta.Pending, 0)

	snc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	ch := make(chan *nats.Msg, 1)
	ssub, err := snc.ChanSubscribe(fmt.Sprintf(serverStatsSubj, metaleader.ID()), ch)
	require_NoError(t, err)
	require_NoError(t, ssub.AutoUnsubscribe(1))

	msg = require_ChanRead(t, ch, time.Second*5)
	var m ServerStatsMsg
	require_NoError(t, json.Unmarshal(msg.Data, &m))
	require_True(t, m.Stats.JetStream != nil)
	require_NotEqual(t, m.Stats.JetStream.Meta.Pending, 0)
}

func TestJetStreamConsumerReplicasAfterScale(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomNonLeader())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 5,
	})
	require_NoError(t, err)

	// Put some messages in to test consumer state transfer.
	for i := 0; i < 100; i++ {
		js.PublishAsync("foo", []byte("ok"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Create four different consumers.
	// Normal where we inherit replicas from parent.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "dur",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)
	require_Equal(t, ci.Config.Replicas, 0)
	require_Equal(t, len(ci.Cluster.Replicas), 4)

	// Ephemeral
	ci, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)
	require_Equal(t, ci.Config.Replicas, 0) // Legacy ephemeral is 0 here too.
	require_Equal(t, len(ci.Cluster.Replicas), 0)
	eName := ci.Name

	// R1
	ci, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "r1",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)
	require_Equal(t, ci.Config.Replicas, 1)
	require_Equal(t, len(ci.Cluster.Replicas), 0)

	// R3
	ci, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name:      "r3",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)
	require_Equal(t, ci.Config.Replicas, 3)
	require_Equal(t, len(ci.Cluster.Replicas), 2)

	// Now create some state on r1 consumer.
	sub, err := js.PullSubscribe("foo", "r1")
	require_NoError(t, err)

	fetch := rand.Intn(99) + 1 // Needs to be at least 1.
	msgs, err := sub.Fetch(fetch, nats.MaxWait(10*time.Second))
	require_NoError(t, err)
	require_Equal(t, len(msgs), fetch)
	ack := rand.Intn(fetch)
	for i := 0; i <= ack; i++ {
		msgs[i].AckSync()
	}
	r1ci, err := js.ConsumerInfo("TEST", "r1")
	require_NoError(t, err)
	r1ci.Delivered.Last, r1ci.AckFloor.Last = nil, nil

	// Now scale stream to R3.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Now check each.
	c.waitOnConsumerLeader(globalAccountName, "TEST", "dur")
	ci, err = js.ConsumerInfo("TEST", "dur")
	require_NoError(t, err)
	require_Equal(t, ci.Config.Replicas, 0)
	require_Equal(t, len(ci.Cluster.Replicas), 2)

	c.waitOnConsumerLeader(globalAccountName, "TEST", eName)
	ci, err = js.ConsumerInfo("TEST", eName)
	require_NoError(t, err)
	require_Equal(t, ci.Config.Replicas, 0)
	require_Equal(t, len(ci.Cluster.Replicas), 0)

	c.waitOnConsumerLeader(globalAccountName, "TEST", "r1")
	ci, err = js.ConsumerInfo("TEST", "r1")
	require_NoError(t, err)
	require_Equal(t, ci.Config.Replicas, 1)
	require_Equal(t, len(ci.Cluster.Replicas), 0)
	// Now check that state transferred correctly.
	ci.Delivered.Last, ci.AckFloor.Last = nil, nil
	if ci.Delivered != r1ci.Delivered {
		t.Fatalf("Delivered state for R1 incorrect, wanted %+v got %+v",
			r1ci.Delivered, ci.Delivered)
	}
	if ci.AckFloor != r1ci.AckFloor {
		t.Fatalf("AckFloor state for R1 incorrect, wanted %+v got %+v",
			r1ci.AckFloor, ci.AckFloor)
	}

	c.waitOnConsumerLeader(globalAccountName, "TEST", "r3")
	ci, err = js.ConsumerInfo("TEST", "r3")
	require_NoError(t, err)
	require_Equal(t, ci.Config.Replicas, 3)
	require_Equal(t, len(ci.Cluster.Replicas), 2)
}

func TestJetStreamClusterDesyncAfterErrorDuringCatchup(t *testing.T) {
	tests := []struct {
		title            string
		onErrorCondition func(server *Server, mset *stream)
	}{
		{
			title: "TooManyRetries",
			onErrorCondition: func(server *Server, mset *stream) {
				// Too many retries while processing snapshot is considered a cluster reset.
				// If a leader is temporarily unavailable we shouldn't blow away our state.
				require_True(t, isClusterResetErr(errCatchupTooManyRetries))
				mset.resetClusteredState(errCatchupTooManyRetries)
			},
		},
		{
			title: "AbortedNoLeader",
			onErrorCondition: func(server *Server, mset *stream) {
				for _, n := range server.raftNodes {
					rn := n.(*raft)
					if rn.accName == "$G" {
						rn.Lock()
						rn.updateLeader(noLeader)
						rn.Unlock()
					}
				}

				// Processing a snapshot while there's no leader elected is considered a cluster reset.
				// If a leader is temporarily unavailable we shouldn't blow away our state.
				var snap StreamReplicatedState
				snap.LastSeq = 1_000 // ensure we can catchup based on the snapshot
				err := mset.processSnapshot(&snap)
				require_True(t, errors.Is(err, errCatchupAbortedNoLeader))
				require_True(t, isClusterResetErr(err))
				mset.resetClusteredState(err)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			si, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Replicas: 3,
			})
			require_NoError(t, err)

			streamLeader := si.Cluster.Leader
			streamLeaderServer := c.serverByName(streamLeader)
			nc.Close()
			nc, js = jsClientConnect(t, streamLeaderServer)
			defer nc.Close()

			servers := slices.DeleteFunc([]string{"S-1", "S-2", "S-3"}, func(s string) bool {
				return s == streamLeader
			})

			// Publish 10 messages.
			for i := 0; i < 10; i++ {
				pubAck, err := js.Publish("foo", []byte("ok"))
				require_NoError(t, err)
				require_Equal(t, pubAck.Sequence, uint64(i+1))
			}

			outdatedServerName := servers[0]
			clusterResetServerName := servers[1]

			outdatedServer := c.serverByName(outdatedServerName)
			outdatedServer.Shutdown()
			outdatedServer.WaitForShutdown()

			// Publish 10 more messages, one server will be behind.
			for i := 0; i < 10; i++ {
				pubAck, err := js.Publish("foo", []byte("ok"))
				require_NoError(t, err)
				require_Equal(t, pubAck.Sequence, uint64(i+11))
			}

			// We will not need the client anymore.
			nc.Close()

			// Shutdown stream leader so one server remains.
			streamLeaderServer.Shutdown()
			streamLeaderServer.WaitForShutdown()

			clusterResetServer := c.serverByName(clusterResetServerName)
			acc, err := clusterResetServer.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)

			// Run error condition.
			test.onErrorCondition(clusterResetServer, mset)

			// Stream leader stays offline, we only start the server with missing stream data.
			// We expect that the reset server must not allow the outdated server to become leader, as that would result in desync.
			c.restartServer(outdatedServer)
			c.waitOnStreamLeader(globalAccountName, "TEST")

			// Outdated server must NOT become the leader.
			newStreamLeaderServer := c.streamLeader(globalAccountName, "TEST")
			require_Equal(t, newStreamLeaderServer.Name(), clusterResetServerName)
		})
	}
}

func TestJetStreamClusterDontInstallSnapshotWhenStoppingStream(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.WorkQueuePolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	// Wait for all servers to have applied everything.
	var maxApplied uint64
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		maxApplied = 0
		for _, s := range c.servers {
			acc, err := s.lookupAccount(globalAccountName)
			if err != nil {
				return err
			}
			mset, err := acc.lookupStream("TEST")
			if err != nil {
				return err
			}
			_, _, applied := mset.node.Progress()
			if maxApplied == 0 {
				maxApplied = applied
			} else if applied < maxApplied {
				return fmt.Errorf("applied not high enough, expected %d, got %d", applied, maxApplied)
			} else if applied > maxApplied {
				return fmt.Errorf("applied higher on one server, expected %d, got %d", applied, maxApplied)
			}
		}
		return nil
	})

	// Install a snapshot on a follower.
	s := c.randomNonStreamLeader(globalAccountName, "TEST")
	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	err = mset.node.InstallSnapshot(mset.stateSnapshotLocked())
	require_NoError(t, err)

	// Validate the snapshot reflects applied.
	validateStreamState := func(snap *snapshot) {
		t.Helper()
		require_Equal(t, snap.lastIndex, maxApplied)
		ss, err := DecodeStreamState(snap.data)
		require_NoError(t, err)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 1)
	}
	snap, err := mset.node.(*raft).loadLastSnapshot()
	require_NoError(t, err)
	validateStreamState(snap)

	// Simulate a message being stored, but not calling Applied yet.
	err = mset.processJetStreamMsg("foo", _EMPTY_, nil, nil, 1, time.Now().UnixNano())
	require_NoError(t, err)

	// Simulate the stream being stopped before we're able to call Applied.
	// If we'd install a snapshot during this, which would be a race condition,
	// we'd store a snapshot with state that's ahead of applied.
	err = mset.stop(false, false)
	require_NoError(t, err)

	// Validate the snapshot is the same as before.
	snap, err = mset.node.(*raft).loadLastSnapshot()
	require_NoError(t, err)
	validateStreamState(snap)
}

func TestJetStreamClusterDontInstallSnapshotWhenStoppingConsumer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.WorkQueuePolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		Replicas:  3,
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// Add a message and let the consumer ack it, this moves the consumer's RAFT applied up.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	sub, err := js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)
	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	err = msgs[0].AckSync()
	require_NoError(t, err)

	// Wait for all servers to have applied everything.
	var maxApplied uint64
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		maxApplied = 0
		for _, s := range c.servers {
			acc, err := s.lookupAccount(globalAccountName)
			if err != nil {
				return err
			}
			mset, err := acc.lookupStream("TEST")
			if err != nil {
				return err
			}
			o := mset.lookupConsumer("CONSUMER")
			if o == nil {
				return errors.New("consumer not found")
			}
			_, _, applied := o.node.Progress()
			if maxApplied == 0 {
				maxApplied = applied
			} else if applied < maxApplied {
				return fmt.Errorf("applied not high enough, expected %d, got %d", applied, maxApplied)
			} else if applied > maxApplied {
				return fmt.Errorf("applied higher on one server, expected %d, got %d", applied, maxApplied)
			}
		}
		return nil
	})

	// Install a snapshot on a follower.
	s := c.randomNonStreamLeader(globalAccountName, "TEST")
	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)
	snapBytes, err := o.store.EncodedState()
	require_NoError(t, err)
	err = o.node.InstallSnapshot(snapBytes)
	require_NoError(t, err)

	// Validate the snapshot reflects applied.
	validateStreamState := func(snap *snapshot) {
		t.Helper()
		require_Equal(t, snap.lastIndex, maxApplied)
		state, err := decodeConsumerState(snap.data)
		require_NoError(t, err)
		require_Equal(t, state.Delivered.Consumer, 1)
		require_Equal(t, state.Delivered.Stream, 1)
	}
	snap, err := o.node.(*raft).loadLastSnapshot()
	require_NoError(t, err)
	validateStreamState(snap)

	// Simulate a message being delivered, but not calling Applied yet.
	err = o.store.UpdateDelivered(2, 2, 1, time.Now().UnixNano())
	require_NoError(t, err)

	// Simulate the consumer being stopped before we're able to call Applied.
	// If we'd install a snapshot during this, which would be a race condition,
	// we'd store a snapshot with state that's ahead of applied.
	err = o.stop()
	require_NoError(t, err)

	// Validate the snapshot is the same as before.
	snap, err = o.node.(*raft).loadLastSnapshot()
	require_NoError(t, err)
	validateStreamState(snap)
}

func TestJetStreamClusterDesyncAfterRestartReplacesLeaderSnapshot(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Reconnect to the leader.
	leader := c.streamLeader(globalAccountName, "TEST")
	nc.Close()
	nc, js = jsClientConnect(t, leader)
	defer nc.Close()

	lookupStream := func(s *Server) *stream {
		t.Helper()
		acc, err := s.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		return mset
	}

	// Stop one follower so it lags behind.
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	mset := lookupStream(rs)
	n := mset.node.(*raft)
	followerSnapshots := path.Join(n.sd, snapshotsDir)
	rs.Shutdown()
	rs.WaitForShutdown()

	// Move the stream forward so the follower requires a snapshot.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 10})
	require_NoError(t, err)
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	// Install a snapshot on the leader, ensuring RAFT entries are compacted and a snapshot remains.
	mset = lookupStream(leader)
	n = mset.node.(*raft)
	err = n.InstallSnapshot(mset.stateSnapshot())
	require_NoError(t, err)

	c.stopAll()

	// Replace follower snapshot with the leader's.
	// This simulates the follower coming online, getting a snapshot from the leader after which it goes offline.
	leaderSnapshots := path.Join(n.sd, snapshotsDir)
	err = os.RemoveAll(followerSnapshots)
	require_NoError(t, err)
	err = copyDir(t, followerSnapshots, leaderSnapshots)
	require_NoError(t, err)

	// Start the follower, it will load the snapshot from the leader.
	rs = c.restartServer(rs)

	// Shutting down must check that the leader's snapshot is not overwritten.
	rs.Shutdown()
	rs.WaitForShutdown()

	// Now start all servers back up.
	c.restartAll()
	c.waitOnAllCurrent()

	checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})
}

func TestJetStreamClusterKeepRaftStateIfStreamCreationFailedDuringShutdown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	nc.Close()

	// Capture RAFT storage directory and JetStream handle before shutdown.
	s := c.randomNonStreamLeader(globalAccountName, "TEST")
	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	sd := mset.node.(*raft).sd
	jss := s.getJetStream()

	// Shutdown the server.
	// Normally there are no actions taken anymore after shutdown completes,
	// but still do so to simulate actions taken while shutdown is in progress.
	s.Shutdown()
	s.WaitForShutdown()

	// Check RAFT state is kept.
	files, err := os.ReadDir(sd)
	require_NoError(t, err)
	require_True(t, len(files) > 0)

	// Simulate server shutting down, JetStream being disabled and a stream being created.
	sa := &streamAssignment{
		Config: &StreamConfig{Name: "TEST"},
		Group:  &raftGroup{node: &raft{}},
	}
	jss.processClusterCreateStream(acc, sa)

	// Check RAFT state is not deleted due to failing stream creation.
	files, err = os.ReadDir(sd)
	require_NoError(t, err)
	require_True(t, len(files) > 0)
}

func TestJetStreamClusterMetaSnapshotMustNotIncludePendingConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	require_NoError(t, err)

	// We're creating an R3 consumer, just so we can copy its state and turn it into pending below.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Name: "consumer", Replicas: 3})
	require_NoError(t, err)
	nc.Close()

	// Bypass normal API so we can simulate having a consumer pending to be created.
	// A snapshot should never create pending consumers, as that would result
	// in ghost consumers if the meta proposal failed.
	ml := c.leader()
	mjs := ml.getJetStream()
	cc := mjs.cluster
	consumers := cc.streams[globalAccountName]["TEST"].consumers
	sampleCa := *consumers["consumer"]
	sampleCa.Name, sampleCa.pending = "pending-consumer", true
	consumers[sampleCa.Name] = &sampleCa

	// Create snapshot, this should not contain pending consumers.
	snap, err := mjs.metaSnapshot()
	require_NoError(t, err)

	ru := &recoveryUpdates{
		removeStreams:   make(map[string]*streamAssignment),
		removeConsumers: make(map[string]map[string]*consumerAssignment),
		addStreams:      make(map[string]*streamAssignment),
		updateStreams:   make(map[string]*streamAssignment),
		updateConsumers: make(map[string]map[string]*consumerAssignment),
	}
	err = mjs.applyMetaSnapshot(snap, ru, true)
	require_NoError(t, err)
	require_Len(t, len(ru.updateStreams), 1)
	for _, sa := range ru.updateStreams {
		for _, ca := range sa.consumers {
			require_NotEqual(t, ca.Name, "pending-consumer")
		}
	}
	for _, cas := range ru.updateConsumers {
		for _, ca := range cas {
			require_NotEqual(t, ca.Name, "pending-consumer")
		}
	}
}

func TestJetStreamClusterDesyncAfterPublishToLeaderWithoutQuorum(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()
	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	streamLeader := si.Cluster.Leader
	streamLeaderServer := c.serverByName(streamLeader)
	nc.Close()
	nc, js = jsClientConnect(t, streamLeaderServer)
	defer nc.Close()
	servers := slices.DeleteFunc([]string{"S-1", "S-2", "S-3"}, func(s string) bool {
		return s == streamLeader
	})
	// Stop followers so further publishes will not have quorum.
	followerName1 := servers[0]
	followerName2 := servers[1]
	followerServer1 := c.serverByName(followerName1)
	followerServer2 := c.serverByName(followerName2)
	followerServer1.Shutdown()
	followerServer2.Shutdown()
	followerServer1.WaitForShutdown()
	followerServer2.WaitForShutdown()
	// Although this request will time out, it will be added to the stream leader's WAL.
	_, err = js.Publish("foo", []byte("first"), nats.AckWait(time.Second))
	require_NotNil(t, err)
	require_Equal(t, err, nats.ErrTimeout)
	// Now shut down the leader as well.
	nc.Close()
	streamLeaderServer.Shutdown()
	streamLeaderServer.WaitForShutdown()
	// Only restart the (previous) followers.
	followerServer1 = c.restartServer(followerServer1)
	c.restartServer(followerServer2)
	c.waitOnStreamLeader(globalAccountName, "TEST")
	nc, js = jsClientConnect(t, followerServer1)
	defer nc.Close()
	// Publishing a message will now have quorum.
	pubAck, err := js.Publish("foo", []byte("first, this is a retry"))
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)
	// Bring up the previous stream leader.
	c.restartServer(streamLeaderServer)
	c.waitOnAllCurrent()
	c.waitOnStreamLeader(globalAccountName, "TEST")
	// Check all servers ended up with the last published message, which had quorum.
	for _, s := range c.servers {
		c.waitOnStreamCurrent(s, globalAccountName, "TEST")
		acc, err := s.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		state := mset.state()
		require_Equal(t, state.Msgs, 1)
		require_Equal(t, state.Bytes, 55)
	}
}

func TestJetStreamClusterPreserveWALDuringCatchupWithMatchingTerm(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>"},
		Replicas: 3,
	})
	nc.Close()
	require_NoError(t, err)
	// Pick one server that will only store a part of the messages in its WAL.
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	ts := time.Now().UnixNano()
	// Manually add 3 append entries to each node's WAL, except for one node who is one behind.
	var scratch [1024]byte
	for _, s := range c.servers {
		for _, n := range s.raftNodes {
			rn := n.(*raft)
			if rn.accName == globalAccountName {
				for i := uint64(0); i < 3; i++ {
					// One server will be one behind and need to catchup.
					if s.Name() == rs.Name() && i >= 2 {
						break
					}
					esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, i, ts, true)
					entries := []*Entry{newEntry(EntryNormal, esm)}
					rn.Lock()
					ae := rn.buildAppendEntry(entries)
					ae.buf, err = ae.encode(scratch[:])
					require_NoError(t, err)
					err = rn.storeToWAL(ae)
					rn.Unlock()
					require_NoError(t, err)
				}
			}
		}
	}
	// Restart all.
	c.stopAll()
	c.restartAll()
	c.waitOnAllCurrent()
	c.waitOnStreamLeader(globalAccountName, "TEST")
	rs = c.serverByName(rs.Name())
	// Check all servers ended up with all published messages, which had quorum.
	for _, s := range c.servers {
		c.waitOnStreamCurrent(s, globalAccountName, "TEST")
		acc, err := s.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		state := mset.state()
		require_Equal(t, state.Msgs, 3)
		require_Equal(t, state.Bytes, 99)
	}
	// Check that the first two published messages came from our WAL, and
	// the last came from a catchup by another leader.
	for _, n := range rs.raftNodes {
		rn := n.(*raft)
		if rn.accName == globalAccountName {
			ae, err := rn.loadEntry(2)
			require_NoError(t, err)
			require_True(t, ae.leader == rn.ID())
			ae, err = rn.loadEntry(3)
			require_NoError(t, err)
			require_True(t, ae.leader == rn.ID())
			ae, err = rn.loadEntry(4)
			require_NoError(t, err)
			require_True(t, ae.leader != rn.ID())
		}
	}
}

func TestJetStreamClusterHardKillAfterStreamAdd(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	copyDir := func(dst, src string) error {
		srcFS := os.DirFS(src)
		return fs.WalkDir(srcFS, ".", func(p string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			newPath := path.Join(dst, p)
			if d.IsDir() {
				return os.MkdirAll(newPath, defaultDirPerms)
			}
			r, err := srcFS.Open(p)
			if err != nil {
				return err
			}
			defer r.Close()

			w, err := os.OpenFile(newPath, os.O_CREATE|os.O_WRONLY, defaultFilePerms)
			if err != nil {
				return err
			}
			defer w.Close()
			_, err = io.Copy(w, r)
			return err
		})
	}

	// Simulate being hard killed by:
	// 1. copy directories before shutdown
	copyToSrcMap := make(map[string]string)
	for _, s := range c.servers {
		sd := s.StoreDir()
		copySd := path.Join(t.TempDir(), JetStreamStoreDir)
		err = copyDir(copySd, sd)
		require_NoError(t, err)
		copyToSrcMap[copySd] = sd
	}

	// 2. stop all
	nc.Close()
	c.stopAll()

	// 3. revert directories to before shutdown
	for cp, dest := range copyToSrcMap {
		err = os.RemoveAll(dest)
		require_NoError(t, err)
		err = copyDir(dest, cp)
		require_NoError(t, err)
	}

	// 4. restart
	c.restartAll()
	c.waitOnAllCurrent()

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Stream should exist still and not be removed after hard killing all servers, so expect no error.
	_, err = js.StreamInfo("TEST")
	require_NoError(t, err)
}

func TestJetStreamClusterStreamConsumerStateResetAfterRecreate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()
	stream := "test:0"
	config := &nats.StreamConfig{
		Name:       stream,
		Subjects:   []string{"test.0.*"},
		Replicas:   3,
		Retention:  nats.WorkQueuePolicy,
		MaxMsgs:    100_000,
		Discard:    nats.DiscardNew,
		Duplicates: 5 * time.Second,
		Storage:    nats.MemoryStorage,
	}
	consumer := "A:0:0"
	subject := "test.0.0"
	var (
		duration        = 30 * time.Minute
		producerMsgs    = 200_000
		producerMsgSize = 1024
		payload         = []byte(strings.Repeat("A", producerMsgSize))
		wg              sync.WaitGroup
		n               atomic.Uint64
		canPublish      atomic.Bool
	)
	createStream := func(t *testing.T) {
		t.Helper()
		_, err := js.AddStream(config)
		require_NoError(t, err)
		consumer := &nats.ConsumerConfig{
			Durable:       consumer,
			Replicas:      3,
			MaxAckPending: 100_000,
			MaxWaiting:    100_000,
			FilterSubject: subject,
			AckPolicy:     nats.AckExplicitPolicy,
		}
		_, err = js.AddConsumer(stream, consumer)
		require_NoError(t, err)
	}
	deleteStream := func(t *testing.T) {
		err := js.DeleteStream(stream)
		require_NoError(t, err)
	}
	stopPublishing := func() {
		canPublish.Store(false)
	}
	resumePublishing := func() {
		canPublish.Store(true)
	}
	// Setup stream
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	createStream(t)
	// Setup producer
	resumePublishing()
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
			if !canPublish.Load() {
				continue
			}
			_, err := js.Publish("test.0.0", payload, nats.AckWait(200*time.Millisecond))
			if err == nil {
				if nn := n.Add(1); int(nn) >= producerMsgs {
					return
				}
			}
		}
	}()
	// Setup consumer
	acked := make(chan struct{}, 100)
	wg.Add(1)
	go func() {
		defer wg.Done()
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()
	Attempts:
		for attempts := 0; attempts < 10; attempts++ {
			_, err := js.ConsumerInfo(stream, consumer)
			if err != nil {
				t.Logf("WRN: Failed creating pull subscriber: %v - %v - %v - %v",
					subject, stream, consumer, err)
				time.Sleep(200 * time.Millisecond)
				continue
			}
			break Attempts
		}
		sub, err := js.PullSubscribe(subject, "", nats.Bind(stream, consumer))
		if err != nil {
			t.Logf("WRN: Failed creating pull subscriber: %v - %v - %v - %v",
				subject, stream, consumer, err)
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
				time.AfterFunc(3*time.Second, func() {
					select {
					case <-ctx.Done():
						return
					default:
					}
					msg.Ack()
					acked <- struct{}{}
				})
			}
			msgs, err = sub.Fetch(10, nats.MaxWait(200*time.Millisecond))
			if err != nil {
				continue
			}
			for _, msg := range msgs {
				msg.Ack()
			}
		}
	}()
	// Let publish and consume to happen for a bit.
	time.Sleep(2 * time.Second)
	// Recreate the stream
	deleteStream(t)
	stopPublishing()
	createStream(t)
	for i := 0; i < 3; i++ {
		js.Publish("test.0.0", payload, nats.AckWait(200*time.Millisecond))
	}
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for ack")
	case <-acked:
		time.Sleep(2 * time.Second)
	}
	sinfo, err := js.StreamInfo(stream)
	require_NoError(t, err)
	cinfo, err := js.ConsumerInfo(stream, consumer)
	require_NoError(t, err)
	cancel()
	if cinfo.Delivered.Stream > sinfo.State.LastSeq {
		t.Fatalf("Consumer Stream sequence is ahead of Stream LastSeq: consumer=%d, stream=%d", cinfo.Delivered.Stream, sinfo.State.LastSeq)
	}
	wg.Wait()
}

func TestJetStreamClusterOnlyPublishAdvisoriesWhenInterest(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	subj := "$JS.ADVISORY.TEST"
	s1 := c.servers[0]
	s2 := c.servers[1]

	// On the first server, see if we think the advisory will be published.
	require_False(t, s1.publishAdvisory(s1.GlobalAccount(), subj, "test"))

	// On the second server, subscribe to the advisory subject.
	nc, _ := jsClientConnect(t, s2)
	defer nc.Close()

	_, err := nc.Subscribe(subj, func(_ *nats.Msg) {})
	require_NoError(t, err)

	// Wait for the interest to propagate to the first server.
	checkFor(t, time.Second, 25*time.Millisecond, func() error {
		if !s1.GlobalAccount().sl.HasInterest(subj) {
			return fmt.Errorf("expected interest in %q, not yet found", subj)
		}
		return nil
	})

	// On the first server, try and publish the advisory again. THis time
	// it should succeed.
	require_True(t, s1.publishAdvisory(s1.GlobalAccount(), subj, "test"))
}

func TestJetStreamClusterRoutedAPIRecoverPerformance(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomNonLeader())
	defer nc.Close()

	// We only run 16 JetStream API workers.
	mp := runtime.GOMAXPROCS(0)
	if mp > 16 {
		mp = 16
	}

	leader := c.leader()
	ljs := leader.js.Load()

	// Take the JS lock, which allows the JS API queue to build up.
	ljs.mu.Lock()
	defer ljs.mu.Unlock()

	count := JSDefaultRequestQueueLimit - 1
	ch := make(chan *nats.Msg, count)

	inbox := nc.NewRespInbox()
	_, err := nc.ChanSubscribe(inbox, ch)
	require_NoError(t, err)

	// To ensure a fair starting line, we need to submit as many tasks as
	// there are JS workers whilst holding the JS lock. This will ensure that
	// each JS API worker is properly wedged.
	msg := &nats.Msg{
		Subject: fmt.Sprintf(JSApiConsumerInfoT, "Doesnt", "Exist"),
		Reply:   "no_one_here",
	}
	for i := 0; i < mp; i++ {
		require_NoError(t, nc.PublishMsg(msg))
	}

	// Then we want to submit a fixed number of tasks, big enough to fill
	// the queue, so that we can measure them.
	msg = &nats.Msg{
		Subject: fmt.Sprintf(JSApiConsumerInfoT, "Doesnt", "Exist"),
		Reply:   inbox,
	}
	for i := 0; i < count; i++ {
		require_NoError(t, nc.PublishMsg(msg))
	}
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if queued := leader.jsAPIRoutedReqs.len(); queued != count {
			return fmt.Errorf("expected %d queued requests, got %d", count, queued)
		}
		return nil
	})

	// Now we're going to release the lock and start timing. The workers
	// will now race to clear the queues and we'll wait to see how long
	// it takes for them all to respond.
	start := time.Now()
	ljs.mu.Unlock()
	for i := 0; i < count; i++ {
		<-ch
	}
	ljs.mu.Lock()
	t.Logf("Took %s to clear %d items", time.Since(start), count)
}
