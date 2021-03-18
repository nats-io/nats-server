// Copyright 2018-2021 The NATS Authors
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

// +build !race

package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// IMPORTANT: Tests in this file are not executed when running with the -race flag.
//            The test name should be prefixed with TestNoRace so we can run only
//            those tests: go test -run=TestNoRace ...

func TestNoRaceAvoidSlowConsumerBigMessages(t *testing.T) {
	opts := DefaultOptions() // Use defaults to make sure they avoid pending slow consumer.
	opts.NoSystemAccount = true
	s := RunServer(opts)
	defer s.Shutdown()

	nc1, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc1.Close()

	nc2, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	data := make([]byte, 1024*1024) // 1MB payload
	rand.Read(data)

	expected := int32(500)
	received := int32(0)

	done := make(chan bool)

	// Create Subscription.
	nc1.Subscribe("slow.consumer", func(m *nats.Msg) {
		// Just eat it so that we are not measuring
		// code time, just delivery.
		atomic.AddInt32(&received, 1)
		if received >= expected {
			done <- true
		}
	})

	// Create Error handler
	nc1.SetErrorHandler(func(c *nats.Conn, s *nats.Subscription, err error) {
		t.Fatalf("Received an error on the subscription's connection: %v\n", err)
	})

	nc1.Flush()

	for i := 0; i < int(expected); i++ {
		nc2.Publish("slow.consumer", data)
	}
	nc2.Flush()

	select {
	case <-done:
		return
	case <-time.After(10 * time.Second):
		r := atomic.LoadInt32(&received)
		if s.NumSlowConsumers() > 0 {
			t.Fatalf("Did not receive all large messages due to slow consumer status: %d of %d", r, expected)
		}
		t.Fatalf("Failed to receive all large messages: %d of %d\n", r, expected)
	}
}

func TestNoRaceRoutedQueueAutoUnsubscribe(t *testing.T) {
	optsA, _ := ProcessConfigFile("./configs/seed.conf")
	optsA.NoSigs, optsA.NoLog = true, true
	optsA.NoSystemAccount = true
	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	srvARouteURL := fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host, srvA.ClusterAddr().Port)
	optsB := nextServerOpts(optsA)
	optsB.Routes = RoutesFromStr(srvARouteURL)

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	// Wait for these 2 to connect to each other
	checkClusterFormed(t, srvA, srvB)

	// Have a client connection to each server
	ncA, err := nats.Connect(fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncA.Close()

	ncB, err := nats.Connect(fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncB.Close()

	rbar := int32(0)
	barCb := func(m *nats.Msg) {
		atomic.AddInt32(&rbar, 1)
	}
	rbaz := int32(0)
	bazCb := func(m *nats.Msg) {
		atomic.AddInt32(&rbaz, 1)
	}

	// Create 125 queue subs with auto-unsubscribe to each server for
	// group bar and group baz. So 250 total per queue group.
	cons := []*nats.Conn{ncA, ncB}
	for _, c := range cons {
		for i := 0; i < 100; i++ {
			qsub, err := c.QueueSubscribe("foo", "bar", barCb)
			if err != nil {
				t.Fatalf("Error on subscribe: %v", err)
			}
			if err := qsub.AutoUnsubscribe(1); err != nil {
				t.Fatalf("Error on auto-unsubscribe: %v", err)
			}
			qsub, err = c.QueueSubscribe("foo", "baz", bazCb)
			if err != nil {
				t.Fatalf("Error on subscribe: %v", err)
			}
			if err := qsub.AutoUnsubscribe(1); err != nil {
				t.Fatalf("Error on auto-unsubscribe: %v", err)
			}
		}
		c.Subscribe("TEST.COMPLETE", func(m *nats.Msg) {})
	}

	// We coelasce now so for each server we will have all local (200) plus
	// two from the remote side for each queue group. We also create one more
	// and will wait til each server has 204 subscriptions, that will make sure
	// that we have everything setup.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		subsA := srvA.NumSubscriptions()
		subsB := srvB.NumSubscriptions()
		if subsA != 204 || subsB != 204 {
			return fmt.Errorf("Not all subs processed yet: %d and %d", subsA, subsB)
		}
		return nil
	})

	expected := int32(200)
	// Now send messages from each server
	for i := int32(0); i < expected; i++ {
		c := cons[i%2]
		c.Publish("foo", []byte("Don't Drop Me!"))
	}
	for _, c := range cons {
		c.Flush()
	}

	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		nbar := atomic.LoadInt32(&rbar)
		nbaz := atomic.LoadInt32(&rbaz)
		if nbar == expected && nbaz == expected {
			return nil
		}
		return fmt.Errorf("Did not receive all %d queue messages, received %d for 'bar' and %d for 'baz'",
			expected, atomic.LoadInt32(&rbar), atomic.LoadInt32(&rbaz))
	})
}

func TestNoRaceClosedSlowConsumerWriteDeadline(t *testing.T) {
	opts := DefaultOptions()
	opts.NoSystemAccount = true
	opts.WriteDeadline = 10 * time.Millisecond // Make very small to trip.
	opts.MaxPending = 500 * 1024 * 1024        // Set high so it will not trip here.
	s := RunServer(opts)
	defer s.Shutdown()

	c, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", opts.Host, opts.Port), 3*time.Second)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("CONNECT {}\r\nPING\r\nSUB foo 1\r\n")); err != nil {
		t.Fatalf("Error sending protocols to server: %v", err)
	}
	// Reduce socket buffer to increase reliability of data backing up in the server destined
	// for our subscribed client.
	c.(*net.TCPConn).SetReadBuffer(128)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	sender, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sender.Close()

	payload := make([]byte, 1024*1024)
	for i := 0; i < 100; i++ {
		if err := sender.Publish("foo", payload); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}

	// Flush sender connection to ensure that all data has been sent.
	if err := sender.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}

	// At this point server should have closed connection c.
	checkClosedConns(t, s, 1, 2*time.Second)
	conns := s.closedClients()
	if lc := len(conns); lc != 1 {
		t.Fatalf("len(conns) expected to be %d, got %d\n", 1, lc)
	}
	checkReason(t, conns[0].Reason, SlowConsumerWriteDeadline)
}

func TestNoRaceClosedSlowConsumerPendingBytes(t *testing.T) {
	opts := DefaultOptions()
	opts.NoSystemAccount = true
	opts.WriteDeadline = 30 * time.Second // Wait for long time so write deadline does not trigger slow consumer.
	opts.MaxPending = 1 * 1024 * 1024     // Set to low value (1MB) to allow SC to trip.
	s := RunServer(opts)
	defer s.Shutdown()

	c, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", opts.Host, opts.Port), 3*time.Second)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("CONNECT {}\r\nPING\r\nSUB foo 1\r\n")); err != nil {
		t.Fatalf("Error sending protocols to server: %v", err)
	}
	// Reduce socket buffer to increase reliability of data backing up in the server destined
	// for our subscribed client.
	c.(*net.TCPConn).SetReadBuffer(128)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	sender, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sender.Close()

	payload := make([]byte, 1024*1024)
	for i := 0; i < 100; i++ {
		if err := sender.Publish("foo", payload); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}

	// Flush sender connection to ensure that all data has been sent.
	if err := sender.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}

	// At this point server should have closed connection c.
	checkClosedConns(t, s, 1, 2*time.Second)
	conns := s.closedClients()
	if lc := len(conns); lc != 1 {
		t.Fatalf("len(conns) expected to be %d, got %d\n", 1, lc)
	}
	checkReason(t, conns[0].Reason, SlowConsumerPendingBytes)
}

func TestNoRaceSlowConsumerPendingBytes(t *testing.T) {
	opts := DefaultOptions()
	opts.NoSystemAccount = true
	opts.WriteDeadline = 30 * time.Second // Wait for long time so write deadline does not trigger slow consumer.
	opts.MaxPending = 1 * 1024 * 1024     // Set to low value (1MB) to allow SC to trip.
	s := RunServer(opts)
	defer s.Shutdown()

	c, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", opts.Host, opts.Port), 3*time.Second)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("CONNECT {}\r\nPING\r\nSUB foo 1\r\n")); err != nil {
		t.Fatalf("Error sending protocols to server: %v", err)
	}
	// Reduce socket buffer to increase reliability of data backing up in the server destined
	// for our subscribed client.
	c.(*net.TCPConn).SetReadBuffer(128)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	sender, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sender.Close()

	payload := make([]byte, 1024*1024)
	for i := 0; i < 100; i++ {
		if err := sender.Publish("foo", payload); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}

	// Flush sender connection to ensure that all data has been sent.
	if err := sender.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}

	// At this point server should have closed connection c.

	// On certain platforms, it may take more than one call before
	// getting the error.
	for i := 0; i < 100; i++ {
		if _, err := c.Write([]byte("PUB bar 5\r\nhello\r\n")); err != nil {
			// ok
			return
		}
	}
	t.Fatal("Connection should have been closed")
}

func TestNoRaceGatewayNoMissingReplies(t *testing.T) {
	// This test will have following setup:
	//
	// responder1		         requestor
	//    |                          |
	//    v                          v
	//   [A1]<-------gw------------[B1]
	//    |  \                      |
	//    |   \______gw__________   | route
	//    |                     _\| |
	//   [  ]--------gw----------->[  ]
	//   [A2]<-------gw------------[B2]
	//   [  ]                      [  ]
	//    ^
	//    |
	// responder2
	//
	// There is a possible race that when the requestor creates
	// a subscription on the reply subject, the subject interest
	// being sent from the inbound gateway, and B1 having none,
	// the SUB first goes to B2 before being sent to A1 from
	// B2's inbound GW. But the request can go from B1 to A1
	// right away and the responder1 connecting to A1 may send
	// back the reply before the interest on the reply makes it
	// to A1 (from B2).
	// This test will also verify that if the responder is instead
	// connected to A2, the reply is properly received by requestor
	// on B1.

	// For this test we want to be in interestOnly mode, so
	// make it happen quickly
	gatewayMaxRUnsubBeforeSwitch = 1
	defer func() { gatewayMaxRUnsubBeforeSwitch = defaultGatewayMaxRUnsubBeforeSwitch }()

	// Start with setting up A2 and B2.
	ob2 := testDefaultOptionsForGateway("B")
	sb2 := runGatewayServer(ob2)
	defer sb2.Shutdown()

	oa2 := testGatewayOptionsFromToWithServers(t, "A", "B", sb2)
	sa2 := runGatewayServer(oa2)
	defer sa2.Shutdown()

	waitForOutboundGateways(t, sa2, 1, time.Second)
	waitForInboundGateways(t, sa2, 1, time.Second)
	waitForOutboundGateways(t, sb2, 1, time.Second)
	waitForInboundGateways(t, sb2, 1, time.Second)

	// Now start A1 which will connect to B2
	oa1 := testGatewayOptionsFromToWithServers(t, "A", "B", sb2)
	oa1.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", oa2.Cluster.Host, oa2.Cluster.Port))
	sa1 := runGatewayServer(oa1)
	defer sa1.Shutdown()

	waitForOutboundGateways(t, sa1, 1, time.Second)
	waitForInboundGateways(t, sb2, 2, time.Second)

	checkClusterFormed(t, sa1, sa2)

	// Finally, start B1 that will connect to A1.
	ob1 := testGatewayOptionsFromToWithServers(t, "B", "A", sa1)
	ob1.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", ob2.Cluster.Host, ob2.Cluster.Port))
	sb1 := runGatewayServer(ob1)
	defer sb1.Shutdown()

	// Check that we have the outbound gateway from B1 to A1
	checkFor(t, 3*time.Second, 15*time.Millisecond, func() error {
		c := sb1.getOutboundGatewayConnection("A")
		if c == nil {
			return fmt.Errorf("Outbound connection to A not created yet")
		}
		c.mu.Lock()
		name := c.opts.Name
		nc := c.nc
		c.mu.Unlock()
		if name != sa1.ID() {
			// Force a disconnect
			nc.Close()
			return fmt.Errorf("Was unable to have B1 connect to A1")
		}
		return nil
	})

	waitForInboundGateways(t, sa1, 1, time.Second)
	checkClusterFormed(t, sb1, sb2)

	a1URL := fmt.Sprintf("nats://%s:%d", oa1.Host, oa1.Port)
	a2URL := fmt.Sprintf("nats://%s:%d", oa2.Host, oa2.Port)
	b1URL := fmt.Sprintf("nats://%s:%d", ob1.Host, ob1.Port)
	b2URL := fmt.Sprintf("nats://%s:%d", ob2.Host, ob2.Port)

	ncb1 := natsConnect(t, b1URL)
	defer ncb1.Close()

	ncb2 := natsConnect(t, b2URL)
	defer ncb2.Close()

	natsSubSync(t, ncb1, "just.a.sub")
	natsSubSync(t, ncb2, "just.a.sub")
	checkExpectedSubs(t, 2, sb1, sb2)

	// For this test, we want A to be checking B's interest in order
	// to send messages (which would cause replies to be dropped if
	// there is no interest registered on A). So from A servers,
	// send to various subjects and cause B's to switch to interestOnly
	// mode.
	nca1 := natsConnect(t, a1URL)
	defer nca1.Close()
	for i := 0; i < 10; i++ {
		natsPub(t, nca1, fmt.Sprintf("reject.%d", i), []byte("hello"))
	}
	nca2 := natsConnect(t, a2URL)
	defer nca2.Close()
	for i := 0; i < 10; i++ {
		natsPub(t, nca2, fmt.Sprintf("reject.%d", i), []byte("hello"))
	}

	checkSwitchedMode := func(t *testing.T, s *Server) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			var switchedMode bool
			c := s.getOutboundGatewayConnection("B")
			ei, _ := c.gw.outsim.Load(globalAccountName)
			if ei != nil {
				e := ei.(*outsie)
				e.RLock()
				switchedMode = e.ni == nil && e.mode == InterestOnly
				e.RUnlock()
			}
			if !switchedMode {
				return fmt.Errorf("Still not switched mode")
			}
			return nil
		})
	}
	checkSwitchedMode(t, sa1)
	checkSwitchedMode(t, sa2)

	// Setup a subscriber on _INBOX.> on each of A's servers.
	total := 1000
	expected := int32(total)
	rcvOnA := int32(0)
	qrcvOnA := int32(0)
	natsSub(t, nca1, "myreply.>", func(_ *nats.Msg) {
		atomic.AddInt32(&rcvOnA, 1)
	})
	natsQueueSub(t, nca2, "myreply.>", "bar", func(_ *nats.Msg) {
		atomic.AddInt32(&qrcvOnA, 1)
	})
	checkExpectedSubs(t, 2, sa1, sa2)

	// Ok.. so now we will run the actual test where we
	// create a responder on A1 and make sure that every
	// single request from B1 gets the reply. Will repeat
	// test with responder connected to A2.
	sendReqs := func(t *testing.T, subConn *nats.Conn) {
		t.Helper()
		responder := natsSub(t, subConn, "foo", func(m *nats.Msg) {
			m.Respond([]byte("reply"))
		})
		natsFlush(t, subConn)
		checkExpectedSubs(t, 3, sa1, sa2)

		// We are not going to use Request() because this sets
		// a wildcard subscription on an INBOX and less likely
		// to produce the race. Instead we will explicitly set
		// the subscription on the reply subject and create one
		// per request.
		for i := 0; i < total/2; i++ {
			reply := fmt.Sprintf("myreply.%d", i)
			replySub := natsQueueSubSync(t, ncb1, reply, "bar")
			natsFlush(t, ncb1)

			// Let's make sure we have interest on B2.
			if r := sb2.globalAccount().sl.Match(reply); len(r.qsubs) == 0 {
				checkFor(t, time.Second, time.Millisecond, func() error {
					if r := sb2.globalAccount().sl.Match(reply); len(r.qsubs) == 0 {
						return fmt.Errorf("B still not registered interest on %s", reply)
					}
					return nil
				})
			}
			natsPubReq(t, ncb1, "foo", reply, []byte("request"))
			if _, err := replySub.NextMsg(time.Second); err != nil {
				t.Fatalf("Did not receive reply: %v", err)
			}
			natsUnsub(t, replySub)
		}

		responder.Unsubscribe()
		natsFlush(t, subConn)
		checkExpectedSubs(t, 2, sa1, sa2)
	}
	sendReqs(t, nca1)
	sendReqs(t, nca2)

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if n := atomic.LoadInt32(&rcvOnA); n != expected {
			return fmt.Errorf("Subs on A expected to get %v replies, got %v", expected, n)
		}
		return nil
	})

	// We should not have received a single message on the queue sub
	// on cluster A because messages will have been delivered to
	// the member on cluster B.
	if n := atomic.LoadInt32(&qrcvOnA); n != 0 {
		t.Fatalf("Queue sub on A should not have received message, got %v", n)
	}
}

func TestNoRaceRouteMemUsage(t *testing.T) {
	oa := DefaultOptions()
	sa := RunServer(oa)
	defer sa.Shutdown()

	ob := DefaultOptions()
	ob.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", oa.Cluster.Host, oa.Cluster.Port))
	sb := RunServer(ob)
	defer sb.Shutdown()

	checkClusterFormed(t, sa, sb)

	responder := natsConnect(t, fmt.Sprintf("nats://%s:%d", oa.Host, oa.Port))
	defer responder.Close()
	for i := 0; i < 10; i++ {
		natsSub(t, responder, "foo", func(m *nats.Msg) {
			m.Respond(m.Data)
		})
	}
	natsFlush(t, responder)

	payload := make([]byte, 50*1024)

	bURL := fmt.Sprintf("nats://%s:%d", ob.Host, ob.Port)

	// Capture mem usage
	mem := runtime.MemStats{}
	runtime.ReadMemStats(&mem)
	inUseBefore := mem.HeapInuse

	for i := 0; i < 100; i++ {
		requestor := natsConnect(t, bURL)
		inbox := nats.NewInbox()
		sub := natsSubSync(t, requestor, inbox)
		natsPubReq(t, requestor, "foo", inbox, payload)
		for j := 0; j < 10; j++ {
			natsNexMsg(t, sub, time.Second)
		}
		requestor.Close()
	}

	runtime.GC()
	debug.FreeOSMemory()
	runtime.ReadMemStats(&mem)
	inUseNow := mem.HeapInuse
	if inUseNow > 3*inUseBefore {
		t.Fatalf("Heap in-use before was %v, now %v: too high", inUseBefore, inUseNow)
	}
}

func TestNoRaceRouteCache(t *testing.T) {
	maxPerAccountCacheSize = 20
	prunePerAccountCacheSize = 5
	closedSubsCheckInterval = 250 * time.Millisecond

	defer func() {
		maxPerAccountCacheSize = defaultMaxPerAccountCacheSize
		prunePerAccountCacheSize = defaultPrunePerAccountCacheSize
		closedSubsCheckInterval = defaultClosedSubsCheckInterval
	}()

	for _, test := range []struct {
		name     string
		useQueue bool
	}{
		{"plain_sub", false},
		{"queue_sub", true},
	} {
		t.Run(test.name, func(t *testing.T) {

			oa := DefaultOptions()
			oa.NoSystemAccount = true
			sa := RunServer(oa)
			defer sa.Shutdown()

			ob := DefaultOptions()
			ob.NoSystemAccount = true
			ob.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", oa.Cluster.Host, oa.Cluster.Port))
			sb := RunServer(ob)
			defer sb.Shutdown()

			checkClusterFormed(t, sa, sb)

			responder := natsConnect(t, fmt.Sprintf("nats://%s:%d", oa.Host, oa.Port))
			defer responder.Close()
			natsSub(t, responder, "foo", func(m *nats.Msg) {
				m.Respond(m.Data)
			})
			natsFlush(t, responder)

			checkExpectedSubs(t, 1, sa)
			checkExpectedSubs(t, 1, sb)

			bURL := fmt.Sprintf("nats://%s:%d", ob.Host, ob.Port)
			requestor := natsConnect(t, bURL)
			defer requestor.Close()

			ch := make(chan struct{}, 1)
			cb := func(_ *nats.Msg) {
				select {
				case ch <- struct{}{}:
				default:
				}
			}

			sendReqs := func(t *testing.T, nc *nats.Conn, count int, unsub bool) {
				t.Helper()
				for i := 0; i < count; i++ {
					inbox := nats.NewInbox()
					var sub *nats.Subscription
					if test.useQueue {
						sub = natsQueueSub(t, nc, inbox, "queue", cb)
					} else {
						sub = natsSub(t, nc, inbox, cb)
					}
					natsPubReq(t, nc, "foo", inbox, []byte("hello"))
					select {
					case <-ch:
					case <-time.After(time.Second):
						t.Fatalf("Failed to get reply")
					}
					if unsub {
						natsUnsub(t, sub)
					}
				}
			}
			sendReqs(t, requestor, maxPerAccountCacheSize+1, true)

			var route *client
			sb.mu.Lock()
			for _, r := range sb.routes {
				route = r
				break
			}
			sb.mu.Unlock()

			checkExpected := func(t *testing.T, expected int) {
				t.Helper()
				checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
					route.mu.Lock()
					n := len(route.in.pacache)
					route.mu.Unlock()
					if n != expected {
						return fmt.Errorf("Expected %v subs in the cache, got %v", expected, n)
					}
					return nil
				})
			}
			checkExpected(t, (maxPerAccountCacheSize+1)-(prunePerAccountCacheSize+1))

			// Wait for more than the orphan check
			time.Sleep(2 * closedSubsCheckInterval)

			// Add a new subs up to point where new prune would occur
			sendReqs(t, requestor, prunePerAccountCacheSize+1, false)

			// Now closed subs should have been removed, so expected
			// subs in the cache should be the new ones.
			checkExpected(t, prunePerAccountCacheSize+1)

			// Now try wil implicit unsubscribe (due to connection close)
			sendReqs(t, requestor, maxPerAccountCacheSize+1, false)
			requestor.Close()

			checkExpected(t, maxPerAccountCacheSize-prunePerAccountCacheSize)

			// Wait for more than the orphan check
			time.Sleep(2 * closedSubsCheckInterval)

			// Now create new connection and send prunePerAccountCacheSize+1
			// and that should cause all subs from previous connection to be
			// removed from cache
			requestor = natsConnect(t, bURL)
			defer requestor.Close()

			sendReqs(t, requestor, prunePerAccountCacheSize+1, false)
			checkExpected(t, prunePerAccountCacheSize+1)
		})
	}
}

func TestNoRaceFetchAccountDoesNotRegisterAccountTwice(t *testing.T) {
	sa, oa, sb, ob, _ := runTrustedGateways(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	// Let's create a user account.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	jwt, _ := nac.Encode(okp)
	userAcc := pub

	// Replace B's account resolver with one that introduces
	// delay during the Fetch()
	sac := &slowAccResolver{AccountResolver: sb.AccountResolver()}
	sb.SetAccountResolver(sac)

	// Add the account in sa and sb
	addAccountToMemResolver(sa, userAcc, jwt)
	addAccountToMemResolver(sb, userAcc, jwt)

	// Tell the slow account resolver which account to slow down
	sac.Lock()
	sac.acc = userAcc
	sac.Unlock()

	urlA := fmt.Sprintf("nats://%s:%d", oa.Host, oa.Port)
	urlB := fmt.Sprintf("nats://%s:%d", ob.Host, ob.Port)

	nca, err := nats.Connect(urlA, createUserCreds(t, sa, akp))
	if err != nil {
		t.Fatalf("Error connecting to A: %v", err)
	}
	defer nca.Close()

	// Since there is an optimistic send, this message will go to B
	// and on processing this message, B will lookup/fetch this
	// account, which can produce race with the fetch of this
	// account from A's system account that sent a notification
	// about this account, or with the client connect just after
	// that.
	nca.Publish("foo", []byte("hello"))

	// Now connect and create a subscription on B
	ncb, err := nats.Connect(urlB, createUserCreds(t, sb, akp))
	if err != nil {
		t.Fatalf("Error connecting to A: %v", err)
	}
	defer ncb.Close()
	sub, err := ncb.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	ncb.Flush()

	// Now send messages from A and B should ultimately start to receive
	// them (once the subscription has been correctly registered)
	ok := false
	for i := 0; i < 10; i++ {
		nca.Publish("foo", []byte("hello"))
		if _, err := sub.NextMsg(100 * time.Millisecond); err != nil {
			continue
		}
		ok = true
		break
	}
	if !ok {
		t.Fatalf("B should be able to receive messages")
	}

	checkTmpAccounts := func(t *testing.T, s *Server) {
		t.Helper()
		empty := true
		s.tmpAccounts.Range(func(_, _ interface{}) bool {
			empty = false
			return false
		})
		if !empty {
			t.Fatalf("tmpAccounts is not empty")
		}
	}
	checkTmpAccounts(t, sa)
	checkTmpAccounts(t, sb)
}

func TestNoRaceWriteDeadline(t *testing.T) {
	opts := DefaultOptions()
	opts.NoSystemAccount = true
	opts.WriteDeadline = 30 * time.Millisecond
	s := RunServer(opts)
	defer s.Shutdown()

	c, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", opts.Host, opts.Port), 3*time.Second)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("CONNECT {}\r\nPING\r\nSUB foo 1\r\n")); err != nil {
		t.Fatalf("Error sending protocols to server: %v", err)
	}
	// Reduce socket buffer to increase reliability of getting
	// write deadline errors.
	c.(*net.TCPConn).SetReadBuffer(4)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	sender, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sender.Close()

	payload := make([]byte, 1000000)
	total := 1000
	for i := 0; i < total; i++ {
		if err := sender.Publish("foo", payload); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}
	// Flush sender connection to ensure that all data has been sent.
	if err := sender.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}

	// At this point server should have closed connection c.

	// On certain platforms, it may take more than one call before
	// getting the error.
	for i := 0; i < 100; i++ {
		if _, err := c.Write([]byte("PUB bar 5\r\nhello\r\n")); err != nil {
			// ok
			return
		}
	}
	t.Fatal("Connection should have been closed")
}

func TestNoRaceLeafNodeClusterNameConflictDeadlock(t *testing.T) {
	o := DefaultOptions()
	o.LeafNode.Port = -1
	s := RunServer(o)
	defer s.Shutdown()

	u, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", o.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}

	o1 := DefaultOptions()
	o1.ServerName = "A1"
	o1.Cluster.Name = "clusterA"
	o1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	s1 := RunServer(o1)
	defer s1.Shutdown()

	checkLeafNodeConnected(t, s1)

	o2 := DefaultOptions()
	o2.ServerName = "A2"
	o2.Cluster.Name = "clusterA"
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	o2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkLeafNodeConnected(t, s2)
	checkClusterFormed(t, s1, s2)

	o3 := DefaultOptions()
	o3.ServerName = "A3"
	o3.Cluster.Name = "" // intentionally not set
	o3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	o3.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	s3 := RunServer(o3)
	defer s3.Shutdown()

	checkLeafNodeConnected(t, s3)
	checkClusterFormed(t, s1, s2, s3)
}

// This test is same than TestAccountAddServiceImportRace but running
// without the -race flag, it would capture more easily the possible
// duplicate sid, resulting in less than expected number of subscriptions
// in the account's internal subscriptions map.
func TestNoRaceAccountAddServiceImportRace(t *testing.T) {
	TestAccountAddServiceImportRace(t)
}

// Similar to the routed version. Make sure we receive all of the
// messages with auto-unsubscribe enabled.
func TestNoRaceQueueAutoUnsubscribe(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	rbar := int32(0)
	barCb := func(m *nats.Msg) {
		atomic.AddInt32(&rbar, 1)
	}
	rbaz := int32(0)
	bazCb := func(m *nats.Msg) {
		atomic.AddInt32(&rbaz, 1)
	}

	// Create 1000 subscriptions with auto-unsubscribe of 1.
	// Do two groups, one bar and one baz.
	total := 1000
	for i := 0; i < total; i++ {
		qsub, err := nc.QueueSubscribe("foo", "bar", barCb)
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		if err := qsub.AutoUnsubscribe(1); err != nil {
			t.Fatalf("Error on auto-unsubscribe: %v", err)
		}
		qsub, err = nc.QueueSubscribe("foo", "baz", bazCb)
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		if err := qsub.AutoUnsubscribe(1); err != nil {
			t.Fatalf("Error on auto-unsubscribe: %v", err)
		}
	}
	nc.Flush()

	expected := int32(total)
	for i := int32(0); i < expected; i++ {
		nc.Publish("foo", []byte("Don't Drop Me!"))
	}
	nc.Flush()

	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		nbar := atomic.LoadInt32(&rbar)
		nbaz := atomic.LoadInt32(&rbaz)
		if nbar == expected && nbaz == expected {
			return nil
		}
		return fmt.Errorf("Did not receive all %d queue messages, received %d for 'bar' and %d for 'baz'",
			expected, atomic.LoadInt32(&rbar), atomic.LoadInt32(&rbaz))
	})
}

func TestNoRaceAcceptLoopsDoNotLeaveOpenedConn(t *testing.T) {
	for _, test := range []struct {
		name string
		url  func(o *Options) (string, int)
	}{
		{"client", func(o *Options) (string, int) { return o.Host, o.Port }},
		{"route", func(o *Options) (string, int) { return o.Cluster.Host, o.Cluster.Port }},
		{"gateway", func(o *Options) (string, int) { return o.Gateway.Host, o.Gateway.Port }},
		{"leafnode", func(o *Options) (string, int) { return o.LeafNode.Host, o.LeafNode.Port }},
		{"websocket", func(o *Options) (string, int) { return o.Websocket.Host, o.Websocket.Port }},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := DefaultOptions()
			o.DisableShortFirstPing = true
			o.Accounts = []*Account{NewAccount("$SYS")}
			o.SystemAccount = "$SYS"
			o.Cluster.Name = "abc"
			o.Cluster.Host = "127.0.0.1"
			o.Cluster.Port = -1
			o.Gateway.Name = "abc"
			o.Gateway.Host = "127.0.0.1"
			o.Gateway.Port = -1
			o.LeafNode.Host = "127.0.0.1"
			o.LeafNode.Port = -1
			o.Websocket.Host = "127.0.0.1"
			o.Websocket.Port = -1
			o.Websocket.HandshakeTimeout = 1
			o.Websocket.NoTLS = true
			s := RunServer(o)
			defer s.Shutdown()

			host, port := test.url(o)
			url := fmt.Sprintf("%s:%d", host, port)
			var conns []net.Conn

			wg := sync.WaitGroup{}
			wg.Add(1)
			done := make(chan struct{}, 1)
			go func() {
				defer wg.Done()
				// Have an upper limit
				for i := 0; i < 200; i++ {
					c, err := net.Dial("tcp", url)
					if err != nil {
						return
					}
					conns = append(conns, c)
					select {
					case <-done:
						return
					default:
					}
				}
			}()
			time.Sleep(15 * time.Millisecond)
			s.Shutdown()
			close(done)
			wg.Wait()
			for _, c := range conns {
				c.SetReadDeadline(time.Now().Add(2 * time.Second))
				br := bufio.NewReader(c)
				// Read INFO for connections that were accepted
				_, _, err := br.ReadLine()
				if err == nil {
					// After that, the connection should be closed,
					// so we should get an error here.
					_, _, err = br.ReadLine()
				}
				// We expect an io.EOF or any other error indicating the use of a closed
				// connection, but we should not get the timeout error.
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					err = nil
				}
				if err == nil {
					var buf [10]byte
					c.SetDeadline(time.Now().Add(2 * time.Second))
					c.Write([]byte("C"))
					_, err = c.Read(buf[:])
					if ne, ok := err.(net.Error); ok && ne.Timeout() {
						err = nil
					}
				}
				if err == nil {
					t.Fatalf("Connection should have been closed")
				}
				c.Close()
			}
		})
	}
}

func TestNoRaceJetStreamDeleteStreamManyConsumers(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	mname := "MYS"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	// This number needs to be higher than the internal sendq size to trigger what this test is testing.
	for i := 0; i < 2000; i++ {
		_, err := mset.addConsumer(&ConsumerConfig{
			Durable:        fmt.Sprintf("D-%d", i),
			DeliverSubject: fmt.Sprintf("deliver.%d", i),
		})
		if err != nil {
			t.Fatalf("Error creating consumer: %v", err)
		}
	}
	// With bug this would not return and would hang.
	mset.delete()
}

func TestNoRaceJetStreamAPIStreamListPaging(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	// Forced cleanup of all persisted state.
	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	// Create 2X limit
	streamsNum := 2 * JSApiNamesLimit
	for i := 1; i <= streamsNum; i++ {
		name := fmt.Sprintf("STREAM-%06d", i)
		cfg := StreamConfig{Name: name, Storage: MemoryStorage}
		_, err := s.GlobalAccount().addStream(&cfg)
		if err != nil {
			t.Fatalf("Unexpected error adding stream: %v", err)
		}
	}

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	reqList := func(offset int) []byte {
		t.Helper()
		var req []byte
		if offset > 0 {
			req, _ = json.Marshal(&ApiPagedRequest{Offset: offset})
		}
		resp, err := nc.Request(JSApiStreams, req, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error getting stream list: %v", err)
		}
		return resp.Data
	}

	checkResp := func(resp []byte, expectedLen, expectedOffset int) {
		t.Helper()
		var listResponse JSApiStreamNamesResponse
		if err := json.Unmarshal(resp, &listResponse); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(listResponse.Streams) != expectedLen {
			t.Fatalf("Expected only %d streams but got %d", expectedLen, len(listResponse.Streams))
		}
		if listResponse.Total != streamsNum {
			t.Fatalf("Expected total to be %d but got %d", streamsNum, listResponse.Total)
		}
		if listResponse.Offset != expectedOffset {
			t.Fatalf("Expected offset to be %d but got %d", expectedOffset, listResponse.Offset)
		}
		if expectedLen < 1 {
			return
		}
		// Make sure we get the right stream.
		sname := fmt.Sprintf("STREAM-%06d", expectedOffset+1)
		if listResponse.Streams[0] != sname {
			t.Fatalf("Expected stream %q to be first, got %q", sname, listResponse.Streams[0])
		}
	}

	checkResp(reqList(0), JSApiNamesLimit, 0)
	checkResp(reqList(JSApiNamesLimit), JSApiNamesLimit, JSApiNamesLimit)
	checkResp(reqList(streamsNum), 0, streamsNum)
	checkResp(reqList(streamsNum-22), 22, streamsNum-22)
	checkResp(reqList(streamsNum+22), 0, streamsNum)
}

func TestNoRaceJetStreamAPIConsumerListPaging(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	// Forced cleanup of all persisted state.
	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	sname := "MYSTREAM"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: sname})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	consumersNum := JSApiNamesLimit
	for i := 1; i <= consumersNum; i++ {
		dsubj := fmt.Sprintf("d.%d", i)
		sub, _ := nc.SubscribeSync(dsubj)
		defer sub.Unsubscribe()
		nc.Flush()

		_, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: dsubj})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	reqListSubject := fmt.Sprintf(JSApiConsumersT, sname)
	reqList := func(offset int) []byte {
		t.Helper()
		var req []byte
		if offset > 0 {
			req, _ = json.Marshal(&JSApiConsumersRequest{ApiPagedRequest: ApiPagedRequest{Offset: offset}})
		}
		resp, err := nc.Request(reqListSubject, req, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error getting stream list: %v", err)
		}
		return resp.Data
	}

	checkResp := func(resp []byte, expectedLen, expectedOffset int) {
		t.Helper()
		var listResponse JSApiConsumerNamesResponse
		if err := json.Unmarshal(resp, &listResponse); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(listResponse.Consumers) != expectedLen {
			t.Fatalf("Expected only %d streams but got %d", expectedLen, len(listResponse.Consumers))
		}
		if listResponse.Total != consumersNum {
			t.Fatalf("Expected total to be %d but got %d", consumersNum, listResponse.Total)
		}
		if listResponse.Offset != expectedOffset {
			t.Fatalf("Expected offset to be %d but got %d", expectedOffset, listResponse.Offset)
		}
	}

	checkResp(reqList(0), JSApiNamesLimit, 0)
	checkResp(reqList(consumersNum-22), 22, consumersNum-22)
	checkResp(reqList(consumersNum+22), 0, consumersNum)
}

func TestNoRaceJetStreamWorkQueueLoadBalance(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	mname := "MY_MSG_SET"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Subjects: []string{"foo", "bar"}})
	if err != nil {
		t.Fatalf("Unexpected error adding message set: %v", err)
	}
	defer mset.delete()

	// Create basic work queue mode consumer.
	oname := "WQ"
	o, err := mset.addConsumer(&ConsumerConfig{Durable: oname, AckPolicy: AckExplicit})
	if err != nil {
		t.Fatalf("Expected no error with durable, got %v", err)
	}
	defer o.delete()

	// To send messages.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// For normal work queue semantics, you send requests to the subject with stream and consumer name.
	reqMsgSubj := o.requestNextMsgSubject()

	numWorkers := 25
	counts := make([]int32, numWorkers)
	var received int32

	rwg := &sync.WaitGroup{}
	rwg.Add(numWorkers)

	wg := &sync.WaitGroup{}
	wg.Add(numWorkers)
	ch := make(chan bool)

	toSend := 1000

	for i := 0; i < numWorkers; i++ {
		nc := clientConnectToServer(t, s)
		defer nc.Close()

		go func(index int32) {
			rwg.Done()
			defer wg.Done()
			<-ch

			for counter := &counts[index]; ; {
				m, err := nc.Request(reqMsgSubj, nil, 100*time.Millisecond)
				if err != nil {
					return
				}
				m.Respond(nil)
				atomic.AddInt32(counter, 1)
				if total := atomic.AddInt32(&received, 1); total >= int32(toSend) {
					return
				}
			}
		}(int32(i))
	}

	// Wait for requestors to be ready
	rwg.Wait()
	close(ch)

	sendSubj := "bar"
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, sendSubj, "Hello World!")
	}

	// Wait for test to complete.
	wg.Wait()

	target := toSend / numWorkers
	delta := target/2 + 5
	low, high := int32(target-delta), int32(target+delta)

	for i := 0; i < numWorkers; i++ {
		if msgs := atomic.LoadInt32(&counts[i]); msgs < low || msgs > high {
			t.Fatalf("Messages received for worker [%d] too far off from target of %d, got %d", i, target, msgs)
		}
	}
}

func TestNoRaceJetStreamClusterLargeStreamInlineCatchup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "LSS", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sr := c.randomNonStreamLeader("$G", "TEST")
	sr.Shutdown()

	// In case sr was meta leader.
	c.waitOnLeader()

	msg, toSend := []byte("Hello JS Clustering"), 5000

	// Now fill up stream.
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}

	// Kill our current leader to make just 2.
	c.streamLeader("$G", "TEST").Shutdown()

	// Now restart the shutdown peer and wait for it to be current.
	sr = c.restartServer(sr)
	c.waitOnStreamCurrent(sr, "$G", "TEST")

	// Ask other servers to stepdown as leader so that sr becomes the leader.
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnStreamLeader("$G", "TEST")
		if sl := c.streamLeader("$G", "TEST"); sl != sr {
			sl.JetStreamStepdownStream("$G", "TEST")
			return fmt.Errorf("Server %s is not leader yet", sr)
		}
		return nil
	})

	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Check that we have all of our messsages stored.
	// Wait for a bit for upper layers to process.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if si.State.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected %d msgs, got %d", toSend, si.State.Msgs)
		}
		return nil
	})
}

func TestNoRaceJetStreamClusterStreamCreateAndLostQuorum(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sub, err := nc.SubscribeSync(JSAdvisoryStreamQuorumLostPre + ".*")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{Name: "NO-LQ-START", Replicas: 3}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.waitOnStreamLeader("$G", "NO-LQ-START")
	checkSubsPending(t, sub, 0)

	c.stopAll()
	// Start up the one we were connected to first and wait for it to be connected.
	s = c.restartServer(s)
	nc, err = nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer nc.Close()

	sub, err = nc.SubscribeSync(JSAdvisoryStreamQuorumLostPre + ".*")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	nc.Flush()

	c.restartAll()
	c.waitOnStreamLeader("$G", "NO-LQ-START")

	checkSubsPending(t, sub, 0)
}

func TestNoRaceJetStreamClusterSuperClusterMirrors(t *testing.T) {
	// These pass locally but are flaky on Travis.
	// Disable for now.
	skip(t)

	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// Client based API
	s := sc.clusterForName("C2").randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Create source stream.
	_, err := js.AddStream(&nats.StreamConfig{Name: "S1", Subjects: []string{"foo", "bar"}, Replicas: 3, Placement: &nats.Placement{Cluster: "C2"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Needed while Go client does not have mirror support.
	createStream := func(cfg *nats.StreamConfig) {
		t.Helper()
		if _, err := js.AddStream(cfg); err != nil {
			t.Fatalf("Unexpected error: %+v", err)
		}
	}

	// Send 100 messages.
	for i := 0; i < 100; i++ {
		if _, err := js.Publish("foo", []byte("MIRRORS!")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	createStream(&nats.StreamConfig{
		Name:      "M1",
		Mirror:    &nats.StreamSource{Name: "S1"},
		Placement: &nats.Placement{Cluster: "C1"},
	})

	// Faster timeout since we loop below checking for condition.
	js2, err := nc.JetStream(nats.MaxWait(50 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("M1")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 100 {
			return fmt.Errorf("Expected 100 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// Purge the source stream.
	if err := js.PurgeStream("S1"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
	// Send 50 more msgs now.
	for i := 0; i < 50; i++ {
		if _, err := js.Publish("bar", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	createStream(&nats.StreamConfig{
		Name:      "M2",
		Mirror:    &nats.StreamSource{Name: "S1"},
		Replicas:  3,
		Placement: &nats.Placement{Cluster: "C3"},
	})

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("M2")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 50 {
			return fmt.Errorf("Expected 50 msgs, got state: %+v", si.State)
		}
		if si.State.FirstSeq != 101 {
			return fmt.Errorf("Expected start seq of 101, got state: %+v", si.State)
		}
		return nil
	})

	sl := sc.clusterForName("C3").streamLeader("$G", "M2")
	doneCh := make(chan bool)

	// Now test that if the mirror get's interrupted that it picks up where it left off etc.
	go func() {
		// Send 100 more messages.
		for i := 0; i < 100; i++ {
			if _, err := js.Publish("foo", []byte("MIRRORS!")); err != nil {
				t.Errorf("Unexpected publish on %d error: %v", i, err)
			}
			time.Sleep(2 * time.Millisecond)
		}
		doneCh <- true
	}()

	time.Sleep(20 * time.Millisecond)
	sl.Shutdown()

	<-doneCh
	sc.clusterForName("C3").waitOnStreamLeader("$G", "M2")

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("M2")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 150 {
			return fmt.Errorf("Expected 150 msgs, got state: %+v", si.State)
		}
		if si.State.FirstSeq != 101 {
			return fmt.Errorf("Expected start seq of 101, got state: %+v", si.State)
		}
		return nil
	})
}

func TestNoRaceJetStreamClusterSuperClusterSources(t *testing.T) {
	// These pass locally but are flaky on Travis.
	// Disable for now.
	skip(t)

	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// Client based API
	s := sc.clusterForName("C1").randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Create our source streams.
	for _, sname := range []string{"foo", "bar", "baz"} {
		if _, err := js.AddStream(&nats.StreamConfig{Name: sname, Replicas: 1}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	sendBatch := func(subject string, n int) {
		for i := 0; i < n; i++ {
			msg := fmt.Sprintf("MSG-%d", i+1)
			if _, err := js.Publish(subject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}
	// Populate each one.
	sendBatch("foo", 10)
	sendBatch("bar", 15)
	sendBatch("baz", 25)

	// Needed while Go client does not have mirror support for creating mirror or source streams.
	createStream := func(cfg *nats.StreamConfig) {
		t.Helper()
		if _, err := js.AddStream(cfg); err != nil {
			t.Fatalf("Unexpected error: %+v", err)
		}
	}

	cfg := &nats.StreamConfig{
		Name: "MS",
		Sources: []*nats.StreamSource{
			{Name: "foo"},
			{Name: "bar"},
			{Name: "baz"},
		},
	}

	createStream(cfg)
	time.Sleep(time.Second)

	// Faster timeout since we loop below checking for condition.
	js2, err := nc.JetStream(nats.MaxWait(50 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("MS")
		if err != nil {
			return err
		}
		if si.State.Msgs != 50 {
			return fmt.Errorf("Expected 50 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// Purge the source streams.
	for _, sname := range []string{"foo", "bar", "baz"} {
		if err := js.PurgeStream(sname); err != nil {
			t.Fatalf("Unexpected purge error: %v", err)
		}
	}

	if err := js.DeleteStream("MS"); err != nil {
		t.Fatalf("Unexpected delete error: %v", err)
	}

	// Send more msgs now.
	sendBatch("foo", 10)
	sendBatch("bar", 15)
	sendBatch("baz", 25)

	cfg = &nats.StreamConfig{
		Name: "MS2",
		Sources: []*nats.StreamSource{
			{Name: "foo"},
			{Name: "bar"},
			{Name: "baz"},
		},
		Replicas:  3,
		Placement: &nats.Placement{Cluster: "C3"},
	}

	createStream(cfg)

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("MS2")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 50 {
			return fmt.Errorf("Expected 50 msgs, got state: %+v", si.State)
		}
		if si.State.FirstSeq != 1 {
			return fmt.Errorf("Expected start seq of 1, got state: %+v", si.State)
		}
		return nil
	})

	sl := sc.clusterForName("C3").streamLeader("$G", "MS2")
	doneCh := make(chan bool)

	if sl == sc.leader() {
		nc.Request(JSApiLeaderStepDown, nil, time.Second)
		sc.waitOnLeader()
	}

	// Now test that if the mirror get's interrupted that it picks up where it left off etc.
	go func() {
		// Send 50 more messages each.
		for i := 0; i < 50; i++ {
			msg := fmt.Sprintf("R-MSG-%d", i+1)
			for _, sname := range []string{"foo", "bar", "baz"} {
				if _, err := js.Publish(sname, []byte(msg)); err != nil {
					t.Errorf("Unexpected publish error: %v", err)
				}
			}
			time.Sleep(2 * time.Millisecond)
		}
		doneCh <- true
	}()

	time.Sleep(20 * time.Millisecond)
	sl.Shutdown()

	sc.clusterForName("C3").waitOnStreamLeader("$G", "MS2")
	<-doneCh

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("MS2")
		if err != nil {
			return err
		}
		if si.State.Msgs != 200 {
			return fmt.Errorf("Expected 200 msgs, got state: %+v", si.State)
		}
		return nil
	})
}

func TestNoRaceJetStreamClusterSourcesMuxd(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "SMUX", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Send in 10000 messages.
	msg, toSend := make([]byte, 1024), 10000
	rand.Read(msg)

	var sources []*nats.StreamSource
	// Create 10 origin streams.
	for i := 1; i <= 10; i++ {
		name := fmt.Sprintf("O-%d", i)
		if _, err := js.AddStream(&nats.StreamConfig{Name: name}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Load them up with a bunch of messages.
		for n := 0; n < toSend; n++ {
			if err := nc.Publish(name, msg); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
		sources = append(sources, &nats.StreamSource{Name: name})
	}

	// Now create our downstream stream that sources from all of them.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "S", Replicas: 2, Sources: sources}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkFor(t, 20*time.Second, 500*time.Millisecond, func() error {
		si, err := js.StreamInfo("S")
		if err != nil {
			t.Fatalf("Could not retrieve stream info")
		}
		if si.State.Msgs != uint64(10*toSend) {
			return fmt.Errorf("Expected %d msgs, got state: %+v", toSend*10, si.State)
		}
		return nil
	})

}
