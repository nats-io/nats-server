// Copyright 2019-2020 The NATS Authors
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

//go:build !race && !skip_no_race_tests
// +build !race,!skip_no_race_tests

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/url"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// IMPORTANT: Tests in this file are not executed when running with the -race flag.
//            The test name should be prefixed with TestNoRace so we can run only
//            those tests: go test -run=TestNoRace ...

func TestNoRaceRouteSendSubs(t *testing.T) {
	template := `
			port: -1
			write_deadline: "2s"
			cluster {
				port: -1
				%s
			}
			no_sys_acc: true
	`
	cfa := createConfFile(t, []byte(fmt.Sprintf(template, "")))
	defer removeFile(t, cfa)
	srvA, optsA := RunServerWithConfig(cfa)
	srvA.Shutdown()
	optsA.DisableShortFirstPing = true
	srvA = RunServer(optsA)
	defer srvA.Shutdown()

	cfb := createConfFile(t, []byte(fmt.Sprintf(template, "")))
	srvB, optsB := RunServerWithConfig(cfb)
	srvB.Shutdown()
	optsB.DisableShortFirstPing = true
	srvB = RunServer(optsB)
	defer srvB.Shutdown()

	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	clientASend, clientAExpect := setupConn(t, clientA)
	clientASend("PING\r\n")
	clientAExpect(pongRe)

	clientB := createClientConn(t, optsB.Host, optsB.Port)
	defer clientB.Close()

	clientBSend, clientBExpect := setupConn(t, clientB)
	clientBSend("PING\r\n")
	clientBExpect(pongRe)

	// total number of subscriptions per server
	totalPerServer := 100000
	for i := 0; i < totalPerServer/2; i++ {
		proto := fmt.Sprintf("SUB foo.%d %d\r\n", i, i*2+1)
		clientASend(proto)
		clientBSend(proto)
		proto = fmt.Sprintf("SUB bar.%d queue.%d %d\r\n", i, i, i*2+2)
		clientASend(proto)
		clientBSend(proto)
	}
	clientASend("PING\r\n")
	clientAExpect(pongRe)
	clientBSend("PING\r\n")
	clientBExpect(pongRe)

	if err := checkExpectedSubs(totalPerServer, srvA, srvB); err != nil {
		t.Fatalf(err.Error())
	}

	routes := fmt.Sprintf(`
		routes: [
			"nats://%s:%d"
		]
	`, optsA.Cluster.Host, optsA.Cluster.Port)
	if err := ioutil.WriteFile(cfb, []byte(fmt.Sprintf(template, routes)), 0600); err != nil {
		t.Fatalf("Error rewriting B's config file: %v", err)
	}
	if err := srvB.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	checkClusterFormed(t, srvA, srvB)
	if err := checkExpectedSubs(2*totalPerServer, srvA, srvB); err != nil {
		t.Fatalf(err.Error())
	}

	checkSlowConsumers := func(t *testing.T) {
		t.Helper()
		if srvB.NumSlowConsumers() != 0 || srvA.NumSlowConsumers() != 0 {
			t.Fatalf("Expected no slow consumers, got %d for srvA and %df for srvB",
				srvA.NumSlowConsumers(), srvB.NumSlowConsumers())
		}
	}
	checkSlowConsumers(t)

	type sender struct {
		c  net.Conn
		sf sendFun
		ef expectFun
	}
	var senders []*sender
	createSenders := func(t *testing.T, host string, port int) {
		t.Helper()
		for i := 0; i < 25; i++ {
			s := &sender{}
			s.c = createClientConn(t, host, port)
			s.sf, s.ef = setupConn(t, s.c)
			s.sf("PING\r\n")
			s.ef(pongRe)
			senders = append(senders, s)
		}
	}
	createSenders(t, optsA.Host, optsA.Port)
	createSenders(t, optsB.Host, optsB.Port)
	for _, s := range senders {
		defer s.c.Close()
	}

	// Now create SUBs on A and B for "ping.replies" and simulate
	// that there are thousands of replies being sent on
	// both sides.
	createSubOnReplies := func(t *testing.T, host string, port int) net.Conn {
		t.Helper()
		c := createClientConn(t, host, port)
		send, expect := setupConn(t, c)
		send("SUB ping.replies 123456789\r\nPING\r\n")
		expect(pongRe)
		return c
	}
	requestorOnA := createSubOnReplies(t, optsA.Host, optsA.Port)
	defer requestorOnA.Close()

	requestorOnB := createSubOnReplies(t, optsB.Host, optsB.Port)
	defer requestorOnB.Close()

	if err := checkExpectedSubs(2*totalPerServer+2, srvA, srvB); err != nil {
		t.Fatalf(err.Error())
	}

	totalReplies := 120000
	payload := sizedBytes(400)
	expectedBytes := (len(fmt.Sprintf("MSG ping.replies 123456789 %d\r\n\r\n", len(payload))) + len(payload)) * totalReplies
	ch := make(chan error, 2)
	recvReplies := func(c net.Conn) {
		var buf [32 * 1024]byte

		for total := 0; total < expectedBytes; {
			n, err := c.Read(buf[:])
			if err != nil {
				ch <- fmt.Errorf("read error: %v", err)
				return
			}
			total += n
		}
		ch <- nil
	}
	go recvReplies(requestorOnA)
	go recvReplies(requestorOnB)

	wg := sync.WaitGroup{}
	wg.Add(len(senders))
	replyMsg := fmt.Sprintf("PUB ping.replies %d\r\n%s\r\n", len(payload), payload)
	for _, s := range senders {
		go func(s *sender, count int) {
			defer wg.Done()
			for i := 0; i < count; i++ {
				s.sf(replyMsg)
			}
			s.sf("PING\r\n")
			s.ef(pongRe)
		}(s, totalReplies/len(senders))
	}

	for i := 0; i < 2; i++ {
		select {
		case e := <-ch:
			if e != nil {
				t.Fatalf("Error: %v", e)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("Did not receive all %v replies", totalReplies)
		}
	}
	checkSlowConsumers(t)
	wg.Wait()
	checkSlowConsumers(t)

	// Let's remove the route and do a config reload.
	// Otherwise, on test shutdown the client close
	// will cause the server to try to send unsubs and
	// this can delay the test.
	if err := ioutil.WriteFile(cfb, []byte(fmt.Sprintf(template, "")), 0600); err != nil {
		t.Fatalf("Error rewriting B's config file: %v", err)
	}
	if err := srvB.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}
}

func TestNoRaceDynamicResponsePermsMemory(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/authorization.conf")
	defer srv.Shutdown()

	// We will test the timeout to make sure that we are not showing excessive growth
	// when a reply subject is not utilized by the responder.

	// Alice can do anything, so she will be our requestor
	rc := createClientConn(t, opts.Host, opts.Port)
	defer rc.Close()
	expectAuthRequired(t, rc)
	doAuthConnect(t, rc, "", "alice", DefaultPass)
	expectResult(t, rc, okRe)

	// MY_STREAM_SERVICE has an expiration of 10ms for the response permissions.
	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", "svcb", DefaultPass)
	expectResult(t, c, okRe)

	sendProto(t, c, "SUB my.service.req 1\r\n")
	expectResult(t, c, okRe)

	var m runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&m)
	pta := m.TotalAlloc

	// Need this so we do not blow the allocs on expectResult which makes 32k each time.
	expBuf := make([]byte, 32768)
	expect := func(c net.Conn, re *regexp.Regexp) {
		t.Helper()
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _ := c.Read(expBuf)
		c.SetReadDeadline(time.Time{})
		buf := expBuf[:n]
		if !re.Match(buf) {
			t.Fatalf("Response did not match expected: \n\tReceived:'%q'\n\tExpected:'%s'", buf, re)
		}
	}

	// Now send off some requests. We will not answer them and this will build up reply
	// permissions in the server.
	for i := 0; i < 10000; i++ {
		pub := fmt.Sprintf("PUB my.service.req resp.%d 2\r\nok\r\n", i)
		sendProto(t, rc, pub)
		expect(rc, okRe)
		expect(c, msgRe)
	}

	const max = 20 * 1024 * 1024 // 20MB
	checkFor(t, time.Second, 25*time.Millisecond, func() error {
		runtime.GC()
		runtime.ReadMemStats(&m)
		used := m.TotalAlloc - pta
		if used > max {
			return fmt.Errorf("Using too much memory, expect < 20MB, got %dMB", used/(1024*1024))
		}
		return nil
	})
}

func TestNoRaceLargeClusterMem(t *testing.T) {
	// Try to clean up.
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	pta := m.TotalAlloc

	opts := func() *server.Options {
		o := DefaultTestOptions
		o.Host = "127.0.0.1"
		o.Port = -1
		o.Cluster.Host = o.Host
		o.Cluster.Port = -1
		return &o
	}

	var servers []*server.Server

	// Create seed first.
	o := opts()
	s := RunServer(o)
	servers = append(servers, s)

	// For connecting to seed server above.
	routeAddr := fmt.Sprintf("nats-route://%s:%d", o.Cluster.Host, o.Cluster.Port)
	rurl, _ := url.Parse(routeAddr)
	routes := []*url.URL{rurl}

	numServers := 15

	for i := 1; i < numServers; i++ {
		o := opts()
		o.Routes = routes
		s := RunServer(o)
		servers = append(servers, s)
	}
	checkClusterFormed(t, servers...)

	// Calculate in MB what we are using now.
	const max = 80 * 1024 * 1024 // 80MB
	runtime.ReadMemStats(&m)
	used := m.TotalAlloc - pta
	if used > max {
		t.Fatalf("Cluster using too much memory, expect < 80MB, got %dMB", used/(1024*1024))
	}

	for _, s := range servers {
		s.Shutdown()
	}
}

// Make sure we have the correct remote state when dealing with queue subscribers
// across many client connections.
func TestNoRaceQueueSubWeightOrderMultipleConnections(t *testing.T) {
	opts, err := server.ProcessConfigFile("./configs/new_cluster.conf")
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	opts.DisableShortFirstPing = true
	s := RunServer(opts)
	defer s.Shutdown()

	// Create 100 connections to s
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	clients := make([]*nats.Conn, 0, 100)
	for i := 0; i < 100; i++ {
		nc, err := nats.Connect(url, nats.NoReconnect())
		if err != nil {
			t.Fatalf("Error connecting: %v", err)
		}
		defer nc.Close()
		clients = append(clients, nc)
	}

	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	routeID := "RTEST_NEW:22"
	routeSend, routeExpect := setupRouteEx(t, rc, opts, routeID)

	info := checkInfoMsg(t, rc)

	info.ID = routeID
	info.Name = routeID

	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	// Send our INFO and wait for a PONG. This will prevent a race
	// where server will have started processing queue subscriptions
	// and then processes the route's INFO (sending its current subs)
	// followed by updates.
	routeSend(fmt.Sprintf("INFO %s\r\nPING\r\n", b))
	routeExpect(pongRe)

	start := make(chan bool)
	for _, nc := range clients {
		go func(nc *nats.Conn) {
			<-start
			// Now create 100 identical queue subscribers on each connection.
			for i := 0; i < 100; i++ {
				if _, err := nc.QueueSubscribe("foo", "bar", func(_ *nats.Msg) {}); err != nil {
					return
				}
			}
			nc.Flush()
		}(nc)
	}
	close(start)

	// We did have this where we wanted to get every update, but now with optimizations
	// we just want to make sure we always are increasing and that a previous update to
	// a lesser queue weight is never delivered for this test.
	maxExpected := 10000
	updates := 0
	for qw := 0; qw < maxExpected; {
		buf := routeExpect(rsubRe)
		matches := rsubRe.FindAllSubmatch(buf, -1)
		for _, m := range matches {
			if len(m) != 5 {
				t.Fatalf("Expected a weight for the queue group")
			}
			nqw, err := strconv.Atoi(string(m[4]))
			if err != nil {
				t.Fatalf("Got an error converting queue weight: %v", err)
			}
			// Make sure the new value only increases, ok to skip since we will
			// optimize this now, but needs to always be increasing.
			if nqw <= qw {
				t.Fatalf("Was expecting increasing queue weight after %d, got %d", qw, nqw)
			}
			qw = nqw
			updates++
		}
	}
	if updates >= maxExpected {
		t.Fatalf("Was not expecting all %v updates to be received", maxExpected)
	}
}

func TestNoRaceClusterLeaksSubscriptions(t *testing.T) {
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	urlA := fmt.Sprintf("nats://%s:%d/", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, optsB.Port)

	numResponses := 100
	repliers := make([]*nats.Conn, 0, numResponses)

	var noOpErrHandler = func(_ *nats.Conn, _ *nats.Subscription, _ error) {}

	// Create 100 repliers
	for i := 0; i < 50; i++ {
		nc1, _ := nats.Connect(urlA)
		nc1.SetErrorHandler(noOpErrHandler)
		nc2, _ := nats.Connect(urlB)
		nc2.SetErrorHandler(noOpErrHandler)
		repliers = append(repliers, nc1, nc2)
		nc1.Subscribe("test.reply", func(m *nats.Msg) {
			m.Respond([]byte("{\"sender\": 22 }"))
		})
		nc2.Subscribe("test.reply", func(m *nats.Msg) {
			m.Respond([]byte("{\"sender\": 33 }"))
		})
		nc1.Flush()
		nc2.Flush()
	}

	servers := fmt.Sprintf("%s, %s", urlA, urlB)
	req := sizedBytes(8 * 1024)

	// Now run a requestor in a loop, creating and tearing down each time to
	// simulate running a modified nats-req.
	doReq := func() {
		msgs := make(chan *nats.Msg, 1)
		inbox := nats.NewInbox()
		grp := nuid.Next()
		// Create 8 queue Subscribers for responses.
		for i := 0; i < 8; i++ {
			nc, _ := nats.Connect(servers)
			nc.SetErrorHandler(noOpErrHandler)
			nc.ChanQueueSubscribe(inbox, grp, msgs)
			nc.Flush()
			defer nc.Close()
		}
		nc, _ := nats.Connect(servers)
		nc.SetErrorHandler(noOpErrHandler)
		nc.PublishRequest("test.reply", inbox, req)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		var received int
		for {
			select {
			case <-msgs:
				received++
				if received >= numResponses {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}

	var wg sync.WaitGroup

	doRequests := func(n int) {
		for i := 0; i < n; i++ {
			doReq()
		}
		wg.Done()
	}

	concurrent := 10
	wg.Add(concurrent)
	for i := 0; i < concurrent; i++ {
		go doRequests(10)
	}
	wg.Wait()

	// Close responders too, should have zero(0) subs attached to routes.
	for _, nc := range repliers {
		nc.Close()
	}

	// Make sure no clients remain. This is to make sure the test is correct and that
	// we have closed all the client connections.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		v1, _ := srvA.Varz(nil)
		v2, _ := srvB.Varz(nil)
		if v1.Connections != 0 || v2.Connections != 0 {
			return fmt.Errorf("We have lingering client connections %d:%d", v1.Connections, v2.Connections)
		}
		return nil
	})

	loadRoutez := func() (*server.Routez, *server.Routez) {
		v1, err := srvA.Routez(&server.RoutezOptions{Subscriptions: true})
		if err != nil {
			t.Fatalf("Error getting Routez: %v", err)
		}
		v2, err := srvB.Routez(&server.RoutezOptions{Subscriptions: true})
		if err != nil {
			t.Fatalf("Error getting Routez: %v", err)
		}
		return v1, v2
	}

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		r1, r2 := loadRoutez()
		if r1.Routes[0].NumSubs != 0 {
			return fmt.Errorf("Leaked %d subs: %+v", r1.Routes[0].NumSubs, r1.Routes[0].Subs)
		}
		if r2.Routes[0].NumSubs != 0 {
			return fmt.Errorf("Leaked %d subs: %+v", r2.Routes[0].NumSubs, r2.Routes[0].Subs)
		}
		return nil
	})
}

func TestNoRaceLeafNodeSmapUpdate(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	// Create a client on leaf server
	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()
	csend, cexpect := setupConn(t, c)

	numSubs := make(chan int, 1)
	doneCh := make(chan struct{}, 1)
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for i := 1; ; i++ {
			csend(fmt.Sprintf("SUB foo.%d %d\r\n", i, i))
			select {
			case <-doneCh:
				numSubs <- i
				return
			default:
			}
		}
	}()

	time.Sleep(5 * time.Millisecond)
	// Create leaf node
	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	setupConn(t, lc)
	checkLeafNodeConnected(t, s)

	close(doneCh)
	ns := <-numSubs
	csend("PING\r\n")
	cexpect(pongRe)
	wg.Wait()

	// Make sure we receive as many LS+ protocols (since all subs are unique).
	// But we also have to count for LDS subject.
	// There may be so many protocols and partials, that expectNumberOfProtos may
	// not work. Do a manual search here.
	checkLS := func(proto string, expected int) {
		t.Helper()
		p := []byte(proto)
		cur := 0
		buf := make([]byte, 32768)
		for ls := 0; ls < expected; {
			lc.SetReadDeadline(time.Now().Add(2 * time.Second))
			n, err := lc.Read(buf)
			lc.SetReadDeadline(time.Time{})
			if err == nil && n > 0 {
				for i := 0; i < n; i++ {
					if buf[i] == p[cur] {
						cur++
						if cur == len(p) {
							ls++
							cur = 0
						}
					} else {
						cur = 0
					}
				}
			}
			if err != nil || ls > expected {
				t.Fatalf("Expected %v %sgot %v, err: %v", expected, proto, ls, err)
			}
		}
	}
	checkLS("LS+ ", ns+1)

	// Now unsub all those subs...
	for i := 1; i <= ns; i++ {
		csend(fmt.Sprintf("UNSUB %d\r\n", i))
	}
	csend("PING\r\n")
	cexpect(pongRe)

	// Expect that many LS-
	checkLS("LS- ", ns)
}

func TestNoRaceSlowProxy(t *testing.T) {
	t.Skip()

	opts := DefaultTestOptions
	opts.Port = -1
	s := RunServer(&opts)
	defer s.Shutdown()

	rttTarget := 22 * time.Millisecond
	bwTarget := 10 * 1024 * 1024 / 8 // 10mbit

	sp := newSlowProxy(rttTarget, bwTarget, bwTarget, &opts)
	defer sp.stop()

	nc, err := nats.Connect(sp.clientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	doRTT := func() time.Duration {
		t.Helper()
		const samples = 5
		var total time.Duration
		for i := 0; i < samples; i++ {
			rtt, _ := nc.RTT()
			total += rtt
		}
		return total / samples
	}

	rtt := doRTT()
	if rtt < rttTarget || rtt > (rttTarget*3/2) {
		t.Fatalf("rtt is out of range, target of %v, actual %v", rttTarget, rtt)
	}

	// Now test send BW.
	const payloadSize = 64 * 1024
	var payload [payloadSize]byte
	rand.Read(payload[:])

	// 5MB total.
	bytesSent := (5 * 1024 * 1024)
	toSend := bytesSent / payloadSize

	start := time.Now()
	for i := 0; i < toSend; i++ {
		nc.Publish("z", payload[:])
	}
	nc.Flush()
	tt := time.Since(start)
	bps := float64(bytesSent) / tt.Seconds()
	min, max := float64(bwTarget)*0.8, float64(bwTarget)*1.25
	if bps < min || bps > max {
		t.Fatalf("bps is off, target is %v, actual is %v", bwTarget, bps)
	}
}
