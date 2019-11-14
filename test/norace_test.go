// Copyright 2019 The NATS Authors
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

package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// IMPORTANT: Tests in this file are not executed when running with the -race flag.

func TestNoRaceRouteSendSubs(t *testing.T) {
	template := `
			port: -1
			write_deadline: "2s"
			cluster {
				port: -1
				%s
			}
	`
	cfa := createConfFile(t, []byte(fmt.Sprintf(template, "")))
	defer os.Remove(cfa)
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
	for i := 0; i < totalPerServer; i++ {
		proto := fmt.Sprintf("SUB foo.%d %d\r\n", i, i*2)
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
	const max = 50 * 1024 * 1024 // 50MB
	runtime.ReadMemStats(&m)
	used := m.TotalAlloc - pta
	if used > max {
		t.Fatalf("Cluster using too much memory, expect < 50MB, got %dMB", used/(1024*1024))
	}

	for _, s := range servers {
		s.Shutdown()
	}
}

// Make sure we have the correct remote state when dealing with queue subscribers
// across many client connections.
func TestQueueSubWeightOrderMultipleConnections(t *testing.T) {
	s, opts := runNewRouteServer(t)
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
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	routeSend(fmt.Sprintf("INFO %s\r\n", b))

	start := make(chan bool)
	for _, nc := range clients {
		go func(nc *nats.Conn) {
			<-start
			// Now create 100 identical queue subscribers on each connection.
			for i := 0; i < 100; i++ {
				if _, err := nc.QueueSubscribeSync("foo", "bar"); err != nil {
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
