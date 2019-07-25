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
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"regexp"
	"runtime"
	"sync"
	"testing"
	"time"
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
	defer srvA.Shutdown()

	cfb := createConfFile(t, []byte(fmt.Sprintf(template, "")))
	srvB, optsB := RunServerWithConfig(cfb)
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
			for i := 0; i < count; i++ {
				s.sf(replyMsg)
			}
			s.sf("PING\r\n")
			s.ef(pongRe)
			wg.Done()
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
