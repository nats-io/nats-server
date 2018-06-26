// Copyright 2018 The NATS Authors
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

package server

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	nats "github.com/nats-io/go-nats"
)

func closedConnsEqual(s *Server, num int, wait time.Duration) bool {
	end := time.Now().Add(wait)
	for time.Now().Before(end) {
		if s.numClosedConns() == num {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	n := s.numClosedConns()
	return n == num
}

func totalClosedConnsEqual(s *Server, num uint64, wait time.Duration) bool {
	end := time.Now().Add(wait)
	for time.Now().Before(end) {
		if s.totalClosedConns() == num {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	n := s.totalClosedConns()
	return n == num
}

func TestClosedConnsAccounting(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxClosedClients = 10

	s := RunServer(opts)
	defer s.Shutdown()

	wait := 20 * time.Millisecond

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	nc.Close()

	if !closedConnsEqual(s, 1, wait) {
		t.Fatalf("Closed conns expected to be 1, got %d\n", s.numClosedConns())
	}

	conns := s.closedClients()
	if lc := len(conns); lc != 1 {
		t.Fatalf("len(conns) expected to be %d, got %d\n", 1, lc)
	}
	if conns[0].Cid != 1 {
		t.Fatalf("Expected CID to be 1, got %d\n", conns[0].Cid)
	}

	// Now create 21 more
	for i := 0; i < 21; i++ {
		nc, err = nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		nc.Close()
	}

	if !closedConnsEqual(s, opts.MaxClosedClients, wait) {
		t.Fatalf("Closed conns expected to be %d, got %d\n",
			opts.MaxClosedClients,
			s.numClosedConns())
	}

	if !totalClosedConnsEqual(s, 22, wait) {
		t.Fatalf("Closed conns expected to be 22, got %d\n",
			s.numClosedConns())
	}

	conns = s.closedClients()
	if lc := len(conns); lc != opts.MaxClosedClients {
		t.Fatalf("len(conns) expected to be %d, got %d\n",
			opts.MaxClosedClients, lc)
	}

	// Set it to the start after overflow.
	cid := uint64(22 - opts.MaxClosedClients)
	for _, ci := range conns {
		cid++
		if ci.Cid != cid {
			t.Fatalf("Expected cid of %d, got %d\n", cid, ci.Cid)
		}
	}
}

func TestClosedConnsSubsAccounting(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	// Now create some subscriptions
	numSubs := 10
	for i := 0; i < numSubs; i++ {
		subj := fmt.Sprintf("foo.%d", i)
		nc.Subscribe(subj, func(m *nats.Msg) {})
	}
	nc.Flush()
	nc.Close()

	if !closedConnsEqual(s, 1, 20*time.Millisecond) {
		t.Fatalf("Closed conns expected to be 1, got %d\n",
			s.numClosedConns())
	}
	conns := s.closedClients()
	if lc := len(conns); lc != 1 {
		t.Fatalf("len(conns) expected to be 1, got %d\n", lc)
	}
	ci := conns[0]

	if len(ci.subs) != numSubs {
		t.Fatalf("Expected number of Subs to be %d, got %d\n", numSubs, len(ci.subs))
	}
}

func checkReason(t *testing.T, reason string, expected ClosedState) {
	if !strings.Contains(reason, expected.String()) {
		t.Fatalf("Expected closed connection with `%s` state, got `%s`\n",
			expected, reason)
	}
}

func TestClosedAuthorizationTimeout(t *testing.T) {
	serverOptions := DefaultOptions()
	serverOptions.Authorization = "my_token"
	serverOptions.AuthTimeout = 0.4
	s := RunServer(serverOptions)
	defer s.Shutdown()

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", serverOptions.Host, serverOptions.Port))
	if err != nil {
		t.Fatalf("Error dialing server: %v\n", err)
	}
	defer conn.Close()

	if !closedConnsEqual(s, 1, 2*time.Second) {
		t.Fatalf("Closed conns expected to be 1, got %d\n", s.numClosedConns())
	}
	conns := s.closedClients()
	if lc := len(conns); lc != 1 {
		t.Fatalf("len(conns) expected to be %d, got %d\n", 1, lc)
	}
	checkReason(t, conns[0].Reason, AuthenticationTimeout)
}

func TestClosedAuthorizationViolation(t *testing.T) {
	serverOptions := DefaultOptions()
	serverOptions.Authorization = "my_token"
	s := RunServer(serverOptions)
	defer s.Shutdown()

	opts := s.getOpts()
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	nc, err := nats.Connect(url)
	if err == nil {
		nc.Close()
		t.Fatal("Expected failure for connection")
	}

	if !closedConnsEqual(s, 1, 2*time.Second) {
		t.Fatalf("Closed conns expected to be 1, got %d\n", s.numClosedConns())
	}
	conns := s.closedClients()
	if lc := len(conns); lc != 1 {
		t.Fatalf("len(conns) expected to be %d, got %d\n", 1, lc)
	}
	checkReason(t, conns[0].Reason, AuthenticationViolation)
}

func TestClosedUPAuthorizationViolation(t *testing.T) {
	serverOptions := DefaultOptions()
	serverOptions.Username = "my_user"
	serverOptions.Password = "my_secret"
	s := RunServer(serverOptions)
	defer s.Shutdown()

	opts := s.getOpts()
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	nc, err := nats.Connect(url)
	if err == nil {
		nc.Close()
		t.Fatal("Expected failure for connection")
	}

	url2 := fmt.Sprintf("nats://my_user:wrong_pass@%s:%d", opts.Host, opts.Port)
	nc, err = nats.Connect(url2)
	if err == nil {
		nc.Close()
		t.Fatal("Expected failure for connection")
	}

	if !closedConnsEqual(s, 2, 2*time.Second) {
		t.Fatalf("Closed conns expected to be 2, got %d\n", s.numClosedConns())
	}
	conns := s.closedClients()
	if lc := len(conns); lc != 2 {
		t.Fatalf("len(conns) expected to be %d, got %d\n", 2, lc)
	}
	checkReason(t, conns[0].Reason, AuthenticationViolation)
	checkReason(t, conns[1].Reason, AuthenticationViolation)
}

func TestClosedMaxPayload(t *testing.T) {
	serverOptions := DefaultOptions()
	serverOptions.MaxPayload = 100

	s := RunServer(serverOptions)
	defer s.Shutdown()

	opts := s.getOpts()
	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)

	conn, err := net.DialTimeout("tcp", endpoint, time.Second)
	if err != nil {
		t.Fatalf("Could not make a raw connection to the server: %v", err)
	}
	defer conn.Close()

	// This should trigger it.
	pub := fmt.Sprintf("PUB foo.bar 1024\r\n")
	conn.Write([]byte(pub))

	if !closedConnsEqual(s, 1, 2*time.Second) {
		t.Fatalf("Closed conns expected to be 1, got %d\n", s.numClosedConns())
	}
	conns := s.closedClients()
	if lc := len(conns); lc != 1 {
		t.Fatalf("len(conns) expected to be %d, got %d\n", 1, lc)
	}
	checkReason(t, conns[0].Reason, MaxPayloadExceeded)
}

func TestClosedSlowConsumerWriteDeadline(t *testing.T) {
	opts := DefaultOptions()
	opts.WriteDeadline = 10 * time.Microsecond // Make very small to trip.
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
	if !closedConnsEqual(s, 1, 2*time.Second) {
		t.Fatalf("Closed conns expected to be 1, got %d\n", s.numClosedConns())
	}
	conns := s.closedClients()
	if lc := len(conns); lc != 1 {
		t.Fatalf("len(conns) expected to be %d, got %d\n", 1, lc)
	}
	checkReason(t, conns[0].Reason, SlowConsumerWriteDeadline)
}

func TestClosedSlowConsumerPendingBytes(t *testing.T) {
	opts := DefaultOptions()
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
	if !closedConnsEqual(s, 1, 2*time.Second) {
		t.Fatalf("Closed conns expected to be 1, got %d\n", s.numClosedConns())
	}
	conns := s.closedClients()
	if lc := len(conns); lc != 1 {
		t.Fatalf("len(conns) expected to be %d, got %d\n", 1, lc)
	}
	checkReason(t, conns[0].Reason, SlowConsumerPendingBytes)
}

func TestClosedTLSHandshake(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/tls.conf")
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	opts.TLSVerify = true
	opts.NoLog = true
	opts.NoSigs = true
	s := RunServer(opts)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://%s:%d", opts.Host, opts.Port))
	if err == nil {
		nc.Close()
		t.Fatal("Expected failure for connection")
	}

	if !closedConnsEqual(s, 1, 2*time.Second) {
		t.Fatalf("Closed conns expected to be 1, got %d\n", s.numClosedConns())
	}
	conns := s.closedClients()
	if lc := len(conns); lc != 1 {
		t.Fatalf("len(conns) expected to be %d, got %d\n", 1, lc)
	}
	checkReason(t, conns[0].Reason, TLSHandshakeError)
}
