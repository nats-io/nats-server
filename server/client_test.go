// Copyright 2012-2018 The NATS Authors
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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crypto/rand"
	"crypto/tls"

	"github.com/nats-io/go-nats"
)

type serverInfo struct {
	Id           string `json:"server_id"`
	Host         string `json:"host"`
	Port         uint   `json:"port"`
	Version      string `json:"version"`
	AuthRequired bool   `json:"auth_required"`
	TLSRequired  bool   `json:"tls_required"`
	MaxPayload   int64  `json:"max_payload"`
}

func createClientAsync(ch chan *client, s *Server, cli net.Conn) {
	go func() {
		c := s.createClient(cli)
		// Must be here to suppress +OK
		c.opts.Verbose = false
		ch <- c
	}()
}

var defaultServerOptions = Options{
	Trace:  false,
	Debug:  false,
	NoLog:  true,
	NoSigs: true,
}

func rawSetup(serverOptions Options) (*Server, *client, *bufio.Reader, string) {
	cli, srv := net.Pipe()
	cr := bufio.NewReaderSize(cli, maxBufSize)
	s := New(&serverOptions)

	ch := make(chan *client)
	createClientAsync(ch, s, srv)

	l, _ := cr.ReadString('\n')

	// Grab client
	c := <-ch
	return s, c, cr, l
}

func setUpClientWithResponse() (*client, string) {
	_, c, _, l := rawSetup(defaultServerOptions)
	return c, l
}

func setupClient() (*Server, *client, *bufio.Reader) {
	s, c, cr, _ := rawSetup(defaultServerOptions)
	return s, c, cr
}

func checkClientsCount(t *testing.T, s *Server, expected int) {
	t.Helper()
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if nc := s.NumClients(); nc != expected {
			return fmt.Errorf("The number of expected connections was %v, got %v", expected, nc)
		}
		return nil
	})
}

func TestClientCreateAndInfo(t *testing.T) {
	c, l := setUpClientWithResponse()

	if c.cid != 1 {
		t.Fatalf("Expected cid of 1 vs %d\n", c.cid)
	}
	if c.state != OP_START {
		t.Fatal("Expected state to be OP_START")
	}

	if !strings.HasPrefix(l, "INFO ") {
		t.Fatalf("INFO response incorrect: %s\n", l)
	}
	// Make sure payload is proper json
	var info serverInfo
	err := json.Unmarshal([]byte(l[5:]), &info)
	if err != nil {
		t.Fatalf("Could not parse INFO json: %v\n", err)
	}
	// Sanity checks
	if info.MaxPayload != MAX_PAYLOAD_SIZE ||
		info.AuthRequired || info.TLSRequired ||
		info.Port != DEFAULT_PORT {
		t.Fatalf("INFO inconsistent: %+v\n", info)
	}
}

func TestNonTLSConnectionState(t *testing.T) {
	_, c, _ := setupClient()
	state := c.GetTLSConnectionState()
	if state != nil {
		t.Error("GetTLSConnectionState() returned non-nil")
	}
}

func TestClientConnect(t *testing.T) {
	_, c, _ := setupClient()

	// Basic Connect setting flags
	connectOp := []byte("CONNECT {\"verbose\":true,\"pedantic\":true,\"tls_required\":false,\"echo\":false}\r\n")
	err := c.parse(connectOp)
	if err != nil {
		t.Fatalf("Received error: %v\n", err)
	}
	if c.state != OP_START {
		t.Fatalf("Expected state of OP_START vs %d\n", c.state)
	}
	if !reflect.DeepEqual(c.opts, clientOpts{Verbose: true, Pedantic: true, Echo: false}) {
		t.Fatalf("Did not parse connect options correctly: %+v\n", c.opts)
	}

	// Test that we can capture user/pass
	connectOp = []byte("CONNECT {\"user\":\"derek\",\"pass\":\"foo\"}\r\n")
	c.opts = defaultOpts
	err = c.parse(connectOp)
	if err != nil {
		t.Fatalf("Received error: %v\n", err)
	}
	if c.state != OP_START {
		t.Fatalf("Expected state of OP_START vs %d\n", c.state)
	}
	if !reflect.DeepEqual(c.opts, clientOpts{Echo: true, Verbose: true, Pedantic: true, Username: "derek", Password: "foo"}) {
		t.Fatalf("Did not parse connect options correctly: %+v\n", c.opts)
	}

	// Test that we can capture client name
	connectOp = []byte("CONNECT {\"user\":\"derek\",\"pass\":\"foo\",\"name\":\"router\"}\r\n")
	c.opts = defaultOpts
	err = c.parse(connectOp)
	if err != nil {
		t.Fatalf("Received error: %v\n", err)
	}
	if c.state != OP_START {
		t.Fatalf("Expected state of OP_START vs %d\n", c.state)
	}

	if !reflect.DeepEqual(c.opts, clientOpts{Echo: true, Verbose: true, Pedantic: true, Username: "derek", Password: "foo", Name: "router"}) {
		t.Fatalf("Did not parse connect options correctly: %+v\n", c.opts)
	}

	// Test that we correctly capture auth tokens
	connectOp = []byte("CONNECT {\"auth_token\":\"YZZ222\",\"name\":\"router\"}\r\n")
	c.opts = defaultOpts
	err = c.parse(connectOp)
	if err != nil {
		t.Fatalf("Received error: %v\n", err)
	}
	if c.state != OP_START {
		t.Fatalf("Expected state of OP_START vs %d\n", c.state)
	}

	if !reflect.DeepEqual(c.opts, clientOpts{Echo: true, Verbose: true, Pedantic: true, Authorization: "YZZ222", Name: "router"}) {
		t.Fatalf("Did not parse connect options correctly: %+v\n", c.opts)
	}
}

func TestClientConnectProto(t *testing.T) {
	_, c, r := setupClient()

	// Basic Connect setting flags, proto should be zero (original proto)
	connectOp := []byte("CONNECT {\"verbose\":true,\"pedantic\":true,\"tls_required\":false}\r\n")
	err := c.parse(connectOp)
	if err != nil {
		t.Fatalf("Received error: %v\n", err)
	}
	if c.state != OP_START {
		t.Fatalf("Expected state of OP_START vs %d\n", c.state)
	}
	if !reflect.DeepEqual(c.opts, clientOpts{Echo: true, Verbose: true, Pedantic: true, Protocol: ClientProtoZero}) {
		t.Fatalf("Did not parse connect options correctly: %+v\n", c.opts)
	}

	// ProtoInfo
	connectOp = []byte(fmt.Sprintf("CONNECT {\"verbose\":true,\"pedantic\":true,\"tls_required\":false,\"protocol\":%d}\r\n", ClientProtoInfo))
	err = c.parse(connectOp)
	if err != nil {
		t.Fatalf("Received error: %v\n", err)
	}
	if c.state != OP_START {
		t.Fatalf("Expected state of OP_START vs %d\n", c.state)
	}
	if !reflect.DeepEqual(c.opts, clientOpts{Echo: true, Verbose: true, Pedantic: true, Protocol: ClientProtoInfo}) {
		t.Fatalf("Did not parse connect options correctly: %+v\n", c.opts)
	}
	if c.opts.Protocol != ClientProtoInfo {
		t.Fatalf("Protocol should have been set to %v, but is set to %v", ClientProtoInfo, c.opts.Protocol)
	}

	// Illegal Option
	connectOp = []byte("CONNECT {\"protocol\":22}\r\n")
	wg := sync.WaitGroup{}
	wg.Add(1)
	// The client here is using a pipe, we need to be dequeuing
	// data otherwise the server would be blocked trying to send
	// the error back to it.
	go func() {
		defer wg.Done()
		for {
			if _, _, err := r.ReadLine(); err != nil {
				return
			}
		}
	}()
	err = c.parse(connectOp)
	if err == nil {
		t.Fatalf("Expected to receive an error\n")
	}
	if err != ErrBadClientProtocol {
		t.Fatalf("Expected err of %q, got  %q\n", ErrBadClientProtocol, err)
	}
	wg.Wait()
}

func TestClientPing(t *testing.T) {
	_, c, cr := setupClient()

	// PING
	pingOp := []byte("PING\r\n")
	go c.parse(pingOp)
	l, err := cr.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving info from server: %v\n", err)
	}
	if !strings.HasPrefix(l, "PONG\r\n") {
		t.Fatalf("PONG response incorrect: %s\n", l)
	}
}

var msgPat = regexp.MustCompile(`\AMSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\r\n`)

const (
	SUB_INDEX   = 1
	SID_INDEX   = 2
	REPLY_INDEX = 4
	LEN_INDEX   = 5
)

func checkPayload(cr *bufio.Reader, expected []byte, t *testing.T) {
	// Read in payload
	d := make([]byte, len(expected))
	n, err := cr.Read(d)
	if err != nil {
		t.Fatalf("Error receiving msg payload from server: %v\n", err)
	}
	if n != len(expected) {
		t.Fatalf("Did not read correct amount of bytes: %d vs %d\n", n, len(expected))
	}
	if !bytes.Equal(d, expected) {
		t.Fatalf("Did not read correct payload:: <%s>\n", d)
	}
}

func TestClientSimplePubSub(t *testing.T) {
	_, c, cr := setupClient()
	// SUB/PUB
	go c.parse([]byte("SUB foo 1\r\nPUB foo 5\r\nhello\r\nPING\r\n"))
	l, err := cr.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving msg from server: %v\n", err)
	}
	matches := msgPat.FindAllStringSubmatch(l, -1)[0]
	if len(matches) != 6 {
		t.Fatalf("Did not get correct # matches: %d vs %d\n", len(matches), 6)
	}
	if matches[SUB_INDEX] != "foo" {
		t.Fatalf("Did not get correct subject: '%s'\n", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'\n", matches[SID_INDEX])
	}
	if matches[LEN_INDEX] != "5" {
		t.Fatalf("Did not get correct msg length: '%s'\n", matches[LEN_INDEX])
	}
	checkPayload(cr, []byte("hello\r\n"), t)
}

func TestClientPubSubNoEcho(t *testing.T) {
	_, c, cr := setupClient()
	// Specify no echo
	connectOp := []byte("CONNECT {\"echo\":false}\r\n")
	err := c.parse(connectOp)
	if err != nil {
		t.Fatalf("Received error: %v\n", err)
	}
	// SUB/PUB
	go c.parse([]byte("SUB foo 1\r\nPUB foo 5\r\nhello\r\nPING\r\n"))
	l, err := cr.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving msg from server: %v\n", err)
	}
	// We should not receive anything but a PONG since we specified no echo.
	if !strings.HasPrefix(l, "PONG\r\n") {
		t.Fatalf("PONG response incorrect: %q\n", l)
	}
}

func TestClientSimplePubSubWithReply(t *testing.T) {
	_, c, cr := setupClient()

	// SUB/PUB
	go c.parse([]byte("SUB foo 1\r\nPUB foo bar 5\r\nhello\r\nPING\r\n"))
	l, err := cr.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving msg from server: %v\n", err)
	}
	matches := msgPat.FindAllStringSubmatch(l, -1)[0]
	if len(matches) != 6 {
		t.Fatalf("Did not get correct # matches: %d vs %d\n", len(matches), 6)
	}
	if matches[SUB_INDEX] != "foo" {
		t.Fatalf("Did not get correct subject: '%s'\n", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'\n", matches[SID_INDEX])
	}
	if matches[REPLY_INDEX] != "bar" {
		t.Fatalf("Did not get correct reply subject: '%s'\n", matches[REPLY_INDEX])
	}
	if matches[LEN_INDEX] != "5" {
		t.Fatalf("Did not get correct msg length: '%s'\n", matches[LEN_INDEX])
	}
}

func TestClientNoBodyPubSubWithReply(t *testing.T) {
	_, c, cr := setupClient()

	// SUB/PUB
	go c.parse([]byte("SUB foo 1\r\nPUB foo bar 0\r\n\r\nPING\r\n"))
	l, err := cr.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving msg from server: %v\n", err)
	}
	matches := msgPat.FindAllStringSubmatch(l, -1)[0]
	if len(matches) != 6 {
		t.Fatalf("Did not get correct # matches: %d vs %d\n", len(matches), 6)
	}
	if matches[SUB_INDEX] != "foo" {
		t.Fatalf("Did not get correct subject: '%s'\n", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'\n", matches[SID_INDEX])
	}
	if matches[REPLY_INDEX] != "bar" {
		t.Fatalf("Did not get correct reply subject: '%s'\n", matches[REPLY_INDEX])
	}
	if matches[LEN_INDEX] != "0" {
		t.Fatalf("Did not get correct msg length: '%s'\n", matches[LEN_INDEX])
	}
}

func (c *client) parseFlushAndClose(op []byte) {
	c.parse(op)
	for cp := range c.pcd {
		cp.mu.Lock()
		cp.flushOutbound()
		cp.mu.Unlock()
	}
	c.nc.Close()
}

func TestClientPubWithQueueSub(t *testing.T) {
	_, c, cr := setupClient()

	num := 100

	// Queue SUB/PUB
	subs := []byte("SUB foo g1 1\r\nSUB foo g1 2\r\n")
	pubs := []byte("PUB foo bar 5\r\nhello\r\n")
	op := []byte{}
	op = append(op, subs...)
	for i := 0; i < num; i++ {
		op = append(op, pubs...)
	}

	go c.parseFlushAndClose(op)

	var n1, n2, received int
	for ; ; received++ {
		l, err := cr.ReadString('\n')
		if err != nil {
			break
		}
		matches := msgPat.FindAllStringSubmatch(l, -1)[0]

		// Count which sub
		switch matches[SID_INDEX] {
		case "1":
			n1++
		case "2":
			n2++
		}
		checkPayload(cr, []byte("hello\r\n"), t)
	}
	if received != num {
		t.Fatalf("Received wrong # of msgs: %d vs %d\n", received, num)
	}
	// Threshold for randomness for now
	if n1 < 20 || n2 < 20 {
		t.Fatalf("Received wrong # of msgs per subscriber: %d - %d\n", n1, n2)
	}
}

func TestClientPubWithQueueSubNoEcho(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	nc1, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc1.Close()

	// Grab the client from server and set no echo by hand.
	s.mu.Lock()
	lc := len(s.clients)
	c := s.clients[s.gcid]
	s.mu.Unlock()

	if lc != 1 {
		t.Fatalf("Expected only 1 client but got %d\n", lc)
	}
	if c == nil {
		t.Fatal("Expected to retrieve client\n")
	}
	c.mu.Lock()
	c.echo = false
	c.mu.Unlock()

	// Queue sub on nc1.
	_, err = nc1.QueueSubscribe("foo", "bar", func(*nats.Msg) {})
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	nc1.Flush()

	nc2, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	n := int32(0)
	cb := func(m *nats.Msg) {
		atomic.AddInt32(&n, 1)
	}

	_, err = nc2.QueueSubscribe("foo", "bar", cb)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	nc2.Flush()

	// Now publish 100 messages on nc1 which does not allow echo.
	for i := 0; i < 100; i++ {
		nc1.Publish("foo", []byte("Hello"))
	}
	nc1.Flush()
	nc2.Flush()

	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		num := atomic.LoadInt32(&n)
		if num != int32(100) {
			return fmt.Errorf("Expected all the msgs to be received by nc2, got %d\n", num)
		}
		return nil
	})
}

func TestClientUnSub(t *testing.T) {
	_, c, cr := setupClient()

	num := 1

	// SUB/PUB
	subs := []byte("SUB foo 1\r\nSUB foo 2\r\n")
	unsub := []byte("UNSUB 1\r\n")
	pub := []byte("PUB foo bar 5\r\nhello\r\n")

	op := []byte{}
	op = append(op, subs...)
	op = append(op, unsub...)
	op = append(op, pub...)

	go c.parseFlushAndClose(op)

	var received int
	for ; ; received++ {
		l, err := cr.ReadString('\n')
		if err != nil {
			break
		}
		matches := msgPat.FindAllStringSubmatch(l, -1)[0]
		if matches[SID_INDEX] != "2" {
			t.Fatalf("Received msg on unsubscribed subscription!\n")
		}
		checkPayload(cr, []byte("hello\r\n"), t)
	}
	if received != num {
		t.Fatalf("Received wrong # of msgs: %d vs %d\n", received, num)
	}
}

func TestClientUnSubMax(t *testing.T) {
	_, c, cr := setupClient()

	num := 10
	exp := 5

	// SUB/PUB
	subs := []byte("SUB foo 1\r\n")
	unsub := []byte("UNSUB 1 5\r\n")
	pub := []byte("PUB foo bar 5\r\nhello\r\n")

	op := []byte{}
	op = append(op, subs...)
	op = append(op, unsub...)
	for i := 0; i < num; i++ {
		op = append(op, pub...)
	}

	go c.parseFlushAndClose(op)

	var received int
	for ; ; received++ {
		l, err := cr.ReadString('\n')
		if err != nil {
			break
		}
		matches := msgPat.FindAllStringSubmatch(l, -1)[0]
		if matches[SID_INDEX] != "1" {
			t.Fatalf("Received msg on unsubscribed subscription!\n")
		}
		checkPayload(cr, []byte("hello\r\n"), t)
	}
	if received != exp {
		t.Fatalf("Received wrong # of msgs: %d vs %d\n", received, exp)
	}
}

func TestClientAutoUnsubExactReceived(t *testing.T) {
	_, c, _ := setupClient()
	defer c.nc.Close()

	// SUB/PUB
	subs := []byte("SUB foo 1\r\n")
	unsub := []byte("UNSUB 1 1\r\n")
	pub := []byte("PUB foo bar 2\r\nok\r\n")

	op := []byte{}
	op = append(op, subs...)
	op = append(op, unsub...)
	op = append(op, pub...)

	ch := make(chan bool)
	go func() {
		c.parse(op)
		ch <- true
	}()

	// Wait for processing
	<-ch

	// We should not have any subscriptions in place here.
	if len(c.subs) != 0 {
		t.Fatalf("Wrong number of subscriptions: expected 0, got %d\n", len(c.subs))
	}
}

func TestClientUnsubAfterAutoUnsub(t *testing.T) {
	_, c, _ := setupClient()
	defer c.nc.Close()

	// SUB/UNSUB/UNSUB
	subs := []byte("SUB foo 1\r\n")
	asub := []byte("UNSUB 1 1\r\n")
	unsub := []byte("UNSUB 1\r\n")

	op := []byte{}
	op = append(op, subs...)
	op = append(op, asub...)
	op = append(op, unsub...)

	ch := make(chan bool)
	go func() {
		c.parse(op)
		ch <- true
	}()

	// Wait for processing
	<-ch

	// We should not have any subscriptions in place here.
	if len(c.subs) != 0 {
		t.Fatalf("Wrong number of subscriptions: expected 0, got %d\n", len(c.subs))
	}
}

func TestClientRemoveSubsOnDisconnect(t *testing.T) {
	s, c, _ := setupClient()
	subs := []byte("SUB foo 1\r\nSUB bar 2\r\n")

	ch := make(chan bool)
	go func() {
		c.parse(subs)
		ch <- true
	}()
	<-ch

	if s.sl.Count() != 2 {
		t.Fatalf("Should have 2 subscriptions, got %d\n", s.sl.Count())
	}
	c.closeConnection(ClientClosed)
	if s.sl.Count() != 0 {
		t.Fatalf("Should have no subscriptions after close, got %d\n", s.sl.Count())
	}
}

func TestClientDoesNotAddSubscriptionsWhenConnectionClosed(t *testing.T) {
	s, c, _ := setupClient()
	c.closeConnection(ClientClosed)
	subs := []byte("SUB foo 1\r\nSUB bar 2\r\n")

	ch := make(chan bool)
	go func() {
		c.parse(subs)
		ch <- true
	}()
	<-ch

	if s.sl.Count() != 0 {
		t.Fatalf("Should have no subscriptions after close, got %d\n", s.sl.Count())
	}
}

func TestClientMapRemoval(t *testing.T) {
	s, c, _ := setupClient()
	c.nc.Close()

	checkClientsCount(t, s, 0)
}

func TestAuthorizationTimeout(t *testing.T) {
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
	client := bufio.NewReaderSize(conn, maxBufSize)
	if _, err := client.ReadString('\n'); err != nil {
		t.Fatalf("Error receiving info from server: %v\n", err)
	}
	time.Sleep(3 * secondsToDuration(serverOptions.AuthTimeout))
	l, err := client.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving info from server: %v\n", err)
	}
	if !strings.Contains(l, "Authorization Timeout") {
		t.Fatalf("Authorization Timeout response incorrect: %q\n", l)
	}
}

// This is from bug report #18
func TestTwoTokenPubMatchSingleTokenSub(t *testing.T) {
	_, c, cr := setupClient()
	test := []byte("PUB foo.bar 5\r\nhello\r\nSUB foo 1\r\nPING\r\nPUB foo.bar 5\r\nhello\r\nPING\r\n")
	go c.parse(test)
	l, err := cr.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving info from server: %v\n", err)
	}
	if !strings.HasPrefix(l, "PONG\r\n") {
		t.Fatalf("PONG response incorrect: %q\n", l)
	}
	// Expect just a pong, no match should exist here..
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "PONG\r\n") {
		t.Fatalf("PONG response was expected, got: %q\n", l)
	}
}

func TestUnsubRace(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	url := fmt.Sprintf("nats://%s:%d",
		s.getOpts().Host,
		s.Addr().(*net.TCPAddr).Port,
	)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error creating client to %s: %v\n", url, err)
	}
	defer nc.Close()

	ncp, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer ncp.Close()

	sub, _ := nc.Subscribe("foo", func(m *nats.Msg) {
		// Just eat it..
	})
	nc.Flush()

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		for i := 0; i < 10000; i++ {
			ncp.Publish("foo", []byte("hello"))
		}
		wg.Done()
	}()

	time.Sleep(5 * time.Millisecond)

	sub.Unsubscribe()

	wg.Wait()
}

func TestTLSCloseClientConnection(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/tls.conf")
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	opts.TLSTimeout = 100
	opts.NoLog = true
	opts.NoSigs = true
	s := RunServer(opts)
	defer s.Shutdown()

	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	conn, err := net.DialTimeout("tcp", endpoint, 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on dial: %v", err)
	}
	defer conn.Close()
	br := bufio.NewReaderSize(conn, 100)
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("Unexpected error reading INFO: %v", err)
	}

	tlsConn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
	defer tlsConn.Close()
	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("Unexpected error during handshake: %v", err)
	}
	br = bufio.NewReaderSize(tlsConn, 100)
	connectOp := []byte("CONNECT {\"user\":\"derek\",\"pass\":\"foo\",\"verbose\":false,\"pedantic\":false,\"tls_required\":true}\r\n")
	if _, err := tlsConn.Write(connectOp); err != nil {
		t.Fatalf("Unexpected error writing CONNECT: %v", err)
	}
	if _, err := tlsConn.Write([]byte("PING\r\n")); err != nil {
		t.Fatalf("Unexpected error writing PING: %v", err)
	}
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("Unexpected error reading PONG: %v", err)
	}

	// Check that client is registered.
	checkClientsCount(t, s, 1)
	var cli *client
	s.mu.Lock()
	for _, c := range s.clients {
		cli = c
		break
	}
	s.mu.Unlock()
	if cli == nil {
		t.Fatal("Did not register client on time")
	}
	// Test GetTLSConnectionState
	state := cli.GetTLSConnectionState()
	if state == nil {
		t.Error("GetTLSConnectionState() returned nil")
	}
	// Fill the buffer. Need to send 1 byte at a time so that we timeout here
	// the nc.Close() would block due to a write that can not complete.
	done := false
	for !done {
		cli.nc.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err := cli.nc.Write([]byte("a")); err != nil {
			done = true
		}
		cli.nc.SetWriteDeadline(time.Time{})
	}
	ch := make(chan bool)
	go func() {
		select {
		case <-ch:
			return
		case <-time.After(3 * time.Second):
			fmt.Println("!!!! closeConnection is blocked, test will hang !!!")
			return
		}
	}()
	// Close the client
	cli.closeConnection(ClientClosed)
	ch <- true
}

// This tests issue #558
func TestWildcardCharsInLiteralSubjectWorks(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	ch := make(chan bool, 1)
	// This subject is a literal even though it contains `*` and `>`,
	// they are not treated as wildcards.
	subj := "foo.bar,*,>,baz"
	cb := func(_ *nats.Msg) {
		ch <- true
	}
	for i := 0; i < 2; i++ {
		sub, err := nc.Subscribe(subj, cb)
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatalf("Error on flush: %v", err)
		}
		if err := nc.LastError(); err != nil {
			t.Fatalf("Server reported error: %v", err)
		}
		if err := nc.Publish(subj, []byte("msg")); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("Should have received the message")
		}
		if err := sub.Unsubscribe(); err != nil {
			t.Fatalf("Error on unsubscribe: %v", err)
		}
	}
}

func TestDynamicBuffers(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Grab the client from server.
	s.mu.Lock()
	lc := len(s.clients)
	c := s.clients[s.gcid]
	s.mu.Unlock()

	if lc != 1 {
		t.Fatalf("Expected only 1 client but got %d\n", lc)
	}
	if c == nil {
		t.Fatal("Expected to retrieve client\n")
	}

	// Create some helper functions and data structures.
	done := make(chan bool)          // Used to stop recording.
	type maxv struct{ rsz, wsz int } // Used to hold max values.
	results := make(chan maxv)

	// stopRecording stops the recording ticker and releases go routine.
	stopRecording := func() maxv {
		done <- true
		return <-results
	}
	// max just grabs max values.
	max := func(a, b int) int {
		if a > b {
			return a
		}
		return b
	}
	// Returns current value of the buffer sizes.
	getBufferSizes := func() (int, int) {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.in.rsz, c.out.sz
	}
	// Record the max values seen.
	recordMaxBufferSizes := func() {
		ticker := time.NewTicker(10 * time.Microsecond)
		defer ticker.Stop()

		var m maxv

		recordMax := func() {
			rsz, wsz := getBufferSizes()
			m.rsz = max(m.rsz, rsz)
			m.wsz = max(m.wsz, wsz)
		}

		for {
			select {
			case <-done:
				recordMax()
				results <- m
				return
			case <-ticker.C:
				recordMax()
			}
		}
	}
	// Check that the current value is what we expected.
	checkBuffers := func(ers, ews int) {
		t.Helper()
		rsz, wsz := getBufferSizes()
		if rsz != ers {
			t.Fatalf("Expected read buffer of %d, but got %d\n", ers, rsz)
		}
		if wsz != ews {
			t.Fatalf("Expected write buffer of %d, but got %d\n", ews, wsz)
		}
	}

	// Check that the max was as expected.
	checkResults := func(m maxv, rsz, wsz int) {
		t.Helper()
		if rsz != m.rsz {
			t.Fatalf("Expected read buffer of %d, but got %d\n", rsz, m.rsz)
		}
		if wsz != m.wsz {
			t.Fatalf("Expected write buffer of %d, but got %d\n", wsz, m.wsz)
		}
	}

	// Here is where testing begins..

	// Should be at or below the startBufSize for both.
	rsz, wsz := getBufferSizes()
	if rsz > startBufSize {
		t.Fatalf("Expected read buffer of <= %d, but got %d\n", startBufSize, rsz)
	}
	if wsz > startBufSize {
		t.Fatalf("Expected write buffer of <= %d, but got %d\n", startBufSize, wsz)
	}

	// Send some data.
	data := make([]byte, 2048)
	rand.Read(data)

	go recordMaxBufferSizes()
	for i := 0; i < 200; i++ {
		nc.Publish("foo", data)
	}
	nc.Flush()
	m := stopRecording()

	if m.rsz != maxBufSize && m.rsz != maxBufSize/2 {
		t.Fatalf("Expected read buffer of %d or %d, but got %d\n", maxBufSize, maxBufSize/2, m.rsz)
	}
	if m.wsz > startBufSize {
		t.Fatalf("Expected write buffer of <= %d, but got %d\n", startBufSize, m.wsz)
	}

	// Create Subscription to test outbound buffer from server.
	nc.Subscribe("foo", func(m *nats.Msg) {
		// Just eat it..
	})
	go recordMaxBufferSizes()

	for i := 0; i < 200; i++ {
		nc.Publish("foo", data)
	}
	nc.Flush()

	m = stopRecording()
	checkResults(m, maxBufSize, maxBufSize)

	// Now test that we shrink correctly.

	// Should go to minimum for both..
	for i := 0; i < 20; i++ {
		nc.Flush()
	}
	checkBuffers(minBufSize, minBufSize)
}

// Similar to the routed version. Make sure we receive all of the
// messages with auto-unsubscribe enabled.
func TestQueueAutoUnsubscribe(t *testing.T) {
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
	for i := 0; i < 1000; i++ {
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

	expected := int32(1000)
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
