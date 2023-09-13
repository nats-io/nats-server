// Copyright 2012-2022 The NATS Authors
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
	"io"
	"math"
	"net"
	"net/url"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crypto/tls"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

type serverInfo struct {
	ID           string   `json:"server_id"`
	Host         string   `json:"host"`
	Port         uint     `json:"port"`
	Version      string   `json:"version"`
	AuthRequired bool     `json:"auth_required"`
	TLSRequired  bool     `json:"tls_required"`
	MaxPayload   int64    `json:"max_payload"`
	Headers      bool     `json:"headers"`
	ConnectURLs  []string `json:"connect_urls,omitempty"`
	LameDuckMode bool     `json:"ldm,omitempty"`
	CID          uint64   `json:"client_id,omitempty"`
}

type testAsyncClient struct {
	*client
	parseAsync func(string)
	quitCh     chan bool
}

func (c *testAsyncClient) close() {
	c.client.closeConnection(ClientClosed)
	c.quitCh <- true
}

func (c *testAsyncClient) parse(proto []byte) error {
	err := c.client.parse(proto)
	c.client.flushClients(0)
	return err
}

func (c *testAsyncClient) parseAndClose(proto []byte) {
	c.client.parse(proto)
	c.client.flushClients(0)
	c.closeConnection(ClientClosed)
}

func createClientAsync(ch chan *client, s *Server, cli net.Conn) {
	// Normally, those type of clients are used against non running servers.
	// However, some don't, which would then cause the writeLoop to be
	// started twice for the same client (since createClient() start both
	// read and write loop if it is detected as running).
	startWriteLoop := !s.isRunning()
	if startWriteLoop {
		s.grWG.Add(1)
	}
	go func() {
		c := s.createClient(cli)
		// Must be here to suppress +OK
		c.opts.Verbose = false
		if startWriteLoop {
			go c.writeLoop()
		}
		ch <- c
	}()
}

func newClientForServer(s *Server) (*testAsyncClient, *bufio.Reader, string) {
	cli, srv := net.Pipe()
	cr := bufio.NewReaderSize(cli, maxBufSize)
	ch := make(chan *client)
	createClientAsync(ch, s, srv)
	// So failing tests don't just hang.
	cli.SetReadDeadline(time.Now().Add(10 * time.Second))
	l, _ := cr.ReadString('\n')
	// Grab client
	c := <-ch
	parse, quitCh := genAsyncParser(c)
	asyncClient := &testAsyncClient{
		client:     c,
		parseAsync: parse,
		quitCh:     quitCh,
	}
	return asyncClient, cr, l
}

func genAsyncParser(c *client) (func(string), chan bool) {
	pab := make(chan []byte, 16)
	pas := func(cs string) { pab <- []byte(cs) }
	quit := make(chan bool)
	go func() {
		for {
			select {
			case cs := <-pab:
				c.parse(cs)
				c.flushClients(0)
			case <-quit:
				return
			}
		}
	}()
	return pas, quit
}

var defaultServerOptions = Options{
	Host:                  "127.0.0.1",
	Port:                  -1,
	Trace:                 true,
	Debug:                 true,
	DisableShortFirstPing: true,
	NoLog:                 true,
	NoSigs:                true,
}

func rawSetup(serverOptions Options) (*Server, *testAsyncClient, *bufio.Reader, string) {
	s := New(&serverOptions)
	c, cr, l := newClientForServer(s)
	return s, c, cr, l
}

func setUpClientWithResponse() (*testAsyncClient, string) {
	_, c, _, l := rawSetup(defaultServerOptions)
	return c, l
}

func setupClient() (*Server, *testAsyncClient, *bufio.Reader) {
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

func checkAccClientsCount(t *testing.T, acc *Account, expected int) {
	t.Helper()
	checkFor(t, 4*time.Second, 10*time.Millisecond, func() error {
		if nc := acc.NumConnections(); nc != expected {
			return fmt.Errorf("Expected account %q to have %v clients, got %v",
				acc.Name, expected, nc)
		}
		return nil
	})
}

func TestAsyncClientWithRunningServer(t *testing.T) {
	o := DefaultOptions()
	s := RunServer(o)
	defer s.Shutdown()

	c, _, _ := newClientForServer(s)
	defer c.close()

	buf := make([]byte, 1000000)
	writeLoopTxt := fmt.Sprintf("writeLoop(%p)", c.client)
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		n := runtime.Stack(buf, true)
		if count := strings.Count(string(buf[:n]), writeLoopTxt); count != 1 {
			return fmt.Errorf("writeLoop for client should have been started only once: %v", count)
		}
		return nil
	})
}

func TestClientCreateAndInfo(t *testing.T) {
	s, c, _, l := rawSetup(defaultServerOptions)
	defer c.close()

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
		int(info.Port) != s.opts.Port {
		t.Fatalf("INFO inconsistent: %+v\n", info)
	}
}

func TestClientNoResponderSupport(t *testing.T) {
	opts := defaultServerOptions
	s := New(&opts)

	c, _, _ := newClientForServer(s)
	defer c.close()

	// Force header support if you want to do no_responders. Make sure headers are set.
	if err := c.parse([]byte("CONNECT {\"no_responders\":true}\r\n")); err == nil {
		t.Fatalf("Expected error")
	}

	c, cr, _ := newClientForServer(s)
	defer c.close()

	c.parseAsync("CONNECT {\"headers\":true, \"no_responders\":true}\r\nSUB reply 1\r\nPUB foo reply 2\r\nok\r\n")

	l, err := cr.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving msg from server: %v\n", err)
	}

	am := hmsgPat.FindAllStringSubmatch(l, -1)
	if len(am) == 0 {
		t.Fatalf("Did not get a match for %q", l)
	}
	checkPayload(cr, []byte("NATS/1.0 503\r\n\r\n"), t)
}

func TestServerHeaderSupport(t *testing.T) {
	opts := defaultServerOptions
	s := New(&opts)

	c, _, l := newClientForServer(s)
	defer c.close()

	if !strings.HasPrefix(l, "INFO ") {
		t.Fatalf("INFO response incorrect: %s\n", l)
	}
	var info serverInfo
	if err := json.Unmarshal([]byte(l[5:]), &info); err != nil {
		t.Fatalf("Could not parse INFO json: %v\n", err)
	}
	if !info.Headers {
		t.Fatalf("Expected by default for header support to be enabled")
	}

	opts.NoHeaderSupport = true
	opts.Port = -1
	s = New(&opts)

	c, _, l = newClientForServer(s)
	defer c.close()

	if err := json.Unmarshal([]byte(l[5:]), &info); err != nil {
		t.Fatalf("Could not parse INFO json: %v\n", err)
	}
	if info.Headers {
		t.Fatalf("Expected header support to be disabled")
	}
}

// This test specifically is not testing how headers are encoded in a raw msg.
// It wants to make sure the serve and clients agreement on when to use headers
// is bi-directional and functions properly.
func TestClientHeaderSupport(t *testing.T) {
	opts := defaultServerOptions
	s := New(&opts)

	c, _, _ := newClientForServer(s)
	defer c.close()

	// Even though the server supports headers we need to explicitly say we do in the
	// CONNECT. If we do not we should get an error.
	if err := c.parse([]byte("CONNECT {}\r\nHPUB foo 0 2\r\nok\r\n")); err != ErrMsgHeadersNotSupported {
		t.Fatalf("Expected to receive an error, got %v", err)
	}

	// This should succeed.
	c, _, _ = newClientForServer(s)
	defer c.close()

	if err := c.parse([]byte("CONNECT {\"headers\":true}\r\nHPUB foo 0 2\r\nok\r\n")); err != nil {
		t.Fatalf("Unexpected error %v", err)
	}

	// Now start a server without support.
	opts.NoHeaderSupport = true
	opts.Port = -1
	s = New(&opts)

	c, _, _ = newClientForServer(s)
	defer c.close()
	if err := c.parse([]byte("CONNECT {\"headers\":true}\r\nHPUB foo 0 2\r\nok\r\n")); err != ErrMsgHeadersNotSupported {
		t.Fatalf("Expected to receive an error, got %v", err)
	}
}

var hmsgPat = regexp.MustCompile(`HMSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)[^\S\r\n]+(\d+)\r\n`)

func TestClientHeaderDeliverMsg(t *testing.T) {
	opts := defaultServerOptions
	s := New(&opts)

	c, cr, _ := newClientForServer(s)
	defer c.close()

	connect := "CONNECT {\"headers\":true}"
	subOp := "SUB foo 1"
	pubOp := "HPUB foo 12 14\r\nName:Derek\r\nOK\r\n"
	cmd := strings.Join([]string{connect, subOp, pubOp}, "\r\n")

	c.parseAsync(cmd)
	l, err := cr.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving msg from server: %v\n", err)
	}

	am := hmsgPat.FindAllStringSubmatch(l, -1)
	if len(am) == 0 {
		t.Fatalf("Did not get a match for %q", l)
	}
	matches := am[0]
	if len(matches) != 7 {
		t.Fatalf("Did not get correct # matches: %d vs %d\n", len(matches), 7)
	}
	if matches[SUB_INDEX] != "foo" {
		t.Fatalf("Did not get correct subject: '%s'\n", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'\n", matches[SID_INDEX])
	}
	if matches[HDR_INDEX] != "12" {
		t.Fatalf("Did not get correct msg length: '%s'\n", matches[HDR_INDEX])
	}
	if matches[TLEN_INDEX] != "14" {
		t.Fatalf("Did not get correct msg length: '%s'\n", matches[TLEN_INDEX])
	}
	checkPayload(cr, []byte("Name:Derek\r\nOK\r\n"), t)
}

var smsgPat = regexp.MustCompile(`^MSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\r\n`)

func TestClientHeaderDeliverStrippedMsg(t *testing.T) {
	opts := defaultServerOptions
	s := New(&opts)

	c, _, _ := newClientForServer(s)
	defer c.close()

	b, br, _ := newClientForServer(s)
	defer b.close()

	// Does not support headers
	b.parseAsync("SUB foo 1\r\nPING\r\n")
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("Error receiving msg from server: %v\n", err)
	}

	connect := "CONNECT {\"headers\":true}"
	pubOp := "HPUB foo 12 14\r\nName:Derek\r\nOK\r\n"
	cmd := strings.Join([]string{connect, pubOp}, "\r\n")
	c.parseAsync(cmd)
	// Read from 'b' client.
	l, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving msg from server: %v\n", err)
	}
	am := smsgPat.FindAllStringSubmatch(l, -1)
	if len(am) == 0 {
		t.Fatalf("Did not get a correct match for %q", l)
	}
	matches := am[0]
	if len(matches) != 6 {
		t.Fatalf("Did not get correct # matches: %d vs %d\n", len(matches), 6)
	}
	if matches[SUB_INDEX] != "foo" {
		t.Fatalf("Did not get correct subject: '%s'\n", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'\n", matches[SID_INDEX])
	}
	if matches[LEN_INDEX] != "2" {
		t.Fatalf("Did not get correct msg length: '%s'\n", matches[LEN_INDEX])
	}
	checkPayload(br, []byte("OK\r\n"), t)
	if br.Buffered() != 0 {
		t.Fatalf("Expected no extra bytes to be buffered, got %d", br.Buffered())
	}
}

func TestClientHeaderDeliverQueueSubStrippedMsg(t *testing.T) {
	opts := defaultServerOptions
	s := New(&opts)

	c, _, _ := newClientForServer(s)
	defer c.close()

	b, br, _ := newClientForServer(s)
	defer b.close()

	// Does not support headers
	b.parseAsync("SUB foo bar 1\r\nPING\r\n")
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("Error receiving msg from server: %v\n", err)
	}

	connect := "CONNECT {\"headers\":true}"
	pubOp := "HPUB foo 12 14\r\nName:Derek\r\nOK\r\n"
	cmd := strings.Join([]string{connect, pubOp}, "\r\n")
	c.parseAsync(cmd)
	// Read from 'b' client.
	l, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving msg from server: %v\n", err)
	}
	am := smsgPat.FindAllStringSubmatch(l, -1)
	if len(am) == 0 {
		t.Fatalf("Did not get a correct match for %q", l)
	}
	matches := am[0]
	if len(matches) != 6 {
		t.Fatalf("Did not get correct # matches: %d vs %d\n", len(matches), 6)
	}
	if matches[SUB_INDEX] != "foo" {
		t.Fatalf("Did not get correct subject: '%s'\n", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'\n", matches[SID_INDEX])
	}
	if matches[LEN_INDEX] != "2" {
		t.Fatalf("Did not get correct msg length: '%s'\n", matches[LEN_INDEX])
	}
	checkPayload(br, []byte("OK\r\n"), t)
}

func TestNonTLSConnectionState(t *testing.T) {
	_, c, _ := setupClient()
	defer c.close()
	state := c.GetTLSConnectionState()
	if state != nil {
		t.Error("GetTLSConnectionState() returned non-nil")
	}
}

func TestClientConnect(t *testing.T) {
	_, c, _ := setupClient()
	defer c.close()

	// Basic Connect setting flags
	connectOp := []byte("CONNECT {\"verbose\":true,\"pedantic\":true,\"tls_required\":false,\"echo\":false}\r\n")
	err := c.parse(connectOp)
	if err != nil {
		t.Fatalf("Received error: %v\n", err)
	}
	if c.state != OP_START {
		t.Fatalf("Expected state of OP_START vs %d\n", c.state)
	}
	if !reflect.DeepEqual(c.opts, ClientOpts{Verbose: true, Pedantic: true, Echo: false}) {
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
	if !reflect.DeepEqual(c.opts, ClientOpts{Echo: true, Verbose: true, Pedantic: true, Username: "derek", Password: "foo"}) {
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

	if !reflect.DeepEqual(c.opts, ClientOpts{Echo: true, Verbose: true, Pedantic: true, Username: "derek", Password: "foo", Name: "router"}) {
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

	if !reflect.DeepEqual(c.opts, ClientOpts{Echo: true, Verbose: true, Pedantic: true, Token: "YZZ222", Name: "router"}) {
		t.Fatalf("Did not parse connect options correctly: %+v\n", c.opts)
	}
}

func TestClientConnectProto(t *testing.T) {
	_, c, r := setupClient()
	defer c.close()

	// Basic Connect setting flags, proto should be zero (original proto)
	connectOp := []byte("CONNECT {\"verbose\":true,\"pedantic\":true,\"tls_required\":false}\r\n")
	err := c.parse(connectOp)
	if err != nil {
		t.Fatalf("Received error: %v\n", err)
	}
	if c.state != OP_START {
		t.Fatalf("Expected state of OP_START vs %d\n", c.state)
	}
	if !reflect.DeepEqual(c.opts, ClientOpts{Echo: true, Verbose: true, Pedantic: true, Protocol: ClientProtoZero}) {
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
	if !reflect.DeepEqual(c.opts, ClientOpts{Echo: true, Verbose: true, Pedantic: true, Protocol: ClientProtoInfo}) {
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

func TestRemoteAddress(t *testing.T) {
	rc := &client{}

	// though in reality this will panic if it does not, adding coverage anyway
	if rc.RemoteAddress() != nil {
		t.Errorf("RemoteAddress() did not handle nil connection correctly")
	}

	_, c, _ := setupClient()
	defer c.close()
	addr := c.RemoteAddress()

	if addr.Network() != "pipe" {
		t.Errorf("RemoteAddress() returned invalid network: %s", addr.Network())
	}

	if addr.String() != "pipe" {
		t.Errorf("RemoteAddress() returned invalid string: %s", addr.String())
	}
}

func TestClientPing(t *testing.T) {
	_, c, cr := setupClient()
	defer c.close()

	// PING
	pingOp := "PING\r\n"
	c.parseAsync(pingOp)
	l, err := cr.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving info from server: %v\n", err)
	}
	if !strings.HasPrefix(l, "PONG\r\n") {
		t.Fatalf("PONG response incorrect: %s\n", l)
	}
}

var msgPat = regexp.MustCompile(`MSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\r\n`)

const (
	SUB_INDEX   = 1
	SID_INDEX   = 2
	REPLY_INDEX = 4
	LEN_INDEX   = 5
	HDR_INDEX   = 5
	TLEN_INDEX  = 6
)

func grabPayload(cr *bufio.Reader, expected int) []byte {
	d := make([]byte, expected)
	n, _ := cr.Read(d)
	cr.ReadString('\n')
	return d[:n]
}

func checkPayload(cr *bufio.Reader, expected []byte, t *testing.T) {
	t.Helper()
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
	defer c.close()
	// SUB/PUB
	c.parseAsync("SUB foo 1\r\nPUB foo 5\r\nhello\r\nPING\r\n")
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
	defer c.close()
	// Specify no echo
	connectOp := []byte("CONNECT {\"echo\":false}\r\n")
	err := c.parse(connectOp)
	if err != nil {
		t.Fatalf("Received error: %v\n", err)
	}
	// SUB/PUB
	c.parseAsync("SUB foo 1\r\nPUB foo 5\r\nhello\r\nPING\r\n")
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
	defer c.close()

	// SUB/PUB
	c.parseAsync("SUB foo 1\r\nPUB foo bar 5\r\nhello\r\nPING\r\n")
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
	defer c.close()

	// SUB/PUB
	c.parseAsync("SUB foo 1\r\nPUB foo bar 0\r\n\r\nPING\r\n")
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

func TestClientPubWithQueueSub(t *testing.T) {
	_, c, cr := setupClient()
	defer c.close()

	num := 100

	// Queue SUB/PUB
	subs := []byte("SUB foo g1 1\r\nSUB foo g1 2\r\n")
	pubs := []byte("PUB foo bar 5\r\nhello\r\n")
	op := []byte{}
	op = append(op, subs...)
	for i := 0; i < num; i++ {
		op = append(op, pubs...)
	}

	go c.parseAndClose(op)

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

func TestSplitSubjectQueue(t *testing.T) {
	cases := []struct {
		name        string
		sq          string
		wantSubject []byte
		wantQueue   []byte
		wantErr     bool
	}{
		{name: "single subject",
			sq: "foo", wantSubject: []byte("foo"), wantQueue: nil},
		{name: "subject and queue",
			sq: "foo bar", wantSubject: []byte("foo"), wantQueue: []byte("bar")},
		{name: "subject and queue with surrounding spaces",
			sq: " foo bar ", wantSubject: []byte("foo"), wantQueue: []byte("bar")},
		{name: "subject and queue with extra spaces in the middle",
			sq: "foo  bar", wantSubject: []byte("foo"), wantQueue: []byte("bar")},
		{name: "subject, queue, and extra token",
			sq: "foo  bar fizz", wantSubject: []byte(nil), wantQueue: []byte(nil), wantErr: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sub, que, err := splitSubjectQueue(c.sq)
			if err == nil && c.wantErr {
				t.Fatal("Expected error, but got nil")
			}
			if err != nil && !c.wantErr {
				t.Fatalf("Expected nil error, but got %v", err)
			}

			if !reflect.DeepEqual(sub, c.wantSubject) {
				t.Fatalf("Expected to get subject %#v, but instead got %#v", c.wantSubject, sub)
			}
			if !reflect.DeepEqual(que, c.wantQueue) {
				t.Fatalf("Expected to get queue %#v, but instead got %#v", c.wantQueue, que)
			}
		})
	}
}

func TestTypeString(t *testing.T) {
	cases := []struct {
		intType    int
		stringType string
	}{
		{
			intType:    CLIENT,
			stringType: "Client",
		},
		{
			intType:    ROUTER,
			stringType: "Router",
		},
		{
			intType:    GATEWAY,
			stringType: "Gateway",
		},
		{
			intType:    LEAF,
			stringType: "Leafnode",
		},
		{
			intType:    JETSTREAM,
			stringType: "JetStream",
		},
		{
			intType:    ACCOUNT,
			stringType: "Account",
		},
		{
			intType:    SYSTEM,
			stringType: "System",
		},
		{
			intType:    -1,
			stringType: "Unknown Type",
		},
	}
	for _, cs := range cases {
		c := &client{kind: cs.intType}
		typeStringVal := c.kindString()

		if typeStringVal != cs.stringType {
			t.Fatalf("Expected typeString  value  %q, but instead received %q", cs.stringType, typeStringVal)
		}
	}
}

func TestQueueSubscribePermissions(t *testing.T) {
	cases := []struct {
		name    string
		perms   *SubjectPermission
		subject string
		queue   string
		want    string
	}{
		{
			name:    "plain subscription on foo",
			perms:   &SubjectPermission{Allow: []string{"foo"}},
			subject: "foo",
			want:    "+OK\r\n",
		},
		{
			name:    "queue subscribe with allowed group",
			perms:   &SubjectPermission{Allow: []string{"foo bar"}},
			subject: "foo",
			queue:   "bar",
			want:    "+OK\r\n",
		},
		{
			name:    "queue subscribe with wildcard allowed group",
			perms:   &SubjectPermission{Allow: []string{"foo bar.*"}},
			subject: "foo",
			queue:   "bar.fizz",
			want:    "+OK\r\n",
		},
		{
			name:    "queue subscribe with full wildcard subject and subgroup",
			perms:   &SubjectPermission{Allow: []string{"> bar.>"}},
			subject: "whizz",
			queue:   "bar.bang",
			want:    "+OK\r\n",
		},
		{
			name:    "plain subscribe with full wildcard subject and subgroup",
			perms:   &SubjectPermission{Allow: []string{"> bar.>"}},
			subject: "whizz",
			want:    "-ERR 'Permissions Violation for Subscription to \"whizz\"'\r\n",
		},
		{
			name:    "deny plain subscription on foo",
			perms:   &SubjectPermission{Allow: []string{">"}, Deny: []string{"foo"}},
			subject: "foo",
			queue:   "bar",
			want:    "-ERR 'Permissions Violation for Subscription to \"foo\" using queue \"bar\"'\r\n",
		},
		{
			name:    "allow plain subscription, except foo",
			perms:   &SubjectPermission{Allow: []string{">"}, Deny: []string{"foo"}},
			subject: "bar",
			want:    "+OK\r\n",
		},
		{
			name:    "deny everything",
			perms:   &SubjectPermission{Allow: []string{">"}, Deny: []string{">"}},
			subject: "foo",
			queue:   "bar",
			want:    "-ERR 'Permissions Violation for Subscription to \"foo\" using queue \"bar\"'\r\n",
		},
		{
			name:    "can only subscribe to queues v1",
			perms:   &SubjectPermission{Allow: []string{"> v1.>"}},
			subject: "foo",
			queue:   "v1.prod",
			want:    "+OK\r\n",
		},
		{
			name:    "cannot subscribe to queues, plain subscribe ok",
			perms:   &SubjectPermission{Allow: []string{">"}, Deny: []string{"> >"}},
			subject: "foo",
			want:    "+OK\r\n",
		},
		{
			name:    "cannot subscribe to queues, queue subscribe not ok",
			perms:   &SubjectPermission{Deny: []string{"> >"}},
			subject: "foo",
			queue:   "bar",
			want:    "-ERR 'Permissions Violation for Subscription to \"foo\" using queue \"bar\"'\r\n",
		},
		{
			name:    "deny all queue subscriptions on dev or stg only",
			perms:   &SubjectPermission{Deny: []string{"> *.dev", "> *.stg"}},
			subject: "foo",
			queue:   "bar",
			want:    "+OK\r\n",
		},
		{
			name:    "allow only queue subscription on dev or stg",
			perms:   &SubjectPermission{Allow: []string{"> *.dev", "> *.stg"}},
			subject: "foo",
			queue:   "bar",
			want:    "-ERR 'Permissions Violation for Subscription to \"foo\" using queue \"bar\"'\r\n",
		},
		{
			name:    "deny queue subscriptions with subject foo",
			perms:   &SubjectPermission{Deny: []string{"foo >"}},
			subject: "foo",
			queue:   "bar",
			want:    "-ERR 'Permissions Violation for Subscription to \"foo\" using queue \"bar\"'\r\n",
		},
		{
			name:    "plain sub is allowed, but queue subscribe with queue not in list",
			perms:   &SubjectPermission{Allow: []string{"foo bar"}},
			subject: "foo",
			queue:   "fizz",
			want:    "-ERR 'Permissions Violation for Subscription to \"foo\" using queue \"fizz\"'\r\n",
		},
		{
			name:    "allow plain sub, but do queue subscribe",
			perms:   &SubjectPermission{Allow: []string{"foo"}},
			subject: "foo",
			queue:   "bar",
			want:    "+OK\r\n",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, client, r := setupClient()
			defer client.close()

			client.RegisterUser(&User{
				Permissions: &Permissions{Subscribe: c.perms},
			})
			connect := []byte("CONNECT {\"verbose\":true}\r\n")
			qsub := []byte(fmt.Sprintf("SUB %s %s 1\r\n", c.subject, c.queue))

			go client.parseAndClose(append(connect, qsub...))

			var buf bytes.Buffer
			if _, err := io.Copy(&buf, r); err != nil {
				t.Fatal(err)
			}

			// Extra OK is from the successful CONNECT.
			want := "+OK\r\n" + c.want
			if got := buf.String(); got != want {
				t.Fatalf("Expected to receive %q, but instead received %q", want, got)
			}
		})
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
	defer c.close()

	num := 1

	// SUB/PUB
	subs := []byte("SUB foo 1\r\nSUB foo 2\r\n")
	unsub := []byte("UNSUB 1\r\n")
	pub := []byte("PUB foo bar 5\r\nhello\r\n")

	op := []byte{}
	op = append(op, subs...)
	op = append(op, unsub...)
	op = append(op, pub...)

	go c.parseAndClose(op)

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
	defer c.close()

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

	go c.parseAndClose(op)

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
	defer c.close()

	// SUB/PUB
	subs := []byte("SUB foo 1\r\n")
	unsub := []byte("UNSUB 1 1\r\n")
	pub := []byte("PUB foo bar 2\r\nok\r\n")

	op := []byte{}
	op = append(op, subs...)
	op = append(op, unsub...)
	op = append(op, pub...)

	c.parse(op)

	// We should not have any subscriptions in place here.
	if len(c.subs) != 0 {
		t.Fatalf("Wrong number of subscriptions: expected 0, got %d\n", len(c.subs))
	}
}

func TestClientUnsubAfterAutoUnsub(t *testing.T) {
	_, c, _ := setupClient()
	defer c.close()

	// SUB/UNSUB/UNSUB
	subs := []byte("SUB foo 1\r\n")
	asub := []byte("UNSUB 1 1\r\n")
	unsub := []byte("UNSUB 1\r\n")

	op := []byte{}
	op = append(op, subs...)
	op = append(op, asub...)
	op = append(op, unsub...)

	c.parse(op)

	// We should not have any subscriptions in place here.
	if len(c.subs) != 0 {
		t.Fatalf("Wrong number of subscriptions: expected 0, got %d\n", len(c.subs))
	}
}

func TestClientRemoveSubsOnDisconnect(t *testing.T) {
	s, c, _ := setupClient()
	defer c.close()
	subs := []byte("SUB foo 1\r\nSUB bar 2\r\n")

	c.parse(subs)

	if s.NumSubscriptions() != 2 {
		t.Fatalf("Should have 2 subscriptions, got %d\n", s.NumSubscriptions())
	}
	c.closeConnection(ClientClosed)
	checkExpectedSubs(t, 0, s)
}

func TestClientDoesNotAddSubscriptionsWhenConnectionClosed(t *testing.T) {
	_, c, _ := setupClient()
	c.close()
	subs := []byte("SUB foo 1\r\nSUB bar 2\r\n")

	c.parse(subs)

	if c.acc.sl.Count() != 0 {
		t.Fatalf("Should have no subscriptions after close, got %d\n", c.acc.sl.Count())
	}
}

func TestClientMapRemoval(t *testing.T) {
	s, c, _ := setupClient()
	c.close()

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
	if !strings.Contains(l, "Authentication Timeout") {
		t.Fatalf("Authentication Timeout response incorrect: %q\n", l)
	}
}

// This is from bug report #18
func TestTwoTokenPubMatchSingleTokenSub(t *testing.T) {
	_, c, cr := setupClient()
	defer c.close()
	test := "PUB foo.bar 5\r\nhello\r\nSUB foo 1\r\nPING\r\nPUB foo.bar 5\r\nhello\r\nPING\r\n"
	c.parseAsync(test)
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

func TestClientCloseTLSConnection(t *testing.T) {
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

	// Test RemoteAddress
	addr := cli.RemoteAddress()
	if addr == nil {
		t.Error("RemoteAddress() returned nil")
	}

	if addr.(*net.TCPAddr).IP.String() != "127.0.0.1" {
		t.Error("RemoteAddress() returned incorrect ip " + addr.String())
	}

	// Fill the buffer. We want to timeout on write so that nc.Close()
	// would block due to a write that cannot complete.
	buf := make([]byte, 64*1024)
	done := false
	for !done {
		cli.nc.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err := cli.nc.Write(buf); err != nil {
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

// This test ensures that coalescing into the fixed-size output
// queues works as expected. When bytes are queued up, they should
// not overflow a buffer until the capacity is exceeded, at which
// point a new buffer should be added.
func TestClientOutboundQueueCoalesce(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	clients := s.GlobalAccount().getClients()
	if len(clients) != 1 {
		t.Fatal("Expecting a client to exist")
	}
	client := clients[0]
	client.mu.Lock()
	defer client.mu.Unlock()

	// First up, queue something small into the queue.
	client.queueOutbound([]byte{1, 2, 3, 4, 5})

	if len(client.out.nb) != 1 {
		t.Fatal("Expecting a single queued buffer")
	}
	if l := len(client.out.nb[0]); l != 5 {
		t.Fatalf("Expecting only 5 bytes in the first queued buffer, found %d instead", l)
	}

	// Then queue up a few more bytes, but not enough
	// to overflow into the next buffer.
	client.queueOutbound([]byte{6, 7, 8, 9, 10})

	if len(client.out.nb) != 1 {
		t.Fatal("Expecting a single queued buffer")
	}
	if l := len(client.out.nb[0]); l != 10 {
		t.Fatalf("Expecting 10 bytes in the first queued buffer, found %d instead", l)
	}

	// Finally, queue up something that is guaranteed
	// to overflow.
	b := nbPoolSmall.Get().(*[nbPoolSizeSmall]byte)[:]
	b = b[:cap(b)]
	client.queueOutbound(b)
	if len(client.out.nb) != 2 {
		t.Fatal("Expecting buffer to have overflowed")
	}
	if l := len(client.out.nb[0]); l != cap(b) {
		t.Fatalf("Expecting %d bytes in the first queued buffer, found %d instead", cap(b), l)
	}
	if l := len(client.out.nb[1]); l != 10 {
		t.Fatalf("Expecting 10 bytes in the second queued buffer, found %d instead", l)
	}
}

func TestClientTraceRace(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	// Activate trace logging
	s.SetLogger(&DummyLogger{}, false, true)

	nc1, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc1.Close()
	total := 10000
	count := 0
	ch := make(chan bool, 1)
	if _, err := nc1.Subscribe("foo", func(_ *nats.Msg) {
		count++
		if count == total {
			ch <- true
		}
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	nc2, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			nc1.Publish("bar", []byte("hello"))
		}
	}()
	for i := 0; i < total; i++ {
		nc2.Publish("foo", []byte("hello"))
	}
	if err := wait(ch); err != nil {
		t.Fatal("Did not get all our messages")
	}
	wg.Wait()
}

func TestClientUserInfo(t *testing.T) {
	pnkey := "UD6AYQSOIN2IN5OGC6VQZCR4H3UFMIOXSW6NNS6N53CLJA4PB56CEJJI"
	c := &client{
		cid: 1024,
		opts: ClientOpts{
			Nkey: pnkey,
		},
	}
	got := c.getAuthUser()
	expected := `Nkey "UD6AYQSOIN2IN5OGC6VQZCR4H3UFMIOXSW6NNS6N53CLJA4PB56CEJJI"`
	if got != expected {
		t.Errorf("Expected %q, got %q", expected, got)
	}

	c = &client{
		cid: 1024,
		opts: ClientOpts{
			Username: "foo",
		},
	}
	got = c.getAuthUser()
	expected = `User "foo"`
	if got != expected {
		t.Errorf("Expected %q, got %q", expected, got)
	}

	c = &client{
		cid:  1024,
		opts: ClientOpts{},
	}
	got = c.getAuthUser()
	expected = `User "N/A"`
	if got != expected {
		t.Errorf("Expected %q, got %q", expected, got)
	}
}

type captureWarnLogger struct {
	DummyLogger
	warn chan string
}

func (l *captureWarnLogger) Warnf(format string, v ...interface{}) {
	select {
	case l.warn <- fmt.Sprintf(format, v...):
	default:
	}
}

func TestReadloopWarning(t *testing.T) {
	readLoopReportThreshold = 100 * time.Millisecond
	defer func() { readLoopReportThreshold = readLoopReport }()

	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	l := &captureWarnLogger{warn: make(chan string, 1)}
	s.SetLogger(l, false, false)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc := natsConnect(t, url)
	defer nc.Close()
	natsSubSync(t, nc, "foo")
	natsFlush(t, nc)
	cid, _ := nc.GetClientID()

	sender := natsConnect(t, url)
	defer sender.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	c := s.getClient(cid)
	c.mu.Lock()
	go func() {
		defer wg.Done()
		time.Sleep(250 * time.Millisecond)
		c.mu.Unlock()
	}()

	natsPub(t, sender, "foo", make([]byte, 100))
	natsFlush(t, sender)

	select {
	case warn := <-l.warn:
		if !strings.Contains(warn, "Readloop") {
			t.Fatalf("unexpected warning: %v", warn)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("No warning printed")
	}
	wg.Wait()
}

func TestTraceMsg(t *testing.T) {
	c := &client{}
	// Enable message trace
	c.trace = true

	cases := []struct {
		Desc            string
		Msg             []byte
		Wanted          string
		MaxTracedMsgLen int
	}{
		{
			Desc:            "normal length",
			Msg:             []byte(fmt.Sprintf("normal%s", CR_LF)),
			Wanted:          " - <<- MSG_PAYLOAD: [\"normal\"]",
			MaxTracedMsgLen: 10,
		},
		{
			Desc:            "over length",
			Msg:             []byte(fmt.Sprintf("over length%s", CR_LF)),
			Wanted:          " - <<- MSG_PAYLOAD: [\"over lengt...\"]",
			MaxTracedMsgLen: 10,
		},
		{
			Desc:            "unlimited length",
			Msg:             []byte(fmt.Sprintf("unlimited length%s", CR_LF)),
			Wanted:          " - <<- MSG_PAYLOAD: [\"unlimited length\"]",
			MaxTracedMsgLen: 0,
		},
		{
			Desc:            "negative max traced msg len",
			Msg:             []byte(fmt.Sprintf("negative max traced msg len%s", CR_LF)),
			Wanted:          " - <<- MSG_PAYLOAD: [\"negative max traced msg len\"]",
			MaxTracedMsgLen: -1,
		},
	}

	for _, ut := range cases {
		c.srv = &Server{
			opts: &Options{MaxTracedMsgLen: ut.MaxTracedMsgLen},
		}
		c.srv.SetLogger(&DummyLogger{}, true, true)

		c.traceMsg(ut.Msg)

		got := c.srv.logging.logger.(*DummyLogger).Msg
		if !reflect.DeepEqual(ut.Wanted, got) {
			t.Errorf("Desc: %s. Msg %q. Traced msg want: %s, got: %s", ut.Desc, ut.Msg, ut.Wanted, got)
		}
	}
}

func TestClientMaxPending(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxPending = math.MaxInt32 + 1
	s := RunServer(opts)
	defer s.Shutdown()

	nc := natsConnect(t, fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	defer nc.Close()

	sub := natsSubSync(t, nc, "foo")
	natsPub(t, nc, "foo", []byte("msg"))
	natsNexMsg(t, sub, 100*time.Millisecond)
}

func TestResponsePermissions(t *testing.T) {
	for i, test := range []struct {
		name  string
		perms *ResponsePermission
	}{
		{"max_msgs", &ResponsePermission{MaxMsgs: 2, Expires: time.Hour}},
		{"no_expire_limit", &ResponsePermission{MaxMsgs: 3, Expires: -1 * time.Millisecond}},
		{"expire", &ResponsePermission{MaxMsgs: 1000, Expires: 100 * time.Millisecond}},
		{"no_msgs_limit", &ResponsePermission{MaxMsgs: -1, Expires: 100 * time.Millisecond}},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := DefaultOptions()
			u1 := &User{
				Username:    "service",
				Password:    "pwd",
				Permissions: &Permissions{Response: test.perms},
			}
			u2 := &User{Username: "ivan", Password: "pwd"}
			opts.Users = []*User{u1, u2}
			s := RunServer(opts)
			defer s.Shutdown()

			svcNC := natsConnect(t, fmt.Sprintf("nats://service:pwd@%s:%d", opts.Host, opts.Port))
			defer svcNC.Close()
			reqSub := natsSubSync(t, svcNC, "request")
			natsFlush(t, svcNC)

			nc := natsConnect(t, fmt.Sprintf("nats://ivan:pwd@%s:%d", opts.Host, opts.Port))
			defer nc.Close()

			replySub := natsSubSync(t, nc, "reply")

			natsPubReq(t, nc, "request", "reply", []byte("req1"))

			req1 := natsNexMsg(t, reqSub, 100*time.Millisecond)

			checkFailed := func(t *testing.T) {
				t.Helper()
				if reply, err := replySub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
					if reply != nil {
						t.Fatalf("Expected to receive timeout, got reply=%q", reply.Data)
					} else {
						t.Fatalf("Unexpected error: %v", err)
					}
				}
			}

			switch i {
			case 0:
				// Should allow only 2 replies...
				for i := 0; i < 10; i++ {
					natsPub(t, svcNC, req1.Reply, []byte("reply"))
				}
				natsNexMsg(t, replySub, 100*time.Millisecond)
				natsNexMsg(t, replySub, 100*time.Millisecond)
				// The next should fail...
				checkFailed(t)
			case 1:
				// Expiration is set to -1ms, which should count as infinite...
				natsPub(t, svcNC, req1.Reply, []byte("reply"))
				// Sleep a bit before next send
				time.Sleep(50 * time.Millisecond)
				natsPub(t, svcNC, req1.Reply, []byte("reply"))
				// Make sure we receive both
				natsNexMsg(t, replySub, 100*time.Millisecond)
				natsNexMsg(t, replySub, 100*time.Millisecond)
			case 2:
				fallthrough
			case 3:
				// Expire set to 100ms so make sure we wait more between
				// next publish
				natsPub(t, svcNC, req1.Reply, []byte("reply"))
				time.Sleep(200 * time.Millisecond)
				natsPub(t, svcNC, req1.Reply, []byte("reply"))
				// Should receive one, and fail on the other
				natsNexMsg(t, replySub, 100*time.Millisecond)
				checkFailed(t)
			}
			// When testing expiration, sleep before sending next reply
			if i >= 2 {
				time.Sleep(400 * time.Millisecond)
			}
		})
	}
}

func TestPingNotSentTooSoon(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	doneCh := make(chan bool, 1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			s.Connz(nil)
			select {
			case <-doneCh:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()

	for i := 0; i < 100; i++ {
		nc, err := nats.Connect(s.ClientURL())
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		nc.Close()
	}
	close(doneCh)
	wg.Wait()

	c, br, _ := newClientForServer(s)
	defer c.close()
	connectOp := []byte("CONNECT {\"user\":\"ivan\",\"pass\":\"bar\"}\r\n")
	c.parse(connectOp)

	// Since client has not send PING, having server try to send RTT ping
	// to client should not do anything
	if c.sendRTTPing() {
		t.Fatalf("RTT ping should not have been sent")
	}
	// Speed up detection of time elapsed by moving the c.start to more than
	// 2 secs in the past.
	c.mu.Lock()
	c.start = time.Unix(0, c.start.UnixNano()-int64(maxNoRTTPingBeforeFirstPong+time.Second))
	c.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		l, _ := br.ReadString('\n')
		if l != "PING\r\n" {
			errCh <- fmt.Errorf("expected to get PING, got %s", l)
			return
		}
		errCh <- nil
	}()
	if !c.sendRTTPing() {
		t.Fatalf("RTT ping should have been sent")
	}
	wg.Wait()
	if e := <-errCh; e != nil {
		t.Fatal(e.Error())
	}
}

func TestClientCheckUseOfGWReplyPrefix(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	ech := make(chan error, 1)
	nc, err := nats.Connect(s.ClientURL(),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) {
			ech <- e
		}))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Expect to fail if publish on gateway reply prefix
	nc.Publish(gwReplyPrefix+"anything", []byte("should fail"))

	// Wait for publish violation error
	select {
	case e := <-ech:
		if e == nil || !strings.Contains(strings.ToLower(e.Error()), "violation for publish") {
			t.Fatalf("Expected violation error, got %v", e)
		}
	case <-time.After(time.Second):
		t.Fatalf("Did not receive permissions violation error")
	}

	// Now publish a message with a reply set to the prefix,
	// it should be rejected too.
	nc.PublishRequest("foo", gwReplyPrefix+"anything", []byte("should fail"))

	// Wait for publish violation error with reply
	select {
	case e := <-ech:
		if e == nil || !strings.Contains(strings.ToLower(e.Error()), "violation for publish with reply") {
			t.Fatalf("Expected violation error, got %v", e)
		}
	case <-time.After(time.Second):
		t.Fatalf("Did not receive permissions violation error")
	}
}

func TestNoClientLeakOnSlowConsumer(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer c.Close()

	cr := bufio.NewReader(c)

	// Wait for INFO...
	line, _, _ := cr.ReadLine()
	var info serverInfo
	if err = json.Unmarshal(line[5:], &info); err != nil {
		t.Fatalf("Could not parse INFO json: %v\n", err)
	}

	// Send our connect
	if _, err := c.Write([]byte("CONNECT {\"verbose\": false}\r\nSUB foo 1\r\nPING\r\n")); err != nil {
		t.Fatalf("Error sending CONNECT and SUB: %v", err)
	}
	// Wait for PONG
	line, _, _ = cr.ReadLine()
	if string(line) != "PONG" {
		t.Fatalf("Expected 'PONG' but got %q", line)
	}

	// Get the client from server map
	cli := s.GetClient(info.CID)
	if cli == nil {
		t.Fatalf("No client registered")
	}
	// Change the write deadline to very low value
	cli.mu.Lock()
	cli.out.wdl = time.Nanosecond
	cli.mu.Unlock()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	// Send some messages to cause write deadline error on "cli"
	payload := make([]byte, 1000)
	for i := 0; i < 100; i++ {
		natsPub(t, nc, "foo", payload)
	}
	natsFlush(t, nc)
	nc.Close()

	// Now make sure that the number of clients goes to 0.
	checkClientsCount(t, s, 0)
}

func TestClientSlowConsumerWithoutConnect(t *testing.T) {
	opts := DefaultOptions()
	opts.WriteDeadline = 100 * time.Millisecond
	s := RunServer(opts)
	defer s.Shutdown()

	url := fmt.Sprintf("127.0.0.1:%d", opts.Port)
	c, err := net.Dial("tcp", url)
	if err != nil {
		t.Fatalf("Error on dial: %v", err)
	}
	defer c.Close()
	c.Write([]byte("SUB foo 1\r\n"))

	payload := make([]byte, 10000)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	for i := 0; i < 10000; i++ {
		nc.Publish("foo", payload)
	}
	nc.Flush()
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		// Expect slow consumer..
		if n := atomic.LoadInt64(&s.slowConsumers); n != 1 {
			return fmt.Errorf("Expected 1 slow consumer, got: %v", n)
		}
		if n := s.scStats.clients.Load(); n != 1 {
			return fmt.Errorf("Expected 1 slow consumer, got: %v", n)
		}
		return nil
	})
	varz, err := s.Varz(nil)
	if err != nil {
		t.Fatal(err)
	}
	if varz.SlowConsumersStats.Clients != 1 {
		t.Error("Expected a slow consumer client in varz")
	}
}

func TestClientNoSlowConsumerIfConnectExpected(t *testing.T) {
	opts := DefaultOptions()
	opts.Username = "ivan"
	opts.Password = "pass"
	// Make it very slow so that the INFO sent to client fails...
	opts.WriteDeadline = time.Nanosecond
	s := RunServer(opts)
	defer s.Shutdown()

	// Expect server to close the connection, but will bump the slow
	// consumer count.
	nc, err := nats.Connect(fmt.Sprintf("nats://ivan:pass@%s:%d", opts.Host, opts.Port))
	if err == nil {
		nc.Close()
		t.Fatal("Expected connect error")
	}
	if n := atomic.LoadInt64(&s.slowConsumers); n != 0 {
		t.Fatalf("Expected 0 slow consumer, got: %v", n)
	}
}

func TestClientStalledDuration(t *testing.T) {
	for _, test := range []struct {
		name        string
		pb          int64
		mp          int64
		expectedTTL time.Duration
	}{
		{"pb above mp", 110, 100, stallClientMaxDuration},
		{"pb equal mp", 100, 100, stallClientMaxDuration},
		{"pb below mp/2", 49, 100, stallClientMinDuration},
		{"pb equal mp/2", 50, 100, stallClientMinDuration},
		{"pb at 55% of mp", 55, 100, stallClientMinDuration + 1*stallClientMinDuration},
		{"pb at 60% of mp", 60, 100, stallClientMinDuration + 2*stallClientMinDuration},
		{"pb at 70% of mp", 70, 100, stallClientMinDuration + 4*stallClientMinDuration},
		{"pb at 80% of mp", 80, 100, stallClientMinDuration + 6*stallClientMinDuration},
		{"pb at 90% of mp", 90, 100, stallClientMinDuration + 8*stallClientMinDuration},
		{"pb at 99% of mp", 99, 100, stallClientMinDuration + 9*stallClientMinDuration},
	} {
		t.Run(test.name, func(t *testing.T) {
			if ttl := stallDuration(test.pb, test.mp); ttl != test.expectedTTL {
				t.Fatalf("For pb=%v mp=%v, expected TTL to be %v, got %v", test.pb, test.mp, test.expectedTTL, ttl)
			}
		})
	}
}

func TestClientIPv6Address(t *testing.T) {
	opts := DefaultOptions()
	opts.Host = "0.0.0.0"
	s := RunServer(opts)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://[::1]:%v", opts.Port))
	// Travis may not accept IPv6, in that case, skip the test.
	if err != nil {
		t.Skipf("Skipping test because could not connect: %v", err)
	}
	defer nc.Close()

	cid, _ := nc.GetClientID()
	c := s.GetClient(cid)
	c.mu.Lock()
	ncs := c.String()
	c.mu.Unlock()
	if !strings.HasPrefix(ncs, "[::1]") {
		t.Fatalf("Wrong string representation of an IPv6 address: %q", ncs)
	}
}

func TestPBNotIncreasedOnMaxPending(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxPending = 100
	s := &Server{opts: opts}
	c := &client{srv: s}
	c.initClient()

	c.mu.Lock()
	c.queueOutbound(make([]byte, 200))
	pb := c.out.pb
	c.mu.Unlock()

	if pb != 0 {
		t.Fatalf("c.out.pb should be 0, got %v", pb)
	}
}

type testConnWritePartial struct {
	net.Conn
	partial bool
	buf     bytes.Buffer
}

func (c *testConnWritePartial) Write(p []byte) (int, error) {
	n := len(p)
	if c.partial {
		n = 15
	}
	return c.buf.Write(p[:n])
}

func (c *testConnWritePartial) RemoteAddr() net.Addr {
	return nil
}

func (c *testConnWritePartial) SetWriteDeadline(_ time.Time) error {
	return nil
}

func TestFlushOutboundNoSliceReuseIfPartial(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxPending = 1024
	s := &Server{opts: opts}

	fakeConn := &testConnWritePartial{partial: true}
	c := &client{srv: s, nc: fakeConn}
	c.initClient()

	bufs := [][]byte{
		[]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
		[]byte("------"),
		[]byte("0123456789"),
	}
	expected := bytes.Buffer{}
	for _, buf := range bufs {
		expected.Write(buf)
		c.mu.Lock()
		c.queueOutbound(buf)
		c.flushOutbound()
		fakeConn.partial = false
		c.mu.Unlock()
	}
	// Ensure everything is flushed.
	for done := false; !done; {
		c.mu.Lock()
		if c.out.pb > 0 {
			c.flushOutbound()
		} else {
			done = true
		}
		c.mu.Unlock()
	}
	if !bytes.Equal(expected.Bytes(), fakeConn.buf.Bytes()) {
		t.Fatalf("Expected\n%q\ngot\n%q", expected.String(), fakeConn.buf.String())
	}
}

type captureNoticeLogger struct {
	DummyLogger
	notices []string
}

func (l *captureNoticeLogger) Noticef(format string, v ...interface{}) {
	l.Lock()
	l.notices = append(l.notices, fmt.Sprintf(format, v...))
	l.Unlock()
}

func TestCloseConnectionLogsReason(t *testing.T) {
	o1 := DefaultOptions()
	s1 := RunServer(o1)
	defer s1.Shutdown()

	l := &captureNoticeLogger{}
	s1.SetLogger(l, true, true)

	o2 := DefaultOptions()
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)
	s2.Shutdown()

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if s1.NumRoutes() != 0 {
			return fmt.Errorf("route still connected")
		}
		return nil
	})
	// Now check that s1 has logged that the connection is closed and that the reason is included.
	ok := false
	l.Lock()
	for _, n := range l.notices {
		if strings.Contains(n, "connection closed: "+ClientClosed.String()) {
			ok = true
			break
		}
	}
	l.Unlock()
	if !ok {
		t.Fatal("Log does not contain closed reason")
	}
}

func TestCloseConnectionVeryEarly(t *testing.T) {
	for _, test := range []struct {
		name   string
		useTLS bool
	}{
		{"no_tls", false},
		{"tls", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := DefaultOptions()
			if test.useTLS {
				tc := &TLSConfigOpts{
					CertFile: "../test/configs/certs/server-cert.pem",
					KeyFile:  "../test/configs/certs/server-key.pem",
					CaFile:   "../test/configs/certs/ca.pem",
				}
				tlsConfig, err := GenTLSConfig(tc)
				if err != nil {
					t.Fatalf("Error generating tls config: %v", err)
				}
				o.TLSConfig = tlsConfig
			}
			s := RunServer(o)
			defer s.Shutdown()

			// The issue was with a connection that would break right when
			// server was sending the INFO. Creating a bare TCP connection
			// and closing it right away won't help reproduce the problem.
			// So testing in 2 steps.

			// Get a normal TCP connection to the server.
			c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", o.Port))
			if err != nil {
				t.Fatalf("Unable to create tcp connection")
			}
			// Now close it.
			c.Close()

			// Wait that num clients falls to 0.
			checkClientsCount(t, s, 0)

			// Call again with this closed connection. Alternatively, we
			// would have to call with a fake connection that implements
			// net.Conn but returns an error on Write.
			s.createClient(c)

			// This connection should not have been added to the server.
			checkClientsCount(t, s, 0)
		})
	}
}

type connAddrString struct {
	net.Addr
}

func (a *connAddrString) String() string {
	return "[fe80::abc:def:ghi:123%utun0]:4222"
}

type connString struct {
	net.Conn
}

func (c *connString) RemoteAddr() net.Addr {
	return &connAddrString{}
}

func TestClientConnectionName(t *testing.T) {
	s, err := NewServer(DefaultOptions())
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	l := &DummyLogger{}
	s.SetLogger(l, true, true)

	for _, test := range []struct {
		name    string
		kind    int
		kindStr string
		ws      bool
		mqtt    bool
	}{
		{"client", CLIENT, "cid:", false, false},
		{"ws client", CLIENT, "wid:", true, false},
		{"mqtt client", CLIENT, "mid:", false, true},
		{"route", ROUTER, "rid:", false, false},
		{"gateway", GATEWAY, "gid:", false, false},
		{"leafnode", LEAF, "lid:", false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := &client{srv: s, nc: &connString{}, kind: test.kind}
			if test.ws {
				c.ws = &websocket{}
			}
			if test.mqtt {
				c.mqtt = &mqtt{}
			}
			c.initClient()

			if host := "fe80::abc:def:ghi:123%utun0"; host != c.host {
				t.Fatalf("expected host to be %q, got %q", host, c.host)
			}
			if port := uint16(4222); port != c.port {
				t.Fatalf("expected port to be %v, got %v", port, c.port)
			}

			checkLog := func(suffix string) {
				t.Helper()
				l.Lock()
				msg := l.Msg
				l.Unlock()
				if strings.Contains(msg, "(MISSING)") {
					t.Fatalf("conn name was not escaped properly, got MISSING: %s", msg)
				}
				if !strings.Contains(l.Msg, test.kindStr) {
					t.Fatalf("expected kind to be %q, got: %s", test.kindStr, msg)
				}
				if !strings.HasSuffix(l.Msg, suffix) {
					t.Fatalf("expected statement to end with %q, got %s", suffix, msg)
				}
			}

			c.Debugf("debug: %v", 1)
			checkLog(" 1")
			c.Tracef("trace: %s", "2")
			checkLog(" 2")
			c.Warnf("warn: %s %d", "3", 4)
			checkLog(" 3 4")
			c.Errorf("error: %v %s", 5, "6")
			checkLog(" 5 6")
		})
	}
}

func TestClientLimits(t *testing.T) {
	accKp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatalf("Error creating account key: %v", err)
	}
	uKp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("Error creating user key: %v", err)
	}
	uPub, err := uKp.PublicKey()
	if err != nil {
		t.Fatalf("Error obtaining publicKey: %v", err)
	}
	s, err := NewServer(DefaultOptions())
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	for _, test := range []struct {
		client int32
		acc    int32
		srv    int32
		expect int32
	}{
		// all identical
		{1, 1, 1, 1},
		{-1, -1, 0, -1},
		// only one value unlimited
		{1, -1, 0, 1},
		{-1, 1, 0, 1},
		{-1, -1, 1, 1},
		// all combinations of distinct values
		{1, 2, 3, 1},
		{1, 3, 2, 1},
		{2, 1, 3, 1},
		{2, 3, 1, 1},
		{3, 1, 2, 1},
		{3, 2, 1, 1},
	} {
		t.Run("", func(t *testing.T) {
			s.opts.MaxPayload = test.srv
			s.opts.MaxSubs = int(test.srv)
			c := &client{srv: s, acc: &Account{
				limits: limits{mpay: test.acc, msubs: test.acc},
			}}
			uc := jwt.NewUserClaims(uPub)
			uc.Limits.Subs = int64(test.client)
			uc.Limits.Payload = int64(test.client)
			c.opts.JWT, err = uc.Encode(accKp)
			if err != nil {
				t.Fatalf("Error encoding jwt: %v", err)
			}
			c.applyAccountLimits()
			if c.mpay != test.expect {
				t.Fatalf("payload %d not as ecpected %d", c.mpay, test.expect)
			}
			if c.msubs != test.expect {
				t.Fatalf("subscriber %d not as ecpected %d", c.msubs, test.expect)
			}
		})
	}
}

func TestClientClampMaxSubsErrReport(t *testing.T) {
	maxSubLimitReportThreshold = int64(100 * time.Millisecond)
	defer func() { maxSubLimitReportThreshold = defaultMaxSubLimitReportThreshold }()

	o1 := DefaultOptions()
	o1.MaxSubs = 1
	o1.LeafNode.Host = "127.0.0.1"
	o1.LeafNode.Port = -1
	s1 := RunServer(o1)
	defer s1.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s1.SetLogger(l, false, false)

	o2 := DefaultOptions()
	o2.Cluster.Name = "xyz"
	u, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", o1.LeafNode.Port))
	o2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkLeafNodeConnected(t, s1)
	checkLeafNodeConnected(t, s2)

	nc := natsConnect(t, s2.ClientURL())
	defer nc.Close()
	natsSubSync(t, nc, "foo")
	natsSubSync(t, nc, "bar")

	// Make sure we receive only 1
	check := func() {
		t.Helper()
		for i := 0; i < 2; i++ {
			select {
			case errStr := <-l.errCh:
				if i > 0 {
					t.Fatalf("Should not have logged a second time: %s", errStr)
				}
				if !strings.Contains(errStr, "maximum subscriptions") {
					t.Fatalf("Unexpected error: %s", errStr)
				}
			case <-time.After(300 * time.Millisecond):
				if i == 0 {
					t.Fatal("Error should have been logged")
				}
			}
		}
	}
	check()

	// The above will have waited long enough to clear the report threshold.
	// So create two new subs and check again that we get only 1 report.
	natsSubSync(t, nc, "baz")
	natsSubSync(t, nc, "bat")
	check()
}

func TestClientDenySysGroupSub(t *testing.T) {
	s := RunServer(DefaultOptions())
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
	require_NoError(t, err)
	defer nc.Close()

	_, err = nc.QueueSubscribeSync("foo", sysGroup)
	require_NoError(t, err)
	nc.Flush()
	err = nc.LastError()
	require_Error(t, err)
	require_Contains(t, err.Error(), "Permissions Violation")
}

func TestClientAuthRequiredNoAuthUser(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts: {
			A: { users: [ { user: user, password: pass } ] }
		}
		no_auth_user: user
	`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)
	defer nc.Close()

	if nc.AuthRequired() {
		t.Fatalf("Expected AuthRequired to be false due to 'no_auth_user'")
	}
}

func TestClientUserInfoReq(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		PERMS = {
			publish = { allow: "$SYS.REQ.>", deny: "$SYS.REQ.ACCOUNT.>" }
			subscribe = "_INBOX.>"
			allow_responses: true
		}
		accounts: {
			A: { users: [ { user: dlc, password: pass, permissions: $PERMS } ] }
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
		no_auth_user: dlc
	`))
	defer removeFile(t, conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)
	defer nc.Close()

	resp, err := nc.Request("$SYS.REQ.USER.INFO", nil, time.Second)
	require_NoError(t, err)

	response := ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)

	userInfo := response.Data.(*UserInfo)

	dlc := &UserInfo{
		UserID:  "dlc",
		Account: "A",
		Permissions: &Permissions{
			Publish: &SubjectPermission{
				Allow: []string{"$SYS.REQ.>"},
				Deny:  []string{"$SYS.REQ.ACCOUNT.>"},
			},
			Subscribe: &SubjectPermission{
				Allow: []string{"_INBOX.>"},
			},
			Response: &ResponsePermission{
				MaxMsgs: DEFAULT_ALLOW_RESPONSE_MAX_MSGS,
				Expires: DEFAULT_ALLOW_RESPONSE_EXPIRATION,
			},
		},
	}
	if !reflect.DeepEqual(dlc, userInfo) {
		t.Fatalf("User info for %q did not match", "dlc")
	}

	// Make sure system users work ok too.
	nc, err = nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	resp, err = nc.Request("$SYS.REQ.USER.INFO", nil, time.Second)
	require_NoError(t, err)

	response = ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)

	userInfo = response.Data.(*UserInfo)

	admin := &UserInfo{
		UserID:  "admin",
		Account: "$SYS",
	}
	if !reflect.DeepEqual(admin, userInfo) {
		t.Fatalf("User info for %q did not match", "admin")
	}
}
