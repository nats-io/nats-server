// Copyright 2013-2023 The NATS Authors
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
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const CLIENT_PORT = -1
const MONITOR_PORT = -1
const CLUSTER_PORT = -1

func DefaultMonitorOptions() *Options {
	return &Options{
		Host:         "127.0.0.1",
		Port:         CLIENT_PORT,
		HTTPHost:     "127.0.0.1",
		HTTPPort:     MONITOR_PORT,
		HTTPBasePath: "/",
		ServerName:   "monitor_server",
		NoLog:        true,
		NoSigs:       true,
		Tags:         []string{"tag"},
	}
}

func runMonitorServer() *Server {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	return RunServer(opts)
}

func runMonitorServerWithAccounts() *Server {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	aA := NewAccount("A")
	aB := NewAccount("B")
	opts.Accounts = append(opts.Accounts, aA, aB)
	opts.Users = append(opts.Users,
		&User{Username: "a", Password: "a", Account: aA},
		&User{Username: "b", Password: "b", Account: aB})
	return RunServer(opts)
}

func runMonitorServerNoHTTPPort() *Server {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.HTTPPort = 0
	return RunServer(opts)
}

func resetPreviousHTTPConnections() {
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
}

func TestMyUptime(t *testing.T) {
	// Make sure we print this stuff right.
	var d time.Duration
	var s string

	d = 22 * time.Second
	s = myUptime(d)
	if s != "22s" {
		t.Fatalf("Expected `22s`, go ``%s`", s)
	}
	d = 4*time.Minute + d
	s = myUptime(d)
	if s != "4m22s" {
		t.Fatalf("Expected `4m22s`, go ``%s`", s)
	}
	d = 4*time.Hour + d
	s = myUptime(d)
	if s != "4h4m22s" {
		t.Fatalf("Expected `4h4m22s`, go ``%s`", s)
	}
	d = 32*24*time.Hour + d
	s = myUptime(d)
	if s != "32d4h4m22s" {
		t.Fatalf("Expected `32d4h4m22s`, go ``%s`", s)
	}
	d = 22*365*24*time.Hour + d
	s = myUptime(d)
	if s != "22y32d4h4m22s" {
		t.Fatalf("Expected `22y32d4h4m22s`, go ``%s`", s)
	}
}

// Make sure that we do not run the http server for monitoring unless asked.
func TestNoMonitorPort(t *testing.T) {
	s := runMonitorServerNoHTTPPort()
	defer s.Shutdown()

	// this test might be meaningless now that we're testing with random ports?
	url := fmt.Sprintf("http://127.0.0.1:%d/", 11245)
	if resp, err := http.Get(url + "varz"); err == nil {
		t.Fatalf("Expected error: Got %+v\n", resp)
	}
	if resp, err := http.Get(url + "healthz"); err == nil {
		t.Fatalf("Expected error: Got %+v\n", resp)
	}
	if resp, err := http.Get(url + "connz"); err == nil {
		t.Fatalf("Expected error: Got %+v\n", resp)
	}
}

var (
	appJSONContent = "application/json"
	appJSContent   = "application/javascript"
	textPlain      = "text/plain; charset=utf-8"
	textHTML       = "text/html; charset=utf-8"
)

func readBodyEx(t *testing.T, url string, status int, content string) []byte {
	resp, err := http.Get(url)
	if err != nil {
		stackFatalf(t, "Expected no error: Got %v\n", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != status {
		stackFatalf(t, "Expected a %d response, got %d\n", status, resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != content {
		stackFatalf(t, "Expected %s content-type, got %s\n", content, ct)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		stackFatalf(t, "Got an error reading the body: %v\n", err)
	}
	return body
}

func TestHTTPBasePath(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.HTTPBasePath = "/nats"

	s := RunServer(opts)
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/nats", s.MonitorAddr().Port)
	readBodyEx(t, url, http.StatusOK, textHTML)
}

func readBody(t *testing.T, url string) []byte {
	return readBodyEx(t, url, http.StatusOK, appJSONContent)
}

func pollVarz(t *testing.T, s *Server, mode int, url string, opts *VarzOptions) *Varz {
	t.Helper()
	if mode == 0 {
		v := &Varz{}
		body := readBody(t, url)
		if err := json.Unmarshal(body, v); err != nil {
			t.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		return v
	}
	v, err := s.Varz(opts)
	if err != nil {
		t.Fatalf("Error on Varz: %v", err)
	}
	return v
}

// https://github.com/nats-io/nats-server/issues/2170
// Just the ever increasing subs part.
func TestVarzSubscriptionsResetProperly(t *testing.T) {
	// Run with JS to create a bunch of subs to start.
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.JetStream = true
	s := RunServer(opts)
	defer s.Shutdown()

	// This bug seems to only happen via the http endpoint, not direct calls.
	// Every time you call it doubles.
	url := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	osubs := pollVarz(t, s, 0, url, nil).Subscriptions
	// Make sure we get same number back.
	if v := pollVarz(t, s, 0, url, nil); v.Subscriptions != osubs {
		t.Fatalf("Expected subs to stay the same, %d vs %d", osubs, v.Subscriptions)
	}
}

func TestHandleVarz(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, s, mode, url+"varz", nil)

		// Do some sanity checks on values
		if time.Since(v.Start) > 10*time.Second {
			t.Fatal("Expected start time to be within 10 seconds.")
		}
	}

	time.Sleep(100 * time.Millisecond)

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, s, mode, url+"varz", nil)

		if v.Connections != 1 {
			t.Fatalf("Expected Connections of 1, got %v\n", v.Connections)
		}
		if v.TotalConnections < 1 {
			t.Fatalf("Expected Total Connections of at least 1, got %v\n", v.TotalConnections)
		}
		if v.InMsgs != 1 {
			t.Fatalf("Expected InMsgs of 1, got %v\n", v.InMsgs)
		}
		if v.OutMsgs != 1 {
			t.Fatalf("Expected OutMsgs of 1, got %v\n", v.OutMsgs)
		}
		if v.InBytes != 5 {
			t.Fatalf("Expected InBytes of 5, got %v\n", v.InBytes)
		}
		if v.OutBytes != 5 {
			t.Fatalf("Expected OutBytes of 5, got %v\n", v.OutBytes)
		}
		if v.Subscriptions != 0 {
			t.Fatalf("Expected Subscriptions of 0, got %v\n", v.Subscriptions)
		}
		if v.Name != "monitor_server" {
			t.Fatal("Expected ServerName to be 'monitor_server'")
		}
		if !v.Tags.Contains("tag") {
			t.Fatal("Expected tags to be 'tag'")
		}
	}

	// Test JSONP
	readBodyEx(t, url+"varz?callback=callback", http.StatusOK, appJSContent)
}

func pollConz(t *testing.T, s *Server, mode int, url string, opts *ConnzOptions) *Connz {
	t.Helper()
	if mode == 0 {
		body := readBody(t, url)
		c := &Connz{}
		if err := json.Unmarshal(body, &c); err != nil {
			t.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		return c
	}
	c, err := s.Connz(opts)
	if err != nil {
		t.Fatalf("Error on Connz(): %v", err)
	}
	return c
}

func TestConnz(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	testConnz := func(mode int) {
		c := pollConz(t, s, mode, url+"connz", nil)

		// Test contents..
		if c.NumConns != 0 {
			t.Fatalf("Expected 0 connections, got %d\n", c.NumConns)
		}
		if c.Total != 0 {
			t.Fatalf("Expected 0 live connections, got %d\n", c.Total)
		}
		if c.Conns == nil || len(c.Conns) != 0 {
			t.Fatalf("Expected 0 connections in array, got %p\n", c.Conns)
		}

		// Test with connections.
		nc := createClientConnSubscribeAndPublish(t, s)
		defer nc.Close()

		time.Sleep(50 * time.Millisecond)

		c = pollConz(t, s, mode, url+"connz", nil)

		if c.NumConns != 1 {
			t.Fatalf("Expected 1 connection, got %d\n", c.NumConns)
		}
		if c.Total != 1 {
			t.Fatalf("Expected 1 live connection, got %d\n", c.Total)
		}
		if c.Conns == nil || len(c.Conns) != 1 {
			t.Fatalf("Expected 1 connection in array, got %d\n", len(c.Conns))
		}

		if c.Limit != DefaultConnListSize {
			t.Fatalf("Expected limit of %d, got %v\n", DefaultConnListSize, c.Limit)
		}

		if c.Offset != 0 {
			t.Fatalf("Expected offset of 0, got %v\n", c.Offset)
		}

		// Test inside details of each connection
		ci := c.Conns[0]

		if ci.Cid == 0 {
			t.Fatalf("Expected non-zero cid, got %v\n", ci.Cid)
		}
		if ci.IP != "127.0.0.1" {
			t.Fatalf("Expected \"127.0.0.1\" for IP, got %v\n", ci.IP)
		}
		if ci.Port == 0 {
			t.Fatalf("Expected non-zero port, got %v\n", ci.Port)
		}
		if ci.NumSubs != 0 {
			t.Fatalf("Expected num_subs of 0, got %v\n", ci.NumSubs)
		}
		if len(ci.Subs) != 0 {
			t.Fatalf("Expected subs of 0, got %v\n", ci.Subs)
		}
		if len(ci.SubsDetail) != 0 {
			t.Fatalf("Expected subsdetail of 0, got %v\n", ci.SubsDetail)
		}
		if ci.InMsgs != 1 {
			t.Fatalf("Expected InMsgs of 1, got %v\n", ci.InMsgs)
		}
		if ci.OutMsgs != 1 {
			t.Fatalf("Expected OutMsgs of 1, got %v\n", ci.OutMsgs)
		}
		if ci.InBytes != 5 {
			t.Fatalf("Expected InBytes of 1, got %v\n", ci.InBytes)
		}
		if ci.OutBytes != 5 {
			t.Fatalf("Expected OutBytes of 1, got %v\n", ci.OutBytes)
		}
		if ci.Start.IsZero() {
			t.Fatal("Expected Start to be valid\n")
		}
		if ci.Uptime == "" {
			t.Fatal("Expected Uptime to be valid\n")
		}
		if ci.LastActivity.IsZero() {
			t.Fatal("Expected LastActivity to be valid\n")
		}
		if ci.LastActivity.UnixNano() < ci.Start.UnixNano() {
			t.Fatalf("Expected LastActivity [%v] to be > Start [%v]\n", ci.LastActivity, ci.Start)
		}
		if ci.Idle == "" {
			t.Fatal("Expected Idle to be valid\n")
		}
		// This is a change, we now expect them to be set for connections when the
		// client sends a connect.
		if ci.RTT == "" {
			t.Fatal("Expected RTT to be set for new connection\n")
		}
	}

	for mode := 0; mode < 2; mode++ {
		testConnz(mode)
		checkClientsCount(t, s, 0)
	}

	// Test JSONP
	readBodyEx(t, url+"connz?callback=callback", http.StatusOK, appJSContent)
}

func TestConnzBadParams(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/connz?", s.MonitorAddr().Port)
	readBodyEx(t, url+"auth=xxx", http.StatusBadRequest, textPlain)
	readBodyEx(t, url+"subs=xxx", http.StatusBadRequest, textPlain)
	readBodyEx(t, url+"offset=xxx", http.StatusBadRequest, textPlain)
	readBodyEx(t, url+"limit=xxx", http.StatusBadRequest, textPlain)
	readBodyEx(t, url+"state=xxx", http.StatusBadRequest, textPlain)
}

func TestConnzWithSubs(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	nc.Subscribe("hello.foo", func(m *nats.Msg) {})
	ensureServerActivityRecorded(t, nc)

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz?subs=1", &ConnzOptions{Subscriptions: true})
		// Test inside details of each connection
		ci := c.Conns[0]
		if len(ci.Subs) != 1 || ci.Subs[0] != "hello.foo" {
			t.Fatalf("Expected subs of 1, got %v\n", ci.Subs)
		}
	}
}

func TestConnzWithSubsDetail(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	nc.Subscribe("hello.foo", func(m *nats.Msg) {})
	ensureServerActivityRecorded(t, nc)

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz?subs=detail", &ConnzOptions{SubscriptionsDetail: true})
		// Test inside details of each connection
		ci := c.Conns[0]
		if len(ci.SubsDetail) != 1 || ci.SubsDetail[0].Subject != "hello.foo" {
			t.Fatalf("Expected subsdetail of 1, got %v\n", ci.Subs)
		}
	}
}

func TestClosedConnzWithSubsDetail(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)

	nc.Subscribe("hello.foo", func(m *nats.Msg) {})
	ensureServerActivityRecorded(t, nc)
	nc.Close()

	s.mu.Lock()
	for len(s.clients) != 0 {
		s.mu.Unlock()
		<-time.After(100 * time.Millisecond)
		s.mu.Lock()
	}
	s.mu.Unlock()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz?state=closed&subs=detail", &ConnzOptions{State: ConnClosed,
			SubscriptionsDetail: true})
		// Test inside details of each connection
		ci := c.Conns[0]
		if len(ci.SubsDetail) != 1 || ci.SubsDetail[0].Subject != "hello.foo" {
			t.Fatalf("Expected subsdetail of 1, got %v\n", ci.Subs)
		}
	}
}

func TestConnzWithCID(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	// The one we will request
	cid := 5
	total := 10

	// Create 10
	for i := 1; i <= total; i++ {
		nc := createClientConnSubscribeAndPublish(t, s)
		defer nc.Close()
		if i == cid {
			nc.Subscribe("hello.foo", func(m *nats.Msg) {})
			nc.Subscribe("hello.bar", func(m *nats.Msg) {})
			ensureServerActivityRecorded(t, nc)
		}
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/connz?cid=%d", s.MonitorAddr().Port, cid)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url, &ConnzOptions{CID: uint64(cid)})
		// Test inside details of each connection
		if len(c.Conns) != 1 {
			t.Fatalf("Expected only one connection, but got %d\n", len(c.Conns))
		}
		if c.NumConns != 1 {
			t.Fatalf("Expected NumConns to be 1, but got %d\n", c.NumConns)
		}
		ci := c.Conns[0]
		if ci.Cid != uint64(cid) {
			t.Fatalf("Expected to receive connection %v, but received %v\n", cid, ci.Cid)
		}
		if ci.NumSubs != 2 {
			t.Fatalf("Expected to receive connection with %d subs, but received %d\n", 2, ci.NumSubs)
		}
		// Now test a miss
		badUrl := fmt.Sprintf("http://127.0.0.1:%d/connz?cid=%d", s.MonitorAddr().Port, 100)
		c = pollConz(t, s, mode, badUrl, &ConnzOptions{CID: uint64(100)})
		if len(c.Conns) != 0 {
			t.Fatalf("Expected no connections, got %d\n", len(c.Conns))
		}
		if c.NumConns != 0 {
			t.Fatalf("Expected NumConns of 0, got %d\n", c.NumConns)
		}
	}
}

// Helper to map to connection name
func createConnMap(t *testing.T, cz *Connz) map[string]*ConnInfo {
	cm := make(map[string]*ConnInfo)
	for _, c := range cz.Conns {
		cm[c.Name] = c
	}
	return cm
}

func getFooAndBar(t *testing.T, cm map[string]*ConnInfo) (*ConnInfo, *ConnInfo) {
	return cm["foo"], cm["bar"]
}

func ensureServerActivityRecorded(t *testing.T, nc *nats.Conn) {
	nc.Flush()
	err := nc.Flush()
	if err != nil {
		t.Fatalf("Error flushing: %v\n", err)
	}
}

func TestConnzRTT(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	testRTT := func(mode int) {
		// Test with connections.
		nc := createClientConnSubscribeAndPublish(t, s)
		defer nc.Close()

		c := pollConz(t, s, mode, url+"connz", nil)

		if c.NumConns != 1 {
			t.Fatalf("Expected 1 connection, got %d\n", c.NumConns)
		}

		// Send a server side PING to record RTT
		s.mu.Lock()
		ci := c.Conns[0]
		sc := s.clients[ci.Cid]
		if sc == nil {
			t.Fatalf("Error looking up client %v\n", ci.Cid)
		}
		s.mu.Unlock()
		sc.mu.Lock()
		sc.sendPing()
		sc.mu.Unlock()

		// Wait for client to respond with PONG
		time.Sleep(20 * time.Millisecond)

		// Repoll for updated information.
		c = pollConz(t, s, mode, url+"connz", nil)
		ci = c.Conns[0]

		rtt, err := time.ParseDuration(ci.RTT)
		if err != nil {
			t.Fatalf("Could not parse RTT properly, %v (ci.RTT=%v)", err, ci.RTT)
		}
		if rtt <= 0 {
			t.Fatal("Expected RTT to be valid and non-zero\n")
		}
		if (runtime.GOOS == "windows" && rtt > 20*time.Millisecond) ||
			rtt > 20*time.Millisecond || rtt < 100*time.Nanosecond {
			t.Fatalf("Invalid RTT of %s\n", ci.RTT)
		}
	}

	for mode := 0; mode < 2; mode++ {
		testRTT(mode)
		checkClientsCount(t, s, 0)
	}
}

func TestConnzLastActivity(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	url += "connz?subs=1"
	opts := &ConnzOptions{Subscriptions: true}

	var sleepTime time.Duration
	if runtime.GOOS == "windows" {
		sleepTime = 10 * time.Millisecond
	}

	testActivity := func(mode int) {
		ncFoo := createClientConnWithName(t, "foo", s)
		defer ncFoo.Close()

		ncBar := createClientConnWithName(t, "bar", s)
		defer ncBar.Close()

		// Test inside details of each connection
		ciFoo, ciBar := getFooAndBar(t, createConnMap(t, pollConz(t, s, mode, url, opts)))

		// Test that LastActivity is non-zero
		if ciFoo.LastActivity.IsZero() {
			t.Fatalf("Expected LastActivity for connection '%s'to be valid\n", ciFoo.Name)
		}
		if ciBar.LastActivity.IsZero() {
			t.Fatalf("Expected LastActivity for connection '%s'to be valid\n", ciBar.Name)
		}
		// Foo should be older than Bar
		if ciFoo.LastActivity.After(ciBar.LastActivity) {
			t.Fatal("Expected connection 'foo' to be older than 'bar'\n")
		}

		fooLA := ciFoo.LastActivity
		barLA := ciBar.LastActivity

		ensureServerActivityRecorded(t, ncFoo)
		ensureServerActivityRecorded(t, ncBar)

		time.Sleep(sleepTime)

		// Sub should trigger update.
		sub, _ := ncFoo.Subscribe("hello.world", func(m *nats.Msg) {})
		ensureServerActivityRecorded(t, ncFoo)

		ciFoo, _ = getFooAndBar(t, createConnMap(t, pollConz(t, s, mode, url, opts)))
		nextLA := ciFoo.LastActivity
		if fooLA.Equal(nextLA) {
			t.Fatalf("Subscribe should have triggered update to LastActivity %+v\n", ciFoo)
		}
		fooLA = nextLA

		time.Sleep(sleepTime)

		// Publish and Message Delivery should trigger as well. So both connections
		// should have updates.
		ncBar.Publish("hello.world", []byte("Hello"))

		ensureServerActivityRecorded(t, ncFoo)
		ensureServerActivityRecorded(t, ncBar)

		ciFoo, ciBar = getFooAndBar(t, createConnMap(t, pollConz(t, s, mode, url, opts)))
		nextLA = ciBar.LastActivity
		if barLA.Equal(nextLA) {
			t.Fatalf("Publish should have triggered update to LastActivity\n")
		}

		// Message delivery on ncFoo should have triggered as well.
		nextLA = ciFoo.LastActivity
		if fooLA.Equal(nextLA) {
			t.Fatalf("Message delivery should have triggered update to LastActivity\n")
		}
		fooLA = nextLA

		time.Sleep(sleepTime)

		// Unsub should trigger as well
		sub.Unsubscribe()
		ensureServerActivityRecorded(t, ncFoo)

		ciFoo, _ = getFooAndBar(t, createConnMap(t, pollConz(t, s, mode, url, opts)))
		nextLA = ciFoo.LastActivity
		if fooLA.Equal(nextLA) {
			t.Fatalf("Message delivery should have triggered update to LastActivity\n")
		}
	}

	for mode := 0; mode < 2; mode++ {
		testActivity(mode)
	}
}

func TestConnzWithOffsetAndLimit(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz?offset=1&limit=1", &ConnzOptions{Offset: 1, Limit: 1})
		if c.Conns == nil || len(c.Conns) != 0 {
			t.Fatalf("Expected 0 connections in array, got %p\n", c.Conns)
		}

		// Test that when given negative values, 0 or default is used
		c = pollConz(t, s, mode, url+"connz?offset=-1&limit=-1", &ConnzOptions{Offset: -11, Limit: -11})
		if c.Conns == nil || len(c.Conns) != 0 {
			t.Fatalf("Expected 0 connections in array, got %p\n", c.Conns)
		}
		if c.Offset != 0 {
			t.Fatalf("Expected offset to be 0, and limit to be %v, got %v and %v",
				DefaultConnListSize, c.Offset, c.Limit)
		}
	}

	cl1 := createClientConnSubscribeAndPublish(t, s)
	defer cl1.Close()

	cl2 := createClientConnSubscribeAndPublish(t, s)
	defer cl2.Close()

	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz?offset=1&limit=1", &ConnzOptions{Offset: 1, Limit: 1})
		if c.Limit != 1 {
			t.Fatalf("Expected limit of 1, got %v\n", c.Limit)
		}

		if c.Offset != 1 {
			t.Fatalf("Expected offset of 1, got %v\n", c.Offset)
		}

		if len(c.Conns) != 1 {
			t.Fatalf("Expected conns of 1, got %v\n", len(c.Conns))
		}

		if c.NumConns != 1 {
			t.Fatalf("Expected NumConns to be 1, got %v\n", c.NumConns)
		}

		if c.Total != 2 {
			t.Fatalf("Expected Total to be at least 2, got %v", c.Total)
		}

		c = pollConz(t, s, mode, url+"connz?offset=2&limit=1", &ConnzOptions{Offset: 2, Limit: 1})
		if c.Limit != 1 {
			t.Fatalf("Expected limit of 1, got %v\n", c.Limit)
		}

		if c.Offset != 2 {
			t.Fatalf("Expected offset of 2, got %v\n", c.Offset)
		}

		if len(c.Conns) != 0 {
			t.Fatalf("Expected conns of 0, got %v\n", len(c.Conns))
		}

		if c.NumConns != 0 {
			t.Fatalf("Expected NumConns to be 0, got %v\n", c.NumConns)
		}

		if c.Total != 2 {
			t.Fatalf("Expected Total to be 2, got %v", c.Total)
		}
	}
}

func TestConnzDefaultSorted(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	clients := make([]*nats.Conn, 4)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz", nil)
		if c.Conns[0].Cid > c.Conns[1].Cid ||
			c.Conns[1].Cid > c.Conns[2].Cid ||
			c.Conns[2].Cid > c.Conns[3].Cid {
			t.Fatalf("Expected conns sorted in ascending order by cid, got %v < %v\n", c.Conns[0].Cid, c.Conns[3].Cid)
		}
	}
}

func TestConnzSortedByCid(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	clients := make([]*nats.Conn, 4)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz?sort=cid", &ConnzOptions{Sort: ByCid})
		if c.Conns[0].Cid > c.Conns[1].Cid ||
			c.Conns[1].Cid > c.Conns[2].Cid ||
			c.Conns[2].Cid > c.Conns[3].Cid {
			t.Fatalf("Expected conns sorted in ascending order by cid, got [%v, %v, %v, %v]\n",
				c.Conns[0].Cid, c.Conns[1].Cid, c.Conns[2].Cid, c.Conns[3].Cid)
		}
	}
}

func TestConnzSortedByStart(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	clients := make([]*nats.Conn, 4)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz?sort=start", &ConnzOptions{Sort: ByStart})
		if c.Conns[0].Start.After(c.Conns[1].Start) ||
			c.Conns[1].Start.After(c.Conns[2].Start) ||
			c.Conns[2].Start.After(c.Conns[3].Start) {
			t.Fatalf("Expected conns sorted in ascending order by startime, got [%v, %v, %v, %v]\n",
				c.Conns[0].Start, c.Conns[1].Start, c.Conns[2].Start, c.Conns[3].Start)
		}
	}
}

func TestConnzSortedByBytesAndMsgs(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	// Create a connection and make it send more messages than others
	firstClient := createClientConnSubscribeAndPublish(t, s)
	for i := 0; i < 100; i++ {
		firstClient.Publish("foo", []byte("Hello World"))
	}
	defer firstClient.Close()
	firstClient.Flush()

	clients := make([]*nats.Conn, 3)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz?sort=bytes_to", &ConnzOptions{Sort: ByOutBytes})
		if c.Conns[0].OutBytes < c.Conns[1].OutBytes ||
			c.Conns[0].OutBytes < c.Conns[2].OutBytes ||
			c.Conns[0].OutBytes < c.Conns[3].OutBytes {
			t.Fatalf("Expected conns sorted in descending order by bytes to, got %v < one of [%v, %v, %v]\n",
				c.Conns[0].OutBytes, c.Conns[1].OutBytes, c.Conns[2].OutBytes, c.Conns[3].OutBytes)
		}

		c = pollConz(t, s, mode, url+"connz?sort=msgs_to", &ConnzOptions{Sort: ByOutMsgs})
		if c.Conns[0].OutMsgs < c.Conns[1].OutMsgs ||
			c.Conns[0].OutMsgs < c.Conns[2].OutMsgs ||
			c.Conns[0].OutMsgs < c.Conns[3].OutMsgs {
			t.Fatalf("Expected conns sorted in descending order by msgs from, got %v < one of [%v, %v, %v]\n",
				c.Conns[0].OutMsgs, c.Conns[1].OutMsgs, c.Conns[2].OutMsgs, c.Conns[3].OutMsgs)
		}

		c = pollConz(t, s, mode, url+"connz?sort=bytes_from", &ConnzOptions{Sort: ByInBytes})
		if c.Conns[0].InBytes < c.Conns[1].InBytes ||
			c.Conns[0].InBytes < c.Conns[2].InBytes ||
			c.Conns[0].InBytes < c.Conns[3].InBytes {
			t.Fatalf("Expected conns sorted in descending order by bytes from, got %v < one of [%v, %v, %v]\n",
				c.Conns[0].InBytes, c.Conns[1].InBytes, c.Conns[2].InBytes, c.Conns[3].InBytes)
		}

		c = pollConz(t, s, mode, url+"connz?sort=msgs_from", &ConnzOptions{Sort: ByInMsgs})
		if c.Conns[0].InMsgs < c.Conns[1].InMsgs ||
			c.Conns[0].InMsgs < c.Conns[2].InMsgs ||
			c.Conns[0].InMsgs < c.Conns[3].InMsgs {
			t.Fatalf("Expected conns sorted in descending order by msgs from, got %v < one of [%v, %v, %v]\n",
				c.Conns[0].InMsgs, c.Conns[1].InMsgs, c.Conns[2].InMsgs, c.Conns[3].InMsgs)
		}
	}
}

func TestConnzSortedByPending(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	firstClient := createClientConnSubscribeAndPublish(t, s)
	firstClient.Subscribe("hello.world", func(m *nats.Msg) {})
	clients := make([]*nats.Conn, 3)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
	}
	defer firstClient.Close()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz?sort=pending", &ConnzOptions{Sort: ByPending})
		if c.Conns[0].Pending < c.Conns[1].Pending ||
			c.Conns[0].Pending < c.Conns[2].Pending ||
			c.Conns[0].Pending < c.Conns[3].Pending {
			t.Fatalf("Expected conns sorted in descending order by number of pending, got %v < one of [%v, %v, %v]\n",
				c.Conns[0].Pending, c.Conns[1].Pending, c.Conns[2].Pending, c.Conns[3].Pending)
		}
	}
}

func TestConnzSortedBySubs(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	firstClient := createClientConnSubscribeAndPublish(t, s)
	firstClient.Subscribe("hello.world", func(m *nats.Msg) {})
	defer firstClient.Close()

	clients := make([]*nats.Conn, 3)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz?sort=subs", &ConnzOptions{Sort: BySubs})
		if c.Conns[0].NumSubs < c.Conns[1].NumSubs ||
			c.Conns[0].NumSubs < c.Conns[2].NumSubs ||
			c.Conns[0].NumSubs < c.Conns[3].NumSubs {
			t.Fatalf("Expected conns sorted in descending order by number of subs, got %v < one of [%v, %v, %v]\n",
				c.Conns[0].NumSubs, c.Conns[1].NumSubs, c.Conns[2].NumSubs, c.Conns[3].NumSubs)
		}
	}
}

func TestConnzSortedByLast(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	s := RunServer(opts)
	defer s.Shutdown()

	firstClient := createClientConnSubscribeAndPublish(t, s)
	defer firstClient.Close()
	firstClient.Subscribe("hello.world", func(m *nats.Msg) {})
	firstClient.Flush()

	clients := make([]*nats.Conn, 3)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
		clients[i].Flush()
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz?sort=last", &ConnzOptions{Sort: ByLast})
		if c.Conns[0].LastActivity.UnixNano() < c.Conns[1].LastActivity.UnixNano() ||
			c.Conns[1].LastActivity.UnixNano() < c.Conns[2].LastActivity.UnixNano() ||
			c.Conns[2].LastActivity.UnixNano() < c.Conns[3].LastActivity.UnixNano() {
			t.Fatalf("Expected conns sorted in descending order by lastActivity, got %v < one of [%v, %v, %v]\n",
				c.Conns[0].LastActivity, c.Conns[1].LastActivity, c.Conns[2].LastActivity, c.Conns[3].LastActivity)
		}
	}
}

func TestConnzSortedByUptime(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	for i := 0; i < 4; i++ {
		client := createClientConnSubscribeAndPublish(t, s)
		defer client.Close()
		// Since we check times (now-start) does not have to be big.
		time.Sleep(50 * time.Millisecond)
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz?sort=uptime", &ConnzOptions{Sort: ByUptime})
		now := time.Now()
		ups := make([]int, 4)
		for i := 0; i < 4; i++ {
			ups[i] = int(now.Sub(c.Conns[i].Start))
		}
		if !sort.IntsAreSorted(ups) {
			d := make([]time.Duration, 4)
			for i := 0; i < 4; i++ {
				d[i] = time.Duration(ups[i])
			}
			t.Fatalf("Expected conns sorted in ascending order by uptime (now-Start), got %+v\n", d)
		}
	}
}

func TestConnzSortedByUptimeClosedConn(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	for i := time.Duration(1); i <= 4; i++ {
		c := createClientConnSubscribeAndPublish(t, s)

		// Grab client and asjust start time such that
		client := s.getClient(uint64(i))
		if client == nil {
			t.Fatalf("Could nopt retrieve client for %d\n", i)
		}
		client.mu.Lock()
		client.start = client.start.Add(-10 * (4 - i) * time.Second)
		client.mu.Unlock()

		c.Close()
	}

	checkClosedConns(t, s, 4, time.Second)

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz?state=closed&sort=uptime", &ConnzOptions{State: ConnClosed, Sort: ByUptime})
		ups := make([]int, 4)
		for i := 0; i < 4; i++ {
			ups[i] = int(c.Conns[i].Stop.Sub(c.Conns[i].Start))
		}
		if !sort.IntsAreSorted(ups) {
			d := make([]time.Duration, 4)
			for i := 0; i < 4; i++ {
				d[i] = time.Duration(ups[i])
			}
			t.Fatalf("Expected conns sorted in ascending order by uptime, got %+v\n", d)
		}
	}
}

func TestConnzSortedByStopOnOpen(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	opts := s.getOpts()
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	// 4 clients
	for i := 0; i < 4; i++ {
		c, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Could not create client: %v\n", err)
		}
		defer c.Close()
	}

	c, err := s.Connz(&ConnzOptions{Sort: ByStop})
	if err == nil {
		t.Fatalf("Expected err to be non-nil, got %+v\n", c)
	}
}

func TestConnzSortedByStopTimeClosedConn(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	opts := s.getOpts()
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	// 4 clients
	for i := 0; i < 4; i++ {
		c, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Could not create client: %v\n", err)
		}
		c.Close()
	}
	checkClosedConns(t, s, 4, time.Second)

	// Now adjust the Stop times for these with some random values.
	s.mu.Lock()
	now := time.Now().UTC()
	ccs := s.closed.closedClients()
	for _, cc := range ccs {
		newStop := now.Add(time.Duration(rand.Int()%120) * -time.Minute)
		cc.Stop = &newStop
	}
	s.mu.Unlock()

	url = fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz?state=closed&sort=stop", &ConnzOptions{State: ConnClosed, Sort: ByStop})
		ups := make([]int, 4)
		nowU := time.Now().UnixNano()
		for i := 0; i < 4; i++ {
			ups[i] = int(nowU - c.Conns[i].Stop.UnixNano())
		}
		if !sort.IntsAreSorted(ups) {
			d := make([]time.Duration, 4)
			for i := 0; i < 4; i++ {
				d[i] = time.Duration(ups[i])
			}
			t.Fatalf("Expected conns sorted in ascending order by stop time, got %+v\n", d)
		}
	}
}

func TestConnzSortedByReason(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	opts := s.getOpts()
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	// 20 clients
	for i := 0; i < 20; i++ {
		c, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Could not create client: %v\n", err)
		}
		c.Close()
	}
	checkClosedConns(t, s, 20, time.Second)

	// Now adjust the Reasons for these with some random values.
	s.mu.Lock()
	ccs := s.closed.closedClients()
	max := int(ServerShutdown)
	for _, cc := range ccs {
		cc.Reason = ClosedState(rand.Int() % max).String()
	}
	s.mu.Unlock()

	url = fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz?state=closed&sort=reason", &ConnzOptions{State: ConnClosed, Sort: ByReason})
		rs := make([]string, 20)
		for i := 0; i < 20; i++ {
			rs[i] = c.Conns[i].Reason
		}
		if !sort.StringsAreSorted(rs) {
			t.Fatalf("Expected conns sorted in order by stop reason, got %#v\n", rs)
		}
	}
}

func TestConnzSortedByReasonOnOpen(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	opts := s.getOpts()
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	// 4 clients
	for i := 0; i < 4; i++ {
		c, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Could not create client: %v\n", err)
		}
		defer c.Close()
	}

	c, err := s.Connz(&ConnzOptions{Sort: ByReason})
	if err == nil {
		t.Fatalf("Expected err to be non-nil, got %+v\n", c)
	}
}

func TestConnzSortedByIdle(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	testIdle := func(mode int) {
		firstClient := createClientConnSubscribeAndPublish(t, s)
		defer firstClient.Close()
		firstClient.Subscribe("client.1", func(m *nats.Msg) {})
		firstClient.Flush()

		secondClient := createClientConnSubscribeAndPublish(t, s)
		defer secondClient.Close()

		// Make it such that the second client started 10 secs ago. 10 is important since bug
		// was strcmp, e.g. 1s vs 11s
		var cid uint64
		switch mode {
		case 0:
			cid = uint64(2)
		case 1:
			cid = uint64(4)
		}
		client := s.getClient(cid)
		if client == nil {
			t.Fatalf("Error looking up client %v\n", 2)
		}

		// We want to make sure that we set start/last after the server has finished
		// updating this client's last activity. Doing another Flush() now (even though
		// one is done in createClientConnSubscribeAndPublish) ensures that server has
		// finished updating the client's last activity, since for that last flush there
		// should be no new message/sub/unsub activity.
		secondClient.Flush()

		client.mu.Lock()
		client.start = client.start.Add(-10 * time.Second)
		client.last = client.start
		client.mu.Unlock()

		// The Idle granularity is a whole second
		time.Sleep(time.Second)
		firstClient.Publish("client.1", []byte("new message"))

		c := pollConz(t, s, mode, url+"connz?sort=idle", &ConnzOptions{Sort: ByIdle})
		// Make sure we are returned 2 connections...
		if len(c.Conns) != 2 {
			t.Fatalf("Expected to get two connections, got %v", len(c.Conns))
		}

		// And that the Idle time is valid (even if equal to "0s")
		if c.Conns[0].Idle == "" || c.Conns[1].Idle == "" {
			t.Fatal("Expected Idle value to be valid")
		}

		idle1, err := time.ParseDuration(c.Conns[0].Idle)
		if err != nil {
			t.Fatalf("Unable to parse duration %v, err=%v", c.Conns[0].Idle, err)
		}
		idle2, err := time.ParseDuration(c.Conns[1].Idle)
		if err != nil {
			t.Fatalf("Unable to parse duration %v, err=%v", c.Conns[0].Idle, err)
		}

		if idle2 < idle1 {
			t.Fatalf("Expected conns sorted in descending order by Idle, got %v < %v\n",
				idle2, idle1)
		}
	}
	for mode := 0; mode < 2; mode++ {
		testIdle(mode)
	}
}

func TestConnzSortBadRequest(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	firstClient := createClientConnSubscribeAndPublish(t, s)
	firstClient.Subscribe("hello.world", func(m *nats.Msg) {})
	clients := make([]*nats.Conn, 3)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
	}
	defer firstClient.Close()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	readBodyEx(t, url+"connz?sort=foo", http.StatusBadRequest, textPlain)

	if _, err := s.Connz(&ConnzOptions{Sort: "foo"}); err == nil {
		t.Fatal("Expected error, got none")
	}
}

func pollRoutez(t *testing.T, s *Server, mode int, url string, opts *RoutezOptions) *Routez {
	t.Helper()
	if mode == 0 {
		rz := &Routez{}
		body := readBody(t, url)
		if err := json.Unmarshal(body, rz); err != nil {
			t.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		return rz
	}
	rz, err := s.Routez(opts)
	if err != nil {
		t.Fatalf("Error on Routez: %v", err)
	}
	return rz
}

func TestConnzWithRoutes(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.Cluster.Name = "A"
	opts.Cluster.Host = "127.0.0.1"
	opts.Cluster.Port = CLUSTER_PORT

	s := RunServer(opts)
	defer s.Shutdown()

	opts = &Options{
		Host: "127.0.0.1",
		Port: -1,
		Cluster: ClusterOpts{
			Name: "A",
			Host: "127.0.0.1",
			Port: -1,
		},
		NoLog:           true,
		NoSigs:          true,
		NoSystemAccount: true,
	}
	routeURL, _ := url.Parse(fmt.Sprintf("nats-route://127.0.0.1:%d", s.ClusterAddr().Port))
	opts.Routes = []*url.URL{routeURL}

	start := time.Now()
	sc := RunServer(opts)
	defer sc.Shutdown()

	checkClusterFormed(t, s, sc)

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz", nil)
		// Test contents..
		// Make sure routes don't show up under connz, but do under routez
		if c.NumConns != 0 {
			t.Fatalf("Expected 0 connections, got %d", c.NumConns)
		}
		if c.Conns == nil || len(c.Conns) != 0 {
			t.Fatalf("Expected 0 connections in array, got %p", c.Conns)
		}
	}

	nc := createClientConnSubscribeAndPublish(t, sc)
	defer nc.Close()

	nc.Subscribe("hello.bar", func(m *nats.Msg) {})
	nc.Flush()
	checkExpectedSubs(t, 1, s, sc)

	// Now check routez
	urls := []string{"routez", "routez?subs=1", "routez?subs=detail"}
	for subs, urlSuffix := range urls {
		for mode := 0; mode < 2; mode++ {
			rz := pollRoutez(t, s, mode, url+urlSuffix, &RoutezOptions{Subscriptions: subs == 1, SubscriptionsDetail: subs == 2})

			if rz.NumRoutes != DEFAULT_ROUTE_POOL_SIZE {
				t.Fatalf("Expected %d route, got %d", DEFAULT_ROUTE_POOL_SIZE, rz.NumRoutes)
			}

			if len(rz.Routes) != DEFAULT_ROUTE_POOL_SIZE {
				t.Fatalf("Expected route array of %d, got %v", DEFAULT_ROUTE_POOL_SIZE, len(rz.Routes))
			}

			route := rz.Routes[0]

			if route.DidSolicit {
				t.Fatalf("Expected unsolicited route, got %v", route.DidSolicit)
			}

			if route.Start.IsZero() {
				t.Fatalf("Expected Start to be set, got %+v", route)
			} else if route.Start.Before(start) {
				t.Fatalf("Unexpected start time: route was started around %v, got %v", start, route.Start)
			}
			if route.LastActivity.IsZero() {
				t.Fatalf("Expected LastActivity to be set, got %+v", route)
			}
			if route.Uptime == _EMPTY_ {
				t.Fatalf("Expected Uptime to be set, it was not")
			}
			if route.Idle == _EMPTY_ {
				t.Fatalf("Expected Idle to be set, it was not")
			}

			// Don't ask for subs, so there should not be any
			if subs == 0 {
				if len(route.Subs) != 0 {
					t.Fatalf("There should not be subs, got %v", len(route.Subs))
				}
			} else if subs == 1 {
				if len(route.Subs) != 1 && len(route.SubsDetail) != 0 {
					t.Fatalf("There should be 1 sub, got %v", len(route.Subs))
				}
			} else if subs == 2 {
				if len(route.SubsDetail) != 1 && len(route.Subs) != 0 {
					t.Fatalf("There should be 1 sub, got %v", len(route.SubsDetail))
				}
			}
		}
	}

	// Test JSONP
	readBodyEx(t, url+"routez?callback=callback", http.StatusOK, appJSContent)
}

func TestRoutezWithBadParams(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/routez?", s.MonitorAddr().Port)
	readBodyEx(t, url+"subs=xxx", http.StatusBadRequest, textPlain)
}

func pollSubsz(t *testing.T, s *Server, mode int, url string, opts *SubszOptions) *Subsz {
	t.Helper()
	if mode == 0 {
		body := readBody(t, url)
		sz := &Subsz{}
		if err := json.Unmarshal(body, sz); err != nil {
			t.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		return sz
	}
	sz, err := s.Subsz(opts)
	if err != nil {
		t.Fatalf("Error on Subsz: %v", err)
	}
	return sz
}

func TestSubsz(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		sl := pollSubsz(t, s, mode, url+"subsz", nil)
		if sl.NumSubs != 0 {
			t.Fatalf("Expected NumSubs of 0, got %d\n", sl.NumSubs)
		}
		if sl.NumInserts != 1 {
			t.Fatalf("Expected NumInserts of 1, got %d\n", sl.NumInserts)
		}
		if sl.NumMatches != 1 {
			t.Fatalf("Expected NumMatches of 1, got %d\n", sl.NumMatches)
		}
	}

	// Test JSONP
	readBodyEx(t, url+"subsz?callback=callback", http.StatusOK, appJSContent)
}

func TestSubszDetails(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	nc.Subscribe("foo.*", func(m *nats.Msg) {})
	nc.Subscribe("foo.bar", func(m *nats.Msg) {})
	nc.Subscribe("foo.foo", func(m *nats.Msg) {})

	nc.Publish("foo.bar", []byte("Hello"))
	nc.Publish("foo.baz", []byte("Hello"))
	nc.Publish("foo.foo", []byte("Hello"))

	nc.Flush()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		sl := pollSubsz(t, s, mode, url+"subsz?subs=1", &SubszOptions{Subscriptions: true})
		if sl.NumSubs != 3 {
			t.Fatalf("Expected NumSubs of 3, got %d\n", sl.NumSubs)
		}
		if sl.Total != 3 {
			t.Fatalf("Expected Total of 3, got %d\n", sl.Total)
		}
		if len(sl.Subs) != 3 {
			t.Fatalf("Expected subscription details for 3 subs, got %d\n", len(sl.Subs))
		}
	}
}

func TestSubszWithOffsetAndLimit(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	for i := 0; i < 200; i++ {
		nc.Subscribe(fmt.Sprintf("foo.%d", i), func(m *nats.Msg) {})
	}
	nc.Flush()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		sl := pollSubsz(t, s, mode, url+"subsz?subs=1&offset=10&limit=100", &SubszOptions{Subscriptions: true, Offset: 10, Limit: 100})
		if sl.NumSubs != 200 {
			t.Fatalf("Expected NumSubs of 200, got %d\n", sl.NumSubs)
		}
		if sl.Total != 100 {
			t.Fatalf("Expected Total of 100, got %d\n", sl.Total)
		}
		if sl.Offset != 10 {
			t.Fatalf("Expected Offset of 10, got %d\n", sl.Offset)
		}
		if sl.Limit != 100 {
			t.Fatalf("Expected Total of 100, got %d\n", sl.Limit)
		}
		if len(sl.Subs) != 100 {
			t.Fatalf("Expected subscription details for 100 subs, got %d\n", len(sl.Subs))
		}
	}
}

func TestSubszTestPubSubject(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	nc.Subscribe("foo.*", func(m *nats.Msg) {})
	nc.Subscribe("foo.bar", func(m *nats.Msg) {})
	nc.Subscribe("foo.foo", func(m *nats.Msg) {})
	nc.Flush()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		sl := pollSubsz(t, s, mode, url+"subsz?subs=1&test=foo.foo", &SubszOptions{Subscriptions: true, Test: "foo.foo"})
		if sl.Total != 2 {
			t.Fatalf("Expected Total of 2 match, got %d\n", sl.Total)
		}
		if len(sl.Subs) != 2 {
			t.Fatalf("Expected subscription details for 2 matching subs, got %d\n", len(sl.Subs))
		}
		sl = pollSubsz(t, s, mode, url+"subsz?subs=1&test=foo", &SubszOptions{Subscriptions: true, Test: "foo"})
		if len(sl.Subs) != 0 {
			t.Fatalf("Expected no matching subs, got %d\n", len(sl.Subs))
		}
	}
	// Make sure we get an error with invalid test subject.
	testUrl := url + "subsz?subs=1&"
	readBodyEx(t, testUrl+"test=*", http.StatusBadRequest, textPlain)
	readBodyEx(t, testUrl+"test=foo.*", http.StatusBadRequest, textPlain)
	readBodyEx(t, testUrl+"test=foo.>", http.StatusBadRequest, textPlain)
	readBodyEx(t, testUrl+"test=foo..bar", http.StatusBadRequest, textPlain)
}

func TestSubszMultiAccount(t *testing.T) {
	s := runMonitorServerWithAccounts()
	defer s.Shutdown()

	ncA := createClientConnWithUserSubscribeAndPublish(t, s, "a", "a")
	defer ncA.Close()

	ncA.Subscribe("foo.*", func(m *nats.Msg) {})
	ncA.Subscribe("foo.bar", func(m *nats.Msg) {})
	ncA.Subscribe("foo.foo", func(m *nats.Msg) {})

	ncA.Publish("foo.bar", []byte("Hello"))
	ncA.Publish("foo.baz", []byte("Hello"))
	ncA.Publish("foo.foo", []byte("Hello"))

	ncA.Flush()

	ncB := createClientConnWithUserSubscribeAndPublish(t, s, "b", "b")
	defer ncB.Close()

	ncB.Subscribe("foo.*", func(m *nats.Msg) {})
	ncB.Subscribe("foo.bar", func(m *nats.Msg) {})
	ncB.Subscribe("foo.foo", func(m *nats.Msg) {})

	ncB.Publish("foo.bar", []byte("Hello"))
	ncB.Publish("foo.baz", []byte("Hello"))
	ncB.Publish("foo.foo", []byte("Hello"))

	ncB.Flush()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		sl := pollSubsz(t, s, mode, url+"subsz?subs=1", &SubszOptions{Subscriptions: true})
		if sl.NumSubs != 6 {
			t.Fatalf("Expected NumSubs of 6, got %d\n", sl.NumSubs)
		}
		if sl.Total != 6 {
			t.Fatalf("Expected Total of 6, got %d\n", sl.Total)
		}
		if len(sl.Subs) != 6 {
			t.Fatalf("Expected subscription details for 6 subs, got %d\n", len(sl.Subs))
		}
		for _, sd := range sl.Subs {
			if sd.Account != "A" && sd.Account != "B" {
				t.Fatalf("Expected account information to be present and be 'A' or 'B', got %q", sd.Account)
			}
		}

		// Now make sure we can filter on account.
		sl = pollSubsz(t, s, mode, url+"subsz?subs=1&acc=A", &SubszOptions{Account: "A", Subscriptions: true})
		if sl.NumSubs != 3 {
			t.Fatalf("Expected NumSubs of 3, got %d\n", sl.NumSubs)
		}
		if sl.Total != 3 {
			t.Fatalf("Expected Total of 6, got %d\n", sl.Total)
		}
		if len(sl.Subs) != 3 {
			t.Fatalf("Expected subscription details for 6 subs, got %d\n", len(sl.Subs))
		}
		for _, sd := range sl.Subs {
			if sd.Account != "A" {
				t.Fatalf("Expected account information to be present and be 'A', got %q", sd.Account)
			}
		}
	}
}

func TestSubszMultiAccountWithOffsetAndLimit(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	ncA := createClientConnWithUserSubscribeAndPublish(t, s, "a", "a")
	defer ncA.Close()

	for i := 0; i < 200; i++ {
		ncA.Subscribe(fmt.Sprintf("foo.%d", i), func(m *nats.Msg) {})
	}
	ncA.Flush()

	ncB := createClientConnWithUserSubscribeAndPublish(t, s, "b", "b")
	defer ncB.Close()

	for i := 0; i < 200; i++ {
		ncB.Subscribe(fmt.Sprintf("foo.%d", i), func(m *nats.Msg) {})
	}
	ncB.Flush()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		sl := pollSubsz(t, s, mode, url+"subsz?subs=1&offset=10&limit=100", &SubszOptions{Subscriptions: true, Offset: 10, Limit: 100})
		if sl.NumSubs != 400 {
			t.Fatalf("Expected NumSubs of 200, got %d\n", sl.NumSubs)
		}
		if sl.Total != 100 {
			t.Fatalf("Expected Total of 100, got %d\n", sl.Total)
		}
		if sl.Offset != 10 {
			t.Fatalf("Expected Offset of 10, got %d\n", sl.Offset)
		}
		if sl.Limit != 100 {
			t.Fatalf("Expected Total of 100, got %d\n", sl.Limit)
		}
		if len(sl.Subs) != 100 {
			t.Fatalf("Expected subscription details for 100 subs, got %d\n", len(sl.Subs))
		}
	}
}

// Tests handle root
func TestHandleRoot(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port))
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected a %d response, got %d\n", http.StatusOK, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Expected no error reading body: Got %v\n", err)
	}
	for _, b := range body {
		if b > unicode.MaxASCII {
			t.Fatalf("Expected body to contain only ASCII characters, but got %v\n", b)
		}
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Fatalf("Expected text/html response, got %s\n", ct)
	}
}

func TestConnzWithNamedClient(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	clientName := "test-client"
	nc := createClientConnWithName(t, clientName, s)
	defer nc.Close()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		// Confirm server is exposing client name in monitoring endpoint.
		c := pollConz(t, s, mode, url+"connz", nil)
		got := len(c.Conns)
		expected := 1
		if got != expected {
			t.Fatalf("Expected %d connection in array, got %d\n", expected, got)
		}

		conn := c.Conns[0]
		if conn.Name != clientName {
			t.Fatalf("Expected client to have name %q. got %q", clientName, conn.Name)
		}
	}
}

func TestConnzWithStateForClosedConns(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	numEach := 10
	// Create 10 closed, and 10 to leave open.
	for i := 0; i < numEach; i++ {
		nc := createClientConnSubscribeAndPublish(t, s)
		nc.Subscribe("hello.closed.conns", func(m *nats.Msg) {})
		nc.Close()
		nc = createClientConnSubscribeAndPublish(t, s)
		nc.Subscribe("hello.open.conns", func(m *nats.Msg) {})
		defer nc.Close()
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
			// Look at all open
			c := pollConz(t, s, mode, url+"connz?state=open", &ConnzOptions{State: ConnOpen})
			if lc := len(c.Conns); lc != numEach {
				return fmt.Errorf("Expected %d connections in array, got %d", numEach, lc)
			}
			// Look at all closed
			c = pollConz(t, s, mode, url+"connz?state=closed", &ConnzOptions{State: ConnClosed})
			if lc := len(c.Conns); lc != numEach {
				return fmt.Errorf("Expected %d connections in array, got %d", numEach, lc)
			}
			// Look at all
			c = pollConz(t, s, mode, url+"connz?state=ALL", &ConnzOptions{State: ConnAll})
			if lc := len(c.Conns); lc != numEach*2 {
				return fmt.Errorf("Expected %d connections in array, got %d", 2*numEach, lc)
			}
			// Look at CID #1, which is in closed.
			c = pollConz(t, s, mode, url+"connz?cid=1&state=open", &ConnzOptions{CID: 1, State: ConnOpen})
			if lc := len(c.Conns); lc != 0 {
				return fmt.Errorf("Expected no connections in open array, got %d", lc)
			}
			c = pollConz(t, s, mode, url+"connz?cid=1&state=closed", &ConnzOptions{CID: 1, State: ConnClosed})
			if lc := len(c.Conns); lc != 1 {
				return fmt.Errorf("Expected a connection in closed array, got %d", lc)
			}
			c = pollConz(t, s, mode, url+"connz?cid=1&state=ALL", &ConnzOptions{CID: 1, State: ConnAll})
			if lc := len(c.Conns); lc != 1 {
				return fmt.Errorf("Expected a connection in closed array, got %d", lc)
			}
			c = pollConz(t, s, mode, url+"connz?cid=1&state=closed&subs=true",
				&ConnzOptions{CID: 1, State: ConnClosed, Subscriptions: true})
			if lc := len(c.Conns); lc != 1 {
				return fmt.Errorf("Expected a connection in closed array, got %d", lc)
			}
			ci := c.Conns[0]
			if ci.NumSubs != 1 {
				return fmt.Errorf("Expected NumSubs to be 1, got %d", ci.NumSubs)
			}
			if len(ci.Subs) != 1 {
				return fmt.Errorf("Expected len(ci.Subs) to be 1 also, got %d", len(ci.Subs))
			}
			// Now ask for same thing without subs and make sure they are not returned.
			c = pollConz(t, s, mode, url+"connz?cid=1&state=closed&subs=false",
				&ConnzOptions{CID: 1, State: ConnClosed, Subscriptions: false})
			if lc := len(c.Conns); lc != 1 {
				return fmt.Errorf("Expected a connection in closed array, got %d", lc)
			}
			ci = c.Conns[0]
			if ci.NumSubs != 1 {
				return fmt.Errorf("Expected NumSubs to be 1, got %d", ci.NumSubs)
			}
			if len(ci.Subs) != 0 {
				return fmt.Errorf("Expected len(ci.Subs) to be 0 since subs=false, got %d", len(ci.Subs))
			}

			// CID #2 is in open
			c = pollConz(t, s, mode, url+"connz?cid=2&state=open", &ConnzOptions{CID: 2, State: ConnOpen})
			if lc := len(c.Conns); lc != 1 {
				return fmt.Errorf("Expected a connection in open array, got %d", lc)
			}
			c = pollConz(t, s, mode, url+"connz?cid=2&state=closed", &ConnzOptions{CID: 2, State: ConnClosed})
			if lc := len(c.Conns); lc != 0 {
				return fmt.Errorf("Expected no connections in closed array, got %d", lc)
			}
			return nil
		})
	}
}

// Make sure options for ConnInfo like subs=1, authuser, etc do not cause a race.
func TestConnzClosedConnsRace(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	// Create 100 closed connections.
	for i := 0; i < 100; i++ {
		nc := createClientConnSubscribeAndPublish(t, s)
		nc.Close()
	}

	urlWithoutSubs := fmt.Sprintf("http://127.0.0.1:%d/connz?state=closed", s.MonitorAddr().Port)
	urlWithSubs := urlWithoutSubs + "&subs=true"

	checkClosedConns(t, s, 100, 2*time.Second)

	wg := &sync.WaitGroup{}

	fn := func(url string) {
		deadline := time.Now().Add(1 * time.Second)
		for time.Now().Before(deadline) {
			c := pollConz(t, s, 0, url, nil)
			if len(c.Conns) != 100 {
				t.Errorf("Incorrect Results: %+v\n", c)
			}
		}
		wg.Done()
	}

	wg.Add(2)
	go fn(urlWithSubs)
	go fn(urlWithoutSubs)
	wg.Wait()
}

// Make sure a bad client that is disconnected right away has proper values.
func TestConnzClosedConnsBadClient(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	opts := s.getOpts()

	rc, err := net.Dial("tcp", fmt.Sprintf("%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on dial: %v", err)
	}
	rc.Close()

	checkClosedConns(t, s, 1, 2*time.Second)

	c := pollConz(t, s, 1, "", &ConnzOptions{State: ConnClosed})
	if len(c.Conns) != 1 {
		t.Errorf("Incorrect Results: %+v\n", c)
	}
	ci := c.Conns[0]

	uptime := ci.Stop.Sub(ci.Start)
	idle, err := time.ParseDuration(ci.Idle)
	if err != nil {
		t.Fatalf("Could not parse Idle: %v\n", err)
	}
	if idle > uptime {
		t.Fatalf("Idle can't be larger then uptime, %v vs %v\n", idle, uptime)
	}
	if ci.LastActivity.IsZero() {
		t.Fatalf("LastActivity should not be Zero\n")
	}
}

// Make sure a bad client that tries to connect plain to TLS has proper values.
func TestConnzClosedConnsBadTLSClient(t *testing.T) {
	resetPreviousHTTPConnections()

	tc := &TLSConfigOpts{}
	tc.CertFile = "configs/certs/server.pem"
	tc.KeyFile = "configs/certs/key.pem"

	var err error
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.TLSTimeout = 1.5 // 1.5 seconds
	opts.TLSConfig, err = GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error creating TSL config: %v", err)
	}

	s := RunServer(opts)
	defer s.Shutdown()

	opts = s.getOpts()

	rc, err := net.Dial("tcp", fmt.Sprintf("%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on dial: %v", err)
	}
	rc.Write([]byte("CONNECT {}\r\n"))
	rc.Close()

	checkClosedConns(t, s, 1, 2*time.Second)

	c := pollConz(t, s, 1, "", &ConnzOptions{State: ConnClosed})
	if len(c.Conns) != 1 {
		t.Errorf("Incorrect Results: %+v\n", c)
	}
	ci := c.Conns[0]

	uptime := ci.Stop.Sub(ci.Start)
	idle, err := time.ParseDuration(ci.Idle)
	if err != nil {
		t.Fatalf("Could not parse Idle: %v\n", err)
	}
	if idle > uptime {
		t.Fatalf("Idle can't be larger then uptime, %v vs %v\n", idle, uptime)
	}
	if ci.LastActivity.IsZero() {
		t.Fatalf("LastActivity should not be Zero\n")
	}
}

// Create a connection to test ConnInfo
func createClientConnWithUserSubscribeAndPublish(t *testing.T, s *Server, user, pwd string) *nats.Conn {
	natsURL := ""
	if user == "" {
		natsURL = fmt.Sprintf("nats://127.0.0.1:%d", s.Addr().(*net.TCPAddr).Port)
	} else {
		natsURL = fmt.Sprintf("nats://%s:%s@127.0.0.1:%d", user, pwd, s.Addr().(*net.TCPAddr).Port)
	}
	client := nats.DefaultOptions
	client.Servers = []string{natsURL}
	nc, err := client.Connect()
	if err != nil {
		t.Fatalf("Error creating client: %v to: %s\n", err, natsURL)
	}

	ch := make(chan bool)
	inbox := nats.NewInbox()
	sub, err := nc.Subscribe(inbox, func(m *nats.Msg) { ch <- true })
	if err != nil {
		t.Fatalf("Error subscribing to `%s`: %v\n", inbox, err)
	}
	nc.Publish(inbox, []byte("Hello"))
	// Wait for message
	<-ch
	sub.Unsubscribe()
	close(ch)
	nc.Flush()
	return nc
}

func createClientConnSubscribeAndPublish(t *testing.T, s *Server) *nats.Conn {
	return createClientConnWithUserSubscribeAndPublish(t, s, "", "")
}

func createClientConnWithName(t *testing.T, name string, s *Server) *nats.Conn {
	natsURI := fmt.Sprintf("nats://127.0.0.1:%d", s.Addr().(*net.TCPAddr).Port)

	client := nats.DefaultOptions
	client.Servers = []string{natsURI}
	client.Name = name
	nc, err := client.Connect()
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	return nc
}

func TestStacksz(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	body := readBody(t, url+"stacksz")
	// Check content
	str := string(body)
	if !strings.Contains(str, "HandleStacksz") {
		t.Fatalf("Result does not seem to contain server's stacks:\n%v", str)
	}
}

func TestConcurrentMonitoring(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	// Get some endpoints. Make sure we have at least varz,
	// and the more the merrier.
	endpoints := []string{"varz", "varz", "varz", "connz", "connz", "subsz", "subsz", "routez", "routez"}
	wg := &sync.WaitGroup{}
	wg.Add(len(endpoints))
	ech := make(chan string, len(endpoints))

	for _, e := range endpoints {
		go func(endpoint string) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				resp, err := http.Get(url + endpoint)
				if err != nil {
					ech <- fmt.Sprintf("Expected no error: Got %v\n", err)
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					ech <- fmt.Sprintf("Expected a %v response, got %d\n", http.StatusOK, resp.StatusCode)
					return
				}
				ct := resp.Header.Get("Content-Type")
				if ct != "application/json" {
					ech <- fmt.Sprintf("Expected application/json content-type, got %s\n", ct)
					return
				}
				if _, err := io.ReadAll(resp.Body); err != nil {
					ech <- fmt.Sprintf("Got an error reading the body: %v\n", err)
					return
				}
				resp.Body.Close()
			}
		}(e)
	}
	wg.Wait()
	// Check for any errors
	select {
	case err := <-ech:
		t.Fatal(err)
	default:
	}
}

func TestMonitorHandler(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()
	handler := s.HTTPHandler()
	if handler == nil {
		t.Fatal("HTTP Handler should be set")
	}
	s.Shutdown()
	handler = s.HTTPHandler()
	if handler != nil {
		t.Fatal("HTTP Handler should be nil")
	}
}

func TestMonitorRoutezRace(t *testing.T) {
	resetPreviousHTTPConnections()
	srvAOpts := DefaultMonitorOptions()
	srvAOpts.NoSystemAccount = true
	srvAOpts.Cluster.Name = "B"
	srvAOpts.Cluster.Port = -1
	srvA := RunServer(srvAOpts)
	defer srvA.Shutdown()

	srvBOpts := nextServerOpts(srvAOpts)
	srvBOpts.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srvA.ClusterAddr().Port))

	doneCh := make(chan struct{})
	go func() {
		defer func() {
			doneCh <- struct{}{}
		}()
		for i := 0; i < 10; i++ {
			time.Sleep(10 * time.Millisecond)
			// Reset ports
			srvBOpts.Port = -1
			srvBOpts.Cluster.Port = -1
			srvB := RunServer(srvBOpts)
			time.Sleep(20 * time.Millisecond)
			srvB.Shutdown()
		}
	}()
	done := false
	for !done {
		if _, err := srvA.Routez(nil); err != nil {
			time.Sleep(10 * time.Millisecond)
		}
		select {
		case <-doneCh:
			done = true
		default:
		}
	}
}

func TestConnzTLSInHandshake(t *testing.T) {
	resetPreviousHTTPConnections()

	tc := &TLSConfigOpts{}
	tc.CertFile = "configs/certs/server.pem"
	tc.KeyFile = "configs/certs/key.pem"

	var err error
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.TLSTimeout = 1.5 // 1.5 seconds
	opts.TLSConfig, err = GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error creating TSL config: %v", err)
	}

	s := RunServer(opts)
	defer s.Shutdown()

	// Create bare TCP connection to delay client TLS handshake
	c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on dial: %v", err)
	}
	defer c.Close()

	// Wait for the connection to be registered
	checkClientsCount(t, s, 1)

	start := time.Now()
	endpoint := fmt.Sprintf("http://%s:%d/connz", opts.HTTPHost, s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		connz := pollConz(t, s, mode, endpoint, nil)
		duration := time.Since(start)
		if duration >= 1500*time.Millisecond {
			t.Fatalf("Looks like connz blocked on handshake, took %v", duration)
		}
		if len(connz.Conns) != 1 {
			t.Fatalf("Expected 1 conn, got %v", len(connz.Conns))
		}
		conn := connz.Conns[0]
		// TLS fields should be not set
		if conn.TLSVersion != "" || conn.TLSCipher != "" {
			t.Fatalf("Expected TLS fields to not be set, got version:%v cipher:%v", conn.TLSVersion, conn.TLSCipher)
		}
	}
}

func TestConnzTLSCfg(t *testing.T) {
	resetPreviousHTTPConnections()

	tc := &TLSConfigOpts{}
	tc.CertFile = "configs/certs/server.pem"
	tc.KeyFile = "configs/certs/key.pem"

	var err error
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.TLSTimeout = 1.5 // 1.5 seconds
	opts.TLSConfig, err = GenTLSConfig(tc)
	require_NoError(t, err)
	opts.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
	opts.Gateway.TLSConfig, err = GenTLSConfig(tc)
	require_NoError(t, err)
	opts.Gateway.TLSTimeout = 1.5
	opts.LeafNode.TLSConfig, err = GenTLSConfig(tc)
	require_NoError(t, err)
	opts.LeafNode.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
	opts.LeafNode.TLSTimeout = 1.5
	opts.Cluster.TLSConfig, err = GenTLSConfig(tc)
	require_NoError(t, err)
	opts.Cluster.TLSTimeout = 1.5

	s := RunServer(opts)
	defer s.Shutdown()

	check := func(verify, required bool, timeout float64) {
		t.Helper()
		if !verify {
			t.Fatalf("Expected tls_verify to be true")
		}
		if !required {
			t.Fatalf("Expected tls_required to be true")
		}
		if timeout != 1.5 {
			t.Fatalf("Expected tls_timeout to be 1.5")
		}
	}

	start := time.Now()
	endpoint := fmt.Sprintf("http://%s:%d/varz", opts.HTTPHost, s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		varz := pollVarz(t, s, mode, endpoint, nil)
		duration := time.Since(start)
		if duration >= 1500*time.Millisecond {
			t.Fatalf("Looks like varz blocked on handshake, took %v", duration)
		}
		check(varz.TLSVerify, varz.TLSRequired, varz.TLSTimeout)
		check(varz.Cluster.TLSVerify, varz.Cluster.TLSRequired, varz.Cluster.TLSTimeout)
		check(varz.Gateway.TLSVerify, varz.Gateway.TLSRequired, varz.Gateway.TLSTimeout)
		check(varz.LeafNode.TLSVerify, varz.LeafNode.TLSRequired, varz.LeafNode.TLSTimeout)
	}
}

func TestConnzTLSPeerCerts(t *testing.T) {
	resetPreviousHTTPConnections()

	tc := &TLSConfigOpts{}
	tc.CertFile = "../test/configs/certs/server-cert.pem"
	tc.KeyFile = "../test/configs/certs/server-key.pem"
	tc.CaFile = "../test/configs/certs/ca.pem"
	tc.Verify = true
	tc.Timeout = 2.0

	var err error
	opts := DefaultMonitorOptions()
	opts.TLSConfig, err = GenTLSConfig(tc)
	require_NoError(t, err)

	s := RunServer(opts)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(),
		nats.ClientCert("../test/configs/certs/client-cert.pem", "../test/configs/certs/client-key.pem"),
		nats.RootCAs("../test/configs/certs/ca.pem"))
	defer nc.Close()

	endpoint := fmt.Sprintf("http://%s:%d/connz", opts.HTTPHost, s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		// Without "auth" option, we should not get the details
		connz := pollConz(t, s, mode, endpoint, nil)
		require_True(t, len(connz.Conns) == 1)
		c := connz.Conns[0]
		if c.TLSPeerCerts != nil {
			t.Fatalf("Did not expect TLSPeerCerts when auth is not specified: %+v", c.TLSPeerCerts)
		}
		// Now specify "auth" option
		connz = pollConz(t, s, mode, endpoint+"?auth=1", &ConnzOptions{Username: true})
		require_True(t, len(connz.Conns) == 1)
		c = connz.Conns[0]
		if c.TLSPeerCerts == nil {
			t.Fatal("Expected TLSPeerCerts to be set, was not")
		} else if len(c.TLSPeerCerts) != 1 {
			t.Fatalf("Unexpected peer certificates: %+v", c.TLSPeerCerts)
		} else {
			for _, d := range c.TLSPeerCerts {
				if d.Subject != "CN=localhost,OU=nats.io,O=Synadia,ST=California,C=US" {
					t.Fatalf("Unexpected subject: %s", d.Subject)
				}
				if len(d.SubjectPKISha256) != 64 {
					t.Fatalf("Unexpected spki_sha256: %s", d.SubjectPKISha256)
				}
				if len(d.CertSha256) != 64 {
					t.Fatalf("Unexpected cert_sha256: %s", d.CertSha256)
				}
			}
		}
	}
}

func TestServerIDs(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	murl := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, s, mode, murl+"varz", nil)
		if v.ID == _EMPTY_ {
			t.Fatal("Varz ID is empty")
		}
		c := pollConz(t, s, mode, murl+"connz", nil)
		if c.ID == _EMPTY_ {
			t.Fatal("Connz ID is empty")
		}
		r := pollRoutez(t, s, mode, murl+"routez", nil)
		if r.ID == _EMPTY_ {
			t.Fatal("Routez ID is empty")
		}
		if v.ID != c.ID || v.ID != r.ID {
			t.Fatalf("Varz ID [%s] is not equal to Connz ID [%s] or Routez ID [%s]", v.ID, c.ID, r.ID)
		}
	}
}

func TestHttpStatsNoUpdatedWhenUsingServerFuncs(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	for i := 0; i < 10; i++ {
		s.Varz(nil)
		s.Connz(nil)
		s.Routez(nil)
		s.Subsz(nil)
	}

	v, _ := s.Varz(nil)
	endpoints := []string{VarzPath, ConnzPath, RoutezPath, SubszPath}
	for _, e := range endpoints {
		stats := v.HTTPReqStats[e]
		if stats != 0 {
			t.Fatalf("Expected HTTPReqStats for %q to be 0, got %v", e, stats)
		}
	}
}

func TestClusterEmptyWhenNotDefined(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	body := readBody(t, fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port))
	var v map[string]interface{}
	if err := json.Unmarshal(body, &v); err != nil {
		stackFatalf(t, "Got an error unmarshalling the body: %v\n", err)
	}
	// Cluster can empty, or be defined but that needs to be empty.
	c, ok := v["cluster"]
	if !ok {
		return
	}
	if len(c.(map[string]interface{})) != 0 {
		t.Fatalf("Expected an empty cluster definition, instead got %+v\n", c)
	}
}

func TestRoutezPermissions(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.Cluster.Name = "A"
	opts.Cluster.Host = "127.0.0.1"
	opts.Cluster.Port = -1
	opts.Cluster.Permissions = &RoutePermissions{
		Import: &SubjectPermission{
			Allow: []string{"foo"},
		},
		Export: &SubjectPermission{
			Allow: []string{"*"},
			Deny:  []string{"foo", "nats"},
		},
	}

	s1 := RunServer(opts)
	defer s1.Shutdown()

	opts = DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.ServerName = "monitor_server_2"
	opts.Cluster.Host = "127.0.0.1"
	opts.Cluster.Name = "A"
	opts.Cluster.Port = -1
	routeURL, _ := url.Parse(fmt.Sprintf("nats-route://127.0.0.1:%d", s1.ClusterAddr().Port))
	opts.Routes = []*url.URL{routeURL}
	opts.HTTPPort = -1

	s2 := RunServer(opts)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	urls := []string{
		fmt.Sprintf("http://127.0.0.1:%d/routez", s1.MonitorAddr().Port),
		fmt.Sprintf("http://127.0.0.1:%d/routez", s2.MonitorAddr().Port),
	}
	servers := []*Server{s1, s2}

	for i, url := range urls {
		for mode := 0; mode < 2; mode++ {
			rz := pollRoutez(t, servers[i], mode, url, nil)
			// For server 1, we expect to see imports and exports
			if i == 0 {
				if rz.Import == nil || rz.Import.Allow == nil ||
					len(rz.Import.Allow) != 1 || rz.Import.Allow[0] != "foo" ||
					rz.Import.Deny != nil {
					t.Fatalf("Unexpected Import %v", rz.Import)
				}
				if rz.Export == nil || rz.Export.Allow == nil || rz.Export.Deny == nil ||
					len(rz.Export.Allow) != 1 || rz.Export.Allow[0] != "*" ||
					len(rz.Export.Deny) != 2 || rz.Export.Deny[0] != "foo" || rz.Export.Deny[1] != "nats" {
					t.Fatalf("Unexpected Export %v", rz.Export)
				}
			} else {
				// We expect to see NO imports and exports for server B by default.
				if rz.Import != nil {
					t.Fatal("Routez body should NOT contain \"import\" information.")
				}
				if rz.Export != nil {
					t.Fatal("Routez body should NOT contain \"export\" information.")
				}
				// We do expect to see them show up for the information we have on Server A though.
				if len(rz.Routes) != DEFAULT_ROUTE_POOL_SIZE {
					t.Fatalf("Expected route array of %d, got %v\n", DEFAULT_ROUTE_POOL_SIZE, len(rz.Routes))
				}
				route := rz.Routes[0]
				if route.Import == nil || route.Import.Allow == nil ||
					len(route.Import.Allow) != 1 || route.Import.Allow[0] != "foo" ||
					route.Import.Deny != nil {
					t.Fatalf("Unexpected Import %v", route.Import)
				}
				if route.Export == nil || route.Export.Allow == nil || route.Export.Deny == nil ||
					len(route.Export.Allow) != 1 || route.Export.Allow[0] != "*" ||
					len(route.Export.Deny) != 2 || route.Export.Deny[0] != "foo" || route.Export.Deny[1] != "nats" {
					t.Fatalf("Unexpected Export %v", route.Export)
				}
			}
		}
	}
}

// Benchmark our Connz generation. Don't use HTTP here, just measure server endpoint.
func Benchmark_Connz(b *testing.B) {
	runtime.MemProfileRate = 0

	s := runMonitorServerNoHTTPPort()
	defer s.Shutdown()

	opts := s.getOpts()
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	// Create 250 connections with 100 subs each.
	for i := 0; i < 250; i++ {
		nc, err := nats.Connect(url)
		if err != nil {
			b.Fatalf("Error on connection[%d] to %s: %v", i, url, err)
		}
		for x := 0; x < 100; x++ {
			subj := fmt.Sprintf("foo.%d", x)
			nc.Subscribe(subj, func(m *nats.Msg) {})
		}
		nc.Flush()
		defer nc.Close()
	}

	b.ResetTimer()
	runtime.MemProfileRate = 1

	copts := &ConnzOptions{Subscriptions: false}
	for i := 0; i < b.N; i++ {
		_, err := s.Connz(copts)
		if err != nil {
			b.Fatalf("Error on Connz(): %v", err)
		}
	}
}

func Benchmark_Varz(b *testing.B) {
	runtime.MemProfileRate = 0

	s := runMonitorServerNoHTTPPort()
	defer s.Shutdown()

	b.ResetTimer()
	runtime.MemProfileRate = 1

	for i := 0; i < b.N; i++ {
		_, err := s.Varz(nil)
		if err != nil {
			b.Fatalf("Error on Connz(): %v", err)
		}
	}
}

func Benchmark_VarzHttp(b *testing.B) {
	runtime.MemProfileRate = 0

	s := runMonitorServer()
	defer s.Shutdown()

	murl := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)

	b.ResetTimer()
	runtime.MemProfileRate = 1

	for i := 0; i < b.N; i++ {
		v := &Varz{}
		resp, err := http.Get(murl)
		if err != nil {
			b.Fatalf("Expected no error: Got %v\n", err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			b.Fatalf("Got an error reading the body: %v\n", err)
		}
		if err := json.Unmarshal(body, v); err != nil {
			b.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		resp.Body.Close()
	}
}

func TestVarzRaces(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	murl := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	done := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			for i := 0; i < 2; i++ {
				v := pollVarz(t, s, i, murl, nil)
				// Check the field that we are setting in main thread
				// to ensure that we have a copy and there is no
				// race with fields set in s.info and s.opts
				if v.ID == "abc" || v.MaxConn == -1 {
					// We will not get there. Need to have something
					// otherwise staticcheck will report empty branch
					return
				}

				select {
				case <-done:
					return
				default:
				}
			}
		}
	}()

	for i := 0; i < 1000; i++ {
		// Simulate a change in server's info and options
		// by changing something.
		s.mu.Lock()
		s.info.ID = fmt.Sprintf("serverid_%d", i)
		s.opts.MaxConn = 100 + i
		s.mu.Unlock()
		time.Sleep(time.Nanosecond)
	}
	close(done)
	wg.Wait()

	// Now check that there is no race doing parallel polling
	wg.Add(3)
	done = make(chan struct{})
	poll := func() {
		defer wg.Done()
		for {
			for mode := 0; mode < 2; mode++ {
				pollVarz(t, s, mode, murl, nil)
			}
			select {
			case <-done:
				return
			default:
			}
		}
	}
	for i := 0; i < 3; i++ {
		go poll()
	}
	time.Sleep(500 * time.Millisecond)
	close(done)
	wg.Wait()
}

func testMonitorStructPresent(t *testing.T, tag string) {
	t.Helper()

	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	s := RunServer(opts)
	defer s.Shutdown()

	varzURL := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	body := readBody(t, varzURL)
	if !bytes.Contains(body, []byte(`"`+tag+`": {}`)) {
		t.Fatalf("%s should be present and empty, got %s", tag, body)
	}
}

func TestMonitorCluster(t *testing.T) {
	testMonitorStructPresent(t, "cluster")

	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.Cluster.Name = "A"
	opts.Cluster.Port = -1
	opts.Cluster.AuthTimeout = 1
	opts.Routes = RoutesFromStr("nats://127.0.0.1:1234")
	s := RunServer(opts)
	defer s.Shutdown()

	expected := ClusterOptsVarz{
		"A",
		opts.Cluster.Host,
		opts.Cluster.Port,
		opts.Cluster.AuthTimeout,
		[]string{"127.0.0.1:1234"},
		opts.Cluster.TLSTimeout,
		opts.Cluster.TLSConfig != nil,
		opts.Cluster.TLSConfig != nil,
		DEFAULT_ROUTE_POOL_SIZE,
	}

	varzURL := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		check := func(t *testing.T, v *Varz) {
			t.Helper()
			if !reflect.DeepEqual(v.Cluster, expected) {
				t.Fatalf("mode=%v - expected %+v, got %+v", mode, expected, v.Cluster)
			}
		}
		v := pollVarz(t, s, mode, varzURL, nil)
		check(t, v)

		// Having this here to make sure that if fields are added in ClusterOptsVarz,
		// we make sure to update this test (compiler will report an error if we don't)
		_ = ClusterOptsVarz{"", "", 0, 0, nil, 2, false, false, 0}

		// Alter the fields to make sure that we have a proper deep copy
		// of what may be stored in the server. Anything we change here
		// should not affect the next returned value.
		v.Cluster.Name = "wrong"
		v.Cluster.Host = "wrong"
		v.Cluster.Port = 0
		v.Cluster.AuthTimeout = 0
		v.Cluster.URLs = []string{"wrong"}
		v = pollVarz(t, s, mode, varzURL, nil)
		check(t, v)
	}
}

func TestMonitorClusterURLs(t *testing.T) {
	resetPreviousHTTPConnections()

	o2 := DefaultOptions()
	o2.Cluster.Host = "127.0.0.1"
	o2.Cluster.Name = "A"

	s2 := RunServer(o2)
	defer s2.Shutdown()

	s2ClusterHostPort := fmt.Sprintf("127.0.0.1:%d", s2.ClusterAddr().Port)

	template := `
		port: -1
		http: -1
		cluster: {
			name: "A"
			port: -1
			routes [
				%s
				%s
			]
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, "nats://"+s2ClusterHostPort, "")))
	s1, _ := RunServerWithConfig(conf)
	defer s1.Shutdown()

	checkClusterFormed(t, s1, s2)

	// Check /varz cluster{} to see the URLs from s1 to s2
	varzURL := fmt.Sprintf("http://127.0.0.1:%d/varz", s1.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, s1, mode, varzURL, nil)
		if n := len(v.Cluster.URLs); n != 1 {
			t.Fatalf("mode=%v - Expected 1 URL, got %v", mode, n)
		}
		if v.Cluster.URLs[0] != s2ClusterHostPort {
			t.Fatalf("mode=%v - Expected url %q, got %q", mode, s2ClusterHostPort, v.Cluster.URLs[0])
		}
	}

	otherClusterHostPort := "127.0.0.1:1234"
	// Now update the config and add a route
	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(template, "nats://"+s2ClusterHostPort, "nats://"+otherClusterHostPort)))

	if err := s1.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	// Verify cluster still ok
	checkClusterFormed(t, s1, s2)

	// Now verify that s1 reports in /varz the new URL
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for mode := 0; mode < 2; mode++ {
			v := pollVarz(t, s1, mode, varzURL, nil)
			if n := len(v.Cluster.URLs); n != 2 {
				t.Fatalf("mode=%v - Expected 2 URL, got %v", mode, n)
			}
			gotS2 := false
			gotOther := false
			for _, u := range v.Cluster.URLs {
				if u == s2ClusterHostPort {
					gotS2 = true
				} else if u == otherClusterHostPort {
					gotOther = true
				} else {
					t.Fatalf("mode=%v - Incorrect url: %q", mode, u)
				}
			}
			if !gotS2 {
				t.Fatalf("mode=%v - Did not get cluster URL for s2", mode)
			}
			if !gotOther {
				t.Fatalf("mode=%v - Did not get the new cluster URL", mode)
			}
		}
		return nil
	})

	// Remove all routes from config
	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(template, "", "")))

	if err := s1.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	// Now verify that s1 reports no ULRs in /varz
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for mode := 0; mode < 2; mode++ {
			v := pollVarz(t, s1, mode, varzURL, nil)
			if n := len(v.Cluster.URLs); n != 0 {
				t.Fatalf("mode=%v - Expected 0 URL, got %v", mode, n)
			}
		}
		return nil
	})
}

func TestMonitorGateway(t *testing.T) {
	testMonitorStructPresent(t, "gateway")

	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.Gateway.Name = "A"
	opts.Gateway.Port = -1
	opts.Gateway.AuthTimeout = 1
	opts.Gateway.TLSTimeout = 1
	opts.Gateway.Advertise = "127.0.0.1"
	opts.Gateway.ConnectRetries = 1
	opts.Gateway.RejectUnknown = false
	u1, _ := url.Parse("nats://ivan:pwd@localhost:1234")
	u2, _ := url.Parse("nats://localhost:1235")
	opts.Gateway.Gateways = []*RemoteGatewayOpts{
		{
			Name:       "B",
			TLSTimeout: 1,
			URLs: []*url.URL{
				u1,
				u2,
			},
		},
	}
	s := RunServer(opts)
	defer s.Shutdown()

	expected := GatewayOptsVarz{
		"A",
		opts.Gateway.Host,
		opts.Gateway.Port,
		opts.Gateway.AuthTimeout,
		opts.Gateway.TLSTimeout,
		opts.Gateway.TLSConfig != nil,
		opts.Gateway.TLSConfig != nil,
		opts.Gateway.Advertise,
		opts.Gateway.ConnectRetries,
		[]RemoteGatewayOptsVarz{{"B", 1, nil}},
		opts.Gateway.RejectUnknown,
	}
	// Since URLs array is not guaranteed to be always the same order,
	// we don't add it in the expected GatewayOptsVarz, instead we
	// maintain here.
	expectedURLs := []string{"localhost:1234", "localhost:1235"}

	varzURL := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		check := func(t *testing.T, v *Varz) {
			t.Helper()
			var urls []string
			if len(v.Gateway.Gateways) == 1 {
				urls = v.Gateway.Gateways[0].URLs
				v.Gateway.Gateways[0].URLs = nil
			}
			if !reflect.DeepEqual(v.Gateway, expected) {
				t.Fatalf("mode=%v - expected %+v, got %+v", mode, expected, v.Gateway)
			}
			// Now compare urls
			for _, u := range expectedURLs {
				ok := false
				for _, u2 := range urls {
					if u == u2 {
						ok = true
						break
					}
				}
				if !ok {
					t.Fatalf("mode=%v - expected urls to be %v, got %v", mode, expected.Gateways[0].URLs, urls)
				}
			}
		}
		v := pollVarz(t, s, mode, varzURL, nil)
		check(t, v)

		// Having this here to make sure that if fields are added in GatewayOptsVarz,
		// we make sure to update this test (compiler will report an error if we don't)
		_ = GatewayOptsVarz{"", "", 0, 0, 0, false, false, "", 0, []RemoteGatewayOptsVarz{{"", 0, nil}}, false}

		// Alter the fields to make sure that we have a proper deep copy
		// of what may be stored in the server. Anything we change here
		// should not affect the next returned value.
		v.Gateway.Name = "wrong"
		v.Gateway.Host = "wrong"
		v.Gateway.Port = 0
		v.Gateway.AuthTimeout = 1234.5
		v.Gateway.TLSTimeout = 1234.5
		v.Gateway.Advertise = "wrong"
		v.Gateway.ConnectRetries = 1234
		v.Gateway.Gateways[0].Name = "wrong"
		v.Gateway.Gateways[0].TLSTimeout = 1234.5
		v.Gateway.Gateways[0].URLs = []string{"wrong"}
		v.Gateway.RejectUnknown = true
		v = pollVarz(t, s, mode, varzURL, nil)
		check(t, v)
	}
}

func TestMonitorGatewayURLsUpdated(t *testing.T) {
	resetPreviousHTTPConnections()

	ob1 := testDefaultOptionsForGateway("B")
	sb1 := runGatewayServer(ob1)
	defer sb1.Shutdown()

	// Start a1 that has a single URL to sb1.
	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	oa.HTTPHost = "127.0.0.1"
	oa.HTTPPort = MONITOR_PORT
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)

	varzURL := fmt.Sprintf("http://127.0.0.1:%d/varz", sa.MonitorAddr().Port)
	// Check the /varz gateway's URLs
	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, sa, mode, varzURL, nil)
		if n := len(v.Gateway.Gateways); n != 1 {
			t.Fatalf("mode=%v - Expected 1 remote gateway, got %v", mode, n)
		}
		gw := v.Gateway.Gateways[0]
		if n := len(gw.URLs); n != 1 {
			t.Fatalf("mode=%v - Expected 1 url, got %v", mode, n)
		}
		expected := oa.Gateway.Gateways[0].URLs[0].Host
		if u := gw.URLs[0]; u != expected {
			t.Fatalf("mode=%v - Expected URL %q, got %q", mode, expected, u)
		}
	}

	// Now start sb2 that clusters with sb1. sa should add to its list of URLs
	// sb2 gateway's connect URL.
	ob2 := testDefaultOptionsForGateway("B")
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sb1.ClusterAddr().Port))
	sb2 := runGatewayServer(ob2)
	defer sb2.Shutdown()

	// Wait for sb1 and sb2 to connect
	checkClusterFormed(t, sb1, sb2)
	// sb2 should be made aware of gateway A and connect to sa
	waitForInboundGateways(t, sa, 2, 2*time.Second)
	// Now check that URLs in /varz get updated
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for mode := 0; mode < 2; mode++ {
			v := pollVarz(t, sa, mode, varzURL, nil)
			if n := len(v.Gateway.Gateways); n != 1 {
				return fmt.Errorf("mode=%v - Expected 1 remote gateway, got %v", mode, n)
			}
			gw := v.Gateway.Gateways[0]
			if n := len(gw.URLs); n != 2 {
				return fmt.Errorf("mode=%v - Expected 2 urls, got %v", mode, n)
			}

			gotSB1 := false
			gotSB2 := false
			for _, u := range gw.URLs {
				if u == fmt.Sprintf("127.0.0.1:%d", sb1.GatewayAddr().Port) {
					gotSB1 = true
				} else if u == fmt.Sprintf("127.0.0.1:%d", sb2.GatewayAddr().Port) {
					gotSB2 = true
				} else {
					return fmt.Errorf("mode=%v - Incorrect URL to gateway B: %v", mode, u)
				}
			}
			if !gotSB1 {
				return fmt.Errorf("mode=%v - Did not get URL to sb1", mode)
			}
			if !gotSB2 {
				return fmt.Errorf("mode=%v - Did not get URL to sb2", mode)
			}
		}
		return nil
	})

	// Now stop sb2 and make sure that its removal is reflected in varz.
	sb2.Shutdown()
	// Wait for it to disappear from sa.
	waitForInboundGateways(t, sa, 1, 2*time.Second)
	// Now check that URLs in /varz get updated.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for mode := 0; mode < 2; mode++ {
			v := pollVarz(t, sa, mode, varzURL, nil)
			if n := len(v.Gateway.Gateways); n != 1 {
				return fmt.Errorf("mode=%v - Expected 1 remote gateway, got %v", mode, n)
			}
			gw := v.Gateway.Gateways[0]
			if n := len(gw.URLs); n != 1 {
				return fmt.Errorf("mode=%v - Expected 1 url, got %v", mode, n)
			}
			u := gw.URLs[0]
			if u != fmt.Sprintf("127.0.0.1:%d", sb1.GatewayAddr().Port) {
				return fmt.Errorf("mode=%v - Did not get URL to sb1", mode)
			}
		}
		return nil
	})
}

func TestMonitorGatewayReportItsOwnURLs(t *testing.T) {
	resetPreviousHTTPConnections()

	// In this test, we show that if a server has its own gateway information
	// as a remote (which is the case when remote gateway definitions is copied
	// on all clusters), we display the defined URLs.
	oa := testGatewayOptionsFromToWithURLs(t, "A", "A", []string{"nats://127.0.0.1:1234", "nats://127.0.0.1:1235"})
	oa.HTTPHost = "127.0.0.1"
	oa.HTTPPort = MONITOR_PORT
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	varzURL := fmt.Sprintf("http://127.0.0.1:%d/varz", sa.MonitorAddr().Port)
	// Check the /varz gateway's URLs
	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, sa, mode, varzURL, nil)
		if n := len(v.Gateway.Gateways); n != 1 {
			t.Fatalf("mode=%v - Expected 1 remote gateway, got %v", mode, n)
		}
		gw := v.Gateway.Gateways[0]
		if n := len(gw.URLs); n != 2 {
			t.Fatalf("mode=%v - Expected 2 urls, got %v", mode, gw.URLs)
		}
		expected := []string{"127.0.0.1:1234", "127.0.0.1:1235"}
		if !reflect.DeepEqual(gw.URLs, expected) {
			t.Fatalf("mode=%v - Expected URLs %q, got %q", mode, expected, gw.URLs)
		}
	}
}

func TestMonitorLeafNode(t *testing.T) {
	testMonitorStructPresent(t, "leaf")

	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.LeafNode.Port = -1
	opts.LeafNode.AuthTimeout = 1
	opts.LeafNode.TLSTimeout = 1
	opts.Accounts = []*Account{NewAccount("acc")}
	u, _ := url.Parse("nats://ivan:pwd@localhost:1234")
	opts.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			LocalAccount: "acc",
			URLs:         []*url.URL{u},
			TLSTimeout:   1,
		},
	}
	s := RunServer(opts)
	defer s.Shutdown()

	expected := LeafNodeOptsVarz{
		opts.LeafNode.Host,
		opts.LeafNode.Port,
		opts.LeafNode.AuthTimeout,
		opts.LeafNode.TLSTimeout,
		opts.LeafNode.TLSConfig != nil,
		opts.LeafNode.TLSConfig != nil,
		[]RemoteLeafOptsVarz{
			{
				"acc", 1, []string{"localhost:1234"}, nil, false,
			},
		},
		false,
	}

	varzURL := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		check := func(t *testing.T, v *Varz) {
			t.Helper()
			if !reflect.DeepEqual(v.LeafNode, expected) {
				t.Fatalf("mode=%v - expected %+v, got %+v", mode, expected, v.LeafNode)
			}
		}
		v := pollVarz(t, s, mode, varzURL, nil)
		check(t, v)

		// Having this here to make sure that if fields are added in ClusterOptsVarz,
		// we make sure to update this test (compiler will report an error if we don't)
		_ = LeafNodeOptsVarz{"", 0, 0, 0, false, false, []RemoteLeafOptsVarz{{"", 0, nil, nil, false}}, false}

		// Alter the fields to make sure that we have a proper deep copy
		// of what may be stored in the server. Anything we change here
		// should not affect the next returned value.
		v.LeafNode.Host = "wrong"
		v.LeafNode.Port = 0
		v.LeafNode.AuthTimeout = 1234.5
		v.LeafNode.TLSTimeout = 1234.5
		v.LeafNode.Remotes[0].LocalAccount = "wrong"
		v.LeafNode.Remotes[0].URLs = append(v.LeafNode.Remotes[0].URLs, "wrong")
		v.LeafNode.Remotes[0].TLSTimeout = 1234.5
		v = pollVarz(t, s, mode, varzURL, nil)
		check(t, v)
	}
}

func pollGatewayz(t *testing.T, s *Server, mode int, url string, opts *GatewayzOptions) *Gatewayz {
	t.Helper()
	if mode == 0 {
		g := &Gatewayz{}
		body := readBody(t, url)
		if err := json.Unmarshal(body, g); err != nil {
			t.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		return g
	}
	g, err := s.Gatewayz(opts)
	if err != nil {
		t.Fatalf("Error on Gatewayz: %v", err)
	}
	return g
}

func TestMonitorGatewayz(t *testing.T) {
	resetPreviousHTTPConnections()

	// First check that without gateway configured
	s := runMonitorServer()
	defer s.Shutdown()
	url := fmt.Sprintf("http://127.0.0.1:%d/gatewayz", s.MonitorAddr().Port)
	for pollMode := 0; pollMode < 2; pollMode++ {
		g := pollGatewayz(t, s, pollMode, url, nil)
		// Expect Name and port to be empty
		if g.Name != _EMPTY_ || g.Port != 0 {
			t.Fatalf("Expected no gateway, got %+v", g)
		}
	}
	s.Shutdown()

	ob1 := testDefaultOptionsForGateway("B")
	sb1 := runGatewayServer(ob1)
	defer sb1.Shutdown()

	// Start a1 that has a single URL to sb1.
	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	oa.HTTPHost = "127.0.0.1"
	oa.HTTPPort = MONITOR_PORT
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)

	waitForOutboundGateways(t, sb1, 1, 2*time.Second)
	waitForInboundGateways(t, sb1, 1, 2*time.Second)

	gatewayzURL := fmt.Sprintf("http://127.0.0.1:%d/gatewayz", sa.MonitorAddr().Port)
	for pollMode := 0; pollMode < 2; pollMode++ {
		g := pollGatewayz(t, sa, pollMode, gatewayzURL, nil)
		if g.Host != oa.Gateway.Host {
			t.Fatalf("mode=%v - Expected host to be %q, got %q", pollMode, oa.Gateway.Host, g.Host)
		}
		if g.Port != oa.Gateway.Port {
			t.Fatalf("mode=%v - Expected port to be %v, got %v", pollMode, oa.Gateway.Port, g.Port)
		}
		if n := len(g.OutboundGateways); n != 1 {
			t.Fatalf("mode=%v - Expected outbound to 1 gateway, got %v", pollMode, n)
		}
		if n := len(g.InboundGateways); n != 1 {
			t.Fatalf("mode=%v - Expected inbound from 1 gateway, got %v", pollMode, n)
		}
		og := g.OutboundGateways["B"]
		if og == nil {
			t.Fatalf("mode=%v - Expected to find outbound connection to B, got none", pollMode)
		}
		if !og.IsConfigured {
			t.Fatalf("mode=%v - Expected gw connection to be configured, was not", pollMode)
		}
		if og.Connection == nil {
			t.Fatalf("mode=%v - Expected outbound connection to B to be set, wat not", pollMode)
		}
		if og.Connection.Name != sb1.ID() {
			t.Fatalf("mode=%v - Expected outbound connection to B to have name %q, got %q", pollMode, sb1.ID(), og.Connection.Name)
		}
		if n := len(og.Accounts); n != 0 {
			t.Fatalf("mode=%v - Expected no account, got %v", pollMode, n)
		}
		ig := g.InboundGateways["B"]
		if ig == nil {
			t.Fatalf("mode=%v - Expected to find inbound connection from B, got none", pollMode)
		}
		if n := len(ig); n != 1 {
			t.Fatalf("mode=%v - Expected 1 inbound connection, got %v", pollMode, n)
		}
		igc := ig[0]
		if igc.Connection == nil {
			t.Fatalf("mode=%v - Expected inbound connection to B to be set, wat not", pollMode)
		}
		if igc.Connection.Name != sb1.ID() {
			t.Fatalf("mode=%v - Expected inbound connection to B to have name %q, got %q", pollMode, sb1.ID(), igc.Connection.Name)
		}
	}

	// Now start sb2 that clusters with sb1. sa should add to its list of URLs
	// sb2 gateway's connect URL.
	ob2 := testDefaultOptionsForGateway("B")
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sb1.ClusterAddr().Port))
	sb2 := runGatewayServer(ob2)
	defer sb2.Shutdown()

	// Wait for sb1 and sb2 to connect
	checkClusterFormed(t, sb1, sb2)
	// sb2 should be made aware of gateway A and connect to sa
	waitForInboundGateways(t, sa, 2, 2*time.Second)
	// Now check that URLs in /varz get updated
	checkGatewayB := func(t *testing.T, url string, opts *GatewayzOptions) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			for pollMode := 0; pollMode < 2; pollMode++ {
				g := pollGatewayz(t, sa, pollMode, url, opts)
				if n := len(g.OutboundGateways); n != 1 {
					t.Fatalf("mode=%v - Expected outbound to 1 gateway, got %v", pollMode, n)
				}
				// The InboundGateways is a map with key the gateway names,
				// then value is array of connections. So should be 1 here.
				if n := len(g.InboundGateways); n != 1 {
					t.Fatalf("mode=%v - Expected inbound from 1 gateway, got %v", pollMode, n)
				}
				ig := g.InboundGateways["B"]
				if ig == nil {
					t.Fatalf("mode=%v - Expected to find inbound connection from B, got none", pollMode)
				}
				if n := len(ig); n != 2 {
					t.Fatalf("mode=%v - Expected 2 inbound connections from gateway B, got %v", pollMode, n)
				}
				gotSB1 := false
				gotSB2 := false
				for _, rg := range ig {
					if rg.Connection != nil {
						if rg.Connection.Name == sb1.ID() {
							gotSB1 = true
						} else if rg.Connection.Name == sb2.ID() {
							gotSB2 = true
						}
					}
				}
				if !gotSB1 {
					t.Fatalf("mode=%v - Missing inbound connection from sb1", pollMode)
				}
				if !gotSB2 {
					t.Fatalf("mode=%v - Missing inbound connection from sb2", pollMode)
				}
			}
			return nil
		})
	}
	checkGatewayB(t, gatewayzURL, nil)

	// Start a new cluser C that connects to B. A should see it as
	// a non-configured gateway.
	oc := testGatewayOptionsFromToWithServers(t, "C", "B", sb1)
	sc := runGatewayServer(oc)
	defer sc.Shutdown()

	// All servers should have 2 outbound connections (one for each other cluster)
	waitForOutboundGateways(t, sa, 2, 2*time.Second)
	waitForOutboundGateways(t, sb1, 2, 2*time.Second)
	waitForOutboundGateways(t, sb2, 2, 2*time.Second)
	waitForOutboundGateways(t, sc, 2, 2*time.Second)

	// Server sa should have 3 inbounds now
	waitForInboundGateways(t, sa, 3, 2*time.Second)

	// Check gatewayz again to see that we have C now.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for pollMode := 0; pollMode < 2; pollMode++ {
			g := pollGatewayz(t, sa, pollMode, gatewayzURL, nil)
			if n := len(g.OutboundGateways); n != 2 {
				t.Fatalf("mode=%v - Expected outbound to 2 gateways, got %v", pollMode, n)
			}
			// The InboundGateways is a map with key the gateway names,
			// then value is array of connections. So should be 2 here.
			if n := len(g.InboundGateways); n != 2 {
				t.Fatalf("mode=%v - Expected inbound from 2 gateways, got %v", pollMode, n)
			}
			og := g.OutboundGateways["C"]
			if og == nil {
				t.Fatalf("mode=%v - Expected to find outbound connection to C, got none", pollMode)
			}
			if og.IsConfigured {
				t.Fatalf("mode=%v - Expected IsConfigured for gateway C to be false, was true", pollMode)
			}
			if og.Connection == nil {
				t.Fatalf("mode=%v - Expected connection to C, got none", pollMode)
			}
			if og.Connection.Name != sc.ID() {
				t.Fatalf("mode=%v - Expected outbound connection to C to have name %q, got %q", pollMode, sc.ID(), og.Connection.Name)
			}
			ig := g.InboundGateways["C"]
			if ig == nil {
				t.Fatalf("mode=%v - Expected to find inbound connection from C, got none", pollMode)
			}
			if n := len(ig); n != 1 {
				t.Fatalf("mode=%v - Expected 1 inbound connections from gateway C, got %v", pollMode, n)
			}
			igc := ig[0]
			if igc.Connection == nil {
				t.Fatalf("mode=%v - Expected connection to C, got none", pollMode)
			}
			if igc.Connection.Name != sc.ID() {
				t.Fatalf("mode=%v - Expected outbound connection to C to have name %q, got %q", pollMode, sc.ID(), og.Connection.Name)
			}
		}
		return nil
	})

	// Select only 1 gateway by passing the name to option/url
	opts := &GatewayzOptions{Name: "B"}
	checkGatewayB(t, gatewayzURL+"?gw_name=B", opts)

	// Stop gateway C and check that we have only B, with and without filter.
	sc.Shutdown()
	checkGatewayB(t, gatewayzURL+"?gw_name=B", opts)
	checkGatewayB(t, gatewayzURL, nil)
}

func TestMonitorGatewayzAccounts(t *testing.T) {
	GatewayDoNotForceInterestOnlyMode(true)
	defer GatewayDoNotForceInterestOnlyMode(false)

	resetPreviousHTTPConnections()

	// Create bunch of Accounts
	totalAccounts := 15
	accounts := ""
	for i := 0; i < totalAccounts; i++ {
		acc := fmt.Sprintf("	acc_%d: { users=[{user:user_%d, password: pwd}] }\n", i, i)
		accounts += acc
	}

	bConf := createConfFile(t, []byte(fmt.Sprintf(`
		accounts {
			%s
		}
		port: -1
		http: -1
		gateway: {
			name: "B"
			port: -1
		}
		no_sys_acc = true
	`, accounts)))

	sb, ob := RunServerWithConfig(bConf)
	defer sb.Shutdown()
	sb.SetLogger(&DummyLogger{}, true, true)

	// Start a1 that has a single URL to sb1.
	aConf := createConfFile(t, []byte(fmt.Sprintf(`
		accounts {
			%s
		}
		port: -1
		http: -1
		gateway: {
			name: "A"
			port: -1
			gateways [
				{
					name: "B"
					url: "nats://127.0.0.1:%d"
				}
			]
		}
		no_sys_acc = true
	`, accounts, sb.GatewayAddr().Port)))

	sa, oa := RunServerWithConfig(aConf)
	defer sa.Shutdown()
	sa.SetLogger(&DummyLogger{}, true, true)

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)

	// Create clients for each account on A and publish a message
	// so that list of accounts appear in gatewayz
	produceMsgsFromA := func(t *testing.T) {
		t.Helper()
		for i := 0; i < totalAccounts; i++ {
			nc, err := nats.Connect(fmt.Sprintf("nats://user_%d:pwd@%s:%d", i, oa.Host, oa.Port))
			if err != nil {
				t.Fatalf("Error on connect: %v", err)
			}
			nc.Publish("foo", []byte("hello"))
			nc.Flush()
			nc.Close()
		}
	}
	produceMsgsFromA(t)

	// Wait for A- for all accounts
	gwc := sa.getOutboundGatewayConnection("B")
	for i := 0; i < totalAccounts; i++ {
		checkForAccountNoInterest(t, gwc, fmt.Sprintf("acc_%d", i), true, 2*time.Second)
	}

	// Check accounts...
	gatewayzURL := fmt.Sprintf("http://127.0.0.1:%d/gatewayz", sa.MonitorAddr().Port)
	for pollMode := 0; pollMode < 2; pollMode++ {
		// First, without asking for it, they should not be present.
		g := pollGatewayz(t, sa, pollMode, gatewayzURL, nil)
		og := g.OutboundGateways["B"]
		if og == nil {
			t.Fatalf("mode=%v - Expected outbound gateway to B, got none", pollMode)
		}
		if n := len(og.Accounts); n != 0 {
			t.Fatalf("mode=%v - Expected accounts list to not be present by default, got %v", pollMode, n)
		}
		// Now ask for the accounts
		g = pollGatewayz(t, sa, pollMode, gatewayzURL+"?accs=1", &GatewayzOptions{Accounts: true})
		og = g.OutboundGateways["B"]
		if og == nil {
			t.Fatalf("mode=%v - Expected outbound gateway to B, got none", pollMode)
		}
		if n := len(og.Accounts); n != totalAccounts {
			t.Fatalf("mode=%v - Expected to get all %d accounts, got %v", pollMode, totalAccounts, n)
		}
		// Now account details
		for _, acc := range og.Accounts {
			if acc.InterestMode != Optimistic.String() {
				t.Fatalf("mode=%v - Expected optimistic mode, got %q", pollMode, acc.InterestMode)
			}
			// Since there is no interest at all on B, the publish
			// will have resulted in total account no interest, so
			// the number of no interest (subject wise) should be 0
			if acc.NoInterestCount != 0 {
				t.Fatalf("mode=%v - Expected 0 no-interest, got %v", pollMode, acc.NoInterestCount)
			}
			if acc.NumQueueSubscriptions != 0 || acc.TotalSubscriptions != 0 {
				t.Fatalf("mode=%v - Expected total subs to be 0, got %v - and num queue subs to be 0, got %v",
					pollMode, acc.TotalSubscriptions, acc.NumQueueSubscriptions)
			}
		}
	}

	// Check inbound on B
	gwURLServerB := fmt.Sprintf("http://127.0.0.1:%d/gatewayz", sb.MonitorAddr().Port)
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for pollMode := 0; pollMode < 2; pollMode++ {
			// First, without asking for it, they should not be present.
			g := pollGatewayz(t, sb, pollMode, gwURLServerB, nil)
			igs := g.InboundGateways["A"]
			if igs == nil {
				return fmt.Errorf("mode=%v - Expected inbound gateway to A, got none", pollMode)
			}
			if len(igs) != 1 {
				return fmt.Errorf("mode=%v - Expected single inbound, got %v", pollMode, len(igs))
			}
			ig := igs[0]
			if n := len(ig.Accounts); n != 0 {
				return fmt.Errorf("mode=%v - Expected no account, got %v", pollMode, n)
			}
			// Check that list of accounts
			g = pollGatewayz(t, sb, pollMode, gwURLServerB+"?accs=1", &GatewayzOptions{Accounts: true})
			igs = g.InboundGateways["A"]
			if igs == nil {
				return fmt.Errorf("mode=%v - Expected inbound gateway to A, got none", pollMode)
			}
			if len(igs) != 1 {
				return fmt.Errorf("mode=%v - Expected single inbound, got %v", pollMode, len(igs))
			}
			ig = igs[0]
			if ig.Connection == nil {
				return fmt.Errorf("mode=%v - Expected inbound connection from A to be set, wat not", pollMode)
			}
			if ig.Connection.Name != sa.ID() {
				t.Fatalf("mode=%v - Expected inbound connection from A to have name %q, got %q", pollMode, sa.ID(), ig.Connection.Name)
			}
			if n := len(ig.Accounts); n != totalAccounts {
				return fmt.Errorf("mode=%v - Expected to get all %d accounts, got %v", pollMode, totalAccounts, n)
			}
			// Now account details
			for _, acc := range ig.Accounts {
				if acc.InterestMode != Optimistic.String() {
					return fmt.Errorf("mode=%v - Expected optimistic mode, got %q", pollMode, acc.InterestMode)
				}
				// Since there is no interest at all on B, the publish
				// will have resulted in total account no interest, so
				// the number of no interest (subject wise) should be 0
				if acc.NoInterestCount != 0 {
					t.Fatalf("mode=%v - Expected 0 no-interest, got %v", pollMode, acc.NoInterestCount)
				}
				// For inbound gateway, NumQueueSubscriptions and TotalSubscriptions
				// are not relevant.
				if acc.NumQueueSubscriptions != 0 || acc.TotalSubscriptions != 0 {
					return fmt.Errorf("mode=%v - For inbound connection, expected num queue subs and total subs to be 0, got %v and %v",
						pollMode, acc.TotalSubscriptions, acc.NumQueueSubscriptions)
				}
			}
		}
		return nil
	})

	// Now create subscriptions on B to prevent A- and check on subject no interest
	for i := 0; i < totalAccounts; i++ {
		nc, err := nats.Connect(fmt.Sprintf("nats://user_%d:pwd@%s:%d", i, ob.Host, ob.Port))
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()
		// Create a queue sub so it shows up in gatewayz
		nc.QueueSubscribeSync("bar", "queue")
		// Create plain subscriptions on baz.0, baz.1 and baz.2.
		// Create to for each subject. Since gateways will send
		// only once per subject, the number of subs should be 3, not 6.
		for j := 0; j < 3; j++ {
			subj := fmt.Sprintf("baz.%d", j)
			nc.SubscribeSync(subj)
			nc.SubscribeSync(subj)
		}
		nc.Flush()
	}

	for i := 0; i < totalAccounts; i++ {
		accName := fmt.Sprintf("acc_%d", i)
		checkForRegisteredQSubInterest(t, sa, "B", accName, "bar", 1, 2*time.Second)
	}

	// Resend msgs from A on foo, on all accounts. There will be no interest on this subject.
	produceMsgsFromA(t)

	for i := 0; i < totalAccounts; i++ {
		accName := fmt.Sprintf("acc_%d", i)
		checkForSubjectNoInterest(t, gwc, accName, "foo", true, 2*time.Second)
		// Verify that we still have the queue interest registered
		checkForRegisteredQSubInterest(t, sa, "B", accName, "bar", 1, 2*time.Second)
	}

	// Check accounts...
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for pollMode := 0; pollMode < 2; pollMode++ {
			g := pollGatewayz(t, sa, pollMode, gatewayzURL+"?accs=1", &GatewayzOptions{Accounts: true})
			og := g.OutboundGateways["B"]
			if og == nil {
				return fmt.Errorf("mode=%v - Expected outbound gateway to B, got none", pollMode)
			}
			if n := len(og.Accounts); n != totalAccounts {
				return fmt.Errorf("mode=%v - Expected to get all %d accounts, got %v", pollMode, totalAccounts, n)
			}
			// Now account details
			for _, acc := range og.Accounts {
				if acc.InterestMode != Optimistic.String() {
					return fmt.Errorf("mode=%v - Expected optimistic mode, got %q", pollMode, acc.InterestMode)
				}
				if acc.NoInterestCount != 1 {
					return fmt.Errorf("mode=%v - Expected 1 no-interest, got %v", pollMode, acc.NoInterestCount)
				}
				if acc.NumQueueSubscriptions != 1 || acc.TotalSubscriptions != 1 {
					return fmt.Errorf("mode=%v - Expected total subs to be 1, got %v - and num queue subs to be 1, got %v",
						pollMode, acc.TotalSubscriptions, acc.NumQueueSubscriptions)
				}
			}
		}
		return nil
	})

	// Check inbound on server B
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for pollMode := 0; pollMode < 2; pollMode++ {
			// Ask for accounts list
			g := pollGatewayz(t, sb, pollMode, gwURLServerB+"?accs=1", &GatewayzOptions{Accounts: true})
			igs := g.InboundGateways["A"]
			if igs == nil {
				return fmt.Errorf("mode=%v - Expected inbound gateway to A, got none", pollMode)
			}
			if len(igs) != 1 {
				return fmt.Errorf("mode=%v - Expected single inbound, got %v", pollMode, len(igs))
			}
			ig := igs[0]
			if ig.Connection == nil {
				return fmt.Errorf("mode=%v - Expected inbound connection from A to be set, wat not", pollMode)
			}
			if ig.Connection.Name != sa.ID() {
				t.Fatalf("mode=%v - Expected inbound connection from A to have name %q, got %q", pollMode, sa.ID(), ig.Connection.Name)
			}
			if n := len(ig.Accounts); n != totalAccounts {
				return fmt.Errorf("mode=%v - Expected to get all %d accounts, got %v", pollMode, totalAccounts, n)
			}
			// Now account details
			for _, acc := range ig.Accounts {
				if acc.InterestMode != Optimistic.String() {
					return fmt.Errorf("mode=%v - Expected optimistic mode, got %q", pollMode, acc.InterestMode)
				}
				if acc.NoInterestCount != 1 {
					return fmt.Errorf("mode=%v - Expected 1 no-interest, got %v", pollMode, acc.NoInterestCount)
				}
				// For inbound gateway, NumQueueSubscriptions and TotalSubscriptions
				// are not relevant.
				if acc.NumQueueSubscriptions != 0 || acc.TotalSubscriptions != 0 {
					return fmt.Errorf("mode=%v - For inbound connection, expected num queue subs and total subs to be 0, got %v and %v",
						pollMode, acc.TotalSubscriptions, acc.NumQueueSubscriptions)
				}
			}
		}
		return nil
	})

	// Make one of the account to switch to interest only
	nc, err := nats.Connect(fmt.Sprintf("nats://user_1:pwd@%s:%d", oa.Host, oa.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	for i := 0; i < 1100; i++ {
		nc.Publish(fmt.Sprintf("foo.%d", i), []byte("hello"))
	}
	nc.Flush()
	nc.Close()

	// Check that we can select single account
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for pollMode := 0; pollMode < 2; pollMode++ {
			g := pollGatewayz(t, sa, pollMode, gatewayzURL+"?gw_name=B&acc_name=acc_1", &GatewayzOptions{Name: "B", AccountName: "acc_1"})
			og := g.OutboundGateways["B"]
			if og == nil {
				return fmt.Errorf("mode=%v - Expected outbound gateway to B, got none", pollMode)
			}
			if n := len(og.Accounts); n != 1 {
				return fmt.Errorf("mode=%v - Expected to get 1 account, got %v", pollMode, n)
			}
			// Now account details
			acc := og.Accounts[0]
			if acc.InterestMode != InterestOnly.String() {
				return fmt.Errorf("mode=%v - Expected interest-only mode, got %q", pollMode, acc.InterestMode)
			}
			// Since we switched, this should be set to 0
			if acc.NoInterestCount != 0 {
				return fmt.Errorf("mode=%v - Expected 0 no-interest, got %v", pollMode, acc.NoInterestCount)
			}
			// We have created 3 subs on that account on B, and 1 queue sub.
			// So total should be 4 and 1 for queue sub.
			if acc.NumQueueSubscriptions != 1 {
				return fmt.Errorf("mode=%v - Expected num queue subs to be 1, got %v",
					pollMode, acc.NumQueueSubscriptions)
			}
			if acc.TotalSubscriptions != 4 {
				return fmt.Errorf("mode=%v - Expected total subs to be 4, got %v",
					pollMode, acc.TotalSubscriptions)
			}
		}
		return nil
	})

	// Check inbound on B now...
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for pollMode := 0; pollMode < 2; pollMode++ {
			g := pollGatewayz(t, sb, pollMode, gwURLServerB+"?gw_name=A&acc_name=acc_1", &GatewayzOptions{Name: "A", AccountName: "acc_1"})
			igs := g.InboundGateways["A"]
			if igs == nil {
				return fmt.Errorf("mode=%v - Expected inbound gateway from A, got none", pollMode)
			}
			if len(igs) != 1 {
				return fmt.Errorf("mode=%v - Expected single inbound, got %v", pollMode, len(igs))
			}
			ig := igs[0]
			if n := len(ig.Accounts); n != 1 {
				return fmt.Errorf("mode=%v - Expected to get 1 account, got %v", pollMode, n)
			}
			// Now account details
			acc := ig.Accounts[0]
			if acc.InterestMode != InterestOnly.String() {
				return fmt.Errorf("mode=%v - Expected interest-only mode, got %q", pollMode, acc.InterestMode)
			}
			if acc.InterestMode != InterestOnly.String() {
				return fmt.Errorf("Should be in %q mode, got %q", InterestOnly.String(), acc.InterestMode)
			}
			// Since we switched, this should be set to 0
			if acc.NoInterestCount != 0 {
				return fmt.Errorf("mode=%v - Expected 0 no-interest, got %v", pollMode, acc.NoInterestCount)
			}
			// Again, for inbound, these should be always 0.
			if acc.NumQueueSubscriptions != 0 || acc.TotalSubscriptions != 0 {
				return fmt.Errorf("mode=%v - For inbound connection, expected num queue subs and total subs to be 0, got %v and %v",
					pollMode, acc.TotalSubscriptions, acc.NumQueueSubscriptions)
			}
		}
		return nil
	})
}

func TestMonitorRouteRTT(t *testing.T) {
	// Do not change default PingInterval and expect RTT to still be reported

	ob := DefaultOptions()
	sb := RunServer(ob)
	defer sb.Shutdown()

	oa := DefaultOptions()
	oa.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", ob.Cluster.Host, ob.Cluster.Port))
	sa := RunServer(oa)
	defer sa.Shutdown()

	checkClusterFormed(t, sa, sb)

	checkRouteInfo := func(t *testing.T, s *Server) {
		t.Helper()
		routezURL := fmt.Sprintf("http://127.0.0.1:%d/routez", s.MonitorAddr().Port)
		for pollMode := 0; pollMode < 2; pollMode++ {
			checkFor(t, 2*firstPingInterval, 15*time.Millisecond, func() error {
				rz := pollRoutez(t, s, pollMode, routezURL, nil)
				// Pool size + 1 for system account
				if len(rz.Routes) != DEFAULT_ROUTE_POOL_SIZE+1 {
					return fmt.Errorf("Expected %d route, got %v", DEFAULT_ROUTE_POOL_SIZE+1, len(rz.Routes))
				}
				for _, ri := range rz.Routes {
					if ri.RTT == _EMPTY_ {
						return fmt.Errorf("Route's RTT not reported")
					}
				}
				return nil
			})
		}
	}
	checkRouteInfo(t, sa)
	checkRouteInfo(t, sb)
}

func pollLeafz(t *testing.T, s *Server, mode int, url string, opts *LeafzOptions) *Leafz {
	t.Helper()
	if mode == 0 {
		l := &Leafz{}
		body := readBody(t, url)
		if err := json.Unmarshal(body, l); err != nil {
			t.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		return l
	}
	l, err := s.Leafz(opts)
	if err != nil {
		t.Fatalf("Error on Leafz: %v", err)
	}
	return l
}

func TestMonitorOpJWT(t *testing.T) {
	content := `
	listen: "127.0.0.1:-1"
	http: "127.0.0.1:-1"
	operator = "../test/configs/nkeys/op.jwt"
	resolver = MEMORY
	`
	conf := createConfFile(t, []byte(content))
	sa, _ := RunServerWithConfig(conf)
	defer sa.Shutdown()

	theJWT, err := os.ReadFile("../test/configs/nkeys/op.jwt")
	require_NoError(t, err)
	theJWT = []byte(strings.Split(string(theJWT), "\n")[1])
	claim, err := jwt.DecodeOperatorClaims(string(theJWT))
	require_NoError(t, err)

	pollURL := fmt.Sprintf("http://127.0.0.1:%d/varz", sa.MonitorAddr().Port)
	for pollMode := 1; pollMode < 2; pollMode++ {
		l := pollVarz(t, sa, pollMode, pollURL, nil)

		if len(l.TrustedOperatorsJwt) != 1 {
			t.Fatalf("Expected one operator jwt")
		}
		if len(l.TrustedOperatorsClaim) != 1 {
			t.Fatalf("Expected one operator claim")
		}
		if l.TrustedOperatorsJwt[0] != string(theJWT) {
			t.Fatalf("Expected operator to be identical to configuration")
		}
		if !reflect.DeepEqual(l.TrustedOperatorsClaim[0], claim) {
			t.Fatal("claims need to be equal")
		}
	}
}

func TestMonitorLeafz(t *testing.T) {
	content := `
	server_name: "hub"
	listen: "127.0.0.1:-1"
	http: "127.0.0.1:-1"
	operator = "../test/configs/nkeys/op.jwt"
	resolver = MEMORY
	ping_interval = 1
	leafnodes {
		listen: "127.0.0.1:-1"
	}
	`
	conf := createConfFile(t, []byte(content))
	sb, ob := RunServerWithConfig(conf)
	defer sb.Shutdown()

	createAcc := func(t *testing.T) (*Account, string) {
		t.Helper()
		acc, akp := createAccount(sb)
		kp, _ := nkeys.CreateUser()
		pub, _ := kp.PublicKey()
		nuc := jwt.NewUserClaims(pub)
		ujwt, err := nuc.Encode(akp)
		if err != nil {
			t.Fatalf("Error generating user JWT: %v", err)
		}
		seed, _ := kp.Seed()
		creds := genCredsFile(t, ujwt, seed)
		return acc, creds
	}
	acc1, mycreds1 := createAcc(t)
	acc2, mycreds2 := createAcc(t)
	leafName := "my-leaf-node"

	content = `
		port: -1
		http: "127.0.0.1:-1"
		ping_interval = 1
		server_name: %s
		accounts {
			%s {
				users [
					{user: user1, password: pwd}
				]
			}
			%s {
				users [
					{user: user2, password: pwd}
				]
			}
		}
		leafnodes {
			remotes = [
				{
					account: "%s"
					url: nats-leaf://127.0.0.1:%d
					credentials: '%s'
				}
				{
					account: "%s"
					url: nats-leaf://127.0.0.1:%d
					credentials: '%s'
				}
			]
		}
		`
	config := fmt.Sprintf(content,
		leafName,
		acc1.Name, acc2.Name,
		acc1.Name, ob.LeafNode.Port, mycreds1,
		acc2.Name, ob.LeafNode.Port, mycreds2)
	conf = createConfFile(t, []byte(config))
	sa, oa := RunServerWithConfig(conf)
	defer sa.Shutdown()

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if n := sa.NumLeafNodes(); n != 2 {
			return fmt.Errorf("Expected 2 leaf connections, got %v", n)
		}
		return nil
	})

	// Wait for initial RTT to be computed
	time.Sleep(firstPingInterval + 500*time.Millisecond)

	ch := make(chan bool, 1)
	nc1B := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", ob.Port), nats.UserCredentials(mycreds1))
	defer nc1B.Close()
	natsSub(t, nc1B, "foo", func(_ *nats.Msg) { ch <- true })
	natsSub(t, nc1B, "bar", func(_ *nats.Msg) {})
	natsFlush(t, nc1B)

	nc2B := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", ob.Port), nats.UserCredentials(mycreds2))
	defer nc2B.Close()
	natsSub(t, nc2B, "bar", func(_ *nats.Msg) { ch <- true })
	natsSub(t, nc2B, "foo", func(_ *nats.Msg) {})
	natsFlush(t, nc2B)

	nc1A := natsConnect(t, fmt.Sprintf("nats://user1:pwd@127.0.0.1:%d", oa.Port))
	defer nc1A.Close()
	natsPub(t, nc1A, "foo", []byte("hello"))
	natsFlush(t, nc1A)

	waitCh(t, ch, "Did not get the message")

	nc2A := natsConnect(t, fmt.Sprintf("nats://user2:pwd@127.0.0.1:%d", oa.Port))
	defer nc2A.Close()
	natsPub(t, nc2A, "bar", []byte("hello"))
	natsPub(t, nc2A, "bar", []byte("hello"))
	natsFlush(t, nc2A)

	waitCh(t, ch, "Did not get the message")
	waitCh(t, ch, "Did not get the message")

	// Let's poll server A
	pollURL := fmt.Sprintf("http://127.0.0.1:%d/leafz?subs=1", sa.MonitorAddr().Port)
	for pollMode := 1; pollMode < 2; pollMode++ {
		l := pollLeafz(t, sa, pollMode, pollURL, &LeafzOptions{Subscriptions: true})
		if l.ID != sa.ID() {
			t.Fatalf("Expected ID to be %q, got %q", sa.ID(), l.ID)
		}
		if l.Now.IsZero() {
			t.Fatalf("Expected Now to be set, was not")
		}
		if l.NumLeafs != 2 {
			t.Fatalf("Expected NumLeafs to be 2, got %v", l.NumLeafs)
		}
		if len(l.Leafs) != 2 {
			t.Fatalf("Expected array to be len 2, got %v", len(l.Leafs))
		}
		for _, ln := range l.Leafs {
			if ln.Account == acc1.Name {
				if ln.OutMsgs != 1 || ln.OutBytes == 0 || ln.InMsgs != 0 || ln.InBytes != 0 {
					t.Fatalf("Expected 1 OutMsgs/Bytes and 0 InMsgs/Bytes, got %+v", ln)
				}
			} else if ln.Account == acc2.Name {
				if ln.OutMsgs != 2 || ln.OutBytes == 0 || ln.InMsgs != 0 || ln.InBytes != 0 {
					t.Fatalf("Expected 2 OutMsgs/Bytes and 0 InMsgs/Bytes, got %+v", ln)
				}
			} else {
				t.Fatalf("Expected account to be %q or %q, got %q", acc1.Name, acc2.Name, ln.Account)
			}
			if ln.Name != "hub" {
				t.Fatalf("Expected name to be %q, got %q", "hub", ln.Name)
			}
			if !ln.IsSpoke {
				t.Fatal("Expected leafnode connection to be spoke")
			}
			if ln.RTT == "" {
				t.Fatalf("RTT not tracked?")
			}
			if ln.NumSubs != 3 {
				t.Fatalf("Expected 3 subs, got %v", ln.NumSubs)
			}
			if len(ln.Subs) != 3 {
				t.Fatalf("Expected subs to be returned, got %v", len(ln.Subs))
			}
			var foundFoo bool
			var foundBar bool
			for _, sub := range ln.Subs {
				if sub == "foo" {
					foundFoo = true
				} else if sub == "bar" {
					foundBar = true
				}
			}
			if !foundFoo {
				t.Fatal("Did not find subject foo")
			}
			if !foundBar {
				t.Fatal("Did not find subject bar")
			}
		}
	}
	// Make sure that if we don't ask for subs, we don't get them
	pollURL = fmt.Sprintf("http://127.0.0.1:%d/leafz", sa.MonitorAddr().Port)
	for pollMode := 1; pollMode < 2; pollMode++ {
		l := pollLeafz(t, sa, pollMode, pollURL, nil)
		for _, ln := range l.Leafs {
			if ln.NumSubs != 3 {
				t.Fatalf("Number of subs should be 3, got %v", ln.NumSubs)
			}
			if len(ln.Subs) != 0 {
				t.Fatalf("Subs should not have been returned, got %v", ln.Subs)
			}
		}
	}
	// Make sure that we can request per account - existing account
	pollURL = fmt.Sprintf("http://127.0.0.1:%d/leafz?acc=%s", sa.MonitorAddr().Port, acc1.Name)
	for pollMode := 1; pollMode < 2; pollMode++ {
		l := pollLeafz(t, sa, pollMode, pollURL, &LeafzOptions{Account: acc1.Name})
		for _, ln := range l.Leafs {
			if ln.Account != acc1.Name {
				t.Fatalf("Expected leaf node to be from account %s, got: %v", acc1.Name, ln)
			}
		}
		if len(l.Leafs) != 1 {
			t.Fatalf("Expected only two leaf node for this account, got: %v", len(l.Leafs))
		}
	}
	// Make sure that we can request per account - non existing account
	pollURL = fmt.Sprintf("http://127.0.0.1:%d/leafz?acc=%s", sa.MonitorAddr().Port, "DOESNOTEXIST")
	for pollMode := 1; pollMode < 2; pollMode++ {
		l := pollLeafz(t, sa, pollMode, pollURL, &LeafzOptions{Account: "DOESNOTEXIST"})
		if len(l.Leafs) != 0 {
			t.Fatalf("Expected no leaf node for this account, got: %v", len(l.Leafs))
		}
	}
	// Now polling server B.
	pollURL = fmt.Sprintf("http://127.0.0.1:%d/leafz?subs=1", sb.MonitorAddr().Port)
	for pollMode := 1; pollMode < 2; pollMode++ {
		l := pollLeafz(t, sb, pollMode, pollURL, &LeafzOptions{Subscriptions: true})
		if l.ID != sb.ID() {
			t.Fatalf("Expected ID to be %q, got %q", sb.ID(), l.ID)
		}
		if l.Now.IsZero() {
			t.Fatalf("Expected Now to be set, was not")
		}
		if l.NumLeafs != 2 {
			t.Fatalf("Expected NumLeafs to be 1, got %v", l.NumLeafs)
		}
		if len(l.Leafs) != 2 {
			t.Fatalf("Expected array to be len 2, got %v", len(l.Leafs))
		}
		for _, ln := range l.Leafs {
			if ln.Account == acc1.Name {
				if ln.OutMsgs != 0 || ln.OutBytes != 0 || ln.InMsgs != 1 || ln.InBytes == 0 {
					t.Fatalf("Expected 1 InMsgs/Bytes and 0 OutMsgs/Bytes, got %+v", ln)
				}
			} else if ln.Account == acc2.Name {
				if ln.OutMsgs != 0 || ln.OutBytes != 0 || ln.InMsgs != 2 || ln.InBytes == 0 {
					t.Fatalf("Expected 2 InMsgs/Bytes and 0 OutMsgs/Bytes, got %+v", ln)
				}
			} else {
				t.Fatalf("Expected account to be %q or %q, got %q", acc1.Name, acc2.Name, ln.Account)
			}
			if ln.Name != leafName {
				t.Fatalf("Expected name to be %q, got %q", leafName, ln.Name)
			}
			if ln.IsSpoke {
				t.Fatal("Expected leafnode connection to be hub")
			}
			if ln.RTT == "" {
				t.Fatalf("RTT not tracked?")
			}
			// LDS should be only one.
			if ln.NumSubs != 5 || len(ln.Subs) != 5 {
				t.Fatalf("Expected 5 subs, got %v (%v)", ln.NumSubs, ln.Subs)
			}
		}
	}
}

func TestMonitorAccountz(t *testing.T) {
	s := RunServer(DefaultMonitorOptions())
	defer s.Shutdown()
	body := string(readBody(t, fmt.Sprintf("http://127.0.0.1:%d%s", s.MonitorAddr().Port, AccountzPath)))
	require_Contains(t, body, `$G`)
	require_Contains(t, body, `$SYS`)
	require_Contains(t, body, `"accounts": [`)
	require_Contains(t, body, `"system_account": "$SYS"`)

	body = string(readBody(t, fmt.Sprintf("http://127.0.0.1:%d%s?acc=$SYS", s.MonitorAddr().Port, AccountzPath)))
	require_Contains(t, body, `"account_detail": {`)
	require_Contains(t, body, `"account_name": "$SYS",`)
	require_Contains(t, body, `"subscriptions": 49,`)
	require_Contains(t, body, `"is_system": true,`)
	require_Contains(t, body, `"system_account": "$SYS"`)

	body = string(readBody(t, fmt.Sprintf("http://127.0.0.1:%d%s?unused=1", s.MonitorAddr().Port, AccountStatzPath)))
	require_Contains(t, body, `"acc": "$G"`)
	require_Contains(t, body, `"acc": "$SYS"`)
	require_Contains(t, body, `"sent": {`)
	require_Contains(t, body, `"received": {`)
	require_Contains(t, body, `"total_conns": 0,`)
	require_Contains(t, body, `"leafnodes": 0,`)
}

func TestMonitorAuthorizedUsers(t *testing.T) {
	kp, _ := nkeys.FromSeed(seed)
	usrNKey, _ := kp.PublicKey()
	opts := DefaultMonitorOptions()
	opts.Nkeys = []*NkeyUser{{Nkey: string(usrNKey)}}
	opts.Users = []*User{{Username: "user", Password: "pwd"}}
	s := RunServer(opts)
	defer s.Shutdown()

	checkAuthUser := func(expected string) {
		t.Helper()
		resetPreviousHTTPConnections()
		url := fmt.Sprintf("http://127.0.0.1:%d/connz?auth=true", s.MonitorAddr().Port)
		for mode := 0; mode < 2; mode++ {
			connz := pollConz(t, s, mode, url, &ConnzOptions{Username: true})
			if l := len(connz.Conns); l != 1 {
				t.Fatalf("Expected 1, got %v", l)
			}
			conn := connz.Conns[0]
			au := conn.AuthorizedUser
			if au == _EMPTY_ {
				t.Fatal("AuthorizedUser is empty!")
			}
			if au != expected {
				t.Fatalf("Expected %q, got %q", expected, au)
			}
		}
	}

	c := natsConnect(t, fmt.Sprintf("nats://user:pwd@127.0.0.1:%d", opts.Port))
	defer c.Close()
	checkAuthUser("user")
	c.Close()

	c = natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", opts.Port),
		nats.Nkey(usrNKey, func(nonce []byte) ([]byte, error) {
			return kp.Sign(nonce)
		}))
	defer c.Close()
	// we should get the user's NKey
	checkAuthUser(usrNKey)
	c.Close()

	s.Shutdown()
	opts = DefaultMonitorOptions()
	opts.Authorization = "sometoken"
	s = RunServer(opts)
	defer s.Shutdown()

	c = natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", opts.Port),
		nats.Token("sometoken"))
	defer c.Close()
	// We should get the token specified by the user
	checkAuthUser("sometoken")
	c.Close()
	s.Shutdown()

	opts = DefaultMonitorOptions()
	// User an operator seed
	kp, _ = nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()
	opts.TrustedKeys = []string{pub}
	s = RunServer(opts)
	defer s.Shutdown()

	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	ajwt, err := nac.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	nkp, _ := nkeys.CreateUser()
	upub, _ := nkp.PublicKey()
	nuc := jwt.NewUserClaims(upub)
	jwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	buildMemAccResolver(s)
	addAccountToMemResolver(s, apub, ajwt)

	c = natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", opts.Port),
		nats.UserJWT(
			func() (string, error) { return jwt, nil },
			func(nonce []byte) ([]byte, error) { return nkp.Sign(nonce) }))
	defer c.Close()
	// we should get the user's pubkey
	checkAuthUser(upub)
}

// Helper function to check that a JS cluster is formed
func checkForJSClusterUp(t *testing.T, servers ...*Server) {
	t.Helper()
	// We will use the other JetStream helpers here.
	c := &cluster{t: t, servers: servers}
	c.checkClusterFormed()
	c.waitOnClusterReady()
}

func TestMonitorJszNonJszServer(t *testing.T) {
	srv := RunServer(DefaultOptions())
	defer srv.Shutdown()

	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatalf("server did not become ready")
	}

	jsi, err := srv.Jsz(&JSzOptions{})
	if err != nil {
		t.Fatalf("jsi failed: %v", err)
	}
	if jsi.ID != srv.ID() {
		t.Fatalf("did not receive valid info")
	}

	jsi, err = srv.Jsz(&JSzOptions{LeaderOnly: true})
	if !errors.Is(err, errSkipZreq) {
		t.Fatalf("expected a skip z req error: %v", err)
	}
	if jsi != nil {
		t.Fatalf("expected no jsi: %v", jsi)
	}
}

func TestMonitorJsz(t *testing.T) {
	readJsInfo := func(url string) *JSInfo {
		t.Helper()
		body := readBody(t, url)
		info := &JSInfo{}
		err := json.Unmarshal(body, info)
		require_NoError(t, err)
		return info
	}
	srvs := []*Server{}
	for _, test := range []struct {
		port   int
		mport  int
		cport  int
		routed int
	}{
		{7500, 7501, 7502, 5502},
		{5500, 5501, 5502, 7502},
	} {
		tmpDir := t.TempDir()
		cf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:%d
		http: 127.0.0.1:%d
		system_account: SYS
		accounts {
			SYS {
				users [{user: sys, password: pwd}]
			}
			ACC {
				users [{user: usr, password: pwd}]
				// In clustered mode, these reservations will not impact any one server.
				jetstream: {max_store: 4Mb, max_memory: 5Mb}
			}
			BCC_TO_HAVE_ONE_EXTRA {
				users [{user: usr2, password: pwd}]
				jetstream: enabled
			}
		}
		jetstream: {
			max_mem_store: 10Mb
			max_file_store: 10Mb
			store_dir: '%s'
			unique_tag: az
		}
		cluster {
			name: cluster_name
			listen: 127.0.0.1:%d
			routes: [nats-route://127.0.0.1:%d]
		}
		server_name: server_%d
		server_tags: [ "az:%d" ] `, test.port, test.mport, tmpDir, test.cport, test.routed, test.port, test.port)))

		s, _ := RunServerWithConfig(cf)
		defer s.Shutdown()
		srvs = append(srvs, s)
	}
	checkClusterFormed(t, srvs...)
	checkForJSClusterUp(t, srvs...)

	nc := natsConnect(t, "nats://usr:pwd@127.0.0.1:7500")
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(5 * time.Second))
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "my-stream-replicated",
		Subjects: []string{"foo", "bar"},
		Replicas: 2,
	})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "my-stream-non-replicated",
		Subjects: []string{"baz"},
		Replicas: 1,
	})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "my-stream-mirror",
		Replicas: 2,
		Mirror: &nats.StreamSource{
			Name: "my-stream-replicated",
		},
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("my-stream-replicated", &nats.ConsumerConfig{
		Durable:   "my-consumer-replicated",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("my-stream-non-replicated", &nats.ConsumerConfig{
		Durable:   "my-consumer-non-replicated",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("my-stream-mirror", &nats.ConsumerConfig{
		Durable:   "my-consumer-mirror",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)
	nc.Flush()
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	// Wait for mirror replication
	time.Sleep(200 * time.Millisecond)

	monUrl1 := fmt.Sprintf("http://127.0.0.1:%d/jsz", 7501)
	monUrl2 := fmt.Sprintf("http://127.0.0.1:%d/jsz", 5501)

	t.Run("default", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url)
			if len(info.AccountDetails) != 0 {
				t.Fatalf("expected no account to be returned by %s but got %v", url, info)
			}
			if info.Streams == 0 {
				t.Fatalf("expected stream count to be 3 but got %d", info.Streams)
			}
			if info.Consumers == 0 {
				t.Fatalf("expected consumer count to be 3 but got %d", info.Consumers)
			}
			if info.Messages != 2 {
				t.Fatalf("expected two message but got %d", info.Messages)
			}
		}
	})
	t.Run("accounts", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?accounts=true")
			if len(info.AccountDetails) != 2 {
				t.Fatalf("expected both accounts to be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("accounts reserved metrics", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?accounts=true&acc=ACC")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected single account")
			}
			acc := info.AccountDetails[0]
			got := int(acc.ReservedMemory)
			expected := 5242880
			if got != expected {
				t.Errorf("Expected: %v, got: %v", expected, got)
			}
			got = int(acc.ReservedStore)
			expected = 4194304
			if got != expected {
				t.Errorf("Expected: %v, got: %v", expected, got)
			}

			info = readJsInfo(url + "?accounts=true&acc=BCC_TO_HAVE_ONE_EXTRA")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected single account")
			}
			acc = info.AccountDetails[0]
			got = int(acc.ReservedMemory)
			expected = -1
			if got != expected {
				t.Errorf("Expected: %v, got: %v", expected, got)
			}
			got = int(acc.ReservedStore)
			expected = -1
			if got != expected {
				t.Errorf("Expected: %v, got: %v", expected, got)
			}
		}
	})
	t.Run("offset-too-big", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?accounts=true&offset=10")
			if len(info.AccountDetails) != 0 {
				t.Fatalf("expected no accounts to be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("limit", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?accounts=true&limit=1")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected one account to be returned by %s but got %v", url, info)
			}
			if info := readJsInfo(url + "?accounts=true&offset=1&limit=1"); len(info.AccountDetails) != 1 {
				t.Fatalf("expected one account to be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("offset-stable", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info1 := readJsInfo(url + "?accounts=true&offset=1&limit=1")
			if len(info1.AccountDetails) != 1 {
				t.Fatalf("expected one account to be returned by %s but got %v", url, info1)
			}
			info2 := readJsInfo(url + "?accounts=true&offset=1&limit=1")
			if len(info2.AccountDetails) != 1 {
				t.Fatalf("expected one account to be returned by %s but got %v", url, info2)
			}
			if info1.AccountDetails[0].Name != info2.AccountDetails[0].Name {
				t.Fatalf("absent changes, same offset should result in same account but gut: %v %v",
					info1.AccountDetails[0].Name, info2.AccountDetails[0].Name)
			}
		}
	})
	t.Run("filter-account", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?acc=ACC")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}
			if info.AccountDetails[0].Name != "ACC" {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}
			if len(info.AccountDetails[0].Streams) != 0 {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("streams", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?acc=ACC&streams=true")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}
			if len(info.AccountDetails[0].Streams) == 0 {
				t.Fatalf("expected streams to be returned by %s but got %v", url, info)
			}
			if len(info.AccountDetails[0].Streams[0].Consumer) != 0 {
				t.Fatalf("expected no consumers to be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("consumers", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?acc=ACC&consumers=true")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}
			if len(info.AccountDetails[0].Streams[0].Consumer) == 0 {
				t.Fatalf("expected consumers to be returned by %s but got %v", url, info)
			}
			if info.AccountDetails[0].Streams[0].Config != nil {
				t.Fatal("Config expected to not be present")
			}
			if info.AccountDetails[0].Streams[0].Consumer[0].Config != nil {
				t.Fatal("Config expected to not be present")
			}
			if len(info.AccountDetails[0].Streams[0].ConsumerRaftGroups) != 0 {
				t.Fatalf("expected consumer raft groups to not be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("config", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?acc=ACC&consumers=true&config=true")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}
			if info.AccountDetails[0].Streams[0].Config == nil {
				t.Fatal("Config expected to be present")
			}
			if info.AccountDetails[0].Streams[0].Consumer[0].Config == nil {
				t.Fatal("Config expected to be present")
			}
		}
	})
	t.Run("replication", func(t *testing.T) {
		// The replication lag may only be present on the leader
		replicationFound := false
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?acc=ACC&streams=true")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}
			streamFound := false
			for _, stream := range info.AccountDetails[0].Streams {
				if stream.Name == "my-stream-mirror" {
					streamFound = true
					if stream.Mirror != nil {
						replicationFound = true
					}
				}
			}
			if !streamFound {
				t.Fatalf("Did not locate my-stream-mirror stream in results")
			}
		}
		if !replicationFound {
			t.Fatal("ReplicationLag expected to be present for my-stream-mirror stream")
		}
	})
	t.Run("cluster-info", func(t *testing.T) {
		found := 0
		for i, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "")
			if info.Meta.Peer != getHash(info.Meta.Leader) {
				t.Fatalf("Invalid Peer: %+v", info.Meta)
			}
			if info.Meta.Replicas != nil {
				found++
				for _, r := range info.Meta.Replicas {
					if r.Peer == _EMPTY_ {
						t.Fatalf("Replicas' Peer is empty: %+v", r)
					}
				}
				if info.Meta.Leader != srvs[i].Name() {
					t.Fatalf("received cluster info from non leader: leader %s, server: %s", info.Meta.Leader, srvs[i].Name())
				}
			}
		}
		if found == 0 {
			t.Fatalf("did not receive cluster info from any node")
		}
		if found > 1 {
			t.Fatalf("received cluster info from multiple nodes")
		}
	})
	t.Run("account-non-existing", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?acc=DOES_NOT_EXIT")
			if len(info.AccountDetails) != 0 {
				t.Fatalf("expected no account to be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("unique-tag-exists", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url)
			if len(info.Config.UniqueTag) == 0 {
				t.Fatalf("expected unique_tag to be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("raftgroups", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?acc=ACC&consumers=true&raft=true")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}

			// We will have two streams and order is not guaranteed. So grab the one we want.
			var si StreamDetail
			if info.AccountDetails[0].Streams[0].Name == "my-stream-replicated" {
				si = info.AccountDetails[0].Streams[0]
			} else {
				si = info.AccountDetails[0].Streams[1]
			}

			if len(si.Consumer) == 0 {
				t.Fatalf("expected consumers to be returned by %s but got %v", url, info)
			}
			if len(si.ConsumerRaftGroups) == 0 {
				t.Fatalf("expected consumer raft groups to be returned by %s but got %v", url, info)
			}
			if len(si.RaftGroup) == 0 {
				t.Fatal("expected stream raft group info to be included")
			}
			crgroup := si.ConsumerRaftGroups[0]
			if crgroup.Name != "my-consumer-replicated" && crgroup.Name != "my-consumer-mirror" {
				t.Fatalf("expected consumer name to be included in raft group info, got: %v", crgroup.Name)
			}
			if len(crgroup.RaftGroup) == 0 {
				t.Fatal("expected consumer raft group info to be included")
			}
		}
	})
}

func TestMonitorReloadTLSConfig(t *testing.T) {
	template := `
		listen: "127.0.0.1:-1"
		https: "127.0.0.1:-1"
		tls {
			cert_file: '%s'
			key_file: '%s'
			ca_file: '../test/configs/certs/ca.pem'

			# Set this to make sure that it does not impact secure monitoring
			# (which it did, see issue: https://github.com/nats-io/nats-server/issues/2980)
			verify_and_map: true
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template,
		"../test/configs/certs/server-noip.pem",
		"../test/configs/certs/server-key-noip.pem")))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	addr := fmt.Sprintf("127.0.0.1:%d", s.MonitorAddr().Port)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating ws connection: %v", err)
	}
	defer c.Close()

	tc := &TLSConfigOpts{CaFile: "../test/configs/certs/ca.pem"}
	tlsConfig, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating TLS config: %v", err)
	}
	tlsConfig.ServerName = "127.0.0.1"
	tlsConfig.RootCAs = tlsConfig.ClientCAs
	tlsConfig.ClientCAs = nil
	c = tls.Client(c, tlsConfig.Clone())
	if err := c.(*tls.Conn).Handshake(); err == nil || !strings.Contains(err.Error(), "SAN") {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.Close()

	reloadUpdateConfig(t, s, conf, fmt.Sprintf(template,
		"../test/configs/certs/server-cert.pem",
		"../test/configs/certs/server-key.pem"))

	c, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating ws connection: %v", err)
	}
	defer c.Close()

	c = tls.Client(c, tlsConfig.Clone())
	if err := c.(*tls.Conn).Handshake(); err != nil {
		t.Fatalf("Error on TLS handshake: %v", err)
	}

	// Need to read something to see if there is a problem with the certificate or not.
	var buf [64]byte
	c.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
	_, err = c.Read(buf[:])
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("Error: %v", err)
	}
}

func TestMonitorMQTT(t *testing.T) {
	o := DefaultOptions()
	o.HTTPHost = "127.0.0.1"
	o.HTTPPort = -1
	o.ServerName = "mqtt_server"
	o.Users = []*User{{Username: "someuser"}}
	pinnedCerts := make(PinnedCertSet)
	pinnedCerts["7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069"] = struct{}{}
	o.MQTT = MQTTOpts{
		Host:           "127.0.0.1",
		Port:           -1,
		NoAuthUser:     "someuser",
		JsDomain:       "js",
		AuthTimeout:    2.0,
		TLSMap:         true,
		TLSTimeout:     3.0,
		TLSPinnedCerts: pinnedCerts,
		AckWait:        4 * time.Second,
		MaxAckPending:  256,
	}
	s := RunServer(o)
	defer s.Shutdown()

	expected := &MQTTOptsVarz{
		Host:           "127.0.0.1",
		Port:           o.MQTT.Port,
		NoAuthUser:     "someuser",
		JsDomain:       "js",
		AuthTimeout:    2.0,
		TLSMap:         true,
		TLSTimeout:     3.0,
		TLSPinnedCerts: []string{"7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069"},
		AckWait:        4 * time.Second,
		MaxAckPending:  256,
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, s, mode, url, nil)
		vm := &v.MQTT
		if !reflect.DeepEqual(vm, expected) {
			t.Fatalf("Expected\n%+v\nGot:\n%+v", expected, vm)
		}
	}
}

func TestMonitorWebsocket(t *testing.T) {
	o := DefaultOptions()
	o.HTTPHost = "127.0.0.1"
	o.HTTPPort = -1
	kp, _ := nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()
	o.TrustedKeys = []string{pub}
	o.Users = []*User{{Username: "someuser"}}
	pinnedCerts := make(PinnedCertSet)
	pinnedCerts["7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069"] = struct{}{}
	o.Websocket = WebsocketOpts{
		Host:             "127.0.0.1",
		Port:             -1,
		Advertise:        "somehost:8080",
		NoAuthUser:       "someuser",
		JWTCookie:        "somecookiename",
		AuthTimeout:      2.0,
		NoTLS:            true,
		TLSMap:           true,
		TLSPinnedCerts:   pinnedCerts,
		SameOrigin:       true,
		AllowedOrigins:   []string{"origin1", "origin2"},
		Compression:      true,
		HandshakeTimeout: 4 * time.Second,
	}
	s := RunServer(o)
	defer s.Shutdown()

	expected := &WebsocketOptsVarz{
		Host:             "127.0.0.1",
		Port:             o.Websocket.Port,
		Advertise:        "somehost:8080",
		NoAuthUser:       "someuser",
		JWTCookie:        "somecookiename",
		AuthTimeout:      2.0,
		NoTLS:            true,
		TLSMap:           true,
		TLSPinnedCerts:   []string{"7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069"},
		SameOrigin:       true,
		AllowedOrigins:   []string{"origin1", "origin2"},
		Compression:      true,
		HandshakeTimeout: 4 * time.Second,
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, s, mode, url, nil)
		vw := &v.Websocket
		if !reflect.DeepEqual(vw, expected) {
			t.Fatalf("Expected\n%+v\nGot:\n%+v", expected, vw)
		}
	}
}

func TestServerIDZRequest(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: TEST22
		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`))
	defer removeFile(t, conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)

	subject := fmt.Sprintf(serverPingReqSubj, "IDZ")
	resp, err := nc.Request(subject, nil, time.Second)
	require_NoError(t, err)

	var sid ServerID
	err = json.Unmarshal(resp.Data, &sid)
	require_NoError(t, err)

	require_True(t, sid.Name == "TEST22")
	require_True(t, strings.HasPrefix(sid.ID, "N"))
}

func TestMonitorProfilez(t *testing.T) {
	s := RunServer(DefaultOptions())
	defer s.Shutdown()

	// Then start profiling.
	s.StartProfiler()

	// Now check that all of the profiles that we expect are
	// returning instead of erroring.
	for _, try := range []*ProfilezOptions{
		{Name: "allocs", Debug: 0},
		{Name: "allocs", Debug: 1},
		{Name: "block", Debug: 0},
		{Name: "goroutine", Debug: 0},
		{Name: "goroutine", Debug: 1},
		{Name: "goroutine", Debug: 2},
		{Name: "heap", Debug: 0},
		{Name: "heap", Debug: 1},
		{Name: "mutex", Debug: 0},
		{Name: "threadcreate", Debug: 0},
	} {
		if ps := s.profilez(try); ps.Error != _EMPTY_ {
			t.Fatalf("Unexpected error on %v: %s", try, ps.Error)
		}
	}
}

func TestMonitorRoutePoolSize(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		http: -1
		cluster {
			port: -1
			name: "local"
			pool_size: 5
		}
		no_sys_acc: true
	`))
	defer removeFile(t, conf1)
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf23 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		http: -1
		cluster {
			port: -1
			name: "local"
			routes: ["nats://127.0.0.1:%d"]
			pool_size: 5
		}
		no_sys_acc: true
	`, o1.Cluster.Port)))
	defer removeFile(t, conf23)

	s2, _ := RunServerWithConfig(conf23)
	defer s2.Shutdown()
	s3, _ := RunServerWithConfig(conf23)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	for i, s := range []*Server{s1, s2, s3} {
		url := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
		for mode := 0; mode < 2; mode++ {
			v := pollVarz(t, s, mode, url, nil)
			if v.Cluster.PoolSize != 5 {
				t.Fatalf("Expected Cluster.PoolSize==5, got %v", v.Cluster.PoolSize)
			}
			if v.Remotes != 2 {
				t.Fatalf("Expected Remotes==2, got %v", v.Remotes)
			}
			if v.Routes != 10 {
				t.Fatalf("Expected NumRoutes==10, got %v", v.Routes)
			}
		}

		url = fmt.Sprintf("http://127.0.0.1:%d/routez", s.MonitorAddr().Port)
		for mode := 0; mode < 2; mode++ {
			v := pollRoutez(t, s, mode, url, nil)
			if v.NumRoutes != 10 {
				t.Fatalf("Expected NumRoutes==10, got %v", v.NumRoutes)
			}
			if n := len(v.Routes); n != 10 {
				t.Fatalf("Expected len(Routes)==10, got %v", n)
			}
			remotes := make(map[string]int)
			for _, r := range v.Routes {
				remotes[r.RemoteID]++
			}
			if n := len(remotes); n != 2 {
				t.Fatalf("Expected routes for 2 different servers, got %v", n)
			}
			switch i {
			case 0:
				if n := remotes[s2.ID()]; n != 5 {
					t.Fatalf("Expected 5 routes from S1 to S2, got %v", n)
				}
				if n := remotes[s3.ID()]; n != 5 {
					t.Fatalf("Expected 5 routes from S1 to S3, got %v", n)
				}
			case 1:
				if n := remotes[s1.ID()]; n != 5 {
					t.Fatalf("Expected 5 routes from S2 to S1, got %v", n)
				}
				if n := remotes[s3.ID()]; n != 5 {
					t.Fatalf("Expected 5 routes from S2 to S3, got %v", n)
				}
			case 2:
				if n := remotes[s1.ID()]; n != 5 {
					t.Fatalf("Expected 5 routes from S3 to S1, got %v", n)
				}
				if n := remotes[s2.ID()]; n != 5 {
					t.Fatalf("Expected 5 routes from S3 to S2, got %v", n)
				}
			}
		}
	}
}

func TestMonitorRoutePerAccount(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		http: -1
		accounts {
			A { users: [{user: "a", password: "pwd"}] }
		}
		cluster {
			port: -1
			name: "local"
			accounts: ["A"]
		}
	`))
	defer removeFile(t, conf1)
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf23 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		http: -1
		accounts {
			A { users: [{user: "a", password: "pwd"}] }
		}
		cluster {
			port: -1
			name: "local"
			routes: ["nats://127.0.0.1:%d"]
			accounts: ["A"]
		}
	`, o1.Cluster.Port)))
	defer removeFile(t, conf23)

	s2, _ := RunServerWithConfig(conf23)
	defer s2.Shutdown()
	s3, _ := RunServerWithConfig(conf23)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	for _, s := range []*Server{s1, s2, s3} {
		// Default pool size + account "A" + system account (added by default)
		enr := 2 * (DEFAULT_ROUTE_POOL_SIZE + 1 + 1)
		url := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
		for mode := 0; mode < 2; mode++ {
			v := pollVarz(t, s, mode, url, nil)
			if v.Remotes != 2 {
				t.Fatalf("Expected Remotes==2, got %v", v.Remotes)
			}
			if v.Routes != enr {
				t.Fatalf("Expected NumRoutes==%d, got %v", enr, v.Routes)
			}
		}

		url = fmt.Sprintf("http://127.0.0.1:%d/routez", s.MonitorAddr().Port)
		for mode := 0; mode < 2; mode++ {
			v := pollRoutez(t, s, mode, url, nil)
			if v.NumRoutes != enr {
				t.Fatalf("Expected NumRoutes==%d, got %v", enr, v.NumRoutes)
			}
			if n := len(v.Routes); n != enr {
				t.Fatalf("Expected len(Routes)==%d, got %v", enr, n)
			}
			remotes := make(map[string]int)
			for _, r := range v.Routes {
				var acc int
				if r.Account == "A" {
					acc = 1
				}
				remotes[r.RemoteID] += acc
			}
			if n := len(remotes); n != 2 {
				t.Fatalf("Expected routes for 2 different servers, got %v", n)
			}
			for remoteID, v := range remotes {
				if v != 1 {
					t.Fatalf("Expected one and only one connection for account A for remote %q, got %v", remoteID, v)
				}
			}
		}
	}
}

func TestMonitorConnzOperatorModeFilterByUser(t *testing.T) {
	accKp, accPub := createKey(t)
	accClaim := jwt.NewAccountClaims(accPub)
	accJwt := encodeClaim(t, accClaim, accPub)

	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		http: 127.0.0.1:-1
		operator = %s
		resolver = MEMORY
		resolver_preload = {
			%s : %s
		}
	`, ojwt, accPub, accJwt)))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	createUser := func() (string, string) {
		ukp, _ := nkeys.CreateUser()
		seed, _ := ukp.Seed()
		upub, _ := ukp.PublicKey()
		uclaim := newJWTTestUserClaims()
		uclaim.Subject = upub
		ujwt, err := uclaim.Encode(accKp)
		require_NoError(t, err)
		return upub, genCredsFile(t, ujwt, seed)
	}

	// Now create 2 users.
	aUser, aCreds := createUser()
	bUser, bCreds := createUser()

	var users []*nats.Conn

	// Create 2 for A
	for i := 0; i < 2; i++ {
		nc, err := nats.Connect(s.ClientURL(), nats.UserCredentials(aCreds))
		require_NoError(t, err)
		defer nc.Close()
		users = append(users, nc)
	}
	// Create 5 for B
	for i := 0; i < 5; i++ {
		nc, err := nats.Connect(s.ClientURL(), nats.UserCredentials(bCreds))
		require_NoError(t, err)
		defer nc.Close()
		users = append(users, nc)
	}

	// Test A
	connz := pollConz(t, s, 1, _EMPTY_, &ConnzOptions{User: aUser, Username: true})
	require_True(t, connz.NumConns == 2)
	for _, ci := range connz.Conns {
		require_True(t, ci.AuthorizedUser == aUser)
	}
	// Test B
	connz = pollConz(t, s, 1, _EMPTY_, &ConnzOptions{User: bUser, Username: true})
	require_True(t, connz.NumConns == 5)
	for _, ci := range connz.Conns {
		require_True(t, ci.AuthorizedUser == bUser)
	}

	// Make sure URL access is the same.
	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	urlFull := url + fmt.Sprintf("connz?auth=true&user=%s", aUser)
	connz = pollConz(t, s, 0, urlFull, nil)
	require_True(t, connz.NumConns == 2)
	for _, ci := range connz.Conns {
		require_True(t, ci.AuthorizedUser == aUser)
	}

	// Now test closed filtering as well.
	for _, nc := range users {
		nc.Close()
	}
	// Let them process and be moved to closed ring buffer in server.
	time.Sleep(100 * time.Millisecond)

	connz = pollConz(t, s, 1, _EMPTY_, &ConnzOptions{User: aUser, Username: true, State: ConnClosed})
	require_True(t, connz.NumConns == 2)
	for _, ci := range connz.Conns {
		require_True(t, ci.AuthorizedUser == aUser)
	}
}

func TestMonitorConnzSortByRTT(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	for i := 0; i < 10; i++ {
		nc, err := nats.Connect(s.ClientURL())
		require_NoError(t, err)
		defer nc.Close()
	}

	connz := pollConz(t, s, 1, _EMPTY_, &ConnzOptions{Sort: ByRTT})
	require_True(t, connz.NumConns == 10)

	var rtt int64
	for _, ci := range connz.Conns {
		if rtt == 0 {
			rtt = ci.rtt
		} else {
			if ci.rtt > rtt {
				t.Fatalf("RTT not in descending order: %v vs %v",
					time.Duration(rtt), time.Duration(ci.rtt))
			}
			rtt = ci.rtt
		}
	}

	// Make sure url works as well.
	url := fmt.Sprintf("http://127.0.0.1:%d/connz?sort=rtt", s.MonitorAddr().Port)
	connz = pollConz(t, s, 0, url, nil)
	require_True(t, connz.NumConns == 10)

	rtt = 0
	for _, ci := range connz.Conns {
		crttd, err := time.ParseDuration(ci.RTT)
		require_NoError(t, err)
		crtt := int64(crttd)
		if rtt == 0 {
			rtt = crtt
		} else {
			if crtt > rtt {
				t.Fatalf("RTT not in descending order: %v vs %v",
					time.Duration(rtt), time.Duration(crtt))
			}
			rtt = ci.rtt
		}
	}
}

// https://github.com/nats-io/nats-server/issues/4144
func TestMonitorAccountszMappingOrderReporting(t *testing.T) {
	conf := createConfFile(t, []byte(`
	listen: 127.0.0.1:-1
	server_name: SR22
	accounts {
		CLOUD {
			exports [ { service: "downlink.>" } ]
		}
		APP {
			imports [ { service: { account: CLOUD, subject: "downlink.>"}, to: "event.>"} ]
		}
	}`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	az, err := s.Accountz(&AccountzOptions{"APP"})
	require_NoError(t, err)
	require_NotNil(t, az.Account)
	require_True(t, len(az.Account.Imports) > 0)

	var found bool
	for _, si := range az.Account.Imports {
		if si.Import.Subject == "downlink.>" {
			found = true
			require_True(t, si.Import.LocalSubject == "event.>")
			break
		}
	}
	require_True(t, found)
}
