// Copyright 2013-2018 The NATS Authors
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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"net"

	"github.com/nats-io/go-nats"
)

const CLIENT_PORT = -1
const MONITOR_PORT = -1
const CLUSTER_PORT = -1

func DefaultMonitorOptions() *Options {
	return &Options{
		Host:     "127.0.0.1",
		Port:     CLIENT_PORT,
		HTTPHost: "127.0.0.1",
		HTTPPort: MONITOR_PORT,
		NoLog:    true,
		NoSigs:   true,
	}
}

func runMonitorServer() *Server {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	return RunServer(opts)
}

func runMonitorServerNoHTTPPort() *Server {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.HTTPPort = 0
	return RunServer(opts)
}

func resetPreviousHTTPConnections() {
	http.DefaultTransport = &http.Transport{}
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
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		stackFatalf(t, "Got an error reading the body: %v\n", err)
	}
	return body
}

func readBody(t *testing.T, url string) []byte {
	return readBodyEx(t, url, http.StatusOK, appJSONContent)
}

func pollVarz(t *testing.T, s *Server, mode int, url string, opts *VarzOptions) *Varz {
	if mode == 0 {
		v := &Varz{}
		body := readBody(t, url)
		if err := json.Unmarshal(body, v); err != nil {
			stackFatalf(t, "Got an error unmarshalling the body: %v\n", err)
		}
		return v
	}
	v, _ := s.Varz(opts)
	return v
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
	}

	// Test JSONP
	readBodyEx(t, url+"varz?callback=callback", http.StatusOK, appJSContent)
}

func pollConz(t *testing.T, s *Server, mode int, url string, opts *ConnzOptions) *Connz {
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
		stackFatalf(t, "Error on Connz(): %v", err)
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
		if ci.RTT != "" {
			t.Fatal("Expected RTT to NOT be set for new connection\n")
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
		if rtt > 20*time.Millisecond || rtt < 100*time.Nanosecond {
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

		// Sub should trigger update.
		sub, _ := ncFoo.Subscribe("hello.world", func(m *nats.Msg) {})
		ensureServerActivityRecorded(t, ncFoo)

		ciFoo, _ = getFooAndBar(t, createConnMap(t, pollConz(t, s, mode, url, opts)))
		nextLA := ciFoo.LastActivity
		if fooLA.Equal(nextLA) {
			t.Fatalf("Subscribe should have triggered update to LastActivity %+v\n", ciFoo)
		}
		fooLA = nextLA

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
		barLA = nextLA

		// Message delivery on ncFoo should have triggered as well.
		nextLA = ciFoo.LastActivity
		if fooLA.Equal(nextLA) {
			t.Fatalf("Message delivery should have triggered update to LastActivity\n")
		}
		fooLA = nextLA

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
	opts := DefaultMonitorOptions()
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

	//Now adjust the Stop times for these with some random values.
	s.mu.Lock()
	now := time.Now()
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

	//Now adjust the Reasons for these with some random values.
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
	if mode == 0 {
		rz := &Routez{}
		body := readBody(t, url)
		if err := json.Unmarshal(body, rz); err != nil {
			stackFatalf(t, "Got an error unmarshalling the body: %v\n", err)
		}
		return rz
	}
	rz, _ := s.Routez(opts)
	return rz
}

func TestConnzWithRoutes(t *testing.T) {

	opts := DefaultMonitorOptions()
	opts.Cluster.Host = "127.0.0.1"
	opts.Cluster.Port = CLUSTER_PORT

	s := RunServer(opts)
	defer s.Shutdown()

	opts = &Options{
		Host: "127.0.0.1",
		Port: -1,
		Cluster: ClusterOpts{
			Host: "127.0.0.1",
			Port: -1,
		},
		NoLog:  true,
		NoSigs: true,
	}
	routeURL, _ := url.Parse(fmt.Sprintf("nats-route://127.0.0.1:%d", s.ClusterAddr().Port))
	opts.Routes = []*url.URL{routeURL}

	sc := RunServer(opts)
	defer sc.Shutdown()

	checkClusterFormed(t, s, sc)

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz", nil)
		// Test contents..
		// Make sure routes don't show up under connz, but do under routez
		if c.NumConns != 0 {
			t.Fatalf("Expected 0 connections, got %d\n", c.NumConns)
		}
		if c.Conns == nil || len(c.Conns) != 0 {
			t.Fatalf("Expected 0 connections in array, got %p\n", c.Conns)
		}
	}

	nc := createClientConnSubscribeAndPublish(t, sc)
	defer nc.Close()

	nc.Subscribe("hello.bar", func(m *nats.Msg) {})
	nc.Flush()
	checkExpectedSubs(t, 1, s, sc)

	// Now check routez
	urls := []string{"routez", "routez?subs=1"}
	for subs, urlSuffix := range urls {
		for mode := 0; mode < 2; mode++ {
			rz := pollRoutez(t, s, mode, url+urlSuffix, &RoutezOptions{Subscriptions: subs == 1})

			if rz.NumRoutes != 1 {
				t.Fatalf("Expected 1 route, got %d\n", rz.NumRoutes)
			}

			if len(rz.Routes) != 1 {
				t.Fatalf("Expected route array of 1, got %v\n", len(rz.Routes))
			}

			route := rz.Routes[0]

			if route.DidSolicit {
				t.Fatalf("Expected unsolicited route, got %v\n", route.DidSolicit)
			}

			// Don't ask for subs, so there should not be any
			if subs == 0 {
				if len(route.Subs) != 0 {
					t.Fatalf("There should not be subs, got %v", len(route.Subs))
				}
			} else {
				if len(route.Subs) != 1 {
					t.Fatalf("There should be 1 sub, got %v", len(route.Subs))
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
	if mode == 0 {
		body := readBody(t, url)
		sz := &Subsz{}
		if err := json.Unmarshal(body, sz); err != nil {
			stackFatalf(t, "Got an error unmarshalling the body: %v\n", err)
		}
		return sz
	}
	sz, _ := s.Subsz(opts)
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
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected a %d response, got %d\n", http.StatusOK, resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
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
	defer resp.Body.Close()
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
		// Look at all open
		c := pollConz(t, s, mode, url+"connz?state=open", &ConnzOptions{State: ConnOpen})
		if lc := len(c.Conns); lc != numEach {
			t.Fatalf("Expected %d connections in array, got %d\n", numEach, lc)
		}
		// Look at all closed
		c = pollConz(t, s, mode, url+"connz?state=closed", &ConnzOptions{State: ConnClosed})
		if lc := len(c.Conns); lc != numEach {
			t.Fatalf("Expected %d connections in array, got %d\n", numEach, lc)
		}
		// Look at all
		c = pollConz(t, s, mode, url+"connz?state=ALL", &ConnzOptions{State: ConnAll})
		if lc := len(c.Conns); lc != numEach*2 {
			t.Fatalf("Expected %d connections in array, got %d\n", 2*numEach, lc)
		}
		// Look at CID #1, which is in closed.
		c = pollConz(t, s, mode, url+"connz?cid=1&state=open", &ConnzOptions{CID: 1, State: ConnOpen})
		if lc := len(c.Conns); lc != 0 {
			t.Fatalf("Expected no connections in open array, got %d\n", lc)
		}
		c = pollConz(t, s, mode, url+"connz?cid=1&state=closed", &ConnzOptions{CID: 1, State: ConnClosed})
		if lc := len(c.Conns); lc != 1 {
			t.Fatalf("Expected a connection in closed array, got %d\n", lc)
		}
		c = pollConz(t, s, mode, url+"connz?cid=1&state=ALL", &ConnzOptions{CID: 1, State: ConnAll})
		if lc := len(c.Conns); lc != 1 {
			t.Fatalf("Expected a connection in closed array, got %d\n", lc)
		}
		c = pollConz(t, s, mode, url+"connz?cid=1&state=closed&subs=true",
			&ConnzOptions{CID: 1, State: ConnClosed, Subscriptions: true})
		if lc := len(c.Conns); lc != 1 {
			t.Fatalf("Expected a connection in closed array, got %d\n", lc)
		}
		ci := c.Conns[0]
		if ci.NumSubs != 1 {
			t.Fatalf("Expected NumSubs to be 1, got %d\n", ci.NumSubs)
		}
		if len(ci.Subs) != 1 {
			t.Fatalf("Expected len(ci.Subs) to be 1 also, got %d\n", len(ci.Subs))
		}
		// Now ask for same thing without subs and make sure they are not returned.
		c = pollConz(t, s, mode, url+"connz?cid=1&state=closed&subs=false",
			&ConnzOptions{CID: 1, State: ConnClosed, Subscriptions: false})
		if lc := len(c.Conns); lc != 1 {
			t.Fatalf("Expected a connection in closed array, got %d\n", lc)
		}
		ci = c.Conns[0]
		if ci.NumSubs != 1 {
			t.Fatalf("Expected NumSubs to be 1, got %d\n", ci.NumSubs)
		}
		if len(ci.Subs) != 0 {
			t.Fatalf("Expected len(ci.Subs) to be 0 since subs=false, got %d\n", len(ci.Subs))
		}

		// CID #2 is in open
		c = pollConz(t, s, mode, url+"connz?cid=2&state=open", &ConnzOptions{CID: 2, State: ConnOpen})
		if lc := len(c.Conns); lc != 1 {
			t.Fatalf("Expected a connection in open array, got %d\n", lc)
		}
		c = pollConz(t, s, mode, url+"connz?cid=2&state=closed", &ConnzOptions{CID: 2, State: ConnClosed})
		if lc := len(c.Conns); lc != 0 {
			t.Fatalf("Expected no connections in closed array, got %d\n", lc)
		}
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
func createClientConnSubscribeAndPublish(t *testing.T, s *Server) *nats.Conn {
	natsURL := fmt.Sprintf("nats://127.0.0.1:%d", s.Addr().(*net.TCPAddr).Port)
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
				if resp.StatusCode != http.StatusOK {
					ech <- fmt.Sprintf("Expected a %v response, got %d\n", http.StatusOK, resp.StatusCode)
					return
				}
				ct := resp.Header.Get("Content-Type")
				if ct != "application/json" {
					ech <- fmt.Sprintf("Expected application/json content-type, got %s\n", ct)
					return
				}
				defer resp.Body.Close()
				if _, err := ioutil.ReadAll(resp.Body); err != nil {
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
	srvAOpts.Cluster.Port = -1
	srvA := RunServer(srvAOpts)
	defer srvA.Shutdown()

	srvBOpts := nextServerOpts(srvAOpts)
	srvBOpts.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srvA.ClusterAddr().Port))

	url := fmt.Sprintf("http://127.0.0.1:%d/", srvA.MonitorAddr().Port)
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
		if resp, err := http.Get(url + "routez"); err != nil {
			time.Sleep(10 * time.Millisecond)
		} else {
			resp.Body.Close()
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

func TestServerIDs(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	murl := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, s, mode, murl+"varz", nil)
		if v.ID == "" {
			t.Fatal("Varz ID is empty")
		}
		c := pollConz(t, s, mode, murl+"connz", nil)
		if c.ID == "" {
			t.Fatal("Connz ID is empty")
		}
		r := pollRoutez(t, s, mode, murl+"routez", nil)
		if r.ID == "" {
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
