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

package test

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/go-nats"
)

const CLIENT_PORT = 11422
const MONITOR_PORT = 11522

func runMonitorServer() *server.Server {
	resetPreviousHTTPConnections()
	opts := DefaultTestOptions
	opts.Port = CLIENT_PORT
	opts.HTTPPort = MONITOR_PORT
	opts.HTTPHost = "127.0.0.1"

	return RunServer(&opts)
}

// Runs a clustered pair of monitor servers for testing the /routez endpoint
func runMonitorServerClusteredPair(t *testing.T) (*server.Server, *server.Server) {
	resetPreviousHTTPConnections()
	opts := DefaultTestOptions
	opts.Port = CLIENT_PORT
	opts.HTTPPort = MONITOR_PORT
	opts.HTTPHost = "127.0.0.1"
	opts.Cluster = server.ClusterOpts{Host: "127.0.0.1", Port: 10223}
	opts.Routes = server.RoutesFromStr("nats-route://127.0.0.1:10222")

	s1 := RunServer(&opts)

	opts2 := DefaultTestOptions
	opts2.Port = CLIENT_PORT + 1
	opts2.HTTPPort = MONITOR_PORT + 1
	opts2.HTTPHost = "127.0.0.1"
	opts2.Cluster = server.ClusterOpts{Host: "127.0.0.1", Port: 10222}
	opts2.Routes = server.RoutesFromStr("nats-route://127.0.0.1:10223")

	s2 := RunServer(&opts2)

	checkClusterFormed(t, s1, s2)

	return s1, s2
}

func runMonitorServerNoHTTPPort() *server.Server {
	resetPreviousHTTPConnections()
	opts := DefaultTestOptions
	opts.Port = CLIENT_PORT
	opts.HTTPPort = 0

	return RunServer(&opts)
}

func resetPreviousHTTPConnections() {
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
}

// Make sure that we do not run the http server for monitoring unless asked.
func TestNoMonitorPort(t *testing.T) {
	s := runMonitorServerNoHTTPPort()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", MONITOR_PORT)
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

// testEndpointDataRace tests a monitoring endpoint for data races by polling
// while client code acts to ensure statistics are updated. It is designed to
// run under the -race flag to catch violations. The caller must start the
// NATS server.
func testEndpointDataRace(endpoint string, t *testing.T) {
	var doneWg sync.WaitGroup

	url := fmt.Sprintf("http://127.0.0.1:%d/", MONITOR_PORT)

	// Poll as fast as we can, while creating connections, publishing,
	// and subscribing.
	clientDone := int64(0)
	doneWg.Add(1)
	go func() {
		for atomic.LoadInt64(&clientDone) == 0 {
			resp, err := http.Get(url + endpoint)
			if err != nil {
				t.Errorf("Expected no error: Got %v\n", err)
			} else {
				resp.Body.Close()
			}
		}
		doneWg.Done()
	}()

	// create connections, subscriptions, and publish messages to
	// update the monitor variables.
	var conns []net.Conn
	for i := 0; i < 50; i++ {
		cl := createClientConnSubscribeAndPublish(t)
		// keep a few connections around to test monitor variables.
		if i%10 == 0 {
			conns = append(conns, cl)
		} else {
			cl.Close()
		}
	}
	atomic.AddInt64(&clientDone, 1)

	// wait for the endpoint polling goroutine to exit
	doneWg.Wait()

	// cleanup the conns
	for _, cl := range conns {
		cl.Close()
	}
}

func TestEndpointDataRaces(t *testing.T) {
	// setup a small cluster to test /routez
	s1, s2 := runMonitorServerClusteredPair(t)
	defer s1.Shutdown()
	defer s2.Shutdown()

	// test all of our endpoints
	testEndpointDataRace("varz", t)
	testEndpointDataRace("connz", t)
	testEndpointDataRace("routez", t)
	testEndpointDataRace("subsz", t)
	testEndpointDataRace("stacksz", t)
}

func TestVarz(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", MONITOR_PORT)
	resp, err := http.Get(url + "varz")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}

	v := server.Varz{}
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	// Do some sanity checks on values
	if time.Since(v.Start) > 10*time.Second {
		t.Fatal("Expected start time to be within 10 seconds.")
	}

	cl := createClientConnSubscribeAndPublish(t)
	defer cl.Close()

	resp, err = http.Get(url + "varz")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}

	if strings.Contains(string(body), "cluster_port") {
		t.Fatal("Varz body contains cluster information when no cluster is defined.")
	}

	v = server.Varz{}
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	if v.Connections != 1 {
		t.Fatalf("Expected Connections of 1, got %v\n", v.Connections)
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
	if v.MaxPending != server.MAX_PENDING_SIZE {
		t.Fatalf("Expected MaxPending of %d, got %v\n",
			server.MAX_PENDING_SIZE, v.MaxPending)
	}
	if v.WriteDeadline != server.DEFAULT_FLUSH_DEADLINE {
		t.Fatalf("Expected WriteDeadline of %d, got %v\n",
			server.DEFAULT_FLUSH_DEADLINE, v.WriteDeadline)
	}
}

func TestConnz(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", MONITOR_PORT)
	resp, err := http.Get(url + "connz")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}

	c := server.Connz{}
	if err := json.Unmarshal(body, &c); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

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

	cl := createClientConnSubscribeAndPublish(t)
	defer cl.Close()

	resp, err = http.Get(url + "connz")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}
	if err := json.Unmarshal(body, &c); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	if c.NumConns != 1 {
		t.Fatalf("Expected 1 connection, got %d\n", c.NumConns)
	}
	if c.Total != 1 {
		t.Fatalf("Expected 1 live connection, got %d\n", c.Total)
	}
	if c.Conns == nil || len(c.Conns) != 1 {
		t.Fatalf("Expected 1 connection in array, got %p\n", c.Conns)
	}

	if c.Limit != server.DefaultConnListSize {
		t.Fatalf("Expected limit of %d, got %v\n", server.DefaultConnListSize, c.Limit)
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
	if ci.NumSubs != 1 {
		t.Fatalf("Expected num_subs of 1, got %v\n", ci.NumSubs)
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
}

func TestTLSConnz(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/tls.conf")
	defer srv.Shutdown()
	rootCAFile := "./configs/certs/ca.pem"
	clientCertFile := "./configs/certs/client-cert.pem"
	clientKeyFile := "./configs/certs/client-key.pem"

	// Test with secure connection
	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	nurl := fmt.Sprintf("tls://%s:%s@%s/", opts.Username, opts.Password, endpoint)
	nc, err := nats.Connect(nurl, nats.RootCAs(rootCAFile))
	if err != nil {
		t.Fatalf("Got an error on Connect with Secure Options: %+v\n", err)
	}
	defer nc.Close()
	ch := make(chan struct{})
	nc.Subscribe("foo", func(m *nats.Msg) { ch <- struct{}{} })
	nc.Publish("foo", []byte("Hello"))

	// Wait for message
	<-ch

	url := fmt.Sprintf("https://127.0.0.1:%d/", opts.HTTPSPort)
	tlsConfig := &tls.Config{}
	caCert, err := ioutil.ReadFile(rootCAFile)
	if err != nil {
		t.Fatalf("Got error reading RootCA file: %s", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		t.Fatalf("Got error reading client certificates: %s", err)
	}
	tlsConfig.Certificates = []tls.Certificate{cert}
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	httpClient := &http.Client{Transport: transport}

	resp, err := httpClient.Get(url + "connz")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}
	c := server.Connz{}
	if err := json.Unmarshal(body, &c); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	if c.NumConns != 1 {
		t.Fatalf("Expected 1 connection, got %d\n", c.NumConns)
	}
	if c.Total != 1 {
		t.Fatalf("Expected 1 live connection, got %d\n", c.Total)
	}
	if c.Conns == nil || len(c.Conns) != 1 {
		t.Fatalf("Expected 1 connection in array, got %d\n", len(c.Conns))
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
	if ci.NumSubs != 1 {
		t.Fatalf("Expected num_subs of 1, got %v\n", ci.NumSubs)
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
		t.Fatalf("Expected Start to be valid\n")
	}
	if ci.Uptime == "" {
		t.Fatalf("Expected Uptime to be valid\n")
	}
	if ci.LastActivity.IsZero() {
		t.Fatalf("Expected LastActivity to be valid\n")
	}
	if ci.LastActivity.UnixNano() < ci.Start.UnixNano() {
		t.Fatalf("Expected LastActivity [%v] to be > Start [%v]\n", ci.LastActivity, ci.Start)
	}
	if ci.Idle == "" {
		t.Fatalf("Expected Idle to be valid\n")
	}
}

func TestConnzWithSubs(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	cl := createClientConnSubscribeAndPublish(t)
	defer cl.Close()

	url := fmt.Sprintf("http://127.0.0.1:%d/", MONITOR_PORT)
	resp, err := http.Get(url + "connz?subs=1")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}

	c := server.Connz{}
	if err := json.Unmarshal(body, &c); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	// Test inside details of each connection
	ci := c.Conns[0]
	if len(ci.Subs) != 1 || ci.Subs[0] != "foo" {
		t.Fatalf("Expected subs of 1, got %v\n", ci.Subs)
	}
}

func TestConnzWithAuth(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/multi_user.conf")
	defer srv.Shutdown()

	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	curl := fmt.Sprintf("nats://%s:%s@%s/", opts.Users[0].Username, opts.Users[0].Password, endpoint)
	nc, err := nats.Connect(curl)
	if err != nil {
		t.Fatalf("Got an error on Connect: %+v\n", err)
	}
	defer nc.Close()

	ch := make(chan struct{})
	nc.Subscribe("foo", func(m *nats.Msg) { ch <- struct{}{} })
	nc.Publish("foo", []byte("Hello"))

	// Wait for message
	<-ch

	url := fmt.Sprintf("http://127.0.0.1:%d/", opts.HTTPPort)

	resp, err := http.Get(url + "connz?auth=1")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}

	c := server.Connz{}
	if err := json.Unmarshal(body, &c); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	// Test that we have authorized_user and its Alice.
	ci := c.Conns[0]
	if ci.AuthorizedUser != opts.Users[0].Username {
		t.Fatalf("Expected authorized_user to be %q, got %q\n",
			opts.Users[0].Username, ci.AuthorizedUser)
	}

}

func TestConnzWithOffsetAndLimit(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	cl1 := createClientConnSubscribeAndPublish(t)
	defer cl1.Close()

	cl2 := createClientConnSubscribeAndPublish(t)
	defer cl2.Close()

	url := fmt.Sprintf("http://127.0.0.1:%d/", MONITOR_PORT)
	resp, err := http.Get(url + "connz?offset=1&limit=1")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}

	c := server.Connz{}
	if err := json.Unmarshal(body, &c); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	if c.Limit != 1 {
		t.Fatalf("Expected limit of 1, got %v\n", c.Limit)
	}

	if c.Offset != 1 {
		t.Fatalf("Expected offset of 1, got %v\n", c.Offset)
	}

	if len(c.Conns) != 1 {
		t.Fatalf("Expected conns of 1, got %v\n", len(c.Conns))
	}
}

func TestSubsz(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	cl := createClientConnSubscribeAndPublish(t)
	defer cl.Close()

	url := fmt.Sprintf("http://127.0.0.1:%d/", MONITOR_PORT)
	resp, err := http.Get(url + "subscriptionsz")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}

	su := server.Subsz{}
	if err := json.Unmarshal(body, &su); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	// Do some sanity checks on values
	if su.NumSubs != 1 {
		t.Fatalf("Expected num_subs of 1, got %v\n", su.NumSubs)
	}
}

func TestHTTPHost(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	// Grab non-127.0.0.1 address and try to use that to connect.
	// Should fail.
	var ip net.IP
	ifaces, _ := net.Interfaces()
	for _, i := range ifaces {
		addrs, _ := i.Addrs()
		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			// Skip loopback/127.0.0.1 or any ipv6 for now.
			if ip.IsLoopback() || ip.To4() == nil {
				ip = nil
				continue
			}
			break
		}
		if ip != nil {
			break
		}
	}
	if ip == nil {
		t.Fatalf("Could not find non-loopback IPV4 address")
	}
	url := fmt.Sprintf("http://%v:%d/", ip, MONITOR_PORT)
	if resp, err := http.Get(url + "varz"); err == nil {
		t.Fatalf("Expected error: Got %+v\n", resp)
	}
}

// Create a connection to test ConnInfo
func createClientConnSubscribeAndPublish(t *testing.T) net.Conn {
	cl := createClientConn(t, "127.0.0.1", CLIENT_PORT)
	send, expect := setupConn(t, cl)
	expectMsgs := expectMsgsCommand(t, expect)

	send("SUB foo 1\r\nPUB foo 5\r\nhello\r\n")
	expectMsgs(1)

	return cl
}

func TestMonitorNoTLSConfig(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = CLIENT_PORT
	opts.HTTPHost = "127.0.0.1"
	opts.HTTPSPort = MONITOR_PORT
	s := server.New(&opts)
	defer s.Shutdown()
	// Check with manually starting the monitoring, which should return an error
	if err := s.StartMonitoring(); err == nil || !strings.Contains(err.Error(), "TLS") {
		t.Fatalf("Expected error about missing TLS config, got %v", err)
	}
	// Also check by calling Start(), which should produce a fatal error
	dl := &dummyLogger{}
	s.SetLogger(dl, false, false)
	defer s.SetLogger(nil, false, false)
	s.Start()
	if !strings.Contains(dl.msg, "TLS") {
		t.Fatalf("Expected error about missing TLS config, got %v", dl.msg)
	}
}

func TestMonitorErrorOnListen(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	opts := DefaultTestOptions
	opts.Port = CLIENT_PORT + 1
	opts.HTTPHost = "127.0.0.1"
	opts.HTTPPort = MONITOR_PORT
	s2 := server.New(&opts)
	defer s2.Shutdown()
	if err := s2.StartMonitoring(); err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("Expected error about not able to start listener, got %v", err)
	}
}

func TestMonitorBothPortsConfigured(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = CLIENT_PORT
	opts.HTTPHost = "127.0.0.1"
	opts.HTTPPort = MONITOR_PORT
	opts.HTTPSPort = MONITOR_PORT + 1
	s := server.New(&opts)
	defer s.Shutdown()
	if err := s.StartMonitoring(); err == nil || !strings.Contains(err.Error(), "specify both") {
		t.Fatalf("Expected error about ports configured, got %v", err)
	}
}

func TestMonitorStop(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultTestOptions
	opts.Port = CLIENT_PORT
	opts.HTTPHost = "127.0.0.1"
	opts.HTTPPort = MONITOR_PORT
	url := fmt.Sprintf("http://%v:%d/", opts.HTTPHost, MONITOR_PORT)
	// Create a server instance and start only the monitoring http server.
	s := server.New(&opts)
	if err := s.StartMonitoring(); err != nil {
		t.Fatalf("Error starting monitoring: %v", err)
	}
	// Make sure http server is started
	resp, err := http.Get(url + "varz")
	if err != nil {
		t.Fatalf("Error on http request: %v", err)
	}
	resp.Body.Close()
	// Although the server itself was not started (we did not call s.Start()),
	// Shutdown() should stop the http server.
	s.Shutdown()
	// HTTP request should now fail
	if resp, err := http.Get(url + "varz"); err == nil {
		t.Fatalf("Expected error: Got %+v\n", resp)
	}
}
