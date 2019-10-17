// Copyright 2012-2019 The NATS Authors
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
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func checkFor(t *testing.T, totalWait, sleepDur time.Duration, f func() error) {
	t.Helper()
	timeout := time.Now().Add(totalWait)
	var err error
	for time.Now().Before(timeout) {
		err = f()
		if err == nil {
			return
		}
		time.Sleep(sleepDur)
	}
	if err != nil {
		t.Fatal(err.Error())
	}
}

func DefaultOptions() *Options {
	return &Options{
		Host:     "127.0.0.1",
		Port:     -1,
		HTTPPort: -1,
		Cluster:  ClusterOpts{Port: -1},
		NoLog:    true,
		NoSigs:   true,
		Debug:    true,
		Trace:    true,
	}
}

// New Go Routine based server
func RunServer(opts *Options) *Server {
	if opts == nil {
		opts = DefaultOptions()
	}
	s, err := NewServer(opts)
	if err != nil || s == nil {
		panic(fmt.Sprintf("No NATS Server object returned: %v", err))
	}

	if !opts.NoLog {
		s.ConfigureLogger()
	}

	// Run server in Go routine.
	go s.Start()

	// Wait for accept loop(s) to be started
	if !s.ReadyForConnections(10 * time.Second) {
		panic("Unable to start NATS Server in Go Routine")
	}
	return s
}

// LoadConfig loads a configuration from a filename
func LoadConfig(configFile string) (opts *Options) {
	opts, err := ProcessConfigFile(configFile)
	if err != nil {
		panic(fmt.Sprintf("Error processing configuration file: %v", err))
	}
	opts.NoSigs, opts.NoLog = true, true
	return
}

// RunServerWithConfig starts a new Go routine based server with a configuration file.
func RunServerWithConfig(configFile string) (srv *Server, opts *Options) {
	opts = LoadConfig(configFile)
	srv = RunServer(opts)
	return
}

func TestVersionMatchesTag(t *testing.T) {
	tag := os.Getenv("TRAVIS_TAG")
	if tag == "" {
		t.SkipNow()
	}
	// We expect a tag of the form vX.Y.Z. If that's not the case,
	// we need someone to have a look. So fail if first letter is not
	// a `v`
	if tag[0] != 'v' {
		t.Fatalf("Expect tag to start with `v`, tag is: %s", tag)
	}
	// Strip the `v` from the tag for the version comparison.
	if VERSION != tag[1:] {
		t.Fatalf("Version (%s) does not match tag (%s)", VERSION, tag[1:])
	}
}

func TestStartProfiler(t *testing.T) {
	s := New(DefaultOptions())
	s.StartProfiler()
	s.mu.Lock()
	s.profiler.Close()
	s.mu.Unlock()
}

func TestStartupAndShutdown(t *testing.T) {

	opts := DefaultOptions()

	s := RunServer(opts)
	defer s.Shutdown()

	if !s.isRunning() {
		t.Fatal("Could not run server")
	}

	// Debug stuff.
	numRoutes := s.NumRoutes()
	if numRoutes != 0 {
		t.Fatalf("Expected numRoutes to be 0 vs %d\n", numRoutes)
	}

	numRemotes := s.NumRemotes()
	if numRemotes != 0 {
		t.Fatalf("Expected numRemotes to be 0 vs %d\n", numRemotes)
	}

	numClients := s.NumClients()
	if numClients != 0 && numClients != 1 {
		t.Fatalf("Expected numClients to be 1 or 0 vs %d\n", numClients)
	}

	numSubscriptions := s.NumSubscriptions()
	if numSubscriptions != 0 {
		t.Fatalf("Expected numSubscriptions to be 0 vs %d\n", numSubscriptions)
	}
}

func TestTlsCipher(t *testing.T) {
	if strings.Compare(tlsCipher(0x0005), "TLS_RSA_WITH_RC4_128_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0x000a), "TLS_RSA_WITH_3DES_EDE_CBC_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0x002f), "TLS_RSA_WITH_AES_128_CBC_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0x0035), "TLS_RSA_WITH_AES_256_CBC_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc007), "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc009), "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc00a), "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc011), "TLS_ECDHE_RSA_WITH_RC4_128_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc012), "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc013), "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc014), "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA") != 0 {
		t.Fatalf("IUnknownnvalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc02f), "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc02b), "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc030), "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc02c), "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if !strings.Contains(tlsCipher(0x9999), "Unknown") {
		t.Fatalf("Expected an unknown cipher.")
	}
}

func TestGetConnectURLs(t *testing.T) {
	opts := DefaultOptions()
	opts.Port = 4222

	var globalIP net.IP

	checkGlobalConnectURLs := func() {
		s := New(opts)
		defer s.Shutdown()

		s.mu.Lock()
		urls := s.getClientConnectURLs()
		s.mu.Unlock()
		if len(urls) == 0 {
			t.Fatalf("Expected to get a list of urls, got none for listen addr: %v", opts.Host)
		}
		for _, u := range urls {
			tcpaddr, err := net.ResolveTCPAddr("tcp", u)
			if err != nil {
				t.Fatalf("Error resolving: %v", err)
			}
			ip := tcpaddr.IP
			if !ip.IsGlobalUnicast() {
				t.Fatalf("IP %v is not global", ip.String())
			}
			if ip.IsUnspecified() {
				t.Fatalf("IP %v is unspecified", ip.String())
			}
			addr := strings.TrimSuffix(u, ":4222")
			if addr == opts.Host {
				t.Fatalf("Returned url is not right: %v", u)
			}
			if globalIP == nil {
				globalIP = ip
			}
		}
	}

	listenAddrs := []string{"0.0.0.0", "::"}
	for _, listenAddr := range listenAddrs {
		opts.Host = listenAddr
		checkGlobalConnectURLs()
	}

	checkConnectURLsHasOnlyOne := func() {
		s := New(opts)
		defer s.Shutdown()

		s.mu.Lock()
		urls := s.getClientConnectURLs()
		s.mu.Unlock()
		if len(urls) != 1 {
			t.Fatalf("Expected one URL, got %v", urls)
		}
		tcpaddr, err := net.ResolveTCPAddr("tcp", urls[0])
		if err != nil {
			t.Fatalf("Error resolving: %v", err)
		}
		ip := tcpaddr.IP
		if ip.String() != opts.Host {
			t.Fatalf("Expected connect URL to be %v, got %v", opts.Host, ip.String())
		}
	}

	singleConnectReturned := []string{"127.0.0.1", "::1"}
	if globalIP != nil {
		singleConnectReturned = append(singleConnectReturned, globalIP.String())
	}
	for _, listenAddr := range singleConnectReturned {
		opts.Host = listenAddr
		checkConnectURLsHasOnlyOne()
	}
}

func TestInfoServerNameDefaultsToPK(t *testing.T) {
	opts := DefaultOptions()
	opts.Port = 4222
	opts.ClientAdvertise = "nats.example.com"
	s := New(opts)
	defer s.Shutdown()

	if s.info.Name != s.info.ID {
		t.Fatalf("server info hostname is incorrect, got: '%v' expected: '%v'", s.info.Name, s.info.ID)
	}
}

func TestInfoServerNameIsSettable(t *testing.T) {
	opts := DefaultOptions()
	opts.Port = 4222
	opts.ClientAdvertise = "nats.example.com"
	opts.ServerName = "test_server_name"
	s := New(opts)
	defer s.Shutdown()

	if s.info.Name != "test_server_name" {
		t.Fatalf("server info hostname is incorrect, got: '%v' expected: 'test_server_name'", s.info.Name)
	}
}

func TestClientAdvertiseConnectURL(t *testing.T) {
	opts := DefaultOptions()
	opts.Port = 4222
	opts.ClientAdvertise = "nats.example.com"
	s := New(opts)
	defer s.Shutdown()

	s.mu.Lock()
	urls := s.getClientConnectURLs()
	s.mu.Unlock()
	if len(urls) != 1 {
		t.Fatalf("Expected to get one url, got none: %v with ClientAdvertise %v",
			opts.Host, opts.ClientAdvertise)
	}
	if urls[0] != "nats.example.com:4222" {
		t.Fatalf("Expected to get '%s', got: '%v'", "nats.example.com:4222", urls[0])
	}
	s.Shutdown()

	opts.ClientAdvertise = "nats.example.com:7777"
	s = New(opts)
	s.mu.Lock()
	urls = s.getClientConnectURLs()
	s.mu.Unlock()
	if len(urls) != 1 {
		t.Fatalf("Expected to get one url, got none: %v with ClientAdvertise %v",
			opts.Host, opts.ClientAdvertise)
	}
	if urls[0] != "nats.example.com:7777" {
		t.Fatalf("Expected 'nats.example.com:7777', got: '%v'", urls[0])
	}
	if s.info.Host != "nats.example.com" {
		t.Fatalf("Expected host to be set to nats.example.com")
	}
	if s.info.Port != 7777 {
		t.Fatalf("Expected port to be set to 7777")
	}
	s.Shutdown()

	opts = DefaultOptions()
	opts.Port = 0
	opts.ClientAdvertise = "nats.example.com:7777"
	s = New(opts)
	if s.info.Host != "nats.example.com" && s.info.Port != 7777 {
		t.Fatalf("Expected Client Advertise Host:Port to be nats.example.com:7777, got: %s:%d",
			s.info.Host, s.info.Port)
	}
	s.Shutdown()
}

func TestClientAdvertiseErrorOnStartup(t *testing.T) {
	opts := DefaultOptions()
	// Set invalid address
	opts.ClientAdvertise = "addr:::123"
	s := New(opts)
	defer s.Shutdown()
	dl := &DummyLogger{}
	s.SetLogger(dl, false, false)

	// Expect this to return due to failure
	s.Start()
	dl.Lock()
	msg := dl.msg
	dl.Unlock()
	if !strings.Contains(msg, "ClientAdvertise") {
		t.Fatalf("Unexpected error: %v", msg)
	}
}

func TestNoDeadlockOnStartFailure(t *testing.T) {
	opts := DefaultOptions()
	opts.Host = "x.x.x.x" // bad host
	opts.Port = 4222
	opts.HTTPHost = opts.Host
	opts.Cluster.Host = "127.0.0.1"
	opts.Cluster.Port = -1
	opts.ProfPort = -1
	s := New(opts)

	// This should return since it should fail to start a listener
	// on x.x.x.x:4222
	s.Start()

	// We should be able to shutdown
	s.Shutdown()
}

func TestMaxConnections(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxConn = 1
	s := RunServer(opts)
	defer s.Shutdown()

	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc.Close()

	nc2, err := nats.Connect(addr)
	if err == nil {
		nc2.Close()
		t.Fatal("Expected connection to fail")
	}
}

func TestMaxSubscriptions(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxSubs = 10
	s := RunServer(opts)
	defer s.Shutdown()

	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc.Close()

	for i := 0; i < 10; i++ {
		_, err := nc.Subscribe(fmt.Sprintf("foo.%d", i), func(*nats.Msg) {})
		if err != nil {
			t.Fatalf("Error subscribing: %v\n", err)
		}
	}
	// This should cause the error.
	nc.Subscribe("foo.22", func(*nats.Msg) {})
	nc.Flush()
	if err := nc.LastError(); err == nil {
		t.Fatal("Expected an error but got none\n")
	}
}

func TestProcessCommandLineArgs(t *testing.T) {
	var host string
	var port int
	cmd := flag.NewFlagSet("nats-server", flag.ExitOnError)
	cmd.StringVar(&host, "a", "0.0.0.0", "Host.")
	cmd.IntVar(&port, "p", 4222, "Port.")

	cmd.Parse([]string{"-a", "127.0.0.1", "-p", "9090"})
	showVersion, showHelp, err := ProcessCommandLineArgs(cmd)
	if err != nil {
		t.Errorf("Expected no errors, got: %s", err)
	}
	if showVersion || showHelp {
		t.Errorf("Expected not having to handle subcommands")
	}

	cmd.Parse([]string{"version"})
	showVersion, showHelp, err = ProcessCommandLineArgs(cmd)
	if err != nil {
		t.Errorf("Expected no errors, got: %s", err)
	}
	if !showVersion {
		t.Errorf("Expected having to handle version command")
	}
	if showHelp {
		t.Errorf("Expected not having to handle help command")
	}

	cmd.Parse([]string{"help"})
	showVersion, showHelp, err = ProcessCommandLineArgs(cmd)
	if err != nil {
		t.Errorf("Expected no errors, got: %s", err)
	}
	if showVersion {
		t.Errorf("Expected not having to handle version command")
	}
	if !showHelp {
		t.Errorf("Expected having to handle help command")
	}

	cmd.Parse([]string{"foo", "-p", "9090"})
	_, _, err = ProcessCommandLineArgs(cmd)
	if err == nil {
		t.Errorf("Expected an error handling the command arguments")
	}
}

func TestWriteDeadline(t *testing.T) {
	opts := DefaultOptions()
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
	for i := 0; i < 10; i++ {
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

func TestRandomPorts(t *testing.T) {
	opts := DefaultOptions()
	opts.HTTPPort = -1
	opts.Port = -1
	s := RunServer(opts)

	defer s.Shutdown()

	if s.Addr() == nil || s.Addr().(*net.TCPAddr).Port <= 0 {
		t.Fatal("Should have dynamically assigned server port.")
	}

	if s.Addr() == nil || s.Addr().(*net.TCPAddr).Port == 4222 {
		t.Fatal("Should not have dynamically assigned default port: 4222.")
	}

	if s.MonitorAddr() == nil || s.MonitorAddr().Port <= 0 {
		t.Fatal("Should have dynamically assigned monitoring port.")
	}

}

func TestNilMonitoringPort(t *testing.T) {
	opts := DefaultOptions()
	opts.HTTPPort = 0
	opts.HTTPSPort = 0
	s := RunServer(opts)

	defer s.Shutdown()

	if s.MonitorAddr() != nil {
		t.Fatal("HttpAddr should be nil.")
	}
}

type DummyAuth struct{}

func (d *DummyAuth) Check(c ClientAuthentication) bool {
	return c.GetOpts().Username == "valid"
}

func TestCustomClientAuthentication(t *testing.T) {
	var clientAuth DummyAuth

	opts := DefaultOptions()
	opts.CustomClientAuthentication = &clientAuth

	s := RunServer(opts)

	defer s.Shutdown()

	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	nc, err := nats.Connect(addr, nats.UserInfo("valid", ""))
	if err != nil {
		t.Fatalf("Expected client to connect, got: %s", err)
	}
	nc.Close()
	if _, err := nats.Connect(addr, nats.UserInfo("invalid", "")); err == nil {
		t.Fatal("Expected client to fail to connect")
	}
}

func TestCustomRouterAuthentication(t *testing.T) {
	opts := DefaultOptions()
	opts.CustomRouterAuthentication = &DummyAuth{}
	opts.Cluster.Host = "127.0.0.1"
	s := RunServer(opts)
	defer s.Shutdown()
	clusterPort := s.ClusterAddr().Port

	opts2 := DefaultOptions()
	opts2.Cluster.Host = "127.0.0.1"
	opts2.Routes = RoutesFromStr(fmt.Sprintf("nats://invalid@127.0.0.1:%d", clusterPort))
	s2 := RunServer(opts2)
	defer s2.Shutdown()

	// s2 will attempt to connect to s, which should reject.
	// Keep in mind that s2 will try again...
	time.Sleep(50 * time.Millisecond)
	checkNumRoutes(t, s2, 0)

	opts3 := DefaultOptions()
	opts3.Cluster.Host = "127.0.0.1"
	opts3.Routes = RoutesFromStr(fmt.Sprintf("nats://valid@127.0.0.1:%d", clusterPort))
	s3 := RunServer(opts3)
	defer s3.Shutdown()
	checkClusterFormed(t, s, s3)
	checkNumRoutes(t, s3, 1)
}

func TestMonitoringNoTimeout(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	s.mu.Lock()
	srv := s.monitoringServer
	s.mu.Unlock()

	if srv == nil {
		t.Fatalf("Monitoring server not set")
	}
	if srv.ReadTimeout != 0 {
		t.Fatalf("ReadTimeout should not be set, was set to %v", srv.ReadTimeout)
	}
	if srv.WriteTimeout != 0 {
		t.Fatalf("WriteTimeout should not be set, was set to %v", srv.WriteTimeout)
	}
}

func TestProfilingNoTimeout(t *testing.T) {
	opts := DefaultOptions()
	opts.ProfPort = -1
	s := RunServer(opts)
	defer s.Shutdown()

	paddr := s.ProfilerAddr()
	if paddr == nil {
		t.Fatalf("Profiler not started")
	}
	pport := paddr.Port
	if pport <= 0 {
		t.Fatalf("Expected profiler port to be set, got %v", pport)
	}
	s.mu.Lock()
	srv := s.profilingServer
	s.mu.Unlock()

	if srv == nil {
		t.Fatalf("Profiling server not set")
	}
	if srv.ReadTimeout != 0 {
		t.Fatalf("ReadTimeout should not be set, was set to %v", srv.ReadTimeout)
	}
	if srv.WriteTimeout != 0 {
		t.Fatalf("WriteTimeout should not be set, was set to %v", srv.WriteTimeout)
	}
}

func TestLameDuckMode(t *testing.T) {
	atomic.StoreInt64(&lameDuckModeInitialDelay, 0)
	defer atomic.StoreInt64(&lameDuckModeInitialDelay, lameDuckModeDefaultInitialDelay)

	optsA := DefaultOptions()
	optsA.Cluster.Host = "127.0.0.1"
	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	// Check that if there is no client, server is shutdown
	srvA.lameDuckMode()
	srvA.mu.Lock()
	shutdown := srvA.shutdown
	srvA.mu.Unlock()
	if !shutdown {
		t.Fatalf("Server should have shutdown")
	}

	optsA.LameDuckDuration = 10 * time.Nanosecond
	srvA = RunServer(optsA)
	defer srvA.Shutdown()

	optsB := DefaultOptions()
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srvA.ClusterAddr().Port))
	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	total := 50
	connectClients := func() []*nats.Conn {
		ncs := make([]*nats.Conn, 0, total)
		for i := 0; i < total; i++ {
			nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port),
				nats.ReconnectWait(50*time.Millisecond))
			if err != nil {
				t.Fatalf("Error on connect: %v", err)
			}
			ncs = append(ncs, nc)
		}
		return ncs
	}
	stopClientsAndSrvB := func(ncs []*nats.Conn) {
		for _, nc := range ncs {
			nc.Close()
		}
		srvB.Shutdown()
	}

	ncs := connectClients()

	checkClientsCount(t, srvA, total)
	checkClientsCount(t, srvB, 0)

	start := time.Now()
	srvA.lameDuckMode()
	// Make sure that nothing bad happens if called twice
	srvA.lameDuckMode()
	// Wait that shutdown completes
	elapsed := time.Since(start)
	// It should have taken more than the allotted time of 10ms since we had 50 clients.
	if elapsed <= optsA.LameDuckDuration {
		t.Fatalf("Expected to take more than %v, got %v", optsA.LameDuckDuration, elapsed)
	}

	checkClientsCount(t, srvA, 0)
	checkClientsCount(t, srvB, total)

	// Check closed status on server A
	cz := pollConz(t, srvA, 1, "", &ConnzOptions{State: ConnClosed})
	if n := len(cz.Conns); n != total {
		t.Fatalf("Expected %v closed connections, got %v", total, n)
	}
	for _, c := range cz.Conns {
		checkReason(t, c.Reason, ServerShutdown)
	}

	stopClientsAndSrvB(ncs)

	optsA.LameDuckDuration = time.Second
	srvA = RunServer(optsA)
	defer srvA.Shutdown()

	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srvA.ClusterAddr().Port))
	srvB = RunServer(optsB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	ncs = connectClients()

	checkClientsCount(t, srvA, total)
	checkClientsCount(t, srvB, 0)

	start = time.Now()
	go srvA.lameDuckMode()
	// Check that while in lameDuckMode, it is not possible to connect
	// to the server. Wait to be in LD mode first
	checkFor(t, 500*time.Millisecond, 15*time.Millisecond, func() error {
		srvA.mu.Lock()
		ldm := srvA.ldm
		srvA.mu.Unlock()
		if !ldm {
			return fmt.Errorf("Did not reach lame duck mode")
		}
		return nil
	})
	if _, err := nats.Connect(fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port)); err != nats.ErrNoServers {
		t.Fatalf("Expected %v, got %v", nats.ErrNoServers, err)
	}
	srvA.grWG.Wait()
	elapsed = time.Since(start)

	checkClientsCount(t, srvA, 0)
	checkClientsCount(t, srvB, total)

	if elapsed > optsA.LameDuckDuration {
		t.Fatalf("Expected to not take more than %v, got %v", optsA.LameDuckDuration, elapsed)
	}

	stopClientsAndSrvB(ncs)

	// Now check that we can shutdown server while in LD mode.
	optsA.LameDuckDuration = 60 * time.Second
	srvA = RunServer(optsA)
	defer srvA.Shutdown()

	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srvA.ClusterAddr().Port))
	srvB = RunServer(optsB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	ncs = connectClients()

	checkClientsCount(t, srvA, total)
	checkClientsCount(t, srvB, 0)

	start = time.Now()
	go srvA.lameDuckMode()
	time.Sleep(100 * time.Millisecond)
	srvA.Shutdown()
	elapsed = time.Since(start)
	// Make sure that it did not take that long
	if elapsed > time.Second {
		t.Fatalf("Took too long: %v", elapsed)
	}
	checkClientsCount(t, srvA, 0)
	checkClientsCount(t, srvB, total)

	stopClientsAndSrvB(ncs)

	// Now test that we introduce delay before starting closing client connections.
	// This allow to "signal" multiple servers and avoid their clients to reconnect
	// to a server that is going to be going in LD mode.
	atomic.StoreInt64(&lameDuckModeInitialDelay, int64(100*time.Millisecond))

	optsA.LameDuckDuration = 10 * time.Millisecond
	srvA = RunServer(optsA)
	defer srvA.Shutdown()

	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srvA.ClusterAddr().Port))
	optsB.LameDuckDuration = 10 * time.Millisecond
	srvB = RunServer(optsB)
	defer srvB.Shutdown()

	optsC := DefaultOptions()
	optsC.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srvA.ClusterAddr().Port))
	optsC.LameDuckDuration = 10 * time.Millisecond
	srvC := RunServer(optsC)
	defer srvC.Shutdown()

	checkClusterFormed(t, srvA, srvB, srvC)

	rt := int32(0)
	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", optsA.Port),
		nats.ReconnectWait(15*time.Millisecond),
		nats.ReconnectHandler(func(*nats.Conn) {
			atomic.AddInt32(&rt, 1)
		}))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	go srvA.lameDuckMode()
	// Wait a bit, but less than lameDuckModeInitialDelay that we set in this
	// test to 100ms.
	time.Sleep(30 * time.Millisecond)
	go srvB.lameDuckMode()

	srvA.grWG.Wait()
	srvB.grWG.Wait()
	checkClientsCount(t, srvC, 1)
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if n := atomic.LoadInt32(&rt); n != 1 {
			return fmt.Errorf("Expected client to reconnect only once, got %v", n)
		}
		return nil
	})
}

func TestServerValidateGatewaysOptions(t *testing.T) {
	baseOpt := testDefaultOptionsForGateway("A")
	u, _ := url.Parse("host:5222")
	g := &RemoteGatewayOpts{
		URLs: []*url.URL{u},
	}
	baseOpt.Gateway.Gateways = append(baseOpt.Gateway.Gateways, g)

	for _, test := range []struct {
		name        string
		opts        func() *Options
		expectedErr string
	}{
		{
			name: "gateway_has_no_name",
			opts: func() *Options {
				o := baseOpt.Clone()
				o.Gateway.Name = ""
				return o
			},
			expectedErr: "has no name",
		},
		{
			name: "gateway_has_no_port",
			opts: func() *Options {
				o := baseOpt.Clone()
				o.Gateway.Port = 0
				return o
			},
			expectedErr: "no port specified",
		},
		{
			name: "gateway_dst_has_no_name",
			opts: func() *Options {
				o := baseOpt.Clone()
				return o
			},
			expectedErr: "has no name",
		},
		{
			name: "gateway_dst_urls_is_nil",
			opts: func() *Options {
				o := baseOpt.Clone()
				o.Gateway.Gateways[0].Name = "B"
				o.Gateway.Gateways[0].URLs = nil
				return o
			},
			expectedErr: "has no URL",
		},
		{
			name: "gateway_dst_urls_is_empty",
			opts: func() *Options {
				o := baseOpt.Clone()
				o.Gateway.Gateways[0].Name = "B"
				o.Gateway.Gateways[0].URLs = []*url.URL{}
				return o
			},
			expectedErr: "has no URL",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateOptions(test.opts()); err == nil || !strings.Contains(err.Error(), test.expectedErr) {
				t.Fatalf("Expected error about %q, got %v", test.expectedErr, err)
			}
		})
	}
}

func TestAcceptError(t *testing.T) {
	o := DefaultOptions()
	s := New(o)
	s.mu.Lock()
	s.running = true
	s.mu.Unlock()
	defer s.Shutdown()
	orgDelay := time.Hour
	delay := s.acceptError("Test", fmt.Errorf("any error"), orgDelay)
	if delay != orgDelay {
		t.Fatalf("With this type of error, delay should have stayed same, got %v", delay)
	}

	// Create any net.Error and make it a temporary
	ne := &net.DNSError{IsTemporary: true}
	orgDelay = 10 * time.Millisecond
	delay = s.acceptError("Test", ne, orgDelay)
	if delay != 2*orgDelay {
		t.Fatalf("Expected delay to double, got %v", delay)
	}
	// Now check the max
	orgDelay = 60 * ACCEPT_MAX_SLEEP / 100
	delay = s.acceptError("Test", ne, orgDelay)
	if delay != ACCEPT_MAX_SLEEP {
		t.Fatalf("Expected delay to double, got %v", delay)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	start := time.Now()
	go func() {
		s.acceptError("Test", ne, orgDelay)
		wg.Done()
	}()
	time.Sleep(100 * time.Millisecond)
	// This should kick out the sleep in acceptError
	s.Shutdown()
	if dur := time.Since(start); dur >= ACCEPT_MAX_SLEEP {
		t.Fatalf("Shutdown took too long: %v", dur)
	}
}

type myDummyDNSResolver struct {
	ips []string
	err error
}

func (r *myDummyDNSResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.ips, nil
}

func TestGetRandomIP(t *testing.T) {
	s := &Server{}
	resolver := &myDummyDNSResolver{}
	// no port...
	if _, err := s.getRandomIP(resolver, "noport"); err == nil || !strings.Contains(err.Error(), "port") {
		t.Fatalf("Expected error about port missing, got %v", err)
	}
	resolver.err = fmt.Errorf("on purpose")
	if _, err := s.getRandomIP(resolver, "localhost:4222"); err == nil || !strings.Contains(err.Error(), "on purpose") {
		t.Fatalf("Expected error about no port, got %v", err)
	}
	resolver.err = nil
	a, err := s.getRandomIP(resolver, "localhost:4222")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if a != "localhost:4222" {
		t.Fatalf("Expected address to be %q, got %q", "localhost:4222", a)
	}
	resolver.ips = []string{"1.2.3.4"}
	a, err = s.getRandomIP(resolver, "localhost:4222")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if a != "1.2.3.4:4222" {
		t.Fatalf("Expected address to be %q, got %q", "1.2.3.4:4222", a)
	}
	// Check for randomness
	resolver.ips = []string{"1.2.3.4", "2.2.3.4", "3.2.3.4"}
	dist := [3]int{}
	for i := 0; i < 100; i++ {
		ip, err := s.getRandomIP(resolver, "localhost:4222")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		v := int(ip[0]-'0') - 1
		dist[v]++
	}
	low := 20
	high := 47
	for i, d := range dist {
		if d == 0 || d == 100 {
			t.Fatalf("Unexpected distribution for ip %v, got %v", i, d)
		} else if d < low || d > high {
			t.Logf("Warning: out of expected range [%v,%v] for ip %v, got %v", low, high, i, d)
		}
	}
}

type slowWriteConn struct {
	net.Conn
}

func (swc *slowWriteConn) Write(b []byte) (int, error) {
	// Limit the write to 10 bytes at a time.
	max := len(b)
	if max > 10 {
		max = 10
	}
	return swc.Conn.Write(b[:max])
}

func TestClientWriteLoopStall(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	errCh := make(chan error, 1)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(url,
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) {
			select {
			case errCh <- e:
			default:
			}
		}))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	cid, _ := nc.GetClientID()

	sender, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sender.Close()

	c := s.getClient(cid)
	c.mu.Lock()
	c.nc = &slowWriteConn{Conn: c.nc}
	c.mu.Unlock()

	sender.Publish("foo", make([]byte, 100))

	if _, err := sub.NextMsg(3 * time.Second); err != nil {
		t.Fatalf("WriteLoop has stalled!")
	}

	// Make sure that we did not get any async error
	select {
	case e := <-errCh:
		t.Fatalf("Got error: %v", e)
	case <-time.After(250 * time.Millisecond):
	}
}

func TestInsecureSkipVerifyWarning(t *testing.T) {
	checkWarnReported := func(t *testing.T, o *Options, expectedWarn string) {
		t.Helper()
		s, err := NewServer(o)
		if err != nil {
			t.Fatalf("Error on new server: %v", err)
		}
		l := &captureWarnLogger{warn: make(chan string, 1)}
		s.SetLogger(l, false, false)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			s.Start()
			wg.Done()
		}()
		select {
		case w := <-l.warn:
			if !strings.Contains(w, expectedWarn) {
				t.Fatalf("Expected warning %q, got %q", expectedWarn, w)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Did not get warning %q", expectedWarn)
		}
		s.Shutdown()
		wg.Wait()
	}

	tc := &TLSConfigOpts{}
	tc.CertFile = "../test/configs/certs/server-cert.pem"
	tc.KeyFile = "../test/configs/certs/server-key.pem"
	tc.CaFile = "../test/configs/certs/ca.pem"
	tc.Insecure = true
	config, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating tls config: %v", err)
	}

	o := DefaultOptions()
	o.Cluster.Port = -1
	o.Cluster.TLSConfig = config.Clone()
	checkWarnReported(t, o, clusterTLSInsecureWarning)

	// Remove the route setting
	o.Cluster.Port = 0
	o.Cluster.TLSConfig = nil

	// Configure LeafNode with no TLS in the main block first, but only with remotes.
	o.LeafNode.Port = -1
	rurl, _ := url.Parse("nats://127.0.0.1:1234")
	o.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs:      []*url.URL{rurl},
			TLSConfig: config.Clone(),
		},
	}
	checkWarnReported(t, o, leafnodeTLSInsecureWarning)

	// Now add to main block.
	o.LeafNode.TLSConfig = config.Clone()
	checkWarnReported(t, o, leafnodeTLSInsecureWarning)

	// Now remove remote and check warning still reported
	o.LeafNode.Remotes = nil
	checkWarnReported(t, o, leafnodeTLSInsecureWarning)

	// Remove the LN setting
	o.LeafNode.Port = 0
	o.LeafNode.TLSConfig = nil

	// Configure GW with no TLS in main block first, but only with remotes
	o.Gateway.Name = "A"
	o.Gateway.Host = "127.0.0.1"
	o.Gateway.Port = -1
	o.Gateway.Gateways = []*RemoteGatewayOpts{
		{
			Name:      "B",
			URLs:      []*url.URL{rurl},
			TLSConfig: config.Clone(),
		},
	}
	checkWarnReported(t, o, gatewayTLSInsecureWarning)

	// Now add to main block.
	o.Gateway.TLSConfig = config.Clone()
	checkWarnReported(t, o, gatewayTLSInsecureWarning)

	// Now remove remote and check warning still reported
	o.Gateway.Gateways = nil
	checkWarnReported(t, o, gatewayTLSInsecureWarning)
}

func TestConnectErrorReports(t *testing.T) {
	// Check that default report attempts is as expected
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	if ra := s.getOpts().ConnectErrorReports; ra != DEFAULT_CONNECT_ERROR_REPORTS {
		t.Fatalf("Expected default value to be %v, got %v", DEFAULT_CONNECT_ERROR_REPORTS, ra)
	}

	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("Error creating temp file: %v", err)
	}
	log := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(log)

	remoteURLs := RoutesFromStr("nats://127.0.0.1:1234")

	opts = DefaultOptions()
	opts.ConnectErrorReports = 3
	opts.Cluster.Port = -1
	opts.Routes = remoteURLs
	opts.NoLog = false
	opts.LogFile = log
	opts.Logtime = true
	opts.Debug = true
	s = RunServer(opts)
	defer s.Shutdown()

	// Wait long enough for the number of recurring attempts to happen
	time.Sleep(10 * routeConnectDelay)
	s.Shutdown()

	content, err := ioutil.ReadFile(log)
	if err != nil {
		t.Fatalf("Error reading log file: %v", err)
	}

	checkContent := func(t *testing.T, txt string, attempt int, shouldBeThere bool) {
		t.Helper()
		present := bytes.Contains(content, []byte(fmt.Sprintf("%s (attempt %d)", txt, attempt)))
		if shouldBeThere && !present {
			t.Fatalf("Did not find expected log statement (%s) for attempt %d: %s", txt, attempt, content)
		} else if !shouldBeThere && present {
			t.Fatalf("Log statement (%s) for attempt %d should not be present: %s", txt, attempt, content)
		}
	}

	type testConnect struct {
		name        string
		attempt     int
		errExpected bool
	}
	for _, test := range []testConnect{
		{"route_attempt_1", 1, true},
		{"route_attempt_2", 2, false},
		{"route_attempt_3", 3, true},
		{"route_attempt_4", 4, false},
		{"route_attempt_6", 6, true},
		{"route_attempt_7", 7, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			debugExpected := !test.errExpected
			checkContent(t, "[DBG] Error trying to connect to route", test.attempt, debugExpected)
			checkContent(t, "[ERR] Error trying to connect to route", test.attempt, test.errExpected)
		})
	}

	os.Remove(log)

	// Now try with leaf nodes
	opts.Cluster.Port = 0
	opts.Routes = nil
	opts.LeafNode.Remotes = []*RemoteLeafOpts{&RemoteLeafOpts{URLs: []*url.URL{remoteURLs[0]}}}
	opts.LeafNode.ReconnectInterval = 15 * time.Millisecond
	s = RunServer(opts)
	defer s.Shutdown()

	// Wait long enough for the number of recurring attempts to happen
	time.Sleep(10 * opts.LeafNode.ReconnectInterval)
	s.Shutdown()

	content, err = ioutil.ReadFile(log)
	if err != nil {
		t.Fatalf("Error reading log file: %v", err)
	}

	checkLeafContent := func(t *testing.T, txt, host string, attempt int, shouldBeThere bool) {
		t.Helper()
		present := bytes.Contains(content, []byte(fmt.Sprintf("%s %q (attempt %d)", txt, host, attempt)))
		if shouldBeThere && !present {
			t.Fatalf("Did not find expected log statement (%s %q) for attempt %d: %s", txt, host, attempt, content)
		} else if !shouldBeThere && present {
			t.Fatalf("Log statement (%s %q) for attempt %d should not be present: %s", txt, host, attempt, content)
		}
	}

	for _, test := range []testConnect{
		{"leafnode_attempt_1", 1, true},
		{"leafnode_attempt_2", 2, false},
		{"leafnode_attempt_3", 3, true},
		{"leafnode_attempt_4", 4, false},
		{"leafnode_attempt_6", 6, true},
		{"leafnode_attempt_7", 7, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			debugExpected := !test.errExpected
			checkLeafContent(t, "[DBG] Error trying to connect as leafnode to remote server", remoteURLs[0].Host, test.attempt, debugExpected)
			checkLeafContent(t, "[ERR] Error trying to connect as leafnode to remote server", remoteURLs[0].Host, test.attempt, test.errExpected)
		})
	}

	os.Remove(log)

	// Now try with gateways
	opts.LeafNode.Remotes = nil
	opts.Gateway.Name = "A"
	opts.Gateway.Port = -1
	opts.Gateway.Gateways = []*RemoteGatewayOpts{
		&RemoteGatewayOpts{
			Name: "B",
			URLs: remoteURLs,
		},
	}
	opts.gatewaysSolicitDelay = 15 * time.Millisecond
	s = RunServer(opts)
	defer s.Shutdown()

	// Wait long enough for the number of recurring attempts to happen
	time.Sleep(10 * gatewayConnectDelay)
	s.Shutdown()

	content, err = ioutil.ReadFile(log)
	if err != nil {
		t.Fatalf("Error reading log file: %v", err)
	}

	for _, test := range []testConnect{
		{"gateway_attempt_1", 1, true},
		{"gateway_attempt_2", 2, false},
		{"gateway_attempt_3", 3, true},
		{"gateway_attempt_4", 4, false},
		{"gateway_attempt_6", 6, true},
		{"gateway_attempt_7", 7, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			debugExpected := !test.errExpected
			infoExpected := test.errExpected
			// For gateways, we also check our notice that we attempt to connect
			checkContent(t, "[DBG] Connecting to explicit gateway \"B\" (127.0.0.1:1234) at 127.0.0.1:1234", test.attempt, debugExpected)
			checkContent(t, "[INF] Connecting to explicit gateway \"B\" (127.0.0.1:1234) at 127.0.0.1:1234", test.attempt, infoExpected)
			checkContent(t, "[DBG] Error connecting to explicit gateway \"B\" (127.0.0.1:1234) at 127.0.0.1:1234", test.attempt, debugExpected)
			checkContent(t, "[ERR] Error connecting to explicit gateway \"B\" (127.0.0.1:1234) at 127.0.0.1:1234", test.attempt, test.errExpected)
		})
	}
}

func TestReconnectErrorReports(t *testing.T) {
	// Check that default report attempts is as expected
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	if ra := s.getOpts().ReconnectErrorReports; ra != DEFAULT_RECONNECT_ERROR_REPORTS {
		t.Fatalf("Expected default value to be %v, got %v", DEFAULT_RECONNECT_ERROR_REPORTS, ra)
	}

	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("Error creating temp file: %v", err)
	}
	log := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(log)

	csOpts := DefaultOptions()
	csOpts.Cluster.Port = -1
	cs := RunServer(csOpts)
	defer cs.Shutdown()

	opts = DefaultOptions()
	opts.ReconnectErrorReports = 3
	opts.Cluster.Port = -1
	opts.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", cs.ClusterAddr().Port))
	opts.NoLog = false
	opts.LogFile = log
	opts.Logtime = true
	opts.Debug = true
	s = RunServer(opts)
	defer s.Shutdown()

	// Wait for cluster to be formed
	checkClusterFormed(t, s, cs)

	// Now shutdown the server s connected to.
	cs.Shutdown()

	// Wait long enough for the number of recurring attempts to happen
	time.Sleep(DEFAULT_ROUTE_RECONNECT + 15*routeConnectDelay)
	s.Shutdown()

	content, err := ioutil.ReadFile(log)
	if err != nil {
		t.Fatalf("Error reading log file: %v", err)
	}

	checkContent := func(t *testing.T, txt string, attempt int, shouldBeThere bool) {
		t.Helper()
		present := bytes.Contains(content, []byte(fmt.Sprintf("%s (attempt %d)", txt, attempt)))
		if shouldBeThere && !present {
			t.Fatalf("Did not find expected log statement (%s) for attempt %d: %s", txt, attempt, content)
		} else if !shouldBeThere && present {
			t.Fatalf("Log statement (%s) for attempt %d should not be present: %s", txt, attempt, content)
		}
	}

	type testConnect struct {
		name        string
		attempt     int
		errExpected bool
	}
	for _, test := range []testConnect{
		{"route_attempt_1", 1, true},
		{"route_attempt_2", 2, false},
		{"route_attempt_3", 3, true},
		{"route_attempt_4", 4, false},
		{"route_attempt_6", 6, true},
		{"route_attempt_7", 7, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			debugExpected := !test.errExpected
			checkContent(t, "[DBG] Error trying to connect to route", test.attempt, debugExpected)
			checkContent(t, "[ERR] Error trying to connect to route", test.attempt, test.errExpected)
		})
	}

	os.Remove(log)

	// Now try with leaf nodes
	csOpts.Cluster.Port = 0
	csOpts.LeafNode.Host = "127.0.0.1"
	csOpts.LeafNode.Port = -1

	cs = RunServer(csOpts)
	defer cs.Shutdown()

	opts.Cluster.Port = 0
	opts.Routes = nil
	u, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", csOpts.LeafNode.Port))
	opts.LeafNode.Remotes = []*RemoteLeafOpts{&RemoteLeafOpts{URLs: []*url.URL{u}}}
	opts.LeafNode.ReconnectInterval = 15 * time.Millisecond
	s = RunServer(opts)
	defer s.Shutdown()

	checkFor(t, 3*time.Second, 10*time.Millisecond, func() error {
		if nln := s.NumLeafNodes(); nln != 1 {
			return fmt.Errorf("Number of leaf nodes is %d", nln)
		}
		return nil
	})

	// Now shutdown the server s is connected to
	cs.Shutdown()

	// Wait long enough for the number of recurring attempts to happen
	time.Sleep(opts.LeafNode.ReconnectInterval + 15*opts.LeafNode.ReconnectInterval)
	s.Shutdown()

	content, err = ioutil.ReadFile(log)
	if err != nil {
		t.Fatalf("Error reading log file: %v", err)
	}

	checkLeafContent := func(t *testing.T, txt, host string, attempt int, shouldBeThere bool) {
		t.Helper()
		present := bytes.Contains(content, []byte(fmt.Sprintf("%s %q (attempt %d)", txt, host, attempt)))
		if shouldBeThere && !present {
			t.Fatalf("Did not find expected log statement (%s %q) for attempt %d: %s", txt, host, attempt, content)
		} else if !shouldBeThere && present {
			t.Fatalf("Log statement (%s %q) for attempt %d should not be present: %s", txt, host, attempt, content)
		}
	}

	for _, test := range []testConnect{
		{"leafnode_attempt_1", 1, true},
		{"leafnode_attempt_2", 2, false},
		{"leafnode_attempt_3", 3, true},
		{"leafnode_attempt_4", 4, false},
		{"leafnode_attempt_6", 6, true},
		{"leafnode_attempt_7", 7, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			debugExpected := !test.errExpected
			checkLeafContent(t, "[DBG] Error trying to connect as leafnode to remote server", u.Host, test.attempt, debugExpected)
			checkLeafContent(t, "[ERR] Error trying to connect as leafnode to remote server", u.Host, test.attempt, test.errExpected)
		})
	}

	os.Remove(log)

	// Now try with gateways
	csOpts.LeafNode.Port = 0
	csOpts.Gateway.Name = "B"
	csOpts.Gateway.Port = -1
	cs = RunServer(csOpts)

	opts.LeafNode.Remotes = nil
	opts.Gateway.Name = "A"
	opts.Gateway.Port = -1
	remoteGWPort := cs.GatewayAddr().Port
	u, _ = url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", remoteGWPort))
	opts.Gateway.Gateways = []*RemoteGatewayOpts{
		&RemoteGatewayOpts{
			Name: "B",
			URLs: []*url.URL{u},
		},
	}
	opts.gatewaysSolicitDelay = 15 * time.Millisecond
	s = RunServer(opts)
	defer s.Shutdown()

	waitForOutboundGateways(t, s, 1, 2*time.Second)
	waitForInboundGateways(t, s, 1, 2*time.Second)

	// Now stop server s is connecting to
	cs.Shutdown()

	// Wait long enough for the number of recurring attempts to happen
	time.Sleep(2*gatewayReconnectDelay + 15*gatewayConnectDelay)
	s.Shutdown()

	content, err = ioutil.ReadFile(log)
	if err != nil {
		t.Fatalf("Error reading log file: %v", err)
	}

	connTxt := fmt.Sprintf("Connecting to explicit gateway \"B\" (127.0.0.1:%d) at 127.0.0.1:%d", remoteGWPort, remoteGWPort)
	dbgConnTxt := fmt.Sprintf("[DBG] %s", connTxt)
	infConnTxt := fmt.Sprintf("[INF] %s", connTxt)

	errTxt := fmt.Sprintf("Error connecting to explicit gateway \"B\" (127.0.0.1:%d) at 127.0.0.1:%d", remoteGWPort, remoteGWPort)
	dbgErrTxt := fmt.Sprintf("[DBG] %s", errTxt)
	errErrTxt := fmt.Sprintf("[ERR] %s", errTxt)

	for _, test := range []testConnect{
		{"gateway_attempt_1", 1, true},
		{"gateway_attempt_2", 2, false},
		{"gateway_attempt_3", 3, true},
		{"gateway_attempt_4", 4, false},
		{"gateway_attempt_6", 6, true},
		{"gateway_attempt_7", 7, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			debugExpected := !test.errExpected
			infoExpected := test.errExpected
			// For gateways, we also check our notice that we attempt to connect
			checkContent(t, dbgConnTxt, test.attempt, debugExpected)
			checkContent(t, infConnTxt, test.attempt, infoExpected)
			checkContent(t, dbgErrTxt, test.attempt, debugExpected)
			checkContent(t, errErrTxt, test.attempt, test.errExpected)
		})
	}
}
