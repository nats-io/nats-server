// Copyright 2012-2020 The NATS Authors
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
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"sort"
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
		Cluster:  ClusterOpts{Port: -1, Name: "abc"},
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
	// Travis started to return '' when no tag is set. Support both now.
	if tag == "" || tag == "''" {
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
	opts.NoSystemAccount = true

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

func TestTLSVersions(t *testing.T) {
	for _, test := range []struct {
		name     string
		value    uint16
		expected string
	}{
		{"1.0", tls.VersionTLS10, "1.0"},
		{"1.1", tls.VersionTLS11, "1.1"},
		{"1.2", tls.VersionTLS12, "1.2"},
		{"1.3", tls.VersionTLS13, "1.3"},
		{"unknown", 0x999, "Unknown [0x999]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if v := tlsVersion(test.value); v != test.expected {
				t.Fatalf("Expected value 0x%x to be %q, got %q", test.value, test.expected, v)
			}
		})
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
	if strings.Compare(tlsCipher(0x1301), "TLS_AES_128_GCM_SHA256") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0x1302), "TLS_AES_256_GCM_SHA384") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0x1303), "TLS_CHACHA20_POLY1305_SHA256") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0x9999), "Unknown [0x9999]") != 0 {
		t.Fatalf("Expected an unknown cipher")
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

func TestClientAdvertiseInCluster(t *testing.T) {
	optsA := DefaultOptions()
	optsA.ClientAdvertise = "srvA:4222"
	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	nc := natsConnect(t, srvA.ClientURL())
	defer nc.Close()

	optsB := DefaultOptions()
	optsB.ClientAdvertise = "srvBC:4222"
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", optsA.Cluster.Port))
	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	checkURLs := func(expected string) {
		t.Helper()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			srvs := nc.DiscoveredServers()
			for _, u := range srvs {
				if u == expected {
					return nil
				}
			}
			return fmt.Errorf("Url %q not found in %q", expected, srvs)
		})
	}
	checkURLs("nats://srvBC:4222")

	optsC := DefaultOptions()
	optsC.ClientAdvertise = "srvBC:4222"
	optsC.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", optsA.Cluster.Port))
	srvC := RunServer(optsC)
	defer srvC.Shutdown()

	checkClusterFormed(t, srvA, srvB, srvC)
	checkURLs("nats://srvBC:4222")

	srvB.Shutdown()
	checkNumRoutes(t, srvA, 1)
	checkURLs("nats://srvBC:4222")
}

func TestClientAdvertiseErrorOnStartup(t *testing.T) {
	opts := DefaultOptions()
	// Set invalid address
	opts.ClientAdvertise = "addr:::123"
	testFatalErrorOnStart(t, opts, "ClientAdvertise")
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
	ch := make(chan struct{})
	go func() {
		s.Start()
		close(ch)
	}()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("Start() should have returned due to failure to start listener")
	}

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

func TestLameDuckOptionsValidation(t *testing.T) {
	o := DefaultOptions()
	o.LameDuckDuration = 5 * time.Second
	o.LameDuckGracePeriod = 10 * time.Second
	s, err := NewServer(o)
	if s != nil {
		s.Shutdown()
	}
	if err == nil || !strings.Contains(err.Error(), "should be strictly lower") {
		t.Fatalf("Expected error saying that ldm grace period should be lower than ldm duration, got %v", err)
	}
}

func testSetLDMGracePeriod(o *Options, val time.Duration) {
	// For tests, we set the grace period as a negative value
	// so we can have a grace period bigger than the total duration.
	// When validating options, we would not be able to run the
	// server without this trick.
	o.LameDuckGracePeriod = val * -1
}

func TestLameDuckMode(t *testing.T) {
	optsA := DefaultOptions()
	testSetLDMGracePeriod(optsA, time.Nanosecond)
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
	// Connections are saved in go routines, so although we have evaluated the number
	// of connections in the server A to be 0, the polling of connection closed may
	// need a bit more time.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		cz := pollConz(t, srvA, 1, "", &ConnzOptions{State: ConnClosed})
		if n := len(cz.Conns); n != total {
			return fmt.Errorf("expected %v closed connections, got %v", total, n)
		}
		return nil
	})
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

	if elapsed > time.Duration(float64(optsA.LameDuckDuration)*1.1) {
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
	testSetLDMGracePeriod(optsA, 100*time.Millisecond)
	optsA.LameDuckDuration = 10 * time.Millisecond
	srvA = RunServer(optsA)
	defer srvA.Shutdown()

	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srvA.ClusterAddr().Port))
	testSetLDMGracePeriod(optsB, 100*time.Millisecond)
	optsB.LameDuckDuration = 10 * time.Millisecond
	srvB = RunServer(optsB)
	defer srvB.Shutdown()

	optsC := DefaultOptions()
	optsC.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srvA.ClusterAddr().Port))
	testSetLDMGracePeriod(optsC, 100*time.Millisecond)
	optsC.LameDuckGracePeriod = -100 * time.Millisecond
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

func TestLameDuckModeInfo(t *testing.T) {
	optsA := testWSOptions()
	optsA.Cluster.Name = "abc"
	optsA.Cluster.Host = "127.0.0.1"
	optsA.Cluster.Port = -1
	// Ensure that initial delay is set very high so that we can
	// check that some events occur as expected before the client
	// is disconnected.
	testSetLDMGracePeriod(optsA, 5*time.Second)
	optsA.LameDuckDuration = 50 * time.Millisecond
	optsA.DisableShortFirstPing = true
	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	curla := fmt.Sprintf("127.0.0.1:%d", optsA.Port)
	wscurla := fmt.Sprintf("127.0.0.1:%d", optsA.Websocket.Port)
	c, err := net.Dial("tcp", curla)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer c.Close()
	client := bufio.NewReaderSize(c, maxBufSize)

	wsconn, wsclient := testWSCreateClient(t, false, false, optsA.Websocket.Host, optsA.Websocket.Port)
	defer wsconn.Close()

	getInfo := func(ws bool) *serverInfo {
		t.Helper()
		var l string
		var err error
		if ws {
			l = string(testWSReadFrame(t, wsclient))
		} else {
			l, err = client.ReadString('\n')
			if err != nil {
				t.Fatalf("Error receiving info from server: %v\n", err)
			}
		}
		var info serverInfo
		if err = json.Unmarshal([]byte(l[5:]), &info); err != nil {
			t.Fatalf("Could not parse INFO json: %v\n", err)
		}
		return &info
	}

	getInfo(false)
	c.Write([]byte("CONNECT {\"protocol\":1,\"verbose\":false}\r\nPING\r\n"))
	client.ReadString('\n')

	optsB := testWSOptions()
	optsB.Cluster.Name = "abc"
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srvA.ClusterAddr().Port))
	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	checkConnectURLs := func(expected [][]string) *serverInfo {
		t.Helper()
		var si *serverInfo
		for i, ws := range []bool{false, true} {
			sort.Strings(expected[i])
			si = getInfo(ws)
			sort.Strings(si.ConnectURLs)
			if !reflect.DeepEqual(expected[i], si.ConnectURLs) {
				t.Fatalf("Expected %q, got %q", expected, si.ConnectURLs)
			}
		}
		return si
	}

	curlb := fmt.Sprintf("127.0.0.1:%d", optsB.Port)
	wscurlb := fmt.Sprintf("127.0.0.1:%d", optsB.Websocket.Port)
	expected := [][]string{{curla, curlb}, {wscurla, wscurlb}}
	checkConnectURLs(expected)

	optsC := testWSOptions()
	testSetLDMGracePeriod(optsA, 5*time.Second)
	optsC.Cluster.Name = "abc"
	optsC.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srvA.ClusterAddr().Port))
	srvC := RunServer(optsC)
	defer srvC.Shutdown()

	checkClusterFormed(t, srvA, srvB, srvC)

	curlc := fmt.Sprintf("127.0.0.1:%d", optsC.Port)
	wscurlc := fmt.Sprintf("127.0.0.1:%d", optsC.Websocket.Port)
	expected = [][]string{{curla, curlb, curlc}, {wscurla, wscurlb, wscurlc}}
	checkConnectURLs(expected)

	optsD := testWSOptions()
	optsD.Cluster.Name = "abc"
	optsD.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srvA.ClusterAddr().Port))
	srvD := RunServer(optsD)
	defer srvD.Shutdown()

	checkClusterFormed(t, srvA, srvB, srvC, srvD)

	curld := fmt.Sprintf("127.0.0.1:%d", optsD.Port)
	wscurld := fmt.Sprintf("127.0.0.1:%d", optsD.Websocket.Port)
	expected = [][]string{{curla, curlb, curlc, curld}, {wscurla, wscurlb, wscurlc, wscurld}}
	checkConnectURLs(expected)

	// Now lame duck server A and C. We should have client connected to A
	// receive info that A is in LDM without A's URL, but also receive
	// an update with C's URL gone.
	// But first we need to create a client to C because otherwise the
	// LDM signal will just shut it down because it would have no client.
	nc, err := nats.Connect(srvC.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()
	nc.Flush()

	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		srvA.lameDuckMode()
	}()

	expected = [][]string{{curlb, curlc, curld}, {wscurlb, wscurlc, wscurld}}
	si := checkConnectURLs(expected)
	if !si.LameDuckMode {
		t.Fatal("Expected LameDuckMode to be true, it was not")
	}

	// Start LDM for server C. This should send an update to A
	// which in turn should remove C from the list of URLs and
	// update its client.
	go func() {
		defer wg.Done()
		srvC.lameDuckMode()
	}()

	expected = [][]string{{curlb, curld}, {wscurlb, wscurld}}
	si = checkConnectURLs(expected)
	// This update should not say that it is LDM.
	if si.LameDuckMode {
		t.Fatal("Expected LameDuckMode to be false, it was true")
	}

	// Now shutdown D, and we also should get an update.
	srvD.Shutdown()

	expected = [][]string{{curlb}, {wscurlb}}
	si = checkConnectURLs(expected)
	// This update should not say that it is LDM.
	if si.LameDuckMode {
		t.Fatal("Expected LameDuckMode to be false, it was true")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("Did not get the expected events prior of server A and C shutting down")
	}

	// Now explicitly shutdown srvA. When a server shutdown, it closes all its
	// connections. For routes, it means that it is going to remove the remote's
	// URL from its map. We want to make sure that in that case, server does not
	// actually send an updated INFO to its clients.
	srvA.Shutdown()

	// Expect nothing to be received on the client connection.
	if l, err := client.ReadString('\n'); err == nil {
		t.Fatalf("Expected connection to fail, instead got %q", l)
	}

	c.Close()
	nc.Close()
	// Don't need to wait for actual disconnect of clients.
	srvC.Shutdown()
	wg.Wait()
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
	wg.Wait()
	if d := s.acceptError("Test", ne, orgDelay); d >= 0 {
		t.Fatalf("Expected delay to be negative, got %v", d)
	}
}

func TestServerShutdownDuringStart(t *testing.T) {
	o := DefaultOptions()
	o.DisableShortFirstPing = true
	o.Accounts = []*Account{NewAccount("$SYS")}
	o.SystemAccount = "$SYS"
	o.Cluster.Name = "abc"
	o.Cluster.Host = "127.0.0.1"
	o.Cluster.Port = -1
	o.Gateway.Name = "abc"
	o.Gateway.Host = "127.0.0.1"
	o.Gateway.Port = -1
	o.LeafNode.Host = "127.0.0.1"
	o.LeafNode.Port = -1
	o.Websocket.Host = "127.0.0.1"
	o.Websocket.Port = -1
	o.Websocket.HandshakeTimeout = 1
	o.Websocket.NoTLS = true
	o.MQTT.Host = "127.0.0.1"
	o.MQTT.Port = -1

	// We are going to test that if the server is shutdown
	// while Start() runs (in this case, before), we don't
	// start the listeners and therefore leave accept loops
	// hanging.
	s, err := NewServer(o)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	s.Shutdown()

	// Start() should not block, but just in case, start in
	// different go routine.
	ch := make(chan struct{}, 1)
	go func() {
		s.Start()
		close(ch)
	}()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("Start appear to have blocked after server was shutdown")
	}
	// Now make sure that none of the listeners have been created
	listeners := []string{}
	s.mu.Lock()
	if s.listener != nil {
		listeners = append(listeners, "client")
	}
	if s.routeListener != nil {
		listeners = append(listeners, "route")
	}
	if s.gatewayListener != nil {
		listeners = append(listeners, "gateway")
	}
	if s.leafNodeListener != nil {
		listeners = append(listeners, "leafnode")
	}
	if s.websocket.listener != nil {
		listeners = append(listeners, "websocket")
	}
	if s.mqtt.listener != nil {
		listeners = append(listeners, "mqtt")
	}
	s.mu.Unlock()
	if len(listeners) > 0 {
		lst := ""
		for i, l := range listeners {
			if i > 0 {
				lst += ", "
			}
			lst += l
		}
		t.Fatalf("Following listeners have been created: %s", lst)
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
	if _, err := s.getRandomIP(resolver, "noport", nil); err == nil || !strings.Contains(err.Error(), "port") {
		t.Fatalf("Expected error about port missing, got %v", err)
	}
	resolver.err = fmt.Errorf("on purpose")
	if _, err := s.getRandomIP(resolver, "localhost:4222", nil); err == nil || !strings.Contains(err.Error(), "on purpose") {
		t.Fatalf("Expected error about no port, got %v", err)
	}
	resolver.err = nil
	a, err := s.getRandomIP(resolver, "localhost:4222", nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if a != "localhost:4222" {
		t.Fatalf("Expected address to be %q, got %q", "localhost:4222", a)
	}
	resolver.ips = []string{"1.2.3.4"}
	a, err = s.getRandomIP(resolver, "localhost:4222", nil)
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
		ip, err := s.getRandomIP(resolver, "localhost:4222", nil)
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

	// Check IP exclusions
	excludedIPs := map[string]struct{}{"1.2.3.4:4222": struct{}{}}
	for i := 0; i < 100; i++ {
		ip, err := s.getRandomIP(resolver, "localhost:4222", excludedIPs)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ip[0] == '1' {
			t.Fatalf("Should not have returned this ip: %q", ip)
		}
	}
	excludedIPs["2.2.3.4:4222"] = struct{}{}
	for i := 0; i < 100; i++ {
		ip, err := s.getRandomIP(resolver, "localhost:4222", excludedIPs)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ip[0] != '3' {
			t.Fatalf("Should only have returned '3.2.3.4', got returned %q", ip)
		}
	}
	excludedIPs["3.2.3.4:4222"] = struct{}{}
	for i := 0; i < 100; i++ {
		if _, err := s.getRandomIP(resolver, "localhost:4222", excludedIPs); err != errNoIPAvail {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Now check that exclusion takes into account the port number.
	resolver.ips = []string{"127.0.0.1"}
	excludedIPs = map[string]struct{}{"127.0.0.1:4222": struct{}{}}
	for i := 0; i < 100; i++ {
		if _, err := s.getRandomIP(resolver, "localhost:4223", excludedIPs); err == errNoIPAvail {
			t.Fatal("Should not have failed")
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
	nc.Flush()
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
		if !s.ReadyForConnections(time.Second) {
			t.Fatal("Unable to start the server")
		}
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
	o.Cluster.Name = "A"
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
	// On Windows, an attempt to connect to a port that has no listener will
	// take whatever timeout specified in DialTimeout() before failing.
	// So skip for now.
	if runtime.GOOS == "windows" {
		t.Skip()
	}
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

	checkContent := func(t *testing.T, txt string, attempt int, shouldBeThere bool) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			content, err := ioutil.ReadFile(log)
			if err != nil {
				return fmt.Errorf("Error reading log file: %v", err)
			}
			present := bytes.Contains(content, []byte(fmt.Sprintf("%s (attempt %d)", txt, attempt)))
			if shouldBeThere && !present {
				return fmt.Errorf("Did not find expected log statement (%s) for attempt %d: %s", txt, attempt, content)
			} else if !shouldBeThere && present {
				return fmt.Errorf("Log statement (%s) for attempt %d should not be present: %s", txt, attempt, content)
			}
			return nil
		})
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

	s.Shutdown()
	os.Remove(log)

	// Now try with leaf nodes
	opts.Cluster.Port = 0
	opts.Routes = nil
	opts.LeafNode.Remotes = []*RemoteLeafOpts{&RemoteLeafOpts{URLs: []*url.URL{remoteURLs[0]}}}
	opts.LeafNode.ReconnectInterval = 15 * time.Millisecond
	s = RunServer(opts)
	defer s.Shutdown()

	checkLeafContent := func(t *testing.T, txt, host string, attempt int, shouldBeThere bool) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			content, err := ioutil.ReadFile(log)
			if err != nil {
				return fmt.Errorf("Error reading log file: %v", err)
			}
			present := bytes.Contains(content, []byte(fmt.Sprintf("%s %q (attempt %d)", txt, host, attempt)))
			if shouldBeThere && !present {
				return fmt.Errorf("Did not find expected log statement (%s %q) for attempt %d: %s", txt, host, attempt, content)
			} else if !shouldBeThere && present {
				return fmt.Errorf("Log statement (%s %q) for attempt %d should not be present: %s", txt, host, attempt, content)
			}
			return nil
		})
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

	s.Shutdown()
	os.Remove(log)

	// Now try with gateways
	opts.LeafNode.Remotes = nil
	opts.Cluster.Name = "A"
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
	// On Windows, an attempt to connect to a port that has no listener will
	// take whatever timeout specified in DialTimeout() before failing.
	// So skip for now.
	if runtime.GOOS == "windows" {
		t.Skip()
	}
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

	// Specifically for route test, wait at least reconnect interval before checking logs
	time.Sleep(DEFAULT_ROUTE_RECONNECT)

	checkContent := func(t *testing.T, txt string, attempt int, shouldBeThere bool) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			content, err := ioutil.ReadFile(log)
			if err != nil {
				return fmt.Errorf("Error reading log file: %v", err)
			}
			present := bytes.Contains(content, []byte(fmt.Sprintf("%s (attempt %d)", txt, attempt)))
			if shouldBeThere && !present {
				return fmt.Errorf("Did not find expected log statement (%s) for attempt %d: %s", txt, attempt, content)
			} else if !shouldBeThere && present {
				return fmt.Errorf("Log statement (%s) for attempt %d should not be present: %s", txt, attempt, content)
			}
			return nil
		})
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

	s.Shutdown()
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

	checkLeafNodeConnected(t, s)

	// Now shutdown the server s is connected to
	cs.Shutdown()

	checkLeafContent := func(t *testing.T, txt, host string, attempt int, shouldBeThere bool) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			content, err := ioutil.ReadFile(log)
			if err != nil {
				return fmt.Errorf("Error reading log file: %v", err)
			}
			present := bytes.Contains(content, []byte(fmt.Sprintf("%s %q (attempt %d)", txt, host, attempt)))
			if shouldBeThere && !present {
				return fmt.Errorf("Did not find expected log statement (%s %q) for attempt %d: %s", txt, host, attempt, content)
			} else if !shouldBeThere && present {
				return fmt.Errorf("Log statement (%s %q) for attempt %d should not be present: %s", txt, host, attempt, content)
			}
			return nil
		})
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

	s.Shutdown()
	os.Remove(log)

	// Now try with gateways
	csOpts.LeafNode.Port = 0
	csOpts.Cluster.Name = "B"
	csOpts.Gateway.Name = "B"
	csOpts.Gateway.Port = -1
	cs = RunServer(csOpts)

	opts.LeafNode.Remotes = nil
	opts.Cluster.Name = "A"
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

func TestServerLogsConfigurationFile(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "_nats-server")
	if err != nil {
		t.Fatal("Could not create tmp dir")
	}
	defer os.RemoveAll(tmpDir)

	file, err := ioutil.TempFile(tmpDir, "nats_server_log_")
	if err != nil {
		t.Fatalf("Could not create the temp file: %v", err)
	}
	file.Close()

	conf := createConfFile(t, []byte(fmt.Sprintf(`
	port: -1
	logfile: "%s"
	`, file.Name())))
	defer os.Remove(conf)

	o := LoadConfig(conf)
	o.ConfigFile = file.Name()
	o.NoLog = false
	s := RunServer(o)
	s.Shutdown()

	log, err := ioutil.ReadFile(file.Name())
	if err != nil {
		t.Fatalf("Error reading log file: %v", err)
	}
	if !bytes.Contains(log, []byte(fmt.Sprintf("Using configuration file: %s", file.Name()))) {
		t.Fatalf("Config file location was not reported in log: %s", log)
	}
}
