// Copyright 2013-2020 The NATS Authors
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
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func init() {
	routeConnectDelay = 15 * time.Millisecond
}

func checkNumRoutes(t *testing.T, s *Server, expected int) {
	t.Helper()
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		if nr := s.NumRoutes(); nr != expected {
			return fmt.Errorf("Expected %v routes, got %v", expected, nr)
		}
		return nil
	})
}

func checkSubInterest(t *testing.T, s *Server, accName, subject string, timeout time.Duration) {
	t.Helper()
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		acc, err := s.LookupAccount(accName)
		if err != nil {
			return fmt.Errorf("error looking up account %q: %v", accName, err)
		}
		if acc.SubscriptionInterest(subject) {
			return nil
		}
		return fmt.Errorf("no subscription interest for account %q on %q", accName, subject)
	})
}

func TestRouteConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/cluster.conf")
	if err != nil {
		t.Fatalf("Received an error reading route config file: %v\n", err)
	}

	golden := &Options{
		ConfigFile:  "./configs/cluster.conf",
		Host:        "127.0.0.1",
		Port:        4242,
		Username:    "derek",
		Password:    "porkchop",
		AuthTimeout: 1.0,
		Cluster: ClusterOpts{
			Name:           "abc",
			Host:           "127.0.0.1",
			Port:           4244,
			Username:       "route_user",
			Password:       "top_secret",
			AuthTimeout:    1.0,
			NoAdvertise:    true,
			ConnectRetries: 2,
		},
		PidFile: "/tmp/nats-server/nats_cluster_test.pid",
	}

	// Setup URLs
	r1, _ := url.Parse("nats-route://foo:bar@127.0.0.1:4245")
	r2, _ := url.Parse("nats-route://foo:bar@127.0.0.1:4246")

	golden.Routes = []*url.URL{r1, r2}

	checkOptionsEqual(t, golden, opts)
}

func TestClusterAdvertise(t *testing.T) {
	lst, err := natsListen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Error starting listener: %v", err)
	}
	ch := make(chan error)
	go func() {
		c, err := lst.Accept()
		if err != nil {
			ch <- err
			return
		}
		c.Close()
		ch <- nil
	}()

	optsA, err := ProcessConfigFile("./configs/seed.conf")
	require_NoError(t, err)
	optsA.NoSigs, optsA.NoLog = true, true
	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	srvARouteURL := fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host, srvA.ClusterAddr().Port)
	optsB := nextServerOpts(optsA)
	optsB.Routes = RoutesFromStr(srvARouteURL)

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	// Wait for these 2 to connect to each other
	checkClusterFormed(t, srvA, srvB)

	// Now start server C that connects to A. A should ask B to connect to C,
	// based on C's URL. But since C configures a Cluster.Advertise, it will connect
	// to our listener.
	optsC := nextServerOpts(optsB)
	optsC.Cluster.Advertise = lst.Addr().String()
	optsC.ClientAdvertise = "me:1"
	optsC.Routes = RoutesFromStr(srvARouteURL)

	srvC := RunServer(optsC)
	defer srvC.Shutdown()

	select {
	case e := <-ch:
		if e != nil {
			t.Fatalf("Error: %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Test timed out")
	}
}

func TestClusterAdvertiseErrorOnStartup(t *testing.T) {
	opts := DefaultOptions()
	// Set invalid address
	opts.Cluster.Advertise = "addr:::123"
	testFatalErrorOnStart(t, opts, "Cluster.Advertise")
}

func TestClientAdvertise(t *testing.T) {
	optsA, err := ProcessConfigFile("./configs/seed.conf")
	require_NoError(t, err)
	optsA.NoSigs, optsA.NoLog = true, true

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	optsB := nextServerOpts(optsA)
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host, optsA.Cluster.Port))
	optsB.ClientAdvertise = "me:1"
	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		ds := nc.DiscoveredServers()
		if len(ds) == 1 {
			if ds[0] == "nats://me:1" {
				return nil
			}
		}
		return fmt.Errorf("Did not get expected discovered servers: %v", nc.DiscoveredServers())
	})
}

func TestServerRoutesWithClients(t *testing.T) {
	optsA, err := ProcessConfigFile("./configs/srv_a.conf")
	require_NoError(t, err)
	optsB, err := ProcessConfigFile("./configs/srv_b.conf")
	require_NoError(t, err)

	optsA.NoSigs, optsA.NoLog = true, true
	optsB.NoSigs, optsB.NoLog = true, true

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	urlA := fmt.Sprintf("nats://%s:%d/", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, optsB.Port)

	nc1, err := nats.Connect(urlA)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc1.Close()

	ch := make(chan bool)
	sub, _ := nc1.Subscribe("foo", func(m *nats.Msg) { ch <- true })
	nc1.QueueSubscribe("foo", "bar", func(m *nats.Msg) {})
	nc1.Publish("foo", []byte("Hello"))
	// Wait for message
	<-ch
	sub.Unsubscribe()

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	// Wait for route to form.
	checkClusterFormed(t, srvA, srvB)

	nc2, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()
	nc2.Publish("foo", []byte("Hello"))
	nc2.Flush()
}

func TestServerRoutesWithAuthAndBCrypt(t *testing.T) {
	optsA, err := ProcessConfigFile("./configs/srv_a_bcrypt.conf")
	require_NoError(t, err)
	optsB, err := ProcessConfigFile("./configs/srv_b_bcrypt.conf")
	require_NoError(t, err)

	optsA.NoSigs, optsA.NoLog = true, true
	optsB.NoSigs, optsB.NoLog = true, true

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	// Wait for route to form.
	checkClusterFormed(t, srvA, srvB)

	urlA := fmt.Sprintf("nats://%s:%s@%s:%d/", optsA.Username, optsA.Password, optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%s@%s:%d/", optsB.Username, optsB.Password, optsB.Host, optsB.Port)

	nc1, err := nats.Connect(urlA)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc1.Close()

	// Test that we are connected.
	ch := make(chan bool)
	sub, err := nc1.Subscribe("foo", func(m *nats.Msg) { ch <- true })
	if err != nil {
		t.Fatalf("Error creating subscription: %v\n", err)
	}
	nc1.Flush()
	defer sub.Unsubscribe()

	checkSubInterest(t, srvB, globalAccountName, "foo", time.Second)

	nc2, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()
	nc2.Publish("foo", []byte("Hello"))
	nc2.Flush()

	// Wait for message
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message across route")
	}
}

// Helper function to check that a cluster is formed
func checkClusterFormed(t *testing.T, servers ...*Server) {
	t.Helper()
	expectedNumRoutes := len(servers) - 1
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		for _, s := range servers {
			if numRoutes := s.NumRoutes(); numRoutes != expectedNumRoutes {
				return fmt.Errorf("Expected %d routes for server %q, got %d", expectedNumRoutes, s.ID(), numRoutes)
			}
		}
		return nil
	})
}

// Helper function to generate next opts to make sure no port conflicts etc.
func nextServerOpts(opts *Options) *Options {
	nopts := *opts
	nopts.Port = -1
	nopts.Cluster.Port = -1
	nopts.HTTPPort = -1
	if nopts.Gateway.Name != "" {
		nopts.Gateway.Port = -1
	}
	nopts.ServerName = ""
	return &nopts
}

func TestSeedSolicitWorks(t *testing.T) {
	optsSeed, err := ProcessConfigFile("./configs/seed.conf")
	require_NoError(t, err)

	optsSeed.NoSigs, optsSeed.NoLog = true, true
	optsSeed.NoSystemAccount = true

	srvSeed := RunServer(optsSeed)
	defer srvSeed.Shutdown()

	optsA := nextServerOpts(optsSeed)
	optsA.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host,
		srvSeed.ClusterAddr().Port))

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	urlA := fmt.Sprintf("nats://%s:%d/", optsA.Host, srvA.ClusterAddr().Port)

	nc1, err := nats.Connect(urlA)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc1.Close()

	// Test that we are connected.
	ch := make(chan bool)
	nc1.Subscribe("foo", func(m *nats.Msg) { ch <- true })
	nc1.Flush()

	optsB := nextServerOpts(optsA)
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host,
		srvSeed.ClusterAddr().Port))

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, srvB.ClusterAddr().Port)

	nc2, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()

	checkClusterFormed(t, srvSeed, srvA, srvB)
	checkExpectedSubs(t, 1, srvB)

	nc2.Publish("foo", []byte("Hello"))

	// Wait for message
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message across route")
	}
}

func TestTLSSeedSolicitWorks(t *testing.T) {
	optsSeed, err := ProcessConfigFile("./configs/seed_tls.conf")
	require_NoError(t, err)

	optsSeed.NoSigs, optsSeed.NoLog = true, true
	optsSeed.NoSystemAccount = true

	srvSeed := RunServer(optsSeed)
	defer srvSeed.Shutdown()

	seedRouteURL := fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host,
		srvSeed.ClusterAddr().Port)
	optsA := nextServerOpts(optsSeed)
	optsA.Routes = RoutesFromStr(seedRouteURL)

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	urlA := fmt.Sprintf("nats://%s:%d/", optsA.Host, srvA.Addr().(*net.TCPAddr).Port)

	nc1, err := nats.Connect(urlA)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc1.Close()

	// Test that we are connected.
	ch := make(chan bool)
	nc1.Subscribe("foo", func(m *nats.Msg) { ch <- true })
	nc1.Flush()

	optsB := nextServerOpts(optsA)
	optsB.Routes = RoutesFromStr(seedRouteURL)

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, srvB.Addr().(*net.TCPAddr).Port)

	nc2, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()

	checkClusterFormed(t, srvSeed, srvA, srvB)
	checkExpectedSubs(t, 1, srvB)

	nc2.Publish("foo", []byte("Hello"))

	// Wait for message
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message across route")
	}
}

func TestChainedSolicitWorks(t *testing.T) {
	optsSeed, err := ProcessConfigFile("./configs/seed.conf")
	require_NoError(t, err)

	optsSeed.NoSigs, optsSeed.NoLog = true, true
	optsSeed.NoSystemAccount = true

	srvSeed := RunServer(optsSeed)
	defer srvSeed.Shutdown()

	seedRouteURL := fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host,
		srvSeed.ClusterAddr().Port)
	optsA := nextServerOpts(optsSeed)
	optsA.Routes = RoutesFromStr(seedRouteURL)

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	urlSeed := fmt.Sprintf("nats://%s:%d/", optsSeed.Host, srvA.Addr().(*net.TCPAddr).Port)

	nc1, err := nats.Connect(urlSeed)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc1.Close()

	// Test that we are connected.
	ch := make(chan bool)
	nc1.Subscribe("foo", func(m *nats.Msg) { ch <- true })
	nc1.Flush()

	optsB := nextServerOpts(optsA)
	// Server B connects to A
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host,
		srvA.ClusterAddr().Port))

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, srvB.Addr().(*net.TCPAddr).Port)

	nc2, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()

	checkClusterFormed(t, srvSeed, srvA, srvB)
	checkExpectedSubs(t, 1, srvB)

	nc2.Publish("foo", []byte("Hello"))

	// Wait for message
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message across route")
	}
}

// Helper function to check that a server (or list of servers) have the
// expected number of subscriptions.
func checkExpectedSubs(t *testing.T, expected int, servers ...*Server) {
	t.Helper()
	checkFor(t, 4*time.Second, 10*time.Millisecond, func() error {
		for _, s := range servers {
			if numSubs := int(s.NumSubscriptions()); numSubs != expected {
				return fmt.Errorf("Expected %d subscriptions for server %q, got %d", expected, s.ID(), numSubs)
			}
		}
		return nil
	})
}

func TestTLSChainedSolicitWorks(t *testing.T) {
	optsSeed, err := ProcessConfigFile("./configs/seed_tls.conf")
	require_NoError(t, err)

	optsSeed.NoSigs, optsSeed.NoLog = true, true
	optsSeed.NoSystemAccount = true

	srvSeed := RunServer(optsSeed)
	defer srvSeed.Shutdown()

	urlSeedRoute := fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host,
		srvSeed.ClusterAddr().Port)
	optsA := nextServerOpts(optsSeed)
	optsA.Routes = RoutesFromStr(urlSeedRoute)

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	urlSeed := fmt.Sprintf("nats://%s:%d/", optsSeed.Host, srvSeed.Addr().(*net.TCPAddr).Port)

	nc1, err := nats.Connect(urlSeed)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc1.Close()

	// Test that we are connected.
	ch := make(chan bool)
	nc1.Subscribe("foo", func(m *nats.Msg) { ch <- true })
	nc1.Flush()

	optsB := nextServerOpts(optsA)
	// Server B connects to A
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host,
		srvA.ClusterAddr().Port))

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvSeed, srvA, srvB)
	checkExpectedSubs(t, 1, srvA, srvB)

	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, srvB.Addr().(*net.TCPAddr).Port)

	nc2, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()

	nc2.Publish("foo", []byte("Hello"))

	// Wait for message
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message across route")
	}
}

func TestRouteTLSHandshakeError(t *testing.T) {
	optsSeed, err := ProcessConfigFile("./configs/seed_tls.conf")
	require_NoError(t, err)
	optsSeed.NoLog = true
	optsSeed.NoSigs = true
	srvSeed := RunServer(optsSeed)
	defer srvSeed.Shutdown()

	opts := DefaultOptions()
	opts.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host, optsSeed.Cluster.Port))

	srv := RunServer(opts)
	defer srv.Shutdown()

	time.Sleep(500 * time.Millisecond)

	checkNumRoutes(t, srv, 0)
}

func TestBlockedShutdownOnRouteAcceptLoopFailure(t *testing.T) {
	opts := DefaultOptions()
	opts.Cluster.Host = "x.x.x.x"
	opts.Cluster.Port = 7222

	s := New(opts)
	go s.Start()
	// Wait a second
	time.Sleep(time.Second)
	ch := make(chan bool)
	go func() {
		s.Shutdown()
		ch <- true
	}()

	timeout := time.NewTimer(5 * time.Second)
	select {
	case <-ch:
		return
	case <-timeout.C:
		t.Fatal("Shutdown did not complete")
	}
}

func TestRouteUseIPv6(t *testing.T) {
	opts := DefaultOptions()
	opts.Cluster.Host = "::"
	opts.Cluster.Port = 6222

	// I believe that there is no IPv6 support on Travis...
	// Regardless, cannot have this test fail simply because IPv6 is disabled
	// on the host.
	hp := net.JoinHostPort(opts.Cluster.Host, strconv.Itoa(opts.Cluster.Port))
	_, err := net.ResolveTCPAddr("tcp", hp)
	if err != nil {
		t.Skipf("Skipping this test since there is no IPv6 support on this host: %v", err)
	}

	s := RunServer(opts)
	defer s.Shutdown()

	routeUp := false
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) && !routeUp {
		// We know that the server is local and listening to
		// all IPv6 interfaces. Try connect using IPv6 loopback.
		if conn, err := net.Dial("tcp", "[::1]:6222"); err != nil {
			// Travis seem to have the server actually listening to 0.0.0.0,
			// so try with 127.0.0.1
			if conn, err := net.Dial("tcp", "127.0.0.1:6222"); err != nil {
				time.Sleep(time.Second)
				continue
			} else {
				conn.Close()
			}
		} else {
			conn.Close()
		}
		routeUp = true
	}
	if !routeUp {
		t.Fatal("Server failed to start route accept loop")
	}
}

func TestClientConnectToRoutePort(t *testing.T) {
	opts := DefaultOptions()

	// Since client will first connect to the route listen port, set the
	// cluster's Host to 127.0.0.1 so it works on Windows too, since on
	// Windows, a client can't use 0.0.0.0 in a connect.
	opts.Cluster.Host = "127.0.0.1"
	s := RunServer(opts)
	defer s.Shutdown()

	url := fmt.Sprintf("nats://%s:%d", opts.Cluster.Host, s.ClusterAddr().Port)
	clientURL := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	// When connecting to the ROUTE port, the client library will receive the
	// CLIENT port in the INFO protocol. This URL is added to the client's pool
	// and will be tried after the initial connect failure. So all those
	// nats.Connect() should succeed.
	// The only reason for a failure would be if there are too many FDs in time-wait
	// which would delay the creation of TCP connection. So keep the total of
	// attempts rather small.
	total := 10
	for i := 0; i < total; i++ {
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Unexepected error on connect: %v", err)
		}
		defer nc.Close()
		if nc.ConnectedUrl() != clientURL {
			t.Fatalf("Expected client to be connected to %v, got %v", clientURL, nc.ConnectedUrl())
		}
	}

	s.Shutdown()
	// Try again with NoAdvertise and this time, the client should fail to connect.
	opts.Cluster.NoAdvertise = true
	s = RunServer(opts)
	defer s.Shutdown()

	for i := 0; i < total; i++ {
		nc, err := nats.Connect(url)
		if err == nil {
			nc.Close()
			t.Fatal("Expected error on connect, got none")
		}
	}
}

type checkDuplicateRouteLogger struct {
	sync.Mutex
	gotDuplicate bool
}

func (l *checkDuplicateRouteLogger) Noticef(format string, v ...interface{}) {}
func (l *checkDuplicateRouteLogger) Errorf(format string, v ...interface{})  {}
func (l *checkDuplicateRouteLogger) Warnf(format string, v ...interface{})   {}
func (l *checkDuplicateRouteLogger) Fatalf(format string, v ...interface{})  {}
func (l *checkDuplicateRouteLogger) Tracef(format string, v ...interface{})  {}
func (l *checkDuplicateRouteLogger) Debugf(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "duplicate remote route") {
		l.gotDuplicate = true
	}
}

func TestRoutesToEachOther(t *testing.T) {
	optsA := DefaultOptions()
	optsA.Cluster.Port = 7246
	optsA.Routes = RoutesFromStr("nats://127.0.0.1:7247")

	optsB := DefaultOptions()
	optsB.Cluster.Port = 7247
	optsB.Routes = RoutesFromStr("nats://127.0.0.1:7246")

	srvALogger := &checkDuplicateRouteLogger{}
	srvA := New(optsA)
	srvA.SetLogger(srvALogger, true, false)
	defer srvA.Shutdown()

	srvBLogger := &checkDuplicateRouteLogger{}
	srvB := New(optsB)
	srvB.SetLogger(srvBLogger, true, false)
	defer srvB.Shutdown()

	go srvA.Start()
	go srvB.Start()

	start := time.Now()
	checkClusterFormed(t, srvA, srvB)
	end := time.Now()

	srvALogger.Lock()
	gotIt := srvALogger.gotDuplicate
	srvALogger.Unlock()
	if !gotIt {
		srvBLogger.Lock()
		gotIt = srvBLogger.gotDuplicate
		srvBLogger.Unlock()
	}
	if gotIt {
		dur := end.Sub(start)
		// It should not take too long to have a successful connection
		// between the 2 servers.
		if dur > 5*time.Second {
			t.Logf("Cluster formed, but took a long time: %v", dur)
		}
	} else {
		t.Log("Was not able to get duplicate route this time!")
	}
}

func wait(ch chan bool) error {
	select {
	case <-ch:
		return nil
	case <-time.After(5 * time.Second):
	}
	return fmt.Errorf("timeout")
}

func TestServerPoolUpdatedWhenRouteGoesAway(t *testing.T) {
	s1Opts := DefaultOptions()
	s1Opts.ServerName = "A"
	s1Opts.Host = "127.0.0.1"
	s1Opts.Port = 4222
	s1Opts.Cluster.Host = "127.0.0.1"
	s1Opts.Cluster.Port = 6222
	s1Opts.Routes = RoutesFromStr("nats://127.0.0.1:6223,nats://127.0.0.1:6224")
	s1 := RunServer(s1Opts)
	defer s1.Shutdown()

	s1Url := "nats://127.0.0.1:4222"
	s2Url := "nats://127.0.0.1:4223"
	s3Url := "nats://127.0.0.1:4224"

	ch := make(chan bool, 1)
	chch := make(chan bool, 1)
	connHandler := func(_ *nats.Conn) {
		chch <- true
	}
	nc, err := nats.Connect(s1Url,
		nats.ReconnectWait(50*time.Millisecond),
		nats.ReconnectHandler(connHandler),
		nats.DiscoveredServersHandler(func(_ *nats.Conn) {
			ch <- true
		}))
	if err != nil {
		t.Fatalf("Error on connect")
	}

	s2Opts := DefaultOptions()
	s2Opts.ServerName = "B"
	s2Opts.Host = "127.0.0.1"
	s2Opts.Port = s1Opts.Port + 1
	s2Opts.Cluster.Host = "127.0.0.1"
	s2Opts.Cluster.Port = 6223
	s2Opts.Routes = RoutesFromStr("nats://127.0.0.1:6222,nats://127.0.0.1:6224")
	s2 := RunServer(s2Opts)
	defer s2.Shutdown()

	// Wait to be notified
	if err := wait(ch); err != nil {
		t.Fatal("New server callback was not invoked")
	}

	checkPool := func(expected []string) {
		t.Helper()
		// Don't use discovered here, but Servers to have the full list.
		// Also, there may be cases where the mesh is not formed yet,
		// so try again on failure.
		checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
			ds := nc.Servers()
			if len(ds) == len(expected) {
				m := make(map[string]struct{}, len(ds))
				for _, url := range ds {
					m[url] = struct{}{}
				}
				ok := true
				for _, url := range expected {
					if _, present := m[url]; !present {
						ok = false
						break
					}
				}
				if ok {
					return nil
				}
			}
			return fmt.Errorf("Expected %v, got %v", expected, ds)
		})
	}
	// Verify that we now know about s2
	checkPool([]string{s1Url, s2Url})

	s3Opts := DefaultOptions()
	s3Opts.ServerName = "C"
	s3Opts.Host = "127.0.0.1"
	s3Opts.Port = s2Opts.Port + 1
	s3Opts.Cluster.Host = "127.0.0.1"
	s3Opts.Cluster.Port = 6224
	s3Opts.Routes = RoutesFromStr("nats://127.0.0.1:6222,nats://127.0.0.1:6223")
	s3 := RunServer(s3Opts)
	defer s3.Shutdown()

	// Wait to be notified
	if err := wait(ch); err != nil {
		t.Fatal("New server callback was not invoked")
	}
	// Verify that we now know about s3
	checkPool([]string{s1Url, s2Url, s3Url})

	// Stop s1. Since this was passed to the Connect() call, this one should
	// still be present.
	s1.Shutdown()
	// Wait for reconnect
	if err := wait(chch); err != nil {
		t.Fatal("Reconnect handler not invoked")
	}
	checkPool([]string{s1Url, s2Url, s3Url})

	// Check the server we reconnected to.
	reConnectedTo := nc.ConnectedUrl()
	expected := []string{s1Url}
	if reConnectedTo == s2Url {
		s2.Shutdown()
		expected = append(expected, s3Url)
	} else if reConnectedTo == s3Url {
		s3.Shutdown()
		expected = append(expected, s2Url)
	} else {
		t.Fatalf("Unexpected server client has reconnected to: %v", reConnectedTo)
	}
	// Wait for reconnect
	if err := wait(chch); err != nil {
		t.Fatal("Reconnect handler not invoked")
	}
	// The implicit server that we just shutdown should have been removed from the pool
	checkPool(expected)
	nc.Close()
}

func TestRouteFailedConnRemovedFromTmpMap(t *testing.T) {
	optsA, err := ProcessConfigFile("./configs/srv_a.conf")
	require_NoError(t, err)
	optsA.NoSigs, optsA.NoLog = true, true

	optsB, err := ProcessConfigFile("./configs/srv_b.conf")
	require_NoError(t, err)
	optsB.NoSigs, optsB.NoLog = true, true

	srvA := New(optsA)
	defer srvA.Shutdown()
	srvB := New(optsB)
	defer srvB.Shutdown()

	// Start this way to increase chance of having the two connect
	// to each other at the same time. This will cause one of the
	// route to be dropped.
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		srvA.Start()
		wg.Done()
	}()
	go func() {
		srvB.Start()
		wg.Done()
	}()

	checkClusterFormed(t, srvA, srvB)

	// Ensure that maps are empty
	checkMap := func(s *Server) {
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			s.grMu.Lock()
			l := len(s.grTmpClients)
			s.grMu.Unlock()
			if l != 0 {
				return fmt.Errorf("grTmpClients map should be empty, got %v", l)
			}
			return nil
		})
	}
	checkMap(srvA)
	checkMap(srvB)

	srvB.Shutdown()
	srvA.Shutdown()
	wg.Wait()
}

func TestRoutePermsAppliedOnInboundAndOutboundRoute(t *testing.T) {

	perms := &RoutePermissions{
		Import: &SubjectPermission{
			Allow: []string{"imp.foo"},
			Deny:  []string{"imp.bar"},
		},
		Export: &SubjectPermission{
			Allow: []string{"exp.foo"},
			Deny:  []string{"exp.bar"},
		},
	}

	optsA, err := ProcessConfigFile("./configs/seed.conf")
	require_NoError(t, err)
	optsA.NoLog = true
	optsA.NoSigs = true
	optsA.Cluster.Permissions = perms
	srva := RunServer(optsA)
	defer srva.Shutdown()

	optsB := DefaultOptions()
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host, optsA.Cluster.Port))
	srvb := RunServer(optsB)
	defer srvb.Shutdown()

	checkClusterFormed(t, srva, srvb)

	// Ensure permission is properly set
	check := func(t *testing.T, s *Server) {
		t.Helper()
		var route *client
		s.mu.Lock()
		for _, r := range s.routes {
			route = r
			break
		}
		s.mu.Unlock()
		route.mu.Lock()
		perms := route.perms
		route.mu.Unlock()
		if perms == nil {
			t.Fatal("Expected perms to be set")
		}
		if perms.pub.allow == nil || perms.pub.allow.Count() != 1 {
			t.Fatal("unexpected pub allow perms")
		}
		if r := perms.pub.allow.Match("imp.foo"); len(r.psubs) != 1 {
			t.Fatal("unexpected pub allow match")
		}
		if perms.pub.deny == nil || perms.pub.deny.Count() != 1 {
			t.Fatal("unexpected pub deny perms")
		}
		if r := perms.pub.deny.Match("imp.bar"); len(r.psubs) != 1 {
			t.Fatal("unexpected pub deny match")
		}
		if perms.sub.allow == nil || perms.sub.allow.Count() != 1 {
			t.Fatal("unexpected sub allow perms")
		}
		if r := perms.sub.allow.Match("exp.foo"); len(r.psubs) != 1 {
			t.Fatal("unexpected sub allow match")
		}
		if perms.sub.deny == nil || perms.sub.deny.Count() != 1 {
			t.Fatal("unexpected sub deny perms")
		}
		if r := perms.sub.deny.Match("exp.bar"); len(r.psubs) != 1 {
			t.Fatal("unexpected sub deny match")
		}
	}

	// First check when permissions are set on the server accepting the route connection
	check(t, srva)

	srvb.Shutdown()
	srva.Shutdown()

	optsA.Cluster.Permissions = nil
	optsB.Cluster.Permissions = perms

	srva = RunServer(optsA)
	defer srva.Shutdown()

	srvb = RunServer(optsB)
	defer srvb.Shutdown()

	checkClusterFormed(t, srva, srvb)

	// Now check for permissions set on server initiating the route connection
	check(t, srvb)
}

func TestRouteSendLocalSubsWithLowMaxPending(t *testing.T) {
	optsA := DefaultOptions()
	optsA.MaxPayload = 1024
	optsA.MaxPending = 1024
	optsA.NoSystemAccount = true
	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	numSubs := 1000
	for i := 0; i < numSubs; i++ {
		subj := fmt.Sprintf("fo.bar.%d", i)
		nc.Subscribe(subj, func(_ *nats.Msg) {})
	}
	checkExpectedSubs(t, numSubs, srvA)

	// Now create a route between B and A
	optsB := DefaultOptions()
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host, optsA.Cluster.Port))
	optsB.NoSystemAccount = true
	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	// Check that all subs have been sent ok
	checkExpectedSubs(t, numSubs, srvA, srvB)
}

func TestRouteNoCrashOnAddingSubToRoute(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	numRoutes := routeTargetInit + 2
	total := int32(numRoutes)
	count := int32(0)
	ch := make(chan bool, 1)
	cb := func(_ *nats.Msg) {
		if n := atomic.AddInt32(&count, 1); n == total {
			ch <- true
		}
	}

	var servers []*Server
	servers = append(servers, s)

	seedURL := fmt.Sprintf("nats://%s:%d", opts.Cluster.Host, opts.Cluster.Port)
	for i := 0; i < numRoutes; i++ {
		ropts := DefaultOptions()
		ropts.Routes = RoutesFromStr(seedURL)
		rs := RunServer(ropts)
		defer rs.Shutdown()
		servers = append(servers, rs)

		// Create a sub on each routed server
		nc := natsConnect(t, fmt.Sprintf("nats://%s:%d", ropts.Host, ropts.Port))
		defer nc.Close()
		natsSub(t, nc, "foo", cb)
	}
	checkClusterFormed(t, servers...)

	// Make sure all subs are registered in s.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if ts := s.globalAccount().TotalSubs() - 3; ts != int(numRoutes) {
			return fmt.Errorf("Not all %d routed subs were registered: %d", numRoutes, ts)
		}
		return nil
	})

	pubNC := natsConnect(t, fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	defer pubNC.Close()
	natsPub(t, pubNC, "foo", []byte("hello world!"))

	waitCh(t, ch, "Did not get all messages")
}

func TestRouteRTT(t *testing.T) {
	ob := DefaultOptions()
	ob.PingInterval = 15 * time.Millisecond
	sb := RunServer(ob)
	defer sb.Shutdown()

	oa := DefaultOptions()
	oa.PingInterval = 15 * time.Millisecond
	oa.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", ob.Cluster.Host, ob.Cluster.Port))
	sa := RunServer(oa)
	defer sa.Shutdown()

	checkClusterFormed(t, sa, sb)

	checkRTT := func(t *testing.T, s *Server) time.Duration {
		t.Helper()
		var route *client
		s.mu.Lock()
		for _, r := range s.routes {
			route = r
			break
		}
		s.mu.Unlock()

		var rtt time.Duration
		checkFor(t, 2*firstPingInterval, 15*time.Millisecond, func() error {
			route.mu.Lock()
			rtt = route.rtt
			route.mu.Unlock()
			if rtt == 0 {
				return fmt.Errorf("RTT not tracked")
			}
			return nil
		})
		return rtt
	}

	prevA := checkRTT(t, sa)
	prevB := checkRTT(t, sb)

	checkUpdated := func(t *testing.T, s *Server, prev time.Duration) {
		t.Helper()
		attempts := 0
		timeout := time.Now().Add(2 * firstPingInterval)
		for time.Now().Before(timeout) {
			if rtt := checkRTT(t, s); rtt != 0 {
				return
			}
			attempts++
			if attempts == 5 {
				// If could be that we are very unlucky
				// and the RTT is constant. So override
				// the route's RTT to 0 to see if it gets
				// updated.
				s.mu.Lock()
				for _, r := range s.routes {
					r.mu.Lock()
					r.rtt = 0
					r.mu.Unlock()
					break
				}
				s.mu.Unlock()
			}
			time.Sleep(15 * time.Millisecond)
		}
		t.Fatalf("RTT probably not updated")
	}
	checkUpdated(t, sa, prevA)
	checkUpdated(t, sb, prevB)

	sa.Shutdown()
	sb.Shutdown()

	// Now check that initial RTT is computed prior to first PingInterval
	// Get new options to avoid possible race changing the ping interval.
	ob = DefaultOptions()
	ob.PingInterval = time.Minute
	sb = RunServer(ob)
	defer sb.Shutdown()

	oa = DefaultOptions()
	oa.PingInterval = time.Minute
	oa.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", ob.Cluster.Host, ob.Cluster.Port))
	sa = RunServer(oa)
	defer sa.Shutdown()

	checkClusterFormed(t, sa, sb)
	checkRTT(t, sa)
	checkRTT(t, sb)
}

func TestRouteCloseTLSConnection(t *testing.T) {
	opts := DefaultOptions()
	opts.DisableShortFirstPing = true
	opts.Cluster.Name = "A"
	opts.Cluster.Host = "127.0.0.1"
	opts.Cluster.Port = -1
	opts.Cluster.TLSTimeout = 100
	tc := &TLSConfigOpts{
		CertFile: "./configs/certs/server.pem",
		KeyFile:  "./configs/certs/key.pem",
		Insecure: true,
	}
	tlsConf, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating tls config: %v", err)
	}
	opts.Cluster.TLSConfig = tlsConf
	opts.NoLog = true
	opts.NoSigs = true
	s := RunServer(opts)
	defer s.Shutdown()

	endpoint := fmt.Sprintf("%s:%d", opts.Cluster.Host, opts.Cluster.Port)
	conn, err := net.DialTimeout("tcp", endpoint, 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on dial: %v", err)
	}
	defer conn.Close()

	tlsConn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
	defer tlsConn.Close()
	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("Unexpected error during handshake: %v", err)
	}
	connectOp := []byte("CONNECT {\"name\":\"route\",\"verbose\":false,\"pedantic\":false,\"tls_required\":true,\"cluster\":\"A\"}\r\n")
	if _, err := tlsConn.Write(connectOp); err != nil {
		t.Fatalf("Unexpected error writing CONNECT: %v", err)
	}
	infoOp := []byte("INFO {\"server_id\":\"route\",\"tls_required\":true}\r\n")
	if _, err := tlsConn.Write(infoOp); err != nil {
		t.Fatalf("Unexpected error writing CONNECT: %v", err)
	}
	if _, err := tlsConn.Write([]byte("PING\r\n")); err != nil {
		t.Fatalf("Unexpected error writing PING: %v", err)
	}

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if s.NumRoutes() != 1 {
			return fmt.Errorf("No route registered yet")
		}
		return nil
	})

	// Get route connection
	var route *client
	s.mu.Lock()
	for _, r := range s.routes {
		route = r
		break
	}
	s.mu.Unlock()
	// Fill the buffer. We want to timeout on write so that nc.Close()
	// would block due to a write that cannot complete.
	buf := make([]byte, 64*1024)
	done := false
	for !done {
		route.nc.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err := route.nc.Write(buf); err != nil {
			done = true
		}
		route.nc.SetWriteDeadline(time.Time{})
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
	// Close the route
	route.closeConnection(SlowConsumerWriteDeadline)
	ch <- true
}

func TestRouteClusterNameConflictBetweenStaticAndDynamic(t *testing.T) {
	o1 := DefaultOptions()
	o1.Cluster.Name = "AAAAAAAAAAAAAAAAAAAA" // make it alphabetically the "smallest"
	s1 := RunServer(o1)
	defer s1.Shutdown()

	o2 := DefaultOptions()
	o2.Cluster.Name = "" // intentional, let it be assigned dynamically
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)
}

type testRouteResolver struct{}

func (r *testRouteResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	return []string{"127.0.0.1", "other.host.in.cluster"}, nil
}

type routeHostLookupLogger struct {
	DummyLogger
	errCh chan string
	ch    chan bool
	count int
}

func (l *routeHostLookupLogger) Debugf(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "127.0.0.1:1234") {
		l.errCh <- msg
	} else if strings.Contains(msg, "other.host.in.cluster") {
		if l.count++; l.count == 10 {
			l.ch <- true
		}
	}
}

func TestRouteIPResolutionAndRouteToSelf(t *testing.T) {
	o := DefaultOptions()
	o.Cluster.Port = 1234
	r := &testRouteResolver{}
	o.Cluster.resolver = r
	o.Routes = RoutesFromStr("nats://routehost:1234")
	o.Debug = true
	o.NoLog = false
	s, err := NewServer(o)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	defer s.Shutdown()
	l := &routeHostLookupLogger{errCh: make(chan string, 1), ch: make(chan bool, 1)}
	s.SetLogger(l, true, true)
	go func() {
		s.Start()
	}()
	if err := s.readyForConnections(time.Second); err != nil {
		t.Fatal(err)
	}

	select {
	case e := <-l.errCh:
		t.Fatalf("Unexpected trace: %q", e)
	case <-l.ch:
		// Ok
		return
	}
}

func TestRouteDuplicateServerName(t *testing.T) {
	o := DefaultOptions()
	o.ServerName = "A"
	s := RunServer(o)
	defer s.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 1)}
	s.SetLogger(l, false, false)

	o2 := DefaultOptions()
	// Set the same server name on purpose
	o2.ServerName = "A"
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	// This is an error now so can't wait on cluster formed.
	select {
	case w := <-l.errCh:
		if !strings.Contains(w, "Remote server has a duplicate name") {
			t.Fatalf("Expected warning about same name, got %q", w)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Should have gotten a warning regarding duplicate server name")
	}
}

func TestRouteLockReleasedOnTLSFailure(t *testing.T) {
	o1 := DefaultOptions()
	o1.Cluster.Name = "abc"
	o1.Cluster.Host = "127.0.0.1"
	o1.Cluster.Port = -1
	o1.Cluster.TLSTimeout = 0.25
	tc := &TLSConfigOpts{
		CertFile: "./configs/certs/server.pem",
		KeyFile:  "./configs/certs/key.pem",
		Insecure: true,
	}
	tlsConf, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating tls config: %v", err)
	}
	o1.Cluster.TLSConfig = tlsConf
	s1 := RunServer(o1)
	defer s1.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s1.SetLogger(l, false, false)

	o2 := DefaultOptions()
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	select {
	case err := <-l.errCh:
		if !strings.Contains(err, "TLS") {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(time.Second):
	}

	s2.Shutdown()

	// Wait for longer than the TLS timeout and check that tlsTimeout is not stuck
	time.Sleep(500 * time.Millisecond)

	buf := make([]byte, 10000)
	n := runtime.Stack(buf, true)
	if bytes.Contains(buf[:n], []byte("tlsTimeout")) {
		t.Fatal("Seem connection lock was not released")
	}
}

type localhostResolver struct{}

func (r *localhostResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	return []string{"127.0.0.1"}, nil
}

func TestTLSRoutesCertificateImplicitAllowPass(t *testing.T) {
	testTLSRoutesCertificateImplicitAllow(t, true)
}

func TestTLSRoutesCertificateImplicitAllowFail(t *testing.T) {
	testTLSRoutesCertificateImplicitAllow(t, false)
}

func testTLSRoutesCertificateImplicitAllow(t *testing.T, pass bool) {
	// Base config for the servers
	cfg := createFile(t, "cfg")
	defer removeFile(t, cfg.Name())
	cfg.WriteString(fmt.Sprintf(`
		cluster {
		  tls {
			cert_file = "../test/configs/certs/tlsauth/server.pem"
			key_file = "../test/configs/certs/tlsauth/server-key.pem"
			ca_file = "../test/configs/certs/tlsauth/ca.pem"
			verify_cert_and_check_known_urls = true
			insecure = %t
			timeout = 1
		  }
		}
	`, !pass)) // set insecure to skip verification on the outgoing end
	if err := cfg.Sync(); err != nil {
		t.Fatal(err)
	}
	cfg.Close()

	optsA := LoadConfig(cfg.Name())
	optsB := LoadConfig(cfg.Name())

	routeURLs := "nats://localhost:9935, nats://localhost:9936"
	if !pass {
		routeURLs = "nats://127.0.0.1:9935, nats://127.0.0.1:9936"
	}
	optsA.Host = "127.0.0.1"
	optsA.Port = 9335
	optsA.Cluster.Name = "xyz"
	optsA.Cluster.Host = optsA.Host
	optsA.Cluster.Port = 9935
	optsA.Cluster.resolver = &localhostResolver{}
	optsA.Routes = RoutesFromStr(routeURLs)
	optsA.NoSystemAccount = true
	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	optsB.Host = "127.0.0.1"
	optsB.Port = 9336
	optsB.Cluster.Name = "xyz"
	optsB.Cluster.Host = optsB.Host
	optsB.Cluster.Port = 9936
	optsB.Cluster.resolver = &localhostResolver{}
	optsB.Routes = RoutesFromStr(routeURLs)
	optsB.NoSystemAccount = true
	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	if pass {
		checkNumRoutes(t, srvA, 1)
		checkNumRoutes(t, srvB, 1)
	} else {
		time.Sleep(1 * time.Second) // the fail case uses the IP, so a short wait is sufficient
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			if srvA.NumRoutes() != 0 || srvB.NumRoutes() != 0 {
				return fmt.Errorf("No route connection expected")
			}
			return nil
		})
	}
}
