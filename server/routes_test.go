// Copyright 2013-2016 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"net"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/go-nats"
)

func TestRouteConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/cluster.conf")
	if err != nil {
		t.Fatalf("Received an error reading route config file: %v\n", err)
	}

	golden := &Options{
		ConfigFile:  "./configs/cluster.conf",
		Host:        "localhost",
		Port:        4242,
		Username:    "derek",
		Password:    "bella",
		AuthTimeout: 1.0,
		Cluster: ClusterOpts{
			Host:           "127.0.0.1",
			Port:           4244,
			Username:       "route_user",
			Password:       "top_secret",
			AuthTimeout:    1.0,
			NoAdvertise:    true,
			ConnectRetries: 2,
		},
		PidFile: "/tmp/nats_cluster_test.pid",
	}

	// Setup URLs
	r1, _ := url.Parse("nats-route://foo:bar@localhost:4245")
	r2, _ := url.Parse("nats-route://foo:bar@localhost:4246")

	golden.Routes = []*url.URL{r1, r2}

	if !reflect.DeepEqual(golden, opts) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}
}

func TestServerRoutesWithClients(t *testing.T) {
	optsA, _ := ProcessConfigFile("./configs/srv_a.conf")
	optsB, _ := ProcessConfigFile("./configs/srv_b.conf")

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
	optsA, _ := ProcessConfigFile("./configs/srv_a_bcrypt.conf")
	optsB, _ := ProcessConfigFile("./configs/srv_b_bcrypt.conf")

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
	// Wait for the cluster to form
	var err string
	expectedNumRoutes := len(servers) - 1
	maxTime := time.Now().Add(10 * time.Second)
	for time.Now().Before(maxTime) {
		err = ""
		for _, s := range servers {
			if numRoutes := s.NumRoutes(); numRoutes != expectedNumRoutes {
				err = fmt.Sprintf("Expected %d routes for server %q, got %d", expectedNumRoutes, s.ID(), numRoutes)
				break
			}
		}
		if err != "" {
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}
	if err != "" {
		stackFatalf(t, "%s", err)
	}
}

// Helper function to generate next opts to make sure no port conflicts etc.
func nextServerOpts(opts *Options) *Options {
	nopts := *opts
	nopts.Port = -1
	nopts.Cluster.Port = -1
	nopts.HTTPPort = -1
	return &nopts
}

func TestSeedSolicitWorks(t *testing.T) {
	optsSeed, _ := ProcessConfigFile("./configs/seed.conf")

	optsSeed.NoSigs, optsSeed.NoLog = true, true

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

	nc2.Publish("foo", []byte("Hello"))

	// Wait for message
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message across route")
	}
}

func TestTLSSeedSolicitWorks(t *testing.T) {
	optsSeed, _ := ProcessConfigFile("./configs/seed_tls.conf")

	optsSeed.NoSigs, optsSeed.NoLog = true, true

	srvSeed := RunServer(optsSeed)
	defer srvSeed.Shutdown()

	seedRouteUrl := fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host,
		srvSeed.ClusterAddr().Port)
	optsA := nextServerOpts(optsSeed)
	optsA.Routes = RoutesFromStr(seedRouteUrl)

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
	optsB.Routes = RoutesFromStr(seedRouteUrl)

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, srvB.Addr().(*net.TCPAddr).Port)

	nc2, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()

	checkClusterFormed(t, srvSeed, srvA, srvB)

	nc2.Publish("foo", []byte("Hello"))

	// Wait for message
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message across route")
	}
}

func TestChainedSolicitWorks(t *testing.T) {
	optsSeed, _ := ProcessConfigFile("./configs/seed.conf")

	optsSeed.NoSigs, optsSeed.NoLog = true, true

	srvSeed := RunServer(optsSeed)
	defer srvSeed.Shutdown()

	seedRouteUrl := fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host,
		srvSeed.ClusterAddr().Port)
	optsA := nextServerOpts(optsSeed)
	optsA.Routes = RoutesFromStr(seedRouteUrl)

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

	nc2.Publish("foo", []byte("Hello"))

	// Wait for message
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message across route")
	}
}

func TestTLSChainedSolicitWorks(t *testing.T) {
	optsSeed, _ := ProcessConfigFile("./configs/seed_tls.conf")

	optsSeed.NoSigs, optsSeed.NoLog = true, true

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

	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, srvB.Addr().(*net.TCPAddr).Port)

	nc2, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()

	checkClusterFormed(t, srvSeed, srvA, srvB)

	nc2.Publish("foo", []byte("Hello"))

	// Wait for message
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message across route")
	}
}

func TestRouteTLSHandshakeError(t *testing.T) {
	optsSeed, _ := ProcessConfigFile("./configs/seed_tls.conf")
	optsSeed.NoLog = true
	srvSeed := RunServer(optsSeed)
	defer srvSeed.Shutdown()

	opts := DefaultOptions()
	opts.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host, optsSeed.Cluster.Port))

	srv := RunServer(opts)
	defer srv.Shutdown()

	time.Sleep(500 * time.Millisecond)

	maxTime := time.Now().Add(1 * time.Second)
	for time.Now().Before(maxTime) {
		if srv.NumRoutes() > 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
	if srv.NumRoutes() > 0 {
		t.Fatal("Route should have failed")
	}
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
	// cluster's Host to localhost so it works on Windows too, since on
	// Windows, a client can't use 0.0.0.0 in a connect.
	opts.Cluster.Host = "localhost"
	opts.Cluster.NoAdvertise = true
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
}

type checkDuplicateRouteLogger struct {
	sync.Mutex
	gotDuplicate bool
}

func (l *checkDuplicateRouteLogger) Noticef(format string, v ...interface{}) {}
func (l *checkDuplicateRouteLogger) Errorf(format string, v ...interface{})  {}
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
