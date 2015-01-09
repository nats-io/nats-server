// Copyright 2013-2014 Apcera Inc. All rights reserved.

package test

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/apcera/gnatsd/server"
	"github.com/apcera/nats"
)

func runServers(t *testing.T) (srvA, srvB *server.Server, optsA, optsB *server.Options) {
	optsA, _ = server.ProcessConfigFile("./configs/srv_a.conf")
	optsB, _ = server.ProcessConfigFile("./configs/srv_b.conf")

	optsA.NoSigs, optsA.NoLog = true, true
	optsB.NoSigs, optsB.NoLog = true, true

	srvA = RunServer(optsA)
	srvB = RunServer(optsB)
	return
}

func TestProperServerWithRoutesShutdown(t *testing.T) {
	before := runtime.NumGoroutine()
	srvA, srvB, _, _ := runServers(t)
	time.Sleep(100 * time.Millisecond)
	srvA.Shutdown()
	srvB.Shutdown()
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	delta := after - before
	// There may be some finalizers or IO, but in general more than
	// 2 as a delta represents a problem.
	if delta > 2 {
		t.Fatalf("Expected same number of goroutines, %d vs %d\n", before, after)
	}
}

func TestDoubleRouteConfig(t *testing.T) {
	srvA, srvB, _, _ := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	// Wait for the setup
	time.Sleep(1 * time.Second)

	if numRoutesA := srvA.NumRoutes(); numRoutesA != 1 {
		t.Fatalf("Expected one route for srvA, got %d\n", numRoutesA)
	}
	if numRoutesB := srvB.NumRoutes(); numRoutesB != 1 {
		t.Fatalf("Expected one route for srvB, got %d\n", numRoutesB)
	}
}

func TestBasicClusterPubSub(t *testing.T) {
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	clientB := createClientConn(t, optsB.Host, optsB.Port)
	defer clientB.Close()

	sendA, expectA := setupConn(t, clientA)
	sendA("SUB foo 22\r\n")
	sendA("PING\r\n")
	expectA(pongRe)

	sendB, expectB := setupConn(t, clientB)
	sendB("PUB foo 2\r\nok\r\n")
	sendB("PING\r\n")
	expectB(pongRe)

	expectMsgs := expectMsgsCommand(t, expectA)

	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "22", "", "2", "ok")
}

func TestClusterQueueSubs(t *testing.T) {
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	clientB := createClientConn(t, optsB.Host, optsB.Port)
	defer clientB.Close()

	sendA, expectA := setupConn(t, clientA)
	sendB, expectB := setupConn(t, clientB)

	expectMsgsA := expectMsgsCommand(t, expectA)
	expectMsgsB := expectMsgsCommand(t, expectB)

	// Capture sids for checking later.
	qg1Sids_a := []string{"1", "2", "3"}

	// Three queue subscribers
	for _, sid := range qg1Sids_a {
		sendA(fmt.Sprintf("SUB foo qg1 %s\r\n", sid))
	}
	sendA("PING\r\n")
	expectA(pongRe)

	sendB("PUB foo 2\r\nok\r\n")
	sendB("PING\r\n")
	expectB(pongRe)

	// Make sure we get only 1.
	matches := expectMsgsA(1)
	checkMsg(t, matches[0], "foo", "", "", "2", "ok")

	// Capture sids for checking later.
	pSids := []string{"4", "5", "6"}

	// Create 3 normal subscribers
	for _, sid := range pSids {
		sendA(fmt.Sprintf("SUB foo %s\r\n", sid))
	}

	// Create a FWC Subscriber
	pSids = append(pSids, "7")
	sendA("SUB > 7\r\n")
	sendA("PING\r\n")
	expectA(pongRe)

	// Send to B
	sendB("PUB foo 2\r\nok\r\n")
	sendB("PING\r\n")
	expectB(pongRe)

	// Should receive 5.
	matches = expectMsgsA(5)
	checkForQueueSid(t, matches, qg1Sids_a)
	checkForPubSids(t, matches, pSids)

	// Send to A
	sendA("PUB foo 2\r\nok\r\n")

	// Should receive 5.
	matches = expectMsgsA(5)
	checkForQueueSid(t, matches, qg1Sids_a)
	checkForPubSids(t, matches, pSids)

	// Now add queue subscribers to B
	qg2Sids_b := []string{"1", "2", "3"}
	for _, sid := range qg2Sids_b {
		sendB(fmt.Sprintf("SUB foo qg2 %s\r\n", sid))
	}
	sendB("PING\r\n")
	expectB(pongRe)

	// Send to B
	sendB("PUB foo 2\r\nok\r\n")

	// Should receive 1 from B.
	matches = expectMsgsB(1)
	checkForQueueSid(t, matches, qg2Sids_b)

	// Should receive 5 still from A.
	matches = expectMsgsA(5)
	checkForQueueSid(t, matches, qg1Sids_a)
	checkForPubSids(t, matches, pSids)

	// Now drop queue subscribers from A
	for _, sid := range qg1Sids_a {
		sendA(fmt.Sprintf("UNSUB %s\r\n", sid))
	}
	sendA("PING\r\n")
	expectA(pongRe)

	// Send to B
	sendB("PUB foo 2\r\nok\r\n")

	// Should receive 1 from B.
	matches = expectMsgsB(1)
	checkForQueueSid(t, matches, qg2Sids_b)

	sendB("PING\r\n")
	expectB(pongRe)

	// Should receive 4 now.
	matches = expectMsgsA(4)
	checkForPubSids(t, matches, pSids)

	// Send to A
	sendA("PUB foo 2\r\nok\r\n")

	// Should receive 4 now.
	matches = expectMsgsA(4)
	checkForPubSids(t, matches, pSids)
}

// Issue #22
func TestClusterDoubleMsgs(t *testing.T) {
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	clientA1 := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA1.Close()

	clientA2 := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA2.Close()

	clientB := createClientConn(t, optsB.Host, optsB.Port)
	defer clientB.Close()

	sendA1, expectA1 := setupConn(t, clientA1)
	sendA2, expectA2 := setupConn(t, clientA2)
	sendB, expectB := setupConn(t, clientB)

	expectMsgsA1 := expectMsgsCommand(t, expectA1)
	expectMsgsA2 := expectMsgsCommand(t, expectA2)

	// Capture sids for checking later.
	qg1Sids_a := []string{"1", "2", "3"}

	// Three queue subscribers
	for _, sid := range qg1Sids_a {
		sendA1(fmt.Sprintf("SUB foo qg1 %s\r\n", sid))
	}
	sendA1("PING\r\n")
	expectA1(pongRe)

	sendB("PUB foo 2\r\nok\r\n")
	sendB("PING\r\n")
	expectB(pongRe)

	// Make sure we get only 1.
	matches := expectMsgsA1(1)
	checkMsg(t, matches[0], "foo", "", "", "2", "ok")
	checkForQueueSid(t, matches, qg1Sids_a)

	// Add a FWC subscriber on A2
	sendA2("SUB > 1\r\n")
	sendA2("SUB foo 2\r\n")
	sendA2("PING\r\n")
	expectA2(pongRe)
	pSids := []string{"1", "2"}

	sendB("PUB foo 2\r\nok\r\n")
	sendB("PING\r\n")
	expectB(pongRe)

	matches = expectMsgsA1(1)
	checkMsg(t, matches[0], "foo", "", "", "2", "ok")
	checkForQueueSid(t, matches, qg1Sids_a)

	matches = expectMsgsA2(2)
	checkMsg(t, matches[0], "foo", "", "", "2", "ok")
	checkForPubSids(t, matches, pSids)

	// Close ClientA1
	clientA1.Close()

	sendB("PUB foo 2\r\nok\r\n")
	sendB("PING\r\n")
	expectB(pongRe)

	matches = expectMsgsA2(2)
	checkMsg(t, matches[0], "foo", "", "", "2", "ok")
	checkForPubSids(t, matches, pSids)
}

// This will test that we drop remote sids correctly.
func TestClusterDropsRemoteSids(t *testing.T) {
	srvA, srvB, optsA, _ := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	sendA, expectA := setupConn(t, clientA)

	// Add a subscription
	sendA("SUB foo 1\r\n")
	sendA("PING\r\n")
	expectA(pongRe)

	// Wait for propogation.
	time.Sleep(100 * time.Millisecond)

	if sc := srvA.NumSubscriptions(); sc != 1 {
		t.Fatalf("Expected one subscription for srvA, got %d\n", sc)
	}
	if sc := srvB.NumSubscriptions(); sc != 1 {
		t.Fatalf("Expected one subscription for srvB, got %d\n", sc)
	}

	// Add another subscription
	sendA("SUB bar 2\r\n")
	sendA("PING\r\n")
	expectA(pongRe)

	// Wait for propogation.
	time.Sleep(100 * time.Millisecond)

	if sc := srvA.NumSubscriptions(); sc != 2 {
		t.Fatalf("Expected two subscriptions for srvA, got %d\n", sc)
	}
	if sc := srvB.NumSubscriptions(); sc != 2 {
		t.Fatalf("Expected two subscriptions for srvB, got %d\n", sc)
	}

	// unsubscription
	sendA("UNSUB 1\r\n")
	sendA("PING\r\n")
	expectA(pongRe)

	// Wait for propogation.
	time.Sleep(100 * time.Millisecond)

	if sc := srvA.NumSubscriptions(); sc != 1 {
		t.Fatalf("Expected one subscription for srvA, got %d\n", sc)
	}
	if sc := srvB.NumSubscriptions(); sc != 1 {
		t.Fatalf("Expected one subscription for srvB, got %d\n", sc)
	}

	// Close the client and make sure we remove subscription state.
	clientA.Close()

	// Wait for propogation.
	time.Sleep(100 * time.Millisecond)
	if sc := srvA.NumSubscriptions(); sc != 0 {
		t.Fatalf("Expected no subscriptions for srvA, got %d\n", sc)
	}
	if sc := srvB.NumSubscriptions(); sc != 0 {
		t.Fatalf("Expected no subscriptions for srvB, got %d\n", sc)
	}
}

// This will test that we drop remote sids correctly.
func TestAutoUnsubscribePropogation(t *testing.T) {
	srvA, srvB, optsA, _ := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	sendA, expectA := setupConn(t, clientA)
	expectMsgs := expectMsgsCommand(t, expectA)

	// We will create subscriptions that will auto-unsubscribe and make sure
	// we are not accumulating orphan subscriptions on the other side.
	for i := 1; i <= 100; i++ {
		sub := fmt.Sprintf("SUB foo %d\r\n", i)
		auto := fmt.Sprintf("UNSUB %d 1\r\n", i)
		sendA(sub)
		sendA(auto)
		// This will trip the auto-unsubscribe
		sendA("PUB foo 2\r\nok\r\n")
		expectMsgs(1)
	}

	sendA("PING\r\n")
	expectA(pongRe)

	// Make sure number of subscriptions on B is correct
	if subs := srvB.NumSubscriptions(); subs != 0 {
		t.Fatalf("Expected no subscriptions on remote server, got %d\n", subs)
	}
}

func TestPubSubInRouteMode(t *testing.T) {
	servs := setupCluster(t)
	testPubSub(t, servs)
	tearDownServers(t, servs)
}

func TestPubSubInStandalone(t *testing.T) {
	servs := setupStandalone(t)
	testPubSub(t, servs)
	tearDownServers(t, servs)
}

const (
	host         = "127.0.0.1"
	natHost      = "nats://127.0.0.1"
	natRouteHost = "nats-route://127.0.0.1"
)

type GNATSDPort struct {
	client int //client port
	route  int //route port
}

// setupCluster setups a gnatsd cluster
// supposed we use the following configuration
// serv1: client port: 14222, route port: 14322
// serv2: client port: 14223, route port: 14323
// serv3: client port: 14224, route port: 14324
func setupCluster(t *testing.T) []*server.Server {
	ports := []GNATSDPort{
		GNATSDPort{
			client: 14222,
			route:  14322,
		},
		GNATSDPort{
			client: 14223,
			route:  14323,
		},
		GNATSDPort{
			client: 14224,
			route:  14324,
		},
	}

	servs, _ := RunRouteServerWithPorts(t, ports)
	return servs
}

func RunRouteServerWithPorts(
	t *testing.T,
	ports []GNATSDPort) ([]*server.Server, []*server.Options) {

	var servs []*server.Server
	var opts []*server.Options

	for i, p := range ports {

		// generate route address
		var routes []*url.URL
		for j, rp := range ports {
			if j == i {
				continue // skip myself
			}
			url, err := url.Parse(fmt.Sprintf("%s:%d", natRouteHost, rp.route))
			if err != nil {
				t.Fatalf("parse url fail, err:%v", err)
			}
			routes = append(routes, url)
		}

		fmt.Printf("route:%v\n", routes)
		opt := &server.Options{
			// NOTE: we cannot specify host in route mode
			//Host:        host,
			Port:        p.client,
			Trace:       true, // enable trace
			Debug:       true, // enable debug
			ClusterHost: host,
			ClusterPort: p.route,
			Routes:      routes,
			LogFile:     filepath.Join(os.TempDir(), fmt.Sprintf("gnatsd-%d.log", i)),
		}

		opts = append(opts, opt)
		// wakeup servers one-by-one
		servs = append(servs, RunServer(opt))
	}

	return servs, opts
}

// setupCluster setups a standalone gnatsd
func setupStandalone(t *testing.T) []*server.Server {
	opts := DefaultTestOptions
	opts.Port = server.RANDOM_PORT
	return []*server.Server{RunServer(&opts)}
}

func tearDownServers(t *testing.T, servs []*server.Server) {
	for _, v := range servs {
		v.Shutdown()
	}
}

func encodedConn(addrs []string) (*nats.EncodedConn, error) {
	opts := nats.DefaultOptions
	opts.Servers = addrs

	nc, err := opts.Connect()
	if err != nil {
		return nil, err
	}
	return nats.NewEncodedConn(nc, "json")
}

func testPubSub(t *testing.T, servs []*server.Server) {
	// get all server's address
	var addrs []string
	for _, s := range servs {
		_, port, _ := net.SplitHostPort(s.Addr().String())
		addrs = append(addrs, fmt.Sprintf("%s:%s", natHost, port))
	}

	fmt.Println(addrs)
	pubSubTest(t, addrs)
}

func pubSubTest(t *testing.T, addrs []string) {

	numPublisher := 1000
	numSubscriptor := 3
	numMessage := 10
	topic := "pubsubtest"

	wg := sync.WaitGroup{}

	nc, err := encodedConn(addrs)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	publisher := func(id int, nc *nats.EncodedConn, topic string) {
		defer wg.Done()

		ch := make(chan interface{})
		if err := nc.BindSendChan(topic, ch); err != nil {
			t.Fatal(err)
		}

		for n := 0; n < numMessage; n++ {
			ch <- fmt.Sprintf("msg:%d:%d", id, n)
		}
		close(ch)
	}

	subscriptor := func(id int, nc *nats.EncodedConn, topic string) {
		defer wg.Done()

		ch := make(chan interface{})
		sub, err := nc.BindRecvChan(topic, ch)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			close(ch)
			sub.Unsubscribe()
		}()

		// push a defer to check the result
		var recvMsg []string
		defer func() {
			exp := numPublisher * numMessage
			fmt.Printf("exp:%v, got:%v\n", exp, len(recvMsg))
			if exp != len(recvMsg) {
				t.Fatalf("exp:%v, got:%v\n", exp, len(recvMsg))
			}
		}()

		for {
			select {
			case msg, more := <-ch:
				if !more {
					return
				}
				recvMsg = append(recvMsg, msg.(string))
			case <-time.After(10 * time.Second):
				// if no further message, return
				return
			}
		}
	}

	wg.Add(numSubscriptor)
	for i := 0; i < numSubscriptor; i++ {
		go subscriptor(i, nc, topic)
	}

	time.Sleep(1 * time.Second)

	wg.Add(numPublisher)
	for i := 0; i < numPublisher; i++ {
		go publisher(i, nc, topic)
	}

	wg.Wait()

}
