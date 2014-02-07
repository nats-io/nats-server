// Copyright 2013-2014 Apcera Inc. All rights reserved.

package test

import (
"fmt"
	"testing"
	"time"

	"github.com/apcera/gnatsd/server"
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

//	time.Sleep(time.Second)

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
	time.Sleep(50 * time.Millisecond)

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
	time.Sleep(50 * time.Millisecond)

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
	time.Sleep(50 * time.Millisecond)

	if sc := srvA.NumSubscriptions(); sc != 1 {
		t.Fatalf("Expected one subscription for srvA, got %d\n", sc)
	}
	if sc := srvB.NumSubscriptions(); sc != 1 {
		t.Fatalf("Expected one subscription for srvB, got %d\n", sc)
	}

	// Close the client and make sure we remove subscription state.
	clientA.Close()

	// Wait for propogation.
	time.Sleep(50 * time.Millisecond)
	if sc := srvA.NumSubscriptions(); sc != 0 {
		t.Fatalf("Expected no subscriptions for srvA, got %d\n", sc)
	}
	if sc := srvB.NumSubscriptions(); sc != 0 {
		t.Fatalf("Expected no subscriptions for srvB, got %d\n", sc)
	}
}
