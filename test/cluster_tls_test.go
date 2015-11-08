// Copyright 2013-2015 Apcera Inc. All rights reserved.

package test

import (
	"testing"
	"time"

	"github.com/nats-io/gnatsd/server"
)

func runTLSServers(t *testing.T) (srvA, srvB *server.Server, optsA, optsB *server.Options) {
	srvA, optsA = RunServerWithConfig("./configs/srv_a_tls.conf")
	srvB, optsB = RunServerWithConfig("./configs/srv_b_tls.conf")
	return
}

func TestTLSClusterConfig(t *testing.T) {
	srvA, srvB, _, _ := runTLSServers(t)
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

func TestBasicTLSClusterPubSub(t *testing.T) {
	srvA, srvB, optsA, optsB := runTLSServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	// Wait for the setup
	time.Sleep(500 * time.Millisecond)

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
