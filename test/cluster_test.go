// Copyright 2013 Apcera Inc. All rights reserved.

package test

import (
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
