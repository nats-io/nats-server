// Copyright 2012 Apcera Inc. All rights reserved.

package test

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/apcera/gnatsd/server"
)

func runRouteServer(t *testing.T) (*server.Server, *server.Options) {
	opts, err := server.ProcessConfigFile("./configs/cluster.conf")

	// Override for running in Go routine.
	opts.NoSigs = true
	// opts.Debug  = true
	// opts.Trace  = true
	opts.NoLog = true

	if err != nil {
		t.Fatalf("Error parsing config file: %v\n", err)
	}
	return RunServer(opts), opts
}

func TestRouterListeningSocket(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	// Check that the cluster socket is able to be connected.
	addr := fmt.Sprintf("%s:%d", opts.ClusterHost, opts.ClusterPort)
	checkSocket(t, addr, 2*time.Second)
}

func TestRouteGoServerShutdown(t *testing.T) {
	base := runtime.NumGoroutine()
	s, _ := runRouteServer(t)
	s.Shutdown()
	time.Sleep(10 * time.Millisecond)
	delta := (runtime.NumGoroutine() - base)
	if delta > 1 {
		t.Fatalf("%d Go routines still exist post Shutdown()", delta)
	}
}

func TestSendRouteInfoOnConnect(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()
	rc := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	_, expect := setupRoute(t, rc, opts)
	buf := expect(infoRe)

	info := server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}

	if !info.AuthRequired {
		t.Fatal("Expected to see AuthRequired")
	}
	if info.Port != opts.ClusterPort {
		t.Fatalf("Received wrong information for port, expected %d, got %d",
			info.Port, opts.ClusterPort)
	}
}

func TestSendRouteSubAndUnsub(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, _ := setupConn(t, c)

	// We connect to the route.
	rc := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	expectAuthRequired(t, rc)
	setupRoute(t, rc, opts)

	// Send SUB via client connection
	send("SUB foo 22\r\n")

	// Make sure the SUB is broadcast via the route
	buf := expectResult(t, rc, subRe)
	matches := subRe.FindAllSubmatch(buf, -1)
	rsid := string(matches[0][5])
	if !strings.HasPrefix(rsid, "RSID:") {
		t.Fatalf("Got wrong RSID: %s\n", rsid)
	}

	// Send UNSUB via client connection
	send("UNSUB 22\r\n")

	// Make sure the SUB is broadcast via the route
	buf = expectResult(t, rc, unsubRe)
	matches = unsubRe.FindAllSubmatch(buf, -1)
	rsid2 := string(matches[0][1])

	if rsid2 != rsid {
		t.Fatalf("Expected rsid's to match. %q vs %q\n", rsid, rsid2)
	}
}

func TestSendRouteSolicit(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	// Listen for a connection from the server on the first route.
	if len(opts.Routes) <= 0 {
		t.Fatalf("Need an outbound solicted route for this test")
	}
	rUrl := opts.Routes[0]

	conn := acceptRouteConn(t, rUrl.Host, server.DEFAULT_ROUTE_CONNECT)
	defer conn.Close()

	// We should receive a connect message right away due to auth.
	buf := expectResult(t, conn, connectRe)

	// Check INFO follows. Could be inline, with first result, if not
	// check followon buffer.
	if !inlineInfoRe.Match(buf) {
		expectResult(t, conn, infoRe)
	}
}

func TestRouteForwardsMsgFromClients(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	client := createClientConn(t, opts.Host, opts.Port)
	defer client.Close()

	clientSend, _ := setupConn(t, client)

	route := acceptRouteConn(t, opts.Routes[0].Host, server.DEFAULT_ROUTE_CONNECT)
	defer route.Close()

	routeSend, routeExpect := setupRoute(t, route, opts)
	expectMsgs := expectMsgsCommand(t, routeExpect)

	// Eat the CONNECT and INFO protos
	buf := routeExpect(connectRe)
	if !inlineInfoRe.Match(buf) {
		fmt.Printf("Looking for separate INFO\n")
		routeExpect(infoRe)
	}

	// Send SUB via route connection
	routeSend("SUB foo RSID:2:22\r\n")

	// Send PUB via client connection
	clientSend("PUB foo 2\r\nok\r\n")

	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "RSID:2:22", "", "2", "ok")
}

func TestRouteForwardsMsgToClients(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	client := createClientConn(t, opts.Host, opts.Port)
	defer client.Close()

	clientSend, clientExpect := setupConn(t, client)
	expectMsgs := expectMsgsCommand(t, clientExpect)

	route := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	expectAuthRequired(t, route)
	routeSend, _ := setupRoute(t, route, opts)

	// Subscribe to foo
	clientSend("SUB foo 1\r\n")
	// Use ping roundtrip to make sure its processed.
	clientSend("PING\r\n")
	clientExpect(pongRe)

	// Send MSG proto via route connection
	routeSend("MSG foo 1 2\r\nok\r\n")

	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "1", "", "2", "ok")
}

func TestRouteOneHopSemantics(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	route := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	expectAuthRequired(t, route)
	routeSend, _ := setupRoute(t, route, opts)

	// Express interest on this route for foo.
	routeSend("SUB foo RSID:2:2\r\n")

	// Send MSG proto via route connection
	routeSend("MSG foo 1 2\r\nok\r\n")

	// Make sure it does not come back!
	expectNothing(t, route)
}