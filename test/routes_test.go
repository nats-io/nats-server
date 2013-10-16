// Copyright 2012-2013 Apcera Inc. All rights reserved.

package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	opts.Debug = true
	opts.Trace = true
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
	routeSend, routeExpect := setupRoute(t, rc, opts)
	buf := routeExpect(infoRe)

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

	// Now send it back and make sure it is processed correctly inbound.
	routeSend(string(buf))
	routeSend("PING\r\n")
	routeExpect(pongRe)
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
	// check follow-on buffer.
	if !infoRe.Match(buf) {
		expectResult(t, conn, infoRe)
	}
}

func TestRouteForwardsMsgFromClients(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	client := createClientConn(t, opts.Host, opts.Port)
	defer client.Close()

	clientSend, clientExpect := setupConn(t, client)

	route := acceptRouteConn(t, opts.Routes[0].Host, server.DEFAULT_ROUTE_CONNECT)
	defer route.Close()

	routeSend, routeExpect := setupRoute(t, route, opts)
	expectMsgs := expectMsgsCommand(t, routeExpect)

	// Eat the CONNECT and INFO protos
	routeExpect(infoRe)

	// Send SUB via route connection
	routeSend("SUB foo RSID:2:22\r\n")
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Send PUB via client connection
	clientSend("PUB foo 2\r\nok\r\n")
	clientSend("PING\r\n")
	clientExpect(pongRe)

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

func TestRouteOnlySendOnce(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	client := createClientConn(t, opts.Host, opts.Port)
	defer client.Close()

	clientSend, clientExpect := setupConn(t, client)

	route := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	expectAuthRequired(t, route)
	routeSend, routeExpect := setupRoute(t, route, opts)
	expectMsgs := expectMsgsCommand(t, routeExpect)

	// Express multiple interest on this route for foo.
	routeSend("SUB foo RSID:2:1\r\n")
	routeSend("SUB foo RSID:2:2\r\n")
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Send PUB via client connection
	clientSend("PUB foo 2\r\nok\r\n")
	clientSend("PING\r\n")
	clientExpect(pongRe)

	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "RSID:2:1", "", "2", "ok")
}

func TestRouteQueueSemantics(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	client := createClientConn(t, opts.Host, opts.Port)
	clientSend, clientExpect := setupConn(t, client)
	clientExpectMsgs := expectMsgsCommand(t, clientExpect)

	defer client.Close()

	route := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	expectAuthRequired(t, route)
	routeSend, routeExpect := setupRoute(t, route, opts)
	expectMsgs := expectMsgsCommand(t, routeExpect)

	// Express multiple interest on this route for foo, queue group bar.
	qrsid1 := "RSID:2:1"
	routeSend(fmt.Sprintf("SUB foo bar %s\r\n", qrsid1))
	qrsid2 := "RSID:2:2"
	routeSend(fmt.Sprintf("SUB foo bar %s\r\n", qrsid2))

	// Use ping roundtrip to make sure its processed.
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Send PUB via client connection
	clientSend("PUB foo 2\r\nok\r\n")
	// Use ping roundtrip to make sure its processed.
	clientSend("PING\r\n")
	clientExpect(pongRe)

	// Only 1
	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "", "", "2", "ok")

	// Add normal Interest as well to route interest.
	routeSend("SUB foo RSID:2:4\r\n")

	// Use ping roundtrip to make sure its processed.
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Send PUB via client connection
	clientSend("PUB foo 2\r\nok\r\n")
	// Use ping roundtrip to make sure its processed.
	clientSend("PING\r\n")
	clientExpect(pongRe)

	// Should be 2 now, 1 for all normal, and one for specific queue subscriber.
	matches = expectMsgs(2)

	// Expect first to be the normal subscriber, next will be the queue one.
	checkMsg(t, matches[0], "foo", "RSID:2:4", "", "2", "ok")
	checkMsg(t, matches[1], "foo", "", "", "2", "ok")

	// Check the rsid to verify it is one of the queue group subscribers.
	rsid := string(matches[1][SID_INDEX])
	if rsid != qrsid1 && rsid != qrsid2 {
		t.Fatalf("Expected a queue group rsid, got %s\n", rsid)
	}

	// Now create a queue subscription for the client as well as a normal one.
	clientSend("SUB foo 1\r\n")
	// Use ping roundtrip to make sure its processed.
	clientSend("PING\r\n")
	clientExpect(pongRe)
	routeExpect(subRe)

	clientSend("SUB foo bar 2\r\n")
	// Use ping roundtrip to make sure its processed.
	clientSend("PING\r\n")
	clientExpect(pongRe)
	routeExpect(subRe)

	// Deliver a MSG from the route itself, make sure the client receives both.
	routeSend("MSG foo RSID:2:1 2\r\nok\r\n")
	// Queue group one.
	routeSend("MSG foo QRSID:2:2 2\r\nok\r\n")

	// Use ping roundtrip to make sure its processed.
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Should be 2 now, 1 for all normal, and one for specific queue subscriber.
	matches = clientExpectMsgs(2)
	// Expect first to be the normal subscriber, next will be the queue one.
	checkMsg(t, matches[0], "foo", "1", "", "2", "ok")
	checkMsg(t, matches[1], "foo", "2", "", "2", "ok")
}

func TestSolicitRouteReconnect(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	rUrl := opts.Routes[0]

	route := acceptRouteConn(t, rUrl.Host, server.DEFAULT_ROUTE_CONNECT)

	// Go ahead and close the Route.
	route.Close()

	// We expect to get called back..
	route = acceptRouteConn(t, rUrl.Host, 2*server.DEFAULT_ROUTE_CONNECT)
}

func TestMultipleRoutesSameId(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	route1 := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	expectAuthRequired(t, route1)
	route1Send, _ := setupRouteEx(t, route1, opts, "ROUTE:2222")

	route2 := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	expectAuthRequired(t, route2)
	route2Send, _ := setupRouteEx(t, route2, opts, "ROUTE:2222")

	// Send SUB via route connections
	sub := "SUB foo RSID:2:22\r\n"
	route1Send(sub)
	route2Send(sub)

	// Make sure we do not get anything on a MSG send to a router.
	// Send MSG proto via route connection
	route1Send("MSG foo 1 2\r\nok\r\n")

	expectNothing(t, route1)
	expectNothing(t, route2)

	// Setup a client
	client := createClientConn(t, opts.Host, opts.Port)
	clientSend, clientExpect := setupConn(t, client)
	defer client.Close()

	// Send PUB via client connection
	clientSend("PUB foo 2\r\nok\r\n")
	clientSend("PING\r\n")
	clientExpect(pongRe)

	// We should only receive on one route, not both.
	// Check both manually.
	route1.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	buf, _ := ioutil.ReadAll(route1)
	route1.SetReadDeadline(time.Time{})
	if len(buf) <= 0 {
		route2.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		buf, _ = ioutil.ReadAll(route2)
		route2.SetReadDeadline(time.Time{})
		if len(buf) <= 0 {
			t.Fatal("Expected to get one message on a route, received none.")
		}
	}

	matches := msgRe.FindAllSubmatch(buf, -1)
	if len(matches) != 1 {
		t.Fatalf("Expected 1 msg, got %d\n", len(matches))
	}
	checkMsg(t, matches[0], "foo", "", "", "2", "ok")
}

func TestRouteResendsLocalSubsOnReconnect(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	client := createClientConn(t, opts.Host, opts.Port)
	clientSend, clientExpect := setupConn(t, client)

	// Setup a local subscription
	clientSend("SUB foo 1\r\n")
	clientSend("PING\r\n")
	clientExpect(pongRe)

	route := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	_, routeExpect := setupRouteEx(t, route, opts, "ROUTE:4222")

	// Expect to see the local sub echoed through.
	routeExpect(subRe)

	// Close and re-open
	route.Close()

	route = createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	_, routeExpect = setupRouteEx(t, route, opts, "ROUTE:4222")

	// Expect to see the local sub echoed through.
	routeExpect(subRe)
}
