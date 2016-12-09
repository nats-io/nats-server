// Copyright 2012-2016 Apcera Inc. All rights reserved.

package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"reflect"
	"strconv"

	"github.com/nats-io/gnatsd/server"
)

const clientProtoInfo = 1

func runRouteServer(t *testing.T) (*server.Server, *server.Options) {
	return RunServerWithConfig("./configs/cluster.conf")
}

func TestRouterListeningSocket(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	// Check that the cluster socket is able to be connected.
	addr := fmt.Sprintf("%s:%d", opts.Cluster.Host, opts.Cluster.Port)
	checkSocket(t, addr, 2*time.Second)
}

func TestRouteGoServerShutdown(t *testing.T) {
	base := runtime.NumGoroutine()
	s, _ := runRouteServer(t)
	s.Shutdown()
	time.Sleep(50 * time.Millisecond)
	delta := (runtime.NumGoroutine() - base)
	if delta > 1 {
		t.Fatalf("%d Go routines still exist post Shutdown()", delta)
	}
}

func TestSendRouteInfoOnConnect(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	routeSend, routeExpect := setupRoute(t, rc, opts)
	buf := routeExpect(infoRe)

	info := server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}

	if !info.AuthRequired {
		t.Fatal("Expected to see AuthRequired")
	}
	if info.Port != opts.Cluster.Port {
		t.Fatalf("Received wrong information for port, expected %d, got %d",
			info.Port, opts.Cluster.Port)
	}

	// Need to send a different INFO than the one received, otherwise the server
	// will detect as a "cycle" and close the connection.
	info.ID = "RouteID"
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	infoJSON := fmt.Sprintf("INFO %s\r\n", b)
	routeSend(infoJSON)
	routeSend("PING\r\n")
	routeExpect(pongRe)
}

func TestRouteToSelf(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	routeSend, routeExpect := setupRouteEx(t, rc, opts, s.ID())
	buf := routeExpect(infoRe)

	info := server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}

	if !info.AuthRequired {
		t.Fatal("Expected to see AuthRequired")
	}
	if info.Port != opts.Cluster.Port {
		t.Fatalf("Received wrong information for port, expected %d, got %d",
			info.Port, opts.Cluster.Port)
	}

	// Now send it back and that should be detected as a route to self and the
	// connection closed.
	routeSend(string(buf))
	routeSend("PING\r\n")
	rc.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := rc.Read(buf); err == nil {
		t.Fatal("Expected route connection to be closed")
	}
}

func TestSendRouteSubAndUnsub(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, _ := setupConn(t, c)

	// We connect to the route.
	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	expectAuthRequired(t, rc)
	routeSend, routeExpect := setupRouteEx(t, rc, opts, "ROUTER:xyz")
	routeSend("INFO {\"server_id\":\"ROUTER:xyz\"}\r\n")

	routeSend("PING\r\n")
	routeExpect(pongRe)

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

	// Explicitly shutdown the server, otherwise this test would
	// cause following test to fail.
	s.Shutdown()
}

func TestSendRouteSolicit(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	// Listen for a connection from the server on the first route.
	if len(opts.Routes) <= 0 {
		t.Fatalf("Need an outbound solicted route for this test")
	}
	rURL := opts.Routes[0]

	conn := acceptRouteConn(t, rURL.Host, server.DEFAULT_ROUTE_CONNECT)
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
	buf := routeExpect(connectRe)
	if !infoRe.Match(buf) {
		routeExpect(infoRe)
	}

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

	route := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer route.Close()
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

	route := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer route.Close()

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

	route := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer route.Close()

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

	expectMsgs(1)
	routeSend("PING\r\n")
	routeExpect(pongRe)
}

func TestRouteQueueSemantics(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	client := createClientConn(t, opts.Host, opts.Port)
	clientSend, clientExpect := setupConn(t, client)
	clientExpectMsgs := expectMsgsCommand(t, clientExpect)

	defer client.Close()

	route := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer route.Close()

	expectAuthRequired(t, route)
	routeSend, routeExpect := setupRouteEx(t, route, opts, "ROUTER:xyz")
	routeSend("INFO {\"server_id\":\"ROUTER:xyz\"}\r\n")
	expectMsgs := expectMsgsCommand(t, routeExpect)

	// Express multiple interest on this route for foo, queue group bar.
	qrsid1 := "QRSID:1:1"
	routeSend(fmt.Sprintf("SUB foo bar %s\r\n", qrsid1))
	qrsid2 := "QRSID:1:2"
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
	routeSend("SUB foo RSID:1:4\r\n")

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
	if string(matches[0][sidIndex]) != "RSID:1:4" &&
		string(matches[1][sidIndex]) != "RSID:1:4" {
		t.Fatalf("Did not received routed sid\n")
	}
	checkMsg(t, matches[0], "foo", "", "", "2", "ok")
	checkMsg(t, matches[1], "foo", "", "", "2", "ok")

	// Check the rsid to verify it is one of the queue group subscribers.
	var rsid string
	if matches[0][sidIndex][0] == 'Q' {
		rsid = string(matches[0][sidIndex])
	} else {
		rsid = string(matches[1][sidIndex])
	}
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
	routeSend("MSG foo RSID:1:1 2\r\nok\r\n")
	// Queue group one.
	routeSend("MSG foo QRSID:1:2 2\r\nok\r\n")

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

	rURL := opts.Routes[0]

	route := acceptRouteConn(t, rURL.Host, 2*server.DEFAULT_ROUTE_CONNECT)

	// Go ahead and close the Route.
	route.Close()

	// We expect to get called back..
	route = acceptRouteConn(t, rURL.Host, 2*server.DEFAULT_ROUTE_CONNECT)
	route.Close()
}

func TestMultipleRoutesSameId(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	route1 := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer route1.Close()

	expectAuthRequired(t, route1)
	route1Send, _ := setupRouteEx(t, route1, opts, "ROUTE:2222")

	route2 := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer route2.Close()

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
	defer client.Close()

	clientSend, clientExpect := setupConn(t, client)

	// Setup a local subscription, make sure it reaches.
	clientSend("SUB foo 1\r\n")
	clientSend("PING\r\n")
	clientExpect(pongRe)

	route := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer route.Close()
	routeSend, routeExpect := setupRouteEx(t, route, opts, "ROUTE:4222")

	// Expect to see the local sub echoed through after we send our INFO.
	time.Sleep(50 * time.Millisecond)
	buf := routeExpect(infoRe)

	// Generate our own INFO so we can send one to trigger the local subs.
	info := server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}
	info.ID = "ROUTE:4222"
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	infoJSON := fmt.Sprintf("INFO %s\r\n", b)

	// Trigger the send of local subs.
	routeSend(infoJSON)

	routeExpect(subRe)

	// Close and then re-open
	route.Close()

	route = createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer route.Close()

	routeSend, routeExpect = setupRouteEx(t, route, opts, "ROUTE:4222")

	routeExpect(infoRe)

	routeSend(infoJSON)
	routeExpect(subRe)
}

func TestAutoUnsubPropagation(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	client := createClientConn(t, opts.Host, opts.Port)
	defer client.Close()

	clientSend, clientExpect := setupConn(t, client)

	route := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer route.Close()

	expectAuthRequired(t, route)
	routeSend, routeExpect := setupRouteEx(t, route, opts, "ROUTER:xyz")
	routeSend("INFO {\"server_id\":\"ROUTER:xyz\"}\r\n")

	// Setup a local subscription
	clientSend("SUB foo 2\r\n")
	clientSend("PING\r\n")
	clientExpect(pongRe)

	routeExpect(subRe)

	clientSend("UNSUB 2 1\r\n")
	clientSend("PING\r\n")
	clientExpect(pongRe)

	routeExpect(unsubmaxRe)

	clientSend("PUB foo 2\r\nok\r\n")
	clientExpect(msgRe)

	clientSend("PING\r\n")
	clientExpect(pongRe)

	clientSend("UNSUB 2\r\n")
	clientSend("PING\r\n")
	clientExpect(pongRe)

	routeExpect(unsubnomaxRe)
}

type ignoreLogger struct {
}

func (l *ignoreLogger) Fatalf(f string, args ...interface{}) {
}
func (l *ignoreLogger) Errorf(f string, args ...interface{}) {
}

func TestRouteConnectOnShutdownRace(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	l := &ignoreLogger{}

	var wg sync.WaitGroup

	cQuit := make(chan bool, 1)

	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			route := createRouteConn(l, opts.Cluster.Host, opts.Cluster.Port)
			if route != nil {
				setupRouteEx(l, route, opts, "ROUTE:4222")
				route.Close()
			}
			select {
			case <-cQuit:
				return
			default:
			}
		}
	}()

	time.Sleep(5 * time.Millisecond)
	s.Shutdown()

	cQuit <- true

	wg.Wait()
}

func TestRouteSendAsyncINFOToClients(t *testing.T) {
	f := func(opts *server.Options) {
		s := RunServer(opts)
		defer s.Shutdown()

		clientURL := net.JoinHostPort(opts.Host, strconv.Itoa(opts.Port))

		oldClient := createClientConn(t, opts.Host, opts.Port)
		defer oldClient.Close()

		oldClientSend, oldClientExpect := setupConn(t, oldClient)
		oldClientSend("PING\r\n")
		oldClientExpect(pongRe)

		newClient := createClientConn(t, opts.Host, opts.Port)
		defer newClient.Close()

		newClientSend, newClientExpect := setupConnWithProto(t, newClient, clientProtoInfo)
		newClientSend("PING\r\n")
		newClientExpect(pongRe)

		// Check that even a new client does not receive an async INFO at this point
		// since there is no route created yet.
		expectNothing(t, newClient)

		routeID := "Server-B"

		createRoute := func() (net.Conn, sendFun, expectFun) {
			rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
			routeSend, routeExpect := setupRouteEx(t, rc, opts, routeID)

			buf := routeExpect(infoRe)
			info := server.Info{}
			if err := json.Unmarshal(buf[4:], &info); err != nil {
				t.Fatalf("Could not unmarshal route info: %v", err)
			}
			if len(info.ClientConnectURLs) == 0 {
				t.Fatal("Expected a list of URLs, got none")
			}
			if info.ClientConnectURLs[0] != clientURL {
				t.Fatalf("Expected ClientConnectURLs to be %q, got %q", clientURL, info.ClientConnectURLs[0])
			}

			return rc, routeSend, routeExpect
		}

		sendRouteINFO := func(routeSend sendFun, routeExpect expectFun, urls []string) {
			routeInfo := server.Info{}
			routeInfo.ID = routeID
			routeInfo.Host = "localhost"
			routeInfo.Port = 5222
			routeInfo.ClientConnectURLs = urls
			b, err := json.Marshal(routeInfo)
			if err != nil {
				t.Fatalf("Could not marshal test route info: %v", err)
			}
			infoJSON := fmt.Sprintf("INFO %s\r\n", b)
			routeSend(infoJSON)
			routeSend("PING\r\n")
			routeExpect(pongRe)
		}

		checkINFOReceived := func(client net.Conn, clientExpect expectFun, expectedURLs []string) {
			if opts.Cluster.NoAdvertise {
				expectNothing(t, client)
				return
			}
			buf := clientExpect(infoRe)
			info := server.Info{}
			if err := json.Unmarshal(buf[4:], &info); err != nil {
				t.Fatalf("Could not unmarshal route info: %v", err)
			}
			if !reflect.DeepEqual(info.ClientConnectURLs, expectedURLs) {
				t.Fatalf("Expected ClientConnectURLs to be %v, got %v", expectedURLs, info.ClientConnectURLs)
			}
		}

		// Create a route
		rc, routeSend, routeExpect := createRoute()
		defer rc.Close()

		// Send an INFO with single URL
		routeConnectURLs := []string{"localhost:5222"}
		sendRouteINFO(routeSend, routeExpect, routeConnectURLs)

		// Expect nothing for old clients
		expectNothing(t, oldClient)

		// Expect new client to receive an INFO (unless disabled)
		checkINFOReceived(newClient, newClientExpect, routeConnectURLs)

		// Disconnect and reconnect the route.
		rc.Close()
		rc, routeSend, routeExpect = createRoute()
		defer rc.Close()

		// Resend the same route INFO json, since there is no new URL,
		// no client should receive an INFO
		sendRouteINFO(routeSend, routeExpect, routeConnectURLs)

		// Expect nothing for old clients
		expectNothing(t, oldClient)

		// Expect nothing for new clients as well (no real update)
		expectNothing(t, newClient)

		// Now stop the route and restart with an additional URL
		rc.Close()
		rc, routeSend, routeExpect = createRoute()
		defer rc.Close()

		// Create a client not sending the CONNECT until after route is added
		clientNoConnect := createClientConn(t, opts.Host, opts.Port)
		defer clientNoConnect.Close()

		// Create a client that does not send the first PING yet
		clientNoPing := createClientConn(t, opts.Host, opts.Port)
		defer clientNoPing.Close()
		clientNoPingSend, clientNoPingExpect := setupConnWithProto(t, clientNoPing, clientProtoInfo)

		// The route now has an additional URL
		routeConnectURLs = append(routeConnectURLs, "localhost:7777")
		// This causes the server to add the route and send INFO to clients
		sendRouteINFO(routeSend, routeExpect, routeConnectURLs)

		// Expect nothing for old clients
		expectNothing(t, oldClient)

		// Expect new client to receive an INFO, and verify content as expected.
		checkINFOReceived(newClient, newClientExpect, routeConnectURLs)

		// Expect nothing yet for client that did not send the PING
		expectNothing(t, clientNoPing)

		// Now send the first PING
		clientNoPingSend("PING\r\n")
		// Should receive PONG followed by INFO
		// Receive PONG only first
		pongBuf := make([]byte, len("PONG\r\n"))
		clientNoPing.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := clientNoPing.Read(pongBuf)
		clientNoPing.SetReadDeadline(time.Time{})
		if n <= 0 && err != nil {
			t.Fatalf("Error reading from conn: %v\n", err)
		}
		if !pongRe.Match(pongBuf) {
			t.Fatalf("Response did not match expected: \n\tReceived:'%q'\n\tExpected:'%s'\n", pongBuf, pongRe)
		}
		checkINFOReceived(clientNoPing, clientNoPingExpect, routeConnectURLs)

		// Have the client that did not send the connect do it now
		clientNoConnectSend, clientNoConnectExpect := setupConnWithProto(t, clientNoConnect, clientProtoInfo)
		// Send the PING
		clientNoConnectSend("PING\r\n")
		// Should receive PONG followed by INFO
		// Receive PONG only first
		clientNoConnect.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err = clientNoConnect.Read(pongBuf)
		clientNoConnect.SetReadDeadline(time.Time{})
		if n <= 0 && err != nil {
			t.Fatalf("Error reading from conn: %v\n", err)
		}
		if !pongRe.Match(pongBuf) {
			t.Fatalf("Response did not match expected: \n\tReceived:'%q'\n\tExpected:'%s'\n", pongBuf, pongRe)
		}
		checkINFOReceived(clientNoConnect, clientNoConnectExpect, routeConnectURLs)

		// Create a client connection and verify content of initial INFO contains array
		// (but empty if no advertise option is set)
		cli := createClientConn(t, opts.Host, opts.Port)
		defer cli.Close()
		buf := expectResult(t, cli, infoRe)
		js := infoRe.FindAllSubmatch(buf, 1)[0][1]
		var sinfo server.Info
		err = json.Unmarshal(js, &sinfo)
		if err != nil {
			t.Fatalf("Could not unmarshal INFO json: %v\n", err)
		}
		if opts.Cluster.NoAdvertise {
			if len(sinfo.ClientConnectURLs) != 0 {
				t.Fatalf("Expected ClientConnectURLs to be empty, got %v", sinfo.ClientConnectURLs)
			}
		} else if !reflect.DeepEqual(sinfo.ClientConnectURLs, routeConnectURLs) {
			t.Fatalf("Expected ClientConnectURLs to be %v, got %v", routeConnectURLs, sinfo.ClientConnectURLs)
		}
	}

	opts := LoadConfig("./configs/cluster.conf")
	for i := 0; i < 2; i++ {
		if i == 1 {
			opts.Cluster.NoAdvertise = true
		}
		f(opts)
	}
}
