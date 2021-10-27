// Copyright 2012-2019 The NATS Authors
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

package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/internal/testhelper"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const clientProtoInfo = 1

func runRouteServer(t *testing.T) (*server.Server, *server.Options) {
	return RunServerWithConfig("./configs/cluster.conf")
}

func runRouteServerOverrides(t *testing.T, cbo func(*server.Options), cbs func(*server.Server)) (*server.Server, *server.Options) {
	return RunServerWithConfigOverrides("./configs/cluster.conf", cbo, cbs)
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

	routeID := "RouteID"
	routeSend, routeExpect := setupRouteEx(t, rc, opts, routeID)
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
	info.ID = routeID
	info.Name = ""
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
	l := testhelper.NewDummyLogger(100)
	s, opts := runRouteServerOverrides(t, nil,
		func(s *server.Server) {
			s.SetLogger(l, true, true)
		})
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
	// This should have been removed by removePassFromTrace(), but we also check debug logs here
	l.CheckForProhibited(t, "route authorization password found", "top_secret")
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

	// Make sure the RS+ is broadcast via the route
	expectResult(t, rc, rsubRe)

	// Send UNSUB via client connection
	send("UNSUB 22\r\n")

	// Make sure the RS- is broadcast via the route
	expectResult(t, rc, runsubRe)

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

	// Send SUB via route connection, RS+
	routeSend("RS+ $G foo\r\nPING\r\n")
	routeExpect(pongRe)

	// Send PUB via client connection
	clientSend("PUB foo 2\r\nok\r\nPING\r\n")
	clientExpect(pongRe)

	matches := expectMsgs(1)
	checkRmsg(t, matches[0], "$G", "foo", "", "2", "ok")
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
	clientSend("SUB foo 1\r\nPING\r\n")
	// Use ping roundtrip to make sure its processed.
	clientExpect(pongRe)

	// Send RMSG proto via route connection
	routeSend("RMSG $G foo 2\r\nok\r\n")

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
	routeSend("RS+ $G foo\r\n")

	// Send MSG proto via route connection
	routeSend("RMSG foo 2\r\nok\r\n")

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
	routeSend("RS+ $G foo\r\n")
	routeSend("RS+ $G foo\r\n")
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Send PUB via client connection
	clientSend("PUB foo 2\r\nok\r\nPING\r\n")
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
	expectMsgs := expectRmsgsCommand(t, routeExpect)

	// Express multiple interest on this route for foo, queue group bar.
	routeSend("RS+ $G foo bar 1\r\n")
	routeSend("RS+ $G foo bar 2\r\n")

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
	checkRmsg(t, matches[0], "$G", "foo", "| bar", "2", "ok")

	// Add normal Interest as well to route interest.
	routeSend("RS+ $G foo\r\n")

	// Use ping roundtrip to make sure its processed.
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Send PUB via client connection
	clientSend("PUB foo 2\r\nok\r\nPING\r\n")
	// Use ping roundtrip to make sure its processed.
	clientExpect(pongRe)

	// Should be 1 now for everything. Always receive 1 message.
	matches = expectMsgs(1)
	checkRmsg(t, matches[0], "$G", "foo", "| bar", "2", "ok")

	// Now create a queue subscription for the client as well as a normal one.
	clientSend("SUB foo 1\r\n")
	// Use ping roundtrip to make sure its processed.
	clientSend("PING\r\n")
	clientExpect(pongRe)
	routeExpect(rsubRe)

	clientSend("SUB foo bar 2\r\nPING\r\n")
	// Use ping roundtrip to make sure its processed.
	clientExpect(pongRe)
	routeExpect(rsubRe)

	// Deliver a MSG from the route itself, make sure the client receives both.
	routeSend("RMSG $G foo | bar 2\r\nok\r\n")

	// Use ping roundtrip to make sure its processed.
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Should get 2 msgs.
	matches = clientExpectMsgs(2)

	// Expect first to be the normal subscriber, next will be the queue one.
	checkMsg(t, matches[0], "foo", "", "", "2", "ok")
}

func TestSolicitRouteReconnect(t *testing.T) {
	l := testhelper.NewDummyLogger(100)
	s, opts := runRouteServerOverrides(t, nil,
		func(s *server.Server) {
			s.SetLogger(l, true, true)
		})
	defer s.Shutdown()

	rURL := opts.Routes[0]

	route := acceptRouteConn(t, rURL.Host, 2*server.DEFAULT_ROUTE_CONNECT)

	// Go ahead and close the Route.
	route.Close()

	// We expect to get called back..
	route = acceptRouteConn(t, rURL.Host, 2*server.DEFAULT_ROUTE_CONNECT)
	route.Close()

	// Now we want to check for the debug logs when it tries to reconnect
	l.CheckForProhibited(t, "route authorization password found", ":bar")
}

func TestMultipleRoutesSameId(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	route1 := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer route1.Close()

	expectAuthRequired(t, route1)
	route1Send, route1Expect := setupRouteEx(t, route1, opts, "ROUTE:2222")

	route2 := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer route2.Close()

	expectAuthRequired(t, route2)
	route2Send, route2Expect := setupRouteEx(t, route2, opts, "ROUTE:2222")

	// Send SUB via route connections
	sub := "RS+ $G foo\r\nPING\r\n"
	route1Send(sub)
	route2Send(sub)
	route1Expect(pongRe)
	route2Expect(pongRe)

	// Make sure we do not get anything on a RMSG send to a router.
	// Send RMSG proto via route connection
	route1Send("RMSG $G foo 2\r\nok\r\n")

	expectNothing(t, route1)
	expectNothing(t, route2)

	// Setup a client
	client := createClientConn(t, opts.Host, opts.Port)
	clientSend, clientExpect := setupConn(t, client)
	defer client.Close()

	// Send PUB via client connection
	clientSend("PUB foo 2\r\nok\r\nPING\r\n")
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

	matches := rmsgRe.FindAllSubmatch(buf, -1)
	if len(matches) != 1 {
		t.Fatalf("Expected 1 msg, got %d\n", len(matches))
	}
	checkRmsg(t, matches[0], "$G", "foo", "", "2", "ok")
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
	routeSend, routeExpect := setupRouteEx(t, route, opts, "ROUTE:1234")

	// Expect to see the local sub echoed through after we send our INFO.
	time.Sleep(50 * time.Millisecond)
	buf := routeExpect(infoRe)

	// Generate our own INFO so we can send one to trigger the local subs.
	info := server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}
	info.ID = "ROUTE:1234"
	info.Name = ""
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	infoJSON := fmt.Sprintf("INFO %s\r\n", b)

	// Trigger the send of local subs.
	routeSend(infoJSON)

	routeExpect(rsubRe)

	// Close and then re-open
	route.Close()

	// Give some time for the route close to be processed before trying to recreate.
	checkNumRoutes(t, s, 0)

	route = createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer route.Close()

	routeSend, routeExpect = setupRouteEx(t, route, opts, "ROUTE:1234")

	routeExpect(infoRe)

	routeSend(infoJSON)
	routeExpect(rsubRe)
}

type ignoreLogger struct{}

func (l *ignoreLogger) Fatalf(f string, args ...interface{}) {}
func (l *ignoreLogger) Errorf(f string, args ...interface{}) {}

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
				setupRouteEx(l, route, opts, "ROUTE:1234")
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
				stackFatalf(t, "Could not unmarshal route info: %v", err)
			}
			if opts.Cluster.NoAdvertise {
				if len(info.ClientConnectURLs) != 0 {
					stackFatalf(t, "Expected ClientConnectURLs to be empty, got %v", info.ClientConnectURLs)
				}
			} else {
				if len(info.ClientConnectURLs) == 0 {
					stackFatalf(t, "Expected a list of URLs, got none")
				}
				if info.ClientConnectURLs[0] != clientURL {
					stackFatalf(t, "Expected ClientConnectURLs to be %q, got %q", clientURL, info.ClientConnectURLs[0])
				}
			}

			return rc, routeSend, routeExpect
		}

		sendRouteINFO := func(routeSend sendFun, routeExpect expectFun, urls []string) {
			routeInfo := server.Info{}
			routeInfo.ID = routeID
			routeInfo.Cluster = "xyz"
			routeInfo.Host = "127.0.0.1"
			routeInfo.Port = 5222
			routeInfo.ClientConnectURLs = urls
			b, err := json.Marshal(routeInfo)
			if err != nil {
				stackFatalf(t, "Could not marshal test route info: %v", err)
			}
			infoJSON := fmt.Sprintf("INFO %s\r\n", b)
			routeSend(infoJSON)
			routeSend("PING\r\n")
			routeExpect(pongRe)
		}

		checkClientConnectURLS := func(urls, expected []string) {
			// Order of array is not guaranteed.
			ok := false
			if len(urls) == len(expected) {
				m := make(map[string]struct{}, len(expected))
				for _, url := range expected {
					m[url] = struct{}{}
				}
				ok = true
				for _, url := range urls {
					if _, present := m[url]; !present {
						ok = false
						break
					}
				}
			}
			if !ok {
				stackFatalf(t, "Expected ClientConnectURLs to be %v, got %v", expected, urls)
			}
		}

		checkINFOReceived := func(client net.Conn, clientExpect expectFun, expectedURLs []string) {
			if opts.Cluster.NoAdvertise {
				expectNothing(t, client)
				return
			}
			buf := clientExpect(infoRe)
			info := server.Info{}
			if err := json.Unmarshal(buf[4:], &info); err != nil {
				stackFatalf(t, "Could not unmarshal route info: %v", err)
			}
			checkClientConnectURLS(info.ClientConnectURLs, expectedURLs)
		}

		// Create a route
		rc, routeSend, routeExpect := createRoute()
		defer rc.Close()

		// Send an INFO with single URL
		routeClientConnectURLs := []string{"127.0.0.1:5222"}
		sendRouteINFO(routeSend, routeExpect, routeClientConnectURLs)

		// Expect nothing for old clients
		expectNothing(t, oldClient)

		// We expect to get the one from the server we connect to and the other route.
		expectedURLs := []string{clientURL, routeClientConnectURLs[0]}

		// Expect new client to receive an INFO (unless disabled)
		checkINFOReceived(newClient, newClientExpect, expectedURLs)

		// Disconnect the route
		rc.Close()

		// Expect nothing for old clients
		expectNothing(t, oldClient)

		// Expect new client to receive an INFO (unless disabled).
		// The content will now have the disconnected route ClientConnectURLs
		// removed from the INFO. So it should be the one from the server the
		// client is connected to.
		checkINFOReceived(newClient, newClientExpect, []string{clientURL})

		// Reconnect the route.
		rc, routeSend, routeExpect = createRoute()
		defer rc.Close()

		// Resend the same route INFO json. The server will now send
		// the INFO since the disconnected route ClientConnectURLs was
		// removed in previous step.
		sendRouteINFO(routeSend, routeExpect, routeClientConnectURLs)

		// Expect nothing for old clients
		expectNothing(t, oldClient)

		// Expect new client to receive an INFO (unless disabled)
		checkINFOReceived(newClient, newClientExpect, expectedURLs)

		// Now stop the route and restart with an additional URL
		rc.Close()

		// On route disconnect, clients will receive an updated INFO
		expectNothing(t, oldClient)
		checkINFOReceived(newClient, newClientExpect, []string{clientURL})

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
		routeClientConnectURLs = append(routeClientConnectURLs, "127.0.0.1:7777")
		expectedURLs = append(expectedURLs, "127.0.0.1:7777")
		// This causes the server to add the route and send INFO to clients
		sendRouteINFO(routeSend, routeExpect, routeClientConnectURLs)

		// Expect nothing for old clients
		expectNothing(t, oldClient)

		// Expect new client to receive an INFO, and verify content as expected.
		checkINFOReceived(newClient, newClientExpect, expectedURLs)

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
		checkINFOReceived(clientNoPing, clientNoPingExpect, expectedURLs)

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
		checkINFOReceived(clientNoConnect, clientNoConnectExpect, expectedURLs)

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
		} else {
			checkClientConnectURLS(sinfo.ClientConnectURLs, expectedURLs)
		}

		// Add a new route
		routeID = "Server-C"
		rc2, route2Send, route2Expect := createRoute()
		defer rc2.Close()

		// Send an INFO with single URL
		rc2ConnectURLs := []string{"127.0.0.1:8888"}
		sendRouteINFO(route2Send, route2Expect, rc2ConnectURLs)

		// This is the combined client connect URLs array
		totalConnectURLs := expectedURLs
		totalConnectURLs = append(totalConnectURLs, rc2ConnectURLs...)

		// Expect nothing for old clients
		expectNothing(t, oldClient)

		// Expect new client to receive an INFO (unless disabled)
		checkINFOReceived(newClient, newClientExpect, totalConnectURLs)

		// Make first route disconnect
		rc.Close()

		// Expect nothing for old clients
		expectNothing(t, oldClient)

		// Expect new client to receive an INFO (unless disabled)
		// The content should be the server client is connected to and the last route
		checkINFOReceived(newClient, newClientExpect, []string{"127.0.0.1:5242", "127.0.0.1:8888"})
	}

	opts := LoadConfig("./configs/cluster.conf")
	// For this test, be explicit about listen spec.
	opts.Host = "127.0.0.1"
	opts.Port = 5242
	opts.DisableShortFirstPing = true

	f(opts)
	opts.Cluster.NoAdvertise = true
	f(opts)
}

func TestRouteBasicPermissions(t *testing.T) {
	srvA, optsA := RunServerWithConfig("./configs/srv_a_perms.conf")
	defer srvA.Shutdown()

	srvB, optsB := RunServerWithConfig("./configs/srv_b.conf")
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	// Create a connection to server B
	ncb, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", optsB.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncb.Close()
	ch := make(chan bool, 1)
	cb := func(_ *nats.Msg) {
		ch <- true
	}
	// Subscribe on Server B on "bar" and "baz", which should be accepted by server A across the route
	// Due to allowing "*"
	subBbar, err := ncb.Subscribe("bar", cb)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	defer subBbar.Unsubscribe()
	subBbaz, err := ncb.Subscribe("baz", cb)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	defer subBbaz.Unsubscribe()
	ncb.Flush()
	if err := checkExpectedSubs(2, srvA, srvB); err != nil {
		t.Fatal(err.Error())
	}

	// Create a connection to server A
	nca, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", optsA.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nca.Close()
	// Publish on bar and baz, messages should be received.
	if err := nca.Publish("bar", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := nca.Publish("baz", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("Did not get the messages")
		}
	}

	// From B, start a subscription on "foo", which server A should drop since
	// it only exports on "bar" and "baz"
	subBfoo, err := ncb.Subscribe("foo", cb)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	defer subBfoo.Unsubscribe()
	ncb.Flush()
	// B should have now 3 subs
	if err := checkExpectedSubs(3, srvB); err != nil {
		t.Fatal(err.Error())
	}
	// and A still 2.
	if err := checkExpectedSubs(2, srvA); err != nil {
		t.Fatal(err.Error())
	}
	// So producing on "foo" from A should not be forwarded to B.
	if err := nca.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	select {
	case <-ch:
		t.Fatal("Message should not have been received")
	case <-time.After(100 * time.Millisecond):
	}

	// Now on A, create a subscription on something that A does not import,
	// like "bat".
	subAbat, err := nca.Subscribe("bat", cb)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	defer subAbat.Unsubscribe()
	nca.Flush()
	// A should have 3 subs
	if err := checkExpectedSubs(3, srvA); err != nil {
		t.Fatal(err.Error())
	}
	// And from B, send a message on that subject and make sure it is not received.
	if err := ncb.Publish("bat", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	select {
	case <-ch:
		t.Fatal("Message should not have been received")
	case <-time.After(100 * time.Millisecond):
	}

	// Stop subscription on foo from B
	subBfoo.Unsubscribe()
	ncb.Flush()
	// Back to 2 subs on B
	if err := checkExpectedSubs(2, srvB); err != nil {
		t.Fatal(err.Error())
	}

	// Create subscription on foo from A, this should be forwared to B.
	subAfoo, err := nca.Subscribe("foo", cb)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	defer subAfoo.Unsubscribe()
	// Create another one so that test the import permissions cache
	subAfoo2, err := nca.Subscribe("foo", cb)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	defer subAfoo2.Unsubscribe()
	nca.Flush()
	// A should have 5 subs
	if err := checkExpectedSubs(5, srvA); err != nil {
		t.Fatal(err.Error())
	}
	// B should have 3 since we coalesce te two for 'foo'
	if err := checkExpectedSubs(3, srvB); err != nil {
		t.Fatal(err.Error())
	}
	// Send a message from B and check that it is received.
	if err := ncb.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("Did not get the message")
		}
	}

	// Close connection from B, and restart server B too.
	// We want to make sure that
	ncb.Close()
	srvB.Shutdown()

	// Since B had 2 local subs, A should still only go from 4 to 3
	if err := checkExpectedSubs(3, srvA); err != nil {
		t.Fatal(err.Error())
	}

	// Restart server B
	srvB, optsB = RunServerWithConfig("./configs/srv_b.conf")
	defer srvB.Shutdown()
	// Check that subs from A that can be sent to B are sent.
	// That would be 2 (the 2 subscriptions on foo) as one.
	if err := checkExpectedSubs(1, srvB); err != nil {
		t.Fatal(err.Error())
	}

	// Connect to B and send on "foo" and make sure we receive
	ncb, err = nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", optsB.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncb.Close()
	if err := ncb.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("Did not get the message")
		}
	}

	// Send on "bat" and make sure that this is not received.
	if err := ncb.Publish("bat", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	select {
	case <-ch:
		t.Fatal("Message should not have been received")
	case <-time.After(100 * time.Millisecond):
	}

	nca.Close()
	ncb.Close()

	srvA.Shutdown()
	srvB.Shutdown()

	optsA.Cluster.Permissions.Export = nil
	srvA = RunServer(optsA)
	defer srvA.Shutdown()

	srvB = RunServer(optsB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	nca, err = nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", optsA.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nca.Close()
	// Subscribe on "bar" which is not imported
	if _, err := nca.Subscribe("bar", cb); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := checkExpectedSubs(1, srvA); err != nil {
		t.Fatal(err.Error())
	}

	// Publish from B, should not be received
	ncb, err = nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", optsB.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncb.Close()
	if err := ncb.Publish("bar", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	select {
	case <-ch:
		t.Fatal("Message should not have been received")
	case <-time.After(100 * time.Millisecond):
		//ok
	}
	// Subscribe on "baz" on B
	if _, err := ncb.Subscribe("baz", cb); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := checkExpectedSubs(1, srvB); err != nil {
		t.Fatal(err.Error())
	}
	if err := checkExpectedSubs(2, srvA); err != nil {
		t.Fatal(err.Error())
	}
	// Publish from A, since there is no export restriction, message should be received.
	if err := nca.Publish("baz", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	select {
	case <-ch:
	// ok
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Message should have been received")
	}
}

func createConfFile(t *testing.T, content []byte) string {
	t.Helper()
	conf := createFile(t, "")
	fName := conf.Name()
	conf.Close()
	if err := ioutil.WriteFile(fName, content, 0666); err != nil {
		removeFile(t, fName)
		t.Fatalf("Error writing conf file: %v", err)
	}
	return fName
}

func TestRoutesOnlyImportOrExport(t *testing.T) {
	contents := []string{
		`import: "foo"`,
		`import: {
			allow: "foo"
		}`,
		`import: {
			deny: "foo"
		}`,
		`import: {
			allow: "foo"
			deny: "foo"
		}`,
		`export: "foo"`,
		`export: {
			allow: "foo"
		}`,
		`export: {
			deny: "foo"
		}`,
		`export: {
			allow: "foo"
			deny: "foo"
		}`,
	}
	f := func(c string) {
		cf := createConfFile(t, []byte(fmt.Sprintf(`
			port: -1
			cluster {
				name: "Z"
				port: -1
				authorization {
					user: ivan
					password: pwd
					permissions {
						%s
					}
				}
			}
		`, c)))
		defer removeFile(t, cf)
		s, _ := RunServerWithConfig(cf)
		s.Shutdown()
	}
	for _, c := range contents {
		f(c)
	}
}
