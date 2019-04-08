// Copyright 2019 The NATS Authors
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
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/jwt"
	"github.com/nats-io/nkeys"
)

func createLeafConn(t tLogger, host string, port int) net.Conn {
	return createClientConn(t, host, port)
}

func testDefaultOptionsForLeafNodes() *server.Options {
	o := DefaultTestOptions
	o.Host = "127.0.0.1"
	o.Port = -1
	o.LeafNode.Host = o.Host
	o.LeafNode.Port = -1
	return &o
}

func runLeafServer() (*server.Server, *server.Options) {
	o := testDefaultOptionsForLeafNodes()
	return RunServer(o), o
}

func runLeafServerOnPort(port int) (*server.Server, *server.Options) {
	o := testDefaultOptionsForLeafNodes()
	o.LeafNode.Port = port
	return RunServer(o), o
}

func runSolicitLeafServer(lso *server.Options) (*server.Server, *server.Options) {
	o := DefaultTestOptions
	o.Host = "127.0.0.1"
	o.Port = -1
	rurl, _ := url.Parse(fmt.Sprintf("nats-leaf://%s:%d", lso.LeafNode.Host, lso.LeafNode.Port))
	o.LeafNode.Remotes = []*server.RemoteLeafOpts{&server.RemoteLeafOpts{URL: rurl}}
	o.LeafNode.ReconnectInterval = 100 * time.Millisecond
	return RunServer(&o), &o
}

func TestLeafNodeInfo(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	info := checkInfoMsg(t, lc)
	if !info.AuthRequired {
		t.Fatalf("AuthRequired should always be true for leaf nodes")
	}
	sendProto(t, lc, "CONNECT {}\r\n")

	checkLeafNodeConnected(t, s)

	// Now close connection, make sure we are doing the right accounting in the server.
	lc.Close()

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if nln := s.NumLeafNodes(); nln != 0 {
			return fmt.Errorf("Number of leaf nodes is %d", nln)
		}
		return nil
	})
}

func TestNumLeafNodes(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	createNewLeafNode := func() net.Conn {
		t.Helper()
		lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
		checkInfoMsg(t, lc)
		sendProto(t, lc, "CONNECT {}\r\n")
		return lc
	}
	checkLFCount := func(n int) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if nln := s.NumLeafNodes(); nln != n {
				return fmt.Errorf("Number of leaf nodes is %d", nln)
			}
			return nil
		})
	}
	checkLFCount(0)

	lc1 := createNewLeafNode()
	defer lc1.Close()
	checkLFCount(1)

	lc2 := createNewLeafNode()
	defer lc2.Close()
	checkLFCount(2)

	// Now test remove works.
	lc1.Close()
	checkLFCount(1)

	lc2.Close()
	checkLFCount(0)
}

func TestLeafNodeRequiresConnect(t *testing.T) {
	opts := testDefaultOptionsForLeafNodes()
	opts.LeafNode.AuthTimeout = 0.001
	s := RunServer(opts)
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	info := checkInfoMsg(t, lc)
	if !info.AuthRequired {
		t.Fatalf("Expected AuthRequired to force CONNECT")
	}
	if info.TLSRequired {
		t.Fatalf("Expected TLSRequired to be false")
	}
	if info.TLSVerify {
		t.Fatalf("Expected TLSVerify to be false")
	}

	// Now wait and make sure we get disconnected.
	errBuf := expectResult(t, lc, errRe)

	if !strings.Contains(string(errBuf), "Authentication Timeout") {
		t.Fatalf("Authentication Timeout response incorrect: %q", errBuf)
	}
	expectDisconnect(t, lc)
}

func TestLeafNodeSendsSubsAfterConnect(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("SUB foo 1\r\n")
	send("SUB bar 2\r\n")
	send("SUB foo baz 3\r\n")
	send("SUB foo baz 4\r\n")
	send("SUB bar 5\r\n")
	send("PING\r\n")
	expect(pongRe)

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	_, leafExpect := setupConn(t, lc)
	matches := lsubRe.FindAllSubmatch(leafExpect(lsubRe), -1)
	// This should compress down to 1 for foo, 1 for bar, and 1 for foo [baz]
	if len(matches) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(matches))
	}
}

func TestLeafNodeSendsSubsOngoing(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PING\r\n")
	expect(pongRe)

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupConn(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	send("SUB foo 1\r\n")
	leafExpect(lsubRe)

	// Check queues send updates each time.
	// TODO(dlc) - If we decide to suppress this with a timer approach this test will break.
	send("SUB foo bar 2\r\n")
	leafExpect(lsubRe)
	send("SUB foo bar 3\r\n")
	leafExpect(lsubRe)
	send("SUB foo bar 4\r\n")
	leafExpect(lsubRe)

	// Now check more normal subs do nothing.
	send("SUB foo 5\r\n")
	expectNothing(t, lc)

	// Check going back down does nothing til we hit 0.
	send("UNSUB 5\r\n")
	expectNothing(t, lc)
	send("UNSUB 1\r\n")
	leafExpect(lunsubRe)

	// Queues going down should always send updates.
	send("UNSUB 2\r\n")
	leafExpect(lsubRe)
	send("UNSUB 3\r\n")
	leafExpect(lsubRe)
	send("UNSUB 4\r\n")
	leafExpect(lunsubRe)
}

func TestLeafNodeSubs(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupConn(t, lc)

	leafSend("PING\r\n")
	leafExpect(pongRe)

	leafSend("LS+ foo\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PING\r\n")
	expect(pongRe)

	send("PUB foo 2\r\nOK\r\n")
	matches := lmsgRe.FindAllSubmatch(leafExpect(lmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkLmsg(t, matches[0], "foo", "", "2", "OK")

	// Second sub should not change delivery
	leafSend("LS+ foo\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	send("PUB foo 3\r\nOK!\r\n")
	matches = lmsgRe.FindAllSubmatch(leafExpect(lmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkLmsg(t, matches[0], "foo", "", "3", "OK!")

	// Now add in a queue sub with weight 4.
	leafSend("LS+ foo bar 4\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	send("PUB foo 4\r\nOKOK\r\n")
	matches = lmsgRe.FindAllSubmatch(leafExpect(lmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkLmsg(t, matches[0], "foo", "| bar", "4", "OKOK")

	// Now add in a queue sub with weight 4.
	leafSend("LS+ foo baz 2\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	send("PUB foo 5\r\nHELLO\r\n")
	matches = lmsgRe.FindAllSubmatch(leafExpect(lmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkLmsg(t, matches[0], "foo", "| bar baz", "5", "HELLO")

	// Test Unsub
	leafSend("LS- foo\r\n")
	leafSend("LS- foo bar\r\n")
	leafSend("LS- foo baz\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	send("PUB foo 5\r\nHELLO\r\n")
	expectNothing(t, lc)
}

func TestLeafNodeMsgDelivery(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupConn(t, lc)

	leafSend("PING\r\n")
	leafExpect(pongRe)

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PING\r\n")
	expect(pongRe)

	send("SUB foo 1\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lsubRe)

	// Now send from leaf side.
	leafSend("LMSG foo 2\r\nOK\r\n")
	expectNothing(t, lc)

	matches := msgRe.FindAllSubmatch(expect(msgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkMsg(t, matches[0], "foo", "1", "", "2", "OK")

	send("UNSUB 1\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lunsubRe)
	send("SUB foo bar 2\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lsubRe)

	// Now send again from leaf side. This is targeted so this should
	// not be delivered.
	leafSend("LMSG foo 2\r\nOK\r\n")
	expectNothing(t, lc)
	expectNothing(t, c)

	// Now send targeted, and we should receive it.
	leafSend("LMSG foo | bar 2\r\nOK\r\n")
	expectNothing(t, lc)

	matches = msgRe.FindAllSubmatch(expect(msgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkMsg(t, matches[0], "foo", "2", "", "2", "OK")

	// Check reply + queues
	leafSend("LMSG foo + myreply bar 2\r\nOK\r\n")
	expectNothing(t, lc)

	matches = msgRe.FindAllSubmatch(expect(msgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkMsg(t, matches[0], "foo", "2", "myreply", "2", "OK")
}

func TestLeafNodeAndRoutes(t *testing.T) {
	srvA, optsA := RunServerWithConfig("./configs/srv_a_leaf.conf")
	srvB, optsB := RunServerWithConfig("./configs/srv_b.conf")
	checkClusterFormed(t, srvA, srvB)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	lc := createLeafConn(t, optsA.LeafNode.Host, optsA.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupConn(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	c := createClientConn(t, optsB.Host, optsB.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PING\r\n")
	expect(pongRe)

	send("SUB foo 1\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lsubRe)

	send("SUB foo 2\r\nPING\r\n")
	expect(pongRe)
	expectNothing(t, lc)

	send("UNSUB 2\r\n")
	expectNothing(t, lc)
	send("UNSUB 1\r\n")
	leafExpect(lunsubRe)

	// Now put it back and test msg flow.
	send("SUB foo 1\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lsubRe)

	leafSend("LMSG foo + myreply bar 2\r\nOK\r\n")
	expectNothing(t, lc)

	matches := msgRe.FindAllSubmatch(expect(msgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkMsg(t, matches[0], "foo", "1", "myreply", "2", "OK")

	// Now check reverse.
	leafSend("LS+ bar\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	send("PUB bar 2\r\nOK\r\n")
	matches = lmsgRe.FindAllSubmatch(leafExpect(lmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkLmsg(t, matches[0], "bar", "", "2", "OK")
}

// Helper function to check that a leaf node has connected to our server.
func checkLeafNodeConnected(t *testing.T, s *server.Server) {
	t.Helper()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if nln := s.NumLeafNodes(); nln != 1 {
			return fmt.Errorf("Expected a connected leafnode for server %q, got none", s.ID())
		}
		return nil
	})
}

func TestLeafNodeSolicit(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	sl, _ := runSolicitLeafServer(opts)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)

	// Now test reconnect.
	s.Shutdown()
	// Need to restart it on the same port.
	s, _ = runLeafServerOnPort(opts.LeafNode.Port)
	checkLeafNodeConnected(t, s)
}

func TestLeafNodeNoEcho(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PING\r\n")
	expect(pongRe)

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupConn(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	// We should not echo back to ourselves. Set up 'foo' subscriptions
	// on both sides and send message across the leafnode connection. It
	// should not come back.

	send("SUB foo 1\r\n")
	leafExpect(lsubRe)

	leafSend("LS+ foo\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	leafSend("LMSG foo 2\r\nOK\r\n")
	expectNothing(t, lc)
}

// Used to setup clusters of clusters for tests.
type cluster struct {
	servers []*server.Server
	opts    []*server.Options
	name    string
}

func testDefaultClusterOptionsForLeafNodes() *server.Options {
	o := DefaultTestOptions
	o.Host = "127.0.0.1"
	o.Port = -1
	o.Cluster.Host = o.Host
	o.Cluster.Port = -1
	o.Gateway.Host = o.Host
	o.Gateway.Port = -1
	o.LeafNode.Host = o.Host
	o.LeafNode.Port = -1
	return &o
}

func shutdownCluster(c *cluster) {
	if c == nil {
		return
	}
	for _, s := range c.servers {
		s.Shutdown()
	}
}

// Wait for the expected number of outbound gateways, or fails.
func waitForOutboundGateways(t *testing.T, s *server.Server, expected int, timeout time.Duration) {
	t.Helper()
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		if n := s.NumOutboundGateways(); n != expected {
			return fmt.Errorf("Expected %v outbound gateway(s), got %v", expected, n)
		}
		return nil
	})
}

// Creates a full cluster with numServers and given name and makes sure its well formed.
// Will have Gateways and Leaf Node connections active.
func createClusterWithName(t *testing.T, clusterName string, numServers int, connectTo ...*cluster) *cluster {
	t.Helper()

	if clusterName == "" || numServers < 1 {
		t.Fatalf("Bad params")
	}

	// If we are going to connect to another cluster set that up now for options.
	var gws []*server.RemoteGatewayOpts
	for _, c := range connectTo {
		// Gateways autodiscover here too, so just need one address from the set.
		gwAddr := fmt.Sprintf("nats-gw://%s:%d", c.opts[0].Gateway.Host, c.opts[0].Gateway.Port)
		gwurl, _ := url.Parse(gwAddr)
		gws = append(gws, &server.RemoteGatewayOpts{Name: c.name, URLs: []*url.URL{gwurl}})
	}

	// Create seed first.
	o := testDefaultClusterOptionsForLeafNodes()
	o.Gateway.Name = clusterName
	o.Gateway.Gateways = gws
	// All of these need system accounts.
	o.Accounts = []*server.Account{server.NewAccount("$SYS")}
	o.SystemAccount = "$SYS"
	s := RunServer(o)

	c := &cluster{servers: make([]*server.Server, 0, 3), opts: make([]*server.Options, 0, 3), name: clusterName}
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)

	// For connecting to seed server above.
	routeAddr := fmt.Sprintf("nats-route://%s:%d", o.Cluster.Host, o.Cluster.Port)
	rurl, _ := url.Parse(routeAddr)
	routes := []*url.URL{rurl}

	for i := 1; i < numServers; i++ {
		o := testDefaultClusterOptionsForLeafNodes()
		o.Gateway.Name = clusterName
		o.Gateway.Gateways = gws
		o.Routes = routes
		// All of these need system accounts.
		o.Accounts = []*server.Account{server.NewAccount("$SYS")}
		o.SystemAccount = "$SYS"
		s := RunServer(o)
		c.servers = append(c.servers, s)
		c.opts = append(c.opts, o)
	}
	checkClusterFormed(t, c.servers...)

	// Wait on gateway connections if we were asked to connect to other gateways.
	if numGWs := len(connectTo); numGWs > 0 {
		for _, s := range c.servers {
			waitForOutboundGateways(t, s, numGWs, 2*time.Second)
		}
	}

	return c
}

func TestLeafNodeGatewayRequiresSystemAccount(t *testing.T) {
	o := testDefaultClusterOptionsForLeafNodes()
	o.Gateway.Name = "CLUSTER-A"
	_, err := server.NewServer(o)
	if err == nil {
		t.Fatalf("Expected an error with no system account defined")
	}
}

func TestLeafNodeGatewaySendsSystemEvent(t *testing.T) {
	server.SetGatewaysSolicitDelay(50 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	ca := createClusterWithName(t, "A", 1)
	defer shutdownCluster(ca)
	cb := createClusterWithName(t, "B", 1, ca)
	defer shutdownCluster(cb)

	// Create client on a server in cluster A
	opts := ca.opts[0]
	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	// Listen for the leaf node event.
	send, expect := setupConnWithAccount(t, c, "$SYS")
	send("SUB $SYS.ACCOUNT.*.LEAFNODE.CONNECT 1\r\nPING\r\n")
	expect(pongRe)

	opts = cb.opts[0]
	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupConn(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	matches := rawMsgRe.FindAllSubmatch(expect(rawMsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	m := matches[0]
	if string(m[subIndex]) != "$SYS.ACCOUNT.$G.LEAFNODE.CONNECT" {
		t.Fatalf("Got wrong subject for leaf node event, got %q, wanted %q",
			m[subIndex], "$SYS.ACCOUNT.$G.LEAFNODE.CONNECT")
	}
}

func TestLeafNodeWithRouteAndGateway(t *testing.T) {
	server.SetGatewaysSolicitDelay(50 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	ca := createClusterWithName(t, "A", 3)
	defer shutdownCluster(ca)
	cb := createClusterWithName(t, "B", 3, ca)
	defer shutdownCluster(cb)

	// Create client on a server in cluster A
	opts := ca.opts[0]
	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PING\r\n")
	expect(pongRe)

	// Create a leaf node connection on a server in cluster B
	opts = cb.opts[0]
	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupConn(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	// Make sure we see interest graph propagation on the leaf node
	// connection. This is required since leaf nodes only send data
	// in the presence of interest.
	send("SUB foo 1\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lsubRe)

	send("SUB foo 2\r\nPING\r\n")
	expect(pongRe)
	expectNothing(t, lc)

	send("UNSUB 2\r\n")
	expectNothing(t, lc)
	send("UNSUB 1\r\n")
	leafExpect(lunsubRe)

	// Now put it back and test msg flow.
	send("SUB foo 1\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lsubRe)

	//leafSend("LMSG foo + myreply bar 2\r\nOK\r\n")
	leafSend("LMSG foo 2\r\nOK\r\n")
	expectNothing(t, lc)

	matches := msgRe.FindAllSubmatch(expect(msgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkMsg(t, matches[0], "foo", "1", "", "2", "OK")

	// Now check reverse.
	leafSend("LS+ bar\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	send("PUB bar 2\r\nOK\r\n")
	matches = lmsgRe.FindAllSubmatch(leafExpect(lmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkLmsg(t, matches[0], "bar", "", "2", "OK")
}

func TestLeafNodeLocalizedDQ(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	sl, slOpts := runSolicitLeafServer(opts)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)

	c := createClientConn(t, slOpts.Host, slOpts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("SUB foo bar 1\r\n")
	send("SUB foo bar 2\r\n")
	send("SUB foo bar 3\r\n")
	send("SUB foo bar 4\r\n")
	send("PING\r\n")
	expect(pongRe)

	// Now create another client on the main leaf server.
	sc := createClientConn(t, opts.Host, opts.Port)
	defer sc.Close()

	sendL, expectL := setupConn(t, sc)
	sendL("SUB foo bar 11\r\n")
	sendL("SUB foo bar 12\r\n")
	sendL("SUB foo bar 13\r\n")
	sendL("SUB foo bar 14\r\n")
	sendL("PING\r\n")
	expectL(pongRe)

	for i := 0; i < 10; i++ {
		send("PUB foo 2\r\nOK\r\n")
	}
	expectNothing(t, sc)

	matches := msgRe.FindAllSubmatch(expect(msgRe), -1)
	if len(matches) != 10 {
		t.Fatalf("Expected 10 msgs, got %d", len(matches))
	}
	for i := 0; i < 10; i++ {
		checkMsg(t, matches[i], "foo", "", "", "2", "OK")
	}
}

func TestLeafNodeBasicAuth(t *testing.T) {
	content := `
	leafnodes {
		listen: "127.0.0.1:-1"
		authorization {
			user: "derek"
			password: "s3cr3t!"
			timeout: 2.2
		}
	}
	`
	conf := createConfFile(t, []byte(content))
	defer os.Remove(conf)

	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	// This should fail since we want u/p
	setupConn(t, lc)
	errBuf := expectResult(t, lc, errRe)
	if !strings.Contains(string(errBuf), "Authorization Violation") {
		t.Fatalf("Authentication Timeout response incorrect: %q", errBuf)
	}
	expectDisconnect(t, lc)

	// Try bad password as well.
	lc = createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	// This should fail since we want u/p
	setupConnWithUserPass(t, lc, "derek", "badpassword")
	errBuf = expectResult(t, lc, errRe)
	if !strings.Contains(string(errBuf), "Authorization Violation") {
		t.Fatalf("Authentication Timeout response incorrect: %q", errBuf)
	}
	expectDisconnect(t, lc)

	// This one should work.
	lc = createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	// This should fail since we want u/p
	leafSend, leafExpect := setupConnWithUserPass(t, lc, "derek", "s3cr3t!")
	leafSend("PING\r\n")
	leafExpect(pongRe)

	checkLeafNodeConnected(t, s)
}

func runTLSSolicitLeafServer(lso *server.Options) (*server.Server, *server.Options) {
	o := DefaultTestOptions
	o.Host = "127.0.0.1"
	o.Port = -1
	rurl, _ := url.Parse(fmt.Sprintf("nats-leaf://%s:%d", lso.LeafNode.Host, lso.LeafNode.Port))
	remote := &server.RemoteLeafOpts{URL: rurl}
	remote.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	host, _, _ := net.SplitHostPort(lso.LeafNode.Host)
	remote.TLSConfig.ServerName = host
	remote.TLSConfig.InsecureSkipVerify = true
	o.LeafNode.Remotes = []*server.RemoteLeafOpts{remote}
	return RunServer(&o), &o
}

func TestLeafNodeTLS(t *testing.T) {
	content := `
	leafnodes {
		listen: "127.0.0.1:-1"
		tls {
			cert_file: "./configs/certs/server-cert.pem"
			key_file: "./configs/certs/server-key.pem"
			timeout: 0.1
		}
	}
	`
	conf := createConfFile(t, []byte(content))
	defer os.Remove(conf)

	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	info := checkInfoMsg(t, lc)
	if !info.TLSRequired {
		t.Fatalf("Expected TLSRequired to be true")
	}
	if info.TLSVerify {
		t.Fatalf("Expected TLSVerify to be false")
	}
	// We should get a disconnect here since we have not upgraded to TLS.
	expectDisconnect(t, lc)

	// This should work ok.
	sl, _ := runTLSSolicitLeafServer(opts)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)
}

func runLeafNodeOperatorServer(t *testing.T) (*server.Server, *server.Options, string) {
	t.Helper()
	content := `
	port: -1
	operator = "./configs/nkeys/op.jwt"
	resolver = MEMORY
	leafnodes {
		listen: "127.0.0.1:-1"
	}
	`
	conf := createConfFile(t, []byte(content))
	s, opts := RunServerWithConfig(conf)
	return s, opts, conf
}

func genCredsFile(t *testing.T, jwt string, seed []byte) string {
	creds := `
		-----BEGIN NATS USER JWT-----
		%s
		------END NATS USER JWT------

		************************* IMPORTANT *************************
		NKEY Seed printed below can be used to sign and prove identity.
		NKEYs are sensitive and should be treated as secrets.

		-----BEGIN USER NKEY SEED-----
		%s
		------END USER NKEY SEED------

		*************************************************************
		`
	return createConfFile(t, []byte(strings.Replace(fmt.Sprintf(creds, jwt, seed), "\t\t", "", -1)))
}

func runSolicitWithCredentials(t *testing.T, opts *server.Options, creds string) (*server.Server, *server.Options, string) {
	content := `
		port: -1
		leafnodes {
			remotes = [
				{
					url: nats-leaf://127.0.0.1:%d
					credentials: "%s"
				}
			]
		}
		`
	config := fmt.Sprintf(content, opts.LeafNode.Port, creds)
	conf := createConfFile(t, []byte(config))
	s, opts := RunServerWithConfig(conf)
	return s, opts, conf
}

func TestLeafNodeOperatorModel(t *testing.T) {
	s, opts, conf := runLeafNodeOperatorServer(t)
	defer os.Remove(conf)
	defer s.Shutdown()

	// Make sure we get disconnected without proper credentials etc.
	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	// This should fail since we want user jwt, signed nonce etc.
	setupConn(t, lc)
	errBuf := expectResult(t, lc, errRe)
	if !strings.Contains(string(errBuf), "Authorization Violation") {
		t.Fatalf("Authentication Timeout response incorrect: %q", errBuf)
	}
	expectDisconnect(t, lc)

	// Setup account and a user that will be used by the remote leaf node server.
	// createAccount automatically registers with resolver etc..
	_, akp := createAccount(t, s)
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	nuc := jwt.NewUserClaims(pub)
	ujwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	seed, _ := kp.Seed()
	mycreds := genCredsFile(t, ujwt, seed)
	defer os.Remove(mycreds)

	sl, _, lnconf := runSolicitWithCredentials(t, opts, mycreds)
	defer os.Remove(lnconf)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)
}

func TestLeafNodeMultipleAccounts(t *testing.T) {
	// So we will create a main server with two accounts. The remote server, acting as a leaf node, will simply have
	// the $G global account and no auth. Make sure things work properly here.
	s, opts, conf := runLeafNodeOperatorServer(t)
	defer os.Remove(conf)
	defer s.Shutdown()

	// Setup the two accounts for this server.
	_, akp1 := createAccount(t, s)
	kp1, _ := nkeys.CreateUser()
	pub1, _ := kp1.PublicKey()
	nuc1 := jwt.NewUserClaims(pub1)
	ujwt1, err := nuc1.Encode(akp1)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	// Create second account.
	createAccount(t, s)

	// Create the leaf node server using the first account.
	seed, _ := kp1.Seed()
	mycreds := genCredsFile(t, ujwt1, seed)
	defer os.Remove(mycreds)

	sl, lopts, lnconf := runSolicitWithCredentials(t, opts, mycreds)
	defer os.Remove(lnconf)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)

	// To connect to main server.
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	nc1, err := nats.Connect(url, createUserCreds(t, s, akp1))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc1.Close()

	// This is a client connected to the leaf node with no auth,
	// binding to account1 via leafnode connection.
	// To connect to leafnode server.
	lurl := fmt.Sprintf("nats://%s:%d", lopts.Host, lopts.Port)
	ncl, err := nats.Connect(lurl)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncl.Close()

	lsub, _ := ncl.SubscribeSync("foo.test")

	// Wait for the sub to propagate.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if subs := s.NumSubscriptions(); subs < 1 {
			return fmt.Errorf("Number of subs is %d", subs)
		}
		return nil
	})

	// Now send from nc1 with account 1, should be received by our leafnode subscriber.
	nc1.Publish("foo.test", nil)

	_, err = lsub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error during wait for next message: %s", err)
	}
}

func TestLeafNodeExportsImports(t *testing.T) {
	// So we will create a main server with two accounts. The remote server, acting as a leaf node, will simply have
	// the $G global account and no auth. Make sure things work properly here.
	s, opts, conf := runLeafNodeOperatorServer(t)
	defer os.Remove(conf)
	defer s.Shutdown()

	// Setup the two accounts for this server.
	okp, _ := nkeys.FromSeed(oSeed)

	// Create second account with exports
	acc2, akp2 := createAccount(t, s)
	akp2Pub, _ := akp2.PublicKey()
	akp2AC := jwt.NewAccountClaims(akp2Pub)
	streamExport := &jwt.Export{Subject: "foo.stream", Type: jwt.Stream}
	serviceExport := &jwt.Export{Subject: "req.echo", Type: jwt.Service}
	akp2AC.Exports.Add(streamExport, serviceExport)
	akp2ACJWT, err := akp2AC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	if err := s.AccountResolver().Store(akp2Pub, akp2ACJWT); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	s.UpdateAccountClaims(acc2, akp2AC)

	// Now create the first account and add on the imports. This will be what is used in the leafnode.
	acc1, akp1 := createAccount(t, s)
	akp1Pub, _ := akp1.PublicKey()
	akp1AC := jwt.NewAccountClaims(akp1Pub)
	streamImport := &jwt.Import{Account: akp2Pub, Subject: "foo.stream", To: "import", Type: jwt.Stream}
	serviceImport := &jwt.Import{Account: akp2Pub, Subject: "import.request", To: "req.echo", Type: jwt.Service}
	akp1AC.Imports.Add(streamImport, serviceImport)
	akp1ACJWT, err := akp1AC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	if err := s.AccountResolver().Store(akp1Pub, akp1ACJWT); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	s.UpdateAccountClaims(acc1, akp1AC)

	// Create the user will we use to connect the leafnode.
	kp1, _ := nkeys.CreateUser()
	pub1, _ := kp1.PublicKey()
	nuc1 := jwt.NewUserClaims(pub1)
	ujwt1, err := nuc1.Encode(akp1)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	// Create the leaf node server using the first account.
	seed, _ := kp1.Seed()
	mycreds := genCredsFile(t, ujwt1, seed)
	defer os.Remove(mycreds)

	sl, lopts, lnconf := runSolicitWithCredentials(t, opts, mycreds)
	defer os.Remove(lnconf)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)

	// To connect to main server.
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	// Imported
	nc1, err := nats.Connect(url, createUserCreds(t, s, akp1))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc1.Close()

	// Exported
	nc2, err := nats.Connect(url, createUserCreds(t, s, akp2))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	// Leaf node connection.
	lurl := fmt.Sprintf("nats://%s:%d", lopts.Host, lopts.Port)
	ncl, err := nats.Connect(lurl)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncl.Close()

	// So everything should be setup here. So let's test streams first.
	lsub, _ := ncl.SubscribeSync("import.foo.stream")

	// Wait for the sub to propagate.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if subs := s.NumSubscriptions(); subs < 1 {
			return fmt.Errorf("Number of subs is %d", subs)
		}
		return nil
	})

	// Pub to other account with export on original subject.
	nc2.Publish("foo.stream", nil)

	_, err = lsub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error during wait for next message: %s", err)
	}

	// Services
	// Create listener on nc1
	nc2.Subscribe("req.echo", func(msg *nats.Msg) {
		nc2.Publish(msg.Reply, []byte("WORKED"))
	})
	nc2.Flush()

	// Now send the request on the leaf node client.
	if _, err := ncl.Request("import.request", []byte("fingers crossed"), 500*time.Millisecond); err != nil {
		t.Fatalf("Did not receive response: %v", err)
	}
}

func TestLeafNodeInfoURLs(t *testing.T) {
	for _, test := range []struct {
		name         string
		useAdvertise bool
	}{
		{
			"without advertise",
			false,
		},
		{
			"with advertise",
			true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := testDefaultOptionsForLeafNodes()
			opts.Cluster.Port = -1
			opts.LeafNode.Host = "127.0.0.1"
			if test.useAdvertise {
				opts.LeafNode.Advertise = "me:1"
			}
			s1 := RunServer(opts)
			defer s1.Shutdown()

			lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
			defer lc.Close()
			info := checkInfoMsg(t, lc)
			if sz := len(info.LeafNodeURLs); sz != 1 {
				t.Fatalf("Expected LeafNodeURLs array to be size 1, got %v", sz)
			}
			var s1LNURL string
			if test.useAdvertise {
				s1LNURL = "me:1"
			} else {
				s1LNURL = net.JoinHostPort(opts.LeafNode.Host, strconv.Itoa(opts.LeafNode.Port))
			}
			if url := info.LeafNodeURLs[0]; url != s1LNURL {
				t.Fatalf("Expected URL to be %s, got %s", s1LNURL, url)
			}
			lc.Close()

			opts2 := testDefaultOptionsForLeafNodes()
			opts2.Cluster.Port = -1
			opts2.Routes = server.RoutesFromStr(fmt.Sprintf("nats://%s:%d", opts.Cluster.Host, opts.Cluster.Port))
			opts2.LeafNode.Host = "127.0.0.1"
			if test.useAdvertise {
				opts2.LeafNode.Advertise = "me:2"
			}
			s2 := RunServer(opts2)
			defer s2.Shutdown()

			checkClusterFormed(t, s1, s2)

			lc = createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
			defer lc.Close()
			info = checkInfoMsg(t, lc)
			if sz := len(info.LeafNodeURLs); sz != 2 {
				t.Fatalf("Expected LeafNodeURLs array to be size 2, got %v", sz)
			}
			var s2LNURL string
			if test.useAdvertise {
				s2LNURL = "me:2"
			} else {
				s2LNURL = net.JoinHostPort(opts2.LeafNode.Host, strconv.Itoa(opts2.LeafNode.Port))
			}
			var ok [2]int
			for _, url := range info.LeafNodeURLs {
				if url == s1LNURL {
					ok[0]++
				} else if url == s2LNURL {
					ok[1]++
				}
			}
			for i, res := range ok {
				if res != 1 {
					t.Fatalf("URL from server %v was found %v times", i+1, res)
				}
			}
			lc.Close()

			// Remove s2, and wait for route to be lost on s1.
			s2.Shutdown()
			checkNumRoutes(t, s1, 0)

			// Now check that s1 returns only itself in the URLs array.
			lc = createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
			defer lc.Close()
			info = checkInfoMsg(t, lc)
			if sz := len(info.LeafNodeURLs); sz != 1 {
				t.Fatalf("Expected LeafNodeURLs array to be size 1, got %v", sz)
			}
			if url := info.LeafNodeURLs[0]; url != s1LNURL {
				t.Fatalf("Expected URL to be %s, got %s", s1LNURL, url)
			}
			lc.Close()
		})
	}
}

func TestLeafNodeFailover(t *testing.T) {
	server.SetGatewaysSolicitDelay(50 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	ca := createClusterWithName(t, "A", 2)
	defer shutdownCluster(ca)

	cb := createClusterWithName(t, "B", 1, ca)
	defer shutdownCluster(cb)

	// Start a server that creates LeafNode connection to first
	// server in cluster A.
	s, opts := runSolicitLeafServer(ca.opts[0])
	defer s.Shutdown()

	// Shutdown that server on A.
	ca.servers[0].Shutdown()

	// Make sure that s reconnects its LN connection
	checkLNConnected := func(t *testing.T, s *server.Server) {
		t.Helper()
		checkFor(t, 3*time.Second, 15*time.Millisecond, func() error {
			if s.NumLeafNodes() == 1 {
				return nil
			}
			return fmt.Errorf("Server did not reconnect to second server in cluster A")
		})
	}
	checkLNConnected(t, ca.servers[1])

	// Verify that LeafNode info protocol is sent to the server `s`
	// with list of new servers. To do that, we will restart
	// ca[0] but with a different LN listen port.
	ca.opts[0].Port = -1
	ca.opts[0].Cluster.Port = -1
	ca.opts[0].Routes = server.RoutesFromStr(fmt.Sprintf("nats://%s:%d", ca.opts[1].Cluster.Host, ca.opts[1].Cluster.Port))
	ca.opts[0].LeafNode.Port = -1
	newa0 := RunServer(ca.opts[0])
	defer newa0.Shutdown()

	checkClusterFormed(t, newa0, ca.servers[1])

	// Shutdown the server the LN is currently connected to. It should
	// reconnect to newa0.
	ca.servers[1].Shutdown()
	checkLNConnected(t, newa0)

	// Now shutdown newa0 and make sure `s` does not reconnect
	// to server in gateway.
	newa0.Shutdown()

	// Wait for more than the reconnect attempts.
	time.Sleep(opts.LeafNode.ReconnectInterval + 50*time.Millisecond)

	if cb.servers[0].NumLeafNodes() != 0 {
		t.Fatalf("Server reconnected to server in cluster B")
	}
}

func TestLeafNodeAdvertise(t *testing.T) {
	// Create a dummy listener which will we use for the advertise address.
	// We will then stop the server and the test will be a success if
	// this listener accepts a connection.
	ch := make(chan struct{}, 1)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Error starting listener: %v", err)
	}
	defer l.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, _ := l.Accept()
		if c != nil {
			c.Close()
		}
		l.Close()
		ch <- struct{}{}
	}()

	port := l.Addr().(*net.TCPAddr).Port

	o2 := testDefaultOptionsForLeafNodes()
	o2.LeafNode.Advertise = fmt.Sprintf("127.0.0.1:%d", port)
	o2.Cluster.Port = -1
	s2 := RunServer(o2)
	defer s2.Shutdown()

	o1 := testDefaultOptionsForLeafNodes()
	o1.Cluster.Port = -1
	o1.Routes = server.RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o2.Cluster.Port))
	s1 := RunServer(o1)
	defer s1.Shutdown()

	checkClusterFormed(t, s1, s2)

	// Start a server that connects to s1. It should be made aware
	// of s2 (and its advertise address).
	s, _ := runSolicitLeafServer(o1)
	defer s.Shutdown()

	// Wait for leaf node connection to be established on s1.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if s1.NumLeafNodes() == 1 {
			return nil
		}
		return fmt.Errorf("Leaf node connection still not established")
	})

	// Shutdown s1. The listener that we created should be the one
	// receiving the connection from s.
	s1.Shutdown()

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("Server did not reconnect to advertised address")
	}
	wg.Wait()
}
