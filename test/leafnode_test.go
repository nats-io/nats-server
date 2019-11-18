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
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt"
	"github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

func createLeafConn(t tLogger, host string, port int) net.Conn {
	return createClientConn(t, host, port)
}

func testDefaultOptionsForLeafNodes() *server.Options {
	o := DefaultTestOptions
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
	return runSolicitLeafServerToURL(fmt.Sprintf("nats-leaf://%s:%d", lso.LeafNode.Host, lso.LeafNode.Port))
}

func runSolicitLeafServerToURL(surl string) (*server.Server, *server.Options) {
	o := DefaultTestOptions
	o.Port = -1
	rurl, _ := url.Parse(surl)
	o.LeafNode.Remotes = []*server.RemoteLeafOpts{{URLs: []*url.URL{rurl}}}
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

func TestLeafNodeSplitBuffer(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	nc.QueueSubscribe("foo", "bar", func(m *nats.Msg) {
		m.Respond([]byte("ok"))
	})
	nc.Flush()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()
	sendProto(t, lc, "CONNECT {}\r\n")
	checkLeafNodeConnected(t, s)

	leafSend, leafExpect := setupLeaf(t, lc, 2)

	leafSend("LS+ reply\r\nPING\r\n")
	leafExpect(pongRe)

	leafSend("LMSG foo ")
	time.Sleep(time.Millisecond)
	leafSend("+ reply bar 2\r\n")
	time.Sleep(time.Millisecond)
	leafSend("OK\r")
	time.Sleep(time.Millisecond)
	leafSend("\n")
	leafExpect(lmsgRe)
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

func setupLeaf(t *testing.T, lc net.Conn, expectedSubs int) (sendFun, expectFun) {
	t.Helper()
	send, expect := setupConn(t, lc)
	// A loop detection subscription is sent, so consume this here, along
	// with the ones that caller expect on setup.
	expectNumberOfProtos(t, expect, lsubRe, expectedSubs)
	return send, expect
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

	// This should compress down to 1 for foo, 1 for bar, and 1 for foo [baz]
	// and one for the loop detection subject.
	setupLeaf(t, lc, 4)
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

	leafSend, leafExpect := setupLeaf(t, lc, 1)
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

	leafSend, leafExpect := setupLeaf(t, lc, 1)

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

	leafSend, leafExpect := setupLeaf(t, lc, 1)

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

	leafSend, leafExpect := setupLeaf(t, lc, 1)
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

	send("UNSUB 2\r\nPING\r\n")
	expect(pongRe)
	expectNothing(t, lc)
	send("UNSUB 1\r\nPING\r\n")
	expect(pongRe)
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
	checkLeafNodeConnections(t, s, 1)
}

func checkLeafNodeConnections(t *testing.T, s *server.Server, expected int) {
	t.Helper()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if nln := s.NumLeafNodes(); nln != expected {
			return fmt.Errorf("Expected a connected leafnode for server %q, got %d", s.ID(), nln)
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

	leafSend, leafExpect := setupLeaf(t, lc, 1)
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

func (c *cluster) totalSubs() int {
	totalSubs := 0
	for _, s := range c.servers {
		totalSubs += int(s.NumSubscriptions())
	}
	return totalSubs
}

// Wait for the expected number of outbound gateways, or fails.
func waitForOutboundGateways(t *testing.T, s *server.Server, expected int, timeout time.Duration) {
	t.Helper()
	if timeout < 2*time.Second {
		timeout = 2 * time.Second
	}
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
	return createClusterEx(t, false, clusterName, numServers, connectTo...)
}

// Creates a cluster and optionally additional accounts and users.
// Will have Gateways and Leaf Node connections active.
func createClusterEx(t *testing.T, doAccounts bool, clusterName string, numServers int, connectTo ...*cluster) *cluster {
	t.Helper()

	if clusterName == "" || numServers < 1 {
		t.Fatalf("Bad params")
	}

	// Setup some accounts and users.
	// $SYS is always the system account. And we have default FOO and BAR accounts, as well
	// as DLC and NGS which do a service import.
	createAccountsAndUsers := func() ([]*server.Account, []*server.User) {
		if !doAccounts {
			return []*server.Account{server.NewAccount("$SYS")}, nil
		}

		sys := server.NewAccount("$SYS")
		ngs := server.NewAccount("NGS")
		dlc := server.NewAccount("DLC")
		foo := server.NewAccount("FOO")
		bar := server.NewAccount("BAR")

		accounts := []*server.Account{sys, foo, bar, ngs, dlc}

		ngs.AddServiceExport("ngs.usage.*", nil)
		dlc.AddServiceImport(ngs, "ngs.usage", "ngs.usage.dlc")

		// Setup users
		users := []*server.User{
			&server.User{Username: "dlc", Password: "pass", Permissions: nil, Account: dlc},
			&server.User{Username: "ngs", Password: "pass", Permissions: nil, Account: ngs},
			&server.User{Username: "foo", Password: "pass", Permissions: nil, Account: foo},
			&server.User{Username: "bar", Password: "pass", Permissions: nil, Account: bar},
			&server.User{Username: "sys", Password: "pass", Permissions: nil, Account: sys},
		}
		return accounts, users
	}

	bindGlobal := func(s *server.Server) {
		ngs, _ := s.LookupAccount("NGS")
		// Bind global to service import
		gacc, _ := s.LookupAccount("$G")
		gacc.AddServiceImport(ngs, "ngs.usage", "ngs.usage.$G")
	}

	// If we are going to connect to another cluster set that up now for options.
	var gws []*server.RemoteGatewayOpts
	for _, c := range connectTo {
		// Gateways autodiscover here too, so just need one address from the set.
		gwAddr := fmt.Sprintf("nats-gw://%s:%d", c.opts[0].Gateway.Host, c.opts[0].Gateway.Port)
		gwurl, _ := url.Parse(gwAddr)
		gws = append(gws, &server.RemoteGatewayOpts{Name: c.name, URLs: []*url.URL{gwurl}})
	}

	// Make the GWs form faster for the tests.
	server.SetGatewaysSolicitDelay(5 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	// Create seed first.
	o := testDefaultClusterOptionsForLeafNodes()
	o.Gateway.Name = clusterName
	o.Gateway.Gateways = gws
	// All of these need system accounts.
	o.Accounts, o.Users = createAccountsAndUsers()
	o.SystemAccount = "$SYS"
	// Run the server
	s := RunServer(o)
	bindGlobal(s)

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
		o.Accounts, o.Users = createAccountsAndUsers()
		o.SystemAccount = "$SYS"
		s := RunServer(o)
		bindGlobal(s)

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

	// This is for our global responses since we are setting up GWs above.
	leafSend, leafExpect := setupLeaf(t, lc, 3)
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

func TestLeafNodeGatewayInterestPropagation(t *testing.T) {
	server.SetGatewaysSolicitDelay(10 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	ca := createClusterWithName(t, "A", 3)
	defer shutdownCluster(ca)
	cb := createClusterWithName(t, "B", 3, ca)
	defer shutdownCluster(cb)

	sl1, sl1Opts := runSolicitLeafServer(ca.opts[1])
	defer sl1.Shutdown()

	c := createClientConn(t, sl1Opts.Host, sl1Opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("SUB foo 1\r\n")
	send("PING\r\n")
	expect(pongRe)

	// Now we will create a new leaf node on cluster B, expect to get the
	// interest for "foo".
	opts := cb.opts[0]
	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()
	_, leafExpect := setupConn(t, lc)
	var totalBuf []byte
	for count := 0; count != 4; {
		buf := leafExpect(lsubRe)
		totalBuf = append(totalBuf, buf...)
		count += len(lsubRe.FindAllSubmatch(buf, -1))
		if count > 4 {
			t.Fatalf("Expected %v matches, got %v (buf=%s)", 3, count, totalBuf)
		}
	}
	if !strings.Contains(string(totalBuf), "foo") {
		t.Fatalf("Expected interest for 'foo' as 'LS+ foo\\r\\n', got %q", totalBuf)
	}
}

func TestLeafNodeAuthSystemEventNoCrash(t *testing.T) {
	ca := createClusterWithName(t, "A", 1)
	defer shutdownCluster(ca)

	opts := ca.opts[0]
	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend := sendCommand(t, lc)
	leafSend("LS+ foo\r\n")
	checkInfoMsg(t, lc)
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

	// This is for our global responses since we are setting up GWs above.
	leafSend, leafExpect := setupLeaf(t, lc, 3)
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
	leafSend, leafExpect := setupConnWithUserPass(t, lc, "derek", "s3cr3t!")
	leafExpect(lsubRe)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	checkLeafNodeConnected(t, s)
}

func runTLSSolicitLeafServer(lso *server.Options) (*server.Server, *server.Options) {
	o := DefaultTestOptions
	o.Port = -1
	rurl, _ := url.Parse(fmt.Sprintf("nats-leaf://%s:%d", lso.LeafNode.Host, lso.LeafNode.Port))
	remote := &server.RemoteLeafOpts{URLs: []*url.URL{rurl}}
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

type captureLeafNodeErrLogger struct {
	dummyLogger
	ch chan string
}

func (c *captureLeafNodeErrLogger) Errorf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	select {
	case c.ch <- msg:
	default:
	}
}

func TestLeafNodeTLSMixIP(t *testing.T) {
	content := `
	listen: "127.0.0.1:-1"
	leafnodes {
		listen: "127.0.0.1:-1"
        authorization {
			user: dlc
			pass: monkey
		}
		tls {
			cert_file: "./configs/certs/server-noip.pem"
			key_file:  "./configs/certs/server-key-noip.pem"
			ca_file:   "./configs/certs/ca.pem"
		}
	}
	`
	conf := createConfFile(t, []byte(content))
	defer os.Remove(conf)

	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	slContent := `
	listen: "127.0.0.1:-1"
	leafnodes {
		reconnect: 1
		remotes: [
			{
				url: [tls://127.0.0.1:%d, "tls://localhost:%d"]
				tls { ca_file: "./configs/certs/ca.pem" }
			}
		]
	}
	`
	slconf := createConfFile(t, []byte(fmt.Sprintf(slContent, opts.LeafNode.Port, opts.LeafNode.Port)))
	defer os.Remove(slconf)

	// This will fail but we want to make sure in the correct way, not with
	// TLS issue because we used an IP for serverName.
	sl, _ := RunServerWithConfig(slconf)
	defer s.Shutdown()

	ll := &captureLeafNodeErrLogger{ch: make(chan string, 2)}
	sl.SetLogger(ll, false, false)

	// We may or may not get an error depending on timing. For the handshake bug
	// we would always get it, so make sure if we have anything it is not that.
	select {
	case msg := <-ll.ch:
		if strings.Contains(msg, "TLS handshake error") && strings.Contains(msg, "doesn't contain any IP SANs") {
			t.Fatalf("Got bad error about TLS handshake")
		}
	default:
	}
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

func TestLeafNodeSignerUser(t *testing.T) {
	s, opts, conf := runLeafNodeOperatorServer(t)
	defer os.Remove(conf)
	defer s.Shutdown()

	// Setup the two accounts for this server.
	_, akp1 := createAccount(t, s)
	apk1, _ := akp1.PublicKey()

	// add a signing key to the account
	akp2, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	apk2, _ := akp2.PublicKey()

	token, err := s.AccountResolver().Fetch(apk1)
	if err != nil {
		t.Fatal(err)
	}
	ac, err := jwt.DecodeAccountClaims(token)
	if err != nil {
		t.Fatal(err)
	}

	ac.SigningKeys.Add(apk2)
	okp, _ := nkeys.FromSeed(oSeed)
	token, err = ac.Encode(okp)
	if err != nil {
		t.Fatal(err)
	}
	// update the resolver
	account, _ := s.LookupAccount(apk1)
	err = s.AccountResolver().Store(apk1, token)
	if err != nil {
		t.Fatal(err)
	}
	s.UpdateAccountClaims(account, ac)

	tt, err := s.AccountResolver().Fetch(apk1)
	if err != nil {
		t.Fatal(err)
	}
	ac2, err := jwt.DecodeAccountClaims(tt)
	if err != nil {
		t.Fatal(err)
	}
	if len(ac2.SigningKeys) != 1 && ac2.SigningKeys[0] != apk2 {
		t.Fatal("signing key is not added")
	}

	// create an user signed by the signing key
	kp1, _ := nkeys.CreateUser()
	pub1, _ := kp1.PublicKey()
	nuc1 := jwt.NewUserClaims(pub1)
	nuc1.IssuerAccount = apk1
	ujwt1, err := nuc1.Encode(akp2)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	// Create the leaf node server using the first account.
	seed, _ := kp1.Seed()
	mycreds := genCredsFile(t, ujwt1, seed)
	defer os.Remove(mycreds)

	sl, _, lnconf := runSolicitWithCredentials(t, opts, mycreds)
	defer os.Remove(lnconf)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)
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
	// Create listener on nc2
	nc2.Subscribe("req.echo", func(msg *nats.Msg) {
		nc2.Publish(msg.Reply, []byte("WORKED"))
	})
	nc2.Flush()

	// Now send the request on the leaf node client.
	if _, err := ncl.Request("import.request", []byte("fingers crossed"), 500*time.Millisecond); err != nil {
		t.Fatalf("Did not receive response: %v", err)
	}
}

func TestLeadNodeExportImportComplexSetup(t *testing.T) {
	content := `
	port: -1
	operator = "./configs/nkeys/op.jwt"
	resolver = MEMORY
	cluster {
		port: -1
	}
	leafnodes {
		listen: "127.0.0.1:-1"
	}
	`
	conf := createConfFile(t, []byte(content))
	defer os.Remove(conf)
	s1, s1Opts := RunServerWithConfig(conf)
	defer s1.Shutdown()

	content = fmt.Sprintf(`
	port: -1
	operator = "./configs/nkeys/op.jwt"
	resolver = MEMORY
	cluster {
		port: -1
		routes: ["nats://%s:%d"]
	}
	leafnodes {
		listen: "127.0.0.1:-1"
	}
	`, s1Opts.Cluster.Host, s1Opts.Cluster.Port)
	conf = createConfFile(t, []byte(content))
	s2, s2Opts := RunServerWithConfig(conf)
	defer s2.Shutdown()

	// Setup the two accounts for this server.
	okp, _ := nkeys.FromSeed(oSeed)

	// Create second account with exports
	acc2, akp2 := createAccount(t, s1)
	akp2Pub, _ := akp2.PublicKey()
	akp2AC := jwt.NewAccountClaims(akp2Pub)
	streamExport := &jwt.Export{Subject: "foo.stream", Type: jwt.Stream}
	serviceExport := &jwt.Export{Subject: "req.echo", Type: jwt.Service}
	akp2AC.Exports.Add(streamExport, serviceExport)
	akp2ACJWT, err := akp2AC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	if err := s1.AccountResolver().Store(akp2Pub, akp2ACJWT); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	s1.UpdateAccountClaims(acc2, akp2AC)

	// Now create the first account and add on the imports. This will be what is used in the leafnode.
	acc1, akp1 := createAccount(t, s1)
	akp1Pub, _ := akp1.PublicKey()
	akp1AC := jwt.NewAccountClaims(akp1Pub)
	streamImport := &jwt.Import{Account: akp2Pub, Subject: "foo.stream", To: "import", Type: jwt.Stream}
	serviceImport := &jwt.Import{Account: akp2Pub, Subject: "import.request", To: "req.echo", Type: jwt.Service}
	akp1AC.Imports.Add(streamImport, serviceImport)
	akp1ACJWT, err := akp1AC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	if err := s1.AccountResolver().Store(akp1Pub, akp1ACJWT); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	s1.UpdateAccountClaims(acc1, akp1AC)

	if err := s2.AccountResolver().Store(akp2Pub, akp2ACJWT); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	// Just make sure that account object registered in S2 is not acc2
	if a, err := s2.LookupAccount(acc2.Name); err != nil || a == acc2 {
		t.Fatalf("Lookup account error: %v - accounts are same: %v", err, a == acc2)
	}

	if err := s2.AccountResolver().Store(akp1Pub, akp1ACJWT); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	// Just make sure that account object registered in S2 is not acc1
	if a, err := s2.LookupAccount(acc1.Name); err != nil || a == acc1 {
		t.Fatalf("Lookup account error: %v - accounts are same: %v", err, a == acc1)
	}

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

	sl, lopts, lnconf := runSolicitWithCredentials(t, s1Opts, mycreds)
	defer os.Remove(lnconf)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s1)

	// Url to server s2
	s2URL := fmt.Sprintf("nats://%s:%d", s2Opts.Host, s2Opts.Port)

	// Imported
	nc1, err := nats.Connect(s2URL, createUserCreds(t, s2, akp1))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc1.Close()

	// Exported
	nc2, err := nats.Connect(s2URL, createUserCreds(t, s2, akp2))
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

	// Wait for the sub to propagate to s2.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if acc1.RoutedSubs() == 0 {
			return fmt.Errorf("Still no routed subscription")
		}
		return nil
	})

	// Pub to other account with export on original subject.
	nc2.Publish("foo.stream", nil)

	if _, err = lsub.NextMsg(1 * time.Second); err != nil {
		t.Fatalf("Did not receive stream message: %s", err)
	}

	// Services
	// Create listener on nc2 (which connects to s2)
	gotIt := int32(0)
	nc2.Subscribe("req.echo", func(msg *nats.Msg) {
		atomic.AddInt32(&gotIt, 1)
		nc2.Publish(msg.Reply, []byte("WORKED"))
	})
	nc2.Flush()

	// Now send the request on the leaf node client.
	if _, err := ncl.Request("import.request", []byte("fingers crossed"), 5500*time.Millisecond); err != nil {
		if atomic.LoadInt32(&gotIt) == 0 {
			t.Fatalf("Request was not received")
		}
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

			s1.Shutdown()

			// Now we need a configuration where both s1 and s2 have a route
			// to each other, so we need explicit configuration. We are trying
			// to get S1->S2 and S2->S1 so one of the route is dropped. This
			// should not affect the number of URLs reported in INFO.

			opts.Cluster.Port = 5223
			opts.Routes = server.RoutesFromStr(fmt.Sprintf("nats://%s:5224", opts2.Host))
			s1, _ = server.NewServer(opts)
			defer s1.Shutdown()

			opts2.Cluster.Port = 5224
			opts2.Routes = server.RoutesFromStr(fmt.Sprintf("nats://%s:5223", opts.Host))
			s2, _ = server.NewServer(opts2)
			defer s2.Shutdown()

			// Start this way to increase chance of having the two connect
			// to each other at the same time. This will cause one of the
			// route to be dropped.
			wg := &sync.WaitGroup{}
			wg.Add(2)
			go func() {
				s1.Start()
				wg.Done()
			}()
			go func() {
				s2.Start()
				wg.Done()
			}()

			checkClusterFormed(t, s1, s2)

			lc = createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
			defer lc.Close()
			info = checkInfoMsg(t, lc)
			if sz := len(info.LeafNodeURLs); sz != 2 {
				t.Fatalf("Expected LeafNodeURLs array to be size 2, got %v", sz)
			}
			ok[0], ok[1] = 0, 0
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

func TestLeafNodeConnectionLimitsSingleServer(t *testing.T) {
	s, opts, conf := runLeafNodeOperatorServer(t)
	defer os.Remove(conf)
	defer s.Shutdown()

	// Setup account and a user that will be used by the remote leaf node server.
	// createAccount automatically registers with resolver etc..
	acc, akp := createAccount(t, s)

	// Now update with limits for lead node connections.
	const maxleafs = 2

	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	nac.Limits.LeafNodeConn = maxleafs
	s.UpdateAccountClaims(acc, nac)

	// Make sure we have the limits updated in acc.
	if mleafs := acc.MaxActiveLeafNodes(); mleafs != maxleafs {
		t.Fatalf("Expected to have max leafnodes of %d, got %d", maxleafs, mleafs)
	}

	// Create the user credentials for the leadnode connection.
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

	checkLFCount := func(n int) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if nln := s.NumLeafNodes(); nln != n {
				return fmt.Errorf("Number of leaf nodes is %d", nln)
			}
			return nil
		})
	}

	sl, _, lnconf := runSolicitWithCredentials(t, opts, mycreds)
	defer os.Remove(lnconf)
	defer sl.Shutdown()
	checkLFCount(1)

	// Make sure we are accounting properly here.
	if nln := acc.NumLeafNodes(); nln != 1 {
		t.Fatalf("Expected 1 leaf node, got %d", nln)
	}
	// clients and leafnodes counted together.
	if nc := acc.NumConnections(); nc != 1 {
		t.Fatalf("Expected 1 for total connections, got %d", nc)
	}

	s2, _, lnconf2 := runSolicitWithCredentials(t, opts, mycreds)
	defer os.Remove(lnconf2)
	defer s2.Shutdown()
	checkLFCount(2)

	// Make sure we are accounting properly here.
	if nln := acc.NumLeafNodes(); nln != 2 {
		t.Fatalf("Expected 2 leaf nodes, got %d", nln)
	}
	// clients and leafnodes counted together.
	if nc := acc.NumConnections(); nc != 2 {
		t.Fatalf("Expected 2 total connections, got %d", nc)
	}
	s2.Shutdown()
	checkLFCount(1)

	// Make sure we are accounting properly here.
	if nln := acc.NumLeafNodes(); nln != 1 {
		t.Fatalf("Expected 1 leaf node, got %d", nln)
	}
	// clients and leafnodes counted together.
	if nc := acc.NumConnections(); nc != 1 {
		t.Fatalf("Expected 1 for total connections, got %d", nc)
	}

	// Now add back the second one as #3.
	s3, _, lnconf3 := runSolicitWithCredentials(t, opts, mycreds)
	defer os.Remove(lnconf3)
	defer s3.Shutdown()
	checkLFCount(2)

	if nln := acc.NumLeafNodes(); nln != 2 {
		t.Fatalf("Expected 2 leaf nodes, got %d", nln)
	}

	// Once we are here we should not be able to create anymore. Limit == 2.
	s4, _, lnconf4 := runSolicitWithCredentials(t, opts, mycreds)
	defer os.Remove(lnconf4)
	defer s4.Shutdown()

	if nln := acc.NumLeafNodes(); nln != 2 {
		t.Fatalf("Expected 2 leaf nodes, got %d", nln)
	}

	// Make sure s4 has 0 still.
	if nln := s4.NumLeafNodes(); nln != 0 {
		t.Fatalf("Expected no leafnodes accounted for in s4, got %d", nln)
	}

	// Make sure this is still 2.
	checkLFCount(2)
}

func TestLeafNodeConnectionLimitsCluster(t *testing.T) {
	content := `
	port: -1
	operator = "./configs/nkeys/op.jwt"
    system_account = "AD2VB6C25DQPEUUQ7KJBUFX2J4ZNVBPOHSCBISC7VFZXVWXZA7VASQZG"
	resolver = MEMORY
	cluster {
		port: -1
	}
	leafnodes {
		listen: "127.0.0.1:-1"
	}
    resolver_preload = {
        AD2VB6C25DQPEUUQ7KJBUFX2J4ZNVBPOHSCBISC7VFZXVWXZA7VASQZG : "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJDSzU1UERKSUlTWU5QWkhLSUpMVURVVTdJT1dINlM3UkE0RUc2TTVGVUQzUEdGQ1RWWlJRIiwiaWF0IjoxNTQzOTU4NjU4LCJpc3MiOiJPQ0FUMzNNVFZVMlZVT0lNR05HVU5YSjY2QUgyUkxTREFGM01VQkNZQVk1UU1JTDY1TlFNNlhRRyIsInN1YiI6IkFEMlZCNkMyNURRUEVVVVE3S0pCVUZYMko0Wk5WQlBPSFNDQklTQzdWRlpYVldYWkE3VkFTUVpHIiwidHlwZSI6ImFjY291bnQiLCJuYXRzIjp7ImxpbWl0cyI6e319fQ.7m1fysYUsBw15Lj88YmYoHxOI4HlOzu6qgP8Zg-1q9mQXUURijuDGVZrtb7gFYRlo-nG9xZyd2ZTRpMA-b0xCQ"
    }
	`
	conf := createConfFile(t, []byte(content))
	defer os.Remove(conf)
	s1, s1Opts := RunServerWithConfig(conf)
	defer s1.Shutdown()

	content = fmt.Sprintf(`
	port: -1
	operator = "./configs/nkeys/op.jwt"
    system_account = "AD2VB6C25DQPEUUQ7KJBUFX2J4ZNVBPOHSCBISC7VFZXVWXZA7VASQZG"
	resolver = MEMORY
	cluster {
		port: -1
		routes: ["nats://%s:%d"]
	}
	leafnodes {
		listen: "127.0.0.1:-1"
	}
    resolver_preload = {
        AD2VB6C25DQPEUUQ7KJBUFX2J4ZNVBPOHSCBISC7VFZXVWXZA7VASQZG : "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJDSzU1UERKSUlTWU5QWkhLSUpMVURVVTdJT1dINlM3UkE0RUc2TTVGVUQzUEdGQ1RWWlJRIiwiaWF0IjoxNTQzOTU4NjU4LCJpc3MiOiJPQ0FUMzNNVFZVMlZVT0lNR05HVU5YSjY2QUgyUkxTREFGM01VQkNZQVk1UU1JTDY1TlFNNlhRRyIsInN1YiI6IkFEMlZCNkMyNURRUEVVVVE3S0pCVUZYMko0Wk5WQlBPSFNDQklTQzdWRlpYVldYWkE3VkFTUVpHIiwidHlwZSI6ImFjY291bnQiLCJuYXRzIjp7ImxpbWl0cyI6e319fQ.7m1fysYUsBw15Lj88YmYoHxOI4HlOzu6qgP8Zg-1q9mQXUURijuDGVZrtb7gFYRlo-nG9xZyd2ZTRpMA-b0xCQ"
    }
	`, s1Opts.Cluster.Host, s1Opts.Cluster.Port)
	conf = createConfFile(t, []byte(content))
	s2, s2Opts := RunServerWithConfig(conf)
	defer s2.Shutdown()

	// Setup the two accounts for this server.
	okp, _ := nkeys.FromSeed(oSeed)

	// Setup account and a user that will be used by the remote leaf node server.
	// createAccount automatically registers with resolver etc..
	acc, akp := createAccount(t, s1)

	// Now update with limits for lead node connections.
	const maxleafs = 10

	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	nac.Limits.LeafNodeConn = maxleafs

	ajwt, err := nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	if err := s1.AccountResolver().Store(apub, ajwt); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	s1.UpdateAccountClaims(acc, nac)

	if err := s2.AccountResolver().Store(apub, ajwt); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	// Make sure that account object registered in S2 is not acc2
	acc2, err := s2.LookupAccount(acc.Name)
	if err != nil || acc == acc2 {
		t.Fatalf("Lookup account error: %v - accounts are same: %v", err, acc == acc2)
	}

	// Create the user credentials for the leadnode connection.
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

	loop := maxleafs / 2

	// Now create maxleafs/2 leaf node servers on each operator server.
	for i := 0; i < loop; i++ {
		sl1, _, lnconf1 := runSolicitWithCredentials(t, s1Opts, mycreds)
		defer os.Remove(lnconf1)
		defer sl1.Shutdown()

		sl2, _, lnconf2 := runSolicitWithCredentials(t, s2Opts, mycreds)
		defer os.Remove(lnconf2)
		defer sl2.Shutdown()
	}

	checkLFCount := func(s *server.Server, n int) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if nln := s.NumLeafNodes(); nln != n {
				return fmt.Errorf("Number of leaf nodes is %d", nln)
			}
			return nil
		})
	}
	checkLFCount(s1, loop)
	checkLFCount(s2, loop)

	// Now check that we have the remotes registered. This will prove we are sending
	// and processing the leaf node connect events properly etc.
	checkAccRemoteLFCount := func(acc *server.Account, n int) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if nrln := acc.NumRemoteLeafNodes(); nrln != n {
				return fmt.Errorf("Number of remote leaf nodes is %d", nrln)
			}
			return nil
		})
	}
	checkAccRemoteLFCount(acc, loop)
	checkAccRemoteLFCount(acc2, loop)

	// Now that we are here we should not be allowed anymore leaf nodes.
	l, _, lnconf := runSolicitWithCredentials(t, s1Opts, mycreds)
	defer os.Remove(lnconf)
	defer l.Shutdown()

	if nln := acc.NumLeafNodes(); nln != maxleafs {
		t.Fatalf("Expected %d leaf nodes, got %d", maxleafs, nln)
	}
	// Should still be at loop size.
	checkLFCount(s1, loop)

	l, _, lnconf = runSolicitWithCredentials(t, s2Opts, mycreds)
	defer os.Remove(lnconf)
	defer l.Shutdown()
	if nln := acc2.NumLeafNodes(); nln != maxleafs {
		t.Fatalf("Expected %d leaf nodes, got %d", maxleafs, nln)
	}
	// Should still be at loop size.
	checkLFCount(s2, loop)
}

func TestLeafNodeSwitchGatewayToInterestModeOnly(t *testing.T) {
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

	// Send a message from this client on "foo" so that B
	// registers a no-interest for account "$G"
	send("PUB foo 2\r\nok\r\nPING\r\n")
	expect(pongRe)

	// Create a leaf node connection on a server in cluster B
	opts = cb.opts[0]
	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	// This is for our global responses since we are setting up GWs above.
	leafSend, leafExpect := setupLeaf(t, lc, 3)
	leafSend("PING\r\n")
	leafExpect(pongRe)
}

// The MSG proto for routes and gateways is RMSG, and we have an
// optimization that a scratch buffer has RMSG and when doing a
// client we just start at scratch[1]. For leaf nodes its LMSG and we
// rewrite scratch[0], but never reset it which causes protocol
// errors when used with routes or gateways after use to send
// to a leafnode.
// We will create a server with a leafnode connection and a route
// and a gateway connection.

// route connections to simulate.
func TestLeafNodeResetsMSGProto(t *testing.T) {
	opts := testDefaultOptionsForLeafNodes()
	opts.Cluster.Host = opts.Host
	opts.Cluster.Port = -1
	opts.Gateway.Name = "lproto"
	opts.Gateway.Host = opts.Host
	opts.Gateway.Port = -1
	opts.Accounts = []*server.Account{server.NewAccount("$SYS")}
	opts.SystemAccount = "$SYS"

	s := RunServer(opts)
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupConn(t, lc)

	// To avoid possible INFO when switching to interest mode only,
	// delay start of gateway.
	time.Sleep(500 * time.Millisecond)

	gw := createGatewayConn(t, opts.Gateway.Host, opts.Gateway.Port)
	defer gw.Close()

	gwSend, gwExpect := setupGatewayConn(t, gw, "A", "lproto")
	gwSend("PING\r\n")
	gwExpect(pongRe)

	// This is for our global responses since we are setting up GWs above.
	leafExpect(lsubRe)

	// Now setup interest in the leaf node for 'foo'.
	leafSend("LS+ foo\r\nPING\r\n")
	leafExpect(pongRe)

	// Send msg from the gateway.
	gwSend("RMSG $G foo 2\r\nok\r\nPING\r\n")
	gwExpect(pongRe)

	leafExpect(lmsgRe)

	// At this point the gw inside our main server's scratch buffer is LMSG. When we do
	// same with a connected route with interest it should fail.
	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()
	checkInfoMsg(t, rc)
	routeSend, routeExpect := setupRouteEx(t, rc, opts, "RC")

	routeSend("RS+ $G foo\r\nPING\r\n")
	routeExpect(pongRe)

	// This is for the route interest we just created.
	leafExpect(lsubRe)

	// Send msg from the gateway.
	gwSend("RMSG $G foo 2\r\nok\r\nPING\r\n")
	gwExpect(pongRe)

	leafExpect(lmsgRe)

	// Now make sure we get it on route. This will fail with the proto bug.
	routeExpect(rmsgRe)
}

// We need to make sure that as a remote server we also send our local subs on connect.
func TestLeafNodeSendsRemoteSubsOnConnect(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	sl, slOpts := runSolicitLeafServer(opts)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)
	s.Shutdown()

	c := createClientConn(t, slOpts.Host, slOpts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("SUB foo 1\r\n")
	send("PING\r\n")
	expect(pongRe)

	// Need to restart it on the same port.
	s, _ = runLeafServerOnPort(opts.LeafNode.Port)
	checkLeafNodeConnected(t, s)

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	setupLeaf(t, lc, 2)
}

func TestLeafNodeServiceImportLikeNGS(t *testing.T) {
	server.SetGatewaysSolicitDelay(10 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	ca := createClusterEx(t, true, "A", 3)
	defer shutdownCluster(ca)
	cb := createClusterEx(t, true, "B", 3, ca)
	defer shutdownCluster(cb)

	// Hang a responder off of cluster A.
	opts := ca.opts[0]
	url := fmt.Sprintf("nats://ngs:pass@%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Create a queue subscriber to send results
	nc.QueueSubscribe("ngs.usage.*", "ngs", func(m *nats.Msg) {
		m.Respond([]byte("22"))
	})
	nc.Flush()

	// Now create a leafnode server on B.
	opts = cb.opts[1]
	sl, slOpts := runSolicitLeafServer(opts)
	defer sl.Shutdown()

	// Create a normal direct connect client on B.
	url = fmt.Sprintf("nats://dlc:pass@%s:%d", opts.Host, opts.Port)
	nc2, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	if _, err := nc2.Request("ngs.usage", []byte("fingers crossed"), 500*time.Millisecond); err != nil {
		t.Fatalf("Did not receive response: %v", err)
	}

	// Now create a client on the leafnode.
	url = fmt.Sprintf("nats://%s:%d", slOpts.Host, slOpts.Port)
	ncl, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncl.Close()

	if _, err := ncl.Request("ngs.usage", []byte("fingers crossed"), 500*time.Millisecond); err != nil {
		t.Fatalf("Did not receive response: %v", err)
	}
}

func TestLeafNodeSendsAccountingEvents(t *testing.T) {
	s, opts, conf := runLeafNodeOperatorServer(t)
	defer os.Remove(conf)
	defer s.Shutdown()

	// System account
	acc, akp := createAccount(t, s)
	if err := s.SetSystemAccount(acc.Name); err != nil {
		t.Fatalf("Expected this succeed, got %v", err)
	}

	// Leafnode Account
	lacc, lakp := createAccount(t, s)

	// Create a system account user and connect a client to listen for the events.
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Watch only for our leaf node account.
	cSub, _ := nc.SubscribeSync(fmt.Sprintf("$SYS.ACCOUNT.%s.CONNECT", lacc.Name))
	dSub, _ := nc.SubscribeSync(fmt.Sprintf("$SYS.ACCOUNT.%s.DISCONNECT", lacc.Name))
	nc.Flush()

	// Now create creds for the leafnode and connect the server.
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	nuc := jwt.NewUserClaims(pub)
	ujwt, err := nuc.Encode(lakp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	seed, _ := kp.Seed()
	mycreds := genCredsFile(t, ujwt, seed)
	defer os.Remove(mycreds)

	sl, _, lnconf := runSolicitWithCredentials(t, opts, mycreds)
	defer os.Remove(lnconf)
	defer sl.Shutdown()

	// Wait for connect event
	msg, err := cSub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error waiting for account connect event: %v", err)
	}
	m := server.ConnectEventMsg{}
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		t.Fatal("Did not get correctly formatted event")
	}

	// Shutdown leafnode to generate disconnect event.
	sl.Shutdown()

	msg, err = dSub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error waiting for account disconnect event: %v", err)
	}
	dm := server.DisconnectEventMsg{}
	if err := json.Unmarshal(msg.Data, &dm); err != nil {
		t.Fatal("Did not get correctly formatted event")
	}
}

func TestLeafNodeDistributedQueueAcrossGWs(t *testing.T) {
	server.SetGatewaysSolicitDelay(10 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	ca := createClusterEx(t, true, "A", 3)
	defer shutdownCluster(ca)
	cb := createClusterEx(t, true, "B", 3, ca)
	defer shutdownCluster(cb)
	cc := createClusterEx(t, true, "C", 3, ca, cb)
	defer shutdownCluster(cc)

	// Create queue subscribers
	createQS := func(c *cluster) *nats.Conn {
		t.Helper()
		opts := c.opts[rand.Intn(len(c.opts))]
		url := fmt.Sprintf("nats://ngs:pass@%s:%d", opts.Host, opts.Port)
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		nc.QueueSubscribe("ngs.usage.*", "dq", func(m *nats.Msg) {
			m.Respond([]byte(c.name))
		})
		nc.Flush()
		return nc
	}

	ncA := createQS(ca)
	defer ncA.Close()
	ncB := createQS(cb)
	defer ncB.Close()
	ncC := createQS(cc)
	defer ncC.Close()

	connectAndRequest := func(url, clusterName string, nreqs int) {
		t.Helper()
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()
		for i := 0; i < nreqs; i++ {
			m, err := nc.Request("ngs.usage", nil, 500*time.Millisecond)
			if err != nil {
				t.Fatalf("Did not receive a response: %v", err)
			}
			if string(m.Data) != clusterName {
				t.Fatalf("Expected to prefer %q, but got response from %q", clusterName, m.Data)
			}
		}
	}

	checkClientDQ := func(c *cluster, nreqs int) {
		t.Helper()
		// Pick one at random.
		opts := c.opts[rand.Intn(len(c.opts))]
		url := fmt.Sprintf("nats://dlc:pass@%s:%d", opts.Host, opts.Port)
		connectAndRequest(url, c.name, nreqs)
	}

	// First check that this works with direct connected clients.
	checkClientDQ(ca, 100)
	checkClientDQ(cb, 100)
	checkClientDQ(cc, 100)

	createLNS := func(c *cluster) (*server.Server, *server.Options) {
		t.Helper()
		// Pick one at random.
		s, opts := runSolicitLeafServer(c.opts[rand.Intn(len(c.servers))])
		checkLeafNodeConnected(t, s)
		return s, opts
	}

	checkLeafDQ := func(opts *server.Options, clusterName string, nreqs int) {
		t.Helper()
		url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
		connectAndRequest(url, clusterName, nreqs)
	}

	// Test leafnodes to all clusters.
	for _, c := range []*cluster{ca, cb, cc} {
		// Now create a leafnode on cluster.
		sl, slOpts := createLNS(c)
		defer sl.Shutdown()
		// Now connect to the leafnode server and run test.
		checkLeafDQ(slOpts, c.name, 100)
	}
}

func TestLeafNodeDistributedQueueEvenly(t *testing.T) {
	server.SetGatewaysSolicitDelay(10 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	ca := createClusterEx(t, true, "A", 3)
	defer shutdownCluster(ca)
	cb := createClusterEx(t, true, "B", 3, ca)
	defer shutdownCluster(cb)

	// Create queue subscribers
	createQS := func(c *cluster) *nats.Conn {
		t.Helper()
		opts := c.opts[rand.Intn(len(c.opts))]
		url := fmt.Sprintf("nats://ngs:pass@%s:%d", opts.Host, opts.Port)
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		cid, _ := nc.GetClientID()
		response := []byte(fmt.Sprintf("%s:%d:%d", c.name, opts.Port, cid))
		nc.QueueSubscribe("ngs.usage.*", "dq", func(m *nats.Msg) {
			m.Respond(response)
		})
		nc.Flush()
		return nc
	}

	ncA1 := createQS(ca)
	defer ncA1.Close()

	ncA2 := createQS(ca)
	defer ncA2.Close()

	ncA3 := createQS(ca)
	defer ncA3.Close()

	resp := make(map[string]int)

	connectAndRequest := func(url, clusterName string, nreqs int) {
		t.Helper()
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()
		for i := 0; i < nreqs; i++ {
			m, err := nc.Request("ngs.usage", nil, 500*time.Millisecond)
			if err != nil {
				t.Fatalf("Did not receive a response: %v", err)
			}
			if string(m.Data[0]) != clusterName {
				t.Fatalf("Expected to prefer %q, but got response from %q", clusterName, m.Data[0])
			}
			resp[string(m.Data)]++
		}
	}

	createLNS := func(c *cluster) (*server.Server, *server.Options) {
		t.Helper()
		// Pick one at random.
		copts := c.opts[rand.Intn(len(c.servers))]
		s, opts := runSolicitLeafServer(copts)
		checkLeafNodeConnected(t, s)
		return s, opts
	}

	checkLeafDQ := func(opts *server.Options, clusterName string, nreqs int) {
		t.Helper()
		url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
		connectAndRequest(url, clusterName, nreqs)
	}

	// Now create a leafnode on cluster A.
	sl, slOpts := createLNS(ca)
	defer sl.Shutdown()

	// Now connect to the leafnode server and run test.
	checkLeafDQ(slOpts, ca.name, 100)

	// Should have some for all 3 QS [ncA1, ncA2, ncA3]
	if lr := len(resp); lr != 3 {
		t.Fatalf("Expected all 3 queue subscribers to have received some messages, only got %d", lr)
	}
	// Now check that we have at least 10% for each subscriber.
	for _, r := range resp {
		if r < 10 {
			t.Fatalf("Got a subscriber with less than 10 responses: %d", r)
		}
	}
}

func TestLeafNodeDefaultPort(t *testing.T) {
	o := testDefaultOptionsForLeafNodes()
	o.LeafNode.Port = server.DEFAULT_LEAFNODE_PORT
	s := RunServer(o)
	defer s.Shutdown()

	conf := createConfFile(t, []byte(`
		port: -1
		leaf {
			remotes = [
				{
					url: "leafnode://127.0.0.1"
				}
			]
		}
	`))
	defer os.Remove(conf)

	sl, _ := RunServerWithConfig(conf)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)
}

// Since leafnode's are true interest only we need to make sure that we
// register the proper interest with global routing $GR.xxxxxx._INBOX.>
func TestLeafNodeAndGatewayGlobalRouting(t *testing.T) {
	server.SetGatewaysSolicitDelay(50 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	ca := createClusterWithName(t, "A", 3)
	defer shutdownCluster(ca)
	cb := createClusterWithName(t, "B", 3, ca)
	defer shutdownCluster(cb)

	sl, slOpts := runSolicitLeafServer(ca.opts[1])
	defer sl.Shutdown()

	checkLeafNodeConnected(t, ca.servers[1])

	// Create a client on the leafnode. This will listen for requests.
	ncl, err := nats.Connect(fmt.Sprintf("nats://%s:%d", slOpts.Host, slOpts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncl.Close()

	ncl.Subscribe("foo", func(m *nats.Msg) {
		m.Respond([]byte("World"))
	})
	ncl.Flush()

	// Create a direct connect requestor. Try with all possible
	// servers in cluster B to make sure that we also receive the
	// reply when the accepting leafnode server does not have
	// its outbound GW connection to the requestor's server.
	for i := 0; i < 3; i++ {
		opts := cb.opts[i]
		url := fmt.Sprintf("nats://ngs:pass@%s:%d", opts.Host, opts.Port)
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()

		// We don't use an INBOX here because we had a bug where
		// leafnode would subscribe to _GR_.*.*.*.> instead of
		// _GR_.>, and inbox masked that because of their number
		// of tokens.
		reply := nuid.Next()
		sub, err := nc.SubscribeSync(reply)
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		if err := nc.PublishRequest("foo", reply, []byte("Hello")); err != nil {
			t.Fatalf("Failed to get response: %v", err)
		}
		if _, err := sub.NextMsg(250 * time.Millisecond); err != nil {
			t.Fatalf("Did not get reply: %v", err)
		}
		nc.Close()
	}
}

func checkLeafNode2Connected(t *testing.T, s *server.Server) {
	t.Helper()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if nln := s.NumLeafNodes(); nln != 2 {
			return fmt.Errorf("Expected a connected leafnode for server %q, got none", s.ID())
		}
		return nil
	})
}

func TestLeafNodesStaggeredSubPub(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	sl1, sl1Opts := runSolicitLeafServer(opts)
	defer sl1.Shutdown()

	checkLeafNodeConnected(t, s)

	// Create a client on the leafnode and a subscription.
	ncl, err := nats.Connect(fmt.Sprintf("nats://%s:%d", sl1Opts.Host, sl1Opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncl.Close()

	sub, err := ncl.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	sl2, sl2Opts := runSolicitLeafServer(opts)
	defer sl2.Shutdown()

	checkLeafNode2Connected(t, s)

	// Create a client on the second leafnode and publish a message.
	ncl2, err := nats.Connect(fmt.Sprintf("nats://%s:%d", sl2Opts.Host, sl2Opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncl2.Close()

	ncl2.Publish("foo", nil)

	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, err := sub.Pending(); err != nil || nmsgs != 1 {
			return fmt.Errorf("Did not receive the message: %v", err)
		}
		return nil
	})
}

func TestLeafNodeMultipleRemoteURLs(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	content := `
		port: -1
		leafnodes {
			remotes = [
				{
					urls: [nats-leaf://127.0.0.1:%d,nats-leaf://localhost:%d]
				}
			]
		}
		`

	config := fmt.Sprintf(content, opts.LeafNode.Port, opts.LeafNode.Port)
	conf := createConfFile(t, []byte(config))
	sl, _ := RunServerWithConfig(conf)
	defer os.Remove(conf)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)
}

func runSolicitLeafCluster(t *testing.T, clusterName string, d1, d2 *cluster) *cluster {
	c := &cluster{servers: make([]*server.Server, 0, 2), opts: make([]*server.Options, 0, 2), name: clusterName}

	// Who we will solicit for server 1
	ci := rand.Intn(len(d1.opts))
	opts := d1.opts[ci]
	surl := fmt.Sprintf("nats-leaf://%s:%d", opts.LeafNode.Host, opts.LeafNode.Port)

	o := DefaultTestOptions
	o.Port = -1
	rurl, _ := url.Parse(surl)
	o.LeafNode.Remotes = []*server.RemoteLeafOpts{{URLs: []*url.URL{rurl}}}
	o.LeafNode.ReconnectInterval = 100 * time.Millisecond
	o.Cluster.Host = o.Host
	o.Cluster.Port = -1
	s := RunServer(&o)
	checkLeafNodeConnected(t, d1.servers[ci])

	c.servers = append(c.servers, s)
	c.opts = append(c.opts, &o)

	// Grab route info
	routeAddr := fmt.Sprintf("nats-route://%s:%d", o.Cluster.Host, o.Cluster.Port)
	curl, _ := url.Parse(routeAddr)

	// Who we will solicit for server 2
	ci = rand.Intn(len(d2.opts))
	opts = d2.opts[ci]
	surl = fmt.Sprintf("nats-leaf://%s:%d", opts.LeafNode.Host, opts.LeafNode.Port)

	// This is for the case were d1 == d2 and we select the same server.
	plfn := d2.servers[ci].NumLeafNodes()

	o2 := DefaultTestOptions
	o2.Port = -1
	rurl, _ = url.Parse(surl)
	o2.LeafNode.Remotes = []*server.RemoteLeafOpts{{URLs: []*url.URL{rurl}}}
	o2.LeafNode.ReconnectInterval = 100 * time.Millisecond
	o2.Cluster.Host = o.Host
	o2.Cluster.Port = -1
	o2.Routes = []*url.URL{curl}
	s = RunServer(&o2)

	if plfn == 0 {
		checkLeafNodeConnected(t, d2.servers[ci])
	} else {
		checkLeafNode2Connected(t, d2.servers[ci])
	}

	c.servers = append(c.servers, s)
	c.opts = append(c.opts, &o2)

	checkClusterFormed(t, c.servers...)

	return c
}

func clientForCluster(t *testing.T, c *cluster) *nats.Conn {
	t.Helper()
	opts := c.opts[rand.Intn(len(c.opts))]
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	return nc
}

func TestLeafNodeCycleWithSolicited(t *testing.T) {
	server.SetGatewaysSolicitDelay(10 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	// Accepting leafnode cluster, e.g. NGS
	ca := createClusterWithName(t, "A", 3)
	defer shutdownCluster(ca)
	cb := createClusterWithName(t, "B", 3, ca)
	defer shutdownCluster(cb)

	// Create the responders.
	requestsReceived := int32(0)

	nc := clientForCluster(t, ca)
	defer nc.Close()
	nc.QueueSubscribe("request", "cycles", func(m *nats.Msg) {
		atomic.AddInt32(&requestsReceived, 1)
		m.Respond([]byte("22"))
	})
	nc.Flush()

	nc = clientForCluster(t, cb)
	defer nc.Close()
	nc.QueueSubscribe("request", "cycles", func(m *nats.Msg) {
		atomic.AddInt32(&requestsReceived, 1)
		m.Respond([]byte("33"))
	})
	nc.Flush()

	// Soliciting cluster, both solicited connected to the "A" cluster
	sc := runSolicitLeafCluster(t, "SC", ca, ca)
	defer shutdownCluster(sc)

	// Connect a client to a random server in sc
	createClientAndRequest := func(c *cluster) (*nats.Conn, *nats.Subscription) {
		nc := clientForCluster(t, c)
		reply := nats.NewInbox()
		sub, err := nc.SubscribeSync(reply)
		if err != nil {
			t.Fatalf("Could not subscribe: %v", err)
		}
		if err := nc.PublishRequest("request", reply, []byte("fingers crossed")); err != nil {
			t.Fatalf("Error sending request: %v", err)
		}
		return nc, sub
	}

	verifyOneResponse := func(sub *nats.Subscription) {
		time.Sleep(250 * time.Millisecond)
		m, _, err := sub.Pending()
		if err != nil {
			t.Fatalf("Error calling Pending(): %v", err)
		}
		if m > 1 {
			t.Fatalf("Received more then one response, cycle indicated: %d", m)
		}
	}

	verifyRequestTotal := func(nre int32) {
		if nr := atomic.LoadInt32(&requestsReceived); nr != nre {
			t.Fatalf("Expected %d requests received, got %d", nre, nr)
		}
	}

	// This should pass to here, but if we have a cycle things will be spinning and we will receive
	// too many responses when it should only be 1.
	nc, rsub := createClientAndRequest(sc)
	defer nc.Close()
	verifyOneResponse(rsub)
	verifyRequestTotal(1)

	// Do a solicit across GW, so shut this one down.
	nc.Close()
	shutdownCluster(sc)

	// Soliciting cluster, connect to different clusters across a GW.
	sc = runSolicitLeafCluster(t, "SC", ca, cb)
	defer shutdownCluster(sc)

	nc, rsub = createClientAndRequest(sc)
	defer nc.Close()
	verifyOneResponse(rsub)
	verifyRequestTotal(2) // This is total since use same responders.
}

func TestLeafNodeNoRaceGeneratingNonce(t *testing.T) {
	opts := testDefaultOptionsForLeafNodes()
	opts.Cluster.Port = -1
	s := RunServer(opts)
	defer s.Shutdown()

	quitCh := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
			checkInfoMsg(t, lc)
			lc.Close()
			select {
			case <-quitCh:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
			checkInfoMsg(t, rc)
			rc.Close()
			select {
			case <-quitCh:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()

	// Let this run for a bit to see if we get data race
	time.Sleep(100 * time.Millisecond)
	close(quitCh)
	wg.Wait()
}

func runSolicitAndAcceptLeafServer(lso *server.Options) (*server.Server, *server.Options) {
	surl := fmt.Sprintf("nats-leaf://%s:%d", lso.LeafNode.Host, lso.LeafNode.Port)
	o := testDefaultOptionsForLeafNodes()
	o.Port = -1
	rurl, _ := url.Parse(surl)
	o.LeafNode.Remotes = []*server.RemoteLeafOpts{{URLs: []*url.URL{rurl}}}
	o.LeafNode.ReconnectInterval = 100 * time.Millisecond
	return RunServer(o), o
}

func TestLeafNodeDaisyChain(t *testing.T) {
	// To quickly enable trace and debug logging
	// doLog, doTrace, doDebug = true, true, true
	s1, opts1 := runLeafServer()
	defer s1.Shutdown()

	s2, opts2 := runSolicitAndAcceptLeafServer(opts1)
	defer s2.Shutdown()
	checkLeafNodeConnected(t, s1)

	s3, _ := runSolicitLeafServer(opts2)
	defer s3.Shutdown()
	checkLeafNodeConnections(t, s2, 2)

	// Make so we can tell the two apart since in same PID.
	if doLog {
		s1.SetLogger(logger.NewTestLogger("[S-1] - ", false), true, true)
		s2.SetLogger(logger.NewTestLogger("[S-2] - ", false), true, true)
		s3.SetLogger(logger.NewTestLogger("[S-3] - ", false), true, true)
	}

	nc1, err := nats.Connect(s1.ClientURL())
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc1.Close()

	nc1.Subscribe("ngs.usage", func(msg *nats.Msg) {
		msg.Respond([]byte("22 msgs"))
	})
	nc1.Flush()

	nc2, err := nats.Connect(s3.ClientURL())
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc2.Close()

	if _, err = nc2.Request("ngs.usage", []byte("1h"), time.Second); err != nil {
		t.Fatalf("Expected a response")
	}
}
