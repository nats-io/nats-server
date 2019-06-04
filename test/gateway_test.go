// Copyright 2018-2019 The NATS Authors
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
	"bufio"
	"bytes"
	"fmt"
	"net"
	"regexp"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
)

func testDefaultOptionsForGateway(name string) *server.Options {
	o := DefaultTestOptions
	o.Port = -1
	o.Gateway.Name = name
	o.Gateway.Host = "127.0.0.1"
	o.Gateway.Port = -1
	return &o
}

func runGatewayServer(o *server.Options) *server.Server {
	s := RunServer(o)
	return s
}

func createGatewayConn(t testing.TB, host string, port int) net.Conn {
	t.Helper()
	return createClientConn(t, host, port)
}

func setupGatewayConn(t testing.TB, c net.Conn, org, dst string) (sendFun, expectFun) {
	t.Helper()
	dstInfo := checkInfoMsg(t, c)
	if dstInfo.Gateway != dst {
		t.Fatalf("Expected to connect to %q, got %q", dst, dstInfo.Gateway)
	}
	cs := fmt.Sprintf("CONNECT {\"verbose\":%v,\"pedantic\":%v,\"tls_required\":%v,\"gateway\":%q}\r\n",
		false, false, false, org)
	sendProto(t, c, cs)
	sendProto(t, c, fmt.Sprintf("INFO {\"gateway\":%q}\r\n", org))
	return sendCommand(t, c), expectCommand(t, c)
}

func expectNumberOfProtos(t *testing.T, expFn expectFun, proto *regexp.Regexp, expected int) {
	t.Helper()
	for count := 0; count != expected; {
		buf := expFn(proto)
		count += len(proto.FindAllSubmatch(buf, -1))
		if count > expected {
			t.Fatalf("Expected %v matches, got %v", expected, count)
		}
	}
}

func TestGatewayAccountInterest(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	gA := createGatewayConn(t, ob.Gateway.Host, ob.Gateway.Port)
	defer gA.Close()

	gASend, gAExpect := setupGatewayConn(t, gA, "A", "B")
	gASend("PING\r\n")
	gAExpect(pongRe)

	// Sending a bunch of messages. On the first, "B" will send an A-
	// protocol.
	for i := 0; i < 100; i++ {
		gASend("RMSG $foo foo 2\r\nok\r\n")
	}
	// We expect single A- followed by PONG. If "B" was sending more
	// this expect call would fail.
	gAExpect(aunsubRe)
	gASend("PING\r\n")
	gAExpect(pongRe)

	// Start gateway C that connects to B
	gC := createGatewayConn(t, ob.Gateway.Host, ob.Gateway.Port)
	defer gC.Close()

	gCSend, gCExpect := setupGatewayConn(t, gC, "C", "B")
	gCSend("PING\r\n")
	gCExpect(pongRe)
	// Send more messages, C should get A-, but A should not (already
	// got it).
	for i := 0; i < 100; i++ {
		gCSend("RMSG $foo foo 2\r\nok\r\n")
	}
	gCExpect(aunsubRe)
	gCSend("PING\r\n")
	gCExpect(pongRe)
	expectNothing(t, gA)

	// Restart one of the gateway, and resend a message, verify
	// that it receives A- (things get cleared on reconnect)
	gC.Close()
	gC = createGatewayConn(t, ob.Gateway.Host, ob.Gateway.Port)
	defer gC.Close()

	gCSend, gCExpect = setupGatewayConn(t, gC, "C", "B")
	gCSend("PING\r\n")
	gCExpect(pongRe)
	gCSend("RMSG $foo foo 2\r\nok\r\n")
	gCExpect(aunsubRe)
	gCSend("PING\r\n")
	gCExpect(pongRe)
	expectNothing(t, gA)

	// Close again and re-create, but this time don't send anything.
	gC.Close()
	gC = createGatewayConn(t, ob.Gateway.Host, ob.Gateway.Port)
	defer gC.Close()

	gCSend, gCExpect = setupGatewayConn(t, gC, "C", "B")
	gCSend("PING\r\n")
	gCExpect(pongRe)

	// Now register the $foo account on B and create a subscription,
	// A should receive an A+ because B knows that it previously sent
	// an A-, but since it did not send one to C, C should not receive
	// the A+.
	sb.RegisterAccount("$foo")
	client := createClientConn(t, ob.Host, ob.Port)
	defer client.Close()
	clientSend, clientExpect := setupConnWithAccount(t, client, "$foo")
	clientSend("SUB not.used 1234567\r\nPING\r\n")
	clientExpect(pongRe)
	gAExpect(asubRe)
	expectNothing(t, gC)
}

func TestGatewaySubjectInterest(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	fooAcc := server.NewAccount("$foo")
	ob.Accounts = []*server.Account{fooAcc}
	ob.Users = []*server.User{{Username: "ivan", Password: "password", Account: fooAcc}}
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	// Create a client on B
	client := createClientConn(t, ob.Host, ob.Port)
	defer client.Close()
	clientSend, clientExpect := setupConnWithUserPass(t, client, "ivan", "password")
	// Since we want to test RS+/-, we need to have at
	// least a subscription on B so that sending from A does
	// not result in A-
	clientSend("SUB not.used 1234567\r\nPING\r\n")
	clientExpect(pongRe)

	gA := createGatewayConn(t, ob.Gateway.Host, ob.Gateway.Port)
	defer gA.Close()

	gASend, gAExpect := setupGatewayConn(t, gA, "A", "B")
	gASend("PING\r\n")
	gAExpect(pongRe)

	for i := 0; i < 100; i++ {
		gASend("RMSG $foo foo 2\r\nok\r\n")
	}
	// We expect single RS- followed by PONG. If "B" was sending more
	// this expect call would fail.
	gAExpect(runsubRe)
	gASend("PING\r\n")
	gAExpect(pongRe)

	// Start gateway C that connects to B
	gC := createGatewayConn(t, ob.Gateway.Host, ob.Gateway.Port)
	defer gC.Close()

	gCSend, gCExpect := setupGatewayConn(t, gC, "C", "B")
	gCSend("PING\r\n")
	gCExpect(pongRe)
	// Send more messages, C should get RS-, but A should not (already
	// got it).
	for i := 0; i < 100; i++ {
		gCSend("RMSG $foo foo 2\r\nok\r\n")
	}
	gCExpect(runsubRe)
	gCSend("PING\r\n")
	gCExpect(pongRe)
	expectNothing(t, gA)

	// Restart one of the gateway, and resend a message, verify
	// that it receives RS- (things get cleared on reconnect)
	gC.Close()
	gC = createGatewayConn(t, ob.Gateway.Host, ob.Gateway.Port)
	defer gC.Close()

	gCSend, gCExpect = setupGatewayConn(t, gC, "C", "B")
	gCSend("PING\r\n")
	gCExpect(pongRe)
	gCSend("RMSG $foo foo 2\r\nok\r\n")
	gCExpect(runsubRe)
	gCSend("PING\r\n")
	gCExpect(pongRe)
	expectNothing(t, gA)

	// Close again and re-create, but this time don't send anything.
	gC.Close()
	gC = createGatewayConn(t, ob.Gateway.Host, ob.Gateway.Port)
	defer gC.Close()

	gCSend, gCExpect = setupGatewayConn(t, gC, "C", "B")
	gCSend("PING\r\n")
	gCExpect(pongRe)

	// Now register a subscription on foo for account $foo on B.
	// A should receive a RS+ because B knows that it previously
	// sent a RS-, but since it did not send one to C, C should
	// not receive the RS+.
	clientSend("SUB foo 1\r\nSUB foo 2\r\n")
	// Also subscribe to subject that was not used before,
	// so there should be no RS+ for this one.
	clientSend("SUB bar 3\r\nPING\r\n")
	clientExpect(pongRe)

	gAExpect(rsubRe)
	expectNothing(t, gC)
	// Check that we get only one protocol
	expectNothing(t, gA)

	// Unsubscribe the 2 subs on foo, expect to receive nothing.
	clientSend("UNSUB 1\r\nUNSUB 2\r\nPING\r\n")
	clientExpect(pongRe)

	expectNothing(t, gC)
	expectNothing(t, gA)

	gC.Close()

	// Send on foo, should get an RS-
	gASend("RMSG $foo foo 2\r\nok\r\n")
	gAExpect(runsubRe)
	// Subscribe on foo, should get an RS+ that removes the no-interest
	clientSend("SUB foo 4\r\nPING\r\n")
	clientExpect(pongRe)
	gAExpect(rsubRe)
	// Send on bar, message should be received.
	gASend("RMSG $foo bar 2\r\nok\r\n")
	clientExpect(msgRe)
	// Unsub foo and bar
	clientSend("UNSUB 3\r\nUNSUB 4\r\nPING\r\n")
	clientExpect(pongRe)
	expectNothing(t, gA)
	// Send on both foo and bar expect RS-
	gASend("RMSG $foo foo 2\r\nok\r\n")
	gAExpect(runsubRe)
	gASend("RMSG $foo bar 2\r\nok\r\n")
	gAExpect(runsubRe)
	// Now have client create sub on "*", this should cause RS+ on *
	// The remote will have cleared its no-interest on foo and bar
	// and this receiving side is supposed to be doing the same.
	clientSend("SUB * 5\r\nPING\r\n")
	clientExpect(pongRe)
	buf := gAExpect(rsubRe)
	if !bytes.Contains(buf, []byte("$foo *")) {
		t.Fatalf("Expected RS+ on %q, got %q", "*", buf)
	}
	// Check that the remote has cleared by sending from the client
	// on foo and bar
	clientSend("PUB foo 2\r\nok\r\n")
	clientExpect(msgRe)
	clientSend("PUB bar 2\r\nok\r\n")
	clientExpect(msgRe)
	// Check that A can send too and does not receive an RS-
	gASend("RMSG $foo foo 2\r\nok\r\n")
	expectNothing(t, gA)
	clientExpect(msgRe)
	gASend("RMSG $foo bar 2\r\nok\r\n")
	expectNothing(t, gA)
	clientExpect(msgRe)
}

func TestGatewayQueue(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	fooAcc := server.NewAccount("$foo")
	ob.Accounts = []*server.Account{fooAcc}
	ob.Users = []*server.User{{Username: "ivan", Password: "password", Account: fooAcc}}
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	gA := createGatewayConn(t, ob.Gateway.Host, ob.Gateway.Port)
	defer gA.Close()

	gASend, gAExpect := setupGatewayConn(t, gA, "A", "B")
	gASend("PING\r\n")
	gAExpect(pongRe)

	client := createClientConn(t, ob.Host, ob.Port)
	defer client.Close()
	clientSend, clientExpect := setupConnWithUserPass(t, client, "ivan", "password")

	// Create one queue sub on foo.* for group bar.
	clientSend("SUB foo.* bar 1\r\nPING\r\n")
	clientExpect(pongRe)
	// Expect RS+
	gAExpect(rsubRe)
	// Add another queue sub on same group
	clientSend("SUB foo.* bar 2\r\nPING\r\n")
	clientExpect(pongRe)
	// Should not receive another RS+ for that one
	expectNothing(t, gA)
	// However, if subject is different, we can expect to receive another RS+
	clientSend("SUB foo.> bar 3\r\nPING\r\n")
	clientExpect(pongRe)
	gAExpect(rsubRe)

	// Unsub one of the foo.* qsub, no RS- should be received
	clientSend("UNSUB 1\r\nPING\r\n")
	clientExpect(pongRe)
	expectNothing(t, gA)
	// Remove the other one, now we should get the RS-
	clientSend("UNSUB 2\r\nPING\r\n")
	clientExpect(pongRe)
	gAExpect(runsubRe)
	// Remove last one
	clientSend("UNSUB 3\r\nPING\r\n")
	clientExpect(pongRe)
	gAExpect(runsubRe)

	// Create some queues and check that interest is sent
	// when GW reconnects.
	clientSend("SUB foo bar 4\r\n")
	gAExpect(rsubRe)
	clientSend("SUB foo baz 5\r\n")
	gAExpect(rsubRe)
	clientSend("SUB foo bat 6\r\n")
	gAExpect(rsubRe)
	// There is already one on foo/bar, so nothing sent
	clientSend("SUB foo bar 7\r\n")
	expectNothing(t, gA)
	// Add regular sub that should not cause RS+
	clientSend("SUB foo 8\r\n")
	expectNothing(t, gA)

	// Recreate gA
	gA.Close()
	gA = createGatewayConn(t, ob.Gateway.Host, ob.Gateway.Port)
	defer gA.Close()
	gASend, gAExpect = setupGatewayConn(t, gA, "A", "B")
	// A should receive 3 RS+
	expectNumberOfProtos(t, gAExpect, rsubRe, 3)
	// Nothing more
	expectNothing(t, gA)
	gASend("PING\r\n")
	gAExpect(pongRe)

	// Have A send a message on subject that has no sub
	gASend("RMSG $foo new.subject 2\r\nok\r\n")
	gAExpect(runsubRe)
	// Now create a queue sub and check that we do not receive
	// an RS+ without the queue name.
	clientSend("SUB new.* queue 9\r\nPING\r\n")
	clientExpect(pongRe)
	buf := gAExpect(rsubRe)
	if !bytes.Contains(buf, []byte("new.* queue")) {
		t.Fatalf("Should have receives RS+ for new.* for queue, did not: %v", buf)
	}
	// Check for no other RS+. A should still keep an RS- for plain
	// sub on new.subject
	expectNothing(t, gA)
	// Send message, expected to be received by client
	gASend("RMSG $foo new.subject | queue 2\r\nok\r\n")
	clientExpect(msgRe)
	// Unsubscribe the queue sub
	clientSend("UNSUB 9\r\nPING\r\n")
	clientExpect(pongRe)
	// A should receive RS- for this queue sub
	buf = gAExpect(runsubRe)
	if !bytes.Contains(buf, []byte("new.* queue")) {
		t.Fatalf("Should have receives RS- for new.* for queue, did not: %v", buf)
	}
	expectNothing(t, gA)
}

func TestGatewaySendAllSubs(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	// Create a client on B
	client := createClientConn(t, ob.Host, ob.Port)
	defer client.Close()
	clientSend, clientExpect := setupConn(t, client)
	// Since we want to test RS+/-, we need to have at
	// least a subscription on B so that sending from A does
	// not result in A-
	clientSend("SUB not.used 1234567\r\nPING\r\n")
	clientExpect(pongRe)

	gA := createGatewayConn(t, ob.Gateway.Host, ob.Gateway.Port)
	defer gA.Close()

	gASend, gAExpect := setupGatewayConn(t, gA, "A", "B")
	gASend("PING\r\n")
	gAExpect(pongRe)

	// Bombard B with messages on different subjects.
	// TODO(ik): Adapt if/when we change the conditions for the
	// switch.
	for i := 0; i < 1010; i++ {
		gASend(fmt.Sprintf("RMSG $G foo.%d 2\r\nok\r\n", i))
		if i < 1000 {
			gAExpect(runsubRe)
		}
	}
	// Expect an INFO + RS+ $G not.used + INFO
	buf := bufio.NewReader(gA)
	for i := 0; i < 3; i++ {
		line, _, err := buf.ReadLine()
		if err != nil {
			t.Fatalf("Error reading: %v", err)
		}
		switch i {
		case 0:
		case 2:
			if !bytes.HasPrefix(line, []byte("INFO {")) {
				t.Fatalf("Expected INFO, got: %s", line)
			}
		case 1:
			if !bytes.HasPrefix(line, []byte("RS+ ")) {
				t.Fatalf("Expected RS+, got: %s", line)
			}
		}
	}
	// After this point, any new sub or unsub on B should be
	// sent to A.
	clientSend("SUB foo 1\r\n")
	gAExpect(rsubRe)
	clientSend("UNSUB 1\r\n")
	gAExpect(runsubRe)
}

func TestGatewayNoPanicOnBadProtocol(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	for _, test := range []struct {
		name  string
		proto string
	}{
		{"sub", "SUB > 1\r\n"},
		{"unsub", "UNSUB 1\r\n"},
		{"rsub", "RS+ $foo foo 2\r\n"},
		{"runsub", "RS- $foo foo 2\r\n"},
		{"pub", "PUB foo 2\r\nok\r\n"},
		{"msg", "MSG foo 2\r\nok\r\n"},
		{"rmsg", "RMSG $foo foo 2\r\nok\r\n"},
		{"anything", "xxxx\r\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Create raw tcp connection to gateway port
			client := createClientConn(t, ob.Gateway.Host, ob.Gateway.Port)
			defer client.Close()
			clientSend := sendCommand(t, client)
			clientSend(test.proto)
		})
	}

	// Server should not have crashed.
	client := createClientConn(t, ob.Host, ob.Port)
	defer client.Close()
	clientSend, clientExpect := setupConn(t, client)
	clientSend("PING\r\n")
	clientExpect(pongRe)
}

func TestGatewayNoAccUnsubAfterQSub(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	gA := createGatewayConn(t, ob.Gateway.Host, ob.Gateway.Port)
	defer gA.Close()

	gASend, gAExpect := setupGatewayConn(t, gA, "A", "B")
	gASend("PING\r\n")
	gAExpect(pongRe)

	// Simulate a client connecting to A and publishing a message
	// so we get an A- from B since there is no interest.
	gASend("RMSG $G foo 2\r\nok\r\n")
	gAExpect(aunsubRe)

	// Now create client on B and create queue sub.
	client := createClientConn(t, ob.Host, ob.Port)
	defer client.Close()
	clientSend, clientExpect := setupConn(t, client)

	clientSend("SUB bar queue 1\r\nPING\r\n")
	clientExpect(pongRe)

	// A should receive an RS+ for this queue sub.
	gAExpect(rsubRe)

	// On B, create a plain sub now. We should get nothing.
	clientSend("SUB baz 2\r\nPING\r\n")
	clientExpect(pongRe)

	expectNothing(t, gA)
}
