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
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

func runNewRouteServer(t *testing.T) (*server.Server, *server.Options) {
	return RunServerWithConfig("./configs/new_cluster.conf")
}

func TestNewRouteInfoOnConnect(t *testing.T) {
	s, opts := runNewRouteServer(t)
	defer s.Shutdown()

	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	info := checkInfoMsg(t, rc)
	if info.Port != opts.Cluster.Port {
		t.Fatalf("Received wrong information for port, expected %d, got %d",
			info.Port, opts.Cluster.Port)
	}

	// Make sure we advertise new proto.
	if info.Proto < server.RouteProtoV2 {
		t.Fatalf("Expected routeProtoV2, got %d", info.Proto)
	}
	// New proto should always send nonce too.
	if info.Nonce == "" {
		t.Fatalf("Expected a non empty nonce in new route INFO")
	}
}

func TestNewRouteConnectSubs(t *testing.T) {
	s, opts := runNewRouteServer(t)
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)

	// Create 10 normal subs and 10 queue subscribers.
	for i := 0; i < 10; i++ {
		send(fmt.Sprintf("SUB foo %d\r\n", i))
		send(fmt.Sprintf("SUB foo bar %d\r\n", 100+i))
	}
	send("PING\r\n")
	expect(pongRe)

	// This client should not be considered active since no subscriptions or
	// messages have been published.
	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	routeID := "RTEST_NEW:22"
	routeSend, routeExpect := setupRouteEx(t, rc, opts, routeID)

	info := checkInfoMsg(t, rc)

	info.ID = routeID
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	routeSend(fmt.Sprintf("INFO %s\r\n", b))

	buf := routeExpect(rsubRe)

	matches := rsubRe.FindAllSubmatch(buf, -1)
	if len(matches) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(matches))
	}
	for _, m := range matches {
		if string(m[1]) != "$G" {
			t.Fatalf("Expected global account name of '$G', got %q", m[1])
		}
		if string(m[2]) != "foo" {
			t.Fatalf("Expected subject of 'foo', got %q", m[2])
		}
		if m[3] != nil {
			if string(m[3]) != "bar" {
				t.Fatalf("Expected group of 'bar', got %q", m[3])
			}
			// Expect a weighted count for the queue group
			if len(m) != 5 {
				t.Fatalf("Expected a weight for the queue group")
			}
			if m[4] == nil || string(m[4]) != "10" {
				t.Fatalf("Expected Weight of '10', got %q", m[4])
			}
		}
	}

	// Close the client connection, check the results.
	c.Close()

	// Expect 2
	for numUnSubs := 0; numUnSubs != 2; {
		buf := routeExpect(runsubRe)
		numUnSubs += len(runsubRe.FindAllSubmatch(buf, -1))
	}
}

func TestNewRouteConnectSubsWithAccount(t *testing.T) {
	s, opts := runNewRouteServer(t)
	defer s.Shutdown()

	accName := "$FOO"
	s.RegisterAccount(accName)

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConnWithAccount(t, c, accName)

	// Create 10 normal subs and 10 queue subscribers.
	for i := 0; i < 10; i++ {
		send(fmt.Sprintf("SUB foo %d\r\n", i))
		send(fmt.Sprintf("SUB foo bar %d\r\n", 100+i))
	}
	send("PING\r\n")
	expect(pongRe)

	// This client should not be considered active since no subscriptions or
	// messages have been published.
	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	routeID := "RTEST_NEW:22"
	routeSend, routeExpect := setupRouteEx(t, rc, opts, routeID)

	info := checkInfoMsg(t, rc)

	info.ID = routeID
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	routeSend(fmt.Sprintf("INFO %s\r\n", b))

	buf := routeExpect(rsubRe)

	matches := rsubRe.FindAllSubmatch(buf, -1)
	if len(matches) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(matches))
	}
	for _, m := range matches {
		if string(m[1]) != accName {
			t.Fatalf("Expected global account name of %q, got %q", accName, m[1])
		}
		if string(m[2]) != "foo" {
			t.Fatalf("Expected subject of 'foo', got %q", m[2])
		}
		if m[3] != nil {
			if string(m[3]) != "bar" {
				t.Fatalf("Expected group of 'bar', got %q", m[3])
			}
			// Expect the SID to be the total weighted count for the queue group
			if len(m) != 5 {
				t.Fatalf("Expected a weight for the queue group")
			}
			if m[4] == nil || string(m[4]) != "10" {
				t.Fatalf("Expected Weight of '10', got %q", m[4])
			}
		}
	}

	// Close the client connection, check the results.
	c.Close()

	// Expect 2
	for numUnSubs := 0; numUnSubs != 2; {
		buf := routeExpect(runsubRe)
		numUnSubs += len(runsubRe.FindAllSubmatch(buf, -1))
	}
}

func TestNewRouteRSubs(t *testing.T) {
	s, opts := runNewRouteServer(t)
	defer s.Shutdown()

	foo, err := s.RegisterAccount("$foo")
	if err != nil {
		t.Fatalf("Error creating account '$foo': %v", err)
	}
	bar, err := s.RegisterAccount("$bar")
	if err != nil {
		t.Fatalf("Error creating account '$bar': %v", err)
	}

	// Create a client an account foo.
	clientA := createClientConn(t, opts.Host, opts.Port)
	sendA, expectA := setupConnWithAccount(t, clientA, "$foo")
	defer clientA.Close()
	sendA("PING\r\n")
	expectA(pongRe)

	if foonc := foo.NumConnections(); foonc != 1 {
		t.Fatalf("Expected foo account to have 1 client, got %d", foonc)
	}
	if barnc := bar.NumConnections(); barnc != 0 {
		t.Fatalf("Expected bar account to have 0 clients, got %d", barnc)
	}

	// Create a routeConn
	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	routeID := "RTEST_NEW:33"
	routeSend, routeExpect := setupRouteEx(t, rc, opts, routeID)

	info := checkInfoMsg(t, rc)
	info.ID = routeID
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	routeSend(fmt.Sprintf("INFO %s\r\n", b))
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Have the client listen on foo.
	sendA("SUB foo 1\r\nPING\r\n")
	expectA(pongRe)

	// Now create a new client for account $bar and have them subscribe.
	clientB := createClientConn(t, opts.Host, opts.Port)
	sendB, expectB := setupConnWithAccount(t, clientB, "$bar")
	defer clientB.Close()

	sendB("PING\r\n")
	expectB(pongRe)

	if foonc := foo.NumConnections(); foonc != 1 {
		t.Fatalf("Expected foo account to have 1 client, got %d", foonc)
	}
	if barnc := bar.NumConnections(); barnc != 1 {
		t.Fatalf("Expected bar account to have 1 client, got %d", barnc)
	}

	// Have the client listen on foo.
	sendB("SUB foo 1\r\nPING\r\n")
	expectB(pongRe)

	routeExpect(rsubRe)

	// Unsubscribe on clientA from foo subject.
	sendA("UNSUB 1\r\nPING\r\n")
	expectA(pongRe)

	// We should get an RUSUB here.
	routeExpect(runsubRe)

	// Now unsubscribe clientB, which should trigger an RS-.
	sendB("UNSUB 1\r\nPING\r\n")
	expectB(pongRe)
	// We should get an RUSUB here.
	routeExpect(runsubRe)

	// Now close down the clients.
	clientA.Close()

	sendB("SUB foo 2\r\nPING\r\n")
	expectB(pongRe)

	routeExpect(rsubRe)

	// Now close down client B.
	clientB.Close()

	// This should trigger an RS-
	routeExpect(runsubRe)
}

func TestNewRouteProgressiveNormalSubs(t *testing.T) {
	s, opts := runNewRouteServer(t)
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)

	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	routeID := "RTEST_NEW:33"
	routeSend, routeExpect := setupRouteEx(t, rc, opts, routeID)

	info := checkInfoMsg(t, rc)
	info.ID = routeID
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	routeSend(fmt.Sprintf("INFO %s\r\n", b))
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// For progressive we will expect to receive first normal sub but
	// not subsequent ones.
	send("SUB foo 1\r\nPING\r\n")
	expect(pongRe)

	routeExpect(rsubRe)

	send("SUB foo 2\r\nPING\r\n")
	expect(pongRe)
	expectNothing(t, rc)

	var buf []byte

	// Check that sid is showing us total number of subscriptions.
	checkQueueSub := func(n string) {
		matches := rsubRe.FindAllSubmatch(buf, -1)
		if len(matches) != 1 {
			t.Fatalf("Expected 1 result, got %d", len(matches))
		}
		m := matches[0]
		if len(m) != 5 {
			t.Fatalf("Expected a SID for the queue group, only got %d elements", len(m))
		}
		if string(m[4]) != n {
			t.Fatalf("Expected %q, got %q", n, m[4])
		}
	}

	// We should always get the SUB info for QUEUES.
	send("SUB foo bar 3\r\nPING\r\n")
	expect(pongRe)
	buf = routeExpect(rsubRe)
	checkQueueSub("1")

	send("SUB foo bar 4\r\nPING\r\n")
	expect(pongRe)
	buf = routeExpect(rsubRe)
	checkQueueSub("2")

	send("SUB foo bar 5\r\nPING\r\n")
	expect(pongRe)
	buf = routeExpect(rsubRe)
	checkQueueSub("3")

	// Now walk them back down.
	// Again we should always get updates for queue subscribers.
	// And these will be RS+ protos walking the weighted count back down.
	send("UNSUB 5\r\nPING\r\n")
	expect(pongRe)
	buf = routeExpect(rsubRe)
	checkQueueSub("2")

	send("UNSUB 4\r\nPING\r\n")
	expect(pongRe)
	buf = routeExpect(rsubRe)
	checkQueueSub("1")

	// This one should send UNSUB
	send("UNSUB 3\r\nPING\r\n")
	expect(pongRe)
	routeExpect(runsubRe)

	// Now normal ones.
	send("UNSUB 1\r\nPING\r\n")
	expect(pongRe)
	expectNothing(t, rc)

	send("UNSUB 2\r\nPING\r\n")
	expect(pongRe)
	routeExpect(runsubRe)
}

func TestNewRouteClientClosedWithNormalSubscriptions(t *testing.T) {
	s, opts := runNewRouteServer(t)
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)

	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	routeID := "RTEST_NEW:44"
	routeSend, routeExpect := setupRouteEx(t, rc, opts, routeID)

	info := checkInfoMsg(t, rc)
	info.ID = routeID
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	routeSend(fmt.Sprintf("INFO %s\r\n", b))
	routeSend("PING\r\n")
	routeExpect(pongRe)

	send("SUB foo 1\r\nPING\r\n")
	expect(pongRe)
	routeExpect(rsubRe)

	for i := 2; i < 100; i++ {
		send(fmt.Sprintf("SUB foo %d\r\n", i))
	}
	send("PING\r\n")
	expect(pongRe)

	// Expect nothing from the route.
	expectNothing(t, rc)

	// Now close connection.
	c.Close()
	expectNothing(t, c)

	buf := routeExpect(runsubRe)
	matches := runsubRe.FindAllSubmatch(buf, -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 unsub response when closing client connection, got %d", len(matches))
	}
}

func TestNewRouteClientClosedWithQueueSubscriptions(t *testing.T) {
	s, opts := runNewRouteServer(t)
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)

	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	routeID := "RTEST_NEW:44"
	routeSend, routeExpect := setupRouteEx(t, rc, opts, routeID)

	info := checkInfoMsg(t, rc)
	info.ID = routeID
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	routeSend(fmt.Sprintf("INFO %s\r\n", b))
	routeSend("PING\r\n")
	routeExpect(pongRe)

	for i := 0; i < 100; i++ {
		send(fmt.Sprintf("SUB foo bar %d\r\n", i))
	}
	send("PING\r\n")
	expect(pongRe)

	// Queue subscribers will send all updates.
	for numRSubs := 0; numRSubs != 100; {
		buf := routeExpect(rsubRe)
		numRSubs += len(rsubRe.FindAllSubmatch(buf, -1))
	}

	// Now close connection.
	c.Close()
	expectNothing(t, c)

	// We should only get one unsub for the queue subscription.
	matches := runsubRe.FindAllSubmatch(routeExpect(runsubRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 unsub response when closing client connection, got %d", len(matches))
	}
}

func TestNewRouteRUnsubAccountSpecific(t *testing.T) {
	s, opts := runNewRouteServer(t)
	defer s.Shutdown()

	// Allow new accounts to be created on the fly.
	opts.AllowNewAccounts = true

	// Create a routeConn
	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	routeID := "RTEST_NEW:77"
	routeSend, routeExpect := setupRouteEx(t, rc, opts, routeID)

	info := checkInfoMsg(t, rc)
	info.ID = routeID
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	routeSend(fmt.Sprintf("INFO %s\r\n", b))

	// Now create 500 subs on same subject but all different accounts.
	for i := 0; i < 500; i++ {
		account := fmt.Sprintf("$foo.account.%d", i)
		routeSend(fmt.Sprintf("RS+ %s foo\r\n", account))
	}
	routeSend("PING\r\n")
	routeExpect(pongRe)

	routeSend("RS- $foo.account.22 foo\r\nPING\r\n")
	routeExpect(pongRe)

	// Do not expect a message on that account.
	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConnWithAccount(t, c, "$foo.account.22")
	send("PUB foo 2\r\nok\r\nPING\r\n")
	expect(pongRe)
	c.Close()

	// But make sure we still receive on others
	c = createClientConn(t, opts.Host, opts.Port)
	defer c.Close()
	send, expect = setupConnWithAccount(t, c, "$foo.account.33")
	send("PUB foo 2\r\nok\r\nPING\r\n")
	expect(pongRe)

	matches := rmsgRe.FindAllSubmatch(routeExpect(rmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkRmsg(t, matches[0], "$foo.account.33", "foo", "", "2", "ok")
}

func TestNewRouteRSubCleanupOnDisconnect(t *testing.T) {
	s, opts := runNewRouteServer(t)
	defer s.Shutdown()

	// Allow new accounts to be created on the fly.
	opts.AllowNewAccounts = true

	// Create a routeConn
	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	routeID := "RTEST_NEW:77"
	routeSend, routeExpect := setupRouteEx(t, rc, opts, routeID)

	info := checkInfoMsg(t, rc)
	info.ID = routeID
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	routeSend(fmt.Sprintf("INFO %s\r\n", b))

	// Now create 100 subs on 3 different accounts.
	for i := 0; i < 100; i++ {
		subject := fmt.Sprintf("foo.%d", i)
		routeSend(fmt.Sprintf("RS+ $foo %s\r\n", subject))
		routeSend(fmt.Sprintf("RS+ $bar %s\r\n", subject))
		routeSend(fmt.Sprintf("RS+ $baz %s bar %d\r\n", subject, i+1))
	}
	routeSend("PING\r\n")
	routeExpect(pongRe)

	rc.Close()

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if ns := s.NumSubscriptions(); ns != 0 {
			return fmt.Errorf("Number of subscriptions is %d", ns)
		}
		return nil
	})
}

func TestNewRouteSendSubsAndMsgs(t *testing.T) {
	s, opts := runNewRouteServer(t)
	defer s.Shutdown()

	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	routeID := "RTEST_NEW:44"
	routeSend, routeExpect := setupRouteEx(t, rc, opts, routeID)

	info := checkInfoMsg(t, rc)
	info.ID = routeID
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	routeSend(fmt.Sprintf("INFO %s\r\n", b))
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Now let's send in interest from the new protocol.
	// Normal Subscription
	routeSend("RS+ $G foo\r\n")
	// Make sure things were processed.
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Now create a client and send a message, make sure we receive it
	// over the route connection.
	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PUB foo 2\r\nok\r\nPING\r\n")
	expect(pongRe)

	buf := routeExpect(rmsgRe)
	matches := rmsgRe.FindAllSubmatch(buf, -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkRmsg(t, matches[0], "$G", "foo", "", "2", "ok")

	// Queue Subscription
	routeSend("RS+ $G foo bar 1\r\n")
	// Make sure things were processed.
	routeSend("PING\r\n")
	routeExpect(pongRe)

	send("PUB foo reply 2\r\nok\r\nPING\r\n")
	expect(pongRe)

	matches = rmsgRe.FindAllSubmatch(routeExpect(rmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkRmsg(t, matches[0], "$G", "foo", "+ reply bar", "2", "ok")

	// Another Queue Subscription
	routeSend("RS+ $G foo baz 1\r\n")
	// Make sure things were processed.
	routeSend("PING\r\n")
	routeExpect(pongRe)

	send("PUB foo reply 2\r\nok\r\nPING\r\n")
	expect(pongRe)

	matches = rmsgRe.FindAllSubmatch(routeExpect(rmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkRmsg(t, matches[0], "$G", "foo", "+ reply bar baz", "2", "ok")

	// Matching wildcard
	routeSend("RS+ $G *\r\n")
	// Make sure things were processed.
	routeSend("PING\r\n")
	routeExpect(pongRe)

	send("PUB foo reply 2\r\nok\r\nPING\r\n")
	expect(pongRe)

	matches = rmsgRe.FindAllSubmatch(routeExpect(rmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkRmsg(t, matches[0], "$G", "foo", "+ reply bar baz", "2", "ok")

	// No reply
	send("PUB foo 2\r\nok\r\nPING\r\n")
	expect(pongRe)

	matches = rmsgRe.FindAllSubmatch(routeExpect(rmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkRmsg(t, matches[0], "$G", "foo", "| bar baz", "2", "ok")

	// Now unsubscribe from the queue group.
	routeSend("RS- $G foo baz\r\n")
	routeSend("RS- $G foo bar\r\n")

	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Now send and make sure they are removed. We should still get the message.
	send("PUB foo 2\r\nok\r\nPING\r\n")
	expect(pongRe)

	matches = rmsgRe.FindAllSubmatch(routeExpect(rmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkRmsg(t, matches[0], "$G", "foo", "", "2", "ok")

	routeSend("RS- $G foo\r\n")
	routeSend("RS- $G *\r\n")

	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Now we should not receive messages anymore.
	send("PUB foo 2\r\nok\r\nPING\r\n")
	expect(pongRe)

	expectNothing(t, rc)
}

func TestNewRouteProcessRoutedMsgs(t *testing.T) {
	s, opts := runNewRouteServer(t)
	defer s.Shutdown()

	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	routeID := "RTEST_NEW:55"
	routeSend, routeExpect := setupRouteEx(t, rc, opts, routeID)

	info := checkInfoMsg(t, rc)
	info.ID = routeID
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	routeSend(fmt.Sprintf("INFO %s\r\n", b))
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Create a client
	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)

	// Normal sub to start
	send("SUB foo 1\r\nPING\r\n")
	expect(pongRe)
	routeExpect(rsubRe)

	expectMsgs := expectMsgsCommand(t, expect)

	// Now send in a RMSG to the route and make sure its delivered to the client.
	routeSend("RMSG $G foo 2\r\nok\r\nPING\r\n")
	routeExpect(pongRe)
	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "1", "", "2", "ok")

	// Now send in a RMSG to the route witha reply and make sure its delivered to the client.
	routeSend("RMSG $G foo reply 2\r\nok\r\nPING\r\n")
	routeExpect(pongRe)

	matches = expectMsgs(1)
	checkMsg(t, matches[0], "foo", "1", "reply", "2", "ok")

	// Now add in a queue subscriber for the client.
	send("SUB foo bar 11\r\nPING\r\n")
	expect(pongRe)
	routeExpect(rsubRe)

	// Now add in another queue subscriber for the client.
	send("SUB foo baz 22\r\nPING\r\n")
	expect(pongRe)
	routeExpect(rsubRe)

	// If we send from a route with no queues. Should only get one message.
	routeSend("RMSG $G foo 2\r\nok\r\nPING\r\n")
	routeExpect(pongRe)
	matches = expectMsgs(1)
	checkMsg(t, matches[0], "foo", "1", "", "2", "ok")

	// Now send to a specific queue group. We should get multiple messages now.
	routeSend("RMSG $G foo | bar 2\r\nok\r\nPING\r\n")
	routeExpect(pongRe)
	matches = expectMsgs(2)
	checkMsg(t, matches[0], "foo", "1", "", "2", "ok")

	// Now send to both queue groups. We should get all messages now.
	routeSend("RMSG $G foo | bar baz 2\r\nok\r\nPING\r\n")
	routeExpect(pongRe)
	matches = expectMsgs(3)
	checkMsg(t, matches[0], "foo", "1", "", "2", "ok")

	// Make sure we do the right thing with reply.
	routeSend("RMSG $G foo + reply bar baz 2\r\nok\r\nPING\r\n")
	routeExpect(pongRe)
	matches = expectMsgs(3)
	checkMsg(t, matches[0], "foo", "1", "reply", "2", "ok")
}

func TestNewRouteQueueSubsDistribution(t *testing.T) {
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	clientB := createClientConn(t, optsB.Host, optsB.Port)
	defer clientB.Close()

	sendA, expectA := setupConn(t, clientA)
	sendB, expectB := setupConn(t, clientB)

	// Create 100 subscribers on each server.
	for i := 0; i < 100; i++ {
		sproto := fmt.Sprintf("SUB foo bar %d\r\n", i)
		sendA(sproto)
		sendB(sproto)
	}
	sendA("PING\r\n")
	expectA(pongRe)
	sendB("PING\r\n")
	expectB(pongRe)

	sender := createClientConn(t, optsA.Host, optsA.Port)
	defer sender.Close()
	send, expect := setupConn(t, sender)

	// Send 100 messages from Sender
	for i := 0; i < 100; i++ {
		send("PUB foo 2\r\nok\r\n")
	}
	send("PING\r\n")
	expect(pongRe)

	numAReceived := len(msgRe.FindAllSubmatch(expectA(msgRe), -1))
	numBReceived := len(msgRe.FindAllSubmatch(expectB(msgRe), -1))

	// We may not be able to properly time all messages being ready.
	for numAReceived+numBReceived != 100 {
		if buf := peek(clientB); buf != nil {
			numBReceived += len(msgRe.FindAllSubmatch(buf, -1))
		}
		if buf := peek(clientA); buf != nil {
			numAReceived += len(msgRe.FindAllSubmatch(buf, -1))
		}
	}
	// These should be close to 50/50
	if numAReceived < 30 || numBReceived < 30 {
		t.Fatalf("Expected numbers to be close to 50/50, got %d/%d", numAReceived, numBReceived)
	}
}

// Since we trade interest in accounts now, we have a potential issue with a new client
// connecting via a brand new account, publishing and properly doing a flush, then exiting.
// If existing subscribers were present but on a remote server they may not get the message.
func TestNewRouteSinglePublishOnNewAccount(t *testing.T) {
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	// Allow new accounts to be created on the fly.
	optsA.AllowNewAccounts = true
	optsB.AllowNewAccounts = true

	// Create and establish a listener on foo for $TEST22 account.
	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	sendA, expectA := setupConnWithAccount(t, clientA, "$TEST22")
	sendA("SUB foo 1\r\nPING\r\n")
	expectA(pongRe)

	clientB := createClientConn(t, optsB.Host, optsB.Port)
	defer clientB.Close()

	// Send a message, flush to make sure server processed and close connection.
	sendB, expectB := setupConnWithAccount(t, clientB, "$TEST22")
	sendB("PUB foo 2\r\nok\r\nPING\r\n")
	expectB(pongRe)
	clientB.Close()

	expectMsgs := expectMsgsCommand(t, expectA)
	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "1", "", "2", "ok")
}

// Same as above but make sure it works for queue subscribers as well.
func TestNewRouteSinglePublishToQueueSubscriberOnNewAccount(t *testing.T) {
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	// Allow new accounts to be created on the fly.
	optsA.AllowNewAccounts = true
	optsB.AllowNewAccounts = true

	// Create and establish a listener on foo for $TEST22 account.
	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	sendA, expectA := setupConnWithAccount(t, clientA, "$TEST22")
	sendA("SUB foo bar 1\r\nPING\r\n")
	expectA(pongRe)

	clientB := createClientConn(t, optsB.Host, optsB.Port)
	defer clientB.Close()

	// Send a message, flush to make sure server processed and close connection.
	sendB, expectB := setupConnWithAccount(t, clientB, "$TEST22")
	sendB("PUB foo bar 2\r\nok\r\nPING\r\n")
	expectB(pongRe)
	defer clientB.Close()

	expectMsgs := expectMsgsCommand(t, expectA)
	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "1", "bar", "2", "ok")

	sendB("PUB foo bar 2\r\nok\r\nPING\r\n")
	expectB(pongRe)
	matches = expectMsgs(1)
	checkMsg(t, matches[0], "foo", "1", "bar", "2", "ok")
}

// Same as above but make sure it works for queue subscribers over multiple routes as well.
func TestNewRouteSinglePublishToMultipleQueueSubscriberOnNewAccount(t *testing.T) {
	srvA, srvB, srvC, optsA, optsB, optsC := runThreeServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()
	defer srvC.Shutdown()

	// Allow new accounts to be created on the fly.
	optsA.AllowNewAccounts = true
	optsB.AllowNewAccounts = true
	optsC.AllowNewAccounts = true

	// Create and establish a listener on foo/bar for $TEST22 account. Do this on ClientA and ClientC.
	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	sendA, expectA := setupConnWithAccount(t, clientA, "$TEST22")
	sendA("SUB foo bar 11\r\nPING\r\n")
	expectA(pongRe)

	clientC := createClientConn(t, optsC.Host, optsC.Port)
	defer clientC.Close()

	sendC, expectC := setupConnWithAccount(t, clientC, "$TEST22")
	sendC("SUB foo bar 33\r\nPING\r\n")
	expectC(pongRe)

	sendA("PING\r\n")
	expectA(pongRe)
	sendC("PING\r\n")
	expectC(pongRe)

	clientB := createClientConn(t, optsB.Host, optsB.Port)
	defer clientB.Close()

	time.Sleep(100 * time.Millisecond)

	// Send a message, flush to make sure server processed and close connection.
	sendB, expectB := setupConnWithAccount(t, clientB, "$TEST22")
	sendB("PUB foo 2\r\nok\r\nPING\r\n")
	expectB(pongRe)
	defer clientB.Close()

	// This should trigger either clientA or clientC, but not both..
	bufA := peek(clientA)
	bufC := peek(clientC)

	if bufA != nil && bufC != nil {
		t.Fatalf("Expected one or the other, but got something on both")
	}
	numReceived := len(msgRe.FindAllSubmatch(bufA, -1))
	numReceived += len(msgRe.FindAllSubmatch(bufC, -1))
	if numReceived != 1 {
		t.Fatalf("Expected only 1 msg, got %d", numReceived)
	}

	// Now make sure that we are distributing correctly between A and C
	// Send 100 messages from Sender
	for i := 0; i < 100; i++ {
		sendB("PUB foo 2\r\nok\r\n")
	}
	sendB("PING\r\n")
	expectB(pongRe)

	numAReceived := len(msgRe.FindAllSubmatch(expectA(msgRe), -1))
	numCReceived := len(msgRe.FindAllSubmatch(expectC(msgRe), -1))

	// We may not be able to properly time all messages being ready.

	for numAReceived+numCReceived != 100 {
		if buf := peek(clientC); buf != nil {
			numCReceived += len(msgRe.FindAllSubmatch(buf, -1))
		}
		if buf := peek(clientA); buf != nil {
			numAReceived += len(msgRe.FindAllSubmatch(buf, -1))
		}
	}

	// These should be close to 50/50
	if numAReceived < 30 || numCReceived < 30 {
		t.Fatalf("Expected numbers to be close to 50/50, got %d/%d", numAReceived, numCReceived)
	}
}

func registerAccounts(t *testing.T, s *server.Server) (*server.Account, *server.Account) {
	// Now create two accounts.
	f, err := s.RegisterAccount("$foo")
	if err != nil {
		t.Fatalf("Error creating account '$foo': %v", err)
	}
	b, err := s.RegisterAccount("$bar")
	if err != nil {
		t.Fatalf("Error creating account '$bar': %v", err)
	}
	return f, b
}

func addStreamExport(subject string, authorized []*server.Account, targets ...*server.Account) {
	for _, acc := range targets {
		acc.AddStreamExport(subject, authorized)
	}
}

func addServiceExport(subject string, authorized []*server.Account, targets ...*server.Account) {
	for _, acc := range targets {
		acc.AddServiceExport(subject, authorized)
	}
}

var isPublic = []*server.Account(nil)

func TestNewRouteStreamImport(t *testing.T) {
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	// Do Accounts for the servers.
	fooA, _ := registerAccounts(t, srvA)
	fooB, barB := registerAccounts(t, srvB)

	// Add export to both.
	addStreamExport("foo", isPublic, fooA, fooB)
	// Add import abilities to server B's bar account from foo.
	barB.AddStreamImport(fooB, "foo", "")

	// clientA will be connected to srvA and be the stream producer.
	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	sendA, expectA := setupConnWithAccount(t, clientA, "$foo")

	// Now setup client B on srvB who will do a sub from account $bar
	// that should map account $foo's foo subject.
	clientB := createClientConn(t, optsB.Host, optsB.Port)
	defer clientB.Close()

	sendB, expectB := setupConnWithAccount(t, clientB, "$bar")
	sendB("SUB foo 1\r\nPING\r\n")
	expectB(pongRe)

	// Send on clientA
	sendA("PING\r\n")
	expectA(pongRe)

	sendA("PUB foo 2\r\nok\r\nPING\r\n")
	expectA(pongRe)

	expectMsgs := expectMsgsCommand(t, expectB)
	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "1", "", "2", "ok")

	// Send Again on clientA
	sendA("PUB foo 2\r\nok\r\nPING\r\n")
	expectA(pongRe)

	matches = expectMsgs(1)
	checkMsg(t, matches[0], "foo", "1", "", "2", "ok")

	sendB("UNSUB 1\r\nPING\r\n")
	expectB(pongRe)

	sendA("PUB foo 2\r\nok\r\nPING\r\n")
	expectA(pongRe)
	expectNothing(t, clientA)
}

func TestNewRouteStreamImportLargeFanout(t *testing.T) {
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	// Do Accounts for the servers.
	// This account will export a stream.
	fooA, err := srvA.RegisterAccount("$foo")
	if err != nil {
		t.Fatalf("Error creating account '$foo': %v", err)
	}
	fooB, err := srvB.RegisterAccount("$foo")
	if err != nil {
		t.Fatalf("Error creating account '$foo': %v", err)
	}

	// Add export to both.
	addStreamExport("foo", isPublic, fooA, fooB)

	// Now we will create 100 accounts who will all import from foo.
	fanout := 100
	barA := make([]*server.Account, fanout)
	for i := 0; i < fanout; i++ {
		acc := fmt.Sprintf("$bar-%d", i)
		barA[i], err = srvB.RegisterAccount(acc)
		if err != nil {
			t.Fatalf("Error creating account %q: %v", acc, err)
		}
		// Add import abilities to server B's bar account from foo.
		barA[i].AddStreamImport(fooB, "foo", "")
	}

	// clientA will be connected to srvA and be the stream producer.
	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	// Now setup fanout clients on srvB who will do a sub from account $bar
	// that should map account $foo's foo subject.
	clientB := make([]net.Conn, fanout)
	sendB := make([]sendFun, fanout)
	expectB := make([]expectFun, fanout)

	for i := 0; i < fanout; i++ {
		clientB[i] = createClientConn(t, optsB.Host, optsB.Port)
		defer clientB[i].Close()
		sendB[i], expectB[i] = setupConnWithAccount(t, clientB[i], barA[i].Name)
		sendB[i]("SUB foo 1\r\nPING\r\n")
		expectB[i](pongRe)
	}

	// Since we do not shadow all the bar acounts on srvA they will be dropped
	// when they hit the other side, which means we could only have one sub for
	// all the imports on srvA, and srvB will have 2*fanout, one normal and one
	// that represents the import.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if ns := srvA.NumSubscriptions(); ns != uint32(1) {
			return fmt.Errorf("Number of subscriptions is %d", ns)
		}
		if ns := srvB.NumSubscriptions(); ns != uint32(2*fanout) {
			return fmt.Errorf("Number of subscriptions is %d", ns)
		}
		return nil
	})
}

func TestNewRouteReservedReply(t *testing.T) {
	s, opts := runNewRouteServer(t)
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)

	// Test that clients can't send to reserved service import replies.
	send("PUB foo _R_.foo 2\r\nok\r\nPING\r\n")
	expect(errRe)
}

func TestNewRouteServiceImport(t *testing.T) {
	// To quickly enable trace and debug logging
	//doLog, doTrace, doDebug = true, true, true
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	// Make so we can tell the two apart since in same PID.
	if doLog {
		srvA.SetLogger(logger.NewTestLogger("[SRV-A] - ", false), true, true)
		srvB.SetLogger(logger.NewTestLogger("[SRV-B] - ", false), true, true)
	}

	// Do Accounts for the servers.
	fooA, barA := registerAccounts(t, srvA)
	fooB, barB := registerAccounts(t, srvB)

	// Add export to both.
	addServiceExport("test.request", isPublic, fooA, fooB)

	// Add import abilities to server B's bar account from foo.
	// Meaning that when a user sends a request on foo.request from account bar,
	// the request will be mapped to be received by the responder on account foo.
	if err := barB.AddServiceImport(fooB, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}
	// Do same on A.
	if err := barA.AddServiceImport(fooA, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}

	// clientA will be connected to srvA and be the service endpoint and responder.
	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	sendA, expectA := setupConnWithAccount(t, clientA, "$foo")
	sendA("SUB test.request 1\r\nPING\r\n")
	expectA(pongRe)

	// Now setup client B on srvB who will do a sub from account $bar
	// that should map account $foo's foo subject.
	clientB := createClientConn(t, optsB.Host, optsB.Port)
	defer clientB.Close()

	sendB, expectB := setupConnWithAccount(t, clientB, "$bar")
	sendB("SUB reply 1\r\nPING\r\n")
	expectB(pongRe)

	// Send the request from clientB on foo.request,
	sendB("PUB foo.request reply 2\r\nhi\r\nPING\r\n")
	expectB(pongRe)

	expectMsgsA := expectMsgsCommand(t, expectA)
	expectMsgsB := expectMsgsCommand(t, expectB)

	// Expect the request on A
	matches := expectMsgsA(1)
	reply := string(matches[0][replyIndex])
	checkMsg(t, matches[0], "test.request", "1", reply, "2", "hi")
	if reply == "reply" {
		t.Fatalf("Expected randomized reply, but got original")
	}

	sendA(fmt.Sprintf("PUB %s 2\r\nok\r\nPING\r\n", reply))
	expectA(pongRe)

	matches = expectMsgsB(1)
	checkMsg(t, matches[0], "reply", "1", "", "2", "ok")

	// This will be the responder and the wildcard for all service replies.
	if ts := fooA.TotalSubs(); ts != 2 {
		t.Fatalf("Expected two subs to be left on fooA, but got %d", ts)
	}

	routez, _ := srvA.Routez(&server.RoutezOptions{Subscriptions: true})
	r := routez.Routes[0]
	if r == nil {
		t.Fatalf("Expected 1 route, got none")
	}
	if r.NumSubs != 2 {
		t.Fatalf("Expected 2 subs in the route connection, got %v", r.NumSubs)
	}
}

func TestNewRouteServiceExportWithWildcards(t *testing.T) {
	for _, test := range []struct {
		name   string
		public bool
	}{
		{
			name:   "public",
			public: true,
		},
		{
			name:   "private",
			public: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			srvA, srvB, optsA, optsB := runServers(t)
			defer srvA.Shutdown()
			defer srvB.Shutdown()

			// Do Accounts for the servers.
			fooA, barA := registerAccounts(t, srvA)
			fooB, barB := registerAccounts(t, srvB)

			var accs []*server.Account
			// Add export to both.
			if !test.public {
				accs = []*server.Account{barA}
			}
			addServiceExport("ngs.update.*", accs, fooA)
			if !test.public {
				accs = []*server.Account{barB}
			}
			addServiceExport("ngs.update.*", accs, fooB)

			// Add import abilities to server B's bar account from foo.
			if err := barB.AddServiceImport(fooB, "ngs.update", "ngs.update.$bar"); err != nil {
				t.Fatalf("Error adding service import: %v", err)
			}
			// Do same on A.
			if err := barA.AddServiceImport(fooA, "ngs.update", "ngs.update.$bar"); err != nil {
				t.Fatalf("Error adding service import: %v", err)
			}

			// clientA will be connected to srvA and be the service endpoint and responder.
			clientA := createClientConn(t, optsA.Host, optsA.Port)
			defer clientA.Close()

			sendA, expectA := setupConnWithAccount(t, clientA, "$foo")
			sendA("SUB ngs.update.* 1\r\nPING\r\n")
			expectA(pongRe)

			// Now setup client B on srvB who will do a sub from account $bar
			// that should map account $foo's foo subject.
			clientB := createClientConn(t, optsB.Host, optsB.Port)
			defer clientB.Close()

			sendB, expectB := setupConnWithAccount(t, clientB, "$bar")
			sendB("SUB reply 1\r\nPING\r\n")
			expectB(pongRe)

			// Send the request from clientB on foo.request,
			sendB("PUB ngs.update reply 2\r\nhi\r\nPING\r\n")
			expectB(pongRe)

			expectMsgsA := expectMsgsCommand(t, expectA)
			expectMsgsB := expectMsgsCommand(t, expectB)

			// Expect the request on A
			matches := expectMsgsA(1)
			reply := string(matches[0][replyIndex])
			checkMsg(t, matches[0], "ngs.update.$bar", "1", reply, "2", "hi")
			if reply == "reply" {
				t.Fatalf("Expected randomized reply, but got original")
			}

			sendA(fmt.Sprintf("PUB %s 2\r\nok\r\nPING\r\n", reply))
			expectA(pongRe)

			matches = expectMsgsB(1)
			checkMsg(t, matches[0], "reply", "1", "", "2", "ok")

			if ts := fooA.TotalSubs(); ts != 2 {
				t.Fatalf("Expected two subs to be left on fooA, but got %d", ts)
			}

			routez, _ := srvA.Routez(&server.RoutezOptions{Subscriptions: true})
			r := routez.Routes[0]
			if r == nil {
				t.Fatalf("Expected 1 route, got none")
			}
			if r.NumSubs != 2 {
				t.Fatalf("Expected 2 subs in the route connection, got %v", r.NumSubs)
			}
		})
	}
}

func TestNewRouteServiceImportQueueGroups(t *testing.T) {
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	// Do Accounts for the servers.
	fooA, barA := registerAccounts(t, srvA)
	fooB, barB := registerAccounts(t, srvB)

	// Add export to both.
	addServiceExport("test.request", isPublic, fooA, fooB)

	// Add import abilities to server B's bar account from foo.
	if err := barB.AddServiceImport(fooB, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}
	// Do same on A.
	if err := barA.AddServiceImport(fooA, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}

	// clientA will be connected to srvA and be the service endpoint and responder.
	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	sendA, expectA := setupConnWithAccount(t, clientA, "$foo")
	sendA("SUB test.request QGROUP 1\r\nPING\r\n")
	expectA(pongRe)

	// Now setup client B on srvB who will do a sub from account $bar
	// that should map account $foo's foo subject.
	clientB := createClientConn(t, optsB.Host, optsB.Port)
	defer clientB.Close()

	sendB, expectB := setupConnWithAccount(t, clientB, "$bar")
	sendB("SUB reply QGROUP_TOO 1\r\nPING\r\n")
	expectB(pongRe)

	// Send the request from clientB on foo.request,
	sendB("PUB foo.request reply 2\r\nhi\r\nPING\r\n")
	expectB(pongRe)

	expectMsgsA := expectMsgsCommand(t, expectA)
	expectMsgsB := expectMsgsCommand(t, expectB)

	// Expect the request on A
	matches := expectMsgsA(1)
	reply := string(matches[0][replyIndex])
	checkMsg(t, matches[0], "test.request", "1", reply, "2", "hi")
	if reply == "reply" {
		t.Fatalf("Expected randomized reply, but got original")
	}

	sendA(fmt.Sprintf("PUB %s 2\r\nok\r\nPING\r\n", reply))
	expectA(pongRe)

	matches = expectMsgsB(1)
	checkMsg(t, matches[0], "reply", "1", "", "2", "ok")

	if ts := fooA.TotalSubs(); ts != 2 {
		t.Fatalf("Expected two subs to be left on fooA, but got %d", ts)
	}

	routez, _ := srvA.Routez(&server.RoutezOptions{Subscriptions: true})
	r := routez.Routes[0]
	if r == nil {
		t.Fatalf("Expected 1 route, got none")
	}
	if r.NumSubs != 2 {
		t.Fatalf("Expected 2 subs in the route connection, got %v", r.NumSubs)
	}
}

func TestNewRouteServiceImportDanglingRemoteSubs(t *testing.T) {
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	// Do Accounts for the servers.
	fooA, _ := registerAccounts(t, srvA)
	fooB, barB := registerAccounts(t, srvB)

	fooA.SetAutoExpireTTL(10 * time.Millisecond)

	// Add in the service export for the requests. Make it public.
	if err := fooA.AddServiceExport("test.request", nil); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}

	// Add export to both.
	addServiceExport("test.request", isPublic, fooA, fooB)

	// Add import abilities to server B's bar account from foo.
	if err := barB.AddServiceImport(fooB, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}

	// clientA will be connected to srvA and be the service endpoint, but will not send responses.
	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	sendA, expectA := setupConnWithAccount(t, clientA, "$foo")
	// Express interest.
	sendA("SUB test.request 1\r\nPING\r\n")
	expectA(pongRe)

	// Now setup client B on srvB who will do a sub from account $bar
	// that should map account $foo's foo subject.
	clientB := createClientConn(t, optsB.Host, optsB.Port)
	defer clientB.Close()

	sendB, expectB := setupConnWithAccount(t, clientB, "$bar")
	sendB("SUB reply 1\r\nPING\r\n")
	expectB(pongRe)

	// Send 100 requests from clientB on foo.request,
	for i := 0; i < 100; i++ {
		sendB("PUB foo.request reply 2\r\nhi\r\n")
	}
	sendB("PING\r\n")
	expectB(pongRe)

	numRequests := 0
	// Expect the request on A
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		buf := expectA(msgRe)
		matches := msgRe.FindAllSubmatch(buf, -1)
		numRequests += len(matches)
		if numRequests != 100 {
			return fmt.Errorf("Number of requests is %d", numRequests)
		}
		return nil
	})

	expectNothing(t, clientB)

	// These reply subjects will be dangling off of $foo account on serverA.
	// Remove our service endpoint and wait for the dangling replies to go to zero.
	sendA("UNSUB 1\r\nPING\r\n")
	expectA(pongRe)

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if ts := fooA.TotalSubs(); ts != 1 {
			return fmt.Errorf("Number of subs is %d, should be only 1", ts)
		}
		return nil
	})
}

func TestNewRouteNoQueueSubscribersBounce(t *testing.T) {
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	urlA := fmt.Sprintf("nats://%s:%d/", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, optsB.Port)

	ncA, err := nats.Connect(urlA)
	if err != nil {
		t.Fatalf("Failed to create connection for ncA: %v\n", err)
	}
	defer ncA.Close()

	ncB, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Failed to create connection for ncB: %v\n", err)
	}
	defer ncB.Close()

	response := []byte("I will help you")

	// Create a lot of queue subscribers on A, and have one on B.
	ncB.QueueSubscribe("foo.request", "workers", func(m *nats.Msg) {
		ncB.Publish(m.Reply, response)
	})

	for i := 0; i < 100; i++ {
		ncA.QueueSubscribe("foo.request", "workers", func(m *nats.Msg) {
			ncA.Publish(m.Reply, response)
		})
	}
	ncB.Flush()
	ncA.Flush()

	// Send all requests from B
	numAnswers := 0
	for i := 0; i < 500; i++ {
		if _, err := ncB.Request("foo.request", []byte("Help Me"), time.Second); err != nil {
			t.Fatalf("Received an error on Request test [%d]: %s", i, err)
		}
		numAnswers++
		// After we have sent 20 close the ncA client.
		if i == 20 {
			ncA.Close()
		}
	}

	if numAnswers != 500 {
		t.Fatalf("Expect to get all 500 responses, got %d", numAnswers)
	}
}

func TestNewRouteLargeDistinctQueueSubscribers(t *testing.T) {
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	urlA := fmt.Sprintf("nats://%s:%d/", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, optsB.Port)

	ncA, err := nats.Connect(urlA)
	if err != nil {
		t.Fatalf("Failed to create connection for ncA: %v\n", err)
	}
	defer ncA.Close()

	ncB, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Failed to create connection for ncB: %v\n", err)
	}
	defer ncB.Close()

	const nqsubs = 100

	qsubs := make([]*nats.Subscription, 100)

	// Create 100 queue subscribers on B all with different queue groups.
	for i := 0; i < nqsubs; i++ {
		qg := fmt.Sprintf("worker-%d", i)
		qsubs[i], _ = ncB.QueueSubscribeSync("foo", qg)
	}
	ncB.Flush()
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if ns := srvA.NumSubscriptions(); ns != 100 {
			return fmt.Errorf("Number of subscriptions is %d", ns)
		}
		return nil
	})

	// Send 10 messages. We should receive 1000 responses.
	for i := 0; i < 10; i++ {
		ncA.Publish("foo", nil)
	}
	ncA.Flush()

	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		for i := 0; i < nqsubs; i++ {
			if n, _, _ := qsubs[i].Pending(); n != 10 {
				return fmt.Errorf("Number of messages is %d", n)
			}
		}
		return nil
	})
}

func TestClusterLeaksSubscriptions(t *testing.T) {
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	urlA := fmt.Sprintf("nats://%s:%d/", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, optsB.Port)

	numResponses := 100
	repliers := make([]*nats.Conn, 0, numResponses)

	// Create 100 repliers
	for i := 0; i < 50; i++ {
		nc1, _ := nats.Connect(urlA)
		nc2, _ := nats.Connect(urlB)
		repliers = append(repliers, nc1, nc2)
		nc1.Subscribe("test.reply", func(m *nats.Msg) {
			m.Respond([]byte("{\"sender\": 22 }"))
		})
		nc2.Subscribe("test.reply", func(m *nats.Msg) {
			m.Respond([]byte("{\"sender\": 33 }"))
		})
		nc1.Flush()
		nc2.Flush()
	}

	servers := fmt.Sprintf("%s, %s", urlA, urlB)
	req := sizedBytes(8 * 1024)

	// Now run a requestor in a loop, creating and tearing down each time to
	// simulate running a modified nats-req.
	doReq := func() {
		msgs := make(chan *nats.Msg, 1)
		inbox := nats.NewInbox()
		grp := nuid.Next()
		// Create 8 queue Subscribers for responses.
		for i := 0; i < 8; i++ {
			nc, _ := nats.Connect(servers)
			nc.ChanQueueSubscribe(inbox, grp, msgs)
			nc.Flush()
			defer nc.Close()
		}
		nc, _ := nats.Connect(servers)
		nc.PublishRequest("test.reply", inbox, req)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		var received int
		for {
			select {
			case <-msgs:
				received++
				if received >= numResponses {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}

	var wg sync.WaitGroup

	doRequests := func(n int) {
		for i := 0; i < n; i++ {
			doReq()
		}
		wg.Done()
	}

	concurrent := 10
	wg.Add(concurrent)
	for i := 0; i < concurrent; i++ {
		go doRequests(10)
	}
	wg.Wait()

	// Close responders too, should have zero(0) subs attached to routes.
	for _, nc := range repliers {
		nc.Close()
	}

	// Make sure no clients remain. This is to make sure the test is correct and that
	// we have closed all the client connections.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		v1, _ := srvA.Varz(nil)
		v2, _ := srvB.Varz(nil)
		if v1.Connections != 0 || v2.Connections != 0 {
			return fmt.Errorf("We have lingering client connections %d:%d", v1.Connections, v2.Connections)
		}
		return nil
	})

	loadRoutez := func() (*server.Routez, *server.Routez) {
		v1, err := srvA.Routez(&server.RoutezOptions{Subscriptions: true})
		if err != nil {
			t.Fatalf("Error getting Routez: %v", err)
		}
		v2, err := srvB.Routez(&server.RoutezOptions{Subscriptions: true})
		if err != nil {
			t.Fatalf("Error getting Routez: %v", err)
		}
		return v1, v2
	}

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		r1, r2 := loadRoutez()
		if r1.Routes[0].NumSubs != 0 {
			return fmt.Errorf("Leaked %d subs: %+v", r1.Routes[0].NumSubs, r1.Routes[0].Subs)
		}
		if r2.Routes[0].NumSubs != 0 {
			return fmt.Errorf("Leaked %d subs: %+v", r2.Routes[0].NumSubs, r2.Routes[0].Subs)
		}
		return nil
	})
}
