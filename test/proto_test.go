// Copyright 2012-2013 Apcera Inc. All rights reserved.

package test

import (
	"testing"
	"time"

	"github.com/apcera/gnatsd/server"
)

const PROTO_TEST_PORT = 9922

func runProtoServer() *server.Server {
	opts := DefaultTestOptions
	opts.Port = PROTO_TEST_PORT
	return RunServer(&opts)
}

func TestProtoBasics(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()
	c := createClientConn(t, "localhost", PROTO_TEST_PORT)
	send, expect := setupConn(t, c)
	expectMsgs := expectMsgsCommand(t, expect)
	defer c.Close()

	// Ping
	send("PING\r\n")
	expect(pongRe)

	// Single Msg
	send("SUB foo 1\r\nPUB foo 5\r\nhello\r\n")
	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "1", "", "5", "hello")

	// 2 Messages
	send("SUB * 2\r\nPUB foo 2\r\nok\r\n")
	matches = expectMsgs(2)
	checkMsg(t, matches[0], "foo", "1", "", "2", "ok")
	checkMsg(t, matches[1], "foo", "2", "", "2", "ok")
}

func TestProtoErr(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()
	c := createClientConn(t, "localhost", PROTO_TEST_PORT)
	send, expect := setupConn(t, c)
	defer c.Close()

	// Make sure we get an error on bad proto
	send("ZZZ")
	expect(errRe)
}

func TestUnsubMax(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()
	c := createClientConn(t, "localhost", PROTO_TEST_PORT)
	send, expect := setupConn(t, c)
	expectMsgs := expectMsgsCommand(t, expect)
	defer c.Close()

	send("SUB foo 22\r\n")
	send("UNSUB 22 2\r\n")
	for i := 0; i < 100; i++ {
		send("PUB foo 2\r\nok\r\n")
	}
	matches := expectMsgs(2)
	checkMsg(t, matches[0], "foo", "22", "", "2", "ok")
	checkMsg(t, matches[1], "foo", "22", "", "2", "ok")
}

func TestQueueSub(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()
	c := createClientConn(t, "localhost", PROTO_TEST_PORT)
	send, expect := setupConn(t, c)
	expectMsgs := expectMsgsCommand(t, expect)
	defer c.Close()

	sent := 100
	send("SUB foo qgroup1 22\r\n")
	send("SUB foo qgroup1 32\r\n")
	for i := 0; i < sent; i++ {
		send("PUB foo 2\r\nok\r\n")
	}
	// Wait for responses
	time.Sleep(250 * time.Millisecond)

	matches := expectMsgs(sent)
	sids := make(map[string]int)
	for _, m := range matches {
		sids[string(m[SID_INDEX])]++
	}
	if len(sids) != 2 {
		t.Fatalf("Expected only 2 sids, got %d\n", len(sids))
	}
	for k, c := range sids {
		if c < 35 {
			t.Fatalf("Expected ~50 (+-15) msgs for sid:'%s', got %d\n", k, c)
		}
	}
}

func TestMultipleQueueSub(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()
	c := createClientConn(t, "localhost", PROTO_TEST_PORT)
	send, expect := setupConn(t, c)
	expectMsgs := expectMsgsCommand(t, expect)
	defer c.Close()

	sent := 100
	send("SUB foo g1 1\r\n")
	send("SUB foo g1 2\r\n")
	send("SUB foo g2 3\r\n")
	send("SUB foo g2 4\r\n")

	for i := 0; i < sent; i++ {
		send("PUB foo 2\r\nok\r\n")
	}
	// Wait for responses
	time.Sleep(250 * time.Millisecond)

	matches := expectMsgs(sent * 2)
	sids := make(map[string]int)
	for _, m := range matches {
		sids[string(m[SID_INDEX])]++
	}
	if len(sids) != 4 {
		t.Fatalf("Expected 4 sids, got %d\n", len(sids))
	}
	for k, c := range sids {
		if c < 35 {
			t.Fatalf("Expected ~50 (+-15) msgs for '%s', got %d\n", k, c)
		}
	}
}

func TestPubToArgState(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()
	c := createClientConn(t, "localhost", PROTO_TEST_PORT)
	send, expect := setupConn(t, c)
	defer c.Close()
	send("PUBS foo 2\r\nok\r\n")
	expect(errRe)
}

func TestSubToArgState(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()
	c := createClientConn(t, "localhost", PROTO_TEST_PORT)
	send, expect := setupConn(t, c)
	defer c.Close()
	send("SUBZZZ foo 1\r\n")
	expect(errRe)
}
