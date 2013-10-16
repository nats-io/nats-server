// Copyright 2012-2013 Apcera Inc. All rights reserved.

package test

import (
	"testing"

	"github.com/apcera/gnatsd/server"
)

func runPedanticServer() *server.Server {
	opts := DefaultTestOptions
	opts.Port = PROTO_TEST_PORT
	return RunServer(&opts)
}

func TestPedanticSub(t *testing.T) {
	s := runPedanticServer()
	defer s.Shutdown()
	c := createClientConn(t, "localhost", PROTO_TEST_PORT)
	defer c.Close()
	send := sendCommand(t, c)
	expect := expectCommand(t, c)
	doConnect(t, c, true, true, false)
	expect(okRe)

	// Ping should still be same
	send("PING\r\n")
	expect(pongRe)

	// Test malformed subjects for SUB
	// Sub can contain wildcards, but
	// subject must still be legit.

	// Empty terminal token
	send("SUB foo. 1\r\n")
	expect(errRe)

	// Empty beginning token
	send("SUB .foo. 1\r\n")
	expect(errRe)

	// Empty middle token
	send("SUB foo..bar 1\r\n")
	expect(errRe)

	// Bad non-terminal FWC
	send("SUB foo.>.bar 1\r\n")
	buf := expect(errRe)

	// Check that itr is 'Invalid Subject'
	matches := errRe.FindAllSubmatch(buf, -1)
	if len(matches) != 1 {
		t.Fatal("Wanted one overall match")
	}
	if string(matches[0][1]) != "'Invalid Subject'" {
		t.Fatalf("Expected 'Invalid Subject', got %s", string(matches[0][1]))
	}

}

func TestPedanticPub(t *testing.T) {
	s := runPedanticServer()
	defer s.Shutdown()
	c := createClientConn(t, "localhost", PROTO_TEST_PORT)
	defer c.Close()
	send := sendCommand(t, c)
	expect := expectCommand(t, c)
	doConnect(t, c, true, true, false)
	expect(okRe)

	// Ping should still be same
	send("PING\r\n")
	expect(pongRe)

	// Test malformed subjects for PUB
	// PUB subjects can not have wildcards
	// This will error in pedantic mode
	send("PUB foo.* 2\r\nok\r\n")
	expect(errRe)

	send("PUB foo.> 2\r\nok\r\n")
	expect(errRe)

	send("PUB foo. 2\r\nok\r\n")
	expect(errRe)

	send("PUB .foo 2\r\nok\r\n")
	expect(errRe)

	send("PUB foo..* 2\r\nok\r\n")
	expect(errRe)
}
