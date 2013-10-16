// Copyright 2012-2013 Apcera Inc. All rights reserved.

package test

import (
	"testing"
)

func TestVerbosePing(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()
	c := createClientConn(t, "localhost", PROTO_TEST_PORT)
	doConnect(t, c, true, false, false)
	defer c.Close()

	send := sendCommand(t, c)
	expect := expectCommand(t, c)

	expect(okRe)

	// Ping should still be same
	send("PING\r\n")
	expect(pongRe)
}

func TestVerboseConnect(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()
	c := createClientConn(t, "localhost", PROTO_TEST_PORT)
	doConnect(t, c, true, false, false)
	defer c.Close()

	send := sendCommand(t, c)
	expect := expectCommand(t, c)

	expect(okRe)

	// Connect
	send("CONNECT {\"verbose\":true,\"pedantic\":true,\"ssl_required\":false}\r\n")
	expect(okRe)
}

func TestVerbosePubSub(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()
	c := createClientConn(t, "localhost", PROTO_TEST_PORT)
	doConnect(t, c, true, false, false)
	send := sendCommand(t, c)
	expect := expectCommand(t, c)

	expect(okRe)

	// Pub
	send("PUB foo 2\r\nok\r\n")
	expect(okRe)

	// Sub
	send("SUB foo 1\r\n")
	expect(okRe)

	// UnSub
	send("UNSUB 1\r\n")
	expect(okRe)
}
