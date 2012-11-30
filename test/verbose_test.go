// Copyright 2012 Apcera Inc. All rights reserved.

package test

import (
	"testing"
)

func TestStartupVerbose(t *testing.T) {
	s = startServer(t, PROTO_TEST_PORT, "")
}

func TestVerbosePing(t *testing.T) {
	c := createClientConn(t, "localhost", PROTO_TEST_PORT)
	doConnect(t, c, true, false, false)
	send := sendCommand(t, c)
	expect := expectCommand(t, c)

	// Ping should still be same
	send("PING\r\n")
	expect(pongRe)
}

func TestVerboseConnect(t *testing.T) {
	c := createClientConn(t, "localhost", PROTO_TEST_PORT)
	doConnect(t, c, true, false, false)
	send := sendCommand(t, c)
	expect := expectCommand(t, c)

	// Connect
	send("CONNECT {\"verbose\":true,\"pedantic\":true,\"ssl_required\":false}\r\n")
	expect(okRe)
}

func TestVerbosePubSub(t *testing.T) {
	c := createClientConn(t, "localhost", PROTO_TEST_PORT)
	doConnect(t, c, true, false, false)
	send := sendCommand(t, c)
	expect := expectCommand(t, c)

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

func TestStopServerVerbose(t *testing.T) {
	s.stopServer()
}
