// Copyright 2012-2014 Apcera Inc. All rights reserved.

package test

import (
	"net"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/server"
)

const (
	PING_TEST_PORT = 9972
	PING_INTERVAL  = 50 * time.Millisecond
	PING_MAX       = 2
)

func runPingServer() *server.Server {
	opts := DefaultTestOptions
	opts.Port = PING_TEST_PORT
	opts.PingInterval = PING_INTERVAL
	opts.MaxPingsOut = PING_MAX
	return RunServer(&opts)
}

func TestPingInterval(t *testing.T) {
	s := runPingServer()
	defer s.Shutdown()

	c := createClientConn(t, "localhost", PING_TEST_PORT)
	defer c.Close()

	doConnect(t, c, false, false, false)

	expect := expectCommand(t, c)

	// Expect the max to be delivered correctly..
	for i := 0; i < PING_MAX; i++ {
		time.Sleep(PING_INTERVAL / 2)
		expect(pingRe)
	}

	// We should get an error from the server
	time.Sleep(PING_INTERVAL)
	expect(errRe)

	// Server should close the connection at this point..
	time.Sleep(PING_INTERVAL)
	c.SetWriteDeadline(time.Now().Add(PING_INTERVAL))

	var err error
	for {
		_, err = c.Write([]byte("PING\r\n"))
		if err != nil {
			break
		}
	}
	c.SetWriteDeadline(time.Time{})

	if err == nil {
		t.Fatal("No error: Expected to have connection closed")
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("timeout: Expected to have connection closed")
	}
}

func TestUnpromptedPong(t *testing.T) {
	s := runPingServer()
	defer s.Shutdown()

	c := createClientConn(t, "localhost", PING_TEST_PORT)
	defer c.Close()

	doConnect(t, c, false, false, false)

	expect := expectCommand(t, c)

	// Send lots of PONGs in a row...
	for i := 0; i < 100; i++ {
		c.Write([]byte("PONG\r\n"))
	}

	// The server should still send the max number of PINGs and then
	// close the connection.
	for i := 0; i < PING_MAX; i++ {
		time.Sleep(PING_INTERVAL / 2)
		expect(pingRe)
	}

	// We should get an error from the server
	time.Sleep(PING_INTERVAL)
	expect(errRe)

	// Server should close the connection at this point..
	c.SetWriteDeadline(time.Now().Add(PING_INTERVAL))
	var err error
	for {
		_, err = c.Write([]byte("PING\r\n"))
		if err != nil {
			break
		}
	}
	c.SetWriteDeadline(time.Time{})

	if err == nil {
		t.Fatal("No error: Expected to have connection closed")
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("timeout: Expected to have connection closed")
	}
}
