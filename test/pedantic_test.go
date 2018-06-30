// Copyright 2012-2018 The NATS Authors
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
	"testing"

	"github.com/nats-io/gnatsd/server"
)

func runPedanticServer() *server.Server {
	opts := DefaultTestOptions

	opts.NoLog = false
	opts.Trace = true

	opts.Port = PROTO_TEST_PORT
	return RunServer(&opts)
}

func TestPedanticSub(t *testing.T) {
	s := runPedanticServer()
	defer s.Shutdown()

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()

	send := sendCommand(t, c)
	expect := expectCommand(t, c)
	doConnect(t, c, false, true, false)

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

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()

	send := sendCommand(t, c)
	expect := expectCommand(t, c)
	doConnect(t, c, false, true, false)

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
