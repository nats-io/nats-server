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
)

func TestVerbosePing(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()

	doConnect(t, c, true, false, false)

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

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()

	doConnect(t, c, true, false, false)

	send := sendCommand(t, c)
	expect := expectCommand(t, c)

	expect(okRe)

	// Connect
	send("CONNECT {\"verbose\":true,\"pedantic\":true,\"tls_required\":false}\r\n")
	expect(okRe)
}

func TestVerbosePubSub(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()

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
