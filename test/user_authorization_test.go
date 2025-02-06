// Copyright 2016-2020 The NATS Authors
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
	"regexp"
	"testing"
	"time"
)

const DefaultPass = "foo"

var permErrRe = regexp.MustCompile(`\A\-ERR\s+'Permissions Violation([^\r\n]+)\r\n`)

func TestUserAuthorizationProto(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/authorization.conf")
	defer srv.Shutdown()

	// Alice can do anything, check a few for OK result.
	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", "alice", DefaultPass)
	expectResult(t, c, okRe)
	sendProto(t, c, "PUB foo 2\r\nok\r\n")
	expectResult(t, c, okRe)
	sendProto(t, c, "SUB foo 1\r\n")
	expectResult(t, c, okRe)

	// Check that _ is ok
	sendProto(t, c, "PUB _ 2\r\nok\r\n")
	expectResult(t, c, okRe)

	c.Close()

	// Bob is a requestor only, e.g. req.foo, req.bar for publish, subscribe only to INBOXes.
	c = createClientConn(t, opts.Host, opts.Port)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", "bob", DefaultPass)
	expectResult(t, c, okRe)

	// These should error.
	sendProto(t, c, "SUB foo 1\r\n")
	expectResult(t, c, permErrRe)
	sendProto(t, c, "PUB foo 2\r\nok\r\n")
	expectResult(t, c, permErrRe)

	// These should work ok.
	sendProto(t, c, "SUB _INBOX.abcd 1\r\n")
	expectResult(t, c, okRe)
	sendProto(t, c, "PUB req.foo 2\r\nok\r\n")
	expectResult(t, c, okRe)
	sendProto(t, c, "PUB req.bar 2\r\nok\r\n")
	expectResult(t, c, okRe)
	c.Close()

	// Joe is a default user
	c = createClientConn(t, opts.Host, opts.Port)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", "joe", DefaultPass)
	expectResult(t, c, okRe)

	// These should error.
	sendProto(t, c, "SUB foo.bar.* 1\r\n")
	expectResult(t, c, permErrRe)
	sendProto(t, c, "PUB foo.bar.baz 2\r\nok\r\n")
	expectResult(t, c, permErrRe)

	// These should work ok.
	sendProto(t, c, "SUB _INBOX.abcd 1\r\n")
	expectResult(t, c, okRe)
	sendProto(t, c, "SUB PUBLIC.abcd 1\r\n")
	expectResult(t, c, okRe)

	sendProto(t, c, "PUB SANDBOX.foo 2\r\nok\r\n")
	expectResult(t, c, okRe)
	sendProto(t, c, "PUB SANDBOX.bar 2\r\nok\r\n")
	expectResult(t, c, okRe)

	// Since only PWC, this should fail (too many tokens).
	sendProto(t, c, "PUB SANDBOX.foo.bar 2\r\nok\r\n")
	expectResult(t, c, permErrRe)

	c.Close()

	// This is the new style permissions with allow and deny clauses.
	c = createClientConn(t, opts.Host, opts.Port)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", "ns", DefaultPass)
	expectResult(t, c, okRe)

	// These should work
	sendProto(t, c, "PUB SANDBOX.foo 2\r\nok\r\n")
	expectResult(t, c, okRe)
	sendProto(t, c, "PUB baz.bar 2\r\nok\r\n")
	expectResult(t, c, okRe)
	sendProto(t, c, "PUB baz.foo 2\r\nok\r\n")
	expectResult(t, c, okRe)

	// These should error.
	sendProto(t, c, "PUB foo 2\r\nok\r\n")
	expectResult(t, c, permErrRe)
	sendProto(t, c, "PUB bar 2\r\nok\r\n")
	expectResult(t, c, permErrRe)
	sendProto(t, c, "PUB foo.bar 2\r\nok\r\n")
	expectResult(t, c, permErrRe)
	sendProto(t, c, "PUB foo.bar.baz 2\r\nok\r\n")
	expectResult(t, c, permErrRe)
	sendProto(t, c, "PUB SYS.1 2\r\nok\r\n")
	expectResult(t, c, permErrRe)

	// Subscriptions

	// These should work ok.
	sendProto(t, c, "SUB foo.bar 1\r\n")
	expectResult(t, c, okRe)
	sendProto(t, c, "SUB foo.foo 1\r\n")
	expectResult(t, c, okRe)

	// These should error.
	sendProto(t, c, "SUB foo 1\r\n")
	expectResult(t, c, permErrRe)
	sendProto(t, c, "SUB foo.baz 1\r\n")
	expectResult(t, c, permErrRe)
	sendProto(t, c, "SUB foo.baz 1\r\n")
	expectResult(t, c, permErrRe)
	sendProto(t, c, "SUB foo.baz 1\r\n")
	expectResult(t, c, permErrRe)

	// Deny clauses for subscriptions need to be able to allow subscriptions
	// on larger scoped wildcards, but prevent delivery of a message whose
	// subject matches a deny clause.

	// Clear old stuff
	c.Close()

	c = createClientConn(t, opts.Host, opts.Port)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", "ns", DefaultPass)
	expectResult(t, c, okRe)

	sendProto(t, c, "SUB foo.* 1\r\n")
	expectResult(t, c, okRe)

	sendProto(t, c, "SUB foo.* bar 2\r\n")
	expectResult(t, c, okRe)

	// Now send on foo.baz which should not be received on first client.
	// Joe is a default user
	nc := createClientConn(t, opts.Host, opts.Port)
	defer nc.Close()
	expectAuthRequired(t, nc)
	doAuthConnect(t, nc, "", "ns-pub", DefaultPass)
	expectResult(t, nc, okRe)

	sendProto(t, nc, "PUB foo.baz 2\r\nok\r\n")
	expectResult(t, nc, okRe)

	// Expect nothing from the wildcard subscription.
	expectNothing(t, c)

	sendProto(t, c, "PING\r\n")
	expectResult(t, c, pongRe)

	// Now create a queue sub on our ns-pub user. We want to test that
	// queue subscribers can be denied and delivery will route around.
	sendProto(t, nc, "SUB foo.baz bar 2\r\n")
	expectResult(t, nc, okRe)

	// Make sure we always get the message on our queue subscriber.
	// Do this several times since we should select the other subscriber
	// but get permission denied..
	for i := 0; i < 20; i++ {
		sendProto(t, nc, "PUB foo.baz 2\r\nok\r\n")
		buf := expectResult(t, nc, okRe)
		if msgRe.Match(buf) {
			continue
		} else {
			expectResult(t, nc, msgRe)
		}
	}

	// Clear old stuff
	c.Close()

	c = createClientConn(t, opts.Host, opts.Port)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", "ns", DefaultPass)
	expectResult(t, c, okRe)

	sendProto(t, c, "SUB foo.bar 1\r\n")
	expectResult(t, c, okRe)

	sendProto(t, c, "SUB foo.bar.baz 2\r\n")
	expectResult(t, c, errRe)

	sendProto(t, c, "SUB > 3\r\n")
	expectResult(t, c, errRe)

	sendProto(t, c, "SUB SYS.> 4\r\n")
	expectResult(t, c, errRe)

	sendProto(t, c, "SUB SYS.TEST.foo 5\r\n")
	expectResult(t, c, okRe)

	sendProto(t, c, "SUB SYS.bar 5\r\n")
	expectResult(t, c, errRe)
}

func TestUserAuthorizationAllowResponses(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/authorization.conf")
	defer srv.Shutdown()

	// Alice can do anything, so she will be our requestor
	rc := createClientConn(t, opts.Host, opts.Port)
	defer rc.Close()
	expectAuthRequired(t, rc)
	doAuthConnect(t, rc, "", "alice", DefaultPass)
	expectResult(t, rc, okRe)

	// MY_SERVICE can subscribe to a single request subject but can
	// respond to any reply subject that it receives, but only
	// for one response.
	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", "svca", DefaultPass)
	expectResult(t, c, okRe)

	sendProto(t, c, "SUB my.service.req 1\r\n")
	expectResult(t, c, okRe)

	sendProto(t, rc, "PUB my.service.req resp.bar.22 2\r\nok\r\n")
	expectResult(t, rc, okRe)

	matches := msgRe.FindAllSubmatch(expectResult(t, c, msgRe), -1)
	checkMsg(t, matches[0], "my.service.req", "1", "resp.bar.22", "2", "ok")

	// This should be allowed
	sendProto(t, c, "PUB resp.bar.22 2\r\nok\r\n")
	expectResult(t, c, okRe)

	// This should not be allowed
	sendProto(t, c, "PUB resp.bar.33 2\r\nok\r\n")
	expectResult(t, c, errRe)

	// This should also not be allowed now since we already sent a response and max is 1.
	sendProto(t, c, "PUB resp.bar.22 2\r\nok\r\n")
	expectResult(t, c, errRe)

	c.Close() // from MY_SERVICE

	// MY_STREAM_SERVICE can subscribe to a single request subject but can
	// respond to any reply subject that it receives, and send up to 10 responses.
	// Each permission for a response can last up to 10ms.
	c = createClientConn(t, opts.Host, opts.Port)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", "svcb", DefaultPass)
	expectResult(t, c, okRe)

	sendProto(t, c, "SUB my.service.req 1\r\n")
	expectResult(t, c, okRe)

	// Same rules as above.
	sendProto(t, rc, "PUB my.service.req resp.bar.22 2\r\nok\r\n")
	expectResult(t, rc, okRe)

	matches = msgRe.FindAllSubmatch(expectResult(t, c, msgRe), -1)
	checkMsg(t, matches[0], "my.service.req", "1", "resp.bar.22", "2", "ok")

	// This should be allowed
	sendProto(t, c, "PUB resp.bar.22 2\r\nok\r\n")
	expectResult(t, c, okRe)

	// This should not be allowed
	sendProto(t, c, "PUB resp.bar.33 2\r\nok\r\n")
	expectResult(t, c, errRe)

	// We should be able to send 9 more here since we are allowed 10 total
	for i := 0; i < 9; i++ {
		sendProto(t, c, "PUB resp.bar.22 2\r\nok\r\n")
		expectResult(t, c, okRe)
	}
	// Now this should fail since we already sent 10 responses.
	sendProto(t, c, "PUB resp.bar.22 2\r\nok\r\n")
	expectResult(t, c, errRe)

	// Now test timeout.
	sendProto(t, rc, "PUB my.service.req resp.bar.11 2\r\nok\r\n")
	expectResult(t, rc, okRe)

	matches = msgRe.FindAllSubmatch(expectResult(t, c, msgRe), -1)
	checkMsg(t, matches[0], "my.service.req", "1", "resp.bar.11", "2", "ok")

	time.Sleep(100 * time.Millisecond)

	sendProto(t, c, "PUB resp.bar.11 2\r\nok\r\n")
	expectResult(t, c, errRe)
}
