// Copyright 2016-2018 The NATS Authors
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

	// Check that we now reserve _SYS.> though for internal, so no clients.
	sendProto(t, c, "PUB _SYS.HB 2\r\nok\r\n")
	expectResult(t, c, permErrRe)

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
}
