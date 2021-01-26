// Copyright 2012-2020 The NATS Authors
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

package server

import (
	"bytes"
	"testing"
)

func dummyClient() *client {
	return &client{srv: New(&defaultServerOptions), msubs: -1, mpay: -1, mcl: MAX_CONTROL_LINE_SIZE}
}

func dummyRouteClient() *client {
	return &client{srv: New(&defaultServerOptions), kind: ROUTER}
}

func TestParsePing(t *testing.T) {
	c := dummyClient()
	if c.state != OP_START {
		t.Fatalf("Expected OP_START vs %d\n", c.state)
	}
	ping := []byte("PING\r\n")
	err := c.parse(ping[:1])
	if err != nil || c.state != OP_P {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	err = c.parse(ping[1:2])
	if err != nil || c.state != OP_PI {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	err = c.parse(ping[2:3])
	if err != nil || c.state != OP_PIN {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	err = c.parse(ping[3:4])
	if err != nil || c.state != OP_PING {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	err = c.parse(ping[4:5])
	if err != nil || c.state != OP_PING {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	err = c.parse(ping[5:6])
	if err != nil || c.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	err = c.parse(ping)
	if err != nil || c.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	// Should tolerate spaces
	ping = []byte("PING  \r")
	err = c.parse(ping)
	if err != nil || c.state != OP_PING {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	c.state = OP_START
	ping = []byte("PING  \r  \n")
	err = c.parse(ping)
	if err != nil || c.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
}

func TestParsePong(t *testing.T) {
	c := dummyClient()
	if c.state != OP_START {
		t.Fatalf("Expected OP_START vs %d\n", c.state)
	}
	pong := []byte("PONG\r\n")
	err := c.parse(pong[:1])
	if err != nil || c.state != OP_P {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	err = c.parse(pong[1:2])
	if err != nil || c.state != OP_PO {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	err = c.parse(pong[2:3])
	if err != nil || c.state != OP_PON {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	err = c.parse(pong[3:4])
	if err != nil || c.state != OP_PONG {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	err = c.parse(pong[4:5])
	if err != nil || c.state != OP_PONG {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	err = c.parse(pong[5:6])
	if err != nil || c.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if c.ping.out != 0 {
		t.Fatalf("Unexpected ping.out value: %d vs 0\n", c.ping.out)
	}
	err = c.parse(pong)
	if err != nil || c.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if c.ping.out != 0 {
		t.Fatalf("Unexpected ping.out value: %d vs 0\n", c.ping.out)
	}
	// Should tolerate spaces
	pong = []byte("PONG  \r")
	err = c.parse(pong)
	if err != nil || c.state != OP_PONG {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	c.state = OP_START
	pong = []byte("PONG  \r  \n")
	err = c.parse(pong)
	if err != nil || c.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if c.ping.out != 0 {
		t.Fatalf("Unexpected ping.out value: %d vs 0\n", c.ping.out)
	}

	// Should be adjusting c.pout (Pings Outstanding): reset to 0
	c.state = OP_START
	c.ping.out = 10
	pong = []byte("PONG\r\n")
	err = c.parse(pong)
	if err != nil || c.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if c.ping.out != 0 {
		t.Fatalf("Unexpected ping.out: %d vs 0\n", c.ping.out)
	}
}

func TestParseConnect(t *testing.T) {
	c := dummyClient()
	connect := []byte("CONNECT {\"verbose\":false,\"pedantic\":true,\"tls_required\":false}\r\n")
	err := c.parse(connect)
	if err != nil || c.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	// Check saved state
	if c.as != 8 {
		t.Fatalf("ArgStart state incorrect: 8 vs %d\n", c.as)
	}
}

func TestParseSub(t *testing.T) {
	c := dummyClient()
	sub := []byte("SUB foo 1\r")
	err := c.parse(sub)
	if err != nil || c.state != SUB_ARG {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	// Check saved state
	if c.as != 4 {
		t.Fatalf("ArgStart state incorrect: 4 vs %d\n", c.as)
	}
	if c.drop != 1 {
		t.Fatalf("Drop state incorrect: 1 vs %d\n", c.as)
	}
	if !bytes.Equal(sub[c.as:], []byte("foo 1\r")) {
		t.Fatalf("Arg state incorrect: %s\n", sub[c.as:])
	}
}

func TestParsePub(t *testing.T) {
	c := dummyClient()

	pub := []byte("PUB foo 5\r\nhello\r")
	err := c.parse(pub)
	if err != nil || c.state != MSG_END_N {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if !bytes.Equal(c.pa.subject, []byte("foo")) {
		t.Fatalf("Did not parse subject correctly: 'foo' vs '%s'\n", string(c.pa.subject))
	}
	if c.pa.reply != nil {
		t.Fatalf("Did not parse reply correctly: 'nil' vs '%s'\n", string(c.pa.reply))
	}
	if c.pa.size != 5 {
		t.Fatalf("Did not parse msg size correctly: 5 vs %d\n", c.pa.size)
	}

	// Clear snapshots
	c.argBuf, c.msgBuf, c.state = nil, nil, OP_START

	pub = []byte("PUB foo.bar INBOX.22 11\r\nhello world\r")
	err = c.parse(pub)
	if err != nil || c.state != MSG_END_N {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if !bytes.Equal(c.pa.subject, []byte("foo.bar")) {
		t.Fatalf("Did not parse subject correctly: 'foo' vs '%s'\n", string(c.pa.subject))
	}
	if !bytes.Equal(c.pa.reply, []byte("INBOX.22")) {
		t.Fatalf("Did not parse reply correctly: 'INBOX.22' vs '%s'\n", string(c.pa.reply))
	}
	if c.pa.size != 11 {
		t.Fatalf("Did not parse msg size correctly: 11 vs %d\n", c.pa.size)
	}

	// Clear snapshots
	c.argBuf, c.msgBuf, c.state = nil, nil, OP_START

	// This is the case when data has more bytes than expected by size.
	pub = []byte("PUB foo.bar 11\r\nhello world hello world\r")
	err = c.parse(pub)
	if err == nil {
		t.Fatalf("Expected an error parsing longer than expected message body")
	}
	if c.msgBuf != nil {
		t.Fatalf("Did not expect a c.msgBuf to be non-nil")
	}
}

// https://www.twistlock.com/labs-blog/finding-dos-vulnerability-nats-go-fuzz-cve-2019-13126/
func TestParsePubSizeOverflow(t *testing.T) {
	c := dummyClient()

	pub := []byte("PUB foo 3333333333333333333333333333333333333333333333333333333333333333\r\n")
	if err := c.parse(pub); err == nil {
		t.Fatalf("Expected an error")
	}
}

func TestParsePubArg(t *testing.T) {
	c := dummyClient()

	for _, test := range []struct {
		arg     string
		subject string
		reply   string
		size    int
		szb     string
	}{
		{arg: "a 2", subject: "a", reply: "", size: 2, szb: "2"},
		{arg: "a 222", subject: "a", reply: "", size: 222, szb: "222"},
		{arg: "foo 22", subject: "foo", reply: "", size: 22, szb: "22"},
		{arg: " foo 22", subject: "foo", reply: "", size: 22, szb: "22"},
		{arg: "foo 22 ", subject: "foo", reply: "", size: 22, szb: "22"},
		{arg: "foo   22", subject: "foo", reply: "", size: 22, szb: "22"},
		{arg: " foo 22 ", subject: "foo", reply: "", size: 22, szb: "22"},
		{arg: " foo   22 ", subject: "foo", reply: "", size: 22, szb: "22"},
		{arg: "foo bar 22", subject: "foo", reply: "bar", size: 22, szb: "22"},
		{arg: " foo bar 22", subject: "foo", reply: "bar", size: 22, szb: "22"},
		{arg: "foo bar 22 ", subject: "foo", reply: "bar", size: 22, szb: "22"},
		{arg: "foo  bar  22", subject: "foo", reply: "bar", size: 22, szb: "22"},
		{arg: " foo bar 22 ", subject: "foo", reply: "bar", size: 22, szb: "22"},
		{arg: "  foo   bar  22  ", subject: "foo", reply: "bar", size: 22, szb: "22"},
		{arg: "  foo   bar  2222  ", subject: "foo", reply: "bar", size: 2222, szb: "2222"},
		{arg: "  foo     2222  ", subject: "foo", reply: "", size: 2222, szb: "2222"},
		{arg: "a\t2", subject: "a", reply: "", size: 2, szb: "2"},
		{arg: "a\t222", subject: "a", reply: "", size: 222, szb: "222"},
		{arg: "foo\t22", subject: "foo", reply: "", size: 22, szb: "22"},
		{arg: "\tfoo\t22", subject: "foo", reply: "", size: 22, szb: "22"},
		{arg: "foo\t22\t", subject: "foo", reply: "", size: 22, szb: "22"},
		{arg: "foo\t\t\t22", subject: "foo", reply: "", size: 22, szb: "22"},
		{arg: "\tfoo\t22\t", subject: "foo", reply: "", size: 22, szb: "22"},
		{arg: "\tfoo\t\t\t22\t", subject: "foo", reply: "", size: 22, szb: "22"},
		{arg: "foo\tbar\t22", subject: "foo", reply: "bar", size: 22, szb: "22"},
		{arg: "\tfoo\tbar\t22", subject: "foo", reply: "bar", size: 22, szb: "22"},
		{arg: "foo\tbar\t22\t", subject: "foo", reply: "bar", size: 22, szb: "22"},
		{arg: "foo\t\tbar\t\t22", subject: "foo", reply: "bar", size: 22, szb: "22"},
		{arg: "\tfoo\tbar\t22\t", subject: "foo", reply: "bar", size: 22, szb: "22"},
		{arg: "\t \tfoo\t \t \tbar\t \t22\t \t", subject: "foo", reply: "bar", size: 22, szb: "22"},
		{arg: "\t\tfoo\t\t\tbar\t\t2222\t\t", subject: "foo", reply: "bar", size: 2222, szb: "2222"},
		{arg: "\t \tfoo\t \t \t\t\t2222\t \t", subject: "foo", reply: "", size: 2222, szb: "2222"},
	} {
		t.Run(test.arg, func(t *testing.T) {
			if err := c.processPub([]byte(test.arg)); err != nil {
				t.Fatalf("Unexpected parse error: %v\n", err)
			}
			if !bytes.Equal(c.pa.subject, []byte(test.subject)) {
				t.Fatalf("Mismatched subject: '%s'\n", c.pa.subject)
			}
			if !bytes.Equal(c.pa.reply, []byte(test.reply)) {
				t.Fatalf("Mismatched reply subject: '%s'\n", c.pa.reply)
			}
			if !bytes.Equal(c.pa.szb, []byte(test.szb)) {
				t.Fatalf("Bad size buf: '%s'\n", c.pa.szb)
			}
			if c.pa.size != test.size {
				t.Fatalf("Bad size: %d\n", c.pa.size)
			}
		})
	}
}

func TestParsePubBadSize(t *testing.T) {
	c := dummyClient()
	// Setup localized max payload
	c.mpay = 32768
	if err := c.processPub([]byte("foo 2222222222222222")); err == nil {
		t.Fatalf("Expected parse error for size too large")
	}
}

func TestParseHeaderPub(t *testing.T) {
	c := dummyClient()
	c.headers = true

	hpub := []byte("HPUB foo 12 17\r\nname:derek\r\nHELLO\r")
	if err := c.parse(hpub); err != nil || c.state != MSG_END_N {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if !bytes.Equal(c.pa.subject, []byte("foo")) {
		t.Fatalf("Did not parse subject correctly: 'foo' vs '%s'", c.pa.subject)
	}
	if c.pa.reply != nil {
		t.Fatalf("Did not parse reply correctly: 'nil' vs '%s'", c.pa.reply)
	}
	if c.pa.hdr != 12 {
		t.Fatalf("Did not parse msg header size correctly: 12 vs %d", c.pa.hdr)
	}
	if !bytes.Equal(c.pa.hdb, []byte("12")) {
		t.Fatalf("Did not parse or capture the header size as bytes correctly: %q", c.pa.hdb)
	}
	if c.pa.size != 17 {
		t.Fatalf("Did not parse msg size correctly: 17 vs %d", c.pa.size)
	}

	// Clear snapshots
	c.argBuf, c.msgBuf, c.state = nil, nil, OP_START

	hpub = []byte("HPUB foo INBOX.22 12 17\r\nname:derek\r\nHELLO\r")
	if err := c.parse(hpub); err != nil || c.state != MSG_END_N {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if !bytes.Equal(c.pa.subject, []byte("foo")) {
		t.Fatalf("Did not parse subject correctly: 'foo' vs '%s'", c.pa.subject)
	}
	if !bytes.Equal(c.pa.reply, []byte("INBOX.22")) {
		t.Fatalf("Did not parse reply correctly: 'INBOX.22' vs '%s'", c.pa.reply)
	}
	if c.pa.hdr != 12 {
		t.Fatalf("Did not parse msg header size correctly: 12 vs %d", c.pa.hdr)
	}
	if !bytes.Equal(c.pa.hdb, []byte("12")) {
		t.Fatalf("Did not parse or capture the header size as bytes correctly: %q", c.pa.hdb)
	}
	if c.pa.size != 17 {
		t.Fatalf("Did not parse msg size correctly: 17 vs %d", c.pa.size)
	}

	// Clear snapshots
	c.argBuf, c.msgBuf, c.state = nil, nil, OP_START

	hpub = []byte("HPUB foo INBOX.22 0 5\r\nHELLO\r")
	if err := c.parse(hpub); err != nil || c.state != MSG_END_N {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if !bytes.Equal(c.pa.subject, []byte("foo")) {
		t.Fatalf("Did not parse subject correctly: 'foo' vs '%s'\n", c.pa.subject)
	}
	if !bytes.Equal(c.pa.reply, []byte("INBOX.22")) {
		t.Fatalf("Did not parse reply correctly: 'INBOX.22' vs '%s'\n", c.pa.reply)
	}
	if c.pa.hdr != 0 {
		t.Fatalf("Did not parse msg header size correctly: 0 vs %d\n", c.pa.hdr)
	}
	if !bytes.Equal(c.pa.hdb, []byte("0")) {
		t.Fatalf("Did not parse or capture the header size as bytes correctly: %q", c.pa.hdb)
	}
	if c.pa.size != 5 {
		t.Fatalf("Did not parse msg size correctly: 5 vs %d\n", c.pa.size)
	}
}

func TestParseHeaderPubArg(t *testing.T) {
	c := dummyClient()
	c.headers = true

	for _, test := range []struct {
		arg     string
		subject string
		reply   string
		hdr     int
		size    int
		szb     string
	}{
		{arg: "a 2 4", subject: "a", reply: "", hdr: 2, size: 4, szb: "4"},
		{arg: "a 22 222", subject: "a", reply: "", hdr: 22, size: 222, szb: "222"},
		{arg: "foo 3 22", subject: "foo", reply: "", hdr: 3, size: 22, szb: "22"},
		{arg: " foo   1 22", subject: "foo", reply: "", hdr: 1, size: 22, szb: "22"},
		{arg: "foo 0   22 ", subject: "foo", reply: "", hdr: 0, size: 22, szb: "22"},
		{arg: "foo  0     22", subject: "foo", reply: "", hdr: 0, size: 22, szb: "22"},
		{arg: " foo 1 22 ", subject: "foo", reply: "", hdr: 1, size: 22, szb: "22"},
		{arg: " foo   3 22 ", subject: "foo", reply: "", hdr: 3, size: 22, szb: "22"},
		{arg: "foo bar 1 22", subject: "foo", reply: "bar", hdr: 1, size: 22, szb: "22"},
		{arg: " foo bar 11 22", subject: "foo", reply: "bar", hdr: 11, size: 22, szb: "22"},
		{arg: "foo bar 11 22 ", subject: "foo", reply: "bar", hdr: 11, size: 22, szb: "22"},
		{arg: "foo  bar  11  22", subject: "foo", reply: "bar", hdr: 11, size: 22, szb: "22"},
		{arg: " foo bar  11 22 ", subject: "foo", reply: "bar", hdr: 11, size: 22, szb: "22"},
		{arg: "  foo   bar  11   22  ", subject: "foo", reply: "bar", hdr: 11, size: 22, szb: "22"},
		{arg: "  foo   bar  22   2222  ", subject: "foo", reply: "bar", hdr: 22, size: 2222, szb: "2222"},
		{arg: "  foo 1    2222  ", subject: "foo", reply: "", hdr: 1, size: 2222, szb: "2222"},
		{arg: "a\t2\t22", subject: "a", reply: "", hdr: 2, size: 22, szb: "22"},
		{arg: "a\t2\t\t222", subject: "a", reply: "", hdr: 2, size: 222, szb: "222"},
		{arg: "foo\t2 22", subject: "foo", reply: "", hdr: 2, size: 22, szb: "22"},
		{arg: "\tfoo\t11\t  22", subject: "foo", reply: "", hdr: 11, size: 22, szb: "22"},
		{arg: "foo\t11\t22\t", subject: "foo", reply: "", hdr: 11, size: 22, szb: "22"},
		{arg: "foo\t\t\t11 22", subject: "foo", reply: "", hdr: 11, size: 22, szb: "22"},
		{arg: "\tfoo\t11\t \t 22\t", subject: "foo", reply: "", hdr: 11, size: 22, szb: "22"},
		{arg: "\tfoo\t\t\t11 22\t", subject: "foo", reply: "", hdr: 11, size: 22, szb: "22"},
		{arg: "foo\tbar\t2 22", subject: "foo", reply: "bar", hdr: 2, size: 22, szb: "22"},
		{arg: "\tfoo\tbar\t11\t22", subject: "foo", reply: "bar", hdr: 11, size: 22, szb: "22"},
		{arg: "foo\tbar\t11\t\t22\t ", subject: "foo", reply: "bar", hdr: 11, size: 22, szb: "22"},
		{arg: "foo\t\tbar\t\t11\t\t\t22", subject: "foo", reply: "bar", hdr: 11, size: 22, szb: "22"},
		{arg: "\tfoo\tbar\t11\t22\t", subject: "foo", reply: "bar", hdr: 11, size: 22, szb: "22"},
		{arg: "\t \tfoo\t \t \tbar\t \t11\t 22\t \t", subject: "foo", reply: "bar", hdr: 11, size: 22, szb: "22"},
		{arg: "\t\tfoo\t\t\tbar\t\t22\t\t\t2222\t\t", subject: "foo", reply: "bar", hdr: 22, size: 2222, szb: "2222"},
		{arg: "\t \tfoo\t \t \t\t\t11\t\t 2222\t \t", subject: "foo", reply: "", hdr: 11, size: 2222, szb: "2222"},
	} {
		t.Run(test.arg, func(t *testing.T) {
			if err := c.processHeaderPub([]byte(test.arg)); err != nil {
				t.Fatalf("Unexpected parse error: %v\n", err)
			}
			if !bytes.Equal(c.pa.subject, []byte(test.subject)) {
				t.Fatalf("Mismatched subject: '%s'\n", c.pa.subject)
			}
			if !bytes.Equal(c.pa.reply, []byte(test.reply)) {
				t.Fatalf("Mismatched reply subject: '%s'\n", c.pa.reply)
			}
			if !bytes.Equal(c.pa.szb, []byte(test.szb)) {
				t.Fatalf("Bad size buf: '%s'\n", c.pa.szb)
			}
			if c.pa.hdr != test.hdr {
				t.Fatalf("Bad header size: %d\n", c.pa.hdr)
			}
			if c.pa.size != test.size {
				t.Fatalf("Bad size: %d\n", c.pa.size)
			}
		})
	}
}

func TestParseRoutedHeaderMsg(t *testing.T) {
	c := dummyRouteClient()

	pub := []byte("HMSG $foo foo 10 8\r\nXXXhello\r")
	if err := c.parse(pub); err == nil {
		t.Fatalf("Expected an error")
	}

	// Clear snapshots
	c.argBuf, c.msgBuf, c.state = nil, nil, OP_START

	pub = []byte("HMSG $foo foo 3 8\r\nXXXhello\r")
	err := c.parse(pub)
	if err != nil || c.state != MSG_END_N {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if !bytes.Equal(c.pa.account, []byte("$foo")) {
		t.Fatalf("Did not parse account correctly: '$foo' vs '%s'\n", c.pa.account)
	}
	if !bytes.Equal(c.pa.subject, []byte("foo")) {
		t.Fatalf("Did not parse subject correctly: 'foo' vs '%s'\n", c.pa.subject)
	}
	if c.pa.reply != nil {
		t.Fatalf("Did not parse reply correctly: 'nil' vs '%s'\n", c.pa.reply)
	}
	if c.pa.hdr != 3 {
		t.Fatalf("Did not parse header size correctly: 3 vs %d\n", c.pa.hdr)
	}
	if c.pa.size != 8 {
		t.Fatalf("Did not parse msg size correctly: 8 vs %d\n", c.pa.size)
	}

	// Clear snapshots
	c.argBuf, c.msgBuf, c.state = nil, nil, OP_START

	pub = []byte("HMSG $G foo.bar INBOX.22 3 14\r\nOK:hello world\r")
	err = c.parse(pub)
	if err != nil || c.state != MSG_END_N {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if !bytes.Equal(c.pa.account, []byte("$G")) {
		t.Fatalf("Did not parse account correctly: '$G' vs '%s'\n", c.pa.account)
	}
	if !bytes.Equal(c.pa.subject, []byte("foo.bar")) {
		t.Fatalf("Did not parse subject correctly: 'foo' vs '%s'\n", c.pa.subject)
	}
	if !bytes.Equal(c.pa.reply, []byte("INBOX.22")) {
		t.Fatalf("Did not parse reply correctly: 'INBOX.22' vs '%s'\n", c.pa.reply)
	}
	if c.pa.hdr != 3 {
		t.Fatalf("Did not parse header size correctly: 3 vs %d\n", c.pa.hdr)
	}
	if c.pa.size != 14 {
		t.Fatalf("Did not parse msg size correctly: 14 vs %d\n", c.pa.size)
	}

	// Clear snapshots
	c.argBuf, c.msgBuf, c.state = nil, nil, OP_START

	pub = []byte("HMSG $G foo.bar + reply baz 3 14\r\nOK:hello world\r")
	err = c.parse(pub)
	if err != nil || c.state != MSG_END_N {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if !bytes.Equal(c.pa.account, []byte("$G")) {
		t.Fatalf("Did not parse account correctly: '$G' vs '%s'\n", c.pa.account)
	}
	if !bytes.Equal(c.pa.subject, []byte("foo.bar")) {
		t.Fatalf("Did not parse subject correctly: 'foo' vs '%s'\n", c.pa.subject)
	}
	if !bytes.Equal(c.pa.reply, []byte("reply")) {
		t.Fatalf("Did not parse reply correctly: 'reply' vs '%s'\n", c.pa.reply)
	}
	if len(c.pa.queues) != 1 {
		t.Fatalf("Expected 1 queue, got %d", len(c.pa.queues))
	}
	if !bytes.Equal(c.pa.queues[0], []byte("baz")) {
		t.Fatalf("Did not parse queues correctly: 'baz' vs '%q'\n", c.pa.queues[0])
	}
	if c.pa.hdr != 3 {
		t.Fatalf("Did not parse header size correctly: 3 vs %d\n", c.pa.hdr)
	}
	if c.pa.size != 14 {
		t.Fatalf("Did not parse msg size correctly: 14 vs %d\n", c.pa.size)
	}

	// Clear snapshots
	c.argBuf, c.msgBuf, c.state = nil, nil, OP_START

	pub = []byte("HMSG $G foo.bar | baz 3 14\r\nOK:hello world\r")
	err = c.parse(pub)
	if err != nil || c.state != MSG_END_N {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if !bytes.Equal(c.pa.account, []byte("$G")) {
		t.Fatalf("Did not parse account correctly: '$G' vs '%s'\n", c.pa.account)
	}
	if !bytes.Equal(c.pa.subject, []byte("foo.bar")) {
		t.Fatalf("Did not parse subject correctly: 'foo' vs '%s'\n", c.pa.subject)
	}
	if !bytes.Equal(c.pa.reply, []byte("")) {
		t.Fatalf("Did not parse reply correctly: '' vs '%s'\n", c.pa.reply)
	}
	if len(c.pa.queues) != 1 {
		t.Fatalf("Expected 1 queue, got %d", len(c.pa.queues))
	}
	if !bytes.Equal(c.pa.queues[0], []byte("baz")) {
		t.Fatalf("Did not parse queues correctly: 'baz' vs '%q'\n", c.pa.queues[0])
	}
	if c.pa.hdr != 3 {
		t.Fatalf("Did not parse header size correctly: 3 vs %d\n", c.pa.hdr)
	}
	if c.pa.size != 14 {
		t.Fatalf("Did not parse msg size correctly: 14 vs %d\n", c.pa.size)
	}
}

func TestParseRouteMsg(t *testing.T) {
	c := dummyRouteClient()

	pub := []byte("MSG $foo foo 5\r\nhello\r")
	err := c.parse(pub)
	if err == nil {
		t.Fatalf("Expected an error, got none")
	}
	pub = []byte("RMSG $foo foo 5\r\nhello\r")
	err = c.parse(pub)
	if err != nil || c.state != MSG_END_N {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if !bytes.Equal(c.pa.account, []byte("$foo")) {
		t.Fatalf("Did not parse account correctly: '$foo' vs '%s'\n", c.pa.account)
	}
	if !bytes.Equal(c.pa.subject, []byte("foo")) {
		t.Fatalf("Did not parse subject correctly: 'foo' vs '%s'\n", c.pa.subject)
	}
	if c.pa.reply != nil {
		t.Fatalf("Did not parse reply correctly: 'nil' vs '%s'\n", c.pa.reply)
	}
	if c.pa.size != 5 {
		t.Fatalf("Did not parse msg size correctly: 5 vs %d\n", c.pa.size)
	}

	// Clear snapshots
	c.argBuf, c.msgBuf, c.state = nil, nil, OP_START

	pub = []byte("RMSG $G foo.bar INBOX.22 11\r\nhello world\r")
	err = c.parse(pub)
	if err != nil || c.state != MSG_END_N {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if !bytes.Equal(c.pa.account, []byte("$G")) {
		t.Fatalf("Did not parse account correctly: '$G' vs '%s'\n", c.pa.account)
	}
	if !bytes.Equal(c.pa.subject, []byte("foo.bar")) {
		t.Fatalf("Did not parse subject correctly: 'foo' vs '%s'\n", c.pa.subject)
	}
	if !bytes.Equal(c.pa.reply, []byte("INBOX.22")) {
		t.Fatalf("Did not parse reply correctly: 'INBOX.22' vs '%s'\n", c.pa.reply)
	}
	if c.pa.size != 11 {
		t.Fatalf("Did not parse msg size correctly: 11 vs %d\n", c.pa.size)
	}

	// Clear snapshots
	c.argBuf, c.msgBuf, c.state = nil, nil, OP_START

	pub = []byte("RMSG $G foo.bar + reply baz 11\r\nhello world\r")
	err = c.parse(pub)
	if err != nil || c.state != MSG_END_N {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if !bytes.Equal(c.pa.account, []byte("$G")) {
		t.Fatalf("Did not parse account correctly: '$G' vs '%s'\n", c.pa.account)
	}
	if !bytes.Equal(c.pa.subject, []byte("foo.bar")) {
		t.Fatalf("Did not parse subject correctly: 'foo' vs '%s'\n", c.pa.subject)
	}
	if !bytes.Equal(c.pa.reply, []byte("reply")) {
		t.Fatalf("Did not parse reply correctly: 'reply' vs '%s'\n", c.pa.reply)
	}
	if len(c.pa.queues) != 1 {
		t.Fatalf("Expected 1 queue, got %d", len(c.pa.queues))
	}
	if !bytes.Equal(c.pa.queues[0], []byte("baz")) {
		t.Fatalf("Did not parse queues correctly: 'baz' vs '%q'\n", c.pa.queues[0])
	}

	// Clear snapshots
	c.argBuf, c.msgBuf, c.state = nil, nil, OP_START

	pub = []byte("RMSG $G foo.bar | baz 11\r\nhello world\r")
	err = c.parse(pub)
	if err != nil || c.state != MSG_END_N {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if !bytes.Equal(c.pa.account, []byte("$G")) {
		t.Fatalf("Did not parse account correctly: '$G' vs '%s'\n", c.pa.account)
	}
	if !bytes.Equal(c.pa.subject, []byte("foo.bar")) {
		t.Fatalf("Did not parse subject correctly: 'foo' vs '%s'\n", c.pa.subject)
	}
	if !bytes.Equal(c.pa.reply, []byte("")) {
		t.Fatalf("Did not parse reply correctly: '' vs '%s'\n", c.pa.reply)
	}
	if len(c.pa.queues) != 1 {
		t.Fatalf("Expected 1 queue, got %d", len(c.pa.queues))
	}
	if !bytes.Equal(c.pa.queues[0], []byte("baz")) {
		t.Fatalf("Did not parse queues correctly: 'baz' vs '%q'\n", c.pa.queues[0])
	}
}

func TestParseMsgSpace(t *testing.T) {
	c := dummyRouteClient()

	// Ivan bug he found
	if err := c.parse([]byte("MSG \r\n")); err == nil {
		t.Fatalf("Expected parse error for MSG <SPC>")
	}

	c = dummyClient()

	// Anything with an M from a client should parse error
	if err := c.parse([]byte("M")); err == nil {
		t.Fatalf("Expected parse error for M* from a client")
	}
}

func TestShouldFail(t *testing.T) {
	wrongProtos := []string{
		"xxx",
		"Px", "PIx", "PINx", " PING",
		"POx", "PONx",
		"+x", "+Ox",
		"-x", "-Ex", "-ERx", "-ERRx",
		"Cx", "COx", "CONx", "CONNx", "CONNEx", "CONNECx", "CONNECx", "CONNECT \r\n",
		"PUx", "PUB foo\r\n", "PUB  \r\n", "PUB foo bar       \r\n",
		"PUB foo 2\r\nok \r\n", "PUB foo 2\r\nok\r \n",
		"Sx", "SUx", "SUB\r\n", "SUB  \r\n", "SUB foo\r\n",
		"SUB foo bar baz 22\r\n",
		"Ux", "UNx", "UNSx", "UNSUx", "UNSUBx", "UNSUBUNSUB 1\r\n", "UNSUB_2\r\n",
		"UNSUB_UNSUB_UNSUB 2\r\n", "UNSUB_\t2\r\n", "UNSUB\r\n", "UNSUB \r\n",
		"UNSUB          \t       \r\n",
		"Ix", "INx", "INFx", "INFO  \r\n",
	}
	for _, proto := range wrongProtos {
		c := dummyClient()
		if err := c.parse([]byte(proto)); err == nil {
			t.Fatalf("Should have received a parse error for: %v", proto)
		}
	}

	// Special case for MSG, type needs to not be client.
	wrongProtos = []string{"Mx", "MSx", "MSGx", "MSG  \r\n"}
	for _, proto := range wrongProtos {
		c := dummyClient()
		c.kind = ROUTER
		if err := c.parse([]byte(proto)); err == nil {
			t.Fatalf("Should have received a parse error for: %v", proto)
		}
	}
}

func TestProtoSnippet(t *testing.T) {
	sample := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	tests := []struct {
		input    int
		expected string
	}{
		{0, `"abcdefghijklmnopqrstuvwxyzABCDEF"`},
		{1, `"bcdefghijklmnopqrstuvwxyzABCDEFG"`},
		{2, `"cdefghijklmnopqrstuvwxyzABCDEFGH"`},
		{3, `"defghijklmnopqrstuvwxyzABCDEFGHI"`},
		{4, `"efghijklmnopqrstuvwxyzABCDEFGHIJ"`},
		{5, `"fghijklmnopqrstuvwxyzABCDEFGHIJK"`},
		{6, `"ghijklmnopqrstuvwxyzABCDEFGHIJKL"`},
		{7, `"hijklmnopqrstuvwxyzABCDEFGHIJKLM"`},
		{8, `"ijklmnopqrstuvwxyzABCDEFGHIJKLMN"`},
		{9, `"jklmnopqrstuvwxyzABCDEFGHIJKLMNO"`},
		{10, `"klmnopqrstuvwxyzABCDEFGHIJKLMNOP"`},
		{11, `"lmnopqrstuvwxyzABCDEFGHIJKLMNOPQ"`},
		{12, `"mnopqrstuvwxyzABCDEFGHIJKLMNOPQR"`},
		{13, `"nopqrstuvwxyzABCDEFGHIJKLMNOPQRS"`},
		{14, `"opqrstuvwxyzABCDEFGHIJKLMNOPQRST"`},
		{15, `"pqrstuvwxyzABCDEFGHIJKLMNOPQRSTU"`},
		{16, `"qrstuvwxyzABCDEFGHIJKLMNOPQRSTUV"`},
		{17, `"rstuvwxyzABCDEFGHIJKLMNOPQRSTUVW"`},
		{18, `"stuvwxyzABCDEFGHIJKLMNOPQRSTUVWX"`},
		{19, `"tuvwxyzABCDEFGHIJKLMNOPQRSTUVWXY"`},
		{20, `"uvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"`},
		{21, `"vwxyzABCDEFGHIJKLMNOPQRSTUVWXY"`},
		{22, `"wxyzABCDEFGHIJKLMNOPQRSTUVWXY"`},
		{23, `"xyzABCDEFGHIJKLMNOPQRSTUVWXY"`},
		{24, `"yzABCDEFGHIJKLMNOPQRSTUVWXY"`},
		{25, `"zABCDEFGHIJKLMNOPQRSTUVWXY"`},
		{26, `"ABCDEFGHIJKLMNOPQRSTUVWXY"`},
		{27, `"BCDEFGHIJKLMNOPQRSTUVWXY"`},
		{28, `"CDEFGHIJKLMNOPQRSTUVWXY"`},
		{29, `"DEFGHIJKLMNOPQRSTUVWXY"`},
		{30, `"EFGHIJKLMNOPQRSTUVWXY"`},
		{31, `"FGHIJKLMNOPQRSTUVWXY"`},
		{32, `"GHIJKLMNOPQRSTUVWXY"`},
		{33, `"HIJKLMNOPQRSTUVWXY"`},
		{34, `"IJKLMNOPQRSTUVWXY"`},
		{35, `"JKLMNOPQRSTUVWXY"`},
		{36, `"KLMNOPQRSTUVWXY"`},
		{37, `"LMNOPQRSTUVWXY"`},
		{38, `"MNOPQRSTUVWXY"`},
		{39, `"NOPQRSTUVWXY"`},
		{40, `"OPQRSTUVWXY"`},
		{41, `"PQRSTUVWXY"`},
		{42, `"QRSTUVWXY"`},
		{43, `"RSTUVWXY"`},
		{44, `"STUVWXY"`},
		{45, `"TUVWXY"`},
		{46, `"UVWXY"`},
		{47, `"VWXY"`},
		{48, `"WXY"`},
		{49, `"XY"`},
		{50, `"Y"`},
		{51, `""`},
		{52, `""`},
		{53, `""`},
		{54, `""`},
	}

	for _, tt := range tests {
		got := protoSnippet(tt.input, PROTO_SNIPPET_SIZE, sample)
		if tt.expected != got {
			t.Errorf("Expected protocol snippet to be %s when start=%d but got %s\n", tt.expected, tt.input, got)
		}
	}
}

func TestParseOK(t *testing.T) {
	c := dummyClient()
	if c.state != OP_START {
		t.Fatalf("Expected OP_START vs %d\n", c.state)
	}
	okProto := []byte("+OK\r\n")
	err := c.parse(okProto[:1])
	if err != nil || c.state != OP_PLUS {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	err = c.parse(okProto[1:2])
	if err != nil || c.state != OP_PLUS_O {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	err = c.parse(okProto[2:3])
	if err != nil || c.state != OP_PLUS_OK {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	err = c.parse(okProto[3:4])
	if err != nil || c.state != OP_PLUS_OK {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	err = c.parse(okProto[4:5])
	if err != nil || c.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
}

func TestMaxControlLine(t *testing.T) {
	for _, test := range []struct {
		name       string
		kind       int
		shouldFail bool
	}{
		{"client", CLIENT, true},
		{"leaf", LEAF, false},
		{"route", ROUTER, false},
		{"gateway", GATEWAY, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			pub := []byte("PUB foo.bar.baz 2\r\nok\r\n")

			setupClient := func() *client {
				c := dummyClient()
				c.setNoReconnect()
				c.flags.set(connectReceived)
				c.kind = test.kind
				if test.kind == GATEWAY {
					c.gw = &gateway{outbound: false, connected: true, insim: make(map[string]*insie)}
				}
				c.mcl = 8
				return c
			}

			c := setupClient()
			// First try with a partial:
			// PUB foo.bar.baz 2\r\nok\r\n
			// .............^
			err := c.parse(pub[:14])
			switch test.shouldFail {
			case true:
				if !ErrorIs(err, ErrMaxControlLine) {
					t.Fatalf("Expected an error parsing longer than expected control line")
				}
			case false:
				if err != nil {
					t.Fatalf("Should not have failed, got %v", err)
				}
			}

			// Now with full protocol (no split) and we should still enforce.
			c = setupClient()
			err = c.parse(pub)
			switch test.shouldFail {
			case true:
				if !ErrorIs(err, ErrMaxControlLine) {
					t.Fatalf("Expected an error parsing longer than expected control line")
				}
			case false:
				if err != nil {
					t.Fatalf("Should not have failed, got %v", err)
				}
			}
		})
	}
}
