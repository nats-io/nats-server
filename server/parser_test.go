package server

import (
	"bytes"
	"testing"
)

func dummyClient() *client {
	return &client{}
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
	err = c.parse(pong)
	if err != nil || c.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
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

	// Should be adjusting c.pout, Pings Outstanding
	c.state = OP_START
	c.pout = 10
	pong = []byte("PONG\r\n")
	err = c.parse(pong)
	if err != nil || c.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
	}
	if c.pout != 9 {
		t.Fatalf("Unexpected pout: %d vs %d\n", c.pout, 9)
	}
}

func TestParseConnect(t *testing.T) {
	c := dummyClient()
	connect := []byte("CONNECT {\"verbose\":false,\"pedantic\":true,\"ssl_required\":false}\r\n")
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
	if err != nil || c.state != MSG_END {
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
	if err != nil || c.state != MSG_END {
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
}

func testPubArg(c *client, t *testing.T) {
	if !bytes.Equal(c.pa.subject, []byte("foo")) {
		t.Fatalf("Mismatched subject: '%s'\n", c.pa.subject)
	}
	if !bytes.Equal(c.pa.szb, []byte("22")) {
		t.Fatalf("Bad size buf: '%s'\n", c.pa.szb)
	}
	if c.pa.size != 22 {
		t.Fatalf("Bad size: %d\n", c.pa.size)
	}
}

func TestParsePubArg(t *testing.T) {
	c := dummyClient()
	if err := c.processPub([]byte("foo 22")); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	testPubArg(c, t)
	if err := c.processPub([]byte(" foo 22")); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	testPubArg(c, t)
	if err := c.processPub([]byte(" foo 22 ")); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	testPubArg(c, t)
	if err := c.processPub([]byte("foo   22")); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if err := c.processPub([]byte("foo   22\r")); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	testPubArg(c, t)
}

func TestParseMsg(t *testing.T) {
	c := dummyClient()

	pub := []byte("MSG foo RSID:1:2 5\r\nhello\r")
	err := c.parse(pub)
	if err != nil || c.state != MSG_END {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
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
	if !bytes.Equal(c.pa.sid, []byte("RSID:1:2")) {
		t.Fatalf("Did not parse sid correctly: 'RSID:1:2' vs '%s'\n", c.pa.sid)
	}

	// Clear snapshots
	c.argBuf, c.msgBuf, c.state = nil, nil, OP_START

	pub = []byte("MSG foo.bar RSID:1:2 INBOX.22 11\r\nhello world\r")
	err = c.parse(pub)
	if err != nil || c.state != MSG_END {
		t.Fatalf("Unexpected: %d : %v\n", c.state, err)
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
}

func testMsgArg(c *client, t *testing.T) {
	if !bytes.Equal(c.pa.subject, []byte("foobar")) {
		t.Fatalf("Mismatched subject: '%s'\n", c.pa.subject)
	}
	if !bytes.Equal(c.pa.szb, []byte("22")) {
		t.Fatalf("Bad size buf: '%s'\n", c.pa.szb)
	}
	if c.pa.size != 22 {
		t.Fatalf("Bad size: %d\n", c.pa.size)
	}
	if !bytes.Equal(c.pa.sid, []byte("RSID:22:1")) {
		t.Fatalf("Bad sid: '%s'\n", c.pa.sid)
	}
}

func TestParseMsgArg(t *testing.T) {
	c := dummyClient()
	if err := c.processMsgArgs([]byte("foobar RSID:22:1 22")); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	testMsgArg(c, t)
	if err := c.processMsgArgs([]byte(" foobar RSID:22:1 22")); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	testMsgArg(c, t)
	if err := c.processMsgArgs([]byte(" foobar   RSID:22:1 22 ")); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	testMsgArg(c, t)
	if err := c.processMsgArgs([]byte("foobar   RSID:22:1  \t22")); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if err := c.processMsgArgs([]byte("foobar\t\tRSID:22:1\t22\r")); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	testMsgArg(c, t)
}

func TestShouldFail(t *testing.T) {
	c := dummyClient()

	if err := c.parse([]byte(" PING")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.state = OP_START
	if err := c.parse([]byte("CONNECT \r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.state = OP_START
	if err := c.parse([]byte("POO")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.state = OP_START
	if err := c.parse([]byte("PUB foo\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.state = OP_START
	if err := c.parse([]byte("PUB \r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.state = OP_START
	if err := c.parse([]byte("PUB foo bar       \r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.state = OP_START
	if err := c.parse([]byte("SUB\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.state = OP_START
	if err := c.parse([]byte("SUB \r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.state = OP_START
	if err := c.parse([]byte("SUB foo\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.state = OP_START
	if err := c.parse([]byte("SUB foo bar baz 22\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.state = OP_START
	if err := c.parse([]byte("PUB foo 2\r\nok \r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.state = OP_START
	if err := c.parse([]byte("PUB foo 2\r\nok\r \n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
}
