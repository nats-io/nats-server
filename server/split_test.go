package server

import (
	"bytes"
	"testing"

	"github.com/apcera/gnatsd/hashmap"
	"github.com/apcera/gnatsd/sublist"
)

func TestSplitBufferSubOp(t *testing.T) {
	s := &Server{sl: sublist.New()}
	c := &client{srv: s, subs: hashmap.New()}

	subop := []byte("SUB foo 1\r\n")
	subop1 := subop[:6]
	subop2 := subop[6:]

	if err := c.parse(subop1); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if c.state != SUB_ARG {
		t.Fatalf("Expected SUB_ARG state vs %d\n", c.state)
	}
	if err := c.parse(subop2); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if c.state != OP_START {
		t.Fatalf("Expected OP_START state vs %d\n", c.state)
	}
	r := s.sl.Match([]byte("foo"))
	if r == nil || len(r) != 1 {
		t.Fatalf("Did not match subscription properly: %+v\n", r)
	}
	sub := r[0].(*subscription)
	if !bytes.Equal(sub.subject, []byte("foo")) {
		t.Fatalf("Subject did not match expected 'foo' : '%s'\n", sub.subject)
	}
	if !bytes.Equal(sub.sid, []byte("1")) {
		t.Fatalf("Sid did not match expected '1' : '%s'\n", sub.sid)
	}
	if sub.queue != nil {
		t.Fatalf("Received a non-nil queue: '%s'\n", sub.queue)
	}
}

func TestSplitBufferUnsubOp(t *testing.T) {
	s := &Server{sl: sublist.New()}
	c := &client{srv: s, subs: hashmap.New()}

	subop := []byte("SUB foo 1024\r\n")
	if err := c.parse(subop); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if c.state != OP_START {
		t.Fatalf("Expected OP_START state vs %d\n", c.state)
	}

	unsubop := []byte("UNSUB 1024\r\n")
	unsubop1 := unsubop[:8]
	unsubop2 := unsubop[8:]

	if err := c.parse(unsubop1); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if c.state != UNSUB_ARG {
		t.Fatalf("Expected UNSUB_ARG state vs %d\n", c.state)
	}
	if err := c.parse(unsubop2); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if c.state != OP_START {
		t.Fatalf("Expected OP_START state vs %d\n", c.state)
	}
	r := s.sl.Match([]byte("foo"))
	if r != nil && len(r) != 0 {
		t.Fatalf("Should be no subscriptions in results: %+v\n", r)
	}
}

func TestSplitBufferPubOp(t *testing.T) {
	c := &client{subs: hashmap.New()}
	pub := []byte("PUB foo.bar INBOX.22 11\r\nhello world\r")
	pub1 := pub[:2]
	pub2 := pub[2:9]
	pub3 := pub[9:15]
	pub4 := pub[15:22]
	pub5 := pub[22:25]
	pub6 := pub[25:33]
	pub7 := pub[33:]

	if err := c.parse(pub1); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if c.state != OP_PU {
		t.Fatalf("Expected OP_PU state vs %d\n", c.state)
	}
	if err := c.parse(pub2); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if c.state != PUB_ARG {
		t.Fatalf("Expected OP_PU state vs %d\n", c.state)
	}
	if err := c.parse(pub3); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if c.state != PUB_ARG {
		t.Fatalf("Expected OP_PU state vs %d\n", c.state)
	}
	if err := c.parse(pub4); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if c.state != PUB_ARG {
		t.Fatalf("Expected PUB_ARG state vs %d\n", c.state)
	}
	if err := c.parse(pub5); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if c.state != MSG_PAYLOAD {
		t.Fatalf("Expected MSG_PAYLOAD state vs %d\n", c.state)
	}

	// Check c.pa
	if !bytes.Equal(c.pa.subject, []byte("foo.bar")) {
		t.Fatalf("PUB arg subject incorrect: '%s'\n", c.pa.subject)
	}
	if !bytes.Equal(c.pa.reply, []byte("INBOX.22")) {
		t.Fatalf("PUB arg reply subject incorrect: '%s'\n", c.pa.reply)
	}
	if c.pa.size != 11 {
		t.Fatalf("PUB arg msg size incorrect: %d\n", c.pa.size)
	}
	if err := c.parse(pub6); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if c.state != MSG_PAYLOAD {
		t.Fatalf("Expected MSG_PAYLOAD state vs %d\n", c.state)
	}
	if err := c.parse(pub7); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if c.state != MSG_END {
		t.Fatalf("Expected MSG_END state vs %d\n", c.state)
	}
}

func TestSplitBufferPubOp2(t *testing.T) {
	c := &client{subs: hashmap.New()}
	pub := []byte("PUB foo.bar INBOX.22 11\r\nhello world\r\n")
	pub1 := pub[:30]
	pub2 := pub[30:]

	if err := c.parse(pub1); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if c.state != MSG_PAYLOAD {
		t.Fatalf("Expected MSG_PAYLOAD state vs %d\n", c.state)
	}
	if err := c.parse(pub2); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if c.state != OP_START {
		t.Fatalf("Expected OP_START state vs %d\n", c.state)
	}
}

func TestSplitBufferPubOp3(t *testing.T) {
	c := &client{subs: hashmap.New()}
	pubAll := []byte("PUB foo bar 11\r\nhello world\r\n")
	pub := pubAll[:16]

	if err := c.parse(pub); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if !bytes.Equal(c.pa.subject, []byte("foo")) {
		t.Fatalf("Unexpected subject: '%s' vs '%s'\n", c.pa.subject, "foo")
	}

	// Simulate next read of network, make sure pub state is saved
	// until msg payload has cleared.
	copy(pubAll, "XXXXXXXXXXXXXXXX")
	if !bytes.Equal(c.pa.subject, []byte("foo")) {
		t.Fatalf("Unexpected subject: '%s' vs '%s'\n", c.pa.subject, "foo")
	}
	if !bytes.Equal(c.pa.reply, []byte("bar")) {
		t.Fatalf("Unexpected reply: '%s' vs '%s'\n", c.pa.reply, "bar")
	}
	if !bytes.Equal(c.pa.szb, []byte("11")) {
		t.Fatalf("Unexpected size bytes: '%s' vs '%s'\n", c.pa.szb, "11")
	}
}

func TestSplitBufferPubOp4(t *testing.T) {
	c := &client{subs: hashmap.New()}
	pubAll := []byte("PUB foo 11\r\nhello world\r\n")
	pub := pubAll[:12]

	if err := c.parse(pub); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if !bytes.Equal(c.pa.subject, []byte("foo")) {
		t.Fatalf("Unexpected subject: '%s' vs '%s'\n", c.pa.subject, "foo")
	}

	// Simulate next read of network, make sure pub state is saved
	// until msg payload has cleared.
	copy(pubAll, "XXXXXXXXXXXX")
	if !bytes.Equal(c.pa.subject, []byte("foo")) {
		t.Fatalf("Unexpected subject: '%s' vs '%s'\n", c.pa.subject, "foo")
	}
	if !bytes.Equal(c.pa.reply, []byte("")) {
		t.Fatalf("Unexpected reply: '%s' vs '%s'\n", c.pa.reply, "")
	}
	if !bytes.Equal(c.pa.szb, []byte("11")) {
		t.Fatalf("Unexpected size bytes: '%s' vs '%s'\n", c.pa.szb, "11")
	}
}

func TestSplitBufferPubOp5(t *testing.T) {
	c := &client{subs: hashmap.New()}
	pubAll := []byte("PUB foo 11\r\nhello world\r\n")

	// Splits need to be on MSG_END now too, so make sure we check that.
	// Split between \r and \n
	pub := pubAll[:len(pubAll)-1]

	if err := c.parse(pub); err != nil {
		t.Fatalf("Unexpected parse error: %v\n", err)
	}
	if c.msgBuf == nil {
		t.Fatalf("msgBuf should not be nil!\n")
	}
	if !bytes.Equal(c.msgBuf, []byte("hello world\r")) {
		t.Fatalf("c.msgBuf did not snaphot the msg")
	}
}
