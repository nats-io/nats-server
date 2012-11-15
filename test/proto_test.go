// Copyright 2012 Apcera Inc. All rights reserved.

package test

import (
	"encoding/json"
	"net"
	"regexp"
	"testing"
	"time"

	"github.com/apcera/gnatsd/server"
)

var s *natsServer

func TestStartup(t *testing.T) {
	s = startServer(t, server.DEFAULT_PORT, "")
}

type sendFun func(string)
type expectFun func(*regexp.Regexp) []byte

// Closure version for easier reading
func sendCommand(t *testing.T, c net.Conn) sendFun {
	return func(op string) {
		sendProto(t, c, op)
	}
}

// Closure version for easier reading
func expectCommand(t *testing.T, c net.Conn) expectFun {
	return func(re *regexp.Regexp)([]byte) {
		return expectResult(t, c, re)
	}
}

// Send the protocol command to the server.
func sendProto(t *testing.T, c net.Conn, op string) {
	n, err := c.Write([]byte(op))
	if err != nil {
		t.Fatalf("Error writing command to conn: %v\n", err)
	}
	if n != len(op) {
		t.Fatalf("Partial write: %d vs %d\n", n, len(op))
	}
}

// Reuse expect buffer
var expBuf = make([]byte, 32768)

// Test result from server against regexp
func expectResult(t *testing.T, c net.Conn, re *regexp.Regexp) []byte {
	// Wait for commands to be processed and results queued for read
	time.Sleep(10 * time.Millisecond)
	c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	defer c.SetReadDeadline(time.Time{})

	n, err := c.Read(expBuf)
	if err != nil {
		t.Fatalf("Error reading from conn: %v\n", err)
	}
	buf := expBuf[:n]
	if !re.Match(buf) {
		t.Fatalf("Response did not match expected: '%s' vs '%s'\n", buf, re)
	}
	return buf
}

// This will check that we got what we expected.
func checkMsg(t *testing.T, m [][]byte, subject, sid, reply, len, msg string) {
	if string(m[SUB_INDEX]) != subject {
		t.Fatalf("Did not get correct subject: expected '%s' got '%s'\n", subject, m[SUB_INDEX])
	}
	if string(m[SID_INDEX]) != sid {
		t.Fatalf("Did not get correct sid: exepected '%s' got '%s'\n", sid, m[SID_INDEX])
	}
	if string(m[REPLY_INDEX]) != reply {
		t.Fatalf("Did not get correct reply: exepected '%s' got '%s'\n", reply, m[REPLY_INDEX])
	}
	if string(m[LEN_INDEX]) != len {
		t.Fatalf("Did not get correct msg length: expected '%s' got '%s'\n", len, m[LEN_INDEX])
	}
	if string(m[MSG_INDEX]) != msg {
		t.Fatalf("Did not get correct msg: expected '%s' got '%s'\n", msg, m[MSG_INDEX])
	}
}

// Closure for expectMsgs
func expectMsgsCommand(t *testing.T, ef expectFun) func(int) [][][]byte {
	return func(expected int) [][][]byte {
		buf := ef(msgRe)
		matches := msgRe.FindAllSubmatch(buf, -1)
		if len(matches) != expected {
			t.Fatalf("Did not get correct # msgs: %d vs %d\n", len(matches), expected)
		}
		return matches
	}
}

var infoRe = regexp.MustCompile(`\AINFO\s+([^\r\n]+)\r\n`)
var pongRe = regexp.MustCompile(`\APONG\r\n`)
var msgRe  = regexp.MustCompile(`(?:(?:MSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\r\n([^\\r\\n]*?)\r\n)+?)`)

const (
	SUB_INDEX   = 1
	SID_INDEX   = 2
	REPLY_INDEX = 4
	LEN_INDEX   = 5
	MSG_INDEX   = 6
)

func doDefaultConnect(t *testing.T, c net.Conn) {
	// Basic Connect
	sendProto(t, c, "CONNECT {\"verbose\":false,\"pedantic\":false,\"ssl_required\":false}\r\n")
	buf := expectResult(t, c, infoRe)
	js := infoRe.FindAllSubmatch(buf, 1)[0][1]
	var sinfo server.Info
	err := json.Unmarshal(js, &sinfo)
	if err != nil {
		t.Fatalf("Could not unmarshal INFO json: %v\n", err)
	}
}

func setupConn(t *testing.T, c net.Conn) (sendFun, expectFun) {
	doDefaultConnect(t, c)
	send := sendCommand(t, c)
	expect := expectCommand(t, c)
	return send, expect
}

func TestProtoBasics(t *testing.T) {
	c := createClientConn(t, "localhost", server.DEFAULT_PORT)
	send, expect := setupConn(t, c)
	expectMsgs := expectMsgsCommand(t, expect)
	defer c.Close()

	// Ping
	send("PING\r\n")
	expect(pongRe)

	// Single Msg
	send("SUB foo 1\r\nPUB foo 5\r\nhello\r\n")
	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "1", "", "5", "hello")

	// 2 Messages
	send("SUB * 2\r\nPUB foo 2\r\nok\r\n")
	matches = expectMsgs(2)
	checkMsg(t, matches[0], "foo", "1", "", "2", "ok")
	checkMsg(t, matches[1], "foo", "2", "", "2", "ok")
}

func TestUnsubMax(t *testing.T) {
	c := createClientConn(t, "localhost", server.DEFAULT_PORT)
	send, expect := setupConn(t, c)
	expectMsgs := expectMsgsCommand(t, expect)
	defer c.Close()

	send("SUB foo 22\r\n")
	send("UNSUB 22 2\r\n")
	for i := 0; i < 100; i++ {
		send("PUB foo 2\r\nok\r\n")
	}
	matches := expectMsgs(2)
	checkMsg(t, matches[0], "foo", "22", "", "2", "ok")
	checkMsg(t, matches[1], "foo", "22", "", "2", "ok")
}

func TestQueueSub(t *testing.T) {
	c := createClientConn(t, "localhost", server.DEFAULT_PORT)
	send, expect := setupConn(t, c)
	expectMsgs := expectMsgsCommand(t, expect)
	defer c.Close()

	sent := 100
	send("SUB foo qgroup1 22\r\n")
	send("SUB foo qgroup1 32\r\n")
	for i := 0; i < sent; i++ {
		send("PUB foo 2\r\nok\r\n")
	}
	matches := expectMsgs(sent)
	sids := make(map[string]int)
	for _, m := range matches {
		sids[string(m[SID_INDEX])]++
	}
	if len(sids) != 2 {
		t.Fatalf("Expected only 2 sids, got %d\n", len(sids))
	}
	for k, c := range sids {
		if c < 35 {
			t.Fatalf("Expected ~50 (+-15) msgs for '%s', got %d\n", k, c)
		}
	}
}

func TestStopServer(t *testing.T) {
	s.stopServer()
}
