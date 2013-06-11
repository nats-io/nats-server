// Copyright 2012 Apcera Inc. All rights reserved.

package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/apcera/gnatsd/server"
)

const natsServerExe = "../gnatsd"

type natsServer struct {
	args []string
	cmd  *exec.Cmd
}

// So we can pass tests and benchmarks..
type tLogger interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

var defaultServerOptions = server.Options{
	Host:   "localhost",
	Port:   4222,
	NoSigs: true,
}

func runDefaultServer() *server.Server {
	return runServer(&defaultServerOptions)
}

// New Go Routine based server
func runServer(opts *server.Options) *server.Server {
	if opts == nil {
		opts = &defaultServerOptions
	}
	s := server.New(opts)
	if s == nil {
		panic("No nats server object returned.")
	}

	go s.AcceptLoop()

	// Make sure we are running and can bind before returning.
	addr := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	end := time.Now().Add(10 * time.Second)
	for time.Now().Before(end) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			// Retry
			continue
		}
		conn.Close()
		return s
	}
	panic("Unable to start NATs Server in Go Routine")
}

func startServer(t tLogger, port int, other string) *natsServer {
	var s natsServer
	args := fmt.Sprintf("-p %d %s", port, other)
	s.args = strings.Split(args, " ")
	s.cmd = exec.Command(natsServerExe, s.args...)
	err := s.cmd.Start()
	if err != nil {
		s.cmd = nil
		t.Errorf("Could not start <%s> [%s], is NATS installed and in path?", natsServerExe, err)
		return &s
	}
	// Give it time to start up
	start := time.Now()
	for {
		addr := fmt.Sprintf("localhost:%d", port)
		c, err := net.Dial("tcp", addr)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			if time.Since(start) > (5 * time.Second) {
				t.Fatalf("Timed out trying to connect to %s", natsServerExe)
				return nil
			}
		} else {
			c.Close()
			break
		}
	}
	return &s
}

func (s *natsServer) stopServer() {
	if s.cmd != nil && s.cmd.Process != nil {
		s.cmd.Process.Kill()
		s.cmd.Process.Wait()
	}
}

func createClientConn(t tLogger, host string, port int) net.Conn {
	addr := fmt.Sprintf("%s:%d", host, port)
	c, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		t.Fatalf("Could not connect to server: %v\n", err)
	}
	return c
}

func doConnect(t tLogger, c net.Conn, verbose, pedantic, ssl bool) {
	buf := expectResult(t, c, infoRe)
	js := infoRe.FindAllSubmatch(buf, 1)[0][1]
	var sinfo server.Info
	err := json.Unmarshal(js, &sinfo)
	if err != nil {
		t.Fatalf("Could not unmarshal INFO json: %v\n", err)
	}
	cs := fmt.Sprintf("CONNECT {\"verbose\":%v,\"pedantic\":%v,\"ssl_required\":%v}\r\n", verbose, pedantic, ssl)
	sendProto(t, c, cs)
}

func doDefaultConnect(t tLogger, c net.Conn) {
	// Basic Connect
	doConnect(t, c, false, false, false)
}

func setupConn(t tLogger, c net.Conn) (sendFun, expectFun) {
	doDefaultConnect(t, c)
	send := sendCommand(t, c)
	expect := expectCommand(t, c)
	return send, expect
}

type sendFun func(string)
type expectFun func(*regexp.Regexp) []byte

// Closure version for easier reading
func sendCommand(t tLogger, c net.Conn) sendFun {
	return func(op string) {
		sendProto(t, c, op)
	}
}

// Closure version for easier reading
func expectCommand(t tLogger, c net.Conn) expectFun {
	return func(re *regexp.Regexp) []byte {
		return expectResult(t, c, re)
	}
}

// Send the protocol command to the server.
func sendProto(t tLogger, c net.Conn, op string) {
	n, err := c.Write([]byte(op))
	if err != nil {
		t.Fatalf("Error writing command to conn: %v\n", err)
	}
	if n != len(op) {
		t.Fatalf("Partial write: %d vs %d\n", n, len(op))
	}
}

var (
	infoRe = regexp.MustCompile(`\AINFO\s+([^\r\n]+)\r\n`)
	pingRe = regexp.MustCompile(`\APING\r\n`)
	pongRe = regexp.MustCompile(`\APONG\r\n`)
	msgRe  = regexp.MustCompile(`(?:(?:MSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\r\n([^\\r\\n]*?)\r\n)+?)`)
	okRe   = regexp.MustCompile(`\A\+OK\r\n`)
	errRe  = regexp.MustCompile(`\A\-ERR\s+([^\r\n]+)\r\n`)
)

const (
	SUB_INDEX   = 1
	SID_INDEX   = 2
	REPLY_INDEX = 4
	LEN_INDEX   = 5
	MSG_INDEX   = 6
)

// Reuse expect buffer
var expBuf = make([]byte, 32768)

// Test result from server against regexp
func expectResult(t tLogger, c net.Conn, re *regexp.Regexp) []byte {
	// Wait for commands to be processed and results queued for read
	// time.Sleep(10 * time.Millisecond)
	c.SetReadDeadline(time.Now().Add(1 * time.Second))
	defer c.SetReadDeadline(time.Time{})

	n, err := c.Read(expBuf)
	if n <= 0 && err != nil {
		t.Fatalf("Error reading from conn: %v\n", err)
	}
	buf := expBuf[:n]

	if !re.Match(buf) {
		buf = bytes.Replace(buf, []byte("\r\n"), []byte("\\r\\n"), -1)
		t.Fatalf("Response did not match expected: \n\tReceived:'%s'\n\tExpected:'%s'\n", buf, re)
	}
	return buf
}

// This will check that we got what we expected.
func checkMsg(t tLogger, m [][]byte, subject, sid, reply, len, msg string) {
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
func expectMsgsCommand(t tLogger, ef expectFun) func(int) [][][]byte {
	return func(expected int) [][][]byte {
		buf := ef(msgRe)
		matches := msgRe.FindAllSubmatch(buf, -1)
		if len(matches) != expected {
			t.Fatalf("Did not get correct # msgs: %d vs %d\n", len(matches), expected)
		}
		return matches
	}
}
