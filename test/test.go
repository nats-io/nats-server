// Copyright 2012-2013 Apcera Inc. All rights reserved.

package test

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os/exec"
	"regexp"
	"runtime"
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

var DefaultTestOptions = server.Options{
	Host:   "localhost",
	Port:   4222,
	NoLog:  true,
	NoSigs: true,
}

func runDefaultServer() *server.Server {
	return RunServer(&DefaultTestOptions)
}

// New Go Routine based server
func RunServer(opts *server.Options) *server.Server {
	if opts == nil {
		opts = &DefaultTestOptions
	}
	s := server.New(opts)
	if s == nil {
		panic("No NATS Server object returned.")
	}

	// Run server in Go routine.
	go s.Start()

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
	panic("Unable to start NATS Server in Go Routine")
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

func stackFatalf(t tLogger, f string, args ...interface{}) {
	lines := make([]string, 0, 32)
	msg := fmt.Sprintf(f, args...)
	lines = append(lines, msg)

	// Ignore ourselves
	_, testFile, _, _ := runtime.Caller(0)

	// Generate the Stack of callers:
	for i := 0; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if ok == false {
			break
		}
		if file == testFile {
			continue
		}
		msg := fmt.Sprintf("%d - %s:%d", i, file, line)
		lines = append(lines, msg)
	}

	t.Fatalf("%s", strings.Join(lines, "\n"))
}

func acceptRouteConn(t tLogger, host string, timeout time.Duration) net.Conn {
	l, e := net.Listen("tcp", host)
	if e != nil {
		stackFatalf(t, "Error listening for route connection on %v: %v", host, e)
	}
	defer l.Close()

	tl := l.(*net.TCPListener)
	tl.SetDeadline(time.Now().Add(timeout))

	conn, err := l.Accept()
	if err != nil {
		stackFatalf(t, "Did not receive a route connection request: %v", err)
	}
	return conn
}

func createRouteConn(t tLogger, host string, port int) net.Conn {
	return createClientConn(t, host, port)
}

func createClientConn(t tLogger, host string, port int) net.Conn {
	addr := fmt.Sprintf("%s:%d", host, port)
	c, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		stackFatalf(t, "Could not connect to server: %v\n", err)
	}
	return c
}

func doConnect(t tLogger, c net.Conn, verbose, pedantic, ssl bool) {
	buf := expectResult(t, c, infoRe)
	js := infoRe.FindAllSubmatch(buf, 1)[0][1]
	var sinfo server.Info
	err := json.Unmarshal(js, &sinfo)
	if err != nil {
		stackFatalf(t, "Could not unmarshal INFO json: %v\n", err)
	}
	cs := fmt.Sprintf("CONNECT {\"verbose\":%v,\"pedantic\":%v,\"ssl_required\":%v}\r\n", verbose, pedantic, ssl)
	sendProto(t, c, cs)
}

func doDefaultConnect(t tLogger, c net.Conn) {
	// Basic Connect
	doConnect(t, c, false, false, false)
}

func checkSocket(t tLogger, addr string, wait time.Duration) {
	end := time.Now().Add(wait)
	for time.Now().Before(end) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			// Retry
			continue
		}
		// We bound to the addr, so close and return success.
		conn.Close()
		return
	}
	// We have failed to bind the socket in the time allowed.
	t.Fatalf("Failed to connect to the socket: %q", addr)
}

const CONNECT_F = "CONNECT {\"verbose\":false,\"user\":\"%s\",\"pass\":\"%s\",\"name\":\"%s\"}\r\n"

func doRouteAuthConnect(t tLogger, c net.Conn, user, pass, id string) {
	cs := fmt.Sprintf(CONNECT_F, user, pass, id)
	sendProto(t, c, cs)
}

func setupRouteEx(t tLogger, c net.Conn, opts *server.Options, id string) (sendFun, expectFun) {
	user := opts.ClusterUsername
	pass := opts.ClusterPassword
	doRouteAuthConnect(t, c, user, pass, id)
	send := sendCommand(t, c)
	expect := expectCommand(t, c)
	return send, expect
}

func setupRoute(t tLogger, c net.Conn, opts *server.Options) (sendFun, expectFun) {
	u := make([]byte, 16)
	io.ReadFull(rand.Reader, u)
	id := fmt.Sprintf("ROUTER:%s", hex.EncodeToString(u))
	return setupRouteEx(t, c, opts, id)
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
		stackFatalf(t, "Error writing command to conn: %v\n", err)
	}
	if n != len(op) {
		stackFatalf(t, "Partial write: %d vs %d\n", n, len(op))
	}
}

var (
	infoRe    = regexp.MustCompile(`INFO\s+([^\r\n]+)\r\n`)
	pingRe    = regexp.MustCompile(`PING\r\n`)
	pongRe    = regexp.MustCompile(`PONG\r\n`)
	msgRe     = regexp.MustCompile(`(?:(?:MSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\s*\r\n([^\\r\\n]*?)\r\n)+?)`)
	okRe      = regexp.MustCompile(`\A\+OK\r\n`)
	errRe     = regexp.MustCompile(`\A\-ERR\s+([^\r\n]+)\r\n`)
	subRe     = regexp.MustCompile(`SUB\s+([^\s]+)((\s+)([^\s]+))?\s+([^\s]+)\r\n`)
	unsubRe   = regexp.MustCompile(`UNSUB\s+([^\s]+)(\s+(\d+))?\r\n`)
	connectRe = regexp.MustCompile(`CONNECT\s+([^\r\n]+)\r\n`)
)

const (
	SUB_INDEX   = 1
	SID_INDEX   = 2
	REPLY_INDEX = 4
	LEN_INDEX   = 5
	MSG_INDEX   = 6
)

// Reuse expect buffer
// TODO(dlc) - This may be too simplistic in the long run, may need
// to consider holding onto data from previous reads matched by conn.
var expBuf = make([]byte, 32768)

// Test result from server against regexp
func expectResult(t tLogger, c net.Conn, re *regexp.Regexp) []byte {
	// Wait for commands to be processed and results queued for read
	c.SetReadDeadline(time.Now().Add(1 * time.Second))
	defer c.SetReadDeadline(time.Time{})

	n, err := c.Read(expBuf)
	if n <= 0 && err != nil {
		stackFatalf(t, "Error reading from conn: %v\n", err)
	}
	buf := expBuf[:n]

	if !re.Match(buf) {
		stackFatalf(t, "Response did not match expected: \n\tReceived:'%q'\n\tExpected:'%s'\n", buf, re)
	}
	return buf
}

func expectNothing(t tLogger, c net.Conn) {
	c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	defer c.SetReadDeadline(time.Time{})

	n, err := c.Read(expBuf)
	if err == nil && n > 0 {
		stackFatalf(t, "Expected nothing, received: '%q'\n", expBuf[:n])
	}
}

// This will check that we got what we expected.
func checkMsg(t tLogger, m [][]byte, subject, sid, reply, len, msg string) {
	if string(m[SUB_INDEX]) != subject {
		stackFatalf(t, "Did not get correct subject: expected '%s' got '%s'\n", subject, m[SUB_INDEX])
	}
	if sid != "" && string(m[SID_INDEX]) != sid {
		stackFatalf(t, "Did not get correct sid: expected '%s' got '%s'\n", sid, m[SID_INDEX])
	}
	if string(m[REPLY_INDEX]) != reply {
		stackFatalf(t, "Did not get correct reply: expected '%s' got '%s'\n", reply, m[REPLY_INDEX])
	}
	if string(m[LEN_INDEX]) != len {
		stackFatalf(t, "Did not get correct msg length: expected '%s' got '%s'\n", len, m[LEN_INDEX])
	}
	if string(m[MSG_INDEX]) != msg {
		stackFatalf(t, "Did not get correct msg: expected '%s' got '%s'\n", msg, m[MSG_INDEX])
	}
}

// Closure for expectMsgs
func expectMsgsCommand(t tLogger, ef expectFun) func(int) [][][]byte {
	return func(expected int) [][][]byte {
		buf := ef(msgRe)
		matches := msgRe.FindAllSubmatch(buf, -1)
		if len(matches) != expected {
			stackFatalf(t, "Did not get correct # msgs: %d vs %d\n", len(matches), expected)
		}
		return matches
	}
}
