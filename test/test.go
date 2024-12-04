// Copyright 2012-2024 The NATS Authors
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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	srvlog "github.com/nats-io/nats-server/v2/logger"
)

// So we can pass tests and benchmarks..
type tLogger interface {
	Fatalf(format string, args ...any)
	Errorf(format string, args ...any)
}

// DefaultTestOptions are default options for the unit tests.
var DefaultTestOptions = server.Options{
	Host:                  "127.0.0.1",
	Port:                  4222,
	NoLog:                 true,
	NoSigs:                true,
	MaxControlLine:        4096,
	DisableShortFirstPing: true,
}

// RunDefaultServer starts a new Go routine based server using the default options
func RunDefaultServer() *server.Server {
	return RunServer(&DefaultTestOptions)
}

func RunRandClientPortServer() *server.Server {
	opts := DefaultTestOptions
	opts.Port = -1
	return RunServer(&opts)
}

// To turn on server tracing and debugging and logging which are
// normally suppressed.
var (
	doLog   = false
	doTrace = false
	doDebug = false
)

// RunServer starts a new Go routine based server
func RunServer(opts *server.Options) *server.Server {
	return RunServerCallback(opts, nil)
}

func RunServerCallback(opts *server.Options, callback func(*server.Server)) *server.Server {
	if opts == nil {
		opts = &DefaultTestOptions
	}
	// Optionally override for individual debugging of tests
	opts.NoLog = !doLog
	opts.Trace = doTrace
	opts.Debug = doDebug
	// For all tests in the "test" package, we will disable route pooling.
	opts.Cluster.PoolSize = -1
	// Also disable compression for "test" package.
	opts.Cluster.Compression.Mode = server.CompressionOff
	opts.LeafNode.Compression.Mode = server.CompressionOff

	s, err := server.NewServer(opts)
	if err != nil || s == nil {
		panic(fmt.Sprintf("No NATS Server object returned: %v", err))
	}

	if doLog {
		s.ConfigureLogger()
	}

	if ll := os.Getenv("NATS_LOGGING"); ll != "" {
		log := srvlog.NewTestLogger(fmt.Sprintf("[%s] | ", s), true)
		debug := ll == "debug" || ll == "trace"
		trace := ll == "trace"
		s.SetLoggerV2(log, debug, trace, false)
	}

	if callback != nil {
		callback(s)
	}

	// Run server in Go routine.
	go s.Start()

	// Wait for accept loop(s) to be started
	if !s.ReadyForConnections(10 * time.Second) {
		panic("Unable to start NATS Server in Go Routine")
	}
	return s
}

// LoadConfig loads a configuration from a filename
func LoadConfig(configFile string) *server.Options {
	opts, err := server.ProcessConfigFile(configFile)
	if err != nil {
		panic(fmt.Sprintf("Error processing configuration file: %v", err))
	}
	return opts
}

// RunServerWithConfig starts a new Go routine based server with a configuration file.
func RunServerWithConfig(configFile string) (srv *server.Server, opts *server.Options) {
	opts = LoadConfig(configFile)
	srv = RunServer(opts)
	return
}

// RunServerWithConfigOverrides starts a new Go routine based server with a configuration file,
// providing a callback to update the options configured.
func RunServerWithConfigOverrides(configFile string, optsCallback func(*server.Options), svrCallback func(*server.Server)) (srv *server.Server, opts *server.Options) {
	opts = LoadConfig(configFile)
	if optsCallback != nil {
		optsCallback(opts)
	}
	srv = RunServerCallback(opts, svrCallback)
	return
}

func stackFatalf(t tLogger, f string, args ...any) {
	lines := make([]string, 0, 32)
	msg := fmt.Sprintf(f, args...)
	lines = append(lines, msg)

	// Ignore ourselves
	_, testFile, _, _ := runtime.Caller(0)

	// Generate the Stack of callers:
	for i := 0; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
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
	tl.SetDeadline(time.Time{})

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
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		stackFatalf(t, "Could not connect to server: %v\n", err)
	}
	return c
}

func checkSocket(t tLogger, addr string, wait time.Duration) {
	end := time.Now().Add(wait)
	for time.Now().Before(end) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			// Retry after 50ms
			time.Sleep(50 * time.Millisecond)
			continue
		}
		conn.Close()
		// Wait a bit to give a chance to the server to remove this
		// "client" from its state, which may otherwise interfere with
		// some tests.
		time.Sleep(25 * time.Millisecond)
		return
	}
	// We have failed to bind the socket in the time allowed.
	t.Fatalf("Failed to connect to the socket: %q", addr)
}

func checkInfoMsg(t tLogger, c net.Conn) server.Info {
	buf := expectResult(t, c, infoRe)
	js := infoRe.FindAllSubmatch(buf, 1)[0][1]
	var sinfo server.Info
	err := json.Unmarshal(js, &sinfo)
	if err != nil {
		stackFatalf(t, "Could not unmarshal INFO json: %v\n", err)
	}
	return sinfo
}

func doHeadersConnect(t tLogger, c net.Conn, verbose, pedantic, ssl, headers bool) {
	checkInfoMsg(t, c)
	cs := fmt.Sprintf("CONNECT {\"verbose\":%v,\"pedantic\":%v,\"tls_required\":%v,\"headers\":%v}\r\n",
		verbose, pedantic, ssl, headers)
	sendProto(t, c, cs)
}

func doConnect(t tLogger, c net.Conn, verbose, pedantic, ssl bool) {
	doHeadersConnect(t, c, verbose, pedantic, ssl, false)
}

func doDefaultHeadersConnect(t tLogger, c net.Conn) {
	doHeadersConnect(t, c, false, false, false, true)
}

func doDefaultConnect(t tLogger, c net.Conn) {
	// Basic Connect
	doConnect(t, c, false, false, false)
}

const routeConnectProto = "CONNECT {\"verbose\":false,\"user\":\"%s\",\"pass\":\"%s\",\"name\":\"%s\",\"cluster\":\"xyz\"}\r\n"

func doRouteAuthConnect(t tLogger, c net.Conn, user, pass, id string) {
	cs := fmt.Sprintf(routeConnectProto, user, pass, id)
	sendProto(t, c, cs)
}

func setupRouteEx(t tLogger, c net.Conn, opts *server.Options, id string) (sendFun, expectFun) {
	user := opts.Cluster.Username
	pass := opts.Cluster.Password
	doRouteAuthConnect(t, c, user, pass, id)
	return sendCommand(t, c), expectCommand(t, c)
}

func setupRoute(t tLogger, c net.Conn, opts *server.Options) (sendFun, expectFun) {
	u := make([]byte, 16)
	io.ReadFull(rand.Reader, u)
	id := fmt.Sprintf("ROUTER:%s", hex.EncodeToString(u))
	return setupRouteEx(t, c, opts, id)
}

func setupHeaderConn(t tLogger, c net.Conn) (sendFun, expectFun) {
	doDefaultHeadersConnect(t, c)
	return sendCommand(t, c), expectCommand(t, c)
}

func setupConn(t tLogger, c net.Conn) (sendFun, expectFun) {
	doDefaultConnect(t, c)
	return sendCommand(t, c), expectCommand(t, c)
}

func setupConnWithProto(t tLogger, c net.Conn, proto int) (sendFun, expectFun) {
	checkInfoMsg(t, c)
	cs := fmt.Sprintf("CONNECT {\"verbose\":%v,\"pedantic\":%v,\"tls_required\":%v,\"protocol\":%d}\r\n", false, false, false, proto)
	sendProto(t, c, cs)
	return sendCommand(t, c), expectCommand(t, c)
}

func setupConnWithAccount(t tLogger, s *server.Server, c net.Conn, account string) (sendFun, expectFun) {
	info := checkInfoMsg(t, c)
	s.RegisterAccount(account)
	acc, err := s.LookupAccount(account)
	if err != nil {
		t.Fatalf("Unexpected Error: %v", err)
	}
	cs := fmt.Sprintf("CONNECT {\"verbose\":%v,\"pedantic\":%v,\"tls_required\":%v}\r\n", false, false, false)
	sendProto(t, c, cs)

	send, expect := sendCommand(t, c), expectCommand(t, c)
	send("PING\r\n")
	expect(pongRe)

	nc := s.GetClient(info.CID)
	if nc == nil {
		t.Fatalf("Could not get client for CID:%d", info.CID)
	}
	nc.RegisterUser(&server.User{Account: acc})

	return send, expect
}

func setupConnWithUserPass(t tLogger, c net.Conn, username, password string) (sendFun, expectFun) {
	checkInfoMsg(t, c)
	cs := fmt.Sprintf("CONNECT {\"verbose\":%v,\"pedantic\":%v,\"tls_required\":%v,\"protocol\":1,\"user\":%q,\"pass\":%q}\r\n",
		false, false, false, username, password)
	sendProto(t, c, cs)
	return sendCommand(t, c), expectLefMostCommand(t, c)
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

// Closure version for easier reading
func expectLefMostCommand(t tLogger, c net.Conn) expectFun {
	var buf []byte
	return func(re *regexp.Regexp) []byte {
		return expectLeftMostResult(t, c, re, &buf)
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
	anyRe       = regexp.MustCompile(`.*`)
	infoRe      = regexp.MustCompile(`INFO\s+([^\r\n]+)\r\n`)
	infoStartRe = regexp.MustCompile(`^INFO\s+([^\r\n]+)\r\n`)
	pingRe      = regexp.MustCompile(`^PING\r\n`)
	pongRe      = regexp.MustCompile(`^PONG\r\n`)
	hmsgRe      = regexp.MustCompile(`(?:(?:HMSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\s+(\d+)\s*\r\n([^\\r\\n]*?)\r\n)+?)`)
	msgRe       = regexp.MustCompile(`(?:(?:MSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\s*\r\n([^\\r\\n]*?)\r\n)+?)`)
	rawMsgRe    = regexp.MustCompile(`(?:(?:MSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\s*\r\n(.*?)))`)
	okRe        = regexp.MustCompile(`\A\+OK\r\n`)
	errRe       = regexp.MustCompile(`\A\-ERR\s+([^\r\n]+)\r\n`)
	connectRe   = regexp.MustCompile(`CONNECT\s+([^\r\n]+)\r\n`)
	rsubRe      = regexp.MustCompile(`RS\+\s+([^\s]+)\s+([^\s]+)\s*([^\s]+)?\s*(\d+)?\r\n`)
	runsubRe    = regexp.MustCompile(`RS\-\s+([^\s]+)\s+([^\s]+)\s*([^\s]+)?\r\n`)
	rmsgRe      = regexp.MustCompile(`(?:(?:RMSG\s+([^\s]+)\s+([^\s]+)\s+(?:([|+]\s+([\w\s]+)|[^\s]+)[^\S\r\n]+)?(\d+)\s*\r\n([^\\r\\n]*?)\r\n)+?)`)
	asubRe      = regexp.MustCompile(`A\+\s+([^\r\n]+)\r\n`)
	aunsubRe    = regexp.MustCompile(`A\-\s+([^\r\n]+)\r\n`)
	lsubRe      = regexp.MustCompile(`LS\+\s+([^\s]+)\s*([^\s]+)?\s*(\d+)?\r\n`)
	lunsubRe    = regexp.MustCompile(`LS\-\s+([^\s]+)\s*([^\s]+)\s*([^\s]+)?\r\n`)
	lmsgRe      = regexp.MustCompile(`(?:(?:LMSG\s+([^\s]+)\s+(?:([|+]\s+([\w\s]+)|[^\s]+)[^\S\r\n]+)?(\d+)\s*\r\n([^\\r\\n]*?)\r\n)+?)`)
	rlsubRe     = regexp.MustCompile(`LS\+\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s*([^\s]+)?\s*(\d+)?\r\n`)
	rlunsubRe   = regexp.MustCompile(`LS\-\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s*([^\s]+)?\r\n`)
)

const (
	// Regular Messages
	subIndex   = 1
	sidIndex   = 2
	replyIndex = 4
	lenIndex   = 5
	msgIndex   = 6
	// Headers
	hlenIndex = 5
	tlenIndex = 6
	hmsgIndex = 7

	// Routed Messages
	accIndex           = 1
	rsubIndex          = 2
	replyAndQueueIndex = 3
)

// Test result from server against regexp and return left most match
func expectLeftMostResult(t tLogger, c net.Conn, re *regexp.Regexp, buf *[]byte) []byte {
	recv := func() []byte {
		expBuf := make([]byte, 32768)
		// Wait for commands to be processed and results queued for read
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := c.Read(expBuf)
		c.SetReadDeadline(time.Time{})

		if n <= 0 && err != nil {
			stackFatalf(t, "Error reading from conn: %v\n", err)
		}
		return expBuf[:n]
	}
	if len(*buf) == 0 {
		*buf = recv()
	}
	emptyCnt := 0
	for {
		result := re.Find(*buf)
		if result == nil {
			emptyCnt++
			if emptyCnt > 5 {
				stackFatalf(t, "Reading empty data too often\n")
			}
			*buf = append(*buf, recv()...)
		} else {
			cutIdx := strings.Index(string(*buf), string(result)) + len(result)
			*buf = (*buf)[cutIdx:]
			return result
		}
	}
}

// Test result from server against regexp
func expectResult(t tLogger, c net.Conn, re *regexp.Regexp) []byte {
	expBuf := make([]byte, 32768)
	// Wait for commands to be processed and results queued for read
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := c.Read(expBuf)
	c.SetReadDeadline(time.Time{})

	if n <= 0 && err != nil {
		stackFatalf(t, "Error reading from conn: %v\n", err)
	}
	buf := expBuf[:n]
	if !re.Match(buf) {
		stackFatalf(t, "Response did not match expected: \n\tReceived:'%q'\n\tExpected:'%s'", buf, re)
	}
	return buf
}

func peek(c net.Conn) []byte {
	expBuf := make([]byte, 32768)
	c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	n, err := c.Read(expBuf)
	c.SetReadDeadline(time.Time{})
	if err != nil || n <= 0 {
		return nil
	}
	return expBuf
}

func expectDisconnect(t *testing.T, c net.Conn) {
	t.Helper()
	var b [8]byte
	c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err := c.Read(b[:])
	c.SetReadDeadline(time.Time{})
	if err != io.EOF {
		t.Fatalf("Expected a disconnect")
	}
}

func expectNothing(t tLogger, c net.Conn) {
	expectNothingTimeout(t, c, time.Now().Add(100*time.Millisecond))
}

func expectNothingTimeout(t tLogger, c net.Conn, dl time.Time) {
	expBuf := make([]byte, 32)
	c.SetReadDeadline(dl)
	n, err := c.Read(expBuf)
	c.SetReadDeadline(time.Time{})
	if err == nil && n > 0 {
		stackFatalf(t, "Expected nothing, received: '%q'\n", expBuf[:n])
	}
}

// This will check that we got what we expected from a normal message.
func checkMsg(t tLogger, m [][]byte, subject, sid, reply, len, msg string) {
	if string(m[subIndex]) != subject {
		stackFatalf(t, "Did not get correct subject: expected '%s' got '%s'\n", subject, m[subIndex])
	}
	if sid != "" && string(m[sidIndex]) != sid {
		stackFatalf(t, "Did not get correct sid: expected '%s' got '%s'\n", sid, m[sidIndex])
	}
	if string(m[replyIndex]) != reply {
		stackFatalf(t, "Did not get correct reply: expected '%s' got '%s'\n", reply, m[replyIndex])
	}
	if string(m[lenIndex]) != len {
		stackFatalf(t, "Did not get correct msg length: expected '%s' got '%s'\n", len, m[lenIndex])
	}
	if string(m[msgIndex]) != msg {
		stackFatalf(t, "Did not get correct msg: expected '%s' got '%s'\n", msg, m[msgIndex])
	}
}

func checkRmsg(t tLogger, m [][]byte, account, subject, replyAndQueues, len, msg string) {
	if string(m[accIndex]) != account {
		stackFatalf(t, "Did not get correct account: expected '%s' got '%s'\n", account, m[accIndex])
	}
	if string(m[rsubIndex]) != subject {
		stackFatalf(t, "Did not get correct subject: expected '%s' got '%s'\n", subject, m[rsubIndex])
	}
	if string(m[lenIndex]) != len {
		stackFatalf(t, "Did not get correct msg length: expected '%s' got '%s'\n", len, m[lenIndex])
	}
	if string(m[replyAndQueueIndex]) != replyAndQueues {
		stackFatalf(t, "Did not get correct reply/queues: expected '%s' got '%s'\n", replyAndQueues, m[replyAndQueueIndex])
	}
}

func checkLmsg(t tLogger, m [][]byte, subject, replyAndQueues, len, msg string) {
	if string(m[rsubIndex-1]) != subject {
		stackFatalf(t, "Did not get correct subject: expected '%s' got '%s'\n", subject, m[rsubIndex-1])
	}
	if string(m[lenIndex-1]) != len {
		stackFatalf(t, "Did not get correct msg length: expected '%s' got '%s'\n", len, m[lenIndex-1])
	}
	if string(m[replyAndQueueIndex-1]) != replyAndQueues {
		stackFatalf(t, "Did not get correct reply/queues: expected '%s' got '%s'\n", replyAndQueues, m[replyAndQueueIndex-1])
	}
}

// This will check that we got what we expected from a header message.
func checkHmsg(t tLogger, m [][]byte, subject, sid, reply, hlen, len, hdr, msg string) {
	if string(m[subIndex]) != subject {
		stackFatalf(t, "Did not get correct subject: expected '%s' got '%s'\n", subject, m[subIndex])
	}
	if sid != "" && string(m[sidIndex]) != sid {
		stackFatalf(t, "Did not get correct sid: expected '%s' got '%s'\n", sid, m[sidIndex])
	}
	if string(m[replyIndex]) != reply {
		stackFatalf(t, "Did not get correct reply: expected '%s' got '%s'\n", reply, m[replyIndex])
	}
	if string(m[hlenIndex]) != hlen {
		stackFatalf(t, "Did not get correct header length: expected '%s' got '%s'\n", hlen, m[hlenIndex])
	}
	if string(m[tlenIndex]) != len {
		stackFatalf(t, "Did not get correct msg length: expected '%s' got '%s'\n", len, m[tlenIndex])
	}
	// Extract the payload and break up the headers and msg.
	payload := string(m[hmsgIndex])
	hi, _ := strconv.Atoi(hlen)
	rhdr, rmsg := payload[:hi], payload[hi:]
	if rhdr != hdr {
		stackFatalf(t, "Did not get correct headers: expected '%s' got '%s'\n", hdr, rhdr)
	}
	if rmsg != msg {
		stackFatalf(t, "Did not get correct msg: expected '%s' got '%s'\n", msg, rmsg)
	}
}

// Closure for expectMsgs
func expectRmsgsCommand(t tLogger, ef expectFun) func(int) [][][]byte {
	return func(expected int) [][][]byte {
		buf := ef(rmsgRe)
		matches := rmsgRe.FindAllSubmatch(buf, -1)
		if len(matches) != expected {
			stackFatalf(t, "Did not get correct # routed msgs: %d vs %d\n", len(matches), expected)
		}
		return matches
	}
}

// Closure for expectHMsgs
func expectHeaderMsgsCommand(t tLogger, ef expectFun) func(int) [][][]byte {
	return func(expected int) [][][]byte {
		buf := ef(hmsgRe)
		matches := hmsgRe.FindAllSubmatch(buf, -1)
		if len(matches) != expected {
			stackFatalf(t, "Did not get correct # msgs: %d vs %d\n", len(matches), expected)
		}
		return matches
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

// This will check that the matches include at least one of the sids. Useful for checking
// that we received messages on a certain queue group.
func checkForQueueSid(t tLogger, matches [][][]byte, sids []string) {
	seen := make(map[string]int, len(sids))
	for _, sid := range sids {
		seen[sid] = 0
	}
	for _, m := range matches {
		sid := string(m[sidIndex])
		if _, ok := seen[sid]; ok {
			seen[sid]++
		}
	}
	// Make sure we only see one and exactly one.
	total := 0
	for _, n := range seen {
		total += n
	}
	if total != 1 {
		stackFatalf(t, "Did not get a msg for queue sids group: expected 1 got %d\n", total)
	}
}

// This will check that the matches include all of the sids. Useful for checking
// that we received messages on all subscribers.
func checkForPubSids(t tLogger, matches [][][]byte, sids []string) {
	seen := make(map[string]int, len(sids))
	for _, sid := range sids {
		seen[sid] = 0
	}
	for _, m := range matches {
		sid := string(m[sidIndex])
		if _, ok := seen[sid]; ok {
			seen[sid]++
		}
	}
	// Make sure we only see one and exactly one for each sid.
	for sid, n := range seen {
		if n != 1 {
			stackFatalf(t, "Did not get a msg for sid[%s]: expected 1 got %d\n", sid, n)

		}
	}
}

// Helper function to generate next opts to make sure no port conflicts etc.
func nextServerOpts(opts *server.Options) *server.Options {
	nopts := opts.Clone()
	nopts.Port++
	nopts.Cluster.Port++
	nopts.HTTPPort++
	return nopts
}

func createTempFile(t testing.TB, prefix string) *os.File {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	return file
}
