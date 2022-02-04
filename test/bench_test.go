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

// Please note that these tests will stress a system and they need generous
// amounts of CPU, Memory and network sockets. Make sure the 'open files'
// setting for your platform is at least 8192. On linux and MacOSX you can
// do this via 'ulimit -n 8192'
//

package test

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

const PERF_PORT = 8422

// For Go routine based server.
func runBenchServer() *server.Server {
	opts := DefaultTestOptions
	opts.Port = PERF_PORT
	opts.DisableShortFirstPing = true
	return RunServer(&opts)
}

const defaultRecBufSize = 32768
const defaultSendBufSize = 32768

func flushConnection(b *testing.B, c net.Conn) {
	buf := make([]byte, 32)
	c.Write([]byte("PING\r\n"))
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := c.Read(buf)
	c.SetReadDeadline(time.Time{})
	if err != nil {
		b.Fatalf("Failed read: %v\n", err)
	}
	if n != 6 && buf[0] != 'P' && buf[1] != 'O' {
		b.Fatalf("Failed read of PONG: %s\n", buf)
	}
}

func benchPub(b *testing.B, subject, payload string) {
	b.StopTimer()
	s := runBenchServer()
	c := createClientConn(b, "127.0.0.1", PERF_PORT)
	doDefaultConnect(b, c)
	bw := bufio.NewWriterSize(c, defaultSendBufSize)
	sendOp := []byte(fmt.Sprintf("PUB %s %d\r\n%s\r\n", subject, len(payload), payload))
	b.SetBytes(int64(len(sendOp)))
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bw.Write(sendOp)
	}
	bw.Flush()
	flushConnection(b, c)
	b.StopTimer()
	c.Close()
	s.Shutdown()
}

var ch = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@$#%^&*()")

func sizedBytes(sz int) []byte {
	b := make([]byte, sz)
	for i := range b {
		b[i] = ch[rand.Intn(len(ch))]
	}
	return b
}

func sizedString(sz int) string {
	return string(sizedBytes(sz))
}

// Publish subject for pub benchmarks.
var psub = "a"

func Benchmark______Pub0b_Payload(b *testing.B) {
	benchPub(b, psub, "")
}

func Benchmark______Pub8b_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(8)
	benchPub(b, psub, s)
}

func Benchmark_____Pub32b_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(32)
	benchPub(b, psub, s)
}

func Benchmark____Pub128B_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(128)
	benchPub(b, psub, s)
}

func Benchmark____Pub256B_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(256)
	benchPub(b, psub, s)
}

func Benchmark______Pub1K_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(1024)
	benchPub(b, psub, s)
}

func Benchmark______Pub4K_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(4 * 1024)
	benchPub(b, psub, s)
}

func Benchmark______Pub8K_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(8 * 1024)
	benchPub(b, psub, s)
}

func Benchmark_____Pub32K_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(32 * 1024)
	benchPub(b, psub, s)
}

func drainConnection(b *testing.B, c net.Conn, ch chan bool, expected int) {
	buf := make([]byte, defaultRecBufSize)
	bytes := 0

	for {
		c.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := c.Read(buf)
		if err != nil {
			b.Errorf("Error on read: %v\n", err)
			break
		}
		bytes += n
		if bytes >= expected {
			break
		}
	}
	if bytes != expected {
		b.Errorf("Did not receive all bytes: %d vs %d\n", bytes, expected)
	}
	ch <- true
}

// Benchmark the authorization code path.
func Benchmark__AuthPub0b_Payload(b *testing.B) {
	b.StopTimer()

	srv, opts := RunServerWithConfig("./configs/authorization.conf")
	opts.DisableShortFirstPing = true
	defer srv.Shutdown()

	c := createClientConn(b, opts.Host, opts.Port)
	defer c.Close()
	expectAuthRequired(b, c)

	cs := fmt.Sprintf("CONNECT {\"verbose\":false,\"user\":\"%s\",\"pass\":\"%s\"}\r\n", "bench", DefaultPass)
	sendProto(b, c, cs)

	bw := bufio.NewWriterSize(c, defaultSendBufSize)
	sendOp := []byte("PUB a 0\r\n\r\n")
	b.SetBytes(int64(len(sendOp)))
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bw.Write(sendOp)
	}
	bw.Flush()
	flushConnection(b, c)
	b.StopTimer()
}

func Benchmark_____________PubSub(b *testing.B) {
	b.StopTimer()
	s := runBenchServer()
	c := createClientConn(b, "127.0.0.1", PERF_PORT)
	doDefaultConnect(b, c)
	sendProto(b, c, "SUB foo 1\r\n")
	bw := bufio.NewWriterSize(c, defaultSendBufSize)
	sendOp := []byte("PUB foo 2\r\nok\r\n")
	ch := make(chan bool)
	expected := len("MSG foo 1 2\r\nok\r\n") * b.N
	go drainConnection(b, c, ch, expected)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := bw.Write(sendOp)
		if err != nil {
			b.Errorf("Received error on PUB write: %v\n", err)
		}
	}
	err := bw.Flush()
	if err != nil {
		b.Errorf("Received error on FLUSH write: %v\n", err)
	}

	// Wait for connection to be drained
	<-ch

	b.StopTimer()
	c.Close()
	s.Shutdown()
}

func Benchmark_____PubSubTwoConns(b *testing.B) {
	b.StopTimer()
	s := runBenchServer()
	c := createClientConn(b, "127.0.0.1", PERF_PORT)
	doDefaultConnect(b, c)
	bw := bufio.NewWriterSize(c, defaultSendBufSize)

	c2 := createClientConn(b, "127.0.0.1", PERF_PORT)
	doDefaultConnect(b, c2)
	sendProto(b, c2, "SUB foo 1\r\n")
	flushConnection(b, c2)

	sendOp := []byte("PUB foo 2\r\nok\r\n")
	ch := make(chan bool)

	expected := len("MSG foo 1 2\r\nok\r\n") * b.N
	go drainConnection(b, c2, ch, expected)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bw.Write(sendOp)
	}
	err := bw.Flush()
	if err != nil {
		b.Errorf("Received error on FLUSH write: %v\n", err)
	}

	// Wait for connection to be drained
	<-ch

	b.StopTimer()
	c.Close()
	c2.Close()
	s.Shutdown()
}

func benchDefaultOptionsForAccounts() *server.Options {
	o := DefaultTestOptions
	o.Host = "127.0.0.1"
	o.Port = -1
	o.Cluster.Host = o.Host
	o.Cluster.Port = -1
	o.DisableShortFirstPing = true
	fooAcc := server.NewAccount("$foo")
	fooAcc.AddStreamExport("foo", nil)
	barAcc := server.NewAccount("$bar")
	barAcc.AddStreamImport(fooAcc, "foo", "")
	o.Accounts = []*server.Account{fooAcc, barAcc}

	return &o
}

func createClientWithAccount(b *testing.B, account, host string, port int) net.Conn {
	c := createClientConn(b, host, port)
	checkInfoMsg(b, c)
	cs := fmt.Sprintf("CONNECT {\"verbose\":%v,\"pedantic\":%v,\"tls_required\":%v,\"account\":%q}\r\n", false, false, false, account)
	sendProto(b, c, cs)
	return c
}

func benchOptionsForServiceImports() *server.Options {
	o := DefaultTestOptions
	o.Host = "127.0.0.1"
	o.Port = -1
	o.DisableShortFirstPing = true
	foo := server.NewAccount("$foo")
	bar := server.NewAccount("$bar")
	o.Accounts = []*server.Account{foo, bar}

	return &o
}

func addServiceImports(b *testing.B, s *server.Server) {
	// Add a bunch of service exports with wildcards, similar to JS.
	var exports = []string{
		server.JSApiAccountInfo,
		server.JSApiTemplateCreate,
		server.JSApiTemplates,
		server.JSApiTemplateInfo,
		server.JSApiTemplateDelete,
		server.JSApiStreamCreate,
		server.JSApiStreamUpdate,
		server.JSApiStreams,
		server.JSApiStreamInfo,
		server.JSApiStreamDelete,
		server.JSApiStreamPurge,
		server.JSApiMsgDelete,
		server.JSApiConsumerCreate,
		server.JSApiConsumers,
		server.JSApiConsumerInfo,
		server.JSApiConsumerDelete,
	}
	foo, _ := s.LookupAccount("$foo")
	bar, _ := s.LookupAccount("$bar")

	for _, export := range exports {
		if err := bar.AddServiceExport(export, nil); err != nil {
			b.Fatalf("Could not add service export: %v", err)
		}
		if err := foo.AddServiceImport(bar, export, ""); err != nil {
			b.Fatalf("Could not add service import: %v", err)
		}
	}
}

func Benchmark__PubServiceImports(b *testing.B) {
	o := benchOptionsForServiceImports()
	s := RunServer(o)
	defer s.Shutdown()

	addServiceImports(b, s)

	c := createClientWithAccount(b, "$foo", o.Host, o.Port)
	defer c.Close()

	bw := bufio.NewWriterSize(c, defaultSendBufSize)
	sendOp := []byte("PUB foo 2\r\nok\r\n")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bw.Write(sendOp)
	}
	err := bw.Flush()
	if err != nil {
		b.Errorf("Received error on FLUSH write: %v\n", err)
	}
	b.StopTimer()
}

func Benchmark___PubSubAccsImport(b *testing.B) {
	o := benchDefaultOptionsForAccounts()
	s := RunServer(o)
	defer s.Shutdown()

	pub := createClientWithAccount(b, "$foo", o.Host, o.Port)
	defer pub.Close()

	sub := createClientWithAccount(b, "$bar", o.Host, o.Port)
	defer sub.Close()

	sendProto(b, sub, "SUB foo 1\r\n")
	flushConnection(b, sub)
	ch := make(chan bool)
	expected := len("MSG foo 1 2\r\nok\r\n") * b.N
	go drainConnection(b, sub, ch, expected)

	bw := bufio.NewWriterSize(pub, defaultSendBufSize)
	sendOp := []byte("PUB foo 2\r\nok\r\n")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bw.Write(sendOp)
	}
	err := bw.Flush()
	if err != nil {
		b.Errorf("Received error on FLUSH write: %v\n", err)
	}

	// Wait for connection to be drained
	<-ch
	b.StopTimer()
}

func Benchmark_____PubTwoQueueSub(b *testing.B) {
	b.StopTimer()
	s := runBenchServer()
	c := createClientConn(b, "127.0.0.1", PERF_PORT)
	doDefaultConnect(b, c)
	sendProto(b, c, "SUB foo group1 1\r\n")
	sendProto(b, c, "SUB foo group1 2\r\n")
	bw := bufio.NewWriterSize(c, defaultSendBufSize)
	sendOp := []byte("PUB foo 2\r\nok\r\n")
	ch := make(chan bool)
	expected := len("MSG foo 1 2\r\nok\r\n") * b.N
	go drainConnection(b, c, ch, expected)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := bw.Write(sendOp)
		if err != nil {
			b.Fatalf("Received error on PUB write: %v\n", err)
		}
	}
	err := bw.Flush()
	if err != nil {
		b.Fatalf("Received error on FLUSH write: %v\n", err)
	}

	// Wait for connection to be drained
	<-ch

	b.StopTimer()
	c.Close()
	s.Shutdown()
}

func Benchmark____PubFourQueueSub(b *testing.B) {
	b.StopTimer()
	s := runBenchServer()
	c := createClientConn(b, "127.0.0.1", PERF_PORT)
	doDefaultConnect(b, c)
	sendProto(b, c, "SUB foo group1 1\r\n")
	sendProto(b, c, "SUB foo group1 2\r\n")
	sendProto(b, c, "SUB foo group1 3\r\n")
	sendProto(b, c, "SUB foo group1 4\r\n")
	bw := bufio.NewWriterSize(c, defaultSendBufSize)
	sendOp := []byte("PUB foo 2\r\nok\r\n")
	ch := make(chan bool)
	expected := len("MSG foo 1 2\r\nok\r\n") * b.N
	go drainConnection(b, c, ch, expected)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := bw.Write(sendOp)
		if err != nil {
			b.Fatalf("Received error on PUB write: %v\n", err)
		}
	}
	err := bw.Flush()
	if err != nil {
		b.Fatalf("Received error on FLUSH write: %v\n", err)
	}

	// Wait for connection to be drained
	<-ch

	b.StopTimer()
	c.Close()
	s.Shutdown()
}

func Benchmark___PubEightQueueSub(b *testing.B) {
	b.StopTimer()
	s := runBenchServer()
	c := createClientConn(b, "127.0.0.1", PERF_PORT)
	doDefaultConnect(b, c)
	sendProto(b, c, "SUB foo group1 1\r\n")
	sendProto(b, c, "SUB foo group1 2\r\n")
	sendProto(b, c, "SUB foo group1 3\r\n")
	sendProto(b, c, "SUB foo group1 4\r\n")
	sendProto(b, c, "SUB foo group1 5\r\n")
	sendProto(b, c, "SUB foo group1 6\r\n")
	sendProto(b, c, "SUB foo group1 7\r\n")
	sendProto(b, c, "SUB foo group1 8\r\n")
	bw := bufio.NewWriterSize(c, defaultSendBufSize)
	sendOp := []byte("PUB foo 2\r\nok\r\n")
	ch := make(chan bool)
	expected := len("MSG foo 1 2\r\nok\r\n") * b.N
	go drainConnection(b, c, ch, expected)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := bw.Write(sendOp)
		if err != nil {
			b.Fatalf("Received error on PUB write: %v\n", err)
		}
	}
	err := bw.Flush()
	if err != nil {
		b.Fatalf("Received error on FLUSH write: %v\n", err)
	}

	// Wait for connection to be drained
	<-ch

	b.StopTimer()
	c.Close()
	s.Shutdown()
}

func Benchmark_PubSub512kTwoConns(b *testing.B) {
	b.StopTimer()
	s := runBenchServer()
	c := createClientConn(b, "127.0.0.1", PERF_PORT)
	doDefaultConnect(b, c)
	bw := bufio.NewWriterSize(c, defaultSendBufSize)

	c2 := createClientConn(b, "127.0.0.1", PERF_PORT)
	doDefaultConnect(b, c2)
	sendProto(b, c2, "SUB foo 1\r\n")
	flushConnection(b, c2)

	sz := 1024 * 512
	payload := sizedString(sz)

	sendOp := []byte(fmt.Sprintf("PUB foo %d\r\n%s\r\n", sz, payload))
	ch := make(chan bool)

	expected := len(fmt.Sprintf("MSG foo 1 %d\r\n%s\r\n", sz, payload)) * b.N
	go drainConnection(b, c2, ch, expected)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bw.Write(sendOp)
	}
	err := bw.Flush()
	if err != nil {
		b.Errorf("Received error on FLUSH write: %v\n", err)
	}

	// Wait for connection to be drained
	<-ch

	b.StopTimer()
	c.Close()
	c2.Close()
	s.Shutdown()
}

func Benchmark__DenyMsgNoWCPubSub(b *testing.B) {
	s, opts := RunServerWithConfig("./configs/authorization.conf")
	opts.DisableShortFirstPing = true
	defer s.Shutdown()

	c := createClientConn(b, opts.Host, opts.Port)
	defer c.Close()

	expectAuthRequired(b, c)
	cs := fmt.Sprintf("CONNECT {\"verbose\":false,\"pedantic\":false,\"user\":\"%s\",\"pass\":\"%s\"}\r\n", "bench-deny", DefaultPass)
	sendProto(b, c, cs)

	sendProto(b, c, "SUB foo 1\r\n")
	bw := bufio.NewWriterSize(c, defaultSendBufSize)
	sendOp := []byte("PUB foo 2\r\nok\r\n")
	ch := make(chan bool)
	expected := len("MSG foo 1 2\r\nok\r\n") * b.N
	go drainConnection(b, c, ch, expected)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := bw.Write(sendOp)
		if err != nil {
			b.Errorf("Received error on PUB write: %v\n", err)
		}
	}
	err := bw.Flush()
	if err != nil {
		b.Errorf("Received error on FLUSH write: %v\n", err)
	}

	// Wait for connection to be drained
	<-ch

	// To not count defer cleanup of client and server.
	b.StopTimer()
}

func Benchmark_DenyMsgYesWCPubSub(b *testing.B) {
	s, opts := RunServerWithConfig("./configs/authorization.conf")
	opts.DisableShortFirstPing = true
	defer s.Shutdown()

	c := createClientConn(b, opts.Host, opts.Port)
	defer c.Close()

	expectAuthRequired(b, c)
	cs := fmt.Sprintf("CONNECT {\"verbose\":false,\"pedantic\":false,\"user\":\"%s\",\"pass\":\"%s\"}\r\n", "bench-deny", DefaultPass)
	sendProto(b, c, cs)

	sendProto(b, c, "SUB * 1\r\n")
	bw := bufio.NewWriterSize(c, defaultSendBufSize)
	sendOp := []byte("PUB foo 2\r\nok\r\n")
	ch := make(chan bool)
	expected := len("MSG foo 1 2\r\nok\r\n") * b.N
	go drainConnection(b, c, ch, expected)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := bw.Write(sendOp)
		if err != nil {
			b.Errorf("Received error on PUB write: %v\n", err)
		}
	}
	err := bw.Flush()
	if err != nil {
		b.Errorf("Received error on FLUSH write: %v\n", err)
	}

	// Wait for connection to be drained
	<-ch

	// To not count defer cleanup of client and server.
	b.StopTimer()
}

func routePubSub(b *testing.B, size int) {
	b.StopTimer()

	s1, o1 := RunServerWithConfig("./configs/srv_a.conf")
	o1.DisableShortFirstPing = true
	defer s1.Shutdown()
	s2, o2 := RunServerWithConfig("./configs/srv_b.conf")
	o2.DisableShortFirstPing = true
	defer s2.Shutdown()

	sub := createClientConn(b, o1.Host, o1.Port)
	doDefaultConnect(b, sub)
	sendProto(b, sub, "SUB foo 1\r\n")
	flushConnection(b, sub)

	payload := sizedString(size)

	pub := createClientConn(b, o2.Host, o2.Port)
	doDefaultConnect(b, pub)
	bw := bufio.NewWriterSize(pub, defaultSendBufSize)

	ch := make(chan bool)
	sendOp := []byte(fmt.Sprintf("PUB foo %d\r\n%s\r\n", len(payload), payload))
	expected := len(fmt.Sprintf("MSG foo 1 %d\r\n%s\r\n", len(payload), payload)) * b.N
	go drainConnection(b, sub, ch, expected)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := bw.Write(sendOp)
		if err != nil {
			b.Fatalf("Received error on PUB write: %v\n", err)
		}

	}
	err := bw.Flush()
	if err != nil {
		b.Errorf("Received error on FLUSH write: %v\n", err)
	}

	// Wait for connection to be drained
	<-ch

	b.StopTimer()
	pub.Close()
	sub.Close()
}

func Benchmark____RoutedPubSub_0b(b *testing.B) {
	routePubSub(b, 2)
}

func Benchmark____RoutedPubSub_1K(b *testing.B) {
	routePubSub(b, 1024)
}

func Benchmark__RoutedPubSub_100K(b *testing.B) {
	routePubSub(b, 100*1024)
}

func routeQueue(b *testing.B, numQueueSubs, size int) {
	s1, o1 := RunServerWithConfig("./configs/srv_a.conf")
	o1.DisableShortFirstPing = true
	defer s1.Shutdown()
	s2, o2 := RunServerWithConfig("./configs/srv_b.conf")
	o2.DisableShortFirstPing = true
	defer s2.Shutdown()

	sub := createClientConn(b, o1.Host, o1.Port)
	defer sub.Close()
	doDefaultConnect(b, sub)
	for i := 0; i < numQueueSubs; i++ {
		sendProto(b, sub, fmt.Sprintf("SUB foo bar %d\r\n", 100+i))
	}
	flushConnection(b, sub)

	payload := sizedString(size)

	pub := createClientConn(b, o2.Host, o2.Port)
	defer pub.Close()
	doDefaultConnect(b, pub)
	bw := bufio.NewWriterSize(pub, defaultSendBufSize)

	ch := make(chan bool)
	sendOp := []byte(fmt.Sprintf("PUB foo %d\r\n%s\r\n", len(payload), payload))
	expected := len(fmt.Sprintf("MSG foo 100 %d\r\n%s\r\n", len(payload), payload)) * b.N
	go drainConnection(b, sub, ch, expected)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := bw.Write(sendOp)
		if err != nil {
			b.Fatalf("Received error on PUB write: %v\n", err)
		}

	}
	err := bw.Flush()
	if err != nil {
		b.Errorf("Received error on FLUSH write: %v\n", err)
	}

	// Wait for connection to be drained
	<-ch

	b.StopTimer()
}

func Benchmark____Routed2QueueSub(b *testing.B) {
	routeQueue(b, 2, 2)
}

func Benchmark____Routed4QueueSub(b *testing.B) {
	routeQueue(b, 4, 2)
}

func Benchmark____Routed8QueueSub(b *testing.B) {
	routeQueue(b, 8, 2)
}

func Benchmark___Routed16QueueSub(b *testing.B) {
	routeQueue(b, 16, 2)
}

func doFanout(b *testing.B, numServers, numConnections, subsPerConnection int, subject, payload string) {
	var s1, s2 *server.Server
	var o1, o2 *server.Options

	switch numServers {
	case 1:
		s1, o1 = RunServerWithConfig("./configs/srv_a.conf")
		o1.DisableShortFirstPing = true
		defer s1.Shutdown()
		s2, o2 = s1, o1
	case 2:
		s1, o1 = RunServerWithConfig("./configs/srv_a.conf")
		o1.DisableShortFirstPing = true
		defer s1.Shutdown()
		s2, o2 = RunServerWithConfig("./configs/srv_b.conf")
		o2.DisableShortFirstPing = true
		defer s2.Shutdown()
	default:
		b.Fatalf("%d servers not supported for this test\n", numServers)
	}

	// To get a consistent length sid in MSG sent to us for drainConnection.
	var sidFloor int
	switch {
	case subsPerConnection <= 100:
		sidFloor = 100
	case subsPerConnection <= 1000:
		sidFloor = 1000
	case subsPerConnection <= 10000:
		sidFloor = 10000
	default:
		b.Fatalf("Unsupported SubsPerConnection argument of %d\n", subsPerConnection)
	}

	msgOp := fmt.Sprintf("MSG %s %d %d\r\n%s\r\n", subject, sidFloor, len(payload), payload)
	expected := len(msgOp) * subsPerConnection * b.N

	// Client connections and subscriptions.
	clients := make([]chan bool, 0, numConnections)
	for i := 0; i < numConnections; i++ {
		c := createClientConn(b, o2.Host, o2.Port)
		doDefaultConnect(b, c)
		defer c.Close()

		ch := make(chan bool)
		clients = append(clients, ch)

		for s := 0; s < subsPerConnection; s++ {
			subOp := fmt.Sprintf("SUB %s %d\r\n", subject, sidFloor+s)
			sendProto(b, c, subOp)
		}
		flushConnection(b, c)
		go drainConnection(b, c, ch, expected)
	}
	// Publish Connection
	c := createClientConn(b, o1.Host, o1.Port)
	doDefaultConnect(b, c)
	flushConnection(b, c)

	bw := bufio.NewWriterSize(c, defaultSendBufSize)
	sendOp := []byte(fmt.Sprintf("PUB %s %d\r\n%s\r\n", subject, len(payload), payload))

	b.SetBytes(int64(len(sendOp) + (len(msgOp) * numConnections * subsPerConnection)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := bw.Write(sendOp)
		if err != nil {
			b.Fatalf("Received error on PUB write: %v\n", err)
		}
	}
	err := bw.Flush()
	if err != nil {
		b.Fatalf("Received error on FLUSH write: %v\n", err)
	}

	// Wait for connections to be drained
	for i := 0; i < numConnections; i++ {
		<-clients[i]
	}
	b.StopTimer()
}

var sub = "x"
var payload = "12345678"

func Benchmark______FanOut_8x1x10(b *testing.B) {
	doFanout(b, 1, 1, 10, sub, payload)
}

func Benchmark_____FanOut_8x1x100(b *testing.B) {
	doFanout(b, 1, 1, 100, sub, payload)
}

func Benchmark____FanOut_8x10x100(b *testing.B) {
	doFanout(b, 1, 10, 100, sub, payload)
}

func Benchmark___FanOut_8x10x1000(b *testing.B) {
	doFanout(b, 1, 10, 1000, sub, payload)
}

func Benchmark___FanOut_8x100x100(b *testing.B) {
	doFanout(b, 1, 100, 100, sub, payload)
}

func Benchmark__FanOut_8x100x1000(b *testing.B) {
	doFanout(b, 1, 100, 1000, sub, payload)
}

func Benchmark__FanOut_8x10x10000(b *testing.B) {
	doFanout(b, 1, 10, 10000, sub, payload)
}

func Benchmark___FanOut_8x500x100(b *testing.B) {
	doFanout(b, 1, 500, 100, sub, payload)
}

func Benchmark___FanOut_128x1x100(b *testing.B) {
	doFanout(b, 1, 1, 100, sub, sizedString(128))
}

func Benchmark__FanOut_128x10x100(b *testing.B) {
	doFanout(b, 1, 10, 100, sub, sizedString(128))
}

func Benchmark_FanOut_128x10x1000(b *testing.B) {
	doFanout(b, 1, 10, 1000, sub, sizedString(128))
}

func Benchmark_FanOut_128x100x100(b *testing.B) {
	doFanout(b, 1, 100, 100, sub, sizedString(128))
}

func BenchmarkFanOut_128x100x1000(b *testing.B) {
	doFanout(b, 1, 100, 1000, sub, sizedString(128))
}

func BenchmarkFanOut_128x10x10000(b *testing.B) {
	doFanout(b, 1, 10, 10000, sub, sizedString(128))
}

func BenchmarkFanOut__128x500x100(b *testing.B) {
	doFanout(b, 1, 500, 100, sub, sizedString(128))
}

func Benchmark_FanOut_512x100x100(b *testing.B) {
	doFanout(b, 1, 100, 100, sub, sizedString(512))
}

func Benchmark__FanOut_512x100x1k(b *testing.B) {
	doFanout(b, 1, 100, 1000, sub, sizedString(512))
}

func Benchmark____FanOut_1kx10x1k(b *testing.B) {
	doFanout(b, 1, 10, 1000, sub, sizedString(1024))
}

func Benchmark__FanOut_1kx100x100(b *testing.B) {
	doFanout(b, 1, 100, 100, sub, sizedString(1024))
}

func Benchmark_____RFanOut_8x1x10(b *testing.B) {
	doFanout(b, 2, 1, 10, sub, payload)
}

func Benchmark____RFanOut_8x1x100(b *testing.B) {
	doFanout(b, 2, 1, 100, sub, payload)
}

func Benchmark___RFanOut_8x10x100(b *testing.B) {
	doFanout(b, 2, 10, 100, sub, payload)
}

func Benchmark__RFanOut_8x10x1000(b *testing.B) {
	doFanout(b, 2, 10, 1000, sub, payload)
}

func Benchmark__RFanOut_8x100x100(b *testing.B) {
	doFanout(b, 2, 100, 100, sub, payload)
}

func Benchmark_RFanOut_8x100x1000(b *testing.B) {
	doFanout(b, 2, 100, 1000, sub, payload)
}

func Benchmark_RFanOut_8x10x10000(b *testing.B) {
	doFanout(b, 2, 10, 10000, sub, payload)
}

func Benchmark_RFanOut_1kx10x1000(b *testing.B) {
	doFanout(b, 2, 10, 1000, sub, sizedString(1024))
}

func doFanIn(b *testing.B, numServers, numPublishers, numSubscribers int, subject, payload string) {
	b.Helper()
	if b.N < numPublishers {
		return
	}
	// Don't check for number of subscribers being lower than the number of publishers.
	// We also use this bench to show the performance impact of increased number of publishers,
	// and for those tests, the number of publishers will start at 1 and increase to 10,
	// while the number of subscribers will always be 3.
	if numSubscribers > 10 {
		b.Fatalf("numSubscribers should be <= 10")
	}

	var s1, s2 *server.Server
	var o1, o2 *server.Options

	switch numServers {
	case 1:
		s1, o1 = RunServerWithConfig("./configs/srv_a.conf")
		o1.DisableShortFirstPing = true
		defer s1.Shutdown()
		s2, o2 = s1, o1
	case 2:
		s1, o1 = RunServerWithConfig("./configs/srv_a.conf")
		o1.DisableShortFirstPing = true
		defer s1.Shutdown()
		s2, o2 = RunServerWithConfig("./configs/srv_b.conf")
		o2.DisableShortFirstPing = true
		defer s2.Shutdown()
	default:
		b.Fatalf("%d servers not supported for this test\n", numServers)
	}

	msgOp := fmt.Sprintf("MSG %s %d %d\r\n%s\r\n", subject, 9, len(payload), payload)
	l := b.N / numPublishers
	expected := len(msgOp) * l * numPublishers

	// Client connections and subscriptions. For fan in these are smaller then numPublishers.
	clients := make([]chan bool, 0, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		c := createClientConn(b, o2.Host, o2.Port)
		doDefaultConnect(b, c)
		defer c.Close()

		ch := make(chan bool)
		clients = append(clients, ch)

		subOp := fmt.Sprintf("SUB %s %d\r\n", subject, i)
		sendProto(b, c, subOp)
		flushConnection(b, c)
		go drainConnection(b, c, ch, expected)
	}

	sendOp := []byte(fmt.Sprintf("PUB %s %d\r\n%s\r\n", subject, len(payload), payload))
	startCh := make(chan bool)

	pubLoop := func(c net.Conn, ch chan bool) {
		bw := bufio.NewWriterSize(c, defaultSendBufSize)

		// Signal we are ready
		close(ch)

		// Wait to start up actual sends.
		<-startCh

		for i := 0; i < l; i++ {
			_, err := bw.Write(sendOp)
			if err != nil {
				b.Errorf("Received error on PUB write: %v\n", err)
				return
			}
		}
		err := bw.Flush()
		if err != nil {
			b.Errorf("Received error on FLUSH write: %v\n", err)
			return
		}
	}

	// Publish Connections SPINUP
	for i := 0; i < numPublishers; i++ {
		c := createClientConn(b, o1.Host, o1.Port)
		doDefaultConnect(b, c)
		flushConnection(b, c)
		ch := make(chan bool)

		go pubLoop(c, ch)
		<-ch
	}

	b.SetBytes(int64(len(sendOp) + len(msgOp)))
	b.ResetTimer()

	// Closing this will start all publishers at once (roughly)
	close(startCh)

	// Wait for connections to be drained
	for i := 0; i < len(clients); i++ {
		<-clients[i]
	}
	b.StopTimer()
}

func Benchmark_____FanIn_1kx100x1(b *testing.B) {
	doFanIn(b, 1, 100, 1, sub, sizedString(1024))
}

func Benchmark_____FanIn_4kx100x1(b *testing.B) {
	doFanIn(b, 1, 100, 1, sub, sizedString(4096))
}

func Benchmark_____FanIn_8kx100x1(b *testing.B) {
	doFanIn(b, 1, 100, 1, sub, sizedString(8192))
}

func Benchmark____FanIn_16kx100x1(b *testing.B) {
	doFanIn(b, 1, 100, 1, sub, sizedString(16384))
}

func Benchmark____FanIn_64kx100x1(b *testing.B) {
	doFanIn(b, 1, 100, 1, sub, sizedString(65536))
}

func Benchmark___FanIn_128kx100x1(b *testing.B) {
	doFanIn(b, 1, 100, 1, sub, sizedString(65536*2))
}

func Benchmark___BumpPubCount_1x3(b *testing.B) {
	doFanIn(b, 1, 1, 3, sub, sizedString(128))
}

func Benchmark___BumpPubCount_2x3(b *testing.B) {
	doFanIn(b, 1, 2, 3, sub, sizedString(128))
}

func Benchmark___BumpPubCount_5x3(b *testing.B) {
	doFanIn(b, 1, 5, 3, sub, sizedString(128))
}

func Benchmark__BumpPubCount_10x3(b *testing.B) {
	doFanIn(b, 1, 10, 3, sub, sizedString(128))
}

func testDefaultBenchOptionsForGateway(name string) *server.Options {
	opts := testDefaultOptionsForGateway(name)
	opts.DisableShortFirstPing = true
	return opts
}

func gatewaysBench(b *testing.B, optimisticMode bool, payload string, numPublishers int, subInterest bool) {
	b.Helper()
	if b.N < numPublishers {
		return
	}

	ob := testDefaultBenchOptionsForGateway("B")
	sb := RunServer(ob)
	defer sb.Shutdown()

	server.SetGatewaysSolicitDelay(10 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	gwbURL, err := url.Parse(fmt.Sprintf("nats://%s:%d", ob.Gateway.Host, ob.Gateway.Port))
	if err != nil {
		b.Fatalf("Error parsing url: %v", err)
	}
	oa := testDefaultBenchOptionsForGateway("A")
	oa.Gateway.Gateways = []*server.RemoteGatewayOpts{
		{
			Name: "B",
			URLs: []*url.URL{gwbURL},
		},
	}
	sa := RunServer(oa)
	defer sa.Shutdown()

	sub := createClientConn(b, ob.Host, ob.Port)
	defer sub.Close()
	doDefaultConnect(b, sub)
	sendProto(b, sub, "SUB end.test 1\r\n")
	if subInterest {
		sendProto(b, sub, "SUB foo 2\r\n")
	}
	flushConnection(b, sub)

	// If not optimisticMode, make B switch GW connection
	// to interest mode only
	if !optimisticMode {
		pub := createClientConn(b, oa.Host, oa.Port)
		doDefaultConnect(b, pub)
		// has to be more that defaultGatewayMaxRUnsubBeforeSwitch
		for i := 0; i < 2000; i++ {
			sendProto(b, pub, fmt.Sprintf("PUB reject.me.%d 2\r\nok\r\n", i+1))
		}
		flushConnection(b, pub)
		pub.Close()
	}

	ch := make(chan bool)
	var msgOp string
	var expected int
	l := b.N / numPublishers
	if subInterest {
		msgOp = fmt.Sprintf("MSG foo 2 %d\r\n%s\r\n", len(payload), payload)
		expected = len(msgOp) * l * numPublishers
	}
	// Last message sent to end.test
	lastMsg := "MSG end.test 1 2\r\nok\r\n"
	expected += len(lastMsg) * numPublishers
	go drainConnection(b, sub, ch, expected)

	sendOp := []byte(fmt.Sprintf("PUB foo %d\r\n%s\r\n", len(payload), payload))
	startCh := make(chan bool)

	lastMsgSendOp := []byte("PUB end.test 2\r\nok\r\n")

	pubLoop := func(c net.Conn, ch chan bool) {
		bw := bufio.NewWriterSize(c, defaultSendBufSize)

		// Signal we are ready
		close(ch)

		// Wait to start up actual sends.
		<-startCh

		for i := 0; i < l; i++ {
			if _, err := bw.Write(sendOp); err != nil {
				b.Errorf("Received error on PUB write: %v\n", err)
				return
			}
		}
		if _, err := bw.Write(lastMsgSendOp); err != nil {
			b.Errorf("Received error on PUB write: %v\n", err)
			return
		}
		if err := bw.Flush(); err != nil {
			b.Errorf("Received error on FLUSH write: %v\n", err)
			return
		}
		flushConnection(b, c)
	}

	// Publish Connections SPINUP
	for i := 0; i < numPublishers; i++ {
		c := createClientConn(b, oa.Host, oa.Port)
		defer c.Close()
		doDefaultConnect(b, c)
		flushConnection(b, c)
		ch := make(chan bool)

		go pubLoop(c, ch)
		<-ch
	}

	// To report the number of bytes:
	// from publisher to server on cluster A:
	numBytes := len(sendOp)
	if subInterest {
		// from server in cluster A to server on cluster B:
		// RMSG $G foo <payload size> <payload>\r\n
		numBytes += len("RMSG $G foo xxxx ") + len(payload) + 2

		// From server in cluster B to sub:
		numBytes += len(msgOp)
	}
	b.SetBytes(int64(numBytes))
	b.ResetTimer()

	// Closing this will start all publishers at once (roughly)
	close(startCh)

	// Wait for end of test
	<-ch

	b.StopTimer()
}

func Benchmark____GWs_Opt_1kx01x0(b *testing.B) {
	gatewaysBench(b, true, sizedString(1024), 1, false)
}

func Benchmark____GWs_Opt_2kx01x0(b *testing.B) {
	gatewaysBench(b, true, sizedString(2048), 1, false)
}

func Benchmark____GWs_Opt_4kx01x0(b *testing.B) {
	gatewaysBench(b, true, sizedString(4096), 1, false)
}

func Benchmark____GWs_Opt_1kx10x0(b *testing.B) {
	gatewaysBench(b, true, sizedString(1024), 10, false)
}

func Benchmark____GWs_Opt_2kx10x0(b *testing.B) {
	gatewaysBench(b, true, sizedString(2048), 10, false)
}

func Benchmark____GWs_Opt_4kx10x0(b *testing.B) {
	gatewaysBench(b, true, sizedString(4096), 10, false)
}

func Benchmark____GWs_Opt_1kx01x1(b *testing.B) {
	gatewaysBench(b, true, sizedString(1024), 1, true)
}

func Benchmark____GWs_Opt_2kx01x1(b *testing.B) {
	gatewaysBench(b, true, sizedString(2048), 1, true)
}

func Benchmark____GWs_Opt_4kx01x1(b *testing.B) {
	gatewaysBench(b, true, sizedString(4096), 1, true)
}

func Benchmark____GWs_Opt_1kx10x1(b *testing.B) {
	gatewaysBench(b, true, sizedString(1024), 10, true)
}

func Benchmark____GWs_Opt_2kx10x1(b *testing.B) {
	gatewaysBench(b, true, sizedString(2048), 10, true)
}

func Benchmark____GWs_Opt_4kx10x1(b *testing.B) {
	gatewaysBench(b, true, sizedString(4096), 10, true)
}

func Benchmark____GWs_Int_1kx01x0(b *testing.B) {
	gatewaysBench(b, false, sizedString(1024), 1, false)
}

func Benchmark____GWs_Int_2kx01x0(b *testing.B) {
	gatewaysBench(b, false, sizedString(2048), 1, false)
}

func Benchmark____GWs_Int_4kx01x0(b *testing.B) {
	gatewaysBench(b, false, sizedString(4096), 1, false)
}

func Benchmark____GWs_Int_1kx10x0(b *testing.B) {
	gatewaysBench(b, false, sizedString(1024), 10, false)
}

func Benchmark____GWs_Int_2kx10x0(b *testing.B) {
	gatewaysBench(b, false, sizedString(2048), 10, false)
}

func Benchmark____GWs_Int_4kx10x0(b *testing.B) {
	gatewaysBench(b, false, sizedString(4096), 10, false)
}

func Benchmark____GWs_Int_1kx01x1(b *testing.B) {
	gatewaysBench(b, false, sizedString(1024), 1, true)
}

func Benchmark____GWs_Int_2kx01x1(b *testing.B) {
	gatewaysBench(b, false, sizedString(2048), 1, true)
}

func Benchmark____GWs_Int_4kx01x1(b *testing.B) {
	gatewaysBench(b, false, sizedString(4096), 1, true)
}

func Benchmark____GWs_Int_1kx10x1(b *testing.B) {
	gatewaysBench(b, false, sizedString(1024), 10, true)
}

func Benchmark____GWs_Int_2kx10x1(b *testing.B) {
	gatewaysBench(b, false, sizedString(2048), 10, true)
}

func Benchmark____GWs_Int_4kx10x1(b *testing.B) {
	gatewaysBench(b, false, sizedString(4096), 10, true)
}

// This bench only sends the requests to verify impact
// of reply mapping in GW code.
func gatewaySendRequestsBench(b *testing.B, singleReplySub bool) {
	server.SetGatewaysSolicitDelay(10 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	ob := testDefaultBenchOptionsForGateway("B")
	sb := RunServer(ob)
	defer sb.Shutdown()

	gwbURL, err := url.Parse(fmt.Sprintf("nats://%s:%d", ob.Gateway.Host, ob.Gateway.Port))
	if err != nil {
		b.Fatalf("Error parsing url: %v", err)
	}
	oa := testDefaultBenchOptionsForGateway("A")
	oa.Gateway.Gateways = []*server.RemoteGatewayOpts{
		{
			Name: "B",
			URLs: []*url.URL{gwbURL},
		},
	}
	sa := RunServer(oa)
	defer sa.Shutdown()

	sub := createClientConn(b, ob.Host, ob.Port)
	defer sub.Close()
	doDefaultConnect(b, sub)
	sendProto(b, sub, "SUB foo 1\r\n")
	flushConnection(b, sub)

	lenMsg := len("MSG foo reply.xxxxxxxxxx 1 2\r\nok\r\n")
	expected := b.N * lenMsg
	ch := make(chan bool, 1)
	go drainConnection(b, sub, ch, expected)

	c := createClientConn(b, oa.Host, oa.Port)
	defer c.Close()
	doDefaultConnect(b, c)
	flushConnection(b, c)

	// From pub to server in cluster A:
	numBytes := len("PUB foo reply.0123456789 2\r\nok\r\n")
	if !singleReplySub {
		// Add the preceding SUB
		numBytes += len("SUB reply.0123456789 0123456789\r\n")
		// And UNSUB...
		numBytes += len("UNSUB 0123456789\r\n")
	}
	// From server in cluster A to cluster B
	numBytes += len("RMSG $G foo reply.0123456789 2\r\nok\r\n")
	// If mapping of reply...
	if !singleReplySub {
		// the mapping uses about 24 more bytes. So add them
		// for RMSG from server to server.
		numBytes += 24
	}
	// From server in cluster B to sub
	numBytes += lenMsg
	b.SetBytes(int64(numBytes))

	bw := bufio.NewWriterSize(c, defaultSendBufSize)
	var subStr string

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !singleReplySub {
			subStr = fmt.Sprintf("SUB reply.%010d %010d\r\n", i+1, i+1)
		}
		bw.Write([]byte(fmt.Sprintf("%sPUB foo reply.%010d 2\r\nok\r\n", subStr, i+1)))
		// Simulate that we are doing actual request/reply and therefore
		// unsub'ing the subs on the reply subject.
		if !singleReplySub && i > 1000 {
			bw.Write([]byte(fmt.Sprintf("UNSUB %010d\r\n", (i - 1000))))
		}
	}
	bw.Flush()
	flushConnection(b, c)

	<-ch
}

func Benchmark__GWs_Reqs_1_SubAll(b *testing.B) {
	gatewaySendRequestsBench(b, true)
}

func Benchmark__GWs_Reqs_1SubEach(b *testing.B) {
	gatewaySendRequestsBench(b, false)
}
