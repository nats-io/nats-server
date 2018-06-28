// Copyright 2012-2018 The NATS Authors
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
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/server"
)

const PERF_PORT = 8422

// For Go routine based server.
func runBenchServer() *server.Server {
	opts := DefaultTestOptions
	opts.Port = PERF_PORT
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

func Benchmark______Pub32K_Payload(b *testing.B) {
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
	sendOp := []byte(fmt.Sprintf("PUB foo 2\r\nok\r\n"))
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

	sendOp := []byte(fmt.Sprintf("PUB foo 2\r\nok\r\n"))
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

func Benchmark_____PubTwoQueueSub(b *testing.B) {
	b.StopTimer()
	s := runBenchServer()
	c := createClientConn(b, "127.0.0.1", PERF_PORT)
	doDefaultConnect(b, c)
	sendProto(b, c, "SUB foo group1 1\r\n")
	sendProto(b, c, "SUB foo group1 2\r\n")
	bw := bufio.NewWriterSize(c, defaultSendBufSize)
	sendOp := []byte(fmt.Sprintf("PUB foo 2\r\nok\r\n"))
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
	sendOp := []byte(fmt.Sprintf("PUB foo 2\r\nok\r\n"))
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
	sendOp := []byte(fmt.Sprintf("PUB foo 2\r\nok\r\n"))
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

func routePubSub(b *testing.B, size int) {
	b.StopTimer()

	s1, o1 := RunServerWithConfig("./configs/srv_a.conf")
	defer s1.Shutdown()
	s2, o2 := RunServerWithConfig("./configs/srv_b.conf")
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
	b.StopTimer()

	s1, o1 := RunServerWithConfig("./configs/srv_a.conf")
	defer s1.Shutdown()
	s2, o2 := RunServerWithConfig("./configs/srv_b.conf")
	defer s2.Shutdown()

	sub := createClientConn(b, o1.Host, o1.Port)
	doDefaultConnect(b, sub)
	for i := 0; i < numQueueSubs; i++ {
		sendProto(b, sub, fmt.Sprintf("SUB foo bar %d\r\n", 100+i))
	}
	flushConnection(b, sub)

	payload := sizedString(size)

	pub := createClientConn(b, o2.Host, o2.Port)
	doDefaultConnect(b, pub)
	bw := bufio.NewWriterSize(pub, defaultSendBufSize)

	ch := make(chan bool)
	sendOp := []byte(fmt.Sprintf("PUB foo %d\r\n%s\r\n", len(payload), payload))
	expected := len(fmt.Sprintf("MSG foo 100 %d\r\n%s\r\n", len(payload), payload)) * b.N
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
		defer s1.Shutdown()
		s2, o2 = s1, o1
	case 2:
		s1, o1 = RunServerWithConfig("./configs/srv_a.conf")
		defer s1.Shutdown()
		s2, o2 = RunServerWithConfig("./configs/srv_b.conf")
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
	bw := bufio.NewWriterSize(c, defaultSendBufSize)
	sendOp := []byte(fmt.Sprintf("PUB %s %d\r\n%s\r\n", subject, len(payload), payload))
	flushConnection(b, c)

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

	// Wait for connections to be drained
	for i := 0; i < numConnections; i++ {
		<-clients[i]
	}
	b.StopTimer()
}

var sub = "x"
var payload = "12345678"

func Benchmark___FanOut_512x1kx1k(b *testing.B) {
	doFanout(b, 1, 1000, 1000, sub, sizedString(512))
}

func Benchmark__FanOut_8x1000x100(b *testing.B) {
	doFanout(b, 1, 1000, 100, sub, payload)
}

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

func Benchmark__FanOut_1kx10x1000(b *testing.B) {
	doFanout(b, 1, 10, 1000, sub, sizedString(1024))
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
