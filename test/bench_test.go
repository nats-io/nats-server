// Copyright 2012-2015 Apcera Inc. All rights reserved.

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
	c.SetReadDeadline(time.Now().Add(1 * time.Second))
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
	c := createClientConn(b, "localhost", PERF_PORT)
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

func Benchmark_____Pub0b_Payload(b *testing.B) {
	benchPub(b, psub, "")
}

func Benchmark_____Pub8b_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(8)
	benchPub(b, psub, s)
}

func Benchmark____Pub32b_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(32)
	benchPub(b, psub, s)
}

func Benchmark___Pub128B_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(128)
	benchPub(b, psub, s)
}

func Benchmark___Pub256B_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(256)
	benchPub(b, psub, s)
}

func Benchmark_____Pub1K_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(1024)
	benchPub(b, psub, s)
}

func Benchmark_____Pub4K_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(4 * 1024)
	benchPub(b, psub, s)
}

func Benchmark_____Pub8K_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(8 * 1024)
	benchPub(b, psub, s)
}

func drainConnection(b *testing.B, c net.Conn, ch chan bool, expected int) {
	buf := make([]byte, defaultRecBufSize)
	bytes := 0

	for {
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
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
func Benchmark_AuthPub0b_Payload(b *testing.B) {
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

func Benchmark____________PubSub(b *testing.B) {
	b.StopTimer()
	s := runBenchServer()
	c := createClientConn(b, "localhost", PERF_PORT)
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

func Benchmark____PubSubTwoConns(b *testing.B) {
	b.StopTimer()
	s := runBenchServer()
	c := createClientConn(b, "localhost", PERF_PORT)
	doDefaultConnect(b, c)
	bw := bufio.NewWriterSize(c, defaultSendBufSize)

	c2 := createClientConn(b, "localhost", PERF_PORT)
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

func Benchmark____PubTwoQueueSub(b *testing.B) {
	b.StopTimer()
	s := runBenchServer()
	c := createClientConn(b, "localhost", PERF_PORT)
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

func Benchmark___PubFourQueueSub(b *testing.B) {
	b.StopTimer()
	s := runBenchServer()
	c := createClientConn(b, "localhost", PERF_PORT)
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

func Benchmark__PubEightQueueSub(b *testing.B) {
	b.StopTimer()
	s := runBenchServer()
	c := createClientConn(b, "localhost", PERF_PORT)
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

func Benchmark___RoutedPubSub_0b(b *testing.B) {
	routePubSub(b, 2)
}

func Benchmark___RoutedPubSub_1K(b *testing.B) {
	routePubSub(b, 1024)
}

func Benchmark_RoutedPubSub_100K(b *testing.B) {
	routePubSub(b, 100*1024)
}
