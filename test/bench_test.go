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

func Benchmark____PubNo_Payload(b *testing.B) {
	benchPub(b, "a", "")
}

func Benchmark____Pub8b_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(8)
	benchPub(b, "a", s)
}

func Benchmark___Pub32b_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(32)
	benchPub(b, "a", s)
}

func Benchmark__Pub256B_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(256)
	benchPub(b, "a", s)
}

func Benchmark____Pub1K_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(1024)
	benchPub(b, "a", s)
}

func Benchmark____Pub4K_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(4 * 1024)
	benchPub(b, "a", s)
}

func Benchmark____Pub8K_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(8 * 1024)
	benchPub(b, "a", s)
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

func Benchmark___________PubSub(b *testing.B) {
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

func Benchmark___PubSubTwoConns(b *testing.B) {
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

func Benchmark___PubTwoQueueSub(b *testing.B) {
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

func Benchmark__PubFourQueueSub(b *testing.B) {
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

func Benchmark_PubEightQueueSub(b *testing.B) {
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
