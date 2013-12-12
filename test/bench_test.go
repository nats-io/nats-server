// Copyright 2012-2013 Apcera Inc. All rights reserved.

package test

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/apcera/gnatsd/server"
)

const PERF_PORT = 8422

// For Go routine based server.
func runBenchServer() *server.Server {
	opts := DefaultTestOptions
	opts.Port = PERF_PORT
	return RunServer(&opts)
}

const defaultRecBufSize = 32768
const defaultSendBufSize = 16384

func flushConnection(b *testing.B, c net.Conn, buf []byte) {
	c.Write([]byte("PING\r\n"))
	c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	n, err := c.Read(buf)
	c.SetReadDeadline(time.Time{})
	if err != nil {
		b.Fatalf("Failed read: %v\n", err)
	}
	if n != 6 && buf[0] != 'P' {
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
	buf := make([]byte, 1024)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bw.Write(sendOp)
	}
	bw.Flush()
	flushConnection(b, c, buf)
	b.StopTimer()
	c.Close()
	s.Shutdown()
}

func sizedString(sz int) string {
	u := make([]byte, sz)
	io.ReadFull(rand.Reader, u)
	return hex.EncodeToString(u)
}

func Benchmark___PubNo_Payload(b *testing.B) {
	benchPub(b, "a", "")
}

func Benchmark___Pub8b_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(8)
	benchPub(b, "a", s)
}

func Benchmark__Pub32b_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(32)
	benchPub(b, "a", s)
}

func Benchmark_Pub256B_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(256)
	benchPub(b, "a", s)
}

func Benchmark___Pub1K_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(1024)
	benchPub(b, "a", s)
}

func Benchmark___Pub4K_Payload(b *testing.B) {
	b.StopTimer()
	s := sizedString(4 * 1024)
	benchPub(b, "a", s)
}

func drainConnection(b *testing.B, c net.Conn, ch chan bool, expected int) {
	buf := make([]byte, defaultRecBufSize)
	bytes := 0

	for {
		c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
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

func Benchmark__________PubSub(b *testing.B) {
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

func Benchmark__PubSubTwoConns(b *testing.B) {
	b.StopTimer()
	s := runBenchServer()
	c := createClientConn(b, "localhost", PERF_PORT)
	doDefaultConnect(b, c)
	bw := bufio.NewWriterSize(c, defaultSendBufSize)

	c2 := createClientConn(b, "localhost", PERF_PORT)
	doDefaultConnect(b, c2)
	sendProto(b, c2, "SUB foo 1\r\n")

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

func Benchmark__PubTwoQueueSub(b *testing.B) {
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

func Benchmark_PubFourQueueSub(b *testing.B) {
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
