package logger

import (
	"log"
	"net"
	"strings"
	"testing"
	"time"
)

var serverAddr string

func TestSysLogger(t *testing.T) {
	logger := NewSysLogger(false, false)

	if logger.debug != false {
		t.Fatalf("Expected %b, received %b\n", false, logger.debug)
	}

	if logger.trace != false {
		t.Fatalf("Expected %b, received %b\n", false, logger.trace)
	}
}

func TestSysLoggerWithDebugAndTrace(t *testing.T) {
	logger := NewSysLogger(true, true)

	if logger.debug != true {
		t.Fatalf("Expected %b, received %b\n", true, logger.debug)
	}

	if logger.trace != true {
		t.Fatalf("Expected %b, received %b\n", true, logger.trace)
	}
}

func TestRemoteSysLogger(t *testing.T) {
	done := make(chan string)
	startServer(done)
	logger := NewRemoteSysLogger("udp", serverAddr, true, true)

	if logger.debug != true {
		t.Fatalf("Expected %b, received %b\n", true, logger.debug)
	}

	if logger.trace != true {
		t.Fatalf("Expected %b, received %b\n", true, logger.trace)
	}
}

func TestRemoteSysLoggerLog(t *testing.T) {
	done := make(chan string)
	startServer(done)
	logger := NewRemoteSysLogger("udp", serverAddr, true, true)

	logger.Log("foo %s", "bar")
	expectSyslogOutput(t, <-done, "foo bar\n")
}

func TestRemoteSysLoggerDebug(t *testing.T) {
	done := make(chan string)
	startServer(done)
	logger := NewRemoteSysLogger("udp", serverAddr, true, true)

	logger.Debug("foo %s", "qux")
	expectSyslogOutput(t, <-done, "foo qux\n")
}

func TestRemoteSysLoggerDebugDisabled(t *testing.T) {
	done := make(chan string)
	startServer(done)
	logger := NewRemoteSysLogger("udp", serverAddr, false, false)

	logger.Debug("foo %s", "qux")
	rcvd := <-done
	if rcvd != "" {
		t.Fatalf("Unexpected syslog response %s\n", rcvd)
	}
}

func TestRemoteSysLoggerTrace(t *testing.T) {
	done := make(chan string)
	startServer(done)
	logger := NewRemoteSysLogger("udp", serverAddr, true, true)

	logger.Trace("foo %s", "qux")
	expectSyslogOutput(t, <-done, "foo qux\n")
}

func TestRemoteSysLoggerTraceDisabled(t *testing.T) {
	done := make(chan string)
	startServer(done)
	logger := NewRemoteSysLogger("udp", serverAddr, true, false)

	logger.Trace("foo %s", "qux")
	rcvd := <-done
	if rcvd != "" {
		t.Fatalf("Unexpected syslog response %s\n", rcvd)
	}
}

func expectSyslogOutput(t *testing.T, line string, expected string) {
	data := strings.Split(line, "]: ")
	if len(data) != 2 {
		t.Fatalf("Unexpected syslog line %s\n", line)
	}

	if data[1] != expected {
		t.Fatalf("Expected '%s', received '%s'\n", expected, data[1])
	}
}

func runSyslog(c net.PacketConn, done chan<- string) {
	var buf [4096]byte
	var rcvd string = ""
	for {
		n, _, err := c.ReadFrom(buf[:])
		if err != nil || n == 0 {
			break
		}
		rcvd += string(buf[:n])
	}
	done <- rcvd
}

func startServer(done chan<- string) {
	c, e := net.ListenPacket("udp", "127.0.0.1:0")
	if e != nil {
		log.Fatalf("net.ListenPacket failed udp :0 %v", e)
	}

	serverAddr = c.LocalAddr().String()
	c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	go runSyslog(c, done)
}
