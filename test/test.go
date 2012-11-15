// Copyright 2012 Apcera Inc. All rights reserved.

package test

import (
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"
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

func startServer(t tLogger, port uint, other string) *natsServer {
	var s natsServer
	args := fmt.Sprintf("-p %d %s", port, other)
	s.args = strings.Split(args, " ")
	s.cmd = exec.Command(natsServerExe, s.args...)
	err := s.cmd.Start()
	if err != nil {
		s.cmd = nil
		t.Errorf("Could not start <%s>, is NATS installed and in path?", natsServerExe)
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
	c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Could not connect to server: %v\n", err)
	}
	return c
}


