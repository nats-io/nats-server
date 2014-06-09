// Copyright 2014 Apcera Inc. All rights reserved.

package test

import (
	"net"
	"strconv"
	"testing"

	"github.com/apcera/gnatsd/server"
)

func TestResolveRandomPort(t *testing.T) {
	opts := &server.Options{Port: server.RANDOM_PORT}
	s := RunServer(opts)
	defer s.Shutdown()

	addr := s.Addr()
	_, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}

	portNum, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}

	if portNum == server.DEFAULT_PORT {
		t.Fatalf("Expected server to choose a random port\nGot: %d", server.DEFAULT_PORT)
	}

	if portNum == server.RANDOM_PORT {
		t.Fatalf("Expected server to choose a random port\nGot: %d", server.RANDOM_PORT)
	}

	if opts.Port != portNum {
		t.Fatalf("Options port (%d) should have been overridden by chosen random port (%d)",
			opts.Port, portNum)
	}
}
