// Copyright 2015 Apcera Inc. All rights reserved.

package server

import (
	"net"
	"testing"
	"time"
)

var DefaultOptions = Options{
	Host:        "localhost",
	Port:        11222,
	HTTPPort:    11333,
	ClusterPort: 11444,
	ProfPort:    11280,
	NoLog:       true,
	NoSigs:      true,
}

// New Go Routine based server
func RunServer(opts *Options) *Server {
	if opts == nil {
		opts = &DefaultOptions
	}
	s := New(opts)
	if s == nil {
		panic("No NATS Server object returned.")
	}

	// Run server in Go routine.
	go s.Start()

	end := time.Now().Add(10 * time.Second)
	for time.Now().Before(end) {
		addr := s.GetListenEndpoint()
		if addr == "" {
			time.Sleep(10 * time.Millisecond)
			// Retry. We might take a little while to open a connection.
			continue
		}
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			// Retry after 50ms
			time.Sleep(50 * time.Millisecond)
			continue
		}
		conn.Close()
		return s
	}
	panic("Unable to start NATS Server in Go Routine")

}

func TestStartupAndShutdown(t *testing.T) {
	s := RunServer(&DefaultOptions)
	defer s.Shutdown()

	if s.isRunning() != true {
		t.Fatal("Could not run server")
	}

	// Debug stuff.
	numRoutes := s.NumRoutes()
	if numRoutes != 0 {
		t.Fatalf("Expected numRoutes to be 0 vs %d\n", numRoutes)
	}

	numRemotes := s.NumRemotes()
	if numRemotes != 0 {
		t.Fatalf("Expected numRemotes to be 0 vs %d\n", numRemotes)
	}

	numClients := s.NumClients()
	if numClients != 0 && numClients != 1 {
		t.Fatalf("Expected numClients to be 1 or 0 vs %d\n", numClients)
	}

	numSubscriptions := s.NumSubscriptions()
	if numSubscriptions != 0 {
		t.Fatalf("Expected numSubscriptions to be 0 vs %d\n", numSubscriptions)
	}
}
