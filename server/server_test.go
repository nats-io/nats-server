// Copyright 2015 Apcera Inc. All rights reserved.

package server

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats"
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
		// Wait a bit to give a chance to the server to remove this
		// "client" from its state, which may otherwise interfere with
		// some tests.
		time.Sleep(25 * time.Millisecond)

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

type connEvent struct {
	Name string `json:"name"`
}

func TestSysEvents(t *testing.T) {
	s := RunServer(nil)
	defer s.Shutdown()

	serverURL := fmt.Sprintf("nats://%s:%d",
		DefaultOptions.Host, DefaultOptions.Port)

	// nil test.  Make sure we run OK w/o any subscriptions to _SYS functions, etc.
	nc, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatalf("Error creating connection: %v\n", err)
	}
	nc.Close()

	eventNc, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatalf("Error creating system event connection: %v\n", err)
	}
	defer eventNc.Close()

	ch := make(chan bool)

	processMsg := func(m *nats.Msg) {

		c := connEvent{}

		if err := json.Unmarshal(m.Data, &c); err != nil {
			t.Fatalf("Error unmarshalling data: %s", err)
		}

		if c.Name != "TestClient" {
			t.Fatalf("Expected name of \"TestClient\", received %s\n", c.Name)
		}

		ch <- true
	}

	connSub, err := eventNc.Subscribe("_SYS.CLIENT.*.CONNECT", func(m *nats.Msg) {
		processMsg(m)
	})
	if err != nil {
		t.Fatalf("Could not subscribe: %v\n", err)
	}

	eventNc.Subscribe("_SYS.CLIENT.*.DISCONNECT", func(m *nats.Msg) {
		processMsg(m)
	})
	if err != nil {
		t.Fatalf("Could not subscribe: %v\n", err)
	}

	eventNc.Flush()

	// Test connection exception
	opts := nats.DefaultOptions
	opts.Servers = []string{serverURL}
	opts.Name = "TestClient"
	nc, err = opts.Connect()
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}

	// wait for connect
	<-ch

	// unsubscribe to ensure a message is published on DISCONNECT.
	connSub.Unsubscribe()

	nc.Close()

	// wait for disconnect
	<-ch
}
