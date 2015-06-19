// Copyright 2015 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats"
)

const PING_CLIENT_PORT = 11228

var DefaultPingOptions = Options{
	Host:         "localhost",
	Port:         PING_CLIENT_PORT,
	NoLog:        true,
	NoSigs:       true,
	PingInterval: 5 * time.Millisecond,
}

func TestPing(t *testing.T) {
	s := RunServer(&DefaultPingOptions)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://localhost:%d", PING_CLIENT_PORT))
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc.Close()
	time.Sleep(10 * time.Millisecond)
}
