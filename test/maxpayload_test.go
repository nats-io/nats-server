// Copyright 2015 Apcera Inc. All rights reserved.

package test

import (
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/nats-io/nats"
)

func TestMaxPayload(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/override.conf")
	defer srv.Shutdown()

	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(fmt.Sprintf("nats://%s/", endpoint))
	if err != nil {
		t.Fatalf("Could not connect to server: %v", err)
	}
	defer nc.Close()

	size := 4 * 1024 * 1024
	big := sizedBytes(size)
	err = nc.Publish("foo", big)

	if err != nats.ErrMaxPayload {
		t.Fatalf("Expected a Max Payload error")
	}

	conn, err := net.DialTimeout("tcp", endpoint, nc.Opts.Timeout)
	if err != nil {
		t.Fatalf("Could not make a raw connection to the server: %v", err)
	}
	info := make([]byte, 512)
	_, err = conn.Read(info)
	if err != nil {
		t.Fatalf("Expected an info message to be sent by the server: %s", err)
	}
	pub := fmt.Sprintf("PUB bar %d\r\n", size)
	conn.Write([]byte(pub))
	if err != nil {
		t.Fatalf("Could not publish event to the server: %s", err)
	}

	errMsg := make([]byte, 35)
	_, err = conn.Read(errMsg)
	if err != nil {
		t.Fatalf("Expected an error message to be sent by the server: %s", err)
	}

	if strings.Contains(string(errMsg), "Maximum Payload Violation") != true {
		t.Errorf("Received wrong error message (%v)\n", string(errMsg))
	}

	// Client proactively omits sending the message so server
	// does not close the connection.
	if nc.IsClosed() {
		t.Errorf("Expected connection to not be closed.")
	}

	// On the other hand client which did not proactively omitted
	// publishing the bytes following what is suggested by server
	// in the info message has its connection closed.
	_, err = conn.Write(big)
	if err == nil {
		t.Errorf("Expected error due to maximum payload transgression.")
	}
}
