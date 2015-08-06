// Copyright 2015 Apcera Inc. All rights reserved.

package test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats"
)

func TestMaxPayload(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/override.conf")
	defer srv.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d/", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Could not connect to server: %v", err)
	}
	defer nc.Close()

	big := sizedBytes(4 * 1024 * 1024)
	nc.Publish("foo", big)
	err = nc.FlushTimeout(1 * time.Second)
	if err == nil {
		t.Fatalf("Expected an error from flush")
	}
	if strings.Contains(err.Error(), "Maximum Payload Violation") != true {
		t.Fatalf("Received wrong error message (%v)\n", err)
	}
	if !nc.IsClosed() {
		t.Fatalf("Expected connection to be closed")
	}
}
