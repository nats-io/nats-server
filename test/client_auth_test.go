// Copyright 2016 Apcera Inc. All rights reserved.

package test

import (
	"fmt"
	"testing"

	"github.com/nats-io/nats"
)

func TestMultipleUserAuth(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/multi_user.conf")
	defer srv.Shutdown()

	if opts.Users == nil {
		t.Fatal("Expected a user array that is not nil")
	}
	if len(opts.Users) != 2 {
		t.Fatal("Expected a user array that had 2 users")
	}

	// Test first user
	url := fmt.Sprintf("nats://%s:%s@%s:%d/",
		opts.Users[0].Username,
		opts.Users[0].Password,
		opts.Host, opts.Port)

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Expected a successful connect, got %v\n", err)
	}
	defer nc.Close()

	if !nc.AuthRequired() {
		t.Fatal("Expected auth to be required for the server")
	}

	// Test second user
	url = fmt.Sprintf("nats://%s:%s@%s:%d/",
		opts.Users[1].Username,
		opts.Users[1].Password,
		opts.Host, opts.Port)

	nc, err = nats.Connect(url)
	if err != nil {
		t.Fatalf("Expected a successful connect, got %v\n", err)
	}
	defer nc.Close()
}
