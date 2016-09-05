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

func TestDynamicUserAuth(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/dynamic_user.conf")
	defer srv.Shutdown()

	if opts.Users == nil {
		t.Fatal("Expected a user array that is not nil")
	}
	if opts.DynamicUser != true {
		t.Fatal("Expected DynamicUser options is True")
	}
	if len(opts.Users) != 7 {
		t.Fatal("Expected a user array that had 7 users")
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

	// Test third user with defalut permissions
	url = fmt.Sprintf("nats://%s:%s@%s:%d/",
		opts.Users[2].Username,
		opts.Users[2].Password,
		opts.Host, opts.Port)

	nc, err = nats.Connect(url)
	if err != nil {
		t.Fatalf("Expected a successful connect, got %v\n", err)
	}
	defer nc.Close()

	// Test fourth user with only token
	url = fmt.Sprintf("nats://%s:%d/", opts.Host, opts.Port)

	nc, err = nats.Connect(url, nats.Token("248%BH7fZhfdOh6h%P3JDa"))
	if err != nil {
		t.Fatalf("Expected a successful connect, got %v\n", err)
	}
	defer nc.Close()
	if !nc.AuthRequired() {
		t.Fatal("Expected auth to be required for the server")
	}

	// Test fifth user with only token
	url = fmt.Sprintf("nats://%s:%s@%s:%d/",
		opts.Users[4].Username,
		opts.Users[4].Password,
		opts.Host, opts.Port)

	nc.Subscribe(opts.Users[4].Authenticator, func(msg *nats.Msg) {
		nc.Publish(msg.Reply, msg.Data)
	})
	nc, err = nats.Connect(url)
	if err != nil {
		t.Fatalf("Expected a successful connect, got %v\n", err)
	}
	defer nc.Close()
}
