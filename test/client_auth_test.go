// Copyright 2016 Apcera Inc. All rights reserved.

package test

import (
	"fmt"
	"testing"

	"github.com/nats-io/go-nats"
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
	if len(opts.Users) != 6 {
		t.Fatal("Expected a user array that had 6 users")
	}

	// Test first user
	// Test same as multiuser auth
	url1 := fmt.Sprintf("nats://%s:%s@%s:%d/",
		opts.Users[0].Username,
		opts.Users[0].Password,
		opts.Host, opts.Port)

	nc1, err1 := nats.Connect(url1)
	if err1 != nil {
		t.Fatalf("Expected a successful connect, got %v\n", err1)
	}
	// nc1 will be used to authenticator server
	// nc1.Close()

	if !nc1.AuthRequired() {
		t.Fatal("Expected auth to be required for the server")
	}

	// Test second user
	// Test defalut permissions and regex
	url2 := fmt.Sprintf("nats://%s:%s@%s:%d/",
		"bar.2",
		opts.Users[1].Password,
		opts.Host, opts.Port)

	nc2, err2 := nats.Connect(url2)
	if err2 != nil {
		t.Fatalf("Expected a successful connect, got %v\n", err2)
	}
	nc2.Close()

	// Test third user with only token
	// Test encrypted token auth
	url3 := fmt.Sprintf("nats://%s:%d/", opts.Host, opts.Port)

	nc3, err3 := nats.Connect(url3, nats.Token("248%BH7fZhfdOh6h%P3JDa"))
	if err3 != nil {
		t.Fatalf("Expected a successful connect, got %v\n", err3)
	}
	nc3.Close()

	// Test fourth user
	// Test authenticator and regex
	url4 := fmt.Sprintf("nats://%s:%s@%s:%d/",
		"vincent.1",
		opts.Users[3].Password,
		opts.Host, opts.Port)

	// using nc1 as a authenticator server
	nc1.Subscribe(opts.Users[3].Authenticator, func(msg *nats.Msg) {
		// to make auth successful, return msg.Data with same password and token
		nc1.Publish(msg.Reply, msg.Data)
	})

	nc4, err4 := nats.Connect(url4)
	if err4 != nil {
		t.Fatalf("Expected a successful connect, got %v\n", err4)
	}
	nc4.Close()

	// Test fifth user
	// Test authenticator and wildcard
	url5 := fmt.Sprintf("nats://%s:%s@%s:%d/",
		"zhigao.1",
		opts.Users[4].Password,
		opts.Host, opts.Port)

	// using nc1 as a authenticator server
	nc1.Subscribe(opts.Users[4].Authenticator, func(msg *nats.Msg) {
		// to make auth successful, return msg.Data with same password and token
		nc1.Publish(msg.Reply, msg.Data)
	})

	nc5, err5 := nats.Connect(url5)
	if err5 != nil {
		t.Fatalf("Expected a successful connect, got %v\n", err5)
	}
	nc5.Close()

	// Test sixth user
	// Test token check in authenticator server
	url6 := fmt.Sprintf("nats://%s:%d/", opts.Host, opts.Port)

	// using nc1 as a authenticator server
	nc1.Subscribe(opts.Users[5].Authenticator, func(msg *nats.Msg) {
		// to make auth failed, return different msg.Data
		msg.Data = []byte("")
		nc1.Publish(msg.Reply, msg.Data)
	})

	nc6, err6 := nats.Connect(url6, nats.Token(opts.Users[5].Token))
	if nc6 != nil {
		t.Fatalf("Expected a failed connect, got %v\n", err6)
		nc6.Close()
	}

	nc1.Close()
}
