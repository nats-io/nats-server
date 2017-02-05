// Copyright 2016 Apcera Inc. All rights reserved.

package test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	httpmock "gopkg.in/jarcoal/httpmock.v1"

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
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("POST", "http://127.0.0.1:9292/authenticate",
		func(req *http.Request) (*http.Response, error) {
			credential := make(map[string]interface{})
			if err := json.NewDecoder(req.Body).Decode(&credential); err != nil {
				return httpmock.NewStringResponse(400, ""), nil
			}
			fmt.Println("Authenticating ", credential["username"], credential["password"])

			if credential["username"] == credential["password"] {
				return httpmock.NewJsonResponse(200, credential)
			}
			return httpmock.NewStringResponse(401, ""), nil

		})
	srv, opts := RunServerWithConfig("./configs/dynamic_user.conf")
	defer srv.Shutdown()

	if opts.Users == nil {
		t.Fatal("Expected a user array that is not nil")
	}
	if len(opts.Users) != 4 {
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

	// Test second user with same value for user name and password
	url2 := fmt.Sprintf("nats://%s:%s@%s:%d/",
		"test2",
		"test2",
		opts.Host, opts.Port)

	nc2, err2 := nats.Connect(url2)
	if err2 != nil {
		t.Fatalf("Expected a successful connect, got %v\n", err2)
	}
	nc2.Close()

	// Test third user with different value for user name and password, which voilate the authentication check
	url3 := fmt.Sprintf("nats://%s:%s@%s:%d/",
		"test2_wrong",
		"test2",
		opts.Host, opts.Port)

	nc3, err3 := nats.Connect(url3)
	if err3 == nil {
		t.Fatalf("Expected an unsuccessful connect to %s, got %v\n", url3, err3)
	}
	nc3.Close()
}
