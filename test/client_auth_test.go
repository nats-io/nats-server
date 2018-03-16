// Copyright 2016-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"fmt"
	"io/ioutil"
	"os"
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

// Resolves to "test"
const testToken = "$2a$05$3sSWEVA1eMCbV0hWavDjXOx.ClBjI6u1CuUdLqf22cbJjXsnzz8/."

func TestTokenInConfig(t *testing.T) {
	confFileName := "test.conf"
	defer os.Remove(confFileName)
	content := `
	listen: 127.0.0.1:4567
	authorization={
		token: ` + testToken + `
		timeout: 5
	}`
	if err := ioutil.WriteFile(confFileName, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config file: %v", err)
	}
	s, opts := RunServerWithConfig(confFileName)
	defer s.Shutdown()

	url := fmt.Sprintf("nats://test@%s:%d/", opts.Host, opts.Port)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Expected a successful connect, got %v\n", err)
	}
	defer nc.Close()
	if !nc.AuthRequired() {
		t.Fatal("Expected auth to be required for the server")
	}
}
