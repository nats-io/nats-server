// Copyright 2016-2025 The NATS Authors
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
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
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
	content := `
	listen: 127.0.0.1:4567
	authorization={
		token: ` + testToken + `
		timeout: 5
	}`
	confFile := createConfFile(t, []byte(content))
	if err := os.WriteFile(confFile, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config file: %v", err)
	}
	s, opts := RunServerWithConfig(confFile)
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

func TestClientConnectInfo(t *testing.T) {
	for _, test := range []struct {
		name     string
		sys      string
		hasSys   bool
		protocol int
	}{
		{"async info support with explicit system account", "system_account: SYS", true, server.ClientProtoInfo},
		{"async info support without explicit system account", "", false, server.ClientProtoInfo},
		{"no async info support with explicit system account", "system_account: SYS", true, server.ClientProtoZero},
		{"no async info support without explicit system account", "", false, server.ClientProtoZero},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				%s
				accounts {
					SYS: { users: [{ user: sys, password: pwd}] }
					A:   { users: [{ user: a, password: pwd}] }
					B:   { users: [{ user: b, password: pwd}] }
				}
			`, test.sys)))
			hub, oHub := RunServerWithConfig(conf)
			defer hub.Shutdown()

			checkInfoOnConnect := func(user, acc string, isSys bool) {
				t.Helper()
				cc := createClientConn(t, oHub.Host, oHub.Port)
				defer cc.Close()

				checkInfoMsg(t, cc)
				sendProto(t, cc, fmt.Sprintf("CONNECT {\"user\":%q,\"pass\":\"pwd\",\"pedantic\":false,\"verbose\":false,\"protocol\":%d}\r\nPING\r\n", user, test.protocol))
				// Since the PONG and INFO may be receive as a single TCP read,
				// make sure we consume the pong alone here.
				pongBuf := make([]byte, len("PONG\r\n"))
				cc.SetReadDeadline(time.Now().Add(2 * time.Second))
				n, err := cc.Read(pongBuf)
				cc.SetReadDeadline(time.Time{})
				if n <= 0 && err != nil {
					t.Fatalf("Error reading from conn: %v\n", err)
				}
				if !pongRe.Match(pongBuf) {
					t.Fatalf("Response did not match expected: \n\tReceived:'%q'\n\tExpected:'%s'\n", pongBuf, pongRe)
				}
				if test.protocol < server.ClientProtoInfo {
					expectNothing(t, cc)
				} else {
					info := checkInfoMsg(t, cc)
					if !info.ConnectInfo {
						t.Fatal("Expected ConnectInfo to be true")
					}
					if an := info.RemoteAccount; an != acc {
						t.Fatalf("Expected account %q, got %q", acc, info.RemoteAccount)
					}
					if ais := info.IsSystemAccount; ais != isSys {
						t.Fatalf("Expected IsSystemAccount to be %v, got %v", isSys, ais)
					}
				}
			}
			checkInfoOnConnect("a", "A", false)
			checkInfoOnConnect("sys", "SYS", test.hasSys)
			checkInfoOnConnect("b", "B", false)
		})
	}
}
