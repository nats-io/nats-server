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
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
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

func TestProxyKeyVerification(t *testing.T) {
	u1, _ := nkeys.CreateUser()
	u1Pub, _ := u1.PublicKey()

	u2, _ := nkeys.CreateUser()
	u2Pub, _ := u2.PublicKey()

	u3, _ := nkeys.CreateUser()
	u3Pub, _ := u3.PublicKey()

	tmpl := `
		listen: "127.0.0.1:-1"
		proxies {
			trusted [
				{key: %q}
				{key: %q}
			]
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`
	conf := createConfFile(t, fmt.Appendf(nil, tmpl, u1Pub, u2Pub))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	varz, err := s.Varz(nil)
	if err != nil {
		t.Fatalf("Error getting varz: %v", err)
	}
	vproxies := varz.Proxies
	if vproxies == nil {
		t.Fatal("Expected proxies to be not nil")
	}
	trusted := vproxies.Trusted
	if len(trusted) != 2 {
		t.Fatalf("Expected 2 trusted proxies, got %+v", trusted)
	}
	if k := trusted[0].Key; k != u1Pub {
		t.Fatalf("Expected key to be %q, got %q", u1Pub, k)
	}
	if k := trusted[1].Key; k != u2Pub {
		t.Fatalf("Expected key to be %q, got %q", u2Pub, k)
	}

	var currentCID uint64
	connect := func(t *testing.T, kp nkeys.KeyPair, bsig bool, port int, isClient bool, user string) (expectFun, net.Conn) {
		c := createClientConn(t, "127.0.0.1", port)
		expect := expectCommand(t, c)
		info := checkInfoMsg(t, c)
		currentCID = info.CID
		if info.Nonce == "" {
			c.Close()
			t.Fatalf("Expected nonce from INFO, got %+v", info)
		}
		if info.JSApiLevel != server.JSApiLevel {
			c.Close()
			t.Fatalf("Expected JSApiLevel to be set to %v, got %v", server.JSApiLevel, info.JSApiLevel)
		}
		sig, err := kp.Sign([]byte(info.Nonce))
		if err != nil {
			c.Close()
			t.Fatalf("Error signing nonce %q: %v", info.Nonce, err)
		}
		encodedSig := make([]byte, base64.RawURLEncoding.EncodedLen(len(sig)))
		base64.RawURLEncoding.Encode(encodedSig, sig)
		if bsig {
			encodedSig[0] = '*'
		}
		sendProto(t, c,
			fmt.Sprintf("CONNECT {\"pedantic\":false,\"verbose\":false,\"user\":%q,\"pass\":\"pwd\",\"proxy_sig\":\"%s\"}\r\n",
				user, string(encodedSig)))
		if isClient {
			sendProto(t, c, "PING\r\n")
		}
		return expect, c
	}

	checkClosedState := func(t *testing.T, cid uint64, reason server.ClosedState) {
		// Closed connection getting in the closed state is done in a go routine
		// so make sure we wait for it.
		checkFor(t, time.Second, 100*time.Millisecond, func() error {
			connz, err := s.Connz(&server.ConnzOptions{
				CID:   cid,
				State: server.ConnClosed,
			})
			if err != nil {
				return fmt.Errorf("Error getting connz for closed connection %v: %v", cid, err)
			}
			if len(connz.Conns) != 1 {
				return fmt.Errorf("Expected 1 connection, got %v", connz.Conns)
			}
			conn := connz.Conns[0]
			if conn.Reason != reason.String() {
				return fmt.Errorf("Expected reason %q, got %q", reason.String(), conn.Reason)
			}
			return nil
		})
	}

	for _, test := range []struct {
		name string
		kp   nkeys.KeyPair
		bsig bool
		ok   bool
	}{
		{"key present first", u1, false, true},
		{"key present last", u2, false, true},
		{"key not present", u3, false, false},
		{"bad signature", u2, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, subTest := range []struct {
				name     string
				port     int
				isClient bool
			}{
				{"client", o.Port, true},
				{"leafnodes", o.LeafNode.Port, false},
			} {
				t.Run(subTest.name, func(t *testing.T) {
					expect, c := connect(t, test.kp, test.bsig, subTest.port, subTest.isClient, "user")
					defer c.Close()
					if test.ok {
						var p *server.ProxyInfo
						if subTest.isClient {
							expect(pongRe)

							connz, err := s.Connz(nil)
							if err != nil {
								t.Fatalf("Error getting connz: %v", err)
							}
							if n := len(connz.Conns); n != 1 {
								t.Fatalf("Expected 1 ConnInfo, got %v", n)
							}
							ci := connz.Conns[0]
							p = ci.Proxy
						} else {
							expect(infoRe)

							leafz, err := s.Leafz(nil)
							if err != nil {
								t.Fatalf("Error getting leafz: %v", err)
							}
							if n := len(leafz.Leafs); n != 1 {
								t.Fatalf("Expected 1 LeafInfo, got %v", n)
							}
							li := leafz.Leafs[0]
							if li.Proxy == nil {
								t.Fatalf("Required Proxy to be present, was not: %+v", li)
							}
							p = li.Proxy
						}
						if p == nil {
							t.Fatalf("Required Proxy to be present, was not")
						}
						pub, _ := test.kp.PublicKey()
						if p.Key != pub {
							t.Fatalf("Expected public key to be %q, got %q", pub, p.Key)
						}
					} else {
						expect(errRe)
						checkClosedState(t, currentCID, server.ProxyNotTrusted)
					}
				})
			}
		})
	}

	// Create a connection using u1Pub and we will config reload and remove
	// that key from the list. The client should get disconnected.
	expectc, c := connect(t, u1, false, o.Port, true, "user")
	defer c.Close()
	expectc(pongRe)
	// Capture this connection CID before the next connect.
	cid1 := currentCID

	expectl, l := connect(t, u1, false, o.LeafNode.Port, false, "user")
	defer l.Close()
	expectl(infoRe)
	cid2 := currentCID
	checkLeafNodeConnected(t, s)

	os.WriteFile(conf, fmt.Appendf(nil, tmpl, u3Pub, u2Pub), 0660)
	if err := s.Reload(); err != nil {
		t.Fatalf("Reload failed: %v", err)
	}
	// Connections should get disconnected.
	// We need to consume what is sent by the server, but for leaf we may
	// get some LS+, etc... so just consumer until we get the io.EOF
	readAll := func(c net.Conn) {
		for {
			var buf [1024]byte
			if _, err := c.Read(buf[:]); err != nil {
				break
			}
		}
	}
	readAll(c)
	expectDisconnect(t, c)
	readAll(l)
	expectDisconnect(t, l)
	c.Close()
	l.Close()

	// Now check that the connection with CID `cid` was closed because the
	// proxy is (no longer) trusted.
	checkClosedState(t, cid1, server.ProxyNotTrusted)
	checkClosedState(t, cid2, server.ProxyNotTrusted)

	varz, err = s.Varz(nil)
	if err != nil {
		t.Fatalf("Error getting varz: %v", err)
	}
	vproxies = varz.Proxies
	if vproxies == nil {
		t.Fatal("Expected proxies to be not nil")
	}
	trusted = vproxies.Trusted
	if len(trusted) != 2 {
		t.Fatalf("Expected 2 trusted proxies, got %+v", trusted)
	}
	if k := trusted[0].Key; k != u3Pub {
		t.Fatalf("Expected key to be %q, got %q", u3Pub, k)
	}
	if k := trusted[1].Key; k != u2Pub {
		t.Fatalf("Expected key to be %q, got %q", u2Pub, k)
	}

	// And we should be able to connect using u3 key pair.
	expectc, c = connect(t, u3, false, o.Port, true, "user")
	expectc(pongRe)
	c.Close()
	expectl, l = connect(t, u3, false, o.LeafNode.Port, false, "user")
	expectl(infoRe)
	l.Close()

	// But now using u1 key pair should fail.
	expectc, c = connect(t, u1, false, o.Port, true, "user")
	expectc(errRe)
	c.Close()
	expectl, l = connect(t, u1, false, o.LeafNode.Port, false, "user")
	expectl(errRe)
	l.Close()

	s.Shutdown()

	// Now check that if no trusted proxy are provided, a proxy connection is
	// accepted. Use leafnodes since we always send NONCE for this type of connection.
	conf = createConfFile(t, []byte(`
		port: -1
		leafnodes {
			port: -1
		}
	`))
	s, o = RunServerWithConfig(conf)
	defer s.Shutdown()

	for _, test := range []struct {
		name string
		bsig bool
	}{
		{"no trusted proxy and correct signature", false},
		{"no trusted proxy and invalid signature", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			expect, c := connect(t, u1, test.bsig, o.LeafNode.Port, false, "user")
			defer c.Close()
			// Does not matter if bad signature, since there is no trusted keys
			// in the server, we accept without signature check.
			expect(infoRe)
		})
	}

	s.Shutdown()

	// Finally, check that if there are no configured trusted proxies
	// and a user requires a proxy connection, then the connection will fail.
	conf = createConfFile(t, []byte(`
		port: -1
		leafnodes {
			port: -1
			authorization {
				users [
					{user: u1, password: pwd}
					{user: u2, password: pwd, proxy_required: true}
				]
			}
		}
	`))
	s, o = RunServerWithConfig(conf)
	defer s.Shutdown()

	for _, test := range []struct {
		name string
		user string
		ok   bool
	}{
		{"no trusted proxy configured no proxy required", "u1", true},
		{"no trusted proxy configured and proxy required", "u2", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			// We can use any key pair here, it is not important.
			expect, c := connect(t, u1, false, o.LeafNode.Port, false, test.user)
			defer c.Close()
			if test.ok {
				expect(infoRe)
			} else {
				expect(errRe)
				// Check that the reason is "proxy required"
				checkClosedState(t, currentCID, server.ProxyRequired)
			}
		})
	}

	// Same test, but the "proxy_required" is at the authorization{} to-level,
	// so any user should fail.
	conf = createConfFile(t, []byte(`
		port: -1
		leafnodes {
			port: -1
			authorization {
				users [
					{user: u1, password: pwd}
					{user: u2, password: pwd}
				]
				proxy_required: true
			}
		}
	`))
	s, o = RunServerWithConfig(conf)
	defer s.Shutdown()

	t.Run("all users required to use trusted proxy", func(t *testing.T) {
		for _, user := range []string{"u1", "u2"} {
			// We can use any key pair here, it is not important.
			expect, c := connect(t, u1, false, o.LeafNode.Port, false, user)
			defer c.Close()
			expect(errRe)
			// Check that the reason is "proxy required"
			checkClosedState(t, currentCID, server.ProxyRequired)
		}
	})

	varz, err = s.Varz(nil)
	if err != nil {
		t.Fatalf("Error getting varz: %v", err)
	}
	vproxies = varz.Proxies
	if vproxies != nil {
		t.Fatalf("Expected proxies to be nil, got %+v", vproxies)
	}
}
