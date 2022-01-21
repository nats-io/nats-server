// Copyright 2012-2018 The NATS Authors
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

package server

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
)

func TestUserCloneNilPermissions(t *testing.T) {
	user := &User{
		Username: "foo",
		Password: "bar",
	}

	clone := user.clone()

	if !reflect.DeepEqual(user, clone) {
		t.Fatalf("Cloned Users are incorrect.\nexpected: %+v\ngot: %+v",
			user, clone)
	}

	clone.Password = "baz"
	if reflect.DeepEqual(user, clone) {
		t.Fatal("Expected Users to be different")
	}
}

func TestUserClone(t *testing.T) {
	user := &User{
		Username: "foo",
		Password: "bar",
		Permissions: &Permissions{
			Publish: &SubjectPermission{
				Allow: []string{"foo"},
			},
			Subscribe: &SubjectPermission{
				Allow: []string{"bar"},
			},
		},
	}

	clone := user.clone()

	if !reflect.DeepEqual(user, clone) {
		t.Fatalf("Cloned Users are incorrect.\nexpected: %+v\ngot: %+v",
			user, clone)
	}

	clone.Permissions.Subscribe.Allow = []string{"baz"}
	if reflect.DeepEqual(user, clone) {
		t.Fatal("Expected Users to be different")
	}
}

func TestUserClonePermissionsNoLists(t *testing.T) {
	user := &User{
		Username:    "foo",
		Password:    "bar",
		Permissions: &Permissions{},
	}

	clone := user.clone()

	if clone.Permissions.Publish != nil {
		t.Fatalf("Expected Publish to be nil, got: %v", clone.Permissions.Publish)
	}
	if clone.Permissions.Subscribe != nil {
		t.Fatalf("Expected Subscribe to be nil, got: %v", clone.Permissions.Subscribe)
	}
}

func TestUserCloneNoPermissions(t *testing.T) {
	user := &User{
		Username: "foo",
		Password: "bar",
	}

	clone := user.clone()

	if clone.Permissions != nil {
		t.Fatalf("Expected Permissions to be nil, got: %v", clone.Permissions)
	}
}

func TestUserCloneNil(t *testing.T) {
	user := (*User)(nil)
	clone := user.clone()
	if clone != nil {
		t.Fatalf("Expected nil, got: %+v", clone)
	}
}

func TestUserUnknownAllowedConnectionType(t *testing.T) {
	o := DefaultOptions()
	o.Users = []*User{{
		Username:               "user",
		Password:               "pwd",
		AllowedConnectionTypes: testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard, "someNewType"}),
	}}
	_, err := NewServer(o)
	if err == nil || !strings.Contains(err.Error(), "connection type") {
		t.Fatalf("Expected error about unknown connection type, got %v", err)
	}

	o.Users[0].AllowedConnectionTypes = testCreateAllowedConnectionTypes([]string{"websocket"})
	s, err := NewServer(o)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	s.mu.Lock()
	user := s.opts.Users[0]
	s.mu.Unlock()
	for act := range user.AllowedConnectionTypes {
		if act != jwt.ConnectionTypeWebsocket {
			t.Fatalf("Expected map to have been updated with proper case, got %v", act)
		}
	}
	// Same with NKey user now.
	o.Users = nil
	o.Nkeys = []*NkeyUser{{
		Nkey:                   "somekey",
		AllowedConnectionTypes: testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard, "someNewType"}),
	}}
	_, err = NewServer(o)
	if err == nil || !strings.Contains(err.Error(), "connection type") {
		t.Fatalf("Expected error about unknown connection type, got %v", err)
	}
	o.Nkeys[0].AllowedConnectionTypes = testCreateAllowedConnectionTypes([]string{"websocket"})
	s, err = NewServer(o)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	s.mu.Lock()
	nkey := s.opts.Nkeys[0]
	s.mu.Unlock()
	for act := range nkey.AllowedConnectionTypes {
		if act != jwt.ConnectionTypeWebsocket {
			t.Fatalf("Expected map to have been updated with proper case, got %v", act)
		}
	}
}

func TestDNSAltNameMatching(t *testing.T) {
	for idx, test := range []struct {
		altName string
		urls    []string
		match   bool
	}{
		{"foo", []string{"FOO"}, true},
		{"foo", []string{".."}, false},
		{"foo", []string{"."}, false},
		{"Foo", []string{"foO"}, true},
		{"FOO", []string{"foo"}, true},
		{"foo1", []string{"bar"}, false},
		{"multi", []string{"m", "mu", "mul", "multi"}, true},
		{"multi", []string{"multi", "m", "mu", "mul"}, true},
		{"foo.bar", []string{"foo", "foo.bar.bar", "foo.baz"}, false},
		{"foo.Bar", []string{"foo", "bar.foo", "Foo.Bar"}, true},
		{"foo.*", []string{"foo", "bar.foo", "Foo.Bar"}, false}, // only match left most
		{"f*.bar", []string{"foo", "bar.foo", "Foo.Bar"}, false},
		{"*.bar", []string{"foo.bar"}, true},
		{"*", []string{"baz.bar", "bar", "z.y"}, true},
		{"*", []string{"bar"}, true},
		{"*", []string{"."}, false},
		{"*", []string{""}, true},
		{"*", []string{"*"}, true},
		{"bar.*", []string{"bar.*"}, true},
		{"*.Y-X-red-mgmt.default.svc", []string{"A.Y-X-red-mgmt.default.svc"}, true},
		{"*.Y-X-green-mgmt.default.svc", []string{"A.Y-X-green-mgmt.default.svc"}, true},
		{"*.Y-X-blue-mgmt.default.svc", []string{"A.Y-X-blue-mgmt.default.svc"}, true},
		{"Y-X-red-mgmt", []string{"Y-X-red-mgmt"}, true},
		{"Y-X-red-mgmt", []string{"X-X-red-mgmt"}, false},
		{"Y-X-red-mgmt", []string{"Y-X-green-mgmt"}, false},
		{"Y-X-red-mgmt", []string{"Y"}, false},
		{"Y-X-red-mgmt", []string{"Y-X"}, false},
		{"Y-X-red-mgmt", []string{"Y-X-red"}, false},
		{"Y-X-red-mgmt", []string{"X-red-mgmt"}, false},
		{"Y-X-green-mgmt", []string{"Y-X-green-mgmt"}, true},
		{"Y-X-blue-mgmt", []string{"Y-X-blue-mgmt"}, true},
		{"connect.Y.local", []string{"connect.Y.local"}, true},
		{"connect.Y.local", []string{".Y.local"}, false},
		{"connect.Y.local", []string{"..local"}, false},
		{"gcp.Y.local", []string{"gcp.Y.local"}, true},
		{"uswest1.gcp.Y.local", []string{"uswest1.gcp.Y.local"}, true},
	} {
		urlSet := make([]*url.URL, len(test.urls))
		for i, u := range test.urls {
			var err error
			urlSet[i], err = url.Parse("nats://" + u)
			if err != nil {
				t.Fatal(err)
			}
		}
		if dnsAltNameMatches(dnsAltNameLabels(test.altName), urlSet) != test.match {
			t.Fatal("Test", idx, "Match miss match, expected:", test.match)
		}
	}
}

func TestNoAuthUser(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		accounts {
			FOO { users [{user: "foo", password: "pwd1"}] }
			BAR { users [{user: "bar", password: "pwd2"}] }
		}
		no_auth_user: "foo"
	`))
	defer os.Remove(conf)
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	for _, test := range []struct {
		name    string
		usrInfo string
		ok      bool
		account string
	}{
		{"valid user/pwd", "bar:pwd2@", true, "BAR"},
		{"invalid pwd", "bar:wrong@", false, _EMPTY_},
		{"some token", "sometoken@", false, _EMPTY_},
		{"user used without pwd", "bar@", false, _EMPTY_}, // will be treated as a token
		{"user with empty password", "bar:@", false, _EMPTY_},
		{"no user", _EMPTY_, true, "FOO"},
	} {
		t.Run(test.name, func(t *testing.T) {
			url := fmt.Sprintf("nats://%s127.0.0.1:%d", test.usrInfo, o.Port)
			nc, err := nats.Connect(url)
			if err != nil {
				if test.ok {
					t.Fatalf("Unexpected error: %v", err)
				}
				return
			} else if !test.ok {
				nc.Close()
				t.Fatalf("Should have failed, did not")
			}
			var accName string
			s.mu.Lock()
			for _, c := range s.clients {
				c.mu.Lock()
				if c.acc != nil {
					accName = c.acc.Name
				}
				c.mu.Unlock()
				break
			}
			s.mu.Unlock()
			nc.Close()
			checkClientsCount(t, s, 0)
			if accName != test.account {
				t.Fatalf("The account should have been %q, got %q", test.account, accName)
			}
		})
	}
}

func TestNoAuthUserNoConnectProto(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		accounts {
			A { users [{user: "foo", password: "pwd"}] }
		}
		authorization { timeout: 1 }
		no_auth_user: "foo"
	`))
	defer os.Remove(conf)
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	checkClients := func(n int) {
		t.Helper()
		time.Sleep(100 * time.Millisecond)
		if nc := s.NumClients(); nc != n {
			t.Fatalf("Expected %d clients, got %d", n, nc)
		}
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", o.Host, o.Port))
	require_NoError(t, err)
	defer conn.Close()
	checkClientsCount(t, s, 1)

	// With no auth user we should not require a CONNECT.
	// Make sure we are good on not sending CONN first.
	_, err = conn.Write([]byte("PUB foo 2\r\nok\r\n"))
	require_NoError(t, err)
	checkClients(1)
	conn.Close()

	// Now make sure we still do get timed out though.
	conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", o.Host, o.Port))
	require_NoError(t, err)
	defer conn.Close()
	checkClientsCount(t, s, 1)

	time.Sleep(1200 * time.Millisecond)
	checkClientsCount(t, s, 0)
}
