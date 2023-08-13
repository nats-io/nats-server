// Copyright 2019-2023 The NATS Authors
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
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nkeys"

	"github.com/klauspost/compress/s2"
	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"

	"github.com/nats-io/nats-server/v2/internal/testhelper"
)

type captureLeafNodeRandomIPLogger struct {
	DummyLogger
	ch  chan struct{}
	ips [3]int
}

func (c *captureLeafNodeRandomIPLogger) Debugf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "hostname_to_resolve") {
		ippos := strings.Index(msg, "127.0.0.")
		if ippos != -1 {
			n := int(msg[ippos+8] - '1')
			c.ips[n]++
			for _, v := range c.ips {
				if v < 2 {
					return
				}
			}
			// All IPs got at least some hit, we are done.
			c.ch <- struct{}{}
		}
	}
}

func TestLeafNodeRandomIP(t *testing.T) {
	u, err := url.Parse("nats://hostname_to_resolve:1234")
	if err != nil {
		t.Fatalf("Error parsing: %v", err)
	}

	resolver := &myDummyDNSResolver{ips: []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"}}

	o := DefaultOptions()
	o.Host = "127.0.0.1"
	o.Port = -1
	o.LeafNode.Port = 0
	o.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	o.LeafNode.ReconnectInterval = 50 * time.Millisecond
	o.LeafNode.resolver = resolver
	o.LeafNode.dialTimeout = 15 * time.Millisecond
	s := RunServer(o)
	defer s.Shutdown()

	l := &captureLeafNodeRandomIPLogger{ch: make(chan struct{})}
	s.SetLogger(l, true, true)

	select {
	case <-l.ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("Does not seem to have used random IPs")
	}
}

func TestLeafNodeRandomRemotes(t *testing.T) {
	// 16! possible permutations.
	orderedURLs := make([]*url.URL, 0, 16)
	for i := 0; i < cap(orderedURLs); i++ {
		orderedURLs = append(orderedURLs, &url.URL{
			Scheme: "nats-leaf",
			Host:   fmt.Sprintf("host%d:7422", i),
		})
	}

	o := DefaultOptions()
	o.LeafNode.Remotes = []*RemoteLeafOpts{
		{NoRandomize: true},
		{NoRandomize: false},
	}
	o.LeafNode.Remotes[0].URLs = make([]*url.URL, cap(orderedURLs))
	copy(o.LeafNode.Remotes[0].URLs, orderedURLs)
	o.LeafNode.Remotes[1].URLs = make([]*url.URL, cap(orderedURLs))
	copy(o.LeafNode.Remotes[1].URLs, orderedURLs)

	s := RunServer(o)
	defer s.Shutdown()

	s.mu.Lock()
	r1 := s.leafRemoteCfgs[0]
	r2 := s.leafRemoteCfgs[1]
	s.mu.Unlock()

	r1.RLock()
	gotOrdered := r1.urls
	r1.RUnlock()
	if got, want := len(gotOrdered), len(orderedURLs); got != want {
		t.Fatalf("Unexpected rem0 len URLs, got %d, want %d", got, want)
	}

	// These should be IN order.
	for i := range orderedURLs {
		if got, want := gotOrdered[i].String(), orderedURLs[i].String(); got != want {
			t.Fatalf("Unexpected ordered url, got %s, want %s", got, want)
		}
	}

	r2.RLock()
	gotRandom := r2.urls
	r2.RUnlock()
	if got, want := len(gotRandom), len(orderedURLs); got != want {
		t.Fatalf("Unexpected rem1 len URLs, got %d, want %d", got, want)
	}

	// These should be OUT of order.
	var random bool
	for i := range orderedURLs {
		if gotRandom[i].String() != orderedURLs[i].String() {
			random = true
			break
		}
	}
	if !random {
		t.Fatal("Expected urls to be random")
	}
}

type testLoopbackResolver struct{}

func (r *testLoopbackResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	return []string{"127.0.0.1"}, nil
}

func TestLeafNodeTLSWithCerts(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		leaf {
			listen: "127.0.0.1:-1"
			tls {
				ca_file: "../test/configs/certs/tlsauth/ca.pem"
				cert_file: "../test/configs/certs/tlsauth/server.pem"
				key_file:  "../test/configs/certs/tlsauth/server-key.pem"
				timeout: 2
			}
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	u, err := url.Parse(fmt.Sprintf("nats://localhost:%d", o1.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}
	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leaf {
			remotes [
				{
					url: "%s"
					tls {
						ca_file: "../test/configs/certs/tlsauth/ca.pem"
						cert_file: "../test/configs/certs/tlsauth/client.pem"
						key_file:  "../test/configs/certs/tlsauth/client-key.pem"
						timeout: 2
					}
				}
			]
		}
	`, u.String())))
	o2, err := ProcessConfigFile(conf2)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	o2.NoLog, o2.NoSigs = true, true
	o2.LeafNode.resolver = &testLoopbackResolver{}
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkFor(t, 3*time.Second, 10*time.Millisecond, func() error {
		if nln := s1.NumLeafNodes(); nln != 1 {
			return fmt.Errorf("Number of leaf nodes is %d", nln)
		}
		return nil
	})
}

func TestLeafNodeTLSRemoteWithNoCerts(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		leaf {
			listen: "127.0.0.1:-1"
			tls {
				ca_file: "../test/configs/certs/tlsauth/ca.pem"
				cert_file: "../test/configs/certs/tlsauth/server.pem"
				key_file:  "../test/configs/certs/tlsauth/server-key.pem"
				timeout: 2
			}
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	u, err := url.Parse(fmt.Sprintf("nats://localhost:%d", o1.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}
	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leaf {
			remotes [
				{
					url: "%s"
					tls {
						ca_file: "../test/configs/certs/tlsauth/ca.pem"
						timeout: 5
					}
				}
			]
		}
	`, u.String())))
	o2, err := ProcessConfigFile(conf2)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	if len(o2.LeafNode.Remotes) == 0 {
		t.Fatal("Expected at least a single leaf remote")
	}

	var (
		got      float64 = o2.LeafNode.Remotes[0].TLSTimeout
		expected float64 = 5
	)
	if got != expected {
		t.Fatalf("Expected %v, got: %v", expected, got)
	}
	o2.NoLog, o2.NoSigs = true, true
	o2.LeafNode.resolver = &testLoopbackResolver{}
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkFor(t, 3*time.Second, 10*time.Millisecond, func() error {
		if nln := s1.NumLeafNodes(); nln != 1 {
			return fmt.Errorf("Number of leaf nodes is %d", nln)
		}
		return nil
	})

	// Here we only process options without starting the server
	// and without a root CA for the remote.
	conf3 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leaf {
			remotes [
				{
					url: "%s"
					tls {
						timeout: 10
					}
				}
			]
		}
	`, u.String())))
	o3, err := ProcessConfigFile(conf3)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	if len(o3.LeafNode.Remotes) == 0 {
		t.Fatal("Expected at least a single leaf remote")
	}
	got = o3.LeafNode.Remotes[0].TLSTimeout
	expected = 10
	if got != expected {
		t.Fatalf("Expected %v, got: %v", expected, got)
	}

	// Here we only process options without starting the server
	// and check the default for leafnode remotes.
	conf4 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leaf {
			remotes [
				{
					url: "%s"
					tls {
						ca_file: "../test/configs/certs/tlsauth/ca.pem"
					}
				}
			]
		}
	`, u.String())))
	o4, err := ProcessConfigFile(conf4)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	if len(o4.LeafNode.Remotes) == 0 {
		t.Fatal("Expected at least a single leaf remote")
	}
	got = o4.LeafNode.Remotes[0].TLSTimeout
	expected = float64(DEFAULT_LEAF_TLS_TIMEOUT) / float64(time.Second)
	if int(got) != int(expected) {
		t.Fatalf("Expected %v, got: %v", expected, got)
	}
}

type captureErrorLogger struct {
	DummyLogger
	errCh chan string
}

func (l *captureErrorLogger) Errorf(format string, v ...interface{}) {
	select {
	case l.errCh <- fmt.Sprintf(format, v...):
	default:
	}
}

func TestLeafNodeAccountNotFound(t *testing.T) {
	ob := DefaultOptions()
	ob.LeafNode.Host = "127.0.0.1"
	ob.LeafNode.Port = -1
	sb := RunServer(ob)
	defer sb.Shutdown()

	u, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ob.LeafNode.Port))

	oa := DefaultOptions()
	oa.Cluster.Name = "xyz"
	oa.LeafNode.ReconnectInterval = 10 * time.Millisecond
	oa.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			LocalAccount: "foo",
			URLs:         []*url.URL{u},
		},
	}
	// Expected to fail
	if _, err := NewServer(oa); err == nil || !strings.Contains(err.Error(), "local account") {
		t.Fatalf("Expected server to fail with error about no local account, got %v", err)
	}
	oa.Accounts = []*Account{NewAccount("foo")}
	sa := RunServer(oa)
	defer sa.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 1)}
	sa.SetLogger(l, false, false)

	checkLeafNodeConnected(t, sa)

	// Now simulate account is removed with config reload, or it expires.
	sa.accounts.Delete("foo")

	// Restart B (with same Port)
	sb.Shutdown()
	sb = RunServer(ob)
	defer sb.Shutdown()

	// Wait for report of error
	select {
	case e := <-l.errCh:
		if !strings.Contains(e, "Unable to lookup account") {
			t.Fatalf("Expected error about no local account, got %s", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get the error")
	}

	// TODO below test is bogus. Instead add the account, do a reload, and make sure the connection works.

	// For now, sa would try to recreate the connection for ever.
	// Check that lid is increasing...
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if lid := atomic.LoadUint64(&sa.gcid); lid < 3 {
			return fmt.Errorf("Seems like connection was not retried, lid currently only at %d", lid)
		}
		return nil
	})
}

// This test ensures that we can connect using proper user/password
// to a LN URL that was discovered through the INFO protocol.
// We also check that the password doesn't leak to debug/trace logs.
func TestLeafNodeBasicAuthFailover(t *testing.T) {
	// Something a little longer than "pwd" to prevent false positives amongst many log lines;
	// don't make it complex enough to be subject to %-escaping, we want a simple needle search.
	fatalPassword := "pwdfatal"

	content := `
	listen: "127.0.0.1:-1"
	cluster {
		name: "abc"
		listen: "127.0.0.1:-1"
		%s
	}
	leafnodes {
		listen: "127.0.0.1:-1"
		authorization {
			user: foo
			password: %s
			timeout: 1
		}
	}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(content, "", fatalPassword)))

	sb1, ob1 := RunServerWithConfig(conf)
	defer sb1.Shutdown()

	conf = createConfFile(t, []byte(fmt.Sprintf(content, fmt.Sprintf("routes: [nats://127.0.0.1:%d]", ob1.Cluster.Port), fatalPassword)))

	sb2, _ := RunServerWithConfig(conf)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)

	content = `
	port: -1
	accounts {
		foo {}
	}
	leafnodes {
		listen: "127.0.0.1:-1"
		remotes [
			{
				account: "foo"
				url: "nats://foo:%s@127.0.0.1:%d"
			}
		]
	}
	`
	conf = createConfFile(t, []byte(fmt.Sprintf(content, fatalPassword, ob1.LeafNode.Port)))

	sa, _ := RunServerWithConfig(conf)
	defer sa.Shutdown()

	l := testhelper.NewDummyLogger(100)
	sa.SetLogger(l, true, true) // we want debug & trace logs, to check for passwords in them

	checkLeafNodeConnected(t, sa)

	// Shutdown sb1, sa should reconnect to sb2
	sb1.Shutdown()

	// Wait a bit to make sure there was a disconnect and attempt to reconnect.
	time.Sleep(250 * time.Millisecond)

	// Should be able to reconnect
	checkLeafNodeConnected(t, sa)

	// Look at all our logs for the password; at time of writing it doesn't appear
	// but we want to safe-guard against it.
	l.CheckForProhibited(t, "fatal password", fatalPassword)
}

func TestLeafNodeRTT(t *testing.T) {
	ob := DefaultOptions()
	ob.PingInterval = 15 * time.Millisecond
	ob.LeafNode.Host = "127.0.0.1"
	ob.LeafNode.Port = -1
	sb := RunServer(ob)
	defer sb.Shutdown()

	lnBURL, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ob.LeafNode.Port))
	oa := DefaultOptions()
	oa.Cluster.Name = "xyz"
	oa.PingInterval = 15 * time.Millisecond
	oa.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{lnBURL}}}
	sa := RunServer(oa)
	defer sa.Shutdown()

	checkLeafNodeConnected(t, sa)

	checkRTT := func(t *testing.T, s *Server) time.Duration {
		t.Helper()
		var ln *client
		s.mu.Lock()
		for _, l := range s.leafs {
			ln = l
			break
		}
		s.mu.Unlock()
		var rtt time.Duration
		checkFor(t, 2*firstPingInterval, 15*time.Millisecond, func() error {
			ln.mu.Lock()
			rtt = ln.rtt
			ln.mu.Unlock()
			if rtt == 0 {
				return fmt.Errorf("RTT not tracked")
			}
			return nil
		})
		return rtt
	}

	prevA := checkRTT(t, sa)
	prevB := checkRTT(t, sb)

	// Wait to see if RTT is updated
	checkUpdated := func(t *testing.T, s *Server, prev time.Duration) {
		attempts := 0
		timeout := time.Now().Add(2 * firstPingInterval)
		for time.Now().Before(timeout) {
			if rtt := checkRTT(t, s); rtt != prev {
				return
			}
			attempts++
			if attempts == 5 {
				s.mu.Lock()
				for _, ln := range s.leafs {
					ln.mu.Lock()
					ln.rtt = 0
					ln.mu.Unlock()
					break
				}
				s.mu.Unlock()
			}
			time.Sleep(15 * time.Millisecond)
		}
		t.Fatalf("RTT probably not updated")
	}
	checkUpdated(t, sa, prevA)
	checkUpdated(t, sb, prevB)

	sa.Shutdown()
	sb.Shutdown()

	// Now check that initial RTT is computed prior to first PingInterval
	// Get new options to avoid possible race changing the ping interval.
	ob = DefaultOptions()
	ob.Cluster.Name = "xyz"
	ob.PingInterval = time.Minute
	ob.LeafNode.Host = "127.0.0.1"
	ob.LeafNode.Port = -1
	sb = RunServer(ob)
	defer sb.Shutdown()

	lnBURL, _ = url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ob.LeafNode.Port))
	oa = DefaultOptions()
	oa.PingInterval = time.Minute
	oa.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{lnBURL}}}
	sa = RunServer(oa)
	defer sa.Shutdown()

	checkLeafNodeConnected(t, sa)

	checkRTT(t, sa)
	checkRTT(t, sb)
}

func TestLeafNodeValidateAuthOptions(t *testing.T) {
	opts := DefaultOptions()
	opts.LeafNode.Username = "user1"
	opts.LeafNode.Password = "pwd"
	opts.LeafNode.Users = []*User{{Username: "user", Password: "pwd"}}
	if _, err := NewServer(opts); err == nil || !strings.Contains(err.Error(),
		"can not have a single user/pass and a users array") {
		t.Fatalf("Expected error about mixing single/multi users, got %v", err)
	}

	// Check duplicate user names
	opts.LeafNode.Username = _EMPTY_
	opts.LeafNode.Password = _EMPTY_
	opts.LeafNode.Users = append(opts.LeafNode.Users, &User{Username: "user", Password: "pwd"})
	if _, err := NewServer(opts); err == nil || !strings.Contains(err.Error(), "duplicate user") {
		t.Fatalf("Expected error about duplicate user, got %v", err)
	}
}

func TestLeafNodeBasicAuthSingleton(t *testing.T) {
	opts := DefaultOptions()
	opts.LeafNode.Port = -1
	opts.LeafNode.Account = "unknown"
	if s, err := NewServer(opts); err == nil || !strings.Contains(err.Error(), "cannot find") {
		if s != nil {
			s.Shutdown()
		}
		t.Fatalf("Expected error about account not found, got %v", err)
	}

	template := `
		port: -1
		accounts: {
			ACC1: { users = [{user: "user1", password: "user1"}] }
			ACC2: { users = [{user: "user2", password: "user2"}] }
		}
		leafnodes: {
			port: -1
			authorization {
			  %s
              account: "ACC1"
            }
		}
	`
	for iter, test := range []struct {
		name       string
		userSpec   string
		lnURLCreds string
		shouldFail bool
	}{
		{"no user creds required and no user so binds to ACC1", "", "", false},
		{"no user creds required and pick user2 associated to ACC2", "", "user2:user2@", false},
		{"no user creds required and unknown user should fail", "", "unknown:user@", true},
		{"user creds required so binds to ACC1", "user: \"ln\"\npass: \"pwd\"", "ln:pwd@", false},
	} {
		t.Run(test.name, func(t *testing.T) {

			conf := createConfFile(t, []byte(fmt.Sprintf(template, test.userSpec)))
			s1, o1 := RunServerWithConfig(conf)
			defer s1.Shutdown()

			// Create a sub on "foo" for account ACC1 (user user1), which is the one
			// bound to the accepted LN connection.
			ncACC1 := natsConnect(t, fmt.Sprintf("nats://user1:user1@%s:%d", o1.Host, o1.Port))
			defer ncACC1.Close()
			sub1 := natsSubSync(t, ncACC1, "foo")
			natsFlush(t, ncACC1)

			// Create a sub on "foo" for account ACC2 (user user2). This one should
			// not receive any message.
			ncACC2 := natsConnect(t, fmt.Sprintf("nats://user2:user2@%s:%d", o1.Host, o1.Port))
			defer ncACC2.Close()
			sub2 := natsSubSync(t, ncACC2, "foo")
			natsFlush(t, ncACC2)

			conf = createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				leafnodes: {
					remotes = [ { url: "nats-leaf://%s%s:%d" } ]
				}
			`, test.lnURLCreds, o1.LeafNode.Host, o1.LeafNode.Port)))
			s2, _ := RunServerWithConfig(conf)
			defer s2.Shutdown()

			if test.shouldFail {
				// Wait a bit and ensure that there is no leaf node connection
				time.Sleep(100 * time.Millisecond)
				checkFor(t, time.Second, 15*time.Millisecond, func() error {
					if n := s1.NumLeafNodes(); n != 0 {
						return fmt.Errorf("Expected no leafnode connection, got %v", n)
					}
					return nil
				})
				return
			}

			checkLeafNodeConnected(t, s2)

			nc := natsConnect(t, s2.ClientURL())
			defer nc.Close()
			natsPub(t, nc, "foo", []byte("hello"))
			// If url contains known user, even when there is no credentials
			// required, the connection will be bound to the user's account.
			if iter == 1 {
				// Should not receive on "ACC1", but should on "ACC2"
				if _, err := sub1.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("Expected timeout error, got %v", err)
				}
				natsNexMsg(t, sub2, time.Second)
			} else {
				// Should receive on "ACC1"...
				natsNexMsg(t, sub1, time.Second)
				// but not received on "ACC2" since leafnode bound to account "ACC1".
				if _, err := sub2.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("Expected timeout error, got %v", err)
				}
			}
		})
	}
}

func TestLeafNodeBasicAuthMultiple(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: -1
		accounts: {
			S1ACC1: { users = [{user: "user1", password: "user1"}] }
			S1ACC2: { users = [{user: "user2", password: "user2"}] }
		}
		leafnodes: {
			port: -1
			authorization {
			  users = [
				  {user: "ln1", password: "ln1", account: "S1ACC1"}
				  {user: "ln2", password: "ln2", account: "S1ACC2"}
				  {user: "ln3", password: "ln3"}
			  ]
            }
		}
	`))
	s1, o1 := RunServerWithConfig(conf)
	defer s1.Shutdown()

	// Make sure that we reject a LN connection if user does not match
	conf = createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes: {
			remotes = [{url: "nats-leaf://wron:user@%s:%d"}]
		}
	`, o1.LeafNode.Host, o1.LeafNode.Port)))
	s2, _ := RunServerWithConfig(conf)
	defer s2.Shutdown()
	// Give a chance for s2 to attempt to connect and make sure that s1
	// did not register a LN connection.
	time.Sleep(100 * time.Millisecond)
	if n := s1.NumLeafNodes(); n != 0 {
		t.Fatalf("Expected no leafnode connection, got %v", n)
	}
	s2.Shutdown()

	ncACC1 := natsConnect(t, fmt.Sprintf("nats://user1:user1@%s:%d", o1.Host, o1.Port))
	defer ncACC1.Close()
	sub1 := natsSubSync(t, ncACC1, "foo")
	natsFlush(t, ncACC1)

	ncACC2 := natsConnect(t, fmt.Sprintf("nats://user2:user2@%s:%d", o1.Host, o1.Port))
	defer ncACC2.Close()
	sub2 := natsSubSync(t, ncACC2, "foo")
	natsFlush(t, ncACC2)

	// We will start s2 with 2 LN connections that should bind local account S2ACC1
	// to account S1ACC1 and S2ACC2 to account S1ACC2 on s1.
	conf = createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		accounts {
			S2ACC1 { users = [{user: "user1", password: "user1"}] }
			S2ACC2 { users = [{user: "user2", password: "user2"}] }
		}
		leafnodes: {
			remotes = [
				{
					url: "nats-leaf://ln1:ln1@%s:%d"
					account: "S2ACC1"
				}
				{
					url: "nats-leaf://ln2:ln2@%s:%d"
					account: "S2ACC2"
				}
			]
		}
	`, o1.LeafNode.Host, o1.LeafNode.Port, o1.LeafNode.Host, o1.LeafNode.Port)))
	s2, o2 := RunServerWithConfig(conf)
	defer s2.Shutdown()

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if nln := s2.NumLeafNodes(); nln != 2 {
			return fmt.Errorf("Expected 2 connected leafnodes for server %q, got %d", s2.ID(), nln)
		}
		return nil
	})

	// Create a user connection on s2 that binds to S2ACC1 (use user1).
	nc1 := natsConnect(t, fmt.Sprintf("nats://user1:user1@%s:%d", o2.Host, o2.Port))
	defer nc1.Close()

	// Create an user connection on s2 that binds to S2ACC2 (use user2).
	nc2 := natsConnect(t, fmt.Sprintf("nats://user2:user2@%s:%d", o2.Host, o2.Port))
	defer nc2.Close()

	// Now if a message is published from nc1, sub1 should receive it since
	// their account are bound together.
	natsPub(t, nc1, "foo", []byte("hello"))
	natsNexMsg(t, sub1, time.Second)
	// But sub2 should not receive it since different account.
	if _, err := sub2.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Expected timeout error, got %v", err)
	}

	// Now use nc2 (S2ACC2) to publish
	natsPub(t, nc2, "foo", []byte("hello"))
	// Expect sub2 to receive and sub1 not to.
	natsNexMsg(t, sub2, time.Second)
	if _, err := sub1.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Expected timeout error, got %v", err)
	}

	// Now check that we don't panic if no account is specified for
	// a given user.
	conf = createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes: {
			remotes = [
				{ url: "nats-leaf://ln3:ln3@%s:%d" }
			]
		}
	`, o1.LeafNode.Host, o1.LeafNode.Port)))
	s3, _ := RunServerWithConfig(conf)
	defer s3.Shutdown()
}

type loopDetectedLogger struct {
	DummyLogger
	ch chan string
}

func (l *loopDetectedLogger) Errorf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "Loop") {
		select {
		case l.ch <- msg:
		default:
		}
	}
}

func TestLeafNodeLoop(t *testing.T) {
	test := func(t *testing.T, cluster bool) {
		// This test requires that we set the port to known value because
		// we want A point to B and B to A.
		oa := DefaultOptions()
		if !cluster {
			oa.Cluster.Port = 0
			oa.Cluster.Name = _EMPTY_
		}
		oa.LeafNode.ReconnectInterval = 10 * time.Millisecond
		oa.LeafNode.Port = 1234
		ub, _ := url.Parse("nats://127.0.0.1:5678")
		oa.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{ub}}}
		oa.LeafNode.connDelay = 50 * time.Millisecond
		sa := RunServer(oa)
		defer sa.Shutdown()

		l := &loopDetectedLogger{ch: make(chan string, 1)}
		sa.SetLogger(l, false, false)

		ob := DefaultOptions()
		if !cluster {
			ob.Cluster.Port = 0
			ob.Cluster.Name = _EMPTY_
		} else {
			ob.Cluster.Name = "xyz"
		}
		ob.LeafNode.ReconnectInterval = 10 * time.Millisecond
		ob.LeafNode.Port = 5678
		ua, _ := url.Parse("nats://127.0.0.1:1234")
		ob.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{ua}}}
		ob.LeafNode.connDelay = 50 * time.Millisecond
		sb := RunServer(ob)
		defer sb.Shutdown()

		select {
		case <-l.ch:
			// OK!
		case <-time.After(2 * time.Second):
			t.Fatalf("Did not get any error regarding loop")
		}

		sb.Shutdown()
		ob.Port = -1
		ob.Cluster.Port = -1
		ob.LeafNode.Remotes = nil
		sb = RunServer(ob)
		defer sb.Shutdown()

		checkLeafNodeConnected(t, sa)
	}
	t.Run("standalone", func(t *testing.T) { test(t, false) })
	t.Run("cluster", func(t *testing.T) { test(t, true) })
}

func TestLeafNodeLoopFromDAG(t *testing.T) {
	// We want B & C to point to A, A itself does not point to any other server.
	// We need to cancel clustering since now this will suppress on its own.
	oa := DefaultOptions()
	oa.ServerName = "A"
	oa.LeafNode.ReconnectInterval = 10 * time.Millisecond
	oa.LeafNode.Port = -1
	oa.Cluster = ClusterOpts{}
	sa := RunServer(oa)
	defer sa.Shutdown()

	ua, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", oa.LeafNode.Port))

	// B will point to A
	ob := DefaultOptions()
	ob.ServerName = "B"
	ob.LeafNode.ReconnectInterval = 10 * time.Millisecond
	ob.LeafNode.Port = -1
	ob.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{ua}}}
	ob.Cluster = ClusterOpts{}
	sb := RunServer(ob)
	defer sb.Shutdown()

	ub, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ob.LeafNode.Port))

	checkLeafNodeConnected(t, sa)
	checkLeafNodeConnected(t, sb)

	// C will point to A and B
	oc := DefaultOptions()
	oc.ServerName = "C"
	oc.LeafNode.ReconnectInterval = 10 * time.Millisecond
	oc.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{ua}}, {URLs: []*url.URL{ub}}}
	oc.LeafNode.connDelay = 100 * time.Millisecond // Allow logger to be attached before connecting.
	oc.Cluster = ClusterOpts{}
	sc := RunServer(oc)

	lc := &loopDetectedLogger{ch: make(chan string, 1)}
	sc.SetLogger(lc, false, false)

	// We should get an error.
	select {
	case <-lc.ch:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get any error regarding loop")
	}

	// C should not be connected to anything.
	checkLeafNodeConnectedCount(t, sc, 0)
	// A and B are connected to each other.
	checkLeafNodeConnectedCount(t, sa, 1)
	checkLeafNodeConnectedCount(t, sb, 1)

	// Shutdown C and restart without the loop.
	sc.Shutdown()
	oc.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{ub}}}
	sc = RunServer(oc)
	defer sc.Shutdown()

	checkLeafNodeConnectedCount(t, sa, 1)
	checkLeafNodeConnectedCount(t, sb, 2)
	checkLeafNodeConnectedCount(t, sc, 1)
}

func TestLeafNodeCloseTLSConnection(t *testing.T) {
	opts := DefaultOptions()
	opts.DisableShortFirstPing = true
	opts.LeafNode.Host = "127.0.0.1"
	opts.LeafNode.Port = -1
	opts.LeafNode.TLSTimeout = 100
	tc := &TLSConfigOpts{
		CertFile: "./configs/certs/server.pem",
		KeyFile:  "./configs/certs/key.pem",
		Insecure: true,
	}
	tlsConf, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating tls config: %v", err)
	}
	opts.LeafNode.TLSConfig = tlsConf
	opts.NoLog = true
	opts.NoSigs = true
	s := RunServer(opts)
	defer s.Shutdown()

	endpoint := fmt.Sprintf("%s:%d", opts.LeafNode.Host, opts.LeafNode.Port)
	conn, err := net.DialTimeout("tcp", endpoint, 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on dial: %v", err)
	}
	defer conn.Close()

	br := bufio.NewReaderSize(conn, 100)
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("Unexpected error reading INFO: %v", err)
	}

	tlsConn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
	defer tlsConn.Close()
	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("Unexpected error during handshake: %v", err)
	}
	connectOp := []byte("CONNECT {\"name\":\"leaf\",\"verbose\":false,\"pedantic\":false}\r\n")
	if _, err := tlsConn.Write(connectOp); err != nil {
		t.Fatalf("Unexpected error writing CONNECT: %v", err)
	}
	if _, err := tlsConn.Write([]byte("PING\r\n")); err != nil {
		t.Fatalf("Unexpected error writing PING: %v", err)
	}

	checkLeafNodeConnected(t, s)

	// Get leaf connection
	var leaf *client
	s.mu.Lock()
	for _, l := range s.leafs {
		leaf = l
		break
	}
	s.mu.Unlock()
	// Fill the buffer. We want to timeout on write so that nc.Close()
	// would block due to a write that cannot complete.
	buf := make([]byte, 64*1024)
	done := false
	for !done {
		leaf.nc.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err := leaf.nc.Write(buf); err != nil {
			done = true
		}
		leaf.nc.SetWriteDeadline(time.Time{})
	}
	ch := make(chan bool)
	go func() {
		select {
		case <-ch:
			return
		case <-time.After(3 * time.Second):
			fmt.Println("!!!! closeConnection is blocked, test will hang !!!")
			return
		}
	}()
	// Close the route
	leaf.closeConnection(SlowConsumerWriteDeadline)
	ch <- true
}

func TestLeafNodeTLSSaveName(t *testing.T) {
	opts := DefaultOptions()
	opts.LeafNode.Host = "127.0.0.1"
	opts.LeafNode.Port = -1
	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/server-noip.pem",
		KeyFile:  "../test/configs/certs/server-key-noip.pem",
		Insecure: true,
	}
	tlsConf, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating tls config: %v", err)
	}
	opts.LeafNode.TLSConfig = tlsConf
	s := RunServer(opts)
	defer s.Shutdown()

	lo := DefaultOptions()
	u, _ := url.Parse(fmt.Sprintf("nats://localhost:%d", opts.LeafNode.Port))
	lo.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	lo.LeafNode.ReconnectInterval = 15 * time.Millisecond
	ln := RunServer(lo)
	defer ln.Shutdown()

	// We know connection will fail, but it should not fail because of error such as:
	// "cannot validate certificate for 127.0.0.1 because it doesn't contain any IP SANs"
	// This would mean that we are not saving the hostname to use during the TLS handshake.

	le := &captureErrorLogger{errCh: make(chan string, 100)}
	ln.SetLogger(le, false, false)

	tm := time.NewTimer(time.Second)
	var done bool
	for !done {
		select {
		case err := <-le.errCh:
			if strings.Contains(err, "doesn't contain any IP SANs") {
				t.Fatalf("Got this error: %q", err)
			}
		case <-tm.C:
			done = true
		}
	}
}

func TestLeafNodeRemoteWrongPort(t *testing.T) {
	for _, test1 := range []struct {
		name              string
		clusterAdvertise  bool
		leafnodeAdvertise bool
	}{
		{"advertise_on", false, false},
		{"cluster_no_advertise", true, false},
		{"leafnode_no_advertise", false, true},
	} {
		t.Run(test1.name, func(t *testing.T) {
			oa := DefaultOptions()
			// Make sure we have all ports (client, route, gateway) and we will try
			// to create a leafnode to connection to each and make sure we get the error.
			oa.Cluster.NoAdvertise = test1.clusterAdvertise
			oa.Cluster.Name = "A"
			oa.Cluster.Host = "127.0.0.1"
			oa.Cluster.Port = -1
			oa.Gateway.Host = "127.0.0.1"
			oa.Gateway.Port = -1
			oa.Gateway.Name = "A"
			oa.LeafNode.Host = "127.0.0.1"
			oa.LeafNode.Port = -1
			oa.LeafNode.NoAdvertise = test1.leafnodeAdvertise
			oa.Accounts = []*Account{NewAccount("sys")}
			oa.SystemAccount = "sys"
			sa := RunServer(oa)
			defer sa.Shutdown()

			ob := DefaultOptions()
			ob.Cluster.NoAdvertise = test1.clusterAdvertise
			ob.Cluster.Name = "A"
			ob.Cluster.Host = "127.0.0.1"
			ob.Cluster.Port = -1
			ob.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", oa.Cluster.Host, oa.Cluster.Port))
			ob.Gateway.Host = "127.0.0.1"
			ob.Gateway.Port = -1
			ob.Gateway.Name = "A"
			ob.LeafNode.Host = "127.0.0.1"
			ob.LeafNode.Port = -1
			ob.LeafNode.NoAdvertise = test1.leafnodeAdvertise
			ob.Accounts = []*Account{NewAccount("sys")}
			ob.SystemAccount = "sys"
			sb := RunServer(ob)
			defer sb.Shutdown()

			checkClusterFormed(t, sa, sb)

			for _, test := range []struct {
				name string
				port int
			}{
				{"client", oa.Port},
				{"cluster", oa.Cluster.Port},
				{"gateway", oa.Gateway.Port},
			} {
				t.Run(test.name, func(t *testing.T) {
					oc := DefaultOptions()
					// Server with the wrong config against non leafnode port.
					leafURL, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", test.port))
					oc.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{leafURL}}}
					oc.LeafNode.ReconnectInterval = 5 * time.Millisecond
					sc := RunServer(oc)
					defer sc.Shutdown()
					l := &captureErrorLogger{errCh: make(chan string, 10)}
					sc.SetLogger(l, true, true)

					select {
					case e := <-l.errCh:
						if strings.Contains(e, ErrConnectedToWrongPort.Error()) {
							return
						}
					case <-time.After(2 * time.Second):
						t.Fatalf("Did not get any error about connecting to wrong port for %q - %q",
							test1.name, test.name)
					}
				})
			}
		})
	}
}

func TestLeafNodeRemoteIsHub(t *testing.T) {
	oa := testDefaultOptionsForGateway("A")
	oa.Accounts = []*Account{NewAccount("sys")}
	oa.SystemAccount = "sys"
	sa := RunServer(oa)
	defer sa.Shutdown()

	lno := DefaultOptions()
	lno.LeafNode.Host = "127.0.0.1"
	lno.LeafNode.Port = -1
	ln := RunServer(lno)
	defer ln.Shutdown()

	ob1 := testGatewayOptionsFromToWithServers(t, "B", "A", sa)
	ob1.Accounts = []*Account{NewAccount("sys")}
	ob1.SystemAccount = "sys"
	ob1.Cluster.Host = "127.0.0.1"
	ob1.Cluster.Port = -1
	ob1.LeafNode.Host = "127.0.0.1"
	ob1.LeafNode.Port = -1
	u, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", lno.LeafNode.Port))
	ob1.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs: []*url.URL{u},
			Hub:  true,
		},
	}
	sb1 := RunServer(ob1)
	defer sb1.Shutdown()

	waitForOutboundGateways(t, sb1, 1, 2*time.Second)
	waitForInboundGateways(t, sb1, 1, 2*time.Second)
	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)

	checkLeafNodeConnected(t, sb1)

	// For now, due to issue 977, let's restart the leafnode so that the
	// leafnode connect is propagated in the super-cluster.
	ln.Shutdown()
	ln = RunServer(lno)
	defer ln.Shutdown()
	checkLeafNodeConnected(t, sb1)

	// Connect another server in cluster B
	ob2 := testGatewayOptionsFromToWithServers(t, "B", "A", sa)
	ob2.Accounts = []*Account{NewAccount("sys")}
	ob2.SystemAccount = "sys"
	ob2.Cluster.Host = "127.0.0.1"
	ob2.Cluster.Port = -1
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", ob1.Cluster.Port))
	sb2 := RunServer(ob2)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)
	waitForOutboundGateways(t, sb2, 1, 2*time.Second)

	expectedSubs := ln.NumSubscriptions() + 2

	// Create sub on "foo" connected to sa
	ncA := natsConnect(t, sa.ClientURL())
	defer ncA.Close()
	subFoo := natsSubSync(t, ncA, "foo")

	// Create sub on "bar" connected to sb2
	ncB2 := natsConnect(t, sb2.ClientURL())
	defer ncB2.Close()
	subBar := natsSubSync(t, ncB2, "bar")

	// Make sure subscriptions have propagated to the leafnode.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if subs := ln.NumSubscriptions(); subs < expectedSubs {
			return fmt.Errorf("Number of subs is %d", subs)
		}
		return nil
	})

	// Create pub connection on leafnode
	ncLN := natsConnect(t, ln.ClientURL())
	defer ncLN.Close()

	// Publish on foo and make sure it is received.
	natsPub(t, ncLN, "foo", []byte("msg"))
	natsNexMsg(t, subFoo, time.Second)

	// Publish on foo and make sure it is received.
	natsPub(t, ncLN, "bar", []byte("msg"))
	natsNexMsg(t, subBar, time.Second)
}

func TestLeafNodePermissions(t *testing.T) {
	lo1 := DefaultOptions()
	lo1.LeafNode.Host = "127.0.0.1"
	lo1.LeafNode.Port = -1
	ln1 := RunServer(lo1)
	defer ln1.Shutdown()

	errLog := &captureErrorLogger{errCh: make(chan string, 1)}
	ln1.SetLogger(errLog, false, false)

	u, _ := url.Parse(fmt.Sprintf("nats://%s:%d", lo1.LeafNode.Host, lo1.LeafNode.Port))
	lo2 := DefaultOptions()
	lo2.Cluster.Name = "xyz"
	lo2.LeafNode.ReconnectInterval = 5 * time.Millisecond
	lo2.LeafNode.connDelay = 100 * time.Millisecond
	lo2.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs:        []*url.URL{u},
			DenyExports: []string{"export.*", "export"},
			DenyImports: []string{"import.*", "import"},
		},
	}
	ln2 := RunServer(lo2)
	defer ln2.Shutdown()

	checkLeafNodeConnected(t, ln1)

	// Create clients on ln1 and ln2
	nc1, err := nats.Connect(ln1.ClientURL())
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc1.Close()
	nc2, err := nats.Connect(ln2.ClientURL())
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc2.Close()

	checkSubs := func(acc *Account, expected int) {
		t.Helper()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			if n := acc.TotalSubs(); n != expected {
				return fmt.Errorf("Expected %d subs, got %v", expected, n)
			}
			return nil
		})
	}

	// Create a sub on ">" on LN1
	subAll := natsSubSync(t, nc1, ">")
	// this should be registered in LN2 (there is 1 sub for LN1 $LDS subject) + SYS IMPORTS
	checkSubs(ln2.globalAccount(), 12)

	// Check deny export clause from messages published from LN2
	for _, test := range []struct {
		name     string
		subject  string
		received bool
	}{
		{"do not send on export.bat", "export.bat", false},
		{"do not send on export", "export", false},
		{"send on foo", "foo", true},
		{"send on export.this.one", "export.this.one", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			nc2.Publish(test.subject, []byte("msg"))
			if test.received {
				natsNexMsg(t, subAll, time.Second)
			} else {
				if _, err := subAll.NextMsg(50 * time.Millisecond); err == nil {
					t.Fatalf("Should not have received message on %q", test.subject)
				}
			}
		})
	}

	subAll.Unsubscribe()
	// Goes down by 1.
	checkSubs(ln2.globalAccount(), 11)

	// We used to make sure we would not do subscriptions however that
	// was incorrect. We need to check publishes, not the subscriptions.
	// For instance if we can publish across a leafnode to foo, and the
	// other side has a subsxcription for '*' the message should cross
	// the leafnode. The old way would not allow this.

	// Now check deny import clause.
	// As of now, we don't suppress forwarding of subscriptions on LN2 that
	// match the deny import clause to be forwarded to LN1. However, messages
	// should still not be able to come back to LN2.
	for _, test := range []struct {
		name       string
		subSubject string
		pubSubject string
		ok         bool
	}{
		{"reject import on import.*", "import.*", "import.bad", false},
		{"reject import on import", "import", "import", false},
		{"accepts import on foo", "foo", "foo", true},
		{"accepts import on import.this.one.ok", "import.*.>", "import.this.one.ok", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sub := natsSubSync(t, nc2, test.subSubject)
			checkSubs(ln2.globalAccount(), 12)

			if !test.ok {
				nc1.Publish(test.pubSubject, []byte("msg"))
				if _, err := sub.NextMsg(50 * time.Millisecond); err == nil {
					t.Fatalf("Did not expect to get the message")
				}
			} else {
				checkSubs(ln1.globalAccount(), 11)
				nc1.Publish(test.pubSubject, []byte("msg"))
				natsNexMsg(t, sub, time.Second)
			}
			sub.Unsubscribe()
			checkSubs(ln1.globalAccount(), 10)
		})
	}
}

func TestLeafNodePermissionsConcurrentAccess(t *testing.T) {
	lo1 := DefaultOptions()
	lo1.LeafNode.Host = "127.0.0.1"
	lo1.LeafNode.Port = -1
	ln1 := RunServer(lo1)
	defer ln1.Shutdown()

	nc1 := natsConnect(t, ln1.ClientURL())
	defer nc1.Close()

	natsSub(t, nc1, "_INBOX.>", func(_ *nats.Msg) {})
	natsFlush(t, nc1)

	ch := make(chan struct{}, 1)
	wg := sync.WaitGroup{}
	wg.Add(2)

	publish := func(nc *nats.Conn) {
		defer wg.Done()

		for {
			select {
			case <-ch:
				return
			default:
				nc.Publish(nats.NewInbox(), []byte("hello"))
			}
		}
	}

	go publish(nc1)

	u, _ := url.Parse(fmt.Sprintf("nats://%s:%d", lo1.LeafNode.Host, lo1.LeafNode.Port))
	lo2 := DefaultOptions()
	lo2.Cluster.Name = "xyz"
	lo2.LeafNode.ReconnectInterval = 5 * time.Millisecond
	lo2.LeafNode.connDelay = 500 * time.Millisecond
	lo2.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs:        []*url.URL{u},
			DenyExports: []string{"foo"},
			DenyImports: []string{"bar"},
		},
	}
	ln2 := RunServer(lo2)
	defer ln2.Shutdown()

	nc2 := natsConnect(t, ln2.ClientURL())
	defer nc2.Close()

	natsSub(t, nc2, "_INBOX.>", func(_ *nats.Msg) {})
	natsFlush(t, nc2)

	go publish(nc2)

	checkLeafNodeConnected(t, ln1)
	checkLeafNodeConnected(t, ln2)

	time.Sleep(50 * time.Millisecond)
	close(ch)
	wg.Wait()
}

func TestLeafNodePubAllowedPruning(t *testing.T) {
	c := &client{}
	c.setPermissions(&Permissions{Publish: &SubjectPermission{Allow: []string{"foo"}}})

	gr := 100
	wg := sync.WaitGroup{}
	wg.Add(gr)
	for i := 0; i < gr; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				c.pubAllowed(nats.NewInbox())
			}
		}()
	}

	wg.Wait()
	if n := int(atomic.LoadInt32(&c.perms.pcsz)); n > maxPermCacheSize {
		t.Fatalf("Expected size to be less than %v, got %v", maxPermCacheSize, n)
	}
	if n := atomic.LoadInt32(&c.perms.prun); n != 0 {
		t.Fatalf("c.perms.prun should be 0, was %v", n)
	}
}

func TestLeafNodeExportPermissionsNotForSpecialSubs(t *testing.T) {
	lo1 := DefaultOptions()
	lo1.Accounts = []*Account{NewAccount("SYS")}
	lo1.SystemAccount = "SYS"
	lo1.Cluster.Name = "A"
	lo1.Gateway.Name = "A"
	lo1.Gateway.Port = -1
	lo1.LeafNode.Host = "127.0.0.1"
	lo1.LeafNode.Port = -1
	ln1 := RunServer(lo1)
	defer ln1.Shutdown()

	u, _ := url.Parse(fmt.Sprintf("nats://%s:%d", lo1.LeafNode.Host, lo1.LeafNode.Port))
	lo2 := DefaultOptions()
	lo2.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs:        []*url.URL{u},
			DenyExports: []string{">"},
		},
	}
	ln2 := RunServer(lo2)
	defer ln2.Shutdown()

	checkLeafNodeConnected(t, ln1)

	// The deny is totally restrictive, but make sure that we still accept the $LDS, $GR and _GR_ go from LN1.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		// We should have registered the 3 subs from the accepting leafnode.
		if n := ln2.globalAccount().TotalSubs(); n != 9 {
			return fmt.Errorf("Expected %d subs, got %v", 9, n)
		}
		return nil
	})
}

// Make sure that if the node that detects the loop (and sends the error and
// close the connection) is the accept side, the remote node (the one that solicits)
// properly use the reconnect delay.
func TestLeafNodeLoopDetectedOnAcceptSide(t *testing.T) {
	bo := DefaultOptions()
	bo.LeafNode.Host = "127.0.0.1"
	bo.LeafNode.Port = -1
	b := RunServer(bo)
	defer b.Shutdown()

	l := &loopDetectedLogger{ch: make(chan string, 1)}
	b.SetLogger(l, false, false)

	u, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", bo.LeafNode.Port))

	ao := testDefaultOptionsForGateway("A")
	ao.Accounts = []*Account{NewAccount("SYS")}
	ao.SystemAccount = "SYS"
	ao.LeafNode.ReconnectInterval = 5 * time.Millisecond
	ao.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs: []*url.URL{u},
			Hub:  true,
		},
	}
	a := RunServer(ao)
	defer a.Shutdown()

	co := testGatewayOptionsFromToWithServers(t, "C", "A", a)
	co.Accounts = []*Account{NewAccount("SYS")}
	co.SystemAccount = "SYS"
	co.LeafNode.ReconnectInterval = 5 * time.Millisecond
	co.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs: []*url.URL{u},
			Hub:  true,
		},
	}
	c := RunServer(co)
	defer c.Shutdown()

	for i := 0; i < 2; i++ {
		select {
		case <-l.ch:
			// OK
		case <-time.After(200 * time.Millisecond):
			// We are likely to detect from each A and C servers,
			// but consider a failure if we did not receive any.
			if i == 0 {
				t.Fatalf("Should have detected loop")
			}
		}
	}

	// The reconnect attempt is set to 5ms, but the default loop delay
	// is 30 seconds, so we should not get any new error for that long.
	// Check if we are getting more errors..
	select {
	case e := <-l.ch:
		t.Fatalf("Should not have gotten another error, got %q", e)
	case <-time.After(50 * time.Millisecond):
		// OK!
	}
}

func TestLeafNodeHubWithGateways(t *testing.T) {
	ao := DefaultOptions()
	ao.ServerName = "A"
	ao.LeafNode.Host = "127.0.0.1"
	ao.LeafNode.Port = -1
	a := RunServer(ao)
	defer a.Shutdown()

	ua, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ao.LeafNode.Port))

	bo := testDefaultOptionsForGateway("B")
	bo.ServerName = "B"
	bo.Accounts = []*Account{NewAccount("SYS")}
	bo.SystemAccount = "SYS"
	bo.LeafNode.ReconnectInterval = 5 * time.Millisecond
	bo.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs: []*url.URL{ua},
			Hub:  true,
		},
	}
	b := RunServer(bo)
	defer b.Shutdown()

	do := DefaultOptions()
	do.ServerName = "D"
	do.LeafNode.Host = "127.0.0.1"
	do.LeafNode.Port = -1
	d := RunServer(do)
	defer d.Shutdown()

	ud, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", do.LeafNode.Port))

	co := testGatewayOptionsFromToWithServers(t, "C", "B", b)
	co.ServerName = "C"
	co.Accounts = []*Account{NewAccount("SYS")}
	co.SystemAccount = "SYS"
	co.LeafNode.ReconnectInterval = 5 * time.Millisecond
	co.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs: []*url.URL{ud},
			Hub:  true,
		},
	}
	c := RunServer(co)
	defer c.Shutdown()

	waitForInboundGateways(t, b, 1, 2*time.Second)
	waitForInboundGateways(t, c, 1, 2*time.Second)
	checkLeafNodeConnected(t, a)
	checkLeafNodeConnected(t, d)

	// Create a responder on D
	ncD := natsConnect(t, d.ClientURL())
	defer ncD.Close()

	ncD.Subscribe("service", func(m *nats.Msg) {
		m.Respond([]byte("reply"))
	})
	ncD.Flush()

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		acc := a.globalAccount()
		if r := acc.sl.Match("service"); r != nil && len(r.psubs) == 1 {
			return nil
		}
		return fmt.Errorf("subscription still not registered")
	})

	// Create requestor on A and send the request, expect a reply.
	ncA := natsConnect(t, a.ClientURL())
	defer ncA.Close()
	if msg, err := ncA.Request("service", []byte("request"), time.Second); err != nil {
		t.Fatalf("Failed to get reply: %v", err)
	} else if string(msg.Data) != "reply" {
		t.Fatalf("Unexpected reply: %q", msg.Data)
	}
}

func TestLeafNodeTmpClients(t *testing.T) {
	ao := DefaultOptions()
	ao.LeafNode.Host = "127.0.0.1"
	ao.LeafNode.Port = -1
	a := RunServer(ao)
	defer a.Shutdown()

	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", ao.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer c.Close()
	// Read info
	br := bufio.NewReader(c)
	br.ReadLine()

	checkTmp := func(expected int) {
		t.Helper()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			a.grMu.Lock()
			l := len(a.grTmpClients)
			a.grMu.Unlock()
			if l != expected {
				return fmt.Errorf("Expected tmp map to have %v entries, got %v", expected, l)
			}
			return nil
		})
	}
	checkTmp(1)

	// Close client and wait check that it is removed.
	c.Close()
	checkTmp(0)

	// Check with normal leafnode connection that once connected,
	// the tmp map is also emptied.
	bo := DefaultOptions()
	bo.Cluster.Name = "xyz"
	bo.LeafNode.ReconnectInterval = 5 * time.Millisecond
	u, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ao.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error creating url: %v", err)
	}
	bo.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	b := RunServer(bo)
	defer b.Shutdown()

	checkLeafNodeConnected(t, b)
	checkTmp(0)
}

func TestLeafNodeTLSVerifyAndMap(t *testing.T) {
	accName := "MyAccount"
	acc := NewAccount(accName)
	certUserName := "CN=example.com,OU=NATS.io"
	users := []*User{{Username: certUserName, Account: acc}}

	for _, test := range []struct {
		name        string
		leafUsers   bool
		provideCert bool
	}{
		{"no users override, provides cert", false, true},
		{"no users override, does not provide cert", false, false},
		{"users override, provides cert", true, true},
		{"users override, does not provide cert", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := DefaultOptions()
			o.Accounts = []*Account{acc}
			o.LeafNode.Host = "127.0.0.1"
			o.LeafNode.Port = -1
			if test.leafUsers {
				o.LeafNode.Users = users
			} else {
				o.Users = users
			}
			tc := &TLSConfigOpts{
				CertFile: "../test/configs/certs/tlsauth/server.pem",
				KeyFile:  "../test/configs/certs/tlsauth/server-key.pem",
				CaFile:   "../test/configs/certs/tlsauth/ca.pem",
				Verify:   true,
			}
			tlsc, err := GenTLSConfig(tc)
			if err != nil {
				t.Fatalf("Error creating tls config: %v", err)
			}
			o.LeafNode.TLSConfig = tlsc
			o.LeafNode.TLSMap = true
			s := RunServer(o)
			defer s.Shutdown()

			slo := DefaultOptions()
			slo.Cluster.Name = "xyz"

			sltlsc := &tls.Config{}
			if test.provideCert {
				tc := &TLSConfigOpts{
					CertFile: "../test/configs/certs/tlsauth/client.pem",
					KeyFile:  "../test/configs/certs/tlsauth/client-key.pem",
				}
				var err error
				sltlsc, err = GenTLSConfig(tc)
				if err != nil {
					t.Fatalf("Error generating tls config: %v", err)
				}
			}
			sltlsc.InsecureSkipVerify = true
			u, _ := url.Parse(fmt.Sprintf("nats://%s:%d", o.LeafNode.Host, o.LeafNode.Port))
			slo.LeafNode.Remotes = []*RemoteLeafOpts{
				{
					TLSConfig: sltlsc,
					URLs:      []*url.URL{u},
				},
			}
			sl := RunServer(slo)
			defer sl.Shutdown()

			if !test.provideCert {
				// Wait a bit and make sure we are not connecting
				time.Sleep(100 * time.Millisecond)
				checkLeafNodeConnectedCount(t, s, 0)
				return
			}
			checkLeafNodeConnected(t, s)

			var uname string
			var accname string
			s.mu.Lock()
			for _, c := range s.leafs {
				c.mu.Lock()
				uname = c.opts.Username
				if c.acc != nil {
					accname = c.acc.GetName()
				}
				c.mu.Unlock()
			}
			s.mu.Unlock()
			if uname != certUserName {
				t.Fatalf("Expected username %q, got %q", certUserName, uname)
			}
			if accname != accName {
				t.Fatalf("Expected account %q, got %v", accName, accname)
			}
		})
	}
}

type chanLogger struct {
	DummyLogger
	triggerChan chan string
}

func (l *chanLogger) Warnf(format string, v ...interface{}) {
	l.triggerChan <- fmt.Sprintf(format, v...)
}

func (l *chanLogger) Errorf(format string, v ...interface{}) {
	l.triggerChan <- fmt.Sprintf(format, v...)
}

const (
	testLeafNodeTLSVerifyAndMapSrvA = `
listen: 127.0.0.1:-1
leaf {
	listen: "127.0.0.1:-1"
	tls {
		cert_file: "../test/configs/certs/server-cert.pem"
		key_file:  "../test/configs/certs/server-key.pem"
		ca_file:   "../test/configs/certs/ca.pem"
		timeout: 2
		verify_and_map: true
	}
	authorization {
		users [{
			user: "%s"
		}]
	}
}
`
	testLeafNodeTLSVerifyAndMapSrvB = `
listen: -1
leaf {
	remotes [
		{
			url: "tls://user-provided-in-url@localhost:%d"
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file:  "../test/configs/certs/server-key.pem"
				ca_file:   "../test/configs/certs/ca.pem"
			}
		}
	]
}`
)

func TestLeafNodeTLSVerifyAndMapCfgPass(t *testing.T) {
	l := &chanLogger{triggerChan: make(chan string, 100)}
	defer close(l.triggerChan)

	confA := createConfFile(t, []byte(fmt.Sprintf(testLeafNodeTLSVerifyAndMapSrvA, "localhost")))
	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()
	srvA.SetLogger(l, true, true)

	confB := createConfFile(t, []byte(fmt.Sprintf(testLeafNodeTLSVerifyAndMapSrvB, optsA.LeafNode.Port)))
	ob := LoadConfig(confB)
	ob.LeafNode.ReconnectInterval = 50 * time.Millisecond
	srvB := RunServer(ob)
	defer srvB.Shutdown()

	// Now make sure that the leaf node connection is up and the correct account was picked
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		for _, srv := range []*Server{srvA, srvB} {
			if nln := srv.NumLeafNodes(); nln != 1 {
				return fmt.Errorf("Number of leaf nodes is %d", nln)
			}
			if leafz, err := srv.Leafz(nil); err != nil {
				if len(leafz.Leafs) != 1 {
					return fmt.Errorf("Number of leaf nodes returned by LEAFZ is not one: %d", len(leafz.Leafs))
				} else if leafz.Leafs[0].Account != DEFAULT_GLOBAL_ACCOUNT {
					return fmt.Errorf("Account used is not $G: %s", leafz.Leafs[0].Account)
				}
			}
		}
		return nil
	})
	// Make sure that the user name in the url was ignored and a warning printed
	for {
		select {
		case w := <-l.triggerChan:
			if w == `User "user-provided-in-url" found in connect proto, but user required from cert` {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Did not get expected warning")
		}
	}
}

func TestLeafNodeTLSVerifyAndMapCfgFail(t *testing.T) {
	l := &chanLogger{triggerChan: make(chan string, 100)}
	defer close(l.triggerChan)

	// use certificate with SAN localhost, but configure the server to not accept it
	// instead provide a name matching the user (to be matched by failed
	confA := createConfFile(t, []byte(fmt.Sprintf(testLeafNodeTLSVerifyAndMapSrvA, "user-provided-in-url")))
	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()
	srvA.SetLogger(l, true, true)

	confB := createConfFile(t, []byte(fmt.Sprintf(testLeafNodeTLSVerifyAndMapSrvB, optsA.LeafNode.Port)))
	ob := LoadConfig(confB)
	ob.LeafNode.ReconnectInterval = 50 * time.Millisecond
	srvB := RunServer(ob)
	defer srvB.Shutdown()

	// Now make sure that the leaf node connection is down
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		for _, srv := range []*Server{srvA, srvB} {
			if nln := srv.NumLeafNodes(); nln != 0 {
				return fmt.Errorf("Number of leaf nodes is %d", nln)
			}
		}
		return nil
	})
	// Make sure that the connection was closed for the right reason
	for {
		select {
		case w := <-l.triggerChan:
			if strings.Contains(w, ErrAuthentication.Error()) {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Did not get expected warning")
		}
	}
}

func TestLeafNodeOriginClusterInfo(t *testing.T) {
	hopts := DefaultOptions()
	hopts.ServerName = "hub"
	hopts.LeafNode.Port = -1

	hub := RunServer(hopts)
	defer hub.Shutdown()

	conf := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leaf {
			remotes [ { url: "nats://127.0.0.1:%d" } ]
		}
	`, hopts.LeafNode.Port)))

	opts, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	opts.NoLog, opts.NoSigs = true, true

	s := RunServer(opts)
	defer s.Shutdown()

	checkLeafNodeConnected(t, s)

	// Check the info on the leadnode client in the hub.
	grabLeaf := func() *client {
		var l *client
		hub.mu.Lock()
		for _, l = range hub.leafs {
			break
		}
		hub.mu.Unlock()
		return l
	}

	l := grabLeaf()
	if rc := l.remoteCluster(); rc != "" {
		t.Fatalf("Expected an empty remote cluster, got %q", rc)
	}

	s.Shutdown()

	// Now make our leafnode part of a cluster.
	conf = createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leaf {
			remotes [ { url: "nats://127.0.0.1:%d" } ]
		}
		cluster {
			name: "xyz"
			listen: "127.0.0.1:-1"
		}
	`, hopts.LeafNode.Port)))

	opts, err = ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	opts.NoLog, opts.NoSigs = true, true

	s = RunServer(opts)
	defer s.Shutdown()

	checkLeafNodeConnected(t, s)

	l = grabLeaf()
	if rc := l.remoteCluster(); rc != "xyz" {
		t.Fatalf("Expected a remote cluster name of \"xyz\", got %q", rc)
	}
	pcid := l.cid

	// Now make sure that if we update our cluster name, simulating the settling
	// of dynamic cluster names between competing servers.
	s.setClusterName("xyz")
	// Make sure we disconnect and reconnect.
	checkLeafNodeConnectedCount(t, s, 0)
	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, hub)

	l = grabLeaf()
	if rc := l.remoteCluster(); rc != "xyz" {
		t.Fatalf("Expected a remote cluster name of \"xyz\", got %q", rc)
	}
	// Make sure we reconnected and have a new CID.
	if l.cid == pcid {
		t.Fatalf("Expected a different id, got the same")
	}
}

type proxyAcceptDetectFailureLate struct {
	sync.Mutex
	wg         sync.WaitGroup
	acceptPort int
	l          net.Listener
	srvs       []net.Conn
	leaf       net.Conn
	startChan  chan struct{}
}

func (p *proxyAcceptDetectFailureLate) run(t *testing.T) int {
	return p.runEx(t, false)
}

func (p *proxyAcceptDetectFailureLate) runEx(t *testing.T, needStart bool) int {
	l, err := natsListen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Error on listen: %v", err)
	}
	p.Lock()
	var startChan chan struct{}
	if needStart {
		startChan = make(chan struct{})
		p.startChan = startChan
	}
	p.l = l
	p.Unlock()
	port := l.Addr().(*net.TCPAddr).Port
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer l.Close()
		defer func() {
			p.Lock()
			for _, c := range p.srvs {
				c.Close()
			}
			p.Unlock()
		}()
		if startChan != nil {
			<-startChan
		}
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			srv, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", p.acceptPort))
			if err != nil {
				return
			}
			p.Lock()
			p.leaf = c
			p.srvs = append(p.srvs, srv)
			p.Unlock()

			transfer := func(c1, c2 net.Conn) {
				var buf [1024]byte
				for {
					n, err := c1.Read(buf[:])
					if err != nil {
						return
					}
					if _, err := c2.Write(buf[:n]); err != nil {
						return
					}
				}
			}

			go transfer(srv, c)
			go transfer(c, srv)
		}
	}()
	return port
}

func (p *proxyAcceptDetectFailureLate) start() {
	p.Lock()
	if p.startChan != nil {
		close(p.startChan)
		p.startChan = nil
	}
	p.Unlock()
}

func (p *proxyAcceptDetectFailureLate) close() {
	p.Lock()
	if p.startChan != nil {
		close(p.startChan)
		p.startChan = nil
	}
	p.l.Close()
	p.Unlock()

	p.wg.Wait()
}

type oldConnReplacedLogger struct {
	DummyLogger
	errCh  chan string
	warnCh chan string
}

func (l *oldConnReplacedLogger) Errorf(format string, v ...interface{}) {
	select {
	case l.errCh <- fmt.Sprintf(format, v...):
	default:
	}
}

func (l *oldConnReplacedLogger) Warnf(format string, v ...interface{}) {
	select {
	case l.warnCh <- fmt.Sprintf(format, v...):
	default:
	}
}

// This test will simulate that the accept side does not detect the connection
// has been closed early enough. The soliciting side will attempt to reconnect
// and we should not be getting the "loop detected" error.
func TestLeafNodeLoopDetectedDueToReconnect(t *testing.T) {
	o := DefaultOptions()
	o.LeafNode.Host = "127.0.0.1"
	o.LeafNode.Port = -1
	s := RunServer(o)
	defer s.Shutdown()

	l := &oldConnReplacedLogger{errCh: make(chan string, 10), warnCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	p := &proxyAcceptDetectFailureLate{acceptPort: o.LeafNode.Port}
	defer p.close()
	port := p.run(t)

	aurl, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}
	ol := DefaultOptions()
	ol.Cluster.Name = "cde"
	ol.LeafNode.ReconnectInterval = 50 * time.Millisecond
	ol.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{aurl}}}
	sl := RunServer(ol)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, sl)

	// Cause disconnect client side...
	p.Lock()
	p.leaf.Close()
	p.Unlock()

	// Make sure we did not get the loop detected error
	select {
	case e := <-l.errCh:
		if strings.Contains(e, "Loop detected") {
			t.Fatalf("Loop detected: %v", e)
		}
	case <-time.After(250 * time.Millisecond):
		// We are ok
	}

	// Now make sure we got the warning
	select {
	case w := <-l.warnCh:
		if !strings.Contains(w, "Replacing connection from same server") {
			t.Fatalf("Unexpected warning: %v", w)
		}
	case <-time.After(time.Second):
		t.Fatal("Did not get expected warning")
	}

	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, sl)
}

func TestLeafNodeTwoRemotesBindToSameHubAccount(t *testing.T) {
	opts := DefaultOptions()
	opts.LeafNode.Host = "127.0.0.1"
	opts.LeafNode.Port = -1
	s := RunServer(opts)
	defer s.Shutdown()

	for _, test := range []struct {
		name    string
		account string
		fail    bool
	}{
		{"different local accounts", "b", false},
		{"same local accounts", "a", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := `
			listen: 127.0.0.1:-1
			cluster { name: ln22, listen: 127.0.0.1:-1 }
			accounts {
				a { users [ {user: a, password: a} ]}
				b { users [ {user: b, password: b} ]}
			}
			leafnodes {
				remotes = [
					{
						url: nats-leaf://127.0.0.1:%[1]d
						account: a
					}
					{
						url: nats-leaf://127.0.0.1:%[1]d
						account: %s
					}
				]
			}
			`
			lconf := createConfFile(t, []byte(fmt.Sprintf(conf, opts.LeafNode.Port, test.account)))

			lopts, err := ProcessConfigFile(lconf)
			if err != nil {
				t.Fatalf("Error loading config file: %v", err)
			}
			lopts.NoLog = false
			ln, err := NewServer(lopts)
			if err != nil {
				t.Fatalf("Error creating server: %v", err)
			}
			defer ln.Shutdown()
			l := &captureErrorLogger{errCh: make(chan string, 10)}
			ln.SetLogger(l, false, false)

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				ln.Start()
			}()

			select {
			case err := <-l.errCh:
				if test.fail && !strings.Contains(err, DuplicateRemoteLeafnodeConnection.String()) {
					t.Fatalf("Did not get expected duplicate connection error: %v", err)
				} else if !test.fail && strings.Contains(err, DuplicateRemoteLeafnodeConnection.String()) {
					t.Fatalf("Incorrectly detected a duplicate connection: %v", err)
				}
			case <-time.After(250 * time.Millisecond):
				if test.fail {
					t.Fatal("Did not get expected error")
				}
			}
			ln.Shutdown()
			wg.Wait()
		})
	}
}

func TestLeafNodeNoDuplicateWithinCluster(t *testing.T) {
	// This set the cluster name to "abc"
	oSrv1 := DefaultOptions()
	oSrv1.ServerName = "srv1"
	oSrv1.LeafNode.Host = "127.0.0.1"
	oSrv1.LeafNode.Port = -1
	srv1 := RunServer(oSrv1)
	defer srv1.Shutdown()

	u, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", oSrv1.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}

	oLeaf1 := DefaultOptions()
	oLeaf1.ServerName = "leaf1"
	oLeaf1.Cluster.Name = "xyz"
	oLeaf1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	leaf1 := RunServer(oLeaf1)
	defer leaf1.Shutdown()

	leaf1ClusterURL := fmt.Sprintf("nats://127.0.0.1:%d", oLeaf1.Cluster.Port)

	oLeaf2 := DefaultOptions()
	oLeaf2.ServerName = "leaf2"
	oLeaf2.Cluster.Name = "xyz"
	oLeaf2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	oLeaf2.Routes = RoutesFromStr(leaf1ClusterURL)
	leaf2 := RunServer(oLeaf2)
	defer leaf2.Shutdown()

	checkClusterFormed(t, leaf1, leaf2)

	checkLeafNodeConnectedCount(t, srv1, 2)
	checkLeafNodeConnected(t, leaf1)
	checkLeafNodeConnected(t, leaf2)

	ncSrv1 := natsConnect(t, srv1.ClientURL())
	defer ncSrv1.Close()
	natsQueueSub(t, ncSrv1, "foo", "queue", func(m *nats.Msg) {
		m.Respond([]byte("from srv1"))
	})

	ncLeaf1 := natsConnect(t, leaf1.ClientURL())
	defer ncLeaf1.Close()
	natsQueueSub(t, ncLeaf1, "foo", "queue", func(m *nats.Msg) {
		m.Respond([]byte("from leaf1"))
	})

	ncLeaf2 := natsConnect(t, leaf2.ClientURL())
	defer ncLeaf2.Close()

	// Check that "foo" interest is available everywhere.
	// For this test, we want to make sure that the 2 queue subs are
	// registered on all servers, so we don't use checkSubInterest
	// which would simply return "true" if there is any interest on "foo".
	servers := []*Server{srv1, leaf1, leaf2}
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		for _, s := range servers {
			acc, err := s.LookupAccount(globalAccountName)
			if err != nil {
				return err
			}
			acc.mu.RLock()
			r := acc.sl.Match("foo")
			ok := len(r.qsubs) == 1 && len(r.qsubs[0]) == 2
			acc.mu.RUnlock()
			if !ok {
				return fmt.Errorf("interest not propagated on %q", s.Name())
			}
		}
		return nil
	})

	// Send requests (from leaf2). For this test to make sure that
	// there is no duplicate, we want to make sure that we check for
	// multiple replies and that the reply subject subscription has
	// been propagated everywhere.
	sub := natsSubSync(t, ncLeaf2, "reply_subj")
	natsFlush(t, ncLeaf2)

	// Here we have a single sub on "reply_subj" so using checkSubInterest is ok.
	checkSubInterest(t, srv1, globalAccountName, "reply_subj", time.Second)
	checkSubInterest(t, leaf1, globalAccountName, "reply_subj", time.Second)
	checkSubInterest(t, leaf2, globalAccountName, "reply_subj", time.Second)

	for i := 0; i < 5; i++ {
		// Now send the request
		natsPubReq(t, ncLeaf2, "foo", sub.Subject, []byte("req"))
		// Check that we get the reply
		replyMsg := natsNexMsg(t, sub, time.Second)
		// But make sure we received only 1!
		if otherReply, _ := sub.NextMsg(100 * time.Millisecond); otherReply != nil {
			t.Fatalf("Received duplicate reply, first was %q, followed by %q",
				replyMsg.Data, otherReply.Data)
		}
		// We also should have preferred the queue sub that is in the leaf cluster.
		if string(replyMsg.Data) != "from leaf1" {
			t.Fatalf("Expected reply from leaf1, got %q", replyMsg.Data)
		}
	}
}

func TestLeafNodeLMsgSplit(t *testing.T) {
	// This set the cluster name to "abc"
	oSrv1 := DefaultOptions()
	oSrv1.LeafNode.Host = "127.0.0.1"
	oSrv1.LeafNode.Port = -1
	srv1 := RunServer(oSrv1)
	defer srv1.Shutdown()

	oSrv2 := DefaultOptions()
	oSrv2.LeafNode.Host = "127.0.0.1"
	oSrv2.LeafNode.Port = -1
	oSrv2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", oSrv1.Cluster.Port))
	srv2 := RunServer(oSrv2)
	defer srv2.Shutdown()

	checkClusterFormed(t, srv1, srv2)

	u1, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", oSrv1.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}
	u2, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", oSrv2.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}

	oLeaf1 := DefaultOptions()
	oLeaf1.Cluster.Name = "xyz"
	oLeaf1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u1, u2}}}
	leaf1 := RunServer(oLeaf1)
	defer leaf1.Shutdown()

	oLeaf2 := DefaultOptions()
	oLeaf2.Cluster.Name = "xyz"
	oLeaf2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u1, u2}}}
	oLeaf2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", oLeaf1.Cluster.Port))
	leaf2 := RunServer(oLeaf2)
	defer leaf2.Shutdown()

	checkClusterFormed(t, leaf1, leaf2)

	checkLeafNodeConnected(t, leaf1)
	checkLeafNodeConnected(t, leaf2)

	ncSrv2 := natsConnect(t, srv2.ClientURL())
	defer ncSrv2.Close()
	natsQueueSub(t, ncSrv2, "foo", "queue", func(m *nats.Msg) {
		m.Respond([]byte("from srv2"))
	})

	// Check that "foo" interest is available everywhere.
	checkSubInterest(t, srv1, globalAccountName, "foo", time.Second)
	checkSubInterest(t, srv2, globalAccountName, "foo", time.Second)
	checkSubInterest(t, leaf1, globalAccountName, "foo", time.Second)
	checkSubInterest(t, leaf2, globalAccountName, "foo", time.Second)

	// Not required, but have a request payload that is more than 100 bytes
	reqPayload := make([]byte, 150)
	for i := 0; i < len(reqPayload); i++ {
		reqPayload[i] = byte((i % 26)) + 'A'
	}

	// Send repeated requests (from scratch) from leaf-2:
	sendReq := func() {
		t.Helper()

		ncLeaf2 := natsConnect(t, leaf2.ClientURL())
		defer ncLeaf2.Close()

		if _, err := ncLeaf2.Request("foo", reqPayload, time.Second); err != nil {
			t.Fatalf("Did not receive reply: %v", err)
		}
	}
	for i := 0; i < 100; i++ {
		sendReq()
	}
}

type parseRouteLSUnsubLogger struct {
	DummyLogger
	gotTrace chan struct{}
	gotErr   chan error
}

func (l *parseRouteLSUnsubLogger) Errorf(format string, v ...interface{}) {
	err := fmt.Errorf(format, v...)
	select {
	case l.gotErr <- err:
	default:
	}
}

func (l *parseRouteLSUnsubLogger) Tracef(format string, v ...interface{}) {
	trace := fmt.Sprintf(format, v...)
	if strings.Contains(trace, "LS- $G foo bar") {
		l.gotTrace <- struct{}{}
	}
}

func TestLeafNodeRouteParseLSUnsub(t *testing.T) {
	// This set the cluster name to "abc"
	oSrv1 := DefaultOptions()
	oSrv1.LeafNode.Host = "127.0.0.1"
	oSrv1.LeafNode.Port = -1
	srv1 := RunServer(oSrv1)
	defer srv1.Shutdown()

	l := &parseRouteLSUnsubLogger{gotTrace: make(chan struct{}, 1), gotErr: make(chan error, 1)}
	srv1.SetLogger(l, true, true)

	oSrv2 := DefaultOptions()
	oSrv2.LeafNode.Host = "127.0.0.1"
	oSrv2.LeafNode.Port = -1
	oSrv2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", oSrv1.Cluster.Port))
	srv2 := RunServer(oSrv2)
	defer srv2.Shutdown()

	checkClusterFormed(t, srv1, srv2)

	u2, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", oSrv2.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}

	oLeaf2 := DefaultOptions()
	oLeaf2.Cluster.Name = "xyz"
	oLeaf2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u2}}}
	leaf2 := RunServer(oLeaf2)
	defer leaf2.Shutdown()

	checkLeafNodeConnected(t, srv2)
	checkLeafNodeConnected(t, leaf2)

	ncLeaf2 := natsConnect(t, leaf2.ClientURL())
	defer ncLeaf2.Close()

	sub := natsQueueSubSync(t, ncLeaf2, "foo", "bar")
	// The issue was with the unsubscribe of this queue subscription
	natsUnsub(t, sub)

	// We should get the trace
	select {
	case <-l.gotTrace:
		// OK!
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Did not get LS- trace")
	}
	// And no error...
	select {
	case e := <-l.gotErr:
		t.Fatalf("There was an error on server 1: %q", e.Error())
	case <-time.After(100 * time.Millisecond):
		// OK!
	}
}

func TestLeafNodeOperatorBadCfg(t *testing.T) {
	sysAcc, err := nkeys.CreateAccount()
	require_NoError(t, err)
	sysAccPk, err := sysAcc.PublicKey()
	require_NoError(t, err)
	tmpDir := t.TempDir()

	configTmpl := `
		port: -1
		operator: %s
		system_account: %s
		resolver: {
			type: cache
			dir: '%s'
		}
		leafnodes: {
			%s
		}
	`

	cases := []struct {
		name      string
		errorText string
		cfg       string
	}{
		{
			name:      "Operator with Leafnode",
			errorText: "operator mode does not allow specifying user in leafnode config",
			cfg: `
			port: -1
			authorization {
				users = [{user: "u", password: "p"}]}
			}`,
		},
		{
			name:      "Operator with NKey",
			errorText: "operator mode and non account nkeys are incompatible",
			cfg: `
			port: -1
			authorization {
				account: notankey
			}`,
		},
		{
			name: "Operator remote account NKeys",
			errorText: "operator mode requires account nkeys in remotes. " +
				"Please add an `account` key to each remote in your `leafnodes` section, to assign it to an account. " +
				"Each account value should be a 56 character public key, starting with the letter 'A'",
			cfg: `remotes: [{url: u}]`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			conf := createConfFile(t, []byte(fmt.Sprintf(configTmpl, ojwt, sysAccPk, tmpDir, c.cfg)))
			opts := LoadConfig(conf)
			s, err := NewServer(opts)
			if err == nil {
				s.Shutdown()
				t.Fatal("Expected an error")
			}
			// Since the server cannot be stopped, since it did not start,
			// let's manually close the account resolver to avoid leaking go routines.
			opts.AccountResolver.Close()
			if err.Error() != c.errorText {
				t.Fatalf("Expected error %s but got %s", c.errorText, err)
			}
		})
	}
}

func TestLeafNodeTLSConfigReload(t *testing.T) {
	template := `
		listen: 127.0.0.1:-1
		leaf {
			listen: "127.0.0.1:-1"
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file:  "../test/configs/certs/server-key.pem"
				%s
				timeout: 2
				verify: true
			}
		}
	`
	confA := createConfFile(t, []byte(fmt.Sprintf(template, "")))

	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()

	lg := &captureErrorLogger{errCh: make(chan string, 10)}
	srvA.SetLogger(lg, false, false)

	confB := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		leaf {
			remotes [
				{
					url: "tls://127.0.0.1:%d"
					tls {
						cert_file: "../test/configs/certs/server-cert.pem"
						key_file:  "../test/configs/certs/server-key.pem"
						ca_file:   "../test/configs/certs/ca.pem"
					}
				}
			]
		}
	`, optsA.LeafNode.Port)))

	optsB, err := ProcessConfigFile(confB)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	optsB.LeafNode.ReconnectInterval = 50 * time.Millisecond
	optsB.NoLog, optsB.NoSigs = true, true

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	// Wait for the error
	select {
	case err := <-lg.errCh:
		// Since Go 1.18, we had to regenerate certs to not have to use GODEBUG="x509sha1=1"
		// But on macOS, with our test CA certs, no SCTs included, it will fail
		// for the reason "x509: “localhost” certificate is not standards compliant"
		// instead of "unknown authority".
		if !strings.Contains(err, "unknown") && !strings.Contains(err, "compliant") {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get TLS error")
	}

	// Add the CA to srvA
	reloadUpdateConfig(t, srvA, confA, fmt.Sprintf(template, `ca_file: "../test/configs/certs/ca.pem"`))

	// Now make sure that srvB can create a LN connection.
	checkFor(t, 3*time.Second, 10*time.Millisecond, func() error {
		if nln := srvB.NumLeafNodes(); nln != 1 {
			return fmt.Errorf("Number of leaf nodes is %d", nln)
		}
		return nil
	})
}

func TestLeafNodeTLSConfigReloadForRemote(t *testing.T) {
	confA := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		leaf {
			listen: "127.0.0.1:-1"
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file:  "../test/configs/certs/server-key.pem"
				ca_file: "../test/configs/certs/ca.pem"
				timeout: 2
				verify: true
			}
		}
	`))

	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()

	lg := &captureErrorLogger{errCh: make(chan string, 10)}
	srvA.SetLogger(lg, false, false)

	template := `
		listen: -1
		leaf {
			remotes [
				{
					url: "tls://127.0.0.1:%d"
					tls {
						cert_file: "../test/configs/certs/server-cert.pem"
						key_file:  "../test/configs/certs/server-key.pem"
						%s
					}
				}
			]
		}
	`
	confB := createConfFile(t, []byte(fmt.Sprintf(template, optsA.LeafNode.Port, "")))

	srvB, _ := RunServerWithConfig(confB)
	defer srvB.Shutdown()

	// Wait for the error
	select {
	case err := <-lg.errCh:
		if !strings.Contains(err, "bad certificate") {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get TLS error")
	}

	// Add the CA to srvB
	reloadUpdateConfig(t, srvB, confB, fmt.Sprintf(template, optsA.LeafNode.Port, `ca_file: "../test/configs/certs/ca.pem"`))

	// Now make sure that srvB can create a LN connection.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if nln := srvB.NumLeafNodes(); nln != 1 {
			return fmt.Errorf("Number of leaf nodes is %d", nln)
		}
		return nil
	})
}

func testDefaultLeafNodeWSOptions() *Options {
	o := DefaultOptions()
	o.Websocket.Host = "127.0.0.1"
	o.Websocket.Port = -1
	o.Websocket.NoTLS = true
	o.LeafNode.Host = "127.0.0.1"
	o.LeafNode.Port = -1
	return o
}

func testDefaultRemoteLeafNodeWSOptions(t *testing.T, o *Options, tls bool) *Options {
	// Use some path in the URL.. we don't use that, but internally
	// the server will prefix the path with /leafnode so that the
	// WS webserver knows that it needs to create a LEAF connection.
	u, _ := url.Parse(fmt.Sprintf("ws://127.0.0.1:%d/some/path", o.Websocket.Port))
	lo := DefaultOptions()
	lo.Cluster.Name = "LN"
	remote := &RemoteLeafOpts{URLs: []*url.URL{u}}
	if tls {
		tc := &TLSConfigOpts{
			CertFile: "../test/configs/certs/server-cert.pem",
			KeyFile:  "../test/configs/certs/server-key.pem",
			CaFile:   "../test/configs/certs/ca.pem",
		}
		tlsConf, err := GenTLSConfig(tc)
		if err != nil {
			t.Fatalf("Error generating TLS config: %v", err)
		}
		// GenTLSConfig sets the CA in ClientCAs, but since here we act
		// as a client, set RootCAs...
		tlsConf.RootCAs = tlsConf.ClientCAs
		remote.TLSConfig = tlsConf
	}
	lo.LeafNode.Remotes = []*RemoteLeafOpts{remote}
	return lo
}

func TestLeafNodeWSMixURLs(t *testing.T) {
	for _, test := range []struct {
		name string
		urls []string
	}{
		{"mix 1", []string{"nats://127.0.0.1:1234", "ws://127.0.0.1:5678", "wss://127.0.0.1:9012"}},
		{"mix 2", []string{"ws://127.0.0.1:1234", "nats://127.0.0.1:5678", "wss://127.0.0.1:9012"}},
		{"mix 3", []string{"wss://127.0.0.1:1234", "ws://127.0.0.1:5678", "nats://127.0.0.1:9012"}},
		{"mix 4", []string{"ws://127.0.0.1:1234", "nats://127.0.0.1:9012"}},
		{"mix 5", []string{"nats://127.0.0.1:1234", "ws://127.0.0.1:9012"}},
		{"mix 6", []string{"wss://127.0.0.1:1234", "nats://127.0.0.1:9012"}},
		{"mix 7", []string{"nats://127.0.0.1:1234", "wss://127.0.0.1:9012"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := DefaultOptions()
			remote := &RemoteLeafOpts{}
			urls := make([]*url.URL, 0, 3)
			for _, ustr := range test.urls {
				u, err := url.Parse(ustr)
				if err != nil {
					t.Fatalf("Error parsing url: %v", err)
				}
				urls = append(urls, u)
			}
			remote.URLs = urls
			o.LeafNode.Remotes = []*RemoteLeafOpts{remote}
			s, err := NewServer(o)
			if err == nil || !strings.Contains(err.Error(), "mix") {
				if s != nil {
					s.Shutdown()
				}
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

type testConnTrackSize struct {
	sync.Mutex
	net.Conn
	sz int
}

func (c *testConnTrackSize) Write(p []byte) (int, error) {
	c.Lock()
	defer c.Unlock()
	n, err := c.Conn.Write(p)
	c.sz += n
	return n, err
}

func TestLeafNodeWSBasic(t *testing.T) {
	for _, test := range []struct {
		name              string
		masking           bool
		tls               bool
		acceptCompression bool
		remoteCompression bool
	}{
		{"masking plain no compression", true, false, false, false},
		{"masking plain compression", true, false, true, true},
		{"masking plain compression disagree", true, false, false, true},
		{"masking plain compression disagree 2", true, false, true, false},
		{"masking tls no compression", true, true, false, false},
		{"masking tls compression", true, true, true, true},
		{"masking tls compression disagree", true, true, false, true},
		{"masking tls compression disagree 2", true, true, true, false},
		{"no masking plain no compression", false, false, false, false},
		{"no masking plain compression", false, false, true, true},
		{"no masking plain compression disagree", false, false, false, true},
		{"no masking plain compression disagree 2", false, false, true, false},
		{"no masking tls no compression", false, true, false, false},
		{"no masking tls compression", false, true, true, true},
		{"no masking tls compression disagree", false, true, false, true},
		{"no masking tls compression disagree 2", false, true, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testDefaultLeafNodeWSOptions()
			o.Websocket.NoTLS = !test.tls
			if test.tls {
				tc := &TLSConfigOpts{
					CertFile: "../test/configs/certs/server-cert.pem",
					KeyFile:  "../test/configs/certs/server-key.pem",
					CaFile:   "../test/configs/certs/ca.pem",
				}
				tlsConf, err := GenTLSConfig(tc)
				if err != nil {
					t.Fatalf("Error generating TLS config: %v", err)
				}
				o.Websocket.TLSConfig = tlsConf
			}
			o.Websocket.Compression = test.acceptCompression
			s := RunServer(o)
			defer s.Shutdown()

			lo := testDefaultRemoteLeafNodeWSOptions(t, o, test.tls)
			lo.LeafNode.Remotes[0].Websocket.Compression = test.remoteCompression
			lo.LeafNode.Remotes[0].Websocket.NoMasking = !test.masking
			ln := RunServer(lo)
			defer ln.Shutdown()

			checkLeafNodeConnected(t, s)
			checkLeafNodeConnected(t, ln)

			var trackSizeConn *testConnTrackSize
			if !test.tls {
				var cln *client
				ln.mu.Lock()
				for _, l := range ln.leafs {
					cln = l
					break
				}
				ln.mu.Unlock()
				cln.mu.Lock()
				trackSizeConn = &testConnTrackSize{Conn: cln.nc}
				cln.nc = trackSizeConn
				cln.mu.Unlock()
			}

			nc1 := natsConnect(t, s.ClientURL())
			defer nc1.Close()
			sub1 := natsSubSync(t, nc1, "foo")
			natsFlush(t, nc1)

			checkSubInterest(t, ln, globalAccountName, "foo", time.Second)

			nc2 := natsConnect(t, ln.ClientURL())
			defer nc2.Close()
			msg1Payload := make([]byte, 2048)
			for i := 0; i < len(msg1Payload); i++ {
				msg1Payload[i] = 'A'
			}
			natsPub(t, nc2, "foo", msg1Payload)

			msg := natsNexMsg(t, sub1, time.Second)
			if !bytes.Equal(msg.Data, msg1Payload) {
				t.Fatalf("Invalid message: %q", msg.Data)
			}

			sub2 := natsSubSync(t, nc2, "bar")
			natsFlush(t, nc2)

			checkSubInterest(t, s, globalAccountName, "bar", time.Second)

			msg2Payload := make([]byte, 2048)
			for i := 0; i < len(msg2Payload); i++ {
				msg2Payload[i] = 'B'
			}
			natsPub(t, nc1, "bar", msg2Payload)

			msg = natsNexMsg(t, sub2, time.Second)
			if !bytes.Equal(msg.Data, msg2Payload) {
				t.Fatalf("Invalid message: %q", msg.Data)
			}

			if !test.tls {
				trackSizeConn.Lock()
				size := trackSizeConn.sz
				trackSizeConn.Unlock()

				if test.acceptCompression && test.remoteCompression {
					if size >= 1024 {
						t.Fatalf("Seems that there was no compression: size=%v", size)
					}
				} else if size < 2048 {
					t.Fatalf("Seems compression was on while it should not: size=%v", size)
				}
			}
		})
	}
}

func TestLeafNodeWSRemoteCompressAndMaskingOptions(t *testing.T) {
	for _, test := range []struct {
		name      string
		compress  bool
		compStr   string
		noMasking bool
		noMaskStr string
	}{
		{"compression masking", true, "true", false, "false"},
		{"compression no masking", true, "true", true, "true"},
		{"no compression masking", false, "false", false, "false"},
		{"no compression no masking", false, "false", true, "true"},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				leafnodes {
					remotes [
						{url: "ws://127.0.0.1:1234", ws_compression: %s, ws_no_masking: %s}
					]
				}
			`, test.compStr, test.noMaskStr)))
			o, err := ProcessConfigFile(conf)
			if err != nil {
				t.Fatalf("Error loading conf: %v", err)
			}
			if nr := len(o.LeafNode.Remotes); nr != 1 {
				t.Fatalf("Expected 1 remote, got %v", nr)
			}
			r := o.LeafNode.Remotes[0]
			if cur := r.Websocket.Compression; cur != test.compress {
				t.Fatalf("Expected compress to be %v, got %v", test.compress, cur)
			}
			if cur := r.Websocket.NoMasking; cur != test.noMasking {
				t.Fatalf("Expected ws_masking to be %v, got %v", test.noMasking, cur)
			}
		})
	}
}

func TestLeafNodeWSNoMaskingRejected(t *testing.T) {
	wsTestRejectNoMasking = true
	defer func() { wsTestRejectNoMasking = false }()

	o := testDefaultLeafNodeWSOptions()
	s := RunServer(o)
	defer s.Shutdown()

	lo := testDefaultRemoteLeafNodeWSOptions(t, o, false)
	lo.LeafNode.Remotes[0].Websocket.NoMasking = true
	ln := RunServer(lo)
	defer ln.Shutdown()

	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, ln)

	var cln *client
	ln.mu.Lock()
	for _, l := range ln.leafs {
		cln = l
		break
	}
	ln.mu.Unlock()

	cln.mu.Lock()
	maskWrite := cln.ws.maskwrite
	cln.mu.Unlock()

	if !maskWrite {
		t.Fatal("Leafnode remote connection should mask writes, it does not")
	}
}

func TestLeafNodeWSFailedConnection(t *testing.T) {
	o := testDefaultLeafNodeWSOptions()
	s := RunServer(o)
	defer s.Shutdown()

	lo := testDefaultRemoteLeafNodeWSOptions(t, o, true)
	lo.LeafNode.ReconnectInterval = 100 * time.Millisecond
	ln := RunServer(lo)
	defer ln.Shutdown()

	el := &captureErrorLogger{errCh: make(chan string, 100)}
	ln.SetLogger(el, false, false)

	select {
	case err := <-el.errCh:
		if !strings.Contains(err, "handshake error") {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("No error reported!")
	}
	ln.Shutdown()
	s.Shutdown()

	lst, err := natsListen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Error starting listener: %v", err)
	}
	defer lst.Close()

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := 0; i < 10; i++ {
			c, err := lst.Accept()
			if err != nil {
				return
			}
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			if rand.Intn(2) == 1 {
				c.Write([]byte("something\r\n"))
			}
			c.Close()
		}
	}()

	time.Sleep(100 * time.Millisecond)

	port := lst.Addr().(*net.TCPAddr).Port
	u, _ := url.Parse(fmt.Sprintf("ws://127.0.0.1:%d", port))
	lo = DefaultOptions()
	lo.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	lo.LeafNode.ReconnectInterval = 10 * time.Millisecond
	ln, _ = NewServer(lo)
	el = &captureErrorLogger{errCh: make(chan string, 100)}
	ln.SetLogger(el, false, false)

	go func() {
		ln.Start()
		wg.Done()
	}()

	timeout := time.NewTimer(time.Second)
	for i := 0; i < 10; i++ {
		select {
		case err := <-el.errCh:
			if !strings.Contains(err, "Error soliciting") {
				t.Fatalf("Unexpected error: %v", err)
			}
		case <-timeout.C:
			t.Fatal("No error reported!")
		}
	}
	ln.Shutdown()
	lst.Close()
	wg.Wait()
}

func TestLeafNodeWSAuth(t *testing.T) {
	template := `
		port: -1
		authorization {
			users [
				{user: "user", pass: "puser", connection_types: ["%s"]}
				{user: "leaf", pass: "pleaf", connection_types: ["%s"%s]}
			]
		}
		websocket {
			port: -1
			no_tls: true
		}
		leafnodes {
			port: -1
		}
	`
	s, o, conf := runReloadServerWithContent(t,
		[]byte(fmt.Sprintf(template, jwt.ConnectionTypeStandard, jwt.ConnectionTypeLeafnode, "")))
	defer s.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	lo := testDefaultRemoteLeafNodeWSOptions(t, o, false)
	u, _ := url.Parse(fmt.Sprintf("ws://leaf:pleaf@127.0.0.1:%d", o.Websocket.Port))
	remote := &RemoteLeafOpts{URLs: []*url.URL{u}}
	lo.LeafNode.Remotes = []*RemoteLeafOpts{remote}
	lo.LeafNode.ReconnectInterval = 50 * time.Millisecond
	ln := RunServer(lo)
	defer ln.Shutdown()

	var lasterr string
	tm := time.NewTimer(2 * time.Second)
	for done := false; !done; {
		select {
		case lasterr = <-l.errCh:
			if strings.Contains(lasterr, "authentication") {
				done = true
			}
		case <-tm.C:
			t.Fatalf("Expected auth error, got %v", lasterr)
		}
	}

	ws := fmt.Sprintf(`, "%s"`, jwt.ConnectionTypeLeafnodeWS)
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(template,
		jwt.ConnectionTypeStandard, jwt.ConnectionTypeLeafnode, ws))

	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, ln)

	nc1 := natsConnect(t, fmt.Sprintf("nats://user:puser@127.0.0.1:%d", o.Port))
	defer nc1.Close()

	sub := natsSubSync(t, nc1, "foo")
	natsFlush(t, nc1)

	checkSubInterest(t, ln, globalAccountName, "foo", time.Second)

	nc2 := natsConnect(t, ln.ClientURL())
	defer nc2.Close()

	natsPub(t, nc2, "foo", []byte("msg1"))
	msg := natsNexMsg(t, sub, time.Second)

	if md := string(msg.Data); md != "msg1" {
		t.Fatalf("Invalid message: %q", md)
	}
}

func TestLeafNodeWSGossip(t *testing.T) {
	o1 := testDefaultLeafNodeWSOptions()
	s1 := RunServer(o1)
	defer s1.Shutdown()

	// Now connect from a server that knows only about s1
	lo := testDefaultRemoteLeafNodeWSOptions(t, o1, false)
	lo.LeafNode.ReconnectInterval = 15 * time.Millisecond
	ln := RunServer(lo)
	defer ln.Shutdown()

	checkLeafNodeConnected(t, s1)
	checkLeafNodeConnected(t, ln)

	// Now add a routed server to s1
	o2 := testDefaultLeafNodeWSOptions()
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	// Wait for cluster to form
	checkClusterFormed(t, s1, s2)

	// Now shutdown s1 and check that ln is able to reconnect to s2.
	s1.Shutdown()

	checkLeafNodeConnected(t, s2)
	checkLeafNodeConnected(t, ln)

	// Make sure that the reconnection was as a WS connection, not simply to
	// the regular LN port.
	var s2lc *client
	s2.mu.Lock()
	for _, l := range s2.leafs {
		s2lc = l
		break
	}
	s2.mu.Unlock()

	s2lc.mu.Lock()
	isWS := s2lc.isWebsocket()
	s2lc.mu.Unlock()

	if !isWS {
		t.Fatal("Leafnode connection is not websocket!")
	}
}

// This test was showing an issue if one set maxBufSize to very small value,
// such as maxBufSize = 10. With such small value, we would get a corruption
// in that LMSG would arrive with missing bytes. We are now always making
// a copy when dealing with messages that are bigger than maxBufSize.
func TestLeafNodeWSNoBufferCorruption(t *testing.T) {
	o := testDefaultLeafNodeWSOptions()
	s := RunServer(o)
	defer s.Shutdown()

	lo1 := testDefaultRemoteLeafNodeWSOptions(t, o, false)
	lo1.LeafNode.ReconnectInterval = 15 * time.Millisecond
	ln1 := RunServer(lo1)
	defer ln1.Shutdown()

	lo2 := DefaultOptions()
	lo2.Cluster.Name = "LN"
	lo2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", lo1.Cluster.Port))
	ln2 := RunServer(lo2)
	defer ln2.Shutdown()

	checkClusterFormed(t, ln1, ln2)

	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, ln1)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	sub := natsSubSync(t, nc, "foo")

	nc1 := natsConnect(t, ln1.ClientURL())
	defer nc1.Close()

	nc2 := natsConnect(t, ln2.ClientURL())
	defer nc2.Close()
	sub2 := natsSubSync(t, nc2, "foo")

	checkSubInterest(t, s, globalAccountName, "foo", time.Second)
	checkSubInterest(t, ln2, globalAccountName, "foo", time.Second)
	checkSubInterest(t, ln1, globalAccountName, "foo", time.Second)

	payload := make([]byte, 100*1024)
	for i := 0; i < len(payload); i++ {
		payload[i] = 'A'
	}
	natsPub(t, nc1, "foo", payload)

	checkMsgRcv := func(sub *nats.Subscription) {
		msg := natsNexMsg(t, sub, time.Second)
		if !bytes.Equal(msg.Data, payload) {
			t.Fatalf("Invalid message content: %q", msg.Data)
		}
	}
	checkMsgRcv(sub2)
	checkMsgRcv(sub)
}

func TestLeafNodeWSRemoteNoTLSBlockWithWSSProto(t *testing.T) {
	o := testDefaultLeafNodeWSOptions()
	o.Websocket.NoTLS = false
	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/server-cert.pem",
		KeyFile:  "../test/configs/certs/server-key.pem",
		CaFile:   "../test/configs/certs/ca.pem",
	}
	tlsConf, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating TLS config: %v", err)
	}
	o.Websocket.TLSConfig = tlsConf
	s := RunServer(o)
	defer s.Shutdown()

	// The test will make sure that if the protocol is "wss://", a TLS handshake must
	// be initiated, regardless of the presence of a TLS config block in config file
	// or here directly.
	// A bug was causing the absence of TLS config block to initiate a non TLS connection
	// even if "wss://" proto was specified, which would lead to "invalid websocket connection"
	// errors in the log.
	// With the fix, the connection will fail because the remote will fail to verify
	// the root CA, but at least, we will make sure that this is not an "invalid websocket connection"

	u, _ := url.Parse(fmt.Sprintf("wss://127.0.0.1:%d/some/path", o.Websocket.Port))
	lo := DefaultOptions()
	lo.Cluster.Name = "LN"
	remote := &RemoteLeafOpts{URLs: []*url.URL{u}}
	lo.LeafNode.Remotes = []*RemoteLeafOpts{remote}
	lo.LeafNode.ReconnectInterval = 100 * time.Millisecond
	ln := RunServer(lo)
	defer ln.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	ln.SetLogger(l, false, false)

	select {
	case e := <-l.errCh:
		if strings.Contains(e, "invalid websocket connection") {
			t.Fatalf("The remote did not try to create a TLS connection: %v", e)
		}
		// OK!
		return
	case <-time.After(2 * time.Second):
		t.Fatal("Connection should fail")
	}
}

func TestLeafNodeWSNoAuthUser(t *testing.T) {
	conf := createConfFile(t, []byte(`
	port: -1
	accounts {
		A { users [ {user: a, password: a} ]}
		B { users [ {user: b, password: b} ]}
	}
	websocket {
		port: -1
		no_tls: true
		no_auth_user: a
	}
	leafnodes {
		port: -1
	}
	`))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc1 := natsConnect(t, fmt.Sprintf("nats://a:a@127.0.0.1:%d", o.Port))
	defer nc1.Close()

	lconf := createConfFile(t, []byte(fmt.Sprintf(`
	port: -1
	accounts {
		A { users [ {user: a, password: a} ]}
		B { users [ {user: b, password: b} ]}
	}
	leafnodes {
		remotes [
			{
				url: "ws://127.0.0.1:%d"
				account: A
			}
		]
	}
	`, o.Websocket.Port)))

	ln, lo := RunServerWithConfig(lconf)
	defer ln.Shutdown()

	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, ln)

	nc2 := natsConnect(t, fmt.Sprintf("nats://a:a@127.0.0.1:%d", lo.Port))
	defer nc2.Close()

	sub := natsSubSync(t, nc2, "foo")
	natsFlush(t, nc2)

	checkSubInterest(t, s, "A", "foo", time.Second)

	natsPub(t, nc1, "foo", []byte("msg1"))
	msg := natsNexMsg(t, sub, time.Second)

	if md := string(msg.Data); md != "msg1" {
		t.Fatalf("Invalid message: %q", md)
	}
}

func TestLeafNodeStreamImport(t *testing.T) {
	o1 := DefaultOptions()
	o1.LeafNode.Port = -1
	accA := NewAccount("A")
	o1.Accounts = []*Account{accA}
	o1.Users = []*User{{Username: "a", Password: "a", Account: accA}}
	o1.LeafNode.Account = "A"
	o1.NoAuthUser = "a"
	s1 := RunServer(o1)
	defer s1.Shutdown()

	o2 := DefaultOptions()
	o2.LeafNode.Port = -1
	o2.Cluster.Name = "xyz"

	accB := NewAccount("B")
	if err := accB.AddStreamExport(">", nil); err != nil {
		t.Fatalf("Error adding stream export: %v", err)
	}

	accC := NewAccount("C")
	if err := accC.AddStreamImport(accB, ">", ""); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}

	o2.Accounts = []*Account{accB, accC}
	o2.Users = []*User{{Username: "b", Password: "b", Account: accB}, {Username: "c", Password: "c", Account: accC}}
	o2.NoAuthUser = "b"
	u, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", o1.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}
	o2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}, LocalAccount: "C"}}
	s2 := RunServer(o2)
	defer s2.Shutdown()

	nc1 := natsConnect(t, s1.ClientURL())
	defer nc1.Close()

	sub := natsSubSync(t, nc1, "a")

	checkSubInterest(t, s2, "C", "a", time.Second)

	nc2 := natsConnect(t, s2.ClientURL())
	defer nc2.Close()

	natsPub(t, nc2, "a", []byte("hello?"))

	natsNexMsg(t, sub, time.Second)
}

func TestLeafNodeRouteSubWithOrigin(t *testing.T) {
	lo1 := DefaultOptions()
	lo1.LeafNode.Host = "127.0.0.1"
	lo1.LeafNode.Port = -1
	lo1.Cluster.Name = "local"
	lo1.Cluster.Host = "127.0.0.1"
	lo1.Cluster.Port = -1
	l1 := RunServer(lo1)
	defer l1.Shutdown()

	lo2 := DefaultOptions()
	lo2.LeafNode.Host = "127.0.0.1"
	lo2.LeafNode.Port = -1
	lo2.Cluster.Name = "local"
	lo2.Cluster.Host = "127.0.0.1"
	lo2.Cluster.Port = -1
	lo2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", lo1.Cluster.Port))
	l2 := RunServer(lo2)
	defer l2.Shutdown()

	checkClusterFormed(t, l1, l2)

	u1, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", lo1.LeafNode.Port))
	urls := []*url.URL{u1}

	ro1 := DefaultOptions()
	ro1.Cluster.Name = "remote"
	ro1.Cluster.Host = "127.0.0.1"
	ro1.Cluster.Port = -1
	ro1.LeafNode.ReconnectInterval = 50 * time.Millisecond
	ro1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: urls}}
	r1 := RunServer(ro1)
	defer r1.Shutdown()

	checkLeafNodeConnected(t, r1)

	nc := natsConnect(t, r1.ClientURL(), nats.NoReconnect())
	defer nc.Close()
	natsSubSync(t, nc, "foo")
	natsQueueSubSync(t, nc, "bar", "baz")
	checkSubInterest(t, l2, globalAccountName, "foo", time.Second)
	checkSubInterest(t, l2, globalAccountName, "bar", time.Second)

	// Now shutdown the leafnode and check that any subscription for $G on l2 are gone.
	r1.Shutdown()
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		acc := l2.GlobalAccount()
		if n := acc.TotalSubs(); n != 5 {
			return fmt.Errorf("Account %q should have 5 subs, got %v", acc.GetName(), n)
		}
		return nil
	})
}

func TestLeafNodeLoopDetectionWithMultipleClusters(t *testing.T) {
	lo1 := DefaultOptions()
	lo1.LeafNode.Host = "127.0.0.1"
	lo1.LeafNode.Port = -1
	lo1.Cluster.Name = "local"
	lo1.Cluster.Host = "127.0.0.1"
	lo1.Cluster.Port = -1
	l1 := RunServer(lo1)
	defer l1.Shutdown()

	lo2 := DefaultOptions()
	lo2.LeafNode.Host = "127.0.0.1"
	lo2.LeafNode.Port = -1
	lo2.Cluster.Name = "local"
	lo2.Cluster.Host = "127.0.0.1"
	lo2.Cluster.Port = -1
	lo2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", lo1.Cluster.Port))
	l2 := RunServer(lo2)
	defer l2.Shutdown()

	checkClusterFormed(t, l1, l2)

	ro1 := DefaultOptions()
	ro1.Cluster.Name = "remote"
	ro1.Cluster.Host = "127.0.0.1"
	ro1.Cluster.Port = -1
	ro1.LeafNode.ReconnectInterval = 50 * time.Millisecond
	ro1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{
		{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", lo1.LeafNode.Port)},
		{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", lo2.LeafNode.Port)},
	}}}
	r1 := RunServer(ro1)
	defer r1.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 100)}
	r1.SetLogger(l, false, false)

	ro2 := DefaultOptions()
	ro2.Cluster.Name = "remote"
	ro2.Cluster.Host = "127.0.0.1"
	ro2.Cluster.Port = -1
	ro2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", ro1.Cluster.Port))
	ro2.LeafNode.ReconnectInterval = 50 * time.Millisecond
	ro2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{
		{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", lo1.LeafNode.Port)},
		{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", lo2.LeafNode.Port)},
	}}}
	r2 := RunServer(ro2)
	defer r2.Shutdown()

	checkClusterFormed(t, r1, r2)
	checkLeafNodeConnected(t, r1)
	checkLeafNodeConnected(t, r2)

	l1.Shutdown()

	// Now wait for r1 and r2 to reconnect, they should not have a problem of loop detection.
	checkLeafNodeConnected(t, r1)
	checkLeafNodeConnected(t, r2)

	// Wait and make sure we don't have a loop error
	timeout := time.NewTimer(500 * time.Millisecond)
	for {
		select {
		case err := <-l.errCh:
			if strings.Contains(err, "Loop detected") {
				t.Fatal(err)
			}
		case <-timeout.C:
			// OK, we are done.
			return
		}
	}
}

func TestLeafNodeUnsubOnRouteDisconnect(t *testing.T) {
	lo1 := DefaultOptions()
	lo1.LeafNode.Host = "127.0.0.1"
	lo1.LeafNode.Port = -1
	lo1.Cluster.Name = "local"
	lo1.Cluster.Host = "127.0.0.1"
	lo1.Cluster.Port = -1
	l1 := RunServer(lo1)
	defer l1.Shutdown()

	lo2 := DefaultOptions()
	lo2.LeafNode.Host = "127.0.0.1"
	lo2.LeafNode.Port = -1
	lo2.Cluster.Name = "local"
	lo2.Cluster.Host = "127.0.0.1"
	lo2.Cluster.Port = -1
	lo2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", lo1.Cluster.Port))
	l2 := RunServer(lo2)
	defer l2.Shutdown()

	checkClusterFormed(t, l1, l2)

	u1, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", lo1.LeafNode.Port))
	u2, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", lo2.LeafNode.Port))
	urls := []*url.URL{u1, u2}

	ro1 := DefaultOptions()
	// DefaultOptions sets a cluster name, so make sure they are different.
	// Also, we don't have r1 and r2 clustered in this test, so set port to 0.
	ro1.Cluster.Name = _EMPTY_
	ro1.Cluster.Port = 0
	ro1.LeafNode.ReconnectInterval = 50 * time.Millisecond
	ro1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: urls}}
	r1 := RunServer(ro1)
	defer r1.Shutdown()

	ro2 := DefaultOptions()
	ro1.Cluster.Name = _EMPTY_
	ro2.Cluster.Port = 0
	ro2.LeafNode.ReconnectInterval = 50 * time.Millisecond
	// Have this one point only to l2
	ro2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u2}}}
	r2 := RunServer(ro2)
	defer r2.Shutdown()

	checkLeafNodeConnected(t, r1)
	checkLeafNodeConnected(t, r2)

	// Create a subscription on r1.
	nc := natsConnect(t, r1.ClientURL())
	defer nc.Close()
	sub := natsSubSync(t, nc, "foo")
	natsFlush(t, nc)

	checkSubInterest(t, l2, globalAccountName, "foo", time.Second)
	checkSubInterest(t, r2, globalAccountName, "foo", time.Second)

	nc2 := natsConnect(t, r2.ClientURL())
	defer nc2.Close()
	natsPub(t, nc, "foo", []byte("msg1"))

	// Check message received
	natsNexMsg(t, sub, time.Second)

	// Now shutdown l1, l2 should update subscription interest to r2.
	// When r1 reconnects to l2, subscription should be updated too.
	l1.Shutdown()

	// Wait a bit (so that the check of interest is not OK just because
	// the route would not have been yet detected as broken), and check
	// interest still present on r2, l2.
	time.Sleep(100 * time.Millisecond)
	checkSubInterest(t, l2, globalAccountName, "foo", time.Second)
	checkSubInterest(t, r2, globalAccountName, "foo", time.Second)

	// Check again that message received ok
	natsPub(t, nc, "foo", []byte("msg2"))
	natsNexMsg(t, sub, time.Second)

	// Now close client. Interest should disappear on r2. Due to a bug,
	// it was not.
	nc.Close()

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		acc := r2.GlobalAccount()
		if n := acc.Interest("foo"); n != 0 {
			return fmt.Errorf("Still interest on subject: %v", n)
		}
		return nil
	})
}

func TestLeafNodeNoPingBeforeConnect(t *testing.T) {
	o := DefaultOptions()
	o.LeafNode.Port = -1
	o.LeafNode.AuthTimeout = 0.5
	// For this test we need to disable compression, because we do use
	// the ping timer instead of the auth timer before the negotiation
	// is complete.
	o.LeafNode.Compression.Mode = CompressionOff
	s := RunServer(o)
	defer s.Shutdown()

	addr := fmt.Sprintf("127.0.0.1:%d", o.LeafNode.Port)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error on dial: %v", err)
	}
	defer c.Close()

	// Read the info
	br := bufio.NewReader(c)
	c.SetReadDeadline(time.Now().Add(time.Second))
	l, _, err := br.ReadLine()
	if err != nil {
		t.Fatalf("Error on read: %v", err)
	}
	if !strings.HasPrefix(string(l), "INFO") {
		t.Fatalf("Wrong proto: %q", l)
	}

	var leaf *client
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		s.grMu.Lock()
		for _, l := range s.grTmpClients {
			leaf = l
			break
		}
		s.grMu.Unlock()
		if leaf == nil {
			return fmt.Errorf("No leaf connection found")
		}
		return nil
	})

	// Make sure that ping timer is not set
	leaf.mu.Lock()
	ptmrSet := leaf.ping.tmr != nil
	leaf.mu.Unlock()

	if ptmrSet {
		t.Fatal("Ping timer was set before CONNECT was processed")
	}

	// Send CONNECT
	if _, err := c.Write([]byte("CONNECT {}\r\n")); err != nil {
		t.Fatalf("Error writing connect: %v", err)
	}

	// Check that we correctly set the timer now
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		leaf.mu.Lock()
		ptmrSet := leaf.ping.tmr != nil
		leaf.mu.Unlock()
		if !ptmrSet {
			return fmt.Errorf("Timer still not set")
		}
		return nil
	})

	// Reduce the first ping..
	leaf.mu.Lock()
	leaf.ping.tmr.Reset(15 * time.Millisecond)
	leaf.mu.Unlock()

	// Now consume that PING (we may get LS+, etc..)
	for {
		c.SetReadDeadline(time.Now().Add(time.Second))
		l, _, err = br.ReadLine()
		if err != nil {
			t.Fatalf("Error on read: %v", err)
		}
		if strings.HasPrefix(string(l), "PING") {
			checkLeafNodeConnected(t, s)
			return
		}
	}
}

func TestLeafNodeNoMsgLoop(t *testing.T) {
	hubConf := `
		listen: "127.0.0.1:-1"
		accounts {
			FOO {
				users [
					{username: leaf, password: pass}
					{username: user, password: pass}
				]
			}
		}
		cluster {
			name: "hub"
			listen: "127.0.0.1:-1"
			%s
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			authorization {
				account: FOO
			}
		}
	`
	configS1 := createConfFile(t, []byte(fmt.Sprintf(hubConf, "")))
	s1, o1 := RunServerWithConfig(configS1)
	defer s1.Shutdown()

	configS2S3 := createConfFile(t, []byte(fmt.Sprintf(hubConf, fmt.Sprintf(`routes: ["nats://127.0.0.1:%d"]`, o1.Cluster.Port))))
	s2, o2 := RunServerWithConfig(configS2S3)
	defer s2.Shutdown()

	s3, _ := RunServerWithConfig(configS2S3)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	contentLN := `
		listen: "127.0.0.1:%d"
		accounts {
			FOO {
				users [
					{username: leaf, password: pass}
					{username: user, password: pass}
				]
			}
		}
		leafnodes {
			remotes = [
				{
					url: "nats://leaf:pass@127.0.0.1:%d"
					account: FOO
				}
			]
		}
	`
	lnconf := createConfFile(t, []byte(fmt.Sprintf(contentLN, -1, o1.LeafNode.Port)))
	sl1, slo1 := RunServerWithConfig(lnconf)
	defer sl1.Shutdown()

	sl2, slo2 := RunServerWithConfig(lnconf)
	defer sl2.Shutdown()

	checkLeafNodeConnected(t, sl1)
	checkLeafNodeConnected(t, sl2)

	// Create users on each leafnode
	nc1, err := nats.Connect(fmt.Sprintf("nats://user:pass@127.0.0.1:%d", slo1.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc1.Close()

	rch := make(chan struct{}, 1)
	nc2, err := nats.Connect(
		fmt.Sprintf("nats://user:pass@127.0.0.1:%d", slo2.Port),
		nats.ReconnectWait(50*time.Millisecond),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			rch <- struct{}{}
		}),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	// Create queue subs on sl2
	nc2.QueueSubscribe("foo", "bar", func(_ *nats.Msg) {})
	nc2.QueueSubscribe("foo", "bar", func(_ *nats.Msg) {})
	nc2.Flush()

	// Wait for interest to propagate to sl1
	checkSubInterest(t, sl1, "FOO", "foo", 250*time.Millisecond)

	// Create sub on sl1
	ch := make(chan *nats.Msg, 10)
	nc1.Subscribe("foo", func(m *nats.Msg) {
		select {
		case ch <- m:
		default:
		}
	})
	nc1.Flush()

	checkSubInterest(t, sl2, "FOO", "foo", 250*time.Millisecond)

	// Produce from sl1
	nc1.Publish("foo", []byte("msg1"))

	// Check message is received by plain sub
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("Did not receive message")
	}

	// Restart leaf node, this time make sure we connect to 2nd server.
	sl2.Shutdown()

	// Use config file but this time reuse the client port and set the 2nd server for
	// the remote leaf node port.
	lnconf = createConfFile(t, []byte(fmt.Sprintf(contentLN, slo2.Port, o2.LeafNode.Port)))
	sl2, _ = RunServerWithConfig(lnconf)
	defer sl2.Shutdown()

	checkLeafNodeConnected(t, sl2)

	// Wait for client to reconnect
	select {
	case <-rch:
	case <-time.After(time.Second):
		t.Fatalf("Did not reconnect")
	}

	// Produce a new messages
	for i := 0; i < 10; i++ {
		nc1.Publish("foo", []byte(fmt.Sprintf("msg%d", 2+i)))

		// Check sub receives 1 message
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("Did not receive message")
		}
		// Check that there is no more...
		select {
		case m := <-ch:
			t.Fatalf("Loop: received second message %s", m.Data)
		case <-time.After(50 * time.Millisecond):
			// OK
		}
	}
}

func TestLeafNodeInterestPropagationDaisychain(t *testing.T) {
	aTmpl := `
		port: %d
		leafnodes {
			port: %d
		   }
		}`

	confA := createConfFile(t, []byte(fmt.Sprintf(aTmpl, -1, -1)))
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()

	aPort := sA.opts.Port
	aLeafPort := sA.opts.LeafNode.Port

	confB := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes {
			port: -1
			remotes = [{
				url:"nats://127.0.0.1:%d"
			}]
		}`, aLeafPort)))
	sB, _ := RunServerWithConfig(confB)
	defer sB.Shutdown()

	confC := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes {
			port: -1
			remotes = [{url:"nats://127.0.0.1:%d"}]
		}`, sB.opts.LeafNode.Port)))
	sC, _ := RunServerWithConfig(confC)
	defer sC.Shutdown()

	checkLeafNodeConnectedCount(t, sC, 1)
	checkLeafNodeConnectedCount(t, sB, 2)
	checkLeafNodeConnectedCount(t, sA, 1)

	ncC := natsConnect(t, sC.ClientURL())
	defer ncC.Close()
	_, err := ncC.SubscribeSync("foo")
	require_NoError(t, err)
	require_NoError(t, ncC.Flush())

	checkSubInterest(t, sC, "$G", "foo", time.Second)
	checkSubInterest(t, sB, "$G", "foo", time.Second)
	checkSubInterest(t, sA, "$G", "foo", time.Second)

	ncA := natsConnect(t, sA.ClientURL())
	defer ncA.Close()

	sA.Shutdown()
	sA.WaitForShutdown()

	confAA := createConfFile(t, []byte(fmt.Sprintf(aTmpl, aPort, aLeafPort)))
	sAA, _ := RunServerWithConfig(confAA)
	defer sAA.Shutdown()

	checkLeafNodeConnectedCount(t, sAA, 1)
	checkLeafNodeConnectedCount(t, sB, 2)
	checkLeafNodeConnectedCount(t, sC, 1)

	checkSubInterest(t, sC, "$G", "foo", time.Second)
	checkSubInterest(t, sB, "$G", "foo", time.Second)
	checkSubInterest(t, sAA, "$G", "foo", time.Second) // failure issue 2448
}

func TestLeafNodeQueueGroupWithLateLNJoin(t *testing.T) {
	/*

		Topology: A cluster of leafnodes LN2 and LN3, connect
		to a cluster C1, C2.

		sub(foo)     sub(foo)
		    \         /
		    C1  <->  C2
		    ^        ^
		    |        |
		    LN2 <-> LN3
		    /         \
		sub(foo)     sub(foo)

		Once the above is set, start LN1 that connects to C1.

		    sub(foo)     sub(foo)
		        \         /
		LN1 ->  C1  <->  C2
		        ^        ^
		        |        |
		        LN2 <-> LN3
		        /         \
		    sub(foo)     sub(foo)

		Remove subs to LN3, C2 and C1.

		LN1 ->  C1  <->  C2
		        ^        ^
		        |        |
		        LN2 <-> LN3
		        /
		    sub(foo)

		Publish from LN1 and verify message is received by sub on LN2.

		pub(foo)
		\
		LN1 -> C1  <->  C2
		        ^        ^
		        |        |
		        LN2 <-> LN3
		        /
		    sub(foo)
	*/
	co1 := DefaultOptions()
	co1.LeafNode.Host = "127.0.0.1"
	co1.LeafNode.Port = -1
	co1.Cluster.Name = "ngs"
	co1.Cluster.Host = "127.0.0.1"
	co1.Cluster.Port = -1
	c1 := RunServer(co1)
	defer c1.Shutdown()

	co2 := DefaultOptions()
	co2.LeafNode.Host = "127.0.0.1"
	co2.LeafNode.Port = -1
	co2.Cluster.Name = "ngs"
	co2.Cluster.Host = "127.0.0.1"
	co2.Cluster.Port = -1
	co2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", co1.Cluster.Port))
	c2 := RunServer(co2)
	defer c2.Shutdown()

	checkClusterFormed(t, c1, c2)

	lo2 := DefaultOptions()
	lo2.Cluster.Name = "local"
	lo2.Cluster.Host = "127.0.0.1"
	lo2.Cluster.Port = -1
	lo2.LeafNode.ReconnectInterval = 50 * time.Millisecond
	lo2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", co1.LeafNode.Port)}}}}
	ln2 := RunServer(lo2)
	defer ln2.Shutdown()

	lo3 := DefaultOptions()
	lo3.Cluster.Name = "local"
	lo3.Cluster.Host = "127.0.0.1"
	lo3.Cluster.Port = -1
	lo3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", lo2.Cluster.Port))
	lo3.LeafNode.ReconnectInterval = 50 * time.Millisecond
	lo3.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", co2.LeafNode.Port)}}}}
	ln3 := RunServer(lo3)
	defer ln3.Shutdown()

	checkClusterFormed(t, ln2, ln3)
	checkLeafNodeConnected(t, ln2)
	checkLeafNodeConnected(t, ln3)

	cln2 := natsConnect(t, ln2.ClientURL())
	defer cln2.Close()
	sln2 := natsQueueSubSync(t, cln2, "foo", "qgroup")
	natsFlush(t, cln2)

	cln3 := natsConnect(t, ln3.ClientURL())
	defer cln3.Close()
	sln3 := natsQueueSubSync(t, cln3, "foo", "qgroup")
	natsFlush(t, cln3)

	cc1 := natsConnect(t, c1.ClientURL())
	defer cc1.Close()
	sc1 := natsQueueSubSync(t, cc1, "foo", "qgroup")
	natsFlush(t, cc1)

	cc2 := natsConnect(t, c2.ClientURL())
	defer cc2.Close()
	sc2 := natsQueueSubSync(t, cc2, "foo", "qgroup")
	natsFlush(t, cc2)

	checkSubInterest(t, c1, globalAccountName, "foo", time.Second)
	checkSubInterest(t, c2, globalAccountName, "foo", time.Second)
	checkSubInterest(t, ln2, globalAccountName, "foo", time.Second)
	checkSubInterest(t, ln3, globalAccountName, "foo", time.Second)

	lo1 := DefaultOptions()
	lo1.LeafNode.ReconnectInterval = 50 * time.Millisecond
	lo1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", co1.LeafNode.Port)}}}}
	ln1 := RunServer(lo1)
	defer ln1.Shutdown()

	checkLeafNodeConnected(t, ln1)
	checkSubInterest(t, ln1, globalAccountName, "foo", time.Second)

	sln3.Unsubscribe()
	natsFlush(t, cln3)
	sc2.Unsubscribe()
	natsFlush(t, cc2)
	sc1.Unsubscribe()
	natsFlush(t, cc1)

	cln1 := natsConnect(t, ln1.ClientURL())
	defer cln1.Close()

	natsPub(t, cln1, "foo", []byte("hello"))
	natsNexMsg(t, sln2, time.Second)
}

func TestLeafNodeJetStreamDomainMapCrossTalk(t *testing.T) {
	accs := `
accounts :{
    A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
    SYS:{ users:[ {user:s1,password:s1}]},
}
system_account: SYS
`

	sd1 := t.TempDir()
	confA := createConfFile(t, []byte(fmt.Sprintf(`
listen: 127.0.0.1:-1
%s
jetstream: { domain: da, store_dir: '%s', max_mem: 50Mb, max_file: 50Mb }
leafnodes: {
	listen: 127.0.0.1:-1
	no_advertise: true
	authorization: {
		timeout: 0.5
	}
}
`, accs, sd1)))
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()

	sd2 := t.TempDir()
	confL := createConfFile(t, []byte(fmt.Sprintf(`
listen: 127.0.0.1:-1
%s
jetstream: { domain: dl, store_dir: '%s', max_mem: 50Mb, max_file: 50Mb }
leafnodes:{
	no_advertise: true
    remotes:[{url:nats://a1:a1@127.0.0.1:%d, account: A},
		     {url:nats://s1:s1@127.0.0.1:%d, account: SYS}]
}
`, accs, sd2, sA.opts.LeafNode.Port, sA.opts.LeafNode.Port)))
	sL, _ := RunServerWithConfig(confL)
	defer sL.Shutdown()

	ncA := natsConnect(t, sA.ClientURL(), nats.UserInfo("a1", "a1"))
	defer ncA.Close()
	ncL := natsConnect(t, sL.ClientURL(), nats.UserInfo("a1", "a1"))
	defer ncL.Close()

	test := func(jsA, jsL nats.JetStreamContext) {
		kvA, err := jsA.CreateKeyValue(&nats.KeyValueConfig{Bucket: "bucket"})
		require_NoError(t, err)
		kvL, err := jsL.CreateKeyValue(&nats.KeyValueConfig{Bucket: "bucket"})
		require_NoError(t, err)

		_, err = kvA.Put("A", nil)
		require_NoError(t, err)
		_, err = kvL.Put("L", nil)
		require_NoError(t, err)

		// check for unwanted cross talk
		_, err = kvA.Get("A")
		require_NoError(t, err)
		_, err = kvA.Get("l")
		require_Error(t, err)
		require_True(t, err == nats.ErrKeyNotFound)

		_, err = kvL.Get("A")
		require_Error(t, err)
		require_True(t, err == nats.ErrKeyNotFound)
		_, err = kvL.Get("L")
		require_NoError(t, err)

		err = jsA.DeleteKeyValue("bucket")
		require_NoError(t, err)
		err = jsL.DeleteKeyValue("bucket")
		require_NoError(t, err)
	}

	jsA, err := ncA.JetStream()
	require_NoError(t, err)
	jsL, err := ncL.JetStream()
	require_NoError(t, err)
	test(jsA, jsL)

	jsAL, err := ncA.JetStream(nats.Domain("dl"))
	require_NoError(t, err)
	jsLA, err := ncL.JetStream(nats.Domain("da"))
	require_NoError(t, err)
	test(jsAL, jsLA)

	jsAA, err := ncA.JetStream(nats.Domain("da"))
	require_NoError(t, err)
	jsLL, err := ncL.JetStream(nats.Domain("dl"))
	require_NoError(t, err)
	test(jsAA, jsLL)
}

type checkLeafMinVersionLogger struct {
	DummyLogger
	errCh  chan string
	connCh chan string
}

func (l *checkLeafMinVersionLogger) Errorf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, "minimum version") {
		select {
		case l.errCh <- msg:
		default:
		}
	}
}

func (l *checkLeafMinVersionLogger) Noticef(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, "Leafnode connection created") {
		select {
		case l.connCh <- msg:
		default:
		}
	}
}

func TestLeafNodeMinVersion(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: -1
		leafnodes {
			port: -1
			min_version: 2.8.0
		}
	`))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	rconf := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes {
			remotes [
				{url: "nats://127.0.0.1:%d" }
			]
		}
	`, o.LeafNode.Port)))
	ln, _ := RunServerWithConfig(rconf)
	defer ln.Shutdown()

	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, ln)

	ln.Shutdown()
	s.Shutdown()

	// Now makes sure we validate options, not just config file.
	for _, test := range []struct {
		name    string
		version string
		err     string
	}{
		{"invalid version", "abc", "semver"},
		{"version too low", "2.7.9", "the minimum version should be at least 2.8.0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			o.Port = -1
			o.LeafNode.Port = -1
			o.LeafNode.MinVersion = test.version
			if s, err := NewServer(o); err == nil || !strings.Contains(err.Error(), test.err) {
				if s != nil {
					s.Shutdown()
				}
				t.Fatalf("Expected error to contain %q, got %v", test.err, err)
			}
		})
	}

	// Ok, so now to verify that a server rejects a leafnode connection
	// we will set the min_version above our current VERSION. So first
	// decompose the version:
	major, minor, _, err := versionComponents(VERSION)
	if err != nil {
		t.Fatalf("The current server version %q is not valid: %v", VERSION, err)
	}
	// Let's make our minimum server an minor version above
	mv := fmt.Sprintf("%d.%d.0", major, minor+1)
	conf = createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes {
			port: -1
			min_version: "%s"
		}
	`, mv)))
	s, o = RunServerWithConfig(conf)
	defer s.Shutdown()

	l := &checkLeafMinVersionLogger{errCh: make(chan string, 1), connCh: make(chan string, 1)}
	s.SetLogger(l, false, false)

	rconf = createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes {
			remotes [
				{url: "nats://127.0.0.1:%d" }
			]
		}
	`, o.LeafNode.Port)))
	lo := LoadConfig(rconf)
	lo.LeafNode.ReconnectInterval = 50 * time.Millisecond
	ln = RunServer(lo)
	defer ln.Shutdown()

	select {
	case <-l.connCh:
	case <-time.After(time.Second):
		t.Fatal("Remote did not try to connect")
	}

	select {
	case <-l.errCh:
	case <-time.After(time.Second):
		t.Fatal("Did not get the minimum version required error")
	}

	// Since we have a very small reconnect interval, if the connection was
	// closed "right away", then we should have had a reconnect attempt with
	// another failure. This should not be the case because the server will
	// wait 5s before closing the connection.
	select {
	case <-l.connCh:
		t.Fatal("Should not have tried to reconnect")
	case <-time.After(250 * time.Millisecond):
		// OK
	}
}

func TestLeafNodeStreamAndShadowSubs(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		port: -1
		leafnodes {
			port: -1
			authorization: {
			  user: leaf
			  password: leaf
			  account: B
			}
		}
		accounts: {
			A: {
			  users = [{user: usrA, password: usrA}]
			  exports: [{stream: foo.*.>}]
			}
			B: {
			  imports: [{stream: {account: A, subject: foo.*.>}}]
			}
		}
	`))
	hub, hubo := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	leafConfContet := fmt.Sprintf(`
		port: -1
		leafnodes {
			remotes = [
				{
					url: "nats-leaf://leaf:leaf@127.0.0.1:%d"
					account: B
				}
			]
		}
		accounts: {
			B: {
				exports: [{stream: foo.*.>}]
			}
			C: {
				users: [{user: usrC, password: usrC}]
				imports: [{stream: {account: B, subject: foo.bar.>}}]
			}
		}
	`, hubo.LeafNode.Port)
	leafConf := createConfFile(t, []byte(leafConfContet))
	leafo := LoadConfig(leafConf)
	leafo.LeafNode.ReconnectInterval = 50 * time.Millisecond
	leaf := RunServer(leafo)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	subPubAndCheck := func() {
		t.Helper()

		ncl, err := nats.Connect(leaf.ClientURL(), nats.UserInfo("usrC", "usrC"))
		if err != nil {
			t.Fatalf("Error connecting: %v", err)
		}
		defer ncl.Close()

		// This will send an LS+ to the "hub" server.
		sub, err := ncl.SubscribeSync("foo.*.baz")
		if err != nil {
			t.Fatalf("Error subscribing: %v", err)
		}
		ncl.Flush()

		ncm, err := nats.Connect(hub.ClientURL(), nats.UserInfo("usrA", "usrA"))
		if err != nil {
			t.Fatalf("Error connecting: %v", err)
		}
		defer ncm.Close()

		// Try a few times in case subject interest has not propagated yet
		for i := 0; i < 5; i++ {
			ncm.Publish("foo.bar.baz", []byte("msg"))
			if _, err := sub.NextMsg(time.Second); err == nil {
				// OK, done!
				return
			}
		}
		t.Fatal("Message was not received")
	}
	subPubAndCheck()

	// Now cause a restart of the accepting side so that the leaf connection
	// is recreated.
	hub.Shutdown()
	hub = RunServer(hubo)
	defer hub.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	subPubAndCheck()

	// Issue a config reload even though we make no modification. There was
	// a defect that caused the interest propagation to break.
	// Set the ReconnectInterval to the default value so that reload does not complain.
	leaf.getOpts().LeafNode.ReconnectInterval = DEFAULT_LEAF_NODE_RECONNECT
	reloadUpdateConfig(t, leaf, leafConf, leafConfContet)

	// Check again
	subPubAndCheck()
}

func TestLeafNodeAuthConfigReload(t *testing.T) {
	template := `
		listen: 127.0.0.1:-1
		accounts { test: {} }
		leaf {
			listen: "127.0.0.1:7422"
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file:  "../test/configs/certs/server-key.pem"
				ca_file:   "../test/configs/certs/ca.pem"
			}
			authorization {
				# These are only fields allowed atm.
				users = [ { user: test, password: "s3cret1", account: "test"  } ]
			}
		}
	`
	conf := createConfFile(t, []byte(template))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	lg := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(lg, false, false)

	// Reload here should work ok.
	reloadUpdateConfig(t, s, conf, template)
}

func TestLeafNodeSignatureCB(t *testing.T) {
	content := `
		port: -1
		server_name: OP
		operator = "../test/configs/nkeys/op.jwt"
		resolver = MEMORY
		listen: "127.0.0.1:-1"
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`
	conf := createConfFile(t, []byte(content))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	_, akp := createAccount(s)
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	nuc := jwt.NewUserClaims(pub)
	ujwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	lopts := &DefaultTestOptions
	u, err := url.Parse(fmt.Sprintf("nats://%s:%d", opts.LeafNode.Host, opts.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}
	remote := &RemoteLeafOpts{URLs: []*url.URL{u}}
	remote.SignatureCB = func(nonce []byte) (string, []byte, error) {
		return "", nil, fmt.Errorf("on purpose")
	}
	lopts.LeafNode.Remotes = []*RemoteLeafOpts{remote}
	lopts.LeafNode.ReconnectInterval = 100 * time.Millisecond
	sl := RunServer(lopts)
	defer sl.Shutdown()

	slog := &captureErrorLogger{errCh: make(chan string, 10)}
	sl.SetLogger(slog, false, false)

	// Now check that the leafnode got the error that the callback returned.
	select {
	case err := <-slog.errCh:
		if !strings.Contains(err, "on purpose") {
			t.Fatalf("Expected error from cb, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Did not get expected error")
	}

	sl.Shutdown()
	// Now check what happens if the connection is closed while in the callback.
	blockCh := make(chan struct{})
	remote.SignatureCB = func(nonce []byte) (string, []byte, error) {
		<-blockCh
		sig, err := kp.Sign(nonce)
		return ujwt, sig, err
	}
	sl = RunServer(lopts)
	defer sl.Shutdown()

	// Recreate the logger so that we are sure not to have possible previous errors
	slog = &captureErrorLogger{errCh: make(chan string, 10)}
	sl.SetLogger(slog, false, false)

	// Get the leaf connection from the temp clients map and close it.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		var c *client
		sl.grMu.Lock()
		for _, cli := range sl.grTmpClients {
			c = cli
		}
		sl.grMu.Unlock()
		if c == nil {
			return fmt.Errorf("Client still not found in temp map")
		}
		c.closeConnection(ClientClosed)
		return nil
	})

	// Release the callback, and check we get the appropriate error.
	close(blockCh)
	select {
	case err := <-slog.errCh:
		if !strings.Contains(err, ErrConnectionClosed.Error()) {
			t.Fatalf("Expected error that connection was closed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Did not get expected error")
	}

	sl.Shutdown()
	// Change to a good CB and now it should work
	remote.SignatureCB = func(nonce []byte) (string, []byte, error) {
		sig, err := kp.Sign(nonce)
		return ujwt, sig, err
	}
	sl = RunServer(lopts)
	defer sl.Shutdown()
	checkLeafNodeConnected(t, sl)
}

type testLeafTraceLogger struct {
	DummyLogger
	ch chan string
}

func (l *testLeafTraceLogger) Tracef(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	// We will sub to 'baz' and to 'bar', so filter on 'ba' prefix.
	if strings.Contains(msg, "[LS+ ba") {
		select {
		case l.ch <- msg:
		default:
		}
	}
}

// Make sure permissioned denied subs do not make it to the leafnode even if existing.
func TestLeafNodePermsSuppressSubs(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		authorization {
		  PERMS = {
		    publish = "foo"
		    subscribe = ["_INBOX.>"]
		  }
		  users = [
		    {user: "user", password: "pass"}
		    {user: "ln",  password: "pass" , permissions: $PERMS }
		  ]
		}
		no_auth_user: user

		leafnodes {
		  listen: 127.0.0.1:7422
		}
	`))

	lconf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		leafnodes {
		  remotes = [ { url: "nats://ln:pass@127.0.0.1" } ]
		}
		trace = true
	`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Connect client to the hub.
	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)

	// This should not be seen on leafnode side since we only allow pub to "foo"
	_, err = nc.SubscribeSync("baz")
	require_NoError(t, err)

	ln, _ := RunServerWithConfig(lconf)
	defer ln.Shutdown()

	// Setup logger to capture trace events.
	l := &testLeafTraceLogger{ch: make(chan string, 10)}
	ln.SetLogger(l, true, true)

	checkLeafNodeConnected(t, ln)

	// Need to have ot reconnect to trigger since logger attaches too late.
	ln.mu.Lock()
	for _, c := range ln.leafs {
		c.mu.Lock()
		c.nc.Close()
		c.mu.Unlock()
	}
	ln.mu.Unlock()
	checkLeafNodeConnectedCount(t, ln, 0)
	checkLeafNodeConnectedCount(t, ln, 1)

	select {
	case msg := <-l.ch:
		t.Fatalf("Unexpected LS+ seen on leafnode: %s", msg)
	case <-time.After(50 * time.Millisecond):
		// OK
	}

	// Now double check that new subs also do not propagate.
	// This behavior was working already.
	_, err = nc.SubscribeSync("bar")
	require_NoError(t, err)

	select {
	case msg := <-l.ch:
		t.Fatalf("Unexpected LS+ seen on leafnode: %s", msg)
	case <-time.After(50 * time.Millisecond):
		// OK
	}
}

func TestLeafNodeDuplicateMsg(t *testing.T) {
	// This involves 2 clusters with leafnodes to each other with a different
	// account, and those accounts import/export a subject that caused
	// duplicate messages. This test requires static ports since we need to
	// have A->B and B->A.
	a1Conf := createConfFile(t, []byte(`
	cluster : {
		name : A
		port : -1
	}
	leafnodes : {
		port : 14333
		remotes : [{
			account : A
			urls : [nats://leafa:pwd@127.0.0.1:24333]
		}]
	}
	port : -1
	server_name : A_1

	accounts:{
		A:{
			users:[
				{user: leafa, password: pwd},
				{user: usera, password: usera, permissions: {
					publish:{ allow:["iot.b.topic"] }
					subscribe:{ allow:["iot.a.topic"] }
				}}
			]
			imports:[
				{stream:{account:"B", subject:"iot.a.topic"}}
			]
		},
		B:{
			users:[
				{user: leafb, password: pwd},
			]
			exports:[
				{stream: "iot.a.topic", accounts: ["A"]}
			]
		}
	}
	`))
	a1, oa1 := RunServerWithConfig(a1Conf)
	defer a1.Shutdown()

	a2Conf := createConfFile(t, []byte(fmt.Sprintf(`
	cluster : {
		name : A
		port : -1
		routes : [nats://127.0.0.1:%d]
	}
	leafnodes : {
		port : 14334
		remotes : [{
			account : A
			urls : [nats://leafa:pwd@127.0.0.1:24334]
		}]
	}
	port : -1
	server_name : A_2

	accounts:{
		A:{
			users:[
				{user: leafa, password: pwd},
				{user: usera, password: usera, permissions: {
					publish:{ allow:["iot.b.topic"] }
					subscribe:{ allow:["iot.a.topic"] }
				}}
			]
			imports:[
				{stream:{account:"B", subject:"iot.a.topic"}}
			]
		},
		B:{
			users:[
				{user: leafb, password: pwd},
			]
			exports:[
				{stream: "iot.a.topic", accounts: ["A"]}
			]
		}
	}`, oa1.Cluster.Port)))
	a2, _ := RunServerWithConfig(a2Conf)
	defer a2.Shutdown()

	checkClusterFormed(t, a1, a2)

	b1Conf := createConfFile(t, []byte(`
	cluster : {
		name : B
		port : -1
	}
	leafnodes : {
		port : 24333
		remotes : [{
			account : B
			urls : [nats://leafb:pwd@127.0.0.1:14333]
		}]
	}
	port : -1
	server_name : B_1

	accounts:{
		A:{
			users:[
				{user: leafa, password: pwd},
			]
			exports:[
				{stream: "iot.b.topic", accounts: ["B"]}
			]
		},
		B:{
			users:[
				{user: leafb, password: pwd},
				{user: userb, password: userb, permissions: {
					publish:{ allow:["iot.a.topic"] },
					subscribe:{ allow:["iot.b.topic"] }
				}}
			]
			imports:[
				{stream:{account:"A", subject:"iot.b.topic"}}
			]
		}
	}`))
	b1, ob1 := RunServerWithConfig(b1Conf)
	defer b1.Shutdown()

	b2Conf := createConfFile(t, []byte(fmt.Sprintf(`
	cluster : {
		name : B
		port : -1
		routes : [nats://127.0.0.1:%d]
	}
	leafnodes : {
		port : 24334
		remotes : [{
			account : B
			urls : [nats://leafb:pwd@127.0.0.1:14334]
		}]
	}
	port : -1
	server_name : B_2

	accounts:{
		A:{
			users:[
				{user: leafa, password: pwd},
			]
			exports:[
				{stream: "iot.b.topic", accounts: ["B"]}
			]
		},
		B:{
			users:[
				{user: leafb, password: pwd},
				{user: userb, password: userb, permissions: {
					publish:{ allow:["iot.a.topic"] },
					subscribe:{ allow:["iot.b.topic"] }
				}}
			]
			imports:[
				{stream:{account:"A", subject:"iot.b.topic"}}
			]
		}
	}`, ob1.Cluster.Port)))
	b2, _ := RunServerWithConfig(b2Conf)
	defer b2.Shutdown()

	checkClusterFormed(t, b1, b2)

	checkLeafNodeConnectedCount(t, a1, 2)
	checkLeafNodeConnectedCount(t, a2, 2)
	checkLeafNodeConnectedCount(t, b1, 2)
	checkLeafNodeConnectedCount(t, b2, 2)

	check := func(t *testing.T, subSrv *Server, pubSrv *Server) {

		sc := natsConnect(t, subSrv.ClientURL(), nats.UserInfo("userb", "userb"))
		defer sc.Close()

		subject := "iot.b.topic"
		sub := natsSubSync(t, sc, subject)

		// Wait for this to be available in A cluster
		checkSubInterest(t, a1, "A", subject, time.Second)
		checkSubInterest(t, a2, "A", subject, time.Second)

		pb := natsConnect(t, pubSrv.ClientURL(), nats.UserInfo("usera", "usera"))
		defer pb.Close()

		natsPub(t, pb, subject, []byte("msg"))
		natsNexMsg(t, sub, time.Second)
		// Should be only 1
		if msg, err := sub.NextMsg(100 * time.Millisecond); err == nil {
			t.Fatalf("Received duplicate on %q: %s", msg.Subject, msg.Data)
		}
	}
	t.Run("sub_b1_pub_a1", func(t *testing.T) { check(t, b1, a1) })
	t.Run("sub_b1_pub_a2", func(t *testing.T) { check(t, b1, a2) })
	t.Run("sub_b2_pub_a1", func(t *testing.T) { check(t, b2, a1) })
	t.Run("sub_b2_pub_a2", func(t *testing.T) { check(t, b2, a2) })
}

func TestLeafNodeTLSHandshakeFirstVerifyNoInfoSent(t *testing.T) {
	confHub := createConfFile(t, []byte(`
		port : -1
		leafnodes : {
			port : -1
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file:  "../test/configs/certs/server-key.pem"
				ca_file: "../test/configs/certs/ca.pem"
				timeout: 2
				handshake_first: true
			}
		}
	`))
	s1, o1 := RunServerWithConfig(confHub)
	defer s1.Shutdown()

	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", o1.LeafNode.Port), 2*time.Second)
	require_NoError(t, err)
	defer c.Close()

	buf := make([]byte, 1024)
	// We will wait for up to 500ms to see if the server is sending (incorrectly)
	// the INFO.
	c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	n, err := c.Read(buf)
	c.SetReadDeadline(time.Time{})
	// If we did not get an error, this is an issue...
	if err == nil {
		t.Fatalf("Should not have received anything, got n=%v buf=%s", n, buf[:n])
	}
	// We expect a timeout error
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("Expected a timeout error, got %v", err)
	}
}

func TestLeafNodeTLSHandshakeFirst(t *testing.T) {
	tmpl1 := `
		port : -1
		leafnodes : {
			port : -1
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file:  "../test/configs/certs/server-key.pem"
				ca_file: "../test/configs/certs/ca.pem"
				timeout: 2
				handshake_first: %s
			}
		}
	`
	confHub := createConfFile(t, []byte(fmt.Sprintf(tmpl1, "true")))
	s1, o1 := RunServerWithConfig(confHub)
	defer s1.Shutdown()

	tmpl2 := `
		port: -1
		leafnodes : {
			port : -1
			remotes : [
				{
					urls : [tls://127.0.0.1:%d]
					tls {
						cert_file: "../test/configs/certs/client-cert.pem"
						key_file:  "../test/configs/certs/client-key.pem"
						ca_file: "../test/configs/certs/ca.pem"
						timeout: 2
						first: %s
					}
				}
			]
		}
	`
	confSpoke := createConfFile(t, []byte(fmt.Sprintf(tmpl2, o1.LeafNode.Port, "true")))
	s2, _ := RunServerWithConfig(confSpoke)
	defer s2.Shutdown()

	checkLeafNodeConnected(t, s2)

	s2.Shutdown()

	// Now check that there will be a failure if the remote does not ask for
	// handshake first since the hub is configured that way.
	// Set a logger on s1 to capture errors
	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s1.SetLogger(l, false, false)

	confSpoke = createConfFile(t, []byte(fmt.Sprintf(tmpl2, o1.LeafNode.Port, "false")))
	s2, _ = RunServerWithConfig(confSpoke)
	defer s2.Shutdown()

	select {
	case err := <-l.errCh:
		if !strings.Contains(err, "handshake error") {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Did not get TLS handshake failure")
	}

	// Check configuration reload for this remote
	reloadUpdateConfig(t, s2, confSpoke, fmt.Sprintf(tmpl2, o1.LeafNode.Port, "true"))
	checkLeafNodeConnected(t, s2)
	s2.Shutdown()

	// Drain the logger error channel
	for done := false; !done; {
		select {
		case <-l.errCh:
		default:
			done = true
		}
	}

	// Now change the config on the hub
	reloadUpdateConfig(t, s1, confHub, fmt.Sprintf(tmpl1, "false"))
	// Restart s2
	s2, _ = RunServerWithConfig(confSpoke)
	defer s2.Shutdown()

	select {
	case err := <-l.errCh:
		if !strings.Contains(err, "handshake error") {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Did not get TLS handshake failure")
	}

	// Reload again with "true"
	reloadUpdateConfig(t, s1, confHub, fmt.Sprintf(tmpl1, "true"))
	checkLeafNodeConnected(t, s2)
}

func TestLeafNodeCompressionOptions(t *testing.T) {
	org := testDefaultLeafNodeCompression
	testDefaultLeafNodeCompression = _EMPTY_
	defer func() { testDefaultLeafNodeCompression = org }()

	tmpl := `
		port: -1
		leafnodes {
			port: -1
			compression: %s
		}
	`
	for _, test := range []struct {
		name     string
		mode     string
		rttVals  []int
		expected string
		rtts     []time.Duration
	}{
		{"boolean enabled", "true", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string enabled", "enabled", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string EnaBled", "EnaBled", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string on", "on", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string ON", "ON", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string fast", "fast", nil, CompressionS2Fast, nil},
		{"string Fast", "Fast", nil, CompressionS2Fast, nil},
		{"string s2_fast", "s2_fast", nil, CompressionS2Fast, nil},
		{"string s2_Fast", "s2_Fast", nil, CompressionS2Fast, nil},
		{"boolean disabled", "false", nil, CompressionOff, nil},
		{"string disabled", "disabled", nil, CompressionOff, nil},
		{"string DisableD", "DisableD", nil, CompressionOff, nil},
		{"string off", "off", nil, CompressionOff, nil},
		{"string OFF", "OFF", nil, CompressionOff, nil},
		{"better", "better", nil, CompressionS2Better, nil},
		{"Better", "Better", nil, CompressionS2Better, nil},
		{"s2_better", "s2_better", nil, CompressionS2Better, nil},
		{"S2_BETTER", "S2_BETTER", nil, CompressionS2Better, nil},
		{"best", "best", nil, CompressionS2Best, nil},
		{"BEST", "BEST", nil, CompressionS2Best, nil},
		{"s2_best", "s2_best", nil, CompressionS2Best, nil},
		{"S2_BEST", "S2_BEST", nil, CompressionS2Best, nil},
		{"auto no rtts", "auto", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"s2_auto no rtts", "s2_auto", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"auto", "{mode: auto, rtt_thresholds: [%s]}", []int{1}, CompressionS2Auto, []time.Duration{time.Millisecond}},
		{"Auto", "{Mode: Auto, thresholds: [%s]}", []int{1, 2}, CompressionS2Auto, []time.Duration{time.Millisecond, 2 * time.Millisecond}},
		{"s2_auto", "{mode: s2_auto, thresholds: [%s]}", []int{1, 2, 3}, CompressionS2Auto, []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond}},
		{"s2_AUTO", "{mode: s2_AUTO, thresholds: [%s]}", []int{1, 2, 3, 4}, CompressionS2Auto, []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond}},
		{"s2_auto:-10,5,10", "{mode: s2_auto, thresholds: [%s]}", []int{-10, 5, 10}, CompressionS2Auto, []time.Duration{0, 5 * time.Millisecond, 10 * time.Millisecond}},
		{"s2_auto:5,10,15", "{mode: s2_auto, thresholds: [%s]}", []int{5, 10, 15}, CompressionS2Auto, []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 15 * time.Millisecond}},
		{"s2_auto:0,5,10", "{mode: s2_auto, thresholds: [%s]}", []int{0, 5, 10}, CompressionS2Auto, []time.Duration{0, 5 * time.Millisecond, 10 * time.Millisecond}},
		{"s2_auto:5,10,0,20", "{mode: s2_auto, thresholds: [%s]}", []int{5, 10, 0, 20}, CompressionS2Auto, []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 0, 20 * time.Millisecond}},
		{"s2_auto:0,10,0,20", "{mode: s2_auto, thresholds: [%s]}", []int{0, 10, 0, 20}, CompressionS2Auto, []time.Duration{0, 10 * time.Millisecond, 0, 20 * time.Millisecond}},
		{"s2_auto:0,0,0,20", "{mode: s2_auto, thresholds: [%s]}", []int{0, 0, 0, 20}, CompressionS2Auto, []time.Duration{0, 0, 0, 20 * time.Millisecond}},
		{"s2_auto:0,10,0,0", "{mode: s2_auto, rtt_thresholds: [%s]}", []int{0, 10, 0, 0}, CompressionS2Auto, []time.Duration{0, 10 * time.Millisecond}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var val string
			if len(test.rttVals) > 0 {
				var rtts string
				for i, v := range test.rttVals {
					if i > 0 {
						rtts += ", "
					}
					rtts += fmt.Sprintf("%dms", v)
				}
				val = fmt.Sprintf(test.mode, rtts)
			} else {
				val = test.mode
			}
			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, val)))
			s, o := RunServerWithConfig(conf)
			defer s.Shutdown()

			if cm := o.LeafNode.Compression.Mode; cm != test.expected {
				t.Fatalf("Expected compression value to be %q, got %q", test.expected, cm)
			}
			if !reflect.DeepEqual(test.rtts, o.LeafNode.Compression.RTTThresholds) {
				t.Fatalf("Expected RTT tresholds to be %+v, got %+v", test.rtts, o.LeafNode.Compression.RTTThresholds)
			}
			s.Shutdown()

			o.LeafNode.Port = -1
			o.LeafNode.Compression.Mode = test.mode
			if len(test.rttVals) > 0 {
				o.LeafNode.Compression.Mode = CompressionS2Auto
				o.LeafNode.Compression.RTTThresholds = o.LeafNode.Compression.RTTThresholds[:0]
				for _, v := range test.rttVals {
					o.LeafNode.Compression.RTTThresholds = append(o.LeafNode.Compression.RTTThresholds, time.Duration(v)*time.Millisecond)
				}
			}
			s = RunServer(o)
			defer s.Shutdown()
			if cm := o.LeafNode.Compression.Mode; cm != test.expected {
				t.Fatalf("Expected compression value to be %q, got %q", test.expected, cm)
			}
			if !reflect.DeepEqual(test.rtts, o.LeafNode.Compression.RTTThresholds) {
				t.Fatalf("Expected RTT tresholds to be %+v, got %+v", test.rtts, o.LeafNode.Compression.RTTThresholds)
			}
		})
	}

	// Same, but with remotes
	tmpl = `
		port: -1
		leafnodes {
			port: -1
			remotes [
				{
					url: "nats://127.0.0.1:1234"
					compression: %s
				}
			]
		}
	`
	for _, test := range []struct {
		name     string
		mode     string
		rttVals  []int
		expected string
		rtts     []time.Duration
	}{
		{"boolean enabled", "true", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string enabled", "enabled", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string EnaBled", "EnaBled", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string on", "on", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string ON", "ON", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string fast", "fast", nil, CompressionS2Fast, nil},
		{"string Fast", "Fast", nil, CompressionS2Fast, nil},
		{"string s2_fast", "s2_fast", nil, CompressionS2Fast, nil},
		{"string s2_Fast", "s2_Fast", nil, CompressionS2Fast, nil},
		{"boolean disabled", "false", nil, CompressionOff, nil},
		{"string disabled", "disabled", nil, CompressionOff, nil},
		{"string DisableD", "DisableD", nil, CompressionOff, nil},
		{"string off", "off", nil, CompressionOff, nil},
		{"string OFF", "OFF", nil, CompressionOff, nil},
		{"better", "better", nil, CompressionS2Better, nil},
		{"Better", "Better", nil, CompressionS2Better, nil},
		{"s2_better", "s2_better", nil, CompressionS2Better, nil},
		{"S2_BETTER", "S2_BETTER", nil, CompressionS2Better, nil},
		{"best", "best", nil, CompressionS2Best, nil},
		{"BEST", "BEST", nil, CompressionS2Best, nil},
		{"s2_best", "s2_best", nil, CompressionS2Best, nil},
		{"S2_BEST", "S2_BEST", nil, CompressionS2Best, nil},
		{"auto no rtts", "auto", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"s2_auto no rtts", "s2_auto", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"auto", "{mode: auto, rtt_thresholds: [%s]}", []int{1}, CompressionS2Auto, []time.Duration{time.Millisecond}},
		{"Auto", "{Mode: Auto, thresholds: [%s]}", []int{1, 2}, CompressionS2Auto, []time.Duration{time.Millisecond, 2 * time.Millisecond}},
		{"s2_auto", "{mode: s2_auto, thresholds: [%s]}", []int{1, 2, 3}, CompressionS2Auto, []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond}},
		{"s2_AUTO", "{mode: s2_AUTO, thresholds: [%s]}", []int{1, 2, 3, 4}, CompressionS2Auto, []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond}},
		{"s2_auto:-10,5,10", "{mode: s2_auto, thresholds: [%s]}", []int{-10, 5, 10}, CompressionS2Auto, []time.Duration{0, 5 * time.Millisecond, 10 * time.Millisecond}},
		{"s2_auto:5,10,15", "{mode: s2_auto, thresholds: [%s]}", []int{5, 10, 15}, CompressionS2Auto, []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 15 * time.Millisecond}},
		{"s2_auto:0,5,10", "{mode: s2_auto, thresholds: [%s]}", []int{0, 5, 10}, CompressionS2Auto, []time.Duration{0, 5 * time.Millisecond, 10 * time.Millisecond}},
		{"s2_auto:5,10,0,20", "{mode: s2_auto, thresholds: [%s]}", []int{5, 10, 0, 20}, CompressionS2Auto, []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 0, 20 * time.Millisecond}},
		{"s2_auto:0,10,0,20", "{mode: s2_auto, thresholds: [%s]}", []int{0, 10, 0, 20}, CompressionS2Auto, []time.Duration{0, 10 * time.Millisecond, 0, 20 * time.Millisecond}},
		{"s2_auto:0,0,0,20", "{mode: s2_auto, thresholds: [%s]}", []int{0, 0, 0, 20}, CompressionS2Auto, []time.Duration{0, 0, 0, 20 * time.Millisecond}},
		{"s2_auto:0,10,0,0", "{mode: s2_auto, rtt_thresholds: [%s]}", []int{0, 10, 0, 0}, CompressionS2Auto, []time.Duration{0, 10 * time.Millisecond}},
	} {
		t.Run("remote leaf "+test.name, func(t *testing.T) {
			var val string
			if len(test.rttVals) > 0 {
				var rtts string
				for i, v := range test.rttVals {
					if i > 0 {
						rtts += ", "
					}
					rtts += fmt.Sprintf("%dms", v)
				}
				val = fmt.Sprintf(test.mode, rtts)
			} else {
				val = test.mode
			}
			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, val)))
			s, o := RunServerWithConfig(conf)
			defer s.Shutdown()

			r := o.LeafNode.Remotes[0]

			if cm := r.Compression.Mode; cm != test.expected {
				t.Fatalf("Expected compression value to be %q, got %q", test.expected, cm)
			}
			if !reflect.DeepEqual(test.rtts, r.Compression.RTTThresholds) {
				t.Fatalf("Expected RTT tresholds to be %+v, got %+v", test.rtts, r.Compression.RTTThresholds)
			}
			s.Shutdown()

			o.LeafNode.Port = -1
			o.LeafNode.Remotes[0].Compression.Mode = test.mode
			if len(test.rttVals) > 0 {
				o.LeafNode.Remotes[0].Compression.Mode = CompressionS2Auto
				o.LeafNode.Remotes[0].Compression.RTTThresholds = o.LeafNode.Remotes[0].Compression.RTTThresholds[:0]
				for _, v := range test.rttVals {
					o.LeafNode.Remotes[0].Compression.RTTThresholds = append(o.LeafNode.Remotes[0].Compression.RTTThresholds, time.Duration(v)*time.Millisecond)
				}
			}
			s = RunServer(o)
			defer s.Shutdown()
			if cm := o.LeafNode.Remotes[0].Compression.Mode; cm != test.expected {
				t.Fatalf("Expected compression value to be %q, got %q", test.expected, cm)
			}
			if !reflect.DeepEqual(test.rtts, o.LeafNode.Remotes[0].Compression.RTTThresholds) {
				t.Fatalf("Expected RTT tresholds to be %+v, got %+v", test.rtts, o.LeafNode.Remotes[0].Compression.RTTThresholds)
			}
		})
	}

	// Test that with no compression specified, we default to "s2_auto"
	conf := createConfFile(t, []byte(`
		port: -1
		leafnodes {
			port: -1
		}
	`))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()
	if o.LeafNode.Compression.Mode != CompressionS2Auto {
		t.Fatalf("Expected compression value to be %q, got %q", CompressionAccept, o.LeafNode.Compression.Mode)
	}
	if !reflect.DeepEqual(defaultCompressionS2AutoRTTThresholds, o.LeafNode.Compression.RTTThresholds) {
		t.Fatalf("Expected RTT tresholds to be %+v, got %+v", defaultCompressionS2AutoRTTThresholds, o.LeafNode.Compression.RTTThresholds)
	}
	// Same for remotes
	conf = createConfFile(t, []byte(`
		port: -1
		leafnodes {
			port: -1
			remotes [ { url: "nats://127.0.0.1:1234" } ]
		}
	`))
	s, o = RunServerWithConfig(conf)
	defer s.Shutdown()
	if cm := o.LeafNode.Remotes[0].Compression.Mode; cm != CompressionS2Auto {
		t.Fatalf("Expected compression value to be %q, got %q", CompressionAccept, cm)
	}
	if !reflect.DeepEqual(defaultCompressionS2AutoRTTThresholds, o.LeafNode.Remotes[0].Compression.RTTThresholds) {
		t.Fatalf("Expected RTT tresholds to be %+v, got %+v", defaultCompressionS2AutoRTTThresholds, o.LeafNode.Remotes[0].Compression.RTTThresholds)
	}
	for _, test := range []struct {
		name string
		mode string
		rtts []time.Duration
		err  string
	}{
		{"unsupported mode", "gzip", nil, "unsupported"},
		{"not ascending order", "s2_auto", []time.Duration{
			5 * time.Millisecond,
			10 * time.Millisecond,
			2 * time.Millisecond,
		}, "ascending"},
		{"too many thresholds", "s2_auto", []time.Duration{
			5 * time.Millisecond,
			10 * time.Millisecond,
			20 * time.Millisecond,
			40 * time.Millisecond,
			60 * time.Millisecond,
		}, "more than 4"},
		{"all 0", "s2_auto", []time.Duration{0, 0, 0, 0}, "at least one"},
		{"single 0", "s2_auto", []time.Duration{0}, "at least one"},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := DefaultOptions()
			o.LeafNode.Port = -1
			o.LeafNode.Compression = CompressionOpts{test.mode, test.rtts}
			if _, err := NewServer(o); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Same with remotes
			o.LeafNode.Compression = CompressionOpts{}
			o.LeafNode.Remotes = []*RemoteLeafOpts{{Compression: CompressionOpts{test.mode, test.rtts}}}
			if _, err := NewServer(o); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestLeafNodeCompression(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "Hub"
		accounts {
			A { users: [{user: a, password: pwd}] }
			B { users: [{user: b, password: pwd}] }
			C { users: [{user: c, password: pwd}] }
		}
		leafnodes {
			port: -1
			compression: s2_fast
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	port := o1.LeafNode.Port
	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "Spoke"
		accounts {
			A { users: [{user: a, password: pwd}] }
			B { users: [{user: b, password: pwd}] }
			C { users: [{user: c, password: pwd}] }
		}
		leafnodes {
			remotes [
				{ url: "nats://a:pwd@127.0.0.1:%d", account: "A", compression: s2_better }
				{ url: "nats://b:pwd@127.0.0.1:%d", account: "B", compression: s2_best }
				{ url: "nats://c:pwd@127.0.0.1:%d", account: "C", compression: off }
			]
		}
	`, port, port, port)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnectedCount(t, s1, 3)
	checkLeafNodeConnectedCount(t, s2, 3)

	s1.mu.RLock()
	for _, l := range s1.leafs {
		l.mu.Lock()
		l.nc = &testConnSentBytes{Conn: l.nc}
		l.mu.Unlock()
	}
	s1.mu.RUnlock()

	var payloads [][]byte
	totalPayloadSize := 0
	count := 26
	for i := 0; i < count; i++ {
		n := rand.Intn(2048) + 1
		p := make([]byte, n)
		for j := 0; j < n; j++ {
			p[j] = byte(i) + 'A'
		}
		totalPayloadSize += len(p)
		payloads = append(payloads, p)
	}

	check := func(acc, user, subj string) {
		t.Helper()
		nc2 := natsConnect(t, s2.ClientURL(), nats.UserInfo(user, "pwd"))
		defer nc2.Close()
		sub := natsSubSync(t, nc2, subj)
		natsFlush(t, nc2)
		checkSubInterest(t, s1, acc, subj, time.Second)

		nc1 := natsConnect(t, s1.ClientURL(), nats.UserInfo(user, "pwd"))
		defer nc1.Close()

		for i := 0; i < count; i++ {
			natsPub(t, nc1, subj, payloads[i])
		}
		for i := 0; i < count; i++ {
			m := natsNexMsg(t, sub, time.Second)
			if !bytes.Equal(m.Data, payloads[i]) {
				t.Fatalf("Expected payload %q - got %q", payloads[i], m.Data)
			}
		}

		// Also check that the leafnode stats shows that compression likely occurred
		var out int
		s1.mu.RLock()
		for _, l := range s1.leafs {
			l.mu.Lock()
			if l.acc.Name == acc && l.nc != nil {
				nc := l.nc.(*testConnSentBytes)
				nc.Lock()
				out = nc.sent
				nc.sent = 0
				nc.Unlock()
			}
			l.mu.Unlock()
		}
		s1.mu.RUnlock()
		// Except for account "C", where compression should be off,
		// "out" should at least be smaller than totalPayloadSize, use 20%.
		if acc == "C" {
			if int(out) < totalPayloadSize {
				t.Fatalf("Expected s1's sent bytes to be at least payload size (%v), got %v", totalPayloadSize, out)
			}
		} else {
			limit := totalPayloadSize * 80 / 100
			if int(out) > limit {
				t.Fatalf("Expected s1's sent bytes to be less than %v, got %v (total payload was %v)", limit, out, totalPayloadSize)
			}
		}
	}
	check("A", "a", "foo")
	check("B", "b", "bar")
	check("C", "c", "baz")

	// Check compression settings. S1 should always be s2_fast, except for account "C"
	// since "C" wanted compression "off"
	l, err := s1.Leafz(nil)
	require_NoError(t, err)
	for _, r := range l.Leafs {
		switch r.Account {
		case "C":
			if r.Compression != CompressionOff {
				t.Fatalf("Expected compression of remote for C account to be %q, got %q", CompressionOff, r.Compression)
			}
		default:
			if r.Compression != CompressionS2Fast {
				t.Fatalf("Expected compression of remote for %s account to be %q, got %q", r.Account, CompressionS2Fast, r.Compression)
			}
		}
	}

	l, err = s2.Leafz(nil)
	require_NoError(t, err)
	for _, r := range l.Leafs {
		switch r.Account {
		case "A":
			if r.Compression != CompressionS2Better {
				t.Fatalf("Expected compression for A account to be %q, got %q", CompressionS2Better, r.Compression)
			}
		case "B":
			if r.Compression != CompressionS2Best {
				t.Fatalf("Expected compression for B account to be %q, got %q", CompressionS2Best, r.Compression)
			}
		case "C":
			if r.Compression != CompressionOff {
				t.Fatalf("Expected compression for C account to be %q, got %q", CompressionOff, r.Compression)
			}
		}
	}
}

func TestLeafNodeCompressionMatrixModes(t *testing.T) {
	for _, test := range []struct {
		name       string
		s1         string
		s2         string
		s1Expected string
		s2Expected string
	}{
		{"off off", "off", "off", CompressionOff, CompressionOff},
		{"off accept", "off", "accept", CompressionOff, CompressionOff},
		{"off on", "off", "on", CompressionOff, CompressionOff},
		{"off better", "off", "better", CompressionOff, CompressionOff},
		{"off best", "off", "best", CompressionOff, CompressionOff},

		{"accept off", "accept", "off", CompressionOff, CompressionOff},
		{"accept accept", "accept", "accept", CompressionOff, CompressionOff},
		// Note: "on", means s2_auto, which will mean uncompressed since RTT is low.
		{"accept on", "accept", "on", CompressionS2Fast, CompressionS2Uncompressed},
		{"accept better", "accept", "better", CompressionS2Better, CompressionS2Better},
		{"accept best", "accept", "best", CompressionS2Best, CompressionS2Best},

		{"on off", "on", "off", CompressionOff, CompressionOff},
		{"on accept", "on", "accept", CompressionS2Uncompressed, CompressionS2Fast},
		{"on on", "on", "on", CompressionS2Uncompressed, CompressionS2Uncompressed},
		{"on better", "on", "better", CompressionS2Uncompressed, CompressionS2Better},
		{"on best", "on", "best", CompressionS2Uncompressed, CompressionS2Best},

		{"better off", "better", "off", CompressionOff, CompressionOff},
		{"better accept", "better", "accept", CompressionS2Better, CompressionS2Better},
		{"better on", "better", "on", CompressionS2Better, CompressionS2Uncompressed},
		{"better better", "better", "better", CompressionS2Better, CompressionS2Better},
		{"better best", "better", "best", CompressionS2Better, CompressionS2Best},

		{"best off", "best", "off", CompressionOff, CompressionOff},
		{"best accept", "best", "accept", CompressionS2Best, CompressionS2Best},
		{"best on", "best", "on", CompressionS2Best, CompressionS2Uncompressed},
		{"best better", "best", "better", CompressionS2Best, CompressionS2Better},
		{"best best", "best", "best", CompressionS2Best, CompressionS2Best},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf1 := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				server_name: "A"
				leafnodes {
					port: -1
					compression: %s
				}
			`, test.s1)))
			s1, o1 := RunServerWithConfig(conf1)
			defer s1.Shutdown()

			conf2 := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				server_name: "B"
				leafnodes {
					remotes: [
						{url: "nats://127.0.0.1:%d", compression: %s}
					]
				}
			`, o1.LeafNode.Port, test.s2)))
			s2, _ := RunServerWithConfig(conf2)
			defer s2.Shutdown()

			checkLeafNodeConnected(t, s2)

			nc1 := natsConnect(t, s1.ClientURL())
			defer nc1.Close()

			nc2 := natsConnect(t, s2.ClientURL())
			defer nc2.Close()

			payload := make([]byte, 128)
			check := func(ncp, ncs *nats.Conn, subj string, s *Server) {
				t.Helper()
				sub := natsSubSync(t, ncs, subj)
				checkSubInterest(t, s, globalAccountName, subj, time.Second)
				natsPub(t, ncp, subj, payload)
				natsNexMsg(t, sub, time.Second)

				for _, srv := range []*Server{s1, s2} {
					lz, err := srv.Leafz(nil)
					require_NoError(t, err)
					var expected string
					if srv == s1 {
						expected = test.s1Expected
					} else {
						expected = test.s2Expected
					}
					if cm := lz.Leafs[0].Compression; cm != expected {
						t.Fatalf("Server %s - expected compression %q, got %q", srv, expected, cm)
					}
				}
			}
			check(nc1, nc2, "foo", s1)
			check(nc2, nc1, "bar", s2)
		})
	}
}

func TestLeafNodeCompressionWithOlderServer(t *testing.T) {
	tmpl1 := `
		port: -1
		server_name: "A"
		leafnodes {
			port: -1
			compression: "%s"
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl1, CompressionS2Fast)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	tmpl2 := `
		port: -1
		server_name: "B"
		leafnodes {
			remotes [
				{url: "nats://127.0.0.1:%d", compression: "%s"}
			]
		}
	`
	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl2, o1.LeafNode.Port, CompressionNotSupported)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnected(t, s2)

	getLeafCompMode := func(s *Server) string {
		var cm string
		s.mu.RLock()
		defer s.mu.RUnlock()
		for _, l := range s1.leafs {
			l.mu.Lock()
			cm = l.leaf.compression
			l.mu.Unlock()
			return cm
		}
		return _EMPTY_
	}
	for _, s := range []*Server{s1, s2} {
		if cm := getLeafCompMode(s); cm != CompressionNotSupported {
			t.Fatalf("Expected compression not supported, got %q", cm)
		}
	}

	s2.Shutdown()
	s1.Shutdown()

	conf1 = createConfFile(t, []byte(fmt.Sprintf(tmpl1, CompressionNotSupported)))
	s1, o1 = RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 = createConfFile(t, []byte(fmt.Sprintf(tmpl2, o1.LeafNode.Port, CompressionS2Fast)))
	s2, _ = RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnected(t, s2)
	for _, s := range []*Server{s1, s2} {
		if cm := getLeafCompMode(s); cm != CompressionNotSupported {
			t.Fatalf("Expected compression not supported, got %q", cm)
		}
	}
}

func TestLeafNodeCompressionAuto(t *testing.T) {
	for _, test := range []struct {
		name          string
		s1Ping        string
		s1Compression string
		s2Ping        string
		s2Compression string
		checkS1       bool
	}{
		{"remote side", "10s", CompressionS2Fast, "100ms", "{mode: s2_auto, rtt_thresholds: [10ms, 20ms, 30ms]}", false},
		{"accept side", "100ms", "{mode: s2_auto, rtt_thresholds: [10ms, 20ms, 30ms]}", "10s", CompressionS2Fast, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf1 := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				server_name: "A"
				ping_interval: "%s"
				leafnodes {
					port: -1
					compression: %s
				}
			`, test.s1Ping, test.s1Compression)))
			s1, o1 := RunServerWithConfig(conf1)
			defer s1.Shutdown()

			// Start with 0ms RTT
			np := createNetProxy(0, 1024*1024*1024, 1024*1024*1024, fmt.Sprintf("nats://127.0.0.1:%d", o1.LeafNode.Port), true)

			conf2 := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				server_name: "B"
				ping_interval: "%s"
				leafnodes {
					remotes [
						{url: %s, compression %s}
					]
				}
			`, test.s2Ping, np.routeURL(), test.s2Compression)))
			s2, _ := RunServerWithConfig(conf2)
			defer s2.Shutdown()
			defer np.stop()

			checkLeafNodeConnected(t, s2)

			checkComp := func(expected string) {
				t.Helper()
				var s *Server
				if test.checkS1 {
					s = s1
				} else {
					s = s2
				}
				checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
					s.mu.RLock()
					defer s.mu.RUnlock()
					for _, l := range s.leafs {
						l.mu.Lock()
						cm := l.leaf.compression
						l.mu.Unlock()
						if cm != expected {
							return fmt.Errorf("Leaf %v compression mode expected to be %q, got %q", l, expected, cm)
						}
					}
					return nil
				})
			}
			checkComp(CompressionS2Uncompressed)

			// Change the proxy RTT and we should get compression "fast"
			np.updateRTT(15 * time.Millisecond)
			checkComp(CompressionS2Fast)

			// Now 25ms, and get "better"
			np.updateRTT(25 * time.Millisecond)
			checkComp(CompressionS2Better)

			// Above 35 and we should get "best"
			np.updateRTT(35 * time.Millisecond)
			checkComp(CompressionS2Best)

			// Down to 1ms and again should get "uncompressed"
			np.updateRTT(1 * time.Millisecond)
			checkComp(CompressionS2Uncompressed)
		})
	}

	// Make sure that if compression is off on one side, the update of RTT does
	// not trigger a compression change.
	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "A"
		leafnodes {
			port: -1
			compression: off
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	// Start with 0ms RTT
	np := createNetProxy(0, 1024*1024*1024, 1024*1024*1024, fmt.Sprintf("nats://127.0.0.1:%d", o1.LeafNode.Port), true)

	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "B"
		ping_interval: "50ms"
		leafnodes {
			remotes [
				{url: %s, compression s2_auto}
			]
		}
	`, np.routeURL())))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()
	defer np.stop()

	checkLeafNodeConnected(t, s2)

	// Even with a bug of updating compression level while it should have been
	// off, the check done below would almost always pass because after
	// reconnecting, there could be a chance to get at first compression set
	// to "off". So we will double check that the leaf node CID did not change
	// at the end of the test.
	getCID := func() uint64 {
		s2.mu.RLock()
		defer s2.mu.RUnlock()
		for _, l := range s2.leafs {
			l.mu.Lock()
			cid := l.cid
			l.mu.Unlock()
			return cid
		}
		return 0
	}
	oldCID := getCID()

	checkCompOff := func() {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			s2.mu.RLock()
			defer s2.mu.RUnlock()
			if len(s2.leafs) != 1 {
				return fmt.Errorf("Leaf not currently connected")
			}
			for _, l := range s2.leafs {
				l.mu.Lock()
				cm := l.leaf.compression
				l.mu.Unlock()
				if cm != CompressionOff {
					return fmt.Errorf("Leaf %v compression mode expected to be %q, got %q", l, CompressionOff, cm)
				}
			}
			return nil
		})
	}
	checkCompOff()

	// Now change RTT and again, make sure that it is still off
	np.updateRTT(20 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	checkCompOff()
	if cid := getCID(); cid != oldCID {
		t.Fatalf("Leafnode has reconnected, cid was %v, now %v", oldCID, cid)
	}
}

func TestLeafNodeCompressionWithWSCompression(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "A"
		websocket {
			port: -1
			no_tls: true
			compression: true
		}
		leafnodes {
			port: -1
			compression: s2_fast
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "B"
		leafnodes {
			remotes [
				{
					url: "ws://127.0.0.1:%d"
					ws_compression: true
					compression: s2_fast
				}
			]
		}
	`, o1.Websocket.Port)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnected(t, s2)

	nc1 := natsConnect(t, s1.ClientURL())
	defer nc1.Close()

	sub := natsSubSync(t, nc1, "foo")
	checkSubInterest(t, s2, globalAccountName, "foo", time.Second)

	nc2 := natsConnect(t, s2.ClientURL())
	defer nc2.Close()

	payload := make([]byte, 1024)
	for i := 0; i < len(payload); i++ {
		payload[i] = 'A'
	}
	natsPub(t, nc2, "foo", payload)
	msg := natsNexMsg(t, sub, time.Second)
	require_True(t, len(msg.Data) == 1024)
	for i := 0; i < len(msg.Data); i++ {
		if msg.Data[i] != 'A' {
			t.Fatalf("Invalid msg: %s", msg.Data)
		}
	}
}

func TestLeafNodeCompressionWithWSGetNeedsData(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "A"
		websocket {
			port: -1
			no_tls: true
		}
		leafnodes {
			port: -1
			compression: s2_fast
		}
	`))
	srv1, o1 := RunServerWithConfig(conf1)
	defer srv1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "B"
		leafnodes {
			remotes [
				{
					url: "ws://127.0.0.1:%d"
					ws_no_masking: true
					compression: s2_fast
				}
			]
		}
	`, o1.Websocket.Port)))
	srv2, _ := RunServerWithConfig(conf2)
	defer srv2.Shutdown()

	checkLeafNodeConnected(t, srv2)

	nc1 := natsConnect(t, srv1.ClientURL())
	defer nc1.Close()

	sub := natsSubSync(t, nc1, "foo")
	checkSubInterest(t, srv2, globalAccountName, "foo", time.Second)

	// We want to have the payload more than 126 bytes so that the websocket
	// code need to read 2 bytes for the length. See below.
	payload := "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ"
	sentBytes := []byte("LMSG foo 156\r\n" + payload + "\r\n")
	h, _ := wsCreateFrameHeader(false, false, wsBinaryMessage, len(sentBytes))
	combined := &bytes.Buffer{}
	combined.Write(h)
	combined.Write(sentBytes)
	toSend := combined.Bytes()

	// We will make a compressed block that cuts the websocket header that
	// makes the reader want to read bytes directly from the connection.
	// We want to make sure that we are not going to get compressed data
	// without going through the (de)compress library. So for that, compress
	// the first 3 bytes.
	b := &bytes.Buffer{}
	w := s2.NewWriter(b)
	w.Write(toSend[:3])
	w.Close()

	var nc net.Conn
	srv2.mu.RLock()
	for _, l := range srv2.leafs {
		l.mu.Lock()
		nc = l.nc
		l.mu.Unlock()
	}
	srv2.mu.RUnlock()

	nc.Write(b.Bytes())

	// Pause to make sure other side just gets a partial of the whole WS frame.
	time.Sleep(100 * time.Millisecond)

	b.Reset()
	w.Reset(b)
	w.Write(toSend[3:])
	w.Close()

	nc.Write(b.Bytes())

	msg := natsNexMsg(t, sub, time.Second)
	require_True(t, len(msg.Data) == 156)
	require_Equal(t, string(msg.Data), payload)
}

func TestLeafNodeCompressionAuthTimeout(t *testing.T) {
	hconf := createConfFile(t, []byte(`
		port: -1
		server_name: "hub"
		leafnodes {
			port: -1
			authorization {
				timeout: 0.75
			}
		}
	`))
	sh, oh := RunServerWithConfig(hconf)
	defer sh.Shutdown()

	sconfTmpl := `
		port: -1
		server_name: "%s"
		cluster {
			port: -1
			name: "spoke"
			%s
		}
		leafnodes {
			port: -1
			remotes [
				{ url: "nats://127.0.0.1:%d" }
			]
		}
	`
	s1conf := createConfFile(t, []byte(fmt.Sprintf(sconfTmpl, "SP1", _EMPTY_, oh.LeafNode.Port)))
	s1, o1 := RunServerWithConfig(s1conf)
	defer s1.Shutdown()

	s2conf := createConfFile(t, []byte(fmt.Sprintf(sconfTmpl, "SP2", fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port), oh.LeafNode.Port)))
	s2, _ := RunServerWithConfig(s2conf)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	checkLeafNodeConnected(t, s1)
	checkLeafNodeConnected(t, s2)

	getCID := func(s *Server) uint64 {
		s.mu.RLock()
		defer s.mu.RUnlock()
		var cid uint64
		for _, l := range s.leafs {
			l.mu.Lock()
			cid = l.cid
			l.mu.Unlock()
		}
		return cid
	}
	leaf1 := getCID(s1)
	leaf2 := getCID(s2)

	// Wait for more than auth timeout
	time.Sleep(time.Second)

	checkLeafNodeConnected(t, s1)
	checkLeafNodeConnected(t, s2)
	if l1 := getCID(s1); l1 != leaf1 {
		t.Fatalf("Leaf connection first connection had CID %v, now %v", leaf1, l1)
	}
	if l2 := getCID(s2); l2 != leaf2 {
		t.Fatalf("Leaf connection first connection had CID %v, now %v", leaf2, l2)
	}
}

func TestLeafNodeWithWeightedDQRequestsToSuperClusterWithSeparateAccounts(t *testing.T) {
	sc := createJetStreamSuperClusterWithTemplate(t, jsClusterAccountsTempl, 3, 2)
	defer sc.shutdown()

	// Now create a leafnode cluster that has 2 LNs, one to each cluster but on separate accounts, ONE and TWO.
	var lnTmpl = `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		{{leaf}}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }}
		`

	var leafFrag = `
		leaf {
			listen: 127.0.0.1:-1
			remotes [
				{ urls: [ %s ] }
				{ urls: [ %s ] }
			]
		}`

	// We want to have two leaf node connections that join to the same local account on the leafnode servers,
	// but connect to different accounts in different clusters.
	c1 := sc.clusters[0] // Will connect to account ONE
	c2 := sc.clusters[1] // Will connect to account TWO

	genLeafTmpl := func(tmpl string) string {
		t.Helper()

		var ln1, ln2 []string
		for _, s := range c1.servers {
			if s.ClusterName() != c1.name {
				continue
			}
			ln := s.getOpts().LeafNode
			ln1 = append(ln1, fmt.Sprintf("nats://one:p@%s:%d", ln.Host, ln.Port))
		}

		for _, s := range c2.servers {
			if s.ClusterName() != c2.name {
				continue
			}
			ln := s.getOpts().LeafNode
			ln2 = append(ln2, fmt.Sprintf("nats://two:p@%s:%d", ln.Host, ln.Port))
		}
		return strings.Replace(tmpl, "{{leaf}}", fmt.Sprintf(leafFrag, strings.Join(ln1, ", "), strings.Join(ln2, ", ")), 1)
	}

	tmpl := strings.Replace(lnTmpl, "store_dir:", fmt.Sprintf(`domain: "%s", store_dir:`, "SA"), 1)
	tmpl = genLeafTmpl(tmpl)

	ln := createJetStreamCluster(t, tmpl, "SA", "SA-", 3, 22280, false)
	ln.waitOnClusterReady()
	defer ln.shutdown()

	for _, s := range ln.servers {
		checkLeafNodeConnectedCount(t, s, 2)
	}

	// Now connect DQ subscribers to each cluster and they separate accounts, and make sure we get the right behavior, balanced between
	// them when requests originate from the leaf cluster.

	// Create 5 clients for each cluster / account
	var c1c, c2c []*nats.Conn
	for i := 0; i < 5; i++ {
		nc1, _ := jsClientConnect(t, c1.randomServer(), nats.UserInfo("one", "p"))
		defer nc1.Close()
		c1c = append(c1c, nc1)
		nc2, _ := jsClientConnect(t, c2.randomServer(), nats.UserInfo("two", "p"))
		defer nc2.Close()
		c2c = append(c2c, nc2)
	}

	createSubs := func(num int, conns []*nats.Conn) (subs []*nats.Subscription) {
		for i := 0; i < num; i++ {
			nc := conns[rand.Intn(len(conns))]
			sub, err := nc.QueueSubscribeSync("REQUEST", "MC")
			require_NoError(t, err)
			subs = append(subs, sub)
			nc.Flush()
		}
		// Let subs propagate.
		time.Sleep(100 * time.Millisecond)
		return subs
	}
	closeSubs := func(subs []*nats.Subscription) {
		for _, sub := range subs {
			sub.Unsubscribe()
		}
	}

	// Simple test first.
	subs1 := createSubs(1, c1c)
	defer closeSubs(subs1)
	subs2 := createSubs(1, c2c)
	defer closeSubs(subs2)

	sendRequests := func(num int) {
		t.Helper()
		// Now connect to the leaf cluster and send some requests.
		nc, _ := jsClientConnect(t, ln.randomServer())
		defer nc.Close()

		for i := 0; i < num; i++ {
			require_NoError(t, nc.Publish("REQUEST", []byte("HELP")))
		}
		nc.Flush()
	}

	pending := func(subs []*nats.Subscription) (total int) {
		t.Helper()
		for _, sub := range subs {
			n, _, err := sub.Pending()
			require_NoError(t, err)
			total += n
		}
		return total
	}

	num := 1000
	checkAllReceived := func() error {
		total := pending(subs1) + pending(subs2)
		if total == num {
			return nil
		}
		return fmt.Errorf("Not all received: %d vs %d", total, num)
	}

	checkBalanced := func(total, pc1, pc2 int) {
		t.Helper()
		tf := float64(total)
		e1 := tf * (float64(pc1) / 100.00)
		e2 := tf * (float64(pc2) / 100.00)
		delta := tf / 10
		p1 := float64(pending(subs1))
		if p1 < e1-delta || p1 > e1+delta {
			t.Fatalf("Value out of range for subs1, expected %v got %v", e1, p1)
		}
		p2 := float64(pending(subs2))
		if p2 < e2-delta || p2 > e2+delta {
			t.Fatalf("Value out of range for subs2, expected %v got %v", e2, p2)
		}
	}

	// Now connect to the leaf cluster and send some requests.

	// Simple 50/50
	sendRequests(num)
	checkFor(t, time.Second, 200*time.Millisecond, checkAllReceived)
	checkBalanced(num, 50, 50)

	closeSubs(subs1)
	closeSubs(subs2)

	// Now test unbalanced. 10/90
	subs1 = createSubs(1, c1c)
	defer closeSubs(subs1)
	subs2 = createSubs(9, c2c)
	defer closeSubs(subs2)

	sendRequests(num)
	checkFor(t, time.Second, 200*time.Millisecond, checkAllReceived)
	checkBalanced(num, 10, 90)

	// Now test draining the subs as we are sending from an initial balanced situation simulating a draining of a cluster.

	closeSubs(subs1)
	closeSubs(subs2)
	subs1, subs2 = nil, nil

	// These subs slightly different.
	var r1, r2 atomic.Uint64
	for i := 0; i < 20; i++ {
		nc := c1c[rand.Intn(len(c1c))]
		sub, err := nc.QueueSubscribe("REQUEST", "MC", func(m *nats.Msg) { r1.Add(1) })
		require_NoError(t, err)
		subs1 = append(subs1, sub)
		nc.Flush()

		nc = c2c[rand.Intn(len(c2c))]
		sub, err = nc.QueueSubscribe("REQUEST", "MC", func(m *nats.Msg) { r2.Add(1) })
		require_NoError(t, err)
		subs2 = append(subs2, sub)
		nc.Flush()
	}
	defer closeSubs(subs1)
	defer closeSubs(subs2)

	nc, _ := jsClientConnect(t, ln.randomServer())
	defer nc.Close()

	for i, dindex := 0, 1; i < num; i++ {
		require_NoError(t, nc.Publish("REQUEST", []byte("HELP")))
		// Check if we have more to simulate draining.
		// Will drain within first ~100 requests using 20% rand test below.
		// Will leave 1 behind.
		if dindex < len(subs1)-1 && rand.Intn(6) > 4 {
			sub := subs1[dindex]
			dindex++
			sub.Drain()
		}
	}
	nc.Flush()

	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		total := int(r1.Load() + r2.Load())
		if total == num {
			return nil
		}
		return fmt.Errorf("Not all received: %d vs %d", total, num)
	})
	require_True(t, r2.Load() > r1.Load())
}

func TestLeafNodeWithWeightedDQRequestsToSuperClusterWithStreamImportAccounts(t *testing.T) {
	var tmpl = `
	listen: 127.0.0.1:-1

	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	leaf { listen: 127.0.0.1:-1 }

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {
		EFG {
			users = [ { user: "efg", pass: "p" } ]
			jetstream: enabled
			imports [
				{ stream: { account: STL, subject: "REQUEST"} }
				{ stream: { account: KSC, subject: "REQUEST"} }
			]
			exports [ { stream: "RESPONSE" } ]
		}
		STL {
			users = [ { user: "stl", pass: "p" } ]
			exports [ { stream: "REQUEST" } ]
			imports [ { stream: { account: EFG, subject: "RESPONSE"} } ]
		}
		KSC {
			users = [ { user: "ksc", pass: "p" } ]
			exports [ { stream: "REQUEST" } ]
			imports [ { stream: { account: EFG, subject: "RESPONSE"} } ]
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}`

	sc := createJetStreamSuperClusterWithTemplate(t, tmpl, 5, 2)
	defer sc.shutdown()

	// Now create a leafnode cluster that has 2 LNs, one to each cluster but on separate accounts, STL and KSC.
	var lnTmpl = `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		{{leaf}}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }}
		`

	var leafFrag = `
		leaf {
			listen: 127.0.0.1:-1
			remotes [
				{ urls: [ %s ] }
				{ urls: [ %s ] }
				{ urls: [ %s ] ; deny_export: [REQUEST, RESPONSE], deny_import: RESPONSE }
			]
		}`

	// We want to have two leaf node connections that join to the same local account on the leafnode servers,
	// but connect to different accounts in different clusters.
	c1 := sc.clusters[0] // Will connect to account KSC
	c2 := sc.clusters[1] // Will connect to account STL

	genLeafTmpl := func(tmpl string) string {
		t.Helper()

		var ln1, ln2, ln3 []string
		for _, s := range c1.servers {
			if s.ClusterName() != c1.name {
				continue
			}
			ln := s.getOpts().LeafNode
			ln1 = append(ln1, fmt.Sprintf("nats://ksc:p@%s:%d", ln.Host, ln.Port))
		}

		for _, s := range c2.servers {
			if s.ClusterName() != c2.name {
				continue
			}
			ln := s.getOpts().LeafNode
			ln2 = append(ln2, fmt.Sprintf("nats://stl:p@%s:%d", ln.Host, ln.Port))
			ln3 = append(ln3, fmt.Sprintf("nats://efg:p@%s:%d", ln.Host, ln.Port))
		}
		return strings.Replace(tmpl, "{{leaf}}", fmt.Sprintf(leafFrag, strings.Join(ln1, ", "), strings.Join(ln2, ", "), strings.Join(ln3, ", ")), 1)
	}

	tmpl = strings.Replace(lnTmpl, "store_dir:", fmt.Sprintf(`domain: "%s", store_dir:`, "SA"), 1)
	tmpl = genLeafTmpl(tmpl)

	ln := createJetStreamCluster(t, tmpl, "SA", "SA-", 3, 22280, false)
	ln.waitOnClusterReady()
	defer ln.shutdown()

	for _, s := range ln.servers {
		checkLeafNodeConnectedCount(t, s, 3)
	}

	// Now connect DQ subscribers to each cluster but to the global account.

	// Create 5 clients for each cluster / account
	var c1c, c2c []*nats.Conn
	for i := 0; i < 5; i++ {
		nc1, _ := jsClientConnect(t, c1.randomServer(), nats.UserInfo("efg", "p"))
		defer nc1.Close()
		c1c = append(c1c, nc1)
		nc2, _ := jsClientConnect(t, c2.randomServer(), nats.UserInfo("efg", "p"))
		defer nc2.Close()
		c2c = append(c2c, nc2)
	}

	createSubs := func(num int, conns []*nats.Conn) (subs []*nats.Subscription) {
		for i := 0; i < num; i++ {
			nc := conns[rand.Intn(len(conns))]
			sub, err := nc.QueueSubscribeSync("REQUEST", "MC")
			require_NoError(t, err)
			subs = append(subs, sub)
			nc.Flush()
		}
		// Let subs propagate.
		time.Sleep(100 * time.Millisecond)
		return subs
	}
	closeSubs := func(subs []*nats.Subscription) {
		for _, sub := range subs {
			sub.Unsubscribe()
		}
	}

	// Simple test first.
	subs1 := createSubs(1, c1c)
	defer closeSubs(subs1)
	subs2 := createSubs(1, c2c)
	defer closeSubs(subs2)

	sendRequests := func(num int) {
		t.Helper()
		// Now connect to the leaf cluster and send some requests.
		nc, _ := jsClientConnect(t, ln.randomServer())
		defer nc.Close()

		for i := 0; i < num; i++ {
			require_NoError(t, nc.Publish("REQUEST", []byte("HELP")))
		}
		nc.Flush()
	}

	pending := func(subs []*nats.Subscription) (total int) {
		t.Helper()
		for _, sub := range subs {
			n, _, err := sub.Pending()
			require_NoError(t, err)
			total += n
		}
		return total
	}

	num := 1000
	checkAllReceived := func() error {
		total := pending(subs1) + pending(subs2)
		if total == num {
			return nil
		}
		return fmt.Errorf("Not all received: %d vs %d", total, num)
	}

	checkBalanced := func(total, pc1, pc2 int) {
		t.Helper()
		tf := float64(total)
		e1 := tf * (float64(pc1) / 100.00)
		e2 := tf * (float64(pc2) / 100.00)
		delta := tf / 10
		p1 := float64(pending(subs1))
		if p1 < e1-delta || p1 > e1+delta {
			t.Fatalf("Value out of range for subs1, expected %v got %v", e1, p1)
		}
		p2 := float64(pending(subs2))
		if p2 < e2-delta || p2 > e2+delta {
			t.Fatalf("Value out of range for subs2, expected %v got %v", e2, p2)
		}
	}

	// Now connect to the leaf cluster and send some requests.

	// Simple 50/50
	sendRequests(num)
	checkFor(t, time.Second, 200*time.Millisecond, checkAllReceived)
	checkBalanced(num, 50, 50)

	closeSubs(subs1)
	closeSubs(subs2)

	// Now test unbalanced. 10/90
	subs1 = createSubs(1, c1c)
	defer closeSubs(subs1)
	subs2 = createSubs(9, c2c)
	defer closeSubs(subs2)

	sendRequests(num)
	checkFor(t, time.Second, 200*time.Millisecond, checkAllReceived)
	checkBalanced(num, 10, 90)

	closeSubs(subs1)
	closeSubs(subs2)

	// Now test unbalanced. 80/20
	subs1 = createSubs(80, c1c)
	defer closeSubs(subs1)
	subs2 = createSubs(20, c2c)
	defer closeSubs(subs2)

	sendRequests(num)
	checkFor(t, time.Second, 200*time.Millisecond, checkAllReceived)
	checkBalanced(num, 80, 20)

	// Now test draining the subs as we are sending from an initial balanced situation simulating a draining of a cluster.

	closeSubs(subs1)
	closeSubs(subs2)
	subs1, subs2 = nil, nil

	// These subs slightly different.
	var r1, r2 atomic.Uint64
	for i := 0; i < 20; i++ {
		nc := c1c[rand.Intn(len(c1c))]
		sub, err := nc.QueueSubscribe("REQUEST", "MC", func(m *nats.Msg) { r1.Add(1) })
		require_NoError(t, err)
		subs1 = append(subs1, sub)
		nc.Flush()

		nc = c2c[rand.Intn(len(c2c))]
		sub, err = nc.QueueSubscribe("REQUEST", "MC", func(m *nats.Msg) { r2.Add(1) })
		require_NoError(t, err)
		subs2 = append(subs2, sub)
		nc.Flush()
	}
	defer closeSubs(subs1)
	defer closeSubs(subs2)

	nc, _ := jsClientConnect(t, ln.randomServer())
	defer nc.Close()

	for i, dindex := 0, 1; i < num; i++ {
		require_NoError(t, nc.Publish("REQUEST", []byte("HELP")))
		// Check if we have more to simulate draining.
		// Will drain within first ~100 requests using 20% rand test below.
		// Will leave 1 behind.
		if dindex < len(subs1)-1 && rand.Intn(6) > 4 {
			sub := subs1[dindex]
			dindex++
			sub.Drain()
		}
	}
	nc.Flush()

	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		total := int(r1.Load() + r2.Load())
		if total == num {
			return nil
		}
		return fmt.Errorf("Not all received: %d vs %d", total, num)
	})
	require_True(t, r2.Load() > r1.Load())

	// Now check opposite flow for responses.

	// Create 10 subscribers.
	var rsubs []*nats.Subscription

	for i := 0; i < 10; i++ {
		nc, _ := jsClientConnect(t, ln.randomServer())
		defer nc.Close()
		sub, err := nc.QueueSubscribeSync("RESPONSE", "SA")
		require_NoError(t, err)
		nc.Flush()
		rsubs = append(rsubs, sub)
	}

	nc, _ = jsClientConnect(t, ln.randomServer())
	defer nc.Close()
	_, err := nc.SubscribeSync("RESPONSE")
	require_NoError(t, err)
	nc.Flush()

	// Now connect and send responses from EFG in cloud.
	nc, _ = jsClientConnect(t, sc.randomServer(), nats.UserInfo("efg", "p"))

	for i := 0; i < 100; i++ {
		require_NoError(t, nc.Publish("RESPONSE", []byte("OK")))
	}
	nc.Flush()

	checkAllRespReceived := func() error {
		p := pending(rsubs)
		if p == 100 {
			return nil
		}
		return fmt.Errorf("Not all responses received: %d vs %d", p, 100)
	}

	checkFor(t, time.Second, 200*time.Millisecond, checkAllRespReceived)
}

func TestLeafNodeWithWeightedDQResponsesWithStreamImportAccountsWithUnsub(t *testing.T) {
	var tmpl = `
	listen: 127.0.0.1:-1

	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	leaf { listen: 127.0.0.1:-1 }

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {
		EFG {
			users = [ { user: "efg", pass: "p" } ]
			jetstream: enabled
			exports [ { stream: "RESPONSE" } ]
		}
		STL {
			users = [ { user: "stl", pass: "p" } ]
			imports [ { stream: { account: EFG, subject: "RESPONSE"} } ]
		}
		KSC {
			users = [ { user: "ksc", pass: "p" } ]
			imports [ { stream: { account: EFG, subject: "RESPONSE"} } ]
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}`

	c := createJetStreamClusterWithTemplate(t, tmpl, "US-CENTRAL", 3)
	defer c.shutdown()

	// Now create a leafnode cluster that has 2 LNs, one to each cluster but on separate accounts, STL and KSC.
	var lnTmpl = `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		{{leaf}}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }}
		`

	var leafFrag = `
		leaf {
			listen: 127.0.0.1:-1
			remotes [ { urls: [ %s ] } ]
		}`

	genLeafTmpl := func(tmpl string) string {
		t.Helper()

		var ln []string
		for _, s := range c.servers {
			lno := s.getOpts().LeafNode
			ln = append(ln, fmt.Sprintf("nats://ksc:p@%s:%d", lno.Host, lno.Port))
		}
		return strings.Replace(tmpl, "{{leaf}}", fmt.Sprintf(leafFrag, strings.Join(ln, ", ")), 1)
	}

	tmpl = strings.Replace(lnTmpl, "store_dir:", fmt.Sprintf(`domain: "%s", store_dir:`, "SA"), 1)
	tmpl = genLeafTmpl(tmpl)

	ln := createJetStreamCluster(t, tmpl, "SA", "SA-", 3, 22280, false)
	ln.waitOnClusterReady()
	defer ln.shutdown()

	for _, s := range ln.servers {
		checkLeafNodeConnectedCount(t, s, 1)
	}

	// Create 10 subscribers.
	var rsubs []*nats.Subscription

	closeSubs := func(subs []*nats.Subscription) {
		for _, sub := range subs {
			sub.Unsubscribe()
		}
	}

	checkAllRespReceived := func() error {
		t.Helper()
		var total int
		for _, sub := range rsubs {
			n, _, err := sub.Pending()
			require_NoError(t, err)
			total += n
		}
		if total == 100 {
			return nil
		}
		return fmt.Errorf("Not all responses received: %d vs %d", total, 100)
	}

	s := ln.randomServer()
	for i := 0; i < 4; i++ {
		nc, _ := jsClientConnect(t, s)
		defer nc.Close()
		sub, err := nc.QueueSubscribeSync("RESPONSE", "SA")
		require_NoError(t, err)
		nc.Flush()
		rsubs = append(rsubs, sub)
	}

	// Now connect and send responses from EFG in cloud.
	nc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("efg", "p"))
	for i := 0; i < 100; i++ {
		require_NoError(t, nc.Publish("RESPONSE", []byte("OK")))
	}
	nc.Flush()

	// Make sure all received.
	checkFor(t, time.Second, 200*time.Millisecond, checkAllRespReceived)

	checkAccountInterest := func(s *Server, accName string) *SublistResult {
		t.Helper()
		acc, err := s.LookupAccount(accName)
		require_NoError(t, err)
		acc.mu.RLock()
		r := acc.sl.Match("RESPONSE")
		acc.mu.RUnlock()
		return r
	}

	checkInterest := func() error {
		t.Helper()
		for _, s := range c.servers {
			if r := checkAccountInterest(s, "KSC"); len(r.psubs)+len(r.qsubs) > 0 {
				return fmt.Errorf("Subs still present for %q: %+v", "KSC", r)
			}
			if r := checkAccountInterest(s, "EFG"); len(r.psubs)+len(r.qsubs) > 0 {
				return fmt.Errorf("Subs still present for %q: %+v", "EFG", r)
			}
		}
		return nil
	}

	// Now unsub them and create new ones on a different server.
	closeSubs(rsubs)
	rsubs = rsubs[:0]

	// Also restart the server that we had all the rsubs on.
	s.Shutdown()
	s.WaitForShutdown()
	s = ln.restartServer(s)
	ln.waitOnClusterReady()
	ln.waitOnServerCurrent(s)

	checkFor(t, time.Second, 200*time.Millisecond, checkInterest)

	for i := 0; i < 4; i++ {
		nc, _ := jsClientConnect(t, s)
		defer nc.Close()
		sub, err := nc.QueueSubscribeSync("RESPONSE", "SA")
		require_NoError(t, err)
		nc.Flush()
		rsubs = append(rsubs, sub)
	}

	for i := 0; i < 100; i++ {
		require_NoError(t, nc.Publish("RESPONSE", []byte("OK")))
	}
	nc.Flush()

	// Make sure all received.
	checkFor(t, time.Second, 200*time.Millisecond, checkAllRespReceived)

	closeSubs(rsubs)
	checkFor(t, time.Second, 200*time.Millisecond, checkInterest)
}

func TestLeafNodeTwoRemotesToSameHubAccount(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "hub"
		accounts {
			HA { users: [{user: ha, password: pwd}] }
		}
		leafnodes {
			port: -1
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "spoke"
		accounts {
			A { users: [{user: A, password: pwd}] }
			B { users: [{user: B, password: pwd}] }
			C { users: [{user: C, password: pwd}] }
		}
		leafnodes {
			remotes [
				{
					url: "nats://ha:pwd@127.0.0.1:%d"
					local: "A"
				}
				{
					url: "nats://ha:pwd@127.0.0.1:%d"
					local: "C"
				}
			]
		}
	`, o1.LeafNode.Port, o1.LeafNode.Port)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s2.SetLogger(l, false, false)

	checkLeafNodeConnectedCount(t, s2, 2)

	// Make sure we don't get duplicate leafnode connection errors
	deadline := time.NewTimer(1500 * time.Millisecond)
	for done := false; !done; {
		select {
		case err := <-l.errCh:
			if strings.Contains(err, DuplicateRemoteLeafnodeConnection.String()) {
				t.Fatalf("Got error: %v", err)
			}
		case <-deadline.C:
			done = true
		}
	}

	nca := natsConnect(t, s2.ClientURL(), nats.UserInfo("A", "pwd"))
	defer nca.Close()
	suba := natsSubSync(t, nca, "A")
	ncb := natsConnect(t, s2.ClientURL(), nats.UserInfo("B", "pwd"))
	defer ncb.Close()
	subb := natsSubSync(t, ncb, "B")
	ncc := natsConnect(t, s2.ClientURL(), nats.UserInfo("C", "pwd"))
	defer ncc.Close()
	subc := natsSubSync(t, ncc, "C")
	subs := map[string]*nats.Subscription{"A": suba, "B": subb, "C": subc}

	for _, subj := range []string{"A", "C"} {
		checkSubInterest(t, s1, "HA", subj, time.Second)
	}

	nc := natsConnect(t, s1.ClientURL(), nats.UserInfo("ha", "pwd"))
	defer nc.Close()

	for _, subj := range []string{"A", "B", "C"} {
		natsPub(t, nc, subj, []byte("hello"))
	}

	for _, subj := range []string{"A", "B", "C"} {
		var expected bool
		if subj != "B" {
			expected = true
		}
		sub := subs[subj]
		if expected {
			natsNexMsg(t, sub, time.Second)
		} else {
			if _, err := sub.NextMsg(50 * time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Expected timeout error, got %v", err)
			}
		}
	}
}

func TestLeafNodeTwoRemotesToSameHubAccountWithClusters(t *testing.T) {
	hubTmpl := `
		port: -1
		server_name: "%s"
		accounts {
			HA { users: [{user: HA, password: pwd}] }
		}
		cluster {
			name: "hub"
			port: -1
			%s
		}
		leafnodes {
			port: -1
		}
	`
	confH1 := createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "H1", _EMPTY_)))
	sh1, oh1 := RunServerWithConfig(confH1)
	defer sh1.Shutdown()

	confH2 := createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "H2", fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", oh1.Cluster.Port))))
	sh2, oh2 := RunServerWithConfig(confH2)
	defer sh2.Shutdown()

	checkClusterFormed(t, sh1, sh2)

	spokeTmpl := `
		port: -1
		server_name: "%s"
		accounts {
			A { users: [{user: A, password: pwd}] }
			B { users: [{user: B, password: pwd}] }
		}
		cluster {
			name: "spoke"
			port: -1
			%s
		}
		leafnodes {
			remotes [
				{
					url: "nats://HA:pwd@127.0.0.1:%d"
					local: "A"
				}
				{
					url: "nats://HA:pwd@127.0.0.1:%d"
					local: "B"
				}
			]
		}
	`
	for _, test := range []struct {
		name        string
		sp2Leafport int
	}{
		{"connect to different hub servers", oh2.LeafNode.Port},
		{"connect to same hub server", oh1.LeafNode.Port},
	} {
		t.Run(test.name, func(t *testing.T) {
			confSP1 := createConfFile(t, []byte(fmt.Sprintf(spokeTmpl, "SP1", _EMPTY_, oh1.LeafNode.Port, oh1.LeafNode.Port)))
			sp1, osp1 := RunServerWithConfig(confSP1)
			defer sp1.Shutdown()

			confSP2 := createConfFile(t, []byte(fmt.Sprintf(spokeTmpl, "SP2",
				fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", osp1.Cluster.Port), test.sp2Leafport, test.sp2Leafport)))
			sp2, _ := RunServerWithConfig(confSP2)
			defer sp2.Shutdown()

			checkClusterFormed(t, sp1, sp2)
			checkLeafNodeConnectedCount(t, sp1, 2)
			checkLeafNodeConnectedCount(t, sp2, 2)

			var conns []*nats.Conn
			createConn := func(s *Server, user string) {
				t.Helper()
				nc := natsConnect(t, s.ClientURL(), nats.UserInfo(user, "pwd"))
				conns = append(conns, nc)
			}
			for _, nc := range conns {
				defer nc.Close()
			}
			createConn(sh1, "HA")
			createConn(sh2, "HA")
			createConn(sp1, "A")
			createConn(sp2, "A")
			createConn(sp1, "B")
			createConn(sp2, "B")

			check := func(subConn *nats.Conn, subj string, checkA, checkB bool) {
				t.Helper()
				sub := natsSubSync(t, subConn, subj)
				defer sub.Unsubscribe()

				checkSubInterest(t, sh1, "HA", subj, time.Second)
				checkSubInterest(t, sh2, "HA", subj, time.Second)
				if checkA {
					checkSubInterest(t, sp1, "A", subj, time.Second)
					checkSubInterest(t, sp2, "A", subj, time.Second)
				}
				if checkB {
					checkSubInterest(t, sp1, "B", subj, time.Second)
					checkSubInterest(t, sp2, "B", subj, time.Second)
				}

				for i, ncp := range conns {
					// Don't publish from account "A" connections if we are
					// dealing with account "B", and vice-versa.
					if !checkA && i >= 2 && i <= 3 {
						continue
					}
					if !checkB && i >= 4 {
						continue
					}
					natsPub(t, ncp, subj, []byte("hello"))
					natsNexMsg(t, sub, time.Second)
					// Make sure we don't get a duplicate
					if msg, err := sub.NextMsg(50 * time.Millisecond); err != nats.ErrTimeout {
						t.Fatalf("Unexpected message or error: msg=%v - err=%v", msg, err)
					}
				}
			}
			check(conns[0], "HA.1", true, true)
			check(conns[1], "HA.2", true, true)
			check(conns[2], "SPA.1", true, false)
			check(conns[3], "SPA.2", true, false)
			check(conns[4], "SPB.1", false, true)
			check(conns[5], "SPB.2", false, true)
		})
	}
}

func TestLeafNodeSameLocalAccountToMultipleHubs(t *testing.T) {
	hub1Conf := createConfFile(t, []byte(`
		port: -1
		server_name: hub1
		accounts {
			hub1 { users: [{user: hub1, password: pwd}] }
		}
		leafnodes {
			port: -1
		}
	`))
	sh1, oh1 := RunServerWithConfig(hub1Conf)
	defer sh1.Shutdown()

	hub2Conf := createConfFile(t, []byte(`
		port: -1
		server_name: hub2
		accounts {
			hub2 { users: [{user: hub2, password: pwd}] }
		}
		leafnodes {
			port: -1
		}
	`))
	sh2, oh2 := RunServerWithConfig(hub2Conf)
	defer sh2.Shutdown()

	lconf := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: leaf
		accounts {
			A { users: [{user: A, password: pwd}] }
			B { users: [{user: B, password: pwd}] }
			C { users: [{user: C, password: pwd}] }
		}
		leafnodes {
			port: -1
			remotes [
				{
					url: nats://hub1:pwd@127.0.0.1:%[1]d
					local: "A"
				}
				{
					url: nats://hub1:pwd@127.0.0.1:%[1]d
					local: "C"
				}
				{
					url: nats://hub2:pwd@127.0.0.1:%[2]d
					local: "A"
				}
				{
					url: nats://hub2:pwd@127.0.0.1:%[2]d
					local: "B"
				}
			]
		}
	`, oh1.LeafNode.Port, oh2.LeafNode.Port)))
	s, _ := RunServerWithConfig(lconf)
	defer s.Shutdown()

	// The leafnode to hub1 should have 2 connections (A and C)
	// while the one to hub2 should have 2 connections (A and B)
	checkLeafNodeConnectedCount(t, sh1, 2)
	checkLeafNodeConnectedCount(t, sh2, 2)
	checkLeafNodeConnectedCount(t, s, 4)

	nca := natsConnect(t, s.ClientURL(), nats.UserInfo("A", "pwd"))
	defer nca.Close()
	suba := natsSubSync(t, nca, "A")
	ncb := natsConnect(t, s.ClientURL(), nats.UserInfo("B", "pwd"))
	defer ncb.Close()
	subb := natsSubSync(t, ncb, "B")
	ncc := natsConnect(t, s.ClientURL(), nats.UserInfo("C", "pwd"))
	defer ncc.Close()
	subc := natsSubSync(t, ncc, "C")

	checkSubInterest(t, sh1, "hub1", "A", time.Second)
	checkSubNoInterest(t, sh1, "hub1", "B", time.Second)
	checkSubInterest(t, sh1, "hub1", "C", time.Second)

	checkSubInterest(t, sh2, "hub2", "A", time.Second)
	checkSubInterest(t, sh2, "hub2", "B", time.Second)
	checkSubNoInterest(t, sh2, "hub2", "C", time.Second)

	nch1 := natsConnect(t, sh1.ClientURL(), nats.UserInfo("hub1", "pwd"))
	defer nch1.Close()
	nch2 := natsConnect(t, sh2.ClientURL(), nats.UserInfo("hub2", "pwd"))
	defer nch2.Close()

	checkNoMsg := func(sub *nats.Subscription) {
		t.Helper()
		if msg, err := sub.NextMsg(50 * time.Millisecond); err != nats.ErrTimeout {
			t.Fatalf("Unexpected message: %s", msg.Data)
		}
	}

	checkSub := func(sub *nats.Subscription, subj, payload string) {
		t.Helper()
		msg := natsNexMsg(t, sub, time.Second)
		require_Equal(t, subj, msg.Subject)
		require_Equal(t, payload, string(msg.Data))
		// Make sure we don't get duplicates
		checkNoMsg(sub)
	}

	natsPub(t, nch1, "A", []byte("msgA1"))
	checkSub(suba, "A", "msgA1")
	natsPub(t, nch1, "B", []byte("msgB1"))
	checkNoMsg(subb)
	natsPub(t, nch1, "C", []byte("msgC1"))
	checkSub(subc, "C", "msgC1")

	natsPub(t, nch2, "A", []byte("msgA2"))
	checkSub(suba, "A", "msgA2")
	natsPub(t, nch2, "B", []byte("msgB2"))
	checkSub(subb, "B", "msgB2")
	natsPub(t, nch2, "C", []byte("msgC2"))
	checkNoMsg(subc)
}

func TestLeafNodeSlowConsumer(t *testing.T) {
	ao := DefaultOptions()
	ao.LeafNode.Host = "127.0.0.1"
	ao.LeafNode.Port = -1
	ao.WriteDeadline = 1 * time.Millisecond
	a := RunServer(ao)
	defer a.Shutdown()

	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", ao.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	a.mu.Lock()
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		a.grMu.Lock()
		defer a.grMu.Unlock()
		for _, cli := range a.grTmpClients {
			cli.out.wdl = time.Nanosecond
			return nil
		}
		return nil
	})
	a.mu.Unlock()

	// Only leafnode slow consumers that made it past connect are tracked
	// in the slow consumers counter.
	if _, err := c.Write([]byte("CONNECT {}\r\n")); err != nil {
		t.Fatalf("Error writing connect: %v", err)
	}
	// Read info
	br := bufio.NewReader(c)
	br.ReadLine()
	for i := 0; i < 10; i++ {
		if _, err := c.Write([]byte("PING\r\n")); err != nil {
			t.Fatalf("Unexpected error writing PING: %v", err)
		}
	}
	defer c.Close()
	timeout := time.Now().Add(time.Second)
	var (
		got      uint64
		expected uint64 = 1
	)
	for time.Now().Before(timeout) {
		got = a.NumSlowConsumersLeafs()
		if got == expected {
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
	t.Fatalf("Timed out waiting for slow consumer leafnodes, got: %v, expected: %v", got, expected)

}
