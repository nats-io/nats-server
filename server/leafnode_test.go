// Copyright 2019 The NATS Authors
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
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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
	defer os.Remove(conf1)
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
	defer os.Remove(conf2)
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
	defer os.Remove(conf1)
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
	defer os.Remove(conf2)
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
	defer os.Remove(conf3)
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
	defer os.Remove(conf4)
	o4, err := ProcessConfigFile(conf4)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	if len(o4.LeafNode.Remotes) == 0 {
		t.Fatal("Expected at least a single leaf remote")
	}
	got = o4.LeafNode.Remotes[0].TLSTimeout
	expected = float64(DEFAULT_LEAF_TLS_TIMEOUT)
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

	logFileName := createConfFile(t, []byte(""))
	defer os.Remove(logFileName)

	oa := DefaultOptions()
	oa.LeafNode.ReconnectInterval = 15 * time.Millisecond
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
		if !strings.Contains(e, "No local account") {
			t.Fatalf("Expected error about no local account, got %s", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get the error")
	}

	// For now, sa would try to recreate the connection for ever.
	// Check that lid is increasing...
	time.Sleep(100 * time.Millisecond)
	lid := atomic.LoadUint64(&sa.gcid)
	if lid < 4 {
		t.Fatalf("Seems like connection was not retried")
	}
}

// This test ensures that we can connect using proper user/password
// to a LN URL that was discovered through the INFO protocol.
func TestLeafNodeBasicAuthFailover(t *testing.T) {
	content := `
	listen: "127.0.0.1:-1"
	cluster {
		listen: "127.0.0.1:-1"
		%s
	}
	leafnodes {
		listen: "127.0.0.1:-1"
		authorization {
			user: foo
			password: pwd
			timeout: 1
		}
	}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(content, "")))
	defer os.Remove(conf)

	sb1, ob1 := RunServerWithConfig(conf)
	defer sb1.Shutdown()

	conf = createConfFile(t, []byte(fmt.Sprintf(content, fmt.Sprintf("routes: [nats://127.0.0.1:%d]", ob1.Cluster.Port))))
	defer os.Remove(conf)

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
				url: "nats://foo:pwd@127.0.0.1:%d"
			}
		]
	}
	`
	conf = createConfFile(t, []byte(fmt.Sprintf(content, ob1.LeafNode.Port)))
	defer os.Remove(conf)

	sa, _ := RunServerWithConfig(conf)
	defer sa.Shutdown()

	checkLeafNodeConnected(t, sa)

	// Shutdown sb1, sa should reconnect to sb2
	sb1.Shutdown()

	// Wait a bit to make sure there was a disconnect and attempt to reconnect.
	time.Sleep(250 * time.Millisecond)

	// Should be able to reconnect
	checkLeafNodeConnected(t, sa)
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
