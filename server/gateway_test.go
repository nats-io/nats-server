// Copyright 2018 The NATS Authors
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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/logger"
	"github.com/nats-io/go-nats"
)

func init() {
	gatewayConnectDelay = 15 * time.Millisecond
	gatewayReconnectDelay = 15 * time.Millisecond
}

// Wait for the expected number of outbound gateways, or fails.
func waitForOutboundGateways(t *testing.T, s *Server, expected int, timeout time.Duration) {
	t.Helper()
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		if n := s.numOutboundGateways(); n != expected {
			return fmt.Errorf("Expected %v outbound gateway(s), got %v", expected, n)
		}
		return nil
	})
}

// Wait for the expected number of inbound gateways, or fails.
func waitForInboundGateways(t *testing.T, s *Server, expected int, timeout time.Duration) {
	t.Helper()
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		if n := s.numInboundGateways(); n != expected {
			return fmt.Errorf("Expected %v inbound gateway(s), got %v", expected, n)
		}
		return nil
	})
}

func waitForGatewayFailedConnect(t *testing.T, s *Server, gwName string, expectFailure bool, timeout time.Duration) {
	t.Helper()
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		var c int
		cfg := s.getRemoteGateway(gwName)
		if cfg != nil {
			c = cfg.getConnAttempts()
		}
		if expectFailure && c <= 1 {
			return fmt.Errorf("Expected several attempts to connect, got %v", c)
		} else if !expectFailure && c > 1 {
			return fmt.Errorf("Expected single attempt to connect, got %v", c)
		}
		return nil
	})
}

func checkForRegisteredQSubInterest(t *testing.T, s *Server, gwName, acc, subj string, expected int, timeout time.Duration) {
	t.Helper()
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		count := 0
		c := s.getOutboundGatewayConnection(gwName)
		ei, _ := c.gw.outsim.Load(acc)
		if ei != nil {
			sl := ei.(*outsie).sl
			r := sl.Match(subj)
			for _, qsubs := range r.qsubs {
				count += len(qsubs)
			}
		}
		if count == expected {
			return nil
		}
		return fmt.Errorf("Expected %v qsubs in sublist, got %v", expected, count)
	})
}

func checkForNoInterest(t *testing.T, c *client, account, subject string, expectNoInterest bool, timeout time.Duration) {
	t.Helper()
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		ei, _ := c.gw.outsim.Load(account)
		if ei == nil {
			return fmt.Errorf("Did not receive subject no-interest")
		}
		e := ei.(*outsie)
		e.RLock()
		_, inMap := e.ni[subject]
		e.RUnlock()
		if expectNoInterest {
			if inMap {
				return nil
			}
			return fmt.Errorf("Did not receive subject no-interest on %q", subject)
		}
		if inMap {
			return fmt.Errorf("No-interest on subject %q was not cleared", subject)
		}
		return nil
	})
}

func waitCh(t *testing.T, ch chan bool, errTxt string) {
	t.Helper()
	select {
	case <-ch:
		return
	case <-time.After(5 * time.Second):
		t.Fatalf(errTxt)
	}
}

func natsConnect(t *testing.T, url string, options ...nats.Option) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url, options...)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	return nc
}

func natsSub(t *testing.T, nc *nats.Conn, subj string, cb nats.MsgHandler) *nats.Subscription {
	t.Helper()
	sub, err := nc.Subscribe(subj, cb)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	return sub
}

func natsSubSync(t *testing.T, nc *nats.Conn, subj string) *nats.Subscription {
	t.Helper()
	sub, err := nc.SubscribeSync(subj)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	return sub
}

func natsQueueSub(t *testing.T, nc *nats.Conn, subj, queue string, cb nats.MsgHandler) *nats.Subscription {
	t.Helper()
	sub, err := nc.QueueSubscribe(subj, queue, cb)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	return sub
}

func natsQueueSubSync(t *testing.T, nc *nats.Conn, subj, queue string) *nats.Subscription {
	t.Helper()
	sub, err := nc.QueueSubscribeSync(subj, queue)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	return sub
}

func natsFlush(t *testing.T, nc *nats.Conn) {
	t.Helper()
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}
}

func natsPub(t *testing.T, nc *nats.Conn, subj string, payload []byte) {
	t.Helper()
	if err := nc.Publish(subj, payload); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
}

func natsPubReq(t *testing.T, nc *nats.Conn, subj, reply string, payload []byte) {
	t.Helper()
	if err := nc.PublishRequest(subj, reply, payload); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
}

func natsUnsub(t *testing.T, sub *nats.Subscription) {
	t.Helper()
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Error on unsubscribe: %v", err)
	}
}

func testDefaultOptionsForGateway(name string) *Options {
	o := DefaultOptions()
	o.Gateway.Name = name
	o.Gateway.Host = "127.0.0.1"
	o.Gateway.Port = -1
	o.Gateway.DefaultPermissions = &GatewayPermissions{
		Import: &SubjectPermission{Allow: []string{">"}},
		Export: &SubjectPermission{Allow: []string{">"}},
	}
	o.gatewaysSolicitDelay = 15 * time.Millisecond
	return o
}

func runGatewayServer(o *Options) *Server {
	s := RunServer(o)
	s.SetLogger(&DummyLogger{}, true, true)
	return s
}

func testGatewayOptionsFromToWithServers(t *testing.T, org, dst string, servers ...*Server) *Options {
	t.Helper()
	o := testDefaultOptionsForGateway(org)
	gw := &RemoteGatewayOpts{
		Name: dst,
		Permissions: &GatewayPermissions{
			Import: &SubjectPermission{Allow: []string{">"}},
			Export: &SubjectPermission{Allow: []string{">"}},
		},
	}
	for _, s := range servers {
		us := fmt.Sprintf("nats://127.0.0.1:%d", s.GatewayAddr().Port)
		u, err := url.Parse(us)
		if err != nil {
			t.Fatalf("Error parsing url: %v", err)
		}
		gw.URLs = append(gw.URLs, u)
	}
	o.Gateway.Gateways = append(o.Gateway.Gateways, gw)
	return o
}

func testAddGatewayURLs(t *testing.T, o *Options, dst string, urls []string) {
	t.Helper()
	gw := &RemoteGatewayOpts{
		Name: dst,
		Permissions: &GatewayPermissions{
			Import: &SubjectPermission{Allow: []string{">"}},
			Export: &SubjectPermission{Allow: []string{">"}},
		},
	}
	for _, us := range urls {
		u, err := url.Parse(us)
		if err != nil {
			t.Fatalf("Error parsing url: %v", err)
		}
		gw.URLs = append(gw.URLs, u)
	}
	o.Gateway.Gateways = append(o.Gateway.Gateways, gw)
}

func testGatewayOptionsFromToWithURLs(t *testing.T, org, dst string, urls []string) *Options {
	o := testDefaultOptionsForGateway(org)
	testAddGatewayURLs(t, o, dst, urls)
	return o
}

func testGatewayOptionsWithTLS(t *testing.T, name string) *Options {
	t.Helper()
	o := testDefaultOptionsForGateway(name)
	var (
		tc  = &TLSConfigOpts{}
		err error
	)
	if name == "A" {
		tc.CertFile = "../test/configs/certs/srva-cert.pem"
		tc.KeyFile = "../test/configs/certs/srva-key.pem"
	} else {
		tc.CertFile = "../test/configs/certs/srvb-cert.pem"
		tc.KeyFile = "../test/configs/certs/srvb-key.pem"
	}
	tc.CaFile = "../test/configs/certs/ca.pem"
	o.Gateway.TLSConfig, err = GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating TLS config: %v", err)
	}
	o.Gateway.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
	o.Gateway.TLSConfig.RootCAs = o.Gateway.TLSConfig.ClientCAs
	o.Gateway.TLSTimeout = 2.0
	return o
}

func testGatewayOptionsFromToWithTLS(t *testing.T, org, dst string, urls []string) *Options {
	o := testGatewayOptionsWithTLS(t, org)
	testAddGatewayURLs(t, o, dst, urls)
	return o
}

func TestGatewayBasic(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	o2.Gateway.ConnectRetries = 0
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should have an outbound gateway to s2.
	waitForOutboundGateways(t, s1, 1, time.Second)
	// s2 should have an inbound gateway
	waitForInboundGateways(t, s2, 1, time.Second)
	// and an outbound too
	waitForOutboundGateways(t, s2, 1, time.Second)

	// Stop s2 server
	s2.Shutdown()

	// gateway should go away
	waitForOutboundGateways(t, s1, 0, time.Second)
	waitForInboundGateways(t, s1, 0, time.Second)

	// Restart server
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	// gateway should reconnect
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)
	waitForInboundGateways(t, s1, 1, 2*time.Second)
	waitForInboundGateways(t, s2, 1, 2*time.Second)

	// Shutdown s1, remove the gateway from A to B and restart.
	s1.Shutdown()
	// When s2 detects the connection is closed, it will attempt
	// to reconnect once (even if the route is implicit).
	// Wait a bit before restarting s1. For Windows, we need to wait
	// more than the dialTimeout before restarting the server.
	wait := 500 * time.Millisecond
	if runtime.GOOS == "windows" {
		wait = 1200 * time.Millisecond
	}
	time.Sleep(wait)
	// Restart s1 without gateway to B.
	o1.Gateway.Gateways = nil
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should not have any outbound nor inbound
	waitForOutboundGateways(t, s1, 0, 2*time.Second)
	waitForInboundGateways(t, s1, 0, 2*time.Second)

	// Same for s2
	waitForOutboundGateways(t, s2, 0, 2*time.Second)
	waitForInboundGateways(t, s2, 0, 2*time.Second)

	// Verify that s2 no longer has A gateway in its list
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if s2.getRemoteGateway("A") != nil {
			return fmt.Errorf("Gateway A should have been removed from s2")
		}
		return nil
	})
}

func TestGatewayIgnoreSelfReference(t *testing.T) {
	o := testDefaultOptionsForGateway("A")
	// To create a reference to itself before running the server
	// it means that we have to assign an explicit port
	o.Gateway.Port = 5222
	o.gatewaysSolicitDelay = 0
	u, _ := url.Parse(fmt.Sprintf("nats://%s:%d", o.Gateway.Host, o.Gateway.Port))
	cfg := &RemoteGatewayOpts{
		Name: "A",
		URLs: []*url.URL{u},
	}
	o.Gateway.Gateways = append(o.Gateway.Gateways, cfg)
	s := runGatewayServer(o)
	defer s.Shutdown()

	// Wait a bit to make sure that there is no attempt to connect.
	time.Sleep(20 * time.Millisecond)

	// No outbound connection expected, and no attempt to connect.
	if s.getRemoteGateway("A") != nil {
		t.Fatalf("Should not have a remote gateway config for A")
	}
	if s.getOutboundGatewayConnection("A") != nil {
		t.Fatalf("Should not have a gateway connection to A")
	}
	s.Shutdown()

	// Now try with config files and include
	s1, _ := RunServerWithConfig("configs/gwa.conf")
	defer s1.Shutdown()

	s2, _ := RunServerWithConfig("configs/gwb.conf")
	defer s2.Shutdown()

	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)
	waitForInboundGateways(t, s1, 1, 2*time.Second)
	waitForInboundGateways(t, s2, 1, 2*time.Second)

	if s1.getRemoteGateway("A") != nil {
		t.Fatalf("Should not have a remote gateway config for A")
	}
	if s1.getOutboundGatewayConnection("A") != nil {
		t.Fatalf("Should not have a gateway connection to A")
	}
	if s2.getRemoteGateway("B") != nil {
		t.Fatalf("Should not have a remote gateway config for B")
	}
	if s2.getOutboundGatewayConnection("B") != nil {
		t.Fatalf("Should not have a gateway connection to B")
	}
}

func TestGatewaySolicitDelay(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	// Set the solicit delay to 0. This tests that server will use its
	// default value, currently set at 1 sec.
	o1.gatewaysSolicitDelay = 0
	start := time.Now()
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// After 500ms, check outbound gateway. Should not be there.
	time.Sleep(500 * time.Millisecond)
	if time.Since(start) < defaultSolicitGatewaysDelay {
		if s1.numOutboundGateways() > 0 {
			t.Fatalf("The outbound gateway was initiated sooner than expected (%v)", time.Since(start))
		}
	}
	// Ultimately, s1 should have an outbound gateway to s2.
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	// s2 should have an inbound gateway
	waitForInboundGateways(t, s2, 1, 2*time.Second)

	s1.Shutdown()
	// Make sure that server can be shutdown while waiting
	// for that initial solicit delay
	o1.gatewaysSolicitDelay = 2 * time.Second
	s1 = runGatewayServer(o1)
	start = time.Now()
	s1.Shutdown()
	if dur := time.Since(start); dur >= 2*time.Second {
		t.Fatalf("Looks like shutdown was delayed: %v", dur)
	}
}

func TestGatewaySolicitDelayWithImplicitOutbounds(t *testing.T) {
	// Cause a situation where A connects to B, and because of
	// delay of solicit gateways set on B, we want to make sure
	// that B does not end-up with 2 connections to A.
	o2 := testDefaultOptionsForGateway("B")
	o2.gatewaysSolicitDelay = 500 * time.Millisecond
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should have an outbound and inbound gateway to s2.
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	// s2 should have an inbound gateway
	waitForInboundGateways(t, s2, 1, 2*time.Second)
	// Wait for more than s2 solicit delay
	time.Sleep(750 * time.Millisecond)
	// The way we store outbound (map key'ed by gw name), we would
	// not know if we had created 2 (since the newer would replace
	// the older in the map). But if a second connection was made,
	// then s1 would have 2 inbounds. So check it has only 1.
	waitForInboundGateways(t, s1, 1, time.Second)
}

type slowResolver struct{}

func (r *slowResolver) LookupHost(ctx context.Context, h string) ([]string, error) {
	time.Sleep(500 * time.Millisecond)
	return []string{h}, nil
}

func TestGatewaySolicitShutdown(t *testing.T) {
	var urls []string
	for i := 0; i < 5; i++ {
		u := fmt.Sprintf("nats://127.0.0.1:%d", 1234+i)
		urls = append(urls, u)
	}
	o1 := testGatewayOptionsFromToWithURLs(t, "A", "B", urls)
	o1.Gateway.resolver = &slowResolver{}
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	time.Sleep(o1.gatewaysSolicitDelay + 10*time.Millisecond)

	start := time.Now()
	s1.Shutdown()
	if dur := time.Since(start); dur > 1200*time.Millisecond {
		t.Fatalf("Took too long to shutdown: %v", dur)
	}
}

func TestGatewayListenError(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testDefaultOptionsForGateway("A")
	o1.Gateway.Port = s2.GatewayAddr().Port
	s1 := New(o1)
	defer s1.Shutdown()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s1.Start()
		wg.Done()
	}()
	// We call Fatalf on listen error, but since there is no actual logger
	// associated, we just check that the listener is not created.
	time.Sleep(100 * time.Millisecond)
	addr := s1.GatewayAddr()
	if addr != nil {
		t.Fatal("Listener should not have been created")
	}
	s1.Shutdown()
	wg.Wait()
}

func TestGatewayWithListenToAny(t *testing.T) {
	confB1 := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		cluster {
			listen: "127.0.0.1:-1"
		}
		gateway {
			name: "B"
			listen: "0.0.0.0:-1"
		}
	`))
	sb1, ob1 := RunServerWithConfig(confB1)
	defer sb1.Shutdown()

	confB2 := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		cluster {
			listen: "127.0.0.1:-1"
			routes: ["%s"]
		}
		gateway {
			name: "B"
			listen: "0.0.0.0:-1"
		}
	`, fmt.Sprintf("nats://127.0.0.1:%d", sb1.ClusterAddr().Port))))
	sb2, ob2 := RunServerWithConfig(confB2)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)

	confA := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		cluster {
			listen: "127.0.0.1:-1"
		}
		gateway {
			name: "A"
			listen: "0.0.0.0:-1"
			gateways [
				{
					name: "B"
					urls: ["%s", "%s"]
				}
			]
		}
	`, fmt.Sprintf("nats://127.0.0.1:%d", ob1.Gateway.Port), fmt.Sprintf("nats://127.0.0.1:%d", ob2.Gateway.Port))))
	oa := LoadConfig(confA)
	oa.gatewaysSolicitDelay = 15 * time.Millisecond
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb1, 1, time.Second)
	waitForOutboundGateways(t, sb2, 1, time.Second)
	waitForInboundGateways(t, sa, 2, time.Second)

	checkAll := func(t *testing.T) {
		t.Helper()
		checkURL := func(t *testing.T, s *Server) {
			t.Helper()
			url := s.getGatewayURL()
			if strings.HasPrefix(url, "0.0.0.0") {
				t.Fatalf("URL still references 0.0.0.0")
			}
			s.gateway.RLock()
			for url := range s.gateway.URLs {
				if strings.HasPrefix(url, "0.0.0.0") {
					s.gateway.RUnlock()
					t.Fatalf("URL still references 0.0.0.0")
				}
			}
			s.gateway.RUnlock()

			var cfg *gatewayCfg
			if s.getGatewayName() == "A" {
				cfg = s.getRemoteGateway("B")
			} else {
				cfg = s.getRemoteGateway("A")
			}
			urls := cfg.getURLs()
			for _, url := range urls {
				if strings.HasPrefix(url.Host, "0.0.0.0") {
					t.Fatalf("URL still references 0.0.0.0")
				}
			}
		}
		checkURL(t, sb1)
		checkURL(t, sb2)
		checkURL(t, sa)
	}
	checkAll(t)
	// Perform a reload and ensure that nothing has changed
	servers := []*Server{sb1, sb2, sa}
	for _, s := range servers {
		if err := s.Reload(); err != nil {
			t.Fatalf("Error on reload: %v", err)
		}
		checkAll(t)
	}
}

func TestGatewayAdvertise(t *testing.T) {
	o3 := testDefaultOptionsForGateway("C")
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	// Set the advertise so that this points to C
	o1.Gateway.Advertise = fmt.Sprintf("127.0.0.1:%d", s3.GatewayAddr().Port)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// We should have outbound from s1 to s2
	waitForOutboundGateways(t, s1, 1, time.Second)
	// But no inbound from s2
	waitForInboundGateways(t, s1, 0, time.Second)

	// And since B tries to connect to A but reaches C, it should fail to connect,
	// and without connect retries, stop trying. So no outbound for s2, and no
	// inbound/outbound for s3.
	waitForInboundGateways(t, s2, 1, time.Second)
	waitForOutboundGateways(t, s2, 0, time.Second)
	waitForInboundGateways(t, s3, 0, time.Second)
	waitForOutboundGateways(t, s3, 0, time.Second)
}

func TestGatewayAdvertiseErr(t *testing.T) {
	o1 := testDefaultOptionsForGateway("A")
	o1.Gateway.Advertise = "wrong:address"
	s1 := New(o1)
	defer s1.Shutdown()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s1.Start()
		wg.Done()
	}()
	// We call Fatalf on listen error, but since there is no actual logger
	// associated, we just check that the listener is not created.
	time.Sleep(100 * time.Millisecond)
	addr := s1.GatewayAddr()
	if addr != nil {
		t.Fatal("Listener should not have been created")
	}
	s1.Shutdown()
	wg.Wait()
}

func TestGatewayAuth(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	o2.Gateway.Username = "me"
	o2.Gateway.Password = "pwd"
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithURLs(t, "A", "B", []string{fmt.Sprintf("nats://me:pwd@127.0.0.1:%d", s2.GatewayAddr().Port)})
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should have an outbound gateway to s2.
	waitForOutboundGateways(t, s1, 1, time.Second)
	// s2 should have an inbound gateway
	waitForInboundGateways(t, s2, 1, time.Second)

	s2.Shutdown()
	s1.Shutdown()

	o2.Gateway.Username = "me"
	o2.Gateway.Password = "wrong"
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	// Connection should fail...
	waitForGatewayFailedConnect(t, s1, "B", true, 2*time.Second)

	s2.Shutdown()
	s1.Shutdown()
	o2.Gateway.Username = "wrong"
	o2.Gateway.Password = "pwd"
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	// Connection should fail...
	waitForGatewayFailedConnect(t, s1, "B", true, 2*time.Second)
}

func TestGatewayTLS(t *testing.T) {
	o2 := testGatewayOptionsWithTLS(t, "B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithTLS(t, "A", "B", []string{fmt.Sprintf("nats://127.0.0.1:%d", s2.GatewayAddr().Port)})
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should have an outbound gateway to s2.
	waitForOutboundGateways(t, s1, 1, time.Second)
	// s2 should have an inbound gateway
	waitForInboundGateways(t, s2, 1, time.Second)
	// and vice-versa
	waitForOutboundGateways(t, s2, 1, time.Second)
	waitForInboundGateways(t, s1, 1, time.Second)

	// Stop s2 server
	s2.Shutdown()

	// gateway should go away
	waitForOutboundGateways(t, s1, 0, time.Second)
	waitForInboundGateways(t, s1, 0, time.Second)
	waitForOutboundGateways(t, s2, 0, time.Second)
	waitForInboundGateways(t, s2, 0, time.Second)

	// Restart server
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	// gateway should reconnect
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)
	waitForInboundGateways(t, s1, 1, 2*time.Second)
	waitForInboundGateways(t, s2, 1, 2*time.Second)

	s1.Shutdown()
	// Wait for s2 to lose connections to s1.
	waitForOutboundGateways(t, s2, 0, 2*time.Second)
	waitForInboundGateways(t, s2, 0, 2*time.Second)

	// Make an explicit TLS config for remote gateway config "B"
	// on cluster A.
	o1.Gateway.Gateways[0].TLSConfig = o1.Gateway.TLSConfig.Clone()
	u, _ := url.Parse(fmt.Sprintf("tls://localhost:%d", s2.GatewayAddr().Port))
	o1.Gateway.Gateways[0].URLs = []*url.URL{u}
	// Make the TLSTimeout so small that it should fail to connect.
	smallTimeout := 0.00000001
	o1.Gateway.Gateways[0].TLSTimeout = smallTimeout
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	// Check that s1 reports connection failures
	waitForGatewayFailedConnect(t, s1, "B", true, 2*time.Second)

	// Check that TLSConfig from s1's remote "B" is based on
	// what we have configured.
	cfg := s1.getRemoteGateway("B")
	cfg.RLock()
	tlsName := cfg.tlsName
	timeout := cfg.TLSTimeout
	cfg.RUnlock()
	if tlsName != "localhost" {
		t.Fatalf("Expected server name to be localhost, got %v", tlsName)
	}
	if timeout != smallTimeout {
		t.Fatalf("Expected tls timeout to be %v, got %v", smallTimeout, timeout)
	}
	s1.Shutdown()
	// Wait for s2 to lose connections to s1.
	waitForOutboundGateways(t, s2, 0, 2*time.Second)
	waitForInboundGateways(t, s2, 0, 2*time.Second)

	// Remove explicit TLSTimeout from gateway "B" and check that
	// we use the A's spec one.
	o1.Gateway.Gateways[0].TLSTimeout = 0
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)
	waitForInboundGateways(t, s1, 1, time.Second)
	waitForInboundGateways(t, s2, 1, time.Second)

	cfg = s1.getRemoteGateway("B")
	cfg.RLock()
	timeout = cfg.TLSTimeout
	cfg.RUnlock()
	if timeout != o1.Gateway.TLSTimeout {
		t.Fatalf("Expected tls timeout to be %v, got %v", o1.Gateway.TLSTimeout, timeout)
	}
}

func TestGatewayTLSErrors(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithTLS(t, "A", "B", []string{fmt.Sprintf("nats://127.0.0.1:%d", s2.ClusterAddr().Port)})
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// Expect s1 to have a failed to connect count > 0
	waitForGatewayFailedConnect(t, s1, "B", true, 2*time.Second)
}

func TestGatewayWrongDestination(t *testing.T) {
	// Start a server with a gateway named "C"
	o2 := testDefaultOptionsForGateway("C")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	// Configure a gateway to "B", but since we are connecting to "C"...
	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// we should not be able to connect.
	waitForGatewayFailedConnect(t, s1, "B", true, time.Second)

	// Shutdown s2 and fix the gateway name.
	// s1 should then connect ok and failed connect should be cleared.
	s2.Shutdown()

	// Reset the conn attempts
	cfg := s1.getRemoteGateway("B")
	cfg.resetConnAttempts()

	o2.Gateway.Name = "B"
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	// At some point, the number of failed connect count should be reset to 0.
	waitForGatewayFailedConnect(t, s1, "B", false, 2*time.Second)
}

func TestGatewayConnectToWrongPort(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	// Configure a gateway to "B", but connect to the wrong port
	urls := []string{fmt.Sprintf("nats://127.0.0.1:%d", s2.Addr().(*net.TCPAddr).Port)}
	o1 := testGatewayOptionsFromToWithURLs(t, "A", "B", urls)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// we should not be able to connect.
	waitForGatewayFailedConnect(t, s1, "B", true, time.Second)

	s1.Shutdown()

	// Repeat with route port
	urls = []string{fmt.Sprintf("nats://127.0.0.1:%d", s2.ClusterAddr().Port)}
	o1 = testGatewayOptionsFromToWithURLs(t, "A", "B", urls)
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	// we should not be able to connect.
	waitForGatewayFailedConnect(t, s1, "B", true, time.Second)

	s1.Shutdown()

	// Now have a client connect to s2's gateway port.
	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", s2.GatewayAddr().Port))
	if err == nil {
		nc.Close()
		t.Fatal("Expected error, got none")
	}
}

func TestGatewayCreateImplicit(t *testing.T) {
	// Create a regular cluster of 2 servers
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o3 := testDefaultOptionsForGateway("B")
	o3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s2.ClusterAddr().Port))
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	checkClusterFormed(t, s2, s3)

	// Now start s1 that creates a Gateway connection to s2 or s3
	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2, s3)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// We should have an outbound gateway connection on ALL servers.
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)
	waitForOutboundGateways(t, s3, 1, 2*time.Second)

	// Server s1 must have 2 inbound ones
	waitForInboundGateways(t, s1, 2, 2*time.Second)

	// However, s1 may have created the outbound to s2 or s3. It is possible that
	// either s2 or s3 does not an inbound connection.
	s2Inbound := s2.numInboundGateways()
	s3Inbound := s3.numInboundGateways()
	if (s2Inbound == 1 && s3Inbound != 0) || (s3Inbound == 1 && s2Inbound != 0) {
		t.Fatalf("Unexpected inbound for s2/s3: %v/%v", s2Inbound, s3Inbound)
	}
}

func TestGatewayCreateImplicitOnNewRoute(t *testing.T) {
	// Start with only 2 clusters of 1 server each
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	// Now start s1 that creates a Gateway connection to s2
	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// Check outbounds
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)

	// Now add a server to cluster B
	o3 := testDefaultOptionsForGateway("B")
	o3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s2.ClusterAddr().Port))
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	// Wait for cluster between s2/s3 to form
	checkClusterFormed(t, s2, s3)

	// s3 should have been notified about existence of A and create its gateway to A.
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)
	waitForOutboundGateways(t, s3, 1, 2*time.Second)
}

func TestGatewayImplicitReconnect(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	o2.Gateway.ConnectRetries = 5
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should have an outbound gateway to s2.
	waitForOutboundGateways(t, s1, 1, time.Second)
	// s2 should have an inbound gateway
	waitForInboundGateways(t, s2, 1, time.Second)
	// It will have also created an implicit outbound connection to s1.
	// We need to wait for that implicit outbound connection to be made
	// to show that it will try to reconnect when we stop/restart s1
	// (without config to connect to B).
	waitForOutboundGateways(t, s2, 1, time.Second)

	// Shutdown s1, remove the gateway from A to B and restart.
	s1.Shutdown()
	o1.Gateway.Gateways = o1.Gateway.Gateways[:0]
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should have both outbound and inbound to s2
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForInboundGateways(t, s1, 1, 2*time.Second)

	// Same for s2
	waitForOutboundGateways(t, s2, 1, 2*time.Second)
	waitForInboundGateways(t, s2, 1, 2*time.Second)

	// Verify that s2 still has "A" in its gateway config
	if s2.getRemoteGateway("A") == nil {
		t.Fatal("Gateway A should be in s2")
	}
}

func TestGatewayURLsFromClusterSentInINFO(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o3 := testDefaultOptionsForGateway("B")
	o3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s2.ClusterAddr().Port))
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	checkClusterFormed(t, s2, s3)

	// Now start s1 that creates a Gateway connection to s2
	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// Make sure we have proper outbound/inbound
	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)
	waitForOutboundGateways(t, s3, 1, time.Second)

	// Although s1 connected to s2 and knew only about s2, it should have
	// received the list of gateway URLs in the B cluster. So if we shutdown
	// server s2, it should be able to reconnect to s3.
	s2.Shutdown()
	// Wait for s3 to register that there s2 is gone.
	checkNumRoutes(t, s3, 0)
	// s1 should have reconnected to s3 because it learned about it
	// when connecting earlier to s2.
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	// Also make sure that the gateway's urls map has 2 urls.
	gw := s1.getRemoteGateway("B")
	if gw == nil {
		t.Fatal("Did not find gateway B")
	}
	gw.RLock()
	l := len(gw.urls)
	gw.RUnlock()
	if l != 2 {
		t.Fatalf("S1 should have 2 urls, got %v", l)
	}
}

func TestGatewayAutoDiscovery(t *testing.T) {
	o4 := testDefaultOptionsForGateway("D")
	s4 := runGatewayServer(o4)
	defer s4.Shutdown()

	o3 := testGatewayOptionsFromToWithServers(t, "C", "D", s4)
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	o2 := testGatewayOptionsFromToWithServers(t, "B", "C", s3)
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// Each server should have 3 outbound gateway connections.
	waitForOutboundGateways(t, s1, 3, 2*time.Second)
	waitForOutboundGateways(t, s2, 3, 2*time.Second)
	waitForOutboundGateways(t, s3, 3, 2*time.Second)
	waitForOutboundGateways(t, s4, 3, 2*time.Second)

	s1.Shutdown()
	s2.Shutdown()
	s3.Shutdown()
	s4.Shutdown()

	o2 = testDefaultOptionsForGateway("B")
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	o4 = testGatewayOptionsFromToWithServers(t, "D", "B", s2)
	s4 = runGatewayServer(o4)
	defer s4.Shutdown()

	o3 = testGatewayOptionsFromToWithServers(t, "C", "B", s2)
	s3 = runGatewayServer(o3)
	defer s3.Shutdown()

	o1 = testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	// Each server should have 3 outbound gateway connections.
	waitForOutboundGateways(t, s1, 3, 2*time.Second)
	waitForOutboundGateways(t, s2, 3, 2*time.Second)
	waitForOutboundGateways(t, s3, 3, 2*time.Second)
	waitForOutboundGateways(t, s4, 3, 2*time.Second)

	s1.Shutdown()
	s2.Shutdown()
	s3.Shutdown()
	s4.Shutdown()

	o1 = testDefaultOptionsForGateway("A")
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	o2 = testDefaultOptionsForGateway("A")
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s1.ClusterAddr().Port))
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	o3 = testDefaultOptionsForGateway("A")
	o3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s1.ClusterAddr().Port))
	s3 = runGatewayServer(o3)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	o4 = testGatewayOptionsFromToWithServers(t, "B", "A", s1)
	s4 = runGatewayServer(o4)
	defer s4.Shutdown()

	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)
	waitForOutboundGateways(t, s3, 1, 2*time.Second)
	waitForOutboundGateways(t, s4, 1, 2*time.Second)
	waitForInboundGateways(t, s4, 3, 2*time.Second)

	o5 := testGatewayOptionsFromToWithServers(t, "C", "B", s4)
	s5 := runGatewayServer(o5)
	defer s5.Shutdown()

	waitForOutboundGateways(t, s1, 2, 2*time.Second)
	waitForOutboundGateways(t, s2, 2, 2*time.Second)
	waitForOutboundGateways(t, s3, 2, 2*time.Second)
	waitForOutboundGateways(t, s4, 2, 2*time.Second)
	waitForInboundGateways(t, s4, 4, 2*time.Second)
	waitForOutboundGateways(t, s5, 2, 2*time.Second)
	waitForInboundGateways(t, s5, 4, 2*time.Second)
}

func TestGatewayRejectUnknown(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	// Create a gateway from A to B, but configure B to reject non configured ones.
	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	o1.Gateway.RejectUnknown = true
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// Wait for outbound/inbound to be created.
	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)
	waitForInboundGateways(t, s1, 1, time.Second)
	waitForInboundGateways(t, s2, 1, time.Second)

	// Create gateway C to B. B will tell C to connect to A,
	// which A should reject.
	o3 := testGatewayOptionsFromToWithServers(t, "C", "B", s2)
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	// s3 should have outbound to B, but not to A
	waitForOutboundGateways(t, s3, 1, time.Second)
	// s2 should have 2 inbounds (one from s1 one from s3)
	waitForInboundGateways(t, s2, 2, time.Second)

	// s1 should have single outbound/inbound with s2.
	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForInboundGateways(t, s1, 1, time.Second)

	// It should not have a registered remote gateway with C (s3)
	if s1.getOutboundGatewayConnection("C") != nil {
		t.Fatalf("A should not have outbound gateway to C")
	}
	if s1.getRemoteGateway("C") != nil {
		t.Fatalf("A should not have a registered remote gateway to C")
	}

	// Restart s1 and this time, B will tell A to connect to C.
	// But A will not even attempt that since it does not have
	// C configured.
	s1.Shutdown()
	waitForOutboundGateways(t, s2, 1, time.Second)
	waitForInboundGateways(t, s2, 1, time.Second)
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()
	waitForOutboundGateways(t, s2, 2, time.Second)
	waitForInboundGateways(t, s2, 2, time.Second)
	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForInboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s3, 1, time.Second)
	waitForInboundGateways(t, s3, 1, time.Second)
	// It should not have a registered remote gateway with C (s3)
	if s1.getOutboundGatewayConnection("C") != nil {
		t.Fatalf("A should not have outbound gateway to C")
	}
	if s1.getRemoteGateway("C") != nil {
		t.Fatalf("A should not have a registered remote gateway to C")
	}
}

func TestGatewayNoReconnectOnClose(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	// Shutdown s1, and check that there is no attempt to reconnect.
	s1.Shutdown()
	time.Sleep(250 * time.Millisecond)
	waitForOutboundGateways(t, s1, 0, time.Second)
	waitForOutboundGateways(t, s2, 0, time.Second)
	waitForInboundGateways(t, s2, 0, time.Second)
}

func TestGatewayDontSendSubInterest(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	s2Url := fmt.Sprintf("nats://127.0.0.1:%d", o2.Port)
	subnc := natsConnect(t, s2Url)
	defer subnc.Close()
	natsSub(t, subnc, "foo", func(_ *nats.Msg) {})
	natsFlush(t, subnc)

	checkExpectedSubs(t, 1, s2)
	// Subscription should not be sent to s1
	checkExpectedSubs(t, 0, s1)

	// Restart s1
	s1.Shutdown()
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()
	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	checkExpectedSubs(t, 1, s2)
	checkExpectedSubs(t, 0, s1)
}

func setAccountUserPassInOptions(o *Options, accName, username, password string) {
	acc := NewAccount(accName)
	o.Accounts = append(o.Accounts, acc)
	o.Users = append(o.Users, &User{Username: username, Password: password, Account: acc})
}

func TestGatewayAccountInterest(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	setAccountUserPassInOptions(o1, "$foo", "ivan", "password")
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// Make this server initiate connection to A, so it is faster
	// when restarting it at the end of this test.
	o3 := testGatewayOptionsFromToWithServers(t, "C", "A", s1)
	setAccountUserPassInOptions(o3, "$foo", "ivan", "password")
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	waitForOutboundGateways(t, s1, 2, time.Second)
	waitForOutboundGateways(t, s2, 2, time.Second)
	waitForOutboundGateways(t, s3, 2, time.Second)

	s1Url := fmt.Sprintf("nats://ivan:password@127.0.0.1:%d", o1.Port)
	nc := natsConnect(t, s1Url)
	defer nc.Close()
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)

	// On first send, the message should be sent.
	checkCount := func(t *testing.T, c *client, expected int) {
		t.Helper()
		c.mu.Lock()
		out := c.outMsgs
		c.mu.Unlock()
		if int(out) != expected {
			t.Fatalf("Expected %d message(s) to be sent over, got %v", expected, out)
		}
	}
	gwcb := s1.getOutboundGatewayConnection("B")
	checkCount(t, gwcb, 1)
	gwcc := s1.getOutboundGatewayConnection("C")
	checkCount(t, gwcc, 1)

	// S2 should have sent a protocol indicating no interest.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if _, inMap := gwcb.gw.outsim.Load("$foo"); !inMap {
			return fmt.Errorf("Did not receive account no interest")
		}
		return nil
	})
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		ei, inMap := gwcc.gw.outsim.Load("$foo")
		if !inMap {
			return fmt.Errorf("Did not receive subject no interest")
		}
		e := ei.(*outsie)
		e.RLock()
		_, inMap = e.ni["foo"]
		e.RUnlock()
		if !inMap {
			return fmt.Errorf("Did not receive subject no interest")
		}
		return nil
	})
	// Second send should not go through to B
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 1)
	// it won't go to C, not because there is no account interest,
	// but because there is no interest on the subject.
	checkCount(t, gwcc, 1)

	// Add account to S2, this should clear the no interest for that account.
	s2.RegisterAccount("$foo")
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if _, inMap := gwcb.gw.outsim.Load("$foo"); inMap {
			return fmt.Errorf("NoInterest has not been cleared")
		}
		return nil
	})
	// Now publish a message that should go to B
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 2)
	// Still won't go to C since there is no sub interest
	checkCount(t, gwcc, 1)

	// Restart C and that should reset the no-interest
	s3.Shutdown()
	s3 = runGatewayServer(o3)
	defer s3.Shutdown()

	waitForOutboundGateways(t, s1, 2, 2*time.Second)
	waitForOutboundGateways(t, s2, 2, 2*time.Second)
	waitForOutboundGateways(t, s3, 2, 2*time.Second)

	// First refresh gwcc
	gwcc = s1.getOutboundGatewayConnection("C")
	// Verify that it's count is 0
	checkCount(t, gwcc, 0)
	// Publish and now...
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	// it should not go to B (no sub interest)
	checkCount(t, gwcb, 2)
	// but will go to C
	checkCount(t, gwcc, 1)
}

func TestGatewaySubjectInterest(t *testing.T) {
	o1 := testDefaultOptionsForGateway("A")
	setAccountUserPassInOptions(o1, "$foo", "ivan", "password")
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	o2 := testGatewayOptionsFromToWithServers(t, "B", "A", s1)
	setAccountUserPassInOptions(o2, "$foo", "ivan", "password")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	s1Url := fmt.Sprintf("nats://ivan:password@127.0.0.1:%d", o1.Port)
	nc := natsConnect(t, s1Url)
	defer nc.Close()
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)

	// On first send, the message should be sent.
	checkCount := func(t *testing.T, c *client, expected int) {
		t.Helper()
		c.mu.Lock()
		out := c.outMsgs
		c.mu.Unlock()
		if int(out) != expected {
			t.Fatalf("Expected %d message(s) to be sent over, got %v", expected, out)
		}
	}
	gwcb := s1.getOutboundGatewayConnection("B")
	checkCount(t, gwcb, 1)

	// S2 should have sent a protocol indicating no subject interest.
	checkNoInterest := func(t *testing.T, subject string, expectedNoInterest bool) {
		t.Helper()
		checkForNoInterest(t, gwcb, "$foo", subject, expectedNoInterest, 2*time.Second)
	}
	checkNoInterest(t, "foo", true)
	// Second send should not go through to B
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 1)

	// Now create subscription interest on B (s2)
	s2Url := fmt.Sprintf("nats://ivan:password@127.0.0.1:%d", o2.Port)
	ncb := natsConnect(t, s2Url)
	defer ncb.Close()
	ch := make(chan bool, 1)
	sub := natsSub(t, ncb, "foo", func(_ *nats.Msg) {
		ch <- true
	})
	natsFlush(t, ncb)
	checkExpectedSubs(t, 1, s2)
	checkExpectedSubs(t, 0, s1)

	// This should clear the no interest for this subject
	checkNoInterest(t, "foo", false)
	// Third send should go to B
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 2)

	// Make sure message is received
	waitCh(t, ch, "Did not get our message")
	// Now unsubscribe, there won't be an UNSUB sent to the gateway.
	natsUnsub(t, sub)
	natsFlush(t, ncb)
	checkExpectedSubs(t, 0, s2)
	checkExpectedSubs(t, 0, s1)

	// So now sending a message should go over, but then we should get an RS-
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 3)

	checkNoInterest(t, "foo", true)

	// Send one more time and now it should not go to B
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 3)

	// Send on bar, message should go over.
	natsPub(t, nc, "bar", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 4)

	// But now we should have receives an RS- on bar.
	checkNoInterest(t, "bar", true)

	// Check that wildcards are supported. Create a subscription on '*' on B.
	// This should clear the no-interest on both "foo" and "bar"
	natsSub(t, ncb, "*", func(_ *nats.Msg) {})
	natsFlush(t, ncb)
	checkExpectedSubs(t, 1, s2)
	checkExpectedSubs(t, 0, s1)
	checkNoInterest(t, "foo", false)
	checkNoInterest(t, "bar", false)
	// Publish on message on foo and one on bar and they should go.
	natsPub(t, nc, "foo", []byte("hello"))
	natsPub(t, nc, "bar", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 6)

	// Restart B and that should clear everything on A
	ncb.Close()
	s2.Shutdown()
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	gwcb = s1.getOutboundGatewayConnection("B")
	checkCount(t, gwcb, 0)
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 1)

	checkNoInterest(t, "foo", true)

	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 1)

	// Add a node to B cluster and subscribe there.
	// We want to ensure that the no-interest is cleared
	// when s2 receives remote SUB from s2bis
	o2bis := testGatewayOptionsFromToWithServers(t, "B", "A", s1)
	setAccountUserPassInOptions(o2bis, "$foo", "ivan", "password")
	o2bis.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s2.ClusterAddr().Port))
	s2bis := runGatewayServer(o2bis)
	defer s2bis.Shutdown()

	checkClusterFormed(t, s2, s2bis)

	// Make sure all outbound gateway connections are setup
	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)
	waitForOutboundGateways(t, s2bis, 1, time.Second)

	// A should have 2 inbound
	waitForInboundGateways(t, s1, 2, time.Second)

	// Create sub on s2bis
	ncb2bis := natsConnect(t, fmt.Sprintf("nats://ivan:password@127.0.0.1:%d", o2bis.Port))
	defer ncb2bis.Close()
	natsSub(t, ncb2bis, "foo", func(_ *nats.Msg) {})
	natsFlush(t, ncb2bis)

	// Wait for subscription to be registered locally on s2bis and remotely on s2
	checkExpectedSubs(t, 1, s2, s2bis)

	// Check that subject no-interest on A was cleared.
	checkNoInterest(t, "foo", false)

	// Now publish. Remember, s1 has outbound gateway to s2, and s2 does not
	// have a local subscription and has previously sent a no-interest on "foo".
	// We check that this has been cleared due to the interest on s2bis.
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 2)
}

func TestGatewayDoesntSendBackToItself(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	s2Url := fmt.Sprintf("nats://127.0.0.1:%d", o2.Port)
	nc2 := natsConnect(t, s2Url)
	defer nc2.Close()

	count := int32(0)
	cb := func(_ *nats.Msg) {
		atomic.AddInt32(&count, 1)
	}
	natsSub(t, nc2, "foo", cb)
	natsFlush(t, nc2)

	s1Url := fmt.Sprintf("nats://127.0.0.1:%d", o1.Port)
	nc1 := natsConnect(t, s1Url)
	defer nc1.Close()

	natsSub(t, nc1, "foo", cb)
	natsFlush(t, nc1)

	// Now send 1 message. If there is a cycle, after few ms we
	// should have tons of messages...
	natsPub(t, nc1, "foo", []byte("cycle"))
	natsFlush(t, nc1)
	time.Sleep(100 * time.Millisecond)
	if c := atomic.LoadInt32(&count); c != 2 {
		t.Fatalf("Expected only 2 messages, got %v", c)
	}
}

func TestGatewayOrderedOutbounds(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	o3 := testGatewayOptionsFromToWithServers(t, "C", "B", s2)
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	waitForOutboundGateways(t, s1, 2, time.Second)
	waitForOutboundGateways(t, s2, 2, time.Second)
	waitForOutboundGateways(t, s3, 2, time.Second)

	gws := make([]*client, 0, 2)
	s2.getOutboundGatewayConnections(&gws)

	// RTTs are expected to be initially 0. So update RTT of first
	// in the array so that its value is no longer 0, this should
	// cause order to be flipped.
	c := gws[0]
	c.mu.Lock()
	c.sendPing()
	c.mu.Unlock()

	// Wait a tiny but
	time.Sleep(15 * time.Millisecond)
	// Get the ordering again.
	gws = gws[:0]
	s2.getOutboundGatewayConnections(&gws)
	// Verify order is correct.
	fRTT := gws[0].getRTTValue()
	sRTT := gws[1].getRTTValue()
	if fRTT > sRTT {
		t.Fatalf("Wrong ordering: %v, %v", fRTT, sRTT)
	}

	// What is the first in the array?
	gws[0].mu.Lock()
	gwName := gws[0].gw.name
	gws[0].mu.Unlock()
	if gwName == "A" {
		s1.Shutdown()
	} else {
		s3.Shutdown()
	}
	waitForOutboundGateways(t, s2, 1, time.Second)
	gws = gws[:0]
	s2.getOutboundGatewayConnections(&gws)
	if len(gws) != 1 {
		t.Fatalf("Expected size of outo to be 1, got %v", len(gws))
	}
	gws[0].mu.Lock()
	name := gws[0].gw.name
	gws[0].mu.Unlock()
	if gwName == name {
		t.Fatalf("Gateway %q should have been removed", gwName)
	}
	// Stop the remaining gateway
	if gwName == "A" {
		s3.Shutdown()
	} else {
		s1.Shutdown()
	}
	waitForOutboundGateways(t, s2, 0, time.Second)
	gws = gws[:0]
	s2.getOutboundGatewayConnections(&gws)
	if len(gws) != 0 {
		t.Fatalf("Expected size of outo to be 0, got %v", len(gws))
	}
}

func TestGatewayQueueSub(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	sBUrl := fmt.Sprintf("nats://127.0.0.1:%d", o2.Port)
	ncB := natsConnect(t, sBUrl)
	defer ncB.Close()

	count2 := int32(0)
	cb2 := func(_ *nats.Msg) {
		atomic.AddInt32(&count2, 1)
	}
	qsubOnB := natsQueueSub(t, ncB, "foo", "bar", cb2)
	natsFlush(t, ncB)

	sAUrl := fmt.Sprintf("nats://127.0.0.1:%d", o1.Port)
	ncA := natsConnect(t, sAUrl)
	defer ncA.Close()

	count1 := int32(0)
	cb1 := func(_ *nats.Msg) {
		atomic.AddInt32(&count1, 1)
	}
	qsubOnA := natsQueueSub(t, ncA, "foo", "bar", cb1)
	natsFlush(t, ncA)

	// Make sure subs are registered on each server
	checkExpectedSubs(t, 1, s1, s2)
	checkForRegisteredQSubInterest(t, s1, "B", globalAccountName, "foo", 1, time.Second)
	checkForRegisteredQSubInterest(t, s2, "A", globalAccountName, "foo", 1, time.Second)

	total := 100
	send := func(t *testing.T, nc *nats.Conn) {
		t.Helper()
		for i := 0; i < total; i++ {
			// Alternate with adding a reply
			if i%2 == 0 {
				natsPubReq(t, nc, "foo", "reply", []byte("msg"))
			} else {
				natsPub(t, nc, "foo", []byte("msg"))
			}
		}
		natsFlush(t, nc)
	}
	// Send from client connecting to S1 (cluster A)
	send(t, ncA)

	check := func(t *testing.T, count *int32, expected int) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			if n := int(atomic.LoadInt32(count)); n != expected {
				return fmt.Errorf("Expected to get %v messages, got %v", expected, n)
			}
			return nil
		})
	}
	// Check that all messages stay on S1 (cluster A)
	check(t, &count1, total)
	check(t, &count2, 0)

	// Now send from the other side
	send(t, ncB)
	check(t, &count1, total)
	check(t, &count2, total)

	// Reset counters
	atomic.StoreInt32(&count1, 0)
	atomic.StoreInt32(&count2, 0)

	// Add different queue group and make sure that messages are received
	count3 := int32(0)
	cb3 := func(_ *nats.Msg) {
		atomic.AddInt32(&count3, 1)
	}
	batQSub := natsQueueSub(t, ncB, "foo", "bat", cb3)
	natsFlush(t, ncB)
	checkExpectedSubs(t, 2, s2)

	checkForRegisteredQSubInterest(t, s1, "B", globalAccountName, "foo", 2, time.Second)

	send(t, ncA)
	check(t, &count1, total)
	check(t, &count2, 0)
	check(t, &count3, total)

	// Reset counters
	atomic.StoreInt32(&count1, 0)
	atomic.StoreInt32(&count2, 0)
	atomic.StoreInt32(&count3, 0)

	natsUnsub(t, batQSub)
	natsFlush(t, ncB)
	checkExpectedSubs(t, 1, s2)

	checkForRegisteredQSubInterest(t, s1, "B", globalAccountName, "foo", 1, time.Second)

	// Stop qsub on A, and send messages to A, they should
	// be routed to B.
	qsubOnA.Unsubscribe()
	checkExpectedSubs(t, 0, s1)
	send(t, ncA)
	check(t, &count1, 0)
	check(t, &count2, total)

	// Reset counters
	atomic.StoreInt32(&count1, 0)
	atomic.StoreInt32(&count2, 0)

	// Create a C gateway now
	o3 := testGatewayOptionsFromToWithServers(t, "C", "B", s2)
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	waitForOutboundGateways(t, s1, 2, time.Second)
	waitForOutboundGateways(t, s2, 2, time.Second)
	waitForOutboundGateways(t, s3, 2, time.Second)

	waitForInboundGateways(t, s1, 2, time.Second)
	waitForInboundGateways(t, s2, 2, time.Second)
	waitForInboundGateways(t, s3, 2, time.Second)

	// Create another qsub "bar"
	sCUrl := fmt.Sprintf("nats://127.0.0.1:%d", o3.Port)
	ncC := natsConnect(t, sCUrl)
	defer ncC.Close()
	// Associate this with count1 (since A qsub is no longer running)
	natsQueueSub(t, ncC, "foo", "bar", cb1)
	natsFlush(t, ncC)
	checkExpectedSubs(t, 1, s3)
	checkForRegisteredQSubInterest(t, s1, "C", globalAccountName, "foo", 1, time.Second)
	checkForRegisteredQSubInterest(t, s2, "C", globalAccountName, "foo", 1, time.Second)

	// Artificially bump the RTT from A to C so that
	// the code should favor sending to B.
	gwcC := s1.getOutboundGatewayConnection("C")
	gwcC.mu.Lock()
	gwcC.rtt = 10 * time.Second
	gwcC.mu.Unlock()
	s1.gateway.orderOutboundConnections()

	send(t, ncA)
	check(t, &count1, 0)
	check(t, &count2, total)

	// Add a new group on s3 that should receive all messages
	natsQueueSub(t, ncC, "foo", "baz", cb3)
	natsFlush(t, ncC)
	checkExpectedSubs(t, 2, s3)
	checkForRegisteredQSubInterest(t, s1, "C", globalAccountName, "foo", 2, time.Second)
	checkForRegisteredQSubInterest(t, s2, "C", globalAccountName, "foo", 2, time.Second)

	// Reset counters
	atomic.StoreInt32(&count1, 0)
	atomic.StoreInt32(&count2, 0)

	// Make the RTTs equal
	gwcC.mu.Lock()
	gwcC.rtt = time.Second
	gwcC.mu.Unlock()

	gwcB := s1.getOutboundGatewayConnection("B")
	gwcB.mu.Lock()
	gwcB.rtt = time.Second
	gwcB.mu.Unlock()

	s1.gateway.Lock()
	s1.gateway.orderOutboundConnectionsLocked()
	destName := s1.gateway.outo[0].gw.name
	s1.gateway.Unlock()

	send(t, ncA)
	// Group baz should receive all messages
	check(t, &count3, total)

	// Ordering is normally re-evaluated when processing PONGs,
	// but rest of the time order will remain the same.
	// Since RTT are equal, messages will go to the first
	// GW in the array.
	if destName == "B" {
		check(t, &count2, total)
	} else if destName == "C" && int(atomic.LoadInt32(&count2)) != total {
		check(t, &count1, total)
	}

	// Unsubscribe qsub on B and C should receive
	// all messages on count1 and count3.
	qsubOnB.Unsubscribe()
	checkExpectedSubs(t, 0, s2)

	// gwcB should have the qsubs interest map empty now.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		ei, _ := gwcB.gw.outsim.Load(globalAccountName)
		if ei != nil {
			sl := ei.(*outsie).sl
			if sl.Count() == 0 {
				return nil
			}
		}
		return fmt.Errorf("Qsub interest for account should have been removed")
	})

	// Reset counters
	atomic.StoreInt32(&count1, 0)
	atomic.StoreInt32(&count2, 0)
	atomic.StoreInt32(&count3, 0)

	send(t, ncA)
	check(t, &count1, total)
	check(t, &count3, total)
}

func TestGatewayTotalQSubs(t *testing.T) {
	ob1 := testDefaultOptionsForGateway("B")
	sb1 := runGatewayServer(ob1)
	defer sb1.Shutdown()

	ob2 := testDefaultOptionsForGateway("B")
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sb1.ClusterAddr().Port))
	sb2 := runGatewayServer(ob2)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)

	sb1URL := fmt.Sprintf("nats://%s:%d", ob1.Host, ob1.Port)
	ncb1 := natsConnect(t, sb1URL, nats.ReconnectWait(50*time.Millisecond))
	defer ncb1.Close()

	sb2URL := fmt.Sprintf("nats://%s:%d", ob2.Host, ob2.Port)
	ncb2 := natsConnect(t, sb2URL, nats.ReconnectWait(50*time.Millisecond))
	defer ncb2.Close()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb1, 1, 2*time.Second)
	waitForOutboundGateways(t, sb2, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 2, 2*time.Second)
	waitForInboundGateways(t, sb1, 1, 2*time.Second)

	checkTotalQSubs := func(t *testing.T, s *Server, expected int) {
		t.Helper()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			if n := int(atomic.LoadInt64(&s.gateway.totalQSubs)); n != expected {
				return fmt.Errorf("Expected TotalQSubs to be %v, got %v", expected, n)
			}
			return nil
		})
	}

	cb := func(_ *nats.Msg) {}

	natsQueueSub(t, ncb1, "foo", "bar", cb)
	checkTotalQSubs(t, sa, 1)
	qsub2 := natsQueueSub(t, ncb1, "foo", "baz", cb)
	checkTotalQSubs(t, sa, 2)
	qsub3 := natsQueueSub(t, ncb1, "foo", "baz", cb)
	checkTotalQSubs(t, sa, 2)

	// Shutdown sb1, there should be a failover from clients
	// to sb2. sb2 will then send the queue subs to sa.
	sb1.Shutdown()

	checkClientsCount(t, sb2, 2)
	checkExpectedSubs(t, 3, sb2)

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb2, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sb2, 1, 2*time.Second)

	// When sb1 is shutdown, the total qsubs on sa should fall
	// down to 0, but will be updated as soon as sa and sb2
	// connect to each other. So instead we will verify by
	// making sure that the count is 2 instead of 4 if there
	// was a bug.
	// (note that there are 2 qsubs on same group, so only
	// 1 RS+ would have been sent for that group)
	checkTotalQSubs(t, sa, 2)

	// Restart sb1
	sb1 = runGatewayServer(ob1)
	defer sb1.Shutdown()

	checkClusterFormed(t, sb1, sb2)

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb1, 1, 2*time.Second)
	waitForOutboundGateways(t, sb2, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 2, 2*time.Second)
	waitForInboundGateways(t, sb1, 0, 2*time.Second)
	waitForInboundGateways(t, sb2, 1, 2*time.Second)

	// Now start unsubscribing. Start with one of the duplicate
	// and check that count stays same.
	natsUnsub(t, qsub3)
	checkTotalQSubs(t, sa, 2)
	// Now the other, which would cause an RS-
	natsUnsub(t, qsub2)
	checkTotalQSubs(t, sa, 1)
	// Now test that if connections are closed, things are updated
	// properly.
	ncb1.Close()
	ncb2.Close()
	checkTotalQSubs(t, sa, 0)
}

func TestGatewaySendQSubsOnGatewayConnect(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	s2Url := fmt.Sprintf("nats://127.0.0.1:%d", o2.Port)
	subnc := natsConnect(t, s2Url)
	defer subnc.Close()

	ch := make(chan bool, 1)
	cb := func(_ *nats.Msg) {
		ch <- true
	}
	natsQueueSub(t, subnc, "foo", "bar", cb)
	natsFlush(t, subnc)

	// Now start s1 that creates a gateway to s2
	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	checkForRegisteredQSubInterest(t, s1, "B", globalAccountName, "foo", 1, time.Second)

	// Publish from s1, message should be received on s2.
	pubnc := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", o1.Port))
	defer pubnc.Close()
	// Publish 1 message
	natsPub(t, pubnc, "foo", []byte("hello"))
	waitCh(t, ch, "Did not get out message")
	pubnc.Close()

	s1.Shutdown()
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	checkForRegisteredQSubInterest(t, s1, "B", globalAccountName, "foo", 1, time.Second)

	pubnc = natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", o1.Port))
	defer pubnc.Close()
	// Publish 1 message
	natsPub(t, pubnc, "foo", []byte("hello"))
	waitCh(t, ch, "Did not get out message")
}

func TestGatewaySendRemoteQSubs(t *testing.T) {
	ob1 := testDefaultOptionsForGateway("B")
	sb1 := runGatewayServer(ob1)
	defer sb1.Shutdown()

	ob2 := testDefaultOptionsForGateway("B")
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", ob1.Cluster.Host, ob1.Cluster.Port))
	sb2 := runGatewayServer(ob2)

	checkClusterFormed(t, sb1, sb2)

	sbURL := fmt.Sprintf("nats://127.0.0.1:%d", ob2.Port)
	subnc := natsConnect(t, sbURL)
	defer subnc.Close()

	ch := make(chan bool, 1)
	cb := func(_ *nats.Msg) {
		ch <- true
	}
	qsub1 := natsQueueSub(t, subnc, "foo", "bar", cb)
	qsub2 := natsQueueSub(t, subnc, "foo", "bar", cb)
	natsFlush(t, subnc)

	// There will be 2 local qsubs on the sb2 server where the client is connected
	checkExpectedSubs(t, 2, sb2)
	// But only 1 remote on sb1
	checkExpectedSubs(t, 1, sb1)

	// Now start s1 that creates a gateway to sb1 (the one that does not have the local QSub)
	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb1, 1, time.Second)
	waitForOutboundGateways(t, sb2, 1, time.Second)

	checkForRegisteredQSubInterest(t, sa, "B", globalAccountName, "foo", 1, time.Second)

	// Publish from s1, message should be received on s2.
	saURL := fmt.Sprintf("nats://127.0.0.1:%d", oa.Port)
	pubnc := natsConnect(t, saURL)
	defer pubnc.Close()
	// Publish 1 message
	natsPub(t, pubnc, "foo", []byte("hello"))
	natsFlush(t, pubnc)
	waitCh(t, ch, "Did not get out message")

	// Note that since cluster B has no plain sub, an "RS- $G foo" will have been sent.
	// Wait for the no interest to be received by A
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		gw := sa.getOutboundGatewayConnection("B").gw
		ei, _ := gw.outsim.Load(globalAccountName)
		if ei != nil {
			e := ei.(*outsie)
			e.RLock()
			defer e.RUnlock()
			if _, inMap := e.ni["foo"]; inMap {
				return nil
			}
		}
		return fmt.Errorf("No-interest still not registered")
	})

	// Unsubscribe 1 qsub
	natsUnsub(t, qsub1)
	natsFlush(t, subnc)
	// There should be only 1 local qsub on sb2 now, and the remote should still exist on sb1
	checkExpectedSubs(t, 1, sb1, sb2)

	// Publish 1 message
	natsPub(t, pubnc, "foo", []byte("hello"))
	natsFlush(t, pubnc)
	waitCh(t, ch, "Did not get out message")

	// Unsubscribe the remaining
	natsUnsub(t, qsub2)
	natsFlush(t, subnc)

	// No more subs now on both sb1 and sb2
	checkExpectedSubs(t, 0, sb1, sb2)

	// Server sb1 should not have qsub in its sub interest map
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		var entry *sitally
		sb1.gateway.pasi.Lock()
		asim := sb1.gateway.pasi.m[globalAccountName]
		if asim != nil {
			entry = asim["foo bar"]
		}
		sb1.gateway.pasi.Unlock()
		if entry != nil {
			return fmt.Errorf("Map should not have an entry, got %#v", entry)
		}
		return nil
	})

	// Let's wait for A to receive the unsubscribe
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		gw := sa.getOutboundGatewayConnection("B").gw
		ei, _ := gw.outsim.Load(globalAccountName)
		if ei != nil {
			sl := ei.(*outsie).sl
			if sl.Count() == 0 {
				return nil
			}
		}
		return fmt.Errorf("Interest still present")
	})

	// Now send a message, it won't be sent because A received an RS-
	// on the first published message since there was no plain sub interest.
	natsPub(t, pubnc, "foo", []byte("hello"))
	natsFlush(t, pubnc)

	// Get the gateway connection from A (sa) to B (sb1)
	gw := sa.getOutboundGatewayConnection("B")
	gw.mu.Lock()
	out := gw.outMsgs
	gw.mu.Unlock()
	if out != 2 {
		t.Fatalf("Expected 2 out messages, got %v", out)
	}

	// Restart A
	pubnc.Close()
	sa.Shutdown()
	sa = runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)

	// Check qsubs interest should be empty
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		gw := sa.getOutboundGatewayConnection("B").gw
		if ei, _ := gw.outsim.Load(globalAccountName); ei == nil {
			return nil
		}
		return fmt.Errorf("Interest still present")
	})
}

func TestGatewayComplexSetup(t *testing.T) {
	doLog := false

	// This test will have the following setup:
	// --- means route connection
	// === means gateway connection
	// [o] is outbound
	// [i] is inbound
	// Each server as an outbound connection to the other cluster.
	// It may have 0 or more inbound connection(s).
	//
	//  Cluster A		Cluster B
	//   sa1 [o]===========>[i]
	//    |  [i]<===========[o]
	//    |            		sb1 ------- sb2
	//    |					[i]			[o]
	//   sa2 [o]=============^   		 ||
	//		 [i]<========================||
	ob1 := testDefaultOptionsForGateway("B")
	sb1 := runGatewayServer(ob1)
	defer sb1.Shutdown()
	if doLog {
		sb1.SetLogger(logger.NewTestLogger("[B1] - ", true), true, true)
	}

	oa1 := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	sa1 := runGatewayServer(oa1)
	defer sa1.Shutdown()
	if doLog {
		sa1.SetLogger(logger.NewTestLogger("[A1] - ", true), true, true)
	}

	waitForOutboundGateways(t, sa1, 1, time.Second)
	waitForOutboundGateways(t, sb1, 1, time.Second)

	waitForInboundGateways(t, sa1, 1, time.Second)
	waitForInboundGateways(t, sb1, 1, time.Second)

	oa2 := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	oa2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sa1.ClusterAddr().Port))
	sa2 := runGatewayServer(oa2)
	defer sa2.Shutdown()
	if doLog {
		sa2.SetLogger(logger.NewTestLogger("[A2] - ", true), true, true)
	}

	checkClusterFormed(t, sa1, sa2)

	waitForOutboundGateways(t, sa2, 1, time.Second)
	waitForInboundGateways(t, sb1, 2, time.Second)

	ob2 := testGatewayOptionsFromToWithServers(t, "B", "A", sa2)
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sb1.ClusterAddr().Port))
	var sb2 *Server
	for {
		sb2 = runGatewayServer(ob2)
		defer sb2.Shutdown()

		checkClusterFormed(t, sb1, sb2)

		waitForOutboundGateways(t, sb2, 1, time.Second)
		waitForInboundGateways(t, sb2, 0, time.Second)
		// For this test, we want the outbound to be to sa2, so if we don't have that,
		// restart sb2 until we get lucky.
		time.Sleep(100 * time.Millisecond)
		if sa2.numInboundGateways() == 0 {
			sb2.Shutdown()
			sb2 = nil
		} else {
			break
		}
	}
	if doLog {
		sb2.SetLogger(logger.NewTestLogger("[B2] - ", true), true, true)
	}

	ch := make(chan bool, 1)
	cb := func(_ *nats.Msg) {
		ch <- true
	}

	// Create a subscription on sa1 and sa2.
	ncsa1 := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", oa1.Port))
	defer ncsa1.Close()
	sub1 := natsSub(t, ncsa1, "foo", cb)
	natsFlush(t, ncsa1)

	ncsa2 := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", oa2.Port))
	defer ncsa2.Close()
	sub2 := natsSub(t, ncsa2, "foo", cb)
	natsFlush(t, ncsa2)

	// sa1 will have 1 local, one remote (from sa2), same for sa2.
	checkExpectedSubs(t, 2, sa1, sa2)

	// Connect to sb2 and send 1 message
	ncsb2 := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", ob2.Port))
	defer ncsb2.Close()
	natsPub(t, ncsb2, "foo", []byte("hello"))
	natsFlush(t, ncsb2)

	for i := 0; i < 2; i++ {
		waitCh(t, ch, "Did not get our message")
	}

	// Unsubscribe sub2, and send 1, should still get it.
	natsUnsub(t, sub2)
	natsFlush(t, ncsa2)
	natsPub(t, ncsb2, "foo", []byte("hello"))
	natsFlush(t, ncsb2)
	waitCh(t, ch, "Did not get our message")

	// Unsubscribe sub1, all server's sublist should be empty
	sub1.Unsubscribe()
	natsFlush(t, ncsa1)

	checkExpectedSubs(t, 0, sa1, sa2, sb1, sb2)

	// Create queue subs
	total := 100
	c1 := int32(0)
	c2 := int32(0)
	c3 := int32(0)
	tc := int32(0)
	natsQueueSub(t, ncsa1, "foo", "bar", func(_ *nats.Msg) {
		atomic.AddInt32(&c1, 1)
		if c := atomic.AddInt32(&tc, 1); int(c) == total {
			ch <- true
		}
	})
	natsFlush(t, ncsa1)
	natsQueueSub(t, ncsa2, "foo", "bar", func(_ *nats.Msg) {
		atomic.AddInt32(&c2, 1)
		if c := atomic.AddInt32(&tc, 1); int(c) == total {
			ch <- true
		}
	})
	natsFlush(t, ncsa2)
	checkExpectedSubs(t, 2, sa1, sa2)

	qsubOnB2 := natsQueueSub(t, ncsb2, "foo", "bar", func(_ *nats.Msg) {
		atomic.AddInt32(&c3, 1)
		if c := atomic.AddInt32(&tc, 1); int(c) == total {
			ch <- true
		}
	})
	natsFlush(t, ncsb2)
	checkExpectedSubs(t, 1, sb2)

	checkForRegisteredQSubInterest(t, sb1, "A", globalAccountName, "foo", 1, time.Second)

	// Publish all messages. The queue sub on cluster B should receive all
	// messages.
	for i := 0; i < total; i++ {
		natsPub(t, ncsb2, "foo", []byte("msg"))
	}
	natsFlush(t, ncsb2)

	waitCh(t, ch, "Did not get all our queue messages")
	if n := int(atomic.LoadInt32(&c1)); n != 0 {
		t.Fatalf("No message should have been received by qsub1, got %v", n)
	}
	if n := int(atomic.LoadInt32(&c2)); n != 0 {
		t.Fatalf("No message should have been received by qsub2, got %v", n)
	}
	if n := int(atomic.LoadInt32(&c3)); n != total {
		t.Fatalf("All messages should have been delivered to qsub on B, got %v", n)
	}

	// Reset counters
	atomic.StoreInt32(&c1, 0)
	atomic.StoreInt32(&c2, 0)
	atomic.StoreInt32(&c3, 0)
	atomic.StoreInt32(&tc, 0)

	// Now send from cluster A, messages should be distributed to qsubs on A.
	for i := 0; i < total; i++ {
		natsPub(t, ncsa1, "foo", []byte("msg"))
	}
	natsFlush(t, ncsa1)

	expectedLow := int(float32(total/2) * 0.7)
	expectedHigh := int(float32(total/2) * 1.3)
	checkCount := func(t *testing.T, count *int32) {
		t.Helper()
		c := int(atomic.LoadInt32(count))
		if c < expectedLow || c > expectedHigh {
			t.Fatalf("Expected value to be between %v/%v, got %v", expectedLow, expectedHigh, c)
		}
	}
	waitCh(t, ch, "Did not get all our queue messages")
	checkCount(t, &c1)
	checkCount(t, &c2)

	// Now unsubscribe sub on B and reset counters
	natsUnsub(t, qsubOnB2)
	checkExpectedSubs(t, 0, sb2)
	atomic.StoreInt32(&c1, 0)
	atomic.StoreInt32(&c2, 0)
	atomic.StoreInt32(&c3, 0)
	atomic.StoreInt32(&tc, 0)
	// Publish from cluster B, messages should be delivered to cluster A.
	for i := 0; i < total; i++ {
		natsPub(t, ncsb2, "foo", []byte("msg"))
	}
	natsFlush(t, ncsb2)

	waitCh(t, ch, "Did not get all our queue messages")
	if n := int(atomic.LoadInt32(&c3)); n != 0 {
		t.Fatalf("There should not have been messages on unsubscribed sub, got %v", n)
	}
	checkCount(t, &c1)
	checkCount(t, &c2)
}

func TestGatewayMsgSentOnlyOnce(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	s2Url := fmt.Sprintf("nats://127.0.0.1:%d", o2.Port)
	nc2 := natsConnect(t, s2Url)
	defer nc2.Close()

	s1Url := fmt.Sprintf("nats://127.0.0.1:%d", o1.Port)
	nc1 := natsConnect(t, s1Url)
	defer nc1.Close()

	ch := make(chan bool, 1)
	count := int32(0)
	expected := int32(4)
	cb := func(_ *nats.Msg) {
		if c := atomic.AddInt32(&count, 1); c == expected {
			ch <- true
		}
	}

	// On s1, create 2 plain subs, 2 queue members for group
	// "bar" and 1 for group "baz".
	natsSub(t, nc1, ">", cb)
	natsSub(t, nc1, "foo", cb)
	natsQueueSub(t, nc1, "foo", "bar", cb)
	natsQueueSub(t, nc1, "foo", "bar", cb)
	natsQueueSub(t, nc1, "foo", "baz", cb)
	natsFlush(t, nc1)

	// Ensure subs registered in S1
	checkExpectedSubs(t, 5, s1)

	// Also need to wait for qsubs to be registered on s2.
	checkForRegisteredQSubInterest(t, s2, "A", globalAccountName, "foo", 2, time.Second)

	// From s2, send 1 message, s1 should receive 1 only,
	// and total we should get the callback notified 4 times.
	natsPub(t, nc2, "foo", []byte("hello"))
	natsFlush(t, nc2)

	waitCh(t, ch, "Did not get our messages")
	// Verifiy that count is still 4
	if c := atomic.LoadInt32(&count); c != expected {
		t.Fatalf("Expected %v messages, got %v", expected, c)
	}
	// Check s2 outbound connection stats. It should say that it
	// sent only 1 message.
	c := s2.getOutboundGatewayConnection("A")
	if c == nil {
		t.Fatalf("S2 outbound gateway not found")
	}
	c.mu.Lock()
	out := c.outMsgs
	c.mu.Unlock()
	if out != 1 {
		t.Fatalf("Expected s2's outbound gateway to have sent a single message, got %v", out)
	}
	// Now check s1's inbound gateway
	s1.gateway.RLock()
	c = nil
	for _, ci := range s1.gateway.in {
		c = ci
		break
	}
	s1.gateway.RUnlock()
	if c == nil {
		t.Fatalf("S1 inbound gateway not found")
	}
	c.mu.Lock()
	in := c.inMsgs
	c.mu.Unlock()
	if in != 1 {
		t.Fatalf("Expected s1's inbound gateway to have received a single message, got %v", in)
	}
}

type checkErrorLogger struct {
	DummyLogger
	checkErrorStr string
	gotError      bool
}

func (l *checkErrorLogger) Errorf(format string, args ...interface{}) {
	l.DummyLogger.Errorf(format, args...)
	l.Lock()
	if strings.Contains(l.msg, l.checkErrorStr) {
		l.gotError = true
	}
	l.Unlock()
}

func TestGatewayRoutedServerWithoutGatewayConfigured(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	o3 := DefaultOptions()
	o3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s2.ClusterAddr().Port))
	s3 := New(o3)
	defer s3.Shutdown()
	l := &checkErrorLogger{checkErrorStr: "not configured"}
	s3.SetLogger(l, true, true)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s3.Start()
		wg.Done()
	}()

	checkClusterFormed(t, s2, s3)

	// Check that server s3 does not panic when being notified
	// about the A gateway, but report an error.
	deadline := time.Now().Add(2 * time.Second)
	gotIt := false
	for time.Now().Before(deadline) {
		l.Lock()
		gotIt = l.gotError
		l.Unlock()
		if gotIt {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if !gotIt {
		t.Fatalf("Should have reported error about gateway not configured")
	}

	s3.Shutdown()
	wg.Wait()
}

func TestGatewaySendsToNonLocalSubs(t *testing.T) {
	ob1 := testDefaultOptionsForGateway("B")
	sb1 := runGatewayServer(ob1)
	defer sb1.Shutdown()

	oa1 := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	sa1 := runGatewayServer(oa1)
	defer sa1.Shutdown()

	waitForOutboundGateways(t, sa1, 1, time.Second)
	waitForOutboundGateways(t, sb1, 1, time.Second)

	waitForInboundGateways(t, sa1, 1, time.Second)
	waitForInboundGateways(t, sb1, 1, time.Second)

	oa2 := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	oa2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sa1.ClusterAddr().Port))
	sa2 := runGatewayServer(oa2)
	defer sa2.Shutdown()

	checkClusterFormed(t, sa1, sa2)

	waitForOutboundGateways(t, sa2, 1, time.Second)
	waitForInboundGateways(t, sb1, 2, time.Second)

	ch := make(chan bool, 1)
	// Create an interest of sa2
	ncSub := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", oa2.Port))
	defer ncSub.Close()
	natsSub(t, ncSub, "foo", func(_ *nats.Msg) { ch <- true })
	natsFlush(t, ncSub)
	checkExpectedSubs(t, 1, sa1, sa2)

	// Produce a message from sb1, make sure it can be received.
	ncPub := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", ob1.Port))
	defer ncPub.Close()
	natsPub(t, ncPub, "foo", []byte("hello"))
	waitCh(t, ch, "Did not get our message")

	ncSub.Close()
	ncPub.Close()
	checkExpectedSubs(t, 0, sa1, sa2)

	// Now create sb2 that has a route to sb1 and gateway connects to sa2.
	ob2 := testGatewayOptionsFromToWithServers(t, "B", "A", sa2)
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sb1.ClusterAddr().Port))
	sb2 := runGatewayServer(ob2)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)
	waitForOutboundGateways(t, sb2, 1, time.Second)

	ncSub = natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", oa1.Port))
	defer ncSub.Close()
	natsSub(t, ncSub, "foo", func(_ *nats.Msg) { ch <- true })
	natsFlush(t, ncSub)
	checkExpectedSubs(t, 1, sa1, sa2)

	ncPub = natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", ob2.Port))
	defer ncPub.Close()
	natsPub(t, ncPub, "foo", []byte("hello"))
	waitCh(t, ch, "Did not get our message")
}

func TestGatewayUnknownGatewayCommand(t *testing.T) {
	o1 := testDefaultOptionsForGateway("A")
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	l := &checkErrorLogger{checkErrorStr: "Unknown command"}
	s1.SetLogger(l, true, true)

	o2 := testDefaultOptionsForGateway("A")
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s1.ClusterAddr().Port))
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	var route *client
	s2.mu.Lock()
	for _, r := range s2.routes {
		route = r
		break
	}
	s2.mu.Unlock()

	route.mu.Lock()
	info := &Info{
		Gateway:    "B",
		GatewayCmd: 255,
	}
	b, _ := json.Marshal(info)
	route.sendProto([]byte(fmt.Sprintf(InfoProto, b)), true)
	route.mu.Unlock()

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		l.Lock()
		gotIt := l.gotError
		l.Unlock()
		if gotIt {
			return nil
		}
		return fmt.Errorf("Did not get expected error")
	})
}

func TestGatewayRandomIP(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithURLs(t, "A", "B",
		[]string{
			"nats://noport",
			fmt.Sprintf("nats://localhost:%d", sb.GatewayAddr().Port),
		})
	// Create a dummy resolver that returns error since we
	// don't provide any IP. The code should then use the configured
	// url (localhost:port) and try with that, which in this case
	// should work.
	oa.Gateway.resolver = &myDummyDNSResolver{}
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb, 1, 2*time.Second)
}

func TestGatewaySendQSubsBufSize(t *testing.T) {
	for _, test := range []struct {
		name    string
		bufSize int
	}{
		{
			name:    "Bufsize 45, more than one at a time",
			bufSize: 45,
		},
		{
			name:    "Bufsize 15, one at a time",
			bufSize: 15,
		},
		{
			name:    "Bufsize 0, default to maxBufSize, all at once",
			bufSize: 0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {

			o2 := testDefaultOptionsForGateway("B")
			o2.Gateway.sendQSubsBufSize = test.bufSize
			s2 := runGatewayServer(o2)
			defer s2.Shutdown()

			s2Url := fmt.Sprintf("nats://%s:%d", o2.Host, o2.Port)
			nc := natsConnect(t, s2Url)
			defer nc.Close()
			natsQueueSub(t, nc, "foo", "bar", func(_ *nats.Msg) {})
			natsQueueSub(t, nc, "foo", "baz", func(_ *nats.Msg) {})
			natsQueueSub(t, nc, "foo", "bat", func(_ *nats.Msg) {})
			natsQueueSub(t, nc, "foo", "bax", func(_ *nats.Msg) {})

			checkExpectedSubs(t, 4, s2)

			o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
			s1 := runGatewayServer(o1)
			defer s1.Shutdown()

			waitForOutboundGateways(t, s1, 1, time.Second)
			waitForOutboundGateways(t, s2, 1, time.Second)

			checkForRegisteredQSubInterest(t, s1, "B", globalAccountName, "foo", 4, time.Second)

			// Make sure we have the 4 we expected
			c := s1.getOutboundGatewayConnection("B")
			ei, _ := c.gw.outsim.Load(globalAccountName)
			if ei == nil {
				t.Fatalf("No interest found")
			}
			sl := ei.(*outsie).sl
			r := sl.Match("foo")
			if len(r.qsubs) != 4 {
				t.Fatalf("Expected 4 groups, got %v", len(r.qsubs))
			}
			var gotBar, gotBaz, gotBat, gotBax bool
			for _, qs := range r.qsubs {
				if len(qs) != 1 {
					t.Fatalf("Unexpected number of subs for group %s: %v", qs[0].queue, len(qs))
				}
				q := qs[0].queue
				switch string(q) {
				case "bar":
					gotBar = true
				case "baz":
					gotBaz = true
				case "bat":
					gotBat = true
				case "bax":
					gotBax = true
				default:
					t.Fatalf("Unexpected group name: %s", q)
				}
			}
			if !gotBar || !gotBaz || !gotBat || !gotBax {
				t.Fatalf("Did not get all we wanted: bar=%v baz=%v bat=%v bax=%v",
					gotBar, gotBaz, gotBat, gotBax)
			}

			nc.Close()
			s1.Shutdown()
			s2.Shutdown()

			waitForOutboundGateways(t, s1, 0, time.Second)
			waitForOutboundGateways(t, s2, 0, time.Second)
		})
	}
}

func TestGatewayRaceBetweenPubAndSub(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	s2Url := fmt.Sprintf("nats://127.0.0.1:%d", o2.Port)
	nc2 := natsConnect(t, s2Url)
	defer nc2.Close()

	s1Url := fmt.Sprintf("nats://127.0.0.1:%d", o1.Port)
	var ncaa [5]*nats.Conn
	var nca = ncaa[:0]
	for i := 0; i < 5; i++ {
		nc := natsConnect(t, s1Url)
		defer nc.Close()
		nca = append(nca, nc)
	}

	ch := make(chan bool, 1)
	wg := sync.WaitGroup{}
	wg.Add(5)
	for _, nc := range nca {
		nc := nc
		go func(n *nats.Conn) {
			defer wg.Done()
			for {
				n.Publish("foo", []byte("hello"))
				select {
				case <-ch:
					return
				default:
				}
			}
		}(nc)
	}
	time.Sleep(100 * time.Millisecond)
	natsQueueSub(t, nc2, "foo", "bar", func(m *nats.Msg) {
		natsUnsub(t, m.Sub)
		close(ch)
	})
	wg.Wait()
}

// Returns the first (if any) of the inbound connections for this name.
func getInboundGatewayConnection(s *Server, name string) *client {
	var gwsa [4]*client
	var gws = gwsa[:0]
	s.getInboundGatewayConnections(&gws)
	if len(gws) > 0 {
		return gws[0]
	}
	return nil
}

func TestGatewaySendAllSubs(t *testing.T) {
	gatewayMaxRUnsubBeforeSwitch = 100
	defer func() { gatewayMaxRUnsubBeforeSwitch = defaultGatewayMaxRUnsubBeforeSwitch }()

	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	oc := testGatewayOptionsFromToWithServers(t, "C", "B", sb)
	sc := runGatewayServer(oc)
	defer sc.Shutdown()

	waitForOutboundGateways(t, sa, 2, time.Second)
	waitForOutboundGateways(t, sb, 2, time.Second)
	waitForOutboundGateways(t, sc, 2, time.Second)
	waitForInboundGateways(t, sa, 2, time.Second)
	waitForInboundGateways(t, sb, 2, time.Second)
	waitForInboundGateways(t, sc, 2, time.Second)

	// On B, create a sub that will reply to requests
	bURL := fmt.Sprintf("nats://%s:%d", ob.Host, ob.Port)
	ncB := natsConnect(t, bURL)
	defer ncB.Close()
	natsSub(t, ncB, "foo", func(m *nats.Msg) {
		ncB.Publish(m.Reply, m.Data)
	})
	natsFlush(t, ncB)
	checkExpectedSubs(t, 1, sb)

	// On C, have some delayed activity while it receives
	// unwanted messages and switches to sendAllSubs.
	cURL := fmt.Sprintf("nats://%s:%d", oc.Host, oc.Port)
	ncC := natsConnect(t, cURL)
	defer ncC.Close()
	wg := sync.WaitGroup{}
	wg.Add(2)
	done := make(chan bool)
	consCount := 0
	accsCount := 0
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			// Create subs and qsubs on same subject
			natsSub(t, ncC, fmt.Sprintf("foo.%d", i+1), func(_ *nats.Msg) {})
			natsQueueSub(t, ncC, fmt.Sprintf("foo.%d", i+1), fmt.Sprintf("bar.%d", i+1), func(_ *nats.Msg) {})
			// Create psubs and qsubs on unique subjects
			natsSub(t, ncC, fmt.Sprintf("foox.%d", i+1), func(_ *nats.Msg) {})
			natsQueueSub(t, ncC, fmt.Sprintf("fooy.%d", i+1), fmt.Sprintf("bar.%d", i+1), func(_ *nats.Msg) {})
			consCount += 4
			// Register account
			sc.RegisterAccount(fmt.Sprintf("acc.%d", i+1))
			accsCount++
			select {
			case <-done:
				return
			case <-time.After(15 * time.Millisecond):
			}
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		for {
			for i := 0; i < 10; i++ {
				natsPub(t, ncB, fmt.Sprintf("foo.%d", i+1), []byte("hello"))
			}
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()

	// From A, send a lot of requests.
	aURL := fmt.Sprintf("nats://%s:%d", oa.Host, oa.Port)
	ncA := natsConnect(t, aURL)
	defer ncA.Close()
	// TODO(ik): May need to change that if we change the threshold
	// for when the switch happens.
	total := 300
	for i := 0; i < total; i++ {
		req := fmt.Sprintf("%d", i)
		reply, err := ncA.Request("foo", []byte(req), time.Second)
		if err != nil {
			t.Fatalf("Error waiting for reply: %v", err)
		}
		if string(reply.Data) != req {
			t.Fatalf("Expected reply %q, got %q", req, reply.Data)
		}
	}
	close(done)

	// Normally, C would receive a message for each req inbox and
	// would send and RS- on that to B, making both have an unbounded
	// growth of the no-interest map. But after a certain amount
	// of RS-, C will send all its sub for the given account and
	// instruct B to send only if there is explicit interest.
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		// Check C inbound connection from B
		c := getInboundGatewayConnection(sc, "B")
		c.mu.Lock()
		var switchedMode bool
		e := c.gw.insim[globalAccountName]
		if e != nil {
			switchedMode = e.ni == nil && e.mode == modeInterestOnly
		}
		c.mu.Unlock()
		if !switchedMode {
			return fmt.Errorf("C has still not switched mode")
		}
		switchedMode = false
		// Now check B outbound connection to C
		c = sb.getOutboundGatewayConnection("C")
		ei, _ := c.gw.outsim.Load(globalAccountName)
		if ei != nil {
			e := ei.(*outsie)
			e.RLock()
			switchedMode = e.ni == nil && e.mode == modeInterestOnly
			e.RUnlock()
		}
		if !switchedMode {
			return fmt.Errorf("C has still not switched mode")
		}
		return nil
	})
	wg.Wait()

	// Check consCount and accsCount on C
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		sc.gateway.pasi.Lock()
		scount := len(sc.gateway.pasi.m[globalAccountName])
		sc.gateway.pasi.Unlock()
		if scount != consCount {
			return fmt.Errorf("Expected %v consumers for global account, got %v", consCount, scount)
		}
		sc.mu.Lock()
		acount := len(sc.accounts)
		sc.mu.Unlock()
		if acount != accsCount+1 {
			return fmt.Errorf("Expected %v accounts, got %v", accsCount+1, acount)
		}
		return nil
	})

	for i := 0; i < total; i++ {
		req := fmt.Sprintf("%d", i)
		reply, err := ncA.Request("foo", []byte(req), time.Second)
		if err != nil {
			t.Fatalf("Error waiting for reply: %v", err)
		}
		if string(reply.Data) != req {
			t.Fatalf("Expected reply %q, got %q", req, reply.Data)
		}
	}

	// Also, after all that, if a sub is created on C, it should
	// be sent to B (but not A). Check that this is the case.
	// So first send from A on the subject that we are going to
	// use for this new sub.
	natsPub(t, ncA, "newsub", []byte("hello"))
	natsFlush(t, ncA)
	aOutboundToC := sa.getOutboundGatewayConnection("C")
	checkForNoInterest(t, aOutboundToC, globalAccountName, "newsub", true, 2*time.Second)

	newSubSub := natsSub(t, ncC, "newsub", func(_ *nats.Msg) {})
	natsFlush(t, ncC)
	checkExpectedSubs(t, consCount+1)
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		c := sb.getOutboundGatewayConnection("C")
		ei, _ := c.gw.outsim.Load(globalAccountName)
		if ei != nil {
			sl := ei.(*outsie).sl
			r := sl.Match("newsub")
			if len(r.psubs) == 1 {
				return nil
			}
		}
		return fmt.Errorf("Newsub not registered on B")
	})
	checkForNoInterest(t, aOutboundToC, globalAccountName, "newsub", false, 2*time.Second)

	natsUnsub(t, newSubSub)
	natsFlush(t, ncC)
	checkExpectedSubs(t, consCount)
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		c := sb.getOutboundGatewayConnection("C")
		ei, _ := c.gw.outsim.Load(globalAccountName)
		if ei != nil {
			sl := ei.(*outsie).sl
			r := sl.Match("newsub")
			if len(r.psubs) == 0 {
				return nil
			}
		}
		return fmt.Errorf("Newsub still registered on B")
	})
}

func TestGatewaySendAllSubsBadProtocol(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	// For this test, make sure to use inbound from A so
	// A will reconnect when we send bad proto that
	// causes connection to be closed.
	c := getInboundGatewayConnection(sa, "A")
	// Mock an invalid protocol (account name missing)
	info := &Info{
		Gateway:    "B",
		GatewayCmd: gatewayCmdAllSubsStart,
	}
	b, _ := json.Marshal(info)
	c.mu.Lock()
	c.sendProto([]byte(fmt.Sprintf("INFO %s\r\n", b)), true)
	c.mu.Unlock()

	orgConn := c
	checkFor(t, 3*time.Second, 100*time.Millisecond, func() error {
		curConn := getInboundGatewayConnection(sa, "A")
		if orgConn == curConn {
			return fmt.Errorf("Not reconnected")
		}
		return nil
	})

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb, 1, 2*time.Second)

	// Refresh
	c = nil
	checkFor(t, 3*time.Second, 15*time.Millisecond, func() error {
		c = getInboundGatewayConnection(sa, "A")
		if c == nil {
			t.Fatalf("Did not reconnect")
		}
		return nil
	})
	// Do correct start
	info.GatewayCmdPayload = []byte(globalAccountName)
	b, _ = json.Marshal(info)
	c.mu.Lock()
	c.sendProto([]byte(fmt.Sprintf("INFO %s\r\n", b)), true)
	c.mu.Unlock()
	// But incorrect end.
	info.GatewayCmd = gatewayCmdAllSubsComplete
	info.GatewayCmdPayload = nil
	b, _ = json.Marshal(info)
	c.mu.Lock()
	c.sendProto([]byte(fmt.Sprintf("INFO %s\r\n", b)), true)
	c.mu.Unlock()

	orgConn = c
	checkFor(t, 3*time.Second, 100*time.Millisecond, func() error {
		curConn := getInboundGatewayConnection(sa, "A")
		if orgConn == curConn {
			return fmt.Errorf("Not reconnected")
		}
		return nil
	})
}

func TestGatewayRaceOnClose(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	bURL := fmt.Sprintf("nats://%s:%d", ob.Host, ob.Port)
	ncB := natsConnect(t, bURL, nats.NoReconnect())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		cb := func(_ *nats.Msg) {}
		for {
			// Expect failure at one point and just return.
			qsub, err := ncB.QueueSubscribe("foo", "bar", cb)
			if err != nil {
				return
			}
			if err := qsub.Unsubscribe(); err != nil {
				return
			}
		}
	}()
	// Wait a bit and kill B
	time.Sleep(200 * time.Millisecond)
	sb.Shutdown()
	wg.Wait()
}

// Similar to TestNewRoutesServiceImport but with 2 GW servers instead
// of a cluster of 2 servers.
func TestGatewayServiceImport(t *testing.T) {
	oa := testDefaultOptionsForGateway("A")
	setAccountUserPassInOptions(oa, "$foo", "clientA", "password")
	setAccountUserPassInOptions(oa, "$bar", "yyyyyyy", "password")
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	ob := testGatewayOptionsFromToWithServers(t, "B", "A", sa)
	setAccountUserPassInOptions(ob, "$foo", "xxxxxxx", "password")
	setAccountUserPassInOptions(ob, "$bar", "clientB", "password")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	// Get accounts
	fooA, _ := sa.LookupAccount("$foo")
	barA, _ := sa.LookupAccount("$bar")
	fooB, _ := sb.LookupAccount("$foo")
	barB, _ := sb.LookupAccount("$bar")

	// Add in the service export for the requests. Make it public.
	fooA.AddServiceExport("test.request", nil)
	fooB.AddServiceExport("test.request", nil)

	// Add import abilities to server B's bar account from foo.
	if err := barB.AddServiceImport(fooB, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}
	// Same on A.
	if err := barA.AddServiceImport(fooA, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}

	// clientA will be connected to srvA and be the service endpoint and responder.
	aURL := fmt.Sprintf("nats://clientA:password@127.0.0.1:%d", oa.Port)
	clientA := natsConnect(t, aURL)
	defer clientA.Close()

	subA := natsSubSync(t, clientA, "test.request")
	natsFlush(t, clientA)

	// Now setup client B on srvB who will do a sub from account $bar
	// that should map account $foo's foo subject.
	bURL := fmt.Sprintf("nats://clientB:password@127.0.0.1:%d", ob.Port)
	clientB := natsConnect(t, bURL)
	defer clientB.Close()

	subB := natsSubSync(t, clientB, "reply")
	natsFlush(t, clientB)

	for i := 0; i < 1; i++ {
		// Send the request from clientB on foo.request,
		natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
		natsFlush(t, clientB)

		// Expect the request on A
		msg, err := subA.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("subA failed to get request: %v", err)
		}
		if msg.Subject != "test.request" || string(msg.Data) != "hi" {
			t.Fatalf("Unexpected message: %v", msg)
		}
		if msg.Reply == "reply" {
			t.Fatalf("Expected randomized reply, but got original")
		}

		// Send reply
		natsPub(t, clientA, msg.Reply, []byte("ok"))
		natsFlush(t, clientA)

		msg, err = subB.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("subB failed to get reply: %v", err)
		}
		if msg.Subject != "reply" || string(msg.Data) != "ok" {
			t.Fatalf("Unexpected message: %v", msg)
		}

		expected := int64((i + 1) * 3)
		vz, _ := sa.Varz(nil)
		if vz.OutMsgs != expected {
			t.Fatalf("Expected %d outMsgs for A, got %v", expected, vz.OutMsgs)
		}

		if i == 0 {
			expected = 3
		} else {
			expected = 5
		}
		vz, _ = sb.Varz(nil)
		if vz.OutMsgs != expected {
			t.Fatalf("Expected %d outMsgs for B, got %v", expected, vz.OutMsgs)
		}
	}

	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if ts := fooA.TotalSubs(); ts != 1 {
			return fmt.Errorf("Expected one sub to be left on fooA, but got %d", ts)
		}
		return nil
	})

	// Speed up exiration
	fooA.SetAutoExpireTTL(10 * time.Millisecond)

	// Send 100 requests from clientB on foo.request,
	for i := 0; i < 100; i++ {
		natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
	}
	natsFlush(t, clientB)

	// Consume the requests, but don't reply to them...
	for i := 0; i < 100; i++ {
		if _, err := subA.NextMsg(time.Second); err != nil {
			t.Fatalf("subA did not receive request: %v", err)
		}
	}

	// These reply subjects will be dangling off of $foo account on serverA.
	// Remove our service endpoint and wait for the dangling replies to go to zero.
	natsUnsub(t, subA)
	natsFlush(t, clientA)

	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if ts := fooA.TotalSubs(); ts != 0 {
			return fmt.Errorf("Number of subs is %d, should be zero", ts)
		}
		return nil
	})

	// Repeat similar test but without the small TTL and verify
	// that if B is shutdown, the dangling subs for replies are
	// cleared from the account sublist.
	fooA.SetAutoExpireTTL(10 * time.Second)

	subA = natsSubSync(t, clientA, "test.request")
	natsFlush(t, clientA)

	// Send 100 requests from clientB on foo.request,
	for i := 0; i < 100; i++ {
		natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
	}
	natsFlush(t, clientB)

	// Consume the requests, but don't reply to them...
	for i := 0; i < 100; i++ {
		if _, err := subA.NextMsg(time.Second); err != nil {
			t.Fatalf("subA did not receive request: %v", err)
		}
	}

	// Shutdown B
	clientB.Close()
	sb.Shutdown()

	// Close our last sub
	natsUnsub(t, subA)
	natsFlush(t, clientA)

	// Verify that they are gone before the 10 sec TTL
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if ts := fooA.TotalSubs(); ts != 0 {
			return fmt.Errorf("Number of subs is %d, should be zero", ts)
		}
		return nil
	})
}

func TestGatewayServiceImportWithQueue(t *testing.T) {
	oa := testDefaultOptionsForGateway("A")
	setAccountUserPassInOptions(oa, "$foo", "clientA", "password")
	setAccountUserPassInOptions(oa, "$bar", "yyyyyyy", "password")
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	ob := testGatewayOptionsFromToWithServers(t, "B", "A", sa)
	setAccountUserPassInOptions(ob, "$foo", "xxxxxxx", "password")
	setAccountUserPassInOptions(ob, "$bar", "clientB", "password")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	// Get accounts
	fooA, _ := sa.LookupAccount("$foo")
	barA, _ := sa.LookupAccount("$bar")
	fooB, _ := sb.LookupAccount("$foo")
	barB, _ := sb.LookupAccount("$bar")

	// Add in the service export for the requests. Make it public.
	fooA.AddServiceExport("test.request", nil)
	fooB.AddServiceExport("test.request", nil)

	// Add import abilities to server B's bar account from foo.
	if err := barB.AddServiceImport(fooB, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}
	// Same on A.
	if err := barA.AddServiceImport(fooA, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}

	// clientA will be connected to srvA and be the service endpoint and responder.
	aURL := fmt.Sprintf("nats://clientA:password@127.0.0.1:%d", oa.Port)
	clientA := natsConnect(t, aURL)
	defer clientA.Close()

	subA := natsQueueSubSync(t, clientA, "test.request", "queue")
	natsFlush(t, clientA)

	// Now setup client B on srvB who will do a sub from account $bar
	// that should map account $foo's foo subject.
	bURL := fmt.Sprintf("nats://clientB:password@127.0.0.1:%d", ob.Port)
	clientB := natsConnect(t, bURL)
	defer clientB.Close()

	subB := natsQueueSubSync(t, clientB, "reply", "queue2")
	natsFlush(t, clientB)

	// Wait for queue interest on test.request from A to be registered
	// on server B.
	checkForRegisteredQSubInterest(t, sb, "A", "$foo", "test.request", 1, time.Second)

	for i := 0; i < 1; i++ {
		// Send the request from clientB on foo.request,
		natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
		natsFlush(t, clientB)

		// Expect the request on A
		msg, err := subA.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("subA failed to get request: %v", err)
		}
		if msg.Subject != "test.request" || string(msg.Data) != "hi" {
			t.Fatalf("Unexpected message: %v", msg)
		}
		if msg.Reply == "reply" {
			t.Fatalf("Expected randomized reply, but got original")
		}

		// Send reply
		natsPub(t, clientA, msg.Reply, []byte("ok"))
		natsFlush(t, clientA)

		msg, err = subB.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("subB failed to get reply: %v", err)
		}
		if msg.Subject != "reply" || string(msg.Data) != "ok" {
			t.Fatalf("Unexpected message: %v", msg)
		}

		expected := int64(i + 3)
		vz, _ := sa.Varz(nil)
		if vz.OutMsgs != expected {
			t.Fatalf("Expected %d outMsgs for A, got %v", expected, vz.OutMsgs)
		}

		if i == 0 {
			expected = 3
		} else {
			expected = 4
		}
		vz, _ = sb.Varz(nil)
		if vz.OutMsgs != expected {
			t.Fatalf("Expected %d outMsgs for B, got %v", expected, vz.OutMsgs)
		}
	}

	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if ts := fooA.TotalSubs(); ts != 1 {
			return fmt.Errorf("Expected one sub to be left on fooA, but got %d", ts)
		}
		return nil
	})

	// Speed up exiration
	fooA.SetAutoExpireTTL(10 * time.Millisecond)

	// Send 100 requests from clientB on foo.request,
	for i := 0; i < 100; i++ {
		natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
	}
	natsFlush(t, clientB)

	// Consume the requests, but don't reply to them...
	for i := 0; i < 100; i++ {
		if _, err := subA.NextMsg(time.Second); err != nil {
			t.Fatalf("subA did not receive request: %v", err)
		}
	}

	// These reply subjects will be dangling off of $foo account on serverA.
	// Remove our service endpoint and wait for the dangling replies to go to zero.
	natsUnsub(t, subA)
	natsFlush(t, clientA)

	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if ts := fooA.TotalSubs(); ts != 0 {
			return fmt.Errorf("Number of subs is %d, should be zero", ts)
		}
		return nil
	})

	// Repeat similar test but without the small TTL and verify
	// that if B is shutdown, the dangling subs for replies are
	// cleared from the account sublist.
	fooA.SetAutoExpireTTL(10 * time.Second)

	subA = natsQueueSubSync(t, clientA, "test.request", "queue")
	natsFlush(t, clientA)

	// Send 100 requests from clientB on foo.request,
	for i := 0; i < 100; i++ {
		natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
	}
	natsFlush(t, clientB)

	// Consume the requests, but don't reply to them...
	for i := 0; i < 100; i++ {
		if _, err := subA.NextMsg(time.Second); err != nil {
			t.Fatalf("subA did not receive request: %v", err)
		}
	}

	// Shutdown B
	clientB.Close()
	sb.Shutdown()

	// Close our last sub
	natsUnsub(t, subA)
	natsFlush(t, clientA)

	// Verify that they are gone before the 10 sec TTL
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if ts := fooA.TotalSubs(); ts != 0 {
			return fmt.Errorf("Number of subs is %d, should be zero", ts)
		}
		return nil
	})
}

/*
func TestGatewayPermissions(t *testing.T) {
	bo := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(bo)
	defer sb.Shutdown()

	ao := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	// test setup by default sets import and export to ">".
	// For this test, we override.
	ao.Gateway.Permissions.Import = &SubjectPermission{Allow: []string{"foo"}}
	ao.Gateway.Permissions.Export = &SubjectPermission{Allow: []string{"bar"}}
	sa := runGatewayServer(ao)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)

	// Create client connections, one of cluster A, one on B.
	nca := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", ao.Port))
	defer nca.Close()
	ncb := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", bo.Port))
	defer ncb.Close()

	ch := make(chan bool, 1)
	cb := func(m *nats.Msg) {
		ch <- true
	}

	// Check import permissions...

	// Create a local sub on "foo" on Cluster A.
	natsSub(t, nca, "foo", cb)
	natsFlush(t, nca)

	// Message should be received

	natsPub(t, ncb, "foo", []byte("message from B to A on foo"))
	natsFlush(t, ncb)

	waitCh(t, ch, "Did not get message on foo")

	// Create a sub on "baz" on Cluster A, no message should be received on that one
	natsSub(t, nca, "baz", cb)
	natsFlush(t, nca)

	natsPub(t, ncb, "baz", []byte("message from B to A on baz"))
	natsFlush(t, ncb)

	select {
	case <-ch:
		t.Fatalf("Message should not have been received")
	case <-time.After(250 * time.Millisecond):
		// still no message, we are ok.
	}

	// Check export permissions now...

	// Create sub on "bar" on Cluster B
	natsSub(t, ncb, "bar", cb)
	natsFlush(t, ncb)

	// Send from A, that should be possible so message should be received.
	natsPub(t, nca, "bar", []byte("message from A to B on bar"))
	natsFlush(t, nca)

	waitCh(t, ch, "Did not get message on bar")

	// Create a sub on "bozo" on Cluster B
	natsSub(t, ncb, "bozo", cb)
	natsFlush(t, ncb)

	// That message should not be received
	natsPub(t, nca, "bozo", []byte("message from A to B on bozo"))
	natsFlush(t, nca)

	select {
	case <-ch:
		t.Fatalf("Message should not have been received")
	case <-time.After(250 * time.Millisecond):
		// still no message, we are ok.
	}
}

func TestGatewayDefaultPermissions(t *testing.T) {
	bo := testDefaultOptionsForGateway("B")
	// test setup by default sets import and export to ">".
	// For this test, we override.
	bo.Gateway.DefaultPermissions.Import = &SubjectPermission{Allow: []string{"foo"}}
	bo.Gateway.DefaultPermissions.Export = &SubjectPermission{Allow: []string{"bar", "baz"}}
	sb := runGatewayServer(bo)
	defer sb.Shutdown()

	ao := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(ao)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)

	// Check permissions on cluster B. Since there was no
	// explicit gateway defined to A, when server on B accepted
	// the gateway connection, it should have "inherited" the
	// default permissions.
	gw := sb.getRemoteGateway("A")
	if gw == nil {
		t.Fatalf("There should be a remote gateway for A")
	}
	gw.RLock()
	impc := gw.imports.Count()
	expc := gw.exports.Count()
	gw.RUnlock()

	if impc != 1 {
		t.Fatalf("Expected import sublist to be size 1, got %v", impc)
	}
	if expc != 2 {
		t.Fatalf("Expected export sublist to be size 2, got %v", expc)
	}

	// Check server on Cluster A too. By default, tests create
	// gateway permissions and remote gateways default permissions
	// for import/export to be ">".
	gw = sa.getRemoteGateway("B")
	if gw == nil {
		t.Fatalf("There should be a remote gateway for B")
	}
	gw.RLock()
	impc = gw.imports.Count()
	expc = gw.exports.Count()
	gw.RUnlock()
	if impc != 1 {
		t.Fatalf("Expected import sublist to be size 1, got %v", impc)
	}
	if expc != 1 {
		t.Fatalf("Expected export sublist to be size 1, got %v", expc)
	}
}
*/
