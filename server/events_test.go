// Copyright 2018-2019 The NATS Authors
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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func createAccount(s *Server) (*Account, nkeys.KeyPair) {
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	jwt, _ := nac.Encode(okp)
	addAccountToMemResolver(s, pub, jwt)
	acc, err := s.LookupAccount(pub)
	if err != nil {
		panic(err)
	}
	return acc, akp
}

func createUserCreds(t *testing.T, s *Server, akp nkeys.KeyPair) nats.Option {
	t.Helper()
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	nuc := jwt.NewUserClaims(pub)
	ujwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	userCB := func() (string, error) {
		return ujwt, nil
	}
	sigCB := func(nonce []byte) ([]byte, error) {
		sig, _ := kp.Sign(nonce)
		return sig, nil
	}
	return nats.UserJWT(userCB, sigCB)
}

func runTrustedServer(t *testing.T) (*Server, *Options) {
	t.Helper()
	opts := DefaultOptions()
	kp, _ := nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()
	opts.TrustedKeys = []string{pub}
	opts.AccountResolver = &MemAccResolver{}
	s := RunServer(opts)
	return s, opts
}

func runTrustedCluster(t *testing.T) (*Server, *Options, *Server, *Options, nkeys.KeyPair) {
	t.Helper()

	kp, _ := nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()

	mr := &MemAccResolver{}

	// Now create a system account.
	// NOTE: This can NOT be shared directly between servers.
	// Set via server options.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	jwt, _ := nac.Encode(okp)

	mr.Store(apub, jwt)

	optsA := DefaultOptions()
	optsA.Cluster.Host = "127.0.0.1"
	optsA.TrustedKeys = []string{pub}
	optsA.AccountResolver = mr
	optsA.SystemAccount = apub
	optsA.ServerName = "A"
	// Add in dummy gateway
	optsA.Gateway.Name = "TEST CLUSTER 22"
	optsA.Gateway.Host = "127.0.0.1"
	optsA.Gateway.Port = -1
	optsA.gatewaysSolicitDelay = 30 * time.Second

	sa := RunServer(optsA)

	optsB := nextServerOpts(optsA)
	optsB.ServerName = "B"
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host, optsA.Cluster.Port))
	sb := RunServer(optsB)

	checkClusterFormed(t, sa, sb)

	return sa, optsA, sb, optsB, akp
}

func runTrustedGateways(t *testing.T) (*Server, *Options, *Server, *Options, nkeys.KeyPair) {
	t.Helper()

	kp, _ := nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()

	mr := &MemAccResolver{}

	// Now create a system account.
	// NOTE: This can NOT be shared directly between servers.
	// Set via server options.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	jwt, _ := nac.Encode(okp)

	mr.Store(apub, jwt)

	optsA := testDefaultOptionsForGateway("A")
	optsA.Cluster.Host = "127.0.0.1"
	optsA.TrustedKeys = []string{pub}
	optsA.AccountResolver = mr
	optsA.SystemAccount = apub

	sa := RunServer(optsA)

	optsB := testGatewayOptionsFromToWithServers(t, "B", "A", sa)
	optsB.TrustedKeys = []string{pub}
	optsB.AccountResolver = mr
	optsB.SystemAccount = apub

	sb := RunServer(optsB)

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)

	return sa, optsA, sb, optsB, akp
}

func TestSystemAccount(t *testing.T) {
	s, _ := runTrustedServer(t)
	defer s.Shutdown()

	acc, _ := createAccount(s)
	s.setSystemAccount(acc)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sys == nil || s.sys.account == nil {
		t.Fatalf("Expected sys.account to be non-nil")
	}
	if s.sys.client == nil {
		t.Fatalf("Expected sys.client to be non-nil")
	}
	if s.sys.client.echo {
		t.Fatalf("Internal clients should always have echo false")
	}
}

func TestSystemAccountNewConnection(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	acc, akp := createAccount(s)
	s.setSystemAccount(acc)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	ncs, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncs.Close()

	// We may not be able to hear ourselves (if the event is processed
	// before we create the sub), so we need to create a second client to
	// trigger the connect/disconnect events.
	acc2, akp2 := createAccount(s)

	// Be explicit to only receive the event for acc2.
	sub, _ := ncs.SubscribeSync(fmt.Sprintf("$SYS.ACCOUNT.%s.>", acc2.Name))
	defer sub.Unsubscribe()
	ncs.Flush()

	nc, err := nats.Connect(url, createUserCreds(t, s, akp2), nats.Name("TEST EVENTS"))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}

	if !strings.HasPrefix(msg.Subject, fmt.Sprintf("$SYS.ACCOUNT.%s.CONNECT", acc2.Name)) {
		t.Fatalf("Expected subject to start with %q, got %q", "$SYS.ACCOUNT.<account>.CONNECT", msg.Subject)
	}
	tokens := strings.Split(msg.Subject, ".")
	if len(tokens) < 4 {
		t.Fatalf("Expected 4 tokens, got %d", len(tokens))
	}
	account := tokens[2]
	if account != acc2.Name {
		t.Fatalf("Expected %q for account, got %q", acc2.Name, account)
	}

	cem := ConnectEventMsg{}
	if err := json.Unmarshal(msg.Data, &cem); err != nil {
		t.Fatalf("Error unmarshalling connect event message: %v", err)
	}
	if cem.Server.ID != s.ID() {
		t.Fatalf("Expected server to be %q, got %q", s.ID(), cem.Server)
	}
	if cem.Server.Seq == 0 {
		t.Fatalf("Expected sequence to be non-zero")
	}
	if cem.Client.Name != "TEST EVENTS" {
		t.Fatalf("Expected client name to be %q, got %q", "TEST EVENTS", cem.Client.Name)
	}
	if cem.Client.Lang != "go" {
		t.Fatalf("Expected client lang to be \"go\", got %q", cem.Client.Lang)
	}

	// Now close the other client. Should fire a disconnect event.
	// First send and receive some messages.
	sub2, _ := nc.SubscribeSync("foo")
	defer sub2.Unsubscribe()
	sub3, _ := nc.SubscribeSync("*")
	defer sub3.Unsubscribe()

	for i := 0; i < 10; i++ {
		nc.Publish("foo", []byte("HELLO WORLD"))
	}
	nc.Flush()
	nc.Close()

	msg, err = sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}

	if !strings.HasPrefix(msg.Subject, fmt.Sprintf("$SYS.ACCOUNT.%s.DISCONNECT", acc2.Name)) {
		t.Fatalf("Expected subject to start with %q, got %q", "$SYS.ACCOUNT.<account>.DISCONNECT", msg.Subject)
	}
	tokens = strings.Split(msg.Subject, ".")
	if len(tokens) < 4 {
		t.Fatalf("Expected 4 tokens, got %d", len(tokens))
	}
	account = tokens[2]
	if account != acc2.Name {
		t.Fatalf("Expected %q for account, got %q", acc2.Name, account)
	}

	dem := DisconnectEventMsg{}
	if err := json.Unmarshal(msg.Data, &dem); err != nil {
		t.Fatalf("Error unmarshalling disconnect event message: %v", err)
	}

	if dem.Server.ID != s.ID() {
		t.Fatalf("Expected server to be %q, got %q", s.ID(), dem.Server)
	}
	if dem.Server.Seq == 0 {
		t.Fatalf("Expected sequence to be non-zero")
	}
	if dem.Server.Seq <= cem.Server.Seq {
		t.Fatalf("Expected sequence to be increasing")
	}

	if cem.Client.Name != "TEST EVENTS" {
		t.Fatalf("Expected client name to be %q, got %q", "TEST EVENTS", dem.Client.Name)
	}
	if dem.Client.Lang != "go" {
		t.Fatalf("Expected client lang to be \"go\", got %q", dem.Client.Lang)
	}

	if dem.Sent.Msgs != 10 {
		t.Fatalf("Expected 10 msgs sent, got %d", dem.Sent.Msgs)
	}
	if dem.Sent.Bytes != 110 {
		t.Fatalf("Expected 110 bytes sent, got %d", dem.Sent.Bytes)
	}
	if dem.Received.Msgs != 20 {
		t.Fatalf("Expected 20 msgs received, got %d", dem.Sent.Msgs)
	}
	if dem.Received.Bytes != 220 {
		t.Fatalf("Expected 220 bytes sent, got %d", dem.Sent.Bytes)
	}
}

func runTrustedLeafServer(t *testing.T) (*Server, *Options) {
	t.Helper()
	opts := DefaultOptions()
	kp, _ := nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()
	opts.TrustedKeys = []string{pub}
	opts.AccountResolver = &MemAccResolver{}
	opts.LeafNode.Port = -1
	s := RunServer(opts)
	return s, opts
}

func genCredsFile(t *testing.T, jwt string, seed []byte) string {
	creds := `
		-----BEGIN NATS USER JWT-----
		%s
		------END NATS USER JWT------

		************************* IMPORTANT *************************
		NKEY Seed printed below can be used to sign and prove identity.
		NKEYs are sensitive and should be treated as secrets.

		-----BEGIN USER NKEY SEED-----
		%s
		------END USER NKEY SEED------

		*************************************************************
		`
	return createConfFile(t, []byte(strings.Replace(fmt.Sprintf(creds, jwt, seed), "\t\t", "", -1)))
}

func runSolicitWithCredentials(t *testing.T, opts *Options, creds string) (*Server, *Options, string) {
	content := `
		port: -1
		leafnodes {
			remotes = [
				{
					url: nats-leaf://127.0.0.1:%d
					credentials: "%s"
				}
			]
		}
		`
	config := fmt.Sprintf(content, opts.LeafNode.Port, creds)
	conf := createConfFile(t, []byte(config))
	s, opts := RunServerWithConfig(conf)
	return s, opts, conf
}

// Helper function to check that a leaf node has connected to our server.
func checkLeafNodeConnected(t *testing.T, s *Server) {
	t.Helper()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if nln := s.NumLeafNodes(); nln != 1 {
			return fmt.Errorf("Expected a connected leafnode for server %q, got none", s.ID())
		}
		return nil
	})
}

func TestSystemAccountingWithLeafNodes(t *testing.T) {
	s, opts := runTrustedLeafServer(t)
	defer s.Shutdown()

	acc, akp := createAccount(s)
	s.setSystemAccount(acc)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	ncs, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncs.Close()

	acc2, akp2 := createAccount(s)

	// Be explicit to only receive the event for global account.
	sub, _ := ncs.SubscribeSync(fmt.Sprintf("$SYS.ACCOUNT.%s.DISCONNECT", acc2.Name))
	defer sub.Unsubscribe()
	ncs.Flush()

	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	nuc := jwt.NewUserClaims(pub)
	ujwt, err := nuc.Encode(akp2)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	seed, _ := kp.Seed()
	mycreds := genCredsFile(t, ujwt, seed)
	defer os.Remove(mycreds)

	// Create a server that solicits a leafnode connection.
	sl, slopts, lnconf := runSolicitWithCredentials(t, opts, mycreds)
	defer os.Remove(lnconf)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)

	nc, err := nats.Connect(url, createUserCreds(t, s, akp2), nats.Name("TEST LEAFNODE EVENTS"))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	nc.SubscribeSync("foo")
	nc.Flush()

	surl := fmt.Sprintf("nats://%s:%d", slopts.Host, slopts.Port)
	nc2, err := nats.Connect(surl, nats.Name("TEST LEAFNODE EVENTS"))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	m := []byte("HELLO WORLD")

	// Now generate some traffic
	nc2.SubscribeSync("*")
	for i := 0; i < 10; i++ {
		nc2.Publish("foo", m)
		nc2.Publish("bar", m)
	}
	nc2.Flush()

	// Now send some from the cluster side too.
	for i := 0; i < 10; i++ {
		nc.Publish("foo", m)
		nc.Publish("bar", m)
	}
	nc.Flush()

	// Now shutdown the leafnode server since this is where the event tracking should
	// happen. Right now we do not track local clients to the leafnode server that
	// solicited to the cluster, but we should track usage once the leafnode connection stops.
	sl.Shutdown()

	// Make sure we get disconnect event and that tracking is correct.
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}

	dem := DisconnectEventMsg{}
	if err := json.Unmarshal(msg.Data, &dem); err != nil {
		t.Fatalf("Error unmarshalling disconnect event message: %v", err)
	}
	if dem.Sent.Msgs != 10 {
		t.Fatalf("Expected 10 msgs sent, got %d", dem.Sent.Msgs)
	}
	if dem.Sent.Bytes != 110 {
		t.Fatalf("Expected 110 bytes sent, got %d", dem.Sent.Bytes)
	}
	if dem.Received.Msgs != 20 {
		t.Fatalf("Expected 20 msgs received, got %d", dem.Sent.Msgs)
	}
	if dem.Received.Bytes != 220 {
		t.Fatalf("Expected 220 bytes sent, got %d", dem.Sent.Bytes)
	}
}

func TestSystemAccountDisconnectBadLogin(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	acc, akp := createAccount(s)
	s.setSystemAccount(acc)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	ncs, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncs.Close()

	// We should never hear $G account events for bad logins.
	sub, _ := ncs.SubscribeSync("$SYS.ACCOUNT.$G.*")
	defer sub.Unsubscribe()

	// Listen for auth error events though.
	asub, _ := ncs.SubscribeSync("$SYS.SERVER.*.CLIENT.AUTH.ERR")
	defer asub.Unsubscribe()

	ncs.Flush()

	nats.Connect(url, nats.Name("TEST BAD LOGIN"))

	// Should not hear these.
	if _, err := sub.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatalf("Received a disconnect message from bad login, expected none")
	}

	m, err := asub.NextMsg(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("Should have heard an auth error event")
	}
	dem := DisconnectEventMsg{}
	if err := json.Unmarshal(m.Data, &dem); err != nil {
		t.Fatalf("Error unmarshalling disconnect event message: %v", err)
	}
	if dem.Reason != "Authentication Failure" {
		t.Fatalf("Expected auth error, got %q", dem.Reason)
	}
}

func TestSystemAccountInternalSubscriptions(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	sub, err := s.sysSubscribe("foo", nil)
	if sub != nil || err != ErrNoSysAccount {
		t.Fatalf("Expected to get proper error, got %v", err)
	}

	acc, akp := createAccount(s)
	s.setSystemAccount(acc)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	nc, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	sub, err = s.sysSubscribe("foo", nil)
	if sub != nil || err == nil {
		t.Fatalf("Expected to get error for no handler, got %v", err)
	}

	received := make(chan *nats.Msg)
	// Create message callback handler.
	cb := func(sub *subscription, _ *client, subject, reply string, msg []byte) {
		copy := append([]byte(nil), msg...)
		received <- &nats.Msg{Subject: subject, Reply: reply, Data: copy}
	}

	// Now create an internal subscription
	sub, err = s.sysSubscribe("foo", cb)
	if sub == nil || err != nil {
		t.Fatalf("Expected to subscribe, got %v", err)
	}
	// Now send out a message from our normal client.
	nc.Publish("foo", []byte("HELLO WORLD"))

	var msg *nats.Msg

	select {
	case msg = <-received:
		if msg.Subject != "foo" {
			t.Fatalf("Expected \"foo\" as subject, got %q", msg.Subject)
		}
		if msg.Reply != "" {
			t.Fatalf("Expected no reply, got %q", msg.Reply)
		}
		if !bytes.Equal(msg.Data, []byte("HELLO WORLD")) {
			t.Fatalf("Got the wrong msg payload: %q", msg.Data)
		}
		break
	case <-time.After(time.Second):
		t.Fatalf("Did not receive the message")
	}
	s.sysUnsubscribe(sub)

	// Now send out a message from our normal client.
	// We should not see this one.
	nc.Publish("foo", []byte("You There?"))

	select {
	case <-received:
		t.Fatalf("Received a message when we should not have")
	case <-time.After(100 * time.Millisecond):
		break
	}

	// Now make sure we do not hear ourselves. We optimize this for internally
	// generated messages.
	s.mu.Lock()
	s.sendInternalMsg("foo", "", nil, msg.Data)
	s.mu.Unlock()

	select {
	case <-received:
		t.Fatalf("Received a message when we should not have")
	case <-time.After(100 * time.Millisecond):
		break
	}
}

func TestSystemAccountConnectionUpdatesStopAfterNoLocal(t *testing.T) {
	sa, _, sb, optsB, _ := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	// Normal Account
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 4 // Limit to 4 connections.
	jwt, _ := nac.Encode(okp)

	addAccountToMemResolver(sa, pub, jwt)

	// Listen for updates to the new account connection activity.
	received := make(chan *nats.Msg, 10)
	cb := func(sub *subscription, _ *client, subject, reply string, msg []byte) {
		copy := append([]byte(nil), msg...)
		received <- &nats.Msg{Subject: subject, Reply: reply, Data: copy}
	}
	subj := fmt.Sprintf(accConnsEventSubj, pub)
	sub, err := sa.sysSubscribe(subj, cb)
	if sub == nil || err != nil {
		t.Fatalf("Expected to subscribe, got %v", err)
	}
	defer sa.sysUnsubscribe(sub)

	// Create a few users on the new account.
	clients := []*nats.Conn{}

	url := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)
	for i := 0; i < 4; i++ {
		nc, err := nats.Connect(url, createUserCreds(t, sb, akp))
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		clients = append(clients, nc)
	}

	// Wait for all 4 notifications.
	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		if len(received) == 4 {
			return nil
		}
		return fmt.Errorf("Not enough messages, %d vs 4", len(received))
	})

	// Now lookup the account doing the events on sb.
	acc, _ := sb.LookupAccount(pub)
	// Make sure we have the timer running.
	acc.mu.RLock()
	ctmr := acc.ctmr
	acc.mu.RUnlock()
	if ctmr == nil {
		t.Fatalf("Expected event timer for acc conns to be running")
	}

	// Now close all of the connections.
	for _, nc := range clients {
		nc.Close()
	}

	// Wait for the 4 new notifications, 8 total (4 for connect, 4 for disconnect)
	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		if len(received) == 8 {
			return nil
		}
		return fmt.Errorf("Not enough messages, %d vs 4", len(received))
	})
	// Drain the messages.
	for i := 0; i < 7; i++ {
		<-received
	}
	// Check last one.
	msg := <-received
	m := AccountNumConns{}
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		t.Fatalf("Error unmarshalling account connections request message: %v", err)
	}
	if m.Conns != 0 {
		t.Fatalf("Expected Conns to be 0, got %d", m.Conns)
	}

	// Should not receive any more messages..
	select {
	case <-received:
		t.Fatalf("Did not expect a message here")
	case <-time.After(50 * time.Millisecond):
		break
	}

	// Make sure we have the timer is NOT running.
	acc.mu.RLock()
	ctmr = acc.ctmr
	acc.mu.RUnlock()
	if ctmr != nil {
		t.Fatalf("Expected event timer for acc conns to NOT be running after reaching zero local clients")
	}
}

func TestSystemAccountConnectionLimits(t *testing.T) {
	sa, optsA, sb, optsB, _ := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	// We want to test that we are limited to a certain number of active connections
	// across multiple servers.

	// Let's create a user account.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 4 // Limit to 4 connections.
	jwt, _ := nac.Encode(okp)

	addAccountToMemResolver(sa, pub, jwt)

	urlA := fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)

	// Create a user on each server. Break on first failure.
	for {
		nca1, err := nats.Connect(urlA, createUserCreds(t, sa, akp))
		if err != nil {
			break
		}
		defer nca1.Close()
		ncb1, err := nats.Connect(urlB, createUserCreds(t, sb, akp))
		if err != nil {
			break
		}
		defer ncb1.Close()
	}

	checkFor(t, 1*time.Second, 50*time.Millisecond, func() error {
		total := sa.NumClients() + sb.NumClients()
		if total > int(nac.Limits.Conn) {
			return fmt.Errorf("Expected only %d connections, was allowed to connect %d", nac.Limits.Conn, total)
		}
		return nil
	})
}

// Make sure connection limits apply to the system account itself.
func TestSystemAccountSystemConnectionLimitsHonored(t *testing.T) {
	sa, optsA, sb, optsB, sakp := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	okp, _ := nkeys.FromSeed(oSeed)
	// Update system account to have 10 connections
	pub, _ := sakp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 10
	ajwt, _ := nac.Encode(okp)

	addAccountToMemResolver(sa, pub, ajwt)
	addAccountToMemResolver(sb, pub, ajwt)

	// Update the accounts on each server with new claims to force update.
	sysAccA := sa.SystemAccount()
	sa.updateAccountWithClaimJWT(sysAccA, ajwt)
	sysAccB := sb.SystemAccount()
	sb.updateAccountWithClaimJWT(sysAccB, ajwt)

	// Check system here first, with no external it should be zero.
	sacc := sa.SystemAccount()
	if nlc := sacc.NumLocalConnections(); nlc != 0 {
		t.Fatalf("Expected no local connections, got %d", nlc)
	}

	urlA := fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)

	// Create a user on each server. Break on first failure.
	tc := 0
	for {
		nca1, err := nats.Connect(urlA, createUserCreds(t, sa, sakp))
		if err != nil {
			break
		}
		defer nca1.Close()
		tc++

		ncb1, err := nats.Connect(urlB, createUserCreds(t, sb, sakp))
		if err != nil {
			break
		}
		defer ncb1.Close()
		tc++
	}
	if tc != 10 {
		t.Fatalf("Expected to get 10 external connections, got %d", tc)
	}

	checkFor(t, 1*time.Second, 50*time.Millisecond, func() error {
		total := sa.NumClients() + sb.NumClients()
		if total > int(nac.Limits.Conn) {
			return fmt.Errorf("Expected only %d connections, was allowed to connect %d", nac.Limits.Conn, total)
		}
		return nil
	})
}

// Test that the remote accounting works when a server is started some time later.
func TestSystemAccountConnectionLimitsServersStaggered(t *testing.T) {
	sa, optsA, sb, optsB, _ := runTrustedCluster(t)
	defer sa.Shutdown()
	sb.Shutdown()

	// Let's create a user account.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 4 // Limit to 4 connections.
	jwt, _ := nac.Encode(okp)

	addAccountToMemResolver(sa, pub, jwt)

	urlA := fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port)
	// Create max connections on sa.
	for i := 0; i < int(nac.Limits.Conn); i++ {
		nc, err := nats.Connect(urlA, createUserCreds(t, sa, akp))
		if err != nil {
			t.Fatalf("Unexpected error on #%d try: %v", i+1, err)
		}
		defer nc.Close()
	}

	// Restart server B.
	optsB.AccountResolver = sa.AccountResolver()
	optsB.SystemAccount = sa.systemAccount().Name
	sb = RunServer(optsB)
	defer sb.Shutdown()
	checkClusterFormed(t, sa, sb)

	// Trigger a load of the user account on the new server
	// NOTE: If we do not load the user, the user can be the first
	// to request this account, hence the connection will succeed.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if acc, err := sb.LookupAccount(pub); acc == nil || err != nil {
			return fmt.Errorf("LookupAccount did not return account or failed, err=%v", err)
		}
		return nil
	})

	// Expect this to fail.
	urlB := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)
	if _, err := nats.Connect(urlB, createUserCreds(t, sb, akp)); err == nil {
		t.Fatalf("Expected connection to fail due to max limit")
	}
}

// Test that the remote accounting works when a server is shutdown.
func TestSystemAccountConnectionLimitsServerShutdownGraceful(t *testing.T) {
	sa, optsA, sb, optsB, _ := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	// Let's create a user account.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 10 // Limit to 10 connections.
	jwt, _ := nac.Encode(okp)

	addAccountToMemResolver(sa, pub, jwt)
	addAccountToMemResolver(sb, pub, jwt)

	urlA := fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)

	for i := 0; i < 5; i++ {
		_, err := nats.Connect(urlA, nats.NoReconnect(), createUserCreds(t, sa, akp))
		if err != nil {
			t.Fatalf("Expected to connect, got %v", err)
		}
		_, err = nats.Connect(urlB, nats.NoReconnect(), createUserCreds(t, sb, akp))
		if err != nil {
			t.Fatalf("Expected to connect, got %v", err)
		}
	}

	// We are at capacity so both of these should fail.
	if _, err := nats.Connect(urlA, createUserCreds(t, sa, akp)); err == nil {
		t.Fatalf("Expected connection to fail due to max limit")
	}
	if _, err := nats.Connect(urlB, createUserCreds(t, sb, akp)); err == nil {
		t.Fatalf("Expected connection to fail due to max limit")
	}

	// Now shutdown Server B.
	sb.Shutdown()

	// Now we should be able to create more on A now.
	for i := 0; i < 5; i++ {
		_, err := nats.Connect(urlA, createUserCreds(t, sa, akp))
		if err != nil {
			t.Fatalf("Expected to connect on %d, got %v", i, err)
		}
	}
}

// Test that the remote accounting works when a server goes away.
func TestSystemAccountConnectionLimitsServerShutdownForced(t *testing.T) {
	sa, optsA, sb, optsB, _ := runTrustedCluster(t)
	defer sa.Shutdown()

	// Let's create a user account.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 20 // Limit to 20 connections.
	jwt, _ := nac.Encode(okp)

	addAccountToMemResolver(sa, pub, jwt)
	addAccountToMemResolver(sb, pub, jwt)

	urlA := fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)

	for i := 0; i < 10; i++ {
		c, err := nats.Connect(urlA, nats.NoReconnect(), createUserCreds(t, sa, akp))
		if err != nil {
			t.Fatalf("Expected to connect, got %v", err)
		}
		defer c.Close()
		c, err = nats.Connect(urlB, nats.NoReconnect(), createUserCreds(t, sb, akp))
		if err != nil {
			t.Fatalf("Expected to connect, got %v", err)
		}
		defer c.Close()
	}

	// Now shutdown Server B. Do so such that no communications go out.
	sb.mu.Lock()
	sb.sys = nil
	sb.mu.Unlock()
	sb.Shutdown()

	if _, err := nats.Connect(urlA, createUserCreds(t, sa, akp)); err == nil {
		t.Fatalf("Expected connection to fail due to max limit")
	}

	// Let's speed up the checking process.
	sa.mu.Lock()
	sa.sys.chkOrph = 10 * time.Millisecond
	sa.sys.orphMax = 30 * time.Millisecond
	sa.sys.sweeper.Reset(sa.sys.chkOrph)
	sa.mu.Unlock()

	// We should eventually be able to connect.
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		if c, err := nats.Connect(urlA, createUserCreds(t, sa, akp)); err != nil {
			return err
		} else {
			c.Close()
		}
		return nil
	})
}

func TestSystemAccountFromConfig(t *testing.T) {
	kp, _ := nkeys.FromSeed(oSeed)
	opub, _ := kp.PublicKey()
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	ajwt, err := nac.Encode(kp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(ajwt))
	}))
	defer ts.Close()

	confTemplate := `
		listen: -1
		trusted: %s
		system_account: %s
		resolver: URL("%s/jwt/v1/accounts/")
    `

	conf := createConfFile(t, []byte(fmt.Sprintf(confTemplate, opub, apub, ts.URL)))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if acc := s.SystemAccount(); acc == nil || acc.Name != apub {
		t.Fatalf("System Account not properly set")
	}
}

func TestAccountClaimsUpdates(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	sacc, sakp := createAccount(s)
	s.setSystemAccount(sacc)

	// Let's create a normal  account with limits we can update.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 4
	ajwt, _ := nac.Encode(okp)

	addAccountToMemResolver(s, pub, ajwt)

	acc, _ := s.LookupAccount(pub)
	if acc.MaxActiveConnections() != 4 {
		t.Fatalf("Expected to see a limit of 4 connections")
	}

	// Simulate a systems publisher so we can do an account claims update.
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(url, createUserCreds(t, s, sakp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Update the account
	nac = jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 8
	issAt := time.Now().Add(-30 * time.Second).Unix()
	nac.IssuedAt = issAt
	expires := time.Now().Add(2 * time.Second).Unix()
	nac.Expires = expires
	ajwt, _ = nac.Encode(okp)

	// Publish to the system update subject.
	claimUpdateSubj := fmt.Sprintf(accUpdateEventSubj, pub)
	nc.Publish(claimUpdateSubj, []byte(ajwt))
	nc.Flush()

	acc, _ = s.LookupAccount(pub)
	if acc.MaxActiveConnections() != 8 {
		t.Fatalf("Account was not updated")
	}
}

func TestAccountConnsLimitExceededAfterUpdate(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	sacc, _ := createAccount(s)
	s.setSystemAccount(sacc)

	// Let's create a normal  account with limits we can update.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 10
	ajwt, _ := nac.Encode(okp)

	addAccountToMemResolver(s, pub, ajwt)
	acc, _ := s.LookupAccount(pub)

	// Now create the max connections.
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	for {
		nc, err := nats.Connect(url, createUserCreds(t, s, akp))
		if err != nil {
			break
		}
		defer nc.Close()
	}

	// We should have max here.
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		if total := s.NumClients(); total != acc.MaxActiveConnections() {
			return fmt.Errorf("Expected %d connections, got %d", acc.MaxActiveConnections(), total)
		}
		return nil
	})

	// Now change limits to make current connections over the limit.
	nac = jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 2
	ajwt, _ = nac.Encode(okp)

	s.updateAccountWithClaimJWT(acc, ajwt)
	if acc.MaxActiveConnections() != 2 {
		t.Fatalf("Expected max connections to be set to 2, got %d", acc.MaxActiveConnections())
	}
	// We should have closed the excess connections.
	if total := s.NumClients(); total != acc.MaxActiveConnections() {
		t.Fatalf("Expected %d connections, got %d", acc.MaxActiveConnections(), total)
	}
}

func TestAccountConnsLimitExceededAfterUpdateDisconnectNewOnly(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	sacc, _ := createAccount(s)
	s.setSystemAccount(sacc)

	// Let's create a normal  account with limits we can update.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 10
	ajwt, _ := nac.Encode(okp)

	addAccountToMemResolver(s, pub, ajwt)
	acc, _ := s.LookupAccount(pub)

	// Now create the max connections.
	// We create half then we will wait and then create the rest.
	// Will test that we disconnect the newest ones.
	newConns := make([]*nats.Conn, 0, 5)
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	for i := 0; i < 5; i++ {
		nats.Connect(url, nats.NoReconnect(), createUserCreds(t, s, akp))
	}
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < 5; i++ {
		nc, _ := nats.Connect(url, nats.NoReconnect(), createUserCreds(t, s, akp))
		newConns = append(newConns, nc)
	}

	// We should have max here.
	if total := s.NumClients(); total != acc.MaxActiveConnections() {
		t.Fatalf("Expected %d connections, got %d", acc.MaxActiveConnections(), total)
	}

	// Now change limits to make current connections over the limit.
	nac = jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 5
	ajwt, _ = nac.Encode(okp)

	s.updateAccountWithClaimJWT(acc, ajwt)
	if acc.MaxActiveConnections() != 5 {
		t.Fatalf("Expected max connections to be set to 2, got %d", acc.MaxActiveConnections())
	}
	// We should have closed the excess connections.
	if total := s.NumClients(); total != acc.MaxActiveConnections() {
		t.Fatalf("Expected %d connections, got %d", acc.MaxActiveConnections(), total)
	}

	// Now make sure that only the new ones were closed.
	var closed int
	for _, nc := range newConns {
		if !nc.IsClosed() {
			closed++
		}
	}
	if closed != 5 {
		t.Fatalf("Expected all new clients to be closed, only got %d of 5", closed)
	}
}

func TestSystemAccountWithBadRemoteLatencyUpdate(t *testing.T) {
	s, _ := runTrustedServer(t)
	defer s.Shutdown()

	acc, _ := createAccount(s)
	s.setSystemAccount(acc)

	rl := remoteLatency{
		Account: "NONSENSE",
		ReqId:   "_INBOX.22",
	}
	b, _ := json.Marshal(&rl)
	s.remoteLatencyUpdate(nil, nil, "foo", "", b)
}

func TestSystemAccountWithGateways(t *testing.T) {
	sa, oa, sb, ob, akp := runTrustedGateways(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	// Create a client on A that will subscribe on $SYS.ACCOUNT.>
	urla := fmt.Sprintf("nats://%s:%d", oa.Host, oa.Port)
	nca := natsConnect(t, urla, createUserCreds(t, sa, akp))
	defer nca.Close()

	sub, _ := nca.SubscribeSync("$SYS.ACCOUNT.>")
	defer sub.Unsubscribe()
	nca.Flush()

	// If this tests fails with wrong number after 10 seconds we may have
	// added a new inititial subscription for the eventing system.
	checkExpectedSubs(t, 13, sa)

	// Create a client on B and see if we receive the event
	urlb := fmt.Sprintf("nats://%s:%d", ob.Host, ob.Port)
	ncb := natsConnect(t, urlb, createUserCreds(t, sb, akp), nats.Name("TEST EVENTS"))
	defer ncb.Close()

	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	// Basic checks, could expand on that...
	accName := sa.SystemAccount().Name
	if !strings.HasPrefix(msg.Subject, fmt.Sprintf("$SYS.ACCOUNT.%s.CONNECT", accName)) {
		t.Fatalf("Expected subject to start with %q, got %q", "$SYS.ACCOUNT.<account>.CONNECT", msg.Subject)
	}
	tokens := strings.Split(msg.Subject, ".")
	if len(tokens) < 4 {
		t.Fatalf("Expected 4 tokens, got %d", len(tokens))
	}
	account := tokens[2]
	if account != accName {
		t.Fatalf("Expected %q for account, got %q", accName, account)
	}
}
func TestServerEventsStatsZ(t *testing.T) {
	preStart := time.Now()
	sa, optsA, sb, _, akp := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()
	postStart := time.Now()

	url := fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port)
	ncs, err := nats.Connect(url, createUserCreds(t, sa, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncs.Close()

	subj := fmt.Sprintf(serverStatsSubj, sa.ID())
	sub, _ := ncs.SubscribeSync(subj)
	defer sub.Unsubscribe()
	ncs.Publish("foo", []byte("HELLO WORLD"))
	ncs.Flush()

	// Let's speed up the checking process.
	sa.mu.Lock()
	sa.sys.statsz = 10 * time.Millisecond
	sa.sys.stmr.Reset(sa.sys.statsz)
	sa.mu.Unlock()

	_, err = sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	// Get it the second time so we can check some stats
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	m := ServerStatsMsg{}
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		t.Fatalf("Error unmarshalling the statz json: %v", err)
	}
	if m.Server.ID != sa.ID() {
		t.Fatalf("Did not match IDs")
	}
	if m.Server.Cluster != "TEST CLUSTER 22" {
		t.Fatalf("Did not match cluster name")
	}
	if m.Server.Version != VERSION {
		t.Fatalf("Did not match server version")
	}
	if !m.Stats.Start.After(preStart) && m.Stats.Start.Before(postStart) {
		t.Fatalf("Got a wrong start time for the server %v", m.Stats.Start)
	}
	if m.Stats.Connections != 1 {
		t.Fatalf("Did not match connections of 1, got %d", m.Stats.Connections)
	}
	if m.Stats.ActiveAccounts != 2 {
		t.Fatalf("Did not match active accounts of 2, got %d", m.Stats.ActiveAccounts)
	}
	if m.Stats.Sent.Msgs < 1 {
		t.Fatalf("Did not match sent msgs of >=1, got %d", m.Stats.Sent.Msgs)
	}
	if m.Stats.Received.Msgs < 1 {
		t.Fatalf("Did not match received msgs of >=1, got %d", m.Stats.Received.Msgs)
	}
	if lr := len(m.Stats.Routes); lr != 1 {
		t.Fatalf("Expected a route, but got %d", lr)
	}

	// Now let's prompt this server to send us the statsz
	subj = fmt.Sprintf(serverStatsReqSubj, sa.ID())
	msg, err = ncs.Request(subj, nil, time.Second)
	if err != nil {
		t.Fatalf("Error trying to request statsz: %v", err)
	}
	m2 := ServerStatsMsg{}
	if err := json.Unmarshal(msg.Data, &m2); err != nil {
		t.Fatalf("Error unmarshalling the statz json: %v", err)
	}
	if m2.Server.ID != sa.ID() {
		t.Fatalf("Did not match IDs")
	}
	if m2.Stats.Connections != 1 {
		t.Fatalf("Did not match connections of 1, got %d", m2.Stats.Connections)
	}
	if m2.Stats.ActiveAccounts != 2 {
		t.Fatalf("Did not match active accounts of 2, got %d", m2.Stats.ActiveAccounts)
	}
	if m2.Stats.Sent.Msgs < 3 {
		t.Fatalf("Did not match sent msgs of >= 3, got %d", m2.Stats.Sent.Msgs)
	}
	if m2.Stats.Received.Msgs < 1 {
		t.Fatalf("Did not match received msgs of >= 1, got %d", m2.Stats.Received.Msgs)
	}
	if lr := len(m2.Stats.Routes); lr != 1 {
		t.Fatalf("Expected a route, but got %d", lr)
	}

	msg, err = ncs.Request(subj, nil, time.Second)
	if err != nil {
		t.Fatalf("Error trying to request statsz: %v", err)
	}
	m3 := ServerStatsMsg{}
	if err := json.Unmarshal(msg.Data, &m3); err != nil {
		t.Fatalf("Error unmarshalling the statz json: %v", err)
	}
	if m3.Server.ID != sa.ID() {
		t.Fatalf("Did not match IDs")
	}
	if m3.Stats.Connections != 1 {
		t.Fatalf("Did not match connections of 1, got %d", m3.Stats.Connections)
	}
	if m3.Stats.ActiveAccounts != 2 {
		t.Fatalf("Did not match active accounts of 2, got %d", m3.Stats.ActiveAccounts)
	}
	if m3.Stats.Sent.Msgs < 5 {
		t.Fatalf("Did not match sent msgs of >= 5, got %d", m3.Stats.Sent.Msgs)
	}
	if m3.Stats.Received.Msgs < 2 {
		t.Fatalf("Did not match received msgs of >= 2, got %d", m3.Stats.Received.Msgs)
	}
	if lr := len(m3.Stats.Routes); lr != 1 {
		t.Fatalf("Expected a route, but got %d", lr)
	}
	if sr := m3.Stats.Routes[0]; sr.Name != "B" {
		t.Fatalf("Expected server A's route to B to have Name set to %q, got %q", "B", sr.Name)
	}

	// Now query B and check that route's name is "A"
	subj = fmt.Sprintf(serverStatsReqSubj, sb.ID())
	ncs.SubscribeSync(subj)
	msg, err = ncs.Request(subj, nil, time.Second)
	if err != nil {
		t.Fatalf("Error trying to request statsz: %v", err)
	}
	m = ServerStatsMsg{}
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		t.Fatalf("Error unmarshalling the statz json: %v", err)
	}
	if lr := len(m.Stats.Routes); lr != 1 {
		t.Fatalf("Expected a route, but got %d", lr)
	}
	if sr := m.Stats.Routes[0]; sr.Name != "A" {
		t.Fatalf("Expected server B's route to A to have Name set to %q, got %q", "A", sr.Name)
	}
}

func TestServerEventsPingStatsZ(t *testing.T) {
	sa, _, sb, optsB, akp := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	url := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)
	nc, err := nats.Connect(url, createUserCreds(t, sb, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	reply := nc.NewRespInbox()
	sub, _ := nc.SubscribeSync(reply)

	nc.PublishRequest(serverStatsPingReqSubj, reply, nil)

	// Make sure its a statsz
	m := ServerStatsMsg{}

	// Receive both manually.
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		t.Fatalf("Error unmarshalling the statz json: %v", err)
	}
	msg, err = sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		t.Fatalf("Error unmarshalling the statz json: %v", err)
	}
}

func TestGatewayNameClientInfo(t *testing.T) {
	sa, _, sb, _, _ := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	_, _, l := newClientForServer(sa)

	var info Info
	err := json.Unmarshal([]byte(l[5:]), &info)
	if err != nil {
		t.Fatalf("Could not parse INFO json: %v\n", err)
	}
	if info.Cluster != "TEST CLUSTER 22" {
		t.Fatalf("Expected a cluster name of 'TEST CLUSTER 22', got %q", info.Cluster)
	}
}

type slowAccResolver struct {
	sync.Mutex
	AccountResolver
	acc string
}

func (sr *slowAccResolver) Fetch(name string) (string, error) {
	sr.Lock()
	delay := sr.acc == name
	sr.Unlock()
	if delay {
		time.Sleep(200 * time.Millisecond)
	}
	return sr.AccountResolver.Fetch(name)
}

func TestConnectionUpdatesTimerProperlySet(t *testing.T) {
	origEventsHBInterval := eventsHBInterval
	eventsHBInterval = 50 * time.Millisecond
	defer func() { eventsHBInterval = origEventsHBInterval }()

	sa, _, sb, optsB, _ := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	// Normal Account
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 10 // set any limit...
	jwt, _ := nac.Encode(okp)

	addAccountToMemResolver(sa, pub, jwt)

	// Listen for HB updates...
	count := int32(0)
	cb := func(sub *subscription, _ *client, subject, reply string, msg []byte) {
		atomic.AddInt32(&count, 1)
	}
	subj := fmt.Sprintf(accConnsEventSubj, pub)
	sub, err := sa.sysSubscribe(subj, cb)
	if sub == nil || err != nil {
		t.Fatalf("Expected to subscribe, got %v", err)
	}
	defer sa.sysUnsubscribe(sub)

	url := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)
	nc := natsConnect(t, url, createUserCreds(t, sb, akp))
	defer nc.Close()

	time.Sleep(500 * time.Millisecond)
	// After waiting 500ms with HB interval of 50ms, we should get
	// about 10 updates, no much more
	if n := atomic.LoadInt32(&count); n > 15 {
		t.Fatalf("Expected about 10 updates, got %v", n)
	}

	// Now lookup the account doing the events on sb.
	acc, _ := sb.LookupAccount(pub)
	// Make sure we have the timer running.
	acc.mu.RLock()
	ctmr := acc.ctmr
	acc.mu.RUnlock()
	if ctmr == nil {
		t.Fatalf("Expected event timer for acc conns to be running")
	}

	nc.Close()

	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		// Make sure we have the timer is NOT running.
		acc.mu.RLock()
		ctmr = acc.ctmr
		acc.mu.RUnlock()
		if ctmr != nil {
			return fmt.Errorf("Expected event timer for acc conns to NOT be running after reaching zero local clients")
		}
		return nil
	})
}
