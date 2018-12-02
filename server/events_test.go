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
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/jwt"
	"github.com/nats-io/nkeys"
)

func createAccount(s *Server) (*Account, nkeys.KeyPair) {
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	jwt, _ := nac.Encode(okp)
	addAccountToMemResolver(s, pub, jwt)
	return s.LookupAccount(pub), akp
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
	opts.TrustedNkeys = []string{pub}
	opts.accResolver = &MemAccResolver{}
	s := RunServer(opts)
	return s, opts
}

func runTrustedCluster(t *testing.T) (*Server, *Options, *Server, *Options) {
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
	optsA.TrustedNkeys = []string{pub}
	optsA.accResolver = mr
	optsA.SystemAccount = apub

	sa := RunServer(optsA)

	optsB := nextServerOpts(optsA)
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host, optsA.Cluster.Port))
	sb := RunServer(optsB)

	checkClusterFormed(t, sa, sb)

	return sa, optsA, sb, optsB
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

	sub, _ := ncs.SubscribeSync("$SYS.ACCOUNT.>")
	defer sub.Unsubscribe()
	ncs.Flush()

	// We can't hear ourselves, so we need to create a second client to
	// trigger the connect/disconnect events.
	acc2, akp2 := createAccount(s)

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
	cb := func(sub *subscription, subject, reply string, msg []byte) {
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
	r := SublistResult{psubs: []*subscription{sub}}
	s.mu.Lock()
	s.sendInternalMsg(&r, "foo", "", nil, msg.Data)
	s.mu.Unlock()

	select {
	case <-received:
		t.Fatalf("Received a message when we should not have")
	case <-time.After(100 * time.Millisecond):
		break
	}
}

func TestSystemAccountConnectionLimits(t *testing.T) {
	sa, optsA, sb, optsB := runTrustedCluster(t)

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

	total := sa.NumClients() + sb.NumClients()
	if total > int(nac.Limits.Conn) {
		t.Fatalf("Expected only %d connections, was allowed to connect %d", nac.Limits.Conn, total)
	}
}

// Test that the remote accounting works when a server is started some time later.
func TestSystemAccountConnectionLimitsServersStaggered(t *testing.T) {
	sa, optsA, sb, optsB := runTrustedCluster(t)
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
	optsB.accResolver = sa.accResolver
	optsB.SystemAccount = sa.systemAccount().Name
	sb = RunServer(optsB)
	defer sb.Shutdown()
	checkClusterFormed(t, sa, sb)

	// Trigger a load of the user account on the new server
	// NOTE: If we do not load the user can be the first to request this
	// account, hence the connection will succeed.
	sb.LookupAccount(pub)

	// Expect this to fail.
	urlB := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)
	if _, err := nats.Connect(urlB, createUserCreds(t, sb, akp)); err == nil {
		t.Fatalf("Expected connection to fail due to max limit")
	}
}

// Test that the remote accounting works when a server is shutdown.
func TestSystemAccountConnectionLimitsServerShutdownGraceful(t *testing.T) {
	sa, optsA, sb, optsB := runTrustedCluster(t)
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
	sa, optsA, sb, optsB := runTrustedCluster(t)
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

	// Now shutdown Server B. Do so such that now communications go out.
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
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		if c, err := nats.Connect(urlA, createUserCreds(t, sa, akp)); err != nil {
			return err
		} else {
			c.Close()
		}
		return nil
	})
}
