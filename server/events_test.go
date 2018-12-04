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
	"net/http"
	"net/http/httptest"
	"os"
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

	sa := RunServer(optsA)

	optsB := nextServerOpts(optsA)
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
	optsB.AccountResolver = sa.accResolver
	optsB.SystemAccount = sa.systemAccount().Name
	sb = RunServer(optsB)
	defer sb.Shutdown()
	checkClusterFormed(t, sa, sb)

	// Trigger a load of the user account on the new server
	// NOTE: If we do not load the user, the user can be the first
	// to request this account, hence the connection will succeed.
	sb.LookupAccount(pub)

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

	acc := s.LookupAccount(pub)
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

	acc = s.LookupAccount(pub)
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
	acc := s.LookupAccount(pub)

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
	acc := s.LookupAccount(pub)

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
	checkExpectedSubs(t, 7, sa)

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
func TestServerEventStatusZ(t *testing.T) {
	sa, optsA, sb, _, akp := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

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
	if m.Stats.Connections != 1 {
		t.Fatalf("Did not match connections of 1, got %d", m.Stats.Connections)
	}
	if m.Stats.ActiveAccounts != 2 {
		t.Fatalf("Did not match active accounts of 2, got %d", m.Stats.ActiveAccounts)
	}
	if m.Stats.Sent.Msgs != 1 {
		t.Fatalf("Did not match sent msgs of 1, got %d", m.Stats.Sent.Msgs)
	}
	if m.Stats.Received.Msgs != 1 {
		t.Fatalf("Did not match received msgs of 1, got %d", m.Stats.Received.Msgs)
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
	if m2.Stats.Sent.Msgs != 3 {
		t.Fatalf("Did not match sent msgs of 3, got %d", m2.Stats.Sent.Msgs)
	}
	if m2.Stats.Received.Msgs != 1 {
		t.Fatalf("Did not match received msgs of 1, got %d", m2.Stats.Received.Msgs)
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
	if m3.Stats.Sent.Msgs != 5 {
		t.Fatalf("Did not match sent msgs of 5, got %d", m3.Stats.Sent.Msgs)
	}
	if m3.Stats.Received.Msgs != 2 {
		t.Fatalf("Did not match received msgs of 2, got %d", m3.Stats.Received.Msgs)
	}
	if lr := len(m3.Stats.Routes); lr != 1 {
		t.Fatalf("Expected a route, but got %d", lr)
	}
}
