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

package test

import (
	"fmt"
	"testing"

	"github.com/nats-io/jwt"
	"github.com/nats-io/nats-server/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const testOpConfig = "./configs/operator.conf"

// This matches ./configs/nkeys_jwts/test.seed
// Test operator seed.
var oSeed = []byte("SOAFYNORQLQFJYBYNUGC5D7SH2MXMUX5BFEWWGHN3EK4VGG5TPT5DZP7QU")

// This is a signing key seed.
var skSeed = []byte("SOAEL3NFOTU6YK3DBTEKQYZ2C5IWSVZWWZCQDASBUOHJKBFLVANK27JMMQ")

func checkKeys(t *testing.T, opts *server.Options, opc *jwt.OperatorClaims, expected int) {
	// We should have filled in the TrustedKeys here.
	if len(opts.TrustedKeys) != expected {
		t.Fatalf("Should have %d trusted keys, got %d", expected, len(opts.TrustedKeys))
	}
	// Check that we properly placed all keys from the opc into TrustedKeys
	chkMember := func(s string) {
		for _, c := range opts.TrustedKeys {
			if s == c {
				return
			}
		}
		t.Fatalf("Expected %q to be in TrustedKeys", s)
	}
	chkMember(opc.Issuer)
	for _, sk := range opc.SigningKeys {
		chkMember(sk)
	}
}

// This will test that we enforce certain restrictions when you use trusted operators.
// Like auth is always true, can't define accounts or users, required to define an account resolver, etc.
func TestOperatorRestrictions(t *testing.T) {
	opts, err := server.ProcessConfigFile(testOpConfig)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	if _, err := server.NewServer(opts); err != nil {
		t.Fatalf("Expected to create a server successfully")
	}
	// TrustedKeys get defined when processing from above, trying again with
	// same opts should not work.
	if _, err := server.NewServer(opts); err == nil {
		t.Fatalf("Expected an error with TrustedKeys defined")
	}
	// Must wipe and rebuild to succeed.
	wipeOpts := func() {
		opts.TrustedKeys = nil
		opts.Accounts = nil
		opts.Users = nil
		opts.Nkeys = nil
		opts.AllowNewAccounts = false
	}

	wipeOpts()
	opts.Accounts = []*server.Account{{Name: "TEST"}}
	if _, err := server.NewServer(opts); err == nil {
		t.Fatalf("Expected an error with Accounts defined")
	}
	wipeOpts()
	opts.Users = []*server.User{{Username: "TEST"}}
	if _, err := server.NewServer(opts); err == nil {
		t.Fatalf("Expected an error with Users defined")
	}
	wipeOpts()
	opts.Nkeys = []*server.NkeyUser{{Nkey: "TEST"}}
	if _, err := server.NewServer(opts); err == nil {
		t.Fatalf("Expected an error with Nkey Users defined")
	}
	wipeOpts()
	opts.AllowNewAccounts = true
	if _, err := server.NewServer(opts); err == nil {
		t.Fatalf("Expected an error with AllowNewAccounts set to true")
	}

	wipeOpts()
	opts.AccountResolver = nil
	if _, err := server.NewServer(opts); err == nil {
		t.Fatalf("Expected an error without an AccountResolver defined")
	}
}

func TestOperatorConfig(t *testing.T) {
	opts, err := server.ProcessConfigFile(testOpConfig)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	// Check we have the TrustedOperators
	if len(opts.TrustedOperators) != 1 {
		t.Fatalf("Expected to load the operator")
	}
	_, err = server.NewServer(opts)
	if err != nil {
		t.Fatalf("Expected to create a server: %v", err)
	}
	// We should have filled in the TrustedKeys here.
	// Our master key (issuer) plus the signing keys (3).
	checkKeys(t, opts, opts.TrustedOperators[0], 4)
}

func runOperatorServer(t *testing.T) (*server.Server, *server.Options) {
	return RunServerWithConfig(testOpConfig)
}

func createAccountForOperatorKey(t *testing.T, s *server.Server, seed []byte) (*server.Account, nkeys.KeyPair) {
	t.Helper()
	okp, _ := nkeys.FromSeed(seed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	jwt, _ := nac.Encode(okp)
	if err := s.AccountResolver().Store(pub, jwt); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	acc, err := s.LookupAccount(pub)
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}
	return acc, akp
}

func createAccount(t *testing.T, s *server.Server) (*server.Account, nkeys.KeyPair) {
	t.Helper()
	return createAccountForOperatorKey(t, s, oSeed)
}

func createUserCreds(t *testing.T, s *server.Server, akp nkeys.KeyPair) nats.Option {
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

func TestOperatorServer(t *testing.T) {
	s, opts := runOperatorServer(t)
	defer s.Shutdown()

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	if _, err := nats.Connect(url); err == nil {
		t.Fatalf("Expected to fail with no credentials")
	}

	_, akp := createAccount(t, s)
	nc, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	nc.Close()

	// Now create an account from another operator, this should fail.
	okp, _ := nkeys.CreateOperator()
	seed, _ := okp.Seed()
	_, akp = createAccountForOperatorKey(t, s, seed)
	_, err = nats.Connect(url, createUserCreds(t, s, akp))
	if err == nil {
		t.Fatalf("Expected error on connect")
	}
}

func TestOperatorSystemAccount(t *testing.T) {
	s, _ := runOperatorServer(t)
	defer s.Shutdown()

	// Create an account from another operator, this should fail if used as a system account.
	okp, _ := nkeys.CreateOperator()
	seed, _ := okp.Seed()
	acc, _ := createAccountForOperatorKey(t, s, seed)
	if err := s.SetSystemAccount(acc.Name); err == nil {
		t.Fatalf("Expected this to fail")
	}
	if acc := s.SystemAccount(); acc != nil {
		t.Fatalf("Expected no account to be set for system account")
	}

	acc, _ = createAccount(t, s)
	if err := s.SetSystemAccount(acc.Name); err != nil {
		t.Fatalf("Expected this succeed, got %v", err)
	}
	if sysAcc := s.SystemAccount(); sysAcc != acc {
		t.Fatalf("Did not get matching account for system account")
	}
}

func TestOperatorSigningKeys(t *testing.T) {
	s, opts := runOperatorServer(t)
	defer s.Shutdown()

	// Create an account with a signing key, not the master key.
	acc, akp := createAccountForOperatorKey(t, s, skSeed)

	// Make sure we can set system account.
	if err := s.SetSystemAccount(acc.Name); err != nil {
		t.Fatalf("Expected this succeed, got %v", err)
	}

	// Make sure we can create users with it too.
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	nc.Close()
}

func TestOperatorMemResolverPreload(t *testing.T) {
	s, opts := RunServerWithConfig("./configs/resolver_preload.conf")
	defer s.Shutdown()

	// Make sure we can look up the account.
	acc, _ := s.LookupAccount("ADM2CIIL3RWXBA6T2HW3FODNCQQOUJEHHQD6FKCPVAMHDNTTSMO73ROX")
	if acc == nil {
		t.Fatalf("Expected to properly lookup account")
	}
	sacc := s.SystemAccount()
	if sacc == nil {
		t.Fatalf("Expected to have system account registered")
	}
	if sacc.Name != opts.SystemAccount {
		t.Fatalf("System account does not match, wanted %q, got %q", opts.SystemAccount, sacc.Name)
	}
}

func TestOperatorConfigReloadDoesntKillNonce(t *testing.T) {
	s, _ := runOperatorServer(t)
	defer s.Shutdown()

	if !s.NonceRequired() {
		t.Fatalf("Error nonce should be required")
	}

	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	if !s.NonceRequired() {
		t.Fatalf("Error nonce should still be required after reload")
	}
}
