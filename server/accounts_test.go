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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nkeys"
)

func simpleAccountServer(t *testing.T) (*Server, *Account, *Account) {
	opts := defaultServerOptions
	s := New(&opts)

	// Now create two accounts.
	f, err := s.RegisterAccount("$foo")
	if err != nil {
		t.Fatalf("Error creating account 'foo': %v", err)
	}
	b, err := s.RegisterAccount("$bar")
	if err != nil {
		t.Fatalf("Error creating account 'bar': %v", err)
	}
	return s, f, b
}

func TestRegisterDuplicateAccounts(t *testing.T) {
	s, _, _ := simpleAccountServer(t)
	if _, err := s.RegisterAccount("$foo"); err == nil {
		t.Fatal("Expected an error registering 'foo' twice")
	}
}

func TestAccountIsolation(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	cfoo, crFoo, _ := newClientForServer(s)
	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error register client with 'foo' account: %v", err)
	}
	cbar, crBar, _ := newClientForServer(s)
	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error register client with 'bar' account: %v", err)
	}

	// Make sure they are different accounts/sl.
	if cfoo.acc == cbar.acc {
		t.Fatalf("Error, accounts the same for both clients")
	}

	// Now do quick test that makes sure messages do not cross over.
	// setup bar as a foo subscriber.
	go cbar.parse([]byte("SUB foo 1\r\nPING\r\nPING\r\n"))
	l, err := crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}
	if !strings.HasPrefix(l, "PONG\r\n") {
		t.Fatalf("PONG response incorrect: %q", l)
	}

	go cfoo.parse([]byte("SUB foo 1\r\nPUB foo 5\r\nhello\r\nPING\r\n"))
	l, err = crFoo.ReadString('\n')
	if err != nil {
		t.Fatalf("Error for client 'foo' from server: %v", err)
	}

	matches := msgPat.FindAllStringSubmatch(l, -1)[0]
	if matches[SUB_INDEX] != "foo" {
		t.Fatalf("Did not get correct subject: '%s'\n", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	checkPayload(crFoo, []byte("hello\r\n"), t)

	// Now make sure nothing shows up on bar.
	l, err = crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}
	if !strings.HasPrefix(l, "PONG\r\n") {
		t.Fatalf("PONG response incorrect: %q", l)
	}
}

func TestAccountFromOptions(t *testing.T) {
	opts := defaultServerOptions
	opts.Accounts = []*Account{NewAccount("foo"), NewAccount("bar")}
	s := New(&opts)

	ta := s.numReservedAccounts() + 2
	if la := s.numAccounts(); la != ta {
		t.Fatalf("Expected to have a server with %d active accounts, got %v", ta, la)
	}
	// Check that sl is filled in.
	fooAcc, _ := s.LookupAccount("foo")
	barAcc, _ := s.LookupAccount("bar")
	if fooAcc == nil || barAcc == nil {
		t.Fatalf("Error retrieving accounts for 'foo' and 'bar'")
	}
	if fooAcc.sl == nil || barAcc.sl == nil {
		t.Fatal("Expected Sublists to be filled in on Opts.Accounts")
	}
}

func TestNewAccountsFromClients(t *testing.T) {
	opts := defaultServerOptions
	s := New(&opts)

	c, cr, _ := newClientForServer(s)
	connectOp := []byte("CONNECT {\"account\":\"foo\"}\r\n")
	go c.parse(connectOp)
	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}

	opts.AllowNewAccounts = true
	s = New(&opts)

	c, cr, _ = newClientForServer(s)
	err := c.parse(connectOp)
	if err != nil {
		t.Fatalf("Received an error trying to connect: %v", err)
	}
	go c.parse([]byte("PING\r\n"))
	l, err = cr.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading response for client from server: %v", err)
	}
	if !strings.HasPrefix(l, "PONG\r\n") {
		t.Fatalf("PONG response incorrect: %q", l)
	}
}

func TestActiveAccounts(t *testing.T) {
	opts := defaultServerOptions
	opts.AllowNewAccounts = true
	opts.Cluster.Port = 22

	s := New(&opts)

	if s.NumActiveAccounts() != 0 {
		t.Fatalf("Expected no active accounts, got %d", s.NumActiveAccounts())
	}

	addClientWithAccount := func(accName string) *client {
		t.Helper()
		c, _, _ := newClientForServer(s)
		connectOp := fmt.Sprintf("CONNECT {\"account\":\"%s\"}\r\n", accName)
		err := c.parse([]byte(connectOp))
		if err != nil {
			t.Fatalf("Received an error trying to connect: %v", err)
		}
		return c
	}

	// Now add some clients.
	cf1 := addClientWithAccount("foo")
	if s.activeAccounts != 1 {
		t.Fatalf("Expected active accounts to be 1, got %d", s.activeAccounts)
	}
	// Adding in same one should not change total.
	cf2 := addClientWithAccount("foo")
	if s.activeAccounts != 1 {
		t.Fatalf("Expected active accounts to be 1, got %d", s.activeAccounts)
	}
	// Add in new one.
	cb1 := addClientWithAccount("bar")
	if s.activeAccounts != 2 {
		t.Fatalf("Expected active accounts to be 2, got %d", s.activeAccounts)
	}

	// Make sure the Accounts track clients.
	foo, _ := s.LookupAccount("foo")
	bar, _ := s.LookupAccount("bar")
	if foo == nil || bar == nil {
		t.Fatalf("Error looking up accounts")
	}
	if nc := foo.NumConnections(); nc != 2 {
		t.Fatalf("Expected account foo to have 2 clients, got %d", nc)
	}
	if nc := bar.NumConnections(); nc != 1 {
		t.Fatalf("Expected account bar to have 1 client, got %d", nc)
	}

	waitTilActiveCount := func(n int32) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if active := s.NumActiveAccounts(); active != n {
				return fmt.Errorf("Number of active accounts is %d", active)
			}
			return nil
		})
	}

	// Test Removal
	cb1.closeConnection(ClientClosed)
	waitTilActiveCount(1)

	if nc := bar.NumConnections(); nc != 0 {
		t.Fatalf("Expected account bar to have 0 clients, got %d", nc)
	}

	// This should not change the count.
	cf1.closeConnection(ClientClosed)
	waitTilActiveCount(1)

	if nc := foo.NumConnections(); nc != 1 {
		t.Fatalf("Expected account foo to have 1 client, got %d", nc)
	}

	cf2.closeConnection(ClientClosed)
	waitTilActiveCount(0)

	if nc := foo.NumConnections(); nc != 0 {
		t.Fatalf("Expected account bar to have 0 clients, got %d", nc)
	}
}

// Clients can ask that the account be forced to be new. If it exists this is an error.
func TestNewAccountRequireNew(t *testing.T) {
	// This has foo and bar accounts already.
	s, _, _ := simpleAccountServer(t)

	c, cr, _ := newClientForServer(s)
	connectOp := []byte("CONNECT {\"account\":\"foo\",\"new_account\":true}\r\n")
	go c.parse(connectOp)
	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}

	// Now allow new accounts on the fly, make sure second time does not work.
	opts := defaultServerOptions
	opts.AllowNewAccounts = true
	s = New(&opts)

	c, _, _ = newClientForServer(s)
	err := c.parse(connectOp)
	if err != nil {
		t.Fatalf("Received an error trying to create an account: %v", err)
	}

	c, cr, _ = newClientForServer(s)
	go c.parse(connectOp)
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}
}

func accountNameExists(name string, accounts []*Account) bool {
	for _, acc := range accounts {
		if strings.Compare(acc.Name, name) == 0 {
			return true
		}
	}
	return false
}

func TestAccountSimpleConfig(t *testing.T) {
	confFileName := createConfFile(t, []byte(`accounts = [foo, bar]`))
	defer os.Remove(confFileName)
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received an error processing config file: %v", err)
	}
	if la := len(opts.Accounts); la != 2 {
		t.Fatalf("Expected to see 2 accounts in opts, got %d", la)
	}
	if !accountNameExists("foo", opts.Accounts) {
		t.Fatal("Expected a 'foo' account")
	}
	if !accountNameExists("bar", opts.Accounts) {
		t.Fatal("Expected a 'bar' account")
	}

	// Make sure double entries is an error.
	confFileName = createConfFile(t, []byte(`accounts = [foo, foo]`))
	defer os.Remove(confFileName)
	_, err = ProcessConfigFile(confFileName)
	if err == nil {
		t.Fatalf("Expected an error with double account entries")
	}
}

func TestAccountParseConfig(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
    accounts {
      synadia {
        users = [
          {user: alice, password: foo}
          {user: bob, password: bar}
        ]
      }
      nats.io {
        users = [
          {user: derek, password: foo}
          {user: ivan, password: bar}
        ]
      }
    }
    `))
	defer os.Remove(confFileName)
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received an error processing config file: %v", err)
	}

	if la := len(opts.Accounts); la != 2 {
		t.Fatalf("Expected to see 2 accounts in opts, got %d", la)
	}

	if lu := len(opts.Users); lu != 4 {
		t.Fatalf("Expected 4 total Users, got %d", lu)
	}

	var natsAcc *Account
	for _, acc := range opts.Accounts {
		if acc.Name == "nats.io" {
			natsAcc = acc
			break
		}
	}
	if natsAcc == nil {
		t.Fatalf("Error retrieving account for 'nats.io'")
	}

	for _, u := range opts.Users {
		if u.Username == "derek" {
			if u.Account != natsAcc {
				t.Fatalf("Expected to see the 'nats.io' account, but received %+v", u.Account)
			}
		}
	}
}

func TestAccountParseConfigDuplicateUsers(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
    accounts {
      synadia {
        users = [
          {user: alice, password: foo}
          {user: bob, password: bar}
        ]
      }
      nats.io {
        users = [
          {user: alice, password: bar}
        ]
      }
    }
    `))
	defer os.Remove(confFileName)
	_, err := ProcessConfigFile(confFileName)
	if err == nil {
		t.Fatalf("Expected an error with double user entries")
	}
}

func TestAccountParseConfigImportsExports(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/accounts.conf")
	if err != nil {
		t.Fatal(err)
	}
	if la := len(opts.Accounts); la != 3 {
		t.Fatalf("Expected to see 3 accounts in opts, got %d", la)
	}
	if lu := len(opts.Nkeys); lu != 4 {
		t.Fatalf("Expected 4 total Nkey users, got %d", lu)
	}
	if lu := len(opts.Users); lu != 0 {
		t.Fatalf("Expected no Users, got %d", lu)
	}
	var natsAcc, synAcc *Account
	for _, acc := range opts.Accounts {
		if acc.Name == "nats.io" {
			natsAcc = acc
		} else if acc.Name == "synadia" {
			synAcc = acc
		}
	}
	if natsAcc == nil {
		t.Fatalf("Error retrieving account for 'nats.io'")
	}
	if natsAcc.Nkey != "AB5UKNPVHDWBP5WODG742274I3OGY5FM3CBIFCYI4OFEH7Y23GNZPXFE" {
		t.Fatalf("Expected nats account to have an nkey, got %q\n", natsAcc.Nkey)
	}
	// Check user assigned to the correct account.
	for _, nk := range opts.Nkeys {
		if nk.Nkey == "UBRYMDSRTC6AVJL6USKKS3FIOE466GMEU67PZDGOWYSYHWA7GSKO42VW" {
			if nk.Account != natsAcc {
				t.Fatalf("Expected user to be associated with natsAcc, got %q\n", nk.Account.Name)
			}
			break
		}
	}

	// Now check for the imports and exports of streams and services.
	if lis := len(natsAcc.imports.streams); lis != 2 {
		t.Fatalf("Expected 2 imported streams, got %d\n", lis)
	}
	if lis := len(natsAcc.imports.services); lis != 1 {
		t.Fatalf("Expected 1 imported service, got %d\n", lis)
	}
	if les := len(natsAcc.exports.services); les != 4 {
		t.Fatalf("Expected 4 exported services, got %d\n", les)
	}
	if les := len(natsAcc.exports.streams); les != 0 {
		t.Fatalf("Expected no exported streams, got %d\n", les)
	}

	ea := natsAcc.exports.services["nats.time"]
	if ea == nil {
		t.Fatalf("Expected to get a non-nil exportAuth for service")
	}
	if ea.respType != Stream {
		t.Fatalf("Expected to get a Stream response type, got %q", ea.respType)
	}
	ea = natsAcc.exports.services["nats.photo"]
	if ea == nil {
		t.Fatalf("Expected to get a non-nil exportAuth for service")
	}
	if ea.respType != Chunked {
		t.Fatalf("Expected to get a Chunked response type, got %q", ea.respType)
	}
	ea = natsAcc.exports.services["nats.add"]
	if ea == nil {
		t.Fatalf("Expected to get a non-nil exportAuth for service")
	}
	if ea.respType != Singleton {
		t.Fatalf("Expected to get a Singleton response type, got %q", ea.respType)
	}

	if synAcc == nil {
		t.Fatalf("Error retrieving account for 'synadia'")
	}

	if lis := len(synAcc.imports.streams); lis != 0 {
		t.Fatalf("Expected no imported streams, got %d\n", lis)
	}
	if lis := len(synAcc.imports.services); lis != 1 {
		t.Fatalf("Expected 1 imported service, got %d\n", lis)
	}
	if les := len(synAcc.exports.services); les != 2 {
		t.Fatalf("Expected 2 exported service, got %d\n", les)
	}
	if les := len(synAcc.exports.streams); les != 2 {
		t.Fatalf("Expected 2 exported streams, got %d\n", les)
	}
}

func TestImportExportConfigFailures(t *testing.T) {
	// Import from unknow account
	cf := createConfFile(t, []byte(`
    accounts {
      nats.io {
        imports = [{stream: {account: "synadia", subject:"foo"}}]
      }
    }
    `))
	defer os.Remove(cf)
	if _, err := ProcessConfigFile(cf); err == nil {
		t.Fatalf("Expected an error with import from unknown account")
	}
	// Import a service with no account.
	cf = createConfFile(t, []byte(`
    accounts {
      nats.io {
        imports = [{service: subject:"foo.*"}]
      }
    }
    `))
	defer os.Remove(cf)
	if _, err := ProcessConfigFile(cf); err == nil {
		t.Fatalf("Expected an error with import of a service with no account")
	}
	// Import a service with a wildcard subject.
	cf = createConfFile(t, []byte(`
    accounts {
      nats.io {
        imports = [{service: {account: "nats.io", subject:"foo.*"}]
      }
    }
    `))
	defer os.Remove(cf)
	if _, err := ProcessConfigFile(cf); err == nil {
		t.Fatalf("Expected an error with import of a service with wildcard subject")
	}
	// Export with unknown keyword.
	cf = createConfFile(t, []byte(`
    accounts {
      nats.io {
        exports = [{service: "foo.*", wat:true}]
      }
    }
    `))
	defer os.Remove(cf)
	if _, err := ProcessConfigFile(cf); err == nil {
		t.Fatalf("Expected an error with export with unknown keyword")
	}
	// Import with unknown keyword.
	cf = createConfFile(t, []byte(`
    accounts {
      nats.io {
        imports = [{stream: {account: nats.io, subject: "foo.*"}, wat:true}]
      }
    }
    `))
	defer os.Remove(cf)
	if _, err := ProcessConfigFile(cf); err == nil {
		t.Fatalf("Expected an error with import with unknown keyword")
	}
	// Export with an account.
	cf = createConfFile(t, []byte(`
    accounts {
      nats.io {
        exports = [{service: {account: nats.io, subject:"foo.*"}}]
      }
    }
    `))
	defer os.Remove(cf)
	if _, err := ProcessConfigFile(cf); err == nil {
		t.Fatalf("Expected an error with export with account")
	}
}

func TestImportAuthorized(t *testing.T) {
	_, foo, bar := simpleAccountServer(t)

	checkBool(foo.checkStreamImportAuthorized(bar, "foo", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, ">", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.*", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.>", nil), false, t)

	foo.AddStreamExport("foo", IsPublicExport)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "bar", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*", nil), false, t)

	foo.AddStreamExport("*", []*Account{bar})
	checkBool(foo.checkStreamImportAuthorized(bar, "foo", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "bar", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "baz", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.bar", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, ">", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.*", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*.*", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*.>", nil), false, t)

	// Reset and test '>' public export
	_, foo, bar = simpleAccountServer(t)
	foo.AddStreamExport(">", nil)
	// Everything should work.
	checkBool(foo.checkStreamImportAuthorized(bar, "foo", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "bar", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "baz", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.bar", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, ">", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.*", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*.*", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*.>", nil), true, t)

	// Reset and test pwc and fwc
	s, foo, bar := simpleAccountServer(t)
	foo.AddStreamExport("foo.*.baz.>", []*Account{bar})
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.bar.baz.1", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.bar.baz.*", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.*.baz.1.1", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.22.baz.22", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.bar.baz", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.bar.*.*", nil), false, t)

	// Make sure we match the account as well

	fb, _ := s.RegisterAccount("foobar")
	bz, _ := s.RegisterAccount("baz")

	checkBool(foo.checkStreamImportAuthorized(fb, "foo.bar.baz.1", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bz, "foo.bar.baz.1", nil), false, t)
}

func TestSimpleMapping(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, _, _ := newClientForServer(s)
	defer cfoo.nc.Close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	cbar, crBar, _ := newClientForServer(s)
	defer cbar.nc.Close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	// Test first that trying to import with no matching export permission returns an error.
	if err := cbar.acc.AddStreamImport(fooAcc, "foo", "import"); err != ErrStreamImportAuthorization {
		t.Fatalf("Expected error of ErrAccountImportAuthorization but got %v", err)
	}

	// Now map the subject space between foo and bar.
	// Need to do export first.
	if err := cfoo.acc.AddStreamExport("foo", nil); err != nil { // Public with no accounts defined.
		t.Fatalf("Error adding account export to client foo: %v", err)
	}
	if err := cbar.acc.AddStreamImport(fooAcc, "foo", "import"); err != nil {
		t.Fatalf("Error adding account import to client bar: %v", err)
	}

	// Normal and Queue Subscription on bar client.
	if err := cbar.parse([]byte("SUB import.foo 1\r\nSUB import.foo bar 2\r\n")); err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}

	// Now publish our message.
	go cfoo.parseAndFlush([]byte("PUB foo 5\r\nhello\r\n"))

	checkMsg := func(l, sid string) {
		t.Helper()
		mraw := msgPat.FindAllStringSubmatch(l, -1)
		if len(mraw) == 0 {
			t.Fatalf("No message received")
		}
		matches := mraw[0]
		if matches[SUB_INDEX] != "import.foo" {
			t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
		}
		if matches[SID_INDEX] != sid {
			t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
		}
	}

	// Now check we got the message from normal subscription.
	l, err := crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	checkMsg(l, "1")
	checkPayload(crBar, []byte("hello\r\n"), t)

	l, err = crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	checkMsg(l, "2")
	checkPayload(crBar, []byte("hello\r\n"), t)

	// We should have 2 subscriptions in both. Normal and Queue Subscriber
	// for barAcc which are local, and 2 that are shadowed in fooAcc.
	// Now make sure that when we unsubscribe we clean up properly for both.
	if bslc := barAcc.sl.Count(); bslc != 2 {
		t.Fatalf("Expected 2 normal subscriptions on barAcc, got %d", bslc)
	}
	if fslc := fooAcc.sl.Count(); fslc != 2 {
		t.Fatalf("Expected 2 shadowed subscriptions on fooAcc, got %d", fslc)
	}

	// Now unsubscribe.
	if err := cbar.parse([]byte("UNSUB 1\r\nUNSUB 2\r\n")); err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}

	// We should have zero on both.
	if bslc := barAcc.sl.Count(); bslc != 0 {
		t.Fatalf("Expected no normal subscriptions on barAcc, got %d", bslc)
	}
	if fslc := fooAcc.sl.Count(); fslc != 0 {
		t.Fatalf("Expected no shadowed subscriptions on fooAcc, got %d", fslc)
	}
}

func TestShadowSubsCleanupOnClientClose(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	// Now map the subject space between foo and bar.
	// Need to do export first.
	if err := fooAcc.AddStreamExport("foo", nil); err != nil { // Public with no accounts defined.
		t.Fatalf("Error adding account export to client foo: %v", err)
	}
	if err := barAcc.AddStreamImport(fooAcc, "foo", "import"); err != nil {
		t.Fatalf("Error adding account import to client bar: %v", err)
	}

	cbar, _, _ := newClientForServer(s)
	defer cbar.nc.Close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	// Normal and Queue Subscription on bar client.
	if err := cbar.parse([]byte("SUB import.foo 1\r\nSUB import.foo bar 2\r\n")); err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}

	if fslc := fooAcc.sl.Count(); fslc != 2 {
		t.Fatalf("Expected 2 shadowed subscriptions on fooAcc, got %d", fslc)
	}

	// Now close cbar and make sure we remove shadows.
	cbar.closeConnection(ClientClosed)

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if fslc := fooAcc.sl.Count(); fslc != 0 {
			return fmt.Errorf("Number of shadow subscriptions is %d", fslc)
		}
		return nil
	})
}

func TestNoPrefixWildcardMapping(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, _, _ := newClientForServer(s)
	defer cfoo.nc.Close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	cbar, crBar, _ := newClientForServer(s)
	defer cbar.nc.Close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	if err := cfoo.acc.AddStreamExport(">", []*Account{barAcc}); err != nil {
		t.Fatalf("Error adding stream export to client foo: %v", err)
	}
	if err := cbar.acc.AddStreamImport(fooAcc, "*", ""); err != nil {
		t.Fatalf("Error adding stream import to client bar: %v", err)
	}

	// Normal Subscription on bar client for literal "foo".
	go cbar.parse([]byte("SUB foo 1\r\nPING\r\n"))
	_, err := crBar.ReadString('\n') // Make sure subscriptions were processed.
	if err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}

	// Now publish our message.
	go cfoo.parseAndFlush([]byte("PUB foo 5\r\nhello\r\n"))

	// Now check we got the message from normal subscription.
	l, err := crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	mraw := msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches := mraw[0]
	if matches[SUB_INDEX] != "foo" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	checkPayload(crBar, []byte("hello\r\n"), t)
}

func TestPrefixWildcardMapping(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, _, _ := newClientForServer(s)
	defer cfoo.nc.Close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	cbar, crBar, _ := newClientForServer(s)
	defer cbar.nc.Close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	if err := cfoo.acc.AddStreamExport(">", []*Account{barAcc}); err != nil {
		t.Fatalf("Error adding stream export to client foo: %v", err)
	}
	// Checking that trailing '.' is accepted, tested that it is auto added above.
	if err := cbar.acc.AddStreamImport(fooAcc, "*", "pub.imports."); err != nil {
		t.Fatalf("Error adding stream import to client bar: %v", err)
	}

	// Normal Subscription on bar client for wildcard.
	go cbar.parse([]byte("SUB pub.imports.* 1\r\nPING\r\n"))
	_, err := crBar.ReadString('\n') // Make sure subscriptions were processed.
	if err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}

	// Now publish our message.
	go cfoo.parseAndFlush([]byte("PUB foo 5\r\nhello\r\n"))

	// Now check we got the messages from wildcard subscription.
	l, err := crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	mraw := msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches := mraw[0]
	if matches[SUB_INDEX] != "pub.imports.foo" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	checkPayload(crBar, []byte("hello\r\n"), t)
}

func TestPrefixWildcardMappingWithLiteralSub(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, _, _ := newClientForServer(s)
	defer cfoo.nc.Close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	cbar, crBar, _ := newClientForServer(s)
	defer cbar.nc.Close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	if err := fooAcc.AddStreamExport(">", []*Account{barAcc}); err != nil {
		t.Fatalf("Error adding stream export to client foo: %v", err)
	}
	if err := barAcc.AddStreamImport(fooAcc, "*", "pub.imports."); err != nil {
		t.Fatalf("Error adding stream import to client bar: %v", err)
	}

	// Normal Subscription on bar client for wildcard.
	go cbar.parse([]byte("SUB pub.imports.foo 1\r\nPING\r\n"))
	_, err := crBar.ReadString('\n') // Make sure subscriptions were processed.
	if err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}

	// Now publish our message.
	go cfoo.parseAndFlush([]byte("PUB foo 5\r\nhello\r\n"))

	// Now check we got the messages from wildcard subscription.
	l, err := crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	mraw := msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches := mraw[0]
	if matches[SUB_INDEX] != "pub.imports.foo" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	checkPayload(crBar, []byte("hello\r\n"), t)
}

func TestMultipleImportsAndSingleWCSub(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, _, _ := newClientForServer(s)
	defer cfoo.nc.Close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	cbar, crBar, _ := newClientForServer(s)
	defer cbar.nc.Close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	if err := fooAcc.AddStreamExport("foo", []*Account{barAcc}); err != nil {
		t.Fatalf("Error adding stream export to account foo: %v", err)
	}
	if err := fooAcc.AddStreamExport("bar", []*Account{barAcc}); err != nil {
		t.Fatalf("Error adding stream export to account foo: %v", err)
	}

	if err := barAcc.AddStreamImport(fooAcc, "foo", "pub."); err != nil {
		t.Fatalf("Error adding stream import to account bar: %v", err)
	}
	if err := barAcc.AddStreamImport(fooAcc, "bar", "pub."); err != nil {
		t.Fatalf("Error adding stream import to account bar: %v", err)
	}

	// Wildcard Subscription on bar client for both imports.
	cbar.parse([]byte("SUB pub.* 1\r\n"))

	// Now publish a message on 'foo' and 'bar'
	go cfoo.parseAndFlush([]byte("PUB foo 5\r\nhello\r\nPUB bar 5\r\nworld\r\n"))

	// Now check we got the messages from the wildcard subscription.
	l, err := crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	mraw := msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches := mraw[0]
	if matches[SUB_INDEX] != "pub.foo" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	checkPayload(crBar, []byte("hello\r\n"), t)

	l, err = crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	mraw = msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches = mraw[0]
	if matches[SUB_INDEX] != "pub.bar" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	checkPayload(crBar, []byte("world\r\n"), t)

	// Check subscription count.
	if fslc := fooAcc.sl.Count(); fslc != 2 {
		t.Fatalf("Expected 2 shadowed subscriptions on fooAcc, got %d", fslc)
	}
	if bslc := barAcc.sl.Count(); bslc != 1 {
		t.Fatalf("Expected 1 normal subscriptions on barAcc, got %d", bslc)
	}

	// Now unsubscribe.
	if err := cbar.parse([]byte("UNSUB 1\r\n")); err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}
	// We should have zero on both.
	if bslc := barAcc.sl.Count(); bslc != 0 {
		t.Fatalf("Expected no normal subscriptions on barAcc, got %d", bslc)
	}
	if fslc := fooAcc.sl.Count(); fslc != 0 {
		t.Fatalf("Expected no shadowed subscriptions on fooAcc, got %d", fslc)
	}
}

// Make sure the AddServiceExport function is additive if called multiple times.
func TestAddServiceExport(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	bazAcc, err := s.RegisterAccount("$baz")
	if err != nil {
		t.Fatalf("Error creating account 'baz': %v", err)
	}
	defer s.Shutdown()

	if err := fooAcc.AddServiceExport("test.request", nil); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	tr := fooAcc.exports.services["test.request"]
	if tr != nil {
		t.Fatalf("Expected no authorized accounts, got %d", len(tr.approved))
	}
	if err := fooAcc.AddServiceExport("test.request", []*Account{barAcc}); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	tr = fooAcc.exports.services["test.request"]
	if tr == nil {
		t.Fatalf("Expected authorized accounts, got nil")
	}
	if ls := len(tr.approved); ls != 1 {
		t.Fatalf("Expected 1 authorized accounts, got %d", ls)
	}
	if err := fooAcc.AddServiceExport("test.request", []*Account{bazAcc}); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	tr = fooAcc.exports.services["test.request"]
	if tr == nil {
		t.Fatalf("Expected authorized accounts, got nil")
	}
	if ls := len(tr.approved); ls != 2 {
		t.Fatalf("Expected 2 authorized accounts, got %d", ls)
	}
}

func TestServiceExportWithWildcards(t *testing.T) {
	for _, test := range []struct {
		name   string
		public bool
	}{
		{
			name:   "public",
			public: true,
		},
		{
			name:   "private",
			public: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, fooAcc, barAcc := simpleAccountServer(t)
			defer s.Shutdown()

			var accs []*Account
			if !test.public {
				accs = []*Account{barAcc}
			}
			// Add service export with a wildcard
			if err := fooAcc.AddServiceExport("ngs.update.*", accs); err != nil {
				t.Fatalf("Error adding account service export: %v", err)
			}
			// Import on bar account
			if err := barAcc.AddServiceImport(fooAcc, "ngs.update", "ngs.update.$bar"); err != nil {
				t.Fatalf("Error adding account service import: %v", err)
			}

			cfoo, crFoo, _ := newClientForServer(s)
			defer cfoo.nc.Close()

			if err := cfoo.registerWithAccount(fooAcc); err != nil {
				t.Fatalf("Error registering client with 'foo' account: %v", err)
			}
			cbar, crBar, _ := newClientForServer(s)
			defer cbar.nc.Close()

			if err := cbar.registerWithAccount(barAcc); err != nil {
				t.Fatalf("Error registering client with 'bar' account: %v", err)
			}

			// Now setup the resonder under cfoo
			cfoo.parse([]byte("SUB ngs.update.* 1\r\n"))

			// Now send the request. Remember we expect the request on our local ngs.update.
			// We added the route with that "from" and will map it to "ngs.update.$bar"
			go cbar.parseAndFlush([]byte("SUB reply 11\r\nPUB ngs.update reply 4\r\nhelp\r\n"))

			// Now read the request from crFoo
			l, err := crFoo.ReadString('\n')
			if err != nil {
				t.Fatalf("Error reading from client 'bar': %v", err)
			}

			mraw := msgPat.FindAllStringSubmatch(l, -1)
			if len(mraw) == 0 {
				t.Fatalf("No message received")
			}
			matches := mraw[0]
			if matches[SUB_INDEX] != "ngs.update.$bar" {
				t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
			}
			if matches[SID_INDEX] != "1" {
				t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
			}
			// Make sure this looks like _INBOX
			if !strings.HasPrefix(matches[REPLY_INDEX], "_R_.") {
				t.Fatalf("Expected an _R_.* like reply, got '%s'", matches[REPLY_INDEX])
			}
			checkPayload(crFoo, []byte("help\r\n"), t)

			replyOp := fmt.Sprintf("PUB %s 2\r\n22\r\n", matches[REPLY_INDEX])
			go cfoo.parseAndFlush([]byte(replyOp))

			// Now read the response from crBar
			l, err = crBar.ReadString('\n')
			if err != nil {
				t.Fatalf("Error reading from client 'bar': %v", err)
			}
			mraw = msgPat.FindAllStringSubmatch(l, -1)
			if len(mraw) == 0 {
				t.Fatalf("No message received")
			}
			matches = mraw[0]
			if matches[SUB_INDEX] != "reply" {
				t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
			}
			if matches[SID_INDEX] != "11" {
				t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
			}
			if matches[REPLY_INDEX] != "" {
				t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
			}
			checkPayload(crBar, []byte("22\r\n"), t)

			// Make sure we have no service imports on fooAcc. An implicit one was created
			// for the response but should be removed when the response was processed.
			if nr := fooAcc.numServiceRoutes(); nr != 0 {
				t.Fatalf("Expected no remaining routes on fooAcc, got %d", nr)
			}
		})
	}
}

// Make sure the AddStreamExport function is additive if called multiple times.
func TestAddStreamExport(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	bazAcc, err := s.RegisterAccount("$baz")
	if err != nil {
		t.Fatalf("Error creating account 'baz': %v", err)
	}
	defer s.Shutdown()

	if err := fooAcc.AddStreamExport("test.request", nil); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	tr := fooAcc.exports.streams["test.request"]
	if tr != nil {
		t.Fatalf("Expected no authorized accounts, got %d", len(tr.approved))
	}
	if err := fooAcc.AddStreamExport("test.request", []*Account{barAcc}); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	tr = fooAcc.exports.streams["test.request"]
	if tr == nil {
		t.Fatalf("Expected authorized accounts, got nil")
	}
	if ls := len(tr.approved); ls != 1 {
		t.Fatalf("Expected 1 authorized accounts, got %d", ls)
	}
	if err := fooAcc.AddStreamExport("test.request", []*Account{bazAcc}); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	tr = fooAcc.exports.streams["test.request"]
	if tr == nil {
		t.Fatalf("Expected authorized accounts, got nil")
	}
	if ls := len(tr.approved); ls != 2 {
		t.Fatalf("Expected 2 authorized accounts, got %d", ls)
	}
}

func TestCrossAccountRequestReply(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, crFoo, _ := newClientForServer(s)
	defer cfoo.nc.Close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	cbar, crBar, _ := newClientForServer(s)
	defer cbar.nc.Close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	// Add in the service export for the requests. Make it public.
	if err := cfoo.acc.AddServiceExport("test.request", nil); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}

	// Test addServiceImport to make sure it requires accounts, and literalsubjects for both from and to subjects.
	if err := cbar.acc.AddServiceImport(nil, "foo", "test.request"); err != ErrMissingAccount {
		t.Fatalf("Expected ErrMissingAccount but received %v.", err)
	}
	if err := cbar.acc.AddServiceImport(fooAcc, "*", "test.request"); err != ErrInvalidSubject {
		t.Fatalf("Expected ErrInvalidSubject but received %v.", err)
	}
	if err := cbar.acc.AddServiceImport(fooAcc, "foo", "test..request."); err != ErrInvalidSubject {
		t.Fatalf("Expected ErrInvalidSubject but received %v.", err)
	}

	// Now add in the route mapping for request to be routed to the foo account.
	if err := cbar.acc.AddServiceImport(fooAcc, "foo", "test.request"); err != nil {
		t.Fatalf("Error adding account service import to client bar: %v", err)
	}

	// Now setup the resonder under cfoo
	cfoo.parse([]byte("SUB test.request 1\r\n"))

	// Now send the request. Remember we expect the request on our local foo. We added the route
	// with that "from" and will map it to "test.request"
	go cbar.parseAndFlush([]byte("SUB bar 11\r\nPUB foo bar 4\r\nhelp\r\n"))

	// Now read the request from crFoo
	l, err := crFoo.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}

	mraw := msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches := mraw[0]
	if matches[SUB_INDEX] != "test.request" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	// Make sure this looks like _INBOX
	if !strings.HasPrefix(matches[REPLY_INDEX], "_R_.") {
		t.Fatalf("Expected an _R_.* like reply, got '%s'", matches[REPLY_INDEX])
	}
	checkPayload(crFoo, []byte("help\r\n"), t)

	replyOp := fmt.Sprintf("PUB %s 2\r\n22\r\n", matches[REPLY_INDEX])
	go cfoo.parseAndFlush([]byte(replyOp))

	// Now read the response from crBar
	l, err = crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	mraw = msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches = mraw[0]
	if matches[SUB_INDEX] != "bar" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "11" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	if matches[REPLY_INDEX] != "" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	checkPayload(crBar, []byte("22\r\n"), t)

	// Make sure we have no service imports on fooAcc. An implicit one was created
	// for the response but should be removed when the response was processed.
	if nr := fooAcc.numServiceRoutes(); nr != 0 {
		t.Fatalf("Expected no remaining routes on fooAcc, got %d", nr)
	}
}

func TestCrossAccountRequestReplyResponseMaps(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	// Make sure they have the correct defaults
	if max := barAcc.MaxAutoExpireResponseMaps(); max != DEFAULT_MAX_ACCOUNT_AE_RESPONSE_MAPS {
		t.Fatalf("Expected %d for max default, but got %d", DEFAULT_MAX_ACCOUNT_AE_RESPONSE_MAPS, max)
	}

	if ttl := barAcc.AutoExpireTTL(); ttl != DEFAULT_TTL_AE_RESPONSE_MAP {
		t.Fatalf("Expected %v for the ttl default, got %v", DEFAULT_TTL_AE_RESPONSE_MAP, ttl)
	}

	ttl := 500 * time.Millisecond
	barAcc.SetMaxAutoExpireResponseMaps(5)
	barAcc.SetAutoExpireTTL(ttl)
	cfoo, _, _ := newClientForServer(s)
	defer cfoo.nc.Close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}

	if err := barAcc.AddServiceExport("test.request", nil); err != nil {
		t.Fatalf("Error adding account service export: %v", err)
	}
	if err := fooAcc.AddServiceImport(barAcc, "foo", "test.request"); err != nil {
		t.Fatalf("Error adding account service import: %v", err)
	}

	for i := 0; i < 10; i++ {
		cfoo.parseAndFlush([]byte("PUB foo bar 4\r\nhelp\r\n"))
	}

	// We should expire because of max.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if nae := barAcc.numAutoExpireResponseMaps(); nae != 5 {
			return fmt.Errorf("Number of responsemaps is %d", nae)
		}
		return nil
	})

	// Wait for the ttl to expire.
	time.Sleep(2 * ttl)

	// Now run prune and make sure we collect the timed-out ones.
	barAcc.pruneAutoExpireResponseMaps()

	// We should expire because ttl.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if nae := barAcc.numAutoExpireResponseMaps(); nae != 0 {
			return fmt.Errorf("Number of responsemaps is %d", nae)
		}
		return nil
	})
}

func TestCrossAccountServiceResponseTypes(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, crFoo, _ := newClientForServer(s)
	defer cfoo.nc.Close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	cbar, crBar, _ := newClientForServer(s)
	defer cbar.nc.Close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	// Add in the service export for the requests. Make it public.
	if err := cfoo.acc.AddServiceExportWithResponse("test.request", Stream, nil); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	// Now add in the route mapping for request to be routed to the foo account.
	if err := cbar.acc.AddServiceImport(fooAcc, "foo", "test.request"); err != nil {
		t.Fatalf("Error adding account service import to client bar: %v", err)
	}

	// Now setup the resonder under cfoo
	cfoo.parse([]byte("SUB test.request 1\r\n"))

	// Now send the request. Remember we expect the request on our local foo. We added the route
	// with that "from" and will map it to "test.request"
	go cbar.parseAndFlush([]byte("SUB bar 11\r\nPUB foo bar 4\r\nhelp\r\n"))

	// Now read the request from crFoo
	l, err := crFoo.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}

	mraw := msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches := mraw[0]
	reply := matches[REPLY_INDEX]
	if !strings.HasPrefix(reply, "_R_.") {
		t.Fatalf("Expected an _R_.* like reply, got '%s'", reply)
	}
	crFoo.ReadString('\n')

	replyOp := []byte(fmt.Sprintf("PUB %s 2\r\n22\r\n", matches[REPLY_INDEX]))
	var mReply []byte
	for i := 0; i < 10; i++ {
		mReply = append(mReply, replyOp...)
	}

	go cfoo.parseAndFlush(mReply)

	var b [256]byte
	n, err := crBar.Read(b[:])
	if err != nil {
		t.Fatalf("Error reading response: %v", err)
	}
	mraw = msgPat.FindAllStringSubmatch(string(b[:n]), -1)
	if len(mraw) != 10 {
		t.Fatalf("Expected a response but got %d", len(mraw))
	}

	// Also make sure the response map gets cleaned up when interest goes away.
	cbar.closeConnection(ClientClosed)

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if nr := fooAcc.numServiceRoutes(); nr != 0 {
			return fmt.Errorf("Number of implicit service imports is %d", nr)
		}
		return nil
	})

	// Now test bogus reply subjects are handled and do not accumulate the response maps.

	cbar, _, _ = newClientForServer(s)
	defer cbar.nc.Close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	// Do not create any interest in the reply subject 'bar'. Just send a request.
	go cbar.parseAndFlush([]byte("PUB foo bar 4\r\nhelp\r\n"))

	// Now read the request from crFoo
	l, err = crFoo.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	mraw = msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches = mraw[0]
	reply = matches[REPLY_INDEX]
	if !strings.HasPrefix(reply, "_R_.") {
		t.Fatalf("Expected an _R_.* like reply, got '%s'", reply)
	}
	crFoo.ReadString('\n')

	replyOp = []byte(fmt.Sprintf("PUB %s 2\r\n22\r\n", matches[REPLY_INDEX]))

	// Make sure we have the response map.
	if nr := fooAcc.numServiceRoutes(); nr != 1 {
		t.Fatalf("Expected a response map to be present, got %d", nr)
	}

	go cfoo.parseAndFlush(replyOp)

	// Now wait for a bit, the reply should trip a no interest condition
	// which should clean this up.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if nr := fooAcc.numServiceRoutes(); nr != 0 {
			return fmt.Errorf("Number of implicit service imports is %d", nr)
		}
		return nil
	})

	// Also make sure the response map entry is gone as well.
	barAcc.mu.RLock()
	lrm := len(barAcc.respMap)
	barAcc.mu.RUnlock()

	if lrm != 0 {
		t.Fatalf("Expected the respMap tp be cleared, got %d entries", lrm)
	}
}

// This is for bogus reply subjects and no responses from a service provider.
func TestCrossAccountServiceResponseLeaks(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	// Set max response maps to < 100
	barAcc.SetMaxResponseMaps(99)

	cfoo, crFoo, _ := newClientForServer(s)
	defer cfoo.nc.Close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	cbar, _, _ := newClientForServer(s)
	defer cbar.nc.Close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	// Add in the service export for the requests. Make it public.
	if err := cfoo.acc.AddServiceExportWithResponse("test.request", Stream, nil); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	// Now add in the route mapping for request to be routed to the foo account.
	if err := cbar.acc.AddServiceImport(fooAcc, "foo", "test.request"); err != nil {
		t.Fatalf("Error adding account service import to client bar: %v", err)
	}

	// Now setup the resonder under cfoo
	cfoo.parse([]byte("SUB test.request 1\r\n"))

	// Now send some requests..We will not respond.
	var sb strings.Builder
	for i := 0; i < 100; i++ {
		sb.WriteString(fmt.Sprintf("PUB foo REPLY.%d 4\r\nhelp\r\n", i))
	}
	go cbar.parseAndFlush([]byte(sb.String()))

	// Make sure requests are processed.
	_, err := crFoo.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}

	// We should have leaked response maps.
	if nr := fooAcc.numServiceRoutes(); nr != 100 {
		t.Fatalf("Expected response maps to be present, got %d", nr)
	}

	// They should be gone here eventually.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if nr := fooAcc.numServiceRoutes(); nr != 0 {
			return fmt.Errorf("Number of implicit service imports is %d", nr)
		}
		return nil
	})

	// Also make sure the response map entry is gone as well.
	barAcc.mu.RLock()
	lrm := len(barAcc.respMap)
	barAcc.mu.RUnlock()

	if lrm != 0 {
		t.Fatalf("Expected the respMap tp be cleared, got %d entries", lrm)
	}
}

func TestAccountMapsUsers(t *testing.T) {
	// Used for the nkey users to properly sign.
	seed1 := "SUAPM67TC4RHQLKBX55NIQXSMATZDOZK6FNEOSS36CAYA7F7TY66LP4BOM"
	seed2 := "SUAIS5JPX4X4GJ7EIIJEQ56DH2GWPYJRPWN5XJEDENJOZHCBLI7SEPUQDE"

	confFileName := createConfFile(t, []byte(`
    accounts {
      synadia {
        users = [
          {user: derek, password: foo},
          {nkey: UCNGL4W5QX66CFX6A6DCBVDH5VOHMI7B2UZZU7TXAUQQSI2JPHULCKBR}
        ]
      }
      nats {
        users = [
          {user: ivan, password: bar},
          {nkey: UDPGQVFIWZ7Q5UH4I5E6DBCZULQS6VTVBG6CYBD7JV3G3N2GMQOMNAUH}
        ]
      }
    }
    `))
	defer os.Remove(confFileName)
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Unexpected error parsing config file: %v", err)
	}
	s := New(opts)
	synadia, _ := s.LookupAccount("synadia")
	nats, _ := s.LookupAccount("nats")

	if synadia == nil || nats == nil {
		t.Fatalf("Expected non nil accounts during lookup")
	}

	// Make sure a normal log in maps the accounts correctly.
	c, _, _ := newClientForServer(s)
	connectOp := []byte("CONNECT {\"user\":\"derek\",\"pass\":\"foo\"}\r\n")
	c.parse(connectOp)
	if c.acc != synadia {
		t.Fatalf("Expected the client's account to match 'synadia', got %v", c.acc)
	}

	c, _, _ = newClientForServer(s)
	connectOp = []byte("CONNECT {\"user\":\"ivan\",\"pass\":\"bar\"}\r\n")
	c.parse(connectOp)
	if c.acc != nats {
		t.Fatalf("Expected the client's account to match 'nats', got %v", c.acc)
	}

	// Now test nkeys as well.
	kp, _ := nkeys.FromSeed([]byte(seed1))
	pubKey, _ := kp.PublicKey()

	c, cr, l := newClientForServer(s)
	// Check for Nonce
	var info nonceInfo
	err = json.Unmarshal([]byte(l[5:]), &info)
	if err != nil {
		t.Fatalf("Could not parse INFO json: %v\n", err)
	}
	if info.Nonce == "" {
		t.Fatalf("Expected a non-empty nonce with nkeys defined")
	}
	sigraw, err := kp.Sign([]byte(info.Nonce))
	if err != nil {
		t.Fatalf("Failed signing nonce: %v", err)
	}
	sig := base64.RawURLEncoding.EncodeToString(sigraw)

	// PING needed to flush the +OK to us.
	cs := fmt.Sprintf("CONNECT {\"nkey\":%q,\"sig\":\"%s\",\"verbose\":true,\"pedantic\":true}\r\nPING\r\n", pubKey, sig)
	go c.parse([]byte(cs))
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "+OK") {
		t.Fatalf("Expected an OK, got: %v", l)
	}
	if c.acc != synadia {
		t.Fatalf("Expected the nkey client's account to match 'synadia', got %v", c.acc)
	}

	// Now nats account nkey user.
	kp, _ = nkeys.FromSeed([]byte(seed2))
	pubKey, _ = kp.PublicKey()

	c, cr, l = newClientForServer(s)
	// Check for Nonce
	err = json.Unmarshal([]byte(l[5:]), &info)
	if err != nil {
		t.Fatalf("Could not parse INFO json: %v\n", err)
	}
	if info.Nonce == "" {
		t.Fatalf("Expected a non-empty nonce with nkeys defined")
	}
	sigraw, err = kp.Sign([]byte(info.Nonce))
	if err != nil {
		t.Fatalf("Failed signing nonce: %v", err)
	}
	sig = base64.RawURLEncoding.EncodeToString(sigraw)

	// PING needed to flush the +OK to us.
	cs = fmt.Sprintf("CONNECT {\"nkey\":%q,\"sig\":\"%s\",\"verbose\":true,\"pedantic\":true}\r\nPING\r\n", pubKey, sig)
	go c.parse([]byte(cs))
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "+OK") {
		t.Fatalf("Expected an OK, got: %v", l)
	}
	if c.acc != nats {
		t.Fatalf("Expected the nkey client's account to match 'nats', got %v", c.acc)
	}
}

func TestAccountGlobalDefault(t *testing.T) {
	opts := defaultServerOptions
	s := New(&opts)

	if acc, _ := s.LookupAccount(globalAccountName); acc == nil {
		t.Fatalf("Expected a global default account on a new server, got none.")
	}
	// Make sure we can not create one with same name..
	if _, err := s.RegisterAccount(globalAccountName); err == nil {
		t.Fatalf("Expected error trying to create a new reserved account")
	}

	// Make sure we can not define one in a config file either.
	confFileName := createConfFile(t, []byte(`accounts { $G {} }`))
	defer os.Remove(confFileName)

	if _, err := ProcessConfigFile(confFileName); err == nil {
		t.Fatalf("Expected an error parsing config file with reserved account")
	}
}

func TestAccountCheckStreamImportsEqual(t *testing.T) {
	// Create bare accounts for this test
	fooAcc := NewAccount("foo")
	if err := fooAcc.AddStreamExport(">", nil); err != nil {
		t.Fatalf("Error adding stream export: %v", err)
	}

	barAcc := NewAccount("bar")
	if err := barAcc.AddStreamImport(fooAcc, "foo", "myPrefix"); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	bazAcc := NewAccount("baz")
	if err := bazAcc.AddStreamImport(fooAcc, "foo", "myPrefix"); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if !barAcc.checkStreamImportsEqual(bazAcc) {
		t.Fatal("Expected stream imports to be the same")
	}

	if err := bazAcc.AddStreamImport(fooAcc, "foo.>", ""); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if barAcc.checkStreamImportsEqual(bazAcc) {
		t.Fatal("Expected stream imports to be different")
	}
	if err := barAcc.AddStreamImport(fooAcc, "foo.>", ""); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if !barAcc.checkStreamImportsEqual(bazAcc) {
		t.Fatal("Expected stream imports to be the same")
	}

	// Create another account that is named "foo". We want to make sure
	// that the comparison still works (based on account name, not pointer)
	newFooAcc := NewAccount("foo")
	if err := newFooAcc.AddStreamExport(">", nil); err != nil {
		t.Fatalf("Error adding stream export: %v", err)
	}
	batAcc := NewAccount("bat")
	if err := batAcc.AddStreamImport(newFooAcc, "foo", "myPrefix"); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if err := batAcc.AddStreamImport(newFooAcc, "foo.>", ""); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if !batAcc.checkStreamImportsEqual(barAcc) {
		t.Fatal("Expected stream imports to be the same")
	}
	if !batAcc.checkStreamImportsEqual(bazAcc) {
		t.Fatal("Expected stream imports to be the same")
	}

	// Test with account with different "from"
	expAcc := NewAccount("new_acc")
	if err := expAcc.AddStreamExport(">", nil); err != nil {
		t.Fatalf("Error adding stream export: %v", err)
	}
	aAcc := NewAccount("a")
	if err := aAcc.AddStreamImport(expAcc, "bar", ""); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	bAcc := NewAccount("b")
	if err := bAcc.AddStreamImport(expAcc, "baz", ""); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if aAcc.checkStreamImportsEqual(bAcc) {
		t.Fatal("Expected stream imports to be different")
	}

	// Test with account with different "prefix"
	aAcc = NewAccount("a")
	if err := aAcc.AddStreamImport(expAcc, "bar", "prefix"); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	bAcc = NewAccount("b")
	if err := bAcc.AddStreamImport(expAcc, "bar", "diff_prefix"); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if aAcc.checkStreamImportsEqual(bAcc) {
		t.Fatal("Expected stream imports to be different")
	}

	// Test with account with different "name"
	expAcc = NewAccount("diff_name")
	if err := expAcc.AddStreamExport(">", nil); err != nil {
		t.Fatalf("Error adding stream export: %v", err)
	}
	bAcc = NewAccount("b")
	if err := bAcc.AddStreamImport(expAcc, "bar", "prefix"); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if aAcc.checkStreamImportsEqual(bAcc) {
		t.Fatal("Expected stream imports to be different")
	}
}

func BenchmarkNewRouteReply(b *testing.B) {
	opts := defaultServerOptions
	s := New(&opts)
	c, _, _ := newClientForServer(s)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.newServiceReply()
	}
}
