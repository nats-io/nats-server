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
	"os"
	"strings"
	"testing"
)

func simpleAccountServer(t *testing.T) (*Server, *Account, *Account) {
	opts := defaultServerOptions
	s := New(&opts)

	// Now create two accounts.
	f, err := s.RegisterAccount("foo")
	if err != nil {
		t.Fatalf("Error creating account 'foo': %v", err)
	}
	b, err := s.RegisterAccount("bar")
	if err != nil {
		t.Fatalf("Error creating account 'bar': %v", err)
	}
	return s, f, b
}

func TestRegisterDuplicateAccounts(t *testing.T) {
	s, _, _ := simpleAccountServer(t)
	if _, err := s.RegisterAccount("foo"); err == nil {
		t.Fatal("Expected an error registering 'foo' twice")
	}
}

func TestAccountIsolation(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	if fooAcc == nil || barAcc == nil {
		t.Fatalf("Error retrieving accounts for 'foo' and 'bar'")
	}
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
		t.Fatalf("PONG response incorrect: %q\n", l)
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
		t.Fatalf("Did not get correct sid: '%s'\n", matches[SID_INDEX])
	}
	checkPayload(crFoo, []byte("hello\r\n"), t)

	// Now make sure nothing shows up on bar.
	l, err = crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}
	if !strings.HasPrefix(l, "PONG\r\n") {
		t.Fatalf("PONG response incorrect: %q\n", l)
	}
}

func TestAccountFromOptions(t *testing.T) {
	opts := defaultServerOptions
	opts.Accounts = []*Account{
		&Account{Name: "foo"},
		&Account{Name: "bar"},
	}
	s := New(&opts)

	if la := len(s.accounts); la != 2 {
		t.Fatalf("Expected to have a server with two accounts, got %v", la)
	}
	// Check that sl is filled in.
	fooAcc := s.LookupAccount("foo")
	barAcc := s.LookupAccount("bar")
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

	c, _, _ = newClientForServer(s)
	err := c.parse(connectOp)
	if err != nil {
		t.Fatalf("Received an error trying to create an account: %v", err)
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
		t.Fatalf("Expected to see 2 accounts in opts, got %d\n", la)
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
		t.Fatalf("Expected to see 2 accounts in opts, got %d\n", la)
	}

	if lu := len(opts.Users); lu != 4 {
		t.Fatalf("Expected 4 total Users, got %d\n", lu)
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
				break
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

func TestImportAuthorized(t *testing.T) {
	_, foo, bar := simpleAccountServer(t)

	checkBool(foo.checkImportAuthorized(bar, "foo"), false, t)
	checkBool(foo.checkImportAuthorized(bar, "*"), false, t)
	checkBool(foo.checkImportAuthorized(bar, ">"), false, t)
	checkBool(foo.checkImportAuthorized(bar, "foo.*"), false, t)
	checkBool(foo.checkImportAuthorized(bar, "foo.>"), false, t)

	foo.addExport("foo", isPublicExport)
	checkBool(foo.checkImportAuthorized(bar, "foo"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "bar"), false, t)
	checkBool(foo.checkImportAuthorized(bar, "*"), false, t)

	foo.addExport("*", []*Account{bar})
	checkBool(foo.checkImportAuthorized(bar, "foo"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "bar"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "baz"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "foo.bar"), false, t)
	checkBool(foo.checkImportAuthorized(bar, ">"), false, t)
	checkBool(foo.checkImportAuthorized(bar, "*"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "foo.*"), false, t)
	checkBool(foo.checkImportAuthorized(bar, "*.*"), false, t)
	checkBool(foo.checkImportAuthorized(bar, "*.>"), false, t)

	// Reset and test '>' public export
	_, foo, bar = simpleAccountServer(t)
	foo.addExport(">", nil)
	// Everything should work.
	checkBool(foo.checkImportAuthorized(bar, "foo"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "bar"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "baz"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "foo.bar"), true, t)
	checkBool(foo.checkImportAuthorized(bar, ">"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "*"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "foo.*"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "*.*"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "*.>"), true, t)

	// Reset and test pwc and fwc
	s, foo, bar := simpleAccountServer(t)
	foo.addExport("foo.*.baz.>", []*Account{bar})
	checkBool(foo.checkImportAuthorized(bar, "foo.bar.baz.1"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "foo.bar.baz.*"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "foo.*.baz.1.1"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "foo.22.baz.22"), true, t)
	checkBool(foo.checkImportAuthorized(bar, "foo.bar.baz"), false, t)
	checkBool(foo.checkImportAuthorized(bar, ""), false, t)
	checkBool(foo.checkImportAuthorized(bar, "foo.bar.*.*"), false, t)

	// Make sure we match the account as well

	fb, _ := s.RegisterAccount("foobar")
	bz, _ := s.RegisterAccount("baz")

	checkBool(foo.checkImportAuthorized(fb, "foo.bar.baz.1"), false, t)
	checkBool(foo.checkImportAuthorized(bz, "foo.bar.baz.1"), false, t)
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
	if err := cbar.acc.addImport(fooAcc, "foo", "import"); err != ErrAccountImportAuthorization {
		t.Fatalf("Expected error of ErrAccountImportAuthorization but got %v", err)
	}

	// Now map the subject space between foo and bar.
	// Need to do export first.
	if err := cfoo.acc.addExport("foo", nil); err != nil { // Public with no accounts defined.
		t.Fatalf("Error adding account export to client foo: %v", err)
	}
	if err := cbar.acc.addImport(fooAcc, "foo", "import"); err != nil {
		t.Fatalf("Error adding account import to client bar: %v", err)
	}

	// Normal Subscription on bar client.
	go cbar.parse([]byte("SUB import.foo 1\r\nSUB import.foo bar 2\r\nPING\r\n"))
	_, err := crBar.ReadString('\n') // Make sure subscriptions were processed.
	if err != nil {
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
			t.Fatalf("Did not get correct subject: '%s'\n", matches[SUB_INDEX])
		}
		if matches[SID_INDEX] != sid {
			t.Fatalf("Did not get correct sid: '%s'\n", matches[SID_INDEX])
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
		t.Fatalf("Error reading from client 'baz': %v", err)
	}
	checkMsg(l, "2")
	checkPayload(crBar, []byte("hello\r\n"), t)
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

	if err := cfoo.acc.addExport(">", []*Account{barAcc}); err != nil { // Public with no accounts defined.
		t.Fatalf("Error adding account export to client foo: %v", err)
	}
	if err := cbar.acc.addImport(fooAcc, "*", ""); err != nil {
		t.Fatalf("Error adding account import to client bar: %v", err)
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
		t.Fatalf("Did not get correct subject: '%s'\n", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'\n", matches[SID_INDEX])
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

	if err := cfoo.acc.addExport(">", []*Account{barAcc}); err != nil { // Public with no accounts defined.
		t.Fatalf("Error adding account export to client foo: %v", err)
	}
	if err := cbar.acc.addImport(fooAcc, "*", "pub.imports."); err != nil {
		t.Fatalf("Error adding account import to client bar: %v", err)
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
		t.Fatalf("Did not get correct subject: '%s'\n", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'\n", matches[SID_INDEX])
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

	if err := cfoo.acc.addExport(">", []*Account{barAcc}); err != nil { // Public with no accounts defined.
		t.Fatalf("Error adding account export to client foo: %v", err)
	}
	if err := cbar.acc.addImport(fooAcc, "*", "pub.imports."); err != nil {
		t.Fatalf("Error adding account import to client bar: %v", err)
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
		t.Fatalf("Did not get correct subject: '%s'\n", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'\n", matches[SID_INDEX])
	}
	checkPayload(crBar, []byte("hello\r\n"), t)
}
