// Copyright 2020-2025 Michael Utech
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
	"testing"
)

func TestUDS_Auth_MaybePeerCredPattern_Valid(t *testing.T) {
	valid := []string{
		"uid=", "gid=", "pid=",
		"uid:", "gid:", "pid:",
		"uid=1000", "gid=100", "pid=1234",
		"uid:name=alice", "pid:exe=/bin/bash",
		"uid!=0", "gid!=0",
	}
	for _, p := range valid {
		if !maybePeerCredPattern(p) {
			t.Errorf("expected true for %q", p)
		}
	}
}

func TestUDS_Auth_MaybePeerCredPattern_Invalid(t *testing.T) {
	invalid := []string{
		"", "u", "ui", "uid",
		"xid=1", "uix=1", "uidx",
		"UID=1", "Uid=1",
		"user=alice", "group=wheel",
	}
	for _, p := range invalid {
		if maybePeerCredPattern(p) {
			t.Errorf("expected false for %q", p)
		}
	}
}

func TestUDS_Auth_ScanPeerCredToken_Basic(t *testing.T) {
	tests := []struct {
		pattern string
		query   string
		value   string
		negated bool
		length  int
	}{
		{"uid=1000", "uid", "1000", false, 8},
		{"gid=100", "gid", "100", false, 7},
		{"pid=1234", "pid", "1234", false, 8},
		{"uid!=0", "uid", "0", true, 6},
		{"uid:name=alice", "uid:name", "alice", false, 14},
		{"pid:exe=/usr/bin/nats", "pid:exe", "/usr/bin/nats", false, 21},
		{"uid=", "uid", "", false, 4},
	}
	for _, tc := range tests {
		tok, err := scanPeerCredToken(tc.pattern, 0)
		if err != nil {
			t.Errorf("%q: unexpected error: %v", tc.pattern, err)
			continue
		}
		if tok.query != tc.query {
			t.Errorf("%q: query = %q, want %q", tc.pattern, tok.query, tc.query)
		}
		if tok.value != tc.value {
			t.Errorf("%q: value = %q, want %q", tc.pattern, tok.value, tc.value)
		}
		if tok.negated != tc.negated {
			t.Errorf("%q: negated = %v, want %v", tc.pattern, tok.negated, tc.negated)
		}
		if tok.length != tc.length {
			t.Errorf("%q: length = %d, want %d", tc.pattern, tok.length, tc.length)
		}
	}
}

func TestUDS_Auth_ScanPeerCredToken_MultipleTokens(t *testing.T) {
	pattern := "uid=1000,gid=100,pid!=1"

	tok1, err := scanPeerCredToken(pattern, 0)
	if err != nil {
		t.Fatalf("token 1: %v", err)
	}
	if tok1.query != "uid" || tok1.value != "1000" || tok1.length != 8 {
		t.Errorf("token 1: got {%s,%s,%d}", tok1.query, tok1.value, tok1.length)
	}

	tok2, err := scanPeerCredToken(pattern, 9)
	if err != nil {
		t.Fatalf("token 2: %v", err)
	}
	if tok2.query != "gid" || tok2.value != "100" || tok2.length != 7 {
		t.Errorf("token 2: got {%s,%s,%d}", tok2.query, tok2.value, tok2.length)
	}

	tok3, err := scanPeerCredToken(pattern, 17)
	if err != nil {
		t.Fatalf("token 3: %v", err)
	}
	if tok3.query != "pid" || tok3.value != "1" || !tok3.negated || tok3.length != 6 {
		t.Errorf("token 3: got {%s,%s,%v,%d}", tok3.query, tok3.value, tok3.negated, tok3.length)
	}
}

func TestUDS_Auth_ScanPeerCredToken_LeadingSpaces(t *testing.T) {
	tok, err := scanPeerCredToken("uid=1000, gid=100", 9)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tok.query != "gid" || tok.value != "100" {
		t.Errorf("got query=%q value=%q, want gid/100", tok.query, tok.value)
	}

	tok, err = scanPeerCredToken("uid=1000,   gid=100", 9)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tok.query != "gid" {
		t.Errorf("got query=%q, want gid", tok.query)
	}
}

func TestUDS_Auth_ScanPeerCredToken_Errors(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		start   int
	}{
		{"StartOutOfBounds", "uid=1", 10},
		{"StartAtEnd", "uid=1", 5},
		{"MissingOperator", "uid", 0},
		{"MissingOperatorComma", "uid,gid=1", 0},
		{"BangWithoutEquals", "uid!1000", 0},
		{"BangAtEnd", "uid!", 0},
		{"EmptyPattern", "", 0},
		{"TrailingSpaceInValue", "uid=1000 ", 0},
		{"TrailingSpaceBeforeComma", "uid=1 , gid=2", 0},
		{"LeadingSpaceInValue", "uid= 1000", 0},
		{"TrailingTabInValue", "uid=1000\t", 0},
		{"LeadingTabInValue", "uid=\t1000", 0},
		{"TrailingNewlineInValue", "uid=1000\n", 0},
		{"LeadingControlCharInValue", "uid=\x011000", 0},
		{"TrailingSpacesAtEnd", "uid=1000, ", 9},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := scanPeerCredToken(tc.pattern, tc.start)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestUDS_Auth_MatchPattern_EmptyPattern(t *testing.T) {
	s := &Server{uds: &udsServerState{peerCredentialQueries: map[string]PeerCredQueryFunc{}}}
	creds := UDSPeerCreds{UID: 1000, GID: 100, PID: 1234}

	matched, err := s.udsPeerCredsMatchPattern(creds, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("empty pattern should match (vacuous truth)")
	}
}

func TestUDS_Auth_MatchPattern_SingleToken(t *testing.T) {
	s := &Server{uds: &udsServerState{peerCredentialQueries: map[string]PeerCredQueryFunc{
		"uid": func(c UDSPeerCreds, v string) (bool, error) {
			return fmt.Sprintf("%d", c.UID) == v, nil
		},
	}}}
	creds := UDSPeerCreds{UID: 1000, GID: 100, PID: 1234}

	matched, err := s.udsPeerCredsMatchPattern(creds, "uid=1000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("should match uid=1000")
	}

	matched, err = s.udsPeerCredsMatchPattern(creds, "uid=999")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("should not match uid=999")
	}
}

func TestUDS_Auth_MatchPattern_Negation(t *testing.T) {
	s := &Server{uds: &udsServerState{peerCredentialQueries: map[string]PeerCredQueryFunc{
		"uid": func(c UDSPeerCreds, v string) (bool, error) {
			return fmt.Sprintf("%d", c.UID) == v, nil
		},
	}}}
	creds := UDSPeerCreds{UID: 1000, GID: 100, PID: 1234}

	matched, err := s.udsPeerCredsMatchPattern(creds, "uid!=0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("uid!=0 should match when UID=1000")
	}

	matched, err = s.udsPeerCredsMatchPattern(creds, "uid!=1000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("uid!=1000 should not match when UID=1000")
	}
}

func TestUDS_Auth_MatchPattern_MultipleTokensAND(t *testing.T) {
	s := &Server{uds: &udsServerState{peerCredentialQueries: map[string]PeerCredQueryFunc{
		"uid": func(c UDSPeerCreds, v string) (bool, error) {
			return fmt.Sprintf("%d", c.UID) == v, nil
		},
		"gid": func(c UDSPeerCreds, v string) (bool, error) {
			return fmt.Sprintf("%d", c.GID) == v, nil
		},
	}}}
	creds := UDSPeerCreds{UID: 1000, GID: 100, PID: 1234}

	matched, err := s.udsPeerCredsMatchPattern(creds, "uid=1000,gid=100")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("both conditions true, should match")
	}

	matched, err = s.udsPeerCredsMatchPattern(creds, "uid=1000,gid=999")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("second condition false, should not match")
	}

	matched, err = s.udsPeerCredsMatchPattern(creds, "uid=999,gid=100")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("first condition false, should not match")
	}
}

func TestUDS_Auth_MatchPattern_UnknownQuery(t *testing.T) {
	s := &Server{uds: &udsServerState{peerCredentialQueries: map[string]PeerCredQueryFunc{}}}
	creds := UDSPeerCreds{UID: 1000, GID: 100, PID: 1234}

	_, err := s.udsPeerCredsMatchPattern(creds, "uid=1000")
	if err == nil {
		t.Error("expected error for unknown query")
	}
}

func TestUDS_Auth_MatchPattern_HandlerError(t *testing.T) {
	s := &Server{uds: &udsServerState{peerCredentialQueries: map[string]PeerCredQueryFunc{
		"uid": func(c UDSPeerCreds, v string) (bool, error) {
			return false, fmt.Errorf("lookup failed")
		},
	}}}
	creds := UDSPeerCreds{UID: 1000, GID: 100, PID: 1234}

	_, err := s.udsPeerCredsMatchPattern(creds, "uid=1000")
	if err == nil {
		t.Error("expected error from handler")
	}
}

func TestUDS_Auth_MatchPattern_InvalidPattern(t *testing.T) {
	s := &Server{uds: &udsServerState{peerCredentialQueries: map[string]PeerCredQueryFunc{
		"uid": func(c UDSPeerCreds, v string) (bool, error) { return true, nil },
	}}}
	creds := UDSPeerCreds{UID: 1000, GID: 100, PID: 1234}

	invalid := []string{
		"uid=1000,",
		",uid=1000",
		"uid=1000,,gid=100",
		"uid",
		"=1000",
	}
	for _, p := range invalid {
		_, err := s.udsPeerCredsMatchPattern(creds, p)
		if err == nil {
			t.Errorf("%q: expected error", p)
		}
	}
}

func TestUDS_Auth_RegisterQuery(t *testing.T) {
	uds, err := newUDSServerState(&Options{UDS: UDSOptions{Path: "/tmp/test.sock"}})
	if err != nil {
		t.Fatalf("newUDSServerState failed: %v", err)
	}
	s := &Server{uds: uds}

	called := false
	fn := func(c UDSPeerCreds, v string) (bool, error) {
		called = true
		return true, nil
	}

	prev, err := s.RegisterUDSPeerCredQuery("test", fn)
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	if prev != nil {
		t.Error("expected nil previous handler")
	}

	if s.uds.peerCredentialQueries["test"] == nil {
		t.Fatal("handler not registered")
	}

	s.uds.peerCredentialQueries["test"](UDSPeerCreds{}, "")
	if !called {
		t.Error("registered handler not called")
	}
}

func TestUDS_Auth_RegisterQuery_Replace(t *testing.T) {
	uds, err := newUDSServerState(&Options{UDS: UDSOptions{Path: "/tmp/test.sock"}})
	if err != nil {
		t.Fatalf("newUDSServerState failed: %v", err)
	}
	s := &Server{uds: uds}

	fn1 := func(c UDSPeerCreds, v string) (bool, error) { return false, nil }
	fn2 := func(c UDSPeerCreds, v string) (bool, error) { return true, nil }

	s.RegisterUDSPeerCredQuery("test", fn1)
	prev, _ := s.RegisterUDSPeerCredQuery("test", fn2)

	if prev == nil {
		t.Error("expected previous handler")
	}

	result, _ := s.uds.peerCredentialQueries["test"](UDSPeerCreds{}, "")
	if !result {
		t.Error("new handler should return true")
	}
}

func TestUDS_Auth_RegisterQuery_NoUDS(t *testing.T) {
	s := &Server{}

	fn := func(c UDSPeerCreds, v string) (bool, error) { return true, nil }
	_, err := s.RegisterUDSPeerCredQuery("test", fn)
	if err == nil {
		t.Error("expected error when UDS not configured")
	}
}

func TestUDS_Auth_AuthenticatePeer_EmptyUsers(t *testing.T) {
	result, err := udsAuthenticatePeer(map[string]*User{}, nil, UDSPeerCreds{UID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.authenticated {
		t.Error("expected not authenticated")
	}
}

var defaultPeerCredQueries = map[string]PeerCredQueryFunc{
	"uid": peerCredQueryUID,
	"gid": peerCredQueryGID,
	"pid": peerCredQueryPID,
}

func TestUDS_Auth_AuthenticatePeer_NoMatchingPattern(t *testing.T) {
	users := map[string]*User{
		"uid=2000": {
			Username:               "uid=2000",
			AllowedConnectionTypes: map[string]struct{}{ConnectionTypeUnix: {}},
			Permissions:            &Permissions{Publish: &SubjectPermission{Allow: []string{">"}}},
		},
	}

	result, err := udsAuthenticatePeer(users, defaultPeerCredQueries, UDSPeerCreds{UID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.authenticated {
		t.Error("expected not authenticated")
	}
}

func TestUDS_Auth_AuthenticatePeer_NoAuthenticatingRule(t *testing.T) {
	users := map[string]*User{
		"uid=1000": {
			Username:    "uid=1000",
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{">"}}},
		},
	}

	result, err := udsAuthenticatePeer(users, defaultPeerCredQueries, UDSPeerCreds{UID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.authenticated {
		t.Error("expected not authenticated")
	}
}

func TestUDS_Auth_AuthenticatePeer_NoPermissionsDefined(t *testing.T) {
	users := map[string]*User{
		"uid=1000": {
			Username:               "uid=1000",
			AllowedConnectionTypes: map[string]struct{}{ConnectionTypeUnix: {}},
		},
	}

	result, err := udsAuthenticatePeer(users, defaultPeerCredQueries, UDSPeerCreds{UID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.authenticated {
		t.Error("expected not authenticated")
	}
	if len(result.warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d", len(result.warnings))
	}
}

func TestUDS_Auth_AuthenticatePeer_EmptyPermissions(t *testing.T) {
	users := map[string]*User{
		"uid=1000": {
			Username:               "uid=1000",
			AllowedConnectionTypes: map[string]struct{}{ConnectionTypeUnix: {}},
			Permissions:            &Permissions{},
		},
	}

	result, err := udsAuthenticatePeer(users, defaultPeerCredQueries, UDSPeerCreds{UID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.authenticated {
		t.Error("expected not authenticated")
	}
	if len(result.warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d", len(result.warnings))
	}
}

func TestUDS_Auth_AuthenticatePeer_ConflictingAccounts(t *testing.T) {
	users := map[string]*User{
		"uid=1000": {Username: "uid=1000", Account: &Account{Name: "acc1"}},
		"gid=1000": {Username: "gid=1000", Account: &Account{Name: "acc2"}},
	}

	_, err := udsAuthenticatePeer(users, defaultPeerCredQueries, UDSPeerCreds{UID: 1000, GID: 1000})
	if err == nil {
		t.Fatal("expected error for conflicting accounts")
	}
}

func TestUDS_Auth_AuthenticatePeer_WithAccount(t *testing.T) {
	acc := &Account{Name: "testaccount"}
	users := map[string]*User{
		"uid=1000": {
			Username:    "uid=1000",
			Account:     acc,
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{"foo.>"}}},
		},
	}

	result, err := udsAuthenticatePeer(users, defaultPeerCredQueries, UDSPeerCreds{UID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.authenticated {
		t.Fatal("expected authenticated")
	}
	if result.account != acc {
		t.Error("expected account to be set")
	}
	if result.authenticatingRule != "uid=1000" {
		t.Errorf("expected authenticatingRule 'uid=1000', got %q", result.authenticatingRule)
	}
}

func TestUDS_Auth_AuthenticatePeer_WithConnectionType(t *testing.T) {
	users := map[string]*User{
		"uid=1000": {
			Username:               "uid=1000",
			AllowedConnectionTypes: map[string]struct{}{ConnectionTypeUnix: {}},
			Permissions:            &Permissions{Subscribe: &SubjectPermission{Allow: []string{"events.>"}}},
		},
	}

	result, err := udsAuthenticatePeer(users, defaultPeerCredQueries, UDSPeerCreds{UID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.authenticated {
		t.Fatal("expected authenticated")
	}
	if result.authenticatingRule != "uid=1000" {
		t.Errorf("expected authenticatingRule 'uid=1000', got %q", result.authenticatingRule)
	}
}

func TestUDS_Auth_AuthenticatePeer_PermissionsMerged(t *testing.T) {
	users := map[string]*User{
		"uid=1000": {
			Username:               "uid=1000",
			AllowedConnectionTypes: map[string]struct{}{ConnectionTypeUnix: {}},
			Permissions:            &Permissions{Publish: &SubjectPermission{Allow: []string{"foo.>"}}},
		},
		"gid=1000": {
			Username:    "gid=1000",
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{"bar.>"}}},
		},
	}

	result, err := udsAuthenticatePeer(users, defaultPeerCredQueries, UDSPeerCreds{UID: 1000, GID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.authenticated {
		t.Fatal("expected authenticated")
	}
	if len(result.permissions.Publish.Allow) != 2 {
		t.Errorf("expected 2 publish allows, got %d", len(result.permissions.Publish.Allow))
	}
}

func TestUDS_Auth_AuthenticatePeer_AccountRuleOverridesAuthRule(t *testing.T) {
	users := map[string]*User{
		"uid=1000": {
			Username:               "uid=1000",
			AllowedConnectionTypes: map[string]struct{}{ConnectionTypeUnix: {}},
			Permissions:            &Permissions{Publish: &SubjectPermission{Allow: []string{"foo.>"}}},
		},
		"gid=1000": {
			Username:    "gid=1000",
			Account:     &Account{Name: "testaccount"},
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{"bar.>"}}},
		},
	}

	result, err := udsAuthenticatePeer(users, defaultPeerCredQueries, UDSPeerCreds{UID: 1000, GID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.authenticated {
		t.Fatal("expected authenticated")
	}
	if result.authenticatingRule != "gid=1000" {
		t.Errorf("expected authenticatingRule 'gid=1000' (account rule), got %q", result.authenticatingRule)
	}
}

func TestUDS_Auth_AuthenticatePeer_ResponsePermissionsMerged(t *testing.T) {
	users := map[string]*User{
		"uid=1000": {
			Username:               "uid=1000",
			AllowedConnectionTypes: map[string]struct{}{ConnectionTypeUnix: {}},
			Permissions: &Permissions{
				Publish:  &SubjectPermission{Allow: []string{"foo"}},
				Response: &ResponsePermission{MaxMsgs: 5, Expires: 100},
			},
		},
		"gid=1000": {
			Username: "gid=1000",
			Permissions: &Permissions{
				Publish:  &SubjectPermission{Allow: []string{"bar"}},
				Response: &ResponsePermission{MaxMsgs: 10, Expires: 50},
			},
		},
	}

	result, err := udsAuthenticatePeer(users, defaultPeerCredQueries, UDSPeerCreds{UID: 1000, GID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.permissions.Response == nil {
		t.Fatal("expected response permissions")
	}
	if result.permissions.Response.MaxMsgs != 10 {
		t.Errorf("expected MaxMsgs 10 (max), got %d", result.permissions.Response.MaxMsgs)
	}
	if result.permissions.Response.Expires != 100 {
		t.Errorf("expected Expires 100 (max), got %d", result.permissions.Response.Expires)
	}
}

func TestUDS_Auth_AuthenticatePeer_DenyListsMerged(t *testing.T) {
	users := map[string]*User{
		"uid=1000": {
			Username:               "uid=1000",
			AllowedConnectionTypes: map[string]struct{}{ConnectionTypeUnix: {}},
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{">"}, Deny: []string{"secret.>"}},
				Subscribe: &SubjectPermission{Allow: []string{">"}, Deny: []string{"admin.>"}},
			},
		},
		"gid=1000": {
			Username: "gid=1000",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Deny: []string{"internal.>"}},
				Subscribe: &SubjectPermission{Deny: []string{"system.>"}},
			},
		},
	}

	result, err := udsAuthenticatePeer(users, defaultPeerCredQueries, UDSPeerCreds{UID: 1000, GID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.permissions.Publish.Deny) != 2 {
		t.Errorf("expected 2 publish denies, got %d", len(result.permissions.Publish.Deny))
	}
	if len(result.permissions.Subscribe.Deny) != 2 {
		t.Errorf("expected 2 subscribe denies, got %d", len(result.permissions.Subscribe.Deny))
	}
}

func TestUDS_Auth_AuthenticatePeer_IdentityFormat(t *testing.T) {
	users := map[string]*User{
		"uid=1000": {
			Username:               "uid=1000",
			AllowedConnectionTypes: map[string]struct{}{ConnectionTypeUnix: {}},
			Permissions:            &Permissions{Publish: &SubjectPermission{Allow: []string{">"}}},
		},
	}

	result, err := udsAuthenticatePeer(users, defaultPeerCredQueries, UDSPeerCreds{UID: 1000, GID: 500, PID: 12345})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.identity != "uid=1000,gid=500,pid=12345" {
		t.Errorf("unexpected identity format: %q", result.identity)
	}
}

func TestUDS_Auth_AuthenticatePeer_MixedPubSubPermissions(t *testing.T) {
	users := map[string]*User{
		"uid=1000": {
			Username:               "uid=1000",
			AllowedConnectionTypes: map[string]struct{}{ConnectionTypeUnix: {}},
			Permissions:            &Permissions{Publish: &SubjectPermission{Allow: []string{"pub.>"}}},
		},
		"gid=1000": {
			Username:    "gid=1000",
			Permissions: &Permissions{Subscribe: &SubjectPermission{Allow: []string{"sub.>"}}},
		},
	}

	result, err := udsAuthenticatePeer(users, defaultPeerCredQueries, UDSPeerCreds{UID: 1000, GID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.permissions.Publish.Allow) != 1 || result.permissions.Publish.Allow[0] != "pub.>" {
		t.Errorf("unexpected publish allows: %v", result.permissions.Publish.Allow)
	}
	if len(result.permissions.Subscribe.Allow) != 1 || result.permissions.Subscribe.Allow[0] != "sub.>" {
		t.Errorf("unexpected subscribe allows: %v", result.permissions.Subscribe.Allow)
	}
}

func TestUDS_Auth_AuthenticatePeer_PatternError(t *testing.T) {
	users := map[string]*User{
		"uid=1000": {
			Username:               "uid=1000",
			AllowedConnectionTypes: map[string]struct{}{ConnectionTypeUnix: {}},
			Permissions:            &Permissions{Publish: &SubjectPermission{Allow: []string{">"}}},
		},
	}

	_, err := udsAuthenticatePeer(users, map[string]PeerCredQueryFunc{}, UDSPeerCreds{UID: 1000})
	if err == nil {
		t.Fatal("expected error for unknown query")
	}
}

func TestUDS_Auth_QueryUID(t *testing.T) {
	tests := []struct {
		uid   int
		value string
		match bool
		err   bool
	}{
		{1000, "1000", true, false},
		{1000, "999", false, false},
		{0, "0", true, false},
		{1000, "abc", false, true},
		{1000, "", false, true},
	}
	for _, tc := range tests {
		creds := UDSPeerCreds{UID: tc.uid}
		match, err := peerCredQueryUID(creds, tc.value)
		if tc.err && err == nil {
			t.Errorf("uid=%d value=%q: expected error", tc.uid, tc.value)
		}
		if !tc.err && err != nil {
			t.Errorf("uid=%d value=%q: unexpected error: %v", tc.uid, tc.value, err)
		}
		if !tc.err && match != tc.match {
			t.Errorf("uid=%d value=%q: match=%v, want %v", tc.uid, tc.value, match, tc.match)
		}
	}
}

func TestUDS_Auth_QueryGID(t *testing.T) {
	tests := []struct {
		gid   int
		value string
		match bool
		err   bool
	}{
		{100, "100", true, false},
		{100, "999", false, false},
		{0, "0", true, false},
		{100, "abc", false, true},
	}
	for _, tc := range tests {
		creds := UDSPeerCreds{GID: tc.gid}
		match, err := peerCredQueryGID(creds, tc.value)
		if tc.err && err == nil {
			t.Errorf("gid=%d value=%q: expected error", tc.gid, tc.value)
		}
		if !tc.err && err != nil {
			t.Errorf("gid=%d value=%q: unexpected error: %v", tc.gid, tc.value, err)
		}
		if !tc.err && match != tc.match {
			t.Errorf("gid=%d value=%q: match=%v, want %v", tc.gid, tc.value, match, tc.match)
		}
	}
}

func TestUDS_Auth_QueryPID(t *testing.T) {
	tests := []struct {
		pid   int
		value string
		match bool
		err   bool
	}{
		{1234, "1234", true, false},
		{1234, "999", false, false},
		{1, "1", true, false},
		{1234, "abc", false, true},
	}
	for _, tc := range tests {
		creds := UDSPeerCreds{PID: tc.pid}
		match, err := peerCredQueryPID(creds, tc.value)
		if tc.err && err == nil {
			t.Errorf("pid=%d value=%q: expected error", tc.pid, tc.value)
		}
		if !tc.err && err != nil {
			t.Errorf("pid=%d value=%q: unexpected error: %v", tc.pid, tc.value, err)
		}
		if !tc.err && match != tc.match {
			t.Errorf("pid=%d value=%q: match=%v, want %v", tc.pid, tc.value, match, tc.match)
		}
	}
}

func TestUDS_ParseUDSOption(t *testing.T) {
	tests := []struct {
		input   string
		path    string
		group   string
		mode    string
		wantErr bool
	}{
		// Valid cases
		{"/tmp/nats.sock", "/tmp/nats.sock", "", "", false},
		{"/tmp/nats.sock;group=nats", "/tmp/nats.sock", "nats", "", false},
		{"/tmp/nats.sock;mode=0660", "/tmp/nats.sock", "", "0660", false},
		{"/tmp/nats.sock;group=nats;mode=0660", "/tmp/nats.sock", "nats", "0660", false},
		{"/tmp/nats.sock;mode=0660;group=nats", "/tmp/nats.sock", "nats", "0660", false},
		{"/path/with spaces/sock;group=123", "/path/with spaces/sock", "123", "", false},
		// Error cases
		{"", "", "", "", true},                  // empty string
		{";group=nats", "", "", "", true},       // empty path
		{"/tmp/sock;invalid", "", "", "", true}, // missing =
		{"/tmp/sock;foo=bar", "", "", "", true}, // unknown option
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			opts, err := ParseUDSOption(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for input %q", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if opts.Path != tc.path {
				t.Errorf("path: got %q, want %q", opts.Path, tc.path)
			}
			if opts.Group != tc.group {
				t.Errorf("group: got %q, want %q", opts.Group, tc.group)
			}
			if opts.Mode != tc.mode {
				t.Errorf("mode: got %q, want %q", opts.Mode, tc.mode)
			}
		})
	}
}
