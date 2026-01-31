// Copyright 2026 Michael Utech
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
	"testing"
)

// Helper to create a simple match pattern for testing
func matchPattern(conditions map[string]any) *UDSRulePattern {
	alt := make(map[UDSRuleExpression]any)
	for k, v := range conditions {
		negate := false
		name := k
		if len(k) > 0 && k[0] == '!' {
			negate = true
			name = k[1:]
		}
		alt[UDSRuleExpression{QueryName: name, Negate: negate}] = v
	}
	pattern := UDSRulePattern{alt}
	return &pattern
}

var testQueries = map[string]udsPeerCredQuery{
	"uid": {evaluate: peerCredQueryUID, validate: validateIsInt, cacheScope: _EMPTY_},
	"gid": {evaluate: peerCredQueryGID, validate: validateIsInt, cacheScope: _EMPTY_},
	"pid": {evaluate: peerCredQueryPID, validate: validateIsInt, cacheScope: _EMPTY_},
}

func newTestAuthContext(creds UDSPeerCreds) *udsAuthContext {
	return &udsAuthContext{
		queries:     testQueries,
		peerCreds:   creds,
		scopedCache: map[string]map[string]any{},
	}
}

func matchesPattern(pattern *UDSRulePattern, creds UDSPeerCreds) (bool, error) {
	return pattern.matches(newTestAuthContext(creds))
}

// =============================================================================
// UDSRulePattern.matches() tests
// =============================================================================

func TestUDS_Pattern_Matches_Single(t *testing.T) {
	tests := []struct {
		name    string
		pattern map[string]any
		creds   UDSPeerCreds
		want    bool
	}{
		{"uid match", map[string]any{"uid": 1000}, UDSPeerCreds{UID: 1000}, true},
		{"uid no match", map[string]any{"uid": 1000}, UDSPeerCreds{UID: 999}, false},
		{"gid match", map[string]any{"gid": 100}, UDSPeerCreds{GID: 100}, true},
		{"pid match", map[string]any{"pid": 1234}, UDSPeerCreds{PID: 1234}, true},
		{"negated match", map[string]any{"!uid": 0}, UDSPeerCreds{UID: 1000}, true},
		{"negated no match", map[string]any{"!uid": 1000}, UDSPeerCreds{UID: 1000}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pattern := matchPattern(tc.pattern)
			got, err := matchesPattern(pattern, tc.creds)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUDS_Pattern_Matches_AND(t *testing.T) {
	creds := UDSPeerCreds{UID: 1000, GID: 100, PID: 1234}

	tests := []struct {
		name    string
		pattern map[string]any
		want    bool
	}{
		{"both match", map[string]any{"uid": 1000, "gid": 100}, true},
		{"first fails", map[string]any{"uid": 999, "gid": 100}, false},
		{"second fails", map[string]any{"uid": 1000, "gid": 999}, false},
		{"all three match", map[string]any{"uid": 1000, "gid": 100, "pid": 1234}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pattern := matchPattern(tc.pattern)
			got, err := matchesPattern(pattern, creds)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUDS_Pattern_Matches_OR(t *testing.T) {
	creds := UDSPeerCreds{UID: 1000, GID: 100}

	// OR pattern: [{uid: 1000}, {uid: 2000}]
	pattern := &UDSRulePattern{
		{UDSRuleExpression{QueryName: "uid"}: 1000},
		{UDSRuleExpression{QueryName: "uid"}: 2000},
	}

	got, err := matchesPattern(pattern, creds)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Error("expected match (first alternative)")
	}

	// Test second alternative matches
	creds.UID = 2000
	got, err = matchesPattern(pattern, creds)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Error("expected match (second alternative)")
	}

	// Test neither matches
	creds.UID = 3000
	got, err = matchesPattern(pattern, creds)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Error("expected no match")
	}
}

func TestUDS_Pattern_Matches_UnknownQuery(t *testing.T) {
	pattern := matchPattern(map[string]any{"unknown": 1})
	_, err := matchesPattern(pattern, UDSPeerCreds{})
	if err == nil {
		t.Error("expected error for unknown query")
	}
}

// =============================================================================
// udsAuthenticatePeer tests
// =============================================================================

func TestUDS_Auth_EmptyRules(t *testing.T) {
	result, err := udsAuthenticatePeer([]*UDSRule{}, testQueries, UDSPeerCreds{UID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.authenticated {
		t.Error("expected not authenticated with empty rules")
	}
}

func TestUDS_Auth_NoMatchingRule(t *testing.T) {
	rules := []*UDSRule{
		{
			Username:    "other",
			Match:       matchPattern(map[string]any{"uid": 2000}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{">"}}},
		},
	}
	result, err := udsAuthenticatePeer(rules, testQueries, UDSPeerCreds{UID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.authenticated {
		t.Error("expected not authenticated")
	}
}

func TestUDS_Auth_SimpleMatch(t *testing.T) {
	rules := []*UDSRule{
		{
			Username:    "alice",
			Match:       matchPattern(map[string]any{"uid": 1000}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{">"}}},
		},
	}
	result, err := udsAuthenticatePeer(rules, testQueries, UDSPeerCreds{UID: 1000, GID: 100, PID: 1234})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.authenticated {
		t.Fatal("expected authenticated")
	}
	if result.authenticatingRule != "alice" {
		t.Errorf("expected authenticatingRule 'alice', got %q", result.authenticatingRule)
	}
}

func TestUDS_Auth_NoPermissions(t *testing.T) {
	rules := []*UDSRule{
		{
			Username: "alice",
			Match:    matchPattern(map[string]any{"uid": 1000}),
			// No permissions
		},
	}
	result, err := udsAuthenticatePeer(rules, testQueries, UDSPeerCreds{UID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.authenticated {
		t.Error("expected not authenticated without permissions")
	}
	if len(result.warnings) == 0 {
		t.Error("expected warning")
	}
}

func TestUDS_Auth_EmptyPermissions(t *testing.T) {
	rules := []*UDSRule{
		{
			Username:    "alice",
			Match:       matchPattern(map[string]any{"uid": 1000}),
			Permissions: &Permissions{},
		},
	}
	result, err := udsAuthenticatePeer(rules, testQueries, UDSPeerCreds{UID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.authenticated {
		t.Error("expected not authenticated with empty permissions")
	}
	if len(result.warnings) == 0 {
		t.Error("expected warning")
	}
}

// =============================================================================
// User/Role precedence tests
// =============================================================================

func TestUDS_Auth_PureUserBeatsMixed(t *testing.T) {
	rules := []*UDSRule{
		{
			Username:    "fallback",
			Rolename:    "common",
			Match:       matchPattern(map[string]any{"gid": 100}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{"common.>"}}},
		},
		{
			Username:    "alice",
			Match:       matchPattern(map[string]any{"uid": 1000}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{"alice.>"}}},
		},
	}
	result, err := udsAuthenticatePeer(rules, testQueries, UDSPeerCreds{UID: 1000, GID: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.authenticated {
		t.Fatal("expected authenticated")
	}
	if result.authenticatingRule != "alice" {
		t.Errorf("expected 'alice' (pure user), got %q", result.authenticatingRule)
	}
	// Mixed rule should contribute role
	if len(result.roles) != 1 || result.roles[0] != "common" {
		t.Errorf("expected roles [common], got %v", result.roles)
	}
}

func TestUDS_Auth_MixedRuleAlone(t *testing.T) {
	rules := []*UDSRule{
		{
			Username:    "service",
			Rolename:    "worker",
			Match:       matchPattern(map[string]any{"uid": 1000}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{">"}}},
		},
	}
	result, err := udsAuthenticatePeer(rules, testQueries, UDSPeerCreds{UID: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.authenticated {
		t.Fatal("expected authenticated")
	}
	if result.authenticatingRule != "service" {
		t.Errorf("expected 'service', got %q", result.authenticatingRule)
	}
	if len(result.roles) != 1 || result.roles[0] != "worker" {
		t.Errorf("expected roles [worker], got %v", result.roles)
	}
}

func TestUDS_Auth_TwoPureUsersConflict(t *testing.T) {
	rules := []*UDSRule{
		{
			Username:    "alice",
			Match:       matchPattern(map[string]any{"uid": 1000}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{">"}}},
		},
		{
			Username:    "bob",
			Match:       matchPattern(map[string]any{"gid": 100}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{">"}}},
		},
	}
	result, err := udsAuthenticatePeer(rules, testQueries, UDSPeerCreds{UID: 1000, GID: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.authenticated {
		t.Error("expected not authenticated (conflict)")
	}
	if len(result.warnings) == 0 {
		t.Error("expected warning about conflict")
	}
}

func TestUDS_Auth_TwoMixedUsersConflict(t *testing.T) {
	rules := []*UDSRule{
		{
			Username:    "alice",
			Rolename:    "role1",
			Match:       matchPattern(map[string]any{"uid": 1000}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{">"}}},
		},
		{
			Username:    "bob",
			Rolename:    "role2",
			Match:       matchPattern(map[string]any{"gid": 100}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{">"}}},
		},
	}
	result, err := udsAuthenticatePeer(rules, testQueries, UDSPeerCreds{UID: 1000, GID: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.authenticated {
		t.Error("expected not authenticated (conflict)")
	}
	if len(result.warnings) == 0 {
		t.Error("expected warning about conflict")
	}
}

func TestUDS_Auth_RoleOnlyRule(t *testing.T) {
	rules := []*UDSRule{
		{
			Username:    "alice",
			Match:       matchPattern(map[string]any{"uid": 1000}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{"user.>"}}},
		},
		{
			Rolename:    "admin",
			Match:       matchPattern(map[string]any{"gid": 100}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{"admin.>"}}},
		},
	}
	result, err := udsAuthenticatePeer(rules, testQueries, UDSPeerCreds{UID: 1000, GID: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.authenticated {
		t.Fatal("expected authenticated")
	}
	if result.authenticatingRule != "alice" {
		t.Errorf("expected 'alice', got %q", result.authenticatingRule)
	}
	if len(result.roles) != 1 || result.roles[0] != "admin" {
		t.Errorf("expected roles [admin], got %v", result.roles)
	}
}

func TestUDS_Auth_MultipleRoles(t *testing.T) {
	rules := []*UDSRule{
		{
			Username:    "alice",
			Match:       matchPattern(map[string]any{"uid": 1000}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{">"}}},
		},
		{
			Rolename:    "dev",
			Match:       matchPattern(map[string]any{"gid": 100}),
			Permissions: &Permissions{},
		},
		{
			Rolename:    "ops",
			Match:       matchPattern(map[string]any{"gid": 100}),
			Permissions: &Permissions{},
		},
	}
	result, err := udsAuthenticatePeer(rules, testQueries, UDSPeerCreds{UID: 1000, GID: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.authenticated {
		t.Fatal("expected authenticated")
	}
	if len(result.roles) != 2 {
		t.Errorf("expected 2 roles, got %v", result.roles)
	}
}

// =============================================================================
// Permission merging tests
// =============================================================================

func TestUDS_Auth_PermissionsMerged(t *testing.T) {
	rules := []*UDSRule{
		{
			Username:    "alice",
			Match:       matchPattern(map[string]any{"uid": 1000}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{"foo.>"}}},
		},
		{
			Rolename:    "extra",
			Match:       matchPattern(map[string]any{"gid": 100}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{"bar.>"}}},
		},
	}
	result, err := udsAuthenticatePeer(rules, testQueries, UDSPeerCreds{UID: 1000, GID: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.permissions.Publish.Allow) != 2 {
		t.Errorf("expected 2 publish allows, got %d", len(result.permissions.Publish.Allow))
	}
}

func TestUDS_Auth_DenyListsMerged(t *testing.T) {
	rules := []*UDSRule{
		{
			Username: "alice",
			Match:    matchPattern(map[string]any{"uid": 1000}),
			Permissions: &Permissions{
				Publish: &SubjectPermission{Allow: []string{">"}, Deny: []string{"secret.>"}},
			},
		},
		{
			Rolename: "restricted",
			Match:    matchPattern(map[string]any{"gid": 100}),
			Permissions: &Permissions{
				Publish: &SubjectPermission{Deny: []string{"internal.>"}},
			},
		},
	}
	result, err := udsAuthenticatePeer(rules, testQueries, UDSPeerCreds{UID: 1000, GID: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.permissions.Publish.Deny) != 2 {
		t.Errorf("expected 2 publish denies, got %d", len(result.permissions.Publish.Deny))
	}
}

func TestUDS_Auth_ResponsePermissionsMerged(t *testing.T) {
	rules := []*UDSRule{
		{
			Username: "alice",
			Match:    matchPattern(map[string]any{"uid": 1000}),
			Permissions: &Permissions{
				Publish:  &SubjectPermission{Allow: []string{">"}},
				Response: &ResponsePermission{MaxMsgs: 5, Expires: 100},
			},
		},
		{
			Rolename: "extra",
			Match:    matchPattern(map[string]any{"gid": 100}),
			Permissions: &Permissions{
				Response: &ResponsePermission{MaxMsgs: 10, Expires: 50},
			},
		},
	}
	result, err := udsAuthenticatePeer(rules, testQueries, UDSPeerCreds{UID: 1000, GID: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.permissions.Response.MaxMsgs != 10 {
		t.Errorf("expected MaxMsgs 10 (max), got %d", result.permissions.Response.MaxMsgs)
	}
	if result.permissions.Response.Expires != 100 {
		t.Errorf("expected Expires 100 (max), got %d", result.permissions.Response.Expires)
	}
}

// =============================================================================
// Identity format tests
// =============================================================================

func TestUDS_Auth_IdentityFormat(t *testing.T) {
	rules := []*UDSRule{
		{
			Username:    "alice",
			Match:       matchPattern(map[string]any{"uid": 1000}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{">"}}},
		},
	}
	result, err := udsAuthenticatePeer(rules, testQueries, UDSPeerCreds{UID: 1000, GID: 100, PID: 1234})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "alice::uid=1000,gid=100,pid=1234"
	if result.identity != expected {
		t.Errorf("expected identity %q, got %q", expected, result.identity)
	}
}

func TestUDS_Auth_IdentityFormatWithRoles(t *testing.T) {
	rules := []*UDSRule{
		{
			Username:    "alice",
			Match:       matchPattern(map[string]any{"uid": 1000}),
			Permissions: &Permissions{Publish: &SubjectPermission{Allow: []string{">"}}},
		},
		{
			Rolename:    "admin",
			Match:       matchPattern(map[string]any{"gid": 100}),
			Permissions: &Permissions{},
		},
		{
			Rolename:    "dev",
			Match:       matchPattern(map[string]any{"gid": 100}),
			Permissions: &Permissions{},
		},
	}
	result, err := udsAuthenticatePeer(rules, testQueries, UDSPeerCreds{UID: 1000, GID: 100, PID: 1234})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Roles order depends on rule order
	expected := "alice:admin,dev:uid=1000,gid=100,pid=1234"
	if result.identity != expected {
		t.Errorf("expected identity %q, got %q", expected, result.identity)
	}
}

// =============================================================================
// Query function tests
// =============================================================================

func TestUDS_Auth_QueryUID(t *testing.T) {
	tests := []struct {
		uid   int
		value any
		want  bool
	}{
		{1000, 1000, true},
		{1000, 999, false},
		{0, 0, true},
	}
	for _, tc := range tests {
		creds := UDSPeerCreds{UID: tc.uid}
		got, err := peerCredQueryUID("uid", creds, tc.value, nil)
		if err != nil {
			t.Errorf("uid=%d value=%v: unexpected error: %v", tc.uid, tc.value, err)
			continue
		}
		if got != tc.want {
			t.Errorf("uid=%d value=%v: got %v, want %v", tc.uid, tc.value, got, tc.want)
		}
	}
}

func TestUDS_Auth_QueryUID_WrongType(t *testing.T) {
	_, err := peerCredQueryUID("uid", UDSPeerCreds{UID: 1000}, "1000", nil)
	if err == nil {
		t.Error("expected error for string value")
	}
}

func TestUDS_Auth_QueryGID(t *testing.T) {
	tests := []struct {
		gid   int
		value any
		want  bool
	}{
		{100, 100, true},
		{100, 999, false},
		{0, 0, true},
	}
	for _, tc := range tests {
		creds := UDSPeerCreds{GID: tc.gid}
		got, err := peerCredQueryGID("gid", creds, tc.value, nil)
		if err != nil {
			t.Errorf("gid=%d value=%v: unexpected error: %v", tc.gid, tc.value, err)
			continue
		}
		if got != tc.want {
			t.Errorf("gid=%d value=%v: got %v, want %v", tc.gid, tc.value, got, tc.want)
		}
	}
}

func TestUDS_Auth_QueryPID(t *testing.T) {
	tests := []struct {
		pid   int
		value any
		want  bool
	}{
		{1234, 1234, true},
		{1234, 999, false},
		{1, 1, true},
	}
	for _, tc := range tests {
		creds := UDSPeerCreds{PID: tc.pid}
		got, err := peerCredQueryPID("pid", creds, tc.value, nil)
		if err != nil {
			t.Errorf("pid=%d value=%v: unexpected error: %v", tc.pid, tc.value, err)
			continue
		}
		if got != tc.want {
			t.Errorf("pid=%d value=%v: got %v, want %v", tc.pid, tc.value, got, tc.want)
		}
	}
}

// =============================================================================
// RegisterQuery tests
// =============================================================================

func TestUDS_Auth_RegisterQuery(t *testing.T) {
	uds, err := newUDSServerState(&Options{UDS: UDSOptions{Path: "/tmp/test.sock"}})
	if err != nil {
		t.Fatalf("newUDSServerState failed: %v", err)
	}
	s := &Server{uds: uds}

	called := false
	fn := func(name string, c UDSPeerCreds, v any, _ map[string]any) (bool, error) {
		called = true
		return true, nil
	}

	err = s.RegisterCustomUDSPeerCredQuery("test", fn, nil, _EMPTY_)
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	query, ok := s.uds.peerCredentialQueries["test"]
	if !ok {
		t.Fatal("handler not registered")
	}
	if query.evaluate == nil {
		t.Fatal("handler not registered")
	}

	_, _ = query.evaluate("test", UDSPeerCreds{}, nil, nil)
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

	fn1 := func(name string, c UDSPeerCreds, v any, _ map[string]any) (bool, error) { return false, nil }
	fn2 := func(name string, c UDSPeerCreds, v any, _ map[string]any) (bool, error) { return true, nil }

	if err := s.RegisterCustomUDSPeerCredQuery("test", fn1, nil, _EMPTY_); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	if err := s.RegisterCustomUDSPeerCredQuery("test", fn2, nil, _EMPTY_); err == nil {
		t.Error("expected error when registering duplicate query")
	}
}

func TestUDS_Auth_RegisterQuery_NoUDS(t *testing.T) {
	s := &Server{}

	fn := func(name string, c UDSPeerCreds, v any, _ map[string]any) (bool, error) { return true, nil }
	err := s.RegisterCustomUDSPeerCredQuery("test", fn, nil, _EMPTY_)
	if err == nil {
		t.Error("expected error when UDS not configured")
	}
}

// =============================================================================
// ParseUDSOption tests
// =============================================================================

func TestUDS_ParseUDSOption(t *testing.T) {
	tests := []struct {
		input   string
		path    string
		group   string
		mode    string
		wantErr bool
	}{
		{"/tmp/nats.sock", "/tmp/nats.sock", "", "", false},
		{"/tmp/nats.sock;group=nats", "/tmp/nats.sock", "nats", "", false},
		{"/tmp/nats.sock;mode=0660", "/tmp/nats.sock", "", "0660", false},
		{"/tmp/nats.sock;group=nats;mode=0660", "/tmp/nats.sock", "nats", "0660", false},
		{"/tmp/nats.sock;mode=0660;group=nats", "/tmp/nats.sock", "nats", "0660", false},
		{"/path/with spaces/sock;group=123", "/path/with spaces/sock", "123", "", false},
		{"", "", "", "", true},
		{";group=nats", "", "", "", true},
		{"/tmp/sock;invalid", "", "", "", true},
		{"/tmp/sock;foo=bar", "", "", "", true},
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
