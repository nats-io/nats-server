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
	"net"
	"strconv"
	"strings"
	"time"
)

// UDSOptions holds UNIX domain socket configuration options.
type UDSOptions struct {
	Path  string
	Group string
	Mode  string
}

// UDSInfo is the UDS information exposed to clients in the INFO message.
type UDSInfo struct {
	Path string `json:"path"`
}

// ParseUDSOption parses a UDS option string in the format "/path;group=grp;mode=0660".
// The path is required, group and mode are optional.
func ParseUDSOption(s string) (UDSOptions, error) {
	var opts UDSOptions
	parts := strings.Split(s, ";")
	opts.Path = parts[0]

	if opts.Path == _EMPTY_ {
		return UDSOptions{}, fmt.Errorf("invalid path: '' (expected absolute path to UNIX domain socket)")
	}

	for _, part := range parts[1:] {
		if part == _EMPTY_ {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return UDSOptions{}, fmt.Errorf("invalid UDS option format: %q (expected <option>=<value>)", part)
		}
		key, value := kv[0], kv[1]
		switch key {
		case "group":
			opts.Group = value
		case "mode":
			opts.Mode = value
		default:
			return UDSOptions{}, fmt.Errorf("unknown UDS option: %q (expected 'group' or 'mode')", key)
		}
	}

	return opts, nil
}

type udsSocketSpec struct {
	path string
	gid  int // -1 = no change
	mode int // -1 = no change, 0 = 0000
}

type udsServerState struct {
	socket                udsSocketSpec
	listener              net.Listener
	listenerErr           error
	peerCredentialQueries map[string]PeerCredQueryFunc
}

// Connection types are defined in nats-io/jwt, I don't want to fork that just for a constant.
// This type is "reused" to differentiate permission granting/restricting rules from
// authenticating rules. The presence of AllowedConnectionTypes in users signifies that
// matching peers are allowed to connect. There is no good reason to
// allow or restrict connection types for UDS, because if any other auth mechanism triggered
// before, the peer cred mechanism would not be used at all. It would be cleaner to add
// a "connect" permission to user.permissions to accomplish this. It would also be cleaner
// if AllowedConnectionTypes == [] would mean none, but it's interpreted as all. The value of
// being able to restrict connections based on peer credentials seems to justify this hack
// (better: adding an explicit setting).
const ConnectionTypeUnix = "UNIXSOCKET" // named to follow convention in jwt (e.g.: WEBSOCKET)

// Determines whether a User entry matching peer credentials authorizes connection via UDS.
// This is semantically equivalent to an existing user entry for a matching TLS certificate.
func (user *User) grantsUdsConnectionPermission() bool {
	_, ok := user.AllowedConnectionTypes[ConnectionTypeUnix]
	return user.AllowedConnectionTypes != nil && ok
}

// UDSPeerCreds holds UNIX domain socket peer credentials.
// Exported for use in PeerCredQueryFunc implementations.
// Not all UNIX derivatives have PID, some have GROUP. Group
// is referenced as "pid:gid", on Linux via lookup, if BSDs
// are supported, may add Groups field.
type UDSPeerCreds struct {
	UID int // User ID, >= 0
	GID int // Group ID, >= 0
	PID int // Process ID, -1 if not supported, > 0 otherwise
}

// Determines if a client is connected via a UNIX domain socket
func (c *client) isUDSPeer() bool {
	_, ok := c.nc.(*net.UnixConn)
	return ok
}

func (s *Server) hasUDSPeerCredentialSupport() bool {
	return s.uds != nil && s.uds.peerCredentialQueries != nil
}

func (s *Server) udsListenerErr() error {
	if s.uds == nil {
		return nil
	}
	return s.uds.listenerErr
}

// Determines if a user name can be a peer credential pattern and is likely not
// a valid user/passwd or TLS subject name. It might still be an invalid peercred
// pattern or a different convention. This should be good enough to justify
// a warning if a user name matches and is not a valid pattern.
// (/^[ugp]id[:!=]/ is unlikely to be anything but a possibly invalid peer-cred pattern)
func maybePeerCredPattern(u string) bool {
	return len(u) >= 4 &&
		u[1] == 'i' && u[2] == 'd' &&
		(u[3] == ':' || u[3] == '=' || u[3] == '!') &&
		(u[0] == 'u' || u[0] == 'g' || u[0] == 'p')
}

// Used to iterate over peer credential pattern tokens (separated by ',')
type peerCredPatternToken struct {
	pattern    string
	startIndex int
	length     int    // pattern[startIndex+length] == ',' or EOS
	query      string // "uid", "gid", "pid", "uid:name", "pid:gid:name" (suppl. groups), "pid:systemd-unit"
	negated    bool   // "!=" instead of "="
	value      string
}

// Scans a single match expression. Space handling is rigid, allows space before
// query (uid/pid:some/...) and inside values (not leading trailing), not around operators
func scanPeerCredToken(pattern string, startIndex int) (peerCredPatternToken, error) {
	// Implementation note: we could just use indexes and do query matching inline.
	// - That would avoid string extraction and allocs
	// - Makes the code much less readable
	// - It's not in the hot message loop, only once per connection
	// - Should be good enough reason to justify favoring readability

	if startIndex >= len(pattern) {
		return peerCredPatternToken{}, fmt.Errorf("'%s': startIndex %d out of bounds", pattern, startIndex)
	}

	// Skip leading spaces (allows "uid=1000, gid=100") - convenience only
	for startIndex < len(pattern) && pattern[startIndex] == ' ' {
		startIndex++
	}

	if startIndex >= len(pattern) {
		return peerCredPatternToken{}, fmt.Errorf("'%s': trailing spaces at end", pattern)
	}

	result := peerCredPatternToken{
		pattern:    pattern,
		startIndex: startIndex,
	}

	// Find the operator (= or !=)
	i := startIndex
	for i < len(pattern) && pattern[i] != '=' && pattern[i] != '!' && pattern[i] != ',' {
		i++
	}

	if i >= len(pattern) || pattern[i] == ',' {
		return result, fmt.Errorf("'%s': missing operator at position %d", pattern, startIndex)
	}

	result.query = pattern[startIndex:i]

	// Check for != or =
	if pattern[i] == '!' {
		if i+1 >= len(pattern) || pattern[i+1] != '=' {
			return result, fmt.Errorf("'%s': invalid '!' without '=' at position %d", pattern, i)
		}
		result.negated = true
		i += 2
	} else {
		// pattern[i] == '='
		i++
	}

	// Find the value (until ',' or end)
	valueStart := i
	for i < len(pattern) && pattern[i] != ',' {
		i++
	}

	result.value = pattern[valueStart:i]
	result.length = i - startIndex

	// Reject leading/trailing non-visible chars in value (likely a typo)
	// This is protection from unexpected+silent mismatch of deny rules, etc.
	// Allow inner spaces in the unlikely case they might be legit
	// Generally, IDs, user names, services, paths should not contain leading/trailing spaces
	if len(result.value) > 0 {
		if result.value[0] <= ' ' {
			return result, fmt.Errorf("'%s': leading whitespace in value at position %d", pattern, valueStart)
		}
		if result.value[len(result.value)-1] <= ' ' {
			return result, fmt.Errorf("'%s': trailing whitespace in value at position %d", pattern, valueStart)
		}
	}

	return result, nil
}

// peerCredsMatchPattern determines whether credentials match all tokens in pattern (AND logic).
// Returns error if pattern is invalid or query handler not found.
// Standalone function for testability.
func peerCredsMatchPattern(queries map[string]PeerCredQueryFunc, creds UDSPeerCreds, pattern string) (bool, error) {
	i := 0
	for i < len(pattern) {
		token, err := scanPeerCredToken(pattern, i)
		if err != nil {
			return false, err
		}

		handler, ok := queries[token.query]
		if !ok {
			return false, fmt.Errorf("'%s': unknown query '%s'", pattern, token.query)
		}

		matched, err := handler(creds, token.value)
		if err != nil {
			return false, fmt.Errorf("'%s': query '%s' failed: %w", pattern, token.query, err)
		}

		if token.negated {
			matched = !matched
		}

		if !matched {
			return false, nil
		}

		// Move to next token (skip comma)
		i = token.startIndex + token.length
		if i < len(pattern) && pattern[i] == ',' {
			i++
			if i >= len(pattern) {
				return false, fmt.Errorf("'%s': trailing comma", pattern)
			}
		}
	}

	return true, nil
}

// udsPeerCredsMatchPattern is a Server method wrapper for peerCredsMatchPattern.
func (s *Server) udsPeerCredsMatchPattern(creds UDSPeerCreds, pattern string) (bool, error) {
	if s.uds == nil || s.uds.peerCredentialQueries == nil {
		s.Fatalf("udsPeerCredsMatchPattern called with nil query registry")
		return false, nil // unreachable, but satisfies compiler
	}
	return peerCredsMatchPattern(s.uds.peerCredentialQueries, creds, pattern)
}

type udsPeerAuthResult struct {
	authenticated      bool
	authenticatingRule string
	identity           string
	account            *Account
	permissions        *Permissions
	warnings           []string
}

func udsAuthenticatePeer(users map[string]*User, queries map[string]PeerCredQueryFunc, peerCreds UDSPeerCreds) (udsPeerAuthResult, error) {
	identity := fmt.Sprintf("uid=%d,gid=%d,pid=%d", peerCreds.UID, peerCreds.GID, peerCreds.PID)
	result := udsPeerAuthResult{
		identity: identity,
	}

	var pubAllow, pubDeny, subAllow, subDeny []string
	var respMaxMsgs int
	var respExpires time.Duration
	hasPermissions := false

	for _, u := range users {
		if !maybePeerCredPattern(u.Username) {
			continue
		}
		matched, err := peerCredsMatchPattern(queries, peerCreds, u.Username)
		if err != nil {
			return result, fmt.Errorf("pattern %q: %w", u.Username, err)
		}
		if !matched {
			continue
		}

		// Check for conflicting accounts
		if u.Account != nil {
			if result.account != nil && result.account != u.Account {
				return result, fmt.Errorf("%q: conflicting accounts: %q redefines %q", result.identity, u.Username, result.authenticatingRule)
			}
			result.authenticated = true
			result.authenticatingRule = u.Username // overwrite possible name of authenticating non-account rule
			result.account = u.Account
		}

		if u.grantsUdsConnectionPermission() {
			result.authenticated = true
			if result.authenticatingRule == _EMPTY_ {
				// only record first authenticating rule
				result.authenticatingRule = u.Username
			}
		}

		// Merge permissions
		if u.Permissions != nil {
			hasPermissions = true
			if u.Permissions.Publish != nil {
				pubAllow = append(pubAllow, u.Permissions.Publish.Allow...)
				pubDeny = append(pubDeny, u.Permissions.Publish.Deny...)
			}
			if u.Permissions.Subscribe != nil {
				subAllow = append(subAllow, u.Permissions.Subscribe.Allow...)
				subDeny = append(subDeny, u.Permissions.Subscribe.Deny...)
			}
			if u.Permissions.Response != nil {
				if u.Permissions.Response.MaxMsgs > respMaxMsgs {
					respMaxMsgs = u.Permissions.Response.MaxMsgs
				}
				if u.Permissions.Response.Expires > respExpires {
					respExpires = u.Permissions.Response.Expires
				}
			}
		}
	}

	if !result.authenticated {
		return result, nil
	}
	if !hasPermissions {
		result.authenticated = false
		result.warnings = append(result.warnings, fmt.Sprintf("%q: no permissions defined", result.identity))
		return result, nil
	}
	if len(pubAllow) == 0 && len(pubDeny) == 0 && len(subAllow) == 0 && len(subDeny) == 0 {
		result.authenticated = false
		result.warnings = append(result.warnings, fmt.Sprintf("%q: empty permissions (no default allow policy for UDS connections)", result.identity))
		return result, nil
	}

	// Build merged permissions
	result.permissions = &Permissions{
		Publish:   &SubjectPermission{Allow: pubAllow, Deny: pubDeny},
		Subscribe: &SubjectPermission{Allow: subAllow, Deny: subDeny},
	}
	if respMaxMsgs > 0 || respExpires > 0 {
		result.permissions.Response = &ResponsePermission{
			MaxMsgs: respMaxMsgs,
			Expires: respExpires,
		}
	}

	return result, nil
}

// PeerCredQueryFunc evaluates a peercred query.
// Returns (matched, error). If negated, caller inverts the result.
type PeerCredQueryFunc func(creds UDSPeerCreds, value string) (bool, error)

// RegisterUDSPeerCredQuery registers a query function for a credential/query pair.
// Must be called before Server.Start(). Returns error if server is already running.
// Returns the previous handler for this query (nil if none).
func (s *Server) RegisterUDSPeerCredQuery(query string, fn PeerCredQueryFunc) (PeerCredQueryFunc, error) {
	if s.isRunning() {
		return nil, fmt.Errorf("cannot register UDS peer credential query after server start")
	}
	if s.uds == nil {
		return nil, fmt.Errorf("UDS not configured")
	}
	result := s.uds.peerCredentialQueries[query]
	s.uds.peerCredentialQueries[query] = fn
	return result, nil
}

func peerCredQueryUID(c UDSPeerCreds, v string) (bool, error) {
	id, err := strconv.Atoi(v)
	if err != nil {
		return false, fmt.Errorf("invalid uid value %q: %w", v, err)
	}
	return c.UID == id, nil
}

func peerCredQueryGID(c UDSPeerCreds, v string) (bool, error) {
	id, err := strconv.Atoi(v)
	if err != nil {
		return false, fmt.Errorf("invalid gid value %q: %w", v, err)
	}
	return c.GID == id, nil
}

func peerCredQueryPID(c UDSPeerCreds, v string) (bool, error) {
	id, err := strconv.Atoi(v)
	if err != nil {
		return false, fmt.Errorf("invalid pid value %q: %w", v, err)
	}
	return c.PID == id, nil
}
