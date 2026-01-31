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
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"
)

// UDSRule represents a UDS-based authorization rule.
type UDSRule struct {
	// NATS username. If not empty, this rule can authenticate a user. The name is
	// part of its identity. It is an error is multiple (pure) user rules match a
	// connections peer creds.
	Username string
	// If not empty, the rule assigns permissions according to the associated role.
	// All matching roles are part of a user's identity. If a role defines both
	// Username and Rolename, it can authenticate a user, but pure user rules (with
	// not Rolename) have priority. It is an error if multiple mixed rules match a
	// connection's peer creds, and there is no overriding pure user rule.
	Rolename string
	// See UDSRulePattern
	Match *UDSRulePattern
	// Permissions are accumulated for all rules matching peer credentials.
	Permissions *Permissions
	// Currently not used
	Account *Account
}

// UDSRulePattern is an OR of ANDs of expressions. An expression has the form
// `query: value` or `!query: value`. An alternative is a set of expressions that
// must all match. A pattern is a list of alternatives, where at least one must
// match.
type UDSRulePattern []map[UDSRuleExpression]any

// UDSRuleExpression represents a single query expression in a UDS rule pattern (alternative).
type UDSRuleExpression struct {
	QueryName string
	Negate    bool
}

// parseUDSUserRuleParts parses the uds-specific parts of a user rule (role, match).
func parseUDSUserRuleParts(mv any, errors *[]error) (string, *UDSRulePattern, error) {
	var (
		tk       token
		lt       token
		rolename string
		patterns *UDSRulePattern
	)
	defer convertPanicToErrorList(&lt, errors)

	tk, mv = unwrapValue(mv, &lt)
	pm, ok := mv.(map[string]any)
	if !ok {
		return _EMPTY_, nil, &configErr{tk, fmt.Sprintf("Expected uds to be a map/struct, got %+v", mv)}
	}
	for k, v := range pm {
		tk, mv = unwrapValue(v, &lt)

		switch strings.ToLower(k) {
		case "match":
			var err error
			patterns, err = parseUDSRulePattern(mv, errors)
			if err != nil {
				*errors = append(*errors, err)
				continue
			}
		case "role":
			rolename = mv.(string)
		default:
			if !tk.IsUsedVariable() {
				err := &configErr{tk, fmt.Sprintf("Unknown field %q parsing uds", k)}
				*errors = append(*errors, err)
			}
		}
	}
	return rolename, patterns, nil
}

// parseUDSRulePattern parses the match clause of a UDS rule.
// Accepts either a map (single AND condition) or an array of maps (OR of ANDs).
// A single map is normalized to an array of one map.
func parseUDSRulePattern(mv any, errors *[]error) (*UDSRulePattern, error) {
	var lt token
	tk, mv := unwrapValue(mv, &lt)

	switch v := mv.(type) {
	case map[string]any:
		// Single map: normalize to an array of one
		alt, err := parseUDSRulePatternAlternative(v, errors)
		if err != nil {
			return nil, err
		}
		pattern := UDSRulePattern{alt}
		return &pattern, nil

	case []any:
		// Array of alternatives (OR)
		if len(v) == 0 {
			return nil, &configErr{tk, "match clause cannot be empty"}
		}
		pattern := make(UDSRulePattern, 0, len(v))
		for _, elem := range v {
			tk, elem = unwrapValue(elem, &lt)
			elemMap, ok := elem.(map[string]any)
			if !ok {
				return nil, &configErr{tk, fmt.Sprintf("match alternative must be a map, got %T", elem)}
			}
			alt, err := parseUDSRulePatternAlternative(elemMap, errors)
			if err != nil {
				*errors = append(*errors, err)
				continue
			}
			pattern = append(pattern, alt)
		}
		return &pattern, nil

	default:
		return nil, &configErr{tk, fmt.Sprintf("match must be a map or array, got %T", mv)}
	}
}

// parseUDSRulePatternAlternative parses a single alternative (AND of conditions).
// Each key is a query name (optionally prefixed with ! for negation).
// Each value is a primitive (int64, string, bool) or array of primitives.
func parseUDSRulePatternAlternative(mv map[string]any, errors *[]error) (map[UDSRuleExpression]any, error) {
	var lt token
	defer convertPanicToErrorList(&lt, errors)
	result := make(map[UDSRuleExpression]any, len(mv))

	for k, v := range mv {
		tk, v := unwrapValue(v, &lt)

		// Parse negation prefix
		query := UDSRuleExpression{}
		if strings.HasPrefix(k, "!") {
			query.Negate = true
			query.QueryName = k[1:]
		} else {
			query.QueryName = k
		}

		// validate query name is non-empty
		if query.QueryName == _EMPTY_ {
			*errors = append(*errors, &configErr{tk, "match condition query name cannot be empty"})
			continue
		}

		// validate value type
		switch val := v.(type) {
		case int64, string, bool:
			result[query] = val
		case []any:
			// unwrap and validate array elements
			unwrapped := make([]any, len(val))
			for i, elem := range val {
				tk, elem = unwrapValue(elem, &lt)
				switch elem.(type) {
				case int64, string, bool:
					unwrapped[i] = elem
				default:
					*errors = append(*errors, &configErr{tk, fmt.Sprintf("match condition %q[%d]: expected scalar (int/string/bool), got %T", k, i, elem)})
				}
			}
			result[query] = unwrapped
		default:
			*errors = append(*errors, &configErr{tk, fmt.Sprintf("match condition %q: expected scalar (int/string/bool) or scalar array, got %T", k, v)})
		}
	}

	return result, nil

}

// UDSPeerCreds holds UNIX domain socket peer credentials.
// Exported for use in PeerCredQueryFunc implementations.
// Not all UNIX derivatives have PID, some have GROUP. Group
// is referenced as "pid:gid", on Linux via lookup, if BSDs
// are supported, may add a Groups field.
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

type udsAuthContext struct {
	rules   []*UDSRule
	queries map[string]udsPeerCredQuery

	peerCreds   UDSPeerCreds
	scopedCache map[string]map[string]any
}

type udsPeerAuthResult struct {
	authenticated      bool
	authenticatingRule string
	identity           string
	roles              []string
	account            *Account
	permissions        *Permissions
	warnings           []string
}

func udsAuthenticatePeer(rules []*UDSRule, queries map[string]udsPeerCredQuery, peerCreds UDSPeerCreds) (udsPeerAuthResult, error) {
	ctx := udsAuthContext{
		rules:       rules,
		queries:     queries,
		peerCreds:   peerCreds,
		scopedCache: map[string]map[string]any{},
	}
	return ctx.authenticatePeer()
}

func (ctx *udsAuthContext) authenticatePeer() (udsPeerAuthResult, error) {
	result := udsPeerAuthResult{}
	peerCredsStr := fmt.Sprintf("uid=%d,gid=%d,pid=%d", ctx.peerCreds.UID, ctx.peerCreds.GID, ctx.peerCreds.PID)

	var pubAllow, pubDeny, subAllow, subDeny []string
	var respMaxMsgs int
	var respExpires time.Duration
	hasPermissions := false

	var authenticatingRule *UDSRule // track THE one authenticating rule

	for _, rule := range ctx.rules {
		if rule.Match == nil {
			continue
		}
		matched, err := rule.Match.matches(ctx)
		if err != nil {
			return result, fmt.Errorf("rule %q: %w", rule.Username, err)
		}
		if !matched {
			continue
		}

		// Check if this rule grants authentication and does not conflict with other authenticating rules
		if rule.Username != _EMPTY_ {
			if authenticatingRule == nil || (authenticatingRule.Rolename != _EMPTY_ && rule.Rolename == _EMPTY_) {
				authenticatingRule = rule
				result.authenticated = true
				result.authenticatingRule = rule.Username
			} else if authenticatingRule.Rolename != _EMPTY_ {
				result.warnings = append(result.warnings,
					fmt.Sprintf("%s: multiple mixed user/role rules match: %q and %q", peerCredsStr, authenticatingRule.Username, rule.Username))
				result.authenticated = false
				return result, nil
			} else if rule.Rolename == _EMPTY_ {
				result.warnings = append(result.warnings,
					fmt.Sprintf("%s: multiple user rules match: %q and %q", peerCredsStr, authenticatingRule.Username, rule.Username))
				result.authenticated = false
				return result, nil
			}
		}

		if rule.Rolename != _EMPTY_ {
			result.roles = append(result.roles, rule.Rolename)
		}

		// Merge permissions
		if rule.Permissions != nil {
			hasPermissions = true
			if rule.Permissions.Publish != nil {
				pubAllow = append(pubAllow, rule.Permissions.Publish.Allow...)
				pubDeny = append(pubDeny, rule.Permissions.Publish.Deny...)
			}
			if rule.Permissions.Subscribe != nil {
				subAllow = append(subAllow, rule.Permissions.Subscribe.Allow...)
				subDeny = append(subDeny, rule.Permissions.Subscribe.Deny...)
			}
			if rule.Permissions.Response != nil {
				if rule.Permissions.Response.MaxMsgs > respMaxMsgs {
					respMaxMsgs = rule.Permissions.Response.MaxMsgs
				}
				if rule.Permissions.Response.Expires > respExpires {
					respExpires = rule.Permissions.Response.Expires
				}
			}
		}
	}

	if !result.authenticated || authenticatingRule == nil {
		return result, nil
	}
	if !hasPermissions {
		result.authenticated = false
		result.warnings = append(result.warnings, fmt.Sprintf("%q: no permissions defined", peerCredsStr))
		return result, nil
	}
	if len(pubAllow) == 0 && len(pubDeny) == 0 && len(subAllow) == 0 && len(subDeny) == 0 {
		result.authenticated = false
		result.warnings = append(result.warnings, fmt.Sprintf("%q: empty permissions (no default allow policy for UDS connections)", peerCredsStr))
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

	if len(result.roles) > 0 {
		result.identity = fmt.Sprintf("%s:%s:%s", authenticatingRule.Username, strings.Join(result.roles, ","), peerCredsStr)
	} else {
		result.identity = fmt.Sprintf("%s::%s", authenticatingRule.Username, peerCredsStr)
	}

	return result, nil
}

// matches checks if peer credentials in context match the pattern (OR of ANDs).
// Returns true if any alternative in the pattern matches (all conditions in that alternative are true).
func (p UDSRulePattern) matches(ctx *udsAuthContext) (bool, error) {
	// OR: any alternative matching is sufficient
	for _, alt := range p {
		matched, err := matchAlternative(alt, *ctx)
		if err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

// matchAlternative checks if all conditions in a single alternative match (AND logic).
func matchAlternative(alt map[UDSRuleExpression]any, ctx udsAuthContext) (bool, error) {
	for expression, value := range alt {
		query, ok := ctx.queries[expression.QueryName]
		if !ok {
			return false, fmt.Errorf("unknown query %q", expression.QueryName)
		}

		// Provide the query with a cache, scope is enforced in RegisterCustomUDSPeerCredQuery
		// and trusted for builtin queries. Important to prevent cache poisoning by 3rd-party
		// queries, isolating the impact of malicious query implementation to a single expression.
		cache := ctx.scopedCache[query.cacheScope]
		if query.cacheScope != _EMPTY_ && cache == nil {
			cache = map[string]any{}
			ctx.scopedCache[query.cacheScope] = cache
		}

		// No need to validate value, this has been done in configuration (validation is for fast feedback of config)

		matched, err := query.evaluate(expression.QueryName, ctx.peerCreds, value, cache)
		if err != nil {
			return false, fmt.Errorf("query %q: %w", expression.QueryName, err)
		}
		if expression.Negate {
			matched = !matched
		}

		if !matched {
			return false, nil // AND: all must match
		}
	}
	return true, nil
}

// UDSPeerCredQueryValidator used in configuration validation to provide early
// notifications for errors there (avoid starting the server if auth config is
// not valid).
type UDSPeerCredQueryValidator func(patternValue any) error

// UDSPeerCredQueryPredicate evaluates a query against peer credentials and
// returns true if the peer credentials satisfy the predicate on the given
// pattern value. The implementation may use the cache to get results from
// previous evaluations of this or related queries (in the same
// udsPeerCredQuery CashScope).
type UDSPeerCredQueryPredicate func(queryName string, creds UDSPeerCreds, patternValue any, cache map[string]any) (bool, error)

type udsPeerCredQuery struct {
	// Queries can use a scoped cache where they can cache values during the auth process. Builtins
	// use a shared cache of "builtin:system" for all caching queries. Custom queries can define a
	// scope on registration, which will be prefixed with "custom:" to enforce the isolation from
	// trusted builtin queries.
	cacheScope string
	// Used by configuration validation to determine if a pattern value is valid for this query.
	validate UDSPeerCredQueryValidator
	// Implements the query. `ctx` lifetime is an auth process or bound to `creds`, can use cache for different `patternValues`.
	evaluate UDSPeerCredQueryPredicate
}

var builtinPeerCredQueries = map[string]udsPeerCredQuery{
	"uid":         {evaluate: peerCredQueryUID, validate: validateIsInt, cacheScope: _EMPTY_},
	"gid":         {evaluate: peerCredQueryGID, validate: validateIsInt, cacheScope: _EMPTY_},
	"pid":         {evaluate: peerCredQueryPID, validate: validateIsInt, cacheScope: _EMPTY_},
	"uid.name":    {evaluate: peerCredQueryUIDName, validate: validateIsString, cacheScope: "builtin:system"},
	"gid.name":    {evaluate: peerCredQueryGIDName, validate: validateIsString, cacheScope: "builtin:system"},
	"groups":      {evaluate: peerCredQueryGroups, validate: validateIsInt, cacheScope: "builtin:system"},
	"groups.name": {evaluate: peerCredQueryGroupsName, validate: validateIsString, cacheScope: "builtin:system"},
}

var udsPeerCredQueryNameRegex *regexp.Regexp

// RegisterCustomUDSPeerCredQuery registers a custom UDS peer credential query.
// Must be called before Server.Start() and requires UDS to be configured. The
// query name must match the regexp `^[a-z][a-z0-9_-]*(\.[a-z][a-z0-9_-]*)*$`.
// The predicate is required; the validator may be nil. If cacheScope is not
// empty, the evaluate method can use a cache identified by the given name.
// Returns an error if the predicate is nil, UDS is not configured, the server is
// already running, the query name is invalid (does not match the regexp), or if
// a query is already registered for the name.
func (s *Server) RegisterCustomUDSPeerCredQuery(
	queryName string,
	predicate UDSPeerCredQueryPredicate,
	validator UDSPeerCredQueryValidator,
	cacheScope string,
) error {
	if predicate == nil {
		return fmt.Errorf("cannot register UDS peer credential query with undefined predicate")
	}
	if s.isRunning() {
		return fmt.Errorf("cannot register UDS peer credential queries after server start")
	}
	if s.uds == nil {
		return fmt.Errorf("cannot register UDS peer credential queries if UDS not configured")
	}
	_, ok := s.uds.peerCredentialQueries[queryName]
	if ok {
		return fmt.Errorf("cannot register UDS peer credential query '%q': already registered", queryName)
	}
	// Assuming custom queries are rarely used, the RT is not an issue here:
	if udsPeerCredQueryNameRegex == nil {
		udsPeerCredQueryNameRegex = regexp.MustCompile(`^[a-z][a-z0-9_-]*(\.[a-z][a-z0-9_-]*)*$`)
	}
	if !udsPeerCredQueryNameRegex.MatchString(queryName) {
		return fmt.Errorf("invalid UDS peer credential query name '%q', must match '%q'", queryName, udsPeerCredQueryNameRegex.String())
	}
	if cacheScope != _EMPTY_ {
		cacheScope = "custom:" + cacheScope
	}
	s.uds.peerCredentialQueries[queryName] = udsPeerCredQuery{
		cacheScope: cacheScope,
		validate:   validator,
		evaluate:   predicate,
	}
	return nil
}

func peerCredQueryUID(name string, c UDSPeerCreds, v any, _ map[string]any) (bool, error) {
	val, ok := asInt64(v)
	if !ok {
		return false, fmt.Errorf("%s: expected integer, got %T", name, v)
	}
	return int64(c.UID) == val, nil
}

func peerCredQueryGID(name string, c UDSPeerCreds, v any, _ map[string]any) (bool, error) {
	val, ok := asInt64(v)
	if !ok {
		return false, fmt.Errorf("%s: expected integer, got %T", name, v)
	}
	return int64(c.GID) == val, nil
}

func peerCredQueryPID(name string, c UDSPeerCreds, v any, _ map[string]any) (bool, error) {
	val, ok := asInt64(v)
	if !ok {
		return false, fmt.Errorf("%s: expected integer, got %T", name, v)
	}
	return int64(c.PID) == val, nil
}

func asInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	}
	return 0, false
}

func validateIsInt(v any) error {
	switch v.(type) {
	case int64, int:
		return nil
	default:
		return fmt.Errorf("expected integer, got %T", v)
	}
}

func validateIsString(v any) error {
	if _, ok := v.(string); !ok {
		return fmt.Errorf("expected string, got %T", v)
	}
	return nil
}
