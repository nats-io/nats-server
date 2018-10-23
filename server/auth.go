// Copyright 2012-2018 The NATS Authors
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
	"crypto/tls"
	"encoding/base64"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nkeys"
	"golang.org/x/crypto/bcrypt"
)

// Authentication is an interface for implementing authentication
type Authentication interface {
	// Check if a client is authorized to connect
	Check(c ClientAuthentication) bool
}

// ClientAuthentication is an interface for client authentication
type ClientAuthentication interface {
	// Get options associated with a client
	GetOpts() *clientOpts
	// If TLS is enabled, TLS ConnectionState, nil otherwise
	GetTLSConnectionState() *tls.ConnectionState
	// Optionally map a user after auth.
	RegisterUser(*User)
}

// For backwards compatibility, users who are not explicitly defined into an
// account will be grouped in the default global account.
const globalAccountName = "$G"

// Accounts
type Account struct {
	Name     string
	Nkey     string
	mu       sync.RWMutex
	sl       *Sublist
	imports  importMap
	exports  exportMap
	nae      int
	maxnae   int
	maxaettl time.Duration
	pruning  bool
	checksti bool // check stream imports during a config reload
}

// Import stream mapping struct
type streamImport struct {
	acc    *Account
	from   string
	prefix string
}

// Import service mapping struct
type serviceImport struct {
	acc  *Account
	from string
	to   string
	ae   bool
	ts   int64
}

// importMap tracks the imported streams and services.
type importMap struct {
	streams  map[string]*streamImport
	services map[string]*serviceImport // TODO(dlc) sync.Map may be better.
}

// exportMap tracks the exported streams and services.
type exportMap struct {
	streams  map[string]map[string]*Account
	services map[string]map[string]*Account
}

func (a *Account) addServiceExport(subject string, accounts []*Account) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a == nil {
		return ErrMissingAccount
	}
	if a.exports.services == nil {
		a.exports.services = make(map[string]map[string]*Account)
	}
	ma := a.exports.services[subject]
	if accounts != nil && ma == nil {
		ma = make(map[string]*Account)
	}
	for _, a := range accounts {
		ma[a.Name] = a
	}
	a.exports.services[subject] = ma
	return nil
}

// numServiceRoutes returns the number of service routes on this account.
func (a *Account) numServiceRoutes() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.imports.services)
}

// This will add a route to an account to send published messages / requests
// to the destination account. From is the local subject to map, To is the
// subject that will appear on the destination account. Destination will need
// to have an import rule to allow access via addService.
func (a *Account) addServiceImport(destination *Account, from, to string) error {
	if destination == nil {
		return ErrMissingAccount
	}
	// Empty means use from.
	if to == "" {
		to = from
	}
	if !IsValidLiteralSubject(from) || !IsValidLiteralSubject(to) {
		return ErrInvalidSubject
	}
	// First check to see if the account has authorized us to route to the "to" subject.
	if !destination.checkServiceImportAuthorized(a, to) {
		return ErrServiceImportAuthorization
	}

	return a.addImplicitServiceImport(destination, from, to, false)
}

// removeServiceImport will remove the route by subject.
func (a *Account) removeServiceImport(subject string) {
	a.mu.Lock()
	si, ok := a.imports.services[subject]
	if ok && si != nil && si.ae {
		a.nae--
	}
	delete(a.imports.services, subject)
	a.mu.Unlock()
}

// Return the number of AutoExpireResponseMaps for request/reply. These are mapped to the account that
func (a *Account) numAutoExpireResponseMaps() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.nae
}

// Set the max outstanding auto expire response maps.
func (a *Account) setMaxAutoExpireResponseMaps(max int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.maxnae = max
}

// Set the max ttl for response maps.
func (a *Account) setMaxAutoExpireTTL(ttl time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.maxaettl = ttl
}

// Return a list of the current autoExpireResponseMaps.
func (a *Account) autoExpireResponseMaps() []*serviceImport {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.imports.services) == 0 {
		return nil
	}
	aesis := make([]*serviceImport, 0, len(a.imports.services))
	for _, si := range a.imports.services {
		if si.ae {
			aesis = append(aesis, si)
		}
	}
	sort.Slice(aesis, func(i, j int) bool {
		return aesis[i].ts < aesis[j].ts
	})
	return aesis
}

// Add a route to connect from an implicit route created for a response to a request.
// This does no checks and should be only called by the msg processing code. Use addRoute
// above if responding to user input or config, etc.
func (a *Account) addImplicitServiceImport(destination *Account, from, to string, autoexpire bool) error {
	a.mu.Lock()
	if a.imports.services == nil {
		a.imports.services = make(map[string]*serviceImport)
	}
	si := &serviceImport{destination, from, to, autoexpire, 0}
	a.imports.services[from] = si
	if autoexpire {
		a.nae++
		si.ts = time.Now().Unix()
		if a.nae > a.maxnae && !a.pruning {
			a.pruning = true
			go a.pruneAutoExpireResponseMaps()
		}
	}
	a.mu.Unlock()
	return nil
}

// This will prune the list to below the threshold and remove all ttl'd maps.
func (a *Account) pruneAutoExpireResponseMaps() {
	defer func() {
		a.mu.Lock()
		a.pruning = false
		a.mu.Unlock()
	}()

	a.mu.RLock()
	ttl := int64(a.maxaettl/time.Second) + 1
	a.mu.RUnlock()

	for {
		sis := a.autoExpireResponseMaps()

		// Check ttl items.
		now := time.Now().Unix()
		for i, si := range sis {
			if now-si.ts >= ttl {
				a.removeServiceImport(si.from)
			} else {
				sis = sis[i:]
				break
			}
		}

		a.mu.RLock()
		numOver := a.nae - a.maxnae
		a.mu.RUnlock()

		if numOver <= 0 {
			return
		} else if numOver >= len(sis) {
			numOver = len(sis) - 1
		}
		// These are in sorted order, remove at least numOver
		for _, si := range sis[:numOver] {
			a.removeServiceImport(si.from)
		}
	}
}

// addStreamImport will add in the stream import from a specific account.
func (a *Account) addStreamImport(account *Account, from, prefix string) error {
	if account == nil {
		return ErrMissingAccount
	}

	// First check to see if the account has authorized export of the subject.
	if !account.checkStreamImportAuthorized(a, from) {
		return ErrStreamImportAuthorization
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.imports.streams == nil {
		a.imports.streams = make(map[string]*streamImport)
	}
	if prefix != "" && prefix[len(prefix)-1] != btsep {
		prefix = prefix + string(btsep)
	}
	// TODO(dlc) - collisions, etc.
	a.imports.streams[from] = &streamImport{account, from, prefix}
	return nil
}

// Placeholder to denote public export.
var isPublicExport = []*Account(nil)

// addExport will add an export to the account. If accounts is nil
// it will signify a public export, meaning anyone can impoort.
func (a *Account) addStreamExport(subject string, accounts []*Account) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a == nil {
		return ErrMissingAccount
	}
	if a.exports.streams == nil {
		a.exports.streams = make(map[string]map[string]*Account)
	}
	var ma map[string]*Account
	for _, aa := range accounts {
		if ma == nil {
			ma = make(map[string]*Account, len(accounts))
		}
		ma[aa.Name] = aa
	}
	a.exports.streams[subject] = ma
	return nil
}

// Check if another account is authorized to import from us.
func (a *Account) checkStreamImportAuthorized(account *Account, subject string) bool {
	// Find the subject in the exports list.
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.exports.streams == nil || !IsValidSubject(subject) {
		return false
	}

	// Check direct match of subject first
	am, ok := a.exports.streams[subject]
	if ok {
		// if am is nil that denotes a public export
		if am == nil {
			return true
		}
		// If we have a matching account we are authorized
		_, ok := am[account.Name]
		return ok
	}
	// ok if we are here we did not match directly so we need to test each one.
	// The import subject arg has to take precedence, meaning the export
	// has to be a true subset of the import claim. We already checked for
	// exact matches above.

	tokens := strings.Split(subject, tsep)
	for subj, am := range a.exports.streams {
		if isSubsetMatch(tokens, subj) {
			if am == nil {
				return true
			}
			_, ok := am[account.Name]
			return ok
		}
	}
	return false
}

// Returns true of `a` and `b` stream imports are the same. Note that the
// check is done with the account's name, not the pointer. This is used
// during config reload where we are comparing current and new config
// in which pointers are different.
// No lock is acquired in this function, so it is assumed that the
// import maps are not changed while this execute.
func (a *Account) checkStreamImportsEqual(b *Account) bool {
	if len(a.imports.streams) != len(b.imports.streams) {
		return false
	}
	for subj, aim := range a.imports.streams {
		bim := b.imports.streams[subj]
		if bim == nil {
			return false
		}
		if aim.acc.Name != bim.acc.Name || aim.from != bim.from || aim.prefix != bim.prefix {
			return false
		}
	}
	return true
}

// Check if another account is authorized to route requests to this service.
func (a *Account) checkServiceImportAuthorized(account *Account, subject string) bool {
	// Find the subject in the services list.
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.exports.services == nil || !IsValidLiteralSubject(subject) {
		return false
	}
	// These are always literal subjects so just lookup.
	am, ok := a.exports.services[subject]
	if !ok {
		return false
	}
	// Check to see if we are public or if we need to search for the account.
	if am == nil {
		return true
	}
	// Check that we allow this account.
	_, ok = am[account.Name]
	return ok
}

// Nkey is for multiple nkey based users
type NkeyUser struct {
	Nkey        string       `json:"user"`
	Permissions *Permissions `json:"permissions,omitempty"`
	Account     *Account     `json:"account,omitempty"`
}

// User is for multiple accounts/users.
type User struct {
	Username    string       `json:"user"`
	Password    string       `json:"password"`
	Permissions *Permissions `json:"permissions,omitempty"`
	Account     *Account     `json:"account,omitempty"`
}

// clone performs a deep copy of the User struct, returning a new clone with
// all values copied.
func (u *User) clone() *User {
	if u == nil {
		return nil
	}
	clone := &User{}
	*clone = *u
	clone.Permissions = u.Permissions.clone()
	return clone
}

// clone performs a deep copy of the NkeyUser struct, returning a new clone with
// all values copied.
func (n *NkeyUser) clone() *NkeyUser {
	if n == nil {
		return nil
	}
	clone := &NkeyUser{}
	*clone = *n
	clone.Permissions = n.Permissions.clone()
	return clone
}

// SubjectPermission is an individual allow and deny struct for publish
// and subscribe authorizations.
type SubjectPermission struct {
	Allow []string `json:"allow,omitempty"`
	Deny  []string `json:"deny,omitempty"`
}

// Permissions are the allowed subjects on a per
// publish or subscribe basis.
type Permissions struct {
	Publish   *SubjectPermission `json:"publish"`
	Subscribe *SubjectPermission `json:"subscribe"`
}

// RoutePermissions are similar to user permissions
// but describe what a server can import/export from and to
// another server.
type RoutePermissions struct {
	Import *SubjectPermission `json:"import"`
	Export *SubjectPermission `json:"export"`
}

// clone will clone an individual subject permission.
func (p *SubjectPermission) clone() *SubjectPermission {
	if p == nil {
		return nil
	}
	clone := &SubjectPermission{}
	if p.Allow != nil {
		clone.Allow = make([]string, len(p.Allow))
		copy(clone.Allow, p.Allow)
	}
	if p.Deny != nil {
		clone.Deny = make([]string, len(p.Deny))
		copy(clone.Deny, p.Deny)
	}
	return clone
}

// clone performs a deep copy of the Permissions struct, returning a new clone
// with all values copied.
func (p *Permissions) clone() *Permissions {
	if p == nil {
		return nil
	}
	clone := &Permissions{}
	if p.Publish != nil {
		clone.Publish = p.Publish.clone()
	}
	if p.Subscribe != nil {
		clone.Subscribe = p.Subscribe.clone()
	}
	return clone
}

// checkAuthforWarnings will look for insecure settings and log concerns.
// Lock is assumed held.
func (s *Server) checkAuthforWarnings() {
	warn := false
	if s.opts.Password != "" && !isBcrypt(s.opts.Password) {
		warn = true
	}
	for _, u := range s.users {
		if !isBcrypt(u.Password) {
			warn = true
			break
		}
	}
	if warn {
		// Warning about using plaintext passwords.
		s.Warnf("Plaintext passwords detected, use nkeys or bcrypt.")
	}
}

// configureAuthorization will do any setup needed for authorization.
// Lock is assumed held.
func (s *Server) configureAuthorization() {
	if s.opts == nil {
		return
	}

	// Snapshot server options.
	opts := s.getOpts()

	// Check for multiple users first
	// This just checks and sets up the user map if we have multiple users.
	if opts.CustomClientAuthentication != nil {
		s.info.AuthRequired = true
	} else if opts.Nkeys != nil || opts.Users != nil {
		// Support both at the same time.
		if opts.Nkeys != nil {
			s.nkeys = make(map[string]*NkeyUser)
			for _, u := range opts.Nkeys {
				s.nkeys[u.Nkey] = u
			}
		}
		if opts.Users != nil {
			s.users = make(map[string]*User)
			for _, u := range opts.Users {
				s.users[u.Username] = u
			}
		}
		s.info.AuthRequired = true
	} else if opts.Username != "" || opts.Authorization != "" {
		s.info.AuthRequired = true
	} else {
		s.users = nil
		s.info.AuthRequired = false
	}
}

// checkAuthorization will check authorization based on client type and
// return boolean indicating if client is authorized.
func (s *Server) checkAuthorization(c *client) bool {
	switch c.typ {
	case CLIENT:
		return s.isClientAuthorized(c)
	case ROUTER:
		return s.isRouterAuthorized(c)
	default:
		return false
	}
}

// isClientAuthorized will check the client against the proper authorization method and data.
// This could be nkey, token, or username/password based.
func (s *Server) isClientAuthorized(c *client) bool {
	// Snapshot server options by hand and only grab what we really need.
	s.optsMu.RLock()
	customClientAuthentication := s.opts.CustomClientAuthentication
	authorization := s.opts.Authorization
	username := s.opts.Username
	password := s.opts.Password
	s.optsMu.RUnlock()

	// Check custom auth first, then nkeys, then multiple users, then token, then single user/pass.
	if customClientAuthentication != nil {
		return customClientAuthentication.Check(c)
	}

	var nkey *NkeyUser
	var user *User
	var ok bool

	s.mu.Lock()
	authRequired := s.info.AuthRequired
	if !authRequired {
		// TODO(dlc) - If they send us credentials should we fail?
		s.mu.Unlock()
		return true
	}

	// Check if we have nkeys or users for client.
	hasNkeys := s.nkeys != nil
	hasUsers := s.users != nil
	if hasNkeys && c.opts.Nkey != "" {
		nkey, ok = s.nkeys[c.opts.Nkey]
		if !ok {
			s.mu.Unlock()
			return false
		}
	} else if hasUsers && c.opts.Username != "" {
		user, ok = s.users[c.opts.Username]
		if !ok {
			s.mu.Unlock()
			return false
		}
	}
	s.mu.Unlock()

	// Verify the signature against the nonce.
	if nkey != nil {
		if c.opts.Sig == "" {
			return false
		}
		sig, err := base64.StdEncoding.DecodeString(c.opts.Sig)
		if err != nil {
			return false
		}
		pub, err := nkeys.FromPublicKey(c.opts.Nkey)
		if err != nil {
			return false
		}
		if err := pub.Verify(c.nonce, sig); err != nil {
			return false
		}
		c.RegisterNkeyUser(nkey)
		return true
	}

	if user != nil {
		ok = comparePasswords(user.Password, c.opts.Password)
		// If we are authorized, register the user which will properly setup any permissions
		// for pub/sub authorizations.
		if ok {
			c.RegisterUser(user)
		}
		return ok
	}

	if authorization != "" {
		return comparePasswords(authorization, c.opts.Authorization)
	} else if username != "" {
		if username != c.opts.Username {
			return false
		}
		return comparePasswords(password, c.opts.Password)
	}

	return false
}

// checkRouterAuth checks optional router authorization which can be nil or username/password.
func (s *Server) isRouterAuthorized(c *client) bool {
	// Snapshot server options.
	opts := s.getOpts()

	if s.opts.CustomRouterAuthentication != nil {
		return s.opts.CustomRouterAuthentication.Check(c)
	}

	if opts.Cluster.Username == "" {
		return true
	}

	if opts.Cluster.Username != c.opts.Username {
		return false
	}
	if !comparePasswords(opts.Cluster.Password, c.opts.Password) {
		return false
	}
	return true
}

// Support for bcrypt stored passwords and tokens.
const bcryptPrefix = "$2a$"

// isBcrypt checks whether the given password or token is bcrypted.
func isBcrypt(password string) bool {
	return strings.HasPrefix(password, bcryptPrefix)
}

func comparePasswords(serverPassword, clientPassword string) bool {
	// Check to see if the server password is a bcrypt hash
	if isBcrypt(serverPassword) {
		if err := bcrypt.CompareHashAndPassword([]byte(serverPassword), []byte(clientPassword)); err != nil {
			return false
		}
	} else if serverPassword != clientPassword {
		return false
	}
	return true
}
