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
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/jwt"
)

// For backwards compatibility, users who are not explicitly defined into an
// account will be grouped in the default global account.
const globalAccountName = "$G"

// Route Map Entry - used for efficient interest graph propagation.
// TODO(dlc) - squeeze even more?
type rme struct {
	qi int   // used to index into key from map for optional queue name
	n  int32 // number of subscriptions directly matching, local subs only.
}

// Account are subject namespace definitions. By default no messages are shared between accounts.
// You can share via exports and imports of streams and services.
type Account struct {
	Name     string
	Nkey     string
	Issuer   string
	claimJWT string
	updated  time.Time
	mu       sync.RWMutex
	sl       *Sublist
	etmr     *time.Timer
	clients  map[*client]*client
	rm       map[string]*rme
	imports  importMap
	exports  exportMap
	limits
	nae     int
	pruning bool
	expired bool
}

// Account based limits.
type limits struct {
	mpay     int32
	msubs    int
	mconns   int
	maxnae   int
	maxaettl time.Duration
}

// Import stream mapping struct
type streamImport struct {
	acc     *Account
	from    string
	prefix  string
	claim   *jwt.Import
	invalid bool
}

// Import service mapping struct
type serviceImport struct {
	acc     *Account
	from    string
	to      string
	ae      bool
	ts      int64
	claim   *jwt.Import
	invalid bool
}

// exportAuth holds configured approvals or boolean indicating an
// auth token is required for import.
type exportAuth struct {
	tokenReq bool
	approved map[string]*Account
}

// importMap tracks the imported streams and services.
type importMap struct {
	streams  map[string]*streamImport
	services map[string]*serviceImport // TODO(dlc) sync.Map may be better.
}

// exportMap tracks the exported streams and services.
type exportMap struct {
	streams  map[string]*exportAuth
	services map[string]*exportAuth
}

// NumClients returns active number of clients for this account.
func (a *Account) NumClients() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.clients)
}

// MaxClientsReached returns if we have reached our limit for number of connections.
func (a *Account) MaxClientsReached() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.mconns != 0 {
		return len(a.clients) >= a.mconns
	}
	return false
}

// RoutedSubs returns how many subjects we would send across a route when first
// connected or expressing interest. Local client subs.
func (a *Account) RoutedSubs() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.rm)
}

// TotalSubs returns total number of Subscriptions for this account.
func (a *Account) TotalSubs() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return int(a.sl.Count())
}

// addClient keeps our accounting of active clients updated.
// Returns previous total.
func (a *Account) addClient(c *client) int {
	a.mu.Lock()
	n := len(a.clients)
	a.clients[c] = c
	a.mu.Unlock()
	return n
}

// removeClient keeps our accounting of active clients updated.
func (a *Account) removeClient(c *client) int {
	a.mu.Lock()
	n := len(a.clients)
	delete(a.clients, c)
	a.mu.Unlock()
	return n
}

// AddServiceExport will configure the account with the defined export.
func (a *Account) AddServiceExport(subject string, accounts []*Account) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a == nil {
		return ErrMissingAccount
	}
	if a.exports.services == nil {
		a.exports.services = make(map[string]*exportAuth)
	}
	ea := a.exports.services[subject]
	if accounts != nil {
		if ea == nil {
			ea = &exportAuth{}
		}
		// empty means auth required but will be import token.
		if len(accounts) == 0 {
			ea.tokenReq = true
		} else {
			if ea.approved == nil {
				ea.approved = make(map[string]*Account, len(accounts))
			}
			for _, acc := range accounts {
				ea.approved[acc.Name] = acc
			}
		}
	}
	a.exports.services[subject] = ea
	return nil
}

// numServiceRoutes returns the number of service routes on this account.
func (a *Account) numServiceRoutes() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.imports.services)
}

func (a *Account) AddServiceImportWithClaim(destination *Account, from, to string, imClaim *jwt.Import) error {
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
	if !destination.checkServiceImportAuthorized(a, to, imClaim) {
		return ErrServiceImportAuthorization
	}

	return a.addImplicitServiceImport(destination, from, to, false, imClaim)
}

// AddServiceImport will add a route to an account to send published messages / requests
// to the destination account. From is the local subject to map, To is the
// subject that will appear on the destination account. Destination will need
// to have an import rule to allow access via addService.
func (a *Account) AddServiceImport(destination *Account, from, to string) error {
	return a.AddServiceImportWithClaim(destination, from, to, nil)
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
// has the service import.
func (a *Account) numAutoExpireResponseMaps() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.nae
}

// MaxAutoExpireResponseMaps return the maximum number of
// auto expire response maps we will allow.
func (a *Account) MaxAutoExpireResponseMaps() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.maxnae
}

// SetMaxAutoExpireResponseMaps sets the max outstanding auto expire response maps.
func (a *Account) SetMaxAutoExpireResponseMaps(max int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.maxnae = max
}

// AutoExpireTTL returns the ttl for response maps.
func (a *Account) AutoExpireTTL() time.Duration {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.maxaettl
}

// SetAutoExpireTTL sets the ttl for response maps.
func (a *Account) SetAutoExpireTTL(ttl time.Duration) {
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
// This does no checks and should be only called by the msg processing code. Use
// addServiceImport from above if responding to user input or config, etc.
func (a *Account) addImplicitServiceImport(destination *Account, from, to string, autoexpire bool, claim *jwt.Import) error {
	a.mu.Lock()
	if a.imports.services == nil {
		a.imports.services = make(map[string]*serviceImport)
	}
	si := &serviceImport{destination, from, to, autoexpire, 0, claim, false}
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

// AddStreamImportWithClaim will add in the stream import from a specific account with optional token.
func (a *Account) AddStreamImportWithClaim(account *Account, from, prefix string, imClaim *jwt.Import) error {
	if account == nil {
		return ErrMissingAccount
	}

	// First check to see if the account has authorized export of the subject.
	if !account.checkStreamImportAuthorized(a, from, imClaim) {
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
	a.imports.streams[from] = &streamImport{account, from, prefix, imClaim, false}
	return nil
}

// AddStreamImport will add in the stream import from a specific account.
func (a *Account) AddStreamImport(account *Account, from, prefix string) error {
	return a.AddStreamImportWithClaim(account, from, prefix, nil)
}

// IsPublicExport is a placeholder to denote public export.
var IsPublicExport = []*Account(nil)

// AddStreamExport will add an export to the account. If accounts is nil
// it will signify a public export, meaning anyone can impoort.
func (a *Account) AddStreamExport(subject string, accounts []*Account) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a == nil {
		return ErrMissingAccount
	}
	if a.exports.streams == nil {
		a.exports.streams = make(map[string]*exportAuth)
	}
	ea := a.exports.streams[subject]
	if accounts != nil {
		if ea == nil {
			ea = &exportAuth{}
		}
		// empty means auth required but will be import token.
		if len(accounts) == 0 {
			ea.tokenReq = true
		} else {
			if ea.approved == nil {
				ea.approved = make(map[string]*Account, len(accounts))
			}
			for _, acc := range accounts {
				ea.approved[acc.Name] = acc
			}
		}
	}
	a.exports.streams[subject] = ea
	return nil
}

// Check if another account is authorized to import from us.
func (a *Account) checkStreamImportAuthorized(account *Account, subject string, imClaim *jwt.Import) bool {
	// Find the subject in the exports list.
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.checkStreamImportAuthorizedNoLock(account, subject, imClaim)
}

func (a *Account) checkStreamImportAuthorizedNoLock(account *Account, subject string, imClaim *jwt.Import) bool {
	if a.exports.streams == nil || !IsValidSubject(subject) {
		return false
	}

	// Check direct match of subject first
	ea, ok := a.exports.streams[subject]
	if ok {
		// if ea is nil that denotes a public export
		if ea == nil {
			return true
		}
		// Check if token required
		if ea.tokenReq {
			return a.checkActivation(account, imClaim, true)
		}
		// If we have a matching account we are authorized
		_, ok := ea.approved[account.Name]
		return ok
	}
	// ok if we are here we did not match directly so we need to test each one.
	// The import subject arg has to take precedence, meaning the export
	// has to be a true subset of the import claim. We already checked for
	// exact matches above.
	tokens := strings.Split(subject, tsep)
	for subj, ea := range a.exports.streams {
		if isSubsetMatch(tokens, subj) {
			if ea == nil || ea.approved == nil {
				return true
			}
			// Check if token required
			if ea.tokenReq {
				return a.checkActivation(account, imClaim, true)
			}
			_, ok := ea.approved[account.Name]
			return ok
		}
	}
	return false
}

// Will fetch the activation token for an import.
func fetchActivation(url string) string {
	// FIXME(dlc) - Make configurable.
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(url)
	if err != nil || resp == nil {
		return ""
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	return string(body)
}

// These are import stream specific versions for when an activation expires.
func (a *Account) streamActivationExpired(subject string) {
	a.mu.RLock()
	if a.expired || a.imports.streams == nil {
		a.mu.RUnlock()
		return
	}
	si := a.imports.streams[subject]
	if si == nil || si.invalid {
		a.mu.RUnlock()
		return
	}
	a.mu.RUnlock()

	if si.acc.checkActivation(a, si.claim, false) {
		// The token has been updated most likely and we are good to go.
		return
	}

	a.mu.Lock()
	si.invalid = true
	clients := make([]*client, 0, len(a.clients))
	for _, c := range a.clients {
		clients = append(clients, c)
	}
	awcsti := map[string]struct{}{a.Name: struct{}{}}
	a.mu.Unlock()
	for _, c := range clients {
		c.processSubsOnConfigReload(awcsti)
	}
}

// These are import service specific versions for when an activation expires.
func (a *Account) serviceActivationExpired(subject string) {
	a.mu.RLock()
	if a.expired || a.imports.services == nil {
		a.mu.RUnlock()
		return
	}
	si := a.imports.services[subject]
	if si == nil || si.invalid {
		a.mu.RUnlock()
		return
	}
	a.mu.RUnlock()

	if si.acc.checkActivation(a, si.claim, false) {
		// The token has been updated most likely and we are good to go.
		return
	}

	a.mu.Lock()
	si.invalid = true
	a.mu.Unlock()
}

// Fires for expired activation tokens. We could track this with timers etc.
// Instead we just re-analyze where we are and if we need to act.
func (a *Account) activationExpired(subject string, kind jwt.ExportType) {
	switch kind {
	case jwt.Stream:
		a.streamActivationExpired(subject)
	case jwt.Service:
		a.serviceActivationExpired(subject)
	}
}

// checkActivation will check the activation token for validity.
func (a *Account) checkActivation(acc *Account, claim *jwt.Import, expTimer bool) bool {
	if claim == nil || claim.Token == "" {
		return false
	}
	// Create a quick clone so we can inline Token JWT.
	clone := *claim

	// We grab the token from a URL by hand here since we need expiration etc.
	if url, err := url.Parse(clone.Token); err == nil && url.Scheme != "" {
		clone.Token = fetchActivation(url.String())
	}
	vr := jwt.CreateValidationResults()
	clone.Validate(a.Name, vr)
	if vr.IsBlocking(true) {
		return false
	}
	act, err := jwt.DecodeActivationClaims(clone.Token)
	if err != nil {
		return false
	}
	vr = jwt.CreateValidationResults()
	act.Validate(vr)
	if vr.IsBlocking(true) {
		return false
	}
	if act.Expires != 0 {
		tn := time.Now().Unix()
		if act.Expires <= tn {
			return false
		}
		if expTimer {
			expiresAt := time.Duration(act.Expires - tn)
			time.AfterFunc(expiresAt*time.Second, func() {
				acc.activationExpired(string(act.ImportSubject), claim.Type)
			})
		}
	}
	return true
}

// Returns true if `a` and `b` stream imports are the same. Note that the
// check is done with the account's name, not the pointer. This is used
// during config reload where we are comparing current and new config
// in which pointers are different.
// No lock is acquired in this function, so it is assumed that the
// import maps are not changed while this executes.
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

func (a *Account) checkStreamExportsEqual(b *Account) bool {
	if len(a.exports.streams) != len(b.exports.streams) {
		return false
	}
	for subj, aea := range a.exports.streams {
		bea, ok := b.exports.streams[subj]
		if !ok {
			return false
		}
		if !reflect.DeepEqual(aea, bea) {
			return false
		}
	}
	return true
}

func (a *Account) checkServiceExportsEqual(b *Account) bool {
	if len(a.exports.services) != len(b.exports.services) {
		return false
	}
	for subj, aea := range a.exports.services {
		bea, ok := b.exports.services[subj]
		if !ok {
			return false
		}
		if !reflect.DeepEqual(aea, bea) {
			return false
		}
	}
	return true
}

func (a *Account) checkServiceImportAuthorized(account *Account, subject string, imClaim *jwt.Import) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.checkServiceImportAuthorizedNoLock(account, subject, imClaim)
}

// Check if another account is authorized to route requests to this service.
func (a *Account) checkServiceImportAuthorizedNoLock(account *Account, subject string, imClaim *jwt.Import) bool {
	// Find the subject in the services list.
	if a.exports.services == nil || !IsValidLiteralSubject(subject) {
		return false
	}
	// These are always literal subjects so just lookup.
	ae, ok := a.exports.services[subject]
	if !ok {
		return false
	}

	if ae != nil && ae.tokenReq {
		return a.checkActivation(account, imClaim, true)
	}

	// Check to see if we are public or if we need to search for the account.
	if ae == nil || ae.approved == nil {
		return true
	}
	// Check that we allow this account.
	_, ok = ae.approved[account.Name]
	return ok
}

// IsExpired returns expiration status.
func (a *Account) IsExpired() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.expired
}

// Called when an account has expired.
func (a *Account) expiredTimeout() {
	// Collect the clients.
	a.mu.Lock()
	a.expired = true
	a.mu.Unlock()

	cs := make([]*client, 0, len(a.clients))
	a.mu.RLock()
	for c := range a.clients {
		cs = append(cs, c)
	}
	a.mu.RUnlock()

	for _, c := range cs {
		c.accountAuthExpired()
	}
}

// Sets the expiration timer for an account JWT that has it set.
func (a *Account) setExpirationTimer(d time.Duration) {
	a.etmr = time.AfterFunc(d, a.expiredTimeout)
}

// Lock should be held
func (a *Account) clearExpirationTimer() bool {
	if a.etmr == nil {
		return true
	}
	stopped := a.etmr.Stop()
	a.etmr = nil
	return stopped
}

func (a *Account) checkExpiration(claims *jwt.ClaimsData) {
	a.clearExpirationTimer()
	if claims.Expires == 0 {
		a.expired = false
		return
	}
	tn := time.Now().Unix()
	if claims.Expires <= tn {
		a.expired = true
		return
	}
	expiresAt := time.Duration(claims.Expires - tn)
	a.setExpirationTimer(expiresAt * time.Second)
	a.expired = false
}

// Placeholder for signaling token auth required.
var tokenAuthReq = []*Account{}

func authAccounts(tokenReq bool) []*Account {
	if tokenReq {
		return tokenAuthReq
	}
	return nil
}

// SetAccountResolver will assign the account resolver.
func (s *Server) SetAccountResolver(ar AccountResolver) {
	s.mu.Lock()
	s.accResolver = ar
	s.mu.Unlock()
}

// updateAccountClaims will update and existing account with new claims.
// This will replace any exports or imports previously defined.
func (s *Server) updateAccountClaims(a *Account, ac *jwt.AccountClaims) {
	if a == nil {
		return
	}
	a.checkExpiration(ac.Claims())

	// Clone to update
	old := &Account{Name: a.Name, imports: a.imports, exports: a.exports}

	// Reset exports and imports here.
	a.exports = exportMap{}
	a.imports = importMap{}

	gatherClients := func() []*client {
		a.mu.RLock()
		clients := make([]*client, 0, len(a.clients))
		for _, c := range a.clients {
			clients = append(clients, c)
		}
		a.mu.RUnlock()
		return clients
	}

	for _, e := range ac.Exports {
		switch e.Type {
		case jwt.Stream:
			if err := a.AddStreamExport(string(e.Subject), authAccounts(e.TokenReq)); err != nil {
				s.Debugf("Error adding stream export to account [%s]: %v", a.Name, err.Error())
			}
		case jwt.Service:
			if err := a.AddServiceExport(string(e.Subject), authAccounts(e.TokenReq)); err != nil {
				s.Debugf("Error adding service export to account [%s]: %v", a.Name, err.Error())
			}
		}
	}
	for _, i := range ac.Imports {
		acc := s.accounts[i.Account]
		if acc == nil {
			if acc = s.fetchAccount(i.Account); acc == nil {
				s.Debugf("Can't locate account [%s] for import of [%v] %s", i.Account, i.Subject, i.Type)
				continue
			}
		}
		switch i.Type {
		case jwt.Stream:
			a.AddStreamImportWithClaim(acc, string(i.Subject), string(i.To), i)
		case jwt.Service:
			a.AddServiceImportWithClaim(acc, string(i.Subject), string(i.To), i)
		}
	}
	// Now let's apply any needed changes from import/export changes.
	if !a.checkStreamImportsEqual(old) {
		awcsti := map[string]struct{}{a.Name: struct{}{}}
		for _, c := range gatherClients() {
			c.processSubsOnConfigReload(awcsti)
		}
	}
	// Now check if stream exports have changed.
	if !a.checkStreamExportsEqual(old) {
		clients := make([]*client, 0, 16)
		// We need to check all accounts that have an import claim from this account.
		awcsti := map[string]struct{}{}
		for _, acc := range s.accounts {
			acc.mu.Lock()
			for _, im := range acc.imports.streams {
				if im != nil && im.acc.Name == a.Name {
					// Check for if we are still authorized for an import.
					im.invalid = !a.checkStreamImportAuthorizedNoLock(im.acc, im.from, im.claim)
					awcsti[acc.Name] = struct{}{}
					for _, c := range acc.clients {
						clients = append(clients, c)
					}
				}
			}
			acc.mu.Unlock()
		}
		// Now walk clients.
		for _, c := range clients {
			c.processSubsOnConfigReload(awcsti)
		}
	}
	// Now check if service exports have changed.
	if !a.checkServiceExportsEqual(old) {
		for _, acc := range s.accounts {
			acc.mu.Lock()
			for _, im := range acc.imports.services {
				if im != nil && im.acc.Name == a.Name {
					// Check for if we are still authorized for an import.
					im.invalid = !a.checkServiceImportAuthorizedNoLock(im.acc, im.from, im.claim)
				}
			}
			acc.mu.Unlock()
		}
	}

	// Now do limits if they are present.
	a.msubs = int(ac.Limits.Subs)
	a.mpay = int32(ac.Limits.Payload)
	a.mconns = int(ac.Limits.Conn)
	for i, c := range gatherClients() {
		if a.mconns > 0 && i >= a.mconns {
			c.maxAccountConnExceeded()
			continue
		}
		c.mu.Lock()
		c.applyAccountLimits()
		c.mu.Unlock()
	}
}

// Helper to build an internal account structure from a jwt.AccountClaims.
func (s *Server) buildInternalAccount(ac *jwt.AccountClaims) *Account {
	acc := &Account{Name: ac.Subject, Issuer: ac.Issuer}
	s.updateAccountClaims(acc, ac)
	return acc
}

// Helper to build internal NKeyUser.
func buildInternalNkeyUser(uc *jwt.UserClaims, acc *Account) *NkeyUser {
	nu := &NkeyUser{Nkey: uc.Subject, Account: acc}

	// Now check for permissions.
	var p *Permissions

	if len(uc.Pub.Allow) > 0 || len(uc.Pub.Deny) > 0 {
		if p == nil {
			p = &Permissions{}
		}
		p.Publish = &SubjectPermission{}
		p.Publish.Allow = uc.Pub.Allow
		p.Publish.Deny = uc.Pub.Deny
	}
	if len(uc.Sub.Allow) > 0 || len(uc.Sub.Deny) > 0 {
		if p == nil {
			p = &Permissions{}
		}
		p.Subscribe = &SubjectPermission{}
		p.Subscribe.Allow = uc.Sub.Allow
		p.Subscribe.Deny = uc.Sub.Deny
	}
	nu.Permissions = p
	return nu
}

// AccountResolver interface. This is to fetch Account JWTs by public nkeys
type AccountResolver interface {
	Fetch(pub string) (string, error)
}

// Mostly for testing.
type MemAccResolver struct {
	sync.Map
}

func (m *MemAccResolver) Fetch(pub string) (string, error) {
	if j, ok := m.Load(pub); ok {
		return j.(string), nil
	}
	return "", ErrMissingAccount
}
