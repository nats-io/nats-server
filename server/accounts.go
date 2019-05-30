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

package server

import (
	"fmt"
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

// For backwards compatibility with NATS < 2.0, users who are not explicitly defined into an
// account will be grouped in the default global account.
const globalAccountName = "$G"

// Account are subject namespace definitions. By default no messages are shared between accounts.
// You can share via Exports and Imports of Streams and Services.
type Account struct {
	Name       string
	Nkey       string
	Issuer     string
	claimJWT   string
	updated    time.Time
	mu         sync.RWMutex
	sl         *Sublist
	etmr       *time.Timer
	ctmr       *time.Timer
	strack     map[string]sconns
	nrclients  int32
	sysclients int32
	nleafs     int32
	nrleafs    int32
	clients    map[*client]*client
	rm         map[string]int32
	lleafs     []*client
	imports    importMap
	exports    exportMap
	limits
	nae         int32
	pruning     bool
	expired     bool
	signingKeys []string
	srv         *Server // server this account is registered with (possibly nil)
}

// Account based limits.
type limits struct {
	mpay     int32
	msubs    int32
	mconns   int32
	mleafs   int32
	maxnae   int32
	maxaettl time.Duration
}

// Used to track remote clients and leafnodes per remote server.
type sconns struct {
	conns int32
	leafs int32
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

// NewAccount creates a new unlimited account with the given name.
func NewAccount(name string) *Account {
	a := &Account{
		Name:   name,
		sl:     NewSublist(),
		limits: limits{-1, -1, -1, -1, 0, 0},
	}
	return a
}

// Used to create shallow copies of accounts for transfer
// from opts to real accounts in server struct.
func (a *Account) shallowCopy() *Account {
	na := NewAccount(a.Name)
	na.Nkey = a.Nkey
	na.Issuer = a.Issuer
	na.imports = a.imports
	na.exports = a.exports
	return na
}

// NumConnections returns active number of clients for this account for
// all known servers.
func (a *Account) NumConnections() int {
	a.mu.RLock()
	nc := len(a.clients) + int(a.nrclients)
	a.mu.RUnlock()
	return nc
}

// NumLocalConnections returns active number of clients for this account
// on this server.
func (a *Account) NumLocalConnections() int {
	a.mu.RLock()
	nlc := a.numLocalConnections()
	a.mu.RUnlock()
	return nlc
}

// Do not account for the system accounts.
func (a *Account) numLocalConnections() int {
	return len(a.clients) - int(a.sysclients) - int(a.nleafs)
}

func (a *Account) numLocalLeafNodes() int {
	return int(a.nleafs)
}

// MaxTotalConnectionsReached returns if we have reached our limit for number of connections.
func (a *Account) MaxTotalConnectionsReached() bool {
	a.mu.RLock()
	mtc := a.maxTotalConnectionsReached()
	a.mu.RUnlock()
	return mtc
}

func (a *Account) maxTotalConnectionsReached() bool {
	if a.mconns != jwt.NoLimit {
		return len(a.clients)-int(a.sysclients)+int(a.nrclients) >= int(a.mconns)
	}
	return false
}

// MaxActiveConnections return the set limit for the account system
// wide for total number of active connections.
func (a *Account) MaxActiveConnections() int {
	a.mu.RLock()
	mconns := int(a.mconns)
	a.mu.RUnlock()
	return mconns
}

// MaxTotalLeafNodesReached returns if we have reached our limit for number of leafnodes.
func (a *Account) MaxTotalLeafNodesReached() bool {
	a.mu.RLock()
	mtc := a.maxTotalLeafNodesReached()
	a.mu.RUnlock()
	return mtc
}

func (a *Account) maxTotalLeafNodesReached() bool {
	if a.mleafs != jwt.NoLimit {
		return a.nleafs+a.nrleafs >= a.mleafs
	}
	return false
}

// NumLeafNodes returns the active number of local and remote
// leaf node connections.
func (a *Account) NumLeafNodes() int {
	a.mu.RLock()
	nln := int(a.nleafs + a.nrleafs)
	a.mu.RUnlock()
	return nln
}

// NumRemoteLeafNodes returns the active number of remote
// leaf node connections.
func (a *Account) NumRemoteLeafNodes() int {
	a.mu.RLock()
	nrn := int(a.nrleafs)
	a.mu.RUnlock()
	return nrn
}

// MaxActiveLeafNodes return the set limit for the account system
// wide for total number of leavenode connections.
// NOTE: these are tracked separately.
func (a *Account) MaxActiveLeafNodes() int {
	a.mu.RLock()
	mleafs := int(a.mleafs)
	a.mu.RUnlock()
	return mleafs
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

// addClient keeps our accounting of local active clients or leafnodes updated.
// Returns previous total.
func (a *Account) addClient(c *client) int {
	a.mu.Lock()
	n := len(a.clients)
	if a.clients != nil {
		a.clients[c] = c
	}
	added := n != len(a.clients)
	if added {
		if c.kind == SYSTEM {
			a.sysclients++
		} else if c.kind == LEAF {
			a.nleafs++
			a.lleafs = append(a.lleafs, c)
		}
	}
	a.mu.Unlock()
	if c != nil && c.srv != nil && a != c.srv.globalAccount() && added {
		c.srv.accConnsUpdate(a)
	}
	return n
}

// Helper function to remove leaf nodes. If number of leafnodes gets large
// this may need to be optimized out of linear search but believe number
// of active leafnodes per account scope to be small and therefore cache friendly.
// Lock should be held on account.
func (a *Account) removeLeafNode(c *client) {
	ll := len(a.lleafs)
	for i, l := range a.lleafs {
		if l == c {
			a.lleafs[i] = a.lleafs[ll-1]
			if ll == 1 {
				a.lleafs = nil
			} else {
				a.lleafs = a.lleafs[:ll-1]
			}
			return
		}
	}
}

// removeClient keeps our accounting of local active clients updated.
func (a *Account) removeClient(c *client) int {
	a.mu.Lock()
	n := len(a.clients)
	delete(a.clients, c)
	removed := n != len(a.clients)
	if removed {
		if c.kind == SYSTEM {
			a.sysclients--
		} else if c.kind == LEAF {
			a.nleafs--
			a.removeLeafNode(c)
		}
	}
	a.mu.Unlock()
	if c != nil && c.srv != nil && a != c.srv.gacc && removed {
		c.srv.accConnsUpdate(a)
	}
	return n
}

func (a *Account) randomClient() *client {
	var c *client
	for _, c = range a.clients {
		break
	}
	return c
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

// AddServiceImportWithClaim will add in the service import via the jwt claim.
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
	if a.srv != nil && a.srv.gateway.enabled {
		a.srv.gatewayHandleServiceImport(a, []byte(subject), nil, -1)
	}
}

// Return the number of AutoExpireResponseMaps for request/reply. These are mapped to the account that
// has the service import.
func (a *Account) numAutoExpireResponseMaps() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return int(a.nae)
}

// MaxAutoExpireResponseMaps return the maximum number of
// auto expire response maps we will allow.
func (a *Account) MaxAutoExpireResponseMaps() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return int(a.maxnae)
}

// SetMaxAutoExpireResponseMaps sets the max outstanding auto expire response maps.
func (a *Account) SetMaxAutoExpireResponseMaps(max int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.maxnae = int32(max)
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
// addServiceImport from above if responding to user input or config changes, etc.
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
		numOver := int(a.nae - a.maxnae)
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

// IsPublicExport is a placeholder to denote a public export.
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
	return a.checkExportApproved(account, subject, imClaim, a.exports.streams)
}

func (a *Account) checkExportApproved(account *Account, subject string, imClaim *jwt.Import, m map[string]*exportAuth) bool {
	// Check direct match of subject first
	ea, ok := m[subject]
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
	for subj, ea := range m {
		if isSubsetMatch(tokens, subj) {
			if ea == nil || ea.approved == nil && !ea.tokenReq {
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
	awcsti := map[string]struct{}{a.Name: {}}
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
	if !a.isIssuerClaimTrusted(act) {
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

// Returns true if the activation claim is trusted. That is the issuer matches
// the account or is an entry in the signing keys.
func (a *Account) isIssuerClaimTrusted(claims *jwt.ActivationClaims) bool {
	// if no issuer account, issuer is the account
	if claims.IssuerAccount == "" {
		return true
	}
	// get the referenced account
	if a.srv != nil {
		ia, err := a.srv.lookupAccount(claims.IssuerAccount)
		if err != nil {
			return false
		}
		return ia.hasIssuer(claims.Issuer)
	}
	// couldn't verify
	return false
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
	return a.checkExportApproved(account, subject, imClaim, a.exports.services)
}

// IsExpired returns expiration status.
func (a *Account) IsExpired() bool {
	a.mu.RLock()
	exp := a.expired
	a.mu.RUnlock()
	return exp
}

// Called when an account has expired.
func (a *Account) expiredTimeout() {
	// Mark expired first.
	a.mu.Lock()
	a.expired = true
	a.mu.Unlock()

	// Collect the clients and expire them.
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

// Check expiration and set the proper state as needed.
func (a *Account) checkExpiration(claims *jwt.ClaimsData) {
	a.mu.Lock()
	defer a.mu.Unlock()

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

// hasIssuer returns true if the issuer matches the account
// issuer or it is a signing key for the account.
func (a *Account) hasIssuer(issuer string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	// same issuer
	if a.Issuer == issuer {
		return true
	}
	for i := 0; i < len(a.signingKeys); i++ {
		if a.signingKeys[i] == issuer {
			return true
		}
	}
	return false
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

// AccountResolver returns the registered account resolver.
func (s *Server) AccountResolver() AccountResolver {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accResolver
}

// UpdateAccountClaims will call updateAccountClaims.
func (s *Server) UpdateAccountClaims(a *Account, ac *jwt.AccountClaims) {
	s.updateAccountClaims(a, ac)
}

// updateAccountClaims will update an existing account with new claims.
// This will replace any exports or imports previously defined.
func (s *Server) updateAccountClaims(a *Account, ac *jwt.AccountClaims) {
	if a == nil {
		return
	}
	s.Debugf("Updating account claims: %s", a.Name)
	a.checkExpiration(ac.Claims())

	a.mu.Lock()
	// Clone to update, only select certain fields.
	old := &Account{Name: a.Name, imports: a.imports, exports: a.exports, limits: a.limits, signingKeys: a.signingKeys}

	// Reset exports and imports here.
	a.exports = exportMap{}
	a.imports = importMap{}

	// update account signing keys
	a.signingKeys = nil
	signersChanged := false
	if len(ac.SigningKeys) > 0 {
		// insure copy the new keys and sort
		a.signingKeys = append(a.signingKeys, ac.SigningKeys...)
		sort.Strings(a.signingKeys)
	}
	if len(a.signingKeys) != len(old.signingKeys) {
		signersChanged = true
	} else {
		for i := 0; i < len(old.signingKeys); i++ {
			if a.signingKeys[i] != old.signingKeys[i] {
				signersChanged = true
				break
			}
		}
	}
	a.mu.Unlock()

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
			s.Debugf("Adding stream export %q for %s", e.Subject, a.Name)
			if err := a.AddStreamExport(string(e.Subject), authAccounts(e.TokenReq)); err != nil {
				s.Debugf("Error adding stream export to account [%s]: %v", a.Name, err.Error())
			}
		case jwt.Service:
			s.Debugf("Adding service export %q for %s", e.Subject, a.Name)
			if err := a.AddServiceExport(string(e.Subject), authAccounts(e.TokenReq)); err != nil {
				s.Debugf("Error adding service export to account [%s]: %v", a.Name, err.Error())
			}
		}
	}
	for _, i := range ac.Imports {
		var acc *Account
		if v, ok := s.accounts.Load(i.Account); ok {
			acc = v.(*Account)
		}
		if acc == nil {
			if acc, _ = s.fetchAccount(i.Account); acc == nil {
				s.Debugf("Can't locate account [%s] for import of [%v] %s", i.Account, i.Subject, i.Type)
				continue
			}
		}
		switch i.Type {
		case jwt.Stream:
			s.Debugf("Adding stream import %s:%q for %s:%q", acc.Name, i.Subject, a.Name, i.To)
			if err := a.AddStreamImportWithClaim(acc, string(i.Subject), string(i.To), i); err != nil {
				s.Debugf("Error adding stream import to account [%s]: %v", a.Name, err.Error())
			}
		case jwt.Service:
			s.Debugf("Adding service import %s:%q for %s:%q", acc.Name, i.Subject, a.Name, i.To)
			if err := a.AddServiceImportWithClaim(acc, string(i.Subject), string(i.To), i); err != nil {
				s.Debugf("Error adding service import to account [%s]: %v", a.Name, err.Error())
			}
		}
	}
	// Now let's apply any needed changes from import/export changes.
	if !a.checkStreamImportsEqual(old) {
		awcsti := map[string]struct{}{a.Name: {}}
		for _, c := range gatherClients() {
			c.processSubsOnConfigReload(awcsti)
		}
	}
	// Now check if stream exports have changed.
	if !a.checkStreamExportsEqual(old) || signersChanged {
		clients := make([]*client, 0, 16)
		// We need to check all accounts that have an import claim from this account.
		awcsti := map[string]struct{}{}
		s.accounts.Range(func(k, v interface{}) bool {
			acc := v.(*Account)
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
			return true
		})
		// Now walk clients.
		for _, c := range clients {
			c.processSubsOnConfigReload(awcsti)
		}
	}
	// Now check if service exports have changed.
	if !a.checkServiceExportsEqual(old) || signersChanged {
		s.accounts.Range(func(k, v interface{}) bool {
			acc := v.(*Account)
			acc.mu.Lock()
			for _, im := range acc.imports.services {
				if im != nil && im.acc.Name == a.Name {
					// Check for if we are still authorized for an import.
					im.invalid = !a.checkServiceImportAuthorizedNoLock(a, im.to, im.claim)
				}
			}
			acc.mu.Unlock()
			return true
		})
	}

	// Now do limits if they are present.
	a.mu.Lock()
	a.msubs = int32(ac.Limits.Subs)
	a.mpay = int32(ac.Limits.Payload)
	a.mconns = int32(ac.Limits.Conn)
	a.mleafs = int32(ac.Limits.LeafNodeConn)
	a.mu.Unlock()

	clients := gatherClients()
	// Sort if we are over the limit.
	if a.maxTotalConnectionsReached() {
		sort.Slice(clients, func(i, j int) bool {
			return clients[i].start.After(clients[j].start)
		})
	}
	for i, c := range clients {
		if a.mconns != jwt.NoLimit && i >= int(a.mconns) {
			c.maxAccountConnExceeded()
			continue
		}
		c.mu.Lock()
		c.applyAccountLimits()
		c.mu.Unlock()
	}

	// Check if the signing keys changed, might have to evict
	if signersChanged {
		for _, c := range clients {
			c.mu.Lock()
			sk := c.user.SigningKey
			c.mu.Unlock()
			if sk != "" && !a.hasIssuer(sk) {
				c.closeConnection(AuthenticationViolation)
			}
		}
	}
}

// Helper to build an internal account structure from a jwt.AccountClaims.
func (s *Server) buildInternalAccount(ac *jwt.AccountClaims) *Account {
	acc := NewAccount(ac.Subject)
	acc.Issuer = ac.Issuer
	s.updateAccountClaims(acc, ac)
	return acc
}

// Helper to build internal NKeyUser.
func buildInternalNkeyUser(uc *jwt.UserClaims, acc *Account) *NkeyUser {
	nu := &NkeyUser{Nkey: uc.Subject, Account: acc}
	if uc.IssuerAccount != "" {
		nu.SigningKey = uc.Issuer
	}

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
	Fetch(name string) (string, error)
	Store(name, jwt string) error
}

// MemAccResolver is a memory only resolver.
// Mostly for testing.
type MemAccResolver struct {
	sm sync.Map
}

// Fetch will fetch the account jwt claims from the internal sync.Map.
func (m *MemAccResolver) Fetch(name string) (string, error) {
	if j, ok := m.sm.Load(name); ok {
		return j.(string), nil
	}
	return _EMPTY_, ErrMissingAccount
}

// Store will store the account jwt claims in the internal sync.Map.
func (m *MemAccResolver) Store(name, jwt string) error {
	m.sm.Store(name, jwt)
	return nil
}

// URLAccResolver implements an http fetcher.
type URLAccResolver struct {
	url string
	c   *http.Client
}

// NewURLAccResolver returns a new resolver for the given base URL.
func NewURLAccResolver(url string) (*URLAccResolver, error) {
	if !strings.HasSuffix(url, "/") {
		url += "/"
	}
	// Do basic test to see if anyone is home.
	// FIXME(dlc) - Make timeout configurable post MVP.
	ur := &URLAccResolver{
		url: url,
		c:   &http.Client{Timeout: 2 * time.Second},
	}
	if _, err := ur.Fetch(""); err != nil {
		return nil, err
	}
	return ur, nil
}

// Fetch will fetch the account jwt claims from the base url, appending the
// account name onto the end.
func (ur *URLAccResolver) Fetch(name string) (string, error) {
	url := ur.url + name
	resp, err := ur.c.Get(url)
	if err != nil {
		return _EMPTY_, fmt.Errorf("could not fetch <%q>: %v", url, err)
	} else if resp == nil {
		return _EMPTY_, fmt.Errorf("could not fetch <%q>: no response", url)
	} else if resp.StatusCode != http.StatusOK {
		return _EMPTY_, fmt.Errorf("could not fetch <%q>: %v", url, resp.Status)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return _EMPTY_, err
	}
	return string(body), nil
}

// Store is not implemented for URL Resolver.
func (ur *URLAccResolver) Store(name, jwt string) error {
	return fmt.Errorf("Store operation not supported for URL Resolver")
}
