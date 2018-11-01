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
	"sort"
	"strings"
	"sync"
	"time"
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
	mu       sync.RWMutex
	sl       *Sublist
	clients  int
	rm       map[string]*rme
	imports  importMap
	exports  exportMap
	nae      int
	maxnae   int
	maxaettl time.Duration
	pruning  bool
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

// NumClients returns active number of clients for this account.
func (a *Account) NumClients() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.clients
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
// Call in with client but just track total for now.
// Returns previous total.
func (a *Account) addClient(c *client) int {
	a.mu.Lock()
	n := a.clients
	a.clients++
	a.mu.Unlock()
	return n
}

// removeClient keeps our accounting of active clients updated.
func (a *Account) removeClient(c *client) int {
	a.mu.Lock()
	n := a.clients
	a.clients--
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

// AddServiceImport will add a route to an account to send published messages / requests
// to the destination account. From is the local subject to map, To is the
// subject that will appear on the destination account. Destination will need
// to have an import rule to allow access via addService.
func (a *Account) AddServiceImport(destination *Account, from, to string) error {
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

// AddStreamImport will add in the stream import from a specific account.
func (a *Account) AddStreamImport(account *Account, from, prefix string) error {
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
