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
	"math/rand"
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
	Name         string
	Nkey         string
	Issuer       string
	claimJWT     string
	updated      time.Time
	mu           sync.RWMutex
	sqmu         sync.Mutex
	sl           *Sublist
	etmr         *time.Timer
	ctmr         *time.Timer
	strack       map[string]sconns
	nrclients    int32
	sysclients   int32
	nleafs       int32
	nrleafs      int32
	clients      map[*client]*client
	rm           map[string]int32
	lqws         map[string]int32
	usersRevoked map[string]int64
	actsRevoked  map[string]int64
	respMap      map[string][]*serviceRespEntry
	lleafs       []*client
	imports      importMap
	exports      exportMap
	limits
	nae           int32
	pruning       bool
	rmPruning     bool
	expired       bool
	signingKeys   []string
	srv           *Server // server this account is registered with (possibly nil)
	lds           string  // loop detection subject for leaf nodes
	siReply       []byte  // service reply prefix, will form wildcard subscription.
	siReplyClient *client
	prand         *rand.Rand
}

// Account based limits.
type limits struct {
	mpay     int32
	msubs    int32
	mconns   int32
	mleafs   int32
	maxnae   int32
	maxnrm   int32
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
	acc      *Account
	claim    *jwt.Import
	from     string
	to       string
	ts       int64
	rt       ServiceRespType
	latency  *serviceLatency
	m1       *ServiceLatency
	ae       bool
	internal bool
	invalid  bool
	tracking bool
}

// This is used to record when we create a mapping for implicit service
// imports. We use this to clean up entries that are not singletons when
// we detect that interest is no longer present. The key to the map will
// be the actual interest. We record the mapped subject and the serviceImport
type serviceRespEntry struct {
	acc  *Account
	msub string
}

// ServiceRespType represents the types of service request response types.
type ServiceRespType uint8

// Service response types. Defaults to a singleton.
const (
	Singleton ServiceRespType = iota
	Stream
	Chunked
)

// String helper.
func (rt ServiceRespType) String() string {
	switch rt {
	case Singleton:
		return "Singleton"
	case Stream:
		return "Stream"
	case Chunked:
		return "Chunked"
	}
	return "Unknown ServiceResType"
}

// exportAuth holds configured approvals or boolean indicating an
// auth token is required for import.
type exportAuth struct {
	tokenReq bool
	approved map[string]*Account
}

// streamExport
type streamExport struct {
	exportAuth
}

// serviceExport holds additional information for exported services.
type serviceExport struct {
	exportAuth
	respType ServiceRespType
	latency  *serviceLatency
}

// Used to track service latency.
type serviceLatency struct {
	sampling int8
	subject  string
}

// exportMap tracks the exported streams and services.
type exportMap struct {
	streams  map[string]*streamExport
	services map[string]*serviceExport
}

// importMap tracks the imported streams and services.
type importMap struct {
	streams  []*streamImport
	services map[string]*serviceImport // TODO(dlc) sync.Map may be better.
}

// NewAccount creates a new unlimited account with the given name.
func NewAccount(name string) *Account {
	a := &Account{
		Name:   name,
		limits: limits{-1, -1, -1, -1, 0, 0, 0},
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

// Called to track a remote server and connections and leafnodes it
// has for this account.
func (a *Account) updateRemoteServer(m *AccountNumConns) {
	a.mu.Lock()
	if a.strack == nil {
		a.strack = make(map[string]sconns)
	}
	// This does not depend on receiving all updates since each one is idempotent.
	// FIXME(dlc) - We should cleanup when these both go to zero.
	prev := a.strack[m.Server.ID]
	a.strack[m.Server.ID] = sconns{conns: int32(m.Conns), leafs: int32(m.LeafNodes)}
	a.nrclients += int32(m.Conns) - prev.conns
	a.nrleafs += int32(m.LeafNodes) - prev.leafs
	a.mu.Unlock()
}

// Removes tracking for a remote server that has shutdown.
func (a *Account) removeRemoteServer(sid string) {
	a.mu.Lock()
	if a.strack != nil {
		prev := a.strack[sid]
		delete(a.strack, sid)
		a.nrclients -= prev.conns
		a.nrleafs -= prev.leafs
	}
	a.mu.Unlock()
}

// When querying for subject interest this is the number of
// expected responses. We need to actually check that the entry
// has active connections.
func (a *Account) expectedRemoteResponses() (expected int32) {
	a.mu.RLock()
	for _, sc := range a.strack {
		if sc.conns > 0 || sc.leafs > 0 {
			expected++
		}
	}
	a.mu.RUnlock()
	return
}

// Clears eventing and tracking for this account.
func (a *Account) clearEventing() {
	a.mu.Lock()
	a.nrclients = 0
	// Now clear state
	clearTimer(&a.etmr)
	clearTimer(&a.ctmr)
	a.clients = nil
	a.strack = nil
	a.mu.Unlock()
}

// NumConnections returns active number of clients for this account for
// all known servers.
func (a *Account) NumConnections() int {
	a.mu.RLock()
	nc := len(a.clients) + int(a.nrclients)
	a.mu.RUnlock()
	return nc
}

// NumRemoteConnections returns the number of client or leaf connections that
// are not on this server.
func (a *Account) NumRemoteConnections() int {
	a.mu.RLock()
	nc := int(a.nrclients + a.nrleafs)
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

// This is for extended local interest.
// Lock should not be held.
func (a *Account) numLocalAndLeafConnections() int {
	a.mu.RLock()
	nlc := len(a.clients) - int(a.sysclients)
	a.mu.RUnlock()
	return nlc
}

func (a *Account) numLocalLeafNodes() int {
	return int(a.nleafs)
}

// MaxTotalConnectionsReached returns if we have reached our limit for number of connections.
func (a *Account) MaxTotalConnectionsReached() bool {
	var mtc bool
	a.mu.RLock()
	if a.mconns != jwt.NoLimit {
		mtc = len(a.clients)-int(a.sysclients)+int(a.nrclients) >= int(a.mconns)
	}
	a.mu.RUnlock()
	return mtc
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
	if a.siReplyClient != nil {
		return a.siReplyClient
	}
	for _, c = range a.clients {
		break
	}
	return c
}

// AddServiceExport will configure the account with the defined export.
func (a *Account) AddServiceExport(subject string, accounts []*Account) error {
	return a.AddServiceExportWithResponse(subject, Singleton, accounts)
}

// AddServiceExportWithresponse will configure the account with the defined export and response type.
func (a *Account) AddServiceExportWithResponse(subject string, respType ServiceRespType, accounts []*Account) error {
	if a == nil {
		return ErrMissingAccount
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.exports.services == nil {
		a.exports.services = make(map[string]*serviceExport)
	}

	ea := a.exports.services[subject]

	if respType != Singleton {
		if ea == nil {
			ea = &serviceExport{}
		}
		ea.respType = respType
	}

	if accounts != nil {
		if ea == nil {
			ea = &serviceExport{}
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

// TrackServiceExport will enable latency tracking of the named service.
// Results will be published in this account to the given results subject.
func (a *Account) TrackServiceExport(service, results string) error {
	return a.TrackServiceExportWithSampling(service, results, DEFAULT_SERVICE_LATENCY_SAMPLING)
}

// TrackServiceExportWithSampling will enable latency tracking of the named service for the given
// sampling rate (1-100). Results will be published in this account to the given results subject.
func (a *Account) TrackServiceExportWithSampling(service, results string, sampling int) error {
	if a == nil {
		return ErrMissingAccount
	}

	if sampling < 1 || sampling > 100 {
		return ErrBadSampling
	}
	if !IsValidPublishSubject(results) {
		return ErrBadPublishSubject
	}
	// Don't loop back on outselves.
	if a.IsExportService(results) {
		return ErrBadPublishSubject
	}

	if a.srv != nil && !a.srv.EventsEnabled() {
		return ErrNoSysAccount
	}

	a.mu.Lock()
	if a.exports.services == nil {
		a.mu.Unlock()
		return ErrMissingService
	}
	ea, ok := a.exports.services[service]
	if !ok {
		a.mu.Unlock()
		return ErrMissingService
	}
	if ea == nil {
		ea = &serviceExport{}
		a.exports.services[service] = ea
	} else if ea.respType != Singleton {
		a.mu.Unlock()
		return ErrBadServiceType
	}
	ea.latency = &serviceLatency{
		sampling: int8(sampling),
		subject:  results,
	}
	s := a.srv
	a.mu.Unlock()

	if s == nil {
		return nil
	}

	// Now track down the imports and add in latency as needed to enable.
	s.accounts.Range(func(k, v interface{}) bool {
		acc := v.(*Account)
		acc.mu.Lock()
		for _, im := range acc.imports.services {
			if im != nil && im.acc.Name == a.Name && subjectIsSubsetMatch(im.to, service) {
				im.latency = ea.latency
			}
		}
		acc.mu.Unlock()
		return true
	})

	return nil
}

// UnTrackServiceExport will disable latency tracking of the named service.
func (a *Account) UnTrackServiceExport(service string) {
	if a == nil || (a.srv != nil && !a.srv.EventsEnabled()) {
		return
	}

	a.mu.Lock()
	if a == nil || a.exports.services == nil {
		a.mu.Unlock()
		return
	}
	ea, ok := a.exports.services[service]
	if !ok || ea == nil || ea.latency == nil {
		a.mu.Unlock()
		return
	}
	// We have latency here.
	ea.latency = nil
	s := a.srv
	a.mu.Unlock()

	if s == nil {
		return
	}

	// Now track down the imports and clean them up.
	s.accounts.Range(func(k, v interface{}) bool {
		acc := v.(*Account)
		acc.mu.Lock()
		for _, im := range acc.imports.services {
			if im != nil && im.acc.Name == a.Name {
				if subjectIsSubsetMatch(im.to, service) {
					im.latency, im.m1 = nil, nil
				}
			}
		}
		acc.mu.Unlock()
		return true
	})
}

// IsExportService will indicate if this service exists. Will check wildcard scenarios.
func (a *Account) IsExportService(service string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	_, ok := a.exports.services[service]
	if ok {
		return true
	}
	tokens := strings.Split(service, tsep)
	for subj := range a.exports.services {
		if isSubsetMatch(tokens, subj) {
			return true
		}
	}
	return false
}

// IsExportServiceTracking will indicate if given publish subject is an export service with tracking enabled.
func (a *Account) IsExportServiceTracking(service string) bool {
	a.mu.RLock()
	ea, ok := a.exports.services[service]
	if ok && ea == nil {
		a.mu.RUnlock()
		return false
	}
	if ok && ea != nil && ea.latency != nil {
		a.mu.RUnlock()
		return true
	}
	// FIXME(dlc) - Might want to cache this is in the hot path checking for latency tracking.
	tokens := strings.Split(service, tsep)
	for subj, ea := range a.exports.services {
		if isSubsetMatch(tokens, subj) && ea != nil && ea.latency != nil {
			a.mu.RUnlock()
			return true
		}
	}
	a.mu.RUnlock()
	return false
}

// NATSLatency represents the internal NATS latencies, including RTTs to clients.
type NATSLatency struct {
	Requestor time.Duration `json:"req"`
	Responder time.Duration `json:"resp"`
	System    time.Duration `json:"sys"`
}

// TotalTime is a helper function that totals the NATS latencies.
func (nl *NATSLatency) TotalTime() time.Duration {
	return nl.Requestor + nl.Responder + nl.System
}

// ServiceLatency is the JSON message sent out in response to latency tracking for
// exported services.
type ServiceLatency struct {
	AppName        string        `json:"app,omitempty"`
	RequestStart   time.Time     `json:"start"`
	ServiceLatency time.Duration `json:"svc"`
	NATSLatency    NATSLatency   `json:"nats"`
	TotalLatency   time.Duration `json:"total"`
}

// Merge function to merge m1 and m2 (requestor and responder) measurements
// when there are two samples. This happens when the requestor and responder
// are on different servers.
//
// m2 ServiceLatency is correct, so use that.
// m1 TotalLatency is correct, so use that.
// Will use those to back into NATS latency.
func (m1 *ServiceLatency) merge(m2 *ServiceLatency) {
	m1.AppName = m2.AppName
	m1.NATSLatency.System = m1.ServiceLatency - (m2.ServiceLatency + m2.NATSLatency.Responder)
	m1.ServiceLatency = m2.ServiceLatency
	m1.NATSLatency.Responder = m2.NATSLatency.Responder
	sanitizeLatencyMetric(m1)
}

// sanitizeLatencyMetric adjusts latency metric values that could go
// negative in some edge conditions since we estimate client RTT
// for both requestor and responder.
// These numbers are never meant to be negative, it just could be
// how we back into the values based on estimated RTT.
func sanitizeLatencyMetric(sl *ServiceLatency) {
	if sl.ServiceLatency < 0 {
		sl.ServiceLatency = 0
	}
	if sl.NATSLatency.System < 0 {
		sl.NATSLatency.System = 0
	}
}

// Used for transporting remote latency measurements.
type remoteLatency struct {
	Account string         `json:"account"`
	ReqId   string         `json:"req_id"`
	M2      ServiceLatency `json:"m2"`
}

// sendTrackingMessage will send out the appropriate tracking information for the
// service request/response latency. This is called when the requestor's server has
// received the response.
// TODO(dlc) - holding locks for RTTs may be too much long term. Should revisit.
func (a *Account) sendTrackingLatency(si *serviceImport, requestor, responder *client) bool {
	ts := time.Now()
	serviceRTT := time.Duration(ts.UnixNano() - si.ts)

	var reqClientRTT = requestor.getRTTValue()
	var respClientRTT time.Duration
	var appName string

	if responder != nil && responder.kind == CLIENT {
		respClientRTT = responder.getRTTValue()
		appName = responder.GetName()
	}

	// We will estimate time when request left the requestor by time we received
	// and the client RTT for the requestor.
	reqStart := time.Unix(0, si.ts-int64(reqClientRTT))
	sl := &ServiceLatency{
		AppName:        appName,
		RequestStart:   reqStart,
		ServiceLatency: serviceRTT - respClientRTT,
		NATSLatency: NATSLatency{
			Requestor: reqClientRTT,
			Responder: respClientRTT,
			System:    0,
		},
		TotalLatency: reqClientRTT + serviceRTT,
	}
	if respClientRTT > 0 {
		sl.NATSLatency.System = time.Since(ts)
		sl.TotalLatency += sl.NATSLatency.System
	}

	sanitizeLatencyMetric(sl)

	// If we are expecting a remote measurement, store our sl here.
	// We need to account for the race between this and us receiving the
	// remote measurement.
	// FIXME(dlc) - We need to clean these up but this should happen
	// already with the auto-expire logic.
	if responder != nil && responder.kind != CLIENT {
		si.acc.mu.Lock()
		if si.m1 != nil {
			m1, m2 := sl, si.m1
			m1.merge(m2)
			si.acc.mu.Unlock()
			a.srv.sendInternalAccountMsg(a, si.latency.subject, m1)
			return true
		}
		si.m1 = sl
		si.acc.mu.Unlock()
		return false
	} else {
		a.srv.sendInternalAccountMsg(a, si.latency.subject, sl)
	}
	return true
}

// numServiceRoutes returns the number of service routes on this account.
func (a *Account) numServiceRoutes() int {
	a.mu.RLock()
	num := len(a.imports.services)
	a.mu.RUnlock()
	return num
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

	_, err := a.addServiceImport(destination, from, to, imClaim)
	return err
}

// AddServiceImport will add a route to an account to send published messages / requests
// to the destination account. From is the local subject to map, To is the
// subject that will appear on the destination account. Destination will need
// to have an import rule to allow access via addService.
func (a *Account) AddServiceImport(destination *Account, from, to string) error {
	return a.AddServiceImportWithClaim(destination, from, to, nil)
}

// NumServiceImports return number of service imports we have.
func (a *Account) NumServiceImports() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.imports.services)
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

// This tracks responses to service requests mappings. This is used for cleanup.
func (a *Account) addRespMapEntry(acc *Account, reply, from string) {
	a.mu.Lock()
	if a.respMap == nil {
		a.respMap = make(map[string][]*serviceRespEntry)
	}
	sre := &serviceRespEntry{acc, from}
	sra := a.respMap[reply]
	a.respMap[reply] = append(sra, sre)
	if len(a.respMap) > int(a.maxnrm) && !a.rmPruning {
		a.rmPruning = true
		go a.pruneNonAutoExpireResponseMaps()
	}
	a.mu.Unlock()
}

// This checks for any response map entries.
func (a *Account) checkForRespEntry(reply string) {
	a.mu.RLock()
	if len(a.imports.services) == 0 || len(a.respMap) == 0 {
		a.mu.RUnlock()
		return
	}
	sra := a.respMap[reply]
	if sra == nil {
		a.mu.RUnlock()
		return
	}
	// If we are here we have an entry we should check. We will first check
	// if there is any interest for this subject for the entire account. If
	// there is we can not delete any entries yet.
	rr := a.sl.Match(reply)
	a.mu.RUnlock()

	// No interest.
	if len(rr.psubs)+len(rr.qsubs) > 0 {
		return
	}

	// Delete all the entries here.
	a.mu.Lock()
	delete(a.respMap, reply)
	a.mu.Unlock()

	// If we are here we no longer have interest and we have a respMap entry
	// that we should clean up.
	for _, sre := range sra {
		sre.acc.removeServiceImport(sre.msub)
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
	if len(a.imports.services) == 0 {
		a.mu.RUnlock()
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

	a.mu.RUnlock()
	return aesis
}

// MaxResponseMaps return the maximum number of
// non auto-expire response maps we will allow.
func (a *Account) MaxResponseMaps() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return int(a.maxnrm)
}

// SetMaxResponseMaps sets the max outstanding non auto-expire response maps.
func (a *Account) SetMaxResponseMaps(max int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.maxnrm = int32(max)
}

// Add a route to connect from an implicit route created for a response to a request.
// This does no checks and should be only called by the msg processing code. Use
// AddServiceImport from above if responding to user input or config changes, etc.
func (a *Account) addServiceImport(dest *Account, from, to string, claim *jwt.Import) (*serviceImport, error) {
	rt := Singleton
	var lat *serviceLatency

	dest.mu.Lock()
	if ea := dest.getServiceExport(to); ea != nil {
		rt = ea.respType
		lat = ea.latency
	}
	dest.mu.Unlock()

	a.mu.Lock()
	if a.imports.services == nil {
		a.imports.services = make(map[string]*serviceImport)
	} else if dup := a.imports.services[from]; dup != nil {
		a.mu.Unlock()
		return nil, fmt.Errorf("duplicate service import subject %q, previously used in import for account %q, subject %q",
			from, dup.acc.Name, dup.to)
	}
	si := &serviceImport{dest, claim, from, to, 0, rt, lat, nil, false, false, false, false}
	a.imports.services[from] = si
	a.mu.Unlock()

	return si, nil
}

// Helper to detrmine when to sample.
func shouldSample(l *serviceLatency) bool {
	if l == nil || l.sampling <= 0 {
		return false
	}
	if l.sampling >= 100 {
		return true
	}
	return rand.Int31n(100) <= int32(l.sampling)
}

// Used to mimic client like replies.
const (
	replyPrefix    = "_R_."
	trackSuffix    = ".T"
	replyPrefixLen = len(replyPrefix)
	baseServerLen  = 10
	replyLen       = 6
	minReplyLen    = 15
	digits         = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	base           = 62
)

// Will create a wildcard subscription to handle interest graph propagation for all
// service replies.
// Lock should not be held.
func (a *Account) createRespWildcard() []byte {
	a.mu.Lock()
	if a.prand == nil {
		a.prand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	var b = [baseServerLen]byte{'_', 'R', '_', '.'}
	rn := a.prand.Int63()
	for i, l := replyPrefixLen, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	a.siReply = append(b[:], '.')
	s := a.srv
	aName := a.Name
	pre := a.siReply
	wcsub := append(a.siReply, '>')
	a.mu.Unlock()

	// Check to see if we need to propagate interest.
	if s != nil {
		now := time.Now()
		c := &client{srv: a.srv, acc: a, kind: SYSTEM, opts: internalOpts, msubs: -1, mpay: -1, start: now, last: now}
		sub := &subscription{client: c, subject: wcsub}
		s.updateRouteSubscriptionMap(a, sub, 1)
		if s.gateway.enabled {
			s.gatewayUpdateSubInterest(aName, sub, 1)
			a.mu.Lock()
			a.siReplyClient = c
			a.mu.Unlock()
		}
	}

	return pre
}

func (a *Account) replyClient() *client {
	a.mu.RLock()
	c := a.siReplyClient
	a.mu.RUnlock()
	return c
}

// Test whether this is a tracked reply.
func isTrackedReply(reply []byte) bool {
	lreply := len(reply) - 1
	return lreply > 3 && reply[lreply-1] == '.' && reply[lreply] == 'T'
}

// Generate a new service reply from the wildcard prefix.
// FIXME(dlc) - probably do not have to use rand here. about 25ns per.
func (a *Account) newServiceReply(tracking bool) []byte {
	a.mu.RLock()
	replyPre := a.siReply
	s := a.srv
	a.mu.RUnlock()

	if replyPre == nil {
		replyPre = a.createRespWildcard()
	}

	var b [replyLen]byte
	rn := a.prand.Int63()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	// Make sure to copy.
	reply := make([]byte, 0, len(replyPre)+len(b))
	reply = append(reply, replyPre...)
	reply = append(reply, b[:]...)

	if tracking && s.sys != nil {
		// Add in our tracking identifier. This allows the metrics to get back to only
		// this server without needless SUBS/UNSUBS.
		reply = append(reply, '.')
		reply = append(reply, s.sys.shash...)
		reply = append(reply, '.', 'T')
	}
	return reply
}

// This is for internal responses.
func (a *Account) addRespServiceImport(dest *Account, from, to string, rt ServiceRespType, lat *serviceLatency) *serviceImport {
	a.mu.Lock()
	if a.imports.services == nil {
		a.imports.services = make(map[string]*serviceImport)
	}
	// dest is the requestor's account. a is the service responder with the export.
	ae := rt == Singleton
	si := &serviceImport{dest, nil, from, to, 0, rt, nil, nil, ae, true, false, false}
	a.imports.services[from] = si
	if ae {
		a.nae++
		si.ts = time.Now().UnixNano()
		if lat != nil {
			si.latency = lat
			si.tracking = true
		}
		if a.nae > a.maxnae && !a.pruning {
			a.pruning = true
			go a.pruneAutoExpireResponseMaps()
		}
	}
	a.mu.Unlock()
	return si
}

// This will prune off the non auto-expire (non singleton) response maps.
func (a *Account) pruneNonAutoExpireResponseMaps() {
	var sres []*serviceRespEntry
	a.mu.Lock()
	for subj, sra := range a.respMap {
		rr := a.sl.Match(subj)
		if len(rr.psubs)+len(rr.qsubs) == 0 {
			delete(a.respMap, subj)
			sres = append(sres, sra...)
		}
	}
	a.rmPruning = false
	a.mu.Unlock()

	for _, sre := range sres {
		sre.acc.removeServiceImport(sre.msub)
	}
}

// This will prune the list to below the threshold and remove all ttl'd maps.
func (a *Account) pruneAutoExpireResponseMaps() {
	defer func() {
		a.mu.Lock()
		a.pruning = false
		a.mu.Unlock()
	}()

	a.mu.RLock()
	ttl := int64(a.maxaettl)
	a.mu.RUnlock()

	for {
		sis := a.autoExpireResponseMaps()

		// Check ttl items.
		now := time.Now().UnixNano()
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

	// Check prefix if it exists and make sure its a literal.
	// Append token separator if not already present.
	if prefix != "" {
		// Make sure there are no wildcards here, this prefix needs to be a literal
		// since it will be prepended to a publish subject.
		if !subjectIsLiteral(prefix) {
			return ErrStreamImportBadPrefix
		}
		if prefix[len(prefix)-1] != btsep {
			prefix = prefix + string(btsep)
		}
	}
	a.mu.Lock()
	if a.isStreamImportDuplicate(account, from) {
		a.mu.Unlock()
		return ErrStreamImportDuplicate
	}
	a.imports.streams = append(a.imports.streams, &streamImport{account, from, prefix, imClaim, false})
	a.mu.Unlock()
	return nil
}

// isStreamImportDuplicate checks for duplicate.
// Lock should be held.
func (a *Account) isStreamImportDuplicate(acc *Account, from string) bool {
	for _, si := range a.imports.streams {
		if si.acc == acc && si.from == from {
			return true
		}
	}
	return false
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
	if a == nil {
		return ErrMissingAccount
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.exports.streams == nil {
		a.exports.streams = make(map[string]*streamExport)
	}
	ea := a.exports.streams[subject]
	if accounts != nil {
		if ea == nil {
			ea = &streamExport{}
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
	auth := a.checkStreamImportAuthorizedNoLock(account, subject, imClaim)
	a.mu.RUnlock()
	return auth
}

func (a *Account) checkStreamImportAuthorizedNoLock(account *Account, subject string, imClaim *jwt.Import) bool {
	if a.exports.streams == nil || !IsValidSubject(subject) {
		return false
	}
	return a.checkStreamExportApproved(account, subject, imClaim)
}

func (a *Account) checkAuth(ea *exportAuth, account *Account, imClaim *jwt.Import) bool {
	// if ea is nil or ea.approved is nil, that denotes a public export
	if ea == nil || (ea.approved == nil && !ea.tokenReq) {
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

func (a *Account) checkStreamExportApproved(account *Account, subject string, imClaim *jwt.Import) bool {
	// Check direct match of subject first
	ea, ok := a.exports.streams[subject]
	if ok {
		if ea == nil {
			return true
		}
		return a.checkAuth(&ea.exportAuth, account, imClaim)
	}
	// ok if we are here we did not match directly so we need to test each one.
	// The import subject arg has to take precedence, meaning the export
	// has to be a true subset of the import claim. We already checked for
	// exact matches above.
	tokens := strings.Split(subject, tsep)
	for subj, ea := range a.exports.streams {
		if isSubsetMatch(tokens, subj) {
			if ea == nil {
				return true
			}
			return a.checkAuth(&ea.exportAuth, account, imClaim)
		}
	}
	return false
}

func (a *Account) checkServiceExportApproved(account *Account, subject string, imClaim *jwt.Import) bool {
	// Check direct match of subject first
	ea, ok := a.exports.services[subject]
	if ok {
		// if ea is nil or eq.approved is nil, that denotes a public export
		if ea == nil || (ea.approved == nil && !ea.tokenReq) {
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
	for subj, ea := range a.exports.services {
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

// Helper function to get a serviceExport.
// Lock should be held on entry.
func (a *Account) getServiceExport(subj string) *serviceExport {
	ea, ok := a.exports.services[subj]
	// The export probably has a wildcard, so lookup that up.
	if !ok {
		ea = a.getWildcardServiceExport(subj)
	}
	return ea
}

// This helper is used when trying to match a serviceExport record that is
// represented by a wildcard.
// Lock should be held on entry.
func (a *Account) getWildcardServiceExport(to string) *serviceExport {
	tokens := strings.Split(to, tsep)
	for subj, ea := range a.exports.services {
		if isSubsetMatch(tokens, subj) {
			return ea
		}
	}
	return nil
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
func (a *Account) streamActivationExpired(exportAcc *Account, subject string) {
	a.mu.RLock()
	if a.expired || a.imports.streams == nil {
		a.mu.RUnlock()
		return
	}
	var si *streamImport
	for _, si = range a.imports.streams {
		if si.acc == exportAcc && si.from == subject {
			break
		}
	}

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
func (a *Account) activationExpired(exportAcc *Account, subject string, kind jwt.ExportType) {
	switch kind {
	case jwt.Stream:
		a.streamActivationExpired(exportAcc, subject)
	case jwt.Service:
		a.serviceActivationExpired(subject)
	}
}

// checkActivation will check the activation token for validity.
func (a *Account) checkActivation(importAcc *Account, claim *jwt.Import, expTimer bool) bool {
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
				importAcc.activationExpired(a, string(act.ImportSubject), claim.Type)
			})
		}
	}
	// Check for token revocation..
	if a.actsRevoked != nil {
		if t, ok := a.actsRevoked[act.Subject]; ok && t <= time.Now().Unix() {
			return false
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
	// If the IssuerAccount is not us, then this is considered an error.
	if a.Name != claims.IssuerAccount {
		if a.srv != nil {
			a.srv.Errorf("Invalid issuer account %q in activation claim (subject: %q - type: %q) for account %q",
				claims.IssuerAccount, claims.Activation.ImportSubject, claims.Activation.ImportType, a.Name)
		}
		return false
	}
	return a.hasIssuerNoLock(claims.Issuer)
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
	// Load the b imports into a map index by what we are looking for.
	bm := make(map[string]*streamImport, len(b.imports.streams))
	for _, bim := range b.imports.streams {
		bm[bim.acc.Name+bim.from+bim.prefix] = bim
	}
	for _, aim := range a.imports.streams {
		if _, ok := bm[aim.acc.Name+aim.from+aim.prefix]; !ok {
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

// Check if another account is authorized to route requests to this service.
func (a *Account) checkServiceImportAuthorized(account *Account, subject string, imClaim *jwt.Import) bool {
	a.mu.RLock()
	authorized := a.checkServiceImportAuthorizedNoLock(account, subject, imClaim)
	a.mu.RUnlock()
	return authorized
}

// Check if another account is authorized to route requests to this service.
func (a *Account) checkServiceImportAuthorizedNoLock(account *Account, subject string, imClaim *jwt.Import) bool {
	// Find the subject in the services list.
	if a.exports.services == nil || !IsValidLiteralSubject(subject) {
		return false
	}
	return a.checkServiceExportApproved(account, subject, imClaim)
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

// checkUserRevoked will check if a user has been revoked.
func (a *Account) checkUserRevoked(nkey string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.usersRevoked == nil {
		return false
	}
	if t, ok := a.usersRevoked[nkey]; !ok || t > time.Now().Unix() {
		return false
	}
	return true
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
	hi := a.hasIssuerNoLock(issuer)
	a.mu.RUnlock()
	return hi
}

// hasIssuerNoLock is the unlocked version of hasIssuer
func (a *Account) hasIssuerNoLock(issuer string) bool {
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

// Returns the loop detection subject used for leafnodes
func (a *Account) getLds() string {
	a.mu.RLock()
	lds := a.lds
	a.mu.RUnlock()
	return lds
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
	ar := s.accResolver
	s.mu.Unlock()
	return ar
}

// UpdateAccountClaims will call updateAccountClaims.
func (s *Server) UpdateAccountClaims(a *Account, ac *jwt.AccountClaims) {
	s.updateAccountClaims(a, ac)
}

// updateAccountClaims will update an existing account with new claims.
// This will replace any exports or imports previously defined.
// Lock MUST NOT be held upon entry.
func (s *Server) updateAccountClaims(a *Account, ac *jwt.AccountClaims) {
	if a == nil {
		return
	}
	s.Debugf("Updating account claims: %s", a.Name)
	a.checkExpiration(ac.Claims())

	a.mu.Lock()
	// Clone to update, only select certain fields.
	old := &Account{Name: a.Name, exports: a.exports, limits: a.limits, signingKeys: a.signingKeys}

	// Reset exports and imports here.
	a.exports = exportMap{}

	// Imports are checked unlocked in processInbound, so we can't change out the struct here. Need to process inline.
	if a.imports.streams != nil {
		old.imports.streams = a.imports.streams
		a.imports.streams = nil
	}
	if a.imports.services != nil {
		old.imports.services = make(map[string]*serviceImport, len(a.imports.services))
	}
	for k, v := range a.imports.services {
		old.imports.services[k] = v
		delete(a.imports.services, k)
	}
	// Reset any notion of export revocations.
	a.actsRevoked = nil

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
			rt := Singleton
			switch e.ResponseType {
			case jwt.ResponseTypeStream:
				rt = Stream
			case jwt.ResponseTypeChunked:
				rt = Chunked
			}
			if err := a.AddServiceExportWithResponse(string(e.Subject), rt, authAccounts(e.TokenReq)); err != nil {
				s.Debugf("Error adding service export to account [%s]: %v", a.Name, err)
			}
			if e.Latency != nil {
				if err := a.TrackServiceExportWithSampling(string(e.Subject), string(e.Latency.Results), e.Latency.Sampling); err != nil {
					s.Debugf("Error adding latency tracking for service export to account [%s]: %v", a.Name, err)
				}
			}
		}
		// We will track these at the account level. Should not have any collisions.
		if e.Revocations != nil {
			a.mu.Lock()
			if a.actsRevoked == nil {
				a.actsRevoked = make(map[string]int64)
			}
			for k, t := range e.Revocations {
				a.actsRevoked[k] = t
			}
			a.mu.Unlock()
		}
	}
	for _, i := range ac.Imports {
		acc, err := s.lookupAccount(i.Account)
		if acc == nil || err != nil {
			s.Errorf("Can't locate account [%s] for import of [%v] %s (err=%v)", i.Account, i.Subject, i.Type, err)
			continue
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
		clients := map[*client]struct{}{}
		// We need to check all accounts that have an import claim from this account.
		awcsti := map[string]struct{}{}
		s.accounts.Range(func(k, v interface{}) bool {
			acc := v.(*Account)
			// Move to the next if this account is actually account "a".
			if acc.Name == a.Name {
				return true
			}
			// TODO: checkStreamImportAuthorized() stack should not be trying
			// to lock "acc". If we find that to be needed, we will need to
			// rework this to ensure we don't lock acc.
			acc.mu.Lock()
			for _, im := range acc.imports.streams {
				if im != nil && im.acc.Name == a.Name {
					// Check for if we are still authorized for an import.
					im.invalid = !a.checkStreamImportAuthorized(acc, im.from, im.claim)
					awcsti[acc.Name] = struct{}{}
					for _, c := range acc.clients {
						clients[c] = struct{}{}
					}
				}
			}
			acc.mu.Unlock()
			return true
		})
		// Now walk clients.
		for c := range clients {
			c.processSubsOnConfigReload(awcsti)
		}
	}
	// Now check if service exports have changed.
	if !a.checkServiceExportsEqual(old) || signersChanged {
		s.accounts.Range(func(k, v interface{}) bool {
			acc := v.(*Account)
			// Move to the next if this account is actually account "a".
			if acc.Name == a.Name {
				return true
			}
			// TODO: checkServiceImportAuthorized() stack should not be trying
			// to lock "acc". If we find that to be needed, we will need to
			// rework this to ensure we don't lock acc.
			acc.mu.Lock()
			for _, im := range acc.imports.services {
				if im != nil && im.acc.Name == a.Name {
					// Check for if we are still authorized for an import.
					im.invalid = !a.checkServiceImportAuthorized(acc, im.to, im.claim)
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
	// Check for any revocations
	if len(ac.Revocations) > 0 {
		// We will always replace whatever we had with most current, so no
		// need to look at what we have.
		a.usersRevoked = make(map[string]int64, len(ac.Revocations))
		for pk, t := range ac.Revocations {
			a.usersRevoked[pk] = t
		}
	}
	a.mu.Unlock()

	clients := gatherClients()
	// Sort if we are over the limit.
	if a.MaxTotalConnectionsReached() {
		sort.Slice(clients, func(i, j int) bool {
			return clients[i].start.After(clients[j].start)
		})
	}
	now := time.Now().Unix()
	for i, c := range clients {
		a.mu.RLock()
		exceeded := a.mconns != jwt.NoLimit && i >= int(a.mconns)
		a.mu.RUnlock()
		if exceeded {
			c.maxAccountConnExceeded()
			continue
		}
		c.mu.Lock()
		c.applyAccountLimits()
		// Check for being revoked here. We use ac one to avoid
		// the account lock.
		var nkey string
		if c.user != nil {
			nkey = c.user.Nkey
		}
		c.mu.Unlock()

		// Check if we have been revoked.
		if ac.Revocations != nil {
			if t, ok := ac.Revocations[nkey]; ok && now >= t {
				c.sendErrAndDebug("User Authentication Revoked")
				c.closeConnection(Revocation)
				continue
			}
		}
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
// Lock MUST NOT be held upon entry.
func (s *Server) buildInternalAccount(ac *jwt.AccountClaims) *Account {
	acc := NewAccount(ac.Subject)
	acc.Issuer = ac.Issuer
	// Set this here since we are placing in s.tmpAccounts below and may be
	// referenced by an route RS+, etc.
	s.setAccountSublist(acc)

	// We don't want to register an account that is in the process of
	// being built, however, to solve circular import dependencies, we
	// need to store it here.
	s.tmpAccounts.Store(ac.Subject, acc)
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
	if uc.Resp != nil {
		if p == nil {
			p = &Permissions{}
		}
		p.Response = &ResponsePermission{
			MaxMsgs: uc.Resp.MaxMsgs,
			Expires: uc.Resp.Expires,
		}
		validateResponsePermissions(p)
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

	// FIXME(dlc) - Make timeout and others configurable.
	// We create our own transport to amortize TLS.
	tr := &http.Transport{
		MaxIdleConns:    10,
		IdleConnTimeout: 30 * time.Second,
	}
	ur := &URLAccResolver{
		url: url,
		c:   &http.Client{Timeout: 2 * time.Second, Transport: tr},
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
