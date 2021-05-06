// Copyright 2019-2020 The NATS Authors
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
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

// Warning when user configures leafnode TLS insecure
const leafnodeTLSInsecureWarning = "TLS certificate chain and hostname of solicited leafnodes will not be verified. DO NOT USE IN PRODUCTION!"

// When a loop is detected, delay the reconnect of solicited connection.
const leafNodeReconnectDelayAfterLoopDetected = 30 * time.Second

// When a server receives a message causing a permission violation, the
// connection is closed and it won't attempt to reconnect for that long.
const leafNodeReconnectAfterPermViolation = 30 * time.Second

// Prefix for loop detection subject
const leafNodeLoopDetectionSubjectPrefix = "$LDS."

// Path added to URL to indicate to WS server that the connection is a
// LEAF connection as opposed to a CLIENT.
const leafNodeWSPath = "/leafnode"

type leaf struct {
	// We have any auth stuff here for solicited connections.
	remote *leafNodeCfg
	// isSpoke tells us what role we are playing.
	// Used when we receive a connection but otherside tells us they are a hub.
	isSpoke bool
	// remoteCluster is when we are a hub but the spoke leafnode is part of a cluster.
	remoteCluster string
	// remoteServer holds onto the remove server's name or ID.
	remoteServer string
	// Used to suppress sub and unsub interest. Same as routes but our audience
	// here is tied to this leaf node. This will hold all subscriptions except this
	// leaf nodes. This represents all the interest we want to send to the other side.
	smap map[string]int32
	// This map will contain all the subscriptions that have been added to the smap
	// during initLeafNodeSmapAndSendSubs. It is short lived and is there to avoid
	// race between processing of a sub where sub is added to account sublist but
	// updateSmap has not be called on that "thread", while in the LN readloop,
	// when processing CONNECT, initLeafNodeSmapAndSendSubs is invoked and add
	// this subscription to smap. When processing of the sub then calls updateSmap,
	// we would add it a second time in the smap causing later unsub to suppress the LS-.
	tsub  map[*subscription]struct{}
	tsubt *time.Timer
}

// Used for remote (solicited) leafnodes.
type leafNodeCfg struct {
	sync.RWMutex
	*RemoteLeafOpts
	urls      []*url.URL
	curURL    *url.URL
	tlsName   string
	username  string
	password  string
	perms     *Permissions
	connDelay time.Duration // Delay before a connect, could be used while detecting loop condition, etc..
}

// Check to see if this is a solicited leafnode. We do special processing for solicited.
func (c *client) isSolicitedLeafNode() bool {
	return c.kind == LEAF && c.leaf.remote != nil
}

// Returns true if this is a solicited leafnode and is not configured to be treated as a hub or a receiving
// connection leafnode where the otherside has declared itself to be the hub.
func (c *client) isSpokeLeafNode() bool {
	return c.kind == LEAF && c.leaf.isSpoke
}

func (c *client) isHubLeafNode() bool {
	return c.kind == LEAF && !c.leaf.isSpoke
}

// Will add in the deny exports and imports for JetStream on solicited connections if we
// are sharing the system account and wanting to extend the JS domain.
// r lock should be held.
func (s *Server) addInJSDeny(r *leafNodeCfg) {
	var hasDE, hasDI bool
	for _, dsubj := range r.DenyExports {
		if dsubj == jsAllAPI {
			hasDE = true
			break
		}
	}
	for _, dsubj := range r.DenyImports {
		if dsubj == jsAllAPI {
			hasDI = true
			break
		}
	}

	var addedDeny bool
	if !hasDE {
		s.Noticef("Adding deny export of %q for leafnode configuration on %q that bridges system account", jsAllAPI, r.LocalAccount)
		r.DenyExports = append(r.DenyExports, jsAllAPI)
		addedDeny = true
	}
	if !hasDI {
		s.Noticef("Adding deny import of %q for leafnode configuration on %q that bridges system account", jsAllAPI, r.LocalAccount)
		r.DenyImports = append(r.DenyImports, jsAllAPI)
		addedDeny = true
	}

	// We added in some deny clauses here so need to regenerate the permissions etc.
	if addedDeny {
		perms := &Permissions{}
		if len(r.DenyExports) > 0 {
			perms.Publish = &SubjectPermission{Deny: r.DenyExports}
		}
		if len(r.DenyImports) > 0 {
			perms.Subscribe = &SubjectPermission{Deny: r.DenyImports}
		}
		r.perms = perms
	}
}

// Used for $SYS accounts when sharing but using separate JS domains.
// r lock should be held.
func (s *Server) addInJSDenyAll(r *leafNodeCfg) {
	denyAll := []string{jscAllSubj, raftAllSubj, jsAllAPI}

	s.Noticef("Sharing system account but utilizing separate JetStream Domains")
	s.Noticef("Adding deny of %+v for leafnode configuration that bridges system account", denyAll)

	r.DenyExports = append(r.DenyExports, denyAll...)
	r.DenyImports = append(r.DenyImports, denyAll...)

	perms := &Permissions{}
	if len(r.DenyExports) > 0 {
		perms.Publish = &SubjectPermission{Deny: r.DenyExports}
	}
	if len(r.DenyImports) > 0 {
		perms.Subscribe = &SubjectPermission{Deny: r.DenyImports}
	}
	r.perms = perms
}

// Determine if we are sharing our local system account with the remote.
func (s *Server) hasSystemRemoteLeaf() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	var sacc string
	if s.sys != nil {
		sacc = s.sys.account.Name
	}

	for _, r := range s.leafRemoteCfgs {
		r.RLock()
		lacc := r.LocalAccount
		r.RUnlock()
		if lacc == sacc {
			return true
		}
	}
	return false
}

// This will spin up go routines to solicit the remote leaf node connections.
func (s *Server) solicitLeafNodeRemotes(remotes []*RemoteLeafOpts) {
	for _, r := range remotes {
		s.mu.Lock()
		remote := newLeafNodeCfg(r)
		s.leafRemoteCfgs = append(s.leafRemoteCfgs, remote)
		s.mu.Unlock()
		s.startGoRoutine(func() { s.connectToRemoteLeafNode(remote, true) })
	}
}

func (s *Server) remoteLeafNodeStillValid(remote *leafNodeCfg) bool {
	for _, ri := range s.getOpts().LeafNode.Remotes {
		// FIXME(dlc) - What about auth changes?
		if reflect.DeepEqual(ri.URLs, remote.URLs) {
			return true
		}
	}
	return false
}

// Ensure that leafnode is properly configured.
func validateLeafNode(o *Options) error {
	if err := validateLeafNodeAuthOptions(o); err != nil {
		return err
	}

	for _, rem := range o.LeafNode.Remotes {
		if rem.NoRandomize {
			continue
		}

		rand.Shuffle(len(rem.URLs), func(i, j int) {
			rem.URLs[i], rem.URLs[j] = rem.URLs[j], rem.URLs[i]
		})
	}

	// In local config mode, check that leafnode configuration refers to accounts that exist.
	if len(o.TrustedOperators) == 0 {
		accNames := map[string]struct{}{}
		for _, a := range o.Accounts {
			accNames[a.Name] = struct{}{}
		}
		// global account is always created
		accNames[DEFAULT_GLOBAL_ACCOUNT] = struct{}{}
		// in the context of leaf nodes, empty account means global account
		accNames[_EMPTY_] = struct{}{}
		// system account either exists or, if not disabled, will be created
		if o.SystemAccount == _EMPTY_ && !o.NoSystemAccount {
			accNames[DEFAULT_SYSTEM_ACCOUNT] = struct{}{}
		}
		checkAccountExists := func(accName string, cfgType string) error {
			if _, ok := accNames[accName]; !ok {
				return fmt.Errorf("cannot find local account %q specified in leafnode %s", accName, cfgType)
			}
			return nil
		}
		if err := checkAccountExists(o.LeafNode.Account, "authorization"); err != nil {
			return err
		}
		for _, lu := range o.LeafNode.Users {
			if lu.Account == nil { // means global account
				continue
			}
			if err := checkAccountExists(lu.Account.Name, "authorization"); err != nil {
				return err
			}
		}
		for _, r := range o.LeafNode.Remotes {
			if err := checkAccountExists(r.LocalAccount, "remote"); err != nil {
				return err
			}
		}
	} else {
		if len(o.LeafNode.Users) != 0 {
			return fmt.Errorf("operator mode does not allow specifying user in leafnode config")
		}
		for _, r := range o.LeafNode.Remotes {
			if !nkeys.IsValidPublicAccountKey(r.LocalAccount) {
				return fmt.Errorf("operator mode requires account nkeys in remotes")
			}
		}
		if o.LeafNode.Port != 0 && o.LeafNode.Account != "" && !nkeys.IsValidPublicAccountKey(o.LeafNode.Account) {
			return fmt.Errorf("operator mode and non account nkeys are incompatible")
		}
	}

	// If a remote has a websocket scheme, all need to have it.
	for _, rcfg := range o.LeafNode.Remotes {
		if len(rcfg.URLs) >= 2 {
			firstIsWS, ok := isWSURL(rcfg.URLs[0]), true
			for i := 1; i < len(rcfg.URLs); i++ {
				u := rcfg.URLs[i]
				if isWS := isWSURL(u); isWS && !firstIsWS || !isWS && firstIsWS {
					ok = false
					break
				}
			}
			if !ok {
				return fmt.Errorf("remote leaf node configuration cannot have a mix of websocket and non-websocket urls: %q", rcfg.URLs)
			}
		}
	}

	if o.LeafNode.Port == 0 {
		return nil
	}
	if o.Gateway.Name == "" && o.Gateway.Port == 0 {
		return nil
	}
	// If we are here we have both leaf nodes and gateways defined, make sure there
	// is a system account defined.
	if o.SystemAccount == "" {
		return fmt.Errorf("leaf nodes and gateways (both being defined) require a system account to also be configured")
	}
	return nil
}

// Used to validate user names in LeafNode configuration.
// - rejects mix of single and multiple users.
// - rejects duplicate user names.
func validateLeafNodeAuthOptions(o *Options) error {
	if len(o.LeafNode.Users) == 0 {
		return nil
	}
	if o.LeafNode.Username != _EMPTY_ {
		return fmt.Errorf("can not have a single user/pass and a users array")
	}
	users := map[string]struct{}{}
	for _, u := range o.LeafNode.Users {
		if _, exists := users[u.Username]; exists {
			return fmt.Errorf("duplicate user %q detected in leafnode authorization", u.Username)
		}
		users[u.Username] = struct{}{}
	}
	return nil
}

// Update remote LeafNode TLS configurations after a config reload.
func (s *Server) updateRemoteLeafNodesTLSConfig(opts *Options) {
	max := len(opts.LeafNode.Remotes)
	if max == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Changes in the list of remote leaf nodes is not supported.
	// However, make sure that we don't go over the arrays.
	if len(s.leafRemoteCfgs) < max {
		max = len(s.leafRemoteCfgs)
	}
	for i := 0; i < max; i++ {
		ro := opts.LeafNode.Remotes[i]
		cfg := s.leafRemoteCfgs[i]
		if ro.TLSConfig != nil {
			cfg.Lock()
			cfg.TLSConfig = ro.TLSConfig.Clone()
			cfg.Unlock()
		}
	}
}

func (s *Server) reConnectToRemoteLeafNode(remote *leafNodeCfg) {
	delay := s.getOpts().LeafNode.ReconnectInterval
	select {
	case <-time.After(delay):
	case <-s.quitCh:
		s.grWG.Done()
		return
	}
	s.connectToRemoteLeafNode(remote, false)
}

// Creates a leafNodeCfg object that wraps the RemoteLeafOpts.
func newLeafNodeCfg(remote *RemoteLeafOpts) *leafNodeCfg {
	cfg := &leafNodeCfg{
		RemoteLeafOpts: remote,
		urls:           make([]*url.URL, 0, len(remote.URLs)),
	}
	if len(remote.DenyExports) > 0 || len(remote.DenyImports) > 0 {
		perms := &Permissions{}
		if len(remote.DenyExports) > 0 {
			perms.Publish = &SubjectPermission{Deny: remote.DenyExports}
		}
		if len(remote.DenyImports) > 0 {
			perms.Subscribe = &SubjectPermission{Deny: remote.DenyImports}
		}
		cfg.perms = perms
	}
	// Start with the one that is configured. We will add to this
	// array when receiving async leafnode INFOs.
	cfg.urls = append(cfg.urls, cfg.URLs...)
	// If we are TLS make sure we save off a proper servername if possible.
	// Do same for user/password since we may need them to connect to
	// a bare URL that we get from INFO protocol.
	for _, u := range cfg.urls {
		cfg.saveTLSHostname(u)
		cfg.saveUserPassword(u)
	}
	return cfg
}

// Will pick an URL from the list of available URLs.
func (cfg *leafNodeCfg) pickNextURL() *url.URL {
	cfg.Lock()
	defer cfg.Unlock()
	// If the current URL is the first in the list and we have more than
	// one URL, then move that one to end of the list.
	if cfg.curURL != nil && len(cfg.urls) > 1 && urlsAreEqual(cfg.curURL, cfg.urls[0]) {
		first := cfg.urls[0]
		copy(cfg.urls, cfg.urls[1:])
		cfg.urls[len(cfg.urls)-1] = first
	}
	cfg.curURL = cfg.urls[0]
	return cfg.curURL
}

// Returns the current URL
func (cfg *leafNodeCfg) getCurrentURL() *url.URL {
	cfg.RLock()
	defer cfg.RUnlock()
	return cfg.curURL
}

// Returns how long the server should wait before attempting
// to solicit a remote leafnode connection.
func (cfg *leafNodeCfg) getConnectDelay() time.Duration {
	cfg.RLock()
	delay := cfg.connDelay
	cfg.RUnlock()
	return delay
}

// Sets the connect delay.
func (cfg *leafNodeCfg) setConnectDelay(delay time.Duration) {
	cfg.Lock()
	cfg.connDelay = delay
	cfg.Unlock()
}

// Ensure that non-exported options (used in tests) have
// been properly set.
func (s *Server) setLeafNodeNonExportedOptions() {
	opts := s.getOpts()
	s.leafNodeOpts.dialTimeout = opts.LeafNode.dialTimeout
	if s.leafNodeOpts.dialTimeout == 0 {
		// Use same timeouts as routes for now.
		s.leafNodeOpts.dialTimeout = DEFAULT_ROUTE_DIAL
	}
	s.leafNodeOpts.resolver = opts.LeafNode.resolver
	if s.leafNodeOpts.resolver == nil {
		s.leafNodeOpts.resolver = net.DefaultResolver
	}
}

func (s *Server) connectToRemoteLeafNode(remote *leafNodeCfg, firstConnect bool) {
	defer s.grWG.Done()

	if remote == nil || len(remote.URLs) == 0 {
		s.Debugf("Empty remote leafnode definition, nothing to connect")
		return
	}

	opts := s.getOpts()
	reconnectDelay := opts.LeafNode.ReconnectInterval
	s.mu.Lock()
	dialTimeout := s.leafNodeOpts.dialTimeout
	resolver := s.leafNodeOpts.resolver
	s.mu.Unlock()

	if connDelay := remote.getConnectDelay(); connDelay > 0 {
		select {
		case <-time.After(connDelay):
		case <-s.quitCh:
			return
		}
		remote.setConnectDelay(0)
	}

	var conn net.Conn

	const connErrFmt = "Error trying to connect as leafnode to remote server %q (attempt %v): %v"

	attempts := 0
	for s.isRunning() && s.remoteLeafNodeStillValid(remote) {
		rURL := remote.pickNextURL()
		url, err := s.getRandomIP(resolver, rURL.Host, nil)
		if err == nil {
			var ipStr string
			if url != rURL.Host {
				ipStr = fmt.Sprintf(" (%s)", url)
			}
			s.Debugf("Trying to connect as leafnode to remote server on %q%s", rURL.Host, ipStr)
			conn, err = natsDialTimeout("tcp", url, dialTimeout)
		}
		if err != nil {
			attempts++
			if s.shouldReportConnectErr(firstConnect, attempts) {
				s.Errorf(connErrFmt, rURL.Host, attempts, err)
			} else {
				s.Debugf(connErrFmt, rURL.Host, attempts, err)
			}
			select {
			case <-s.quitCh:
				return
			case <-time.After(reconnectDelay):
				continue
			}
		}
		if !s.remoteLeafNodeStillValid(remote) {
			conn.Close()
			return
		}

		// We have a connection here to a remote server.
		// Go ahead and create our leaf node and return.
		s.createLeafNode(conn, rURL, remote, nil)
		return
	}
}

// Save off the tlsName for when we use TLS and mix hostnames and IPs. IPs usually
// come from the server we connect to.
//
// We used to save the name only if there was a TLSConfig or scheme equal to "tls".
// However, this was causing failures for users that did not set the scheme (and
// their remote connections did not have a tls{} block).
// We now save the host name regardless in case the remote returns an INFO indicating
// that TLS is required.
func (cfg *leafNodeCfg) saveTLSHostname(u *url.URL) {
	if cfg.tlsName == _EMPTY_ && net.ParseIP(u.Hostname()) == nil {
		cfg.tlsName = u.Hostname()
	}
}

// Save off the username/password for when we connect using a bare URL
// that we get from the INFO protocol.
func (cfg *leafNodeCfg) saveUserPassword(u *url.URL) {
	if cfg.username == _EMPTY_ && u.User != nil {
		cfg.username = u.User.Username()
		cfg.password, _ = u.User.Password()
	}
}

// This starts the leafnode accept loop in a go routine, unless it
// is detected that the server has already been shutdown.
func (s *Server) startLeafNodeAcceptLoop() {
	// Snapshot server options.
	opts := s.getOpts()

	port := opts.LeafNode.Port
	if port == -1 {
		port = 0
	}

	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return
	}
	hp := net.JoinHostPort(opts.LeafNode.Host, strconv.Itoa(port))
	l, e := natsListen("tcp", hp)
	s.leafNodeListenerErr = e
	if e != nil {
		s.mu.Unlock()
		s.Fatalf("Error listening on leafnode port: %d - %v", opts.LeafNode.Port, e)
		return
	}

	s.Noticef("Listening for leafnode connections on %s",
		net.JoinHostPort(opts.LeafNode.Host, strconv.Itoa(l.Addr().(*net.TCPAddr).Port)))

	tlsRequired := opts.LeafNode.TLSConfig != nil
	tlsVerify := tlsRequired && opts.LeafNode.TLSConfig.ClientAuth == tls.RequireAndVerifyClientCert
	info := Info{
		ID:            s.info.ID,
		Name:          s.info.Name,
		Version:       s.info.Version,
		GitCommit:     gitCommit,
		GoVersion:     runtime.Version(),
		AuthRequired:  true,
		TLSRequired:   tlsRequired,
		TLSVerify:     tlsVerify,
		MaxPayload:    s.info.MaxPayload, // TODO(dlc) - Allow override?
		Headers:       s.supportsHeaders(),
		JetStream:     opts.JetStream,
		Proto:         1, // Fixed for now.
		InfoOnConnect: true,
	}
	// If we have selected a random port...
	if port == 0 {
		// Write resolved port back to options.
		opts.LeafNode.Port = l.Addr().(*net.TCPAddr).Port
	}

	s.leafNodeInfo = info
	// Possibly override Host/Port and set IP based on Cluster.Advertise
	if err := s.setLeafNodeInfoHostPortAndIP(); err != nil {
		s.Fatalf("Error setting leafnode INFO with LeafNode.Advertise value of %s, err=%v", s.opts.LeafNode.Advertise, err)
		l.Close()
		s.mu.Unlock()
		return
	}
	s.leafURLsMap[s.leafNodeInfo.IP]++
	s.generateLeafNodeInfoJSON()

	// Setup state that can enable shutdown
	s.leafNodeListener = l

	// As of now, a server that does not have remotes configured would
	// never solicit a connection, so we should not have to warn if
	// InsecureSkipVerify is set in main LeafNodes config (since
	// this TLS setting matters only when soliciting a connection).
	// Still, warn if insecure is set in any of LeafNode block.
	// We need to check remotes, even if tls is not required on accept.
	warn := tlsRequired && opts.LeafNode.TLSConfig.InsecureSkipVerify
	if !warn {
		for _, r := range opts.LeafNode.Remotes {
			if r.TLSConfig != nil && r.TLSConfig.InsecureSkipVerify {
				warn = true
				break
			}
		}
	}
	if warn {
		s.Warnf(leafnodeTLSInsecureWarning)
	}
	go s.acceptConnections(l, "Leafnode", func(conn net.Conn) { s.createLeafNode(conn, nil, nil, nil) }, nil)
	s.mu.Unlock()
}

// RegEx to match a creds file with user JWT and Seed.
var credsRe = regexp.MustCompile(`\s*(?:(?:[-]{3,}[^\n]*[-]{3,}\n)(.+)(?:\n\s*[-]{3,}[^\n]*[-]{3,}\n))`)

// Lock should be held entering here.
func (c *client) sendLeafConnect(clusterName string, tlsRequired, headers bool) error {
	// We support basic user/pass and operator based user JWT with signatures.
	cinfo := leafConnectInfo{
		TLS:     tlsRequired,
		ID:      c.srv.info.ID,
		Name:    c.srv.info.Name,
		Hub:     c.leaf.remote.Hub,
		Cluster: clusterName,
		Headers: headers,
		DenyPub: c.leaf.remote.DenyImports,
	}

	// Check for credentials first, that will take precedence..
	if creds := c.leaf.remote.Credentials; creds != "" {
		c.Debugf("Authenticating with credentials file %q", c.leaf.remote.Credentials)
		contents, err := ioutil.ReadFile(creds)
		if err != nil {
			c.Errorf("%v", err)
			return err
		}
		defer wipeSlice(contents)
		items := credsRe.FindAllSubmatch(contents, -1)
		if len(items) < 2 {
			c.Errorf("Credentials file malformed")
			return err
		}
		// First result should be the user JWT.
		// We copy here so that the file containing the seed will be wiped appropriately.
		raw := items[0][1]
		tmp := make([]byte, len(raw))
		copy(tmp, raw)
		// Seed is second item.
		kp, err := nkeys.FromSeed(items[1][1])
		if err != nil {
			c.Errorf("Credentials file has malformed seed")
			return err
		}
		// Wipe our key on exit.
		defer kp.Wipe()

		sigraw, _ := kp.Sign(c.nonce)
		sig := base64.RawURLEncoding.EncodeToString(sigraw)
		cinfo.JWT = string(tmp)
		cinfo.Sig = sig
	} else if userInfo := c.leaf.remote.curURL.User; userInfo != nil {
		cinfo.User = userInfo.Username()
		cinfo.Pass, _ = userInfo.Password()
	} else if c.leaf.remote.username != _EMPTY_ {
		cinfo.User = c.leaf.remote.username
		cinfo.Pass = c.leaf.remote.password
	}
	b, err := json.Marshal(cinfo)
	if err != nil {
		c.Errorf("Error marshaling CONNECT to route: %v\n", err)
		return err
	}
	// Although this call is made before the writeLoop is created,
	// we don't really need to send in place. The protocol will be
	// sent out by the writeLoop.
	c.enqueueProto([]byte(fmt.Sprintf(ConProto, b)))
	return nil
}

// Makes a deep copy of the LeafNode Info structure.
// The server lock is held on entry.
func (s *Server) copyLeafNodeInfo() *Info {
	clone := s.leafNodeInfo
	// Copy the array of urls.
	if len(s.leafNodeInfo.LeafNodeURLs) > 0 {
		clone.LeafNodeURLs = append([]string(nil), s.leafNodeInfo.LeafNodeURLs...)
	}
	return &clone
}

// Adds a LeafNode URL that we get when a route connects to the Info structure.
// Regenerates the JSON byte array so that it can be sent to LeafNode connections.
// Returns a boolean indicating if the URL was added or not.
// Server lock is held on entry
func (s *Server) addLeafNodeURL(urlStr string) bool {
	if s.leafURLsMap.addUrl(urlStr) {
		s.generateLeafNodeInfoJSON()
		return true
	}
	return false
}

// Removes a LeafNode URL of the route that is disconnecting from the Info structure.
// Regenerates the JSON byte array so that it can be sent to LeafNode connections.
// Returns a boolean indicating if the URL was removed or not.
// Server lock is held on entry.
func (s *Server) removeLeafNodeURL(urlStr string) bool {
	// Don't need to do this if we are removing the route connection because
	// we are shuting down...
	if s.shutdown {
		return false
	}
	if s.leafURLsMap.removeUrl(urlStr) {
		s.generateLeafNodeInfoJSON()
		return true
	}
	return false
}

// Server lock is held on entry
func (s *Server) generateLeafNodeInfoJSON() {
	s.leafNodeInfo.LeafNodeURLs = s.leafURLsMap.getAsStringSlice()
	s.leafNodeInfo.WSConnectURLs = s.websocket.connectURLsMap.getAsStringSlice()
	b, _ := json.Marshal(s.leafNodeInfo)
	pcs := [][]byte{[]byte("INFO"), b, []byte(CR_LF)}
	s.leafNodeInfoJSON = bytes.Join(pcs, []byte(" "))
}

// Sends an async INFO protocol so that the connected servers can update
// their list of LeafNode urls.
func (s *Server) sendAsyncLeafNodeInfo() {
	for _, c := range s.leafs {
		c.mu.Lock()
		c.enqueueProto(s.leafNodeInfoJSON)
		c.mu.Unlock()
	}
}

// Called when an inbound leafnode connection is accepted or we create one for a solicited leafnode.
func (s *Server) createLeafNode(conn net.Conn, rURL *url.URL, remote *leafNodeCfg, ws *websocket) *client {
	// Snapshot server options.
	opts := s.getOpts()

	maxPay := int32(opts.MaxPayload)
	maxSubs := int32(opts.MaxSubs)
	// For system, maxSubs of 0 means unlimited, so re-adjust here.
	if maxSubs == 0 {
		maxSubs = -1
	}
	now := time.Now().UTC()

	c := &client{srv: s, nc: conn, kind: LEAF, opts: defaultOpts, mpay: maxPay, msubs: maxSubs, start: now, last: now}
	// Do not update the smap here, we need to do it in initLeafNodeSmapAndSendSubs
	c.leaf = &leaf{}

	// For accepted LN connections, ws will be != nil if it was accepted
	// through the Websocket port.
	c.ws = ws

	// For remote, check if the scheme starts with "ws", if so, we will initiate
	// a remote Leaf Node connection as a websocket connection.
	if remote != nil && rURL != nil && isWSURL(rURL) {
		remote.RLock()
		c.ws = &websocket{compress: remote.Websocket.Compression, maskwrite: !remote.Websocket.NoMasking}
		remote.RUnlock()
	}

	// Determines if we are soliciting the connection or not.
	var solicited bool
	var acc *Account

	if remote != nil {
		// TODO: Decide what should be the optimal behavior here.
		// For now, if lookup fails, we will constantly try
		// to recreate this LN connection.

		remote.Lock()
		// Users can bind to any local account, if its empty
		// we will assume the $G account.
		if remote.LocalAccount == _EMPTY_ {
			remote.LocalAccount = globalAccountName
		}
		lacc := remote.LocalAccount
		remote.Unlock()

		var err error
		acc, err = s.LookupAccount(lacc)
		if err != nil {
			s.Errorf("No local account %q for leafnode: %v", lacc, err)
			return nil
		}
	}

	c.mu.Lock()
	c.initClient()
	c.Noticef("Leafnode connection created")

	if remote != nil {
		solicited = true
		remote.Lock()
		c.leaf.remote = remote
		c.setPermissions(remote.perms)
		if !c.leaf.remote.Hub {
			c.leaf.isSpoke = true
		}
		remote.Unlock()
		c.acc = acc
	} else {
		c.flags.set(expectConnect)
		if ws != nil {
			c.Debugf("Leafnode compression=%v", c.ws.compress)
		}
	}
	c.mu.Unlock()

	var nonce [nonceLen]byte
	var info *Info

	// Grab this before the client lock below.
	if !solicited {
		// Grab server variables
		s.mu.Lock()
		info = s.copyLeafNodeInfo()
		s.generateNonce(nonce[:])
		s.mu.Unlock()
	}

	// Grab lock
	c.mu.Lock()

	var preBuf []byte
	if solicited {
		// For websocket connection, we need to send an HTTP request,
		// and get the response before starting the readLoop to get
		// the INFO, etc..
		if c.isWebsocket() {
			var err error
			var closeReason ClosedState

			preBuf, closeReason, err = c.leafNodeSolicitWSConnection(opts, rURL, remote)
			if err != nil {
				c.Errorf("Error soliciting websocket connection: %v", err)
				c.mu.Unlock()
				if closeReason != 0 {
					c.closeConnection(closeReason)
				}
				return nil
			}
		} else {
			// We need to wait for the info, but not for too long.
			c.nc.SetReadDeadline(time.Now().Add(DEFAULT_LEAFNODE_INFO_WAIT))
		}

		// We will process the INFO from the readloop and finish by
		// sending the CONNECT and finish registration later.
	} else {
		// Send our info to the other side.
		// Remember the nonce we sent here for signatures, etc.
		c.nonce = make([]byte, nonceLen)
		copy(c.nonce, nonce[:])
		info.Nonce = string(c.nonce)
		info.CID = c.cid
		b, _ := json.Marshal(info)

		pcs := [][]byte{[]byte("INFO"), b, []byte(CR_LF)}
		// We have to send from this go routine because we may
		// have to block for TLS handshake before we start our
		// writeLoop go routine. The other side needs to receive
		// this before it can initiate the TLS handshake..
		c.sendProtoNow(bytes.Join(pcs, []byte(" ")))

		// The above call could have marked the connection as closed (due to TCP error).
		if c.isClosed() {
			c.mu.Unlock()
			c.closeConnection(WriteError)
			return nil
		}

		// Check to see if we need to spin up TLS.
		if !c.isWebsocket() && info.TLSRequired {
			// Perform server-side TLS handshake.
			if err := c.doTLSServerHandshake("leafnode", opts.LeafNode.TLSConfig, opts.LeafNode.TLSTimeout); err != nil {
				c.mu.Unlock()
				return nil
			}
		}

		// Leaf nodes will always require a CONNECT to let us know
		// when we are properly bound to an account.
		c.setAuthTimer(secondsToDuration(opts.LeafNode.AuthTimeout))
	}

	// Keep track in case server is shutdown before we can successfully register.
	if !s.addToTempClients(c.cid, c) {
		c.mu.Unlock()
		c.setNoReconnect()
		c.closeConnection(ServerShutdown)
		return nil
	}

	// Spin up the read loop.
	s.startGoRoutine(func() { c.readLoop(preBuf) })

	// We will sping the write loop for solicited connections only
	// when processing the INFO and after switching to TLS if needed.
	if !solicited {
		s.startGoRoutine(func() { c.writeLoop() })
	}

	c.mu.Unlock()

	return c
}

func (c *client) processLeafnodeInfo(info *Info) {
	s := c.srv
	opts := s.getOpts()
	hasSysShared, sysAcc := s.hasSystemRemoteLeaf(), s.SystemAccount()

	c.mu.Lock()
	if c.leaf == nil || c.isClosed() {
		c.mu.Unlock()
		return
	}

	var firstINFO bool

	// Mark that the INFO protocol has been received.
	// Note: For now, only the initial INFO has a nonce. We
	// will probably do auto key rotation at some point.
	if c.flags.setIfNotSet(infoReceived) {
		firstINFO = true
		// Prevent connecting to non leafnode port. Need to do this only for
		// the first INFO, not for async INFO updates...
		//
		// Content of INFO sent by the server when accepting a tcp connection.
		// -------------------------------------------------------------------
		// Listen Port Of | CID | ClientConnectURLs | LeafNodeURLs | Gateway |
		// -------------------------------------------------------------------
		//      CLIENT    |  X* |        X**        |              |         |
		//      ROUTE     |     |        X**        |      X***    |         |
		//     GATEWAY    |     |                   |              |    X    |
		//     LEAFNODE   |  X  |                   |       X      |         |
		// -------------------------------------------------------------------
		// *   Not on older servers.
		// **  Not if "no advertise" is enabled.
		// *** Not if leafnode's "no advertise" is enabled.
		//
		// As seen from above, a solicited LeafNode connection should receive
		// from the remote server an INFO with CID and LeafNodeURLs. Anything
		// else should be considered an attempt to connect to a wrong port.
		if c.leaf.remote != nil && (info.CID == 0 || info.LeafNodeURLs == nil) {
			c.mu.Unlock()
			c.Errorf(ErrConnectedToWrongPort.Error())
			c.closeConnection(WrongPort)
			return
		}
		// Capture a nonce here.
		c.nonce = []byte(info.Nonce)
		if info.TLSRequired && c.leaf.remote != nil {
			c.leaf.remote.TLS = true
		}
		supportsHeaders := c.srv.supportsHeaders()
		c.headers = supportsHeaders && info.Headers

		// Remember the remote server.
		// Pre 2.2.0 servers are not sending their server name.
		// In that case, use info.ID, which, for those servers, matches
		// the content of the field `Name` in the leafnode CONNECT protocol.
		if info.Name == _EMPTY_ {
			c.leaf.remoteServer = info.ID
		} else {
			c.leaf.remoteServer = info.Name
		}

		// Check for JetStream semantics to deny the JetStream API as needed.
		// This is so that if JetStream is enabled on both sides we can separately address both.
		if remote, acc := c.leaf.remote, c.acc; remote != nil {
			remote.Lock()
			if acc != sysAcc {
				if hasSysShared {
					s.addInJSDeny(remote)
				} else {
					// Here we want to suppress if this local account has JS enabled.
					// This is regardless of whether or not this server is actually running JS.
					// We do consider this if the other side is not running JetStream.
					if acc != nil && acc.jetStreamConfigured() && info.JetStream {
						s.addInJSDeny(remote)
					}
				}
				// If we have a specified JetStream domain we will want to add a mapping to
				// allow access cross domain for each non-system account.
				if opts.JetStreamDomain != _EMPTY_ && acc != sysAcc && acc.jetStreamConfigured() {
					src := fmt.Sprintf(jsDomainAPI, opts.JetStreamDomain)
					if err := acc.AddMapping(src, jsAllAPI); err != nil {
						c.Debugf("Error adding JetStream domain mapping: %v", err)
					}
				}
			} else if opts.JetStreamDomain != _EMPTY_ {
				s.addInJSDenyAll(remote)
			}
			c.setPermissions(remote.perms)
			remote.Unlock()
		}
	}
	// For both initial INFO and async INFO protocols, Possibly
	// update our list of remote leafnode URLs we can connect to.
	if c.leaf.remote != nil && (len(info.LeafNodeURLs) > 0 || len(info.WSConnectURLs) > 0) {
		// Consider the incoming array as the most up-to-date
		// representation of the remote cluster's list of URLs.
		c.updateLeafNodeURLs(info)
	}

	// Check to see if we have permissions updates here.
	if info.Import != nil || info.Export != nil {
		perms := &Permissions{
			Publish:   info.Export,
			Subscribe: info.Import,
		}
		// Check if we have local deny clauses that we need to merge.
		if remote := c.leaf.remote; remote != nil {
			if len(remote.DenyExports) > 0 {
				perms.Publish.Deny = append(perms.Publish.Deny, remote.DenyExports...)
			}
			if len(remote.DenyImports) > 0 {
				perms.Subscribe.Deny = append(perms.Subscribe.Deny, remote.DenyImports...)
			}
		}
		c.setPermissions(perms)
	}

	var resumeConnect bool

	// If this is a remote connection and this is the first INFO protocol,
	// then we need to finish the connect process by sending CONNECT, etc..
	if firstINFO && c.leaf.remote != nil {
		// Clear deadline that was set in createLeafNode while waiting for the INFO.
		c.nc.SetDeadline(time.Time{})
		resumeConnect = true
	}

	// Check if we have the remote account information and if so make sure it's stored.
	if info.RemoteAccount != _EMPTY_ {
		s.leafRemoteAccounts.Store(c.acc.Name, info.RemoteAccount)
	}
	c.mu.Unlock()

	finishConnect := info.ConnectInfo
	if resumeConnect && s != nil {
		s.leafNodeResumeConnectProcess(c)
		if !info.InfoOnConnect {
			finishConnect = true
		}
	}
	if finishConnect {
		s.leafNodeFinishConnectProcess(c)
	}
}

// When getting a leaf node INFO protocol, use the provided
// array of urls to update the list of possible endpoints.
func (c *client) updateLeafNodeURLs(info *Info) {
	cfg := c.leaf.remote
	cfg.Lock()
	defer cfg.Unlock()

	// We have ensured that if a remote has a WS scheme, then all are.
	// So check if first is WS, then add WS URLs, otherwise, add non WS ones.
	if len(cfg.URLs) > 0 && isWSURL(cfg.URLs[0]) {
		// We use wsSchemePrefix. It does not matter if TLS or not since
		// the distinction is done when creating the LN connection based
		// on presence of TLS config, etc..
		c.doUpdateLNURLs(cfg, wsSchemePrefix, info.WSConnectURLs)
		return
	}
	c.doUpdateLNURLs(cfg, "nats-leaf", info.LeafNodeURLs)
}

func (c *client) doUpdateLNURLs(cfg *leafNodeCfg, scheme string, URLs []string) {
	cfg.urls = make([]*url.URL, 0, 1+len(URLs))
	// Add the ones we receive in the protocol
	for _, surl := range URLs {
		url, err := url.Parse(fmt.Sprintf("%s://%s", scheme, surl))
		if err != nil {
			c.Errorf("Error parsing url %q: %v", surl, err)
			continue
		}
		// Do not add if it's the same as what we already have configured.
		var dup bool
		for _, u := range cfg.URLs {
			// URLs that we receive never have user info, but the
			// ones that were configured may have. Simply compare
			// host and port to decide if they are equal or not.
			if url.Host == u.Host && url.Port() == u.Port() {
				dup = true
				break
			}
		}
		if !dup {
			cfg.urls = append(cfg.urls, url)
			cfg.saveTLSHostname(url)
		}
	}
	// Add the configured one
	cfg.urls = append(cfg.urls, cfg.URLs...)
}

// Similar to setInfoHostPortAndGenerateJSON, but for leafNodeInfo.
func (s *Server) setLeafNodeInfoHostPortAndIP() error {
	opts := s.getOpts()
	if opts.LeafNode.Advertise != _EMPTY_ {
		advHost, advPort, err := parseHostPort(opts.LeafNode.Advertise, opts.LeafNode.Port)
		if err != nil {
			return err
		}
		s.leafNodeInfo.Host = advHost
		s.leafNodeInfo.Port = advPort
	} else {
		s.leafNodeInfo.Host = opts.LeafNode.Host
		s.leafNodeInfo.Port = opts.LeafNode.Port
		// If the host is "0.0.0.0" or "::" we need to resolve to a public IP.
		// This will return at most 1 IP.
		hostIsIPAny, ips, err := s.getNonLocalIPsIfHostIsIPAny(s.leafNodeInfo.Host, false)
		if err != nil {
			return err
		}
		if hostIsIPAny {
			if len(ips) == 0 {
				s.Errorf("Could not find any non-local IP for leafnode's listen specification %q",
					s.leafNodeInfo.Host)
			} else {
				// Take the first from the list...
				s.leafNodeInfo.Host = ips[0]
			}
		}
	}
	// Use just host:port for the IP
	s.leafNodeInfo.IP = net.JoinHostPort(s.leafNodeInfo.Host, strconv.Itoa(s.leafNodeInfo.Port))
	if opts.LeafNode.Advertise != _EMPTY_ {
		s.Noticef("Advertise address for leafnode is set to %s", s.leafNodeInfo.IP)
	}
	return nil
}

// Add the connection to the map of leaf nodes.
// If `checkForDup` is true (invoked when a leafnode is accepted), then we check
// if a connection already exists for the same server name and account.
// That can happen when the remote is attempting to reconnect while the accepting
// side did not detect the connection as broken yet.
// But it can also happen when there is a misconfiguration and the remote is
// creating two (or more) connections that bind to the same account on the accept
// side.
// When a duplicate is found, the new connection is accepted and the old is closed
// (this solves the stale connection situation). An error is returned to help the
// remote detect the misconfiguration when the duplicate is the result of that
// misconfiguration.
func (s *Server) addLeafNodeConnection(c *client, srvName string, checkForDup bool) {
	var accName string
	c.mu.Lock()
	cid := c.cid
	if c.acc != nil {
		accName = c.acc.Name
	}
	c.mu.Unlock()

	var old *client
	s.mu.Lock()
	// We check for empty because in some test we may send empty CONNECT{}
	if checkForDup && srvName != _EMPTY_ {
		for _, ol := range s.leafs {
			ol.mu.Lock()
			// We care here only about non solicited Leafnode. This function
			// is more about replacing stale connections than detecting loops.
			// We have code for the loop detection elsewhere, which also delays
			// attempt to reconnect.
			if !ol.isSolicitedLeafNode() && ol.leaf.remoteServer == srvName && ol.acc.Name == accName {
				old = ol
			}
			ol.mu.Unlock()
			if old != nil {
				break
			}
		}
	}
	// Store new connection in the map
	s.leafs[cid] = c
	s.mu.Unlock()
	s.removeFromTempClients(cid)

	// If applicable, evict the old one.
	if old != nil {
		old.sendErrAndErr(DuplicateRemoteLeafnodeConnection.String())
		old.closeConnection(DuplicateRemoteLeafnodeConnection)
		c.Warnf("Replacing connection from same server")
	}
}

func (s *Server) removeLeafNodeConnection(c *client) {
	c.mu.Lock()
	cid := c.cid
	if c.leaf != nil && c.leaf.tsubt != nil {
		c.leaf.tsubt.Stop()
		c.leaf.tsubt = nil
	}
	c.mu.Unlock()
	s.mu.Lock()
	delete(s.leafs, cid)
	s.mu.Unlock()
	s.removeFromTempClients(cid)
}

type leafConnectInfo struct {
	JWT     string `json:"jwt,omitempty"`
	Sig     string `json:"sig,omitempty"`
	User    string `json:"user,omitempty"`
	Pass    string `json:"pass,omitempty"`
	TLS     bool   `json:"tls_required"`
	Comp    bool   `json:"compression,omitempty"`
	ID      string `json:"server_id,omitempty"`
	Name    string `json:"name,omitempty"`
	Hub     bool   `json:"is_hub,omitempty"`
	Cluster string `json:"cluster,omitempty"`
	Headers bool   `json:"headers,omitempty"`
	// Just used to detect wrong connection attempts.
	Gateway string   `json:"gateway,omitempty"`
	DenyPub []string `json:"deny_pub,omitempty"`
}

// processLeafNodeConnect will process the inbound connect args.
// Once we are here we are bound to an account, so can send any interest that
// we would have to the other side.
func (c *client) processLeafNodeConnect(s *Server, arg []byte, lang string) error {
	// Way to detect clients that incorrectly connect to the route listen
	// port. Client provided "lang" in the CONNECT protocol while LEAFNODEs don't.
	if lang != _EMPTY_ {
		c.sendErrAndErr(ErrClientConnectedToLeafNodePort.Error())
		c.closeConnection(WrongPort)
		return ErrClientConnectedToLeafNodePort
	}

	// Unmarshal as a leaf node connect protocol
	proto := &leafConnectInfo{}
	if err := json.Unmarshal(arg, proto); err != nil {
		return err
	}

	// Reject if this has Gateway which means that it would be from a gateway
	// connection that incorrectly connects to the leafnode port.
	if proto.Gateway != "" {
		errTxt := fmt.Sprintf("Rejecting connection from gateway %q on the leafnode port", proto.Gateway)
		c.Errorf(errTxt)
		c.sendErr(errTxt)
		c.closeConnection(WrongGateway)
		return ErrWrongGateway
	}

	// Check if this server supports headers.
	supportHeaders := c.srv.supportsHeaders()

	// Grab system account and server options.
	sysAcc, opts := s.SystemAccount(), s.getOpts()

	c.mu.Lock()
	// Leaf Nodes do not do echo or verbose or pedantic.
	c.opts.Verbose = false
	c.opts.Echo = false
	c.opts.Pedantic = false
	// This inbound connection will be marked as supporting headers if this server
	// support headers and the remote has sent in the CONNECT protocol that it does
	// support headers too.
	c.headers = supportHeaders && proto.Headers

	// Remember the remote server.
	c.leaf.remoteServer = proto.Name

	// If the other side has declared itself a hub, so we will take on the spoke role.
	if proto.Hub {
		c.leaf.isSpoke = true
	}

	// The soliciting side is part of a cluster.
	if proto.Cluster != _EMPTY_ {
		c.leaf.remoteCluster = proto.Cluster
	}

	// Check for JetStream domain
	doDomainMappings := opts.JetStreamDomain != _EMPTY_ && c.acc != sysAcc && c.acc.jetStreamConfigured()

	// Set the Ping timer
	s.setFirstPingTimer(c)

	// If we received pub deny permissions from the other end, merge with existing ones.
	c.mergePubDenyPermissions(proto.DenyPub)

	acc := c.acc
	c.mu.Unlock()

	// Add in the leafnode here since we passed through auth at this point.
	s.addLeafNodeConnection(c, proto.Name, true)

	// If we have permissions bound to this leafnode we need to send then back to the
	// origin server for local enforcement.
	s.sendPermsAndAccountInfo(c)

	// Create and initialize the smap since we know our bound account now.
	// This will send all registered subs too.
	s.initLeafNodeSmapAndSendSubs(c)

	// Announce the account connect event for a leaf node.
	// This will no-op as needed.
	s.sendLeafNodeConnect(c.acc)

	// If we have a specified JetStream domain we will want to add a mapping to
	// allow access cross domain for each non-system account.
	if doDomainMappings {
		src := fmt.Sprintf(jsDomainAPI, opts.JetStreamDomain)
		if err := acc.AddMapping(src, jsAllAPI); err != nil {
			c.Debugf("Error adding JetStream domain mapping: %v", err)
		}
	}

	return nil
}

// Returns the remote cluster name. This is set only once so does not require a lock.
func (c *client) remoteCluster() string {
	if c.leaf == nil {
		return _EMPTY_
	}
	return c.leaf.remoteCluster
}

// Sends back an info block to the soliciting leafnode to let it know about
// its permission settings for local enforcement.
func (s *Server) sendPermsAndAccountInfo(c *client) {
	// Copy
	info := s.copyLeafNodeInfo()
	c.mu.Lock()
	info.CID = c.cid
	info.Import = c.opts.Import
	info.Export = c.opts.Export
	info.RemoteAccount = c.acc.Name
	info.ConnectInfo = true
	b, _ := json.Marshal(info)
	pcs := [][]byte{[]byte("INFO"), b, []byte(CR_LF)}
	c.enqueueProto(bytes.Join(pcs, []byte(" ")))
	c.mu.Unlock()
}

// Snapshot the current subscriptions from the sublist into our smap which
// we will keep updated from now on.
// Also send the registered subscriptions.
func (s *Server) initLeafNodeSmapAndSendSubs(c *client) {
	acc := c.acc
	if acc == nil {
		c.Debugf("Leafnode does not have an account bound")
		return
	}
	// Collect all account subs here.
	_subs := [32]*subscription{}
	subs := _subs[:0]
	ims := []string{}

	acc.mu.Lock()
	accName := acc.Name
	accNTag := acc.nameTag
	// If we are solicited we only send interest for local clients.
	if c.isSpokeLeafNode() {
		acc.sl.localSubs(&subs)
	} else {
		acc.sl.All(&subs)
	}

	// Check if we have an existing service import reply.
	siReply := acc.siReply

	// Since leaf nodes only send on interest, if the bound
	// account has import services we need to send those over.
	for isubj := range acc.imports.services {
		if !c.canSubscribe(isubj) {
			c.Debugf("Not permitted to import service %s on behalf of %s/%s", isubj, accName, accNTag)
			continue
		}
		ims = append(ims, isubj)
	}
	// Likewise for mappings.
	for _, m := range acc.mappings {
		ims = append(ims, m.src)
	}

	// Create a unique subject that will be used for loop detection.
	lds := acc.lds
	if lds == _EMPTY_ {
		lds = leafNodeLoopDetectionSubjectPrefix + nuid.Next()
		acc.lds = lds
	}
	acc.mu.Unlock()

	// Now check for gateway interest. Leafnodes will put this into
	// the proper mode to propagate, but they are not held in the account.
	gwsa := [16]*client{}
	gws := gwsa[:0]
	s.getOutboundGatewayConnections(&gws)
	for _, cgw := range gws {
		cgw.mu.Lock()
		gw := cgw.gw
		cgw.mu.Unlock()
		if gw != nil {
			if ei, _ := gw.outsim.Load(accName); ei != nil {
				if e := ei.(*outsie); e != nil && e.sl != nil {
					e.sl.All(&subs)
				}
			}
		}
	}

	applyGlobalRouting := s.gateway.enabled
	if c.isSpokeLeafNode() {
		// Add a fake subscription for this solicited leafnode connection
		// so that we can send back directly for mapped GW replies.
		c.srv.gwLeafSubs.Insert(&subscription{client: c, subject: []byte(gwReplyPrefix + ">")})
	}

	// Now walk the results and add them to our smap
	c.mu.Lock()
	rc := c.leaf.remoteCluster
	c.leaf.smap = make(map[string]int32)
	for _, sub := range subs {
		if !c.canSubscribe(string(sub.subject)) {
			c.Debugf("Not permitted to subscribe to %s on behalf of %s/%s", string(sub.subject), accName, accNTag)
			continue
		}
		// We ignore ourselves here.
		// Also don't add the subscription if it has a origin cluster and the
		// cluster name matches the one of the client we are sending to.
		if c != sub.client && (sub.origin == nil || (string(sub.origin) != rc)) {
			c.leaf.smap[keyFromSub(sub)]++
			if c.leaf.tsub == nil {
				c.leaf.tsub = make(map[*subscription]struct{})
			}
			c.leaf.tsub[sub] = struct{}{}
		}
	}
	// FIXME(dlc) - We need to update appropriately on an account claims update.
	for _, isubj := range ims {
		c.leaf.smap[isubj]++
	}
	// If we have gateways enabled we need to make sure the other side sends us responses
	// that have been augmented from the original subscription.
	// TODO(dlc) - Should we lock this down more?
	if applyGlobalRouting {
		c.leaf.smap[oldGWReplyPrefix+"*.>"]++
		c.leaf.smap[gwReplyPrefix+">"]++
	}
	// Detect loop by subscribing to a specific subject and checking
	// if this is coming back to us.
	c.leaf.smap[lds]++

	// Check if we need to add an existing siReply to our map.
	// This will be a prefix so add on the wildcard.
	if siReply != nil {
		wcsub := append(siReply, '>')
		c.leaf.smap[string(wcsub)]++
	}
	// Queue all protocols. There is no max pending limit for LN connection,
	// so we don't need chunking. The writes will happen from the writeLoop.
	var b bytes.Buffer
	for key, n := range c.leaf.smap {
		c.writeLeafSub(&b, key, n)
	}
	if b.Len() > 0 {
		c.enqueueProto(b.Bytes())
	}
	if c.leaf.tsub != nil {
		// Clear the tsub map after 5 seconds.
		c.leaf.tsubt = time.AfterFunc(5*time.Second, func() {
			c.mu.Lock()
			if c.leaf != nil {
				c.leaf.tsub = nil
				c.leaf.tsubt = nil
			}
			c.mu.Unlock()
		})
	}
	c.mu.Unlock()
}

// updateInterestForAccountOnGateway called from gateway code when processing RS+ and RS-.
func (s *Server) updateInterestForAccountOnGateway(accName string, sub *subscription, delta int32) {
	acc, err := s.LookupAccount(accName)
	if acc == nil || err != nil {
		s.Debugf("No or bad account for %q, failed to update interest from gateway", accName)
		return
	}
	s.updateLeafNodes(acc, sub, delta)
}

// updateLeafNodes will make sure to update the smap for the subscription. Will
// also forward to all leaf nodes as needed.
func (s *Server) updateLeafNodes(acc *Account, sub *subscription, delta int32) {
	if acc == nil || sub == nil {
		return
	}

	_l := [32]*client{}
	leafs := _l[:0]

	// Grab all leaf nodes. Ignore a leafnode if sub's client is a leafnode and matches.
	acc.mu.RLock()
	for _, ln := range acc.lleafs {
		if ln != sub.client {
			leafs = append(leafs, ln)
		}
	}
	acc.mu.RUnlock()

	for _, ln := range leafs {
		// Check to make sure this sub does not have an origin cluster than matches the leafnode.
		ln.mu.Lock()
		skip := sub.origin != nil && string(sub.origin) == ln.remoteCluster()
		// do not skip on !ln.canSubscribe(string(sub.subject))
		// Given allow:foo, > would be rejected. For leaf nodes filtering is done on the (soliciting) end.
		ln.mu.Unlock()
		if skip {
			continue
		}
		ln.updateSmap(sub, delta)
	}
}

// This will make an update to our internal smap and determine if we should send out
// an interest update to the remote side.
func (c *client) updateSmap(sub *subscription, delta int32) {
	key := keyFromSub(sub)

	c.mu.Lock()
	if c.leaf.smap == nil {
		c.mu.Unlock()
		return
	}

	// If we are solicited make sure this is a local client or a non-solicited leaf node
	skind := sub.client.kind
	updateClient := skind == CLIENT || skind == SYSTEM || skind == JETSTREAM || skind == ACCOUNT
	if c.isSpokeLeafNode() && !(updateClient || (skind == LEAF && !sub.client.isSpokeLeafNode())) {
		c.mu.Unlock()
		return
	}

	// For additions, check if that sub has just been processed during initLeafNodeSmapAndSendSubs
	if delta > 0 && c.leaf.tsub != nil {
		if _, present := c.leaf.tsub[sub]; present {
			delete(c.leaf.tsub, sub)
			if len(c.leaf.tsub) == 0 {
				c.leaf.tsub = nil
				c.leaf.tsubt.Stop()
				c.leaf.tsubt = nil
			}
			c.mu.Unlock()
			return
		}
	}

	n := c.leaf.smap[key]
	// We will update if its a queue, if count is zero (or negative), or we were 0 and are N > 0.
	update := sub.queue != nil || n == 0 || n+delta <= 0
	n += delta
	if n > 0 {
		c.leaf.smap[key] = n
	} else {
		delete(c.leaf.smap, key)
	}
	if update {
		c.sendLeafNodeSubUpdate(key, n)
	}
	c.mu.Unlock()
}

// Used to force add subjects to the subject map.
func (c *client) forceAddToSmap(subj string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := c.leaf.smap[subj]
	if n != 0 {
		return
	}
	// Place into the map since it was not there.
	c.leaf.smap[subj] = 1
	c.sendLeafNodeSubUpdate(subj, 1)
}

// Send the subscription interest change to the other side.
// Lock should be held.
func (c *client) sendLeafNodeSubUpdate(key string, n int32) {
	// If we are a spoke, we need to check if we are allowed to send this subscription over to the hub.
	if c.isSpokeLeafNode() {
		checkPerms := true
		if len(key) > 0 && (key[0] == '$' || key[0] == '_') {
			if strings.HasPrefix(key, leafNodeLoopDetectionSubjectPrefix) ||
				strings.HasPrefix(key, oldGWReplyPrefix) ||
				strings.HasPrefix(key, gwReplyPrefix) {
				checkPerms = false
			}
		}
		if checkPerms && !c.canSubscribe(key) {
			return
		}
	}
	// If we are here we can send over to the other side.
	_b := [64]byte{}
	b := bytes.NewBuffer(_b[:0])
	c.writeLeafSub(b, key, n)
	c.enqueueProto(b.Bytes())
}

// Helper function to build the key.
func keyFromSub(sub *subscription) string {
	var _rkey [1024]byte
	var key []byte

	if sub.queue != nil {
		// Just make the key subject spc group, e.g. 'foo bar'
		key = _rkey[:0]
		key = append(key, sub.subject...)
		key = append(key, byte(' '))
		key = append(key, sub.queue...)
	} else {
		key = sub.subject
	}
	return string(key)
}

// Lock should be held.
func (c *client) writeLeafSub(w *bytes.Buffer, key string, n int32) {
	if key == "" {
		return
	}
	if n > 0 {
		w.WriteString("LS+ " + key)
		// Check for queue semantics, if found write n.
		if strings.Contains(key, " ") {
			w.WriteString(" ")
			var b [12]byte
			var i = len(b)
			for l := n; l > 0; l /= 10 {
				i--
				b[i] = digits[l%10]
			}
			w.Write(b[i:])
			if c.trace {
				arg := fmt.Sprintf("%s %d", key, n)
				c.traceOutOp("LS+", []byte(arg))
			}
		} else if c.trace {
			c.traceOutOp("LS+", []byte(key))
		}
	} else {
		w.WriteString("LS- " + key)
		if c.trace {
			c.traceOutOp("LS-", []byte(key))
		}
	}
	w.WriteString(CR_LF)
}

// processLeafSub will process an inbound sub request for the remote leaf node.
func (c *client) processLeafSub(argo []byte) (err error) {
	// Indicate activity.
	c.in.subs++

	srv := c.srv
	if srv == nil {
		return nil
	}

	// Copy so we do not reference a potentially large buffer
	arg := make([]byte, len(argo))
	copy(arg, argo)

	args := splitArg(arg)
	sub := &subscription{client: c}

	switch len(args) {
	case 1:
		sub.queue = nil
	case 3:
		sub.queue = args[1]
		sub.qw = int32(parseSize(args[2]))
	default:
		return fmt.Errorf("processLeafSub Parse Error: '%s'", arg)
	}
	sub.subject = args[0]

	c.mu.Lock()
	if c.isClosed() {
		c.mu.Unlock()
		return nil
	}

	acc := c.acc
	// Check if we have a loop.
	ldsPrefix := bytes.HasPrefix(sub.subject, []byte(leafNodeLoopDetectionSubjectPrefix))
	if ldsPrefix && string(sub.subject) == acc.getLDSubject() {
		c.mu.Unlock()
		c.handleLeafNodeLoop(true)
		return nil
	}

	// Check permissions if applicable. (but exclude the $LDS, $GR and _GR_)
	checkPerms := true
	if sub.subject[0] == '$' || sub.subject[0] == '_' {
		if ldsPrefix ||
			bytes.HasPrefix(sub.subject, []byte(oldGWReplyPrefix)) ||
			bytes.HasPrefix(sub.subject, []byte(gwReplyPrefix)) {
			checkPerms = false
		}
	}
	if checkPerms && c.isHubLeafNode() && !c.canSubscribe(string(sub.subject)) {
		c.mu.Unlock()
		c.leafSubPermViolation(sub.subject)
		return nil
	}

	// Check if we have a maximum on the number of subscriptions.
	if c.subsAtLimit() {
		c.mu.Unlock()
		c.maxSubsExceeded()
		return nil
	}

	// If we have an origin cluster associated mark that in the sub.
	if rc := c.remoteCluster(); rc != _EMPTY_ {
		sub.origin = []byte(rc)
	}

	// Like Routes, we store local subs by account and subject and optionally queue name.
	// If we have a queue it will have a trailing weight which we do not want.
	if sub.queue != nil {
		sub.sid = arg[:len(arg)-len(args[2])-1]
	} else {
		sub.sid = arg
	}
	key := string(sub.sid)
	osub := c.subs[key]
	updateGWs := false
	delta := int32(1)
	if osub == nil {
		c.subs[key] = sub
		// Now place into the account sl.
		if err := acc.sl.Insert(sub); err != nil {
			delete(c.subs, key)
			c.mu.Unlock()
			c.Errorf("Could not insert subscription: %v", err)
			c.sendErr("Invalid Subscription")
			return nil
		}
		updateGWs = srv.gateway.enabled
	} else if sub.queue != nil {
		// For a queue we need to update the weight.
		delta = sub.qw - atomic.LoadInt32(&osub.qw)
		atomic.StoreInt32(&osub.qw, sub.qw)
		acc.sl.UpdateRemoteQSub(osub)
	}
	spoke := c.isSpokeLeafNode()
	c.mu.Unlock()

	if err := c.addShadowSubscriptions(acc, sub); err != nil {
		c.Errorf(err.Error())
	}

	// If we are not solicited, treat leaf node subscriptions similar to a
	// client subscription, meaning we forward them to routes, gateways and
	// other leaf nodes as needed.
	if !spoke {
		// If we are routing add to the route map for the associated account.
		srv.updateRouteSubscriptionMap(acc, sub, delta)
		if updateGWs {
			srv.gatewayUpdateSubInterest(acc.Name, sub, delta)
		}
	}
	// Now check on leafnode updates for other leaf nodes. We understand solicited
	// and non-solicited state in this call so we will do the right thing.
	srv.updateLeafNodes(acc, sub, delta)

	return nil
}

// If the leafnode is a solicited, set the connect delay based on default
// or private option (for tests). Sends the error to the other side, log and
// close the connection.
func (c *client) handleLeafNodeLoop(sendErr bool) {
	accName, delay := c.setLeafConnectDelayIfSoliciting(leafNodeReconnectDelayAfterLoopDetected)
	errTxt := fmt.Sprintf("Loop detected for leafnode account=%q. Delaying attempt to reconnect for %v", accName, delay)
	if sendErr {
		c.sendErr(errTxt)
	}

	c.Errorf(errTxt)
	// If we are here with "sendErr" false, it means that this is the server
	// that received the error. The other side will have closed the connection,
	// but does not hurt to close here too.
	c.closeConnection(ProtocolViolation)
}

// processLeafUnsub will process an inbound unsub request for the remote leaf node.
func (c *client) processLeafUnsub(arg []byte) error {
	// Indicate any activity, so pub and sub or unsubs.
	c.in.subs++

	acc := c.acc
	srv := c.srv

	c.mu.Lock()
	if c.isClosed() {
		c.mu.Unlock()
		return nil
	}

	updateGWs := false
	spoke := c.isSpokeLeafNode()
	// We store local subs by account and subject and optionally queue name.
	// LS- will have the arg exactly as the key.
	sub, ok := c.subs[string(arg)]
	c.mu.Unlock()

	if ok {
		c.unsubscribe(acc, sub, true, true)
		updateGWs = srv.gateway.enabled
	}

	if !spoke {
		// If we are routing subtract from the route map for the associated account.
		srv.updateRouteSubscriptionMap(acc, sub, -1)
		// Gateways
		if updateGWs {
			srv.gatewayUpdateSubInterest(acc.Name, sub, -1)
		}
	}
	// Now check on leafnode updates for other leaf nodes.
	srv.updateLeafNodes(acc, sub, -1)
	return nil
}

func (c *client) processLeafHeaderMsgArgs(arg []byte) error {
	// Unroll splitArgs to avoid runtime/heap issues
	a := [MAX_MSG_ARGS][]byte{}
	args := a[:0]
	start := -1
	for i, b := range arg {
		switch b {
		case ' ', '\t', '\r', '\n':
			if start >= 0 {
				args = append(args, arg[start:i])
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		args = append(args, arg[start:])
	}

	c.pa.arg = arg
	switch len(args) {
	case 0, 1, 2:
		return fmt.Errorf("processLeafHeaderMsgArgs Parse Error: '%s'", args)
	case 3:
		c.pa.reply = nil
		c.pa.queues = nil
		c.pa.hdb = args[1]
		c.pa.hdr = parseSize(args[1])
		c.pa.szb = args[2]
		c.pa.size = parseSize(args[2])
	case 4:
		c.pa.reply = args[1]
		c.pa.queues = nil
		c.pa.hdb = args[2]
		c.pa.hdr = parseSize(args[2])
		c.pa.szb = args[3]
		c.pa.size = parseSize(args[3])
	default:
		// args[1] is our reply indicator. Should be + or | normally.
		if len(args[1]) != 1 {
			return fmt.Errorf("processLeafHeaderMsgArgs Bad or Missing Reply Indicator: '%s'", args[1])
		}
		switch args[1][0] {
		case '+':
			c.pa.reply = args[2]
		case '|':
			c.pa.reply = nil
		default:
			return fmt.Errorf("processLeafHeaderMsgArgs Bad or Missing Reply Indicator: '%s'", args[1])
		}
		// Grab header size.
		c.pa.hdb = args[len(args)-2]
		c.pa.hdr = parseSize(c.pa.hdb)

		// Grab size.
		c.pa.szb = args[len(args)-1]
		c.pa.size = parseSize(c.pa.szb)

		// Grab queue names.
		if c.pa.reply != nil {
			c.pa.queues = args[3 : len(args)-2]
		} else {
			c.pa.queues = args[2 : len(args)-2]
		}
	}
	if c.pa.hdr < 0 {
		return fmt.Errorf("processLeafHeaderMsgArgs Bad or Missing Header Size: '%s'", arg)
	}
	if c.pa.size < 0 {
		return fmt.Errorf("processLeafHeaderMsgArgs Bad or Missing Size: '%s'", args)
	}
	if c.pa.hdr > c.pa.size {
		return fmt.Errorf("processLeafHeaderMsgArgs Header Size larger then TotalSize: '%s'", arg)
	}

	// Common ones processed after check for arg length
	c.pa.subject = args[0]

	return nil
}

func (c *client) processLeafMsgArgs(arg []byte) error {
	// Unroll splitArgs to avoid runtime/heap issues
	a := [MAX_MSG_ARGS][]byte{}
	args := a[:0]
	start := -1
	for i, b := range arg {
		switch b {
		case ' ', '\t', '\r', '\n':
			if start >= 0 {
				args = append(args, arg[start:i])
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		args = append(args, arg[start:])
	}

	c.pa.arg = arg
	switch len(args) {
	case 0, 1:
		return fmt.Errorf("processLeafMsgArgs Parse Error: '%s'", args)
	case 2:
		c.pa.reply = nil
		c.pa.queues = nil
		c.pa.szb = args[1]
		c.pa.size = parseSize(args[1])
	case 3:
		c.pa.reply = args[1]
		c.pa.queues = nil
		c.pa.szb = args[2]
		c.pa.size = parseSize(args[2])
	default:
		// args[1] is our reply indicator. Should be + or | normally.
		if len(args[1]) != 1 {
			return fmt.Errorf("processLeafMsgArgs Bad or Missing Reply Indicator: '%s'", args[1])
		}
		switch args[1][0] {
		case '+':
			c.pa.reply = args[2]
		case '|':
			c.pa.reply = nil
		default:
			return fmt.Errorf("processLeafMsgArgs Bad or Missing Reply Indicator: '%s'", args[1])
		}
		// Grab size.
		c.pa.szb = args[len(args)-1]
		c.pa.size = parseSize(c.pa.szb)

		// Grab queue names.
		if c.pa.reply != nil {
			c.pa.queues = args[3 : len(args)-1]
		} else {
			c.pa.queues = args[2 : len(args)-1]
		}
	}
	if c.pa.size < 0 {
		return fmt.Errorf("processLeafMsgArgs Bad or Missing Size: '%s'", args)
	}

	// Common ones processed after check for arg length
	c.pa.subject = args[0]

	return nil
}

// processInboundLeafMsg is called to process an inbound msg from a leaf node.
func (c *client) processInboundLeafMsg(msg []byte) {
	// Update statistics
	// The msg includes the CR_LF, so pull back out for accounting.
	c.in.msgs++
	c.in.bytes += int32(len(msg) - LEN_CR_LF)

	// Check pub permissions
	if c.perms != nil && (c.perms.pub.allow != nil || c.perms.pub.deny != nil) {
		subject := c.pa.subject
		// If this subject was mapped we need to check the original subject, not the new one.
		if len(c.pa.mapped) > 0 {
			subject = c.pa.mapped
		}
		if !c.pubAllowed(string(subject)) {
			if c.isHubLeafNode() {
				c.leafPubPermViolation(subject)
			} else {
				c.Debugf("Not permitted to receive from %q", subject)
			}
			return
		}
	}

	srv := c.srv
	acc := c.acc

	// Mostly under testing scenarios.
	if srv == nil || acc == nil {
		return
	}

	// Match the subscriptions. We will use our own L1 map if
	// it's still valid, avoiding contention on the shared sublist.
	var r *SublistResult
	var ok bool

	genid := atomic.LoadUint64(&c.acc.sl.genid)
	if genid == c.in.genid && c.in.results != nil {
		r, ok = c.in.results[string(c.pa.subject)]
	} else {
		// Reset our L1 completely.
		c.in.results = make(map[string]*SublistResult)
		c.in.genid = genid
	}

	// Go back to the sublist data structure.
	if !ok {
		r = c.acc.sl.Match(string(c.pa.subject))
		c.in.results[string(c.pa.subject)] = r
		// Prune the results cache. Keeps us from unbounded growth. Random delete.
		if len(c.in.results) > maxResultCacheSize {
			n := 0
			for subject := range c.in.results {
				delete(c.in.results, subject)
				if n++; n > pruneSize {
					break
				}
			}
		}
	}

	// Collect queue names if needed.
	var qnames [][]byte

	// Check for no interest, short circuit if so.
	// This is the fanout scale.
	if len(r.psubs)+len(r.qsubs) > 0 {
		flag := pmrNoFlag
		// If we have queue subs in this cluster, then if we run in gateway
		// mode and the remote gateways have queue subs, then we need to
		// collect the queue groups this message was sent to so that we
		// exclude them when sending to gateways.
		if len(r.qsubs) > 0 && c.srv.gateway.enabled &&
			atomic.LoadInt64(&c.srv.gateway.totalQSubs) > 0 {
			flag |= pmrCollectQueueNames
		}
		_, qnames = c.processMsgResults(acc, r, msg, nil, c.pa.subject, c.pa.reply, flag)
	}

	// Now deal with gateways
	if c.srv.gateway.enabled {
		c.sendMsgToGateways(acc, msg, c.pa.subject, c.pa.reply, qnames)
	}
}

// Handles a publish permission violation.
// See leafPermViolation() for details.
func (c *client) leafPubPermViolation(subj []byte) {
	c.leafPermViolation(true, subj)
}

// Handles a subscription permission violation.
// See leafPermViolation() for details.
func (c *client) leafSubPermViolation(subj []byte) {
	c.leafPermViolation(false, subj)
}

// Common function to process publish or subscribe leafnode permission violation.
// Sends the permission violation error to the remote, logs it and closes the connection.
// If this is from a server soliciting, the reconnection will be delayed.
func (c *client) leafPermViolation(pub bool, subj []byte) {
	if c.isSpokeLeafNode() {
		// For spokes these are no-ops since the hub server told us our permissions.
		// We just need to not send these over to the other side since we will get cutoff.
		return
	}
	// FIXME(dlc) ?
	c.setLeafConnectDelayIfSoliciting(leafNodeReconnectAfterPermViolation)
	var action string
	if pub {
		c.sendErr(fmt.Sprintf("Permissions Violation for Publish to %q", subj))
		action = "Publish"
	} else {
		c.sendErr(fmt.Sprintf("Permissions Violation for Subscription to %q", subj))
		action = "Subscription"
	}
	c.Errorf("%s Violation on %q - Check other side configuration", action, subj)
	// TODO: add a new close reason that is more appropriate?
	c.closeConnection(ProtocolViolation)
}

// Invoked from generic processErr() for LEAF connections.
func (c *client) leafProcessErr(errStr string) {
	// We will look for Loop detected error coming from the other side.
	// If we solicit, set the connect delay.
	if !strings.Contains(errStr, "Loop detected") {
		return
	}
	c.handleLeafNodeLoop(false)
}

// If this leaf connection solicits, sets the connect delay to the given value,
// or the one from the server option's LeafNode.connDelay if one is set (for tests).
// Returns the connection's account name and delay.
func (c *client) setLeafConnectDelayIfSoliciting(delay time.Duration) (string, time.Duration) {
	c.mu.Lock()
	if c.isSolicitedLeafNode() {
		if s := c.srv; s != nil {
			if srvdelay := s.getOpts().LeafNode.connDelay; srvdelay != 0 {
				delay = srvdelay
			}
		}
		c.leaf.remote.setConnectDelay(delay)
	}
	accName := c.acc.Name
	c.mu.Unlock()
	return accName, delay
}

// For the given remote Leafnode configuration, this function returns
// if TLS is required, and if so, will return a clone of the TLS Config
// (since some fields will be changed during handshake), the TLS server
// name that is remembered, and the TLS timeout.
func (c *client) leafNodeGetTLSConfigForSolicit(remote *leafNodeCfg, needsLock bool) (bool, *tls.Config, string, float64) {
	var (
		tlsConfig  *tls.Config
		tlsName    string
		tlsTimeout float64
	)

	if needsLock {
		remote.RLock()
	}
	tlsRequired := remote.TLS || remote.TLSConfig != nil
	if tlsRequired {
		if remote.TLSConfig != nil {
			tlsConfig = remote.TLSConfig.Clone()
		} else {
			tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		tlsName = remote.tlsName
		tlsTimeout = remote.TLSTimeout
		if tlsTimeout == 0 {
			tlsTimeout = float64(TLS_TIMEOUT / time.Second)
		}
	}
	if needsLock {
		remote.RUnlock()
	}

	return tlsRequired, tlsConfig, tlsName, tlsTimeout
}

// Initiates the LeafNode Websocket connection by:
// - doing the TLS handshake if needed
// - sending the HTTP request
// - waiting for the HTTP response
//
// Since some bufio reader is used to consume the HTTP response, this function
// returns the slice of buffered bytes (if any) so that the readLoop that will
// be started after that consume those first before reading from the socket.
// The boolean
//
// Lock held on entry.
func (c *client) leafNodeSolicitWSConnection(opts *Options, rURL *url.URL, remote *leafNodeCfg) ([]byte, ClosedState, error) {
	remote.RLock()
	compress := remote.Websocket.Compression
	// By default the server will mask outbound frames, but it can be disabled with this option.
	noMasking := remote.Websocket.NoMasking
	tlsRequired, tlsConfig, tlsName, tlsTimeout := c.leafNodeGetTLSConfigForSolicit(remote, false)
	remote.RUnlock()
	// Do TLS here as needed.
	if tlsRequired {
		// Perform the client-side TLS handshake.
		if resetTLSName, err := c.doTLSClientHandshake("leafnode", rURL, tlsConfig, tlsName, tlsTimeout); err != nil {
			// Check if we need to reset the remote's TLS name.
			if resetTLSName {
				remote.Lock()
				remote.tlsName = _EMPTY_
				remote.Unlock()
			}
			// 0 will indicate that the connection was already closed
			return nil, 0, err
		}
	}

	// For http request, we need the passed URL to contain either http or https scheme.
	scheme := "http"
	if tlsRequired {
		scheme = "https"
	}
	// We will use the `/leafnode` path to tell the accepting WS server that it should
	// create a LEAF connection, not a CLIENT.
	// In case we use the user's URL path in the future, make sure we append the user's
	// path to our `/leafnode` path.
	path := leafNodeWSPath
	if curPath := rURL.EscapedPath(); curPath != _EMPTY_ {
		if curPath[0] == '/' {
			curPath = curPath[1:]
		}
		path += curPath
	}
	ustr := fmt.Sprintf("%s://%s%s", scheme, rURL.Host, path)
	u, _ := url.Parse(ustr)
	req := &http.Request{
		Method:     "GET",
		URL:        u,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       u.Host,
	}
	wsKey, err := wsMakeChallengeKey()
	if err != nil {
		return nil, WriteError, err
	}

	req.Header["Upgrade"] = []string{"websocket"}
	req.Header["Connection"] = []string{"Upgrade"}
	req.Header["Sec-WebSocket-Key"] = []string{wsKey}
	req.Header["Sec-WebSocket-Version"] = []string{"13"}
	if compress {
		req.Header.Add("Sec-WebSocket-Extensions", wsPMCReqHeaderValue)
	}
	if noMasking {
		req.Header.Add(wsNoMaskingHeader, wsNoMaskingValue)
	}
	if err := req.Write(c.nc); err != nil {
		return nil, WriteError, err
	}

	var resp *http.Response

	br := bufio.NewReaderSize(c.nc, MAX_CONTROL_LINE_SIZE)
	c.nc.SetReadDeadline(time.Now().Add(DEFAULT_LEAFNODE_INFO_WAIT))
	resp, err = http.ReadResponse(br, req)
	if err == nil &&
		(resp.StatusCode != 101 ||
			!strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") ||
			!strings.EqualFold(resp.Header.Get("Connection"), "upgrade") ||
			resp.Header.Get("Sec-Websocket-Accept") != wsAcceptKey(wsKey)) {

		err = fmt.Errorf("invalid websocket connection")
	}
	// Check compression extension...
	if err == nil && c.ws.compress {
		// Check that not only permessage-deflate extension is present, but that
		// we also have server and client no context take over.
		srvCompress, noCtxTakeover := wsPMCExtensionSupport(resp.Header, false)

		// If server does not support compression, then simply disable it in our side.
		if !srvCompress {
			c.ws.compress = false
		} else if !noCtxTakeover {
			err = fmt.Errorf("compression negotiation error")
		}
	}
	// Same for no masking...
	if err == nil && noMasking {
		// Check if server accepts no masking
		if resp.Header.Get(wsNoMaskingHeader) != wsNoMaskingValue {
			// Nope, need to mask our writes as any client would do.
			c.ws.maskwrite = true
		}
	}
	if resp != nil {
		resp.Body.Close()
	}
	if err != nil {
		return nil, ReadError, err
	}
	c.Debugf("Leafnode compression=%v masking=%v", c.ws.compress, c.ws.maskwrite)

	var preBuf []byte
	// We have to slurp whatever is in the bufio reader and pass that to the readloop.
	if n := br.Buffered(); n != 0 {
		preBuf, _ = br.Peek(n)
	}
	return preBuf, 0, nil
}

const connectProcessTimeout = 2 * time.Second

// This is invoked for remote LEAF remote connections after processing the INFO
// protocol. This will do the TLS handshake (if needed be)
func (s *Server) leafNodeResumeConnectProcess(c *client) {
	clusterName := s.ClusterName()

	c.mu.Lock()
	if c.isClosed() {
		c.mu.Unlock()
		return
	}
	remote := c.leaf.remote

	var tlsRequired bool

	// In case of websocket, the TLS handshake has been already done.
	// So check only for non websocket connections.
	if !c.isWebsocket() {
		var tlsConfig *tls.Config
		var tlsName string
		var tlsTimeout float64

		// Check if TLS is required and gather TLS config variables.
		tlsRequired, tlsConfig, tlsName, tlsTimeout = c.leafNodeGetTLSConfigForSolicit(remote, true)

		// If TLS required, peform handshake.
		if tlsRequired {
			// Get the URL that was used to connect to the remote server.
			rURL := remote.getCurrentURL()

			// Perform the client-side TLS handshake.
			if resetTLSName, err := c.doTLSClientHandshake("leafnode", rURL, tlsConfig, tlsName, tlsTimeout); err != nil {
				// Check if we need to reset the remote's TLS name.
				if resetTLSName {
					remote.Lock()
					remote.tlsName = _EMPTY_
					remote.Unlock()
				}
				c.mu.Unlock()
				return
			}
		}
	}
	if err := c.sendLeafConnect(clusterName, tlsRequired, c.headers); err != nil {
		c.mu.Unlock()
		c.closeConnection(WriteError)
		return
	}

	// Spin up the write loop.
	s.startGoRoutine(func() { c.writeLoop() })

	// timeout leafNodeFinishConnectProcess
	c.ping.tmr = time.AfterFunc(connectProcessTimeout, func() {
		c.mu.Lock()
		// check if leafNodeFinishConnectProcess was called and prevent later leafNodeFinishConnectProcess
		if !c.flags.setIfNotSet(connectProcessFinished) {
			c.mu.Unlock()
			return
		}
		clearTimer(&c.ping.tmr)
		closed := c.isClosed()
		c.mu.Unlock()
		if !closed {
			c.sendErrAndDebug("Stale Leaf Node Connection - Closing")
			c.closeConnection(StaleConnection)
		}
	})
	c.mu.Unlock()
	c.Debugf("Remote leafnode connect msg sent")
}

// This is invoked for remote LEAF remote connections after processing the INFO
// protocol and leafNodeResumeConnectProcess.
// This will send LS+ the CONNECT protocol and register the leaf node.
func (s *Server) leafNodeFinishConnectProcess(c *client) {
	c.mu.Lock()
	if !c.flags.setIfNotSet(connectProcessFinished) {
		c.mu.Unlock()
		return
	}
	if c.isClosed() {
		c.mu.Unlock()
		s.removeLeafNodeConnection(c)
		return
	}
	remote := c.leaf.remote
	// Check if we will need to send the system connect event.
	remote.RLock()
	sendSysConnectEvent := remote.Hub
	remote.RUnlock()

	// Capture account before releasing lock
	acc := c.acc
	// cancel connectProcessTimeout
	clearTimer(&c.ping.tmr)
	c.mu.Unlock()

	// Make sure we register with the account here.
	c.registerWithAccount(acc)
	s.addLeafNodeConnection(c, _EMPTY_, false)
	s.initLeafNodeSmapAndSendSubs(c)
	if sendSysConnectEvent {
		s.sendLeafNodeConnect(acc)
	}

	// The above functions are not atomically under the client
	// lock doing those operations. It is possible - since we
	// have started the read/write loops - that the connection
	// is closed before or in between. This would leave the
	// closed LN connection possible registered with the account
	// and/or the server's leafs map. So check if connection
	// is closed, and if so, manually cleanup.
	c.mu.Lock()
	closed := c.isClosed()
	if !closed {
		s.setFirstPingTimer(c)
	}
	c.mu.Unlock()
	if closed {
		s.removeLeafNodeConnection(c)
		if prev := acc.removeClient(c); prev == 1 {
			s.decActiveAccounts()
		}
	}
}
