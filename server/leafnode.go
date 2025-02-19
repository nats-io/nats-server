// Copyright 2019-2025 The NATS Authors
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
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

const (
	// Warning when user configures leafnode TLS insecure
	leafnodeTLSInsecureWarning = "TLS certificate chain and hostname of solicited leafnodes will not be verified. DO NOT USE IN PRODUCTION!"

	// When a loop is detected, delay the reconnect of solicited connection.
	leafNodeReconnectDelayAfterLoopDetected = 30 * time.Second

	// When a server receives a message causing a permission violation, the
	// connection is closed and it won't attempt to reconnect for that long.
	leafNodeReconnectAfterPermViolation = 30 * time.Second

	// When we have the same cluster name as the hub.
	leafNodeReconnectDelayAfterClusterNameSame = 30 * time.Second

	// Prefix for loop detection subject
	leafNodeLoopDetectionSubjectPrefix = "$LDS."

	// Path added to URL to indicate to WS server that the connection is a
	// LEAF connection as opposed to a CLIENT.
	leafNodeWSPath = "/leafnode"

	// This is the time the server will wait, when receiving a CONNECT,
	// before closing the connection if the required minimum version is not met.
	leafNodeWaitBeforeClose = 5 * time.Second
)

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
	// domain name of remote server
	remoteDomain string
	// account name of remote server
	remoteAccName string
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
	// Selected compression mode, which may be different from the server configured mode.
	compression string
	// This is for GW map replies.
	gwSub *subscription
}

// Used for remote (solicited) leafnodes.
type leafNodeCfg struct {
	sync.RWMutex
	*RemoteLeafOpts
	urls           []*url.URL
	curURL         *url.URL
	tlsName        string
	username       string
	password       string
	perms          *Permissions
	connDelay      time.Duration // Delay before a connect, could be used while detecting loop condition, etc..
	jsMigrateTimer *time.Timer
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

// This will spin up go routines to solicit the remote leaf node connections.
func (s *Server) solicitLeafNodeRemotes(remotes []*RemoteLeafOpts) {
	sysAccName := _EMPTY_
	sAcc := s.SystemAccount()
	if sAcc != nil {
		sysAccName = sAcc.Name
	}
	addRemote := func(r *RemoteLeafOpts, isSysAccRemote bool) *leafNodeCfg {
		s.mu.Lock()
		remote := newLeafNodeCfg(r)
		creds := remote.Credentials
		accName := remote.LocalAccount
		s.leafRemoteCfgs = append(s.leafRemoteCfgs, remote)
		// Print notice if
		if isSysAccRemote {
			if len(remote.DenyExports) > 0 {
				s.Noticef("Remote for System Account uses restricted export permissions")
			}
			if len(remote.DenyImports) > 0 {
				s.Noticef("Remote for System Account uses restricted import permissions")
			}
		}
		s.mu.Unlock()
		if creds != _EMPTY_ {
			contents, err := os.ReadFile(creds)
			defer wipeSlice(contents)
			if err != nil {
				s.Errorf("Error reading LeafNode Remote Credentials file %q: %v", creds, err)
			} else if items := credsRe.FindAllSubmatch(contents, -1); len(items) < 2 {
				s.Errorf("LeafNode Remote Credentials file %q malformed", creds)
			} else if _, err := nkeys.FromSeed(items[1][1]); err != nil {
				s.Errorf("LeafNode Remote Credentials file %q has malformed seed", creds)
			} else if uc, err := jwt.DecodeUserClaims(string(items[0][1])); err != nil {
				s.Errorf("LeafNode Remote Credentials file %q has malformed user jwt", creds)
			} else if isSysAccRemote {
				if !uc.Permissions.Pub.Empty() || !uc.Permissions.Sub.Empty() || uc.Permissions.Resp != nil {
					s.Noticef("LeafNode Remote for System Account uses credentials file %q with restricted permissions", creds)
				}
			} else {
				if !uc.Permissions.Pub.Empty() || !uc.Permissions.Sub.Empty() || uc.Permissions.Resp != nil {
					s.Noticef("LeafNode Remote for Account %s uses credentials file %q with restricted permissions", accName, creds)
				}
			}
		}
		return remote
	}
	for _, r := range remotes {
		remote := addRemote(r, r.LocalAccount == sysAccName)
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

	// Users can bind to any local account, if its empty we will assume the $G account.
	for _, r := range o.LeafNode.Remotes {
		if r.LocalAccount == _EMPTY_ {
			r.LocalAccount = globalAccountName
		}
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
			return fmt.Errorf("operator mode does not allow specifying users in leafnode config")
		}
		for _, r := range o.LeafNode.Remotes {
			if !nkeys.IsValidPublicAccountKey(r.LocalAccount) {
				return fmt.Errorf(
					"operator mode requires account nkeys in remotes. " +
						"Please add an `account` key to each remote in your `leafnodes` section, to assign it to an account. " +
						"Each account value should be a 56 character public key, starting with the letter 'A'")
			}
		}
		if o.LeafNode.Port != 0 && o.LeafNode.Account != "" && !nkeys.IsValidPublicAccountKey(o.LeafNode.Account) {
			return fmt.Errorf("operator mode and non account nkeys are incompatible")
		}
	}

	// Validate compression settings
	if o.LeafNode.Compression.Mode != _EMPTY_ {
		if err := validateAndNormalizeCompressionOption(&o.LeafNode.Compression, CompressionS2Auto); err != nil {
			return err
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
				return fmt.Errorf("remote leaf node configuration cannot have a mix of websocket and non-websocket urls: %q", redactURLList(rcfg.URLs))
			}
		}
		// Validate compression settings
		if rcfg.Compression.Mode != _EMPTY_ {
			if err := validateAndNormalizeCompressionOption(&rcfg.Compression, CompressionS2Auto); err != nil {
				return err
			}
		}
	}

	if o.LeafNode.Port == 0 {
		return nil
	}

	// If MinVersion is defined, check that it is valid.
	if mv := o.LeafNode.MinVersion; mv != _EMPTY_ {
		if err := checkLeafMinVersionConfig(mv); err != nil {
			return err
		}
	}

	// The checks below will be done only when detecting that we are configured
	// with gateways. So if an option validation needs to be done regardless,
	// it MUST be done before this point!

	if o.Gateway.Name == _EMPTY_ && o.Gateway.Port == 0 {
		return nil
	}
	// If we are here we have both leaf nodes and gateways defined, make sure there
	// is a system account defined.
	if o.SystemAccount == _EMPTY_ {
		return fmt.Errorf("leaf nodes and gateways (both being defined) require a system account to also be configured")
	}
	if err := validatePinnedCerts(o.LeafNode.TLSPinnedCerts); err != nil {
		return fmt.Errorf("leafnode: %v", err)
	}
	return nil
}

func checkLeafMinVersionConfig(mv string) error {
	if ok, err := versionAtLeastCheckError(mv, 2, 8, 0); !ok || err != nil {
		if err != nil {
			return fmt.Errorf("invalid leafnode's minimum version: %v", err)
		} else {
			return fmt.Errorf("the minimum version should be at least 2.8.0")
		}
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
	if o.LeafNode.Nkey != _EMPTY_ {
		return fmt.Errorf("can not have a single nkey and a users array")
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

	s.mu.RLock()
	defer s.mu.RUnlock()

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
			cfg.TLSHandshakeFirst = ro.TLSHandshakeFirst
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
	// If allowed to randomize, do it on our copy of URLs
	if !remote.NoRandomize {
		rand.Shuffle(len(cfg.urls), func(i, j int) {
			cfg.urls[i], cfg.urls[j] = cfg.urls[j], cfg.urls[i]
		})
	}
	// If we are TLS make sure we save off a proper servername if possible.
	// Do same for user/password since we may need them to connect to
	// a bare URL that we get from INFO protocol.
	for _, u := range cfg.urls {
		cfg.saveTLSHostname(u)
		cfg.saveUserPassword(u)
		// If the url(s) have the "wss://" scheme, and we don't have a TLS
		// config, mark that we should be using TLS anyway.
		if !cfg.TLS && isWSSURL(u) {
			cfg.TLS = true
		}
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

const sharedSysAccDelay = 250 * time.Millisecond

func (s *Server) connectToRemoteLeafNode(remote *leafNodeCfg, firstConnect bool) {
	defer s.grWG.Done()

	if remote == nil || len(remote.URLs) == 0 {
		s.Debugf("Empty remote leafnode definition, nothing to connect")
		return
	}

	opts := s.getOpts()
	reconnectDelay := opts.LeafNode.ReconnectInterval
	s.mu.RLock()
	dialTimeout := s.leafNodeOpts.dialTimeout
	resolver := s.leafNodeOpts.resolver
	var isSysAcc bool
	if s.eventsEnabled() {
		isSysAcc = remote.LocalAccount == s.sys.account.Name
	}
	jetstreamMigrateDelay := remote.JetStreamClusterMigrateDelay
	s.mu.RUnlock()

	// If we are sharing a system account and we are not standalone delay to gather some info prior.
	if firstConnect && isSysAcc && !s.standAloneMode() {
		s.Debugf("Will delay first leafnode connect to shared system account due to clustering")
		remote.setConnectDelay(sharedSysAccDelay)
	}

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
			// Some test may want to disable remotes from connecting
			if s.isLeafConnectDisabled() {
				s.Debugf("Will not attempt to connect to remote server on %q%s, leafnodes currently disabled", rURL.Host, ipStr)
				err = ErrLeafNodeDisabled
			} else {
				s.Debugf("Trying to connect as leafnode to remote server on %q%s", rURL.Host, ipStr)
				conn, err = natsDialTimeout("tcp", url, dialTimeout)
			}
		}
		if err != nil {
			jitter := time.Duration(rand.Int63n(int64(reconnectDelay)))
			delay := reconnectDelay + jitter
			attempts++
			if s.shouldReportConnectErr(firstConnect, attempts) {
				s.Errorf(connErrFmt, rURL.Host, attempts, err)
			} else {
				s.Debugf(connErrFmt, rURL.Host, attempts, err)
			}
			remote.Lock()
			// if we are using a delay to start migrating assets, kick off a migrate timer.
			if remote.jsMigrateTimer == nil && jetstreamMigrateDelay > 0 {
				remote.jsMigrateTimer = time.AfterFunc(jetstreamMigrateDelay, func() {
					s.checkJetStreamMigrate(remote)
				})
			}
			remote.Unlock()
			select {
			case <-s.quitCh:
				remote.cancelMigrateTimer()
				return
			case <-time.After(delay):
				// Check if we should migrate any JetStream assets immediately while this remote is down.
				// This will be used if JetStreamClusterMigrateDelay was not set
				if jetstreamMigrateDelay == 0 {
					s.checkJetStreamMigrate(remote)
				}
				continue
			}
		}
		remote.cancelMigrateTimer()
		if !s.remoteLeafNodeStillValid(remote) {
			conn.Close()
			return
		}

		// We have a connection here to a remote server.
		// Go ahead and create our leaf node and return.
		s.createLeafNode(conn, rURL, remote, nil)

		// Clear any observer states if we had them.
		s.clearObserverState(remote)

		return
	}
}

func (cfg *leafNodeCfg) cancelMigrateTimer() {
	cfg.Lock()
	stopAndClearTimer(&cfg.jsMigrateTimer)
	cfg.Unlock()
}

// This will clear any observer state such that stream or consumer assets on this server can become leaders again.
func (s *Server) clearObserverState(remote *leafNodeCfg) {
	s.mu.RLock()
	accName := remote.LocalAccount
	s.mu.RUnlock()

	acc, err := s.LookupAccount(accName)
	if err != nil {
		s.Warnf("Error looking up account [%s] checking for JetStream clear observer state on a leafnode", accName)
		return
	}

	acc.jscmMu.Lock()
	defer acc.jscmMu.Unlock()

	// Walk all streams looking for any clustered stream, skip otherwise.
	for _, mset := range acc.streams() {
		node := mset.raftNode()
		if node == nil {
			// Not R>1
			continue
		}
		// Check consumers
		for _, o := range mset.getConsumers() {
			if n := o.raftNode(); n != nil {
				// Ensure we can become a leader again.
				n.SetObserver(false)
			}
		}
		// Ensure we can not become a leader again.
		node.SetObserver(false)
	}
}

// Check to see if we should migrate any assets from this account.
func (s *Server) checkJetStreamMigrate(remote *leafNodeCfg) {
	s.mu.RLock()
	accName, shouldMigrate := remote.LocalAccount, remote.JetStreamClusterMigrate
	s.mu.RUnlock()

	if !shouldMigrate {
		return
	}

	acc, err := s.LookupAccount(accName)
	if err != nil {
		s.Warnf("Error looking up account [%s] checking for JetStream migration on a leafnode", accName)
		return
	}

	acc.jscmMu.Lock()
	defer acc.jscmMu.Unlock()

	// Walk all streams looking for any clustered stream, skip otherwise.
	// If we are the leader force stepdown.
	for _, mset := range acc.streams() {
		node := mset.raftNode()
		if node == nil {
			// Not R>1
			continue
		}
		// Collect any consumers
		for _, o := range mset.getConsumers() {
			if n := o.raftNode(); n != nil {
				n.StepDown()
				// Ensure we can not become a leader while in this state.
				n.SetObserver(true)
			}
		}
		// Stepdown if this stream was leader.
		node.StepDown()
		// Ensure we can not become a leader while in this state.
		node.SetObserver(true)
	}
}

// Helper for checking.
func (s *Server) isLeafConnectDisabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.leafDisableConnect
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

	if s.isShuttingDown() {
		return
	}

	s.mu.Lock()
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
	// Do not set compression in this Info object, it would possibly cause
	// issues when sending asynchronous INFO to the remote.
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
		Domain:        opts.JetStreamDomain,
		Proto:         s.getServerProto(),
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
		s.Fatalf("Error setting leafnode INFO with LeafNode.Advertise value of %s, err=%v", opts.LeafNode.Advertise, err)
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
var credsRe = regexp.MustCompile(`\s*(?:(?:[-]{3,}.*[-]{3,}\r?\n)([\w\-.=]+)(?:\r?\n[-]{3,}.*[-]{3,}(\r?\n|\z)))`)

// clusterName is provided as argument to avoid lock ordering issues with the locked client c
// Lock should be held entering here.
func (c *client) sendLeafConnect(clusterName string, headers bool) error {
	// We support basic user/pass and operator based user JWT with signatures.
	cinfo := leafConnectInfo{
		Version:       VERSION,
		ID:            c.srv.info.ID,
		Domain:        c.srv.info.Domain,
		Name:          c.srv.info.Name,
		Hub:           c.leaf.remote.Hub,
		Cluster:       clusterName,
		Headers:       headers,
		JetStream:     c.acc.jetStreamConfigured(),
		DenyPub:       c.leaf.remote.DenyImports,
		Compression:   c.leaf.compression,
		RemoteAccount: c.acc.GetName(),
		Proto:         c.srv.getServerProto(),
	}

	// If a signature callback is specified, this takes precedence over anything else.
	if cb := c.leaf.remote.SignatureCB; cb != nil {
		nonce := c.nonce
		c.mu.Unlock()
		jwt, sigraw, err := cb(nonce)
		c.mu.Lock()
		if err == nil && c.isClosed() {
			err = ErrConnectionClosed
		}
		if err != nil {
			c.Errorf("Error signing the nonce: %v", err)
			return err
		}
		sig := base64.RawURLEncoding.EncodeToString(sigraw)
		cinfo.JWT, cinfo.Sig = jwt, sig

	} else if creds := c.leaf.remote.Credentials; creds != _EMPTY_ {
		// Check for credentials first, that will take precedence..
		c.Debugf("Authenticating with credentials file %q", c.leaf.remote.Credentials)
		contents, err := os.ReadFile(creds)
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
		cinfo.JWT = bytesToString(tmp)
		cinfo.Sig = sig
	} else if nkey := c.leaf.remote.Nkey; nkey != _EMPTY_ {
		kp, err := nkeys.FromSeed([]byte(nkey))
		if err != nil {
			c.Errorf("Remote nkey has malformed seed")
			return err
		}
		// Wipe our key on exit.
		defer kp.Wipe()
		sigraw, _ := kp.Sign(c.nonce)
		sig := base64.RawURLEncoding.EncodeToString(sigraw)
		pkey, _ := kp.PublicKey()
		cinfo.Nkey = pkey
		cinfo.Sig = sig
	}
	// In addition, and this is to allow auth callout, set user/password or
	// token if applicable.
	if userInfo := c.leaf.remote.curURL.User; userInfo != nil {
		// For backward compatibility, if only username is provided, set both
		// Token and User, not just Token.
		cinfo.User = userInfo.Username()
		var ok bool
		cinfo.Pass, ok = userInfo.Password()
		if !ok {
			cinfo.Token = cinfo.User
		}
	} else if c.leaf.remote.username != _EMPTY_ {
		cinfo.User = c.leaf.remote.username
		cinfo.Pass = c.leaf.remote.password
	}
	b, err := json.Marshal(cinfo)
	if err != nil {
		c.Errorf("Error marshaling CONNECT to remote leafnode: %v\n", err)
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
	if s.isShuttingDown() {
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
	s.leafNodeInfo.Cluster = s.cachedClusterName()
	s.leafNodeInfo.LeafNodeURLs = s.leafURLsMap.getAsStringSlice()
	s.leafNodeInfo.WSConnectURLs = s.websocket.connectURLsMap.getAsStringSlice()
	s.leafNodeInfoJSON = generateInfoJSON(&s.leafNodeInfo)
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
	var remoteSuffix string
	if remote != nil {
		// For now, if lookup fails, we will constantly try
		// to recreate this LN connection.
		lacc := remote.LocalAccount
		var err error
		acc, err = s.LookupAccount(lacc)
		if err != nil {
			// An account not existing is something that can happen with nats/http account resolver and the account
			// has not yet been pushed, or the request failed for other reasons.
			// remote needs to be set or retry won't happen
			c.leaf.remote = remote
			c.closeConnection(MissingAccount)
			s.Errorf("Unable to lookup account %s for solicited leafnode connection: %v", lacc, err)
			return nil
		}
		remoteSuffix = fmt.Sprintf(" for account: %s", acc.traceLabel())
	}

	c.mu.Lock()
	c.initClient()
	c.Noticef("Leafnode connection created%s %s", remoteSuffix, c.opts.Name)

	var (
		tlsFirst         bool
		tlsFirstFallback time.Duration
		infoTimeout      time.Duration
	)
	if remote != nil {
		solicited = true
		remote.Lock()
		c.leaf.remote = remote
		c.setPermissions(remote.perms)
		if !c.leaf.remote.Hub {
			c.leaf.isSpoke = true
		}
		tlsFirst = remote.TLSHandshakeFirst
		infoTimeout = remote.FirstInfoTimeout
		remote.Unlock()
		c.acc = acc
	} else {
		c.flags.set(expectConnect)
		if ws != nil {
			c.Debugf("Leafnode compression=%v", c.ws.compress)
		}
		tlsFirst = opts.LeafNode.TLSHandshakeFirst
		if f := opts.LeafNode.TLSHandshakeFirstFallback; f > 0 {
			tlsFirstFallback = f
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
		// For tests that want to simulate old servers, do not set the compression
		// on the INFO protocol if configured with CompressionNotSupported.
		if cm := opts.LeafNode.Compression.Mode; cm != CompressionNotSupported {
			info.Compression = cm
		}
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
			// If configured to do TLS handshake first
			if tlsFirst {
				if _, err := c.leafClientHandshakeIfNeeded(remote, opts); err != nil {
					c.mu.Unlock()
					return nil
				}
			}
			// We need to wait for the info, but not for too long.
			c.nc.SetReadDeadline(time.Now().Add(infoTimeout))
		}

		// We will process the INFO from the readloop and finish by
		// sending the CONNECT and finish registration later.
	} else {
		// Send our info to the other side.
		// Remember the nonce we sent here for signatures, etc.
		c.nonce = make([]byte, nonceLen)
		copy(c.nonce, nonce[:])
		info.Nonce = bytesToString(c.nonce)
		info.CID = c.cid
		proto := generateInfoJSON(info)

		var pre []byte
		// We need first to check for "TLS First" fallback delay.
		if tlsFirstFallback > 0 {
			// We wait and see if we are getting any data. Since we did not send
			// the INFO protocol yet, only clients that use TLS first should be
			// sending data (the TLS handshake). We don't really check the content:
			// if it is a rogue agent and not an actual client performing the
			// TLS handshake, the error will be detected when performing the
			// handshake on our side.
			pre = make([]byte, 4)
			c.nc.SetReadDeadline(time.Now().Add(tlsFirstFallback))
			n, _ := io.ReadFull(c.nc, pre[:])
			c.nc.SetReadDeadline(time.Time{})
			// If we get any data (regardless of possible timeout), we will proceed
			// with the TLS handshake.
			if n > 0 {
				pre = pre[:n]
			} else {
				// We did not get anything so we will send the INFO protocol.
				pre = nil
				// Set the boolean to false for the rest of the function.
				tlsFirst = false
			}
		}

		if !tlsFirst {
			// We have to send from this go routine because we may
			// have to block for TLS handshake before we start our
			// writeLoop go routine. The other side needs to receive
			// this before it can initiate the TLS handshake..
			c.sendProtoNow(proto)

			// The above call could have marked the connection as closed (due to TCP error).
			if c.isClosed() {
				c.mu.Unlock()
				c.closeConnection(WriteError)
				return nil
			}
		}

		// Check to see if we need to spin up TLS.
		if !c.isWebsocket() && info.TLSRequired {
			// If we have a prebuffer create a multi-reader.
			if len(pre) > 0 {
				c.nc = &tlsMixConn{c.nc, bytes.NewBuffer(pre)}
			}
			// Perform server-side TLS handshake.
			if err := c.doTLSServerHandshake(tlsHandshakeLeaf, opts.LeafNode.TLSConfig, opts.LeafNode.TLSTimeout, opts.LeafNode.TLSPinnedCerts); err != nil {
				c.mu.Unlock()
				return nil
			}
		}

		// If the user wants the TLS handshake to occur first, now that it is
		// done, send the INFO protocol.
		if tlsFirst {
			c.flags.set(didTLSFirst)
			c.sendProtoNow(proto)
			if c.isClosed() {
				c.mu.Unlock()
				c.closeConnection(WriteError)
				return nil
			}
		}

		// Leaf nodes will always require a CONNECT to let us know
		// when we are properly bound to an account.
		//
		// If compression is configured, we can't set the authTimer here because
		// it would cause the parser to fail any incoming protocol that is not a
		// CONNECT (and we need to exchange INFO protocols for compression
		// negotiation). So instead, use the ping timer until we are done with
		// negotiation and can set the auth timer.
		timeout := secondsToDuration(opts.LeafNode.AuthTimeout)
		if needsCompression(opts.LeafNode.Compression.Mode) {
			c.ping.tmr = time.AfterFunc(timeout, func() {
				c.authTimeout()
			})
		} else {
			c.setAuthTimer(timeout)
		}
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

	// We will spin the write loop for solicited connections only
	// when processing the INFO and after switching to TLS if needed.
	if !solicited {
		s.startGoRoutine(func() { c.writeLoop() })
	}

	c.mu.Unlock()

	return c
}

// Will perform the client-side TLS handshake if needed. Assumes that this
// is called by the solicit side (remote will be non nil). Returns `true`
// if TLS is required, `false` otherwise.
// Lock held on entry.
func (c *client) leafClientHandshakeIfNeeded(remote *leafNodeCfg, opts *Options) (bool, error) {
	// Check if TLS is required and gather TLS config variables.
	tlsRequired, tlsConfig, tlsName, tlsTimeout := c.leafNodeGetTLSConfigForSolicit(remote)
	if !tlsRequired {
		return false, nil
	}

	// If TLS required, peform handshake.
	// Get the URL that was used to connect to the remote server.
	rURL := remote.getCurrentURL()

	// Perform the client-side TLS handshake.
	if resetTLSName, err := c.doTLSClientHandshake(tlsHandshakeLeaf, rURL, tlsConfig, tlsName, tlsTimeout, opts.LeafNode.TLSPinnedCerts); err != nil {
		// Check if we need to reset the remote's TLS name.
		if resetTLSName {
			remote.Lock()
			remote.tlsName = _EMPTY_
			remote.Unlock()
		}
		return false, err
	}
	return true, nil
}

func (c *client) processLeafnodeInfo(info *Info) {
	c.mu.Lock()
	if c.leaf == nil || c.isClosed() {
		c.mu.Unlock()
		return
	}
	s := c.srv
	opts := s.getOpts()
	remote := c.leaf.remote
	didSolicit := remote != nil
	firstINFO := !c.flags.isSet(infoReceived)

	// In case of websocket, the TLS handshake has been already done.
	// So check only for non websocket connections and for configurations
	// where the TLS Handshake was not done first.
	if didSolicit && !c.flags.isSet(handshakeComplete) && !c.isWebsocket() && !remote.TLSHandshakeFirst {
		// If the server requires TLS, we need to set this in the remote
		// otherwise if there is no TLS configuration block for the remote,
		// the solicit side will not attempt to perform the TLS handshake.
		if firstINFO && info.TLSRequired {
			remote.TLS = true
		}
		if _, err := c.leafClientHandshakeIfNeeded(remote, opts); err != nil {
			c.mu.Unlock()
			return
		}
	}

	// Check for compression, unless already done.
	if firstINFO && !c.flags.isSet(compressionNegotiated) {
		// Prevent from getting back here.
		c.flags.set(compressionNegotiated)

		var co *CompressionOpts
		if !didSolicit {
			co = &opts.LeafNode.Compression
		} else {
			co = &remote.Compression
		}
		if needsCompression(co.Mode) {
			// Release client lock since following function will need server lock.
			c.mu.Unlock()
			compress, err := s.negotiateLeafCompression(c, didSolicit, info.Compression, co)
			if err != nil {
				c.sendErrAndErr(err.Error())
				c.closeConnection(ProtocolViolation)
				return
			}
			if compress {
				// Done for now, will get back another INFO protocol...
				return
			}
			// No compression because one side does not want/can't, so proceed.
			c.mu.Lock()
			// Check that the connection did not close if the lock was released.
			if c.isClosed() {
				c.mu.Unlock()
				return
			}
		} else {
			// Coming from an old server, the Compression field would be the empty
			// string. For servers that are configured with CompressionNotSupported,
			// this makes them behave as old servers.
			if info.Compression == _EMPTY_ || co.Mode == CompressionNotSupported {
				c.leaf.compression = CompressionNotSupported
			} else {
				c.leaf.compression = CompressionOff
			}
		}
		// Accepting side does not normally process an INFO protocol during
		// initial connection handshake. So we keep it consistent by returning
		// if we are not soliciting.
		if !didSolicit {
			// If we had created the ping timer instead of the auth timer, we will
			// clear the ping timer and set the auth timer now that the compression
			// negotiation is done.
			if info.Compression != _EMPTY_ && c.ping.tmr != nil {
				clearTimer(&c.ping.tmr)
				c.setAuthTimer(secondsToDuration(opts.LeafNode.AuthTimeout))
			}
			c.mu.Unlock()
			return
		}
		// Fall through and process the INFO protocol as usual.
	}

	// Note: For now, only the initial INFO has a nonce. We
	// will probably do auto key rotation at some point.
	if firstINFO {
		// Mark that the INFO protocol has been received.
		c.flags.set(infoReceived)
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
		if didSolicit && (info.CID == 0 || info.LeafNodeURLs == nil) {
			c.mu.Unlock()
			c.Errorf(ErrConnectedToWrongPort.Error())
			c.closeConnection(WrongPort)
			return
		}
		// Reject a cluster that contains spaces.
		if info.Cluster != _EMPTY_ && strings.Contains(info.Cluster, " ") {
			c.mu.Unlock()
			c.sendErrAndErr(ErrClusterNameHasSpaces.Error())
			c.closeConnection(ProtocolViolation)
			return
		}
		// Capture a nonce here.
		c.nonce = []byte(info.Nonce)
		if info.TLSRequired && didSolicit {
			remote.TLS = true
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
		c.leaf.remoteDomain = info.Domain
		c.leaf.remoteCluster = info.Cluster
		// We send the protocol version in the INFO protocol.
		// Keep track of it, so we know if this connection supports message
		// tracing for instance.
		c.opts.Protocol = info.Proto
	}

	// For both initial INFO and async INFO protocols, Possibly
	// update our list of remote leafnode URLs we can connect to.
	if didSolicit && (len(info.LeafNodeURLs) > 0 || len(info.WSConnectURLs) > 0) {
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
				if perms.Publish == nil {
					perms.Publish = &SubjectPermission{}
				}
				perms.Publish.Deny = append(perms.Publish.Deny, remote.DenyExports...)
			}
			if len(remote.DenyImports) > 0 {
				if perms.Subscribe == nil {
					perms.Subscribe = &SubjectPermission{}
				}
				perms.Subscribe.Deny = append(perms.Subscribe.Deny, remote.DenyImports...)
			}
		}
		c.setPermissions(perms)
	}

	var resumeConnect bool

	// If this is a remote connection and this is the first INFO protocol,
	// then we need to finish the connect process by sending CONNECT, etc..
	if firstINFO && didSolicit {
		// Clear deadline that was set in createLeafNode while waiting for the INFO.
		c.nc.SetDeadline(time.Time{})
		resumeConnect = true
	} else if !firstINFO && didSolicit {
		c.leaf.remoteAccName = info.RemoteAccount
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

func (s *Server) negotiateLeafCompression(c *client, didSolicit bool, infoCompression string, co *CompressionOpts) (bool, error) {
	// Negotiate the appropriate compression mode (or no compression)
	cm, err := selectCompressionMode(co.Mode, infoCompression)
	if err != nil {
		return false, err
	}
	c.mu.Lock()
	// For "auto" mode, set the initial compression mode based on RTT
	if cm == CompressionS2Auto {
		if c.rttStart.IsZero() {
			c.rtt = computeRTT(c.start)
		}
		cm = selectS2AutoModeBasedOnRTT(c.rtt, co.RTTThresholds)
	}
	// Keep track of the negotiated compression mode.
	c.leaf.compression = cm
	cid := c.cid
	var nonce string
	if !didSolicit {
		nonce = bytesToString(c.nonce)
	}
	c.mu.Unlock()

	if !needsCompression(cm) {
		return false, nil
	}

	// If we end-up doing compression...

	// Generate an INFO with the chosen compression mode.
	s.mu.Lock()
	info := s.copyLeafNodeInfo()
	info.Compression, info.CID, info.Nonce = compressionModeForInfoProtocol(co, cm), cid, nonce
	infoProto := generateInfoJSON(info)
	s.mu.Unlock()

	// If we solicited, then send this INFO protocol BEFORE switching
	// to compression writer. However, if we did not, we send it after.
	c.mu.Lock()
	if didSolicit {
		c.enqueueProto(infoProto)
		// Make sure it is completely flushed (the pending bytes goes to
		// 0) before proceeding.
		for c.out.pb > 0 && !c.isClosed() {
			c.flushOutbound()
		}
	}
	// This is to notify the readLoop that it should switch to a
	// (de)compression reader.
	c.in.flags.set(switchToCompression)
	// Create the compress writer before queueing the INFO protocol for
	// a route that did not solicit. It will make sure that that proto
	// is sent with compression on.
	c.out.cw = s2.NewWriter(nil, s2WriterOptions(cm)...)
	if !didSolicit {
		c.enqueueProto(infoProto)
	}
	c.mu.Unlock()
	return true, nil
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
		// It does not really matter if we use "ws://" or "wss://" here since
		// we will have already marked that the remote should use TLS anyway.
		// But use proper scheme for log statements, etc...
		proto := wsSchemePrefix
		if cfg.TLS {
			proto = wsSchemePrefixTLS
		}
		c.doUpdateLNURLs(cfg, proto, info.WSConnectURLs)
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
			// As per below, the URLs we receive should not have contained URL info, so this should be safe to log.
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
func (s *Server) addLeafNodeConnection(c *client, srvName, clusterName string, checkForDup bool) {
	var accName string
	c.mu.Lock()
	cid := c.cid
	acc := c.acc
	if acc != nil {
		accName = acc.Name
	}
	myRemoteDomain := c.leaf.remoteDomain
	mySrvName := c.leaf.remoteServer
	remoteAccName := c.leaf.remoteAccName
	myClustName := c.leaf.remoteCluster
	solicited := c.leaf.remote != nil
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
			if !ol.isSolicitedLeafNode() && ol.leaf.remoteServer == srvName &&
				ol.leaf.remoteCluster == clusterName && ol.acc.Name == accName &&
				remoteAccName != _EMPTY_ && ol.leaf.remoteAccName == remoteAccName {
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

	srvDecorated := func() string {
		if myClustName == _EMPTY_ {
			return mySrvName
		}
		return fmt.Sprintf("%s/%s", mySrvName, myClustName)
	}

	opts := s.getOpts()
	sysAcc := s.SystemAccount()
	js := s.getJetStream()
	var meta *raft
	if js != nil {
		if mg := js.getMetaGroup(); mg != nil {
			meta = mg.(*raft)
		}
	}
	blockMappingOutgoing := false
	// Deny (non domain) JetStream API traffic unless system account is shared
	// and domain names are identical and extending is not disabled

	// Check if backwards compatibility has been enabled and needs to be acted on
	forceSysAccDeny := false
	if len(opts.JsAccDefaultDomain) > 0 {
		if acc == sysAcc {
			for _, d := range opts.JsAccDefaultDomain {
				if d == _EMPTY_ {
					// Extending JetStream via leaf node is mutually exclusive with a domain mapping to the empty/default domain.
					// As soon as one mapping to "" is found, disable the ability to extend JS via a leaf node.
					c.Noticef("Not extending remote JetStream domain %q due to presence of empty default domain", myRemoteDomain)
					forceSysAccDeny = true
					break
				}
			}
		} else if domain, ok := opts.JsAccDefaultDomain[accName]; ok && domain == _EMPTY_ {
			// for backwards compatibility with old setups that do not have a domain name set
			c.Debugf("Skipping deny %q for account %q due to default domain", jsAllAPI, accName)
			return
		}
	}

	// If the server has JS disabled, it may still be part of a JetStream that could be extended.
	// This is either signaled by js being disabled and a domain set,
	// or in cases where no domain name exists, an extension hint is set.
	// However, this is only relevant in mixed setups.
	//
	// If the system account connects but default domains are present, JetStream can't be extended.
	if opts.JetStreamDomain != myRemoteDomain || (!opts.JetStream && (opts.JetStreamDomain == _EMPTY_ && opts.JetStreamExtHint != jsWillExtend)) ||
		sysAcc == nil || acc == nil || forceSysAccDeny {
		// If domain names mismatch always deny. This applies to system accounts as well as non system accounts.
		// Not having a system account, account or JetStream disabled is considered a mismatch as well.
		if acc != nil && acc == sysAcc {
			c.Noticef("System account connected from %s", srvDecorated())
			c.Noticef("JetStream not extended, domains differ")
			c.mergeDenyPermissionsLocked(both, denyAllJs)
			// When a remote with a system account is present in a server, unless otherwise disabled, the server will be
			// started in observer mode. Now that it is clear that this not used, turn the observer mode off.
			if solicited && meta != nil && meta.IsObserver() {
				meta.setObserver(false, extNotExtended)
				c.Debugf("Turning JetStream metadata controller Observer Mode off")
				// Take note that the domain was not extended to avoid this state from startup.
				writePeerState(js.config.StoreDir, meta.currentPeerState())
				// Meta controller can't be leader yet.
				// Yet it is possible that due to observer mode every server already stopped campaigning.
				// Therefore this server needs to be kicked into campaigning gear explicitly.
				meta.Campaign()
			}
		} else {
			c.Noticef("JetStream using domains: local %q, remote %q", opts.JetStreamDomain, myRemoteDomain)
			c.mergeDenyPermissionsLocked(both, denyAllClientJs)
		}
		blockMappingOutgoing = true
	} else if acc == sysAcc {
		// system account and same domain
		s.sys.client.Noticef("Extending JetStream domain %q as System Account connected from server %s",
			myRemoteDomain, srvDecorated())
		// In an extension use case, pin leadership to server remotes connect to.
		// Therefore, server with a remote that are not already in observer mode, need to be put into it.
		if solicited && meta != nil && !meta.IsObserver() {
			meta.setObserver(true, extExtended)
			c.Debugf("Turning JetStream metadata controller Observer Mode on - System Account Connected")
			// Take note that the domain was not extended to avoid this state next startup.
			writePeerState(js.config.StoreDir, meta.currentPeerState())
			// If this server is the leader already, step down so a new leader can be elected (that is not an observer)
			meta.StepDown()
		}
	} else {
		// This deny is needed in all cases (system account shared or not)
		// If the system account is shared, jsAllAPI traffic will go through the system account.
		// So in order to prevent duplicate delivery (from system and actual account) suppress it on the account.
		// If the system account is NOT shared, jsAllAPI traffic has no business
		c.Debugf("Adding deny %+v for account %q", denyAllClientJs, accName)
		c.mergeDenyPermissionsLocked(both, denyAllClientJs)
	}
	// If we have a specified JetStream domain we will want to add a mapping to
	// allow access cross domain for each non-system account.
	if opts.JetStreamDomain != _EMPTY_ && opts.JetStream && acc != nil && acc != sysAcc {
		for src, dest := range generateJSMappingTable(opts.JetStreamDomain) {
			if err := acc.AddMapping(src, dest); err != nil {
				c.Debugf("Error adding JetStream domain mapping: %s", err.Error())
			} else {
				c.Debugf("Adding JetStream Domain Mapping %q -> %s to account %q", src, dest, accName)
			}
		}
		if blockMappingOutgoing {
			src := fmt.Sprintf(jsDomainAPI, opts.JetStreamDomain)
			// make sure that messages intended for this domain, do not leave the cluster via this leaf node connection
			// This is a guard against a miss-config with two identical domain names and will only cover some forms
			// of this issue, not all of them.
			// This guards against a hub and a spoke having the same domain name.
			// But not two spokes having the same one and the request coming from the hub.
			c.mergeDenyPermissionsLocked(pub, []string{src})
			c.Debugf("Adding deny %q for outgoing messages to account %q", src, accName)
		}
	}
}

func (s *Server) removeLeafNodeConnection(c *client) {
	c.mu.Lock()
	cid := c.cid
	if c.leaf != nil {
		if c.leaf.tsubt != nil {
			c.leaf.tsubt.Stop()
			c.leaf.tsubt = nil
		}
		if c.leaf.gwSub != nil {
			s.gwLeafSubs.Remove(c.leaf.gwSub)
			// We need to set this to nil for GC to release the connection
			c.leaf.gwSub = nil
		}
	}
	c.mu.Unlock()
	s.mu.Lock()
	delete(s.leafs, cid)
	s.mu.Unlock()
	s.removeFromTempClients(cid)
}

// Connect information for solicited leafnodes.
type leafConnectInfo struct {
	Version   string   `json:"version,omitempty"`
	Nkey      string   `json:"nkey,omitempty"`
	JWT       string   `json:"jwt,omitempty"`
	Sig       string   `json:"sig,omitempty"`
	User      string   `json:"user,omitempty"`
	Pass      string   `json:"pass,omitempty"`
	Token     string   `json:"auth_token,omitempty"`
	ID        string   `json:"server_id,omitempty"`
	Domain    string   `json:"domain,omitempty"`
	Name      string   `json:"name,omitempty"`
	Hub       bool     `json:"is_hub,omitempty"`
	Cluster   string   `json:"cluster,omitempty"`
	Headers   bool     `json:"headers,omitempty"`
	JetStream bool     `json:"jetstream,omitempty"`
	DenyPub   []string `json:"deny_pub,omitempty"`

	// There was an existing field called:
	// >> Comp bool `json:"compression,omitempty"`
	// that has never been used. With support for compression, we now need
	// a field that is a string. So we use a different json tag:
	Compression string `json:"compress_mode,omitempty"`

	// Just used to detect wrong connection attempts.
	Gateway string `json:"gateway,omitempty"`

	// Tells the accept side which account the remote is binding to.
	RemoteAccount string `json:"remote_account,omitempty"`

	// The accept side of a LEAF connection, unlike ROUTER and GATEWAY, receives
	// only the CONNECT protocol, and no INFO. So we need to send the protocol
	// version as part of the CONNECT. It will indicate if a connection supports
	// some features, such as message tracing.
	// We use `protocol` as the JSON tag, so this is automatically unmarshal'ed
	// in the low level process CONNECT.
	Proto int `json:"protocol,omitempty"`
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

	// Reject a cluster that contains spaces.
	if proto.Cluster != _EMPTY_ && strings.Contains(proto.Cluster, " ") {
		c.sendErrAndErr(ErrClusterNameHasSpaces.Error())
		c.closeConnection(ProtocolViolation)
		return ErrClusterNameHasSpaces
	}

	// Check for cluster name collisions.
	if cn := s.cachedClusterName(); cn != _EMPTY_ && proto.Cluster != _EMPTY_ && proto.Cluster == cn {
		c.sendErrAndErr(ErrLeafNodeHasSameClusterName.Error())
		c.closeConnection(ClusterNamesIdentical)
		return ErrLeafNodeHasSameClusterName
	}

	// Reject if this has Gateway which means that it would be from a gateway
	// connection that incorrectly connects to the leafnode port.
	if proto.Gateway != _EMPTY_ {
		errTxt := fmt.Sprintf("Rejecting connection from gateway %q on the leafnode port", proto.Gateway)
		c.Errorf(errTxt)
		c.sendErr(errTxt)
		c.closeConnection(WrongGateway)
		return ErrWrongGateway
	}

	if mv := s.getOpts().LeafNode.MinVersion; mv != _EMPTY_ {
		major, minor, update, _ := versionComponents(mv)
		if !versionAtLeast(proto.Version, major, minor, update) {
			// We are going to send back an INFO because otherwise recent
			// versions of the remote server would simply break the connection
			// after 2 seconds if not receiving it. Instead, we want the
			// other side to just "stall" until we finish waiting for the holding
			// period and close the connection below.
			s.sendPermsAndAccountInfo(c)
			c.sendErrAndErr(fmt.Sprintf("connection rejected since minimum version required is %q", mv))
			select {
			case <-c.srv.quitCh:
			case <-time.After(leafNodeWaitBeforeClose):
			}
			c.closeConnection(MinimumVersionRequired)
			return ErrMinimumVersionRequired
		}
	}

	// Check if this server supports headers.
	supportHeaders := c.srv.supportsHeaders()

	c.mu.Lock()
	// Leaf Nodes do not do echo or verbose or pedantic.
	c.opts.Verbose = false
	c.opts.Echo = false
	c.opts.Pedantic = false
	// This inbound connection will be marked as supporting headers if this server
	// support headers and the remote has sent in the CONNECT protocol that it does
	// support headers too.
	c.headers = supportHeaders && proto.Headers
	// If the compression level is still not set, set it based on what has been
	// given to us in the CONNECT protocol.
	if c.leaf.compression == _EMPTY_ {
		// But if proto.Compression is _EMPTY_, set it to CompressionNotSupported
		if proto.Compression == _EMPTY_ {
			c.leaf.compression = CompressionNotSupported
		} else {
			c.leaf.compression = proto.Compression
		}
	}

	// Remember the remote server.
	c.leaf.remoteServer = proto.Name
	// Remember the remote account name
	c.leaf.remoteAccName = proto.RemoteAccount

	// If the other side has declared itself a hub, so we will take on the spoke role.
	if proto.Hub {
		c.leaf.isSpoke = true
	}

	// The soliciting side is part of a cluster.
	if proto.Cluster != _EMPTY_ {
		c.leaf.remoteCluster = proto.Cluster
	}

	c.leaf.remoteDomain = proto.Domain

	// When a leaf solicits a connection to a hub, the perms that it will use on the soliciting leafnode's
	// behalf are correct for them, but inside the hub need to be reversed since data is flowing in the opposite direction.
	if !c.isSolicitedLeafNode() && c.perms != nil {
		sp, pp := c.perms.sub, c.perms.pub
		c.perms.sub, c.perms.pub = pp, sp
		if c.opts.Import != nil {
			c.darray = c.opts.Import.Deny
		} else {
			c.darray = nil
		}
	}

	// Set the Ping timer
	c.setFirstPingTimer()

	// If we received pub deny permissions from the other end, merge with existing ones.
	c.mergeDenyPermissions(pub, proto.DenyPub)

	c.mu.Unlock()

	// Register the cluster, even if empty, as long as we are acting as a hub.
	if !proto.Hub {
		c.acc.registerLeafNodeCluster(proto.Cluster)
	}

	// Add in the leafnode here since we passed through auth at this point.
	s.addLeafNodeConnection(c, proto.Name, proto.Cluster, true)

	// If we have permissions bound to this leafnode we need to send then back to the
	// origin server for local enforcement.
	s.sendPermsAndAccountInfo(c)

	// Create and initialize the smap since we know our bound account now.
	// This will send all registered subs too.
	s.initLeafNodeSmapAndSendSubs(c)

	// Announce the account connect event for a leaf node.
	// This will no-op as needed.
	s.sendLeafNodeConnect(c.acc)

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
	c.enqueueProto(generateInfoJSON(info))
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
	_subs := [1024]*subscription{}
	subs := _subs[:0]
	ims := []string{}

	// Hold the client lock otherwise there can be a race and miss some subs.
	c.mu.Lock()
	defer c.mu.Unlock()

	acc.mu.RLock()
	accName := acc.Name
	accNTag := acc.nameTag

	// To make printing look better when no friendly name present.
	if accNTag != _EMPTY_ {
		accNTag = "/" + accNTag
	}

	// If we are solicited we only send interest for local clients.
	if c.isSpokeLeafNode() {
		acc.sl.localSubs(&subs, true)
	} else {
		acc.sl.All(&subs)
	}

	// Check if we have an existing service import reply.
	siReply := copyBytes(acc.siReply)

	// Since leaf nodes only send on interest, if the bound
	// account has import services we need to send those over.
	for isubj := range acc.imports.services {
		if c.isSpokeLeafNode() && !c.canSubscribe(isubj) {
			c.Debugf("Not permitted to import service %q on behalf of %s%s", isubj, accName, accNTag)
			continue
		}
		ims = append(ims, isubj)
	}
	// Likewise for mappings.
	for _, m := range acc.mappings {
		if c.isSpokeLeafNode() && !c.canSubscribe(m.src) {
			c.Debugf("Not permitted to import mapping %q on behalf of %s%s", m.src, accName, accNTag)
			continue
		}
		ims = append(ims, m.src)
	}

	// Create a unique subject that will be used for loop detection.
	lds := acc.lds
	acc.mu.RUnlock()

	// Check if we have to create the LDS.
	if lds == _EMPTY_ {
		lds = leafNodeLoopDetectionSubjectPrefix + nuid.Next()
		acc.mu.Lock()
		acc.lds = lds
		acc.mu.Unlock()
	}

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
		// We need to keep track of this subscription so it can be removed
		// when the connection is closed so that the GC can release it.
		c.leaf.gwSub = &subscription{client: c, subject: []byte(gwReplyPrefix + ">")}
		c.srv.gwLeafSubs.Insert(c.leaf.gwSub)
	}

	// Now walk the results and add them to our smap
	rc := c.leaf.remoteCluster
	c.leaf.smap = make(map[string]int32)
	for _, sub := range subs {
		// Check perms regardless of role.
		if c.perms != nil && !c.canSubscribe(string(sub.subject)) {
			c.Debugf("Not permitted to subscribe to %q on behalf of %s%s", sub.subject, accName, accNTag)
			continue
		}
		// We ignore ourselves here.
		// Also don't add the subscription if it has a origin cluster and the
		// cluster name matches the one of the client we are sending to.
		if c != sub.client && (sub.origin == nil || (bytesToString(sub.origin) != rc)) {
			count := int32(1)
			if len(sub.queue) > 0 && sub.qw > 0 {
				count = sub.qw
			}
			c.leaf.smap[keyFromSub(sub)] += count
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
	// Detect loops by subscribing to a specific subject and checking
	// if this sub is coming back to us.
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
}

// updateInterestForAccountOnGateway called from gateway code when processing RS+ and RS-.
func (s *Server) updateInterestForAccountOnGateway(accName string, sub *subscription, delta int32) {
	acc, err := s.LookupAccount(accName)
	if acc == nil || err != nil {
		s.Debugf("No or bad account for %q, failed to update interest from gateway", accName)
		return
	}
	acc.updateLeafNodes(sub, delta)
}

// updateLeafNodes will make sure to update the account smap for the subscription.
// Will also forward to all leaf nodes as needed.
func (acc *Account) updateLeafNodes(sub *subscription, delta int32) {
	if acc == nil || sub == nil {
		return
	}

	// We will do checks for no leafnodes and same cluster here inline and under the
	// general account read lock.
	// If we feel we need to update the leafnodes we will do that out of line to avoid
	// blocking routes or GWs.

	acc.mu.RLock()
	// First check if we even have leafnodes here.
	if acc.nleafs == 0 {
		acc.mu.RUnlock()
		return
	}

	// Is this a loop detection subject.
	isLDS := bytes.HasPrefix(sub.subject, []byte(leafNodeLoopDetectionSubjectPrefix))

	// Capture the cluster even if its empty.
	var cluster string
	if sub.origin != nil {
		cluster = bytesToString(sub.origin)
	}

	// If we have an isolated cluster we can return early, as long as it is not a loop detection subject.
	// Empty clusters will return false for the check.
	if !isLDS && acc.isLeafNodeClusterIsolated(cluster) {
		acc.mu.RUnlock()
		return
	}

	// We can release the general account lock.
	acc.mu.RUnlock()

	// We can hold the list lock here to avoid having to copy a large slice.
	acc.lmu.RLock()
	defer acc.lmu.RUnlock()

	// Do this once.
	subject := string(sub.subject)

	// Walk the connected leafnodes.
	for _, ln := range acc.lleafs {
		if ln == sub.client {
			continue
		}
		// Check to make sure this sub does not have an origin cluster that matches the leafnode.
		ln.mu.Lock()
		// If skipped, make sure that we still let go the "$LDS." subscription that allows
		// the detection of loops as long as different cluster.
		clusterDifferent := cluster != ln.remoteCluster()
		if (isLDS && clusterDifferent) || ((cluster == _EMPTY_ || clusterDifferent) && (delta <= 0 || ln.canSubscribe(subject))) {
			ln.updateSmap(sub, delta, isLDS)
		}
		ln.mu.Unlock()
	}
}

// This will make an update to our internal smap and determine if we should send out
// an interest update to the remote side.
// Lock should be held.
func (c *client) updateSmap(sub *subscription, delta int32, isLDS bool) {
	if c.leaf.smap == nil {
		return
	}

	// If we are solicited make sure this is a local client or a non-solicited leaf node
	skind := sub.client.kind
	updateClient := skind == CLIENT || skind == SYSTEM || skind == JETSTREAM || skind == ACCOUNT
	if !isLDS && c.isSpokeLeafNode() && !(updateClient || (skind == LEAF && !sub.client.isSpokeLeafNode())) {
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
			return
		}
	}

	key := keyFromSub(sub)
	n, ok := c.leaf.smap[key]
	if delta < 0 && !ok {
		return
	}

	// We will update if its a queue, if count is zero (or negative), or we were 0 and are N > 0.
	update := sub.queue != nil || (n <= 0 && n+delta > 0) || (n > 0 && n+delta <= 0)
	n += delta
	if n > 0 {
		c.leaf.smap[key] = n
	} else {
		delete(c.leaf.smap, key)
	}
	if update {
		c.sendLeafNodeSubUpdate(key, n)
	}
}

// Used to force add subjects to the subject map.
func (c *client) forceAddToSmap(subj string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.leaf.smap == nil {
		return
	}
	n := c.leaf.smap[subj]
	if n != 0 {
		return
	}
	// Place into the map since it was not there.
	c.leaf.smap[subj] = 1
	c.sendLeafNodeSubUpdate(subj, 1)
}

// Used to force remove a subject from the subject map.
func (c *client) forceRemoveFromSmap(subj string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.leaf.smap == nil {
		return
	}
	n := c.leaf.smap[subj]
	if n == 0 {
		return
	}
	n--
	if n == 0 {
		// Remove is now zero
		delete(c.leaf.smap, subj)
		c.sendLeafNodeSubUpdate(subj, 0)
	} else {
		c.leaf.smap[subj] = n
	}
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
		if checkPerms {
			var subject string
			if sep := strings.IndexByte(key, ' '); sep != -1 {
				subject = key[:sep]
			} else {
				subject = key
			}
			if !c.canSubscribe(subject) {
				return
			}
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
	var sb strings.Builder
	sb.Grow(len(sub.subject) + len(sub.queue) + 1)
	sb.Write(sub.subject)
	if sub.queue != nil {
		// Just make the key subject spc group, e.g. 'foo bar'
		sb.WriteByte(' ')
		sb.Write(sub.queue)
	}
	return sb.String()
}

const (
	keyRoutedSub         = "R"
	keyRoutedSubByte     = 'R'
	keyRoutedLeafSub     = "L"
	keyRoutedLeafSubByte = 'L'
)

// Helper function to build the key that prevents collisions between normal
// routed subscriptions and routed subscriptions on behalf of a leafnode.
// Keys will look like this:
// "R foo"          -> plain routed sub on "foo"
// "R foo bar"      -> queue routed sub on "foo", queue "bar"
// "L foo bar"      -> plain routed leaf sub on "foo", leaf "bar"
// "L foo bar baz"  -> queue routed sub on "foo", queue "bar", leaf "baz"
func keyFromSubWithOrigin(sub *subscription) string {
	var sb strings.Builder
	sb.Grow(2 + len(sub.origin) + 1 + len(sub.subject) + 1 + len(sub.queue))
	leaf := len(sub.origin) > 0
	if leaf {
		sb.WriteByte(keyRoutedLeafSubByte)
	} else {
		sb.WriteByte(keyRoutedSubByte)
	}
	sb.WriteByte(' ')
	sb.Write(sub.subject)
	if sub.queue != nil {
		sb.WriteByte(' ')
		sb.Write(sub.queue)
	}
	if leaf {
		sb.WriteByte(' ')
		sb.Write(sub.origin)
	}
	return sb.String()
}

// Lock should be held.
func (c *client) writeLeafSub(w *bytes.Buffer, key string, n int32) {
	if key == _EMPTY_ {
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

	delta := int32(1)
	switch len(args) {
	case 1:
		sub.queue = nil
	case 3:
		sub.queue = args[1]
		sub.qw = int32(parseSize(args[2]))
		// TODO: (ik) We should have a non empty queue name and a queue
		// weight >= 1. For 2.11, we may want to return an error if that
		// is not the case, but for now just overwrite `delta` if queue
		// weight is greater than 1 (it is possible after a reconnect/
		// server restart to receive a queue weight > 1 for a new sub).
		if sub.qw > 1 {
			delta = sub.qw
		}
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

	if ldsPrefix && bytesToString(sub.subject) == acc.getLDSubject() {
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

	// If we are a hub check that we can publish to this subject.
	if checkPerms {
		subj := string(sub.subject)
		if subjectIsLiteral(subj) && !c.pubAllowedFullCheck(subj, true, true) {
			c.mu.Unlock()
			c.leafSubPermViolation(sub.subject)
			c.Debugf(fmt.Sprintf("Permissions Violation for Subscription to %q", sub.subject))
			return nil
		}
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
	key := bytesToString(sub.sid)
	osub := c.subs[key]
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
	} else if sub.queue != nil {
		// For a queue we need to update the weight.
		delta = sub.qw - atomic.LoadInt32(&osub.qw)
		atomic.StoreInt32(&osub.qw, sub.qw)
		acc.sl.UpdateRemoteQSub(osub)
	}
	spoke := c.isSpokeLeafNode()
	c.mu.Unlock()

	// Only add in shadow subs if a new sub or qsub.
	if osub == nil {
		if err := c.addShadowSubscriptions(acc, sub, true); err != nil {
			c.Errorf(err.Error())
		}
	}

	// If we are not solicited, treat leaf node subscriptions similar to a
	// client subscription, meaning we forward them to routes, gateways and
	// other leaf nodes as needed.
	if !spoke {
		// If we are routing add to the route map for the associated account.
		srv.updateRouteSubscriptionMap(acc, sub, delta)
		if srv.gateway.enabled {
			srv.gatewayUpdateSubInterest(acc.Name, sub, delta)
		}
	}
	// Now check on leafnode updates for other leaf nodes. We understand solicited
	// and non-solicited state in this call so we will do the right thing.
	acc.updateLeafNodes(sub, delta)

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

	spoke := c.isSpokeLeafNode()
	// We store local subs by account and subject and optionally queue name.
	// LS- will have the arg exactly as the key.
	sub, ok := c.subs[string(arg)]
	if !ok {
		// If not found, don't try to update routes/gws/leaf nodes.
		c.mu.Unlock()
		return nil
	}
	delta := int32(1)
	if len(sub.queue) > 0 {
		delta = sub.qw
	}
	c.mu.Unlock()

	c.unsubscribe(acc, sub, true, true)
	if !spoke {
		// If we are routing subtract from the route map for the associated account.
		srv.updateRouteSubscriptionMap(acc, sub, -delta)
		// Gateways
		if srv.gateway.enabled {
			srv.gatewayUpdateSubInterest(acc.Name, sub, -delta)
		}
	}
	// Now check on leafnode updates for other leaf nodes.
	acc.updateLeafNodes(sub, -delta)
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

	srv, acc, subject := c.srv, c.acc, string(c.pa.subject)

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
		r, ok = c.in.results[subject]
	} else {
		// Reset our L1 completely.
		c.in.results = make(map[string]*SublistResult)
		c.in.genid = genid
	}

	// Go back to the sublist data structure.
	if !ok {
		r = c.acc.sl.Match(subject)
		// Prune the results cache. Keeps us from unbounded growth. Random delete.
		if len(c.in.results) >= maxResultCacheSize {
			n := 0
			for subj := range c.in.results {
				delete(c.in.results, subj)
				if n++; n > pruneSize {
					break
				}
			}
		}
		// Then add the new cache entry.
		c.in.results[subject] = r
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
		// If this is a mapped subject that means the mapped interest
		// is what got us here, but this might not have a queue designation
		// If that is the case, make sure we ignore to process local queue subscribers.
		if len(c.pa.mapped) > 0 && len(c.pa.queues) == 0 {
			flag |= pmrIgnoreEmptyQueueFilter
		}
		_, qnames = c.processMsgResults(acc, r, msg, nil, c.pa.subject, c.pa.reply, flag)
	}

	// Now deal with gateways
	if c.srv.gateway.enabled {
		c.sendMsgToGateways(acc, msg, c.pa.subject, c.pa.reply, qnames, true)
	}
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
	// Check if we got a cluster name collision.
	if strings.Contains(errStr, ErrLeafNodeHasSameClusterName.Error()) {
		_, delay := c.setLeafConnectDelayIfSoliciting(leafNodeReconnectDelayAfterClusterNameSame)
		c.Errorf("Leafnode connection dropped with same cluster name error. Delaying attempt to reconnect for %v", delay)
		return
	}

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
func (c *client) leafNodeGetTLSConfigForSolicit(remote *leafNodeCfg) (bool, *tls.Config, string, float64) {
	var (
		tlsConfig  *tls.Config
		tlsName    string
		tlsTimeout float64
	)

	remote.RLock()
	defer remote.RUnlock()

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
	infoTimeout := remote.FirstInfoTimeout
	remote.RUnlock()
	// Will do the client-side TLS handshake if needed.
	tlsRequired, err := c.leafClientHandshakeIfNeeded(remote, opts)
	if err != nil {
		// 0 will indicate that the connection was already closed
		return nil, 0, err
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
	lpath := leafNodeWSPath
	if curPath := rURL.EscapedPath(); curPath != _EMPTY_ {
		if curPath[0] == '/' {
			curPath = curPath[1:]
		}
		lpath = path.Join(curPath, lpath)
	} else {
		lpath = lpath[1:]
	}
	ustr := fmt.Sprintf("%s://%s/%s", scheme, rURL.Host, lpath)
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
	c.nc.SetDeadline(time.Now().Add(infoTimeout))
	if err := req.Write(c.nc); err != nil {
		return nil, WriteError, err
	}

	var resp *http.Response

	br := bufio.NewReaderSize(c.nc, MAX_CONTROL_LINE_SIZE)
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
// protocol.
func (s *Server) leafNodeResumeConnectProcess(c *client) {
	clusterName := s.ClusterName()

	c.mu.Lock()
	if c.isClosed() {
		c.mu.Unlock()
		return
	}
	if err := c.sendLeafConnect(clusterName, c.headers); err != nil {
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

// This is invoked for remote LEAF connections after processing the INFO
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
	if err := c.registerWithAccount(acc); err != nil {
		if err == ErrTooManyAccountConnections {
			c.maxAccountConnExceeded()
			return
		} else if err == ErrLeafNodeLoop {
			c.handleLeafNodeLoop(true)
			return
		}
		c.Errorf("Registering leaf with account %s resulted in error: %v", acc.Name, err)
		c.closeConnection(ProtocolViolation)
		return
	}
	s.addLeafNodeConnection(c, _EMPTY_, _EMPTY_, false)
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
		c.setFirstPingTimer()
	}
	c.mu.Unlock()
	if closed {
		s.removeLeafNodeConnection(c)
		if prev := acc.removeClient(c); prev == 1 {
			s.decActiveAccounts()
		}
	}
}
