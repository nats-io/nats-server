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
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultSolicitGatewaysDelay         = time.Second
	defaultGatewayConnectDelay          = time.Second
	defaultGatewayReconnectDelay        = time.Second
	defaultGatewayRecentSubExpiration   = 5 * time.Second
	defaultGatewayMaxRUnsubBeforeSwitch = 1000
	gwReplyPrefix                       = "$GR."
	gwReplyStart                        = len(gwReplyPrefix) + 5 // len of prefix above + len of hash (4) + "."
)

var (
	gatewayConnectDelay          = defaultGatewayConnectDelay
	gatewayReconnectDelay        = defaultGatewayReconnectDelay
	gatewayMaxRUnsubBeforeSwitch = defaultGatewayMaxRUnsubBeforeSwitch
	gatewaySolicitDelay          = int64(defaultSolicitGatewaysDelay)
)

// Warning when user configures gateway TLS insecure
const gatewayTLSInsecureWarning = "TLS certificate chain and hostname of solicited gateways will not be verified. DO NOT USE IN PRODUCTION!"

// SetGatewaysSolicitDelay sets the initial delay before gateways
// connections are initiated.
// Used by tests.
func SetGatewaysSolicitDelay(delay time.Duration) {
	atomic.StoreInt64(&gatewaySolicitDelay, int64(delay))
}

// ResetGatewaysSolicitDelay resets the initial delay before gateways
// connections are initiated to its default values.
// Used by tests.
func ResetGatewaysSolicitDelay() {
	atomic.StoreInt64(&gatewaySolicitDelay, int64(defaultSolicitGatewaysDelay))
}

const (
	gatewayCmdGossip          byte = 1
	gatewayCmdAllSubsStart    byte = 2
	gatewayCmdAllSubsComplete byte = 3
)

// GatewayInterestMode represents an account interest mode for a gateway connection
type GatewayInterestMode byte

// GatewayInterestMode values
const (
	// optimistic is the default mode where a cluster will send
	// to a gateway unless it is been told that there is no interest
	// (this is for plain subscribers only).
	Optimistic GatewayInterestMode = iota
	// transitioning is when a gateway has to send too many
	// no interest on subjects to the remote and decides that it is
	// now time to move to modeInterestOnly (this is on a per account
	// basis).
	Transitioning
	// interestOnly means that a cluster sends all it subscriptions
	// interest to the gateway, which in return does not send a message
	// unless it knows that there is explicit interest.
	InterestOnly
)

func (im GatewayInterestMode) String() string {
	switch im {
	case Optimistic:
		return "Optimistic"
	case InterestOnly:
		return "Interest-Only"
	case Transitioning:
		return "Transitioning"
	default:
		return "Unknown"
	}
}

type srvGateway struct {
	totalQSubs int64 //total number of queue subs in all remote gateways (used with atomic operations)
	sync.RWMutex
	enabled  bool                   // Immutable, true if both a name and port are configured
	name     string                 // Name of the Gateway on this server
	out      map[string]*client     // outbound gateways
	outo     []*client              // outbound gateways maintained in an order suitable for sending msgs (currently based on RTT)
	in       map[uint64]*client     // inbound gateways
	remotes  map[string]*gatewayCfg // Config of remote gateways
	URLs     map[string]struct{}    // Set of all Gateway URLs in the cluster
	URL      string                 // This server gateway URL (after possible random port is resolved)
	info     *Info                  // Gateway Info protocol
	infoJSON []byte                 // Marshal'ed Info protocol
	runknown bool                   // Rejects unknown (not configured) gateway connections
	replyPfx []byte                 // Will be "$GR.<this cluster name hash>."

	// We maintain the interest of subjects and queues per account.
	// For a given account, entries in the map could be something like this:
	// foo.bar {n: 3} 			// 3 subs on foo.bar
	// foo.>   {n: 6}			// 6 subs on foo.>
	// foo bar {n: 1, q: true}  // 1 qsub on foo, queue bar
	// foo baz {n: 3, q: true}  // 3 qsubs on foo, queue baz
	pasi struct {
		// Protect map since accessed from different go-routine and avoid
		// possible race resulting in RS+ being sent before RS- resulting
		// in incorrect interest suppression.
		// Will use while sending QSubs (on GW connection accept) and when
		// switching to the send-all-subs mode.
		sync.Mutex
		m map[string]map[string]*sitally
	}

	// This is to track recent subscriptions for a given connection
	rsubs sync.Map

	resolver  netResolver   // Used to resolve host name before calling net.Dial()
	sqbsz     int           // Max buffer size to send queue subs protocol. Used for testing.
	recSubExp time.Duration // For how long do we check if there is a subscription match for a message with reply
}

// Subject interest tally. Also indicates if the key in the map is a
// queue or not.
type sitally struct {
	n int32 // number of subscriptions directly matching
	q bool  // indicate that this is a queue
}

type gatewayCfg struct {
	sync.RWMutex
	*RemoteGatewayOpts
	replyPfx       []byte
	urls           map[string]*url.URL
	connAttempts   int
	tlsName        string
	implicit       bool
	varzUpdateURLs bool // Tells monitoring code to update URLs when varz is inspected.
}

// Struct for client's gateway related fields
type gateway struct {
	name       string
	outbound   bool
	cfg        *gatewayCfg
	connectURL *url.URL          // Needed when sending CONNECT after receiving INFO from remote
	infoJSON   []byte            // Needed when sending INFO after receiving INFO from remote
	outsim     *sync.Map         // Per-account subject interest (or no-interest) (outbound conn)
	insim      map[string]*insie // Per-account subject no-interest sent or modeInterestOnly mode (inbound conn)

	// Set/check in readLoop without lock. This is to know that an inbound has sent the CONNECT protocol first
	connected bool
}

// Outbound subject interest entry.
type outsie struct {
	sync.RWMutex
	// Indicate that all subs should be stored. This is
	// set to true when receiving the command from the
	// remote that we are about to receive all its subs.
	mode GatewayInterestMode
	// If not nil, used for no-interest for plain subs.
	// If a subject is present in this map, it means that
	// the remote is not interested in that subject.
	// When we have received the command that says that
	// the remote has sent all its subs, this is set to nil.
	ni map[string]struct{}
	// Contains queue subscriptions when in optimistic mode,
	// and all subs when pk is > 0.
	sl *Sublist
	// Number of queue subs
	qsubs int
}

// Inbound subject interest entry.
// If `ni` is not nil, it stores the subjects for which an
// RS- was sent to the remote gateway. When a subscription
// is created, this is used to know if we need to send
// an RS+ to clear the no-interest in the remote.
// When an account is switched to modeInterestOnly (we send
// all subs of an account to the remote), then `ni` is nil and
// when all subs have been sent, mode is set to modeInterestOnly
type insie struct {
	ni   map[string]struct{} // Record if RS- was sent for given subject
	mode GatewayInterestMode
}

// clone returns a deep copy of the RemoteGatewayOpts object
func (r *RemoteGatewayOpts) clone() *RemoteGatewayOpts {
	if r == nil {
		return nil
	}
	clone := &RemoteGatewayOpts{
		Name: r.Name,
		URLs: deepCopyURLs(r.URLs),
	}
	if r.TLSConfig != nil {
		clone.TLSConfig = r.TLSConfig.Clone()
		clone.TLSTimeout = r.TLSTimeout
	}
	return clone
}

// Ensure that gateway is properly configured.
func validateGatewayOptions(o *Options) error {
	if o.Gateway.Name == "" && o.Gateway.Port == 0 {
		return nil
	}
	if o.Gateway.Name == "" {
		return fmt.Errorf("gateway has no name")
	}
	if o.Gateway.Port == 0 {
		return fmt.Errorf("gateway %q has no port specified (select -1 for random port)", o.Gateway.Name)
	}
	for i, g := range o.Gateway.Gateways {
		if g.Name == "" {
			return fmt.Errorf("gateway in the list %d has no name", i)
		}
		if len(g.URLs) == 0 {
			return fmt.Errorf("gateway %q has no URL", g.Name)
		}
	}
	return nil
}

// Computes a hash of 4 characters for the given gateway name.
// This will be used for routing of replies.
func getReplyPrefixForGateway(name string) []byte {
	sha := sha256.New()
	sha.Write([]byte(name))
	fullHash := []byte(fmt.Sprintf("%x", sha.Sum(nil)))
	prefix := make([]byte, 0, len(gwReplyPrefix)+5)
	prefix = append(prefix, gwReplyPrefix...)
	prefix = append(prefix, fullHash[:4]...)
	prefix = append(prefix, '.')
	return prefix
}

// Initialize the s.gateway structure. We do this even if the server
// does not have a gateway configured. In some part of the code, the
// server will check the number of outbound gateways, etc.. and so
// we don't have to check if s.gateway is nil or not.
func newGateway(opts *Options) (*srvGateway, error) {
	gateway := &srvGateway{
		name:     opts.Gateway.Name,
		out:      make(map[string]*client),
		outo:     make([]*client, 0, 4),
		in:       make(map[uint64]*client),
		remotes:  make(map[string]*gatewayCfg),
		URLs:     make(map[string]struct{}),
		resolver: opts.Gateway.resolver,
		runknown: opts.Gateway.RejectUnknown,
		replyPfx: getReplyPrefixForGateway(opts.Gateway.Name),
	}
	gateway.Lock()
	defer gateway.Unlock()

	gateway.pasi.m = make(map[string]map[string]*sitally)

	if gateway.resolver == nil {
		gateway.resolver = netResolver(net.DefaultResolver)
	}

	// Create remote gateways
	for _, rgo := range opts.Gateway.Gateways {
		// Ignore if there is a remote gateway with our name.
		if rgo.Name == gateway.name {
			continue
		}
		cfg := &gatewayCfg{
			RemoteGatewayOpts: rgo.clone(),
			replyPfx:          getReplyPrefixForGateway(rgo.Name),
			urls:              make(map[string]*url.URL, len(rgo.URLs)),
		}
		if opts.Gateway.TLSConfig != nil && cfg.TLSConfig == nil {
			cfg.TLSConfig = opts.Gateway.TLSConfig.Clone()
		}
		if cfg.TLSTimeout == 0 {
			cfg.TLSTimeout = opts.Gateway.TLSTimeout
		}
		for _, u := range rgo.URLs {
			// For TLS, look for a hostname that we can use for TLSConfig.ServerName
			cfg.saveTLSHostname(u)
			cfg.urls[u.Host] = u
		}
		gateway.remotes[cfg.Name] = cfg
	}

	gateway.sqbsz = opts.Gateway.sendQSubsBufSize
	if gateway.sqbsz == 0 {
		gateway.sqbsz = maxBufSize
	}
	gateway.recSubExp = defaultGatewayRecentSubExpiration

	gateway.enabled = opts.Gateway.Name != "" && opts.Gateway.Port != 0
	return gateway, nil
}

// Returns the Gateway's name of this server.
func (g *srvGateway) getName() string {
	g.RLock()
	n := g.name
	g.RUnlock()
	return n
}

// Returns the Gateway URLs of all servers in the local cluster.
// This is used to send to other cluster this server connects to.
// The gateway read-lock is held on entry
func (g *srvGateway) getURLs() []string {
	a := make([]string, 0, len(g.URLs))
	for u := range g.URLs {
		a = append(a, u)
	}
	return a
}

// Returns if this server rejects connections from gateways that are not
// explicitly configured.
func (g *srvGateway) rejectUnknown() bool {
	g.RLock()
	reject := g.runknown
	g.RUnlock()
	return reject
}

// Starts the gateways accept loop and solicit explicit gateways
// after an initial delay. This delay is meant to give a chance to
// the cluster to form and this server gathers gateway URLs for this
// cluster in order to send that as part of the connect/info process.
func (s *Server) startGateways() {
	// Spin up the accept loop
	ch := make(chan struct{})
	go s.gatewayAcceptLoop(ch)
	<-ch

	// Delay start of creation of gateways to give a chance
	// to the local cluster to form.
	s.startGoRoutine(func() {
		defer s.grWG.Done()

		dur := s.getOpts().gatewaysSolicitDelay
		if dur == 0 {
			dur = time.Duration(atomic.LoadInt64(&gatewaySolicitDelay))
		}

		select {
		case <-time.After(dur):
			s.solicitGateways()
		case <-s.quitCh:
			return
		}
	})
}

// This is the gateways accept loop. This runs as a go-routine.
// The listen specification is resolved (if use of random port),
// then a listener is started. After that, this routine enters
// a loop (until the server is shutdown) accepting incoming
// gateway connections.
func (s *Server) gatewayAcceptLoop(ch chan struct{}) {
	defer func() {
		if ch != nil {
			close(ch)
		}
	}()

	// Snapshot server options.
	opts := s.getOpts()

	port := opts.Gateway.Port
	if port == -1 {
		port = 0
	}

	hp := net.JoinHostPort(opts.Gateway.Host, strconv.Itoa(port))
	l, e := net.Listen("tcp", hp)
	if e != nil {
		s.Fatalf("Error listening on gateway port: %d - %v", opts.Gateway.Port, e)
		return
	}
	s.Noticef("Gateway name is %s", s.getGatewayName())
	s.Noticef("Listening for gateways connections on %s",
		net.JoinHostPort(opts.Gateway.Host, strconv.Itoa(l.Addr().(*net.TCPAddr).Port)))

	s.mu.Lock()
	tlsReq := opts.Gateway.TLSConfig != nil
	authRequired := opts.Gateway.Username != ""
	info := &Info{
		ID:           s.info.ID,
		Version:      s.info.Version,
		AuthRequired: authRequired,
		TLSRequired:  tlsReq,
		TLSVerify:    tlsReq,
		MaxPayload:   s.info.MaxPayload,
		Gateway:      opts.Gateway.Name,
	}
	// If we have selected a random port...
	if port == 0 {
		// Write resolved port back to options.
		opts.Gateway.Port = l.Addr().(*net.TCPAddr).Port
	}
	// Possibly override Host/Port based on Gateway.Advertise
	if err := s.setGatewayInfoHostPort(info, opts); err != nil {
		s.Fatalf("Error setting gateway INFO with Gateway.Advertise value of %s, err=%v", opts.Gateway.Advertise, err)
		l.Close()
		s.mu.Unlock()
		return
	}
	// Setup state that can enable shutdown
	s.gatewayListener = l

	// Warn if insecure is configured in the main Gateway configuration
	// or any of the RemoteGateway's. This means that we need to check
	// remotes even if TLS would not be configured for the accept.
	warn := tlsReq && opts.Gateway.TLSConfig.InsecureSkipVerify
	if !warn {
		for _, g := range opts.Gateway.Gateways {
			if g.TLSConfig != nil && g.TLSConfig.InsecureSkipVerify {
				warn = true
				break
			}
		}
	}
	if warn {
		s.Warnf(gatewayTLSInsecureWarning)
	}
	s.mu.Unlock()

	// Let them know we are up
	close(ch)
	ch = nil

	tmpDelay := ACCEPT_MIN_SLEEP

	for s.isRunning() {
		conn, err := l.Accept()
		if err != nil {
			tmpDelay = s.acceptError("Gateway", err, tmpDelay)
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP
		s.startGoRoutine(func() {
			s.createGateway(nil, nil, conn)
			s.grWG.Done()
		})
	}
	s.Debugf("Gateway accept loop exiting..")
	s.done <- true
}

// Similar to setInfoHostPortAndGenerateJSON, but for gatewayInfo.
func (s *Server) setGatewayInfoHostPort(info *Info, o *Options) error {
	gw := s.gateway
	gw.Lock()
	defer gw.Unlock()
	delete(gw.URLs, gw.URL)
	if o.Gateway.Advertise != "" {
		advHost, advPort, err := parseHostPort(o.Gateway.Advertise, o.Gateway.Port)
		if err != nil {
			return err
		}
		info.Host = advHost
		info.Port = advPort
	} else {
		info.Host = o.Gateway.Host
		info.Port = o.Gateway.Port
		// If the host is "0.0.0.0" or "::" we need to resolve to a public IP.
		// This will return at most 1 IP.
		hostIsIPAny, ips, err := s.getNonLocalIPsIfHostIsIPAny(info.Host, false)
		if err != nil {
			return err
		}
		if hostIsIPAny {
			if len(ips) == 0 {
				// TODO(ik): Should we fail here (prevent starting)? If not, we
				// are going to "advertise" the 0.0.0.0:<port> url, which means
				// that remote are going to try to connect to 0.0.0.0:<port>,
				// which means a connect to loopback address, which is going
				// to fail with either TLS error, conn refused if the remote
				// is using different gateway port than this one, or error
				// saying that it tried to connect to itself.
				s.Errorf("Could not find any non-local IP for gateway %q with listen specification %q",
					gw.name, info.Host)
			} else {
				// Take the first from the list...
				info.Host = ips[0]
			}
		}
	}
	gw.URL = net.JoinHostPort(info.Host, strconv.Itoa(info.Port))
	if o.Gateway.Advertise != "" {
		s.Noticef("Advertise address for gateway %q is set to %s", gw.name, gw.URL)
	} else {
		s.Noticef("Address for gateway %q is %s", gw.name, gw.URL)
	}
	gw.URLs[gw.URL] = struct{}{}
	gw.info = info
	info.GatewayURL = gw.URL
	// (re)generate the gatewayInfoJSON byte array
	gw.generateInfoJSON()
	return nil
}

// Generates the Gateway INFO protocol.
// The gateway lock is held on entry
func (g *srvGateway) generateInfoJSON() {
	b, err := json.Marshal(g.info)
	if err != nil {
		panic(err)
	}
	g.infoJSON = []byte(fmt.Sprintf(InfoProto, b))
}

// Goes through the list of registered gateways and try to connect to those.
// The list (remotes) is initially containing the explicit remote gateways,
// but the list is augmented with any implicit (discovered) gateway. Therefore,
// this function only solicit explicit ones.
func (s *Server) solicitGateways() {
	gw := s.gateway
	gw.RLock()
	defer gw.RUnlock()
	for _, cfg := range gw.remotes {
		// Since we delay the creation of gateways, it is
		// possible that server starts to receive inbound from
		// other clusters and in turn create outbounds. So here
		// we create only the ones that are configured.
		if !cfg.isImplicit() {
			cfg := cfg // Create new instance for the goroutine.
			s.startGoRoutine(func() {
				s.solicitGateway(cfg, true)
				s.grWG.Done()
			})
		}
	}
}

// Reconnect to the gateway after a little wait period. For explicit
// gateways, we also wait for the default reconnect time.
func (s *Server) reconnectGateway(cfg *gatewayCfg) {
	defer s.grWG.Done()

	delay := time.Duration(rand.Intn(100)) * time.Millisecond
	if !cfg.isImplicit() {
		delay += gatewayReconnectDelay
	}
	select {
	case <-time.After(delay):
	case <-s.quitCh:
		return
	}
	s.solicitGateway(cfg, false)
}

// This function will loop trying to connect to any URL attached
// to the given Gateway. It will return once a connection has been created.
func (s *Server) solicitGateway(cfg *gatewayCfg, firstConnect bool) {
	var (
		opts       = s.getOpts()
		isImplicit = cfg.isImplicit()
		urls       = cfg.getURLs()
		attempts   int
		typeStr    string
	)
	if isImplicit {
		typeStr = "implicit"
	} else {
		typeStr = "explicit"
	}

	const connFmt = "Connecting to %s gateway %q (%s) at %s (attempt %v)"
	const connErrFmt = "Error connecting to %s gateway %q (%s) at %s (attempt %v): %v"

	for s.isRunning() && len(urls) > 0 {
		attempts++
		report := s.shouldReportConnectErr(firstConnect, attempts)
		// Iteration is random
		for _, u := range urls {
			address, err := s.getRandomIP(s.gateway.resolver, u.Host)
			if err != nil {
				s.Errorf("Error getting IP for %s gateway %q (%s): %v", typeStr, cfg.Name, u.Host, err)
				continue
			}
			if report {
				s.Noticef(connFmt, typeStr, cfg.Name, u.Host, address, attempts)
			} else {
				s.Debugf(connFmt, typeStr, cfg.Name, u.Host, address, attempts)
			}
			conn, err := net.DialTimeout("tcp", address, DEFAULT_ROUTE_DIAL)
			if err == nil {
				// We could connect, create the gateway connection and return.
				s.createGateway(cfg, u, conn)
				return
			}
			if report {
				s.Errorf(connErrFmt, typeStr, cfg.Name, u.Host, address, attempts, err)
			} else {
				s.Debugf(connErrFmt, typeStr, cfg.Name, u.Host, address, attempts, err)
			}
			// Break this loop if server is being shutdown...
			if !s.isRunning() {
				break
			}
		}
		if isImplicit {
			if opts.Gateway.ConnectRetries == 0 || attempts > opts.Gateway.ConnectRetries {
				s.gateway.Lock()
				delete(s.gateway.remotes, cfg.Name)
				s.gateway.Unlock()
				return
			}
		}
		select {
		case <-s.quitCh:
			return
		case <-time.After(gatewayConnectDelay):
			continue
		}
	}
}

// Called when a gateway connection is either accepted or solicited.
// If accepted, the gateway is marked as inbound.
// If solicited, the gateway is marked as outbound.
func (s *Server) createGateway(cfg *gatewayCfg, url *url.URL, conn net.Conn) {
	// Snapshot server options.
	opts := s.getOpts()

	now := time.Now()
	c := &client{srv: s, nc: conn, start: now, last: now, kind: GATEWAY}

	// Are we creating the gateway based on the configuration
	solicit := cfg != nil
	var tlsRequired bool
	if solicit {
		tlsRequired = cfg.TLSConfig != nil
	} else {
		tlsRequired = opts.Gateway.TLSConfig != nil
	}

	// Generate INFO to send
	s.gateway.RLock()
	// Make a copy
	info := *s.gateway.info
	info.GatewayURLs = s.gateway.getURLs()
	s.gateway.RUnlock()
	b, _ := json.Marshal(&info)
	infoJSON := []byte(fmt.Sprintf(InfoProto, b))

	// Perform some initialization under the client lock
	c.mu.Lock()
	c.initClient()
	c.gw = &gateway{}
	if solicit {
		// This is an outbound gateway connection
		c.gw.outbound = true
		c.gw.name = cfg.Name
		c.gw.cfg = cfg
		cfg.bumpConnAttempts()
		// Since we are delaying the connect until after receiving
		// the remote's INFO protocol, save the URL we need to connect to.
		c.gw.connectURL = url
		c.gw.infoJSON = infoJSON

		c.Noticef("Creating outbound gateway connection to %q", cfg.Name)
	} else {
		// Inbound gateway connection
		c.Noticef("Processing inbound gateway connection")
	}

	// Check for TLS
	if tlsRequired {
		var timeout float64
		// If we solicited, we will act like the client, otherwise the server.
		if solicit {
			c.Debugf("Starting TLS gateway client handshake")
			cfg.RLock()
			tlsName := cfg.tlsName
			tlsConfig := cfg.TLSConfig
			timeout = cfg.TLSTimeout
			cfg.RUnlock()
			if tlsConfig.ServerName == "" {
				// If the given url is a hostname, use this hostname for the
				// ServerName. If it is an IP, use the cfg's tlsName. If none
				// is available, resort to current IP.
				host := url.Hostname()
				if tlsName != "" && net.ParseIP(host) != nil {
					host = tlsName
				}
				tlsConfig.ServerName = host
			}
			c.nc = tls.Client(c.nc, tlsConfig)
		} else {
			c.Debugf("Starting TLS gateway server handshake")
			c.nc = tls.Server(c.nc, opts.Gateway.TLSConfig)
			timeout = opts.Gateway.TLSTimeout
		}

		conn := c.nc.(*tls.Conn)

		// Setup the timeout
		ttl := secondsToDuration(timeout)
		time.AfterFunc(ttl, func() { tlsTimeout(c, conn) })
		conn.SetReadDeadline(time.Now().Add(ttl))

		c.mu.Unlock()
		if err := conn.Handshake(); err != nil {
			c.Errorf("TLS gateway handshake error: %v", err)
			c.sendErr("Secure Connection - TLS Required")
			c.closeConnection(TLSHandshakeError)
			return
		}
		// Reset the read deadline
		conn.SetReadDeadline(time.Time{})

		// Re-Grab lock
		c.mu.Lock()

		// Verify that the connection did not go away while we released the lock.
		if c.nc == nil {
			c.mu.Unlock()
			return
		}
	}

	// Do final client initialization
	c.in.pacache = make(map[string]*perAccountCache)
	if solicit {
		// This is an outbound gateway connection
		c.gw.outsim = &sync.Map{}
	} else {
		// Inbound gateway connection
		c.gw.insim = make(map[string]*insie)
	}

	// Register in temp map for now until gateway properly registered
	// in out or in gateways.
	if !s.addToTempClients(c.cid, c) {
		c.mu.Unlock()
		c.closeConnection(ServerShutdown)
		return
	}

	// Only send if we accept a connection. Will send CONNECT+INFO as an
	// outbound only after processing peer's INFO protocol.
	if !solicit {
		c.sendInfo(infoJSON)
	}

	// Spin up the read loop.
	s.startGoRoutine(func() { c.readLoop() })

	// Spin up the write loop.
	s.startGoRoutine(func() { c.writeLoop() })

	if tlsRequired {
		c.Debugf("TLS handshake complete")
		cs := c.nc.(*tls.Conn).ConnectionState()
		c.Debugf("TLS version %s, cipher suite %s", tlsVersion(cs.Version), tlsCipher(cs.CipherSuite))
	}

	// Set the Ping timer after sending connect and info.
	c.setPingTimer()

	c.mu.Unlock()
}

// Builds and sends the CONNET protocol for a gateway.
func (c *client) sendGatewayConnect() {
	tlsRequired := c.gw.cfg.TLSConfig != nil
	url := c.gw.connectURL
	c.gw.connectURL = nil
	var user, pass string
	if userInfo := url.User; userInfo != nil {
		user = userInfo.Username()
		pass, _ = userInfo.Password()
	}
	cinfo := connectInfo{
		Verbose:  false,
		Pedantic: false,
		User:     user,
		Pass:     pass,
		TLS:      tlsRequired,
		Name:     c.srv.info.ID,
		Gateway:  c.srv.getGatewayName(),
	}
	b, err := json.Marshal(cinfo)
	if err != nil {
		panic(err)
	}
	c.sendProto([]byte(fmt.Sprintf(ConProto, b)), true)
}

// Process the CONNECT protocol from a gateway connection.
// Returns an error to the connection if the CONNECT is not from a gateway
// (for instance a client or route connecting to the gateway port), or
// if the destination does not match the gateway name of this server.
//
// <Invoked from inbound connection's readLoop>
func (c *client) processGatewayConnect(arg []byte) error {
	connect := &connectInfo{}
	if err := json.Unmarshal(arg, connect); err != nil {
		return err
	}

	// Coming from a client or a route, reject
	if connect.Gateway == "" {
		c.sendErrAndErr(ErrClientOrRouteConnectedToGatewayPort.Error())
		c.closeConnection(WrongPort)
		return ErrClientOrRouteConnectedToGatewayPort
	}

	c.mu.Lock()
	s := c.srv
	c.mu.Unlock()

	// If we reject unknown gateways, make sure we have it configured,
	// otherwise return an error.
	if s.gateway.rejectUnknown() && s.getRemoteGateway(connect.Gateway) == nil {
		c.Errorf("Rejecting connection from gateway %q", connect.Gateway)
		c.sendErr(fmt.Sprintf("Connection to gateway %q rejected", s.getGatewayName()))
		c.closeConnection(WrongGateway)
		return ErrWrongGateway
	}

	// For a gateway connection, c.gw is guaranteed not to be nil here
	// (created in createGateway() and never set to nil).
	// For inbound connections, it is important to know in the parser
	// if the CONNECT was received first, so we use this boolean (as
	// opposed to client.flags that require locking) to indicate that
	// CONNECT was processed. Again, this boolean is set/read in the
	// readLoop without locking.
	c.gw.connected = true

	return nil
}

// Process the INFO protocol from a gateway connection.
//
// If the gateway connection is an outbound (this server initiated the connection),
// this function checks that the incoming INFO contains the Gateway field. If empty,
// it means that this is a response from an older server or that this server connected
// to the wrong port.
// The outbound gateway may also receive a gossip INFO protocol from the remote gateway,
// indicating other gateways that the remote knows about. This server will try to connect
// to those gateways (if not explicitly configured or already implicitly connected).
// In both cases (explicit or implicit), the local cluster is notified about the existence
// of this new gateway. This allows servers in the cluster to ensure that they have an
// outbound connection to this gateway.
//
// For an inbound gateway, the gateway is simply registered and the info protocol
// is saved to be used after processing the CONNECT.
//
// <Invoked from both inbound/outbound readLoop's connection>
func (c *client) processGatewayInfo(info *Info) {
	var (
		gwName string
		cfg    *gatewayCfg
	)
	c.mu.Lock()
	s := c.srv
	cid := c.cid

	// Check if this is the first INFO. (this call sets the flag if not already set).
	isFirstINFO := c.flags.setIfNotSet(infoReceived)

	isOutbound := c.gw.outbound
	if isOutbound {
		gwName = c.gw.name
		cfg = c.gw.cfg
	} else if isFirstINFO {
		c.gw.name = info.Gateway
	}
	if isFirstINFO {
		c.opts.Name = info.ID
	}
	c.mu.Unlock()

	// For an outbound connection...
	if isOutbound {
		// Check content of INFO for fields indicating that it comes from a gateway.
		// If we incorrectly connect to the wrong port (client or route), we won't
		// have the Gateway field set.
		if info.Gateway == "" {
			c.sendErrAndErr(fmt.Sprintf("Attempt to connect to gateway %q using wrong port", gwName))
			c.closeConnection(WrongPort)
			return
		}
		// Check that the gateway name we got is what we expect
		if info.Gateway != gwName {
			// Unless this is the very first INFO, it may be ok if this is
			// a gossip request to connect to other gateways.
			if !isFirstINFO && info.GatewayCmd == gatewayCmdGossip {
				// If we are configured to reject unknown, do not attempt to
				// connect to one that we don't have configured.
				if s.gateway.rejectUnknown() && s.getRemoteGateway(info.Gateway) == nil {
					return
				}
				s.processImplicitGateway(info)
				return
			}
			// Otherwise, this is a failure...
			// We are reporting this error in the log...
			c.Errorf("Failing connection to gateway %q, remote gateway name is %q",
				gwName, info.Gateway)
			// ...and sending this back to the remote so that the error
			// makes more sense in the remote server's log.
			c.sendErr(fmt.Sprintf("Connection from %q rejected, wanted to connect to %q, this is %q",
				s.getGatewayName(), gwName, info.Gateway))
			c.closeConnection(WrongGateway)
			return
		}

		// Possibly add URLs that we get from the INFO protocol.
		if len(info.GatewayURLs) > 0 {
			cfg.updateURLs(info.GatewayURLs)
		}

		// If this is the first INFO, send our connect
		if isFirstINFO {
			// Note, if we want to support NKeys, then we would get the nonce
			// from this INFO protocol and can sign it in the CONNECT we are
			// going to send now.
			c.mu.Lock()
			c.sendGatewayConnect()
			c.Debugf("Gateway connect protocol sent to %q", gwName)
			// Send INFO too
			c.sendInfo(c.gw.infoJSON)
			c.gw.infoJSON = nil
			c.mu.Unlock()

			// Register as an outbound gateway.. if we had a protocol to ack our connect,
			// then we should do that when process that ack.
			if s.registerOutboundGatewayConnection(gwName, c) {
				c.Noticef("Outbound gateway connection to %q (%s) registered", gwName, info.ID)
				// Now that the outbound gateway is registered, we can remove from temp map.
				s.removeFromTempClients(cid)
			} else {
				// There was a bug that would cause a connection to possibly
				// be called twice resulting in reconnection of twice the
				// same outbound connection. The issue is fixed, but adding
				// defensive code above that if we did not register this connection
				// because we already have an outbound for this name, then
				// close this connection (and make sure it does not try to reconnect)
				c.mu.Lock()
				c.flags.set(noReconnect)
				c.mu.Unlock()
				c.closeConnection(WrongGateway)
				return
			}
		} else if info.GatewayCmd > 0 {
			switch info.GatewayCmd {
			case gatewayCmdAllSubsStart:
				c.gatewayAllSubsReceiveStart(info)
				return
			case gatewayCmdAllSubsComplete:
				c.gatewayAllSubsReceiveComplete(info)
				return
			default:
				s.Warnf("Received unknown command %v from gateway %q", info.GatewayCmd, gwName)
				return
			}
		}

		// Flood local cluster with information about this gateway.
		// Servers in this cluster will ensure that they have (or otherwise create)
		// an outbound connection to this gateway.
		s.forwardNewGatewayToLocalCluster(info)

	} else if isFirstINFO {
		// This is the first INFO of an inbound connection...

		s.registerInboundGatewayConnection(cid, c)
		c.Noticef("Inbound gateway connection from %q (%s) registered", info.Gateway, info.ID)

		// Now that it is registered, we can remove from temp map.
		s.removeFromTempClients(cid)

		// Send our QSubs.
		s.sendQueueSubsToGateway(c)

		// Initiate outbound connection. This function will behave correctly if
		// we have already one.
		s.processImplicitGateway(info)

		// Send back to the server that initiated this gateway connection the
		// list of all remote gateways known on this server.
		s.gossipGatewaysToInboundGateway(info.Gateway, c)
	}
}

// Sends to the given inbound gateway connection a gossip INFO protocol
// for each gateway known by this server. This allows for a "full mesh"
// of gateways.
func (s *Server) gossipGatewaysToInboundGateway(gwName string, c *client) {
	gw := s.gateway
	gw.RLock()
	defer gw.RUnlock()
	for gwCfgName, cfg := range gw.remotes {
		// Skip the gateway that we just created
		if gwCfgName == gwName {
			continue
		}
		info := Info{
			ID:         s.info.ID,
			GatewayCmd: gatewayCmdGossip,
		}
		urls := cfg.getURLsAsStrings()
		if len(urls) > 0 {
			info.Gateway = gwCfgName
			info.GatewayURLs = urls
			b, _ := json.Marshal(&info)
			c.mu.Lock()
			c.sendProto([]byte(fmt.Sprintf(InfoProto, b)), true)
			c.mu.Unlock()
		}
	}
}

// Sends the INFO protocol of a gateway to all routes known by this server.
func (s *Server) forwardNewGatewayToLocalCluster(oinfo *Info) {
	// Need to protect s.routes here, so use server's lock
	s.mu.Lock()
	defer s.mu.Unlock()

	// We don't really need the ID to be set, but, we need to make sure
	// that it is not set to the server ID so that if we were to connect
	// to an older server that does not expect a "gateway" INFO, it
	// would think that it needs to create an implicit route (since info.ID
	// would not match the route's remoteID), but will fail to do so because
	// the sent protocol will not have host/port defined.
	info := &Info{
		ID:          "GW" + s.info.ID,
		Gateway:     oinfo.Gateway,
		GatewayURLs: oinfo.GatewayURLs,
		GatewayCmd:  gatewayCmdGossip,
	}
	b, _ := json.Marshal(info)
	infoJSON := []byte(fmt.Sprintf(InfoProto, b))

	for _, r := range s.routes {
		r.mu.Lock()
		r.sendInfo(infoJSON)
		r.mu.Unlock()
	}
}

// Sends queue subscriptions interest to remote gateway.
// This is sent from the inbound side, that is, the side that receives
// messages from the remote's outbound connection. This side is
// the one sending the subscription interest.
func (s *Server) sendQueueSubsToGateway(c *client) {
	s.sendSubsToGateway(c, nil)
}

// Sends all subscriptions for the given account to the remove gateway
// This is sent from the inbound side, that is, the side that receives
// messages from the remote's outbound connection. This side is
// the one sending the subscription interest.
func (s *Server) sendAccountSubsToGateway(c *client, accName []byte) {
	s.sendSubsToGateway(c, accName)
}

// Sends subscriptions to remote gateway.
func (s *Server) sendSubsToGateway(c *client, accountName []byte) {
	var (
		bufa = [32 * 1024]byte{}
		buf  = bufa[:0]
		epa  = [1024]int{}
		ep   = epa[:0]
	)

	gw := s.gateway

	// This needs to run under this lock for the whole duration
	gw.pasi.Lock()
	defer gw.pasi.Unlock()

	// Build the protocols
	buildProto := func(accName []byte, acc map[string]*sitally, doQueues bool) {
		for saq, si := range acc {
			if doQueues && si.q || !doQueues && !si.q {
				buf = append(buf, rSubBytes...)
				buf = append(buf, accName...)
				buf = append(buf, ' ')
				// For queue subs (si.q is true), saq will be
				// subject + ' ' + queue, for plain subs, this is
				// just the subject.
				buf = append(buf, saq...)
				if doQueues {
					buf = append(buf, ' ', '1')
				}
				buf = append(buf, CR_LF...)
				ep = append(ep, len(buf))
			}
		}
	}
	// If account is specified...
	if accountName != nil {
		// Simply send all plain subs (no queues) for this specific account
		buildProto(accountName, gw.pasi.m[string(accountName)], false)
		// Instruct to send all subs (RS+/-) for this account from now on.
		c.mu.Lock()
		e := c.gw.insim[string(accountName)]
		if e == nil {
			e = &insie{}
			c.gw.insim[string(accountName)] = e
		}
		e.mode = InterestOnly
		c.mu.Unlock()
	} else {
		// Send queues for all accounts
		for accName, acc := range gw.pasi.m {
			buildProto([]byte(accName), acc, true)
		}
	}

	// Nothing to send.
	if len(buf) == 0 {
		return
	}
	if len(buf) > cap(bufa) {
		s.Debugf("Sending subscriptions to %q, buffer size: %v", c.gw.name, len(buf))
	}
	// Send
	mbs := gw.sqbsz
	mp := int(c.out.mp / 2)
	if mbs > mp {
		mbs = mp
	}
	le := 0
	li := 0
	for i := 0; i < len(ep); i++ {
		if ep[i]-le > mbs {
			var end int
			// If single proto is bigger than our max buffer size,
			// send anyway. If it reaches a max in queueOutbound,
			// that function will close the connection.
			if i-li <= 1 {
				end = ep[i]
			} else {
				end = ep[i-1]
				i--
			}
			c.mu.Lock()
			c.queueOutbound(buf[le:end])
			c.flushOutbound()
			closed := c.flags.isSet(clearConnection)
			c.mu.Unlock()
			if closed {
				return
			}
			le = ep[i]
			li = i
		}
	}
	c.mu.Lock()
	c.queueOutbound(buf[le:])
	c.flushOutbound()
	if !c.flags.isSet(clearConnection) {
		c.Debugf("Sent queue subscriptions to gateway")
	}
	c.mu.Unlock()
}

// This is invoked when getting an INFO protocol for gateway on the ROUTER port.
// This function will then execute appropriate function based on the command
// contained in the protocol.
// <Invoked from a route connection's readLoop>
func (s *Server) processGatewayInfoFromRoute(info *Info, routeSrvID string, route *client) {
	switch info.GatewayCmd {
	case gatewayCmdGossip:
		s.processImplicitGateway(info)
	default:
		s.Errorf("Unknown command %d from server %v", info.GatewayCmd, routeSrvID)
	}
}

// Sends INFO protocols to the given route connection for each known Gateway.
// These will be processed by the route and delegated to the gateway code to
// imvoke processImplicitGateway.
func (s *Server) sendGatewayConfigsToRoute(route *client) {
	gw := s.gateway
	gw.RLock()
	// Send only to gateways for which we have actual outbound connection to.
	if len(gw.out) == 0 {
		gw.RUnlock()
		return
	}
	// Collect gateway configs for which we have an outbound connection.
	gwCfgsa := [16]*gatewayCfg{}
	gwCfgs := gwCfgsa[:0]
	for _, c := range gw.out {
		c.mu.Lock()
		if c.gw.cfg != nil {
			gwCfgs = append(gwCfgs, c.gw.cfg)
		}
		c.mu.Unlock()
	}
	gw.RUnlock()
	if len(gwCfgs) == 0 {
		return
	}

	// Check forwardNewGatewayToLocalCluster() as to why we set ID this way.
	info := Info{
		ID:         "GW" + s.info.ID,
		GatewayCmd: gatewayCmdGossip,
	}
	for _, cfg := range gwCfgs {
		urls := cfg.getURLsAsStrings()
		if len(urls) > 0 {
			info.Gateway = cfg.Name
			info.GatewayURLs = urls
			b, _ := json.Marshal(&info)
			route.mu.Lock()
			route.sendProto([]byte(fmt.Sprintf(InfoProto, b)), true)
			route.mu.Unlock()
		}
	}
}

// Initiates a gateway connection using the info contained in the INFO protocol.
// If a gateway with the same name is already registered (either because explicitly
// configured, or already implicitly connected), this function will augmment the
// remote URLs with URLs present in the info protocol and return.
// Otherwise, this function will register this remote (to prevent multiple connections
// to the same remote) and call solicitGateway (which will run in a different go-routine).
func (s *Server) processImplicitGateway(info *Info) {
	s.gateway.Lock()
	defer s.gateway.Unlock()
	// Name of the gateway to connect to is the Info.Gateway field.
	gwName := info.Gateway
	// If this is our name, bail.
	if gwName == s.gateway.name {
		return
	}
	// Check if we already have this config, and if so, we are done
	cfg := s.gateway.remotes[gwName]
	if cfg != nil {
		// However, possibly augment the list of URLs with the given
		// info.GatewayURLs content.
		cfg.Lock()
		cfg.addURLs(info.GatewayURLs)
		cfg.Unlock()
		return
	}
	opts := s.getOpts()
	cfg = &gatewayCfg{
		RemoteGatewayOpts: &RemoteGatewayOpts{Name: gwName},
		replyPfx:          getReplyPrefixForGateway(gwName),
		urls:              make(map[string]*url.URL, len(info.GatewayURLs)),
		implicit:          true,
	}
	if opts.Gateway.TLSConfig != nil {
		cfg.TLSConfig = opts.Gateway.TLSConfig.Clone()
		cfg.TLSTimeout = opts.Gateway.TLSTimeout
	}

	// Since we know we don't have URLs (no config, so just based on what we
	// get from INFO), directly call addURLs(). We don't need locking since
	// we just created that structure and no one else has access to it yet.
	cfg.addURLs(info.GatewayURLs)
	// If there is no URL, we can't proceed.
	if len(cfg.urls) == 0 {
		return
	}
	s.gateway.remotes[gwName] = cfg
	s.startGoRoutine(func() {
		s.solicitGateway(cfg, true)
		s.grWG.Done()
	})
}

// NumOutboundGateways is public here mostly for testing.
func (s *Server) NumOutboundGateways() int {
	return s.numOutboundGateways()
}

// Returns the number of outbound gateway connections
func (s *Server) numOutboundGateways() int {
	s.gateway.RLock()
	n := len(s.gateway.out)
	s.gateway.RUnlock()
	return n
}

// Returns the number of inbound gateway connections
func (s *Server) numInboundGateways() int {
	s.gateway.RLock()
	n := len(s.gateway.in)
	s.gateway.RUnlock()
	return n
}

// Returns the remoteGateway (if any) that has the given `name`
func (s *Server) getRemoteGateway(name string) *gatewayCfg {
	s.gateway.RLock()
	cfg := s.gateway.remotes[name]
	s.gateway.RUnlock()
	return cfg
}

// Used in tests
func (g *gatewayCfg) bumpConnAttempts() {
	g.Lock()
	g.connAttempts++
	g.Unlock()
}

// Used in tests
func (g *gatewayCfg) getConnAttempts() int {
	g.Lock()
	ca := g.connAttempts
	g.Unlock()
	return ca
}

// Used in tests
func (g *gatewayCfg) resetConnAttempts() {
	g.Lock()
	g.connAttempts = 0
	g.Unlock()
}

// Returns if this remote gateway is implicit or not.
func (g *gatewayCfg) isImplicit() bool {
	g.RLock()
	ii := g.implicit
	g.RUnlock()
	return ii
}

// getURLs returns an array of URLs in random order suitable for
// an iteration to try to connect.
func (g *gatewayCfg) getURLs() []*url.URL {
	g.RLock()
	a := make([]*url.URL, 0, len(g.urls))
	for _, u := range g.urls {
		a = append(a, u)
	}
	g.RUnlock()
	// Map iteration is random, but not that good with small maps.
	rand.Shuffle(len(a), func(i, j int) {
		a[i], a[j] = a[j], a[i]
	})
	return a
}

// Similar to getURLs but returns the urls as an array of strings.
func (g *gatewayCfg) getURLsAsStrings() []string {
	g.RLock()
	a := make([]string, 0, len(g.urls))
	for _, u := range g.urls {
		a = append(a, u.Host)
	}
	g.RUnlock()
	return a
}

// updateURLs creates the urls map with the content of the config's URLs array
// and the given array that we get from the INFO protocol.
func (g *gatewayCfg) updateURLs(infoURLs []string) {
	g.Lock()
	// Clear the map...
	g.urls = make(map[string]*url.URL, len(g.URLs)+len(infoURLs))
	// Add the urls from the config URLs array.
	for _, u := range g.URLs {
		g.urls[u.Host] = u
	}
	// Then add the ones from the infoURLs array we got.
	g.addURLs(infoURLs)
	g.Unlock()
}

// Saves the hostname of the given URL (if not already done).
// This may be used as the ServerName of the TLSConfig when initiating a
// TLS connection.
// Write lock held on entry.
func (g *gatewayCfg) saveTLSHostname(u *url.URL) {
	if g.TLSConfig != nil && g.tlsName == "" && net.ParseIP(u.Hostname()) == nil {
		g.tlsName = u.Hostname()
	}
}

// add URLs from the given array to the urls map only if not already present.
// remoteGateway write lock is assumed to be held on entry.
// Write lock is held on entry.
func (g *gatewayCfg) addURLs(infoURLs []string) {
	var scheme string
	if g.TLSConfig != nil {
		scheme = "tls"
	} else {
		scheme = "nats"
	}
	for _, iu := range infoURLs {
		if _, present := g.urls[iu]; !present {
			// Urls in Info.GatewayURLs come without scheme. Add it to parse
			// the url (otherwise it fails).
			if u, err := url.Parse(fmt.Sprintf("%s://%s", scheme, iu)); err == nil {
				// Also, if a tlsName has not been set yet and we are dealing
				// with a hostname and not a bare IP, save the hostname.
				g.saveTLSHostname(u)
				// Use u.Host for the key.
				g.urls[u.Host] = u
				// Signal that we have updated the list. Used by monitoring code.
				g.varzUpdateURLs = true
			}
		}
	}
}

// Adds this URL to the set of Gateway URLs
// Server lock held on entry
func (s *Server) addGatewayURL(urlStr string) {
	s.gateway.Lock()
	s.gateway.URLs[urlStr] = struct{}{}
	s.gateway.Unlock()
}

// Remove this URL from the set of gateway URLs
// Server lock held on entry
func (s *Server) removeGatewayURL(urlStr string) {
	s.gateway.Lock()
	delete(s.gateway.URLs, urlStr)
	s.gateway.Unlock()
}

// This returns the URL of the Gateway listen spec, or empty string
// if the server has no gateway configured.
func (s *Server) getGatewayURL() string {
	s.gateway.RLock()
	url := s.gateway.URL
	s.gateway.RUnlock()
	return url
}

// Returns this server gateway name.
// Same than calling s.gateway.getName()
func (s *Server) getGatewayName() string {
	return s.gateway.getName()
}

// All gateway connections (outbound and inbound) are put in the given map.
func (s *Server) getAllGatewayConnections(conns map[uint64]*client) {
	gw := s.gateway
	gw.RLock()
	for _, c := range gw.out {
		c.mu.Lock()
		cid := c.cid
		c.mu.Unlock()
		conns[cid] = c
	}
	for cid, c := range gw.in {
		conns[cid] = c
	}
	gw.RUnlock()
}

// Register the given gateway connection (*client) in the inbound gateways
// map. The key is the connection ID (like for clients and routes).
func (s *Server) registerInboundGatewayConnection(cid uint64, gwc *client) {
	s.gateway.Lock()
	s.gateway.in[cid] = gwc
	s.gateway.Unlock()
}

// Register the given gateway connection (*client) in the outbound gateways
// map with the given name as the key.
func (s *Server) registerOutboundGatewayConnection(name string, gwc *client) bool {
	s.gateway.Lock()
	if _, exist := s.gateway.out[name]; exist {
		s.gateway.Unlock()
		return false
	}
	s.gateway.out[name] = gwc
	s.gateway.outo = append(s.gateway.outo, gwc)
	s.gateway.orderOutboundConnectionsLocked()
	s.gateway.Unlock()
	return true
}

// Returns the outbound gateway connection (*client) with the given name,
// or nil if not found
func (s *Server) getOutboundGatewayConnection(name string) *client {
	s.gateway.RLock()
	gwc := s.gateway.out[name]
	s.gateway.RUnlock()
	return gwc
}

// Returns all outbound gateway connections in the provided array.
// The order of the gateways is suited for the sending of a message.
// Current ordering is based on individual gateway's RTT value.
func (s *Server) getOutboundGatewayConnections(a *[]*client) {
	s.gateway.RLock()
	for i := 0; i < len(s.gateway.outo); i++ {
		*a = append(*a, s.gateway.outo[i])
	}
	s.gateway.RUnlock()
}

// Orders the array of outbound connections.
// Current ordering is by lowest RTT.
// Gateway write lock is held on entry
func (g *srvGateway) orderOutboundConnectionsLocked() {
	// Order the gateways by lowest RTT
	sort.Slice(g.outo, func(i, j int) bool {
		return g.outo[i].getRTTValue() < g.outo[j].getRTTValue()
	})
}

// Orders the array of outbound connections.
// Current ordering is by lowest RTT.
func (g *srvGateway) orderOutboundConnections() {
	g.Lock()
	g.orderOutboundConnectionsLocked()
	g.Unlock()
}

// Returns all inbound gateway connections in the provided array
func (s *Server) getInboundGatewayConnections(a *[]*client) {
	s.gateway.RLock()
	for _, gwc := range s.gateway.in {
		*a = append(*a, gwc)
	}
	s.gateway.RUnlock()
}

// This is invoked when a gateway connection is closed and the server
// is removing this connection from its state.
func (s *Server) removeRemoteGatewayConnection(c *client) {
	c.mu.Lock()
	cid := c.cid
	isOutbound := c.gw.outbound
	gwName := c.gw.name
	c.mu.Unlock()

	gw := s.gateway
	gw.Lock()
	if isOutbound {
		delete(gw.out, gwName)
		louto := len(gw.outo)
		reorder := false
		for i := 0; i < len(gw.outo); i++ {
			if gw.outo[i] == c {
				// If last, simply remove and no need to reorder
				if i != louto-1 {
					gw.outo[i] = gw.outo[louto-1]
					reorder = true
				}
				gw.outo = gw.outo[:louto-1]
			}
		}
		if reorder {
			gw.orderOutboundConnectionsLocked()
		}
	} else {
		delete(gw.in, cid)
	}
	gw.Unlock()
	s.removeFromTempClients(cid)

	if isOutbound {
		// Update number of totalQSubs for this gateway
		qSubsRemoved := int64(0)
		c.mu.Lock()
		for _, sub := range c.subs {
			if sub.queue != nil {
				qSubsRemoved++
			}
		}
		c.mu.Unlock()
		// Update total count of qsubs in remote gateways.
		atomic.AddInt64(&c.srv.gateway.totalQSubs, -qSubsRemoved)

	} else {
		var subsa [1024]*subscription
		var subs = subsa[:0]

		// For inbound GW connection, if we have subs, those are
		// local subs on "_R_." subjects.
		c.mu.Lock()
		for _, sub := range c.subs {
			subs = append(subs, sub)
		}
		c.mu.Unlock()
		for _, sub := range subs {
			c.removeReplySub(sub)
		}
	}
}

// GatewayAddr returns the net.Addr object for the gateway listener.
func (s *Server) GatewayAddr() *net.TCPAddr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gatewayListener == nil {
		return nil
	}
	return s.gatewayListener.Addr().(*net.TCPAddr)
}

// A- protocol received from the remote after sending messages
// on an account that it has no interest in. Mark this account
// with a "no interest" marker to prevent further messages send.
// <Invoked from outbound connection's readLoop>
func (c *client) processGatewayAccountUnsub(accName string) {
	// Just to indicate activity around "subscriptions" events.
	c.in.subs++
	// This account may have an entry because of queue subs.
	// If that's the case, we can reset the no-interest map,
	// but not set the entry to nil.
	setToNil := true
	if ei, ok := c.gw.outsim.Load(accName); ei != nil {
		e := ei.(*outsie)
		e.Lock()
		// Reset the no-interest map if we have queue subs
		// and don't set the entry to nil.
		if e.qsubs > 0 {
			e.ni = make(map[string]struct{})
			setToNil = false
		}
		e.Unlock()
	} else if ok {
		// Already set to nil, so skip
		setToNil = false
	}
	if setToNil {
		c.gw.outsim.Store(accName, nil)
	}
}

// A+ protocol received from remote gateway if it had previously
// sent an A-. Clear the "no interest" marker for this account.
// <Invoked from outbound connection's readLoop>
func (c *client) processGatewayAccountSub(accName string) error {
	// Just to indicate activity around "subscriptions" events.
	c.in.subs++
	// If this account has an entry because of queue subs, we
	// can't delete the entry.
	remove := true
	if ei, ok := c.gw.outsim.Load(accName); ei != nil {
		e := ei.(*outsie)
		e.Lock()
		if e.qsubs > 0 {
			remove = false
		}
		e.Unlock()
	} else if !ok {
		// There is no entry, so skip
		remove = false
	}
	if remove {
		c.gw.outsim.Delete(accName)
	}
	return nil
}

// RS- protocol received from the remote after sending messages
// on a subject that it has no interest in (but knows about the
// account). Mark this subject with a "no interest" marker to
// prevent further messages being sent.
// If in modeInterestOnly or for a queue sub, remove from
// the sublist if present.
// <Invoked from outbound connection's readLoop>
func (c *client) processGatewayRUnsub(arg []byte) error {
	accName, subject, queue, err := c.parseUnsubProto(arg)
	if err != nil {
		return fmt.Errorf("processGatewaySubjectUnsub %s", err.Error())
	}

	var (
		e          *outsie
		useSl      bool
		newe       bool
		callUpdate bool
		srv        *Server
		sub        *subscription
	)

	// Possibly execute this on exit after all locks have been released.
	// If callUpdate is true, srv and sub will be not nil.
	defer func() {
		if callUpdate {
			srv.updateInterestForAccountOnGateway(accName, sub, -1)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	ei, _ := c.gw.outsim.Load(accName)
	if ei != nil {
		e = ei.(*outsie)
		e.Lock()
		defer e.Unlock()
		// If there is an entry, for plain sub we need
		// to know if we should store the sub
		useSl = queue != nil || e.mode != Optimistic
	} else if queue != nil {
		// should not even happen...
		c.Debugf("Received RS- without prior RS+ for subject %q, queue %q", subject, queue)
		return nil
	} else {
		// Plain sub, assume optimistic sends, create entry.
		e = &outsie{ni: make(map[string]struct{}), sl: NewSublistWithCache()}
		newe = true
	}
	// This is when a sub or queue sub is supposed to be in
	// the sublist. Look for it and remove.
	if useSl {
		var ok bool
		key := arg
		// m[string()] does not cause mem allocation
		sub, ok = c.subs[string(key)]
		// if RS- for a sub that we don't have, just ignore.
		if !ok {
			return nil
		}
		if e.sl.Remove(sub) == nil {
			delete(c.subs, string(key))
			if queue != nil {
				e.qsubs--
				atomic.AddInt64(&c.srv.gateway.totalQSubs, -1)
			}
			// If last, we can remove the whole entry only
			// when in optimistic mode and there is no element
			// in the `ni` map.
			if e.sl.Count() == 0 && e.mode == Optimistic && len(e.ni) == 0 {
				c.gw.outsim.Delete(accName)
			}
		}
		// We are going to call updateInterestForAccountOnGateway on exit.
		srv = c.srv
		callUpdate = true
	} else {
		e.ni[string(subject)] = struct{}{}
		if newe {
			c.gw.outsim.Store(accName, e)
		}
	}
	return nil
}

// For plain subs, RS+ protocol received from remote gateway if it
// had previously sent a RS-. Clear the "no interest" marker for
// this subject (under this account).
// For queue subs, or if in modeInterestOnly, register interest
// from remote gateway.
// <Invoked from outbound connection's readLoop>
func (c *client) processGatewayRSub(arg []byte) error {
	c.traceInOp("RS+", arg)

	// Indicate activity.
	c.in.subs++

	var (
		queue []byte
		qw    int32
	)

	args := splitArg(arg)
	switch len(args) {
	case 2:
	case 4:
		queue = args[2]
		qw = int32(parseSize(args[3]))
	default:
		return fmt.Errorf("processGatewaySubjectSub Parse Error: '%s'", arg)
	}
	accName := args[0]
	subject := args[1]

	var (
		e          *outsie
		useSl      bool
		newe       bool
		callUpdate bool
		srv        *Server
		sub        *subscription
	)

	// Possibly execute this on exit after all locks have been released.
	// If callUpdate is true, srv and sub will be not nil.
	defer func() {
		if callUpdate {
			srv.updateInterestForAccountOnGateway(string(accName), sub, 1)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	ei, _ := c.gw.outsim.Load(string(accName))
	// We should always have an existing entry for plain subs because
	// in optimistic mode we would have received RS- first, and
	// in full knowledge, we are receiving RS+ for an account after
	// getting many RS- from the remote..
	if ei != nil {
		e = ei.(*outsie)
		e.Lock()
		defer e.Unlock()
		useSl = queue != nil || e.mode != Optimistic
	} else if queue == nil {
		return nil
	} else {
		e = &outsie{ni: make(map[string]struct{}), sl: NewSublistWithCache()}
		newe = true
		useSl = true
	}
	if useSl {
		var key []byte
		// We store remote subs by account/subject[/queue].
		// For queue, remove the trailing weight
		if queue != nil {
			key = arg[:len(arg)-len(args[3])-1]
		} else {
			key = arg
		}
		// If RS+ for a sub that we already have, ignore.
		// (m[string()] does not allocate memory)
		if _, ok := c.subs[string(key)]; ok {
			return nil
		}
		// new subscription. copy subject (and queue) to
		// not reference the underlying possibly big buffer.
		var csubject []byte
		var cqueue []byte
		if queue != nil {
			// make single allocation and use different slices
			// to point to subject and queue name.
			cbuf := make([]byte, len(subject)+1+len(queue))
			copy(cbuf, key[len(accName)+1:])
			csubject = cbuf[:len(subject)]
			cqueue = cbuf[len(subject)+1:]
		} else {
			csubject = make([]byte, len(subject))
			copy(csubject, subject)
		}
		sub = &subscription{client: c, subject: csubject, queue: cqueue, qw: qw}
		// If no error inserting in sublist...
		if e.sl.Insert(sub) == nil {
			c.subs[string(key)] = sub
			if queue != nil {
				e.qsubs++
				atomic.AddInt64(&c.srv.gateway.totalQSubs, 1)
			}
			if newe {
				c.gw.outsim.Store(string(accName), e)
			}
		}
		// We are going to call updateInterestForAccountOnGateway on exit.
		srv = c.srv
		callUpdate = true
	} else {
		subj := string(subject)
		// If this is an RS+ for a wc subject, then
		// remove from the no interest map all subjects
		// that are a subset of this wc subject.
		if subjectHasWildcard(subj) {
			for k := range e.ni {
				if subjectIsSubsetMatch(k, subj) {
					delete(e.ni, k)
				}
			}
		} else {
			delete(e.ni, subj)
		}
	}
	return nil
}

// Returns true if this gateway has possible interest in the
// given account/subject (which means, it does not have a registered
// no-interest on the account and/or subject) and the sublist result
// for queue subscriptions.
// <Outbound connection: invoked when client message is published,
// so from any client connection's readLoop>
func (c *client) gatewayInterest(acc, subj string) (bool, *SublistResult) {
	ei, accountInMap := c.gw.outsim.Load(acc)
	// If there is an entry for this account and ei is nil,
	// it means that the remote is not interested at all in
	// this account and we could not possibly have queue subs.
	if accountInMap && ei == nil {
		return false, nil
	}
	// Assume interest if account not in map.
	psi := !accountInMap
	var r *SublistResult
	if accountInMap {
		// If in map, check for subs interest with sublist.
		e := ei.(*outsie)
		e.RLock()
		// We may be in transition to modeInterestOnly
		// but until e.ni is nil, use it to know if we
		// should suppress interest or not.
		if e.ni != nil {
			if _, inMap := e.ni[subj]; !inMap {
				psi = true
			}
		}
		// If we are in modeInterestOnly (e.ni will be nil)
		// or if we have queue subs, we also need to check sl.Match.
		if e.ni == nil || e.qsubs > 0 {
			r = e.sl.Match(subj)
			if len(r.psubs) > 0 {
				psi = true
			}
		}
		e.RUnlock()
	}
	return psi, r
}

// switchAccountToInterestMode will switch an account over to interestMode.
// Lock should NOT be held.
func (s *Server) switchAccountToInterestMode(accName string) {
	gwsa := [16]*client{}
	gws := gwsa[:0]
	s.getInboundGatewayConnections(&gws)

	for _, gin := range gws {
		var e *insie
		var ok bool

		gin.mu.Lock()
		if e, ok = gin.gw.insim[accName]; !ok || e == nil {
			e = &insie{}
			gin.gw.insim[accName] = e
		}
		// Do it only if we are in Optimistic mode
		if e.mode == Optimistic {
			gin.gatewaySwitchAccountToSendAllSubs(e, accName)
		}
		gin.mu.Unlock()
	}
}

// This is invoked when registering (or unregistering) the first
// (or last) subscription on a given account/subject. For each
// GWs inbound connections, we will check if we need to send an RS+ or A+
// protocol.
func (s *Server) maybeSendSubOrUnsubToGateways(accName string, sub *subscription, added bool) {
	if sub.queue != nil {
		return
	}
	gwsa := [16]*client{}
	gws := gwsa[:0]
	s.getInboundGatewayConnections(&gws)
	if len(gws) == 0 {
		return
	}
	var (
		rsProtoa  [512]byte
		rsProto   []byte
		accProtoa [256]byte
		accProto  []byte
		proto     []byte
		subject   = string(sub.subject)
		hasWc     = subjectHasWildcard(subject)
	)
	for _, c := range gws {
		proto = nil
		c.mu.Lock()
		e, inMap := c.gw.insim[accName]
		// If there is a inbound subject interest entry...
		if e != nil {
			sendProto := false
			// In optimistic mode, we care only about possibly sending RS+ (or A+)
			// so if e.ni is not nil we do things only when adding a new subscription.
			if e.ni != nil && added {
				// For wildcard subjects, we will remove from our no-interest
				// map, all subjects that are a subset of this wc subject, but we
				// still send the wc subject and let the remote do its own cleanup.
				if hasWc {
					for enis := range e.ni {
						if subjectIsSubsetMatch(enis, subject) {
							delete(e.ni, enis)
							sendProto = true
						}
					}
				} else if _, noInterest := e.ni[subject]; noInterest {
					delete(e.ni, subject)
					sendProto = true
				}
			} else if e.mode == InterestOnly {
				// We are in the mode where we always send RS+/- protocols.
				sendProto = true
			}
			if sendProto {
				if rsProto == nil {
					// Construct the RS+/- only once
					proto = rsProtoa[:0]
					if added {
						proto = append(proto, rSubBytes...)
					} else {
						proto = append(proto, rUnsubBytes...)
					}
					proto = append(proto, accName...)
					proto = append(proto, ' ')
					proto = append(proto, sub.subject...)
					proto = append(proto, CR_LF...)
					rsProto = proto
				} else {
					// Point to the already constructed RS+/-
					proto = rsProto
				}
			}
		} else if added && inMap {
			// Here, we have a `nil` entry for this account in
			// the map, which means that we have previously sent
			// an A-. We have a new subscription, so we need to
			// send an A+ and delete the entry from the map so
			// that we do this only once.
			delete(c.gw.insim, accName)
			if accProto == nil {
				// Construct the A+ only once
				proto = accProtoa[:0]
				proto = append(proto, aSubBytes...)
				proto = append(proto, accName...)
				proto = append(proto, CR_LF...)
				accProto = proto
			} else {
				// Point to the already constructed A+
				proto = accProto
			}
		}
		if proto != nil {
			c.sendProto(proto, false)
			if c.trace {
				c.traceOutOp("", proto[:len(proto)-LEN_CR_LF])
			}
		}
		c.mu.Unlock()
	}
}

// This is invoked when the first (or last) queue subscription on a
// given subject/group is registered (or unregistered). Sent to all
// inbound gateways.
func (s *Server) sendQueueSubOrUnsubToGateways(accName string, qsub *subscription, added bool) {
	if qsub.queue == nil {
		return
	}

	gwsa := [16]*client{}
	gws := gwsa[:0]
	s.getInboundGatewayConnections(&gws)
	if len(gws) == 0 {
		return
	}

	var protoa [512]byte
	var proto []byte
	for _, c := range gws {
		if proto == nil {
			proto = protoa[:0]
			if added {
				proto = append(proto, rSubBytes...)
			} else {
				proto = append(proto, rUnsubBytes...)
			}
			proto = append(proto, accName...)
			proto = append(proto, ' ')
			proto = append(proto, qsub.subject...)
			proto = append(proto, ' ')
			proto = append(proto, qsub.queue...)
			if added {
				// For now, just use 1 for the weight
				proto = append(proto, ' ', '1')
			}
			proto = append(proto, CR_LF...)
		}
		c.mu.Lock()
		// If we add a queue sub, and we had previously sent an A-,
		// we don't need to send an A+ here, but we need to clear
		// the fact that we did sent the A- so that we don't send
		// an A+ when we will get the first non-queue sub registered.
		if added {
			if ei, ok := c.gw.insim[accName]; ok && ei == nil {
				delete(c.gw.insim, accName)
			}
		}
		c.sendProto(proto, false)
		if c.trace {
			c.traceOutOp("", proto[:len(proto)-LEN_CR_LF])
		}
		c.mu.Unlock()
	}
}

// This is invoked when a subscription (plain or queue) is
// added/removed locally or in our cluster. We use ref counting
// to know when to update the inbound gateways.
// <Invoked from client or route connection's readLoop or when such
// connection is closed>
func (s *Server) gatewayUpdateSubInterest(accName string, sub *subscription, change int32) {
	var (
		keya  [1024]byte
		key   = keya[:0]
		entry *sitally
		isNew bool
	)

	s.gateway.pasi.Lock()
	defer s.gateway.pasi.Unlock()

	accMap := s.gateway.pasi.m

	// First see if we have the account
	st := accMap[accName]
	if st == nil {
		// Ignore remove of something we don't have
		if change < 0 {
			return
		}
		st = make(map[string]*sitally)
		accMap[accName] = st
		isNew = true
	}
	// Lookup: build the key as subject[+' '+queue]
	key = append(key, sub.subject...)
	if sub.queue != nil {
		key = append(key, ' ')
		key = append(key, sub.queue...)
	}
	if !isNew {
		entry = st[string(key)]
	}
	first := false
	last := false
	if entry == nil {
		// Ignore remove of something we don't have
		if change < 0 {
			return
		}
		entry = &sitally{n: 1, q: sub.queue != nil}
		st[string(key)] = entry
		first = true
	} else {
		entry.n += change
		if entry.n <= 0 {
			delete(st, string(key))
			last = true
			if len(st) == 0 {
				delete(accMap, accName)
			}
		}
	}
	if first || last {
		if sub.client != nil {
			rsubs := &s.gateway.rsubs
			c := sub.client
			sli, _ := rsubs.Load(c)
			if first {
				var sl *Sublist
				if sli == nil {
					sl = NewSublistNoCache()
					rsubs.Store(c, sl)
				} else {
					sl = sli.(*Sublist)
				}
				sl.Insert(sub)
				time.AfterFunc(s.gateway.recSubExp, func() {
					sl.Remove(sub)
				})
			} else if sli != nil {
				sl := sli.(*Sublist)
				sl.Remove(sub)
				if sl.Count() == 0 {
					rsubs.Delete(c)
				}
			}
		}
		if entry.q {
			s.sendQueueSubOrUnsubToGateways(accName, sub, first)
		} else {
			s.maybeSendSubOrUnsubToGateways(accName, sub, first)
		}
	}
}

// Returns true if the given subject starts with `$GR.`
func subjectStartsWithGatewayReplyPrefix(subj []byte) bool {
	return len(subj) > gwReplyStart && string(subj[:len(gwReplyPrefix)]) == gwReplyPrefix
}

// Evaluates if the given reply should be mapped (adding the origin cluster
// hash as a prefix) or not.
func (g *srvGateway) shouldMapReplyForGatewaySend(c *client, reply []byte) bool {
	sli, _ := g.rsubs.Load(c)
	if sli == nil {
		return false
	}
	sl := sli.(*Sublist)
	if sl.Count() == 0 {
		return false
	}
	if subjectStartsWithGatewayReplyPrefix(reply) {
		return false
	}
	r := sl.Match(string(reply))
	return len(r.psubs)+len(r.qsubs) > 0
}

var subPool = &sync.Pool{
	New: func() interface{} {
		return &subscription{}
	},
}

// May send a message to all outbound gateways. It is possible
// that the message is not sent to a given gateway if for instance
// it is known that this gateway has no interest in the account or
// subject, etc..
// <Invoked from any client connection's readLoop>
func (c *client) sendMsgToGateways(acc *Account, msg, subject, reply []byte, qgroups [][]byte) {
	gwsa := [16]*client{}
	gws := gwsa[:0]
	// This is in fast path, so avoid calling function when possible.
	// Get the outbound connections in place instead of calling
	// getOutboundGatewayConnections().
	gw := c.srv.gateway
	gw.RLock()
	for i := 0; i < len(gw.outo); i++ {
		gws = append(gws, gw.outo[i])
	}
	thisClusterReplyPrefix := gw.replyPfx
	gw.RUnlock()
	if len(gws) == 0 {
		return
	}
	var (
		subj       = string(subject)
		queuesa    = [512]byte{}
		queues     = queuesa[:0]
		accName    = acc.Name
		mreplya    [256]byte
		mreply     []byte
		dstPfx     []byte
		checkReply = len(reply) > 0
	)

	// Get a subscription from the pool
	sub := subPool.Get().(*subscription)

	// Make sure we are an 'R' proto
	c.msgb[0] = 'R'

	// Check if the subject is on "$GR.<cluster hash>.",
	// and if so, send to that GW regardless of its
	// interest on the real subject (that is, skip the
	// check of subject interest).
	if subjectStartsWithGatewayReplyPrefix(subject) {
		dstPfx = subject[:gwReplyStart]
	}
	for i := 0; i < len(gws); i++ {
		gwc := gws[i]
		if dstPfx != nil {
			gwc.mu.Lock()
			ok := gwc.gw.cfg != nil && bytes.Equal(dstPfx, gwc.gw.cfg.replyPfx)
			gwc.mu.Unlock()
			if !ok {
				continue
			}
		} else {
			// Plain sub interest and queue sub results for this account/subject
			psi, qr := gwc.gatewayInterest(accName, subj)
			if !psi && qr == nil {
				continue
			}
			queues = queuesa[:0]
			if qr != nil {
				for i := 0; i < len(qr.qsubs); i++ {
					qsubs := qr.qsubs[i]
					if len(qsubs) > 0 {
						queue := qsubs[0].queue
						add := true
						for _, qn := range qgroups {
							if bytes.Equal(queue, qn) {
								add = false
								break
							}
						}
						if add {
							qgroups = append(qgroups, queue)
							queues = append(queues, queue...)
							queues = append(queues, ' ')
						}
					}
				}
			}
			if !psi && len(queues) == 0 {
				continue
			}
		}
		if checkReply {
			// Check/map only once
			checkReply = false
			// Assume we will use original
			mreply = reply
			// If there was a recent matching subscription on that connection
			// and the reply is not already mapped, then map (add prefix).
			if gw.shouldMapReplyForGatewaySend(c, reply) {
				mreply = mreplya[:0]
				mreply = append(mreply, thisClusterReplyPrefix...)
				mreply = append(mreply, reply...)
			}
		}
		mh := c.msgb[:msgHeadProtoLen]
		mh = append(mh, accName...)
		mh = append(mh, ' ')
		mh = append(mh, subject...)
		mh = append(mh, ' ')
		if len(queues) > 0 {
			if reply != nil {
				mh = append(mh, "+ "...) // Signal that there is a reply.
				mh = append(mh, mreply...)
				mh = append(mh, ' ')
			} else {
				mh = append(mh, "| "...) // Only queues
			}
			mh = append(mh, queues...)
		} else if reply != nil {
			mh = append(mh, mreply...)
			mh = append(mh, ' ')
		}
		mh = append(mh, c.pa.szb...)
		mh = append(mh, CR_LF...)

		// We reuse the subscription object that we pass to deliverMsg.
		// So set/reset important fields.
		sub.nm, sub.max = 0, 0
		sub.client = gwc
		sub.subject = c.pa.subject
		c.deliverMsg(sub, mh, msg)
	}
	// Done with subscription, put back to pool. We don't need
	// to reset content since we explicitly set when using it.
	subPool.Put(sub)
}

func (s *Server) gatewayHandleServiceImport(acc *Account, subject []byte, c *client, change int32) {
	sid := make([]byte, 0, len(acc.Name)+len(subject)+1)
	sid = append(sid, acc.Name...)
	sid = append(sid, ' ')
	sid = append(sid, subject...)
	sub := &subscription{client: c, subject: subject, sid: sid}

	var rspa [1024]byte
	rsproto := rspa[:0]
	if change > 0 {
		rsproto = append(rsproto, rSubBytes...)
	} else {
		rsproto = append(rsproto, rUnsubBytes...)
	}
	rsproto = append(rsproto, ' ')
	rsproto = append(rsproto, sid...)
	rsproto = append(rsproto, CR_LF...)

	s.mu.Lock()
	for _, r := range s.routes {
		r.mu.Lock()
		r.sendProto(rsproto, false)
		if r.trace {
			r.traceOutOp("", rsproto[:len(rsproto)-LEN_CR_LF])
		}
		r.mu.Unlock()
	}
	s.mu.Unlock()
	// Possibly send RS+ to gateways too.
	s.gatewayUpdateSubInterest(acc.Name, sub, change)
}

// Possibly sends an A- to the remote gateway `c`.
// Invoked when processing an inbound message and the account is not found.
// A check under a lock that protects processing of SUBs and UNSUBs is
// done to make sure that we don't send the A- if a subscription has just
// been created at the same time, which would otherwise results in the
// remote never sending messages on this account until a new subscription
// is created.
func (s *Server) gatewayHandleAccountNoInterest(c *client, accName []byte) {
	// Check and possibly send the A- under this lock.
	s.gateway.pasi.Lock()
	defer s.gateway.pasi.Unlock()

	si, inMap := s.gateway.pasi.m[string(accName)]
	if inMap && si != nil && len(si) > 0 {
		return
	}
	c.sendAccountUnsubToGateway(accName)
}

// Helper that sends an A- to this remote gateway if not already done.
// This function should not be invoked directly but instead be invoked
// by functions holding the gateway.pasi's Lock.
func (c *client) sendAccountUnsubToGateway(accName []byte) {
	// Check if we have sent the A- or not.
	c.mu.Lock()
	e, sent := c.gw.insim[string(accName)]
	if e != nil || !sent {
		// Add a nil value to indicate that we have sent an A-
		// so that we know to send A+ when needed.
		c.gw.insim[string(accName)] = nil
		var protoa [256]byte
		proto := protoa[:0]
		proto = append(proto, aUnsubBytes...)
		proto = append(proto, accName...)
		proto = append(proto, CR_LF...)
		c.sendProto(proto, false)
		if c.trace {
			c.traceOutOp("", proto[:len(proto)-LEN_CR_LF])
		}
	}
	c.mu.Unlock()
}

// Possibly sends an A- for this account or RS- for this subject.
// Invoked when processing an inbound message and the account is found
// but there is no interest on this subject.
// A test is done under a lock that protects processing of SUBs and UNSUBs
// and if there is no subscription at this time, we send an A-. If there
// is at least a subscription, but no interest on this subject, we send
// an RS- for this subject (if not already done).
func (s *Server) gatewayHandleSubjectNoInterest(c *client, acc *Account, accName, subject []byte) {
	s.gateway.pasi.Lock()
	defer s.gateway.pasi.Unlock()

	// If there is at least a subscription, possibly send RS-
	if acc.sl.Count() != 0 {
		sendProto := false
		c.mu.Lock()
		// Send an RS- protocol if not already done and only if
		// not in the modeInterestOnly.
		e := c.gw.insim[string(accName)]
		if e == nil {
			e = &insie{ni: make(map[string]struct{})}
			e.ni[string(subject)] = struct{}{}
			c.gw.insim[string(accName)] = e
			sendProto = true
		} else if e.ni != nil {
			// If we are not in modeInterestOnly, check if we
			// have already sent an RS-
			if _, alreadySent := e.ni[string(subject)]; !alreadySent {
				// TODO(ik): pick some threshold as to when
				// we need to switch mode
				if len(e.ni) >= gatewayMaxRUnsubBeforeSwitch {
					// If too many RS-, switch to all-subs-mode.
					c.gatewaySwitchAccountToSendAllSubs(e, string(accName))
				} else {
					e.ni[string(subject)] = struct{}{}
					sendProto = true
				}
			}
		}
		if sendProto {
			var (
				protoa = [512]byte{}
				proto  = protoa[:0]
			)
			proto = append(proto, rUnsubBytes...)
			proto = append(proto, accName...)
			proto = append(proto, ' ')
			proto = append(proto, subject...)
			proto = append(proto, CR_LF...)
			c.sendProto(proto, false)
			if c.trace {
				c.traceOutOp("", proto[:len(proto)-LEN_CR_LF])
			}
		}
		c.mu.Unlock()
	} else {
		// There is not a single subscription, send an A- (if not already done).
		c.sendAccountUnsubToGateway([]byte(acc.Name))
	}
}

func (g *srvGateway) getReplyPrefix() []byte {
	g.RLock()
	replyPfx := g.replyPfx
	g.RUnlock()
	return replyPfx
}

func (s *Server) isGatewayReplyForThisCluster(subj []byte) bool {
	return string(s.gateway.getReplyPrefix()) == string(subj[:gwReplyStart])
}

// Process a message coming from a remote gateway. Send to any sub/qsub
// in our cluster that is matching. When receiving a message for an
// account or subject for which there is no interest in this cluster
// an A-/RS- protocol may be send back.
// <Invoked from inbound connection's readLoop>
func (c *client) processInboundGatewayMsg(msg []byte) {
	// Update statistics
	c.in.msgs++
	// The msg includes the CR_LF, so pull back out for accounting.
	c.in.bytes += int32(len(msg) - LEN_CR_LF)

	if c.trace {
		c.traceMsg(msg)
	}

	if c.opts.Verbose {
		c.sendOK()
	}

	// Mostly under testing scenarios.
	if c.srv == nil {
		return
	}

	// If we receive a message on $GR.<cluster>.<subj>
	// we will drop the prefix before processing interest
	// in this cluster, but we also need to resend to
	// other gateways.
	sendBackToGateways := false

	// First thing to do is to check if the subject starts
	// with "$GR.<hash>.".
	if subjectStartsWithGatewayReplyPrefix(c.pa.subject) {
		// If it does, then is this server/cluster the actual
		// destination for this message?
		if !c.srv.isGatewayReplyForThisCluster(c.pa.subject) {
			// We could report, for now, just drop.
			return
		}
		// Adjust the subject to past the prefix
		c.pa.subject = c.pa.subject[gwReplyStart:]
		// Use a stack buffer to rewrite c.pa.cache since we
		// only need it for getAccAndResultFromCache()
		var _pacache [256]byte
		pacache := _pacache[:0]
		pacache = append(pacache, c.pa.account...)
		pacache = append(pacache, ' ')
		pacache = append(pacache, c.pa.subject...)
		c.pa.pacache = pacache
		sendBackToGateways = true
	}

	acc, r := c.getAccAndResultFromCache()
	if acc == nil {
		c.Debugf("Unknown account %q for gateway message on subject: %q", c.pa.account, c.pa.subject)
		c.srv.gatewayHandleAccountNoInterest(c, c.pa.account)
		return
	}

	// Check to see if we need to map/route to another account.
	if acc.imports.services != nil && isServiceReply(c.pa.subject) {
		// We are handling a response to a request that we mapped
		// via service imports, so if we are here we are the
		// origin server
		c.checkForImportServices(acc, msg)
	}

	if !sendBackToGateways {
		// If there is no interest on plain subs, possibly send an RS-,
		// even if there is qsubs interest.
		if len(r.psubs) == 0 {
			c.srv.gatewayHandleSubjectNoInterest(c, acc, c.pa.account, c.pa.subject)

			// If there is also no queue filter, then no point in continuing
			// (even if r.qsubs i > 0).
			if len(c.pa.queues) == 0 {
				return
			}
		}
		c.processMsgResults(acc, r, msg, c.pa.subject, c.pa.reply, pmrNoFlag)
	} else {
		// We normally would not allow sending to a queue unless the
		// RMSG contains the queue groups, however, if the incoming
		// message was a "$GR." then we need to act as if this was
		// a CLIENT connection..
		qnames := c.processMsgResults(acc, r, msg, c.pa.subject, c.pa.reply,
			pmrCollectQueueNames|pmrTreatGatewayAsClient)
		c.sendMsgToGateways(c.acc, msg, c.pa.subject, c.pa.reply, qnames)
	}
}

// Indicates that the remote which we are sending messages to
// has decided to send us all its subs interest so that we
// stop doing optimistic sends.
// <Invoked from outbound connection's readLoop>
func (c *client) gatewayAllSubsReceiveStart(info *Info) {
	account := getAccountFromGatewayCommand(c, info, "start")
	if account == "" {
		return
	}

	c.Noticef("Gateway %q: switching account %q to %s mode",
		info.Gateway, account, InterestOnly)

	// Since the remote would send us this start command
	// only after sending us too many RS- for this account,
	// we should always have an entry here.
	// TODO(ik): Should we close connection with protocol violation
	// error if that happens?
	ei, _ := c.gw.outsim.Load(account)
	if ei != nil {
		e := ei.(*outsie)
		e.Lock()
		e.mode = Transitioning
		e.Unlock()
	} else {
		e := &outsie{sl: NewSublistWithCache()}
		e.mode = Transitioning
		c.mu.Lock()
		c.gw.outsim.Store(account, e)
		c.mu.Unlock()
	}
}

// Indicates that the remote has finished sending all its
// subscriptions and we should now not send unless we know
// there is explicit interest.
// <Invoked from outbound connection's readLoop>
func (c *client) gatewayAllSubsReceiveComplete(info *Info) {
	account := getAccountFromGatewayCommand(c, info, "complete")
	if account == "" {
		return
	}
	// Done receiving all subs from remote. Set the `ni`
	// map to nil so that gatewayInterest() no longer
	// uses it.
	ei, _ := c.gw.outsim.Load(string(account))
	if ei != nil {
		e := ei.(*outsie)
		// Needs locking here since `ni` is checked by
		// many go-routines calling gatewayInterest()
		e.Lock()
		e.ni = nil
		e.mode = InterestOnly
		e.Unlock()

		c.Noticef("Gateway %q: switching account %q to %s mode complete",
			info.Gateway, account, InterestOnly)
	}
}

// small helper to get the account name from the INFO command.
func getAccountFromGatewayCommand(c *client, info *Info, cmd string) string {
	if info.GatewayCmdPayload == nil {
		c.sendErrAndErr(fmt.Sprintf("Account absent from receive-all-subscriptions-%s command", cmd))
		c.closeConnection(ProtocolViolation)
		return ""
	}
	return string(info.GatewayCmdPayload)
}

// Switch to send-all-subs mode for the given gateway and account.
// This is invoked when processing an inbound message and we
// reach a point where we had to send a lot of RS- for this
// account. We will send an INFO protocol to indicate that we
// start sending all our subs (for this account), followed by
// all subs (RS+) and finally an INFO to indicate the end of it.
// The remote will then send messages only if it finds explicit
// interest in the sublist created based on all RS+ that we just
// sent.
// The client's lock is held on entry.
// <Invoked from inbound connection's readLoop>
func (c *client) gatewaySwitchAccountToSendAllSubs(e *insie, accName string) {
	// Set this map to nil so that the no-interest is
	// no longer checked.
	e.ni = nil
	s := c.srv

	remoteGWName := c.gw.name
	c.Noticef("Gateway %q: switching account %q to %s mode",
		remoteGWName, accName, InterestOnly)

	// Function that will create an INFO protocol
	// and set proper command.
	sendCmd := func(cmd byte, useLock bool) {
		// Use bare server info and simply set the
		// gateway name and command
		info := Info{
			Gateway:           s.getGatewayName(),
			GatewayCmd:        cmd,
			GatewayCmdPayload: []byte(accName),
		}

		b, _ := json.Marshal(&info)
		infoJSON := []byte(fmt.Sprintf(InfoProto, b))
		if useLock {
			c.mu.Lock()
		}
		c.sendProto(infoJSON, true)
		if useLock {
			c.mu.Unlock()
		}
	}
	// Send the start command. When remote receives this,
	// it may continue to send optimistic messages, but
	// it will start to register RS+/RS- in sublist instead
	// of noInterest map.
	sendCmd(gatewayCmdAllSubsStart, false)

	// Execute this in separate go-routine as to not block
	// the readLoop (which may cause the otherside to close
	// the connection due to slow consumer)
	s.startGoRoutine(func() {
		defer s.grWG.Done()

		s.sendAccountSubsToGateway(c, []byte(accName))
		// Send the complete command. When the remote receives
		// this, it will not send a message unless it has a
		// matching sub from us.
		sendCmd(gatewayCmdAllSubsComplete, true)

		c.Noticef("Gateway %q: switching account %q to %s mode complete",
			remoteGWName, accName, InterestOnly)
	})
}
