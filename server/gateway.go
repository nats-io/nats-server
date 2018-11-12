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
	"bytes"
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
	defaultSolicitGatewaysDelay  = time.Second
	defaultGatewayConnectDelay   = time.Second
	defaultGatewayReconnectDelay = time.Second
)

var (
	gatewayConnectDelay   = defaultGatewayConnectDelay
	gatewayReconnectDelay = defaultGatewayReconnectDelay
)

const (
	gatewayCmdGossip byte = 1
)

type srvGateway struct {
	sync.RWMutex
	enabled  bool                     // Immutable, true if both a name and port are configured
	name     string                   // Name of the Gateway on this server
	out      map[string]*client       // outbound gateways
	in       map[uint64]*client       // inbound gateways
	remotes  map[string]*gatewayCfg   // Config of remote gateways
	URLs     map[string]struct{}      // Set of all Gateway URLs in the cluster
	URL      string                   // This server gateway URL (after possible random port is resolved)
	info     *Info                    // Gateway Info protocol
	infoJSON []byte                   // Marshal'ed Info protocol
	defPerms *GatewayPermissions      // Default permissions (when accepting an unknown remote gateway)
	rqs      map[string]*subscription // Map of remote queue subscriptions (key is account+subject+queue)
	runknown bool                     // Rejects unknown (not configured) gateway connections
	resolver netResolver              // Used to resolve host name before calling net.Dial()
}

type gatewayCfg struct {
	sync.RWMutex
	*RemoteGatewayOpts
	urls         map[string]*url.URL
	connAttempts int
	implicit     bool
}

// Struct for client's gateway related fields
type gateway struct {
	name     string
	outbound bool
	cfg      *gatewayCfg
	// Saved in createGateway() since we delay send of CONNECT and INFO
	// for outbound connections
	connectURL *url.URL
	infoJSON   []byte
	// As an outbound connection, this indicates if there is no interest
	// for an account/subject.
	// As an inbound connection, this indicates if we have sent a no-interest
	// protocol to the remote gateway.
	noInterest sync.Map
	// Queue subscriptions interest for this gateway.
	qsubsInterest sync.Map
}

// clone retursn a deep copy of the RemoteGatewayOpts object
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

// Initialize the s.gateway structure. We do this even if the server
// does not have a gateway configured. In some part of the code, the
// server will check the number of outbound gateways, etc.. and so
// we don't have to check if s.gateway is nil or not.
func newGateway(opts *Options) (*srvGateway, error) {
	gateway := &srvGateway{
		name:     opts.Gateway.Name,
		out:      make(map[string]*client),
		in:       make(map[uint64]*client),
		remotes:  make(map[string]*gatewayCfg),
		URLs:     make(map[string]struct{}),
		rqs:      make(map[string]*subscription),
		resolver: opts.Gateway.resolver,
		runknown: opts.Gateway.RejectUnknown,
	}
	gateway.Lock()
	defer gateway.Unlock()

	if gateway.resolver == nil {
		gateway.resolver = netResolver(net.DefaultResolver)
	}

	// Copy default permissions (works if DefaultPermissions is nil)
	gateway.defPerms = opts.Gateway.DefaultPermissions.clone()

	// Create remote gateways
	for _, rgo := range opts.Gateway.Gateways {
		cfg := &gatewayCfg{
			RemoteGatewayOpts: rgo.clone(),
			urls:              make(map[string]*url.URL, len(rgo.URLs)),
		}
		if opts.Gateway.TLSConfig != nil && cfg.TLSConfig == nil {
			cfg.TLSConfig = opts.Gateway.TLSConfig.Clone()
		}
		if cfg.TLSTimeout == 0 {
			cfg.TLSTimeout = opts.Gateway.TLSTimeout
		}
		for _, u := range rgo.URLs {
			cfg.urls[u.Host] = u
		}
		gateway.remotes[cfg.Name] = cfg
	}

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
			dur = defaultSolicitGatewaysDelay
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
	// Keep track of actual listen port. This will be needed in case of
	// config reload.
	s.gatewayActualPort = opts.Gateway.Port
	// Possibly override Host/Port based on Gateway.Advertise
	if err := s.setGatewayInfoHostPort(info, opts); err != nil {
		s.Fatalf("Error setting gateway INFO with Gateway.Advertise value of %s, err=%v", opts.Gateway.Advertise, err)
		l.Close()
		s.mu.Unlock()
		return
	}
	// Setup state that can enable shutdown
	s.gatewayListener = l
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
	}
	gw := s.gateway
	gw.Lock()
	delete(gw.URLs, gw.URL)
	gw.URL = net.JoinHostPort(info.Host, strconv.Itoa(info.Port))
	gw.URLs[gw.URL] = struct{}{}
	gw.info = info
	info.GatewayURL = gw.URL
	// (re)generate the gatewayInfoJSON byte array
	gw.generateInfoJSON()
	gw.Unlock()
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
				s.solicitGateway(cfg)
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
	s.solicitGateway(cfg)
}

// This function will loop trying to connect to any URL attached
// to the given Gateway. It will return once a connection has been created.
func (s *Server) solicitGateway(cfg *gatewayCfg) {
	var (
		opts       = s.getOpts()
		isImplicit = cfg.isImplicit()
		urls       = cfg.getURLs()
		attempts   int
	)
	for s.isRunning() && len(urls) > 0 {
		// Iteration is random
		for _, u := range urls {
			address, err := s.getRandomIP(s.gateway.resolver, u.Host)
			if err != nil {
				s.Errorf("Error getting IP for %s: %v", u.Host, err)
				continue
			}
			s.Debugf("Trying to connect to gateway %q at %s", cfg.Name, address)
			conn, err := net.DialTimeout("tcp", address, DEFAULT_ROUTE_DIAL)
			if err == nil {
				// We could connect, create the gateway connection and return.
				s.createGateway(cfg, u, conn)
				return
			}
			s.Errorf("Error trying to connect to gateway: %v", err)
			// Break this loop if server is being shutdown...
			if !s.isRunning() {
				break
			}
		}
		if isImplicit {
			attempts++
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
	tlsRequired := opts.Gateway.TLSConfig != nil

	c := &client{srv: s, nc: conn, typ: GATEWAY}

	// Are we creating the gateway based on the configuration
	solicit := cfg != nil

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
	c.in.rcache = make(map[string]*routeCache, maxRouteCacheSize)
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
			// Specify the ServerName we are expecting.
			host, _, _ := net.SplitHostPort(url.Host)
			tlsConfig := cfg.TLSConfig
			tlsConfig.ServerName = host
			c.nc = tls.Client(c.nc, tlsConfig)
			timeout = cfg.TLSTimeout
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
	s.startGoRoutine(c.readLoop)

	// Spin up the write loop.
	s.startGoRoutine(c.writeLoop)

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
	tlsRequired := c.srv.getOpts().Gateway.TLSConfig != nil
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
func (c *client) processGatewayConnect(arg []byte) error {
	connect := &connectInfo{}
	if err := json.Unmarshal(arg, connect); err != nil {
		return err
	}

	// Coming from a client or a route, reject
	if connect.Gateway == "" {
		errTxt := ErrClientOrRouteConnectedToGatewayPort.Error()
		c.Errorf(errTxt)
		c.sendErr(errTxt)
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
	c.opts.Name = info.ID
	c.mu.Unlock()

	// For an outbound connection...
	if isOutbound {
		// Check content of INFO for fields indicating that it comes from a gateway.
		// If we incorrectly connect to the wrong port (client or route), we won't
		// have the Gateway field set.
		if info.Gateway == "" {
			errTxt := fmt.Sprintf("Attempt to connect to gateway %q using wrong port", gwName)
			s.Errorf(errTxt)
			c.sendErr(errTxt)
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
		cfg.updateURLs(info.GatewayURLs)

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
			s.registerOutboundGateway(gwName, c)
			c.Noticef("Outbound gateway connection to %q (%s) registered", gwName, info.ID)
			// Now that the outbound gateway is registered, we can remove from temp map.
			s.removeFromTempClients(cid)
		}

		// Flood local cluster with information about this gateway.
		// Servers in this cluster will ensure that they have (or otherwise create)
		// an outbound connection to this gateway.
		s.forwardNewGatewayToLocalCluster(info)

	} else if isFirstINFO {
		// This is the first INFO of an inbound connection...

		s.registerInboundGateway(cid, c)
		c.Noticef("Inbound gateway connection from %q (%s) registered", info.Gateway, info.ID)

		// Now that it is registered, we can remove from temp map.
		s.removeFromTempClients(cid)

		// Send our QSubs
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
	// Need tp protect s.routes here, so use server's lock
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
func (s *Server) sendQueueSubsToGateway(c *client) {
	var (
		accsa = [256]*Account{}
		accs  = accsa[:0]
		rqsa  = [4096]*subscription{}
		rqs   = rqsa[:0]
		bufa  = [32 * 1024]byte{}
		buf   = bufa[:0]
	)
	// Collect accounts
	s.mu.Lock()
	for _, acc := range s.accounts {
		accs = append(accs, acc)
	}
	s.mu.Unlock()

	// Collect remote queue subs
	s.gateway.RLock()
	for _, sub := range s.gateway.rqs {
		rqs = append(rqs, sub)
	}
	s.gateway.RUnlock()

	//TODO: Buffer may get too big so should send it by chunks...

	// Build proto for local subs
	for _, acc := range accs {
		acc.mu.RLock()
		for subAndQueue, rm := range acc.rm {
			if rm.qi > 0 {
				buf = append(buf, rSubBytes...)
				buf = append(buf, acc.Name...)
				buf = append(buf, ' ')
				buf = append(buf, subAndQueue...)
				buf = append(buf, ' ', '1')
				buf = append(buf, CR_LF...)
			}
		}
		acc.mu.RUnlock()
	}
	// Now with the remote queue subs
	for _, qsub := range rqs {
		buf = append(buf, rSubBytes...)
		// The remote queue sub sid is account+' '+subject+' '+queue name
		buf = append(buf, qsub.sid...)
		buf = append(buf, ' ', '1')
		buf = append(buf, CR_LF...)
	}

	// Send
	if len(buf) > 0 {
		c.mu.Lock()
		c.queueOutbound(buf)
		c.flushSignal()
		closed := c.flags.isSet(clearConnection)
		if !closed {
			c.Debugf("Sent queue subscriptions to gateway")
		}
		c.mu.Unlock()
	}
}

// This is invoked when getting an INFO protocol for gateway on the ROUTER port.
// This function will then execute appropriate function based on the command
// contained in the protocol.
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
	defer gw.RUnlock()
	// Send only to gateways for which we have actual outbound connection to.
	if len(gw.out) == 0 {
		return
	}
	// Check forwardNewGatewayToLocalCluster() as to why we set ID this way.
	info := Info{
		ID:         "GW" + s.info.ID,
		GatewayCmd: gatewayCmdGossip,
	}
	for gwCfgName, cfg := range gw.remotes {
		if _, exist := gw.out[gwCfgName]; !exist {
			continue
		}
		urls := cfg.getURLsAsStrings()
		if len(urls) > 0 {
			info.Gateway = gwCfgName
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
		s.solicitGateway(cfg)
		s.grWG.Done()
	})
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

// add URLs from the given array to the urls map only if not already present.
// remoteGateway write lock is assumed to be held on entry.
func (g *gatewayCfg) addURLs(infoURLs []string) {
	for _, iu := range infoURLs {
		if _, present := g.urls[iu]; !present {
			// Urls in Info.GatewayURLs come without scheme. Add it to parse
			// the url (otherwise it fails).
			if u, err := url.Parse(fmt.Sprintf("nats://%s", iu)); err == nil {
				// But use u.Host for the key.
				g.urls[u.Host] = u
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
// map with the given name as the key.
func (s *Server) registerInboundGateway(cid uint64, gwc *client) {
	s.gateway.Lock()
	s.gateway.in[cid] = gwc
	s.gateway.Unlock()
}

// Register the given gateway connection (*client) in the outbound gateways
// map with the given name as the key.
func (s *Server) registerOutboundGateway(name string, gwc *client) {
	s.gateway.Lock()
	s.gateway.out[name] = gwc
	s.gateway.Unlock()
}

// Returns the outbound gateway connection (*client) with the given name,
// or nil if not found
func (s *Server) getOutboundGateway(name string) *client {
	s.gateway.RLock()
	gwc := s.gateway.out[name]
	s.gateway.RUnlock()
	return gwc
}

// Returns all outbound gateway connections in the provided array
func (s *Server) getOutboundGateways(a *[]*client) {
	s.gateway.RLock()
	for _, gwc := range s.gateway.out {
		*a = append(*a, gwc)
	}
	s.gateway.RUnlock()
}

// Returns all inbound gateway connections in the provided array
func (s *Server) getInboundGateways(a *[]*client) {
	s.gateway.RLock()
	for _, gwc := range s.gateway.in {
		*a = append(*a, gwc)
	}
	s.gateway.RUnlock()
}

// This is invoked when a gateway connection is closed and the server
// is removing this connection from its state.
func (s *Server) removeRemoteGateway(c *client) {
	c.mu.Lock()
	cid := c.cid
	isOutbound := c.gw.outbound
	gwName := c.gw.name
	c.mu.Unlock()

	gw := s.gateway
	gw.Lock()
	if isOutbound {
		delete(gw.out, gwName)
	} else {
		delete(gw.in, cid)
	}
	gw.Unlock()
	s.removeFromTempClients(cid)
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

// A- protocol received when sending messages on an account that
// the remote gateway has no interest in. Mark this account
// with a "no interest" marker to prevent further messages send.
func (c *client) processGatewayAccountUnsub(accName string) {
	// Just to indicate activity around "subscriptions" events.
	c.in.subs++
	c.gw.noInterest.Store(accName, nil)
}

// A+ protocol received by remote gateway if it had previously
// sent an A-. Clear the "no interest" marker for this account.
func (c *client) processGatewayAccountSub(accName string) error {
	// Just to indicate activity around "subscriptions" events.
	c.in.subs++
	c.gw.noInterest.Delete(accName)
	return nil
}

// RS- protocol received when sending messages on an subject
// that the remote gateway has no interest in (but knows about the account).
// Mark this subject with a "no interest" marker to prevent further
// messages send.
func (c *client) processGatewaySubjectUnsub(arg []byte) error {
	accName, subject, queue, err := c.parseUnsubProto(arg)
	if err != nil {
		return fmt.Errorf("processGatewaySubjectUnsub %s", err.Error())
	}
	// Queue subs are treated differently.
	if queue != nil {
		sli, _ := c.gw.qsubsInterest.Load(accName)
		if sli != nil {
			sl := sli.(*Sublist)
			r := sl.Match(string(subject))
			if len(r.qsubs) > 0 {
				for i := 0; i < len(r.qsubs); i++ {
					qsubs := r.qsubs[i]
					if bytes.Equal(qsubs[0].queue, queue) {
						sl.Remove(qsubs[0])
						if sl.Count() == 0 {
							c.gw.qsubsInterest.Delete(accName)
						}
					}
				}
			}
		}
	} else {
		// For a given gateway, we will receive the A+,A-,RS+ and RS-
		// from the same tcp connection/go routine. So although there
		// may be different go-routines doing lookups on this map,
		// we are guaranteed that there is no Store/Delete happening
		// in parallel.
		sni, noInterest := c.gw.noInterest.Load(accName)
		// Do we have a no-interest at the account level? If so,
		// don't bother setting no-interest on that subject.
		if noInterest && sni == nil {
			return nil
		}
		var (
			subjNoInterest *sync.Map
			store          bool
		)
		if sni == nil {
			subjNoInterest = &sync.Map{}
			store = true
		} else {
			subjNoInterest = sni.(*sync.Map)
		}
		subjNoInterest.Store(string(subject), struct{}{})
		if store {
			c.gw.noInterest.Store(accName, subjNoInterest)
		}
	}
	return nil
}

// For plain subs, RS+ protocol received by remote gateway if it
// had previously sent a RS-. Clear the "no interest" marker for
// this subject (under this account).
// For queue subs, register interest from remote gateway.
func (c *client) processGatewaySubjectSub(arg []byte) error {
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

	if queue != nil {
		var (
			sl    *Sublist
			store bool
		)
		sli, _ := c.gw.qsubsInterest.Load(string(accName))
		if sli != nil {
			sl = sli.(*Sublist)
		} else {
			sl = NewSublist()
			store = true
		}
		// Copy subject and queue to avoid referencing a possibly
		// big underlying buffer.
		cbuf := make([]byte, len(subject)+len(queue))
		copy(cbuf, subject)
		copy(cbuf[len(subject):], queue)
		sub := &subscription{client: c, subject: cbuf[:len(subject)], queue: cbuf[len(subject):], qw: qw}
		sl.Insert(sub)
		if store {
			c.gw.qsubsInterest.Store(string(accName), sl)
		}
	} else {
		// See remark from processGatewaySubjectUnsub().
		sni, _ := c.gw.noInterest.Load(string(accName))
		// If this account is not even present, or if there is no
		// specific subject with no-interest, we are done.
		if sni == nil {
			return nil
		}
		subjNoInterest := sni.(*sync.Map)
		subjNoInterest.Delete(string(subject))
	}
	return nil
}

// Returns true if the remote gateway has no known no-interest
// on the account and/or subject. Returns false if we know that
// we should not be sending messages on that account/subject.
func (c *client) hasInterest(acc, subj string) bool {
	// If there is no value for the `acc` key, then it means that
	// there is interest (or no known no-interest).
	sni, accountNoInterest := c.gw.noInterest.Load(acc)
	if !accountNoInterest {
		return true
	}
	// If what is stored is nil, then it means that there is
	// no interest at the account level, so we return false.
	if sni == nil {
		return false
	}
	// If there is a value, sni is itself a sync.Map for subject
	// no-interest (in that account).
	subjectNoInterest := sni.(*sync.Map)
	if _, noInterest := subjectNoInterest.Load(subj); noInterest {
		return false
	}
	// There is interest
	return true
}

// This is invoked when an account is registered. We check if we did send
// to remote gateways a "no interest" in the past when receiving messages.
// If we did, we send to the remote gateway an A+ protocol (see
// processGatewayAccountSub()).
func (s *Server) endAccountNoInterestForGateways(accName string) {
	var (
		_gws [4]*client
		gws  = _gws[:0]
	)
	s.getInboundGateways(&gws)
	if len(gws) == 0 {
		return
	}
	var (
		protoa = [256]byte{}
		proto  = protoa[:0]
	)
	proto = append(proto, aSubBytes...)
	proto = append(proto, accName...)
	proto = append(proto, CR_LF...)
	for _, c := range gws {
		if _, noInterest := c.gw.noInterest.Load(accName); noInterest {
			c.gw.noInterest.Delete(accName)
			c.mu.Lock()
			if c.trace {
				c.traceOutOp("", proto[:len(proto)-LEN_CR_LF])
			}
			c.sendProto(proto, true)
			c.mu.Unlock()
		}
	}
}

// This is invoked when a subscription is registered. We check if we did
// send to remote gateways a "no interest" in the past when receiving messages.
// If we did, we send the remote gateway a RS+ protocol.
func (s *Server) sendSubInterestToGateways(accName string, sub *subscription) {
	// If this is a remote queue sub, we need to keep track of it now.
	if sub.queue != nil && sub.client != nil && sub.client.typ == ROUTER {
		s.gateway.Lock()
		s.gateway.rqs[string(sub.sid)] = sub
		s.gateway.Unlock()
	}
	var (
		_gws [4]*client
		gws  = _gws[:0]
	)
	s.getInboundGateways(&gws)
	if len(gws) == 0 {
		return
	}
	var (
		protoa = [512]byte{}
		proto  = protoa[:0]
	)
	for _, c := range gws {
		// Always send for queues. Note that by contract this function
		// is called only once per account/subject/queue.
		sendProto := sub.queue != nil
		// We still want to execute following code even if this is a
		// queue and we are going to send the proto anyway, because
		// it may clear the noInterest map if we are going from
		// a no-interest to a queue sub.
		if sni, _ := c.gw.noInterest.Load(accName); sni != nil {
			sn := sni.(*sync.Map)
			if _, noInterest := sn.Load(string(sub.subject)); noInterest {
				sn.Delete(string(sub.subject))
				sendProto = true
			}
		}
		if sendProto {
			if len(proto) == 0 {
				proto = append(proto, rSubBytes...)
				proto = append(proto, accName...)
				proto = append(proto, ' ')
				proto = append(proto, sub.subject...)
				if sub.queue != nil {
					proto = append(proto, ' ')
					proto = append(proto, sub.queue...)
					// For now, just use 1 for the weight
					proto = append(proto, ' ', '1')
				}
				proto = append(proto, CR_LF...)
			}
			c.mu.Lock()
			if c.trace {
				c.traceOutOp("", proto[:len(proto)-LEN_CR_LF])
			}
			c.sendProto(proto, true)
			c.mu.Unlock()
		}
	}
}

func (s *Server) sendQueueUnsubToGateways(accName string, qsub *subscription) {
	// If this is a remote queue sub, need to remove from remote queue subs map
	if qsub.client != nil && qsub.client.typ == ROUTER {
		s.gateway.Lock()
		delete(s.gateway.rqs, string(qsub.sid))
		s.gateway.Unlock()
	}
	var (
		_gws [4]*client
		gws  = _gws[:0]
	)
	s.getInboundGateways(&gws)
	if len(gws) == 0 {
		return
	}
	var (
		protoa = [512]byte{}
		proto  = protoa[:0]
	)
	proto = append(proto, rUnsubBytes...)
	proto = append(proto, accName...)
	proto = append(proto, ' ')
	proto = append(proto, qsub.subject...)
	proto = append(proto, ' ')
	proto = append(proto, qsub.queue...)
	proto = append(proto, CR_LF...)
	for _, c := range gws {
		c.mu.Lock()
		if c.trace {
			c.traceOutOp("", proto[:len(proto)-LEN_CR_LF])
		}
		c.sendProto(proto, true)
		c.mu.Unlock()
	}
}

// May send a message to all outbound gateways. It is possible
// that message is not sent to a given gateway if for instance
// it is known that this gateway has no interest in account or
// subject, etc..
func (c *client) sendMsgToGateways(msg, subject, reply []byte, doQueues bool) {
	var (
		gwsa [4]*client
		gws  = gwsa[:0]
		s    = c.srv
	)
	s.getOutboundGateways(&gws)
	if len(gws) == 0 {
		return
	}
	var (
		subj    = string(subject)
		queuesa = [512]byte{}
		queues  = queuesa[:0]
		groups  = map[string]struct{}{}
		accName = c.acc.Name
	)
	if doQueues {
		// Order the gws by lowest RTT
		sort.Slice(gws, func(i, j int) bool {
			return gws[i].getRTTValue() < gws[j].getRTTValue()
		})
	}
	for _, gwc := range gws {
		// hasInterest is not covering queue subs
		psubInterest := gwc.hasInterest(accName, subj)
		if !psubInterest && !doQueues {
			continue
		}
		mh := c.msgb[:msgHeadProtoLen]
		mh = append(mh, accName...)
		mh = append(mh, ' ')
		mh = append(mh, subject...)
		mh = append(mh, ' ')

		hasQueues := false
		if doQueues {
			queues = queuesa[:0]
			sli, _ := gwc.gw.qsubsInterest.Load(accName)
			if sli != nil {
				sl := sli.(*Sublist)
				r := sl.Match(subj)
				if len(r.qsubs) > 0 {
					for i := 0; i < len(r.qsubs); i++ {
						qsubs := r.qsubs[i]
						if len(qsubs) > 0 {
							queue := qsubs[0].queue
							if _, exists := groups[string(queue)]; !exists {
								groups[string(queue)] = struct{}{}
								queues = append(queues, queue...)
								queues = append(queues, ' ')
								hasQueues = true
							}
						}
					}
				}
			}
			if hasQueues {
				if reply != nil {
					mh = append(mh, "+ "...) // Signal that there is a reply.
					mh = append(mh, reply...)
					mh = append(mh, ' ')
				} else {
					mh = append(mh, "| "...) // Only queues
				}
				mh = append(mh, queues...)
			} else if !psubInterest {
				continue
			}
		}
		if !hasQueues && reply != nil {
			mh = append(mh, reply...)
			mh = append(mh, ' ')
		}
		mh = append(mh, c.pa.szb...)
		mh = append(mh, CR_LF...)
		sub := subscription{client: gwc}
		c.deliverMsg(&sub, mh, msg)
	}
}

func (c *client) processInboundGatewayMsg(msg []byte) {
	// Update statistics
	c.in.msgs++
	// The msg includes the CR_LF, so pull back out for accounting.
	c.in.bytes += len(msg) - LEN_CR_LF

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

	var (
		acc *Account
		rc  *routeCache
		r   *SublistResult
		ok  bool
	)

	// Check our cache first.
	if rc, ok = c.in.rcache[string(c.pa.rcache)]; ok {
		// Check the genid to see if it's still valid.
		if genid := atomic.LoadUint64(&rc.acc.sl.genid); genid != rc.genid {
			ok = false
			delete(c.in.rcache, string(c.pa.rcache))
		} else {
			acc = rc.acc
			r = rc.results
		}
	}

	if !ok {
		// Match correct account and sublist.
		acc = c.srv.LookupAccount(string(c.pa.account))
		if acc == nil {
			c.Debugf("Unknown account %q for routed message on subject: %q", c.pa.account, c.pa.subject)
			// Send A- only once...
			if _, sent := c.gw.noInterest.Load(string(c.pa.account)); !sent {
				// Send an A- protocol, but keep track that we have sent a "no interest"
				// for that account, so that if later this account gets registered,
				// we need to send an A+ to this remote gateway.
				c.gw.noInterest.Store(string(c.pa.account), nil)
				var (
					protoa = [256]byte{}
					proto  = protoa[:0]
				)
				proto = append(proto, aUnsubBytes...)
				proto = append(proto, c.pa.account...)
				if c.trace {
					c.traceOutOp("", proto)
				}
				proto = append(proto, CR_LF...)
				c.mu.Lock()
				c.sendProto(proto, true)
				c.mu.Unlock()
			}
			return
		}

		// Match against the account sublist.
		r = acc.sl.Match(string(c.pa.subject))

		// Store in our cache
		c.in.rcache[string(c.pa.rcache)] = &routeCache{acc, r, atomic.LoadUint64(&acc.sl.genid)}

		// Check if we need to prune.
		if len(c.in.rcache) > maxRouteCacheSize {
			c.pruneRouteCache()
		}
	}

	// Check to see if we need to map/route to another account.
	if acc.imports.services != nil {
		c.checkForImportServices(acc, msg)
	}

	// Check for no interest, short circuit if so.
	// This is the fanout scale.
	if len(r.psubs)+len(r.qsubs) == 0 {
		sendProto := false
		// Send an RS- protocol, but keep track that we have said "no interest"
		// for that account/subject, so that if later there is a subscription on
		// this subject, we need to send an R+ to remote gateways.
		si, _ := c.gw.noInterest.Load(string(c.pa.account))
		if si == nil {
			m := &sync.Map{}
			m.Store(string(c.pa.subject), struct{}{})
			c.gw.noInterest.Store(string(c.pa.account), m)
			sendProto = true
		} else {
			m := si.(*sync.Map)
			if _, alreadySent := m.Load(string(c.pa.subject)); !alreadySent {
				m.Store(string(c.pa.subject), struct{}{})
				sendProto = true
			}
		}
		if sendProto {
			var (
				protoa = [512]byte{}
				proto  = protoa[:0]
			)
			proto = append(proto, rUnsubBytes...)
			proto = append(proto, c.pa.account...)
			proto = append(proto, ' ')
			proto = append(proto, c.pa.subject...)
			c.mu.Lock()
			if c.trace {
				c.traceOutOp("", proto)
			}
			proto = append(proto, CR_LF...)
			c.sendProto(proto, true)
			c.mu.Unlock()
		}
		return
	}

	// Check to see if we have a routed message with a service reply.
	if isServiceReply(c.pa.reply) && acc != nil {
		// Need to add a sub here for local interest to send a response back
		// to the originating server/requestor where it will be re-mapped.
		sid := make([]byte, 0, len(acc.Name)+len(c.pa.reply)+1)
		sid = append(sid, acc.Name...)
		sid = append(sid, ' ')
		sid = append(sid, c.pa.reply...)
		// Copy off the reply since otherwise we are referencing a buffer that will be reused.
		reply := make([]byte, len(c.pa.reply))
		copy(reply, c.pa.reply)
		sub := &subscription{client: c, subject: reply, sid: sid, max: 1}
		if err := acc.sl.Insert(sub); err != nil {
			c.Errorf("Could not insert subscription: %v", err)
		} else {
			ttl := acc.AutoExpireTTL()
			c.mu.Lock()
			c.subs[string(sid)] = sub
			c.addReplySubTimeout(acc, sub, ttl)
			c.mu.Unlock()
		}
	}

	c.processMsgResults(acc, r, msg, c.pa.subject, c.pa.reply)
}
