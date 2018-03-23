// Copyright 2013-2018 The NATS Authors
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
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/gnatsd/util"
)

// RouteType designates the router type
type RouteType int

// Type of Route
const (
	// This route we learned from speaking to other routes.
	Implicit RouteType = iota
	// This route was explicitly configured.
	Explicit
)

type route struct {
	remoteID     string
	didSolicit   bool
	retry        bool
	routeType    RouteType
	url          *url.URL
	authRequired bool
	tlsRequired  bool
	closed       bool
	connectURLs  []string
}

type connectInfo struct {
	Verbose  bool   `json:"verbose"`
	Pedantic bool   `json:"pedantic"`
	User     string `json:"user,omitempty"`
	Pass     string `json:"pass,omitempty"`
	TLS      bool   `json:"tls_required"`
	Name     string `json:"name"`
}

// Route protocol constants
const (
	ConProto  = "CONNECT %s" + _CRLF_
	InfoProto = "INFO %s" + _CRLF_
)

// Lock should be held entering here.
func (c *client) sendConnect(tlsRequired bool) {
	var user, pass string
	if userInfo := c.route.url.User; userInfo != nil {
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
	}
	b, err := json.Marshal(cinfo)
	if err != nil {
		c.Errorf("Error marshaling CONNECT to route: %v\n", err)
		c.closeConnection()
		return
	}
	c.sendProto([]byte(fmt.Sprintf(ConProto, b)), true)
}

// Process the info message if we are a route.
func (c *client) processRouteInfo(info *Info) {
	c.mu.Lock()
	// Connection can be closed at any time (by auth timeout, etc).
	// Does not make sense to continue here if connection is gone.
	if c.route == nil || c.nc == nil {
		c.mu.Unlock()
		return
	}

	s := c.srv
	remoteID := c.route.remoteID

	// We receive an INFO from a server that informs us about another server,
	// so the info.ID in the INFO protocol does not match the ID of this route.
	if remoteID != "" && remoteID != info.ID {
		c.mu.Unlock()

		// Process this implicit route. We will check that it is not an explicit
		// route and/or that it has not been connected already.
		s.processImplicitRoute(info)
		return
	}

	// Need to set this for the detection of the route to self to work
	// in closeConnection().
	c.route.remoteID = info.ID

	// Detect route to self.
	if c.route.remoteID == s.info.ID {
		c.mu.Unlock()
		c.closeConnection()
		return
	}

	// Copy over important information.
	c.route.authRequired = info.AuthRequired
	c.route.tlsRequired = info.TLSRequired

	// If we do not know this route's URL, construct one on the fly
	// from the information provided.
	if c.route.url == nil {
		// Add in the URL from host and port
		hp := net.JoinHostPort(info.Host, strconv.Itoa(info.Port))
		url, err := url.Parse(fmt.Sprintf("nats-route://%s/", hp))
		if err != nil {
			c.Errorf("Error parsing URL from INFO: %v\n", err)
			c.mu.Unlock()
			c.closeConnection()
			return
		}
		c.route.url = url
	}

	// Check to see if we have this remote already registered.
	// This can happen when both servers have routes to each other.
	c.mu.Unlock()

	if added, sendInfo := s.addRoute(c, info); added {
		c.Debugf("Registering remote route %q", info.ID)
		// Send our local subscriptions to this route.
		s.sendLocalSubsToRoute(c)
		// sendInfo will be false if the route that we just accepted
		// is the only route there is.
		if sendInfo {
			// The incoming INFO from the route will have IP set
			// if it has Cluster.Advertise. In that case, use that
			// otherwise contruct it from the remote TCP address.
			if info.IP == "" {
				// Need to get the remote IP address.
				c.mu.Lock()
				switch conn := c.nc.(type) {
				case *net.TCPConn, *tls.Conn:
					addr := conn.RemoteAddr().(*net.TCPAddr)
					info.IP = fmt.Sprintf("nats-route://%s/", net.JoinHostPort(addr.IP.String(),
						strconv.Itoa(info.Port)))
				default:
					info.IP = c.route.url.String()
				}
				c.mu.Unlock()
			}
			// Now let the known servers know about this new route
			s.forwardNewRouteInfoToKnownServers(info)
		}
		// Unless disabled, possibly update the server's INFO protocol
		// and send to clients that know how to handle async INFOs.
		if !s.getOpts().Cluster.NoAdvertise {
			s.addClientConnectURLsAndSendINFOToClients(info.ClientConnectURLs)
		}
	} else {
		c.Debugf("Detected duplicate remote route %q", info.ID)
		c.closeConnection()
	}
}

// sendAsyncInfoToClients sends an INFO protocol to all
// connected clients that accept async INFO updates.
// The server lock is held on entry.
func (s *Server) sendAsyncInfoToClients() {
	// If there are no clients supporting async INFO protocols, we are done.
	// Also don't send if we are shutting down...
	if s.cproto == 0 || s.shutdown {
		return
	}

	for _, c := range s.clients {
		c.mu.Lock()
		// Here, we are going to send only to the clients that are fully
		// registered (server has received CONNECT and first PING). For
		// clients that are not at this stage, this will happen in the
		// processing of the first PING (see client.processPing)
		if c.opts.Protocol >= ClientProtoInfo && c.flags.isSet(firstPongSent) {
			// sendInfo takes care of checking if the connection is still
			// valid or not, so don't duplicate tests here.
			c.sendInfo(s.infoJSON)
		}
		c.mu.Unlock()
	}
}

// This will process implicit route information received from another server.
// We will check to see if we have configured or are already connected,
// and if so we will ignore. Otherwise we will attempt to connect.
func (s *Server) processImplicitRoute(info *Info) {
	remoteID := info.ID

	s.mu.Lock()
	defer s.mu.Unlock()

	// Don't connect to ourself
	if remoteID == s.info.ID {
		return
	}
	// Check if this route already exists
	if _, exists := s.remotes[remoteID]; exists {
		return
	}
	// Check if we have this route as a configured route
	if s.hasThisRouteConfigured(info) {
		return
	}

	// Initiate the connection, using info.IP instead of info.URL here...
	r, err := url.Parse(info.IP)
	if err != nil {
		s.Errorf("Error parsing URL from INFO: %v\n", err)
		return
	}

	// Snapshot server options.
	opts := s.getOpts()

	if info.AuthRequired {
		r.User = url.UserPassword(opts.Cluster.Username, opts.Cluster.Password)
	}
	s.startGoRoutine(func() { s.connectToRoute(r, false) })
}

// hasThisRouteConfigured returns true if info.Host:info.Port is present
// in the server's opts.Routes, false otherwise.
// Server lock is assumed to be held by caller.
func (s *Server) hasThisRouteConfigured(info *Info) bool {
	urlToCheckExplicit := strings.ToLower(net.JoinHostPort(info.Host, strconv.Itoa(info.Port)))
	for _, ri := range s.getOpts().Routes {
		if strings.ToLower(ri.Host) == urlToCheckExplicit {
			return true
		}
	}
	return false
}

// forwardNewRouteInfoToKnownServers sends the INFO protocol of the new route
// to all routes known by this server. In turn, each server will contact this
// new route.
func (s *Server) forwardNewRouteInfoToKnownServers(info *Info) {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, _ := json.Marshal(info)
	infoJSON := []byte(fmt.Sprintf(InfoProto, b))

	for _, r := range s.routes {
		r.mu.Lock()
		if r.route.remoteID != info.ID {
			r.sendInfo(infoJSON)
		}
		r.mu.Unlock()
	}
}

// This will send local subscription state to a new route connection.
// FIXME(dlc) - This could be a DOS or perf issue with many clients
// and large subscription space. Plus buffering in place not a good idea.
func (s *Server) sendLocalSubsToRoute(route *client) {
	b := bytes.Buffer{}
	s.mu.Lock()
	for _, client := range s.clients {
		client.mu.Lock()
		subs := make([]*subscription, 0, len(client.subs))
		for _, sub := range client.subs {
			subs = append(subs, sub)
		}
		client.mu.Unlock()
		for _, sub := range subs {
			rsid := routeSid(sub)
			proto := fmt.Sprintf(subProto, sub.subject, sub.queue, rsid)
			b.WriteString(proto)
		}
	}
	s.mu.Unlock()

	route.mu.Lock()
	defer route.mu.Unlock()
	route.sendProto(b.Bytes(), true)

	route.Debugf("Route sent local subscriptions")
}

func (s *Server) createRoute(conn net.Conn, rURL *url.URL) *client {
	// Snapshot server options.
	opts := s.getOpts()

	didSolicit := rURL != nil
	r := &route{didSolicit: didSolicit}
	for _, route := range opts.Routes {
		if rURL != nil && (strings.ToLower(rURL.Host) == strings.ToLower(route.Host)) {
			r.routeType = Explicit
		}
	}

	c := &client{srv: s, nc: conn, opts: clientOpts{}, typ: ROUTER, route: r}

	// Grab server variables
	s.mu.Lock()
	infoJSON := s.routeInfoJSON
	authRequired := s.routeInfo.AuthRequired
	tlsRequired := s.routeInfo.TLSRequired
	s.mu.Unlock()

	// Grab lock
	c.mu.Lock()

	// Initialize
	c.initClient()

	if didSolicit {
		// Do this before the TLS code, otherwise, in case of failure
		// and if route is explicit, it would try to reconnect to 'nil'...
		r.url = rURL
	}

	// Check for TLS
	if tlsRequired {
		// Copy off the config to add in ServerName if we
		tlsConfig := util.CloneTLSConfig(opts.Cluster.TLSConfig)

		// If we solicited, we will act like the client, otherwise the server.
		if didSolicit {
			c.Debugf("Starting TLS route client handshake")
			// Specify the ServerName we are expecting.
			host, _, _ := net.SplitHostPort(rURL.Host)
			tlsConfig.ServerName = host
			c.nc = tls.Client(c.nc, tlsConfig)
		} else {
			c.Debugf("Starting TLS route server handshake")
			c.nc = tls.Server(c.nc, tlsConfig)
		}

		conn := c.nc.(*tls.Conn)

		// Setup the timeout
		ttl := secondsToDuration(opts.Cluster.TLSTimeout)
		time.AfterFunc(ttl, func() { tlsTimeout(c, conn) })
		conn.SetReadDeadline(time.Now().Add(ttl))

		c.mu.Unlock()
		if err := conn.Handshake(); err != nil {
			c.Errorf("TLS route handshake error: %v", err)
			c.sendErr("Secure Connection - TLS Required")
			c.closeConnection()
			return nil
		}
		// Reset the read deadline
		conn.SetReadDeadline(time.Time{})

		// Re-Grab lock
		c.mu.Lock()

		// Verify that the connection did not go away while we released the lock.
		if c.nc == nil {
			c.mu.Unlock()
			return nil
		}

		// Rewrap bw
		c.bw = bufio.NewWriterSize(c.nc, startBufSize)
	}

	// Do final client initialization

	// Set the Ping timer
	c.setPingTimer()

	// For routes, the "client" is added to s.routes only when processing
	// the INFO protocol, that is much later.
	// In the meantime, if the server shutsdown, there would be no reference
	// to the client (connection) to be closed, leaving this readLoop
	// uinterrupted, causing the Shutdown() to wait indefinitively.
	// We need to store the client in a special map, under a special lock.
	s.grMu.Lock()
	running := s.grRunning
	if running {
		s.grTmpClients[c.cid] = c
	}
	s.grMu.Unlock()
	if !running {
		c.mu.Unlock()
		c.setRouteNoReconnectOnClose()
		c.closeConnection()
		return nil
	}

	// Spin up the read loop.
	s.startGoRoutine(func() { c.readLoop() })

	if tlsRequired {
		c.Debugf("TLS handshake complete")
		cs := c.nc.(*tls.Conn).ConnectionState()
		c.Debugf("TLS version %s, cipher suite %s", tlsVersion(cs.Version), tlsCipher(cs.CipherSuite))
	}

	// Queue Connect proto if we solicited the connection.
	if didSolicit {
		c.Debugf("Route connect msg sent")
		c.sendConnect(tlsRequired)
	}

	// Send our info to the other side.
	c.sendInfo(infoJSON)

	// Check for Auth required state for incoming connections.
	if authRequired && !didSolicit {
		ttl := secondsToDuration(opts.Cluster.AuthTimeout)
		c.setAuthTimer(ttl)
	}

	c.mu.Unlock()

	c.Noticef("Route connection created")
	return c
}

const (
	_CRLF_  = "\r\n"
	_EMPTY_ = ""
)

const (
	subProto   = "SUB %s %s %s" + _CRLF_
	unsubProto = "UNSUB %s%s" + _CRLF_
)

// FIXME(dlc) - Make these reserved and reject if they come in as a sid
// from a client connection.
// Route constants
const (
	RSID  = "RSID"
	QRSID = "QRSID"

	QRSID_LEN = len(QRSID)
)

// Parse the given rsid. If the protocol does not start with QRSID,
// returns false and no subscription nor error.
// If it does start with QRSID, returns true and possibly a subscription
// or an error if the QRSID protocol is malformed.
func (s *Server) routeSidQueueSubscriber(rsid []byte) (bool, *subscription, error) {
	if !bytes.HasPrefix(rsid, []byte(QRSID)) {
		return false, nil, nil
	}
	cid, sid, err := parseRouteQueueSid(rsid)
	if err != nil {
		return true, nil, err
	}

	s.mu.Lock()
	client := s.clients[cid]
	s.mu.Unlock()

	if client == nil {
		return true, nil, nil
	}

	client.mu.Lock()
	sub, ok := client.subs[string(sid)]
	client.mu.Unlock()
	if ok {
		return true, sub, nil
	}
	return true, nil, nil
}

func routeSid(sub *subscription) string {
	var qi string
	if len(sub.queue) > 0 {
		qi = "Q"
	}
	return fmt.Sprintf("%s%s:%d:%s", qi, RSID, sub.client.cid, sub.sid)
}

// Parse the given `rsid` knowing that it starts with `QRSID`.
// Returns the cid and sid or an error not a valid QRSID.
func parseRouteQueueSid(rsid []byte) (uint64, []byte, error) {
	var (
		cid      uint64
		sid      []byte
		cidFound bool
		sidFound bool
	)
	// A valid QRSID needs to be at least QRSID:x:y
	// First character here should be `:`
	if len(rsid) >= QRSID_LEN+4 {
		if rsid[QRSID_LEN] == ':' {
			for i, count := QRSID_LEN+1, len(rsid); i < count; i++ {
				switch rsid[i] {
				case ':':
					cid = uint64(parseInt64(rsid[QRSID_LEN+1 : i]))
					cidFound = true
					sid = rsid[i+1:]
				}
			}
			if cidFound {
				// We can't assume the content of sid, so as long
				// as it is not len 0, we have to say it is a valid one.
				if len(rsid) > 0 {
					sidFound = true
				}
			}
		}
	}
	if cidFound && sidFound {
		return cid, sid, nil
	}
	return 0, nil, fmt.Errorf("invalid QRSID: %s", rsid)
}

func (s *Server) addRoute(c *client, info *Info) (bool, bool) {
	id := c.route.remoteID
	sendInfo := false

	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return false, false
	}
	remote, exists := s.remotes[id]
	if !exists {
		// Remove from the temporary map
		s.grMu.Lock()
		delete(s.grTmpClients, c.cid)
		s.grMu.Unlock()

		s.routes[c.cid] = c
		s.remotes[id] = c
		c.mu.Lock()
		c.route.connectURLs = info.ClientConnectURLs
		c.mu.Unlock()

		// we don't need to send if the only route is the one we just accepted.
		sendInfo = len(s.routes) > 1
	}
	s.mu.Unlock()

	if exists {
		var r *route

		c.mu.Lock()
		// upgrade to solicited?
		if c.route.didSolicit {
			// Make a copy
			rs := *c.route
			r = &rs
		}
		c.mu.Unlock()

		remote.mu.Lock()
		// r will be not nil if c.route.didSolicit was true
		if r != nil {
			// If we upgrade to solicited, we still want to keep the remote's
			// connectURLs. So transfer those.
			r.connectURLs = remote.route.connectURLs
			remote.route = r
		}
		// This is to mitigate the issue where both sides add the route
		// on the opposite connection, and therefore end-up with both
		// connections being dropped.
		remote.route.retry = true
		remote.mu.Unlock()
	}

	return !exists, sendInfo
}

func (s *Server) broadcastInterestToRoutes(proto string) {
	var arg []byte
	if atomic.LoadInt32(&s.logging.trace) == 1 {
		arg = []byte(proto[:len(proto)-LEN_CR_LF])
	}
	protoAsBytes := []byte(proto)
	s.mu.Lock()
	for _, route := range s.routes {
		// FIXME(dlc) - Make same logic as deliverMsg
		route.mu.Lock()
		route.sendProto(protoAsBytes, true)
		route.mu.Unlock()
		route.traceOutOp("", arg)
	}
	s.mu.Unlock()
}

// broadcastSubscribe will forward a client subscription
// to all active routes.
func (s *Server) broadcastSubscribe(sub *subscription) {
	if s.numRoutes() == 0 {
		return
	}
	rsid := routeSid(sub)
	proto := fmt.Sprintf(subProto, sub.subject, sub.queue, rsid)
	s.broadcastInterestToRoutes(proto)
}

// broadcastUnSubscribe will forward a client unsubscribe
// action to all active routes.
func (s *Server) broadcastUnSubscribe(sub *subscription) {
	if s.numRoutes() == 0 {
		return
	}
	rsid := routeSid(sub)
	maxStr := _EMPTY_
	sub.client.mu.Lock()
	// Set max if we have it set and have not tripped auto-unsubscribe
	if sub.max > 0 && sub.nm < sub.max {
		maxStr = fmt.Sprintf(" %d", sub.max)
	}
	sub.client.mu.Unlock()
	proto := fmt.Sprintf(unsubProto, rsid, maxStr)
	s.broadcastInterestToRoutes(proto)
}

func (s *Server) routeAcceptLoop(ch chan struct{}) {
	defer func() {
		if ch != nil {
			close(ch)
		}
	}()

	// Snapshot server options.
	opts := s.getOpts()

	// Snapshot server options.
	port := opts.Cluster.Port

	if port == -1 {
		port = 0
	}

	hp := net.JoinHostPort(opts.Cluster.Host, strconv.Itoa(port))
	s.Noticef("Listening for route connections on %s", hp)
	l, e := net.Listen("tcp", hp)
	if e != nil {
		s.Fatalf("Error listening on router port: %d - %v", opts.Cluster.Port, e)
		return
	}

	s.mu.Lock()
	// Check for TLSConfig
	tlsReq := opts.Cluster.TLSConfig != nil
	info := Info{
		ID:           s.info.ID,
		Version:      s.info.Version,
		AuthRequired: false,
		TLSRequired:  tlsReq,
		TLSVerify:    tlsReq,
		MaxPayload:   s.info.MaxPayload,
	}
	// Set this if only if advertise is not disabled
	if !opts.Cluster.NoAdvertise {
		info.ClientConnectURLs = s.clientConnectURLs
	}
	// If we have selected a random port...
	if port == 0 {
		// Write resolved port back to options.
		opts.Cluster.Port = l.Addr().(*net.TCPAddr).Port
	}
	// Keep track of actual listen port. This will be needed in case of
	// config reload.
	s.clusterActualPort = opts.Cluster.Port
	// Check for Auth items
	if opts.Cluster.Username != "" {
		info.AuthRequired = true
	}
	s.routeInfo = info
	// Possibly override Host/Port and set IP based on Cluster.Advertise
	if err := s.setRouteInfoHostPortAndIP(); err != nil {
		s.Fatalf("Error setting route INFO with Cluster.Advertise value of %s, err=%v", s.opts.Cluster.Advertise, err)
		l.Close()
		s.mu.Unlock()
		return
	}
	// Setup state that can enable shutdown
	s.routeListener = l
	s.mu.Unlock()

	// Let them know we are up
	close(ch)
	ch = nil

	tmpDelay := ACCEPT_MIN_SLEEP

	for s.isRunning() {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				s.Debugf("Temporary Route Accept Errorf(%v), sleeping %dms",
					ne, tmpDelay/time.Millisecond)
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else if s.isRunning() {
				s.Noticef("Accept error: %v", err)
			}
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP
		s.startGoRoutine(func() {
			s.createRoute(conn, nil)
			s.grWG.Done()
		})
	}
	s.Debugf("Router accept loop exiting..")
	s.done <- true
}

// Similar to setInfoHostPortAndGenerateJSON, but for routeInfo.
func (s *Server) setRouteInfoHostPortAndIP() error {
	if s.opts.Cluster.Advertise != "" {
		advHost, advPort, err := parseHostPort(s.opts.Cluster.Advertise, s.opts.Cluster.Port)
		if err != nil {
			return err
		}
		s.routeInfo.Host = advHost
		s.routeInfo.Port = advPort
		s.routeInfo.IP = fmt.Sprintf("nats-route://%s/", net.JoinHostPort(advHost, strconv.Itoa(advPort)))
	} else {
		s.routeInfo.Host = s.opts.Cluster.Host
		s.routeInfo.Port = s.opts.Cluster.Port
		s.routeInfo.IP = ""
	}
	// (re)generate the routeInfoJSON byte array
	s.generateRouteInfoJSON()
	return nil
}

// StartRouting will start the accept loop on the cluster host:port
// and will actively try to connect to listed routes.
func (s *Server) StartRouting(clientListenReady chan struct{}) {
	defer s.grWG.Done()

	// Wait for the client listen port to be opened, and
	// the possible ephemeral port to be selected.
	<-clientListenReady

	// Spin up the accept loop
	ch := make(chan struct{})
	go s.routeAcceptLoop(ch)
	<-ch

	// Solicit Routes if needed.
	s.solicitRoutes(s.getOpts().Routes)
}

func (s *Server) reConnectToRoute(rURL *url.URL, rtype RouteType) {
	tryForEver := rtype == Explicit
	// If A connects to B, and B to A (regardless if explicit or
	// implicit - due to auto-discovery), and if each server first
	// registers the route on the opposite TCP connection, the
	// two connections will end-up being closed.
	// Add some random delay to reduce risk of repeated failures.
	delay := time.Duration(rand.Intn(100)) * time.Millisecond
	if tryForEver {
		delay += DEFAULT_ROUTE_RECONNECT
	}
	time.Sleep(delay)
	s.connectToRoute(rURL, tryForEver)
}

func (s *Server) connectToRoute(rURL *url.URL, tryForEver bool) {
	// Snapshot server options.
	opts := s.getOpts()

	defer s.grWG.Done()
	attempts := 0
	for s.isRunning() && rURL != nil {
		s.Debugf("Trying to connect to route on %s", rURL.Host)
		conn, err := net.DialTimeout("tcp", rURL.Host, DEFAULT_ROUTE_DIAL)
		if err != nil {
			s.Errorf("Error trying to connect to route: %v", err)
			if !tryForEver {
				if opts.Cluster.ConnectRetries <= 0 {
					return
				}
				attempts++
				if attempts > opts.Cluster.ConnectRetries {
					return
				}
			}
			select {
			case <-s.rcQuit:
				return
			case <-time.After(DEFAULT_ROUTE_CONNECT):
				continue
			}
		}
		// We have a route connection here.
		// Go ahead and create it and exit this func.
		s.createRoute(conn, rURL)
		return
	}
}

func (c *client) isSolicitedRoute() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.typ == ROUTER && c.route != nil && c.route.didSolicit
}

func (s *Server) solicitRoutes(routes []*url.URL) {
	for _, r := range routes {
		route := r
		s.startGoRoutine(func() { s.connectToRoute(route, true) })
	}
}

func (s *Server) numRoutes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.routes)
}
