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
	Echo     bool   `json:"echo"`
	Verbose  bool   `json:"verbose"`
	Pedantic bool   `json:"pedantic"`
	User     string `json:"user,omitempty"`
	Pass     string `json:"pass,omitempty"`
	TLS      bool   `json:"tls_required"`
	Name     string `json:"name"`
}

// Used to hold onto mappings for unsubscribed
// routed queue subscribers.
type rqsub struct {
	group []byte
	atime time.Time
}

// Route protocol constants
const (
	ConProto  = "CONNECT %s" + _CRLF_
	InfoProto = "INFO %s" + _CRLF_
)

// Clear up the timer and any map held for remote qsubs.
func (s *Server) clearRemoteQSubs() {
	s.rqsMu.Lock()
	defer s.rqsMu.Unlock()
	if s.rqsubsTimer != nil {
		s.rqsubsTimer.Stop()
		s.rqsubsTimer = nil
	}
	s.rqsubs = nil
}

// Check to see if we can remove any of the remote qsubs mappings
func (s *Server) purgeRemoteQSubs() {
	ri := s.getOpts().RQSubsSweep
	s.rqsMu.Lock()
	exp := time.Now().Add(-ri)
	for k, rqsub := range s.rqsubs {
		if exp.After(rqsub.atime) {
			delete(s.rqsubs, k)
		}
	}
	if s.rqsubsTimer != nil {
		// Reset timer.
		s.rqsubsTimer = time.AfterFunc(ri, s.purgeRemoteQSubs)
	}
	s.rqsMu.Unlock()
}

// Lookup a remote queue group sid.
func (s *Server) lookupRemoteQGroup(sid string) []byte {
	s.rqsMu.RLock()
	rqsub := s.rqsubs[sid]
	s.rqsMu.RUnlock()
	return rqsub.group
}

// This will hold onto a remote queue subscriber to allow
// for mapping and handling if we get a message after the
// subscription goes away.
func (s *Server) holdRemoteQSub(sub *subscription) {
	// Should not happen, but protect anyway.
	if len(sub.queue) == 0 {
		return
	}
	// Add the entry
	s.rqsMu.Lock()
	// Start timer if needed.
	if s.rqsubsTimer == nil {
		ri := s.getOpts().RQSubsSweep
		s.rqsubsTimer = time.AfterFunc(ri, s.purgeRemoteQSubs)
	}
	// Create map if needed.
	if s.rqsubs == nil {
		s.rqsubs = make(map[string]rqsub)
	}
	group := make([]byte, len(sub.queue))
	copy(group, sub.queue)
	rqsub := rqsub{group: group, atime: time.Now()}
	s.rqsubs[routeSid(sub)] = rqsub
	s.rqsMu.Unlock()
}

// This is for when we receive a directed message for a queue subscriber
// that has gone away. We reroute like a new message but scope to only
// the queue subscribers that it was originally intended for. We will
// prefer local clients, but will bounce to another route if needed.
func (c *client) reRouteQMsg(r *SublistResult, msgh, msg, group []byte) {
	c.Debugf("Attempting redelivery of message for absent queue subscriber on group '%q'", group)

	// We only care about qsubs here. Data structure not setup for optimized
	// lookup for our specific group however.

	var qsubs []*subscription
	for _, qs := range r.qsubs {
		if len(qs) != 0 && bytes.Equal(group, qs[0].queue) {
			qsubs = qs
			break
		}
	}

	// If no match return.
	if qsubs == nil {
		c.Debugf("Redelivery failed, no queue subscribers for message on group '%q'", group)
		return
	}

	// We have a matched group of queue subscribers.
	// We prefer a local subscriber since that was the original target.

	// Spin prand if needed.
	if c.in.prand == nil {
		c.in.prand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	// Hold onto a remote if we come across it to utilize in case no locals exist.
	var rsub *subscription

	startIndex := c.in.prand.Intn(len(qsubs))
	for i := 0; i < len(qsubs); i++ {
		index := (startIndex + i) % len(qsubs)
		sub := qsubs[index]
		if sub == nil {
			continue
		}
		if rsub == nil && bytes.HasPrefix(sub.sid, []byte(QRSID)) {
			rsub = sub
			continue
		}
		mh := c.msgHeader(msgh[:], sub)
		if c.deliverMsg(sub, mh, msg) {
			c.Debugf("Redelivery succeeded for message on group '%q'", group)
			return
		}
	}
	// If we are here we failed to find a local, see if we snapshotted a
	// remote sub, and if so deliver to that.
	if rsub != nil {
		mh := c.msgHeader(msgh[:], rsub)
		if c.deliverMsg(rsub, mh, msg) {
			c.Debugf("Re-routing message on group '%q' to remote server", group)
			return
		}
	}
	c.Debugf("Redelivery failed, no queue subscribers for message on group '%q'", group)
}

// processRoutedMsg processes messages inbound from a route.
func (c *client) processRoutedMsg(r *SublistResult, msg []byte) {
	// Snapshot server.
	srv := c.srv

	msgh := c.prepMsgHeader()
	si := len(msgh)

	// If we have a queue subscription, deliver direct
	// since they are sent direct via L2 semantics over routes.
	// If the match is a queue subscription, we will return from
	// here regardless if we find a sub.
	isq, sub, err := srv.routeSidQueueSubscriber(c.pa.sid)
	if isq {
		if err != nil {
			// We got an invalid QRSID, so stop here
			c.Errorf("Unable to deliver routed queue message: %v", err)
			return
		}
		didDeliver := false
		if sub != nil {
			mh := c.msgHeader(msgh[:si], sub)
			didDeliver = c.deliverMsg(sub, mh, msg)
		}
		if !didDeliver && c.srv != nil {
			group := c.srv.lookupRemoteQGroup(string(c.pa.sid))
			c.reRouteQMsg(r, msgh, msg, group)
		}
		return
	}
	// Normal pub/sub message here
	// Loop over all normal subscriptions that match.
	for _, sub := range r.psubs {
		// Check if this is a send to a ROUTER, if so we ignore to
		// enforce 1-hop semantics.
		if sub.client.typ == ROUTER {
			continue
		}
		sub.client.mu.Lock()
		if sub.client.nc == nil {
			sub.client.mu.Unlock()
			continue
		}
		sub.client.mu.Unlock()

		// Normal delivery
		mh := c.msgHeader(msgh[:si], sub)
		c.deliverMsg(sub, mh, msg)
	}
}

// Lock should be held entering here.
func (c *client) sendConnect(tlsRequired bool) {
	var user, pass string
	if userInfo := c.route.url.User; userInfo != nil {
		user = userInfo.Username()
		pass, _ = userInfo.Password()
	}
	cinfo := connectInfo{
		Echo:     true,
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
		c.closeConnection(ProtocolViolation)
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
		c.closeConnection(DuplicateRoute)
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
			c.closeConnection(ParseError)
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
		c.closeConnection(DuplicateRoute)
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
			c.sendInfo(c.generateClientInfoJSON(s.copyInfo()))
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

// canImport is whether or not we will send a SUB for interest to the other side.
// This is for ROUTER connections only.
// Lock is held on entry.
func (c *client) canImport(subject []byte) bool {
	// Use pubAllowed() since this checks Publish permissions which
	// is what Import maps to.
	return c.pubAllowed(subject)
}

// canExport is whether or not we will accept a SUB from the remote for a given subject.
// This is for ROUTER connections only.
// Lock is held on entry
func (c *client) canExport(subject []byte) bool {
	// Use canSubscribe() since this checks Subscribe permissions which
	// is what Export maps to.
	return c.canSubscribe(subject)
}

// Initialize or reset cluster's permissions.
// This is for ROUTER connections only.
// Client lock is held on entry
func (c *client) setRoutePermissions(perms *RoutePermissions) {
	// Reset if some were set
	if perms == nil {
		c.perms = nil
		return
	}
	// Convert route permissions to user permissions.
	// The Import permission is mapped to Publish
	// and Export permission is mapped to Subscribe.
	// For meaning of Import/Export, see canImport and canExport.
	p := &Permissions{
		Publish:   perms.Import,
		Subscribe: perms.Export,
	}
	c.setPermissions(p)
}

// This will send local subscription state to a new route connection.
// FIXME(dlc) - This could be a DOS or perf issue with many clients
// and large subscription space. Plus buffering in place not a good idea.
func (s *Server) sendLocalSubsToRoute(route *client) {
	var raw [4096]*subscription
	subs := raw[:0]

	s.sl.localSubs(&subs)

	route.mu.Lock()
	for _, sub := range subs {
		// Send SUB interest only if subject has a match in import permissions
		if !route.canImport(sub.subject) {
			continue
		}
		proto := fmt.Sprintf(subProto, sub.subject, sub.queue, routeSid(sub))
		route.queueOutbound([]byte(proto))
		if route.out.pb > int64(route.out.sz*2) {
			route.flushSignal()
		}
	}
	route.flushSignal()
	route.mu.Unlock()

	route.Debugf("Sent local subscriptions to route")
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

		// Set permissions associated with the route user (if applicable).
		// No lock needed since we are already under client lock.
		c.setRoutePermissions(opts.Cluster.Permissions)
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
			c.closeConnection(TLSHandshakeError)
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
		c.closeConnection(ServerShutdown)
		return nil
	}

	// Check for Auth required state for incoming connections.
	// Make sure to do this before spinning up readLoop.
	if authRequired && !didSolicit {
		ttl := secondsToDuration(opts.Cluster.AuthTimeout)
		c.setAuthTimer(ttl)
	}

	// Spin up the read loop.
	s.startGoRoutine(func() { c.readLoop() })

	// Spin up the write loop.
	s.startGoRoutine(c.writeLoop)

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
	unsubProto = "UNSUB %s" + _CRLF_
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

// Creates a routable sid that can be used
// to reach remote subscriptions.
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
		s.routes[c.cid] = c
		s.remotes[id] = c
		c.mu.Lock()
		c.route.connectURLs = info.ClientConnectURLs
		cid := c.cid
		c.mu.Unlock()

		// Remove from the temporary map
		s.grMu.Lock()
		delete(s.grTmpClients, cid)
		s.grMu.Unlock()

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

func (s *Server) broadcastInterestToRoutes(sub *subscription, proto string) {
	var arg []byte
	if atomic.LoadInt32(&s.logging.trace) == 1 {
		arg = []byte(proto[:len(proto)-LEN_CR_LF])
	}
	protoAsBytes := []byte(proto)
	s.mu.Lock()
	for _, route := range s.routes {
		// FIXME(dlc) - Make same logic as deliverMsg
		route.mu.Lock()
		// The permission of this cluster applies to all routes, and each
		// route will have the same `perms`, so check with the first route
		// and send SUB interest only if subject has a match in import permissions.
		// If there is no match, we stop here.
		if !route.canImport(sub.subject) {
			route.mu.Unlock()
			break
		}
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
	s.broadcastInterestToRoutes(sub, proto)
}

// broadcastUnSubscribe will forward a client unsubscribe
// action to all active routes.
func (s *Server) broadcastUnSubscribe(sub *subscription) {
	if s.numRoutes() == 0 {
		return
	}
	sub.client.mu.Lock()
	// Max has no meaning on the other side of a route, so do not send.
	hasMax := sub.max > 0 && sub.nm < sub.max
	sub.client.mu.Unlock()
	if hasMax {
		return
	}
	rsid := routeSid(sub)
	proto := fmt.Sprintf(unsubProto, rsid)
	s.broadcastInterestToRoutes(sub, proto)
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
	l, e := net.Listen("tcp", hp)
	if e != nil {
		s.Fatalf("Error listening on router port: %d - %v", opts.Cluster.Port, e)
		return
	}
	s.Noticef("Listening for route connections on %s",
		net.JoinHostPort(opts.Cluster.Host, strconv.Itoa(l.Addr().(*net.TCPAddr).Port)))

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

// Checks to make sure the route is still valid.
func (s *Server) routeStillValid(rURL *url.URL) bool {
	for _, ri := range s.getOpts().Routes {
		if ri == rURL {
			return true
		}
	}
	return false
}

func (s *Server) connectToRoute(rURL *url.URL, tryForEver bool) {
	// Snapshot server options.
	opts := s.getOpts()

	defer s.grWG.Done()

	attempts := 0
	for s.isRunning() && rURL != nil {
		if tryForEver && !s.routeStillValid(rURL) {
			return
		}
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
			case <-s.quitCh:
				return
			case <-time.After(DEFAULT_ROUTE_CONNECT):
				continue
			}
		}

		if tryForEver && !s.routeStillValid(rURL) {
			conn.Close()
			return
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
