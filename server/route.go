// Copyright 2013 Apcera Inc. All rights reserved.

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"time"
)

type route struct {
	remoteId   string
	didSolicit bool
	url        *url.URL
}

type connectInfo struct {
	Verbose  bool   `json:"verbose"`
	Pedantic bool   `json:"pedantic"`
	User     string `json:"user,omitempty"`
	Pass     string `json:"pass,omitempty"`
	Ssl      bool   `json:"ssl_required"`
	Name     string `json:"name"`
}

const conProto = "CONNECT %s" + _CRLF_

func (c *client) sendConnect() {
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
		Ssl:      false,
		Name:     c.srv.info.Id,
	}
	b, err := json.Marshal(cinfo)
	if err != nil {
		Logf("Error marshalling CONNECT to route: %v\n", err)
		c.closeConnection()
	}
	c.bw.WriteString(fmt.Sprintf(conProto, b))
	c.bw.Flush()
}

func (s *Server) sendLocalSubsToRoute(route *client) {
	for _, client := range s.clients {
		for _, s := range client.subs.All() {
			if sub, ok := s.(*subscription); ok {
				rsid := routeSid(sub)
				proto := fmt.Sprintf(subProto, sub.subject, sub.queue, rsid)
				route.bw.WriteString(proto)
			}
		}
	}
	route.bw.Flush()
	Debug("Route sent local subscriptions", clientConnStr(route.nc), route.cid)
}

func (s *Server) createRoute(conn net.Conn, rUrl *url.URL) *client {
	didSolicit := rUrl != nil
	r := &route{didSolicit: didSolicit}
	c := &client{srv: s, nc: conn, opts: defaultOpts, typ: ROUTER, route: r}

	// Initialize
	c.initClient()

	Debug("Route connection created", clientConnStr(c.nc), c.cid)

	// Queue Connect proto if we solicited the connection.
	if didSolicit {
		r.url = rUrl
		Debug("Route connect msg sent", clientConnStr(c.nc), c.cid)
		c.sendConnect()
	}

	// Send our info to the other side.
	s.sendInfo(c)

	// Check for Auth required state for incoming connections.
	if s.routeInfo.AuthRequired && !didSolicit {
		ttl := secondsToDuration(s.opts.ClusterAuthTimeout)
		c.setAuthTimer(ttl)
	}

	// Register with the server.
	s.mu.Lock()
	s.routes[c.cid] = c
	s.mu.Unlock()

	// Send our local subscriptions to this route.
	s.sendLocalSubsToRoute(c)

	return c
}

const (
	_CRLF_  = "\r\n"
	_EMPTY_ = ""
	_SPC_   = " "
)

const (
	subProto   = "SUB %s %s %s" + _CRLF_
	unsubProto = "UNSUB %s%s" + _CRLF_
)

// FIXME(dlc) - Make these reserved and reject if they come in as a sid
// from a client connection.

const (
	RSID  = "RSID"
	QRSID = "QRSID"

	RSID_CID_INDEX   = 1
	RSID_SID_INDEX   = 2
	EXPECTED_MATCHES = 3
)

// FIXME(dlc) - This may be too slow, check at later date.
var qrsidRe = regexp.MustCompile(`QRSID:(\d+):([^\s]+)`)

func (s *Server) routeSidQueueSubscriber(rsid []byte) *subscription {
	if !bytes.HasPrefix(rsid, []byte(QRSID)) {
		return nil
	}
	matches := qrsidRe.FindSubmatch(rsid)
	if matches == nil || len(matches) != EXPECTED_MATCHES {
		return nil
	}
	cid := uint64(parseInt64(matches[RSID_CID_INDEX]))
	client := s.clients[cid]
	if client == nil {
		return nil
	}
	sid := matches[RSID_SID_INDEX]
	if sub, ok := (client.subs.Get(sid)).(*subscription); ok {
		return sub
	}
	return nil
}

func routeSid(sub *subscription) string {
	var qi string
	if len(sub.queue) > 0 {
		qi = "Q"
	}
	return fmt.Sprintf("%s%s:%d:%s", qi, RSID, sub.client.cid, sub.sid)
}

func (s *Server) broadcastToRoutes(proto string) {
	for _, route := range s.routes {
		// FIXME(dlc) - Make same logic as deliverMsg
		route.bw.WriteString(proto)
		route.bw.Flush()
	}
}

// broadcastSubscribe will forward a client subscription
// to all active routes.
func (s *Server) broadcastSubscribe(sub *subscription) {
	rsid := routeSid(sub)
	proto := fmt.Sprintf(subProto, sub.subject, sub.queue, rsid)
	s.broadcastToRoutes(proto)
}

// broadcastUnSubscribe will forward a client unsubscribe
// action to all active routes.
func (s *Server) broadcastUnSubscribe(sub *subscription) {
	rsid := routeSid(sub)
	maxStr := _EMPTY_
	if sub.max > 0 {
		maxStr = fmt.Sprintf("%d ", sub.max)
	}
	proto := fmt.Sprintf(unsubProto, maxStr, rsid)
	s.broadcastToRoutes(proto)
}

func (s *Server) routeAcceptLoop(ch chan struct{}) {
	hp := fmt.Sprintf("%s:%d", s.opts.ClusterHost, s.opts.ClusterPort)
	Logf("Listening for route connections on %s", hp)
	l, e := net.Listen("tcp", hp)
	if e != nil {
		Fatalf("Error listening on router port: %d - %v", s.opts.Port, e)
		return
	}

	// Let them know we are up
	close(ch)

	// Setup state that can enable shutdown
	s.mu.Lock()
	s.routeListener = l
	s.mu.Unlock()

	tmpDelay := ACCEPT_MIN_SLEEP

	for s.isRunning() {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				Debug("Temporary Route Accept Error(%v), sleeping %dms",
					ne, tmpDelay/time.Millisecond)
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else if s.isRunning() {
				Logf("Accept error: %v", err)
			}
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP
		s.createRoute(conn, nil)
	}
	Debug("Router accept loop exiting..")
	s.done <- true
}

// StartRouting will start the accept loop on the cluster host:port
// and will actively try to connect to listed routes.
func (s *Server) StartRouting() {
	info := Info{
		Id:           s.info.Id,
		Version:      s.info.Version,
		Host:         s.opts.ClusterHost,
		Port:         s.opts.ClusterPort,
		AuthRequired: false,
		SslRequired:  false,
		MaxPayload:   MAX_PAYLOAD_SIZE,
	}
	// Check for Auth items
	if s.opts.ClusterUsername != "" {
		info.AuthRequired = true
	}
	s.routeInfo = info
	// Generate the info json
	b, err := json.Marshal(info)
	if err != nil {
		Fatalf("Error marshalling Route INFO JSON: %+v\n", err)
	}
	s.routeInfoJson = []byte(fmt.Sprintf("INFO %s %s", b, CR_LF))

	// Spin up the accept loop
	ch := make(chan struct{})
	go s.routeAcceptLoop(ch)
	<-ch

	// Solicit Routes if needed.
	s.solicitRoutes()
}

// FIXME(dlc): Need to shutdown when exiting
func (s *Server) connectToRoute(rUrl *url.URL) {
	for s.isRunning() {
		Debugf("Trying to connect to route on %s", rUrl.Host)
		conn, err := net.DialTimeout("tcp", rUrl.Host, DEFAULT_ROUTE_DIAL)
		if err != nil {
			Debugf("Error trying to connect to route: %v", err)
			// FIXME(dlc): wait on kick out
			time.Sleep(DEFAULT_ROUTE_CONNECT)
			continue
		}
		// We have a connection here. Go ahead and create it and
		// exit this func.
		s.createRoute(conn, rUrl)
		return
	}
}

func (c *client) isSolicitedRoute() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.typ == ROUTER && c.route != nil && c.route.didSolicit
}

func (s *Server) solicitRoutes() {
	for _, r := range s.opts.Routes {
		go s.connectToRoute(r)
	}
}
