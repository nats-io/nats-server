// Copyright 2013 Apcera Inc. All rights reserved.

package server

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

type route struct {
	remoteId   string
	didSolicit bool
	sid        uint64
}

func (s *Server) createRoute(conn net.Conn, didSolicit bool) *client {
	r := &route{didSolicit: didSolicit}
	c := &client{srv: s, nc: conn, opts: defaultOpts, typ: ROUTER, route: r}

	// Initialize
	c.initClient()

	// FIXME(dlc) Check for auth, queue up if solicited.

	// Send our info to the other side.
	s.sendInfo(c)

	// Check for Auth
	if s.routeInfo.AuthRequired {
		ttl := secondsToDuration(s.opts.ClusterAuthTimeout)
		c.setAuthTimer(ttl)
	}

	// Register with the server.
	s.mu.Lock()
	s.routes[c.cid] = c
	s.mu.Unlock()

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

const RSID = "RSID"

func routeSid(sub *subscription) string {
	return fmt.Sprintf("%s:%d:%s", RSID, sub.client.cid, sub.sid)
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
		s.createRoute(conn, false)
	}
	Debug("Router accept loop exiting..")
	s.done <- true
}

// StartCluster will start the accept loop on the cluster host:port
// and will actively try to connect to listed routes.
func (s *Server) StartCluster() {
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
}
