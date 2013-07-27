// Copyright 2013 Apcera Inc. All rights reserved.

package server

import (
	"bufio"
	"fmt"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type route struct {
	mu   sync.Mutex
	rid  uint64
	conn net.Conn
	bw   *bufio.Writer
	srv  *Server
	url  *url.URL
	atmr *time.Timer
	ptmr *time.Timer
	pout int
	parseState
	stats
}

func (r *route) String() string {
	return fmt.Sprintf("rid:%d", r.rid)
}

func (s *Server) createRoute(conn net.Conn) *route {
	r := &route{srv: s, conn: conn}
	r.rid = atomic.AddUint64(&s.grid, 1)
	r.bw = bufio.NewWriterSize(conn, defaultBufSize)
	return r
}
