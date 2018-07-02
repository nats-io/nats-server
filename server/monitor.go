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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/gnatsd/server/pse"
)

// Snapshot this
var numCores int

func init() {
	numCores = runtime.NumCPU()
}

// Connz represents detailed information on current client connections.
type Connz struct {
	ID       string      `json:"server_id"`
	Now      time.Time   `json:"now"`
	NumConns int         `json:"num_connections"`
	Total    int         `json:"total"`
	Offset   int         `json:"offset"`
	Limit    int         `json:"limit"`
	Conns    []*ConnInfo `json:"connections"`
}

// ConnzOptions are the options passed to Connz()
type ConnzOptions struct {
	// Sort indicates how the results will be sorted. Check SortOpt for possible values.
	// Only the sort by connection ID (ByCid) is ascending, all others are descending.
	Sort SortOpt `json:"sort"`

	// Username indicates if user names should be included in the results.
	Username bool `json:"auth"`

	// Subscriptions indicates if subscriptions should be included in the results.
	Subscriptions bool `json:"subscriptions"`

	// Offset is used for pagination. Connz() only returns connections starting at this
	// offset from the global results.
	Offset int `json:"offset"`

	// Limit is the maximum number of connections that should be returned by Connz().
	Limit int `json:"limit"`

	// Filter for this explicit client connection.
	CID uint64 `json:"cid"`

	// Filter by connection state.
	State ConnState `json:"state"`
}

// For filtering states of connections. We will only have two, open and closed.
type ConnState int

const (
	ConnOpen = ConnState(iota)
	ConnClosed
	ConnAll
)

// ConnInfo has detailed information on a per connection basis.
type ConnInfo struct {
	Cid            uint64     `json:"cid"`
	IP             string     `json:"ip"`
	Port           int        `json:"port"`
	Start          time.Time  `json:"start"`
	LastActivity   time.Time  `json:"last_activity"`
	Stop           *time.Time `json:"stop,omitempty"`
	Reason         string     `json:"reason,omitempty"`
	RTT            string     `json:"rtt,omitempty"`
	Uptime         string     `json:"uptime"`
	Idle           string     `json:"idle"`
	Pending        int        `json:"pending_bytes"`
	InMsgs         int64      `json:"in_msgs"`
	OutMsgs        int64      `json:"out_msgs"`
	InBytes        int64      `json:"in_bytes"`
	OutBytes       int64      `json:"out_bytes"`
	NumSubs        uint32     `json:"subscriptions"`
	Name           string     `json:"name,omitempty"`
	Lang           string     `json:"lang,omitempty"`
	Version        string     `json:"version,omitempty"`
	TLSVersion     string     `json:"tls_version,omitempty"`
	TLSCipher      string     `json:"tls_cipher_suite,omitempty"`
	AuthorizedUser string     `json:"authorized_user,omitempty"`
	Subs           []string   `json:"subscriptions_list,omitempty"`
}

// DefaultConnListSize is the default size of the connection list.
const DefaultConnListSize = 1024

// DefaultSubListSize is the default size of the subscriptions list.
const DefaultSubListSize = 1024

const defaultStackBufSize = 10000

// Connz returns a Connz struct containing inormation about connections.
func (s *Server) Connz(opts *ConnzOptions) (*Connz, error) {
	var (
		sortOpt = ByCid
		auth    bool
		subs    bool
		offset  int
		limit   = DefaultConnListSize
		cid     = uint64(0)
		state   = ConnOpen
	)

	if opts != nil {
		// If no sort option given or sort is by uptime, then sort by cid
		if opts.Sort == "" {
			sortOpt = ByCid
		} else {
			sortOpt = opts.Sort
			if !sortOpt.IsValid() {
				return nil, fmt.Errorf("Invalid sorting option: %s", sortOpt)
			}
		}
		auth = opts.Username
		subs = opts.Subscriptions
		offset = opts.Offset
		if offset < 0 {
			offset = 0
		}
		limit = opts.Limit
		if limit <= 0 {
			limit = DefaultConnListSize
		}
		// state
		state = opts.State

		// ByStop only makes sense on closed connections
		if sortOpt == ByStop && state != ConnClosed {
			return nil, fmt.Errorf("Sort by stop only valid on closed connections")
		}
		// ByReason is the same.
		if sortOpt == ByReason && state != ConnClosed {
			return nil, fmt.Errorf("Sort by reason only valid on closed connections")
		}

		// If searching by CID
		if opts.CID > 0 {
			cid = opts.CID
			limit = 1
		}
	}

	c := &Connz{
		Offset: offset,
		Limit:  limit,
		Now:    time.Now(),
	}

	// Open clients
	var openClients []*client
	// Hold for closed clients if requested.
	var closedClients []*closedClient

	// Walk the open client list with server lock held.
	s.mu.Lock()

	// copy the server id for monitoring
	c.ID = s.info.ID

	// Number of total clients. The resulting ConnInfo array
	// may be smaller if pagination is used.
	switch state {
	case ConnOpen:
		c.Total = len(s.clients)
	case ConnClosed:
		c.Total = s.closed.len()
		closedClients = s.closed.closedClients()
	case ConnAll:
		c.Total = len(s.clients) + s.closed.len()
		closedClients = s.closed.closedClients()
	}

	totalClients := c.Total
	if cid > 0 { // Meaning we only want 1.
		totalClients = 1
	}
	if state == ConnOpen || state == ConnAll {
		openClients = make([]*client, 0, totalClients)
	}

	// Data structures for results.
	var conns []ConnInfo // Limits allocs for actual ConnInfos.
	var pconns ConnInfos

	switch state {
	case ConnOpen:
		conns = make([]ConnInfo, totalClients)
		pconns = make(ConnInfos, totalClients)
	case ConnClosed:
		pconns = make(ConnInfos, totalClients)
	case ConnAll:
		conns = make([]ConnInfo, cap(openClients))
		pconns = make(ConnInfos, totalClients)
	}

	// Search by individual CID.
	if cid > 0 {
		if state == ConnClosed || state == ConnAll {
			copyClosed := closedClients
			closedClients = nil
			for _, cc := range copyClosed {
				if cc.Cid == cid {
					closedClients = []*closedClient{cc}
					break
				}
			}
		} else if state == ConnOpen || state == ConnAll {
			client := s.clients[cid]
			if client != nil {
				openClients = append(openClients, client)
			}
		}
	} else {
		// Gather all open clients.
		if state == ConnOpen || state == ConnAll {
			for _, client := range s.clients {
				openClients = append(openClients, client)
			}
		}
	}
	s.mu.Unlock()

	// Just return with empty array if nothing here.
	if len(openClients) == 0 && len(closedClients) == 0 {
		c.Conns = ConnInfos{}
		return c, nil
	}

	// Now whip through and generate ConnInfo entries

	// Open Clients
	i := 0
	for _, client := range openClients {
		client.mu.Lock()
		ci := &conns[i]
		ci.fill(client, client.nc, c.Now)
		// Fill in subscription data if requested.
		if subs && len(client.subs) > 0 {
			ci.Subs = make([]string, 0, len(client.subs))
			for _, sub := range client.subs {
				ci.Subs = append(ci.Subs, string(sub.subject))
			}
		}
		// Fill in user if auth requested.
		if auth {
			ci.AuthorizedUser = client.opts.Username
		}
		client.mu.Unlock()
		pconns[i] = ci
		i++
	}
	// Closed Clients
	var needCopy bool
	if subs || auth {
		needCopy = true
	}
	for _, cc := range closedClients {
		// Copy if needed for any changes to the ConnInfo
		if needCopy {
			cx := *cc
			cc = &cx
		}
		// Fill in subscription data if requested.
		if subs && len(cc.subs) > 0 {
			cc.Subs = cc.subs
		}
		// Fill in user if auth requested.
		if auth {
			cc.AuthorizedUser = cc.user
		}
		pconns[i] = &cc.ConnInfo
		i++
	}

	switch sortOpt {
	case ByCid, ByStart:
		sort.Sort(byCid{pconns})
	case BySubs:
		sort.Sort(sort.Reverse(bySubs{pconns}))
	case ByPending:
		sort.Sort(sort.Reverse(byPending{pconns}))
	case ByOutMsgs:
		sort.Sort(sort.Reverse(byOutMsgs{pconns}))
	case ByInMsgs:
		sort.Sort(sort.Reverse(byInMsgs{pconns}))
	case ByOutBytes:
		sort.Sort(sort.Reverse(byOutBytes{pconns}))
	case ByInBytes:
		sort.Sort(sort.Reverse(byInBytes{pconns}))
	case ByLast:
		sort.Sort(sort.Reverse(byLast{pconns}))
	case ByIdle:
		sort.Sort(sort.Reverse(byIdle{pconns}))
	case ByUptime:
		sort.Sort(byUptime{pconns, time.Now()})
	case ByStop:
		sort.Sort(sort.Reverse(byStop{pconns}))
	case ByReason:
		sort.Sort(byReason{pconns})
	}

	minoff := c.Offset
	maxoff := c.Offset + c.Limit

	maxIndex := totalClients

	// Make sure these are sane.
	if minoff > maxIndex {
		minoff = maxIndex
	}
	if maxoff > maxIndex {
		maxoff = maxIndex
	}

	// Now pare down to the requested size.
	// TODO(dlc) - for very large number of connections we
	// could save the whole list in a hash, send hash on first
	// request and allow users to use has for subsequent pages.
	// Low TTL, say < 1sec.
	c.Conns = pconns[minoff:maxoff]
	c.NumConns = len(c.Conns)

	return c, nil
}

// Fills in the ConnInfo from the client.
// client should be locked.
func (ci *ConnInfo) fill(client *client, nc net.Conn, now time.Time) {
	ci.Cid = client.cid
	ci.Start = client.start
	ci.LastActivity = client.last
	ci.Uptime = myUptime(now.Sub(client.start))
	ci.Idle = myUptime(now.Sub(client.last))
	ci.RTT = client.getRTT()
	ci.OutMsgs = client.outMsgs
	ci.OutBytes = client.outBytes
	ci.NumSubs = uint32(len(client.subs))
	ci.Pending = int(client.out.pb)
	ci.Name = client.opts.Name
	ci.Lang = client.opts.Lang
	ci.Version = client.opts.Version
	// inMsgs and inBytes are updated outside of the client's lock, so
	// we need to use atomic here.
	ci.InMsgs = atomic.LoadInt64(&client.inMsgs)
	ci.InBytes = atomic.LoadInt64(&client.inBytes)

	// If the connection is gone, too bad, we won't set TLSVersion and TLSCipher.
	// Exclude clients that are still doing handshake so we don't block in
	// ConnectionState().
	if client.flags.isSet(handshakeComplete) && nc != nil {
		conn := nc.(*tls.Conn)
		cs := conn.ConnectionState()
		ci.TLSVersion = tlsVersion(cs.Version)
		ci.TLSCipher = tlsCipher(cs.CipherSuite)
	}

	switch conn := nc.(type) {
	case *net.TCPConn, *tls.Conn:
		addr := conn.RemoteAddr().(*net.TCPAddr)
		ci.Port = addr.Port
		ci.IP = addr.IP.String()
	}
}

// Assume lock is held
func (c *client) getRTT() string {
	if c.rtt == 0 {
		// If a real client, go ahead and send ping now to get a value
		// for RTT. For tests and telnet, etc skip.
		if c.flags.isSet(connectReceived) && c.opts.Lang != "" {
			c.sendPing()
		}
		return ""
	}
	var rtt time.Duration
	if c.rtt > time.Microsecond && c.rtt < time.Millisecond {
		rtt = c.rtt.Truncate(time.Microsecond)
	} else {
		rtt = c.rtt.Truncate(time.Millisecond)
	}
	return rtt.String()
}

func decodeBool(w http.ResponseWriter, r *http.Request, param string) (bool, error) {
	str := r.URL.Query().Get(param)
	if str == "" {
		return false, nil
	}
	val, err := strconv.ParseBool(str)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("Error decoding boolean for '%s': %v", param, err)))
		return false, err
	}
	return val, nil
}

func decodeUint64(w http.ResponseWriter, r *http.Request, param string) (uint64, error) {
	str := r.URL.Query().Get(param)
	if str == "" {
		return 0, nil
	}
	val, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("Error decoding uint64 for '%s': %v", param, err)))
		return 0, err
	}
	return val, nil
}

func decodeInt(w http.ResponseWriter, r *http.Request, param string) (int, error) {
	str := r.URL.Query().Get(param)
	if str == "" {
		return 0, nil
	}
	val, err := strconv.Atoi(str)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("Error decoding int for '%s': %v", param, err)))
		return 0, err
	}
	return val, nil
}

func decodeState(w http.ResponseWriter, r *http.Request) (ConnState, error) {
	str := r.URL.Query().Get("state")
	if str == "" {
		return ConnOpen, nil
	}
	switch strings.ToLower(str) {
	case "open":
		return ConnOpen, nil
	case "closed":
		return ConnClosed, nil
	case "any", "all":
		return ConnAll, nil
	}
	// We do not understand intended state here.
	w.WriteHeader(http.StatusBadRequest)
	err := fmt.Errorf("Error decoding state for %s", str)
	w.Write([]byte(err.Error()))
	return 0, err
}

// HandleConnz process HTTP requests for connection information.
func (s *Server) HandleConnz(w http.ResponseWriter, r *http.Request) {
	sortOpt := SortOpt(r.URL.Query().Get("sort"))
	auth, err := decodeBool(w, r, "auth")
	if err != nil {
		return
	}
	subs, err := decodeBool(w, r, "subs")
	if err != nil {
		return
	}
	offset, err := decodeInt(w, r, "offset")
	if err != nil {
		return
	}
	limit, err := decodeInt(w, r, "limit")
	if err != nil {
		return
	}
	cid, err := decodeUint64(w, r, "cid")
	if err != nil {
		return
	}
	state, err := decodeState(w, r)
	if err != nil {
		return
	}

	connzOpts := &ConnzOptions{
		Sort:          sortOpt,
		Username:      auth,
		Subscriptions: subs,
		Offset:        offset,
		Limit:         limit,
		CID:           cid,
		State:         state,
	}

	s.mu.Lock()
	s.httpReqStats[ConnzPath]++
	s.mu.Unlock()

	c, err := s.Connz(connzOpts)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		s.Errorf("Error marshaling response to /connz request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// Routez represents detailed information on current client connections.
type Routez struct {
	ID        string       `json:"server_id"`
	Now       time.Time    `json:"now"`
	NumRoutes int          `json:"num_routes"`
	Routes    []*RouteInfo `json:"routes"`
}

// RoutezOptions are options passed to Routez
type RoutezOptions struct {
	// Subscriptions indicates that Routez will return a route's subscriptions
	Subscriptions bool `json:"subscriptions"`
}

// RouteInfo has detailed information on a per connection basis.
type RouteInfo struct {
	Rid          uint64   `json:"rid"`
	RemoteID     string   `json:"remote_id"`
	DidSolicit   bool     `json:"did_solicit"`
	IsConfigured bool     `json:"is_configured"`
	IP           string   `json:"ip"`
	Port         int      `json:"port"`
	Pending      int      `json:"pending_size"`
	InMsgs       int64    `json:"in_msgs"`
	OutMsgs      int64    `json:"out_msgs"`
	InBytes      int64    `json:"in_bytes"`
	OutBytes     int64    `json:"out_bytes"`
	NumSubs      uint32   `json:"subscriptions"`
	Subs         []string `json:"subscriptions_list,omitempty"`
}

// Routez returns a Routez struct containing inormation about routes.
func (s *Server) Routez(routezOpts *RoutezOptions) (*Routez, error) {
	rs := &Routez{Routes: []*RouteInfo{}}
	rs.Now = time.Now()

	subs := routezOpts != nil && routezOpts.Subscriptions

	// Walk the list
	s.mu.Lock()
	rs.NumRoutes = len(s.routes)

	// copy the server id for monitoring
	rs.ID = s.info.ID

	for _, r := range s.routes {
		r.mu.Lock()
		ri := &RouteInfo{
			Rid:          r.cid,
			RemoteID:     r.route.remoteID,
			DidSolicit:   r.route.didSolicit,
			IsConfigured: r.route.routeType == Explicit,
			InMsgs:       atomic.LoadInt64(&r.inMsgs),
			OutMsgs:      r.outMsgs,
			InBytes:      atomic.LoadInt64(&r.inBytes),
			OutBytes:     r.outBytes,
			NumSubs:      uint32(len(r.subs)),
		}

		if subs && len(r.subs) > 0 {
			ri.Subs = make([]string, 0, len(r.subs))
			for _, sub := range r.subs {
				ri.Subs = append(ri.Subs, string(sub.subject))
			}
		}
		switch conn := r.nc.(type) {
		case *net.TCPConn, *tls.Conn:
			addr := conn.RemoteAddr().(*net.TCPAddr)
			ri.Port = addr.Port
			ri.IP = addr.IP.String()
		}
		r.mu.Unlock()
		rs.Routes = append(rs.Routes, ri)
	}
	s.mu.Unlock()
	return rs, nil
}

// HandleRoutez process HTTP requests for route information.
func (s *Server) HandleRoutez(w http.ResponseWriter, r *http.Request) {
	subs, err := decodeBool(w, r, "subs")
	if err != nil {
		return
	}
	var opts *RoutezOptions
	if subs {
		opts = &RoutezOptions{Subscriptions: true}
	}

	s.mu.Lock()
	s.httpReqStats[RoutezPath]++
	s.mu.Unlock()

	// As of now, no error is ever returned.
	rs, _ := s.Routez(opts)
	b, err := json.MarshalIndent(rs, "", "  ")
	if err != nil {
		s.Errorf("Error marshaling response to /routez request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// Subsz represents detail information on current connections.
type Subsz struct {
	*SublistStats
	Total  int         `json:"total"`
	Offset int         `json:"offset"`
	Limit  int         `json:"limit"`
	Subs   []SubDetail `json:"subscriptions_list,omitempty"`
}

// SubszOptions are the options passed to Subsz.
// As of now, there are no options defined.
type SubszOptions struct {
	// Offset is used for pagination. Subsz() only returns connections starting at this
	// offset from the global results.
	Offset int `json:"offset"`

	// Limit is the maximum number of subscriptions that should be returned by Subsz().
	Limit int `json:"limit"`

	// Subscriptions indicates if subscriptions should be included in the results.
	Subscriptions bool `json:"subscriptions"`

	// Test the list against this subject. Needs to be literal since it signifies a publish subject.
	// We will only return subscriptions that would match if a message was sent to this subject.
	Test string `json:"test,omitempty"`
}

type SubDetail struct {
	Subject string `json:"subject"`
	Queue   string `json:"qgroup,omitempty"`
	Sid     string `json:"sid"`
	Msgs    int64  `json:"msgs"`
	Max     int64  `json:"max,omitempty"`
	Cid     uint64 `json:"cid"`
}

// Subsz returns a Subsz struct containing subjects statistics
func (s *Server) Subsz(opts *SubszOptions) (*Subsz, error) {
	var (
		subdetail bool
		test      bool
		offset    int
		limit     = DefaultSubListSize
		testSub   = ""
	)

	if opts != nil {
		subdetail = opts.Subscriptions
		offset = opts.Offset
		if offset < 0 {
			offset = 0
		}
		limit = opts.Limit
		if limit <= 0 {
			limit = DefaultSubListSize
		}
		if opts.Test != "" {
			testSub = opts.Test
			test = true
			if !IsValidLiteralSubject(testSub) {
				return nil, fmt.Errorf("Invalid test subject, must be valid publish subject: %s", testSub)
			}
		}
	}

	sz := &Subsz{s.sl.Stats(), 0, offset, limit, nil}

	if subdetail {
		// Now add in subscription's details
		var raw [4096]*subscription
		subs := raw[:0]

		s.sl.localSubs(&subs)
		details := make([]SubDetail, len(subs))
		i := 0
		// TODO(dlc) - may be inefficient and could just do normal match when total subs is large and filtering.
		for _, sub := range subs {
			// Check for filter
			if test && !matchLiteral(testSub, string(sub.subject)) {
				continue
			}
			if sub.client == nil {
				continue
			}
			sub.client.mu.Lock()
			details[i] = SubDetail{
				Subject: string(sub.subject),
				Queue:   string(sub.queue),
				Sid:     string(sub.sid),
				Msgs:    sub.nm,
				Max:     sub.max,
				Cid:     sub.client.cid,
			}
			sub.client.mu.Unlock()
			i++
		}
		minoff := sz.Offset
		maxoff := sz.Offset + sz.Limit

		maxIndex := i

		// Make sure these are sane.
		if minoff > maxIndex {
			minoff = maxIndex
		}
		if maxoff > maxIndex {
			maxoff = maxIndex
		}
		sz.Subs = details[minoff:maxoff]
		sz.Total = len(sz.Subs)
	}

	return sz, nil
}

// HandleSubsz processes HTTP requests for subjects stats.
func (s *Server) HandleSubsz(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.httpReqStats[SubszPath]++
	s.mu.Unlock()

	subs, err := decodeBool(w, r, "subs")
	if err != nil {
		return
	}
	offset, err := decodeInt(w, r, "offset")
	if err != nil {
		return
	}
	limit, err := decodeInt(w, r, "limit")
	if err != nil {
		return
	}
	testSub := r.URL.Query().Get("test")

	subszOpts := &SubszOptions{
		Subscriptions: subs,
		Offset:        offset,
		Limit:         limit,
		Test:          testSub,
	}

	st, err := s.Subsz(subszOpts)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	var b []byte

	if len(st.Subs) == 0 {
		b, err = json.MarshalIndent(st.SublistStats, "", "  ")
	} else {
		b, err = json.MarshalIndent(st, "", "  ")
	}
	if err != nil {
		s.Errorf("Error marshaling response to /subscriptionsz request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// HandleStacksz processes HTTP requests for getting stacks
func (s *Server) HandleStacksz(w http.ResponseWriter, r *http.Request) {
	// Do not get any lock here that would prevent getting the stacks
	// if we were to have a deadlock somewhere.
	var defaultBuf [defaultStackBufSize]byte
	size := defaultStackBufSize
	buf := defaultBuf[:size]
	n := 0
	for {
		n = runtime.Stack(buf, true)
		if n < size {
			break
		}
		size *= 2
		buf = make([]byte, size)
	}
	// Handle response
	ResponseHandler(w, r, buf[:n])
}

// Varz will output server information on the monitoring port at /varz.
type Varz struct {
	*Info
	*Options
	Port             int               `json:"port"`
	MaxPayload       int               `json:"max_payload"`
	Start            time.Time         `json:"start"`
	Now              time.Time         `json:"now"`
	Uptime           string            `json:"uptime"`
	Mem              int64             `json:"mem"`
	Cores            int               `json:"cores"`
	CPU              float64           `json:"cpu"`
	Connections      int               `json:"connections"`
	TotalConnections uint64            `json:"total_connections"`
	Routes           int               `json:"routes"`
	Remotes          int               `json:"remotes"`
	InMsgs           int64             `json:"in_msgs"`
	OutMsgs          int64             `json:"out_msgs"`
	InBytes          int64             `json:"in_bytes"`
	OutBytes         int64             `json:"out_bytes"`
	SlowConsumers    int64             `json:"slow_consumers"`
	MaxPending       int64             `json:"max_pending"`
	WriteDeadline    time.Duration     `json:"write_deadline"`
	Subscriptions    uint32            `json:"subscriptions"`
	HTTPReqStats     map[string]uint64 `json:"http_req_stats"`
	ConfigLoadTime   time.Time         `json:"config_load_time"`
}

// VarzOptions are the options passed to Varz().
// Currently, there are no options defined.
type VarzOptions struct{}

func myUptime(d time.Duration) string {
	// Just use total seconds for uptime, and display days / years
	tsecs := d / time.Second
	tmins := tsecs / 60
	thrs := tmins / 60
	tdays := thrs / 24
	tyrs := tdays / 365

	if tyrs > 0 {
		return fmt.Sprintf("%dy%dd%dh%dm%ds", tyrs, tdays%365, thrs%24, tmins%60, tsecs%60)
	}
	if tdays > 0 {
		return fmt.Sprintf("%dd%dh%dm%ds", tdays, thrs%24, tmins%60, tsecs%60)
	}
	if thrs > 0 {
		return fmt.Sprintf("%dh%dm%ds", thrs, tmins%60, tsecs%60)
	}
	if tmins > 0 {
		return fmt.Sprintf("%dm%ds", tmins, tsecs%60)
	}
	return fmt.Sprintf("%ds", tsecs)
}

// HandleRoot will show basic info and links to others handlers.
func (s *Server) HandleRoot(w http.ResponseWriter, r *http.Request) {
	// This feels dumb to me, but is required: https://code.google.com/p/go/issues/detail?id=4799
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	s.mu.Lock()
	s.httpReqStats[RootPath]++
	s.mu.Unlock()
	fmt.Fprintf(w, `<html lang="en">
   <head>
    <link rel="shortcut icon" href="http://nats.io/img/favicon.ico">
    <style type="text/css">
      body { font-family: "Century Gothic", CenturyGothic, AppleGothic, sans-serif; font-size: 22; }
      a { margin-left: 32px; }
    </style>
  </head>
  <body>
    <img src="http://nats.io/img/logo.png" alt="NATS">
    <br/>
	<a href=/varz>varz</a><br/>
	<a href=/connz>connz</a><br/>
	<a href=/routez>routez</a><br/>
	<a href=/subsz>subsz</a><br/>
    <br/>
    <a href=http://nats.io/documentation/server/gnatsd-monitoring/>help</a>
  </body>
</html>`)
}

// Varz returns a Varz struct containing the server information.
func (s *Server) Varz(varzOpts *VarzOptions) (*Varz, error) {
	// Snapshot server options.
	opts := s.getOpts()

	v := &Varz{Info: &s.info, Options: opts, MaxPayload: opts.MaxPayload, Start: s.start}
	v.Now = time.Now()
	v.Uptime = myUptime(time.Since(s.start))
	v.Port = v.Info.Port

	updateUsage(v)

	s.mu.Lock()
	v.Connections = len(s.clients)
	v.TotalConnections = s.totalClients
	v.Routes = len(s.routes)
	v.Remotes = len(s.remotes)
	v.InMsgs = atomic.LoadInt64(&s.inMsgs)
	v.InBytes = atomic.LoadInt64(&s.inBytes)
	v.OutMsgs = atomic.LoadInt64(&s.outMsgs)
	v.OutBytes = atomic.LoadInt64(&s.outBytes)
	v.SlowConsumers = atomic.LoadInt64(&s.slowConsumers)
	v.MaxPending = opts.MaxPending
	v.WriteDeadline = opts.WriteDeadline
	v.Subscriptions = s.sl.Count()
	v.ConfigLoadTime = s.configTime
	// Need a copy here since s.httpReqStats can change while doing
	// the marshaling down below.
	v.HTTPReqStats = make(map[string]uint64, len(s.httpReqStats))
	for key, val := range s.httpReqStats {
		v.HTTPReqStats[key] = val
	}
	s.mu.Unlock()

	return v, nil
}

// HandleVarz will process HTTP requests for server information.
func (s *Server) HandleVarz(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.httpReqStats[VarzPath]++
	s.mu.Unlock()

	// As of now, no error is ever returned
	v, _ := s.Varz(nil)
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		s.Errorf("Error marshaling response to /varz request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// Grab RSS and PCPU
func updateUsage(v *Varz) {
	var rss, vss int64
	var pcpu float64

	pse.ProcUsage(&pcpu, &rss, &vss)

	v.Mem = rss
	v.CPU = pcpu
	v.Cores = numCores
}

// ResponseHandler handles responses for monitoring routes
func ResponseHandler(w http.ResponseWriter, r *http.Request, data []byte) {
	// Get callback from request
	callback := r.URL.Query().Get("callback")
	// If callback is not empty then
	if callback != "" {
		// Response for JSONP
		w.Header().Set("Content-Type", "application/javascript")
		fmt.Fprintf(w, "%s(%s)", callback, data)
	} else {
		// Otherwise JSON
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}
}

func (reason ClosedState) String() string {
	switch reason {
	case ClientClosed:
		return "Client"
	case AuthenticationTimeout:
		return "Authentication Timeout"
	case AuthenticationViolation:
		return "Authentication Failure"
	case TLSHandshakeError:
		return "TLS Handshake Failure"
	case SlowConsumerPendingBytes:
		return "Slow Consumer (Pending Bytes)"
	case SlowConsumerWriteDeadline:
		return "Slow Consumer (Write Deadline)"
	case WriteError:
		return "Write Error"
	case ReadError:
		return "Read Error"
	case ParseError:
		return "Parse Error"
	case StaleConnection:
		return "Stale Connection"
	case ProtocolViolation:
		return "Protocol Violation"
	case BadClientProtocolVersion:
		return "Bad Client Protocol Version"
	case WrongPort:
		return "Incorrect Port"
	case MaxConnectionsExceeded:
		return "Maximum Connections Exceeded"
	case MaxPayloadExceeded:
		return "Maximum Message Payload Exceeded"
	case MaxControlLineExceeded:
		return "Maximum Control Line Exceeded"
	case DuplicateRoute:
		return "Duplicate Route"
	case RouteRemoved:
		return "Route Removed"
	case ServerShutdown:
		return "Server Shutdown"
	}
	return "Unknown State"
}
