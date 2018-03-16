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
	ID       string     `json:"server_id"`
	Now      time.Time  `json:"now"`
	NumConns int        `json:"num_connections"`
	Total    int        `json:"total"`
	Offset   int        `json:"offset"`
	Limit    int        `json:"limit"`
	Conns    []ConnInfo `json:"connections"`
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
}

// ConnInfo has detailed information on a per connection basis.
type ConnInfo struct {
	Cid            uint64    `json:"cid"`
	IP             string    `json:"ip"`
	Port           int       `json:"port"`
	Start          time.Time `json:"start"`
	LastActivity   time.Time `json:"last_activity"`
	Uptime         string    `json:"uptime"`
	Idle           string    `json:"idle"`
	Pending        int       `json:"pending_bytes"`
	InMsgs         int64     `json:"in_msgs"`
	OutMsgs        int64     `json:"out_msgs"`
	InBytes        int64     `json:"in_bytes"`
	OutBytes       int64     `json:"out_bytes"`
	NumSubs        uint32    `json:"subscriptions"`
	Name           string    `json:"name,omitempty"`
	Lang           string    `json:"lang,omitempty"`
	Version        string    `json:"version,omitempty"`
	TLSVersion     string    `json:"tls_version,omitempty"`
	TLSCipher      string    `json:"tls_cipher_suite,omitempty"`
	AuthorizedUser string    `json:"authorized_user,omitempty"`
	Subs           []string  `json:"subscriptions_list,omitempty"`
}

// DefaultConnListSize is the default size of the connection list.
const DefaultConnListSize = 1024

const defaultStackBufSize = 10000

// Connz returns a Connz struct containing inormation about connections.
func (s *Server) Connz(opts *ConnzOptions) (*Connz, error) {
	var (
		sortOpt = ByCid
		auth    bool
		subs    bool
		offset  int
		limit   = DefaultConnListSize
	)

	if opts != nil {
		// If no sort option given or sort is by uptime, then sort by cid
		if opts.Sort == "" || opts.Sort == ByUptime {
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
	}

	c := &Connz{
		Offset: offset,
		Limit:  limit,
		Now:    time.Now(),
	}

	// Walk the list
	s.mu.Lock()
	tlsRequired := s.info.TLSRequired

	// copy the server id for monitoring
	c.ID = s.info.ID

	// number total of clients. The resulting ConnInfo array
	// may be smaller if pagination is used.
	totalClients := len(s.clients)
	c.Total = totalClients

	i := 0
	pairs := make(Pairs, totalClients)
	for _, client := range s.clients {
		client.mu.Lock()
		switch sortOpt {
		case ByCid:
			pairs[i] = Pair{Key: client, Val: int64(client.cid)}
		case BySubs:
			pairs[i] = Pair{Key: client, Val: int64(len(client.subs))}
		case ByPending:
			pairs[i] = Pair{Key: client, Val: int64(client.bw.Buffered())}
		case ByOutMsgs:
			pairs[i] = Pair{Key: client, Val: client.outMsgs}
		case ByInMsgs:
			pairs[i] = Pair{Key: client, Val: atomic.LoadInt64(&client.inMsgs)}
		case ByOutBytes:
			pairs[i] = Pair{Key: client, Val: client.outBytes}
		case ByInBytes:
			pairs[i] = Pair{Key: client, Val: atomic.LoadInt64(&client.inBytes)}
		case ByLast:
			pairs[i] = Pair{Key: client, Val: client.last.UnixNano()}
		case ByIdle:
			pairs[i] = Pair{Key: client, Val: c.Now.Sub(client.last).Nanoseconds()}
		}
		client.mu.Unlock()
		i++
	}
	s.mu.Unlock()

	if totalClients > 0 {
		if sortOpt == ByCid {
			// Return in ascending order
			sort.Sort(pairs)
		} else {
			// Return in descending order
			sort.Sort(sort.Reverse(pairs))
		}
	}

	minoff := c.Offset
	maxoff := c.Offset + c.Limit

	// Make sure these are sane.
	if minoff > totalClients {
		minoff = totalClients
	}
	if maxoff > totalClients {
		maxoff = totalClients
	}
	pairs = pairs[minoff:maxoff]

	// Now we have the real number of ConnInfo objects, we can set c.NumConns
	// and allocate the array
	c.NumConns = len(pairs)
	c.Conns = make([]ConnInfo, c.NumConns)

	i = 0
	for _, pair := range pairs {

		client := pair.Key

		client.mu.Lock()

		// First, fill ConnInfo with current client's values. We will
		// then overwrite the field used for the sort with what was stored
		// in 'pair'.
		ci := &c.Conns[i]

		ci.Cid = client.cid
		ci.Start = client.start
		ci.LastActivity = client.last
		ci.Uptime = myUptime(c.Now.Sub(client.start))
		ci.Idle = myUptime(c.Now.Sub(client.last))
		ci.OutMsgs = client.outMsgs
		ci.OutBytes = client.outBytes
		ci.NumSubs = uint32(len(client.subs))
		ci.Pending = client.bw.Buffered()
		ci.Name = client.opts.Name
		ci.Lang = client.opts.Lang
		ci.Version = client.opts.Version
		// inMsgs and inBytes are updated outside of the client's lock, so
		// we need to use atomic here.
		ci.InMsgs = atomic.LoadInt64(&client.inMsgs)
		ci.InBytes = atomic.LoadInt64(&client.inBytes)

		// Now overwrite the field that was used as the sort key, so results
		// still look sorted even if the value has changed since sort occurred.
		sortValue := pair.Val
		switch sortOpt {
		case BySubs:
			ci.NumSubs = uint32(sortValue)
		case ByPending:
			ci.Pending = int(sortValue)
		case ByOutMsgs:
			ci.OutMsgs = sortValue
		case ByInMsgs:
			ci.InMsgs = sortValue
		case ByOutBytes:
			ci.OutBytes = sortValue
		case ByInBytes:
			ci.InBytes = sortValue
		case ByLast:
			ci.LastActivity = time.Unix(0, sortValue)
		case ByIdle:
			ci.Idle = myUptime(time.Duration(sortValue))
		}

		// If the connection is gone, too bad, we won't set TLSVersion and TLSCipher.
		// Exclude clients that are still doing handshake so we don't block in
		// ConnectionState().
		if tlsRequired && client.flags.isSet(handshakeComplete) && client.nc != nil {
			conn := client.nc.(*tls.Conn)
			cs := conn.ConnectionState()
			ci.TLSVersion = tlsVersion(cs.Version)
			ci.TLSCipher = tlsCipher(cs.CipherSuite)
		}

		switch conn := client.nc.(type) {
		case *net.TCPConn, *tls.Conn:
			addr := conn.RemoteAddr().(*net.TCPAddr)
			ci.Port = addr.Port
			ci.IP = addr.IP.String()
		}

		// Fill in subscription data if requested.
		if subs {
			sublist := make([]*subscription, 0, len(client.subs))
			for _, sub := range client.subs {
				sublist = append(sublist, sub)
			}
			ci.Subs = castToSliceString(sublist)
		}

		// Fill in user if auth requested.
		if auth {
			ci.AuthorizedUser = client.opts.Username
		}

		client.mu.Unlock()
		i++
	}
	return c, nil
}

func decodeInt(w http.ResponseWriter, r *http.Request, param string) (int, error) {
	str := r.URL.Query().Get(param)
	if str == "" {
		return 0, nil
	}
	val, err := strconv.Atoi(str)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("Error decoding %s: %v", param, err)))
		return 0, err
	}
	return val, nil
}

// HandleConnz process HTTP requests for connection information.
func (s *Server) HandleConnz(w http.ResponseWriter, r *http.Request) {
	sortOpt := SortOpt(r.URL.Query().Get("sort"))
	auth, err := decodeInt(w, r, "auth")
	if err != nil {
		return
	}
	subs, err := decodeInt(w, r, "subs")
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

	connzOpts := &ConnzOptions{
		Sort:          sortOpt,
		Username:      auth == 1,
		Subscriptions: subs == 1,
		Offset:        offset,
		Limit:         limit,
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

func castToSliceString(input []*subscription) []string {
	output := make([]string, 0, len(input))
	for _, line := range input {
		output = append(output, string(line.subject))
	}
	return output
}

// Subsz represents detail information on current connections.
type Subsz struct {
	*SublistStats
}

// SubszOptions are the options passed to Subsz.
// As of now, there are no options defined.
type SubszOptions struct{}

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

		if subs {
			sublist := make([]*subscription, 0, len(r.subs))
			for _, sub := range r.subs {
				sublist = append(sublist, sub)
			}
			ri.Subs = castToSliceString(sublist)
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
	subs, err := decodeInt(w, r, "subs")
	if err != nil {
		return
	}
	var opts *RoutezOptions
	if subs == 1 {
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

// Subsz returns a Subsz struct containing subjects statistics
func (s *Server) Subsz(subszOpts *SubszOptions) (*Subsz, error) {
	return &Subsz{s.sl.Stats()}, nil
}

// HandleSubsz processes HTTP requests for subjects stats.
func (s *Server) HandleSubsz(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.httpReqStats[SubszPath]++
	s.mu.Unlock()

	// As of now, no error is ever returned
	st, _ := s.Subsz(nil)
	b, err := json.MarshalIndent(st, "", "  ")
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
	v.Subscriptions = s.sl.Count()
	v.ConfigLoadTime = s.configTime
	// Need a copy here since s.httpReqStas can change while doing
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
