// Copyright 2013-2015 Apcera Inc. All rights reserved.

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
	"time"

	"github.com/nats-io/gnatsd/sublist"
)

// Snapshot this
var numCores int

func init() {
	numCores = runtime.NumCPU()
}

// Connz represents detailed information on current client connections.
type Connz struct {
	Now      time.Time   `json:"now"`
	NumConns int         `json:"num_connections"`
	Offset   int         `json:"offset"`
	Limit    int         `json:"limit"`
	Conns    []*ConnInfo `json:"connections"`
}

// ConnInfo has detailed information on a per connection basis.
type ConnInfo struct {
	Cid        uint64    `json:"cid"`
	IP         string    `json:"ip"`
	Port       int       `json:"port"`
	Start      time.Time `json:"start"`
	Uptime     string    `json:"uptime"`
	Pending    int       `json:"pending_bytes"`
	InMsgs     int64     `json:"in_msgs"`
	OutMsgs    int64     `json:"out_msgs"`
	InBytes    int64     `json:"in_bytes"`
	OutBytes   int64     `json:"out_bytes"`
	NumSubs    uint32    `json:"subscriptions"`
	Name       string    `json:"name,omitempty"`
	Lang       string    `json:"lang,omitempty"`
	Version    string    `json:"version,omitempty"`
	TLSVersion string    `json:"tls_version,omitempty"`
	TLSCipher  string    `json:"tls_cipher_suite,omitempty"`
	Subs       []string  `json:"subscriptions_list,omitempty"`
}

const DefaultConnListSize = 1024

// HandleConnz process HTTP requests for connection information.
func (s *Server) HandleConnz(w http.ResponseWriter, r *http.Request) {
	c := &Connz{Conns: []*ConnInfo{}}
	c.Now = time.Now()

	subs, _ := strconv.Atoi(r.URL.Query().Get("subs"))
	c.Offset, _ = strconv.Atoi(r.URL.Query().Get("offset"))
	c.Limit, _ = strconv.Atoi(r.URL.Query().Get("limit"))
	sortOpt := SortOpt(r.URL.Query().Get("sort"))

	if c.Limit == 0 {
		c.Limit = DefaultConnListSize
	}

	// Walk the list
	s.mu.Lock()
	s.httpReqStats[ConnzPath]++
	tlsRequired := s.info.TLSRequired
	c.NumConns = len(s.clients)

	// Copy the keys to sort by them
	pairs := make([]Pair, 0, c.NumConns)
	for k, v := range s.clients {
		pairs = append(pairs, Pair{Key: k, Val: v})
	}
	s.mu.Unlock()

	switch sortOpt {
	case byCid:
		sort.Sort(ByCid(pairs))
	case bySubs:
		sort.Sort(sort.Reverse(BySubs(pairs)))
	case byPending:
		sort.Sort(sort.Reverse(ByPending(pairs)))
	case byOutMsgs:
		sort.Sort(sort.Reverse(ByOutMsgs(pairs)))
	case byInMsgs:
		sort.Sort(sort.Reverse(ByInMsgs(pairs)))
	case byOutBytes:
		sort.Sort(sort.Reverse(ByOutBytes(pairs)))
	case byInBytes:
		sort.Sort(sort.Reverse(ByInBytes(pairs)))
	default:
		if sortOpt != "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Invalid sorting option"))
			return
		}
	}

	minoff := c.Offset
	maxoff := c.Offset + c.Limit

	// Make sure these are sane.
	if minoff > c.NumConns {
		minoff = c.NumConns
	}
	if maxoff > c.NumConns {
		maxoff = c.NumConns
	}
	pairs = pairs[minoff:maxoff]

	for _, pair := range pairs {
		client := pair.Val
		client.mu.Lock()

		ci := &ConnInfo{
			Cid:      client.cid,
			Start:    client.start,
			Uptime:   myUptime(c.Now.Sub(client.start)),
			InMsgs:   client.inMsgs,
			OutMsgs:  client.outMsgs,
			InBytes:  client.inBytes,
			OutBytes: client.outBytes,
			NumSubs:  client.subs.Count(),
			Pending:  client.bw.Buffered(),
			Name:     client.opts.Name,
			Lang:     client.opts.Lang,
			Version:  client.opts.Version,
		}

		if tlsRequired {
			conn := client.nc.(*tls.Conn)
			cs := conn.ConnectionState()
			ci.TLSVersion = tlsVersion(cs.Version)
			ci.TLSCipher = tlsCipher(cs.CipherSuite)
		}

		if subs == 1 {
			ci.Subs = castToSliceString(client.subs.All())
		}

		if ip, ok := client.nc.(*net.TCPConn); ok {
			addr := ip.RemoteAddr().(*net.TCPAddr)
			ci.Port = addr.Port
			ci.IP = addr.IP.String()
		}
		client.mu.Unlock()
		c.Conns = append(c.Conns, ci)
	}

	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		Errorf("Error marshalling response to /connz request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

func castToSliceString(input []interface{}) []string {
	output := make([]string, 0, len(input))
	for _, line := range input {
		output = append(output, string(line.(*subscription).subject))
	}
	return output
}

// Subsz represents detail information on current connections.
type Subsz struct {
	*sublist.Stats
}

// Routez represents detailed information on current client connections.
type Routez struct {
	Now       time.Time    `json:"now"`
	NumRoutes int          `json:"num_routes"`
	Routes    []*RouteInfo `json:"routes"`
}

// RouteInfo has detailed information on a per connection basis.
type RouteInfo struct {
	Rid          uint64   `json:"rid"`
	RemoteId     string   `json:"remote_id"`
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

// HandleRoutez process HTTP requests for route information.
func (s *Server) HandleRoutez(w http.ResponseWriter, r *http.Request) {
	rs := &Routez{Routes: []*RouteInfo{}}
	rs.Now = time.Now()

	subs, _ := strconv.Atoi(r.URL.Query().Get("subs"))

	// Walk the list
	s.mu.Lock()

	s.httpReqStats[RoutezPath]++
	rs.NumRoutes = len(s.routes)

	for _, r := range s.routes {
		ri := &RouteInfo{
			Rid:          r.cid,
			RemoteId:     r.route.remoteID,
			DidSolicit:   r.route.didSolicit,
			IsConfigured: r.route.routeType == Explicit,
			InMsgs:       r.inMsgs,
			OutMsgs:      r.outMsgs,
			InBytes:      r.inBytes,
			OutBytes:     r.outBytes,
			NumSubs:      r.subs.Count(),
		}

		if subs == 1 {
			ri.Subs = castToSliceString(r.subs.All())
		}

		if ip, ok := r.nc.(*net.TCPConn); ok {
			addr := ip.RemoteAddr().(*net.TCPAddr)
			ri.Port = addr.Port
			ri.IP = addr.IP.String()
		}
		rs.Routes = append(rs.Routes, ri)
	}
	s.mu.Unlock()

	b, err := json.MarshalIndent(rs, "", "  ")
	if err != nil {
		Errorf("Error marshalling response to /routez request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// HandleStats process HTTP requests for subjects stats.
func (s *Server) HandleSubsz(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.httpReqStats[SubszPath]++
	s.mu.Unlock()

	st := &Subsz{s.sl.Stats()}

	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		Errorf("Error marshalling response to /subscriptionsz request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// Varz will output server information on the monitoring port at /varz.
type Varz struct {
	*Info
	*Options
	Port             int       `json:"port"`
	MaxPayload       int       `json:"max_payload"`
	Start            time.Time `json:"start"`
	Now              time.Time `json:"now"`
	Uptime           string    `json:"uptime"`
	Mem              int64     `json:"mem"`
	Cores            int       `json:"cores"`
	CPU              float64   `json:"cpu"`
	Connections      int       `json:"connections"`
	TotalConnections uint64    `json:"total_connections"`
	Routes           int       `json:"routes"`
	Remotes          int       `json:"remotes"`
	InMsgs           int64     `json:"in_msgs"`
	OutMsgs          int64     `json:"out_msgs"`
	InBytes          int64     `json:"in_bytes"`
	OutBytes         int64     `json:"out_bytes"`
	SlowConsumers    int64     `json:"slow_consumers"`

	HTTPReqStats map[string]uint64 `json:"http_req_stats"`
}

type usage struct {
	CPU   float32
	Cores int
	Mem   int64
}

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
      body { font-family: “Century Gothic”, CenturyGothic, AppleGothic, sans-serif; font-size: 22; }
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

// HandleVarz will process HTTP requests for server information.
func (s *Server) HandleVarz(w http.ResponseWriter, r *http.Request) {
	v := &Varz{Info: &s.info, Options: s.opts, MaxPayload: s.opts.MaxPayload, Start: s.start}
	v.Now = time.Now()
	v.Uptime = myUptime(time.Since(s.start))
	v.Port = v.Info.Port

	updateUsage(v)

	s.mu.Lock()
	v.Connections = len(s.clients)
	v.TotalConnections = s.totalClients
	v.Routes = len(s.routes)
	v.Remotes = len(s.remotes)
	v.InMsgs = s.inMsgs
	v.InBytes = s.inBytes
	v.OutMsgs = s.outMsgs
	v.OutBytes = s.outBytes
	v.SlowConsumers = s.slowConsumers
	s.httpReqStats[VarzPath]++
	v.HTTPReqStats = s.httpReqStats
	s.mu.Unlock()

	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		Errorf("Error marshalling response to /varz request: %v", err)
	}

	// Handle response
	ResponseHandler(w, r, b)
}

// Grab RSS and PCPU
func updateUsage(v *Varz) {
	var rss, vss int64
	var pcpu float64

	procUsage(&pcpu, &rss, &vss)

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
