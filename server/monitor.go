// Copyright 2013-2014 Apcera Inc. All rights reserved.

package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"time"

	"github.com/apcera/gnatsd/sublist"
)

// Connz represents detail information on current connections.
type Connz struct {
	NumConns int         `json:"num_connections"`
	Offset   int         `json:"offset"`
	Limit    int         `json:"limit"`
	Conns    []*ConnInfo `json:"connections"`
}

// ConnInfo has detailed information on a per connection basis.
type ConnInfo struct {
	Cid      uint64   `json:"cid"`
	IP       string   `json:"ip"`
	Port     int      `json:"port"`
	Pending  int      `json:"pending_size"`
	InMsgs   int64    `json:"in_msgs"`
	OutMsgs  int64    `json:"out_msgs"`
	InBytes  int64    `json:"in_bytes"`
	OutBytes int64    `json:"out_bytes"`
	NumSubs  uint32   `json:"subscriptions"`
	Subs     []string `json:"subscriptions_list,omitempty"`
}

// HandleConnz process HTTP requests for connection information.
func (s *Server) HandleConnz(w http.ResponseWriter, r *http.Request) {
	c := &Connz{Conns: []*ConnInfo{}}

	subs, _ := strconv.Atoi(r.URL.Query().Get("subs"))
	c.Offset, _ = strconv.Atoi(r.URL.Query().Get("offset"))
	c.Limit, _ = strconv.Atoi(r.URL.Query().Get("limit"))
	if c.Limit == 0 {
		c.Limit = 100
	}

	// Walk the list
	s.mu.Lock()
	c.NumConns = len(s.clients)

	i := 0
	for _, client := range s.clients {
		if i >= c.Offset+c.Limit {
			break
		}

		i++
		if i <= c.Offset {
			continue
		}

		ci := &ConnInfo{
			Cid:      client.cid,
			InMsgs:   client.inMsgs,
			OutMsgs:  client.outMsgs,
			InBytes:  client.inBytes,
			OutBytes: client.outBytes,
			NumSubs:  client.subs.Count(),
		}

		if subs == 1 {
			ci.Subs = castToSliceString(client.subs.All())
		}

		if ip, ok := client.nc.(*net.TCPConn); ok {
			addr := ip.RemoteAddr().(*net.TCPAddr)
			ci.Port = addr.Port
			ci.IP = addr.IP.String()
		}
		c.Conns = append(c.Conns, ci)
	}
	s.mu.Unlock()

	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		Logf("Error marshalling response to /connz request: %v", err)
	}
	w.Write(b)
}

func castToSliceString(input []interface{}) []string {

	output := make([]string, 0, len(input))
	for _, line := range input {
		output = append(output, string(line.(*subscription).subject))
	}

	return output
}

// Connz represents detail information on current connections.
type Subsz struct {
	SubjectStats *sublist.Stats `json:"stats"`
}

// HandleStats process HTTP requests for subjects stats.
func (s *Server) HandleSubsz(w http.ResponseWriter, r *http.Request) {
	st := &Subsz{SubjectStats: s.sl.Stats()}

	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		Logf("Error marshalling response to /subscriptionsz request: %v", err)
	}
	w.Write(b)
}

// Varz will output server information on the monitoring port at /varz.
type Varz struct {
	Start       time.Time `json:"start"`
	Options     *Options  `json:"options"`
	Mem         int64     `json:"mem"`
	Cores       int       `json:"cores"`
	CPU         float64   `json:"cpu"`
	Connections int       `json:"connections"`
	Routes      int       `json:"routes"`
	Remotes     int       `json:"remotes"`
	InMsgs      int64     `json:"in_msgs"`
	OutMsgs     int64     `json:"out_msgs"`
	InBytes     int64     `json:"in_bytes"`
	OutBytes    int64     `json:"out_bytes"`
	Uptime      string    `json:"uptime"`
}

type usage struct {
	CPU   float32
	Cores int
	Mem   int64
}

// HandleVarz will process HTTP requests for server information.
func (s *Server) HandleVarz(w http.ResponseWriter, r *http.Request) {
	v := &Varz{Start: s.start, Options: s.opts}
	v.Uptime = time.Since(s.start).String()

	updateUsage(v)

	s.mu.Lock()
	v.Connections = len(s.clients)
	v.Routes = len(s.routes)
	v.Remotes = len(s.remotes)
	v.InMsgs = s.inMsgs
	v.InBytes = s.inBytes
	v.OutMsgs = s.outMsgs
	v.OutBytes = s.outBytes
	s.mu.Unlock()

	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		Logf("Error marshalling response to /varz request: %v", err)
	}
	w.Write(b)
}

// FIXME(dlc): This is a big hack, make real..
func updateUsage(v *Varz) {
	v.Cores = runtime.NumCPU()
	pidStr := fmt.Sprintf("%d", os.Getpid())
	out, err := exec.Command("ps", "o", "pcpu=,rss=", "-p", pidStr).Output()
	if err != nil {
		// FIXME(dlc): Log?
		return
	}
	fmt.Sscanf(string(out), "%f %d", &v.CPU, &v.Mem)
	v.Mem *= 1024 // 1k blocks, want bytes.
}
