// Copyright 2013 Apcera Inc. All rights reserved.

package server

import (
	"encoding/json"
	"net"
	"net/http"

	"github.com/apcera/gnatsd/sublist"
)

// Connz represents detail information on current connections.
type Connz struct {
	NumConns     int            `json:"num_connections"`
	SubjectStats *sublist.Stats `json:"stats"`
	Conns        []*ConnInfo    `json:"connections"`
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
	NumSubs  uint32   `json:"num_subscriptions"`
	Subs     []string `json:"subscriptions"`
}

// HandleConnz process HTTP requests for connection information.
func (s *Server) HandleConnz(w http.ResponseWriter, r *http.Request) {
	c := Connz{Conns: []*ConnInfo{}}

	// Walk the list
	s.mu.Lock()
	for _, client := range s.clients {
		ci := &ConnInfo{
			Cid:      client.cid,
			InMsgs:   client.inMsgs,
			OutMsgs:  client.outMsgs,
			InBytes:  client.inBytes,
			OutBytes: client.outBytes,
			NumSubs:  client.subs.Count(),
			Subs:     castToSliceString(client.subs.All()),
		}
		if ip, ok := client.nc.(*net.TCPConn); ok {
			addr := ip.RemoteAddr().(*net.TCPAddr)
			ci.Port = addr.Port
			ci.IP = addr.IP.String()
		}
		c.Conns = append(c.Conns, ci)
	}
	s.mu.Unlock()

	c.NumConns = len(c.Conns)
	c.SubjectStats = s.sl.Stats()

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
