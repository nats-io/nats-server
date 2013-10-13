// Copyright 2013 Apcera Inc. All rights reserved.

package server

import (
	"encoding/json"
	"net"
	"net/http"
)

// Connz represents detail information on current connections.
type Connz struct {
	NumConns int         `json:"num_connections"`
	Conns    []*ConnInfo `json:"connections"`
}

// ConnInfo has detailed information on a per connection basis.
type ConnInfo struct {
	Cid      uint64 `json:"cid"`
	IP       string `json:"ip"`
	Port     int    `json:"port"`
	Subs     uint32 `json:"subscriptions"`
	Pending  int    `json:"pending_size"`
	InMsgs   int64  `json:"in_msgs"`
	OutMsgs  int64  `json:"out_msgs"`
	InBytes  int64  `json:"in_bytes"`
	OutBytes int64  `json:"out_bytes"`
}

// HandleConnz process HTTP requests for connection information.
func (s *Server) HandleConnz(w http.ResponseWriter, r *http.Request) {
	c := Connz{Conns: []*ConnInfo{}}

	// Walk the list
	s.mu.Lock()
	for _, client := range s.clients {
		ci := &ConnInfo{
			Cid:      client.cid,
			Subs:     client.subs.Count(),
			InMsgs:   client.inMsgs,
			OutMsgs:  client.outMsgs,
			InBytes:  client.inBytes,
			OutBytes: client.outBytes,
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
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		Log("Error marshalling response go /connzz request: %v", err)
	}
	w.Write(b)
}
