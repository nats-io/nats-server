// Copyright 2012 Apcera Inc. All rights reserved.

package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"time"
)

// Varz will output server information on the monitoring port at /varz.
type Varz struct {
	Start       time.Time `json:"start"`
	Options     *Options  `json:"options"`
	Mem         int64     `json:"mem"`
	Cores       int       `json:"cores"`
	CPU         float64   `json:"cpu"`
	Connections int       `json:"connections"`
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
	v := Varz{Start: s.start, Options: s.opts}
	v.Uptime = time.Since(s.start).String()

	updateUsage(&v)

	s.mu.Lock()
	v.Connections = len(s.clients)
	v.InMsgs = s.inMsgs
	v.InBytes = s.inBytes
	v.OutMsgs = s.outMsgs
	v.OutBytes = s.outBytes
	s.mu.Unlock()

	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		Log("Error marshalling response go /varz request: %v", err)
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
