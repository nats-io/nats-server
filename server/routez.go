package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
)

// Routez represents detail information on current routes
type Routez struct {
	NumRoutes int          `json:"num_routes"`
	Routes    []*RouteInfo `json:"routes"`
}

// RouteInfo has detailed information on a per connection basis.
type RouteInfo struct {
	Cid       uint64 `json:"cid"`
	URL       string `json:"url"`
	IP        string `json:"ip"`
	Port      int    `json:"port"`
	Solicited bool   `json:"solicited"`
	Subs      uint32 `json:"subscriptions"`
	Pending   int    `json:"pending_size"`
	InMsgs    int64  `json:"in_msgs"`
	OutMsgs   int64  `json:"out_msgs"`
	InBytes   int64  `json:"in_bytes"`
	OutBytes  int64  `json:"out_bytes"`
}

// HandleConnz process HTTP requests for connection information.
func (s *Server) HandleRoutez(w http.ResponseWriter, req *http.Request) {

	if req.Method == "GET" {
		r := Routez{Routes: []*RouteInfo{}}

		// Walk the list
		s.mu.Lock()
		for _, route := range s.routes {
			ri := &RouteInfo{
				Cid:       route.cid,
				Subs:      route.subs.Count(),
				URL:       route.route.url.String(),
				Solicited: route.route.didSolicit,
				InMsgs:    route.inMsgs,
				OutMsgs:   route.outMsgs,
				InBytes:   route.inBytes,
				OutBytes:  route.outBytes,
			}
			if ip, ok := route.nc.(*net.TCPConn); ok {
				addr := ip.RemoteAddr().(*net.TCPAddr)
				ri.Port = addr.Port
				ri.IP = addr.IP.String()
			}
			r.Routes = append(r.Routes, ri)
		}
		s.mu.Unlock()

		r.NumRoutes = len(r.Routes)
		b, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			Logf("Error marshalling response to /routez request: %v", err)
		}
		w.Write(b)
	} else if req.Method == "PUT" {
		body := make([]byte, 1024)
		req.Body.Read(body)
		routeURL, err := url.Parse(string(body))
		if err != nil {
			w.WriteHeader(400)
			w.Write([]byte(fmt.Sprintf(`{"error": "could not parse URL: %v"}`, err)))
			return
		}

		s.connectToRoute(routeURL)
		w.Write([]byte(`{"status": "ok"}`))
	} else if req.Method == "DELETE" {
		body := make([]byte, 1024)
		req.Body.Read(body)
		routeURL, err := url.Parse(string(body))

		routeIP, err := net.ResolveTCPAddr("tcp", routeURL.Host)

		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(fmt.Sprintf(`{"error": "could not resolve url: %v"}`, err)))
			return
		}

		for _, route := range s.routes {
			if ipConn, ok := route.nc.(*net.TCPConn); ok {
				addr := ipConn.RemoteAddr().(*net.TCPAddr)
				if addr.String() == routeIP.String() {
					route.mu.Lock()
					route.route.didSolicit = false // don't reconnect
					route.mu.Unlock()
					route.closeConnection()
					w.WriteHeader(200)
					w.Write([]byte(`{"status": "ok"}`))
					return
				}

			}
		}
		w.WriteHeader(404)
		w.Write([]byte(`{"error": "could not find matching route"}`))
	}
}
