// Copyright 2012-2013 Apcera Inc. All rights reserved.

package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/apcera/gnatsd/sublist"
)

type Info struct {
	Id           string `json:"server_id"`
	Version      string `json:"version"`
	Host         string `json:"host"`
	Port         int    `json:"port"`
	AuthRequired bool   `json:"auth_required"`
	SslRequired  bool   `json:"ssl_required"`
	MaxPayload   int    `json:"max_payload"`
}

type Server struct {
	mu       sync.Mutex
	info     Info
	infoJson []byte
	sl       *sublist.Sublist
	gcid     uint64
	opts     *Options
	trace    bool
	debug    bool
	running  bool
	listener net.Listener
	clients  map[uint64]*client
	routes   map[uint64]*client
	done     chan bool
	start    time.Time
	stats

	routeListener net.Listener
	grid          uint64
	routeInfo     Info
	routeInfoJson []byte
}

type stats struct {
	inMsgs   int64
	outMsgs  int64
	inBytes  int64
	outBytes int64
}

func New(opts *Options) *Server {
	processOptions(opts)
	info := Info{
		Id:           genId(),
		Version:      VERSION,
		Host:         opts.Host,
		Port:         opts.Port,
		AuthRequired: false,
		SslRequired:  false,
		MaxPayload:   MAX_PAYLOAD_SIZE,
	}
	// Check for Auth items
	if opts.Username != "" || opts.Authorization != "" {
		info.AuthRequired = true
	}
	s := &Server{
		info:  info,
		sl:    sublist.New(),
		opts:  opts,
		debug: opts.Debug,
		trace: opts.Trace,
		done:  make(chan bool, 1),
		start: time.Now(),
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Setup logging with flags
	s.LogInit()

	// For tracking clients
	s.clients = make(map[uint64]*client)

	// For tracking routes
	s.routes = make(map[uint64]*client)

	// Generate the info json
	b, err := json.Marshal(s.info)
	if err != nil {
		Fatalf("Error marshalling INFO JSON: %+v\n", err)
	}
	s.infoJson = []byte(fmt.Sprintf("INFO %s %s", b, CR_LF))

	s.handleSignals()

	Logf("Starting nats-server version %s", VERSION)

	s.running = true

	return s
}

// Print our version and exit
func PrintServerAndExit() {
	fmt.Printf("%s\n", VERSION)
	os.Exit(0)
}

// Signal Handling
func (s *Server) handleSignals() {
	if s.opts.NoSigs {
		return
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			Debugf("Trapped Signal; %v", sig)
			// FIXME, trip running?
			Log("Server Exiting..")
			os.Exit(0)
		}
	}()
}

// Protected check on running state
func (s *Server) isRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// Shutdown will shutdown the server instance by kicking out the AcceptLoop
// and closing all associated clients.
func (s *Server) Shutdown() {
	s.mu.Lock()
	s.running = false

	// Copy off the clients
	clients := make(map[uint64]*client)
	for i, c := range s.clients {
		clients[i] = c
	}

	// Number of done channel responses we expect.
	doneExpected := 0

	// Kick client AcceptLoop()
	if s.listener != nil {
		doneExpected++
		s.listener.Close()
		s.listener = nil
	}

	if s.routeListener != nil {
		doneExpected++
		s.routeListener.Close()
		s.routeListener = nil
	}

	s.mu.Unlock()

	// Close client connections
	for _, c := range clients {
		c.closeConnection()
	}

	// Block until the accept loops exit
	for doneExpected > 0 {
		<-s.done
		doneExpected--
	}
}

func (s *Server) AcceptLoop() {
	hp := fmt.Sprintf("%s:%d", s.opts.Host, s.opts.Port)
	Logf("Listening for client connections on %s", hp)
	l, e := net.Listen("tcp", hp)
	if e != nil {
		Fatalf("Error listening on port: %d - %v", s.opts.Port, e)
		return
	}

	Logf("nats-server is ready")

	// Setup state that can enable shutdown
	s.mu.Lock()
	s.listener = l
	s.mu.Unlock()

	tmpDelay := ACCEPT_MIN_SLEEP

	for s.isRunning() {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				Debug("Temporary Client Accept Error(%v), sleeping %dms",
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
		s.createClient(conn)
	}
	Log("Server Exiting..")
	s.done <- true
}

func (s *Server) StartHTTPMonitoring() {
	go func() {
		// FIXME(dlc): port config
		lm := fmt.Sprintf("Starting http monitor on port %d", s.opts.HttpPort)
		Log(lm)
		// Varz
		http.HandleFunc("/varz", func(w http.ResponseWriter, r *http.Request) {
			s.HandleVarz(w, r)
		})
		// Connz
		http.HandleFunc("/connz", func(w http.ResponseWriter, r *http.Request) {
			s.HandleConnz(w, r)
		})

		hp := fmt.Sprintf("%s:%d", s.opts.Host, s.opts.HttpPort)
		Fatal(http.ListenAndServe(hp, nil))
	}()

}

func (s *Server) createClient(conn net.Conn) *client {
	c := &client{srv: s, nc: conn, opts: defaultOpts}

	// Initialize
	c.initClient()

	Debug("Client connection created", clientConnStr(c.nc), c.cid)

	c.mu.Lock()

	// Send our information.
	s.sendInfo(c)

	// Check for Auth
	if s.info.AuthRequired {
		ttl := secondsToDuration(s.opts.AuthTimeout)
		c.setAuthTimer(ttl)
	}

	c.mu.Unlock()

	// Register with the server.
	s.mu.Lock()
	s.clients[c.cid] = c
	s.mu.Unlock()

	return c
}

func (s *Server) sendInfo(c *client) {
	switch c.typ {
	case CLIENT:
		c.nc.Write(s.infoJson)
	case ROUTER:
		c.nc.Write(s.routeInfoJson)
	}
}

func (s *Server) checkClientAuth(c *client) bool {
	if !s.info.AuthRequired {
		return true
	}
	// We require auth here, check the client
	// Authorization tokens trump username/password
	if s.opts.Authorization != "" {
		return s.opts.Authorization == c.opts.Authorization
	} else if s.opts.Username != c.opts.Username ||
		s.opts.Password != c.opts.Password {
		return false
	}
	return true
}

func (s *Server) checkRouterAuth(c *client) bool {
	if !s.routeInfo.AuthRequired {
		return true
	}

	fmt.Printf("s.opts: %+v\n", s.opts)
	fmt.Printf("c.opts: %+v\n", c.opts)

	if s.opts.ClusterUsername != c.opts.Username ||
		s.opts.ClusterPassword != c.opts.Password {
		return false
	}
	return true
}

// Check auth and return boolean indicating if client is ok
func (s *Server) checkAuth(c *client) bool {
	switch c.typ {
	case CLIENT:
		return s.checkClientAuth(c)
	case ROUTER:
		return s.checkRouterAuth(c)
	default:
		return false
	}
}

func (s *Server) removeClient(c *client) {
	c.mu.Lock()
	cid := c.cid
	typ := c.typ
	c.mu.Unlock()

	s.mu.Lock()
	switch typ {
	case CLIENT:
		delete(s.clients, cid)
	case ROUTER:
		delete(s.routes, cid)
	}
	s.mu.Unlock()
}
