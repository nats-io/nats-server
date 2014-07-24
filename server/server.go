// Copyright 2012-2014 Apcera Inc. All rights reserved.

package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	// Allow dynamic profiling.
	_ "net/http/pprof"

	"github.com/apcera/gnatsd/sublist"
)

// Info is the information sent to clients to help them understand information
// about this server.
type Info struct {
	ID           string `json:"server_id"`
	Version      string `json:"version"`
	Host         string `json:"host"`
	Port         int    `json:"port"`
	AuthRequired bool   `json:"auth_required"`
	SslRequired  bool   `json:"ssl_required"`
	MaxPayload   int    `json:"max_payload"`
}

// Server is our main struct.
type Server struct {
	mu       sync.Mutex
	info     Info
	infoJSON []byte
	sl       *sublist.Sublist
	gcid     uint64
	opts     *Options
	trace    bool
	debug    bool
	running  bool
	listener net.Listener
	clients  map[uint64]*client
	routes   map[uint64]*client
	remotes  map[string]*client
	done     chan bool
	start    time.Time
	http     net.Listener
	stats

	routeListener net.Listener
	grid          uint64
	routeInfo     Info
	routeInfoJSON []byte
	rcQuit        chan bool
}

type stats struct {
	inMsgs   int64
	outMsgs  int64
	inBytes  int64
	outBytes int64
}

// New will setup a new server struct after parsing the options.
func New(opts *Options) *Server {
	processOptions(opts)
	info := Info{
		ID:           genID(),
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

	// For tracking routes and their remote ids
	s.routes = make(map[uint64]*client)
	s.remotes = make(map[string]*client)

	// Used to kick out all of the route
	// connect Go routines.
	s.rcQuit = make(chan bool)

	// Generate the info json
	b, err := json.Marshal(s.info)
	if err != nil {
		Fatalf("Error marshalling INFO JSON: %+v\n", err)
	}
	s.infoJSON = []byte(fmt.Sprintf("INFO %s %s", b, CR_LF))

	s.handleSignals()

	Logf("Starting gnatsd version %s", VERSION)

	s.running = true

	return s
}

// PrintAndDie is exported for access in other packages.
func PrintAndDie(msg string) {
	fmt.Fprintf(os.Stderr, "%s\n", msg)
	os.Exit(1)
}

// PrintServerAndExit will print our version and exit.
func PrintServerAndExit() {
	fmt.Printf("gnatsd version %s\n", VERSION)
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

func (s *Server) logPid() {
	pidStr := strconv.Itoa(os.Getpid())
	err := ioutil.WriteFile(s.opts.PidFile, []byte(pidStr), 0660)
	if err != nil {
		PrintAndDie(fmt.Sprintf("Could not write pidfile: %v\n", err))
	}
}

// Start up the server, this will block.
// Start via a Go routine if needed.
func (s *Server) Start() {

	// Log the pid to a file
	if s.opts.PidFile != _EMPTY_ {
		s.logPid()
	}

	// Start up the http server if needed.
	if s.opts.HTTPPort != 0 {
		s.StartHTTPMonitoring()
	}

	// Start up routing as well if needed.
	if s.opts.ClusterPort != 0 {
		s.StartRouting()
	}

	// Pprof http endpoint for the profiler.
	if s.opts.ProfPort != 0 {
		s.StartProfiler()
	}

	// Wait for clients.
	s.AcceptLoop()
}

// Shutdown will shutdown the server instance by kicking out the AcceptLoop
// and closing all associated clients.
func (s *Server) Shutdown() {
	s.mu.Lock()

	// Prevent issues with multiple calls.
	if !s.running {
		s.mu.Unlock()
		return
	}

	s.running = false

	conns := make(map[uint64]*client)

	// Copy off the clients
	for i, c := range s.clients {
		conns[i] = c
	}
	// Copy off the routes
	for i, r := range s.routes {
		conns[i] = r
	}

	// Number of done channel responses we expect.
	doneExpected := 0

	// Kick client AcceptLoop()
	if s.listener != nil {
		doneExpected++
		s.listener.Close()
		s.listener = nil
	}

	// Kick route AcceptLoop()
	if s.routeListener != nil {
		doneExpected++
		s.routeListener.Close()
		s.routeListener = nil
	}

	// Kick HTTP monitoring if its running
	if s.http != nil {
		doneExpected++
		s.http.Close()
		s.http = nil
	}

	// Release the solicited routes connect go routines.
	close(s.rcQuit)

	s.mu.Unlock()

	// Close client and route connections
	for _, c := range conns {
		c.closeConnection()
	}

	// Block until the accept loops exit
	for doneExpected > 0 {
		<-s.done
		doneExpected--
	}
}

// AcceptLoop is exported for easier testing.
func (s *Server) AcceptLoop() {
	hp := fmt.Sprintf("%s:%d", s.opts.Host, s.opts.Port)
	Logf("Listening for client connections on %s", hp)
	l, e := net.Listen("tcp", hp)
	if e != nil {
		Fatalf("Error listening on port: %d - %v", s.opts.Port, e)
		return
	}

	Logf("gnatsd is ready")

	// Setup state that can enable shutdown
	s.mu.Lock()
	s.listener = l
	s.mu.Unlock()

	// Write resolved port back to options.
	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		Fatalf("Error parsing server address (%s): %s", l.Addr().String(), e)
		return
	}
	portNum, err := strconv.Atoi(port)
	if err != nil {
		Fatalf("Error parsing server address (%s): %s", l.Addr().String(), e)
		return
	}
	s.opts.Port = portNum

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

// StartProfiler is called to enable dynamic profiling.
func (s *Server) StartProfiler() {
	Logf("Starting profiling on http port %d", s.opts.ProfPort)

	hp := fmt.Sprintf("%s:%d", s.opts.Host, s.opts.ProfPort)
	go func() {
		Log(http.ListenAndServe(hp, nil))
	}()
}

// StartHTTPMonitoring will enable the HTTP monitoring port.
func (s *Server) StartHTTPMonitoring() {
	Logf("Starting http monitor on port %d", s.opts.HTTPPort)

	hp := fmt.Sprintf("%s:%d", s.opts.Host, s.opts.HTTPPort)

	l, err := net.Listen("tcp", hp)
	if err != nil {
		Fatalf("Can't listen to the monitor port: %v", err)
	}

	mux := http.NewServeMux()

	// Varz
	mux.HandleFunc("/varz", s.HandleVarz)

	// Connz
	mux.HandleFunc("/connz", s.HandleConnz)

	srv := &http.Server{
		Addr:           hp,
		Handler:        mux,
		ReadTimeout:    2 * time.Second,
		WriteTimeout:   2 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	s.http = l

	go func() {
		srv.Serve(s.http)
		srv.Handler = nil
		s.done <- true
	}()
}

func (s *Server) createClient(conn net.Conn) *client {
	c := &client{srv: s, nc: conn, opts: defaultOpts}

	// Grab lock
	c.mu.Lock()

	// Initialize
	c.initClient()

	Debug("Client connection created", clientConnStr(c.nc), c.cid)

	// Send our information.
	s.sendInfo(c)

	// Check for Auth
	if s.info.AuthRequired {
		ttl := secondsToDuration(s.opts.AuthTimeout)
		c.setAuthTimer(ttl)
	}

	// Unlock to register
	c.mu.Unlock()

	// Register with the server.
	s.mu.Lock()
	s.clients[c.cid] = c
	s.mu.Unlock()

	return c
}

// Assume the lock is held upon entry.
func (s *Server) sendInfo(c *client) {
	switch c.typ {
	case CLIENT:
		c.nc.Write(s.infoJSON)
	case ROUTER:
		c.nc.Write(s.routeInfoJSON)
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

// Remove a client or route from our internal accounting.
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
		if c.route != nil {
			rc, ok := s.remotes[c.route.remoteID]
			// Only delete it if it is us..
			if ok && c == rc {
				delete(s.remotes, c.route.remoteID)
			}
		}
	}
	s.mu.Unlock()
}

/////////////////////////////////////////////////////////////////
// These are some helpers for accounting in functional tests.
/////////////////////////////////////////////////////////////////

// NumRoutes will report the number of registered routes.
func (s *Server) NumRoutes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.routes)
}

// NumRemotes will report number of registered remotes.
func (s *Server) NumRemotes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.remotes)
}

// NumClients will report the number of registered clients.
func (s *Server) NumClients() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clients)
}

// NumSubscriptions will report how many subscriptions are active.
func (s *Server) NumSubscriptions() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := s.sl.Stats()
	return stats.NumSubs
}

// Addr will return the net.Addr object for the current listener.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}
