// Copyright 2012-2018 The NATS Authors
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
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	// Allow dynamic profiling.
	_ "net/http/pprof"

	"github.com/nats-io/gnatsd/util"
)

// Info is the information sent to clients to help them understand information
// about this server.
type Info struct {
	ID                string   `json:"server_id"`
	Version           string   `json:"version"`
	Proto             int      `json:"proto"`
	GitCommit         string   `json:"git_commit,omitempty"`
	GoVersion         string   `json:"go"`
	Host              string   `json:"host"`
	Port              int      `json:"port"`
	AuthRequired      bool     `json:"auth_required,omitempty"`
	TLSRequired       bool     `json:"tls_required,omitempty"`
	TLSVerify         bool     `json:"tls_verify,omitempty"`
	MaxPayload        int      `json:"max_payload"`
	IP                string   `json:"ip,omitempty"`
	CID               uint64   `json:"client_id,omitempty"`
	ClientConnectURLs []string `json:"connect_urls,omitempty"` // Contains URLs a client can connect to.
}

// Server is our main struct.
type Server struct {
	gcid uint64
	stats
	mu            sync.Mutex
	info          Info
	sl            *Sublist
	configFile    string
	optsMu        sync.RWMutex
	opts          *Options
	running       bool
	shutdown      bool
	listener      net.Listener
	clients       map[uint64]*client
	routes        map[uint64]*client
	remotes       map[string]*client
	users         map[string]*User
	totalClients  uint64
	closed        *closedRingBuffer
	done          chan bool
	start         time.Time
	http          net.Listener
	httpHandler   http.Handler
	profiler      net.Listener
	httpReqStats  map[string]uint64
	routeListener net.Listener
	routeInfo     Info
	routeInfoJSON []byte
	quitCh        chan struct{}

	// Tracking for remote QRSID tags.
	rqsMu       sync.RWMutex
	rqsubs      map[string]rqsub
	rqsubsTimer *time.Timer

	// Tracking Go routines
	grMu         sync.Mutex
	grTmpClients map[uint64]*client
	grRunning    bool
	grWG         sync.WaitGroup // to wait on various go routines

	cproto     int64     // number of clients supporting async INFO
	configTime time.Time // last time config was loaded

	logging struct {
		sync.RWMutex
		logger Logger
		trace  int32
		debug  int32
	}

	clientConnectURLs []string

	// Used internally for quick look-ups.
	clientConnectURLsMap map[string]struct{}

	lastCURLsUpdate int64

	// These store the real client/cluster listen ports. They are
	// required during config reload to reset the Options (after
	// reload) to the actual listen port values.
	clientActualPort  int
	clusterActualPort int

	// Used by tests to check that http.Servers do
	// not set any timeout.
	monitoringServer *http.Server
	profilingServer  *http.Server
}

// Make sure all are 64bits for atomic use
type stats struct {
	inMsgs        int64
	outMsgs       int64
	inBytes       int64
	outBytes      int64
	slowConsumers int64
}

// New will setup a new server struct after parsing the options.
func New(opts *Options) *Server {
	processOptions(opts)

	// Process TLS options, including whether we require client certificates.
	tlsReq := opts.TLSConfig != nil
	verify := (tlsReq && opts.TLSConfig.ClientAuth == tls.RequireAndVerifyClientCert)

	info := Info{
		ID:           genID(),
		Version:      VERSION,
		Proto:        PROTO,
		GitCommit:    gitCommit,
		GoVersion:    runtime.Version(),
		Host:         opts.Host,
		Port:         opts.Port,
		AuthRequired: false,
		TLSRequired:  tlsReq,
		TLSVerify:    verify,
		MaxPayload:   opts.MaxPayload,
	}

	now := time.Now()
	s := &Server{
		configFile: opts.ConfigFile,
		info:       info,
		sl:         NewSublist(),
		opts:       opts,
		done:       make(chan bool, 1),
		start:      now,
		configTime: now,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// This is normally done in the AcceptLoop, once the
	// listener has been created (possibly with random port),
	// but since some tests may expect the INFO to be properly
	// set after New(), let's do it now.
	s.setInfoHostPortAndGenerateJSON()

	// Used internally for quick look-ups.
	s.clientConnectURLsMap = make(map[string]struct{})

	// For tracking clients
	s.clients = make(map[uint64]*client)

	// For tracking closed clients.
	s.closed = newClosedRingBuffer(opts.MaxClosedClients)

	// For tracking connections that are not yet registered
	// in s.routes, but for which readLoop has started.
	s.grTmpClients = make(map[uint64]*client)

	// For tracking routes and their remote ids
	s.routes = make(map[uint64]*client)
	s.remotes = make(map[string]*client)

	// Used to kick out all go routines possibly waiting on server
	// to shutdown.
	s.quitCh = make(chan struct{})

	// Used to setup Authorization.
	s.configureAuthorization()

	// Start signal handler
	s.handleSignals()

	return s
}

func (s *Server) getOpts() *Options {
	s.optsMu.RLock()
	opts := s.opts
	s.optsMu.RUnlock()
	return opts
}

func (s *Server) setOpts(opts *Options) {
	s.optsMu.Lock()
	s.opts = opts
	s.optsMu.Unlock()
}

func (s *Server) generateRouteInfoJSON() {
	b, _ := json.Marshal(s.routeInfo)
	pcs := [][]byte{[]byte("INFO"), b, []byte(CR_LF)}
	s.routeInfoJSON = bytes.Join(pcs, []byte(" "))
}

// PrintAndDie is exported for access in other packages.
func PrintAndDie(msg string) {
	fmt.Fprintf(os.Stderr, "%s\n", msg)
	os.Exit(1)
}

// PrintServerAndExit will print our version and exit.
func PrintServerAndExit() {
	fmt.Printf("nats-server version %s\n", VERSION)
	os.Exit(0)
}

// ProcessCommandLineArgs takes the command line arguments
// validating and setting flags for handling in case any
// sub command was present.
func ProcessCommandLineArgs(cmd *flag.FlagSet) (showVersion bool, showHelp bool, err error) {
	if len(cmd.Args()) > 0 {
		arg := cmd.Args()[0]
		switch strings.ToLower(arg) {
		case "version":
			return true, false, nil
		case "help":
			return false, true, nil
		default:
			return false, false, fmt.Errorf("unrecognized command: %q", arg)
		}
	}

	return false, false, nil
}

// Protected check on running state
func (s *Server) isRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

func (s *Server) logPid() error {
	pidStr := strconv.Itoa(os.Getpid())
	return ioutil.WriteFile(s.getOpts().PidFile, []byte(pidStr), 0660)
}

// Start up the server, this will block.
// Start via a Go routine if needed.
func (s *Server) Start() {
	s.Noticef("Starting nats-server version %s", VERSION)
	s.Debugf("Go build version %s", s.info.GoVersion)
	gc := gitCommit
	if gc == "" {
		gc = "not set"
	}
	s.Noticef("Git commit [%s]", gc)

	// Avoid RACE between Start() and Shutdown()
	s.mu.Lock()
	s.running = true
	s.mu.Unlock()

	s.grMu.Lock()
	s.grRunning = true
	s.grMu.Unlock()

	// Snapshot server options.
	opts := s.getOpts()

	// Log the pid to a file
	if opts.PidFile != _EMPTY_ {
		if err := s.logPid(); err != nil {
			PrintAndDie(fmt.Sprintf("Could not write pidfile: %v\n", err))
		}
	}

	// Start monitoring if needed
	if err := s.StartMonitoring(); err != nil {
		s.Fatalf("Can't start monitoring: %v", err)
		return
	}

	// The Routing routine needs to wait for the client listen
	// port to be opened and potential ephemeral port selected.
	clientListenReady := make(chan struct{})

	// Start up routing as well if needed.
	if opts.Cluster.Port != 0 {
		s.startGoRoutine(func() {
			s.StartRouting(clientListenReady)
		})
	}

	// Pprof http endpoint for the profiler.
	if opts.ProfPort != 0 {
		s.StartProfiler()
	}

	if opts.PortsFileDir != _EMPTY_ {
		s.logPorts()
	}

	// Wait for clients.
	s.AcceptLoop(clientListenReady)
}

// Shutdown will shutdown the server instance by kicking out the AcceptLoop
// and closing all associated clients.
func (s *Server) Shutdown() {
	s.mu.Lock()
	// Prevent issues with multiple calls.
	if s.shutdown {
		s.mu.Unlock()
		return
	}

	opts := s.getOpts()

	s.shutdown = true
	s.running = false
	s.grMu.Lock()
	s.grRunning = false
	s.grMu.Unlock()

	conns := make(map[uint64]*client)

	// Copy off the clients
	for i, c := range s.clients {
		conns[i] = c
	}
	// Copy off the connections that are not yet registered
	// in s.routes, but for which the readLoop has started
	s.grMu.Lock()
	for i, c := range s.grTmpClients {
		conns[i] = c
	}
	s.grMu.Unlock()
	// Copy off the routes
	for i, r := range s.routes {
		r.setRouteNoReconnectOnClose()
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

	// Kick Profiling if its running
	if s.profiler != nil {
		doneExpected++
		s.profiler.Close()
	}

	// Clear any remote qsub mappings
	s.clearRemoteQSubs()
	s.mu.Unlock()

	// Release go routines that wait on that channel
	close(s.quitCh)

	// Close client and route connections
	for _, c := range conns {
		c.closeConnection(ServerShutdown)
	}

	// Block until the accept loops exit
	for doneExpected > 0 {
		<-s.done
		doneExpected--
	}

	// Wait for go routines to be done.
	s.grWG.Wait()

	if opts.PortsFileDir != _EMPTY_ {
		s.deletePortsFile(opts.PortsFileDir)
	}
}

// AcceptLoop is exported for easier testing.
func (s *Server) AcceptLoop(clr chan struct{}) {
	// If we were to exit before the listener is setup properly,
	// make sure we close the channel.
	defer func() {
		if clr != nil {
			close(clr)
		}
	}()

	// Snapshot server options.
	opts := s.getOpts()

	hp := net.JoinHostPort(opts.Host, strconv.Itoa(opts.Port))
	l, e := net.Listen("tcp", hp)
	if e != nil {
		s.Fatalf("Error listening on port: %s, %q", hp, e)
		return
	}
	s.Noticef("Listening for client connections on %s",
		net.JoinHostPort(opts.Host, strconv.Itoa(l.Addr().(*net.TCPAddr).Port)))

	// Alert of TLS enabled.
	if opts.TLSConfig != nil {
		s.Noticef("TLS required for client connections")
	}

	s.Debugf("Server id is %s", s.info.ID)
	s.Noticef("Server is ready")

	// Setup state that can enable shutdown
	s.mu.Lock()
	s.listener = l

	// If server was started with RANDOM_PORT (-1), opts.Port would be equal
	// to 0 at the beginning this function. So we need to get the actual port
	if opts.Port == 0 {
		// Write resolved port back to options.
		opts.Port = l.Addr().(*net.TCPAddr).Port
	}
	// Keep track of actual listen port. This will be needed in case of
	// config reload.
	s.clientActualPort = opts.Port

	// Now that port has been set (if it was set to RANDOM), set the
	// server's info Host/Port with either values from Options or
	// ClientAdvertise. Also generate the JSON byte array.
	if err := s.setInfoHostPortAndGenerateJSON(); err != nil {
		s.Fatalf("Error setting server INFO with ClientAdvertise value of %s, err=%v", s.opts.ClientAdvertise, err)
		s.mu.Unlock()
		return
	}
	// Keep track of client connect URLs. We may need them later.
	s.clientConnectURLs = s.getClientConnectURLs()
	s.mu.Unlock()

	// Let the caller know that we are ready
	close(clr)
	clr = nil

	tmpDelay := ACCEPT_MIN_SLEEP

	for s.isRunning() {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				s.Errorf("Temporary Client Accept Error (%v), sleeping %dms",
					ne, tmpDelay/time.Millisecond)
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else if s.isRunning() {
				s.Errorf("Client Accept Error: %v", err)
			}
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP
		s.startGoRoutine(func() {
			s.createClient(conn)
			s.grWG.Done()
		})
	}
	s.Noticef("Server Exiting..")
	s.done <- true
}

// This function sets the server's info Host/Port based on server Options.
// Note that this function may be called during config reload, this is why
// Host/Port may be reset to original Options if the ClientAdvertise option
// is not set (since it may have previously been).
// The function then generates the server infoJSON.
func (s *Server) setInfoHostPortAndGenerateJSON() error {
	// When this function is called, opts.Port is set to the actual listen
	// port (if option was originally set to RANDOM), even during a config
	// reload. So use of s.opts.Port is safe.
	if s.opts.ClientAdvertise != "" {
		h, p, err := parseHostPort(s.opts.ClientAdvertise, s.opts.Port)
		if err != nil {
			return err
		}
		s.info.Host = h
		s.info.Port = p
	} else {
		s.info.Host = s.opts.Host
		s.info.Port = s.opts.Port
	}
	return nil
}

// StartProfiler is called to enable dynamic profiling.
func (s *Server) StartProfiler() {
	// Snapshot server options.
	opts := s.getOpts()

	port := opts.ProfPort

	// Check for Random Port
	if port == -1 {
		port = 0
	}

	hp := net.JoinHostPort(opts.Host, strconv.Itoa(port))

	l, err := net.Listen("tcp", hp)
	s.Noticef("profiling port: %d", l.Addr().(*net.TCPAddr).Port)

	if err != nil {
		s.Fatalf("error starting profiler: %s", err)
	}

	srv := &http.Server{
		Addr:           hp,
		Handler:        http.DefaultServeMux,
		MaxHeaderBytes: 1 << 20,
	}

	s.mu.Lock()
	s.profiler = l
	s.profilingServer = srv
	s.mu.Unlock()

	go func() {
		// if this errors out, it's probably because the server is being shutdown
		err := srv.Serve(l)
		if err != nil {
			s.mu.Lock()
			shutdown := s.shutdown
			s.mu.Unlock()
			if !shutdown {
				s.Fatalf("error starting profiler: %s", err)
			}
		}
		s.done <- true
	}()
}

// StartHTTPMonitoring will enable the HTTP monitoring port.
// DEPRECATED: Should use StartMonitoring.
func (s *Server) StartHTTPMonitoring() {
	s.startMonitoring(false)
}

// StartHTTPSMonitoring will enable the HTTPS monitoring port.
// DEPRECATED: Should use StartMonitoring.
func (s *Server) StartHTTPSMonitoring() {
	s.startMonitoring(true)
}

// StartMonitoring starts the HTTP or HTTPs server if needed.
func (s *Server) StartMonitoring() error {
	// Snapshot server options.
	opts := s.getOpts()

	// Specifying both HTTP and HTTPS ports is a misconfiguration
	if opts.HTTPPort != 0 && opts.HTTPSPort != 0 {
		return fmt.Errorf("can't specify both HTTP (%v) and HTTPs (%v) ports", opts.HTTPPort, opts.HTTPSPort)
	}
	var err error
	if opts.HTTPPort != 0 {
		err = s.startMonitoring(false)
	} else if opts.HTTPSPort != 0 {
		if opts.TLSConfig == nil {
			return fmt.Errorf("TLS cert and key required for HTTPS")
		}
		err = s.startMonitoring(true)
	}
	return err
}

// HTTP endpoints
const (
	RootPath    = "/"
	VarzPath    = "/varz"
	ConnzPath   = "/connz"
	RoutezPath  = "/routez"
	SubszPath   = "/subsz"
	StackszPath = "/stacksz"
)

// Start the monitoring server
func (s *Server) startMonitoring(secure bool) error {
	// Snapshot server options.
	opts := s.getOpts()

	// Used to track HTTP requests
	s.httpReqStats = map[string]uint64{
		RootPath:   0,
		VarzPath:   0,
		ConnzPath:  0,
		RoutezPath: 0,
		SubszPath:  0,
	}

	var (
		hp           string
		err          error
		httpListener net.Listener
		port         int
	)

	monitorProtocol := "http"

	if secure {
		monitorProtocol += "s"
		port = opts.HTTPSPort
		if port == -1 {
			port = 0
		}
		hp = net.JoinHostPort(opts.HTTPHost, strconv.Itoa(port))
		config := util.CloneTLSConfig(opts.TLSConfig)
		config.ClientAuth = tls.NoClientCert
		httpListener, err = tls.Listen("tcp", hp, config)

	} else {
		port = opts.HTTPPort
		if port == -1 {
			port = 0
		}
		hp = net.JoinHostPort(opts.HTTPHost, strconv.Itoa(port))
		httpListener, err = net.Listen("tcp", hp)
	}

	if err != nil {
		return fmt.Errorf("can't listen to the monitor port: %v", err)
	}

	s.Noticef("Starting %s monitor on %s", monitorProtocol,
		net.JoinHostPort(opts.HTTPHost, strconv.Itoa(httpListener.Addr().(*net.TCPAddr).Port)))

	mux := http.NewServeMux()

	// Root
	mux.HandleFunc(RootPath, s.HandleRoot)
	// Varz
	mux.HandleFunc(VarzPath, s.HandleVarz)
	// Connz
	mux.HandleFunc(ConnzPath, s.HandleConnz)
	// Routez
	mux.HandleFunc(RoutezPath, s.HandleRoutez)
	// Subz
	mux.HandleFunc(SubszPath, s.HandleSubsz)
	// Subz alias for backwards compatibility
	mux.HandleFunc("/subscriptionsz", s.HandleSubsz)
	// Stacksz
	mux.HandleFunc(StackszPath, s.HandleStacksz)

	// Do not set a WriteTimeout because it could cause cURL/browser
	// to return empty response or unable to display page if the
	// server needs more time to build the response.
	srv := &http.Server{
		Addr:           hp,
		Handler:        mux,
		MaxHeaderBytes: 1 << 20,
	}
	s.mu.Lock()
	s.http = httpListener
	s.httpHandler = mux
	s.monitoringServer = srv
	s.mu.Unlock()

	go func() {
		srv.Serve(httpListener)
		srv.Handler = nil
		s.mu.Lock()
		s.httpHandler = nil
		s.mu.Unlock()
		s.done <- true
	}()

	return nil
}

// HTTPHandler returns the http.Handler object used to handle monitoring
// endpoints. It will return nil if the server is not configured for
// monitoring, or if the server has not been started yet (Server.Start()).
func (s *Server) HTTPHandler() http.Handler {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.httpHandler
}

// Perform a conditional deep copy due to reference nature of ClientConnectURLs.
// If updates are made to Info, this function should be consulted and updated.
// Assume lock is held.
func (s *Server) copyInfo() Info {
	info := s.info
	if info.ClientConnectURLs != nil {
		info.ClientConnectURLs = make([]string, len(s.info.ClientConnectURLs))
		copy(info.ClientConnectURLs, s.info.ClientConnectURLs)
	}
	return info
}

func (s *Server) createClient(conn net.Conn) *client {
	// Snapshot server options.
	opts := s.getOpts()

	max_pay := int64(opts.MaxPayload)
	max_subs := opts.MaxSubs
	now := time.Now()

	c := &client{srv: s, nc: conn, opts: defaultOpts, mpay: max_pay, msubs: max_subs, start: now, last: now}

	// Grab JSON info string
	s.mu.Lock()
	info := s.copyInfo()
	s.totalClients++
	s.mu.Unlock()

	// Grab lock
	c.mu.Lock()

	// Initialize
	c.initClient()

	c.Debugf("Client connection created")

	// Send our information.
	c.sendInfo(c.generateClientInfoJSON(info))

	// Unlock to register
	c.mu.Unlock()

	// Register with the server.
	s.mu.Lock()
	// If server is not running, Shutdown() may have already gathered the
	// list of connections to close. It won't contain this one, so we need
	// to bail out now otherwise the readLoop started down there would not
	// be interrupted.
	if !s.running {
		s.mu.Unlock()
		return c
	}

	// If there is a max connections specified, check that adding
	// this new client would not push us over the max
	if opts.MaxConn > 0 && len(s.clients) >= opts.MaxConn {
		s.mu.Unlock()
		c.maxConnExceeded()
		return nil
	}
	s.clients[c.cid] = c
	s.mu.Unlock()

	// Re-Grab lock
	c.mu.Lock()

	// Check for TLS
	if info.TLSRequired {
		c.Debugf("Starting TLS client connection handshake")
		c.nc = tls.Server(c.nc, opts.TLSConfig)
		conn := c.nc.(*tls.Conn)

		// Setup the timeout
		ttl := secondsToDuration(opts.TLSTimeout)
		time.AfterFunc(ttl, func() { tlsTimeout(c, conn) })
		conn.SetReadDeadline(time.Now().Add(ttl))

		// Force handshake
		c.mu.Unlock()
		if err := conn.Handshake(); err != nil {
			c.Errorf("TLS handshake error: %v", err)
			c.closeConnection(TLSHandshakeError)
			return nil
		}
		// Reset the read deadline
		conn.SetReadDeadline(time.Time{})

		// Re-Grab lock
		c.mu.Lock()

		// Indicate that handshake is complete (used in monitoring)
		c.flags.set(handshakeComplete)
	}

	// The connection may have been closed
	if c.nc == nil {
		c.mu.Unlock()
		return c
	}

	// Check for Auth. We schedule this timer after the TLS handshake to avoid
	// the race where the timer fires during the handshake and causes the
	// server to write bad data to the socket. See issue #432.
	if info.AuthRequired {
		c.setAuthTimer(secondsToDuration(opts.AuthTimeout))
	}

	// Do final client initialization

	// Set the Ping timer
	c.setPingTimer()

	// Spin up the read loop.
	s.startGoRoutine(c.readLoop)

	// Spin up the write loop.
	s.startGoRoutine(c.writeLoop)

	if info.TLSRequired {
		c.Debugf("TLS handshake complete")
		cs := c.nc.(*tls.Conn).ConnectionState()
		c.Debugf("TLS version %s, cipher suite %s", tlsVersion(cs.Version), tlsCipher(cs.CipherSuite))
	}

	c.mu.Unlock()

	return c
}

// This will save off a closed client in a ring buffer such that
// /connz can inspect. Useful for debugging, etc.
func (s *Server) saveClosedClient(c *client, nc net.Conn, reason ClosedState) {
	now := time.Now()

	c.mu.Lock()

	cc := &closedClient{}
	cc.fill(c, nc, now)
	cc.Stop = &now
	cc.Reason = reason.String()

	// Do subs, do not place by default in main ConnInfo
	if len(c.subs) > 0 {
		cc.subs = make([]string, 0, len(c.subs))
		for _, sub := range c.subs {
			cc.subs = append(cc.subs, string(sub.subject))
		}
	}
	// Hold user as well.
	cc.user = c.opts.Username
	c.mu.Unlock()

	// Place in the ring buffer
	s.mu.Lock()
	s.closed.append(cc)
	s.mu.Unlock()
}

// Adds the given array of urls to the server's INFO.ClientConnectURLs
// array. The server INFO JSON is regenerated.
// Note that a check is made to ensure that given URLs are not
// already present. So the INFO JSON is regenerated only if new ULRs
// were added.
// If there was a change, an INFO protocol is sent to registered clients
// that support async INFO protocols.
func (s *Server) addClientConnectURLsAndSendINFOToClients(urls []string) {
	s.updateServerINFOAndSendINFOToClients(urls, true)
}

// Removes the given array of urls from the server's INFO.ClientConnectURLs
// array. The server INFO JSON is regenerated if needed.
// If there was a change, an INFO protocol is sent to registered clients
// that support async INFO protocols.
func (s *Server) removeClientConnectURLsAndSendINFOToClients(urls []string) {
	s.updateServerINFOAndSendINFOToClients(urls, false)
}

// Updates the server's Info object with the given array of URLs and re-generate
// the infoJSON byte array, then send an (async) INFO protocol to clients that
// support it.
func (s *Server) updateServerINFOAndSendINFOToClients(urls []string, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Will be set to true if we alter the server's Info object.
	wasUpdated := false
	remove := !add
	for _, url := range urls {
		_, present := s.clientConnectURLsMap[url]
		if add && !present {
			s.clientConnectURLsMap[url] = struct{}{}
			wasUpdated = true
		} else if remove && present {
			delete(s.clientConnectURLsMap, url)
			wasUpdated = true
		}
	}
	if wasUpdated {
		// Recreate the info.ClientConnectURL array from the map
		s.info.ClientConnectURLs = s.info.ClientConnectURLs[:0]
		// Add this server client connect ULRs first...
		s.info.ClientConnectURLs = append(s.info.ClientConnectURLs, s.clientConnectURLs...)
		for url := range s.clientConnectURLsMap {
			s.info.ClientConnectURLs = append(s.info.ClientConnectURLs, url)
		}
		// Update the time of this update
		s.lastCURLsUpdate = time.Now().UnixNano()
		// Send to all registered clients that support async INFO protocols.
		s.sendAsyncInfoToClients()
	}
}

// Handle closing down a connection when the handshake has timedout.
func tlsTimeout(c *client, conn *tls.Conn) {
	c.mu.Lock()
	nc := c.nc
	c.mu.Unlock()
	// Check if already closed
	if nc == nil {
		return
	}
	cs := conn.ConnectionState()
	if !cs.HandshakeComplete {
		c.Errorf("TLS handshake timeout")
		c.sendErr("Secure Connection - TLS Required")
		c.closeConnection(TLSHandshakeError)
	}
}

// Seems silly we have to write these
func tlsVersion(ver uint16) string {
	switch ver {
	case tls.VersionTLS10:
		return "1.0"
	case tls.VersionTLS11:
		return "1.1"
	case tls.VersionTLS12:
		return "1.2"
	}
	return fmt.Sprintf("Unknown [%x]", ver)
}

// We use hex here so we don't need multiple versions
func tlsCipher(cs uint16) string {
	name, present := cipherMapByID[cs]
	if present {
		return name
	}
	return fmt.Sprintf("Unknown [%x]", cs)
}

// Remove a client or route from our internal accounting.
func (s *Server) removeClient(c *client) {
	var rID string
	c.mu.Lock()
	cid := c.cid
	typ := c.typ
	r := c.route
	if r != nil {
		rID = r.remoteID
	}
	updateProtoInfoCount := false
	if typ == CLIENT && c.opts.Protocol >= ClientProtoInfo {
		updateProtoInfoCount = true
	}
	c.mu.Unlock()

	s.mu.Lock()
	switch typ {
	case CLIENT:
		delete(s.clients, cid)
		if updateProtoInfoCount {
			s.cproto--
		}
	case ROUTER:
		delete(s.routes, cid)
		if r != nil {
			rc, ok := s.remotes[rID]
			// Only delete it if it is us..
			if ok && c == rc {
				delete(s.remotes, rID)
			}
		}
		// Remove from temporary map in case it is there.
		s.grMu.Lock()
		delete(s.grTmpClients, cid)
		s.grMu.Unlock()
	}
	s.mu.Unlock()
}

/////////////////////////////////////////////////////////////////
// These are some helpers for accounting in functional tests.
/////////////////////////////////////////////////////////////////

// NumRoutes will report the number of registered routes.
func (s *Server) NumRoutes() int {
	s.mu.Lock()
	nr := len(s.routes)
	s.mu.Unlock()
	return nr
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

// getClient will return the client associated with cid.
func (s *Server) getClient(cid uint64) *client {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clients[cid]
}

// NumSubscriptions will report how many subscriptions are active.
func (s *Server) NumSubscriptions() uint32 {
	s.mu.Lock()
	subs := s.sl.Count()
	s.mu.Unlock()
	return subs
}

// NumSlowConsumers will report the number of slow consumers.
func (s *Server) NumSlowConsumers() int64 {
	return atomic.LoadInt64(&s.slowConsumers)
}

// ConfigTime will report the last time the server configuration was loaded.
func (s *Server) ConfigTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.configTime
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

// MonitorAddr will return the net.Addr object for the monitoring listener.
func (s *Server) MonitorAddr() *net.TCPAddr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.http == nil {
		return nil
	}
	return s.http.Addr().(*net.TCPAddr)
}

// ClusterAddr returns the net.Addr object for the route listener.
func (s *Server) ClusterAddr() *net.TCPAddr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.routeListener == nil {
		return nil
	}
	return s.routeListener.Addr().(*net.TCPAddr)
}

// ProfilerAddr returns the net.Addr object for the route listener.
func (s *Server) ProfilerAddr() *net.TCPAddr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.profiler == nil {
		return nil
	}
	return s.profiler.Addr().(*net.TCPAddr)
}

// ReadyForConnections returns `true` if the server is ready to accept client
// and, if routing is enabled, route connections. If after the duration
// `dur` the server is still not ready, returns `false`.
func (s *Server) ReadyForConnections(dur time.Duration) bool {
	// Snapshot server options.
	opts := s.getOpts()

	end := time.Now().Add(dur)
	for time.Now().Before(end) {
		s.mu.Lock()
		ok := s.listener != nil && (opts.Cluster.Port == 0 || s.routeListener != nil)
		s.mu.Unlock()
		if ok {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}

// ID returns the server's ID
func (s *Server) ID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.info.ID
}

func (s *Server) startGoRoutine(f func()) {
	s.grMu.Lock()
	if s.grRunning {
		s.grWG.Add(1)
		go f()
	}
	s.grMu.Unlock()
}

func (s *Server) numClosedConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed.len()
}

func (s *Server) totalClosedConns() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed.totalConns()
}

func (s *Server) closedClients() []*closedClient {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed.closedClients()
}

// getClientConnectURLs returns suitable URLs for clients to connect to the listen
// port based on the server options' Host and Port. If the Host corresponds to
// "any" interfaces, this call returns the list of resolved IP addresses.
// If ClientAdvertise is set, returns the client advertise host and port.
// The server lock is assumed held on entry.
func (s *Server) getClientConnectURLs() []string {
	// Snapshot server options.
	opts := s.getOpts()

	urls := make([]string, 0, 1)

	// short circuit if client advertise is set
	if opts.ClientAdvertise != "" {
		// just use the info host/port. This is updated in s.New()
		urls = append(urls, net.JoinHostPort(s.info.Host, strconv.Itoa(s.info.Port)))
	} else {
		sPort := strconv.Itoa(opts.Port)
		ipAddr, err := net.ResolveIPAddr("ip", opts.Host)
		// If the host is "any" (0.0.0.0 or ::), get specific IPs from available
		// interfaces.
		if err == nil && ipAddr.IP.IsUnspecified() {
			var ip net.IP
			ifaces, _ := net.Interfaces()
			for _, i := range ifaces {
				addrs, _ := i.Addrs()
				for _, addr := range addrs {
					switch v := addr.(type) {
					case *net.IPNet:
						ip = v.IP
					case *net.IPAddr:
						ip = v.IP
					}
					// Skip non global unicast addresses
					if !ip.IsGlobalUnicast() || ip.IsUnspecified() {
						ip = nil
						continue
					}
					urls = append(urls, net.JoinHostPort(ip.String(), sPort))
				}
			}
		}
		if err != nil || len(urls) == 0 {
			// We are here if s.opts.Host is not "0.0.0.0" nor "::", or if for some
			// reason we could not add any URL in the loop above.
			// We had a case where a Windows VM was hosed and would have err == nil
			// and not add any address in the array in the loop above, and we
			// ended-up returning 0.0.0.0, which is problematic for Windows clients.
			// Check for 0.0.0.0 or :: specifically, and ignore if that's the case.
			if opts.Host == "0.0.0.0" || opts.Host == "::" {
				s.Errorf("Address %q can not be resolved properly", opts.Host)
			} else {
				urls = append(urls, net.JoinHostPort(opts.Host, sPort))
			}
		}
	}

	return urls
}

// if the ip is not specified, attempt to resolve it
func resolveHostPorts(addr net.Listener) []string {
	hostPorts := make([]string, 0)
	hp := addr.Addr().(*net.TCPAddr)
	port := strconv.Itoa(hp.Port)
	if hp.IP.IsUnspecified() {
		var ip net.IP
		ifaces, _ := net.Interfaces()
		for _, i := range ifaces {
			addrs, _ := i.Addrs()
			for _, addr := range addrs {
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
					hostPorts = append(hostPorts, net.JoinHostPort(ip.String(), port))
				case *net.IPAddr:
					ip = v.IP
					hostPorts = append(hostPorts, net.JoinHostPort(ip.String(), port))
				default:
					continue
				}
			}
		}
	} else {
		hostPorts = append(hostPorts, net.JoinHostPort(hp.IP.String(), port))
	}
	return hostPorts
}

// format the address of a net.Listener with a protocol
func formatURL(protocol string, addr net.Listener) []string {
	hostports := resolveHostPorts(addr)
	for i, hp := range hostports {
		hostports[i] = fmt.Sprintf("%s://%s", protocol, hp)
	}
	return hostports
}

// Ports describes URLs that the server can be contacted in
type Ports struct {
	Nats       []string `json:"nats,omitempty"`
	Monitoring []string `json:"monitoring,omitempty"`
	Cluster    []string `json:"cluster,omitempty"`
	Profile    []string `json:"profile,omitempty"`
}

// Attempts to resolve all the ports. If after maxWait the ports are not
// resolved, it returns nil. Otherwise it returns a Ports struct
// describing ports where the server can be contacted
func (s *Server) PortsInfo(maxWait time.Duration) *Ports {
	if s.readyForListeners(maxWait) {
		opts := s.getOpts()

		s.mu.Lock()
		info := s.copyInfo()
		listener := s.listener
		httpListener := s.http
		clusterListener := s.routeListener
		profileListener := s.profiler
		s.mu.Unlock()

		ports := Ports{}

		if listener != nil {
			natsProto := "nats"
			if info.TLSRequired {
				natsProto = "tls"
			}
			ports.Nats = formatURL(natsProto, listener)
		}

		if httpListener != nil {
			monProto := "http"
			if opts.HTTPSPort != 0 {
				monProto = "https"
			}
			ports.Monitoring = formatURL(monProto, httpListener)
		}

		if clusterListener != nil {
			clusterProto := "nats"
			if opts.Cluster.TLSConfig != nil {
				clusterProto = "tls"
			}
			ports.Cluster = formatURL(clusterProto, clusterListener)
		}

		if profileListener != nil {
			ports.Profile = formatURL("http", profileListener)
		}

		return &ports
	}

	return nil
}

// Returns the portsFile. If a non-empty dirHint is provided, the dirHint
// path is used instead of the server option value
func (s *Server) portFile(dirHint string) string {
	dirname := s.getOpts().PortsFileDir
	if dirHint != "" {
		dirname = dirHint
	}
	if dirname == _EMPTY_ {
		return _EMPTY_
	}
	return path.Join(dirname, fmt.Sprintf("%s_%d.ports", path.Base(os.Args[0]), os.Getpid()))
}

// Delete the ports file. If a non-empty dirHint is provided, the dirHint
// path is used instead of the server option value
func (s *Server) deletePortsFile(hintDir string) {
	portsFile := s.portFile(hintDir)
	if portsFile != "" {
		if err := os.Remove(portsFile); err != nil {
			s.Errorf("Error cleaning up ports file %s: %v", portsFile, err)
		}
	}
}

// Writes a file with a serialized Ports to the specified ports_file_dir.
// The name of the file is `exename_pid.ports`, typically gnatsd_pid.ports.
// if ports file is not set, this function has no effect
func (s *Server) logPorts() {
	opts := s.getOpts()
	portsFile := s.portFile(opts.PortsFileDir)
	if portsFile != _EMPTY_ {
		go func() {
			info := s.PortsInfo(5 * time.Second)
			if info == nil {
				s.Errorf("Unable to resolve the ports in the specified time")
				return
			}
			data, err := json.Marshal(info)
			if err != nil {
				s.Errorf("Error marshaling ports file: %v", err)
				return
			}
			if err := ioutil.WriteFile(portsFile, data, 0666); err != nil {
				s.Errorf("Error writing ports file (%s): %v", portsFile, err)
				return
			}

		}()
	}
}

// waits until a calculated list of listeners is resolved or a timeout
func (s *Server) readyForListeners(dur time.Duration) bool {
	end := time.Now().Add(dur)
	for time.Now().Before(end) {
		s.mu.Lock()
		listeners := s.serviceListeners()
		s.mu.Unlock()
		if len(listeners) == 0 {
			return false
		}

		ok := true
		for _, l := range listeners {
			if l == nil {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
		select {
		case <-s.quitCh:
			return false
		case <-time.After(25 * time.Millisecond):
			// continue - unable to select from quit - we are still running
		}
	}
	return false
}

// returns a list of listeners that are intended for the process
// if the entry is nil, the interface is yet to be resolved
func (s *Server) serviceListeners() []net.Listener {
	listeners := make([]net.Listener, 0)
	opts := s.getOpts()
	listeners = append(listeners, s.listener)
	if opts.Cluster.Port != 0 {
		listeners = append(listeners, s.routeListener)
	}
	if opts.HTTPPort != 0 || opts.HTTPSPort != 0 {
		listeners = append(listeners, s.http)
	}
	if opts.ProfPort != 0 {
		listeners = append(listeners, s.profiler)
	}
	return listeners
}
