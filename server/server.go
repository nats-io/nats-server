// Copyright 2012 Apcera Inc. All rights reserved.

package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apcera/gnatsd/hashmap"
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
	done     chan bool
	start    time.Time
	stats
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
	// Setup logging with flags
	s.LogInit()

	// For tracing clients
	s.clients = make(map[uint64]*client)

	// Generate the info json
	b, err := json.Marshal(s.info)
	if err != nil {
		Fatalf("Err marshalling INFO JSON: %+v\n", err)
	}
	s.infoJson = []byte(fmt.Sprintf("INFO %s %s", b, CR_LF))

	s.handleSignals()

	return s
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

// Shutdown will shutdown the server instance by kicking out the AcceptLoop
// and closing all associated clients.
func (s *Server) Shutdown() {
	s.running = false
	// Close client connections
	// FIXME(dlc) lock? will call back into remove..
	for _, c := range s.clients {
		c.closeConnection()
	}
	// Kick AcceptLoop()
	if s.listener != nil {
		s.listener.Close()
		s.listener = nil
	}
	<-s.done
}

func (s *Server) AcceptLoop() {
	Logf("Starting nats-server version %s on port %d", VERSION, s.opts.Port)

	hp := fmt.Sprintf("%s:%d", s.opts.Host, s.opts.Port)
	l, e := net.Listen("tcp", hp)
	if e != nil {
		Fatalf("Error listening on port: %d - %v", s.opts.Port, e)
		return
	}

	Logf("nats-server is ready")

	// Setup state that can enable shutdown
	s.listener = l
	s.running = true

	for s.running {
		conn, err := s.listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				Logf("Accept error: %v", err)
			}
			continue
		}
		s.createClient(conn)
	}
	s.done <- true
	Log("Server Exiting..")
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
	c := &client{srv: s, conn: conn, opts: defaultOpts}
	c.cid = atomic.AddUint64(&s.gcid, 1)

	c.bw = bufio.NewWriterSize(c.conn, defaultBufSize)
	c.subs = hashmap.New()

	// This is to track pending clients that have data to be flushed
	// after we process inbound msgs from our own connection.
	c.pcd = make(map[*client]struct{})

	Debug("Client connection created", clientConnStr(conn), c.cid)

	if ip, ok := conn.(*net.TCPConn); ok {
		ip.SetReadBuffer(defaultBufSize)
	}

	s.sendInfo(c)
	go c.readLoop()

	// Check for Auth
	if s.info.AuthRequired {
		c.setAuthTimer(AUTH_TIMEOUT) // FIXME(dlc): Make option
	}
	// Set the Ping timer
	c.setPingTimer()

	// Register with the server.
	s.mu.Lock()
	s.clients[c.cid] = c
	s.mu.Unlock()

	return c
}

func (s *Server) sendInfo(c *client) {
	// FIXME, err
	c.conn.Write(s.infoJson)
}

// Check auth and return boolean indicating if client is ok
func (s *Server) checkAuth(c *client) bool {
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

func (s *Server) removeClient(c *client) {
	s.mu.Lock()
	delete(s.clients, c.cid)
	s.mu.Unlock()
}
