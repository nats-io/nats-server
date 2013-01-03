// Copyright 2012 Apcera Inc. All rights reserved.

package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/apcera/gnatsd/hashmap"
	"github.com/apcera/gnatsd/sublist"
)

type Options struct {
	Host           string        `json:"addr"`
	Port           int           `json:"port"`
	Trace          bool          `json:"-"`
	Debug          bool          `json:"-"`
	NoLog          bool          `json:"-"`
	NoSigs         bool          `json:"-"`
	Logtime        bool          `json:"-"`
	MaxConn        int           `json:"max_connections"`
	Username       string        `json:"user,omitempty"`
	Password       string        `json:"-"`
	Authorization  string        `json:"-"`
	PingInterval   time.Duration `json:"ping_interval"`
	MaxPingsOut    int           `json:"ping_max"`
	HttpPort       int           `json:"http_port"`
	SslTimeout     float64       `json:"ssl_timeout"`
	AuthTimeout    float64       `json:"auth_timeout"`
	MaxControlLine int           `json:"max_control_line"`
	MaxPayload     int           `json:"max_payload"`
}

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
	inMsgs int64
	outMsgs int64
	inBytes int64
	outBytes int64
}

func processOptions(opts *Options) {
	// Setup non-standard Go defaults
	if opts.Host == "" {
		opts.Host = DEFAULT_HOST
	}
	if opts.Port == 0 {
		opts.Port = DEFAULT_PORT
	}
	if opts.MaxConn == 0 {
		opts.MaxConn = DEFAULT_MAX_CONNECTIONS
	}
	if opts.PingInterval == 0 {
		opts.PingInterval = DEFAULT_PING_INTERVAL
	}
	if opts.MaxPingsOut == 0 {
		opts.MaxPingsOut = DEFAULT_PING_MAX_OUT
	}
	if opts.SslTimeout == 0 {
		opts.SslTimeout = float64(SSL_TIMEOUT) / float64(time.Second)
	}
	if opts.AuthTimeout == 0 {
		opts.AuthTimeout = float64(AUTH_TIMEOUT) / float64(time.Second)
	}
	if opts.MaxControlLine == 0 {
		opts.MaxControlLine = MAX_CONTROL_LINE_SIZE
	}
	if opts.MaxPayload == 0 {
		opts.MaxPayload = MAX_PAYLOAD_SIZE
	}
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

func clientConnStr(conn net.Conn) interface{} {
	if ip, ok := conn.(*net.TCPConn); ok {
		addr := ip.RemoteAddr().(*net.TCPAddr)
		return []string{fmt.Sprintf("%v, %d", addr.IP, addr.Port)}
	}
	return "N/A"
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
	s.clients[c.cid] = c
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
	delete(s.clients, c.cid)
}