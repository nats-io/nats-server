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
	Host          string
	Port          int
	Trace         bool
	Debug         bool
	NoLog         bool
	Logtime       bool
	MaxConn       int
	Username      string
	Password      string
	Authorization string
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
	opts     Options
	trace    bool
	debug    bool
}

func processOptions(opt *Options) {
	// Setup non-standard Go defaults
	if opt.Host == "" {
		opt.Host = DEFAULT_HOST
	}
	if opt.Port == 0 {
		opt.Port = DEFAULT_PORT
	}
	if opt.MaxConn == 0 {
		opt.MaxConn = DEFAULT_MAX_CONNECTIONS
	}
}

func New(opts Options) *Server {
	processOptions(&opts)
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
	}
	// Setup logging with flags
	s.LogInit()

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
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func(){
		for sig := range c {
			Debugf("Trapped Signal; %v", sig)
			Log("Server Exiting..")
			os.Exit(0)
		}
	}()
}

func (s *Server) AcceptLoop() {
	Logf("Starting nats-server version %s on port %d", VERSION, s.opts.Port)

	hp := fmt.Sprintf("%s:%d", s.opts.Host, s.opts.Port)
	l, e := net.Listen("tcp", hp)
	if e != nil {
		Fatalf("Error listening on port: %d - %v", s.opts.Port, e)
		return
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				Logf("Accept error: %v", err)
			}
			continue
		}
		s.createClient(conn)
	}
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
	if s.info.AuthRequired {
		c.atmr = time.AfterFunc(AUTH_TIMEOUT, func() { c.authViolation() })
	}
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
