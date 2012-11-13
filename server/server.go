// Copyright 2012 Apcera Inc. All rights reserved.

package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/apcera/gnatsd/hashmap"
	"github.com/apcera/gnatsd/sublist"
)

type Options struct {
	Host    string
	Port    int
	Trace   bool
	Debug   bool
	Logtime bool
	MaxConn int
}

type info struct {
	Id           string `json:"server_id"`
	Version      string `json:"version"`
	Host         string `json:"host"`
	Port         int    `json:"port"`
	AuthRequired bool   `json:"auth_required"`
	SslRequired  bool   `json:"ssl_required"`
	MaxPayload   int    `json:"max_payload"`
}

type Server struct {
	info     info
	infoJson []byte
	sl       *sublist.Sublist
	gcid     uint64
	opts     Options
	trace    bool
	debug    bool
}

func optionDefaults(opt *Options) {
	if opt.MaxConn == 0 {
		opt.MaxConn = DEFAULT_MAX_CONNECTIONS
	}
}

func New(opts Options) *Server {
	optionDefaults(&opts)
	inf := info{
		Id:           genId(),
		Version:      VERSION,
		Host:         opts.Host,
		Port:         opts.Port,
		AuthRequired: false,
		SslRequired:  false,
		MaxPayload:   MAX_PAYLOAD_SIZE,
	}
	s := &Server{
		info:  inf,
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
	return s
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
	c := &client{srv: s, conn: conn}
	c.cid = atomic.AddUint64(&s.gcid, 1)
	c.bw = bufio.NewWriterSize(c.conn, defaultBufSize)
	c.br = bufio.NewReaderSize(c.conn, defaultBufSize)
	c.subs = hashmap.New()

     if ip, ok := conn.(*net.TCPConn); ok {
	     ip.SetReadBuffer(32768)
	 }

	s.sendInfo(c)
	go c.readLoop()

	Debug("Client connection created", clientConnStr(conn), c.cid)

	return c
}

func (s *Server) sendInfo(c *client) {
	// FIXME, err
	c.conn.Write(s.infoJson)
}
