// Copyright 2012 Apcera Inc. All rights reserved.

package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/apcera/gnatsd/hashmap"
)

// The size of the bufio reader/writer on top of the socket.
//const defaultBufSize = 32768
const defaultBufSize = 65536

type client struct {
	mu   sync.Mutex
	cid  uint64
	opts clientOpts
	conn net.Conn
	bw   *bufio.Writer
	br   *bufio.Reader
	srv  *Server
	subs *hashmap.HashMap
	cstats
	parseState
}

func (c *client) String() string {
	return fmt.Sprintf("cid:%d", c.cid)
}

type cstats struct {
	nr int
	nb int
	nm int
}

type subscription struct {
	client  *client
	subject []byte
	queue   []byte
	sid     []byte
	nm      int64
	max     int64
}

type clientOpts struct {
	Verbose     bool `json:"verbose"`
	Pedantic    bool `json:"pedantic"`
	SslRequired bool `json:"ssl_required"`
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (c *client) readLoop() {
	b := make([]byte, defaultBufSize)
//	log.Printf("b len = %d, cap = %d\n", len(b), cap(b))
	for {
		n, err := c.conn.Read(b)
		if err != nil {
//			log.Printf("Encountered a read error: %v\n", err)
			c.closeConnection()
			return
		}
		if err := c.parse(b[:n]); err != nil {
			log.Printf("Parse Error: %v\n", err)
			c.closeConnection()
			return
		}
	}
}

func (c *client) processConnect(arg []byte) error {
	//	log.Printf("Got connect arg: '%s'\n", arg)
	// FIXME, check err
	return json.Unmarshal(arg, &c.opts)
}

var pongResp = []byte(fmt.Sprintf("PONG%s", CR_LF))

func (c *client) processPing() {
//	log.Printf("Process ping\n")
	if c.conn == nil {
		return
	}
	// FIXME, check err
	c.conn.Write(pongResp)
}

const argsLenMax = 3

func (c *client) processPub(arg []byte) error {
	//	log.Printf("Got pub arg: '%s'\n", arg)
	args := splitArg(arg)
	switch len(args) {
	case 2:
		c.pa.subject = args[0]
		c.pa.reply = nil
		c.pa.size = parseSize(args[1])
		c.pa.szb = args[1]
	case 3:
		c.pa.subject = args[0]
		c.pa.reply = args[1]
		c.pa.size = parseSize(args[2])
		c.pa.szb = args[2]
	default:
		return fmt.Errorf("processPub Parse Error: '%s'", arg)
	}
	if c.pa.size < 0 {
		return fmt.Errorf("processPub Bad or Missing Size: '%s'", arg)
	}
//	log.Printf("Parsed pubArg: %+v\n", c.pa)
	return nil
}

func splitArg(arg []byte) [][]byte {
	a := [argsLenMax][]byte{}
	args := a[:0]
	start := -1
	for i, b := range arg {
		switch b {
		case ' ', '\t', '\r', '\n':
			if start >= 0 {
				args = append(args, arg[start:i])
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		args = append(args, arg[start:])
	}
	return args
}

func (c *client) processSub(argo []byte) error {
	// Copy so we do not reference a potentially large buffer
	arg := make([]byte, len(argo))
	copy(arg, argo)
//	log.Printf("Got sub arg for client[%v]: '%s'\n", c, arg)
	args := splitArg(arg)
	sub := &subscription{client: c}
	switch len(args) {
	case 2:
		sub.subject = args[0]
		sub.queue = nil
		sub.sid = args[1]
	case 3:
		sub.subject = args[0]
		sub.queue = args[1]
		sub.sid = args[2]
	default:
		return fmt.Errorf("processSub Parse Error: '%s'", arg)
	}

	//	log.Printf("sub.subject = '%s'\n", sub.subject)
	//	log.Printf("sub.queue = '%s'\n", sub.queue)
	//	log.Printf("sub.sid = '%s'\n", sub.sid)

	if c.subs != nil {
		c.subs.Set(sub.sid, sub)
	}
	if c.srv != nil {
		c.srv.sl.Insert(sub.subject, sub)
	}
	return nil
}

func (c *client) unsubscribe(sub *subscription) {
	if sub.max > 0 && sub.nm <= sub.max {
		return
	}
	c.subs.Remove(sub.sid)
	if c.srv != nil {
		c.srv.sl.Remove(sub.subject, sub)
	}
}

func (c *client) processUnsub(arg []byte) error {
//	log.Printf("Got unsub arg for client[%v]: '%s'\n", c, arg)
	args := splitArg(arg)

	var sid []byte
	max := -1

	switch len(args) {
	case 1:
		sid = args[0]
	case 2:
		sid = args[0]
		max = parseSize(args[1])
	default:
		return fmt.Errorf("processUnsub Parse Error: '%s'", arg)
	}
	sub := (c.subs.Get(sid)).(*subscription)
	if max > 0 {
		sub.max = int64(max)
	}
	c.unsubscribe(sub)
	return nil
}

func (c *client) msgHeader(mh []byte, sub *subscription) []byte {
	mh = append(mh, sub.sid...)
	mh = append(mh, ' ')
	if c.pa.reply != nil {
		mh = append(mh, c.pa.reply...)
		mh = append(mh, ' ')
	}
	mh = append(mh, c.pa.szb...)
	mh = append(mh, "\r\n"...)
	return mh
}

func (sub *subscription) deliverMsg(mh, msg []byte) {
	sub.nm++
	if sub.client == nil || sub.client.conn == nil {
		return
	}
	if sub.max > 0 && sub.nm > sub.max {
		sub.client.unsubscribe(sub)
		return
	}

	sub.client.mu.Lock()
	sub.client.bw.Write(mh)
	sub.client.bw.Write(msg)
	sub.client.bw.WriteString("\r\n")
	// FIXME: Make efficient with flusher..
	sub.client.bw.Flush()
	sub.client.mu.Unlock()
}

// TODO
// Block pub goroutine on bufio locked write buffer with
// go flusher routine. Single for all connections?

func (c *client) processMsg(msg []byte) {
	c.nm++
	if c.srv == nil {
		return
	}

	qsubsA := [32]*subscription{}
	qsubs := qsubsA[:0]
	scratch := [512]byte{}
	msgh := scratch[:0]

	r := c.srv.sl.Match(c.pa.subject)
	if len(r) <= 0 {
		return
	}

	// msg header
	msgh = append(msgh, "MSG "...)
	msgh = append(msgh, c.pa.subject...)
	msgh = append(msgh, ' ')
	si := len(msgh)

	for _, v := range r {
		sub := v.(*subscription)
		if sub.queue != nil {
			qsubs = append(qsubs, sub)
			continue
		}
		mh := c.msgHeader(msgh[:si], sub)
		sub.deliverMsg(mh, msg)
	}
	if len(qsubs) > 0 {
		index := rand.Int() % len(qsubs)
		sub := qsubs[index]
		mh := c.msgHeader(msgh[:si], sub)
		sub.deliverMsg(mh, msg)
	}
}

func (c *client) closeConnection() {
	if c.conn == nil {
		return
	}
	//	log.Printf("Closing Connection: %v\n", c)
	//	c.bw.Flush()
	c.conn.Close()
	c.conn = nil

	if c.srv != nil {
		subs := c.subs.All()
		for _, s := range subs {
			sub := s.(*subscription)
			c.srv.sl.Remove(sub.subject, sub)
		}
	}

	/*
		log.Printf("Sublist Stats: %+v\n", c.srv.sl.Stats())
		if c.nr > 0 {
			log.Printf("stats: %d %d %d\n", c.nr, c.nb, c.nm)
			log.Printf("bytes per read: %d\n", c.nb/c.nr)
			log.Printf("msgs per read: %d\n", c.nm/c.nr)
		}
	*/
}
