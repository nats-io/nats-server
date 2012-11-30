// Copyright 2012 Apcera Inc. All rights reserved.

package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/apcera/gnatsd/hashmap"
	"github.com/apcera/gnatsd/sublist"
)

// The size of the bufio reader/writer on top of the socket.
//const defaultBufSize = 32768
const defaultBufSize = 32768

type client struct {
	mu   sync.Mutex
	cid  uint64
	opts clientOpts
	conn net.Conn
	bw   *bufio.Writer
	srv  *Server
	subs *hashmap.HashMap
	pcd  map[*client]struct{}
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

var defaultOpts = clientOpts{true, true, false}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (c *client) readLoop() {
	b := make([]byte, defaultBufSize)
	for {
		n, err := c.conn.Read(b)
		if err != nil {
			c.closeConnection()
			return
		}
		if err := c.parse(b[:n]); err != nil {
			Logf("Parse Error: %v\n", err)
			c.closeConnection()
			return
		}
		// Check pending clients for flush.
		for cp, _ := range c.pcd {
			// Flush those in the set
			cp.mu.Lock()
			err := cp.bw.Flush()
			cp.mu.Unlock()
			if err != nil {
				// FIXME, close connection?
				Logf("Error flushing client connection: %v\n", err)
			}
			delete(c.pcd, cp)
		}
	}
}

func (c *client) traceMsg(msg []byte) {
	pm := fmt.Sprintf("Processing msg: %d", c.nm)
	opa := []interface{}{pm, string(c.pa.subject), string(c.pa.reply), string(msg)}
	Trace(logStr(opa), fmt.Sprintf("c: %d", c.cid))
}

func (c *client) traceOp(op string, arg []byte) {
	if !trace {
		return
	}
	opa := []interface{}{fmt.Sprintf("%s OP", op)}
	if arg != nil {
		opa = append(opa, fmt.Sprintf("%s %s", op, string(arg)))
	}
	Trace(logStr(opa), fmt.Sprintf("c: %d", c.cid))
}

func (c *client) processConnect(arg []byte) error {
	c.traceOp("CONNECT", arg)
	// FIXME, check err
	err := json.Unmarshal(arg, &c.opts)
	if c.opts.Verbose {
		c.sendOK()
	}
	return err
}

func (c *client) sendErr(err string) {
	c.mu.Lock()
	c.bw.WriteString(fmt.Sprintf("-ERR '%s'\r\n", err))
	c.pcd[c] = needFlush
	c.mu.Unlock()
}

func (c *client) sendOK() {
	c.mu.Lock()
	c.bw.WriteString("+OK\r\n")
	c.pcd[c] = needFlush
	c.mu.Unlock()
}

func (c *client) processPing() {
	c.traceOp("PING", nil)
	if c.conn == nil {
		return
	}
	c.mu.Lock()
	c.bw.WriteString("PONG\r\n")
	err := c.bw.Flush()
	c.mu.Unlock()
	if err != nil {
		// FIXME, close connection?
		Logf("Error flushing client connection [PING]: %v\n", err)
	}
}

const argsLenMax = 3

func (c *client) processPub(arg []byte) error {
	if trace {
		c.traceOp("PUB", arg)
	}

	// Unroll splitArgs to avoid runtime/heap issues
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
	if c.opts.Pedantic && !sublist.IsValidLiteralSubject(c.pa.subject) {
		c.sendErr("Invalid Subject")
	}
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

func (c *client) processSub(argo []byte) (err error) {
	c.traceOp("SUB", argo)
	// Copy so we do not reference a potentially large buffer
	arg := make([]byte, len(argo))
	copy(arg, argo)
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

	c.mu.Lock()
	c.subs.Set(sub.sid, sub)
	if c.srv != nil {
		err = c.srv.sl.Insert(sub.subject, sub)
	}
	c.mu.Unlock()
	if err != nil{
		c.sendErr("Invalid Subject")
	} else if c.opts.Verbose {
		c.sendOK()
	}
	return nil
}

func (c *client) unsubscribe(sub *subscription) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if sub.max > 0 && sub.nm <= sub.max {
		return
	}
	c.traceOp("DELSUB", sub.sid)
	c.subs.Remove(sub.sid)
	if c.srv != nil {
		c.srv.sl.Remove(sub.subject, sub)
	}
}

func (c *client) processUnsub(arg []byte) error {
	c.traceOp("UNSUB", arg)
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
	if sub, ok := (c.subs.Get(sid)).(*subscription); ok {
		if max > 0 {
			sub.max = int64(max)
		}
		c.unsubscribe(sub)
	}
	if c.opts.Verbose {
		c.sendOK()
	}
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

// Used to treat map as efficient set
type empty struct{}

var needFlush = empty{}

func (c *client) deliverMsg(sub *subscription, mh, msg []byte) {
	if sub.client == nil || sub.client.conn == nil {
		return
	}
	client := sub.client
	client.mu.Lock()
	sub.nm++
	if sub.max > 0 && sub.nm > sub.max {
		client.mu.Unlock()
		client.unsubscribe(sub)
		return
	}
	// Deliver to the client.
	_, err := client.bw.Write(mh)
	if err != nil {
		Logf("Error writing msg header: %v\n", err)
	}
	_, err = client.bw.Write(msg)
	if err != nil {
		Logf("Error writing msg: %v\n", err)
	}
	// FIXME, this is already attached to original message
	_, err = client.bw.WriteString(CR_LF)
	if err != nil {
		Logf("Error writing CRLF: %v\n", err)
	}
	client.mu.Unlock()
	c.pcd[sub.client] = needFlush
}

func (c *client) processMsg(msg []byte) {
	c.nm++
	if trace {
		c.traceMsg(msg)
	}
	if c.srv == nil {
		return
	}
	if c.opts.Verbose {
		c.sendOK()
	}

	scratch := [512]byte{}
	msgh := scratch[:0]

	r := c.srv.sl.Match(c.pa.subject)
	if len(r) <= 0 {
		return
	}

	// msg header
	// FIXME, put MSG into initializer
	msgh = append(msgh, "MSG "...)
	msgh = append(msgh, c.pa.subject...)
	msgh = append(msgh, ' ')
	si := len(msgh)

	var qmap map[string][]*subscription
	var qsubs []*subscription

	for _, v := range r {
		sub := v.(*subscription)
		if sub.queue != nil {
			// FIXME, this can be more efficient
			if qmap == nil {
				qmap = make(map[string][]*subscription)
			}
			//qname := *(*string)(unsafe.Pointer(&sub.queue))
			qname := string(sub.queue)
			qsubs = qmap[qname]
			if qsubs == nil {
				qsubs = make([]*subscription, 0, 4)
			}
			qsubs = append(qsubs, sub)
			qmap[qname] = qsubs
			continue
		}
		mh := c.msgHeader(msgh[:si], sub)
		c.deliverMsg(sub, mh, msg)
	}
	if qmap != nil {
		for _, qsubs := range qmap {
			index := rand.Int() % len(qsubs)
			sub := qsubs[index]
			mh := c.msgHeader(msgh[:si], sub)
			c.deliverMsg(sub, mh, msg)
		}
	}
}

func (c *client) closeConnection() {
	if c.conn == nil {
		return
	}
	Debug("Client connection closed", clientConnStr(c.conn), c.cid)

	c.mu.Lock()
	c.bw.Flush()
	c.conn.Close()
	c.conn = nil
	subs := c.subs.All()
	c.mu.Unlock()

	if c.srv != nil {
		for _, s := range subs {
			if sub, ok := s.(*subscription); ok {
				c.srv.sl.Remove(sub.subject, sub)
			}
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
