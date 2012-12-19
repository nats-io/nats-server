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
	atmr *time.Timer
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
	Verbose       bool   `json:"verbose"`
	Pedantic      bool   `json:"pedantic"`
	SslRequired   bool   `json:"ssl_required"`
	Authorization string `json:"auth_token"`
	Username      string `json:"user"`
	Password      string `json:"pass"`
	Name          string `json:"name"`
}

var defaultOpts = clientOpts{Verbose: true, Pedantic: true}

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
			Log(err.Error(), clientConnStr(c.conn), c.cid)
			c.closeConnection()
			return
		}
		// Check pending clients for flush.
		for cp, _ := range c.pcd {
			// Flush those in the set
			cp.mu.Lock()
			if cp.conn != nil {
				cp.conn.SetWriteDeadline(time.Now().Add(DEFAULT_FLUSH_DEADLINE))
				err := cp.bw.Flush()
				cp.conn.SetWriteDeadline(time.Time{})
				if err != nil {
					Debugf("Error flushing: %v", err)
					cp.closeConnection()
				}
			}
			cp.mu.Unlock()
			delete(c.pcd, cp)
		}
		// Check to see if we got closed, e.g. slow consumer
		if c.conn == nil {
			return
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

	// This will be resolved regardless before we exit this func,
	// so we can just clear it here.
	if c.atmr != nil {
		c.atmr.Stop()
		c.atmr = nil
	}

	// FIXME, check err
	if err := json.Unmarshal(arg, &c.opts); err != nil {
		return err
	}
	// Check for Auth
	if c.srv != nil {
		if ok := c.srv.checkAuth(c); !ok {
			c.sendErr("Authorization is Required")
			return fmt.Errorf("Authorization Error")
		}
	}
	if c.opts.Verbose {
		c.sendOK()
	}
	return nil
}

func (c *client) authViolation() {
	c.sendErr("Authorization is Required")
	fmt.Printf("AUTH TIMER EXPIRED!!\n")
	c.closeConnection()
}

func (c *client) sendErr(err string) {
	c.mu.Lock()
	if c.bw != nil {
		c.bw.WriteString(fmt.Sprintf("-ERR '%s'\r\n", err))
		c.pcd[c] = needFlush
	}
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
	if err != nil {
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

	// Check to see if our writes will cause a flush
	// in the underlying bufio. If so limit time we
	// will wait for flush to complete.

	deadlineSet := false
	if client.bw.Available() < (len(mh) + len(msg) + len(CR_LF)) {
		client.conn.SetWriteDeadline(time.Now().Add(DEFAULT_FLUSH_DEADLINE))
		deadlineSet = true
	}

	// Deliver to the client.
	_, err := client.bw.Write(mh)
	if err != nil {
		goto writeErr
	}

	_, err = client.bw.Write(msg)
	if err != nil {
		goto writeErr
	}

	// FIXME, this is already attached to original message
	_, err = client.bw.WriteString(CR_LF)
	if err != nil {
		goto writeErr
	}

	if deadlineSet {
		client.conn.SetWriteDeadline(time.Time{})
	}

	client.mu.Unlock()
	c.pcd[client] = needFlush
	return

writeErr:
	if deadlineSet {
		client.conn.SetWriteDeadline(time.Time{})
	}
	client.mu.Unlock()

	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		// FIXME: SlowConsumer logic
		Log("Slow Consumer Detected", clientConnStr(client.conn), client.cid)
		client.closeConnection()
	} else {
		Debugf("Error writing msg: %v", err)
	}
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

// Lock should be held
func (c *client) clearAuthTimer() {
	if c.atmr == nil {
		return
	}
	c.atmr.Stop()
	c.atmr = nil
}

// Lock should be held
func (c *client) clearConnection() {
	if c.conn == nil {
		return
	}
	c.bw.Flush()
	c.conn.Close()
	c.conn = nil
}

func (c *client) closeConnection() {
	if c.conn == nil {
		return
	}
	Debug("Client connection closed", clientConnStr(c.conn), c.cid)

	c.mu.Lock()
	c.clearAuthTimer()
	c.clearConnection()
	subs := c.subs.All()
	c.mu.Unlock()

	if c.srv != nil {
		// Unregister
		c.srv.removeClient(c)

		// Remove subscriptions.
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
