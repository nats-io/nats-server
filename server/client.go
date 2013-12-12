// Copyright 2012-2013 Apcera Inc. All rights reserved.

package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apcera/gnatsd/hashmap"
	"github.com/apcera/gnatsd/sublist"
)

const (
	// The size of the bufio reader/writer on top of the socket.
	defaultBufSize = 32768
	// Scratch buffer size for the processMsg() calls.
	msgScratchSize = 512
	msgHeadProto   = "MSG "
)

// Type of client
const (
	// CLIENT is an end user.
	CLIENT = iota
	// ROUTER is another routers in the cluster.
	ROUTER
)

type client struct {
	mu   sync.Mutex
	typ  int
	cid  uint64
	opts clientOpts
	nc   net.Conn
	bw   *bufio.Writer
	srv  *Server
	subs *hashmap.HashMap
	pcd  map[*client]struct{}
	atmr *time.Timer
	ptmr *time.Timer
	pout int
	msgb [msgScratchSize]byte
	parseState
	stats

	route *route
}

func (c *client) String() (id string) {
	switch c.typ {
	case CLIENT:
		id = fmt.Sprintf("cid:%d", c.cid)
	case ROUTER:
		id = fmt.Sprintf("rid:%d", c.cid)
	}
	return id
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

func clientConnStr(conn net.Conn) interface{} {
	if ip, ok := conn.(*net.TCPConn); ok {
		addr := ip.RemoteAddr().(*net.TCPAddr)
		return []string{fmt.Sprintf("%v, %d", addr.IP, addr.Port)}
	}
	return "N/A"
}

// Lock should be held
func (c *client) initClient() {
	s := c.srv
	c.cid = atomic.AddUint64(&s.gcid, 1)
	c.bw = bufio.NewWriterSize(c.nc, defaultBufSize)
	c.subs = hashmap.New()

	// This is a scratch buffer used for processMsg()
	// The msg header starts with "MSG ",
	// in bytes that is [77 83 71 32].
	c.msgb = [msgScratchSize]byte{77, 83, 71, 32}

	// This is to track pending clients that have data to be flushed
	// after we process inbound msgs from our own connection.
	c.pcd = make(map[*client]struct{})

	// No clue why, but this stalls and kills performance on Mac (Mavericks).
	//
	//	if ip, ok := c.nc.(*net.TCPConn); ok {
	//		ip.SetReadBuffer(defaultBufSize)
	//		ip.SetWriteBuffer(2*defaultBufSize)
	//	}

	// Set the Ping timer
	c.setPingTimer()

	// Spin up the read loop.
	go c.readLoop()
}

func (c *client) readLoop() {
	// Grab the connection off the client, it will be cleared on a close.
	// We check for that after the loop, but want to avoid a nil dereference
	c.mu.Lock()
	nc := c.nc
	c.mu.Unlock()

	if nc == nil {
		return
	}

	b := make([]byte, defaultBufSize)

	for {
		n, err := nc.Read(b)
		if err != nil {
			c.closeConnection()
			return
		}
		if err := c.parse(b[:n]); err != nil {
			Log(err, clientConnStr(c.nc), c.cid)
			// Auth was handled inline
			if err != ErrAuthorization {
				c.sendErr("Parser Error")
				c.closeConnection()
			}
			return
		}
		// Check pending clients for flush.
		for cp := range c.pcd {
			// Flush those in the set
			cp.mu.Lock()
			if cp.nc != nil {
				cp.nc.SetWriteDeadline(time.Now().Add(DEFAULT_FLUSH_DEADLINE))
				err := cp.bw.Flush()
				cp.nc.SetWriteDeadline(time.Time{})
				if err != nil {
					Debugf("Error flushing: %v", err)
					cp.mu.Unlock()
					cp.closeConnection()
					cp.mu.Lock()
				}
			}
			cp.mu.Unlock()
			delete(c.pcd, cp)
		}
		// Check to see if we got closed, e.g. slow consumer
		if c.nc == nil {
			return
		}
	}
}

func (c *client) traceMsg(msg []byte) {
	pm := fmt.Sprintf("Processing %s msg: %d", c.typeString(), c.inMsgs)
	opa := []interface{}{pm, string(c.pa.subject), string(c.pa.reply), string(msg[:len(msg)-LEN_CR_LF])}
	Trace(logStr(opa), fmt.Sprintf("c: %d", c.cid))
}

func (c *client) traceOp(op string, arg []byte) {
	if trace == 0 {
		return
	}
	opa := []interface{}{fmt.Sprintf("%s OP", op)}
	if arg != nil {
		opa = append(opa, fmt.Sprintf("%s %s", op, string(arg)))
	}
	Trace(logStr(opa), fmt.Sprintf("c: %d", c.cid))
}

// Process the info message if we are a route.
func (c *client) processRouteInfo(info *Info) {
	if c.route == nil {
		return
	}
	c.route.remoteID = info.ID
	// Check to see if we have this remote already registered.
	// This can happen when both servers have routes to each other.
	if c.srv.isDuplicateRemote(info.ID) {
		Debug("Detected duplicate remote route", info.ID, clientConnStr(c.nc), c.cid)
		c.closeConnection()
	} else {
		s := c.srv
		s.mu.Lock()
		s.remotes[info.ID] = c
		s.mu.Unlock()
		Debug("Registering remote route", info.ID)
	}
}

// Process the information messages from clients and other routes.
func (c *client) processInfo(arg []byte) error {
	info := Info{}
	if err := json.Unmarshal(arg, &info); err != nil {
		return err
	}
	if c.typ == ROUTER {
		c.processRouteInfo(&info)
	}
	return nil
}

func (c *client) processErr(errStr string) {
	Log(errStr, clientConnStr(c.nc), c.cid)
	c.closeConnection()
}

func (c *client) processConnect(arg []byte) error {
	c.traceOp("CONNECT", arg)

	// This will be resolved regardless before we exit this func,
	// so we can just clear it here.
	c.clearAuthTimer()

	if err := json.Unmarshal(arg, &c.opts); err != nil {
		return err
	}

	if c.srv != nil {
		// Check for Auth
		if ok := c.srv.checkAuth(c); !ok {
			c.authViolation()
			return ErrAuthorization
		}
	}

	// Grab connection name of remote route.
	if c.typ == ROUTER && c.route != nil {
		c.route.remoteID = c.opts.Name
	}

	if c.opts.Verbose {
		c.sendOK()
	}
	return nil
}

func (c *client) authViolation() {
	c.sendErr("Authorization Violation")
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
	if c.nc == nil {
		return
	}
	c.mu.Lock()
	c.bw.WriteString("PONG\r\n")
	err := c.bw.Flush()
	if err != nil {
		c.clearConnection()
		Debug("Error on Flush", err, clientConnStr(c.nc), c.cid)
	}
	c.mu.Unlock()
}

func (c *client) processPong() {
	c.traceOp("PONG", nil)
	c.mu.Lock()
	c.pout -= 1
	c.mu.Unlock()
}

func (c *client) processMsgArgs(arg []byte) error {
	if trace > 0 {
		c.traceOp("MSG", arg)
	}

	// Unroll splitArgs to avoid runtime/heap issues
	a := [MAX_MSG_ARGS][]byte{}
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

	c.pa.subject = args[0]
	c.pa.sid = args[1]

	switch len(args) {
	case 3:
		c.pa.reply = nil
		c.pa.szb = args[2]
		c.pa.size = parseSize(args[2])
	case 4:
		c.pa.reply = args[2]
		c.pa.szb = args[3]
		c.pa.size = parseSize(args[3])
	default:
		return fmt.Errorf("processMsgArgs Parse Error: '%s'", arg)
	}
	if c.pa.size < 0 {
		return fmt.Errorf("processMsgArgs Bad or Missing Size: '%s'", arg)
	}
	return nil
}

func (c *client) processPub(arg []byte) error {
	if trace > 0 {
		c.traceOp("PUB", arg)
	}

	// Unroll splitArgs to avoid runtime/heap issues
	a := [MAX_PUB_ARGS][]byte{}
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
	a := [MAX_MSG_ARGS][]byte{}
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
	shouldForward := c.typ != ROUTER && c.srv != nil
	c.mu.Unlock()
	if err != nil {
		c.sendErr("Invalid Subject")
	} else if c.opts.Verbose {
		c.sendOK()
	}
	if shouldForward {
		c.srv.broadcastSubscribe(sub)
	}
	return nil
}

func (c *client) unsubscribe(sub *subscription) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if sub.max > 0 && sub.nm < sub.max {
		Debugf("Deferring actual UNSUB(%s): %d max, %d received\n",
			string(sub.subject), sub.max, sub.nm)
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
		} else {
			// Clear it here to override
			sub.max = 0
		}
		c.unsubscribe(sub)
		if shouldForward := c.typ != ROUTER && c.srv != nil; shouldForward {
			c.srv.broadcastUnSubscribe(sub)
		}
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

// Used to treat maps as efficient set
var needFlush = struct{}{}
var routeSeen = struct{}{}

func (c *client) deliverMsg(sub *subscription, mh, msg []byte) {
	if sub.client == nil {
		return
	}
	client := sub.client
	client.mu.Lock()
	sub.nm++
	// Check if we should auto-unsubscribe.
	if sub.max > 0 {
		// If we are at the exact number, unsubscribe but
		// still process the message in hand, otherwise
		// unsubscribe and drop message on the floor.
		if sub.nm == sub.max {
			defer client.unsubscribe(sub)
		} else if sub.nm > sub.max {
			client.mu.Unlock()
			client.unsubscribe(sub)
			return
		}
	}

	if client.nc == nil {
		client.mu.Unlock()
		return
	}

	// Update statistics

	// The msg includes the CR_LF, so pull back out for accounting.
	msgSize := int64(len(msg) - LEN_CR_LF)

	client.outMsgs++
	client.outBytes += msgSize

	atomic.AddInt64(&c.srv.outMsgs, 1)
	atomic.AddInt64(&c.srv.outBytes, msgSize)

	// Check to see if our writes will cause a flush
	// in the underlying bufio. If so limit time we
	// will wait for flush to complete.

	deadlineSet := false
	if client.bw.Available() < (len(mh) + len(msg) + len(CR_LF)) {
		client.nc.SetWriteDeadline(time.Now().Add(DEFAULT_FLUSH_DEADLINE))
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

	if deadlineSet {
		client.nc.SetWriteDeadline(time.Time{})
	}

	client.mu.Unlock()
	c.pcd[client] = needFlush
	return

writeErr:
	if deadlineSet {
		client.nc.SetWriteDeadline(time.Time{})
	}
	client.mu.Unlock()

	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		Log("Slow Consumer Detected", clientConnStr(client.nc), client.cid)
		client.closeConnection()
	} else {
		Debugf("Error writing msg: %v", err)
	}
}

// processMsg is called to process an inbound msg from a client.
func (c *client) processMsg(msg []byte) {

	// Update statistics

	// The msg includes the CR_LF, so pull back out for accounting.
	msgSize := int64(len(msg) - LEN_CR_LF)

	c.inMsgs++
	c.inBytes += msgSize

	// Snapshot server.
	srv := c.srv

	if srv != nil {
		atomic.AddInt64(&srv.inMsgs, 1)
		atomic.AddInt64(&srv.inBytes, msgSize)
	}

	if trace > 0 {
		c.traceMsg(msg)
	}
	if srv == nil {
		return
	}
	if c.opts.Verbose {
		c.sendOK()
	}

	// Scratch buffer..
	msgh := c.msgb[:len(msgHeadProto)]

	r := srv.sl.Match(c.pa.subject)
	if len(r) <= 0 {
		return
	}

	// msg header
	msgh = append(msgh, c.pa.subject...)
	msgh = append(msgh, ' ')
	si := len(msgh)

	var qmap map[string][]*subscription
	var qsubs []*subscription

	isRoute := c.typ == ROUTER
	var rmap map[string]struct{}

	// If we are a route and we have a queue subscription, deliver direct
	// since they are sent direct via L2 semantics.
	if isRoute {
		if sub := srv.routeSidQueueSubscriber(c.pa.sid); sub != nil {
			mh := c.msgHeader(msgh[:si], sub)
			c.deliverMsg(sub, mh, msg)
			return
		}
	}

	// Loop over all subscriptions that match.

	for _, v := range r {
		sub := v.(*subscription)

		// Process queue group subscriptions by gathering them all up
		// here. We will pick the winners when we are done processing
		// all of the subscriptions.
		if sub.queue != nil {
			// Queue subscriptions handled from routes directly above.
			if isRoute {
				continue
			}
			// FIXME(dlc), this can be more efficient
			if qmap == nil {
				qmap = make(map[string][]*subscription)
			}
			qname := string(sub.queue)
			qsubs = qmap[qname]
			if qsubs == nil {
				qsubs = make([]*subscription, 0, 4)
			}
			qsubs = append(qsubs, sub)
			qmap[qname] = qsubs
			continue
		}

		// Process normal, non-queue group subscriptions.

		// If this is a send to a ROUTER, make sure we only send it
		// once. The other side will handle the appropriate re-processing.
		// Also enforce 1-Hop.
		if sub.client.typ == ROUTER {
			// Skip if sourced from a ROUTER and going to another ROUTER.
			// This is 1-Hop semantics for ROUTERs.
			if isRoute {
				continue
			}
			// Check to see if we have already sent it here.
			if rmap == nil {
				rmap = make(map[string]struct{}, srv.numRoutes())
			}
			if sub.client == nil || sub.client.route == nil ||
				sub.client.route.remoteID == "" {
				Debug("Bad or Missing ROUTER Identity, not processing msg",
					clientConnStr(c.nc), c.cid)
				continue
			}
			if _, ok := rmap[sub.client.route.remoteID]; ok {
				Debug("Ignoring route, already processed", c.cid)
				continue
			}
			rmap[sub.client.route.remoteID] = routeSeen
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

func (c *client) processPingTimer() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ptmr = nil
	// Check if we are ready yet..
	if _, ok := c.nc.(*net.TCPConn); !ok {
		return
	}

	Debug("Client Ping Timer", clientConnStr(c.nc), c.cid)

	// Check for violation
	c.pout += 1
	if c.pout > c.srv.opts.MaxPingsOut {
		Debug("Stale Client Connection - Closing", clientConnStr(c.nc), c.cid)
		if c.bw != nil {
			c.bw.WriteString(fmt.Sprintf("-ERR '%s'\r\n", "Stale Connection"))
			c.bw.Flush()
		}
		c.clearConnection()
		return
	}

	// Send PING
	c.bw.WriteString("PING\r\n")
	err := c.bw.Flush()
	if err != nil {
		Debug("Error on Client Ping Flush", err, clientConnStr(c.nc), c.cid)
		c.clearConnection()
	} else {
		// Reset to fire again if all OK.
		c.setPingTimer()
	}
}

func (c *client) setPingTimer() {
	if c.srv == nil {
		return
	}
	d := c.srv.opts.PingInterval
	c.ptmr = time.AfterFunc(d, func() { c.processPingTimer() })
}

// Lock should be held
func (c *client) clearPingTimer() {
	if c.ptmr == nil {
		return
	}
	c.ptmr.Stop()
	c.ptmr = nil
}

// Lock should be held
func (c *client) setAuthTimer(d time.Duration) {
	c.atmr = time.AfterFunc(d, func() { c.authViolation() })
}

// Lock should be held
func (c *client) clearAuthTimer() {
	if c.atmr == nil {
		return
	}
	c.atmr.Stop()
	c.atmr = nil
}

func (c *client) isAuthTimerSet() bool {
	c.mu.Lock()
	isSet := c.atmr != nil
	c.mu.Unlock()
	return isSet
}

// Lock should be held
func (c *client) clearConnection() {
	if c.nc == nil {
		return
	}
	c.bw.Flush()
	c.nc.Close()
	c.nc = nil
}

func (c *client) typeString() string {
	switch c.typ {
	case CLIENT:
		return "Client"
	case ROUTER:
		return "Router"
	}
	return "Unknown Type"
}

func (c *client) closeConnection() {
	c.mu.Lock()
	if c.nc == nil {
		c.mu.Unlock()
		return
	}

	// FIXME(dlc) - This creates garbage for no reason.
	dbgString := fmt.Sprintf("%s connection closed", c.typeString())
	Debug(dbgString, clientConnStr(c.nc), c.cid)

	c.clearAuthTimer()
	c.clearPingTimer()
	c.clearConnection()

	// Snapshot for use.
	subs := c.subs.All()
	srv := c.srv

	c.mu.Unlock()

	if srv != nil {
		// Unregister
		srv.removeClient(c)

		// Remove clients subscriptions.
		for _, s := range subs {
			if sub, ok := s.(*subscription); ok {
				srv.sl.Remove(sub.subject, sub)
			}
		}
	}

	// Check for a solicited route. If it was, start up a reconnect unless
	// we are already connected to the other end.
	if c.isSolicitedRoute() {
		rid := c.route.remoteID
		if rid != "" && c.srv.remotes[rid] != nil {
			Debug("Not attempting reconnect for solicited route, already connected.", rid)
			return
		} else {
			Debug("Attempting reconnect for solicited route", c.cid)
			go srv.reConnectToRoute(c.route.url)
		}
	}
}
