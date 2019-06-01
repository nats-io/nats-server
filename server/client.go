// Copyright 2012-2019 The NATS Authors
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
	"fmt"
	"io"
	"math/rand"
	"net"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/jwt"
)

// Type of client connection.
const (
	// CLIENT is an end user.
	CLIENT = iota
	// ROUTER is another router in the cluster.
	ROUTER
	// GATEWAY is a link between 2 clusters.
	GATEWAY
	// SYSTEM is an internal system client.
	SYSTEM
	// LEAF is for leaf node connections.
	LEAF
)

const (
	// ClientProtoZero is the original Client protocol from 2009.
	// http://nats.io/documentation/internals/nats-protocol/
	ClientProtoZero = iota
	// ClientProtoInfo signals a client can receive more then the original INFO block.
	// This can be used to update clients on other cluster members, etc.
	ClientProtoInfo
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const (
	// Scratch buffer size for the processMsg() calls.
	msgScratchSize  = 1024
	msgHeadProto    = "RMSG "
	msgHeadProtoLen = len(msgHeadProto)

	// For controlling dynamic buffer sizes.
	startBufSize    = 512   // For INFO/CONNECT block
	minBufSize      = 64    // Smallest to shrink to for PING/PONG
	maxBufSize      = 65536 // 64k
	shortsToShrink  = 2     // Trigger to shrink dynamic buffers
	maxFlushPending = 10    // Max fsps to have in order to wait for writeLoop
	readLoopReport  = 2 * time.Second
)

var readLoopReportThreshold = readLoopReport

// Represent client booleans with a bitmask
type clientFlag byte

// Some client state represented as flags
const (
	connectReceived   clientFlag = 1 << iota // The CONNECT proto has been received
	infoReceived                             // The INFO protocol has been received
	firstPongSent                            // The first PONG has been sent
	handshakeComplete                        // For TLS clients, indicate that the handshake is complete
	clearConnection                          // Marks that clearConnection has already been called.
	flushOutbound                            // Marks client as having a flushOutbound call in progress.
	noReconnect                              // Indicate that on close, this connection should not attempt a reconnect
)

// set the flag (would be equivalent to set the boolean to true)
func (cf *clientFlag) set(c clientFlag) {
	*cf |= c
}

// clear the flag (would be equivalent to set the boolean to false)
func (cf *clientFlag) clear(c clientFlag) {
	*cf &= ^c
}

// isSet returns true if the flag is set, false otherwise
func (cf clientFlag) isSet(c clientFlag) bool {
	return cf&c != 0
}

// setIfNotSet will set the flag `c` only if that flag was not already
// set and return true to indicate that the flag has been set. Returns
// false otherwise.
func (cf *clientFlag) setIfNotSet(c clientFlag) bool {
	if *cf&c == 0 {
		*cf |= c
		return true
	}
	return false
}

// ClosedState is the reason client was closed. This will
// be passed into calls to clearConnection, but will only
// be stored in ConnInfo for monitoring.
type ClosedState int

const (
	ClientClosed = ClosedState(iota + 1)
	AuthenticationTimeout
	AuthenticationViolation
	TLSHandshakeError
	SlowConsumerPendingBytes
	SlowConsumerWriteDeadline
	WriteError
	ReadError
	ParseError
	StaleConnection
	ProtocolViolation
	BadClientProtocolVersion
	WrongPort
	MaxAccountConnectionsExceeded
	MaxConnectionsExceeded
	MaxPayloadExceeded
	MaxControlLineExceeded
	MaxSubscriptionsExceeded
	DuplicateRoute
	RouteRemoved
	ServerShutdown
	AuthenticationExpired
	WrongGateway
	MissingAccount
)

// Some flags passed to processMsgResultsEx
const pmrNoFlag int = 0
const (
	pmrCollectQueueNames int = 1 << iota
	pmrTreatGatewayAsClient
)

type client struct {
	// Here first because of use of atomics, and memory alignment.
	stats
	mpay   int32
	msubs  int32
	mcl    int32
	mu     sync.Mutex
	kind   int
	cid    uint64
	opts   clientOpts
	start  time.Time
	nonce  []byte
	nc     net.Conn
	ncs    string
	out    outbound
	srv    *Server
	acc    *Account
	user   *NkeyUser
	host   string
	port   uint16
	subs   map[string]*subscription
	perms  *permissions
	mperms *msgDeny
	darray []string
	in     readCache
	pcd    map[*client]struct{}
	atmr   *time.Timer
	ping   pinfo
	msgb   [msgScratchSize]byte
	last   time.Time
	parseState

	rtt      time.Duration
	rttStart time.Time

	route *route
	gw    *gateway
	leaf  *leaf

	debug bool
	trace bool
	echo  bool

	flags clientFlag // Compact booleans into a single field. Size will be increased when needed.
}

// Struct for PING initiation from the server.
type pinfo struct {
	tmr *time.Timer
	out int
}

// outbound holds pending data for a socket.
type outbound struct {
	p   []byte        // Primary write buffer
	s   []byte        // Secondary for use post flush
	nb  net.Buffers   // net.Buffers for writev IO
	sz  int32         // limit size per []byte, uses variable BufSize constants, start, min, max.
	sws int32         // Number of short writes, used for dynamic resizing.
	pb  int32         // Total pending/queued bytes.
	pm  int32         // Total pending/queued messages.
	sg  *sync.Cond    // Flusher conditional for signaling to writeLoop.
	wdl time.Duration // Snapshot of write deadline.
	mp  int32         // Snapshot of max pending for client.
	fsp int32         // Flush signals that are pending per producer from readLoop's pcd.
	lft time.Duration // Last flush time for Write.
	lwb int32         // Last byte size of Write.
	stc chan struct{} // Stall chan we create to slow down producers on overrun, e.g. fan-in.
	sgw bool          // Indicate flusher is waiting on condition wait.
}

type perm struct {
	allow *Sublist
	deny  *Sublist
}
type permissions struct {
	sub    perm
	pub    perm
	pcache map[string]bool
}

// msgDeny is used when a user permission for subscriptions has a deny
// clause but a subscription could be made that is of broader scope.
// e.g. deny = "foo", but user subscribes to "*". That subscription should
// succeed but no message sent on foo should be delivered.
type msgDeny struct {
	deny   *Sublist
	dcache map[string]bool
}

// routeTarget collects information regarding routes and queue groups for
// sending information to a remote.
type routeTarget struct {
	sub *subscription
	qs  []byte
	_qs [32]byte
}

const (
	maxResultCacheSize   = 512
	maxDenyPermCacheSize = 256
	maxPermCacheSize     = 128
	pruneSize            = 32
	routeTargetInit      = 8
)

// Used in readloop to cache hot subject lookups and group statistics.
type readCache struct {
	// These are for clients who are bound to a single account.
	genid   uint64
	results map[string]*SublistResult

	// This is for routes and gateways to have their own L1 as well that is account aware.
	pacache map[string]*perAccountCache

	// This is for when we deliver messages across a route. We use this structure
	// to make sure to only send one message and properly scope to queues as needed.
	rts []routeTarget

	prand *rand.Rand

	// These are all temporary totals for an invocation of a read in readloop.
	msgs  int32
	bytes int32
	subs  int32

	rsz int32 // Read buffer size
	srs int32 // Short reads, used for dynamic buffer resizing.
}

const (
	maxPerAccountCacheSize   = 32768
	prunePerAccountCacheSize = 512
)

// perAccountCache is for L1 semantics for inbound messages from a route or gateway to mimic the performance of clients.
type perAccountCache struct {
	acc     *Account
	results *SublistResult
	genid   uint64
}

func (c *client) String() (id string) {
	return c.ncs
}

func (c *client) GetOpts() *clientOpts {
	return &c.opts
}

// GetTLSConnectionState returns the TLS ConnectionState if TLS is enabled, nil
// otherwise. Implements the ClientAuth interface.
func (c *client) GetTLSConnectionState() *tls.ConnectionState {
	tc, ok := c.nc.(*tls.Conn)
	if !ok {
		return nil
	}
	state := tc.ConnectionState()
	return &state
}

// This is the main subscription struct that indicates
// interest in published messages.
// FIXME(dlc) - This is getting bloated for normal subs, need
// to optionally have an opts section for non-normal stuff.
type subscription struct {
	client  *client
	im      *streamImport   // This is for import stream support.
	shadow  []*subscription // This is to track shadowed accounts.
	subject []byte
	queue   []byte
	sid     []byte
	nm      int64
	max     int64
	qw      int32
}

type clientOpts struct {
	Echo          bool   `json:"echo"`
	Verbose       bool   `json:"verbose"`
	Pedantic      bool   `json:"pedantic"`
	TLSRequired   bool   `json:"tls_required"`
	Nkey          string `json:"nkey,omitempty"`
	JWT           string `json:"jwt,omitempty"`
	Sig           string `json:"sig,omitempty"`
	Authorization string `json:"auth_token,omitempty"`
	Username      string `json:"user,omitempty"`
	Password      string `json:"pass,omitempty"`
	Name          string `json:"name"`
	Lang          string `json:"lang"`
	Version       string `json:"version"`
	Protocol      int    `json:"protocol"`
	Account       string `json:"account,omitempty"`
	AccountNew    bool   `json:"new_account,omitempty"`

	// Routes only
	Import *SubjectPermission `json:"import,omitempty"`
	Export *SubjectPermission `json:"export,omitempty"`
}

var defaultOpts = clientOpts{Verbose: true, Pedantic: true, Echo: true}
var internalOpts = clientOpts{Verbose: false, Pedantic: false, Echo: false}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Lock should be held
func (c *client) initClient() {
	s := c.srv
	c.cid = atomic.AddUint64(&s.gcid, 1)

	// Outbound data structure setup
	c.out.sz = startBufSize
	c.out.sg = sync.NewCond(&c.mu)
	opts := s.getOpts()
	// Snapshots to avoid mutex access in fast paths.
	c.out.wdl = opts.WriteDeadline
	c.out.mp = int32(opts.MaxPending)

	c.subs = make(map[string]*subscription)
	c.echo = true

	c.debug = (atomic.LoadInt32(&c.srv.logging.debug) != 0)
	c.trace = (atomic.LoadInt32(&c.srv.logging.trace) != 0)

	// This is a scratch buffer used for processMsg()
	// The msg header starts with "RMSG ", which can be used
	// for both local and routes.
	// in bytes that is [82 77 83 71 32].
	c.msgb = [msgScratchSize]byte{82, 77, 83, 71, 32}

	// This is to track pending clients that have data to be flushed
	// after we process inbound msgs from our own connection.
	c.pcd = make(map[*client]struct{})

	// snapshot the string version of the connection
	var conn string
	if ip, ok := c.nc.(*net.TCPConn); ok {
		addr := ip.RemoteAddr().(*net.TCPAddr)
		c.host = addr.IP.String()
		c.port = uint16(addr.Port)
		conn = fmt.Sprintf("%s:%d", addr.IP, addr.Port)
	}

	switch c.kind {
	case CLIENT:
		c.ncs = fmt.Sprintf("%s - cid:%d", conn, c.cid)
	case ROUTER:
		c.ncs = fmt.Sprintf("%s - rid:%d", conn, c.cid)
	case GATEWAY:
		c.ncs = fmt.Sprintf("%s - gid:%d", conn, c.cid)
	case LEAF:
		c.ncs = fmt.Sprintf("%s - lid:%d", conn, c.cid)
	case SYSTEM:
		c.ncs = "SYSTEM"
	}
}

// RemoteAddress expose the Address of the client connection,
// nil when not connected or unknown
func (c *client) RemoteAddress() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nc == nil {
		return nil
	}

	return c.nc.RemoteAddr()
}

// Helper function to report errors.
func (c *client) reportErrRegisterAccount(acc *Account, err error) {
	if err == ErrTooManyAccountConnections {
		c.maxAccountConnExceeded()
		return
	}
	c.Errorf("Problem registering with account [%s]", acc.Name)
	c.sendErr("Failed Account Registration")
}

// registerWithAccount will register the given user with a specific
// account. This will change the subject namespace.
func (c *client) registerWithAccount(acc *Account) error {
	if acc == nil || acc.sl == nil {
		return ErrBadAccount
	}
	// If we were previously registered, usually to $G, do accounting here to remove.
	if c.acc != nil {
		if prev := c.acc.removeClient(c); prev == 1 && c.srv != nil {
			c.srv.decActiveAccounts()
		}
	}

	c.mu.Lock()
	kind := c.kind
	srv := c.srv
	c.acc = acc
	c.applyAccountLimits()
	c.mu.Unlock()

	// Check if we have a max connections violation
	if kind == CLIENT && acc.MaxTotalConnectionsReached() {
		return ErrTooManyAccountConnections
	} else if kind == LEAF && acc.MaxTotalLeafNodesReached() {
		return ErrTooManyAccountConnections
	}

	// Add in new one.
	if prev := acc.addClient(c); prev == 0 && srv != nil {
		srv.incActiveAccounts()
	}

	return nil
}

// Helper to determine if we have met or exceeded max subs.
func (c *client) subsAtLimit() bool {
	return c.msubs != jwt.NoLimit && len(c.subs) >= int(c.msubs)
}

// Apply account limits
// Lock is held on entry.
// FIXME(dlc) - Should server be able to override here?
func (c *client) applyAccountLimits() {
	if c.acc == nil || (c.kind != CLIENT && c.kind != LEAF) {
		return
	}

	// Set here, will need to fo checks for NoLimit.
	if c.acc.msubs != jwt.NoLimit {
		c.msubs = c.acc.msubs
	}
	if c.acc.mpay != jwt.NoLimit {
		c.mpay = c.acc.mpay
	}

	opts := c.srv.getOpts()

	// We check here if the server has an option set that is lower than the account limit.
	if c.mpay != jwt.NoLimit && opts.MaxPayload != 0 && int32(opts.MaxPayload) < c.acc.mpay {
		c.Errorf("Max Payload set to %d from server config which overrides %d from account claims", opts.MaxPayload, c.acc.mpay)
		c.mpay = int32(opts.MaxPayload)
	}

	// We check here if the server has an option set that is lower than the account limit.
	if c.msubs != jwt.NoLimit && opts.MaxSubs != 0 && opts.MaxSubs < int(c.acc.msubs) {
		c.Errorf("Max Subscriptions set to %d from server config which overrides %d from account claims", opts.MaxSubs, c.acc.msubs)
		c.msubs = int32(opts.MaxSubs)
	}

	if c.subsAtLimit() {
		go func() {
			c.maxSubsExceeded()
			time.Sleep(20 * time.Millisecond)
			c.closeConnection(MaxSubscriptionsExceeded)
		}()
	}
}

// RegisterUser allows auth to call back into a new client
// with the authenticated user. This is used to map
// any permissions into the client and setup accounts.
func (c *client) RegisterUser(user *User) {
	// Register with proper account and sublist.
	if user.Account != nil {
		if err := c.registerWithAccount(user.Account); err != nil {
			c.reportErrRegisterAccount(user.Account, err)
			return
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Assign permissions.
	if user.Permissions == nil {
		// Reset perms to nil in case client previously had them.
		c.perms = nil
		c.mperms = nil
		return
	}
	c.setPermissions(user.Permissions)
}

// RegisterNkey allows auth to call back into a new nkey
// client with the authenticated user. This is used to map
// any permissions into the client and setup accounts.
func (c *client) RegisterNkeyUser(user *NkeyUser) error {
	// Register with proper account and sublist.
	if user.Account != nil {
		if err := c.registerWithAccount(user.Account); err != nil {
			c.reportErrRegisterAccount(user.Account, err)
			return err
		}
	}

	c.mu.Lock()
	c.user = user
	// Assign permissions.
	if user.Permissions == nil {
		// Reset perms to nil in case client previously had them.
		c.perms = nil
		c.mperms = nil
	} else {
		c.setPermissions(user.Permissions)
	}
	c.mu.Unlock()
	return nil
}

// Initializes client.perms structure.
// Lock is held on entry.
func (c *client) setPermissions(perms *Permissions) {
	if perms == nil {
		return
	}
	c.perms = &permissions{}
	c.perms.pcache = make(map[string]bool)

	// Loop over publish permissions
	if perms.Publish != nil {
		if len(perms.Publish.Allow) > 0 {
			c.perms.pub.allow = NewSublist()
		}
		for _, pubSubject := range perms.Publish.Allow {
			sub := &subscription{subject: []byte(pubSubject)}
			c.perms.pub.allow.Insert(sub)
		}
		if len(perms.Publish.Deny) > 0 {
			c.perms.pub.deny = NewSublist()
		}
		for _, pubSubject := range perms.Publish.Deny {
			sub := &subscription{subject: []byte(pubSubject)}
			c.perms.pub.deny.Insert(sub)
		}
	}

	// Loop over subscribe permissions
	if perms.Subscribe != nil {
		if len(perms.Subscribe.Allow) > 0 {
			c.perms.sub.allow = NewSublist()
		}
		for _, subSubject := range perms.Subscribe.Allow {
			sub := &subscription{subject: []byte(subSubject)}
			c.perms.sub.allow.Insert(sub)
		}
		if len(perms.Subscribe.Deny) > 0 {
			c.perms.sub.deny = NewSublist()
			// Also hold onto this array for later.
			c.darray = perms.Subscribe.Deny
		}
		for _, subSubject := range perms.Subscribe.Deny {
			sub := &subscription{subject: []byte(subSubject)}
			c.perms.sub.deny.Insert(sub)
		}
	}
}

// Check to see if we have an expiration for the user JWT via base claims.
// FIXME(dlc) - Clear on connect with new JWT.
func (c *client) checkExpiration(claims *jwt.ClaimsData) {
	if claims.Expires == 0 {
		return
	}
	tn := time.Now().Unix()
	if claims.Expires < tn {
		return
	}
	expiresAt := time.Duration(claims.Expires - tn)
	c.setExpirationTimer(expiresAt * time.Second)
}

// This will load up the deny structure used for filtering delivered
// messages based on a deny clause for subscriptions.
// Lock should be held.
func (c *client) loadMsgDenyFilter() {
	c.mperms = &msgDeny{NewSublist(), make(map[string]bool)}
	for _, sub := range c.darray {
		c.mperms.deny.Insert(&subscription{subject: []byte(sub)})
	}
}

// writeLoop is the main socket write functionality.
// Runs in its own Go routine.
func (c *client) writeLoop() {
	defer c.srv.grWG.Done()

	// Used to check that we did flush from last wake up.
	waitOk := true

	// Main loop. Will wait to be signaled and then will use
	// buffered outbound structure for efficient writev to the underlying socket.
	for {
		c.mu.Lock()
		owtf := c.out.fsp > 0 && c.out.pb < maxBufSize && c.out.fsp < maxFlushPending
		if waitOk && (c.out.pb == 0 || owtf) && !c.flags.isSet(clearConnection) {
			// Wait on pending data.
			c.out.sgw = true
			c.out.sg.Wait()
			c.out.sgw = false
		}
		// Flush data
		// TODO(dlc) - This could spin if another go routine in flushOutbound waiting on a slow IO.
		waitOk = c.flushOutbound()
		isClosed := c.flags.isSet(clearConnection)
		c.mu.Unlock()

		if isClosed {
			return
		}
	}
}

// flushClients will make sure to flush any clients we may have
// sent to during processing. We pass in a budget as a time.Duration
// for how much time to spend in place flushing for this client. This
// will normally be called in the readLoop of the client who sent the
// message that now is being delivered.
func (c *client) flushClients(budget time.Duration) time.Time {
	last := time.Now()
	// Check pending clients for flush.
	for cp := range c.pcd {
		// TODO(dlc) - Wonder if it makes more sense to create a new map?
		delete(c.pcd, cp)

		// Queue up a flush for those in the set
		cp.mu.Lock()
		// Update last activity for message delivery
		cp.last = last
		// Remove ourselves from the pending list.
		cp.out.fsp--

		// Just ignore if this was closed.
		if cp.flags.isSet(clearConnection) {
			cp.mu.Unlock()
			continue
		}

		if budget > 0 && cp.flushOutbound() {
			budget -= cp.out.lft
		} else {
			cp.flushSignal()
		}

		cp.mu.Unlock()
	}
	return last
}

// readLoop is the main socket read functionality.
// Runs in its own Go routine.
func (c *client) readLoop() {
	// Grab the connection off the client, it will be cleared on a close.
	// We check for that after the loop, but want to avoid a nil dereference
	c.mu.Lock()
	nc := c.nc
	s := c.srv
	c.in.rsz = startBufSize
	// Snapshot max control line since currently can not be changed on reload and we
	// were checking it on each call to parse. If this changes and we allow MaxControlLine
	// to be reloaded without restart, this code will need to change.
	c.mcl = MAX_CONTROL_LINE_SIZE
	if s != nil {
		if opts := s.getOpts(); opts != nil {
			c.mcl = int32(opts.MaxControlLine)
		}
	}
	defer s.grWG.Done()
	c.mu.Unlock()

	if nc == nil {
		return
	}

	// Start read buffer.

	b := make([]byte, c.in.rsz)

	for {
		n, err := nc.Read(b)
		if err != nil {
			if err == io.EOF {
				c.closeConnection(ClientClosed)
			} else {
				c.closeConnection(ReadError)
			}
			return
		}
		start := time.Now()

		// Clear inbound stats cache
		c.in.msgs = 0
		c.in.bytes = 0
		c.in.subs = 0

		// Main call into parser for inbound data. This will generate callouts
		// to process messages, etc.
		if err := c.parse(b[:n]); err != nil {
			if dur := time.Since(start); dur >= readLoopReportThreshold {
				c.Warnf("Readloop processing time: %v", dur)
			}
			// handled inline
			if err != ErrMaxPayload && err != ErrAuthentication {
				c.Errorf("%s", err.Error())
				c.closeConnection(ProtocolViolation)
			}
			return
		}

		// Updates stats for client and server that were collected
		// from parsing through the buffer.
		if c.in.msgs > 0 {
			atomic.AddInt64(&c.inMsgs, int64(c.in.msgs))
			atomic.AddInt64(&c.inBytes, int64(c.in.bytes))
			atomic.AddInt64(&s.inMsgs, int64(c.in.msgs))
			atomic.AddInt64(&s.inBytes, int64(c.in.bytes))
		}

		// Budget to spend in place flushing outbound data.
		// Client will be checked on several fronts to see
		// if applicable. Routes and Gateways will never
		// spend time flushing outbound in place.
		var budget time.Duration
		if c.kind == CLIENT {
			budget = time.Millisecond
		}

		// Flush, or signal to writeLoop to flush to socket.
		last := c.flushClients(budget)

		// Update activity, check read buffer size.
		c.mu.Lock()
		nc := c.nc

		// Activity based on interest changes or data/msgs.
		if c.in.msgs > 0 || c.in.subs > 0 {
			c.last = last
		}

		if n >= cap(b) {
			c.in.srs = 0
		} else if n < cap(b)/2 { // divide by 2 b/c we want less than what we would shrink to.
			c.in.srs++
		}

		// Update read buffer size as/if needed.
		if n >= cap(b) && cap(b) < maxBufSize {
			// Grow
			c.in.rsz = int32(cap(b) * 2)
			b = make([]byte, c.in.rsz)
		} else if n < cap(b) && cap(b) > minBufSize && c.in.srs > shortsToShrink {
			// Shrink, for now don't accelerate, ping/pong will eventually sort it out.
			c.in.rsz = int32(cap(b) / 2)
			b = make([]byte, c.in.rsz)
		}
		c.mu.Unlock()

		if dur := time.Since(start); dur >= readLoopReportThreshold {
			c.Warnf("Readloop processing time: %v", dur)
		}

		// Check to see if we got closed, e.g. slow consumer
		if nc == nil {
			return
		}
	}
}

// collapsePtoNB will place primary onto nb buffer as needed in prep for WriteTo.
// This will return a copy on purpose.
func (c *client) collapsePtoNB() net.Buffers {
	if c.out.p != nil {
		p := c.out.p
		c.out.p = nil
		return append(c.out.nb, p)
	}
	return c.out.nb
}

// This will handle the fixup needed on a partial write.
// Assume pending has been already calculated correctly.
func (c *client) handlePartialWrite(pnb net.Buffers) {
	nb := c.collapsePtoNB()
	// The partial needs to be first, so append nb to pnb
	c.out.nb = append(pnb, nb...)
}

// flushOutbound will flush outbound buffer to a client.
// Will return true if data was attempted to be written.
// Lock must be held
func (c *client) flushOutbound() bool {
	if c.flags.isSet(flushOutbound) {
		// Another go-routine has set this and is either
		// doing the write or waiting to re-acquire the
		// lock post write. Release lock to give it a
		// chance to complete.
		c.mu.Unlock()
		runtime.Gosched()
		c.mu.Lock()
		return false
	}
	c.flags.set(flushOutbound)
	defer c.flags.clear(flushOutbound)

	// Check for nothing to do.
	if c.nc == nil || c.srv == nil || c.out.pb == 0 {
		return true // true because no need to queue a signal.
	}

	// Snapshot opts
	srv := c.srv

	// Place primary on nb, assign primary to secondary, nil out nb and secondary.
	nb := c.collapsePtoNB()
	c.out.p, c.out.nb, c.out.s = c.out.s, nil, nil

	// For selecting primary replacement.
	cnb := nb

	// In case it goes away after releasing the lock.
	nc := c.nc
	attempted := c.out.pb
	apm := c.out.pm

	// Do NOT hold lock during actual IO.
	c.mu.Unlock()

	// flush here
	now := time.Now()
	// FIXME(dlc) - writev will do multiple IOs past 1024 on
	// most platforms, need to account for that with deadline?
	nc.SetWriteDeadline(now.Add(c.out.wdl))

	// Actual write to the socket.
	n, err := nb.WriteTo(nc)
	nc.SetWriteDeadline(time.Time{})
	lft := time.Since(now)

	// Re-acquire client lock.
	c.mu.Lock()

	// Update flush time statistics.
	c.out.lft = lft
	c.out.lwb = int32(n)

	// Subtract from pending bytes and messages.
	c.out.pb -= c.out.lwb
	c.out.pm -= apm // FIXME(dlc) - this will not be totally accurate on partials.

	// Check for partial writes
	// TODO(dlc) - zero write with no error will cause lost message and the writeloop to spin.
	if c.out.lwb != attempted && n > 0 {
		c.handlePartialWrite(nb)
	} else if c.out.lwb >= c.out.sz {
		c.out.sws = 0
	}

	if err != nil {
		if n == 0 {
			c.out.pb -= attempted
		}
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			// report slow consumer error
			sce := true
			if tlsConn, ok := c.nc.(*tls.Conn); ok {
				if !tlsConn.ConnectionState().HandshakeComplete {
					// Likely a TLSTimeout error instead...
					c.clearConnection(TLSHandshakeError)
					// Would need to coordinate with tlstimeout()
					// to avoid double logging, so skip logging
					// here, and don't report a slow consumer error.
					sce = false
				}
			} else if !c.flags.isSet(connectReceived) {
				// Under some conditions, a client may hit a slow consumer write deadline
				// before the authorization or TLS handshake timeout. If that is the case,
				// then we handle as slow consumer though we do not increase the counter
				// as that can be misleading.
				c.clearConnection(SlowConsumerWriteDeadline)
				sce = false
			}
			if sce {
				atomic.AddInt64(&srv.slowConsumers, 1)
				c.clearConnection(SlowConsumerWriteDeadline)
				c.Noticef("Slow Consumer Detected: WriteDeadline of %v exceeded with %d chunks of %d total bytes.",
					c.out.wdl, len(cnb), attempted)
			}
		} else {
			c.clearConnection(WriteError)
			c.Debugf("Error flushing: %v", err)
		}
		return true
	}

	// Adjust based on what we wrote plus any pending.
	pt := c.out.lwb + c.out.pb

	// Adjust sz as needed downward, keeping power of 2.
	// We do this at a slower rate.
	if pt < c.out.sz && c.out.sz > minBufSize {
		c.out.sws++
		if c.out.sws > shortsToShrink {
			c.out.sz >>= 1
		}
	}
	// Adjust sz as needed upward, keeping power of 2.
	if pt > c.out.sz && c.out.sz < maxBufSize {
		c.out.sz <<= 1
	}

	// Check to see if we can reuse buffers.
	if len(cnb) > 0 {
		oldp := cnb[0][:0]
		if cap(oldp) >= int(c.out.sz) {
			// Replace primary or secondary if they are nil, reusing same buffer.
			if c.out.p == nil {
				c.out.p = oldp
			} else if c.out.s == nil || cap(c.out.s) < int(c.out.sz) {
				c.out.s = oldp
			}
		}
	}

	// Check that if there is still data to send and writeLoop is in wait,
	// then we need to signal.
	if c.out.pb > 0 {
		c.flushSignal()
	}

	// Check if we have a stalled gate and if so and we are recovering release
	// any stalled producers. Only kind==CLIENT will stall.
	if c.out.stc != nil && (c.out.lwb == attempted || c.out.pb < c.out.mp/2) {
		close(c.out.stc)
		c.out.stc = nil
	}

	return true
}

// flushSignal will use server to queue the flush IO operation to a pool of flushers.
// Lock must be held.
func (c *client) flushSignal() bool {
	if c.out.sgw {
		c.out.sg.Signal()
		return true
	}
	return false
}

func (c *client) traceMsg(msg []byte) {
	if !c.trace {
		return
	}
	// FIXME(dlc), allow limits to printable payload.
	c.Tracef("<<- MSG_PAYLOAD: [%q]", msg[:len(msg)-LEN_CR_LF])
}

func (c *client) traceInOp(op string, arg []byte) {
	c.traceOp("<<- %s", op, arg)
}

func (c *client) traceOutOp(op string, arg []byte) {
	c.traceOp("->> %s", op, arg)
}

func (c *client) traceOp(format, op string, arg []byte) {
	if !c.trace {
		return
	}

	opa := []interface{}{}
	if op != "" {
		opa = append(opa, op)
	}
	if arg != nil {
		opa = append(opa, string(arg))
	}
	c.Tracef(format, opa)
}

// Process the information messages from Clients and other Routes.
func (c *client) processInfo(arg []byte) error {
	info := Info{}
	if err := json.Unmarshal(arg, &info); err != nil {
		return err
	}
	switch c.kind {
	case ROUTER:
		c.processRouteInfo(&info)
	case GATEWAY:
		c.processGatewayInfo(&info)
	case LEAF:
		c.processLeafnodeInfo(&info)
	}
	return nil
}

func (c *client) processErr(errStr string) {
	switch c.kind {
	case CLIENT:
		c.Errorf("Client Error %s", errStr)
	case ROUTER:
		c.Errorf("Route Error %s", errStr)
	case GATEWAY:
		c.Errorf("Gateway Error %s", errStr)
	}
	c.closeConnection(ParseError)
}

// Password pattern matcher.
var passPat = regexp.MustCompile(`"?\s*pass\S*?"?\s*[:=]\s*"?(([^",\r\n}])*)`)

// removePassFromTrace removes any notion of passwords from trace
// messages for logging.
func removePassFromTrace(arg []byte) []byte {
	if !bytes.Contains(arg, []byte(`pass`)) {
		return arg
	}
	// Take a copy of the connect proto just for the trace message.
	var _arg [4096]byte
	buf := append(_arg[:0], arg...)

	m := passPat.FindAllSubmatchIndex(buf, -1)
	if len(m) == 0 {
		return arg
	}

	redactedPass := []byte("[REDACTED]")
	for _, i := range m {
		if len(i) < 4 {
			continue
		}
		start := i[2]
		end := i[3]

		// Replace password substring.
		buf = append(buf[:start], append(redactedPass, buf[end:]...)...)
		break
	}
	return buf
}

func (c *client) processConnect(arg []byte) error {
	if c.trace {
		c.traceInOp("CONNECT", removePassFromTrace(arg))
	}

	c.mu.Lock()
	// If we can't stop the timer because the callback is in progress...
	if !c.clearAuthTimer() {
		// wait for it to finish and handle sending the failure back to
		// the client.
		for c.nc != nil {
			c.mu.Unlock()
			time.Sleep(25 * time.Millisecond)
			c.mu.Lock()
		}
		c.mu.Unlock()
		return nil
	}
	c.last = time.Now()

	kind := c.kind
	srv := c.srv

	// Moved unmarshalling of clients' Options under the lock.
	// The client has already been added to the server map, so it is possible
	// that other routines lookup the client, and access its options under
	// the client's lock, so unmarshalling the options outside of the lock
	// would cause data RACEs.
	if err := json.Unmarshal(arg, &c.opts); err != nil {
		c.mu.Unlock()
		return err
	}
	// Indicate that the CONNECT protocol has been received, and that the
	// server now knows which protocol this client supports.
	c.flags.set(connectReceived)
	// Capture these under lock
	c.echo = c.opts.Echo
	proto := c.opts.Protocol
	verbose := c.opts.Verbose
	lang := c.opts.Lang
	account := c.opts.Account
	accountNew := c.opts.AccountNew
	ujwt := c.opts.JWT
	c.mu.Unlock()

	if srv != nil {
		// Applicable to clients only:
		// As soon as c.opts is unmarshalled and if the proto is at
		// least ClientProtoInfo, we need to increment the following counter.
		// This is decremented when client is removed from the server's
		// clients map.
		if kind == CLIENT && proto >= ClientProtoInfo {
			srv.mu.Lock()
			srv.cproto++
			srv.mu.Unlock()
		}

		// Check for Auth
		if ok := srv.checkAuthentication(c); !ok {
			// We may fail here because we reached max limits on an account.
			if ujwt != "" {
				c.mu.Lock()
				acc := c.acc
				c.mu.Unlock()
				if acc != nil && acc != srv.gacc {
					return ErrTooManyAccountConnections
				}
			}
			c.authViolation()
			return ErrAuthentication
		}

		// Check for Account designation, this section should be only used when there is not a jwt.
		if account != "" {
			var acc *Account
			var wasNew bool
			var err error
			if !srv.NewAccountsAllowed() {
				acc, err = srv.LookupAccount(account)
				if err != nil {
					c.Errorf(err.Error())
					c.sendErr(ErrMissingAccount.Error())
					return err
				} else if accountNew && acc != nil {
					c.sendErrAndErr(ErrAccountExists.Error())
					return ErrAccountExists
				}
			} else {
				// We can create this one on the fly.
				acc, wasNew = srv.LookupOrRegisterAccount(account)
				if accountNew && !wasNew {
					c.sendErrAndErr(ErrAccountExists.Error())
					return ErrAccountExists
				}
			}
			// If we are here we can register ourselves with the new account.
			if err := c.registerWithAccount(acc); err != nil {
				c.reportErrRegisterAccount(acc, err)
				return ErrBadAccount
			}
		} else if c.acc == nil {
			// By default register with the global account.
			c.registerWithAccount(srv.gacc)
		}

	}

	switch kind {
	case CLIENT:
		// Check client protocol request if it exists.
		if proto < ClientProtoZero || proto > ClientProtoInfo {
			c.sendErr(ErrBadClientProtocol.Error())
			c.closeConnection(BadClientProtocolVersion)
			return ErrBadClientProtocol
		}
		if verbose {
			c.sendOK()
		}
	case ROUTER:
		// Delegate the rest of processing to the route
		return c.processRouteConnect(srv, arg, lang)
	case GATEWAY:
		// Delegate the rest of processing to the gateway
		return c.processGatewayConnect(arg)
	case LEAF:
		// Delegate the rest of processing to the leaf node
		return c.processLeafNodeConnect(srv, arg, lang)
	}
	return nil
}

func (c *client) sendErrAndErr(err string) {
	c.sendErr(err)
	c.Errorf(err)
}

func (c *client) sendErrAndDebug(err string) {
	c.sendErr(err)
	c.Debugf(err)
}

func (c *client) authTimeout() {
	c.sendErrAndDebug("Authentication Timeout")
	c.closeConnection(AuthenticationTimeout)
}

func (c *client) authExpired() {
	c.sendErrAndDebug("User Authentication Expired")
	c.closeConnection(AuthenticationExpired)
}

func (c *client) accountAuthExpired() {
	c.sendErrAndDebug("Account Authentication Expired")
	c.closeConnection(AuthenticationExpired)
}

func (c *client) authViolation() {
	var s *Server
	var hasTrustedNkeys, hasNkeys, hasUsers bool
	if s = c.srv; s != nil {
		s.mu.Lock()
		hasTrustedNkeys = len(s.trustedKeys) > 0
		hasNkeys = s.nkeys != nil
		hasUsers = s.users != nil
		s.mu.Unlock()
	}
	if hasTrustedNkeys {
		c.Errorf("%v", ErrAuthentication)
	} else if hasNkeys {
		c.Errorf("%s - Nkey %q",
			ErrAuthentication.Error(),
			c.opts.Nkey)
	} else if hasUsers {
		c.Errorf("%s - User %q",
			ErrAuthentication.Error(),
			c.opts.Username)
	} else {
		c.Errorf(ErrAuthentication.Error())
	}
	c.sendErr("Authorization Violation")
	c.closeConnection(AuthenticationViolation)
	if s != nil {
		s.sendAuthErrorEvent(c)
	}
}

func (c *client) maxAccountConnExceeded() {
	c.sendErrAndErr(ErrTooManyAccountConnections.Error())
	c.closeConnection(MaxAccountConnectionsExceeded)
}

func (c *client) maxConnExceeded() {
	c.sendErrAndErr(ErrTooManyConnections.Error())
	c.closeConnection(MaxConnectionsExceeded)
}

func (c *client) maxSubsExceeded() {
	c.sendErrAndErr(ErrTooManySubs.Error())
}

func (c *client) maxPayloadViolation(sz int, max int32) {
	c.Errorf("%s: %d vs %d", ErrMaxPayload.Error(), sz, max)
	c.sendErr("Maximum Payload Violation")
	c.closeConnection(MaxPayloadExceeded)
}

// queueOutbound queues data for a clientconnection.
// Return if the data is referenced or not. If referenced, the caller
// should not reuse the `data` array.
// Lock should be held.
func (c *client) queueOutbound(data []byte) bool {
	// Do not keep going if closed or cleared via a slow consumer
	if c.flags.isSet(clearConnection) {
		return false
	}

	// Assume data will not be referenced
	referenced := false
	// Add to pending bytes total.
	c.out.pb += int32(len(data))

	// Check for slow consumer via pending bytes limit.
	// ok to return here, client is going away.
	if c.out.pb > c.out.mp {
		c.clearConnection(SlowConsumerPendingBytes)
		atomic.AddInt64(&c.srv.slowConsumers, 1)
		c.Noticef("Slow Consumer Detected: MaxPending of %d Exceeded", c.out.mp)
		return referenced
	}

	if c.out.p == nil && len(data) < maxBufSize {
		if c.out.sz == 0 {
			c.out.sz = startBufSize
		}
		if c.out.s != nil && cap(c.out.s) >= int(c.out.sz) {
			c.out.p = c.out.s
			c.out.s = nil
		} else {
			// FIXME(dlc) - make power of 2 if less than maxBufSize?
			c.out.p = make([]byte, 0, c.out.sz)
		}
	}
	// Determine if we copy or reference
	available := cap(c.out.p) - len(c.out.p)
	if len(data) > available {
		// We can't fit everything into existing primary, but message will
		// fit in next one we allocate or utilize from the secondary.
		// So copy what we can.
		if available > 0 && len(data) < int(c.out.sz) {
			c.out.p = append(c.out.p, data[:available]...)
			data = data[available:]
		}
		// Put the primary on the nb if it has a payload
		if len(c.out.p) > 0 {
			c.out.nb = append(c.out.nb, c.out.p)
			c.out.p = nil
		}
		// Check for a big message, and if found place directly on nb
		// FIXME(dlc) - do we need signaling of ownership here if we want len(data) < maxBufSize
		if len(data) > maxBufSize {
			c.out.nb = append(c.out.nb, data)
			referenced = true
		} else {
			// We will copy to primary.
			if c.out.p == nil {
				// Grow here
				if (c.out.sz << 1) <= maxBufSize {
					c.out.sz <<= 1
				}
				if len(data) > int(c.out.sz) {
					c.out.p = make([]byte, 0, len(data))
				} else {
					if c.out.s != nil && cap(c.out.s) >= int(c.out.sz) { // TODO(dlc) - Size mismatch?
						c.out.p = c.out.s
						c.out.s = nil
					} else {
						c.out.p = make([]byte, 0, c.out.sz)
					}
				}
			}
			c.out.p = append(c.out.p, data...)
		}
	} else {
		c.out.p = append(c.out.p, data...)
	}

	// Check here if we should create a stall channel if we are falling behind.
	// We do this here since if we wait for consumer's writeLoop it could be
	// too late with large number of fan in producers.
	if c.out.pb > c.out.mp/2 && c.out.stc == nil {
		c.out.stc = make(chan struct{})
	}

	return referenced
}

// Assume the lock is held upon entry.
func (c *client) sendProto(info []byte, doFlush bool) {
	if c.nc == nil {
		return
	}
	c.queueOutbound(info)
	if !(doFlush && c.flushOutbound()) {
		c.flushSignal()
	}
}

// Assume the lock is held upon entry.
func (c *client) sendPong() {
	c.traceOutOp("PONG", nil)
	c.sendProto([]byte("PONG\r\n"), true)
}

// Assume the lock is held upon entry.
func (c *client) sendPing() {
	c.rttStart = time.Now()
	c.ping.out++
	c.traceOutOp("PING", nil)
	c.sendProto([]byte("PING\r\n"), true)
}

// Generates the INFO to be sent to the client with the client ID included.
// info arg will be copied since passed by value.
// Assume lock is held.
func (c *client) generateClientInfoJSON(info Info) []byte {
	info.CID = c.cid
	// Generate the info json
	b, _ := json.Marshal(info)
	pcs := [][]byte{[]byte("INFO"), b, []byte(CR_LF)}
	return bytes.Join(pcs, []byte(" "))
}

// Assume the lock is held upon entry.
func (c *client) sendInfo(info []byte) {
	c.sendProto(info, true)
}

func (c *client) sendErr(err string) {
	c.mu.Lock()
	c.traceOutOp("-ERR", []byte(err))
	c.sendProto([]byte(fmt.Sprintf("-ERR '%s'\r\n", err)), true)
	c.mu.Unlock()
}

func (c *client) sendOK() {
	proto := []byte("+OK\r\n")
	c.mu.Lock()
	c.traceOutOp("OK", nil)
	// Can not autoflush this one, needs to be async.
	c.sendProto(proto, false)
	c.pcd[c] = needFlush
	c.mu.Unlock()
}

func (c *client) processPing() {
	c.mu.Lock()
	c.traceInOp("PING", nil)
	if c.nc == nil {
		c.mu.Unlock()
		return
	}
	c.sendPong()

	// If not a CLIENT, we are done
	if c.kind != CLIENT {
		c.mu.Unlock()
		return
	}

	// The CONNECT should have been received, but make sure it
	// is so before proceeding
	if !c.flags.isSet(connectReceived) {
		c.mu.Unlock()
		return
	}
	// If we are here, the CONNECT has been received so we know
	// if this client supports async INFO or not.
	var (
		checkClusterChange bool
		srv                = c.srv
	)
	// For older clients, just flip the firstPongSent flag if not already
	// set and we are done.
	if c.opts.Protocol < ClientProtoInfo || srv == nil {
		c.flags.setIfNotSet(firstPongSent)
	} else {
		// This is a client that supports async INFO protocols.
		// If this is the first PING (so firstPongSent is not set yet),
		// we will need to check if there was a change in cluster topology.
		checkClusterChange = !c.flags.isSet(firstPongSent)
	}
	c.mu.Unlock()

	if checkClusterChange {
		srv.mu.Lock()
		c.mu.Lock()
		// Now that we are under both locks, we can flip the flag.
		// This prevents sendAsyncInfoToClients() and and code here
		// to send a double INFO protocol.
		c.flags.set(firstPongSent)
		// If there was a cluster update since this client was created,
		// send an updated INFO protocol now.
		if srv.lastCURLsUpdate >= c.start.UnixNano() {
			c.sendInfo(c.generateClientInfoJSON(srv.copyInfo()))
		}
		c.mu.Unlock()
		srv.mu.Unlock()
	}
}

func (c *client) processPong() {
	c.traceInOp("PONG", nil)
	c.mu.Lock()
	c.ping.out = 0
	c.rtt = time.Since(c.rttStart)
	srv := c.srv
	reorderGWs := c.kind == GATEWAY && c.gw.outbound
	c.mu.Unlock()
	if reorderGWs {
		srv.gateway.orderOutboundConnections()
	}
}

func (c *client) processPub(trace bool, arg []byte) error {
	if trace {
		c.traceInOp("PUB", arg)
	}

	// Unroll splitArgs to avoid runtime/heap issues
	a := [MAX_PUB_ARGS][]byte{}
	args := a[:0]
	start := -1
	for i, b := range arg {
		switch b {
		case ' ', '\t':
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

	c.pa.arg = arg
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
	maxPayload := atomic.LoadInt32(&c.mpay)
	if maxPayload != jwt.NoLimit && int32(c.pa.size) > maxPayload {
		c.maxPayloadViolation(c.pa.size, maxPayload)
		return ErrMaxPayload
	}

	if c.opts.Pedantic && !IsValidLiteralSubject(string(c.pa.subject)) {
		c.sendErr("Invalid Publish Subject")
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
	c.traceInOp("SUB", argo)

	// Indicate activity.
	c.in.subs++

	// Copy so we do not reference a potentially large buffer
	// FIXME(dlc) - make more efficient.
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

	// Grab connection type, account and server info.
	kind := c.kind
	acc := c.acc
	srv := c.srv

	sid := string(sub.sid)

	if c.nc == nil && kind != SYSTEM {
		c.mu.Unlock()
		return nil
	}

	// Check permissions if applicable.
	if kind == CLIENT && !c.canSubscribe(string(sub.subject)) {
		c.mu.Unlock()
		c.sendErr(fmt.Sprintf("Permissions Violation for Subscription to %q", sub.subject))
		c.Errorf("Subscription Violation - %s, Subject %q, SID %s",
			c.getAuthUser(), sub.subject, sub.sid)
		return nil
	}

	// Check if we have a maximum on the number of subscriptions.
	if c.subsAtLimit() {
		c.mu.Unlock()
		c.maxSubsExceeded()
		return nil
	}

	updateGWs := false

	// Subscribe here.
	if c.subs[sid] == nil {
		c.subs[sid] = sub
		if acc != nil && acc.sl != nil {
			err = acc.sl.Insert(sub)
			if err != nil {
				delete(c.subs, sid)
			} else {
				updateGWs = c.srv.gateway.enabled
			}
		}
	}
	// Unlocked from here onward
	c.mu.Unlock()

	if err != nil {
		c.sendErr("Invalid Subject")
		return nil
	} else if c.opts.Verbose && kind != SYSTEM {
		c.sendOK()
	}

	// No account just return.
	if acc == nil {
		return nil
	}

	if err := c.addShadowSubscriptions(acc, sub); err != nil {
		c.Errorf(err.Error())
	}
	// If we are routing and this is a local sub, add to the route map for the associated account.
	if kind == CLIENT || kind == SYSTEM {
		srv.updateRouteSubscriptionMap(acc, sub, 1)
		if updateGWs {
			srv.gatewayUpdateSubInterest(acc.Name, sub, 1)
		}
	}
	// Now check on leafnode updates.
	srv.updateLeafNodes(acc, sub, 1)
	return nil
}

// If the client's account has stream imports and there are matches for
// this subscription's subject, then add shadow subscriptions in
// other accounts that can export this subject.
func (c *client) addShadowSubscriptions(acc *Account, sub *subscription) error {
	if acc == nil {
		return ErrMissingAccount
	}

	var (
		rims   [32]*streamImport
		ims    = rims[:0]
		rfroms [32]*streamImport
		froms  = rfroms[:0]
		tokens []string
		tsa    [32]string
		hasWC  bool
	)

	acc.mu.RLock()
	// Loop over the import subjects. We have 3 scenarios. If we exact
	// match or we know the proposed subject is a strict subset of the
	// import we can subscribe to the subscription's subject directly.
	// The third scenario is where the proposed subject has a wildcard
	// and may not be an exact subset, but is a match. Therefore we have to
	// subscribe to the import subject, not the subscription's subject.
	for _, im := range acc.imports.streams {
		if im.invalid {
			continue
		}
		subj := string(sub.subject)
		if subj == im.prefix+im.from {
			ims = append(ims, im)
			continue
		}
		if tokens == nil {
			tokens = tsa[:0]
			start := 0
			for i := 0; i < len(subj); i++ {
				// This is not perfect, but the test below will
				// be more exact, this is just to trigger the
				// additional test.
				if subj[i] == pwc || subj[i] == fwc {
					hasWC = true
				} else if subj[i] == btsep {
					tokens = append(tokens, subj[start:i])
					start = i + 1
				}
			}
			tokens = append(tokens, subj[start:])
		}
		if isSubsetMatch(tokens, im.prefix+im.from) {
			ims = append(ims, im)
		} else if hasWC {
			if subjectIsSubsetMatch(im.prefix+im.from, subj) {
				froms = append(froms, im)
			}
		}
	}
	acc.mu.RUnlock()

	var shadow []*subscription

	if len(ims) > 0 || len(froms) > 0 {
		shadow = make([]*subscription, 0, len(ims)+len(froms))
	}

	// Now walk through collected importMaps
	for _, im := range ims {
		// We will create a shadow subscription.
		nsub, err := c.addShadowSub(sub, im, false)
		if err != nil {
			return err
		}
		shadow = append(shadow, nsub)
	}
	// Now walk through importMaps that we need to subscribe
	// exactly to the "from" property.
	for _, im := range froms {
		// We will create a shadow subscription.
		nsub, err := c.addShadowSub(sub, im, true)
		if err != nil {
			return err
		}
		shadow = append(shadow, nsub)
	}

	if shadow != nil {
		c.mu.Lock()
		sub.shadow = shadow
		c.mu.Unlock()
	}

	return nil
}

// Add in the shadow subscription.
func (c *client) addShadowSub(sub *subscription, im *streamImport, useFrom bool) (*subscription, error) {
	nsub := *sub // copy
	nsub.im = im
	if useFrom {
		nsub.subject = []byte(im.from)
	} else if im.prefix != "" {
		// redo subject here to match subject in the publisher account space.
		// Just remove prefix from what they gave us. That maps into other space.
		nsub.subject = sub.subject[len(im.prefix):]
	}

	c.Debugf("Creating import subscription on %q from account %q", nsub.subject, im.acc.Name)

	if err := im.acc.sl.Insert(&nsub); err != nil {
		errs := fmt.Sprintf("Could not add shadow import subscription for account %q", im.acc.Name)
		c.Debugf(errs)
		return nil, fmt.Errorf(errs)
	}

	// Update our route map here.
	c.srv.updateRouteSubscriptionMap(im.acc, &nsub, 1)
	return &nsub, nil
}

// canSubscribe determines if the client is authorized to subscribe to the
// given subject. Assumes caller is holding lock.
func (c *client) canSubscribe(subject string) bool {
	if c.perms == nil {
		return true
	}

	allowed := true

	// Check allow list. If no allow list that means all are allowed. Deny can overrule.
	if c.perms.sub.allow != nil {
		r := c.perms.sub.allow.Match(subject)
		allowed = len(r.psubs) != 0
	}
	// If we have a deny list and we think we are allowed, check that as well.
	if allowed && c.perms.sub.deny != nil {
		r := c.perms.sub.deny.Match(subject)
		allowed = len(r.psubs) == 0

		// We use the actual subscription to signal us to spin up the deny mperms
		// and cache. We check if the subject is a wildcard that contains any of
		// the deny clauses.
		// FIXME(dlc) - We could be smarter and track when these go away and remove.
		if allowed && c.mperms == nil && subjectHasWildcard(subject) {
			// Whip through the deny array and check if this wildcard subject is within scope.
			for _, sub := range c.darray {
				tokens := strings.Split(sub, tsep)
				if isSubsetMatch(tokens, sub) {
					c.loadMsgDenyFilter()
					break
				}
			}
		}
	}
	return allowed
}

// Low level unsubscribe for a given client.
func (c *client) unsubscribe(acc *Account, sub *subscription, force bool) {
	c.mu.Lock()
	if !force && sub.max > 0 && sub.nm < sub.max {
		c.Debugf(
			"Deferring actual UNSUB(%s): %d max, %d received",
			string(sub.subject), sub.max, sub.nm)
		c.mu.Unlock()
		return
	}
	c.traceOp("<-> %s", "DELSUB", sub.sid)

	delete(c.subs, string(sub.sid))
	if c.kind != CLIENT && c.kind != SYSTEM {
		c.removeReplySubTimeout(sub)
	}

	if acc != nil {
		acc.sl.Remove(sub)
	}

	// Check to see if we have shadow subscriptions.
	var updateRoute bool
	shadowSubs := sub.shadow
	sub.shadow = nil
	if len(shadowSubs) > 0 {
		updateRoute = (c.kind == CLIENT || c.kind == SYSTEM || c.kind == LEAF) && c.srv != nil
	}
	c.mu.Unlock()

	for _, nsub := range shadowSubs {
		if err := nsub.im.acc.sl.Remove(nsub); err != nil {
			c.Debugf("Could not remove shadow import subscription for account %q", nsub.im.acc.Name)
		} else if updateRoute {
			c.srv.updateRouteSubscriptionMap(nsub.im.acc, nsub, -1)
		}
		// Now check on leafnode updates.
		c.srv.updateLeafNodes(nsub.im.acc, nsub, -1)
	}
}

func (c *client) processUnsub(arg []byte) error {
	c.traceInOp("UNSUB", arg)
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
	// Indicate activity.
	c.in.subs++

	var sub *subscription
	var ok, unsub bool

	c.mu.Lock()

	// Grab connection type.
	kind := c.kind
	srv := c.srv
	var acc *Account

	updateGWs := false
	if sub, ok = c.subs[string(sid)]; ok {
		acc = c.acc
		if max > 0 {
			sub.max = int64(max)
		} else {
			// Clear it here to override
			sub.max = 0
			unsub = true
		}
		updateGWs = srv.gateway.enabled
	}
	c.mu.Unlock()

	if c.opts.Verbose {
		c.sendOK()
	}

	if unsub {
		c.unsubscribe(acc, sub, false)
		if acc != nil && kind == CLIENT || kind == SYSTEM {
			srv.updateRouteSubscriptionMap(acc, sub, -1)
			if updateGWs {
				srv.gatewayUpdateSubInterest(acc.Name, sub, -1)
			}
		}
		// Now check on leafnode updates.
		srv.updateLeafNodes(acc, sub, -1)
	}

	return nil
}

// checkDenySub will check if we are allowed to deliver this message in the
// presence of deny clauses for subscriptions. Deny clauses will not prevent
// larger scoped wildcard subscriptions, so we need to check at delivery time.
// Lock should be held.
func (c *client) checkDenySub(subject string) bool {
	if denied, ok := c.mperms.dcache[subject]; ok {
		return denied
	} else if r := c.mperms.deny.Match(subject); len(r.psubs) != 0 {
		c.mperms.dcache[subject] = true
		return true
	} else {
		c.mperms.dcache[subject] = false
	}
	if len(c.mperms.dcache) > maxDenyPermCacheSize {
		c.pruneDenyCache()
	}
	return false
}

func (c *client) msgHeader(mh []byte, sub *subscription, reply []byte) []byte {
	if len(sub.sid) > 0 {
		mh = append(mh, sub.sid...)
		mh = append(mh, ' ')
	}
	if reply != nil {
		mh = append(mh, reply...)
		mh = append(mh, ' ')
	}
	mh = append(mh, c.pa.szb...)
	mh = append(mh, _CRLF_...)
	return mh
}

func (c *client) stalledWait(producer *client) {
	stall := c.out.stc
	c.mu.Unlock()
	defer c.mu.Lock()

	// TODO(dlc) - Make the stall timeout variable based on health of consumer.
	select {
	case <-stall:
	case <-time.After(100 * time.Millisecond):
		producer.Debugf("Timed out of fast producer stall")
	}
}

// Used to treat maps as efficient set
var needFlush = struct{}{}

// deliverMsg will deliver a message to a matching subscription and its underlying client.
// We process all connection/client types. mh is the part that will be protocol/client specific.
func (c *client) deliverMsg(sub *subscription, mh, msg []byte) bool {
	if sub.client == nil {
		return false
	}
	client := sub.client
	client.mu.Lock()

	// Check echo
	if c == client && !client.echo {
		client.mu.Unlock()
		return false
	}

	// Check if we have a subscribe deny clause. This will trigger us to check the subject
	// for a match against the denied subjects.
	if client.mperms != nil && client.checkDenySub(string(c.pa.subject)) {
		client.mu.Unlock()
		return false
	}

	srv := client.srv

	sub.nm++
	// Check if we should auto-unsubscribe.
	if sub.max > 0 {
		if client.kind == ROUTER && sub.nm >= sub.max {
			// The only router based messages that we will see here are remoteReplies.
			// We handle these slightly differently.
			defer client.removeReplySub(sub)
		} else {
			// For routing..
			shouldForward := client.kind == CLIENT || client.kind == SYSTEM && client.srv != nil
			// If we are at the exact number, unsubscribe but
			// still process the message in hand, otherwise
			// unsubscribe and drop message on the floor.
			if sub.nm == sub.max {
				client.Debugf("Auto-unsubscribe limit of %d reached for sid '%s'", sub.max, string(sub.sid))
				// Due to defer, reverse the code order so that execution
				// is consistent with other cases where we unsubscribe.
				if shouldForward {
					defer srv.updateRouteSubscriptionMap(client.acc, sub, -1)
				}
				defer client.unsubscribe(client.acc, sub, true)
			} else if sub.nm > sub.max {
				client.Debugf("Auto-unsubscribe limit [%d] exceeded", sub.max)
				client.mu.Unlock()
				client.unsubscribe(client.acc, sub, true)
				if shouldForward {
					srv.updateRouteSubscriptionMap(client.acc, sub, -1)
				}
				return false
			}
		}
	}

	// Update statistics

	// The msg includes the CR_LF, so pull back out for accounting.
	msgSize := int64(len(msg) - LEN_CR_LF)

	// No atomic needed since accessed under client lock.
	// Monitor is reading those also under client's lock.
	client.outMsgs++
	client.outBytes += msgSize

	atomic.AddInt64(&srv.outMsgs, 1)
	atomic.AddInt64(&srv.outBytes, msgSize)

	// Check for internal subscription.
	if client.kind == SYSTEM {
		s := client.srv
		client.mu.Unlock()
		s.deliverInternalMsg(sub, c.pa.subject, c.pa.reply, msg[:msgSize])
		return true
	}

	// If we are a client and we detect that the consumer we are
	// sending to is in a stalled state, go ahead and wait here
	// with a limit.
	if c.kind == CLIENT && client.out.stc != nil {
		client.stalledWait(c)
	}

	// Check for closed connection
	if client.flags.isSet(clearConnection) {
		client.mu.Unlock()
		return false
	}

	// Queue to outbound buffer
	client.queueOutbound(mh)
	client.queueOutbound(msg)

	client.out.pm++

	// Check outbound threshold and queue IO flush if needed.
	// This is specifically looking at situations where we are getting behind and may want
	// to intervene before this producer goes back to top of readloop. We are in the producer's
	// readloop go routine at this point.
	// FIXME(dlc) - We may call this alot, maybe suppress after first call?
	if client.out.pm > 1 && client.out.pb > maxBufSize*2 {
		client.flushSignal()
	}

	// Add the data size we are responsible for here. This will be processed when we
	// return to the top of the readLoop.
	if _, ok := c.pcd[client]; !ok {
		client.out.fsp++
		c.pcd[client] = needFlush
	}

	if c.trace {
		client.traceOutOp(string(mh[:len(mh)-LEN_CR_LF]), nil)
	}

	client.mu.Unlock()

	return true
}

// pruneDenyCache will prune the deny cache via randomly
// deleting items. Doing so pruneSize items at a time.
// Lock must be held for this one since it is shared under
// deliverMsg.
func (c *client) pruneDenyCache() {
	r := 0
	for subject := range c.mperms.dcache {
		delete(c.mperms.dcache, subject)
		if r++; r > pruneSize {
			break
		}
	}
}

// prunePubPermsCache will prune the cache via randomly
// deleting items. Doing so pruneSize items at a time.
func (c *client) prunePubPermsCache() {
	r := 0
	for subject := range c.perms.pcache {
		delete(c.perms.pcache, subject)
		if r++; r > pruneSize {
			break
		}
	}
}

// pubAllowed checks on publish permissioning.
func (c *client) pubAllowed(subject string) bool {
	if c.perms == nil || (c.perms.pub.allow == nil && c.perms.pub.deny == nil) {
		return true
	}
	// Check if published subject is allowed if we have permissions in place.
	allowed, ok := c.perms.pcache[subject]
	if ok {
		return allowed
	}
	// Cache miss, check allow then deny as needed.
	if c.perms.pub.allow != nil {
		r := c.perms.pub.allow.Match(subject)
		allowed = len(r.psubs) != 0
	} else {
		// No entries means all are allowed. Deny will overrule as needed.
		allowed = true
	}
	// If we have a deny list and are currently allowed, check that as well.
	if allowed && c.perms.pub.deny != nil {
		r := c.perms.pub.deny.Match(subject)
		allowed = len(r.psubs) == 0
	}
	// Update our cache here.
	c.perms.pcache[string(subject)] = allowed
	// Prune if needed.
	if len(c.perms.pcache) > maxPermCacheSize {
		c.prunePubPermsCache()
	}
	return allowed
}

// Used to mimic client like replies.
const (
	replyPrefix    = "_R_."
	replyPrefixLen = len(replyPrefix)
	digits         = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	base           = 62
)

// newServiceReply is used when rewriting replies that cross account boundaries.
// These will look like _R_.XXXXXXXX.
func (c *client) newServiceReply() []byte {
	// Check to see if we have our own rand yet. Global rand
	// has contention with lots of clients, etc.
	if c.in.prand == nil {
		c.in.prand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	var b = [15]byte{'_', 'R', '_', '.'}
	rn := c.in.prand.Int63()
	for i, l := replyPrefixLen, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	return b[:]
}

// Test whether a reply subject is a service import reply.
func isServiceReply(reply []byte) bool {
	return len(reply) > 3 && string(reply[:4]) == replyPrefix
}

// This will decide to call the client code or router code.
func (c *client) processInboundMsg(msg []byte) {
	switch c.kind {
	case CLIENT:
		c.processInboundClientMsg(msg)
	case ROUTER:
		c.processInboundRoutedMsg(msg)
	case GATEWAY:
		c.processInboundGatewayMsg(msg)
	case LEAF:
		c.processInboundLeafMsg(msg)
	}
}

// processInboundClientMsg is called to process an inbound msg from a client.
func (c *client) processInboundClientMsg(msg []byte) {
	// Update statistics
	// The msg includes the CR_LF, so pull back out for accounting.
	c.in.msgs++
	c.in.bytes += int32(len(msg) - LEN_CR_LF)

	if c.trace {
		c.traceMsg(msg)
	}

	// Check pub permissions
	if c.perms != nil && (c.perms.pub.allow != nil || c.perms.pub.deny != nil) && !c.pubAllowed(string(c.pa.subject)) {
		c.pubPermissionViolation(c.pa.subject)
		return
	}

	// Now check for reserved replies. These are used for service imports.
	if isServiceReply(c.pa.reply) {
		c.replySubjectViolation(c.pa.reply)
		return
	}

	if c.opts.Verbose {
		c.sendOK()
	}

	// Mostly under testing scenarios.
	if c.srv == nil || c.acc == nil {
		return
	}

	// Match the subscriptions. We will use our own L1 map if
	// it's still valid, avoiding contention on the shared sublist.
	var r *SublistResult
	var ok bool

	genid := atomic.LoadUint64(&c.acc.sl.genid)
	if genid == c.in.genid && c.in.results != nil {
		r, ok = c.in.results[string(c.pa.subject)]
	} else {
		// Reset our L1 completely.
		c.in.results = make(map[string]*SublistResult)
		c.in.genid = genid
	}

	// Go back to the sublist data structure.
	if !ok {
		r = c.acc.sl.Match(string(c.pa.subject))
		c.in.results[string(c.pa.subject)] = r
		// Prune the results cache. Keeps us from unbounded growth. Random delete.
		if len(c.in.results) > maxResultCacheSize {
			n := 0
			for subject := range c.in.results {
				delete(c.in.results, subject)
				if n++; n > pruneSize {
					break
				}
			}
		}
	}

	// Check to see if we need to map/route to another account.
	if c.acc.imports.services != nil {
		c.checkForImportServices(c.acc, msg)
	}

	var qnames [][]byte

	// Check for no interest, short circuit if so.
	// This is the fanout scale.
	if len(r.psubs)+len(r.qsubs) > 0 {
		flag := pmrNoFlag
		// If we have queue subs in this cluster, then if we run in gateway
		// mode and the remote gateways have queue subs, then we need to
		// collect the queue groups this message was sent to so that we
		// exclude them when sending to gateways.
		if len(r.qsubs) > 0 && c.srv.gateway.enabled &&
			atomic.LoadInt64(&c.srv.gateway.totalQSubs) > 0 {
			flag = pmrCollectQueueNames
		}
		qnames = c.processMsgResults(c.acc, r, msg, c.pa.subject, c.pa.reply, flag)
	}

	// Now deal with gateways
	if c.srv.gateway.enabled {
		c.sendMsgToGateways(c.acc, msg, c.pa.subject, c.pa.reply, qnames)
	}
}

// This checks and process import services by doing the mapping and sending the
// message onward if applicable.
func (c *client) checkForImportServices(acc *Account, msg []byte) {
	if acc == nil || acc.imports.services == nil {
		return
	}
	acc.mu.RLock()
	rm := acc.imports.services[string(c.pa.subject)]
	invalid := rm != nil && rm.invalid
	acc.mu.RUnlock()

	// Get the results from the other account for the mapped "to" subject.
	// If we have been marked invalid simply return here.
	if rm != nil && !invalid && rm.acc != nil && rm.acc.sl != nil {
		var nrr []byte
		if rm.ae {
			acc.removeServiceImport(rm.from)
		}
		if c.pa.reply != nil {
			// We want to remap this to provide anonymity.
			nrr = c.newServiceReply()
			rm.acc.addImplicitServiceImport(acc, string(nrr), string(c.pa.reply), true, nil)
			// If this is a client connection and we are in
			// gateway mode, we need to send RS+ to local cluster
			// and possibly to inbound GW connections for
			// which we are in interest-only mode.
			if c.srv.gateway.enabled && (c.kind == CLIENT || c.kind == LEAF) {
				c.srv.gatewayHandleServiceImport(rm.acc, nrr, c, 1)
			}
		}
		// FIXME(dlc) - Do L1 cache trick from above.
		rr := rm.acc.sl.Match(rm.to)

		// If we are a route or gateway or leafnode and this message is flipped to a queue subscriber we
		// need to handle that since the processMsgResults will want a queue filter.
		if (c.kind == ROUTER || c.kind == GATEWAY || c.kind == LEAF) && c.pa.queues == nil && len(rr.qsubs) > 0 {
			c.makeQFilter(rr.qsubs)
		}

		// If this is not a gateway connection but gateway is enabled,
		// try to send this converted message to all gateways.
		if c.srv.gateway.enabled && (c.kind == CLIENT || c.kind == SYSTEM || c.kind == LEAF) {
			queues := c.processMsgResults(rm.acc, rr, msg, []byte(rm.to), nrr, pmrCollectQueueNames)
			c.sendMsgToGateways(rm.acc, msg, []byte(rm.to), nrr, queues)
		} else {
			c.processMsgResults(rm.acc, rr, msg, []byte(rm.to), nrr, pmrNoFlag)
		}
	}
}

func (c *client) addSubToRouteTargets(sub *subscription) {
	if c.in.rts == nil {
		c.in.rts = make([]routeTarget, 0, routeTargetInit)
	}

	for i := range c.in.rts {
		rt := &c.in.rts[i]
		if rt.sub.client == sub.client {
			if sub.queue != nil {
				rt.qs = append(rt.qs, sub.queue...)
				rt.qs = append(rt.qs, ' ')
			}
			return
		}
	}

	var rt *routeTarget
	lrts := len(c.in.rts)

	// If we are here we do not have the sub yet in our list
	// If we have to grow do so here.
	if lrts == cap(c.in.rts) {
		c.in.rts = append(c.in.rts, routeTarget{})
	}

	c.in.rts = c.in.rts[:lrts+1]
	rt = &c.in.rts[lrts]
	rt.sub = sub
	rt.qs = rt._qs[:0]
	if sub.queue != nil {
		rt.qs = append(rt.qs, sub.queue...)
		rt.qs = append(rt.qs, ' ')
	}
}

// This processes the sublist results for a given message.
func (c *client) processMsgResults(acc *Account, r *SublistResult, msg, subject, reply []byte, flags int) [][]byte {
	var queues [][]byte
	// msg header for clients.
	msgh := c.msgb[1:msgHeadProtoLen]
	msgh = append(msgh, subject...)
	msgh = append(msgh, ' ')
	si := len(msgh)

	// For sending messages across routes. Reset it if we have one.
	// We reuse this data structure.
	if c.in.rts != nil {
		c.in.rts = c.in.rts[:0]
	}

	// Loop over all normal subscriptions that match.
	for _, sub := range r.psubs {
		// Check if this is a send to a ROUTER. We now process
		// these after everything else.
		switch sub.client.kind {
		case ROUTER:
			if c.kind == ROUTER {
				continue
			}
			c.addSubToRouteTargets(sub)
			continue
		case GATEWAY:
			// Never send to gateway from here.
			continue
		case LEAF:
			// We handle similarly to routes and use the same data structures.
			// Leaf node delivery audience is different however.
			// Also leaf nodes are always no echo, so we make sure we are not
			// going to send back to ourselves here.
			if c != sub.client {
				c.addSubToRouteTargets(sub)
			}
			continue
		}
		// Check for stream import mapped subs. These apply to local subs only.
		if sub.im != nil && sub.im.prefix != "" {
			// Redo the subject here on the fly.
			msgh = c.msgb[1:msgHeadProtoLen]
			msgh = append(msgh, sub.im.prefix...)
			msgh = append(msgh, subject...)
			msgh = append(msgh, ' ')
			si = len(msgh)
		}
		// Normal delivery
		mh := c.msgHeader(msgh[:si], sub, reply)
		c.deliverMsg(sub, mh, msg)
	}

	// Set these up to optionally filter based on the queue lists.
	// This is for messages received from routes which will have directed
	// guidance on which queue groups we should deliver to.
	qf := c.pa.queues

	// For all non-client connections, we may still want to send messages to
	// leaf nodes or routes even if there are no queue filters since we collect
	// them above and do not process inline like normal clients.
	if c.kind != CLIENT && qf == nil {
		// However, if this is a gateway connection which should be treated
		// as a client, still go and pick queue subscriptions, otherwise
		// jump to sendToRoutesOrLeafs.
		if !(c.kind == GATEWAY && (flags&pmrTreatGatewayAsClient != 0)) {
			goto sendToRoutesOrLeafs
		}
	}

	// Check to see if we have our own rand yet. Global rand
	// has contention with lots of clients, etc.
	if c.in.prand == nil {
		c.in.prand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	// Process queue subs
	for i := 0; i < len(r.qsubs); i++ {
		qsubs := r.qsubs[i]
		// If we have a filter check that here. We could make this a map or someting more
		// complex but linear search since we expect queues to be small. Should be faster
		// and more cache friendly.
		if qf != nil && len(qsubs) > 0 {
			tqn := qsubs[0].queue
			for _, qn := range qf {
				if bytes.Equal(qn, tqn) {
					goto selectQSub
				}
			}
			continue
		}

	selectQSub:

		// We will hold onto remote or lead qsubs when we are coming from
		// a route or a leaf node just in case we can no longer do local delivery.
		var rsub *subscription

		// Find a subscription that is able to deliver this message
		// starting at a random index.
		startIndex := c.in.prand.Intn(len(qsubs))
		for i := 0; i < len(qsubs); i++ {
			index := (startIndex + i) % len(qsubs)
			sub := qsubs[index]
			if sub == nil {
				continue
			}
			kind := sub.client.kind
			// Potentially sending to a remote sub across a route or leaf node.
			if kind == ROUTER || kind == LEAF {
				if c.kind == ROUTER || c.kind == LEAF || (c.kind == CLIENT && kind == LEAF) {
					// We just came from a route/leaf, so skip and prefer local subs.
					// Keep our first rsub in case all else fails.
					if rsub == nil {
						rsub = sub
					}
					continue
				} else {
					c.addSubToRouteTargets(sub)
					if flags&pmrCollectQueueNames != 0 {
						queues = append(queues, sub.queue)
					}
				}
				break
			}

			// Check for mapped subs
			if sub.im != nil && sub.im.prefix != "" {
				// Redo the subject here on the fly.
				msgh = c.msgb[1:msgHeadProtoLen]
				msgh = append(msgh, sub.im.prefix...)
				msgh = append(msgh, subject...)
				msgh = append(msgh, ' ')
				si = len(msgh)
			}

			mh := c.msgHeader(msgh[:si], sub, reply)
			if c.deliverMsg(sub, mh, msg) {
				// Clear rsub
				rsub = nil
				if flags&pmrCollectQueueNames != 0 {
					queues = append(queues, sub.queue)
				}
				break
			}
		}

		if rsub != nil {
			// If we are here we tried to deliver to a local qsub
			// but failed. So we will send it to a remote or leaf node.
			c.addSubToRouteTargets(rsub)
			if flags&pmrCollectQueueNames != 0 {
				queues = append(queues, rsub.queue)
			}
		}
	}

sendToRoutesOrLeafs:

	// If no messages for routes return here.
	if len(c.in.rts) == 0 {
		return queues
	}

	// We address by index to avoid struct copy.
	// We have inline structs for memory layout and cache coherency.
	for i := range c.in.rts {
		rt := &c.in.rts[i]
		kind := rt.sub.client.kind
		mh := c.msgb[:msgHeadProtoLen]
		if kind == ROUTER {
			// Router (and Gateway) nodes are RMSG. Set here since leafnodes may rewrite.
			mh[0] = 'R'
			mh = append(mh, acc.Name...)
			mh = append(mh, ' ')
		} else {
			// Leaf nodes are LMSG
			mh[0] = 'L'
			// Remap subject if its a shadow subscription, treat like a normal client.
			if rt.sub.im != nil && rt.sub.im.prefix != "" {
				mh = append(mh, rt.sub.im.prefix...)
			}
		}
		mh = append(mh, subject...)
		mh = append(mh, ' ')

		if len(rt.qs) > 0 {
			if reply != nil {
				mh = append(mh, "+ "...) // Signal that there is a reply.
				mh = append(mh, reply...)
				mh = append(mh, ' ')
			} else {
				mh = append(mh, "| "...) // Only queues
			}
			mh = append(mh, rt.qs...)
		} else if reply != nil {
			mh = append(mh, reply...)
			mh = append(mh, ' ')
		}
		mh = append(mh, c.pa.szb...)
		mh = append(mh, _CRLF_...)
		c.deliverMsg(rt.sub, mh, msg)
	}
	return queues
}

func (c *client) pubPermissionViolation(subject []byte) {
	c.sendErr(fmt.Sprintf("Permissions Violation for Publish to %q", subject))
	c.Errorf("Publish Violation - %s, Subject %q", c.getAuthUser(), subject)
}

func (c *client) replySubjectViolation(reply []byte) {
	c.sendErr(fmt.Sprintf("Permissions Violation for Publish with Reply of %q", reply))
	c.Errorf("Publish Violation - %s, Reply %q", c.getAuthUser(), reply)
}

func (c *client) processPingTimer() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ping.tmr = nil
	// Check if connection is still opened
	if c.nc == nil {
		return
	}

	c.Debugf("%s Ping Timer", c.typeString())

	// If we have had activity within the PingInterval no
	// need to send a ping.
	if delta := time.Since(c.last); delta < c.srv.getOpts().PingInterval {
		c.Debugf("Delaying PING due to activity %v ago", delta.Round(time.Second))
	} else {
		// Check for violation
		if c.ping.out+1 > c.srv.getOpts().MaxPingsOut {
			c.Debugf("Stale Client Connection - Closing")
			c.sendProto([]byte(fmt.Sprintf("-ERR '%s'\r\n", "Stale Connection")), true)
			c.clearConnection(StaleConnection)
			return
		}
		// Send PING
		c.sendPing()
	}

	// Reset to fire again.
	c.setPingTimer()
}

// Lock should be held
func (c *client) setPingTimer() {
	if c.srv == nil {
		return
	}
	d := c.srv.getOpts().PingInterval
	c.ping.tmr = time.AfterFunc(d, c.processPingTimer)
}

// Lock should be held
func (c *client) clearPingTimer() {
	if c.ping.tmr == nil {
		return
	}
	c.ping.tmr.Stop()
	c.ping.tmr = nil
}

// Lock should be held
func (c *client) setAuthTimer(d time.Duration) {
	c.atmr = time.AfterFunc(d, c.authTimeout)
}

// Lock should be held
func (c *client) clearAuthTimer() bool {
	if c.atmr == nil {
		return true
	}
	stopped := c.atmr.Stop()
	c.atmr = nil
	return stopped
}

// We may reuse atmr for expiring user jwts,
// so check connectReceived.
// Lock assume held on entry.
func (c *client) awaitingAuth() bool {
	return !c.flags.isSet(connectReceived) && c.atmr != nil
}

// This will set the atmr for the JWT expiration time.
// We will lock on entry.
func (c *client) setExpirationTimer(d time.Duration) {
	c.mu.Lock()
	c.atmr = time.AfterFunc(d, c.authExpired)
	c.mu.Unlock()
}

// Lock should be held
func (c *client) clearConnection(reason ClosedState) {
	if c.flags.isSet(clearConnection) {
		return
	}
	c.flags.set(clearConnection)

	nc := c.nc
	srv := c.srv
	if nc == nil || srv == nil {
		return
	}

	// Unblock anyone who is potentially stalled waiting on us.
	if c.out.stc != nil {
		close(c.out.stc)
		c.out.stc = nil
	}

	// Flush any pending.
	c.flushOutbound()

	// Clear outbound here.
	if c.out.sg != nil {
		c.out.sg.Broadcast()
	}

	// With TLS, Close() is sending an alert (that is doing a write).
	// Need to set a deadline otherwise the server could block there
	// if the peer is not reading from socket.
	if c.flags.isSet(handshakeComplete) {
		nc.SetWriteDeadline(time.Now().Add(c.out.wdl))
	}
	nc.Close()
	// Do this always to also kick out any IO writes.
	nc.SetWriteDeadline(time.Time{})

	// Save off the connection if its a client or leafnode.
	if c.kind == CLIENT || c.kind == LEAF {
		go srv.saveClosedClient(c, nc, reason)
	}
}

func (c *client) typeString() string {
	switch c.kind {
	case CLIENT:
		return "Client"
	case ROUTER:
		return "Router"
	case GATEWAY:
		return "Gateway"
	case LEAF:
		return "LeafNode"
	}
	return "Unknown Type"
}

// processSubsOnConfigReload removes any subscriptions the client has that are no
// longer authorized, and check for imports (accounts) due to a config reload.
func (c *client) processSubsOnConfigReload(awcsti map[string]struct{}) {
	c.mu.Lock()
	var (
		checkPerms = c.perms != nil
		checkAcc   = c.acc != nil
		acc        = c.acc
	)
	if !checkPerms && !checkAcc {
		c.mu.Unlock()
		return
	}
	var (
		_subs    [32]*subscription
		subs     = _subs[:0]
		_removed [32]*subscription
		removed  = _removed[:0]
		srv      = c.srv
	)
	if checkAcc {
		// We actually only want to check if stream imports have changed.
		if _, ok := awcsti[acc.Name]; !ok {
			checkAcc = false
		}
	}
	// We will clear any mperms we have here. It will rebuild on the fly with canSubscribe,
	// so we do that here as we collect them. We will check result down below.
	c.mperms = nil
	// Collect client's subs under the lock
	for _, sub := range c.subs {
		// Just checking to rebuild mperms under the lock, will collect removed though here.
		// Only collect under subs array of canSubscribe and checkAcc true.
		if !c.canSubscribe(string(sub.subject)) {
			removed = append(removed, sub)
		} else if checkAcc {
			subs = append(subs, sub)
		}
	}
	c.mu.Unlock()

	// This list is all subs who are allowed and we need to check accounts.
	for _, sub := range subs {
		c.mu.Lock()
		oldShadows := sub.shadow
		sub.shadow = nil
		c.mu.Unlock()
		c.addShadowSubscriptions(acc, sub)
		for _, nsub := range oldShadows {
			nsub.im.acc.sl.Remove(nsub)
		}
	}

	// Unsubscribe all that need to be removed and report back to client and logs.
	for _, sub := range removed {
		c.unsubscribe(acc, sub, true)
		c.sendErr(fmt.Sprintf("Permissions Violation for Subscription to %q (sid %q)",
			sub.subject, sub.sid))
		srv.Noticef("Removed sub %q (sid %q) for %s - not authorized",
			sub.subject, sub.sid, c.getAuthUser())
	}
}

// Allows us to count up all the queue subscribers during close.
type qsub struct {
	sub *subscription
	n   int32
}

func (c *client) closeConnection(reason ClosedState) {
	c.mu.Lock()
	if c.nc == nil {
		c.mu.Unlock()
		return
	}

	// Be consistent with the creation: for routes and gateways,
	// we use Noticef on create, so use that too for delete.
	if c.kind == ROUTER || c.kind == GATEWAY {
		c.Noticef("%s connection closed", c.typeString())
	} else { // Client and Leaf Node connections.
		c.Debugf("%s connection closed", c.typeString())
	}

	c.clearAuthTimer()
	c.clearPingTimer()
	c.clearConnection(reason)
	c.nc = nil

	var (
		retryImplicit bool
		connectURLs   []string
		gwName        string
		gwIsOutbound  bool
		gwCfg         *gatewayCfg
		kind          = c.kind
		srv           = c.srv
		noReconnect   = c.flags.isSet(noReconnect)
		acc           = c.acc
	)

	// Snapshot for use if we are a client connection.
	// FIXME(dlc) - we can just stub in a new one for client
	// and reference existing one.
	var subs []*subscription
	if kind == CLIENT || kind == LEAF {
		subs = make([]*subscription, 0, len(c.subs))
		for _, sub := range c.subs {
			// Auto-unsubscribe subscriptions must be unsubscribed forcibly.
			sub.max = 0
			subs = append(subs, sub)
		}
	}

	if c.route != nil {
		if !noReconnect {
			retryImplicit = c.route.retry
		}
		connectURLs = c.route.connectURLs
	}
	if kind == GATEWAY {
		gwName = c.gw.name
		gwIsOutbound = c.gw.outbound
		gwCfg = c.gw.cfg
	}

	c.mu.Unlock()

	// Remove client's or leaf node subscriptions.
	if kind == CLIENT || kind == LEAF && acc != nil {
		acc.sl.RemoveBatch(subs)
	} else if kind == ROUTER {
		go c.removeRemoteSubs()
	}

	if srv != nil {
		// This is a route that disconnected, but we are not in lame duck mode...
		if len(connectURLs) > 0 && !srv.isLameDuckMode() {
			// Unless disabled, possibly update the server's INFO protocol
			// and send to clients that know how to handle async INFOs.
			if !srv.getOpts().Cluster.NoAdvertise {
				srv.removeClientConnectURLsAndSendINFOToClients(connectURLs)
			}
		}

		// Unregister
		srv.removeClient(c)

		// Update remote subscriptions.
		if acc != nil && (kind == CLIENT || kind == LEAF) {
			qsubs := map[string]*qsub{}
			for _, sub := range subs {
				if sub.queue == nil {
					srv.updateRouteSubscriptionMap(acc, sub, -1)
				} else {
					// We handle queue subscribers special in case we
					// have a bunch we can just send one update to the
					// connected routes.
					key := string(sub.subject) + " " + string(sub.queue)
					if esub, ok := qsubs[key]; ok {
						esub.n++
					} else {
						qsubs[key] = &qsub{sub, 1}
					}
				}
				if srv.gateway.enabled {
					srv.gatewayUpdateSubInterest(acc.Name, sub, -1)
				}
				// Now check on leafnode updates.
				srv.updateLeafNodes(acc, sub, -1)
			}
			// Process any qsubs here.
			for _, esub := range qsubs {
				srv.updateRouteSubscriptionMap(acc, esub.sub, -(esub.n))
				srv.updateLeafNodes(acc, esub.sub, -(esub.n))
			}
			if prev := acc.removeClient(c); prev == 1 && srv != nil {
				srv.decActiveAccounts()
			}
		}
	}

	// Don't reconnect connections that have been marked with
	// the no reconnect flag.
	if noReconnect {
		return
	}

	// Check for a solicited route. If it was, start up a reconnect unless
	// we are already connected to the other end.
	if c.isSolicitedRoute() || retryImplicit {
		// Capture these under lock
		c.mu.Lock()
		rid := c.route.remoteID
		rtype := c.route.routeType
		rurl := c.route.url
		c.mu.Unlock()

		srv.mu.Lock()
		defer srv.mu.Unlock()

		// It is possible that the server is being shutdown.
		// If so, don't try to reconnect
		if !srv.running {
			return
		}

		if rid != "" && srv.remotes[rid] != nil {
			srv.Debugf("Not attempting reconnect for solicited route, already connected to \"%s\"", rid)
			return
		} else if rid == srv.info.ID {
			srv.Debugf("Detected route to self, ignoring \"%s\"", rurl)
			return
		} else if rtype != Implicit || retryImplicit {
			srv.Debugf("Attempting reconnect for solicited route \"%s\"", rurl)
			// Keep track of this go-routine so we can wait for it on
			// server shutdown.
			srv.startGoRoutine(func() { srv.reConnectToRoute(rurl, rtype) })
		}
	} else if srv != nil && kind == GATEWAY && gwIsOutbound {
		if gwCfg != nil {
			srv.Debugf("Attempting reconnect for gateway %q", gwName)
			// Run this as a go routine since we may be called within
			// the solicitGateway itself if there was an error during
			// the creation of the gateway connection.
			srv.startGoRoutine(func() { srv.reconnectGateway(gwCfg) })
		} else {
			srv.Debugf("Gateway %q not in configuration, not attempting reconnect", gwName)
		}
	} else if c.isSolicitedLeafNode() {
		// Check if this is a solicited leaf node. Start up a reconnect.
		srv.startGoRoutine(func() { srv.reConnectToRemoteLeafNode(c.leaf.remote) })
	}
}

// Set the noReconnect flag. This is used before a call to closeConnection()
// to prevent the connection to reconnect (routes, gateways).
func (c *client) setNoReconnect() {
	c.mu.Lock()
	c.flags.set(noReconnect)
	c.mu.Unlock()
}

// Returns the client's RTT value with the protection of the client's lock.
func (c *client) getRTTValue() time.Duration {
	c.mu.Lock()
	rtt := c.rtt
	c.mu.Unlock()
	return rtt
}

// This function is used by ROUTER and GATEWAY connections to
// look for a subject on a given account (since these type of
// connections are not bound to a specific account).
// If the c.pa.subject is found in the cache, the cached result
// is returned, otherwse, we match the account's sublist and update
// the cache. The cache is pruned if reaching a certain size.
func (c *client) getAccAndResultFromCache() (*Account, *SublistResult) {
	var (
		acc *Account
		pac *perAccountCache
		r   *SublistResult
		ok  bool
	)
	// Check our cache.
	if pac, ok = c.in.pacache[string(c.pa.pacache)]; ok {
		// Check the genid to see if it's still valid.
		if genid := atomic.LoadUint64(&pac.acc.sl.genid); genid != pac.genid {
			ok = false
			delete(c.in.pacache, string(c.pa.pacache))
		} else {
			acc = pac.acc
			r = pac.results
		}
	}

	if !ok {
		// Match correct account and sublist.
		if acc, _ = c.srv.LookupAccount(string(c.pa.account)); acc == nil {
			return nil, nil
		}

		// Match against the account sublist.
		r = acc.sl.Match(string(c.pa.subject))

		// Store in our cache
		c.in.pacache[string(c.pa.pacache)] = &perAccountCache{acc, r, atomic.LoadUint64(&acc.sl.genid)}

		// Check if we need to prune.
		if len(c.in.pacache) > maxPerAccountCacheSize {
			c.prunePerAccountCache()
		}
	}
	return acc, r
}

// Account will return the associated account for this client.
func (c *client) Account() *Account {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.acc
}

// prunePerAccountCache will prune off a random number of cache entries.
func (c *client) prunePerAccountCache() {
	n := 0
	for cacheKey := range c.in.pacache {
		delete(c.in.pacache, cacheKey)
		if n++; n > prunePerAccountCacheSize {
			break
		}
	}
}

// getAuthUser returns the auth user for the client.
func (c *client) getAuthUser() string {
	switch {
	case c.opts.Nkey != "":
		return fmt.Sprintf("Nkey %q", c.opts.Nkey)
	case c.opts.Username != "":
		return fmt.Sprintf("User %q", c.opts.Username)
	default:
		return `User "N/A"`
	}
}

// Logging functionality scoped to a client or route.

func (c *client) Errorf(format string, v ...interface{}) {
	format = fmt.Sprintf("%s - %s", c, format)
	c.srv.Errorf(format, v...)
}

func (c *client) Debugf(format string, v ...interface{}) {
	format = fmt.Sprintf("%s - %s", c, format)
	c.srv.Debugf(format, v...)
}

func (c *client) Noticef(format string, v ...interface{}) {
	format = fmt.Sprintf("%s - %s", c, format)
	c.srv.Noticef(format, v...)
}

func (c *client) Tracef(format string, v ...interface{}) {
	format = fmt.Sprintf("%s - %s", c, format)
	c.srv.Tracef(format, v...)
}

func (c *client) Warnf(format string, v ...interface{}) {
	format = fmt.Sprintf("%s - %s", c, format)
	c.srv.Warnf(format, v...)
}
