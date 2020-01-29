// Copyright 2012-2020 The NATS Authors
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
	"strconv"
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
	// ROUTER represents another server in the cluster.
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

const (
	pingProto = "PING" + _CRLF_
	pongProto = "PONG" + _CRLF_
	errProto  = "-ERR '%s'" + _CRLF_
	okProto   = "+OK" + _CRLF_
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

	// Server should not send a PING (for RTT) before the first PONG has
	// been sent to the client. However, in case some client libs don't
	// send CONNECT+PING, cap the maximum time before server can send
	// the RTT PING.
	maxNoRTTPingBeforeFirstPong = 2 * time.Second

	// For stalling fast producers
	stallClientMinDuration = 100 * time.Millisecond
	stallClientMaxDuration = time.Second
)

var readLoopReportThreshold = readLoopReport

// Represent client booleans with a bitmask
type clientFlag uint16

// Some client state represented as flags
const (
	connectReceived   clientFlag = 1 << iota // The CONNECT proto has been received
	infoReceived                             // The INFO protocol has been received
	firstPongSent                            // The first PONG has been sent
	handshakeComplete                        // For TLS clients, indicate that the handshake is complete
	flushOutbound                            // Marks client as having a flushOutbound call in progress.
	noReconnect                              // Indicate that on close, this connection should not attempt a reconnect
	closeConnection                          // Marks that closeConnection has already been called.
	writeLoopStarted                         // Marks that the writeLoop has been started.
	skipFlushOnClose                         // Marks that flushOutbound() should not be called on connection close.
	expectConnect                            // Marks if this connection is expected to send a CONNECT
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
	Revocation
)

// Some flags passed to processMsgResultsEx
const pmrNoFlag int = 0
const (
	pmrCollectQueueNames int = 1 << iota
	pmrIgnoreEmptyQueueFilter
	pmrAllowSendFromRouteToRoute
)

type client struct {
	// Here first because of use of atomics, and memory alignment.
	stats
	// Indicate if we should check gwrm or not. Since checking gwrm is done
	// when processing inbound messages and requires the lock we want to
	// check only when needed. This is set/get using atomic, so needs to
	// be memory aligned.
	cgwrt   int32
	mpay    int32
	msubs   int32
	mcl     int32
	mu      sync.Mutex
	kind    int
	cid     uint64
	opts    clientOpts
	start   time.Time
	nonce   []byte
	nc      net.Conn
	ncs     string
	out     outbound
	srv     *Server
	acc     *Account
	user    *NkeyUser
	host    string
	port    uint16
	subs    map[string]*subscription
	perms   *permissions
	replies map[string]*resp
	mperms  *msgDeny
	darray  []string
	in      readCache
	pcd     map[*client]struct{}
	atmr    *time.Timer
	ping    pinfo
	msgb    [msgScratchSize]byte
	last    time.Time
	parseState

	rtt        time.Duration
	rttStart   time.Time
	rrTracking map[string]*remoteLatency
	rrMax      int

	route *route
	gw    *gateway
	leaf  *leaf

	// To keep track of gateway replies mapping
	gwrm map[string]*gwReplyMap

	flags clientFlag // Compact booleans into a single field. Size will be increased when needed.

	debug bool
	trace bool
	echo  bool
}

// Struct for PING initiation from the server.
type pinfo struct {
	tmr  *time.Timer
	last time.Time
	out  int
}

// outbound holds pending data for a socket.
type outbound struct {
	p   []byte        // Primary write buffer
	s   []byte        // Secondary for use post flush
	nb  net.Buffers   // net.Buffers for writev IO
	sz  int32         // limit size per []byte, uses variable BufSize constants, start, min, max.
	sws int32         // Number of short writes, used for dynamic resizing.
	pb  int64         // Total pending/queued bytes.
	pm  int32         // Total pending/queued messages.
	fsp int32         // Flush signals that are pending per producer from readLoop's pcd.
	sch chan struct{} // To signal writeLoop that there is data to flush.
	wdl time.Duration // Snapshot of write deadline.
	mp  int64         // Snapshot of max pending for client.
	lft time.Duration // Last flush time for Write.
	stc chan struct{} // Stall chan we create to slow down producers on overrun, e.g. fan-in.
	lwb int32         // Last byte size of Write.
}

type perm struct {
	allow *Sublist
	deny  *Sublist
}

type permissions struct {
	sub    perm
	pub    perm
	resp   *ResponsePermission
	pcache map[string]bool
}

// This is used to dynamically track responses and reply subjects
// for dynamic permissioning.
type resp struct {
	t time.Time
	n int
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
	replyPermLimit       = 4096
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
	defaultMaxPerAccountCacheSize   = 4096
	defaultPrunePerAccountCacheSize = 256
	defaultClosedSubsCheckInterval  = 5 * time.Minute
)

var (
	maxPerAccountCacheSize   = defaultMaxPerAccountCacheSize
	prunePerAccountCacheSize = defaultPrunePerAccountCacheSize
	closedSubsCheckInterval  = defaultClosedSubsCheckInterval
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

// GetName returns the application supplied name for the connection.
func (c *client) GetName() string {
	c.mu.Lock()
	name := c.opts.Name
	c.mu.Unlock()
	return name
}

// GetOpts returns the client options provided by the application.
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
	closed  int32
}

// Indicate that this subscription is closed.
// This is used in pruning of route and gateway cache items.
func (s *subscription) close() {
	atomic.StoreInt32(&s.closed, 1)
}

// Return true if this subscription was unsubscribed
// or its connection has been closed.
func (s *subscription) isClosed() bool {
	return atomic.LoadInt32(&s.closed) == 1
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
	c.out.sch = make(chan struct{}, 1)
	opts := s.getOpts()
	// Snapshots to avoid mutex access in fast paths.
	c.out.wdl = opts.WriteDeadline
	c.out.mp = opts.MaxPending

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
		conn = ip.RemoteAddr().String()
		host, port, _ := net.SplitHostPort(conn)
		iPort, _ := strconv.Atoi(port)
		c.host, c.port = host, uint16(iPort)
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

	s := c.srv
	opts := s.getOpts()

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

	// Assign permissions.
	if user.Permissions == nil {
		// Reset perms to nil in case client previously had them.
		c.perms = nil
		c.mperms = nil
	} else {
		c.setPermissions(user.Permissions)
	}
	c.mu.Unlock()
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

func splitSubjectQueue(sq string) ([]byte, []byte, error) {
	vals := strings.Fields(strings.TrimSpace(sq))
	s := []byte(vals[0])
	var q []byte
	if len(vals) == 2 {
		q = []byte(vals[1])
	} else if len(vals) > 2 {
		return nil, nil, fmt.Errorf("invalid subject-queue %q", sq)
	}
	return s, q, nil
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
		if perms.Publish.Allow != nil {
			c.perms.pub.allow = NewSublistWithCache()
		}
		for _, pubSubject := range perms.Publish.Allow {
			sub := &subscription{subject: []byte(pubSubject)}
			c.perms.pub.allow.Insert(sub)
		}
		if len(perms.Publish.Deny) > 0 {
			c.perms.pub.deny = NewSublistWithCache()
		}
		for _, pubSubject := range perms.Publish.Deny {
			sub := &subscription{subject: []byte(pubSubject)}
			c.perms.pub.deny.Insert(sub)
		}
	}

	// Check if we are allowed to send responses.
	if perms.Response != nil {
		rp := *perms.Response
		c.perms.resp = &rp
		c.replies = make(map[string]*resp)
	}

	// Loop over subscribe permissions
	if perms.Subscribe != nil {
		var err error
		if len(perms.Subscribe.Allow) > 0 {
			c.perms.sub.allow = NewSublistWithCache()
		}
		for _, subSubject := range perms.Subscribe.Allow {
			sub := &subscription{}
			sub.subject, sub.queue, err = splitSubjectQueue(subSubject)
			if err != nil {
				c.Errorf("%s", err.Error())
				continue
			}
			c.perms.sub.allow.Insert(sub)
		}
		if len(perms.Subscribe.Deny) > 0 {
			c.perms.sub.deny = NewSublistWithCache()
			// Also hold onto this array for later.
			c.darray = perms.Subscribe.Deny
		}
		for _, subSubject := range perms.Subscribe.Deny {
			sub := &subscription{}
			sub.subject, sub.queue, err = splitSubjectQueue(subSubject)
			if err != nil {
				c.Errorf("%s", err.Error())
				continue
			}
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
	c.mperms = &msgDeny{NewSublistWithCache(), make(map[string]bool)}
	for _, sub := range c.darray {
		c.mperms.deny.Insert(&subscription{subject: []byte(sub)})
	}
}

// writeLoop is the main socket write functionality.
// Runs in its own Go routine.
func (c *client) writeLoop() {
	defer c.srv.grWG.Done()
	c.mu.Lock()
	if c.isClosed() {
		c.mu.Unlock()
		return
	}
	c.flags.set(writeLoopStarted)
	ch := c.out.sch
	c.mu.Unlock()

	// This will clear connection state and remove it from the server.
	defer c.teardownConn()

	// Used to check that we did flush from last wake up.
	waitOk := true

	// Used to limit the wait for a signal
	const maxWait = time.Second
	t := time.NewTimer(maxWait)

	var close bool

	// Main loop. Will wait to be signaled and then will use
	// buffered outbound structure for efficient writev to the underlying socket.
	for {
		c.mu.Lock()
		if close = c.flags.isSet(closeConnection); !close {
			owtf := c.out.fsp > 0 && c.out.pb < maxBufSize && c.out.fsp < maxFlushPending
			if waitOk && (c.out.pb == 0 || owtf) {
				c.mu.Unlock()

				// Reset our timer
				t.Reset(maxWait)

				// Wait on pending data.
				select {
				case <-ch:
				case <-t.C:
				}

				c.mu.Lock()
				close = c.flags.isSet(closeConnection)
			}
		}
		if close {
			c.flushAndClose(false)
			c.mu.Unlock()
			return
		}
		// Flush data
		waitOk = c.flushOutbound()
		c.mu.Unlock()
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
		if cp.flags.isSet(closeConnection) {
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
	s := c.srv
	defer s.grWG.Done()
	if c.isClosed() {
		c.mu.Unlock()
		return
	}
	nc := c.nc
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
	// Check the per-account-cache for closed subscriptions
	cpacc := c.kind == ROUTER || c.kind == GATEWAY
	// Last per-account-cache check for closed subscriptions
	lpacc := time.Now()
	c.mu.Unlock()

	defer func() {
		// These are used only in the readloop, so we can set them to nil
		// on exit of the readLoop.
		c.in.results, c.in.pacache = nil, nil
	}()

	// Start read buffer.
	b := make([]byte, c.in.rsz)

	for {
		n, err := nc.Read(b)
		// If we have any data we will try to parse and exit at the end.
		if n == 0 && err != nil {
			c.closeConnection(closedStateForErr(err))
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
			// Need to call flushClients because some of the clients have been
			// assigned messages and their "fsp" incremented, and need now to be
			// decremented and their writeLoop signaled.
			c.flushClients(0)
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
		closed := c.isClosed()

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
		if closed {
			return
		}

		// We could have had a read error from above but still read some data.
		// If so do the close here unconditionally.
		if err != nil {
			c.closeConnection(closedStateForErr(err))
			return
		}

		if cpacc && start.Sub(lpacc) >= closedSubsCheckInterval {
			c.pruneClosedSubFromPerAccountCache()
			lpacc = time.Now()
		}
	}
}

// Returns the appropriate closed state for a given read error.
func closedStateForErr(err error) ClosedState {
	if err == io.EOF {
		return ClientClosed
	}
	return ReadError
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
		// For CLIENT connections, it is possible that the readLoop calls
		// flushOutbound(). If writeLoop and readLoop compete and we are
		// here we should release the lock to reduce the risk of spinning.
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

	// Place primary on nb, assign primary to secondary, nil out nb and secondary.
	nb := c.collapsePtoNB()
	c.out.p, c.out.nb, c.out.s = c.out.s, nil, nil

	// For selecting primary replacement.
	cnb := nb

	// In case it goes away after releasing the lock.
	nc := c.nc
	attempted := c.out.pb
	apm := c.out.pm

	// Capture this (we change the value in some tests)
	wdl := c.out.wdl
	// Do NOT hold lock during actual IO.
	c.mu.Unlock()

	// flush here
	now := time.Now()
	// FIXME(dlc) - writev will do multiple IOs past 1024 on
	// most platforms, need to account for that with deadline?
	nc.SetWriteDeadline(now.Add(wdl))

	// Actual write to the socket.
	n, err := nb.WriteTo(nc)
	nc.SetWriteDeadline(time.Time{})
	lft := time.Since(now)

	// Re-acquire client lock.
	c.mu.Lock()

	if err != nil {
		// Handle timeout error (slow consumer) differently
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			if closed := c.handleWriteTimeout(n, attempted, len(cnb)); closed {
				return true
			}
		} else {
			// Other errors will cause connection to be closed.
			// For clients, report as debug but for others report as error.
			report := c.Debugf
			if c.kind != CLIENT {
				report = c.Errorf
			}
			report("Error flushing: %v", err)
			c.markConnAsClosed(WriteError, true)
			return true
		}
	}

	// Update flush time statistics.
	c.out.lft = lft
	c.out.lwb = int32(n)

	// Subtract from pending bytes and messages.
	c.out.pb -= int64(c.out.lwb)
	c.out.pm -= apm // FIXME(dlc) - this will not be totally accurate on partials.

	// Check for partial writes
	// TODO(dlc) - zero write with no error will cause lost message and the writeloop to spin.
	if int64(c.out.lwb) != attempted && n > 0 {
		c.handlePartialWrite(nb)
	} else if c.out.lwb >= c.out.sz {
		c.out.sws = 0
	}

	// Adjust based on what we wrote plus any pending.
	pt := int64(c.out.lwb) + c.out.pb

	// Adjust sz as needed downward, keeping power of 2.
	// We do this at a slower rate.
	if pt < int64(c.out.sz) && c.out.sz > minBufSize {
		c.out.sws++
		if c.out.sws > shortsToShrink {
			c.out.sz >>= 1
		}
	}
	// Adjust sz as needed upward, keeping power of 2.
	if pt > int64(c.out.sz) && c.out.sz < maxBufSize {
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
	if c.out.stc != nil && (int64(c.out.lwb) == attempted || c.out.pb < c.out.mp/2) {
		close(c.out.stc)
		c.out.stc = nil
	}

	return true
}

// This is invoked from flushOutbound() for io/timeout error (slow consumer).
// Returns a boolean to indicate if the connection has been closed or not.
// Lock is held on entry.
func (c *client) handleWriteTimeout(written, attempted int64, numChunks int) bool {
	if tlsConn, ok := c.nc.(*tls.Conn); ok {
		if !tlsConn.ConnectionState().HandshakeComplete {
			// Likely a TLSTimeout error instead...
			c.markConnAsClosed(TLSHandshakeError, true)
			// Would need to coordinate with tlstimeout()
			// to avoid double logging, so skip logging
			// here, and don't report a slow consumer error.
			return true
		}
	} else if c.flags.isSet(expectConnect) && !c.flags.isSet(connectReceived) {
		// Under some conditions, a connection may hit a slow consumer write deadline
		// before the authorization timeout. If that is the case, then we handle
		// as slow consumer though we do not increase the counter as that can be
		// misleading.
		c.markConnAsClosed(SlowConsumerWriteDeadline, true)
		return true
	}

	// Slow consumer here..
	atomic.AddInt64(&c.srv.slowConsumers, 1)
	c.Noticef("Slow Consumer Detected: WriteDeadline of %v exceeded with %d chunks of %d total bytes.",
		c.out.wdl, numChunks, attempted)

	// We always close CLIENT connections, or when nothing was written at all...
	if c.kind == CLIENT || written == 0 {
		c.markConnAsClosed(SlowConsumerWriteDeadline, true)
		return true
	}
	return false
}

// Marks this connection has closed with the given reason.
// Sets the closeConnection flag and skipFlushOnClose flag if asked.
// Depending on the kind of connection, the connection will be saved.
// If a writeLoop has been started, the final flush/close/teardown will
// be done there, otherwise flush and close of TCP connection is done here in place.
// Returns true if closed in place, flase otherwise.
// Lock is held on entry.
func (c *client) markConnAsClosed(reason ClosedState, skipFlush bool) bool {
	if c.flags.isSet(closeConnection) {
		return false
	}
	c.flags.set(closeConnection)
	if skipFlush {
		c.flags.set(skipFlushOnClose)
	}
	// Save off the connection if its a client or leafnode.
	if c.kind == CLIENT || c.kind == LEAF {
		if nc := c.nc; nc != nil && c.srv != nil {
			// TODO: May want to send events to single go routine instead
			// of creating a new go routine for each save.
			go c.srv.saveClosedClient(c, nc, reason)
		}
	}
	// If writeLoop exists, let it do the final flush, close and teardown.
	if c.flags.isSet(writeLoopStarted) {
		c.flushSignal()
		return false
	}
	// Flush (if skipFlushOnClose is not set) and close in place. If flushing,
	// use a small WriteDeadline.
	c.flushAndClose(true)
	return true
}

// flushSignal will use server to queue the flush IO operation to a pool of flushers.
// Lock must be held.
func (c *client) flushSignal() bool {
	select {
	case c.out.sch <- struct{}{}:
		return true
	default:
	}
	return false
}

func (c *client) traceMsg(msg []byte) {
	if !c.trace {
		return
	}

	maxTrace := c.srv.getOpts().MaxTracedMsgLen
	if maxTrace > 0 && (len(msg)-LEN_CR_LF) > maxTrace {
		c.Tracef("<<- MSG_PAYLOAD: [\"%s...\"]", msg[:maxTrace])
	} else {
		c.Tracef("<<- MSG_PAYLOAD: [%q]", msg[:len(msg)-LEN_CR_LF])
	}
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
		return c.processLeafnodeInfo(&info)
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
	case LEAF:
		c.Errorf("Leafnode Error %s", errStr)
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

// Returns the RTT by computing the elapsed time since now and `start`.
// On Windows VM where I (IK) run tests, time.Since() will return 0
// (I suspect some time granularity issues). So return at minimum 1ns.
func computeRTT(start time.Time) time.Duration {
	rtt := time.Since(start)
	if rtt <= 0 {
		rtt = time.Nanosecond
	}
	return rtt
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
		for !c.isClosed() {
			c.mu.Unlock()
			time.Sleep(25 * time.Millisecond)
			c.mu.Lock()
		}
		c.mu.Unlock()
		return nil
	}
	c.last = time.Now()
	// Estimate RTT to start.
	if c.kind == CLIENT {
		c.rtt = computeRTT(c.start)
	}
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
		defer s.sendAuthErrorEvent(c)

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
	// Do not keep going if closed
	if c.flags.isSet(closeConnection) {
		return false
	}

	// Assume data will not be referenced
	referenced := false
	// Add to pending bytes total.
	c.out.pb += int64(len(data))

	// Check for slow consumer via pending bytes limit.
	// ok to return here, client is going away.
	if c.kind == CLIENT && c.out.pb > c.out.mp {
		atomic.AddInt64(&c.srv.slowConsumers, 1)
		c.Noticef("Slow Consumer Detected: MaxPending of %d Exceeded", c.out.mp)
		c.markConnAsClosed(SlowConsumerPendingBytes, true)
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
func (c *client) enqueueProtoAndFlush(proto []byte, doFlush bool) {
	if c.isClosed() {
		return
	}
	c.queueOutbound(proto)
	if !(doFlush && c.flushOutbound()) {
		c.flushSignal()
	}
}

// Queues and then flushes the connection. This should only be called when
// the writeLoop cannot be started yet. Use enqueueProto() otherwise.
// Lock is held on entry.
func (c *client) sendProtoNow(proto []byte) {
	c.enqueueProtoAndFlush(proto, true)
}

// Enqueues the given protocol and signal the writeLoop if necessary.
// Lock is held on entry.
func (c *client) enqueueProto(proto []byte) {
	c.enqueueProtoAndFlush(proto, false)
}

// Assume the lock is held upon entry.
func (c *client) sendPong() {
	c.traceOutOp("PONG", nil)
	c.enqueueProto([]byte(pongProto))
}

// Used to kick off a RTT measurement for latency tracking.
func (c *client) sendRTTPing() bool {
	c.mu.Lock()
	sent := c.sendRTTPingLocked()
	c.mu.Unlock()
	return sent
}

// Used to kick off a RTT measurement for latency tracking.
// This is normally called only when the caller has checked that
// the c.rtt is 0 and wants to force an update by sending a PING.
// Client lock held on entry.
func (c *client) sendRTTPingLocked() bool {
	// Most client libs send a CONNECT+PING and wait for a PONG from the
	// server. So if firstPongSent flag is set, it is ok for server to
	// send the PING. But in case we have client libs that don't do that,
	// allow the send of the PING if more than 2 secs have elapsed since
	// the client TCP connection was accepted.
	if !c.flags.isSet(closeConnection) &&
		(c.flags.isSet(firstPongSent) || time.Since(c.start) > maxNoRTTPingBeforeFirstPong) {
		c.sendPing()
		return true
	}
	return false
}

// Assume the lock is held upon entry.
func (c *client) sendPing() {
	c.rttStart = time.Now()
	c.ping.out++
	c.traceOutOp("PING", nil)
	c.enqueueProto([]byte(pingProto))
}

// Generates the INFO to be sent to the client with the client ID included.
// info arg will be copied since passed by value.
// Assume lock is held.
func (c *client) generateClientInfoJSON(info Info) []byte {
	info.CID = c.cid
	info.MaxPayload = c.mpay
	// Generate the info json
	b, _ := json.Marshal(info)
	pcs := [][]byte{[]byte("INFO"), b, []byte(CR_LF)}
	return bytes.Join(pcs, []byte(" "))
}

func (c *client) sendErr(err string) {
	c.mu.Lock()
	c.traceOutOp("-ERR", []byte(err))
	c.enqueueProto([]byte(fmt.Sprintf(errProto, err)))
	c.mu.Unlock()
}

func (c *client) sendOK() {
	c.mu.Lock()
	c.traceOutOp("OK", nil)
	c.enqueueProto([]byte(okProto))
	c.pcd[c] = needFlush
	c.mu.Unlock()
}

func (c *client) processPing() {
	c.mu.Lock()
	c.traceInOp("PING", nil)
	if c.isClosed() {
		c.mu.Unlock()
		return
	}

	c.sendPong()

	// Record this to suppress us sending one if this
	// is within a given time interval for activity.
	c.ping.last = time.Now()

	// If not a CLIENT, we are done. Also the CONNECT should
	// have been received, but make sure it is so before proceeding
	if c.kind != CLIENT || !c.flags.isSet(connectReceived) {
		c.mu.Unlock()
		return
	}

	// If we are here, the CONNECT has been received so we know
	// if this client supports async INFO or not.
	var (
		checkInfoChange bool
		srv             = c.srv
	)
	// For older clients, just flip the firstPongSent flag if not already
	// set and we are done.
	if c.opts.Protocol < ClientProtoInfo || srv == nil {
		c.flags.setIfNotSet(firstPongSent)
	} else {
		// This is a client that supports async INFO protocols.
		// If this is the first PING (so firstPongSent is not set yet),
		// we will need to check if there was a change in cluster topology
		// or we have a different max payload. We will send this first before
		// pong since most clients do flush after connect call.
		checkInfoChange = !c.flags.isSet(firstPongSent)
	}
	c.mu.Unlock()

	if checkInfoChange {
		opts := srv.getOpts()
		srv.mu.Lock()
		c.mu.Lock()
		// Now that we are under both locks, we can flip the flag.
		// This prevents sendAsyncInfoToClients() and code here to
		// send a double INFO protocol.
		c.flags.set(firstPongSent)
		// If there was a cluster update since this client was created,
		// send an updated INFO protocol now.
		if srv.lastCURLsUpdate >= c.start.UnixNano() || c.mpay != int32(opts.MaxPayload) {
			c.enqueueProto(c.generateClientInfoJSON(srv.copyInfo()))
		}
		c.mu.Unlock()
		srv.mu.Unlock()
	}
}

func (c *client) processPong() {
	c.traceInOp("PONG", nil)
	c.mu.Lock()
	c.ping.out = 0
	c.rtt = computeRTT(c.rttStart)
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
	// If number overruns an int64, parseSize() will have returned a negative value
	if c.pa.size < 0 {
		return fmt.Errorf("processPub Bad or Missing Size: '%s'", arg)
	}
	maxPayload := atomic.LoadInt32(&c.mpay)
	// Use int64() to avoid int32 overrun...
	if maxPayload != jwt.NoLimit && int64(c.pa.size) > int64(maxPayload) {
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

func (c *client) processSub(argo []byte, noForward bool) (*subscription, error) {
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
		return nil, fmt.Errorf("processSub Parse Error: '%s'", arg)
	}

	c.mu.Lock()

	// Grab connection type, account and server info.
	kind := c.kind
	acc := c.acc
	srv := c.srv

	sid := string(sub.sid)

	// This check does not apply to SYSTEM clients (because they don't have a `nc`...)
	if kind != SYSTEM && c.isClosed() {
		c.mu.Unlock()
		return sub, nil
	}

	// Check permissions if applicable.
	if kind == CLIENT {
		// First do a pass whether queue subscription is valid. This does not necessarily
		// mean that it will not be able to plain subscribe.
		//
		// allow = ["foo"]            -> can subscribe or queue subscribe to foo using any queue
		// allow = ["foo v1"]         -> can only queue subscribe to 'foo v1', no plain subs allowed.
		// allow = ["foo", "foo v1"]  -> can subscribe to 'foo' but can only queue subscribe to 'foo v1'
		//
		if sub.queue != nil {
			if !c.canQueueSubscribe(string(sub.subject), string(sub.queue)) {
				c.mu.Unlock()
				c.subPermissionViolation(sub)
				return nil, nil
			}
		} else if !c.canSubscribe(string(sub.subject)) {
			c.mu.Unlock()
			c.subPermissionViolation(sub)
			return nil, nil
		}
	}
	// Check if we have a maximum on the number of subscriptions.
	if c.subsAtLimit() {
		c.mu.Unlock()
		c.maxSubsExceeded()
		return nil, nil
	}

	var updateGWs bool
	var err error

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
		return nil, nil
	} else if c.opts.Verbose && kind != SYSTEM {
		c.sendOK()
	}

	// No account just return.
	if acc == nil {
		return sub, nil
	}

	if err := c.addShadowSubscriptions(acc, sub); err != nil {
		c.Errorf(err.Error())
	}

	if noForward {
		return sub, nil
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
	return sub, nil
}

// If the client's account has stream imports and there are matches for
// this subscription's subject, then add shadow subscriptions in the
// other accounts that export this subject.
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

func queueMatches(queue string, qsubs [][]*subscription) bool {
	if len(qsubs) == 0 {
		return true
	}
	for _, qsub := range qsubs {
		qs := qsub[0]
		qname := string(qs.queue)

		// NOTE: '*' and '>' tokens can also be valid
		// queue names so we first check against the
		// literal name.  e.g. v1.* == v1.*
		if queue == qname || (subjectHasWildcard(qname) && subjectIsSubsetMatch(queue, qname)) {
			return true
		}
	}
	return false
}

func (c *client) canQueueSubscribe(subject, queue string) bool {
	if c.perms == nil {
		return true
	}

	allowed := true

	if c.perms.sub.allow != nil {
		r := c.perms.sub.allow.Match(subject)

		// If perms DO NOT have queue name, then psubs will be greater than
		// zero. If perms DO have queue name, then qsubs will be greater than
		// zero.
		allowed = len(r.psubs) > 0
		if len(r.qsubs) > 0 {
			// If the queue appears in the allow list, then DO allow.
			allowed = queueMatches(queue, r.qsubs)
		}
	}

	if allowed && c.perms.sub.deny != nil {
		r := c.perms.sub.deny.Match(subject)

		// If perms DO NOT have queue name, then psubs will be greater than
		// zero. If perms DO have queue name, then qsubs will be greater than
		// zero.
		allowed = len(r.psubs) == 0
		if len(r.qsubs) > 0 {
			// If the queue appears in the deny list, then DO NOT allow.
			allowed = !queueMatches(queue, r.qsubs)
		}
	}

	return allowed
}

// Low level unsubscribe for a given client.
func (c *client) unsubscribe(acc *Account, sub *subscription, force, remove bool) {
	c.mu.Lock()
	if !force && sub.max > 0 && sub.nm < sub.max {
		c.Debugf(
			"Deferring actual UNSUB(%s): %d max, %d received",
			string(sub.subject), sub.max, sub.nm)
		c.mu.Unlock()
		return
	}
	c.traceOp("<-> %s", "DELSUB", sub.sid)

	if c.kind != CLIENT && c.kind != SYSTEM {
		c.removeReplySubTimeout(sub)
	}

	// Remove accounting if requested. This will be false when we close a connection
	// with open subscriptions.
	if remove {
		delete(c.subs, string(sub.sid))
		if acc != nil {
			acc.sl.Remove(sub)
		}
	}

	// Check to see if we have shadow subscriptions.
	var updateRoute bool
	shadowSubs := sub.shadow
	sub.shadow = nil
	if len(shadowSubs) > 0 {
		updateRoute = (c.kind == CLIENT || c.kind == SYSTEM || c.kind == LEAF) && c.srv != nil
	}
	sub.close()
	c.mu.Unlock()

	// Process shadow subs if we have them.
	for _, nsub := range shadowSubs {
		if err := nsub.im.acc.sl.Remove(nsub); err != nil {
			c.Debugf("Could not remove shadow import subscription for account %q", nsub.im.acc.Name)
		} else if updateRoute {
			c.srv.updateRouteSubscriptionMap(nsub.im.acc, nsub, -1)
		}
		// Now check on leafnode updates.
		c.srv.updateLeafNodes(nsub.im.acc, nsub, -1)
	}

	// Now check to see if this was part of a respMap entry for service imports.
	if acc != nil {
		acc.checkForRespEntry(string(sub.subject))
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
		c.unsubscribe(acc, sub, false, true)
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
	ttl := stallDuration(c.out.pb, c.out.mp)
	c.mu.Unlock()
	defer c.mu.Lock()

	select {
	case <-stall:
	case <-time.After(ttl):
		producer.Debugf("Timed out of fast producer stall (%v)", ttl)
	}
}

func stallDuration(pb, mp int64) time.Duration {
	ttl := stallClientMinDuration
	if pb >= mp {
		ttl = stallClientMaxDuration
	} else if hmp := mp / 2; pb > hmp {
		bsz := hmp / 10
		additional := int64(ttl) * ((pb - hmp) / bsz)
		ttl += time.Duration(additional)
	}
	return ttl
}

// Used to treat maps as efficient set
var needFlush = struct{}{}

// deliverMsg will deliver a message to a matching subscription and its underlying client.
// We process all connection/client types. mh is the part that will be protocol/client specific.
func (c *client) deliverMsg(sub *subscription, subject, mh, msg []byte, gwrply bool) bool {
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
	if client.mperms != nil && client.checkDenySub(string(subject)) {
		client.mu.Unlock()
		return false
	}

	// This is set under the client lock using atomic because it can be
	// checked with atomic without the client lock. Here, we don't need
	// the atomic operation since we are under the lock.
	if sub.closed == 1 {
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
					if srv.gateway.enabled {
						defer srv.gatewayUpdateSubInterest(client.acc.Name, sub, -1)
					}
					defer srv.updateRouteSubscriptionMap(client.acc, sub, -1)
				}
				defer client.unsubscribe(client.acc, sub, true, true)
			} else if sub.nm > sub.max {
				client.Debugf("Auto-unsubscribe limit [%d] exceeded", sub.max)
				client.mu.Unlock()
				client.unsubscribe(client.acc, sub, true, true)
				if shouldForward {
					srv.updateRouteSubscriptionMap(client.acc, sub, -1)
					if srv.gateway.enabled {
						srv.gatewayUpdateSubInterest(client.acc.Name, sub, -1)
					}
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
		s.deliverInternalMsg(sub, c, subject, c.pa.reply, msg[:msgSize])
		return true
	}

	// If we are a client and we detect that the consumer we are
	// sending to is in a stalled state, go ahead and wait here
	// with a limit.
	if c.kind == CLIENT && client.out.stc != nil {
		client.stalledWait(c)
	}

	// Check for closed connection
	if client.isClosed() {
		client.mu.Unlock()
		return false
	}

	// Do a fast check here to see if we should be tracking this from a latency
	// perspective. This will be for a request being received for an exported service.
	// This needs to be from a non-client (otherwise tracking happens at requestor).
	//
	// Also this check captures if the original reply (c.pa.reply) is a GW routed
	// reply (since it is known to be > minReplyLen). If that is the case, we need to
	// track the binding between the routed reply and the reply set in the message
	// header (which is c.pa.reply without the GNR routing prefix).
	if client.kind == CLIENT && len(c.pa.reply) > minReplyLen {

		if gwrply {
			// Note we keep track "in" the destination client (`client`) but the
			// routed reply subject is in `c.pa.reply`. Should that change, we
			// would have to pass the "reply" in deliverMsg().
			srv.trackGWReply(client, c.pa.reply)
		}

		// If we do not have a registered RTT queue that up now.
		if client.rtt == 0 {
			client.sendRTTPingLocked()
		}
		// FIXME(dlc) - We may need to optimize this.
		// We will have tagged this with a suffix ('.T') if we are tracking. This is
		// needed from sampling. Not all will be tracked.
		if c.kind != CLIENT && client.acc.IsExportServiceTracking(string(subject)) && isTrackedReply(c.pa.reply) {
			client.trackRemoteReply(string(c.pa.reply))
		}
	}

	// Queue to outbound buffer
	client.queueOutbound(mh)
	client.queueOutbound(msg)

	client.out.pm++

	// If we are tracking dynamic publish permissions that track reply subjects,
	// do that accounting here. We only look at client.replies which will be non-nil.
	if client.replies != nil && len(c.pa.reply) > 0 {
		client.replies[string(c.pa.reply)] = &resp{time.Now(), 0}
		if len(client.replies) > replyPermLimit {
			client.pruneReplyPerms()
		}
	}

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

// This will track a remote reply for an exported service that has requested
// latency tracking.
// Lock assumed to be held.
func (c *client) trackRemoteReply(reply string) {
	if c.rrTracking == nil {
		c.rrTracking = make(map[string]*remoteLatency)
		c.rrMax = c.acc.MaxAutoExpireResponseMaps()
	}
	rl := remoteLatency{
		Account: c.acc.Name,
		ReqId:   reply,
	}
	rl.M2.RequestStart = time.Now()
	c.rrTracking[reply] = &rl
	if len(c.rrTracking) >= c.rrMax {
		c.pruneRemoteTracking()
	}
}

// pruneReplyPerms will remove any stale or expired entries
// in our reply cache. We make sure to not check too often.
func (c *client) pruneReplyPerms() {
	// Make sure we do not check too often.
	if c.perms.resp == nil {
		return
	}

	mm := c.perms.resp.MaxMsgs
	ttl := c.perms.resp.Expires
	now := time.Now()

	for k, resp := range c.replies {
		if mm > 0 && resp.n >= mm {
			delete(c.replies, k)
		} else if ttl > 0 && now.Sub(resp.t) > ttl {
			delete(c.replies, k)
		}
	}
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

// pruneRemoteTracking will prune any remote tracking objects
// that are too old. These are orphaned when a service is not
// sending reponses etc.
// Lock should be held upon entry.
func (c *client) pruneRemoteTracking() {
	ttl := c.acc.AutoExpireTTL()
	now := time.Now()
	for reply, rl := range c.rrTracking {
		if now.Sub(rl.M2.RequestStart) > ttl {
			delete(c.rrTracking, reply)
		}
	}
}

// pubAllowed checks on publish permissioning.
// Lock should not be held.
func (c *client) pubAllowed(subject string) bool {
	return c.pubAllowedFullCheck(subject, true)
}

// pubAllowedFullCheck checks on all publish permissioning depending
// on the flag for dynamic reply permissions.
func (c *client) pubAllowedFullCheck(subject string, fullCheck bool) bool {
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

	// If we are currently not allowed but we are tracking reply subjects
	// dynamically, check to see if we are allowed here but avoid pcache.
	// We need to acquire the lock though.
	if !allowed && fullCheck && c.perms.resp != nil {
		c.mu.Lock()
		if resp := c.replies[subject]; resp != nil {
			resp.n++
			// Check if we have sent too many responses.
			if c.perms.resp.MaxMsgs > 0 && resp.n > c.perms.resp.MaxMsgs {
				delete(c.replies, subject)
			} else if c.perms.resp.Expires > 0 && time.Since(resp.t) > c.perms.resp.Expires {
				delete(c.replies, subject)
			} else {
				allowed = true
			}
		}
		c.mu.Unlock()
	} else {
		// Update our cache here.
		c.perms.pcache[string(subject)] = allowed
		// Prune if needed.
		if len(c.perms.pcache) > maxPermCacheSize {
			c.prunePubPermsCache()
		}
	}
	return allowed
}

// Test whether a reply subject is a service import reply.
func isServiceReply(reply []byte) bool {
	// This function is inlined and checking this way is actually faster
	// than byte-by-byte comparison.
	return len(reply) > 3 && string(reply[:4]) == replyPrefix
}

// Test whether a reply subject is a service import or a gateway routed reply.
func isReservedReply(reply []byte) bool {
	if isServiceReply(reply) {
		return true
	}
	// Faster to check with string([:]) than byte-by-byte
	if len(reply) > gwReplyPrefixLen && string(reply[:gwReplyPrefixLen]) == gwReplyPrefix {
		return true
	}
	return false
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

	// Check that client (could be here with SYSTEM) is not publishing on reserved "$GNR" prefix.
	if c.kind == CLIENT && hasGWRoutedReplyPrefix(c.pa.subject) {
		c.pubPermissionViolation(c.pa.subject)
		return
	}

	// Check pub permissions
	if c.perms != nil && (c.perms.pub.allow != nil || c.perms.pub.deny != nil) && !c.pubAllowed(string(c.pa.subject)) {
		c.pubPermissionViolation(c.pa.subject)
		return
	}

	// Now check for reserved replies. These are used for service imports.
	if len(c.pa.reply) > 0 && isReservedReply(c.pa.reply) {
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

	// Check if this client's gateway replies map is not empty
	if atomic.LoadInt32(&c.cgwrt) > 0 && c.handleGWReplyMap(msg) {
		return
	}

	// Check to see if we need to map/route to another account.
	if c.acc.imports.services != nil {
		c.checkForImportServices(c.acc, msg)
	}

	// If we have an exported service and we are doing remote tracking, check this subject
	// to see if we need to report the latency.
	if c.rrTracking != nil {
		c.mu.Lock()
		rl := c.rrTracking[string(c.pa.subject)]
		if rl != nil {
			delete(c.rrTracking, string(c.pa.subject))
		}
		rtt := c.rtt
		c.mu.Unlock()
		if rl != nil {
			sl := &rl.M2
			// Fill this in and send it off to the other side.
			sl.AppName = c.opts.Name
			sl.ServiceLatency = time.Since(sl.RequestStart) - rtt
			sl.NATSLatency.Responder = rtt
			sl.TotalLatency = sl.ServiceLatency + rtt
			sanitizeLatencyMetric(sl)

			lsub := remoteLatencySubjectForResponse(c.pa.subject)
			c.srv.sendInternalAccountMsg(nil, lsub, &rl) // Send to SYS account
		}
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

	var qnames [][]byte

	// Check for no interest, short circuit if so.
	// This is the fanout scale.
	if len(r.psubs)+len(r.qsubs) > 0 {
		flag := pmrNoFlag
		// If there are matching queue subs and we are in gateway mode,
		// we need to keep track of the queue names the messages are
		// delivered to. When sending to the GWs, the RMSG will include
		// those names so that the remote clusters do not deliver messages
		// to their queue subs of the same names.
		if len(r.qsubs) > 0 && c.srv.gateway.enabled &&
			atomic.LoadInt64(&c.srv.gateway.totalQSubs) > 0 {
			flag |= pmrCollectQueueNames
		}
		qnames = c.processMsgResults(c.acc, r, msg, c.pa.subject, c.pa.reply, flag)
	}

	// Now deal with gateways
	if c.srv.gateway.enabled {
		c.sendMsgToGateways(c.acc, msg, c.pa.subject, c.pa.reply, qnames)
	}
}

// This is invoked knowing that this client has some GW replies
// in its map. It will check if one is find for the c.pa.subject
// and if so will process it directly (send to GWs and LEAF) and
// return true to notify the caller that the message was handled.
// If there is no mapping for the subject, false is returned.
func (c *client) handleGWReplyMap(msg []byte) bool {
	c.mu.Lock()
	rm, ok := c.gwrm[string(c.pa.subject)]
	if !ok {
		c.mu.Unlock()
		return false
	}
	// Set subject to the mapped reply subject
	c.pa.subject = []byte(rm.ms)

	var rl *remoteLatency
	var rtt time.Duration

	if c.rrTracking != nil {
		rl = c.rrTracking[string(c.pa.subject)]
		if rl != nil {
			delete(c.rrTracking, string(c.pa.subject))
		}
		rtt = c.rtt
	}
	c.mu.Unlock()

	if rl != nil {
		sl := &rl.M2
		// Fill this in and send it off to the other side.
		sl.AppName = c.opts.Name
		sl.ServiceLatency = time.Since(sl.RequestStart) - rtt
		sl.NATSLatency.Responder = rtt
		sl.TotalLatency = sl.ServiceLatency + rtt
		sanitizeLatencyMetric(sl)

		lsub := remoteLatencySubjectForResponse(c.pa.subject)
		c.srv.sendInternalAccountMsg(nil, lsub, &rl) // Send to SYS account
	}

	// Check for leaf nodes
	if c.srv.gwLeafSubs.Count() > 0 {
		if r := c.srv.gwLeafSubs.Match(string(c.pa.subject)); len(r.psubs) > 0 {
			c.processMsgResults(c.acc, r, msg, c.pa.subject, c.pa.reply, pmrNoFlag)
		}
	}
	if c.srv.gateway.enabled {
		c.sendMsgToGateways(c.acc, msg, c.pa.subject, c.pa.reply, nil)
	}

	return true
}

// This checks and process import services by doing the mapping and sending the
// message onward if applicable.
func (c *client) checkForImportServices(acc *Account, msg []byte) {
	if acc == nil || acc.imports.services == nil {
		return
	}

	acc.mu.RLock()
	si := acc.imports.services[string(c.pa.subject)]
	invalid := si != nil && si.invalid
	acc.mu.RUnlock()

	// Get the results from the other account for the mapped "to" subject.
	// If we have been marked invalid simply return here.
	if si != nil && !invalid && si.acc != nil && si.acc.sl != nil {
		var nrr []byte
		if c.pa.reply != nil {
			var latency *serviceLatency
			var tracking bool
			if tracking = shouldSample(si.latency); tracking {
				latency = si.latency
			}
			// We want to remap this to provide anonymity.
			nrr = si.acc.newServiceReply(tracking)
			si.acc.addRespServiceImport(acc, string(nrr), string(c.pa.reply), si.rt, latency)

			// Track our responses for cleanup if not auto-expire.
			if si.rt != Singleton {
				acc.addRespMapEntry(si.acc, string(c.pa.reply), string(nrr))
			} else if si.latency != nil && c.rtt == 0 {
				// We have a service import that we are tracking but have not established RTT.
				c.sendRTTPing()
			}
		}
		// FIXME(dlc) - Do L1 cache trick from above.
		rr := si.acc.sl.Match(si.to)

		// Check to see if we have no results and this is an internal serviceImport. If so we
		// need to clean that up.
		if len(rr.psubs)+len(rr.qsubs) == 0 && si.internal {
			// We may also have a response entry, so go through that way.
			si.acc.checkForRespEntry(si.to)
		}

		flags := pmrNoFlag
		// If we are a route or gateway or leafnode and this message is flipped to a queue subscriber we
		// need to handle that since the processMsgResults will want a queue filter.
		if c.kind == GATEWAY || c.kind == ROUTER || c.kind == LEAF {
			flags |= pmrIgnoreEmptyQueueFilter
		}
		if c.srv.gateway.enabled {
			flags |= pmrCollectQueueNames
			queues := c.processMsgResults(si.acc, rr, msg, []byte(si.to), nrr, flags)
			c.sendMsgToGateways(si.acc, msg, []byte(si.to), nrr, queues)
		} else {
			c.processMsgResults(si.acc, rr, msg, []byte(si.to), nrr, flags)
		}

		shouldRemove := si.ae

		// Calculate tracking info here if we are tracking this request/response.
		if si.tracking {
			if requesting := firstSubFromResult(rr); requesting != nil {
				shouldRemove = acc.sendTrackingLatency(si, requesting.client, c)
			}
		}

		if shouldRemove {
			acc.removeServiceImport(si.from)
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

	// For sending messages across routes and leafnodes.
	// Reset if we have one since we reuse this data structure.
	if c.in.rts != nil {
		c.in.rts = c.in.rts[:0]
	}

	var rplyHasGWPrefix bool
	var creply = reply

	// If the reply subject is a GW routed reply, we will perform some
	// tracking in deliverMsg(). We also want to send to the user the
	// reply without the prefix. `creply` will be set to that and be
	// used to create the message header for client connections.
	if rplyHasGWPrefix = isGWRoutedReply(reply); rplyHasGWPrefix {
		creply = reply[gwSubjectOffset:]
	}

	// Loop over all normal subscriptions that match.
	for _, sub := range r.psubs {
		// Check if this is a send to a ROUTER. We now process
		// these after everything else.
		switch sub.client.kind {
		case ROUTER:
			if (c.kind != ROUTER && !c.isSolicitedLeafNode()) || (flags&pmrAllowSendFromRouteToRoute != 0) {
				c.addSubToRouteTargets(sub)
			}
			continue
		case GATEWAY:
			// Never send to gateway from here.
			continue
		case LEAF:
			// We handle similarly to routes and use the same data structures.
			// Leaf node delivery audience is different however.
			// Also leaf nodes are always no echo, so we make sure we are not
			// going to send back to ourselves here.
			if c != sub.client && (c.kind != ROUTER || !c.isSolicitedLeafNode()) {
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
		mh := c.msgHeader(msgh[:si], sub, creply)
		c.deliverMsg(sub, subject, mh, msg, rplyHasGWPrefix)
	}

	// Set these up to optionally filter based on the queue lists.
	// This is for messages received from routes which will have directed
	// guidance on which queue groups we should deliver to.
	qf := c.pa.queues

	// For all non-client connections, we may still want to send messages to
	// leaf nodes or routes even if there are no queue filters since we collect
	// them above and do not process inline like normal clients.
	// However, do select queue subs if asked to ignore empty queue filter.
	if c.kind != CLIENT && qf == nil && flags&pmrIgnoreEmptyQueueFilter == 0 {
		goto sendToRoutesOrLeafs
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
		var rsub, sub *subscription
		var _ql [32]*subscription

		src := c.kind
		// If we just came from a route we want to prefer local subs.
		// So only select from local subs but remember the first rsub
		// in case all else fails.
		if src == ROUTER {
			ql := _ql[:0]
			for i := 0; i < len(qsubs); i++ {
				sub = qsubs[i]
				if sub.client.kind == CLIENT {
					ql = append(ql, sub)
				} else if rsub == nil {
					rsub = sub
				}
			}
			qsubs = ql
		}

		sindex := 0
		lqs := len(qsubs)
		if lqs > 1 {
			sindex = c.in.prand.Int() % lqs
		}

		// Find a subscription that is able to deliver this message starting at a random index.
		for i := 0; i < lqs; i++ {
			if sindex+i < lqs {
				sub = qsubs[sindex+i]
			} else {
				sub = qsubs[(sindex+i)%lqs]
			}
			if sub == nil {
				continue
			}

			// We have taken care of preferring local subs for a message from a route above.
			// Here we just care about a client or leaf and skipping a leaf and preferring locals.
			if dst := sub.client.kind; dst == ROUTER || dst == LEAF {
				if (src == LEAF || src == CLIENT) && dst == LEAF {
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

			var rreply = reply
			if rplyHasGWPrefix && sub.client.kind == CLIENT {
				rreply = creply
			}
			// "rreply" will be stripped of the $GNR prefix (if present)
			// for client connections only.
			mh := c.msgHeader(msgh[:si], sub, rreply)
			if c.deliverMsg(sub, subject, mh, msg, rplyHasGWPrefix) {
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

	// If no messages for routes or leafnodes return here.
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
		c.deliverMsg(rt.sub, subject, mh, msg, false)
	}
	return queues
}

func (c *client) pubPermissionViolation(subject []byte) {
	c.sendErr(fmt.Sprintf("Permissions Violation for Publish to %q", subject))
	c.Errorf("Publish Violation - %s, Subject %q", c.getAuthUser(), subject)
}

func (c *client) subPermissionViolation(sub *subscription) {
	errTxt := fmt.Sprintf("Permissions Violation for Subscription to %q", sub.subject)
	logTxt := fmt.Sprintf("Subscription Violation - %s, Subject %q, SID %s",
		c.getAuthUser(), sub.subject, sub.sid)

	if sub.queue != nil {
		errTxt = fmt.Sprintf("Permissions Violation for Subscription to %q using queue %q", sub.subject, sub.queue)
		logTxt = fmt.Sprintf("Subscription Violation - %s, Subject %q, Queue: %q, SID %s",
			c.getAuthUser(), sub.subject, sub.queue, sub.sid)
	}

	c.sendErr(errTxt)
	c.Errorf(logTxt)
}

func (c *client) replySubjectViolation(reply []byte) {
	c.sendErr(fmt.Sprintf("Permissions Violation for Publish with Reply of %q", reply))
	c.Errorf("Publish Violation - %s, Reply %q", c.getAuthUser(), reply)
}

func (c *client) processPingTimer() {
	c.mu.Lock()
	c.ping.tmr = nil
	// Check if connection is still opened
	if c.isClosed() {
		c.mu.Unlock()
		return
	}

	c.Debugf("%s Ping Timer", c.typeString())

	// If we have had activity within the PingInterval then
	// there is no need to send a ping. This can be client data
	// or if we received a ping from the other side.
	pingInterval := c.srv.getOpts().PingInterval
	now := time.Now()
	needRTT := c.rtt == 0 || now.Sub(c.rttStart) > DEFAULT_RTT_MEASUREMENT_INTERVAL

	if delta := now.Sub(c.last); delta < pingInterval && !needRTT {
		c.Debugf("Delaying PING due to client activity %v ago", delta.Round(time.Second))
	} else if delta := now.Sub(c.ping.last); delta < pingInterval && !needRTT {
		c.Debugf("Delaying PING due to remote ping %v ago", delta.Round(time.Second))
	} else {
		// Check for violation
		if c.ping.out+1 > c.srv.getOpts().MaxPingsOut {
			c.Debugf("Stale Client Connection - Closing")
			c.enqueueProto([]byte(fmt.Sprintf(errProto, "Stale Connection")))
			c.mu.Unlock()
			c.closeConnection(StaleConnection)
			return
		}
		// Send PING
		c.sendPing()
	}

	// Reset to fire again.
	c.setPingTimer()
	c.mu.Unlock()
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

// Possibly flush the connection and then close the low level connection.
// The boolean `minimalFlush` indicates if the flush operation should have a
// minimal write deadline.
// Lock is held on entry.
func (c *client) flushAndClose(minimalFlush bool) {
	if !c.flags.isSet(skipFlushOnClose) && c.out.pb > 0 {
		if minimalFlush {
			const lowWriteDeadline = 100 * time.Millisecond

			// Reduce the write deadline if needed.
			if c.out.wdl > lowWriteDeadline {
				c.out.wdl = lowWriteDeadline
			}
		}
		c.flushOutbound()
	}
	c.out.p, c.out.s = nil, nil

	// Close the low level connection. WriteDeadline need to be set
	// in case this is a TLS connection.
	if c.nc != nil {
		c.nc.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		c.nc.Close()
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
		canSub := c.canSubscribe(string(sub.subject))
		canQSub := sub.queue != nil && c.canQueueSubscribe(string(sub.subject), string(sub.queue))

		if !canSub && !canQSub {
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
		c.unsubscribe(acc, sub, true, true)
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
	if c.nc == nil || c.flags.isSet(closeConnection) {
		c.mu.Unlock()
		return
	}
	// This will set the closeConnection flag and save the connection, etc..
	// Will return true if no writeLoop was started and TCP connection was
	// closed in place, in which case we need to do the teardown.
	teardownNow := c.markConnAsClosed(reason, false)
	c.mu.Unlock()

	if teardownNow {
		c.teardownConn()
	}
}

// Clear the state of this connection and remove it from the server.
// If the connection was initiated (such as ROUTE, GATEWAY, etc..) this may trigger
// a reconnect. This function MUST be called only once per connection. It normally
// happens when the writeLoop returns, or in closeConnection() if no writeLoop has
// been started.
func (c *client) teardownConn() {
	c.mu.Lock()
	// Be consistent with the creation: for routes and gateways,
	// we use Noticef on create, so use that too for delete.
	if c.kind == ROUTER || c.kind == GATEWAY {
		c.Noticef("%s connection closed", c.typeString())
	} else { // Client and Leaf Node connections.
		c.Debugf("%s connection closed", c.typeString())
	}

	c.clearAuthTimer()
	c.clearPingTimer()
	// Unblock anyone who is potentially stalled waiting on us.
	if c.out.stc != nil {
		close(c.out.stc)
		c.out.stc = nil
	}
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
		var _subs [32]*subscription
		subs = _subs[:0]
		for _, sub := range c.subs {
			// Auto-unsubscribe subscriptions must be unsubscribed forcibly.
			sub.max = 0
			sub.close()
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
	if (kind == CLIENT || kind == LEAF) && acc != nil {
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
				// Call unsubscribe here to cleanup shadow subscriptions and such.
				c.unsubscribe(acc, sub, true, false)
				// Update route as normal for a normal subscriber.
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

// pruneClosedSubFromPerAccountCache remove entries that contain subscriptions
// that have been closed.
func (c *client) pruneClosedSubFromPerAccountCache() {
	for cacheKey, pac := range c.in.pacache {
		for _, sub := range pac.results.psubs {
			if sub.isClosed() {
				goto REMOVE
			}
		}
		for _, qsub := range pac.results.qsubs {
			for _, sub := range qsub {
				if sub.isClosed() {
					goto REMOVE
				}
			}
		}
		continue
	REMOVE:
		delete(c.in.pacache, cacheKey)
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

// isClosed returns true if either closeConnection or clearConnection
// flag have been set, or if `nc` is nil, which may happen in tests.
func (c *client) isClosed() bool {
	return c.flags.isSet(closeConnection) || c.nc == nil
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
