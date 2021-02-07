// Copyright 2020 The NATS Authors
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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nuid"
)

// References to "spec" here is from https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.pdf

const (
	mqttPacketConnect    = byte(0x10)
	mqttPacketConnectAck = byte(0x20)
	mqttPacketPub        = byte(0x30)
	mqttPacketPubAck     = byte(0x40)
	mqttPacketPubRec     = byte(0x50)
	mqttPacketPubRel     = byte(0x60)
	mqttPacketPubComp    = byte(0x70)
	mqttPacketSub        = byte(0x80)
	mqttPacketSubAck     = byte(0x90)
	mqttPacketUnsub      = byte(0xa0)
	mqttPacketUnsubAck   = byte(0xb0)
	mqttPacketPing       = byte(0xc0)
	mqttPacketPingResp   = byte(0xd0)
	mqttPacketDisconnect = byte(0xe0)
	mqttPacketMask       = byte(0xf0)
	mqttPacketFlagMask   = byte(0x0f)

	mqttProtoLevel = byte(0x4)

	// Connect flags
	mqttConnFlagReserved     = byte(0x1)
	mqttConnFlagCleanSession = byte(0x2)
	mqttConnFlagWillFlag     = byte(0x04)
	mqttConnFlagWillQoS      = byte(0x18)
	mqttConnFlagWillRetain   = byte(0x20)
	mqttConnFlagPasswordFlag = byte(0x40)
	mqttConnFlagUsernameFlag = byte(0x80)

	// Publish flags
	mqttPubFlagRetain = byte(0x01)
	mqttPubFlagQoS    = byte(0x06)
	mqttPubFlagDup    = byte(0x08)
	mqttPubQos1       = byte(0x2) // 1 << 1

	// Subscribe flags
	mqttSubscribeFlags = byte(0x2)
	mqttSubAckFailure  = byte(0x80)

	// Unsubscribe flags
	mqttUnsubscribeFlags = byte(0x2)

	// ConnAck returned codes
	mqttConnAckRCConnectionAccepted          = byte(0x0)
	mqttConnAckRCUnacceptableProtocolVersion = byte(0x1)
	mqttConnAckRCIdentifierRejected          = byte(0x2)
	mqttConnAckRCServerUnavailable           = byte(0x3)
	mqttConnAckRCBadUserOrPassword           = byte(0x4)
	mqttConnAckRCNotAuthorized               = byte(0x5)

	// Maximum payload size of a control packet
	mqttMaxPayloadSize = 0xFFFFFFF

	// Topic/Filter characters
	mqttTopicLevelSep = '/'
	mqttSingleLevelWC = '+'
	mqttMultiLevelWC  = '#'

	// This is appended to the sid of a subscription that is
	// created on the upper level subject because of the MQTT
	// wildcard '#' semantic.
	mqttMultiLevelSidSuffix = " fwc"

	// This is the prefix for NATS subscriptions subjects associated as delivery
	// subject of JS consumer. We want to make them unique so will prevent users
	// MQTT subscriptions to start with this.
	mqttSubPrefix = "$MQTT.sub."

	// MQTT Stream names prefix. Will be used to prevent users from creating
	// JS consumers on those.
	mqttStreamNamePrefix = "$MQTT_"

	// Stream name for MQTT messages on a given account
	mqttStreamName = mqttStreamNamePrefix + "msgs"

	// Stream name for MQTT retained messages on a given account
	mqttRetainedMsgsStreamName = mqttStreamNamePrefix + "rmsgs"

	// Stream name for MQTT sessions on a given account
	mqttSessionsStreamName = mqttStreamNamePrefix + "sessions"

	// Normally, MQTT server should not redeliver QoS 1 messages to clients,
	// except after client reconnects. However, NATS Server will redeliver
	// unacknowledged messages after this default interval. This can be
	// changed with the server.Options.MQTT.AckWait option.
	mqttDefaultAckWait = 30 * time.Second

	// This is the default for the outstanding number of pending QoS 1
	// messages sent to a session with QoS 1 subscriptions.
	mqttDefaultMaxAckPending = 1024
)

var (
	mqttPingResponse = []byte{mqttPacketPingResp, 0x0}
	mqttProtoName    = []byte("MQTT")
	mqttOldProtoName = []byte("MQIsdp")
)

type srvMQTT struct {
	listener     net.Listener
	authOverride bool
	sessmgr      mqttSessionManager
}

type mqttSessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*mqttAccountSessionManager // key is account name
}

type mqttAccountSessionManager struct {
	mu       sync.RWMutex
	sstream  *stream                     // stream where sessions are recorded
	mstream  *stream                     // messages stream
	rstream  *stream                     // retained messages stream
	sessions map[string]*mqttSession     // key is MQTT client ID
	sl       *Sublist                    // sublist allowing to find retained messages for given subscription
	retmsgs  map[string]*mqttRetainedMsg // retained messages
}

type mqttSession struct {
	mu       sync.Mutex
	c        *client
	subs     map[string]byte
	cons     map[string]*consumer
	stream   *stream
	sseq     uint64                          // stream sequence where this sesion is recorded
	pending  map[uint16]*mqttPending         // Key is the PUBLISH packet identifier sent to client and maps to a mqttPending record
	cpending map[*consumer]map[uint64]uint16 // For each JS consumer, the key is the stream sequence and maps to the PUBLISH packet identifier
	ppi      uint16                          // publish packet identifier
	maxp     uint16
	stalled  bool
	clean    bool
}

type mqttPersistedSession struct {
	ID    string            `json:"id,omitempty"`
	Clean bool              `json:"clean,omitempty"`
	Subs  map[string]byte   `json:"subs,omitempty"`
	Cons  map[string]string `json:"cons,omitempty"`
}

type mqttRetainedMsg struct {
	Topic  []byte `json:"topic,omitempty"`
	Msg    []byte `json:"msg,omitempty"`
	Flags  byte   `json:"flags,omitempty"`
	Source string `json:"source,omitempty"`

	// non exported
	sseq uint64
	sub  *subscription
}

type mqttSub struct {
	qos byte
	// Pending serialization of retained messages to be sent when subscription is registered
	prm *mqttWriter
	// This is the corresponding JS consumer. This is applicable to a subscription that is
	// done for QoS > 0 (the subscription attached to a JS consumer's delivery subject).
	jsCons *consumer
}

type mqtt struct {
	r    *mqttReader
	cp   *mqttConnectProto
	pp   *mqttPublish
	asm  *mqttAccountSessionManager // quick reference to account session manager, immutable after processConnect()
	sess *mqttSession               // quick reference to session, immutable after processConnect()
}

type mqttPending struct {
	sseq   uint64    // stream sequence
	dseq   uint64    // consumer delivery sequence
	dcount uint64    // consumer delivery count
	jsCons *consumer // pointer to JS consumer (to which we will call ackMsg() on with above info)
}

type mqttConnectProto struct {
	clientID string
	rd       time.Duration
	will     *mqttWill
	flags    byte
}

type mqttIOReader interface {
	io.Reader
	SetReadDeadline(time.Time) error
}

type mqttReader struct {
	reader mqttIOReader
	buf    []byte
	pos    int
}

type mqttWriter struct {
	bytes.Buffer
}

type mqttWill struct {
	topic   []byte
	subject []byte
	message []byte
	qos     byte
	retain  bool
}

type mqttFilter struct {
	filter string
	qos    byte
	// Used only for tracing and should not be used after parsing of (un)sub protocols.
	ttopic []byte
}

type mqttPublish struct {
	topic   []byte
	subject []byte
	msg     []byte
	sz      int
	pi      uint16
	flags   byte
	szb     [9]byte // MQTT max payload size is 268,435,455
}

func (s *Server) startMQTT() {
	sopts := s.getOpts()
	o := &sopts.MQTT

	var hl net.Listener
	var err error

	port := o.Port
	if port == -1 {
		port = 0
	}
	hp := net.JoinHostPort(o.Host, strconv.Itoa(port))
	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return
	}
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	hl, err = net.Listen("tcp", hp)
	if err != nil {
		s.mu.Unlock()
		s.Fatalf("Unable to listen for MQTT connections: %v", err)
		return
	}
	if port == 0 {
		o.Port = hl.Addr().(*net.TCPAddr).Port
	}
	s.mqtt.listener = hl
	scheme := "mqtt"
	if o.TLSConfig != nil {
		scheme = "tls"
	}
	s.Noticef("Listening for MQTT clients on %s://%s:%d", scheme, o.Host, o.Port)
	go s.acceptConnections(hl, "MQTT", func(conn net.Conn) { s.createMQTTClient(conn) }, nil)
	s.mu.Unlock()
}

// This is similar to createClient() but has some modifications specifi to MQTT clients.
// The comments have been kept to minimum to reduce code size. Check createClient() for
// more details.
func (s *Server) createMQTTClient(conn net.Conn) *client {
	opts := s.getOpts()

	maxPay := int32(opts.MaxPayload)
	maxSubs := int32(opts.MaxSubs)
	if maxSubs == 0 {
		maxSubs = -1
	}
	now := time.Now()

	c := &client{srv: s, nc: conn, mpay: maxPay, msubs: maxSubs, start: now, last: now, mqtt: &mqtt{}}
	c.mqtt.pp = &mqttPublish{}
	// MQTT clients don't send NATS CONNECT protocols. So make it an "echo"
	// client, but disable verbose and pedantic (by not setting them).
	c.opts.Echo = true

	c.registerWithAccount(s.globalAccount())

	s.mu.Lock()
	// Check auth, override if applicable.
	authRequired := s.info.AuthRequired || s.mqtt.authOverride
	s.totalClients++
	s.mu.Unlock()

	c.mu.Lock()
	if authRequired {
		c.flags.set(expectConnect)
	}
	c.initClient()
	c.Debugf("Client connection created")
	c.mu.Unlock()

	s.mu.Lock()
	if !s.running || s.ldm {
		if s.shutdown {
			conn.Close()
		}
		s.mu.Unlock()
		return c
	}

	if opts.MaxConn > 0 && len(s.clients) >= opts.MaxConn {
		s.mu.Unlock()
		c.maxConnExceeded()
		return nil
	}
	s.clients[c.cid] = c

	tlsRequired := opts.MQTT.TLSConfig != nil
	s.mu.Unlock()

	c.mu.Lock()

	// In case connection has already been closed
	if c.isClosed() {
		c.mu.Unlock()
		c.closeConnection(WriteError)
		return nil
	}

	var pre []byte
	if tlsRequired && opts.AllowNonTLS {
		pre = make([]byte, 4)
		c.nc.SetReadDeadline(time.Now().Add(secondsToDuration(opts.MQTT.TLSTimeout)))
		n, _ := io.ReadFull(c.nc, pre[:])
		c.nc.SetReadDeadline(time.Time{})
		pre = pre[:n]
		if n > 0 && pre[0] == 0x16 {
			tlsRequired = true
		} else {
			tlsRequired = false
		}
	}

	if tlsRequired {
		if len(pre) > 0 {
			c.nc = &tlsMixConn{c.nc, bytes.NewBuffer(pre)}
			pre = nil
		}

		// Perform server-side TLS handshake.
		if err := c.doTLSServerHandshake("mqtt", opts.MQTT.TLSConfig, opts.MQTT.TLSTimeout); err != nil {
			c.mu.Unlock()
			return nil
		}
	}

	if authRequired {
		timeout := opts.AuthTimeout
		// Possibly override with MQTT specific value.
		if opts.MQTT.AuthTimeout != 0 {
			timeout = opts.MQTT.AuthTimeout
		}
		c.setAuthTimer(secondsToDuration(timeout))
	}

	// No Ping timer for MQTT clients...

	s.startGoRoutine(func() { c.readLoop(pre) })
	s.startGoRoutine(func() { c.writeLoop() })

	if tlsRequired {
		c.Debugf("TLS handshake complete")
		cs := c.nc.(*tls.Conn).ConnectionState()
		c.Debugf("TLS version %s, cipher suite %s", tlsVersion(cs.Version), tlsCipher(cs.CipherSuite))
	}

	c.mu.Unlock()

	return c
}

// Given the mqtt options, we check if any auth configuration
// has been provided. If so, possibly create users/nkey users and
// store them in s.mqtt.users/nkeys.
// Also update a boolean that indicates if auth is required for
// mqtt clients.
// Server lock is held on entry.
func (s *Server) mqttConfigAuth(opts *MQTTOpts) {
	mqtt := &s.mqtt
	// If any of those is specified, we consider that there is an override.
	mqtt.authOverride = opts.Username != _EMPTY_ || opts.Token != _EMPTY_ || opts.NoAuthUser != _EMPTY_
}

// Validate the mqtt related options.
func validateMQTTOptions(o *Options) error {
	mo := &o.MQTT
	// If no port is defined, we don't care about other options
	if mo.Port == 0 {
		return nil
	}
	// If there is a NoAuthUser, we need to have Users defined and
	// the user to be present.
	if mo.NoAuthUser != _EMPTY_ {
		if err := validateNoAuthUser(o, mo.NoAuthUser); err != nil {
			return err
		}
	}
	// Token/Username not possible if there are users/nkeys
	if len(o.Users) > 0 || len(o.Nkeys) > 0 {
		if mo.Username != _EMPTY_ {
			return fmt.Errorf("mqtt authentication username not compatible with presence of users/nkeys")
		}
		if mo.Token != _EMPTY_ {
			return fmt.Errorf("mqtt authentication token not compatible with presence of users/nkeys")
		}
	}
	if mo.AckWait < 0 {
		return fmt.Errorf("ack wait must be a positive value")
	}
	return nil
}

// Returns true if this connection is from a MQTT client.
// Lock held on entry.
func (c *client) isMqtt() bool {
	return c.mqtt != nil
}

// Parse protocols inside the given buffer.
// This is invoked from the readLoop.
func (c *client) mqttParse(buf []byte) error {
	c.mu.Lock()
	s := c.srv
	trace := c.trace
	connected := c.flags.isSet(connectReceived)
	mqtt := c.mqtt
	r := mqtt.r
	var rd time.Duration
	if mqtt.cp != nil {
		rd = mqtt.cp.rd
		if rd > 0 {
			r.reader.SetReadDeadline(time.Time{})
		}
	}
	c.mu.Unlock()

	r.reset(buf)

	var err error
	var b byte
	var pl int

	for err == nil && r.hasMore() {

		// Read packet type and flags
		if b, err = r.readByte("packet type"); err != nil {
			break
		}

		// Packet type
		pt := b & mqttPacketMask

		// If client was not connected yet, the first packet must be
		// a mqttPacketConnect otherwise we fail the connection.
		if !connected && pt != mqttPacketConnect {
			err = errors.New("not connected")
			break
		}

		if pl, err = r.readPacketLen(); err != nil {
			break
		}

		switch pt {
		case mqttPacketPub:
			pp := c.mqtt.pp
			pp.flags = b & mqttPacketFlagMask
			err = c.mqttParsePub(r, pl, pp)
			if trace {
				c.traceInOp("PUBLISH", errOrTrace(err, mqttPubTrace(pp)))
				if err == nil {
					c.mqttTraceMsg(pp.msg)
				}
			}
			if err == nil {
				s.mqttProcessPub(c, pp)
				if pp.pi > 0 {
					c.mqttEnqueuePubAck(pp.pi)
					if trace {
						c.traceOutOp("PUBACK", []byte(fmt.Sprintf("pi=%v", pp.pi)))
					}
				}
			}
		case mqttPacketPubAck:
			var pi uint16
			pi, err = mqttParsePubAck(r, pl)
			if trace {
				c.traceInOp("PUBACK", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				c.mqttProcessPubAck(pi)
			}
		case mqttPacketSub:
			var pi uint16 // packet identifier
			var filters []*mqttFilter
			var subs []*subscription
			pi, filters, err = c.mqttParseSubs(r, b, pl)
			if trace {
				c.traceInOp("SUBSCRIBE", errOrTrace(err, mqttSubscribeTrace(pi, filters)))
			}
			if err == nil {
				subs, err = c.mqttProcessSubs(filters)
				if err == nil && trace {
					c.traceOutOp("SUBACK", []byte(fmt.Sprintf("pi=%v", pi)))
				}
			}
			if err == nil {
				c.mqttEnqueueSubAck(pi, filters)
				c.mqttSendRetainedMsgsToNewSubs(subs)
			}
		case mqttPacketUnsub:
			var pi uint16 // packet identifier
			var filters []*mqttFilter
			pi, filters, err = c.mqttParseUnsubs(r, b, pl)
			if trace {
				c.traceInOp("UNSUBSCRIBE", errOrTrace(err, mqttUnsubscribeTrace(pi, filters)))
			}
			if err == nil {
				err = c.mqttProcessUnsubs(filters)
				if err == nil && trace {
					c.traceOutOp("UNSUBACK", []byte(fmt.Sprintf("pi=%v", pi)))
				}
			}
			if err == nil {
				c.mqttEnqueueUnsubAck(pi)
			}
		case mqttPacketPing:
			if trace {
				c.traceInOp("PINGREQ", nil)
			}
			c.mqttEnqueuePingResp()
			if trace {
				c.traceOutOp("PINGRESP", nil)
			}
		case mqttPacketConnect:
			// It is an error to receive a second connect packet
			if connected {
				err = errors.New("second connect packet")
				break
			}
			var rc byte
			var cp *mqttConnectProto
			var sessp bool
			rc, cp, err = c.mqttParseConnect(r, pl)
			if trace && cp != nil {
				c.traceInOp("CONNECT", errOrTrace(err, c.mqttConnectTrace(cp)))
			}
			if rc != 0 {
				c.mqttEnqueueConnAck(rc, sessp)
				if trace {
					c.traceOutOp("CONNACK", []byte(fmt.Sprintf("sp=%v rc=%v", sessp, rc)))
				}
			} else if err == nil {
				if err = s.mqttProcessConnect(c, cp, trace); err == nil {
					connected = true
					rd = cp.rd
				}
			}
		case mqttPacketDisconnect:
			if trace {
				c.traceInOp("DISCONNECT", nil)
			}
			// Normal disconnect, we need to discard the will.
			// Spec [MQTT-3.1.2-8]
			c.mu.Lock()
			if c.mqtt.cp != nil {
				c.mqtt.cp.will = nil
			}
			c.mu.Unlock()
			c.closeConnection(ClientClosed)
			return nil
		case mqttPacketPubRec:
			fallthrough
		case mqttPacketPubRel:
			fallthrough
		case mqttPacketPubComp:
			err = fmt.Errorf("protocol %d not supported", pt>>4)
		default:
			err = fmt.Errorf("received unknown packet type %d", pt>>4)
		}
	}
	if err == nil && rd > 0 {
		r.reader.SetReadDeadline(time.Now().Add(rd))
	}
	return err
}

func (c *client) mqttTraceMsg(msg []byte) {
	maxTrace := c.srv.getOpts().MaxTracedMsgLen
	if maxTrace > 0 && len(msg) > maxTrace {
		c.Tracef("<<- MSG_PAYLOAD: [\"%s...\"]", msg[:maxTrace])
	} else {
		c.Tracef("<<- MSG_PAYLOAD: [%q]", msg)
	}
}

// Update the session (possibly remove it) of this disconnected client.
func (s *Server) mqttHandleClosedClient(c *client) {
	c.mu.Lock()
	cp := c.mqtt.cp
	accName := c.acc.Name
	c.mu.Unlock()
	if cp == nil {
		return
	}
	sm := &s.mqtt.sessmgr
	sm.mu.RLock()
	asm, ok := sm.sessions[accName]
	sm.mu.RUnlock()
	if !ok {
		return
	}

	asm.mu.Lock()
	defer asm.mu.Unlock()
	es, ok := asm.sessions[cp.clientID]
	// If not found, ignore.
	if !ok {
		return
	}
	es.mu.Lock()
	defer es.mu.Unlock()
	// If this client is not the currently registered client, ignore.
	if es.c != c {
		return
	}
	// It the session was created with "clean session" flag, we cleanup
	// when the client disconnects.
	if es.clean {
		es.clear()
		delete(asm.sessions, cp.clientID)
	} else {
		// Clear the client from the session, but session stays.
		es.c = nil
	}
}

// Updates the MaxAckPending for all MQTT sessions, updating the
// JetStream consumers and updating their max ack pending and forcing
// a expiration of pending messages.
func (s *Server) mqttUpdateMaxAckPending(newmaxp uint16) {
	msm := &s.mqtt.sessmgr
	s.accounts.Range(func(k, _ interface{}) bool {
		accName := k.(string)
		msm.mu.RLock()
		asm := msm.sessions[accName]
		msm.mu.RUnlock()
		if asm == nil {
			// Move to next account
			return true
		}
		asm.mu.RLock()
		for _, sess := range asm.sessions {
			sess.mu.Lock()
			sess.maxp = newmaxp
			// Do not check for sess.stalled here because due to original
			// consumer's maxp it is possible that the consumer was stalled
			// and MQTT code did not have to stall the session.
			for _, cons := range sess.cons {
				cons.mu.Lock()
				cons.maxp = int(newmaxp)
				cons.forceExpirePending()
				cons.mu.Unlock()
			}
			sess.mu.Unlock()
		}
		asm.mu.RUnlock()
		return true
	})
}

//////////////////////////////////////////////////////////////////////////////
//
// Sessions Manager related functions
//
//////////////////////////////////////////////////////////////////////////////

// Returns the MQTT sessions manager for a given account.
// If new, creates the required JetStream streams/consumers
// for handling of sessions and messages.
func (sm *mqttSessionManager) getOrCreateAccountSessionManager(clientID string, c *client) (*mqttAccountSessionManager, error) {
	c.mu.Lock()
	acc := c.acc
	accName := acc.GetName()
	c.mu.Unlock()

	sm.mu.RLock()
	asm, ok := sm.sessions[accName]
	sm.mu.RUnlock()

	if ok {
		return asm, nil
	}

	// Not found, now take the write lock and check again
	sm.mu.Lock()
	defer sm.mu.Unlock()
	asm, ok = sm.sessions[accName]
	if ok {
		return asm, nil
	}
	// First check that we have JS enabled for this account.
	// TODO: Since we check only when creating a session manager for this
	// account, would probably need to do some cleanup if JS can be disabled
	// on config reload.
	if !acc.JetStreamEnabled() {
		return nil, fmt.Errorf("JetStream must be enabled for account %q used by MQTT client ID %q",
			accName, clientID)
	}
	// Need to create one here.
	asm = &mqttAccountSessionManager{sessions: make(map[string]*mqttSession)}
	if err := asm.init(acc, c); err != nil {
		return nil, err
	}
	sm.sessions[accName] = asm
	return asm, nil
}

//////////////////////////////////////////////////////////////////////////////
//
// Account Sessions Manager related functions
//
//////////////////////////////////////////////////////////////////////////////

// Creates JS streams/consumers for handling of sessions and messages for this
// account. Note that lookup are performed in case we are in a restart
// situation and the account is loaded for the first time but had state on disk.
//
// Global session manager lock is held on entry.
func (as *mqttAccountSessionManager) init(acc *Account, c *client) error {
	opts := c.srv.getOpts()
	var err error
	// Start with sessions stream
	as.sstream, err = acc.lookupStream(mqttSessionsStreamName)
	if err != nil {
		as.sstream, err = acc.addStream(&StreamConfig{
			Subjects:       []string{},
			Name:           mqttSessionsStreamName,
			Storage:        FileStorage,
			Retention:      InterestPolicy,
			internal:       true,
			allowNoSubject: true,
		})
		if err != nil {
			return fmt.Errorf("unable to create sessions stream for MQTT account %q: %v", acc.GetName(), err)
		}
	}
	// Create the stream for the messages.
	as.mstream, err = acc.lookupStream(mqttStreamName)
	if err != nil {
		as.mstream, err = acc.addStream(&StreamConfig{
			Subjects:       []string{},
			Name:           mqttStreamName,
			Storage:        FileStorage,
			Retention:      InterestPolicy,
			internal:       true,
			allowNoSubject: true,
		})
		if err != nil {
			return fmt.Errorf("unable to create messages stream for MQTT account %q: %v", acc.GetName(), err)
		}
	}
	// Create the stream for retained messages.
	as.rstream, err = acc.lookupStream(mqttRetainedMsgsStreamName)
	if err != nil {
		as.rstream, err = acc.addStream(&StreamConfig{
			Subjects:       []string{},
			Name:           mqttRetainedMsgsStreamName,
			Storage:        FileStorage,
			Retention:      InterestPolicy,
			internal:       true,
			allowNoSubject: true,
		})
		if err != nil {
			return fmt.Errorf("unable to create retained messages stream for MQTT account %q: %v", acc.GetName(), err)
		}
	}
	// Now recover all sessions (in case it did already exist)
	if state := as.sstream.state(); state.Msgs > 0 {
		for seq := state.FirstSeq; seq <= state.LastSeq; seq++ {
			_, _, content, _, err := as.sstream.store.LoadMsg(seq)
			if err != nil {
				if err != errDeletedMsg {
					c.Errorf("Error loading session record at sequence %v: %v", seq, err)
				}
				continue
			}
			ps := &mqttPersistedSession{}
			if err := json.Unmarshal(content, ps); err != nil {
				c.Errorf("Error unmarshaling session record at sequence %v: %v", seq, err)
				continue
			}
			if as.sessions == nil {
				as.sessions = make(map[string]*mqttSession)
			}
			es, ok := as.sessions[ps.ID]
			if ok && es.sseq != 0 {
				as.sstream.deleteMsg(es.sseq)
			} else if !ok {
				es = mqttSessionCreate(opts)
				es.stream = as.sstream
				as.sessions[ps.ID] = es
			}
			es.sseq = seq
			es.clean = ps.Clean
			es.subs = ps.Subs
			if l := len(ps.Cons); l > 0 {
				if es.cons == nil {
					es.cons = make(map[string]*consumer, l)
				}
				for sid, name := range ps.Cons {
					if cons := as.mstream.lookupConsumer(name); cons != nil {
						es.cons[sid] = cons
					}
				}
			}
		}
	}
	// Finally, recover retained messages.
	if state := as.rstream.state(); state.Msgs > 0 {
		for seq := state.FirstSeq; seq <= state.LastSeq; seq++ {
			subject, _, content, _, err := as.rstream.store.LoadMsg(seq)
			if err != nil {
				if err != errDeletedMsg {
					c.Errorf("Error loading retained message at sequence %v: %v", seq, err)
				}
				continue
			}
			rm := &mqttRetainedMsg{}
			if err := json.Unmarshal(content, &rm); err != nil {
				c.Errorf("Error unmarshaling retained message on subject %q, sequence %v: %v", subject, seq, err)
				continue
			}
			rm = as.handleRetainedMsg(subject, rm)
			rm.sseq = seq
		}
	}
	return nil
}

// Add/Replace this message from the retained messages map.
// Returns the retained message actually stored in the map, which means that
// it may be different from the given `rm`.
//
// Account session manager lock held on entry.
func (as *mqttAccountSessionManager) handleRetainedMsg(key string, rm *mqttRetainedMsg) *mqttRetainedMsg {
	if as.retmsgs == nil {
		as.retmsgs = make(map[string]*mqttRetainedMsg)
		as.sl = NewSublistWithCache()
	} else {
		// Check if we already had one. If so, update the existing one.
		if erm, exists := as.retmsgs[key]; exists {
			erm.Msg = rm.Msg
			erm.Flags = rm.Flags
			erm.Source = rm.Source
			return erm
		}
	}
	rm.sub = &subscription{subject: []byte(key)}
	as.retmsgs[key] = rm
	as.sl.Insert(rm.sub)
	return rm
}

// Process subscriptions for the given session/client.
//
// When `fromSubProto` is false, it means that this is invoked from the CONNECT
// protocol, when restoring subscriptions that were saved for this session.
// In that case, there is no need to update the session record.
//
// When `fromSubProto` is true, it means that this call is invoked from the
// processing of the SUBSCRIBE protocol, which means that the session needs to
// be updated. It also means that if a subscription on same subject with same
// QoS already exist, we should not be recreating the subscription/JS durable,
// since it was already done when processing the CONNECT protocol.
//
// Account session manager lock held on entry.
// Session lock held when `fromSubProto` is false.
func (as *mqttAccountSessionManager) processSubs(sess *mqttSession, clientID string, c *client,
	filters []*mqttFilter, fromSubProto, trace bool) ([]*subscription, error) {

	if fromSubProto {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.c != c {
			return nil, fmt.Errorf("client %q no longer registered with MQTT session", clientID)
		}
	}

	addJSConsToSess := func(sid string, cons *consumer) {
		if cons == nil {
			return
		}
		if sess.cons == nil {
			sess.cons = make(map[string]*consumer)
		}
		sess.cons[sid] = cons
	}

	setupSub := func(sub *subscription, qos byte) {
		if sub.mqtt == nil {
			sub.mqtt = &mqttSub{}
		}
		sub.mqtt.qos = qos
		if fromSubProto {
			as.serializeRetainedMsgsForSub(sess, c, sub, trace)
		}
	}

	subs := make([]*subscription, 0, len(filters))
	for _, f := range filters {
		if f.qos > 1 {
			f.qos = 1
		}
		subject := f.filter
		sid := subject

		if strings.HasPrefix(subject, mqttSubPrefix) {
			f.qos = mqttSubAckFailure
			continue
		}

		var jscons *consumer
		var jssub *subscription

		// Note that if a subscription already exists on this subject,
		// the existing sub is returned. Need to update the qos.
		sub, err := c.processSub([]byte(subject), nil, []byte(sid), mqttDeliverMsgCb, false)
		if err == nil {
			setupSub(sub, f.qos)
			// This will create (if not already exist) a JS consumer for subscriptions
			// of QoS >= 1. But if a JS consumer already exists and the subscription
			// for same subject is now a QoS==0, then the JS consumer will be deleted.
			jscons, jssub, err = c.mqttProcessJSConsumer(sess, as.mstream,
				subject, sid, f.qos, fromSubProto)
		}
		if err != nil {
			c.Errorf("error subscribing to %q: err=%v", subject, err)
			f.qos = mqttSubAckFailure
			c.mqttCleanupFailedSub(sub, jscons, jssub)
			continue
		}
		if mqttNeedSubForLevelUp(subject) {
			var fwjscons *consumer
			var fwjssub *subscription
			var fwcsub *subscription

			// Say subject is "foo.>", remove the ".>" so that it becomes "foo"
			fwcsubject := subject[:len(subject)-2]
			// Change the sid to "foo fwc"
			fwcsid := fwcsubject + mqttMultiLevelSidSuffix
			// See note above about existing subscription.
			fwcsub, err = c.processSub([]byte(fwcsubject), nil, []byte(fwcsid), mqttDeliverMsgCb, false)
			if err == nil {
				setupSub(fwcsub, f.qos)
				fwjscons, fwjssub, err = c.mqttProcessJSConsumer(sess, as.mstream,
					fwcsubject, fwcsid, f.qos, fromSubProto)
			}
			if err != nil {
				c.Errorf("error subscribing to %q: err=%v", fwcsubject, err)
				f.qos = mqttSubAckFailure
				c.mqttCleanupFailedSub(sub, jscons, jssub)
				c.mqttCleanupFailedSub(fwcsub, fwjscons, fwjssub)
				continue
			}
			subs = append(subs, fwcsub)
			addJSConsToSess(fwcsid, fwjscons)
		}
		subs = append(subs, sub)
		addJSConsToSess(sid, jscons)
	}
	var err error
	if fromSubProto {
		err = sess.update(clientID, filters, true)
	}
	return subs, err
}

// Retained publish messages matching this subscription are serialized in the
// subscription's `prm` mqtt writer. This buffer will be queued for outbound
// after the subscription is processed and SUBACK is sent or possibly when
// server processes an incoming published message matching the newly
// registered subscription.
//
// Account session manager lock held on entry.
// Session lock held on entry
func (as *mqttAccountSessionManager) serializeRetainedMsgsForSub(sess *mqttSession, c *client, sub *subscription, trace bool) {
	if len(as.retmsgs) > 0 {
		var rmsa [64]*mqttRetainedMsg
		rms := rmsa[:0]

		as.getRetainedPublishMsgs(string(sub.subject), &rms)
		for _, rm := range rms {
			if sub.mqtt.prm == nil {
				sub.mqtt.prm = &mqttWriter{}
			}
			prm := sub.mqtt.prm
			pi := sess.getPubAckIdentifier(mqttGetQoS(rm.Flags), sub)
			// Need to use the subject for the retained message, not the `sub` subject.
			// We can find the published retained message in rm.sub.subject.
			flags := mqttSerializePublishMsg(prm, pi, false, true, rm.Topic, rm.Msg)
			if trace {
				pp := mqttPublish{
					topic: rm.Topic,
					flags: flags,
					pi:    pi,
					sz:    len(rm.Msg),
				}
				c.traceOutOp("PUBLISH", []byte(mqttPubTrace(&pp)))
			}
		}
	}
}

// Returns in the provided slice all publish retained message records that
// match the given subscription's `subject` (which could have wildcards).
//
// Account session manager lock held on entry.
func (as *mqttAccountSessionManager) getRetainedPublishMsgs(subject string, rms *[]*mqttRetainedMsg) {
	result := as.sl.ReverseMatch(subject)
	if len(result.psubs) == 0 {
		return
	}
	for _, sub := range result.psubs {
		// Since this is a reverse match, the subscription objects here
		// contain literals corresponding to the published subjects.
		if rm, ok := as.retmsgs[string(sub.subject)]; ok {
			*rms = append(*rms, rm)
		}
	}
}

//////////////////////////////////////////////////////////////////////////////
//
// MQTT session related functions
//
//////////////////////////////////////////////////////////////////////////////

// Returns a new mqttSession object with max ack pending set based on
// option or use mqttDefaultMaxAckPending if no option set.
func mqttSessionCreate(opts *Options) *mqttSession {
	maxp := opts.MQTT.MaxAckPending
	if maxp == 0 {
		maxp = mqttDefaultMaxAckPending
	}
	return &mqttSession{maxp: maxp}
}

// Persists a session. Note that if the session's current client does not match
// the given client, nothing is done.
//
// Lock held on entry.
func (sess *mqttSession) save(clientID string) error {
	ps := mqttPersistedSession{
		ID:    clientID,
		Clean: sess.clean,
		Subs:  sess.subs,
	}
	if l := len(sess.cons); l > 0 {
		cons := make(map[string]string, l)
		for sid, jscons := range sess.cons {
			cons[sid] = jscons.String()
		}
		ps.Cons = cons
	}
	sessBytes, _ := json.Marshal(&ps)
	newSeq, _, err := sess.stream.store.StoreMsg("sessions", nil, sessBytes)
	if err != nil {
		return err
	}
	if sess.sseq != 0 {
		sess.stream.deleteMsg(sess.sseq)
	}
	sess.sseq = newSeq
	return nil
}

// Delete JS consumers for this session and delete the persisted session from
// the stream.
//
// Lock held on entry.
func (sess *mqttSession) clear() {
	for consName, cons := range sess.cons {
		delete(sess.cons, consName)
		cons.delete()
	}
	if sess.stream != nil && sess.sseq != 0 {
		sess.stream.deleteMsg(sess.sseq)
		sess.sseq = 0
	}
	sess.subs, sess.pending, sess.cpending = nil, nil, nil
}

// This will update the session record for this client in the account's MQTT
// sessions stream if the session had any change in the subscriptions.
//
// Lock held on entry.
func (sess *mqttSession) update(clientID string, filters []*mqttFilter, add bool) error {
	// Evaluate if we need to persist anything.
	var needUpdate bool
	for _, f := range filters {
		if add {
			if f.qos == mqttSubAckFailure {
				continue
			}
			if qos, ok := sess.subs[f.filter]; !ok || qos != f.qos {
				if sess.subs == nil {
					sess.subs = make(map[string]byte)
				}
				sess.subs[f.filter] = f.qos
				needUpdate = true
			}
		} else {
			if _, ok := sess.subs[f.filter]; ok {
				delete(sess.subs, f.filter)
				needUpdate = true
			}
		}
	}
	var err error
	if needUpdate {
		err = sess.save(clientID)
	}
	return err
}

// If both pQos and sub.mqtt.qos are > 0, then this will return the next
// packet identifier to use for a published message.
//
// Lock held on entry
func (sess *mqttSession) getPubAckIdentifier(pQos byte, sub *subscription) uint16 {
	pi, _ := sess.trackPending(pQos, _EMPTY_, sub)
	return pi
}

// If publish message QoS (pQos) and the subscription's QoS are both at least 1,
// this function will assign a packet identifier (pi) and will keep track of
// the pending ack. If the message has already been redelivered (reply != ""),
// the returned boolean will be `true`.
//
// Lock held on entry
func (sess *mqttSession) trackPending(pQos byte, reply string, sub *subscription) (uint16, bool) {
	if pQos == 0 || sub.mqtt.qos == 0 {
		return 0, false
	}
	var dup bool
	var pi uint16

	bumpPI := func() uint16 {
		var avail bool
		next := sess.ppi
		for i := 0; i < 0xFFFF; i++ {
			next++
			if next == 0 {
				next = 1
			}
			if _, used := sess.pending[next]; !used {
				sess.ppi = next
				avail = true
				break
			}
		}
		if !avail {
			return 0
		}
		return sess.ppi
	}

	if reply == _EMPTY_ || sub.mqtt.jsCons == nil {
		return bumpPI(), false
	}

	// Here, we have an ACK subject and a JS consumer...
	jsCons := sub.mqtt.jsCons
	if sess.pending == nil {
		sess.pending = make(map[uint16]*mqttPending)
		sess.cpending = make(map[*consumer]map[uint64]uint16)
	}
	// Get the stream sequence and other from the ack reply subject
	sseq, dseq, dcount := ackReplyInfo(reply)

	var pending *mqttPending
	// For this JS consumer, check to see if we already have sseq->pi
	sseqToPi, ok := sess.cpending[jsCons]
	if !ok {
		sseqToPi = make(map[uint64]uint16)
		sess.cpending[jsCons] = sseqToPi
	} else if pi, ok = sseqToPi[sseq]; ok {
		// If we already have a pi, get the ack so we update it.
		// We will reuse the save packet identifier (pi).
		pending = sess.pending[pi]
	}
	if pi == 0 {
		// sess.maxp will always have a value > 0.
		if len(sess.pending) >= int(sess.maxp) {
			// Indicate that we did not assign a packet identifier.
			// The caller will not send the message to the subscription
			// and JS will redeliver later, based on consumer's AckWait.
			sess.stalled = true
			return 0, false
		}
		pi = bumpPI()
		sseqToPi[sseq] = pi
	}
	if pending == nil {
		pending = &mqttPending{jsCons: jsCons, sseq: sseq}
		sess.pending[pi] = pending
	}
	// Update pending with consumer delivery sequence and count
	pending.dseq, pending.dcount = dseq, dcount
	// If redelivery, return DUP flag
	if dcount > 1 {
		dup = true
	}
	return pi, dup
}

//////////////////////////////////////////////////////////////////////////////
//
// CONNECT protocol related functions
//
//////////////////////////////////////////////////////////////////////////////

// Parse the MQTT connect protocol
func (c *client) mqttParseConnect(r *mqttReader, pl int) (byte, *mqttConnectProto, error) {

	// Make sure that we have the expected length in the buffer,
	// and if not, this will read it from the underlying reader.
	if err := r.ensurePacketInBuffer(pl); err != nil {
		return 0, nil, err
	}

	// Protocol name
	proto, err := r.readBytes("protocol name", false)
	if err != nil {
		return 0, nil, err
	}

	// Spec [MQTT-3.1.2-1]
	if !bytes.Equal(proto, mqttProtoName) {
		// Check proto name against v3.1 to report better error
		if bytes.Equal(proto, mqttOldProtoName) {
			return 0, nil, fmt.Errorf("older protocol %q not supported", proto)
		}
		return 0, nil, fmt.Errorf("expected connect packet with protocol name %q, got %q", mqttProtoName, proto)
	}

	// Protocol level
	level, err := r.readByte("protocol level")
	if err != nil {
		return 0, nil, err
	}
	// Spec [MQTT-3.1.2-2]
	if level != mqttProtoLevel {
		return mqttConnAckRCUnacceptableProtocolVersion, nil, fmt.Errorf("unacceptable protocol version of %v", level)
	}

	cp := &mqttConnectProto{}
	// Connect flags
	cp.flags, err = r.readByte("flags")
	if err != nil {
		return 0, nil, err
	}

	// Spec [MQTT-3.1.2-3]
	if cp.flags&mqttConnFlagReserved != 0 {
		return 0, nil, fmt.Errorf("connect flags reserved bit not set to 0")
	}

	var hasWill bool
	wqos := (cp.flags & mqttConnFlagWillQoS) >> 3
	wretain := cp.flags&mqttConnFlagWillRetain != 0
	// Spec [MQTT-3.1.2-11]
	if cp.flags&mqttConnFlagWillFlag == 0 {
		// Spec [MQTT-3.1.2-13]
		if wqos != 0 {
			return 0, nil, fmt.Errorf("if Will flag is set to 0, Will QoS must be 0 too, got %v", wqos)
		}
		// Spec [MQTT-3.1.2-15]
		if wretain {
			return 0, nil, fmt.Errorf("if Will flag is set to 0, Will Retain flag must be 0 too")
		}
	} else {
		// Spec [MQTT-3.1.2-14]
		if wqos == 3 {
			return 0, nil, fmt.Errorf("if Will flag is set to 1, Will QoS can be 0, 1 or 2, got %v", wqos)
		}
		hasWill = true
	}

	// Spec [MQTT-3.1.2-19]
	hasUser := cp.flags&mqttConnFlagUsernameFlag != 0
	// Spec [MQTT-3.1.2-21]
	hasPassword := cp.flags&mqttConnFlagPasswordFlag != 0
	// Spec [MQTT-3.1.2-22]
	if !hasUser && hasPassword {
		return 0, nil, fmt.Errorf("password flag set but username flag is not")
	}

	// Keep alive
	var ka uint16
	ka, err = r.readUint16("keep alive")
	if err != nil {
		return 0, nil, err
	}
	// Spec [MQTT-3.1.2-24]
	if ka > 0 {
		cp.rd = time.Duration(float64(ka)*1.5) * time.Second
	}

	// Payload starts here and order is mandated by:
	// Spec [MQTT-3.1.3-1]: client ID, will topic, will message, username, password

	// Client ID
	cp.clientID, err = r.readString("client ID")
	if err != nil {
		return 0, nil, err
	}
	// Spec [MQTT-3.1.3-7]
	if cp.clientID == _EMPTY_ {
		if cp.flags&mqttConnFlagCleanSession == 0 {
			return mqttConnAckRCIdentifierRejected, nil, fmt.Errorf("when client ID is empty, clean session flag must be set to 1")
		}
		// Spec [MQTT-3.1.3-6]
		cp.clientID = nuid.Next()
	}
	// Spec [MQTT-3.1.3-4] and [MQTT-3.1.3-9]
	if !utf8.ValidString(cp.clientID) {
		return mqttConnAckRCIdentifierRejected, nil, fmt.Errorf("invalid utf8 for client ID: %q", cp.clientID)
	}

	if hasWill {
		cp.will = &mqttWill{
			qos:    wqos,
			retain: wretain,
		}
		var topic []byte
		// Need to make a copy since we need to hold to this topic after the
		// parsing of this protocol.
		topic, err = r.readBytes("Will topic", true)
		if err != nil {
			return 0, nil, err
		}
		if len(topic) == 0 {
			return 0, nil, fmt.Errorf("empty Will topic not allowed")
		}
		if !utf8.Valid(topic) {
			return 0, nil, fmt.Errorf("invalide utf8 for Will topic %q", topic)
		}
		cp.will.topic = topic
		// Convert MQTT topic to NATS subject
		if cp.will.subject, err = mqttTopicToNATSPubSubject(topic); err != nil {
			return 0, nil, err
		}
		// Now "will" message.
		// Ask for a copy since we need to hold to this after parsing of this protocol.
		cp.will.message, err = r.readBytes("Will message", true)
		if err != nil {
			return 0, nil, err
		}
	}

	if hasUser {
		c.opts.Username, err = r.readString("user name")
		if err != nil {
			return 0, nil, err
		}
		if c.opts.Username == _EMPTY_ {
			return mqttConnAckRCBadUserOrPassword, nil, fmt.Errorf("empty user name not allowed")
		}
		// Spec [MQTT-3.1.3-11]
		if !utf8.ValidString(c.opts.Username) {
			return mqttConnAckRCBadUserOrPassword, nil, fmt.Errorf("invalid utf8 for user name %q", c.opts.Username)
		}
	}

	if hasPassword {
		c.opts.Password, err = r.readString("password")
		if err != nil {
			return 0, nil, err
		}
		c.opts.Token = c.opts.Password
		c.opts.JWT = c.opts.Password
	}
	return 0, cp, nil
}

func (c *client) mqttConnectTrace(cp *mqttConnectProto) string {
	trace := fmt.Sprintf("clientID=%s", cp.clientID)
	if cp.rd > 0 {
		trace += fmt.Sprintf(" keepAlive=%v", cp.rd)
	}
	if cp.will != nil {
		trace += fmt.Sprintf(" will=(topic=%s QoS=%v retain=%v)",
			cp.will.topic, cp.will.qos, cp.will.retain)
	}
	if c.opts.Username != _EMPTY_ {
		trace += fmt.Sprintf(" username=%s", c.opts.Username)
	}
	if c.opts.Password != _EMPTY_ {
		trace += " password=****"
	}
	return trace
}

func (s *Server) mqttProcessConnect(c *client, cp *mqttConnectProto, trace bool) error {
	sendConnAck := func(rc byte, sessp bool) {
		c.mqttEnqueueConnAck(rc, sessp)
		if trace {
			c.traceOutOp("CONNACK", []byte(fmt.Sprintf("sp=%v rc=%v", sessp, rc)))
		}
	}

	c.mu.Lock()
	c.clearAuthTimer()
	c.mu.Unlock()
	if !s.isClientAuthorized(c) {
		sendConnAck(mqttConnAckRCNotAuthorized, false)
		c.closeConnection(AuthenticationViolation)
		return ErrAuthentication
	}
	// Now that we are are authenticated, we have the client bound to the account.
	// Get the account's level MQTT sessions manager. If it does not exists yet,
	// this will create it along with the streams where sessions and messages
	// are stored.
	sm := &s.mqtt.sessmgr
	asm, err := sm.getOrCreateAccountSessionManager(cp.clientID, c)
	if err != nil {
		return err
	}

	// Rest of code runs under the account's sessions manager write lock.
	asm.mu.Lock()
	defer asm.mu.Unlock()

	// Is the client requesting a clean session or not.
	cleanSess := cp.flags&mqttConnFlagCleanSession != 0
	// Session present? Assume false, will be set to true only when applicable.
	sessp := false
	// Do we have an existing session for this client ID
	es, ok := asm.sessions[cp.clientID]
	if !ok {
		es = mqttSessionCreate(s.getOpts())
	}
	es.mu.Lock()
	defer es.mu.Unlock()
	if ok {
		// Clear the session if client wants a clean session.
		// Also, Spec [MQTT-3.2.2-1]: don't report session present
		if cleanSess || es.clean {
			// Spec [MQTT-3.1.2-6]: If CleanSession is set to 1, the Client and
			// Server MUST discard any previous Session and start a new one.
			// This Session lasts as long as the Network Connection. State data
			// associated with this Session MUST NOT be reused in any subsequent
			// Session.
			es.clear()
		} else {
			// Report to the client that the session was present
			sessp = true
		}
		// Spec [MQTT-3.1.4-2]. If the ClientId represents a Client already
		// connected to the Server then the Server MUST disconnect the existing
		// client.
		if ec := es.c; ec != nil {
			ec.mu.Lock()
			// Remove will before closing
			ec.mqtt.cp.will = nil
			ec.mu.Unlock()
			// Close old client in separate go routine
			go ec.closeConnection(DuplicateClientID)
		}
		// Bind with the new client
		es.c = c
		es.clean = cleanSess
	} else {
		// Spec [MQTT-3.2.2-3]: if the Server does not have stored Session state,
		// it MUST set Session Present to 0 in the CONNACK packet.
		es.c, es.clean, es.stream = c, cleanSess, asm.sstream
		es.save(cp.clientID)
		// Now save this new session into the account sessions
		asm.sessions[cp.clientID] = es
	}
	c.mu.Lock()
	c.flags.set(connectReceived)
	c.mqtt.cp = cp
	c.mqtt.asm = asm
	c.mqtt.sess = es
	c.mu.Unlock()
	//
	// Spec [MQTT-3.2.0-1]: CONNACK must be the first protocol sent to the
	// session. However, we are going to possibly restore the subscriptions
	// first and then send the CONNACK. This will help tests that restore
	// a MQTT connection with subs and immediately use NATS to publish.
	// In that case, message would not be received because the pub could
	// occur before the subscriptions are processed here. It would be
	// easy to fix test with doing a PINGREQ/PINGRESP before doing NATS pub,
	// but it seems better to ensure that everything is setup before sending
	// back the CONNACK.
	// Note that since we are under the session lock, the subs callback will
	// have to wait to acquire the lock, so we are still guaranteed to enqueue
	// the CONNACK before any message.
	//
	// Process possible saved subscriptions.
	if l := len(es.subs); l > 0 {
		filters := make([]*mqttFilter, 0, l)
		for subject, qos := range es.subs {
			filters = append(filters, &mqttFilter{filter: subject, qos: qos})
		}
		if _, err := asm.processSubs(es, cp.clientID, c, filters, false, trace); err != nil {
			return err
		}
	}
	// Now send the CONNACK
	sendConnAck(mqttConnAckRCConnectionAccepted, sessp)
	return nil
}

func (c *client) mqttEnqueueConnAck(rc byte, sessionPresent bool) {
	proto := [4]byte{mqttPacketConnectAck, 2, 0, rc}
	c.mu.Lock()
	// Spec [MQTT-3.2.2-4]. If return code is different from 0, then
	// session present flag must be set to 0.
	if rc == 0 {
		if sessionPresent {
			proto[2] = 1
		}
	}
	c.enqueueProto(proto[:])
	c.mu.Unlock()
}

func (s *Server) mqttHandleWill(c *client) {
	c.mu.Lock()
	if c.mqtt.cp == nil {
		c.mu.Unlock()
		return
	}
	will := c.mqtt.cp.will
	if will == nil {
		c.mu.Unlock()
		return
	}
	pp := c.mqtt.pp
	pp.topic = will.topic
	pp.subject = will.subject
	pp.msg = will.message
	pp.sz = len(will.message)
	pp.pi = 0
	pp.flags = will.qos << 1
	if will.retain {
		pp.flags |= mqttPubFlagRetain
	}
	c.mu.Unlock()
	s.mqttProcessPub(c, pp)
	c.flushClients(0)
}

//////////////////////////////////////////////////////////////////////////////
//
// PUBLISH protocol related functions
//
//////////////////////////////////////////////////////////////////////////////

func (c *client) mqttParsePub(r *mqttReader, pl int, pp *mqttPublish) error {
	qos := mqttGetQoS(pp.flags)
	if qos > 1 {
		return fmt.Errorf("publish QoS=%v not supported", qos)
	}
	if err := r.ensurePacketInBuffer(pl); err != nil {
		return err
	}
	// Keep track of where we are when starting to read the variable header
	start := r.pos

	var err error
	pp.topic, err = r.readBytes("topic", false)
	if err != nil {
		return err
	}
	if len(pp.topic) == 0 {
		return fmt.Errorf("topic cannot be empty")
	}
	// Convert the topic to a NATS subject. This call will also check that
	// there is no MQTT wildcards (Spec [MQTT-3.3.2-2] and [MQTT-4.7.1-1])
	// Note that this may not result in a copy if there is no conversion.
	// It is good because after the message is processed we won't have a
	// reference to the buffer and we save a copy.
	pp.subject, err = mqttTopicToNATSPubSubject(pp.topic)
	if err != nil {
		return err
	}

	if qos > 0 {
		pp.pi, err = r.readUint16("packet identifier")
		if err != nil {
			return err
		}
		if pp.pi == 0 {
			return fmt.Errorf("with QoS=%v, packet identifier cannot be 0", qos)
		}
	} else {
		pp.pi = 0
	}

	// The message payload will be the total packet length minus
	// what we have consumed for the variable header
	pp.sz = pl - (r.pos - start)
	if pp.sz > 0 {
		start = r.pos
		r.pos += pp.sz
		pp.msg = r.buf[start:r.pos]
	} else {
		pp.msg = nil
	}
	return nil
}

func mqttPubTrace(pp *mqttPublish) string {
	dup := pp.flags&mqttPubFlagDup != 0
	qos := mqttGetQoS(pp.flags)
	retain := mqttIsRetained(pp.flags)
	var piStr string
	if pp.pi > 0 {
		piStr = fmt.Sprintf(" pi=%v", pp.pi)
	}
	return fmt.Sprintf("%s dup=%v QoS=%v retain=%v size=%v%s",
		pp.topic, dup, qos, retain, pp.sz, piStr)
}

func (s *Server) mqttProcessPub(c *client, pp *mqttPublish) {
	c.pa.subject, c.pa.hdr, c.pa.size = pp.subject, -1, pp.sz

	// Convert size into bytes.
	i := len(pp.szb)
	if pp.sz > 0 {
		for l := pp.sz; l > 0; l /= 10 {
			i--
			pp.szb[i] = digits[l%10]
		}
	} else {
		i--
		pp.szb[i] = digits[0]
	}
	c.pa.szb = pp.szb[i:]

	// This will work for QoS 0 but mqtt msg delivery callback will ignore
	// delivery for QoS > 0 published messages (since it is handled specifically
	// with call to directProcessInboundJetStreamMsg).
	// However, this needs to be invoked before directProcessInboundJetStreamMsg()
	// in case we are dealing with publish retained messages.
	c.processInboundClientMsg(pp.msg)
	if mqttGetQoS(pp.flags) > 0 {
		// Since this is the fast path, we access the messages stream directly here
		// without locking. All the fields mqtt.asm.mstream are immutable.
		c.mqtt.asm.mstream.processInboundJetStreamMsg(nil, c, string(c.pa.subject), _EMPTY_, pp.msg)
	}
	c.pa.subject, c.pa.hdr, c.pa.size, c.pa.szb = nil, -1, 0, nil
}

// Invoked when processing an inbound client message. If the "retain" flag is
// set, the message is stored so it can be later resent to (re)starting
// subscriptions that match the subject.
//
// Invoked from the publisher's readLoop. No client lock is held on entry.
func (c *client) mqttHandlePubRetain() {
	pp := c.mqtt.pp
	if mqttIsRetained(pp.flags) {
		key := string(pp.subject)
		asm := c.mqtt.asm
		asm.mu.Lock()
		// Spec [MQTT-3.3.1-11]. Payload of size 0 removes the retained message,
		// but should still be delivered as a normal message.
		if pp.sz == 0 {
			if asm.retmsgs != nil {
				if erm, ok := asm.retmsgs[key]; ok {
					delete(asm.retmsgs, key)
					asm.sl.Remove(erm.sub)
					if erm.sseq != 0 {
						asm.rstream.deleteMsg(erm.sseq)
					}
				}
			}
		} else {
			// Spec [MQTT-3.3.1-5]. Store the retained message with its QoS.
			// When coming from a publish protocol, `pp` is referencing a stack
			// variable that itself possibly references the read buffer.
			rm := &mqttRetainedMsg{
				Topic:  copyBytes(pp.topic),
				Msg:    copyBytes(pp.msg),
				Flags:  pp.flags,
				Source: c.opts.Username,
			}
			rm = asm.handleRetainedMsg(key, rm)
			rmBytes, _ := json.Marshal(rm)
			// TODO: For now we will report the error but continue...
			seq, _, err := asm.rstream.store.StoreMsg(key, nil, rmBytes)
			if err != nil {
				c.mu.Lock()
				acc := c.acc
				c.mu.Unlock()
				c.Errorf("unable to store retained message for account %q, subject %q: %v",
					acc.GetName(), key, err)
			}
			// If it has been replaced, rm.sseq will be != 0
			if rm.sseq != 0 {
				asm.rstream.deleteMsg(rm.sseq)
			}
			// Keep track of current stream sequence (possibly 0 if failed to store)
			rm.sseq = seq
		}

		asm.mu.Unlock()

		// Clear the retain flag for a normal published message.
		pp.flags &= ^mqttPubFlagRetain
	}
}

// After a config reload, it is possible that the source of a publish retained
// message is no longer allowed to publish on the given topic. If that is the
// case, the retained message is removed from the map and will no longer be
// sent to (re)starting subscriptions.
//
// Server lock is held on entry
func (s *Server) mqttCheckPubRetainedPerms() {
	sm := &s.mqtt.sessmgr
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, asm := range sm.sessions {
		perms := map[string]*perm{}
		asm.mu.Lock()
		for subject, rm := range asm.retmsgs {
			if rm.Source == _EMPTY_ {
				continue
			}
			// Lookup source from global users.
			u := s.users[rm.Source]
			if u != nil {
				p, ok := perms[rm.Source]
				if !ok {
					p = generatePubPerms(u.Permissions)
					perms[rm.Source] = p
				}
				// If there is permission and no longer allowed to publish in
				// the subject, remove the publish retained message from the map.
				if p != nil && !pubAllowed(p, subject) {
					u = nil
				}
			}

			// Not present or permissions have changed such that the source can't
			// publish on that subject anymore: remove it from the map.
			if u == nil {
				delete(asm.retmsgs, subject)
				asm.rstream.deleteMsg(rm.sseq)
				asm.sl.Remove(rm.sub)
			}
		}
		asm.mu.Unlock()
	}
}

// Helper to generate only pub permissions from a Permissions object
func generatePubPerms(perms *Permissions) *perm {
	var p *perm
	if perms.Publish.Allow != nil {
		p = &perm{}
		p.allow = NewSublistWithCache()
	}
	for _, pubSubject := range perms.Publish.Allow {
		sub := &subscription{subject: []byte(pubSubject)}
		p.allow.Insert(sub)
	}
	if len(perms.Publish.Deny) > 0 {
		if p == nil {
			p = &perm{}
		}
		p.deny = NewSublistWithCache()
	}
	for _, pubSubject := range perms.Publish.Deny {
		sub := &subscription{subject: []byte(pubSubject)}
		p.deny.Insert(sub)
	}
	return p
}

// Helper that checks if given `perms` allow to publish on the given `subject`
func pubAllowed(perms *perm, subject string) bool {
	allowed := true
	if perms.allow != nil {
		r := perms.allow.Match(subject)
		allowed = len(r.psubs) != 0
	}
	// If we have a deny list and are currently allowed, check that as well.
	if allowed && perms.deny != nil {
		r := perms.deny.Match(subject)
		allowed = len(r.psubs) == 0
	}
	return allowed
}

func mqttWritePublish(w *mqttWriter, qos byte, dup, retain bool, subject string, pi uint16, payload []byte) {
	flags := qos << 1
	if dup {
		flags |= mqttPubFlagDup
	}
	if retain {
		flags |= mqttPubFlagRetain
	}
	w.WriteByte(mqttPacketPub | flags)
	pkLen := 2 + len(subject) + len(payload)
	if qos > 0 {
		pkLen += 2
	}
	w.WriteVarInt(pkLen)
	w.WriteString(subject)
	if qos > 0 {
		w.WriteUint16(pi)
	}
	w.Write([]byte(payload))
}

func (c *client) mqttEnqueuePubAck(pi uint16) {
	proto := [4]byte{mqttPacketPubAck, 0x2, 0, 0}
	proto[2] = byte(pi >> 8)
	proto[3] = byte(pi)
	c.mu.Lock()
	c.enqueueProto(proto[:4])
	c.mu.Unlock()
}

func mqttParsePubAck(r *mqttReader, pl int) (uint16, error) {
	if err := r.ensurePacketInBuffer(pl); err != nil {
		return 0, err
	}
	pi, err := r.readUint16("packet identifier")
	if err != nil {
		return 0, err
	}
	if pi == 0 {
		return 0, fmt.Errorf("packet identifier cannot be 0")
	}
	return pi, nil
}

func (c *client) mqttProcessPubAck(pi uint16) {
	sess := c.mqtt.sess
	if sess == nil {
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.c != c {
		return
	}
	if ack, ok := sess.pending[pi]; ok {
		delete(sess.pending, pi)
		jsCons := ack.jsCons
		if sseqToPi, ok := sess.cpending[jsCons]; ok {
			delete(sseqToPi, ack.sseq)
		}
		jsCons.ackMsg(ack.sseq, ack.dseq, ack.dcount)
		if len(sess.pending) == 0 {
			sess.ppi = 0
		}
		if sess.stalled && len(sess.pending) < int(sess.maxp) {
			sess.stalled = false
			for _, cons := range sess.cons {
				cons.mu.Lock()
				cons.forceExpirePending()
				cons.mu.Unlock()
			}
		}
	}
}

// Return the QoS from the given PUBLISH protocol's flags
func mqttGetQoS(flags byte) byte {
	return flags & mqttPubFlagQoS >> 1
}

func mqttIsRetained(flags byte) bool {
	return flags&mqttPubFlagRetain != 0
}

//////////////////////////////////////////////////////////////////////////////
//
// SUBSCRIBE related functions
//
//////////////////////////////////////////////////////////////////////////////

func (c *client) mqttParseSubs(r *mqttReader, b byte, pl int) (uint16, []*mqttFilter, error) {
	return c.mqttParseSubsOrUnsubs(r, b, pl, true)
}

func (c *client) mqttParseSubsOrUnsubs(r *mqttReader, b byte, pl int, sub bool) (uint16, []*mqttFilter, error) {
	var expectedFlag byte
	var action string
	if sub {
		expectedFlag = mqttSubscribeFlags
	} else {
		expectedFlag = mqttUnsubscribeFlags
		action = "un"
	}
	// Spec [MQTT-3.8.1-1], [MQTT-3.10.1-1]
	if rf := b & 0xf; rf != expectedFlag {
		return 0, nil, fmt.Errorf("wrong %ssubscribe reserved flags: %x", action, rf)
	}
	if err := r.ensurePacketInBuffer(pl); err != nil {
		return 0, nil, err
	}
	pi, err := r.readUint16("packet identifier")
	if err != nil {
		return 0, nil, fmt.Errorf("reading packet identifier: %v", err)
	}
	end := r.pos + (pl - 2)
	var filters []*mqttFilter
	for r.pos < end {
		// Don't make a copy now because, this will happen during conversion
		// or when processing the sub.
		filter, err := r.readBytes("topic filter", false)
		if err != nil {
			return 0, nil, err
		}
		if len(filter) == 0 {
			return 0, nil, errors.New("topic filter cannot be empty")
		}
		// Spec [MQTT-3.8.3-1], [MQTT-3.10.3-1]
		if !utf8.Valid(filter) {
			return 0, nil, fmt.Errorf("invalid utf8 for topic filter %q", filter)
		}
		var qos byte
		topic := filter
		// This won't return an error. We will find out if the subject
		// is valid or not when trying to create the subscription.
		filter, _ = mqttFilterToNATSSubject(filter)
		if sub {
			qos, err = r.readByte("QoS")
			if err != nil {
				return 0, nil, err
			}
			// Spec [MQTT-3-8.3-4].
			if qos > 2 {
				return 0, nil, fmt.Errorf("subscribe QoS value must be 0, 1 or 2, got %v", qos)
			}
		}
		f := &mqttFilter{ttopic: topic, filter: string(filter), qos: qos}
		filters = append(filters, f)
	}
	// Spec [MQTT-3.8.3-3], [MQTT-3.10.3-2]
	if len(filters) == 0 {
		return 0, nil, fmt.Errorf("%ssubscribe protocol must contain at least 1 topic filter", action)
	}
	return pi, filters, nil
}

func mqttSubscribeTrace(pi uint16, filters []*mqttFilter) string {
	var sep string
	sb := &strings.Builder{}
	sb.WriteString("[")
	for i, f := range filters {
		sb.WriteString(sep)
		sb.Write(f.ttopic)
		sb.WriteString(" (")
		sb.WriteString(f.filter)
		sb.WriteString(") QoS=")
		sb.WriteString(fmt.Sprintf("%v", f.qos))
		if i == 0 {
			sep = ", "
		}
	}
	sb.WriteString(fmt.Sprintf("] pi=%v", pi))
	return sb.String()
}

func mqttDeliverMsgCb(sub *subscription, pc *client, subject, reply string, msg []byte) {
	var ppFlags byte
	var pQoS byte
	var pi uint16
	var dup bool
	var retained bool
	var topic []byte

	// This is the client associated with the subscription.
	cc := sub.client

	// This is immutable
	sess := cc.mqtt.sess
	// We lock to check some of the subscription's fields and if we need to
	// keep track of pending acks, etc..
	sess.mu.Lock()
	if sess.c != cc || sub.mqtt == nil {
		sess.mu.Unlock()
		return
	}

	// Check the publisher's kind. If JETSTREAM it means that this is a persisted message
	// that is being delivered.
	if pc.kind == JETSTREAM {
		// If there is no JS consumer attached to this subscription, it means that we are
		// dealing with a bare NATS subscription, in which case we simply return to avoid
		// duplicate delivery.
		if sub.mqtt.jsCons == nil {
			sess.mu.Unlock()
			return
		}
		ppFlags = mqttPubQos1
		pQoS = 1
		// This is a QoS1 message for a QoS1 subscription, so get the pi and keep
		// track of ack subject.
		pi, dup = sess.trackPending(pQoS, reply, sub)
		if pi == 0 {
			// We have reached max pending, don't send the message now.
			// JS will cause a redelivery and if by then the number of pending
			// messages has fallen below threshold, the message will be resent.
			sess.mu.Unlock()
			return
		}
		// TODO: We hae to conver the subject back to topic. One approach
		// was the pass the parsed MQTT topic in jsPubMsg but it was decided
		// to not do that for now.
		topic = natsSubjectToMQTTTopic(string(pc.pa.deliver))
	} else if pc.isMqtt() {
		// This is a MQTT publisher...
		topic = pc.mqtt.pp.topic
		ppFlags = pc.mqtt.pp.flags
		pQoS = mqttGetQoS(ppFlags)
		// If the QoS of published message and subscription is 1, then we return here to
		// avoid duplicate delivery. The JetStream publisher will handle that case.
		if pQoS > 0 && sub.mqtt.qos > 0 {
			sess.mu.Unlock()
			return
		}
		retained = mqttIsRetained(ppFlags)
	} else {
		// This is coming from a non MQTT publisher, so Qos 0, no dup nor retain flag, etc..
		// Should probably reject, for now just truncate.
		if len(msg) > mqttMaxPayloadSize {
			msg = msg[:mqttMaxPayloadSize]
		}
		// See TODO for JS producer. Right now, we need to do the conversion
		// back to a subject everytime.
		topic = natsSubjectToMQTTTopic(subject)
	}
	sess.mu.Unlock()

	sw := mqttWriter{}
	w := &sw

	flags := mqttSerializePublishMsg(w, pi, dup, retained, topic, msg)

	cc.mu.Lock()
	if sub.mqtt.prm != nil {
		cc.queueOutbound(sub.mqtt.prm.Bytes())
		sub.mqtt.prm = nil
	}
	cc.queueOutbound(w.Bytes())
	pc.addToPCD(cc)
	if cc.trace {
		pp := mqttPublish{
			topic: topic,
			flags: flags,
			pi:    pi,
			sz:    len(msg),
		}
		cc.traceOutOp("PUBLISH", []byte(mqttPubTrace(&pp)))
	}
	cc.mu.Unlock()
}

// Serializes to the given writer the message for the given subject.
func mqttSerializePublishMsg(w *mqttWriter, pi uint16, dup, retained bool, topic, msg []byte) byte {

	// Compute len (will have to add packet id if message is sent as QoS>=1)
	pkLen := 2 + len(topic) + len(msg)

	var flags byte

	// Set flags for dup/retained/qos1
	if dup {
		flags |= mqttPubFlagDup
	}
	if retained {
		flags |= mqttPubFlagRetain
	}
	// For now, we have only QoS 1
	if pi > 0 {
		pkLen += 2
		flags |= mqttPubQos1
	}

	w.WriteByte(mqttPacketPub | flags)
	w.WriteVarInt(pkLen)
	w.WriteBytes(topic)
	if pi > 0 {
		w.WriteUint16(pi)
	}
	w.Write(msg)

	return flags
}

// Process the list of subscriptions and update the given filter
// with the QoS that has been accepted (or failure).
//
// Spec [MQTT-3.8.4-3] says that if an exact same subscription is
// found, it needs to be replaced with the new one (possibly updating
// the qos) and that the flow of publications must not be interrupted,
// which I read as the replacement cannot be a "remove then add" if there
// is a chance that in between the 2 actions, published messages
// would be "lost" because there would not be any matching subscription.
func (c *client) mqttProcessSubs(filters []*mqttFilter) ([]*subscription, error) {
	// Those things are immutable, but since processing subs is not
	// really in the fast path, let's get them under the client lock.
	c.mu.Lock()
	asm := c.mqtt.asm
	sess := c.mqtt.sess
	clientID := c.mqtt.cp.clientID
	trace := c.trace
	c.mu.Unlock()

	asm.mu.RLock()
	defer asm.mu.RUnlock()
	return asm.processSubs(sess, clientID, c, filters, true, trace)
}

func (c *client) mqttCleanupFailedSub(sub *subscription, jscons *consumer, jssub *subscription) {
	c.mu.Lock()
	acc := c.acc
	c.mu.Unlock()

	if sub != nil {
		c.unsubscribe(acc, sub, true, true)
	}
	if jssub != nil {
		c.unsubscribe(acc, jssub, true, true)
	}
	if jscons != nil {
		jscons.delete()
	}
}

// When invoked with a QoS of 0, looks for an existing JS durable consumer for
// the given sid and if one is found, delete the JS durable consumer and unsub
// the NATS subscription on the delivery subject.
// With a QoS > 0, creates or update the existing JS durable consumer along with
// its NATS subscription on a delivery subject.
//
// Account session manager lock held on entry.
func (c *client) mqttProcessJSConsumer(sess *mqttSession, stream *stream, subject,
	sid string, qos byte, fromSubProto bool) (*consumer, *subscription, error) {

	// Check if we are already a JS consumer for this SID.
	cons, exists := sess.cons[sid]
	if exists {
		// If current QoS is 0, it means that we need to delete the existing
		// one (that was QoS > 0)
		if qos == 0 {
			// The JS durable consumer's delivery subject is on a NUID of
			// the form: mqttSubPrefix + <nuid>. It is also used as the sid
			// for the NATS subscription, so use that for the lookup.
			sub := c.subs[cons.config().DeliverSubject]
			delete(sess.cons, sid)
			cons.delete()
			if sub != nil {
				c.mu.Lock()
				acc := c.acc
				c.mu.Unlock()
				c.unsubscribe(acc, sub, true, true)
			}
			return nil, nil, nil
		}
		// If this is called when processing SUBSCRIBE protocol, then if
		// the JS consumer already exists, we are done (it was created
		// during the processing of CONNECT).
		if fromSubProto {
			return nil, nil, nil
		}
	}
	// Here it means we don't have a JS consumer and if we are QoS 0,
	// we have nothing to do.
	if qos == 0 {
		return nil, nil, nil
	}
	var err error
	inbox := mqttSubPrefix + nuid.Next()
	if exists {
		cons.updateDeliverSubject(inbox)
	} else {
		durName := nuid.Next()
		opts := c.srv.getOpts()
		ackWait := opts.MQTT.AckWait
		if ackWait == 0 {
			ackWait = mqttDefaultAckWait
		}
		maxAckPending := opts.MQTT.MaxAckPending
		if maxAckPending == 0 {
			maxAckPending = mqttDefaultMaxAckPending
		}
		cc := &ConsumerConfig{
			DeliverSubject:  inbox,
			Durable:         durName,
			AckPolicy:       AckExplicit,
			DeliverPolicy:   DeliverNew,
			FilterSubject:   subject,
			AckWait:         ackWait,
			MaxAckPending:   int(maxAckPending),
			allowNoInterest: true,
		}
		cons, err = stream.addConsumer(cc)
		if err != nil {
			c.Errorf("Unable to add JetStream consumer for subscription on %q: err=%v", subject, err)
			return nil, nil, err
		}
	}
	// This is an internal subscription on subject like "$MQTT.sub.<nuid>" that is setup
	// for the JS durable's deliver subject. I don't think that there is any need to
	// forward this subscription in the cluster/super cluster.
	sub, err := c.processSub([]byte(inbox), nil, []byte(inbox), mqttDeliverMsgCb, true)
	if err != nil {
		if !exists {
			cons.delete()
		}
		c.Errorf("Unable to create subscription for JetStream consumer on %q: %v", subject, err)
		return nil, nil, err
	}
	if sub.mqtt == nil {
		sub.mqtt = &mqttSub{}
	}
	sub.mqtt.qos = qos
	sub.mqtt.jsCons = cons
	return cons, sub, nil
}

// Queues the published retained messages for each subscription and signals
// the writeLoop.
func (c *client) mqttSendRetainedMsgsToNewSubs(subs []*subscription) {
	c.mu.Lock()
	for _, sub := range subs {
		if sub.mqtt != nil && sub.mqtt.prm != nil {
			c.queueOutbound(sub.mqtt.prm.Bytes())
			sub.mqtt.prm = nil
		}
	}
	c.flushSignal()
	c.mu.Unlock()
}

func (c *client) mqttEnqueueSubAck(pi uint16, filters []*mqttFilter) {
	w := &mqttWriter{}
	w.WriteByte(mqttPacketSubAck)
	// packet length is 2 (for packet identifier) and 1 byte per filter.
	w.WriteVarInt(2 + len(filters))
	w.WriteUint16(pi)
	for _, f := range filters {
		w.WriteByte(f.qos)
	}
	c.mu.Lock()
	c.enqueueProto(w.Bytes())
	c.mu.Unlock()
}

//////////////////////////////////////////////////////////////////////////////
//
// UNSUBSCRIBE related functions
//
//////////////////////////////////////////////////////////////////////////////

func (c *client) mqttParseUnsubs(r *mqttReader, b byte, pl int) (uint16, []*mqttFilter, error) {
	return c.mqttParseSubsOrUnsubs(r, b, pl, false)
}

func (c *client) mqttProcessUnsubs(filters []*mqttFilter) error {
	// Those things are immutable, but since processing unsubs is not
	// really in the fast path, let's get them under the client lock.
	c.mu.Lock()
	sess := c.mqtt.sess
	clientID := c.mqtt.cp.clientID
	c.mu.Unlock()

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.c != c {
		return fmt.Errorf("client %q no longer registered with MQTT session", clientID)
	}

	removeJSCons := func(sid string) {
		if jscons, ok := sess.cons[sid]; ok {
			delete(sess.cons, sid)
			jscons.delete()
			if seqPis, ok := sess.cpending[jscons]; ok {
				delete(sess.cpending, jscons)
				for _, pi := range seqPis {
					delete(sess.pending, pi)
				}
				if len(sess.pending) == 0 {
					sess.ppi = 0
				}
			}
		}
	}
	for _, f := range filters {
		sid := f.filter
		// Remove JS Consumer if one exists for this sid
		removeJSCons(sid)
		if err := c.processUnsub([]byte(sid)); err != nil {
			c.Errorf("error unsubscribing from %q: %v", sid, err)
		}
		if mqttNeedSubForLevelUp(sid) {
			subject := sid[:len(sid)-2]
			sid = subject + mqttMultiLevelSidSuffix
			removeJSCons(sid)
			if err := c.processUnsub([]byte(sid)); err != nil {
				c.Errorf("error unsubscribing from %q: %v", subject, err)
			}
		}
	}
	return sess.update(clientID, filters, false)
}

func (c *client) mqttEnqueueUnsubAck(pi uint16) {
	w := &mqttWriter{}
	w.WriteByte(mqttPacketUnsubAck)
	w.WriteVarInt(2)
	w.WriteUint16(pi)
	c.mu.Lock()
	c.enqueueProto(w.Bytes())
	c.mu.Unlock()
}

func mqttUnsubscribeTrace(pi uint16, filters []*mqttFilter) string {
	var sep string
	sb := strings.Builder{}
	sb.WriteString("[")
	for i, f := range filters {
		sb.WriteString(sep)
		sb.Write(f.ttopic)
		sb.WriteString(" (")
		sb.WriteString(f.filter)
		sb.WriteString(")")
		if i == 0 {
			sep = ", "
		}
	}
	sb.WriteString(fmt.Sprintf("] pi=%v", pi))
	return sb.String()
}

//////////////////////////////////////////////////////////////////////////////
//
// PINGREQ/PINGRESP related functions
//
//////////////////////////////////////////////////////////////////////////////

func (c *client) mqttEnqueuePingResp() {
	c.mu.Lock()
	c.enqueueProto(mqttPingResponse)
	c.mu.Unlock()
}

//////////////////////////////////////////////////////////////////////////////
//
// Trace functions
//
//////////////////////////////////////////////////////////////////////////////

func errOrTrace(err error, trace string) []byte {
	if err != nil {
		return []byte(err.Error())
	}
	return []byte(trace)
}

//////////////////////////////////////////////////////////////////////////////
//
// Subject/Topic conversion functions
//
//////////////////////////////////////////////////////////////////////////////

// Converts an MQTT Topic Name to a NATS Subject (used by PUBLISH)
// See mqttToNATSSubjectConversion() for details.
func mqttTopicToNATSPubSubject(mt []byte) ([]byte, error) {
	return mqttToNATSSubjectConversion(mt, false)
}

// Converts an MQTT Topic Filter to a NATS Subject (used by SUBSCRIBE)
// See mqttToNATSSubjectConversion() for details.
func mqttFilterToNATSSubject(filter []byte) ([]byte, error) {
	return mqttToNATSSubjectConversion(filter, true)
}

// Converts an MQTT Topic Name or Filter to a NATS Subject.
// In MQTT:
// - a Topic Name does not have wildcard (PUBLISH uses only topic names).
// - a Topic Filter can include wildcards (SUBSCRIBE uses those).
// - '+' and '#' are wildcard characters (single and multiple levels respectively)
// - '/' is the topic level separator.
//
// Conversion that occurs:
// - '/' is replaced with '/.' if it is the first character in mt
// - '/' is replaced with './' if the last or next character in mt is '/'
//   For instance, foo//bar would become foo./.bar
// - '/' is replaced with '.' for all other conditions (foo/bar -> foo.bar)
// - '.' and ' ' cause an error to be returned.
//
// If there is no need to convert anything (say "foo" remains "foo"), then
// the no memory is allocated and the returned slice is the original `mt`.
func mqttToNATSSubjectConversion(mt []byte, wcOk bool) ([]byte, error) {
	var cp bool
	var j int
	res := mt

	makeCopy := func(i int) {
		cp = true
		res = make([]byte, 0, len(mt)+10)
		if i > 0 {
			res = append(res, mt[:i]...)
		}
	}

	end := len(mt) - 1
	for i := 0; i < len(mt); i++ {
		switch mt[i] {
		case mqttTopicLevelSep:
			if i == 0 || res[j-1] == btsep {
				if !cp {
					makeCopy(0)
				}
				res = append(res, mqttTopicLevelSep, btsep)
				j++
			} else if i == end || mt[i+1] == mqttTopicLevelSep {
				if !cp {
					makeCopy(i)
				}
				res = append(res, btsep, mqttTopicLevelSep)
				j++
			} else {
				if !cp {
					makeCopy(i)
				}
				res = append(res, btsep)
			}
		case btsep, ' ':
			// As of now, we cannot support '.' or ' ' in the MQTT topic/filter.
			return nil, fmt.Errorf("characters ' ' and '.' not supported for MQTT topics")
		case mqttSingleLevelWC, mqttMultiLevelWC:
			if !wcOk {
				// Spec [MQTT-3.3.2-2] and [MQTT-4.7.1-1]
				// The wildcard characters can be used in Topic Filters, but MUST NOT be used within a Topic Name
				return nil, fmt.Errorf("wildcards not allowed in publish's topic: %q", mt)
			}
			if !cp {
				makeCopy(i)
			}
			if mt[i] == mqttSingleLevelWC {
				res = append(res, pwc)
			} else {
				res = append(res, fwc)
			}
		default:
			if cp {
				res = append(res, mt[i])
			}
		}
		j++
	}
	if cp && res[j-1] == btsep {
		res = append(res, mqttTopicLevelSep)
		j++
	}
	return res[:j], nil
}

// Converts a NATS subject to MQTT topic. This is for publish
// messages only, so there is no checking for wildcards.
// Rules are reversed of mqttToNATSSubjectConversion.
func natsSubjectToMQTTTopic(subject string) []byte {
	topic := []byte(subject)
	end := len(subject) - 1
	var j int
	for i := 0; i < len(subject); i++ {
		switch subject[i] {
		case mqttTopicLevelSep:
			if !(i == 0 && i < end && subject[i+1] == btsep) {
				topic[j] = mqttTopicLevelSep
				j++
			}
		case btsep:
			topic[j] = mqttTopicLevelSep
			j++
			if i < end && subject[i+1] == mqttTopicLevelSep {
				i++
			}
		default:
			topic[j] = subject[i]
			j++
		}
	}
	return topic[:j]
}

// Returns true if the subject has more than 1 token and ends with ".>"
func mqttNeedSubForLevelUp(subject string) bool {
	if len(subject) < 3 {
		return false
	}
	end := len(subject)
	if subject[end-2] == '.' && subject[end-1] == fwc {
		return true
	}
	return false
}

//////////////////////////////////////////////////////////////////////////////
//
// MQTT Reader functions
//
//////////////////////////////////////////////////////////////////////////////

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	cbuf := make([]byte, len(b))
	copy(cbuf, b)
	return cbuf
}

func (r *mqttReader) reset(buf []byte) {
	r.buf = buf
	r.pos = 0
}

func (r *mqttReader) hasMore() bool {
	return r.pos != len(r.buf)
}

func (r *mqttReader) readByte(field string) (byte, error) {
	if r.pos == len(r.buf) {
		return 0, fmt.Errorf("error reading %s: %v", field, io.EOF)
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *mqttReader) readPacketLen() (int, error) {
	m := 1
	v := 0
	for {
		var b byte
		if r.pos != len(r.buf) {
			b = r.buf[r.pos]
			r.pos++
		} else {
			var buf [1]byte
			if _, err := r.reader.Read(buf[:1]); err != nil {
				if err == io.EOF {
					return 0, io.ErrUnexpectedEOF
				}
				return 0, fmt.Errorf("error reading packet length: %v", err)
			}
			b = buf[0]
		}
		v += int(b&0x7f) * m
		if (b & 0x80) == 0 {
			return v, nil
		}
		m *= 0x80
		if m > 0x200000 {
			return 0, errors.New("malformed variable int")
		}
	}
}

func (r *mqttReader) ensurePacketInBuffer(pl int) error {
	rem := len(r.buf) - r.pos
	if rem >= pl {
		return nil
	}
	b := make([]byte, pl)
	start := copy(b, r.buf[r.pos:])
	for start != pl {
		n, err := r.reader.Read(b[start:cap(b)])
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return fmt.Errorf("error ensuring protocol is loaded: %v", err)
		}
		start += n
	}
	r.reset(b)
	return nil
}

func (r *mqttReader) readString(field string) (string, error) {
	var s string
	bs, err := r.readBytes(field, false)
	if err == nil {
		s = string(bs)
	}
	return s, err
}

func (r *mqttReader) readBytes(field string, cp bool) ([]byte, error) {
	luint, err := r.readUint16(field)
	if err != nil {
		return nil, err
	}
	l := int(luint)
	if l == 0 {
		return nil, nil
	}
	start := r.pos
	if start+l > len(r.buf) {
		return nil, fmt.Errorf("error reading %s: %v", field, io.ErrUnexpectedEOF)
	}
	r.pos += l
	b := r.buf[start:r.pos]
	if cp {
		b = copyBytes(b)
	}
	return b, nil
}

func (r *mqttReader) readUint16(field string) (uint16, error) {
	if len(r.buf)-r.pos < 2 {
		return 0, fmt.Errorf("error reading %s: %v", field, io.ErrUnexpectedEOF)
	}
	start := r.pos
	r.pos += 2
	return binary.BigEndian.Uint16(r.buf[start:r.pos]), nil
}

//////////////////////////////////////////////////////////////////////////////
//
// MQTT Writer functions
//
//////////////////////////////////////////////////////////////////////////////

func (w *mqttWriter) WriteUint16(i uint16) {
	w.WriteByte(byte(i >> 8))
	w.WriteByte(byte(i))
}

func (w *mqttWriter) WriteString(s string) {
	w.WriteBytes([]byte(s))
}

func (w *mqttWriter) WriteBytes(bs []byte) {
	w.WriteUint16(uint16(len(bs)))
	w.Write(bs)
}

func (w *mqttWriter) WriteVarInt(value int) {
	for {
		b := byte(value & 0x7f)
		value >>= 7
		if value > 0 {
			b |= 0x80
		}
		w.WriteByte(b)
		if value == 0 {
			break
		}
	}
}
