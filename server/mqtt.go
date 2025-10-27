// Copyright 2020-2025 The NATS Authors
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
	"cmp"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
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
	mqttPubQos1       = byte(0x1 << 1)
	mqttPubQoS2       = byte(0x2 << 1)

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
	mqttConnAckRCQoS2WillRejected            = byte(0x10)

	// Maximum payload size of a control packet
	mqttMaxPayloadSize = 0xFFFFFFF

	// Topic/Filter characters
	mqttTopicLevelSep = '/'
	mqttSingleLevelWC = '+'
	mqttMultiLevelWC  = '#'
	mqttReservedPre   = '$'

	// This is appended to the sid of a subscription that is
	// created on the upper level subject because of the MQTT
	// wildcard '#' semantic.
	mqttMultiLevelSidSuffix = " fwc"

	// This is the prefix for NATS subscriptions subjects associated as delivery
	// subject of JS consumer. We want to make them unique so will prevent users
	// MQTT subscriptions to start with this.
	mqttSubPrefix = "$MQTT.sub."

	// Stream name for MQTT messages on a given account
	mqttStreamName          = "$MQTT_msgs"
	mqttStreamSubjectPrefix = "$MQTT.msgs."

	// Stream name for MQTT retained messages on a given account
	mqttRetainedMsgsStreamName    = "$MQTT_rmsgs"
	mqttRetainedMsgsStreamSubject = "$MQTT.rmsgs."

	// Stream name for MQTT sessions on a given account
	mqttSessStreamName          = "$MQTT_sess"
	mqttSessStreamSubjectPrefix = "$MQTT.sess."

	// Stream name prefix for MQTT sessions on a given account
	mqttSessionsStreamNamePrefix = "$MQTT_sess_"

	// Stream name and subject for incoming MQTT QoS2 messages
	mqttQoS2IncomingMsgsStreamName          = "$MQTT_qos2in"
	mqttQoS2IncomingMsgsStreamSubjectPrefix = "$MQTT.qos2.in."

	// Stream name and subjects for outgoing MQTT QoS (PUBREL) messages
	mqttOutStreamName               = "$MQTT_out"
	mqttOutSubjectPrefix            = "$MQTT.out."
	mqttPubRelSubjectPrefix         = "$MQTT.out.pubrel."
	mqttPubRelDeliverySubjectPrefix = "$MQTT.deliver.pubrel."
	mqttPubRelConsumerDurablePrefix = "$MQTT_PUBREL_"

	// As per spec, MQTT server may not redeliver QoS 1 and 2 messages to
	// clients, except after client reconnects. However, NATS Server will
	// redeliver unacknowledged messages after this default interval. This can
	// be changed with the server.Options.MQTT.AckWait option.
	mqttDefaultAckWait = 30 * time.Second

	// This is the default for the outstanding number of pending QoS 1
	// messages sent to a session with QoS 1 subscriptions.
	mqttDefaultMaxAckPending = 1024

	// A session's list of subscriptions cannot have a cumulative MaxAckPending
	// of more than this limit.
	mqttMaxAckTotalLimit = 0xFFFF

	// Prefix of the reply subject for JS API requests.
	mqttJSARepliesPrefix = "$MQTT.JSA."

	// Those are tokens that are used for the reply subject of JS API requests.
	// For instance "$MQTT.JSA.<node id>.SC.<number>" is the reply subject
	// for a request to create a stream (where <node id> is the server name hash),
	// while "$MQTT.JSA.<node id>.SL.<number>" is for a stream lookup, etc...
	mqttJSAIdTokenPos     = 3
	mqttJSATokenPos       = 4
	mqttJSAClientIDPos    = 5
	mqttJSAStreamCreate   = "SC"
	mqttJSAStreamUpdate   = "SU"
	mqttJSAStreamLookup   = "SL"
	mqttJSAStreamDel      = "SD"
	mqttJSAConsumerCreate = "CC"
	mqttJSAConsumerLookup = "CL"
	mqttJSAConsumerDel    = "CD"
	mqttJSAMsgStore       = "MS"
	mqttJSAMsgLoad        = "ML"
	mqttJSAMsgDelete      = "MD"
	mqttJSASessPersist    = "SP"
	mqttJSARetainedMsgDel = "RD"
	mqttJSAStreamNames    = "SN"

	// This is how long to keep a client in the flappers map before closing the
	// connection. This prevent quick reconnect from those clients that keep
	// wanting to connect with a client ID already in use.
	mqttSessFlappingJailDur = time.Second

	// This is how frequently the timer to cleanup the sessions flappers map is firing.
	mqttSessFlappingCleanupInterval = 5 * time.Second

	// Default retry delay if transfer of old session streams to new one fails
	mqttDefaultTransferRetry = 5 * time.Second

	// For Websocket URLs
	mqttWSPath = "/mqtt"

	mqttInitialPubHeader        = 16 // An overkill, should need 7 bytes max
	mqttProcessSubTooLong       = 100 * time.Millisecond
	mqttDefaultRetainedCacheTTL = 2 * time.Minute
	mqttRetainedTransferTimeout = 10 * time.Second
	mqttDefaultJSAPITimeout     = 5 * time.Second
)

const (
	sparkbNBIRTH = "NBIRTH"
	sparkbDBIRTH = "DBIRTH"
	sparkbNDEATH = "NDEATH"
	sparkbDDEATH = "DDEATH"
)

var (
	sparkbNamespaceTopicPrefix    = []byte("spBv1.0/")
	sparkbCertificatesTopicPrefix = []byte("$sparkplug/certificates/")
)

var (
	mqttPingResponse     = []byte{mqttPacketPingResp, 0x0}
	mqttProtoName        = []byte("MQTT")
	mqttOldProtoName     = []byte("MQIsdp")
	mqttSessJailDur      = mqttSessFlappingJailDur
	mqttFlapCleanItvl    = mqttSessFlappingCleanupInterval
	mqttRetainedCacheTTL = mqttDefaultRetainedCacheTTL
)

var (
	errMQTTNotWebsocketPort           = errors.New("MQTT clients over websocket must connect to the Websocket port, not the MQTT port")
	errMQTTTopicFilterCannotBeEmpty   = errors.New("topic filter cannot be empty")
	errMQTTMalformedVarInt            = errors.New("malformed variable int")
	errMQTTSecondConnectPacket        = errors.New("received a second CONNECT packet")
	errMQTTServerNameMustBeSet        = errors.New("mqtt requires server name to be explicitly set")
	errMQTTUserMixWithUsersNKeys      = errors.New("mqtt authentication username not compatible with presence of users/nkeys")
	errMQTTTokenMixWIthUsersNKeys     = errors.New("mqtt authentication token not compatible with presence of users/nkeys")
	errMQTTAckWaitMustBePositive      = errors.New("ack wait must be a positive value")
	errMQTTJSAPITimeoutMustBePositive = errors.New("JS API timeout must be a positive value")
	errMQTTStandaloneNeedsJetStream   = errors.New("mqtt requires JetStream to be enabled if running in standalone mode")
	errMQTTConnFlagReserved           = errors.New("connect flags reserved bit not set to 0")
	errMQTTWillAndRetainFlag          = errors.New("if Will flag is set to 0, Will Retain flag must be 0 too")
	errMQTTPasswordFlagAndNoUser      = errors.New("password flag set but username flag is not")
	errMQTTCIDEmptyNeedsCleanFlag     = errors.New("when client ID is empty, clean session flag must be set to 1")
	errMQTTEmptyWillTopic             = errors.New("empty Will topic not allowed")
	errMQTTEmptyUsername              = errors.New("empty user name not allowed")
	errMQTTTopicIsEmpty               = errors.New("topic cannot be empty")
	errMQTTPacketIdentifierIsZero     = errors.New("packet identifier cannot be 0")
	errMQTTUnsupportedCharacters      = errors.New("character ' ' not supported for MQTT topics")
	errMQTTInvalidSession             = errors.New("invalid MQTT session")
)

type srvMQTT struct {
	listener     net.Listener
	listenerErr  error
	authOverride bool
	sessmgr      mqttSessionManager
}

type mqttSessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*mqttAccountSessionManager // key is account name
}

var testDisableRMSCache = false

type mqttAccountSessionManager struct {
	mu         sync.RWMutex
	sessions   map[string]*mqttSession        // key is MQTT client ID
	sessByHash map[string]*mqttSession        // key is MQTT client ID hash
	sessLocked map[string]struct{}            // key is MQTT client ID and indicate that a session can not be taken by a new client at this time
	flappers   map[string]int64               // When connection connects with client ID already in use
	flapTimer  *time.Timer                    // Timer to perform some cleanup of the flappers map
	sl         *Sublist                       // sublist allowing to find retained messages for given subscription
	retmsgs    map[string]*mqttRetainedMsgRef // retained messages
	rmsCache   *sync.Map                      // map[subject]mqttRetainedMsg
	jsa        mqttJSA
	rrmLastSeq uint64        // Restore retained messages expected last sequence
	rrmDoneCh  chan struct{} // To notify the caller that all retained messages have been loaded
	domainTk   string        // Domain (with trailing "."), or possibly empty. This is added to session subject.
}

type mqttJSAResponse struct {
	reply string // will be used to map to the original request in jsa.NewRequestExMulti
	value any
}

type mqttJSA struct {
	mu        sync.Mutex
	id        string
	c         *client
	sendq     *ipQueue[*mqttJSPubMsg]
	rplyr     string
	replies   sync.Map // [string]chan *mqttJSAResponse
	nuid      *nuid.NUID
	quitCh    chan struct{}
	domain    string // Domain or possibly empty. This is added to session subject.
	domainSet bool   // covers if domain was set, even to empty
	timeout   time.Duration
}

type mqttJSPubMsg struct {
	subj  string
	reply string
	hdr   int
	msg   []byte
}

type mqttRetMsgDel struct {
	Subject string `json:"subject"`
	Seq     uint64 `json:"seq"`
}

type mqttSession struct {
	// subsMu is a "quick" version of the session lock, sufficient for the QoS0
	// callback. It only guarantees that a new subscription is initialized, and
	// its retained messages if any have been queued up for delivery. The QoS12
	// callback uses the session lock.
	mu     sync.Mutex
	subsMu sync.RWMutex

	id                     string // client ID
	idHash                 string // client ID hash
	c                      *client
	jsa                    *mqttJSA
	subs                   map[string]byte // Key is MQTT SUBSCRIBE filter, value is the subscription QoS
	cons                   map[string]*ConsumerConfig
	pubRelConsumer         *ConsumerConfig
	pubRelSubscribed       bool
	pubRelDeliverySubject  string
	pubRelDeliverySubjectB []byte
	pubRelSubject          string
	seq                    uint64

	// pendingPublish maps packet identifiers (PI) to JetStream ACK subjects for
	// QoS1 and 2 PUBLISH messages pending delivery to the session's client.
	pendingPublish map[uint16]*mqttPending

	// pendingPubRel maps PIs to JetStream ACK subjects for QoS2 PUBREL
	// messages pending delivery to the session's client.
	pendingPubRel map[uint16]*mqttPending

	// cpending maps delivery attempts (that come with a JS ACK subject) to
	// existing PIs.
	cpending map[string]map[uint64]uint16 // composite key: jsDur, sseq

	// "Last used" publish packet identifier (PI). starting point searching for the next available.
	last_pi uint16

	// Maximum number of pending acks for this session.
	maxp     uint16
	tmaxack  int
	clean    bool
	domainTk string
}

type mqttPersistedSession struct {
	Origin string                     `json:"origin,omitempty"`
	ID     string                     `json:"id,omitempty"`
	Clean  bool                       `json:"clean,omitempty"`
	Subs   map[string]byte            `json:"subs,omitempty"`
	Cons   map[string]*ConsumerConfig `json:"cons,omitempty"`
	PubRel *ConsumerConfig            `json:"pubrel,omitempty"`
}

type mqttRetainedMsg struct {
	Origin  string `json:"origin,omitempty"`
	Subject string `json:"subject,omitempty"`
	Topic   string `json:"topic,omitempty"`
	Msg     []byte `json:"msg,omitempty"`
	Flags   byte   `json:"flags,omitempty"`
	Source  string `json:"source,omitempty"`

	expiresFromCache time.Time
}

type mqttRetainedMsgRef struct {
	sseq  uint64
	floor uint64
	sub   *subscription
}

// mqttSub contains fields associated with a MQTT subscription, and is added to
// the main subscription struct for MQTT message delivery subscriptions. The
// delivery callbacks may get invoked before sub.mqtt is set up, so they should
// acquire either sess.mu or sess.subsMu before accessing it.
type mqttSub struct {
	// The sub's QOS and the JS durable name. They can change when
	// re-subscribing, and are used in the delivery callbacks. They can be
	// quickly accessed using sess.subsMu.RLock, or under the main session lock.
	qos   byte
	jsDur string

	// Pending serialization of retained messages to be sent when subscription
	// is registered. The sub's delivery callbacks must wait until `prm` is
	// ready (can block on sess.mu for that, too).
	prm [][]byte

	// If this subscription needs to be checked for being reserved. E.g. '#' or
	// '*' or '*/'.  It is set up at the time of subscription and is immutable
	// after that.
	reserved bool
}

type mqtt struct {
	r    *mqttReader
	cp   *mqttConnectProto
	pp   *mqttPublish
	asm  *mqttAccountSessionManager // quick reference to account session manager, immutable after processConnect()
	sess *mqttSession               // quick reference to session, immutable after processConnect()
	cid  string                     // client ID

	// rejectQoS2Pub tells the MQTT client to not accept QoS2 PUBLISH, instead
	// error and terminate the connection.
	rejectQoS2Pub bool

	// downgradeQOS2Sub tells the MQTT client to downgrade QoS2 SUBSCRIBE
	// requests to QoS1.
	downgradeQoS2Sub bool
}

type mqttPending struct {
	sseq         uint64 // stream sequence
	jsAckSubject string // the ACK subject to send the ack to
	jsDur        string // JS durable name
}

type mqttConnectProto struct {
	rd    time.Duration
	will  *mqttWill
	flags byte
}

type mqttIOReader interface {
	io.Reader
	SetReadDeadline(time.Time) error
}

type mqttReader struct {
	reader mqttIOReader
	buf    []byte
	pos    int
	pstart int
	pbuf   []byte
}

type mqttWriter struct {
	bytes.Buffer
}

type mqttWill struct {
	topic   []byte
	subject []byte
	mapped  []byte
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
	mapped  []byte
	msg     []byte
	sz      int
	pi      uint16
	flags   byte
}

// When we re-encode incoming MQTT PUBLISH messages for NATS delivery, we add
// the following headers:
//   - "Nmqtt-Pub" (*always) indicates that the message originated from MQTT, and
//     contains the original message QoS.
//   - "Nmqtt-Subject" contains the original MQTT subject from mqttParsePub.
//   - "Nmqtt-Mapped" contains the mapping during mqttParsePub.
//
// When we submit a PUBREL for delivery, we add a "Nmqtt-PubRel" header that
// contains the PI.
const (
	// NATS header that indicates that the message originated from MQTT and
	// stores the published message QOS.
	mqttNatsHeader = "Nmqtt-Pub"

	// NATS headers to store retained message metadata (along with the original
	// message as binary).
	mqttNatsRetainedMessageTopic  = "Nmqtt-RTopic"
	mqttNatsRetainedMessageOrigin = "Nmqtt-ROrigin"
	mqttNatsRetainedMessageFlags  = "Nmqtt-RFlags"
	mqttNatsRetainedMessageSource = "Nmqtt-RSource"

	// NATS header that indicates that the message is an MQTT PubRel and stores
	// the PI.
	mqttNatsPubRelHeader = "Nmqtt-PubRel"

	// NATS headers to store the original MQTT subject and the subject mapping.
	mqttNatsHeaderSubject = "Nmqtt-Subject"
	mqttNatsHeaderMapped  = "Nmqtt-Mapped"
)

type mqttParsedPublishNATSHeader struct {
	qos     byte
	subject []byte
	mapped  []byte
}

func (s *Server) startMQTT() {
	if s.isShuttingDown() {
		return
	}

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
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	hl, err = natsListen("tcp", hp)
	s.mqtt.listenerErr = err
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
	go s.acceptConnections(hl, "MQTT", func(conn net.Conn) { s.createMQTTClient(conn, nil) }, nil)
	s.mu.Unlock()
}

// This is similar to createClient() but has some modifications specifi to MQTT clients.
// The comments have been kept to minimum to reduce code size. Check createClient() for
// more details.
func (s *Server) createMQTTClient(conn net.Conn, ws *websocket) *client {
	opts := s.getOpts()

	maxPay := int32(opts.MaxPayload)
	maxSubs := int32(opts.MaxSubs)
	if maxSubs == 0 {
		maxSubs = -1
	}
	now := time.Now()

	mqtt := &mqtt{
		rejectQoS2Pub:    opts.MQTT.rejectQoS2Pub,
		downgradeQoS2Sub: opts.MQTT.downgradeQoS2Sub,
	}
	c := &client{srv: s, nc: conn, mpay: maxPay, msubs: maxSubs, start: now, last: now, mqtt: mqtt, ws: ws}
	c.headers = true
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
	if !s.isRunning() || s.ldm {
		if s.isShuttingDown() {
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

	// Websocket TLS handshake is already done when getting to this function.
	tlsRequired := opts.MQTT.TLSConfig != nil && ws == nil
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
		if err := c.doTLSServerHandshake(tlsHandshakeMQTT, opts.MQTT.TLSConfig, opts.MQTT.TLSTimeout, opts.MQTT.TLSPinnedCerts); err != nil {
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
		c.Debugf("TLS version %s, cipher suite %s", tlsVersion(cs.Version), tls.CipherSuiteName(cs.CipherSuite))
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
	// We have to force the server name to be explicitly set and be unique when
	// in cluster mode.
	if o.ServerName == _EMPTY_ && (o.Cluster.Port != 0 || o.Gateway.Port != 0) {
		return errMQTTServerNameMustBeSet
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
			return errMQTTUserMixWithUsersNKeys
		}
		if mo.Token != _EMPTY_ {
			return errMQTTTokenMixWIthUsersNKeys
		}
	}
	if mo.AckWait < 0 {
		return errMQTTAckWaitMustBePositive
	}
	if mo.JSAPITimeout < 0 {
		return errMQTTJSAPITimeoutMustBePositive
	}
	// If strictly standalone and there is no JS enabled, then it won't work...
	// For leafnodes, we could either have remote(s) and it would be ok, or no
	// remote but accept from a remote side that has "hub" property set, which
	// then would ok too. So we fail only if we have no leafnode config at all.
	if !o.JetStream && o.Cluster.Port == 0 && o.Gateway.Port == 0 &&
		o.LeafNode.Port == 0 && len(o.LeafNode.Remotes) == 0 {
		return errMQTTStandaloneNeedsJetStream
	}
	if err := validatePinnedCerts(mo.TLSPinnedCerts); err != nil {
		return fmt.Errorf("mqtt: %v", err)
	}
	if mo.ConsumerReplicas > 0 && mo.StreamReplicas > 0 && mo.ConsumerReplicas > mo.StreamReplicas {
		return fmt.Errorf("mqtt: consumer_replicas (%v) cannot be higher than stream_replicas (%v)",
			mo.ConsumerReplicas, mo.StreamReplicas)
	}
	return nil
}

// Returns true if this connection is from a MQTT client.
// Lock held on entry.
func (c *client) isMqtt() bool {
	return c.mqtt != nil
}

// If this is an MQTT client, returns the session client ID,
// otherwise returns the empty string.
// Lock held on entry
func (c *client) getMQTTClientID() string {
	if !c.isMqtt() {
		return _EMPTY_
	}
	return c.mqtt.cid
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
	hasMappings := c.in.flags.isSet(hasMappings)
	c.mu.Unlock()

	r.reset(buf)

	var err error
	var b byte
	var pl int
	var complete bool

	for err == nil && r.hasMore() {

		// Keep track of the starting of the packet, in case we have a partial
		r.pstart = r.pos

		// Read packet type and flags
		if b, err = r.readByte("packet type"); err != nil {
			break
		}

		// Packet type
		pt := b & mqttPacketMask

		// If client was not connected yet, the first packet must be
		// a mqttPacketConnect otherwise we fail the connection.
		if !connected && pt != mqttPacketConnect {
			// If the buffer indicates that it may be a websocket handshake
			// but the client is not websocket, it means that the client
			// connected to the MQTT port instead of the Websocket port.
			if bytes.HasPrefix(buf, []byte("GET ")) && !c.isWebsocket() {
				err = errMQTTNotWebsocketPort
			} else {
				err = fmt.Errorf("the first packet should be a CONNECT (%v), got %v", mqttPacketConnect, pt)
			}
			break
		}

		pl, complete, err = r.readPacketLen()
		if err != nil || !complete {
			break
		}

		switch pt {
		// Packets that we receive back when we act as the "sender": PUBACK,
		// PUBREC, PUBCOMP.
		case mqttPacketPubAck:
			var pi uint16
			pi, err = mqttParsePIPacket(r)
			if trace {
				c.traceInOp("PUBACK", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				err = c.mqttProcessPubAck(pi)
			}

		case mqttPacketPubRec:
			var pi uint16
			pi, err = mqttParsePIPacket(r)
			if trace {
				c.traceInOp("PUBREC", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				err = c.mqttProcessPubRec(pi)
			}

		case mqttPacketPubComp:
			var pi uint16
			pi, err = mqttParsePIPacket(r)
			if trace {
				c.traceInOp("PUBCOMP", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				c.mqttProcessPubComp(pi)
			}

		// Packets where we act as the "receiver": PUBLISH, PUBREL, SUBSCRIBE, UNSUBSCRIBE.
		case mqttPacketPub:
			pp := c.mqtt.pp
			pp.flags = b & mqttPacketFlagMask
			err = c.mqttParsePub(r, pl, pp, hasMappings)
			if trace {
				c.traceInOp("PUBLISH", errOrTrace(err, mqttPubTrace(pp)))
				if err == nil {
					c.mqttTraceMsg(pp.msg)
				}
			}
			if err == nil {
				err = s.mqttProcessPub(c, pp, trace)
			}

		case mqttPacketPubRel:
			var pi uint16
			pi, err = mqttParsePIPacket(r)
			if trace {
				c.traceInOp("PUBREL", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				err = s.mqttProcessPubRel(c, pi, trace)
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

		// Packets that we get both as a receiver and sender: PING, CONNECT, DISCONNECT
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
				err = errMQTTSecondConnectPacket
				break
			}
			var rc byte
			var cp *mqttConnectProto
			var sessp bool
			rc, cp, err = c.mqttParseConnect(r, hasMappings)
			// Add the client id to the client's string, regardless of error.
			// We may still get the client_id if the call above fails somewhere
			// after parsing the client ID itself.
			c.ncs.Store(fmt.Sprintf("%s - %q", c, c.mqtt.cid))
			if trace && cp != nil {
				c.traceInOp("CONNECT", errOrTrace(err, c.mqttConnectTrace(cp)))
			}
			if rc != 0 {
				c.mqttEnqueueConnAck(rc, sessp)
				if trace {
					c.traceOutOp("CONNACK", []byte(fmt.Sprintf("sp=%v rc=%v", sessp, rc)))
				}
			} else if err == nil {
				if err = s.mqttProcessConnect(c, cp, trace); err != nil {
					err = fmt.Errorf("unable to connect: %v", err)
				} else {
					// Add this debug statement so users running in Debug mode
					// will have the client id printed here for the first time.
					c.Debugf("Client connected")
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
			s.mqttHandleClosedClient(c)
			c.closeConnection(ClientClosed)
			return nil

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

// The MQTT client connection has been closed, or the DISCONNECT packet was received.
// For a "clean" session, we will delete the session, otherwise, simply removing
// the binding. We will also send the "will" message if applicable.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttHandleClosedClient(c *client) {
	c.mu.Lock()
	asm := c.mqtt.asm
	sess := c.mqtt.sess
	c.mu.Unlock()

	// If asm or sess are nil, it means that we have failed a client
	// before it was associated with a session, so nothing more to do.
	if asm == nil || sess == nil {
		return
	}

	// Add this session to the locked map for the rest of the execution.
	if err := asm.lockSession(sess, c); err != nil {
		return
	}
	defer asm.unlockSession(sess)

	asm.mu.Lock()
	// Clear the client from the session, but session may stay.
	sess.mu.Lock()
	sess.c = nil
	doClean := sess.clean
	sess.mu.Unlock()
	// If it was a clean session, then we remove from the account manager,
	// and we will call clear() outside of any lock.
	if doClean {
		asm.removeSession(sess, false)
	}
	// Remove in case it was in the flappers map.
	asm.removeSessFromFlappers(sess.id)
	asm.mu.Unlock()

	// This needs to be done outside of any lock.
	if doClean {
		if err := sess.clear(true); err != nil {
			c.Errorf(err.Error())
		}
	}

	// Now handle the "will". This function will be a no-op if there is no "will" to send.
	s.mqttHandleWill(c)
}

// Updates the MaxAckPending for all MQTT sessions, updating the
// JetStream consumers and updating their max ack pending and forcing
// a expiration of pending messages.
//
// Runs from a server configuration reload routine.
// No lock held on entry.
func (s *Server) mqttUpdateMaxAckPending(newmaxp uint16) {
	msm := &s.mqtt.sessmgr
	s.accounts.Range(func(k, _ any) bool {
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
			sess.mu.Unlock()
		}
		asm.mu.RUnlock()
		return true
	})
}

func (s *Server) mqttGetJSAForAccount(acc string) *mqttJSA {
	sm := &s.mqtt.sessmgr

	sm.mu.RLock()
	asm := sm.sessions[acc]
	sm.mu.RUnlock()

	if asm == nil {
		return nil
	}

	asm.mu.RLock()
	jsa := &asm.jsa
	asm.mu.RUnlock()
	return jsa
}

func (s *Server) mqttStoreQoSMsgForAccountOnNewSubject(hdr int, msg []byte, acc, subject string) {
	if s == nil || hdr <= 0 {
		return
	}
	h := mqttParsePublishNATSHeader(msg[:hdr])
	if h == nil || h.qos == 0 {
		return
	}
	jsa := s.mqttGetJSAForAccount(acc)
	if jsa == nil {
		return
	}
	jsa.storeMsg(mqttStreamSubjectPrefix+subject, hdr, msg)
}

func mqttParsePublishNATSHeader(headerBytes []byte) *mqttParsedPublishNATSHeader {
	if len(headerBytes) == 0 {
		return nil
	}

	pubValue := getHeader(mqttNatsHeader, headerBytes)
	if len(pubValue) == 0 {
		return nil
	}
	return &mqttParsedPublishNATSHeader{
		qos:     pubValue[0] - '0',
		subject: getHeader(mqttNatsHeaderSubject, headerBytes),
		mapped:  getHeader(mqttNatsHeaderMapped, headerBytes),
	}
}

func mqttParsePubRelNATSHeader(headerBytes []byte) uint16 {
	if len(headerBytes) == 0 {
		return 0
	}

	pubrelValue := getHeader(mqttNatsPubRelHeader, headerBytes)
	if len(pubrelValue) == 0 {
		return 0
	}
	pi, _ := strconv.ParseUint(string(pubrelValue), 10, 16)
	return uint16(pi)
}

// Returns the MQTT sessions manager for a given account.
// If new, creates the required JetStream streams/consumers
// for handling of sessions and messages.
func (s *Server) getOrCreateMQTTAccountSessionManager(c *client) (*mqttAccountSessionManager, error) {
	sm := &s.mqtt.sessmgr

	c.mu.Lock()
	acc := c.acc
	c.mu.Unlock()
	accName := acc.GetName()

	sm.mu.RLock()
	asm, ok := sm.sessions[accName]
	sm.mu.RUnlock()

	if ok {
		return asm, nil
	}

	// We will pass the quitCh to the account session manager if we happen to create it.
	s.mu.Lock()
	quitCh := s.quitCh
	s.mu.Unlock()

	// Not found, now take the write lock and check again
	sm.mu.Lock()
	defer sm.mu.Unlock()
	asm, ok = sm.sessions[accName]
	if ok {
		return asm, nil
	}
	// Need to create one here.
	asm, err := s.mqttCreateAccountSessionManager(acc, quitCh)
	if err != nil {
		return nil, err
	}
	sm.sessions[accName] = asm
	return asm, nil
}

// Creates JS streams/consumers for handling of sessions and messages for this account.
//
// Global session manager lock is held on entry.
func (s *Server) mqttCreateAccountSessionManager(acc *Account, quitCh chan struct{}) (*mqttAccountSessionManager, error) {
	var err error

	accName := acc.GetName()

	opts := s.getOpts()
	c := s.createInternalAccountClient()
	c.acc = acc

	id := s.NodeName()

	mqttJSAPITimeout := opts.MQTT.JSAPITimeout
	if mqttJSAPITimeout == 0 {
		mqttJSAPITimeout = mqttDefaultJSAPITimeout
	}

	replicas := opts.MQTT.StreamReplicas
	if replicas <= 0 {
		replicas = s.mqttDetermineReplicas()
	}

	streamStorage := FileStorage
	if opts.MQTT.StreamMemoryStorage {
		streamStorage = MemoryStorage
	}

	qname := fmt.Sprintf("[ACC:%s] MQTT ", accName)
	as := &mqttAccountSessionManager{
		sessions:   make(map[string]*mqttSession),
		sessByHash: make(map[string]*mqttSession),
		sessLocked: make(map[string]struct{}),
		flappers:   make(map[string]int64),
		jsa: mqttJSA{
			id:      id,
			c:       c,
			rplyr:   mqttJSARepliesPrefix + id + ".",
			sendq:   newIPQueue[*mqttJSPubMsg](s, qname+"send"),
			nuid:    nuid.New(),
			quitCh:  quitCh,
			timeout: mqttJSAPITimeout,
		},
	}
	if !testDisableRMSCache {
		as.rmsCache = &sync.Map{}
	}
	// TODO record domain name in as here

	// The domain to communicate with may be required for JS calls.
	// Search from specific (per account setting) to generic (mqtt setting)
	if opts.JsAccDefaultDomain != nil {
		if d, ok := opts.JsAccDefaultDomain[accName]; ok {
			if d != _EMPTY_ {
				as.jsa.domain = d
			}
			as.jsa.domainSet = true
		}
		// in case domain was set to empty, check if there are more generic domain overwrites
	}
	if as.jsa.domain == _EMPTY_ {
		if d := opts.MQTT.JsDomain; d != _EMPTY_ {
			as.jsa.domain = d
			as.jsa.domainSet = true
		}
	}
	// We need to include the domain in the subject prefix used to store sessions in the $MQTT_sess stream.
	if as.jsa.domainSet {
		if as.jsa.domain != _EMPTY_ {
			as.domainTk = as.jsa.domain + "."
		}
	} else if d := s.getOpts().JetStreamDomain; d != _EMPTY_ {
		as.domainTk = d + "."
	}
	if as.jsa.domainSet {
		s.Noticef("Creating MQTT streams/consumers with replicas %v for account %q in domain %q", replicas, accName, as.jsa.domain)
	} else {
		s.Noticef("Creating MQTT streams/consumers with replicas %v for account %q", replicas, accName)
	}

	var subs []*subscription
	var success bool
	closeCh := make(chan struct{})

	defer func() {
		if success {
			return
		}
		for _, sub := range subs {
			c.processUnsub(sub.sid)
		}
		close(closeCh)
	}()

	// We create all subscriptions before starting the go routine that will do
	// sends otherwise we could get races.
	// Note that using two different clients (one for the subs, one for the
	// sends) would cause other issues such as registration of recent subs in
	// the "sub" client would be invisible to the check for GW routed replies
	// (shouldMapReplyForGatewaySend) since the client there would be the "sender".

	jsa := &as.jsa
	sid := int64(1)
	// This is a subscription that will process all JS API replies. We could split to
	// individual subscriptions if needed, but since there is a bit of common code,
	// that seemed like a good idea to be all in one place.
	if err := as.createSubscription(jsa.rplyr+">",
		as.processJSAPIReplies, &sid, &subs); err != nil {
		return nil, err
	}

	// We will listen for replies to session persist requests so that we can
	// detect the use of a session with the same client ID anywhere in the cluster.
	//   `$MQTT.JSA.{js-id}.SP.{client-id-hash}.{uuid}`
	if err := as.createSubscription(mqttJSARepliesPrefix+"*."+mqttJSASessPersist+".*.*",
		as.processSessionPersist, &sid, &subs); err != nil {
		return nil, err
	}

	// We create the subscription on "$MQTT.sub.<nuid>" to limit the subjects
	// that a user would allow permissions on.
	rmsubj := mqttSubPrefix + nuid.Next()
	if err := as.createSubscription(rmsubj, as.processRetainedMsg, &sid, &subs); err != nil {
		return nil, err
	}

	// Create a subscription to be notified of retained messages delete requests.
	rmdelsubj := mqttJSARepliesPrefix + "*." + mqttJSARetainedMsgDel
	if err := as.createSubscription(rmdelsubj, as.processRetainedMsgDel, &sid, &subs); err != nil {
		return nil, err
	}

	// No more creation of subscriptions past this point otherwise RACEs may happen.

	// Start the go routine that will send JS API requests.
	s.startGoRoutine(func() {
		defer s.grWG.Done()
		as.sendJSAPIrequests(s, c, accName, closeCh)
	})

	// Start the go routine that will clean up cached retained messages that expired.
	if as.rmsCache != nil {
		s.startGoRoutine(func() {
			defer s.grWG.Done()
			as.cleanupRetainedMessageCache(s, closeCh)
		})
	}

	lookupStream := func(stream, txt string) (*StreamInfo, error) {
		si, err := jsa.lookupStream(stream)
		if err != nil {
			if IsNatsErr(err, JSStreamNotFoundErr) {
				return nil, nil
			}
			return nil, fmt.Errorf("lookup %s stream for account %q: %v", txt, accName, err)
		}
		if opts.MQTT.StreamReplicas == 0 {
			return si, nil
		}
		sr := 1
		if si.Cluster != nil {
			sr += len(si.Cluster.Replicas)
		}
		if replicas != sr {
			s.Warnf("MQTT %s stream replicas mismatch: current is %v but configuration is %v for '%s > %s'",
				txt, sr, replicas, accName, stream)
		}
		return si, nil
	}

	if si, err := lookupStream(mqttSessStreamName, "sessions"); err != nil {
		return nil, err
	} else if si == nil {
		// Create the stream for the sessions.
		cfg := &StreamConfig{
			Name:       mqttSessStreamName,
			Subjects:   []string{mqttSessStreamSubjectPrefix + as.domainTk + ">"},
			Storage:    streamStorage,
			Retention:  LimitsPolicy,
			Replicas:   replicas,
			MaxMsgsPer: 1,
		}
		if _, created, err := jsa.createStream(cfg); err == nil && created {
			as.transferUniqueSessStreamsToMuxed(s)
		} else if isErrorOtherThan(err, JSStreamNameExistErr) {
			return nil, fmt.Errorf("create sessions stream for account %q: %v", accName, err)
		}
	}

	if si, err := lookupStream(mqttStreamName, "messages"); err != nil {
		return nil, err
	} else if si == nil {
		// Create the stream for the messages.
		cfg := &StreamConfig{
			Name:      mqttStreamName,
			Subjects:  []string{mqttStreamSubjectPrefix + ">"},
			Storage:   streamStorage,
			Retention: InterestPolicy,
			Replicas:  replicas,
		}
		if _, _, err := jsa.createStream(cfg); isErrorOtherThan(err, JSStreamNameExistErr) {
			return nil, fmt.Errorf("create messages stream for account %q: %v", accName, err)
		}
	}

	if si, err := lookupStream(mqttQoS2IncomingMsgsStreamName, "QoS2 incoming messages"); err != nil {
		return nil, err
	} else if si == nil {
		// Create the stream for the incoming QoS2 messages that have not been
		// PUBREL-ed by the sender. Subject is
		// "$MQTT.qos2.<session>.<PI>", the .PI is to achieve exactly
		// once for each PI.
		cfg := &StreamConfig{
			Name:          mqttQoS2IncomingMsgsStreamName,
			Subjects:      []string{mqttQoS2IncomingMsgsStreamSubjectPrefix + ">"},
			Storage:       streamStorage,
			Retention:     LimitsPolicy,
			Discard:       DiscardNew,
			MaxMsgsPer:    1,
			DiscardNewPer: true,
			Replicas:      replicas,
		}
		if _, _, err := jsa.createStream(cfg); isErrorOtherThan(err, JSStreamNameExistErr) {
			return nil, fmt.Errorf("create QoS2 incoming messages stream for account %q: %v", accName, err)
		}
	}

	if si, err := lookupStream(mqttOutStreamName, "QoS2 outgoing PUBREL"); err != nil {
		return nil, err
	} else if si == nil {
		// Create the stream for the incoming QoS2 messages that have not been
		// PUBREL-ed by the sender. NATS messages are submitted as
		// "$MQTT.pubrel.<session hash>"
		cfg := &StreamConfig{
			Name:      mqttOutStreamName,
			Subjects:  []string{mqttOutSubjectPrefix + ">"},
			Storage:   streamStorage,
			Retention: InterestPolicy,
			Replicas:  replicas,
		}
		if _, _, err := jsa.createStream(cfg); isErrorOtherThan(err, JSStreamNameExistErr) {
			return nil, fmt.Errorf("create QoS2 outgoing PUBREL stream for account %q: %v", accName, err)
		}
	}

	// This is the only case where we need "si" after lookup/create
	needToTransfer := true
	si, err := lookupStream(mqttRetainedMsgsStreamName, "retained messages")
	switch {
	case err != nil:
		return nil, err

	case si == nil:
		// Create the stream for retained messages.
		cfg := &StreamConfig{
			Name:       mqttRetainedMsgsStreamName,
			Subjects:   []string{mqttRetainedMsgsStreamSubject + ">"},
			Storage:    streamStorage,
			Retention:  LimitsPolicy,
			Replicas:   replicas,
			MaxMsgsPer: 1,
		}
		// We will need "si" outside of this block.
		si, _, err = jsa.createStream(cfg)
		if err != nil {
			if isErrorOtherThan(err, JSStreamNameExistErr) {
				return nil, fmt.Errorf("create retained messages stream for account %q: %v", accName, err)
			}
			// Suppose we had a race and the stream was actually created by another
			// node, we really need "si" after that, so lookup the stream again here.
			si, err = lookupStream(mqttRetainedMsgsStreamName, "retained messages")
			if err != nil {
				return nil, err
			}
		}
		needToTransfer = false

	default:
		needToTransfer = si.Config.MaxMsgsPer != 1
	}

	// Doing this check outside of above if/else due to possible race when
	// creating the stream.
	wantedSubj := mqttRetainedMsgsStreamSubject + ">"
	if len(si.Config.Subjects) != 1 || si.Config.Subjects[0] != wantedSubj {
		// Update only the Subjects at this stage, not MaxMsgsPer yet.
		si.Config.Subjects = []string{wantedSubj}
		if si, err = jsa.updateStream(&si.Config); err != nil {
			return nil, fmt.Errorf("failed to update stream config: %w", err)
		}
	}

	transferRMS := func() error {
		if !needToTransfer {
			return nil
		}

		as.transferRetainedToPerKeySubjectStream(s)

		// We need another lookup to have up-to-date si.State values in order
		// to load all retained messages.
		si, err = lookupStream(mqttRetainedMsgsStreamName, "retained messages")
		if err != nil {
			return err
		}
		needToTransfer = false
		return nil
	}

	// Attempt to transfer all "single subject" retained messages to new
	// subjects. It may fail, will log its own error; ignore it the first time
	// and proceed to updating MaxMsgsPer. Then we invoke transferRMS() again,
	// which will get another chance to resolve the error; if not we bail there.
	if err = transferRMS(); err != nil {
		return nil, err
	}

	// Now, if the stream does not have MaxMsgsPer set to 1, and there are no
	// more messages on the single $MQTT.rmsgs subject, update the stream again.
	if si.Config.MaxMsgsPer != 1 {
		si.Config.MaxMsgsPer = 1
		// We will need an up-to-date si, so don't use local variable here.
		if si, err = jsa.updateStream(&si.Config); err != nil {
			return nil, fmt.Errorf("failed to update stream config: %w", err)
		}
	}

	// If we failed the first time, there is now at most one lingering message
	// in the old subject. Try again (it will be a NO-OP if succeeded the first
	// time).
	if err = transferRMS(); err != nil {
		return nil, err
	}

	var lastSeq uint64
	var rmDoneCh chan struct{}
	st := si.State
	if st.Msgs > 0 {
		lastSeq = st.LastSeq
		if lastSeq > 0 {
			rmDoneCh = make(chan struct{})
			as.rrmLastSeq = lastSeq
			as.rrmDoneCh = rmDoneCh
		}
	}

	// Opportunistically delete the old (legacy) consumer, from v2.10.10 and
	// before. Ignore any errors that might arise.
	rmLegacyDurName := mqttRetainedMsgsStreamName + "_" + jsa.id
	jsa.deleteConsumer(mqttRetainedMsgsStreamName, rmLegacyDurName, true)

	// Create a new, uniquely names consumer for retained messages for this
	// server. The prior one will expire eventually.
	ccfg := &CreateConsumerRequest{
		Stream: mqttRetainedMsgsStreamName,
		Config: ConsumerConfig{
			Name:              mqttRetainedMsgsStreamName + "_" + nuid.Next(),
			FilterSubject:     mqttRetainedMsgsStreamSubject + ">",
			DeliverSubject:    rmsubj,
			ReplayPolicy:      ReplayInstant,
			AckPolicy:         AckNone,
			InactiveThreshold: 5 * time.Minute,
		},
	}
	if _, err := jsa.createEphemeralConsumer(ccfg); err != nil {
		return nil, fmt.Errorf("create retained messages consumer for account %q: %v", accName, err)
	}

	if lastSeq > 0 {
		ttl := time.NewTimer(mqttJSAPITimeout)
		defer ttl.Stop()

		select {
		case <-rmDoneCh:
		case <-ttl.C:
			s.Warnf("Timing out waiting to load %v retained messages", st.Msgs)
		case <-quitCh:
			return nil, ErrServerNotRunning
		}
	}

	// Set this so that on defer we don't cleanup.
	success = true

	return as, nil
}

func (s *Server) mqttDetermineReplicas() int {
	// If not clustered, then replica will be 1.
	if !s.JetStreamIsClustered() {
		return 1
	}
	opts := s.getOpts()
	replicas := 0
	for _, u := range opts.Routes {
		host := u.Hostname()
		// If this is an IP just add one.
		if net.ParseIP(host) != nil {
			replicas++
		} else {
			addrs, _ := net.LookupHost(host)
			replicas += len(addrs)
		}
	}
	if replicas < 1 {
		replicas = 1
	} else if replicas > 3 {
		replicas = 3
	}
	return replicas
}

//////////////////////////////////////////////////////////////////////////////
//
// JS APIs related functions
//
//////////////////////////////////////////////////////////////////////////////

func (jsa *mqttJSA) newRequest(kind, subject string, hdr int, msg []byte) (any, error) {
	return jsa.newRequestEx(kind, subject, _EMPTY_, hdr, msg)
}

func (jsa *mqttJSA) prefixDomain(subject string) string {
	if jsa.domain != _EMPTY_ {
		// rewrite js api prefix with domain
		if sub := strings.TrimPrefix(subject, JSApiPrefix+"."); sub != subject {
			subject = fmt.Sprintf("$JS.%s.API.%s", jsa.domain, sub)
		}
	}
	return subject
}

func (jsa *mqttJSA) newRequestEx(kind, subject, cidHash string, hdr int, msg []byte) (any, error) {
	responses, err := jsa.newRequestExMulti(kind, subject, cidHash, []int{hdr}, [][]byte{msg})
	if err != nil {
		return nil, err
	}
	if len(responses) != 1 {
		return nil, fmt.Errorf("unreachable: invalid number of responses (%d)", len(responses))
	}
	return responses[0].value, nil
}

// newRequestExMulti sends multiple messages on the same subject and waits for
// all responses. It returns the same number of responses in the same order as
// msgs parameter. In case of a timeout it returns an error as well as all
// responses received as a sparsely populated array, matching msgs, with nils
// for the values that have not yet been received.
//
// Note that each response may represent an error and should be inspected as
// such by the caller.
func (jsa *mqttJSA) newRequestExMulti(kind, subject, cidHash string, hdrs []int, msgs [][]byte) ([]*mqttJSAResponse, error) {
	if len(hdrs) != len(msgs) {
		return nil, fmt.Errorf("unreachable: invalid number of messages (%d) or header offsets (%d)", len(msgs), len(hdrs))
	}
	responseCh := make(chan *mqttJSAResponse, len(msgs))

	// Generate and queue all outgoing requests, have all results reported to
	// responseCh, and store a map of reply subjects to the original subjects'
	// indices.
	r2i := map[string]int{}
	for i, msg := range msgs {
		hdr := hdrs[i]
		var sb strings.Builder
		// Either we use nuid.Next() which uses a global lock, or our own nuid object, but
		// then it needs to be "write" protected. This approach will reduce across account
		// contention since we won't use the global nuid's lock.
		jsa.mu.Lock()
		uid := jsa.nuid.Next()
		sb.WriteString(jsa.rplyr)
		jsa.mu.Unlock()

		sb.WriteString(kind)
		sb.WriteByte(btsep)
		if cidHash != _EMPTY_ {
			sb.WriteString(cidHash)
			sb.WriteByte(btsep)
		}
		sb.WriteString(uid)
		reply := sb.String()

		// Add responseCh to the reply channel map. It will be cleaned out on
		// timeout (see below), or in processJSAPIReplies upon receiving the
		// response.
		jsa.replies.Store(reply, responseCh)

		subject = jsa.prefixDomain(subject)
		jsa.sendq.push(&mqttJSPubMsg{
			subj:  subject,
			reply: reply,
			hdr:   hdr,
			msg:   msg,
		})
		r2i[reply] = i
	}

	// Wait for all responses to come back, or for the timeout to expire. We
	// don't want to use time.After() which causes memory growth because the
	// timer can't be stopped and will need to expire to then be garbage
	// collected.
	c := 0
	responses := make([]*mqttJSAResponse, len(msgs))
	start := time.Now()
	t := time.NewTimer(jsa.timeout)
	defer t.Stop()
	for {
		select {
		case r := <-responseCh:
			i := r2i[r.reply]
			responses[i] = r
			c++
			if c == len(msgs) {
				return responses, nil
			}

		case <-jsa.quitCh:
			return nil, ErrServerNotRunning

		case <-t.C:
			var reply string
			now := time.Now()
			for reply = range r2i { // preserve the last value for Errorf
				jsa.replies.Delete(reply)
			}

			if len(msgs) == 1 {
				return responses, fmt.Errorf("timeout after %v: request type %q on %q (reply=%q)", now.Sub(start), kind, subject, reply)
			} else {
				return responses, fmt.Errorf("timeout after %v: request type %q on %q: got %d out of %d", now.Sub(start), kind, subject, c, len(msgs))
			}
		}
	}
}

func (jsa *mqttJSA) sendAck(ackSubject string) {
	// We pass -1 for the hdr so that the send loop does not need to
	// add the "client info" header. This is not a JS API request per se.
	jsa.sendMsg(ackSubject, nil)
}

func (jsa *mqttJSA) sendMsg(subj string, msg []byte) {
	if subj == _EMPTY_ {
		return
	}
	jsa.sendq.push(&mqttJSPubMsg{subj: subj, msg: msg, hdr: -1})
}

func (jsa *mqttJSA) createEphemeralConsumer(cfg *CreateConsumerRequest) (*JSApiConsumerCreateResponse, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	subj := fmt.Sprintf(JSApiConsumerCreateT, cfg.Stream)
	ccri, err := jsa.newRequest(mqttJSAConsumerCreate, subj, 0, cfgb)
	if err != nil {
		return nil, err
	}
	ccr := ccri.(*JSApiConsumerCreateResponse)
	return ccr, ccr.ToError()
}

func (jsa *mqttJSA) createDurableConsumer(cfg *CreateConsumerRequest) (*JSApiConsumerCreateResponse, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	subj := fmt.Sprintf(JSApiDurableCreateT, cfg.Stream, cfg.Config.Durable)
	ccri, err := jsa.newRequest(mqttJSAConsumerCreate, subj, 0, cfgb)
	if err != nil {
		return nil, err
	}
	ccr := ccri.(*JSApiConsumerCreateResponse)
	return ccr, ccr.ToError()
}

// if noWait is specified, does not wait for the JS response, returns nil
func (jsa *mqttJSA) deleteConsumer(streamName, consName string, noWait bool) (*JSApiConsumerDeleteResponse, error) {
	subj := fmt.Sprintf(JSApiConsumerDeleteT, streamName, consName)
	if noWait {
		jsa.sendMsg(subj, nil)
		return nil, nil
	}

	cdri, err := jsa.newRequest(mqttJSAConsumerDel, subj, 0, nil)
	if err != nil {
		return nil, err
	}
	cdr := cdri.(*JSApiConsumerDeleteResponse)
	return cdr, cdr.ToError()
}

func (jsa *mqttJSA) createStream(cfg *StreamConfig) (*StreamInfo, bool, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, false, err
	}
	scri, err := jsa.newRequest(mqttJSAStreamCreate, fmt.Sprintf(JSApiStreamCreateT, cfg.Name), 0, cfgb)
	if err != nil {
		return nil, false, err
	}
	scr := scri.(*JSApiStreamCreateResponse)
	return scr.StreamInfo, scr.DidCreate, scr.ToError()
}

func (jsa *mqttJSA) updateStream(cfg *StreamConfig) (*StreamInfo, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	scri, err := jsa.newRequest(mqttJSAStreamUpdate, fmt.Sprintf(JSApiStreamUpdateT, cfg.Name), 0, cfgb)
	if err != nil {
		return nil, err
	}
	scr := scri.(*JSApiStreamUpdateResponse)
	return scr.StreamInfo, scr.ToError()
}

func (jsa *mqttJSA) lookupStream(name string) (*StreamInfo, error) {
	slri, err := jsa.newRequest(mqttJSAStreamLookup, fmt.Sprintf(JSApiStreamInfoT, name), 0, nil)
	if err != nil {
		return nil, err
	}
	slr := slri.(*JSApiStreamInfoResponse)
	return slr.StreamInfo, slr.ToError()
}

func (jsa *mqttJSA) deleteStream(name string) (bool, error) {
	sdri, err := jsa.newRequest(mqttJSAStreamDel, fmt.Sprintf(JSApiStreamDeleteT, name), 0, nil)
	if err != nil {
		return false, err
	}
	sdr := sdri.(*JSApiStreamDeleteResponse)
	return sdr.Success, sdr.ToError()
}

func (jsa *mqttJSA) loadLastMsgFor(streamName string, subject string) (*StoredMsg, error) {
	mreq := &JSApiMsgGetRequest{LastFor: subject}
	req, err := json.Marshal(mreq)
	if err != nil {
		return nil, err
	}
	lmri, err := jsa.newRequest(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), 0, req)
	if err != nil {
		return nil, err
	}
	lmr := lmri.(*JSApiMsgGetResponse)
	return lmr.Message, lmr.ToError()
}

func (jsa *mqttJSA) loadLastMsgForMulti(streamName string, subjects []string) ([]*JSApiMsgGetResponse, error) {
	marshaled := make([][]byte, 0, len(subjects))
	headerBytes := make([]int, 0, len(subjects))
	for _, subject := range subjects {
		mreq := &JSApiMsgGetRequest{LastFor: subject}
		bb, err := json.Marshal(mreq)
		if err != nil {
			return nil, err
		}
		marshaled = append(marshaled, bb)
		headerBytes = append(headerBytes, 0)
	}

	all, err := jsa.newRequestExMulti(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), _EMPTY_, headerBytes, marshaled)
	// all has the same order as subjects, preserve it as we unmarshal
	responses := make([]*JSApiMsgGetResponse, len(all))
	for i, v := range all {
		if v != nil {
			responses[i] = v.value.(*JSApiMsgGetResponse)
		}
	}
	return responses, err
}

func (jsa *mqttJSA) loadNextMsgFor(streamName string, subject string) (*StoredMsg, error) {
	mreq := &JSApiMsgGetRequest{NextFor: subject}
	req, err := json.Marshal(mreq)
	if err != nil {
		return nil, err
	}
	lmri, err := jsa.newRequest(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), 0, req)
	if err != nil {
		return nil, err
	}
	lmr := lmri.(*JSApiMsgGetResponse)
	return lmr.Message, lmr.ToError()
}

func (jsa *mqttJSA) loadMsg(streamName string, seq uint64) (*StoredMsg, error) {
	mreq := &JSApiMsgGetRequest{Seq: seq}
	req, err := json.Marshal(mreq)
	if err != nil {
		return nil, err
	}
	lmri, err := jsa.newRequest(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), 0, req)
	if err != nil {
		return nil, err
	}
	lmr := lmri.(*JSApiMsgGetResponse)
	return lmr.Message, lmr.ToError()
}

func (jsa *mqttJSA) storeMsg(subject string, headers int, msg []byte) (*JSPubAckResponse, error) {
	return jsa.storeMsgWithKind(mqttJSAMsgStore, subject, headers, msg)
}

func (jsa *mqttJSA) storeMsgWithKind(kind, subject string, headers int, msg []byte) (*JSPubAckResponse, error) {
	smri, err := jsa.newRequest(kind, subject, headers, msg)
	if err != nil {
		return nil, err
	}
	smr := smri.(*JSPubAckResponse)
	return smr, smr.ToError()
}

func (jsa *mqttJSA) storeSessionMsg(domainTk, cidHash string, hdr int, msg []byte) (*JSPubAckResponse, error) {
	// Compute subject where the session is being stored
	subject := mqttSessStreamSubjectPrefix + domainTk + cidHash

	// Passing cidHash will add it to the JS reply subject, so that we can use
	// it in processSessionPersist.
	smri, err := jsa.newRequestEx(mqttJSASessPersist, subject, cidHash, hdr, msg)
	if err != nil {
		return nil, err
	}
	smr := smri.(*JSPubAckResponse)
	return smr, smr.ToError()
}

func (jsa *mqttJSA) loadSessionMsg(domainTk, cidHash string) (*StoredMsg, error) {
	subject := mqttSessStreamSubjectPrefix + domainTk + cidHash
	return jsa.loadLastMsgFor(mqttSessStreamName, subject)
}

func (jsa *mqttJSA) deleteMsg(stream string, seq uint64, wait bool) error {
	dreq := JSApiMsgDeleteRequest{Seq: seq, NoErase: true}
	req, _ := json.Marshal(dreq)
	subj := jsa.prefixDomain(fmt.Sprintf(JSApiMsgDeleteT, stream))
	if !wait {
		jsa.sendq.push(&mqttJSPubMsg{
			subj: subj,
			msg:  req,
		})
		return nil
	}
	dmi, err := jsa.newRequest(mqttJSAMsgDelete, subj, 0, req)
	if err != nil {
		return err
	}
	dm := dmi.(*JSApiMsgDeleteResponse)
	return dm.ToError()
}

//////////////////////////////////////////////////////////////////////////////
//
// Account Sessions Manager related functions
//
//////////////////////////////////////////////////////////////////////////////

// Returns true if `err` is not nil and does not match the api error with ErrorIdentifier id
func isErrorOtherThan(err error, id ErrorIdentifier) bool {
	return err != nil && !IsNatsErr(err, id)
}

// Process JS API replies.
//
// Can run from various go routines (consumer's loop, system send loop, etc..).
func (as *mqttAccountSessionManager) processJSAPIReplies(_ *subscription, pc *client, _ *Account, subject, _ string, msg []byte) {
	token := tokenAt(subject, mqttJSATokenPos)
	if token == _EMPTY_ {
		return
	}
	jsa := &as.jsa
	chi, ok := jsa.replies.Load(subject)
	if !ok {
		return
	}
	jsa.replies.Delete(subject)
	ch := chi.(chan *mqttJSAResponse)
	out := func(value any) {
		ch <- &mqttJSAResponse{reply: subject, value: value}
	}
	switch token {
	case mqttJSAStreamCreate:
		var resp = &JSApiStreamCreateResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAStreamUpdate:
		var resp = &JSApiStreamUpdateResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAStreamLookup:
		var resp = &JSApiStreamInfoResponse{}
		if err := json.Unmarshal(msg, &resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAStreamDel:
		var resp = &JSApiStreamDeleteResponse{}
		if err := json.Unmarshal(msg, &resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAConsumerCreate:
		var resp = &JSApiConsumerCreateResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAConsumerDel:
		var resp = &JSApiConsumerDeleteResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAMsgStore, mqttJSASessPersist:
		var resp = &JSPubAckResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAMsgLoad:
		var resp = &JSApiMsgGetResponse{}
		if err := json.Unmarshal(msg, &resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAStreamNames:
		var resp = &JSApiStreamNamesResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAMsgDelete:
		var resp = &JSApiMsgDeleteResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	default:
		pc.Warnf("Unknown reply code %q", token)
	}
}

// This will both load all retained messages and process updates from the cluster.
//
// Run from various go routines (JS consumer, etc..).
// No lock held on entry.
func (as *mqttAccountSessionManager) processRetainedMsg(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	h, m := c.msgParts(rmsg)
	rm, err := mqttDecodeRetainedMessage(h, m)
	if err != nil {
		return
	}
	// If lastSeq is 0 (nothing to recover, or done doing it) and this is
	// from our own server, ignore.
	as.mu.RLock()
	if as.rrmLastSeq == 0 && rm.Origin == as.jsa.id {
		as.mu.RUnlock()
		return
	}
	as.mu.RUnlock()

	// At this point we either recover from our own server, or process a remote retained message.
	seq, _, _ := ackReplyInfo(reply)

	// Handle this retained message, no need to copy the bytes.
	as.handleRetainedMsg(rm.Subject, &mqttRetainedMsgRef{sseq: seq}, rm, false)

	// If we were recovering (lastSeq > 0), then check if we are done.
	as.mu.Lock()
	if as.rrmLastSeq > 0 && seq >= as.rrmLastSeq {
		as.rrmLastSeq = 0
		close(as.rrmDoneCh)
		as.rrmDoneCh = nil
	}
	as.mu.Unlock()
}

func (as *mqttAccountSessionManager) processRetainedMsgDel(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	idHash := tokenAt(subject, 3)
	if idHash == _EMPTY_ || idHash == as.jsa.id {
		return
	}
	_, msg := c.msgParts(rmsg)
	if len(msg) < LEN_CR_LF {
		return
	}
	var drm mqttRetMsgDel
	if err := json.Unmarshal(msg, &drm); err != nil {
		return
	}
	as.handleRetainedMsgDel(drm.Subject, drm.Seq)
}

// This will receive all JS API replies for a request to store a session record,
// including the reply for our own server, which we will ignore.
// This allows us to detect that some application somewhere else in the cluster
// is connecting with the same client ID, and therefore we need to close the
// connection that is currently using this client ID.
//
// Can run from various go routines (system send loop, etc..).
// No lock held on entry.
func (as *mqttAccountSessionManager) processSessionPersist(_ *subscription, pc *client, _ *Account, subject, _ string, rmsg []byte) {
	// Ignore our own responses here (they are handled elsewhere)
	if tokenAt(subject, mqttJSAIdTokenPos) == as.jsa.id {
		return
	}
	cIDHash := tokenAt(subject, mqttJSAClientIDPos)
	_, msg := pc.msgParts(rmsg)
	if len(msg) < LEN_CR_LF {
		return
	}
	var par = &JSPubAckResponse{}
	if err := json.Unmarshal(msg, par); err != nil {
		return
	}
	if err := par.Error; err != nil {
		return
	}
	as.mu.RLock()
	// Note that as.domainTk includes a terminal '.', so strip to compare to PubAck.Domain.
	dl := len(as.domainTk)
	if dl > 0 {
		dl--
	}
	ignore := par.Domain != as.domainTk[:dl]
	as.mu.RUnlock()
	if ignore {
		return
	}

	as.mu.Lock()
	defer as.mu.Unlock()
	sess, ok := as.sessByHash[cIDHash]
	if !ok {
		return
	}
	// If our current session's stream sequence is higher, it means that this
	// update is stale, so we don't do anything here.
	sess.mu.Lock()
	ignore = par.Sequence < sess.seq
	sess.mu.Unlock()
	if ignore {
		return
	}
	as.removeSession(sess, false)
	sess.mu.Lock()
	if ec := sess.c; ec != nil {
		as.addSessToFlappers(sess.id)
		ec.Warnf("Closing because a remote connection has started with the same client ID: %q", sess.id)
		// Disassociate the client from the session so that on client close,
		// nothing will be done with regards to cleaning up the session,
		// such as deleting stream, etc..
		sess.c = nil
		// Remove in separate go routine.
		go ec.closeConnection(DuplicateClientID)
	}
	sess.mu.Unlock()
}

// Adds this client ID to the flappers map, and if needed start the timer
// for map cleanup.
//
// Lock held on entry.
func (as *mqttAccountSessionManager) addSessToFlappers(clientID string) {
	as.flappers[clientID] = time.Now().UnixNano()
	if as.flapTimer == nil {
		as.flapTimer = time.AfterFunc(mqttFlapCleanItvl, func() {
			as.mu.Lock()
			defer as.mu.Unlock()
			// In case of shutdown, this will be nil
			if as.flapTimer == nil {
				return
			}
			now := time.Now().UnixNano()
			for cID, tm := range as.flappers {
				if now-tm > int64(mqttSessJailDur) {
					delete(as.flappers, cID)
				}
			}
			as.flapTimer.Reset(mqttFlapCleanItvl)
		})
	}
}

// Remove this client ID from the flappers map.
//
// Lock held on entry.
func (as *mqttAccountSessionManager) removeSessFromFlappers(clientID string) {
	delete(as.flappers, clientID)
	// Do not stop/set timer to nil here. Better leave the timer run at its
	// regular interval and detect that there is nothing to do. The timer
	// will be stopped on shutdown.
}

// Helper to create a subscription. It updates the sid and array of subscriptions.
func (as *mqttAccountSessionManager) createSubscription(subject string, cb msgHandler, sid *int64, subs *[]*subscription) error {
	sub, err := as.jsa.c.processSub([]byte(subject), nil, []byte(strconv.FormatInt(*sid, 10)), cb, false)
	if err != nil {
		return err
	}
	*sid++
	*subs = append(*subs, sub)
	return nil
}

// A timer loop to cleanup up expired cached retained messages for a given MQTT account.
// The closeCh is used by the caller to be able to interrupt this routine
// if the rest of the initialization fails, since the quitCh is really
// only used when the server shutdown.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) cleanupRetainedMessageCache(s *Server, closeCh chan struct{}) {
	tt := time.NewTicker(mqttRetainedCacheTTL)
	defer tt.Stop()
	for {
		select {
		case <-tt.C:
			// Set a limit to the number of retained messages to scan since we
			// lock as for it. Since the map enumeration gives random order we
			// should eventually clean up everything.
			i, maxScan := 0, 10*1000
			now := time.Now()
			as.rmsCache.Range(func(key, value any) bool {
				rm := value.(*mqttRetainedMsg)
				if now.After(rm.expiresFromCache) {
					as.rmsCache.Delete(key)
				}
				i++
				return i < maxScan
			})

		case <-closeCh:
			return
		case <-s.quitCh:
			return
		}
	}
}

// Loop to send JS API requests for a given MQTT account.
// The closeCh is used by the caller to be able to interrupt this routine
// if the rest of the initialization fails, since the quitCh is really
// only used when the server shutdown.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) sendJSAPIrequests(s *Server, c *client, accName string, closeCh chan struct{}) {
	var cluster string
	if s.JetStreamEnabled() && !as.jsa.domainSet {
		// Only request the own cluster when it is clear that
		cluster = s.cachedClusterName()
	}
	as.mu.RLock()
	sendq := as.jsa.sendq
	quitCh := as.jsa.quitCh
	ci := ClientInfo{Account: accName, Cluster: cluster}
	as.mu.RUnlock()

	// The account session manager does not have a suhtdown API per-se, instead,
	// we will cleanup things when this go routine exits after detecting that the
	// server is shutdown or the initialization of the account manager failed.
	defer func() {
		as.mu.Lock()
		if as.flapTimer != nil {
			as.flapTimer.Stop()
			as.flapTimer = nil
		}
		as.mu.Unlock()
	}()

	b, _ := json.Marshal(ci)
	hdrStart := bytes.Buffer{}
	hdrStart.WriteString(hdrLine)
	http.Header{ClientInfoHdr: []string{string(b)}}.Write(&hdrStart)
	hdrStart.WriteString(CR_LF)
	hdrStart.WriteString(CR_LF)
	hdrb := hdrStart.Bytes()

	for {
		select {
		case <-sendq.ch:
			pmis := sendq.pop()
			for _, r := range pmis {
				var nsize int

				msg := r.msg
				// If r.hdr is set to -1, it means that there is no need for any header.
				if r.hdr != -1 {
					bb := bytes.Buffer{}
					if r.hdr > 0 {
						// This means that the header has been set by the caller and is
						// already part of `msg`, so simply set c.pa.hdr to the given value.
						c.pa.hdr = r.hdr
						nsize = len(msg)
						msg = append(msg, _CRLF_...)
					} else {
						// We need the ClientInfo header, so add it here.
						bb.Write(hdrb)
						c.pa.hdr = bb.Len()
						bb.Write(r.msg)
						nsize = bb.Len()
						bb.WriteString(_CRLF_)
						msg = bb.Bytes()
					}
					c.pa.hdb = []byte(strconv.Itoa(c.pa.hdr))
				} else {
					c.pa.hdr = -1
					c.pa.hdb = nil
					nsize = len(msg)
					msg = append(msg, _CRLF_...)
				}

				c.pa.subject = []byte(r.subj)
				c.pa.reply = []byte(r.reply)
				c.pa.size = nsize
				c.pa.szb = []byte(strconv.Itoa(nsize))

				c.processInboundClientMsg(msg)
				c.flushClients(0)
			}
			sendq.recycle(&pmis)

		case <-closeCh:
			return
		case <-quitCh:
			return
		}
	}
}

// Add/Replace this message from the retained messages map.
// If a message for this topic already existed, the existing record is updated
// with the provided information.
// Lock not held on entry.
func (as *mqttAccountSessionManager) handleRetainedMsg(key string, rf *mqttRetainedMsgRef, rm *mqttRetainedMsg, copyBytesToCache bool) {
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.retmsgs == nil {
		as.retmsgs = make(map[string]*mqttRetainedMsgRef)
		as.sl = NewSublistWithCache()
	} else {
		// Check if we already had one retained message. If so, update the existing one.
		if erm, exists := as.retmsgs[key]; exists {
			// If the new sequence is below the floor or the existing one,
			// then ignore the new one.
			if rf.sseq <= erm.sseq || rf.sseq <= erm.floor {
				return
			}
			// Capture existing sequence number so we can return it as the old sequence.
			erm.sseq = rf.sseq
			// Clear the floor
			erm.floor = 0
			// If sub is nil, it means that it was removed from sublist following a
			// network delete. So need to add it now.
			if erm.sub == nil {
				erm.sub = &subscription{subject: []byte(key)}
				as.sl.Insert(erm.sub)
			}

			// Update the in-memory retained message cache but only for messages
			// that are already in the cache, i.e. have been (recently) used.
			as.setCachedRetainedMsg(key, rm, true, copyBytesToCache)
			return
		}
	}

	rf.sub = &subscription{subject: []byte(key)}
	as.retmsgs[key] = rf
	as.sl.Insert(rf.sub)
}

// Removes the retained message for the given `subject` if present, and returns the
// stream sequence it was stored at. It will be 0 if no retained message was removed.
// If a sequence is passed and not 0, then the retained message will be removed only
// if the given sequence is equal or higher to what is stored.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) handleRetainedMsgDel(subject string, seq uint64) uint64 {
	var seqToRemove uint64
	as.mu.Lock()
	if as.retmsgs == nil {
		as.retmsgs = make(map[string]*mqttRetainedMsgRef)
		as.sl = NewSublistWithCache()
	}
	if erm, ok := as.retmsgs[subject]; ok {
		if as.rmsCache != nil {
			as.rmsCache.Delete(subject)
		}
		if erm.sub != nil {
			as.sl.Remove(erm.sub)
			erm.sub = nil
		}
		// If processing a delete request from the network, then seq will be > 0.
		// If that is the case and it is greater or equal to what we have, we need
		// to record the floor for this subject.
		if seq != 0 && seq >= erm.sseq {
			erm.sseq = 0
			erm.floor = seq
		} else if seq == 0 {
			delete(as.retmsgs, subject)
			seqToRemove = erm.sseq
		}
	} else if seq != 0 {
		rf := &mqttRetainedMsgRef{floor: seq}
		as.retmsgs[subject] = rf
	}
	as.mu.Unlock()
	return seqToRemove
}

// First check if this session's client ID is already in the "locked" map,
// which if it is the case means that another client is now bound to this
// session and this should return an error.
// If not in the "locked" map, but the client is not bound with this session,
// then same error is returned.
// Finally, if all checks ok, then the session's ID is added to the "locked" map.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) lockSession(sess *mqttSession, c *client) error {
	as.mu.Lock()
	defer as.mu.Unlock()
	var fail bool
	if _, fail = as.sessLocked[sess.id]; !fail {
		sess.mu.Lock()
		fail = sess.c != c
		sess.mu.Unlock()
	}
	if fail {
		return fmt.Errorf("another session is in use with client ID %q", sess.id)
	}
	as.sessLocked[sess.id] = struct{}{}
	return nil
}

// Remove the session from the "locked" map.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) unlockSession(sess *mqttSession) {
	as.mu.Lock()
	delete(as.sessLocked, sess.id)
	as.mu.Unlock()
}

// Simply adds the session to the various sessions maps.
// The boolean `lock` indicates if this function should acquire the lock
// prior to adding to the maps.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) addSession(sess *mqttSession, lock bool) {
	if lock {
		as.mu.Lock()
	}
	as.sessions[sess.id] = sess
	as.sessByHash[sess.idHash] = sess
	if lock {
		as.mu.Unlock()
	}
}

// Simply removes the session from the various sessions maps.
// The boolean `lock` indicates if this function should acquire the lock
// prior to removing from the maps.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) removeSession(sess *mqttSession, lock bool) {
	if lock {
		as.mu.Lock()
	}
	delete(as.sessions, sess.id)
	delete(as.sessByHash, sess.idHash)
	if lock {
		as.mu.Unlock()
	}
}

// Helper to set the sub's mqtt fields and possibly serialize (pre-loaded)
// retained messages.
//
// Session lock held on entry. Acquires the subs lock and holds it for
// the duration. Non-MQTT messages coming into mqttDeliverMsgCbQoS0 will be
// waiting.
func (sess *mqttSession) processQOS12Sub(
	c *client, // subscribing client.
	subject, sid []byte, isReserved bool, qos byte, jsDurName string, h msgHandler, // subscription parameters.
) (*subscription, error) {
	return sess.processSub(c, subject, sid, isReserved, qos, jsDurName, h, false, nil, false, nil)
}

func (sess *mqttSession) processSub(
	c *client, // subscribing client.
	subject, sid []byte, isReserved bool, qos byte, jsDurName string, h msgHandler, // subscription parameters.
	initShadow bool, // do we need to scan for shadow subscriptions? (not for QOS1+)
	rms map[string]*mqttRetainedMsg, // preloaded rms (can be empty, or missing items if errors)
	trace bool, // trace serialized retained messages in the log?
	as *mqttAccountSessionManager, // needed only for rms serialization.
) (*subscription, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		if elapsed > mqttProcessSubTooLong {
			c.Warnf("Took too long to process subscription for %q: %v", subject, elapsed)
		}
	}()

	// Hold subsMu to prevent QOS0 messages callback from doing anything until
	// the (MQTT) sub is initialized.
	sess.subsMu.Lock()
	defer sess.subsMu.Unlock()

	sub, err := c.processSub(subject, nil, sid, h, false)
	if err != nil {
		// c.processSub already called c.Errorf(), so no need here.
		return nil, err
	}
	subs := []*subscription{sub}
	if initShadow {
		subs = append(subs, sub.shadow...)
	}
	for _, ss := range subs {
		if ss.mqtt == nil {
			// reserved is set only once and once the subscription has been
			// created it can be considered immutable.
			ss.mqtt = &mqttSub{
				reserved: isReserved,
			}
		}
		// QOS and jsDurName can be changed on an existing subscription, so
		// accessing it later requires a lock.
		ss.mqtt.qos = qos
		ss.mqtt.jsDur = jsDurName
	}

	if len(rms) > 0 {
		for _, ss := range subs {
			as.serializeRetainedMsgsForSub(rms, sess, c, ss, trace)
		}
	}

	return sub, nil
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
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
func (as *mqttAccountSessionManager) processSubs(sess *mqttSession, c *client,
	filters []*mqttFilter, fromSubProto, trace bool) ([]*subscription, error) {

	c.mu.Lock()
	acc := c.acc
	c.mu.Unlock()

	// Helper to determine if we need to create a separate top-level
	// subscription for a wildcard.
	fwc := func(subject string) (bool, string, string) {
		if !mqttNeedSubForLevelUp(subject) {
			return false, _EMPTY_, _EMPTY_
		}
		// Say subject is "foo.>", remove the ".>" so that it becomes "foo"
		fwcsubject := subject[:len(subject)-2]
		// Change the sid to "foo fwc"
		fwcsid := fwcsubject + mqttMultiLevelSidSuffix

		return true, fwcsubject, fwcsid
	}

	rmSubjects := map[string]struct{}{}
	// Preload retained messages for all requested subscriptions.  Also, since
	// it's the first iteration over the filter list, do some cleanup.
	for _, f := range filters {
		if f.qos > 2 {
			f.qos = 2
		}
		if c.mqtt.downgradeQoS2Sub && f.qos == 2 {
			c.Warnf("Downgrading subscription QoS2 to QoS1 for %q, as configured", f.filter)
			f.qos = 1
		}

		// Do not allow subscribing to our internal subjects.
		//
		// TODO: (levb: not sure why since one can subscribe to `#` and it'll
		// include everything; I guess this would discourage? Otherwise another
		// candidate for DO NOT DELIVER prefix list).
		if strings.HasPrefix(f.filter, mqttSubPrefix) {
			f.qos = mqttSubAckFailure
			continue
		}

		if f.qos == 2 {
			if err := sess.ensurePubRelConsumerSubscription(c); err != nil {
				c.Errorf("failed to initialize PUBREL processing: %v", err)
				f.qos = mqttSubAckFailure
				continue
			}
		}

		// Find retained messages.
		if fromSubProto {
			addRMSubjects := func(subject string) error {
				sub := &subscription{
					client:  c,
					subject: []byte(subject),
					sid:     []byte(subject),
				}
				if err := c.addShadowSubscriptions(acc, sub, false); err != nil {
					return err
				}

				for _, sub := range append([]*subscription{sub}, sub.shadow...) {
					as.addRetainedSubjectsForSubject(rmSubjects, bytesToString(sub.subject))
					for _, ss := range sub.shadow {
						as.addRetainedSubjectsForSubject(rmSubjects, bytesToString(ss.subject))
					}
				}
				return nil
			}

			if err := addRMSubjects(f.filter); err != nil {
				f.qos = mqttSubAckFailure
				continue
			}
			if need, subject, _ := fwc(f.filter); need {
				if err := addRMSubjects(subject); err != nil {
					f.qos = mqttSubAckFailure
					continue
				}
			}
		}
	}

	serializeRMS := len(rmSubjects) > 0
	var rms map[string]*mqttRetainedMsg
	if serializeRMS {
		// Make the best effort to load retained messages. We will identify
		// errors in the next pass.
		rms = as.loadRetainedMessages(rmSubjects, c)
	}

	// Small helper to add the consumer config to the session.
	addJSConsToSess := func(sid string, cc *ConsumerConfig) {
		if cc == nil {
			return
		}
		if sess.cons == nil {
			sess.cons = make(map[string]*ConsumerConfig)
		}
		sess.cons[sid] = cc
	}

	var err error
	subs := make([]*subscription, 0, len(filters))
	for _, f := range filters {
		// Skip what's already been identified as a failure.
		if f.qos == mqttSubAckFailure {
			continue
		}
		subject := f.filter
		bsubject := []byte(subject)
		sid := subject
		bsid := bsubject
		isReserved := isMQTTReservedSubscription(subject)

		var jscons *ConsumerConfig
		var jssub *subscription

		// Note that if a subscription already exists on this subject, the
		// existing sub is returned. Need to update the qos.
		var sub *subscription
		var err error

		const processShadowSubs = true

		as.mu.Lock()
		sess.mu.Lock()
		sub, err = sess.processSub(c,
			bsubject, bsid, isReserved, f.qos, // main subject
			_EMPTY_, mqttDeliverMsgCbQoS0, // no jsDur for QOS0
			processShadowSubs,
			rms, trace, as)
		sess.mu.Unlock()
		as.mu.Unlock()

		if err != nil {
			f.qos = mqttSubAckFailure
			sess.cleanupFailedSub(c, sub, jscons, jssub)
			continue
		}

		// This will create (if not already exist) a JS consumer for
		// subscriptions of QoS >= 1. But if a JS consumer already exists and
		// the subscription for same subject is now a QoS==0, then the JS
		// consumer will be deleted.
		jscons, jssub, err = sess.processJSConsumer(c, subject, sid, f.qos, fromSubProto)
		if err != nil {
			f.qos = mqttSubAckFailure
			sess.cleanupFailedSub(c, sub, jscons, jssub)
			continue
		}

		// Process the wildcard subject if needed.
		if need, fwcsubject, fwcsid := fwc(subject); need {
			var fwjscons *ConsumerConfig
			var fwjssub *subscription
			var fwcsub *subscription

			// See note above about existing subscription.
			as.mu.Lock()
			sess.mu.Lock()
			fwcsub, err = sess.processSub(c,
				[]byte(fwcsubject), []byte(fwcsid), isReserved, f.qos, // FWC (top-level wildcard) subject
				_EMPTY_, mqttDeliverMsgCbQoS0, // no jsDur for QOS0
				processShadowSubs,
				rms, trace, as)
			sess.mu.Unlock()
			as.mu.Unlock()
			if err != nil {
				// c.processSub already called c.Errorf(), so no need here.
				f.qos = mqttSubAckFailure
				sess.cleanupFailedSub(c, sub, jscons, jssub)
				continue
			}

			fwjscons, fwjssub, err = sess.processJSConsumer(c, fwcsubject, fwcsid, f.qos, fromSubProto)
			if err != nil {
				// c.processSub already called c.Errorf(), so no need here.
				f.qos = mqttSubAckFailure
				sess.cleanupFailedSub(c, sub, jscons, jssub)
				sess.cleanupFailedSub(c, fwcsub, fwjscons, fwjssub)
				continue
			}

			subs = append(subs, fwcsub)
			addJSConsToSess(fwcsid, fwjscons)
		}

		subs = append(subs, sub)
		addJSConsToSess(sid, jscons)
	}

	if fromSubProto {
		err = sess.update(filters, true)
	}

	return subs, err
}

// Retained publish messages matching this subscription are serialized in the
// subscription's `prm` mqtt writer. This buffer will be queued for outbound
// after the subscription is processed and SUBACK is sent or possibly when
// server processes an incoming published message matching the newly
// registered subscription.
//
// Runs from the client's readLoop.
// Account session manager lock held on entry.
// Session lock held on entry.
func (as *mqttAccountSessionManager) serializeRetainedMsgsForSub(rms map[string]*mqttRetainedMsg, sess *mqttSession, c *client, sub *subscription, trace bool) error {
	if len(as.retmsgs) == 0 || len(rms) == 0 {
		return nil
	}
	result := as.sl.ReverseMatch(string(sub.subject))
	if len(result.psubs) == 0 {
		return nil
	}
	toTrace := []mqttPublish{}
	for _, psub := range result.psubs {

		rm := rms[string(psub.subject)]
		if rm == nil {
			// This should not happen since we pre-load messages into rms before
			// calling serialize.
			continue
		}
		var pi uint16
		qos := mqttGetQoS(rm.Flags)
		if qos > sub.mqtt.qos {
			qos = sub.mqtt.qos
		}
		if c.mqtt.rejectQoS2Pub && qos == 2 {
			c.Warnf("Rejecting retained message with QoS2 for subscription %q, as configured", sub.subject)
			continue
		}
		if qos > 0 {
			pi = sess.trackPublishRetained()

			// If we failed to get a PI for this message, send it as a QoS0, the
			// best we can do?
			if pi == 0 {
				qos = 0
			}
		}

		// Need to use the subject for the retained message, not the `sub` subject.
		// We can find the published retained message in rm.sub.subject.
		// Set the RETAIN flag: [MQTT-3.3.1-8].
		flags, headerBytes := mqttMakePublishHeader(pi, qos, false, true, []byte(rm.Topic), len(rm.Msg))
		c.mu.Lock()
		sub.mqtt.prm = append(sub.mqtt.prm, headerBytes, rm.Msg)
		c.mu.Unlock()
		if trace {
			toTrace = append(toTrace, mqttPublish{
				topic: []byte(rm.Topic),
				flags: flags,
				pi:    pi,
				sz:    len(rm.Msg),
			})
		}
	}
	for _, pp := range toTrace {
		c.traceOutOp("PUBLISH", []byte(mqttPubTrace(&pp)))
	}
	return nil
}

// Appends the stored message subjects for all retained message records that
// match the given subscription's `subject` (which could have wildcards).
//
// Account session manager NOT lock held on entry.
func (as *mqttAccountSessionManager) addRetainedSubjectsForSubject(list map[string]struct{}, topSubject string) bool {
	as.mu.RLock()
	if len(as.retmsgs) == 0 {
		as.mu.RUnlock()
		return false
	}
	result := as.sl.ReverseMatch(topSubject)
	as.mu.RUnlock()

	added := false
	for _, sub := range result.psubs {
		subject := string(sub.subject)
		if _, ok := list[subject]; ok {
			continue
		}
		list[subject] = struct{}{}
		added = true
	}

	return added
}

type warner interface {
	Warnf(format string, v ...any)
}

// Loads a list of retained messages given a list of stored message subjects.
func (as *mqttAccountSessionManager) loadRetainedMessages(subjects map[string]struct{}, w warner) map[string]*mqttRetainedMsg {
	rms := make(map[string]*mqttRetainedMsg, len(subjects))
	ss := []string{}
	for s := range subjects {
		if rm := as.getCachedRetainedMsg(s); rm != nil {
			rms[s] = rm
		} else {
			ss = append(ss, mqttRetainedMsgsStreamSubject+s)
		}
	}

	if len(ss) == 0 {
		return rms
	}

	results, err := as.jsa.loadLastMsgForMulti(mqttRetainedMsgsStreamName, ss)
	// If an error occurred, warn, but then proceed with what we got.
	if err != nil {
		w.Warnf("error loading retained messages: %v", err)
	}
	for i, result := range results {
		if result == nil {
			continue // skip requests that timed out
		}
		if result.ToError() != nil {
			w.Warnf("failed to load retained message for subject %q: %v", ss[i], err)
			continue
		}
		rm, err := mqttDecodeRetainedMessage(result.Message.Header, result.Message.Data)
		if err != nil {
			w.Warnf("failed to decode retained message for subject %q: %v", ss[i], err)
			continue
		}

		// Add the loaded retained message to the cache, and to the results map.
		key := ss[i][len(mqttRetainedMsgsStreamSubject):]
		as.setCachedRetainedMsg(key, rm, false, false)
		rms[key] = rm
	}
	return rms
}

// Composes a NATS message for a storeable mqttRetainedMsg.
func mqttEncodeRetainedMessage(rm *mqttRetainedMsg) (natsMsg []byte, headerLen int) {
	// No need to encode the subject, we can restore it from topic.
	l := len(hdrLine)
	l += len(mqttNatsRetainedMessageTopic) + 1 + len(rm.Topic) + 2 // 1 byte for ':', 2 bytes for CRLF
	if rm.Origin != _EMPTY_ {
		l += len(mqttNatsRetainedMessageOrigin) + 1 + len(rm.Origin) + 2 // 1 byte for ':', 2 bytes for CRLF
	}
	if rm.Source != _EMPTY_ {
		l += len(mqttNatsRetainedMessageSource) + 1 + len(rm.Source) + 2 // 1 byte for ':', 2 bytes for CRLF
	}
	l += len(mqttNatsRetainedMessageFlags) + 1 + 2 + 2 // 1 byte for ':', 2 bytes for the flags, 2 bytes for CRLF
	l += 2                                             // 2 bytes for the extra CRLF after the header
	l += len(rm.Msg)

	buf := bytes.NewBuffer(make([]byte, 0, l))

	buf.WriteString(hdrLine)

	buf.WriteString(mqttNatsRetainedMessageTopic)
	buf.WriteByte(':')
	buf.WriteString(rm.Topic)
	buf.WriteString(_CRLF_)

	buf.WriteString(mqttNatsRetainedMessageFlags)
	buf.WriteByte(':')
	buf.WriteString(strconv.FormatUint(uint64(rm.Flags), 16))
	buf.WriteString(_CRLF_)

	if rm.Origin != _EMPTY_ {
		buf.WriteString(mqttNatsRetainedMessageOrigin)
		buf.WriteByte(':')
		buf.WriteString(rm.Origin)
		buf.WriteString(_CRLF_)
	}
	if rm.Source != _EMPTY_ {
		buf.WriteString(mqttNatsRetainedMessageSource)
		buf.WriteByte(':')
		buf.WriteString(rm.Source)
		buf.WriteString(_CRLF_)
	}

	// End of header, finalize
	buf.WriteString(_CRLF_)
	headerLen = buf.Len()
	buf.Write(rm.Msg)
	return buf.Bytes(), headerLen
}

func mqttDecodeRetainedMessage(h, m []byte) (*mqttRetainedMsg, error) {
	fHeader := getHeader(mqttNatsRetainedMessageFlags, h)
	if len(fHeader) > 0 {
		flags, err := strconv.ParseUint(string(fHeader), 16, 8)
		if err != nil {
			return nil, fmt.Errorf("invalid retained message flags: %v", err)
		}
		topic := getHeader(mqttNatsRetainedMessageTopic, h)
		subj, _ := mqttToNATSSubjectConversion(topic, false)
		return &mqttRetainedMsg{
			Flags:   byte(flags),
			Subject: string(subj),
			Topic:   string(topic),
			Origin:  string(getHeader(mqttNatsRetainedMessageOrigin, h)),
			Source:  string(getHeader(mqttNatsRetainedMessageSource, h)),
			Msg:     m,
		}, nil
	} else {
		var rm mqttRetainedMsg
		if err := json.Unmarshal(m, &rm); err != nil {
			return nil, err
		}
		return &rm, nil
	}
}

// Creates the session stream (limit msgs of 1) for this client ID if it does
// not already exist. If it exists, recover the single record to rebuild the
// state of the session. If there is a session record but this session is not
// registered in the runtime of this server, then a request is made to the
// owner to close the client associated with this session since specification
// [MQTT-3.1.4-2] specifies that if the ClientId represents a Client already
// connected to the Server then the Server MUST disconnect the existing client.
//
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
func (as *mqttAccountSessionManager) createOrRestoreSession(clientID string, opts *Options) (*mqttSession, bool, error) {
	jsa := &as.jsa
	formatError := func(errTxt string, err error) (*mqttSession, bool, error) {
		accName := jsa.c.acc.GetName()
		return nil, false, fmt.Errorf("%s for account %q, session %q: %v", errTxt, accName, clientID, err)
	}

	hash := getHash(clientID)
	smsg, err := jsa.loadSessionMsg(as.domainTk, hash)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return formatError("loading session record", err)
		}
		// Message not found, so reate the session...
		// Create a session and indicate that this session did not exist.
		sess := mqttSessionCreate(jsa, clientID, hash, 0, opts)
		sess.domainTk = as.domainTk
		return sess, false, nil
	}
	// We need to recover the existing record now.
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		return formatError(fmt.Sprintf("unmarshal of session record at sequence %v", smsg.Sequence), err)
	}

	// Restore this session (even if we don't own it), the caller will do the right thing.
	sess := mqttSessionCreate(jsa, clientID, hash, smsg.Sequence, opts)
	sess.domainTk = as.domainTk
	sess.clean = ps.Clean
	sess.subs = ps.Subs
	sess.cons = ps.Cons
	sess.pubRelConsumer = ps.PubRel
	as.addSession(sess, true)
	return sess, true, nil
}

// Sends a request to delete a message, but does not wait for the response.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) deleteRetainedMsg(seq uint64) {
	as.jsa.deleteMsg(mqttRetainedMsgsStreamName, seq, false)
}

// Sends a message indicating that a retained message on a given subject and stream sequence
// is being removed.
func (as *mqttAccountSessionManager) notifyRetainedMsgDeleted(subject string, seq uint64) {
	req := mqttRetMsgDel{
		Subject: subject,
		Seq:     seq,
	}
	b, _ := json.Marshal(&req)
	jsa := &as.jsa
	jsa.sendq.push(&mqttJSPubMsg{
		subj: jsa.rplyr + mqttJSARetainedMsgDel,
		msg:  b,
	})
}

func (as *mqttAccountSessionManager) transferUniqueSessStreamsToMuxed(log *Server) {
	// Set retry to true, will be set to false on success.
	retry := true
	defer func() {
		if retry {
			next := mqttDefaultTransferRetry
			log.Warnf("Failed to transfer all MQTT session streams, will try again in %v", next)
			time.AfterFunc(next, func() { as.transferUniqueSessStreamsToMuxed(log) })
		}
	}()

	jsa := &as.jsa
	sni, err := jsa.newRequestEx(mqttJSAStreamNames, JSApiStreams, _EMPTY_, 0, nil)
	if err != nil {
		log.Errorf("Unable to transfer MQTT session streams: %v", err)
		return
	}
	snames := sni.(*JSApiStreamNamesResponse)
	if snames.Error != nil {
		log.Errorf("Unable to transfer MQTT session streams: %v", snames.ToError())
		return
	}
	var oldMQTTSessStreams []string
	for _, sn := range snames.Streams {
		if strings.HasPrefix(sn, mqttSessionsStreamNamePrefix) {
			oldMQTTSessStreams = append(oldMQTTSessStreams, sn)
		}
	}
	ns := len(oldMQTTSessStreams)
	if ns == 0 {
		// Nothing to do
		retry = false
		return
	}
	log.Noticef("Transferring %v MQTT session streams...", ns)
	for _, sn := range oldMQTTSessStreams {
		log.Noticef("  Transferring stream %q to %q", sn, mqttSessStreamName)
		smsg, err := jsa.loadLastMsgFor(sn, sn)
		if err != nil {
			log.Errorf("   Unable to load session record: %v", err)
			return
		}
		ps := &mqttPersistedSession{}
		if err := json.Unmarshal(smsg.Data, ps); err != nil {
			log.Warnf("    Unable to unmarshal the content of this stream, may not be a legitimate MQTT session stream, skipping")
			continue
		}
		// Store record to MQTT session stream
		if _, err := jsa.storeSessionMsg(as.domainTk, getHash(ps.ID), 0, smsg.Data); err != nil {
			log.Errorf("    Unable to transfer the session record: %v", err)
			return
		}
		jsa.deleteStream(sn)
	}
	log.Noticef("Transfer of %v MQTT session streams done!", ns)
	retry = false
}

func (as *mqttAccountSessionManager) transferRetainedToPerKeySubjectStream(log *Server) error {
	jsa := &as.jsa
	var processed int
	var transferred int

	start := time.Now()
	deadline := start.Add(mqttRetainedTransferTimeout)
	for {
		// Try and look up messages on the original undivided "$MQTT.rmsgs" subject.
		// If nothing is returned here, we assume to have migrated all old messages.
		smsg, err := jsa.loadNextMsgFor(mqttRetainedMsgsStreamName, "$MQTT.rmsgs")
		if IsNatsErr(err, JSNoMessageFoundErr) {
			// We've ran out of messages to transfer, done.
			break
		}
		if err != nil {
			log.Warnf("    Unable to transfer a retained message: failed to load from '$MQTT.rmsgs': %s", err)
			return err
		}

		// Unmarshal the message so that we can obtain the subject name. Do not
		// use mqttDecodeRetainedMessage() here because these messages are from
		// older versions, and contain the full JSON encoding in payload.
		var rmsg mqttRetainedMsg
		if err = json.Unmarshal(smsg.Data, &rmsg); err == nil {
			// Store the message again, this time with the new per-key subject.
			subject := mqttRetainedMsgsStreamSubject + rmsg.Subject
			if _, err = jsa.storeMsg(subject, 0, smsg.Data); err != nil {
				log.Errorf("    Unable to transfer the retained message with sequence %d: %v", smsg.Sequence, err)
			}
			transferred++
		} else {
			log.Warnf("    Unable to unmarshal retained message with sequence %d, skipping", smsg.Sequence)
		}

		// Delete the original message.
		if err := jsa.deleteMsg(mqttRetainedMsgsStreamName, smsg.Sequence, true); err != nil {
			log.Errorf("    Unable to clean up the retained message with sequence %d: %v", smsg.Sequence, err)
			return err
		}
		processed++

		now := time.Now()
		if now.After(deadline) {
			err := fmt.Errorf("timed out while transferring retained messages from '$MQTT.rmsgs' after %v, %d processed, %d successfully transferred", now.Sub(start), processed, transferred)
			log.Noticef(err.Error())
			return err
		}
	}
	if processed > 0 {
		log.Noticef("Processed %d messages from '$MQTT.rmsgs', successfully transferred %d in %v", processed, transferred, time.Since(start))
	} else {
		log.Debugf("No messages found to transfer from '$MQTT.rmsgs'")
	}
	return nil
}

func (as *mqttAccountSessionManager) getCachedRetainedMsg(subject string) *mqttRetainedMsg {
	if as.rmsCache == nil {
		return nil
	}
	v, ok := as.rmsCache.Load(subject)
	if !ok {
		return nil
	}
	rm := v.(*mqttRetainedMsg)
	if rm.expiresFromCache.Before(time.Now()) {
		as.rmsCache.Delete(subject)
		return nil
	}
	return rm
}

func (as *mqttAccountSessionManager) setCachedRetainedMsg(subject string, rm *mqttRetainedMsg, onlyReplace bool, copyBytesToCache bool) {
	if as.rmsCache == nil || rm == nil {
		return
	}
	rm.expiresFromCache = time.Now().Add(mqttRetainedCacheTTL)
	if onlyReplace {
		if _, ok := as.rmsCache.Load(subject); !ok {
			return
		}
	}
	if copyBytesToCache {
		rm.Msg = copyBytes(rm.Msg)
	}
	as.rmsCache.Store(subject, rm)
}

//////////////////////////////////////////////////////////////////////////////
//
// MQTT session related functions
//
//////////////////////////////////////////////////////////////////////////////

// Returns a new mqttSession object with max ack pending set based on
// option or use mqttDefaultMaxAckPending if no option set.
func mqttSessionCreate(jsa *mqttJSA, id, idHash string, seq uint64, opts *Options) *mqttSession {
	maxp := opts.MQTT.MaxAckPending
	if maxp == 0 {
		maxp = mqttDefaultMaxAckPending
	}

	return &mqttSession{
		jsa:                    jsa,
		id:                     id,
		idHash:                 idHash,
		seq:                    seq,
		maxp:                   maxp,
		pubRelSubject:          mqttPubRelSubjectPrefix + idHash,
		pubRelDeliverySubject:  mqttPubRelDeliverySubjectPrefix + idHash,
		pubRelDeliverySubjectB: []byte(mqttPubRelDeliverySubjectPrefix + idHash),
	}
}

// Persists a session. Note that if the session's current client does not match
// the given client, nothing is done.
//
// Lock not held on entry.
func (sess *mqttSession) save() error {
	sess.mu.Lock()
	ps := mqttPersistedSession{
		Origin: sess.jsa.id,
		ID:     sess.id,
		Clean:  sess.clean,
		Subs:   sess.subs,
		Cons:   sess.cons,
		PubRel: sess.pubRelConsumer,
	}
	b, _ := json.Marshal(&ps)

	domainTk, cidHash := sess.domainTk, sess.idHash
	seq := sess.seq
	sess.mu.Unlock()

	var hdr int
	if seq != 0 {
		bb := bytes.Buffer{}
		bb.WriteString(hdrLine)
		bb.WriteString(JSExpectedLastSubjSeq)
		bb.WriteString(":")
		bb.WriteString(strconv.FormatInt(int64(seq), 10))
		bb.WriteString(CR_LF)
		bb.WriteString(CR_LF)
		hdr = bb.Len()
		bb.Write(b)
		b = bb.Bytes()
	}

	resp, err := sess.jsa.storeSessionMsg(domainTk, cidHash, hdr, b)
	if err != nil {
		return fmt.Errorf("unable to persist session %q (seq=%v): %v", ps.ID, seq, err)
	}
	sess.mu.Lock()
	sess.seq = resp.Sequence
	sess.mu.Unlock()
	return nil
}

// Clear the session.
//
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
func (sess *mqttSession) clear(noWait bool) error {
	var durs []string
	var pubRelDur string

	sess.mu.Lock()
	id := sess.id
	seq := sess.seq
	if l := len(sess.cons); l > 0 {
		durs = make([]string, 0, l)
	}
	for sid, cc := range sess.cons {
		delete(sess.cons, sid)
		durs = append(durs, cc.Durable)
	}
	if sess.pubRelConsumer != nil {
		pubRelDur = sess.pubRelConsumer.Durable
	}

	sess.subs = nil
	sess.pendingPublish = nil
	sess.pendingPubRel = nil
	sess.cpending = nil
	sess.pubRelConsumer = nil
	sess.seq = 0
	sess.tmaxack = 0
	sess.mu.Unlock()

	for _, dur := range durs {
		if _, err := sess.jsa.deleteConsumer(mqttStreamName, dur, noWait); isErrorOtherThan(err, JSConsumerNotFoundErr) {
			return fmt.Errorf("unable to delete consumer %q for session %q: %v", dur, sess.id, err)
		}
	}
	if pubRelDur != _EMPTY_ {
		_, err := sess.jsa.deleteConsumer(mqttOutStreamName, pubRelDur, noWait)
		if isErrorOtherThan(err, JSConsumerNotFoundErr) {
			return fmt.Errorf("unable to delete consumer %q for session %q: %v", pubRelDur, sess.id, err)
		}
	}

	if seq > 0 {
		err := sess.jsa.deleteMsg(mqttSessStreamName, seq, !noWait)
		// Ignore the various errors indicating that the message (or sequence)
		// is already deleted, can happen in a cluster.
		if isErrorOtherThan(err, JSSequenceNotFoundErrF) {
			if isErrorOtherThan(err, JSStreamMsgDeleteFailedF) || !strings.Contains(err.Error(), ErrStoreMsgNotFound.Error()) {
				return fmt.Errorf("unable to delete session %q record at sequence %v: %v", id, seq, err)
			}
		}
	}
	return nil
}

// This will update the session record for this client in the account's MQTT
// sessions stream if the session had any change in the subscriptions.
//
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
func (sess *mqttSession) update(filters []*mqttFilter, add bool) error {
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
		err = sess.save()
	}
	return err
}

func (sess *mqttSession) bumpPI() uint16 {
	var avail bool
	next := sess.last_pi
	for i := 0; i < 0xFFFF; i++ {
		next++
		if next == 0 {
			next = 1
		}

		_, usedInPublish := sess.pendingPublish[next]
		_, usedInPubRel := sess.pendingPubRel[next]
		if !usedInPublish && !usedInPubRel {
			sess.last_pi = next
			avail = true
			break
		}
	}
	if !avail {
		return 0
	}
	return sess.last_pi
}

// trackPublishRetained is invoked when a retained (QoS) message is published.
// It need a new PI to be allocated, so we add it to the pendingPublish map,
// with an empty value. Since cpending (not pending) is used to serialize the PI
// mappings, we need to add this PI there as well. Make a unique key by using
// mqttRetainedMsgsStreamName for the durable name, and PI for sseq.
//
// Lock held on entry
func (sess *mqttSession) trackPublishRetained() uint16 {
	// Make sure we initialize the tracking maps.
	if sess.pendingPublish == nil {
		sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
	}

	pi := sess.bumpPI()
	if pi == 0 {
		return 0
	}
	sess.pendingPublish[pi] = &mqttPending{}

	return pi
}

// trackPublish is invoked when a (QoS) PUBLISH message is to be delivered. It
// detects an untracked (new) message based on its sequence extracted from its
// delivery-time jsAckSubject, and adds it to the tracking maps. Returns a PI to
// use for the message (new, or previously used), and whether this is a
// duplicate delivery attempt.
//
// Lock held on entry
func (sess *mqttSession) trackPublish(jsDur, jsAckSubject string) (uint16, bool) {
	var dup bool
	var pi uint16

	if jsAckSubject == _EMPTY_ || jsDur == _EMPTY_ {
		return 0, false
	}

	// Make sure we initialize the tracking maps.
	if sess.pendingPublish == nil {
		sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
	}

	// Get the stream sequence and duplicate flag from the ack reply subject.
	sseq, _, dcount := ackReplyInfo(jsAckSubject)
	if dcount > 1 {
		dup = true
	}

	var ack *mqttPending
	sseqToPi, ok := sess.cpending[jsDur]
	if !ok {
		sseqToPi = make(map[uint64]uint16)
		sess.cpending[jsDur] = sseqToPi
	} else {
		pi = sseqToPi[sseq]
	}

	if pi != 0 {
		// There is a possible race between a PUBLISH re-delivery calling us,
		// and a PUBREC received already having submitting a PUBREL into JS . If
		// so, indicate no need for (re-)delivery by returning a PI of 0.
		_, usedForPubRel := sess.pendingPubRel[pi]
		if /*dup && */ usedForPubRel {
			return 0, false
		}

		// We should have a pending JS ACK for this PI.
		ack = sess.pendingPublish[pi]
	} else {
		// sess.maxp will always have a value > 0.
		if len(sess.pendingPublish) >= int(sess.maxp) {
			// Indicate that we did not assign a packet identifier.
			// The caller will not send the message to the subscription
			// and JS will redeliver later, based on consumer's AckWait.
			return 0, false
		}

		pi = sess.bumpPI()
		if pi == 0 {
			return 0, false
		}

		sseqToPi[sseq] = pi
	}

	if ack == nil {
		sess.pendingPublish[pi] = &mqttPending{
			jsDur:        jsDur,
			sseq:         sseq,
			jsAckSubject: jsAckSubject,
		}
	} else {
		ack.jsAckSubject = jsAckSubject
		ack.sseq = sseq
		ack.jsDur = jsDur
	}

	return pi, dup
}

// Stops a PI from being tracked as a PUBLISH. It can still be in use for a
// pending PUBREL.
//
// Lock held on entry
func (sess *mqttSession) untrackPublish(pi uint16) (jsAckSubject string) {
	ack, ok := sess.pendingPublish[pi]
	if !ok {
		return _EMPTY_
	}

	delete(sess.pendingPublish, pi)
	if len(sess.pendingPublish) == 0 {
		sess.last_pi = 0
	}

	if len(sess.cpending) != 0 && ack.jsDur != _EMPTY_ {
		if sseqToPi := sess.cpending[ack.jsDur]; sseqToPi != nil {
			delete(sseqToPi, ack.sseq)
		}
	}

	return ack.jsAckSubject
}

// trackAsPubRel is invoked in 2 cases: (a) when we receive a PUBREC and we need
// to change from tracking the PI as a PUBLISH to a PUBREL; and (b) when we
// attempt to deliver the PUBREL to record the JS ack subject for it.
//
// Lock held on entry
func (sess *mqttSession) trackAsPubRel(pi uint16, jsAckSubject string) {
	if sess.pubRelConsumer == nil {
		// The cosumer MUST be set up already.
		return
	}
	jsDur := sess.pubRelConsumer.Durable

	if sess.pendingPubRel == nil {
		sess.pendingPubRel = make(map[uint16]*mqttPending)
	}

	if jsAckSubject == _EMPTY_ {
		sess.pendingPubRel[pi] = &mqttPending{
			jsDur: jsDur,
		}
		return
	}

	sseq, _, _ := ackReplyInfo(jsAckSubject)

	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
	}
	sseqToPi := sess.cpending[jsDur]
	if sseqToPi == nil {
		sseqToPi = make(map[uint64]uint16)
		sess.cpending[jsDur] = sseqToPi
	}
	sseqToPi[sseq] = pi
	sess.pendingPubRel[pi] = &mqttPending{
		jsDur:        sess.pubRelConsumer.Durable,
		sseq:         sseq,
		jsAckSubject: jsAckSubject,
	}
}

// Stops a PI from being tracked as a PUBREL.
//
// Lock held on entry
func (sess *mqttSession) untrackPubRel(pi uint16) (jsAckSubject string) {
	ack, ok := sess.pendingPubRel[pi]
	if !ok {
		return _EMPTY_
	}

	delete(sess.pendingPubRel, pi)

	if sess.pubRelConsumer != nil && len(sess.cpending) > 0 {
		if sseqToPi := sess.cpending[ack.jsDur]; sseqToPi != nil {
			delete(sseqToPi, ack.sseq)
		}
	}

	return ack.jsAckSubject
}

// Sends a consumer delete request, but does not wait for response.
//
// Lock not held on entry.
func (sess *mqttSession) deleteConsumer(cc *ConsumerConfig) {
	sess.mu.Lock()
	sess.tmaxack -= cc.MaxAckPending
	sess.jsa.deleteConsumer(mqttStreamName, cc.Durable, true)
	sess.mu.Unlock()
}

//////////////////////////////////////////////////////////////////////////////
//
// CONNECT protocol related functions
//
//////////////////////////////////////////////////////////////////////////////

// Parse the MQTT connect protocol
func (c *client) mqttParseConnect(r *mqttReader, hasMappings bool) (byte, *mqttConnectProto, error) {
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
		return 0, nil, errMQTTConnFlagReserved
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
			return 0, nil, errMQTTWillAndRetainFlag
		}
	} else {
		// Spec [MQTT-3.1.2-14]
		if wqos == 3 {
			return 0, nil, fmt.Errorf("if Will flag is set to 1, Will QoS can be 0, 1 or 2, got %v", wqos)
		}
		hasWill = true
	}

	if c.mqtt.rejectQoS2Pub && hasWill && wqos == 2 {
		return mqttConnAckRCQoS2WillRejected, nil, fmt.Errorf("server does not accept QoS2 for Will messages")
	}

	// Spec [MQTT-3.1.2-19]
	hasUser := cp.flags&mqttConnFlagUsernameFlag != 0
	// Spec [MQTT-3.1.2-21]
	hasPassword := cp.flags&mqttConnFlagPasswordFlag != 0
	// Spec [MQTT-3.1.2-22]
	if !hasUser && hasPassword {
		return 0, nil, errMQTTPasswordFlagAndNoUser
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
	c.mqtt.cid, err = r.readString("client ID")
	if err != nil {
		return 0, nil, err
	}
	// Spec [MQTT-3.1.3-7]
	if c.mqtt.cid == _EMPTY_ {
		if cp.flags&mqttConnFlagCleanSession == 0 {
			return mqttConnAckRCIdentifierRejected, nil, errMQTTCIDEmptyNeedsCleanFlag
		}
		// Spec [MQTT-3.1.3-6]
		c.mqtt.cid = nuid.Next()
	}
	// Spec [MQTT-3.1.3-4] and [MQTT-3.1.3-9]
	if !utf8.ValidString(c.mqtt.cid) {
		return mqttConnAckRCIdentifierRejected, nil, fmt.Errorf("invalid utf8 for client ID: %q", c.mqtt.cid)
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
			return 0, nil, errMQTTEmptyWillTopic
		}
		if !utf8.Valid(topic) {
			return 0, nil, fmt.Errorf("invalid utf8 for Will topic %q", topic)
		}
		// Convert MQTT topic to NATS subject
		cp.will.subject, err = mqttTopicToNATSPubSubject(topic)
		if err != nil {
			return 0, nil, err
		}
		// Check for subject mapping.
		if hasMappings {
			// For selectMappedSubject to work, we need to have c.pa.subject set.
			// If there is a change, c.pa.mapped will be set after the call.
			c.pa.subject = cp.will.subject
			if changed := c.selectMappedSubject(); changed {
				// We need to keep track of the NATS subject/mapped in the `cp` structure.
				cp.will.subject = c.pa.subject
				cp.will.mapped = c.pa.mapped
				// We also now need to map the original MQTT topic to the new topic
				// based on the new subject.
				topic = natsSubjectToMQTTTopic(cp.will.subject)
			}
			// Reset those now.
			c.pa.subject, c.pa.mapped = nil, nil
		}
		cp.will.topic = topic
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
			return mqttConnAckRCBadUserOrPassword, nil, errMQTTEmptyUsername
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
	trace := fmt.Sprintf("clientID=%s", c.mqtt.cid)
	if cp.rd > 0 {
		trace += fmt.Sprintf(" keepAlive=%v", cp.rd)
	}
	if cp.will != nil {
		trace += fmt.Sprintf(" will=(topic=%s QoS=%v retain=%v)",
			cp.will.topic, cp.will.qos, cp.will.retain)
	}
	if cp.flags&mqttConnFlagCleanSession != 0 {
		trace += " clean"
	}
	if c.opts.Username != _EMPTY_ {
		trace += fmt.Sprintf(" username=%s", c.opts.Username)
	}
	if c.opts.Password != _EMPTY_ {
		trace += " password=****"
	}
	return trace
}

// Process the CONNECT packet.
//
// For the first session on the account, an account session manager will be created,
// along with the JetStream streams/consumer necessary for the working of MQTT.
//
// The session, identified by a client ID, will be registered, or if already existing,
// will be resumed. If the session exists but is associated with an existing client,
// the old client is evicted, as per the specifications.
//
// Due to specific locking requirements around JS API requests, we cannot hold some
// locks for the entire duration of processing of some protocols, therefore, we use
// a map that registers the client ID in a "locked" state. If a different client tries
// to connect and the server detects that the client ID is in that map, it will try
// a little bit until it is not, or fail the new client, since we can't protect
// processing of protocols in the original client. This is not expected to happen often.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttProcessConnect(c *client, cp *mqttConnectProto, trace bool) error {
	sendConnAck := func(rc byte, sessp bool) {
		c.mqttEnqueueConnAck(rc, sessp)
		if trace {
			c.traceOutOp("CONNACK", []byte(fmt.Sprintf("sp=%v rc=%v", sessp, rc)))
		}
	}

	c.mu.Lock()
	cid := c.mqtt.cid
	c.clearAuthTimer()
	c.mu.Unlock()
	if !s.isClientAuthorized(c) {
		if trace {
			c.traceOutOp("CONNACK", []byte(fmt.Sprintf("sp=%v rc=%v", false, mqttConnAckRCNotAuthorized)))
		}
		c.authViolation()
		return ErrAuthentication
	}
	// Now that we are authenticated, we have the client bound to the account.
	// Get the account's level MQTT sessions manager. If it does not exists yet,
	// this will create it along with the streams where sessions and messages
	// are stored.
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		return err
	}

	// Most of the session state is altered only in the readLoop so does not
	// need locking. For things that can be access in the readLoop and in
	// callbacks, we will use explicit locking.
	// To prevent other clients to connect with the same client ID, we will
	// add the client ID to a "locked" map so that the connect somewhere else
	// is put on hold.
	// This keep track of how many times this client is detecting that its
	// client ID is in the locked map. After a short amount, the server will
	// fail this inbound client.
	locked := 0

CHECK:
	asm.mu.Lock()
	// Check if different applications keep trying to connect with the same
	// client ID at the same time.
	if tm, ok := asm.flappers[cid]; ok {
		// If the last time it tried to connect was more than 1 sec ago,
		// then accept and remove from flappers map.
		if time.Now().UnixNano()-tm > int64(mqttSessJailDur) {
			asm.removeSessFromFlappers(cid)
		} else {
			// Will hold this client for a second and then close it. We
			// do this so that if the client has a reconnect feature we
			// don't end-up with very rapid flapping between apps.
			// We need to wait in place and not schedule the connection
			// close because if this is a misbehaved client that does
			// not wait for the CONNACK and sends other protocols, the
			// server would not have a fully setup client and may panic.
			asm.mu.Unlock()
			select {
			case <-s.quitCh:
			case <-time.After(mqttSessJailDur):
			}
			c.closeConnection(DuplicateClientID)
			return ErrConnectionClosed
		}
	}
	// If an existing session is in the process of processing some packet, we can't
	// evict the old client just yet. So try again to see if the state clears, but
	// if it does not, then we have no choice but to fail the new client instead of
	// the old one.
	if _, ok := asm.sessLocked[cid]; ok {
		asm.mu.Unlock()
		if locked++; locked == 10 {
			return fmt.Errorf("other session with client ID %q is in the process of connecting", cid)
		}
		time.Sleep(100 * time.Millisecond)
		goto CHECK
	}

	// Register this client ID the "locked" map for the duration if this function.
	asm.sessLocked[cid] = struct{}{}
	// And remove it on exit, regardless of error or not.
	defer func() {
		asm.mu.Lock()
		delete(asm.sessLocked, cid)
		asm.mu.Unlock()
	}()

	// Is the client requesting a clean session or not.
	cleanSess := cp.flags&mqttConnFlagCleanSession != 0
	// Session present? Assume false, will be set to true only when applicable.
	sessp := false
	// Do we have an existing session for this client ID
	es, exists := asm.sessions[cid]
	asm.mu.Unlock()

	// The session is not in the map, but may be on disk, so try to recover
	// or create the stream if not.
	if !exists {
		es, exists, err = asm.createOrRestoreSession(cid, s.getOpts())
		if err != nil {
			return err
		}
	}
	if exists {
		// Clear the session if client wants a clean session.
		// Also, Spec [MQTT-3.2.2-1]: don't report session present
		if cleanSess || es.clean {
			// Spec [MQTT-3.1.2-6]: If CleanSession is set to 1, the Client and
			// Server MUST discard any previous Session and start a new one.
			// This Session lasts as long as the Network Connection. State data
			// associated with this Session MUST NOT be reused in any subsequent
			// Session.
			if err := es.clear(false); err != nil {
				asm.removeSession(es, true)
				return err
			}
		} else {
			// Report to the client that the session was present
			sessp = true
		}
		// Spec [MQTT-3.1.4-2]. If the ClientId represents a Client already
		// connected to the Server then the Server MUST disconnect the existing
		// client.
		// Bind with the new client. This needs to be protected because can be
		// accessed outside of the readLoop.
		es.mu.Lock()
		ec := es.c
		es.c = c
		es.clean = cleanSess
		es.mu.Unlock()
		if ec != nil {
			// Remove "will" of existing client before closing
			ec.mu.Lock()
			ec.mqtt.cp.will = nil
			ec.mu.Unlock()
			// Add to the map of the flappers
			asm.mu.Lock()
			asm.addSessToFlappers(cid)
			asm.mu.Unlock()
			c.Warnf("Replacing old client %q since both have the same client ID %q", ec, cid)
			// Close old client in separate go routine
			go ec.closeConnection(DuplicateClientID)
		}
	} else {
		// Spec [MQTT-3.2.2-3]: if the Server does not have stored Session state,
		// it MUST set Session Present to 0 in the CONNACK packet.
		es.mu.Lock()
		es.c, es.clean = c, cleanSess
		es.mu.Unlock()
		// Now add this new session into the account sessions
		asm.addSession(es, true)
	}
	// We would need to save only if it did not exist previously, but we save
	// always in case we are running in cluster mode. This will notify other
	// running servers that this session is being used.
	if err := es.save(); err != nil {
		asm.removeSession(es, true)
		return err
	}
	c.mu.Lock()
	c.flags.set(connectReceived)
	c.mqtt.cp = cp
	c.mqtt.asm = asm
	c.mqtt.sess = es
	c.mu.Unlock()

	// Spec [MQTT-3.2.0-1]: CONNACK must be the first protocol sent to the session.
	sendConnAck(mqttConnAckRCConnectionAccepted, sessp)

	// Process possible saved subscriptions.
	if l := len(es.subs); l > 0 {
		filters := make([]*mqttFilter, 0, l)
		for subject, qos := range es.subs {
			filters = append(filters, &mqttFilter{filter: subject, qos: qos})
		}
		if _, err := asm.processSubs(es, c, filters, false, trace); err != nil {
			return err
		}
	}
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
	pp.mapped = will.mapped
	pp.msg = will.message
	pp.sz = len(will.message)
	pp.pi = 0
	pp.flags = will.qos << 1
	if will.retain {
		pp.flags |= mqttPubFlagRetain
	}
	c.mu.Unlock()
	s.mqttInitiateMsgDelivery(c, pp)
	c.flushClients(0)
}

//////////////////////////////////////////////////////////////////////////////
//
// PUBLISH protocol related functions
//
//////////////////////////////////////////////////////////////////////////////

func (c *client) mqttParsePub(r *mqttReader, pl int, pp *mqttPublish, hasMappings bool) error {
	qos := mqttGetQoS(pp.flags)
	if qos > 2 {
		return fmt.Errorf("QoS=%v is invalid in MQTT", qos)
	}

	if c.mqtt.rejectQoS2Pub && qos == 2 {
		return fmt.Errorf("QoS=2 is disabled for PUBLISH messages")
	}

	// Keep track of where we are when starting to read the variable header
	start := r.pos

	var err error
	pp.topic, err = r.readBytes("topic", false)
	if err != nil {
		return err
	}
	if len(pp.topic) == 0 {
		return errMQTTTopicIsEmpty
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

	// Check for subject mapping.
	if hasMappings {
		// For selectMappedSubject to work, we need to have c.pa.subject set.
		// If there is a change, c.pa.mapped will be set after the call.
		c.pa.subject = pp.subject
		if changed := c.selectMappedSubject(); changed {
			// We need to keep track of the NATS subject/mapped in the `pp` structure.
			pp.subject = c.pa.subject
			pp.mapped = c.pa.mapped
			// We also now need to map the original MQTT topic to the new topic
			// based on the new subject.
			pp.topic = natsSubjectToMQTTTopic(pp.subject)
		}
		// Reset those now.
		c.pa.subject, c.pa.mapped = nil, nil
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

// Composes a NATS message from a MQTT PUBLISH packet. The message includes an
// internal header containint the original packet's QoS, and for QoS2 packets
// the original subject.
//
// Example (QoS2, subject: "foo.bar"):
//
//	NATS/1.0\r\n
//	Nmqtt-Pub:2foo.bar\r\n
//	\r\n
func mqttNewDeliverableMessage(pp *mqttPublish, encodePP bool) (natsMsg []byte, headerLen int) {
	size := len(hdrLine) +
		len(mqttNatsHeader) + 2 + 2 + // 2 for ':<qos>', and 2 for CRLF
		2 + // end-of-header CRLF
		pp.sz
	if encodePP {
		size += len(mqttNatsHeaderSubject) + 1 + // +1 for ':'
			len(pp.subject) + 2 // 2 for CRLF

		if len(pp.mapped) > 0 {
			size += len(mqttNatsHeaderMapped) + 1 + // +1 for ':'
				len(pp.mapped) + 2 // 2 for CRLF
		}
	}
	buf := bytes.NewBuffer(make([]byte, 0, size))

	qos := mqttGetQoS(pp.flags)

	buf.WriteString(hdrLine)
	buf.WriteString(mqttNatsHeader)
	buf.WriteByte(':')
	buf.WriteByte(qos + '0')
	buf.WriteString(_CRLF_)

	if encodePP {
		buf.WriteString(mqttNatsHeaderSubject)
		buf.WriteByte(':')
		buf.Write(pp.subject)
		buf.WriteString(_CRLF_)

		if len(pp.mapped) > 0 {
			buf.WriteString(mqttNatsHeaderMapped)
			buf.WriteByte(':')
			buf.Write(pp.mapped)
			buf.WriteString(_CRLF_)
		}
	}

	// End of header
	buf.WriteString(_CRLF_)

	headerLen = buf.Len()

	buf.Write(pp.msg)
	return buf.Bytes(), headerLen
}

// Composes a NATS message for a pending PUBREL packet. The message includes an
// internal header containing the PI for PUBREL/PUBCOMP.
//
// Example (PI:123):
//
//		NATS/1.0\r\n
//	 	Nmqtt-PubRel:123\r\n
//		\r\n
func mqttNewDeliverablePubRel(pi uint16) (natsMsg []byte, headerLen int) {
	size := len(hdrLine) +
		len(mqttNatsPubRelHeader) + 6 + 2 + // 6 for ':65535', and 2 for CRLF
		2 // end-of-header CRLF
	buf := bytes.NewBuffer(make([]byte, 0, size))
	buf.WriteString(hdrLine)
	buf.WriteString(mqttNatsPubRelHeader)
	buf.WriteByte(':')
	buf.WriteString(strconv.FormatInt(int64(pi), 10))
	buf.WriteString(_CRLF_)
	buf.WriteString(_CRLF_)
	return buf.Bytes(), buf.Len()
}

// Process the PUBLISH packet.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttProcessPub(c *client, pp *mqttPublish, trace bool) error {
	qos := mqttGetQoS(pp.flags)

	switch qos {
	case 0:
		return s.mqttInitiateMsgDelivery(c, pp)

	case 1:
		// [MQTT-4.3.2-2]. Initiate onward delivery of the Application Message,
		// Send PUBACK.
		//
		// The receiver is not required to complete delivery of the Application
		// Message before sending the PUBACK. When its original sender receives
		// the PUBACK packet, ownership of the Application Message is
		// transferred to the receiver.
		err := s.mqttInitiateMsgDelivery(c, pp)
		if err == nil {
			c.mqttEnqueuePubResponse(mqttPacketPubAck, pp.pi, trace)
		}
		return err

	case 2:
		// [MQTT-4.3.3-2]. Method A, Store message, send PUBREC.
		//
		// The receiver is not required to complete delivery of the Application
		// Message before sending the PUBREC or PUBCOMP. When its original
		// sender receives the PUBREC packet, ownership of the Application
		// Message is transferred to the receiver.
		err := s.mqttStoreQoS2MsgOnce(c, pp)
		if err == nil {
			c.mqttEnqueuePubResponse(mqttPacketPubRec, pp.pi, trace)
		}
		return err

	default:
		return fmt.Errorf("unreachable: invalid QoS in mqttProcessPub: %v", qos)
	}
}

func (s *Server) mqttInitiateMsgDelivery(c *client, pp *mqttPublish) error {
	natsMsg, headerLen := mqttNewDeliverableMessage(pp, false)

	// Set the client's pubarg for processing.
	c.pa.subject = pp.subject
	c.pa.mapped = pp.mapped
	c.pa.reply = nil
	c.pa.hdr = headerLen
	c.pa.hdb = []byte(strconv.FormatInt(int64(c.pa.hdr), 10))
	c.pa.size = len(natsMsg)
	c.pa.szb = []byte(strconv.FormatInt(int64(c.pa.size), 10))
	defer func() {
		c.pa.subject = nil
		c.pa.mapped = nil
		c.pa.reply = nil
		c.pa.hdr = -1
		c.pa.hdb = nil
		c.pa.size = 0
		c.pa.szb = nil
	}()

	_, permIssue := c.processInboundClientMsg(natsMsg)
	if permIssue {
		return nil
	}

	// If QoS 0 messages don't need to be stored, other (1 and 2) do. Store them
	// JetStream under "$MQTT.msgs.<delivery-subject>"
	if qos := mqttGetQoS(pp.flags); qos == 0 {
		return nil
	}

	// We need to call flushClients now since this we may have called c.addToPCD
	// with destination clients (possibly a route). Without calling flushClients
	// the following call may then be stuck waiting for a reply that may never
	// come because the destination is not flushed (due to c.out.fsp > 0,
	// see addToPCD and writeLoop for details).
	c.flushClients(0)

	_, err := c.mqtt.sess.jsa.storeMsg(mqttStreamSubjectPrefix+string(c.pa.subject), headerLen, natsMsg)

	return err
}

var mqttMaxMsgErrPattern = fmt.Sprintf("%s (%v)", ErrMaxMsgsPerSubject.Error(), JSStreamStoreFailedF)

func (s *Server) mqttStoreQoS2MsgOnce(c *client, pp *mqttPublish) error {
	// `true` means encode the MQTT PUBLISH packet in the NATS message header.
	natsMsg, headerLen := mqttNewDeliverableMessage(pp, true)

	// Do not broadcast the message until it has been deduplicated and released
	// by the sender. Instead store this QoS2 message as
	// "$MQTT.qos2.<client-id>.<PI>". If the message is a duplicate, we get back
	// a ErrMaxMsgsPerSubject, otherwise it does not change the flow, still need
	// to send a PUBREC back to the client. The original subject (translated
	// from MQTT topic) is included in the NATS header of the stored message to
	// use for latter delivery.
	_, err := c.mqtt.sess.jsa.storeMsg(c.mqttQoS2InternalSubject(pp.pi), headerLen, natsMsg)

	// TODO: would prefer a more robust and performant way of checking the
	// error, but it comes back wrapped as an API result.
	if err != nil &&
		(isErrorOtherThan(err, JSStreamStoreFailedF) || err.Error() != mqttMaxMsgErrPattern) {
		return err
	}

	return nil
}

func (c *client) mqttQoS2InternalSubject(pi uint16) string {
	return mqttQoS2IncomingMsgsStreamSubjectPrefix + c.mqtt.cid + "." + strconv.FormatUint(uint64(pi), 10)
}

// Process a PUBREL packet (QoS2, acting as Receiver).
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttProcessPubRel(c *client, pi uint16, trace bool) error {
	// Once done with the processing, send a PUBCOMP back to the client.
	defer c.mqttEnqueuePubResponse(mqttPacketPubComp, pi, trace)

	// See if there is a message pending for this pi. All failures are treated
	// as "not found".
	asm := c.mqtt.asm
	stored, _ := asm.jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName, c.mqttQoS2InternalSubject(pi))

	if stored == nil {
		// No message found, nothing to do.
		return nil
	}
	// Best attempt to delete the message from the QoS2 stream.
	asm.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, stored.Sequence, true)

	// only MQTT QoS2 messages should be here, and they must have a subject.
	h := mqttParsePublishNATSHeader(stored.Header)
	if h == nil || h.qos != 2 || len(h.subject) == 0 {
		return errors.New("invalid message in QoS2 PUBREL stream")
	}

	pp := &mqttPublish{
		topic:   natsSubjectToMQTTTopic(h.subject),
		subject: h.subject,
		mapped:  h.mapped,
		msg:     stored.Data,
		sz:      len(stored.Data),
		pi:      pi,
		flags:   h.qos << 1,
	}

	return s.mqttInitiateMsgDelivery(c, pp)
}

// Invoked when processing an inbound client message. If the "retain" flag is
// set, the message is stored so it can be later resent to (re)starting
// subscriptions that match the subject.
//
// Invoked from the MQTT publisher's readLoop. No client lock is held on entry.
func (c *client) mqttHandlePubRetain() {
	pp := c.mqtt.pp
	retainMQTT := mqttIsRetained(pp.flags)
	isBirth, _, isCertificate := sparkbParseBirthDeathTopic(pp.topic)
	retainSparkbBirth := isBirth && !isCertificate

	// [tck-id-topics-nbirth-mqtt] NBIRTH messages MUST be published with MQTT
	// QoS equal to 0 and retain equal to false.
	//
	// [tck-id-conformance-mqtt-aware-nbirth-mqtt-retain] A Sparkplug Aware MQTT
	// Server MUST make NBIRTH messages available on the topic:
	// $sparkplug/certificates/namespace/group_id/NBIRTH/edge_node_id with the
	// MQTT retain flag set to true.
	if retainMQTT == retainSparkbBirth {
		// (retainSparkbBirth && retainMQTT) : not valid, so ignore altogether.
		// (!retainSparkbBirth && !retainMQTT) : nothing to do.
		return
	}

	asm := c.mqtt.asm
	key := string(pp.subject)

	// Always clear the retain flag to deliver a normal published message.
	defer func() {
		pp.flags &= ^mqttPubFlagRetain
	}()

	// Spec [MQTT-3.3.1-11]. Payload of size 0 removes the retained message, but
	// should still be delivered as a normal message.
	if pp.sz == 0 {
		if seqToRemove := asm.handleRetainedMsgDel(key, 0); seqToRemove > 0 {
			asm.deleteRetainedMsg(seqToRemove)
			asm.notifyRetainedMsgDeleted(key, seqToRemove)
		}
		return
	}

	rm := &mqttRetainedMsg{
		Origin: asm.jsa.id,
		Msg:    pp.msg, // will copy these bytes later as we process rm.
		Flags:  pp.flags,
		Source: c.opts.Username,
	}

	if retainSparkbBirth {
		// [tck-id-conformance-mqtt-aware-store] A Sparkplug Aware MQTT Server
		// MUST store NBIRTH and DBIRTH messages as they pass through the MQTT
		// Server.
		//
		// [tck-id-conformance-mqtt-aware-nbirth-mqtt-topic]. A Sparkplug Aware
		// MQTT Server MUST make NBIRTH messages available on a topic of the
		// form: $sparkplug/certificates/namespace/group_id/NBIRTH/edge_node_id
		//
		// [tck-id-conformance-mqtt-aware-dbirth-mqtt-topic] A Sparkplug Aware
		// MQTT Server MUST make DBIRTH messages available on a topic of the
		// form:
		// $sparkplug/certificates/namespace/group_id/DBIRTH/edge_node_id/device_id
		topic := append(sparkbCertificatesTopicPrefix, pp.topic...)
		subject, _ := mqttTopicToNATSPubSubject(topic)
		rm.Topic = string(topic)
		rm.Subject = string(subject)

		// will use to save the retained message.
		key = string(subject)

		// Store the retained message with the RETAIN flag set.
		rm.Flags |= mqttPubFlagRetain

		// Copy the payload out of pp since we will be sending the message
		// asynchronously.
		msg := make([]byte, pp.sz)
		copy(msg, pp.msg[:pp.sz])
		asm.jsa.sendMsg(key, msg)

	} else { // isRetained
		// Spec [MQTT-3.3.1-5]. Store the retained message with its QoS.
		//
		// When coming from a publish protocol, `pp` is referencing a stack
		// variable that itself possibly references the read buffer.
		rm.Topic = string(pp.topic)
	}

	// Set the key to the subject of the message for retained, or the composed
	// $sparkplug subject for sparkB.
	rm.Subject = key
	rmBytes, hdr := mqttEncodeRetainedMessage(rm) // will copy the payload bytes
	smr, err := asm.jsa.storeMsg(mqttRetainedMsgsStreamSubject+key, hdr, rmBytes)
	if err == nil {
		// Update the new sequence.
		rf := &mqttRetainedMsgRef{
			sseq: smr.Sequence,
		}
		// Add/update the map. `true` to copy the payload bytes if needs to
		// update rmsCache.
		asm.handleRetainedMsg(key, rf, rm, true)
	} else {
		c.mu.Lock()
		acc := c.acc
		c.mu.Unlock()
		c.Errorf("unable to store retained message for account %q, subject %q: %v",
			acc.GetName(), key, err)
	}
}

// After a config reload, it is possible that the source of a publish retained
// message is no longer allowed to publish on the given topic. If that is the
// case, the retained message is removed from the map and will no longer be
// sent to (re)starting subscriptions.
//
// Server lock MUST NOT be held on entry.
func (s *Server) mqttCheckPubRetainedPerms() {
	sm := &s.mqtt.sessmgr
	sm.mu.RLock()
	done := len(sm.sessions) == 0
	sm.mu.RUnlock()

	if done {
		return
	}

	s.mu.Lock()
	users := make(map[string]*User, len(s.users))
	for un, u := range s.users {
		users[un] = u
	}
	s.mu.Unlock()

	// First get a list of all of the sessions.
	sm.mu.RLock()
	asms := make([]*mqttAccountSessionManager, 0, len(sm.sessions))
	for _, asm := range sm.sessions {
		asms = append(asms, asm)
	}
	sm.mu.RUnlock()

	type retainedMsg struct {
		subj string
		rmsg *mqttRetainedMsgRef
	}

	// For each session we will obtain a list of retained messages.
	var _rms [128]retainedMsg
	rms := _rms[:0]
	for _, asm := range asms {
		// Get all of the retained messages. Then we will sort them so
		// that they are in sequence order, which should help the file
		// store to not have to load out-of-order blocks so often.
		asm.mu.RLock()
		rms = rms[:0] // reuse slice
		for subj, rf := range asm.retmsgs {
			rms = append(rms, retainedMsg{
				subj: subj,
				rmsg: rf,
			})
		}
		asm.mu.RUnlock()
		slices.SortFunc(rms, func(i, j retainedMsg) int { return cmp.Compare(i.rmsg.sseq, j.rmsg.sseq) })

		perms := map[string]*perm{}
		deletes := map[string]uint64{}
		for _, rf := range rms {
			jsm, err := asm.jsa.loadMsg(mqttRetainedMsgsStreamName, rf.rmsg.sseq)
			if err != nil || jsm == nil {
				continue
			}
			rm, err := mqttDecodeRetainedMessage(jsm.Header, jsm.Data)
			if err != nil {
				continue
			}
			if rm.Source == _EMPTY_ {
				continue
			}
			// Lookup source from global users.
			u := users[rm.Source]
			if u != nil {
				p, ok := perms[rm.Source]
				if !ok {
					p = generatePubPerms(u.Permissions)
					perms[rm.Source] = p
				}
				// If there is permission and no longer allowed to publish in
				// the subject, remove the publish retained message from the map.
				if p != nil && !pubAllowed(p, rf.subj) {
					u = nil
				}
			}

			// Not present or permissions have changed such that the source can't
			// publish on that subject anymore: remove it from the map.
			if u == nil {
				asm.mu.Lock()
				delete(asm.retmsgs, rf.subj)
				asm.sl.Remove(rf.rmsg.sub)
				asm.mu.Unlock()
				deletes[rf.subj] = rf.rmsg.sseq
			}
		}

		for subject, seq := range deletes {
			asm.deleteRetainedMsg(seq)
			asm.notifyRetainedMsgDeleted(subject, seq)
		}
	}
}

// Helper to generate only pub permissions from a Permissions object
func generatePubPerms(perms *Permissions) *perm {
	var p *perm
	if perms.Publish.Allow != nil {
		p = &perm{}
		p.allow = NewSublistWithCache()
		for _, pubSubject := range perms.Publish.Allow {
			sub := &subscription{subject: []byte(pubSubject)}
			p.allow.Insert(sub)
		}
	}
	if len(perms.Publish.Deny) > 0 {
		if p == nil {
			p = &perm{}
		}
		p.deny = NewSublistWithCache()
		for _, pubSubject := range perms.Publish.Deny {
			sub := &subscription{subject: []byte(pubSubject)}
			p.deny.Insert(sub)
		}
	}
	return p
}

// Helper that checks if given `perms` allow to publish on the given `subject`
func pubAllowed(perms *perm, subject string) bool {
	allowed := true
	if perms.allow != nil {
		np, _ := perms.allow.NumInterest(subject)
		allowed = np != 0
	}
	// If we have a deny list and are currently allowed, check that as well.
	if allowed && perms.deny != nil {
		np, _ := perms.deny.NumInterest(subject)
		allowed = np == 0
	}
	return allowed
}

func (c *client) mqttEnqueuePubResponse(packetType byte, pi uint16, trace bool) {
	proto := [4]byte{packetType, 0x2, 0, 0}
	proto[2] = byte(pi >> 8)
	proto[3] = byte(pi)

	// Bits 3,2,1 and 0 of the fixed header in the PUBREL Control Packet are
	// reserved and MUST be set to 0,0,1 and 0 respectively. The Server MUST treat
	// any other value as malformed and close the Network Connection [MQTT-3.6.1-1].
	if packetType == mqttPacketPubRel {
		proto[0] |= 0x2
	}

	c.mu.Lock()
	c.enqueueProto(proto[:4])
	c.mu.Unlock()

	if trace {
		name := "(???)"
		switch packetType {
		case mqttPacketPubAck:
			name = "PUBACK"
		case mqttPacketPubRec:
			name = "PUBREC"
		case mqttPacketPubRel:
			name = "PUBREL"
		case mqttPacketPubComp:
			name = "PUBCOMP"
		}
		c.traceOutOp(name, []byte(fmt.Sprintf("pi=%v", pi)))
	}
}

func mqttParsePIPacket(r *mqttReader) (uint16, error) {
	pi, err := r.readUint16("packet identifier")
	if err != nil {
		return 0, err
	}
	if pi == 0 {
		return 0, errMQTTPacketIdentifierIsZero
	}
	return pi, nil
}

// Process a PUBACK (QoS1) or a PUBREC (QoS2) packet, acting as Sender. Set
// isPubRec to false to process as a PUBACK.
//
// Runs from the client's readLoop. No lock held on entry.
func (c *client) mqttProcessPublishReceived(pi uint16, isPubRec bool) (err error) {
	sess := c.mqtt.sess
	if sess == nil {
		return errMQTTInvalidSession
	}

	var jsAckSubject string
	sess.mu.Lock()
	// Must be the same client, and the session must have been setup for QoS2.
	if sess.c != c {
		sess.mu.Unlock()
		return errMQTTInvalidSession
	}
	if isPubRec {
		// The JS ACK subject for the PUBREL will be filled in at the delivery
		// attempt.
		sess.trackAsPubRel(pi, _EMPTY_)
	}
	jsAckSubject = sess.untrackPublish(pi)
	sess.mu.Unlock()

	if isPubRec {
		natsMsg, headerLen := mqttNewDeliverablePubRel(pi)
		_, err = sess.jsa.storeMsg(sess.pubRelSubject, headerLen, natsMsg)
		if err != nil {
			// Failure to send out PUBREL will terminate the connection.
			return err
		}
	}

	// Send the ack to JS to remove the pending message from the consumer.
	sess.jsa.sendAck(jsAckSubject)
	return nil
}

func (c *client) mqttProcessPubAck(pi uint16) error {
	return c.mqttProcessPublishReceived(pi, false)
}

func (c *client) mqttProcessPubRec(pi uint16) error {
	return c.mqttProcessPublishReceived(pi, true)
}

// Runs from the client's readLoop. No lock held on entry.
func (c *client) mqttProcessPubComp(pi uint16) {
	sess := c.mqtt.sess
	if sess == nil {
		return
	}

	var jsAckSubject string
	sess.mu.Lock()
	if sess.c != c {
		sess.mu.Unlock()
		return
	}
	jsAckSubject = sess.untrackPubRel(pi)
	sess.mu.Unlock()

	// Send the ack to JS to remove the pending message from the consumer.
	sess.jsa.sendAck(jsAckSubject)
}

// Return the QoS from the given PUBLISH protocol's flags
func mqttGetQoS(flags byte) byte {
	return flags & mqttPubFlagQoS >> 1
}

func mqttIsRetained(flags byte) bool {
	return flags&mqttPubFlagRetain != 0
}

func sparkbParseBirthDeathTopic(topic []byte) (isBirth, isDeath, isCertificate bool) {
	if bytes.HasPrefix(topic, sparkbCertificatesTopicPrefix) {
		isCertificate = true
		topic = topic[len(sparkbCertificatesTopicPrefix):]
	}
	if !bytes.HasPrefix(topic, sparkbNamespaceTopicPrefix) {
		return false, false, false
	}
	topic = topic[len(sparkbNamespaceTopicPrefix):]

	parts := bytes.Split(topic, []byte{'/'})
	if len(parts) < 3 || len(parts) > 4 {
		return false, false, false
	}
	typ := bytesToString(parts[1])
	switch typ {
	case sparkbNBIRTH, sparkbDBIRTH:
		isBirth = true
	case sparkbNDEATH, sparkbDDEATH:
		isDeath = true
	default:
		return false, false, false
	}
	return isBirth, isDeath, isCertificate
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
	pi, err := r.readUint16("packet identifier")
	if err != nil {
		return 0, nil, fmt.Errorf("reading packet identifier: %v", err)
	}
	end := r.pos + (pl - 2)
	var filters []*mqttFilter
	for r.pos < end {
		// Don't make a copy now because, this will happen during conversion
		// or when processing the sub.
		topic, err := r.readBytes("topic filter", false)
		if err != nil {
			return 0, nil, err
		}
		if len(topic) == 0 {
			return 0, nil, errMQTTTopicFilterCannotBeEmpty
		}
		// Spec [MQTT-3.8.3-1], [MQTT-3.10.3-1]
		if !utf8.Valid(topic) {
			return 0, nil, fmt.Errorf("invalid utf8 for topic filter %q", topic)
		}
		var qos byte
		// We are going to report if we had an error during the conversion,
		// but we don't fail the parsing. When processing the sub, we will
		// have an error then, and the processing of subs code will send
		// the proper mqttSubAckFailure flag for this given subscription.
		filter, err := mqttFilterToNATSSubject(topic)
		if err != nil {
			c.Errorf("invalid topic %q: %v", topic, err)
		}
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

// For a MQTT QoS0 subscription, we create a single NATS subscription on the
// actual subject, for instance "foo.bar".
//
// For a MQTT QoS1+ subscription, we create 2 subscriptions, one on "foo.bar"
// (as for QoS0, but sub.mqtt.qos will be 1 or 2), and one on the subject
// "$MQTT.sub.<uid>" which is the delivery subject of the JS durable consumer
// with the filter subject "$MQTT.msgs.foo.bar".
//
// This callback delivers messages to the client as QoS0 messages, either
// because: (a) they have been produced as MQTT QoS0 messages (and therefore
// only this callback can receive them); (b) they are MQTT QoS1+ published
// messages but this callback is for a subscription that is QoS0; or (c) the
// published messages come from (other) NATS publishers on the subject.
//
// This callback must reject a message if it is known to be a QoS1+ published
// message and this is the callback for a QoS1+ subscription because in that
// case, it will be handled by the other callback. This avoid getting duplicate
// deliveries.
func mqttDeliverMsgCbQoS0(sub *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
	if pc.kind == JETSTREAM && len(reply) > 0 && strings.HasPrefix(reply, jsAckPre) {
		return
	}

	// This is the client associated with the subscription.
	cc := sub.client

	// This is immutable
	sess := cc.mqtt.sess

	// Lock here, otherwise we may be called with sub.mqtt == nil. Ignore
	// wildcard subscriptions if this subject starts with '$', per Spec
	// [MQTT-4.7.2-1].
	sess.subsMu.RLock()
	subQoS := sub.mqtt.qos
	ignore := mqttMustIgnoreForReservedSub(sub, subject)
	sess.subsMu.RUnlock()

	if ignore {
		return
	}

	hdr, msg := pc.msgParts(rmsg)
	var topic []byte
	if pc.isMqtt() {
		// This is an MQTT publisher directly connected to this server.

		// Check the subscription's QoS. If the message was published with a
		// QoS>0 and the sub has the QoS>0 then the message will be delivered by
		// mqttDeliverMsgCbQoS12.
		msgQoS := mqttGetQoS(pc.mqtt.pp.flags)
		if subQoS > 0 && msgQoS > 0 {
			return
		}
		topic = pc.mqtt.pp.topic
		// Check for service imports where subject mapping is in play.
		if len(pc.pa.mapped) > 0 && len(pc.pa.psi) > 0 {
			topic = natsSubjectStrToMQTTTopic(subject)
		}

	} else {
		// Non MQTT client, could be NATS publisher, or ROUTER, etc..
		h := mqttParsePublishNATSHeader(hdr)

		// Check the subscription's QoS. If the message was published with a
		// QoS>0 (in the header) and the sub has the QoS>0 then the message will
		// be delivered by mqttDeliverMsgCbQoS12.
		if subQoS > 0 && h != nil && h.qos > 0 {
			return
		}

		// If size is more than what a MQTT client can handle, we should probably reject,
		// for now just truncate.
		if len(msg) > mqttMaxPayloadSize {
			msg = msg[:mqttMaxPayloadSize]
		}
		topic = natsSubjectStrToMQTTTopic(subject)
	}

	// Message never has a packet identifier nor is marked as duplicate.
	pc.mqttEnqueuePublishMsgTo(cc, sub, 0, 0, false, topic, msg)
}

// This is the callback attached to a JS durable subscription for a MQTT QoS 1+
// sub. Only JETSTREAM should be sending a message to this subject (the delivery
// subject associated with the JS durable consumer), but in cluster mode, this
// can be coming from a route, gw, etc... We make sure that if this is the case,
// the message contains a NATS/MQTT header that indicates that this is a
// published QoS1+ message.
func mqttDeliverMsgCbQoS12(sub *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
	// Message on foo.bar is stored under $MQTT.msgs.foo.bar, so the subject has to be
	// at least as long as the stream subject prefix "$MQTT.msgs.", and after removing
	// the prefix, has to be at least 1 character long.
	if len(subject) < len(mqttStreamSubjectPrefix)+1 {
		return
	}

	hdr, msg := pc.msgParts(rmsg)
	h := mqttParsePublishNATSHeader(hdr)
	if pc.kind != JETSTREAM && (h == nil || h.qos == 0) {
		// MQTT QoS 0 messages must be ignored, they will be delivered by the
		// other callback, the direct NATS subscription. All JETSTREAM messages
		// will have the header.
		return
	}

	// This is the client associated with the subscription.
	cc := sub.client

	// This is immutable
	sess := cc.mqtt.sess

	// We lock to check some of the subscription's fields and if we need to keep
	// track of pending acks, etc. There is no need to acquire the subsMu RLock
	// since sess.Lock is overarching for modifying subscriptions.
	sess.mu.Lock()
	if sess.c != cc || sub.mqtt == nil {
		sess.mu.Unlock()
		return
	}

	// In this callback we handle only QoS-published messages to QoS
	// subscriptions. Ignore if either is 0, will be delivered by the other
	// callback, mqttDeliverMsgCbQos1.
	var qos byte
	if h != nil {
		qos = h.qos
	}
	if qos > sub.mqtt.qos {
		qos = sub.mqtt.qos
	}
	if qos == 0 {
		sess.mu.Unlock()
		return
	}

	// Check for reserved subject violation. If so, we will send the ack to
	// remove the message, and do nothing else.
	strippedSubj := subject[len(mqttStreamSubjectPrefix):]
	if mqttMustIgnoreForReservedSub(sub, strippedSubj) {
		sess.mu.Unlock()
		sess.jsa.sendAck(reply)
		return
	}

	pi, dup := sess.trackPublish(sub.mqtt.jsDur, reply)
	sess.mu.Unlock()

	if pi == 0 {
		// We have reached max pending, don't send the message now.
		// JS will cause a redelivery and if by then the number of pending
		// messages has fallen below threshold, the message will be resent.
		return
	}

	originalTopic := natsSubjectStrToMQTTTopic(strippedSubj)
	pc.mqttEnqueuePublishMsgTo(cc, sub, pi, qos, dup, originalTopic, msg)
}

func mqttDeliverPubRelCb(sub *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
	if sub.client.mqtt == nil || sub.client.mqtt.sess == nil || reply == _EMPTY_ {
		return
	}

	hdr, _ := pc.msgParts(rmsg)
	pi := mqttParsePubRelNATSHeader(hdr)
	if pi == 0 {
		return
	}

	// This is the client associated with the subscription.
	cc := sub.client

	// This is immutable
	sess := cc.mqtt.sess

	sess.mu.Lock()
	if sess.c != cc || sess.pubRelConsumer == nil {
		sess.mu.Unlock()
		return
	}
	sess.trackAsPubRel(pi, reply)
	trace := cc.trace
	sess.mu.Unlock()

	cc.mqttEnqueuePubResponse(mqttPacketPubRel, pi, trace)
}

// The MQTT Server MUST NOT match Topic Filters starting with a wildcard
// character (# or +) with Topic Names beginning with a $ character, Spec
// [MQTT-4.7.2-1]. We will return true if there is a violation.
//
// Session or subMu lock must be held on entry to protect access to sub.mqtt.
func mqttMustIgnoreForReservedSub(sub *subscription, subject string) bool {
	// If the subject does not start with $ nothing to do here.
	if !sub.mqtt.reserved || len(subject) == 0 || subject[0] != mqttReservedPre {
		return false
	}
	return true
}

// Check if a sub is a reserved wildcard. E.g. '#', '*', or '*/" prefix.
func isMQTTReservedSubscription(subject string) bool {
	if len(subject) == 1 && (subject[0] == fwc || subject[0] == pwc) {
		return true
	}
	// Match "*.<>"
	if len(subject) > 1 && (subject[0] == pwc && subject[1] == btsep) {
		return true
	}
	return false
}

func sparkbReplaceDeathTimestamp(msg []byte) []byte {
	const VARINT = 0
	const TIMESTAMP = 1

	orig := msg
	buf := bytes.NewBuffer(make([]byte, 0, len(msg)+16)) // 16 bytes should be enough if we need to add a timestamp
	writeDeathTimestamp := func() {
		// [tck-id-conformance-mqtt-aware-ndeath-timestamp] A Sparkplug Aware
		// MQTT Server MAY replace the timestamp of NDEATH messages. If it does,
		// it MUST set the timestamp to the UTC time at which it attempts to
		// deliver the NDEATH to subscribed clients
		//
		// sparkB spec: 6.4.1. Google Protocol Buffer Schema
		//      optional uint64 timestamp = 1; // Timestamp at message sending time
		//
		// SparkplugB timestamps are milliseconds since epoch, represented as
		// uint64 in go, transmitted as protobuf varint.
		ts := uint64(time.Now().UnixMilli())
		buf.Write(protoEncodeVarint(TIMESTAMP<<3 | VARINT))
		buf.Write(protoEncodeVarint(ts))
	}

	for len(msg) > 0 {
		fieldNumericID, fieldType, size, err := protoScanField(msg)
		if err != nil {
			return orig
		}
		if fieldType != VARINT || fieldNumericID != TIMESTAMP {
			// Add the field as is
			buf.Write(msg[:size])
			msg = msg[size:]
			continue
		}

		writeDeathTimestamp()

		// Add the rest of the message as is, we are done
		buf.Write(msg[size:])
		return buf.Bytes()
	}

	// Add timestamp if we did not find one.
	writeDeathTimestamp()

	return buf.Bytes()
}

// Common function to mqtt delivery callbacks to serialize and send the message
// to the `cc` client.
func (c *client) mqttEnqueuePublishMsgTo(cc *client, sub *subscription, pi uint16, qos byte, dup bool, topic, msg []byte) {
	// [tck-id-conformance-mqtt-aware-nbirth-mqtt-retain] A Sparkplug Aware
	// MQTT Server MUST make NBIRTH messages available on the topic:
	// $sparkplug/certificates/namespace/group_id/NBIRTH/edge_node_id with
	// the MQTT retain flag set to true
	//
	// [tck-id-conformance-mqtt-aware-dbirth-mqtt-retain] A Sparkplug Aware
	// MQTT Server MUST make DBIRTH messages available on the topic:
	// $sparkplug/certificates/namespace/group_id/DBIRTH/edge_node_id/device_id
	// with the MQTT retain flag set to true
	//
	// $sparkplug/certificates messages are sent as NATS messages, so we
	// need to add the retain flag when sending them to MQTT clients.

	retain := false
	isBirth, isDeath, isCertificate := sparkbParseBirthDeathTopic(topic)
	if isBirth && qos == 0 {
		retain = isCertificate
	} else if isDeath && !isCertificate {
		msg = sparkbReplaceDeathTimestamp(msg)
	}

	flags, headerBytes := mqttMakePublishHeader(pi, qos, dup, retain, topic, len(msg))

	cc.mu.Lock()
	if sub.mqtt.prm != nil {
		for _, data := range sub.mqtt.prm {
			cc.queueOutbound(data)
		}
		sub.mqtt.prm = nil
	}
	cc.queueOutbound(headerBytes)
	cc.queueOutbound(msg)
	c.addToPCD(cc)
	trace := cc.trace
	cc.mu.Unlock()

	if trace {
		pp := mqttPublish{
			topic: topic,
			flags: flags,
			pi:    pi,
			sz:    len(msg),
		}
		cc.traceOutOp("PUBLISH", []byte(mqttPubTrace(&pp)))
	}
}

// Serializes to the given writer the message for the given subject.
func (w *mqttWriter) WritePublishHeader(pi uint16, qos byte, dup, retained bool, topic []byte, msgLen int) byte {
	// Compute len (will have to add packet id if message is sent as QoS>=1)
	pkLen := 2 + len(topic) + msgLen
	var flags byte

	// Set flags for dup/retained/qos1
	if dup {
		flags |= mqttPubFlagDup
	}
	if retained {
		flags |= mqttPubFlagRetain
	}
	if qos > 0 {
		pkLen += 2
		flags |= qos << 1
	}

	w.WriteByte(mqttPacketPub | flags)
	w.WriteVarInt(pkLen)
	w.WriteBytes(topic)
	if qos > 0 {
		w.WriteUint16(pi)
	}

	return flags
}

// Serializes to the given writer the message for the given subject.
func mqttMakePublishHeader(pi uint16, qos byte, dup, retained bool, topic []byte, msgLen int) (byte, []byte) {
	headerBuf := newMQTTWriter(mqttInitialPubHeader + len(topic))
	flags := headerBuf.WritePublishHeader(pi, qos, dup, retained, topic, msgLen)
	return flags, headerBuf.Bytes()
}

// Process the SUBSCRIBE packet.
//
// Process the list of subscriptions and update the given filter
// with the QoS that has been accepted (or failure).
//
// Spec [MQTT-3.8.4-3] says that if an exact same subscription is
// found, it needs to be replaced with the new one (possibly updating
// the qos) and that the flow of publications must not be interrupted,
// which I read as the replacement cannot be a "remove then add" if there
// is a chance that in between the 2 actions, published messages
// would be "lost" because there would not be any matching subscription.
//
// Run from client's readLoop.
// No lock held on entry.
func (c *client) mqttProcessSubs(filters []*mqttFilter) ([]*subscription, error) {
	// Those things are immutable, but since processing subs is not
	// really in the fast path, let's get them under the client lock.
	c.mu.Lock()
	asm := c.mqtt.asm
	sess := c.mqtt.sess
	trace := c.trace
	c.mu.Unlock()

	if err := asm.lockSession(sess, c); err != nil {
		return nil, err
	}
	defer asm.unlockSession(sess)
	return asm.processSubs(sess, c, filters, true, trace)
}

// Cleanup that is performed in processSubs if there was an error.
//
// Runs from client's readLoop.
// Lock not held on entry, but session is in the locked map.
func (sess *mqttSession) cleanupFailedSub(c *client, sub *subscription, cc *ConsumerConfig, jssub *subscription) {
	if sub != nil {
		c.processUnsub(sub.sid)
	}
	if jssub != nil {
		c.processUnsub(jssub.sid)
	}
	if cc != nil {
		sess.deleteConsumer(cc)
	}
}

// Make sure we are set up to deliver PUBREL messages to this QoS2-subscribed
// session.
func (sess *mqttSession) ensurePubRelConsumerSubscription(c *client) error {
	opts := c.srv.getOpts()
	ackWait := opts.MQTT.AckWait
	if ackWait == 0 {
		ackWait = mqttDefaultAckWait
	}
	maxAckPending := int(opts.MQTT.MaxAckPending)
	if maxAckPending == 0 {
		maxAckPending = mqttDefaultMaxAckPending
	}

	sess.mu.Lock()
	pubRelSubscribed := sess.pubRelSubscribed
	pubRelSubject := sess.pubRelSubject
	pubRelDeliverySubjectB := sess.pubRelDeliverySubjectB
	pubRelDeliverySubject := sess.pubRelDeliverySubject
	pubRelConsumer := sess.pubRelConsumer
	tmaxack := sess.tmaxack
	idHash := sess.idHash
	id := sess.id
	sess.mu.Unlock()

	// Subscribe before the consumer is created so we don't loose any messages.
	if !pubRelSubscribed {
		_, err := c.processSub(pubRelDeliverySubjectB, nil, pubRelDeliverySubjectB,
			mqttDeliverPubRelCb, false)
		if err != nil {
			c.Errorf("Unable to create subscription for JetStream consumer on %q: %v", pubRelDeliverySubject, err)
			return err
		}
		pubRelSubscribed = true
	}

	// Create the consumer if needed.
	if pubRelConsumer == nil {
		// Check that the limit of subs' maxAckPending are not going over the limit
		if after := tmaxack + maxAckPending; after > mqttMaxAckTotalLimit {
			return fmt.Errorf("max_ack_pending for all consumers would be %v which exceeds the limit of %v",
				after, mqttMaxAckTotalLimit)
		}

		ccr := &CreateConsumerRequest{
			Stream: mqttOutStreamName,
			Config: ConsumerConfig{
				DeliverSubject: pubRelDeliverySubject,
				Durable:        mqttPubRelConsumerDurablePrefix + idHash,
				AckPolicy:      AckExplicit,
				DeliverPolicy:  DeliverNew,
				FilterSubject:  pubRelSubject,
				AckWait:        ackWait,
				MaxAckPending:  maxAckPending,
				MemoryStorage:  opts.MQTT.ConsumerMemoryStorage,
			},
		}
		if opts.MQTT.ConsumerInactiveThreshold > 0 {
			ccr.Config.InactiveThreshold = opts.MQTT.ConsumerInactiveThreshold
		}
		if _, err := sess.jsa.createDurableConsumer(ccr); err != nil {
			c.Errorf("Unable to add JetStream consumer for PUBREL for client %q: err=%v", id, err)
			return err
		}
		pubRelConsumer = &ccr.Config
		tmaxack += maxAckPending
	}

	sess.mu.Lock()
	sess.pubRelSubscribed = pubRelSubscribed
	sess.pubRelConsumer = pubRelConsumer
	sess.tmaxack = tmaxack
	sess.mu.Unlock()

	return nil
}

// When invoked with a QoS of 0, looks for an existing JS durable consumer for
// the given sid and if one is found, delete the JS durable consumer and unsub
// the NATS subscription on the delivery subject.
//
// With a QoS > 0, creates or update the existing JS durable consumer along with
// its NATS subscription on a delivery subject.
//
// Session lock is acquired and released as needed. Session is in the locked
// map.
func (sess *mqttSession) processJSConsumer(c *client, subject, sid string,
	qos byte, fromSubProto bool) (*ConsumerConfig, *subscription, error) {

	sess.mu.Lock()
	cc, exists := sess.cons[sid]
	tmaxack := sess.tmaxack
	idHash := sess.idHash
	sess.mu.Unlock()

	// Check if we are already a JS consumer for this SID.
	if exists {
		// If current QoS is 0, it means that we need to delete the existing
		// one (that was QoS > 0)
		if qos == 0 {
			// The JS durable consumer's delivery subject is on a NUID of
			// the form: mqttSubPrefix + <nuid>. It is also used as the sid
			// for the NATS subscription, so use that for the lookup.
			c.mu.Lock()
			sub := c.subs[cc.DeliverSubject]
			c.mu.Unlock()

			sess.mu.Lock()
			delete(sess.cons, sid)
			sess.mu.Unlock()

			sess.deleteConsumer(cc)
			if sub != nil {
				c.processUnsub(sub.sid)
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
	var inbox string
	if exists {
		inbox = cc.DeliverSubject
	} else {
		inbox = mqttSubPrefix + nuid.Next()
		opts := c.srv.getOpts()
		ackWait := opts.MQTT.AckWait
		if ackWait == 0 {
			ackWait = mqttDefaultAckWait
		}
		maxAckPending := int(opts.MQTT.MaxAckPending)
		if maxAckPending == 0 {
			maxAckPending = mqttDefaultMaxAckPending
		}

		// Check that the limit of subs' maxAckPending are not going over the limit
		if after := tmaxack + maxAckPending; after > mqttMaxAckTotalLimit {
			return nil, nil, fmt.Errorf("max_ack_pending for all consumers would be %v which exceeds the limit of %v",
				after, mqttMaxAckTotalLimit)
		}

		durName := idHash + "_" + nuid.Next()
		ccr := &CreateConsumerRequest{
			Stream: mqttStreamName,
			Config: ConsumerConfig{
				DeliverSubject: inbox,
				Durable:        durName,
				AckPolicy:      AckExplicit,
				DeliverPolicy:  DeliverNew,
				FilterSubject:  mqttStreamSubjectPrefix + subject,
				AckWait:        ackWait,
				MaxAckPending:  maxAckPending,
				MemoryStorage:  opts.MQTT.ConsumerMemoryStorage,
			},
		}
		if opts.MQTT.ConsumerInactiveThreshold > 0 {
			ccr.Config.InactiveThreshold = opts.MQTT.ConsumerInactiveThreshold
		}
		if _, err := sess.jsa.createDurableConsumer(ccr); err != nil {
			c.Errorf("Unable to add JetStream consumer for subscription on %q: err=%v", subject, err)
			return nil, nil, err
		}
		cc = &ccr.Config
		tmaxack += maxAckPending
	}

	// This is an internal subscription on subject like "$MQTT.sub.<nuid>" that is setup
	// for the JS durable's deliver subject.
	sess.mu.Lock()
	sess.tmaxack = tmaxack
	sub, err := sess.processQOS12Sub(c, []byte(inbox), []byte(inbox),
		isMQTTReservedSubscription(subject), qos, cc.Durable, mqttDeliverMsgCbQoS12)
	sess.mu.Unlock()

	if err != nil {
		sess.deleteConsumer(cc)
		c.Errorf("Unable to create subscription for JetStream consumer on %q: %v", subject, err)
		return nil, nil, err
	}
	return cc, sub, nil
}

// Queues the published retained messages for each subscription and signals
// the writeLoop.
func (c *client) mqttSendRetainedMsgsToNewSubs(subs []*subscription) {
	c.mu.Lock()
	for _, sub := range subs {
		if sub.mqtt != nil && sub.mqtt.prm != nil {
			for _, data := range sub.mqtt.prm {
				c.queueOutbound(data)
			}
			sub.mqtt.prm = nil
		}
	}
	c.flushSignal()
	c.mu.Unlock()
}

func (c *client) mqttEnqueueSubAck(pi uint16, filters []*mqttFilter) {
	w := newMQTTWriter(7 + len(filters))
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

// Process the UNSUBSCRIBE packet.
//
// Given the list of topics, this is going to unsubscribe the low level NATS subscriptions
// and delete the JS durable consumers when applicable.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (c *client) mqttProcessUnsubs(filters []*mqttFilter) error {
	// Those things are immutable, but since processing unsubs is not
	// really in the fast path, let's get them under the client lock.
	c.mu.Lock()
	asm := c.mqtt.asm
	sess := c.mqtt.sess
	c.mu.Unlock()

	if err := asm.lockSession(sess, c); err != nil {
		return err
	}
	defer asm.unlockSession(sess)

	removeJSCons := func(sid string) {
		cc, ok := sess.cons[sid]
		if ok {
			delete(sess.cons, sid)
			sess.deleteConsumer(cc)
			// Need lock here since these are accessed by callbacks
			sess.mu.Lock()
			if seqPis, ok := sess.cpending[cc.Durable]; ok {
				delete(sess.cpending, cc.Durable)
				for _, pi := range seqPis {
					delete(sess.pendingPublish, pi)
				}
				if len(sess.pendingPublish) == 0 {
					sess.last_pi = 0
				}
			}
			sess.mu.Unlock()
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
	return sess.update(filters, false)
}

func (c *client) mqttEnqueueUnsubAck(pi uint16) {
	w := newMQTTWriter(4)
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
//   - '/' is replaced with '/.' if it is the first character in mt
//   - '/' is replaced with './' if the last or next character in mt is '/'
//     For instance, foo//bar would become foo./.bar
//   - '/' is replaced with '.' for all other conditions (foo/bar -> foo.bar)
//   - '.' is replaced with '//'.
//   - ' ' cause an error to be returned.
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
		case ' ':
			// As of now, we cannot support ' ' in the MQTT topic/filter.
			return nil, errMQTTUnsupportedCharacters
		case btsep:
			if !cp {
				makeCopy(i)
			}
			res = append(res, mqttTopicLevelSep, mqttTopicLevelSep)
			j++
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
func natsSubjectStrToMQTTTopic(subject string) []byte {
	return natsSubjectToMQTTTopic(stringToBytes(subject))
}

func natsSubjectToMQTTTopic(subject []byte) []byte {
	topic := make([]byte, len(subject))
	end := len(subject) - 1
	var j int
	for i := 0; i < len(subject); i++ {
		switch subject[i] {
		case mqttTopicLevelSep:
			if i < end {
				switch c := subject[i+1]; c {
				case btsep, mqttTopicLevelSep:
					if c == btsep {
						topic[j] = mqttTopicLevelSep
					} else {
						topic[j] = btsep
					}
					j++
					i++
				default:
				}
			}
		case btsep:
			topic[j] = mqttTopicLevelSep
			j++
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

func (r *mqttReader) reset(buf []byte) {
	if l := len(r.pbuf); l > 0 {
		tmp := make([]byte, l+len(buf))
		copy(tmp, r.pbuf)
		copy(tmp[l:], buf)
		buf = tmp
		r.pbuf = nil
	}
	r.buf = buf
	r.pos = 0
	r.pstart = 0
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

func (r *mqttReader) readPacketLen() (int, bool, error) {
	return r.readPacketLenWithCheck(true)
}

func (r *mqttReader) readPacketLenWithCheck(check bool) (int, bool, error) {
	m := 1
	v := 0
	for {
		var b byte
		if r.pos != len(r.buf) {
			b = r.buf[r.pos]
			r.pos++
		} else {
			break
		}
		v += int(b&0x7f) * m
		if (b & 0x80) == 0 {
			if check && r.pos+v > len(r.buf) {
				break
			}
			return v, true, nil
		}
		m *= 0x80
		if m > 0x200000 {
			return 0, false, errMQTTMalformedVarInt
		}
	}
	r.pbuf = make([]byte, len(r.buf)-r.pstart)
	copy(r.pbuf, r.buf[r.pstart:])
	return 0, false, nil
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

func newMQTTWriter(cap int) *mqttWriter {
	w := &mqttWriter{}
	w.Grow(cap)
	return w
}
