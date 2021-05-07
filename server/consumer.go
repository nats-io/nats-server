// Copyright 2019-2021 The NATS Authors
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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nuid"
	"golang.org/x/time/rate"
)

type ConsumerInfo struct {
	Stream         string          `json:"stream_name"`
	Name           string          `json:"name"`
	Created        time.Time       `json:"created"`
	Config         *ConsumerConfig `json:"config,omitempty"`
	Delivered      SequencePair    `json:"delivered"`
	AckFloor       SequencePair    `json:"ack_floor"`
	NumAckPending  int             `json:"num_ack_pending"`
	NumRedelivered int             `json:"num_redelivered"`
	NumWaiting     int             `json:"num_waiting"`
	NumPending     uint64          `json:"num_pending"`
	Cluster        *ClusterInfo    `json:"cluster,omitempty"`
}

type ConsumerConfig struct {
	Durable         string        `json:"durable_name,omitempty"`
	DeliverSubject  string        `json:"deliver_subject,omitempty"`
	DeliverPolicy   DeliverPolicy `json:"deliver_policy"`
	OptStartSeq     uint64        `json:"opt_start_seq,omitempty"`
	OptStartTime    *time.Time    `json:"opt_start_time,omitempty"`
	AckPolicy       AckPolicy     `json:"ack_policy"`
	AckWait         time.Duration `json:"ack_wait,omitempty"`
	MaxDeliver      int           `json:"max_deliver,omitempty"`
	FilterSubject   string        `json:"filter_subject,omitempty"`
	ReplayPolicy    ReplayPolicy  `json:"replay_policy"`
	RateLimit       uint64        `json:"rate_limit_bps,omitempty"` // Bits per sec
	SampleFrequency string        `json:"sample_freq,omitempty"`
	MaxWaiting      int           `json:"max_waiting,omitempty"`
	MaxAckPending   int           `json:"max_ack_pending,omitempty"`
	Heartbeat       time.Duration `json:"idle_heartbeat,omitempty"`
	FlowControl     bool          `json:"flow_control,omitempty"`

	// Don't add to general clients.
	Direct bool `json:"direct,omitempty"`
}

type CreateConsumerRequest struct {
	Stream string         `json:"stream_name"`
	Config ConsumerConfig `json:"config"`
}

// DeliverPolicy determines how the consumer should select the first message to deliver.
type DeliverPolicy int

const (
	// DeliverAll will be the default so can be omitted from the request.
	DeliverAll DeliverPolicy = iota
	// DeliverLast will start the consumer with the last sequence received.
	DeliverLast
	// DeliverNew will only deliver new messages that are sent after the consumer is created.
	DeliverNew
	// DeliverByStartSequence will look for a defined starting sequence to start.
	DeliverByStartSequence
	// DeliverByStartTime will select the first messsage with a timestamp >= to StartTime
	DeliverByStartTime
)

func (dp DeliverPolicy) String() string {
	switch dp {
	case DeliverAll:
		return "all"
	case DeliverLast:
		return "last"
	case DeliverNew:
		return "new"
	case DeliverByStartSequence:
		return "by_start_sequence"
	case DeliverByStartTime:
		return "by_start_time"
	default:
		return "undefined"
	}
}

// AckPolicy determines how the consumer should acknowledge delivered messages.
type AckPolicy int

const (
	// AckNone requires no acks for delivered messages.
	AckNone AckPolicy = iota
	// AckAll when acking a sequence number, this implicitly acks all sequences below this one as well.
	AckAll
	// AckExplicit requires ack or nack for all messages.
	AckExplicit
)

func (a AckPolicy) String() string {
	switch a {
	case AckNone:
		return "none"
	case AckAll:
		return "all"
	default:
		return "explicit"
	}
}

// ReplayPolicy determines how the consumer should replay messages it already has queued in the stream.
type ReplayPolicy int

const (
	// ReplayInstant will replay messages as fast as possible.
	ReplayInstant ReplayPolicy = iota
	// ReplayOriginal will maintain the same timing as the messages were received.
	ReplayOriginal
)

func (r ReplayPolicy) String() string {
	switch r {
	case ReplayInstant:
		return "instant"
	default:
		return "original"
	}
}

// OK
const OK = "+OK"

// Ack responses. Note that a nil or no payload is same as AckAck
var (
	// Ack
	AckAck = []byte("+ACK") // nil or no payload to ack subject also means ACK
	AckOK  = []byte(OK)     // deprecated but +OK meant ack as well.

	// Nack
	AckNak = []byte("-NAK")
	// Progress indicator
	AckProgress = []byte("+WPI")
	// Ack + Deliver the next message(s).
	AckNext = []byte("+NXT")
	// Terminate delivery of the message.
	AckTerm = []byte("+TERM")
)

// Consumer is a jetstream consumer.
type consumer struct {
	mu                sync.RWMutex
	js                *jetStream
	mset              *stream
	acc               *Account
	srv               *Server
	client            *client
	sysc              *client
	sid               int
	name              string
	stream            string
	sseq              uint64
	dseq              uint64
	adflr             uint64
	asflr             uint64
	sgap              uint64
	dsubj             string
	rlimit            *rate.Limiter
	reqSub            *subscription
	ackSub            *subscription
	ackReplyT         string
	ackSubj           string
	nextMsgSubj       string
	maxp              int
	pblimit           int
	maxpb             int
	pbytes            int
	fcsz              int
	fcid              string
	fcSub             *subscription
	outq              *jsOutQ
	pending           map[uint64]*Pending
	ptmr              *time.Timer
	rdq               []uint64
	rdqi              map[uint64]struct{}
	rdc               map[uint64]uint64
	maxdc             uint64
	waiting           *waitQueue
	cfg               ConsumerConfig
	store             ConsumerStore
	active            bool
	replay            bool
	filterWC          bool
	dtmr              *time.Timer
	gwdtmr            *time.Timer
	dthresh           time.Duration
	mch               chan struct{}
	qch               chan struct{}
	inch              chan bool
	sfreq             int32
	ackEventT         string
	deliveryExcEventT string
	created           time.Time
	closed            bool

	// Clustered.
	ca      *consumerAssignment
	node    RaftNode
	infoSub *subscription
	lqsent  time.Time

	// R>1 proposals
	pch   chan struct{}
	phead *proposal
	ptail *proposal
}

type proposal struct {
	data []byte
	next *proposal
}

const (
	// JsAckWaitDefault is the default AckWait, only applicable on explicit ack policy consumers.
	JsAckWaitDefault = 30 * time.Second
	// JsDeleteWaitTimeDefault is the default amount of time we will wait for non-durable
	// consumers to be in an inactive state before deleting them.
	JsDeleteWaitTimeDefault = 5 * time.Second
	// JsFlowControlMaxPending specifies default pending bytes during flow control that can be
	// outstanding.
	JsFlowControlMaxPending = 1 * 1024 * 1024
	// JsDefaultMaxAckPending is set for consumers with explicit ack that do not set the max ack pending.
	JsDefaultMaxAckPending = 20_000
)

func (mset *stream) addConsumer(config *ConsumerConfig) (*consumer, error) {
	return mset.addConsumerWithAssignment(config, _EMPTY_, nil)
}

func (mset *stream) addConsumerWithAssignment(config *ConsumerConfig, oname string, ca *consumerAssignment) (*consumer, error) {
	mset.mu.RLock()
	s, jsa := mset.srv, mset.jsa
	mset.mu.RUnlock()

	// If we do not have the consumer currently assigned to us in cluster mode we will proceed but warn.
	// This can happen on startup with restored state where on meta replay we still do not have
	// the assignment. Running in single server mode this always returns true.
	if oname != _EMPTY_ && !jsa.consumerAssigned(mset.name(), oname) {
		s.Debugf("Consumer %q > %q does not seem to be assigned to this server", mset.name(), oname)
	}

	if config == nil {
		return nil, fmt.Errorf("consumer config required")
	}

	var err error
	// For now expect a literal subject if its not empty. Empty means work queue mode (pull mode).
	if config.DeliverSubject != _EMPTY_ {
		if !subjectIsLiteral(config.DeliverSubject) {
			return nil, fmt.Errorf("consumer deliver subject has wildcards")
		}
		if mset.deliveryFormsCycle(config.DeliverSubject) {
			return nil, fmt.Errorf("consumer deliver subject forms a cycle")
		}
		if config.MaxWaiting != 0 {
			return nil, fmt.Errorf("consumer in push mode can not set max waiting")
		}
		if config.MaxAckPending > 0 && config.AckPolicy == AckNone {
			return nil, fmt.Errorf("consumer requires ack policy for max ack pending")
		}
		if config.Heartbeat > 0 && config.Heartbeat < 100*time.Millisecond {
			return nil, fmt.Errorf("consumer idle heartbeat needs to be >= 100ms")
		}
	} else {
		// Pull mode / work queue mode require explicit ack.
		if config.AckPolicy != AckExplicit {
			return nil, fmt.Errorf("consumer in pull mode requires explicit ack policy")
		}
		// They are also required to be durable since otherwise we will not know when to
		// clean them up.
		if config.Durable == _EMPTY_ {
			return nil, fmt.Errorf("consumer in pull mode requires a durable name")
		}
		if config.RateLimit > 0 {
			return nil, fmt.Errorf("consumer in pull mode can not have rate limit set")
		}
		if config.MaxWaiting < 0 {
			return nil, fmt.Errorf("consumer max waiting needs to be positive")
		}
		// Set to default if not specified.
		if config.MaxWaiting == 0 {
			config.MaxWaiting = JSWaitQueueDefaultMax
		}
		if config.Heartbeat > 0 {
			return nil, fmt.Errorf("consumer idle heartbeat requires a push based consumer")
		}
		if config.FlowControl {
			return nil, fmt.Errorf("consumer flow control requires a push based consumer")
		}
	}

	// Direct need to be non-mapped ephemerals.
	if config.Direct {
		if config.DeliverSubject == _EMPTY_ {
			return nil, fmt.Errorf("consumer direct requires a push based consumer")
		}
		if isDurableConsumer(config) {
			return nil, fmt.Errorf("consumer direct requires an ephemeral consumer")
		}
		if ca != nil {
			return nil, fmt.Errorf("consumer direct on a mapped consumer")
		}
	}

	// Setup proper default for ack wait if we are in explicit ack mode.
	if config.AckWait == 0 && (config.AckPolicy == AckExplicit || config.AckPolicy == AckAll) {
		config.AckWait = JsAckWaitDefault
	}
	// Setup default of -1, meaning no limit for MaxDeliver.
	if config.MaxDeliver == 0 {
		config.MaxDeliver = -1
	}
	// Set proper default for max ack pending if we are ack explicit and none has been set.
	if config.AckPolicy == AckExplicit && config.MaxAckPending == 0 {
		config.MaxAckPending = JsDefaultMaxAckPending
	}

	// Make sure any partition subject is also a literal.
	if config.FilterSubject != _EMPTY_ {
		subjects, hasExt := mset.allSubjects()
		if !validFilteredSubject(config.FilterSubject, subjects) && !hasExt {
			return nil, fmt.Errorf("consumer filter subject is not a valid subset of the interest subjects")
		}
	}

	// Check on start position conflicts.
	switch config.DeliverPolicy {
	case DeliverAll:
		if config.OptStartSeq > 0 {
			return nil, fmt.Errorf("consumer delivery policy is deliver all, but optional start sequence is also set")
		}
		if config.OptStartTime != nil {
			return nil, fmt.Errorf("consumer delivery policy is deliver all, but optional start time is also set")
		}
	case DeliverLast:
		if config.OptStartSeq > 0 {
			return nil, fmt.Errorf("consumer delivery policy is deliver last, but optional start sequence is also set")
		}
		if config.OptStartTime != nil {
			return nil, fmt.Errorf("consumer delivery policy is deliver last, but optional start time is also set")
		}
	case DeliverNew:
		if config.OptStartSeq > 0 {
			return nil, fmt.Errorf("consumer delivery policy is deliver new, but optional start sequence is also set")
		}
		if config.OptStartTime != nil {
			return nil, fmt.Errorf("consumer delivery policy is deliver new, but optional start time is also set")
		}
	case DeliverByStartSequence:
		if config.OptStartSeq == 0 {
			return nil, fmt.Errorf("consumer delivery policy is deliver by start sequence, but optional start sequence is not set")
		}
		if config.OptStartTime != nil {
			return nil, fmt.Errorf("consumer delivery policy is deliver by start sequence, but optional start time is also set")
		}
	case DeliverByStartTime:
		if config.OptStartTime == nil {
			return nil, fmt.Errorf("consumer delivery policy is deliver by start time, but optional start time is not set")
		}
		if config.OptStartSeq != 0 {
			return nil, fmt.Errorf("consumer delivery policy is deliver by start time, but optional start sequence is also set")
		}
	}

	sampleFreq := 0
	if config.SampleFrequency != "" {
		s := strings.TrimSuffix(config.SampleFrequency, "%")
		sampleFreq, err = strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse consumer sampling configuration: %v", err)
		}
	}

	// Grab the client, account and server reference.
	c := mset.client
	if c == nil {
		return nil, fmt.Errorf("stream not valid")
	}
	c.mu.Lock()
	s, a := c.srv, c.acc
	c.mu.Unlock()

	// Hold mset lock here.
	mset.mu.Lock()

	// If this one is durable and already exists, we let that be ok as long as the configs match.
	if isDurableConsumer(config) {
		if eo, ok := mset.consumers[config.Durable]; ok {
			mset.mu.Unlock()
			ocfg := eo.config()
			if reflect.DeepEqual(&ocfg, config) {
				return eo, nil
			} else {
				// If we are a push mode and not active and the only difference
				// is deliver subject then update and return.
				if configsEqualSansDelivery(ocfg, *config) && eo.hasNoLocalInterest() {
					eo.updateDeliverSubject(config.DeliverSubject)
					return eo, nil
				} else {
					return nil, fmt.Errorf("consumer already exists")
				}
			}
		}
	}

	// Check for any limits, if the config for the consumer sets a limit we check against that
	// but if not we use the value from account limits, if account limits is more restrictive
	// than stream config we prefer the account limits to handle cases where account limits are
	// updated during the lifecycle of the stream
	maxc := mset.cfg.MaxConsumers
	if mset.cfg.MaxConsumers <= 0 || mset.jsa.limits.MaxConsumers < mset.cfg.MaxConsumers {
		maxc = mset.jsa.limits.MaxConsumers
	}

	if maxc > 0 && len(mset.consumers) >= maxc {
		mset.mu.Unlock()
		return nil, fmt.Errorf("maximum consumers limit reached")
	}

	// Check on stream type conflicts with WorkQueues.
	if mset.cfg.Retention == WorkQueuePolicy && !config.Direct {
		// Force explicit acks here.
		if config.AckPolicy != AckExplicit {
			mset.mu.Unlock()
			return nil, fmt.Errorf("workqueue stream requires explicit ack")
		}

		if len(mset.consumers) > 0 {
			if config.FilterSubject == _EMPTY_ {
				mset.mu.Unlock()
				return nil, fmt.Errorf("multiple non-filtered consumers not allowed on workqueue stream")
			} else if !mset.partitionUnique(config.FilterSubject) {
				// We have a partition but it is not unique amongst the others.
				mset.mu.Unlock()
				return nil, fmt.Errorf("filtered consumer not unique on workqueue stream")
			}
		}
		if config.DeliverPolicy != DeliverAll {
			mset.mu.Unlock()
			return nil, fmt.Errorf("consumer must be deliver all on workqueue stream")
		}
	}

	// Set name, which will be durable name if set, otherwise we create one at random.
	o := &consumer{
		mset:    mset,
		js:      s.getJetStream(),
		acc:     a,
		srv:     s,
		client:  s.createInternalJetStreamClient(),
		sysc:    s.createInternalJetStreamClient(),
		cfg:     *config,
		dsubj:   config.DeliverSubject,
		outq:    mset.outq,
		active:  true,
		qch:     make(chan struct{}),
		mch:     make(chan struct{}, 1),
		sfreq:   int32(sampleFreq),
		maxdc:   uint64(config.MaxDeliver),
		maxp:    config.MaxAckPending,
		created: time.Now().UTC(),
	}

	// Bind internal client to the user account.
	o.client.registerWithAccount(a)
	// Bind to the system account.
	o.sysc.registerWithAccount(s.SystemAccount())

	if isDurableConsumer(config) {
		if len(config.Durable) > JSMaxNameLen {
			mset.mu.Unlock()
			return nil, fmt.Errorf("consumer name is too long, maximum allowed is %d", JSMaxNameLen)
		}
		o.name = config.Durable
		if o.isPullMode() {
			o.waiting = newWaitQueue(config.MaxWaiting)
		}
	} else if oname != _EMPTY_ {
		o.name = oname
	} else {
		for {
			o.name = createConsumerName()
			if _, ok := mset.consumers[o.name]; !ok {
				break
			}
		}
	}

	// Check if we have a rate limit set.
	if config.RateLimit != 0 {
		// TODO(dlc) - Make sane values or error if not sane?
		// We are configured in bits per sec so adjust to bytes.
		rl := rate.Limit(config.RateLimit / 8)
		// Burst should be set to maximum msg size for this account, etc.
		var burst int
		if mset.cfg.MaxMsgSize > 0 {
			burst = int(mset.cfg.MaxMsgSize)
		} else if mset.jsa.account.limits.mpay > 0 {
			burst = int(mset.jsa.account.limits.mpay)
		} else {
			s := mset.jsa.account.srv
			burst = int(s.getOpts().MaxPayload)
		}
		o.rlimit = rate.NewLimiter(rl, burst)
	}

	// Check if we have  filtered subject that is a wildcard.
	if config.FilterSubject != _EMPTY_ && subjectHasWildcard(config.FilterSubject) {
		o.filterWC = true
	}

	// already under lock, mset.Name() would deadlock
	o.stream = mset.cfg.Name
	o.ackEventT = JSMetricConsumerAckPre + "." + o.stream + "." + o.name
	o.deliveryExcEventT = JSAdvisoryConsumerMaxDeliveryExceedPre + "." + o.stream + "." + o.name

	if !isValidName(o.name) {
		mset.mu.Unlock()
		o.deleteWithoutAdvisory()
		return nil, fmt.Errorf("durable name can not contain '.', '*', '>'")
	}

	// Select starting sequence number
	o.selectStartingSeqNo()

	if !config.Direct {
		store, err := mset.store.ConsumerStore(o.name, config)
		if err != nil {
			mset.mu.Unlock()
			o.deleteWithoutAdvisory()
			return nil, fmt.Errorf("error creating store for consumer: %v", err)
		}
		o.store = store
	}

	// Now register with mset and create the ack subscription.
	// Check if we already have this one registered.
	if eo, ok := mset.consumers[o.name]; ok {
		mset.mu.Unlock()
		if !o.isDurable() || !o.isPushMode() {
			o.name = _EMPTY_ // Prevent removal since same name.
			o.deleteWithoutAdvisory()
			return nil, fmt.Errorf("consumer already exists")
		}
		// If we are here we have already registered this durable. If it is still active that is an error.
		if eo.isActive() {
			o.name = _EMPTY_ // Prevent removal since same name.
			o.deleteWithoutAdvisory()
			return nil, fmt.Errorf("consumer already exists and is still active")
		}
		// Since we are here this means we have a potentially new durable so we should update here.
		// Check that configs are the same.
		if !configsEqualSansDelivery(o.cfg, eo.cfg) {
			o.name = _EMPTY_ // Prevent removal since same name.
			o.deleteWithoutAdvisory()
			return nil, fmt.Errorf("consumer replacement durable config not the same")
		}
		// Once we are here we have a replacement push-based durable.
		eo.updateDeliverSubject(o.cfg.DeliverSubject)
		return eo, nil
	}

	// Set up the ack subscription for this consumer. Will use wildcard for all acks.
	// We will remember the template to generate replies with sequence numbers and use
	// that to scanf them back in.
	mn := mset.cfg.Name
	pre := fmt.Sprintf(jsAckT, mn, o.name)
	o.ackReplyT = fmt.Sprintf("%s.%%d.%%d.%%d.%%d.%%d", pre)
	o.ackSubj = fmt.Sprintf("%s.*.*.*.*.*", pre)
	o.nextMsgSubj = fmt.Sprintf(JSApiRequestNextT, mn, o.name)

	if o.isPushMode() {
		o.dthresh = JsDeleteWaitTimeDefault
		if !o.isDurable() {
			// Check if we are not durable that the delivery subject has interest.
			// Check in place here for interest. Will setup properly in setLeader.
			r := o.acc.sl.Match(o.cfg.DeliverSubject)
			if !o.hasDeliveryInterest(len(r.psubs)+len(r.qsubs) > 0) {
				// Directs can let the interest come to us eventually, but setup delete timer.
				if config.Direct {
					o.updateDeliveryInterest(false)
				} else {
					mset.mu.Unlock()
					o.deleteWithoutAdvisory()
					return nil, errNoInterest
				}
			}
		}
	}

	// Set our ca.
	if ca != nil {
		o.setConsumerAssignment(ca)
	}

	mset.setConsumer(o)
	mset.mu.Unlock()

	if config.Direct || (!s.JetStreamIsClustered() && s.standAloneMode()) {
		o.setLeader(true)
	}

	// This is always true in single server mode.
	if o.isLeader() {
		// Send advisory.
		var suppress bool
		if !s.standAloneMode() && ca == nil {
			suppress = true
		} else if ca != nil {
			suppress = ca.responded
		}
		if !suppress {
			o.sendCreateAdvisory()
		}
	}

	return o, nil
}

func (o *consumer) consumerAssignment() *consumerAssignment {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.ca
}

func (o *consumer) setConsumerAssignment(ca *consumerAssignment) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ca = ca
	// Set our node.
	if ca != nil {
		o.node = ca.Group.node
	}
}

// Lock should be held.
func (o *consumer) isLeader() bool {
	if o.node != nil {
		return o.node.Leader()
	}
	return true
}

func (o *consumer) setLeader(isLeader bool) {
	o.mu.RLock()
	mset := o.mset
	isRunning := o.ackSub != nil
	o.mu.RUnlock()

	// If we are here we have a change in leader status.
	if isLeader {
		if mset == nil || isRunning {
			return
		}

		mset.mu.RLock()
		s, jsa, stream := mset.srv, mset.jsa, mset.cfg.Name
		mset.mu.RUnlock()

		o.mu.Lock()
		// Restore our saved state. During non-leader status we just update our underlying store.
		o.readStoredState()

		// Do info sub.
		if o.infoSub == nil && jsa != nil {
			isubj := fmt.Sprintf(clusterConsumerInfoT, jsa.acc(), stream, o.name)
			// Note below the way we subscribe here is so that we can send requests to ourselves.
			o.infoSub, _ = s.systemSubscribe(isubj, _EMPTY_, false, o.sysc, o.handleClusterConsumerInfoRequest)
		}

		var err error
		if o.ackSub, err = o.subscribeInternal(o.ackSubj, o.processAck); err != nil {
			o.mu.Unlock()
			o.deleteWithoutAdvisory()
			return
		}

		// Setup the internal sub for next message requests regardless.
		// Will error if wrong mode to provide feedback to users.
		if o.reqSub, err = o.subscribeInternal(o.nextMsgSubj, o.processNextMsgReq); err != nil {
			o.mu.Unlock()
			o.deleteWithoutAdvisory()
			return
		}

		// Check on flow control settings.
		if o.cfg.FlowControl {
			o.setMaxPendingBytes(JsFlowControlMaxPending)
			fcsubj := fmt.Sprintf(jsFlowControl, stream, o.name)
			if o.fcSub, err = o.subscribeInternal(fcsubj, o.processFlowControl); err != nil {
				o.mu.Unlock()
				o.deleteWithoutAdvisory()
				return
			}
		}

		// Setup initial pending.
		o.setInitialPending()

		// If push mode, register for notifications on interest.
		if o.isPushMode() {
			o.inch = make(chan bool, 8)
			o.acc.sl.RegisterNotification(o.cfg.DeliverSubject, o.inch)
			if o.active = <-o.inch; !o.active {
				// Check gateways in case they are enabled.
				if s.gateway.enabled {
					o.active = s.hasGatewayInterest(o.acc.Name, o.cfg.DeliverSubject)
					stopAndClearTimer(&o.gwdtmr)
					o.gwdtmr = time.AfterFunc(time.Second, func() { o.watchGWinterest() })
				}
			}
		}

		// If we are not in ReplayInstant mode mark us as in replay state until resolved.
		if o.cfg.ReplayPolicy != ReplayInstant {
			o.replay = true
		}

		// Recreate quit channel.
		o.qch = make(chan struct{})
		qch := o.qch
		node := o.node
		if node != nil && o.pch == nil {
			o.pch = make(chan struct{}, 1)
		}
		o.mu.Unlock()

		// Now start up Go routine to deliver msgs.
		go o.loopAndGatherMsgs(qch)

		// If we are R>1 spin up our proposal loop.
		if node != nil {
			go o.loopAndForwardProposals(qch)
		}

	} else {
		// Shutdown the go routines and the subscriptions.
		o.mu.Lock()
		o.unsubscribe(o.ackSub)
		o.unsubscribe(o.reqSub)
		o.unsubscribe(o.fcSub)
		o.ackSub = nil
		o.reqSub = nil
		o.fcSub = nil
		if o.infoSub != nil {
			o.srv.sysUnsubscribe(o.infoSub)
			o.infoSub = nil
		}
		if o.qch != nil {
			close(o.qch)
			o.qch = nil
		}
		o.mu.Unlock()
	}
}

func (o *consumer) handleClusterConsumerInfoRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	o.mu.RLock()
	sysc := o.sysc
	o.mu.RUnlock()
	sysc.sendInternalMsg(reply, _EMPTY_, nil, o.info())
}

// Lock should be held.
func (o *consumer) subscribeInternal(subject string, cb msgHandler) (*subscription, error) {
	c := o.client
	if c == nil {
		return nil, fmt.Errorf("invalid consumer")
	}
	if !c.srv.eventsEnabled() {
		return nil, ErrNoSysAccount
	}
	if cb == nil {
		return nil, fmt.Errorf("undefined message handler")
	}

	o.sid++

	// Now create the subscription
	return c.processSub([]byte(subject), nil, []byte(strconv.Itoa(o.sid)), cb, false)
}

// Unsubscribe from our subscription.
// Lock should be held.
func (o *consumer) unsubscribe(sub *subscription) {
	if sub == nil || o.client == nil {
		return
	}
	o.client.processUnsub(sub.sid)
}

// We need to make sure we protect access to the outq.
// Do all advisory sends here.
func (o *consumer) sendAdvisory(subj string, msg []byte) {
	o.outq.send(&jsPubMsg{subj, subj, _EMPTY_, nil, msg, nil, 0, nil})
}

func (o *consumer) sendDeleteAdvisoryLocked() {
	e := JSConsumerActionAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerActionAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   o.stream,
		Consumer: o.name,
		Action:   DeleteEvent,
		Domain:   o.srv.getOpts().JetStreamDomain,
	}

	j, err := json.Marshal(e)
	if err != nil {
		return
	}

	subj := JSAdvisoryConsumerDeletedPre + "." + o.stream + "." + o.name
	o.sendAdvisory(subj, j)
}

func (o *consumer) sendCreateAdvisory() {
	o.mu.Lock()
	defer o.mu.Unlock()

	e := JSConsumerActionAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerActionAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   o.stream,
		Consumer: o.name,
		Action:   CreateEvent,
		Domain:   o.srv.getOpts().JetStreamDomain,
	}

	j, err := json.Marshal(e)
	if err != nil {
		return
	}

	subj := JSAdvisoryConsumerCreatedPre + "." + o.stream + "." + o.name
	o.sendAdvisory(subj, j)
}

// Created returns created time.
func (o *consumer) createdTime() time.Time {
	o.mu.Lock()
	created := o.created
	o.mu.Unlock()
	return created
}

// Internal to allow creation time to be restored.
func (o *consumer) setCreatedTime(created time.Time) {
	o.mu.Lock()
	o.created = created
	o.mu.Unlock()
}

// This will check for extended interest in a subject. If we have local interest we just return
// that, but in the absence of local interest and presence of gateways or service imports we need
// to check those as well.
func (o *consumer) hasDeliveryInterest(localInterest bool) bool {
	o.mu.Lock()
	mset := o.mset
	if mset == nil {
		o.mu.Unlock()
		return false
	}
	acc := o.acc
	deliver := o.cfg.DeliverSubject
	o.mu.Unlock()

	if localInterest {
		return true
	}

	// If we are here check gateways.
	if s := acc.srv; s != nil && s.hasGatewayInterest(acc.Name, deliver) {
		return true
	}
	return false
}

func (s *Server) hasGatewayInterest(account, subject string) bool {
	gw := s.gateway
	if !gw.enabled {
		return false
	}
	gw.RLock()
	defer gw.RUnlock()
	for _, gwc := range gw.outo {
		psi, qr := gwc.gatewayInterest(account, subject)
		if psi || qr != nil {
			return true
		}
	}
	return false
}

// This processes an update to the local interest for a deliver subject.
func (o *consumer) updateDeliveryInterest(localInterest bool) bool {
	interest := o.hasDeliveryInterest(localInterest)

	o.mu.Lock()
	defer o.mu.Unlock()

	mset := o.mset
	if mset == nil || o.isPullMode() {
		return false
	}

	if interest && !o.active {
		o.signalNewMessages()
	}
	o.active = interest

	// If the delete timer has already been set do not clear here and return.
	if o.dtmr != nil && !o.isDurable() && !interest {
		return true
	}

	// Stop and clear the delete timer always.
	stopAndClearTimer(&o.dtmr)

	// If we do not have interest anymore and we are not durable start
	// a timer to delete us. We wait for a bit in case of server reconnect.
	if !o.isDurable() && !interest {
		o.dtmr = time.AfterFunc(o.dthresh, func() { o.deleteNotActive() })
		return true
	}
	return false
}

func (o *consumer) deleteNotActive() {
	// Need to check again if there is not an interest now that the timer fires.
	if !o.hasNoLocalInterest() {
		return
	}
	o.mu.RLock()
	if o.mset == nil {
		o.mu.RUnlock()
		return
	}
	s, js := o.mset.srv, o.mset.srv.js
	acc, stream, name := o.acc.Name, o.stream, o.name
	o.mu.RUnlock()

	// If we are clustered, check if we still have this consumer assigned.
	// If we do forward a proposal to delete ourselves to the metacontroller leader.
	if s.JetStreamIsClustered() {
		js.mu.RLock()
		ca := js.consumerAssignment(acc, stream, name)
		cc := js.cluster
		js.mu.RUnlock()
		if ca != nil && cc != nil {
			cca := *ca
			cca.Reply = _EMPTY_
			meta, removeEntry := cc.meta, encodeDeleteConsumerAssignment(&cca)
			meta.ForwardProposal(removeEntry)

			// Check to make sure we went away.
			// Don't think this needs to be a monitored go routine.
			go func() {
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()
				for range ticker.C {
					js.mu.RLock()
					ca := js.consumerAssignment(acc, stream, name)
					js.mu.RUnlock()
					if ca != nil {
						s.Warnf("Consumer assignment not cleaned up, retrying")
						meta.ForwardProposal(removeEntry)
					} else {
						return
					}
				}
			}()
		}
	}

	// We will delete here regardless.
	o.delete()
}

func (o *consumer) watchGWinterest() {
	pa := o.isActive()
	// If there is no local interest...
	if o.hasNoLocalInterest() {
		o.updateDeliveryInterest(false)
		if !pa && o.isActive() {
			o.signalNewMessages()
		}
	}

	// We want this to always be running so we can also pick up on interest returning.
	o.mu.Lock()
	if o.gwdtmr != nil {
		o.gwdtmr.Reset(time.Second)
	} else {
		stopAndClearTimer(&o.gwdtmr)
		o.gwdtmr = time.AfterFunc(time.Second, func() { o.watchGWinterest() })
	}
	o.mu.Unlock()
}

// Config returns the consumer's configuration.
func (o *consumer) config() ConsumerConfig {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.cfg
}

// Force expiration of all pending.
// Lock should be held.
func (o *consumer) forceExpirePending() {
	var expired []uint64
	for seq := range o.pending {
		if !o.onRedeliverQueue(seq) {
			expired = append(expired, seq)
		}
	}
	if len(expired) > 0 {
		sort.Slice(expired, func(i, j int) bool { return expired[i] < expired[j] })
		o.addToRedeliverQueue(expired...)
		// Now we should update the timestamp here since we are redelivering.
		// We will use an incrementing time to preserve order for any other redelivery.
		off := time.Now().UnixNano() - o.pending[expired[0]].Timestamp
		for _, seq := range expired {
			if p, ok := o.pending[seq]; ok && p != nil {
				p.Timestamp += off
			}
		}
		o.ptmr.Reset(o.ackWait(0))
	}
	o.signalNewMessages()
}

// This is a config change for the delivery subject for a
// push based consumer.
func (o *consumer) updateDeliverSubject(newDeliver string) {
	// Update the config and the dsubj
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed || o.isPullMode() || o.cfg.DeliverSubject == newDeliver {
		return
	}

	// Force redeliver of all pending on change of delivery subject.
	if len(o.pending) > 0 {
		o.forceExpirePending()
	}

	o.acc.sl.ClearNotification(o.dsubj, o.inch)
	o.dsubj, o.cfg.DeliverSubject = newDeliver, newDeliver
	// When we register new one it will deliver to update state loop.
	o.acc.sl.RegisterNotification(newDeliver, o.inch)
}

// Check that configs are equal but allow delivery subjects to be different.
func configsEqualSansDelivery(a, b ConsumerConfig) bool {
	// These were copied in so can set Delivery here.
	a.DeliverSubject, b.DeliverSubject = _EMPTY_, _EMPTY_
	return a == b
}

// Helper to send a reply to an ack.
func (o *consumer) sendAckReply(subj string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.sendAdvisory(subj, nil)
}

// Process a message for the ack reply subject delivered with a message.
func (o *consumer) processAck(_ *subscription, c *client, subject, reply string, rmsg []byte) {
	_, msg := c.msgParts(rmsg)
	sseq, dseq, dc := ackReplyInfo(subject)

	skipAckReply := sseq == 0

	switch {
	case len(msg) == 0, bytes.Equal(msg, AckAck), bytes.Equal(msg, AckOK):
		o.processAckMsg(sseq, dseq, dc, true)
	case bytes.HasPrefix(msg, AckNext):
		o.processAckMsg(sseq, dseq, dc, true)
		// processNextMsgReq can be invoked from an internal subscription or from here.
		// Therefore, it has to call msgParts(), so we can't simply pass msg[len(AckNext):]
		// with current c.pa.hdr because it would cause a panic.  We will save the current
		// c.pa.hdr value and disable headers before calling processNextMsgReq and then
		// restore so that we don't mess with the calling stack in case it is used
		// somewhere else.
		phdr := c.pa.hdr
		c.pa.hdr = -1
		o.processNextMsgReq(nil, c, subject, reply, msg[len(AckNext):])
		c.pa.hdr = phdr
		skipAckReply = true
	case bytes.Equal(msg, AckNak):
		o.processNak(sseq, dseq)
	case bytes.Equal(msg, AckProgress):
		o.progressUpdate(sseq)
	case bytes.Equal(msg, AckTerm):
		o.processTerm(sseq, dseq, dc)
	}

	// Ack the ack if requested.
	if len(reply) > 0 && !skipAckReply {
		o.sendAckReply(reply)
	}
}

// Used to process a working update to delay redelivery.
func (o *consumer) progressUpdate(seq uint64) {
	o.mu.Lock()
	if len(o.pending) > 0 {
		if p, ok := o.pending[seq]; ok {
			p.Timestamp = time.Now().UnixNano()
			// Update store system.
			o.updateDelivered(p.Sequence, seq, 1, p.Timestamp)
		}
	}
	o.mu.Unlock()
}

// Lock should be held.
func (o *consumer) updateSkipped() {
	// Clustered mode and R>1 only.
	if o.node == nil || !o.isLeader() {
		return
	}
	var b [1 + 8]byte
	b[0] = byte(updateSkipOp)
	var le = binary.LittleEndian
	le.PutUint64(b[1:], o.sseq)
	o.propose(b[:])
}

func (o *consumer) loopAndForwardProposals(qch chan struct{}) {
	o.mu.RLock()
	node, pch := o.node, o.pch
	o.mu.RUnlock()

	if node == nil || pch == nil {
		return
	}

	forwardProposals := func() {
		o.mu.Lock()
		proposal := o.phead
		o.phead, o.ptail = nil, nil
		o.mu.Unlock()
		// 256k max for now per batch.
		const maxBatch = 256 * 1024
		var entries []*Entry
		for sz := 0; proposal != nil; proposal = proposal.next {
			entries = append(entries, &Entry{EntryNormal, proposal.data})
			sz += len(proposal.data)
			if sz > maxBatch {
				node.ProposeDirect(entries)
				sz, entries = 0, entries[:0]
			}
		}
		if len(entries) > 0 {
			node.ProposeDirect(entries)
		}
	}

	for {
		select {
		case <-qch:
			forwardProposals()
			return
		case <-pch:
			forwardProposals()
		}
	}
}

// Lock should be held.
func (o *consumer) propose(entry []byte) {
	var notify bool
	p := &proposal{data: entry}
	if o.phead == nil {
		o.phead = p
		notify = true
	} else {
		o.ptail.next = p
	}
	o.ptail = p

	// Kick our looper routine if needed.
	if notify {
		select {
		case o.pch <- struct{}{}:
		default:
		}
	}
}

// Lock should be held.
func (o *consumer) updateDelivered(dseq, sseq, dc uint64, ts int64) {
	// Clustered mode and R>1.
	if o.node != nil {
		// Inline for now, use variable compression.
		var b [4*binary.MaxVarintLen64 + 1]byte
		b[0] = byte(updateDeliveredOp)
		n := 1
		n += binary.PutUvarint(b[n:], dseq)
		n += binary.PutUvarint(b[n:], sseq)
		n += binary.PutUvarint(b[n:], dc)
		n += binary.PutVarint(b[n:], ts)
		o.propose(b[:n])
	}
	if o.store != nil {
		// Update local state always.
		o.store.UpdateDelivered(dseq, sseq, dc, ts)
	}
}

// Lock should be held.
func (o *consumer) updateAcks(dseq, sseq uint64) {
	if o.node != nil {
		// Inline for now, use variable compression.
		var b [2*binary.MaxVarintLen64 + 1]byte
		b[0] = byte(updateAcksOp)
		n := 1
		n += binary.PutUvarint(b[n:], dseq)
		n += binary.PutUvarint(b[n:], sseq)
		o.propose(b[:n])
	} else if o.store != nil {
		o.store.UpdateAcks(dseq, sseq)
	}
}

// Process a NAK.
func (o *consumer) processNak(sseq, dseq uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Check for out of range.
	if dseq <= o.adflr || dseq > o.dseq {
		return
	}
	// If we are explicit ack make sure this is still on our pending list.
	if len(o.pending) > 0 {
		if _, ok := o.pending[sseq]; !ok {
			return
		}
	}
	// If already queued up also ignore.
	if !o.onRedeliverQueue(sseq) {
		o.addToRedeliverQueue(sseq)
	}

	o.signalNewMessages()
}

// Process a TERM
func (o *consumer) processTerm(sseq, dseq, dc uint64) {
	// Treat like an ack to suppress redelivery.
	o.processAckMsg(sseq, dseq, dc, false)

	o.mu.Lock()
	defer o.mu.Unlock()

	// Deliver an advisory
	e := JSConsumerDeliveryTerminatedAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerDeliveryTerminatedAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:      o.stream,
		Consumer:    o.name,
		ConsumerSeq: dseq,
		StreamSeq:   sseq,
		Deliveries:  dc,
		Domain:      o.srv.getOpts().JetStreamDomain,
	}

	j, err := json.Marshal(e)
	if err != nil {
		return
	}

	subj := JSAdvisoryConsumerMsgTerminatedPre + "." + o.stream + "." + o.name
	o.sendAdvisory(subj, j)
}

// Introduce a small delay in when timer fires to check pending.
// Allows bursts to be treated in same time frame.
const ackWaitDelay = time.Millisecond

// ackWait returns how long to wait to fire the pending timer.
func (o *consumer) ackWait(next time.Duration) time.Duration {
	if next > 0 {
		return next + ackWaitDelay
	}
	return o.cfg.AckWait + ackWaitDelay
}

// This will restore the state from disk.
func (o *consumer) readStoredState() error {
	if o.store == nil {
		return nil
	}
	state, err := o.store.State()
	if err == nil && state != nil {
		o.applyState(state)
	}
	return err
}

// Apply the consumer stored state.
func (o *consumer) applyState(state *ConsumerState) {
	if state == nil {
		return
	}

	o.dseq = state.Delivered.Consumer + 1
	o.sseq = state.Delivered.Stream + 1
	o.adflr = state.AckFloor.Consumer
	o.asflr = state.AckFloor.Stream
	o.pending = state.Pending
	o.rdc = state.Redelivered

	// Setup tracking timer if we have restored pending.
	if len(o.pending) > 0 && o.ptmr == nil {
		o.ptmr = time.AfterFunc(o.ackWait(0), o.checkPending)
	}
}

func (o *consumer) readStoreState() *ConsumerState {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.store == nil {
		return nil
	}
	state, _ := o.store.State()
	return state
}

// Sets our store state from another source. Used in clustered mode on snapshot restore.
func (o *consumer) setStoreState(state *ConsumerState) error {
	if state == nil || o.store == nil {
		return nil
	}
	o.applyState(state)
	return o.store.Update(state)
}

// Update our state to the store.
func (o *consumer) writeStoreState() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.store == nil {
		return nil
	}

	state := ConsumerState{
		Delivered: SequencePair{
			Consumer: o.dseq - 1,
			Stream:   o.sseq - 1,
		},
		AckFloor: SequencePair{
			Consumer: o.adflr,
			Stream:   o.asflr,
		},
		Pending:     o.pending,
		Redelivered: o.rdc,
	}
	return o.store.Update(&state)
}

// Info returns our current consumer state.
func (o *consumer) info() *ConsumerInfo {
	o.mu.RLock()
	mset := o.mset
	if mset == nil || mset.srv == nil {
		o.mu.RUnlock()
		return nil
	}
	js := o.js
	o.mu.RUnlock()

	if js == nil {
		return nil
	}

	ci := js.clusterInfo(o.raftGroup())

	o.mu.RLock()
	defer o.mu.RUnlock()

	cfg := o.cfg
	info := &ConsumerInfo{
		Stream:  o.stream,
		Name:    o.name,
		Created: o.created,
		Config:  &cfg,
		Delivered: SequencePair{
			Consumer: o.dseq - 1,
			Stream:   o.sseq - 1,
		},
		AckFloor: SequencePair{
			Consumer: o.adflr,
			Stream:   o.asflr,
		},
		NumAckPending:  len(o.pending),
		NumRedelivered: len(o.rdc),
		NumPending:     o.sgap,
		Cluster:        ci,
	}
	// If we are a pull mode consumer, report on number of waiting requests.
	if o.isPullMode() {
		info.NumWaiting = o.waiting.len()
	}
	return info
}

// Will signal us that new messages are available. Will break out of waiting.
func (o *consumer) signalNewMessages() {
	// Kick our new message channel
	select {
	case o.mch <- struct{}{}:
	default:
	}
}

// shouldSample lets us know if we are sampling metrics on acks.
func (o *consumer) shouldSample() bool {
	switch {
	case o.sfreq <= 0:
		return false
	case o.sfreq >= 100:
		return true
	}

	// TODO(ripienaar) this is a tad slow so we need to rethink here, however this will only
	// hit for those with sampling enabled and its not the default
	return rand.Int31n(100) <= o.sfreq
}

func (o *consumer) sampleAck(sseq, dseq, dc uint64) {
	if !o.shouldSample() {
		return
	}

	now := time.Now().UTC()
	unow := now.UnixNano()

	e := JSConsumerAckMetric{
		TypedEvent: TypedEvent{
			Type: JSConsumerAckMetricType,
			ID:   nuid.Next(),
			Time: now,
		},
		Stream:      o.stream,
		Consumer:    o.name,
		ConsumerSeq: dseq,
		StreamSeq:   sseq,
		Delay:       unow - o.pending[sseq].Timestamp,
		Deliveries:  dc,
		Domain:      o.srv.getOpts().JetStreamDomain,
	}

	j, err := json.Marshal(e)
	if err != nil {
		return
	}

	o.sendAdvisory(o.ackEventT, j)
}

func (o *consumer) processAckMsg(sseq, dseq, dc uint64, doSample bool) {
	o.mu.Lock()
	var sagap uint64
	var needSignal bool

	switch o.cfg.AckPolicy {
	case AckExplicit:
		if p, ok := o.pending[sseq]; ok {
			if doSample {
				o.sampleAck(sseq, dseq, dc)
			}
			if o.maxp > 0 && len(o.pending) >= o.maxp {
				needSignal = true
			}
			delete(o.pending, sseq)
			// Use the original deliver sequence from our pending record.
			dseq = p.Sequence
		}
		if len(o.pending) == 0 {
			o.adflr, o.asflr = o.dseq-1, o.sseq-1
		} else if dseq == o.adflr+1 {
			o.adflr, o.asflr = dseq, sseq
			for ss := sseq + 1; ss < o.sseq; ss++ {
				if p, ok := o.pending[ss]; ok {
					if p.Sequence > 0 {
						o.adflr, o.asflr = p.Sequence-1, ss-1
					}
					break
				}
			}
		}
		// We do these regardless.
		delete(o.rdc, sseq)
		o.removeFromRedeliverQueue(sseq)
	case AckAll:
		// no-op
		if dseq <= o.adflr || sseq <= o.asflr {
			o.mu.Unlock()
			return
		}
		if o.maxp > 0 && len(o.pending) >= o.maxp {
			needSignal = true
		}
		sagap = sseq - o.asflr
		o.adflr, o.asflr = dseq, sseq
		for seq := sseq; seq > sseq-sagap; seq-- {
			delete(o.pending, seq)
			delete(o.rdc, seq)
			o.removeFromRedeliverQueue(seq)
		}
	case AckNone:
		// FIXME(dlc) - This is error but do we care?
		o.mu.Unlock()
		return
	}

	// Update underlying store.
	o.updateAcks(dseq, sseq)

	mset := o.mset
	clustered := o.node != nil
	o.mu.Unlock()

	// Let the owning stream know if we are interest or workqueue retention based.
	// If this consumer is clustered this will be handled by processReplicatedAck
	// after the ack has propagated.
	if !clustered && mset != nil && mset.cfg.Retention != LimitsPolicy {
		if sagap > 1 {
			// FIXME(dlc) - This is very inefficient, will need to fix.
			for seq := sseq; seq > sseq-sagap; seq-- {
				mset.ackMsg(o, seq)
			}
		} else {
			mset.ackMsg(o, sseq)
		}
	}

	// If we had max ack pending set and were at limit we need to unblock folks.
	if needSignal {
		o.signalNewMessages()
	}
}

// Check if we need an ack for this store seq.
// This is called for interest based retention streams to remove messages.
func (o *consumer) needAck(sseq uint64) bool {
	var needAck bool
	var asflr, osseq uint64
	var pending map[uint64]*Pending
	o.mu.RLock()
	if o.isLeader() {
		asflr, osseq = o.asflr, o.sseq
		pending = o.pending
	} else {
		if o.store == nil {
			o.mu.RUnlock()
			return false
		}
		state, err := o.store.State()
		if err != nil || state == nil {
			// Fall back to what we track internally for now.
			needsAck := sseq > o.asflr && o.cfg.FilterSubject == _EMPTY_
			o.mu.RUnlock()
			return needsAck
		}
		asflr, osseq = state.AckFloor.Stream, o.sseq
		pending = state.Pending
	}
	switch o.cfg.AckPolicy {
	case AckNone, AckAll:
		needAck = sseq > asflr
	case AckExplicit:
		if sseq > asflr {
			// Generally this means we need an ack, but just double check pending acks.
			needAck = true
			if sseq < osseq {
				if len(pending) == 0 {
					needAck = false
				} else {
					_, needAck = pending[sseq]
				}
			}
		}
	}
	o.mu.RUnlock()
	return needAck
}

// Helper for the next message requests.
func nextReqFromMsg(msg []byte) (time.Time, int, bool, error) {
	req := bytes.TrimSpace(msg)

	switch {
	case len(req) == 0:
		return time.Time{}, 1, false, nil

	case req[0] == '{':
		var cr JSApiConsumerGetNextRequest
		if err := json.Unmarshal(req, &cr); err != nil {
			return time.Time{}, -1, false, err
		}
		if cr.Expires == time.Duration(0) {
			return time.Time{}, cr.Batch, cr.NoWait, nil
		}
		return time.Now().Add(cr.Expires), cr.Batch, cr.NoWait, nil
	default:
		if n, err := strconv.Atoi(string(req)); err == nil {
			return time.Time{}, n, false, nil
		}
	}

	return time.Time{}, 1, false, nil
}

// Represents a request that is on the internal waiting queue
type waitingRequest struct {
	client  *client
	reply   string
	n       int // For batching
	expires time.Time
	noWait  bool
}

// waiting queue for requests that are waiting for new messages to arrive.
type waitQueue struct {
	rp, wp int
	reqs   []*waitingRequest
}

// Create a new ring buffer with at most max items.
func newWaitQueue(max int) *waitQueue {
	return &waitQueue{rp: -1, reqs: make([]*waitingRequest, max)}
}

var (
	errWaitQueueFull = errors.New("wait queue is full")
	errWaitQueueNil  = errors.New("wait queue is nil")
)

// Adds in a new request.
func (wq *waitQueue) add(req *waitingRequest) error {
	if wq == nil {
		return errWaitQueueNil
	}
	if wq.isFull() {
		return errWaitQueueFull
	}
	wq.reqs[wq.wp] = req
	// TODO(dlc) - Could make pow2 and get rid of mod.
	wq.wp = (wq.wp + 1) % cap(wq.reqs)

	// Adjust read pointer if we were empty.
	if wq.rp < 0 {
		wq.rp = 0
	}

	return nil
}

func (wq *waitQueue) isFull() bool {
	return wq.rp == wq.wp
}

func (wq *waitQueue) len() int {
	if wq == nil || wq.rp < 0 {
		return 0
	}
	if wq.rp < wq.wp {
		return wq.wp - wq.rp
	}
	return cap(wq.reqs) - wq.rp + wq.wp
}

// Peek will return the next request waiting or nil if empty.
func (wq *waitQueue) peek() *waitingRequest {
	if wq == nil {
		return nil
	}
	var wr *waitingRequest
	if wq.rp >= 0 {
		wr = wq.reqs[wq.rp]
	}
	return wr
}

// pop will return the next request and move the read cursor.
func (wq *waitQueue) pop() *waitingRequest {
	wr := wq.peek()
	if wr != nil {
		wr.n--
		if wr.n <= 0 {
			wq.reqs[wq.rp] = nil
			wq.rp = (wq.rp + 1) % cap(wq.reqs)
			// Check if we are empty.
			if wq.rp == wq.wp {
				wq.rp, wq.wp = -1, 0
			}
		}
	}
	return wr
}

// processNextMsgReq will process a request for the next message available. A nil message payload means deliver
// a single message. If the payload is a formal request or a number parseable with Atoi(), then we will send a
// batch of messages without requiring another request to this endpoint, or an ACK.
func (o *consumer) processNextMsgReq(_ *subscription, c *client, _, reply string, msg []byte) {
	_, msg = c.msgParts(msg)

	o.mu.Lock()
	defer o.mu.Unlock()

	s, mset, js := o.srv, o.mset, o.js
	if mset == nil {
		return
	}

	sendErr := func(status int, description string) {
		hdr := []byte(fmt.Sprintf("NATS/1.0 %d %s\r\n\r\n", status, description))
		o.outq.send(&jsPubMsg{reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0, nil})
	}

	if o.isPushMode() {
		sendErr(409, "Consumer is push based")
		return
	}

	if o.waiting.isFull() {
		// Try to expire some of the requests.
		if expired := o.expireWaiting(); expired == 0 {
			// Force expiration if needed.
			o.forceExpireFirstWaiting()
		}
	}

	// Check payload here to see if they sent in batch size or a formal request.
	expires, batchSize, noWait, err := nextReqFromMsg(msg)
	if err != nil {
		sendErr(400, fmt.Sprintf("Bad Request - %v", err))
		return
	}

	if o.maxp > 0 && batchSize > o.maxp {
		sendErr(409, "Exceeded MaxAckPending")
		return
	}

	// In case we have to queue up this request.
	wr := waitingRequest{client: c, reply: reply, n: batchSize, noWait: noWait, expires: expires}

	// If we are in replay mode, defer to processReplay for delivery.
	if o.replay {
		o.waiting.add(&wr)
		o.signalNewMessages()
		return
	}

	sendBatch := func(wr *waitingRequest) {
		for i, batchSize := 0, wr.n; i < batchSize; i++ {
			// See if we have more messages available.
			if subj, hdr, msg, seq, dc, ts, err := o.getNextMsg(); err == nil {
				o.deliverMsg(reply, subj, hdr, msg, seq, dc, ts)
				// Need to discount this from the total n for the request.
				wr.n--
			} else {
				if wr.noWait {
					switch err {
					case errMaxAckPending:
						sendErr(409, "Exceeded MaxAckPending")
					default:
						sendErr(404, "No Messages")
					}
				} else {
					o.waiting.add(wr)
				}
				return
			}
		}
	}

	// If this is direct from a client can proceed inline.
	if c.kind == CLIENT {
		sendBatch(&wr)
	} else {
		// Check for API outstanding requests.
		if apiOut := atomic.AddInt64(&js.apiCalls, 1); apiOut > 1024 {
			atomic.AddInt64(&js.apiCalls, -1)
			o.mu.Unlock()
			sendErr(503, "JetStream API limit exceeded")
			s.Warnf("JetStream API limit exceeded: %d calls outstanding", apiOut)
			return
		}

		// Dispatch the API call to its own Go routine.
		go func() {
			o.mu.Lock()
			sendBatch(&wr)
			o.mu.Unlock()
			atomic.AddInt64(&js.apiCalls, -1)
		}()
	}
}

// Increase the delivery count for this message.
// ONLY used on redelivery semantics.
// Lock should be held.
func (o *consumer) incDeliveryCount(sseq uint64) uint64 {
	if o.rdc == nil {
		o.rdc = make(map[uint64]uint64)
	}
	o.rdc[sseq] += 1
	return o.rdc[sseq] + 1
}

// send a delivery exceeded advisory.
func (o *consumer) notifyDeliveryExceeded(sseq, dc uint64) {
	e := JSConsumerDeliveryExceededAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerDeliveryExceededAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:     o.stream,
		Consumer:   o.name,
		StreamSeq:  sseq,
		Deliveries: dc,
		Domain:     o.srv.getOpts().JetStreamDomain,
	}

	j, err := json.Marshal(e)
	if err != nil {
		return
	}

	o.sendAdvisory(o.deliveryExcEventT, j)
}

// Check to see if the candidate subject matches a filter if its present.
// Lock should be held.
func (o *consumer) isFilteredMatch(subj string) bool {
	// No filter is automatic match.
	if o.cfg.FilterSubject == _EMPTY_ {
		return true
	}
	if !o.filterWC {
		return subj == o.cfg.FilterSubject
	}
	// If we are here we have a wildcard filter subject.
	// TODO(dlc) at speed might be better to just do a sublist with L2 and/or possibly L1.
	return subjectIsSubsetMatch(subj, o.cfg.FilterSubject)
}

var (
	errMaxAckPending = errors.New("max ack pending reached")
	errBadConsumer   = errors.New("consumer not valid")
	errNoInterest    = errors.New("consumer requires interest for delivery subject when ephemeral")
)

// Get next available message from underlying store.
// Is partition aware and redeliver aware.
// Lock should be held.
func (o *consumer) getNextMsg() (subj string, hdr, msg []byte, seq uint64, dc uint64, ts int64, err error) {
	if o.mset == nil || o.mset.store == nil {
		return _EMPTY_, nil, nil, 0, 0, 0, errBadConsumer
	}
	for {
		seq, dc := o.sseq, uint64(1)
		if o.hasRedeliveries() {
			seq = o.getNextToRedeliver()
			dc = o.incDeliveryCount(seq)
			if o.maxdc > 0 && dc > o.maxdc {
				// Only send once
				if dc == o.maxdc+1 {
					o.notifyDeliveryExceeded(seq, dc-1)
				}
				// Make sure to remove from pending.
				delete(o.pending, seq)
				continue
			}
		} else if o.maxp > 0 && len(o.pending) >= o.maxp {
			// maxp only set when ack policy != AckNone and user set MaxAckPending
			// Stall if we have hit max pending.
			return _EMPTY_, nil, nil, 0, 0, 0, errMaxAckPending
		}

		subj, hdr, msg, ts, err := o.mset.store.LoadMsg(seq)
		if err == nil {
			if dc == 1 { // First delivery.
				o.sseq++
				if o.cfg.FilterSubject != _EMPTY_ && !o.isFilteredMatch(subj) {
					o.updateSkipped()
					continue
				}
			}
			// We have the msg here.
			return subj, hdr, msg, seq, dc, ts, nil
		}
		// We got an error here. If this is an EOF we will return, otherwise
		// we can continue looking.
		if err == ErrStoreEOF || err == ErrStoreClosed {
			return _EMPTY_, nil, nil, 0, 0, 0, err
		}
		// Skip since its probably deleted or expired.
		o.sseq++
	}
}

// forceExpireFirstWaiting will force expire the first waiting.
// Lock should be held.
func (o *consumer) forceExpireFirstWaiting() *waitingRequest {
	// FIXME(dlc) - Should we do advisory here as well?
	wr := o.waiting.pop()
	if wr == nil {
		return wr
	}
	// If we are expiring this and we think there is still interest, alert.
	if rr := o.acc.sl.Match(wr.reply); len(rr.psubs)+len(rr.qsubs) > 0 && o.mset != nil {
		// We still appear to have interest, so send alert as courtesy.
		hdr := []byte("NATS/1.0 408 Request Timeout\r\n\r\n")
		o.outq.send(&jsPubMsg{wr.reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0, nil})
	}
	return wr
}

// Will check for expiration and lack of interest on waiting requests.
func (o *consumer) expireWaiting() int {
	var expired int
	now := time.Now()
	for wr := o.waiting.peek(); wr != nil; wr = o.waiting.peek() {
		if !wr.expires.IsZero() && now.After(wr.expires) {
			o.forceExpireFirstWaiting()
			expired++
			continue
		}
		s, acc := o.acc.srv, o.acc
		rr := acc.sl.Match(wr.reply)
		if len(rr.psubs)+len(rr.qsubs) > 0 {
			break
		}
		// If we are here check on gateways.
		if s != nil && s.hasGatewayInterest(acc.Name, wr.reply) {
			break
		}
		// No more interest so go ahead and remove this one from our list.
		o.forceExpireFirstWaiting()
		expired++
	}
	return expired
}

// Will check to make sure those waiting still have registered interest.
func (o *consumer) checkWaitingForInterest() bool {
	o.expireWaiting()
	return o.waiting.len() > 0
}

// Lock should be held.
func (o *consumer) hbTimer() (time.Duration, *time.Timer) {
	if o.cfg.Heartbeat == 0 {
		return 0, nil
	}
	return o.cfg.Heartbeat, time.NewTimer(o.cfg.Heartbeat)
}

func (o *consumer) loopAndGatherMsgs(qch chan struct{}) {
	// On startup check to see if we are in a a reply situation where replay policy is not instant.
	var (
		lts  int64 // last time stamp seen, used for replay.
		lseq uint64
	)

	o.mu.Lock()
	s := o.srv
	if o.replay {
		// consumer is closed when mset is set to nil.
		if o.mset == nil {
			o.mu.Unlock()
			return
		}
		lseq = o.mset.state().LastSeq
	}
	// For idle heartbeat support.
	var hbc <-chan time.Time
	hbd, hb := o.hbTimer()
	if hb != nil {
		hbc = hb.C
	}
	// Interest changes.
	inch := o.inch
	o.mu.Unlock()

	// Deliver all the msgs we have now, once done or on a condition, we wait for new ones.
	for {
		var (
			seq, dc     uint64
			subj, dsubj string
			hdr         []byte
			msg         []byte
			err         error
			ts          int64
			delay       time.Duration
		)

		o.mu.Lock()
		// consumer is closed when mset is set to nil.
		if o.mset == nil {
			o.mu.Unlock()
			return
		}

		// If we are in push mode and not active or under flowcontrol let's stop sending.
		if o.isPushMode() {
			if !o.active {
				goto waitForMsgs
			}
			if o.maxpb > 0 && o.pbytes > o.maxpb {
				goto waitForMsgs
			}
		}

		// If we are in pull mode and no one is waiting already break and wait.
		if o.isPullMode() && !o.checkWaitingForInterest() {
			goto waitForMsgs
		}

		subj, hdr, msg, seq, dc, ts, err = o.getNextMsg()

		// On error either wait or return.
		if err != nil {
			if err == ErrStoreMsgNotFound || err == ErrStoreEOF || err == errMaxAckPending {
				goto waitForMsgs
			} else {
				o.mu.Unlock()
				s.Errorf("Received an error looking up message for consumer: %v", err)
				return
			}
		}

		if wr := o.waiting.pop(); wr != nil {
			dsubj = wr.reply
		} else {
			dsubj = o.dsubj
		}

		// If we are in a replay scenario and have not caught up check if we need to delay here.
		if o.replay && lts > 0 {
			if delay = time.Duration(ts - lts); delay > time.Millisecond {
				o.mu.Unlock()
				select {
				case <-qch:
					return
				case <-time.After(delay):
				}
				o.mu.Lock()
			}
		}

		// Track this regardless.
		lts = ts

		// If we have a rate limit set make sure we check that here.
		if o.rlimit != nil {
			now := time.Now()
			r := o.rlimit.ReserveN(now, len(msg)+len(hdr)+len(subj)+len(dsubj)+len(o.ackReplyT))
			delay := r.DelayFrom(now)
			if delay > 0 {
				o.mu.Unlock()
				select {
				case <-qch:
					return
				case <-time.After(delay):
				}
				o.mu.Lock()
			}
		}

		// Do actual delivery.
		o.deliverMsg(dsubj, subj, hdr, msg, seq, dc, ts)

		// Reset our idle heartbeat timer if set.
		if hb != nil {
			hb.Reset(hbd)
		}

		o.mu.Unlock()
		continue

	waitForMsgs:
		// If we were in a replay state check to see if we are caught up. If so clear.
		if o.replay && o.sseq > lseq {
			o.replay = false
		}

		// We will wait here for new messages to arrive.
		mch, outq, odsubj, sseq, dseq := o.mch, o.outq, o.cfg.DeliverSubject, o.sseq-1, o.dseq-1
		o.mu.Unlock()

		select {
		case interest := <-inch:
			// inch can be nil on pull-based, but then this will
			// just block and not fire.
			o.updateDeliveryInterest(interest)
		case <-qch:
			return
		case <-mch:
			// Messages are waiting.
		case <-hbc:
			if o.isActive() {
				const t = "NATS/1.0 100 Idle Heartbeat\r\n%s: %d\r\n%s: %d\r\n\r\n"
				hdr := []byte(fmt.Sprintf(t, JSLastConsumerSeq, dseq, JSLastStreamSeq, sseq))
				outq.send(&jsPubMsg{odsubj, _EMPTY_, _EMPTY_, hdr, nil, nil, 0, nil})
			}
			// Reset our idle heartbeat timer.
			hb.Reset(hbd)

			// Now check on flowcontrol if enabled. Make sure if we have any outstanding to resend.
			if o.fcOut() {
				o.sendFlowControl()
			}
		}
	}
}

func (o *consumer) ackReply(sseq, dseq, dc uint64, ts int64, pending uint64) string {
	return fmt.Sprintf(o.ackReplyT, dc, sseq, dseq, ts, pending)
}

// Used mostly for testing. Sets max pending bytes for flow control setups.
func (o *consumer) setMaxPendingBytes(limit int) {
	o.pblimit = limit
	o.maxpb = limit / 16
	if o.maxpb == 0 {
		o.maxpb = 1
	}
}

// Deliver a msg to the consumer.
// Lock should be held and o.mset validated to be non-nil.
func (o *consumer) deliverMsg(dsubj, subj string, hdr, msg []byte, seq, dc uint64, ts int64) {
	if o.mset == nil {
		return
	}
	// Update pending on first attempt
	if dc == 1 && o.sgap > 0 {
		o.sgap--
	}

	dseq := o.dseq
	o.dseq++

	pmsg := &jsPubMsg{dsubj, subj, o.ackReply(seq, dseq, dc, ts, o.sgap), hdr, msg, o, seq, nil}
	if o.maxpb > 0 {
		o.pbytes += pmsg.size()
	}

	mset := o.mset
	ap := o.cfg.AckPolicy

	// Send message.
	o.outq.send(pmsg)

	// If we are ack none and mset is interest only we should make sure stream removes interest.
	if ap == AckNone && mset.cfg.Retention != LimitsPolicy && mset.amch != nil {
		mset.amch <- seq
	}

	if ap == AckExplicit || ap == AckAll {
		o.trackPending(seq, dseq)
	} else if ap == AckNone {
		o.adflr = dseq
		o.asflr = seq
	}

	// Flow control.
	if o.maxpb > 0 && o.needFlowControl() {
		o.sendFlowControl()
	}

	// FIXME(dlc) - Capture errors?
	o.updateDelivered(dseq, seq, dc, ts)
}

func (o *consumer) needFlowControl() bool {
	if o.maxpb == 0 {
		return false
	}
	// Decide whether to send a flow control message which we will need the user to respond.
	// We send when we are over 50% of our current window limit.
	if o.fcid == _EMPTY_ && o.pbytes > o.maxpb/2 {
		return true
	}
	return false
}

func (o *consumer) processFlowControl(_ *subscription, c *client, subj, _ string, _ []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Ignore if not the latest we have sent out.
	if subj != o.fcid {
		return
	}

	// For slow starts and ramping up.
	if o.maxpb < o.pblimit {
		o.maxpb *= 2
		if o.maxpb > o.pblimit {
			o.maxpb = o.pblimit
		}
	}

	// Update accounting.
	o.pbytes -= o.fcsz
	o.fcid, o.fcsz = _EMPTY_, 0

	// In case they are sent out of order or we get duplicates etc.
	if o.pbytes < 0 {
		o.pbytes = 0
	}

	o.signalNewMessages()
}

// Lock should be held.
func (o *consumer) fcReply() string {
	var sb strings.Builder
	sb.WriteString(jsFlowControlPre)
	sb.WriteString(o.stream)
	sb.WriteByte(btsep)
	sb.WriteString(o.name)
	sb.WriteByte(btsep)
	var b [4]byte
	rn := rand.Int63()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	sb.Write(b[:])
	return sb.String()
}

func (o *consumer) fcOut() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.fcid != _EMPTY_
}

// sendFlowControl will send a flow control packet to the consumer.
// Lock should be held.
func (o *consumer) sendFlowControl() {
	if !o.isPushMode() {
		return
	}
	subj, rply := o.cfg.DeliverSubject, o.fcReply()
	o.fcsz, o.fcid = o.pbytes, rply
	hdr := []byte("NATS/1.0 100 FlowControl Request\r\n\r\n")
	o.outq.send(&jsPubMsg{subj, _EMPTY_, rply, hdr, nil, nil, 0, nil})
}

// Tracks our outstanding pending acks. Only applicable to AckExplicit mode.
// Lock should be held.
func (o *consumer) trackPending(sseq, dseq uint64) {
	if o.pending == nil {
		o.pending = make(map[uint64]*Pending)
	}
	if o.ptmr == nil {
		o.ptmr = time.AfterFunc(o.ackWait(0), o.checkPending)
	}
	if p, ok := o.pending[sseq]; ok {
		p.Timestamp = time.Now().UnixNano()
	} else {
		o.pending[sseq] = &Pending{dseq, time.Now().UnixNano()}
	}
}

// didNotDeliver is called when a delivery for a consumer message failed.
// Depending on our state, we will process the failure.
func (o *consumer) didNotDeliver(seq uint64) {
	o.mu.Lock()
	mset := o.mset
	if mset == nil {
		o.mu.Unlock()
		return
	}
	var checkDeliveryInterest bool
	if o.isPushMode() {
		o.active = false
		checkDeliveryInterest = true
	} else if o.pending != nil {
		// pull mode and we have pending.
		if _, ok := o.pending[seq]; ok {
			// We found this messsage on pending, we need
			// to queue it up for immediate redelivery since
			// we know it was not delivered.
			if !o.onRedeliverQueue(seq) {
				o.addToRedeliverQueue(seq)
				o.signalNewMessages()
			}
		}
	}
	o.mu.Unlock()

	// If we do not have interest update that here.
	if checkDeliveryInterest && o.hasNoLocalInterest() {
		o.updateDeliveryInterest(false)
	}
}

// Lock should be held.
func (o *consumer) addToRedeliverQueue(seqs ...uint64) {
	if o.rdqi == nil {
		o.rdqi = make(map[uint64]struct{})
	}
	o.rdq = append(o.rdq, seqs...)
	for _, seq := range seqs {
		o.rdqi[seq] = struct{}{}
	}
}

// Lock should be held.
func (o *consumer) hasRedeliveries() bool {
	return len(o.rdq) > 0
}

func (o *consumer) getNextToRedeliver() uint64 {
	if len(o.rdq) == 0 {
		return 0
	}
	seq := o.rdq[0]
	if len(o.rdq) == 1 {
		o.rdq, o.rdqi = nil, nil
	} else {
		o.rdq = append(o.rdq[:0], o.rdq[1:]...)
		delete(o.rdqi, seq)
	}
	return seq
}

// This checks if we already have this sequence queued for redelivery.
// FIXME(dlc) - This is O(n) but should be fast with small redeliver size.
// Lock should be held.
func (o *consumer) onRedeliverQueue(seq uint64) bool {
	if o.rdqi == nil {
		return false
	}
	_, ok := o.rdqi[seq]
	return ok
}

// Remove a sequence from the redelivery queue.
// Lock should be held.
func (o *consumer) removeFromRedeliverQueue(seq uint64) bool {
	if !o.onRedeliverQueue(seq) {
		return false
	}
	for i, rseq := range o.rdq {
		if rseq == seq {
			if len(o.rdq) == 1 {
				o.rdq, o.rdqi = nil, nil
			} else {
				o.rdq = append(o.rdq[:i], o.rdq[i+1:]...)
				delete(o.rdqi, seq)
			}
			return true
		}
	}
	return false
}

// Checks the pending messages.
func (o *consumer) checkPending() {
	o.mu.Lock()
	defer o.mu.Unlock()

	mset := o.mset
	if mset == nil {
		return
	}
	ttl := int64(o.cfg.AckWait)
	next := int64(o.ackWait(0))
	now := time.Now().UnixNano()

	// Since we can update timestamps, we have to review all pending.
	// We may want to unlock here or warn if list is big.
	var expired []uint64
	for seq, p := range o.pending {
		elapsed := now - p.Timestamp
		if elapsed >= ttl {
			if !o.onRedeliverQueue(seq) {
				expired = append(expired, seq)
				o.signalNewMessages()
			}
		} else if ttl-elapsed < next {
			// Update when we should fire next.
			next = ttl - elapsed
		}
	}

	if len(expired) > 0 {
		// We need to sort.
		sort.Slice(expired, func(i, j int) bool { return expired[i] < expired[j] })
		o.addToRedeliverQueue(expired...)
		// Now we should update the timestamp here since we are redelivering.
		// We will use an incrementing time to preserve order for any other redelivery.
		off := now - o.pending[expired[0]].Timestamp
		for _, seq := range expired {
			if p, ok := o.pending[seq]; ok {
				p.Timestamp += off
			}
		}
	}

	if len(o.pending) > 0 {
		o.ptmr.Reset(o.ackWait(time.Duration(next)))
	} else {
		o.ptmr.Stop()
		o.ptmr = nil
	}
}

// SeqFromReply will extract a sequence number from a reply subject.
func (o *consumer) seqFromReply(reply string) uint64 {
	_, dseq, _ := ackReplyInfo(reply)
	return dseq
}

// StreamSeqFromReply will extract the stream sequence from the reply subject.
func (o *consumer) streamSeqFromReply(reply string) uint64 {
	sseq, _, _ := ackReplyInfo(reply)
	return sseq
}

// Quick parser for positive numbers in ack reply encoding.
func parseAckReplyNum(d string) (n int64) {
	if len(d) == 0 {
		return -1
	}
	for _, dec := range d {
		if dec < asciiZero || dec > asciiNine {
			return -1
		}
		n = n*10 + (int64(dec) - asciiZero)
	}
	return n
}

const expectedNumReplyTokens = 9

// Grab encoded information in the reply subject for a delivered message.
func replyInfo(subject string) (sseq, dseq, dc uint64, ts int64, pending uint64) {
	tsa := [expectedNumReplyTokens]string{}
	start, tokens := 0, tsa[:0]
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	tokens = append(tokens, subject[start:])
	if len(tokens) != expectedNumReplyTokens || tokens[0] != "$JS" || tokens[1] != "ACK" {
		return 0, 0, 0, 0, 0
	}
	// TODO(dlc) - Should we error if we do not match consumer name?
	// stream is tokens[2], consumer is 3.
	dc = uint64(parseAckReplyNum(tokens[4]))
	sseq, dseq = uint64(parseAckReplyNum(tokens[5])), uint64(parseAckReplyNum(tokens[6]))
	ts = parseAckReplyNum(tokens[7])
	pending = uint64(parseAckReplyNum(tokens[8]))

	return sseq, dseq, dc, ts, pending
}

func ackReplyInfo(subject string) (sseq, dseq, dc uint64) {
	tsa := [expectedNumReplyTokens]string{}
	start, tokens := 0, tsa[:0]
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	tokens = append(tokens, subject[start:])
	if len(tokens) != expectedNumReplyTokens || tokens[0] != "$JS" || tokens[1] != "ACK" {
		return 0, 0, 0
	}
	dc = uint64(parseAckReplyNum(tokens[4]))
	sseq, dseq = uint64(parseAckReplyNum(tokens[5])), uint64(parseAckReplyNum(tokens[6]))

	return sseq, dseq, dc
}

// NextSeq returns the next delivered sequence number for this consumer.
func (o *consumer) nextSeq() uint64 {
	o.mu.Lock()
	dseq := o.dseq
	o.mu.Unlock()
	return dseq
}

// This will select the store seq to start with based on the
// partition subject.
func (o *consumer) selectSubjectLast() {
	stats := o.mset.store.State()
	if stats.LastSeq == 0 {
		o.sseq = stats.LastSeq
		return
	}
	// FIXME(dlc) - this is linear and can be optimized by store layer.
	for seq := stats.LastSeq; seq >= stats.FirstSeq; seq-- {
		subj, _, _, _, err := o.mset.store.LoadMsg(seq)
		if err == ErrStoreMsgNotFound {
			continue
		}
		if o.isFilteredMatch(subj) {
			o.sseq = seq
			o.updateSkipped()
			return
		}
	}
}

// Will select the starting sequence.
func (o *consumer) selectStartingSeqNo() {
	stats := o.mset.store.State()
	if o.cfg.OptStartSeq == 0 {
		if o.cfg.DeliverPolicy == DeliverAll {
			o.sseq = stats.FirstSeq
		} else if o.cfg.DeliverPolicy == DeliverLast {
			o.sseq = stats.LastSeq
			// If we are partitioned here we may need to walk backwards.
			if o.cfg.FilterSubject != _EMPTY_ {
				o.selectSubjectLast()
			}
		} else if o.cfg.OptStartTime != nil {
			// If we are here we are time based.
			// TODO(dlc) - Once clustered can't rely on this.
			o.sseq = o.mset.store.GetSeqFromTime(*o.cfg.OptStartTime)
		} else {
			// Default is deliver new only.
			o.sseq = stats.LastSeq + 1
		}
	} else {
		o.sseq = o.cfg.OptStartSeq
	}

	if stats.FirstSeq == 0 {
		o.sseq = 1
	} else if o.sseq < stats.FirstSeq {
		o.sseq = stats.FirstSeq
	} else if o.sseq > stats.LastSeq {
		o.sseq = stats.LastSeq + 1
	}
	// Always set delivery sequence to 1.
	o.dseq = 1
	// Set ack delivery floor to delivery-1
	o.adflr = o.dseq - 1
	// Set ack store floor to store-1
	o.asflr = o.sseq - 1
}

// Test whether a config represents a durable subscriber.
func isDurableConsumer(config *ConsumerConfig) bool {
	return config != nil && config.Durable != _EMPTY_
}

func (o *consumer) isDurable() bool {
	return o.cfg.Durable != _EMPTY_
}

// Are we in push mode, delivery subject, etc.
func (o *consumer) isPushMode() bool {
	return o.cfg.DeliverSubject != _EMPTY_
}

func (o *consumer) isPullMode() bool {
	return o.cfg.DeliverSubject == _EMPTY_
}

// Name returns the name of this consumer.
func (o *consumer) String() string {
	o.mu.RLock()
	n := o.name
	o.mu.RUnlock()
	return n
}

func createConsumerName() string {
	return string(getHash(nuid.Next()))
}

// deleteConsumer will delete the consumer from this stream.
func (mset *stream) deleteConsumer(o *consumer) error {
	return o.delete()
}

func (o *consumer) streamName() string {
	o.mu.RLock()
	mset := o.mset
	o.mu.RUnlock()
	if mset != nil {
		return mset.name()
	}
	return _EMPTY_
}

// Active indicates if this consumer is still active.
func (o *consumer) isActive() bool {
	o.mu.RLock()
	active := o.active && o.mset != nil
	o.mu.RUnlock()
	return active
}

// hasNoLocalInterest return true if we have no local interest.
func (o *consumer) hasNoLocalInterest() bool {
	o.mu.RLock()
	rr := o.acc.sl.Match(o.cfg.DeliverSubject)
	o.mu.RUnlock()
	return len(rr.psubs)+len(rr.qsubs) == 0
}

// This is when the underlying stream has been purged.
func (o *consumer) purge(sseq uint64) {
	o.mu.Lock()
	o.sseq = sseq
	o.asflr = sseq - 1
	o.adflr = o.dseq - 1
	o.sgap = 0
	if len(o.pending) > 0 {
		o.pending = nil
		if o.ptmr != nil {
			o.ptmr.Stop()
			// Do not nil this out here. This allows checkPending to fire
			// and still be ok and not panic.
		}
	}
	// We need to remove all those being queued for redelivery under o.rdq
	if len(o.rdq) > 0 {
		rdq := o.rdq
		o.rdq, o.rdqi = nil, nil
		for _, sseq := range rdq {
			if sseq >= o.sseq {
				o.addToRedeliverQueue(sseq)
			}
		}
	}
	o.mu.Unlock()

	o.writeStoreState()
}

func stopAndClearTimer(tp **time.Timer) {
	if *tp == nil {
		return
	}
	// Will get drained in normal course, do not try to
	// drain here.
	(*tp).Stop()
	*tp = nil
}

// Stop will shutdown  the consumer for the associated stream.
func (o *consumer) stop() error {
	return o.stopWithFlags(false, true, false)
}

func (o *consumer) deleteWithoutAdvisory() error {
	return o.stopWithFlags(true, true, false)
}

// Delete will delete the consumer for the associated stream and send advisories.
func (o *consumer) delete() error {
	return o.stopWithFlags(true, true, true)
}

func (o *consumer) stopWithFlags(dflag, doSignal, advisory bool) error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil
	}
	o.closed = true

	if dflag && advisory && o.isLeader() {
		o.sendDeleteAdvisoryLocked()
	}

	if o.qch != nil {
		close(o.qch)
		o.qch = nil
	}

	a := o.acc
	store := o.store
	mset := o.mset
	o.mset = nil
	o.active = false
	o.unsubscribe(o.ackSub)
	o.unsubscribe(o.reqSub)
	o.unsubscribe(o.fcSub)
	o.ackSub = nil
	o.reqSub = nil
	o.fcSub = nil
	if o.infoSub != nil {
		o.srv.sysUnsubscribe(o.infoSub)
		o.infoSub = nil
	}
	c := o.client
	o.client = nil
	sysc := o.sysc
	o.sysc = nil
	stopAndClearTimer(&o.ptmr)
	stopAndClearTimer(&o.dtmr)
	stopAndClearTimer(&o.gwdtmr)
	delivery := o.cfg.DeliverSubject
	o.waiting = nil
	// Break us out of the readLoop.
	if doSignal {
		o.signalNewMessages()
	}
	n := o.node
	o.mu.Unlock()

	if c != nil {
		c.closeConnection(ClientClosed)
	}
	if sysc != nil {
		sysc.closeConnection(ClientClosed)
	}

	if delivery != _EMPTY_ {
		a.sl.ClearNotification(delivery, o.inch)
	}

	mset.mu.Lock()
	mset.removeConsumer(o)
	rp := mset.cfg.Retention
	mset.mu.Unlock()

	// We need to optionally remove all messages since we are interest based retention.
	// We will do this consistently on all replicas. Note that if in clustered mode the
	// non-leader consumers will need to restore state first.
	if dflag && rp == InterestPolicy {
		stop := mset.lastSeq()

		o.mu.Lock()
		if !o.isLeader() {
			o.readStoredState()
		}
		start := o.asflr
		o.mu.Unlock()

		rmseqs := make([]uint64, 0, stop-start+1)

		mset.mu.RLock()
		for seq := start; seq <= stop; seq++ {
			if !mset.checkInterest(seq, o) {
				rmseqs = append(rmseqs, seq)
			}
		}
		mset.mu.RUnlock()

		for _, seq := range rmseqs {
			mset.store.RemoveMsg(seq)
		}
	}

	// Cluster cleanup.
	if n != nil {
		if dflag {
			n.Delete()
		} else {
			n.Stop()
		}
	}

	var err error
	if store != nil {
		if dflag {
			err = store.Delete()
		} else {
			err = store.Stop()
		}
	}

	return err
}

// Check that we do not form a cycle by delivering to a delivery subject
// that is part of the interest group.
func (mset *stream) deliveryFormsCycle(deliverySubject string) bool {
	mset.mu.RLock()
	defer mset.mu.RUnlock()

	for _, subject := range mset.cfg.Subjects {
		if subjectIsSubsetMatch(deliverySubject, subject) {
			return true
		}
	}
	return false
}

func validFilteredSubject(filteredSubject string, subjects []string) bool {
	for _, subject := range subjects {
		if subjectIsSubsetMatch(filteredSubject, subject) {
			return true
		}
	}
	return false
}

// SetInActiveDeleteThreshold sets the delete threshold for how long to wait
// before deleting an inactive ephemeral consumer.
func (o *consumer) setInActiveDeleteThreshold(dthresh time.Duration) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.isPullMode() {
		return fmt.Errorf("consumer is not push-based")
	}
	if o.isDurable() {
		return fmt.Errorf("consumer is not durable")
	}
	deleteWasRunning := o.dtmr != nil
	stopAndClearTimer(&o.dtmr)
	o.dthresh = dthresh
	if deleteWasRunning {
		o.dtmr = time.AfterFunc(o.dthresh, func() { o.deleteNotActive() })
	}
	return nil
}

// switchToEphemeral is called on startup when recovering ephemerals.
func (o *consumer) switchToEphemeral() {
	o.mu.Lock()
	o.cfg.Durable = _EMPTY_
	store, ok := o.store.(*consumerFileStore)
	rr := o.acc.sl.Match(o.cfg.DeliverSubject)
	o.mu.Unlock()

	// Update interest
	o.updateDeliveryInterest(len(rr.psubs)+len(rr.qsubs) > 0)
	// Write out new config
	if ok {
		store.updateConfig(o.cfg)
	}
}

// RequestNextMsgSubject returns the subject to request the next message when in pull or worker mode.
// Returns empty otherwise.
func (o *consumer) requestNextMsgSubject() string {
	return o.nextMsgSubj
}

// Will set the initial pending.
// mset lock should be held.
func (o *consumer) setInitialPending() {
	mset := o.mset
	if mset == nil {
		return
	}
	// notFiltered means we want all messages.
	notFiltered := o.cfg.FilterSubject == _EMPTY_
	if !notFiltered {
		// Check to see if we directly match the configured stream.
		// Many clients will always send a filtered subject.
		cfg := mset.cfg
		if len(cfg.Subjects) == 1 && cfg.Subjects[0] == o.cfg.FilterSubject {
			notFiltered = true
		}
	}

	if notFiltered {
		state := mset.store.State()
		if state.Msgs > 0 {
			o.sgap = state.Msgs - (o.sseq - state.FirstSeq)
		}
	} else {
		// Here we are filtered.
		o.sgap = o.mset.store.NumFilteredPending(o.sseq, o.cfg.FilterSubject)
	}
}

func (o *consumer) decStreamPending(sseq uint64, subj string) {
	o.mu.Lock()
	// Ignore if we have already seen this one.
	if sseq >= o.sseq && o.sgap > 0 && o.isFilteredMatch(subj) {
		o.sgap--
	}
	// Check if this message was pending.
	p, wasPending := o.pending[sseq]
	var rdc uint64 = 1
	if o.rdc != nil {
		rdc = o.rdc[sseq]
	}
	o.mu.Unlock()

	// If it was pending process it like an ack.
	// TODO(dlc) - we could do a term here instead with a reason to generate the advisory.
	if wasPending {
		o.processAckMsg(sseq, p.Sequence, rdc, false)
	}
}

func (o *consumer) account() *Account {
	o.mu.RLock()
	a := o.acc
	o.mu.RUnlock()
	return a
}
