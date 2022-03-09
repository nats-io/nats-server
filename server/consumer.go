// Copyright 2019-2022 The NATS Authors
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
	"time"

	"github.com/nats-io/nuid"
	"golang.org/x/time/rate"
)

type ConsumerInfo struct {
	Stream         string          `json:"stream_name"`
	Name           string          `json:"name"`
	Created        time.Time       `json:"created"`
	Config         *ConsumerConfig `json:"config,omitempty"`
	Delivered      SequenceInfo    `json:"delivered"`
	AckFloor       SequenceInfo    `json:"ack_floor"`
	NumAckPending  int             `json:"num_ack_pending"`
	NumRedelivered int             `json:"num_redelivered"`
	NumWaiting     int             `json:"num_waiting"`
	NumPending     uint64          `json:"num_pending"`
	Cluster        *ClusterInfo    `json:"cluster,omitempty"`
	PushBound      bool            `json:"push_bound,omitempty"`
}

type ConsumerConfig struct {
	Durable         string          `json:"durable_name,omitempty"`
	Description     string          `json:"description,omitempty"`
	DeliverPolicy   DeliverPolicy   `json:"deliver_policy"`
	OptStartSeq     uint64          `json:"opt_start_seq,omitempty"`
	OptStartTime    *time.Time      `json:"opt_start_time,omitempty"`
	AckPolicy       AckPolicy       `json:"ack_policy"`
	AckWait         time.Duration   `json:"ack_wait,omitempty"`
	MaxDeliver      int             `json:"max_deliver,omitempty"`
	BackOff         []time.Duration `json:"backoff,omitempty"`
	FilterSubject   string          `json:"filter_subject,omitempty"`
	ReplayPolicy    ReplayPolicy    `json:"replay_policy"`
	RateLimit       uint64          `json:"rate_limit_bps,omitempty"` // Bits per sec
	SampleFrequency string          `json:"sample_freq,omitempty"`
	MaxWaiting      int             `json:"max_waiting,omitempty"`
	MaxAckPending   int             `json:"max_ack_pending,omitempty"`
	Heartbeat       time.Duration   `json:"idle_heartbeat,omitempty"`
	FlowControl     bool            `json:"flow_control,omitempty"`
	HeadersOnly     bool            `json:"headers_only,omitempty"`

	// Pull based options.
	MaxRequestBatch   int           `json:"max_batch,omitempty"`
	MaxRequestExpires time.Duration `json:"max_expires,omitempty"`

	// Push based consumers.
	DeliverSubject string `json:"deliver_subject,omitempty"`
	DeliverGroup   string `json:"deliver_group,omitempty"`

	// Ephemeral inactivity threshold.
	InactiveThreshold time.Duration `json:"inactive_threshold,omitempty"`

	// Don't add to general clients.
	Direct bool `json:"direct,omitempty"`
}

// SequenceInfo has both the consumer and the stream sequence and last activity.
type SequenceInfo struct {
	Consumer uint64     `json:"consumer_seq"`
	Stream   uint64     `json:"stream_seq"`
	Last     *time.Time `json:"last_active,omitempty"`
}

type CreateConsumerRequest struct {
	Stream string         `json:"stream_name"`
	Config ConsumerConfig `json:"config"`
}

// ConsumerNakOptions is for optional NAK values, e.g. delay.
type ConsumerNakOptions struct {
	Delay time.Duration `json:"delay"`
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
	// DeliverByStartTime will select the first messsage with a timestamp >= to StartTime.
	DeliverByStartTime
	// DeliverLastPerSubject will start the consumer with the last message for all subjects received.
	DeliverLastPerSubject
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
	case DeliverLastPerSubject:
		return "last_per_subject"
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
	lsgap             uint64
	dsubj             string
	qgroup            string
	lss               *lastSeqSkipList
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
	ici               *ConsumerInfo
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
	ldt               time.Time
	lat               time.Time
	closed            bool

	// Clustered.
	ca      *consumerAssignment
	node    RaftNode
	infoSub *subscription
	lqsent  time.Time
	prm     map[string]struct{}
	prOk    bool

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

// Helper function to set consumer config defaults from above.
func setConsumerConfigDefaults(config *ConsumerConfig) {
	// Set to default if not specified.
	if config.DeliverSubject == _EMPTY_ && config.MaxWaiting == 0 {
		config.MaxWaiting = JSWaitQueueDefaultMax
	}
	// Setup proper default for ack wait if we are in explicit ack mode.
	if config.AckWait == 0 && (config.AckPolicy == AckExplicit || config.AckPolicy == AckAll) {
		config.AckWait = JsAckWaitDefault
	}
	// Setup default of -1, meaning no limit for MaxDeliver.
	if config.MaxDeliver == 0 {
		config.MaxDeliver = -1
	}
	// If BackOff was specified that will override the AckWait and the MaxDeliver.
	if len(config.BackOff) > 0 {
		config.AckWait = config.BackOff[0]
	}
	// Set proper default for max ack pending if we are ack explicit and none has been set.
	if (config.AckPolicy == AckExplicit || config.AckPolicy == AckAll) && config.MaxAckPending == 0 {
		config.MaxAckPending = JsDefaultMaxAckPending
	}
}

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
		return nil, NewJSConsumerConfigRequiredError()
	}

	// Make sure we have sane defaults.
	setConsumerConfigDefaults(config)

	// Check if we have a BackOff defined that MaxDeliver is within range etc.
	if lbo := len(config.BackOff); lbo > 0 && config.MaxDeliver <= lbo {
		return nil, NewJSConsumerMaxDeliverBackoffError()
	}

	if len(config.Description) > JSMaxDescriptionLen {
		return nil, NewJSConsumerDescriptionTooLongError(JSMaxDescriptionLen)
	}

	var err error
	// For now expect a literal subject if its not empty. Empty means work queue mode (pull mode).
	if config.DeliverSubject != _EMPTY_ {
		if !subjectIsLiteral(config.DeliverSubject) {
			return nil, NewJSConsumerDeliverToWildcardsError()
		}
		if !IsValidSubject(config.DeliverSubject) {
			return nil, NewJSConsumerInvalidDeliverSubjectError()
		}
		if mset.deliveryFormsCycle(config.DeliverSubject) {
			return nil, NewJSConsumerDeliverCycleError()
		}
		if config.MaxWaiting != 0 {
			return nil, NewJSConsumerPushMaxWaitingError()
		}
		if config.MaxAckPending > 0 && config.AckPolicy == AckNone {
			return nil, NewJSConsumerMaxPendingAckPolicyRequiredError()
		}
		if config.Heartbeat > 0 && config.Heartbeat < 100*time.Millisecond {
			return nil, NewJSConsumerSmallHeartbeatError()
		}
	} else {
		// Pull mode / work queue mode require explicit ack.
		if config.AckPolicy == AckNone {
			return nil, NewJSConsumerPullRequiresAckError()
		}
		if config.RateLimit > 0 {
			return nil, NewJSConsumerPullWithRateLimitError()
		}
		if config.MaxWaiting < 0 {
			return nil, NewJSConsumerMaxWaitingNegativeError()
		}
		if config.Heartbeat > 0 {
			return nil, NewJSConsumerHBRequiresPushError()
		}
		if config.FlowControl {
			return nil, NewJSConsumerFCRequiresPushError()
		}
		if config.MaxRequestBatch < 0 {
			return nil, NewJSConsumerMaxRequestBatchNegativeError()
		}
		if config.MaxRequestExpires != 0 && config.MaxRequestExpires < time.Millisecond {
			return nil, NewJSConsumerMaxRequestExpiresToSmallError()
		}
	}

	// Direct need to be non-mapped ephemerals.
	if config.Direct {
		if config.DeliverSubject == _EMPTY_ {
			return nil, NewJSConsumerDirectRequiresPushError()
		}
		if isDurableConsumer(config) {
			return nil, NewJSConsumerDirectRequiresEphemeralError()
		}
		if ca != nil {
			return nil, NewJSConsumerOnMappedError()
		}
	}

	// As best we can make sure the filtered subject is valid.
	if config.FilterSubject != _EMPTY_ {
		subjects, hasExt := mset.allSubjects()
		if !validFilteredSubject(config.FilterSubject, subjects) && !hasExt {
			return nil, NewJSConsumerFilterNotSubsetError()
		}
	}

	// Helper function to formulate similar errors.
	badStart := func(dp, start string) error {
		return fmt.Errorf("consumer delivery policy is deliver %s, but optional start %s is also set", dp, start)
	}
	notSet := func(dp, notSet string) error {
		return fmt.Errorf("consumer delivery policy is deliver %s, but optional %s is not set", dp, notSet)
	}

	// Check on start position conflicts.
	switch config.DeliverPolicy {
	case DeliverAll:
		if config.OptStartSeq > 0 {
			return nil, NewJSConsumerInvalidPolicyError(badStart("all", "sequence"))
		}
		if config.OptStartTime != nil {
			return nil, NewJSConsumerInvalidPolicyError(badStart("all", "time"))
		}
	case DeliverLast:
		if config.OptStartSeq > 0 {
			return nil, NewJSConsumerInvalidPolicyError(badStart("last", "sequence"))
		}
		if config.OptStartTime != nil {
			return nil, NewJSConsumerInvalidPolicyError(badStart("last", "time"))
		}
	case DeliverLastPerSubject:
		if config.OptStartSeq > 0 {
			return nil, NewJSConsumerInvalidPolicyError(badStart("last per subject", "sequence"))
		}
		if config.OptStartTime != nil {
			return nil, NewJSConsumerInvalidPolicyError(badStart("last per subject", "time"))
		}
		if config.FilterSubject == _EMPTY_ {
			return nil, NewJSConsumerInvalidPolicyError(notSet("last per subject", "filter subject"))
		}
	case DeliverNew:
		if config.OptStartSeq > 0 {
			return nil, NewJSConsumerInvalidPolicyError(badStart("new", "sequence"))
		}
		if config.OptStartTime != nil {
			return nil, NewJSConsumerInvalidPolicyError(badStart("new", "time"))
		}
	case DeliverByStartSequence:
		if config.OptStartSeq == 0 {
			return nil, NewJSConsumerInvalidPolicyError(notSet("by start sequence", "start sequence"))
		}
		if config.OptStartTime != nil {
			return nil, NewJSConsumerInvalidPolicyError(badStart("by start sequence", "time"))
		}
	case DeliverByStartTime:
		if config.OptStartTime == nil {
			return nil, NewJSConsumerInvalidPolicyError(notSet("by start time", "start time"))
		}
		if config.OptStartSeq != 0 {
			return nil, NewJSConsumerInvalidPolicyError(badStart("by start time", "start sequence"))
		}
	}

	sampleFreq := 0
	if config.SampleFrequency != _EMPTY_ {
		s := strings.TrimSuffix(config.SampleFrequency, "%")
		sampleFreq, err = strconv.Atoi(s)
		if err != nil {
			return nil, NewJSConsumerInvalidSamplingError(err)
		}
	}

	// Grab the client, account and server reference.
	c := mset.client
	if c == nil {
		return nil, NewJSStreamInvalidError()
	}
	c.mu.Lock()
	s, a := c.srv, c.acc
	c.mu.Unlock()

	// Hold mset lock here.
	mset.mu.Lock()
	if mset.client == nil || mset.store == nil {
		mset.mu.Unlock()
		return nil, errors.New("invalid stream")
	}

	// If this one is durable and already exists, we let that be ok as long as only updating what should be allowed.
	if isDurableConsumer(config) {
		if eo, ok := mset.consumers[config.Durable]; ok {
			mset.mu.Unlock()
			err := eo.updateConfig(config)
			if err == nil {
				return eo, nil
			}
			return nil, NewJSConsumerCreateError(err, Unless(err))
		}
	}

	// Check for any limits, if the config for the consumer sets a limit we check against that
	// but if not we use the value from account limits, if account limits is more restrictive
	// than stream config we prefer the account limits to handle cases where account limits are
	// updated during the lifecycle of the stream
	maxc := mset.cfg.MaxConsumers
	if maxc <= 0 || (mset.jsa.limits.MaxConsumers > 0 && mset.jsa.limits.MaxConsumers < maxc) {
		maxc = mset.jsa.limits.MaxConsumers
	}
	if maxc > 0 && mset.numPublicConsumers() >= maxc {
		mset.mu.Unlock()
		return nil, NewJSMaximumConsumersLimitError()
	}

	// Check on stream type conflicts with WorkQueues.
	if mset.cfg.Retention == WorkQueuePolicy && !config.Direct {
		// Force explicit acks here.
		if config.AckPolicy != AckExplicit {
			mset.mu.Unlock()
			return nil, NewJSConsumerWQRequiresExplicitAckError()
		}

		if len(mset.consumers) > 0 {
			if config.FilterSubject == _EMPTY_ {
				mset.mu.Unlock()
				return nil, NewJSConsumerWQMultipleUnfilteredError()
			} else if !mset.partitionUnique(config.FilterSubject) {
				// We have a partition but it is not unique amongst the others.
				mset.mu.Unlock()
				return nil, NewJSConsumerWQConsumerNotUniqueError()
			}
		}
		if config.DeliverPolicy != DeliverAll {
			mset.mu.Unlock()
			return nil, NewJSConsumerWQConsumerNotDeliverAllError()
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
			o.deleteWithoutAdvisory()
			return nil, NewJSConsumerNameTooLongError(JSMaxNameLen)
		}
		o.name = config.Durable
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
	// Create our request waiting queue.
	if o.isPullMode() {
		o.waiting = newWaitQueue(config.MaxWaiting)
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
		return nil, NewJSConsumerBadDurableNameError()
	}

	// Select starting sequence number
	o.selectStartingSeqNo()

	if !config.Direct {
		store, err := mset.store.ConsumerStore(o.name, config)
		if err != nil {
			mset.mu.Unlock()
			o.deleteWithoutAdvisory()
			return nil, NewJSConsumerStoreFailedError(err)
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
			return nil, NewJSConsumerNameExistError()
		}
		// If we are here we have already registered this durable. If it is still active that is an error.
		if eo.isActive() {
			o.name = _EMPTY_ // Prevent removal since same name.
			o.deleteWithoutAdvisory()
			return nil, NewJSConsumerExistingActiveError()
		}
		// Since we are here this means we have a potentially new durable so we should update here.
		// Check that configs are the same.
		if !configsEqualSansDelivery(o.cfg, eo.cfg) {
			o.name = _EMPTY_ // Prevent removal since same name.
			o.deleteWithoutAdvisory()
			return nil, NewJSConsumerReplacementWithDifferentNameError()
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

	// If not durable determine the inactive threshold.
	if !o.isDurable() {
		if o.cfg.InactiveThreshold != 0 {
			o.dthresh = o.cfg.InactiveThreshold
		} else {
			// Add in 1 sec of jitter above and beyond the default of 5s.
			o.dthresh = JsDeleteWaitTimeDefault + time.Duration(rand.Int63n(1000))*time.Millisecond
		}
	}

	if o.isPushMode() {
		if !o.isDurable() {
			// Check if we are not durable that the delivery subject has interest.
			// Check in place here for interest. Will setup properly in setLeader.
			r := o.acc.sl.Match(o.cfg.DeliverSubject)
			if !o.hasDeliveryInterest(len(r.psubs)+len(r.qsubs) > 0) {
				// Let the interest come to us eventually, but setup delete timer.
				o.updateDeliveryInterest(false)
			}
		}
	}

	// Set our ca.
	if ca != nil {
		o.setConsumerAssignment(ca)
	}

	// Check if we have a rate limit set.
	if config.RateLimit != 0 {
		o.setRateLimit(config.RateLimit)
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

// checkQueueInterest will check on our interest's queue group status.
// Lock should be held.
func (o *consumer) checkQueueInterest() {
	if !o.active || o.cfg.DeliverSubject == _EMPTY_ {
		return
	}
	subj := o.dsubj
	if subj == _EMPTY_ {
		subj = o.cfg.DeliverSubject
	}

	if rr := o.acc.sl.Match(subj); len(rr.qsubs) > 0 {
		// Just grab first
		if qsubs := rr.qsubs[0]; len(qsubs) > 0 {
			if sub := rr.qsubs[0][0]; len(sub.queue) > 0 {
				o.qgroup = string(sub.queue)
			}
		}
	}
}

// clears our node if we have one. When we scale down to 1.
func (o *consumer) clearNode() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.node != nil {
		o.node.Delete()
		o.node = nil
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

		// Setup initial pending and proper start sequence.
		o.setInitialPendingAndStart()

		// If push mode, register for notifications on interest.
		if o.isPushMode() {
			o.inch = make(chan bool, 8)
			o.acc.sl.registerNotification(o.cfg.DeliverSubject, o.cfg.DeliverGroup, o.inch)
			if o.active = <-o.inch; o.active {
				o.checkQueueInterest()
			}
			// Check gateways in case they are enabled.
			if s.gateway.enabled {
				if !o.active {
					o.active = s.hasGatewayInterest(o.acc.Name, o.cfg.DeliverSubject)
				}
				stopAndClearTimer(&o.gwdtmr)
				o.gwdtmr = time.AfterFunc(time.Second, func() { o.watchGWinterest() })
			}
		} else if !o.isDurable() {
			// Ephemeral pull consumer. We run the dtmr all the time for this one.
			if o.dtmr != nil {
				stopAndClearTimer(&o.dtmr)
			}
			o.dtmr = time.AfterFunc(o.dthresh, func() { o.deleteNotActive() })
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

		// Snapshot initial info.
		o.infoWithSnap(true)

		// Now start up Go routine to deliver msgs.
		go o.loopAndGatherMsgs(qch)

		// If we are R>1 spin up our proposal loop.
		if node != nil {
			// Determine if we can send pending requests info to the group.
			// They must be on server versions >= 2.7.1
			o.checkAndSetPendingRequestsOk()
			o.checkPendingRequests()
			go o.loopAndForwardProposals(qch)
		}

	} else {
		// Shutdown the go routines and the subscriptions.
		o.mu.Lock()
		// ok if they are nil, we protect inside unsubscribe()
		o.unsubscribe(o.ackSub)
		o.unsubscribe(o.reqSub)
		o.unsubscribe(o.fcSub)
		o.ackSub, o.reqSub, o.fcSub = nil, nil, nil
		if o.infoSub != nil {
			o.srv.sysUnsubscribe(o.infoSub)
			o.infoSub = nil
		}
		if o.qch != nil {
			close(o.qch)
			o.qch = nil
		}
		// Reset waiting if we are in pull mode.
		if o.isPullMode() {
			o.waiting = newWaitQueue(o.cfg.MaxWaiting)
		}
		o.mu.Unlock()
	}
}

func (o *consumer) handleClusterConsumerInfoRequest(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
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
	if !c.srv.EventsEnabled() {
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
	o.outq.sendMsg(subj, msg)
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
	// Update active status, if not active clear any queue group we captured.
	if o.active = interest; !o.active {
		o.qgroup = _EMPTY_
	} else {
		o.checkQueueInterest()
	}

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
	o.mu.Lock()
	if o.mset == nil {
		o.mu.Unlock()
		return
	}
	// Push mode just look at active.
	if o.isPushMode() {
		// If we are active simply return.
		if o.active {
			o.mu.Unlock()
			return
		}
	} else {
		// These need to keep firing so reset first.
		if o.dtmr != nil {
			o.dtmr.Reset(o.dthresh)
		}
		// Check if we have had a request lately, or if we still have valid requests waiting.
		if time.Since(o.waiting.last) <= o.dthresh || o.checkWaitingForInterest() {
			o.mu.Unlock()
			return
		}
	}

	s, js := o.mset.srv, o.mset.srv.js
	acc, stream, name, isDirect := o.acc.Name, o.stream, o.name, o.cfg.Direct
	o.mu.Unlock()

	// If we are clustered, check if we still have this consumer assigned.
	// If we do forward a proposal to delete ourselves to the metacontroller leader.
	if !isDirect && s.JetStreamIsClustered() {
		js.mu.RLock()
		ca, cc := js.consumerAssignment(acc, stream, name), js.cluster
		js.mu.RUnlock()

		if ca != nil && cc != nil {
			cca := *ca
			cca.Reply = _EMPTY_
			meta, removeEntry := cc.meta, encodeDeleteConsumerAssignment(&cca)
			meta.ForwardProposal(removeEntry)

			// Check to make sure we went away.
			// Don't think this needs to be a monitored go routine.
			go func() {
				var fs bool
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()
				for range ticker.C {
					js.mu.RLock()
					ca := js.consumerAssignment(acc, stream, name)
					js.mu.RUnlock()
					if ca != nil {
						if fs {
							s.Warnf("Consumer assignment not cleaned up, retrying")
							meta.ForwardProposal(removeEntry)
						}
						fs = true
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

// Acquire proper locks and update rate limit.
// Will use what is in config.
func (o *consumer) setRateLimitNeedsLocks() {
	o.mu.RLock()
	mset := o.mset
	o.mu.RUnlock()

	if mset == nil {
		return
	}

	mset.mu.RLock()
	o.mu.Lock()
	o.setRateLimit(o.cfg.RateLimit)
	o.mu.Unlock()
	mset.mu.RUnlock()
}

// Set the rate limiter
// Both mset and consumer lock should be held.
func (o *consumer) setRateLimit(bps uint64) {
	if bps == 0 {
		o.rlimit = nil
		return
	}

	// TODO(dlc) - Make sane values or error if not sane?
	// We are configured in bits per sec so adjust to bytes.
	rl := rate.Limit(bps / 8)
	mset := o.mset

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

// Check if new consumer config allowed vs old.
func (acc *Account) checkNewConsumerConfig(cfg, ncfg *ConsumerConfig) error {
	if reflect.DeepEqual(cfg, ncfg) {
		return nil
	}
	// Something different, so check since we only allow certain things to be updated.
	if cfg.FilterSubject != ncfg.FilterSubject {
		return errors.New("filter subject can not be updated")
	}
	if cfg.DeliverPolicy != ncfg.DeliverPolicy {
		return errors.New("deliver policy can not be updated")
	}
	if cfg.OptStartSeq != ncfg.OptStartSeq {
		return errors.New("start sequence can not be updated")
	}
	if cfg.OptStartTime != ncfg.OptStartTime {
		return errors.New("start time can not be updated")
	}
	if cfg.AckPolicy != ncfg.AckPolicy {
		return errors.New("ack policy can not be updated")
	}
	if cfg.ReplayPolicy != ncfg.ReplayPolicy {
		return errors.New("replay policy can not be updated")
	}
	if cfg.Heartbeat != ncfg.Heartbeat {
		return errors.New("heart beats can not be updated")
	}
	if cfg.FlowControl != ncfg.FlowControl {
		return errors.New("flow control can not be updated")
	}

	// Deliver Subject is conditional on if its bound.
	if cfg.DeliverSubject != ncfg.DeliverSubject {
		if cfg.DeliverSubject == _EMPTY_ {
			return errors.New("can not update pull consumer to push based")
		}
		if ncfg.DeliverSubject == _EMPTY_ {
			return errors.New("can not update push consumer to pull based")
		}
		rr := acc.sl.Match(cfg.DeliverSubject)
		if len(rr.psubs)+len(rr.qsubs) != 0 {
			return NewJSConsumerNameExistError()
		}
	}

	return nil
}

// Update the config based on the new config, or error if update not allowed.
func (o *consumer) updateConfig(cfg *ConsumerConfig) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if err := o.acc.checkNewConsumerConfig(&o.cfg, cfg); err != nil {
		return err
	}

	if o.store != nil {
		// Update local state always.
		if err := o.store.UpdateConfig(cfg); err != nil {
			return err
		}
	}

	// DeliverSubject
	if cfg.DeliverSubject != o.cfg.DeliverSubject {
		o.updateDeliverSubjectLocked(cfg.DeliverSubject)
	}

	// MaxAckPending
	if cfg.MaxAckPending != o.cfg.MaxAckPending {
		o.maxp = cfg.MaxAckPending
		o.signalNewMessages()
	}
	// AckWait
	if cfg.AckWait != o.cfg.AckWait {
		if o.ptmr != nil {
			o.ptmr.Reset(100 * time.Millisecond)
		}
	}
	// Rate Limit
	if cfg.RateLimit != o.cfg.RateLimit {
		// We need both locks here so do in Go routine.
		go o.setRateLimitNeedsLocks()
	}

	// Record new config for others that do not need special handling.
	// Allowed but considered no-op, [Description, MaxDeliver, SampleFrequency, MaxWaiting, HeadersOnly]
	o.cfg = *cfg

	return nil
}

// This is a config change for the delivery subject for a
// push based consumer.
func (o *consumer) updateDeliverSubject(newDeliver string) {
	// Update the config and the dsubj
	o.mu.Lock()
	defer o.mu.Unlock()
	o.updateDeliverSubjectLocked(newDeliver)
}

// This is a config change for the delivery subject for a
// push based consumer.
func (o *consumer) updateDeliverSubjectLocked(newDeliver string) {
	if o.closed || o.isPullMode() || o.cfg.DeliverSubject == newDeliver {
		return
	}

	// Force redeliver of all pending on change of delivery subject.
	if len(o.pending) > 0 {
		o.forceExpirePending()
	}

	o.acc.sl.clearNotification(o.dsubj, o.cfg.DeliverGroup, o.inch)
	o.dsubj, o.cfg.DeliverSubject = newDeliver, newDeliver
	// When we register new one it will deliver to update state loop.
	o.acc.sl.registerNotification(newDeliver, o.cfg.DeliverGroup, o.inch)
}

// Check that configs are equal but allow delivery subjects to be different.
func configsEqualSansDelivery(a, b ConsumerConfig) bool {
	// These were copied in so can set Delivery here.
	a.DeliverSubject, b.DeliverSubject = _EMPTY_, _EMPTY_
	return reflect.DeepEqual(a, b)
}

// Helper to send a reply to an ack.
func (o *consumer) sendAckReply(subj string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.sendAdvisory(subj, nil)
}

// Process a message for the ack reply subject delivered with a message.
func (o *consumer) processAck(_ *subscription, c *client, acc *Account, subject, reply string, rmsg []byte) {
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
		o.processNextMsgReq(nil, c, acc, subject, reply, msg[len(AckNext):])
		c.pa.hdr = phdr
		skipAckReply = true
	case bytes.HasPrefix(msg, AckNak):
		o.processNak(sseq, dseq, dc, msg)
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
				// We need to re-craete `entries` because there is a reference
				// to it in the node's pae map.
				sz, entries = 0, nil
			}
		}
		if len(entries) > 0 {
			node.ProposeDirect(entries)
		}
	}

	// In case we have anything pending on entry.
	forwardProposals()

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
	// Update activity.
	o.ldt = time.Now()
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
	// Update activity.
	o.lat = time.Now()
}

// Communicate to the cluster an addition of a pending request.
// Lock should be held.
func (o *consumer) addClusterPendingRequest(reply string) {
	if o.node == nil || !o.pendingRequestsOk() {
		return
	}
	b := make([]byte, len(reply)+1)
	b[0] = byte(addPendingRequest)
	copy(b[1:], reply)
	o.propose(b)
}

// Communicate to the cluster a removal of a pending request.
// Lock should be held.
func (o *consumer) removeClusterPendingRequest(reply string) {
	if o.node == nil || !o.pendingRequestsOk() {
		return
	}
	b := make([]byte, len(reply)+1)
	b[0] = byte(removePendingRequest)
	copy(b[1:], reply)
	o.propose(b)
}

// Set whether or not we can send pending requests to followers.
func (o *consumer) setPendingRequestsOk(ok bool) {
	o.mu.Lock()
	o.prOk = ok
	o.mu.Unlock()
}

// Lock should be held.
func (o *consumer) pendingRequestsOk() bool {
	return o.prOk
}

// Set whether or not we can send info about pending pull requests to our group.
// Will require all peers have a minimum version.
func (o *consumer) checkAndSetPendingRequestsOk() {
	o.mu.RLock()
	s, isValid := o.srv, o.mset != nil
	o.mu.RUnlock()
	if !isValid {
		return
	}

	if ca := o.consumerAssignment(); ca != nil && len(ca.Group.Peers) > 1 {
		for _, pn := range ca.Group.Peers {
			if si, ok := s.nodeToInfo.Load(pn); ok {
				if !versionAtLeast(si.(nodeInfo).version, 2, 7, 1) {
					// We expect all of our peers to eventually be up to date.
					// So check again in awhile.
					time.AfterFunc(eventsHBInterval, func() { o.checkAndSetPendingRequestsOk() })
					o.setPendingRequestsOk(false)
					return
				}
			}
		}
	}
	o.setPendingRequestsOk(true)
}

// On leadership change make sure we alert the pending requests that they are no longer valid.
func (o *consumer) checkPendingRequests() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.mset == nil || o.outq == nil {
		return
	}
	hdr := []byte("NATS/1.0 409 Leadership Change\r\n\r\n")
	for reply := range o.prm {
		o.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
	}
	o.prm = nil
}

// Process a NAK.
func (o *consumer) processNak(sseq, dseq, dc uint64, nak []byte) {
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
	// Check to see if we have delays attached.
	if len(nak) > len(AckNak) {
		arg := bytes.TrimSpace(nak[len(AckNak):])
		if len(arg) > 0 {
			var d time.Duration
			var err error
			if arg[0] == '{' {
				var nd ConsumerNakOptions
				if err = json.Unmarshal(arg, &nd); err == nil {
					d = nd.Delay
				}
			} else {
				d, err = time.ParseDuration(string(arg))
			}
			if err != nil {
				// Treat this as normal NAK.
				o.srv.Warnf("JetStream consumer '%s > %s > %s' bad NAK delay value: %q", o.acc.Name, o.stream, o.name, arg)
			} else {
				// We have a parsed duration that the user wants us to wait before retrying.
				// Make sure we are not on the rdq.
				o.removeFromRedeliverQueue(sseq)
				if p, ok := o.pending[sseq]; ok {
					// now - ackWait is expired now, so offset from there.
					p.Timestamp = time.Now().Add(-o.cfg.AckWait).Add(d).UnixNano()
					// Update store system which will update followers as well.
					o.updateDelivered(p.Sequence, sseq, dc, p.Timestamp)
					if o.ptmr != nil {
						// Want checkPending to run and figure out the next timer ttl.
						// TODO(dlc) - We could optimize this maybe a bit more and track when we expect the timer to fire.
						o.ptmr.Reset(10 * time.Millisecond)
					}
				}
				// Nothing else for use to do now so return.
				return
			}
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

// Due to bug in calculation of sequences on restoring redelivered let's do quick sanity check.
func (o *consumer) checkRedelivered() {
	var lseq uint64
	if mset := o.mset; mset != nil {
		lseq = mset.lastSeq()
	}
	var shouldUpdateState bool
	for sseq := range o.rdc {
		if sseq < o.asflr || sseq > lseq {
			delete(o.rdc, sseq)
			o.removeFromRedeliverQueue(sseq)
			shouldUpdateState = true
		}
	}
	if shouldUpdateState {
		o.writeStoreStateUnlocked()
	}
}

// This will restore the state from disk.
// Lock should be held.
func (o *consumer) readStoredState() error {
	if o.store == nil {
		return nil
	}
	state, err := o.store.State()
	if err == nil && state != nil && state.Delivered.Consumer != 0 {
		o.applyState(state)
		if len(o.rdc) > 0 {
			o.checkRedelivered()
		}
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
		// This is on startup or leader change. We want to check pending
		// sooner in case there are inconsistencies etc. Pick between 500ms - 1.5s
		delay := 500*time.Millisecond + time.Duration(rand.Int63n(1000))*time.Millisecond
		// If normal is lower than this just use that.
		if o.cfg.AckWait < delay {
			delay = o.ackWait(0)
		}
		o.ptmr = time.AfterFunc(delay, o.checkPending)
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
	return o.writeStoreStateUnlocked()
}

// Update our state to the store.
// Lock should be held.
func (o *consumer) writeStoreStateUnlocked() error {
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

// Returns an initial info. Only applicable for non-clustered consumers.
// We will clear after we return it, so one shot.
func (o *consumer) initialInfo() *ConsumerInfo {
	o.mu.Lock()
	ici := o.ici
	o.ici = nil // gc friendly
	o.mu.Unlock()
	if ici == nil {
		ici = o.info()
	}
	return ici
}

// Clears our initial info.
// Used when we have a leader change in cluster mode but do not send a response.
func (o *consumer) clearInitialInfo() {
	o.mu.Lock()
	o.ici = nil // gc friendly
	o.mu.Unlock()
}

// Info returns our current consumer state.
func (o *consumer) info() *ConsumerInfo {
	return o.infoWithSnap(false)
}

func (o *consumer) infoWithSnap(snap bool) *ConsumerInfo {
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

	o.mu.Lock()
	defer o.mu.Unlock()

	cfg := o.cfg
	info := &ConsumerInfo{
		Stream:  o.stream,
		Name:    o.name,
		Created: o.created,
		Config:  &cfg,
		Delivered: SequenceInfo{
			Consumer: o.dseq - 1,
			Stream:   o.sseq - 1,
		},
		AckFloor: SequenceInfo{
			Consumer: o.adflr,
			Stream:   o.asflr,
		},
		NumAckPending:  len(o.pending),
		NumRedelivered: len(o.rdc),
		NumPending:     o.adjustedPending(),
		PushBound:      o.isPushMode() && o.active,
		Cluster:        ci,
	}
	// Adjust active based on non-zero etc. Also make UTC here.
	if !o.ldt.IsZero() {
		ldt := o.ldt.UTC() // This copies as well.
		info.Delivered.Last = &ldt
	}
	if !o.lat.IsZero() {
		lat := o.lat.UTC() // This copies as well.
		info.AckFloor.Last = &lat
	}

	// If we are a pull mode consumer, report on number of waiting requests.
	if o.isPullMode() {
		o.processWaiting()
		info.NumWaiting = o.waiting.len()
	}
	// If we were asked to snapshot do so here.
	if snap {
		o.ici = info
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

// Determine if this is a truly filtered consumer. Modern clients will place filtered subjects
// even if the stream only has a single non-wildcard subject designation.
// Read lock should be held.
func (o *consumer) isFiltered() bool {
	if o.cfg.FilterSubject == _EMPTY_ {
		return false
	}
	// If we are here we want to check if the filtered subject is
	// a direct match for our only listed subject.
	mset := o.mset
	if mset == nil {
		return true
	}
	if len(mset.cfg.Subjects) > 1 {
		return true
	}
	return o.cfg.FilterSubject != mset.cfg.Subjects[0]
}

// Check if we need an ack for this store seq.
// This is called for interest based retention streams to remove messages.
func (o *consumer) needAck(sseq uint64) bool {
	var needAck bool
	var asflr, osseq uint64
	var pending map[uint64]*Pending
	o.mu.RLock()

	// Check first if we are filtered, and if so check if this is even applicable to us.
	if o.isFiltered() && o.mset != nil {
		subj, _, _, _, err := o.mset.store.LoadMsg(sseq)
		if err != nil || !o.isFilteredMatch(subj) {
			o.mu.RUnlock()
			return false
		}
	}

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
			needAck := sseq > o.asflr && !o.isFiltered()
			o.mu.RUnlock()
			return needAck
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
func nextReqFromMsg(msg []byte) (time.Time, int, bool, time.Duration, time.Time, error) {
	req := bytes.TrimSpace(msg)

	switch {
	case len(req) == 0:
		return time.Time{}, 1, false, 0, time.Time{}, nil

	case req[0] == '{':
		var cr JSApiConsumerGetNextRequest
		if err := json.Unmarshal(req, &cr); err != nil {
			return time.Time{}, -1, false, 0, time.Time{}, err
		}
		var hbt time.Time
		if cr.Heartbeat > 0 {
			if cr.Heartbeat*2 > cr.Expires {
				return time.Time{}, 1, false, 0, time.Time{}, errors.New("heartbeat value too large")
			}
			hbt = time.Now().Add(cr.Heartbeat)
		}
		if cr.Expires == time.Duration(0) {
			return time.Time{}, cr.Batch, cr.NoWait, cr.Heartbeat, hbt, nil
		}
		return time.Now().Add(cr.Expires), cr.Batch, cr.NoWait, cr.Heartbeat, hbt, nil
	default:
		if n, err := strconv.Atoi(string(req)); err == nil {
			return time.Time{}, n, false, 0, time.Time{}, nil
		}
	}

	return time.Time{}, 1, false, 0, time.Time{}, nil
}

// Represents a request that is on the internal waiting queue
type waitingRequest struct {
	acc      *Account
	interest string
	reply    string
	n        int // For batching
	d        int
	expires  time.Time
	received time.Time
	hb       time.Duration
	hbt      time.Time
	noWait   bool
}

// sync.Pool for waiting requests.
var wrPool = sync.Pool{
	New: func() interface{} {
		return new(waitingRequest)
	},
}

// Recycle this request. This request can not be accessed after this call.
func (wr *waitingRequest) recycleIfDone() bool {
	if wr != nil && wr.n <= 0 {
		wr.recycle()
		return true
	}
	return false
}

// Force a recycle.
func (wr *waitingRequest) recycle() {
	if wr != nil {
		wr.acc, wr.interest, wr.reply = nil, _EMPTY_, _EMPTY_
		wrPool.Put(wr)
	}
}

// waiting queue for requests that are waiting for new messages to arrive.
type waitQueue struct {
	rp, wp int
	last   time.Time
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
func (wq *waitQueue) add(wr *waitingRequest) error {
	if wq == nil {
		return errWaitQueueNil
	}
	if wq.isFull() {
		return errWaitQueueFull
	}
	wq.reqs[wq.wp] = wr
	// TODO(dlc) - Could make pow2 and get rid of mod.
	wq.wp = (wq.wp + 1) % cap(wq.reqs)

	// Adjust read pointer if we were empty.
	if wq.rp < 0 {
		wq.rp = 0
	}
	// Track last active via when we receive a request.
	wq.last = wr.received
	return nil
}

func (wq *waitQueue) isFull() bool {
	return wq.rp == wq.wp
}

func (wq *waitQueue) isEmpty() bool {
	return wq.len() == 0
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
		wr.d++
		wr.n--
		if wr.n <= 0 {
			wq.removeCurrent()
		}
	}
	return wr
}

// Removes the current read pointer (head FIFO) entry.
func (wq *waitQueue) removeCurrent() {
	if wq.rp < 0 {
		return
	}
	wq.reqs[wq.rp] = nil
	wq.rp = (wq.rp + 1) % cap(wq.reqs)
	// Check if we are empty.
	if wq.rp == wq.wp {
		wq.rp, wq.wp = -1, 0
	}
}

// Will compact when we have interior deletes.
func (wq *waitQueue) compact() {
	if wq.isEmpty() {
		return
	}
	nreqs, i := make([]*waitingRequest, cap(wq.reqs)), 0
	for rp := wq.rp; rp != wq.wp; rp = (rp + 1) % cap(wq.reqs) {
		if wr := wq.reqs[rp]; wr != nil {
			nreqs[i] = wr
			i++
		}
	}
	// Reset here.
	wq.rp, wq.wp, wq.reqs = 0, i, nreqs
}

// Return the replies for our pending requests.
// No-op if push consumer or invalid etc.
func (o *consumer) pendingRequestReplies() []string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.waiting == nil {
		return nil
	}
	wq, m := o.waiting, make(map[string]struct{})
	for rp := o.waiting.rp; o.waiting.rp >= 0 && rp != wq.wp; rp = (rp + 1) % cap(wq.reqs) {
		if wr := wq.reqs[rp]; wr != nil {
			m[wr.reply] = struct{}{}
		}
	}
	var replies []string
	for reply := range m {
		replies = append(replies, reply)
	}
	return replies
}

// Return next waiting request. This will check for expirations but not noWait or interest.
// That will be handled by processWaiting.
// Lock should be held.
func (o *consumer) nextWaiting() *waitingRequest {
	if o.waiting == nil || o.waiting.isEmpty() {
		return nil
	}
	for wr := o.waiting.peek(); !o.waiting.isEmpty(); wr = o.waiting.peek() {
		if wr == nil || wr.expires.IsZero() || time.Now().Before(wr.expires) {
			rr := wr.acc.sl.Match(wr.interest)
			if len(rr.psubs)+len(rr.qsubs) > 0 {
				return o.waiting.pop()
			} else if o.srv.gateway.enabled {
				if o.srv.hasGatewayInterest(wr.acc.Name, wr.interest) || time.Since(wr.received) < defaultGatewayRecentSubExpiration {
					return o.waiting.pop()
				}
			}
		}
		hdr := []byte("NATS/1.0 408 Request Timeout\r\n\r\n")
		o.outq.send(newJSPubMsg(wr.reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		// Remove the current one, no longer valid.
		o.waiting.removeCurrent()
		if o.node != nil {
			o.removeClusterPendingRequest(wr.reply)
		}
		wr.recycle()
	}
	return nil
}

// processNextMsgReq will process a request for the next message available. A nil message payload means deliver
// a single message. If the payload is a formal request or a number parseable with Atoi(), then we will send a
// batch of messages without requiring another request to this endpoint, or an ACK.
func (o *consumer) processNextMsgReq(_ *subscription, c *client, _ *Account, _, reply string, msg []byte) {
	if reply == _EMPTY_ {
		return
	}
	_, msg = c.msgParts(msg)

	o.mu.Lock()
	defer o.mu.Unlock()

	mset := o.mset
	if mset == nil {
		return
	}

	sendErr := func(status int, description string) {
		hdr := []byte(fmt.Sprintf("NATS/1.0 %d %s\r\n\r\n", status, description))
		o.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
	}

	if o.isPushMode() || o.waiting == nil {
		sendErr(409, "Consumer is push based")
		return
	}

	// Check payload here to see if they sent in batch size or a formal request.
	expires, batchSize, noWait, hb, hbt, err := nextReqFromMsg(msg)
	if err != nil {
		sendErr(400, fmt.Sprintf("Bad Request - %v", err))
		return
	}

	// Check for request limits
	if o.cfg.MaxRequestBatch > 0 && batchSize > o.cfg.MaxRequestBatch {
		sendErr(409, fmt.Sprintf("Exceeded MaxRequestBatch of %d", o.cfg.MaxRequestBatch))
		return
	}

	if !expires.IsZero() && o.cfg.MaxRequestExpires > 0 && expires.After(time.Now().Add(o.cfg.MaxRequestExpires)) {
		sendErr(409, fmt.Sprintf("Exceeded MaxRequestExpires of %v", o.cfg.MaxRequestExpires))
		return
	}

	// If we have the max number of requests already pending try to expire.
	if o.waiting.isFull() {
		// Try to expire some of the requests.
		if expired, _, _, _ := o.processWaiting(); expired == 0 {
			// Force expiration if needed.
			o.forceExpireFirstWaiting()
		}
	}

	// If the request is for noWait and we have pending requests already, check if we have room.
	if noWait {
		msgsPending := o.adjustedPending() + uint64(len(o.rdq))
		// If no pending at all, decide what to do with request.
		// If no expires was set then fail.
		if msgsPending == 0 && expires.IsZero() {
			sendErr(404, "No Messages")
			return
		}
		if msgsPending > 0 {
			_, _, batchPending, _ := o.processWaiting()
			if msgsPending < uint64(batchPending) {
				sendErr(408, "Requests Pending")
				return
			}
		}
		// If we are here this should be considered a one-shot situation.
		// We will wait for expires but will return as soon as we have any messages.
	}

	// If we receive this request though an account export, we need to track that interest subject and account.
	acc, interest := o.acc, reply
	for strings.HasPrefix(interest, replyPrefix) && acc.exports.responses != nil {
		if si := acc.exports.responses[interest]; si != nil {
			acc, interest = si.acc, si.to
		} else {
			break
		}
	}

	// In case we have to queue up this request.
	wr := wrPool.Get().(*waitingRequest)
	wr.acc, wr.interest, wr.reply, wr.n, wr.d, wr.noWait, wr.expires, wr.hb, wr.hbt = acc, interest, reply, batchSize, 0, noWait, expires, hb, hbt
	wr.received = time.Now()

	if err := o.waiting.add(wr); err != nil {
		sendErr(409, "Exceeded MaxWaiting")
		return
	}
	o.signalNewMessages()
	// If we are clustered update our followers about this request.
	if o.node != nil {
		o.addClusterPendingRequest(wr.reply)
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
func (o *consumer) getNextMsg() (subj string, hdr, msg []byte, sseq uint64, dc uint64, ts int64, err error) {
	if o.mset == nil || o.mset.store == nil {
		return _EMPTY_, nil, nil, 0, 0, 0, errBadConsumer
	}
	seq, dc := o.sseq, uint64(1)
	if o.hasSkipListPending() {
		seq = o.lss.seqs[0]
		if len(o.lss.seqs) == 1 {
			o.sseq = o.lss.resume
			o.lss = nil
			o.updateSkipped()
		} else {
			o.lss.seqs = o.lss.seqs[1:]
		}
	} else if o.hasRedeliveries() {
		for seq = o.getNextToRedeliver(); seq > 0; seq = o.getNextToRedeliver() {
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
			if seq > 0 {
				subj, hdr, msg, ts, err = o.mset.store.LoadMsg(seq)
				return subj, hdr, msg, seq, dc, ts, err
			}
		}
		// Fallback if all redeliveries are gone.
		seq, dc = o.sseq, 1
	}

	// Check if we have max pending.
	if o.maxp > 0 && len(o.pending) >= o.maxp {
		// maxp only set when ack policy != AckNone and user set MaxAckPending
		// Stall if we have hit max pending.
		return _EMPTY_, nil, nil, 0, 0, 0, errMaxAckPending
	}

	// Grab next message applicable to us.
	subj, sseq, hdr, msg, ts, err = o.mset.store.LoadNextMsg(o.cfg.FilterSubject, o.filterWC, seq)

	if sseq >= o.sseq {
		o.sseq = sseq + 1
		if err == ErrStoreEOF {
			o.updateSkipped()
		}
	}

	return subj, hdr, msg, sseq, dc, ts, err
}

// forceExpireFirstWaiting will force expire the first waiting.
// Lock should be held.
func (o *consumer) forceExpireFirstWaiting() {
	// FIXME(dlc) - Should we do advisory here as well?
	wr := o.waiting.peek()
	if wr == nil {
		return
	}
	// If we are expiring this and we think there is still interest, alert.
	if rr := wr.acc.sl.Match(wr.interest); len(rr.psubs)+len(rr.qsubs) > 0 && o.mset != nil {
		// We still appear to have interest, so send alert as courtesy.
		hdr := []byte("NATS/1.0 408 Request Canceled\r\n\r\n")
		o.outq.send(newJSPubMsg(wr.reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
	}
	o.waiting.removeCurrent()
	if o.node != nil {
		o.removeClusterPendingRequest(wr.reply)
	}
	wr.recycle()
}

// Will check for expiration and lack of interest on waiting requests.
// Will also do any heartbeats and return the next expiration or HB interval.
func (o *consumer) processWaiting() (int, int, int, time.Time) {
	var fexp time.Time
	if o.srv == nil || o.waiting.isEmpty() {
		return 0, 0, 0, fexp
	}

	var expired, brp int
	s, now := o.srv, time.Now()

	// Signals interior deletes, which we will compact if needed.
	var hid bool
	remove := func(wr *waitingRequest, i int) {
		if i == o.waiting.rp {
			o.waiting.removeCurrent()
		} else {
			o.waiting.reqs[i] = nil
			hid = true
		}
		if o.node != nil {
			o.removeClusterPendingRequest(wr.reply)
		}
		expired++
		wr.recycle()
	}

	wq := o.waiting

	for rp := o.waiting.rp; o.waiting.rp >= 0 && rp != wq.wp; rp = (rp + 1) % cap(wq.reqs) {
		wr := wq.reqs[rp]
		// Check expiration.
		if (wr.noWait && wr.d > 0) || (!wr.expires.IsZero() && now.After(wr.expires)) {
			hdr := []byte("NATS/1.0 408 Request Timeout\r\n\r\n")
			o.outq.send(newJSPubMsg(wr.reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
			remove(wr, rp)
			continue
		}
		// Now check interest.
		rr := wr.acc.sl.Match(wr.interest)
		interest := len(rr.psubs)+len(rr.qsubs) > 0
		if !interest && s.gateway.enabled {
			// If we are here check on gateways.
			// If we have interest or the request is too young break and do not expire.
			if s.hasGatewayInterest(wr.acc.Name, wr.interest) || time.Since(wr.received) < defaultGatewayRecentSubExpiration {
				interest = true
			}
		}
		// If interest, update batch pending requests counter and update fexp timer.
		if interest {
			brp += wr.n
			if !wr.hbt.IsZero() {
				if now.After(wr.hbt) {
					// Fire off a heartbeat here.
					o.sendIdleHeartbeat(wr.reply)
					// Update next HB.
					wr.hbt = now.Add(wr.hb)
				}
				if fexp.IsZero() || wr.hbt.Before(fexp) {
					fexp = wr.hbt
				}
			}
			if !wr.expires.IsZero() && (fexp.IsZero() || wr.expires.Before(fexp)) {
				fexp = wr.expires
			}
			continue
		}
		// No more interest here so go ahead and remove this one from our list.
		remove(wr, rp)
	}

	// If we have interior deletes from out of order invalidation, compact the waiting queue.
	if hid {
		o.waiting.compact()
	}

	return expired, o.waiting.len(), brp, fexp
}

// Will check to make sure those waiting still have registered interest.
func (o *consumer) checkWaitingForInterest() bool {
	o.processWaiting()
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
			if !o.active || (o.maxpb > 0 && o.pbytes > o.maxpb) {
				goto waitForMsgs
			}
		} else if o.waiting.isEmpty() {
			// If we are in pull mode and no one is waiting already break and wait.
			goto waitForMsgs
		}

		subj, hdr, msg, seq, dc, ts, err = o.getNextMsg()

		// On error either wait or return.
		if err != nil {
			if err == ErrStoreMsgNotFound || err == ErrStoreEOF || err == errMaxAckPending || err == errPartialCache {
				goto waitForMsgs
			} else {
				s.Errorf("Received an error looking up message for consumer: %v", err)
				goto waitForMsgs
			}
		}

		if o.isPushMode() {
			dsubj = o.dsubj
		} else if wr := o.nextWaiting(); wr != nil {
			dsubj = wr.reply
			if done := wr.recycleIfDone(); done && o.node != nil {
				o.removeClusterPendingRequest(dsubj)
			} else if !done && wr.hb > 0 {
				wr.hbt = time.Now().Add(wr.hb)
			}
		} else {
			// We will redo this one.
			o.sseq--
			goto waitForMsgs
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

		// Make sure to process any expired requests that are pending.
		var wrExp <-chan time.Time
		if o.isPullMode() {
			_, _, _, fexp := o.processWaiting()
			if !fexp.IsZero() {
				expires := time.Until(fexp)
				if expires <= 0 {
					expires = time.Millisecond
				}
				wrExp = time.NewTimer(expires).C
			}
		}

		// We will wait here for new messages to arrive.
		mch, odsubj := o.mch, o.cfg.DeliverSubject
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
		case <-wrExp:
			o.mu.Lock()
			o.processWaiting()
			o.mu.Unlock()
		case <-hbc:
			if o.isActive() {
				o.mu.RLock()
				o.sendIdleHeartbeat(odsubj)
				o.mu.RUnlock()
			}
			// Reset our idle heartbeat timer.
			hb.Reset(hbd)
		}
	}
}

// Lock should be held.
func (o *consumer) sendIdleHeartbeat(subj string) {
	const t = "NATS/1.0 100 Idle Heartbeat\r\n%s: %d\r\n%s: %d\r\n\r\n"
	sseq, dseq := o.sseq-1, o.dseq-1
	hdr := []byte(fmt.Sprintf(t, JSLastConsumerSeq, dseq, JSLastStreamSeq, sseq))
	if fcp := o.fcid; fcp != _EMPTY_ {
		// Add in that we are stalled on flow control here.
		addOn := []byte(fmt.Sprintf("%s: %s\r\n\r\n", JSConsumerStalled, fcp))
		hdr = append(hdr[:len(hdr)-LEN_CR_LF], []byte(addOn)...)
	}
	o.outq.send(newJSPubMsg(subj, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
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

// We have the case where a consumer can become greedy and pick up a messages before the stream has incremented our pending(sgap).
// Instead of trying to slow things down and synchronize we will allow this to wrap and go negative (biggest uint64) for a short time.
// This functions checks for that and returns 0.
// Lock should be held.
func (o *consumer) adjustedPending() uint64 {
	if o.sgap&(1<<63) != 0 {
		return 0
	}
	return o.sgap
}

// Deliver a msg to the consumer.
// Lock should be held and o.mset validated to be non-nil.
func (o *consumer) deliverMsg(dsubj, subj string, hdr, msg []byte, seq, dc uint64, ts int64) {
	if o.mset == nil {
		return
	}
	// Update pending on first attempt. This can go upside down for a short bit, that is ok.
	// See adjustedPending().
	if dc == 1 {
		o.sgap--
	}

	dseq := o.dseq
	o.dseq++

	// If headers only do not send msg payload.
	// Add in msg size itself as header.
	if o.cfg.HeadersOnly {
		var bb bytes.Buffer
		if len(hdr) == 0 {
			bb.WriteString(hdrLine)
		} else {
			bb.Write(hdr)
			bb.Truncate(len(hdr) - LEN_CR_LF)
		}
		bb.WriteString(JSMsgSize)
		bb.WriteString(": ")
		bb.WriteString(strconv.FormatInt(int64(len(msg)), 10))
		bb.WriteString(CR_LF)
		bb.WriteString(CR_LF)
		hdr = bb.Bytes()
		// Cancel msg payload
		msg = nil
	}

	pmsg := newJSPubMsg(dsubj, subj, o.ackReply(seq, dseq, dc, ts, o.adjustedPending()), hdr, msg, o, seq)
	psz := pmsg.size()

	if o.maxpb > 0 {
		o.pbytes += psz
	}

	mset := o.mset
	ap := o.cfg.AckPolicy

	// Send message.
	o.outq.send(pmsg)

	if ap == AckExplicit || ap == AckAll {
		o.trackPending(seq, dseq)
	} else if ap == AckNone {
		o.adflr = dseq
		o.asflr = seq
	}

	// Flow control.
	if o.maxpb > 0 && o.needFlowControl(psz) {
		o.sendFlowControl()
	}

	// FIXME(dlc) - Capture errors?
	o.updateDelivered(dseq, seq, dc, ts)

	// If we are ack none and mset is interest only we should make sure stream removes interest.
	if ap == AckNone && mset.cfg.Retention != LimitsPolicy {
		if o.node == nil || o.cfg.Direct {
			mset.ackq.push(seq)
		} else {
			o.updateAcks(dseq, seq)
		}
	}
}

func (o *consumer) needFlowControl(sz int) bool {
	if o.maxpb == 0 {
		return false
	}
	// Decide whether to send a flow control message which we will need the user to respond.
	// We send when we are over 50% of our current window limit.
	if o.fcid == _EMPTY_ && o.pbytes > o.maxpb/2 {
		return true
	}
	// If we have an existing outstanding FC, check to see if we need to expand the o.fcsz
	if o.fcid != _EMPTY_ && (o.pbytes-o.fcsz) >= o.maxpb {
		o.fcsz += sz
	}
	return false
}

func (o *consumer) processFlowControl(_ *subscription, c *client, _ *Account, subj, _ string, _ []byte) {
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
	if o.pbytes < 0 {
		o.pbytes = 0
	}
	o.fcid, o.fcsz = _EMPTY_, 0

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

// sendFlowControl will send a flow control packet to the consumer.
// Lock should be held.
func (o *consumer) sendFlowControl() {
	if !o.isPushMode() {
		return
	}
	subj, rply := o.cfg.DeliverSubject, o.fcReply()
	o.fcsz, o.fcid = o.pbytes, rply
	hdr := []byte("NATS/1.0 100 FlowControl Request\r\n\r\n")
	o.outq.send(newJSPubMsg(subj, _EMPTY_, rply, hdr, nil, nil, 0))
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

	now := time.Now().UnixNano()
	ttl := int64(o.cfg.AckWait)
	next := int64(o.ackWait(0))

	var shouldUpdateState bool
	var state StreamState
	mset.store.FastState(&state)
	fseq := state.FirstSeq

	// Since we can update timestamps, we have to review all pending.
	// We may want to unlock here or warn if list is big.
	var expired []uint64
	for seq, p := range o.pending {
		// Check if these are no longer valid.
		if seq < fseq {
			delete(o.pending, seq)
			delete(o.rdc, seq)
			o.removeFromRedeliverQueue(seq)
			shouldUpdateState = true
			continue
		}
		elapsed, deadline := now-p.Timestamp, ttl
		if len(o.cfg.BackOff) > 0 && o.rdc != nil {
			dc := int(o.rdc[seq])
			if dc >= len(o.cfg.BackOff) {
				dc = len(o.cfg.BackOff) - 1
			}
			deadline = int64(o.cfg.BackOff[dc])
		}
		if elapsed >= deadline {
			if !o.onRedeliverQueue(seq) {
				expired = append(expired, seq)
			}
		} else if deadline-elapsed < next {
			// Update when we should fire next.
			next = deadline - elapsed
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
		o.signalNewMessages()
	}

	if len(o.pending) > 0 {
		o.ptmr.Reset(o.ackWait(time.Duration(next)))
	} else {
		o.ptmr.Stop()
		o.ptmr = nil
	}

	// Update our state if needed.
	if shouldUpdateState {
		o.writeStoreStateUnlocked()
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
	o.mu.RLock()
	dseq := o.dseq
	o.mu.RUnlock()
	return dseq
}

// Used to hold skip list when deliver policy is last per subject.
type lastSeqSkipList struct {
	resume uint64
	seqs   []uint64
}

// Will create a skip list for us from a store's subjects state.
func createLastSeqSkipList(mss map[string]SimpleState) []uint64 {
	seqs := make([]uint64, 0, len(mss))
	for _, ss := range mss {
		seqs = append(seqs, ss.Last)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs
}

// Let's us know we have a skip list, which is for deliver last per subject and we are just starting.
// Lock should be held.
func (o *consumer) hasSkipListPending() bool {
	return o.lss != nil && len(o.lss.seqs) > 0
}

// Will select the starting sequence.
func (o *consumer) selectStartingSeqNo() {
	if o.mset == nil || o.mset.store == nil {
		o.sseq = 1
	} else {
		var state StreamState
		o.mset.store.FastState(&state)
		if o.cfg.OptStartSeq == 0 {
			if o.cfg.DeliverPolicy == DeliverAll {
				o.sseq = state.FirstSeq
			} else if o.cfg.DeliverPolicy == DeliverLast {
				o.sseq = state.LastSeq
				// If we are partitioned here this will be properly set when we become leader.
				if o.cfg.FilterSubject != _EMPTY_ {
					ss := o.mset.store.FilteredState(1, o.cfg.FilterSubject)
					o.sseq = ss.Last
				}
			} else if o.cfg.DeliverPolicy == DeliverLastPerSubject {
				if mss := o.mset.store.SubjectsState(o.cfg.FilterSubject); len(mss) > 0 {
					o.lss = &lastSeqSkipList{
						resume: state.LastSeq,
						seqs:   createLastSeqSkipList(mss),
					}
					o.sseq = o.lss.seqs[0]
				} else {
					// If no mapping info just set to last.
					o.sseq = state.LastSeq
				}
			} else if o.cfg.OptStartTime != nil {
				// If we are here we are time based.
				// TODO(dlc) - Once clustered can't rely on this.
				o.sseq = o.mset.store.GetSeqFromTime(*o.cfg.OptStartTime)
			} else {
				o.sseq = state.LastSeq + 1
			}
		} else {
			o.sseq = o.cfg.OptStartSeq
		}

		if state.FirstSeq == 0 {
			o.sseq = 1
		} else if o.sseq < state.FirstSeq {
			o.sseq = state.FirstSeq
		} else if o.sseq > state.LastSeq {
			o.sseq = state.LastSeq + 1
		}
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
// sseq is the new first seq for the stream after purge.
// Lock should be held.
func (o *consumer) purge(sseq uint64) {
	// Do not update our state unless we know we are the leader.
	if !o.isLeader() {
		return
	}
	// Signals all have been purged for this consumer.
	if sseq == 0 {
		sseq = o.mset.lastSeq() + 1
	}

	o.mu.Lock()
	// Do not go backwards
	if o.sseq < sseq {
		o.sseq = sseq
	}
	if o.asflr < sseq {
		o.asflr = sseq - 1
		if o.dseq > 0 {
			o.adflr = o.dseq - 1
		}
	}
	o.sgap = 0
	o.pending = nil

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
	return o.stopWithFlags(false, false, true, false)
}

func (o *consumer) deleteWithoutAdvisory() error {
	return o.stopWithFlags(true, false, true, false)
}

// Delete will delete the consumer for the associated stream and send advisories.
func (o *consumer) delete() error {
	return o.stopWithFlags(true, false, true, true)
}

func (o *consumer) stopWithFlags(dflag, sdflag, doSignal, advisory bool) error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil
	}
	o.closed = true

	// Check if we are the leader and are being deleted.
	if dflag && o.isLeader() {
		// If we are clustered and node leader (probable from above), stepdown.
		if node := o.node; node != nil && node.Leader() {
			node.StepDown()
		}
		if advisory {
			o.sendDeleteAdvisoryLocked()
		}
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
	qgroup := o.cfg.DeliverGroup
	o.mu.Unlock()

	if c != nil {
		c.closeConnection(ClientClosed)
	}
	if sysc != nil {
		sysc.closeConnection(ClientClosed)
	}

	if delivery != _EMPTY_ {
		a.sl.clearNotification(delivery, qgroup, o.inch)
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

		var rmseqs []uint64
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

	// Clean up our store.
	var err error
	if store != nil {
		if dflag {
			if sdflag {
				err = store.StreamDelete()
			} else {
				err = store.Delete()
			}
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

// Check that the filtered subject is valid given a set of stream subjects.
func validFilteredSubject(filteredSubject string, subjects []string) bool {
	if !IsValidSubject(filteredSubject) {
		return false
	}
	hasWC := subjectHasWildcard(filteredSubject)

	for _, subject := range subjects {
		if subjectIsSubsetMatch(filteredSubject, subject) {
			return true
		}
		// If we have a wildcard as the filtered subject check to see if we are
		// a wider scope but do match a subject.
		if hasWC && subjectIsSubsetMatch(subject, filteredSubject) {
			return true
		}
	}
	return false
}

// setInActiveDeleteThreshold sets the delete threshold for how long to wait
// before deleting an inactive ephemeral consumer.
func (o *consumer) setInActiveDeleteThreshold(dthresh time.Duration) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.isDurable() {
		return fmt.Errorf("consumer is not ephemeral")
	}
	deleteWasRunning := o.dtmr != nil
	stopAndClearTimer(&o.dtmr)
	// Do not add jitter if set via here.
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
	// Setup dthresh.
	if o.dthresh == 0 {
		if o.cfg.InactiveThreshold != 0 {
			o.dthresh = o.cfg.InactiveThreshold
		} else {
			// Add in 1 sec of jitter above and beyond the default of 5s.
			o.dthresh = JsDeleteWaitTimeDefault + time.Duration(rand.Int63n(1000))*time.Millisecond
		}
	}
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

// Will set the initial pending and start sequence.
// mset lock should be held.
func (o *consumer) setInitialPendingAndStart() {
	mset := o.mset
	if mset == nil || mset.store == nil {
		return
	}

	// !filtered means we want all messages.
	filtered, dp := o.cfg.FilterSubject != _EMPTY_, o.cfg.DeliverPolicy
	if filtered {
		// Check to see if we directly match the configured stream.
		// Many clients will always send a filtered subject.
		cfg := &mset.cfg
		if len(cfg.Subjects) == 1 && cfg.Subjects[0] == o.cfg.FilterSubject {
			filtered = false
		}
	}

	if !filtered && dp != DeliverLastPerSubject {
		var state StreamState
		mset.store.FastState(&state)
		if state.Msgs > 0 {
			o.sgap = state.Msgs - (o.sseq - state.FirstSeq)
			o.lsgap = state.LastSeq
		}
	} else {
		// Here we are filtered.
		if dp == DeliverLastPerSubject && o.hasSkipListPending() && o.sseq < o.lss.resume {
			ss := mset.store.FilteredState(o.lss.resume+1, o.cfg.FilterSubject)
			o.sseq = o.lss.seqs[0]
			o.sgap = ss.Msgs + uint64(len(o.lss.seqs))
			o.lsgap = ss.Last
		} else if ss := mset.store.FilteredState(o.sseq, o.cfg.FilterSubject); ss.Msgs > 0 {
			o.sgap = ss.Msgs
			o.lsgap = ss.Last
			// See if we should update our starting sequence.
			if dp == DeliverLast || dp == DeliverLastPerSubject {
				o.sseq = ss.Last
			} else if dp == DeliverNew {
				o.sseq = ss.Last + 1
			} else {
				// DeliverAll, DeliverByStartSequence, DeliverByStartTime
				o.sseq = ss.First
			}
			// Cleanup lss when we take over in clustered mode.
			if dp == DeliverLastPerSubject && o.hasSkipListPending() && o.sseq >= o.lss.resume {
				o.lss = nil
			}
		}
		o.updateSkipped()
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
