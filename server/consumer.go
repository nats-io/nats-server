// Copyright 2019-2020 The NATS Authors
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
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	mrand "math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nuid"
)

type ConsumerInfo struct {
	Stream string         `json:"stream_name"`
	Name   string         `json:"name"`
	Config ConsumerConfig `json:"config"`
	State  ConsumerState  `json:"state"`
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
	SampleFrequency string        `json:"sample_freq,omitempty"`
}

type CreateConsumerRequest struct {
	Stream string         `json:"stream_name"`
	Config ConsumerConfig `json:"config"`
}

// ConsumerAckMetric is a metric published when a user acknowledges a message, the
// number of these that will be published is dependant on SampleFrequency
type ConsumerAckMetric struct {
	Schema      string `json:"schema"`
	ID          string `json:"id"`
	Time        string `json:"timestamp"`
	Stream      string `json:"stream"`
	Consumer    string `json:"consumer"`
	ConsumerSeq uint64 `json:"consumer_seq"`
	StreamSeq   uint64 `json:"stream_seq"`
	Delay       int64  `json:"ack_time"`
	Deliveries  uint64 `json:"deliveries"`
}

// ConsumerDeliveryExceededAdvisory is an advisory informing that a message hit
// its MaxDeliver threshold and so might be a candidate for DLQ handling
type ConsumerDeliveryExceededAdvisory struct {
	Schema     string `json:"schema"`
	ID         string `json:"id"`
	Time       string `json:"timestamp"`
	Stream     string `json:"stream"`
	Consumer   string `json:"consumer"`
	StreamSeq  uint64 `json:"stream_seq"`
	Deliveries uint64 `json:"deliveries"`
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

// Ack responses. Note that a nil or no payload is same as AckAck
var (
	// Ack
	AckAck = []byte(OK) // nil or no payload to ack subject also means ACK
	// Nack
	AckNak = []byte("-NAK")
	// Progress indicator
	AckProgress = []byte("+WPI")
	// Ack + Deliver the next message(s).
	AckNext = []byte("+NXT")
)

// Consumer is a jetstream consumer.
type Consumer struct {
	mu                sync.Mutex
	mset              *Stream
	acc               *Account
	name              string
	stream            string
	sseq              uint64
	dseq              uint64
	adflr             uint64
	asflr             uint64
	dsubj             string
	reqSub            *subscription
	ackSub            *subscription
	ackReplyT         string
	nextMsgSubj       string
	pending           map[uint64]int64
	ptmr              *time.Timer
	rdq               []uint64
	rdc               map[uint64]uint64
	maxdc             uint64
	waiting           []string
	config            ConsumerConfig
	store             ConsumerStore
	active            bool
	replay            bool
	filterWC          bool
	dtmr              *time.Timer
	dthresh           time.Duration
	fch               chan struct{}
	qch               chan struct{}
	inch              chan bool
	sfreq             int32
	ackEventT         string
	deliveryExcEventT string
}

const (
	// JsAckWaitDefault is the default AckWait, only applicable on explicit ack policy observables.
	JsAckWaitDefault = 30 * time.Second
	// JsDeleteWaitTimeDefault is the default amount of time we will wait for non-durable
	// observables to be in an inactive state before deleting them.
	JsDeleteWaitTimeDefault = 5 * time.Second
)

func (mset *Stream) AddConsumer(config *ConsumerConfig) (*Consumer, error) {
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
	}

	// Setup proper default for ack wait if we are in explicit ack mode.
	if config.AckPolicy == AckExplicit && config.AckWait == time.Duration(0) {
		config.AckWait = JsAckWaitDefault
	}
	// Setup default of -1, meaning no limit for MaxDeliver.
	if config.MaxDeliver == 0 {
		config.MaxDeliver = -1
	}

	// Make sure any partition subject is also a literal.
	if config.FilterSubject != "" {
		// Make sure this is a valid partition of the interest subjects.
		if !mset.validSubject(config.FilterSubject) {
			return nil, fmt.Errorf("consumer filter subject is not a valid subset of the interest subjects")
		}
		if config.AckPolicy == AckAll {
			return nil, fmt.Errorf("consumer with filter subject can not have an ack policy of ack all")
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

	// Hold mset lock here.
	mset.mu.Lock()

	// Check for any limits.
	if mset.config.MaxConsumers > 0 && len(mset.consumers) >= mset.config.MaxConsumers {
		mset.mu.Unlock()
		return nil, fmt.Errorf("maximum consumers limit reached")
	}

	// Check on stream type conflicts.
	switch mset.config.Retention {
	case WorkQueuePolicy:
		// Force explicit acks here.
		if config.AckPolicy != AckExplicit {
			mset.mu.Unlock()
			return nil, fmt.Errorf("workqueue stream requires explicit ack")
		}

		if len(mset.consumers) > 0 {
			if config.FilterSubject == _EMPTY_ {
				mset.mu.Unlock()
				return nil, fmt.Errorf("multiple non-filtered observables not allowed on workqueue stream")
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
	o := &Consumer{mset: mset,
		config: *config,
		dsubj:  config.DeliverSubject,
		active: true,
		qch:    make(chan struct{}),
		fch:    make(chan struct{}),
		sfreq:  int32(sampleFreq),
		maxdc:  uint64(config.MaxDeliver),
	}
	if isDurableConsumer(config) {
		o.name = config.Durable
	} else {
		o.name = createConsumerName()
	}

	// Check if we have  filtered subject that is a wildcard.
	if config.FilterSubject != _EMPTY_ && !subjectIsLiteral(config.FilterSubject) {
		o.filterWC = true
	}

	// already under lock, mset.Name() would deadlock
	o.stream = mset.config.Name
	o.ackEventT = JetStreamMetricConsumerAckPre + "." + o.stream + "." + o.name
	o.deliveryExcEventT = JetStreamAdvisoryConsumerMaxDeliveryExceedPre + "." + o.stream + "." + o.name

	store, err := mset.store.ConsumerStore(o.name, config)
	if err != nil {
		mset.mu.Unlock()
		return nil, fmt.Errorf("error creating store for observable: %v", err)
	}
	o.store = store

	if !isValidName(o.name) {
		mset.mu.Unlock()
		return nil, fmt.Errorf("durable name can not contain '.', '*', '>'")
	}

	// Select starting sequence number
	o.selectStartingSeqNo()

	// Now register with mset and create the ack subscription.
	c := mset.client
	if c == nil {
		mset.mu.Unlock()
		return nil, fmt.Errorf("stream not valid")
	}
	s, a := c.srv, c.acc
	o.acc = a

	// Check if we already have this one registered.
	if eo, ok := mset.consumers[o.name]; ok {
		mset.mu.Unlock()
		if !o.isDurable() || !o.isPushMode() {
			return nil, fmt.Errorf("consumer already exists")
		}
		// If we are here we have already registered this durable. If it is still active that is an error.
		if eo.Active() {
			return nil, fmt.Errorf("consumer already exists and is still active")
		}
		// Since we are here this means we have a potentially new durable so we should update here.
		// Check that configs are the same.
		if !configsEqualSansDelivery(o.config, eo.config) {
			return nil, fmt.Errorf("consumer replacement durable config not the same")
		}
		// Once we are here we have a replacement push-based durable.
		eo.updateDeliverSubject(o.config.DeliverSubject)
		return eo, nil
	}
	// Set up the ack subscription for this observable. Will use wildcard for all acks.
	// We will remember the template to generate replies with sequence numbers and use
	// that to scanf them back in.
	mn := mset.config.Name
	pre := fmt.Sprintf(JetStreamAckT, mn, o.name)
	o.ackReplyT = fmt.Sprintf("%s.%%d.%%d.%%d.%%d", pre)
	ackSubj := fmt.Sprintf("%s.*.*.*.*", pre)
	if sub, err := mset.subscribeInternal(ackSubj, o.processAck); err != nil {
		return nil, err
	} else {
		o.ackSub = sub
	}

	// Setup the internal sub for next message requests.
	if !o.isPushMode() {
		o.nextMsgSubj = fmt.Sprintf(JetStreamRequestNextT, mn, o.name)
		if sub, err := mset.subscribeInternal(o.nextMsgSubj, o.processNextMsgReq); err != nil {
			o.Delete()
			return nil, err
		} else {
			o.reqSub = sub
		}
	}
	mset.consumers[o.name] = o
	mset.mu.Unlock()

	// If push mode, register for notifications on interest.
	if o.isPushMode() {
		o.dthresh = JsDeleteWaitTimeDefault
		o.inch = make(chan bool, 4)
		a.sl.RegisterNotification(config.DeliverSubject, o.inch)
		o.active = o.hasDeliveryInterest(<-o.inch)
		// Check if we are not durable that the delivery subject has interest.
		if !o.isDurable() && !o.active {
			o.Delete()
			return nil, fmt.Errorf("consumer requires interest for delivery subject when ephemeral")
		}
	}

	// If we are not in ReplayInstant mode mark us as in replay state until resolved.
	if config.ReplayPolicy != ReplayInstant {
		o.replay = true
	}

	// Now start up Go routine to deliver msgs.
	go o.loopAndDeliverMsgs(s, a)
	// Startup our state update loop.
	go o.updateStateLoop()

	return o, nil
}

// This will check for extended interest in a subject. If we have local interest we just return
// that, but in the absence of local interest and presence of gateways or service imports we need
// to check those as well.
func (o *Consumer) hasDeliveryInterest(localInterest bool) bool {
	o.mu.Lock()
	mset := o.mset
	if mset == nil {
		o.mu.Unlock()
		return false
	}
	acc := o.acc
	deliver := o.config.DeliverSubject
	o.mu.Unlock()

	if localInterest {
		return true
	}

	// If we are here check gateways.
	if acc.srv != nil && acc.srv.gateway.enabled {
		gw := acc.srv.gateway
		gw.RLock()
		for _, gwc := range gw.outo {
			psi, qr := gwc.gatewayInterest(acc.Name, deliver)
			if psi || qr != nil {
				gw.RUnlock()
				return true
			}
		}
		gw.RUnlock()
	}
	return false
}

// This processes an update to the local interest for a deliver subject.
func (o *Consumer) updateDeliveryInterest(localInterest bool) {
	interest := o.hasDeliveryInterest(localInterest)

	o.mu.Lock()
	mset := o.mset
	if mset == nil || o.isPullMode() {
		o.mu.Unlock()
		return
	}
	shouldSignal := interest && !o.active
	o.active = interest

	// Stop and clear the delete timer always.
	stopAndClearTimer(&o.dtmr)

	// If we do not have interest anymore and we are not durable start
	// a timer to delete us. We wait for a bit in case of server reconnect.
	if !o.isDurable() && !interest {
		o.dtmr = time.AfterFunc(o.dthresh, func() { o.Delete() })
	}
	o.mu.Unlock()

	if shouldSignal {
		mset.signalConsumers()
	}
}

// Config returns the consumer's configuration.
func (o *Consumer) Config() ConsumerConfig {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.config
}

// This is a config change for the delivery subject for a
// push based consumer.
func (o *Consumer) updateDeliverSubject(newDeliver string) {
	// Update the config and the dsubj
	o.mu.Lock()
	defer o.mu.Unlock()

	mset := o.mset
	if mset == nil || o.isPullMode() {
		return
	}

	oldDeliver := o.config.DeliverSubject
	o.dsubj = newDeliver
	o.config.DeliverSubject = newDeliver
	// FIXME(dlc) - check partitions, we may need offset.
	o.dseq = o.adflr
	o.sseq = o.asflr

	// When we register new one it will deliver to update state loop.
	o.acc.sl.ClearNotification(oldDeliver, o.inch)
	o.acc.sl.RegisterNotification(newDeliver, o.inch)
}

// Check that configs are equal but allow delivery subjects to be different.
func configsEqualSansDelivery(a, b ConsumerConfig) bool {
	// These were copied in so can set Delivery here.
	a.DeliverSubject, b.DeliverSubject = _EMPTY_, _EMPTY_
	return a == b
}

// Process a message for the ack reply subject delivered with a message.
func (o *Consumer) processAck(_ *subscription, _ *client, subject, reply string, msg []byte) {
	sseq, dseq, dcount, _ := o.ReplyInfo(subject)

	switch {
	case len(msg) == 0, bytes.Equal(msg, AckAck):
		o.ackMsg(sseq, dseq, dcount)
	case bytes.Equal(msg, AckNext):
		o.ackMsg(sseq, dseq, dcount)
		o.processNextMsgReq(nil, nil, subject, reply, nil)
	case bytes.Equal(msg, AckNak):
		o.processNak(sseq, dseq)
	case bytes.Equal(msg, AckProgress):
		o.progressUpdate(sseq)
	}
}

// Used to process a working update to delay redelivery.
func (o *Consumer) progressUpdate(seq uint64) {
	o.mu.Lock()
	if o.pending != nil {
		if _, ok := o.pending[seq]; ok {
			o.pending[seq] = time.Now().UnixNano()
		}
	}
	o.mu.Unlock()
}

// Process a NAK.
func (o *Consumer) processNak(sseq, dseq uint64) {
	var mset *Stream
	o.mu.Lock()
	// Check for out of range.
	if dseq <= o.adflr || dseq > o.dseq {
		o.mu.Unlock()
		return
	}
	// If we are explicit ack make sure this is still on pending list.
	if len(o.pending) > 0 {
		if _, ok := o.pending[sseq]; !ok {
			o.mu.Unlock()
			return
		}
	}
	// If already queued up also ignore.
	if !o.onRedeliverQueue(sseq) {
		o.rdq = append(o.rdq, sseq)
		mset = o.mset
	}
	o.mu.Unlock()
	if mset != nil {
		mset.signalConsumers()
	}
}

// This will restore the state from disk.
func (o *Consumer) readStoredState() error {
	if o.store == nil {
		return nil
	}
	state, err := o.store.State()
	if err == nil && state != nil {
		// FIXME(dlc) - re-apply state.
		o.dseq = state.Delivered.ConsumerSeq
		o.sseq = state.Delivered.StreamSeq
		o.adflr = state.AckFloor.ConsumerSeq
		o.asflr = state.AckFloor.StreamSeq
		o.pending = state.Pending
		o.rdc = state.Redelivered
	}

	// Setup tracking timer if we have restored pending.
	if len(o.pending) > 0 && o.ptmr == nil {
		o.ptmr = time.AfterFunc(o.config.AckWait, o.checkPending)
	}
	return err
}

func (o *Consumer) updateStateLoop() {
	o.mu.Lock()
	fch := o.fch
	qch := o.qch
	inch := o.inch
	o.mu.Unlock()

	for {
		select {
		case <-qch:
			return
		case interest := <-inch:
			// inch can be nil on pull-based, but then this will
			// just block and not fire.
			o.updateDeliveryInterest(interest)
		case <-fch:
			// FIXME(dlc) - Check for fast changes at quick intervals.
			time.Sleep(25 * time.Millisecond)
			o.mu.Lock()
			if o.store != nil {
				state := &ConsumerState{
					Delivered: SequencePair{
						ConsumerSeq: o.dseq,
						StreamSeq:   o.sseq,
					},
					AckFloor: SequencePair{
						ConsumerSeq: o.adflr,
						StreamSeq:   o.asflr,
					},
					Pending:     o.pending,
					Redelivered: o.rdc,
				}
				// FIXME(dlc) - Hold onto any errors.
				o.store.Update(state)
			}
			o.mu.Unlock()
		}
	}
}

// Info returns our current consumer state.
func (o *Consumer) Info() *ConsumerInfo {
	o.mu.Lock()
	defer o.mu.Unlock()
	info := &ConsumerInfo{
		Stream: o.stream,
		Name:   o.name,
		Config: o.config,
		State: ConsumerState{
			// We track these internally as next to deliver, hence the -1.
			Delivered: SequencePair{
				ConsumerSeq: o.dseq - 1,
				StreamSeq:   o.sseq - 1,
			},
			AckFloor: SequencePair{
				ConsumerSeq: o.adflr,
				StreamSeq:   o.asflr,
			},
		},
	}

	if len(o.pending) > 0 {
		p := make(map[uint64]int64, len(o.pending))
		for k, v := range o.pending {
			p[k] = v
		}
		info.State.Pending = p
	}
	if len(o.rdc) > 0 {
		r := make(map[uint64]uint64, len(o.rdc))
		for k, v := range o.rdc {
			r[k] = v
		}
		info.State.Redelivered = r
	}

	return info
}

// Will update the underlying store.
// Lock should be held.
func (o *Consumer) updateStore() {
	if o.store == nil {
		return
	}
	// Kick our flusher
	select {
	case o.fch <- struct{}{}:
	default:
	}
}

// shouldSample lets us know if we are sampling metrics on acks.
func (o *Consumer) shouldSample() bool {
	switch {
	case o.sfreq <= 0:
		return false
	case o.sfreq >= 100:
		return true
	}

	// TODO(ripienaar) this is a tad slow so we need to rethink here, however this will only
	// hit for those with sampling enabled and its not the default
	return mrand.Int31n(100) <= o.sfreq
}

func (o *Consumer) sampleAck(sseq, dseq, dcount uint64) {
	if !o.shouldSample() {
		return
	}

	now := time.Now().UTC()
	unow := now.UnixNano()
	e := &ConsumerAckMetric{
		Schema:      "io.nats.jetstream.metric.v1.consumer_ack",
		ID:          nuid.Next(),
		Time:        now.Format(time.RFC3339Nano),
		Stream:      o.stream,
		Consumer:    o.name,
		ConsumerSeq: dseq,
		StreamSeq:   sseq,
		Delay:       unow - o.pending[sseq],
		Deliveries:  dcount,
	}

	j, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return
	}

	// can be nil during server Shutdown but with some ACKs in flight internally, lock held in caller
	if o.mset != nil && o.mset.sendq != nil {
		o.mset.sendq <- &jsPubMsg{o.ackEventT, o.ackEventT, _EMPTY_, j, nil, 0}
	}
}

// Process an ack for a message.
func (o *Consumer) ackMsg(sseq, dseq, dcount uint64) {
	var sagap uint64
	o.mu.Lock()
	switch o.config.AckPolicy {
	case AckExplicit:
		o.sampleAck(sseq, dseq, dcount)
		delete(o.pending, sseq)
		// Consumers sequence numbers can skip during redlivery since
		// they always increment. So if we do not have any pending treat
		// as all scenario below. Otherwise check that we filled in a gap.
		// TODO(dlc) - check this.
		if len(o.pending) == 0 || dseq == o.adflr+1 {
			o.adflr, o.asflr = dseq, sseq
		}
		delete(o.rdc, sseq)
	case AckAll:
		// no-op
		if dseq <= o.adflr || sseq <= o.asflr {
			o.mu.Unlock()
			return
		}
		sagap = sseq - o.asflr
		o.adflr, o.asflr = dseq, sseq
		// FIXME(dlc) - delete rdc entries?
	case AckNone:
		// FIXME(dlc) - This is error but do we care?
		o.mu.Unlock()
		return
	}
	o.updateStore()

	mset := o.mset
	o.mu.Unlock()

	// Let the owning stream know if we are interest or workqueue retention based.
	if mset != nil && mset.config.Retention != LimitsPolicy {
		if sagap > 1 {
			// FIXME(dlc) - This is very inefficient, will need to fix.
			for seq := sseq; seq > sseq-sagap; seq-- {
				mset.ackMsg(o, seq)
			}
		} else {
			mset.ackMsg(o, sseq)
		}
	}
}

// Check if we need an ack for this store seq.
func (o *Consumer) needAck(sseq uint64) bool {
	var na bool
	o.mu.Lock()
	switch o.config.AckPolicy {
	case AckNone, AckAll:
		na = sseq > o.asflr
	case AckExplicit:
		if sseq > o.asflr && len(o.pending) > 0 {
			_, na = o.pending[sseq]
		}
	}
	o.mu.Unlock()
	return na
}

// Default is 1 if msg is nil.
func batchSizeFromMsg(msg []byte) int {
	bs := 1
	if len(msg) > 0 {
		if n, err := strconv.Atoi(string(msg)); err == nil {
			bs = n
		}
	}
	return bs
}

// processNextMsgReq will process a request for the next message available. A nil message payload means deliver
// a single message. If the payload is a number parseable with Atoi(), then we will send a batch of messages without
// requiring another request to this endpoint, or an ACK.
func (o *Consumer) processNextMsgReq(_ *subscription, _ *client, _, reply string, msg []byte) {
	// Check payload here to see if they sent in batch size.
	batchSize := batchSizeFromMsg(msg)

	o.mu.Lock()
	mset := o.mset
	if mset == nil || o.isPushMode() {
		o.mu.Unlock()
		return
	}
	shouldSignal := false

	for i := 0; i < batchSize; i++ {
		// If we are in replay mode, defer to processReplay for delivery.
		if o.replay {
			o.waiting = append(o.waiting, reply)
			shouldSignal = true
		} else if subj, msg, seq, dc, ts, err := o.getNextMsg(); err == nil {
			o.deliverMsg(reply, subj, msg, seq, dc, ts)
		} else {
			o.waiting = append(o.waiting, reply)
		}
	}
	o.mu.Unlock()
	if shouldSignal {
		mset.signalConsumers()
	}
}

// Increase the delivery count for this message.
// ONLY used on redelivery semantics.
// Lock should be held.
func (o *Consumer) incDeliveryCount(sseq uint64) uint64 {
	if o.rdc == nil {
		o.rdc = make(map[uint64]uint64)
	}
	o.rdc[sseq] += 1
	return o.rdc[sseq] + 1
}

func (o *Consumer) notifyDeliveryExceeded(sseq, dcount uint64) {
	e := &ConsumerDeliveryExceededAdvisory{
		Schema:     "io.nats.jetstream.advisory.v1.max_deliver",
		ID:         nuid.Next(),
		Time:       time.Now().UTC().Format(time.RFC3339Nano),
		Stream:     o.stream,
		Consumer:   o.name,
		StreamSeq:  sseq,
		Deliveries: dcount,
	}

	j, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return
	}

	// can be nil during shutdown, locks are held in the caller
	if o.mset != nil && o.mset.sendq != nil {
		o.mset.sendq <- &jsPubMsg{o.deliveryExcEventT, o.deliveryExcEventT, _EMPTY_, j, nil, 0}
	}
}

// Check to see if the candidate subject matches a filter if its present.
func (o *Consumer) isFilteredMatch(subj string) bool {
	if !o.filterWC {
		return subj == o.config.FilterSubject
	}
	// If we are here we have a wildcard filter subject.
	// TODO(dlc) at speed might be better to just do a sublist with L2 and/or possibly L1.
	return subjectIsSubsetMatch(subj, o.config.FilterSubject)
}

// Get next available message from underlying store.
// Is partition aware and redeliver aware.
// Lock should be held.
func (o *Consumer) getNextMsg() (subj string, msg []byte, seq uint64, dcount uint64, ts int64, err error) {
	if o.mset == nil {
		return _EMPTY_, nil, 0, 0, 0, fmt.Errorf("consumer not valid")
	}
	for {
		seq, dcount := o.sseq, uint64(1)
		if len(o.rdq) > 0 {
			seq = o.rdq[0]
			o.rdq = append(o.rdq[:0], o.rdq[1:]...)
			dcount = o.incDeliveryCount(seq)
			if o.maxdc > 0 && dcount > o.maxdc {
				// Only send once
				if dcount == o.maxdc+1 {
					o.notifyDeliveryExceeded(seq, dcount-1)
				}
				// Make sure to remove from pending.
				delete(o.pending, seq)
				continue
			}
		}
		subj, msg, ts, err := o.mset.store.LoadMsg(seq)
		if err == nil {
			if dcount == 1 { // First delivery.
				o.sseq++
				if o.config.FilterSubject != _EMPTY_ && !o.isFilteredMatch(subj) {
					continue
				}
			}
			// We have the msg here.
			return subj, msg, seq, dcount, ts, nil
		}
		// We got an error here. If this is an EOF we will return, otherwise
		// we can continue looking.
		if err == ErrStoreEOF || err == ErrStoreClosed {
			return "", nil, 0, 0, 0, err
		}
		// Skip since its probably deleted or expired.
		o.sseq++
	}
}

// Will check to make sure those waiting still have registered interest.
func (o *Consumer) checkWaitingForInterest() bool {
	for len(o.waiting) > 0 {
		rr := o.acc.sl.Match(o.waiting[0])
		if len(rr.psubs)+len(rr.qsubs) > 0 {
			break
		}
		// No more interest so go ahead and remove this one from our list.
		o.waiting = append(o.waiting[:0], o.waiting[1:]...)
	}
	return len(o.waiting) > 0
}

func (o *Consumer) loopAndDeliverMsgs(s *Server, a *Account) {
	// On startup check to see if we are in a a reply situtation where replay policy is not instant.
	var (
		lts  int64 // last time stamp seen, used for replay.
		lseq uint64
	)

	o.mu.Lock()
	if o.replay {
		// consumer is closed when mset is set to nil.
		if o.mset == nil {
			o.mu.Unlock()
			return
		}
		lseq = o.mset.State().LastSeq
	}
	o.mu.Unlock()

	// Deliver all the msgs we have now, once done or on a condition, we wait for new ones.
	for {
		var (
			mset        *Stream
			seq, dcnt   uint64
			subj, dsubj string
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
		mset = o.mset

		// If we are in push mode and not active let's stop sending.
		if o.isPushMode() && !o.active {
			goto waitForMsgs
		}

		// If we are in pull mode and no one is waiting already break and wait.
		if o.isPullMode() && !o.checkWaitingForInterest() {
			goto waitForMsgs
		}

		subj, msg, seq, dcnt, ts, err = o.getNextMsg()

		// On error either wait or return.
		if err != nil {
			if err == ErrStoreMsgNotFound || err == ErrStoreEOF {
				goto waitForMsgs
			} else {
				o.mu.Unlock()
				return
			}
		}

		if len(o.waiting) > 0 {
			dsubj = o.waiting[0]
			o.waiting = append(o.waiting[:0], o.waiting[1:]...)
		} else {
			dsubj = o.dsubj
		}

		// If we are in a replay scenario and have not caught up check if we need to delay here.
		if o.replay && lts > 0 {
			if delay = time.Duration(ts - lts); delay > time.Millisecond {
				qch := o.qch
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

		o.deliverMsg(dsubj, subj, msg, seq, dcnt, ts)

		o.mu.Unlock()
		continue

	waitForMsgs:
		// If we were in a replay state check to see if we are caught up. If so clear.
		if o.replay && o.sseq > lseq {
			o.replay = false
		}
		// We will wait here for new messages to arrive.
		o.mu.Unlock()
		mset.waitForMsgs()
	}
}

func (o *Consumer) ackReply(sseq, dseq, dcount uint64, ts int64) string {
	return fmt.Sprintf(o.ackReplyT, dcount, sseq, dseq, ts)
}

// deliverCurrentMsg is the hot path to deliver a message that was just received.
// Will return if the message was delivered or not.
func (o *Consumer) deliverCurrentMsg(subj string, msg []byte, seq uint64, ts int64) bool {
	o.mu.Lock()
	if seq != o.sseq {
		o.mu.Unlock()
		return false
	}

	// If we are in push mode and not active let's stop sending.
	if o.isPushMode() && !o.active {
		o.mu.Unlock()
		return false
	}

	// If we are in pull mode and no one is waiting already break and wait.
	if o.isPullMode() && !o.checkWaitingForInterest() {
		o.mu.Unlock()
		return false
	}

	// Bump store sequence here.
	o.sseq++

	// If we are partitioned and we do not match, do not consider this a failure.
	// Go ahead and return true.
	if o.config.FilterSubject != _EMPTY_ && !o.isFilteredMatch(subj) {
		o.mu.Unlock()
		return true
	}
	var dsubj string
	if len(o.waiting) > 0 {
		dsubj = o.waiting[0]
		o.waiting = append(o.waiting[:0], o.waiting[1:]...)
	} else {
		dsubj = o.dsubj
	}

	if len(msg) > 0 {
		msg = append(msg[:0:0], msg...)
	}

	o.deliverMsg(dsubj, subj, msg, seq, 1, ts)
	o.mu.Unlock()

	return true
}

// Deliver a msg to the observable.
// Lock should be held and o.mset validated to be non-nil.
func (o *Consumer) deliverMsg(dsubj, subj string, msg []byte, seq, dcount uint64, ts int64) {
	if o.mset == nil {
		return
	}
	sendq := o.mset.sendq
	pmsg := &jsPubMsg{dsubj, subj, o.ackReply(seq, o.dseq, dcount, ts), msg, o, seq}

	// This needs to be unlocked since the other side may need this lock on failed delivery.
	o.mu.Unlock()
	sendq <- pmsg
	o.mu.Lock()

	if o.config.AckPolicy == AckNone {
		o.adflr = o.dseq
		o.asflr = seq
	} else if o.config.AckPolicy == AckExplicit {
		o.trackPending(seq)
	}
	o.dseq++
	o.updateStore()
}

// Tracks our outstanding pending acks. Only applicable to AckExplicit mode.
// Lock should be held.
func (o *Consumer) trackPending(seq uint64) {
	if o.pending == nil {
		o.pending = make(map[uint64]int64)
	}
	if o.ptmr == nil {
		o.ptmr = time.AfterFunc(o.config.AckWait, o.checkPending)
	}
	o.pending[seq] = time.Now().UnixNano()
}

// didNotDeliver is called when a delivery for a consumer message failed.
// Depending on our state, we will process the failure.
func (o *Consumer) didNotDeliver(seq uint64) {
	o.mu.Lock()
	mset := o.mset
	if mset == nil {
		o.mu.Unlock()
		return
	}
	shouldSignal := false
	if o.isPushMode() {
		o.active = false
	} else if o.pending != nil {
		// push mode and we have pending.
		if _, ok := o.pending[seq]; ok {
			// We found this messsage on pending, we need
			// to queue it up for immediate redelivery since
			// we know it was not delivered.
			if !o.onRedeliverQueue(seq) {
				o.rdq = append(o.rdq, seq)
				shouldSignal = true
			}
		}
	}
	o.mu.Unlock()
	if shouldSignal {
		mset.signalConsumers()
	}
}

// This checks if we already have this sequence queued for redelivery.
// FIXME(dlc) - This is O(n) but should be fast with small redeliver size.
// Lock should be held.
func (o *Consumer) onRedeliverQueue(seq uint64) bool {
	for _, rseq := range o.rdq {
		if rseq == seq {
			return true
		}
	}
	return false
}

// Checks the pending messages.
func (o *Consumer) checkPending() {
	now := time.Now().UnixNano()
	shouldSignal := false

	o.mu.Lock()
	mset := o.mset
	if mset == nil {
		o.mu.Unlock()
		return
	}
	aw := int64(o.config.AckWait)

	// Since we can update timestamps, we have to review all pending.
	// We may want to unlock here or warn if list is big.
	// we also need to sort after.
	var expired []uint64
	for seq, ts := range o.pending {
		if now-ts > aw && !o.onRedeliverQueue(seq) {
			expired = append(expired, seq)
			shouldSignal = true
		}
	}

	if len(expired) > 0 {
		sort.Slice(expired, func(i, j int) bool { return expired[i] < expired[j] })
		o.rdq = append(o.rdq, expired...)
	}

	if len(o.pending) > 0 {
		o.ptmr.Reset(o.config.AckWait)
	} else {
		o.ptmr.Stop()
		o.ptmr = nil
	}
	o.mu.Unlock()

	if shouldSignal {
		mset.signalConsumers()
	}
}

// SeqFromReply will extract a sequence number from a reply subject.
func (o *Consumer) SeqFromReply(reply string) uint64 {
	_, seq, _, _ := o.ReplyInfo(reply)
	return seq
}

// StreamSeqFromReply will extract the stream sequence from the reply subject.
func (o *Consumer) StreamSeqFromReply(reply string) uint64 {
	seq, _, _, _ := o.ReplyInfo(reply)
	return seq
}

func (o *Consumer) ReplyInfo(reply string) (sseq, dseq, dcount uint64, ts int64) {
	n, err := fmt.Sscanf(reply, o.ackReplyT, &dcount, &sseq, &dseq, &ts)
	if err != nil || n != 4 {
		return 0, 0, 0, 0
	}
	return
}

// NextSeq returns the next delivered sequence number for this observable.
func (o *Consumer) NextSeq() uint64 {
	o.mu.Lock()
	dseq := o.dseq
	o.mu.Unlock()
	return dseq
}

// This will select the store seq to start with based on the
// partition subject.
func (o *Consumer) selectSubjectLast() {
	stats := o.mset.store.State()
	if stats.LastSeq == 0 {
		o.sseq = stats.LastSeq
		return
	}
	// FIXME(dlc) - this is linear and can be optimized by store layer.
	for seq := stats.LastSeq; seq >= stats.FirstSeq; seq-- {
		subj, _, _, err := o.mset.store.LoadMsg(seq)
		if err == ErrStoreMsgNotFound {
			continue
		}
		if o.isFilteredMatch(subj) {
			o.sseq = seq
			return
		}
	}
}

// Will select the starting sequence.
func (o *Consumer) selectStartingSeqNo() {
	stats := o.mset.store.State()
	if o.config.OptStartSeq == 0 {
		if o.config.DeliverPolicy == DeliverAll {
			o.sseq = stats.FirstSeq
		} else if o.config.DeliverPolicy == DeliverLast {
			o.sseq = stats.LastSeq
			// If we are partitioned here we may need to walk backwards.
			if o.config.FilterSubject != _EMPTY_ {
				o.selectSubjectLast()
			}
		} else if o.config.OptStartTime != nil {
			// If we are here we are time based.
			// TODO(dlc) - Once clustered can't rely on this.
			o.sseq = o.mset.store.GetSeqFromTime(*o.config.OptStartTime)
		} else {
			// Default is deliver new only.
			o.sseq = stats.LastSeq + 1
		}
	} else {
		o.sseq = o.config.OptStartSeq
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

func (o *Consumer) isDurable() bool {
	return o.config.Durable != _EMPTY_
}

// Are we in push mode, delivery subject, etc.
func (o *Consumer) isPushMode() bool {
	return o.config.DeliverSubject != _EMPTY_
}

func (o *Consumer) isPullMode() bool {
	return o.config.DeliverSubject == _EMPTY_
}

// Name returns the name of this observable.
func (o *Consumer) Name() string {
	o.mu.Lock()
	n := o.name
	o.mu.Unlock()
	return n
}

// For now size of 6 for randomly created names.
const randConsumerNameLen = 6

func createConsumerName() string {
	var b [64]byte
	rand.Read(b[:])
	sha := sha256.New()
	sha.Write(b[:])
	return fmt.Sprintf("%x", sha.Sum(nil))[:randConsumerNameLen]
}

// DeleteConsumer will delete the consumer from this stream.
func (mset *Stream) DeleteConsumer(o *Consumer) error {
	return o.Delete()
}

// Active indicates if this consumer is still active.
func (o *Consumer) Active() bool {
	o.mu.Lock()
	active := o.active && o.mset != nil
	o.mu.Unlock()
	return active
}

// This is when the underlying stream has been purged.
func (o *Consumer) purge(sseq uint64) {
	o.mu.Lock()
	o.sseq = sseq
	o.asflr = sseq - 1
	o.adflr = o.dseq - 1
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
		var newRDQ []uint64
		for _, sseq := range o.rdq {
			if sseq >= o.sseq {
				newRDQ = append(newRDQ, sseq)
			}
		}
		// Replace with new list. Most of the time this will be nil.
		o.rdq = newRDQ
	}
	o.mu.Unlock()
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
func (o *Consumer) Stop() error {
	return o.stop(false)
}

// Delete will delete the consumer for the associated stream.
func (o *Consumer) Delete() error {
	return o.stop(true)
}

func (o *Consumer) stop(dflag bool) error {
	o.mu.Lock()
	mset := o.mset
	if mset == nil {
		o.mu.Unlock()
		return nil
	}
	a := o.acc
	close(o.qch)

	store := o.store
	o.mset = nil
	o.active = false
	ackSub := o.ackSub
	reqSub := o.reqSub
	o.ackSub = nil
	o.reqSub = nil
	stopAndClearTimer(&o.ptmr)
	stopAndClearTimer(&o.dtmr)
	delivery := o.config.DeliverSubject
	o.mu.Unlock()

	if delivery != "" {
		a.sl.ClearNotification(delivery, o.inch)
	}

	mset.mu.Lock()
	// Break us out of the readLoop.
	// TODO(dlc) - Should not be bad for small amounts of observables, maybe
	// even into thousands. Above that should check what this might do
	// performance wise.
	mset.sg.Broadcast()
	mset.unsubscribe(ackSub)
	mset.unsubscribe(reqSub)
	delete(mset.consumers, o.name)
	mset.mu.Unlock()

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
func (mset *Stream) deliveryFormsCycle(deliverySubject string) bool {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	for _, subject := range mset.config.Subjects {
		if subjectIsSubsetMatch(deliverySubject, subject) {
			return true
		}
	}
	return false
}

// This is same as check for delivery cycle.
func (mset *Stream) validSubject(partitionSubject string) bool {
	return mset.deliveryFormsCycle(partitionSubject)
}

// SetInActiveDeleteThreshold sets the delete threshold for how long to wait
// before deleting an inactive ephemeral observable.
func (o *Consumer) SetInActiveDeleteThreshold(dthresh time.Duration) error {
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
		o.dtmr = time.AfterFunc(o.dthresh, func() { o.Delete() })
	}
	return nil
}

// RequestNextMsgSubject returns the subject to request the next message when in pull or worker mode.
// Returns empty otherwise.
func (o *Consumer) RequestNextMsgSubject() string {
	return o.nextMsgSubj
}
