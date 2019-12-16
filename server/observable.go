// Copyright 2019 The NATS Authors
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
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

type ObservableInfo struct {
	Config ObservableConfig `json:"configuration"`
	State  ObservableState  `json:"state"`
}

type ObservableConfig struct {
	Delivery     string        `json:"delivery_subject"`
	Durable      string        `json:"durable_name,omitempty"`
	MsgSetSeq    uint64        `json:"msg_set_seq,omitempty"`
	StartTime    time.Time     `json:"start_time,omitempty"`
	DeliverAll   bool          `json:"deliver_all,omitempty"`
	DeliverLast  bool          `json:"deliver_last,omitempty"`
	AckPolicy    AckPolicy     `json:"ack_policy"`
	AckWait      time.Duration `json:"ack_wait,omitempty"`
	Subject      string        `json:"subject,omitempty"`
	ReplayPolicy ReplayPolicy  `json:"replay_policy"`
}

type CreateObservableRequest struct {
	MsgSet string           `json:"msg_set_name"`
	Config ObservableConfig `json:"config"`
}

// AckPolicy determines how the observable should acknowledge delivered messages.
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

// ReplayPolicy determines how the observable should replay messages it already has queued in the message set.
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
	// Ack + deliver next.
	AckNext = []byte("+NXT")
)

// Observable is a jetstream observable/subscriber.
type Observable struct {
	mu          sync.Mutex
	mset        *MsgSet
	name        string
	sseq        uint64
	dseq        uint64
	adflr       uint64
	asflr       uint64
	dsubj       string
	reqSub      *subscription
	ackSub      *subscription
	ackReplyT   string
	nextMsgSubj string
	pending     map[uint64]int64
	ptmr        *time.Timer
	rdq         []uint64
	rdc         map[uint64]uint64
	waiting     []string
	config      ObservableConfig
	store       ObservableStore
	active      bool
	replay      bool
	atmr        *time.Timer
	nointerest  int
	athresh     int
	achk        time.Duration
	fch         chan struct{}
	qch         chan struct{}
}

const (
	// JsAckWaitDefault is the default AckWait, only applicable on explicit ack policy observables.
	JsAckWaitDefault = 30 * time.Second
	// JsActiveCheckIntervalDefault is default hb interval for push based observables.
	JsActiveCheckIntervalDefault = time.Second
	// JsNotActiveThresholdDefault is number of times we detect no interest to close an observable if ephemeral.
	JsNotActiveThresholdDefault = 2
)

func (mset *MsgSet) AddObservable(config *ObservableConfig) (*Observable, error) {
	if config == nil {
		return nil, fmt.Errorf("observable config required")
	}

	// For now expect a literal subject if its not empty. Empty means work queue mode (pull mode).
	if config.Delivery != _EMPTY_ {
		if !subjectIsLiteral(config.Delivery) {
			return nil, fmt.Errorf("observable delivery subject has wildcards")
		}
		if mset.deliveryFormsCycle(config.Delivery) {
			return nil, fmt.Errorf("observable delivery subject forms a cycle")
		}
	} else {
		// Pull mode / work queue mode require explicit ack.
		if config.AckPolicy != AckExplicit {
			return nil, fmt.Errorf("observable in pull mode requires explicit ack policy")
		}
	}

	// Setup proper default for ack wait if we are in explicit ack mode.
	if config.AckPolicy == AckExplicit && config.AckWait == time.Duration(0) {
		config.AckWait = JsAckWaitDefault
	}

	// Make sure any partition subject is also a literal.
	if config.Subject != "" {
		if !subjectIsLiteral(config.Subject) {
			return nil, fmt.Errorf("observable partition subject has wildcards")
		}
		// Make sure this is a valid partition of the interest subjects.
		if !mset.validSubject(config.Subject) {
			return nil, fmt.Errorf("observable partition not a valid subset of the interest subjects")
		}
		if config.AckPolicy == AckAll {
			return nil, fmt.Errorf("observable with partition can not have an ack policy of ack all")
		}
	}

	// Check on start position conflicts.
	noTime := time.Time{}
	if config.MsgSetSeq > 0 && (config.StartTime != noTime || config.DeliverAll || config.DeliverLast) {
		return nil, fmt.Errorf("observable starting position conflict")
	} else if config.StartTime != noTime && (config.DeliverAll || config.DeliverLast) {
		return nil, fmt.Errorf("observable starting position conflict")
	} else if config.DeliverAll && config.DeliverLast {
		return nil, fmt.Errorf("observable starting position conflict")
	}

	// Check if we are not durable that the delivery subject has interest.
	if config.Delivery != _EMPTY_ && config.Durable == _EMPTY_ && mset.noInterest(config.Delivery) {
		return nil, fmt.Errorf("observable requires interest for delivery subject when ephemeral")
	}

	// Hold mset lock here.
	mset.mu.Lock()

	// Check for any limits.
	if mset.config.MaxObservables > 0 && len(mset.obs) >= mset.config.MaxObservables {
		mset.mu.Unlock()
		return nil, fmt.Errorf("maximum observables limit reached")
	}

	// Check on msgset type conflicts.
	switch mset.config.Retention {
	case WorkQueuePolicy:
		if config.Delivery != "" {
			mset.mu.Unlock()
			return nil, fmt.Errorf("delivery subject not allowed on workqueue message set")
		}
		if len(mset.obs) > 0 {
			if config.Subject == _EMPTY_ {
				mset.mu.Unlock()
				return nil, fmt.Errorf("multiple non-partioned observables not allowed on workqueue message set")
			} else if !mset.partitionUnique(config.Subject) {
				// We have a partition but it is not unique amongst the others.
				mset.mu.Unlock()
				return nil, fmt.Errorf("partioned observable not unique on workqueue message set")
			}
		}
		if !config.DeliverAll {
			mset.mu.Unlock()
			return nil, fmt.Errorf("observable must be deliver all on workqueue message set")
		}
	}

	// Set name, which will be durable name if set, otherwise we create one at random.
	o := &Observable{mset: mset, config: *config, dsubj: config.Delivery, active: true, qch: make(chan struct{}), fch: make(chan struct{})}
	if isDurableObservable(config) {
		o.name = config.Durable
	} else {
		o.name = createObservableName()
	}

	store, err := mset.store.ObservableStore(o.name, config)
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
		return nil, fmt.Errorf("message set not valid")
	}
	s, a := c.srv, c.acc
	// Check if we already have this one registered.
	if eo, ok := mset.obs[o.name]; ok {
		mset.mu.Unlock()
		if !o.isDurable() || !o.isPushMode() {
			return nil, fmt.Errorf("observable already exists")
		}
		// If we are here we have already registered this durable. If it is still active that is an error.
		if eo.Active() {
			return nil, fmt.Errorf("observable already exists and is still active")
		}
		// Since we are here this means we have a potentially new durable so we should update here.
		// Check that configs are the same.
		if !configsEqualSansDelivery(o.config, eo.config) {
			return nil, fmt.Errorf("observable replacement durable config not the same")
		}
		// Once we are here we have a replacement push based durable.
		eo.updateDelivery(o.config.Delivery)
		return eo, nil
	}
	// Set up the ack subscription for this observable. Will use wildcard for all acks.
	// We will remember the template to generate replaies with sequence numbers and use
	// that to scanf them back in.
	mn := mset.config.Name
	o.ackReplyT = fmt.Sprintf("%s.%s.%s.%%d.%%d.%%d", JetStreamAckPre, mn, o.name)
	ackSubj := fmt.Sprintf("%s.%s.%s.*.*.*", JetStreamAckPre, mn, o.name)
	if sub, err := mset.subscribeInternal(ackSubj, o.processAck); err != nil {
		return nil, err
	} else {
		o.ackSub = sub
	}
	// Setup the internal sub for next message requests.
	if !o.isPushMode() {
		o.nextMsgSubj = fmt.Sprintf("%s.%s.%s", JetStreamRequestNextPre, mn, o.name)
		if sub, err := mset.subscribeInternal(o.nextMsgSubj, o.processNextMsgReq); err != nil {
			return nil, err
		} else {
			o.reqSub = sub
		}
	}
	mset.obs[o.name] = o
	mset.mu.Unlock()

	// If push mode, start up active hb timer.
	if o.isPushMode() {
		o.athresh = JsNotActiveThresholdDefault
		o.achk = JsActiveCheckIntervalDefault
		o.atmr = time.AfterFunc(o.achk, o.checkActive)
		// If durable and no interest mark as not active to start.
		if o.isDurable() && mset.noInterest(config.Delivery) {
			o.active = false
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

// Config returns the observable's configuration.
func (o *Observable) Config() ObservableConfig {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.config
}

func (o *Observable) updateDelivery(newDelivery string) {
	// Update the config and the dsubj
	o.mu.Lock()
	o.dsubj = newDelivery
	o.config.Delivery = newDelivery
	// FIXME(dlc) - check partitions, we may need offset.
	o.dseq = o.adflr
	o.sseq = o.asflr
	o.mu.Unlock()

	o.checkActive()
}

func configsEqualSansDelivery(a, b ObservableConfig) bool {
	// These were copied in so can set Delivery here.
	a.Delivery, b.Delivery = _EMPTY_, _EMPTY_
	return a == b
}

// Process a message for the ack reply subject delivered with a message.
func (o *Observable) processAck(_ *subscription, _ *client, subject, reply string, msg []byte) {
	sseq, dseq, _ := o.ReplyInfo(subject)
	switch {
	case len(msg) == 0, bytes.Equal(msg, AckAck):
		o.ackMsg(sseq, dseq)
	case bytes.Equal(msg, AckNext):
		o.ackMsg(sseq, dseq)
		o.processNextMsgReq(nil, nil, subject, reply, nil)
	case bytes.Equal(msg, AckNak):
		o.processNak(sseq, dseq)
	case bytes.Equal(msg, AckProgress):
		o.progressUpdate(sseq)
	}
}

// Used to process a working update to delay redelivery.
func (o *Observable) progressUpdate(seq uint64) {
	o.mu.Lock()
	if o.pending != nil {
		if _, ok := o.pending[seq]; ok {
			o.pending[seq] = time.Now().UnixNano()
		}
	}
	o.mu.Unlock()
}

// Process a NAK.
func (o *Observable) processNak(sseq, dseq uint64) {
	var mset *MsgSet
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
		mset.signalObservers()
	}
}

// This will restore the state from disk.
func (o *Observable) readStoredState() error {
	if o.store == nil {
		return nil
	}
	state, err := o.store.State()
	if err == nil && state != nil {
		// FIXME(dlc) - re-apply state.
		o.dseq = state.Delivered.ObsSeq
		o.sseq = state.Delivered.SetSeq
		o.adflr = state.AckFloor.ObsSeq
		o.asflr = state.AckFloor.SetSeq
		o.pending = state.Pending
		o.rdc = state.Redelivery
	}

	// Setup tracking timer if we have restored pending.
	if len(o.pending) > 0 && o.ptmr == nil {
		o.ptmr = time.AfterFunc(o.config.AckWait, o.checkPending)
	}
	return err
}

func (o *Observable) updateStateLoop() {
	o.mu.Lock()
	fch := o.fch
	qch := o.qch
	o.mu.Unlock()

	for {
		select {
		case <-qch:
			return
		case <-fch:
			time.Sleep(25 * time.Millisecond)
			o.mu.Lock()
			if o.store != nil {
				state := &ObservableState{
					Delivered: SequencePair{
						ObsSeq: o.dseq,
						SetSeq: o.sseq,
					},
					AckFloor: SequencePair{
						ObsSeq: o.adflr,
						SetSeq: o.asflr,
					},
					Pending:    o.pending,
					Redelivery: o.rdc,
				}
				// FIXME(dlc) - Hold onto any errors.
				o.store.Update(state)
			}
			o.mu.Unlock()
		}
	}
}

// Info returns our current observable state.
func (o *Observable) Info() *ObservableInfo {
	o.mu.Lock()
	defer o.mu.Unlock()
	info := &ObservableInfo{
		Config: o.config,
		State: ObservableState{
			Delivered: SequencePair{
				ObsSeq: o.dseq,
				SetSeq: o.sseq,
			},
			AckFloor: SequencePair{
				ObsSeq: o.adflr,
				SetSeq: o.asflr,
			},
		},
	}

	info.Config.Durable = o.name
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
		info.State.Redelivery = r
	}

	return info
}

// Will update the underlying store.
// Lock should be held.
func (o *Observable) updateStore() {
	if o.store == nil {
		return
	}
	// Kick our flusher
	select {
	case o.fch <- struct{}{}:
	default:
	}
}

// Process an ack for a message.
func (o *Observable) ackMsg(sseq, dseq uint64) {
	var sagap uint64
	o.mu.Lock()
	switch o.config.AckPolicy {
	case AckExplicit:
		delete(o.pending, sseq)
		if dseq == o.adflr+1 {
			o.adflr = dseq
			o.asflr = sseq
		}
		delete(o.rdc, sseq)
	case AckAll:
		// no-op
		if dseq <= o.adflr || sseq <= o.asflr {
			o.mu.Unlock()
			return
		}
		sagap = sseq - o.asflr
		o.adflr = dseq
		o.asflr = sseq
		// FIXME(dlc) - delete rdc entries?
	case AckNone:
		// FIXME(dlc) - This is error but do we care?
		o.mu.Unlock()
		return
	}
	o.updateStore()

	mset := o.mset
	o.mu.Unlock()

	// Let the owning message set know if we are interest or workqueue retention based.
	if mset != nil && mset.config.Retention != StreamPolicy {
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
func (o *Observable) needAck(sseq uint64) bool {
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
func (o *Observable) processNextMsgReq(_ *subscription, _ *client, _, reply string, msg []byte) {
	// Check payload here to see if they sent in batch size.
	batchSize := batchSizeFromMsg(msg)

	o.mu.Lock()
	mset := o.mset
	if mset == nil || o.isPushMode() {
		o.mu.Unlock()
		return
	}
	for i := 0; i < batchSize; i++ {
		// If we are in replay mode, defer to processReplay for delivery.
		if o.replay {
			o.waiting = append(o.waiting, reply)
		} else if subj, msg, seq, dc, err := o.getNextMsg(); err == nil {
			o.deliverMsg(reply, subj, msg, seq, dc)
		} else {
			o.waiting = append(o.waiting, reply)
		}
	}
	o.mu.Unlock()
}

// Increase the delivery count for this message.
// ONLY used on redelivery semantics.
// Lock should be held.
func (o *Observable) incDeliveryCount(sseq uint64) uint64 {
	if o.rdc == nil {
		o.rdc = make(map[uint64]uint64)
	}
	o.rdc[sseq] += 1
	return o.rdc[sseq] + 1
}

// Get next available message from underlying store.
// Is partition aware and redeliver aware.
// Lock should be held.
func (o *Observable) getNextMsg() (string, []byte, uint64, uint64, error) {
	if o.mset == nil {
		return _EMPTY_, nil, 0, 0, fmt.Errorf("observable not valid")
	}
	for {
		seq, dcount := o.sseq, uint64(1)
		if len(o.rdq) > 0 {
			seq = o.rdq[0]
			o.rdq = append(o.rdq[:0], o.rdq[1:]...)
			dcount = o.incDeliveryCount(seq)
		}
		subj, msg, _, err := o.mset.store.LoadMsg(seq)
		if err == nil {
			if dcount == 1 { // First delivery.
				o.sseq++
				if o.config.Subject != "" && subj != o.config.Subject {
					continue
				}
			}
			// We have the msg here.
			return subj, msg, seq, dcount, nil
		}
		// We got an error here. If this is an EOF we will return, otherwise
		// we can continue looking.
		if err == ErrStoreEOF {
			return "", nil, 0, 0, err
		}
		// Skip since its probably deleted or expired.
		o.sseq++
	}
}

// Returns if we should be doing a non-instant replay of stored messages.
func (o *Observable) needReplay() bool {
	o.mu.Lock()
	doReplay := o.replay
	o.mu.Unlock()
	return doReplay
}

func (o *Observable) clearReplayState() {
	o.mu.Lock()
	o.replay = false
	o.mu.Unlock()
}

// Wait for pull requests.
// FIXME(dlc) - for short wait periods is ok but should signal when waiting comes in.
func (o *Observable) waitForPullRequests(wait time.Duration) {
	o.mu.Lock()
	qch := o.qch
	if qch == nil || !o.isPullMode() || len(o.waiting) > 0 {
		wait = 0
	}
	o.mu.Unlock()

	select {
	case <-qch:
	case <-time.After(wait):
	}
}

// This function is responsible for message replay that is not instant/firehose.
func (o *Observable) processReplay() error {
	defer o.clearReplayState()

	o.mu.Lock()
	mset := o.mset
	partition := o.config.Subject
	pullMode := o.isPullMode()
	o.mu.Unlock()

	if mset == nil {
		return fmt.Errorf("observable not valid")
	}

	// Grab last queued up for us before we start.
	lseq := mset.Stats().LastSeq
	var lts int64 // last time stamp seen.

	// If we are in pull mode, wait up to the waittime to have
	// someone show up to start the replay.
	if pullMode {
		o.waitForPullRequests(time.Millisecond)
	}

	// Loop through all messages to replay.
	for {
		var delay time.Duration

		o.mu.Lock()
		mset = o.mset
		if mset == nil {
			o.mu.Unlock()
			return fmt.Errorf("observable not valid")
		}

		// If push mode but we have no interest wait for it to show up.
		if o.isPushMode() && !o.active {
			// We will wait here for new messages to arrive.
			o.mu.Unlock()
			mset.waitForMsgs()
			continue
		}

		subj, msg, ts, err := o.mset.store.LoadMsg(o.sseq)
		if err != nil && err != ErrStoreMsgNotFound {
			o.mu.Unlock()
			return err
		}

		if lts > 0 {
			if delay = time.Duration(ts - lts); delay > time.Millisecond {
				qch := o.qch
				o.mu.Unlock()
				select {
				case <-qch:
					return fmt.Errorf("observable not valid")
				case <-time.After(delay):
				}
				o.mu.Lock()
			}
		}
		// We have a message to deliver here.
		if err == nil && (partition == _EMPTY_ || subj == partition) {
			// FIXME(dlc) - pull based.
			if !pullMode {
				o.deliverMsg(o.dsubj, subj, msg, o.sseq, 1)
			} else {
				// This is pull mode. We should have folks waiting, but if not
				// just return and let the rest be delivered as needed.
				if len(o.waiting) > 0 {
					dsubj := o.waiting[0]
					o.waiting = append(o.waiting[:0], o.waiting[1:]...)
					o.deliverMsg(dsubj, subj, msg, o.sseq, 1)
				} else {
					lseq = o.sseq
				}
			}
			lts = ts
		}

		sseq := o.sseq
		o.sseq++
		o.mu.Unlock()

		if sseq >= lseq {
			break
		}
	}

	return nil
}

func (o *Observable) loopAndDeliverMsgs(s *Server, a *Account) {
	// On startup check to see if we are in a a reply situtation where replay policy is not instant.
	// Process the replay, return on error.
	if o.needReplay() && o.processReplay() != nil {
		return
	}

	// Deliver all the msgs we have now, once done or on a condition, we wait for new ones.
	for {
		var (
			mset        *MsgSet
			seq, dcnt   uint64
			subj, dsubj string
			msg         []byte
			err         error
		)

		o.mu.Lock()
		// observable is closed when mset is set to nil.
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
		if o.isPullMode() && len(o.waiting) == 0 {
			goto waitForMsgs
		}

		subj, msg, seq, dcnt, err = o.getNextMsg()

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

		o.deliverMsg(dsubj, subj, msg, seq, dcnt)

		o.mu.Unlock()
		continue

	waitForMsgs:
		// We will wait here for new messages to arrive.
		o.mu.Unlock()
		mset.waitForMsgs()
	}
}

func (o *Observable) ackReply(sseq, dseq, dcount uint64) string {
	return fmt.Sprintf(o.ackReplyT, dcount, sseq, dseq)
}

// deliverCurrentMsg is the hot path to deliver a message that was just received.
// Will return if the message was delivered or not.
func (o *Observable) deliverCurrentMsg(subj string, msg []byte, seq uint64) bool {
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
	if o.isPullMode() && len(o.waiting) == 0 {
		o.mu.Unlock()
		return false
	}

	// Bump store sequence here.
	o.sseq++

	// If we are partitioned and we do not match, do not consider this a failure.
	// Go ahead and return true.
	if o.config.Subject != "" && subj != o.config.Subject {
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

	o.deliverMsg(dsubj, subj, msg, seq, 1)
	o.mu.Unlock()

	return true
}

// Deliver a msg to the observable.
// Lock should be held and o.mset validated to be non-nil.
func (o *Observable) deliverMsg(dsubj, subj string, msg []byte, seq, dcount uint64) {
	o.mset.sendq <- &jsPubMsg{dsubj, subj, o.ackReply(seq, o.dseq, dcount), msg, o, o.dseq}
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
func (o *Observable) trackPending(seq uint64) {
	if o.pending == nil {
		o.pending = make(map[uint64]int64)
	}
	if o.ptmr == nil {
		o.ptmr = time.AfterFunc(o.config.AckWait, o.checkPending)
	}
	o.pending[seq] = time.Now().UnixNano()
}

// This will check if a registered delivery subject still has interest, e.g. subscriptions.
func (o *Observable) checkActive() {
	o.mu.Lock()
	mset := o.mset
	if mset == nil || o.isPullMode() {
		o.mu.Unlock()
		return
	}
	var shouldDelete, shouldSignal bool
	delivery := o.config.Delivery
	o.mu.Unlock()

	noInterest := o.mset.noInterest(delivery)

	o.mu.Lock()
	if o.mset == nil {
		o.mu.Unlock()
		return
	}

	if noInterest {
		o.active = false
		o.nointerest++
		if !o.isDurable() && o.nointerest >= o.athresh {
			shouldDelete = true
		}
	} else {
		// reset
		shouldSignal = !o.active
		o.active = true
		o.nointerest = 0
	}
	// Reset our timer here.
	o.atmr.Reset(o.achk)
	o.mu.Unlock()

	if shouldSignal {
		mset.signalObservers()
	}

	// This is for push based ephemerals.
	if shouldDelete {
		o.Delete()
	}
}

// didNotDeliver is called when a delivery for an observable message failed.
// Depending on our state, we will process the failure.
func (o *Observable) didNotDeliver(seq uint64) {
	o.mu.Lock()
	if o.mset == nil {
		o.mu.Unlock()
		return
	}
	if o.config.Delivery != _EMPTY_ {
		o.active = false
		o.nointerest++
	}

	// FIXME(dlc) - Other scenarios. Pull mode, etc.
	o.mu.Unlock()
}

// This checks if we already have this sequence queued for redelivery.
// FIXME(dlc) - This is O(n) but should be fast with small redeliver size.
// Lock should be held.
func (o *Observable) onRedeliverQueue(seq uint64) bool {
	for _, rseq := range o.rdq {
		if rseq == seq {
			return true
		}
	}
	return false
}

func (o *Observable) checkPending() {
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
		mset.signalObservers()
	}
}

// SeqFromReply will extract a sequence number from a reply subject.
func (o *Observable) SeqFromReply(reply string) uint64 {
	_, seq, _ := o.ReplyInfo(reply)
	return seq
}

// SetSeqFromReply will extract the message set sequence from the reply subject.
func (o *Observable) SetSeqFromReply(reply string) uint64 {
	seq, _, _ := o.ReplyInfo(reply)
	return seq
}

func (o *Observable) ReplyInfo(reply string) (sseq, dseq, dcount uint64) {
	n, err := fmt.Sscanf(reply, o.ackReplyT, &dcount, &sseq, &dseq)
	if err != nil || n != 3 {
		return 0, 0, 0
	}
	return
}

// NextSeq returns the next delivered sequence number for this observable.
func (o *Observable) NextSeq() uint64 {
	o.mu.Lock()
	dseq := o.dseq
	o.mu.Unlock()
	return dseq
}

// This will select the store seq to start with based on the
// partition subject.
func (o *Observable) selectSubjectLast() {
	stats := o.mset.store.Stats()
	// FIXME(dlc) - this is linear and can be optimized by store layer.
	for seq := stats.LastSeq; seq >= stats.FirstSeq; seq-- {
		subj, _, _, err := o.mset.store.LoadMsg(seq)
		if err == ErrStoreMsgNotFound {
			continue
		}
		if subj == o.config.Subject {
			o.sseq = seq
			return
		}
	}
}

// Will select the starting sequence.
func (o *Observable) selectStartingSeqNo() {
	stats := o.mset.store.Stats()
	noTime := time.Time{}
	if o.config.MsgSetSeq == 0 {
		if o.config.DeliverAll {
			o.sseq = stats.FirstSeq
		} else if o.config.DeliverLast {
			o.sseq = stats.LastSeq
			// If we are partitioned here we may need to walk backwards.
			if o.config.Subject != _EMPTY_ {
				o.selectSubjectLast()
			}
		} else if o.config.StartTime != noTime {
			// If we are here we are time based.
			// TODO(dlc) - Once clustered can't rely on this.
			o.sseq = o.mset.store.GetSeqFromTime(o.config.StartTime)
		} else {
			// Default is deliver new only.
			o.sseq = stats.LastSeq + 1
		}
	} else {
		o.sseq = o.config.MsgSetSeq
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
func isDurableObservable(config *ObservableConfig) bool {
	return config != nil && config.Durable != _EMPTY_
}

func (o *Observable) isDurable() bool {
	return o.config.Durable != _EMPTY_
}

// Are we in push mode, delivery subject, etc.
func (o *Observable) isPushMode() bool {
	return o.config.Delivery != _EMPTY_
}

func (o *Observable) isPullMode() bool {
	return o.config.Delivery == _EMPTY_
}

// Name returns the name of this observable.
func (o *Observable) Name() string {
	o.mu.Lock()
	n := o.name
	o.mu.Unlock()
	return n
}

// For now size of 6 for randomly created names.
const randObservableNameLen = 6

func createObservableName() string {
	var b [64]byte
	rand.Read(b[:])
	sha := sha256.New()
	sha.Write(b[:])
	return fmt.Sprintf("%x", sha.Sum(nil))[:randObservableNameLen]
}

// DeleteObservable will delete the observable from this message set.
func (mset *MsgSet) DeleteObservable(o *Observable) error {
	return o.Delete()
}

// Active indicates if this observable is still active.
func (o *Observable) Active() bool {
	o.mu.Lock()
	active := o.active && o.mset != nil
	o.mu.Unlock()
	return active
}

func stopAndClearTimer(tp **time.Timer) {
	if *tp == nil {
		return
	}
	(*tp).Stop()
	*tp = nil
}

// Stop will shutdown  the observable for the associated message set.
func (o *Observable) Stop() error {
	return o.stop(false)
}

// Delete will delete the observable for the associated message set.
func (o *Observable) Delete() error {
	return o.stop(true)
}

func (o *Observable) stop(dflag bool) error {
	o.mu.Lock()
	mset := o.mset
	if mset == nil {
		o.mu.Unlock()
		return nil
	}

	close(o.qch)

	if o.store != nil {
		o.store.Stop()
	}
	o.mset = nil
	o.active = false
	ackSub := o.ackSub
	reqSub := o.reqSub
	o.ackSub = nil
	o.reqSub = nil
	stopAndClearTimer(&o.ptmr)
	stopAndClearTimer(&o.atmr)

	o.mu.Unlock()

	mset.mu.Lock()
	// Break us out of the readLoop.
	// TODO(dlc) - Should not be bad for small amounts of observables, maybe
	// even into thousands. Above that should check what this might do
	// performance wise.
	mset.sg.Broadcast()
	mset.unsubscribe(ackSub)
	mset.unsubscribe(reqSub)
	delete(mset.obs, o.name)
	mset.mu.Unlock()

	return nil
}

// Check that we do not form a cycle by delivering to a delivery subject
// that is part of the interest group.
func (mset *MsgSet) deliveryFormsCycle(deliverySubject string) bool {
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
func (mset *MsgSet) validSubject(partitionSubject string) bool {
	return mset.deliveryFormsCycle(partitionSubject)
}

// SetActiveCheckParams allows a server to change the active check parameters
// for push based observables.
func (o *Observable) SetActiveCheckParams(achk time.Duration, thresh int) error {
	o.mu.Lock()
	if o.atmr == nil || o.isPullMode() {
		o.mu.Unlock()
		return fmt.Errorf("observable not push based")
	}
	o.achk = achk
	o.athresh = thresh
	stopAndClearTimer(&o.atmr)
	o.atmr = time.AfterFunc(o.achk, o.checkActive)
	o.mu.Unlock()
	return nil
}

// RequestNextMsgSubject returns the subject to request the next message when in pull or worker mode.
// Returns empty otherwise.1
func (o *Observable) RequestNextMsgSubject() string {
	return o.nextMsgSubj
}
