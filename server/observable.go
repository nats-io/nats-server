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

type ObservableConfig struct {
	Delivery    string        `json:"delivery_subject"`
	Durable     string        `json:"durable_name,omitempty"`
	MsgSetSeq   uint64        `json:"msg_set_seq,omitempty"`
	StartTime   time.Time     `json:"start_time,omitempty"`
	DeliverAll  bool          `json:"deliver_all,omitempty"`
	DeliverLast bool          `json:"deliver_last,omitempty"`
	AckPolicy   AckPolicy     `json:"ack_policy"`
	AckWait     time.Duration `json:"ack_wait"`
	Partition   string        `json:"partition"`
}

// AckPolicy determines how the observable should acknowledge delivered messages.
type AckPolicy int

const (
	// AckNone requires no acks for delivered messages.
	AckNone AckPolicy = iota
	// When acking a sequence number, this implicitly acks all sequences below this one as well.
	AckAll
	// AckExplicit requires ack or nack for all messages.
	AckExplicit
)

// Ack responses. Note that a nil or no payload is same as AckAck
var (
	// Ack
	AckAck = []byte(JsOK) // nil or no payload to ack subject also means ACK
	// Nack
	AckNak = []byte("-NAK")
	// Progress indicator
	AckProgress = []byte("+WPI")
	// Ack + deliver next.
	AckNext = []byte("+NXT")
)

// Observable is a jetstream observable/subscriber.
type Observable struct {
	mu         sync.Mutex
	mset       *MsgSet
	name       string
	sseq       uint64
	dseq       uint64
	aflr       uint64
	soff       uint64
	dsubj      string
	reqSub     *subscription
	ackSub     *subscription
	ackReplyT  string
	pending    map[uint64]int64
	ptmr       *time.Timer
	redeliver  []uint64
	waiting    []string
	config     ObservableConfig
	active     bool
	atmr       *time.Timer
	nointerest int
	athresh    int
	achk       time.Duration
}

const (
	// Default AckWait, only applicable on explicit ack policy observables.
	JsAckWaitDefault = 30 * time.Second
	// JsActiveCheckInterval is default hb interval for push based observables.
	JsActiveCheckIntervalDefault = time.Second
	// JsNotActiveThreshold is number of times we detect no interest to close an observable if ephemeral.
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
	if config.Partition != "" {
		if !subjectIsLiteral(config.Partition) {
			return nil, fmt.Errorf("observable partition subject has wildcards")
		}
		// Make sure this is a valid partition of the interest subjects.
		if !mset.validPartition(config.Partition) {
			return nil, fmt.Errorf("observable partition not a valid subset of the interest subjects")
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

	// Hold mset lock here/
	mset.mu.Lock()

	// Check on msgset type conflicts.
	switch mset.config.Retention {
	case WorkQueuePolicy:
		if config.Delivery != "" {
			mset.mu.Unlock()
			return nil, fmt.Errorf("delivery subject not allowed on workqueue message set")
		}
		if len(mset.obs) > 0 {
			if config.Partition == _EMPTY_ {
				mset.mu.Unlock()
				return nil, fmt.Errorf("multiple non-partioned observables not allowed on workqueue message set")
			} else if !mset.partitionUnique(config.Partition) {
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
	o := &Observable{mset: mset, config: *config, dsubj: config.Delivery, active: true}
	if isDurableObservable(config) {
		o.name = config.Durable
	} else {
		o.name = createObservableName()
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
	cn := mset.cleanName()
	o.ackReplyT = fmt.Sprintf("%s.%s.%s.%%1s.%%d.%%d", JsAckPre, cn, o.name)
	ackSubj := fmt.Sprintf("%s.%s.%s.*.*.*", JsAckPre, cn, o.name)
	if sub, err := mset.subscribeInternal(ackSubj, o.processAck); err != nil {
		return nil, err
	} else {
		o.ackSub = sub
	}
	// Setup the internal sub for next message requests.
	if !o.isPushMode() {
		reqSubj := fmt.Sprintf("%s.%s.%s", JsReqPre, cn, o.name)
		if sub, err := mset.subscribeInternal(reqSubj, o.processNextMsgReq); err != nil {
			return nil, err
		} else {
			o.reqSub = sub
		}
	}
	mset.obs[o.name] = o
	mset.mu.Unlock()

	// If push mode, start up active hb timer.
	if o.isPushMode() {
		o.mu.Lock()
		o.athresh = JsNotActiveThresholdDefault
		o.achk = JsActiveCheckIntervalDefault
		o.atmr = time.AfterFunc(o.achk, o.checkActive)
		// If durable and no interest mark as not active to start.
		if o.isDurable() && mset.noInterest(config.Delivery) {
			o.active = false
		}
		o.mu.Unlock()
	}

	// Now start up Go routine to deliver msgs.
	go o.loopAndDeliverMsgs(s, a)

	return o, nil
}

func (o *Observable) updateDelivery(newDelivery string) {
	// Update the config and the dsubj
	o.mu.Lock()
	o.dsubj = newDelivery
	o.config.Delivery = newDelivery
	// FIXME(dlc) - check partitions, think we need offset.
	o.dseq = o.aflr
	o.sseq = o.aflr
	o.mu.Unlock()

	o.checkActive()
}

func configsEqualSansDelivery(a, b ObservableConfig) bool {
	// These were copied in so can set Delivery here.
	a.Delivery, b.Delivery = _EMPTY_, _EMPTY_
	return a == b
}

func (o *Observable) processAck(_ *subscription, _ *client, subject, reply string, msg []byte) {
	sseq, dseq, _ := o.ReplyInfo(subject)
	switch {
	case len(msg) == 0, bytes.Equal(msg, AckAck):
		o.ackMsg(sseq, dseq)
	case bytes.Equal(msg, AckNext):
		o.ackMsg(sseq, dseq)
		o.processNextMsgReq(nil, nil, subject, reply, nil)
	case bytes.Equal(msg, AckNak):
		if o.isPushMode() && o.config.AckPolicy != AckExplicit {
			// Reset our observable to this sequence number.
			o.resetToSeq(sseq, dseq)
		} else {
			// Queue up this message for redelivery
			o.queueForRedelivery(sseq)
		}
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

// queueForRedelivery will queue up a message for redelivery.
func (o *Observable) queueForRedelivery(seq uint64) {
	var mset *MsgSet
	o.mu.Lock()
	if !o.onRedeliverList(seq) {
		o.redeliver = append(o.redeliver, seq)
	}
	mset = o.mset
	o.mu.Unlock()
	if mset != nil {
		mset.signalObservers()
	}
}

// Process an ack for a message.
func (o *Observable) ackMsg(sseq, dseq uint64) {
	o.mu.Lock()
	switch o.config.AckPolicy {
	case AckNone, AckAll:
		o.aflr = dseq
	case AckExplicit:
		delete(o.pending, sseq)
		if dseq == o.aflr+1 {
			o.aflr++
		}
	}
	mset := o.mset
	o.mu.Unlock()

	if mset != nil && mset.config.Retention == WorkQueuePolicy {
		mset.ackMsg(sseq)
	}
}

// resetToSeq is used when we receive a NAK to reset a push based observable. e.g. a replay.
func (o *Observable) resetToSeq(sseq, dseq uint64) {
	o.mu.Lock()
	o.sseq, o.dseq = sseq, dseq
	mset := o.mset
	o.mu.Unlock()

	if mset != nil {
		mset.signalObservers()
	}
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
		if subj, msg, seq, rd, err := o.getNextMsg(); err == nil {
			o.deliverMsg(reply, subj, msg, seq, rd)
		} else {
			o.waiting = append(o.waiting, reply)
		}
	}
	o.mu.Unlock()
}

// Get next available message from underlying store.
// Is partition aware and redeliver aware.
// Lock should be held.
func (o *Observable) getNextMsg() (string, []byte, uint64, bool, error) {
	if o.mset == nil {
		return _EMPTY_, nil, 0, false, fmt.Errorf("message set not valid")
	}
	for {
		seq, rd := o.sseq, false
		if len(o.redeliver) > 0 {
			seq = o.redeliver[0]
			o.redeliver = append(o.redeliver[:0], o.redeliver[1:]...)
			rd = true
		}
		subj, msg, _, err := o.mset.store.Lookup(seq)
		if err == nil {
			if !rd {
				o.sseq++
				if o.config.Partition != "" && subj != o.config.Partition {
					continue
				}
			}
			// We have the msg here.
			return subj, msg, seq, rd, nil
		}
		// We got an error here.
		// If this was a redelivery the message may have expired so move on to next one.
		if !rd {
			return "", nil, 0, false, err
		}
	}
}

func (o *Observable) loopAndDeliverMsgs(s *Server, a *Account) {
	// Deliver all the msgs we have now, once done or on a condition, we wait for new ones.
	for {
		var (
			mset      *MsgSet
			seq       uint64
			subj      string
			dsubj     string
			msg       []byte
			err       error
			redeliver bool
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

		subj, msg, seq, redeliver, err = o.getNextMsg()

		// On error either wait or return.
		if err != nil {
			if err == ErrStoreMsgNotFound {
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

		o.deliverMsg(dsubj, subj, msg, seq, redeliver)

		o.mu.Unlock()
		continue

	waitForMsgs:
		// We will wait here for new messages to arrive.
		o.mu.Unlock()
		mset.waitForMsgs()
	}
}

const JsOriginalDelivery = "O"
const JsReDelivery = "R"

func (o *Observable) ackReply(sseq, dseq uint64, redelivery bool) string {
	dm := JsOriginalDelivery
	if redelivery {
		dm = JsReDelivery
	}
	return fmt.Sprintf(o.ackReplyT, dm, sseq, dseq)
}

//func (o *Observable) ackRedeliverReply(sseq, dseq uint64) string {
//}

// Deliver a msg to the observable.
// Lock should be held and o.mset validated to be non-nil.
func (o *Observable) deliverMsg(dsubj, subj string, msg []byte, seq uint64, redelivery bool) {
	o.mset.sendq <- &jsPubMsg{dsubj, subj, o.ackReply(seq, o.dseq, redelivery), msg, o, o.dseq}
	if o.config.AckPolicy == AckExplicit {
		o.trackPending(seq)
	}
	o.dseq++
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

func (o *Observable) checkActive() {
	o.mu.Lock()
	mset := o.mset
	if mset == nil || o.isPullMode() {
		o.mu.Unlock()
		return
	}
	var shouldDelete, shouldSignal bool
	if o.mset.noInterest(o.config.Delivery) {
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
func (o *Observable) onRedeliverList(seq uint64) bool {
	for _, rseq := range o.redeliver {
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
		if now-ts > aw && !o.onRedeliverList(seq) {
			expired = append(expired, seq)
			shouldSignal = true
		}
	}
	if len(expired) > 0 {
		sort.Slice(expired, func(i, j int) bool { return expired[i] < expired[j] })
		o.redeliver = append(o.redeliver, expired...)
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

// SeqFromReply will extract a sequence number from a reply ack subject.
func (o *Observable) SeqFromReply(reply string) uint64 {
	_, seq, _ := o.ReplyInfo(reply)
	return seq
}

func (o *Observable) ReplyInfo(reply string) (sseq, dseq uint64, redelivered bool) {
	var ri string
	n, err := fmt.Sscanf(reply, o.ackReplyT, &ri, &sseq, &dseq)
	if err != nil || n != 3 {
		return 0, 0, false
	}
	redelivered = ri == JsReDelivery
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
func (o *Observable) selectPartitionLast() {
	stats := o.mset.Stats()
	// FIXME(dlc) - this is linear and can be optimized by store layer.
	for seq := stats.LastSeq; seq >= stats.FirstSeq; seq-- {
		subj, _, _, err := o.mset.store.Lookup(seq)
		if err == ErrStoreMsgNotFound {
			continue
		}
		if subj == o.config.Partition {
			o.sseq = seq
			return
		}
	}
}

// Will select the starting sequence.
func (o *Observable) selectStartingSeqNo() {
	stats := o.mset.Stats()
	noTime := time.Time{}
	if o.config.MsgSetSeq == 0 {
		if o.config.DeliverAll {
			o.sseq = stats.FirstSeq
		} else if o.config.DeliverLast {
			o.sseq = stats.LastSeq
			// If we are partitioned here we may need to walk backwards.
			if o.config.Partition != _EMPTY_ {
				o.selectPartitionLast()
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
	o.soff = o.sseq - 1
	// Set ack floor to delivery - 1
	o.aflr = o.dseq - 1
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

// Delete will delete the observable for the associated message set.
func (o *Observable) Delete() error {
	o.mu.Lock()
	// TODO(dlc) - Do cleanup here.
	mset := o.mset
	o.mset = nil
	o.active = false
	ackSub := o.ackSub
	reqSub := o.reqSub
	o.ackSub = nil
	o.reqSub = nil
	stopAndClearTimer(&o.ptmr)
	stopAndClearTimer(&o.atmr)
	o.mu.Unlock()

	if mset == nil {
		return nil
	}

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

// Checks to see if there is registered interest in the delivery subject.
// Note that since we require delivery to be a literal this is just like
// a publish match.
func (mset *MsgSet) noInterest(delivery string) bool {
	var c *client
	var acc *Account

	mset.mu.Lock()
	if mset.client != nil {
		c = mset.client
		acc = c.acc
	}
	mset.mu.Unlock()
	if acc == nil {
		return true
	}
	r := acc.sl.Match(delivery)
	interest := len(r.psubs)+len(r.qsubs) > 0

	// Check for service imports here.
	if !interest && acc.imports.services != nil {
		acc.mu.RLock()
		si := acc.imports.services[delivery]
		invalid := si != nil && si.invalid
		acc.mu.RUnlock()
		if si != nil && !invalid && si.acc != nil && si.acc.sl != nil {
			rr := si.acc.sl.Match(si.to)
			interest = len(rr.psubs)+len(rr.qsubs) > 0
		}
	}
	// Process GWs here. This is not going to exact since it could be that the GW does not
	// know, that is ok for here.
	// TODO(@@IK) to check.
	if !interest && (c != nil && c.srv != nil && c.srv.gateway.enabled) {
		gw := c.srv.gateway
		gw.RLock()
		for _, gwc := range gw.outo {
			psi, qr := gwc.gatewayInterest(acc.Name, delivery)
			if psi || qr != nil {
				interest = true
				break
			}
		}
		gw.RUnlock()
	}
	return !interest
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
func (mset *MsgSet) validPartition(partitionSubject string) bool {
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
