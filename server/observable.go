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
	"strconv"
	"sync"
	"time"
)

type ObservableConfig struct {
	Delivery    string        `json:"delivery_subject"`
	Durable     string        `json:"durable_name,omitempty"`
	StartSeq    uint64        `json:"start_seq,omitempty"`
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

// Observable is a jetstream observable/subscriber.
type Observable struct {
	mu        sync.Mutex
	name      string
	mset      *MsgSet
	pseq      uint64
	sseq      uint64
	dseq      uint64
	dsubj     string
	reqSub    *subscription
	ackSub    *subscription
	ackReply  string
	pending   map[uint64]int64
	ptmr      *time.Timer
	redeliver []uint64
	waiting   []string
	config    ObservableConfig
}

// Default AckWait, only applicable on explicit ack policy observables.
const JsAckWaitDefault = 30 * time.Second

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
		if config.AckWait == time.Duration(0) {
			config.AckWait = JsAckWaitDefault
		}
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
	if config.StartSeq > 0 && (config.StartTime != noTime || config.DeliverAll || config.DeliverLast) {
		return nil, fmt.Errorf("observable starting position conflict")
	} else if config.StartTime != noTime && (config.DeliverAll || config.DeliverLast) {
		return nil, fmt.Errorf("observable starting position conflict")
	} else if config.DeliverAll && config.DeliverLast {
		return nil, fmt.Errorf("observable starting position conflict")
	}

	// Check if we are not durable that the delivery subject has interest.
	if config.Durable == _EMPTY_ && config.Delivery != _EMPTY_ {
		if mset.noInterest(config.Delivery) {
			return nil, fmt.Errorf("observable requires interest for delivery subject when ephemeral")
		}
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
	o := &Observable{mset: mset, config: *config, dsubj: config.Delivery}
	if isDurableObservable(config) {
		o.name = config.Durable
	} else {
		o.name = createObservableName()
	}

	// Select starting sequence number
	o.selectStartingSeqNo()

	// Now register with mset and create ack subscription.
	c := mset.client
	if c == nil {
		mset.mu.Unlock()
		return nil, fmt.Errorf("message set not valid")
	}
	s, a := c.srv, c.acc
	if _, ok := mset.obs[o.name]; ok {
		mset.mu.Unlock()
		return nil, fmt.Errorf("observable already exists")
	}
	// Set up the ack subscription for this observable. Will use wildcard for all acks.
	// We will remember the template to generate replaies with sequence numbers and use
	// that to scanf them back in.
	cn := mset.cleanName()
	o.ackReply = fmt.Sprintf("%s.%s.%s.%%d", JsAckPre, cn, o.name)
	ackSubj := fmt.Sprintf("%s.%s.%s.*", JsAckPre, cn, o.name)
	if sub, err := mset.subscribeInternal(ackSubj, o.processAck); err != nil {
		return nil, err
	} else {
		o.ackSub = sub
	}
	// Setup the internal sub for individual message requests.
	reqSubj := fmt.Sprintf("%s.%s.%s", JsReqPre, cn, o.name)
	if sub, err := mset.subscribeInternal(reqSubj, o.processNextMsgReq); err != nil {
		return nil, err
	} else {
		o.reqSub = sub
	}
	mset.obs[o.name] = o
	mset.mu.Unlock()

	// Now start up Go routine to deliver msgs.
	go o.loopAndDeliverMsgs(s, a)

	return o, nil
}

func (o *Observable) msgSet() *MsgSet {
	o.mu.Lock()
	mset := o.mset
	o.mu.Unlock()
	return mset
}

func (o *Observable) processAck(_ *subscription, _ *client, subject, reply string, msg []byte) {
	seq := o.SeqFromReply(subject)
	switch {
	case len(msg) == 0, bytes.Equal(msg, AckAck):
		o.ackMsg(seq)
	case bytes.Equal(msg, AckNext):
		o.ackMsg(seq)
		o.processNextMsgReq(nil, nil, subject, reply, nil)
	case bytes.Equal(msg, AckNak):
		if o.isPushMode() {
			// Reset our observable to this sequence number.
			o.resetToSeq(seq)
		}
	}
}

// Process an ack for a message.
func (o *Observable) ackMsg(seq uint64) {
	o.mu.Lock()
	switch o.config.AckPolicy {
	case AckNone, AckAll:
		o.pseq = seq
	case AckExplicit:
		delete(o.pending, seq)
		if seq == o.pseq+1 {
			o.pseq++
		}
	}
	mset := o.mset
	o.mu.Unlock()
	if mset != nil && mset.config.Retention == WorkQueuePolicy {
		mset.ackMsg(seq)
	}
}

// resetToSeq is used when we receive a NAK to reset a push based observable. e.g. a replay.
func (o *Observable) resetToSeq(seq uint64) {
	o.mu.Lock()
	o.sseq, o.dseq = seq, seq
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
	if mset == nil {
		o.mu.Unlock()
		return
	}
	for i := 0; i < batchSize; i++ {
		if len(o.redeliver) > 0 {
			seq := o.redeliver[0]
			o.redeliver = append(o.redeliver[:0], o.redeliver[1:]...)
			subj, msg, _, err := mset.store.Lookup(seq)
			if err == ErrStoreMsgNotFound {
				continue
			}
			o.reDeliverMsgRequest(mset, reply, subj, msg, seq)
		} else if subj, msg, err := o.getNextMsg(); err == nil {
			o.deliverMsgRequest(o.mset, reply, subj, msg, o.dseq)
			o.incSeqs()
		} else {
			o.waiting = append(o.waiting, reply)
		}
	}
	o.mu.Unlock()
}

// Get next available message from underlying store.
// Is partition aware.
// Lock should be held.
func (o *Observable) getNextMsg() (string, []byte, error) {
	if o.mset == nil {
		return "", nil, fmt.Errorf("message set not valid")
	}
	for {
		subj, msg, _, err := o.mset.store.Lookup(o.sseq)
		if err == nil {
			if o.config.Partition != "" && subj != o.config.Partition {
				o.sseq++
				continue
			}
			// We have the msg here.
			return subj, msg, nil
		}
		// We got an error here.
		return "", nil, err
	}
}

func (o *Observable) loopAndDeliverMsgs(s *Server, a *Account) {
	var mset *MsgSet
	for {
		// observable is closed when mset is set to nil.
		if mset = o.msgSet(); mset == nil {
			return
		}

		// Deliver all the msgs we have now, once done or on a condition, we wait for new ones.
		for {
			o.mu.Lock()
			var seq uint64
			var redelivery bool

			if len(o.redeliver) > 0 {
				seq = o.redeliver[0]
				redelivery = true
			} else {
				seq = o.sseq
			}

			subj, msg, _, err := mset.store.Lookup(seq)

			// On error either break or return.
			if err != nil {
				o.mu.Unlock()
				if err != ErrStoreMsgNotFound {
					s.Warnf("Jetstream internal storage error on lookup: %v", err)
					return
				}
				if !redelivery {
					break
				} else {
					// This was not found so can't be redelivered.
					o.redeliver = append(o.redeliver[:0], o.redeliver[1:]...)
				}
			}

			// We have the message. We need to check if we are in push mode or pull mode.
			// Also need to check if we have a partition filter.
			if o.config.Partition != "" && subj != o.config.Partition {
				o.sseq++
				o.mu.Unlock()
				continue
			}

			if o.isPushMode() {
				if !redelivery {
					o.deliverMsg(mset, subj, msg, o.dseq)
					o.incSeqs()
				} else {
					o.redeliver = append(o.redeliver[:0], o.redeliver[1:]...)
					o.deliverMsg(mset, subj, msg, seq)
				}
			} else if len(o.waiting) > 0 {
				reply := o.waiting[0]
				o.waiting = append(o.waiting[:0], o.waiting[1:]...)
				if !redelivery {
					o.deliverMsgRequest(mset, reply, subj, msg, o.dseq)
					o.incSeqs()
				} else {
					o.redeliver = append(o.redeliver[:0], o.redeliver[1:]...)
					o.reDeliverMsgRequest(mset, reply, subj, msg, seq)
				}
			} else {
				// No one waiting, let's break out and wait.
				o.mu.Unlock()
				break
			}
			o.mu.Unlock()
		}
		// We will wait here for new messages to arrive.
		mset.waitForMsgs()
	}
}

// Advance the sequence numbers.
// Lock should be held.
func (o *Observable) incSeqs() {
	o.sseq++
	o.dseq++
}

// Deliver a msg to the observable push delivery subject.
func (o *Observable) deliverMsg(mset *MsgSet, subj string, msg []byte, seq uint64) {
	mset.sendq <- &jsPubMsg{o.dsubj, subj, fmt.Sprintf(o.ackReply, seq), msg}
}

// Deliver a msg to the msg request subject.
func (o *Observable) deliverMsgRequest(mset *MsgSet, dsubj, subj string, msg []byte, seq uint64) {
	mset.sendq <- &jsPubMsg{dsubj, subj, fmt.Sprintf(o.ackReply, seq), msg}
	if o.config.AckPolicy == AckExplicit {
		o.trackPending(seq)
	}
}

// Redeliver a message.
func (o *Observable) reDeliverMsgRequest(mset *MsgSet, dsubj, subj string, msg []byte, seq uint64) {
	mset.sendq <- &jsPubMsg{dsubj, subj, fmt.Sprintf(o.ackReply, seq), msg}
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
	for seq := o.pseq; seq < o.dseq; seq++ {
		if ts, ok := o.pending[seq]; ok {
			if now-ts > aw {
				// If we have waiting, go ahead and deliver here.
				// FIXME(dlc) - Not sure this is correct.
				o.redeliver = append(o.redeliver, seq)
				shouldSignal = true
			} else {
				break
			}
		}
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
func (o *Observable) SeqFromReply(reply string) (seq uint64) {
	n, err := fmt.Sscanf(reply, o.ackReply, &seq)
	if err != nil || n != 1 {
		return 0
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

// Will select the starting sequence.
func (o *Observable) selectStartingSeqNo() {
	stats := o.mset.Stats()
	noTime := time.Time{}
	if o.config.StartSeq == 0 {
		if o.config.DeliverAll {
			o.sseq = stats.FirstSeq
		} else if o.config.DeliverLast {
			o.sseq = stats.LastSeq
		} else if o.config.StartTime != noTime {
			// If we are here we are time based.
			// TODO(dlc) - Once clustered can't rely on this.
			o.sseq = o.mset.store.GetSeqFromTime(o.config.StartTime)
		} else {
			// Default is deliver new only.
			o.sseq = stats.LastSeq + 1
		}
	} else {
		o.sseq = o.config.StartSeq
	}

	if stats.FirstSeq == 0 {
		o.sseq = 1
	} else if o.sseq < stats.FirstSeq {
		o.sseq = stats.FirstSeq
	} else if o.sseq > stats.LastSeq {
		o.sseq = stats.LastSeq + 1
	}
	// Set delivery sequence to be the same to start.
	o.dseq = o.sseq
	// Set pending sequence to delivery - 1
	o.pseq = o.dseq - 1
}

// Test whether a config represents a durable subscriber.
func isDurableObservable(config *ObservableConfig) bool {
	return config != nil && config.Durable != _EMPTY_
}

// Are we in push mode, delivery subject, etc.
func (o *Observable) isPushMode() bool {
	return o.config.Delivery != _EMPTY_
}

// Name returns the name of this observable.
func (o *Observable) Name() string {
	o.mu.Lock()
	n := o.name
	o.mu.Unlock()
	return n
}

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
	return o.msgSet() != nil
}

// Delete will delete the observable for the associated message set.
func (o *Observable) Delete() error {
	o.mu.Lock()
	// TODO(dlc) - Do cleanup here.
	mset := o.mset
	o.mset = nil
	ackSub := o.ackSub
	reqSub := o.reqSub
	o.ackSub = nil
	o.reqSub = nil
	if o.ptmr != nil {
		o.ptmr.Stop()
		o.ptmr = nil
	}
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
//
// TODO(dlc) - if gateways are enabled we need to do some more digging for the
// real answer.
func (mset *MsgSet) noInterest(delivery string) bool {
	var acc *Account
	mset.mu.Lock()
	if mset.client != nil {
		acc = mset.client.acc
	}
	mset.mu.Unlock()
	if acc == nil {
		return true
	}
	r := acc.sl.Match(delivery)
	return len(r.psubs)+len(r.qsubs) == 0
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
