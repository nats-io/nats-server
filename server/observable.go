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
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type ObservableConfig struct {
	Delivery    string    `json:"delivery_subject"`
	Durable     string    `json:"durable_name,omitempty"`
	StartSeq    uint64    `json:"start_seq,omitempty"`
	StartTime   time.Time `json:"start_time,omitempty"`
	DeliverAll  bool      `json:"deliver_all,omitempty"`
	DeliverLast bool      `json:"deliver_last,omitempty"`
	AckPolicy   AckPolicy `json:"ack_policy"`
}

// AckPolicy determines how the observable shoulc acknowledge delivered messages.
type AckPolicy int

const (
	// AckNone requires no acks for delivered messages.
	AckNone AckPolicy = iota
	// AckExplicit requires ack or nack for all messages.
	AckExplicit
	// When acking a sequence number, this implicitly acks all sequences below this one as well.
	AckAll
)

// Observable is a jetstream observable/subscriber.
type Observable struct {
	mu       sync.Mutex
	name     string
	mset     *MsgSet
	seq      uint64
	dsubj    string
	reqSub   *subscription
	ackSub   *subscription
	ackReply string
	waiting  []string
	config   ObservableConfig
}

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
	mset.mu.Lock()
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
	if sub, err := mset.subscribeInternal(ackSubj, o.processObservableAck); err != nil {
		return nil, err
	} else {
		o.ackSub = sub
	}
	// Setup the internal sub for individual message requests.
	reqSubj := fmt.Sprintf("%s.%s.%s", JsReqPre, cn, o.name)
	if sub, err := mset.subscribeInternal(reqSubj, o.processObservableMsgRequest); err != nil {
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

func (o *Observable) processObservableAck(_ *subscription, _ *client, subject, _ string, msg []byte) {
	// No-op for now.
}

func (o *Observable) processObservableMsgRequest(_ *subscription, _ *client, subject, reply string, msg []byte) {
	o.mu.Lock()
	mset := o.mset
	if mset == nil {
		o.mu.Unlock()
		// FIXME(dlc) - send err?
		return
	}
	// Determine which sequence number they are looking for. nil request means next message.
	wantNextMsg := len(msg) == 0
	var seq uint64

	if wantNextMsg {
		seq = o.seq
	}
	// FIXME(dlc) - do actual sequence numbers.
	subj, msg, _, err := mset.store.Lookup(seq)
	if err == nil {
		o.deliverMsgRequest(mset, reply, subj, msg, seq)
		if seq == o.seq {
			o.seq++
		}
	} else if wantNextMsg {
		o.waiting = append(o.waiting, reply)
	}
	o.mu.Unlock()
}

func (o *Observable) loopAndDeliverMsgs(s *Server, a *Account) {
	var mset *MsgSet
	for {
		// observable is closed when mset is set to nil.
		if mset = o.msgSet(); mset == nil {
			return
		}

		// Deliver all the msgs we have now, once done or on a condition, we wait.
		for {
			o.mu.Lock()
			seq := o.seq
			subj, msg, _, err := mset.store.Lookup(seq)

			if err != nil {
				o.mu.Unlock()
				if err != ErrStoreMsgNotFound {
					s.Warnf("Jetstream internal storage error on lookup: %v", err)
					return
				}
				break
			}

			// We have the message. We need to check if we are in push mode or pull mode.
			if o.config.Delivery != "" {
				o.deliverMsg(mset, subj, msg, seq)
				o.seq++
			} else if len(o.waiting) > 0 {
				reply := o.waiting[0]
				o.waiting = append(o.waiting[:0], o.waiting[1:]...)
				o.deliverMsgRequest(mset, reply, subj, msg, seq)
				o.seq++
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

// Deliver a msg to the observable push delivery subject.
func (o *Observable) deliverMsg(mset *MsgSet, subj string, msg []byte, seq uint64) {
	mset.sendq <- &jsPubMsg{o.dsubj, subj, fmt.Sprintf(o.ackReply, seq), msg}
}

// Deliver a msg to the msg request subject.
func (o *Observable) deliverMsgRequest(mset *MsgSet, dsubj, subj string, msg []byte, seq uint64) {
	mset.sendq <- &jsPubMsg{dsubj, subj, fmt.Sprintf(o.ackReply, seq), msg}
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
	return atomic.LoadUint64(&o.seq)
}

// Will select the starting sequence.
func (o *Observable) selectStartingSeqNo() {
	stats := o.mset.Stats()
	noTime := time.Time{}
	if o.config.StartSeq == 0 {
		if o.config.DeliverAll {
			o.seq = stats.FirstSeq
		} else if o.config.DeliverLast {
			o.seq = stats.LastSeq
		} else if o.config.StartTime != noTime {
			// If we are here we are time based.
			// TODO(dlc) - Once clustered can't rely on this.
			o.seq = o.mset.store.GetSeqFromTime(o.config.StartTime)
		} else {
			// Default is deliver new only.
			o.seq = stats.LastSeq + 1
		}
	} else {
		o.seq = o.config.StartSeq
	}

	if stats.FirstSeq == 0 {
		o.seq = 1
	} else if o.seq < stats.FirstSeq {
		o.seq = stats.FirstSeq
	} else if o.seq > stats.LastSeq {
		o.seq = stats.LastSeq + 1
	}
}

// Test whether a config represents a durable subscriber.
func isDurableObservable(config *ObservableConfig) bool {
	return config != nil && config.Durable != _EMPTY_
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
