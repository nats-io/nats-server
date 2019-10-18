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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MsgSetConfig will determine the name, subjects and retention policy
// for a given message set. If subjects is empty the name will be used.
type MsgSetConfig struct {
	Name      string
	Subjects  []string
	Retention RetentionPolicy
	MaxMsgs   uint64
	MaxBytes  uint64
	MaxAge    time.Duration
	Storage   StorageType
	Replicas  int
	NoAck     bool
}

// RetentionPolicy determines how messages in a set are retained.
type RetentionPolicy int

const (
	// StreamPolicy (default) means that messages are retained until any possible given limit is reached.
	// This could be any one of MaxMsgs, MaxBytes, or MaxAge.
	StreamPolicy RetentionPolicy = iota
	// InterestPolicy specifies that when all known observables have acknowledged a message it can be removed.
	InterestPolicy
	// WorkQueuePolicy specifies that when the first worker or subscriber acknowledges the message it can be removed.
	WorkQueuePolicy
)

// MsgSet is a jetstream message set. When we receive a message internally destined
// for a MsgSet we will direct link from the client to this MsgSet structure.
type MsgSet struct {
	mu     sync.Mutex
	sg     *sync.Cond
	sgw    int
	jsa    *jsAccount
	client *client
	sid    int
	sendq  chan *jsPubMsg
	store  MsgSetStore
	obs    map[string]*Observable
	config MsgSetConfig
}

const (
	MsgSetDefaultReplicas = 1
	MsgSetMaxReplicas     = 8
)

// JetStreamAddMsgSet adds a message set for the given account.
func (s *Server) JetStreamAddMsgSet(a *Account, config *MsgSetConfig) (*MsgSet, error) {
	js := s.getJetStream()
	if js == nil {
		return nil, fmt.Errorf("jetstream not enabled")
	}

	jsa := js.lookupAccount(a)
	if jsa == nil {
		return nil, fmt.Errorf("jetstream not enabled for account")
	}

	// TODO(dlc) - check config for conflicts, e.g replicas > 1 in single server mode.
	if config.Replicas == 0 {
		config.Replicas = 1
	}
	if config.Replicas > MsgSetMaxReplicas {
		return nil, fmt.Errorf("maximum replicas is %d", MsgSetMaxReplicas)
	}

	jsa.mu.Lock()
	if _, ok := jsa.msgSets[config.Name]; ok {
		jsa.mu.Unlock()
		return nil, fmt.Errorf("message set name already taken")
	}
	c := s.createInternalJetStreamClient()
	mset := &MsgSet{jsa: jsa, config: *config, client: c, obs: make(map[string]*Observable)}
	mset.sg = sync.NewCond(&mset.mu)

	if len(mset.config.Subjects) == 0 {
		mset.config.Subjects = append(mset.config.Subjects, mset.config.Name)
	}
	jsa.msgSets[config.Name] = mset
	jsa.mu.Unlock()

	// Bind to the account.
	c.registerWithAccount(a)

	// Create the appropriate storage
	if err := mset.setupStore(); err != nil {
		mset.delete()
		return nil, err
	}
	// Setup our internal send go routine.
	mset.setupSendCapabilities()

	// Setup subscriptions
	if err := mset.subscribeToMsgSet(); err != nil {
		mset.delete()
		return nil, err
	}

	return mset, nil
}

// JetStreamDeleteMsgSet will delete a message set.
func (s *Server) JetStreamDeleteMsgSet(mset *MsgSet) error {
	return mset.Delete()
}

// Delete deletes a message set.
func (mset *MsgSet) Delete() error {
	mset.mu.Lock()
	jsa := mset.jsa
	mset.mu.Unlock()
	if jsa == nil {
		return fmt.Errorf("jetstream not enabled for account")
	}
	jsa.mu.Lock()
	delete(jsa.msgSets, mset.config.Name)
	jsa.mu.Unlock()

	return mset.delete()
}

// Purge will remove all messages from the message set and underlying store.
func (mset *MsgSet) Purge() uint64 {
	return mset.store.Purge()
}

// Will create internal subscriptions for the msgSet.
// Lock should be held.
func (mset *MsgSet) subscribeToMsgSet() error {
	for _, subject := range mset.config.Subjects {
		if _, err := mset.subscribeInternal(subject, mset.processInboundJetStreamMsg); err != nil {
			return err
		}
	}
	return nil
}

// FIXME(dlc) - This only works in single server mode for the moment. Need to fix as we expand to clusters.
func (mset *MsgSet) subscribeInternal(subject string, cb msgHandler) (*subscription, error) {
	return mset.nmsSubscribeInternal(subject, true, cb)
}

func (mset *MsgSet) nmsSubscribeInternal(subject string, internalOnly bool, cb msgHandler) (*subscription, error) {
	c := mset.client
	if c == nil {
		return nil, fmt.Errorf("invalid message set")
	}
	if !c.srv.eventsEnabled() {
		return nil, ErrNoSysAccount
	}
	if cb == nil {
		return nil, fmt.Errorf("undefined message handler")
	}

	mset.sid++

	// Now create the subscription
	sub, err := c.processSub([]byte(subject+" "+strconv.Itoa(mset.sid)), internalOnly)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	sub.icb = cb
	c.mu.Unlock()
	return sub, nil
}

// Lock should be held.
func (mset *MsgSet) unsubscribe(sub *subscription) {
	if sub == nil || mset.client == nil {
		return
	}
	mset.client.unsubscribe(mset.client.acc, sub, true, true)
}

func (mset *MsgSet) setupStore() error {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	switch mset.config.Storage {
	case MemoryStorage:
		ms, err := newMemStore(&mset.config)
		if err != nil {
			return err
		}
		mset.store = ms
	case DiskStorage:
		return fmt.Errorf("NO IMPL")
	}
	return nil
}

func (mset *MsgSet) processInboundJetStreamMsg(_ *subscription, _ *client, subject, reply string, msg []byte) {
	mset.mu.Lock()
	store := mset.store
	c := mset.client
	doAck := !mset.config.NoAck
	mset.mu.Unlock()

	if c == nil {
		return
	}

	// FIXME(dlc) - Not inline unless memory based.
	if _, err := store.StoreMsg(subject, msg); err != nil {
		mset.mu.Lock()
		s := c.srv
		accName := c.acc.Name
		name := mset.config.Name
		mset.mu.Unlock()
		s.Errorf("Jetstream failed to store msg on account: %q message set: %q -  %v", accName, name, err)
		// TODO(dlc) - Send err here
		return
	}
	// Send Ack here.
	if doAck && len(reply) > 0 {
		mset.sendq <- &jsPubMsg{reply, _EMPTY_, _EMPTY_, AckAck, nil, 0}
	}

	mset.signalObservers()
}

func (mset *MsgSet) signalObservers() {
	mset.mu.Lock()
	if mset.sgw > 0 {
		mset.sg.Broadcast()
	}
	mset.mu.Unlock()
}

// Internal message for use by jetstream subsystem.
type jsPubMsg struct {
	subj  string
	dsubj string
	reply string
	msg   []byte
	o     *Observable
	seq   uint64
}

// TODO(dlc) - Maybe look at onering instead of chan - https://github.com/pltr/onering
const nmsSendQSize = 1024

// This is similar to system semantics but did not want to overload the single system sendq,
// or require system account when doing simple setup with jetstream.
func (mset *MsgSet) setupSendCapabilities() {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	if mset.sendq != nil {
		return
	}
	mset.sendq = make(chan *jsPubMsg, nmsSendQSize)
	go mset.internalSendLoop()
}

func (mset *MsgSet) internalSendLoop() {
	mset.mu.Lock()
	c := mset.client
	if c == nil {
		mset.mu.Unlock()
		return
	}
	s := c.srv
	sendq := mset.sendq
	name := mset.config.Name
	mset.mu.Unlock()

	// Warn when internal send queue is backed up past 75%
	warnThresh := 3 * nmsSendQSize / 4
	warnFreq := time.Second
	last := time.Now().Add(-warnFreq)

	for {
		if len(sendq) > warnThresh && time.Since(last) >= warnFreq {
			s.Warnf("Jetstream internal send queue > 75% for account: %q message set: %q", c.acc.Name, name)
			last = time.Now()
		}
		select {
		case pm := <-sendq:
			if pm == nil {
				return
			}
			c.pa.subject = []byte(pm.subj)
			c.pa.deliver = []byte(pm.dsubj)
			c.pa.size = len(pm.msg)
			c.pa.szb = []byte(strconv.Itoa(c.pa.size))
			c.pa.reply = []byte(pm.reply)
			msg := append(pm.msg, _CRLF_...)
			// FIXME(dlc) - capture if this sent to anyone and notify
			// observer if its now zero, meaning no interest.
			didDeliver := c.processInboundClientMsg(msg)
			c.pa.szb = nil
			c.flushClients(0)
			// Check to see if this is a delivery for an observable and
			// we failed to deliver the message. If so alert the observable.
			if pm.o != nil && !didDeliver {
				pm.o.didNotDeliver(pm.seq)
			}
		case <-s.quitCh:
			return
		}
	}
}

func (mset *MsgSet) delete() error {
	var obs []*Observable

	mset.mu.Lock()
	if mset.sendq != nil {
		mset.sendq <- nil
	}
	c := mset.client
	mset.client = nil
	for _, o := range mset.obs {
		obs = append(obs, o)
	}
	mset.obs = nil
	mset.mu.Unlock()
	c.closeConnection(ClientClosed)

	for _, o := range obs {
		o.Delete()
	}
	// TODO(dlc) - Clean up any storage, memory and disk
	return nil
}

// Returns a name that can be used as a single token for subscriptions.
// Really only need to replace token separators.
// Lock should be held
func (mset *MsgSet) cleanName() string {
	return strings.Replace(mset.config.Name, tsep, "-", -1)
}

// NumObservables reports on number of active observables for this message set.
func (mset *MsgSet) NumObservables() int {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	return len(mset.obs)
}

// Stats will return the current stats for this message set.
func (mset *MsgSet) Stats() MsgSetStats {
	// Currently rely on store.
	// TODO(dlc) - This will need to change with clusters.
	return mset.store.Stats()
}

// waitForMsgs will have the message set wait for the arrival of new messages.
func (mset *MsgSet) waitForMsgs() {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	if mset.client == nil {
		return
	}

	mset.sgw++
	mset.sg.Wait()
	mset.sgw--
}

// Determines if the new proposed partition is unique amongst all observables.
// Lock should be held.
func (mset *MsgSet) partitionUnique(partition string) bool {
	for _, o := range mset.obs {
		if o.config.Partition == _EMPTY_ {
			return false
		}
		if subjectIsSubsetMatch(partition, o.config.Partition) {
			return false
		}
	}
	return true
}

// ackMsg is called into from an observable when we have a WorkQueue or Interest retention policy.
func (mset *MsgSet) ackMsg(obs *Observable, seq uint64) {
	switch mset.config.Retention {
	case StreamPolicy:
		return
	case WorkQueuePolicy:
		mset.store.RemoveMsg(seq)
	case InterestPolicy:
		var needAck bool
		mset.mu.Lock()
		for _, o := range mset.obs {
			if o != obs && o.needAck(seq) {
				needAck = true
				break
			}
		}
		mset.mu.Unlock()
		if !needAck {
			mset.store.RemoveMsg(seq)
		}
	}
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
	// know yet, but that is ok for here.
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
