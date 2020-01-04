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
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"
)

// MsgSetConfig will determine the name, subjects and retention policy
// for a given message set. If subjects is empty the name will be used.
type MsgSetConfig struct {
	Name           string          `json:"name"`
	Subjects       []string        `json:"subjects,omitempty"`
	Retention      RetentionPolicy `json:"retention"`
	MaxObservables int             `json:"max_observables"`
	MaxMsgs        int64           `json:"max_msgs"`
	MaxBytes       int64           `json:"max_bytes"`
	MaxAge         time.Duration   `json:"max_age"`
	Storage        StorageType     `json:"storage"`
	Replicas       int             `json:"num_replicas"`
	NoAck          bool            `json:"no_ack,omitempty"`
}

type MsgSetInfo struct {
	Config MsgSetConfig `json:"config"`
	Stats  MsgSetStats  `json:"stats"`
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
	mu     sync.RWMutex
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

// AddMsgSet adds a JetStream message set for the given account.
func (a *Account) AddMsgSet(config *MsgSetConfig) (*MsgSet, error) {
	a.mu.RLock()
	s := a.srv
	a.mu.RUnlock()
	if s == nil {
		return nil, fmt.Errorf("jetstream account not registered")
	}

	// FIXME(dlc) - Change for clustering.
	js := s.getJetStream()
	if js == nil {
		return nil, fmt.Errorf("jetstream not enabled")
	}

	jsa := a.js
	if jsa == nil {
		return nil, fmt.Errorf("jetstream not enabled for account")
	}

	// Sensible defaults.
	cfg, err := checkMsgSetCfg(config)
	if err != nil {
		return nil, err
	}

	jsa.mu.Lock()
	if _, ok := jsa.msgSets[cfg.Name]; ok {
		jsa.mu.Unlock()
		return nil, fmt.Errorf("message set name already in use")
	}
	// Check for limits.
	if err := jsa.checkLimits(&cfg); err != nil {
		jsa.mu.Unlock()
		return nil, err
	}
	// Setup the internal client.
	c := s.createInternalJetStreamClient()
	mset := &MsgSet{jsa: jsa, config: cfg, client: c, obs: make(map[string]*Observable)}
	mset.sg = sync.NewCond(&mset.mu)

	if len(mset.config.Subjects) == 0 {
		mset.config.Subjects = append(mset.config.Subjects, mset.config.Name)
	}
	jsa.msgSets[config.Name] = mset
	storeDir := path.Join(jsa.storeDir, config.Name)
	jsa.mu.Unlock()

	// Bind to the account.
	c.registerWithAccount(a)

	// Create the appropriate storage

	if err := mset.setupStore(storeDir); err != nil {
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

func checkMsgSetCfg(config *MsgSetConfig) (MsgSetConfig, error) {
	if config == nil {
		return MsgSetConfig{}, fmt.Errorf("message set configuration invalid")
	}

	if !isValidName(config.Name) {
		return MsgSetConfig{}, fmt.Errorf("message set name required, can not contain '.', '*', '>'")
	}

	cfg := *config

	// TODO(dlc) - check config for conflicts, e.g replicas > 1 in single server mode.
	if cfg.Replicas == 0 {
		cfg.Replicas = 1
	}
	if cfg.Replicas > MsgSetMaxReplicas {
		return cfg, fmt.Errorf("maximum replicas is %d", MsgSetMaxReplicas)
	}
	if cfg.MaxMsgs == 0 {
		cfg.MaxMsgs = -1
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = -1
	}
	return cfg, nil
}

// Config returns the message set's configuration.
func (mset *MsgSet) Config() MsgSetConfig {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	return mset.config
}

// Delete deletes a message set from the owning account.
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
	mset.mu.Lock()
	if mset.client == nil {
		mset.mu.Unlock()
		return 0
	}
	purged := mset.store.Purge()
	stats := mset.store.Stats()
	var obs []*Observable
	for _, o := range mset.obs {
		obs = append(obs, o)
	}
	mset.mu.Unlock()
	for _, o := range obs {
		o.purge(stats.FirstSeq)
	}
	return purged
}

// RemoveMsg will remove a message from a message set.
// FIXME(dlc) - Should pick one and be consistent.
func (mset *MsgSet) RemoveMsg(seq uint64) bool {
	return mset.store.RemoveMsg(seq)
}

// DeleteMsg will remove a message from a message set.
func (mset *MsgSet) DeleteMsg(seq uint64) bool {
	return mset.store.RemoveMsg(seq)
}

// EraseMsg will securely remove a message and rewrite the data with random data.
func (mset *MsgSet) EraseMsg(seq uint64) bool {
	return mset.store.EraseMsg(seq)
}

// Will create internal subscriptions for the msgSet.
// Lock should be held.
func (mset *MsgSet) subscribeToMsgSet() error {
	for _, subject := range mset.config.Subjects {
		if _, err := mset.subscribeInternal(subject, mset.processInboundJetStreamMsg); err != nil {
			return err
		}
	}
	// Now subscribe for direct access
	subj := fmt.Sprintf("%s.%s", JetStreamMsgBySeqPre, mset.config.Name)
	if _, err := mset.subscribeInternal(subj, mset.processMsgBySeq); err != nil {
		return err
	}
	return nil
}

// FIXME(dlc) - This only works in single server mode for the moment. Need to fix as we expand to clusters.
func (mset *MsgSet) subscribeInternal(subject string, cb msgHandler) (*subscription, error) {
	return mset.nmsSubscribeInternal(subject, false, cb)
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

func (mset *MsgSet) setupStore(storeDir string) error {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	switch mset.config.Storage {
	case MemoryStorage:
		ms, err := newMemStore(&mset.config)
		if err != nil {
			return err
		}
		mset.store = ms
	case FileStorage:
		fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, mset.config)
		if err != nil {
			return err
		}
		mset.store = fs
	}
	jsa, st := mset.jsa, mset.config.Storage
	mset.store.StorageBytesUpdate(func(delta int64) { jsa.updateUsage(st, delta) })
	return nil
}

// processMsgBySeq will return the message at the given sequence, or an -ERR if not found.
func (mset *MsgSet) processMsgBySeq(_ *subscription, _ *client, subject, reply string, msg []byte) {
	mset.mu.Lock()
	store := mset.store
	c := mset.client
	name := mset.config.Name
	mset.mu.Unlock()

	if c == nil {
		return
	}
	var response []byte

	if len(msg) == 0 {
		c.Debugf("JetStream request for message from message set: %q - %q no sequence arg", c.acc.Name, name)
		response = []byte("-ERR 'sequence argument missing'")
		mset.sendq <- &jsPubMsg{reply, _EMPTY_, _EMPTY_, response, nil, 0}
		return
	}
	seq, err := strconv.ParseUint(string(msg), 10, 64)
	if err != nil {
		c.Debugf("JetStream request for message from message: %q - %q bad sequence arg %q", c.acc.Name, name, msg)
		response = []byte("-ERR 'bad sequence argument'")
		mset.sendq <- &jsPubMsg{reply, _EMPTY_, _EMPTY_, response, nil, 0}
		return
	}

	subj, msg, ts, err := store.LoadMsg(seq)
	if err != nil {
		c.Debugf("JetStream request for message: %q - %q - %d error %v", c.acc.Name, name, seq, err)
		response = []byte("-ERR 'could not load message from storage'")
		mset.sendq <- &jsPubMsg{reply, _EMPTY_, _EMPTY_, response, nil, 0}
		return
	}
	sm := &StoredMsg{
		Subject: subj,
		Data:    msg,
		Time:    time.Unix(0, ts),
	}
	response, _ = json.MarshalIndent(sm, "", "  ")
	mset.sendq <- &jsPubMsg{reply, _EMPTY_, _EMPTY_, response, nil, 0}
}

// processInboundJetStreamMsg handles processing messages bound for a message set.
func (mset *MsgSet) processInboundJetStreamMsg(_ *subscription, _ *client, subject, reply string, msg []byte) {
	mset.mu.Lock()
	store := mset.store
	c := mset.client
	ackEnabled := !mset.config.NoAck
	jsa := mset.jsa
	stype := mset.config.Storage
	sname := mset.config.Name
	mset.mu.Unlock()

	if c == nil {
		return
	}

	// Response to send.
	response := AckAck
	shouldAck := ackEnabled && len(reply) > 0

	// Check to see if we are over the account limit.
	seq, err := store.StoreMsg(subject, msg)
	if err != nil {
		c.Errorf("JetStream failed to store a msg on account: %q message set: %q -  %v", c.acc.Name, sname, err)
		response = []byte(fmt.Sprintf("-ERR %q", err.Error()))
	} else if jsa.limitsExceeded(stype) {
		c.Debugf("JetStream resource limits exceeded for account: %q", c.acc.Name)
		response = []byte("-ERR 'resource limits exceeded for account'")
		store.RemoveMsg(seq)
		seq = 0
	} else if shouldAck {
		response = []byte(OK + " " + sname + " " + strconv.Itoa(int(seq)))
	}

	// Send response here.
	if shouldAck {
		mset.sendq <- &jsPubMsg{reply, _EMPTY_, _EMPTY_, response, nil, 0}
	}

	if err == nil && seq > 0 {
		var needSignal bool
		mset.mu.Lock()
		for _, o := range mset.obs {
			if !o.deliverCurrentMsg(subject, msg, seq) {
				needSignal = true
			}
		}
		mset.mu.Unlock()

		if needSignal {
			mset.signalObservers()
		}
	}
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

type StoredMsg struct {
	Subject string
	Data    []byte
	Time    time.Time
}

// TODO(dlc) - Maybe look at onering instead of chan - https://github.com/pltr/onering
const msetSendQSize = 1024

// This is similar to system semantics but did not want to overload the single system sendq,
// or require system account when doing simple setup with jetstream.
func (mset *MsgSet) setupSendCapabilities() {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	if mset.sendq != nil {
		return
	}
	mset.sendq = make(chan *jsPubMsg, msetSendQSize)
	go mset.internalSendLoop()
}

// Name returns the message set name.
func (mset *MsgSet) Name() string {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	return mset.config.Name
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
	warnThresh := 3 * msetSendQSize / 4
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
	return mset.stop(true)
}

func (mset *MsgSet) stop(delete bool) error {
	mset.mu.Lock()
	if mset.sendq != nil {
		mset.sendq <- nil
	}
	c := mset.client
	mset.client = nil
	if c == nil {
		mset.mu.Unlock()
		return nil
	}
	var obs []*Observable
	for _, o := range mset.obs {
		obs = append(obs, o)
	}
	mset.obs = nil
	mset.mu.Unlock()
	c.closeConnection(ClientClosed)

	if mset.store == nil {
		return nil
	}

	if delete {
		mset.store.Delete()
		for _, o := range obs {
			o.Delete()
		}
	} else {
		mset.store.Stop()
		for _, o := range obs {
			o.Stop()
		}
	}

	return nil
}

// Observables will return all the current observables for this message set.
func (mset *MsgSet) Observables() []*Observable {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	var obs []*Observable
	for _, o := range mset.obs {
		obs = append(obs, o)
	}
	return obs
}

// NumObservables reports on number of active observables for this message set.
func (mset *MsgSet) NumObservables() int {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	return len(mset.obs)
}

// LookupObservable will retrieve an observable by name.
func (mset *MsgSet) LookupObservable(name string) *Observable {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	for _, o := range mset.obs {
		if o.name == name {
			return o
		}
	}
	return nil
}

// Stats will return the current stats for this message set.
func (mset *MsgSet) Stats() MsgSetStats {
	mset.mu.Lock()
	c := mset.client
	mset.mu.Unlock()
	if c == nil {
		return MsgSetStats{}
	}
	// Currently rely on store.
	// TODO(dlc) - This will need to change with clusters.
	return mset.store.Stats()
}

// waitForMsgs will have the message set wait for the arrival of new messages.
func (mset *MsgSet) waitForMsgs() {
	mset.mu.Lock()

	if mset.client == nil {
		mset.mu.Unlock()
		return
	}

	mset.sgw++
	mset.sg.Wait()
	mset.sgw--

	mset.mu.Unlock()
}

// Determines if the new proposed partition is unique amongst all observables.
// Lock should be held.
func (mset *MsgSet) partitionUnique(partition string) bool {
	for _, o := range mset.obs {
		if o.config.Subject == _EMPTY_ {
			return false
		}
		if subjectIsSubsetMatch(partition, o.config.Subject) {
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
