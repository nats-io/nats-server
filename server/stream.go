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
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

// StreamConfig will determine the name, subjects and retention policy
// for a given stream. If subjects is empty the name will be used.
type StreamConfig struct {
	Name         string          `json:"name"`
	Subjects     []string        `json:"subjects,omitempty"`
	Retention    RetentionPolicy `json:"retention"`
	MaxConsumers int             `json:"max_consumers"`
	MaxMsgs      int64           `json:"max_msgs"`
	MaxBytes     int64           `json:"max_bytes"`
	MaxAge       time.Duration   `json:"max_age"`
	MaxMsgSize   int32           `json:"max_msg_size,omitempty"`
	Storage      StorageType     `json:"storage"`
	Replicas     int             `json:"num_replicas"`
	NoAck        bool            `json:"no_ack,omitempty"`
	Template     string          `json:"template_owner,omitempty"`
}

type StreamInfo struct {
	Config StreamConfig `json:"config"`
	State  StreamState  `json:"state"`
}

// Stream is a jetstream stream of messages. When we receive a message internally destined
// for a Stream we will direct link from the client to this Stream structure.
type Stream struct {
	mu        sync.RWMutex
	sg        *sync.Cond
	sgw       int
	jsa       *jsAccount
	client    *client
	sid       int
	sendq     chan *jsPubMsg
	store     StreamStore
	consumers map[string]*Consumer
	config    StreamConfig
}

const (
	StreamDefaultReplicas = 1
	StreamMaxReplicas     = 8
)

// AddStream adds a stream for the given account.
func (a *Account) AddStream(config *StreamConfig) (*Stream, error) {
	s, jsa, err := a.checkForJetStream()
	if err != nil {
		return nil, err
	}

	// Sensible defaults.
	cfg, err := checkStreamCfg(config)
	if err != nil {
		return nil, err
	}

	jsa.mu.Lock()
	if _, ok := jsa.streams[cfg.Name]; ok {
		jsa.mu.Unlock()
		return nil, fmt.Errorf("stream name already in use")
	}
	// Check for limits.
	if err := jsa.checkLimits(&cfg); err != nil {
		jsa.mu.Unlock()
		return nil, err
	}
	// Check for template ownership if present.
	if cfg.Template != _EMPTY_ && jsa.account != nil {
		if !jsa.checkTemplateOwnership(cfg.Template, cfg.Name) {
			jsa.mu.Unlock()
			return nil, fmt.Errorf("stream not owned by template")
		}
	}

	if len(cfg.Subjects) == 0 {
		cfg.Subjects = append(cfg.Subjects, cfg.Name)
	}
	// Check for overlapping subjects. These are not allowed for now.
	if jsa.subjectsOverlap(cfg.Subjects) {
		jsa.mu.Unlock()
		return nil, fmt.Errorf("subjects overlap with an existing stream")
	}

	// Setup the internal client.
	c := s.createInternalJetStreamClient()
	mset := &Stream{jsa: jsa, config: cfg, client: c, consumers: make(map[string]*Consumer)}
	mset.sg = sync.NewCond(&mset.mu)

	jsa.streams[cfg.Name] = mset
	storeDir := path.Join(jsa.storeDir, streamsDir, cfg.Name)
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
	if err := mset.subscribeToStream(); err != nil {
		mset.delete()
		return nil, err
	}

	return mset, nil
}

// Check to see if these subjects overlap with existing subjects.
// Lock should be held.
func (jsa *jsAccount) subjectsOverlap(subjects []string) bool {
	for _, mset := range jsa.streams {
		for _, subj := range mset.config.Subjects {
			for _, tsubj := range subjects {
				if SubjectsCollide(tsubj, subj) {
					return true
				}
			}
		}
	}
	return false
}

func checkStreamCfg(config *StreamConfig) (StreamConfig, error) {
	if config == nil {
		return StreamConfig{}, fmt.Errorf("stream configuration invalid")
	}

	if len(config.Name) == 0 || strings.ContainsAny(config.Name, "*>") {
		//if !isValidName(config.Name) {
		return StreamConfig{}, fmt.Errorf("stream name is required and can not contain '*', '>'")
	}

	cfg := *config

	// TODO(dlc) - check config for conflicts, e.g replicas > 1 in single server mode.
	if cfg.Replicas == 0 {
		cfg.Replicas = 1
	}
	if cfg.Replicas > StreamMaxReplicas {
		return cfg, fmt.Errorf("maximum replicas is %d", StreamMaxReplicas)
	}
	if cfg.MaxMsgs == 0 {
		cfg.MaxMsgs = -1
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = -1
	}
	if cfg.MaxMsgSize == 0 {
		cfg.MaxMsgSize = -1
	}
	return cfg, nil
}

// Config returns the stream's configuration.
func (mset *Stream) Config() StreamConfig {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	return mset.config
}

// Delete deletes a stream from the owning account.
func (mset *Stream) Delete() error {
	mset.mu.Lock()
	jsa := mset.jsa
	mset.mu.Unlock()
	if jsa == nil {
		return fmt.Errorf("jetstream not enabled for account")
	}
	jsa.mu.Lock()
	delete(jsa.streams, mset.config.Name)
	jsa.mu.Unlock()

	return mset.delete()
}

// Purge will remove all messages from the stream and underlying store.
func (mset *Stream) Purge() uint64 {
	mset.mu.Lock()
	if mset.client == nil {
		mset.mu.Unlock()
		return 0
	}
	purged := mset.store.Purge()
	stats := mset.store.State()
	var obs []*Consumer
	for _, o := range mset.consumers {
		obs = append(obs, o)
	}
	mset.mu.Unlock()
	for _, o := range obs {
		o.purge(stats.FirstSeq)
	}
	return purged
}

// RemoveMsg will remove a message from a stream.
// FIXME(dlc) - Should pick one and be consistent.
func (mset *Stream) RemoveMsg(seq uint64) bool {
	return mset.store.RemoveMsg(seq)
}

// DeleteMsg will remove a message from a stream.
func (mset *Stream) DeleteMsg(seq uint64) bool {
	return mset.store.RemoveMsg(seq)
}

// EraseMsg will securely remove a message and rewrite the data with random data.
func (mset *Stream) EraseMsg(seq uint64) bool {
	return mset.store.EraseMsg(seq)
}

// Will create internal subscriptions for the msgSet.
// Lock should be held.
func (mset *Stream) subscribeToStream() error {
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
func (mset *Stream) subscribeInternal(subject string, cb msgHandler) (*subscription, error) {
	return mset.nmsSubscribeInternal(subject, false, cb)
}

func (mset *Stream) nmsSubscribeInternal(subject string, internalOnly bool, cb msgHandler) (*subscription, error) {
	c := mset.client
	if c == nil {
		return nil, fmt.Errorf("invalid stream")
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
func (mset *Stream) unsubscribe(sub *subscription) {
	if sub == nil || mset.client == nil {
		return
	}
	mset.client.unsubscribe(mset.client.acc, sub, true, true)
}

func (mset *Stream) setupStore(storeDir string) error {
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
func (mset *Stream) processMsgBySeq(_ *subscription, _ *client, subject, reply string, msg []byte) {
	mset.mu.Lock()
	store := mset.store
	c := mset.client
	name := mset.config.Name
	mset.mu.Unlock()

	if c == nil {
		return
	}
	var response []byte
	var seq uint64
	var err error

	// If no sequence arg assume last sequence we have.
	if len(msg) == 0 {
		stats := store.State()
		seq = stats.LastSeq
	} else {
		seq, err = strconv.ParseUint(string(msg), 10, 64)
		if err != nil {
			c.Debugf("JetStream request for message from message: %q - %q bad sequence arg %q", c.acc.Name, name, msg)
			response = []byte("-ERR 'bad sequence argument'")
			mset.sendq <- &jsPubMsg{reply, _EMPTY_, _EMPTY_, response, nil, 0}
			return
		}
	}

	subj, msg, ts, err := store.LoadMsg(seq)
	if err != nil {
		c.Debugf("JetStream request for message: %q - %q - %d error %v", c.acc.Name, name, seq, err)
		response = []byte("-ERR 'could not load message from storage'")
		mset.sendq <- &jsPubMsg{reply, _EMPTY_, _EMPTY_, response, nil, 0}
		return
	}
	sm := &StoredMsg{
		Subject:  subj,
		Sequence: seq,
		Data:     msg,
		Time:     time.Unix(0, ts),
	}
	response, _ = json.MarshalIndent(sm, "", "  ")
	mset.sendq <- &jsPubMsg{reply, _EMPTY_, _EMPTY_, response, nil, 0}
}

// processInboundJetStreamMsg handles processing messages bound for a stream.
func (mset *Stream) processInboundJetStreamMsg(_ *subscription, _ *client, subject, reply string, msg []byte) {
	mset.mu.Lock()
	store := mset.store
	c := mset.client
	var accName string
	if c != nil && c.acc != nil {
		accName = c.acc.Name
	}
	doAck := !mset.config.NoAck
	jsa := mset.jsa
	stype := mset.config.Storage
	name := mset.config.Name
	maxMsgSize := int(mset.config.MaxMsgSize)
	mset.mu.Unlock()

	if c == nil {
		return
	}

	// Response to send.
	response := AckAck
	var seq uint64
	var err error

	if maxMsgSize >= 0 && len(msg) > maxMsgSize {
		response = []byte("-ERR 'message size exceeds maximum allowed'")
	} else {
		// Check to see if we are over the account limit.
		seq, err = store.StoreMsg(subject, msg)
		if err != nil {
			c.Errorf("JetStream failed to store a msg on account: %q stream: %q -  %v", accName, name, err)
			response = []byte(fmt.Sprintf("-ERR '%s'", err.Error()))
		} else if jsa.limitsExceeded(stype) {
			c.Warnf("JetStream resource limits exceeded for account: %q", accName)
			response = []byte("-ERR 'resource limits exceeded for account'")
			store.RemoveMsg(seq)
			seq = 0
		}
	}

	// Send response here.
	if doAck && len(reply) > 0 {
		mset.sendq <- &jsPubMsg{reply, _EMPTY_, _EMPTY_, response, nil, 0}
	}

	if err == nil && seq > 0 {
		var needSignal bool
		mset.mu.Lock()
		for _, o := range mset.consumers {
			if !o.deliverCurrentMsg(subject, msg, seq) {
				needSignal = true
			}
		}
		mset.mu.Unlock()

		if needSignal {
			mset.signalConsumers()
		}
	}
}

// Will signal all waiting consumers.
func (mset *Stream) signalConsumers() {
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
	o     *Consumer
	seq   uint64
}

// StoredMsg is for raw access to messages in a stream.
type StoredMsg struct {
	Subject  string    `json:"subject"`
	Sequence uint64    `json:"seq"`
	Data     []byte    `json:"data"`
	Time     time.Time `json:"time"`
}

// TODO(dlc) - Maybe look at onering instead of chan - https://github.com/pltr/onering
const msetSendQSize = 1024

// This is similar to system semantics but did not want to overload the single system sendq,
// or require system account when doing simple setup with jetstream.
func (mset *Stream) setupSendCapabilities() {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	if mset.sendq != nil {
		return
	}
	mset.sendq = make(chan *jsPubMsg, msetSendQSize)
	go mset.internalSendLoop()
}

// Name returns the stream name.
func (mset *Stream) Name() string {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	return mset.config.Name
}

func (mset *Stream) internalSendLoop() {
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
			s.Warnf("Jetstream internal send queue > 75% for account: %q stream: %q", c.acc.Name, name)
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

// Internal function to delete a stream.
func (mset *Stream) delete() error {
	return mset.stop(true)
}

// Internal function to stop or delete the stream.
func (mset *Stream) stop(delete bool) error {
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
	var obs []*Consumer
	for _, o := range mset.consumers {
		obs = append(obs, o)
	}
	mset.consumers = nil
	mset.mu.Unlock()
	c.closeConnection(ClientClosed)

	if mset.store == nil {
		return nil
	}

	if delete {
		if err := mset.store.Delete(); err != nil {
			return err
		}
		for _, o := range obs {
			if err := o.Delete(); err != nil {
				return err
			}
		}
	} else {
		if err := mset.store.Stop(); err != nil {
			return err
		}
		for _, o := range obs {
			if err := o.Stop(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Consunmers will return all the current consumers for this stream.
func (mset *Stream) Consumers() []*Consumer {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	var obs []*Consumer
	for _, o := range mset.consumers {
		obs = append(obs, o)
	}
	return obs
}

// NumConsumers reports on number of active observables for this stream.
func (mset *Stream) NumConsumers() int {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	return len(mset.consumers)
}

// LookupConsumer will retrieve a consumer by name.
func (mset *Stream) LookupConsumer(name string) *Consumer {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	for _, o := range mset.consumers {
		if o.name == name {
			return o
		}
	}
	return nil
}

// State will return the current state for this stream.
func (mset *Stream) State() StreamState {
	mset.mu.Lock()
	c := mset.client
	mset.mu.Unlock()
	if c == nil {
		return StreamState{}
	}
	// Currently rely on store.
	// TODO(dlc) - This will need to change with clusters.
	return mset.store.State()
}

// waitForMsgs will have the stream wait for the arrival of new messages.
func (mset *Stream) waitForMsgs() {
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
func (mset *Stream) partitionUnique(partition string) bool {
	for _, o := range mset.consumers {
		if o.config.FilterSubject == _EMPTY_ {
			return false
		}
		if subjectIsSubsetMatch(partition, o.config.FilterSubject) {
			return false
		}
	}
	return true
}

// ackMsg is called into from an observable when we have a WorkQueue or Interest retention policy.
func (mset *Stream) ackMsg(obs *Consumer, seq uint64) {
	switch mset.config.Retention {
	case LimitsPolicy:
		return
	case WorkQueuePolicy:
		mset.store.RemoveMsg(seq)
	case InterestPolicy:
		var needAck bool
		mset.mu.Lock()
		for _, o := range mset.consumers {
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
