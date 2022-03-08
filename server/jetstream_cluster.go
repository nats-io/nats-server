// Copyright 2020-2022 The NATS Authors
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
	"math"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nuid"
)

// jetStreamCluster holds information about the meta group and stream assignments.
type jetStreamCluster struct {
	// The metacontroller raftNode.
	meta RaftNode
	// For stream and consumer assignments. All servers will have this be the same.
	// ACCOUNT -> STREAM -> Stream Assignment -> Consumers
	streams map[string]map[string]*streamAssignment
	// Signals meta-leader should check the stream assignments.
	streamsCheck bool
	// Server.
	s *Server
	// Internal client.
	c *client
	// Processing assignment results.
	streamResults   *subscription
	consumerResults *subscription
	// System level request to have the leader stepdown.
	stepdown *subscription
	// System level requests to remove a peer.
	peerRemove *subscription
}

// Used to guide placement of streams and meta controllers in clustered JetStream.
type Placement struct {
	Cluster string   `json:"cluster,omitempty"`
	Tags    []string `json:"tags,omitempty"`
}

// Define types of the entry.
type entryOp uint8

// ONLY ADD TO THE END, DO NOT INSERT IN BETWEEN WILL BREAK SERVER INTEROP.
const (
	// Meta ops.
	assignStreamOp entryOp = iota
	assignConsumerOp
	removeStreamOp
	removeConsumerOp
	// Stream ops.
	streamMsgOp
	purgeStreamOp
	deleteMsgOp
	// Consumer ops.
	updateDeliveredOp
	updateAcksOp
	// Compressed consumer assignments.
	assignCompressedConsumerOp
	// Filtered Consumer skip.
	updateSkipOp
	// Update Stream.
	updateStreamOp
	// For updating information on pending pull requests.
	addPendingRequest
	removePendingRequest
)

// raftGroups are controlled by the metagroup controller.
// The raftGroups will house streams and consumers.
type raftGroup struct {
	Name      string      `json:"name"`
	Peers     []string    `json:"peers"`
	Storage   StorageType `json:"store"`
	Cluster   string      `json:"cluster,omitempty"`
	Preferred string      `json:"preferred,omitempty"`
	// Internal
	node RaftNode
}

// streamAssignment is what the meta controller uses to assign streams to peers.
type streamAssignment struct {
	Client  *ClientInfo   `json:"client,omitempty"`
	Created time.Time     `json:"created"`
	Config  *StreamConfig `json:"stream"`
	Group   *raftGroup    `json:"group"`
	Sync    string        `json:"sync"`
	Subject string        `json:"subject"`
	Reply   string        `json:"reply"`
	Restore *StreamState  `json:"restore_state,omitempty"`
	// Internal
	consumers map[string]*consumerAssignment
	responded bool
	err       error
}

// consumerAssignment is what the meta controller uses to assign consumers to streams.
type consumerAssignment struct {
	Client  *ClientInfo     `json:"client,omitempty"`
	Created time.Time       `json:"created"`
	Name    string          `json:"name"`
	Stream  string          `json:"stream"`
	Config  *ConsumerConfig `json:"consumer"`
	Group   *raftGroup      `json:"group"`
	Subject string          `json:"subject"`
	Reply   string          `json:"reply"`
	State   *ConsumerState  `json:"state,omitempty"`
	// Internal
	responded bool
	deleted   bool
	pending   bool
	err       error
}

// streamPurge is what the stream leader will replicate when purging a stream.
type streamPurge struct {
	Client  *ClientInfo              `json:"client,omitempty"`
	Stream  string                   `json:"stream"`
	LastSeq uint64                   `json:"last_seq"`
	Subject string                   `json:"subject"`
	Reply   string                   `json:"reply"`
	Request *JSApiStreamPurgeRequest `json:"request,omitempty"`
}

// streamMsgDelete is what the stream leader will replicate when deleting a message.
type streamMsgDelete struct {
	Client  *ClientInfo `json:"client,omitempty"`
	Stream  string      `json:"stream"`
	Seq     uint64      `json:"seq"`
	NoErase bool        `json:"no_erase,omitempty"`
	Subject string      `json:"subject"`
	Reply   string      `json:"reply"`
}

const (
	defaultStoreDirName  = "_js_"
	defaultMetaGroupName = "_meta_"
	defaultMetaFSBlkSize = 1024 * 1024
)

// Returns information useful in mixed mode.
func (s *Server) trackedJetStreamServers() (js, total int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running || !s.eventsEnabled() {
		return -1, -1
	}
	s.nodeToInfo.Range(func(k, v interface{}) bool {
		si := v.(nodeInfo)
		if si.js {
			js++
		}
		total++
		return true
	})
	return js, total
}

func (s *Server) getJetStreamCluster() (*jetStream, *jetStreamCluster) {
	s.mu.Lock()
	shutdown := s.shutdown
	js := s.js
	s.mu.Unlock()

	if shutdown || js == nil {
		return nil, nil
	}

	js.mu.RLock()
	cc := js.cluster
	js.mu.RUnlock()
	if cc == nil {
		return nil, nil
	}
	return js, cc
}

func (s *Server) JetStreamIsClustered() bool {
	js := s.getJetStream()
	if js == nil {
		return false
	}
	return js.isClustered()
}

func (s *Server) JetStreamIsLeader() bool {
	js := s.getJetStream()
	if js == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.cluster.isLeader()
}

func (s *Server) JetStreamIsCurrent() bool {
	js := s.getJetStream()
	if js == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.cluster.isCurrent()
}

func (s *Server) JetStreamSnapshotMeta() error {
	js := s.getJetStream()
	if js == nil {
		return NewJSNotEnabledError()
	}
	js.mu.RLock()
	cc := js.cluster
	isLeader := cc.isLeader()
	meta := cc.meta
	js.mu.RUnlock()

	if !isLeader {
		return errNotLeader
	}

	return meta.InstallSnapshot(js.metaSnapshot())
}

func (s *Server) JetStreamStepdownStream(account, stream string) error {
	js, cc := s.getJetStreamCluster()
	if js == nil {
		return NewJSNotEnabledError()
	}
	if cc == nil {
		return NewJSClusterNotActiveError()
	}
	// Grab account
	acc, err := s.LookupAccount(account)
	if err != nil {
		return err
	}
	// Grab stream
	mset, err := acc.lookupStream(stream)
	if err != nil {
		return err
	}

	if node := mset.raftNode(); node != nil && node.Leader() {
		node.StepDown()
	}

	return nil
}

func (s *Server) JetStreamSnapshotStream(account, stream string) error {
	js, cc := s.getJetStreamCluster()
	if js == nil {
		return NewJSNotEnabledForAccountError()
	}
	if cc == nil {
		return NewJSClusterNotActiveError()
	}
	// Grab account
	acc, err := s.LookupAccount(account)
	if err != nil {
		return err
	}
	// Grab stream
	mset, err := acc.lookupStream(stream)
	if err != nil {
		return err
	}

	mset.mu.RLock()
	if !mset.node.Leader() {
		mset.mu.RUnlock()
		return NewJSNotEnabledForAccountError()
	}
	n := mset.node
	mset.mu.RUnlock()

	return n.InstallSnapshot(mset.stateSnapshot())
}

func (s *Server) JetStreamClusterPeers() []string {
	js := s.getJetStream()
	if js == nil {
		return nil
	}
	js.mu.RLock()
	defer js.mu.RUnlock()

	cc := js.cluster
	if !cc.isLeader() {
		return nil
	}
	peers := cc.meta.Peers()
	var nodes []string
	for _, p := range peers {
		si, ok := s.nodeToInfo.Load(p.ID)
		if !ok || si == nil {
			continue
		}
		ni := si.(nodeInfo)
		// Ignore if offline, no JS, or no current stats have been received.
		if ni.offline || !ni.js || ni.stats == nil {
			continue
		}
		nodes = append(nodes, si.(nodeInfo).name)
	}
	return nodes
}

// Read lock should be held.
func (cc *jetStreamCluster) isLeader() bool {
	if cc == nil {
		// Non-clustered mode
		return true
	}
	return cc.meta != nil && cc.meta.Leader()
}

// isCurrent will determine if this node is a leader or an up to date follower.
// Read lock should be held.
func (cc *jetStreamCluster) isCurrent() bool {
	if cc == nil {
		// Non-clustered mode
		return true
	}
	if cc.meta == nil {
		return false
	}
	return cc.meta.Current()
}

// isStreamCurrent will determine if this node is a participant for the stream and if its up to date.
// Read lock should be held.
func (cc *jetStreamCluster) isStreamCurrent(account, stream string) bool {
	if cc == nil {
		// Non-clustered mode
		return true
	}
	as := cc.streams[account]
	if as == nil {
		return false
	}
	sa := as[stream]
	if sa == nil {
		return false
	}
	rg := sa.Group
	if rg == nil || rg.node == nil {
		return false
	}

	isCurrent := rg.node.Current()
	if isCurrent {
		// Check if we are processing a snapshot and are catching up.
		acc, err := cc.s.LookupAccount(account)
		if err != nil {
			return false
		}
		mset, err := acc.lookupStream(stream)
		if err != nil {
			return false
		}
		if mset.isCatchingUp() {
			return false
		}
	}

	return isCurrent
}

func (a *Account) getJetStreamFromAccount() (*Server, *jetStream, *jsAccount) {
	a.mu.RLock()
	jsa := a.js
	a.mu.RUnlock()
	if jsa == nil {
		return nil, nil, nil
	}
	jsa.mu.RLock()
	js := jsa.js
	jsa.mu.RUnlock()
	if js == nil {
		return nil, nil, nil
	}
	js.mu.RLock()
	s := js.srv
	js.mu.RUnlock()
	return s, js, jsa
}

func (s *Server) JetStreamIsStreamLeader(account, stream string) bool {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return cc.isStreamLeader(account, stream)
}

func (a *Account) JetStreamIsStreamLeader(stream string) bool {
	s, js, jsa := a.getJetStreamFromAccount()
	if s == nil || js == nil || jsa == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.cluster.isStreamLeader(a.Name, stream)
}

func (s *Server) JetStreamIsStreamCurrent(account, stream string) bool {
	js, cc := s.getJetStreamCluster()
	if js == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return cc.isStreamCurrent(account, stream)
}

func (a *Account) JetStreamIsConsumerLeader(stream, consumer string) bool {
	s, js, jsa := a.getJetStreamFromAccount()
	if s == nil || js == nil || jsa == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.cluster.isConsumerLeader(a.Name, stream, consumer)
}

func (s *Server) JetStreamIsConsumerLeader(account, stream, consumer string) bool {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return cc.isConsumerLeader(account, stream, consumer)
}

func (s *Server) enableJetStreamClustering() error {
	if !s.isRunning() {
		return nil
	}
	js := s.getJetStream()
	if js == nil {
		return NewJSNotEnabledForAccountError()
	}
	// Already set.
	if js.cluster != nil {
		return nil
	}

	s.Noticef("Starting JetStream cluster")
	// We need to determine if we have a stable cluster name and expected number of servers.
	s.Debugf("JetStream cluster checking for stable cluster name and peers")

	hasLeafNodeSystemShare := s.canExtendOtherDomain()
	if s.isClusterNameDynamic() && !hasLeafNodeSystemShare {
		return errors.New("JetStream cluster requires cluster name")
	}
	if s.configuredRoutes() == 0 && !hasLeafNodeSystemShare {
		return errors.New("JetStream cluster requires configured routes or solicited leafnode for the system account")
	}

	return js.setupMetaGroup()
}

// isClustered returns if we are clustered.
// Lock should not be held.
func (js *jetStream) isClustered() bool {
	js.mu.RLock()
	isClustered := js.cluster != nil
	js.mu.RUnlock()
	return isClustered
}

func (js *jetStream) setupMetaGroup() error {
	s := js.srv
	s.Noticef("Creating JetStream metadata controller")

	// Setup our WAL for the metagroup.
	sysAcc := s.SystemAccount()
	storeDir := filepath.Join(js.config.StoreDir, sysAcc.Name, defaultStoreDirName, defaultMetaGroupName)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir, BlockSize: defaultMetaFSBlkSize, AsyncFlush: false},
		StreamConfig{Name: defaultMetaGroupName, Storage: FileStorage},
	)
	if err != nil {
		s.Errorf("Error creating filestore: %v", err)
		return err
	}

	cfg := &RaftConfig{Name: defaultMetaGroupName, Store: storeDir, Log: fs}

	// If we are soliciting leafnode connections and we are sharing a system account and do not disable it with a hint,
	// we want to move to observer mode so that we extend the solicited cluster or supercluster but do not form our own.
	cfg.Observer = s.canExtendOtherDomain() && s.opts.JetStreamExtHint != jsNoExtend

	var bootstrap bool
	if ps, err := readPeerState(storeDir); err != nil {
		s.Noticef("JetStream cluster bootstrapping")
		bootstrap = true
		peers := s.ActivePeers()
		s.Debugf("JetStream cluster initial peers: %+v", peers)
		if err := s.bootstrapRaftNode(cfg, peers, false); err != nil {
			return err
		}
		if cfg.Observer {
			s.Noticef("Turning JetStream metadata controller Observer Mode on")
		}
	} else {
		s.Noticef("JetStream cluster recovering state")
		// correlate the value of observer with observations from a previous run.
		if cfg.Observer {
			switch ps.domainExt {
			case extExtended:
				s.Noticef("Keeping JetStream metadata controller Observer Mode on - due to previous contact")
			case extNotExtended:
				s.Noticef("Turning JetStream metadata controller Observer Mode off - due to previous contact")
				cfg.Observer = false
			case extUndetermined:
				s.Noticef("Turning JetStream metadata controller Observer Mode on - no previous contact")
				s.Noticef("In cases where JetStream will not be extended")
				s.Noticef("and waiting for leader election until first contact is not acceptable,")
				s.Noticef(`manually disable Observer Mode by setting the JetStream Option "extension_hint: %s"`, jsNoExtend)
			}
		} else {
			// To track possible configuration changes, responsible for an altered value of cfg.Observer,
			// set extension state to undetermined.
			ps.domainExt = extUndetermined
			if err := writePeerState(storeDir, ps); err != nil {
				return err
			}
		}
	}

	// Start up our meta node.
	n, err := s.startRaftNode(cfg)
	if err != nil {
		s.Warnf("Could not start metadata controller: %v", err)
		return err
	}

	// If we are bootstrapped with no state, start campaign early.
	if bootstrap {
		n.Campaign()
	}

	c := s.createInternalJetStreamClient()
	sacc := s.SystemAccount()

	js.mu.Lock()
	defer js.mu.Unlock()
	js.cluster = &jetStreamCluster{
		meta:    n,
		streams: make(map[string]map[string]*streamAssignment),
		s:       s,
		c:       c,
	}
	c.registerWithAccount(sacc)

	js.srv.startGoRoutine(js.monitorCluster)
	return nil
}

func (js *jetStream) getMetaGroup() RaftNode {
	js.mu.RLock()
	defer js.mu.RUnlock()
	if js.cluster == nil {
		return nil
	}
	return js.cluster.meta
}

func (js *jetStream) server() *Server {
	js.mu.RLock()
	s := js.srv
	js.mu.RUnlock()
	return s
}

// Will respond if we do not think we have a metacontroller leader.
func (js *jetStream) isLeaderless() bool {
	js.mu.RLock()
	defer js.mu.RUnlock()

	cc := js.cluster
	if cc == nil || cc.meta == nil {
		return false
	}

	// If we don't have a leader.
	// Make sure we have been running for enough time.
	if cc.meta.GroupLeader() == _EMPTY_ && time.Since(cc.meta.Created()) > lostQuorumInterval {
		return true
	}
	return false
}

// Will respond iff we are a member and we know we have no leader.
func (js *jetStream) isGroupLeaderless(rg *raftGroup) bool {
	if rg == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()

	cc := js.cluster

	// If we are not a member we can not say..
	if !rg.isMember(cc.meta.ID()) {
		return false
	}
	// Single peer groups always have a leader if we are here.
	if rg.node == nil {
		return false
	}
	// If we don't have a leader.
	if rg.node.GroupLeader() == _EMPTY_ {
		if rg.node.HadPreviousLeader() {
			return true
		}
		// Make sure we have been running for enough time.
		if time.Since(rg.node.Created()) > lostQuorumInterval {
			return true
		}
	}

	return false
}

func (s *Server) JetStreamIsStreamAssigned(account, stream string) bool {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return false
	}
	acc, _ := s.LookupAccount(account)
	if acc == nil {
		return false
	}
	return cc.isStreamAssigned(acc, stream)
}

// streamAssigned informs us if this server has this stream assigned.
func (jsa *jsAccount) streamAssigned(stream string) bool {
	jsa.mu.RLock()
	js, acc := jsa.js, jsa.account
	jsa.mu.RUnlock()

	if js == nil {
		return false
	}
	js.mu.RLock()
	assigned := js.cluster.isStreamAssigned(acc, stream)
	js.mu.RUnlock()
	return assigned
}

// Read lock should be held.
func (cc *jetStreamCluster) isStreamAssigned(a *Account, stream string) bool {
	// Non-clustered mode always return true.
	if cc == nil {
		return true
	}
	as := cc.streams[a.Name]
	if as == nil {
		return false
	}
	sa := as[stream]
	if sa == nil {
		return false
	}
	rg := sa.Group
	if rg == nil {
		return false
	}
	// Check if we are the leader of this raftGroup assigned to the stream.
	ourID := cc.meta.ID()
	for _, peer := range rg.Peers {
		if peer == ourID {
			return true
		}
	}
	return false
}

// Read lock should be held.
func (cc *jetStreamCluster) isStreamLeader(account, stream string) bool {
	// Non-clustered mode always return true.
	if cc == nil {
		return true
	}
	if cc.meta == nil {
		return false
	}

	var sa *streamAssignment
	if as := cc.streams[account]; as != nil {
		sa = as[stream]
	}
	if sa == nil {
		return false
	}
	rg := sa.Group
	if rg == nil {
		return false
	}
	// Check if we are the leader of this raftGroup assigned to the stream.
	ourID := cc.meta.ID()
	for _, peer := range rg.Peers {
		if peer == ourID {
			if len(rg.Peers) == 1 || rg.node != nil && rg.node.Leader() {
				return true
			}
		}
	}
	return false
}

// Read lock should be held.
func (cc *jetStreamCluster) isConsumerLeader(account, stream, consumer string) bool {
	// Non-clustered mode always return true.
	if cc == nil {
		return true
	}
	if cc.meta == nil {
		return false
	}

	var sa *streamAssignment
	if as := cc.streams[account]; as != nil {
		sa = as[stream]
	}
	if sa == nil {
		return false
	}
	// Check if we are the leader of this raftGroup assigned to this consumer.
	ca := sa.consumers[consumer]
	if ca == nil {
		return false
	}
	rg := ca.Group
	ourID := cc.meta.ID()
	for _, peer := range rg.Peers {
		if peer == ourID {
			if len(rg.Peers) == 1 || (rg.node != nil && rg.node.Leader()) {
				return true
			}
		}
	}
	return false
}

func (js *jetStream) monitorCluster() {
	s, n := js.server(), js.getMetaGroup()
	qch, lch, aq := n.QuitC(), n.LeadChangeC(), n.ApplyQ()

	defer s.grWG.Done()

	s.Debugf("Starting metadata monitor")
	defer s.Debugf("Exiting metadata monitor")

	const compactInterval = 2 * time.Minute
	t := time.NewTicker(compactInterval)
	defer t.Stop()

	// Used to check cold boot cluster when possibly in mixed mode.
	const leaderCheckInterval = time.Second
	lt := time.NewTicker(leaderCheckInterval)
	defer lt.Stop()

	var (
		isLeader     bool
		lastSnap     []byte
		lastSnapTime time.Time
		isRecovering bool
		beenLeader   bool
	)

	// Set to true to start.
	isRecovering = true

	// Snapshotting function.
	doSnapshot := func() {
		// Suppress during recovery.
		if isRecovering {
			return
		}
		if snap := js.metaSnapshot(); !bytes.Equal(lastSnap, snap) {
			if err := n.InstallSnapshot(snap); err == nil {
				lastSnap = snap
				lastSnapTime = time.Now()
			}
		}
	}

	for {
		select {
		case <-s.quitCh:
			return
		case <-qch:
			return
		case <-aq.ch:
			ces := aq.pop()
			for _, cei := range ces {
				if cei == nil {
					// Signals we have replayed all of our metadata.
					isRecovering = false
					s.Debugf("Recovered JetStream cluster metadata")
					continue
				}
				ce := cei.(*CommittedEntry)
				// FIXME(dlc) - Deal with errors.
				if didSnap, didRemoval, err := js.applyMetaEntries(ce.Entries, isRecovering); err == nil {
					_, nb := n.Applied(ce.Index)
					if js.hasPeerEntries(ce.Entries) || didSnap || (didRemoval && time.Since(lastSnapTime) > 2*time.Second) {
						// Since we received one make sure we have our own since we do not store
						// our meta state outside of raft.
						doSnapshot()
					} else if lls := len(lastSnap); nb > uint64(lls*8) && lls > 0 {
						doSnapshot()
					}
				}
			}
			aq.recycle(&ces)
		case isLeader = <-lch:
			// We want to make sure we are updated on statsz so ping the extended cluster.
			if isLeader {
				s.sendInternalMsgLocked(serverStatsPingReqSubj, _EMPTY_, nil, nil)
			}
			js.processLeaderChange(isLeader)
			if isLeader && !beenLeader {
				beenLeader = true
				if n.NeedSnapshot() {
					if err := n.InstallSnapshot(js.metaSnapshot()); err != nil {
						s.Warnf("Error snapshotting JetStream cluster state: %v", err)
					}
				}
				js.checkClusterSize()
			}
		case <-t.C:
			doSnapshot()
			// Periodically check the cluster size.
			if n.Leader() {
				js.checkClusterSize()
			}
		case <-lt.C:
			s.Debugf("Checking JetStream cluster state")
			// If we have a current leader or had one in the past we can cancel this here since the metaleader
			// will be in charge of all peer state changes.
			// For cold boot only.
			if n.GroupLeader() != _EMPTY_ || n.HadPreviousLeader() {
				lt.Stop()
				continue
			}
			// If we are here we do not have a leader and we did not have a previous one, so cold start.
			// Check to see if we can adjust our cluster size down iff we are in mixed mode and we have
			// seen a total that is what our original estimate was.
			cs := n.ClusterSize()
			if js, total := s.trackedJetStreamServers(); js < total && total >= cs && js != cs {
				s.Noticef("Adjusting JetStream expected peer set size to %d from original %d", js, cs)
				n.AdjustBootClusterSize(js)
			}
		}
	}
}

// This is called on first leader transition to double check the peers and cluster set size.
func (js *jetStream) checkClusterSize() {
	s, n := js.server(), js.getMetaGroup()
	if n == nil {
		return
	}
	// We will check that we have a correct cluster set size by checking for any non-js servers
	// which can happen in mixed mode.
	ps := n.(*raft).currentPeerState()
	if len(ps.knownPeers) >= ps.clusterSize {
		return
	}

	// Grab our active peers.
	peers := s.ActivePeers()

	// If we have not registered all of our peers yet we can't do
	// any adjustments based on a mixed mode. We will periodically check back.
	if len(peers) < ps.clusterSize {
		return
	}

	s.Debugf("Checking JetStream cluster size")

	// If we are here our known set as the leader is not the same as the cluster size.
	// Check to see if we have a mixed mode setup.
	var totalJS int
	for _, p := range peers {
		if si, ok := s.nodeToInfo.Load(p); ok && si != nil {
			if si.(nodeInfo).js {
				totalJS++
			}
		}
	}
	// If we have less then our cluster size adjust that here. Can not do individual peer removals since
	// they will not be in the tracked peers.
	if totalJS < ps.clusterSize {
		s.Debugf("Adjusting JetStream cluster size from %d to %d", ps.clusterSize, totalJS)
		if err := n.AdjustClusterSize(totalJS); err != nil {
			s.Warnf("Error adjusting JetStream cluster size: %v", err)
		}
	}
}

// Represents our stable meta state that we can write out.
type writeableStreamAssignment struct {
	Client    *ClientInfo   `json:"client,omitempty"`
	Created   time.Time     `json:"created"`
	Config    *StreamConfig `json:"stream"`
	Group     *raftGroup    `json:"group"`
	Sync      string        `json:"sync"`
	Consumers []*consumerAssignment
}

func (js *jetStream) metaSnapshot() []byte {
	var streams []writeableStreamAssignment

	js.mu.RLock()
	cc := js.cluster
	for _, asa := range cc.streams {
		for _, sa := range asa {
			wsa := writeableStreamAssignment{
				Client:  sa.Client,
				Created: sa.Created,
				Config:  sa.Config,
				Group:   sa.Group,
				Sync:    sa.Sync,
			}
			for _, ca := range sa.consumers {
				wsa.Consumers = append(wsa.Consumers, ca)
			}
			streams = append(streams, wsa)
		}
	}

	if len(streams) == 0 {
		js.mu.RUnlock()
		return nil
	}

	b, _ := json.Marshal(streams)
	js.mu.RUnlock()

	return s2.EncodeBetter(nil, b)
}

func (js *jetStream) applyMetaSnapshot(buf []byte, isRecovering bool) error {
	if len(buf) == 0 {
		return nil
	}
	jse, err := s2.Decode(nil, buf)
	if err != nil {
		return err
	}
	var wsas []writeableStreamAssignment
	if err = json.Unmarshal(jse, &wsas); err != nil {
		return err
	}

	// Build our new version here outside of js.
	streams := make(map[string]map[string]*streamAssignment)
	for _, wsa := range wsas {
		as := streams[wsa.Client.serviceAccount()]
		if as == nil {
			as = make(map[string]*streamAssignment)
			streams[wsa.Client.serviceAccount()] = as
		}
		sa := &streamAssignment{Client: wsa.Client, Created: wsa.Created, Config: wsa.Config, Group: wsa.Group, Sync: wsa.Sync}
		if len(wsa.Consumers) > 0 {
			sa.consumers = make(map[string]*consumerAssignment)
			for _, ca := range wsa.Consumers {
				sa.consumers[ca.Name] = ca
			}
		}
		as[wsa.Config.Name] = sa
	}

	js.mu.Lock()
	cc := js.cluster

	var saAdd, saDel, saChk []*streamAssignment
	// Walk through the old list to generate the delete list.
	for account, asa := range cc.streams {
		nasa := streams[account]
		for sn, sa := range asa {
			if nsa := nasa[sn]; nsa == nil {
				saDel = append(saDel, sa)
			} else {
				saChk = append(saChk, nsa)
			}
		}
	}
	// Walk through the new list to generate the add list.
	for account, nasa := range streams {
		asa := cc.streams[account]
		for sn, sa := range nasa {
			if asa[sn] == nil {
				saAdd = append(saAdd, sa)
			}
		}
	}
	// Now walk the ones to check and process consumers.
	var caAdd, caDel []*consumerAssignment
	for _, sa := range saChk {
		if osa := js.streamAssignment(sa.Client.serviceAccount(), sa.Config.Name); osa != nil {
			for _, ca := range osa.consumers {
				if sa.consumers[ca.Name] == nil {
					caDel = append(caDel, ca)
				} else {
					caAdd = append(caAdd, ca)
				}
			}
		}
	}
	js.mu.Unlock()

	// Do removals first.
	for _, sa := range saDel {
		if isRecovering {
			js.setStreamAssignmentRecovering(sa)
		}
		js.processStreamRemoval(sa)
	}
	// Now do add for the streams. Also add in all consumers.
	for _, sa := range saAdd {
		if isRecovering {
			js.setStreamAssignmentRecovering(sa)
		}
		js.processStreamAssignment(sa)
		// We can simply add the consumers.
		for _, ca := range sa.consumers {
			if isRecovering {
				js.setConsumerAssignmentRecovering(ca)
			}
			js.processConsumerAssignment(ca)
		}
	}
	// Now do the deltas for existing stream's consumers.
	for _, ca := range caDel {
		if isRecovering {
			js.setConsumerAssignmentRecovering(ca)
		}
		js.processConsumerRemoval(ca)
	}
	for _, ca := range caAdd {
		if isRecovering {
			js.setConsumerAssignmentRecovering(ca)
		}
		js.processConsumerAssignment(ca)
	}

	return nil
}

// Called on recovery to make sure we do not process like original.
func (js *jetStream) setStreamAssignmentRecovering(sa *streamAssignment) {
	js.mu.Lock()
	defer js.mu.Unlock()
	sa.responded = true
	sa.Restore = nil
	if sa.Group != nil {
		sa.Group.Preferred = _EMPTY_
	}
}

// Called on recovery to make sure we do not process like original.
func (js *jetStream) setConsumerAssignmentRecovering(ca *consumerAssignment) {
	js.mu.Lock()
	defer js.mu.Unlock()
	ca.responded = true
	if ca.Group != nil {
		ca.Group.Preferred = _EMPTY_
	}
}

// Just copies over and changes out the group so it can be encoded.
// Lock should be held.
func (sa *streamAssignment) copyGroup() *streamAssignment {
	csa, cg := *sa, *sa.Group
	csa.Group = &cg
	csa.Group.Peers = copyStrings(sa.Group.Peers)
	return &csa
}

// Just copies over and changes out the group so it can be encoded.
// Lock should be held.
func (ca *consumerAssignment) copyGroup() *consumerAssignment {
	cca, cg := *ca, *ca.Group
	cca.Group = &cg
	cca.Group.Peers = copyStrings(ca.Group.Peers)
	return &cca
}

// Lock should be held.
func (sa *streamAssignment) missingPeers() bool {
	return len(sa.Group.Peers) < sa.Config.Replicas
}

// Called when we detect a new peer. Only the leader will process checking
// for any streams, and consequently any consumers.
func (js *jetStream) processAddPeer(peer string) {
	js.mu.Lock()
	defer js.mu.Unlock()

	s, cc := js.srv, js.cluster
	isLeader := cc.isLeader()

	// Now check if we are meta-leader. We will check for any re-assignments.
	if !isLeader {
		return
	}

	sir, ok := s.nodeToInfo.Load(peer)
	if !ok || sir == nil {
		return
	}
	si := sir.(nodeInfo)

	for _, asa := range cc.streams {
		for _, sa := range asa {
			if sa.missingPeers() {
				// Make sure the right cluster etc.
				if si.cluster != sa.Client.Cluster {
					continue
				}
				// If we are here we can add in this peer.
				csa := sa.copyGroup()
				csa.Group.Peers = append(csa.Group.Peers, peer)
				// Send our proposal for this csa. Also use same group definition for all the consumers as well.
				cc.meta.Propose(encodeAddStreamAssignment(csa))
				for _, ca := range sa.consumers {
					// Ephemerals are R=1, so only auto-remap durables, or R>1.
					if ca.Config.Durable != _EMPTY_ || len(ca.Group.Peers) > 1 {
						cca := ca.copyGroup()
						cca.Group.Peers = csa.Group.Peers
						cc.meta.Propose(encodeAddConsumerAssignment(cca))
					}
				}
			}
		}
	}
}

func (js *jetStream) processRemovePeer(peer string) {
	js.mu.Lock()
	s, cc := js.srv, js.cluster
	isLeader := cc.isLeader()
	// All nodes will check if this is them.
	isUs := cc.meta.ID() == peer
	disabled := js.disabled
	js.mu.Unlock()

	// We may be already disabled.
	if disabled {
		return
	}

	if isUs {
		s.Errorf("JetStream being DISABLED, our server was removed from the cluster")
		adv := &JSServerRemovedAdvisory{
			TypedEvent: TypedEvent{
				Type: JSServerRemovedAdvisoryType,
				ID:   nuid.Next(),
				Time: time.Now().UTC(),
			},
			Server:   s.Name(),
			ServerID: s.ID(),
			Cluster:  s.cachedClusterName(),
			Domain:   s.getOpts().JetStreamDomain,
		}
		s.publishAdvisory(nil, JSAdvisoryServerRemoved, adv)

		go s.DisableJetStream()
	}

	// Now check if we are meta-leader. We will attempt re-assignment.
	if !isLeader {
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	for _, asa := range cc.streams {
		for _, sa := range asa {
			if rg := sa.Group; rg.isMember(peer) {
				js.removePeerFromStreamLocked(sa, peer)
			}
		}
	}
}

// Assumes all checks have already been done.
func (js *jetStream) removePeerFromStream(sa *streamAssignment, peer string) bool {
	js.mu.Lock()
	defer js.mu.Unlock()
	return js.removePeerFromStreamLocked(sa, peer)
}

// Lock should be held.
func (js *jetStream) removePeerFromStreamLocked(sa *streamAssignment, peer string) bool {
	if rg := sa.Group; !rg.isMember(peer) {
		return false
	}

	s, cc, csa := js.srv, js.cluster, sa.copyGroup()
	replaced := cc.remapStreamAssignment(csa, peer)
	if !replaced {
		s.Warnf("JetStream cluster could not replace peer for stream '%s > %s'", sa.Client.serviceAccount(), sa.Config.Name)
	}

	// Send our proposal for this csa. Also use same group definition for all the consumers as well.
	cc.meta.Propose(encodeAddStreamAssignment(csa))
	rg := csa.Group
	for _, ca := range sa.consumers {
		// Ephemerals are R=1, so only auto-remap durables, or R>1.
		if ca.Config.Durable != _EMPTY_ {
			cca := ca.copyGroup()
			cca.Group.Peers, cca.Group.Preferred = rg.Peers, _EMPTY_
			cc.meta.Propose(encodeAddConsumerAssignment(cca))
		} else if ca.Group.isMember(peer) {
			// These are ephemerals. Check to see if we deleted this peer.
			cc.meta.Propose(encodeDeleteConsumerAssignment(ca))
		}
	}
	return replaced
}

// Check if we have peer related entries.
func (js *jetStream) hasPeerEntries(entries []*Entry) bool {
	for _, e := range entries {
		if e.Type == EntryRemovePeer || e.Type == EntryAddPeer {
			return true
		}
	}
	return false
}

func (js *jetStream) applyMetaEntries(entries []*Entry, isRecovering bool) (bool, bool, error) {
	var didSnap, didRemove bool
	for _, e := range entries {
		if e.Type == EntrySnapshot {
			js.applyMetaSnapshot(e.Data, isRecovering)
			didSnap = true
		} else if e.Type == EntryRemovePeer {
			if !isRecovering {
				js.processRemovePeer(string(e.Data))
			}
		} else if e.Type == EntryAddPeer {
			if !isRecovering {
				js.processAddPeer(string(e.Data))
			}
		} else {
			buf := e.Data
			switch entryOp(buf[0]) {
			case assignStreamOp:
				sa, err := decodeStreamAssignment(buf[1:])
				if err != nil {
					js.srv.Errorf("JetStream cluster failed to decode stream assignment: %q", buf[1:])
					return didSnap, didRemove, err
				}
				if isRecovering {
					js.setStreamAssignmentRecovering(sa)
				}
				didRemove = js.processStreamAssignment(sa)
			case removeStreamOp:
				sa, err := decodeStreamAssignment(buf[1:])
				if err != nil {
					js.srv.Errorf("JetStream cluster failed to decode stream assignment: %q", buf[1:])
					return didSnap, didRemove, err
				}
				if isRecovering {
					js.setStreamAssignmentRecovering(sa)
				}
				js.processStreamRemoval(sa)
				didRemove = true
			case assignConsumerOp:
				ca, err := decodeConsumerAssignment(buf[1:])
				if err != nil {
					js.srv.Errorf("JetStream cluster failed to decode consumer assignment: %q", buf[1:])
					return didSnap, didRemove, err
				}
				if isRecovering {
					js.setConsumerAssignmentRecovering(ca)
				}
				js.processConsumerAssignment(ca)
			case assignCompressedConsumerOp:
				ca, err := decodeConsumerAssignmentCompressed(buf[1:])
				if err != nil {
					js.srv.Errorf("JetStream cluster failed to decode compressed consumer assignment: %q", buf[1:])
					return didSnap, didRemove, err
				}
				if isRecovering {
					js.setConsumerAssignmentRecovering(ca)
				}
				js.processConsumerAssignment(ca)
			case removeConsumerOp:
				ca, err := decodeConsumerAssignment(buf[1:])
				if err != nil {
					js.srv.Errorf("JetStream cluster failed to decode consumer assignment: %q", buf[1:])
					return didSnap, didRemove, err
				}
				if isRecovering {
					js.setConsumerAssignmentRecovering(ca)
				}
				js.processConsumerRemoval(ca)
				didRemove = true
			case updateStreamOp:
				sa, err := decodeStreamAssignment(buf[1:])
				if err != nil {
					js.srv.Errorf("JetStream cluster failed to decode stream assignment: %q", buf[1:])
					return didSnap, didRemove, err
				}
				if isRecovering {
					js.setStreamAssignmentRecovering(sa)
				}
				js.processUpdateStreamAssignment(sa)
			default:
				panic("JetStream Cluster Unknown meta entry op type")
			}
		}
	}
	return didSnap, didRemove, nil
}

func (rg *raftGroup) isMember(id string) bool {
	if rg == nil {
		return false
	}
	for _, peer := range rg.Peers {
		if peer == id {
			return true
		}
	}
	return false
}

func (rg *raftGroup) setPreferred() {
	if rg == nil || len(rg.Peers) == 0 {
		return
	}
	if len(rg.Peers) == 1 {
		rg.Preferred = rg.Peers[0]
	} else {
		// For now just randomly select a peer for the preferred.
		pi := rand.Int31n(int32(len(rg.Peers)))
		rg.Preferred = rg.Peers[pi]
	}
}

// createRaftGroup is called to spin up this raft group if needed.
func (js *jetStream) createRaftGroup(rg *raftGroup, storage StorageType) error {
	js.mu.Lock()
	defer js.mu.Unlock()
	s, cc := js.srv, js.cluster
	if cc == nil || cc.meta == nil {
		return NewJSClusterNotActiveError()
	}

	// If this is a single peer raft group or we are not a member return.
	if len(rg.Peers) <= 1 || !rg.isMember(cc.meta.ID()) {
		// Nothing to do here.
		return nil
	}

	// Check if we already have this assigned.
	if node := s.lookupRaftNode(rg.Name); node != nil {
		s.Debugf("JetStream cluster already has raft group %q assigned", rg.Name)
		rg.node = node
		return nil
	}

	s.Debugf("JetStream cluster creating raft group:%+v", rg)

	sysAcc := s.SystemAccount()
	if sysAcc == nil {
		s.Debugf("JetStream cluster detected shutdown processing raft group: %+v", rg)
		return errors.New("shutting down")
	}

	storeDir := filepath.Join(js.config.StoreDir, sysAcc.Name, defaultStoreDirName, rg.Name)
	var store StreamStore
	if storage == FileStorage {
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: storeDir, BlockSize: 4_000_000, AsyncFlush: false, SyncInterval: 5 * time.Minute},
			StreamConfig{Name: rg.Name, Storage: FileStorage},
		)
		if err != nil {
			s.Errorf("Error creating filestore WAL: %v", err)
			return err
		}
		store = fs
	} else {
		ms, err := newMemStore(&StreamConfig{Name: rg.Name, Storage: MemoryStorage})
		if err != nil {
			s.Errorf("Error creating memstore WAL: %v", err)
			return err
		}
		store = ms
	}

	cfg := &RaftConfig{Name: rg.Name, Store: storeDir, Log: store, Track: true}

	if _, err := readPeerState(storeDir); err != nil {
		s.bootstrapRaftNode(cfg, rg.Peers, true)
	}

	n, err := s.startRaftNode(cfg)
	if err != nil || n == nil {
		s.Debugf("Error creating raft group: %v", err)
		return err
	}
	rg.node = n

	// See if we are preferred and should start campaign immediately.
	if n.ID() == rg.Preferred {
		n.Campaign()
	}

	return nil
}

func (mset *stream) raftGroup() *raftGroup {
	if mset == nil {
		return nil
	}
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	if mset.sa == nil {
		return nil
	}
	return mset.sa.Group
}

func (mset *stream) raftNode() RaftNode {
	if mset == nil {
		return nil
	}
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.node
}

func (mset *stream) removeNode() {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	if n := mset.node; n != nil {
		n.Delete()
		mset.node = nil
	}
}

// Monitor our stream node for this stream.
func (js *jetStream) monitorStream(mset *stream, sa *streamAssignment) {
	s, cc, n := js.server(), js.cluster, sa.Group.node
	defer s.grWG.Done()

	if n == nil {
		s.Warnf("No RAFT group for '%s > %s'", sa.Client.serviceAccount(), sa.Config.Name)
		return
	}

	qch, lch, aq := n.QuitC(), n.LeadChangeC(), n.ApplyQ()

	s.Debugf("Starting stream monitor for '%s > %s' [%s]", sa.Client.serviceAccount(), sa.Config.Name, n.Group())
	defer s.Debugf("Exiting stream monitor for '%s > %s' [%s]", sa.Client.serviceAccount(), sa.Config.Name, n.Group())

	// Make sure we do not leave the apply channel to fill up and block the raft layer.
	defer func() {
		if n.State() == Closed {
			return
		}
		if n.Leader() {
			n.StepDown()
		}
		// Drain the commit queue...
		aq.drain()
	}()

	const (
		compactInterval = 2 * time.Minute
		compactSizeMin  = 32 * 1024 * 1024
		compactNumMin   = 8192
	)

	t := time.NewTicker(compactInterval)
	defer t.Stop()

	js.mu.RLock()
	isLeader := cc.isStreamLeader(sa.Client.serviceAccount(), sa.Config.Name)
	isRestore := sa.Restore != nil
	js.mu.RUnlock()

	acc, err := s.LookupAccount(sa.Client.serviceAccount())
	if err != nil {
		s.Warnf("Could not retrieve account for stream '%s > %s", sa.Client.serviceAccount(), sa.Config.Name)
		return
	}

	var lastSnap []byte

	// Should only to be called from leader.
	doSnapshot := func() {
		if mset == nil || isRestore {
			return
		}
		if snap := mset.stateSnapshot(); !bytes.Equal(lastSnap, snap) {
			if err := n.InstallSnapshot(snap); err == nil {
				lastSnap = snap
			}
		}
	}

	// We will establish a restoreDoneCh no matter what. Will never be triggered unless
	// we replace with the restore chan.
	restoreDoneCh := make(<-chan error)
	isRecovering := true

	for {
		select {
		case <-s.quitCh:
			return
		case <-qch:
			return
		case <-aq.ch:
			ces := aq.pop()
			for _, cei := range ces {
				// No special processing needed for when we are caught up on restart.
				if cei == nil {
					isRecovering = false
					// Check on startup if we should snapshot/compact.
					if _, b := n.Size(); b > compactSizeMin || n.NeedSnapshot() {
						doSnapshot()
					}
					continue
				}
				ce := cei.(*CommittedEntry)
				// Apply our entries.
				if err := js.applyStreamEntries(mset, ce, isRecovering); err == nil {
					ne, nb := n.Applied(ce.Index)
					// If we have at least min entries to compact, go ahead and snapshot/compact.
					if ne >= compactNumMin || nb > compactSizeMin {
						doSnapshot()
					}
				} else {
					s.Warnf("Error applying entries to '%s > %s': %v", sa.Client.serviceAccount(), sa.Config.Name, err)
					if isClusterResetErr(err) {
						if mset.isMirror() && mset.IsLeader() {
							mset.retryMirrorConsumer()
							continue
						}
						// We will attempt to reset our cluster state.
						if mset.resetClusteredState(err) {
							aq.recycle(&ces)
							return
						}
					} else if isOutOfSpaceErr(err) {
						// If applicable this will tear all of this down, but don't assume so and return.
						s.handleOutOfSpace(mset)
					}
				}
			}
			aq.recycle(&ces)
		case isLeader = <-lch:
			if isLeader {
				if isRestore {
					acc, _ := s.LookupAccount(sa.Client.serviceAccount())
					restoreDoneCh = s.processStreamRestore(sa.Client, acc, sa.Config, _EMPTY_, sa.Reply, _EMPTY_)
					continue
				} else if n.NeedSnapshot() {
					doSnapshot()
				}
			} else if n.GroupLeader() != noLeader {
				js.setStreamAssignmentRecovering(sa)
			}
			js.processStreamLeaderChange(mset, isLeader)
		case <-t.C:
			doSnapshot()
		case err := <-restoreDoneCh:
			// We have completed a restore from snapshot on this server. The stream assignment has
			// already been assigned but the replicas will need to catch up out of band. Consumers
			// will need to be assigned by forwarding the proposal and stamping the initial state.
			s.Debugf("Stream restore for '%s > %s' completed", sa.Client.serviceAccount(), sa.Config.Name)
			if err != nil {
				s.Debugf("Stream restore failed: %v", err)
			}
			isRestore = false
			sa.Restore = nil
			// If we were successful lookup up our stream now.
			if err == nil {
				mset, err = acc.lookupStream(sa.Config.Name)
				if mset != nil {
					mset.setStreamAssignment(sa)
				}
			}
			if err != nil {
				if mset != nil {
					mset.delete()
				}
				js.mu.Lock()
				sa.err = err
				if n != nil {
					n.Delete()
				}
				result := &streamAssignmentResult{
					Account: sa.Client.serviceAccount(),
					Stream:  sa.Config.Name,
					Restore: &JSApiStreamRestoreResponse{ApiResponse: ApiResponse{Type: JSApiStreamRestoreResponseType}},
				}
				result.Restore.Error = NewJSStreamAssignmentError(err, Unless(err))
				js.mu.Unlock()
				// Send response to the metadata leader. They will forward to the user as needed.
				s.sendInternalMsgLocked(streamAssignmentSubj, _EMPTY_, nil, result)
				return
			}

			if !isLeader {
				panic("Finished restore but not leader")
			}
			// Trigger the stream followers to catchup.
			if n := mset.raftNode(); n != nil {
				n.SendSnapshot(mset.stateSnapshot())
			}
			js.processStreamLeaderChange(mset, isLeader)

			// Check to see if we have restored consumers here.
			// These are not currently assigned so we will need to do so here.
			if consumers := mset.getPublicConsumers(); len(consumers) > 0 {
				for _, o := range consumers {
					rg := cc.createGroupForConsumer(sa)
					// Pick a preferred leader.
					rg.setPreferred()
					name, cfg := o.String(), o.config()
					// Place our initial state here as well for assignment distribution.
					ca := &consumerAssignment{
						Group:   rg,
						Stream:  sa.Config.Name,
						Name:    name,
						Config:  &cfg,
						Client:  sa.Client,
						Created: o.createdTime(),
						State:   o.readStoreState(),
					}

					// We make these compressed in case state is complex.
					addEntry := encodeAddConsumerAssignmentCompressed(ca)
					cc.meta.ForwardProposal(addEntry)

					// Check to make sure we see the assignment.
					go func() {
						ticker := time.NewTicker(time.Second)
						defer ticker.Stop()
						for range ticker.C {
							js.mu.RLock()
							ca, meta := js.consumerAssignment(ca.Client.serviceAccount(), sa.Config.Name, name), cc.meta
							js.mu.RUnlock()
							if ca == nil {
								s.Warnf("Consumer assignment has not been assigned, retrying")
								if meta != nil {
									meta.ForwardProposal(addEntry)
								} else {
									return
								}
							} else {
								return
							}
						}
					}()
				}
			}
		}
	}
}

// resetClusteredState is called when a clustered stream had a sequence mismatch and needs to be reset.
func (mset *stream) resetClusteredState(err error) bool {
	mset.mu.RLock()
	s, js, jsa, sa, acc, node := mset.srv, mset.js, mset.jsa, mset.sa, mset.acc, mset.node
	stype, isLeader := mset.cfg.Storage, mset.isLeader()
	mset.mu.RUnlock()

	// Stepdown regardless if we are the leader here.
	if isLeader && node != nil {
		node.StepDown()
	}

	// Server
	if js.limitsExceeded(stype) {
		s.Debugf("Will not reset stream, server resources exceeded")
		return false
	}

	// Account
	if jsa.limitsExceeded(stype) {
		s.Warnf("stream '%s > %s' errored, account resources exceeded", acc, mset.name())
		return false
	}

	// We delete our raft state. Will recreate.
	if node != nil {
		node.Delete()
	}

	// Preserve our current state and messages unless we have a first sequence mismatch.
	shouldDelete := err == errFirstSequenceMismatch
	mset.stop(shouldDelete, false)

	if sa != nil {
		s.Warnf("Resetting stream cluster state for '%s > %s'", sa.Client.serviceAccount(), sa.Config.Name)
		js.mu.Lock()
		sa.Group.node = nil
		js.mu.Unlock()
		go js.restartClustered(acc, sa)
	}
	return true
}

// This will reset the stream and consumers.
// Should be done in separate go routine.
func (js *jetStream) restartClustered(acc *Account, sa *streamAssignment) {
	// Check and collect consumers first.
	js.mu.RLock()
	var consumers []*consumerAssignment
	if cc := js.cluster; cc != nil && cc.meta != nil {
		ourID := cc.meta.ID()
		for _, ca := range sa.consumers {
			if rg := ca.Group; rg != nil && rg.isMember(ourID) {
				rg.node = nil // Erase group raft/node state.
				consumers = append(consumers, ca)
			}
		}
	}
	js.mu.RUnlock()

	// Reset stream.
	js.processClusterCreateStream(acc, sa)
	// Reset consumers.
	for _, ca := range consumers {
		js.processClusterCreateConsumer(ca, nil, false)
	}
}

func isControlHdr(hdr []byte) bool {
	return bytes.HasPrefix(hdr, []byte("NATS/1.0 100 "))
}

// Apply our stream entries.
func (js *jetStream) applyStreamEntries(mset *stream, ce *CommittedEntry, isRecovering bool) error {
	for _, e := range ce.Entries {
		if e.Type == EntryNormal {
			buf := e.Data
			switch entryOp(buf[0]) {
			case streamMsgOp:
				if mset == nil {
					continue
				}
				s := js.srv

				subject, reply, hdr, msg, lseq, ts, err := decodeStreamMsg(buf[1:])
				if err != nil {
					if node := mset.raftNode(); node != nil {
						s.Errorf("JetStream cluster could not decode stream msg for '%s > %s' [%s]",
							mset.account(), mset.name(), node.Group())
					}
					panic(err.Error())
				}

				// Check for flowcontrol here.
				if len(msg) == 0 && len(hdr) > 0 && reply != _EMPTY_ && isControlHdr(hdr) {
					if !isRecovering {
						mset.sendFlowControlReply(reply)
					}
					continue
				}

				// Grab last sequence.
				last := mset.lastSeq()

				// We can skip if we know this is less than what we already have.
				if lseq < last {
					s.Debugf("Apply stream entries skipping message with sequence %d with last of %d", lseq, last)
					continue
				}

				// Skip by hand here since first msg special case.
				// Reason is sequence is unsigned and for lseq being 0
				// the lseq under stream would have to be -1.
				if lseq == 0 && last != 0 {
					continue
				}

				// Messages to be skipped have no subject or timestamp or msg or hdr.
				if subject == _EMPTY_ && ts == 0 && len(msg) == 0 && len(hdr) == 0 {
					// Skip and update our lseq.
					mset.setLastSeq(mset.store.SkipMsg())
					continue
				}

				// Process the actual message here.
				if err := mset.processJetStreamMsg(subject, reply, hdr, msg, lseq, ts); err != nil {
					// Only return in place if we are going to reset stream or we are out of space.
					if isClusterResetErr(err) || isOutOfSpaceErr(err) {
						return err
					}
					s.Debugf("Apply stream entries error processing message: %v", err)
				}
			case deleteMsgOp:
				md, err := decodeMsgDelete(buf[1:])
				if err != nil {
					if node := mset.raftNode(); node != nil {
						s := js.srv
						s.Errorf("JetStream cluster could not decode delete msg for '%s > %s' [%s]",
							mset.account(), mset.name(), node.Group())
					}
					panic(err.Error())
				}
				s, cc := js.server(), js.cluster

				var removed bool
				if md.NoErase {
					removed, err = mset.removeMsg(md.Seq)
				} else {
					removed, err = mset.eraseMsg(md.Seq)
				}

				// Cluster reset error.
				if err == ErrStoreEOF {
					return err
				}

				if err != nil && !isRecovering {
					s.Debugf("JetStream cluster failed to delete msg %d from stream %q for account %q: %v",
						md.Seq, md.Stream, md.Client.serviceAccount(), err)
				}

				js.mu.RLock()
				isLeader := cc.isStreamLeader(md.Client.serviceAccount(), md.Stream)
				js.mu.RUnlock()

				if isLeader && !isRecovering {
					var resp = JSApiMsgDeleteResponse{ApiResponse: ApiResponse{Type: JSApiMsgDeleteResponseType}}
					if err != nil {
						resp.Error = NewJSStreamMsgDeleteFailedError(err, Unless(err))
						s.sendAPIErrResponse(md.Client, mset.account(), md.Subject, md.Reply, _EMPTY_, s.jsonResponse(resp))
					} else if !removed {
						resp.Error = NewJSSequenceNotFoundError(md.Seq)
						s.sendAPIErrResponse(md.Client, mset.account(), md.Subject, md.Reply, _EMPTY_, s.jsonResponse(resp))
					} else {
						resp.Success = true
						s.sendAPIResponse(md.Client, mset.account(), md.Subject, md.Reply, _EMPTY_, s.jsonResponse(resp))
					}
				}
			case purgeStreamOp:
				sp, err := decodeStreamPurge(buf[1:])
				if err != nil {
					if node := mset.raftNode(); node != nil {
						s := js.srv
						s.Errorf("JetStream cluster could not decode purge msg for '%s > %s' [%s]",
							mset.account(), mset.name(), node.Group())
					}
					panic(err.Error())
				}
				// Ignore if we are recovering and we have already processed.
				if isRecovering {
					if mset.state().FirstSeq <= sp.LastSeq {
						// Make sure all messages from the purge are gone.
						mset.store.Compact(sp.LastSeq + 1)
					}
					continue
				}

				s := js.server()
				purged, err := mset.purge(sp.Request)
				if err != nil {
					s.Warnf("JetStream cluster failed to purge stream %q for account %q: %v", sp.Stream, sp.Client.serviceAccount(), err)
				}

				js.mu.RLock()
				isLeader := js.cluster.isStreamLeader(sp.Client.serviceAccount(), sp.Stream)
				js.mu.RUnlock()

				if isLeader && !isRecovering {
					var resp = JSApiStreamPurgeResponse{ApiResponse: ApiResponse{Type: JSApiStreamPurgeResponseType}}
					if err != nil {
						resp.Error = NewJSStreamGeneralError(err, Unless(err))
						s.sendAPIErrResponse(sp.Client, mset.account(), sp.Subject, sp.Reply, _EMPTY_, s.jsonResponse(resp))
					} else {
						resp.Purged = purged
						resp.Success = true
						s.sendAPIResponse(sp.Client, mset.account(), sp.Subject, sp.Reply, _EMPTY_, s.jsonResponse(resp))
					}
				}
			default:
				panic("JetStream Cluster Unknown group entry op type!")
			}
		} else if e.Type == EntrySnapshot {
			if !isRecovering && mset != nil {
				var snap streamSnapshot
				if err := json.Unmarshal(e.Data, &snap); err != nil {
					return err
				}
				if err := mset.processSnapshot(&snap); err != nil {
					return err
				}
			}
		} else if e.Type == EntryRemovePeer {
			js.mu.RLock()
			var ourID string
			if js.cluster != nil && js.cluster.meta != nil {
				ourID = js.cluster.meta.ID()
			}
			js.mu.RUnlock()
			// We only need to do processing if this is us.
			if peer := string(e.Data); peer == ourID {
				mset.stop(true, false)
			}
			return nil
		}
	}
	return nil
}

// Returns the PeerInfo for all replicas of a raft node. This is different than node.Peers()
// and is used for external facing advisories.
func (s *Server) replicas(node RaftNode) []*PeerInfo {
	now := time.Now()
	var replicas []*PeerInfo
	for _, rp := range node.Peers() {
		if sir, ok := s.nodeToInfo.Load(rp.ID); ok && sir != nil {
			si := sir.(nodeInfo)
			pi := &PeerInfo{Name: si.name, Current: rp.Current, Active: now.Sub(rp.Last), Offline: si.offline, Lag: rp.Lag}
			replicas = append(replicas, pi)
		}
	}
	return replicas
}

// Will check our node peers and see if we should remove a peer.
func (js *jetStream) checkPeers(rg *raftGroup) {
	js.mu.Lock()
	defer js.mu.Unlock()

	// FIXME(dlc) - Single replicas?
	if rg == nil || rg.node == nil {
		return
	}
	for _, peer := range rg.node.Peers() {
		if !rg.isMember(peer.ID) {
			rg.node.ProposeRemovePeer(peer.ID)
		}
	}
}

// Process a leader change for the clustered stream.
func (js *jetStream) processStreamLeaderChange(mset *stream, isLeader bool) {
	if mset == nil {
		return
	}
	sa := mset.streamAssignment()
	if sa == nil {
		return
	}
	js.mu.Lock()
	s, account, err := js.srv, sa.Client.serviceAccount(), sa.err
	client, subject, reply := sa.Client, sa.Subject, sa.Reply
	hasResponded := sa.responded
	sa.responded = true
	js.mu.Unlock()

	streamName := mset.name()

	if isLeader {
		s.Noticef("JetStream cluster new stream leader for '%s > %s'", sa.Client.serviceAccount(), streamName)
		s.sendStreamLeaderElectAdvisory(mset)
		// Check for peer removal and process here if needed.
		js.checkPeers(sa.Group)
	} else {
		// We are stepping down.
		// Make sure if we are doing so because we have lost quorum that we send the appropriate advisories.
		if node := mset.raftNode(); node != nil && !node.Quorum() && time.Since(node.Created()) > 5*time.Second {
			s.sendStreamLostQuorumAdvisory(mset)
		}
	}

	// Tell stream to switch leader status.
	mset.setLeader(isLeader)

	if !isLeader || hasResponded {
		return
	}

	acc, _ := s.LookupAccount(account)
	if acc == nil {
		return
	}

	// Send our response.
	var resp = JSApiStreamCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamCreateResponseType}}
	if err != nil {
		resp.Error = NewJSStreamCreateError(err, Unless(err))
		s.sendAPIErrResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
	} else {
		resp.StreamInfo = &StreamInfo{
			Created: mset.createdTime(),
			State:   mset.state(),
			Config:  mset.config(),
			Cluster: js.clusterInfo(mset.raftGroup()),
			Sources: mset.sourcesInfo(),
			Mirror:  mset.mirrorInfo(),
		}
		resp.DidCreate = true
		s.sendAPIResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
		if node := mset.raftNode(); node != nil {
			mset.sendCreateAdvisory()
		}
	}
}

// Fixed value ok for now.
const lostQuorumAdvInterval = 10 * time.Second

// Determines if we should send lost quorum advisory. We throttle these after first one.
func (mset *stream) shouldSendLostQuorum() bool {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	if time.Since(mset.lqsent) >= lostQuorumAdvInterval {
		mset.lqsent = time.Now()
		return true
	}
	return false
}

func (s *Server) sendStreamLostQuorumAdvisory(mset *stream) {
	if mset == nil {
		return
	}
	node, stream, acc := mset.raftNode(), mset.name(), mset.account()
	if node == nil {
		return
	}
	if !mset.shouldSendLostQuorum() {
		return
	}

	s.Warnf("JetStream cluster stream '%s > %s' has NO quorum, stalled", acc.GetName(), stream)

	subj := JSAdvisoryStreamQuorumLostPre + "." + stream
	adv := &JSStreamQuorumLostAdvisory{
		TypedEvent: TypedEvent{
			Type: JSStreamQuorumLostAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   stream,
		Replicas: s.replicas(node),
		Domain:   s.getOpts().JetStreamDomain,
	}

	// Send to the user's account if not the system account.
	if acc != s.SystemAccount() {
		s.publishAdvisory(acc, subj, adv)
	}
	// Now do system level one. Place account info in adv, and nil account means system.
	adv.Account = acc.GetName()
	s.publishAdvisory(nil, subj, adv)
}

func (s *Server) sendStreamLeaderElectAdvisory(mset *stream) {
	if mset == nil {
		return
	}
	node, stream, acc := mset.raftNode(), mset.name(), mset.account()
	if node == nil {
		return
	}
	subj := JSAdvisoryStreamLeaderElectedPre + "." + stream
	adv := &JSStreamLeaderElectedAdvisory{
		TypedEvent: TypedEvent{
			Type: JSStreamLeaderElectedAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   stream,
		Leader:   s.serverNameForNode(node.GroupLeader()),
		Replicas: s.replicas(node),
		Domain:   s.getOpts().JetStreamDomain,
	}

	// Send to the user's account if not the system account.
	if acc != s.SystemAccount() {
		s.publishAdvisory(acc, subj, adv)
	}
	// Now do system level one. Place account info in adv, and nil account means system.
	adv.Account = acc.GetName()
	s.publishAdvisory(nil, subj, adv)
}

// Will lookup a stream assignment.
// Lock should be held.
func (js *jetStream) streamAssignment(account, stream string) (sa *streamAssignment) {
	cc := js.cluster
	if cc == nil {
		return nil
	}

	if as := cc.streams[account]; as != nil {
		sa = as[stream]
	}
	return sa
}

// processStreamAssignment is called when followers have replicated an assignment.
func (js *jetStream) processStreamAssignment(sa *streamAssignment) bool {
	js.mu.RLock()
	s, cc := js.srv, js.cluster
	accName, stream := sa.Client.serviceAccount(), sa.Config.Name
	noMeta := cc == nil || cc.meta == nil
	var ourID string
	if !noMeta {
		ourID = cc.meta.ID()
	}
	var isMember bool
	if sa.Group != nil && ourID != _EMPTY_ {
		isMember = sa.Group.isMember(ourID)
	}
	js.mu.RUnlock()

	if s == nil || noMeta {
		return false
	}

	acc, err := s.LookupAccount(accName)
	if err != nil {
		ll := fmt.Sprintf("Account [%s] lookup for stream create failed: %v", accName, err)
		if isMember {
			// If we can not lookup the account and we are a member, send this result back to the metacontroller leader.
			result := &streamAssignmentResult{
				Account:  accName,
				Stream:   stream,
				Response: &JSApiStreamCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamCreateResponseType}},
			}
			result.Response.Error = NewJSNoAccountError()
			s.sendInternalMsgLocked(streamAssignmentSubj, _EMPTY_, nil, result)
			s.Warnf(ll)
		} else {
			s.Debugf(ll)
		}
		return false
	}

	js.mu.Lock()
	accStreams := cc.streams[acc.Name]
	if accStreams == nil {
		accStreams = make(map[string]*streamAssignment)
	} else if osa := accStreams[stream]; osa != nil && osa != sa {
		// Copy over private existing state from former SA.
		sa.Group.node = osa.Group.node
		sa.consumers = osa.consumers
		sa.responded = osa.responded
		sa.err = osa.err
	}

	// Update our state.
	accStreams[stream] = sa
	cc.streams[acc.Name] = accStreams
	js.mu.Unlock()

	var didRemove bool

	// Check if this is for us..
	if isMember {
		js.processClusterCreateStream(acc, sa)
	} else {
		// Check if we have a raft node running, meaning we are no longer part of the group but were.
		js.mu.Lock()
		if node := sa.Group.node; node != nil {
			if node.Leader() {
				node.StepDown()
			}
			node.ProposeRemovePeer(ourID)
			didRemove = true
		}
		sa.Group.node = nil
		sa.err = nil
		js.mu.Unlock()
	}

	// If this stream assignment does not have a sync subject (bug) set that the meta-leader should check when elected.
	if sa.Sync == _EMPTY_ {
		js.mu.Lock()
		cc.streamsCheck = true
		js.mu.Unlock()
		return false
	}

	return didRemove
}

// processUpdateStreamAssignment is called when followers have replicated an updated assignment.
func (js *jetStream) processUpdateStreamAssignment(sa *streamAssignment) {
	js.mu.RLock()
	s, cc := js.srv, js.cluster
	js.mu.RUnlock()
	if s == nil || cc == nil {
		// TODO(dlc) - debug at least
		return
	}

	acc, err := s.LookupAccount(sa.Client.serviceAccount())
	if err != nil {
		// TODO(dlc) - log error
		return
	}
	stream := sa.Config.Name

	js.mu.Lock()
	if cc.meta == nil {
		js.mu.Unlock()
		return
	}
	ourID := cc.meta.ID()

	var isMember bool
	if sa.Group != nil {
		isMember = sa.Group.isMember(ourID)
	}

	accStreams := cc.streams[acc.Name]
	if accStreams == nil {
		js.mu.Unlock()
		return
	}
	osa := accStreams[stream]
	if osa == nil {
		js.mu.Unlock()
		return
	}

	// Copy over private existing state from former SA.
	sa.Group.node = osa.Group.node
	sa.consumers = osa.consumers
	sa.err = osa.err

	// Update our state.
	accStreams[stream] = sa
	cc.streams[acc.Name] = accStreams

	// Make sure we respond.
	if isMember {
		sa.responded = false
	}

	js.mu.Unlock()

	// Check if this is for us..
	if isMember {
		js.processClusterUpdateStream(acc, osa, sa)
	} else if mset, _ := acc.lookupStream(sa.Config.Name); mset != nil {
		// We have one here even though we are not a member. This can happen on re-assignment.
		s.Debugf("JetStream removing stream '%s > %s' from this server, re-assigned", sa.Client.serviceAccount(), sa.Config.Name)
		if node := mset.raftNode(); node != nil {
			node.ProposeRemovePeer(ourID)
		}
		mset.stop(true, false)
	}
}

// processClusterUpdateStream is called when we have a stream assignment that
// has been updated for an existing assignment and we are a member.
func (js *jetStream) processClusterUpdateStream(acc *Account, osa, sa *streamAssignment) {
	if sa == nil {
		return
	}

	js.mu.Lock()
	s, rg := js.srv, sa.Group
	client, subject, reply := sa.Client, sa.Subject, sa.Reply
	alreadyRunning, numReplicas := osa.Group.node != nil, sa.Config.Replicas
	needsNode := rg.node == nil
	storage := sa.Config.Storage
	hasResponded := sa.responded
	sa.responded = true
	js.mu.Unlock()

	mset, err := acc.lookupStream(sa.Config.Name)
	if err == nil && mset != nil {
		var needsSetLeader bool
		if !alreadyRunning && numReplicas > 1 {
			if needsNode {
				js.createRaftGroup(rg, storage)
			}
			s.startGoRoutine(func() { js.monitorStream(mset, sa) })
		} else if numReplicas == 1 && alreadyRunning {
			// We downgraded to R1. Make sure we cleanup the raft node and the stream monitor.
			mset.removeNode()
			// Make sure we are leader now that we are R1.
			needsSetLeader = true
			// In case we nned to shutdown the cluster specific subs, etc.
			mset.setLeader(false)
			js.mu.Lock()
			sa.Group.node = nil
			js.mu.Unlock()
		}
		mset.setStreamAssignment(sa)
		if err = mset.update(sa.Config); err != nil {
			s.Warnf("JetStream cluster error updating stream %q for account %q: %v", sa.Config.Name, acc.Name, err)
			mset.setStreamAssignment(osa)
		}
		// Make sure we are the leader now that we are R1.
		if needsSetLeader {
			mset.setLeader(true)
		}
	}

	// If not found we must be expanding into this node since if we are here we know we are a member.
	if err == ErrJetStreamStreamNotFound {
		js.processStreamAssignment(sa)
		return
	}

	if err != nil {
		js.mu.Lock()
		sa.err = err
		result := &streamAssignmentResult{
			Account:  sa.Client.serviceAccount(),
			Stream:   sa.Config.Name,
			Response: &JSApiStreamCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamCreateResponseType}},
			Update:   true,
		}
		result.Response.Error = NewJSStreamGeneralError(err, Unless(err))
		js.mu.Unlock()

		// Send response to the metadata leader. They will forward to the user as needed.
		s.sendInternalMsgLocked(streamAssignmentSubj, _EMPTY_, nil, result)
		return
	}

	mset.mu.RLock()
	isLeader := mset.isLeader()
	mset.mu.RUnlock()

	// Check for missing syncSubject bug.
	if isLeader && osa != nil && osa.Sync == _EMPTY_ {
		if node := mset.raftNode(); node != nil {
			node.StepDown()
		}
		return
	}

	// If we were a single node being promoted assume leadership role for purpose of responding.
	if !hasResponded && !isLeader && !alreadyRunning {
		isLeader = true
	}

	// Check if we should bail.
	if !isLeader || hasResponded {
		return
	}

	// Send our response.
	var resp = JSApiStreamUpdateResponse{ApiResponse: ApiResponse{Type: JSApiStreamUpdateResponseType}}
	resp.StreamInfo = &StreamInfo{
		Created: mset.createdTime(),
		State:   mset.state(),
		Config:  mset.config(),
		Cluster: js.clusterInfo(mset.raftGroup()),
		Mirror:  mset.mirrorInfo(),
		Sources: mset.sourcesInfo(),
	}
	s.sendAPIResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
}

// processClusterCreateStream is called when we have a stream assignment that
// has been committed and this server is a member of the peer group.
func (js *jetStream) processClusterCreateStream(acc *Account, sa *streamAssignment) {
	if sa == nil {
		return
	}

	js.mu.RLock()
	s, rg := js.srv, sa.Group
	alreadyRunning := rg.node != nil
	storage := sa.Config.Storage
	js.mu.RUnlock()

	// Process the raft group and make sure it's running if needed.
	err := js.createRaftGroup(rg, storage)

	// If we are restoring, create the stream if we are R>1 and not the preferred who handles the
	// receipt of the snapshot itself.
	shouldCreate := true
	if sa.Restore != nil {
		if len(rg.Peers) == 1 || rg.node != nil && rg.node.ID() == rg.Preferred {
			shouldCreate = false
		} else {
			sa.Restore = nil
		}
	}

	// Our stream.
	var mset *stream

	// Process here if not restoring or not the leader.
	if shouldCreate && err == nil {
		// Go ahead and create or update the stream.
		mset, err = acc.lookupStream(sa.Config.Name)
		if err == nil && mset != nil {
			osa := mset.streamAssignment()
			mset.setStreamAssignment(sa)
			if err = mset.update(sa.Config); err != nil {
				s.Warnf("JetStream cluster error updating stream %q for account %q: %v", sa.Config.Name, acc.Name, err)
				mset.setStreamAssignment(osa)
			}
		} else if err == NewJSStreamNotFoundError() {
			// Add in the stream here.
			mset, err = acc.addStreamWithAssignment(sa.Config, nil, sa)
		}
		if mset != nil {
			mset.setCreatedTime(sa.Created)
		}
	}

	// This is an error condition.
	if err != nil {
		s.Warnf("Stream create failed for '%s > %s': %v", sa.Client.serviceAccount(), sa.Config.Name, err)
		js.mu.Lock()

		sa.err = err
		hasResponded := sa.responded

		// If out of space do nothing for now.
		if isOutOfSpaceErr(err) {
			hasResponded = true
		}

		if rg.node != nil {
			rg.node.Delete()
		}

		var result *streamAssignmentResult
		if !hasResponded {
			result = &streamAssignmentResult{
				Account:  sa.Client.serviceAccount(),
				Stream:   sa.Config.Name,
				Response: &JSApiStreamCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamCreateResponseType}},
			}
			result.Response.Error = NewJSStreamCreateError(err, Unless(err))
		}
		js.mu.Unlock()

		// Send response to the metadata leader. They will forward to the user as needed.
		if result != nil {
			s.sendInternalMsgLocked(streamAssignmentSubj, _EMPTY_, nil, result)
		}
		return
	}

	// Start our monitoring routine.
	if rg.node != nil {
		if !alreadyRunning {
			s.startGoRoutine(func() { js.monitorStream(mset, sa) })
		}
	} else {
		// Single replica stream, process manually here.
		// If we are restoring, process that first.
		if sa.Restore != nil {
			// We are restoring a stream here.
			restoreDoneCh := s.processStreamRestore(sa.Client, acc, sa.Config, _EMPTY_, sa.Reply, _EMPTY_)
			s.startGoRoutine(func() {
				defer s.grWG.Done()
				select {
				case err := <-restoreDoneCh:
					if err == nil {
						mset, err = acc.lookupStream(sa.Config.Name)
						if mset != nil {
							mset.setStreamAssignment(sa)
							mset.setCreatedTime(sa.Created)
						}
					}
					if err != nil {
						if mset != nil {
							mset.delete()
						}
						js.mu.Lock()
						sa.err = err
						result := &streamAssignmentResult{
							Account: sa.Client.serviceAccount(),
							Stream:  sa.Config.Name,
							Restore: &JSApiStreamRestoreResponse{ApiResponse: ApiResponse{Type: JSApiStreamRestoreResponseType}},
						}
						result.Restore.Error = NewJSStreamRestoreError(err, Unless(err))
						js.mu.Unlock()
						// Send response to the metadata leader. They will forward to the user as needed.
						b, _ := json.Marshal(result) // Avoids auto-processing and doing fancy json with newlines.
						s.sendInternalMsgLocked(streamAssignmentSubj, _EMPTY_, nil, b)
						return
					}
					js.processStreamLeaderChange(mset, true)

					// Check to see if we have restored consumers here.
					// These are not currently assigned so we will need to do so here.
					if consumers := mset.getPublicConsumers(); len(consumers) > 0 {
						js.mu.RLock()
						cc := js.cluster
						js.mu.RUnlock()

						for _, o := range consumers {
							rg := cc.createGroupForConsumer(sa)
							name, cfg := o.String(), o.config()
							// Place our initial state here as well for assignment distribution.
							ca := &consumerAssignment{
								Group:   rg,
								Stream:  sa.Config.Name,
								Name:    name,
								Config:  &cfg,
								Client:  sa.Client,
								Created: o.createdTime(),
							}

							addEntry := encodeAddConsumerAssignment(ca)
							cc.meta.ForwardProposal(addEntry)

							// Check to make sure we see the assignment.
							go func() {
								ticker := time.NewTicker(time.Second)
								defer ticker.Stop()
								for range ticker.C {
									js.mu.RLock()
									ca, meta := js.consumerAssignment(ca.Client.serviceAccount(), sa.Config.Name, name), cc.meta
									js.mu.RUnlock()
									if ca == nil {
										s.Warnf("Consumer assignment has not been assigned, retrying")
										if meta != nil {
											meta.ForwardProposal(addEntry)
										} else {
											return
										}
									} else {
										return
									}
								}
							}()
						}
					}
				case <-s.quitCh:
					return
				}
			})
		} else {
			js.processStreamLeaderChange(mset, true)
		}
	}
}

// processStreamRemoval is called when followers have replicated an assignment.
func (js *jetStream) processStreamRemoval(sa *streamAssignment) {
	js.mu.Lock()
	s, cc := js.srv, js.cluster
	if s == nil || cc == nil || cc.meta == nil {
		// TODO(dlc) - debug at least
		js.mu.Unlock()
		return
	}
	stream := sa.Config.Name
	isMember := sa.Group.isMember(cc.meta.ID())
	wasLeader := cc.isStreamLeader(sa.Client.serviceAccount(), stream)

	// Check if we already have this assigned.
	accStreams := cc.streams[sa.Client.serviceAccount()]
	needDelete := accStreams != nil && accStreams[stream] != nil
	if needDelete {
		delete(accStreams, stream)
		if len(accStreams) == 0 {
			delete(cc.streams, sa.Client.serviceAccount())
		}
	}
	js.mu.Unlock()

	if needDelete {
		js.processClusterDeleteStream(sa, isMember, wasLeader)
	}
}

func (js *jetStream) processClusterDeleteStream(sa *streamAssignment, isMember, wasLeader bool) {
	if sa == nil {
		return
	}
	js.mu.RLock()
	s := js.srv
	hadLeader := sa.Group.node == nil || sa.Group.node.GroupLeader() != noLeader
	js.mu.RUnlock()

	acc, err := s.LookupAccount(sa.Client.serviceAccount())
	if err != nil {
		s.Debugf("JetStream cluster failed to lookup account %q: %v", sa.Client.serviceAccount(), err)
		return
	}

	var resp = JSApiStreamDeleteResponse{ApiResponse: ApiResponse{Type: JSApiStreamDeleteResponseType}}

	// Go ahead and delete the stream.
	mset, err := acc.lookupStream(sa.Config.Name)
	if err != nil {
		resp.Error = NewJSStreamNotFoundError(Unless(err))
	} else if mset != nil {
		err = mset.stop(true, wasLeader)
	}

	if sa.Group.node != nil {
		sa.Group.node.Delete()
	}

	if !isMember || !wasLeader && hadLeader {
		return
	}

	if err != nil {
		if resp.Error == nil {
			resp.Error = NewJSStreamGeneralError(err, Unless(err))
		}
		s.sendAPIErrResponse(sa.Client, acc, sa.Subject, sa.Reply, _EMPTY_, s.jsonResponse(resp))
	} else {
		resp.Success = true
		s.sendAPIResponse(sa.Client, acc, sa.Subject, sa.Reply, _EMPTY_, s.jsonResponse(resp))
	}
}

// processConsumerAssignment is called when followers have replicated an assignment for a consumer.
func (js *jetStream) processConsumerAssignment(ca *consumerAssignment) {
	js.mu.RLock()
	s, cc := js.srv, js.cluster
	accName, stream, consumer := ca.Client.serviceAccount(), ca.Stream, ca.Name
	noMeta := cc == nil || cc.meta == nil
	var ourID string
	if !noMeta {
		ourID = cc.meta.ID()
	}
	var isMember bool
	if ca.Group != nil && ourID != _EMPTY_ {
		isMember = ca.Group.isMember(ourID)
	}
	js.mu.RUnlock()

	if s == nil || noMeta {
		return
	}

	if _, err := s.LookupAccount(accName); err != nil {
		ll := fmt.Sprintf("Account [%s] lookup for consumer create failed: %v", accName, err)
		if isMember {
			// If we can not lookup the account and we are a member, send this result back to the metacontroller leader.
			result := &consumerAssignmentResult{
				Account:  accName,
				Stream:   stream,
				Consumer: consumer,
				Response: &JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}},
			}
			result.Response.Error = NewJSNoAccountError()
			s.sendInternalMsgLocked(consumerAssignmentSubj, _EMPTY_, nil, result)
			s.Warnf(ll)
		} else {
			s.Debugf(ll)
		}
		return
	}

	sa := js.streamAssignment(accName, stream)
	if sa == nil {
		s.Debugf("Consumer create failed, could not locate stream '%s > %s'", accName, stream)
		return
	}

	// Track if this existed already.
	var wasExisting bool

	// Check if we have an existing consumer assignment.
	js.mu.Lock()
	if sa.consumers == nil {
		sa.consumers = make(map[string]*consumerAssignment)
	} else if oca := sa.consumers[ca.Name]; oca != nil {
		wasExisting = true
		// Copy over private existing state from former SA.
		ca.Group.node = oca.Group.node
		ca.responded = oca.responded
		ca.err = oca.err
	}

	// Capture the optional state. We will pass it along if we are a member to apply.
	// This is only applicable when restoring a stream with consumers.
	state := ca.State
	ca.State = nil

	// Place into our internal map under the stream assignment.
	// Ok to replace an existing one, we check on process call below.
	sa.consumers[ca.Name] = ca
	js.mu.Unlock()

	// Check if this is for us..
	if isMember {
		js.processClusterCreateConsumer(ca, state, wasExisting)
	} else {
		// Check if we have a raft node running, meaning we are no longer part of the group but were.
		js.mu.Lock()
		if node := ca.Group.node; node != nil {
			if node.Leader() {
				node.StepDown()
			}
			node.ProposeRemovePeer(ourID)
		}
		ca.Group.node = nil
		ca.err = nil
		js.mu.Unlock()
	}
}

func (js *jetStream) processConsumerRemoval(ca *consumerAssignment) {
	js.mu.Lock()
	s, cc := js.srv, js.cluster
	if s == nil || cc == nil || cc.meta == nil {
		// TODO(dlc) - debug at least
		js.mu.Unlock()
		return
	}
	isMember := ca.Group.isMember(cc.meta.ID())
	wasLeader := cc.isConsumerLeader(ca.Client.serviceAccount(), ca.Stream, ca.Name)

	// Delete from our state.
	var needDelete bool
	if accStreams := cc.streams[ca.Client.serviceAccount()]; accStreams != nil {
		if sa := accStreams[ca.Stream]; sa != nil && sa.consumers != nil && sa.consumers[ca.Name] != nil {
			needDelete = true
			delete(sa.consumers, ca.Name)
		}
	}
	js.mu.Unlock()

	if needDelete {
		js.processClusterDeleteConsumer(ca, isMember, wasLeader)
	}
}

type consumerAssignmentResult struct {
	Account  string                       `json:"account"`
	Stream   string                       `json:"stream"`
	Consumer string                       `json:"consumer"`
	Response *JSApiConsumerCreateResponse `json:"response,omitempty"`
}

// processClusterCreateConsumer is when we are a member of the group and need to create the consumer.
func (js *jetStream) processClusterCreateConsumer(ca *consumerAssignment, state *ConsumerState, wasExisting bool) {
	if ca == nil {
		return
	}
	js.mu.RLock()
	s := js.srv
	acc, err := s.LookupAccount(ca.Client.serviceAccount())
	if err != nil {
		s.Warnf("JetStream cluster failed to lookup account %q: %v", ca.Client.serviceAccount(), err)
		js.mu.RUnlock()
		return
	}
	rg := ca.Group
	alreadyRunning := rg.node != nil
	js.mu.RUnlock()

	// Go ahead and create or update the consumer.
	mset, err := acc.lookupStream(ca.Stream)
	if err != nil {
		js.mu.Lock()
		s.Debugf("Consumer create failed, could not locate stream '%s > %s'", ca.Client.serviceAccount(), ca.Stream)
		ca.err = NewJSStreamNotFoundError()
		result := &consumerAssignmentResult{
			Account:  ca.Client.serviceAccount(),
			Stream:   ca.Stream,
			Consumer: ca.Name,
			Response: &JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}},
		}
		result.Response.Error = NewJSStreamNotFoundError()
		s.sendInternalMsgLocked(consumerAssignmentSubj, _EMPTY_, nil, result)
		js.mu.Unlock()
		return
	}

	if !alreadyRunning {
		// Process the raft group and make sure its running if needed.
		js.createRaftGroup(rg, mset.config().Storage)
	}

	// Check if we already have this consumer running.
	var didCreate bool
	o := mset.lookupConsumer(ca.Name)
	if o == nil {
		// Add in the consumer if needed.
		o, err = mset.addConsumerWithAssignment(ca.Config, ca.Name, ca)
		didCreate = true
	} else {
		if err := o.updateConfig(ca.Config); err != nil {
			// This is essentially an update that has failed.
			js.mu.Lock()
			result := &consumerAssignmentResult{
				Account:  ca.Client.serviceAccount(),
				Stream:   ca.Stream,
				Consumer: ca.Name,
				Response: &JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}},
			}
			result.Response.Error = NewJSConsumerNameExistError()
			s.sendInternalMsgLocked(consumerAssignmentSubj, _EMPTY_, nil, result)
			js.mu.Unlock()
			return
		}
		// Check if we already had a consumer assignment and its still pending.
		cca, oca := ca, o.consumerAssignment()
		js.mu.Lock()
		if oca != nil && !oca.responded {
			// We can't over ride info for replying here otherwise leader once elected can not respond.
			// So just update Config, leave off client and reply to the originals.
			cac := *oca
			cac.Config = ca.Config
			cca = &cac
		}
		js.mu.Unlock()
		// Set CA for our consumer.
		o.setConsumerAssignment(cca)
		s.Debugf("JetStream cluster, consumer was already running")
	}

	// If we have an initial state set apply that now.
	if state != nil && o != nil {
		err = o.setStoreState(state)
	}

	if err != nil {
		if IsNatsErr(err, JSConsumerStoreFailedErrF) {
			s.Warnf("Consumer create failed for '%s > %s > %s': %v", ca.Client.serviceAccount(), ca.Stream, ca.Name, err)
		}

		js.mu.Lock()

		ca.err = err
		hasResponded := ca.responded

		// If out of space do nothing for now.
		if isOutOfSpaceErr(err) {
			hasResponded = true
		}

		if rg.node != nil {
			rg.node.Delete()
		}

		var result *consumerAssignmentResult
		if !hasResponded {
			result = &consumerAssignmentResult{
				Account:  ca.Client.serviceAccount(),
				Stream:   ca.Stream,
				Consumer: ca.Name,
				Response: &JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}},
			}
			result.Response.Error = NewJSConsumerCreateError(err, Unless(err))
		} else if err == errNoInterest {
			// This is a stranded ephemeral, let's clean this one up.
			subject := fmt.Sprintf(JSApiConsumerDeleteT, ca.Stream, ca.Name)
			mset.outq.send(newJSPubMsg(subject, _EMPTY_, _EMPTY_, nil, nil, nil, 0))
		}
		js.mu.Unlock()

		if result != nil {
			// Send response to the metadata leader. They will forward to the user as needed.
			b, _ := json.Marshal(result) // Avoids auto-processing and doing fancy json with newlines.
			s.sendInternalMsgLocked(consumerAssignmentSubj, _EMPTY_, nil, b)
		}
	} else {
		if didCreate {
			o.setCreatedTime(ca.Created)
		} else {
			// Check for scale down to 1..
			if rg.node != nil && len(rg.Peers) == 1 {
				o.clearNode()
				o.setLeader(true)
				// Need to clear from rg too.
				js.mu.Lock()
				rg.node = nil
				js.mu.Unlock()
				return
			}
		}

		// Start our monitoring routine.
		if rg.node == nil {
			// Single replica consumer, process manually here.
			js.processConsumerLeaderChange(o, true)
		} else {
			if !alreadyRunning {
				s.startGoRoutine(func() { js.monitorConsumer(o, ca) })
			}
			// Process if existing.
			if wasExisting && (o.isLeader() || (!didCreate && rg.node.GroupLeader() == _EMPTY_)) {
				// This is essentially an update, so make sure to respond if needed.
				js.mu.RLock()
				client, subject, reply := ca.Client, ca.Subject, ca.Reply
				js.mu.RUnlock()
				var resp = JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}}
				resp.ConsumerInfo = o.info()
				s.sendAPIResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
			}
		}
	}
}

func (js *jetStream) processClusterDeleteConsumer(ca *consumerAssignment, isMember, wasLeader bool) {
	if ca == nil {
		return
	}
	js.mu.RLock()
	s := js.srv
	js.mu.RUnlock()

	acc, err := s.LookupAccount(ca.Client.serviceAccount())
	if err != nil {
		s.Warnf("JetStream cluster failed to lookup account %q: %v", ca.Client.serviceAccount(), err)
		return
	}

	var resp = JSApiConsumerDeleteResponse{ApiResponse: ApiResponse{Type: JSApiConsumerDeleteResponseType}}

	// Go ahead and delete the consumer.
	mset, err := acc.lookupStream(ca.Stream)
	if err != nil {
		resp.Error = NewJSStreamNotFoundError(Unless(err))
	} else if mset != nil {
		if o := mset.lookupConsumer(ca.Name); o != nil {
			err = o.stopWithFlags(true, false, true, wasLeader)
		} else {
			resp.Error = NewJSConsumerNotFoundError()
		}
	}

	if ca.Group.node != nil {
		ca.Group.node.Delete()
	}

	if !wasLeader || ca.Reply == _EMPTY_ {
		return
	}

	if err != nil {
		if resp.Error == nil {
			resp.Error = NewJSStreamNotFoundError(Unless(err))
		}
		s.sendAPIErrResponse(ca.Client, acc, ca.Subject, ca.Reply, _EMPTY_, s.jsonResponse(resp))
	} else {
		resp.Success = true
		s.sendAPIResponse(ca.Client, acc, ca.Subject, ca.Reply, _EMPTY_, s.jsonResponse(resp))
	}
}

// Returns the consumer assignment, or nil if not present.
// Lock should be held.
func (js *jetStream) consumerAssignment(account, stream, consumer string) *consumerAssignment {
	if sa := js.streamAssignment(account, stream); sa != nil {
		return sa.consumers[consumer]
	}
	return nil
}

// consumerAssigned informs us if this server has this consumer assigned.
func (jsa *jsAccount) consumerAssigned(stream, consumer string) bool {
	jsa.mu.RLock()
	js, acc := jsa.js, jsa.account
	jsa.mu.RUnlock()

	if js == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.cluster.isConsumerAssigned(acc, stream, consumer)
}

// Read lock should be held.
func (cc *jetStreamCluster) isConsumerAssigned(a *Account, stream, consumer string) bool {
	// Non-clustered mode always return true.
	if cc == nil {
		return true
	}
	var sa *streamAssignment
	accStreams := cc.streams[a.Name]
	if accStreams != nil {
		sa = accStreams[stream]
	}
	if sa == nil {
		// TODO(dlc) - This should not happen.
		return false
	}
	ca := sa.consumers[consumer]
	if ca == nil {
		return false
	}
	rg := ca.Group
	// Check if we are the leader of this raftGroup assigned to the stream.
	ourID := cc.meta.ID()
	for _, peer := range rg.Peers {
		if peer == ourID {
			return true
		}
	}
	return false
}

// Returns our stream and underlying raft node.
func (o *consumer) streamAndNode() (*stream, RaftNode) {
	if o == nil {
		return nil, nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.mset, o.node
}

func (o *consumer) raftGroup() *raftGroup {
	if o == nil {
		return nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.ca == nil {
		return nil
	}
	return o.ca.Group
}

func (o *consumer) raftNode() RaftNode {
	if o == nil {
		return nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.node
}

func (js *jetStream) monitorConsumer(o *consumer, ca *consumerAssignment) {
	s, n := js.server(), o.raftNode()
	defer s.grWG.Done()

	if n == nil {
		s.Warnf("No RAFT group for consumer")
		return
	}

	qch, lch, aq := n.QuitC(), n.LeadChangeC(), n.ApplyQ()

	s.Debugf("Starting consumer monitor for '%s > %s > %s' [%s]", o.acc.Name, ca.Stream, ca.Name, n.Group())
	defer s.Debugf("Exiting consumer monitor for '%s > %s > %s' [%s]", o.acc.Name, ca.Stream, ca.Name, n.Group())

	const (
		compactInterval = 2 * time.Minute
		compactSizeMin  = 8 * 1024 * 1024
		compactNumMin   = 8192
	)

	t := time.NewTicker(compactInterval)
	defer t.Stop()

	st := o.store.Type()
	var lastSnap []byte

	doSnapshot := func() {
		// Memory store consumers do not keep state in the store itself.
		// Just compact to our applied index.
		if st == MemoryStorage {
			_, _, applied := n.Progress()
			n.Compact(applied)
		} else if state, err := o.store.State(); err == nil && state != nil {
			// FileStore version.
			if snap := encodeConsumerState(state); !bytes.Equal(lastSnap, snap) {
				if err := n.InstallSnapshot(snap); err == nil {
					lastSnap = snap
				}
			}
		}
	}

	// Track if we are leader.
	var isLeader bool
	recovering := true

	for {
		select {
		case <-s.quitCh:
			return
		case <-qch:
			return
		case <-aq.ch:
			ces := aq.pop()
			for _, cei := range ces {
				// No special processing needed for when we are caught up on restart.
				if cei == nil {
					recovering = false
					if n.NeedSnapshot() {
						doSnapshot()
					}
					continue
				}
				ce := cei.(*CommittedEntry)
				if err := js.applyConsumerEntries(o, ce, isLeader); err == nil {
					ne, nb := n.Applied(ce.Index)
					// If we have at least min entries to compact, go ahead and snapshot/compact.
					if nb > 0 && ne >= compactNumMin || nb > compactSizeMin {
						doSnapshot()
					}
				} else {
					s.Warnf("Error applying consumer entries to '%s > %s'", ca.Client.serviceAccount(), ca.Name)
				}
			}
			aq.recycle(&ces)
		case isLeader = <-lch:
			if recovering && !isLeader {
				js.setConsumerAssignmentRecovering(ca)
			}
			js.processConsumerLeaderChange(o, isLeader)
		case <-t.C:
			doSnapshot()
		}
	}
}

func (js *jetStream) applyConsumerEntries(o *consumer, ce *CommittedEntry, isLeader bool) error {
	for _, e := range ce.Entries {
		if e.Type == EntrySnapshot {
			// No-op needed?
			state, err := decodeConsumerState(e.Data)
			if err != nil {
				if mset, node := o.streamAndNode(); mset != nil && node != nil {
					s := js.srv
					s.Errorf("JetStream cluster could not decode consumer snapshot for '%s > %s > %s' [%s]",
						mset.account(), mset.name(), o, node.Group())
				}
				panic(err.Error())
			}
			o.store.Update(state)
		} else if e.Type == EntryRemovePeer {
			js.mu.RLock()
			var ourID string
			if js.cluster != nil && js.cluster.meta != nil {
				ourID = js.cluster.meta.ID()
			}
			js.mu.RUnlock()
			if peer := string(e.Data); peer == ourID {
				o.stopWithFlags(true, false, false, false)
			}
			return nil
		} else if e.Type == EntryAddPeer {
			// Ignore for now.
		} else {
			buf := e.Data
			switch entryOp(buf[0]) {
			case updateDeliveredOp:
				// These are handled in place in leaders.
				if !isLeader {
					dseq, sseq, dc, ts, err := decodeDeliveredUpdate(buf[1:])
					if err != nil {
						if mset, node := o.streamAndNode(); mset != nil && node != nil {
							s := js.srv
							s.Errorf("JetStream cluster could not decode consumer delivered update for '%s > %s > %s' [%s]",
								mset.account(), mset.name(), o, node.Group())
						}
						panic(err.Error())
					}
					if err := o.store.UpdateDelivered(dseq, sseq, dc, ts); err != nil {
						panic(err.Error())
					}
					// Update activity.
					o.mu.Lock()
					o.ldt = time.Now()
					o.mu.Unlock()
				}
			case updateAcksOp:
				dseq, sseq, err := decodeAckUpdate(buf[1:])
				if err != nil {
					if mset, node := o.streamAndNode(); mset != nil && node != nil {
						s := js.srv
						s.Errorf("JetStream cluster could not decode consumer ack update for '%s > %s > %s' [%s]",
							mset.account(), mset.name(), o, node.Group())
					}
					panic(err.Error())
				}
				o.processReplicatedAck(dseq, sseq)
			case updateSkipOp:
				o.mu.Lock()
				if !o.isLeader() {
					var le = binary.LittleEndian
					o.sseq = le.Uint64(buf[1:])
				}
				o.mu.Unlock()
			case addPendingRequest:
				if !o.isLeader() {
					o.mu.Lock()
					if o.prm == nil {
						o.prm = make(map[string]struct{})
					}
					o.prm[string(buf[1:])] = struct{}{}
					o.mu.Unlock()
				}
			case removePendingRequest:
				if !o.isLeader() {
					o.mu.Lock()
					if o.prm != nil {
						delete(o.prm, string(buf[1:]))
					}
					o.mu.Unlock()
				}
			default:
				panic(fmt.Sprintf("JetStream Cluster Unknown group entry op type! %v", entryOp(buf[0])))
			}
		}
	}
	return nil
}

func (o *consumer) processReplicatedAck(dseq, sseq uint64) {
	o.mu.Lock()
	// Update activity.
	o.lat = time.Now()
	// Do actual ack update to store.
	o.store.UpdateAcks(dseq, sseq)

	mset := o.mset
	if mset == nil || mset.cfg.Retention == LimitsPolicy {
		o.mu.Unlock()
		return
	}

	var sagap uint64
	if o.cfg.AckPolicy == AckAll {
		if o.isLeader() {
			sagap = sseq - o.asflr
		} else {
			// We are a follower so only have the store state, so read that in.
			state, err := o.store.State()
			if err != nil {
				o.mu.Unlock()
				return
			}
			sagap = sseq - state.AckFloor.Stream
		}
	}
	o.mu.Unlock()

	if sagap > 1 {
		// FIXME(dlc) - This is very inefficient, will need to fix.
		for seq := sseq; seq > sseq-sagap; seq-- {
			mset.ackMsg(o, seq)
		}
	} else {
		mset.ackMsg(o, sseq)
	}
}

var errBadAckUpdate = errors.New("jetstream cluster bad replicated ack update")
var errBadDeliveredUpdate = errors.New("jetstream cluster bad replicated delivered update")

func decodeAckUpdate(buf []byte) (dseq, sseq uint64, err error) {
	var bi, n int
	if dseq, n = binary.Uvarint(buf); n < 0 {
		return 0, 0, errBadAckUpdate
	}
	bi += n
	if sseq, n = binary.Uvarint(buf[bi:]); n < 0 {
		return 0, 0, errBadAckUpdate
	}
	return dseq, sseq, nil
}

func decodeDeliveredUpdate(buf []byte) (dseq, sseq, dc uint64, ts int64, err error) {
	var bi, n int
	if dseq, n = binary.Uvarint(buf); n < 0 {
		return 0, 0, 0, 0, errBadDeliveredUpdate
	}
	bi += n
	if sseq, n = binary.Uvarint(buf[bi:]); n < 0 {
		return 0, 0, 0, 0, errBadDeliveredUpdate
	}
	bi += n
	if dc, n = binary.Uvarint(buf[bi:]); n < 0 {
		return 0, 0, 0, 0, errBadDeliveredUpdate
	}
	bi += n
	if ts, n = binary.Varint(buf[bi:]); n < 0 {
		return 0, 0, 0, 0, errBadDeliveredUpdate
	}
	return dseq, sseq, dc, ts, nil
}

func (js *jetStream) processConsumerLeaderChange(o *consumer, isLeader bool) {
	ca := o.consumerAssignment()
	if ca == nil {
		return
	}
	js.mu.Lock()
	s, account, err := js.srv, ca.Client.serviceAccount(), ca.err
	client, subject, reply := ca.Client, ca.Subject, ca.Reply
	hasResponded := ca.responded
	ca.responded = true
	js.mu.Unlock()

	streamName := o.streamName()
	consumerName := o.String()
	acc, _ := s.LookupAccount(account)
	if acc == nil {
		return
	}

	if isLeader {
		s.Noticef("JetStream cluster new consumer leader for '%s > %s > %s'", ca.Client.serviceAccount(), streamName, consumerName)
		s.sendConsumerLeaderElectAdvisory(o)
		// Check for peer removal and process here if needed.
		js.checkPeers(ca.Group)
	} else {
		// We are stepping down.
		// Make sure if we are doing so because we have lost quorum that we send the appropriate advisories.
		if node := o.raftNode(); node != nil && !node.Quorum() && time.Since(node.Created()) > 5*time.Second {
			s.sendConsumerLostQuorumAdvisory(o)
		}
	}

	// Tell consumer to switch leader status.
	o.setLeader(isLeader)

	// Synchronize others to our version of state.
	if isLeader {
		if n := o.raftNode(); n != nil {
			if state, err := o.store.State(); err == nil && state != nil {
				if snap := encodeConsumerState(state); len(snap) > 0 {
					n.SendSnapshot(snap)
				}
			}
		}
	}

	if !isLeader || hasResponded {
		if isLeader {
			o.clearInitialInfo()
		}
		return
	}

	var resp = JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}}
	if err != nil {
		resp.Error = NewJSConsumerCreateError(err, Unless(err))
		s.sendAPIErrResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
	} else {
		resp.ConsumerInfo = o.initialInfo()
		s.sendAPIResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
		if node := o.raftNode(); node != nil {
			o.sendCreateAdvisory()
		}
	}
}

// Determines if we should send lost quorum advisory. We throttle these after first one.
func (o *consumer) shouldSendLostQuorum() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if time.Since(o.lqsent) >= lostQuorumAdvInterval {
		o.lqsent = time.Now()
		return true
	}
	return false
}

func (s *Server) sendConsumerLostQuorumAdvisory(o *consumer) {
	if o == nil {
		return
	}
	node, stream, consumer, acc := o.raftNode(), o.streamName(), o.String(), o.account()
	if node == nil {
		return
	}
	if !o.shouldSendLostQuorum() {
		return
	}

	s.Warnf("JetStream cluster consumer '%s > %s > %s' has NO quorum, stalled.", acc.GetName(), stream, consumer)

	subj := JSAdvisoryConsumerQuorumLostPre + "." + stream + "." + consumer
	adv := &JSConsumerQuorumLostAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerQuorumLostAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   stream,
		Consumer: consumer,
		Replicas: s.replicas(node),
		Domain:   s.getOpts().JetStreamDomain,
	}

	// Send to the user's account if not the system account.
	if acc != s.SystemAccount() {
		s.publishAdvisory(acc, subj, adv)
	}
	// Now do system level one. Place account info in adv, and nil account means system.
	adv.Account = acc.GetName()
	s.publishAdvisory(nil, subj, adv)
}

func (s *Server) sendConsumerLeaderElectAdvisory(o *consumer) {
	if o == nil {
		return
	}
	node, stream, consumer, acc := o.raftNode(), o.streamName(), o.String(), o.account()
	if node == nil {
		return
	}

	subj := JSAdvisoryConsumerLeaderElectedPre + "." + stream + "." + consumer
	adv := &JSConsumerLeaderElectedAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerLeaderElectedAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   stream,
		Consumer: consumer,
		Leader:   s.serverNameForNode(node.GroupLeader()),
		Replicas: s.replicas(node),
		Domain:   s.getOpts().JetStreamDomain,
	}

	// Send to the user's account if not the system account.
	if acc != s.SystemAccount() {
		s.publishAdvisory(acc, subj, adv)
	}
	// Now do system level one. Place account info in adv, and nil account means system.
	adv.Account = acc.GetName()
	s.publishAdvisory(nil, subj, adv)
}

type streamAssignmentResult struct {
	Account  string                      `json:"account"`
	Stream   string                      `json:"stream"`
	Response *JSApiStreamCreateResponse  `json:"create_response,omitempty"`
	Restore  *JSApiStreamRestoreResponse `json:"restore_response,omitempty"`
	Update   bool                        `json:"is_update,omitempty"`
}

// Determine if this is an insufficient resources' error type.
func isInsufficientResourcesErr(resp *JSApiStreamCreateResponse) bool {
	return resp != nil && resp.Error != nil && IsNatsErr(resp.Error, JSInsufficientResourcesErr, JSMemoryResourcesExceededErr, JSStorageResourcesExceededErr)
}

// Process error results of stream and consumer assignments.
// Success will be handled by stream leader.
func (js *jetStream) processStreamAssignmentResults(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
	var result streamAssignmentResult
	if err := json.Unmarshal(msg, &result); err != nil {
		// TODO(dlc) - log
		return
	}
	acc, _ := js.srv.LookupAccount(result.Account)
	if acc == nil {
		// TODO(dlc) - log
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	s, cc := js.srv, js.cluster

	// FIXME(dlc) - suppress duplicates?
	if sa := js.streamAssignment(result.Account, result.Stream); sa != nil {
		canDelete := !result.Update && time.Since(sa.Created) < 5*time.Second

		// See if we should retry in case this cluster is full but there are others.
		if cfg, ci := sa.Config, sa.Client; cfg != nil && ci != nil && isInsufficientResourcesErr(result.Response) && canDelete {
			// If cluster is defined we can not retry.
			if cfg.Placement == nil || cfg.Placement.Cluster == _EMPTY_ {
				// If we have additional clusters to try we can retry.
				if ci != nil && len(ci.Alternates) > 0 {
					if rg := js.createGroupForStream(ci, cfg); rg != nil {
						if org := sa.Group; org != nil && len(org.Peers) > 0 {
							s.Warnf("Retrying cluster placement for stream '%s > %s' due to insufficient resources in cluster %q",
								result.Account, result.Stream, s.clusterNameForNode(org.Peers[0]))
						} else {
							s.Warnf("Retrying cluster placement for stream '%s > %s' due to insufficient resources", result.Account, result.Stream)
						}
						// Pick a new preferred leader.
						rg.setPreferred()
						// Get rid of previous attempt.
						cc.meta.Propose(encodeDeleteStreamAssignment(sa))
						// Propose new.
						sa.Group, sa.err = rg, nil
						cc.meta.Propose(encodeAddStreamAssignment(sa))
						return
					}
				}
			}
		}

		// Respond to the user here.
		var resp string
		if result.Response != nil {
			resp = s.jsonResponse(result.Response)
		} else if result.Restore != nil {
			resp = s.jsonResponse(result.Restore)
		}
		if !sa.responded || result.Update {
			sa.responded = true
			js.srv.sendAPIErrResponse(sa.Client, acc, sa.Subject, sa.Reply, _EMPTY_, resp)
		}
		// Remove this assignment if possible.
		if canDelete {
			sa.err = NewJSClusterNotAssignedError()
			cc.meta.Propose(encodeDeleteStreamAssignment(sa))
		}
	}
}

func (js *jetStream) processConsumerAssignmentResults(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
	var result consumerAssignmentResult
	if err := json.Unmarshal(msg, &result); err != nil {
		// TODO(dlc) - log
		return
	}
	acc, _ := js.srv.LookupAccount(result.Account)
	if acc == nil {
		// TODO(dlc) - log
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	s, cc := js.srv, js.cluster

	if sa := js.streamAssignment(result.Account, result.Stream); sa != nil && sa.consumers != nil {
		if ca := sa.consumers[result.Consumer]; ca != nil && !ca.responded {
			js.srv.sendAPIErrResponse(ca.Client, acc, ca.Subject, ca.Reply, _EMPTY_, s.jsonResponse(result.Response))
			ca.responded = true
			// Check if this failed.
			// TODO(dlc) - Could have mixed results, should track per peer.
			if result.Response.Error != nil {
				// So while we are deleting we will not respond to list/names requests.
				ca.err = NewJSClusterNotAssignedError()
				cc.meta.Propose(encodeDeleteConsumerAssignment(ca))
			}
		}
	}
}

const (
	streamAssignmentSubj   = "$SYS.JSC.STREAM.ASSIGNMENT.RESULT"
	consumerAssignmentSubj = "$SYS.JSC.CONSUMER.ASSIGNMENT.RESULT"
)

// Lock should be held.
func (js *jetStream) startUpdatesSub() {
	cc, s, c := js.cluster, js.srv, js.cluster.c
	if cc.streamResults == nil {
		cc.streamResults, _ = s.systemSubscribe(streamAssignmentSubj, _EMPTY_, false, c, js.processStreamAssignmentResults)
	}
	if cc.consumerResults == nil {
		cc.consumerResults, _ = s.systemSubscribe(consumerAssignmentSubj, _EMPTY_, false, c, js.processConsumerAssignmentResults)
	}
	if cc.stepdown == nil {
		cc.stepdown, _ = s.systemSubscribe(JSApiLeaderStepDown, _EMPTY_, false, c, s.jsLeaderStepDownRequest)
	}
	if cc.peerRemove == nil {
		cc.peerRemove, _ = s.systemSubscribe(JSApiRemoveServer, _EMPTY_, false, c, s.jsLeaderServerRemoveRequest)
	}
}

// Lock should be held.
func (js *jetStream) stopUpdatesSub() {
	cc := js.cluster
	if cc.streamResults != nil {
		cc.s.sysUnsubscribe(cc.streamResults)
		cc.streamResults = nil
	}
	if cc.consumerResults != nil {
		cc.s.sysUnsubscribe(cc.consumerResults)
		cc.consumerResults = nil
	}
	if cc.stepdown != nil {
		cc.s.sysUnsubscribe(cc.stepdown)
		cc.stepdown = nil
	}
	if cc.peerRemove != nil {
		cc.s.sysUnsubscribe(cc.peerRemove)
		cc.peerRemove = nil
	}
}

func (js *jetStream) processLeaderChange(isLeader bool) {
	if isLeader {
		js.srv.Noticef("Self is new JetStream cluster metadata leader")
	} else if node := js.getMetaGroup().GroupLeader(); node == _EMPTY_ {
		js.srv.Noticef("JetStream cluster no metadata leader")
	} else if srv := js.srv.serverNameForNode(node); srv == _EMPTY_ {
		js.srv.Noticef("JetStream cluster new remote metadata leader")
	} else if clst := js.srv.clusterNameForNode(node); clst == _EMPTY_ {
		js.srv.Noticef("JetStream cluster new metadata leader: %s", srv)
	} else {
		js.srv.Noticef("JetStream cluster new metadata leader: %s/%s", srv, clst)
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	if isLeader {
		js.startUpdatesSub()
	} else {
		js.stopUpdatesSub()
		// TODO(dlc) - stepdown.
	}

	// If we have been signaled to check the streams, this is for a bug that left stream
	// assignments with no sync subject after and update and no way to sync/catchup outside of the RAFT layer.
	if isLeader && js.cluster.streamsCheck {
		cc := js.cluster
		for acc, asa := range cc.streams {
			for _, sa := range asa {
				if sa.Sync == _EMPTY_ {
					js.srv.Warnf("Stream assigment corrupt for stream '%s > %s'", acc, sa.Config.Name)
					nsa := &streamAssignment{Group: sa.Group, Config: sa.Config, Subject: sa.Subject, Reply: sa.Reply, Client: sa.Client}
					nsa.Sync = syncSubjForStream()
					cc.meta.Propose(encodeUpdateStreamAssignment(nsa))
				}
			}
		}
		// Clear check.
		cc.streamsCheck = false
	}
}

// Lock should be held.
func (cc *jetStreamCluster) remapStreamAssignment(sa *streamAssignment, removePeer string) bool {
	// Need to select a replacement peer
	s, now, cluster := cc.s, time.Now(), sa.Client.Cluster
	if sa.Config.Placement != nil && sa.Config.Placement.Cluster != _EMPTY_ {
		cluster = sa.Config.Placement.Cluster
	}
	ourID := cc.meta.ID()

	for _, p := range cc.meta.Peers() {
		// If it is not in our list it's probably shutdown, so don't consider.
		if si, ok := s.nodeToInfo.Load(p.ID); !ok || si.(nodeInfo).offline {
			continue
		}
		// Make sure they are active and current and not already part of our group.
		current, lastSeen := p.Current, now.Sub(p.Last)
		// We do not track activity of ourselves so ignore.
		if p.ID == ourID {
			lastSeen = 0
		}
		if !current || lastSeen > lostQuorumInterval || sa.Group.isMember(p.ID) {
			continue
		}
		// Make sure the correct cluster.
		if s.clusterNameForNode(p.ID) != cluster {
			continue
		}
		// If we are here we have our candidate replacement, swap out the old one.
		for i, peer := range sa.Group.Peers {
			if peer == removePeer {
				sa.Group.Peers[i] = p.ID
				// Don't influence preferred leader.
				sa.Group.Preferred = _EMPTY_
				return true
			}
		}
	}
	// If we are here let's remove the peer at least.
	for i, peer := range sa.Group.Peers {
		if peer == removePeer {
			sa.Group.Peers[i] = sa.Group.Peers[len(sa.Group.Peers)-1]
			sa.Group.Peers = sa.Group.Peers[:len(sa.Group.Peers)-1]
			break
		}
	}
	return false
}

// selectPeerGroup will select a group of peers to start a raft group.
func (cc *jetStreamCluster) selectPeerGroup(r int, cluster string, cfg *StreamConfig, existing []string) []string {
	if cluster == _EMPTY_ || cfg == nil {
		return nil
	}

	var maxBytes uint64
	if cfg.MaxBytes > 0 {
		maxBytes = uint64(cfg.MaxBytes)
	}

	// Check for tags.
	var tags []string
	if cfg.Placement != nil && len(cfg.Placement.Tags) > 0 {
		tags = cfg.Placement.Tags
	}

	// Used for weighted sorting based on availability.
	type wn struct {
		id    string
		avail uint64
	}

	var nodes []wn
	s, peers := cc.s, cc.meta.Peers()

	// Map existing.
	var ep map[string]struct{}
	if le := len(existing); le > 0 {
		if le >= r {
			return existing
		}
		ep = make(map[string]struct{})
		for _, p := range existing {
			ep[p] = struct{}{}
		}
	}

	for _, p := range peers {
		si, ok := s.nodeToInfo.Load(p.ID)
		if !ok || si == nil {
			continue
		}
		ni := si.(nodeInfo)
		// Only select from the designated named cluster.
		// If we know its offline or we do not have config or stats don't consider.
		if ni.cluster != cluster || ni.offline || ni.cfg == nil || ni.stats == nil {
			continue
		}

		// If existing also skip, we will add back in to front of the list when done.
		if ep != nil {
			if _, ok := ep[p.ID]; ok {
				continue
			}
		}

		if len(tags) > 0 {
			matched := true
			for _, t := range tags {
				if !ni.tags.Contains(t) {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
		}

		var available uint64
		switch cfg.Storage {
		case MemoryStorage:
			used := ni.stats.ReservedMemory
			if ni.stats.Memory > used {
				used = ni.stats.Memory
			}
			if ni.cfg.MaxMemory > int64(used) {
				available = uint64(ni.cfg.MaxMemory) - used
			}
		case FileStorage:
			used := ni.stats.ReservedStore
			if ni.stats.Store > used {
				used = ni.stats.Store
			}
			if ni.cfg.MaxStore > int64(used) {
				available = uint64(ni.cfg.MaxStore) - used
			}
		}

		// Otherwise check if we have enough room if maxBytes set.
		if maxBytes > 0 && maxBytes > available {
			continue
		}
		// Add to our list of potential nodes.
		nodes = append(nodes, wn{p.ID, available})
	}

	// If we could not select enough peers, fail.
	if len(nodes) < (r - len(existing)) {
		return nil
	}
	// Sort based on available from most to least.
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].avail > nodes[j].avail })

	var results []string
	if len(existing) > 0 {
		results = append(results, existing...)
		r -= len(existing)
	}
	for _, r := range nodes[:r] {
		results = append(results, r.id)
	}
	return results
}

func groupNameForStream(peers []string, storage StorageType) string {
	return groupName("S", peers, storage)
}

func groupNameForConsumer(peers []string, storage StorageType) string {
	return groupName("C", peers, storage)
}

func groupName(prefix string, peers []string, storage StorageType) string {
	var gns string
	if len(peers) == 1 {
		gns = peers[0]
	} else {
		gns = string(getHash(nuid.Next()))
	}
	return fmt.Sprintf("%s-R%d%s-%s", prefix, len(peers), storage.String()[:1], gns)
}

// createGroupForStream will create a group for assignment for the stream.
// Lock should be held.
func (js *jetStream) createGroupForStream(ci *ClientInfo, cfg *StreamConfig) *raftGroup {
	replicas := cfg.Replicas
	if replicas == 0 {
		replicas = 1
	}

	// Default connected cluster from the request origin.
	cc, cluster := js.cluster, ci.Cluster
	// If specified, override the default.
	clusterDefined := cfg.Placement != nil && cfg.Placement.Cluster != _EMPTY_
	if clusterDefined {
		cluster = cfg.Placement.Cluster
	}
	clusters := []string{cluster}
	if !clusterDefined {
		clusters = append(clusters, ci.Alternates...)
	}

	// Need to create a group here.
	for _, cn := range clusters {
		peers := cc.selectPeerGroup(replicas, cn, cfg, nil)
		if len(peers) < replicas {
			continue
		}
		return &raftGroup{Name: groupNameForStream(peers, cfg.Storage), Storage: cfg.Storage, Peers: peers, Cluster: cn}
	}
	return nil
}

func (s *Server) jsClusteredStreamRequest(ci *ClientInfo, acc *Account, subject, reply string, rmsg []byte, config *StreamConfig) {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	var resp = JSApiStreamCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamCreateResponseType}}

	// Grab our jetstream account info.
	acc.mu.RLock()
	jsa := acc.js
	acc.mu.RUnlock()

	if jsa == nil {
		resp.Error = NewJSNotEnabledForAccountError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	ccfg, err := checkStreamCfg(config)
	if err != nil {
		resp.Error = NewJSStreamInvalidConfigError(err, Unless(err))
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	cfg := &ccfg

	// Check for stream limits here before proposing. These need to be tracked from meta layer, not jsa.
	js.mu.RLock()
	asa := cc.streams[acc.Name]
	numStreams := len(asa)
	js.mu.RUnlock()

	jsa.mu.RLock()
	exceeded := jsa.limits.MaxStreams > 0 && numStreams >= jsa.limits.MaxStreams
	jsa.mu.RUnlock()

	if exceeded {
		resp.Error = NewJSMaximumStreamsLimitError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	// Check for account limits here before proposing.
	if err := jsa.checkAccountLimits(cfg); err != nil {
		resp.Error = NewJSStreamLimitsError(err, Unless(err))
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	// Now process the request and proposal.
	js.mu.Lock()
	defer js.mu.Unlock()

	// If this stream already exists, turn this into a stream info call.
	if sa := js.streamAssignment(acc.Name, cfg.Name); sa != nil {
		// If they are the same then we will forward on as a stream info request.
		// This now matches single server behavior.
		if reflect.DeepEqual(sa.Config, cfg) {
			isubj := fmt.Sprintf(JSApiStreamInfoT, cfg.Name)
			// We want to make sure we send along the client info.
			cij, _ := json.Marshal(ci)
			hdr := map[string]string{
				ClientInfoHdr:  string(cij),
				JSResponseType: jsCreateResponse,
			}
			// Send this as system account, but include client info header.
			s.sendInternalAccountMsgWithReply(nil, isubj, reply, hdr, nil, true)
			return
		}

		resp.Error = NewJSStreamNameExistError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	if cfg.Sealed {
		resp.Error = NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration for create can not be sealed"))
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	// Check for subject collisions here.
	for _, sa := range asa {
		for _, subj := range sa.Config.Subjects {
			for _, tsubj := range cfg.Subjects {
				if SubjectsCollide(tsubj, subj) {
					resp.Error = NewJSStreamSubjectOverlapError()
					s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
					return
				}
			}
		}
	}

	// Raft group selection and placement.
	rg := js.createGroupForStream(ci, cfg)
	if rg == nil {
		resp.Error = NewJSInsufficientResourcesError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	// Pick a preferred leader.
	rg.setPreferred()
	// Sync subject for post snapshot sync.
	sa := &streamAssignment{Group: rg, Sync: syncSubjForStream(), Config: cfg, Subject: subject, Reply: reply, Client: ci, Created: time.Now().UTC()}
	cc.meta.Propose(encodeAddStreamAssignment(sa))
}

func (s *Server) jsClusteredStreamUpdateRequest(ci *ClientInfo, acc *Account, subject, reply string, rmsg []byte, cfg *StreamConfig) {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	// Now process the request and proposal.
	js.mu.Lock()
	defer js.mu.Unlock()

	var resp = JSApiStreamUpdateResponse{ApiResponse: ApiResponse{Type: JSApiStreamUpdateResponseType}}

	osa := js.streamAssignment(acc.Name, cfg.Name)

	if osa == nil {
		resp.Error = NewJSStreamNotFoundError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	var newCfg *StreamConfig
	if jsa := js.accounts[acc.Name]; jsa != nil {
		if ncfg, err := jsa.configUpdateCheck(osa.Config, cfg); err != nil {
			resp.Error = NewJSStreamUpdateError(err, Unless(err))
			s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
			return
		} else {
			newCfg = ncfg
		}
	} else {
		resp.Error = NewJSNotEnabledForAccountError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	// Check for mirrot changes which are not allowed.
	if !reflect.DeepEqual(newCfg.Mirror, osa.Config.Mirror) {
		resp.Error = NewJSStreamMirrorNotUpdatableError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	// Check for subject collisions here.
	for _, sa := range cc.streams[acc.Name] {
		if sa == osa {
			continue
		}
		for _, subj := range sa.Config.Subjects {
			for _, tsubj := range newCfg.Subjects {
				if SubjectsCollide(tsubj, subj) {
					resp.Error = NewJSStreamSubjectOverlapError()
					s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
					return
				}
			}
		}
	}

	// Check for replica changes.
	rg := osa.Group
	var consumers []*consumerAssignment

	if newCfg.Replicas != len(rg.Peers) {
		// We are adding new peers here.
		if newCfg.Replicas > len(rg.Peers) {
			peers := cc.selectPeerGroup(newCfg.Replicas, rg.Cluster, newCfg, rg.Peers)
			if len(peers) != newCfg.Replicas {
				resp.Error = NewJSInsufficientResourcesError()
				s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
				return
			}
			// Single nodes are not recorded by the NRG layer so we can rename.
			if len(peers) == 1 {
				rg.Name = groupNameForStream(peers, rg.Storage)
			} else if len(rg.Peers) == 1 {
				// This is scale up from being a singelton, set preferred to that singelton.
				rg.Preferred = rg.Peers[0]
			}
			rg.Peers = peers
		} else {
			// We are deleting nodes here.
			rg.Peers = rg.Peers[:newCfg.Replicas]
		}

		// Need to remap any consumers.
		for _, ca := range osa.consumers {
			// Ephemerals are R=1, so only auto-remap durables, or R>1.
			numPeers := len(ca.Group.Peers)
			if ca.Config.Durable != _EMPTY_ || numPeers > 1 {
				cca := ca.copyGroup()
				// Adjust preferred as needed.
				if numPeers == 1 && len(rg.Peers) > 1 {
					cca.Group.Preferred = ca.Group.Peers[0]
				} else {
					cca.Group.Preferred = _EMPTY_
				}
				// Assign new peers.
				cca.Group.Peers = rg.Peers
				// We can not propose here before the stream itself so we collect them.
				consumers = append(consumers, cca)
			}
		}
	}

	sa := &streamAssignment{Group: rg, Sync: osa.Sync, Config: newCfg, Subject: subject, Reply: reply, Client: ci}
	cc.meta.Propose(encodeUpdateStreamAssignment(sa))

	// Process any staged consumers.
	for _, ca := range consumers {
		cc.meta.Propose(encodeAddConsumerAssignment(ca))
	}
}

func (s *Server) jsClusteredStreamDeleteRequest(ci *ClientInfo, acc *Account, stream, subject, reply string, rmsg []byte) {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	osa := js.streamAssignment(acc.Name, stream)
	if osa == nil {
		var resp = JSApiStreamDeleteResponse{ApiResponse: ApiResponse{Type: JSApiStreamDeleteResponseType}}
		resp.Error = NewJSStreamNotFoundError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	// Remove any remaining consumers as well.
	for _, ca := range osa.consumers {
		ca.Reply, ca.State = _EMPTY_, nil
		cc.meta.Propose(encodeDeleteConsumerAssignment(ca))
	}

	sa := &streamAssignment{Group: osa.Group, Config: osa.Config, Subject: subject, Reply: reply, Client: ci}
	cc.meta.Propose(encodeDeleteStreamAssignment(sa))
}

// Process a clustered purge request.
func (s *Server) jsClusteredStreamPurgeRequest(
	ci *ClientInfo,
	acc *Account,
	mset *stream,
	stream, subject, reply string,
	rmsg []byte,
	preq *JSApiStreamPurgeRequest,
) {

	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	js.mu.Lock()
	sa := js.streamAssignment(acc.Name, stream)
	if sa == nil {
		resp := JSApiStreamPurgeResponse{ApiResponse: ApiResponse{Type: JSApiStreamPurgeResponseType}}
		resp.Error = NewJSStreamNotFoundError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		js.mu.Unlock()
		return
	}

	if n := sa.Group.node; n != nil {
		sp := &streamPurge{Stream: stream, LastSeq: mset.state().LastSeq, Subject: subject, Reply: reply, Client: ci, Request: preq}
		n.Propose(encodeStreamPurge(sp))
		js.mu.Unlock()
		return
	}
	js.mu.Unlock()

	if mset == nil {
		return
	}

	var resp = JSApiStreamPurgeResponse{ApiResponse: ApiResponse{Type: JSApiStreamPurgeResponseType}}
	purged, err := mset.purge(preq)
	if err != nil {
		resp.Error = NewJSStreamGeneralError(err, Unless(err))
	} else {
		resp.Purged = purged
		resp.Success = true
	}
	s.sendAPIResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(resp))
}

func (s *Server) jsClusteredStreamRestoreRequest(
	ci *ClientInfo,
	acc *Account,
	req *JSApiStreamRestoreRequest,
	stream, subject, reply string, rmsg []byte) {

	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	cfg := &req.Config
	resp := JSApiStreamRestoreResponse{ApiResponse: ApiResponse{Type: JSApiStreamRestoreResponseType}}

	if sa := js.streamAssignment(ci.serviceAccount(), cfg.Name); sa != nil {
		resp.Error = NewJSStreamNameExistError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	// Raft group selection and placement.
	rg := js.createGroupForStream(ci, cfg)
	if rg == nil {
		resp.Error = NewJSInsufficientResourcesError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	// Pick a preferred leader.
	rg.setPreferred()
	sa := &streamAssignment{Group: rg, Sync: syncSubjForStream(), Config: cfg, Subject: subject, Reply: reply, Client: ci, Created: time.Now().UTC()}
	// Now add in our restore state and pre-select a peer to handle the actual receipt of the snapshot.
	sa.Restore = &req.State
	cc.meta.Propose(encodeAddStreamAssignment(sa))
}

func (s *Server) allPeersOffline(rg *raftGroup) bool {
	if rg == nil {
		return false
	}
	// Check to see if this stream has any servers online to respond.
	for _, peer := range rg.Peers {
		if si, ok := s.nodeToInfo.Load(peer); ok && si != nil {
			if !si.(nodeInfo).offline {
				return false
			}
		}
	}
	return true
}

// This will do a scatter and gather operation for all streams for this account. This is only called from metadata leader.
// This will be running in a separate Go routine.
func (s *Server) jsClusteredStreamListRequest(acc *Account, ci *ClientInfo, filter string, offset int, subject, reply string, rmsg []byte) {
	defer s.grWG.Done()

	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	js.mu.Lock()

	var streams []*streamAssignment
	for _, sa := range cc.streams[acc.Name] {
		if IsNatsErr(sa.err, JSClusterNotAssignedErr) {
			continue
		}

		if filter != _EMPTY_ {
			// These could not have subjects auto-filled in since they are raw and unprocessed.
			if len(sa.Config.Subjects) == 0 {
				if SubjectsCollide(filter, sa.Config.Name) {
					streams = append(streams, sa)
				}
			} else {
				for _, subj := range sa.Config.Subjects {
					if SubjectsCollide(filter, subj) {
						streams = append(streams, sa)
						break
					}
				}
			}
		} else {
			streams = append(streams, sa)
		}
	}

	// Needs to be sorted for offsets etc.
	if len(streams) > 1 {
		sort.Slice(streams, func(i, j int) bool {
			return strings.Compare(streams[i].Config.Name, streams[j].Config.Name) < 0
		})
	}

	scnt := len(streams)
	if offset > scnt {
		offset = scnt
	}
	if offset > 0 {
		streams = streams[offset:]
	}
	if len(streams) > JSApiListLimit {
		streams = streams[:JSApiListLimit]
	}

	var resp = JSApiStreamListResponse{
		ApiResponse: ApiResponse{Type: JSApiStreamListResponseType},
		Streams:     make([]*StreamInfo, 0, len(streams)),
	}

	if len(streams) == 0 {
		js.mu.Unlock()
		resp.Limit = JSApiListLimit
		resp.Offset = offset
		s.sendAPIResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(resp))
		return
	}

	// Create an inbox for our responses and send out our requests.
	s.mu.Lock()
	inbox := s.newRespInbox()
	rc := make(chan *StreamInfo, len(streams))

	// Store our handler.
	s.sys.replies[inbox] = func(sub *subscription, _ *client, _ *Account, subject, _ string, msg []byte) {
		var si StreamInfo
		if err := json.Unmarshal(msg, &si); err != nil {
			s.Warnf("Error unmarshaling clustered stream info response:%v", err)
			return
		}
		select {
		case rc <- &si:
		default:
			s.Warnf("Failed placing remote stream info result on internal channel")
		}
	}
	s.mu.Unlock()

	// Cleanup after.
	defer func() {
		s.mu.Lock()
		if s.sys != nil && s.sys.replies != nil {
			delete(s.sys.replies, inbox)
		}
		s.mu.Unlock()
	}()

	var missingNames []string
	sent := map[string]int{}
	// Send out our requests here.
	for _, sa := range streams {
		if s.allPeersOffline(sa.Group) {
			// Place offline onto our results by hand here.
			si := &StreamInfo{Config: *sa.Config, Created: sa.Created, Cluster: js.offlineClusterInfo(sa.Group)}
			resp.Streams = append(resp.Streams, si)
			missingNames = append(missingNames, sa.Config.Name)
		} else {
			isubj := fmt.Sprintf(clusterStreamInfoT, sa.Client.serviceAccount(), sa.Config.Name)
			s.sendInternalMsgLocked(isubj, inbox, nil, nil)
			sent[sa.Config.Name] = len(sa.consumers)
		}
	}
	// Don't hold lock.
	js.mu.Unlock()

	const timeout = 4 * time.Second
	notActive := time.NewTimer(timeout)
	defer notActive.Stop()

LOOP:
	for {
		select {
		case <-s.quitCh:
			return
		case <-notActive.C:
			s.Warnf("Did not receive all stream info results for %q", acc)
			for sName := range sent {
				missingNames = append(missingNames, sName)
			}
			resp.Missing = missingNames
			break LOOP
		case si := <-rc:
			consCount := sent[si.Config.Name]
			if consCount > 0 {
				si.State.Consumers = consCount
			}
			delete(sent, si.Config.Name)
			resp.Streams = append(resp.Streams, si)
			// Check to see if we are done.
			if len(resp.Streams) == len(streams) {
				break LOOP
			}
		}
	}

	// Needs to be sorted as well.
	if len(resp.Streams) > 1 {
		sort.Slice(resp.Streams, func(i, j int) bool {
			return strings.Compare(resp.Streams[i].Config.Name, resp.Streams[j].Config.Name) < 0
		})
	}

	resp.Total = len(resp.Streams)
	resp.Limit = JSApiListLimit
	resp.Offset = offset
	s.sendAPIResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(resp))
}

// This will do a scatter and gather operation for all consumers for this stream and account.
// This will be running in a separate Go routine.
func (s *Server) jsClusteredConsumerListRequest(acc *Account, ci *ClientInfo, offset int, stream, subject, reply string, rmsg []byte) {
	defer s.grWG.Done()

	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	js.mu.Lock()

	var consumers []*consumerAssignment
	if sas := cc.streams[acc.Name]; sas != nil {
		if sa := sas[stream]; sa != nil {
			// Copy over since we need to sort etc.
			for _, ca := range sa.consumers {
				consumers = append(consumers, ca)
			}
		}
	}
	// Needs to be sorted.
	if len(consumers) > 1 {
		sort.Slice(consumers, func(i, j int) bool {
			return strings.Compare(consumers[i].Name, consumers[j].Name) < 0
		})
	}

	ocnt := len(consumers)
	if offset > ocnt {
		offset = ocnt
	}
	if offset > 0 {
		consumers = consumers[offset:]
	}
	if len(consumers) > JSApiListLimit {
		consumers = consumers[:JSApiListLimit]
	}

	// Send out our requests here.
	var resp = JSApiConsumerListResponse{
		ApiResponse: ApiResponse{Type: JSApiConsumerListResponseType},
		Consumers:   []*ConsumerInfo{},
	}

	if len(consumers) == 0 {
		js.mu.Unlock()
		resp.Limit = JSApiListLimit
		resp.Offset = offset
		s.sendAPIResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(resp))
		return
	}

	// Create an inbox for our responses and send out requests.
	s.mu.Lock()
	inbox := s.newRespInbox()
	rc := make(chan *ConsumerInfo, len(consumers))

	// Store our handler.
	s.sys.replies[inbox] = func(sub *subscription, _ *client, _ *Account, subject, _ string, msg []byte) {
		var ci ConsumerInfo
		if err := json.Unmarshal(msg, &ci); err != nil {
			s.Warnf("Error unmarshaling clustered consumer info response:%v", err)
			return
		}
		select {
		case rc <- &ci:
		default:
			s.Warnf("Failed placing consumer info result on internal chan")
		}
	}
	s.mu.Unlock()

	// Cleanup after.
	defer func() {
		s.mu.Lock()
		if s.sys != nil && s.sys.replies != nil {
			delete(s.sys.replies, inbox)
		}
		s.mu.Unlock()
	}()

	var missingNames []string
	sent := map[string]struct{}{}
	for _, ca := range consumers {
		if s.allPeersOffline(ca.Group) {
			// Place offline onto our results by hand here.
			ci := &ConsumerInfo{Config: ca.Config, Created: ca.Created, Cluster: js.offlineClusterInfo(ca.Group)}
			resp.Consumers = append(resp.Consumers, ci)
			missingNames = append(missingNames, ci.Name)
		} else {
			isubj := fmt.Sprintf(clusterConsumerInfoT, ca.Client.serviceAccount(), stream, ca.Name)
			s.sendInternalMsgLocked(isubj, inbox, nil, nil)
			sent[ca.Name] = struct{}{}
		}
	}
	js.mu.Unlock()

	const timeout = 4 * time.Second
	notActive := time.NewTimer(timeout)
	defer notActive.Stop()

LOOP:
	for {
		select {
		case <-s.quitCh:
			return
		case <-notActive.C:
			s.Warnf("Did not receive all consumer info results for %q", acc)
			for cName := range sent {
				missingNames = append(missingNames, cName)
			}
			resp.Missing = missingNames
			break LOOP
		case ci := <-rc:
			delete(sent, ci.Name)
			resp.Consumers = append(resp.Consumers, ci)
			// Check to see if we are done.
			if len(resp.Consumers) == len(consumers) {
				break LOOP
			}
		}
	}

	// Needs to be sorted as well.
	if len(resp.Consumers) > 1 {
		sort.Slice(resp.Consumers, func(i, j int) bool {
			return strings.Compare(resp.Consumers[i].Name, resp.Consumers[j].Name) < 0
		})
	}

	resp.Total = len(resp.Consumers)
	resp.Limit = JSApiListLimit
	resp.Offset = offset
	s.sendAPIResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(resp))
}

func encodeStreamPurge(sp *streamPurge) []byte {
	var bb bytes.Buffer
	bb.WriteByte(byte(purgeStreamOp))
	json.NewEncoder(&bb).Encode(sp)
	return bb.Bytes()
}

func decodeStreamPurge(buf []byte) (*streamPurge, error) {
	var sp streamPurge
	err := json.Unmarshal(buf, &sp)
	return &sp, err
}

func (s *Server) jsClusteredConsumerDeleteRequest(ci *ClientInfo, acc *Account, stream, consumer, subject, reply string, rmsg []byte) {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	var resp = JSApiConsumerDeleteResponse{ApiResponse: ApiResponse{Type: JSApiConsumerDeleteResponseType}}

	sa := js.streamAssignment(acc.Name, stream)
	if sa == nil {
		resp.Error = NewJSStreamNotFoundError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return

	}
	if sa.consumers == nil {
		resp.Error = NewJSConsumerNotFoundError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	oca := sa.consumers[consumer]
	if oca == nil {
		resp.Error = NewJSConsumerNotFoundError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	oca.deleted = true
	ca := &consumerAssignment{Group: oca.Group, Stream: stream, Name: consumer, Config: oca.Config, Subject: subject, Reply: reply, Client: ci}
	cc.meta.Propose(encodeDeleteConsumerAssignment(ca))
}

func encodeMsgDelete(md *streamMsgDelete) []byte {
	var bb bytes.Buffer
	bb.WriteByte(byte(deleteMsgOp))
	json.NewEncoder(&bb).Encode(md)
	return bb.Bytes()
}

func decodeMsgDelete(buf []byte) (*streamMsgDelete, error) {
	var md streamMsgDelete
	err := json.Unmarshal(buf, &md)
	return &md, err
}

func (s *Server) jsClusteredMsgDeleteRequest(ci *ClientInfo, acc *Account, mset *stream, stream, subject, reply string, req *JSApiMsgDeleteRequest, rmsg []byte) {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	js.mu.Lock()
	sa := js.streamAssignment(acc.Name, stream)
	if sa == nil {
		s.Debugf("Message delete failed, could not locate stream '%s > %s'", acc.Name, stream)
		js.mu.Unlock()
		return
	}

	// Check for single replica items.
	if n := sa.Group.node; n != nil {
		md := streamMsgDelete{Seq: req.Seq, NoErase: req.NoErase, Stream: stream, Subject: subject, Reply: reply, Client: ci}
		n.Propose(encodeMsgDelete(&md))
		js.mu.Unlock()
		return
	}
	js.mu.Unlock()

	if mset == nil {
		return
	}

	var err error
	var removed bool
	if req.NoErase {
		removed, err = mset.removeMsg(req.Seq)
	} else {
		removed, err = mset.eraseMsg(req.Seq)
	}
	var resp = JSApiMsgDeleteResponse{ApiResponse: ApiResponse{Type: JSApiMsgDeleteResponseType}}
	if err != nil {
		resp.Error = NewJSStreamMsgDeleteFailedError(err, Unless(err))
	} else if !removed {
		resp.Error = NewJSSequenceNotFoundError(req.Seq)
	} else {
		resp.Success = true
	}
	s.sendAPIResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(resp))
}

func encodeAddStreamAssignment(sa *streamAssignment) []byte {
	var bb bytes.Buffer
	bb.WriteByte(byte(assignStreamOp))
	json.NewEncoder(&bb).Encode(sa)
	return bb.Bytes()
}

func encodeUpdateStreamAssignment(sa *streamAssignment) []byte {
	var bb bytes.Buffer
	bb.WriteByte(byte(updateStreamOp))
	json.NewEncoder(&bb).Encode(sa)
	return bb.Bytes()
}

func encodeDeleteStreamAssignment(sa *streamAssignment) []byte {
	var bb bytes.Buffer
	bb.WriteByte(byte(removeStreamOp))
	json.NewEncoder(&bb).Encode(sa)
	return bb.Bytes()
}

func decodeStreamAssignment(buf []byte) (*streamAssignment, error) {
	var sa streamAssignment
	err := json.Unmarshal(buf, &sa)
	return &sa, err
}

// createGroupForConsumer will create a new group with same peer set as the stream.
func (cc *jetStreamCluster) createGroupForConsumer(sa *streamAssignment) *raftGroup {
	peers := sa.Group.Peers
	if len(peers) == 0 {
		return nil
	}
	return &raftGroup{Name: groupNameForConsumer(peers, sa.Config.Storage), Storage: sa.Config.Storage, Peers: peers}
}

// jsClusteredConsumerRequest is first point of entry to create a consumer with R > 1.
func (s *Server) jsClusteredConsumerRequest(ci *ClientInfo, acc *Account, subject, reply string, rmsg []byte, stream string, cfg *ConsumerConfig) {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	var resp = JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}}

	// Lookup the stream assignment.
	sa := js.streamAssignment(acc.Name, stream)
	if sa == nil {
		resp.Error = NewJSStreamNotFoundError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	// Check for max consumers here to short circuit if possible.
	if maxc := sa.Config.MaxConsumers; maxc > 0 {
		// Don't count DIRECTS.
		total := 0
		for _, ca := range sa.consumers {
			if ca.Config != nil && !ca.Config.Direct {
				total++
			}
		}
		if total >= maxc {
			resp.Error = NewJSMaximumConsumersLimitError()
			s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
			return
		}
	}

	// Also short circuit if DeliverLastPerSubject is set with no FilterSubject.
	if cfg.DeliverPolicy == DeliverLastPerSubject {
		if cfg.FilterSubject == _EMPTY_ {
			resp.Error = NewJSConsumerInvalidPolicyError(fmt.Errorf("consumer delivery policy is deliver last per subject, but FilterSubject is not set"))
			s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
			return
		}
	}

	// Setup proper default for ack wait if we are in explicit ack mode.
	if cfg.AckWait == 0 && (cfg.AckPolicy == AckExplicit || cfg.AckPolicy == AckAll) {
		cfg.AckWait = JsAckWaitDefault
	}
	// Setup default of -1, meaning no limit for MaxDeliver.
	if cfg.MaxDeliver == 0 {
		cfg.MaxDeliver = -1
	}
	// Set proper default for max ack pending if we are ack explicit and none has been set.
	if cfg.AckPolicy == AckExplicit && cfg.MaxAckPending == 0 {
		cfg.MaxAckPending = JsDefaultMaxAckPending
	}

	var ca *consumerAssignment
	var oname string

	// See if we have an existing one already under same durable name.
	if isDurableConsumer(cfg) {
		oname = cfg.Durable
		if ca = sa.consumers[oname]; ca != nil && !ca.deleted {
			// Do quick sanity check on new cfg to prevent here if possible.
			if err := acc.checkNewConsumerConfig(ca.Config, cfg); err != nil {
				resp.Error = NewJSConsumerCreateError(err, Unless(err))
				s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
				return
			}
		}
	}

	// If this is new consumer.
	if ca == nil {
		rg := cc.createGroupForConsumer(sa)
		if rg == nil {
			resp.Error = NewJSInsufficientResourcesError()
			s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
			return
		}
		// Pick a preferred leader.
		rg.setPreferred()

		// We need to set the ephemeral here before replicating.
		if !isDurableConsumer(cfg) {
			// We chose to have ephemerals be R=1 unless stream is interest or workqueue.
			if sa.Config.Retention == LimitsPolicy {
				rg.Peers = []string{rg.Preferred}
				rg.Name = groupNameForConsumer(rg.Peers, rg.Storage)
			}
			// Make sure name is unique.
			for {
				oname = createConsumerName()
				if sa.consumers != nil {
					if sa.consumers[oname] != nil {
						continue
					}
				}
				break
			}
		}
		ca = &consumerAssignment{
			Group:   rg,
			Stream:  stream,
			Name:    oname,
			Config:  cfg,
			Subject: subject,
			Reply:   reply,
			Client:  ci,
			Created: time.Now().UTC(),
		}
	} else {
		// Update config and client info on copy of existing.
		nca := *ca
		nca.Config = cfg
		nca.Client = ci
		nca.Subject = subject
		nca.Reply = reply
		ca = &nca
	}

	eca := encodeAddConsumerAssignment(ca)

	// Mark this as pending.
	if sa.consumers == nil {
		sa.consumers = make(map[string]*consumerAssignment)
	}
	ca.pending = true
	sa.consumers[ca.Name] = ca

	// Do formal proposal.
	cc.meta.Propose(eca)
}

func encodeAddConsumerAssignment(ca *consumerAssignment) []byte {
	var bb bytes.Buffer
	bb.WriteByte(byte(assignConsumerOp))
	json.NewEncoder(&bb).Encode(ca)
	return bb.Bytes()
}

func encodeDeleteConsumerAssignment(ca *consumerAssignment) []byte {
	var bb bytes.Buffer
	bb.WriteByte(byte(removeConsumerOp))
	json.NewEncoder(&bb).Encode(ca)
	return bb.Bytes()
}

func decodeConsumerAssignment(buf []byte) (*consumerAssignment, error) {
	var ca consumerAssignment
	err := json.Unmarshal(buf, &ca)
	return &ca, err
}

func encodeAddConsumerAssignmentCompressed(ca *consumerAssignment) []byte {
	b, err := json.Marshal(ca)
	if err != nil {
		return nil
	}
	// TODO(dlc) - Streaming better approach here probably.
	var bb bytes.Buffer
	bb.WriteByte(byte(assignCompressedConsumerOp))
	bb.Write(s2.Encode(nil, b))
	return bb.Bytes()
}

func decodeConsumerAssignmentCompressed(buf []byte) (*consumerAssignment, error) {
	var ca consumerAssignment
	js, err := s2.Decode(nil, buf)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(js, &ca)
	return &ca, err
}

var errBadStreamMsg = errors.New("jetstream cluster bad replicated stream msg")

func decodeStreamMsg(buf []byte) (subject, reply string, hdr, msg []byte, lseq uint64, ts int64, err error) {
	var le = binary.LittleEndian
	if len(buf) < 26 {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, errBadStreamMsg
	}
	lseq = le.Uint64(buf)
	buf = buf[8:]
	ts = int64(le.Uint64(buf))
	buf = buf[8:]
	sl := int(le.Uint16(buf))
	buf = buf[2:]
	if len(buf) < sl {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, errBadStreamMsg
	}
	subject = string(buf[:sl])
	buf = buf[sl:]
	if len(buf) < 2 {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, errBadStreamMsg
	}
	rl := int(le.Uint16(buf))
	buf = buf[2:]
	if len(buf) < rl {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, errBadStreamMsg
	}
	reply = string(buf[:rl])
	buf = buf[rl:]
	if len(buf) < 2 {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, errBadStreamMsg
	}
	hl := int(le.Uint16(buf))
	buf = buf[2:]
	if len(buf) < hl {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, errBadStreamMsg
	}
	if hdr = buf[:hl]; len(hdr) == 0 {
		hdr = nil
	}
	buf = buf[hl:]
	if len(buf) < 4 {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, errBadStreamMsg
	}
	ml := int(le.Uint32(buf))
	buf = buf[4:]
	if len(buf) < ml {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, errBadStreamMsg
	}
	if msg = buf[:ml]; len(msg) == 0 {
		msg = nil
	}
	return subject, reply, hdr, msg, lseq, ts, nil
}

func encodeStreamMsg(subject, reply string, hdr, msg []byte, lseq uint64, ts int64) []byte {
	elen := 1 + 8 + 8 + len(subject) + len(reply) + len(hdr) + len(msg)
	elen += (2 + 2 + 2 + 4) // Encoded lengths, 4bytes
	// TODO(dlc) - check sizes of subject, reply and hdr, make sure uint16 ok.
	buf := make([]byte, elen)
	buf[0] = byte(streamMsgOp)
	var le = binary.LittleEndian
	wi := 1
	le.PutUint64(buf[wi:], lseq)
	wi += 8
	le.PutUint64(buf[wi:], uint64(ts))
	wi += 8
	le.PutUint16(buf[wi:], uint16(len(subject)))
	wi += 2
	copy(buf[wi:], subject)
	wi += len(subject)
	le.PutUint16(buf[wi:], uint16(len(reply)))
	wi += 2
	copy(buf[wi:], reply)
	wi += len(reply)
	le.PutUint16(buf[wi:], uint16(len(hdr)))
	wi += 2
	if len(hdr) > 0 {
		copy(buf[wi:], hdr)
		wi += len(hdr)
	}
	le.PutUint32(buf[wi:], uint32(len(msg)))
	wi += 4
	if len(msg) > 0 {
		copy(buf[wi:], msg)
		wi += len(msg)
	}
	return buf[:wi]
}

// StreamSnapshot is used for snapshotting and out of band catch up in clustered mode.
type streamSnapshot struct {
	Msgs     uint64   `json:"messages"`
	Bytes    uint64   `json:"bytes"`
	FirstSeq uint64   `json:"first_seq"`
	LastSeq  uint64   `json:"last_seq"`
	Failed   uint64   `json:"clfs"`
	Deleted  []uint64 `json:"deleted,omitempty"`
}

// Grab a snapshot of a stream for clustered mode.
func (mset *stream) stateSnapshot() []byte {
	mset.mu.RLock()
	defer mset.mu.RUnlock()

	state := mset.store.State()
	snap := &streamSnapshot{
		Msgs:     state.Msgs,
		Bytes:    state.Bytes,
		FirstSeq: state.FirstSeq,
		LastSeq:  state.LastSeq,
		Failed:   mset.clfs,
		Deleted:  state.Deleted,
	}
	b, _ := json.Marshal(snap)
	return b
}

// processClusteredMsg will propose the inbound message to the underlying raft group.
func (mset *stream) processClusteredInboundMsg(subject, reply string, hdr, msg []byte) error {
	// For possible error response.
	var response []byte

	mset.mu.RLock()
	canRespond := !mset.cfg.NoAck && len(reply) > 0
	name, stype := mset.cfg.Name, mset.cfg.Storage
	s, js, jsa, st, rf, outq, node := mset.srv, mset.js, mset.jsa, mset.cfg.Storage, mset.cfg.Replicas, mset.outq, mset.node
	maxMsgSize, lseq := int(mset.cfg.MaxMsgSize), mset.lseq
	mset.mu.RUnlock()

	// This should not happen but possible now that we allow scale up, and scale down where this could trigger.
	if node == nil {
		return mset.processJetStreamMsg(subject, reply, hdr, msg, 0, 0)
	}

	// Check here pre-emptively if we have exceeded this server limits.
	if js.limitsExceeded(stype) {
		s.resourcesExeededError()
		if canRespond {
			b, _ := json.Marshal(&JSPubAckResponse{PubAck: &PubAck{Stream: name}, Error: NewJSInsufficientResourcesError()})
			outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, nil, b, nil, 0))
		}
		// Stepdown regardless.
		if node := mset.raftNode(); node != nil {
			node.StepDown()
		}
		return NewJSInsufficientResourcesError()
	}

	// Check here pre-emptively if we have exceeded our account limits.
	var exceeded bool
	jsa.mu.RLock()
	if st == MemoryStorage {
		total := jsa.storeTotal + int64(memStoreMsgSize(subject, hdr, msg)*uint64(rf))
		if jsa.limits.MaxMemory > 0 && total > jsa.limits.MaxMemory {
			exceeded = true
		}
	} else {
		total := jsa.storeTotal + int64(fileStoreMsgSize(subject, hdr, msg)*uint64(rf))
		if jsa.limits.MaxStore > 0 && total > jsa.limits.MaxStore {
			exceeded = true
		}
	}
	jsa.mu.RUnlock()

	// If we have exceeded our account limits go ahead and return.
	if exceeded {
		err := fmt.Errorf("JetStream resource limits exceeded for account: %q", jsa.acc().Name)
		s.Warnf(err.Error())
		if canRespond {
			var resp = &JSPubAckResponse{PubAck: &PubAck{Stream: name}}
			resp.Error = NewJSAccountResourcesExceededError()
			response, _ = json.Marshal(resp)
			outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, nil, response, nil, 0))
		}
		return err
	}

	// Check msgSize if we have a limit set there. Again this works if it goes through but better to be pre-emptive.
	if maxMsgSize >= 0 && (len(hdr)+len(msg)) > maxMsgSize {
		err := fmt.Errorf("JetStream message size exceeds limits for '%s > %s'", jsa.acc().Name, mset.cfg.Name)
		s.Warnf(err.Error())
		if canRespond {
			var resp = &JSPubAckResponse{PubAck: &PubAck{Stream: name}}
			resp.Error = NewJSStreamMessageExceedsMaximumError()
			response, _ = json.Marshal(resp)
			outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, nil, response, nil, 0))
		}
		return err
	}

	// Since we encode header len as u16 make sure we do not exceed.
	// Again this works if it goes through but better to be pre-emptive.
	if len(hdr) > math.MaxUint16 {
		err := fmt.Errorf("JetStream header size exceeds limits for '%s > %s'", jsa.acc().Name, mset.cfg.Name)
		s.Warnf(err.Error())
		if canRespond {
			var resp = &JSPubAckResponse{PubAck: &PubAck{Stream: name}}
			resp.Error = NewJSStreamHeaderExceedsMaximumError()
			response, _ = json.Marshal(resp)
			outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, nil, response, nil, 0))
		}
		return err
	}

	// Proceed with proposing this message.

	// We only use mset.clseq for clustering and in case we run ahead of actual commits.
	// Check if we need to set initial value here
	mset.clMu.Lock()
	if mset.clseq == 0 || mset.clseq < lseq {
		mset.clseq = mset.lastSeq()
	}

	esm := encodeStreamMsg(subject, reply, hdr, msg, mset.clseq, time.Now().UnixNano())
	mset.clseq++

	// Do proposal.
	err := node.Propose(esm)
	if err != nil && mset.clseq > 0 {
		mset.clseq--
	}
	mset.clMu.Unlock()

	if err != nil {
		if canRespond {
			var resp = &JSPubAckResponse{PubAck: &PubAck{Stream: mset.cfg.Name}}
			resp.Error = &ApiError{Code: 503, Description: err.Error()}
			response, _ = json.Marshal(resp)
			// If we errored out respond here.
			outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, nil, response, nil, 0))
		}
	}

	if err != nil && isOutOfSpaceErr(err) {
		s.handleOutOfSpace(mset)
	}

	return err
}

// For requesting messages post raft snapshot to catch up streams post server restart.
// Any deleted msgs etc will be handled inline on catchup.
type streamSyncRequest struct {
	Peer     string `json:"peer,omitempty"`
	FirstSeq uint64 `json:"first_seq"`
	LastSeq  uint64 `json:"last_seq"`
}

// Given a stream state that represents a snapshot, calculate the sync request based on our current state.
func (mset *stream) calculateSyncRequest(state *StreamState, snap *streamSnapshot) *streamSyncRequest {
	// Quick check if we are already caught up.
	if state.LastSeq >= snap.LastSeq {
		return nil
	}
	return &streamSyncRequest{FirstSeq: state.LastSeq + 1, LastSeq: snap.LastSeq, Peer: mset.node.ID()}
}

// processSnapshotDeletes will update our current store based on the snapshot
// but only processing deletes and new FirstSeq / purges.
func (mset *stream) processSnapshotDeletes(snap *streamSnapshot) {
	state := mset.state()

	// Adjust if FirstSeq has moved.
	if snap.FirstSeq > state.FirstSeq {
		mset.store.Compact(snap.FirstSeq)
		state = mset.store.State()
		mset.setLastSeq(snap.LastSeq)
	}
	// Range the deleted and delete if applicable.
	for _, dseq := range snap.Deleted {
		if dseq <= state.LastSeq {
			mset.store.RemoveMsg(dseq)
		}
	}
}

func (mset *stream) setCatchupPeer(peer string, lag uint64) {
	if peer == _EMPTY_ {
		return
	}
	mset.mu.Lock()
	if mset.catchups == nil {
		mset.catchups = make(map[string]uint64)
	}
	mset.catchups[peer] = lag
	mset.mu.Unlock()
}

// Will decrement by one.
func (mset *stream) updateCatchupPeer(peer string) {
	if peer == _EMPTY_ {
		return
	}
	mset.mu.Lock()
	if lag := mset.catchups[peer]; lag > 0 {
		mset.catchups[peer] = lag - 1
	}
	mset.mu.Unlock()
}

func (mset *stream) clearCatchupPeer(peer string) {
	mset.mu.Lock()
	if mset.catchups != nil {
		delete(mset.catchups, peer)
	}
	mset.mu.Unlock()
}

// Lock should be held.
func (mset *stream) clearAllCatchupPeers() {
	if mset.catchups != nil {
		mset.catchups = nil
	}
}

func (mset *stream) lagForCatchupPeer(peer string) uint64 {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	if mset.catchups == nil {
		return 0
	}
	return mset.catchups[peer]
}

func (mset *stream) hasCatchupPeers() bool {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return len(mset.catchups) > 0
}

func (mset *stream) setCatchingUp() {
	mset.mu.Lock()
	mset.catchup = true
	mset.mu.Unlock()
}

func (mset *stream) clearCatchingUp() {
	mset.mu.Lock()
	mset.catchup = false
	mset.mu.Unlock()
}

func (mset *stream) isCatchingUp() bool {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.catchup
}

// Process a stream snapshot.
func (mset *stream) processSnapshot(snap *streamSnapshot) error {
	// Update any deletes, etc.
	mset.processSnapshotDeletes(snap)

	mset.mu.Lock()
	var state StreamState
	mset.clfs = snap.Failed
	mset.store.FastState(&state)
	sreq := mset.calculateSyncRequest(&state, snap)
	s, js, subject, n := mset.srv, mset.js, mset.sa.Sync, mset.node
	qname := fmt.Sprintf("Stream %q snapshot", mset.cfg.Name)
	mset.mu.Unlock()

	// Make sure our state's first sequence is <= the leader's snapshot.
	if snap.FirstSeq < state.FirstSeq {
		return errFirstSequenceMismatch
	}

	// Bug that would cause this to be empty on stream update.
	if subject == _EMPTY_ {
		return errors.New("corrupt stream assignment detected")
	}

	// Just return if up to date or already exceeded limits.
	if sreq == nil || js.limitsExceeded(mset.cfg.Storage) {
		return nil
	}

	// Pause the apply channel for our raft group while we catch up.
	n.PauseApply()
	defer n.ResumeApply()

	// Set our catchup state.
	mset.setCatchingUp()
	defer mset.clearCatchingUp()

	var sub *subscription
	var err error

	const activityInterval = 5 * time.Second
	notActive := time.NewTimer(activityInterval)
	defer notActive.Stop()

	defer func() {
		if sub != nil {
			s.sysUnsubscribe(sub)
		}
		// Make sure any consumers are updated for the pending amounts.
		mset.mu.Lock()
		for _, o := range mset.consumers {
			o.mu.Lock()
			if o.isLeader() {
				// This expects mset lock to be held.
				o.setInitialPendingAndStart()
			}
			o.mu.Unlock()
		}
		mset.mu.Unlock()
	}()

RETRY:
	// If we have a sub clear that here.
	if sub != nil {
		s.sysUnsubscribe(sub)
		sub = nil
	}

	// Grab sync request again on failures.
	if sreq == nil {
		mset.mu.Lock()
		var state StreamState
		mset.store.FastState(&state)
		sreq = mset.calculateSyncRequest(&state, snap)
		mset.mu.Unlock()
		if sreq == nil {
			return nil
		}
	}

	// Used to transfer message from the wire to another Go routine internally.
	type im struct {
		msg   []byte
		reply string
	}

	msgsQ := newIPQueue(ipQueue_Logger(qname, s.ipqLog)) // of *im

	// Send our catchup request here.
	reply := syncReplySubject()
	sub, err = s.sysSubscribe(reply, func(_ *subscription, _ *client, _ *Account, _, reply string, msg []byte) {
		// Make copies
		// TODO(dlc) - Since we are using a buffer from the inbound client/route.
		msgsQ.push(&im{copyBytes(msg), reply})
	})
	if err != nil {
		s.Errorf("Could not subscribe to stream catchup: %v", err)
		return err
	}

	b, _ := json.Marshal(sreq)
	s.sendInternalMsgLocked(subject, reply, nil, b)

	// Clear our sync request and capture last.
	last := sreq.LastSeq
	sreq = nil

	// Run our own select loop here.
	for qch, lch := n.QuitC(), n.LeadChangeC(); ; {
		select {
		case <-msgsQ.ch:
			notActive.Reset(activityInterval)

			mrecs := msgsQ.pop()
			for _, mreci := range mrecs {
				mrec := mreci.(*im)
				msg := mrec.msg

				// Check for eof signaling.
				if len(msg) == 0 {
					return nil
				}
				if lseq, err := mset.processCatchupMsg(msg); err == nil {
					if lseq >= last {
						return nil
					}
				} else if isOutOfSpaceErr(err) {
					return err
				} else if err == NewJSInsufficientResourcesError() {
					if mset.js.limitsExceeded(mset.cfg.Storage) {
						s.resourcesExeededError()
					} else {
						s.Warnf("Catchup for stream '%s > %s' errored, account resources exceeded: %v", mset.account(), mset.name(), err)
					}
					return err
				} else {
					s.Warnf("Catchup for stream '%s > %s' errored, will retry: %v", mset.account(), mset.name(), err)
					goto RETRY
				}
				if mrec.reply != _EMPTY_ {
					s.sendInternalMsgLocked(mrec.reply, _EMPTY_, nil, nil)
				}
			}
			msgsQ.recycle(&mrecs)
		case <-notActive.C:
			s.Warnf("Catchup for stream '%s > %s' stalled", mset.account(), mset.name())
			notActive.Reset(activityInterval)
			goto RETRY
		case <-s.quitCh:
			return nil
		case <-qch:
			return nil
		case isLeader := <-lch:
			js.processStreamLeaderChange(mset, isLeader)
			return nil
		}
	}
}

// processCatchupMsg will be called to process out of band catchup msgs from a sync request.
func (mset *stream) processCatchupMsg(msg []byte) (uint64, error) {
	if len(msg) == 0 || entryOp(msg[0]) != streamMsgOp {
		return 0, errors.New("bad catchup msg")
	}

	subj, _, hdr, msg, seq, ts, err := decodeStreamMsg(msg[1:])
	if err != nil {
		return 0, errors.New("bad catchup msg")
	}

	mset.mu.RLock()
	st := mset.cfg.Storage
	mset.mu.RUnlock()

	if mset.js.limitsExceeded(st) || mset.jsa.limitsExceeded(st) {
		return 0, NewJSInsufficientResourcesError()
	}

	// Put into our store
	// Messages to be skipped have no subject or timestamp.
	// TODO(dlc) - formalize with skipMsgOp
	if subj == _EMPTY_ && ts == 0 {
		lseq := mset.store.SkipMsg()
		if lseq != seq {
			return 0, errors.New("wrong sequence for skipped msg")
		}
	} else if err := mset.store.StoreRawMsg(subj, hdr, msg, seq, ts); err != nil {
		return 0, err
	}
	// Update our lseq.
	mset.setLastSeq(seq)

	return seq, nil
}

func (mset *stream) handleClusterSyncRequest(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
	var sreq streamSyncRequest
	if err := json.Unmarshal(msg, &sreq); err != nil {
		// Log error.
		return
	}
	mset.srv.startGoRoutine(func() { mset.runCatchup(reply, &sreq) })
}

// Lock should be held.
func (js *jetStream) offlineClusterInfo(rg *raftGroup) *ClusterInfo {
	s := js.srv

	ci := &ClusterInfo{Name: s.ClusterName()}
	for _, peer := range rg.Peers {
		if sir, ok := s.nodeToInfo.Load(peer); ok && sir != nil {
			si := sir.(nodeInfo)
			pi := &PeerInfo{Name: si.name, Current: false, Offline: true}
			ci.Replicas = append(ci.Replicas, pi)
		}
	}
	return ci
}

// clusterInfo will report on the status of the raft group.
func (js *jetStream) clusterInfo(rg *raftGroup) *ClusterInfo {
	if js == nil {
		return nil
	}
	js.mu.RLock()
	defer js.mu.RUnlock()

	s := js.srv
	if rg == nil || rg.node == nil {
		return &ClusterInfo{
			Name:   s.ClusterName(),
			Leader: s.Name(),
		}
	}
	n := rg.node

	ci := &ClusterInfo{
		Name:   s.ClusterName(),
		Leader: s.serverNameForNode(n.GroupLeader()),
	}

	now := time.Now()

	id, peers := n.ID(), n.Peers()

	// If we are leaderless, do not suppress putting us in the peer list.
	if ci.Leader == _EMPTY_ {
		id = _EMPTY_
	}

	for _, rp := range peers {
		if rp.ID != id && rg.isMember(rp.ID) {
			var lastSeen time.Duration
			if now.After(rp.Last) && rp.Last.Unix() != 0 {
				lastSeen = now.Sub(rp.Last)
			}
			current := rp.Current
			if current && lastSeen > lostQuorumInterval {
				current = false
			}
			if sir, ok := s.nodeToInfo.Load(rp.ID); ok && sir != nil {
				si := sir.(nodeInfo)
				pi := &PeerInfo{Name: si.name, Current: current, Offline: si.offline, Active: lastSeen, Lag: rp.Lag}
				ci.Replicas = append(ci.Replicas, pi)
			}
		}
	}
	return ci
}

func (mset *stream) checkClusterInfo(si *StreamInfo) {
	for _, r := range si.Cluster.Replicas {
		peer := string(getHash(r.Name))
		if lag := mset.lagForCatchupPeer(peer); lag > 0 {
			r.Current = false
			r.Lag = lag
		}
	}
}

func (mset *stream) handleClusterStreamInfoRequest(sub *subscription, c *client, _ *Account, subject, reply string, _ []byte) {
	mset.mu.RLock()
	sysc, js, sa, config := mset.sysc, mset.srv.js, mset.sa, mset.cfg
	stype := mset.cfg.Storage
	isLeader := mset.isLeader()
	mset.mu.RUnlock()

	// By design all members will receive this. Normally we only want the leader answering.
	// But if we have stalled and lost quorom all can respond.
	if sa != nil && !js.isGroupLeaderless(sa.Group) && !isLeader {
		return
	}

	// If we are here we are in a compromised state due to server limits let someone else answer if they can.
	if !isLeader && js.limitsExceeded(stype) {
		time.Sleep(100 * time.Millisecond)
	}

	si := &StreamInfo{
		Created: mset.createdTime(),
		State:   mset.state(),
		Config:  config,
		Cluster: js.clusterInfo(mset.raftGroup()),
		Sources: mset.sourcesInfo(),
		Mirror:  mset.mirrorInfo(),
	}

	// Check for out of band catchups.
	if mset.hasCatchupPeers() {
		mset.checkClusterInfo(si)
	}

	sysc.sendInternalMsg(reply, _EMPTY_, nil, si)
}

func (mset *stream) runCatchup(sendSubject string, sreq *streamSyncRequest) {
	s := mset.srv
	defer s.grWG.Done()

	const maxOutBytes = int64(1 * 1024 * 1024) // 1MB for now.
	const maxOutMsgs = int32(16384)
	outb := int64(0)
	outm := int32(0)

	// Flow control processing.
	ackReplySize := func(subj string) int64 {
		if li := strings.LastIndexByte(subj, btsep); li > 0 && li < len(subj) {
			return parseAckReplyNum(subj[li+1:])
		}
		return 0
	}

	nextBatchC := make(chan struct{}, 1)
	nextBatchC <- struct{}{}

	// Setup ackReply for flow control.
	ackReply := syncAckSubject()
	ackSub, _ := s.sysSubscribe(ackReply, func(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
		sz := ackReplySize(subject)
		atomic.AddInt64(&outb, -sz)
		atomic.AddInt32(&outm, -1)
		mset.updateCatchupPeer(sreq.Peer)
		select {
		case nextBatchC <- struct{}{}:
		default:
		}
	})
	defer s.sysUnsubscribe(ackSub)
	ackReplyT := strings.ReplaceAll(ackReply, ".*", ".%d")

	// EOF
	defer s.sendInternalMsgLocked(sendSubject, _EMPTY_, nil, nil)

	const activityInterval = 5 * time.Second
	notActive := time.NewTimer(activityInterval)
	defer notActive.Stop()

	// Setup sequences to walk through.
	seq, last := sreq.FirstSeq, sreq.LastSeq
	mset.setCatchupPeer(sreq.Peer, last-seq)
	defer mset.clearCatchupPeer(sreq.Peer)

	sendNextBatch := func() {
		for ; seq <= last && atomic.LoadInt64(&outb) <= maxOutBytes && atomic.LoadInt32(&outm) <= maxOutMsgs; seq++ {
			subj, hdr, msg, ts, err := mset.store.LoadMsg(seq)
			// if this is not a deleted msg, bail out.
			if err != nil && err != ErrStoreMsgNotFound && err != errDeletedMsg {
				// break, something changed.
				seq = last + 1
				return
			}
			// S2?
			em := encodeStreamMsg(subj, _EMPTY_, hdr, msg, seq, ts)
			// Place size in reply subject for flow control.
			reply := fmt.Sprintf(ackReplyT, len(em))
			atomic.AddInt64(&outb, int64(len(em)))
			atomic.AddInt32(&outm, 1)
			s.sendInternalMsgLocked(sendSubject, reply, nil, em)
		}
	}

	// Grab stream quit channel.
	mset.mu.RLock()
	qch := mset.qch
	mset.mu.RUnlock()
	if qch == nil {
		return
	}

	// Run as long as we are still active and need catchup.
	// FIXME(dlc) - Purge event? Stream delete?
	for {
		select {
		case <-s.quitCh:
			return
		case <-qch:
			return
		case <-notActive.C:
			s.Warnf("Catchup for stream '%s > %s' stalled", mset.account(), mset.name())
			return
		case <-nextBatchC:
			// Update our activity timer.
			notActive.Reset(activityInterval)
			sendNextBatch()
			// Check if we are finished.
			if seq > last {
				s.Debugf("Done resync for stream '%s > %s'", mset.account(), mset.name())
				return
			}
		}
	}
}

const jscAllSubj = "$JSC.>"

func syncSubjForStream() string {
	return syncSubject("$JSC.SYNC")
}

func syncReplySubject() string {
	return syncSubject("$JSC.R")
}

func infoReplySubject() string {
	return syncSubject("$JSC.R")
}

func syncAckSubject() string {
	return syncSubject("$JSC.ACK") + ".*"
}

func syncSubject(pre string) string {
	var sb strings.Builder
	sb.WriteString(pre)
	sb.WriteByte(btsep)

	var b [replySuffixLen]byte
	rn := rand.Int63()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}

	sb.Write(b[:])
	return sb.String()
}

const (
	clusterStreamInfoT   = "$JSC.SI.%s.%s"
	clusterConsumerInfoT = "$JSC.CI.%s.%s.%s"
	jsaUpdatesSubT       = "$JSC.ARU.%s.*"
	jsaUpdatesPubT       = "$JSC.ARU.%s.%s"
)
