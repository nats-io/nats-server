// Copyright 2020-2025 The NATS Authors
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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats-server/v2/internal/fastrand"

	"github.com/minio/highwayhash"
)

type RaftNode interface {
	Propose(entry []byte) error
	ProposeMulti(entries []*Entry) error
	ForwardProposal(entry []byte) error
	InstallSnapshot(snap []byte) error
	SendSnapshot(snap []byte) error
	NeedSnapshot() bool
	Applied(index uint64) (entries uint64, bytes uint64)
	State() RaftState
	Size() (entries, bytes uint64)
	Progress() (index, commit, applied uint64)
	Leader() bool
	Quorum() bool
	Current() bool
	Healthy() bool
	Term() uint64
	Leaderless() bool
	GroupLeader() string
	HadPreviousLeader() bool
	StepDown(preferred ...string) error
	SetObserver(isObserver bool)
	IsObserver() bool
	Campaign() error
	ID() string
	Group() string
	Peers() []*Peer
	ProposeKnownPeers(knownPeers []string)
	UpdateKnownPeers(knownPeers []string)
	ProposeAddPeer(peer string) error
	ProposeRemovePeer(peer string) error
	AdjustClusterSize(csz int) error
	AdjustBootClusterSize(csz int) error
	ClusterSize() int
	ApplyQ() *ipQueue[*CommittedEntry]
	PauseApply() error
	ResumeApply()
	LeadChangeC() <-chan bool
	QuitC() <-chan struct{}
	Created() time.Time
	Stop()
	WaitForStop()
	Delete()
	RecreateInternalSubs() error
	IsSystemAccount() bool
}

type WAL interface {
	Type() StorageType
	StoreMsg(subj string, hdr, msg []byte, ttl int64) (uint64, int64, error)
	LoadMsg(index uint64, sm *StoreMsg) (*StoreMsg, error)
	RemoveMsg(index uint64) (bool, error)
	Compact(index uint64) (uint64, error)
	Purge() (uint64, error)
	PurgeEx(subject string, seq, keep uint64, noMarkers bool) (uint64, error)
	Truncate(seq uint64) error
	State() StreamState
	FastState(*StreamState)
	Stop() error
	Delete() error
}

type Peer struct {
	ID      string
	Current bool
	Last    time.Time
	Lag     uint64
}

type RaftState uint8

// Allowable states for a NATS Consensus Group.
const (
	Follower RaftState = iota
	Leader
	Candidate
	Closed
)

func (state RaftState) String() string {
	switch state {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	case Closed:
		return "CLOSED"
	}
	return "UNKNOWN"
}

type raft struct {
	sync.RWMutex

	created time.Time      // Time that the group was created
	accName string         // Account name of the asset this raft group is for
	acc     *Account       // Account that NRG traffic will be sent/received in
	group   string         // Raft group
	sd      string         // Store directory
	id      string         // Node ID
	wg      sync.WaitGroup // Wait for running goroutines to exit on shutdown

	wal   WAL         // WAL store (filestore or memstore)
	wtype StorageType // WAL type, e.g. FileStorage or MemoryStorage
	track bool        //
	werr  error       // Last write error

	state       atomic.Int32 // RaftState
	leaderState atomic.Bool  // Is in (complete) leader state.
	hh          hash.Hash64  // Highwayhash, used for snapshots
	snapfile    string       // Snapshot filename

	csz   int             // Cluster size
	qn    int             // Number of nodes needed to establish quorum
	peers map[string]*lps // Other peers in the Raft group

	removed map[string]struct{}            // Peers that were removed from the group
	acks    map[uint64]map[string]struct{} // Append entry responses/acks, map of entry index -> peer ID
	pae     map[uint64]*appendEntry        // Pending append entries

	elect  *time.Timer // Election timer, normally accessed via electTimer
	etlr   time.Time   // Election timer last reset time, for unit tests only
	active time.Time   // Last activity time, i.e. for heartbeats
	llqrt  time.Time   // Last quorum lost time
	lsut   time.Time   // Last scale-up time

	term    uint64 // The current vote term
	pterm   uint64 // Previous term from the last snapshot
	pindex  uint64 // Previous index from the last snapshot
	commit  uint64 // Index of the most recent commit
	applied uint64 // Index of the most recently applied commit

	aflr uint64 // Index when to signal initial messages have been applied after becoming leader. 0 means signaling is disabled.

	leader string // The ID of the leader
	vote   string // Our current vote state
	lxfer  bool   // Are we doing a leadership transfer?

	hcbehind bool // Were we falling behind at the last health check? (see: isCurrent)

	s  *Server    // Reference to top-level server
	c  *client    // Internal client for subscriptions
	js *jetStream // JetStream, if running, to see if we are out of resources

	dflag     bool        // Debug flag
	hasleader atomic.Bool // Is there a group leader right now?
	pleader   atomic.Bool // Has the group ever had a leader?
	isSysAcc  atomic.Bool // Are we utilizing the system account?

	observer bool // The node is observing, i.e. not participating in voting

	extSt extensionState // Extension state

	psubj  string // Proposals subject
	rpsubj string // Remove peers subject
	vsubj  string // Vote requests subject
	vreply string // Vote responses subject
	asubj  string // Append entries subject
	areply string // Append entries responses subject

	sq    *sendq        // Send queue for outbound RPC messages
	aesub *subscription // Subscription for handleAppendEntry callbacks

	wtv []byte // Term and vote to be written
	wps []byte // Peer state to be written

	catchup  *catchupState               // For when we need to catch up as a follower.
	progress map[string]*ipQueue[uint64] // For leader or server catching up a follower.

	paused    bool   // Whether or not applies are paused
	hcommit   uint64 // The commit at the time that applies were paused
	pobserver bool   // Whether we were an observer at the time that applies were paused

	prop  *ipQueue[*proposedEntry]       // Proposals
	entry *ipQueue[*appendEntry]         // Append entries
	resp  *ipQueue[*appendEntryResponse] // Append entries responses
	apply *ipQueue[*CommittedEntry]      // Apply queue (committed entries to be passed to upper layer)
	reqs  *ipQueue[*voteRequest]         // Vote requests
	votes *ipQueue[*voteResponse]        // Vote responses
	leadc chan bool                      // Leader changes
	quit  chan struct{}                  // Raft group shutdown
}

type proposedEntry struct {
	*Entry
	reply string // Optional, to respond once proposal handled
}

// cacthupState structure that holds our subscription, and catchup term and index
// as well as starting term and index and how many updates we have seen.
type catchupState struct {
	sub    *subscription // Subscription that catchup messages will arrive on
	cterm  uint64        // Catchup term
	cindex uint64        // Catchup index
	pterm  uint64        // Starting term
	pindex uint64        // Starting index
	active time.Time     // Last time we received a message for this catchup
}

// lps holds peer state of last time and last index replicated.
type lps struct {
	ts int64  // Last timestamp
	li uint64 // Last index replicated
	kp bool   // Known peer
}

const (
	minElectionTimeoutDefault      = 4 * time.Second
	maxElectionTimeoutDefault      = 9 * time.Second
	minCampaignTimeoutDefault      = 100 * time.Millisecond
	maxCampaignTimeoutDefault      = 8 * minCampaignTimeoutDefault
	hbIntervalDefault              = 1 * time.Second
	lostQuorumIntervalDefault      = hbIntervalDefault * 10 // 10 seconds
	lostQuorumCheckIntervalDefault = hbIntervalDefault * 10 // 10 seconds
	observerModeIntervalDefault    = 48 * time.Hour
)

var (
	minElectionTimeout   = minElectionTimeoutDefault
	maxElectionTimeout   = maxElectionTimeoutDefault
	minCampaignTimeout   = minCampaignTimeoutDefault
	maxCampaignTimeout   = maxCampaignTimeoutDefault
	hbInterval           = hbIntervalDefault
	lostQuorumInterval   = lostQuorumIntervalDefault
	lostQuorumCheck      = lostQuorumCheckIntervalDefault
	observerModeInterval = observerModeIntervalDefault
)

type RaftConfig struct {
	Name     string
	Store    string
	Log      WAL
	Track    bool
	Observer bool
}

var (
	errNotLeader         = errors.New("raft: not leader")
	errAlreadyLeader     = errors.New("raft: already leader")
	errNilCfg            = errors.New("raft: no config given")
	errCorruptPeers      = errors.New("raft: corrupt peer state")
	errEntryLoadFailed   = errors.New("raft: could not load entry from WAL")
	errEntryStoreFailed  = errors.New("raft: could not store entry to WAL")
	errNodeClosed        = errors.New("raft: node is closed")
	errBadSnapName       = errors.New("raft: snapshot name could not be parsed")
	errNoSnapAvailable   = errors.New("raft: no snapshot available")
	errCatchupsRunning   = errors.New("raft: snapshot can not be installed while catchups running")
	errSnapshotCorrupt   = errors.New("raft: snapshot corrupt")
	errTooManyPrefs      = errors.New("raft: stepdown requires at most one preferred new leader")
	errNoPeerState       = errors.New("raft: no peerstate")
	errAdjustBootCluster = errors.New("raft: can not adjust boot peer size on established group")
	errLeaderLen         = fmt.Errorf("raft: leader should be exactly %d bytes", idLen)
	errTooManyEntries    = errors.New("raft: append entry can contain a max of 64k entries")
	errBadAppendEntry    = errors.New("raft: append entry corrupt")
	errNoInternalClient  = errors.New("raft: no internal client")
)

// This will bootstrap a raftNode by writing its config into the store directory.
func (s *Server) bootstrapRaftNode(cfg *RaftConfig, knownPeers []string, allPeersKnown bool) error {
	if cfg == nil {
		return errNilCfg
	}
	// Check validity of peers if presented.
	for _, p := range knownPeers {
		if len(p) != idLen {
			return fmt.Errorf("raft: illegal peer: %q", p)
		}
	}
	expected := len(knownPeers)
	// We need to adjust this is all peers are not known.
	if !allPeersKnown {
		s.Debugf("Determining expected peer size for JetStream meta group")
		if expected < 2 {
			expected = 2
		}
		opts := s.getOpts()
		nrs := len(opts.Routes)

		cn := s.ClusterName()
		ngwps := 0
		for _, gw := range opts.Gateway.Gateways {
			// Ignore our own cluster if specified.
			if gw.Name == cn {
				continue
			}
			for _, u := range gw.URLs {
				host := u.Hostname()
				// If this is an IP just add one.
				if net.ParseIP(host) != nil {
					ngwps++
				} else {
					addrs, _ := net.LookupHost(host)
					ngwps += len(addrs)
				}
			}
		}

		if expected < nrs+ngwps {
			expected = nrs + ngwps
			s.Debugf("Adjusting expected peer set size to %d with %d known", expected, len(knownPeers))
		}
	}

	// Check the store directory. If we have a memory based WAL we need to make sure the directory is setup.
	if stat, err := os.Stat(cfg.Store); os.IsNotExist(err) {
		if err := os.MkdirAll(cfg.Store, defaultDirPerms); err != nil {
			return fmt.Errorf("raft: could not create storage directory - %v", err)
		}
	} else if stat == nil || !stat.IsDir() {
		return fmt.Errorf("raft: storage directory is not a directory")
	}
	tmpfile, err := os.CreateTemp(cfg.Store, "_test_")
	if err != nil {
		return fmt.Errorf("raft: storage directory is not writable")
	}
	tmpfile.Close()
	os.Remove(tmpfile.Name())

	return writePeerState(cfg.Store, &peerState{knownPeers, expected, extUndetermined})
}

// initRaftNode will initialize the raft node, to be used by startRaftNode or when testing to not run the Go routine.
func (s *Server) initRaftNode(accName string, cfg *RaftConfig, labels pprofLabels) (*raft, error) {
	if cfg == nil {
		return nil, errNilCfg
	}
	s.mu.RLock()
	if s.sys == nil {
		s.mu.RUnlock()
		return nil, ErrNoSysAccount
	}
	hash := s.sys.shash
	s.mu.RUnlock()

	// Do this here to process error quicker.
	ps, err := readPeerState(cfg.Store)
	if err != nil {
		return nil, err
	}
	if ps == nil {
		return nil, errNoPeerState
	}

	qpfx := fmt.Sprintf("[ACC:%s] RAFT '%s' ", accName, cfg.Name)
	n := &raft{
		created:  time.Now(),
		id:       hash[:idLen],
		group:    cfg.Name,
		sd:       cfg.Store,
		wal:      cfg.Log,
		wtype:    cfg.Log.Type(),
		track:    cfg.Track,
		csz:      ps.clusterSize,
		qn:       ps.clusterSize/2 + 1,
		peers:    make(map[string]*lps),
		acks:     make(map[uint64]map[string]struct{}),
		pae:      make(map[uint64]*appendEntry),
		s:        s,
		js:       s.getJetStream(),
		quit:     make(chan struct{}),
		reqs:     newIPQueue[*voteRequest](s, qpfx+"vreq"),
		votes:    newIPQueue[*voteResponse](s, qpfx+"vresp"),
		prop:     newIPQueue[*proposedEntry](s, qpfx+"entry"),
		entry:    newIPQueue[*appendEntry](s, qpfx+"appendEntry"),
		resp:     newIPQueue[*appendEntryResponse](s, qpfx+"appendEntryResponse"),
		apply:    newIPQueue[*CommittedEntry](s, qpfx+"committedEntry"),
		accName:  accName,
		leadc:    make(chan bool, 32),
		observer: cfg.Observer,
		extSt:    ps.domainExt,
	}

	// Setup our internal subscriptions for proposals, votes and append entries.
	// If we fail to do this for some reason then this is fatal — we cannot
	// continue setting up or the Raft node may be partially/totally isolated.
	if err := n.RecreateInternalSubs(); err != nil {
		n.shutdown()
		return nil, err
	}

	if atomic.LoadInt32(&s.logging.debug) > 0 {
		n.dflag = true
	}

	// Set up the highwayhash for the snapshots.
	key := sha256.Sum256([]byte(n.group))
	n.hh, _ = highwayhash.New64(key[:])

	// If we have a term and vote file (tav.idx on the filesystem) then read in
	// what we think the term and vote was. It's possible these are out of date
	// so a catch-up may be required.
	if term, vote, err := n.readTermVote(); err == nil && term > 0 {
		n.term = term
		n.vote = vote
	}

	// Can't recover snapshots if memory based since wal will be reset.
	// We will inherit from the current leader.
	if _, ok := n.wal.(*memStore); ok {
		_ = os.RemoveAll(filepath.Join(n.sd, snapshotsDir))
	} else {
		// See if we have any snapshots and if so load and process on startup.
		n.setupLastSnapshot()
	}

	// Make sure that the snapshots directory exists.
	if err := os.MkdirAll(filepath.Join(n.sd, snapshotsDir), defaultDirPerms); err != nil {
		return nil, fmt.Errorf("could not create snapshots directory - %v", err)
	}

	truncateAndErr := func(index uint64) {
		if err := n.wal.Truncate(index); err != nil {
			n.setWriteErr(err)
		}
	}

	// Retrieve the stream state from the WAL. If there are pending append
	// entries that were committed but not applied before we last shut down,
	// we will try to replay them and process them here.
	var state StreamState
	n.wal.FastState(&state)
	if state.Msgs > 0 {
		n.debug("Replaying state of %d entries", state.Msgs)
		if first, err := n.loadFirstEntry(); err == nil {
			n.pterm, n.pindex = first.pterm, first.pindex
			if first.commit > 0 && first.commit > n.commit {
				n.commit = first.commit
			}
		}

		// This process will queue up entries on our applied queue but prior to the upper
		// state machine running. So we will monitor how much we have queued and if we
		// reach a limit will pause the apply queue and resume inside of run() go routine.
		const maxQsz = 32 * 1024 * 1024 // 32MB max

		// It looks like there are entries we have committed but not applied
		// yet. Replay them.
		for index, qsz := state.FirstSeq, 0; index <= state.LastSeq; index++ {
			ae, err := n.loadEntry(index)
			if err != nil {
				n.warn("Could not load %d from WAL [%+v]: %v", index, state, err)
				truncateAndErr(index)
				break
			}
			if ae.pindex != index-1 {
				n.warn("Corrupt WAL, will truncate")
				truncateAndErr(index)
				break
			}
			n.processAppendEntry(ae, nil)
			// Check how much we have queued up so far to determine if we should pause.
			for _, e := range ae.entries {
				qsz += len(e.Data)
				if qsz > maxQsz && !n.paused {
					n.PauseApply()
				}
			}
		}
	}

	// Make sure to track ourselves.
	n.peers[n.id] = &lps{time.Now().UnixNano(), 0, true}

	// Track known peers
	for _, peer := range ps.knownPeers {
		if peer != n.id {
			// Set these to 0 to start but mark as known peer.
			n.peers[peer] = &lps{0, 0, true}
		}
	}

	n.debug("Started")

	// Check if we need to start in observer mode due to lame duck status.
	// This will stop us from taking on the leader role when we're about to
	// shutdown anyway.
	if s.isLameDuckMode() {
		n.debug("Will start in observer mode due to lame duck status")
		n.SetObserver(true)
	}

	// Set the election timer and lost quorum timers to now, so that we
	// won't accidentally trigger either state without knowing the real state
	// of the other nodes.
	n.Lock()
	n.resetElectionTimeout()
	n.llqrt = time.Now()
	n.Unlock()

	// Register the Raft group.
	labels["group"] = n.group
	s.registerRaftNode(n.group, n)

	return n, nil
}

// startRaftNode will start the raft node.
func (s *Server) startRaftNode(accName string, cfg *RaftConfig, labels pprofLabels) (RaftNode, error) {
	n, err := s.initRaftNode(accName, cfg, labels)
	if err != nil {
		return nil, err
	}

	// Start the run goroutine for the Raft state machine.
	n.wg.Add(1)
	s.startGoRoutine(n.run, labels)

	return n, nil
}

// Returns whether peers within this group claim to support
// moving NRG traffic into the asset account.
// Lock must be held.
func (n *raft) checkAccountNRGStatus() bool {
	if !n.s.accountNRGAllowed.Load() {
		return false
	}
	enabled := true
	for pn := range n.peers {
		if si, ok := n.s.nodeToInfo.Load(pn); ok && si != nil {
			enabled = enabled && si.(nodeInfo).accountNRG
		}
	}
	return enabled
}

// Whether we are using the system account or not.
func (n *raft) IsSystemAccount() bool {
	return n.isSysAcc.Load()
}

func (n *raft) RecreateInternalSubs() error {
	n.Lock()
	defer n.Unlock()
	return n.recreateInternalSubsLocked()
}

func (n *raft) recreateInternalSubsLocked() error {
	// Sanity check for system account, as it can disappear when
	// the system is shutting down.
	if n.s == nil {
		return fmt.Errorf("server not found")
	}
	n.s.mu.RLock()
	sys := n.s.sys
	n.s.mu.RUnlock()
	if sys == nil {
		return fmt.Errorf("system account not found")
	}

	// Default is the system account.
	nrgAcc := sys.account
	n.isSysAcc.Store(true)

	// Is account NRG enabled in this account and do all group
	// peers claim to also support account NRG?
	if n.checkAccountNRGStatus() {
		// Check whether the account that the asset belongs to
		// has volunteered a different NRG account.
		target := nrgAcc.Name
		if a, _ := n.s.lookupAccount(n.accName); a != nil {
			a.mu.RLock()
			if a.js != nil {
				target = a.js.nrgAccount
			}
			a.mu.RUnlock()
		}

		// If the target account exists, then we'll use that.
		if target != _EMPTY_ {
			if a, _ := n.s.lookupAccount(target); a != nil {
				nrgAcc = a
				if a != sys.account {
					n.isSysAcc.Store(false)
				}
			}
		}
	}
	if n.aesub != nil && n.acc == nrgAcc {
		// Subscriptions already exist and the account NRG state
		// hasn't changed.
		return nil
	}

	// Need to cancel any in-progress catch-ups, otherwise the
	// inboxes are about to be pulled out from underneath it in
	// the next step...
	n.cancelCatchup()

	// If we have an existing client then tear down any existing
	// subscriptions and close the internal client.
	if c := n.c; c != nil {
		c.mu.Lock()
		subs := make([]*subscription, 0, len(c.subs))
		for _, sub := range c.subs {
			subs = append(subs, sub)
		}
		c.mu.Unlock()
		for _, sub := range subs {
			n.unsubscribe(sub)
		}
		c.closeConnection(InternalClient)
	}

	if n.acc != nrgAcc {
		n.debug("Subscribing in '%s'", nrgAcc.GetName())
	}

	c := n.s.createInternalSystemClient()
	c.registerWithAccount(nrgAcc)
	if nrgAcc.sq == nil {
		nrgAcc.sq = n.s.newSendQ(nrgAcc)
	}
	n.c = c
	n.sq = nrgAcc.sq
	n.acc = nrgAcc

	// Recreate any internal subscriptions for voting, append
	// entries etc in the new account.
	return n.createInternalSubs()
}

// outOfResources checks to see if we are out of resources.
func (n *raft) outOfResources() bool {
	js := n.js
	if !n.track || js == nil {
		return false
	}
	return js.limitsExceeded(n.wtype)
}

// Maps node names back to server names.
func (s *Server) serverNameForNode(node string) string {
	if si, ok := s.nodeToInfo.Load(node); ok && si != nil {
		return si.(nodeInfo).name
	}
	return _EMPTY_
}

// Maps node names back to cluster names.
func (s *Server) clusterNameForNode(node string) string {
	if si, ok := s.nodeToInfo.Load(node); ok && si != nil {
		return si.(nodeInfo).cluster
	}
	return _EMPTY_
}

// Registers the Raft node with the server, as it will track all of the Raft
// nodes.
func (s *Server) registerRaftNode(group string, n RaftNode) {
	s.rnMu.Lock()
	defer s.rnMu.Unlock()
	if s.raftNodes == nil {
		s.raftNodes = make(map[string]RaftNode)
	}
	s.raftNodes[group] = n
}

// Unregisters the Raft node from the server, i.e. at shutdown.
func (s *Server) unregisterRaftNode(group string) {
	s.rnMu.Lock()
	defer s.rnMu.Unlock()
	if s.raftNodes != nil {
		delete(s.raftNodes, group)
	}
}

// Returns how many Raft nodes are running in this server instance.
func (s *Server) numRaftNodes() int {
	s.rnMu.RLock()
	defer s.rnMu.RUnlock()
	return len(s.raftNodes)
}

// Finds the Raft node for a given Raft group, if any. If there is no Raft node
// running for this group then it can return nil.
func (s *Server) lookupRaftNode(group string) RaftNode {
	s.rnMu.RLock()
	defer s.rnMu.RUnlock()
	var n RaftNode
	if s.raftNodes != nil {
		n = s.raftNodes[group]
	}
	return n
}

// Reloads the debug state for all running Raft nodes. This is necessary when
// the configuration has been reloaded and the debug log level has changed.
func (s *Server) reloadDebugRaftNodes(debug bool) {
	if s == nil {
		return
	}
	s.rnMu.RLock()
	for _, ni := range s.raftNodes {
		n := ni.(*raft)
		n.Lock()
		n.dflag = debug
		n.Unlock()
	}
	s.rnMu.RUnlock()
}

// Requests that all Raft nodes on this server step down and place them into
// observer mode. This is called when the server is shutting down.
func (s *Server) stepdownRaftNodes() {
	if s == nil {
		return
	}
	s.rnMu.RLock()
	if len(s.raftNodes) == 0 {
		s.rnMu.RUnlock()
		return
	}
	s.Debugf("Stepping down all leader raft nodes")
	nodes := make([]RaftNode, 0, len(s.raftNodes))
	for _, n := range s.raftNodes {
		nodes = append(nodes, n)
	}
	s.rnMu.RUnlock()

	for _, node := range nodes {
		node.StepDown()
		node.SetObserver(true)
	}
}

// Shuts down all Raft nodes on this server. This is called either when the
// server is either entering lame duck mode, shutting down or when JetStream
// has been disabled.
func (s *Server) shutdownRaftNodes() {
	if s == nil {
		return
	}
	s.rnMu.RLock()
	if len(s.raftNodes) == 0 {
		s.rnMu.RUnlock()
		return
	}
	nodes := make([]RaftNode, 0, len(s.raftNodes))
	s.Debugf("Shutting down all raft nodes")
	for _, n := range s.raftNodes {
		nodes = append(nodes, n)
	}
	s.rnMu.RUnlock()

	for _, node := range nodes {
		node.Stop()
	}
}

// Used in lameduck mode to move off the leaders.
// We also put all nodes in observer mode so new leaders
// can not be placed on this server.
func (s *Server) transferRaftLeaders() bool {
	if s == nil {
		return false
	}
	s.rnMu.RLock()
	if len(s.raftNodes) == 0 {
		s.rnMu.RUnlock()
		return false
	}
	nodes := make([]RaftNode, 0, len(s.raftNodes))
	for _, n := range s.raftNodes {
		nodes = append(nodes, n)
	}
	s.rnMu.RUnlock()

	var didTransfer bool
	for _, node := range nodes {
		if err := node.StepDown(); err == nil {
			didTransfer = true
		}
		node.SetObserver(true)
	}
	return didTransfer
}

// Formal API

// Propose will propose a new entry to the group.
// This should only be called on the leader.
func (n *raft) Propose(data []byte) error {
	if state := n.State(); state != Leader {
		n.debug("Proposal ignored, not leader (state: %v)", state)
		return errNotLeader
	}
	n.Lock()
	defer n.Unlock()

	// Error if we had a previous write error.
	if werr := n.werr; werr != nil {
		return werr
	}
	n.prop.push(newProposedEntry(newEntry(EntryNormal, data), _EMPTY_))
	return nil
}

// ProposeDirect will propose multiple entries at once.
// This should only be called on the leader.
func (n *raft) ProposeMulti(entries []*Entry) error {
	if state := n.State(); state != Leader {
		n.debug("Direct proposal ignored, not leader (state: %v)", state)
		return errNotLeader
	}
	n.Lock()
	defer n.Unlock()

	// Error if we had a previous write error.
	if werr := n.werr; werr != nil {
		return werr
	}
	for _, e := range entries {
		n.prop.push(newProposedEntry(e, _EMPTY_))
	}
	return nil
}

// ForwardProposal will forward the proposal to the leader if known.
// If we are the leader this is the same as calling propose.
func (n *raft) ForwardProposal(entry []byte) error {
	if n.State() == Leader {
		return n.Propose(entry)
	}

	// TODO: Currently we do not set a reply subject, even though we are
	// now capable of responding. Do this once enough time has passed,
	// i.e. maybe in 2.12.
	n.sendRPC(n.psubj, _EMPTY_, entry)
	return nil
}

// ProposeAddPeer is called to add a peer to the group.
func (n *raft) ProposeAddPeer(peer string) error {
	if n.State() != Leader {
		return errNotLeader
	}
	n.RLock()
	// Error if we had a previous write error.
	if werr := n.werr; werr != nil {
		n.RUnlock()
		return werr
	}
	prop := n.prop
	n.RUnlock()

	prop.push(newProposedEntry(newEntry(EntryAddPeer, []byte(peer)), _EMPTY_))
	return nil
}

// As a leader if we are proposing to remove a peer assume its already gone.
func (n *raft) doRemovePeerAsLeader(peer string) {
	n.Lock()
	if n.removed == nil {
		n.removed = map[string]struct{}{}
	}
	n.removed[peer] = struct{}{}
	if _, ok := n.peers[peer]; ok {
		delete(n.peers, peer)
		// We should decrease our cluster size since we are tracking this peer and the peer is most likely already gone.
		n.adjustClusterSizeAndQuorum()
	}
	n.Unlock()
}

// ProposeRemovePeer is called to remove a peer from the group.
func (n *raft) ProposeRemovePeer(peer string) error {
	n.RLock()
	prop, subj := n.prop, n.rpsubj
	isLeader := n.State() == Leader
	werr := n.werr
	n.RUnlock()

	// Error if we had a previous write error.
	if werr != nil {
		return werr
	}

	// If we are the leader then we are responsible for processing the
	// peer remove and then notifying the rest of the group that the
	// peer was removed.
	if isLeader {
		prop.push(newProposedEntry(newEntry(EntryRemovePeer, []byte(peer)), _EMPTY_))
		n.doRemovePeerAsLeader(peer)
		return nil
	}

	// Otherwise we need to forward the proposal to the leader.
	n.sendRPC(subj, _EMPTY_, []byte(peer))
	return nil
}

// ClusterSize reports back the total cluster size.
// This effects quorum etc.
func (n *raft) ClusterSize() int {
	n.Lock()
	defer n.Unlock()
	return n.csz
}

// AdjustBootClusterSize can be called to adjust the boot cluster size.
// Will error if called on a group with a leader or a previous leader.
// This can be helpful in mixed mode.
func (n *raft) AdjustBootClusterSize(csz int) error {
	n.Lock()
	defer n.Unlock()

	if n.leader != noLeader || n.pleader.Load() {
		return errAdjustBootCluster
	}
	// Same floor as bootstrap.
	if csz < 2 {
		csz = 2
	}
	// Adjust the cluster size and the number of nodes needed to establish
	// a quorum.
	n.csz = csz
	n.qn = n.csz/2 + 1

	return nil
}

// AdjustClusterSize will change the cluster set size.
// Must be the leader.
func (n *raft) AdjustClusterSize(csz int) error {
	if n.State() != Leader {
		return errNotLeader
	}
	n.Lock()
	// Same floor as bootstrap.
	if csz < 2 {
		csz = 2
	}

	// Adjust the cluster size and the number of nodes needed to establish
	// a quorum.
	n.csz = csz
	n.qn = n.csz/2 + 1
	n.Unlock()

	n.sendPeerState()
	return nil
}

// PauseApply will allow us to pause processing of append entries onto our
// external apply queue. In effect this means that the upper layer will no longer
// receive any new entries from the Raft group.
func (n *raft) PauseApply() error {
	if n.State() == Leader {
		return errAlreadyLeader
	}

	n.Lock()
	defer n.Unlock()

	// If we are currently a candidate make sure we step down.
	if n.State() == Candidate {
		n.stepdownLocked(noLeader)
	}

	n.debug("Pausing our apply channel")
	n.paused = true
	n.hcommit = n.commit
	// Also prevent us from trying to become a leader while paused and catching up.
	n.pobserver, n.observer = n.observer, true
	n.resetElect(observerModeInterval)

	return nil
}

// ResumeApply will resume sending applies to the external apply queue. This
// means that we will start sending new entries to the upper layer.
func (n *raft) ResumeApply() {
	n.Lock()
	defer n.Unlock()

	if !n.paused {
		return
	}

	n.debug("Resuming our apply channel")

	// Reset before we start.
	n.resetElectionTimeout()

	// Run catchup..
	if n.hcommit > n.commit {
		n.debug("Resuming %d replays", n.hcommit+1-n.commit)
		for index := n.commit + 1; index <= n.hcommit; index++ {
			if err := n.applyCommit(index); err != nil {
				n.warn("Got error on apply commit during replay: %v", err)
				break
			}
			// We want to unlock here to allow the upper layers to call Applied() without blocking.
			n.Unlock()
			// Give hint to let other Go routines run.
			// Might not be necessary but seems to make it more fine grained interleaving.
			runtime.Gosched()
			// Simply re-acquire
			n.Lock()
			// Need to check if we got closed.
			if n.State() == Closed {
				return
			}
		}
	}

	// Clear our observer and paused state after we apply.
	n.observer, n.pobserver = n.pobserver, false
	n.paused = false
	n.hcommit = 0

	// If we had been selected to be the next leader campaign here now that we have resumed.
	if n.lxfer {
		n.xferCampaign()
	} else {
		n.resetElectionTimeout()
	}
}

// Applied is a callback that must be called by the upper layer when it
// has successfully applied the committed entries that it received from the
// apply queue. It will return the number of entries and an estimation of the
// byte size that could be removed with a snapshot/compact.
func (n *raft) Applied(index uint64) (entries uint64, bytes uint64) {
	n.Lock()
	defer n.Unlock()

	// Ignore if not applicable. This can happen during a reset.
	if index > n.commit {
		return 0, 0
	}

	// Ignore if already applied.
	if index > n.applied {
		n.applied = index
	}

	// If it was set, and we reached the minimum applied index, reset and send signal to upper layer.
	if n.aflr > 0 && index >= n.aflr {
		n.aflr = 0
		// Quick sanity-check to confirm we're still leader.
		// In which case we must signal, since switchToLeader would not have done so already.
		if n.State() == Leader {
			n.leaderState.Store(true)
			n.updateLeadChange(true)
		}
	}

	// Calculate the number of entries and estimate the byte size that
	// we can now remove with a compaction/snapshot.
	var state StreamState
	n.wal.FastState(&state)
	if n.applied > state.FirstSeq {
		entries = n.applied - state.FirstSeq
	}
	if state.Msgs > 0 {
		bytes = entries * state.Bytes / state.Msgs
	}
	return entries, bytes
}

// For capturing data needed by snapshot.
type snapshot struct {
	lastTerm  uint64
	lastIndex uint64
	peerstate []byte
	data      []byte
}

const minSnapshotLen = 28

// Encodes a snapshot into a buffer for storage.
// Lock should be held.
func (n *raft) encodeSnapshot(snap *snapshot) []byte {
	if snap == nil {
		return nil
	}
	var le = binary.LittleEndian
	buf := make([]byte, minSnapshotLen+len(snap.peerstate)+len(snap.data))
	le.PutUint64(buf[0:], snap.lastTerm)
	le.PutUint64(buf[8:], snap.lastIndex)
	// Peer state
	le.PutUint32(buf[16:], uint32(len(snap.peerstate)))
	wi := 20
	copy(buf[wi:], snap.peerstate)
	wi += len(snap.peerstate)
	// data itself.
	copy(buf[wi:], snap.data)
	wi += len(snap.data)

	// Now do the hash for the end.
	n.hh.Reset()
	n.hh.Write(buf[:wi])
	checksum := n.hh.Sum(nil)
	copy(buf[wi:], checksum)
	wi += len(checksum)
	return buf[:wi]
}

// SendSnapshot will send the latest snapshot as a normal AE.
// Should only be used when the upper layers know this is most recent.
// Used when restoring streams, moving a stream from R1 to R>1, etc.
func (n *raft) SendSnapshot(data []byte) error {
	n.sendAppendEntry([]*Entry{{EntrySnapshot, data}})
	return nil
}

// Used to install a snapshot for the given term and applied index. This will release
// all of the log entries up to and including index. This should not be called with
// entries that have been applied to the FSM but have not been applied to the raft state.
func (n *raft) InstallSnapshot(data []byte) error {
	if n.State() == Closed {
		return errNodeClosed
	}

	n.Lock()
	defer n.Unlock()

	// If a write error has occurred already then stop here.
	if werr := n.werr; werr != nil {
		return werr
	}

	// Check that a catchup isn't already taking place. If it is then we won't
	// allow installing snapshots until it is done.
	if len(n.progress) > 0 || n.paused {
		return errCatchupsRunning
	}

	if n.applied == 0 {
		n.debug("Not snapshotting as there are no applied entries")
		return errNoSnapAvailable
	}

	term := n.pterm
	if ae, _ := n.loadEntry(n.applied); ae != nil {
		term = ae.term
	}

	n.debug("Installing snapshot of %d bytes", len(data))

	return n.installSnapshot(&snapshot{
		lastTerm:  term,
		lastIndex: n.applied,
		peerstate: encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt}),
		data:      data,
	})
}

// Install the snapshot.
// Lock should be held.
func (n *raft) installSnapshot(snap *snapshot) error {
	snapDir := filepath.Join(n.sd, snapshotsDir)
	sn := fmt.Sprintf(snapFileT, snap.lastTerm, snap.lastIndex)
	sfile := filepath.Join(snapDir, sn)

	if err := writeFileWithSync(sfile, n.encodeSnapshot(snap), defaultFilePerms); err != nil {
		// We could set write err here, but if this is a temporary situation, too many open files etc.
		// we want to retry and snapshots are not fatal.
		return err
	}

	// Delete our previous snapshot file if it exists.
	if n.snapfile != _EMPTY_ && n.snapfile != sfile {
		os.Remove(n.snapfile)
	}
	// Remember our latest snapshot file.
	n.snapfile = sfile
	if _, err := n.wal.Compact(snap.lastIndex + 1); err != nil {
		n.setWriteErrLocked(err)
		return err
	}

	return nil
}

// NeedSnapshot returns true if it is necessary to try to install a snapshot, i.e.
// after we have finished recovering/replaying at startup, on a regular interval or
// as a part of cleaning up when shutting down.
func (n *raft) NeedSnapshot() bool {
	n.RLock()
	defer n.RUnlock()
	return n.snapfile == _EMPTY_ && n.applied > 1
}

const (
	snapshotsDir = "snapshots"
	snapFileT    = "snap.%d.%d"
)

// termAndIndexFromSnapfile tries to load the snapshot file and returns the term
// and index from that snapshot.
func termAndIndexFromSnapFile(sn string) (term, index uint64, err error) {
	if sn == _EMPTY_ {
		return 0, 0, errBadSnapName
	}
	fn := filepath.Base(sn)
	if n, err := fmt.Sscanf(fn, snapFileT, &term, &index); err != nil || n != 2 {
		return 0, 0, errBadSnapName
	}
	return term, index, nil
}

// setupLastSnapshot is called at startup to try and recover the last snapshot from
// the disk if possible. We will try to recover the term, index and commit/applied
// indices and then notify the upper layer what we found. Compacts the WAL if needed.
func (n *raft) setupLastSnapshot() {
	snapDir := filepath.Join(n.sd, snapshotsDir)
	psnaps, err := os.ReadDir(snapDir)
	if err != nil {
		return
	}

	var lterm, lindex uint64
	var latest string
	for _, sf := range psnaps {
		sfile := filepath.Join(snapDir, sf.Name())
		var term, index uint64
		term, index, err := termAndIndexFromSnapFile(sf.Name())
		if err == nil {
			if term > lterm {
				lterm, lindex = term, index
				latest = sfile
			} else if term == lterm && index > lindex {
				lindex = index
				latest = sfile
			}
		} else {
			// Clean this up, can't parse the name.
			// TODO(dlc) - We could read in and check actual contents.
			n.debug("Removing snapshot, can't parse name: %q", sf.Name())
			os.Remove(sfile)
		}
	}

	// Now cleanup any old entries
	for _, sf := range psnaps {
		sfile := filepath.Join(snapDir, sf.Name())
		if sfile != latest {
			n.debug("Removing old snapshot: %q", sfile)
			os.Remove(sfile)
		}
	}

	if latest == _EMPTY_ {
		return
	}

	// Set latest snapshot we have.
	n.Lock()
	defer n.Unlock()

	n.snapfile = latest
	snap, err := n.loadLastSnapshot()
	if err != nil {
		// We failed to recover the last snapshot for some reason, so we will
		// assume it has been corrupted and will try to delete it.
		if n.snapfile != _EMPTY_ {
			os.Remove(n.snapfile)
			n.snapfile = _EMPTY_
		}
		return
	}

	// We successfully recovered the last snapshot from the disk.
	// Recover state from the snapshot and then notify the upper layer.
	// Compact the WAL when we're done if needed.
	n.pindex = snap.lastIndex
	n.pterm = snap.lastTerm
	n.commit = snap.lastIndex
	n.applied = snap.lastIndex
	n.apply.push(newCommittedEntry(n.commit, []*Entry{{EntrySnapshot, snap.data}}))
	if _, err := n.wal.Compact(snap.lastIndex + 1); err != nil {
		n.setWriteErrLocked(err)
	}
}

// loadLastSnapshot will load and return our last snapshot.
// Lock should be held.
func (n *raft) loadLastSnapshot() (*snapshot, error) {
	if n.snapfile == _EMPTY_ {
		return nil, errNoSnapAvailable
	}

	<-dios
	buf, err := os.ReadFile(n.snapfile)
	dios <- struct{}{}

	if err != nil {
		n.warn("Error reading snapshot: %v", err)
		os.Remove(n.snapfile)
		n.snapfile = _EMPTY_
		return nil, err
	}
	if len(buf) < minSnapshotLen {
		n.warn("Snapshot corrupt, too short")
		os.Remove(n.snapfile)
		n.snapfile = _EMPTY_
		return nil, errSnapshotCorrupt
	}

	// Check to make sure hash is consistent.
	hoff := len(buf) - 8
	lchk := buf[hoff:]
	n.hh.Reset()
	n.hh.Write(buf[:hoff])
	if !bytes.Equal(lchk[:], n.hh.Sum(nil)) {
		n.warn("Snapshot corrupt, checksums did not match")
		os.Remove(n.snapfile)
		n.snapfile = _EMPTY_
		return nil, errSnapshotCorrupt
	}

	var le = binary.LittleEndian
	lps := le.Uint32(buf[16:])
	snap := &snapshot{
		lastTerm:  le.Uint64(buf[0:]),
		lastIndex: le.Uint64(buf[8:]),
		peerstate: buf[20 : 20+lps],
		data:      buf[20+lps : hoff],
	}

	// We had a bug in 2.9.12 that would allow snapshots on last index of 0.
	// Detect that here and return err.
	if snap.lastIndex == 0 {
		n.warn("Snapshot with last index 0 is invalid, cleaning up")
		os.Remove(n.snapfile)
		n.snapfile = _EMPTY_
		return nil, errSnapshotCorrupt
	}

	return snap, nil
}

// Leader returns if we are the leader for our group.
// We use an atomic here now vs acquiring the read lock.
func (n *raft) Leader() bool {
	if n == nil {
		return false
	}
	return n.leaderState.Load()
}

// stepdown immediately steps down the Raft node to the
// follower state. This will take the lock itself.
func (n *raft) stepdown(newLeader string) {
	n.Lock()
	defer n.Unlock()
	n.stepdownLocked(newLeader)
}

// stepdownLocked immediately steps down the Raft node to the
// follower state. This requires the lock is already held.
func (n *raft) stepdownLocked(newLeader string) {
	n.debug("Stepping down")
	n.switchToFollowerLocked(newLeader)
}

// isCatchingUp returns true if a catchup is currently taking place.
func (n *raft) isCatchingUp() bool {
	n.RLock()
	defer n.RUnlock()
	return n.catchup != nil
}

// isCurrent is called from the healthchecks and returns true if we believe
// that the upper layer is current with the Raft layer, i.e. that it has applied
// all of the commits that we have given it.
// Optionally we can also check whether or not we're making forward progress if we
// aren't current, in which case this function may block for up to ~10ms to find out.
// Lock should be held.
func (n *raft) isCurrent(includeForwardProgress bool) bool {
	// Check if we are closed.
	if n.State() == Closed {
		n.debug("Not current, node is closed")
		return false
	}

	// Check whether we've made progress on any state, 0 is invalid so not healthy.
	if n.commit == 0 {
		n.debug("Not current, no commits")
		return false
	}

	// If we were previously logging about falling behind, also log when the problem
	// was cleared.
	clearBehindState := func() {
		if n.hcbehind {
			n.warn("Health check OK, no longer falling behind")
			n.hcbehind = false
		}
	}

	// Make sure we are the leader or we know we have heard from the leader recently.
	if n.State() == Leader {
		clearBehindState()
		return true
	}

	// Check to see that we have heard from the current leader lately.
	if n.leader != noLeader && n.leader != n.id && n.catchup == nil {
		okInterval := int64(hbInterval) * 2
		ts := time.Now().UnixNano()
		if ps := n.peers[n.leader]; ps == nil || ps.ts == 0 && (ts-ps.ts) > okInterval {
			n.debug("Not current, no recent leader contact")
			return false
		}
	}
	if cs := n.catchup; cs != nil {
		// We're actively catching up, can't mark current even if commit==applied.
		n.debug("Not current, still catching up pindex=%d, cindex=%d", n.pindex, cs.cindex)
		return false
	}

	if n.paused && n.hcommit > n.commit {
		// We're currently paused, waiting to be resumed to apply pending commits.
		n.debug("Not current, waiting to resume applies commit=%d, hcommit=%d", n.commit, n.hcommit)
		return false
	}

	if n.commit == n.applied {
		// At this point if we are current, we can return saying so.
		clearBehindState()
		return true
	} else if !includeForwardProgress {
		// Otherwise, if we aren't allowed to include forward progress
		// (i.e. we are checking "current" instead of "healthy") then
		// give up now.
		return false
	}

	// Otherwise, wait for a short period of time and see if we are making any
	// forward progress.
	if startDelta := n.commit - n.applied; startDelta > 0 {
		for i := 0; i < 10; i++ { // 10ms, in 1ms increments
			n.Unlock()
			time.Sleep(time.Millisecond)
			n.Lock()
			if n.commit-n.applied < startDelta {
				// The gap is getting smaller, so we're making forward progress.
				clearBehindState()
				return true
			}
		}
	}

	n.hcbehind = true
	n.warn("Falling behind in health check, commit %d != applied %d", n.commit, n.applied)
	return false
}

// Current returns if we are the leader for our group or an up to date follower.
func (n *raft) Current() bool {
	if n == nil {
		return false
	}
	n.Lock()
	defer n.Unlock()
	return n.isCurrent(false)
}

// Healthy returns if we are the leader for our group and nearly up-to-date.
func (n *raft) Healthy() bool {
	if n == nil {
		return false
	}
	n.Lock()
	defer n.Unlock()
	return n.isCurrent(true)
}

// HadPreviousLeader indicates if this group ever had a leader.
func (n *raft) HadPreviousLeader() bool {
	return n.pleader.Load()
}

// GroupLeader returns the current leader of the group.
func (n *raft) GroupLeader() string {
	if n == nil {
		return noLeader
	}
	n.RLock()
	defer n.RUnlock()
	return n.leader
}

// Leaderless is a lockless way of finding out if the group has a
// leader or not. Use instead of GroupLeader in hot paths.
func (n *raft) Leaderless() bool {
	if n == nil {
		return true
	}
	// Negated because we want the default state of hasLeader to be
	// false until the first setLeader() call.
	return !n.hasleader.Load()
}

// Guess the best next leader. Stepdown will check more thoroughly.
// Lock should be held.
func (n *raft) selectNextLeader() string {
	nextLeader, hli := noLeader, uint64(0)
	for peer, ps := range n.peers {
		if peer == n.id || ps.li <= hli {
			continue
		}
		hli = ps.li
		nextLeader = peer
	}
	return nextLeader
}

// StepDown will have a leader stepdown and optionally do a leader transfer.
func (n *raft) StepDown(preferred ...string) error {
	if n.State() != Leader {
		return errNotLeader
	}

	n.Lock()
	if len(preferred) > 1 {
		n.Unlock()
		return errTooManyPrefs
	}

	n.debug("Being asked to stepdown")

	// See if we have up to date followers.
	maybeLeader := noLeader
	if len(preferred) > 0 {
		if preferred[0] != _EMPTY_ {
			maybeLeader = preferred[0]
		} else {
			preferred = nil
		}
	}

	// Can't pick ourselves.
	if maybeLeader == n.id {
		maybeLeader = noLeader
		preferred = nil
	}

	nowts := time.Now().UnixNano()

	// If we have a preferred check it first.
	if maybeLeader != noLeader {
		var isHealthy bool
		if ps, ok := n.peers[maybeLeader]; ok {
			si, ok := n.s.nodeToInfo.Load(maybeLeader)
			isHealthy = ok && !si.(nodeInfo).offline && (nowts-ps.ts) < int64(hbInterval*3)
		}
		if !isHealthy {
			maybeLeader = noLeader
		}
	}

	// If we do not have a preferred at this point pick the first healthy one.
	// Make sure not ourselves.
	if maybeLeader == noLeader {
		for peer, ps := range n.peers {
			if peer == n.id {
				continue
			}
			si, ok := n.s.nodeToInfo.Load(peer)
			isHealthy := ok && !si.(nodeInfo).offline && (nowts-ps.ts) < int64(hbInterval*3)
			if isHealthy {
				maybeLeader = peer
				break
			}
		}
	}

	// Clear our vote state.
	n.vote = noVote
	n.writeTermVote()

	n.Unlock()

	if len(preferred) > 0 && maybeLeader == noLeader {
		n.debug("Can not transfer to preferred peer %q", preferred[0])
	}

	// If we have a new leader selected, transfer over to them.
	// Send the append entry directly rather than via the proposals queue,
	// as we will switch to follower state immediately and will blow away
	// the contents of the proposal queue in the process.
	if maybeLeader != noLeader {
		n.debug("Selected %q for new leader, stepping down due to leadership transfer", maybeLeader)
		ae := newEntry(EntryLeaderTransfer, []byte(maybeLeader))
		n.sendAppendEntry([]*Entry{ae})
	}

	// Force us to stepdown here.
	n.stepdown(noLeader)

	return nil
}

// Campaign will have our node start a leadership vote.
func (n *raft) Campaign() error {
	n.Lock()
	defer n.Unlock()
	return n.campaign()
}

func randCampaignTimeout() time.Duration {
	delta := rand.Int63n(int64(maxCampaignTimeout - minCampaignTimeout))
	return (minCampaignTimeout + time.Duration(delta))
}

// Campaign will have our node start a leadership vote.
// Lock should be held.
func (n *raft) campaign() error {
	n.debug("Starting campaign")
	if n.State() == Leader {
		return errAlreadyLeader
	}
	n.resetElect(randCampaignTimeout())
	return nil
}

// xferCampaign will have our node start an immediate leadership vote.
// Lock should be held.
func (n *raft) xferCampaign() error {
	n.debug("Starting transfer campaign")
	if n.State() == Leader {
		n.lxfer = false
		return errAlreadyLeader
	}
	n.resetElect(10 * time.Millisecond)
	return nil
}

// State returns the current state for this node.
// Upper layers should not check State to check if we're Leader, use n.Leader() instead.
func (n *raft) State() RaftState {
	return RaftState(n.state.Load())
}

// Progress returns the current index, commit and applied values.
func (n *raft) Progress() (index, commit, applied uint64) {
	n.RLock()
	defer n.RUnlock()
	return n.pindex, n.commit, n.applied
}

// Size returns number of entries and total bytes for our WAL.
func (n *raft) Size() (uint64, uint64) {
	n.RLock()
	var state StreamState
	n.wal.FastState(&state)
	n.RUnlock()
	return state.Msgs, state.Bytes
}

func (n *raft) ID() string {
	if n == nil {
		return _EMPTY_
	}
	// Lock not needed as n.id is never changed after creation.
	return n.id
}

func (n *raft) Group() string {
	// Lock not needed as n.group is never changed after creation.
	return n.group
}

func (n *raft) Peers() []*Peer {
	n.RLock()
	defer n.RUnlock()

	var peers []*Peer
	for id, ps := range n.peers {
		var lag uint64
		if n.commit > ps.li {
			lag = n.commit - ps.li
		}
		p := &Peer{
			ID:      id,
			Current: id == n.leader || ps.li >= n.applied,
			Last:    time.Unix(0, ps.ts),
			Lag:     lag,
		}
		peers = append(peers, p)
	}
	return peers
}

// Update and propose our known set of peers.
func (n *raft) ProposeKnownPeers(knownPeers []string) {
	// If we are the leader update and send this update out.
	if n.State() != Leader {
		return
	}
	n.UpdateKnownPeers(knownPeers)
	n.sendPeerState()
}

// Update our known set of peers.
func (n *raft) UpdateKnownPeers(knownPeers []string) {
	n.Lock()
	// Process like peer state update.
	ps := &peerState{knownPeers, len(knownPeers), n.extSt}
	n.processPeerState(ps)
	n.Unlock()
}

// ApplyQ returns the apply queue that new commits will be sent to for the
// upper layer to apply.
func (n *raft) ApplyQ() *ipQueue[*CommittedEntry] { return n.apply }

// LeadChangeC returns the leader change channel, notifying when the Raft
// leader role has moved.
func (n *raft) LeadChangeC() <-chan bool { return n.leadc }

// QuitC returns the quit channel, notifying when the Raft group has shut down.
func (n *raft) QuitC() <-chan struct{} { return n.quit }

func (n *raft) Created() time.Time {
	// Lock not needed as n.created is never changed after creation.
	return n.created
}

func (n *raft) Stop() {
	n.shutdown()
}

func (n *raft) WaitForStop() {
	if n.state.Load() == int32(Closed) {
		n.wg.Wait()
	}
}

func (n *raft) Delete() {
	n.shutdown()
	n.wg.Wait()

	n.Lock()
	defer n.Unlock()

	if wal := n.wal; wal != nil {
		wal.Delete()
	}
	os.RemoveAll(n.sd)
	n.debug("Deleted")
}

func (n *raft) shutdown() {
	// First call to Stop or Delete should close the quit chan
	// to notify the runAs goroutines to stop what they're doing.
	if n.state.Swap(int32(Closed)) != int32(Closed) {
		n.leaderState.Store(false)
		close(n.quit)
	}
}

const (
	raftAllSubj        = "$NRG.>"
	raftVoteSubj       = "$NRG.V.%s"
	raftAppendSubj     = "$NRG.AE.%s"
	raftPropSubj       = "$NRG.P.%s"
	raftRemovePeerSubj = "$NRG.RP.%s"
	raftReply          = "$NRG.R.%s"
	raftCatchupReply   = "$NRG.CR.%s"
)

// Lock should be held (due to use of random generator)
func (n *raft) newCatchupInbox() string {
	var b [replySuffixLen]byte
	rn := fastrand.Uint64()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	return fmt.Sprintf(raftCatchupReply, b[:])
}

func (n *raft) newInbox() string {
	var b [replySuffixLen]byte
	rn := fastrand.Uint64()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	return fmt.Sprintf(raftReply, b[:])
}

// Our internal subscribe.
// Lock should be held.
func (n *raft) subscribe(subject string, cb msgHandler) (*subscription, error) {
	if n.c == nil {
		return nil, errNoInternalClient
	}
	return n.s.systemSubscribe(subject, _EMPTY_, false, n.c, cb)
}

// Lock should be held.
func (n *raft) unsubscribe(sub *subscription) {
	if n.c != nil && sub != nil {
		n.c.processUnsub(sub.sid)
	}
}

// Lock should be held.
func (n *raft) createInternalSubs() error {
	n.vsubj, n.vreply = fmt.Sprintf(raftVoteSubj, n.group), n.newInbox()
	n.asubj, n.areply = fmt.Sprintf(raftAppendSubj, n.group), n.newInbox()
	n.psubj = fmt.Sprintf(raftPropSubj, n.group)
	n.rpsubj = fmt.Sprintf(raftRemovePeerSubj, n.group)

	// Votes
	if _, err := n.subscribe(n.vreply, n.handleVoteResponse); err != nil {
		return err
	}
	if _, err := n.subscribe(n.vsubj, n.handleVoteRequest); err != nil {
		return err
	}
	// AppendEntry
	if _, err := n.subscribe(n.areply, n.handleAppendEntryResponse); err != nil {
		return err
	}
	if sub, err := n.subscribe(n.asubj, n.handleAppendEntry); err != nil {
		return err
	} else {
		n.aesub = sub
	}

	return nil
}

func randElectionTimeout() time.Duration {
	delta := rand.Int63n(int64(maxElectionTimeout - minElectionTimeout))
	return (minElectionTimeout + time.Duration(delta))
}

// Lock should be held.
func (n *raft) resetElectionTimeout() {
	n.resetElect(randElectionTimeout())
}

func (n *raft) resetElectionTimeoutWithLock() {
	n.resetElectWithLock(randElectionTimeout())
}

// Lock should be held.
func (n *raft) resetElect(et time.Duration) {
	n.etlr = time.Now()
	if n.elect == nil {
		n.elect = time.NewTimer(et)
	} else {
		if !n.elect.Stop() {
			select {
			case <-n.elect.C:
			default:
			}
		}
		n.elect.Reset(et)
	}
}

func (n *raft) resetElectWithLock(et time.Duration) {
	n.Lock()
	n.resetElect(et)
	n.Unlock()
}

// run is the top-level runner for the Raft state machine. Depending on the
// state of the node (leader, follower, candidate, observer), this will call
// through to other functions. It is expected that this function will run for
// the entire life of the Raft node once started.
func (n *raft) run() {
	s := n.s
	defer s.grWG.Done()
	defer n.wg.Done()

	// We want to wait for some routing to be enabled, so we will wait for
	// at least a route, leaf or gateway connection to be established before
	// starting the run loop.
	for gw := s.gateway; ; {
		s.mu.RLock()
		ready, gwEnabled := s.numRemotes()+len(s.leafs) > 0, gw.enabled
		s.mu.RUnlock()
		if !ready && gwEnabled {
			gw.RLock()
			ready = len(gw.out)+len(gw.in) > 0
			gw.RUnlock()
		}
		if !ready {
			select {
			case <-s.quitCh:
				return
			case <-time.After(100 * time.Millisecond):
				s.RateLimitWarnf("Waiting for routing to be established...")
			}
		} else {
			break
		}
	}

	// We may have paused adding entries to apply queue, resume here.
	// No-op if not paused.
	n.ResumeApply()

	// Send nil entry to signal the upper layers we are done doing replay/restore.
	n.apply.push(nil)

runner:
	for s.isRunning() {
		switch n.State() {
		case Follower:
			n.runAsFollower()
		case Candidate:
			n.runAsCandidate()
		case Leader:
			n.runAsLeader()
		case Closed:
			break runner
		}
	}

	// If we've reached this point then we're shutting down, either because
	// the server is stopping or because the Raft group is closing/closed.
	n.Lock()
	defer n.Unlock()

	if c := n.c; c != nil {
		var subs []*subscription
		c.mu.Lock()
		for _, sub := range c.subs {
			subs = append(subs, sub)
		}
		c.mu.Unlock()
		for _, sub := range subs {
			n.unsubscribe(sub)
		}
		c.closeConnection(InternalClient)
		n.c = nil
	}

	// Unregistering ipQueues do not prevent them from push/pop
	// just will remove them from the central monitoring map
	queues := []interface {
		unregister()
		drain() int
	}{n.reqs, n.votes, n.prop, n.entry, n.resp, n.apply}
	for _, q := range queues {
		q.drain()
		q.unregister()
	}

	n.s.unregisterRaftNode(n.group)

	if wal := n.wal; wal != nil {
		wal.Stop()
	}

	n.debug("Shutdown")
}

func (n *raft) debug(format string, args ...any) {
	if n.dflag {
		nf := fmt.Sprintf("RAFT [%s - %s] %s", n.id, n.group, format)
		n.s.Debugf(nf, args...)
	}
}

func (n *raft) warn(format string, args ...any) {
	nf := fmt.Sprintf("RAFT [%s - %s] %s", n.id, n.group, format)
	n.s.RateLimitWarnf(nf, args...)
}

func (n *raft) error(format string, args ...any) {
	nf := fmt.Sprintf("RAFT [%s - %s] %s", n.id, n.group, format)
	n.s.Errorf(nf, args...)
}

func (n *raft) electTimer() *time.Timer {
	n.RLock()
	defer n.RUnlock()
	return n.elect
}

func (n *raft) IsObserver() bool {
	n.RLock()
	defer n.RUnlock()
	return n.observer
}

// Sets the state to observer only.
func (n *raft) SetObserver(isObserver bool) {
	n.setObserver(isObserver, extUndetermined)
}

func (n *raft) setObserver(isObserver bool, extSt extensionState) {
	n.Lock()
	defer n.Unlock()

	if n.paused {
		// Applies are paused so we're already in observer state.
		// Resuming the applies will set the state back to whatever
		// is in "pobserver", so update that instead.
		n.pobserver = isObserver
		return
	}

	wasObserver := n.observer
	n.observer = isObserver
	n.extSt = extSt

	// If we're leaving observer state then reset the election timer or
	// we might end up waiting for up to the observerModeInterval.
	if wasObserver && !isObserver {
		n.resetElect(randCampaignTimeout())
	}
}

// processAppendEntries is called by the Raft state machine when there are
// new append entries to be committed and sent to the upper state machine.
func (n *raft) processAppendEntries() {
	canProcess := true
	if n.isClosed() {
		n.debug("AppendEntry not processing inbound, closed")
		canProcess = false
	}
	if n.outOfResources() {
		n.debug("AppendEntry not processing inbound, no resources")
		canProcess = false
	}
	// Always pop the entries, but check if we can process them. If we can't
	// then the entries are effectively dropped.
	aes := n.entry.pop()
	if canProcess {
		for _, ae := range aes {
			n.processAppendEntry(ae, ae.sub)
		}
	}
	n.entry.recycle(&aes)
}

// runAsFollower is called by run and will block for as long as the node is
// running in the follower state.
func (n *raft) runAsFollower() {
	for n.State() == Follower {
		elect := n.electTimer()

		select {
		case <-n.entry.ch:
			// New append entries have arrived over the network.
			n.processAppendEntries()
		case <-n.s.quitCh:
			// The server is shutting down.
			return
		case <-n.quit:
			// The Raft node is shutting down.
			return
		case <-elect.C:
			// The election timer has fired so we think it's time to call an election.
			// If we are out of resources we just want to stay in this state for the moment.
			if n.outOfResources() {
				n.resetElectionTimeoutWithLock()
				n.debug("Not switching to candidate, no resources")
			} else if n.IsObserver() {
				n.resetElectWithLock(observerModeInterval)
				n.debug("Not switching to candidate, observer only")
			} else if n.isCatchingUp() {
				n.debug("Not switching to candidate, catching up")
				// Check to see if our catchup has stalled.
				n.Lock()
				if n.catchupStalled() {
					n.cancelCatchup()
				}
				n.resetElectionTimeout()
				n.Unlock()
			} else {
				n.switchToCandidate()
				return
			}
		case <-n.votes.ch:
			// We're receiving votes from the network, probably because we have only
			// just stepped down and they were already in flight. Ignore them.
			n.debug("Ignoring old vote response, we have stepped down")
			n.votes.popOne()
		case <-n.resp.ch:
			// Ignore append entry responses received from before the state change.
			n.resp.drain()
		case <-n.prop.ch:
			// Ignore proposals received from before the state change.
			n.prop.drain()
		case <-n.reqs.ch:
			// We've just received a vote request from the network.
			// Because of drain() it is possible that we get nil from popOne().
			if voteReq, ok := n.reqs.popOne(); ok {
				n.processVoteRequest(voteReq)
			}
		}
	}
}

// Pool for CommittedEntry re-use.
var cePool = sync.Pool{
	New: func() any {
		return &CommittedEntry{}
	},
}

// CommittedEntry is handed back to the user to apply a commit to their upper layer.
type CommittedEntry struct {
	Index   uint64
	Entries []*Entry
}

// Create a new CommittedEntry. When the returned entry is no longer needed, it
// should be returned to the pool by calling ReturnToPool.
func newCommittedEntry(index uint64, entries []*Entry) *CommittedEntry {
	ce := cePool.Get().(*CommittedEntry)
	ce.Index, ce.Entries = index, entries
	return ce
}

// ReturnToPool returns the CommittedEntry to the pool, after which point it is
// no longer safe to reuse.
func (ce *CommittedEntry) ReturnToPool() {
	if ce == nil {
		return
	}
	if len(ce.Entries) > 0 {
		for _, e := range ce.Entries {
			entryPool.Put(e)
		}
	}
	ce.Index, ce.Entries = 0, nil
	cePool.Put(ce)
}

// Pool for Entry re-use.
var entryPool = sync.Pool{
	New: func() any {
		return &Entry{}
	},
}

// Helper to create new entries. When the returned entry is no longer needed, it
// should be returned to the entryPool pool.
func newEntry(t EntryType, data []byte) *Entry {
	entry := entryPool.Get().(*Entry)
	entry.Type, entry.Data = t, data
	return entry
}

// Pool for appendEntry re-use.
var aePool = sync.Pool{
	New: func() any {
		return &appendEntry{}
	},
}

// appendEntry is the main struct that is used to sync raft peers.
type appendEntry struct {
	leader  string   // The leader that this append entry came from.
	term    uint64   // The current term, as the leader understands it.
	commit  uint64   // The commit index, as the leader understands it.
	pterm   uint64   // The previous term, for checking consistency.
	pindex  uint64   // The previous commit index, for checking consistency.
	entries []*Entry // Entries to process.
	// Below fields are for internal use only:
	reply string        // Reply subject to respond to once committed.
	sub   *subscription // The subscription that the append entry came in on.
	buf   []byte
}

// Create a new appendEntry.
func newAppendEntry(leader string, term, commit, pterm, pindex uint64, entries []*Entry) *appendEntry {
	ae := aePool.Get().(*appendEntry)
	ae.leader, ae.term, ae.commit, ae.pterm, ae.pindex, ae.entries = leader, term, commit, pterm, pindex, entries
	ae.reply, ae.sub, ae.buf = _EMPTY_, nil, nil
	return ae
}

// Will return this append entry, and its interior entries to their respective pools.
func (ae *appendEntry) returnToPool() {
	ae.entries, ae.buf, ae.sub, ae.reply = nil, nil, nil, _EMPTY_
	aePool.Put(ae)
}

// Pool for proposedEntry re-use.
var pePool = sync.Pool{
	New: func() any {
		return &proposedEntry{}
	},
}

// Create a new proposedEntry.
func newProposedEntry(entry *Entry, reply string) *proposedEntry {
	pe := pePool.Get().(*proposedEntry)
	pe.Entry, pe.reply = entry, reply
	return pe
}

// Will return this proosed entry.
func (pe *proposedEntry) returnToPool() {
	pe.Entry, pe.reply = nil, _EMPTY_
	pePool.Put(pe)
}

type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryOldSnapshot
	EntryPeerState
	EntryAddPeer
	EntryRemovePeer
	EntryLeaderTransfer
	EntrySnapshot
)

func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "Normal"
	case EntryOldSnapshot:
		return "OldSnapshot"
	case EntryPeerState:
		return "PeerState"
	case EntryAddPeer:
		return "AddPeer"
	case EntryRemovePeer:
		return "RemovePeer"
	case EntryLeaderTransfer:
		return "LeaderTransfer"
	case EntrySnapshot:
		return "Snapshot"
	}
	return fmt.Sprintf("Unknown [%d]", uint8(t))
}

type Entry struct {
	Type EntryType
	Data []byte
}

func (ae *appendEntry) String() string {
	return fmt.Sprintf("&{leader:%s term:%d commit:%d pterm:%d pindex:%d entries: %d}",
		ae.leader, ae.term, ae.commit, ae.pterm, ae.pindex, len(ae.entries))
}

const appendEntryBaseLen = idLen + 4*8 + 2

func (ae *appendEntry) encode(b []byte) ([]byte, error) {
	if ll := len(ae.leader); ll != idLen && ll != 0 {
		return nil, errLeaderLen
	}
	if len(ae.entries) > math.MaxUint16 {
		return nil, errTooManyEntries
	}

	var elen int
	for _, e := range ae.entries {
		elen += len(e.Data) + 1 + 4 // 1 is type, 4 is for size.
	}
	tlen := appendEntryBaseLen + elen + 1

	var buf []byte
	if cap(b) >= tlen {
		buf = b[:tlen]
	} else {
		buf = make([]byte, tlen)
	}

	var le = binary.LittleEndian
	copy(buf[:idLen], ae.leader)
	le.PutUint64(buf[8:], ae.term)
	le.PutUint64(buf[16:], ae.commit)
	le.PutUint64(buf[24:], ae.pterm)
	le.PutUint64(buf[32:], ae.pindex)
	le.PutUint16(buf[40:], uint16(len(ae.entries)))
	wi := 42
	for _, e := range ae.entries {
		le.PutUint32(buf[wi:], uint32(len(e.Data)+1))
		wi += 4
		buf[wi] = byte(e.Type)
		wi++
		copy(buf[wi:], e.Data)
		wi += len(e.Data)
	}
	return buf[:wi], nil
}

// This can not be used post the wire level callback since we do not copy.
func (n *raft) decodeAppendEntry(msg []byte, sub *subscription, reply string) (*appendEntry, error) {
	if len(msg) < appendEntryBaseLen {
		return nil, errBadAppendEntry
	}

	var le = binary.LittleEndian

	ae := newAppendEntry(string(msg[:idLen]), le.Uint64(msg[8:]), le.Uint64(msg[16:]), le.Uint64(msg[24:]), le.Uint64(msg[32:]), nil)
	ae.reply, ae.sub = reply, sub

	// Decode Entries.
	ne, ri := int(le.Uint16(msg[40:])), 42
	for i, max := 0, len(msg); i < ne; i++ {
		if ri >= max-1 {
			return nil, errBadAppendEntry
		}
		le := int(le.Uint32(msg[ri:]))
		ri += 4
		if le <= 0 || ri+le > max {
			return nil, errBadAppendEntry
		}
		entry := newEntry(EntryType(msg[ri]), msg[ri+1:ri+le])
		ae.entries = append(ae.entries, entry)
		ri += le
	}
	ae.buf = msg
	return ae, nil
}

// Pool for appendEntryResponse re-use.
var arPool = sync.Pool{
	New: func() any {
		return &appendEntryResponse{}
	},
}

// We want to make sure this does not change from system changing length of syshash.
const idLen = 8
const appendEntryResponseLen = 24 + 1

// appendEntryResponse is our response to a received appendEntry.
type appendEntryResponse struct {
	term    uint64
	index   uint64
	peer    string
	reply   string // internal usage.
	success bool
}

// Create a new appendEntryResponse.
func newAppendEntryResponse(term, index uint64, peer string, success bool) *appendEntryResponse {
	ar := arPool.Get().(*appendEntryResponse)
	ar.term, ar.index, ar.peer, ar.success = term, index, peer, success
	// Always empty out.
	ar.reply = _EMPTY_
	return ar
}

func (ar *appendEntryResponse) encode(b []byte) []byte {
	var buf []byte
	if cap(b) >= appendEntryResponseLen {
		buf = b[:appendEntryResponseLen]
	} else {
		buf = make([]byte, appendEntryResponseLen)
	}
	var le = binary.LittleEndian
	le.PutUint64(buf[0:], ar.term)
	le.PutUint64(buf[8:], ar.index)
	copy(buf[16:16+idLen], ar.peer)
	if ar.success {
		buf[24] = 1
	} else {
		buf[24] = 0
	}
	return buf[:appendEntryResponseLen]
}

// Track all peers we may have ever seen to use an string interns for appendEntryResponse decoding.
var peers sync.Map

func (n *raft) decodeAppendEntryResponse(msg []byte) *appendEntryResponse {
	if len(msg) != appendEntryResponseLen {
		return nil
	}
	var le = binary.LittleEndian
	ar := arPool.Get().(*appendEntryResponse)
	ar.term = le.Uint64(msg[0:])
	ar.index = le.Uint64(msg[8:])

	peer, ok := peers.Load(string(msg[16 : 16+idLen]))
	if !ok {
		// We missed so store inline here.
		peer = string(msg[16 : 16+idLen])
		peers.Store(peer, peer)
	}
	ar.peer = peer.(string)
	ar.success = msg[24] == 1
	return ar
}

// Called when a remove peer proposal has been forwarded
func (n *raft) handleForwardedRemovePeerProposal(sub *subscription, c *client, _ *Account, _, reply string, msg []byte) {
	n.debug("Received forwarded remove peer proposal: %q", msg)

	if n.State() != Leader {
		n.debug("Ignoring forwarded peer removal proposal, not leader")
		return
	}
	if len(msg) != idLen {
		n.warn("Received invalid peer name for remove proposal: %q", msg)
		return
	}

	n.RLock()
	prop, werr := n.prop, n.werr
	n.RUnlock()

	// Ignore if we have had a write error previous.
	if werr != nil {
		return
	}

	// Need to copy since this is underlying client/route buffer.
	peer := copyBytes(msg)
	prop.push(newProposedEntry(newEntry(EntryRemovePeer, peer), reply))
}

// Called when a peer has forwarded a proposal.
func (n *raft) handleForwardedProposal(sub *subscription, c *client, _ *Account, _, reply string, msg []byte) {
	if n.State() != Leader {
		n.debug("Ignoring forwarded proposal, not leader")
		return
	}
	// Need to copy since this is underlying client/route buffer.
	msg = copyBytes(msg)

	n.RLock()
	prop, werr := n.prop, n.werr
	n.RUnlock()

	// Ignore if we have had a write error previous.
	if werr != nil {
		return
	}

	prop.push(newProposedEntry(newEntry(EntryNormal, msg), reply))
}

func (n *raft) runAsLeader() {
	if n.State() == Closed {
		return
	}

	n.Lock()
	psubj, rpsubj := n.psubj, n.rpsubj

	// For forwarded proposals, both normal and remove peer proposals.
	fsub, err := n.subscribe(psubj, n.handleForwardedProposal)
	if err != nil {
		n.warn("Error subscribing to forwarded proposals: %v", err)
		n.stepdownLocked(noLeader)
		n.Unlock()
		return
	}
	rpsub, err := n.subscribe(rpsubj, n.handleForwardedRemovePeerProposal)
	if err != nil {
		n.warn("Error subscribing to forwarded remove peer proposals: %v", err)
		n.unsubscribe(fsub)
		n.stepdownLocked(noLeader)
		n.Unlock()
		return
	}
	n.Unlock()

	// Cleanup our subscription when we leave.
	defer func() {
		n.Lock()
		n.unsubscribe(fsub)
		n.unsubscribe(rpsub)
		n.Unlock()
	}()

	// To send out our initial peer state.
	n.sendPeerState()

	hb := time.NewTicker(hbInterval)
	defer hb.Stop()

	lq := time.NewTicker(lostQuorumCheck)
	defer lq.Stop()

	for n.State() == Leader {
		select {
		case <-n.s.quitCh:
			return
		case <-n.quit:
			return
		case <-n.resp.ch:
			ars := n.resp.pop()
			for _, ar := range ars {
				n.processAppendEntryResponse(ar)
			}
			n.resp.recycle(&ars)
		case <-n.prop.ch:
			const maxBatch = 256 * 1024
			const maxEntries = 512
			var entries []*Entry

			es, sz := n.prop.pop(), 0
			for _, b := range es {
				if b.Type == EntryRemovePeer {
					n.doRemovePeerAsLeader(string(b.Data))
				}
				entries = append(entries, b.Entry)
				// Increment size.
				sz += len(b.Data) + 1
				// If below thresholds go ahead and send.
				if sz < maxBatch && len(entries) < maxEntries {
					continue
				}
				n.sendAppendEntry(entries)
				// Reset our sz and entries.
				// We need to re-create `entries` because there is a reference
				// to it in the node's pae map.
				sz, entries = 0, nil
			}
			if len(entries) > 0 {
				n.sendAppendEntry(entries)
			}
			// Respond to any proposals waiting for a confirmation.
			for _, pe := range es {
				if pe.reply != _EMPTY_ {
					n.sendReply(pe.reply, nil)
				}
				pe.returnToPool()
			}
			n.prop.recycle(&es)

		case <-hb.C:
			if n.notActive() {
				n.sendHeartbeat()
			}
		case <-lq.C:
			if n.lostQuorum() {
				n.stepdown(noLeader)
				return
			}
		case <-n.votes.ch:
			// Because of drain() it is possible that we get nil from popOne().
			vresp, ok := n.votes.popOne()
			if !ok {
				continue
			}
			if vresp.term > n.Term() {
				n.stepdown(noLeader)
				return
			}
			n.trackPeer(vresp.peer)
		case <-n.reqs.ch:
			// Because of drain() it is possible that we get nil from popOne().
			if voteReq, ok := n.reqs.popOne(); ok {
				n.processVoteRequest(voteReq)
			}
		case <-n.entry.ch:
			n.processAppendEntries()
		}
	}
}

// Quorum reports the quorum status. Will be called on former leaders.
func (n *raft) Quorum() bool {
	n.RLock()
	defer n.RUnlock()

	now, nc := time.Now().UnixNano(), 0
	for id, peer := range n.peers {
		if id == n.id || time.Duration(now-peer.ts) < lostQuorumInterval {
			nc++
			if nc >= n.qn {
				return true
			}
		}
	}
	return false
}

func (n *raft) lostQuorum() bool {
	n.RLock()
	defer n.RUnlock()
	return n.lostQuorumLocked()
}

func (n *raft) lostQuorumLocked() bool {
	// In order to avoid false positives that can happen in heavily loaded systems
	// make sure nothing is queued up that we have not processed yet.
	// Also make sure we let any scale up actions settle before deciding.
	if n.resp.len() != 0 || (!n.lsut.IsZero() && time.Since(n.lsut) < lostQuorumInterval) {
		return false
	}

	now, nc := time.Now().UnixNano(), 0
	for id, peer := range n.peers {
		if id == n.id || time.Duration(now-peer.ts) < lostQuorumInterval {
			nc++
			if nc >= n.qn {
				return false
			}
		}
	}
	return true
}

// Check for being not active in terms of sending entries.
// Used in determining if we need to send a heartbeat.
func (n *raft) notActive() bool {
	n.RLock()
	defer n.RUnlock()
	return time.Since(n.active) > hbInterval
}

// Return our current term.
func (n *raft) Term() uint64 {
	n.RLock()
	defer n.RUnlock()
	return n.term
}

// Lock should be held.
func (n *raft) loadFirstEntry() (ae *appendEntry, err error) {
	var state StreamState
	n.wal.FastState(&state)
	return n.loadEntry(state.FirstSeq)
}

func (n *raft) runCatchup(ar *appendEntryResponse, indexUpdatesQ *ipQueue[uint64]) {
	n.RLock()
	s, reply := n.s, n.areply
	peer, subj, last := ar.peer, ar.reply, n.pindex
	n.RUnlock()

	defer s.grWG.Done()
	defer arPool.Put(ar)

	defer func() {
		n.Lock()
		delete(n.progress, peer)
		if len(n.progress) == 0 {
			n.progress = nil
		}
		// Check if this is a new peer and if so go ahead and propose adding them.
		_, exists := n.peers[peer]
		n.Unlock()
		if !exists {
			n.debug("Catchup done for %q, will add into peers", peer)
			n.ProposeAddPeer(peer)
		}
		indexUpdatesQ.unregister()
	}()

	n.debug("Running catchup for %q", peer)

	const maxOutstanding = 2 * 1024 * 1024 // 2MB for now.
	next, total, om := uint64(0), 0, make(map[uint64]int)

	sendNext := func() bool {
		for total <= maxOutstanding {
			next++
			if next > last {
				return true
			}
			ae, err := n.loadEntry(next)
			if err != nil {
				if err != ErrStoreEOF {
					n.warn("Got an error loading %d index: %v", next, err)
				}
				return true
			}
			// Update our tracking total.
			om[next] = len(ae.buf)
			total += len(ae.buf)
			n.sendRPC(subj, reply, ae.buf)
		}
		return false
	}

	const activityInterval = 2 * time.Second
	timeout := time.NewTimer(activityInterval)
	defer timeout.Stop()

	stepCheck := time.NewTicker(100 * time.Millisecond)
	defer stepCheck.Stop()

	// Run as long as we are leader and still not caught up.
	for n.State() == Leader {
		select {
		case <-n.s.quitCh:
			return
		case <-n.quit:
			return
		case <-stepCheck.C:
			if n.State() != Leader {
				n.debug("Catching up canceled, no longer leader")
				return
			}
		case <-timeout.C:
			n.debug("Catching up for %q stalled", peer)
			return
		case <-indexUpdatesQ.ch:
			if index, ok := indexUpdatesQ.popOne(); ok {
				// Update our activity timer.
				timeout.Reset(activityInterval)
				// Update outstanding total.
				total -= om[index]
				delete(om, index)
				if next == 0 {
					next = index
				}
				// Check if we are done.
				if index > last || sendNext() {
					n.debug("Finished catching up")
					return
				}
			}
		}
	}
}

// Lock should be held.
func (n *raft) sendSnapshotToFollower(subject string) (uint64, error) {
	snap, err := n.loadLastSnapshot()
	if err != nil {
		// We need to stepdown here when this happens.
		n.stepdownLocked(noLeader)
		// We need to reset our state here as well.
		n.resetWAL()
		return 0, err
	}
	// Go ahead and send the snapshot and peerstate here as first append entry to the catchup follower.
	ae := n.buildAppendEntry([]*Entry{{EntrySnapshot, snap.data}, {EntryPeerState, snap.peerstate}})
	ae.pterm, ae.pindex = snap.lastTerm, snap.lastIndex
	var state StreamState
	n.wal.FastState(&state)

	fpIndex := state.FirstSeq - 1
	if snap.lastIndex < fpIndex && state.FirstSeq != 0 {
		snap.lastIndex = fpIndex
		ae.pindex = fpIndex
	}

	encoding, err := ae.encode(nil)
	if err != nil {
		return 0, err
	}
	n.sendRPC(subject, n.areply, encoding)
	return snap.lastIndex, nil
}

func (n *raft) catchupFollower(ar *appendEntryResponse) {
	n.debug("Being asked to catch up follower: %q", ar.peer)
	n.Lock()
	if n.progress == nil {
		n.progress = make(map[string]*ipQueue[uint64])
	} else if q, ok := n.progress[ar.peer]; ok {
		n.debug("Will cancel existing entry for catching up %q", ar.peer)
		delete(n.progress, ar.peer)
		q.push(n.pindex)
	}

	// Check to make sure we have this entry.
	start := ar.index + 1
	var state StreamState
	n.wal.FastState(&state)

	if start < state.FirstSeq || (state.Msgs == 0 && start <= state.LastSeq) {
		n.debug("Need to send snapshot to follower")
		if lastIndex, err := n.sendSnapshotToFollower(ar.reply); err != nil {
			n.error("Error sending snapshot to follower [%s]: %v", ar.peer, err)
			n.Unlock()
			arPool.Put(ar)
			return
		} else {
			start = lastIndex + 1
			// If no other entries, we can just return here.
			if state.Msgs == 0 || start > state.LastSeq {
				n.debug("Finished catching up")
				n.Unlock()
				arPool.Put(ar)
				return
			}
			n.debug("Snapshot sent, reset first catchup entry to %d", lastIndex)
		}
	}

	ae, err := n.loadEntry(start)
	if err != nil {
		n.warn("Request from follower for entry at index [%d] errored for state %+v - %v", start, state, err)
		if err == ErrStoreEOF {
			// If we are here we are seeing a request for an item beyond our state, meaning we should stepdown.
			n.stepdownLocked(noLeader)
			n.Unlock()
			arPool.Put(ar)
			return
		}
		ae, err = n.loadFirstEntry()
	}
	if err != nil || ae == nil {
		n.warn("Could not find a starting entry for catchup request: %v", err)
		// If we are here we are seeing a request for an item we do not have, meaning we should stepdown.
		// This is possible on a reset of our WAL but the other side has a snapshot already.
		// If we do not stepdown this can cycle.
		n.stepdownLocked(noLeader)
		n.Unlock()
		arPool.Put(ar)
		return
	}
	if ae.pindex != ar.index || ae.pterm != ar.term {
		n.debug("Our first entry [%d:%d] does not match request from follower [%d:%d]", ae.pterm, ae.pindex, ar.term, ar.index)
	}
	// Create a queue for delivering updates from responses.
	indexUpdates := newIPQueue[uint64](n.s, fmt.Sprintf("[ACC:%s] RAFT '%s' indexUpdates", n.accName, n.group))
	indexUpdates.push(ae.pindex)
	n.progress[ar.peer] = indexUpdates
	n.Unlock()

	n.wg.Add(1)
	n.s.startGoRoutine(func() {
		defer n.wg.Done()
		n.runCatchup(ar, indexUpdates)
	})
}

func (n *raft) loadEntry(index uint64) (*appendEntry, error) {
	var smp StoreMsg
	sm, err := n.wal.LoadMsg(index, &smp)
	if err != nil {
		return nil, err
	}
	return n.decodeAppendEntry(sm.msg, nil, _EMPTY_)
}

// applyCommit will update our commit index and apply the entry to the apply queue.
// lock should be held.
func (n *raft) applyCommit(index uint64) error {
	if n.State() == Closed {
		return errNodeClosed
	}
	if index <= n.commit {
		n.debug("Ignoring apply commit for %d, already processed", index)
		return nil
	}

	if n.State() == Leader {
		delete(n.acks, index)
	}

	ae := n.pae[index]
	if ae == nil {
		var state StreamState
		n.wal.FastState(&state)
		if index < state.FirstSeq {
			return nil
		}
		var err error
		if ae, err = n.loadEntry(index); err != nil {
			if err != ErrStoreClosed && err != ErrStoreEOF {
				n.warn("Got an error loading %d index: %v - will reset", index, err)
				if n.State() == Leader {
					n.stepdownLocked(n.selectNextLeader())
				}
				// Reset and cancel any catchup.
				n.resetWAL()
				n.cancelCatchup()
			}
			return errEntryLoadFailed
		}
	} else {
		defer delete(n.pae, index)
	}

	n.commit = index
	ae.buf = nil

	var committed []*Entry
	for _, e := range ae.entries {
		switch e.Type {
		case EntryNormal:
			committed = append(committed, e)
		case EntryOldSnapshot:
			// For old snapshots in our WAL.
			committed = append(committed, newEntry(EntrySnapshot, e.Data))
		case EntrySnapshot:
			committed = append(committed, e)
		case EntryPeerState:
			if n.State() != Leader {
				if ps, err := decodePeerState(e.Data); err == nil {
					n.processPeerState(ps)
				}
			}
		case EntryAddPeer:
			newPeer := string(e.Data)
			n.debug("Added peer %q", newPeer)

			// Store our peer in our global peer map for all peers.
			peers.LoadOrStore(newPeer, newPeer)

			// If we were on the removed list reverse that here.
			if n.removed != nil {
				delete(n.removed, newPeer)
			}

			if lp, ok := n.peers[newPeer]; !ok {
				// We are not tracking this one automatically so we need to bump cluster size.
				n.peers[newPeer] = &lps{time.Now().UnixNano(), 0, true}
			} else {
				// Mark as added.
				lp.kp = true
			}
			// Adjust cluster size and quorum if needed.
			n.adjustClusterSizeAndQuorum()
			// Write out our new state.
			n.writePeerState(&peerState{n.peerNames(), n.csz, n.extSt})
			// We pass these up as well.
			committed = append(committed, e)

		case EntryRemovePeer:
			peer := string(e.Data)
			n.debug("Removing peer %q", peer)

			// Make sure we have our removed map.
			if n.removed == nil {
				n.removed = make(map[string]struct{})
			}
			n.removed[peer] = struct{}{}

			if _, ok := n.peers[peer]; ok {
				delete(n.peers, peer)
				// We should decrease our cluster size since we are tracking this peer.
				n.adjustClusterSizeAndQuorum()
				// Write out our new state.
				n.writePeerState(&peerState{n.peerNames(), n.csz, n.extSt})
			}

			// If this is us and we are the leader we should attempt to stepdown.
			if peer == n.id && n.State() == Leader {
				n.stepdownLocked(n.selectNextLeader())
			}

			// Remove from string intern map.
			peers.Delete(peer)

			// We pass these up as well.
			committed = append(committed, e)
		}
	}
	// Pass to the upper layers if we have normal entries. It is
	// entirely possible that 'committed' might be an empty slice here,
	// which will happen if we've processed updates inline (like peer
	// states). In which case the upper layer will just call down with
	// Applied() with no further action.
	n.apply.push(newCommittedEntry(index, committed))
	// Place back in the pool.
	ae.returnToPool()
	return nil
}

// Used to track a success response and apply entries.
func (n *raft) trackResponse(ar *appendEntryResponse) {
	if n.State() == Closed {
		return
	}

	n.Lock()

	// Update peer's last index.
	if ps := n.peers[ar.peer]; ps != nil && ar.index > ps.li {
		ps.li = ar.index
	}

	// If we are tracking this peer as a catchup follower, update that here.
	if indexUpdateQ := n.progress[ar.peer]; indexUpdateQ != nil {
		indexUpdateQ.push(ar.index)
	}

	// Ignore items already committed.
	if ar.index <= n.commit {
		n.Unlock()
		return
	}

	// See if we have items to apply.
	var sendHB bool

	results := n.acks[ar.index]
	if results == nil {
		results = make(map[string]struct{})
		n.acks[ar.index] = results
	}
	results[ar.peer] = struct{}{}

	// We don't count ourselves to account for leader changes, so add 1.
	if nr := len(results); nr+1 >= n.qn {
		// We have a quorum.
		for index := n.commit + 1; index <= ar.index; index++ {
			if err := n.applyCommit(index); err != nil && err != errNodeClosed {
				n.error("Got an error applying commit for %d: %v", index, err)
				break
			}
		}
		sendHB = n.prop.len() == 0
	}
	n.Unlock()

	if sendHB {
		n.sendHeartbeat()
	}
}

// Used to adjust cluster size and peer count based on added official peers.
// lock should be held.
func (n *raft) adjustClusterSizeAndQuorum() {
	pcsz, ncsz := n.csz, 0
	for _, peer := range n.peers {
		if peer.kp {
			ncsz++
		}
	}
	n.csz = ncsz
	n.qn = n.csz/2 + 1

	if ncsz > pcsz {
		n.debug("Expanding our clustersize: %d -> %d", pcsz, ncsz)
		n.lsut = time.Now()
	} else if ncsz < pcsz {
		n.debug("Decreasing our clustersize: %d -> %d", pcsz, ncsz)
		if n.State() == Leader {
			go n.sendHeartbeat()
		}
	}
	if ncsz != pcsz {
		n.recreateInternalSubsLocked()
	}
}

// Track interactions with this peer.
func (n *raft) trackPeer(peer string) error {
	n.Lock()
	var needPeerAdd, isRemoved bool
	if n.removed != nil {
		_, isRemoved = n.removed[peer]
	}
	if n.State() == Leader {
		if lp, ok := n.peers[peer]; !ok || !lp.kp {
			// Check if this peer had been removed previously.
			needPeerAdd = !isRemoved
		}
	}
	if ps := n.peers[peer]; ps != nil {
		ps.ts = time.Now().UnixNano()
	} else if !isRemoved {
		n.peers[peer] = &lps{time.Now().UnixNano(), 0, false}
	}
	n.Unlock()

	if needPeerAdd {
		n.ProposeAddPeer(peer)
	}
	return nil
}

func (n *raft) runAsCandidate() {
	n.Lock()
	// Drain old responses.
	n.votes.drain()
	n.Unlock()

	// Send out our request for votes.
	n.requestVote()

	// We vote for ourselves.
	votes := map[string]struct{}{
		n.ID(): {},
	}

	for n.State() == Candidate {
		elect := n.electTimer()
		select {
		case <-n.entry.ch:
			n.processAppendEntries()
		case <-n.resp.ch:
			// Ignore append entry responses received from before the state change.
			n.resp.drain()
		case <-n.prop.ch:
			// Ignore proposals received from before the state change.
			n.prop.drain()
		case <-n.s.quitCh:
			return
		case <-n.quit:
			return
		case <-elect.C:
			n.switchToCandidate()
			return
		case <-n.votes.ch:
			// Because of drain() it is possible that we get nil from popOne().
			vresp, ok := n.votes.popOne()
			if !ok {
				continue
			}
			n.RLock()
			nterm := n.term
			n.RUnlock()

			if vresp.granted && nterm == vresp.term {
				// only track peers that would be our followers
				n.trackPeer(vresp.peer)
				votes[vresp.peer] = struct{}{}
				if n.wonElection(len(votes)) {
					// Become LEADER if we have won and gotten a quorum with everyone we should hear from.
					n.switchToLeader()
					return
				}
			} else if vresp.term > nterm {
				// if we observe a bigger term, we should start over again or risk forming a quorum fully knowing
				// someone with a better term exists. This is even the right thing to do if won == true.
				n.Lock()
				n.debug("Stepping down from candidate, detected higher term: %d vs %d", vresp.term, n.term)
				n.term = vresp.term
				n.vote = noVote
				n.writeTermVote()
				n.lxfer = false
				n.stepdownLocked(noLeader)
				n.Unlock()
			}
		case <-n.reqs.ch:
			// Because of drain() it is possible that we get nil from popOne().
			if voteReq, ok := n.reqs.popOne(); ok {
				n.processVoteRequest(voteReq)
			}
		}
	}
}

// handleAppendEntry handles an append entry from the wire. This function
// is an internal callback from the "asubj" append entry subscription.
func (n *raft) handleAppendEntry(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
	msg = copyBytes(msg)
	if ae, err := n.decodeAppendEntry(msg, sub, reply); err == nil {
		// Push to the new entry channel. From here one of the worker
		// goroutines (runAsLeader, runAsFollower, runAsCandidate) will
		// pick it up.
		n.entry.push(ae)
	} else {
		n.warn("AppendEntry failed to be placed on internal channel: corrupt entry")
	}
}

// cancelCatchup will stop an in-flight catchup by unsubscribing from the
// catchup subscription.
// Lock should be held.
func (n *raft) cancelCatchup() {
	n.debug("Canceling catchup subscription since we are now up to date")

	if n.catchup != nil && n.catchup.sub != nil {
		n.unsubscribe(n.catchup.sub)
	}
	n.catchup = nil
}

// catchupStalled will try to determine if we are stalled. This is called
// on a new entry from our leader.
// Lock should be held.
func (n *raft) catchupStalled() bool {
	if n.catchup == nil {
		return false
	}
	if n.catchup.pindex == n.pindex {
		return time.Since(n.catchup.active) > 2*time.Second
	}
	n.catchup.pindex = n.pindex
	n.catchup.active = time.Now()
	return false
}

// createCatchup will create the state needed to track a catchup as it
// runs. It then creates a unique inbox for this catchup and subscribes
// to it. The remote side will stream entries to that subject.
// Lock should be held.
func (n *raft) createCatchup(ae *appendEntry) string {
	// Cleanup any old ones.
	if n.catchup != nil && n.catchup.sub != nil {
		n.unsubscribe(n.catchup.sub)
	}
	// Snapshot term and index.
	n.catchup = &catchupState{
		cterm:  ae.pterm,
		cindex: ae.pindex,
		pterm:  n.pterm,
		pindex: n.pindex,
		active: time.Now(),
	}
	inbox := n.newCatchupInbox()
	sub, _ := n.subscribe(inbox, n.handleAppendEntry)
	n.catchup.sub = sub

	return inbox
}

// Truncate our WAL and reset.
// Lock should be held.
func (n *raft) truncateWAL(term, index uint64) {
	n.debug("Truncating and repairing WAL to Term %d Index %d", term, index)

	if term == 0 && index == 0 {
		n.warn("Resetting WAL state")
	}

	defer func() {
		// Check to see if we invalidated any snapshots that might have held state
		// from the entries we are truncating.
		if snap, _ := n.loadLastSnapshot(); snap != nil && snap.lastIndex > index {
			os.Remove(n.snapfile)
			n.snapfile = _EMPTY_
		}
		// Make sure to reset commit and applied if above
		if n.commit > n.pindex {
			n.commit = n.pindex
		}
		if n.applied > n.commit {
			n.applied = n.commit
		}
	}()

	if err := n.wal.Truncate(index); err != nil {
		// If we get an invalid sequence, reset our wal all together.
		// We will not have holes, so this means we do not have this message stored anymore.
		if err == ErrInvalidSequence {
			n.debug("Resetting WAL")
			n.wal.Truncate(0)
			// If our index is non-zero use PurgeEx to set us to the correct next index.
			if index > 0 {
				n.wal.PurgeEx(fwcs, index+1, 0, true)
			}
		} else {
			n.warn("Error truncating WAL: %v", err)
			n.setWriteErrLocked(err)
			return
		}
	}
	// Set after we know we have truncated properly.
	n.pterm, n.pindex = term, index
}

// Reset our WAL. This is equivalent to truncating all data from the log.
// Lock should be held.
func (n *raft) resetWAL() {
	n.truncateWAL(0, 0)
}

// Lock should be held
func (n *raft) updateLeader(newLeader string) {
	n.leader = newLeader
	n.hasleader.Store(newLeader != _EMPTY_)
	if !n.pleader.Load() && newLeader != noLeader {
		n.pleader.Store(true)
	}
}

// processAppendEntry will process an appendEntry. This is called either
// during recovery or from processAppendEntries when there are new entries
// to be committed.
func (n *raft) processAppendEntry(ae *appendEntry, sub *subscription) {
	n.Lock()
	// Don't reset here if we have been asked to assume leader position.
	if !n.lxfer {
		n.resetElectionTimeout()
	}

	// Just return if closed or we had previous write error.
	if n.State() == Closed || n.werr != nil {
		n.Unlock()
		return
	}

	// Scratch buffer for responses.
	var scratch [appendEntryResponseLen]byte
	arbuf := scratch[:]

	// Are we receiving from another leader.
	if n.State() == Leader {
		// If we are the same we should step down to break the tie.
		if ae.term >= n.term {
			// If the append entry term is newer than the current term, erase our
			// vote.
			if ae.term > n.term {
				n.term = ae.term
				n.vote = noVote
				n.writeTermVote()
			}
			n.debug("Received append entry from another leader, stepping down to %q", ae.leader)
			n.stepdownLocked(ae.leader)
		} else {
			// Let them know we are the leader.
			ar := newAppendEntryResponse(n.term, n.pindex, n.id, false)
			n.debug("AppendEntry ignoring old term from another leader")
			n.sendRPC(ae.reply, _EMPTY_, ar.encode(arbuf))
			arPool.Put(ar)
		}
		// Always return here from processing.
		n.Unlock()
		return
	}

	// If we received an append entry as a candidate then it would appear that
	// another node has taken on the leader role already, so we should convert
	// to a follower of that node instead.
	if n.State() == Candidate {
		// If we have a leader in the current term or higher, we should stepdown,
		// write the term and vote if the term of the request is higher.
		if ae.term >= n.term {
			// If the append entry term is newer than the current term, erase our
			// vote.
			if ae.term > n.term {
				n.term = ae.term
				n.vote = noVote
				n.writeTermVote()
			}
			n.debug("Received append entry in candidate state from %q, converting to follower", ae.leader)
			n.stepdownLocked(ae.leader)
		}
	}

	// Catching up state.
	catchingUp := n.catchup != nil
	// Is this a new entry? New entries will be delivered on the append entry
	// sub, rather than a catch-up sub.
	isNew := sub != nil && sub == n.aesub

	// Track leader directly
	if isNew && ae.leader != noLeader {
		if ps := n.peers[ae.leader]; ps != nil {
			ps.ts = time.Now().UnixNano()
		} else {
			n.peers[ae.leader] = &lps{time.Now().UnixNano(), 0, true}
		}
	}

	// If we are catching up ignore old catchup subs.
	// This could happen when we stall or cancel a catchup.
	if !isNew && catchingUp && sub != n.catchup.sub {
		n.Unlock()
		n.debug("AppendEntry ignoring old entry from previous catchup")
		return
	}

	// Check state if we are catching up.
	if catchingUp {
		if cs := n.catchup; cs != nil && n.pterm >= cs.cterm && n.pindex >= cs.cindex {
			// If we are here we are good, so if we have a catchup pending we can cancel.
			n.cancelCatchup()
			// Reset our notion of catching up.
			catchingUp = false
		} else if isNew {
			var ar *appendEntryResponse
			var inbox string
			// Check to see if we are stalled. If so recreate our catchup state and resend response.
			if n.catchupStalled() {
				n.debug("Catchup may be stalled, will request again")
				inbox = n.createCatchup(ae)
				ar = newAppendEntryResponse(n.pterm, n.pindex, n.id, false)
			}
			n.Unlock()
			if ar != nil {
				n.sendRPC(ae.reply, inbox, ar.encode(arbuf))
				arPool.Put(ar)
			}
			// Ignore new while catching up or replaying.
			return
		}
	}

	// If this term is greater than ours.
	if ae.term > n.term {
		n.term = ae.term
		n.vote = noVote
		if isNew {
			n.writeTermVote()
		}
		if n.State() != Follower {
			n.debug("Term higher than ours and we are not a follower: %v, stepping down to %q", n.State(), ae.leader)
			n.stepdownLocked(ae.leader)
		}
	} else if ae.term < n.term && !catchingUp && isNew {
		n.debug("Rejected AppendEntry from a leader (%s) with term %d which is less than ours", ae.leader, ae.term)
		ar := newAppendEntryResponse(n.term, n.pindex, n.id, false)
		n.Unlock()
		n.sendRPC(ae.reply, _EMPTY_, ar.encode(arbuf))
		arPool.Put(ar)
		return
	}

	if isNew && n.leader != ae.leader && n.State() == Follower {
		n.debug("AppendEntry updating leader to %q", ae.leader)
		n.updateLeader(ae.leader)
		n.writeTermVote()
		n.resetElectionTimeout()
		n.updateLeadChange(false)
	}

	if ae.pterm != n.pterm || ae.pindex != n.pindex {
		// Check if this is a lower or equal index than what we were expecting.
		if ae.pindex <= n.pindex {
			n.debug("AppendEntry detected pindex less than/equal to ours: %d:%d vs %d:%d", ae.pterm, ae.pindex, n.pterm, n.pindex)
			var ar *appendEntryResponse
			var success bool

			if ae.pindex < n.commit {
				// If we have already committed this entry, just mark success.
				success = true
			} else if eae, _ := n.loadEntry(ae.pindex); eae == nil {
				// If terms are equal, and we are not catching up, we have simply already processed this message.
				// So we will ACK back to the leader. This can happen on server restarts based on timings of snapshots.
				if ae.pterm == n.pterm && !catchingUp {
					success = true
				} else if ae.pindex == n.pindex {
					// Check if only our terms do not match here.
					// Make sure pterms match and we take on the leader's.
					// This prevents constant spinning.
					n.truncateWAL(ae.pterm, ae.pindex)
				} else {
					n.resetWAL()
				}
			} else if eae.term == ae.pterm {
				// If terms match we can delete all entries past this one, and then continue storing the current entry.
				n.truncateWAL(ae.pterm, ae.pindex)
				// Only continue if truncation was successful, and we ended up such that we can safely continue.
				if ae.pterm == n.pterm && ae.pindex == n.pindex {
					goto CONTINUE
				}
			} else {
				// If terms mismatched, delete that entry and all others past it.
				// Make sure to cancel any catchups in progress.
				// Truncate will reset our pterm and pindex. Only do so if we have an entry.
				n.truncateWAL(eae.pterm, eae.pindex)
			}
			// Cancel regardless if unsuccessful.
			if !success {
				n.cancelCatchup()
			}

			// Create response.
			ar = newAppendEntryResponse(ae.pterm, ae.pindex, n.id, success)
			n.Unlock()
			n.sendRPC(ae.reply, _EMPTY_, ar.encode(arbuf))
			arPool.Put(ar)
			return
		}

		// Check if we are catching up. If we are here we know the leader did not have all of the entries
		// so make sure this is a snapshot entry. If it is not start the catchup process again since it
		// means we may have missed additional messages.
		if catchingUp {
			// This means we already entered into a catchup state but what the leader sent us did not match what we expected.
			// Snapshots and peerstate will always be together when a leader is catching us up in this fashion.
			if len(ae.entries) != 2 || ae.entries[0].Type != EntrySnapshot || ae.entries[1].Type != EntryPeerState {
				n.warn("Expected first catchup entry to be a snapshot and peerstate, will retry")
				n.cancelCatchup()
				n.Unlock()
				return
			}

			if ps, err := decodePeerState(ae.entries[1].Data); err == nil {
				n.processPeerState(ps)
				// Also need to copy from client's buffer.
				ae.entries[0].Data = copyBytes(ae.entries[0].Data)
			} else {
				n.warn("Could not parse snapshot peerstate correctly")
				n.cancelCatchup()
				n.Unlock()
				return
			}

			// Inherit state from appendEntry with the leader's snapshot.
			n.pindex = ae.pindex
			n.pterm = ae.pterm
			n.commit = ae.pindex

			if _, err := n.wal.Compact(n.pindex + 1); err != nil {
				n.setWriteErrLocked(err)
				n.Unlock()
				return
			}

			snap := &snapshot{
				lastTerm:  n.pterm,
				lastIndex: n.pindex,
				peerstate: encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt}),
				data:      ae.entries[0].Data,
			}
			// Install the leader's snapshot as our own.
			if err := n.installSnapshot(snap); err != nil {
				n.setWriteErrLocked(err)
				n.Unlock()
				return
			}

			// Now send snapshot to upper levels. Only send the snapshot, not the peerstate entry.
			n.apply.push(newCommittedEntry(n.commit, ae.entries[:1]))
			n.Unlock()
			return
		}

		// Setup our state for catching up.
		n.debug("AppendEntry did not match %d %d with %d %d", ae.pterm, ae.pindex, n.pterm, n.pindex)
		inbox := n.createCatchup(ae)
		ar := newAppendEntryResponse(n.pterm, n.pindex, n.id, false)
		n.Unlock()
		n.sendRPC(ae.reply, inbox, ar.encode(arbuf))
		arPool.Put(ar)
		return
	}

CONTINUE:
	// Save to our WAL if we have entries.
	if ae.shouldStore() {
		// Only store if an original which will have sub != nil
		if sub != nil {
			if err := n.storeToWAL(ae); err != nil {
				if err != ErrStoreClosed {
					n.warn("Error storing entry to WAL: %v", err)
				}
				n.Unlock()
				return
			}
			// Save in memory for faster processing during applyCommit.
			// Only save so many however to avoid memory bloat.
			if l := len(n.pae); l <= paeDropThreshold {
				n.pae[n.pindex], l = ae, l+1
				if l > paeWarnThreshold && l%paeWarnModulo == 0 {
					n.warn("%d append entries pending", len(n.pae))
				}
			} else {
				// Invalidate cache entry at this index, we might have
				// stored it previously with a different value.
				delete(n.pae, n.pindex)
				if l%paeWarnModulo == 0 {
					n.debug("Not saving to append entries pending")
				}
			}
		} else {
			// This is a replay on startup so just take the appendEntry version.
			n.pterm = ae.term
			n.pindex = ae.pindex + 1
		}
	}

	// Check to see if we have any related entries to process here.
	for _, e := range ae.entries {
		switch e.Type {
		case EntryLeaderTransfer:
			// Only process these if they are new, so no replays or catchups.
			if isNew {
				maybeLeader := string(e.Data)
				// This is us. We need to check if we can become the leader.
				if maybeLeader == n.id {
					// If not an observer and not paused we are good to go.
					if !n.observer && !n.paused {
						n.lxfer = true
						n.xferCampaign()
					} else if n.paused && !n.pobserver {
						// Here we can become a leader but need to wait for resume of the apply queue.
						n.lxfer = true
					}
				} else if n.vote != noVote {
					// Since we are here we are not the chosen one but we should clear any vote preference.
					n.vote = noVote
					n.writeTermVote()
				}
			}
		case EntryAddPeer:
			if newPeer := string(e.Data); len(newPeer) == idLen {
				// Track directly, but wait for commit to be official
				if ps := n.peers[newPeer]; ps != nil {
					ps.ts = time.Now().UnixNano()
				} else {
					n.peers[newPeer] = &lps{time.Now().UnixNano(), 0, false}
				}
				// Store our peer in our global peer map for all peers.
				peers.LoadOrStore(newPeer, newPeer)
			}
		}
	}

	// Make a copy of these values, as the AppendEntry might be cached and returned to the pool in applyCommit.
	aeCommit := ae.commit
	aeReply := ae.reply

	// Apply anything we need here.
	if aeCommit > n.commit {
		if n.paused {
			n.hcommit = aeCommit
			n.debug("Paused, not applying %d", aeCommit)
		} else {
			for index := n.commit + 1; index <= aeCommit; index++ {
				if err := n.applyCommit(index); err != nil {
					break
				}
			}
		}
	}

	var ar *appendEntryResponse
	if sub != nil {
		ar = newAppendEntryResponse(n.pterm, n.pindex, n.id, true)
	}
	n.Unlock()

	// Success. Send our response.
	if ar != nil {
		n.sendRPC(aeReply, _EMPTY_, ar.encode(arbuf))
		arPool.Put(ar)
	}
}

// processPeerState is called when a peer state entry is received
// over the wire or when we're updating known peers.
// Lock should be held.
func (n *raft) processPeerState(ps *peerState) {
	// Update our version of peers to that of the leader. Calculate
	// the number of nodes needed to establish a quorum.
	n.csz = ps.clusterSize
	n.qn = n.csz/2 + 1

	old := n.peers
	n.peers = make(map[string]*lps)
	for _, peer := range ps.knownPeers {
		if lp := old[peer]; lp != nil {
			lp.kp = true
			n.peers[peer] = lp
		} else {
			n.peers[peer] = &lps{0, 0, true}
		}
	}
	n.debug("Update peers from leader to %+v", n.peers)
	n.writePeerState(ps)
}

// processAppendEntryResponse is called when we receive an append entry
// response from another node. They will send a confirmation to tell us
// whether they successfully committed the entry or not.
func (n *raft) processAppendEntryResponse(ar *appendEntryResponse) {
	n.trackPeer(ar.peer)

	if ar.success {
		// The remote node successfully committed the append entry.
		n.trackResponse(ar)
		arPool.Put(ar)
	} else if ar.term > n.term {
		// The remote node didn't commit the append entry, it looks like
		// they are on a newer term than we are. Step down.
		n.Lock()
		n.term = ar.term
		n.vote = noVote
		n.writeTermVote()
		n.warn("Detected another leader with higher term, will stepdown")
		n.stepdownLocked(noLeader)
		n.Unlock()
		arPool.Put(ar)
	} else if ar.reply != _EMPTY_ {
		// The remote node didn't commit the append entry and they are
		// still on the same term, so let's try to catch them up.
		n.catchupFollower(ar)
	}
}

// handleAppendEntryResponse processes responses to append entries.
func (n *raft) handleAppendEntryResponse(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
	ar := n.decodeAppendEntryResponse(msg)
	ar.reply = reply
	n.resp.push(ar)
}

func (n *raft) buildAppendEntry(entries []*Entry) *appendEntry {
	return newAppendEntry(n.id, n.term, n.commit, n.pterm, n.pindex, entries)
}

// Determine if we should store an entry. This stops us from storing
// heartbeat messages.
func (ae *appendEntry) shouldStore() bool {
	return ae != nil && len(ae.entries) > 0
}

// Store our append entry to our WAL.
// lock should be held.
func (n *raft) storeToWAL(ae *appendEntry) error {
	if ae == nil {
		return fmt.Errorf("raft: Missing append entry for storage")
	}
	if n.werr != nil {
		return n.werr
	}

	seq, _, err := n.wal.StoreMsg(_EMPTY_, nil, ae.buf, 0)
	if err != nil {
		n.setWriteErrLocked(err)
		return err
	}

	// Sanity checking for now.
	if index := ae.pindex + 1; index != seq {
		n.warn("Wrong index, ae is %+v, index stored was %d, n.pindex is %d, will reset", ae, seq, n.pindex)
		if n.State() == Leader {
			n.stepdownLocked(n.selectNextLeader())
		}
		// Reset and cancel any catchup.
		n.resetWAL()
		n.cancelCatchup()
		return errEntryStoreFailed
	}

	n.pterm = ae.term
	n.pindex = seq
	return nil
}

const (
	paeDropThreshold = 20_000
	paeWarnThreshold = 10_000
	paeWarnModulo    = 5_000
)

func (n *raft) sendAppendEntry(entries []*Entry) {
	n.Lock()
	defer n.Unlock()
	ae := n.buildAppendEntry(entries)

	var err error
	var scratch [1024]byte
	ae.buf, err = ae.encode(scratch[:])
	if err != nil {
		return
	}

	// If we have entries store this in our wal.
	shouldStore := ae.shouldStore()
	if shouldStore {
		if err := n.storeToWAL(ae); err != nil {
			return
		}
		n.active = time.Now()

		// Save in memory for faster processing during applyCommit.
		n.pae[n.pindex] = ae
		if l := len(n.pae); l > paeWarnThreshold && l%paeWarnModulo == 0 {
			n.warn("%d append entries pending", len(n.pae))
		}
	}
	n.sendRPC(n.asubj, n.areply, ae.buf)
	if !shouldStore {
		ae.returnToPool()
	}
}

type extensionState uint16

const (
	extUndetermined = extensionState(iota)
	extExtended
	extNotExtended
)

type peerState struct {
	knownPeers  []string
	clusterSize int
	domainExt   extensionState
}

func peerStateBufSize(ps *peerState) int {
	return 4 + 4 + (idLen * len(ps.knownPeers)) + 2
}

func encodePeerState(ps *peerState) []byte {
	var le = binary.LittleEndian
	buf := make([]byte, peerStateBufSize(ps))
	le.PutUint32(buf[0:], uint32(ps.clusterSize))
	le.PutUint32(buf[4:], uint32(len(ps.knownPeers)))
	wi := 8
	for _, peer := range ps.knownPeers {
		copy(buf[wi:], peer)
		wi += idLen
	}
	le.PutUint16(buf[wi:], uint16(ps.domainExt))
	return buf
}

func decodePeerState(buf []byte) (*peerState, error) {
	if len(buf) < 8 {
		return nil, errCorruptPeers
	}
	var le = binary.LittleEndian
	ps := &peerState{clusterSize: int(le.Uint32(buf[0:]))}
	expectedPeers := int(le.Uint32(buf[4:]))
	buf = buf[8:]
	ri := 0
	for i, n := 0, expectedPeers; i < n && ri < len(buf); i++ {
		ps.knownPeers = append(ps.knownPeers, string(buf[ri:ri+idLen]))
		ri += idLen
	}
	if len(ps.knownPeers) != expectedPeers {
		return nil, errCorruptPeers
	}
	if len(buf[ri:]) >= 2 {
		ps.domainExt = extensionState(le.Uint16(buf[ri:]))
	}
	return ps, nil
}

// Lock should be held.
func (n *raft) peerNames() []string {
	var peers []string
	for name, peer := range n.peers {
		if peer.kp {
			peers = append(peers, name)
		}
	}
	return peers
}

func (n *raft) currentPeerState() *peerState {
	n.RLock()
	ps := &peerState{n.peerNames(), n.csz, n.extSt}
	n.RUnlock()
	return ps
}

// sendPeerState will send our current peer state to the cluster.
func (n *raft) sendPeerState() {
	n.sendAppendEntry([]*Entry{{EntryPeerState, encodePeerState(n.currentPeerState())}})
}

// Send a heartbeat.
func (n *raft) sendHeartbeat() {
	n.sendAppendEntry(nil)
}

type voteRequest struct {
	term      uint64
	lastTerm  uint64
	lastIndex uint64
	candidate string
	// internal only.
	reply string
}

const voteRequestLen = 24 + idLen

func (vr *voteRequest) encode() []byte {
	var buf [voteRequestLen]byte
	var le = binary.LittleEndian
	le.PutUint64(buf[0:], vr.term)
	le.PutUint64(buf[8:], vr.lastTerm)
	le.PutUint64(buf[16:], vr.lastIndex)
	copy(buf[24:24+idLen], vr.candidate)

	return buf[:voteRequestLen]
}

func decodeVoteRequest(msg []byte, reply string) *voteRequest {
	if len(msg) != voteRequestLen {
		return nil
	}

	var le = binary.LittleEndian
	return &voteRequest{
		term:      le.Uint64(msg[0:]),
		lastTerm:  le.Uint64(msg[8:]),
		lastIndex: le.Uint64(msg[16:]),
		candidate: string(copyBytes(msg[24 : 24+idLen])),
		reply:     reply,
	}
}

const peerStateFile = "peers.idx"

// Lock should be held.
func (n *raft) writePeerState(ps *peerState) {
	pse := encodePeerState(ps)
	if bytes.Equal(n.wps, pse) {
		return
	}
	// Stamp latest and write the peer state file.
	n.wps = pse
	if err := writePeerState(n.sd, ps); err != nil && !n.isClosed() {
		n.setWriteErrLocked(err)
		n.warn("Error writing peer state file for %q: %v", n.group, err)
	}
}

// Writes out our peer state outside of a specific raft context.
func writePeerState(sd string, ps *peerState) error {
	psf := filepath.Join(sd, peerStateFile)
	if _, err := os.Stat(psf); err != nil && !os.IsNotExist(err) {
		return err
	}
	return writeFileWithSync(psf, encodePeerState(ps), defaultFilePerms)
}

func readPeerState(sd string) (ps *peerState, err error) {
	<-dios
	buf, err := os.ReadFile(filepath.Join(sd, peerStateFile))
	dios <- struct{}{}

	if err != nil {
		return nil, err
	}
	return decodePeerState(buf)
}

const termVoteFile = "tav.idx"
const termLen = 8 // uint64
const termVoteLen = idLen + termLen

// Writes out our term & vote outside of a specific raft context.
func writeTermVote(sd string, wtv []byte) error {
	psf := filepath.Join(sd, termVoteFile)
	if _, err := os.Stat(psf); err != nil && !os.IsNotExist(err) {
		return err
	}
	return writeFileWithSync(psf, wtv, defaultFilePerms)
}

// readTermVote will read the largest term and who we voted from to stable storage.
// Lock should be held.
func (n *raft) readTermVote() (term uint64, voted string, err error) {
	<-dios
	buf, err := os.ReadFile(filepath.Join(n.sd, termVoteFile))
	dios <- struct{}{}

	if err != nil {
		return 0, noVote, err
	}
	if len(buf) < termLen {
		// Not enough bytes for the uint64 below, so avoid a panic.
		return 0, noVote, nil
	}
	var le = binary.LittleEndian
	term = le.Uint64(buf[0:])
	if len(buf) < termVoteLen {
		return term, noVote, nil
	}
	voted = string(buf[8:])
	return term, voted, nil
}

// Lock should be held.
func (n *raft) setWriteErrLocked(err error) {
	// Check if we are closed already.
	if n.State() == Closed {
		return
	}
	// Ignore if already set.
	if n.werr == err || err == nil {
		return
	}
	// Ignore non-write errors.
	if err == ErrStoreClosed ||
		err == ErrStoreEOF ||
		err == ErrInvalidSequence ||
		err == ErrStoreMsgNotFound ||
		err == errNoPending ||
		err == errPartialCache {
		return
	}
	// If this is a not found report but do not disable.
	if os.IsNotExist(err) {
		n.error("Resource not found: %v", err)
		return
	}
	n.error("Critical write error: %v", err)
	n.werr = err

	if isPermissionError(err) {
		go n.s.handleWritePermissionError()
	}

	if isOutOfSpaceErr(err) {
		// For now since this can be happening all under the covers, we will call up and disable JetStream.
		go n.s.handleOutOfSpace(nil)
	}
}

// Helper to check if we are closed when we do not hold a lock already.
func (n *raft) isClosed() bool {
	return n.State() == Closed
}

// Capture our write error if any and hold.
func (n *raft) setWriteErr(err error) {
	n.Lock()
	defer n.Unlock()
	n.setWriteErrLocked(err)
}

// writeTermVote will record the largest term and who we voted for to stable storage.
// Lock should be held.
func (n *raft) writeTermVote() {
	var buf [termVoteLen]byte
	var le = binary.LittleEndian
	le.PutUint64(buf[0:], n.term)
	copy(buf[8:], n.vote)
	b := buf[:8+len(n.vote)]

	// If the term and vote hasn't changed then don't rewrite to disk.
	if bytes.Equal(n.wtv, b) {
		return
	}
	// Stamp latest and write the term & vote file.
	n.wtv = b
	if err := writeTermVote(n.sd, n.wtv); err != nil && !n.isClosed() {
		// Clear wtv since we failed.
		n.wtv = nil
		n.setWriteErrLocked(err)
		n.warn("Error writing term and vote file for %q: %v", n.group, err)
	}
}

// voteResponse is a response to a vote request.
type voteResponse struct {
	term    uint64
	peer    string
	granted bool
}

const voteResponseLen = 8 + 8 + 1

func (vr *voteResponse) encode() []byte {
	var buf [voteResponseLen]byte
	var le = binary.LittleEndian
	le.PutUint64(buf[0:], vr.term)
	copy(buf[8:], vr.peer)
	if vr.granted {
		buf[16] = 1
	} else {
		buf[16] = 0
	}
	return buf[:voteResponseLen]
}

func decodeVoteResponse(msg []byte) *voteResponse {
	if len(msg) != voteResponseLen {
		return nil
	}
	var le = binary.LittleEndian
	vr := &voteResponse{term: le.Uint64(msg[0:]), peer: string(msg[8:16])}
	vr.granted = msg[16] == 1
	return vr
}

func (n *raft) handleVoteResponse(sub *subscription, c *client, _ *Account, _, reply string, msg []byte) {
	vr := decodeVoteResponse(msg)
	n.debug("Received a voteResponse %+v", vr)
	if vr == nil {
		n.error("Received malformed vote response for %q", n.group)
		return
	}

	if state := n.State(); state != Candidate && state != Leader {
		n.debug("Ignoring old vote response, we have stepped down")
		return
	}

	n.votes.push(vr)
}

func (n *raft) processVoteRequest(vr *voteRequest) error {
	// To simplify calling code, we can possibly pass `nil` to this function.
	// If that is the case, does not consider it an error.
	if vr == nil {
		return nil
	}
	n.debug("Received a voteRequest %+v", vr)

	if err := n.trackPeer(vr.candidate); err != nil {
		return err
	}

	n.Lock()

	vresp := &voteResponse{n.term, n.id, false}
	defer n.debug("Sending a voteResponse %+v -> %q", vresp, vr.reply)

	// Ignore if we are newer. This is important so that we don't accidentally process
	// votes from a previous term if they were still in flight somewhere.
	if vr.term < n.term {
		n.Unlock()
		n.sendReply(vr.reply, vresp.encode())
		return nil
	}

	// If this is a higher term go ahead and stepdown.
	if vr.term > n.term {
		if n.State() != Follower {
			n.debug("Stepping down from %s, detected higher term: %d vs %d",
				strings.ToLower(n.State().String()), vr.term, n.term)
			n.stepdownLocked(noLeader)
		}
		n.cancelCatchup()
		n.term = vr.term
		n.vote = noVote
		n.writeTermVote()
	}

	// Only way we get to yes is through here.
	voteOk := n.vote == noVote || n.vote == vr.candidate
	if voteOk && (vr.lastTerm > n.pterm || vr.lastTerm == n.pterm && vr.lastIndex >= n.pindex) {
		vresp.granted = true
		n.term = vr.term
		n.vote = vr.candidate
		n.writeTermVote()
		n.resetElectionTimeout()
	} else if n.vote == noVote && n.State() != Candidate {
		// We have a more up-to-date log, and haven't voted yet.
		// Start campaigning earlier, but only if not candidate already, as that would short-circuit us.
		n.resetElect(randCampaignTimeout())
	}

	// Term might have changed, make sure response has the most current
	vresp.term = n.term

	n.Unlock()

	n.sendReply(vr.reply, vresp.encode())

	return nil
}

func (n *raft) handleVoteRequest(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
	vr := decodeVoteRequest(msg, reply)
	if vr == nil {
		n.error("Received malformed vote request for %q", n.group)
		return
	}
	n.reqs.push(vr)
}

func (n *raft) requestVote() {
	n.Lock()
	if n.State() != Candidate {
		n.Unlock()
		return
	}
	n.vote = n.id
	n.writeTermVote()
	vr := voteRequest{n.term, n.pterm, n.pindex, n.id, _EMPTY_}
	subj, reply := n.vsubj, n.vreply
	n.Unlock()

	n.debug("Sending out voteRequest %+v", vr)

	// Now send it out.
	n.sendRPC(subj, reply, vr.encode())
}

func (n *raft) sendRPC(subject, reply string, msg []byte) {
	if n.sq != nil {
		n.sq.send(subject, reply, nil, msg)
	}
}

func (n *raft) sendReply(subject string, msg []byte) {
	if n.sq != nil {
		n.sq.send(subject, _EMPTY_, nil, msg)
	}
}

func (n *raft) wonElection(votes int) bool {
	return votes >= n.quorumNeeded()
}

// Return the quorum size for a given cluster config.
func (n *raft) quorumNeeded() int {
	n.RLock()
	qn := n.qn
	n.RUnlock()
	return qn
}

// Lock should be held.
func (n *raft) updateLeadChange(isLeader bool) {
	// We don't care about values that have not been consumed (transitory states),
	// so we dequeue any state that is pending and push the new one.
	for {
		select {
		case n.leadc <- isLeader:
			return
		default:
			select {
			case <-n.leadc:
			default:
				// May have been consumed by the "reader" go routine, so go back
				// to the top of the loop and try to send again.
			}
		}
	}
}

// Lock should be held.
func (n *raft) switchState(state RaftState) bool {
retry:
	pstate := n.State()
	if pstate == Closed {
		return false
	}

	// Set our state. If something else has changed our state
	// then retry, this will either be a Stop or Delete call.
	if !n.state.CompareAndSwap(int32(pstate), int32(state)) {
		goto retry
	}

	// Reset the election timer.
	n.resetElectionTimeout()

	var leadChange bool
	if pstate == Leader && state != Leader {
		leadChange = true
		n.updateLeadChange(false)
		// Drain the append entry response and proposal queues.
		n.resp.drain()
		n.prop.drain()
	} else if state == Leader && pstate != Leader {
		// Don't updateLeadChange here, it will be done in switchToLeader or after initial messages are applied.
		leadChange = true
		if len(n.pae) > 0 {
			n.pae = make(map[uint64]*appendEntry)
		}
	}

	n.writeTermVote()
	return leadChange
}

const (
	noLeader = _EMPTY_
	noVote   = _EMPTY_
)

func (n *raft) switchToFollower(leader string) {
	n.Lock()
	defer n.Unlock()

	n.switchToFollowerLocked(leader)
}

func (n *raft) switchToFollowerLocked(leader string) {
	if n.State() == Closed {
		return
	}

	n.debug("Switching to follower")

	n.aflr = 0
	n.leaderState.Store(false)
	n.lxfer = false
	// Reset acks, we can't assume acks from a previous term are still valid in another term.
	if len(n.acks) > 0 {
		n.acks = make(map[uint64]map[string]struct{})
	}
	n.updateLeader(leader)
	n.switchState(Follower)
}

func (n *raft) switchToCandidate() {
	if n.State() == Closed {
		return
	}

	n.Lock()
	defer n.Unlock()

	// If we are catching up or are in observer mode we can not switch.
	// Avoid petitioning to become leader if we're behind on applies.
	if n.observer || n.paused || n.applied < n.commit {
		n.resetElect(minElectionTimeout / 4)
		return
	}

	if n.State() != Candidate {
		n.debug("Switching to candidate")
	} else {
		if n.lostQuorumLocked() && time.Since(n.llqrt) > 20*time.Second {
			// We signal to the upper layers such that can alert on quorum lost.
			n.updateLeadChange(false)
			n.llqrt = time.Now()
		}
	}
	// Increment the term.
	n.term++
	// Clear current Leader.
	n.updateLeader(noLeader)
	n.switchState(Candidate)
}

func (n *raft) switchToLeader() {
	if n.State() == Closed {
		return
	}

	n.Lock()

	n.debug("Switching to leader")

	var state StreamState
	n.wal.FastState(&state)

	// Check if we have items pending as we are taking over.
	sendHB := state.LastSeq > n.commit

	n.lxfer = false
	n.updateLeader(n.id)
	leadChange := n.switchState(Leader)

	if leadChange {
		// Wait for messages to be applied if we've stored more, otherwise signal immediately.
		// It's important to wait signaling we're leader if we're not up-to-date yet, as that
		// would mean we're in a consistent state compared with the previous leader.
		if n.pindex > n.applied {
			n.aflr = n.pindex
		} else {
			// We know we have applied all entries in our log and can signal immediately.
			// For sanity reset applied floor back down to 0, so we aren't able to signal twice.
			n.aflr = 0
			n.leaderState.Store(true)
			n.updateLeadChange(true)
		}
	}
	n.Unlock()

	if sendHB {
		n.sendHeartbeat()
	}
}
