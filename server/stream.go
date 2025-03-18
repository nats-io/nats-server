// Copyright 2019-2025 The NATS Authors
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
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats-server/v2/server/gsl"
	"github.com/nats-io/nuid"
)

// StreamConfigRequest is used to create or update a stream.
type StreamConfigRequest struct {
	StreamConfig
	// This is not part of the StreamConfig, because its scoped to request,
	// and not to the stream itself.
	Pedantic bool `json:"pedantic,omitempty"`
}

// StreamConfig will determine the name, subjects and retention policy
// for a given stream. If subjects is empty the name will be used.
type StreamConfig struct {
	Name         string           `json:"name"`
	Description  string           `json:"description,omitempty"`
	Subjects     []string         `json:"subjects,omitempty"`
	Retention    RetentionPolicy  `json:"retention"`
	MaxConsumers int              `json:"max_consumers"`
	MaxMsgs      int64            `json:"max_msgs"`
	MaxBytes     int64            `json:"max_bytes"`
	MaxAge       time.Duration    `json:"max_age"`
	MaxMsgsPer   int64            `json:"max_msgs_per_subject"`
	MaxMsgSize   int32            `json:"max_msg_size,omitempty"`
	Discard      DiscardPolicy    `json:"discard"`
	Storage      StorageType      `json:"storage"`
	Replicas     int              `json:"num_replicas"`
	NoAck        bool             `json:"no_ack,omitempty"`
	Template     string           `json:"template_owner,omitempty"`
	Duplicates   time.Duration    `json:"duplicate_window,omitempty"`
	Placement    *Placement       `json:"placement,omitempty"`
	Mirror       *StreamSource    `json:"mirror,omitempty"`
	Sources      []*StreamSource  `json:"sources,omitempty"`
	Compression  StoreCompression `json:"compression"`
	FirstSeq     uint64           `json:"first_seq,omitempty"`

	// Allow applying a subject transform to incoming messages before doing anything else
	SubjectTransform *SubjectTransformConfig `json:"subject_transform,omitempty"`

	// Allow republish of the message after being sequenced and stored.
	RePublish *RePublish `json:"republish,omitempty"`

	// Allow higher performance, direct access to get individual messages. E.g. KeyValue
	AllowDirect bool `json:"allow_direct"`
	// Allow higher performance and unified direct access for mirrors as well.
	MirrorDirect bool `json:"mirror_direct"`

	// Allow KV like semantics to also discard new on a per subject basis
	DiscardNewPer bool `json:"discard_new_per_subject,omitempty"`

	// Optional qualifiers. These can not be modified after set to true.

	// Sealed will seal a stream so no messages can get out or in.
	Sealed bool `json:"sealed"`
	// DenyDelete will restrict the ability to delete messages.
	DenyDelete bool `json:"deny_delete"`
	// DenyPurge will restrict the ability to purge messages.
	DenyPurge bool `json:"deny_purge"`
	// AllowRollup allows messages to be placed into the system and purge
	// all older messages using a special msg header.
	AllowRollup bool `json:"allow_rollup_hdrs"`

	// The following defaults will apply to consumers when created against
	// this stream, unless overridden manually.
	// TODO(nat): Can/should we name these better?
	ConsumerLimits StreamConsumerLimits `json:"consumer_limits"`

	// AllowMsgTTL allows header initiated per-message TTLs. If disabled,
	// then the `NATS-TTL` header will be ignored.
	AllowMsgTTL bool `json:"allow_msg_ttl"`

	// SubjectDeleteMarkerTTL sets the TTL of delete marker messages left behind by
	// subject delete markers.
	SubjectDeleteMarkerTTL time.Duration `json:"subject_delete_marker_ttl,omitempty"`

	// Metadata is additional metadata for the Stream.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// clone performs a deep copy of the StreamConfig struct, returning a new clone with
// all values copied.
func (cfg *StreamConfig) clone() *StreamConfig {
	clone := *cfg
	if cfg.Placement != nil {
		placement := *cfg.Placement
		clone.Placement = &placement
	}
	if cfg.Mirror != nil {
		mirror := *cfg.Mirror
		clone.Mirror = &mirror
	}
	if len(cfg.Sources) > 0 {
		clone.Sources = make([]*StreamSource, len(cfg.Sources))
		for i, cfgSource := range cfg.Sources {
			source := *cfgSource
			clone.Sources[i] = &source
		}
	}
	if cfg.SubjectTransform != nil {
		transform := *cfg.SubjectTransform
		clone.SubjectTransform = &transform
	}
	if cfg.RePublish != nil {
		rePublish := *cfg.RePublish
		clone.RePublish = &rePublish
	}
	if cfg.Metadata != nil {
		clone.Metadata = make(map[string]string, len(cfg.Metadata))
		for k, v := range cfg.Metadata {
			clone.Metadata[k] = v
		}
	}
	return &clone
}

type StreamConsumerLimits struct {
	InactiveThreshold time.Duration `json:"inactive_threshold,omitempty"`
	MaxAckPending     int           `json:"max_ack_pending,omitempty"`
}

// SubjectTransformConfig is for applying a subject transform (to matching messages) before doing anything else when a new message is received
type SubjectTransformConfig struct {
	Source      string `json:"src"`
	Destination string `json:"dest"`
}

// RePublish is for republishing messages once committed to a stream.
type RePublish struct {
	Source      string `json:"src,omitempty"`
	Destination string `json:"dest"`
	HeadersOnly bool   `json:"headers_only,omitempty"`
}

// JSPubAckResponse is a formal response to a publish operation.
type JSPubAckResponse struct {
	Error *ApiError `json:"error,omitempty"`
	*PubAck
}

// ToError checks if the response has a error and if it does converts it to an error
// avoiding the pitfalls described by https://yourbasic.org/golang/gotcha-why-nil-error-not-equal-nil/
func (r *JSPubAckResponse) ToError() error {
	if r.Error == nil {
		return nil
	}
	return r.Error
}

// PubAck is the detail you get back from a publish to a stream that was successful.
// e.g. +OK {"stream": "Orders", "seq": 22}
type PubAck struct {
	Stream    string `json:"stream"`
	Sequence  uint64 `json:"seq"`
	Domain    string `json:"domain,omitempty"`
	Duplicate bool   `json:"duplicate,omitempty"`
}

// StreamInfo shows config and current state for this stream.
type StreamInfo struct {
	Config     StreamConfig        `json:"config"`
	Created    time.Time           `json:"created"`
	State      StreamState         `json:"state"`
	Domain     string              `json:"domain,omitempty"`
	Cluster    *ClusterInfo        `json:"cluster,omitempty"`
	Mirror     *StreamSourceInfo   `json:"mirror,omitempty"`
	Sources    []*StreamSourceInfo `json:"sources,omitempty"`
	Alternates []StreamAlternate   `json:"alternates,omitempty"`
	// TimeStamp indicates when the info was gathered
	TimeStamp time.Time `json:"ts"`
}

type StreamAlternate struct {
	Name    string `json:"name"`
	Domain  string `json:"domain,omitempty"`
	Cluster string `json:"cluster"`
}

// ClusterInfo shows information about the underlying set of servers
// that make up the stream or consumer.
type ClusterInfo struct {
	Name      string      `json:"name,omitempty"`
	RaftGroup string      `json:"raft_group,omitempty"`
	Leader    string      `json:"leader,omitempty"`
	Replicas  []*PeerInfo `json:"replicas,omitempty"`
}

// PeerInfo shows information about all the peers in the cluster that
// are supporting the stream or consumer.
type PeerInfo struct {
	Name    string        `json:"name"`
	Current bool          `json:"current"`
	Offline bool          `json:"offline,omitempty"`
	Active  time.Duration `json:"active"`
	Lag     uint64        `json:"lag,omitempty"`
	Peer    string        `json:"peer"`
	// For migrations.
	cluster string
}

// StreamSourceInfo shows information about an upstream stream source.
type StreamSourceInfo struct {
	Name              string                   `json:"name"`
	External          *ExternalStream          `json:"external,omitempty"`
	Lag               uint64                   `json:"lag"`
	Active            time.Duration            `json:"active"`
	Error             *ApiError                `json:"error,omitempty"`
	FilterSubject     string                   `json:"filter_subject,omitempty"`
	SubjectTransforms []SubjectTransformConfig `json:"subject_transforms,omitempty"`
}

// StreamSource dictates how streams can source from other streams.
type StreamSource struct {
	Name              string                   `json:"name"`
	OptStartSeq       uint64                   `json:"opt_start_seq,omitempty"`
	OptStartTime      *time.Time               `json:"opt_start_time,omitempty"`
	FilterSubject     string                   `json:"filter_subject,omitempty"`
	SubjectTransforms []SubjectTransformConfig `json:"subject_transforms,omitempty"`
	External          *ExternalStream          `json:"external,omitempty"`

	// Internal
	iname string // For indexing when stream names are the same for multiple sources.
}

// ExternalStream allows you to qualify access to a stream source in another account.
type ExternalStream struct {
	ApiPrefix     string `json:"api"`
	DeliverPrefix string `json:"deliver"`
}

// For managing stream ingest.
const (
	streamDefaultMaxQueueMsgs  = 10_000
	streamDefaultMaxQueueBytes = 1024 * 1024 * 128
)

// Stream is a jetstream stream of messages. When we receive a message internally destined
// for a Stream we will direct link from the client to this structure.
type stream struct {
	mu     sync.RWMutex // Read/write lock for the stream.
	js     *jetStream   // The internal *jetStream for the account.
	jsa    *jsAccount   // The JetStream account-level information.
	acc    *Account     // The account this stream is defined in.
	srv    *Server      // The server we are running in.
	client *client      // The internal JetStream client.
	sysc   *client      // The internal JetStream system client.

	// The current last subscription ID for the subscriptions through `client`.
	// Those subscriptions are for the subjects filters being listened to and captured by the stream.
	sid atomic.Uint64

	pubAck    []byte                  // The template (prefix) to generate the pubAck responses for this stream quickly.
	outq      *jsOutQ                 // Queue of *jsPubMsg for sending messages.
	msgs      *ipQueue[*inMsg]        // Intra-process queue for the ingress of messages.
	gets      *ipQueue[*directGetReq] // Intra-process queue for the direct get requests.
	store     StreamStore             // The storage for this stream.
	ackq      *ipQueue[uint64]        // Intra-process queue for acks.
	lseq      uint64                  // The sequence number of the last message stored in the stream.
	lmsgId    string                  // The de-duplication message ID of the last message stored in the stream.
	consumers map[string]*consumer    // The consumers for this stream.
	numFilter int                     // The number of filtered consumers.
	cfg       StreamConfig            // The stream's config.
	cfgMu     sync.RWMutex            // Config mutex used to solve some races with consumer code
	created   time.Time               // Time the stream was created.
	stype     StorageType             // The storage type.
	tier      string                  // The tier is the number of replicas for the stream (e.g. "R1" or "R3").
	ddmap     map[string]*ddentry     // The dedupe map.
	ddarr     []*ddentry              // The dedupe array.
	ddindex   int                     // The dedupe index.
	ddtmr     *time.Timer             // The dedupe timer.
	qch       chan struct{}           // The quit channel.
	mqch      chan struct{}           // The monitor's quit channel.
	active    bool                    // Indicates that there are active internal subscriptions (for the subject filters)
	// and/or mirror/sources consumers are scheduled to be established or already started.
	ddloaded bool        // set to true when the deduplication structures are been built.
	closed   atomic.Bool // Set to true when stop() is called on the stream.

	// Mirror
	mirror *sourceInfo

	// Sources
	sources              map[string]*sourceInfo
	sourceSetupSchedules map[string]*time.Timer
	sourcesConsumerSetup *time.Timer
	smsgs                *ipQueue[*inMsg] // Intra-process queue for all incoming sourced messages.

	// Indicates we have direct consumers.
	directs int

	// For input subject transform.
	itr *subjectTransform

	// For republishing.
	tr *subjectTransform

	// For processing consumers without main stream lock.
	clsMu sync.RWMutex
	cList []*consumer                    // Consumer list.
	sch   chan struct{}                  // Channel to signal consumers.
	sigq  *ipQueue[*cMsg]                // Intra-process queue for the messages to signal to the consumers.
	csl   *gsl.GenericSublist[*consumer] // Consumer subscription list.

	// Leader will store seq/msgTrace in clustering mode. Used in applyStreamEntries
	// to know if trace event should be sent after processing.
	mt map[uint64]*msgTrace

	// For non limits policy streams when they process an ack before the actual msg.
	// Can happen in stretch clusters, multi-cloud, or during catchup for a restarted server.
	preAcks map[uint64]map[*consumer]struct{}

	// TODO(dlc) - Hide everything below behind two pointers.
	// Clustered mode.
	sa        *streamAssignment // What the meta controller uses to assign streams to peers.
	node      RaftNode          // Our RAFT node for the stream's group.
	catchup   atomic.Bool       // Used to signal we are in catchup mode.
	catchups  map[string]uint64 // The number of messages that need to be caught per peer.
	syncSub   *subscription     // Internal subscription for sync messages (on "$JSC.SYNC").
	infoSub   *subscription     // Internal subscription for stream info requests.
	clMu      sync.Mutex        // The mutex for clseq and clfs.
	clseq     uint64            // The current last seq being proposed to the NRG layer.
	clfs      uint64            // The count (offset) of the number of failed NRG sequences used to compute clseq.
	inflight  map[uint64]uint64 // Inflight message sizes per clseq.
	lqsent    time.Time         // The time at which the last lost quorum advisory was sent. Used to rate limit.
	uch       chan struct{}     // The channel to signal updates to the monitor routine.
	inMonitor bool              // True if the monitor routine has been started.

	expectedPerSubjectSequence  map[uint64]string   // Inflight 'expected per subject' subjects per clseq.
	expectedPerSubjectInProcess map[string]struct{} // Current 'expected per subject' subjects in process.

	// Direct get subscription.
	directSub *subscription
	lastBySub *subscription

	monitorWg sync.WaitGroup // Wait group for the monitor routine.
}

type sourceInfo struct {
	name  string        // The name of the stream being sourced.
	iname string        // The unique index name of this particular source.
	cname string        // The name of the current consumer for this source.
	sub   *subscription // The subscription to the consumer.

	// (mirrors only) The subscription to the direct get request subject for
	// the source stream's name on the `_sys_` queue group.
	dsub *subscription

	// (mirrors only) The subscription to the direct get last per subject request subject for
	// the source stream's name on the `_sys_` queue group.
	lbsub *subscription

	msgs  *ipQueue[*inMsg]    // Intra-process queue for incoming messages.
	sseq  uint64              // Last stream message sequence number seen from the source.
	dseq  uint64              // Last delivery (i.e. consumer's) sequence number.
	lag   uint64              // 0 or number of messages pending (as last reported by the consumer) - 1.
	err   *ApiError           // The API error that caused the last consumer setup to fail.
	fails int                 // The number of times trying to setup the consumer failed.
	last  atomic.Int64        // Time the consumer was created or of last message it received.
	lreq  time.Time           // The last time setupMirrorConsumer/setupSourceConsumer was called.
	qch   chan struct{}       // Quit channel.
	sip   bool                // Setup in progress.
	wg    sync.WaitGroup      // WaitGroup for the consumer's go routine.
	sf    string              // The subject filter.
	sfs   []string            // The subject filters.
	trs   []*subjectTransform // The subject transforms.
}

// For mirrors and direct get
const (
	dgetGroup          = sysGroup
	dgetCaughtUpThresh = 10
)

// Headers for published messages.
const (
	JSMsgId                   = "Nats-Msg-Id"
	JSExpectedStream          = "Nats-Expected-Stream"
	JSExpectedLastSeq         = "Nats-Expected-Last-Sequence"
	JSExpectedLastSubjSeq     = "Nats-Expected-Last-Subject-Sequence"
	JSExpectedLastSubjSeqSubj = "Nats-Expected-Last-Subject-Sequence-Subject"
	JSExpectedLastMsgId       = "Nats-Expected-Last-Msg-Id"
	JSStreamSource            = "Nats-Stream-Source"
	JSLastConsumerSeq         = "Nats-Last-Consumer"
	JSLastStreamSeq           = "Nats-Last-Stream"
	JSConsumerStalled         = "Nats-Consumer-Stalled"
	JSMsgRollup               = "Nats-Rollup"
	JSMsgSize                 = "Nats-Msg-Size"
	JSResponseType            = "Nats-Response-Type"
	JSMessageTTL              = "Nats-TTL"
	JSMarkerReason            = "Nats-Marker-Reason"
)

// Headers for republished messages and direct gets.
const (
	JSStream       = "Nats-Stream"
	JSSequence     = "Nats-Sequence"
	JSTimeStamp    = "Nats-Time-Stamp"
	JSSubject      = "Nats-Subject"
	JSLastSequence = "Nats-Last-Sequence"
	JSNumPending   = "Nats-Num-Pending"
	JSUpToSequence = "Nats-UpTo-Sequence"
)

// Rollups, can be subject only or all messages.
const (
	JSMsgRollupSubject = "sub"
	JSMsgRollupAll     = "all"
)

// Applied limits in the Nats-Applied-Limit header.
const (
	JSMarkerReasonMaxAge = "MaxAge"
	JSMarkerReasonPurge  = "Purge"
	JSMarkerReasonRemove = "Remove"
)

const (
	jsCreateResponse = "create"
)

// Dedupe entry
type ddentry struct {
	id  string // The unique message ID provided by the client.
	seq uint64 // The sequence number of the message.
	ts  int64  // The timestamp of the message.
}

// Replicas Range
const StreamMaxReplicas = 5

// AddStream adds a stream for the given account.
func (a *Account) addStream(config *StreamConfig) (*stream, error) {
	return a.addStreamWithAssignment(config, nil, nil, false)
}

// AddStreamWithStore adds a stream for the given account with custome store config options.
func (a *Account) addStreamWithStore(config *StreamConfig, fsConfig *FileStoreConfig) (*stream, error) {
	return a.addStreamWithAssignment(config, fsConfig, nil, false)
}

func (a *Account) addStreamPedantic(config *StreamConfig, pedantic bool) (*stream, error) {
	return a.addStreamWithAssignment(config, nil, nil, pedantic)
}

func (a *Account) addStreamWithAssignment(config *StreamConfig, fsConfig *FileStoreConfig, sa *streamAssignment, pedantic bool) (*stream, error) {
	s, jsa, err := a.checkForJetStream()
	if err != nil {
		return nil, err
	}

	// If we do not have the stream currently assigned to us in cluster mode we will proceed but warn.
	// This can happen on startup with restored state where on meta replay we still do not have
	// the assignment. Running in single server mode this always returns true.
	if !jsa.streamAssigned(config.Name) {
		s.Debugf("Stream '%s > %s' does not seem to be assigned to this server", a.Name, config.Name)
	}

	// Sensible defaults.
	cfg, apiErr := s.checkStreamCfg(config, a, pedantic)
	if apiErr != nil {
		return nil, apiErr
	}

	singleServerMode := !s.JetStreamIsClustered() && s.standAloneMode()
	if singleServerMode && cfg.Replicas > 1 {
		return nil, ApiErrors[JSStreamReplicasNotSupportedErr]
	}

	// Make sure we are ok when these are done in parallel.
	// We used to call Add(1) in the "else" clause of the "if loaded"
	// statement. This caused a data race because it was possible
	// that one go routine stores (with count==0) and another routine
	// gets "loaded==true" and calls wg.Wait() while the other routine
	// then calls wg.Add(1). It also could mean that two routines execute
	// the rest of the code concurrently.
	swg := &sync.WaitGroup{}
	swg.Add(1)
	v, loaded := jsa.inflight.LoadOrStore(cfg.Name, swg)
	wg := v.(*sync.WaitGroup)
	if loaded {
		wg.Wait()
		// This waitgroup is "thrown away" (since there was an existing one).
		swg.Done()
	} else {
		defer func() {
			jsa.inflight.Delete(cfg.Name)
			wg.Done()
		}()
	}

	js, isClustered := jsa.jetStreamAndClustered()
	jsa.mu.Lock()
	if mset, ok := jsa.streams[cfg.Name]; ok {
		jsa.mu.Unlock()
		// Check to see if configs are same.
		ocfg := mset.config()

		// set the index name on cfg since it would not contain a value for iname while the return from mset.config() does to ensure the DeepEqual works
		for _, s := range cfg.Sources {
			s.setIndexName()
		}

		if reflect.DeepEqual(ocfg, cfg) {
			if sa != nil {
				mset.setStreamAssignment(sa)
			}
			return mset, nil
		} else {
			return nil, ApiErrors[JSStreamNameExistErr]
		}
	}
	jsa.usageMu.RLock()
	selected, tier, hasTier := jsa.selectLimits(cfg.Replicas)
	jsa.usageMu.RUnlock()
	reserved := int64(0)
	if !isClustered {
		reserved = jsa.tieredReservation(tier, &cfg)
	}
	jsa.mu.Unlock()

	if !hasTier {
		return nil, NewJSNoLimitsError()
	}
	js.mu.RLock()
	if isClustered {
		_, reserved = tieredStreamAndReservationCount(js.cluster.streams[a.Name], tier, &cfg)
	}
	if err := js.checkAllLimits(&selected, &cfg, reserved, 0); err != nil {
		js.mu.RUnlock()
		return nil, err
	}
	js.mu.RUnlock()
	jsa.mu.Lock()
	// Check for template ownership if present.
	if cfg.Template != _EMPTY_ && jsa.account != nil {
		if !jsa.checkTemplateOwnership(cfg.Template, cfg.Name) {
			jsa.mu.Unlock()
			return nil, fmt.Errorf("stream not owned by template")
		}
	}

	// If mirror, check if the transforms (if any) are valid.
	if cfg.Mirror != nil {
		if len(cfg.Mirror.SubjectTransforms) == 0 {
			if cfg.Mirror.FilterSubject != _EMPTY_ && !IsValidSubject(cfg.Mirror.FilterSubject) {
				jsa.mu.Unlock()
				return nil, fmt.Errorf("subject filter '%s' for the mirror %w", cfg.Mirror.FilterSubject, ErrBadSubject)
			}
		} else {
			for _, st := range cfg.Mirror.SubjectTransforms {
				if st.Source != _EMPTY_ && !IsValidSubject(st.Source) {
					jsa.mu.Unlock()
					return nil, fmt.Errorf("invalid subject transform source '%s' for the mirror: %w", st.Source, ErrBadSubject)
				}
				// check the transform, if any, is valid
				if st.Destination != _EMPTY_ {
					if _, err = NewSubjectTransform(st.Source, st.Destination); err != nil {
						jsa.mu.Unlock()
						return nil, fmt.Errorf("subject transform from '%s' to '%s' for the mirror: %w", st.Source, st.Destination, err)
					}
				}
			}
		}
	}

	// Setup our internal indexed names here for sources and check if the transforms (if any) are valid.
	for _, ssi := range cfg.Sources {
		if len(ssi.SubjectTransforms) == 0 {
			// check the filter, if any, is valid
			if ssi.FilterSubject != _EMPTY_ && !IsValidSubject(ssi.FilterSubject) {
				jsa.mu.Unlock()
				return nil, fmt.Errorf("subject filter '%s' for the source: %w", ssi.FilterSubject, ErrBadSubject)
			}
		} else {
			for _, st := range ssi.SubjectTransforms {
				if st.Source != _EMPTY_ && !IsValidSubject(st.Source) {
					jsa.mu.Unlock()
					return nil, fmt.Errorf("subject filter '%s' for the source: %w", st.Source, ErrBadSubject)
				}
				// check the transform, if any, is valid
				if st.Destination != _EMPTY_ {
					if _, err = NewSubjectTransform(st.Source, st.Destination); err != nil {
						jsa.mu.Unlock()
						return nil, fmt.Errorf("subject transform from '%s' to '%s' for the source: %w", st.Source, st.Destination, err)
					}
				}
			}
		}
	}

	// Check for overlapping subjects with other streams.
	// These are not allowed for now.
	if jsa.subjectsOverlap(cfg.Subjects, nil) {
		jsa.mu.Unlock()
		return nil, NewJSStreamSubjectOverlapError()
	}

	if !hasTier {
		jsa.mu.Unlock()
		return nil, fmt.Errorf("no applicable tier found")
	}

	// Setup the internal clients.
	c := s.createInternalJetStreamClient()
	ic := s.createInternalJetStreamClient()

	// Work out the stream ingest limits.
	mlen := s.opts.StreamMaxBufferedMsgs
	msz := uint64(s.opts.StreamMaxBufferedSize)
	if mlen == 0 {
		mlen = streamDefaultMaxQueueMsgs
	}
	if msz == 0 {
		msz = streamDefaultMaxQueueBytes
	}

	qpfx := fmt.Sprintf("[ACC:%s] stream '%s' ", a.Name, config.Name)
	mset := &stream{
		acc:       a,
		jsa:       jsa,
		cfg:       cfg,
		js:        js,
		srv:       s,
		client:    c,
		sysc:      ic,
		tier:      tier,
		stype:     cfg.Storage,
		consumers: make(map[string]*consumer),
		msgs: newIPQueue[*inMsg](s, qpfx+"messages",
			ipqSizeCalculation(func(msg *inMsg) uint64 {
				return uint64(len(msg.hdr) + len(msg.msg) + len(msg.rply) + len(msg.subj))
			}),
			ipqLimitByLen[*inMsg](mlen),
			ipqLimitBySize[*inMsg](msz),
		),
		gets: newIPQueue[*directGetReq](s, qpfx+"direct gets"),
		qch:  make(chan struct{}),
		mqch: make(chan struct{}),
		uch:  make(chan struct{}, 4),
		sch:  make(chan struct{}, 1),
	}

	// Start our signaling routine to process consumers.
	mset.sigq = newIPQueue[*cMsg](s, qpfx+"obs") // of *cMsg
	go mset.signalConsumersLoop()

	// For no-ack consumers when we are interest retention.
	if cfg.Retention != LimitsPolicy {
		mset.ackq = newIPQueue[uint64](s, qpfx+"acks")
	}

	// Check for input subject transform
	if cfg.SubjectTransform != nil {
		tr, err := NewSubjectTransform(cfg.SubjectTransform.Source, cfg.SubjectTransform.Destination)
		if err != nil {
			jsa.mu.Unlock()
			return nil, fmt.Errorf("stream subject transform from '%s' to '%s': %w", cfg.SubjectTransform.Source, cfg.SubjectTransform.Destination, err)
		}
		mset.itr = tr
	}

	// Check for RePublish.
	if cfg.RePublish != nil {
		tr, err := NewSubjectTransform(cfg.RePublish.Source, cfg.RePublish.Destination)
		if err != nil {
			jsa.mu.Unlock()
			return nil, fmt.Errorf("stream republish transform from '%s' to '%s': %w", cfg.RePublish.Source, cfg.RePublish.Destination, err)
		}
		// Assign our transform for republishing.
		mset.tr = tr
	}
	storeDir := filepath.Join(jsa.storeDir, streamsDir, cfg.Name)
	jsa.mu.Unlock()

	// Bind to the user account.
	c.registerWithAccount(a)
	// Bind to the system account.
	ic.registerWithAccount(s.SystemAccount())

	// Create the appropriate storage
	fsCfg := fsConfig
	if fsCfg == nil {
		fsCfg = &FileStoreConfig{}
		// If we are file based and not explicitly configured
		// we may be able to auto-tune based on max msgs or bytes.
		if cfg.Storage == FileStorage {
			mset.autoTuneFileStorageBlockSize(fsCfg)
		}
	}
	fsCfg.StoreDir = storeDir
	fsCfg.AsyncFlush = false
	// Grab configured sync interval.
	fsCfg.SyncInterval = s.getOpts().SyncInterval
	fsCfg.SyncAlways = s.getOpts().SyncAlways
	fsCfg.Compression = config.Compression

	if err := mset.setupStore(fsCfg); err != nil {
		mset.stop(true, false)
		return nil, NewJSStreamStoreFailedError(err)
	}

	// Create our pubAck template here. Better than json marshal each time on success.
	if domain := s.getOpts().JetStreamDomain; domain != _EMPTY_ {
		mset.pubAck = []byte(fmt.Sprintf("{%q:%q, %q:%q, %q:", "stream", cfg.Name, "domain", domain, "seq"))
	} else {
		mset.pubAck = []byte(fmt.Sprintf("{%q:%q, %q:", "stream", cfg.Name, "seq"))
	}
	end := len(mset.pubAck)
	mset.pubAck = mset.pubAck[:end:end]

	// Set our known last sequence.
	var state StreamState
	mset.store.FastState(&state)

	// Possible race with consumer.setLeader during recovery.
	mset.mu.Lock()
	mset.lseq = state.LastSeq
	mset.mu.Unlock()

	// If no msgs (new stream), set dedupe state loaded to true.
	if state.Msgs == 0 {
		mset.ddloaded = true
	}

	// Set our stream assignment if in clustered mode.
	reserveResources := true
	if sa != nil {
		mset.setStreamAssignment(sa)

		// If the stream is resetting we must not double-account resources, they were already accounted for.
		js.mu.Lock()
		if sa.resetting {
			reserveResources, sa.resetting = false, false
		}
		js.mu.Unlock()
	}

	// Setup our internal send go routine.
	mset.setupSendCapabilities()

	// Reserve resources if MaxBytes present.
	if reserveResources {
		mset.js.reserveStreamResources(&mset.cfg)
	}

	// Call directly to set leader if not in clustered mode.
	// This can be called though before we actually setup clustering, so check both.
	if singleServerMode {
		if err := mset.setLeader(true); err != nil {
			mset.stop(true, false)
			return nil, err
		}
	}

	// This is always true in single server mode.
	if mset.IsLeader() {
		// Send advisory.
		var suppress bool
		if !s.standAloneMode() && sa == nil {
			if cfg.Replicas > 1 {
				suppress = true
			}
		} else if sa != nil {
			suppress = sa.responded
		}
		if !suppress {
			mset.sendCreateAdvisory()
		}
	}

	// Register with our account last.
	jsa.mu.Lock()
	jsa.streams[cfg.Name] = mset
	jsa.mu.Unlock()

	return mset, nil
}

// Composes the index name. Contains the stream name, subject filter, and transform destination
// when the stream is external we will use the api prefix as part of the index name
// (as the same stream name could be used in multiple JS domains)
func (ssi *StreamSource) composeIName() string {
	var iName = ssi.Name

	if ssi.External != nil {
		iName = iName + ":" + getHash(ssi.External.ApiPrefix)
	}

	source := ssi.FilterSubject
	destination := fwcs

	if len(ssi.SubjectTransforms) == 0 {
		// normalize filter and destination in case they are empty
		if source == _EMPTY_ {
			source = fwcs
		}
		if destination == _EMPTY_ {
			destination = fwcs
		}
	} else {
		var sources, destinations []string

		for _, tr := range ssi.SubjectTransforms {
			trsrc, trdest := tr.Source, tr.Destination
			if trsrc == _EMPTY_ {
				trsrc = fwcs
			}
			if trdest == _EMPTY_ {
				trdest = fwcs
			}
			sources = append(sources, trsrc)
			destinations = append(destinations, trdest)
		}
		source = strings.Join(sources, "\f")
		destination = strings.Join(destinations, "\f")
	}

	return strings.Join([]string{iName, source, destination}, " ")
}

// Sets the index name.
func (ssi *StreamSource) setIndexName() {
	ssi.iname = ssi.composeIName()
}

func (mset *stream) streamAssignment() *streamAssignment {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.sa
}

func (mset *stream) setStreamAssignment(sa *streamAssignment) {
	var node RaftNode
	var peers []string

	mset.mu.RLock()
	js := mset.js
	mset.mu.RUnlock()

	if js != nil {
		js.mu.RLock()
		if sa.Group != nil {
			node = sa.Group.node
			peers = sa.Group.Peers
		}
		js.mu.RUnlock()
	}

	mset.mu.Lock()
	defer mset.mu.Unlock()

	mset.sa = sa
	if sa == nil {
		return
	}

	// Set our node.
	mset.node = node
	if mset.node != nil {
		mset.node.UpdateKnownPeers(peers)
	}

	// Setup our info sub here as well for all stream members. This is now by design.
	if mset.infoSub == nil {
		isubj := fmt.Sprintf(clusterStreamInfoT, mset.jsa.acc(), mset.cfg.Name)
		// Note below the way we subscribe here is so that we can send requests to ourselves.
		mset.infoSub, _ = mset.srv.systemSubscribe(isubj, _EMPTY_, false, mset.sysc, mset.handleClusterStreamInfoRequest)
	}

	// Trigger update chan.
	select {
	case mset.uch <- struct{}{}:
	default:
	}
}

func (mset *stream) monitorQuitC() <-chan struct{} {
	if mset == nil {
		return nil
	}
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.mqch
}

func (mset *stream) updateC() <-chan struct{} {
	if mset == nil {
		return nil
	}
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.uch
}

// IsLeader will return if we are the current leader.
func (mset *stream) IsLeader() bool {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.isLeader()
}

// Lock should be held.
func (mset *stream) isLeader() bool {
	if mset.isClustered() {
		return mset.node.Leader()
	}
	return true
}

// isLeaderNodeState should NOT be used normally, use isLeader instead.
// Returns whether the node thinks it is the leader, regardless of whether applies are up-to-date yet
// (unlike isLeader, which requires applies to be caught up).
// May be used to respond to clients after a leader change, when applying entries from a former leader.
// Lock should be held.
func (mset *stream) isLeaderNodeState() bool {
	if mset.isClustered() {
		return mset.node.State() == Leader
	}
	return true
}

// TODO(dlc) - Check to see if we can accept being the leader or we should step down.
func (mset *stream) setLeader(isLeader bool) error {
	mset.mu.Lock()
	// If we are here we have a change in leader status.
	if isLeader {
		// Make sure we are listening for sync requests.
		// TODO(dlc) - Original design was that all in sync members of the group would do DQ.
		if mset.isClustered() {
			mset.startClusterSubs()
		}

		// Setup subscriptions if we were not already the leader.
		if err := mset.subscribeToStream(); err != nil {
			if mset.isClustered() {
				// Stepdown since we have an error.
				mset.node.StepDown()
			}
			mset.mu.Unlock()
			return err
		}
	} else {
		// cancel timer to create the source consumers if not fired yet
		if mset.sourcesConsumerSetup != nil {
			mset.sourcesConsumerSetup.Stop()
			mset.sourcesConsumerSetup = nil
		} else {
			// Stop any source consumers
			mset.stopSourceConsumers()
		}

		// Stop responding to sync requests.
		mset.stopClusterSubs()
		// Unsubscribe from direct stream.
		mset.unsubscribeToStream(false)
		// Clear catchup state
		mset.clearAllCatchupPeers()
	}
	mset.mu.Unlock()

	// If we are interest based make sure to check consumers.
	// This is to make sure we process any outstanding acks.
	mset.checkInterestState()

	return nil
}

// Lock should be held.
func (mset *stream) startClusterSubs() {
	if mset.syncSub == nil {
		mset.syncSub, _ = mset.srv.systemSubscribe(mset.sa.Sync, _EMPTY_, false, mset.sysc, mset.handleClusterSyncRequest)
	}
}

// Lock should be held.
func (mset *stream) stopClusterSubs() {
	if mset.syncSub != nil {
		mset.srv.sysUnsubscribe(mset.syncSub)
		mset.syncSub = nil
	}
}

// account gets the account for this stream.
func (mset *stream) account() *Account {
	mset.mu.RLock()
	jsa := mset.jsa
	mset.mu.RUnlock()
	if jsa == nil {
		return nil
	}
	return jsa.acc()
}

// Helper to determine the max msg size for this stream if file based.
func (mset *stream) maxMsgSize() uint64 {
	maxMsgSize := mset.cfg.MaxMsgSize
	if maxMsgSize <= 0 {
		// Pull from the account.
		if mset.jsa != nil {
			if acc := mset.jsa.acc(); acc != nil {
				acc.mu.RLock()
				maxMsgSize = acc.mpay
				acc.mu.RUnlock()
			}
		}
		// If all else fails use default.
		if maxMsgSize <= 0 {
			maxMsgSize = MAX_PAYLOAD_SIZE
		}
	}
	// Now determine an estimation for the subjects etc.
	maxSubject := -1
	for _, subj := range mset.cfg.Subjects {
		if subjectIsLiteral(subj) {
			if len(subj) > maxSubject {
				maxSubject = len(subj)
			}
		}
	}
	if maxSubject < 0 {
		const defaultMaxSubject = 256
		maxSubject = defaultMaxSubject
	}
	// filestore will add in estimates for record headers, etc.
	return fileStoreMsgSizeEstimate(maxSubject, int(maxMsgSize))
}

// If we are file based and the file storage config was not explicitly set
// we can autotune block sizes to better match. Our target will be to store 125%
// of the theoretical limit. We will round up to nearest 100 bytes as well.
func (mset *stream) autoTuneFileStorageBlockSize(fsCfg *FileStoreConfig) {
	var totalEstSize uint64

	// MaxBytes will take precedence for now.
	if mset.cfg.MaxBytes > 0 {
		totalEstSize = uint64(mset.cfg.MaxBytes)
	} else if mset.cfg.MaxMsgs > 0 {
		// Determine max message size to estimate.
		totalEstSize = mset.maxMsgSize() * uint64(mset.cfg.MaxMsgs)
	} else if mset.cfg.MaxMsgsPer > 0 {
		fsCfg.BlockSize = uint64(defaultKVBlockSize)
		return
	} else {
		// If nothing set will let underlying filestore determine blkSize.
		return
	}

	blkSize := (totalEstSize / 4) + 1 // (25% overhead)
	// Round up to nearest 100
	if m := blkSize % 100; m != 0 {
		blkSize += 100 - m
	}
	if blkSize <= FileStoreMinBlkSize {
		blkSize = FileStoreMinBlkSize
	} else if blkSize >= FileStoreMaxBlkSize {
		blkSize = FileStoreMaxBlkSize
	} else {
		blkSize = defaultMediumBlockSize
	}
	fsCfg.BlockSize = uint64(blkSize)
}

// rebuildDedupe will rebuild any dedupe structures needed after recovery of a stream.
// Will be called lazily to avoid penalizing startup times.
// TODO(dlc) - Might be good to know if this should be checked at all for streams with no
// headers and msgId in them. Would need signaling from the storage layer.
// Lock should be held.
func (mset *stream) rebuildDedupe() {
	if mset.ddloaded {
		return
	}

	mset.ddloaded = true

	// We have some messages. Lookup starting sequence by duplicate time window.
	sseq := mset.store.GetSeqFromTime(time.Now().Add(-mset.cfg.Duplicates))
	if sseq == 0 {
		return
	}

	var smv StoreMsg
	var state StreamState
	mset.store.FastState(&state)

	for seq := sseq; seq <= state.LastSeq; seq++ {
		sm, err := mset.store.LoadMsg(seq, &smv)
		if err != nil {
			continue
		}
		var msgId string
		if len(sm.hdr) > 0 {
			if msgId = getMsgId(sm.hdr); msgId != _EMPTY_ {
				mset.storeMsgIdLocked(&ddentry{msgId, sm.seq, sm.ts})
			}
		}
		if seq == state.LastSeq {
			mset.lmsgId = msgId
		}
	}
}

func (mset *stream) lastSeqAndCLFS() (uint64, uint64) {
	return mset.lastSeq(), mset.getCLFS()
}

func (mset *stream) getCLFS() uint64 {
	if mset == nil {
		return 0
	}
	mset.clMu.Lock()
	defer mset.clMu.Unlock()
	return mset.clfs
}

func (mset *stream) setCLFS(clfs uint64) {
	mset.clMu.Lock()
	mset.clfs = clfs
	mset.clMu.Unlock()
}

func (mset *stream) lastSeq() uint64 {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.lseq
}

// Set last seq.
// Write lock should be held.
func (mset *stream) setLastSeq(lseq uint64) {
	mset.lseq = lseq
}

func (mset *stream) sendCreateAdvisory() {
	mset.mu.RLock()
	name := mset.cfg.Name
	template := mset.cfg.Template
	outq := mset.outq
	srv := mset.srv
	mset.mu.RUnlock()

	if outq == nil {
		return
	}

	// finally send an event that this stream was created
	m := JSStreamActionAdvisory{
		TypedEvent: TypedEvent{
			Type: JSStreamActionAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   name,
		Action:   CreateEvent,
		Template: template,
		Domain:   srv.getOpts().JetStreamDomain,
	}

	j, err := json.Marshal(m)
	if err != nil {
		return
	}

	subj := JSAdvisoryStreamCreatedPre + "." + name
	outq.sendMsg(subj, j)
}

func (mset *stream) sendDeleteAdvisoryLocked() {
	if mset.outq == nil {
		return
	}

	m := JSStreamActionAdvisory{
		TypedEvent: TypedEvent{
			Type: JSStreamActionAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   mset.cfg.Name,
		Action:   DeleteEvent,
		Template: mset.cfg.Template,
		Domain:   mset.srv.getOpts().JetStreamDomain,
	}

	j, err := json.Marshal(m)
	if err == nil {
		subj := JSAdvisoryStreamDeletedPre + "." + mset.cfg.Name
		mset.outq.sendMsg(subj, j)
	}
}

func (mset *stream) sendUpdateAdvisoryLocked() {
	if mset.outq == nil {
		return
	}

	m := JSStreamActionAdvisory{
		TypedEvent: TypedEvent{
			Type: JSStreamActionAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream: mset.cfg.Name,
		Action: ModifyEvent,
		Domain: mset.srv.getOpts().JetStreamDomain,
	}

	j, err := json.Marshal(m)
	if err == nil {
		subj := JSAdvisoryStreamUpdatedPre + "." + mset.cfg.Name
		mset.outq.sendMsg(subj, j)
	}
}

// Created returns created time.
func (mset *stream) createdTime() time.Time {
	mset.mu.RLock()
	created := mset.created
	mset.mu.RUnlock()
	return created
}

// Internal to allow creation time to be restored.
func (mset *stream) setCreatedTime(created time.Time) {
	mset.mu.Lock()
	mset.created = created
	mset.mu.Unlock()
}

// subjectsOverlap to see if these subjects overlap with existing subjects.
// Use only for non-clustered JetStream
// RLock minimum should be held.
func (jsa *jsAccount) subjectsOverlap(subjects []string, self *stream) bool {
	for _, mset := range jsa.streams {
		if self != nil && mset == self {
			continue
		}
		for _, subj := range mset.cfg.Subjects {
			for _, tsubj := range subjects {
				if SubjectsCollide(tsubj, subj) {
					return true
				}
			}
		}
	}
	return false
}

// StreamDefaultDuplicatesWindow default duplicates window.
const StreamDefaultDuplicatesWindow = 2 * time.Minute

func (s *Server) checkStreamCfg(config *StreamConfig, acc *Account, pedantic bool) (StreamConfig, *ApiError) {
	lim := &s.getOpts().JetStreamLimits

	if config == nil {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration invalid"))
	}
	if !isValidName(config.Name) {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("stream name is required and can not contain '.', '*', '>'"))
	}
	if len(config.Name) > JSMaxNameLen {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("stream name is too long, maximum allowed is %d", JSMaxNameLen))
	}
	if len(config.Description) > JSMaxDescriptionLen {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("stream description is too long, maximum allowed is %d", JSMaxDescriptionLen))
	}

	var metadataLen int
	for k, v := range config.Metadata {
		metadataLen += len(k) + len(v)
	}
	if metadataLen > JSMaxMetadataLen {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("stream metadata exceeds maximum size of %d bytes", JSMaxMetadataLen))
	}

	cfg := *config

	// Make file the default.
	if cfg.Storage == 0 {
		cfg.Storage = FileStorage
	}
	if cfg.Replicas == 0 {
		cfg.Replicas = 1
	}
	if cfg.Replicas > StreamMaxReplicas {
		return cfg, NewJSStreamInvalidConfigError(fmt.Errorf("maximum replicas is %d", StreamMaxReplicas))
	}
	if cfg.Replicas < 0 {
		return cfg, NewJSReplicasCountCannotBeNegativeError()
	}
	if cfg.MaxMsgs == 0 {
		cfg.MaxMsgs = -1
	}
	if cfg.MaxMsgsPer == 0 {
		cfg.MaxMsgsPer = -1
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = -1
	}
	if cfg.MaxMsgSize == 0 {
		cfg.MaxMsgSize = -1
	}
	if cfg.MaxConsumers == 0 {
		cfg.MaxConsumers = -1
	}
	if cfg.Duplicates == 0 && cfg.Mirror == nil {
		maxWindow := StreamDefaultDuplicatesWindow
		if lim.Duplicates > 0 && maxWindow > lim.Duplicates {
			if pedantic {
				return StreamConfig{}, NewJSPedanticError(fmt.Errorf("pedantic mode: duplicate window limits are higher than current limits"))
			}
			maxWindow = lim.Duplicates
		}
		if cfg.MaxAge != 0 && cfg.MaxAge < maxWindow {
			if pedantic {
				return StreamConfig{}, NewJSPedanticError(fmt.Errorf("pedantic mode: duplicate window cannot be bigger than max age"))
			}
			cfg.Duplicates = cfg.MaxAge
		} else {
			cfg.Duplicates = maxWindow
		}
	}
	if cfg.MaxAge > 0 && cfg.MaxAge < 100*time.Millisecond {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("max age needs to be >= 100ms"))
	}
	if cfg.Duplicates < 0 {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("duplicates window can not be negative"))
	}
	// Check that duplicates is not larger then age if set.
	if cfg.MaxAge != 0 && cfg.Duplicates > cfg.MaxAge {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("duplicates window can not be larger then max age"))
	}
	if lim.Duplicates > 0 && cfg.Duplicates > lim.Duplicates {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("duplicates window can not be larger then server limit of %v",
			lim.Duplicates.String()))
	}
	if cfg.Duplicates > 0 && cfg.Duplicates < 100*time.Millisecond {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("duplicates window needs to be >= 100ms"))
	}

	if cfg.DenyPurge && cfg.AllowRollup {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("roll-ups require the purge permission"))
	}

	// Check for new discard new per subject, we require the discard policy to also be new.
	if cfg.DiscardNewPer {
		if cfg.Discard != DiscardNew {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("discard new per subject requires discard new policy to be set"))
		}
		if cfg.MaxMsgsPer <= 0 {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("discard new per subject requires max msgs per subject > 0"))
		}
	}

	if cfg.SubjectDeleteMarkerTTL > 0 {
		if !cfg.AllowMsgTTL {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("subject marker delete cannot be set if message TTLs are disabled"))
		}
		if cfg.SubjectDeleteMarkerTTL < time.Second {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("subject marker delete TTL must be at least 1 second"))
		}
	} else if cfg.SubjectDeleteMarkerTTL < 0 {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("subject marker delete TTL must not be negative"))
	}

	getStream := func(streamName string) (bool, StreamConfig) {
		var exists bool
		var cfg StreamConfig
		if s.JetStreamIsClustered() {
			if js, _ := s.getJetStreamCluster(); js != nil {
				js.mu.RLock()
				if sa := js.streamAssignment(acc.Name, streamName); sa != nil {
					cfg = *sa.Config.clone()
					exists = true
				}
				js.mu.RUnlock()
			}
		} else if mset, err := acc.lookupStream(streamName); err == nil {
			cfg = mset.cfg
			exists = true
		}
		return exists, cfg
	}

	hasStream := func(streamName string) (bool, int32, []string) {
		exists, cfg := getStream(streamName)
		return exists, cfg.MaxMsgSize, cfg.Subjects
	}

	var streamSubs []string
	var deliveryPrefixes []string
	var apiPrefixes []string

	// Do some pre-checking for mirror config to avoid cycles in clustered mode.
	if cfg.Mirror != nil {
		if cfg.FirstSeq > 0 {
			return StreamConfig{}, NewJSMirrorWithFirstSeqError()
		}
		if len(cfg.Subjects) > 0 {
			return StreamConfig{}, NewJSMirrorWithSubjectsError()
		}
		if len(cfg.Sources) > 0 {
			return StreamConfig{}, NewJSMirrorWithSourcesError()
		}
		if cfg.Mirror.FilterSubject != _EMPTY_ && len(cfg.Mirror.SubjectTransforms) != 0 {
			return StreamConfig{}, NewJSMirrorMultipleFiltersNotAllowedError()
		}
		if cfg.SubjectDeleteMarkerTTL > 0 {
			// Delete markers cannot be configured on a mirror as it would result in new
			// tombstones which would use up sequence numbers, diverging from the origin
			// stream.
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("subject delete markers forbidden on mirrors"))
		}
		// Check subject filters overlap.
		for outer, tr := range cfg.Mirror.SubjectTransforms {
			if tr.Source != _EMPTY_ && !IsValidSubject(tr.Source) {
				return StreamConfig{}, NewJSMirrorInvalidSubjectFilterError(fmt.Errorf("%w %s", ErrBadSubject, tr.Source))
			}

			err := ValidateMapping(tr.Source, tr.Destination)
			if err != nil {
				return StreamConfig{}, NewJSMirrorInvalidTransformDestinationError(err)
			}

			for inner, innertr := range cfg.Mirror.SubjectTransforms {
				if inner != outer && SubjectsCollide(tr.Source, innertr.Source) {
					return StreamConfig{}, NewJSMirrorOverlappingSubjectFiltersError()
				}
			}
		}
		// Do not perform checks if External is provided, as it could lead to
		// checking against itself (if sourced stream name is the same on different JetStream)
		if cfg.Mirror.External == nil {
			if !isValidName(cfg.Mirror.Name) {
				return StreamConfig{}, NewJSMirrorInvalidStreamNameError()
			}
			// We do not require other stream to exist anymore, but if we can see it check payloads.
			exists, maxMsgSize, subs := hasStream(cfg.Mirror.Name)
			if len(subs) > 0 {
				streamSubs = append(streamSubs, subs...)
			}
			if exists {
				if cfg.MaxMsgSize > 0 && maxMsgSize > 0 && cfg.MaxMsgSize < maxMsgSize {
					return StreamConfig{}, NewJSMirrorMaxMessageSizeTooBigError()
				}
			}
			// Determine if we are inheriting direct gets.
			if exists, ocfg := getStream(cfg.Mirror.Name); exists {
				if pedantic && cfg.MirrorDirect != ocfg.AllowDirect {
					return StreamConfig{}, NewJSPedanticError(fmt.Errorf("origin stream has direct get set, mirror has it disabled"))
				}
				cfg.MirrorDirect = ocfg.AllowDirect
			} else if js := s.getJetStream(); js != nil && js.isClustered() {
				// Could not find it here. If we are clustered we can look it up.
				js.mu.RLock()
				if cc := js.cluster; cc != nil {
					if as := cc.streams[acc.Name]; as != nil {
						if sa := as[cfg.Mirror.Name]; sa != nil {
							if pedantic && cfg.MirrorDirect != sa.Config.AllowDirect {
								js.mu.RUnlock()
								return StreamConfig{}, NewJSPedanticError(fmt.Errorf("origin stream has direct get set, mirror has it disabled"))
							}
							cfg.MirrorDirect = sa.Config.AllowDirect
						}
					}
				}
				js.mu.RUnlock()
			}
		} else {
			if cfg.Mirror.External.DeliverPrefix != _EMPTY_ {
				deliveryPrefixes = append(deliveryPrefixes, cfg.Mirror.External.DeliverPrefix)
			}

			if cfg.Mirror.External.ApiPrefix != _EMPTY_ {
				apiPrefixes = append(apiPrefixes, cfg.Mirror.External.ApiPrefix)
			}
		}
	}

	// check for duplicates
	var iNames = make(map[string]struct{})
	for _, src := range cfg.Sources {
		if !isValidName(src.Name) {
			return StreamConfig{}, NewJSSourceInvalidStreamNameError()
		}
		if _, ok := iNames[src.composeIName()]; !ok {
			iNames[src.composeIName()] = struct{}{}
		} else {
			return StreamConfig{}, NewJSSourceDuplicateDetectedError()
		}
		// Do not perform checks if External is provided, as it could lead to
		// checking against itself (if sourced stream name is the same on different JetStream)
		if src.External == nil {
			exists, maxMsgSize, subs := hasStream(src.Name)
			if len(subs) > 0 {
				streamSubs = append(streamSubs, subs...)
			}
			if exists {
				if cfg.MaxMsgSize > 0 && maxMsgSize > 0 && cfg.MaxMsgSize < maxMsgSize {
					return StreamConfig{}, NewJSSourceMaxMessageSizeTooBigError()
				}
			}

			if src.FilterSubject != _EMPTY_ && len(src.SubjectTransforms) != 0 {
				return StreamConfig{}, NewJSSourceMultipleFiltersNotAllowedError()
			}

			for _, tr := range src.SubjectTransforms {
				if tr.Source != _EMPTY_ && !IsValidSubject(tr.Source) {
					return StreamConfig{}, NewJSSourceInvalidSubjectFilterError(fmt.Errorf("%w %s", ErrBadSubject, tr.Source))
				}

				err := ValidateMapping(tr.Source, tr.Destination)
				if err != nil {
					return StreamConfig{}, NewJSSourceInvalidTransformDestinationError(err)
				}
			}

			// Check subject filters overlap.
			for outer, tr := range src.SubjectTransforms {
				for inner, innertr := range src.SubjectTransforms {
					if inner != outer && subjectIsSubsetMatch(tr.Source, innertr.Source) {
						return StreamConfig{}, NewJSSourceOverlappingSubjectFiltersError()
					}
				}
			}
			continue
		} else {
			if src.External.DeliverPrefix != _EMPTY_ {
				deliveryPrefixes = append(deliveryPrefixes, src.External.DeliverPrefix)
			}
			if src.External.ApiPrefix != _EMPTY_ {
				apiPrefixes = append(apiPrefixes, src.External.ApiPrefix)
			}
		}
	}

	// check prefix overlap with subjects
	for _, pfx := range deliveryPrefixes {
		if !IsValidPublishSubject(pfx) {
			return StreamConfig{}, NewJSStreamInvalidExternalDeliverySubjError(pfx)
		}
		for _, sub := range streamSubs {
			if SubjectsCollide(sub, fmt.Sprintf("%s.%s", pfx, sub)) {
				return StreamConfig{}, NewJSStreamExternalDelPrefixOverlapsError(pfx, sub)
			}
		}
	}
	// check if api prefixes overlap
	for _, apiPfx := range apiPrefixes {
		if !IsValidPublishSubject(apiPfx) {
			return StreamConfig{}, NewJSStreamInvalidConfigError(
				fmt.Errorf("stream external api prefix %q must be a valid subject without wildcards", apiPfx))
		}
		if SubjectsCollide(apiPfx, JSApiPrefix) {
			return StreamConfig{}, NewJSStreamExternalApiOverlapError(apiPfx, JSApiPrefix)
		}
	}

	// cycle check for source cycle
	toVisit := []*StreamConfig{&cfg}
	visited := make(map[string]struct{})
	overlaps := func(subjects []string, filter string) bool {
		if filter == _EMPTY_ {
			return true
		}
		for _, subject := range subjects {
			if SubjectsCollide(subject, filter) {
				return true
			}
		}
		return false
	}

	for len(toVisit) > 0 {
		cfg := toVisit[0]
		toVisit = toVisit[1:]
		visited[cfg.Name] = struct{}{}
		for _, src := range cfg.Sources {
			if src.External != nil {
				continue
			}
			// We can detect a cycle between streams, but let's double check that the
			// subjects actually form a cycle.
			if _, ok := visited[src.Name]; ok {
				if overlaps(cfg.Subjects, src.FilterSubject) {
					return StreamConfig{}, NewJSStreamInvalidConfigError(errors.New("detected cycle"))
				}
			} else if exists, cfg := getStream(src.Name); exists {
				toVisit = append(toVisit, &cfg)
			}
		}
		// Avoid cycles hiding behind mirrors
		if m := cfg.Mirror; m != nil {
			if m.External == nil {
				if _, ok := visited[m.Name]; ok {
					return StreamConfig{}, NewJSStreamInvalidConfigError(errors.New("detected cycle"))
				}
				if exists, cfg := getStream(m.Name); exists {
					toVisit = append(toVisit, &cfg)
				}
			}
		}
	}

	if len(cfg.Subjects) == 0 {
		if cfg.Mirror == nil && len(cfg.Sources) == 0 {
			cfg.Subjects = append(cfg.Subjects, cfg.Name)
		}
	} else {
		if cfg.Mirror != nil {
			return StreamConfig{}, NewJSMirrorWithSubjectsError()
		}

		// Check for literal duplication of subject interest in config
		// and no overlap with any JS or SYS API subject space.
		dset := make(map[string]struct{}, len(cfg.Subjects))
		for _, subj := range cfg.Subjects {
			// Make sure the subject is valid. Check this first.
			if !IsValidSubject(subj) {
				return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("invalid subject"))
			}
			if _, ok := dset[subj]; ok {
				return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("duplicate subjects detected"))
			}
			// Check for trying to capture everything.
			if subj == fwcs {
				if !cfg.NoAck {
					return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("capturing all subjects requires no-ack to be true"))
				}
				// Capturing everything also will require R1.
				if cfg.Replicas != 1 {
					return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("capturing all subjects requires replicas of 1"))
				}
			}
			// Also check to make sure we do not overlap with our $JS API subjects.
			if !cfg.NoAck && (subjectIsSubsetMatch(subj, "$JS.>") || subjectIsSubsetMatch(subj, "$JSC.>")) {
				// We allow an exception for $JS.EVENT.> since these could have been created in the past.
				if !subjectIsSubsetMatch(subj, "$JS.EVENT.>") {
					return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("subjects that overlap with jetstream api require no-ack to be true"))
				}
			}
			// And the $SYS subjects.
			if !cfg.NoAck && subjectIsSubsetMatch(subj, "$SYS.>") {
				if !subjectIsSubsetMatch(subj, "$SYS.ACCOUNT.>") {
					return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("subjects that overlap with system api require no-ack to be true"))
				}
			}
			// Mark for duplicate check.
			dset[subj] = struct{}{}
		}
	}

	if len(cfg.Subjects) == 0 && len(cfg.Sources) == 0 && cfg.Mirror == nil {
		return StreamConfig{}, NewJSStreamInvalidConfigError(
			fmt.Errorf("stream needs at least one configured subject or be a source/mirror"))
	}

	// Check for MaxBytes required and it's limit
	if required, limit := acc.maxBytesLimits(&cfg); required && cfg.MaxBytes <= 0 {
		return StreamConfig{}, NewJSStreamMaxBytesRequiredError()
	} else if limit > 0 && cfg.MaxBytes > limit {
		return StreamConfig{}, NewJSStreamMaxStreamBytesExceededError()
	}

	// Now check if we have multiple subjects they we do not overlap ourselves
	// which would cause duplicate entries (assuming no MsgID).
	if len(cfg.Subjects) > 1 {
		for _, subj := range cfg.Subjects {
			for _, tsubj := range cfg.Subjects {
				if tsubj != subj && SubjectsCollide(tsubj, subj) {
					return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("subject %q overlaps with %q", subj, tsubj))
				}
			}
		}
	}

	// If we have a republish directive check if we can create a transform here.
	if cfg.RePublish != nil {
		// Check to make sure source is a valid subset of the subjects we have.
		// Also make sure it does not form a cycle.
		// Empty same as all.
		if cfg.RePublish.Source == _EMPTY_ {
			if pedantic {
				return StreamConfig{}, NewJSPedanticError(fmt.Errorf("pedantic mode: republish source can not be empty"))
			}
			cfg.RePublish.Source = fwcs
		}
		var formsCycle bool
		for _, subj := range cfg.Subjects {
			if SubjectsCollide(cfg.RePublish.Destination, subj) {
				formsCycle = true
				break
			}
		}
		if formsCycle {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration for republish destination forms a cycle"))
		}
		if _, err := NewSubjectTransform(cfg.RePublish.Source, cfg.RePublish.Destination); err != nil {
			return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration for republish with transform from '%s' to '%s' not valid", cfg.RePublish.Source, cfg.RePublish.Destination))
		}
	}

	// Check the subject transform if any
	if cfg.SubjectTransform != nil {
		if cfg.SubjectTransform.Source != _EMPTY_ && !IsValidSubject(cfg.SubjectTransform.Source) {
			return StreamConfig{}, NewJSStreamTransformInvalidSourceError(fmt.Errorf("%w %s", ErrBadSubject, cfg.SubjectTransform.Source))
		}

		err := ValidateMapping(cfg.SubjectTransform.Source, cfg.SubjectTransform.Destination)
		if err != nil {
			return StreamConfig{}, NewJSStreamTransformInvalidDestinationError(err)
		}
	}

	// For now don't allow preferred server in placement.
	if cfg.Placement != nil && cfg.Placement.Preferred != _EMPTY_ {
		return StreamConfig{}, NewJSStreamInvalidConfigError(fmt.Errorf("preferred server not permitted in placement"))
	}

	return cfg, nil
}

// Config returns the stream's configuration.
func (mset *stream) config() StreamConfig {
	mset.cfgMu.RLock()
	defer mset.cfgMu.RUnlock()
	return mset.cfg
}

func (mset *stream) fileStoreConfig() (FileStoreConfig, error) {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	fs, ok := mset.store.(*fileStore)
	if !ok {
		return FileStoreConfig{}, ErrStoreWrongType
	}
	return fs.fileStoreConfig(), nil
}

// Do not hold jsAccount or jetStream lock
func (jsa *jsAccount) configUpdateCheck(old, new *StreamConfig, s *Server, pedantic bool) (*StreamConfig, error) {
	cfg, apiErr := s.checkStreamCfg(new, jsa.acc(), pedantic)
	if apiErr != nil {
		return nil, apiErr
	}

	// Name must match.
	if cfg.Name != old.Name {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration name must match original"))
	}
	// Can't change MaxConsumers for now.
	if cfg.MaxConsumers != old.MaxConsumers {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not change MaxConsumers"))
	}
	// Can't change storage types.
	if cfg.Storage != old.Storage {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not change storage type"))
	}
	// Can only change retention from limits to interest or back, not to/from work queue for now.
	if cfg.Retention != old.Retention {
		if old.Retention == WorkQueuePolicy || cfg.Retention == WorkQueuePolicy {
			return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not change retention policy to/from workqueue"))
		}
	}
	// Can not have a template owner for now.
	if old.Template != _EMPTY_ {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update not allowed on template owned stream"))
	}
	if cfg.Template != _EMPTY_ {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not be owned by a template"))
	}
	// Can not change from true to false.
	if !cfg.Sealed && old.Sealed {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not unseal a sealed stream"))
	}
	// Can not change from true to false.
	if !cfg.DenyDelete && old.DenyDelete {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not cancel deny message deletes"))
	}
	// Can not change from true to false.
	if !cfg.DenyPurge && old.DenyPurge {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not cancel deny purge"))
	}
	// Check for mirror changes which are not allowed.
	if !reflect.DeepEqual(cfg.Mirror, old.Mirror) {
		return nil, NewJSStreamMirrorNotUpdatableError()
	}

	// Check on new discard new per subject.
	if cfg.DiscardNewPer {
		if cfg.Discard != DiscardNew {
			return nil, NewJSStreamInvalidConfigError(fmt.Errorf("discard new per subject requires discard new policy to be set"))
		}
		if cfg.MaxMsgsPer <= 0 {
			return nil, NewJSStreamInvalidConfigError(fmt.Errorf("discard new per subject requires max msgs per subject > 0"))
		}
	}

	// Check on the allowed message TTL status.
	if cfg.AllowMsgTTL != old.AllowMsgTTL {
		return nil, NewJSStreamInvalidConfigError(fmt.Errorf("message TTL status can not be changed after stream creation"))
	}

	// Do some adjustments for being sealed.
	// Pedantic mode will allow those changes to be made, as they are determinictic and important to get a sealed stream.
	if cfg.Sealed {
		cfg.MaxAge = 0
		cfg.Discard = DiscardNew
		cfg.DenyDelete, cfg.DenyPurge = true, true
		cfg.AllowRollup = false
	}

	// Check limits. We need some extra handling to allow updating MaxBytes.

	// First, let's calculate the difference between the new and old MaxBytes.
	maxBytesDiff := cfg.MaxBytes - old.MaxBytes
	if maxBytesDiff < 0 {
		// If we're updating to a lower MaxBytes (maxBytesDiff is negative),
		// then set to zero so checkBytesLimits doesn't set addBytes to 1.
		maxBytesDiff = 0
	}
	// If maxBytesDiff == 0, then that means MaxBytes didn't change.
	// If maxBytesDiff > 0, then we want to reserve additional bytes.

	// Save the user configured MaxBytes.
	newMaxBytes := cfg.MaxBytes
	maxBytesOffset := int64(0)

	// We temporarily set cfg.MaxBytes to maxBytesDiff because checkAllLimits
	// adds cfg.MaxBytes to the current reserved limit and checks if we've gone
	// over. However, we don't want an addition cfg.MaxBytes, we only want to
	// reserve the difference between the new and the old values.
	cfg.MaxBytes = maxBytesDiff

	// Check limits.
	js, isClustered := jsa.jetStreamAndClustered()
	jsa.mu.RLock()
	acc := jsa.account
	jsa.usageMu.RLock()
	selected, tier, hasTier := jsa.selectLimits(cfg.Replicas)
	if !hasTier && old.Replicas != cfg.Replicas {
		selected, tier, hasTier = jsa.selectLimits(old.Replicas)
	}
	jsa.usageMu.RUnlock()
	reserved := int64(0)
	if !isClustered {
		reserved = jsa.tieredReservation(tier, &cfg)
	}
	jsa.mu.RUnlock()
	if !hasTier {
		return nil, NewJSNoLimitsError()
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	if isClustered {
		_, reserved = tieredStreamAndReservationCount(js.cluster.streams[acc.Name], tier, &cfg)
	}
	// reservation does not account for this stream, hence add the old value
	if tier == _EMPTY_ && old.Replicas > 1 {
		reserved += old.MaxBytes * int64(old.Replicas)
	} else {
		reserved += old.MaxBytes
	}
	if err := js.checkAllLimits(&selected, &cfg, reserved, maxBytesOffset); err != nil {
		return nil, err
	}
	// Restore the user configured MaxBytes.
	cfg.MaxBytes = newMaxBytes
	return &cfg, nil
}

// Update will allow certain configuration properties of an existing stream to be updated.
func (mset *stream) update(config *StreamConfig) error {
	return mset.updateWithAdvisory(config, true, false)
}

func (mset *stream) updatePedantic(config *StreamConfig, pedantic bool) error {
	return mset.updateWithAdvisory(config, true, pedantic)
}

// Update will allow certain configuration properties of an existing stream to be updated.
func (mset *stream) updateWithAdvisory(config *StreamConfig, sendAdvisory bool, pedantic bool) error {
	_, jsa, err := mset.acc.checkForJetStream()
	if err != nil {
		return err
	}

	mset.mu.RLock()
	ocfg := mset.cfg
	s := mset.srv
	mset.mu.RUnlock()

	cfg, err := mset.jsa.configUpdateCheck(&ocfg, config, s, pedantic)
	if err != nil {
		return NewJSStreamInvalidConfigError(err, Unless(err))
	}

	// In the event that some of the stream-level limits have changed, yell appropriately
	// if any of the consumers exceed that limit.
	updateLimits := ocfg.ConsumerLimits.InactiveThreshold != cfg.ConsumerLimits.InactiveThreshold ||
		ocfg.ConsumerLimits.MaxAckPending != cfg.ConsumerLimits.MaxAckPending
	if updateLimits {
		var errorConsumers []string
		consumers := map[string]*ConsumerConfig{}
		if mset.js.isClustered() {
			for _, c := range mset.sa.consumers {
				consumers[c.Name] = c.Config
			}
		} else {
			for _, c := range mset.consumers {
				consumers[c.name] = &c.cfg
			}
		}
		for name, ccfg := range consumers {
			if ccfg.InactiveThreshold > cfg.ConsumerLimits.InactiveThreshold ||
				ccfg.MaxAckPending > cfg.ConsumerLimits.MaxAckPending {
				errorConsumers = append(errorConsumers, name)
			}
		}
		if len(errorConsumers) > 0 {
			// TODO(nat): Return a parsable error so that we can surface something
			// sensible through the JS API.
			return fmt.Errorf("change to limits violates consumers: %s", strings.Join(errorConsumers, ", "))
		}
	}

	jsa.mu.RLock()
	if jsa.subjectsOverlap(cfg.Subjects, mset) {
		jsa.mu.RUnlock()
		return NewJSStreamSubjectOverlapError()
	}
	jsa.mu.RUnlock()

	mset.mu.Lock()
	if mset.isLeader() {
		// Now check for subject interest differences.
		current := make(map[string]struct{}, len(ocfg.Subjects))
		for _, s := range ocfg.Subjects {
			current[s] = struct{}{}
		}
		// Update config with new values. The store update will enforce any stricter limits.

		// Now walk new subjects. All of these need to be added, but we will check
		// the originals first, since if it is in there we can skip, already added.
		for _, s := range cfg.Subjects {
			if _, ok := current[s]; !ok {
				if _, err := mset.subscribeInternal(s, mset.processInboundJetStreamMsg); err != nil {
					mset.mu.Unlock()
					return err
				}
			}
			delete(current, s)
		}
		// What is left in current needs to be deleted.
		for s := range current {
			if err := mset.unsubscribeInternal(s); err != nil {
				mset.mu.Unlock()
				return err
			}
		}

		// Check for the Duplicates
		if cfg.Duplicates != ocfg.Duplicates && mset.ddtmr != nil {
			// Let it fire right away, it will adjust properly on purge.
			mset.ddtmr.Reset(time.Microsecond)
		}

		// Check for Sources.
		if len(cfg.Sources) > 0 || len(ocfg.Sources) > 0 {
			currentIName := make(map[string]struct{})
			needsStartingSeqNum := make(map[string]struct{})

			for _, s := range ocfg.Sources {
				currentIName[s.iname] = struct{}{}
			}
			for _, s := range cfg.Sources {
				s.setIndexName()
				if _, ok := currentIName[s.iname]; !ok {
					// new source
					if mset.sources == nil {
						mset.sources = make(map[string]*sourceInfo)
					}
					mset.cfg.Sources = append(mset.cfg.Sources, s)

					var si *sourceInfo

					if len(s.SubjectTransforms) == 0 {
						si = &sourceInfo{name: s.Name, iname: s.iname, sf: s.FilterSubject}
					} else {
						si = &sourceInfo{name: s.Name, iname: s.iname}
						si.trs = make([]*subjectTransform, len(s.SubjectTransforms))
						si.sfs = make([]string, len(s.SubjectTransforms))
						for i := range s.SubjectTransforms {
							// err can be ignored as already validated in config check
							si.sfs[i] = s.SubjectTransforms[i].Source
							var err error
							si.trs[i], err = NewSubjectTransform(s.SubjectTransforms[i].Source, s.SubjectTransforms[i].Destination)
							if err != nil {
								mset.mu.Unlock()
								return fmt.Errorf("unable to get subject transform for source: %v", err)
							}
						}
					}

					mset.sources[s.iname] = si
					needsStartingSeqNum[s.iname] = struct{}{}
				} else {
					// source already exists
					delete(currentIName, s.iname)
				}
			}
			// What is left in currentIName needs to be deleted.
			for iName := range currentIName {
				mset.cancelSourceConsumer(iName)
				delete(mset.sources, iName)
			}
			neededCopy := make(map[string]struct{}, len(needsStartingSeqNum))
			for iName := range needsStartingSeqNum {
				neededCopy[iName] = struct{}{}
			}
			mset.setStartingSequenceForSources(needsStartingSeqNum)
			for iName := range neededCopy {
				mset.setupSourceConsumer(iName, mset.sources[iName].sseq+1, time.Time{})
			}
		}
	}

	// Check for a change in allow direct status.
	// These will run on all members, so just update as appropriate here.
	// We do make sure we are caught up under monitorStream() during initial startup.
	if cfg.AllowDirect != ocfg.AllowDirect {
		if cfg.AllowDirect {
			mset.subscribeToDirect()
		} else {
			mset.unsubscribeToDirect()
		}
	}

	// Check for changes to RePublish.
	if cfg.RePublish != nil {
		// Empty same as all.
		if cfg.RePublish.Source == _EMPTY_ {
			cfg.RePublish.Source = fwcs
		}
		if cfg.RePublish.Destination == _EMPTY_ {
			cfg.RePublish.Destination = fwcs
		}
		tr, err := NewSubjectTransform(cfg.RePublish.Source, cfg.RePublish.Destination)
		if err != nil {
			mset.mu.Unlock()
			return fmt.Errorf("stream configuration for republish from '%s' to '%s': %w", cfg.RePublish.Source, cfg.RePublish.Destination, err)
		}
		// Assign our transform for republishing.
		mset.tr = tr
	} else {
		mset.tr = nil
	}

	// Check for changes to subject transform
	if ocfg.SubjectTransform == nil && cfg.SubjectTransform != nil {
		tr, err := NewSubjectTransform(cfg.SubjectTransform.Source, cfg.SubjectTransform.Destination)
		if err != nil {
			mset.mu.Unlock()
			return fmt.Errorf("stream configuration for subject transform from '%s' to '%s': %w", cfg.SubjectTransform.Source, cfg.SubjectTransform.Destination, err)
		}
		mset.itr = tr
	} else if ocfg.SubjectTransform != nil && cfg.SubjectTransform != nil &&
		(ocfg.SubjectTransform.Source != cfg.SubjectTransform.Source || ocfg.SubjectTransform.Destination != cfg.SubjectTransform.Destination) {
		tr, err := NewSubjectTransform(cfg.SubjectTransform.Source, cfg.SubjectTransform.Destination)
		if err != nil {
			mset.mu.Unlock()
			return fmt.Errorf("stream configuration for subject transform from '%s' to '%s': %w", cfg.SubjectTransform.Source, cfg.SubjectTransform.Destination, err)
		}
		mset.itr = tr
	} else if ocfg.SubjectTransform != nil && cfg.SubjectTransform == nil {
		mset.itr = nil
	}

	js := mset.js

	if targetTier := tierName(cfg.Replicas); mset.tier != targetTier {
		// In cases such as R1->R3, only one update is needed
		jsa.usageMu.RLock()
		_, ok := jsa.limits[targetTier]
		jsa.usageMu.RUnlock()
		if ok {
			// error never set
			_, reported, _ := mset.store.Utilization()
			jsa.updateUsage(mset.tier, mset.stype, -int64(reported))
			jsa.updateUsage(targetTier, mset.stype, int64(reported))
			mset.tier = targetTier
		}
		// else in case the new tier does not exist (say on move), keep the old tier around
		// a subsequent update to an existing tier will then move from existing past tier to existing new tier
	}

	if mset.isLeader() && mset.sa != nil && ocfg.Retention != cfg.Retention && cfg.Retention == InterestPolicy {
		// Before we can update the retention policy for the consumer, we need
		// the replica count of all consumers to match the stream.
		for _, c := range mset.sa.consumers {
			if c.Config.Replicas > 0 && c.Config.Replicas != cfg.Replicas {
				mset.mu.Unlock()
				return fmt.Errorf("consumer %q replica count must be %d", c.Name, cfg.Replicas)
			}
		}
	}

	// Now update config and store's version of our config.
	// Although we are under the stream write lock, we will also assign the new
	// configuration under mset.cfgMu lock. This is so that in places where
	// mset.mu cannot be acquired (like many cases in consumer.go where code
	// is under the consumer's lock), and the stream's configuration needs to
	// be inspected, one can use mset.cfgMu's read lock to do that safely.
	mset.cfgMu.Lock()
	mset.cfg = *cfg
	mset.cfgMu.Unlock()

	// If we're changing retention and haven't errored because of consumer
	// replicas by now, whip through and update the consumer retention.
	if ocfg.Retention != cfg.Retention && cfg.Retention == InterestPolicy {
		toUpdate := make([]*consumer, 0, len(mset.consumers))
		for _, c := range mset.consumers {
			toUpdate = append(toUpdate, c)
		}
		var ss StreamState
		mset.store.FastState(&ss)
		mset.mu.Unlock()
		for _, c := range toUpdate {
			c.mu.Lock()
			c.retention = cfg.Retention
			c.mu.Unlock()
			if c.retention == InterestPolicy {
				// If we're switching to interest, force a check of the
				// interest of existing stream messages.
				c.checkStateForInterestStream(&ss)
			}
		}
		mset.mu.Lock()
	}

	// If we are the leader never suppress update advisory, simply send.
	if mset.isLeader() && sendAdvisory {
		mset.sendUpdateAdvisoryLocked()
	}
	mset.mu.Unlock()

	if js != nil {
		maxBytesDiff := cfg.MaxBytes - ocfg.MaxBytes
		if maxBytesDiff > 0 {
			// Reserve the difference
			js.reserveStreamResources(&StreamConfig{
				MaxBytes: maxBytesDiff,
				Storage:  cfg.Storage,
			})
		} else if maxBytesDiff < 0 {
			// Release the difference
			js.releaseStreamResources(&StreamConfig{
				MaxBytes: -maxBytesDiff,
				Storage:  ocfg.Storage,
			})
		}
	}

	mset.store.UpdateConfig(cfg)

	return nil
}

// Small helper to return the Name field from mset.cfg, protected by
// the mset.cfgMu mutex. This is simply because we have several places
// in consumer.go where we need it.
func (mset *stream) getCfgName() string {
	mset.cfgMu.RLock()
	defer mset.cfgMu.RUnlock()
	return mset.cfg.Name
}

// Purge will remove all messages from the stream and underlying store based on the request.
func (mset *stream) purge(preq *JSApiStreamPurgeRequest) (purged uint64, err error) {
	mset.mu.RLock()
	if mset.closed.Load() {
		mset.mu.RUnlock()
		return 0, errStreamClosed
	}
	if mset.cfg.Sealed {
		mset.mu.RUnlock()
		return 0, errors.New("sealed stream")
	}
	store, mlseq := mset.store, mset.lseq
	mset.mu.RUnlock()

	if preq != nil {
		purged, err = mset.store.PurgeEx(preq.Subject, preq.Sequence, preq.Keep, false /*preq.NoMarkers*/)
	} else {
		purged, err = mset.store.Purge()
	}
	if err != nil {
		return purged, err
	}

	// Grab our stream state.
	var state StreamState
	store.FastState(&state)
	fseq, lseq := state.FirstSeq, state.LastSeq

	mset.mu.Lock()
	// Check if our last has moved past what our original last sequence was, if so reset.
	if lseq > mlseq {
		mset.setLastSeq(lseq)
	}

	// Clear any pending acks below first seq.
	mset.clearAllPreAcksBelowFloor(fseq)
	mset.mu.Unlock()

	// Purge consumers.
	// Check for filtered purge.
	if preq != nil && preq.Subject != _EMPTY_ {
		ss := store.FilteredState(fseq, preq.Subject)
		fseq = ss.First
	}

	mset.clsMu.RLock()
	for _, o := range mset.cList {
		start := fseq
		o.mu.RLock()
		// we update consumer sequences if:
		// no subject was specified, we can purge all consumers sequences
		doPurge := preq == nil ||
			preq.Subject == _EMPTY_ ||
			// consumer filter subject is equal to purged subject
			// or consumer filter subject is subset of purged subject,
			// but not the other way around.
			o.isEqualOrSubsetMatch(preq.Subject)
		// Check if a consumer has a wider subject space then what we purged
		var isWider bool
		if !doPurge && preq != nil && o.isFilteredMatch(preq.Subject) {
			doPurge, isWider = true, true
			start = state.FirstSeq
		}
		o.mu.RUnlock()
		if doPurge {
			o.purge(start, lseq, isWider)
		}
	}
	mset.clsMu.RUnlock()

	return purged, nil
}

// RemoveMsg will remove a message from a stream.
// FIXME(dlc) - Should pick one and be consistent.
func (mset *stream) removeMsg(seq uint64) (bool, error) {
	return mset.deleteMsg(seq)
}

// DeleteMsg will remove a message from a stream.
func (mset *stream) deleteMsg(seq uint64) (bool, error) {
	if mset.closed.Load() {
		return false, errStreamClosed
	}
	removed, err := mset.store.RemoveMsg(seq)
	if err != nil {
		return removed, err
	}
	mset.mu.Lock()
	mset.clearAllPreAcks(seq)
	mset.mu.Unlock()
	return removed, err
}

// EraseMsg will securely remove a message and rewrite the data with random data.
func (mset *stream) eraseMsg(seq uint64) (bool, error) {
	if mset.closed.Load() {
		return false, errStreamClosed
	}
	removed, err := mset.store.EraseMsg(seq)
	if err != nil {
		return removed, err
	}
	mset.mu.Lock()
	mset.clearAllPreAcks(seq)
	mset.mu.Unlock()
	return removed, err
}

// Are we a mirror?
func (mset *stream) isMirror() bool {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.cfg.Mirror != nil
}

func (mset *stream) sourcesInfo() (sis []*StreamSourceInfo) {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	for _, si := range mset.sources {
		sis = append(sis, mset.sourceInfo(si))
	}
	return sis
}

// Lock should be held
func (mset *stream) sourceInfo(si *sourceInfo) *StreamSourceInfo {
	if si == nil {
		return nil
	}

	var ssi = StreamSourceInfo{Name: si.name, Lag: si.lag, Error: si.err, FilterSubject: si.sf}

	trConfigs := make([]SubjectTransformConfig, len(si.sfs))
	for i := range si.sfs {
		destination := _EMPTY_
		if si.trs[i] != nil {
			destination = si.trs[i].dest
		}
		trConfigs[i] = SubjectTransformConfig{si.sfs[i], destination}
	}

	ssi.SubjectTransforms = trConfigs

	// If we have not heard from the source, set Active to -1.
	if last := si.last.Load(); last == 0 {
		ssi.Active = -1
	} else {
		ssi.Active = time.Since(time.Unix(0, last))
	}

	var ext *ExternalStream
	if mset.cfg.Mirror != nil {
		ext = mset.cfg.Mirror.External
	} else if ss := mset.streamSource(si.iname); ss != nil && ss.External != nil {
		ext = ss.External
	}
	if ext != nil {
		ssi.External = &ExternalStream{
			ApiPrefix:     ext.ApiPrefix,
			DeliverPrefix: ext.DeliverPrefix,
		}
	}
	return &ssi
}

// Return our source info for our mirror.
func (mset *stream) mirrorInfo() *StreamSourceInfo {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.sourceInfo(mset.mirror)
}

const (
	// Our consumer HB interval.
	sourceHealthHB = 1 * time.Second
	// How often we check and our stalled interval.
	sourceHealthCheckInterval = 10 * time.Second
)

// Will run as a Go routine to process mirror consumer messages.
func (mset *stream) processMirrorMsgs(mirror *sourceInfo, ready *sync.WaitGroup) {
	s := mset.srv
	defer func() {
		mirror.wg.Done()
		s.grWG.Done()
	}()

	// Grab stream quit channel.
	mset.mu.Lock()
	msgs, qch, siqch := mirror.msgs, mset.qch, mirror.qch
	// Set the last seen as now so that we don't fail at the first check.
	mirror.last.Store(time.Now().UnixNano())
	mset.mu.Unlock()

	// Signal the caller that we have captured the above fields.
	ready.Done()

	// Make sure we have valid ipq for msgs.
	if msgs == nil {
		mset.mu.Lock()
		mset.cancelMirrorConsumer()
		mset.mu.Unlock()
		return
	}

	t := time.NewTicker(sourceHealthCheckInterval)
	defer t.Stop()

	for {
		select {
		case <-s.quitCh:
			return
		case <-qch:
			return
		case <-siqch:
			return
		case <-msgs.ch:
			ims := msgs.pop()
			for _, im := range ims {
				if !mset.processInboundMirrorMsg(im) {
					break
				}
				im.returnToPool()
			}
			msgs.recycle(&ims)
		case <-t.C:
			mset.mu.RLock()
			var stalled bool
			if mset.mirror != nil {
				stalled = time.Since(time.Unix(0, mset.mirror.last.Load())) > sourceHealthCheckInterval
			}
			isLeader := mset.isLeader()
			mset.mu.RUnlock()
			// No longer leader.
			if !isLeader {
				mset.mu.Lock()
				mset.cancelMirrorConsumer()
				mset.mu.Unlock()
				return
			}
			// We are stalled.
			if stalled {
				mset.retryMirrorConsumer()
			}
		}
	}
}

// Checks that the message is from our current direct consumer. We can not depend on sub comparison
// since cross account imports break.
func (si *sourceInfo) isCurrentSub(reply string) bool {
	return si.cname != _EMPTY_ && strings.HasPrefix(reply, jsAckPre) && si.cname == tokenAt(reply, 4)
}

// processInboundMirrorMsg handles processing messages bound for a stream.
func (mset *stream) processInboundMirrorMsg(m *inMsg) bool {
	mset.mu.Lock()
	if mset.mirror == nil {
		mset.mu.Unlock()
		return false
	}
	if !mset.isLeader() {
		mset.cancelMirrorConsumer()
		mset.mu.Unlock()
		return false
	}

	isControl := m.isControlMsg()

	// Ignore from old subscriptions.
	// The reason we can not just compare subs is that on cross account imports they will not match.
	if !mset.mirror.isCurrentSub(m.rply) && !isControl {
		mset.mu.Unlock()
		return false
	}

	// Check for heartbeats and flow control messages.
	if isControl {
		var needsRetry bool
		// Flow controls have reply subjects.
		if m.rply != _EMPTY_ {
			mset.handleFlowControl(m)
		} else {
			// For idle heartbeats make sure we did not miss anything and check if we are considered stalled.
			if ldseq := parseInt64(getHeader(JSLastConsumerSeq, m.hdr)); ldseq > 0 && uint64(ldseq) != mset.mirror.dseq {
				needsRetry = true
			} else if fcReply := getHeader(JSConsumerStalled, m.hdr); len(fcReply) > 0 {
				// Other side thinks we are stalled, so send flow control reply.
				mset.outq.sendMsg(string(fcReply), nil)
			}
		}
		mset.mu.Unlock()
		if needsRetry {
			mset.retryMirrorConsumer()
		}
		return !needsRetry
	}

	sseq, dseq, dc, ts, pending := replyInfo(m.rply)

	if dc > 1 {
		mset.mu.Unlock()
		return false
	}

	// Mirror info tracking.
	olag, osseq, odseq := mset.mirror.lag, mset.mirror.sseq, mset.mirror.dseq
	if sseq == mset.mirror.sseq+1 {
		mset.mirror.dseq = dseq
		mset.mirror.sseq++
	} else if sseq <= mset.mirror.sseq {
		// Ignore older messages.
		mset.mu.Unlock()
		return true
	} else if mset.mirror.cname == _EMPTY_ {
		mset.mirror.cname = tokenAt(m.rply, 4)
		mset.mirror.dseq, mset.mirror.sseq = dseq, sseq
	} else {
		// If the deliver sequence matches then the upstream stream has expired or deleted messages.
		if dseq == mset.mirror.dseq+1 {
			mset.skipMsgs(mset.mirror.sseq+1, sseq-1)
			mset.mirror.dseq++
			mset.mirror.sseq = sseq
		} else {
			mset.mu.Unlock()
			mset.retryMirrorConsumer()
			return false
		}
	}

	if pending == 0 {
		mset.mirror.lag = 0
	} else {
		mset.mirror.lag = pending - 1
	}

	// Check if we allow mirror direct here. If so check they we have mostly caught up.
	// The reason we do not require 0 is if the source is active we may always be slightly behind.
	if mset.cfg.MirrorDirect && mset.mirror.dsub == nil && pending < dgetCaughtUpThresh {
		if err := mset.subscribeToMirrorDirect(); err != nil {
			// Disable since we had problems above.
			mset.cfg.MirrorDirect = false
		}
	}

	// Do the subject transform if there's one
	if len(mset.mirror.trs) > 0 {
		for _, tr := range mset.mirror.trs {
			if tr == nil {
				continue
			} else {
				tsubj, err := tr.Match(m.subj)
				if err == nil {
					m.subj = tsubj
					break
				}
			}
		}
	}

	s, js, stype := mset.srv, mset.js, mset.cfg.Storage
	node := mset.node
	mset.mu.Unlock()

	var err error
	if node != nil {
		if js.limitsExceeded(stype) {
			s.resourcesExceededError()
			err = ApiErrors[JSInsufficientResourcesErr]
		} else {
			err = node.Propose(encodeStreamMsg(m.subj, _EMPTY_, m.hdr, m.msg, sseq-1, ts, true))
		}
	} else {
		err = mset.processJetStreamMsg(m.subj, _EMPTY_, m.hdr, m.msg, sseq-1, ts, nil, true)
	}
	if err != nil {
		if strings.Contains(err.Error(), "no space left") {
			s.Errorf("JetStream out of space, will be DISABLED")
			s.DisableJetStream()
			return false
		}
		if err != errLastSeqMismatch {
			mset.mu.RLock()
			accName, sname := mset.acc.Name, mset.cfg.Name
			mset.mu.RUnlock()
			s.RateLimitWarnf("Error processing inbound mirror message for '%s' > '%s': %v",
				accName, sname, err)
		} else {
			// We may have missed messages, restart.
			if sseq <= mset.lastSeq() {
				mset.mu.Lock()
				mset.mirror.lag = olag
				mset.mirror.sseq = osseq
				mset.mirror.dseq = odseq
				mset.mu.Unlock()
				return false
			} else {
				mset.mu.Lock()
				mset.mirror.dseq = odseq
				mset.mirror.sseq = osseq
				mset.mu.Unlock()
				mset.retryMirrorConsumer()
			}
		}
	}
	return err == nil
}

func (mset *stream) setMirrorErr(err *ApiError) {
	mset.mu.Lock()
	if mset.mirror != nil {
		mset.mirror.err = err
	}
	mset.mu.Unlock()
}

// Cancels a mirror consumer.
//
// Lock held on entry
func (mset *stream) cancelMirrorConsumer() {
	if mset.mirror == nil {
		return
	}
	mset.cancelSourceInfo(mset.mirror)
}

// Similar to setupMirrorConsumer except that it will print a debug statement
// indicating that there is a retry.
//
// Lock is acquired in this function
func (mset *stream) retryMirrorConsumer() error {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	mset.srv.Debugf("Retrying mirror consumer for '%s > %s'", mset.acc.Name, mset.cfg.Name)
	mset.cancelMirrorConsumer()
	return mset.setupMirrorConsumer()
}

// Lock should be held.
func (mset *stream) skipMsgs(start, end uint64) {
	node, store := mset.node, mset.store
	// If we are not clustered we can short circuit now with store.SkipMsgs
	if node == nil {
		store.SkipMsgs(start, end-start+1)
		mset.lseq = end
		return
	}

	// FIXME (dlc) - We should allow proposals of DeleteRange, but would need to make sure all peers support.
	// With syncRequest was easy to add bool into request.
	var entries []*Entry
	for seq := start; seq <= end; seq++ {
		entries = append(entries, newEntry(EntryNormal, encodeStreamMsg(_EMPTY_, _EMPTY_, nil, nil, seq-1, 0, false)))
		// So a single message does not get too big.
		if len(entries) > 10_000 {
			node.ProposeMulti(entries)
			// We need to re-create `entries` because there is a reference
			// to it in the node's pae map.
			entries = entries[:0]
		}
	}
	// Send all at once.
	if len(entries) > 0 {
		node.ProposeMulti(entries)
	}
}

const (
	// Base retry backoff duration.
	retryBackOff = 5 * time.Second
	// Maximum amount we will wait.
	retryMaximum = 2 * time.Minute
)

// Calculate our backoff based on number of failures.
func calculateRetryBackoff(fails int) time.Duration {
	backoff := time.Duration(retryBackOff) * time.Duration(fails*2)
	if backoff > retryMaximum {
		backoff = retryMaximum
	}
	return backoff
}

// This will schedule a call to setupMirrorConsumer, taking into account the last
// time it was retried and determine the soonest setupMirrorConsumer can be called
// without tripping the sourceConsumerRetryThreshold. We will also take into account
// number of failures and will back off our retries.
// The mset.mirror pointer has been verified to be not nil by the caller.
//
// Lock held on entry
func (mset *stream) scheduleSetupMirrorConsumerRetry() {
	// We are trying to figure out how soon we can retry. setupMirrorConsumer will reject
	// a retry if last was done less than "sourceConsumerRetryThreshold" ago.
	next := sourceConsumerRetryThreshold - time.Since(mset.mirror.lreq)
	if next < 0 {
		// It means that we have passed the threshold and so we are ready to go.
		next = 0
	}
	// Take into account failures here.
	next += calculateRetryBackoff(mset.mirror.fails)

	// Add some jitter.
	next += time.Duration(rand.Intn(int(100*time.Millisecond))) + 100*time.Millisecond

	time.AfterFunc(next, func() {
		mset.mu.Lock()
		mset.setupMirrorConsumer()
		mset.mu.Unlock()
	})
}

// How long we wait for a response from a consumer create request for a source or mirror.
var srcConsumerWaitTime = 30 * time.Second

// Setup our mirror consumer.
// Lock should be held.
func (mset *stream) setupMirrorConsumer() error {
	if mset.closed.Load() {
		return errStreamClosed
	}
	if mset.outq == nil {
		return errors.New("outq required")
	}
	// We use to prevent update of a mirror configuration in cluster
	// mode but not in standalone. This is now fixed. However, without
	// rejecting the update, it could be that if the source stream was
	// removed and then later the mirrored stream config changed to
	// remove mirror configuration, this function would panic when
	// accessing mset.cfg.Mirror fields. Adding this protection in case
	// we allow in the future the mirror config to be changed (removed).
	if mset.cfg.Mirror == nil {
		return errors.New("invalid mirror configuration")
	}

	// If this is the first time
	if mset.mirror == nil {
		mset.mirror = &sourceInfo{name: mset.cfg.Mirror.Name}
	} else {
		mset.cancelSourceInfo(mset.mirror)
		mset.mirror.sseq = mset.lseq
	}

	// If we are no longer the leader stop trying.
	if !mset.isLeader() {
		return nil
	}

	mirror := mset.mirror

	// We want to throttle here in terms of how fast we request new consumers,
	// or if the previous is still in progress.
	if last := time.Since(mirror.lreq); last < sourceConsumerRetryThreshold || mirror.sip {
		mset.scheduleSetupMirrorConsumerRetry()
		return nil
	}
	mirror.lreq = time.Now()

	// Determine subjects etc.
	var deliverSubject string
	ext := mset.cfg.Mirror.External

	if ext != nil && ext.DeliverPrefix != _EMPTY_ {
		deliverSubject = strings.ReplaceAll(ext.DeliverPrefix+syncSubject(".M"), "..", ".")
	} else {
		deliverSubject = syncSubject("$JS.M")
	}

	// Now send off request to create/update our consumer. This will be all API based even in single server mode.
	// We calculate durable names apriori so we do not need to save them off.

	var state StreamState
	mset.store.FastState(&state)

	req := &CreateConsumerRequest{
		Stream: mset.cfg.Mirror.Name,
		Config: ConsumerConfig{
			DeliverSubject:    deliverSubject,
			DeliverPolicy:     DeliverByStartSequence,
			OptStartSeq:       state.LastSeq + 1,
			AckPolicy:         AckNone,
			AckWait:           22 * time.Hour,
			MaxDeliver:        1,
			Heartbeat:         sourceHealthHB,
			FlowControl:       true,
			Direct:            true,
			InactiveThreshold: sourceHealthCheckInterval,
		},
	}

	// Only use start optionals on first time.
	if state.Msgs == 0 && state.FirstSeq == 0 {
		req.Config.OptStartSeq = 0
		if mset.cfg.Mirror.OptStartSeq > 0 {
			req.Config.OptStartSeq = mset.cfg.Mirror.OptStartSeq
		} else if mset.cfg.Mirror.OptStartTime != nil {
			req.Config.OptStartTime = mset.cfg.Mirror.OptStartTime
			req.Config.DeliverPolicy = DeliverByStartTime
		}
	}
	if req.Config.OptStartSeq == 0 && req.Config.OptStartTime == nil {
		// If starting out and lastSeq is 0.
		req.Config.DeliverPolicy = DeliverAll
	}

	// Filters
	if mset.cfg.Mirror.FilterSubject != _EMPTY_ {
		req.Config.FilterSubject = mset.cfg.Mirror.FilterSubject
		mirror.sf = mset.cfg.Mirror.FilterSubject
	}

	if lst := len(mset.cfg.Mirror.SubjectTransforms); lst > 0 {
		sfs := make([]string, lst)
		trs := make([]*subjectTransform, lst)

		for i, tr := range mset.cfg.Mirror.SubjectTransforms {
			// will not fail as already checked before that the transform will work
			subjectTransform, err := NewSubjectTransform(tr.Source, tr.Destination)
			if err != nil {
				mset.srv.Errorf("Unable to get transform for mirror consumer: %v", err)
			}
			sfs[i] = tr.Source
			trs[i] = subjectTransform
		}
		mirror.sfs = sfs
		mirror.trs = trs
		req.Config.FilterSubjects = sfs
	}

	respCh := make(chan *JSApiConsumerCreateResponse, 1)
	reply := infoReplySubject()
	crSub, err := mset.subscribeInternal(reply, func(sub *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
		mset.unsubscribe(sub)
		_, msg := c.msgParts(rmsg)

		var ccr JSApiConsumerCreateResponse
		if err := json.Unmarshal(msg, &ccr); err != nil {
			c.Warnf("JetStream bad mirror consumer create response: %q", msg)
			mset.setMirrorErr(ApiErrors[JSInvalidJSONErr])
			return
		}
		select {
		case respCh <- &ccr:
		default:
		}
	})
	if err != nil {
		mirror.err = NewJSMirrorConsumerSetupFailedError(err, Unless(err))
		mset.scheduleSetupMirrorConsumerRetry()
		return nil
	}

	b, _ := json.Marshal(req)

	var subject string
	if req.Config.FilterSubject != _EMPTY_ {
		req.Config.Name = fmt.Sprintf("mirror-%s", createConsumerName())
		subject = fmt.Sprintf(JSApiConsumerCreateExT, mset.cfg.Mirror.Name, req.Config.Name, req.Config.FilterSubject)
	} else {
		subject = fmt.Sprintf(JSApiConsumerCreateT, mset.cfg.Mirror.Name)
	}
	if ext != nil {
		subject = strings.Replace(subject, JSApiPrefix, ext.ApiPrefix, 1)
		subject = strings.ReplaceAll(subject, "..", ".")
	}

	// Reset
	mirror.msgs = nil
	mirror.err = nil
	mirror.sip = true

	// Send the consumer create request
	mset.outq.send(newJSPubMsg(subject, _EMPTY_, reply, nil, b, nil, 0))

	go func() {

		var retry bool
		defer func() {
			mset.mu.Lock()
			// Check that this is still valid and if so, clear the "setup in progress" flag.
			if mset.mirror != nil {
				mset.mirror.sip = false
				// If we need to retry, schedule now
				if retry {
					mset.mirror.fails++
					// Cancel here since we can not do anything with this consumer at this point.
					mset.cancelSourceInfo(mset.mirror)
					mset.scheduleSetupMirrorConsumerRetry()
				} else {
					// Clear on success.
					mset.mirror.fails = 0
				}
			}
			mset.mu.Unlock()
		}()

		// Wait for previous processMirrorMsgs go routine to be completely done.
		// If none is running, this will not block.
		mirror.wg.Wait()

		select {
		case ccr := <-respCh:
			mset.mu.Lock()
			// Mirror config has been removed.
			if mset.mirror == nil {
				mset.mu.Unlock()
				return
			}
			ready := sync.WaitGroup{}
			mirror := mset.mirror
			mirror.err = nil
			if ccr.Error != nil || ccr.ConsumerInfo == nil {
				mset.srv.Warnf("JetStream error response for create mirror consumer: %+v", ccr.Error)
				mirror.err = ccr.Error
				// Let's retry as soon as possible, but we are gated by sourceConsumerRetryThreshold
				retry = true
				mset.mu.Unlock()
				return
			} else {
				// Setup actual subscription to process messages from our source.
				qname := fmt.Sprintf("[ACC:%s] stream mirror '%s' of '%s' msgs", mset.acc.Name, mset.cfg.Name, mset.cfg.Mirror.Name)
				// Create a new queue each time
				mirror.msgs = newIPQueue[*inMsg](mset.srv, qname)
				msgs := mirror.msgs
				sub, err := mset.subscribeInternal(deliverSubject, func(sub *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
					hdr, msg := c.msgParts(copyBytes(rmsg)) // Need to copy.
					mset.queueInbound(msgs, subject, reply, hdr, msg, nil, nil)
					mirror.last.Store(time.Now().UnixNano())
				})
				if err != nil {
					mirror.err = NewJSMirrorConsumerSetupFailedError(err, Unless(err))
					retry = true
					mset.mu.Unlock()
					return
				}
				// Save our sub.
				mirror.sub = sub

				// When an upstream stream expires messages or in general has messages that we want
				// that are no longer available we need to adjust here.
				var state StreamState
				mset.store.FastState(&state)

				// Check if we need to skip messages.
				if state.LastSeq != ccr.ConsumerInfo.Delivered.Stream {
					// Check to see if delivered is past our last and we have no msgs. This will help the
					// case when mirroring a stream that has a very high starting sequence number.
					if state.Msgs == 0 && ccr.ConsumerInfo.Delivered.Stream > state.LastSeq {
						mset.store.PurgeEx(_EMPTY_, ccr.ConsumerInfo.Delivered.Stream+1, 0, true)
						mset.lseq = ccr.ConsumerInfo.Delivered.Stream
					} else {
						mset.skipMsgs(state.LastSeq+1, ccr.ConsumerInfo.Delivered.Stream)
					}
				}

				// Capture consumer name.
				mirror.cname = ccr.ConsumerInfo.Name
				mirror.dseq = 0
				mirror.sseq = ccr.ConsumerInfo.Delivered.Stream
				mirror.qch = make(chan struct{})
				mirror.wg.Add(1)
				ready.Add(1)
				if !mset.srv.startGoRoutine(
					func() { mset.processMirrorMsgs(mirror, &ready) },
					pprofLabels{
						"type":     "mirror",
						"account":  mset.acc.Name,
						"stream":   mset.cfg.Name,
						"consumer": mirror.cname,
					},
				) {
					ready.Done()
				}
			}
			mset.mu.Unlock()
			ready.Wait()
		case <-time.After(srcConsumerWaitTime):
			mset.unsubscribe(crSub)
			// We already waited 30 seconds, let's retry now.
			retry = true
		}
	}()

	return nil
}

func (mset *stream) streamSource(iname string) *StreamSource {
	for _, ssi := range mset.cfg.Sources {
		if ssi.iname == iname {
			return ssi
		}
	}
	return nil
}

// Lock should be held.
func (mset *stream) retrySourceConsumerAtSeq(iName string, seq uint64) {
	s := mset.srv

	s.Debugf("Retrying source consumer for '%s > %s'", mset.acc.Name, mset.cfg.Name)

	// setupSourceConsumer will check that the source is still configured.
	mset.setupSourceConsumer(iName, seq, time.Time{})
}

// Lock should be held.
func (mset *stream) cancelSourceConsumer(iname string) {
	if si := mset.sources[iname]; si != nil {
		mset.cancelSourceInfo(si)
		si.sseq, si.dseq = 0, 0
	}
}

// The `si` has been verified to be not nil. The sourceInfo's sub will
// be unsubscribed and set to nil (if not already done) and the
// cname will be reset. The message processing's go routine quit channel
// will be closed if still opened.
//
// Lock should be held
func (mset *stream) cancelSourceInfo(si *sourceInfo) {
	if si.sub != nil {
		mset.unsubscribe(si.sub)
		si.sub = nil
	}
	// In case we had a mirror direct subscription.
	if si.dsub != nil {
		mset.unsubscribe(si.dsub)
		si.dsub = nil
	}
	mset.removeInternalConsumer(si)
	if si.qch != nil {
		close(si.qch)
		si.qch = nil
	}
	if si.msgs != nil {
		si.msgs.drain()
		si.msgs.unregister()
	}
	// If we have a schedule setup go ahead and delete that.
	if t := mset.sourceSetupSchedules[si.iname]; t != nil {
		t.Stop()
		delete(mset.sourceSetupSchedules, si.iname)
	}
}

const sourceConsumerRetryThreshold = 2 * time.Second

// This is the main function to call when needing to setup a new consumer for the source.
// It actually only does the scheduling of the execution of trySetupSourceConsumer in order to implement retry backoff
// and throttle the number of requests.
// Lock should be held.
func (mset *stream) setupSourceConsumer(iname string, seq uint64, startTime time.Time) {
	if mset.sourceSetupSchedules == nil {
		mset.sourceSetupSchedules = map[string]*time.Timer{}
	}

	if _, ok := mset.sourceSetupSchedules[iname]; ok {
		// If there is already a timer scheduled, we don't need to do anything.
		return
	}

	si := mset.sources[iname]
	if si == nil || si.sip { // if sourceInfo was removed or setup is in progress, nothing to do
		return
	}

	// First calculate the delay until the next time we can
	var scheduleDelay time.Duration

	if !si.lreq.IsZero() { // it's not the very first time we are called, compute the delay
		// We want to throttle here in terms of how fast we request new consumers
		if sinceLast := time.Since(si.lreq); sinceLast < sourceConsumerRetryThreshold {
			scheduleDelay = sourceConsumerRetryThreshold - sinceLast
		}
		// Is it a retry? If so, add a backoff
		if si.fails > 0 {
			scheduleDelay += calculateRetryBackoff(si.fails)
		}
	}

	// Always add some jitter
	scheduleDelay += time.Duration(rand.Intn(int(100*time.Millisecond))) + 100*time.Millisecond

	// Schedule the call to trySetupSourceConsumer
	mset.sourceSetupSchedules[iname] = time.AfterFunc(scheduleDelay, func() {
		mset.mu.Lock()
		defer mset.mu.Unlock()

		delete(mset.sourceSetupSchedules, iname)
		mset.trySetupSourceConsumer(iname, seq, startTime)
	})
}

// This is where we will actually try to create a new consumer for the source
// Lock should be held.
func (mset *stream) trySetupSourceConsumer(iname string, seq uint64, startTime time.Time) {
	// Ignore if closed or not leader.
	if mset.closed.Load() || !mset.isLeader() {
		return
	}

	si := mset.sources[iname]
	if si == nil {
		return
	}

	// Cancel previous instance if applicable
	mset.cancelSourceInfo(si)

	ssi := mset.streamSource(iname)
	if ssi == nil {
		return
	}

	si.lreq = time.Now()

	// Determine subjects etc.
	var deliverSubject string
	ext := ssi.External

	if ext != nil && ext.DeliverPrefix != _EMPTY_ {
		deliverSubject = strings.ReplaceAll(ext.DeliverPrefix+syncSubject(".S"), "..", ".")
	} else {
		deliverSubject = syncSubject("$JS.S")
	}

	req := &CreateConsumerRequest{
		Stream: si.name,
		Config: ConsumerConfig{
			DeliverSubject:    deliverSubject,
			AckPolicy:         AckNone,
			AckWait:           22 * time.Hour,
			MaxDeliver:        1,
			Heartbeat:         sourceHealthHB,
			FlowControl:       true,
			Direct:            true,
			InactiveThreshold: sourceHealthCheckInterval,
		},
	}

	// If starting, check any configs.
	if !startTime.IsZero() && seq > 1 {
		req.Config.OptStartTime = &startTime
		req.Config.DeliverPolicy = DeliverByStartTime
	} else if seq <= 1 {
		if ssi.OptStartSeq > 0 {
			req.Config.OptStartSeq = ssi.OptStartSeq
			req.Config.DeliverPolicy = DeliverByStartSequence
		} else {
			// We have not recovered state so check that configured time is less that our first seq time.
			var state StreamState
			mset.store.FastState(&state)
			if ssi.OptStartTime != nil {
				if !state.LastTime.IsZero() && ssi.OptStartTime.Before(state.LastTime) {
					req.Config.OptStartTime = &state.LastTime
				} else {
					req.Config.OptStartTime = ssi.OptStartTime
				}
				req.Config.DeliverPolicy = DeliverByStartTime
			} else if state.FirstSeq > 1 && !state.LastTime.IsZero() {
				req.Config.OptStartTime = &state.LastTime
				req.Config.DeliverPolicy = DeliverByStartTime
			}
		}

	} else {
		req.Config.OptStartSeq = seq
		req.Config.DeliverPolicy = DeliverByStartSequence
	}
	// Filters
	if ssi.FilterSubject != _EMPTY_ {
		req.Config.FilterSubject = ssi.FilterSubject
	}

	var filterSubjects []string
	for _, tr := range ssi.SubjectTransforms {
		filterSubjects = append(filterSubjects, tr.Source)
	}
	req.Config.FilterSubjects = filterSubjects

	respCh := make(chan *JSApiConsumerCreateResponse, 1)
	reply := infoReplySubject()
	crSub, err := mset.subscribeInternal(reply, func(sub *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
		mset.unsubscribe(sub)
		_, msg := c.msgParts(rmsg)
		var ccr JSApiConsumerCreateResponse
		if err := json.Unmarshal(msg, &ccr); err != nil {
			c.Warnf("JetStream bad source consumer create response: %q", msg)
			return
		}
		select {
		case respCh <- &ccr:
		default:
		}
	})
	if err != nil {
		si.err = NewJSSourceConsumerSetupFailedError(err, Unless(err))
		mset.setupSourceConsumer(iname, seq, startTime)
		return
	}

	var subject string
	if req.Config.FilterSubject != _EMPTY_ {
		req.Config.Name = fmt.Sprintf("src-%s", createConsumerName())
		subject = fmt.Sprintf(JSApiConsumerCreateExT, si.name, req.Config.Name, req.Config.FilterSubject)
	} else if len(req.Config.FilterSubjects) == 1 {
		req.Config.Name = fmt.Sprintf("src-%s", createConsumerName())
		// It is necessary to switch to using FilterSubject here as the extended consumer
		// create API checks for it, so as to not accidentally allow multiple filtered subjects.
		req.Config.FilterSubject = req.Config.FilterSubjects[0]
		req.Config.FilterSubjects = nil
		subject = fmt.Sprintf(JSApiConsumerCreateExT, si.name, req.Config.Name, req.Config.FilterSubject)
	} else {
		subject = fmt.Sprintf(JSApiConsumerCreateT, si.name)
	}
	if ext != nil {
		subject = strings.Replace(subject, JSApiPrefix, ext.ApiPrefix, 1)
		subject = strings.ReplaceAll(subject, "..", ".")
	}

	// Marshal request.
	b, _ := json.Marshal(req)

	// Reset
	si.msgs = nil
	si.err = nil
	si.sip = true

	// Send the consumer create request
	mset.outq.send(newJSPubMsg(subject, _EMPTY_, reply, nil, b, nil, 0))

	go func() {

		var retry bool
		defer func() {
			mset.mu.Lock()
			// Check that this is still valid and if so, clear the "setup in progress" flag.
			if si := mset.sources[iname]; si != nil {
				si.sip = false
				// If we need to retry, schedule now
				if retry {
					si.fails++
					// Cancel here since we can not do anything with this consumer at this point.
					mset.cancelSourceInfo(si)
					mset.setupSourceConsumer(iname, seq, startTime)
				} else {
					// Clear on success.
					si.fails = 0
				}
			}
			mset.mu.Unlock()
		}()

		select {
		case ccr := <-respCh:
			mset.mu.Lock()
			// Check that it has not been removed or canceled (si.sub would be nil)
			if si := mset.sources[iname]; si != nil {
				si.err = nil
				if ccr.Error != nil || ccr.ConsumerInfo == nil {
					// Note: this warning can happen a few times when starting up the server when sourcing streams are
					// defined, this is normal as the streams are re-created in no particular order and it is possible
					// that a stream sourcing another could come up before all of its sources have been recreated.
					mset.srv.Warnf("JetStream error response for stream %s create source consumer %s: %+v", mset.cfg.Name, si.name, ccr.Error)
					si.err = ccr.Error
					// Let's retry as soon as possible, but we are gated by sourceConsumerRetryThreshold
					retry = true
					mset.mu.Unlock()
					return
				} else {
					// Check if our shared msg queue and go routine is running or not.
					if mset.smsgs == nil {
						qname := fmt.Sprintf("[ACC:%s] stream sources '%s' msgs", mset.acc.Name, mset.cfg.Name)
						mset.smsgs = newIPQueue[*inMsg](mset.srv, qname)
						mset.srv.startGoRoutine(func() { mset.processAllSourceMsgs() },
							pprofLabels{
								"type":    "source",
								"account": mset.acc.Name,
								"stream":  mset.cfg.Name,
							},
						)
					}

					// Setup actual subscription to process messages from our source.
					if si.sseq != ccr.ConsumerInfo.Delivered.Stream {
						si.sseq = ccr.ConsumerInfo.Delivered.Stream + 1
					}
					// Capture consumer name.
					si.cname = ccr.ConsumerInfo.Name

					// Do not set si.sseq to seq here. si.sseq will be set in processInboundSourceMsg
					si.dseq = 0
					si.qch = make(chan struct{})
					// Set the last seen as now so that we don't fail at the first check.
					si.last.Store(time.Now().UnixNano())

					msgs := mset.smsgs
					sub, err := mset.subscribeInternal(deliverSubject, func(sub *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
						hdr, msg := c.msgParts(copyBytes(rmsg)) // Need to copy.
						mset.queueInbound(msgs, subject, reply, hdr, msg, si, nil)
						si.last.Store(time.Now().UnixNano())
					})
					if err != nil {
						si.err = NewJSSourceConsumerSetupFailedError(err, Unless(err))
						retry = true
						mset.mu.Unlock()
						return
					}
					// Save our sub.
					si.sub = sub
				}
			}
			mset.mu.Unlock()
		case <-time.After(srcConsumerWaitTime):
			mset.unsubscribe(crSub)
			// We already waited 30 seconds, let's retry now.
			retry = true
		}
	}()
}

// This will process all inbound source msgs.
// We mux them into one go routine to avoid lock contention and high cpu and thread thrashing.
// TODO(dlc) make this more then one and pin sources to one of a group.
func (mset *stream) processAllSourceMsgs() {
	s := mset.srv
	defer s.grWG.Done()

	mset.mu.RLock()
	msgs, qch := mset.smsgs, mset.qch
	mset.mu.RUnlock()

	t := time.NewTicker(sourceHealthCheckInterval)
	defer t.Stop()

	// When we detect we are no longer leader, we will cleanup.
	// Should always return right after this is called.
	cleanUp := func() {
		mset.mu.Lock()
		defer mset.mu.Unlock()
		for _, si := range mset.sources {
			mset.cancelSourceConsumer(si.iname)
		}
		mset.smsgs.drain()
		mset.smsgs.unregister()
		mset.smsgs = nil
	}

	for {
		select {
		case <-s.quitCh:
			return
		case <-qch:
			return
		case <-msgs.ch:
			ims := msgs.pop()
			for _, im := range ims {
				if !mset.processInboundSourceMsg(im.si, im) {
					// If we are no longer leader bail.
					if !mset.IsLeader() {
						msgs.recycle(&ims)
						cleanUp()
						return
					}
					break
				}
				im.returnToPool()
			}
			msgs.recycle(&ims)
		case <-t.C:
			// If we are no longer leader bail.
			if !mset.IsLeader() {
				cleanUp()
				return
			}

			// Check health of all sources.
			var stalled []*sourceInfo
			mset.mu.RLock()
			for _, si := range mset.sources {
				if time.Since(time.Unix(0, si.last.Load())) > sourceHealthCheckInterval {
					stalled = append(stalled, si)
				}
			}
			numSources := len(mset.sources)
			mset.mu.RUnlock()

			// This can happen on an update when no longer have sources.
			if numSources == 0 {
				cleanUp()
				return
			}

			// We do not want to block here so do in separate Go routine.
			if len(stalled) > 0 {
				go func() {
					mset.mu.Lock()
					defer mset.mu.Unlock()
					for _, si := range stalled {
						mset.setupSourceConsumer(si.iname, si.sseq+1, time.Time{})
						si.last.Store(time.Now().UnixNano())
					}
				}()
			}
		}
	}
}

// isControlMsg determines if this is a control message.
func (m *inMsg) isControlMsg() bool {
	return len(m.msg) == 0 && len(m.hdr) > 0 && bytes.HasPrefix(m.hdr, []byte("NATS/1.0 100 "))
}

// Sends a reply to a flow control request.
func (mset *stream) sendFlowControlReply(reply string) {
	mset.mu.RLock()
	if mset.isLeader() && mset.outq != nil {
		mset.outq.sendMsg(reply, nil)
	}
	mset.mu.RUnlock()
}

// handleFlowControl will properly handle flow control messages for both R==1 and R>1.
// Lock should be held.
func (mset *stream) handleFlowControl(m *inMsg) {
	// If we are clustered we will send the flow control message through the replication stack.
	if mset.isClustered() {
		mset.node.Propose(encodeStreamMsg(_EMPTY_, m.rply, m.hdr, nil, 0, 0, false))
	} else {
		mset.outq.sendMsg(m.rply, nil)
	}
}

// processInboundSourceMsg handles processing other stream messages bound for this stream.
func (mset *stream) processInboundSourceMsg(si *sourceInfo, m *inMsg) bool {
	mset.mu.Lock()
	// If we are no longer the leader cancel this subscriber.
	if !mset.isLeader() {
		mset.cancelSourceConsumer(si.iname)
		mset.mu.Unlock()
		return false
	}

	isControl := m.isControlMsg()

	// Ignore from old subscriptions.
	if !si.isCurrentSub(m.rply) && !isControl {
		mset.mu.Unlock()
		return false
	}

	// Check for heartbeats and flow control messages.
	if isControl {
		var needsRetry bool
		// Flow controls have reply subjects.
		if m.rply != _EMPTY_ {
			mset.handleFlowControl(m)
		} else {
			// For idle heartbeats make sure we did not miss anything.
			if ldseq := parseInt64(getHeader(JSLastConsumerSeq, m.hdr)); ldseq > 0 && uint64(ldseq) != si.dseq {
				needsRetry = true
				mset.retrySourceConsumerAtSeq(si.iname, si.sseq+1)
			} else if fcReply := getHeader(JSConsumerStalled, m.hdr); len(fcReply) > 0 {
				// Other side thinks we are stalled, so send flow control reply.
				mset.outq.sendMsg(string(fcReply), nil)
			}
		}
		mset.mu.Unlock()
		return !needsRetry
	}

	sseq, dseq, dc, _, pending := replyInfo(m.rply)

	if dc > 1 {
		mset.mu.Unlock()
		return false
	}

	// Tracking is done here.
	if dseq == si.dseq+1 {
		si.dseq++
		si.sseq = sseq
	} else if dseq > si.dseq {
		if si.cname == _EMPTY_ {
			si.cname = tokenAt(m.rply, 4)
			si.dseq, si.sseq = dseq, sseq
		} else {
			mset.retrySourceConsumerAtSeq(si.iname, si.sseq+1)
			mset.mu.Unlock()
			return false
		}
	} else {
		mset.mu.Unlock()
		return false
	}

	if pending == 0 {
		si.lag = 0
	} else {
		si.lag = pending - 1
	}
	node := mset.node
	mset.mu.Unlock()

	hdr, msg := m.hdr, m.msg

	// If we are daisy chained here make sure to remove the original one.
	if len(hdr) > 0 {
		hdr = removeHeaderIfPresent(hdr, JSStreamSource)
		// Remove any Nats-Expected- headers as we don't want to validate them.
		hdr = removeHeaderIfPrefixPresent(hdr, "Nats-Expected-")
	}
	// Hold onto the origin reply which has all the metadata.
	hdr = genHeader(hdr, JSStreamSource, si.genSourceHeader(m.rply))

	// Do the subject transform for the source if there's one
	if len(si.trs) > 0 {
		for _, tr := range si.trs {
			if tr == nil {
				continue
			} else {
				tsubj, err := tr.Match(m.subj)
				if err == nil {
					m.subj = tsubj
					break
				}
			}
		}
	}

	var err error
	// If we are clustered we need to propose this message to the underlying raft group.
	if node != nil {
		err = mset.processClusteredInboundMsg(m.subj, _EMPTY_, hdr, msg, nil, true)
	} else {
		err = mset.processJetStreamMsg(m.subj, _EMPTY_, hdr, msg, 0, 0, nil, true)
	}

	if err != nil {
		s := mset.srv
		if strings.Contains(err.Error(), "no space left") {
			s.Errorf("JetStream out of space, will be DISABLED")
			s.DisableJetStream()
		} else {
			mset.mu.RLock()
			accName, sname, iName := mset.acc.Name, mset.cfg.Name, si.iname
			mset.mu.RUnlock()

			// Can happen temporarily all the time during normal operations when the sourcing stream
			// is working queue/interest with a limit and discard new.
			// TODO - Improve sourcing to WQ with limit and new to use flow control rather than re-creating the consumer.
			if errors.Is(err, ErrMaxMsgs) || errors.Is(err, ErrMaxBytes) {
				// Do not need to do a full retry that includes finding the last sequence in the stream
				// for that source. Just re-create starting with the seq we couldn't store instead.
				mset.mu.Lock()
				mset.retrySourceConsumerAtSeq(iName, si.sseq)
				mset.mu.Unlock()
			} else {
				// Log some warning for errors other than errLastSeqMismatch or errMaxMsgs.
				if !errors.Is(err, errLastSeqMismatch) {
					s.RateLimitWarnf("Error processing inbound source %q for '%s' > '%s': %v",
						iName, accName, sname, err)
				}
				// Retry in all type of errors if we are still leader.
				if mset.isLeader() {
					// This will make sure the source is still in mset.sources map,
					// find the last sequence and then call setupSourceConsumer.
					iNameMap := map[string]struct{}{iName: {}}
					mset.setStartingSequenceForSources(iNameMap)
					mset.mu.Lock()
					mset.retrySourceConsumerAtSeq(iName, si.sseq+1)
					mset.mu.Unlock()
				}
			}
		}
		return false
	}

	return true
}

// Generate a new (2.10) style source header (stream name, sequence number, source filter, source destination transform).
func (si *sourceInfo) genSourceHeader(reply string) string {
	var b strings.Builder
	iNameParts := strings.Split(si.iname, " ")

	b.WriteString(iNameParts[0])
	b.WriteByte(' ')
	// Grab sequence as text here from reply subject.
	var tsa [expectedNumReplyTokens]string
	start, tokens := 0, tsa[:0]
	for i := 0; i < len(reply); i++ {
		if reply[i] == btsep {
			tokens, start = append(tokens, reply[start:i]), i+1
		}
	}
	tokens = append(tokens, reply[start:])
	seq := "1" // Default
	if len(tokens) == expectedNumReplyTokens && tokens[0] == "$JS" && tokens[1] == "ACK" {
		seq = tokens[5]
	}
	b.WriteString(seq)

	b.WriteByte(' ')
	b.WriteString(iNameParts[1])
	b.WriteByte(' ')
	b.WriteString(iNameParts[2])
	return b.String()
}

// Original version of header that stored ack reply direct.
func streamAndSeqFromAckReply(reply string) (string, string, uint64) {
	tsa := [expectedNumReplyTokens]string{}
	start, tokens := 0, tsa[:0]
	for i := 0; i < len(reply); i++ {
		if reply[i] == btsep {
			tokens, start = append(tokens, reply[start:i]), i+1
		}
	}
	tokens = append(tokens, reply[start:])
	if len(tokens) != expectedNumReplyTokens || tokens[0] != "$JS" || tokens[1] != "ACK" {
		return _EMPTY_, _EMPTY_, 0
	}
	return tokens[2], _EMPTY_, uint64(parseAckReplyNum(tokens[5]))
}

// Extract the stream name, the source index name and the message sequence number from the source header.
// Uses the filter and transform arguments to provide backwards compatibility
func streamAndSeq(shdr string) (string, string, uint64) {
	if strings.HasPrefix(shdr, jsAckPre) {
		return streamAndSeqFromAckReply(shdr)
	}
	// New version which is stream index name <SPC> sequence
	fields := strings.Split(shdr, " ")
	nFields := len(fields)

	if nFields != 2 && nFields <= 3 {
		return _EMPTY_, _EMPTY_, 0
	}

	if nFields >= 4 {
		return fields[0], strings.Join([]string{fields[0], fields[2], fields[3]}, " "), uint64(parseAckReplyNum(fields[1]))
	} else {
		return fields[0], _EMPTY_, uint64(parseAckReplyNum(fields[1]))
	}

}

// Lock should be held.
func (mset *stream) setStartingSequenceForSources(iNames map[string]struct{}) {
	var state StreamState
	mset.store.FastState(&state)

	// Do not reset sseq here so we can remember when purge/expiration happens.
	if state.Msgs == 0 {
		for iName := range iNames {
			si := mset.sources[iName]
			if si == nil {
				continue
			} else {
				si.dseq = 0
			}
		}
		return
	}

	var smv StoreMsg
	for seq := state.LastSeq; seq >= state.FirstSeq; {
		sm, err := mset.store.LoadPrevMsg(seq, &smv)
		if err == ErrStoreEOF || err != nil {
			break
		}
		seq = sm.seq - 1
		if len(sm.hdr) == 0 {
			continue
		}

		ss := getHeader(JSStreamSource, sm.hdr)
		if len(ss) == 0 {
			continue
		}
		streamName, indexName, sseq := streamAndSeq(bytesToString(ss))

		if _, ok := iNames[indexName]; ok {
			si := mset.sources[indexName]
			si.sseq = sseq
			si.dseq = 0
			delete(iNames, indexName)
		} else if indexName == _EMPTY_ && streamName != _EMPTY_ {
			for iName := range iNames {
				// TODO streamSource is a linear walk, to optimize later
				if si := mset.sources[iName]; si != nil && streamName == si.name ||
					(mset.streamSource(iName).External != nil && streamName == si.name+":"+getHash(mset.streamSource(iName).External.ApiPrefix)) {
					si.sseq = sseq
					si.dseq = 0
					delete(iNames, iName)
					break
				}
			}
		}
		if len(iNames) == 0 {
			break
		}
	}
}

// Resets the SourceInfo for all the sources
// lock should be held.
func (mset *stream) resetSourceInfo() {
	// Reset if needed.
	mset.stopSourceConsumers()
	mset.sources = make(map[string]*sourceInfo)

	for _, ssi := range mset.cfg.Sources {
		if ssi.iname == _EMPTY_ {
			ssi.setIndexName()
		}

		var si *sourceInfo

		if len(ssi.SubjectTransforms) == 0 {
			si = &sourceInfo{name: ssi.Name, iname: ssi.iname, sf: ssi.FilterSubject}
		} else {
			sfs := make([]string, len(ssi.SubjectTransforms))
			trs := make([]*subjectTransform, len(ssi.SubjectTransforms))
			for i, str := range ssi.SubjectTransforms {
				tr, err := NewSubjectTransform(str.Source, str.Destination)
				if err != nil {
					mset.srv.Errorf("Unable to get subject transform for source: %v", err)
				}
				sfs[i] = str.Source
				trs[i] = tr
			}
			si = &sourceInfo{name: ssi.Name, iname: ssi.iname, sfs: sfs, trs: trs}
		}
		mset.sources[ssi.iname] = si
	}
}

// This will do a reverse scan on startup or leader election
// searching for the starting sequence number.
// This can be slow in degenerative cases.
// Lock should be held.
func (mset *stream) startingSequenceForSources() {
	if len(mset.cfg.Sources) == 0 {
		return
	}

	// Always reset here.
	mset.resetSourceInfo()

	var state StreamState
	mset.store.FastState(&state)

	// Bail if no messages, meaning no context.
	if state.Msgs == 0 {
		return
	}

	// For short circuiting return.
	expected := len(mset.cfg.Sources)
	seqs := make(map[string]uint64)

	// Stamp our si seq records on the way out.
	defer func() {
		for sname, seq := range seqs {
			// Ignore if not set.
			if seq == 0 {
				continue
			}
			if si := mset.sources[sname]; si != nil {
				si.sseq = seq
				si.dseq = 0
			}
		}
	}()

	update := func(iName string, seq uint64) {
		// Only update active in case we have older ones in here that got configured out.
		if si := mset.sources[iName]; si != nil {
			if _, ok := seqs[iName]; !ok {
				seqs[iName] = seq
			}
		}
	}

	var smv StoreMsg
	for seq := state.LastSeq; ; {
		sm, err := mset.store.LoadPrevMsg(seq, &smv)
		if err == ErrStoreEOF || err != nil {
			break
		}
		seq = sm.seq - 1
		if len(sm.hdr) == 0 {
			continue
		}
		ss := getHeader(JSStreamSource, sm.hdr)
		if len(ss) == 0 {
			continue
		}

		streamName, iName, sseq := streamAndSeq(bytesToString(ss))
		if iName == _EMPTY_ { // Pre-2.10 message header means it's a match for any source using that stream name
			for _, ssi := range mset.cfg.Sources {
				if streamName == ssi.Name || (ssi.External != nil && streamName == ssi.Name+":"+getHash(ssi.External.ApiPrefix)) {
					update(ssi.iname, sseq)
				}
			}
		} else {
			update(iName, sseq)
		}
		if len(seqs) == expected {
			return
		}
	}
}

// Setup our source consumers.
// Lock should be held.
func (mset *stream) setupSourceConsumers() error {
	if mset.outq == nil {
		return errors.New("outq required")
	}
	// Reset if needed.
	for _, si := range mset.sources {
		if si.sub != nil {
			mset.cancelSourceConsumer(si.iname)
		}
	}

	// If we are no longer the leader, give up
	if !mset.isLeader() {
		return nil
	}

	mset.startingSequenceForSources()

	// Setup our consumers at the proper starting position.
	for _, ssi := range mset.cfg.Sources {
		if si := mset.sources[ssi.iname]; si != nil {
			mset.setupSourceConsumer(ssi.iname, si.sseq+1, time.Time{})
		}
	}

	return nil
}

// Will create internal subscriptions for the stream.
// Lock should be held.
func (mset *stream) subscribeToStream() error {
	if mset.active {
		return nil
	}
	for _, subject := range mset.cfg.Subjects {
		if _, err := mset.subscribeInternal(subject, mset.processInboundJetStreamMsg); err != nil {
			return err
		}
	}
	// Check if we need to setup mirroring.
	if mset.cfg.Mirror != nil {
		// setup the initial mirror sourceInfo
		mset.mirror = &sourceInfo{name: mset.cfg.Mirror.Name}
		sfs := make([]string, len(mset.cfg.Mirror.SubjectTransforms))
		trs := make([]*subjectTransform, len(mset.cfg.Mirror.SubjectTransforms))

		for i, tr := range mset.cfg.Mirror.SubjectTransforms {
			// will not fail as already checked before that the transform will work
			subjectTransform, err := NewSubjectTransform(tr.Source, tr.Destination)
			if err != nil {
				mset.srv.Errorf("Unable to get transform for mirror consumer: %v", err)
			}

			sfs[i] = tr.Source
			trs[i] = subjectTransform
		}
		mset.mirror.sfs = sfs
		mset.mirror.trs = trs
		// delay the actual mirror consumer creation for after a delay
		mset.scheduleSetupMirrorConsumerRetry()
	} else if len(mset.cfg.Sources) > 0 && mset.sourcesConsumerSetup == nil {
		// Setup the initial source infos for the sources
		mset.resetSourceInfo()
		// Delay the actual source consumer(s) creation(s) for after a delay if a replicated stream.
		// If it's an R1, this is done at startup and we will do inline.
		if mset.cfg.Replicas == 1 {
			mset.setupSourceConsumers()
		} else {
			mset.sourcesConsumerSetup = time.AfterFunc(time.Duration(rand.Intn(int(500*time.Millisecond)))+100*time.Millisecond, func() {
				mset.mu.Lock()
				mset.setupSourceConsumers()
				mset.mu.Unlock()
			})
		}
	}
	// Check for direct get access.
	// We spin up followers for clustered streams in monitorStream().
	if mset.cfg.AllowDirect {
		if err := mset.subscribeToDirect(); err != nil {
			return err
		}
	}

	mset.active = true
	return nil
}

// Lock should be held.
func (mset *stream) subscribeToDirect() error {
	// We will make this listen on a queue group by default, which can allow mirrors to participate on opt-in basis.
	if mset.directSub == nil {
		dsubj := fmt.Sprintf(JSDirectMsgGetT, mset.cfg.Name)
		if sub, err := mset.queueSubscribeInternal(dsubj, dgetGroup, mset.processDirectGetRequest); err == nil {
			mset.directSub = sub
		} else {
			return err
		}
	}
	// Now the one that will have subject appended past stream name.
	if mset.lastBySub == nil {
		dsubj := fmt.Sprintf(JSDirectGetLastBySubjectT, mset.cfg.Name, fwcs)
		// We will make this listen on a queue group by default, which can allow mirrors to participate on opt-in basis.
		if sub, err := mset.queueSubscribeInternal(dsubj, dgetGroup, mset.processDirectGetLastBySubjectRequest); err == nil {
			mset.lastBySub = sub
		} else {
			return err
		}
	}

	return nil
}

// Lock should be held.
func (mset *stream) unsubscribeToDirect() {
	if mset.directSub != nil {
		mset.unsubscribe(mset.directSub)
		mset.directSub = nil
	}
	if mset.lastBySub != nil {
		mset.unsubscribe(mset.lastBySub)
		mset.lastBySub = nil
	}
}

// Lock should be held.
func (mset *stream) subscribeToMirrorDirect() error {
	if mset.mirror == nil {
		return nil
	}

	// We will make this listen on a queue group by default, which can allow mirrors to participate on opt-in basis.
	if mset.mirror.dsub == nil {
		dsubj := fmt.Sprintf(JSDirectMsgGetT, mset.mirror.name)
		// We will make this listen on a queue group by default, which can allow mirrors to participate on opt-in basis.
		if sub, err := mset.queueSubscribeInternal(dsubj, dgetGroup, mset.processDirectGetRequest); err == nil {
			mset.mirror.dsub = sub
		} else {
			return err
		}
	}
	// Now the one that will have subject appended past stream name.
	if mset.mirror.lbsub == nil {
		dsubj := fmt.Sprintf(JSDirectGetLastBySubjectT, mset.mirror.name, fwcs)
		// We will make this listen on a queue group by default, which can allow mirrors to participate on opt-in basis.
		if sub, err := mset.queueSubscribeInternal(dsubj, dgetGroup, mset.processDirectGetLastBySubjectRequest); err == nil {
			mset.mirror.lbsub = sub
		} else {
			return err
		}
	}

	return nil
}

// Stop our source consumers.
// Lock should be held.
func (mset *stream) stopSourceConsumers() {
	for _, si := range mset.sources {
		mset.cancelSourceInfo(si)
	}
}

// Lock should be held.
func (mset *stream) removeInternalConsumer(si *sourceInfo) {
	if si == nil || si.cname == _EMPTY_ {
		return
	}
	si.cname = _EMPTY_
}

// Will unsubscribe from the stream.
// Lock should be held.
func (mset *stream) unsubscribeToStream(stopping bool) error {
	for _, subject := range mset.cfg.Subjects {
		mset.unsubscribeInternal(subject)
	}
	if mset.mirror != nil {
		mset.cancelSourceInfo(mset.mirror)
		mset.mirror = nil
	}

	if len(mset.sources) > 0 {
		mset.stopSourceConsumers()
	}

	// In case we had a direct get subscriptions.
	if stopping {
		mset.unsubscribeToDirect()
	}

	mset.active = false
	return nil
}

// Lock does NOT need to be held, we set the client on setup and never change it at this point.
func (mset *stream) subscribeInternal(subject string, cb msgHandler) (*subscription, error) {
	if mset.closed.Load() {
		return nil, errStreamClosed
	}
	if cb == nil {
		return nil, errInvalidMsgHandler
	}
	c := mset.client
	sid := int(mset.sid.Add(1))
	// Now create the subscription
	return c.processSub([]byte(subject), nil, []byte(strconv.Itoa(sid)), cb, false)
}

// Lock does NOT need to be held, we set the client on setup and never change it at this point.
func (mset *stream) queueSubscribeInternal(subject, group string, cb msgHandler) (*subscription, error) {
	if mset.closed.Load() {
		return nil, errStreamClosed
	}
	if cb == nil {
		return nil, errInvalidMsgHandler
	}
	c := mset.client
	sid := int(mset.sid.Add(1))
	// Now create the subscription
	return c.processSub([]byte(subject), []byte(group), []byte(strconv.Itoa(sid)), cb, false)
}

// This will unsubscribe us from the exact subject given.
// We do not currently track the subs so do not have the sid.
// This should be called only on an update.
// Lock does NOT need to be held, we set the client on setup and never change it at this point.
func (mset *stream) unsubscribeInternal(subject string) error {
	if mset.closed.Load() {
		return errStreamClosed
	}
	c := mset.client
	var sid []byte
	c.mu.Lock()
	for _, sub := range c.subs {
		if subject == string(sub.subject) {
			sid = sub.sid
			break
		}
	}
	c.mu.Unlock()

	if sid != nil {
		return c.processUnsub(sid)
	}
	return nil
}

// Lock should be held.
func (mset *stream) unsubscribe(sub *subscription) {
	if sub == nil || mset.closed.Load() {
		return
	}
	mset.client.processUnsub(sub.sid)
}

func (mset *stream) setupStore(fsCfg *FileStoreConfig) error {
	mset.mu.Lock()
	mset.created = time.Now().UTC()

	switch mset.cfg.Storage {
	case MemoryStorage:
		ms, err := newMemStore(&mset.cfg)
		if err != nil {
			mset.mu.Unlock()
			return err
		}
		mset.store = ms
	case FileStorage:
		s := mset.srv
		prf := s.jsKeyGen(s.getOpts().JetStreamKey, mset.acc.Name)
		if prf != nil {
			// We are encrypted here, fill in correct cipher selection.
			fsCfg.Cipher = s.getOpts().JetStreamCipher
		}
		oldprf := s.jsKeyGen(s.getOpts().JetStreamOldKey, mset.acc.Name)
		cfg := *fsCfg
		cfg.srv = s
		fs, err := newFileStoreWithCreated(cfg, mset.cfg, mset.created, prf, oldprf)
		if err != nil {
			mset.mu.Unlock()
			return err
		}
		mset.store = fs
	}
	// This will fire the callback but we do not require the lock since md will be 0 here.
	mset.store.RegisterStorageUpdates(mset.storeUpdates)
	mset.store.RegisterSubjectDeleteMarkerUpdates(func(im *inMsg) {
		if mset.IsClustered() {
			if mset.IsLeader() {
				mset.processClusteredInboundMsg(im.subj, im.rply, im.hdr, im.msg, im.mt, false)
			}
		} else {
			mset.processJetStreamMsg(im.subj, im.rply, im.hdr, im.msg, 0, 0, im.mt, false)
		}
	})
	mset.mu.Unlock()

	return nil
}

// Called for any updates to the underlying stream. We pass through the bytes to the
// jetstream account. We do local processing for stream pending for consumers, but only
// for removals.
// Lock should not be held.
func (mset *stream) storeUpdates(md, bd int64, seq uint64, subj string) {
	// If we have a single negative update then we will process our consumers for stream pending.
	// Purge and Store handled separately inside individual calls.
	if md == -1 && seq > 0 && subj != _EMPTY_ {
		// We use our consumer list mutex here instead of the main stream lock since it may be held already.
		mset.clsMu.RLock()
		if mset.csl != nil {
			mset.csl.Match(subj, func(o *consumer) {
				o.decStreamPending(seq, subj)
			})
		} else {
			for _, o := range mset.cList {
				o.decStreamPending(seq, subj)
			}
		}
		mset.clsMu.RUnlock()
	} else if md < 0 {
		// Batch decrements we need to force consumers to re-calculate num pending.
		mset.clsMu.RLock()
		for _, o := range mset.cList {
			o.streamNumPendingLocked()
		}
		mset.clsMu.RUnlock()
	}

	if mset.jsa != nil {
		mset.jsa.updateUsage(mset.tier, mset.stype, bd)
	}
}

// NumMsgIds returns the number of message ids being tracked for duplicate suppression.
func (mset *stream) numMsgIds() int {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	if !mset.ddloaded {
		mset.rebuildDedupe()
	}
	return len(mset.ddmap)
}

// checkMsgId will process and check for duplicates.
// Lock should be held.
func (mset *stream) checkMsgId(id string) *ddentry {
	if !mset.ddloaded {
		mset.rebuildDedupe()
	}
	if id == _EMPTY_ || len(mset.ddmap) == 0 {
		return nil
	}
	return mset.ddmap[id]
}

// Will purge the entries that are past the window.
// Should be called from a timer.
func (mset *stream) purgeMsgIds() {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	now := time.Now().UnixNano()
	tmrNext := mset.cfg.Duplicates
	window := int64(tmrNext)

	for i, dde := range mset.ddarr[mset.ddindex:] {
		if now-dde.ts >= window {
			delete(mset.ddmap, dde.id)
		} else {
			mset.ddindex += i
			// Check if we should garbage collect here if we are 1/3 total size.
			if cap(mset.ddarr) > 3*(len(mset.ddarr)-mset.ddindex) {
				mset.ddarr = append([]*ddentry(nil), mset.ddarr[mset.ddindex:]...)
				mset.ddindex = 0
			}
			tmrNext = time.Duration(window - (now - dde.ts))
			break
		}
	}
	if len(mset.ddmap) > 0 {
		// Make sure to not fire too quick
		const minFire = 50 * time.Millisecond
		if tmrNext < minFire {
			tmrNext = minFire
		}
		if mset.ddtmr != nil {
			mset.ddtmr.Reset(tmrNext)
		} else {
			mset.ddtmr = time.AfterFunc(tmrNext, mset.purgeMsgIds)
		}
	} else {
		if mset.ddtmr != nil {
			mset.ddtmr.Stop()
			mset.ddtmr = nil
		}
		mset.ddmap = nil
		mset.ddarr = nil
		mset.ddindex = 0
	}
}

// storeMsgIdLocked will store the message id for duplicate detection.
// Lock should be held.
func (mset *stream) storeMsgIdLocked(dde *ddentry) {
	if mset.ddmap == nil {
		mset.ddmap = make(map[string]*ddentry)
	}
	mset.ddmap[dde.id] = dde
	mset.ddarr = append(mset.ddarr, dde)
	if mset.ddtmr == nil {
		mset.ddtmr = time.AfterFunc(mset.cfg.Duplicates, mset.purgeMsgIds)
	}
}

// Fast lookup of msgId.
func getMsgId(hdr []byte) string {
	return string(getHeader(JSMsgId, hdr))
}

// Fast lookup of expected last msgId.
func getExpectedLastMsgId(hdr []byte) string {
	return string(getHeader(JSExpectedLastMsgId, hdr))
}

// Fast lookup of expected stream.
func getExpectedStream(hdr []byte) string {
	return string(getHeader(JSExpectedStream, hdr))
}

// Fast lookup of expected stream.
func getExpectedLastSeq(hdr []byte) (uint64, bool) {
	bseq := getHeader(JSExpectedLastSeq, hdr)
	if len(bseq) == 0 {
		return 0, false
	}
	return uint64(parseInt64(bseq)), true
}

// Fast lookup of rollups.
func getRollup(hdr []byte) string {
	r := getHeader(JSMsgRollup, hdr)
	if len(r) == 0 {
		return _EMPTY_
	}
	return strings.ToLower(string(r))
}

// Fast lookup of expected stream sequence per subject.
func getExpectedLastSeqPerSubject(hdr []byte) (uint64, bool) {
	bseq := getHeader(JSExpectedLastSubjSeq, hdr)
	if len(bseq) == 0 {
		return 0, false
	}
	return uint64(parseInt64(bseq)), true
}

// Fast lookup of expected subject for the expected stream sequence per subject.
func getExpectedLastSeqPerSubjectForSubject(hdr []byte) string {
	return string(getHeader(JSExpectedLastSubjSeqSubj, hdr))
}

// Fast lookup of the message TTL from headers:
// - Positive return value: duration in seconds.
// - Zero return value: no TTL or parse error.
// - Negative return value: never expires.
func getMessageTTL(hdr []byte) (int64, error) {
	ttl := getHeader(JSMessageTTL, hdr)
	if len(ttl) == 0 {
		return 0, nil
	}
	return parseMessageTTL(bytesToString(ttl))
}

// - Positive return value: duration in seconds.
// - Zero return value: no TTL or parse error.
// - Negative return value: never expires.
func parseMessageTTL(ttl string) (int64, error) {
	if strings.ToLower(ttl) == "never" {
		return -1, nil
	}
	dur, err := time.ParseDuration(ttl)
	if err == nil {
		if dur < time.Second {
			return 0, NewJSMessageTTLInvalidError()
		}
		return int64(dur.Seconds()), nil
	}
	t := parseInt64(stringToBytes(ttl))
	if t < 0 {
		// This probably means a parse failure, hence why
		// we have a special case "never" for returning -1.
		// Otherwise we can't know if it's a genuine TTL
		// that says never expire or if it's a parse error.
		return 0, NewJSMessageTTLInvalidError()
	}
	return t, nil
}

// Signal if we are clustered. Will acquire rlock.
func (mset *stream) IsClustered() bool {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.isClustered()
}

// Lock should be held.
func (mset *stream) isClustered() bool {
	return mset.node != nil
}

// Used if we have to queue things internally to avoid the route/gw path.
type inMsg struct {
	subj string
	rply string
	hdr  []byte
	msg  []byte
	si   *sourceInfo
	mt   *msgTrace
}

var inMsgPool = sync.Pool{
	New: func() any {
		return &inMsg{}
	},
}

func (im *inMsg) returnToPool() {
	im.subj, im.rply, im.hdr, im.msg, im.si, im.mt = _EMPTY_, _EMPTY_, nil, nil, nil, nil
	inMsgPool.Put(im)
}

func (mset *stream) queueInbound(ib *ipQueue[*inMsg], subj, rply string, hdr, msg []byte, si *sourceInfo, mt *msgTrace) {
	im := inMsgPool.Get().(*inMsg)
	im.subj, im.rply, im.hdr, im.msg, im.si, im.mt = subj, rply, hdr, msg, si, mt
	if _, err := ib.push(im); err != nil {
		im.returnToPool()
		mset.srv.RateLimitWarnf("Dropping messages due to excessive stream ingest rate on '%s' > '%s': %s", mset.acc.Name, mset.name(), err)
		if rply != _EMPTY_ {
			hdr := []byte("NATS/1.0 429 Too Many Requests\r\n\r\n")
			b, _ := json.Marshal(&JSPubAckResponse{PubAck: &PubAck{Stream: mset.cfg.Name}, Error: NewJSStreamTooManyRequestsError()})
			mset.outq.send(newJSPubMsg(rply, _EMPTY_, _EMPTY_, hdr, b, nil, 0))
		}
	}
}

var dgPool = sync.Pool{
	New: func() any {
		return &directGetReq{}
	},
}

// For when we need to not inline the request.
type directGetReq struct {
	// Copy of this is correct for this.
	req   JSApiMsgGetRequest
	reply string
}

// processDirectGetRequest handles direct get request for stream messages.
func (mset *stream) processDirectGetRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	if len(reply) == 0 {
		return
	}
	_, msg := c.msgParts(rmsg)
	if len(msg) == 0 {
		hdr := []byte("NATS/1.0 408 Empty Request\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}
	var req JSApiMsgGetRequest
	err := json.Unmarshal(msg, &req)
	if err != nil {
		hdr := []byte("NATS/1.0 408 Malformed Request\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}
	// Check if nothing set.
	if req.Seq == 0 && req.LastFor == _EMPTY_ && req.NextFor == _EMPTY_ && len(req.MultiLastFor) == 0 && req.StartTime == nil {
		hdr := []byte("NATS/1.0 408 Empty Request\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}
	// Check we don't have conflicting options set.
	// We do not allow batch mode for lastFor requests.
	if (req.Seq > 0 && req.LastFor != _EMPTY_) ||
		(req.Seq > 0 && req.StartTime != nil) ||
		(req.StartTime != nil && req.LastFor != _EMPTY_) ||
		(req.LastFor != _EMPTY_ && req.NextFor != _EMPTY_) ||
		(req.LastFor != _EMPTY_ && req.Batch > 0) ||
		(req.LastFor != _EMPTY_ && len(req.MultiLastFor) > 0) ||
		(req.NextFor != _EMPTY_ && len(req.MultiLastFor) > 0) ||
		(req.UpToSeq > 0 && req.UpToTime != nil) {
		hdr := []byte("NATS/1.0 408 Bad Request\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}

	inlineOk := c.kind != ROUTER && c.kind != GATEWAY && c.kind != LEAF
	if !inlineOk {
		dg := dgPool.Get().(*directGetReq)
		dg.req, dg.reply = req, reply
		mset.gets.push(dg)
	} else {
		mset.getDirectRequest(&req, reply)
	}
}

// This is for direct get by last subject which is part of the subject itself.
func (mset *stream) processDirectGetLastBySubjectRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	if len(reply) == 0 {
		return
	}
	_, msg := c.msgParts(rmsg)
	// This version expects no payload.
	if len(msg) != 0 {
		hdr := []byte("NATS/1.0 408 Bad Request\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}
	// Extract the key.
	var key string
	for i, n := 0, 0; i < len(subject); i++ {
		if subject[i] == btsep {
			if n == 4 {
				if start := i + 1; start < len(subject) {
					key = subject[i+1:]
				}
				break
			}
			n++
		}
	}
	if len(key) == 0 {
		hdr := []byte("NATS/1.0 408 Bad Request\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}

	req := JSApiMsgGetRequest{LastFor: key}

	inlineOk := c.kind != ROUTER && c.kind != GATEWAY && c.kind != LEAF
	if !inlineOk {
		dg := dgPool.Get().(*directGetReq)
		dg.req, dg.reply = req, reply
		mset.gets.push(dg)
	} else {
		mset.getDirectRequest(&req, reply)
	}
}

// For direct get batch and multi requests.
const (
	dg   = "NATS/1.0\r\nNats-Stream: %s\r\nNats-Subject: %s\r\nNats-Sequence: %d\r\nNats-Time-Stamp: %s\r\n\r\n"
	dgb  = "NATS/1.0\r\nNats-Stream: %s\r\nNats-Subject: %s\r\nNats-Sequence: %d\r\nNats-Time-Stamp: %s\r\nNats-Num-Pending: %d\r\nNats-Last-Sequence: %d\r\n\r\n"
	eob  = "NATS/1.0 204 EOB\r\nNats-Num-Pending: %d\r\nNats-Last-Sequence: %d\r\n\r\n"
	eobm = "NATS/1.0 204 EOB\r\nNats-Num-Pending: %d\r\nNats-Last-Sequence: %d\r\nNats-UpTo-Sequence: %d\r\n\r\n"
)

// Handle a multi request.
func (mset *stream) getDirectMulti(req *JSApiMsgGetRequest, reply string) {
	// TODO(dlc) - Make configurable?
	const maxAllowedResponses = 1024

	// We hold the lock here to try to avoid changes out from underneath of us.
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	// Grab store and name.
	store, name, s := mset.store, mset.cfg.Name, mset.srv

	// Grab MaxBytes
	mb := req.MaxBytes
	if mb == 0 && s != nil {
		// Fill in with the server's MaxPending.
		mb = int(s.opts.MaxPending)
	}

	upToSeq := req.UpToSeq
	// If we have UpToTime set get the proper sequence.
	if req.UpToTime != nil {
		upToSeq = store.GetSeqFromTime((*req.UpToTime).UTC())
		// We need to back off one since this is used to determine start sequence normally,
		// were as here we want it to be the ceiling.
		upToSeq--
	}
	// If not set, set to the last sequence and remember that for EOB.
	if upToSeq == 0 {
		var state StreamState
		mset.store.FastState(&state)
		upToSeq = state.LastSeq
	}

	seqs, err := store.MultiLastSeqs(req.MultiLastFor, upToSeq, maxAllowedResponses)
	if err != nil {
		var hdr []byte
		if err == ErrTooManyResults {
			hdr = []byte("NATS/1.0 413 Too Many Results\r\n\r\n")
		} else {
			hdr = []byte(fmt.Sprintf("NATS/1.0 500 %v\r\n\r\n", err))
		}
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}
	if len(seqs) == 0 {
		hdr := []byte("NATS/1.0 404 No Results\r\n\r\n")
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
		return
	}

	np, lseq, sentBytes, sent := uint64(len(seqs)-1), uint64(0), 0, 0
	for _, seq := range seqs {
		if seq < req.Seq {
			if np > 0 {
				np--
			}
			continue
		}
		var svp StoreMsg
		sm, err := store.LoadMsg(seq, &svp)
		if err != nil {
			hdr := []byte("NATS/1.0 404 Message Not Found\r\n\r\n")
			mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
			return
		}

		hdr := sm.hdr
		ts := time.Unix(0, sm.ts).UTC()

		if len(hdr) == 0 {
			hdr = fmt.Appendf(nil, dgb, name, sm.subj, sm.seq, ts.Format(time.RFC3339Nano), np, lseq)
		} else {
			hdr = copyBytes(hdr)
			hdr = genHeader(hdr, JSStream, name)
			hdr = genHeader(hdr, JSSubject, sm.subj)
			hdr = genHeader(hdr, JSSequence, strconv.FormatUint(sm.seq, 10))
			hdr = genHeader(hdr, JSTimeStamp, ts.Format(time.RFC3339Nano))
			hdr = genHeader(hdr, JSNumPending, strconv.FormatUint(np, 10))
			hdr = genHeader(hdr, JSLastSequence, strconv.FormatUint(lseq, 10))
		}
		// Decrement num pending. This is optimization and we do not continue to look it up for these operations.
		if np > 0 {
			np--
		}
		// Track our lseq
		lseq = sm.seq
		// Send out our message.
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, sm.msg, nil, 0))
		// Check if we have exceeded max bytes.
		sentBytes += len(sm.subj) + len(sm.hdr) + len(sm.msg)
		if sentBytes >= mb {
			break
		}
		sent++
		if req.Batch > 0 && sent >= req.Batch {
			break
		}
	}

	// Send out EOB
	hdr := fmt.Appendf(nil, eobm, np, lseq, upToSeq)
	mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
}

// Do actual work on a direct msg request.
// This could be called in a Go routine if we are inline for a non-client connection.
func (mset *stream) getDirectRequest(req *JSApiMsgGetRequest, reply string) {
	// Handle multi in separate function.
	if len(req.MultiLastFor) > 0 {
		mset.getDirectMulti(req, reply)
		return
	}

	mset.mu.RLock()
	store, name, s := mset.store, mset.cfg.Name, mset.srv
	mset.mu.RUnlock()

	var seq uint64
	// Lookup start seq if AsOfTime is set.
	if req.StartTime != nil {
		seq = store.GetSeqFromTime(*req.StartTime)
	} else {
		seq = req.Seq
	}

	wc := subjectHasWildcard(req.NextFor)
	// For tracking num pending if we are batch.
	var np, lseq, validThrough uint64
	var isBatchRequest bool
	batch := req.Batch
	if batch == 0 {
		batch = 1
	} else {
		// This is a batch request, capture initial numPending.
		isBatchRequest = true
		np, validThrough = store.NumPending(seq, req.NextFor, false)
	}

	// Grab MaxBytes
	mb := req.MaxBytes
	if mb == 0 && s != nil {
		// Fill in with the server's MaxPending.
		mb = int(s.opts.MaxPending)
	}
	// Track what we have sent.
	var sentBytes int

	// Loop over batch, which defaults to 1.
	for i := 0; i < batch; i++ {
		var (
			svp StoreMsg
			sm  *StoreMsg
			err error
		)
		if seq > 0 && req.NextFor == _EMPTY_ {
			// Only do direct lookup for first in a batch.
			if i == 0 {
				sm, err = store.LoadMsg(seq, &svp)
			} else {
				// We want to use load next with fwcs to step over deleted msgs.
				sm, seq, err = store.LoadNextMsg(fwcs, true, seq, &svp)
			}
			// Bump for next loop if applicable.
			seq++
		} else if req.NextFor != _EMPTY_ {
			sm, seq, err = store.LoadNextMsg(req.NextFor, wc, seq, &svp)
			seq++
		} else {
			// Batch is not applicable here, this is checked before we get here.
			sm, err = store.LoadLastMsg(req.LastFor, &svp)
		}
		if err != nil {
			// For batches, if we stop early we want to do EOB logic below.
			if batch > 1 && i > 0 {
				break
			}
			hdr := []byte("NATS/1.0 404 Message Not Found\r\n\r\n")
			mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
			return
		}

		hdr := sm.hdr
		ts := time.Unix(0, sm.ts).UTC()

		if isBatchRequest {
			if len(hdr) == 0 {
				hdr = fmt.Appendf(nil, dgb, name, sm.subj, sm.seq, ts.Format(time.RFC3339Nano), np, lseq)
			} else {
				hdr = copyBytes(hdr)
				hdr = genHeader(hdr, JSStream, name)
				hdr = genHeader(hdr, JSSubject, sm.subj)
				hdr = genHeader(hdr, JSSequence, strconv.FormatUint(sm.seq, 10))
				hdr = genHeader(hdr, JSTimeStamp, ts.Format(time.RFC3339Nano))
				hdr = genHeader(hdr, JSNumPending, strconv.FormatUint(np, 10))
				hdr = genHeader(hdr, JSLastSequence, strconv.FormatUint(lseq, 10))
			}
			// Decrement num pending. This is optimization and we do not continue to look it up for these operations.
			np--
		} else {
			if len(hdr) == 0 {
				hdr = fmt.Appendf(nil, dg, name, sm.subj, sm.seq, ts.Format(time.RFC3339Nano))
			} else {
				hdr = copyBytes(hdr)
				hdr = genHeader(hdr, JSStream, name)
				hdr = genHeader(hdr, JSSubject, sm.subj)
				hdr = genHeader(hdr, JSSequence, strconv.FormatUint(sm.seq, 10))
				hdr = genHeader(hdr, JSTimeStamp, ts.Format(time.RFC3339Nano))
			}
		}
		// Track our lseq
		lseq = sm.seq
		// Send out our message.
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, sm.msg, nil, 0))
		// Check if we have exceeded max bytes.
		sentBytes += len(sm.subj) + len(sm.hdr) + len(sm.msg)
		if sentBytes >= mb {
			break
		}
	}

	// If batch was requested send EOB.
	if isBatchRequest {
		// Update if the stream's lasts sequence has moved past our validThrough.
		if mset.lastSeq() > validThrough {
			np, _ = store.NumPending(seq, req.NextFor, false)
		}
		hdr := fmt.Appendf(nil, eob, np, lseq)
		mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
	}
}

// processInboundJetStreamMsg handles processing messages bound for a stream.
func (mset *stream) processInboundJetStreamMsg(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	hdr, msg := c.msgParts(copyBytes(rmsg)) // Need to copy.
	if mt, traceOnly := c.isMsgTraceEnabled(); mt != nil {
		// If message is delivered, we need to disable the message trace headers
		// to prevent a trace event to be generated when a stored message
		// is delivered to a consumer and routed.
		if !traceOnly {
			disableTraceHeaders(c, hdr)
		}
		// This will add the jetstream event while in the client read loop.
		// Since the event will be updated in a different go routine, the
		// tracing object will have a separate reference to the JS trace
		// object.
		mt.addJetStreamEvent(mset.name())
	}
	mset.queueInbound(mset.msgs, subject, reply, hdr, msg, nil, c.pa.trace)
}

var (
	errLastSeqMismatch   = errors.New("last sequence mismatch")
	errMsgIdDuplicate    = errors.New("msgid is duplicate")
	errStreamClosed      = errors.New("stream closed")
	errInvalidMsgHandler = errors.New("undefined message handler")
	errStreamMismatch    = errors.New("expected stream does not match")
	errMsgTTLDisabled    = errors.New("message TTL disabled")
)

// processJetStreamMsg is where we try to actually process the stream msg.
func (mset *stream) processJetStreamMsg(subject, reply string, hdr, msg []byte, lseq uint64, ts int64, mt *msgTrace, sourced bool) (retErr error) {
	if mt != nil {
		// Only the leader/standalone will have mt!=nil. On exit, send the
		// message trace event.
		defer func() {
			mt.sendEventFromJetStream(retErr)
		}()
	}

	if mset.closed.Load() {
		return errStreamClosed
	}

	mset.mu.Lock()
	s, store := mset.srv, mset.store

	traceOnly := mt.traceOnly()
	bumpCLFS := func() {
		// Do not bump if tracing and not doing message delivery.
		if traceOnly {
			return
		}
		mset.clMu.Lock()
		mset.clfs++
		mset.clMu.Unlock()
	}

	// Apply the input subject transform if any
	if mset.itr != nil {
		ts, err := mset.itr.Match(subject)
		if err == nil {
			// no filtering: if the subject doesn't map the source of the transform, don't change it
			subject = ts
		}
	}

	var accName string
	if mset.acc != nil {
		accName = mset.acc.Name
	}

	js, jsa, doAck := mset.js, mset.jsa, !mset.cfg.NoAck
	name, stype := mset.cfg.Name, mset.cfg.Storage
	maxMsgSize := int(mset.cfg.MaxMsgSize)
	numConsumers := len(mset.consumers)
	interestRetention := mset.cfg.Retention == InterestPolicy
	// Snapshot if we are the leader and if we can respond.
	isLeader, isSealed := mset.isLeaderNodeState(), mset.cfg.Sealed
	canRespond := doAck && len(reply) > 0 && isLeader

	var resp = &JSPubAckResponse{}

	// Bail here if sealed.
	if isSealed {
		outq := mset.outq
		mset.mu.Unlock()
		bumpCLFS()
		if canRespond && outq != nil {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = ApiErrors[JSStreamSealedErr]
			b, _ := json.Marshal(resp)
			outq.sendMsg(reply, b)
		}
		return ApiErrors[JSStreamSealedErr]
	}

	var buf [256]byte
	pubAck := append(buf[:0], mset.pubAck...)

	// If this is a non-clustered msg and we are not considered active, meaning no active subscription, do not process.
	if lseq == 0 && ts == 0 && !mset.active {
		mset.mu.Unlock()
		return nil
	}

	// For clustering the lower layers will pass our expected lseq. If it is present check for that here.
	if lseq > 0 && lseq != (mset.lseq+mset.clfs) {
		isMisMatch := true
		// We may be able to recover here if we have no state whatsoever, or we are a mirror.
		// See if we have to adjust our starting sequence.
		if mset.lseq == 0 || mset.cfg.Mirror != nil {
			var state StreamState
			mset.store.FastState(&state)
			if state.FirstSeq == 0 {
				mset.store.Compact(lseq + 1)
				mset.lseq = lseq
				isMisMatch = false
			}
		}
		// Really is a mismatch.
		if isMisMatch {
			outq := mset.outq
			mset.mu.Unlock()
			if canRespond && outq != nil {
				resp.PubAck = &PubAck{Stream: name}
				resp.Error = ApiErrors[JSStreamSequenceNotMatchErr]
				b, _ := json.Marshal(resp)
				outq.sendMsg(reply, b)
			}
			return errLastSeqMismatch
		}
	}

	// If we have received this message across an account we may have request information attached.
	// For now remove. TODO(dlc) - Should this be opt-in or opt-out?
	if len(hdr) > 0 {
		hdr = removeHeaderIfPresent(hdr, ClientInfoHdr)
	}

	// Process additional msg headers if still present.
	var msgId string
	var rollupSub, rollupAll bool
	isClustered := mset.isClustered()

	if len(hdr) > 0 {
		outq := mset.outq

		// Certain checks have already been performed if in clustered mode, so only check if not.
		// Note, for cluster mode but with message tracing (without message delivery), we need
		// to do this check here since it was not done in processClusteredInboundMsg().
		if !isClustered || traceOnly {
			// Expected stream.
			if sname := getExpectedStream(hdr); sname != _EMPTY_ && sname != name {
				mset.mu.Unlock()
				bumpCLFS()
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = NewJSStreamNotMatchError()
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return errStreamMismatch
			}
		}

		// TTL'd messages are rejected entirely if TTLs are not enabled on the stream.
		// Shouldn't happen in clustered mode since we should have already caught this
		// in processClusteredInboundMsg, but needed here for non-clustered etc.
		if ttl, _ := getMessageTTL(hdr); !sourced && ttl != 0 && !mset.cfg.AllowMsgTTL {
			mset.mu.Unlock()
			bumpCLFS()
			if canRespond {
				resp.PubAck = &PubAck{Stream: name}
				resp.Error = NewJSMessageTTLDisabledError()
				b, _ := json.Marshal(resp)
				outq.sendMsg(reply, b)
			}
			return errMsgTTLDisabled
		}

		// Dedupe detection. This is done at the cluster level for dedupe detectiom above the
		// lower layers. But we still need to pull out the msgId.
		if msgId = getMsgId(hdr); msgId != _EMPTY_ {
			// Do real check only if not clustered or traceOnly flag is set.
			if !isClustered || traceOnly {
				if dde := mset.checkMsgId(msgId); dde != nil {
					mset.mu.Unlock()
					bumpCLFS()
					if canRespond {
						response := append(pubAck, strconv.FormatUint(dde.seq, 10)...)
						response = append(response, ",\"duplicate\": true}"...)
						outq.sendMsg(reply, response)
					}
					return errMsgIdDuplicate
				}
			}
		}

		// Expected last sequence per subject.
		if seq, exists := getExpectedLastSeqPerSubject(hdr); exists {
			// Allow override of the subject used for the check.
			seqSubj := subject
			if optSubj := getExpectedLastSeqPerSubjectForSubject(hdr); optSubj != _EMPTY_ {
				seqSubj = optSubj
			}

			// TODO(dlc) - We could make a new store func that does this all in one.
			var smv StoreMsg
			var fseq uint64
			sm, err := store.LoadLastMsg(seqSubj, &smv)
			if sm != nil {
				fseq = sm.seq
			}
			if err == ErrStoreMsgNotFound {
				if seq == 0 {
					fseq, err = 0, nil
				} else if mset.isClustered() {
					// Do not bump clfs in case message was not found and could have been deleted.
					var ss StreamState
					store.FastState(&ss)
					if seq <= ss.LastSeq {
						fseq, err = seq, nil
					}
				}
			}
			if err != nil || fseq != seq {
				mset.mu.Unlock()
				bumpCLFS()
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = NewJSStreamWrongLastSequenceError(fseq)
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return fmt.Errorf("last sequence by subject mismatch: %d vs %d", seq, fseq)
			}
		}

		// Expected last sequence.
		if seq, exists := getExpectedLastSeq(hdr); exists && seq != mset.lseq {
			mlseq := mset.lseq
			mset.mu.Unlock()
			bumpCLFS()
			if canRespond {
				resp.PubAck = &PubAck{Stream: name}
				resp.Error = NewJSStreamWrongLastSequenceError(mlseq)
				b, _ := json.Marshal(resp)
				outq.sendMsg(reply, b)
			}
			return fmt.Errorf("last sequence mismatch: %d vs %d", seq, mlseq)
		}
		// Expected last msgId.
		if lmsgId := getExpectedLastMsgId(hdr); lmsgId != _EMPTY_ {
			if mset.lmsgId == _EMPTY_ && !mset.ddloaded {
				mset.rebuildDedupe()
			}
			if lmsgId != mset.lmsgId {
				last := mset.lmsgId
				mset.mu.Unlock()
				bumpCLFS()
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = NewJSStreamWrongLastMsgIDError(last)
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return fmt.Errorf("last msgid mismatch: %q vs %q", lmsgId, last)
			}
		}
		// Check for any rollups.
		if rollup := getRollup(hdr); rollup != _EMPTY_ {
			if !mset.cfg.AllowRollup || mset.cfg.DenyPurge {
				mset.mu.Unlock()
				bumpCLFS()
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = NewJSStreamRollupFailedError(errors.New("rollup not permitted"))
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return errors.New("rollup not permitted")
			}
			switch rollup {
			case JSMsgRollupSubject:
				rollupSub = true
			case JSMsgRollupAll:
				rollupAll = true
			default:
				mset.mu.Unlock()
				bumpCLFS()
				err := fmt.Errorf("rollup value invalid: %q", rollup)
				if canRespond {
					resp.PubAck = &PubAck{Stream: name}
					resp.Error = NewJSStreamRollupFailedError(err)
					b, _ := json.Marshal(resp)
					outq.sendMsg(reply, b)
				}
				return err
			}
		}
	}

	// Response Ack.
	var (
		response []byte
		seq      uint64
		err      error
	)

	// Check to see if we are over the max msg size.
	if maxMsgSize >= 0 && (len(hdr)+len(msg)) > maxMsgSize {
		mset.mu.Unlock()
		bumpCLFS()
		if canRespond {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = NewJSStreamMessageExceedsMaximumError()
			response, _ = json.Marshal(resp)
			mset.outq.sendMsg(reply, response)
		}
		return ErrMaxPayload
	}

	if len(hdr) > math.MaxUint16 {
		mset.mu.Unlock()
		bumpCLFS()
		if canRespond {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = NewJSStreamHeaderExceedsMaximumError()
			response, _ = json.Marshal(resp)
			mset.outq.sendMsg(reply, response)
		}
		return ErrMaxPayload
	}

	// Check to see if we have exceeded our limits.
	if js.limitsExceeded(stype) {
		s.resourcesExceededError()
		mset.mu.Unlock()
		bumpCLFS()
		if canRespond {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = NewJSInsufficientResourcesError()
			response, _ = json.Marshal(resp)
			mset.outq.sendMsg(reply, response)
		}
		// Stepdown regardless.
		if node := mset.raftNode(); node != nil {
			node.StepDown()
		}
		return NewJSInsufficientResourcesError()
	}

	var noInterest bool

	// If we are interest based retention and have no consumers then we can skip.
	if interestRetention {
		mset.clsMu.RLock()
		noInterest = numConsumers == 0 || mset.csl == nil || !mset.csl.HasInterest(subject)
		mset.clsMu.RUnlock()
	}

	// Grab timestamp if not already set.
	if ts == 0 && lseq > 0 {
		ts = time.Now().UnixNano()
	}

	mt.updateJetStreamEvent(subject, noInterest)
	if traceOnly {
		mset.mu.Unlock()
		return nil
	}

	// Skip msg here.
	if noInterest {
		mset.lseq = store.SkipMsg()
		mset.lmsgId = msgId
		// If we have a msgId make sure to save.
		if msgId != _EMPTY_ {
			mset.storeMsgIdLocked(&ddentry{msgId, mset.lseq, ts})
		}
		if canRespond {
			response = append(pubAck, strconv.FormatUint(mset.lseq, 10)...)
			response = append(response, '}')
			mset.outq.sendMsg(reply, response)
		}
		mset.mu.Unlock()
		return nil
	}

	// If here we will attempt to store the message.
	// Assume this will succeed.
	olmsgId := mset.lmsgId
	mset.lmsgId = msgId
	clfs := mset.clfs
	mset.lseq++
	tierName := mset.tier

	// Republish state if needed.
	var tsubj string
	var tlseq uint64
	var thdrsOnly bool
	if mset.tr != nil {
		tsubj, _ = mset.tr.Match(subject)
		if mset.cfg.RePublish != nil {
			thdrsOnly = mset.cfg.RePublish.HeadersOnly
		}
	}
	republish := tsubj != _EMPTY_ && isLeader

	// If we are republishing grab last sequence for this exact subject. Aids in gap detection for lightweight clients.
	if republish {
		var smv StoreMsg
		if sm, _ := store.LoadLastMsg(subject, &smv); sm != nil {
			tlseq = sm.seq
		}
	}

	// If clustered this was already checked and we do not want to check here and possibly introduce skew.
	if !isClustered {
		if exceeded, err := jsa.wouldExceedLimits(stype, tierName, mset.cfg.Replicas, subject, hdr, msg); exceeded {
			if err == nil {
				err = NewJSAccountResourcesExceededError()
			}
			s.RateLimitWarnf("JetStream resource limits exceeded for account: %q", accName)
			if canRespond {
				resp.PubAck = &PubAck{Stream: name}
				resp.Error = err
				response, _ = json.Marshal(resp)
				mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, nil, response, nil, 0))
			}
			mset.mu.Unlock()
			return err
		}
	}

	// Find the message TTL if any.
	ttl, err := getMessageTTL(hdr)
	if err != nil {
		if canRespond {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = NewJSMessageTTLInvalidError()
			response, _ = json.Marshal(resp)
			mset.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, nil, response, nil, 0))
		}
		mset.mu.Unlock()
		return err
	}

	// Store actual msg.
	if lseq == 0 && ts == 0 {
		seq, ts, err = store.StoreMsg(subject, hdr, msg, ttl)
	} else {
		// Make sure to take into account any message assignments that we had to skip (clfs).
		seq = lseq + 1 - clfs
		// Check for preAcks and the need to clear it.
		if mset.hasAllPreAcks(seq, subject) {
			mset.clearAllPreAcks(seq)
		}
		err = store.StoreRawMsg(subject, hdr, msg, seq, ts, ttl)
	}

	if err != nil {
		if isPermissionError(err) {
			mset.mu.Unlock()
			// messages in block cache could be lost in the worst case.
			// In the clustered mode it is very highly unlikely as a result of replication.
			mset.srv.DisableJetStream()
			mset.srv.Warnf("Filesystem permission denied while writing msg, disabling JetStream: %v", err)
			return err
		}
		// If we did not succeed put those values back and increment clfs in case we are clustered.
		var state StreamState
		mset.store.FastState(&state)
		mset.lseq = state.LastSeq
		mset.lmsgId = olmsgId
		mset.mu.Unlock()
		bumpCLFS()

		switch err {
		case ErrMaxMsgs, ErrMaxBytes, ErrMaxMsgsPerSubject, ErrMsgTooLarge:
			s.RateLimitDebugf("JetStream failed to store a msg on stream '%s > %s': %v", accName, name, err)
		case ErrStoreClosed:
		default:
			s.Errorf("JetStream failed to store a msg on stream '%s > %s': %v", accName, name, err)
		}

		if canRespond {
			resp.PubAck = &PubAck{Stream: name}
			resp.Error = NewJSStreamStoreFailedError(err, Unless(err))
			response, _ = json.Marshal(resp)
			mset.outq.sendMsg(reply, response)
		}
		return err
	}

	// If we have a msgId make sure to save.
	// This will replace our estimate from the cluster layer if we are clustered.
	if msgId != _EMPTY_ {
		if isClustered && isLeader && mset.ddmap != nil {
			if dde := mset.ddmap[msgId]; dde != nil {
				dde.seq, dde.ts = seq, ts
			} else {
				mset.storeMsgIdLocked(&ddentry{msgId, seq, ts})
			}
		} else {
			// R1 or not leader..
			mset.storeMsgIdLocked(&ddentry{msgId, seq, ts})
		}
	}

	// If here we succeeded in storing the message.
	mset.mu.Unlock()

	// No errors, this is the normal path.
	if rollupSub {
		mset.purge(&JSApiStreamPurgeRequest{Subject: subject, Keep: 1})
	} else if rollupAll {
		mset.purge(&JSApiStreamPurgeRequest{Keep: 1})
	}

	// Check for republish.
	if republish {
		const ht = "NATS/1.0\r\nNats-Stream: %s\r\nNats-Subject: %s\r\nNats-Sequence: %d\r\nNats-Time-Stamp: %s\r\nNats-Last-Sequence: %d\r\n\r\n"
		const htho = "NATS/1.0\r\nNats-Stream: %s\r\nNats-Subject: %s\r\nNats-Sequence: %d\r\nNats-Time-Stamp: %s\r\nNats-Last-Sequence: %d\r\nNats-Msg-Size: %d\r\n\r\n"
		// When adding to existing headers, will use the fmt.Append version so this skips the headers from above.
		const hoff = 10

		tsStr := time.Unix(0, ts).UTC().Format(time.RFC3339Nano)
		var rpMsg []byte
		if len(hdr) == 0 {
			if !thdrsOnly {
				hdr = fmt.Appendf(nil, ht, name, subject, seq, tsStr, tlseq)
				rpMsg = copyBytes(msg)
			} else {
				hdr = fmt.Appendf(nil, htho, name, subject, seq, tsStr, tlseq, len(msg))
			}
		} else {
			// use hdr[:end:end] to make sure as we add we copy the original hdr.
			end := len(hdr) - LEN_CR_LF
			if !thdrsOnly {
				hdr = fmt.Appendf(hdr[:end:end], ht[hoff:], name, subject, seq, tsStr, tlseq)
				rpMsg = copyBytes(msg)
			} else {
				hdr = fmt.Appendf(hdr[:end:end], htho[hoff:], name, subject, seq, tsStr, tlseq, len(msg))
			}
		}
		mset.outq.send(newJSPubMsg(tsubj, _EMPTY_, _EMPTY_, hdr, rpMsg, nil, seq))
	}

	// Send response here.
	if canRespond {
		response = append(pubAck, strconv.FormatUint(seq, 10)...)
		response = append(response, '}')
		mset.outq.sendMsg(reply, response)
	}

	// Signal consumers for new messages.
	if numConsumers > 0 {
		mset.sigq.push(newCMsg(subject, seq))
		select {
		case mset.sch <- struct{}{}:
		default:
		}
	}

	return nil
}

// Used to signal inbound message to registered consumers.
type cMsg struct {
	seq  uint64
	subj string
}

// Pool to recycle consumer bound msgs.
var cMsgPool sync.Pool

// Used to queue up consumer bound msgs for signaling.
func newCMsg(subj string, seq uint64) *cMsg {
	var m *cMsg
	cm := cMsgPool.Get()
	if cm != nil {
		m = cm.(*cMsg)
	} else {
		m = new(cMsg)
	}
	m.subj, m.seq = subj, seq

	return m
}

func (m *cMsg) returnToPool() {
	if m == nil {
		return
	}
	m.subj, m.seq = _EMPTY_, 0
	cMsgPool.Put(m)
}

// Go routine to signal consumers.
// Offloaded from stream msg processing.
func (mset *stream) signalConsumersLoop() {
	mset.mu.RLock()
	s, qch, sch, msgs := mset.srv, mset.qch, mset.sch, mset.sigq
	mset.mu.RUnlock()

	for {
		select {
		case <-s.quitCh:
			return
		case <-qch:
			return
		case <-sch:
			cms := msgs.pop()
			for _, m := range cms {
				seq, subj := m.seq, m.subj
				m.returnToPool()
				// Signal all appropriate consumers.
				mset.signalConsumers(subj, seq)
			}
			msgs.recycle(&cms)
		}
	}
}

// This will update and signal all consumers that match.
func (mset *stream) signalConsumers(subj string, seq uint64) {
	mset.clsMu.RLock()
	defer mset.clsMu.RUnlock()
	csl := mset.csl
	if csl == nil {
		return
	}
	csl.Match(subj, func(o *consumer) {
		o.processStreamSignal(seq)
	})
}

// Internal message for use by jetstream subsystem.
type jsPubMsg struct {
	dsubj string // Subject to send to, e.g. _INBOX.xxx
	reply string
	StoreMsg
	o *consumer
}

var jsPubMsgPool sync.Pool

func newJSPubMsg(dsubj, subj, reply string, hdr, msg []byte, o *consumer, seq uint64) *jsPubMsg {
	var m *jsPubMsg
	var buf []byte
	pm := jsPubMsgPool.Get()
	if pm != nil {
		m = pm.(*jsPubMsg)
		buf = m.buf[:0]
		if hdr != nil {
			hdr = append(m.hdr[:0], hdr...)
		}
	} else {
		m = new(jsPubMsg)
	}
	// When getting something from a pool it is critical that all fields are
	// initialized. Doing this way guarantees that if someone adds a field to
	// the structure, the compiler will fail the build if this line is not updated.
	(*m) = jsPubMsg{dsubj, reply, StoreMsg{subj, hdr, msg, buf, seq, 0}, o}

	return m
}

// Gets a jsPubMsg from the pool.
func getJSPubMsgFromPool() *jsPubMsg {
	pm := jsPubMsgPool.Get()
	if pm != nil {
		return pm.(*jsPubMsg)
	}
	return new(jsPubMsg)
}

func (pm *jsPubMsg) returnToPool() {
	if pm == nil {
		return
	}
	pm.subj, pm.dsubj, pm.reply, pm.hdr, pm.msg, pm.o = _EMPTY_, _EMPTY_, _EMPTY_, nil, nil, nil
	if len(pm.buf) > 0 {
		pm.buf = pm.buf[:0]
	}
	if len(pm.hdr) > 0 {
		pm.hdr = pm.hdr[:0]
	}
	jsPubMsgPool.Put(pm)
}

func (pm *jsPubMsg) size() int {
	if pm == nil {
		return 0
	}
	return len(pm.dsubj) + len(pm.reply) + len(pm.hdr) + len(pm.msg)
}

// Queue of *jsPubMsg for sending internal system messages.
type jsOutQ struct {
	*ipQueue[*jsPubMsg]
}

func (q *jsOutQ) sendMsg(subj string, msg []byte) {
	if q != nil {
		q.send(newJSPubMsg(subj, _EMPTY_, _EMPTY_, nil, msg, nil, 0))
	}
}

func (q *jsOutQ) send(msg *jsPubMsg) {
	if q == nil || msg == nil {
		return
	}
	q.push(msg)
}

func (q *jsOutQ) unregister() {
	if q == nil {
		return
	}
	q.ipQueue.unregister()
}

// StoredMsg is for raw access to messages in a stream.
type StoredMsg struct {
	Subject  string    `json:"subject"`
	Sequence uint64    `json:"seq"`
	Header   []byte    `json:"hdrs,omitempty"`
	Data     []byte    `json:"data,omitempty"`
	Time     time.Time `json:"time"`
}

// This is similar to system semantics but did not want to overload the single system sendq,
// or require system account when doing simple setup with jetstream.
func (mset *stream) setupSendCapabilities() {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	if mset.outq != nil {
		return
	}
	qname := fmt.Sprintf("[ACC:%s] stream '%s' sendQ", mset.acc.Name, mset.cfg.Name)
	mset.outq = &jsOutQ{newIPQueue[*jsPubMsg](mset.srv, qname)}
	go mset.internalLoop()
}

// Returns the associated account name.
func (mset *stream) accName() string {
	if mset == nil {
		return _EMPTY_
	}
	mset.mu.RLock()
	acc := mset.acc
	mset.mu.RUnlock()
	return acc.Name
}

// Name returns the stream name.
func (mset *stream) name() string {
	if mset == nil {
		return _EMPTY_
	}
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.cfg.Name
}

func (mset *stream) internalLoop() {
	mset.mu.RLock()
	setGoRoutineLabels(pprofLabels{
		"account": mset.acc.Name,
		"stream":  mset.cfg.Name,
	})
	s := mset.srv
	c := s.createInternalJetStreamClient()
	c.registerWithAccount(mset.acc)
	defer c.closeConnection(ClientClosed)
	outq, qch, msgs, gets := mset.outq, mset.qch, mset.msgs, mset.gets

	// For the ack msgs queue for interest retention.
	var (
		amch chan struct{}
		ackq *ipQueue[uint64]
	)
	if mset.ackq != nil {
		ackq, amch = mset.ackq, mset.ackq.ch
	}
	mset.mu.RUnlock()

	// Raw scratch buffer.
	// This should be rarely used now so can be smaller.
	var _r [1024]byte

	// To optimize for not converting a string to a []byte slice.
	var (
		subj  [256]byte
		dsubj [256]byte
		rply  [256]byte
		szb   [10]byte
		hdb   [10]byte
	)

	for {
		select {
		case <-outq.ch:
			pms := outq.pop()
			for _, pm := range pms {
				c.pa.subject = append(dsubj[:0], pm.dsubj...)
				c.pa.deliver = append(subj[:0], pm.subj...)
				c.pa.size = len(pm.msg) + len(pm.hdr)
				c.pa.szb = append(szb[:0], strconv.Itoa(c.pa.size)...)
				if len(pm.reply) > 0 {
					c.pa.reply = append(rply[:0], pm.reply...)
				} else {
					c.pa.reply = nil
				}

				// If we have an underlying buf that is the wire contents for hdr + msg, else construct on the fly.
				var msg []byte
				if len(pm.buf) > 0 {
					msg = pm.buf
				} else {
					if len(pm.hdr) > 0 {
						msg = pm.hdr
						if len(pm.msg) > 0 {
							msg = _r[:0]
							msg = append(msg, pm.hdr...)
							msg = append(msg, pm.msg...)
						}
					} else if len(pm.msg) > 0 {
						// We own this now from a low level buffer perspective so can use directly here.
						msg = pm.msg
					}
				}

				if len(pm.hdr) > 0 {
					c.pa.hdr = len(pm.hdr)
					c.pa.hdb = []byte(strconv.Itoa(c.pa.hdr))
					c.pa.hdb = append(hdb[:0], strconv.Itoa(c.pa.hdr)...)
				} else {
					c.pa.hdr = -1
					c.pa.hdb = nil
				}

				msg = append(msg, _CRLF_...)

				didDeliver, _ := c.processInboundClientMsg(msg)
				c.pa.szb, c.pa.subject, c.pa.deliver = nil, nil, nil

				// Check to see if this is a delivery for a consumer and
				// we failed to deliver the message. If so alert the consumer.
				if pm.o != nil && pm.seq > 0 && !didDeliver {
					pm.o.didNotDeliver(pm.seq, pm.dsubj)
				}
				pm.returnToPool()
			}
			// TODO: Move in the for-loop?
			c.flushClients(0)
			outq.recycle(&pms)
		case <-msgs.ch:
			// This can possibly change now so needs to be checked here.
			isClustered := mset.IsClustered()
			ims := msgs.pop()
			for _, im := range ims {
				// If we are clustered we need to propose this message to the underlying raft group.
				if isClustered {
					mset.processClusteredInboundMsg(im.subj, im.rply, im.hdr, im.msg, im.mt, false)
				} else {
					mset.processJetStreamMsg(im.subj, im.rply, im.hdr, im.msg, 0, 0, im.mt, false)
				}
				im.returnToPool()
			}
			msgs.recycle(&ims)
		case <-gets.ch:
			dgs := gets.pop()
			for _, dg := range dgs {
				mset.getDirectRequest(&dg.req, dg.reply)
				dgPool.Put(dg)
			}
			gets.recycle(&dgs)

		case <-amch:
			seqs := ackq.pop()
			for _, seq := range seqs {
				mset.ackMsg(nil, seq)
			}
			ackq.recycle(&seqs)
		case <-qch:
			return
		case <-s.quitCh:
			return
		}
	}
}

// Used to break consumers out of their monitorConsumer go routines.
func (mset *stream) resetAndWaitOnConsumers() {
	mset.mu.RLock()
	consumers := make([]*consumer, 0, len(mset.consumers))
	for _, o := range mset.consumers {
		consumers = append(consumers, o)
	}
	mset.mu.RUnlock()

	for _, o := range consumers {
		if node := o.raftNode(); node != nil {
			node.StepDown()
			node.Delete()
		}
		if o.isMonitorRunning() {
			o.monitorWg.Wait()
		}
	}
}

// Internal function to delete a stream.
func (mset *stream) delete() error {
	if mset == nil {
		return nil
	}
	return mset.stop(true, true)
}

// Internal function to stop or delete the stream.
func (mset *stream) stop(deleteFlag, advisory bool) error {
	mset.mu.RLock()
	js, jsa, name := mset.js, mset.jsa, mset.cfg.Name
	mset.mu.RUnlock()

	if jsa == nil {
		return NewJSNotEnabledForAccountError()
	}

	// Remove from our account map first.
	jsa.mu.Lock()
	delete(jsa.streams, name)
	accName := jsa.account.Name
	jsa.mu.Unlock()

	// Kick monitor and collect consumers first.
	mset.mu.Lock()

	// Mark closed.
	mset.closed.Store(true)

	// Signal to the monitor loop.
	// Can't use qch here.
	if mset.mqch != nil {
		close(mset.mqch)
		mset.mqch = nil
	}

	// Stop responding to sync requests.
	mset.stopClusterSubs()
	// Unsubscribe from direct stream.
	mset.unsubscribeToStream(true)

	// Our info sub if we spun it up.
	if mset.infoSub != nil {
		mset.srv.sysUnsubscribe(mset.infoSub)
		mset.infoSub = nil
	}

	// Clean up consumers.
	var obs []*consumer
	for _, o := range mset.consumers {
		obs = append(obs, o)
	}
	mset.clsMu.Lock()
	mset.consumers, mset.cList, mset.csl = nil, nil, nil
	mset.clsMu.Unlock()

	// Check if we are a mirror.
	if mset.mirror != nil && mset.mirror.sub != nil {
		mset.unsubscribe(mset.mirror.sub)
		mset.mirror.sub = nil
		mset.removeInternalConsumer(mset.mirror)
	}
	// Now check for sources.
	if len(mset.sources) > 0 {
		for _, si := range mset.sources {
			mset.cancelSourceConsumer(si.iname)
		}
	}
	mset.mu.Unlock()

	isShuttingDown := js.isShuttingDown()
	for _, o := range obs {
		if !o.isClosed() {
			// Third flag says do not broadcast a signal.
			// TODO(dlc) - If we have an err here we don't want to stop
			// but should we log?
			o.stopWithFlags(deleteFlag, deleteFlag, false, advisory)
			if !isShuttingDown {
				o.monitorWg.Wait()
			}
		}
	}

	mset.mu.Lock()
	// Send stream delete advisory after the consumers.
	if deleteFlag && advisory {
		mset.sendDeleteAdvisoryLocked()
	}

	// Quit channel, do this after sending the delete advisory
	if mset.qch != nil {
		close(mset.qch)
		mset.qch = nil
	}

	// Cluster cleanup
	var sa *streamAssignment
	if n := mset.node; n != nil {
		if deleteFlag {
			n.Delete()
			sa = mset.sa
		} else {
			n.Stop()
		}
	}

	// Cleanup duplicate timer if running.
	if mset.ddtmr != nil {
		mset.ddtmr.Stop()
		mset.ddtmr = nil
		mset.ddmap = nil
		mset.ddarr = nil
		mset.ddindex = 0
	}

	sysc := mset.sysc
	mset.sysc = nil

	if deleteFlag {
		// Unregistering ipQueues do not prevent them from push/pop
		// just will remove them from the central monitoring map
		mset.msgs.unregister()
		mset.ackq.unregister()
		mset.outq.unregister()
		mset.sigq.unregister()
		mset.smsgs.unregister()
	}

	// Snapshot store.
	store := mset.store
	c := mset.client

	// Clustered cleanup.
	mset.mu.Unlock()

	// Check if the stream assignment has the group node specified.
	// We need this cleared for if the stream gets reassigned here.
	if sa != nil {
		js.mu.Lock()
		if sa.Group != nil {
			sa.Group.node = nil
		}
		js.mu.Unlock()
	}

	if c != nil {
		c.closeConnection(ClientClosed)
	}

	if sysc != nil {
		sysc.closeConnection(ClientClosed)
	}

	if deleteFlag {
		if store != nil {
			// Ignore errors.
			store.Delete()
		}
		// Release any resources.
		js.releaseStreamResources(&mset.cfg)
		// cleanup directories after the stream
		accDir := filepath.Join(js.config.StoreDir, accName)
		// Do cleanup in separate go routine similar to how fs will use purge here..
		go func() {
			// no op if not empty
			os.Remove(filepath.Join(accDir, streamsDir))
			os.Remove(accDir)
		}()
	} else if store != nil {
		// Ignore errors.
		store.Stop()
	}

	return nil
}

func (mset *stream) getMsg(seq uint64) (*StoredMsg, error) {
	var smv StoreMsg
	sm, err := mset.store.LoadMsg(seq, &smv)
	if err != nil {
		return nil, err
	}
	// This only used in tests directly so no need to pool etc.
	return &StoredMsg{
		Subject:  sm.subj,
		Sequence: sm.seq,
		Header:   sm.hdr,
		Data:     sm.msg,
		Time:     time.Unix(0, sm.ts).UTC(),
	}, nil
}

// getConsumers will return a copy of all the current consumers for this stream.
func (mset *stream) getConsumers() []*consumer {
	mset.clsMu.RLock()
	defer mset.clsMu.RUnlock()
	return append([]*consumer(nil), mset.cList...)
}

// Lock should be held for this one.
func (mset *stream) numPublicConsumers() int {
	return len(mset.consumers) - mset.directs
}

// This returns all consumers that are not DIRECT.
func (mset *stream) getPublicConsumers() []*consumer {
	mset.clsMu.RLock()
	defer mset.clsMu.RUnlock()

	var obs []*consumer
	for _, o := range mset.cList {
		if !o.cfg.Direct {
			obs = append(obs, o)
		}
	}
	return obs
}

// 2 minutes plus up to 30s jitter.
const (
	defaultCheckInterestStateT = 2 * time.Minute
	defaultCheckInterestStateJ = 30
)

var (
	checkInterestStateT = defaultCheckInterestStateT // Interval
	checkInterestStateJ = defaultCheckInterestStateJ // Jitter (secs)
)

// Will check for interest retention and make sure messages
// that have been acked are processed and removed.
// This will check the ack floors of all consumers, and adjust our first sequence accordingly.
func (mset *stream) checkInterestState() {
	if mset == nil || !mset.isInterestRetention() {
		// If we are limits based nothing to do.
		return
	}

	var ss StreamState
	mset.store.FastState(&ss)

	for _, o := range mset.getConsumers() {
		o.checkStateForInterestStream(&ss)
	}
}

func (mset *stream) isInterestRetention() bool {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.cfg.Retention != LimitsPolicy
}

// NumConsumers reports on number of active consumers for this stream.
func (mset *stream) numConsumers() int {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return len(mset.consumers)
}

// Lock should be held.
func (mset *stream) setConsumer(o *consumer) {
	mset.consumers[o.name] = o
	if len(o.subjf) > 0 {
		mset.numFilter++
	}
	if o.cfg.Direct {
		mset.directs++
	}
	// Now update consumers list as well
	mset.clsMu.Lock()
	mset.cList = append(mset.cList, o)
	if mset.csl == nil {
		mset.csl = gsl.NewSublist[*consumer]()
	}
	for _, sub := range o.signalSubs() {
		mset.csl.Insert(sub, o)
	}
	mset.clsMu.Unlock()
}

// Lock should be held.
func (mset *stream) removeConsumer(o *consumer) {
	if o.cfg.FilterSubject != _EMPTY_ && mset.numFilter > 0 {
		mset.numFilter--
	}
	if o.cfg.Direct && mset.directs > 0 {
		mset.directs--
	}
	if mset.consumers != nil {
		delete(mset.consumers, o.name)
		// Now update consumers list as well
		mset.clsMu.Lock()
		for i, ol := range mset.cList {
			if ol == o {
				mset.cList = append(mset.cList[:i], mset.cList[i+1:]...)
				break
			}
		}
		// Always remove from the leader sublist.
		if mset.csl != nil {
			for _, sub := range o.signalSubs() {
				mset.csl.Remove(sub, o)
			}
		}
		mset.clsMu.Unlock()
	}
}

// swapSigSubs will update signal Subs for a new subject filter.
// consumer lock should not be held.
func (mset *stream) swapSigSubs(o *consumer, newFilters []string) {
	mset.clsMu.Lock()
	o.mu.Lock()

	if o.closed || o.mset == nil {
		o.mu.Unlock()
		mset.clsMu.Unlock()
		return
	}

	if o.sigSubs != nil {
		if mset.csl != nil {
			for _, sub := range o.sigSubs {
				mset.csl.Remove(sub, o)
			}
		}
		o.sigSubs = nil
	}

	if o.isLeader() {
		if mset.csl == nil {
			mset.csl = gsl.NewSublist[*consumer]()
		}
		// If no filters are preset, add fwcs to sublist for that consumer.
		if newFilters == nil {
			mset.csl.Insert(fwcs, o)
			o.sigSubs = append(o.sigSubs, fwcs)
			// If there are filters, add their subjects to sublist.
		} else {
			for _, filter := range newFilters {
				mset.csl.Insert(filter, o)
				o.sigSubs = append(o.sigSubs, filter)
			}
		}
	}
	o.mu.Unlock()
	mset.clsMu.Unlock()

	mset.mu.Lock()
	defer mset.mu.Unlock()

	if mset.numFilter > 0 && len(o.subjf) > 0 {
		mset.numFilter--
	}
	if len(newFilters) > 0 {
		mset.numFilter++
	}
}

// lookupConsumer will retrieve a consumer by name.
func (mset *stream) lookupConsumer(name string) *consumer {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.consumers[name]
}

func (mset *stream) numDirectConsumers() (num int) {
	mset.clsMu.RLock()
	defer mset.clsMu.RUnlock()

	// Consumers that are direct are not recorded at the store level.
	for _, o := range mset.cList {
		o.mu.RLock()
		if o.cfg.Direct {
			num++
		}
		o.mu.RUnlock()
	}
	return num
}

// State will return the current state for this stream.
func (mset *stream) state() StreamState {
	return mset.stateWithDetail(false)
}

func (mset *stream) stateWithDetail(details bool) StreamState {
	// mset.store does not change once set, so ok to reference here directly.
	// We do this elsewhere as well.
	store := mset.store
	if store == nil {
		return StreamState{}
	}

	// Currently rely on store for details.
	if details {
		return store.State()
	}
	// Here we do the fast version.
	var state StreamState
	store.FastState(&state)
	return state
}

func (mset *stream) Store() StreamStore {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.store
}

// Determines if the new proposed partition is unique amongst all consumers.
// Lock should be held.
func (mset *stream) partitionUnique(name string, partitions []string) bool {
	for _, partition := range partitions {
		for n, o := range mset.consumers {
			// Skip the consumer being checked.
			if n == name {
				continue
			}
			if o.subjf == nil {
				return false
			}
			for _, filter := range o.subjf {
				if SubjectsCollide(partition, filter.subject) {
					return false
				}
			}
		}
	}
	return true
}

// Lock should be held.
func (mset *stream) potentialFilteredConsumers() bool {
	numSubjects := len(mset.cfg.Subjects)
	if len(mset.consumers) == 0 || numSubjects == 0 {
		return false
	}
	if numSubjects > 1 || subjectHasWildcard(mset.cfg.Subjects[0]) {
		return true
	}
	return false
}

// Check if there is no interest in this sequence number across our consumers.
// The consumer passed is optional if we are processing the ack for that consumer.
// Write lock should be held.
func (mset *stream) noInterest(seq uint64, obs *consumer) bool {
	return !mset.checkForInterest(seq, obs)
}

// Check if there is no interest in this sequence number and subject across our consumers.
// The consumer passed is optional if we are processing the ack for that consumer.
// Write lock should be held.
func (mset *stream) noInterestWithSubject(seq uint64, subj string, obs *consumer) bool {
	return !mset.checkForInterestWithSubject(seq, subj, obs)
}

// Write lock should be held here for the stream to avoid race conditions on state.
func (mset *stream) checkForInterest(seq uint64, obs *consumer) bool {
	var subj string
	if mset.potentialFilteredConsumers() {
		pmsg := getJSPubMsgFromPool()
		defer pmsg.returnToPool()
		sm, err := mset.store.LoadMsg(seq, &pmsg.StoreMsg)
		if err != nil {
			if err == ErrStoreEOF {
				// Register this as a preAck.
				mset.registerPreAck(obs, seq)
				return true
			}
			mset.clearAllPreAcks(seq)
			return false
		}
		subj = sm.subj
	}
	return mset.checkForInterestWithSubject(seq, subj, obs)
}

// Checks for interest given a sequence and subject.
func (mset *stream) checkForInterestWithSubject(seq uint64, subj string, obs *consumer) bool {
	for _, o := range mset.consumers {
		// If this is us or we have a registered preAck for this consumer continue inspecting.
		if o == obs || mset.hasPreAck(o, seq) {
			continue
		}
		// Check if we need an ack.
		if o.needAck(seq, subj) {
			return true
		}
	}
	mset.clearAllPreAcks(seq)
	return false
}

// Check if we have a pre-registered ack for this sequence.
// Write lock should be held.
func (mset *stream) hasPreAck(o *consumer, seq uint64) bool {
	if o == nil || len(mset.preAcks) == 0 {
		return false
	}
	consumers := mset.preAcks[seq]
	if len(consumers) == 0 {
		return false
	}
	_, found := consumers[o]
	return found
}

// Check if we have all consumers pre-acked for this sequence and subject.
// Write lock should be held.
func (mset *stream) hasAllPreAcks(seq uint64, subj string) bool {
	if len(mset.preAcks) == 0 || len(mset.preAcks[seq]) == 0 {
		return false
	}
	// Since these can be filtered and mutually exclusive,
	// if we have some preAcks we need to check all interest here.
	return mset.noInterestWithSubject(seq, subj, nil)
}

// Check if we have all consumers pre-acked.
// Write lock should be held.
func (mset *stream) clearAllPreAcks(seq uint64) {
	delete(mset.preAcks, seq)
}

// Clear all preAcks below floor.
// Write lock should be held.
func (mset *stream) clearAllPreAcksBelowFloor(floor uint64) {
	for seq := range mset.preAcks {
		if seq < floor {
			delete(mset.preAcks, seq)
		}
	}
}

// This will register an ack for a consumer if it arrives before the actual message.
func (mset *stream) registerPreAckLock(o *consumer, seq uint64) {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	mset.registerPreAck(o, seq)
}

// This will register an ack for a consumer if it arrives before
// the actual message.
// Write lock should be held.
func (mset *stream) registerPreAck(o *consumer, seq uint64) {
	if o == nil {
		return
	}
	if mset.preAcks == nil {
		mset.preAcks = make(map[uint64]map[*consumer]struct{})
	}
	if mset.preAcks[seq] == nil {
		mset.preAcks[seq] = make(map[*consumer]struct{})
	}
	mset.preAcks[seq][o] = struct{}{}
}

// This will clear an ack for a consumer.
// Write lock should be held.
func (mset *stream) clearPreAck(o *consumer, seq uint64) {
	if o == nil || len(mset.preAcks) == 0 {
		return
	}
	if consumers := mset.preAcks[seq]; len(consumers) > 0 {
		delete(consumers, o)
		if len(consumers) == 0 {
			delete(mset.preAcks, seq)
		}
	}
}

// ackMsg is called into from a consumer when we have a WorkQueue or Interest Retention Policy.
// Returns whether the message at seq was removed as a result of the ACK.
// (Or should be removed in the case of clustered streams, since it requires a message delete proposal)
func (mset *stream) ackMsg(o *consumer, seq uint64) bool {
	if seq == 0 {
		return false
	}

	// Don't make this RLock(). We need to have only 1 running at a time to gauge interest across all consumers.
	mset.mu.Lock()
	if mset.closed.Load() || mset.cfg.Retention == LimitsPolicy {
		mset.mu.Unlock()
		return false
	}

	store := mset.store
	var state StreamState
	store.FastState(&state)

	// If this has arrived before we have processed the message itself.
	if seq > state.LastSeq {
		mset.registerPreAck(o, seq)
		mset.mu.Unlock()
		// We have not removed the message, but should still signal so we could retry later
		// since we potentially need to remove it then.
		return true
	}

	// Always clear pre-ack if here.
	mset.clearPreAck(o, seq)

	// Make sure this sequence is not below our first sequence.
	if seq < state.FirstSeq {
		mset.mu.Unlock()
		return false
	}

	var shouldRemove bool
	switch mset.cfg.Retention {
	case WorkQueuePolicy:
		// Normally we just remove a message when its ack'd here but if we have direct consumers
		// from sources and/or mirrors we need to make sure they have delivered the msg.
		shouldRemove = mset.directs <= 0 || mset.noInterest(seq, o)
	case InterestPolicy:
		shouldRemove = mset.noInterest(seq, o)
	}

	// If nothing else to do.
	if !shouldRemove {
		mset.mu.Unlock()
		return false
	}

	if !mset.isClustered() {
		mset.mu.Unlock()
		// If we are here we should attempt to remove.
		if _, err := store.RemoveMsg(seq); err == ErrStoreEOF {
			// This should not happen, but being pedantic.
			mset.registerPreAckLock(o, seq)
		}
		return true
	}

	// Only propose message deletion to the stream if we're consumer leader, otherwise all followers would also propose.
	// We must be the consumer leader, since we know for sure we've stored the message and don't register as pre-ack.
	if o != nil && !o.IsLeader() {
		mset.mu.Unlock()
		// Must still mark as removal if follower. If we become leader later, we must be able to retry the proposal.
		return true
	}

	md := streamMsgDelete{Seq: seq, NoErase: true, Stream: mset.cfg.Name}
	mset.node.ForwardProposal(encodeMsgDelete(&md))
	mset.mu.Unlock()
	return true
}

// Snapshot creates a snapshot for the stream and possibly consumers.
func (mset *stream) snapshot(deadline time.Duration, checkMsgs, includeConsumers bool) (*SnapshotResult, error) {
	if mset.closed.Load() {
		return nil, errStreamClosed
	}
	store := mset.store
	return store.Snapshot(deadline, checkMsgs, includeConsumers)
}

const snapsDir = "__snapshots__"

// RestoreStream will restore a stream from a snapshot.
func (a *Account) RestoreStream(ncfg *StreamConfig, r io.Reader) (*stream, error) {
	if ncfg == nil {
		return nil, errors.New("nil config on stream restore")
	}

	s, jsa, err := a.checkForJetStream()
	if err != nil {
		return nil, err
	}

	cfg, apiErr := s.checkStreamCfg(ncfg, a, false)
	if apiErr != nil {
		return nil, apiErr
	}

	sd := filepath.Join(jsa.storeDir, snapsDir)
	if _, err := os.Stat(sd); os.IsNotExist(err) {
		if err := os.MkdirAll(sd, defaultDirPerms); err != nil {
			return nil, fmt.Errorf("could not create snapshots directory - %v", err)
		}
	}
	sdir, err := os.MkdirTemp(sd, "snap-")
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(sdir); os.IsNotExist(err) {
		if err := os.MkdirAll(sdir, defaultDirPerms); err != nil {
			return nil, fmt.Errorf("could not create snapshots directory - %v", err)
		}
	}
	defer os.RemoveAll(sdir)

	logAndReturnError := func() error {
		a.mu.RLock()
		err := fmt.Errorf("unexpected content (account=%s)", a.Name)
		if a.srv != nil {
			a.srv.Errorf("Stream restore failed due to %v", err)
		}
		a.mu.RUnlock()
		return err
	}
	sdirCheck := filepath.Clean(sdir) + string(os.PathSeparator)

	tr := tar.NewReader(s2.NewReader(r))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break // End of snapshot
		}
		if err != nil {
			return nil, err
		}
		if hdr.Typeflag != tar.TypeReg {
			return nil, logAndReturnError()
		}
		fpath := filepath.Join(sdir, filepath.Clean(hdr.Name))
		if !strings.HasPrefix(fpath, sdirCheck) {
			return nil, logAndReturnError()
		}
		os.MkdirAll(filepath.Dir(fpath), defaultDirPerms)
		fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		_, err = io.Copy(fd, tr)
		fd.Close()
		if err != nil {
			return nil, err
		}
	}

	// Check metadata.
	// The cfg passed in will be the new identity for the stream.
	var fcfg FileStreamInfo
	b, err := os.ReadFile(filepath.Join(sdir, JetStreamMetaFile))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &fcfg); err != nil {
		return nil, err
	}

	// Check to make sure names match.
	if fcfg.Name != cfg.Name {
		return nil, errors.New("stream names do not match")
	}

	// See if this stream already exists.
	if _, err := a.lookupStream(cfg.Name); err == nil {
		return nil, NewJSStreamNameExistRestoreFailedError()
	}
	// Move into the correct place here.
	ndir := filepath.Join(jsa.storeDir, streamsDir, cfg.Name)
	// Remove old one if for some reason it is still here.
	if _, err := os.Stat(ndir); err == nil {
		os.RemoveAll(ndir)
	}
	// Make sure our destination streams directory exists.
	if err := os.MkdirAll(filepath.Join(jsa.storeDir, streamsDir), defaultDirPerms); err != nil {
		return nil, err
	}
	// Move into new location.
	if err := os.Rename(sdir, ndir); err != nil {
		return nil, err
	}

	if cfg.Template != _EMPTY_ {
		if err := jsa.addStreamNameToTemplate(cfg.Template, cfg.Name); err != nil {
			return nil, err
		}
	}
	mset, err := a.addStream(&cfg)
	if err != nil {
		// Make sure to clean up after ourselves here.
		os.RemoveAll(ndir)
		return nil, err
	}
	if !fcfg.Created.IsZero() {
		mset.setCreatedTime(fcfg.Created)
	}
	lseq := mset.lastSeq()

	// Make sure we do an update if the configs have changed.
	if !reflect.DeepEqual(fcfg.StreamConfig, cfg) {
		if err := mset.update(&cfg); err != nil {
			return nil, err
		}
	}

	// Now do consumers.
	odir := filepath.Join(ndir, consumerDir)
	ofis, _ := os.ReadDir(odir)
	for _, ofi := range ofis {
		metafile := filepath.Join(odir, ofi.Name(), JetStreamMetaFile)
		metasum := filepath.Join(odir, ofi.Name(), JetStreamMetaFileSum)
		if _, err := os.Stat(metafile); os.IsNotExist(err) {
			mset.stop(true, false)
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
		buf, err := os.ReadFile(metafile)
		if err != nil {
			mset.stop(true, false)
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
		if _, err := os.Stat(metasum); os.IsNotExist(err) {
			mset.stop(true, false)
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
		var cfg FileConsumerInfo
		if err := json.Unmarshal(buf, &cfg); err != nil {
			mset.stop(true, false)
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
		isEphemeral := !isDurableConsumer(&cfg.ConsumerConfig)
		if isEphemeral {
			// This is an ephermal consumer and this could fail on restart until
			// the consumer can reconnect. We will create it as a durable and switch it.
			cfg.ConsumerConfig.Durable = ofi.Name()
		}
		obs, err := mset.addConsumer(&cfg.ConsumerConfig)
		if err != nil {
			mset.stop(true, false)
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
		if isEphemeral {
			obs.switchToEphemeral()
		}
		if !cfg.Created.IsZero() {
			obs.setCreatedTime(cfg.Created)
		}
		obs.mu.Lock()
		err = obs.readStoredState(lseq)
		obs.mu.Unlock()
		if err != nil {
			mset.stop(true, false)
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
	}
	return mset, nil
}

// This is to check for dangling messages on interest retention streams. Only called on account enable.
// Issue https://github.com/nats-io/nats-server/issues/3612
func (mset *stream) checkForOrphanMsgs() {
	mset.mu.RLock()
	consumers := make([]*consumer, 0, len(mset.consumers))
	for _, o := range mset.consumers {
		consumers = append(consumers, o)
	}
	accName, stream := mset.acc.Name, mset.cfg.Name

	var ss StreamState
	mset.store.FastState(&ss)
	mset.mu.RUnlock()

	for _, o := range consumers {
		if err := o.checkStateForInterestStream(&ss); err == errAckFloorHigherThanLastSeq {
			o.mu.RLock()
			s, consumer := o.srv, o.name
			state, _ := o.store.State()
			asflr := state.AckFloor.Stream
			o.mu.RUnlock()
			// Warn about stream state vs our ack floor.
			s.RateLimitWarnf("Detected consumer '%s > %s > %s' ack floor %d is ahead of stream's last sequence %d",
				accName, stream, consumer, asflr, ss.LastSeq)
		}
	}
}

// Check on startup to make sure that consumers replication matches us.
// Interest retention requires replication matches.
func (mset *stream) checkConsumerReplication() {
	mset.mu.RLock()
	defer mset.mu.RUnlock()

	if mset.cfg.Retention != InterestPolicy {
		return
	}

	s, acc := mset.srv, mset.acc
	for _, o := range mset.consumers {
		o.mu.RLock()
		// Consumer replicas 0 can be a legit config for the replicas and we will inherit from the stream
		// when this is the case.
		if mset.cfg.Replicas != o.cfg.Replicas && o.cfg.Replicas != 0 {
			s.Errorf("consumer '%s > %s > %s' MUST match replication (%d vs %d) of stream with interest policy",
				acc, mset.cfg.Name, o.cfg.Name, mset.cfg.Replicas, o.cfg.Replicas)
		}
		o.mu.RUnlock()
	}
}

// Will check if we are running in the monitor already and if not set the appropriate flag.
func (mset *stream) checkInMonitor() bool {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	if mset.inMonitor {
		return true
	}
	mset.inMonitor = true
	return false
}

// Clear us being in the monitor routine.
func (mset *stream) clearMonitorRunning() {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	mset.inMonitor = false
}

// Check if our monitor is running.
func (mset *stream) isMonitorRunning() bool {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.inMonitor
}
