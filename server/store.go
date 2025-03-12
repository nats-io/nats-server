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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unsafe"

	"github.com/nats-io/nats-server/v2/server/avl"
)

// StorageType determines how messages are stored for retention.
type StorageType int

const (
	// File specifies on disk, designated by the JetStream config StoreDir.
	FileStorage = StorageType(22)
	// MemoryStorage specifies in memory only.
	MemoryStorage = StorageType(33)
	// Any is for internals.
	AnyStorage = StorageType(44)
)

var (
	// ErrStoreClosed is returned when the store has been closed
	ErrStoreClosed = errors.New("store is closed")
	// ErrStoreMsgNotFound when message was not found but was expected to be.
	ErrStoreMsgNotFound = errors.New("no message found")
	// ErrStoreEOF is returned when message seq is greater than the last sequence.
	ErrStoreEOF = errors.New("stream store EOF")
	// ErrMaxMsgs is returned when we have discard new as a policy and we reached the message limit.
	ErrMaxMsgs = errors.New("maximum messages exceeded")
	// ErrMaxBytes is returned when we have discard new as a policy and we reached the bytes limit.
	ErrMaxBytes = errors.New("maximum bytes exceeded")
	// ErrMaxMsgsPerSubject is returned when we have discard new as a policy and we reached the message limit per subject.
	ErrMaxMsgsPerSubject = errors.New("maximum messages per subject exceeded")
	// ErrStoreSnapshotInProgress is returned when RemoveMsg or EraseMsg is called
	// while a snapshot is in progress.
	ErrStoreSnapshotInProgress = errors.New("snapshot in progress")
	// ErrMsgTooLarge is returned when a message is considered too large.
	ErrMsgTooLarge = errors.New("message to large")
	// ErrStoreWrongType is for when you access the wrong storage type.
	ErrStoreWrongType = errors.New("wrong storage type")
	// ErrNoAckPolicy is returned when trying to update a consumer's acks with no ack policy.
	ErrNoAckPolicy = errors.New("ack policy is none")
	// ErrInvalidSequence is returned when the sequence is not present in the stream store.
	ErrInvalidSequence = errors.New("invalid sequence")
	// ErrSequenceMismatch is returned when storing a raw message and the expected sequence is wrong.
	ErrSequenceMismatch = errors.New("expected sequence does not match store")
	// ErrCorruptStreamState
	ErrCorruptStreamState = errors.New("stream state snapshot is corrupt")
	// ErrTooManyResults
	ErrTooManyResults = errors.New("too many matching results for request")
	// ErrNoLockHeld
	ErrNoLockHeld = errors.New("expected lock was not held")
)

// StoreMsg is the stored message format for messages that are retained by the Store layer.
type StoreMsg struct {
	subj string
	hdr  []byte
	msg  []byte
	buf  []byte
	seq  uint64
	ts   int64
}

// Used to call back into the upper layers to report on changes in storage resources.
// For the cases where its a single message we will also supply sequence number and subject.
type StorageUpdateHandler func(msgs, bytes int64, seq uint64, subj string)

// Used to call back into the upper layers to report on newly created subject delete markers.
type SubjectDeleteMarkerUpdateHandler func(seq uint64, subj string)

type StreamStore interface {
	StoreMsg(subject string, hdr, msg []byte, ttl int64) (uint64, int64, error)
	StoreRawMsg(subject string, hdr, msg []byte, seq uint64, ts int64, ttl int64) error
	SkipMsg() uint64
	SkipMsgs(seq uint64, num uint64) error
	LoadMsg(seq uint64, sm *StoreMsg) (*StoreMsg, error)
	LoadNextMsg(filter string, wc bool, start uint64, smp *StoreMsg) (sm *StoreMsg, skip uint64, err error)
	LoadNextMsgMulti(sl *Sublist, start uint64, smp *StoreMsg) (sm *StoreMsg, skip uint64, err error)
	LoadLastMsg(subject string, sm *StoreMsg) (*StoreMsg, error)
	LoadPrevMsg(start uint64, smp *StoreMsg) (sm *StoreMsg, err error)
	RemoveMsg(seq uint64) (bool, error)
	EraseMsg(seq uint64) (bool, error)
	Purge() (uint64, error)
	PurgeEx(subject string, seq, keep uint64, noMarkers bool) (uint64, error)
	Compact(seq uint64) (uint64, error)
	Truncate(seq uint64) error
	GetSeqFromTime(t time.Time) uint64
	FilteredState(seq uint64, subject string) SimpleState
	SubjectsState(filterSubject string) map[string]SimpleState
	SubjectsTotals(filterSubject string) map[string]uint64
	MultiLastSeqs(filters []string, maxSeq uint64, maxAllowed int) ([]uint64, error)
	NumPending(sseq uint64, filter string, lastPerSubject bool) (total, validThrough uint64)
	NumPendingMulti(sseq uint64, sl *Sublist, lastPerSubject bool) (total, validThrough uint64)
	State() StreamState
	FastState(*StreamState)
	EncodedStreamState(failed uint64) (enc []byte, err error)
	SyncDeleted(dbs DeleteBlocks)
	Type() StorageType
	RegisterStorageUpdates(StorageUpdateHandler)
	RegisterSubjectDeleteMarkerUpdates(SubjectDeleteMarkerUpdateHandler)
	UpdateConfig(cfg *StreamConfig) error
	Delete() error
	Stop() error
	ConsumerStore(name string, cfg *ConsumerConfig) (ConsumerStore, error)
	AddConsumer(o ConsumerStore) error
	RemoveConsumer(o ConsumerStore) error
	Snapshot(deadline time.Duration, includeConsumers, checkMsgs bool) (*SnapshotResult, error)
	Utilization() (total, reported uint64, err error)
}

// RetentionPolicy determines how messages in a set are retained.
type RetentionPolicy int

const (
	// LimitsPolicy (default) means that messages are retained until any given limit is reached.
	// This could be one of MaxMsgs, MaxBytes, or MaxAge.
	LimitsPolicy RetentionPolicy = iota
	// InterestPolicy specifies that when all known consumers have acknowledged a message it can be removed.
	InterestPolicy
	// WorkQueuePolicy specifies that when the first worker or subscriber acknowledges the message it can be removed.
	WorkQueuePolicy
)

// Discard Policy determines how we proceed when limits of messages or bytes are hit. The default, DicscardOld will
// remove older messages. DiscardNew will fail to store the new message.
type DiscardPolicy int

const (
	// DiscardOld will remove older messages to return to the limits.
	DiscardOld = iota
	// DiscardNew will error on a StoreMsg call
	DiscardNew
)

// StreamState is information about the given stream.
type StreamState struct {
	Msgs        uint64            `json:"messages"`
	Bytes       uint64            `json:"bytes"`
	FirstSeq    uint64            `json:"first_seq"`
	FirstTime   time.Time         `json:"first_ts"`
	LastSeq     uint64            `json:"last_seq"`
	LastTime    time.Time         `json:"last_ts"`
	NumSubjects int               `json:"num_subjects,omitempty"`
	Subjects    map[string]uint64 `json:"subjects,omitempty"`
	NumDeleted  int               `json:"num_deleted,omitempty"`
	Deleted     []uint64          `json:"deleted,omitempty"`
	Lost        *LostStreamData   `json:"lost,omitempty"`
	Consumers   int               `json:"consumer_count"`
}

// SimpleState for filtered subject specific state.
type SimpleState struct {
	Msgs  uint64 `json:"messages"`
	First uint64 `json:"first_seq"`
	Last  uint64 `json:"last_seq"`

	// Internal usage for when the first needs to be updated before use.
	firstNeedsUpdate bool
	// Internal usage for when the last needs to be updated before use.
	lastNeedsUpdate bool
}

// LostStreamData indicates msgs that have been lost.
type LostStreamData struct {
	Msgs  []uint64 `json:"msgs"`
	Bytes uint64   `json:"bytes"`
}

// SnapshotResult contains information about the snapshot.
type SnapshotResult struct {
	Reader io.ReadCloser
	State  StreamState
	errCh  chan string
}

const (
	// Magic is used to identify stream state encodings.
	streamStateMagic = uint8(42)
	// Version
	streamStateVersion = uint8(1)
	// Magic / Identifier for run length encodings.
	runLengthMagic = uint8(33)
	// Magic / Identifier for AVL seqsets.
	seqSetMagic = uint8(22)
)

// Interface for DeleteBlock.
// These will be of three types:
// 1. AVL seqsets.
// 2. Run length encoding of a deleted range.
// 3. Legacy []uint64
type DeleteBlock interface {
	State() (first, last, num uint64)
	Range(f func(uint64) bool)
}

type DeleteBlocks []DeleteBlock

// StreamReplicatedState represents what is encoded in a binary stream snapshot used
// for stream replication in an NRG.
type StreamReplicatedState struct {
	Msgs     uint64
	Bytes    uint64
	FirstSeq uint64
	LastSeq  uint64
	Failed   uint64
	Deleted  DeleteBlocks
}

// Determine if this is an encoded stream state.
func IsEncodedStreamState(buf []byte) bool {
	return len(buf) >= hdrLen && buf[0] == streamStateMagic && buf[1] == streamStateVersion
}

var ErrBadStreamStateEncoding = errors.New("bad stream state encoding")

func DecodeStreamState(buf []byte) (*StreamReplicatedState, error) {
	ss := &StreamReplicatedState{}
	if len(buf) < hdrLen || buf[0] != streamStateMagic || buf[1] != streamStateVersion {
		return nil, ErrBadStreamStateEncoding
	}
	var bi = hdrLen

	readU64 := func() uint64 {
		if bi < 0 || bi >= len(buf) {
			bi = -1
			return 0
		}
		num, n := binary.Uvarint(buf[bi:])
		if n <= 0 {
			bi = -1
			return 0
		}
		bi += n
		return num
	}

	parserFailed := func() bool {
		return bi < 0
	}

	ss.Msgs = readU64()
	ss.Bytes = readU64()
	ss.FirstSeq = readU64()
	ss.LastSeq = readU64()
	ss.Failed = readU64()

	if parserFailed() {
		return nil, ErrCorruptStreamState
	}

	if numDeleted := readU64(); numDeleted > 0 {
		// If we have some deleted blocks.
		for l := len(buf); l > bi; {
			switch buf[bi] {
			case seqSetMagic:
				dmap, n, err := avl.Decode(buf[bi:])
				if err != nil {
					return nil, ErrCorruptStreamState
				}
				bi += n
				ss.Deleted = append(ss.Deleted, dmap)
			case runLengthMagic:
				bi++
				var rl DeleteRange
				rl.First = readU64()
				rl.Num = readU64()
				if parserFailed() {
					return nil, ErrCorruptStreamState
				}
				ss.Deleted = append(ss.Deleted, &rl)
			default:
				return nil, ErrCorruptStreamState
			}
		}
	}

	return ss, nil
}

// DeleteRange is a run length encoded delete range.
type DeleteRange struct {
	First uint64
	Num   uint64
}

func (dr *DeleteRange) State() (first, last, num uint64) {
	deletesAfterFirst := dr.Num
	if deletesAfterFirst > 0 {
		deletesAfterFirst--
	}
	return dr.First, dr.First + deletesAfterFirst, dr.Num
}

// Range will range over all the deleted sequences represented by this block.
func (dr *DeleteRange) Range(f func(uint64) bool) {
	for seq := dr.First; seq < dr.First+dr.Num; seq++ {
		if !f(seq) {
			return
		}
	}
}

// Legacy []uint64
type DeleteSlice []uint64

func (ds DeleteSlice) State() (first, last, num uint64) {
	if len(ds) == 0 {
		return 0, 0, 0
	}
	return ds[0], ds[len(ds)-1], uint64(len(ds))
}

// Range will range over all the deleted sequences represented by this []uint64.
func (ds DeleteSlice) Range(f func(uint64) bool) {
	for _, seq := range ds {
		if !f(seq) {
			return
		}
	}
}

func (dbs DeleteBlocks) NumDeleted() (total uint64) {
	for _, db := range dbs {
		_, _, num := db.State()
		total += num
	}
	return total
}

// ConsumerStore stores state on consumers for streams.
type ConsumerStore interface {
	SetStarting(sseq uint64) error
	UpdateStarting(sseq uint64)
	HasState() bool
	UpdateDelivered(dseq, sseq, dc uint64, ts int64) error
	UpdateAcks(dseq, sseq uint64) error
	UpdateConfig(cfg *ConsumerConfig) error
	Update(*ConsumerState) error
	State() (*ConsumerState, error)
	BorrowState() (*ConsumerState, error)
	EncodedState() ([]byte, error)
	Type() StorageType
	Stop() error
	Delete() error
	StreamDelete() error
}

// SequencePair has both the consumer and the stream sequence. They point to same message.
type SequencePair struct {
	Consumer uint64 `json:"consumer_seq"`
	Stream   uint64 `json:"stream_seq"`
}

// ConsumerState represents a stored state for a consumer.
type ConsumerState struct {
	// Delivered keeps track of last delivered sequence numbers for both the stream and the consumer.
	Delivered SequencePair `json:"delivered"`
	// AckFloor keeps track of the ack floors for both the stream and the consumer.
	AckFloor SequencePair `json:"ack_floor"`
	// These are both in stream sequence context.
	// Pending is for all messages pending and the timestamp for the delivered time.
	// This will only be present when the AckPolicy is ExplicitAck.
	Pending map[uint64]*Pending `json:"pending,omitempty"`
	// This is for messages that have been redelivered, so count > 1.
	Redelivered map[uint64]uint64 `json:"redelivered,omitempty"`
}

// Encode consumer state.
func encodeConsumerState(state *ConsumerState) []byte {
	var hdr [seqsHdrSize]byte
	var buf []byte

	maxSize := seqsHdrSize
	if lp := len(state.Pending); lp > 0 {
		maxSize += lp*(3*binary.MaxVarintLen64) + binary.MaxVarintLen64
	}
	if lr := len(state.Redelivered); lr > 0 {
		maxSize += lr*(2*binary.MaxVarintLen64) + binary.MaxVarintLen64
	}
	if maxSize == seqsHdrSize {
		buf = hdr[:seqsHdrSize]
	} else {
		buf = make([]byte, maxSize)
	}

	// Write header
	buf[0] = magic
	buf[1] = 2

	n := hdrLen
	n += binary.PutUvarint(buf[n:], state.AckFloor.Consumer)
	n += binary.PutUvarint(buf[n:], state.AckFloor.Stream)
	n += binary.PutUvarint(buf[n:], state.Delivered.Consumer)
	n += binary.PutUvarint(buf[n:], state.Delivered.Stream)
	n += binary.PutUvarint(buf[n:], uint64(len(state.Pending)))

	asflr := state.AckFloor.Stream
	adflr := state.AckFloor.Consumer

	// These are optional, but always write len. This is to avoid a truncate inline.
	if len(state.Pending) > 0 {
		// To save space we will use now rounded to seconds to be our base timestamp.
		mints := time.Now().Round(time.Second).Unix()
		// Write minimum timestamp we found from above.
		n += binary.PutVarint(buf[n:], mints)

		for k, v := range state.Pending {
			n += binary.PutUvarint(buf[n:], k-asflr)
			n += binary.PutUvarint(buf[n:], v.Sequence-adflr)
			// Downsample to seconds to save on space.
			// Subsecond resolution not needed for recovery etc.
			ts := v.Timestamp / int64(time.Second)
			n += binary.PutVarint(buf[n:], mints-ts)
		}
	}

	// We always write the redelivered len.
	n += binary.PutUvarint(buf[n:], uint64(len(state.Redelivered)))

	// We expect these to be small.
	if len(state.Redelivered) > 0 {
		for k, v := range state.Redelivered {
			n += binary.PutUvarint(buf[n:], k-asflr)
			n += binary.PutUvarint(buf[n:], v)
		}
	}

	return buf[:n]
}

// Represents a pending message for explicit ack or ack all.
// Sequence is the original consumer sequence.
type Pending struct {
	Sequence  uint64
	Timestamp int64
}

// TemplateStore stores templates.
type TemplateStore interface {
	Store(*streamTemplate) error
	Delete(*streamTemplate) error
}

const (
	limitsPolicyJSONString    = `"limits"`
	interestPolicyJSONString  = `"interest"`
	workQueuePolicyJSONString = `"workqueue"`
)

var (
	limitsPolicyJSONBytes    = []byte(limitsPolicyJSONString)
	interestPolicyJSONBytes  = []byte(interestPolicyJSONString)
	workQueuePolicyJSONBytes = []byte(workQueuePolicyJSONString)
)

func (rp RetentionPolicy) String() string {
	switch rp {
	case LimitsPolicy:
		return "Limits"
	case InterestPolicy:
		return "Interest"
	case WorkQueuePolicy:
		return "WorkQueue"
	default:
		return "Unknown Retention Policy"
	}
}

func (rp RetentionPolicy) MarshalJSON() ([]byte, error) {
	switch rp {
	case LimitsPolicy:
		return limitsPolicyJSONBytes, nil
	case InterestPolicy:
		return interestPolicyJSONBytes, nil
	case WorkQueuePolicy:
		return workQueuePolicyJSONBytes, nil
	default:
		return nil, fmt.Errorf("can not marshal %v", rp)
	}
}

func (rp *RetentionPolicy) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case limitsPolicyJSONString:
		*rp = LimitsPolicy
	case interestPolicyJSONString:
		*rp = InterestPolicy
	case workQueuePolicyJSONString:
		*rp = WorkQueuePolicy
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}
	return nil
}

func (dp DiscardPolicy) String() string {
	switch dp {
	case DiscardOld:
		return "DiscardOld"
	case DiscardNew:
		return "DiscardNew"
	default:
		return "Unknown Discard Policy"
	}
}

func (dp DiscardPolicy) MarshalJSON() ([]byte, error) {
	switch dp {
	case DiscardOld:
		return []byte(`"old"`), nil
	case DiscardNew:
		return []byte(`"new"`), nil
	default:
		return nil, fmt.Errorf("can not marshal %v", dp)
	}
}

func (dp *DiscardPolicy) UnmarshalJSON(data []byte) error {
	switch strings.ToLower(string(data)) {
	case `"old"`:
		*dp = DiscardOld
	case `"new"`:
		*dp = DiscardNew
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}
	return nil
}

const (
	memoryStorageJSONString = `"memory"`
	fileStorageJSONString   = `"file"`
	anyStorageJSONString    = `"any"`
)

var (
	memoryStorageJSONBytes = []byte(memoryStorageJSONString)
	fileStorageJSONBytes   = []byte(fileStorageJSONString)
	anyStorageJSONBytes    = []byte(anyStorageJSONString)
)

func (st StorageType) String() string {
	switch st {
	case MemoryStorage:
		return "Memory"
	case FileStorage:
		return "File"
	case AnyStorage:
		return "Any"
	default:
		return "Unknown Storage Type"
	}
}

func (st StorageType) MarshalJSON() ([]byte, error) {
	switch st {
	case MemoryStorage:
		return memoryStorageJSONBytes, nil
	case FileStorage:
		return fileStorageJSONBytes, nil
	case AnyStorage:
		return anyStorageJSONBytes, nil
	default:
		return nil, fmt.Errorf("can not marshal %v", st)
	}
}

func (st *StorageType) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case memoryStorageJSONString:
		*st = MemoryStorage
	case fileStorageJSONString:
		*st = FileStorage
	case anyStorageJSONString:
		*st = AnyStorage
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}
	return nil
}

const (
	ackNonePolicyJSONString     = `"none"`
	ackAllPolicyJSONString      = `"all"`
	ackExplicitPolicyJSONString = `"explicit"`
)

var (
	ackNonePolicyJSONBytes     = []byte(ackNonePolicyJSONString)
	ackAllPolicyJSONBytes      = []byte(ackAllPolicyJSONString)
	ackExplicitPolicyJSONBytes = []byte(ackExplicitPolicyJSONString)
)

func (ap AckPolicy) MarshalJSON() ([]byte, error) {
	switch ap {
	case AckNone:
		return ackNonePolicyJSONBytes, nil
	case AckAll:
		return ackAllPolicyJSONBytes, nil
	case AckExplicit:
		return ackExplicitPolicyJSONBytes, nil
	default:
		return nil, fmt.Errorf("can not marshal %v", ap)
	}
}

func (ap *AckPolicy) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case ackNonePolicyJSONString:
		*ap = AckNone
	case ackAllPolicyJSONString:
		*ap = AckAll
	case ackExplicitPolicyJSONString:
		*ap = AckExplicit
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}
	return nil
}

const (
	replayInstantPolicyJSONString  = `"instant"`
	replayOriginalPolicyJSONString = `"original"`
)

var (
	replayInstantPolicyJSONBytes  = []byte(replayInstantPolicyJSONString)
	replayOriginalPolicyJSONBytes = []byte(replayOriginalPolicyJSONString)
)

func (rp ReplayPolicy) MarshalJSON() ([]byte, error) {
	switch rp {
	case ReplayInstant:
		return replayInstantPolicyJSONBytes, nil
	case ReplayOriginal:
		return replayOriginalPolicyJSONBytes, nil
	default:
		return nil, fmt.Errorf("can not marshal %v", rp)
	}
}

func (rp *ReplayPolicy) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case replayInstantPolicyJSONString:
		*rp = ReplayInstant
	case replayOriginalPolicyJSONString:
		*rp = ReplayOriginal
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}
	return nil
}

const (
	deliverAllPolicyJSONString       = `"all"`
	deliverLastPolicyJSONString      = `"last"`
	deliverNewPolicyJSONString       = `"new"`
	deliverByStartSequenceJSONString = `"by_start_sequence"`
	deliverByStartTimeJSONString     = `"by_start_time"`
	deliverLastPerPolicyJSONString   = `"last_per_subject"`
	deliverUndefinedJSONString       = `"undefined"`
)

var (
	deliverAllPolicyJSONBytes       = []byte(deliverAllPolicyJSONString)
	deliverLastPolicyJSONBytes      = []byte(deliverLastPolicyJSONString)
	deliverNewPolicyJSONBytes       = []byte(deliverNewPolicyJSONString)
	deliverByStartSequenceJSONBytes = []byte(deliverByStartSequenceJSONString)
	deliverByStartTimeJSONBytes     = []byte(deliverByStartTimeJSONString)
	deliverLastPerPolicyJSONBytes   = []byte(deliverLastPerPolicyJSONString)
	deliverUndefinedJSONBytes       = []byte(deliverUndefinedJSONString)
)

func (p *DeliverPolicy) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case deliverAllPolicyJSONString, deliverUndefinedJSONString:
		*p = DeliverAll
	case deliverLastPolicyJSONString:
		*p = DeliverLast
	case deliverLastPerPolicyJSONString:
		*p = DeliverLastPerSubject
	case deliverNewPolicyJSONString:
		*p = DeliverNew
	case deliverByStartSequenceJSONString:
		*p = DeliverByStartSequence
	case deliverByStartTimeJSONString:
		*p = DeliverByStartTime
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}

	return nil
}

func (p DeliverPolicy) MarshalJSON() ([]byte, error) {
	switch p {
	case DeliverAll:
		return deliverAllPolicyJSONBytes, nil
	case DeliverLast:
		return deliverLastPolicyJSONBytes, nil
	case DeliverLastPerSubject:
		return deliverLastPerPolicyJSONBytes, nil
	case DeliverNew:
		return deliverNewPolicyJSONBytes, nil
	case DeliverByStartSequence:
		return deliverByStartSequenceJSONBytes, nil
	case DeliverByStartTime:
		return deliverByStartTimeJSONBytes, nil
	default:
		return deliverUndefinedJSONBytes, nil
	}
}

func isOutOfSpaceErr(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "no space left"))
}

// For when our upper layer catchup detects its missing messages from the beginning of the stream.
var errFirstSequenceMismatch = errors.New("first sequence mismatch")

func isClusterResetErr(err error) bool {
	return err == errLastSeqMismatch || err == ErrStoreEOF || err == errFirstSequenceMismatch || errors.Is(err, errCatchupAbortedNoLeader) || err == errCatchupTooManyRetries
}

// Copy all fields.
func (smo *StoreMsg) copy(sm *StoreMsg) {
	if sm.buf != nil {
		sm.buf = sm.buf[:0]
	}
	sm.buf = append(sm.buf, smo.buf...)
	// We set cap on header in case someone wants to expand it.
	sm.hdr, sm.msg = sm.buf[:len(smo.hdr):len(smo.hdr)], sm.buf[len(smo.hdr):]
	sm.subj, sm.seq, sm.ts = smo.subj, smo.seq, smo.ts
}

// Clear all fields except underlying buffer but reset that if present to [:0].
func (sm *StoreMsg) clear() {
	if sm == nil {
		return
	}
	*sm = StoreMsg{_EMPTY_, nil, nil, sm.buf, 0, 0}
	if len(sm.buf) > 0 {
		sm.buf = sm.buf[:0]
	}
}

// Note this will avoid a copy of the data used for the string, but it will also reference the existing slice's data pointer.
// So this should be used sparingly when we know the encompassing byte slice's lifetime is the same.
func bytesToString(b []byte) string {
	if len(b) == 0 {
		return _EMPTY_
	}
	p := unsafe.SliceData(b)
	return unsafe.String(p, len(b))
}

// Same in reverse. Used less often.
func stringToBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	p := unsafe.StringData(s)
	b := unsafe.Slice(p, len(s))
	return b
}

// Forces a copy of a string, for use in the case that you might have been passed a value when bytesToString was used,
// but now you need a separate copy of it to store for longer-term use.
func copyString(s string) string {
	b := make([]byte, len(s))
	copy(b, s)
	return bytesToString(b)
}

func isPermissionError(err error) bool {
	return err != nil && os.IsPermission(err)
}
