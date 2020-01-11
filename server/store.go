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
	"errors"
	"fmt"
	"strings"
	"time"
)

// StorageType determines how messages are stored for retention.
type StorageType int

const (
	// MemoryStorage specifies in memory only.
	MemoryStorage StorageType = iota
	// FileStorage specifies on disk, designated by the JetStream config StoreDir.
	FileStorage
)

var (
	// ErrStoreMsgNotFound when message was not found but was expected to be.
	ErrStoreMsgNotFound = errors.New("no message found")
	// ErrStoreEOF is returned when message seq is greater than the last sequence.
	ErrStoreEOF = errors.New("msgset EOF")
)

type MsgSetStore interface {
	StoreMsg(subj string, msg []byte) (uint64, error)
	LoadMsg(seq uint64) (subj string, msg []byte, ts int64, err error)
	RemoveMsg(seq uint64) bool
	EraseMsg(seq uint64) bool
	Purge() uint64
	GetSeqFromTime(t time.Time) uint64
	StorageBytesUpdate(func(int64))
	Stats() MsgSetStats
	Delete() error
	Stop() error
	ObservableStore(name string, cfg *ObservableConfig) (ObservableStore, error)
}

// MsgSetStats are stats about this given message set.
type MsgSetStats struct {
	Msgs        uint64 `json:"messages"`
	Bytes       uint64 `json:"bytes"`
	FirstSeq    uint64 `json:"first_seq"`
	LastSeq     uint64 `json:"last_seq"`
	Observables int    `json:"observable_count"`
}

type ObservableStore interface {
	State() (*ObservableState, error)
	Update(*ObservableState) error
	Stop() error
	Delete() error
}

// SequencePair has both the observable and the message set sequence. This point to same message.
type SequencePair struct {
	ObsSeq uint64 `json:"observable_sequence"`
	SetSeq uint64 `json:"msg_set_sequence"`
}

// ObservableState represents a stored state for an observable.
type ObservableState struct {
	// Delivered keep track of last delivered sequence numbers for both set and observable.
	Delivered SequencePair `json:"delivered"`
	// AckFloor keeps track of the ack floors for both set and observable.
	AckFloor SequencePair `json:"ack_floor"`
	// These are both in set sequence context.
	// Pending is for all messages pending and the timestamp for the delivered time.
	// This will only be present when the AckPolicy is ExplicitAck.
	Pending map[uint64]int64 `json:"pending"`
	// This is for messages that have been redelivered, so count > 1.
	Redelivery map[uint64]uint64 `json:"redelivery"`
}

func jsonString(s string) string {
	return "\"" + s + "\""
}

const (
	streamPolicyString    = "stream_limits"
	interestPolicyString  = "interest_based"
	workQueuePolicyString = "work_queue"
)

func (rp RetentionPolicy) String() string {
	switch rp {
	case StreamPolicy:
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
	case StreamPolicy:
		return json.Marshal(streamPolicyString)
	case InterestPolicy:
		return json.Marshal(interestPolicyString)
	case WorkQueuePolicy:
		return json.Marshal(workQueuePolicyString)
	default:
		return nil, fmt.Errorf("can not marshal %v", rp)
	}
}

func (rp *RetentionPolicy) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case jsonString(streamPolicyString):
		*rp = StreamPolicy
	case jsonString(interestPolicyString):
		*rp = InterestPolicy
	case jsonString(workQueuePolicyString):
		*rp = WorkQueuePolicy
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}
	return nil
}

const (
	memoryStorageString = "memory"
	fileStorageString   = "file"
)

func (st StorageType) String() string {
	switch st {
	case MemoryStorage:
		return strings.Title(memoryStorageString)
	case FileStorage:
		return strings.Title(fileStorageString)
	default:
		return "Unknown Storage Type"
	}
}

func (st StorageType) MarshalJSON() ([]byte, error) {
	switch st {
	case MemoryStorage:
		return json.Marshal(memoryStorageString)
	case FileStorage:
		return json.Marshal(fileStorageString)
	default:
		return nil, fmt.Errorf("can not marshal %v", st)
	}
}

func (st *StorageType) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case jsonString(memoryStorageString):
		*st = MemoryStorage
	case jsonString(fileStorageString):
		*st = FileStorage
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}
	return nil
}

const (
	ackNonePolicyString     = "none"
	ackAllPolicyString      = "all"
	ackExplicitPolicyString = "explicit"
)

func (ap AckPolicy) MarshalJSON() ([]byte, error) {
	switch ap {
	case AckNone:
		return json.Marshal(ackNonePolicyString)
	case AckAll:
		return json.Marshal(ackAllPolicyString)
	case AckExplicit:
		return json.Marshal(ackExplicitPolicyString)
	default:
		return nil, fmt.Errorf("can not marshal %v", ap)
	}
}

func (ap *AckPolicy) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case jsonString(ackNonePolicyString):
		*ap = AckNone
	case jsonString(ackAllPolicyString):
		*ap = AckAll
	case jsonString(ackExplicitPolicyString):
		*ap = AckExplicit
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}
	return nil
}

const (
	replayInstantPolicyString  = "instant"
	replayOriginalPolicyString = "original"
)

func (rp ReplayPolicy) MarshalJSON() ([]byte, error) {
	switch rp {
	case ReplayInstant:
		return json.Marshal(replayInstantPolicyString)
	case ReplayOriginal:
		return json.Marshal(replayOriginalPolicyString)
	default:
		return nil, fmt.Errorf("can not marshal %v", rp)
	}
}

func (rp *ReplayPolicy) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case jsonString(replayInstantPolicyString):
		*rp = ReplayInstant
	case jsonString(replayOriginalPolicyString):
		*rp = ReplayOriginal
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}
	return nil
}
