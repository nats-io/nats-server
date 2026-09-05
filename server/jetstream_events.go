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
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// jsAdvisoryForwardSubj is the subject template used when forwarding a copy of a
// JetStream advisory into the system account for cross-account operational
// visibility. The originating account name is encoded as a token so advisories
// from different accounts can be distinguished, e.g.
// "$SYS.ACCOUNT.<account>.JS.EVENT.ADVISORY.STREAM.CREATED.<stream>".
const jsAdvisoryForwardSubj = "$SYS.ACCOUNT.%s.%s"

// isJSAdvisoryOrMetricSubject reports whether subject is a JetStream advisory or
// metric subject (under $JS.EVENT.ADVISORY or $JS.EVENT.METRIC).
func isJSAdvisoryOrMetricSubject(subject string) bool {
	return strings.HasPrefix(subject, JSAdvisoryPrefix+".") || strings.HasPrefix(subject, JSMetricPrefix+".")
}

// jsAdvisoriesForwarded reports whether JetStream advisories for the named
// account should be mirrored into the system account. Advisories that already
// originate in the system account are never forwarded to itself.
func (s *Server) jsAdvisoriesForwarded(accName string) bool {
	if accName == _EMPTY_ || !s.getOpts().JetStreamForwardAdvisories {
		return false
	}
	sysAcc := s.SystemAccount()
	return sysAcc != nil && accName != sysAcc.Name
}

// forwardJSAdvisoryToSysAcct mirrors an already-encoded advisory payload into the
// system account, namespaced by the originating account name so a system
// operator can observe advisories across all accounts. Only advisory/metric
// subjects are forwarded; the subject guard keeps the invariant local so a future
// caller cannot inadvertently leak non-advisory traffic into the system account.
// Callers should gate on jsAdvisoriesForwarded first. The payload is copied
// because the system send queue delivers it asynchronously and the caller's
// buffer may be reused.
func (s *Server) forwardJSAdvisoryToSysAcct(accName, subject string, payload []byte) {
	if !isJSAdvisoryOrMetricSubject(subject) {
		return
	}
	sysAcc := s.SystemAccount()
	if sysAcc == nil {
		return
	}
	fsubj := fmt.Sprintf(jsAdvisoryForwardSubj, accName, strings.TrimPrefix(subject, "$"))
	cp := append([]byte(nil), payload...)
	if err := s.sendInternalAccountMsg(sysAcc, fsubj, cp); err != nil {
		s.Warnf("Advisory could not be forwarded to system account for account %q: %v", accName, err)
	}
}

// publishAdvisory marshals adv and delivers it to the originating account (when
// it has local interest) and/or mirrors it into the system account (when
// forwarding is enabled). It returns false on a marshal error, when there is
// neither local interest nor forwarding, or when the originating-account send
// fails; the success of a system-account forward is logged but not reflected in
// the return value.
func (s *Server) publishAdvisory(acc *Account, subject string, adv any) bool {
	if acc == nil {
		acc = s.SystemAccount()
		if acc == nil {
			return false
		}
	}

	// Determine whether to mirror this advisory into the system account. This is
	// independent of interest in the originating account, since a system operator
	// typically observes advisories without the originating account subscribing.
	forward := s.jsAdvisoriesForwarded(acc.Name)

	// If there is no one listening for this advisory and we are not forwarding it,
	// then save ourselves the effort and don't bother encoding the JSON or sending it.
	hasInterest := true
	if sl := acc.sl; (sl != nil && !sl.HasInterest(subject)) && !s.hasGatewayInterest(acc.Name, subject) {
		hasInterest = false
	}
	if !hasInterest && !forward {
		return false
	}

	ej, err := json.Marshal(adv)
	if err != nil {
		s.Warnf("Advisory could not be serialized for account %q: %v", acc.Name, err)
		return false
	}

	if hasInterest {
		if err = s.sendInternalAccountMsg(acc, subject, ej); err != nil {
			s.Warnf("Advisory could not be sent for account %q: %v", acc.Name, err)
		}
	}
	if forward {
		s.forwardJSAdvisoryToSysAcct(acc.Name, subject, ej)
	}
	return err == nil
}

// JSAPIAudit is an advisory about administrative actions taken on JetStream
type JSAPIAudit struct {
	TypedEvent
	Server   string      `json:"server"`
	Client   *ClientInfo `json:"client"`
	Subject  string      `json:"subject"`
	Request  string      `json:"request,omitempty"`
	Response string      `json:"response"`
	Domain   string      `json:"domain,omitempty"`
}

const JSAPIAuditType = "io.nats.jetstream.advisory.v1.api_audit"

// ActionAdvisoryType indicates which action against a stream, consumer or template triggered an advisory
type ActionAdvisoryType string

const (
	CreateEvent ActionAdvisoryType = "create"
	DeleteEvent ActionAdvisoryType = "delete"
	ModifyEvent ActionAdvisoryType = "modify"
)

// JSStreamActionAdvisory indicates that a stream was created, edited or deleted
type JSStreamActionAdvisory struct {
	TypedEvent
	Stream string             `json:"stream"`
	Action ActionAdvisoryType `json:"action"`
	Domain string             `json:"domain,omitempty"`
}

const JSStreamActionAdvisoryType = "io.nats.jetstream.advisory.v1.stream_action"

// JSConsumerActionAdvisory indicates that a consumer was created or deleted
type JSConsumerActionAdvisory struct {
	TypedEvent
	Stream   string             `json:"stream"`
	Consumer string             `json:"consumer"`
	Action   ActionAdvisoryType `json:"action"`
	Domain   string             `json:"domain,omitempty"`
}

const JSConsumerActionAdvisoryType = "io.nats.jetstream.advisory.v1.consumer_action"

// JSConsumerPauseAdvisory indicates that a consumer was paused or unpaused
type JSConsumerPauseAdvisory struct {
	TypedEvent
	Stream     string    `json:"stream"`
	Consumer   string    `json:"consumer"`
	Paused     bool      `json:"paused"`
	PauseUntil time.Time `json:"pause_until,omitempty"`
	Domain     string    `json:"domain,omitempty"`
}

const JSConsumerPauseAdvisoryType = "io.nats.jetstream.advisory.v1.consumer_pause"

// JSConsumerAckMetric is a metric published when a user acknowledges a message, the
// number of these that will be published is dependent on SampleFrequency
type JSConsumerAckMetric struct {
	TypedEvent
	Stream      string `json:"stream"`
	Consumer    string `json:"consumer"`
	ConsumerSeq uint64 `json:"consumer_seq"`
	StreamSeq   uint64 `json:"stream_seq"`
	Delay       int64  `json:"ack_time"`
	Deliveries  uint64 `json:"deliveries"`
	Domain      string `json:"domain,omitempty"`
}

// JSConsumerAckMetricType is the schema type for JSConsumerAckMetricType
const JSConsumerAckMetricType = "io.nats.jetstream.metric.v1.consumer_ack"

// JSConsumerDeliveryExceededAdvisory is an advisory informing that a message hit
// its MaxDeliver threshold and so might be a candidate for DLQ handling
type JSConsumerDeliveryExceededAdvisory struct {
	TypedEvent
	Stream     string `json:"stream"`
	Consumer   string `json:"consumer"`
	StreamSeq  uint64 `json:"stream_seq"`
	Deliveries uint64 `json:"deliveries"`
	Domain     string `json:"domain,omitempty"`
}

// JSConsumerDeliveryExceededAdvisoryType is the schema type for JSConsumerDeliveryExceededAdvisory
const JSConsumerDeliveryExceededAdvisoryType = "io.nats.jetstream.advisory.v1.max_deliver"

// JSConsumerDeliveryNakAdvisory is an advisory informing that a message was
// naked by the consumer
type JSConsumerDeliveryNakAdvisory struct {
	TypedEvent
	Stream      string `json:"stream"`
	Consumer    string `json:"consumer"`
	ConsumerSeq uint64 `json:"consumer_seq"`
	StreamSeq   uint64 `json:"stream_seq"`
	Deliveries  uint64 `json:"deliveries"`
	Domain      string `json:"domain,omitempty"`
}

// JSConsumerDeliveryNakAdvisoryType is the schema type for JSConsumerDeliveryNakAdvisory
const JSConsumerDeliveryNakAdvisoryType = "io.nats.jetstream.advisory.v1.nak"

// JSConsumerDeliveryTerminatedAdvisory is an advisory informing that a message was
// terminated by the consumer, so might be a candidate for DLQ handling
type JSConsumerDeliveryTerminatedAdvisory struct {
	TypedEvent
	Stream      string `json:"stream"`
	Consumer    string `json:"consumer"`
	ConsumerSeq uint64 `json:"consumer_seq"`
	StreamSeq   uint64 `json:"stream_seq"`
	Deliveries  uint64 `json:"deliveries"`
	Reason      string `json:"reason,omitempty"`
	Domain      string `json:"domain,omitempty"`
}

// JSConsumerDeliveryTerminatedAdvisoryType is the schema type for JSConsumerDeliveryTerminatedAdvisory
const JSConsumerDeliveryTerminatedAdvisoryType = "io.nats.jetstream.advisory.v1.terminated"

// JSSnapshotCreateAdvisory is an advisory sent after a snapshot is successfully started
type JSSnapshotCreateAdvisory struct {
	TypedEvent
	Stream string      `json:"stream"`
	State  StreamState `json:"state"`
	Client *ClientInfo `json:"client"`
	Domain string      `json:"domain,omitempty"`
}

// JSSnapshotCreatedAdvisoryType is the schema type for JSSnapshotCreateAdvisory
const JSSnapshotCreatedAdvisoryType = "io.nats.jetstream.advisory.v1.snapshot_create"

// JSSnapshotCompleteAdvisory is an advisory sent after a snapshot is successfully started
type JSSnapshotCompleteAdvisory struct {
	TypedEvent
	Stream string      `json:"stream"`
	Start  time.Time   `json:"start"`
	End    time.Time   `json:"end"`
	Client *ClientInfo `json:"client"`
	Domain string      `json:"domain,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// JSSnapshotCompleteAdvisoryType is the schema type for JSSnapshotCreateAdvisory
const JSSnapshotCompleteAdvisoryType = "io.nats.jetstream.advisory.v1.snapshot_complete"

// JSRestoreCreateAdvisory is an advisory sent after a snapshot is successfully started
type JSRestoreCreateAdvisory struct {
	TypedEvent
	Stream string      `json:"stream"`
	Client *ClientInfo `json:"client"`
	Domain string      `json:"domain,omitempty"`
}

// JSRestoreCreateAdvisoryType is the schema type for JSSnapshotCreateAdvisory
const JSRestoreCreateAdvisoryType = "io.nats.jetstream.advisory.v1.restore_create"

// JSRestoreCompleteAdvisory is an advisory sent after a snapshot is successfully started
type JSRestoreCompleteAdvisory struct {
	TypedEvent
	Stream string      `json:"stream"`
	Start  time.Time   `json:"start"`
	End    time.Time   `json:"end"`
	Bytes  int64       `json:"bytes"`
	Client *ClientInfo `json:"client"`
	Domain string      `json:"domain,omitempty"`
}

// JSRestoreCompleteAdvisoryType is the schema type for JSSnapshotCreateAdvisory
const JSRestoreCompleteAdvisoryType = "io.nats.jetstream.advisory.v1.restore_complete"

// Clustering specific.

// JSClusterLeaderElectedAdvisoryType is sent when the system elects a new meta leader.
const JSDomainLeaderElectedAdvisoryType = "io.nats.jetstream.advisory.v1.domain_leader_elected"

// JSClusterLeaderElectedAdvisory indicates that a domain has elected a new leader.
type JSDomainLeaderElectedAdvisory struct {
	TypedEvent
	Leader   string      `json:"leader"`
	Replicas []*PeerInfo `json:"replicas"`
	Cluster  string      `json:"cluster"`
	Domain   string      `json:"domain,omitempty"`
}

// JSStreamLeaderElectedAdvisoryType is sent when the system elects a new leader for a stream.
const JSStreamLeaderElectedAdvisoryType = "io.nats.jetstream.advisory.v1.stream_leader_elected"

// JSStreamLeaderElectedAdvisory indicates that a stream has elected a new leader.
type JSStreamLeaderElectedAdvisory struct {
	TypedEvent
	Account  string      `json:"account,omitempty"`
	Stream   string      `json:"stream"`
	Leader   string      `json:"leader"`
	Replicas []*PeerInfo `json:"replicas"`
	Domain   string      `json:"domain,omitempty"`
}

// JSStreamQuorumLostAdvisoryType is sent when the system detects a clustered stream and
// its consumers are stalled and unable to make progress.
const JSStreamQuorumLostAdvisoryType = "io.nats.jetstream.advisory.v1.stream_quorum_lost"

// JSStreamQuorumLostAdvisory indicates that a stream has lost quorum and is stalled.
type JSStreamQuorumLostAdvisory struct {
	TypedEvent
	Account  string      `json:"account,omitempty"`
	Stream   string      `json:"stream"`
	Replicas []*PeerInfo `json:"replicas"`
	Domain   string      `json:"domain,omitempty"`
}

// JSStreamBatchAbandonedAdvisoryType is sent when a stream's atomic batch is abandoned.
const JSStreamBatchAbandonedAdvisoryType = "io.nats.jetstream.advisory.v1.stream_batch_abandoned"

// JSStreamBatchAbandonedAdvisory indicates that a stream's batch was abandoned.
type JSStreamBatchAbandonedAdvisory struct {
	TypedEvent
	Account string             `json:"account,omitempty"`
	Stream  string             `json:"stream"`
	Domain  string             `json:"domain,omitempty"`
	BatchId string             `json:"batch"`
	Reason  BatchAbandonReason `json:"reason"`
}

type BatchAbandonReason string

var (
	BatchTimeout            BatchAbandonReason = "timeout"
	BatchLarge              BatchAbandonReason = "large"
	BatchIncomplete         BatchAbandonReason = "incomplete"
	BatchRequirementsNotMet BatchAbandonReason = "unsupported"
)

// JSConsumerLeaderElectedAdvisoryType is sent when the system elects a leader for a consumer.
const JSConsumerLeaderElectedAdvisoryType = "io.nats.jetstream.advisory.v1.consumer_leader_elected"

// JSConsumerLeaderElectedAdvisory indicates that a consumer has elected a new leader.
type JSConsumerLeaderElectedAdvisory struct {
	TypedEvent
	Account  string      `json:"account,omitempty"`
	Stream   string      `json:"stream"`
	Consumer string      `json:"consumer"`
	Leader   string      `json:"leader"`
	Replicas []*PeerInfo `json:"replicas"`
	Domain   string      `json:"domain,omitempty"`
}

// JSConsumerQuorumLostAdvisoryType is sent when the system detects a clustered consumer and
// is stalled and unable to make progress.
const JSConsumerQuorumLostAdvisoryType = "io.nats.jetstream.advisory.v1.consumer_quorum_lost"

// JSConsumerQuorumLostAdvisory indicates that a consumer has lost quorum and is stalled.
type JSConsumerQuorumLostAdvisory struct {
	TypedEvent
	Account  string      `json:"account,omitempty"`
	Stream   string      `json:"stream"`
	Consumer string      `json:"consumer"`
	Replicas []*PeerInfo `json:"replicas"`
	Domain   string      `json:"domain,omitempty"`
}

const JSConsumerGroupPinnedAdvisoryType = "io.nats.jetstream.advisory.v1.consumer_group_pinned"

// JSConsumerGroupPinnedAdvisory that a group switched to a new pinned client
type JSConsumerGroupPinnedAdvisory struct {
	TypedEvent
	Account        string `json:"account,omitempty"`
	Stream         string `json:"stream"`
	Consumer       string `json:"consumer"`
	Domain         string `json:"domain,omitempty"`
	Group          string `json:"group"`
	PinnedClientId string `json:"pinned_id"`
}

const JSConsumerGroupUnpinnedAdvisoryType = "io.nats.jetstream.advisory.v1.consumer_group_unpinned"

// JSConsumerGroupUnpinnedAdvisory indicates that a pin was lost
type JSConsumerGroupUnpinnedAdvisory struct {
	TypedEvent
	Account  string `json:"account,omitempty"`
	Stream   string `json:"stream"`
	Consumer string `json:"consumer"`
	Domain   string `json:"domain,omitempty"`
	Group    string `json:"group"`
	// one of "admin" or "timeout", could be an enum up to the implementor to decide
	Reason string `json:"reason"`
}

// JSServerOutOfStorageAdvisoryType is sent when the server is out of storage space.
const JSServerOutOfStorageAdvisoryType = "io.nats.jetstream.advisory.v1.server_out_of_space"

// JSServerOutOfSpaceAdvisory indicates that a stream has lost quorum and is stalled.
type JSServerOutOfSpaceAdvisory struct {
	TypedEvent
	Server   string `json:"server"`
	ServerID string `json:"server_id"`
	Stream   string `json:"stream,omitempty"`
	Cluster  string `json:"cluster"`
	Domain   string `json:"domain,omitempty"`
}

// JSServerRemovedAdvisoryType is sent when the server has been removed and JS disabled.
const JSServerRemovedAdvisoryType = "io.nats.jetstream.advisory.v1.server_removed"

// JSServerRemovedAdvisory indicates that a stream has lost quorum and is stalled.
type JSServerRemovedAdvisory struct {
	TypedEvent
	Server   string `json:"server"`
	ServerID string `json:"server_id"`
	Cluster  string `json:"cluster"`
	Domain   string `json:"domain,omitempty"`
}

// JSMetaRescueAdvisoryType is sent when a server unsafely lowers the meta
// group's quorum requirement for disaster recovery.
const JSMetaRescueAdvisoryType = "io.nats.jetstream.advisory.v1.meta_rescue"

// JSMetaRescueAdvisory indicates that a server has unsafely lowered the meta
// group's quorum requirement for disaster recovery.
type JSMetaRescueAdvisory struct {
	TypedEvent
	Server     string `json:"server"`
	ServerID   string `json:"server_id"`
	PrevQuorum int    `json:"prev_quorum"`
	NewQuorum  int    `json:"new_quorum"`
	Cluster    string `json:"cluster"`
	Domain     string `json:"domain,omitempty"`
}

// JSAPILimitReachedAdvisoryType is sent when the JS API request queue limit is reached.
const JSAPILimitReachedAdvisoryType = "io.nats.jetstream.advisory.v1.api_limit_reached"

// JSAPILimitReachedAdvisory is a advisory published when JetStream hits the queue length limit.
type JSAPILimitReachedAdvisory struct {
	TypedEvent
	Server  string `json:"server"`           // Server that created the event, name or ID
	Domain  string `json:"domain,omitempty"` // Domain the server belongs to
	Dropped int64  `json:"dropped"`          // How many messages did we drop from the queue
}
