// Copyright 2020-2021 The NATS Authors
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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/nats-io/nuid"
)

// Request API subjects for JetStream.
const (
	// All API endpoints.
	jsAllApi = "$JS.API.>"

	// JSApiPrefix
	JSApiPrefix = "$JS.API"

	// JSApiInfo is for obtaining general information about JetStream for this account.
	// Will return JSON response.
	JSApiAccountInfo = "$JS.API.INFO"

	// JSApiTemplateCreate is the endpoint to create new stream templates.
	// Will return JSON response.
	JSApiTemplateCreate  = "$JS.API.STREAM.TEMPLATE.CREATE.*"
	JSApiTemplateCreateT = "$JS.API.STREAM.TEMPLATE.CREATE.%s"

	// JSApiTemplates is the endpoint to list all stream template names for this account.
	// Will return JSON response.
	JSApiTemplates = "$JS.API.STREAM.TEMPLATE.NAMES"

	// JSApiTemplateInfo is for obtaining general information about a named stream template.
	// Will return JSON response.
	JSApiTemplateInfo  = "$JS.API.STREAM.TEMPLATE.INFO.*"
	JSApiTemplateInfoT = "$JS.API.STREAM.TEMPLATE.INFO.%s"

	// JSApiTemplateDelete is the endpoint to delete stream templates.
	// Will return JSON response.
	JSApiTemplateDelete  = "$JS.API.STREAM.TEMPLATE.DELETE.*"
	JSApiTemplateDeleteT = "$JS.API.STREAM.TEMPLATE.DELETE.%s"

	// JSApiStreamCreate is the endpoint to create new streams.
	// Will return JSON response.
	JSApiStreamCreate  = "$JS.API.STREAM.CREATE.*"
	JSApiStreamCreateT = "$JS.API.STREAM.CREATE.%s"

	// JSApiStreamUpdate is the endpoint to update existing streams.
	// Will return JSON response.
	JSApiStreamUpdate  = "$JS.API.STREAM.UPDATE.*"
	JSApiStreamUpdateT = "$JS.API.STREAM.UPDATE.%s"

	// JSApiStreams is the endpoint to list all stream names for this account.
	// Will return JSON response.
	JSApiStreams = "$JS.API.STREAM.NAMES"
	// JSApiStreamList is the endpoint that will return all detailed stream information
	JSApiStreamList = "$JS.API.STREAM.LIST"

	// JSApiStreamInfo is for obtaining general information about a named stream.
	// Will return JSON response.
	JSApiStreamInfo  = "$JS.API.STREAM.INFO.*"
	JSApiStreamInfoT = "$JS.API.STREAM.INFO.%s"

	// JSApiStreamDelete is the endpoint to delete streams.
	// Will return JSON response.
	JSApiStreamDelete  = "$JS.API.STREAM.DELETE.*"
	JSApiStreamDeleteT = "$JS.API.STREAM.DELETE.%s"

	// JSApiPurgeStream is the endpoint to purge streams.
	// Will return JSON response.
	JSApiStreamPurge  = "$JS.API.STREAM.PURGE.*"
	JSApiStreamPurgeT = "$JS.API.STREAM.PURGE.%s"

	// JSApiStreamSnapshot is the endpoint to snapshot streams.
	// Will return a stream of chunks with a nil chunk as EOF to
	// the deliver subject. Caller should respond to each chunk
	// with a nil body response for ack flow.
	JSApiStreamSnapshot  = "$JS.API.STREAM.SNAPSHOT.*"
	JSApiStreamSnapshotT = "$JS.API.STREAM.SNAPSHOT.%s"

	// JSApiStreamRestore is the endpoint to restore a stream from a snapshot.
	// Caller should resond to each chunk with a nil body response.
	JSApiStreamRestore  = "$JS.API.STREAM.RESTORE.*"
	JSApiStreamRestoreT = "$JS.API.STREAM.RESTORE.%s"

	// JSApiDeleteMsg is the endpoint to delete messages from a stream.
	// Will return JSON response.
	JSApiMsgDelete  = "$JS.API.STREAM.MSG.DELETE.*"
	JSApiMsgDeleteT = "$JS.API.STREAM.MSG.DELETE.%s"

	// JSApiMsgGet is the template for direct requests for a message by its stream sequence number.
	// Will return JSON response.
	JSApiMsgGet  = "$JS.API.STREAM.MSG.GET.*"
	JSApiMsgGetT = "$JS.API.STREAM.MSG.GET.%s"

	// JSApiConsumerCreate is the endpoint to create ephemeral consumers for streams.
	// Will return JSON response.
	JSApiConsumerCreate  = "$JS.API.CONSUMER.CREATE.*"
	JSApiConsumerCreateT = "$JS.API.CONSUMER.CREATE.%s"

	// JSApiDurableCreate is the endpoint to create durable consumers for streams.
	// You need to include the stream and consumer name in the subject.
	JSApiDurableCreate  = "$JS.API.CONSUMER.DURABLE.CREATE.*.*"
	JSApiDurableCreateT = "$JS.API.CONSUMER.DURABLE.CREATE.%s.%s"

	// JSApiConsumers is the endpoint to list all consumer names for the stream.
	// Will return JSON response.
	JSApiConsumers  = "$JS.API.CONSUMER.NAMES.*"
	JSApiConsumersT = "$JS.API.CONSUMER.NAMES.%s"

	// JSApiConsumerList is the endpoint that will return all detailed consumer information
	JSApiConsumerList  = "$JS.API.CONSUMER.LIST.*"
	JSApiConsumerListT = "$JS.API.CONSUMER.LIST.%s"

	// JSApiConsumerInfo is for obtaining general information about a consumer.
	// Will return JSON response.
	JSApiConsumerInfo  = "$JS.API.CONSUMER.INFO.*.*"
	JSApiConsumerInfoT = "$JS.API.CONSUMER.INFO.%s.%s"

	// JSApiDeleteConsumer is the endpoint to delete consumers.
	// Will return JSON response.
	JSApiConsumerDelete  = "$JS.API.CONSUMER.DELETE.*.*"
	JSApiConsumerDeleteT = "$JS.API.CONSUMER.DELETE.%s.%s"

	// JSApiRequestNextT is the prefix for the request next message(s) for a consumer in worker/pull mode.
	JSApiRequestNextT = "$JS.API.CONSUMER.MSG.NEXT.%s.%s"

	// For snapshots and restores. The ack will have additional tokens.
	jsSnapshotAckT    = "$JS.SNAPSHOT.ACK.%s.%s"
	jsRestoreDeliverT = "$JS.SNAPSHOT.RESTORE.%s.%s"

	// JSApiStreamRemovePeer is the endpoint to remove a peer from a clustered stream and its consumers.
	// Will return JSON response.
	JSApiStreamRemovePeer  = "$JS.API.STREAM.PEER.REMOVE.*"
	JSApiStreamRemovePeerT = "$JS.API.STREAM.PEER.REMOVE.%s"

	// JSApiStreamLeaderStepDown is the endpoint to have stream leader stepdown.
	// Will return JSON response.
	JSApiStreamLeaderStepDown  = "$JS.API.STREAM.LEADER.STEPDOWN.*"
	JSApiStreamLeaderStepDownT = "$JS.API.STREAM.LEADER.STEPDOWN.%s"

	// JSApiConsumerLeaderStepDown is the endpoint to have consumer leader stepdown.
	// Will return JSON response.
	JSApiConsumerLeaderStepDown  = "$JS.API.CONSUMER.LEADER.STEPDOWN.*.*"
	JSApiConsumerLeaderStepDownT = "$JS.API.CONSUMER.LEADER.STEPDOWN.%s.%s"

	// JSApiLeaderStepDown is the endpoint to have our metaleader stepdown.
	// Only works from system account.
	// Will return JSON response.
	JSApiLeaderStepDown = "$JS.API.META.LEADER.STEPDOWN"

	// JSApiRemoveServer is the endpoint to remove a peer server from the cluster.
	// Only works from system account.
	// Will return JSON response.
	JSApiRemoveServer = "$JS.API.SERVER.REMOVE"

	// jsAckT is the template for the ack message stream coming back from a consumer
	// when they ACK/NAK, etc a message.
	jsAckT   = "$JS.ACK.%s.%s"
	jsAckPre = "$JS.ACK."

	// jsFlowControl is for flow control subjects.
	jsFlowControlPre = "$JS.FC."
	// jsFlowControl is for FC responses.
	jsFlowControl = "$JS.FC.%s.%s.*"

	// JSAdvisoryPrefix is a prefix for all JetStream advisories.
	JSAdvisoryPrefix = "$JS.EVENT.ADVISORY"

	// JSMetricPrefix is a prefix for all JetStream metrics.
	JSMetricPrefix = "$JS.EVENT.METRIC"

	// JSMetricConsumerAckPre is a metric containing ack latency.
	JSMetricConsumerAckPre = "$JS.EVENT.METRIC.CONSUMER.ACK"

	// JSAdvisoryConsumerMaxDeliveryExceedPre is a notification published when a message exceeds its delivery threshold.
	JSAdvisoryConsumerMaxDeliveryExceedPre = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES"

	// JSAdvisoryConsumerMsgTerminatedPre is a notification published when a message has been terminated.
	JSAdvisoryConsumerMsgTerminatedPre = "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED"

	// JSAdvisoryStreamCreatedPre notification that a stream was created.
	JSAdvisoryStreamCreatedPre = "$JS.EVENT.ADVISORY.STREAM.CREATED"

	// JSAdvisoryStreamDeletedPre notification that a stream was deleted.
	JSAdvisoryStreamDeletedPre = "$JS.EVENT.ADVISORY.STREAM.DELETED"

	// JSAdvisoryStreamUpdatedPre notification that a stream was updated.
	JSAdvisoryStreamUpdatedPre = "$JS.EVENT.ADVISORY.STREAM.UPDATED"

	// JSAdvisoryConsumerCreatedPre notification that a template created.
	JSAdvisoryConsumerCreatedPre = "$JS.EVENT.ADVISORY.CONSUMER.CREATED"

	// JSAdvisoryConsumerDeletedPre notification that a template deleted.
	JSAdvisoryConsumerDeletedPre = "$JS.EVENT.ADVISORY.CONSUMER.DELETED"

	// JSAdvisoryStreamSnapshotCreatePre notification that a snapshot was created.
	JSAdvisoryStreamSnapshotCreatePre = "$JS.EVENT.ADVISORY.STREAM.SNAPSHOT_CREATE"

	// JSAdvisoryStreamSnapshotCompletePre notification that a snapshot was completed.
	JSAdvisoryStreamSnapshotCompletePre = "$JS.EVENT.ADVISORY.STREAM.SNAPSHOT_COMPLETE"

	// JSAdvisoryStreamRestoreCreatePre notification that a restore was start.
	JSAdvisoryStreamRestoreCreatePre = "$JS.EVENT.ADVISORY.STREAM.RESTORE_CREATE"

	// JSAdvisoryStreamRestoreCompletePre notification that a restore was completed.
	JSAdvisoryStreamRestoreCompletePre = "$JS.EVENT.ADVISORY.STREAM.RESTORE_COMPLETE"

	// JSAdvisoryStreamLeaderElectPre notification that a replicated stream has elected a leader.
	JSAdvisoryStreamLeaderElectedPre = "$JS.EVENT.ADVISORY.STREAM.LEADER_ELECTED"

	// JSAdvisoryStreamQuorumLostPre notification that a stream and its consumers are stalled.
	JSAdvisoryStreamQuorumLostPre = "$JS.EVENT.ADVISORY.STREAM.QUORUM_LOST"

	// JSAdvisoryConsumerLeaderElectPre notification that a replicated consumer has elected a leader.
	JSAdvisoryConsumerLeaderElectedPre = "$JS.EVENT.ADVISORY.CONSUMER.LEADER_ELECTED"

	// JSAdvisoryConsumerQuorumLostPre notification that a consumer is stalled.
	JSAdvisoryConsumerQuorumLostPre = "$JS.EVENT.ADVISORY.CONSUMER.QUORUM_LOST"

	// JSAdvisoryServerOutOfStorage notification that a server has no more storage.
	JSAdvisoryServerOutOfStorage = "$JS.EVENT.ADVISORY.SERVER.OUT_OF_STORAGE"

	// JSAdvisoryServerRemoved notification that a server has been removed from the system.
	JSAdvisoryServerRemoved = "$JS.EVENT.ADVISORY.SERVER.REMOVED"

	// JSAuditAdvisory is a notification about JetStream API access.
	// FIXME - Add in details about who..
	JSAuditAdvisory = "$JS.EVENT.ADVISORY.API"
)

// Maximum name lengths for streams, consumers and templates.
const JSMaxNameLen = 256

// Responses for API calls.

// ApiError is included in all responses if there was an error.
// TODO(dlc) - Move to more generic location.
type ApiError struct {
	Code        int    `json:"code"`
	Description string `json:"description,omitempty"`
}

// ApiResponse is a standard response from the JetStream JSON API
type ApiResponse struct {
	Type  string    `json:"type"`
	Error *ApiError `json:"error,omitempty"`
}

const JSApiOverloadedType = "io.nats.jetstream.api.v1.system_overloaded"

// ApiPaged includes variables used to create paged responses from the JSON API
type ApiPaged struct {
	Total  int `json:"total"`
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
}

// ApiPagedRequest includes parameters allowing specific pages to be requests from APIs responding with ApiPaged
type ApiPagedRequest struct {
	Offset int `json:"offset"`
}

// JSApiAccountInfoResponse reports back information on jetstream for this account.
type JSApiAccountInfoResponse struct {
	ApiResponse
	*JetStreamAccountStats
}

const JSApiAccountInfoResponseType = "io.nats.jetstream.api.v1.account_info_response"

// JSApiStreamCreateResponse stream creation.
type JSApiStreamCreateResponse struct {
	ApiResponse
	*StreamInfo
}

const JSApiStreamCreateResponseType = "io.nats.jetstream.api.v1.stream_create_response"

// JSApiStreamDeleteResponse stream removal.
type JSApiStreamDeleteResponse struct {
	ApiResponse
	Success bool `json:"success,omitempty"`
}

const JSApiStreamDeleteResponseType = "io.nats.jetstream.api.v1.stream_delete_response"

// JSApiStreamInfoResponse.
type JSApiStreamInfoResponse struct {
	ApiResponse
	*StreamInfo
}

const JSApiStreamInfoResponseType = "io.nats.jetstream.api.v1.stream_info_response"

// Maximum entries we will return for streams or consumers lists.
// TODO(dlc) - with header or request support could request chunked response.
const JSApiNamesLimit = 1024
const JSApiListLimit = 256

type JSApiStreamNamesRequest struct {
	ApiPagedRequest
	// These are filters that can be applied to the list.
	Subject string `json:"subject,omitempty"`
}

// JSApiStreamNamesResponse list of streams.
// A nil request is valid and means all streams.
type JSApiStreamNamesResponse struct {
	ApiResponse
	ApiPaged
	Streams []string `json:"streams"`
}

const JSApiStreamNamesResponseType = "io.nats.jetstream.api.v1.stream_names_response"

// JSApiStreamListResponse list of detailed stream information.
// A nil request is valid and means all streams.
type JSApiStreamListResponse struct {
	ApiResponse
	ApiPaged
	Streams []*StreamInfo `json:"streams"`
}

const JSApiStreamListResponseType = "io.nats.jetstream.api.v1.stream_list_response"

// JSApiStreamPurgeResponse.
type JSApiStreamPurgeResponse struct {
	ApiResponse
	Success bool   `json:"success,omitempty"`
	Purged  uint64 `json:"purged"`
}

const JSApiStreamPurgeResponseType = "io.nats.jetstream.api.v1.stream_purge_response"

// JSApiStreamUpdateResponse for updating a stream.
type JSApiStreamUpdateResponse struct {
	ApiResponse
	*StreamInfo
}

const JSApiStreamUpdateResponseType = "io.nats.jetstream.api.v1.stream_update_response"

// JSApiMsgDeleteRequest delete message request.
type JSApiMsgDeleteRequest struct {
	Seq     uint64 `json:"seq"`
	NoErase bool   `json:"no_erase,omitempty"`
}

// JSApiMsgDeleteResponse.
type JSApiMsgDeleteResponse struct {
	ApiResponse
	Success bool `json:"success,omitempty"`
}

const JSApiMsgDeleteResponseType = "io.nats.jetstream.api.v1.stream_msg_delete_response"

type JSApiStreamSnapshotRequest struct {
	// Subject to deliver the chunks to for the snapshot.
	DeliverSubject string `json:"deliver_subject"`
	// Do not include consumers in the snapshot.
	NoConsumers bool `json:"no_consumers,omitempty"`
	// Optional chunk size preference.
	// Best to just let server select.
	ChunkSize int `json:"chunk_size,omitempty"`
	// Check all message's checksums prior to snapshot.
	CheckMsgs bool `json:"jsck,omitempty"`
}

// JSApiStreamSnapshotResponse is the direct response to the snapshot request.
type JSApiStreamSnapshotResponse struct {
	ApiResponse
	// Configuration of the given stream.
	Config *StreamConfig `json:"config,omitempty"`
	// Current State for the given stream.
	State *StreamState `json:"state,omitempty"`
}

const JSApiStreamSnapshotResponseType = "io.nats.jetstream.api.v1.stream_snapshot_response"

// JSApiStreamRestoreRequest is the required restore request.
type JSApiStreamRestoreRequest struct {
	// Configuration of the given stream.
	Config StreamConfig `json:"config"`
	// Current State for the given stream.
	State StreamState `json:"state"`
}

const JSApiStreamRestoreRequestType = "io.nats.jetstream.api.v1.stream_restore_request"

// JSApiStreamRestoreResponse is the direct response to the restore request.
type JSApiStreamRestoreResponse struct {
	ApiResponse
	// Subject to deliver the chunks to for the snapshot restore.
	DeliverSubject string `json:"deliver_subject"`
}

const JSApiStreamRestoreResponseType = "io.nats.jetstream.api.v1.stream_restore_response"

// JSApiStreamRemovePeerRequest is the required remove peer request.
type JSApiStreamRemovePeerRequest struct {
	// Server name of the peer to be removed.
	Peer string `json:"peer"`
}

// JSApiStreamRemovePeerResponse is the response to a remove peer request.
type JSApiStreamRemovePeerResponse struct {
	ApiResponse
	Success bool `json:"success,omitempty"`
}

const JSApiStreamRemovePeerResponseType = "io.nats.jetstream.api.v1.stream_remove_peer_response"

// JSApiStreamLeaderStepDownResponse is the response to a leader stepdown request.
type JSApiStreamLeaderStepDownResponse struct {
	ApiResponse
	Success bool `json:"success,omitempty"`
}

const JSApiStreamLeaderStepDownResponseType = "io.nats.jetstream.api.v1.stream_leader_stepdown_response"

// JSApiConsumerLeaderStepDownResponse is the response to a consumer leader stepdown request.
type JSApiConsumerLeaderStepDownResponse struct {
	ApiResponse
	Success bool `json:"success,omitempty"`
}

const JSApiConsumerLeaderStepDownResponseType = "io.nats.jetstream.api.v1.consumer_leader_stepdown_response"

// JSApiLeaderStepdownRequest allows placement control over the meta leader placement.
type JSApiLeaderStepdownRequest struct {
	Placement *Placement `json:"placement,omitempty"`
}

// JSApiLeaderStepDownResponse is the response to a meta leader stepdown request.
type JSApiLeaderStepDownResponse struct {
	ApiResponse
	Success bool `json:"success,omitempty"`
}

const JSApiLeaderStepDownResponseType = "io.nats.jetstream.api.v1.meta_leader_stepdown_response"

// JSApiMetaServerRemoveRequest will remove a peer from the meta group.
type JSApiMetaServerRemoveRequest struct {
	// Server name of the peer to be removed.
	Server string `json:"peer"`
}

// JSApiMetaServerRemoveResponse is the response to a peer removal request in the meta group.
type JSApiMetaServerRemoveResponse struct {
	ApiResponse
	Success bool `json:"success,omitempty"`
}

const JSApiMetaServerRemoveResponseType = "io.nats.jetstream.api.v1.meta_server_remove_response"

// JSApiMsgGetRequest get a message request.
type JSApiMsgGetRequest struct {
	Seq uint64 `json:"seq"`
}

// JSApiMsgGetResponse.
type JSApiMsgGetResponse struct {
	ApiResponse
	Message *StoredMsg `json:"message,omitempty"`
}

const JSApiMsgGetResponseType = "io.nats.jetstream.api.v1.stream_msg_get_response"

// JSWaitQueueDefaultMax is the default max number of outstanding requests for pull consumers.
const JSWaitQueueDefaultMax = 512

// JSApiConsumerCreateResponse.
type JSApiConsumerCreateResponse struct {
	ApiResponse
	*ConsumerInfo
}

const JSApiConsumerCreateResponseType = "io.nats.jetstream.api.v1.consumer_create_response"

// JSApiConsumerDeleteResponse.
type JSApiConsumerDeleteResponse struct {
	ApiResponse
	Success bool `json:"success,omitempty"`
}

const JSApiConsumerDeleteResponseType = "io.nats.jetstream.api.v1.consumer_delete_response"

// JSApiConsumerInfoResponse.
type JSApiConsumerInfoResponse struct {
	ApiResponse
	*ConsumerInfo
}

const JSApiConsumerInfoResponseType = "io.nats.jetstream.api.v1.consumer_info_response"

// JSApiConsumersRequest
type JSApiConsumersRequest struct {
	ApiPagedRequest
}

// JSApiConsumerNamesResponse.
type JSApiConsumerNamesResponse struct {
	ApiResponse
	ApiPaged
	Consumers []string `json:"consumers"`
}

const JSApiConsumerNamesResponseType = "io.nats.jetstream.api.v1.consumer_names_response"

// JSApiConsumerListResponse.
type JSApiConsumerListResponse struct {
	ApiResponse
	ApiPaged
	Consumers []*ConsumerInfo `json:"consumers"`
}

const JSApiConsumerListResponseType = "io.nats.jetstream.api.v1.consumer_list_response"

// JSApiConsumerGetNextRequest is for getting next messages for pull based consumers.
type JSApiConsumerGetNextRequest struct {
	Expires time.Duration `json:"expires,omitempty"`
	Batch   int           `json:"batch,omitempty"`
	NoWait  bool          `json:"no_wait,omitempty"`
}

// JSApiStreamTemplateCreateResponse for creating templates.
type JSApiStreamTemplateCreateResponse struct {
	ApiResponse
	*StreamTemplateInfo
}

const JSApiStreamTemplateCreateResponseType = "io.nats.jetstream.api.v1.stream_template_create_response"

// JSApiStreamTemplateDeleteResponse
type JSApiStreamTemplateDeleteResponse struct {
	ApiResponse
	Success bool `json:"success,omitempty"`
}

const JSApiStreamTemplateDeleteResponseType = "io.nats.jetstream.api.v1.stream_template_delete_response"

// JSApiStreamTemplateInfoResponse for information about stream templates.
type JSApiStreamTemplateInfoResponse struct {
	ApiResponse
	*StreamTemplateInfo
}

const JSApiStreamTemplateInfoResponseType = "io.nats.jetstream.api.v1.stream_template_info_response"

// JSApiStreamTemplatesRequest
type JSApiStreamTemplatesRequest struct {
	ApiPagedRequest
}

// JSApiStreamTemplateNamesResponse list of templates
type JSApiStreamTemplateNamesResponse struct {
	ApiResponse
	ApiPaged
	Templates []string `json:"streams"`
}

const JSApiStreamTemplateNamesResponseType = "io.nats.jetstream.api.v1.stream_template_names_response"

var (
	jsNotEnabledErr        = &ApiError{Code: 503, Description: "JetStream not enabled for account"}
	jsBadRequestErr        = &ApiError{Code: 400, Description: "bad request"}
	jsNotEmptyRequestErr   = &ApiError{Code: 400, Description: "expected an empty request payload"}
	jsInvalidJSONErr       = &ApiError{Code: 400, Description: "invalid JSON"}
	jsInsufficientErr      = &ApiError{Code: 503, Description: "insufficient resources"}
	jsNoConsumerErr        = &ApiError{Code: 404, Description: "consumer not found"}
	jsStreamMismatchErr    = &ApiError{Code: 400, Description: "stream name in subject does not match request"}
	jsNoClusterSupportErr  = &ApiError{Code: 503, Description: "not currently supported in clustered mode"}
	jsClusterNotAvailErr   = &ApiError{Code: 503, Description: "JetStream system temporarily unavailable"}
	jsClusterRequiredErr   = &ApiError{Code: 503, Description: "JetStream clustering support required"}
	jsPeerNotMemberErr     = &ApiError{Code: 400, Description: "peer not a member"}
	jsClusterIncompleteErr = &ApiError{Code: 503, Description: "incomplete results"}
	jsClusterTagsErr       = &ApiError{Code: 400, Description: "tags placement not supported for operation"}
	jsClusterNoPeersErr    = &ApiError{Code: 400, Description: "no suitable peers for placement"}
	jsServerNotMemberErr   = &ApiError{Code: 400, Description: "server is not a member of the cluster"}
	jsNoMessageFoundErr    = &ApiError{Code: 404, Description: "no message found"}
)

// For easier handling of exports and imports.
var allJsExports = []string{
	JSApiAccountInfo,
	JSApiTemplateCreate,
	JSApiTemplates,
	JSApiTemplateInfo,
	JSApiTemplateDelete,
	JSApiStreamCreate,
	JSApiStreamUpdate,
	JSApiStreams,
	JSApiStreamList,
	JSApiStreamInfo,
	JSApiStreamDelete,
	JSApiStreamPurge,
	JSApiStreamSnapshot,
	JSApiStreamRestore,
	JSApiStreamRemovePeer,
	JSApiStreamLeaderStepDown,
	JSApiConsumerLeaderStepDown,
	JSApiMsgDelete,
	JSApiMsgGet,
	JSApiConsumerCreate,
	JSApiDurableCreate,
	JSApiConsumers,
	JSApiConsumerList,
	JSApiConsumerInfo,
	JSApiConsumerDelete,
}

func (js *jetStream) apiDispatch(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	js.mu.RLock()
	s := js.srv
	rr := js.apiSubs.Match(subject)
	js.mu.RUnlock()

	// Shortcircuit.
	if len(rr.psubs)+len(rr.qsubs) == 0 {
		return
	}

	// We should only have psubs and only 1 per result.
	// FIXME(dlc) - Should we respond here with NoResponders or error?
	if len(rr.psubs) != 1 {
		s.Warnf("Malformed JetStream API Request: [%s] %q", subject, rmsg)
		return
	}
	jsub := rr.psubs[0]

	// If this is directly from a client connection ok to do in place.
	if c.kind == CLIENT {
		jsub.icb(sub, c, subject, reply, rmsg)
		return
	}

	// If we are here we have received this request over a non client connection.
	// We need to make sure not to block. We will spin a Go routine per but also make
	// sure we do not have too many outstanding.
	if apiOut := atomic.AddInt64(&js.apiCalls, 1); apiOut > 1024 {
		atomic.AddInt64(&js.apiCalls, -1)
		ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
		if err == nil {
			resp := &ApiResponse{Type: JSApiOverloadedType, Error: jsInsufficientErr}
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		} else {
			s.Warnf(badAPIRequestT, rmsg)
		}
		s.Warnf("JetStream API limit exceeded: %d calls outstanding", apiOut)
		return
	}

	// If we are here we can properly dispatch this API call.
	// Copy the message and the client. Client for the pubArgs
	// but note the JSAPI only uses the hdr index to piece apart
	// the header from the msg body. No other references are needed.
	// FIXME(dlc) - Should cleanup eventually and make sending
	// and receiving internal messages more formal.
	rmsg = append(rmsg[:0:0], rmsg...)
	client := &client{srv: s, kind: JETSTREAM}
	client.pa = c.pa

	// Dispatch the API call to its own Go routine.
	go func() {
		jsub.icb(sub, client, subject, reply, rmsg)
		atomic.AddInt64(&js.apiCalls, -1)
	}()
}

func (s *Server) setJetStreamExportSubs() error {
	js := s.getJetStream()
	if js == nil {
		return ErrJetStreamNotEnabled
	}

	// This is the catch all now for all JetStream API calls.
	if _, err := s.sysSubscribe(jsAllApi, js.apiDispatch); err != nil {
		return err
	}

	if err := s.SystemAccount().AddServiceExport(jsAllApi, nil); err != nil {
		s.Warnf("Error setting up jetstream service exports: %v", err)
		return err
	}

	// API handles themselves.
	pairs := []struct {
		subject string
		handler msgHandler
	}{
		{JSApiAccountInfo, s.jsAccountInfoRequest},
		{JSApiTemplateCreate, s.jsTemplateCreateRequest},
		{JSApiTemplates, s.jsTemplateNamesRequest},
		{JSApiTemplateInfo, s.jsTemplateInfoRequest},
		{JSApiTemplateDelete, s.jsTemplateDeleteRequest},
		{JSApiStreamCreate, s.jsStreamCreateRequest},
		{JSApiStreamUpdate, s.jsStreamUpdateRequest},
		{JSApiStreams, s.jsStreamNamesRequest},
		{JSApiStreamList, s.jsStreamListRequest},
		{JSApiStreamInfo, s.jsStreamInfoRequest},
		{JSApiStreamDelete, s.jsStreamDeleteRequest},
		{JSApiStreamPurge, s.jsStreamPurgeRequest},
		{JSApiStreamSnapshot, s.jsStreamSnapshotRequest},
		{JSApiStreamRestore, s.jsStreamRestoreRequest},
		{JSApiStreamRemovePeer, s.jsStreamRemovePeerRequest},
		{JSApiStreamLeaderStepDown, s.jsStreamLeaderStepDownRequest},
		{JSApiConsumerLeaderStepDown, s.jsConsumerLeaderStepDownRequest},
		{JSApiMsgDelete, s.jsMsgDeleteRequest},
		{JSApiMsgGet, s.jsMsgGetRequest},
		{JSApiConsumerCreate, s.jsConsumerCreateRequest},
		{JSApiDurableCreate, s.jsDurableCreateRequest},
		{JSApiConsumers, s.jsConsumerNamesRequest},
		{JSApiConsumerList, s.jsConsumerListRequest},
		{JSApiConsumerInfo, s.jsConsumerInfoRequest},
		{JSApiConsumerDelete, s.jsConsumerDeleteRequest},
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	for _, p := range pairs {
		sub := &subscription{subject: []byte(p.subject), icb: p.handler}
		if err := js.apiSubs.Insert(sub); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) sendAPIResponse(ci *ClientInfo, acc *Account, subject, reply, request, response string) {
	acc.trackAPI()
	if reply != _EMPTY_ {
		s.sendInternalAccountMsg(nil, reply, response)
	}
	s.sendJetStreamAPIAuditAdvisory(ci, acc, subject, request, response)
}

func (s *Server) sendAPIErrResponse(ci *ClientInfo, acc *Account, subject, reply, request, response string) {
	acc.trackAPIErr()
	if reply != _EMPTY_ {
		s.sendInternalAccountMsg(nil, reply, response)
	}
	s.sendJetStreamAPIAuditAdvisory(ci, acc, subject, request, response)
}

func (s *Server) getRequestInfo(c *client, raw []byte) (pci *ClientInfo, acc *Account, hdr, msg []byte, err error) {
	hdr, msg = c.msgParts(raw)
	var ci ClientInfo

	if len(hdr) > 0 {
		if err := json.Unmarshal(getHeader(ClientInfoHdr, hdr), &ci); err != nil {
			return nil, nil, nil, nil, err
		}
	}

	if ci.Service != _EMPTY_ {
		acc, _ = s.LookupAccount(ci.Service)
	} else if ci.Account != _EMPTY_ {
		acc, _ = s.LookupAccount(ci.Account)
	} else {
		// Direct $SYS access.
		acc = c.acc
		if acc == nil {
			acc = s.SystemAccount()
		}
	}
	if acc == nil {
		return nil, nil, nil, nil, ErrMissingAccount
	}
	return &ci, acc, hdr, msg, nil
}

func (a *Account) trackAPI() {
	a.mu.RLock()
	jsa := a.js
	a.mu.RUnlock()
	if jsa != nil {
		jsa.mu.Lock()
		jsa.usage.api++
		jsa.apiTotal++
		jsa.sendClusterUsageUpdate()
		jsa.mu.Unlock()
	}
}

func (a *Account) trackAPIErr() {
	a.mu.RLock()
	jsa := a.js
	a.mu.RUnlock()
	if jsa != nil {
		jsa.mu.Lock()
		jsa.usage.api++
		jsa.apiTotal++
		jsa.usage.err++
		jsa.apiErrors++
		jsa.sendClusterUsageUpdate()
		jsa.mu.Unlock()
	}
}

const badAPIRequestT = "Malformed JetStream API Request: %q"

// Request for current usage and limits for this account.
func (s *Server) jsAccountInfoRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiAccountInfoResponse{ApiResponse: ApiResponse{Type: JSApiAccountInfoResponseType}}

	// Determine if we should proceed here when we are in clustered mode.
	if s.JetStreamIsClustered() {
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			return
		}
		if js.isLeaderless() {
			resp.Error = jsClusterNotAvailErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		// Make sure we are meta leader.
		if !s.JetStreamIsLeader() {
			return
		}
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
	} else {
		stats := acc.JetStreamUsage()
		resp.JetStreamAccountStats = &stats
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return
	}
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), string(b))
}

// Helpers for token extraction.
func templateNameFromSubject(subject string) string {
	return tokenAt(subject, 6)
}

func streamNameFromSubject(subject string) string {
	return tokenAt(subject, 5)
}

func consumerNameFromSubject(subject string) string {
	return tokenAt(subject, 6)
}

// Request to create a new template.
func (s *Server) jsTemplateCreateRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiStreamTemplateCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamTemplateCreateResponseType}}
	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	// Not supported for now.
	if s.JetStreamIsClustered() {
		resp.Error = jsNoClusterSupportErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	var cfg StreamTemplateConfig
	if err := json.Unmarshal(msg, &cfg); err != nil {
		resp.Error = jsInvalidJSONErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	templateName := templateNameFromSubject(subject)
	if templateName != cfg.Name {
		resp.Error = &ApiError{Code: 400, Description: "template name in subject does not match request"}
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	t, err := acc.addStreamTemplate(&cfg)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	t.mu.Lock()
	tcfg := t.StreamTemplateConfig.deepCopy()
	streams := t.streams
	if streams == nil {
		streams = []string{}
	}
	t.mu.Unlock()
	resp.StreamTemplateInfo = &StreamTemplateInfo{Config: tcfg, Streams: streams}
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for the list of all template names.
func (s *Server) jsTemplateNamesRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiStreamTemplateNamesResponse{ApiResponse: ApiResponse{Type: JSApiStreamTemplateNamesResponseType}}
	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	// Not supported for now.
	if s.JetStreamIsClustered() {
		resp.Error = jsNoClusterSupportErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	var offset int
	if !isEmptyRequest(msg) {
		var req JSApiStreamTemplatesRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			resp.Error = jsInvalidJSONErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		offset = req.Offset
	}

	ts := acc.templates()
	sort.Slice(ts, func(i, j int) bool {
		return strings.Compare(ts[i].StreamTemplateConfig.Name, ts[j].StreamTemplateConfig.Name) < 0
	})

	tcnt := len(ts)
	if offset > tcnt {
		offset = tcnt
	}

	for _, t := range ts[offset:] {
		t.mu.Lock()
		name := t.Name
		t.mu.Unlock()
		resp.Templates = append(resp.Templates, name)
		if len(resp.Templates) >= JSApiNamesLimit {
			break
		}
	}
	resp.Total = tcnt
	resp.Limit = JSApiNamesLimit
	resp.Offset = offset
	if resp.Templates == nil {
		resp.Templates = []string{}
	}
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for information about a stream template.
func (s *Server) jsTemplateInfoRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiStreamTemplateInfoResponse{ApiResponse: ApiResponse{Type: JSApiStreamTemplateInfoResponseType}}
	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsNotEmptyRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	name := templateNameFromSubject(subject)
	t, err := acc.lookupStreamTemplate(name)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	t.mu.Lock()
	cfg := t.StreamTemplateConfig.deepCopy()
	streams := t.streams
	if streams == nil {
		streams = []string{}
	}
	t.mu.Unlock()

	resp.StreamTemplateInfo = &StreamTemplateInfo{Config: cfg, Streams: streams}
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to delete a stream template.
func (s *Server) jsTemplateDeleteRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiStreamTemplateDeleteResponse{ApiResponse: ApiResponse{Type: JSApiStreamTemplateDeleteResponseType}}
	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsNotEmptyRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	name := templateNameFromSubject(subject)
	err = acc.deleteStreamTemplate(name)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.Success = true
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

func (s *Server) jsonResponse(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		s.Warnf("Problem marshaling JSON for JetStream API:", err)
		return ""
	}
	return string(b)
}

func jsError(err error) *ApiError {
	return &ApiError{
		Code:        500,
		Description: err.Error(),
	}
}

func jsNotFoundError(err error) *ApiError {
	return &ApiError{
		Code:        404,
		Description: err.Error(),
	}
}

// Request to create a stream.
func (s *Server) jsStreamCreateRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiStreamCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamCreateResponseType}}

	// Determine if we should proceed here when we are in clustered mode.
	if s.JetStreamIsClustered() {
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			return
		}
		if js.isLeaderless() {
			resp.Error = jsClusterNotAvailErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		// Make sure we are meta leader.
		if !s.JetStreamIsLeader() {
			return
		}
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	var cfg StreamConfig
	if err := json.Unmarshal(msg, &cfg); err != nil {
		resp.Error = jsInvalidJSONErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	streamName := streamNameFromSubject(subject)
	if streamName != cfg.Name {
		resp.Error = jsStreamMismatchErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	hasStream := func(streamName string) (bool, int32, []string) {
		var exists bool
		var maxMsgSize int32
		var subs []string
		if s.JetStreamIsClustered() {
			if js, _ := s.getJetStreamCluster(); js != nil {
				js.mu.RLock()
				if sa := js.streamAssignment(acc.Name, streamName); sa != nil {
					maxMsgSize = sa.Config.MaxMsgSize
					subs = sa.Config.Subjects
					exists = true
				}
				js.mu.RUnlock()
			}
		} else if mset, err := acc.lookupStream(streamName); err == nil {
			maxMsgSize = mset.cfg.MaxMsgSize
			subs = mset.cfg.Subjects
			exists = true
		}
		return exists, maxMsgSize, subs
	}

	var streamSubs []string
	var deliveryPrefixes []string
	var apiPrefixes []string

	// Do some pre-checking for mirror config to avoid cycles in clustered mode.
	if cfg.Mirror != nil {
		if len(cfg.Subjects) > 0 {
			resp.Error = &ApiError{Code: 400, Description: "stream mirrors can not also contain subjects"}
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		if len(cfg.Sources) > 0 {
			resp.Error = &ApiError{Code: 400, Description: "stream mirrors can not also contain other sources"}
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		if cfg.Mirror.FilterSubject != _EMPTY_ {
			resp.Error = &ApiError{Code: 400, Description: "stream mirrors can not contain filtered subjects"}
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		if cfg.Mirror.OptStartSeq > 0 && cfg.Mirror.OptStartTime != nil {
			resp.Error = &ApiError{Code: 400, Description: "stream mirrors can not have both start seq and start time configured"}
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}

		// We do not require other stream to exist anymore, but if we can see it check payloads.
		exists, maxMsgSize, subs := hasStream(cfg.Mirror.Name)
		if len(subs) > 0 {
			streamSubs = append(streamSubs, subs...)
		}
		if exists && cfg.MaxMsgSize > 0 && maxMsgSize > 0 && cfg.MaxMsgSize < maxMsgSize {
			resp.Error = &ApiError{Code: 400, Description: "stream mirror must have max message size >= source"}
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		if cfg.Mirror.External != nil {
			if cfg.Mirror.External.DeliverPrefix != _EMPTY_ {
				deliveryPrefixes = append(deliveryPrefixes, cfg.Mirror.External.DeliverPrefix)
			}
			if cfg.Mirror.External.ApiPrefix != _EMPTY_ {
				apiPrefixes = append(apiPrefixes, cfg.Mirror.External.ApiPrefix)
			}
		}
	}
	if len(cfg.Sources) > 0 {
		for _, src := range cfg.Sources {
			if src.External == nil {
				continue
			}
			exists, maxMsgSize, subs := hasStream(src.Name)
			if len(subs) > 0 {
				streamSubs = append(streamSubs, subs...)
			}
			if src.External.DeliverPrefix != _EMPTY_ {
				deliveryPrefixes = append(deliveryPrefixes, src.External.DeliverPrefix)
			}
			if src.External.ApiPrefix != _EMPTY_ {
				apiPrefixes = append(apiPrefixes, src.External.ApiPrefix)
			}
			if exists && cfg.MaxMsgSize > 0 && maxMsgSize > 0 && cfg.MaxMsgSize < maxMsgSize {
				resp.Error = &ApiError{Code: 400, Description: "stream source must have max message size >= target"}
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
				return
			}
		}
	}
	// check prefix overlap with subjects
	for _, pfx := range deliveryPrefixes {
		if !IsValidPublishSubject(pfx) {
			resp.Error = &ApiError{Code: 400, Description: fmt.Sprintf("stream external delivery prefix %q must not contain wildcards", pfx)}
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		for _, sub := range streamSubs {
			if SubjectsCollide(sub, fmt.Sprintf("%s.%s", pfx, sub)) {
				resp.Error = &ApiError{Code: 400, Description: fmt.Sprintf("stream external delivery prefix %q overlaps with stream subject %q", pfx, sub)}
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
				return
			}
		}
	}
	// check if api prefixes overlap
	for _, apiPfx := range apiPrefixes {
		if SubjectsCollide(apiPfx, JSApiPrefix) {
			resp.Error = &ApiError{Code: 400, Description: fmt.Sprintf("stream external api prefix %q must not overlap with %s", apiPfx, JSApiPrefix)}
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
	}

	if s.JetStreamIsClustered() {
		s.jsClusteredStreamRequest(ci, acc, subject, reply, rmsg, &cfg)
		return
	}

	mset, err := acc.addStream(&cfg)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.StreamInfo = &StreamInfo{Created: mset.createdTime(), State: mset.state(), Config: mset.config()}
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to update a stream.
func (s *Server) jsStreamUpdateRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}

	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiStreamUpdateResponse{ApiResponse: ApiResponse{Type: JSApiStreamUpdateResponseType}}

	// Determine if we should proceed here when we are in clustered mode.
	if s.JetStreamIsClustered() {
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			return
		}
		if js.isLeaderless() {
			resp.Error = jsClusterNotAvailErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		// Make sure we are meta leader.
		if !s.JetStreamIsLeader() {
			return
		}
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	var ncfg StreamConfig
	if err := json.Unmarshal(msg, &ncfg); err != nil {
		resp.Error = jsInvalidJSONErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	cfg, err := checkStreamCfg(&ncfg)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	streamName := streamNameFromSubject(subject)
	if streamName != cfg.Name {
		resp.Error = jsStreamMismatchErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	if s.JetStreamIsClustered() {
		s.jsClusteredStreamUpdateRequest(ci, acc, subject, reply, rmsg, &cfg)
		return
	}

	mset, err := acc.lookupStream(streamName)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	if err := mset.update(&cfg); err != nil {
		resp.Error = jsError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	js, _ := s.getJetStreamCluster()

	resp.StreamInfo = &StreamInfo{Created: mset.createdTime(), State: mset.state(), Config: mset.config(), Cluster: js.clusterInfo(mset.raftGroup())}
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for the list of all stream names.
func (s *Server) jsStreamNamesRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiStreamNamesResponse{ApiResponse: ApiResponse{Type: JSApiStreamNamesResponseType}}

	// Determine if we should proceed here when we are in clustered mode.
	if s.JetStreamIsClustered() {
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			return
		}
		if js.isLeaderless() {
			resp.Error = jsClusterNotAvailErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		// Make sure we are meta leader.
		if !s.JetStreamIsLeader() {
			return
		}
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	var offset int
	var filter string

	if !isEmptyRequest(msg) {
		var req JSApiStreamNamesRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			resp.Error = jsInvalidJSONErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		offset = req.Offset
		if req.Subject != _EMPTY_ {
			filter = req.Subject
		}
	}

	// TODO(dlc) - Maybe hold these results for large results that we expect to be paged.
	// TODO(dlc) - If this list is long maybe do this in a Go routine?
	var numStreams int
	if s.JetStreamIsClustered() {
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			// TODO(dlc) - Debug or Warn?
			return
		}
		js.mu.RLock()
		for stream, sa := range cc.streams[acc.Name] {
			if sa.err == ErrJetStreamNotAssigned {
				continue
			}
			if filter != _EMPTY_ {
				// These could not have subjects auto-filled in since they are raw and unprocessed.
				if len(sa.Config.Subjects) == 0 {
					if SubjectsCollide(filter, sa.Config.Name) {
						resp.Streams = append(resp.Streams, stream)
					}
				} else {
					for _, subj := range sa.Config.Subjects {
						if SubjectsCollide(filter, subj) {
							resp.Streams = append(resp.Streams, stream)
							break
						}
					}
				}
			} else {
				resp.Streams = append(resp.Streams, stream)
			}
		}
		js.mu.RUnlock()
		if len(resp.Streams) > 1 {
			sort.Slice(resp.Streams, func(i, j int) bool { return strings.Compare(resp.Streams[i], resp.Streams[j]) < 0 })
		}
		numStreams = len(resp.Streams)
		if offset > numStreams {
			offset = numStreams
			resp.Streams = resp.Streams[:offset]
		}
	} else {
		msets := acc.filteredStreams(filter)
		// Since we page results order matters.
		if len(msets) > 1 {
			sort.Slice(msets, func(i, j int) bool {
				return strings.Compare(msets[i].cfg.Name, msets[j].cfg.Name) < 0
			})
		}

		numStreams = len(msets)
		if offset > numStreams {
			offset = numStreams
		}

		for _, mset := range msets[offset:] {
			resp.Streams = append(resp.Streams, mset.cfg.Name)
			if len(resp.Streams) >= JSApiNamesLimit {
				break
			}
		}
	}
	resp.Total = numStreams
	resp.Limit = JSApiNamesLimit
	resp.Offset = offset
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for the list of all detailed stream info.
// TODO(dlc) - combine with above long term
func (s *Server) jsStreamListRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiStreamListResponse{
		ApiResponse: ApiResponse{Type: JSApiStreamListResponseType},
		Streams:     []*StreamInfo{},
	}

	// Determine if we should proceed here when we are in clustered mode.
	if s.JetStreamIsClustered() {
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			return
		}
		if js.isLeaderless() {
			resp.Error = jsClusterNotAvailErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		// Make sure we are meta leader.
		if !s.JetStreamIsLeader() {
			return
		}
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	var offset int
	if !isEmptyRequest(msg) {
		var req JSApiStreamNamesRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			resp.Error = jsInvalidJSONErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		offset = req.Offset
	}

	// Clustered mode will invoke a scatter and gather.
	if s.JetStreamIsClustered() {
		// Need to copy these off before sending..
		msg = append(msg[:0:0], msg...)
		s.startGoRoutine(func() { s.jsClusteredStreamListRequest(acc, ci, offset, subject, reply, msg) })
		return
	}

	// TODO(dlc) - Maybe hold these results for large results that we expect to be paged.
	// TODO(dlc) - If this list is long maybe do this in a Go routine?
	msets := acc.streams()
	sort.Slice(msets, func(i, j int) bool {
		return strings.Compare(msets[i].cfg.Name, msets[j].cfg.Name) < 0
	})

	scnt := len(msets)
	if offset > scnt {
		offset = scnt
	}

	for _, mset := range msets[offset:] {
		resp.Streams = append(resp.Streams, &StreamInfo{Created: mset.createdTime(), State: mset.state(), Config: mset.config(), Mirror: mset.mirrorInfo(), Sources: mset.sourcesInfo()})
		if len(resp.Streams) >= JSApiListLimit {
			break
		}
	}
	resp.Total = scnt
	resp.Limit = JSApiListLimit
	resp.Offset = offset
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for information about a stream.
func (s *Server) jsStreamInfoRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	streamName := streamNameFromSubject(subject)

	var resp = JSApiStreamInfoResponse{ApiResponse: ApiResponse{Type: JSApiStreamInfoResponseType}}

	// If we are in clustered mode we need to be the stream leader to proceed.
	if s.JetStreamIsClustered() {
		// Check to make sure the stream is assigned.
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			return
		}

		js.mu.RLock()
		isLeader, sa := cc.isLeader(), js.streamAssignment(acc.Name, streamName)
		js.mu.RUnlock()

		if isLeader && sa == nil {
			// We can't find the stream, so mimic what would be the errors below.
			if !acc.JetStreamEnabled() {
				resp.Error = jsNotEnabledErr
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
				return
			}
			// No stream present.
			resp.Error = jsNotFoundError(ErrJetStreamStreamNotFound)
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		} else if sa == nil {
			if js.isLeaderless() {
				resp.Error = jsClusterNotAvailErr
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			}
			return
		}

		// Check to see if we are a member of the group and if the group has no leader.
		isLeaderless := js.isGroupLeaderless(sa.Group)

		// We have the stream assigned and a leader, so only the stream leader should answer.
		if !acc.JetStreamIsStreamLeader(streamName) && !isLeaderless {
			if js.isLeaderless() {
				resp.Error = jsClusterNotAvailErr
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			}
			return
		}
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsNotEmptyRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	mset, err := acc.lookupStream(streamName)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	config := mset.config()

	js, _ := s.getJetStreamCluster()

	resp.StreamInfo = &StreamInfo{Created: mset.createdTime(), State: mset.state(), Config: config, Cluster: js.clusterInfo(mset.raftGroup())}
	if mset.isMirror() {
		resp.StreamInfo.Mirror = mset.mirrorInfo()
	} else if mset.hasSources() {
		resp.StreamInfo.Sources = mset.sourcesInfo()
	}

	// Check for out of band catchups.
	if mset.hasCatchupPeers() {
		mset.checkClusterInfo(resp.StreamInfo)
	}

	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to have a stream leader stepdown.
func (s *Server) jsStreamLeaderStepDownRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	// Have extra token for this one.
	name := tokenAt(subject, 6)

	var resp = JSApiStreamLeaderStepDownResponse{ApiResponse: ApiResponse{Type: JSApiStreamLeaderStepDownResponseType}}

	// If we are not in clustered mode this is a failed request.
	if !s.JetStreamIsClustered() {
		resp.Error = jsClusterRequiredErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	// If we are here we are clustered. See if we are the stream leader in order to proceed.
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}
	if js.isLeaderless() {
		resp.Error = jsClusterNotAvailErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	js.mu.RLock()
	isLeader, sa := cc.isLeader(), js.streamAssignment(acc.Name, name)
	js.mu.RUnlock()

	if isLeader && sa == nil {
		resp.Error = jsNotFoundError(ErrJetStreamStreamNotFound)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	} else if sa == nil {
		return
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsBadRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	// Check to see if we are a member of the group and if the group has no leader.
	if js.isGroupLeaderless(sa.Group) {
		resp.Error = jsClusterNotAvailErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	// We have the stream assigned and a leader, so only the stream leader should answer.
	if !acc.JetStreamIsStreamLeader(name) {
		return
	}

	mset, err := acc.lookupStream(name)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	// Call actual stepdown.
	if mset != nil {
		if node := mset.raftNode(); node != nil {
			node.StepDown()
		}
	}

	resp.Success = true
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to have a consumer leader stepdown.
func (s *Server) jsConsumerLeaderStepDownRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiConsumerLeaderStepDownResponse{ApiResponse: ApiResponse{Type: JSApiConsumerLeaderStepDownResponseType}}

	// If we are not in clustered mode this is a failed request.
	if !s.JetStreamIsClustered() {
		resp.Error = jsClusterRequiredErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	// If we are here we are clustered. See if we are the stream leader in order to proceed.
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}
	if js.isLeaderless() {
		resp.Error = jsClusterNotAvailErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	// Have extra token for this one.
	stream := tokenAt(subject, 6)
	consumer := tokenAt(subject, 7)

	js.mu.RLock()
	isLeader, sa := cc.isLeader(), js.streamAssignment(acc.Name, stream)
	js.mu.RUnlock()

	if isLeader && sa == nil {
		resp.Error = jsNotFoundError(ErrJetStreamStreamNotFound)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	} else if sa == nil {
		return
	}
	var ca *consumerAssignment
	if sa.consumers != nil {
		ca = sa.consumers[consumer]
	}
	if ca == nil {
		resp.Error = jsNoConsumerErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	// Check to see if we are a member of the group and if the group has no leader.
	if js.isGroupLeaderless(ca.Group) {
		resp.Error = jsClusterNotAvailErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	if !acc.JetStreamIsConsumerLeader(stream, consumer) {
		return
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsBadRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	mset, err := acc.lookupStream(stream)
	if err != nil {
		resp.Error = jsNotFoundError(ErrJetStreamStreamNotFound)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	o := mset.lookupConsumer(consumer)
	if o == nil {
		resp.Error = jsNoConsumerErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	// Call actual stepdown.
	o.raftNode().StepDown()

	resp.Success = true
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to remove a peer from a clustered stream.
func (s *Server) jsStreamRemovePeerRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	// Have extra token for this one.
	name := tokenAt(subject, 6)

	var resp = JSApiStreamRemovePeerResponse{ApiResponse: ApiResponse{Type: JSApiStreamRemovePeerResponseType}}

	// If we are not in clustered mode this is a failed request.
	if !s.JetStreamIsClustered() {
		resp.Error = jsClusterRequiredErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	// If we are here we are clustered. See if we are the stream leader in order to proceed.
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}
	if js.isLeaderless() {
		resp.Error = jsClusterNotAvailErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	js.mu.RLock()
	isLeader, sa := cc.isLeader(), js.streamAssignment(acc.Name, name)
	js.mu.RUnlock()

	// Make sure we are meta leader.
	if !isLeader {
		return
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if isEmptyRequest(msg) {
		resp.Error = jsBadRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	var req JSApiStreamRemovePeerRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		resp.Error = jsInvalidJSONErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if req.Peer == _EMPTY_ {
		resp.Error = jsBadRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	if sa == nil {
		// No stream present.
		resp.Error = jsNotFoundError(ErrJetStreamStreamNotFound)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	// Check to see if we are a member of the group and if the group has no leader.
	// Peers here is a server name, convert to node name.
	nodeName := string(getHash(req.Peer))

	js.mu.RLock()
	rg := sa.Group
	isMember := rg.isMember(nodeName)
	js.mu.RUnlock()

	// Make sure we are a member.
	if !isMember {
		resp.Error = jsPeerNotMemberErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	// If we are here we have a valid peer member set for removal.
	js.mu.Lock()
	js.removePeerFromStream(sa, nodeName)
	js.mu.Unlock()

	resp.Success = true
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to have the metaleader remove a peer from the system.
func (s *Server) jsLeaderServerRemoveRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}

	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil || cc.meta == nil {
		return
	}

	// Extra checks here but only leader is listening.
	js.mu.RLock()
	isLeader := cc.isLeader()
	js.mu.RUnlock()

	if !isLeader {
		return
	}

	var resp = JSApiMetaServerRemoveResponse{ApiResponse: ApiResponse{Type: JSApiMetaServerRemoveResponseType}}

	if isEmptyRequest(msg) {
		resp.Error = jsBadRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	var req JSApiMetaServerRemoveRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		resp.Error = jsInvalidJSONErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	var found string
	js.mu.RLock()
	for _, p := range cc.meta.Peers() {
		si, ok := s.nodeToInfo.Load(p.ID)
		if ok && si.(nodeInfo).name == req.Server {
			found = p.ID
			break
		}
	}
	js.mu.RUnlock()

	if found == _EMPTY_ {
		resp.Error = jsServerNotMemberErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	// So we have a valid peer.
	js.mu.Lock()
	cc.meta.ProposeRemovePeer(found)
	js.mu.Unlock()

	resp.Success = true
	s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
}

// Request to have the meta leader stepdown.
// These will only be received the the meta leaders, so less checking needed.
func (s *Server) jsLeaderStepDownRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}

	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil || cc.meta == nil {
		return
	}

	// Extra checks here but only leader is listening.
	js.mu.RLock()
	isLeader := cc.isLeader()
	js.mu.RUnlock()

	if !isLeader {
		return
	}

	var preferredLeader string
	var resp = JSApiLeaderStepDownResponse{ApiResponse: ApiResponse{Type: JSApiLeaderStepDownResponseType}}

	if !isEmptyRequest(msg) {
		var req JSApiLeaderStepdownRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			resp.Error = jsInvalidJSONErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		if len(req.Placement.Tags) > 0 {
			// Tags currently not supported.
			resp.Error = jsClusterTagsErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		cn := req.Placement.Cluster
		var peers []string
		ourID := cc.meta.ID()
		for _, p := range cc.meta.Peers() {
			if si, ok := s.nodeToInfo.Load(p.ID); ok && si != nil {
				if ni := si.(nodeInfo); ni.offline || ni.cluster != cn || p.ID == ourID {
					continue
				}
				peers = append(peers, p.ID)
			}
		}
		if len(peers) == 0 {
			resp.Error = jsClusterNoPeersErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		// Randomize and select.
		if len(peers) > 1 {
			rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
		}
		preferredLeader = peers[0]
	}

	// Call actual stepdown.
	err = cc.meta.StepDown(preferredLeader)

	if err != nil {
		resp.Error = jsError(err)
	} else {
		resp.Success = true
	}
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

func isEmptyRequest(req []byte) bool {
	if len(req) == 0 {
		return true
	}
	if bytes.Equal(req, []byte("{}")) {
		return true
	}
	// If we are here we didn't get our simple match, but still could be valid.
	var v interface{}
	if err := json.Unmarshal(req, &v); err != nil {
		return false
	}
	vm, ok := v.(map[string]interface{})
	if !ok {
		return false
	}
	return len(vm) == 0
}

// Request to delete a stream.
func (s *Server) jsStreamDeleteRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiStreamDeleteResponse{ApiResponse: ApiResponse{Type: JSApiStreamDeleteResponseType}}

	// Determine if we should proceed here when we are in clustered mode.
	if s.JetStreamIsClustered() {
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			return
		}
		if js.isLeaderless() {
			resp.Error = jsClusterNotAvailErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		// Make sure we are meta leader.
		if !s.JetStreamIsLeader() {
			return
		}
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsNotEmptyRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	stream := streamNameFromSubject(subject)

	// Clustered.
	if s.JetStreamIsClustered() {
		s.jsClusteredStreamDeleteRequest(ci, acc, stream, subject, reply, msg)
		return
	}

	mset, err := acc.lookupStream(stream)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	if err := mset.delete(); err != nil {
		resp.Error = jsError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.Success = true
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to delete a message.
// This expects a stream sequence number as the msg body.
func (s *Server) jsMsgDeleteRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	stream := tokenAt(subject, 6)

	var resp = JSApiMsgDeleteResponse{ApiResponse: ApiResponse{Type: JSApiMsgDeleteResponseType}}

	// If we are in clustered mode we need to be the stream leader to proceed.
	if s.JetStreamIsClustered() {
		// Check to make sure the stream is assigned.
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			return
		}
		if js.isLeaderless() {
			resp.Error = jsClusterNotAvailErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}

		js.mu.RLock()
		isLeader, sa := cc.isLeader(), js.streamAssignment(acc.Name, stream)
		js.mu.RUnlock()

		if isLeader && sa == nil {
			// We can't find the stream, so mimic what would be the errors below.
			if !acc.JetStreamEnabled() {
				resp.Error = jsNotEnabledErr
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
				return
			}
			// No stream present.
			resp.Error = jsNotFoundError(ErrJetStreamStreamNotFound)
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		} else if sa == nil {
			return
		}

		// Check to see if we are a member of the group and if the group has no leader.
		if js.isGroupLeaderless(sa.Group) {
			resp.Error = jsClusterNotAvailErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}

		// We have the stream assigned and a leader, so only the stream leader should answer.
		if !acc.JetStreamIsStreamLeader(stream) {
			return
		}
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if isEmptyRequest(msg) {
		resp.Error = jsBadRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	var req JSApiMsgDeleteRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		resp.Error = jsInvalidJSONErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	mset, err := acc.lookupStream(stream)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	if s.JetStreamIsClustered() {
		s.jsClusteredMsgDeleteRequest(ci, acc, mset, stream, subject, reply, &req, rmsg)
		return
	}

	var removed bool
	if req.NoErase {
		removed, err = mset.removeMsg(req.Seq)
	} else {
		removed, err = mset.eraseMsg(req.Seq)
	}
	if err != nil {
		resp.Error = jsError(err)
	} else if !removed {
		resp.Error = &ApiError{Code: 400, Description: fmt.Sprintf("sequence [%d] not found", req.Seq)}
	} else {
		resp.Success = true
	}
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to get a raw stream message.
func (s *Server) jsMsgGetRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	stream := tokenAt(subject, 6)

	var resp = JSApiMsgGetResponse{ApiResponse: ApiResponse{Type: JSApiMsgGetResponseType}}

	// If we are in clustered mode we need to be the stream leader to proceed.
	if s.JetStreamIsClustered() {
		// Check to make sure the stream is assigned.
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			return
		}
		if js.isLeaderless() {
			resp.Error = jsClusterNotAvailErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}

		js.mu.RLock()
		isLeader, sa := cc.isLeader(), js.streamAssignment(acc.Name, stream)
		js.mu.RUnlock()

		if isLeader && sa == nil {
			// We can't find the stream, so mimic what would be the errors below.
			if !acc.JetStreamEnabled() {
				resp.Error = jsNotEnabledErr
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
				return
			}
			// No stream present.
			resp.Error = jsNotFoundError(ErrJetStreamStreamNotFound)
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		} else if sa == nil {
			return
		}

		// Check to see if we are a member of the group and if the group has no leader.
		if js.isGroupLeaderless(sa.Group) {
			resp.Error = jsClusterNotAvailErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}

		// We have the stream assigned and a leader, so only the stream leader should answer.
		if !acc.JetStreamIsStreamLeader(stream) {
			return
		}
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if isEmptyRequest(msg) {
		resp.Error = jsBadRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	var req JSApiMsgGetRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		resp.Error = jsInvalidJSONErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	mset, err := acc.lookupStream(stream)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	subj, hdr, msg, ts, err := mset.store.LoadMsg(req.Seq)
	if err != nil {
		resp.Error = jsNoMessageFoundErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.Message = &StoredMsg{
		Subject:  subj,
		Sequence: req.Seq,
		Header:   hdr,
		Data:     msg,
		Time:     time.Unix(0, ts).UTC(),
	}
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to purge a stream.
func (s *Server) jsStreamPurgeRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	stream := streamNameFromSubject(subject)

	var resp = JSApiStreamPurgeResponse{ApiResponse: ApiResponse{Type: JSApiStreamPurgeResponseType}}

	// If we are in clustered mode we need to be the stream leader to proceed.
	if s.JetStreamIsClustered() {
		// Check to make sure the stream is assigned.
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			return
		}

		js.mu.RLock()
		isLeader, sa := cc.isLeader(), js.streamAssignment(acc.Name, stream)
		js.mu.RUnlock()

		if isLeader && sa == nil {
			// We can't find the stream, so mimic what would be the errors below.
			if !acc.JetStreamEnabled() {
				resp.Error = jsNotEnabledErr
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
				return
			}
			// No stream present.
			resp.Error = jsNotFoundError(ErrJetStreamStreamNotFound)
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		} else if sa == nil {
			if js.isLeaderless() {
				resp.Error = jsClusterNotAvailErr
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			}
			return
		}

		// Check to see if we are a member of the group and if the group has no leader.
		if js.isGroupLeaderless(sa.Group) {
			resp.Error = jsClusterNotAvailErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}

		// We have the stream assigned and a leader, so only the stream leader should answer.
		if !acc.JetStreamIsStreamLeader(stream) {
			if js.isLeaderless() {
				resp.Error = jsClusterNotAvailErr
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			}
			return
		}
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsNotEmptyRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	mset, err := acc.lookupStream(stream)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	if s.JetStreamIsClustered() {
		s.jsClusteredStreamPurgeRequest(ci, acc, mset, stream, subject, reply, rmsg)
		return
	}

	purged, err := mset.purge()
	if err != nil {
		resp.Error = jsError(err)
	} else {
		resp.Purged = purged
		resp.Success = true
	}
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to restore a stream.
func (s *Server) jsStreamRestoreRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamIsLeader() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiStreamRestoreResponse{ApiResponse: ApiResponse{Type: JSApiStreamRestoreResponseType}}
	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if isEmptyRequest(msg) {
		resp.Error = jsBadRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	var req JSApiStreamRestoreRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		resp.Error = jsInvalidJSONErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	stream := streamNameFromSubject(subject)

	if stream != req.Config.Name && req.Config.Name == _EMPTY_ {
		req.Config.Name = stream
	}

	if s.JetStreamIsClustered() {
		s.jsClusteredStreamRestoreRequest(ci, acc, &req, stream, subject, reply, rmsg)
		return
	}

	if _, err := acc.lookupStream(stream); err == nil {
		resp.Error = jsError(ErrJetStreamStreamAlreadyUsed)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	s.processStreamRestore(ci, acc, &req.Config, subject, reply, string(msg))
}

func (s *Server) processStreamRestore(ci *ClientInfo, acc *Account, cfg *StreamConfig, subject, reply, msg string) <-chan error {
	js := s.getJetStream()

	var resp = JSApiStreamRestoreResponse{ApiResponse: ApiResponse{Type: JSApiStreamRestoreResponseType}}

	// FIXME(dlc) - Need to close these up if we fail for some reason.
	// TODO(dlc) - Might need to make configurable or stream direct to storage dir.
	tfile, err := ioutil.TempFile("", "jetstream-restore-")
	if err != nil {
		resp.Error = &ApiError{Code: 500, Description: "JetStream unable to open temp storage for restore"}
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return nil
	}

	streamName := cfg.Name
	s.Noticef("Starting restore for stream '%s > %s'", acc.Name, streamName)

	start := time.Now().UTC()

	s.publishAdvisory(acc, JSAdvisoryStreamRestoreCreatePre+"."+streamName, &JSRestoreCreateAdvisory{
		TypedEvent: TypedEvent{
			Type: JSRestoreCreateAdvisoryType,
			ID:   nuid.Next(),
			Time: start,
		},
		Stream: streamName,
		Client: ci,
	})

	// Create our internal subscription to accept the snapshot.
	restoreSubj := fmt.Sprintf(jsRestoreDeliverT, streamName, nuid.Next())

	type result struct {
		err   error
		reply string
	}

	// For signaling to upper layers.
	resultCh := make(chan result, 1)
	activeCh := make(chan int, 32)

	var total int

	// FIXM(dlc) - Probably take out of network path eventually do to disk I/O?
	processChunk := func(sub *subscription, c *client, subject, reply string, msg []byte) {
		// We require reply subjects to communicate back failures, flow etc. If they do not have one log and cancel.
		if reply == _EMPTY_ {
			sub.client.processUnsub(sub.sid)
			resultCh <- result{
				fmt.Errorf("restore for stream '%s > %s' requires reply subject for each chunk", acc.Name, streamName),
				reply,
			}
			return
		}
		// Account client messages have \r\n on end. This is an error.
		if len(msg) < LEN_CR_LF {
			sub.client.processUnsub(sub.sid)
			resultCh <- result{
				fmt.Errorf("restore for stream '%s > %s' received short chunk", acc.Name, streamName),
				reply,
			}
			return
		}
		// Adjust.
		msg = msg[:len(msg)-LEN_CR_LF]

		// This means we are complete with our transfer from the client.
		if len(msg) == 0 {
			s.Debugf("Finished staging restore for stream '%s > %s'", acc.Name, streamName)
			resultCh <- result{err, reply}
			return
		}

		// We track total and check on server limits.
		// TODO(dlc) - We could check apriori and cancel initial request if we know it won't fit.
		total += len(msg)
		if js.wouldExceedLimits(FileStorage, total) {
			s.resourcesExeededError()
			resultCh <- result{ErrJetStreamResourcesExceeded, reply}
			return
		}

		// Append chunk to temp file. Mark as issue if we encounter an error.
		if n, err := tfile.Write(msg); n != len(msg) || err != nil {
			resultCh <- result{err, reply}
			if reply != _EMPTY_ {
				s.sendInternalAccountMsg(acc, reply, "-ERR 'storage failure during restore'")
			}
			return
		}

		activeCh <- len(msg)

		s.sendInternalAccountMsg(acc, reply, nil)
	}

	sub, err := acc.subscribeInternal(restoreSubj, processChunk)
	if err != nil {
		resp.Error = &ApiError{Code: 500, Description: "JetStream unable to subscribe to restore snapshot"}
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return nil
	}

	// Mark the subject so the end user knows where to send the snapshot chunks.
	resp.DeliverSubject = restoreSubj
	s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))

	doneCh := make(chan error, 1)

	// Monitor the progress from another Go routine.
	s.startGoRoutine(func() {
		defer s.grWG.Done()
		defer func() {
			tfile.Close()
			os.Remove(tfile.Name())
			sub.client.processUnsub(sub.sid)
		}()

		const activityInterval = 5 * time.Second
		notActive := time.NewTimer(activityInterval)
		defer notActive.Stop()

		total := 0
		for {
			select {
			case result := <-resultCh:
				err := result.err
				var mset *stream

				// If we staged properly go ahead and do restore now.
				if err == nil {
					s.Debugf("Finalizing restore for  stream '%s > %s'", acc.Name, streamName)
					tfile.Seek(0, 0)
					mset, err = acc.RestoreStream(cfg, tfile)
				} else {
					errStr := err.Error()
					tmp := []rune(errStr)
					tmp[0] = unicode.ToUpper(tmp[0])
					s.Warnf(errStr)
				}

				end := time.Now().UTC()

				// TODO(rip) - Should this have the error code in it??
				s.publishAdvisory(acc, JSAdvisoryStreamRestoreCompletePre+"."+streamName, &JSRestoreCompleteAdvisory{
					TypedEvent: TypedEvent{
						Type: JSRestoreCompleteAdvisoryType,
						ID:   nuid.Next(),
						Time: end,
					},
					Stream: streamName,
					Start:  start,
					End:    end,
					Bytes:  int64(total),
					Client: ci,
				})

				var resp = JSApiStreamCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamCreateResponseType}}

				if err != nil {
					resp.Error = jsError(err)
					s.Warnf("Restore failed for %s for stream '%s > %s' in %v",
						friendlyBytes(int64(total)), streamName, acc.Name, end.Sub(start))
				} else {
					resp.StreamInfo = &StreamInfo{Created: mset.createdTime(), State: mset.state(), Config: mset.config()}
					s.Noticef("Completed restore of %s for stream '%s > %s' in %v",
						friendlyBytes(int64(total)), streamName, acc.Name, end.Sub(start))
				}

				// On the last EOF, send back the stream info or error status.
				s.sendInternalAccountMsg(acc, result.reply, s.jsonResponse(&resp))
				// Signal to the upper layers.
				doneCh <- err
				return
			case n := <-activeCh:
				total += n
				notActive.Reset(activityInterval)
			case <-notActive.C:
				err := fmt.Errorf("restore for stream '%s > %s' is stalled", acc, streamName)
				doneCh <- err
				return
			}
		}
	})

	return doneCh
}

// Process a snapshot request.
func (s *Server) jsStreamSnapshotRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	smsg := string(msg)
	stream := streamNameFromSubject(subject)

	// If we are in clustered mode we need to be the stream leader to proceed.
	if s.JetStreamIsClustered() && !acc.JetStreamIsStreamLeader(stream) {
		return
	}

	var resp = JSApiStreamSnapshotResponse{ApiResponse: ApiResponse{Type: JSApiStreamSnapshotResponseType}}
	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, smsg, s.jsonResponse(&resp))
		return
	}
	if isEmptyRequest(msg) {
		resp.Error = jsBadRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, smsg, s.jsonResponse(&resp))
		return
	}

	mset, err := acc.lookupStream(stream)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, smsg, s.jsonResponse(&resp))
		return
	}

	var req JSApiStreamSnapshotRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		resp.Error = jsInvalidJSONErr
		s.sendAPIErrResponse(ci, acc, subject, reply, smsg, s.jsonResponse(&resp))
		return
	}
	if !IsValidSubject(req.DeliverSubject) {
		resp.Error = &ApiError{Code: 400, Description: "deliver subject not valid"}
		s.sendAPIErrResponse(ci, acc, subject, reply, smsg, s.jsonResponse(&resp))
		return
	}

	// We will do the snapshot in a go routine as well since check msgs may
	// stall this go routine.
	go func() {
		if req.CheckMsgs {
			s.Noticef("Starting health check and snapshot for stream '%s > %s'", mset.jsa.account.Name, mset.name())
		} else {
			s.Noticef("Starting snapshot for stream '%s > %s'", mset.jsa.account.Name, mset.name())
		}

		start := time.Now().UTC()

		sr, err := mset.snapshot(0, req.CheckMsgs, !req.NoConsumers)
		if err != nil {
			s.Warnf("Snapshot of stream '%s > %s' failed: %v", mset.jsa.account.Name, mset.name(), err)
			resp.Error = jsError(err)
			s.sendAPIErrResponse(ci, acc, subject, reply, smsg, s.jsonResponse(&resp))
			return
		}

		config := mset.config()
		resp.State = &sr.State
		resp.Config = &config

		s.sendAPIResponse(ci, acc, subject, reply, smsg, s.jsonResponse(resp))

		s.publishAdvisory(acc, JSAdvisoryStreamSnapshotCreatePre+"."+mset.name(), &JSSnapshotCreateAdvisory{
			TypedEvent: TypedEvent{
				Type: JSSnapshotCreatedAdvisoryType,
				ID:   nuid.Next(),
				Time: time.Now().UTC(),
			},
			Stream: mset.name(),
			State:  sr.State,
			Client: ci,
		})

		// Now do the real streaming.
		s.streamSnapshot(ci, acc, mset, sr, &req)

		end := time.Now().UTC()

		s.publishAdvisory(acc, JSAdvisoryStreamSnapshotCompletePre+"."+mset.name(), &JSSnapshotCompleteAdvisory{
			TypedEvent: TypedEvent{
				Type: JSSnapshotCompleteAdvisoryType,
				ID:   nuid.Next(),
				Time: end,
			},
			Stream: mset.name(),
			Start:  start,
			End:    end,
			Client: ci,
		})

		s.Noticef("Completed snapshot of %s for stream '%s > %s' in %v",
			friendlyBytes(int64(sr.State.Bytes)),
			mset.jsa.account.Name,
			mset.name(),
			end.Sub(start))
	}()
}

// Default chunk size for now.
const defaultSnapshotChunkSize = 256 * 1024
const defaultSnapshotWindowSize = 32 * 1024 * 1024 // 32MB

// streamSnapshot will stream out our snapshot to the reply subject.
func (s *Server) streamSnapshot(ci *ClientInfo, acc *Account, mset *stream, sr *SnapshotResult, req *JSApiStreamSnapshotRequest) {
	chunkSize := req.ChunkSize
	if chunkSize == 0 {
		chunkSize = defaultSnapshotChunkSize
	}
	// Setup for the chunk stream.
	reply := req.DeliverSubject
	r := sr.Reader
	defer r.Close()

	// Check interest for the snapshot deliver subject.
	inch := make(chan bool, 1)
	acc.sl.RegisterNotification(req.DeliverSubject, inch)
	defer acc.sl.ClearNotification(req.DeliverSubject, inch)
	hasInterest := <-inch
	if !hasInterest {
		// Allow 2 seconds or so for interest to show up.
		select {
		case <-inch:
		case <-time.After(2 * time.Second):
		}
	}

	// Create our ack flow handler.
	// This is very simple for now.
	acks := make(chan struct{}, 1)
	acks <- struct{}{}

	// Track bytes outstanding.
	var out int32

	// We will place sequence number and size of chunk sent in the reply.
	ackSubj := fmt.Sprintf(jsSnapshotAckT, mset.name(), nuid.Next())
	ackSub, _ := mset.subscribeInternalUnlocked(ackSubj+".>", func(_ *subscription, _ *client, subject, _ string, _ []byte) {
		cs, _ := strconv.Atoi(tokenAt(subject, 6))
		// This is very crude and simple, but ok for now.
		// This only matters when sending multiple chunks.
		if atomic.AddInt32(&out, int32(-cs)) < defaultSnapshotWindowSize {
			select {
			case acks <- struct{}{}:
			default:
			}
		}
	})
	defer mset.unsubscribeUnlocked(ackSub)

	// TODO(dlc) - Add in NATS-Chunked-Sequence header

	for index := 1; ; index++ {
		chunk := make([]byte, chunkSize)
		n, err := r.Read(chunk)
		chunk = chunk[:n]
		if err != nil {
			if n > 0 {
				mset.outq.send(&jsPubMsg{reply, _EMPTY_, _EMPTY_, nil, chunk, nil, 0, nil})
			}
			break
		}

		// Wait on acks for flow control if past our window size.
		// Wait up to 1ms for now if no acks received.
		if atomic.LoadInt32(&out) > defaultSnapshotWindowSize {
			select {
			case <-acks:
			case <-inch: // Lost interest
				goto done
			case <-time.After(10 * time.Millisecond):
			}
		}
		ackReply := fmt.Sprintf("%s.%d.%d", ackSubj, len(chunk), index)
		mset.outq.send(&jsPubMsg{reply, _EMPTY_, ackReply, nil, chunk, nil, 0, nil})
		atomic.AddInt32(&out, int32(len(chunk)))
	}
done:
	// Send last EOF
	// TODO(dlc) - place hash in header
	mset.outq.send(&jsPubMsg{reply, _EMPTY_, _EMPTY_, nil, nil, nil, 0, nil})
}

// Request to create a durable consumer.
func (s *Server) jsDurableCreateRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	s.jsConsumerCreate(sub, c, subject, reply, msg, true)
}

// Request to create a consumer.
func (s *Server) jsConsumerCreateRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	s.jsConsumerCreate(sub, c, subject, reply, msg, false)
}

func (s *Server) jsConsumerCreate(sub *subscription, c *client, subject, reply string, rmsg []byte, expectDurable bool) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}

	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}}

	var streamName string
	if expectDurable {
		streamName = tokenAt(subject, 6)
	} else {
		streamName = tokenAt(subject, 5)
	}

	var req CreateConsumerRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		resp.Error = jsInvalidJSONErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	// Determine if we should proceed here when we are in clustered mode.
	if s.JetStreamIsClustered() {
		if req.Config.Direct {
			// Check to see if we have this stream and are the stream leader.
			if !acc.JetStreamIsStreamLeader(streamName) {
				return
			}
		} else {
			js, cc := s.getJetStreamCluster()
			if js == nil || cc == nil {
				return
			}
			if js.isLeaderless() {
				resp.Error = jsClusterNotAvailErr
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
				return
			}
			// Make sure we are meta leader.
			if !s.JetStreamIsLeader() {
				return
			}
		}
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if streamName != req.Stream {
		resp.Error = jsStreamMismatchErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	if expectDurable {
		if numTokens(subject) != 7 {
			resp.Error = &ApiError{Code: 400, Description: "consumer expected to be durable but no durable name set in subject"}
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		// Now check on requirements for durable request.
		if req.Config.Durable == _EMPTY_ {
			resp.Error = &ApiError{Code: 400, Description: "consumer expected to be durable but a durable name was not set"}
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		consumerName := tokenAt(subject, 7)
		if consumerName != req.Config.Durable {
			resp.Error = &ApiError{Code: 400, Description: "consumer name in subject does not match durable name in request"}
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
	} else {
		if numTokens(subject) != 5 {
			resp.Error = &ApiError{Code: 400, Description: "consumer expected to be ephemeral but detected a durable name set in subject"}
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		if req.Config.Durable != _EMPTY_ {
			resp.Error = &ApiError{Code: 400, Description: "consumer expected to be ephemeral but a durable name was set in request"}
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
	}

	if s.JetStreamIsClustered() && !req.Config.Direct {
		s.jsClusteredConsumerRequest(ci, acc, subject, reply, rmsg, req.Stream, &req.Config)
		return
	}

	stream, err := acc.lookupStream(req.Stream)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	o, err := stream.addConsumer(&req.Config)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.ConsumerInfo = o.info()
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for the list of all consumer names.
func (s *Server) jsConsumerNamesRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiConsumerNamesResponse{
		ApiResponse: ApiResponse{Type: JSApiConsumerNamesResponseType},
		Consumers:   []string{},
	}

	// Determine if we should proceed here when we are in clustered mode.
	if s.JetStreamIsClustered() {
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			return
		}
		if js.isLeaderless() {
			resp.Error = jsClusterNotAvailErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		// Make sure we are meta leader.
		if !s.JetStreamIsLeader() {
			return
		}
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	var offset int
	if !isEmptyRequest(msg) {
		var req JSApiConsumersRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			resp.Error = jsInvalidJSONErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		offset = req.Offset
	}

	streamName := streamNameFromSubject(subject)
	var numConsumers int

	if s.JetStreamIsClustered() {
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			// TODO(dlc) - Debug or Warn?
			return
		}
		js.mu.RLock()
		sas := cc.streams[acc.Name]
		if sas == nil {
			js.mu.RUnlock()
			resp.Error = jsNotFoundError(ErrJetStreamNotEnabled)
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		sa := sas[streamName]
		if sa == nil || sa.err != nil {
			js.mu.RUnlock()
			resp.Error = jsNotFoundError(ErrJetStreamStreamNotFound)
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		for consumer := range sa.consumers {
			resp.Consumers = append(resp.Consumers, consumer)
		}
		if len(resp.Consumers) > 1 {
			sort.Slice(resp.Consumers, func(i, j int) bool { return strings.Compare(resp.Consumers[i], resp.Consumers[j]) < 0 })
		}
		numConsumers = len(resp.Consumers)
		if offset > numConsumers {
			offset = numConsumers
			resp.Consumers = resp.Consumers[:offset]
		}
		js.mu.RUnlock()

	} else {
		mset, err := acc.lookupStream(streamName)
		if err != nil {
			resp.Error = jsNotFoundError(err)
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}

		obs := mset.getConsumers()
		sort.Slice(obs, func(i, j int) bool {
			return strings.Compare(obs[i].name, obs[j].name) < 0
		})

		numConsumers = len(obs)
		if offset > numConsumers {
			offset = numConsumers
		}

		for _, o := range obs[offset:] {
			resp.Consumers = append(resp.Consumers, o.String())
			if len(resp.Consumers) >= JSApiNamesLimit {
				break
			}
		}
	}
	resp.Total = numConsumers
	resp.Limit = JSApiNamesLimit
	resp.Offset = offset
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for the list of all detailed consumer information.
func (s *Server) jsConsumerListRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}

	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiConsumerListResponse{
		ApiResponse: ApiResponse{Type: JSApiConsumerListResponseType},
		Consumers:   []*ConsumerInfo{},
	}

	// Determine if we should proceed here when we are in clustered mode.
	if s.JetStreamIsClustered() {
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			return
		}
		if js.isLeaderless() {
			resp.Error = jsClusterNotAvailErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		// Make sure we are meta leader.
		if !s.JetStreamIsLeader() {
			return
		}
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	var offset int
	if !isEmptyRequest(msg) {
		var req JSApiConsumersRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			resp.Error = jsInvalidJSONErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		offset = req.Offset
	}

	streamName := streamNameFromSubject(subject)

	// Clustered mode will invoke a scatter and gather.
	if s.JetStreamIsClustered() {
		msg = append(msg[:0:0], msg...)
		s.startGoRoutine(func() {
			s.jsClusteredConsumerListRequest(acc, ci, offset, streamName, subject, reply, msg)
		})
		return
	}

	mset, err := acc.lookupStream(streamName)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	obs := mset.getConsumers()
	sort.Slice(obs, func(i, j int) bool {
		return strings.Compare(obs[i].name, obs[j].name) < 0
	})

	ocnt := len(obs)
	if offset > ocnt {
		offset = ocnt
	}

	for _, o := range obs[offset:] {
		resp.Consumers = append(resp.Consumers, o.info())
		if len(resp.Consumers) >= JSApiListLimit {
			break
		}
	}
	resp.Total = ocnt
	resp.Limit = JSApiListLimit
	resp.Offset = offset
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for information about an consumer.
func (s *Server) jsConsumerInfoRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	streamName := streamNameFromSubject(subject)
	consumerName := consumerNameFromSubject(subject)

	var resp = JSApiConsumerInfoResponse{ApiResponse: ApiResponse{Type: JSApiConsumerInfoResponseType}}

	// If we are in clustered mode we need to be the stream leader to proceed.
	if s.JetStreamIsClustered() {
		// Check to make sure the consumer is assigned.
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			return
		}

		js.mu.RLock()
		isLeader, sa, ca := cc.isLeader(), js.streamAssignment(acc.Name, streamName), js.consumerAssignment(acc.Name, streamName, consumerName)
		js.mu.RUnlock()

		if isLeader && ca == nil {
			// We can't find the consumer, so mimic what would be the errors below.
			if !acc.JetStreamEnabled() {
				resp.Error = jsNotEnabledErr
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
				return
			}
			if sa == nil {
				resp.Error = jsNotFoundError(ErrJetStreamStreamNotFound)
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
				return
			}
			// If we are here the consumer is not present.
			resp.Error = jsNoConsumerErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		} else if ca == nil {
			if js.isLeaderless() {
				resp.Error = jsClusterNotAvailErr
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			}
			return
		}

		// Check to see if we are a member of the group and if the group has no leader.
		if js.isGroupLeaderless(ca.Group) {
			resp.Error = jsClusterNotAvailErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}

		// We have the consumer assigned and a leader, so only the consumer leader should answer.
		if !acc.JetStreamIsConsumerLeader(streamName, consumerName) {
			if js.isLeaderless() {
				resp.Error = jsClusterNotAvailErr
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			}
			return
		}
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsNotEmptyRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	mset, err := acc.lookupStream(streamName)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	obs := mset.lookupConsumer(consumerName)
	if obs == nil {
		resp.Error = jsNoConsumerErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.ConsumerInfo = obs.info()
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to delete an Consumer.
func (s *Server) jsConsumerDeleteRequest(sub *subscription, c *client, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, _, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}

	var resp = JSApiConsumerDeleteResponse{ApiResponse: ApiResponse{Type: JSApiConsumerDeleteResponseType}}

	// Determine if we should proceed here when we are in clustered mode.
	if s.JetStreamIsClustered() {
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			return
		}
		if js.isLeaderless() {
			resp.Error = jsClusterNotAvailErr
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		// Make sure we are meta leader.
		if !s.JetStreamIsLeader() {
			return
		}
	}

	if !acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsNotEmptyRequestErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	stream := streamNameFromSubject(subject)
	consumer := consumerNameFromSubject(subject)

	if s.JetStreamIsClustered() {
		s.jsClusteredConsumerDeleteRequest(ci, acc, stream, consumer, subject, reply, rmsg)
		return
	}

	mset, err := acc.lookupStream(stream)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	obs := mset.lookupConsumer(consumer)
	if obs == nil {
		resp.Error = jsNoConsumerErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if err := obs.delete(); err != nil {
		resp.Error = jsError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.Success = true
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// sendJetStreamAPIAuditAdvisor will send the audit event for a given event.
func (s *Server) sendJetStreamAPIAuditAdvisory(ci *ClientInfo, acc *Account, subject, request, response string) {
	s.publishAdvisory(acc, JSAuditAdvisory, JSAPIAudit{
		TypedEvent: TypedEvent{
			Type: JSAPIAuditType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Server:   s.Name(),
		Client:   ci,
		Subject:  subject,
		Request:  request,
		Response: response,
	})
}
