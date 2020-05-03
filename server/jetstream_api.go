// Copyright 2020 The NATS Authors
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
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nuid"
)

// Request API subjects for JetStream.
const (
	// JSApiInfo is for obtaining general information about JetStream for this account.
	// Will return JSON response.
	JSApiAccountInfo = "$JS.API.INFO"

	// JSApiCreateTemplate is the endpoint to create new stream templates.
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

	// JSApiDeleteTemplate is the endpoint to delete stream templates.
	// Will return JSON response.
	JSApiTemplateDelete  = "$JS.API.STREAM.TEMPLATE.DELETE.*"
	JSApiTemplateDeleteT = "$JS.API.STREAM.TEMPLATE.DELETE.%s"

	// JSApiCreateStream is the endpoint to create new streams.
	// Will return JSON response.
	JSApiStreamCreate  = "$JS.API.STREAM.CREATE.*"
	JSApiStreamCreateT = "$JS.API.STREAM.CREATE.%s"

	// JSApiUpdateStream is the endpoint to update existing streams.
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

	// JSApiDeleteStream is the endpoint to delete streams.
	// Will return JSON response.
	JSApiStreamDelete  = "$JS.API.STREAM.DELETE.*"
	JSApiStreamDeleteT = "$JS.API.STREAM.DELETE.%s"

	// JSApiPurgeStream is the endpoint to purge streams.
	// Will return JSON response.
	JSApiStreamPurge  = "$JS.API.STREAM.PURGE.*"
	JSApiStreamPurgeT = "$JS.API.STREAM.PURGE.%s"

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

	// JSApiDurableCreate is the endpoint to create ephemeral consumers for streams.
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

	///////////////////////
	// FIXME(dlc)
	///////////////////////

	// JetStreamAckT is the template for the ack message stream coming back from a consumer
	// when they ACK/NAK, etc a message.
	// FIXME(dlc) - What do we really need here??
	jsAckT   = "$JS.ACK.%s.%s"
	jsAckPre = "$JS.ACK."

	// JSAdvisoryPrefix is a prefix for all JetStream advisories.
	JSAdvisoryPrefix = "$JS.EVENT.ADVISORY"

	// JSMetricPrefix is a prefix for all JetStream metrics.
	JSMetricPrefix = "$JS.EVENT.METRIC"

	// JSMetricConsumerAckPre is a metric containing ack latency.
	JSMetricConsumerAckPre = "$JS.EVENT.METRIC.CONSUMER.ACK"

	// JetStreamAdvisoryConsumerMaxDeliveryExceedPre is a notification published when a message exceeds its delivery threshold.
	JSAdvisoryConsumerMaxDeliveryExceedPre = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES"

	// JetStreamAPIAuditAdvisory is a notification about JetStream API access.
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

// JSApiAccountInfoResponse reports back information on jetstream for this account.
type JSApiAccountInfoResponse struct {
	Error *ApiError `json:"error,omitempty"`
	*JetStreamAccountStats
}

// JSApiStreamCreateResponse stream creation.
type JSApiStreamCreateResponse struct {
	Error *ApiError `json:"error,omitempty"`
	*StreamInfo
}

// JSApiStreamDeleteResponse stream removal.
type JSApiStreamDeleteResponse struct {
	Error   *ApiError `json:"error,omitempty"`
	Success bool      `json:"success,omitempty"`
}

// JSApiStreamInfoResponse.
type JSApiStreamInfoResponse struct {
	Error *ApiError `json:"error,omitempty"`
	*StreamInfo
}

// Maximum entries we will return for streams or consumers lists.
// TODO(dlc) - with header or request support could request chunked response.
const JSApiNamesLimit = 1024
const JSApiListLimit = 256

type JSApiStreamsRequest struct {
	Offset int `json:"offset"`
}

// JSApiStreamsResponse list of streams.
// A nil request is valid and means all streams.
type JSApiStreamsResponse struct {
	Error   *ApiError `json:"error,omitempty"`
	Total   int       `json:"total"`
	Offset  int       `json:"offset"`
	Limit   int       `json:"limit"`
	Streams []string  `json:"streams"`
}

// JSApiStreamListResponse list of detailed stream information.
// A nil request is valid and means all streams.
type JSApiStreamListResponse struct {
	Error   *ApiError     `json:"error,omitempty"`
	Total   int           `json:"total"`
	Offset  int           `json:"offset"`
	Limit   int           `json:"limit"`
	Streams []*StreamInfo `json:"streams"`
}

// JSApiStreamPurgeResponse.
type JSApiStreamPurgeResponse struct {
	Error   *ApiError `json:"error,omitempty"`
	Success bool      `json:"success,omitempty"`
	Purged  uint64    `json:"purged,omitempty"`
}

// JSApiStreamUpdateResponse for updating a stream.
type JSApiStreamUpdateResponse struct {
	Error *ApiError `json:"error,omitempty"`
	*StreamInfo
}

// JSApiMsgDeleteRequest delete message request.
type JSApiMsgDeleteRequest struct {
	Seq uint64 `json:"seq"`
}

// JSApiMsgDeleteResponse.
type JSApiMsgDeleteResponse struct {
	Error   *ApiError `json:"error,omitempty"`
	Success bool      `json:"success,omitempty"`
}

// JSApiMsgGetRequest get a message request.
type JSApiMsgGetRequest struct {
	Seq uint64 `json:"seq"`
}

// JSApiMsgGetResponse.
type JSApiMsgGetResponse struct {
	Error   *ApiError  `json:"error,omitempty"`
	Message *StoredMsg `json:"message,omitempty"`
}

// JSApiConsumerCreateResponse.
type JSApiConsumerCreateResponse struct {
	Error *ApiError `json:"error,omitempty"`
	*ConsumerInfo
}

// JSApiConsumerDeleteResponse.
type JSApiConsumerDeleteResponse struct {
	Error   *ApiError `json:"error,omitempty"`
	Success bool      `json:"success,omitempty"`
}

// JSApiConsumerInfoResponse.
type JSApiConsumerInfoResponse struct {
	Error *ApiError `json:"error,omitempty"`
	*ConsumerInfo
}

// JSApiConsumersRequest
type JSApiConsumersRequest struct {
	Offset int `json:"offset"`
}

// JSApiConsumersResponse.
type JSApiConsumersResponse struct {
	Error     *ApiError `json:"error,omitempty"`
	Total     int       `json:"total"`
	Offset    int       `json:"offset"`
	Limit     int       `json:"limit"`
	Consumers []string  `json:"streams"`
}

// JSApiConsumerListResponse.
type JSApiConsumerListResponse struct {
	Error     *ApiError       `json:"error,omitempty"`
	Total     int             `json:"total"`
	Offset    int             `json:"offset"`
	Limit     int             `json:"limit"`
	Consumers []*ConsumerInfo `json:"streams"`
}

// JSApiStreamTemplateCreateResponse for creating templates.
type JSApiStreamTemplateCreateResponse struct {
	Error *ApiError `json:"error,omitempty"`
	*StreamTemplateInfo
}

// JSApiStreamTemplateDeleteResponse
type JSApiStreamTemplateDeleteResponse struct {
	Error   *ApiError `json:"error,omitempty"`
	Success bool      `json:"success,omitempty"`
}

// JSApiStreamTemplateInfoResponse for information about stream templates.
type JSApiStreamTemplateInfoResponse struct {
	Error *ApiError `json:"error,omitempty"`
	*StreamTemplateInfo
}

// JSApiTemplatessRequest
type JSApiTemplatessRequest struct {
	Offset int `json:"offset"`
}

// JSApiStreamTemplateListResponse list of templates.
type JSApiStreamTemplatesResponse struct {
	Error     *ApiError `json:"error,omitempty"`
	Total     int       `json:"total"`
	Offset    int       `json:"offset"`
	Limit     int       `json:"limit"`
	Templates []string  `json:"streams,omitempty"`
}

var (
	jsNotEnabledErr = &ApiError{Code: 503, Description: "jetstream not enabled for account"}
	jsBadRequestErr = &ApiError{Code: 400, Description: "bad request"}
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
	JSApiMsgDelete,
	JSApiMsgGet,
	JSApiConsumerCreate,
	JSApiDurableCreate,
	JSApiConsumers,
	JSApiConsumerList,
	JSApiConsumerInfo,
	JSApiConsumerDelete,
}

func (s *Server) setJetStreamExportSubs() error {
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
		{JSApiMsgDelete, s.jsMsgDeleteRequest},
		{JSApiMsgGet, s.jsMsgGetRequest},
		{JSApiConsumerCreate, s.jsConsumerCreateRequest},
		{JSApiDurableCreate, s.jsDurableCreateRequest},
		{JSApiConsumers, s.jsConsumerNamesRequest},
		{JSApiConsumerList, s.jsConsumerListRequest},
		{JSApiConsumerInfo, s.jsConsumerInfoRequest},
		{JSApiConsumerDelete, s.jsConsumerDeleteRequest},
	}

	for _, p := range pairs {
		if _, err := s.sysSubscribe(p.subject, p.handler); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) sendAPIResponse(c *client, subject, reply, request, response string) {
	s.sendInternalAccountMsg(c.acc, reply, response)
	s.sendJetStreamAPIAuditAdvisory(c, subject, request, response)
}

// Request for current usage and limits for this account.
func (s *Server) jsAccountInfoRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiAccountInfoResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
	} else {
		stats := c.acc.JetStreamUsage()
		resp.JetStreamAccountStats = &stats
	}
	b, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		return
	}
	s.sendAPIResponse(c, subject, reply, string(msg), string(b))
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
func (s *Server) jsTemplateCreateRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiStreamTemplateCreateResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	var cfg StreamTemplateConfig
	if err := json.Unmarshal(msg, &cfg); err != nil {
		resp.Error = jsBadRequestErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	templateName := templateNameFromSubject(subject)
	if templateName != cfg.Name {
		resp.Error = &ApiError{Code: 400, Description: "template name in subject does not match request"}
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	t, err := c.acc.AddStreamTemplate(&cfg)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
	}
	t.mu.Lock()
	tcfg := t.StreamTemplateConfig.deepCopy()
	streams := t.streams
	t.mu.Unlock()
	resp.StreamTemplateInfo = &StreamTemplateInfo{Config: tcfg, Streams: streams}
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for the list of all template names.
func (s *Server) jsTemplateNamesRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiStreamTemplatesResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	var offset int
	if !isEmptyRequest(msg) {
		var req JSApiTemplatessRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			resp.Error = jsBadRequestErr
			s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		offset = req.Offset
	}

	ts := c.acc.Templates()
	sort.Slice(ts, func(i, j int) bool {
		return strings.Compare(ts[i].StreamTemplateConfig.Name, ts[j].StreamTemplateConfig.Name) < 0
	})

	for _, t := range ts[offset:] {
		t.mu.Lock()
		name := t.Name
		t.mu.Unlock()
		resp.Templates = append(resp.Templates, name)
		if len(resp.Templates) >= JSApiNamesLimit {
			break
		}
	}
	resp.Total = len(ts)
	resp.Limit = JSApiNamesLimit
	resp.Offset = offset
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for information about a stream template.
func (s *Server) jsTemplateInfoRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiStreamTemplateInfoResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsBadRequestErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	name := templateNameFromSubject(subject)
	t, err := c.acc.LookupStreamTemplate(name)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	t.mu.Lock()
	cfg := t.StreamTemplateConfig.deepCopy()
	streams := t.streams
	t.mu.Unlock()

	resp.StreamTemplateInfo = &StreamTemplateInfo{Config: cfg, Streams: streams}
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to delete a stream template.
func (s *Server) jsTemplateDeleteRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiStreamTemplateDeleteResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsBadRequestErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	name := templateNameFromSubject(subject)
	err := c.acc.DeleteStreamTemplate(name)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.Success = true
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

func (s *Server) jsonResponse(v interface{}) string {
	b, err := json.MarshalIndent(v, "", "  ")
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
func (s *Server) jsStreamCreateRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}

	var resp JSApiStreamCreateResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	var cfg StreamConfig
	if err := json.Unmarshal(msg, &cfg); err != nil {
		resp.Error = jsBadRequestErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	streamName := streamNameFromSubject(subject)
	if streamName != cfg.Name {
		resp.Error = &ApiError{Code: 400, Description: "stream name in subject does not match request"}
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	mset, err := c.acc.AddStream(&cfg)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.StreamInfo = &StreamInfo{Created: mset.Created(), State: mset.State(), Config: mset.Config()}
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to update a stream.
func (s *Server) jsStreamUpdateRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiStreamUpdateResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	var cfg StreamConfig
	if err := json.Unmarshal(msg, &cfg); err != nil {
		resp.Error = jsBadRequestErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	streamName := streamNameFromSubject(subject)
	if streamName != cfg.Name {
		resp.Error = &ApiError{Code: 400, Description: "stream name in subject does not match request"}
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	mset, err := c.acc.LookupStream(streamName)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
	}

	if err := mset.Update(&cfg); err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	resp.StreamInfo = &StreamInfo{Created: mset.Created(), State: mset.State(), Config: mset.Config()}
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for the list of all stream names.
func (s *Server) jsStreamNamesRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiStreamsResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	var offset int
	if !isEmptyRequest(msg) {
		var req JSApiStreamsRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			resp.Error = jsBadRequestErr
			s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		offset = req.Offset
	}

	// TODO(dlc) - Maybe hold these results for large results that we expect to be paged.
	// TODO(dlc) - If this list is long maybe do this in a Go routine?
	msets := c.acc.Streams()
	sort.Slice(msets, func(i, j int) bool {
		return strings.Compare(msets[i].config.Name, msets[j].config.Name) < 0
	})

	for _, mset := range msets[offset:] {
		resp.Streams = append(resp.Streams, mset.config.Name)
		if len(resp.Streams) >= JSApiNamesLimit {
			break
		}
	}
	resp.Total = len(msets)
	resp.Limit = JSApiNamesLimit
	resp.Offset = offset
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for the list of all detailed stream info.
// TODO(dlc) - combine with above long term
func (s *Server) jsStreamListRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiStreamListResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	var offset int
	if !isEmptyRequest(msg) {
		var req JSApiStreamsRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			resp.Error = jsBadRequestErr
			s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		offset = req.Offset
	}

	// TODO(dlc) - Maybe hold these results for large results that we expect to be paged.
	// TODO(dlc) - If this list is long maybe do this in a Go routine?
	msets := c.acc.Streams()
	sort.Slice(msets, func(i, j int) bool {
		return strings.Compare(msets[i].config.Name, msets[j].config.Name) < 0
	})

	for _, mset := range msets[offset:] {
		resp.Streams = append(resp.Streams, &StreamInfo{Created: mset.Created(), State: mset.State(), Config: mset.Config()})
		if len(resp.Streams) >= JSApiListLimit {
			break
		}
	}
	resp.Total = len(msets)
	resp.Limit = JSApiListLimit
	resp.Offset = offset
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for information about a stream.
func (s *Server) jsStreamInfoRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiStreamInfoResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsBadRequestErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	name := streamNameFromSubject(subject)
	mset, err := c.acc.LookupStream(name)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.StreamInfo = &StreamInfo{Created: mset.Created(), State: mset.State(), Config: mset.Config()}
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
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
func (s *Server) jsStreamDeleteRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiStreamDeleteResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsBadRequestErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	stream := streamNameFromSubject(subject)
	mset, err := c.acc.LookupStream(stream)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if err := mset.Delete(); err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.Success = true
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to delete a message.
// This expects a stream sequence number as the msg body.
func (s *Server) jsMsgDeleteRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiMsgDeleteResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if len(msg) == 0 {
		resp.Error = jsBadRequestErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	var req JSApiMsgDeleteRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		resp.Error = jsBadRequestErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	stream := tokenAt(subject, 6)
	mset, err := c.acc.LookupStream(stream)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	removed, err := mset.EraseMsg(req.Seq)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	} else if !removed {
		resp.Error = &ApiError{Code: 400, Description: fmt.Sprintf("sequence [%d] not found", req.Seq)}
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.Success = true
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to get a raw stream message.
func (s *Server) jsMsgGetRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiMsgGetResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if len(msg) == 0 {
		resp.Error = jsBadRequestErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	var req JSApiMsgGetRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		resp.Error = jsBadRequestErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	stream := tokenAt(subject, 6)
	mset, err := c.acc.LookupStream(stream)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	subj, msg, ts, err := mset.store.LoadMsg(req.Seq)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.Message = &StoredMsg{
		Subject:  subj,
		Sequence: req.Seq,
		Data:     msg,
		Time:     time.Unix(0, ts),
	}
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to purge a stream.
func (s *Server) jsStreamPurgeRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiStreamPurgeResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsBadRequestErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	stream := streamNameFromSubject(subject)
	mset, err := c.acc.LookupStream(stream)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.Purged = mset.Purge()
	resp.Success = true
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to create a durable consumer.
func (s *Server) jsDurableCreateRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	s.jsConsumerCreate(sub, c, subject, reply, msg, true)
}

// Request to create a consumer.
func (s *Server) jsConsumerCreateRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	s.jsConsumerCreate(sub, c, subject, reply, msg, false)
}

func (s *Server) jsConsumerCreate(sub *subscription, c *client, subject, reply string, msg []byte, expectDurable bool) {
	if c == nil || c.acc == nil {
		return
	}

	var resp JSApiConsumerCreateResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	var req CreateConsumerRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		resp.Error = jsBadRequestErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	var streamName string
	if expectDurable {
		streamName = tokenAt(subject, 6)
	} else {
		streamName = tokenAt(subject, 5)
	}
	if streamName != req.Stream {
		resp.Error = &ApiError{Code: 400, Description: "stream name in subject does not match request"}
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	stream, err := c.acc.LookupStream(req.Stream)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	if expectDurable {
		if numTokens(subject) != 7 {
			resp.Error = &ApiError{Code: 400, Description: "consumer expected to be durable but no durable name set in subject"}
			s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		// Now check on requirements for durable request.
		if req.Config.Durable == _EMPTY_ {
			resp.Error = &ApiError{Code: 400, Description: "consumer expected to be durable but a durable name was not set"}
			s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		consumerName := tokenAt(subject, 7)
		if consumerName != req.Config.Durable {
			resp.Error = &ApiError{Code: 400, Description: "consumer name in subject does not match durable name in request"}
			s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
	} else {
		if numTokens(subject) != 5 {
			resp.Error = &ApiError{Code: 400, Description: "consumer expected to be ephemeral but detected a durable name set in subject"}
			s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		if req.Config.Durable != _EMPTY_ {
			resp.Error = &ApiError{Code: 400, Description: "consumer expected to be ephemeral but a durable name was set in request"}
			s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
	}

	o, err := stream.AddConsumer(&req.Config)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.ConsumerInfo = o.Info()
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for the list of all consumer names.
func (s *Server) jsConsumerNamesRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiConsumersResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	var offset int
	if !isEmptyRequest(msg) {
		var req JSApiConsumersRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			resp.Error = jsBadRequestErr
			s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		offset = req.Offset
	}

	streamName := streamNameFromSubject(subject)
	mset, err := c.acc.LookupStream(streamName)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	obs := mset.Consumers()
	sort.Slice(obs, func(i, j int) bool {
		return strings.Compare(obs[i].name, obs[j].name) < 0
	})
	for _, o := range obs[offset:] {
		resp.Consumers = append(resp.Consumers, o.Name())
		if len(resp.Consumers) >= JSApiNamesLimit {
			break
		}
	}
	resp.Total = len(obs)
	resp.Limit = JSApiNamesLimit
	resp.Offset = offset
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for the list of all detailed consumer information.
func (s *Server) jsConsumerListRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiConsumerListResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	var offset int
	if !isEmptyRequest(msg) {
		var req JSApiConsumersRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			resp.Error = jsBadRequestErr
			s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		offset = req.Offset
	}

	streamName := streamNameFromSubject(subject)
	mset, err := c.acc.LookupStream(streamName)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	obs := mset.Consumers()
	sort.Slice(obs, func(i, j int) bool {
		return strings.Compare(obs[i].name, obs[j].name) < 0
	})
	for _, o := range obs[offset:] {
		resp.Consumers = append(resp.Consumers, o.Info())
		if len(resp.Consumers) >= JSApiListLimit {
			break
		}
	}
	resp.Total = len(obs)
	resp.Limit = JSApiListLimit
	resp.Offset = offset
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for information about an consumer.
func (s *Server) jsConsumerInfoRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiConsumerInfoResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsBadRequestErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	stream := streamNameFromSubject(subject)
	mset, err := c.acc.LookupStream(stream)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	consumer := consumerNameFromSubject(subject)
	obs := mset.LookupConsumer(consumer)
	if obs == nil {
		resp.Error = &ApiError{Code: 404, Description: "consumer not found"}
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.ConsumerInfo = obs.Info()
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to delete an Consumer.
func (s *Server) jsConsumerDeleteRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiConsumerDeleteResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if !isEmptyRequest(msg) {
		resp.Error = jsBadRequestErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	stream := streamNameFromSubject(subject)
	mset, err := c.acc.LookupStream(stream)
	if err != nil {
		resp.Error = jsNotFoundError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	consumer := consumerNameFromSubject(subject)
	obs := mset.LookupConsumer(consumer)
	if obs == nil {
		resp.Error = &ApiError{Code: 404, Description: "consumer not found"}
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if err := obs.Delete(); err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.Success = true
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// For delivering advisories for API calls.

// ClientAPIAudit is for identifying a client who initiated an API call to the system.
type ClientAPIAudit struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	CID      uint64 `json:"cid"`
	Account  string `json:"account"`
	User     string `json:"user,omitempty"`
	Name     string `json:"name,omitempty"`
	Language string `json:"lang,omitempty"`
	Version  string `json:"version,omitempty"`
}

// JetStreamAPIAudit is an advisory about administrative actions taken on JetStream
type JetStreamAPIAudit struct {
	TypedEvent
	Server   string         `json:"server"`
	Client   ClientAPIAudit `json:"client"`
	Subject  string         `json:"subject"`
	Request  string         `json:"request,omitempty"`
	Response string         `json:"response"`
}

const auditType = "io.nats.jetstream.advisory.v1.api_audit"

// sendJetStreamAPIAuditAdvisor will send the audit event for a given event.
func (s *Server) sendJetStreamAPIAuditAdvisory(c *client, subject, request, response string) {
	c.mu.Lock()
	auditUser := c.auditUser()
	h, p := c.auditClient()
	appName := c.opts.Name
	lang := c.opts.Lang
	version := c.opts.Version
	cid := c.cid
	c.mu.Unlock()

	e := &JetStreamAPIAudit{
		TypedEvent: TypedEvent{
			Type: auditType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Server: s.Name(),
		Client: ClientAPIAudit{
			Host:     h,
			Port:     p,
			CID:      cid,
			Account:  c.Account().Name,
			User:     auditUser,
			Name:     appName,
			Language: lang,
			Version:  version,
		},
		Subject:  subject,
		Request:  request,
		Response: response,
	}

	ej, err := json.MarshalIndent(e, "", "  ")
	if err == nil {
		s.sendInternalAccountMsg(c.acc, JSAuditAdvisory, ej)
	} else {
		s.Warnf("JetStream could not marshal audit event for account %q: %v", c.acc.Name, err)
	}
}

// Returns a string identifying the user for an audit event. Returns empty string when no user is present.
func (c *client) auditUser() string {
	switch {
	case c.opts.Nkey != "":
		return c.opts.Nkey
	case c.opts.Username != "":
		return c.opts.Username
	default:
		return ""
	}
}

// Returns the audit client name, which will just be IP:Port
func (c *client) auditClient() (string, int) {
	parts := strings.Split(c.ncs, " ")
	h, p, _ := net.SplitHostPort(parts[0])
	port, _ := strconv.Atoi(p)
	return h, port
}
