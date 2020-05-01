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
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nuid"
)

// Request API subjects for JetStream.
const (
	// JetStreamEnabled allows a user to dynamically check if JetStream is enabled for an account.
	// Will return +OK on success, otherwise will timeout.
	JetStreamEnabled = "$JS.ENABLED"

	// JetStreamInfo is for obtaining general information about JetStream for this account.
	// Will return JSON response.
	JetStreamInfo = "$JS.INFO"

	// JetStreamCreateTemplate is the endpoint to create new stream templates.
	// Will return +OK on success and -ERR on failure.
	JetStreamCreateTemplate  = "$JS.TEMPLATE.*.CREATE"
	JetStreamCreateTemplateT = "$JS.TEMPLATE.%s.CREATE"

	// JetStreamListTemplates is the endpoint to list all stream templates for this account.
	// Will return json list of string on success and -ERR on failure.
	JetStreamListTemplates = "$JS.TEMPLATE.LIST"

	// JetStreamTemplateInfo is for obtaining general information about a named stream template.
	// Will return JSON response.
	JetStreamTemplateInfo  = "$JS.TEMPLATE.*.INFO"
	JetStreamTemplateInfoT = "$JS.TEMPLATE.%s.INFO"

	// JetStreamDeleteTemplate is the endpoint to delete stream templates.
	// Will return +OK on success and -ERR on failure.
	JetStreamDeleteTemplate  = "$JS.TEMPLATE.*.DELETE"
	JetStreamDeleteTemplateT = "$JS.TEMPLATE.%s.DELETE"

	// JetStreamCreateStream is the endpoint to create new streams.
	// Will return +OK on success and -ERR on failure.
	JetStreamCreateStream  = "$JS.STREAM.*.CREATE"
	JetStreamCreateStreamT = "$JS.STREAM.%s.CREATE"

	// JetStreamUpdateStream is the endpoint to update existing streams.
	// Will return +OK on success and -ERR on failure.
	JetStreamUpdateStream  = "$JS.STREAM.*.UPDATE"
	JetStreamUpdateStreamT = "$JS.STREAM.%s.UPDATE"

	// JetStreamListStreams is the endpoint to list all streams for this account.
	// Will return json list of string on success and -ERR on failure.
	JetStreamListStreams = "$JS.STREAM.LIST"

	// JetStreamStreamInfo is for obtaining general information about a named stream.
	// Will return JSON response.
	JetStreamStreamInfo  = "$JS.STREAM.*.INFO"
	JetStreamStreamInfoT = "$JS.STREAM.%s.INFO"

	// JetStreamDeleteStream is the endpoint to delete streams.
	// Will return +OK on success and -ERR on failure.
	JetStreamDeleteStream  = "$JS.STREAM.*.DELETE"
	JetStreamDeleteStreamT = "$JS.STREAM.%s.DELETE"

	// JetStreamPurgeStream is the endpoint to purge streams.
	// Will return +OK on success and -ERR on failure.
	JetStreamPurgeStream  = "$JS.STREAM.*.PURGE"
	JetStreamPurgeStreamT = "$JS.STREAM.%s.PURGE"

	// JetStreamDeleteMsg is the endpoint to delete messages from a stream.
	// Will return +OK on success and -ERR on failure.
	JetStreamDeleteMsg  = "$JS.STREAM.*.MSG.DELETE"
	JetStreamDeleteMsgT = "$JS.STREAM.%s.MSG.DELETE"

	// JetStreamCreateConsumer is the endpoint to create durable consumers for streams.
	// You need to include the stream and consumer name in the subject.
	// Will return +OK on success and -ERR on failure.
	JetStreamCreateConsumer  = "$JS.STREAM.*.CONSUMER.*.CREATE"
	JetStreamCreateConsumerT = "$JS.STREAM.%s.CONSUMER.%s.CREATE"

	// JetStreamCreateEphemeralConsumer is the endpoint to create ephemeral consumers for streams.
	// Will return +OK <consumer name> on success and -ERR on failure.
	JetStreamCreateEphemeralConsumer  = "$JS.STREAM.*.EPHEMERAL.CONSUMER.CREATE"
	JetStreamCreateEphemeralConsumerT = "$JS.STREAM.%s.EPHEMERAL.CONSUMER.CREATE"

	// JetStreamConsumers is the endpoint to list all consumers for the stream.
	// Will return json list of string on success and -ERR on failure.
	JetStreamConsumers  = "$JS.STREAM.*.CONSUMERS"
	JetStreamConsumersT = "$JS.STREAM.%s.CONSUMERS"

	// JetStreamConsumerInfo is for obtaining general information about a consumer.
	// Will return JSON response.
	JetStreamConsumerInfo  = "$JS.STREAM.*.CONSUMER.*.INFO"
	JetStreamConsumerInfoT = "$JS.STREAM.%s.CONSUMER.%s.INFO"

	// JetStreamDeleteConsumer is the endpoint to delete consumers.
	// Will return +OK on success and -ERR on failure.
	JetStreamDeleteConsumer  = "$JS.STREAM.*.CONSUMER.*.DELETE"
	JetStreamDeleteConsumerT = "$JS.STREAM.%s.CONSUMER.%s.DELETE"

	// JetStreamAckT is the template for the ack message stream coming back from an consumer
	// when they ACK/NAK, etc a message.
	JetStreamAckT   = "$JS.ACK.%s.%s"
	jetStreamAckPre = "$JS.ACK."

	// JetStreamRequestNextT is the prefix for the request next message(s) for a consumer in worker/pull mode.
	JetStreamRequestNextT = "$JS.STREAM.%s.CONSUMER.%s.NEXT"

	// JetStreamMsgBySeqT is the template for direct requests for a message by its stream sequence number.
	JetStreamMsgBySeqT = "$JS.STREAM.%s.MSG.BYSEQ"

	// JetStreamAdvisoryPrefix is a prefix for all JetStream advisories.
	JetStreamAdvisoryPrefix = "$JS.EVENT.ADVISORY"

	// JetStreamMetricPrefix is a prefix for all JetStream metrics.
	JetStreamMetricPrefix = "$JS.EVENT.METRIC"

	// JetStreamMetricConsumerAckPre is a metric containing ack latency.
	JetStreamMetricConsumerAckPre = JetStreamMetricPrefix + ".CONSUMER_ACK"

	// JetStreamAdvisoryConsumerMaxDeliveryExceedPre is a notification published when a message exceeds its delivery threshold.
	JetStreamAdvisoryConsumerMaxDeliveryExceedPre = JetStreamAdvisoryPrefix + ".MAX_DELIVERIES"

	// JetStreamAPIAuditAdvisory is a notification about JetStream API access.
	JetStreamAPIAuditAdvisory = JetStreamAdvisoryPrefix + ".API"
)

// Responses for API calls.

// ApiError is included in all responses if there was an error.
// TODO(dlc) - Move to more generic location.
type ApiError struct {
	Code        int    `json:"code"`
	Description string `json:"description,omitempty"`
}

// JSEnabledResponse is the response to see if JetStream is enabled for this account.
type JSApiEnabledResponse struct {
	Error   *ApiError `json:"error,omitempty"`
	Enabled bool      `json:"enabled"`
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

// JSApiStreamListResponse list of streams.
type JSApiStreamListResponse struct {
	Error   *ApiError `json:"error,omitempty"`
	Streams []string  `json:"streams,omitempty"`
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

// JSApiConsumerListResponse.
type JSApiConsumerListResponse struct {
	Error     *ApiError `json:"error,omitempty"`
	Consumers []string  `json:"streams,omitempty"`
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

// JSApiStreamTemplateListResponse list of templates.
type JSApiStreamTemplateListResponse struct {
	Error     *ApiError `json:"error,omitempty"`
	Templates []string  `json:"streams,omitempty"`
}

var (
	jsNotEnabledErr = &ApiError{Code: 503, Description: "jetstream not enabled for account"}
	jsBadRequestErr = &ApiError{Code: 400, Description: "bad request"}
)

// For easier handling of exports and imports.
var allJsExports = []string{
	JetStreamEnabled,
	JetStreamInfo,
	JetStreamCreateTemplate,
	JetStreamListTemplates,
	JetStreamTemplateInfo,
	JetStreamDeleteTemplate,
	JetStreamCreateStream,
	JetStreamUpdateStream,
	JetStreamListStreams,
	JetStreamStreamInfo,
	JetStreamDeleteStream,
	JetStreamPurgeStream,
	JetStreamDeleteMsg,
	JetStreamCreateConsumer,
	JetStreamCreateEphemeralConsumer,
	JetStreamConsumers,
	JetStreamConsumerInfo,
	JetStreamDeleteConsumer,
}

func (s *Server) setJetStreamExportSubs() error {
	pairs := []struct {
		subject string
		handler msgHandler
	}{
		{JetStreamEnabled, s.isJsEnabledRequest},
		{JetStreamInfo, s.jsAccountInfoRequest},
		{JetStreamCreateTemplate, s.jsCreateTemplateRequest},
		{JetStreamListTemplates, s.jsTemplateListRequest},
		{JetStreamTemplateInfo, s.jsTemplateInfoRequest},
		{JetStreamDeleteTemplate, s.jsTemplateDeleteRequest},
		{JetStreamCreateStream, s.jsCreateStreamRequest},
		{JetStreamUpdateStream, s.jsStreamUpdateRequest},
		{JetStreamListStreams, s.jsStreamListRequest},
		{JetStreamStreamInfo, s.jsStreamInfoRequest},
		{JetStreamDeleteStream, s.jsStreamDeleteRequest},
		{JetStreamPurgeStream, s.jsStreamPurgeRequest},
		{JetStreamDeleteMsg, s.jsMsgDeleteRequest},
		{JetStreamCreateConsumer, s.jsCreateConsumerRequest},
		{JetStreamCreateEphemeralConsumer, s.jsCreateEphemeralConsumerRequest},
		{JetStreamConsumers, s.jsConsumersRequest},
		{JetStreamConsumerInfo, s.jsConsumerInfoRequest},
		{JetStreamDeleteConsumer, s.jsConsumerDeleteRequest},
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

// Request to check if jetstream is enabled.
func (s *Server) isJsEnabledRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	b, _ := json.MarshalIndent(&JSApiEnabledResponse{Enabled: c.acc.JetStreamEnabled()}, "", "  ")
	s.sendAPIResponse(c, subject, reply, string(msg), string(b))
}

type JSApiAccountInfo struct {
	Error *ApiError `json:"error,omitempty"`
	*JetStreamAccountStats
}

// Request for current usage and limits for this account.
func (s *Server) jsAccountInfoRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiAccountInfo
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

// Request to create a new template.
func (s *Server) jsCreateTemplateRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
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
	templateName := subjectToken(subject, 2)
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

// Request for the list of all templates.
func (s *Server) jsTemplateListRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiStreamTemplateListResponse
	if !c.acc.JetStreamEnabled() {
		resp.Error = jsNotEnabledErr
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	ts := c.acc.Templates()
	for _, t := range ts {
		t.mu.Lock()
		name := t.Name
		t.mu.Unlock()
		resp.Templates = append(resp.Templates, name)
	}
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
	name := subjectToken(subject, 2)
	t, err := c.acc.LookupStreamTemplate(name)
	if err != nil {
		resp.Error = jsError(err)
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
	name := subjectToken(subject, 2)
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

// Request to create a stream.
func (s *Server) jsCreateStreamRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
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
	streamName := subjectToken(subject, 2)
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
	resp.StreamInfo = &StreamInfo{State: mset.State(), Config: mset.Config()}
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
	streamName := subjectToken(subject, 2)
	if streamName != cfg.Name {
		resp.Error = &ApiError{Code: 400, Description: "stream name in subject does not match request"}
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	mset, err := c.acc.LookupStream(streamName)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
	}

	if err := mset.Update(&cfg); err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	resp.StreamInfo = &StreamInfo{State: mset.State(), Config: mset.Config()}
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request for the list of all streams.
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
	msets := c.acc.Streams()
	for _, mset := range msets {
		resp.Streams = append(resp.Streams, mset.Name())
	}
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
	name := subjectToken(subject, 2)
	mset, err := c.acc.LookupStream(name)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.StreamInfo = &StreamInfo{State: mset.State(), Config: mset.Config()}
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
	stream := subjectToken(subject, 2)
	mset, err := c.acc.LookupStream(stream)
	if err != nil {
		resp.Error = jsError(err)
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

	stream := subjectToken(subject, 2)
	mset, err := c.acc.LookupStream(stream)
	if err != nil {
		resp.Error = jsError(err)
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
	stream := subjectToken(subject, 2)
	mset, err := c.acc.LookupStream(stream)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	resp.Purged = mset.Purge()
	resp.Success = true
	s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(resp))
}

// Request to create a durable consumer.
func (s *Server) jsCreateConsumerRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
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
	streamName := subjectToken(subject, 2)
	if streamName != req.Stream {
		resp.Error = &ApiError{Code: 400, Description: "stream name in subject does not match request"}
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	stream, err := c.acc.LookupStream(req.Stream)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	// Now check we do not have a durable.
	if req.Config.Durable == _EMPTY_ {
		resp.Error = &ApiError{Code: 400, Description: "consumer expected to be durable but a durable name was not set"}
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	consumerName := subjectToken(subject, 4)
	if consumerName != req.Config.Durable {
		resp.Error = &ApiError{Code: 400, Description: "consumer name in subject does not match durable name in request"}
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
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

// Request to create an ephemeral consumer.
func (s *Server) jsCreateEphemeralConsumerRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
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
	streamName := subjectToken(subject, 2)
	if streamName != req.Stream {
		resp.Error = &ApiError{Code: 400, Description: "stream name in subject does not match request"}
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	stream, err := c.acc.LookupStream(req.Stream)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	// Now check we do not have a durable.
	if req.Config.Durable != _EMPTY_ {
		resp.Error = &ApiError{Code: 400, Description: "consumer expected to be ephemeral but a durable name was set"}
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
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

// Request for the list of all consumers.
func (s *Server) jsConsumersRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	var resp JSApiConsumerListResponse
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
	name := subjectToken(subject, 2)
	mset, err := c.acc.LookupStream(name)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	obs := mset.Consumers()
	for _, o := range obs {
		resp.Consumers = append(resp.Consumers, o.Name())
	}
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

	stream := subjectToken(subject, 2)
	mset, err := c.acc.LookupStream(stream)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	consumer := subjectToken(subject, 4)
	obs := mset.LookupConsumer(consumer)
	if obs == nil {
		resp.Error = &ApiError{Code: 400, Description: "consumer not found"}
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
	stream := subjectToken(subject, 2)
	mset, err := c.acc.LookupStream(stream)
	if err != nil {
		resp.Error = jsError(err)
		s.sendAPIResponse(c, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	consumer := subjectToken(subject, 4)
	obs := mset.LookupConsumer(consumer)
	if obs == nil {
		resp.Error = &ApiError{Code: 400, Description: "consumer not found"}
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
	Type     string         `json:"type"`
	ID       string         `json:"id"`
	Time     string         `json:"timestamp"`
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
		Type:   auditType,
		ID:     nuid.Next(),
		Time:   time.Now().UTC().Format(time.RFC3339Nano),
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
		s.sendInternalAccountMsg(c.acc, JetStreamAPIAuditAdvisory, ej)
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
