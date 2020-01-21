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
	JetStreamListTemplates = "$JS.TEMPLATES.LIST"

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
	JetStreamAckT = "$JS.ACK.%s.%s"

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

// Responses to requests sent to a server from a client.
const (
	// OK response
	OK = "+OK"
	// ERR prefix response
	ErrPrefix = "-ERR"

	// JetStreamNotEnabled is returned when JetStream is not enabled.
	JetStreamNotEnabled = "-ERR 'jetstream not enabled for account'"
	// JetStreamBadRequest is returned when the request could not be properly parsed.
	JetStreamBadRequest = "-ERR 'bad request'"
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
	if c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), OK)
	} else {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
	}
}

// Request for current usage and limits for this account.
func (s *Server) jsAccountInfoRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	stats := c.acc.JetStreamUsage()
	b, err := json.MarshalIndent(stats, "", "  ")
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
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	var cfg StreamTemplateConfig
	if err := json.Unmarshal(msg, &cfg); err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamBadRequest)
		return
	}
	templateName := subjectToken(subject, 2)
	if templateName != cfg.Name {
		s.sendInternalAccountMsg(c.acc, reply, protoErr("template name in subject does not match request"))
		return
	}

	var response = OK
	if _, err := c.acc.AddStreamTemplate(&cfg); err != nil {
		response = protoErr(err)
	}
	s.sendAPIResponse(c, subject, reply, string(msg), response)
}

// Request for the list of all templates.
func (s *Server) jsTemplateListRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	var names []string
	ts := c.acc.Templates()
	for _, t := range ts {
		t.mu.Lock()
		name := t.Name
		t.mu.Unlock()
		names = append(names, name)
	}
	b, err := json.MarshalIndent(names, "", "  ")
	if err != nil {
		return
	}
	s.sendAPIResponse(c, subject, reply, string(msg), string(b))
}

// Request for information about a stream template.
func (s *Server) jsTemplateInfoRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	if len(msg) != 0 {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamBadRequest)
		return
	}
	name := subjectToken(subject, 2)
	t, err := c.acc.LookupStreamTemplate(name)
	if err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr(err))
		return
	}
	t.mu.Lock()
	cfg := t.StreamTemplateConfig.deepCopy()
	streams := t.streams
	t.mu.Unlock()
	si := &StreamTemplateInfo{
		Config:  cfg,
		Streams: streams,
	}
	b, err := json.MarshalIndent(si, "", "  ")
	if err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr(err))
		return
	}
	s.sendAPIResponse(c, subject, reply, string(msg), string(b))
}

// Request to delete a stream template.
func (s *Server) jsTemplateDeleteRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	if len(msg) != 0 {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamBadRequest)
		return
	}
	name := subjectToken(subject, 2)
	err := c.acc.DeleteStreamTemplate(name)
	if err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr(err))
		return
	}
	s.sendAPIResponse(c, subject, reply, string(msg), OK)
}

// Request to create a stream.
func (s *Server) jsCreateStreamRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	var cfg StreamConfig
	if err := json.Unmarshal(msg, &cfg); err != nil {
		s.sendInternalAccountMsg(c.acc, reply, JetStreamBadRequest)
		return
	}
	streamName := subjectToken(subject, 2)
	if streamName != cfg.Name {
		s.sendInternalAccountMsg(c.acc, reply, protoErr("stream name in subject does not match request"))
		return
	}

	var response = OK
	if _, err := c.acc.AddStream(&cfg); err != nil {
		response = protoErr(err)
	}
	s.sendAPIResponse(c, subject, reply, string(msg), response)
}

// Request for the list of all streams.
func (s *Server) jsStreamListRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	var names []string
	msets := c.acc.Streams()
	for _, mset := range msets {
		names = append(names, mset.Name())
	}
	b, err := json.MarshalIndent(names, "", "  ")
	if err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr(err))
		return
	}
	s.sendAPIResponse(c, subject, reply, string(msg), string(b))
}

// Request for information about a stream.
func (s *Server) jsStreamInfoRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	if len(msg) != 0 {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamBadRequest)
		return
	}
	name := subjectToken(subject, 2)
	mset, err := c.acc.LookupStream(name)
	if err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr(err))
		return
	}
	msi := StreamInfo{
		State:  mset.State(),
		Config: mset.Config(),
	}
	b, err := json.MarshalIndent(msi, "", "  ")
	if err != nil {
		return
	}
	s.sendAPIResponse(c, subject, reply, string(msg), string(b))
}

// Request to delete a stream.
func (s *Server) jsStreamDeleteRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	if len(msg) != 0 {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamBadRequest)
		return
	}
	name := subjectToken(subject, 2)
	mset, err := c.acc.LookupStream(name)
	if err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr(err))
		return
	}
	var response = OK
	if err := mset.Delete(); err != nil {
		response = protoErr(err)
	}
	s.sendAPIResponse(c, subject, reply, string(msg), response)
}

// Request to delete a message.
// This expects a stream sequence number as the msg body.
func (s *Server) jsMsgDeleteRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	if len(msg) == 0 {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamBadRequest)
		return
	}
	name := subjectToken(subject, 2)
	mset, err := c.acc.LookupStream(name)
	if err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr(err))
		return
	}
	var response = OK
	seq, _ := strconv.Atoi(string(msg))
	if !mset.EraseMsg(uint64(seq)) {
		response = protoErr(fmt.Sprintf("sequence [%d] not found", seq))
	}
	s.sendAPIResponse(c, subject, reply, string(msg), response)
}

// Request to purge a stream.
func (s *Server) jsStreamPurgeRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	if len(msg) != 0 {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamBadRequest)
		return
	}
	name := subjectToken(subject, 2)
	mset, err := c.acc.LookupStream(name)
	if err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr(err))
		return
	}

	mset.Purge()
	s.sendAPIResponse(c, subject, reply, string(msg), OK)
}

// Request to create a durable consumer.
func (s *Server) jsCreateConsumerRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	var req CreateConsumerRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamBadRequest)
		return
	}
	streamName := subjectToken(subject, 2)
	if streamName != req.Stream {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr("stream name in subject does not match request"))
		return
	}
	stream, err := c.acc.LookupStream(req.Stream)
	if err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr(err))
		return
	}
	// Now check we do not have a durable.
	if req.Config.Durable == _EMPTY_ {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr("consumer expected to be durable but a durable name was not set"))
		return
	}
	consumerName := subjectToken(subject, 4)
	if consumerName != req.Config.Durable {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr("consumer name in subject does not match durable name in request"))
		return
	}
	var response = OK
	if _, err := stream.AddConsumer(&req.Config); err != nil {
		response = protoErr(err)
	}
	s.sendAPIResponse(c, subject, reply, string(msg), response)
}

// Request to create an ephemeral consumer.
func (s *Server) jsCreateEphemeralConsumerRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	var req CreateConsumerRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamBadRequest)
		return
	}
	streamName := subjectToken(subject, 2)
	if streamName != req.Stream {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr("stream name in subject does not match request"))
		return
	}
	stream, err := c.acc.LookupStream(req.Stream)
	if err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr(err))
		return
	}
	// Now check we do not have a durable.
	if req.Config.Durable != _EMPTY_ {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr("consumer expected to be ephemeral but a durable name was set"))
		return
	}
	var response = OK
	if o, err := stream.AddConsumer(&req.Config); err != nil {
		response = protoErr(err)
	} else {
		// Add in the name since this one is ephemeral.
		response = OK + " " + o.Name()
	}
	s.sendAPIResponse(c, subject, reply, string(msg), response)
}

// Request for the list of all consumers.
func (s *Server) jsConsumersRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	if len(msg) != 0 {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamBadRequest)
		return
	}
	name := subjectToken(subject, 2)
	mset, err := c.acc.LookupStream(name)
	if err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr(err))
		return
	}
	var onames []string
	obs := mset.Consumers()
	for _, o := range obs {
		onames = append(onames, o.Name())
	}
	b, err := json.MarshalIndent(onames, "", "  ")
	if err != nil {
		return
	}
	s.sendAPIResponse(c, subject, reply, string(msg), string(b))
}

// Request for information about an consumer.
func (s *Server) jsConsumerInfoRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	if len(msg) != 0 {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamBadRequest)
		return
	}
	stream := subjectToken(subject, 2)
	mset, err := c.acc.LookupStream(stream)
	if err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr(err))
		return
	}
	consumer := subjectToken(subject, 4)
	obs := mset.LookupConsumer(consumer)
	if obs == nil {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr("consumer not found"))
		return
	}
	info := obs.Info()
	b, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return
	}
	s.sendAPIResponse(c, subject, reply, string(msg), string(b))
}

// Request to delete an Consumer.
func (s *Server) jsConsumerDeleteRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c == nil || c.acc == nil {
		return
	}
	if !c.acc.JetStreamEnabled() {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamNotEnabled)
		return
	}
	if len(msg) != 0 {
		s.sendAPIResponse(c, subject, reply, string(msg), JetStreamBadRequest)
		return
	}
	stream := subjectToken(subject, 2)
	mset, err := c.acc.LookupStream(stream)
	if err != nil {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr(err))
		return
	}
	consumer := subjectToken(subject, 4)
	obs := mset.LookupConsumer(consumer)
	if obs == nil {
		s.sendAPIResponse(c, subject, reply, string(msg), protoErr("consumer not found"))
		return
	}
	var response = OK
	if err := obs.Delete(); err != nil {
		response = protoErr(err)
	}
	s.sendAPIResponse(c, subject, reply, string(msg), response)
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
	Schema   string         `json:"schema"`
	ID       string         `json:"id"`
	Time     time.Time      `json:"time"`
	Server   string         `json:"server"`
	Client   ClientAPIAudit `json:"client"`
	Subject  string         `json:"subject"`
	Request  string         `json:"request,omitempty"`
	Response string         `json:"response"`
}

const auditSchema = "io.nats.jetstream.advisory.v1.api_audit"

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
		Schema: auditSchema,
		ID:     nuid.Next(),
		Time:   time.Now(),
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
