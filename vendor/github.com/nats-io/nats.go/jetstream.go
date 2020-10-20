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

package nats

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// JetStreamMsgMetaData is metadata related to a JetStream originated message
type JetStreamMsgMetaData struct {
	Stream      string
	Consumer    string
	Parsed      bool
	Delivered   int
	StreamSeq   int
	ConsumerSeq int
	TimeStamp   time.Time
}

func (m *Msg) JetStreamMetaData() (*JetStreamMsgMetaData, error) {
	var err error

	if m.jsMeta != nil && m.jsMeta.Parsed {
		return m.jsMeta, nil
	}

	m.jsMeta, err = m.parseJSMsgMetadata()

	return m.jsMeta, err
}

func (m *Msg) parseJSMsgMetadata() (*JetStreamMsgMetaData, error) {
	if m.jsMeta != nil {
		return m.jsMeta, nil
	}

	if len(m.Reply) == 0 {
		return nil, ErrNotJSMessage
	}

	meta := &JetStreamMsgMetaData{}

	tsa := [32]string{}
	parts := tsa[:0]
	start := 0
	btsep := byte('.')
	for i := 0; i < len(m.Reply); i++ {
		if m.Reply[i] == btsep {
			parts = append(parts, m.Reply[start:i])
			start = i + 1
		}
	}
	parts = append(parts, m.Reply[start:])

	if len(parts) != 8 || parts[0] != "$JS" || parts[1] != "ACK" {
		return nil, ErrNotJSMessage
	}

	var err error

	meta.Stream = parts[2]
	meta.Consumer = parts[3]
	meta.Delivered, err = strconv.Atoi(parts[4])
	if err != nil {
		return nil, ErrNotJSMessage
	}

	meta.StreamSeq, err = strconv.Atoi(parts[5])
	if err != nil {
		return nil, ErrNotJSMessage
	}

	meta.ConsumerSeq, err = strconv.Atoi(parts[6])
	if err != nil {
		return nil, ErrNotJSMessage
	}

	tsi, err := strconv.Atoi(parts[7])
	if err != nil {
		return nil, ErrNotJSMessage
	}
	meta.TimeStamp = time.Unix(0, int64(tsi))

	meta.Parsed = true

	return meta, nil
}

const jsStreamUnspecified = "not.set"

type jsOpts struct {
	timeout time.Duration
	ctx     context.Context

	ackstr     string
	consumer   *ConsumerConfig
	streamName string
}

func newJsOpts() *jsOpts {
	return &jsOpts{ackstr: jsStreamUnspecified, consumer: &ConsumerConfig{}}
}

func (j *jsOpts) context(dftl time.Duration) (context.Context, context.CancelFunc) {
	if j.ctx != nil {
		return context.WithCancel(j.ctx)
	}

	if j.timeout == 0 {
		j.timeout = dftl
	}

	return context.WithTimeout(context.Background(), j.timeout)
}

// AckOption configures the various JetStream message acknowledgement helpers
type AckOption func(opts *jsOpts) error

// PublishOption configures publishing messages
type PublishOption func(opts *jsOpts) error

// SubscribeOption configures JetStream consumer behavior
type SubscribeOption func(opts *jsOpts) error

// Consumer creates a JetStream Consumer on a Stream
func Consumer(stream string, cfg ConsumerConfig) SubscribeOption {
	return func(jopts *jsOpts) error {
		jopts.consumer = &cfg
		jopts.streamName = stream
		return nil
	}
}

// PublishExpectsStream waits for an ack after publishing and ensure it's from a specific stream, empty arguments waits for any valid acknowledgement
func PublishExpectsStream(stream ...string) PublishOption {
	return func(opts *jsOpts) error {
		switch len(stream) {
		case 0:
			opts.ackstr = ""
		case 1:
			opts.ackstr = stream[0]
			if !isValidJSName(opts.ackstr) {
				return ErrInvalidStreamName
			}
		default:
			return ErrMultiStreamUnsupported
		}

		return nil
	}
}

// PublishStreamTimeout sets the period of time to wait for JetStream to acknowledge receipt, defaults to JetStreamTimeout option
func PublishStreamTimeout(t time.Duration) PublishOption {
	return func(opts *jsOpts) error {
		opts.timeout = t
		return nil
	}
}

// PublishCtx sets an interrupt context for waiting on a stream to reply
func PublishCtx(ctx context.Context) PublishOption {
	return func(opts *jsOpts) error {
		opts.ctx = ctx
		return nil
	}
}

// AckWaitDuration waits for confirmation from the JetStream server
func AckWaitDuration(d time.Duration) AckOption {
	return func(opts *jsOpts) error {
		opts.timeout = d
		return nil
	}
}

func (m *Msg) jsAck(body []byte, opts ...AckOption) error {
	if m.Reply == "" {
		return ErrMsgNoReply
	}

	if m == nil || m.Sub == nil {
		return ErrMsgNotBound
	}

	m.Sub.mu.Lock()
	nc := m.Sub.conn
	m.Sub.mu.Unlock()

	var err error
	var aopts *jsOpts

	if len(opts) > 0 {
		aopts = newJsOpts()
		for _, f := range opts {
			if err = f(aopts); err != nil {
				return err
			}
		}
	}

	if aopts == nil || aopts.timeout == 0 {
		return m.Respond(body)
	}

	_, err = nc.Request(m.Reply, body, aopts.timeout)

	return err
}

// Ack acknowledges a JetStream messages received from a Consumer, indicating the message
// should not be received again later
func (m *Msg) Ack(opts ...AckOption) error {
	return m.jsAck(AckAck, opts...)
}

// Nak acknowledges a JetStream message received from a Consumer, indicating that the message
// is not completely processed and should be sent again later
func (m *Msg) Nak(opts ...AckOption) error {
	return m.jsAck(AckNak, opts...)
}

// AckProgress acknowledges a Jetstream message received from a Consumer, indicating that work is
// ongoing and further processing time is required equal to the configured AckWait of the Consumer
func (m *Msg) AckProgress(opts ...AckOption) error {
	return m.jsAck(AckProgress, opts...)
}

// AckNext performs an Ack() and request that the next message be sent to subject ib
func (m *Msg) AckNext(ib string) error {
	return m.RespondMsg(&Msg{Subject: m.Reply, Reply: ib, Data: AckNext})
}

// AckAndFetch performs an AckNext() and returns the next message from the stream
func (m *Msg) AckAndFetch(opts ...AckOption) (*Msg, error) {
	if m.Reply == "" {
		return nil, ErrMsgNoReply
	}

	if m == nil || m.Sub == nil {
		return nil, ErrMsgNotBound
	}

	m.Sub.mu.Lock()
	nc := m.Sub.conn
	m.Sub.mu.Unlock()

	var err error

	aopts := newJsOpts()
	for _, f := range opts {
		if err = f(aopts); err != nil {
			return nil, err
		}
	}

	ctx, cancel := aopts.context(nc.Opts.JetStreamTimeout)
	defer cancel()

	sub, err := nc.SubscribeSync(NewInbox())
	if err != nil {
		return nil, err
	}
	sub.AutoUnsubscribe(1)
	defer sub.Unsubscribe()

	err = m.RespondMsg(&Msg{Reply: sub.Subject, Data: AckNext, Subject: m.Reply})
	if err != nil {
		return nil, err
	}
	nc.Flush()

	return sub.NextMsgWithContext(ctx)
}

// AckTerm acknowledges a message received from JetStream indicating the message will not be processed
// and should not be sent to another consumer
func (m *Msg) AckTerm(opts ...AckOption) error {
	return m.jsAck(AckTerm, opts...)
}

// JetStreamPublishAck metadata received from JetStream when publishing messages
type JetStreamPublishAck struct {
	Stream   string `json:"stream"`
	Sequence int    `json:"seq"`
}

// ParsePublishAck parses the publish acknowledgement sent by JetStream
func ParsePublishAck(m []byte) (*JetStreamPublishAck, error) {
	if bytes.HasPrefix([]byte("-ERR"), m) {
		if len(m) > 7 {
			return nil, fmt.Errorf(string(m[6 : len(m)-1]))
		}

		return nil, fmt.Errorf(string(m))
	}

	if !bytes.HasPrefix(m, []byte("+OK {")) {
		return nil, fmt.Errorf("invalid JetStream Ack: %v", string(m))
	}

	ack := &JetStreamPublishAck{}
	err := json.Unmarshal(m[3:], ack)
	return ack, err
}

func (nc *Conn) jsPublish(subj string, data []byte, opts []PublishOption) error {
	var err error
	var aopts *jsOpts

	if len(opts) > 0 {
		aopts = newJsOpts()
		for _, f := range opts {
			if err = f(aopts); err != nil {
				return err
			}
		}
	}

	if aopts == nil || aopts.timeout == 0 && aopts.ctx == nil && aopts.ackstr == jsStreamUnspecified {
		return nc.publish(subj, _EMPTY_, nil, data)
	}

	ctx, cancel := aopts.context(nc.Opts.JetStreamTimeout)
	defer cancel()

	resp, err := nc.RequestWithContext(ctx, subj, data)
	if err != nil {
		return err
	}

	ack, err := ParsePublishAck(resp.Data)
	if err != nil {
		return err
	}

	if ack.Stream == "" || ack.Sequence == 0 {
		return ErrInvalidJSAck
	}

	if aopts.ackstr == jsStreamUnspecified || aopts.ackstr == "" {
		return nil
	}

	if ack.Stream == aopts.ackstr {
		return nil
	}

	return fmt.Errorf("received ack from stream %q", ack.Stream)
}
