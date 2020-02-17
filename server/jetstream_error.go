package server

import (
	"fmt"
	"time"

	"github.com/nats-io/nuid"
	"golang.org/x/time/rate"
)

var jsLimitsExceededError = NewJSError("JS1", "A message received exceeded Stream storage limits and was dropped").UserMessage("message size exceeds maximum allowed").RateLimit()
var jsStorageWriteError = NewJSError("JS2", "A message could not be stored in the JetStream storage system").UserMessage("failed to store a msg").RateLimit()
var jsBadSequenceError = NewJSError("JS3", "A invalid sequence number were requested").UserMessage("bad sequence argument")
var jsStorageReadError = NewJSError("JS4", "Reading from the JetStream storage system failed").UserMessage("could not load message from storage").RateLimit()
var jsStartUpError = NewJSError("JS5", "Errors encountered during JetStream startup for an account")

var errorEventLimit = rate.NewLimiter(rate.Every(time.Second), 10)

// JetStreamErrorEvent is an advisory about errors encountered during the operation of JetStream
type JetStreamErrorEvent struct {
	Schema      string                 `json:"schema"`
	ID          string                 `json:"id"`
	Time        string                 `json:"timestamp"`
	Code        string                 `json:"code"`
	Detail      map[string]interface{} `json:"detail"`
	Message     string                 `json:"message"`
	Description string                 `json:"description"`
	Error       string                 `json:"error,omitempty"`
	Stream      string                 `json:"stream,omitempty"`
	Consumer    string                 `json:"consumer,omitempty"`
	Server      string                 `json:"server,omitempty"`
	Client      *ClientAPIAudit        `json:"client,omitempty"`
}

const errorAdvisorySchema = "io.nats.jetstream.advisory.v1.error"

type JSErrorDetail map[string]interface{}
type JSError struct {
	code        string
	err         error
	detail      JSErrorDetail
	msg         string
	description string
	rateLimited bool
}

func NewJSError(code string, description string) JSError {
	return JSError{code: code, description: description, detail: make(map[string]interface{})}
}

func (j JSError) UserResponse() []byte {
	return []byte("-ERR '" + j.msg + "'")
}

func (j JSError) Detail(detail JSErrorDetail) JSError {
	for k, v := range detail {
		j.detail[k] = v
	}

	return j
}

func (j JSError) Wrap(err error) JSError {
	j.err = err
	return j
}

func (j JSError) UserMessage(m string) JSError {
	j.msg = m
	return j
}

func (j JSError) RateLimit() JSError {
	j.rateLimited = true
	return j
}

func (j JSError) ShouldRateLimit() bool {
	return j.rateLimited
}

func (j JSError) UserMessagef(format string, arg ...interface{}) {
	j.UserMessage(fmt.Sprintf(format, arg...))
}

func (j JSError) Error() string {
	body := j.msg
	if j.msg == "" {
		body = "jserror: " + j.description
	}

	if j.err != nil {
		return fmt.Sprintf("%s: %v: %v", body, j.err, j.detail)
	}

	return fmt.Sprintf("%s: %v", body, j.detail)
}

func (j JSError) LogLine() string {
	return fmt.Sprintf("JetStream Error: %s %v", j.msg, j.detail)
}

func (j JSError) Advisory() JetStreamErrorEvent {
	adv := JetStreamErrorEvent{
		Schema:      errorAdvisorySchema,
		ID:          nuid.Next(),
		Time:        time.Now().UTC().Format(time.RFC3339Nano),
		Code:        j.code,
		Description: j.description,
		Detail:      j.detail,
		Message:     j.msg,
	}

	if j.err != nil {
		adv.Error = j.err.Error()
	}

	return adv
}
