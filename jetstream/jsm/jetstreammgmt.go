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

package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	api "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// JetStreamMgmt provides helpers for Message Set and Observable maintenance on a JetStream server
type JetStreamMgmt struct {
	timeout time.Duration
	nc      *nats.Conn
}

// NewJSM creates a new instance of the JetStream Management package
func NewJSM(c *nats.Conn, timeout time.Duration) *JetStreamMgmt {
	return &JetStreamMgmt{
		timeout: time.Second,
		nc:      c,
	}
}

// MessageSetCreate creates a new message set
func (j *JetStreamMgmt) MessageSetCreate(c *api.MsgSetConfig) error {
	req, err := json.Marshal(c)
	if err != nil {
		return err
	}

	_, err = j.request(api.JetStreamCreateMsgSet, req)
	if err != nil {
		return err
	}

	return nil
}

// IsMessageSetKnown determines if a message set is known to the server
func (j *JetStreamMgmt) IsMessageSetKnown(set string) (bool, error) {
	sets, err := j.MessageSets()
	if err != nil {
		return false, fmt.Errorf("could not retrieve list of message sets: %v", err)
	}

	for _, s := range sets {
		if s == set {
			return true, nil
		}
	}

	return false, nil
}

// MessageSets retrieve a list of known message sets
func (j *JetStreamMgmt) MessageSets() (sets []string, err error) {
	sets = []string{}
	err = j.requestAndUnMarshal(api.JetStreamMsgSets, nil, &sets)

	sort.Strings(sets)

	return sets, err
}

// MessageSetDelete removes a message set
func (j *JetStreamMgmt) MessageSetDelete(setName string) (err error) {
	_, err = j.request(api.JetStreamDeleteMsgSet, []byte(setName))
	if err != nil {
		return err
	}

	return nil
}

// MessageSetPurge deletes all messages from a message set while leaving it active
func (j *JetStreamMgmt) MessageSetPurge(setName string) (err error) {
	_, err = j.request(api.JetStreamPurgeMsgSet, []byte(setName))

	return err
}

// MessageSetGetItem retrieves a specific item from a message set
func (j *JetStreamMgmt) MessageSetGetItem(setName string, id int64) (msg *api.StoredMsg, err error) {
	msg = &api.StoredMsg{}
	err = j.requestAndUnMarshal(fmt.Sprintf("%s.%s", api.JetStreamMsgBySeqPre, setName), []byte(fmt.Sprintf("%d", id)), msg)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// MessageSetInfo retrieves configuration and state of a message set
func (j *JetStreamMgmt) MessageSetInfo(setName string) (info *api.MsgSetInfo, err error) {
	known, err := j.IsMessageSetKnown(setName)
	if err != nil {
		return nil, err
	}

	if !known {
		return nil, fmt.Errorf("message set %s is not known", setName)
	}

	info = &api.MsgSetInfo{}
	err = j.requestAndUnMarshal(api.JetStreamMsgSetInfo, []byte(setName), info)

	return info, err
}

// ObservableCreate creates a new observable within a message set
func (j *JetStreamMgmt) ObservableCreate(set string, cfg *api.ObservableConfig) error {
	req := &api.CreateObservableRequest{
		MsgSet: set,
		Config: *cfg,
	}

	jreq, err := json.Marshal(req)
	if err != nil {
		return err
	}

	_, err = j.request(api.JetStreamCreateObservable, jreq)
	if err != nil {
		return err
	}

	return nil
}

// IsObservableKnown determines if an observable is known within a message set
func (j *JetStreamMgmt) IsObservableKnown(set string, obs string) (bool, error) {
	observables, err := j.Observables(set)
	if err != nil {
		return false, fmt.Errorf("could not retrieve list of observables: %v", err)
	}

	for _, o := range observables {
		if o == obs {
			return true, nil
		}
	}

	return false, nil
}

// Observables retrieves a list of observables in a given message set
func (j *JetStreamMgmt) Observables(set string) (observables []string, err error) {
	observables = []string{}
	err = j.requestAndUnMarshal(api.JetStreamObservables, []byte(set), &observables)
	return observables, err
}

// ObservableDelete removes an observable from a message set
func (j *JetStreamMgmt) ObservableDelete(setName string, obsName string) (err error) {
	_, err = j.request(api.JetStreamDeleteObservable, []byte(fmt.Sprintf("%s %s", setName, obsName)))
	if err != nil {
		return err
	}

	return nil
}

// ObservableInfo retrieves configuration and state information about an observable
func (j *JetStreamMgmt) ObservableInfo(setName string, obsName string) (info *api.ObservableInfo, err error) {
	info = &api.ObservableInfo{}
	err = j.requestAndUnMarshal(api.JetStreamObservableInfo, []byte(fmt.Sprintf("%s %s", setName, obsName)), info)

	return info, err
}

// ObservableNext retrieves the next message from a pull based observable
func (j *JetStreamMgmt) ObservableNext(set string, observable string) (msg *nats.Msg, err error) {
	s := fmt.Sprintf("%s.%s.%s", api.JetStreamRequestNextPre, set, observable)

	msg, err = j.request(s, nil)
	if err != nil {
		return nil, err
	}

	return msg, err
}

// AccountStats retrieves status of an account
func (j *JetStreamMgmt) AccountStats() (info *api.JetStreamAccountStats, err error) {
	info = &api.JetStreamAccountStats{}
	err = j.requestAndUnMarshal(api.JetStreamInfo, nil, info)

	return info, err
}

func (j *JetStreamMgmt) requestAndUnMarshal(t string, payload []byte, target interface{}) (err error) {
	resp, err := j.request(t, payload)
	if err != nil {
		return err
	}

	err = json.Unmarshal(resp.Data, target)
	if err != nil {
		return err
	}

	return nil
}

func (j *JetStreamMgmt) request(t string, payload []byte) (msg *nats.Msg, err error) {
	resp, err := j.nc.Request(t, payload, j.timeout)
	if err != nil {
		return nil, err
	}

	if strings.HasPrefix(string(resp.Data), "-ERR") {
		return nil, fmt.Errorf(strings.TrimPrefix(string(resp.Data), "-ERR "))
	}

	return resp, nil
}
