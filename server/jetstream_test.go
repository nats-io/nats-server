// Copyright 2019-2023 The NATS Authors
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

//go:build !skip_js_tests
// +build !skip_js_tests

package server

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server/sysmem"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

func TestJetStreamBasicNilConfig(t *testing.T) {
	s := RunRandClientPortServer()
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	if err := s.EnableJetStream(nil); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}
	if s.SystemAccount() == nil {
		t.Fatalf("Expected system account to be created automatically")
	}
	// Grab our config since it was dynamically generated.
	config := s.JetStreamConfig()
	if config == nil {
		t.Fatalf("Expected non-nil config")
	}
	// Check dynamic max memory.
	hwMem := sysmem.Memory()
	if hwMem != 0 {
		// Make sure its about 75%
		est := hwMem / 4 * 3
		if config.MaxMemory != est {
			t.Fatalf("Expected memory to be 80 percent of system memory, got %v vs %v", config.MaxMemory, est)
		}
	}
	// Make sure it was created.
	stat, err := os.Stat(config.StoreDir)
	if err != nil {
		t.Fatalf("Expected the store directory to be present, %v", err)
	}
	if stat == nil || !stat.IsDir() {
		t.Fatalf("Expected a directory")
	}
}

func RunBasicJetStreamServer(t testing.TB) *Server {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	return RunServer(&opts)
}

func RunJetStreamServerOnPort(port int, sd string) *Server {
	opts := DefaultTestOptions
	opts.Port = port
	opts.JetStream = true
	opts.StoreDir = filepath.Dir(sd)
	return RunServer(&opts)
}

func clientConnectToServer(t *testing.T, s *Server) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(),
		nats.Name("JS-TEST"),
		nats.ReconnectWait(5*time.Millisecond),
		nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	return nc
}

func clientConnectWithOldRequest(t *testing.T, s *Server) *nats.Conn {
	nc, err := nats.Connect(s.ClientURL(), nats.UseOldRequestStyle())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	return nc
}

func TestJetStreamEnableAndDisableAccount(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Global in simple setup should be enabled already.
	if !s.GlobalAccount().JetStreamEnabled() {
		t.Fatalf("Expected to have jetstream enabled on global account")
	}
	if na := s.JetStreamNumAccounts(); na != 1 {
		t.Fatalf("Expected 1 account, got %d", na)
	}

	if err := s.GlobalAccount().DisableJetStream(); err != nil {
		t.Fatalf("Did not expect error on disabling account: %v", err)
	}
	if na := s.JetStreamNumAccounts(); na != 0 {
		t.Fatalf("Expected no accounts, got %d", na)
	}
	// Make sure we unreserved resources.
	if rm, rd, err := s.JetStreamReservedResources(); err != nil {
		t.Fatalf("Unexpected error requesting jetstream reserved resources: %v", err)
	} else if rm != 0 || rd != 0 {
		t.Fatalf("Expected reserved memory and store to be 0, got %v and %v", friendlyBytes(rm), friendlyBytes(rd))
	}

	acc, _ := s.LookupOrRegisterAccount("$FOO")
	if err := acc.EnableJetStream(nil); err != nil {
		t.Fatalf("Did not expect error on enabling account: %v", err)
	}
	if na := s.JetStreamNumAccounts(); na != 1 {
		t.Fatalf("Expected 1 account, got %d", na)
	}
	if err := acc.DisableJetStream(); err != nil {
		t.Fatalf("Did not expect error on disabling account: %v", err)
	}
	if na := s.JetStreamNumAccounts(); na != 0 {
		t.Fatalf("Expected no accounts, got %d", na)
	}
	// We should get error if disabling something not enabled.
	acc, _ = s.LookupOrRegisterAccount("$BAR")
	if err := acc.DisableJetStream(); err == nil {
		t.Fatalf("Expected error on disabling account that was not enabled")
	}
	// Should get an error for trying to enable a non-registered account.
	acc = NewAccount("$BAZ")
	if err := acc.EnableJetStream(nil); err == nil {
		t.Fatalf("Expected error on enabling account that was not registered")
	}
}

func TestJetStreamAddStream(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   FileStorage,
				Replicas:  1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			js.Publish("foo", []byte("Hello World!"))
			state := mset.state()
			if state.Msgs != 1 {
				t.Fatalf("Expected 1 message, got %d", state.Msgs)
			}
			if state.Bytes == 0 {
				t.Fatalf("Expected non-zero bytes")
			}

			js.Publish("foo", []byte("Hello World Again!"))
			state = mset.state()
			if state.Msgs != 2 {
				t.Fatalf("Expected 2 messages, got %d", state.Msgs)
			}

			if err := mset.delete(); err != nil {
				t.Fatalf("Got an error deleting the stream: %v", err)
			}
		})
	}
}

func TestJetStreamAddStreamDiscardNew(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:     "foo",
				MaxMsgs:  10,
				MaxBytes: 4096,
				Discard:  DiscardNew,
				Storage:  MemoryStorage,
				Replicas: 1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:     "foo",
				MaxMsgs:  10,
				MaxBytes: 4096,
				Discard:  DiscardNew,
				Storage:  FileStorage,
				Replicas: 1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			subj := "foo"
			toSend := 10
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subj, fmt.Sprintf("MSG: %d", i+1))
			}
			// We expect this one to fail due to discard policy.
			resp, _ := nc.Request(subj, []byte("discard me"), 100*time.Millisecond)
			if resp == nil {
				t.Fatalf("No response, possible timeout?")
			}
			if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error.Description != "maximum messages exceeded" || pa.Stream != "foo" {
				t.Fatalf("Expected to get an error about maximum messages, got %q", resp.Data)
			}

			// Now do bytes.
			mset.purge(nil)

			big := make([]byte, 8192)
			resp, _ = nc.Request(subj, big, 100*time.Millisecond)
			if resp == nil {
				t.Fatalf("No response, possible timeout?")
			}
			if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error.Description != "maximum bytes exceeded" || pa.Stream != "foo" {
				t.Fatalf("Expected to get an error about maximum bytes, got %q", resp.Data)
			}
		})
	}
}

func TestJetStreamAutoTuneFSConfig(t *testing.T) {
	s := RunRandClientPortServer()
	defer s.Shutdown()

	jsconfig := &JetStreamConfig{MaxMemory: -1, MaxStore: 128 * 1024 * 1024}
	if err := s.EnableJetStream(jsconfig); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	maxMsgSize := int32(512)
	streamConfig := func(name string, maxMsgs, maxBytes int64) *StreamConfig {
		t.Helper()
		cfg := &StreamConfig{Name: name, MaxMsgSize: maxMsgSize, Storage: FileStorage}
		if maxMsgs > 0 {
			cfg.MaxMsgs = maxMsgs
		}
		if maxBytes > 0 {
			cfg.MaxBytes = maxBytes
		}
		return cfg
	}

	acc := s.GlobalAccount()

	testBlkSize := func(subject string, maxMsgs, maxBytes int64, expectedBlkSize uint64) {
		t.Helper()
		mset, err := acc.addStream(streamConfig(subject, maxMsgs, maxBytes))
		if err != nil {
			t.Fatalf("Unexpected error adding stream: %v", err)
		}
		defer mset.delete()
		fsCfg, err := mset.fileStoreConfig()
		if err != nil {
			t.Fatalf("Unexpected error retrieving file store: %v", err)
		}
		if fsCfg.BlockSize != expectedBlkSize {
			t.Fatalf("Expected auto tuned block size to be %d, got %d", expectedBlkSize, fsCfg.BlockSize)
		}
	}

	testBlkSize("foo", 1, 0, FileStoreMinBlkSize)
	testBlkSize("foo", 1, 512, FileStoreMinBlkSize)
	testBlkSize("foo", 1, 1024*1024, defaultMediumBlockSize)
	testBlkSize("foo", 1, 8*1024*1024, defaultMediumBlockSize)
	testBlkSize("foo_bar_baz", -1, 32*1024*1024, FileStoreMaxBlkSize)
}

func TestJetStreamConsumerAndStreamDescriptions(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	descr := "foo asset"
	acc := s.GlobalAccount()

	// Check stream's first.
	mset, err := acc.addStream(&StreamConfig{Name: "foo", Description: descr})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	if cfg := mset.config(); cfg.Description != descr {
		t.Fatalf("Expected a description of %q, got %q", descr, cfg.Description)
	}

	// Now consumer
	edescr := "analytics"
	o, err := mset.addConsumer(&ConsumerConfig{
		Description:    edescr,
		DeliverSubject: "to",
		AckPolicy:      AckNone})
	if err != nil {
		t.Fatalf("Unexpected error adding consumer: %v", err)
	}
	if cfg := o.config(); cfg.Description != edescr {
		t.Fatalf("Expected a description of %q, got %q", edescr, cfg.Description)
	}

	// Test max.
	data := make([]byte, JSMaxDescriptionLen+1)
	crand.Read(data)
	bigDescr := base64.StdEncoding.EncodeToString(data)

	_, err = acc.addStream(&StreamConfig{Name: "bar", Description: bigDescr})
	if err == nil || !strings.Contains(err.Error(), "description is too long") {
		t.Fatalf("Expected an error but got none")
	}

	_, err = mset.addConsumer(&ConsumerConfig{
		Description:    bigDescr,
		DeliverSubject: "to",
		AckPolicy:      AckNone})
	if err == nil || !strings.Contains(err.Error(), "description is too long") {
		t.Fatalf("Expected an error but got none")
	}
}
func TestJetStreamConsumerWithNameAndDurable(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	descr := "foo asset"
	name := "name"
	durable := "durable"
	acc := s.GlobalAccount()

	// Check stream's first.
	mset, err := acc.addStream(&StreamConfig{Name: "foo", Description: descr})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	if cfg := mset.config(); cfg.Description != descr {
		t.Fatalf("Expected a description of %q, got %q", descr, cfg.Description)
	}

	// it's ok to specify  both durable and name, but they have to be the same.
	_, err = mset.addConsumer(&ConsumerConfig{
		DeliverSubject: "to",
		Durable:        "consumer",
		Name:           "consumer",
		AckPolicy:      AckNone})
	if err != nil {
		t.Fatalf("Unexpected error adding consumer: %v", err)
	}

	// if they're not the same, expect error
	_, err = mset.addConsumer(&ConsumerConfig{
		DeliverSubject: "to",
		Durable:        durable,
		Name:           name,
		AckPolicy:      AckNone})

	if !strings.Contains(err.Error(), "Consumer Durable and Name have to be equal") {
		t.Fatalf("Wrong error while adding consumer with not matching Name and Durable: %v", err)
	}

}

func TestJetStreamPubAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	sname := "PUBACK"
	acc := s.GlobalAccount()
	mconfig := &StreamConfig{Name: sname, Subjects: []string{"foo"}, Storage: MemoryStorage}
	mset, err := acc.addStream(mconfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	checkRespDetails := func(resp *nats.Msg, err error, seq uint64) {
		if err != nil {
			t.Fatalf("Unexpected error from send stream msg: %v", err)
		}
		if resp == nil {
			t.Fatalf("No response from send stream msg")
		}
		pa := getPubAckResponse(resp.Data)
		if pa == nil || pa.Error != nil {
			t.Fatalf("Expected a valid JetStreamPubAck, got %q", resp.Data)
		}
		if pa.Stream != sname {
			t.Fatalf("Expected %q for stream name, got %q", sname, pa.Stream)
		}
		if pa.Sequence != seq {
			t.Fatalf("Expected %d for sequence, got %d", seq, pa.Sequence)
		}
	}

	// Send messages and make sure pubAck details are correct.
	for i := uint64(1); i <= 1000; i++ {
		resp, err := nc.Request("foo", []byte("HELLO"), 100*time.Millisecond)
		checkRespDetails(resp, err, i)
	}
}

func TestJetStreamConsumerWithStartTime(t *testing.T) {
	subj := "my_stream"
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: subj, Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: subj, Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			fsCfg := &FileStoreConfig{BlockSize: 100}
			mset, err := s.GlobalAccount().addStreamWithStore(c.mconfig, fsCfg)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 250
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subj, fmt.Sprintf("MSG: %d", i+1))
			}

			time.Sleep(10 * time.Millisecond)
			startTime := time.Now().UTC()

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subj, fmt.Sprintf("MSG: %d", i+1))
			}

			if msgs := mset.state().Msgs; msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, msgs)
			}

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:       "d",
				DeliverPolicy: DeliverByStartTime,
				OptStartTime:  &startTime,
				AckPolicy:     AckExplicit,
			})
			require_NoError(t, err)
			defer o.delete()

			msg, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
			require_NoError(t, err)
			sseq, dseq, _, _, _ := replyInfo(msg.Reply)
			if dseq != 1 {
				t.Fatalf("Expected delivered seq of 1, got %d", dseq)
			}
			if sseq != uint64(toSend+1) {
				t.Fatalf("Expected to get store seq of %d, got %d", toSend+1, sseq)
			}
		})
	}
}

// Test for https://github.com/nats-io/jetstream/issues/143
func TestJetStreamConsumerWithMultipleStartOptions(t *testing.T) {
	subj := "my_stream"
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: subj, Subjects: []string{"foo.>"}, Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: subj, Subjects: []string{"foo.>"}, Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			obsReq := CreateConsumerRequest{
				Stream: subj,
				Config: ConsumerConfig{
					Durable:       "d",
					DeliverPolicy: DeliverLast,
					FilterSubject: "foo.22",
					AckPolicy:     AckExplicit,
				},
			}
			req, err := json.Marshal(obsReq)
			require_NoError(t, err)
			_, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, subj), req, time.Second)
			require_NoError(t, err)
			nc.Close()
			s.Shutdown()
		})
	}
}

func TestJetStreamConsumerMaxDeliveries(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_WQ", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "MY_WQ", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Queue up our work item.
			sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			maxDeliver := 5
			ackWait := 10 * time.Millisecond

			o, err := mset.addConsumer(&ConsumerConfig{
				DeliverSubject: sub.Subject,
				AckPolicy:      AckExplicit,
				AckWait:        ackWait,
				MaxDeliver:     maxDeliver,
			})
			require_NoError(t, err)
			defer o.delete()

			// Wait for redeliveries to pile up.
			checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, err := sub.Pending(); err != nil || nmsgs != maxDeliver {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, maxDeliver)
				}
				return nil
			})

			// Now wait a bit longer and make sure we do not have more than maxDeliveries.
			time.Sleep(2 * ackWait)
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxDeliver {
				t.Fatalf("Did not receive correct number of messages: %d vs %d", nmsgs, maxDeliver)
			}
		})
	}
}

func TestJetStreamNextReqFromMsg(t *testing.T) {
	bef := time.Now()
	expires, _, _, _, _, _, err := nextReqFromMsg([]byte(`{"expires":5000000000}`)) // nanoseconds
	require_NoError(t, err)
	now := time.Now()
	if expires.Before(bef.Add(5*time.Second)) || expires.After(now.Add(5*time.Second)) {
		t.Fatal("Expires out of expected range")
	}
}

func TestJetStreamPullConsumerDelayedFirstPullWithReplayOriginal(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_WQ", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "MY_WQ", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Queue up our work item.
			sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:      "d",
				AckPolicy:    AckExplicit,
				ReplayPolicy: ReplayOriginal,
			})
			require_NoError(t, err)
			defer o.delete()

			// Force delay here which triggers the bug.
			time.Sleep(250 * time.Millisecond)

			if _, err = nc.Request(o.requestNextMsgSubject(), nil, time.Second); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestJetStreamConsumerAckFloorFill(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MQ", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "MQ", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			for i := 1; i <= 4; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, fmt.Sprintf("msg-%d", i))
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "d",
				DeliverSubject: sub.Subject,
				AckPolicy:      AckExplicit,
			})
			require_NoError(t, err)
			defer o.delete()

			var first *nats.Msg

			for i := 1; i <= 3; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error receiving message %d: %v", i, err)
				}
				// Don't ack 1 or 4.
				if i == 1 {
					first = m
				} else if i == 2 || i == 3 {
					m.Respond(nil)
				}
			}
			nc.Flush()
			if info := o.info(); info.AckFloor.Consumer != 0 {
				t.Fatalf("Expected the ack floor to be 0, got %d", info.AckFloor.Consumer)
			}
			// Now ack first, should move ack floor to 3.
			first.Respond(nil)
			nc.Flush()
			if info := o.info(); info.AckFloor.Consumer != 3 {
				t.Fatalf("Expected the ack floor to be 3, got %d", info.AckFloor.Consumer)
			}
		})
	}
}

func TestJetStreamNoPanicOnRaceBetweenShutdownAndConsumerDelete(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_STREAM", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "MY_STREAM", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			var cons []*consumer
			for i := 0; i < 100; i++ {
				o, err := mset.addConsumer(&ConsumerConfig{
					Durable:   fmt.Sprintf("d%d", i),
					AckPolicy: AckExplicit,
				})
				require_NoError(t, err)
				defer o.delete()
				cons = append(cons, o)
			}

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, c := range cons {
					c.delete()
				}
			}()
			time.Sleep(10 * time.Millisecond)
			s.Shutdown()
		})
	}
}

func TestJetStreamAddStreamMaxMsgSize(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:       "foo",
				Retention:  LimitsPolicy,
				MaxAge:     time.Hour,
				Storage:    MemoryStorage,
				MaxMsgSize: 22,
				Replicas:   1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:       "foo",
				Retention:  LimitsPolicy,
				MaxAge:     time.Hour,
				Storage:    FileStorage,
				MaxMsgSize: 22,
				Replicas:   1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			if _, err := nc.Request("foo", []byte("Hello World!"), time.Second); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			tooBig := []byte("1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ")
			resp, err := nc.Request("foo", tooBig, time.Second)
			require_NoError(t, err)
			if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error.Description != "message size exceeds maximum allowed" {
				t.Fatalf("Expected to get an error for maximum message size, got %q", pa.Error)
			}
		})
	}
}

func TestJetStreamAddStreamCanonicalNames(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	expectErr := func(_ *stream, err error) {
		t.Helper()
		if !IsNatsErr(err, JSStreamInvalidConfigF) {
			t.Fatalf("Expected error but got none")
		}
	}

	expectErr(acc.addStream(&StreamConfig{Name: "foo.bar"}))
	expectErr(acc.addStream(&StreamConfig{Name: "foo.bar."}))
	expectErr(acc.addStream(&StreamConfig{Name: "foo.*"}))
	expectErr(acc.addStream(&StreamConfig{Name: "foo.>"}))
	expectErr(acc.addStream(&StreamConfig{Name: "*"}))
	expectErr(acc.addStream(&StreamConfig{Name: ">"}))
	expectErr(acc.addStream(&StreamConfig{Name: "*>"}))
}

func TestJetStreamAddStreamBadSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	expectAPIErr := func(cfg StreamConfig) {
		t.Helper()
		req, err := json.Marshal(cfg)
		require_NoError(t, err)
		resp, _ := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
		var scResp JSApiStreamCreateResponse
		if err := json.Unmarshal(resp.Data, &scResp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		require_Error(t, scResp.ToError(), NewJSStreamInvalidConfigError(fmt.Errorf("invalid subject")))
	}

	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{"foo.bar."}})
	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{".."}})
	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{".*"}})
	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{".>"}})
	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{" x"}})
	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{"y "}})
}

func TestJetStreamMaxConsumers(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:         "MAXC",
		Storage:      nats.MemoryStorage,
		Subjects:     []string{"in.maxc.>"},
		MaxConsumers: 1,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	si, err := js.StreamInfo("MAXC")
	require_NoError(t, err)
	if si.Config.MaxConsumers != 1 {
		t.Fatalf("Expected max of 1, got %d", si.Config.MaxConsumers)
	}
	// Make sure we get the right error.
	// This should succeed.
	if _, err := js.SubscribeSync("in.maxc.foo"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("in.maxc.bar"); err == nil {
		t.Fatalf("Eexpected error but got none")
	}
}

func TestJetStreamAddStreamOverlappingSubjects(t *testing.T) {
	mconfig := &StreamConfig{
		Name:     "ok",
		Storage:  MemoryStorage,
		Subjects: []string{"foo", "bar", "baz.*", "foo.bar.baz.>"},
	}

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()
	mset, err := acc.addStream(mconfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	expectErr := func(_ *stream, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "subjects overlap") {
			t.Fatalf("Expected error but got none")
		}
	}

	// Test that any overlapping subjects will fail.
	expectErr(acc.addStream(&StreamConfig{Name: "foo"}))
	expectErr(acc.addStream(&StreamConfig{Name: "a", Subjects: []string{"baz", "bar"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "b", Subjects: []string{">"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "c", Subjects: []string{"baz.33"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "d", Subjects: []string{"*.33"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "e", Subjects: []string{"*.>"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "f", Subjects: []string{"foo.bar", "*.bar.>"}}))
}

func TestJetStreamAddStreamOverlapWithJSAPISubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	expectErr := func(_ *stream, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "subjects overlap") {
			t.Fatalf("Expected error but got none")
		}
	}

	// Test that any overlapping subjects with our JSAPI should fail.
	expectErr(acc.addStream(&StreamConfig{Name: "a", Subjects: []string{"$JS.API.foo", "$JS.API.bar"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "b", Subjects: []string{"$JS.API.>"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "c", Subjects: []string{"$JS.API.*"}}))

	// Events and Advisories etc should be ok.
	if _, err := acc.addStream(&StreamConfig{Name: "a", Subjects: []string{"$JS.EVENT.>"}}); err != nil {
		t.Fatalf("Expected this to work: %v", err)
	}
}

func TestJetStreamAddStreamSameConfigOK(t *testing.T) {
	mconfig := &StreamConfig{
		Name:     "ok",
		Subjects: []string{"foo", "bar", "baz.*", "foo.bar.baz.>"},
		Storage:  MemoryStorage,
	}

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()
	mset, err := acc.addStream(mconfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	// Adding again with same config should be idempotent.
	if _, err = acc.addStream(mconfig); err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
}

func sendStreamMsg(t *testing.T, nc *nats.Conn, subject, msg string) *PubAck {
	t.Helper()
	resp, _ := nc.Request(subject, []byte(msg), 500*time.Millisecond)
	if resp == nil {
		t.Fatalf("No response for %q, possible timeout?", msg)
	}
	pa := getPubAckResponse(resp.Data)
	if pa == nil || pa.Error != nil {
		t.Fatalf("Expected a valid JetStreamPubAck, got %q", resp.Data)
	}
	return pa.PubAck
}

func TestJetStreamBasicAckPublish(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "foo", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "foo", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			for i := 0; i < 50; i++ {
				sendStreamMsg(t, nc, "foo.bar", "Hello World!")
			}
			state := mset.state()
			if state.Msgs != 50 {
				t.Fatalf("Expected 50 messages, got %d", state.Msgs)
			}
		})
	}
}

func TestJetStreamStateTimestamps(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "foo", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "foo", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			start := time.Now()
			delay := 250 * time.Millisecond
			sendStreamMsg(t, nc, "foo.bar", "Hello World!")
			time.Sleep(delay)
			sendStreamMsg(t, nc, "foo.bar", "Hello World Again!")

			state := mset.state()
			if state.FirstTime.Before(start) {
				t.Fatalf("Unexpected first message timestamp: %v", state.FirstTime)
			}
			if state.LastTime.Before(start.Add(delay)) {
				t.Fatalf("Unexpected last message timestamp: %v", state.LastTime)
			}
		})
	}
}

func TestJetStreamNoAckStream(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "foo", Storage: MemoryStorage, NoAck: true}},
		{"FileStore", &StreamConfig{Name: "foo", Storage: FileStorage, NoAck: true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			// We can use NoAck to suppress acks even when reply subjects are present.
			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			if _, err := nc.Request("foo", []byte("Hello World!"), 25*time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Expected a timeout error and no response with acks suppressed")
			}

			state := mset.state()
			if state.Msgs != 1 {
				t.Fatalf("Expected 1 message, got %d", state.Msgs)
			}
		})
	}
}

func TestJetStreamCreateConsumer(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "foo", Storage: MemoryStorage, Subjects: []string{"foo", "bar"}, Retention: WorkQueuePolicy}},
		{"FileStore", &StreamConfig{Name: "foo", Storage: FileStorage, Subjects: []string{"foo", "bar"}, Retention: WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// Check for basic errors.
			if _, err := mset.addConsumer(nil); err == nil {
				t.Fatalf("Expected an error for no config")
			}

			// No deliver subject, meaning its in pull mode, work queue mode means it is required to
			// do explicit ack.
			if _, err := mset.addConsumer(&ConsumerConfig{}); err == nil {
				t.Fatalf("Expected an error on work queue / pull mode without explicit ack mode")
			}

			// Check for delivery subject errors.

			// Literal delivery subject required.
			if _, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: "foo.*"}); err == nil {
				t.Fatalf("Expected an error on bad delivery subject")
			}
			// Check for cycles
			if _, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: "foo"}); err == nil {
				t.Fatalf("Expected an error on delivery subject that forms a cycle")
			}
			if _, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: "bar"}); err == nil {
				t.Fatalf("Expected an error on delivery subject that forms a cycle")
			}
			if _, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: "*"}); err == nil {
				t.Fatalf("Expected an error on delivery subject that forms a cycle")
			}

			// StartPosition conflicts
			now := time.Now().UTC()
			if _, err := mset.addConsumer(&ConsumerConfig{
				DeliverSubject: "A",
				OptStartSeq:    1,
				OptStartTime:   &now,
			}); err == nil {
				t.Fatalf("Expected an error on start position conflicts")
			}
			if _, err := mset.addConsumer(&ConsumerConfig{
				DeliverSubject: "A",
				OptStartTime:   &now,
			}); err == nil {
				t.Fatalf("Expected an error on start position conflicts")
			}

			// Non-Durables need to have subscription to delivery subject.
			delivery := nats.NewInbox()
			nc := clientConnectToServer(t, s)
			defer nc.Close()
			sub, _ := nc.SubscribeSync(delivery)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery, AckPolicy: AckExplicit})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}

			if err := mset.deleteConsumer(o); err != nil {
				t.Fatalf("Expected no error on delete, got %v", err)
			}

			// Now let's check that durables can be created and a duplicate call to add will be ok.
			dcfg := &ConsumerConfig{
				Durable:        "ddd",
				DeliverSubject: delivery,
				AckPolicy:      AckExplicit,
			}
			if _, err = mset.addConsumer(dcfg); err != nil {
				t.Fatalf("Unexpected error creating consumer: %v", err)
			}
			if _, err = mset.addConsumer(dcfg); err != nil {
				t.Fatalf("Unexpected error creating second identical consumer: %v", err)
			}
			// Not test that we can change the delivery subject if that is only thing that has not
			// changed and we are not active.
			sub.Unsubscribe()
			sub, _ = nc.SubscribeSync("d.d.d")
			nc.Flush()
			defer sub.Unsubscribe()
			dcfg.DeliverSubject = "d.d.d"
			if _, err = mset.addConsumer(dcfg); err != nil {
				t.Fatalf("Unexpected error creating third consumer with just deliver subject changed: %v", err)
			}
		})
	}
}

func TestJetStreamBasicDeliverSubject(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MSET", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "MSET", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 100
			sendSubj := "foo.bar"
			for i := 1; i <= toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, strconv.Itoa(i))
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			// Now create an consumer. Use different connection.
			nc2 := clientConnectToServer(t, s)
			defer nc2.Close()

			sub, _ := nc2.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc2.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			// Check for our messages.
			checkMsgs := func(seqOff int) {
				t.Helper()

				checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
					}
					return nil
				})

				// Now let's check the messages
				for i := 0; i < toSend; i++ {
					m, _ := sub.NextMsg(time.Second)
					// JetStream will have the subject match the stream subject, not delivery subject.
					if m.Subject != sendSubj {
						t.Fatalf("Expected original subject of %q, but got %q", sendSubj, m.Subject)
					}
					// Now check that reply subject exists and has a sequence as the last token.
					if seq := o.seqFromReply(m.Reply); seq != uint64(i+seqOff) {
						t.Fatalf("Expected sequence of %d , got %d", i+seqOff, seq)
					}
					// Ack the message here.
					m.Respond(nil)
				}
			}

			checkMsgs(1)

			// Now send more and make sure delivery picks back up.
			for i := toSend + 1; i <= toSend*2; i++ {
				sendStreamMsg(t, nc, sendSubj, strconv.Itoa(i))
			}
			state = mset.state()
			if state.Msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, state.Msgs)
			}

			checkMsgs(101)

			checkSubEmpty := func() {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 0 {
					t.Fatalf("Expected sub to have no pending")
				}
			}
			checkSubEmpty()
			o.delete()

			// Now check for deliver last, deliver new and deliver by seq.
			o, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject, DeliverPolicy: DeliverLast})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			m, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("Did not get expected message, got %v", err)
			}
			// All Consumers start with sequence #1.
			if seq := o.seqFromReply(m.Reply); seq != 1 {
				t.Fatalf("Expected sequence to be 1, but got %d", seq)
			}
			// Check that is is the last msg we sent though.
			if mseq, _ := strconv.Atoi(string(m.Data)); mseq != 200 {
				t.Fatalf("Expected messag sequence to be 200, but got %d", mseq)
			}

			checkSubEmpty()
			o.delete()

			// Make sure we only got one message.
			if m, err := sub.NextMsg(5 * time.Millisecond); err == nil {
				t.Fatalf("Expected no msg, got %+v", m)
			}

			checkSubEmpty()
			o.delete()

			// Now try by sequence number.
			o, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject, DeliverPolicy: DeliverByStartSequence, OptStartSeq: 101})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			checkMsgs(1)

			// Now do push based queue-subscribers
			sub, _ = nc2.QueueSubscribeSync("_qg_", "dev")
			defer sub.Unsubscribe()
			nc2.Flush()

			o, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject, DeliverGroup: "dev"})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			// Since we sent another batch need check to be looking for 2x.
			toSend *= 2
			checkMsgs(1)
		})
	}
}

func workerModeConfig(name string) *ConsumerConfig {
	return &ConsumerConfig{Durable: name, AckPolicy: AckExplicit}
}

func TestJetStreamBasicWorkQueue(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_MSG_SET", Storage: MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &StreamConfig{Name: "MY_MSG_SET", Storage: FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// Create basic work queue mode consumer.
			oname := "WQ"
			o, err := mset.addConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			if o.nextSeq() != 1 {
				t.Fatalf("Expected to be starting at sequence 1")
			}

			nc := clientConnectWithOldRequest(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			sendSubj := "bar"
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			getNext := func(seqno int) {
				t.Helper()
				nextMsg, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error for seq %d: %v", seqno, err)
				}
				if nextMsg.Subject != "bar" {
					t.Fatalf("Expected subject of %q, got %q", "bar", nextMsg.Subject)
				}
				if seq := o.seqFromReply(nextMsg.Reply); seq != uint64(seqno) {
					t.Fatalf("Expected sequence of %d , got %d", seqno, seq)
				}
			}

			// Make sure we can get the messages already there.
			for i := 1; i <= toSend; i++ {
				getNext(i)
			}

			// Now we want to make sure we can get a message that is published to the message
			// set as we are waiting for it.
			nextDelay := 50 * time.Millisecond

			go func() {
				time.Sleep(nextDelay)
				sendStreamMsg(t, nc, sendSubj, "Hello World!")
			}()

			start := time.Now()
			getNext(toSend + 1)
			if time.Since(start) < nextDelay {
				t.Fatalf("Received message too quickly")
			}

			// Now do same thing but combine waiting for new ones with sending.
			go func() {
				time.Sleep(nextDelay)
				for i := 0; i < toSend; i++ {
					nc.Request(sendSubj, []byte("Hello World!"), 50*time.Millisecond)
				}
			}()

			for i := toSend + 2; i < toSend*2+2; i++ {
				getNext(i)
			}
		})
	}
}

func TestJetStreamWorkQueueMaxWaiting(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_MSG_SET", Storage: MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &StreamConfig{Name: "MY_MSG_SET", Storage: FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// Make sure these cases fail
			cfg := &ConsumerConfig{Durable: "foo", AckPolicy: AckExplicit, MaxWaiting: 10, DeliverSubject: "_INBOX.22"}
			if _, err := mset.addConsumer(cfg); err == nil {
				t.Fatalf("Expected an error with MaxWaiting set on non-pull based consumer")
			}
			cfg = &ConsumerConfig{Durable: "foo", AckPolicy: AckExplicit, MaxWaiting: -1}
			if _, err := mset.addConsumer(cfg); err == nil {
				t.Fatalf("Expected an error with MaxWaiting being negative")
			}

			// Create basic work queue mode consumer.
			wcfg := workerModeConfig("MAXWQ")
			o, err := mset.addConsumer(wcfg)
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			// Make sure we set default correctly.
			if cfg := o.config(); cfg.MaxWaiting != JSWaitQueueDefaultMax {
				t.Fatalf("Expected default max waiting to have been set to %d, got %d", JSWaitQueueDefaultMax, cfg.MaxWaiting)
			}

			expectWaiting := func(expected int) {
				t.Helper()
				checkFor(t, time.Second, 25*time.Millisecond, func() error {
					if oi := o.info(); oi.NumWaiting != expected {
						return fmt.Errorf("Expected %d waiting, got %d", expected, oi.NumWaiting)
					}
					return nil
				})
			}

			nc := clientConnectWithOldRequest(t, s)
			defer nc.Close()

			// Like muxed new INBOX.
			sub, _ := nc.SubscribeSync("req.*")
			defer sub.Unsubscribe()
			nc.Flush()

			checkSubPending := func(numExpected int) {
				t.Helper()
				checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
					if nmsgs, _, err := sub.Pending(); err != nil || nmsgs != numExpected {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
					}
					return nil
				})
			}

			getSubj := o.requestNextMsgSubject()
			// Queue up JSWaitQueueDefaultMax requests.
			for i := 0; i < JSWaitQueueDefaultMax; i++ {
				nc.PublishRequest(getSubj, fmt.Sprintf("req.%d", i), nil)
			}
			expectWaiting(JSWaitQueueDefaultMax)

			// We are at the max, so we should get a 409 saying that we have
			// exceeded the number of pull requests.
			m, err := nc.Request(getSubj, nil, 100*time.Millisecond)
			require_NoError(t, err)
			// Make sure this is the 409
			if v := m.Header.Get("Status"); v != "409" {
				t.Fatalf("Expected a 409 status code, got %q", v)
			}
			// The sub for the other requests should not have received anything
			checkSubPending(0)
			// Now send some messages that should make some of the requests complete
			sendStreamMsg(t, nc, "foo", "Hello World!")
			sendStreamMsg(t, nc, "bar", "Hello World!")
			expectWaiting(JSWaitQueueDefaultMax - 2)
		})
	}
}

func TestJetStreamWorkQueueWrapWaiting(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_MSG_SET", Storage: MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &StreamConfig{Name: "MY_MSG_SET", Storage: FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			maxWaiting := 8
			wcfg := workerModeConfig("WRAP")
			wcfg.MaxWaiting = maxWaiting

			o, err := mset.addConsumer(wcfg)
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			getSubj := o.requestNextMsgSubject()

			expectWaiting := func(expected int) {
				t.Helper()
				checkFor(t, time.Second, 25*time.Millisecond, func() error {
					if oi := o.info(); oi.NumWaiting != expected {
						return fmt.Errorf("Expected %d waiting, got %d", expected, oi.NumWaiting)
					}
					return nil
				})
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync("req.*")
			defer sub.Unsubscribe()
			nc.Flush()

			// Fill up waiting.
			for i := 0; i < maxWaiting; i++ {
				nc.PublishRequest(getSubj, fmt.Sprintf("req.%d", i), nil)
			}
			expectWaiting(maxWaiting)

			// Now use 1/2 of the waiting.
			for i := 0; i < maxWaiting/2; i++ {
				sendStreamMsg(t, nc, "foo", "Hello World!")
			}
			expectWaiting(maxWaiting / 2)

			// Now add in two (2) more pull requests.
			for i := maxWaiting; i < maxWaiting+2; i++ {
				nc.PublishRequest(getSubj, fmt.Sprintf("req.%d", i), nil)
			}
			expectWaiting(maxWaiting/2 + 2)

			// Now use second 1/2 of the waiting and the 2 extra.
			for i := 0; i < maxWaiting/2+2; i++ {
				sendStreamMsg(t, nc, "bar", "Hello World!")
			}
			expectWaiting(0)

			checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxWaiting+2 {
					return fmt.Errorf("Expected sub to have %d pending, got %d", maxWaiting+2, nmsgs)
				}
				return nil
			})
		})
	}
}

func TestJetStreamWorkQueueRequest(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_MSG_SET", Storage: MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &StreamConfig{Name: "MY_MSG_SET", Storage: FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			o, err := mset.addConsumer(workerModeConfig("WRAP"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 25
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "bar", "Hello World!")
			}

			reply := "_.consumer._"
			sub, _ := nc.SubscribeSync(reply)
			defer sub.Unsubscribe()

			getSubj := o.requestNextMsgSubject()

			checkSubPending := func(numExpected int) {
				t.Helper()
				checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != numExpected {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
					}
					return nil
				})
			}

			// Create a formal request object.
			req := &JSApiConsumerGetNextRequest{Batch: toSend}
			jreq, _ := json.Marshal(req)
			nc.PublishRequest(getSubj, reply, jreq)

			checkSubPending(toSend)

			// Now check that we can ask for NoWait
			req.Batch = 1
			req.NoWait = true
			jreq, _ = json.Marshal(req)

			resp, err := nc.Request(getSubj, jreq, 100*time.Millisecond)
			require_NoError(t, err)
			if status := resp.Header.Get("Status"); !strings.HasPrefix(status, "404") {
				t.Fatalf("Expected status code of 404")
			}
			// Load up more messages.
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo", "Hello World!")
			}
			// Now we will ask for a batch larger then what is queued up.
			req.Batch = toSend + 10
			req.NoWait = true
			jreq, _ = json.Marshal(req)

			nc.PublishRequest(getSubj, reply, jreq)
			// We should now have 2 * toSend + the 404 message.
			checkSubPending(2*toSend + 1)
			for i := 0; i < 2*toSend+1; i++ {
				sub.NextMsg(time.Millisecond)
			}
			checkSubPending(0)
			mset.purge(nil)

			// Now do expiration
			req.Batch = 1
			req.NoWait = false
			req.Expires = 100 * time.Millisecond
			jreq, _ = json.Marshal(req)

			nc.PublishRequest(getSubj, reply, jreq)
			// Let it expire
			time.Sleep(200 * time.Millisecond)

			// Send a few more messages. These should not be delivered to the sub.
			sendStreamMsg(t, nc, "foo", "Hello World!")
			sendStreamMsg(t, nc, "bar", "Hello World!")
			time.Sleep(100 * time.Millisecond)

			// Expect the request timed out message.
			checkSubPending(1)
			if resp, _ = sub.NextMsg(time.Millisecond); resp == nil {
				t.Fatalf("Expected an expired status message")
			}
			if status := resp.Header.Get("Status"); !strings.HasPrefix(status, "408") {
				t.Fatalf("Expected status code of 408")
			}

			// Send a new request, we should not get the 408 because our previous request
			// should have expired.
			nc.PublishRequest(getSubj, reply, jreq)
			checkSubPending(1)
			sub.NextMsg(time.Second)
			checkSubPending(0)
		})
	}
}

func TestJetStreamSubjectFiltering(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MSET", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "MSET", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 50
			subjA := "foo.A"
			subjB := "foo.B"

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subjA, "Hello World!")
				sendStreamMsg(t, nc, subjB, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, state.Msgs)
			}

			delivery := nats.NewInbox()
			sub, _ := nc.SubscribeSync(delivery)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery, FilterSubject: subjB})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			// Now let's check the messages
			for i := 1; i <= toSend; i++ {
				m, err := sub.NextMsg(time.Second)
				require_NoError(t, err)
				// JetStream will have the subject match the stream subject, not delivery subject.
				// We want these to only be subjB.
				if m.Subject != subjB {
					t.Fatalf("Expected original subject of %q, but got %q", subjB, m.Subject)
				}
				// Now check that reply subject exists and has a sequence as the last token.
				if seq := o.seqFromReply(m.Reply); seq != uint64(i) {
					t.Fatalf("Expected sequence of %d , got %d", i, seq)
				}
				// Ack the message here.
				m.Respond(nil)
			}

			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 0 {
				t.Fatalf("Expected sub to have no pending")
			}
		})
	}
}

func TestJetStreamWorkQueueSubjectFiltering(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_MSG_SET", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "MY_MSG_SET", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 50
			subjA := "foo.A"
			subjB := "foo.B"

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subjA, "Hello World!")
				sendStreamMsg(t, nc, subjB, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, state.Msgs)
			}

			oname := "WQ"
			o, err := mset.addConsumer(&ConsumerConfig{Durable: oname, FilterSubject: subjA, AckPolicy: AckExplicit})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			if o.nextSeq() != 1 {
				t.Fatalf("Expected to be starting at sequence 1")
			}

			getNext := func(seqno int) {
				t.Helper()
				nextMsg, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				require_NoError(t, err)
				if nextMsg.Subject != subjA {
					t.Fatalf("Expected subject of %q, got %q", subjA, nextMsg.Subject)
				}
				if seq := o.seqFromReply(nextMsg.Reply); seq != uint64(seqno) {
					t.Fatalf("Expected sequence of %d , got %d", seqno, seq)
				}
				nextMsg.Respond(nil)
			}

			// Make sure we can get the messages already there.
			for i := 1; i <= toSend; i++ {
				getNext(i)
			}
		})
	}
}

func TestJetStreamWildcardSubjectFiltering(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "ORDERS", Storage: MemoryStorage, Subjects: []string{"orders.*.*"}}},
		{"FileStore", &StreamConfig{Name: "ORDERS", Storage: FileStorage, Subjects: []string{"orders.*.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 100
			for i := 1; i <= toSend; i++ {
				subj := fmt.Sprintf("orders.%d.%s", i, "NEW")
				sendStreamMsg(t, nc, subj, "new order")
			}
			// Randomly move 25 to shipped.
			toShip := 25
			shipped := make(map[int]bool)
			for i := 0; i < toShip; {
				orderId := rand.Intn(toSend-1) + 1
				if shipped[orderId] {
					continue
				}
				subj := fmt.Sprintf("orders.%d.%s", orderId, "SHIPPED")
				sendStreamMsg(t, nc, subj, "shipped order")
				shipped[orderId] = true
				i++
			}
			state := mset.state()
			if state.Msgs != uint64(toSend+toShip) {
				t.Fatalf("Expected %d messages, got %d", toSend+toShip, state.Msgs)
			}

			delivery := nats.NewInbox()
			sub, _ := nc.SubscribeSync(delivery)
			defer sub.Unsubscribe()
			nc.Flush()

			// Get all shipped.
			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery, FilterSubject: "orders.*.SHIPPED"})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			checkFor(t, time.Second, 25*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toShip {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toShip)
				}
				return nil
			})
			for nmsgs, _, _ := sub.Pending(); nmsgs > 0; nmsgs, _, _ = sub.Pending() {
				sub.NextMsg(time.Second)
			}
			if nmsgs, _, _ := sub.Pending(); nmsgs != 0 {
				t.Fatalf("Expected no pending, got %d", nmsgs)
			}

			// Get all new
			o, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery, FilterSubject: "orders.*.NEW"})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			checkFor(t, time.Second, 25*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
				}
				return nil
			})
			for nmsgs, _, _ := sub.Pending(); nmsgs > 0; nmsgs, _, _ = sub.Pending() {
				sub.NextMsg(time.Second)
			}
			if nmsgs, _, _ := sub.Pending(); nmsgs != 0 {
				t.Fatalf("Expected no pending, got %d", nmsgs)
			}

			// Now grab a single orderId that has shipped, so we should have two messages.
			var orderId int
			for orderId = range shipped {
				break
			}
			subj := fmt.Sprintf("orders.%d.*", orderId)
			o, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery, FilterSubject: subj})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			checkFor(t, time.Second, 25*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 2 {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, 2)
				}
				return nil
			})
		})
	}
}

func TestJetStreamWorkQueueAckAndNext(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_MSG_SET", Storage: MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &StreamConfig{Name: "MY_MSG_SET", Storage: FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// Create basic work queue mode consumer.
			oname := "WQ"
			o, err := mset.addConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			if o.nextSeq() != 1 {
				t.Fatalf("Expected to be starting at sequence 1")
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			sendSubj := "bar"
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			// Kick things off.
			// For normal work queue semantics, you send requests to the subject with stream and consumer name.
			// We will do this to start it off then use ack+next to get other messages.
			nc.PublishRequest(o.requestNextMsgSubject(), sub.Subject, nil)

			for i := 0; i < toSend; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error waiting for messages: %v", err)
				}

				if !bytes.Equal(m.Data, []byte("Hello World!")) {
					t.Fatalf("Got an invalid message from the stream: %q", m.Data)
				}

				nc.PublishRequest(m.Reply, sub.Subject, AckNext)
			}
		})
	}
}

func TestJetStreamWorkQueueRequestBatch(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_MSG_SET", Storage: MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &StreamConfig{Name: "MY_MSG_SET", Storage: FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// Create basic work queue mode consumer.
			oname := "WQ"
			o, err := mset.addConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			if o.nextSeq() != 1 {
				t.Fatalf("Expected to be starting at sequence 1")
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			sendSubj := "bar"
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			// For normal work queue semantics, you send requests to the subject with stream and consumer name.
			// We will do this to start it off then use ack+next to get other messages.
			// Kick things off with batch size of 50.
			batchSize := 50
			nc.PublishRequest(o.requestNextMsgSubject(), sub.Subject, []byte(strconv.Itoa(batchSize)))

			// We should receive batchSize with no acks or additional requests.
			checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != batchSize {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, batchSize)
				}
				return nil
			})

			// Now queue up the request without messages and add them after.
			sub, _ = nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			mset.purge(nil)

			nc.PublishRequest(o.requestNextMsgSubject(), sub.Subject, []byte(strconv.Itoa(batchSize)))
			nc.Flush() // Make sure its registered.

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, "Hello World!")
			}

			// We should receive batchSize with no acks or additional requests.
			checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != batchSize {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, batchSize)
				}
				return nil
			})
		})
	}
}

func TestJetStreamWorkQueueRetentionStream(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore", mconfig: &StreamConfig{
			Name:      "MWQ",
			Storage:   MemoryStorage,
			Subjects:  []string{"MY_WORK_QUEUE.>"},
			Retention: WorkQueuePolicy},
		},
		{name: "FileStore", mconfig: &StreamConfig{
			Name:      "MWQ",
			Storage:   FileStorage,
			Subjects:  []string{"MY_WORK_QUEUE.>"},
			Retention: WorkQueuePolicy},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// This type of stream has restrictions which we will test here.
			// DeliverAll is only start mode allowed.
			if _, err := mset.addConsumer(&ConsumerConfig{DeliverPolicy: DeliverLast}); err == nil {
				t.Fatalf("Expected an error with anything but DeliverAll")
			}

			// We will create a non-partitioned consumer. This should succeed.
			o, err := mset.addConsumer(&ConsumerConfig{Durable: "PBO", AckPolicy: AckExplicit})
			require_NoError(t, err)
			defer o.delete()

			// Now if we create another this should fail, only can have one non-partitioned.
			if _, err := mset.addConsumer(&ConsumerConfig{}); err == nil {
				t.Fatalf("Expected an error on attempt for second consumer for a workqueue")
			}
			o.delete()

			if numo := mset.numConsumers(); numo != 0 {
				t.Fatalf("Expected to have zero consumers, got %d", numo)
			}

			// Now add in an consumer that has a partition.
			pindex := 1
			pConfig := func(pname string) *ConsumerConfig {
				dname := fmt.Sprintf("PPBO-%d", pindex)
				pindex += 1
				return &ConsumerConfig{Durable: dname, FilterSubject: pname, AckPolicy: AckExplicit}
			}
			o, err = mset.addConsumer(pConfig("MY_WORK_QUEUE.A"))
			require_NoError(t, err)
			defer o.delete()

			// Now creating another with separate partition should work.
			o2, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.B"))
			require_NoError(t, err)
			defer o2.delete()

			// Anything that would overlap should fail though.
			if _, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.A")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}
			if _, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.B")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}

			o3, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.C"))
			require_NoError(t, err)

			o.delete()
			o2.delete()
			o3.delete()

			// Test with wildcards, first from wider to narrower
			o, err = mset.addConsumer(pConfig("MY_WORK_QUEUE.>"))
			require_NoError(t, err)
			if _, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.*.BAR")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}
			o.delete()

			// Now from narrower to wider
			o, err = mset.addConsumer(pConfig("MY_WORK_QUEUE.*.BAR"))
			require_NoError(t, err)
			if _, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.>")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}
			o.delete()

			// Push based will be allowed now, including ephemerals.
			// They can not overlap etc meaning same rules as above apply.
			o4, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "DURABLE",
				DeliverSubject: "SOME.SUBJ",
				AckPolicy:      AckExplicit,
			})
			if err != nil {
				t.Fatalf("Unexpected Error: %v", err)
			}
			defer o4.delete()

			// Now try to create an ephemeral
			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			// This should fail at first due to conflict above.
			ephCfg := &ConsumerConfig{DeliverSubject: sub.Subject, AckPolicy: AckExplicit}
			if _, err := mset.addConsumer(ephCfg); err == nil {
				t.Fatalf("Expected an error ")
			}
			// Delete of o4 should clear.
			o4.delete()
			o5, err := mset.addConsumer(ephCfg)
			if err != nil {
				t.Fatalf("Unexpected Error: %v", err)
			}
			defer o5.delete()
		})
	}
}

func TestJetStreamAckAllRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_S22", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "MY_S22", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{
				DeliverSubject: sub.Subject,
				AckWait:        50 * time.Millisecond,
				AckPolicy:      AckAll,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o.delete()

			// Wait for messages.
			// We will do 5 redeliveries.
			for i := 1; i <= 5; i++ {
				checkFor(t, 500*time.Millisecond, 10*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend*i {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend*i)
					}
					return nil
				})
			}
			// Stop redeliveries.
			o.delete()

			// Now make sure that they are all redelivered in order for each redelivered batch.
			for l := 1; l <= 5; l++ {
				for i := 1; i <= toSend; i++ {
					m, _ := sub.NextMsg(time.Second)
					if seq := o.streamSeqFromReply(m.Reply); seq != uint64(i) {
						t.Fatalf("Expected stream sequence of %d, got %d", i, seq)
					}
				}
			}
		})
	}
}

func TestJetStreamAckReplyStreamPending(t *testing.T) {
	msc := StreamConfig{
		Name:      "MY_WQ",
		Subjects:  []string{"foo.*"},
		Storage:   MemoryStorage,
		MaxAge:    250 * time.Millisecond,
		Retention: WorkQueuePolicy,
	}
	fsc := msc
	fsc.Storage = FileStorage

	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &msc},
		{"FileStore", &fsc},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.1", "Hello World!")
			}
			nc.Flush()

			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			o, err := mset.addConsumer(&ConsumerConfig{Durable: "PBO", AckPolicy: AckExplicit})
			require_NoError(t, err)
			defer o.delete()

			expectPending := func(ep int) {
				t.Helper()
				// Now check consumer info.
				checkFor(t, time.Second, 10*time.Millisecond, func() error {
					if info, pep := o.info(), ep+1; int(info.NumPending) != pep {
						return fmt.Errorf("Expected consumer info pending of %d, got %d", pep, info.NumPending)
					}
					return nil
				})
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				_, _, _, _, pending := replyInfo(m.Reply)
				if pending != uint64(ep) {
					t.Fatalf("Expected ack reply pending of %d, got %d - reply: %q", ep, pending, m.Reply)
				}
			}

			expectPending(toSend - 1)
			// Send some more while we are connected.
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.1", "Hello World!")
			}
			nc.Flush()

			expectPending(toSend*2 - 2)
			// Purge and send a new one.
			mset.purge(nil)
			nc.Flush()

			sendStreamMsg(t, nc, "foo.1", "Hello World!")
			expectPending(0)
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.22", "Hello World!")
			}
			expectPending(toSend - 1) // 201
			// Test that delete will not register for consumed messages.
			mset.removeMsg(mset.state().FirstSeq)
			expectPending(toSend - 2) // 202
			// Now remove one that has not been delivered.
			mset.removeMsg(250)
			expectPending(toSend - 4) // 203

			// Test Expiration.
			mset.purge(nil)
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.1", "Hello World!")
			}
			nc.Flush()

			// Wait for expiration to kick in.
			checkFor(t, time.Second, 10*time.Millisecond, func() error {
				if state := mset.state(); state.Msgs != 0 {
					return fmt.Errorf("Stream still has messages")
				}
				return nil
			})
			sendStreamMsg(t, nc, "foo.33", "Hello World!")
			expectPending(0)

			// Now do filtered consumers.
			o.delete()
			o, err = mset.addConsumer(&ConsumerConfig{Durable: "PBO-FILTERED", AckPolicy: AckExplicit, FilterSubject: "foo.22"})
			require_NoError(t, err)
			defer o.delete()

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.33", "Hello World!")
			}
			nc.Flush()

			if info := o.info(); info.NumPending != 0 {
				t.Fatalf("Expected no pending, got %d", info.NumPending)
			}
			// Now send one message that will match us.
			sendStreamMsg(t, nc, "foo.22", "Hello World!")
			expectPending(0)
			sendStreamMsg(t, nc, "foo.22", "Hello World!") // 504
			sendStreamMsg(t, nc, "foo.22", "Hello World!") // 505
			sendStreamMsg(t, nc, "foo.22", "Hello World!") // 506
			sendStreamMsg(t, nc, "foo.22", "Hello World!") // 507
			expectPending(3)
			mset.removeMsg(506)
			expectPending(1)
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.22", "Hello World!")
			}
			nc.Flush()
			expectPending(100)
			mset.purge(nil)
			sendStreamMsg(t, nc, "foo.22", "Hello World!")
			expectPending(0)
		})
	}
}

func TestJetStreamAckReplyStreamPendingWithAcks(t *testing.T) {
	msc := StreamConfig{
		Name:     "MY_STREAM",
		Subjects: []string{"foo", "bar", "baz"},
		Storage:  MemoryStorage,
	}
	fsc := msc
	fsc.Storage = FileStorage

	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &msc},
		{"FileStore", &fsc},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 500
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo", "Hello Foo!")
				sendStreamMsg(t, nc, "bar", "Hello Bar!")
				sendStreamMsg(t, nc, "baz", "Hello Baz!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend*3) {
				t.Fatalf("Expected %d messages, got %d", toSend*3, state.Msgs)
			}
			dsubj := "_d_"
			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "D-1",
				AckPolicy:      AckExplicit,
				FilterSubject:  "foo",
				DeliverSubject: dsubj,
			})
			require_NoError(t, err)
			defer o.delete()

			if info := o.info(); int(info.NumPending) != toSend {
				t.Fatalf("Expected consumer info pending of %d, got %d", toSend, info.NumPending)
			}

			sub, _ := nc.SubscribeSync(dsubj)
			defer sub.Unsubscribe()

			checkFor(t, 500*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
				}
				return nil
			})

			// Should be zero.
			if info := o.info(); int(info.NumPending) != 0 {
				t.Fatalf("Expected consumer info pending of %d, got %d", 0, info.NumPending)
			} else if info.NumAckPending != toSend {
				t.Fatalf("Expected %d to be pending acks, got %d", toSend, info.NumAckPending)
			}
		})
	}
}

func TestJetStreamWorkQueueAckWaitRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_WQ", Storage: MemoryStorage, Retention: WorkQueuePolicy}},
		{"FileStore", &StreamConfig{Name: "MY_WQ", Storage: FileStorage, Retention: WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			ackWait := 100 * time.Millisecond

			o, err := mset.addConsumer(&ConsumerConfig{Durable: "PBO", AckPolicy: AckExplicit, AckWait: ackWait})
			require_NoError(t, err)
			defer o.delete()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			reqNextMsgSubj := o.requestNextMsgSubject()

			// Consume all the messages. But do not ack.
			for i := 0; i < toSend; i++ {
				nc.PublishRequest(reqNextMsgSubj, sub.Subject, nil)
				if _, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error waiting for messages: %v", err)
				}
			}

			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 0 {
				t.Fatalf("Did not consume all messages, still have %d", nmsgs)
			}

			// All messages should still be there.
			state = mset.state()
			if int(state.Msgs) != toSend {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			// Now consume and ack.
			for i := 1; i <= toSend; i++ {
				nc.PublishRequest(reqNextMsgSubj, sub.Subject, nil)
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error waiting for message[%d]: %v", i, err)
				}
				sseq, dseq, dcount, _, _ := replyInfo(m.Reply)
				if sseq != uint64(i) {
					t.Fatalf("Expected set sequence of %d , got %d", i, sseq)
				}
				// Delivery sequences should always increase.
				if dseq != uint64(toSend+i) {
					t.Fatalf("Expected delivery sequence of %d , got %d", toSend+i, dseq)
				}
				if dcount == 1 {
					t.Fatalf("Expected these to be marked as redelivered")
				}
				// Ack the message here.
				m.AckSync()
			}

			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 0 {
				t.Fatalf("Did not consume all messages, still have %d", nmsgs)
			}

			// Flush acks
			nc.Flush()

			// Now check the mset as well, since we have a WorkQueue retention policy this should be empty.
			if state := mset.state(); state.Msgs != 0 {
				t.Fatalf("Expected no messages, got %d", state.Msgs)
			}
		})
	}
}

func TestJetStreamWorkQueueNakRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_WQ", Storage: MemoryStorage, Retention: WorkQueuePolicy}},
		{"FileStore", &StreamConfig{Name: "MY_WQ", Storage: FileStorage, Retention: WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 10
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			o, err := mset.addConsumer(&ConsumerConfig{Durable: "PBO", AckPolicy: AckExplicit})
			require_NoError(t, err)
			defer o.delete()

			getMsg := func(sseq, dseq int) *nats.Msg {
				t.Helper()
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, rdseq, _, _, _ := replyInfo(m.Reply)
				if rdseq != uint64(dseq) {
					t.Fatalf("Expected delivered sequence of %d , got %d", dseq, rdseq)
				}
				if rsseq != uint64(sseq) {
					t.Fatalf("Expected store sequence of %d , got %d", sseq, rsseq)
				}
				return m
			}

			for i := 1; i <= 5; i++ {
				m := getMsg(i, i)
				// Ack the message here.
				m.Respond(nil)
			}

			// Grab #6
			m := getMsg(6, 6)
			// NAK this one and make sure its processed.
			m.Respond(AckNak)
			nc.Flush()

			// When we request again should be store sequence 6 again.
			getMsg(6, 7)
			// Then we should get 7, 8, etc.
			getMsg(7, 8)
			getMsg(8, 9)
		})
	}
}

func TestJetStreamWorkQueueWorkingIndicator(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_WQ", Storage: MemoryStorage, Retention: WorkQueuePolicy}},
		{"FileStore", &StreamConfig{Name: "MY_WQ", Storage: FileStorage, Retention: WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 2
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			ackWait := 100 * time.Millisecond

			o, err := mset.addConsumer(&ConsumerConfig{Durable: "PBO", AckPolicy: AckExplicit, AckWait: ackWait})
			require_NoError(t, err)
			defer o.delete()

			getMsg := func(sseq, dseq int) *nats.Msg {
				t.Helper()
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, rdseq, _, _, _ := replyInfo(m.Reply)
				if rdseq != uint64(dseq) {
					t.Fatalf("Expected delivered sequence of %d , got %d", dseq, rdseq)
				}
				if rsseq != uint64(sseq) {
					t.Fatalf("Expected store sequence of %d , got %d", sseq, rsseq)
				}
				return m
			}

			getMsg(1, 1)
			// Now wait past ackWait
			time.Sleep(ackWait * 2)

			// We should get 1 back.
			m := getMsg(1, 2)

			// Now let's take longer than ackWait to process but signal we are working on the message.
			timeout := time.Now().Add(3 * ackWait)
			for time.Now().Before(timeout) {
				m.Respond(AckProgress)
				nc.Flush()
				time.Sleep(ackWait / 5)
			}
			// We should get 2 here, not 1 since we have indicated we are working on it.
			m2 := getMsg(2, 3)
			time.Sleep(ackWait / 2)
			m2.Respond(AckProgress)

			// Now should get 1 back then 2.
			m = getMsg(1, 4)
			m.Respond(nil)
			getMsg(2, 5)
		})
	}
}

func TestJetStreamWorkQueueTerminateDelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_WQ", Storage: MemoryStorage, Retention: WorkQueuePolicy}},
		{"FileStore", &StreamConfig{Name: "MY_WQ", Storage: FileStorage, Retention: WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 22
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			ackWait := 25 * time.Millisecond

			o, err := mset.addConsumer(&ConsumerConfig{Durable: "PBO", AckPolicy: AckExplicit, AckWait: ackWait})
			require_NoError(t, err)
			defer o.delete()

			getMsg := func(sseq, dseq int) *nats.Msg {
				t.Helper()
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, rdseq, _, _, _ := replyInfo(m.Reply)
				if rdseq != uint64(dseq) {
					t.Fatalf("Expected delivered sequence of %d , got %d", dseq, rdseq)
				}
				if rsseq != uint64(sseq) {
					t.Fatalf("Expected store sequence of %d , got %d", sseq, rsseq)
				}
				return m
			}

			// Make sure we get the correct advisory
			sub, _ := nc.SubscribeSync(JSAdvisoryConsumerMsgTerminatedPre + ".>")
			defer sub.Unsubscribe()

			getMsg(1, 1)
			// Now wait past ackWait
			time.Sleep(ackWait * 2)

			// We should get 1 back.
			m := getMsg(1, 2)
			// Now terminate
			m.Respond(AckTerm)
			time.Sleep(ackWait * 2)

			// We should get 2 here, not 1 since we have indicated we wanted to terminate.
			getMsg(2, 3)

			// Check advisory was delivered.
			am, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var adv JSConsumerDeliveryTerminatedAdvisory
			json.Unmarshal(am.Data, &adv)
			if adv.Stream != "MY_WQ" {
				t.Fatalf("Expected stream of %s, got %s", "MY_WQ", adv.Stream)
			}
			if adv.Consumer != "PBO" {
				t.Fatalf("Expected consumer of %s, got %s", "PBO", adv.Consumer)
			}
			if adv.StreamSeq != 1 {
				t.Fatalf("Expected stream sequence of %d, got %d", 1, adv.StreamSeq)
			}
			if adv.ConsumerSeq != 2 {
				t.Fatalf("Expected consumer sequence of %d, got %d", 2, adv.ConsumerSeq)
			}
			if adv.Deliveries != 2 {
				t.Fatalf("Expected delivery count of %d, got %d", 2, adv.Deliveries)
			}
		})
	}
}

func TestJetStreamConsumerAckAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	mname := "ACK-ACK"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	o, err := mset.addConsumer(&ConsumerConfig{Durable: "worker", AckPolicy: AckExplicit})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()
	rqn := o.requestNextMsgSubject()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// 4 for number of ack protocols to test them all.
	for i := 0; i < 4; i++ {
		sendStreamMsg(t, nc, mname, "Hello World!")
	}

	testAck := func(ackType []byte) {
		m, err := nc.Request(rqn, nil, 10*time.Millisecond)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Send a request for the ack and make sure the server "ack's" the ack.
		if _, err := nc.Request(m.Reply, ackType, 10*time.Millisecond); err != nil {
			t.Fatalf("Unexpected error on ack/ack: %v", err)
		}
	}

	testAck(AckAck)
	testAck(AckNak)
	testAck(AckProgress)
	testAck(AckTerm)
}

func TestJetStreamAckNext(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "ACKNXT"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	o, err := mset.addConsumer(&ConsumerConfig{Durable: "worker", AckPolicy: AckExplicit})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	for i := 0; i < 12; i++ {
		sendStreamMsg(t, nc, mname, fmt.Sprintf("msg %d", i))
	}

	q := make(chan *nats.Msg, 10)
	sub, err := nc.ChanSubscribe(nats.NewInbox(), q)
	if err != nil {
		t.Fatalf("SubscribeSync failed: %s", err)
	}

	nc.PublishRequest(o.requestNextMsgSubject(), sub.Subject, []byte("1"))

	// normal next should imply 1
	msg := <-q
	err = msg.RespondMsg(&nats.Msg{Reply: sub.Subject, Subject: msg.Reply, Data: AckNext})
	if err != nil {
		t.Fatalf("RespondMsg failed: %s", err)
	}

	// read 1 message and check ack was done etc
	msg = <-q
	if len(q) != 0 {
		t.Fatalf("Expected empty q got %d", len(q))
	}
	if o.info().AckFloor.Stream != 1 {
		t.Fatalf("First message was not acknowledged")
	}
	if !bytes.Equal(msg.Data, []byte("msg 1")) {
		t.Fatalf("wrong message received, expected: msg 1 got %q", msg.Data)
	}

	// now ack and request 5 more using a naked number
	err = msg.RespondMsg(&nats.Msg{Reply: sub.Subject, Subject: msg.Reply, Data: append(AckNext, []byte(" 5")...)})
	if err != nil {
		t.Fatalf("RespondMsg failed: %s", err)
	}

	getMsgs := func(start, count int) {
		t.Helper()

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		for i := start; i < count+1; i++ {
			select {
			case msg := <-q:
				expect := fmt.Sprintf("msg %d", i+1)
				if !bytes.Equal(msg.Data, []byte(expect)) {
					t.Fatalf("wrong message received, expected: %s got %#v", expect, msg)
				}
			case <-ctx.Done():
				t.Fatalf("did not receive all messages")
			}
		}

	}

	getMsgs(1, 5)

	// now ack and request 5 more using the full request
	err = msg.RespondMsg(&nats.Msg{Reply: sub.Subject, Subject: msg.Reply, Data: append(AckNext, []byte(`{"batch": 5}`)...)})
	if err != nil {
		t.Fatalf("RespondMsg failed: %s", err)
	}

	getMsgs(6, 10)

	if o.info().AckFloor.Stream != 2 {
		t.Fatalf("second message was not acknowledged")
	}
}

func TestJetStreamPublishDeDupe(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "DeDupe"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: FileStorage, MaxAge: time.Hour, Subjects: []string{"foo.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	// Check Duplicates setting.
	duplicates := mset.config().Duplicates
	if duplicates != StreamDefaultDuplicatesWindow {
		t.Fatalf("Expected a default of %v, got %v", StreamDefaultDuplicatesWindow, duplicates)
	}

	cfg := mset.config()
	// Make sure can't be negative.
	cfg.Duplicates = -25 * time.Millisecond
	if err := mset.update(&cfg); err == nil {
		t.Fatalf("Expected an error but got none")
	}
	// Make sure can't be longer than age if its set.
	cfg.Duplicates = 2 * time.Hour
	if err := mset.update(&cfg); err == nil {
		t.Fatalf("Expected an error but got none")
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sendMsg := func(seq uint64, id, msg string) *PubAck {
		t.Helper()
		m := nats.NewMsg(fmt.Sprintf("foo.%d", seq))
		m.Header.Add(JSMsgId, id)
		m.Data = []byte(msg)
		resp, _ := nc.RequestMsg(m, 100*time.Millisecond)
		if resp == nil {
			t.Fatalf("No response for %q, possible timeout?", msg)
		}
		pa := getPubAckResponse(resp.Data)
		if pa == nil || pa.Error != nil {
			t.Fatalf("Expected a JetStreamPubAck, got %q", resp.Data)
		}
		if pa.Sequence != seq {
			t.Fatalf("Did not get correct sequence in PubAck, expected %d, got %d", seq, pa.Sequence)
		}
		return pa.PubAck
	}

	expect := func(n uint64) {
		t.Helper()
		state := mset.state()
		if state.Msgs != n {
			t.Fatalf("Expected %d messages, got %d", n, state.Msgs)
		}
	}

	sendMsg(1, "AA", "Hello DeDupe!")
	sendMsg(2, "BB", "Hello DeDupe!")
	sendMsg(3, "CC", "Hello DeDupe!")
	sendMsg(4, "ZZ", "Hello DeDupe!")
	expect(4)

	sendMsg(1, "AA", "Hello DeDupe!")
	sendMsg(2, "BB", "Hello DeDupe!")
	sendMsg(4, "ZZ", "Hello DeDupe!")
	expect(4)

	cfg = mset.config()
	cfg.Duplicates = 100 * time.Millisecond
	if err := mset.update(&cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	nmids := func(expected int) {
		t.Helper()
		checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
			if nids := mset.numMsgIds(); nids != expected {
				return fmt.Errorf("Expected %d message ids, got %d", expected, nids)
			}
			return nil
		})
	}

	nmids(4)
	time.Sleep(cfg.Duplicates * 2)

	sendMsg(5, "AAA", "Hello DeDupe!")
	sendMsg(6, "BBB", "Hello DeDupe!")
	sendMsg(7, "CCC", "Hello DeDupe!")
	sendMsg(8, "DDD", "Hello DeDupe!")
	sendMsg(9, "ZZZ", "Hello DeDupe!")
	nmids(5)
	// Eventually will drop to zero.
	nmids(0)

	// Now test server restart
	cfg.Duplicates = 30 * time.Minute
	if err := mset.update(&cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	mset.purge(nil)

	// Send 5 new messages.
	sendMsg(10, "AAAA", "Hello DeDupe!")
	sendMsg(11, "BBBB", "Hello DeDupe!")
	sendMsg(12, "CCCC", "Hello DeDupe!")
	sendMsg(13, "DDDD", "Hello DeDupe!")
	sendMsg(14, "EEEE", "Hello DeDupe!")

	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc = clientConnectToServer(t, s)
	defer nc.Close()

	mset, _ = s.GlobalAccount().lookupStream(mname)
	if nms := mset.state().Msgs; nms != 5 {
		t.Fatalf("Expected 5 restored messages, got %d", nms)
	}
	nmids(5)

	// Send same and make sure duplicate detection still works.
	// Send 5 duplicate messages.
	sendMsg(10, "AAAA", "Hello DeDupe!")
	sendMsg(11, "BBBB", "Hello DeDupe!")
	sendMsg(12, "CCCC", "Hello DeDupe!")
	sendMsg(13, "DDDD", "Hello DeDupe!")
	sendMsg(14, "EEEE", "Hello DeDupe!")

	if nms := mset.state().Msgs; nms != 5 {
		t.Fatalf("Expected 5 restored messages, got %d", nms)
	}
	nmids(5)

	// Check we set duplicate properly.
	pa := sendMsg(10, "AAAA", "Hello DeDupe!")
	if !pa.Duplicate {
		t.Fatalf("Expected duplicate to be set")
	}

	// Purge should NOT wipe the msgIds. They should still persist.
	mset.purge(nil)
	nmids(5)
}

func getPubAckResponse(msg []byte) *JSPubAckResponse {
	var par JSPubAckResponse
	if err := json.Unmarshal(msg, &par); err != nil {
		return nil
	}
	return &par
}

func TestJetStreamPublishExpect(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "EXPECT"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: FileStorage, MaxAge: time.Hour, Subjects: []string{"foo.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Test that we get no error when expected stream is correct.
	m := nats.NewMsg("foo.bar")
	m.Data = []byte("HELLO")
	m.Header.Set(JSExpectedStream, mname)
	resp, err := nc.RequestMsg(m, 100*time.Millisecond)
	require_NoError(t, err)
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error != nil {
		t.Fatalf("Expected a valid JetStreamPubAck, got %q", resp.Data)
	}

	// Now test that we get an error back when expecting a different stream.
	m.Header.Set(JSExpectedStream, "ORDERS")
	resp, err = nc.RequestMsg(m, 100*time.Millisecond)
	require_NoError(t, err)
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error == nil {
		t.Fatalf("Expected an error, got %q", resp.Data)
	}

	// Now test that we get an error back when expecting a different sequence number.
	m.Header.Set(JSExpectedStream, mname)
	m.Header.Set(JSExpectedLastSeq, "10")
	resp, err = nc.RequestMsg(m, 100*time.Millisecond)
	require_NoError(t, err)
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error == nil {
		t.Fatalf("Expected an error, got %q", resp.Data)
	}

	// Or if we expect that there are no messages by setting "0" as the expected last seq
	m.Header.Set(JSExpectedLastSeq, "0")
	resp, err = nc.RequestMsg(m, 100*time.Millisecond)
	require_NoError(t, err)
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error == nil {
		t.Fatalf("Expected an error, got %q", resp.Data)
	}

	// Now send a message with a message ID and make sure we can match that.
	m = nats.NewMsg("foo.bar")
	m.Data = []byte("HELLO")
	m.Header.Set(JSMsgId, "AAA")
	if _, err = nc.RequestMsg(m, 100*time.Millisecond); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now try again with new message ID but require last one to be 'BBB'
	m.Header.Set(JSMsgId, "ZZZ")
	m.Header.Set(JSExpectedLastMsgId, "BBB")
	resp, err = nc.RequestMsg(m, 100*time.Millisecond)
	require_NoError(t, err)
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error == nil {
		t.Fatalf("Expected an error, got %q", resp.Data)
	}

	// Restart the server and make sure we remember/rebuild last seq and last msgId.
	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc = clientConnectToServer(t, s)
	defer nc.Close()

	// Our last sequence was 2 and last msgId was "AAA"
	m = nats.NewMsg("foo.baz")
	m.Data = []byte("HELLO AGAIN")
	m.Header.Set(JSExpectedLastSeq, "2")
	m.Header.Set(JSExpectedLastMsgId, "AAA")
	m.Header.Set(JSMsgId, "BBB")
	resp, err = nc.RequestMsg(m, 100*time.Millisecond)
	require_NoError(t, err)
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error != nil {
		t.Fatalf("Expected a valid JetStreamPubAck, got %q", resp.Data)
	}
}

func TestJetStreamPullConsumerRemoveInterest(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "MYS-PULL"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	wcfg := &ConsumerConfig{Durable: "worker", AckPolicy: AckExplicit}
	o, err := mset.addConsumer(wcfg)
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	rqn := o.requestNextMsgSubject()
	defer o.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Ask for a message even though one is not there. This will queue us up for waiting.
	if _, err := nc.Request(rqn, nil, 10*time.Millisecond); err == nil {
		t.Fatalf("Expected an error, got none")
	}

	// This is using new style request mechanism. so drop the connection itself to get rid of interest.
	nc.Close()

	// Wait for client cleanup
	checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
		if n := s.NumClients(); err != nil || n != 0 {
			return fmt.Errorf("Still have %d clients", n)
		}
		return nil
	})

	nc = clientConnectToServer(t, s)
	defer nc.Close()

	// Send a message
	sendStreamMsg(t, nc, mname, "Hello World!")

	msg, err := nc.Request(rqn, nil, time.Second)
	require_NoError(t, err)
	_, dseq, dc, _, _ := replyInfo(msg.Reply)
	if dseq != 1 {
		t.Fatalf("Expected consumer sequence of 1, got %d", dseq)
	}
	if dc != 1 {
		t.Fatalf("Expected delivery count of 1, got %d", dc)
	}

	// Now do old school request style and more than one waiting.
	nc = clientConnectWithOldRequest(t, s)
	defer nc.Close()

	// Now queue up 10 waiting via failed requests.
	for i := 0; i < 10; i++ {
		if _, err := nc.Request(rqn, nil, 1*time.Millisecond); err == nil {
			t.Fatalf("Expected an error, got none")
		}
	}

	// Send a second message
	sendStreamMsg(t, nc, mname, "Hello World!")

	msg, err = nc.Request(rqn, nil, time.Second)
	require_NoError(t, err)
	_, dseq, dc, _, _ = replyInfo(msg.Reply)
	if dseq != 2 {
		t.Fatalf("Expected consumer sequence of 2, got %d", dseq)
	}
	if dc != 1 {
		t.Fatalf("Expected delivery count of 1, got %d", dc)
	}
}

func TestJetStreamConsumerRateLimit(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "RATELIMIT"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	msgSize := 128 * 1024
	msg := make([]byte, msgSize)
	crand.Read(msg)

	// 10MB
	totalSize := 10 * 1024 * 1024
	toSend := totalSize / msgSize
	for i := 0; i < toSend; i++ {
		nc.Publish(mname, msg)
	}
	nc.Flush()

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		state := mset.state()
		if state.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected %d messages, got %d", toSend, state.Msgs)
		}
		return nil
	})

	// 100Mbit
	rateLimit := uint64(100 * 1024 * 1024)
	// Make sure if you set a rate with a pull based consumer it errors.
	_, err = mset.addConsumer(&ConsumerConfig{Durable: "to", AckPolicy: AckExplicit, RateLimit: rateLimit})
	if err == nil {
		t.Fatalf("Expected an error, got none")
	}

	// Now create one and measure the rate delivered.
	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:        "rate",
		DeliverSubject: "to",
		RateLimit:      rateLimit,
		AckPolicy:      AckNone})
	require_NoError(t, err)
	defer o.delete()

	var received int
	done := make(chan bool)

	start := time.Now()

	nc.Subscribe("to", func(m *nats.Msg) {
		received++
		if received >= toSend {
			done <- true
		}
	})
	nc.Flush()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive all the messages in time")
	}

	tt := time.Since(start)
	rate := float64(8*toSend*msgSize) / tt.Seconds()
	if rate > float64(rateLimit)*1.25 {
		t.Fatalf("Exceeded desired rate of %d mbps, got %0.f mbps", rateLimit/(1024*1024), rate/(1024*1024))
	}
}

func TestJetStreamEphemeralConsumerRecoveryAfterServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "MYS"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sub, _ := nc.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()
	nc.Flush()

	o, err := mset.addConsumer(&ConsumerConfig{
		DeliverSubject: sub.Subject,
		AckPolicy:      AckExplicit,
	})
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}
	defer o.delete()

	// Snapshot our name.
	oname := o.String()

	// Send 100 messages
	for i := 0; i < 100; i++ {
		sendStreamMsg(t, nc, mname, "Hello World!")
	}
	if state := mset.state(); state.Msgs != 100 {
		t.Fatalf("Expected %d messages, got %d", 100, state.Msgs)
	}

	// Read 6 messages
	for i := 0; i <= 6; i++ {
		if m, err := sub.NextMsg(time.Second); err == nil {
			m.Respond(nil)
		} else {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())

	restartServer := func() {
		t.Helper()
		// Stop current
		sd := s.JetStreamConfig().StoreDir
		s.Shutdown()
		// Restart.
		s = RunJetStreamServerOnPort(port, sd)
	}

	// Do twice
	for i := 0; i < 2; i++ {
		// Restart.
		restartServer()
		defer s.Shutdown()

		mset, err = s.GlobalAccount().lookupStream(mname)
		if err != nil {
			t.Fatalf("Expected to find a stream for %q", mname)
		}
		o = mset.lookupConsumer(oname)
		if o == nil {
			t.Fatalf("Error looking up consumer %q", oname)
		}
		// Make sure config does not have durable.
		if cfg := o.config(); cfg.Durable != _EMPTY_ {
			t.Fatalf("Expected no durable to be set")
		}
		// Wait for it to become active
		checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
			if !o.isActive() {
				return fmt.Errorf("Consumer not active")
			}
			return nil
		})
	}

	// Now close the connection. Make sure this acts like an ephemeral and goes away.
	o.setInActiveDeleteThreshold(10 * time.Millisecond)
	nc.Close()

	checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
		if o := mset.lookupConsumer(oname); o != nil {
			return fmt.Errorf("Consumer still active")
		}
		return nil
	})
}

func TestJetStreamConsumerMaxDeliveryAndServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "MYS"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	streamCreated := mset.createdTime()

	dsubj := "D.TO"
	max := 3

	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:        "TO",
		DeliverSubject: dsubj,
		AckPolicy:      AckExplicit,
		AckWait:        100 * time.Millisecond,
		MaxDeliver:     max,
	})
	defer o.delete()

	consumerCreated := o.createdTime()
	// For calculation of consumer created times below.
	time.Sleep(5 * time.Millisecond)

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sub, _ := nc.SubscribeSync(dsubj)
	nc.Flush()
	defer sub.Unsubscribe()

	// Send one message.
	sendStreamMsg(t, nc, mname, "order-1")

	checkSubPending := func(numExpected int) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if nmsgs, _, _ := sub.Pending(); nmsgs != numExpected {
				return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
			}
			return nil
		})
	}

	checkNumMsgs := func(numExpected uint64) {
		t.Helper()
		mset, err = s.GlobalAccount().lookupStream(mname)
		if err != nil {
			t.Fatalf("Expected to find a stream for %q", mname)
		}
		state := mset.state()
		if state.Msgs != numExpected {
			t.Fatalf("Expected %d msgs, got %d", numExpected, state.Msgs)
		}
	}

	// Wait til we know we have max queued up.
	checkSubPending(max)

	// Once here we have gone over the limit for the 1st message for max deliveries.
	// Send second
	sendStreamMsg(t, nc, mname, "order-2")

	// Just wait for first delivery + one redelivery.
	checkSubPending(max + 2)

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())

	restartServer := func() {
		t.Helper()
		sd := s.JetStreamConfig().StoreDir
		// Stop current
		s.Shutdown()
		// Restart.
		s = RunJetStreamServerOnPort(port, sd)
	}

	waitForClientReconnect := func() {
		checkFor(t, 2500*time.Millisecond, 5*time.Millisecond, func() error {
			if !nc.IsConnected() {
				return fmt.Errorf("Not connected")
			}
			return nil
		})
	}

	// Restart.
	restartServer()
	defer s.Shutdown()

	checkNumMsgs(2)

	// Wait for client to be reconnected.
	waitForClientReconnect()

	// Once we are here send third order.
	sendStreamMsg(t, nc, mname, "order-3")
	checkNumMsgs(3)

	// Restart.
	restartServer()
	defer s.Shutdown()

	checkNumMsgs(3)

	// Wait for client to be reconnected.
	waitForClientReconnect()

	// Now we should have max times three on our sub.
	checkSubPending(max * 3)

	// Now do some checks on created timestamps.
	mset, err = s.GlobalAccount().lookupStream(mname)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", mname)
	}
	if mset.createdTime() != streamCreated {
		t.Fatalf("Stream creation time not restored, wanted %v, got %v", streamCreated, mset.createdTime())
	}
	o = mset.lookupConsumer("TO")
	if o == nil {
		t.Fatalf("Error looking up consumer: %v", err)
	}
	// Consumer created times can have a very small skew.
	delta := o.createdTime().Sub(consumerCreated)
	if delta > 5*time.Millisecond {
		t.Fatalf("Consumer creation time not restored, wanted %v, got %v", consumerCreated, o.createdTime())
	}
}

func TestJetStreamDeleteConsumerAndServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	sendSubj := "MYQ"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: sendSubj, Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	// Create basic work queue mode consumer.
	oname := "WQ"
	o, err := mset.addConsumer(workerModeConfig(oname))
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}

	// Now delete and then we will restart the
	o.delete()

	if numo := mset.numConsumers(); numo != 0 {
		t.Fatalf("Expected to have zero consumers, got %d", numo)
	}

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())
	sd := s.JetStreamConfig().StoreDir

	// Stop current
	s.Shutdown()

	// Restart.
	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()

	mset, err = s.GlobalAccount().lookupStream(sendSubj)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", sendSubj)
	}

	if numo := mset.numConsumers(); numo != 0 {
		t.Fatalf("Expected to have zero consumers, got %d", numo)
	}
}

func TestJetStreamRedeliveryAfterServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	sendSubj := "MYQ"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: sendSubj, Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Now load up some messages.
	toSend := 25
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, sendSubj, "Hello World!")
	}
	state := mset.state()
	if state.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
	}

	sub, _ := nc.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()
	nc.Flush()

	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:        "TO",
		DeliverSubject: sub.Subject,
		AckPolicy:      AckExplicit,
		AckWait:        100 * time.Millisecond,
	})
	require_NoError(t, err)
	defer o.delete()

	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
		}
		return nil
	})

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())
	sd := s.JetStreamConfig().StoreDir

	// Stop current
	s.Shutdown()

	// Restart.
	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()

	// Don't wait for reconnect from old client.
	nc = clientConnectToServer(t, s)
	defer nc.Close()

	sub, _ = nc.SubscribeSync(sub.Subject)
	defer sub.Unsubscribe()

	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
		}
		return nil
	})
}

func TestJetStreamSnapshots(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "MY-STREAM"
	subjects := []string{"foo", "bar", "baz"}
	cfg := StreamConfig{
		Name:     mname,
		Storage:  FileStorage,
		Subjects: subjects,
		MaxMsgs:  1000,
	}

	acc := s.GlobalAccount()
	mset, err := acc.addStream(&cfg)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Make sure we send some as floor.
	toSend := rand.Intn(200) + 22
	for i := 1; i <= toSend; i++ {
		msg := fmt.Sprintf("Hello World %d", i)
		subj := subjects[rand.Intn(len(subjects))]
		sendStreamMsg(t, nc, subj, msg)
	}

	// Create up to 10 consumers.
	numConsumers := rand.Intn(10) + 1
	var obs []obsi
	for i := 1; i <= numConsumers; i++ {
		cname := fmt.Sprintf("WQ-%d", i)
		o, err := mset.addConsumer(workerModeConfig(cname))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Now grab some messages.
		toReceive := rand.Intn(toSend/2) + 1
		for r := 0; r < toReceive; r++ {
			resp, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if resp != nil {
				resp.Respond(nil)
			}
		}
		obs = append(obs, obsi{o.config(), toReceive})
	}
	nc.Flush()

	// Snapshot state of the stream and consumers.
	info := info{mset.config(), mset.state(), obs}

	sr, err := mset.snapshot(5*time.Second, false, true)
	if err != nil {
		t.Fatalf("Error getting snapshot: %v", err)
	}
	zr := sr.Reader
	snapshot, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("Error reading snapshot")
	}
	// Try to restore from snapshot with current stream present, should error.
	r := bytes.NewReader(snapshot)
	if _, err := acc.RestoreStream(&info.cfg, r); err == nil {
		t.Fatalf("Expected an error trying to restore existing stream")
	} else if !strings.Contains(err.Error(), "name already in use") {
		t.Fatalf("Incorrect error received: %v", err)
	}
	// Now delete so we can restore.
	pusage := acc.JetStreamUsage()
	mset.delete()
	r.Reset(snapshot)

	mset, err = acc.RestoreStream(&info.cfg, r)
	require_NoError(t, err)
	// Now compare to make sure they are equal.
	if nusage := acc.JetStreamUsage(); !reflect.DeepEqual(nusage, pusage) {
		t.Fatalf("Usage does not match after restore: %+v vs %+v", nusage, pusage)
	}
	if state := mset.state(); !reflect.DeepEqual(state, info.state) {
		t.Fatalf("State does not match: %+v vs %+v", state, info.state)
	}
	if cfg := mset.config(); !reflect.DeepEqual(cfg, info.cfg) {
		t.Fatalf("Configs do not match: %+v vs %+v", cfg, info.cfg)
	}
	// Consumers.
	if mset.numConsumers() != len(info.obs) {
		t.Fatalf("Number of consumers do not match: %d vs %d", mset.numConsumers(), len(info.obs))
	}
	for _, oi := range info.obs {
		if o := mset.lookupConsumer(oi.cfg.Durable); o != nil {
			if uint64(oi.ack+1) != o.nextSeq() {
				t.Fatalf("[%v] Consumer next seq is not correct: %d vs %d", o.String(), oi.ack+1, o.nextSeq())
			}
		} else {
			t.Fatalf("Expected to get an consumer")
		}
	}

	// Now try restoring to a different
	s2 := RunBasicJetStreamServer(t)
	defer s2.Shutdown()

	acc = s2.GlobalAccount()
	r.Reset(snapshot)
	mset, err = acc.RestoreStream(&info.cfg, r)
	require_NoError(t, err)

	o := mset.lookupConsumer("WQ-1")
	if o == nil {
		t.Fatalf("Could not lookup consumer")
	}

	nc2 := clientConnectToServer(t, s2)
	defer nc2.Close()

	// Make sure we can read messages.
	if _, err := nc2.Request(o.requestNextMsgSubject(), nil, 5*time.Second); err != nil {
		t.Fatalf("Unexpected error getting next message: %v", err)
	}
}

func TestJetStreamSnapshotsAPI(t *testing.T) {
	lopts := DefaultTestOptions
	lopts.ServerName = "LS"
	lopts.Port = -1
	lopts.LeafNode.Host = lopts.Host
	lopts.LeafNode.Port = -1

	ls := RunServer(&lopts)
	defer ls.Shutdown()

	opts := DefaultTestOptions
	opts.ServerName = "S"
	opts.Port = -1
	tdir := t.TempDir()
	opts.JetStream = true
	opts.JetStreamDomain = "domain"
	opts.StoreDir = tdir
	maxStore := int64(1024 * 1024 * 1024)
	opts.maxStoreSet = true
	opts.JetStreamMaxStore = maxStore
	rurl, _ := url.Parse(fmt.Sprintf("nats-leaf://%s:%d", lopts.LeafNode.Host, lopts.LeafNode.Port))
	opts.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{rurl}}}

	s := RunServer(&opts)
	defer s.Shutdown()

	checkLeafNodeConnected(t, s)

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	mname := "MY-STREAM"
	subjects := []string{"foo", "bar", "baz"}
	cfg := StreamConfig{
		Name:     mname,
		Storage:  FileStorage,
		Subjects: subjects,
		MaxMsgs:  1000,
	}

	acc := s.GlobalAccount()
	mset, err := acc.addStreamWithStore(&cfg, &FileStoreConfig{BlockSize: 128})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := rand.Intn(100) + 1
	for i := 1; i <= toSend; i++ {
		msg := fmt.Sprintf("Hello World %d", i)
		subj := subjects[rand.Intn(len(subjects))]
		sendStreamMsg(t, nc, subj, msg)
	}

	o, err := mset.addConsumer(workerModeConfig("WQ"))
	require_NoError(t, err)
	// Now grab some messages.
	toReceive := rand.Intn(toSend) + 1
	for r := 0; r < toReceive; r++ {
		resp, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if resp != nil {
			resp.Respond(nil)
		}
	}

	// Make sure we get proper error for non-existent request, streams,etc,
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "foo"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	var resp JSApiStreamSnapshotResponse
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error == nil || resp.Error.Code != 400 || resp.Error.Description != "bad request" {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}

	sreq := &JSApiStreamSnapshotRequest{}
	req, _ := json.Marshal(sreq)
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "foo"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error == nil || resp.Error.Code != 404 || resp.Error.Description != "stream not found" {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}

	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, mname), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error == nil || resp.Error.Code != 400 || resp.Error.Description != "deliver subject not valid" {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}

	// Set delivery subject, do not subscribe yet. Want this to be an ok pattern.
	sreq.DeliverSubject = nats.NewInbox()
	// Just for test, usually left alone.
	sreq.ChunkSize = 1024
	req, _ = json.Marshal(sreq)
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, mname), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	resp.Error = nil
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error != nil {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}
	// Check that we have the config and the state.
	if resp.Config == nil {
		t.Fatalf("Expected a stream config in the response, got %+v\n", resp)
	}
	if resp.State == nil {
		t.Fatalf("Expected a stream state in the response, got %+v\n", resp)
	}

	// Grab state for comparison.
	state := *resp.State
	config := *resp.Config

	// Setup to process snapshot chunks.
	var snapshot []byte
	done := make(chan bool)

	sub, _ := nc.Subscribe(sreq.DeliverSubject, func(m *nats.Msg) {
		// EOF
		if len(m.Data) == 0 {
			done <- true
			return
		}
		// Could be writing to a file here too.
		snapshot = append(snapshot, m.Data...)
		// Flow ack
		m.Respond(nil)
	})
	defer sub.Unsubscribe()

	// Wait to receive the snapshot.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive our snapshot in time")
	}

	// Now make sure this snapshot is legit.
	var rresp JSApiStreamRestoreResponse
	rreq := &JSApiStreamRestoreRequest{
		Config: config,
		State:  state,
	}
	req, _ = json.Marshal(rreq)

	// Make sure we get an error since stream still exists.
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, mname), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	json.Unmarshal(rmsg.Data, &rresp)
	if !IsNatsErr(rresp.Error, JSStreamNameExistRestoreFailedErr) {
		t.Fatalf("Did not get correct error response: %+v", rresp.Error)
	}

	// Delete this stream.
	mset.delete()

	// Sending no request message will error now.
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, mname), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	// Make sure to clear.
	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error == nil || rresp.Error.Code != 400 || rresp.Error.Description != "bad request" {
		t.Fatalf("Did not get correct error response: %+v", rresp.Error)
	}

	// This should work.
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, mname), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	// Make sure to clear.
	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
	}

	// Can be any size message.
	var chunk [512]byte
	for r := bytes.NewReader(snapshot); ; {
		n, err := r.Read(chunk[:])
		if err != nil {
			break
		}
		nc.Request(rresp.DeliverSubject, chunk[:n], time.Second)
	}
	nc.Request(rresp.DeliverSubject, nil, time.Second)

	mset, err = acc.lookupStream(mname)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", mname)
	}
	if !reflect.DeepEqual(mset.state(), state) {
		t.Fatalf("Did not match states, %+v vs %+v", mset.state(), state)
	}

	// Now ask that the stream be checked first.
	sreq.ChunkSize = 0
	sreq.CheckMsgs = true
	snapshot = snapshot[:0]

	req, _ = json.Marshal(sreq)
	if _, err = nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, mname), req, 5*time.Second); err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	// Wait to receive the snapshot.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive our snapshot in time")
	}

	// Now connect through a cluster server and make sure we can get things to work this way as well.
	// This client, connecting to a leaf without shared system account and domain needs to provide the domain explicitly.
	nc2 := clientConnectToServer(t, ls)
	defer nc2.Close()
	// Wait a bit for interest to propagate.
	time.Sleep(100 * time.Millisecond)

	snapshot = snapshot[:0]

	req, _ = json.Marshal(sreq)
	rmsg, err = nc2.Request(fmt.Sprintf(strings.ReplaceAll(JSApiStreamSnapshotT, JSApiPrefix, "$JS.domain.API"), mname), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	resp.Error = nil
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error != nil {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}
	// Wait to receive the snapshot.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive our snapshot in time")
	}

	// Now do a restore through the new client connection.
	// Delete this stream first.
	mset, err = acc.lookupStream(mname)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", mname)
	}
	state = mset.state()
	mset.delete()

	rmsg, err = nc2.Request(strings.ReplaceAll(JSApiStreamRestoreT, JSApiPrefix, "$JS.domain.API"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	// Make sure to clear.
	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
	}

	// Make sure when we send something without a reply subject the subscription is shutoff.
	r := bytes.NewReader(snapshot)
	n, _ := r.Read(chunk[:])
	nc2.Publish(rresp.DeliverSubject, chunk[:n])
	nc2.Flush()
	n, _ = r.Read(chunk[:])
	if _, err := nc2.Request(rresp.DeliverSubject, chunk[:n], 100*time.Millisecond); err == nil {
		t.Fatalf("Expected restore subscription to be closed")
	}

	req, _ = json.Marshal(rreq)
	rmsg, err = nc2.Request(strings.ReplaceAll(JSApiStreamRestoreT, JSApiPrefix, "$JS.domain.API"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	// Make sure to clear.
	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
	}

	for r := bytes.NewReader(snapshot); ; {
		n, err := r.Read(chunk[:])
		if err != nil {
			break
		}
		// Make sure other side responds to reply subjects for ack flow. Optional.
		if _, err := nc2.Request(rresp.DeliverSubject, chunk[:n], time.Second); err != nil {
			t.Fatalf("Restore not honoring reply subjects for ack flow")
		}
	}

	// For EOF this will send back stream info or an error.
	si, err := nc2.Request(rresp.DeliverSubject, nil, time.Second)
	if err != nil {
		t.Fatalf("Got an error restoring stream: %v", err)
	}
	var scResp JSApiStreamCreateResponse
	if err := json.Unmarshal(si.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.Error != nil {
		t.Fatalf("Got an unexpected error from EOF on restore: %+v", scResp.Error)
	}

	if !reflect.DeepEqual(scResp.StreamInfo.State, state) {
		t.Fatalf("Did not match states, %+v vs %+v", scResp.StreamInfo.State, state)
	}

	// Now make sure that if we try to change the name/identity of the stream we get an error.
	mset, err = acc.lookupStream(mname)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", mname)
	}
	mset.state()
	mset.delete()

	rreq.Config.Name = "NEW_STREAM"
	req, _ = json.Marshal(rreq)

	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, rreq.Config.Name), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	// Make sure to clear.
	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	// We should not get an error here.
	if rresp.Error != nil {
		t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
	}
	for r := bytes.NewReader(snapshot); ; {
		n, err := r.Read(chunk[:])
		if err != nil {
			break
		}
		nc.Request(rresp.DeliverSubject, chunk[:n], time.Second)
	}

	si, err = nc2.Request(rresp.DeliverSubject, nil, time.Second)
	require_NoError(t, err)

	scResp.Error = nil
	if err := json.Unmarshal(si.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.Error == nil {
		t.Fatalf("Expected an error but got none")
	}
	expect := "names do not match"
	if !strings.Contains(scResp.Error.Description, expect) {
		t.Fatalf("Expected description of %q, got %q", expect, scResp.Error.Description)
	}
}

func TestJetStreamPubAckPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "foo", Storage: nats.MemoryStorage}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 1_000_000
	start := time.Now()
	for i := 0; i < toSend; i++ {
		js.PublishAsync("foo", []byte("OK"))
	}
	<-js.PublishAsyncComplete()
	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toSend)/tt.Seconds())
}

func TestJetStreamPubPerfWithFullStream(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	toSend, msg := 1_000_000, []byte("OK")

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", MaxMsgs: int64(toSend)})
	require_NoError(t, err)

	start := time.Now()
	for i := 0; i < toSend; i++ {
		js.PublishAsync("foo", msg)
	}
	<-js.PublishAsyncComplete()
	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toSend)/tt.Seconds())

	// Now do same amount but knowing we are at our limit.
	start = time.Now()
	for i := 0; i < toSend; i++ {
		js.PublishAsync("foo", msg)
	}
	<-js.PublishAsyncComplete()
	tt = time.Since(start)
	fmt.Printf("\ntime is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toSend)/tt.Seconds())
}

func TestJetStreamSnapshotsAPIPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	cfg := StreamConfig{
		Name:    "snap-perf",
		Storage: FileStorage,
	}

	acc := s.GlobalAccount()
	if _, err := acc.addStream(&cfg); err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	msg := make([]byte, 128*1024)
	// If you don't give gzip some data will spend too much time compressing everything to zero.
	crand.Read(msg)

	for i := 0; i < 10000; i++ {
		nc.Publish("snap-perf", msg)
	}
	nc.Flush()

	sreq := &JSApiStreamSnapshotRequest{DeliverSubject: nats.NewInbox()}
	req, _ := json.Marshal(sreq)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "snap-perf"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}

	var resp JSApiStreamSnapshotResponse
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error != nil {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}

	done := make(chan bool)
	total := 0
	sub, _ := nc.Subscribe(sreq.DeliverSubject, func(m *nats.Msg) {
		// EOF
		if len(m.Data) == 0 {
			m.Sub.Unsubscribe()
			done <- true
			return
		}
		// We don't do anything with the snapshot, just take
		// note of the size.
		total += len(m.Data)
		// Flow ack
		m.Respond(nil)
	})
	defer sub.Unsubscribe()

	start := time.Now()
	// Wait to receive the snapshot.
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("Did not receive our snapshot in time")
	}
	td := time.Since(start)
	fmt.Printf("Received %d bytes in %v\n", total, td)
	fmt.Printf("Rate %.0f MB/s\n", float64(total)/td.Seconds()/(1024*1024))
}

func TestJetStreamActiveDelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "ADS", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "ADS", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			sendSubj := "foo.22"
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			o, err := mset.addConsumer(&ConsumerConfig{Durable: "to", DeliverSubject: "d"})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			// We have no active interest above. So consumer will be considered inactive. Let's subscribe and make sure
			// we get the messages instantly. This will test that we hook interest activation correctly.
			sub, _ := nc.SubscribeSync("d")
			defer sub.Unsubscribe()
			nc.Flush()

			checkFor(t, 100*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
				}
				return nil
			})
		})
	}
}

func TestJetStreamEphemeralConsumers(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "EP", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "EP", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !o.isActive() {
				t.Fatalf("Expected the consumer to be considered active")
			}
			if numo := mset.numConsumers(); numo != 1 {
				t.Fatalf("Expected number of consumers to be 1, got %d", numo)
			}
			// Set our delete threshold to something low for testing purposes.
			o.setInActiveDeleteThreshold(100 * time.Millisecond)

			// Make sure works now.
			nc.Request("foo.22", nil, 100*time.Millisecond)
			checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 1 {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, 1)
				}
				return nil
			})

			// Now close the subscription, this should trip active state on the ephemeral consumer.
			sub.Unsubscribe()
			checkFor(t, time.Second, 10*time.Millisecond, func() error {
				if o.isActive() {
					return fmt.Errorf("Expected the ephemeral consumer to be considered inactive")
				}
				return nil
			})
			// The reason for this still being 1 is that we give some time in case of a reconnect scenario.
			// We detect right away on the interest change but we wait for interest to be re-established.
			// This is in case server goes away but app is fine, we do not want to recycle those consumers.
			if numo := mset.numConsumers(); numo != 1 {
				t.Fatalf("Expected number of consumers to be 1, got %d", numo)
			}

			// We should delete this one after the delete threshold.
			checkFor(t, time.Second, 100*time.Millisecond, func() error {
				if numo := mset.numConsumers(); numo != 0 {
					return fmt.Errorf("Expected number of consumers to be 0, got %d", numo)
				}
				return nil
			})
		})
	}
}

func TestJetStreamConsumerReconnect(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "ET", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "ET", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			// Capture the subscription.
			delivery := sub.Subject

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery, AckPolicy: AckExplicit})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !o.isActive() {
				t.Fatalf("Expected the consumer to be considered active")
			}
			if numo := mset.numConsumers(); numo != 1 {
				t.Fatalf("Expected number of consumers to be 1, got %d", numo)
			}

			// We will simulate reconnect by unsubscribing on one connection and forming
			// the same on another. Once we have cluster tests we will do more testing on
			// reconnect scenarios.
			getMsg := func(seqno int) *nats.Msg {
				t.Helper()
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error for %d: %v", seqno, err)
				}
				if seq := o.seqFromReply(m.Reply); seq != uint64(seqno) {
					t.Fatalf("Expected sequence of %d , got %d", seqno, seq)
				}
				m.Respond(nil)
				return m
			}

			sendMsg := func() {
				t.Helper()
				if err := nc.Publish("foo.22", []byte("OK!")); err != nil {
					return
				}
			}

			checkForInActive := func() {
				checkFor(t, 250*time.Millisecond, 50*time.Millisecond, func() error {
					if o.isActive() {
						return fmt.Errorf("Consumer is still active")
					}
					return nil
				})
			}

			// Send and Pull first message.
			sendMsg() // 1
			getMsg(1)
			// Cancel first one.
			sub.Unsubscribe()
			// Re-establish new sub on same subject.
			sub, _ = nc.SubscribeSync(delivery)
			nc.Flush()

			// We should be getting 2 here.
			sendMsg() // 2
			getMsg(2)

			sub.Unsubscribe()
			checkForInActive()

			// send 3-10
			for i := 0; i <= 7; i++ {
				sendMsg()
			}
			// Make sure they are all queued up with no interest.
			nc.Flush()

			// Restablish again.
			sub, _ = nc.SubscribeSync(delivery)
			nc.Flush()

			// We should be getting 3-10 here.
			for i := 3; i <= 10; i++ {
				getMsg(i)
			}
		})
	}
}

func TestJetStreamDurableConsumerReconnect(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DT", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "DT", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			dname := "d22"
			subj1 := nats.NewInbox()

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj1,
				AckPolicy:      AckExplicit,
				AckWait:        50 * time.Millisecond})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			sendMsg := func() {
				t.Helper()
				if err := nc.Publish("foo.22", []byte("OK!")); err != nil {
					return
				}
			}

			// Send 10 msgs
			toSend := 10
			for i := 0; i < toSend; i++ {
				sendMsg()
			}

			sub, _ := nc.SubscribeSync(subj1)
			defer sub.Unsubscribe()

			checkFor(t, 500*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
				}
				return nil
			})

			getMsg := func(seqno int) *nats.Msg {
				t.Helper()
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if seq := o.streamSeqFromReply(m.Reply); seq != uint64(seqno) {
					t.Fatalf("Expected sequence of %d , got %d", seqno, seq)
				}
				m.Respond(nil)
				return m
			}

			// Ack first half
			for i := 1; i <= toSend/2; i++ {
				m := getMsg(i)
				m.Respond(nil)
			}

			// Now unsubscribe and wait to become inactive
			sub.Unsubscribe()
			checkFor(t, 250*time.Millisecond, 50*time.Millisecond, func() error {
				if o.isActive() {
					return fmt.Errorf("Consumer is still active")
				}
				return nil
			})

			// Now we should be able to replace the delivery subject.
			subj2 := nats.NewInbox()
			sub, _ = nc.SubscribeSync(subj2)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err = mset.addConsumer(&ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj2,
				AckPolicy:      AckExplicit,
				AckWait:        50 * time.Millisecond})
			if err != nil {
				t.Fatalf("Unexpected error trying to add a new durable consumer: %v", err)
			}

			// We should get the remaining messages here.
			for i := toSend/2 + 1; i <= toSend; i++ {
				m := getMsg(i)
				m.Respond(nil)
			}
		})
	}
}

func TestJetStreamDurableConsumerReconnectWithOnlyPending(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DT", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "DT", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			dname := "d22"
			subj1 := nats.NewInbox()

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj1,
				AckPolicy:      AckExplicit,
				AckWait:        25 * time.Millisecond})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			sendMsg := func(payload string) {
				t.Helper()
				if err := nc.Publish("foo.22", []byte(payload)); err != nil {
					return
				}
			}

			sendMsg("1")

			sub, _ := nc.SubscribeSync(subj1)
			defer sub.Unsubscribe()

			checkFor(t, 500*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 1 {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, 1)
				}
				return nil
			})

			// Now unsubscribe and wait to become inactive
			sub.Unsubscribe()
			checkFor(t, 250*time.Millisecond, 50*time.Millisecond, func() error {
				if o.isActive() {
					return fmt.Errorf("Consumer is still active")
				}
				return nil
			})

			// Send the second message while delivery subscriber is not running
			sendMsg("2")

			// Now we should be able to replace the delivery subject.
			subj2 := nats.NewInbox()
			o, err = mset.addConsumer(&ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj2,
				AckPolicy:      AckExplicit,
				AckWait:        25 * time.Millisecond})
			if err != nil {
				t.Fatalf("Unexpected error trying to add a new durable consumer: %v", err)
			}
			sub, _ = nc.SubscribeSync(subj2)
			defer sub.Unsubscribe()
			nc.Flush()

			// We should get msg "1" and "2" delivered. They will be reversed.
			for i := 0; i < 2; i++ {
				msg, err := sub.NextMsg(500 * time.Millisecond)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				sseq, _, dc, _, _ := replyInfo(msg.Reply)
				if sseq == 1 && dc == 1 {
					t.Fatalf("Expected a redelivery count greater then 1 for sseq 1, got %d", dc)
				}
				if sseq != 1 && sseq != 2 {
					t.Fatalf("Expected stream sequence of 1 or 2 but got %d", sseq)
				}
			}
		})
	}
}

func TestJetStreamDurableFilteredSubjectConsumerReconnect(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DT", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "DT", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			sendMsgs := func(toSend int) {
				for i := 0; i < toSend; i++ {
					var subj string
					if i%2 == 0 {
						subj = "foo.AA"
					} else {
						subj = "foo.ZZ"
					}
					_, err := js.Publish(subj, []byte("OK!"))
					require_NoError(t, err)
				}
			}

			// Send 50 msgs
			toSend := 50
			sendMsgs(toSend)

			dname := "d33"
			dsubj := nats.NewInbox()

			// Now create an consumer for foo.AA, only requesting the last one.
			_, err = mset.addConsumer(&ConsumerConfig{
				Durable:        dname,
				DeliverSubject: dsubj,
				FilterSubject:  "foo.AA",
				DeliverPolicy:  DeliverLast,
				AckPolicy:      AckExplicit,
				AckWait:        100 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			sub, _ := nc.SubscribeSync(dsubj)
			defer sub.Unsubscribe()

			// Used to calculate difference between store seq and delivery seq.
			storeBaseOff := 47

			getMsg := func(seq int) *nats.Msg {
				t.Helper()
				sseq := 2*seq + storeBaseOff
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, roseq, dcount, _, _ := replyInfo(m.Reply)
				if roseq != uint64(seq) {
					t.Fatalf("Expected consumer sequence of %d , got %d", seq, roseq)
				}
				if rsseq != uint64(sseq) {
					t.Fatalf("Expected stream sequence of %d , got %d", sseq, rsseq)
				}
				if dcount != 1 {
					t.Fatalf("Expected message to not be marked as redelivered")
				}
				return m
			}

			getRedeliveredMsg := func(seq int) *nats.Msg {
				t.Helper()
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				_, roseq, dcount, _, _ := replyInfo(m.Reply)
				if roseq != uint64(seq) {
					t.Fatalf("Expected consumer sequence of %d , got %d", seq, roseq)
				}
				if dcount < 2 {
					t.Fatalf("Expected message to be marked as redelivered")
				}
				// Ack this message.
				m.Respond(nil)
				return m
			}

			// All consumers start at 1 and always have increasing sequence numbers.
			m := getMsg(1)
			m.Respond(nil)

			// Now send 50 more, so 100 total, 26 (last + 50/2) for this consumer.
			sendMsgs(toSend)

			state := mset.state()
			if state.Msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, state.Msgs)
			}

			// For tracking next expected.
			nextSeq := 2
			noAcks := 0
			for i := 0; i < toSend/2; i++ {
				m := getMsg(nextSeq)
				if i%2 == 0 {
					m.Respond(nil) // Ack evens.
				} else {
					noAcks++
				}
				nextSeq++
			}

			// We should now get those redelivered.
			for i := 0; i < noAcks; i++ {
				getRedeliveredMsg(nextSeq)
				nextSeq++
			}

			// Now send 50 more.
			sendMsgs(toSend)

			storeBaseOff -= noAcks * 2

			for i := 0; i < toSend/2; i++ {
				m := getMsg(nextSeq)
				m.Respond(nil)
				nextSeq++
			}
		})
	}
}

func TestJetStreamConsumerInactiveNoDeadlock(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			// Send lots of msgs and have them queued up.
			for i := 0; i < 10000; i++ {
				js.Publish("DC", []byte("OK!"))
			}

			if state := mset.state(); state.Msgs != 10000 {
				t.Fatalf("Expected %d messages, got %d", 10000, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			sub.SetPendingLimits(-1, -1)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()

			for i := 0; i < 10; i++ {
				if _, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			// Force us to become inactive but we want to make sure we do not lock up
			// the internal sendq.
			sub.Unsubscribe()
			nc.Flush()
		})
	}
}

func TestJetStreamMetadata(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Retention: WorkQueuePolicy, Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Retention: WorkQueuePolicy, Storage: FileStorage}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			for i := 0; i < 10; i++ {
				nc.Publish("DC", []byte("OK!"))
				nc.Flush()
				time.Sleep(time.Millisecond)
			}

			if state := mset.state(); state.Msgs != 10 {
				t.Fatalf("Expected %d messages, got %d", 10, state.Msgs)
			}

			o, err := mset.addConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			for i := uint64(1); i <= 10; i++ {
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				sseq, dseq, dcount, ts, _ := replyInfo(m.Reply)

				mreq := &JSApiMsgGetRequest{Seq: sseq}
				req, err := json.Marshal(mreq)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				// Load the original message from the stream to verify ReplyInfo ts against stored message
				smsgj, err := nc.Request(fmt.Sprintf(JSApiMsgGetT, c.mconfig.Name), req, time.Second)
				if err != nil {
					t.Fatalf("Could not retrieve stream message: %v", err)
				}

				var resp JSApiMsgGetResponse
				err = json.Unmarshal(smsgj.Data, &resp)
				if err != nil {
					t.Fatalf("Could not parse stream message: %v", err)
				}
				if resp.Message == nil || resp.Error != nil {
					t.Fatalf("Did not receive correct response")
				}
				smsg := resp.Message
				if ts != smsg.Time.UnixNano() {
					t.Fatalf("Wrong timestamp in ReplyInfo for msg %d, expected %v got %v", i, ts, smsg.Time.UnixNano())
				}
				if sseq != i {
					t.Fatalf("Expected set sequence of %d, got %d", i, sseq)
				}
				if dseq != i {
					t.Fatalf("Expected delivery sequence of %d, got %d", i, dseq)
				}
				if dcount != 1 {
					t.Fatalf("Expected delivery count to be 1, got %d", dcount)
				}
				m.Respond(AckAck)
			}

			// Now make sure we get right response when message is missing.
			mreq := &JSApiMsgGetRequest{Seq: 1}
			req, err := json.Marshal(mreq)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Load the original message from the stream to verify ReplyInfo ts against stored message
			rmsg, err := nc.Request(fmt.Sprintf(JSApiMsgGetT, c.mconfig.Name), req, time.Second)
			if err != nil {
				t.Fatalf("Could not retrieve stream message: %v", err)
			}
			var resp JSApiMsgGetResponse
			err = json.Unmarshal(rmsg.Data, &resp)
			if err != nil {
				t.Fatalf("Could not parse stream message: %v", err)
			}
			if resp.Error == nil || resp.Error.Code != 404 || resp.Error.Description != "no message found" {
				t.Fatalf("Did not get correct error response: %+v", resp.Error)
			}
		})
	}
}
func TestJetStreamRedeliverCount(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			if _, err = js.Publish("DC", []byte("OK!")); err != nil {
				t.Fatal(err)
			}
			checkFor(t, time.Second, time.Millisecond*250, func() error {
				if state := mset.state(); state.Msgs != 1 {
					return fmt.Errorf("Expected %d messages, got %d", 1, state.Msgs)
				}
				return nil
			})

			o, err := mset.addConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			for i := uint64(1); i <= 10; i++ {
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				sseq, dseq, dcount, _, _ := replyInfo(m.Reply)

				// Make sure we keep getting stream sequence #1
				if sseq != 1 {
					t.Fatalf("Expected set sequence of 1, got %d", sseq)
				}
				if dseq != i {
					t.Fatalf("Expected delivery sequence of %d, got %d", i, dseq)
				}
				// Now make sure dcount is same as dseq (or i).
				if dcount != i {
					t.Fatalf("Expected delivery count to be %d, got %d", i, dcount)
				}

				// Make sure it keeps getting sent back.
				m.Respond(AckNak)
				nc.Flush()
			}
		})
	}
}

// We want to make sure that for pull based consumers that if we ack
// late with no interest the redelivery attempt is removed and we do
// not get the message back.
func TestJetStreamRedeliverAndLateAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: "LA", Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	o, err := mset.addConsumer(&ConsumerConfig{Durable: "DDD", AckPolicy: AckExplicit, AckWait: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Queue up message
	sendStreamMsg(t, nc, "LA", "Hello World!")

	nextSubj := o.requestNextMsgSubject()
	msg, err := nc.Request(nextSubj, nil, time.Second)
	require_NoError(t, err)

	// Wait for past ackwait time
	time.Sleep(150 * time.Millisecond)
	// Now ack!
	msg.AckSync()
	// We should not get this back.
	if _, err := nc.Request(nextSubj, nil, 10*time.Millisecond); err == nil {
		t.Fatalf("Message should not have been sent back")
	}
}

// https://github.com/nats-io/nats-server/issues/1502
func TestJetStreamPendingNextTimer(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: "NT", Storage: MemoryStorage, Subjects: []string{"ORDERS.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:       "DDD",
		AckPolicy:     AckExplicit,
		FilterSubject: "ORDERS.test",
		AckWait:       100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	sendAndReceive := func() {
		nc := clientConnectToServer(t, s)
		defer nc.Close()

		// Queue up message
		sendStreamMsg(t, nc, "ORDERS.test", "Hello World! #1")
		sendStreamMsg(t, nc, "ORDERS.test", "Hello World! #2")

		nextSubj := o.requestNextMsgSubject()
		for i := 0; i < 2; i++ {
			if _, err := nc.Request(nextSubj, nil, time.Second); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
		nc.Close()
		time.Sleep(200 * time.Millisecond)
	}

	sendAndReceive()
	sendAndReceive()
	sendAndReceive()
}

func TestJetStreamCanNotNakAckd(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			// Send 10 msgs
			for i := 0; i < 10; i++ {
				js.Publish("DC", []byte("OK!"))
			}
			if state := mset.state(); state.Msgs != 10 {
				t.Fatalf("Expected %d messages, got %d", 10, state.Msgs)
			}

			o, err := mset.addConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			for i := uint64(1); i <= 10; i++ {
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				// Ack evens.
				if i%2 == 0 {
					m.Respond(nil)
				}
			}
			nc.Flush()

			// Fake these for now.
			ackReplyT := "$JS.A.DC.WQ.1.%d.%d"
			checkBadNak := func(seq int) {
				t.Helper()
				if err := nc.Publish(fmt.Sprintf(ackReplyT, seq, seq), AckNak); err != nil {
					t.Fatalf("Error sending nak: %v", err)
				}
				nc.Flush()
				if _, err := nc.Request(o.requestNextMsgSubject(), nil, 10*time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("Did not expect new delivery on nak of %d", seq)
				}
			}

			// If the nak took action it will deliver another message, incrementing the next delivery seq.
			// We ack evens above, so these should fail
			for i := 2; i <= 10; i += 2 {
				checkBadNak(i)
			}

			// Now check we can not nak something we do not have.
			checkBadNak(22)
		})
	}
}

func TestJetStreamStreamPurge(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			// Send 100 msgs
			for i := 0; i < 100; i++ {
				js.Publish("DC", []byte("OK!"))
			}
			if state := mset.state(); state.Msgs != 100 {
				t.Fatalf("Expected %d messages, got %d", 100, state.Msgs)
			}
			mset.purge(nil)
			state := mset.state()
			if state.Msgs != 0 {
				t.Fatalf("Expected %d messages, got %d", 0, state.Msgs)
			}
			// Make sure first timestamp are reset.
			if !state.FirstTime.IsZero() {
				t.Fatalf("Expected the state's first time to be zero after purge")
			}
			time.Sleep(10 * time.Millisecond)
			now := time.Now()
			js.Publish("DC", []byte("OK!"))

			state = mset.state()
			if state.Msgs != 1 {
				t.Fatalf("Expected %d message, got %d", 1, state.Msgs)
			}
			if state.FirstTime.Before(now) {
				t.Fatalf("First time is incorrect after adding messages back in")
			}
			if state.FirstTime != state.LastTime {
				t.Fatalf("Expected first and last times to be the same for only message")
			}
		})
	}
}

func TestJetStreamStreamPurgeWithConsumer(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			// Send 100 msgs
			for i := 0; i < 100; i++ {
				js.Publish("DC", []byte("OK!"))
			}
			if state := mset.state(); state.Msgs != 100 {
				t.Fatalf("Expected %d messages, got %d", 100, state.Msgs)
			}
			// Now create an consumer and make sure it functions properly.
			o, err := mset.addConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()
			nextSubj := o.requestNextMsgSubject()
			for i := 0; i < 50; i++ {
				msg, err := nc.Request(nextSubj, nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				// Ack.
				msg.Respond(nil)
			}
			// Now grab next 25 without ack.
			for i := 0; i < 25; i++ {
				if _, err := nc.Request(nextSubj, nil, time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			state := o.info()
			if state.AckFloor.Consumer != 50 {
				t.Fatalf("Expected ack floor of 50, got %d", state.AckFloor.Consumer)
			}
			if state.NumAckPending != 25 {
				t.Fatalf("Expected len(pending) to be 25, got %d", state.NumAckPending)
			}
			// Now do purge.
			mset.purge(nil)
			if state := mset.state(); state.Msgs != 0 {
				t.Fatalf("Expected %d messages, got %d", 0, state.Msgs)
			}
			// Now re-acquire state and check that we did the right thing.
			// Pending should be cleared, and stream sequences should have been set
			// to the total messages before purge + 1.
			state = o.info()
			if state.NumAckPending != 0 {
				t.Fatalf("Expected no pending, got %d", state.NumAckPending)
			}
			if state.Delivered.Stream != 100 {
				t.Fatalf("Expected to have setseq now at next seq of 100, got %d", state.Delivered.Stream)
			}
			// Check AckFloors which should have also been adjusted.
			if state.AckFloor.Stream != 100 {
				t.Fatalf("Expected ackfloor for setseq to be 100, got %d", state.AckFloor.Stream)
			}
			if state.AckFloor.Consumer != 75 {
				t.Fatalf("Expected ackfloor for obsseq to be 75, got %d", state.AckFloor.Consumer)
			}
			// Also make sure we can get new messages correctly.
			js.Publish("DC", []byte("OK-22"))
			if msg, err := nc.Request(nextSubj, nil, time.Second); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			} else if string(msg.Data) != "OK-22" {
				t.Fatalf("Received wrong message, wanted 'OK-22', got %q", msg.Data)
			}
		})
	}
}

func TestJetStreamStreamPurgeWithConsumerAndRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			// Send 100 msgs
			for i := 0; i < 100; i++ {
				js.Publish("DC", []byte("OK!"))
			}
			if state := mset.state(); state.Msgs != 100 {
				t.Fatalf("Expected %d messages, got %d", 100, state.Msgs)
			}
			// Now create an consumer and make sure it functions properly.
			// This will test redelivery state and purge of the stream.
			wcfg := &ConsumerConfig{
				Durable:   "WQ",
				AckPolicy: AckExplicit,
				AckWait:   20 * time.Millisecond,
			}
			o, err := mset.addConsumer(wcfg)
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()
			nextSubj := o.requestNextMsgSubject()
			for i := 0; i < 50; i++ {
				// Do not ack these.
				if _, err := nc.Request(nextSubj, nil, time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			// Now wait to make sure we are in a redelivered state.
			time.Sleep(wcfg.AckWait * 2)
			// Now do purge.
			mset.purge(nil)
			if state := mset.state(); state.Msgs != 0 {
				t.Fatalf("Expected %d messages, got %d", 0, state.Msgs)
			}
			// Now get the state and check that we did the right thing.
			// Pending should be cleared, and stream sequences should have been set
			// to the total messages before purge + 1.
			state := o.info()
			if state.NumAckPending != 0 {
				t.Fatalf("Expected no pending, got %d", state.NumAckPending)
			}
			if state.Delivered.Stream != 100 {
				t.Fatalf("Expected to have setseq now at next seq of 100, got %d", state.Delivered.Stream)
			}
			// Check AckFloors which should have also been adjusted.
			if state.AckFloor.Stream != 100 {
				t.Fatalf("Expected ackfloor for setseq to be 100, got %d", state.AckFloor.Stream)
			}
			if state.AckFloor.Consumer != 50 {
				t.Fatalf("Expected ackfloor for obsseq to be 75, got %d", state.AckFloor.Consumer)
			}
			// Also make sure we can get new messages correctly.
			js.Publish("DC", []byte("OK-22"))
			if msg, err := nc.Request(nextSubj, nil, time.Second); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			} else if string(msg.Data) != "OK-22" {
				t.Fatalf("Received wrong message, wanted 'OK-22', got %q", msg.Data)
			}
		})
	}
}

func TestJetStreamInterestRetentionStream(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage, Retention: InterestPolicy}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage, Retention: InterestPolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			// Send 100 msgs
			totalMsgs := 100

			for i := 0; i < totalMsgs; i++ {
				js.Publish("DC", []byte("OK!"))
			}

			checkNumMsgs := func(numExpected int) {
				t.Helper()
				checkFor(t, time.Second, 15*time.Millisecond, func() error {
					if state := mset.state(); state.Msgs != uint64(numExpected) {
						return fmt.Errorf("Expected %d messages, got %d", numExpected, state.Msgs)
					}
					return nil
				})
			}

			// Since we had no interest this should be 0.
			checkNumMsgs(0)

			syncSub := func() *nats.Subscription {
				sub, _ := nc.SubscribeSync(nats.NewInbox())
				nc.Flush()
				return sub
			}

			// Now create three consumers.
			// 1. AckExplicit
			// 2. AckAll
			// 3. AckNone

			sub1 := syncSub()
			mset.addConsumer(&ConsumerConfig{DeliverSubject: sub1.Subject, AckPolicy: AckExplicit})

			sub2 := syncSub()
			mset.addConsumer(&ConsumerConfig{DeliverSubject: sub2.Subject, AckPolicy: AckAll})

			sub3 := syncSub()
			mset.addConsumer(&ConsumerConfig{DeliverSubject: sub3.Subject, AckPolicy: AckNone})

			for i := 0; i < totalMsgs; i++ {
				js.Publish("DC", []byte("OK!"))
			}

			checkNumMsgs(totalMsgs)

			// Wait for all messsages to be pending for each sub.
			for i, sub := range []*nats.Subscription{sub1, sub2, sub3} {
				checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); nmsgs != totalMsgs {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d for sub %d", nmsgs, totalMsgs, i+1)
					}
					return nil
				})
			}

			getAndAck := func(sub *nats.Subscription) {
				t.Helper()
				if m, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				} else {
					m.Respond(nil)
				}
				nc.Flush()
			}

			// Ack evens for the explicit ack sub.
			var odds []*nats.Msg
			for i := 1; i <= totalMsgs; i++ {
				if m, err := sub1.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				} else if i%2 == 0 {
					m.Respond(nil) // Ack evens.
				} else {
					odds = append(odds, m)
				}
			}
			nc.Flush()

			checkNumMsgs(totalMsgs)

			// Now ack first for AckAll sub2
			getAndAck(sub2)
			// We should be at the same number since we acked 1, explicit acked 2
			checkNumMsgs(totalMsgs)
			// Now ack second for AckAll sub2
			getAndAck(sub2)
			// We should now have 1 removed.
			checkNumMsgs(totalMsgs - 1)
			// Now ack third for AckAll sub2
			getAndAck(sub2)
			// We should still only have 1 removed.
			checkNumMsgs(totalMsgs - 1)

			// Now ack odds from explicit.
			for _, m := range odds {
				m.Respond(nil) // Ack
			}
			nc.Flush()

			// we should have 1, 2, 3 acks now.
			checkNumMsgs(totalMsgs - 3)

			nm, _, _ := sub2.Pending()
			// Now ack last ackAll message. This should clear all of them.
			for i := 1; i <= nm; i++ {
				if m, err := sub2.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				} else if i == nm {
					m.Respond(nil)
				}
			}
			nc.Flush()

			// Should be zero now.
			checkNumMsgs(0)
		})
	}
}

func TestJetStreamInterestRetentionStreamWithFilteredConsumers(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Subjects: []string{"*"}, Storage: MemoryStorage, Retention: InterestPolicy}},
		{"FileStore", &StreamConfig{Name: "DC", Subjects: []string{"*"}, Storage: FileStorage, Retention: InterestPolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			fsub, err := js.SubscribeSync("foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer fsub.Unsubscribe()

			bsub, err := js.SubscribeSync("bar")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer bsub.Unsubscribe()

			msg := []byte("FILTERED")
			sendMsg := func(subj string) {
				t.Helper()
				if _, err = js.Publish(subj, msg); err != nil {
					t.Fatalf("Unexpected publish error: %v", err)
				}
			}

			getAndAck := func(sub *nats.Subscription) {
				t.Helper()
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error getting msg: %v", err)
				}
				m.AckSync()
			}

			checkState := func(expected uint64) {
				t.Helper()
				si, err := js.StreamInfo("DC")
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if si.State.Msgs != expected {
					t.Fatalf("Expected %d msgs, got %d", expected, si.State.Msgs)
				}
			}

			sendMsg("foo")
			checkState(1)
			getAndAck(fsub)
			checkState(0)
			sendMsg("bar")
			sendMsg("foo")
			checkState(2)
			getAndAck(bsub)
			checkState(1)
			getAndAck(fsub)
			checkState(0)
		})
	}
}

func TestJetStreamInterestRetentionWithWildcardsAndFilteredConsumers(t *testing.T) {
	msc := StreamConfig{
		Name:      "DCWC",
		Subjects:  []string{"foo.*"},
		Storage:   MemoryStorage,
		Retention: InterestPolicy,
	}
	fsc := msc
	fsc.Storage = FileStorage

	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &msc},
		{"FileStore", &fsc},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			for i := 0; i < 10; i++ {
				sendStreamMsg(t, nc, "foo.bar", "Hello World!")
			}
			if state := mset.state(); state.Msgs != 0 {
				t.Fatalf("Expected %d messages, got %d", 0, state.Msgs)
			}

			cfg := &ConsumerConfig{Durable: "ddd", FilterSubject: "foo.bar", AckPolicy: AckExplicit}
			o, err := mset.addConsumer(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()

			sendStreamMsg(t, nc, "foo.bar", "Hello World!")
			if state := mset.state(); state.Msgs != 1 {
				t.Fatalf("Expected %d message, got %d", 1, state.Msgs)
			} else if state.FirstSeq != 11 {
				t.Fatalf("Expected %d for first seq, got %d", 11, state.FirstSeq)
			}
			// Now send to foo.baz, which has no interest, so we should not hold onto this message.
			sendStreamMsg(t, nc, "foo.baz", "Hello World!")
			if state := mset.state(); state.Msgs != 1 {
				t.Fatalf("Expected %d message, got %d", 1, state.Msgs)
			}
		})
	}
}

func TestJetStreamInterestRetentionStreamWithDurableRestart(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "IK", Storage: MemoryStorage, Retention: InterestPolicy}},
		{"FileStore", &StreamConfig{Name: "IK", Storage: FileStorage, Retention: InterestPolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			checkNumMsgs := func(numExpected int) {
				t.Helper()
				checkFor(t, time.Second, 50*time.Millisecond, func() error {
					if state := mset.state(); state.Msgs != uint64(numExpected) {
						return fmt.Errorf("Expected %d messages, got %d", numExpected, state.Msgs)
					}
					return nil
				})
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			nc.Flush()

			cfg := &ConsumerConfig{Durable: "ivan", DeliverPolicy: DeliverNew, DeliverSubject: sub.Subject, AckPolicy: AckNone}

			o, _ := mset.addConsumer(cfg)

			sendStreamMsg(t, nc, "IK", "M1")
			sendStreamMsg(t, nc, "IK", "M2")

			checkSubPending := func(numExpected int) {
				t.Helper()
				checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != numExpected {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
					}
					return nil
				})
			}

			checkSubPending(2)
			checkNumMsgs(0)

			// Now stop the subscription.
			sub.Unsubscribe()
			checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
				if o.isActive() {
					return fmt.Errorf("Still active consumer")
				}
				return nil
			})

			sendStreamMsg(t, nc, "IK", "M3")
			sendStreamMsg(t, nc, "IK", "M4")

			checkNumMsgs(2)

			// Now restart the durable.
			sub, _ = nc.SubscribeSync(nats.NewInbox())
			nc.Flush()
			cfg.DeliverSubject = sub.Subject
			if o, err = mset.addConsumer(cfg); err != nil {
				t.Fatalf("Error re-establishing the durable consumer: %v", err)
			}
			checkSubPending(2)

			for _, expected := range []string{"M3", "M4"} {
				if m, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				} else if string(m.Data) != expected {
					t.Fatalf("Expected %q, got %q", expected, m.Data)
				}
			}

			// Should all be gone now.
			checkNumMsgs(0)

			// Now restart again and make sure we do not get any messages.
			sub.Unsubscribe()
			checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
				if o.isActive() {
					return fmt.Errorf("Still active consumer")
				}
				return nil
			})
			o.delete()

			sub, _ = nc.SubscribeSync(nats.NewInbox())
			nc.Flush()

			cfg.DeliverSubject = sub.Subject
			cfg.AckPolicy = AckExplicit // Set ack
			if o, err = mset.addConsumer(cfg); err != nil {
				t.Fatalf("Error re-establishing the durable consumer: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
			checkSubPending(0)
			checkNumMsgs(0)

			// Now queue up some messages.
			for i := 1; i <= 10; i++ {
				sendStreamMsg(t, nc, "IK", fmt.Sprintf("M%d", i))
			}
			checkNumMsgs(10)
			checkSubPending(10)

			// Create second consumer
			sub2, _ := nc.SubscribeSync(nats.NewInbox())
			nc.Flush()
			cfg.DeliverSubject = sub2.Subject
			cfg.Durable = "derek"
			o2, err := mset.addConsumer(cfg)
			if err != nil {
				t.Fatalf("Error creating second durable consumer: %v", err)
			}

			// Now queue up some messages.
			for i := 11; i <= 20; i++ {
				sendStreamMsg(t, nc, "IK", fmt.Sprintf("M%d", i))
			}
			checkNumMsgs(20)
			checkSubPending(20)

			// Now make sure deleting the consumers will remove messages from
			// the stream since we are interest retention based.
			o.delete()
			checkNumMsgs(10)

			o2.delete()
			checkNumMsgs(0)
		})
	}
}

func TestJetStreamConsumerReplayRate(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			totalMsgs := 10

			var gaps []time.Duration
			lst := time.Now()

			for i := 0; i < totalMsgs; i++ {
				gaps = append(gaps, time.Since(lst))
				lst = time.Now()
				nc.Publish("DC", []byte("OK!"))
				// Calculate a gap between messages.
				gap := 10*time.Millisecond + time.Duration(rand.Intn(20))*time.Millisecond
				time.Sleep(gap)
			}

			if state := mset.state(); state.Msgs != uint64(totalMsgs) {
				t.Fatalf("Expected %d messages, got %d", totalMsgs, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()

			// Firehose/instant which is default.
			last := time.Now()
			for i := 0; i < totalMsgs; i++ {
				if _, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				now := time.Now()
				// Delivery from addConsumer starts in a go routine, so be
				// more tolerant for the first message.
				limit := 5 * time.Millisecond
				if i == 0 {
					limit = 10 * time.Millisecond
				}
				if now.Sub(last) > limit {
					t.Fatalf("Expected firehose/instant delivery, got message gap of %v", now.Sub(last))
				}
				last = now
			}

			// Now do replay rate to match original.
			o, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject, ReplayPolicy: ReplayOriginal})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()

			// Original rate messsages were received for push based consumer.
			for i := 0; i < totalMsgs; i++ {
				start := time.Now()
				if _, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				gap := time.Since(start)
				// 15ms is high but on macs time.Sleep(delay) does not sleep only delay.
				// Also on travis if things get bogged down this could be delayed.
				gl, gh := gaps[i]-10*time.Millisecond, gaps[i]+15*time.Millisecond
				if gap < gl || gap > gh {
					t.Fatalf("Gap is off for %d, expected %v got %v", i, gaps[i], gap)
				}
			}

			// Now create pull based.
			oc := workerModeConfig("PM")
			oc.ReplayPolicy = ReplayOriginal
			o, err = mset.addConsumer(oc)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()

			for i := 0; i < totalMsgs; i++ {
				start := time.Now()
				if _, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				gap := time.Since(start)
				// 10ms is high but on macs time.Sleep(delay) does not sleep only delay.
				gl, gh := gaps[i]-5*time.Millisecond, gaps[i]+10*time.Millisecond
				if gap < gl || gap > gh {
					t.Fatalf("Gap is incorrect for %d, expected %v got %v", i, gaps[i], gap)
				}
			}
		})
	}
}

func TestJetStreamConsumerReplayRateNoAck(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			totalMsgs := 10
			for i := 0; i < totalMsgs; i++ {
				nc.Request("DC", []byte("Hello World"), time.Second)
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			}
			if state := mset.state(); state.Msgs != uint64(totalMsgs) {
				t.Fatalf("Expected %d messages, got %d", totalMsgs, state.Msgs)
			}
			subj := "d.dc"
			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "derek",
				DeliverSubject: subj,
				AckPolicy:      AckNone,
				ReplayPolicy:   ReplayOriginal,
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()
			// Sleep a random amount of time.
			time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)

			sub, _ := nc.SubscribeSync(subj)
			nc.Flush()

			checkFor(t, time.Second, 25*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != totalMsgs {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, totalMsgs)
				}
				return nil
			})
		})
	}
}

func TestJetStreamConsumerReplayQuit(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 2 msgs
			nc.Request("DC", []byte("OK!"), time.Second)
			time.Sleep(100 * time.Millisecond)
			nc.Request("DC", []byte("OK!"), time.Second)

			if state := mset.state(); state.Msgs != 2 {
				t.Fatalf("Expected %d messages, got %d", 2, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			// Now do replay rate to match original.
			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject, ReplayPolicy: ReplayOriginal})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Allow loop and deliver / replay go routine to spin up
			time.Sleep(50 * time.Millisecond)
			base := runtime.NumGoroutine()
			o.delete()

			checkFor(t, 100*time.Millisecond, 10*time.Millisecond, func() error {
				if runtime.NumGoroutine() >= base {
					return fmt.Errorf("Consumer go routines still running")
				}
				return nil
			})
		})
	}
}

func TestJetStreamSystemLimits(t *testing.T) {
	s := RunRandClientPortServer()
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	if _, _, err := s.JetStreamReservedResources(); err == nil {
		t.Fatalf("Expected error requesting jetstream reserved resources when not enabled")
	}
	// Create some accounts.
	facc, _ := s.LookupOrRegisterAccount("FOO")
	bacc, _ := s.LookupOrRegisterAccount("BAR")
	zacc, _ := s.LookupOrRegisterAccount("BAZ")

	jsconfig := &JetStreamConfig{MaxMemory: 1024, MaxStore: 8192}
	if err := s.EnableJetStream(jsconfig); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if rm, rd, err := s.JetStreamReservedResources(); err != nil {
		t.Fatalf("Unexpected error requesting jetstream reserved resources: %v", err)
	} else if rm != 0 || rd != 0 {
		t.Fatalf("Expected reserved memory and store to be 0, got %d and %d", rm, rd)
	}

	limits := func(mem int64, store int64) map[string]JetStreamAccountLimits {
		return map[string]JetStreamAccountLimits{
			_EMPTY_: {
				MaxMemory:    mem,
				MaxStore:     store,
				MaxStreams:   -1,
				MaxConsumers: -1,
			},
		}
	}

	if err := facc.EnableJetStream(limits(24, 192)); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Use up rest of our resources in memory
	if err := bacc.EnableJetStream(limits(1000, 0)); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now ask for more memory. Should error.
	if err := zacc.EnableJetStream(limits(1000, 0)); err == nil {
		t.Fatalf("Expected an error when exhausting memory resource limits")
	}
	// Disk too.
	if err := zacc.EnableJetStream(limits(0, 10000)); err == nil {
		t.Fatalf("Expected an error when exhausting memory resource limits")
	}
	facc.DisableJetStream()
	bacc.DisableJetStream()
	zacc.DisableJetStream()

	// Make sure we unreserved resources.
	if rm, rd, err := s.JetStreamReservedResources(); err != nil {
		t.Fatalf("Unexpected error requesting jetstream reserved resources: %v", err)
	} else if rm != 0 || rd != 0 {
		t.Fatalf("Expected reserved memory and store to be 0, got %v and %v", friendlyBytes(rm), friendlyBytes(rd))
	}

	if err := facc.EnableJetStream(limits(24, 192)); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Test Adjust
	lim := limits(jsconfig.MaxMemory, jsconfig.MaxStore)
	l := lim[_EMPTY_]
	l.MaxStreams = 10
	l.MaxConsumers = 10
	lim[_EMPTY_] = l
	if err := facc.UpdateJetStreamLimits(lim); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	var msets []*stream
	// Now test max streams and max consumers. Note max consumers is per stream.
	for i := 0; i < 10; i++ {
		mname := fmt.Sprintf("foo.%d", i)
		mset, err := facc.addStream(&StreamConfig{Name: strconv.Itoa(i), Storage: MemoryStorage, Subjects: []string{mname}})
		if err != nil {
			t.Fatalf("Unexpected error adding stream: %v", err)
		}
		msets = append(msets, mset)
	}

	// Remove them all
	for _, mset := range msets {
		mset.delete()
	}

	// Now try to add one with bytes limit that would exceed the account limit.
	if _, err := facc.addStream(&StreamConfig{Name: "22", Storage: MemoryStorage, MaxBytes: jsconfig.MaxStore * 2}); err == nil {
		t.Fatalf("Expected error adding stream over limit")
	}

	// Replicas can't be > 1
	if _, err := facc.addStream(&StreamConfig{Name: "22", Storage: MemoryStorage, Replicas: 10}); err == nil {
		t.Fatalf("Expected error adding stream over limit")
	}

	// Test consumers limit against account limit when the stream does not set a limit
	mset, err := facc.addStream(&StreamConfig{Name: "22", Storage: MemoryStorage, Subjects: []string{"foo.22"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	for i := 0; i < 10; i++ {
		oname := fmt.Sprintf("O:%d", i)
		_, err := mset.addConsumer(&ConsumerConfig{Durable: oname, AckPolicy: AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// This one should fail.
	if _, err := mset.addConsumer(&ConsumerConfig{Durable: "O:22", AckPolicy: AckExplicit}); err == nil {
		t.Fatalf("Expected error adding consumer over the limit")
	}

	// Test consumer limit against stream limit
	mset.delete()
	mset, err = facc.addStream(&StreamConfig{Name: "22", Storage: MemoryStorage, Subjects: []string{"foo.22"}, MaxConsumers: 5})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	for i := 0; i < 5; i++ {
		oname := fmt.Sprintf("O:%d", i)
		_, err := mset.addConsumer(&ConsumerConfig{Durable: oname, AckPolicy: AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// This one should fail.
	if _, err := mset.addConsumer(&ConsumerConfig{Durable: "O:22", AckPolicy: AckExplicit}); err == nil {
		t.Fatalf("Expected error adding consumer over the limit")
	}

	// Test the account having smaller limits than the stream
	mset.delete()

	mset, err = facc.addStream(&StreamConfig{Name: "22", Storage: MemoryStorage, Subjects: []string{"foo.22"}, MaxConsumers: 10})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	l.MaxConsumers = 5
	lim[_EMPTY_] = l
	if err := facc.UpdateJetStreamLimits(lim); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	for i := 0; i < 5; i++ {
		oname := fmt.Sprintf("O:%d", i)
		_, err := mset.addConsumer(&ConsumerConfig{Durable: oname, AckPolicy: AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// This one should fail.
	if _, err := mset.addConsumer(&ConsumerConfig{Durable: "O:22", AckPolicy: AckExplicit}); err == nil {
		t.Fatalf("Expected error adding consumer over the limit")
	}
}

func TestJetStreamSystemLimitsPlacement(t *testing.T) {
	const smallSystemLimit = 128
	const mediumSystemLimit = smallSystemLimit * 2
	const largeSystemLimit = smallSystemLimit * 3

	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {
			max_mem_store: _MAXMEM_
			max_file_store: _MAXFILE_
			store_dir: '%s'
		}

		server_tags: [
			_TAG_
		]

		leaf {
			listen: 127.0.0.1:-1
		}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
	`
	storeCnf := func(serverName, clusterName, storeDir, conf string) string {
		switch serverName {
		case "S-1":
			conf = strings.Replace(conf, "_MAXMEM_", fmt.Sprint(smallSystemLimit), 1)
			conf = strings.Replace(conf, "_MAXFILE_", fmt.Sprint(smallSystemLimit), 1)
			return strings.Replace(conf, "_TAG_", "small", 1)
		case "S-2":
			conf = strings.Replace(conf, "_MAXMEM_", fmt.Sprint(mediumSystemLimit), 1)
			conf = strings.Replace(conf, "_MAXFILE_", fmt.Sprint(mediumSystemLimit), 1)
			return strings.Replace(conf, "_TAG_", "medium", 1)
		case "S-3":
			conf = strings.Replace(conf, "_MAXMEM_", fmt.Sprint(largeSystemLimit), 1)
			conf = strings.Replace(conf, "_MAXFILE_", fmt.Sprint(largeSystemLimit), 1)
			return strings.Replace(conf, "_TAG_", "large", 1)
		default:
			return conf
		}
	}

	cluster := createJetStreamClusterWithTemplateAndModHook(t, tmpl, "cluster-a", 3, storeCnf)
	defer cluster.shutdown()

	requestLeaderStepDown := func(clientURL string) error {
		nc, err := nats.Connect(clientURL)
		if err != nil {
			return err
		}
		defer nc.Close()

		ncResp, err := nc.Request(JSApiLeaderStepDown, nil, 3*time.Second)
		if err != nil {
			return err
		}

		var resp JSApiLeaderStepDownResponse
		if err := json.Unmarshal(ncResp.Data, &resp); err != nil {
			return err
		}
		if resp.Error != nil {
			return resp.Error
		}
		if !resp.Success {
			return fmt.Errorf("leader step down request not successful")
		}

		return nil
	}

	largeSrv := cluster.servers[2]
	// Force large server to be leader
	err := checkForErr(15*time.Second, 500*time.Millisecond, func() error {
		if largeSrv.JetStreamIsLeader() {
			return nil
		}

		if err := requestLeaderStepDown(largeSrv.ClientURL()); err != nil {
			return err
		}
		return fmt.Errorf("large server is not leader")
	})
	if err != nil {
		t.Skipf("failed to get desired layout: %s", err)
	}

	nc, js := jsClientConnect(t, largeSrv)
	defer nc.Close()

	cases := []struct {
		name           string
		storage        nats.StorageType
		createMaxBytes int64
		serverTag      string
		wantErr        bool
	}{
		{
			name:           "file create large stream on small server",
			storage:        nats.FileStorage,
			createMaxBytes: largeSystemLimit,
			serverTag:      "small",
			wantErr:        true,
		},
		{
			name:           "memory create large stream on small server",
			storage:        nats.MemoryStorage,
			createMaxBytes: largeSystemLimit,
			serverTag:      "small",
			wantErr:        true,
		},
		{
			name:           "file create large stream on medium server",
			storage:        nats.FileStorage,
			createMaxBytes: largeSystemLimit,
			serverTag:      "medium",
			wantErr:        true,
		},
		{
			name:           "memory create large stream on medium server",
			storage:        nats.MemoryStorage,
			createMaxBytes: largeSystemLimit,
			serverTag:      "medium",
			wantErr:        true,
		},
		{
			name:           "file create large stream on large server",
			storage:        nats.FileStorage,
			createMaxBytes: largeSystemLimit,
			serverTag:      "large",
		},
		{
			name:           "memory create large stream on large server",
			storage:        nats.MemoryStorage,
			createMaxBytes: largeSystemLimit,
			serverTag:      "large",
		},
	}

	for i := 0; i < len(cases) && !t.Failed(); i++ {
		c := cases[i]
		t.Run(c.name, func(st *testing.T) {
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Storage:  c.storage,
				MaxBytes: c.createMaxBytes,
				Placement: &nats.Placement{
					Cluster: "cluster-a",
					Tags:    []string{c.serverTag},
				},
			})
			if c.wantErr && err == nil {
				st.Fatalf("unexpected stream create success, maxBytes=%d, tag=%s",
					c.createMaxBytes, c.serverTag)
			} else if !c.wantErr && err != nil {
				st.Fatalf("unexpected error: %s", err)
			}

			if err == nil {
				err = js.DeleteStream("TEST")
				require_NoError(st, err)
			}
		})
	}

	// These next two tests should fail because although the stream fits in the
	// large and medium server, it doesn't fit on the small server.
	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  nats.FileStorage,
		MaxBytes: smallSystemLimit + 1,
		Replicas: 3,
	})
	if err == nil {
		t.Fatalf("unexpected file stream create success, maxBytes=%d, replicas=%d",
			si.Config.MaxBytes, si.Config.Replicas)
	}

	si, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  nats.MemoryStorage,
		MaxBytes: smallSystemLimit + 1,
		Replicas: 3,
	})
	if err == nil {
		t.Fatalf("unexpected memory stream create success, maxBytes=%d, replicas=%d",
			si.Config.MaxBytes, si.Config.Replicas)
	}
}

func TestJetStreamStreamLimitUpdate(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	err := s.GlobalAccount().UpdateJetStreamLimits(map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxMemory:  128,
			MaxStore:   128,
			MaxStreams: 1,
		},
	})
	require_NoError(t, err)

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for _, storage := range []nats.StorageType{nats.MemoryStorage, nats.FileStorage} {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Storage:  storage,
			MaxBytes: 32,
		})
		require_NoError(t, err)

		_, err = js.UpdateStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Storage:  storage,
			MaxBytes: 16,
		})
		require_NoError(t, err)

		require_NoError(t, js.DeleteStream("TEST"))
	}
}

func TestJetStreamStreamStorageTrackingAndLimits(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	gacc := s.GlobalAccount()

	al := map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxMemory:    8192,
			MaxStore:     -1,
			MaxStreams:   -1,
			MaxConsumers: -1,
		},
	}

	if err := gacc.UpdateJetStreamLimits(al); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	mset, err := gacc.addStream(&StreamConfig{Name: "LIMITS", Storage: MemoryStorage, Retention: WorkQueuePolicy})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 100
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "LIMITS", "Hello World!")
	}

	state := mset.state()
	usage := gacc.JetStreamUsage()

	// Make sure these are working correctly.
	if state.Bytes != usage.Memory {
		t.Fatalf("Expected to have stream bytes match memory usage, %d vs %d", state.Bytes, usage.Memory)
	}
	if usage.Streams != 1 {
		t.Fatalf("Expected to have 1 stream, got %d", usage.Streams)
	}

	// Do second stream.
	mset2, err := gacc.addStream(&StreamConfig{Name: "NUM22", Storage: MemoryStorage, Retention: WorkQueuePolicy})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset2.delete()

	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "NUM22", "Hello World!")
	}

	stats2 := mset2.state()
	usage = gacc.JetStreamUsage()

	if usage.Memory != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, account is %v, stream1 is %v, stream2 is %v", usage.Memory, state.Bytes, stats2.Bytes)
	}

	// Make sure delete works.
	mset2.delete()
	stats2 = mset2.state()
	usage = gacc.JetStreamUsage()

	if usage.Memory != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, account is %v, stream1 is %v, stream2 is %v", usage.Memory, state.Bytes, stats2.Bytes)
	}

	// Now drain the first one by consuming the messages.
	o, err := mset.addConsumer(workerModeConfig("WQ"))
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	for i := 0; i < toSend; i++ {
		msg, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msg.Respond(nil)
	}
	nc.Flush()

	state = mset.state()
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		usage = gacc.JetStreamUsage()
		if usage.Memory != 0 {
			return fmt.Errorf("Expected usage memory to be 0, got %d", usage.Memory)
		}
		return nil
	})

	// Now send twice the number of messages. Should receive an error at some point, and we will check usage against limits.
	var errSeen string
	for i := 0; i < toSend*2; i++ {
		resp, _ := nc.Request("LIMITS", []byte("The quick brown fox jumped over the..."), 50*time.Millisecond)
		if string(resp.Data) != OK {
			errSeen = string(resp.Data)
			break
		}
	}

	if errSeen == "" {
		t.Fatalf("Expected to see an error when exceeding the account limits")
	}

	state = mset.state()
	var lim JetStreamAccountLimits
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		usage = gacc.JetStreamUsage()
		lim = al[_EMPTY_]
		if usage.Memory > uint64(lim.MaxMemory) {
			return fmt.Errorf("Expected memory to not exceed limit of %d, got %d", lim.MaxMemory, usage.Memory)
		}
		return nil
	})

	// make sure that unlimited accounts work
	lim.MaxMemory = -1

	if err := gacc.UpdateJetStreamLimits(al); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "LIMITS", "Hello World!")
	}
}

func TestJetStreamStreamFileTrackingAndLimits(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	gacc := s.GlobalAccount()

	al := map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxMemory:    8192,
			MaxStore:     9600,
			MaxStreams:   -1,
			MaxConsumers: -1,
		},
	}

	if err := gacc.UpdateJetStreamLimits(al); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	mconfig := &StreamConfig{Name: "LIMITS", Storage: FileStorage, Retention: WorkQueuePolicy}
	mset, err := gacc.addStream(mconfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 100
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "LIMITS", "Hello World!")
	}

	state := mset.state()
	usage := gacc.JetStreamUsage()

	// Make sure these are working correctly.
	if usage.Store != state.Bytes {
		t.Fatalf("Expected to have stream bytes match the store usage, %d vs %d", usage.Store, state.Bytes)
	}
	if usage.Streams != 1 {
		t.Fatalf("Expected to have 1 stream, got %d", usage.Streams)
	}

	// Do second stream.
	mconfig2 := &StreamConfig{Name: "NUM22", Storage: FileStorage, Retention: WorkQueuePolicy}
	mset2, err := gacc.addStream(mconfig2)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset2.delete()

	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "NUM22", "Hello World!")
	}

	stats2 := mset2.state()
	usage = gacc.JetStreamUsage()

	if usage.Store != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, usage is %v, stream1 is %v, stream2 is %v", usage.Store, state.Bytes, stats2.Bytes)
	}

	// Make sure delete works.
	mset2.delete()
	stats2 = mset2.state()
	usage = gacc.JetStreamUsage()

	if usage.Store != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, account is %v, stream1 is %v, stream2 is %v", usage.Store, state.Bytes, stats2.Bytes)
	}

	// Now drain the first one by consuming the messages.
	o, err := mset.addConsumer(workerModeConfig("WQ"))
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	for i := 0; i < toSend; i++ {
		msg, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msg.Respond(nil)
	}
	nc.Flush()

	state = mset.state()
	usage = gacc.JetStreamUsage()

	if usage.Memory != 0 {
		t.Fatalf("Expected usage memeory to be 0, got %d", usage.Memory)
	}

	// Now send twice the number of messages. Should receive an error at some point, and we will check usage against limits.
	var errSeen string
	for i := 0; i < toSend*2; i++ {
		resp, _ := nc.Request("LIMITS", []byte("The quick brown fox jumped over the..."), 50*time.Millisecond)
		if string(resp.Data) != OK {
			errSeen = string(resp.Data)
			break
		}
	}

	if errSeen == "" {
		t.Fatalf("Expected to see an error when exceeding the account limits")
	}

	state = mset.state()
	usage = gacc.JetStreamUsage()

	lim := al[_EMPTY_]
	if usage.Memory > uint64(lim.MaxMemory) {
		t.Fatalf("Expected memory to not exceed limit of %d, got %d", lim.MaxMemory, usage.Memory)
	}
}

func TestJetStreamTieredLimits(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	gacc := s.GlobalAccount()

	tFail := map[string]JetStreamAccountLimits{
		"nottheer": {
			MaxMemory:    8192,
			MaxStore:     9600,
			MaxStreams:   -1,
			MaxConsumers: -1,
		},
	}

	if err := gacc.UpdateJetStreamLimits(tFail); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	mconfig := &StreamConfig{Name: "LIMITS", Storage: FileStorage, Retention: WorkQueuePolicy}
	mset, err := gacc.addStream(mconfig)
	defer mset.delete()
	require_Error(t, err)
	require_Contains(t, err.Error(), "no JetStream default or applicable tiered limit present")

	tPass := map[string]JetStreamAccountLimits{
		"R1": {
			MaxMemory:    8192,
			MaxStore:     9600,
			MaxStreams:   -1,
			MaxConsumers: -1,
		},
	}

	if err := gacc.UpdateJetStreamLimits(tPass); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}
}

type obsi struct {
	cfg ConsumerConfig
	ack int
}

type info struct {
	cfg   StreamConfig
	state StreamState
	obs   []obsi
}

func TestJetStreamSimpleFileRecovery(t *testing.T) {
	base := runtime.NumGoroutine()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	ostate := make(map[string]info)

	nid := nuid.New()
	randomSubject := func() string {
		nid.RandomizePrefix()
		return fmt.Sprintf("SUBJ.%s", nid.Next())
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	numStreams := 10
	for i := 1; i <= numStreams; i++ {
		msetName := fmt.Sprintf("MMS-%d", i)
		subjects := []string{randomSubject(), randomSubject(), randomSubject()}
		msetConfig := StreamConfig{
			Name:     msetName,
			Storage:  FileStorage,
			Subjects: subjects,
			MaxMsgs:  100,
		}
		mset, err := acc.addStream(&msetConfig)
		if err != nil {
			t.Fatalf("Unexpected error adding stream %q: %v", msetName, err)
		}

		toSend := rand.Intn(100) + 1
		for n := 1; n <= toSend; n++ {
			msg := fmt.Sprintf("Hello %d", n*i)
			subj := subjects[rand.Intn(len(subjects))]
			sendStreamMsg(t, nc, subj, msg)
		}
		// Create up to 5 consumers.
		numObs := rand.Intn(5) + 1
		var obs []obsi
		for n := 1; n <= numObs; n++ {
			oname := fmt.Sprintf("WQ-%d-%d", i, n)
			o, err := mset.addConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Now grab some messages.
			toReceive := rand.Intn(toSend) + 1
			rsubj := o.requestNextMsgSubject()
			for r := 0; r < toReceive; r++ {
				resp, err := nc.Request(rsubj, nil, time.Second)
				require_NoError(t, err)
				if resp != nil {
					resp.Respond(nil)
				}
			}
			obs = append(obs, obsi{o.config(), toReceive})
		}
		ostate[msetName] = info{mset.config(), mset.state(), obs}
	}
	pusage := acc.JetStreamUsage()
	nc.Flush()

	// Shutdown and restart and make sure things come back.
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		delta := (runtime.NumGoroutine() - base)
		if delta > 3 {
			return fmt.Errorf("%d Go routines still exist post Shutdown()", delta)
		}
		return nil
	})

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	acc = s.GlobalAccount()

	nusage := acc.JetStreamUsage()
	if !reflect.DeepEqual(nusage, pusage) {
		t.Fatalf("Usage does not match after restore: %+v vs %+v", nusage, pusage)
	}

	for mname, info := range ostate {
		mset, err := acc.lookupStream(mname)
		if err != nil {
			t.Fatalf("Expected to find a stream for %q", mname)
		}
		if state := mset.state(); !reflect.DeepEqual(state, info.state) {
			t.Fatalf("State does not match: %+v vs %+v", state, info.state)
		}
		if cfg := mset.config(); !reflect.DeepEqual(cfg, info.cfg) {
			t.Fatalf("Configs do not match: %+v vs %+v", cfg, info.cfg)
		}
		// Consumers.
		if mset.numConsumers() != len(info.obs) {
			t.Fatalf("Number of consumers do not match: %d vs %d", mset.numConsumers(), len(info.obs))
		}
		for _, oi := range info.obs {
			if o := mset.lookupConsumer(oi.cfg.Durable); o != nil {
				if uint64(oi.ack+1) != o.nextSeq() {
					t.Fatalf("Consumer next seq is not correct: %d vs %d", oi.ack+1, o.nextSeq())
				}
			} else {
				t.Fatalf("Expected to get an consumer")
			}
		}
	}
}

func TestJetStreamPushConsumerFlowControl(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := nc.SubscribeSync(nats.NewInbox())
	require_NoError(t, err)
	defer sub.Unsubscribe()

	obsReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			Durable:        "dlc",
			DeliverSubject: sub.Subject,
			FlowControl:    true,
			Heartbeat:      5 * time.Second,
		},
	}
	req, err := json.Marshal(obsReq)
	require_NoError(t, err)
	resp, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", "dlc"), req, time.Second)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ccResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", ccResp.Error)
	}

	// Grab the low level consumer so we can manually set the fc max.
	if mset, err := s.GlobalAccount().lookupStream("TEST"); err != nil {
		t.Fatalf("Error looking up stream: %v", err)
	} else if obs := mset.lookupConsumer("dlc"); obs == nil {
		t.Fatalf("Error looking up stream: %v", err)
	} else {
		obs.mu.Lock()
		obs.setMaxPendingBytes(16 * 1024)
		obs.mu.Unlock()
	}

	msgSize := 1024
	msg := make([]byte, msgSize)
	crand.Read(msg)

	sendBatch := func(n int) {
		for i := 0; i < n; i++ {
			if _, err := js.Publish("TEST", msg); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	checkSubPending := func(numExpected int) {
		t.Helper()
		checkFor(t, time.Second, 100*time.Millisecond, func() error {
			if nmsgs, _, err := sub.Pending(); err != nil || nmsgs != numExpected {
				return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
			}
			return nil
		})
	}

	sendBatch(100)
	checkSubPending(2) // First four data and flowcontrol from slow start pause.

	var n int
	for m, err := sub.NextMsg(time.Second); err == nil; m, err = sub.NextMsg(time.Second) {
		if m.Subject == "TEST" {
			n++
		} else {
			// This should be a FC control message.
			if m.Header.Get("Status") != "100" {
				t.Fatalf("Expected a 100 status code, got %q", m.Header.Get("Status"))
			}
			if m.Header.Get("Description") != "FlowControl Request" {
				t.Fatalf("Wrong description, got %q", m.Header.Get("Description"))
			}
			m.Respond(nil)
		}
	}

	if n != 100 {
		t.Fatalf("Expected to receive all 100 messages but got %d", n)
	}
}

func TestJetStreamFlowControlRequiresHeartbeats(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "dlc",
		DeliverSubject: nats.NewInbox(),
		FlowControl:    true,
	}); err == nil || IsNatsErr(err, JSConsumerWithFlowControlNeedsHeartbeats) {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamPushConsumerIdleHeartbeats(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := nc.SubscribeSync(nats.NewInbox())
	require_NoError(t, err)
	defer sub.Unsubscribe()

	// Test errors first
	obsReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			DeliverSubject: sub.Subject,
			Heartbeat:      time.Millisecond,
		},
	}
	req, err := json.Marshal(obsReq)
	require_NoError(t, err)
	resp, err := nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "TEST"), req, time.Second)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ccResp.Error == nil {
		t.Fatalf("Expected an error, got none")
	}
	// Set acceptable heartbeat.
	obsReq.Config.Heartbeat = 100 * time.Millisecond
	req, err = json.Marshal(obsReq)
	require_NoError(t, err)
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "TEST"), req, time.Second)
	require_NoError(t, err)
	ccResp.Error = nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkFor(t, time.Second, 20*time.Millisecond, func() error {
		if nmsgs, _, err := sub.Pending(); err != nil || nmsgs < 9 {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, 9)
		}
		return nil
	})
	m, _ := sub.NextMsg(0)
	if m.Header.Get("Status") != "100" {
		t.Fatalf("Expected a 100 status code, got %q", m.Header.Get("Status"))
	}
	if m.Header.Get("Description") != "Idle Heartbeat" {
		t.Fatalf("Wrong description, got %q", m.Header.Get("Description"))
	}
}

func TestJetStreamPushConsumerIdleHeartbeatsWithFilterSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo", "bar"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	hbC := make(chan *nats.Msg, 8)
	sub, err := nc.ChanSubscribe(nats.NewInbox(), hbC)
	require_NoError(t, err)
	defer sub.Unsubscribe()

	obsReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			DeliverSubject: sub.Subject,
			FilterSubject:  "bar",
			Heartbeat:      100 * time.Millisecond,
		},
	}

	req, err := json.Marshal(obsReq)
	require_NoError(t, err)
	resp, err := nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "TEST"), req, time.Second)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	st := time.NewTicker(10 * time.Millisecond)
	defer st.Stop()

	done := time.NewTimer(time.Second)
	defer done.Stop()

	for {
		select {
		case <-st.C:
			js.Publish("foo", []byte("HELLO FOO"))
		case <-done.C:
			t.Fatalf("Expected to have seen idle heartbeats for consumer")
		case <-hbC:
			return
		}
	}
}

func TestJetStreamPushConsumerIdleHeartbeatsWithNoInterest(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	dsubj := "d.22"
	hbC := make(chan *nats.Msg, 8)
	sub, err := nc.ChanSubscribe("d.>", hbC)
	require_NoError(t, err)
	defer sub.Unsubscribe()

	obsReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			DeliverSubject: dsubj,
			Heartbeat:      100 * time.Millisecond,
		},
	}

	req, err := json.Marshal(obsReq)
	require_NoError(t, err)
	resp, err := nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "TEST"), req, time.Second)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ccResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", ccResp.Error)
	}

	done := time.NewTimer(400 * time.Millisecond)
	defer done.Stop()

	for {
		select {
		case <-done.C:
			return
		case m := <-hbC:
			if m.Header.Get("Status") == "100" {
				t.Fatalf("Did not expect to see a heartbeat with no formal interest")
			}
		}
	}
}

func TestJetStreamInfoAPIWithHeaders(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	m := nats.NewMsg(JSApiAccountInfo)
	m.Header.Add("Accept-Encoding", "json")
	m.Header.Add("Authorization", "s3cr3t")
	m.Data = []byte("HELLO-JS!")

	resp, err := nc.RequestMsg(m, time.Second)
	require_NoError(t, err)

	var info JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Error != nil {
		t.Fatalf("Received an error: %+v", info.Error)
	}
}

func TestJetStreamRequestAPI(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// This will get the current information about usage and limits for this account.
	resp, err := nc.Request(JSApiAccountInfo, nil, time.Second)
	require_NoError(t, err)
	var info JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now create a stream.
	msetCfg := StreamConfig{
		Name:     "MSET22",
		Storage:  FileStorage,
		Subjects: []string{"foo", "bar", "baz"},
		MaxMsgs:  100,
	}
	req, err := json.Marshal(msetCfg)
	require_NoError(t, err)
	resp, _ = nc.Request(fmt.Sprintf(JSApiStreamCreateT, msetCfg.Name), req, time.Second)
	var scResp JSApiStreamCreateResponse
	if err := json.Unmarshal(resp.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo == nil || scResp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", scResp.Error)
	}
	if time.Since(scResp.Created) > time.Second {
		t.Fatalf("Created time seems wrong: %v\n", scResp.Created)
	}

	// Check that the name in config has to match the name in the subject
	resp, _ = nc.Request(fmt.Sprintf(JSApiStreamCreateT, "BOB"), req, time.Second)
	scResp.Error, scResp.StreamInfo = nil, nil
	if err := json.Unmarshal(resp.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkNatsError(t, scResp.Error, JSStreamMismatchErr)

	// Check that update works.
	msetCfg.Subjects = []string{"foo", "bar", "baz"}
	msetCfg.MaxBytes = 2222222
	req, err = json.Marshal(msetCfg)
	require_NoError(t, err)
	resp, _ = nc.Request(fmt.Sprintf(JSApiStreamUpdateT, msetCfg.Name), req, time.Second)
	scResp.Error, scResp.StreamInfo = nil, nil
	if err := json.Unmarshal(resp.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo == nil || scResp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", scResp.Error)
	}

	// Check that updating a non existing stream fails
	cfg := StreamConfig{
		Name:     "UNKNOWN_STREAM",
		Storage:  FileStorage,
		Subjects: []string{"foo"},
	}
	req, err = json.Marshal(cfg)
	require_NoError(t, err)
	resp, _ = nc.Request(fmt.Sprintf(JSApiStreamUpdateT, cfg.Name), req, time.Second)
	scResp.Error, scResp.StreamInfo = nil, nil
	if err := json.Unmarshal(resp.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo != nil || scResp.Error == nil || scResp.Error.Code != 404 {
		t.Fatalf("Unexpected error: %+v", scResp.Error)
	}

	// Now lookup info again and see that we can see the new stream.
	resp, err = nc.Request(JSApiAccountInfo, nil, time.Second)
	require_NoError(t, err)
	if err = json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Streams != 1 {
		t.Fatalf("Expected to see 1 Stream, got %d", info.Streams)
	}

	// Make sure list names works.
	resp, err = nc.Request(JSApiStreams, nil, time.Second)
	require_NoError(t, err)
	var namesResponse JSApiStreamNamesResponse
	if err = json.Unmarshal(resp.Data, &namesResponse); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(namesResponse.Streams) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(namesResponse.Streams))
	}
	if namesResponse.Total != 1 {
		t.Fatalf("Expected total to be 1 but got %d", namesResponse.Total)
	}
	if namesResponse.Offset != 0 {
		t.Fatalf("Expected offset to be 0 but got %d", namesResponse.Offset)
	}
	if namesResponse.Limit != JSApiNamesLimit {
		t.Fatalf("Expected limit to be %d but got %d", JSApiNamesLimit, namesResponse.Limit)
	}
	if namesResponse.Streams[0] != msetCfg.Name {
		t.Fatalf("Expected to get %q, but got %q", msetCfg.Name, namesResponse.Streams[0])
	}

	// Now do detailed version.
	resp, err = nc.Request(JSApiStreamList, nil, time.Second)
	require_NoError(t, err)
	var listResponse JSApiStreamListResponse
	if err = json.Unmarshal(resp.Data, &listResponse); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(listResponse.Streams) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(listResponse.Streams))
	}
	if listResponse.Total != 1 {
		t.Fatalf("Expected total to be 1 but got %d", listResponse.Total)
	}
	if listResponse.Offset != 0 {
		t.Fatalf("Expected offset to be 0 but got %d", listResponse.Offset)
	}
	if listResponse.Limit != JSApiListLimit {
		t.Fatalf("Expected limit to be %d but got %d", JSApiListLimit, listResponse.Limit)
	}
	if listResponse.Streams[0].Config.Name != msetCfg.Name {
		t.Fatalf("Expected to get %q, but got %q", msetCfg.Name, listResponse.Streams[0].Config.Name)
	}

	// Now send some messages, then we can poll for info on this stream.
	toSend := 10
	for i := 0; i < toSend; i++ {
		nc.Request("foo", []byte("WELCOME JETSTREAM"), time.Second)
	}

	resp, err = nc.Request(fmt.Sprintf(JSApiStreamInfoT, msetCfg.Name), nil, time.Second)
	require_NoError(t, err)
	var msi StreamInfo
	if err = json.Unmarshal(resp.Data, &msi); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if msi.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected to get %d msgs, got %d", toSend, msi.State.Msgs)
	}
	if time.Since(msi.Created) > time.Second {
		t.Fatalf("Created time seems wrong: %v\n", msi.Created)
	}

	// Looking up one that is not there should yield an error.
	resp, err = nc.Request(fmt.Sprintf(JSApiStreamInfoT, "BOB"), nil, time.Second)
	require_NoError(t, err)
	var bResp JSApiStreamInfoResponse
	if err = json.Unmarshal(resp.Data, &bResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkNatsError(t, bResp.Error, JSStreamNotFoundErr)

	// Now create a consumer.
	delivery := nats.NewInbox()
	obsReq := CreateConsumerRequest{
		Stream: msetCfg.Name,
		Config: ConsumerConfig{DeliverSubject: delivery},
	}
	req, err = json.Marshal(obsReq)
	require_NoError(t, err)
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, msetCfg.Name), req, time.Second)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Ephemerals are now not rejected when there is no interest.
	if ccResp.ConsumerInfo == nil || ccResp.Error != nil {
		t.Fatalf("Got a bad response %+v", ccResp)
	}
	if time.Since(ccResp.Created) > time.Second {
		t.Fatalf("Created time seems wrong: %v\n", ccResp.Created)
	}

	// Now create subscription and make sure we get proper response.
	sub, _ := nc.SubscribeSync(delivery)
	nc.Flush()

	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
		}
		return nil
	})

	// Check that we get an error if the stream name in the subject does not match the config.
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "BOB"), req, time.Second)
	require_NoError(t, err)
	ccResp.Error, ccResp.ConsumerInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Since we do not have interest this should have failed.
	checkNatsError(t, ccResp.Error, JSStreamMismatchErr)

	// Get the list of all of the consumers for our stream.
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumersT, msetCfg.Name), nil, time.Second)
	require_NoError(t, err)
	var clResponse JSApiConsumerNamesResponse
	if err = json.Unmarshal(resp.Data, &clResponse); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(clResponse.Consumers) != 1 {
		t.Fatalf("Expected only 1 consumer but got %d", len(clResponse.Consumers))
	}
	// Now let's get info about our consumer.
	cName := clResponse.Consumers[0]
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerInfoT, msetCfg.Name, cName), nil, time.Second)
	require_NoError(t, err)
	var oinfo ConsumerInfo
	if err = json.Unmarshal(resp.Data, &oinfo); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Do some sanity checking.
	// Must match consumer.go
	const randConsumerNameLen = 8
	if len(oinfo.Name) != randConsumerNameLen {
		t.Fatalf("Expected ephemeral name, got %q", oinfo.Name)
	}
	if len(oinfo.Config.Durable) != 0 {
		t.Fatalf("Expected no durable name, but got %q", oinfo.Config.Durable)
	}
	if oinfo.Config.DeliverSubject != delivery {
		t.Fatalf("Expected to have delivery subject of %q, got %q", delivery, oinfo.Config.DeliverSubject)
	}
	if oinfo.Delivered.Consumer != 10 {
		t.Fatalf("Expected consumer delivered sequence of 10, got %d", oinfo.Delivered.Consumer)
	}
	if oinfo.AckFloor.Consumer != 10 {
		t.Fatalf("Expected ack floor to be 10, got %d", oinfo.AckFloor.Consumer)
	}

	// Now delete the consumer.
	resp, _ = nc.Request(fmt.Sprintf(JSApiConsumerDeleteT, msetCfg.Name, cName), nil, time.Second)
	var cdResp JSApiConsumerDeleteResponse
	if err = json.Unmarshal(resp.Data, &cdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !cdResp.Success || cdResp.Error != nil {
		t.Fatalf("Got a bad response %+v", ccResp)
	}

	// Make sure we can't create a durable using the ephemeral API endpoint.
	obsReq = CreateConsumerRequest{
		Stream: msetCfg.Name,
		Config: ConsumerConfig{Durable: "myd", DeliverSubject: delivery},
	}
	req, err = json.Marshal(obsReq)
	require_NoError(t, err)
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, msetCfg.Name), req, time.Second)
	require_NoError(t, err)
	ccResp.Error, ccResp.ConsumerInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkNatsError(t, ccResp.Error, JSConsumerEphemeralWithDurableNameErr)

	// Now make sure we can create a durable on the subject with the proper name.
	resp, err = nc.Request(fmt.Sprintf(JSApiDurableCreateT, msetCfg.Name, obsReq.Config.Durable), req, time.Second)
	ccResp.Error, ccResp.ConsumerInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ccResp.ConsumerInfo == nil || ccResp.Error != nil {
		t.Fatalf("Did not receive correct response")
	}

	// Make sure empty durable in cfg does not work
	obsReq2 := CreateConsumerRequest{
		Stream: msetCfg.Name,
		Config: ConsumerConfig{DeliverSubject: delivery},
	}
	req2, err := json.Marshal(obsReq2)
	require_NoError(t, err)
	resp, err = nc.Request(fmt.Sprintf(JSApiDurableCreateT, msetCfg.Name, obsReq.Config.Durable), req2, time.Second)
	require_NoError(t, err)
	ccResp.Error, ccResp.ConsumerInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkNatsError(t, ccResp.Error, JSConsumerDurableNameNotSetErr)

	// Now delete a msg.
	dreq := JSApiMsgDeleteRequest{Seq: 2}
	dreqj, err := json.Marshal(dreq)
	require_NoError(t, err)
	resp, _ = nc.Request(fmt.Sprintf(JSApiMsgDeleteT, msetCfg.Name), dreqj, time.Second)
	var delMsgResp JSApiMsgDeleteResponse
	if err = json.Unmarshal(resp.Data, &delMsgResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !delMsgResp.Success || delMsgResp.Error != nil {
		t.Fatalf("Got a bad response %+v", delMsgResp.Error)
	}

	// Now purge the stream.
	resp, _ = nc.Request(fmt.Sprintf(JSApiStreamPurgeT, msetCfg.Name), nil, time.Second)
	var pResp JSApiStreamPurgeResponse
	if err = json.Unmarshal(resp.Data, &pResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !pResp.Success || pResp.Error != nil {
		t.Fatalf("Got a bad response %+v", pResp)
	}
	if pResp.Purged != 9 {
		t.Fatalf("Expected 9 purged, got %d", pResp.Purged)
	}

	// Now delete the stream.
	resp, _ = nc.Request(fmt.Sprintf(JSApiStreamDeleteT, msetCfg.Name), nil, time.Second)
	var dResp JSApiStreamDeleteResponse
	if err = json.Unmarshal(resp.Data, &dResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !dResp.Success || dResp.Error != nil {
		t.Fatalf("Got a bad response %+v", dResp.Error)
	}

	// Now grab stats again.
	// This will get the current information about usage and limits for this account.
	resp, err = nc.Request(JSApiAccountInfo, nil, time.Second)
	require_NoError(t, err)
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Streams != 0 {
		t.Fatalf("Expected no remaining streams, got %d", info.Streams)
	}

	// Now do templates.
	mcfg := &StreamConfig{
		Subjects:  []string{"kv.*"},
		Retention: LimitsPolicy,
		MaxAge:    time.Hour,
		MaxMsgs:   4,
		Storage:   MemoryStorage,
		Replicas:  1,
	}
	template := &StreamTemplateConfig{
		Name:       "kv",
		Config:     mcfg,
		MaxStreams: 4,
	}
	req, err = json.Marshal(template)
	require_NoError(t, err)

	// Check that the name in config has to match the name in the subject
	resp, _ = nc.Request(fmt.Sprintf(JSApiTemplateCreateT, "BOB"), req, time.Second)
	var stResp JSApiStreamTemplateCreateResponse
	if err = json.Unmarshal(resp.Data, &stResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkNatsError(t, stResp.Error, JSTemplateNameNotMatchSubjectErr)

	resp, _ = nc.Request(fmt.Sprintf(JSApiTemplateCreateT, template.Name), req, time.Second)
	stResp.Error, stResp.StreamTemplateInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &stResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if stResp.StreamTemplateInfo == nil || stResp.Error != nil {
		t.Fatalf("Did not receive correct response")
	}

	// Create a second one.
	template.Name = "ss"
	template.Config.Subjects = []string{"foo", "bar"}

	req, err = json.Marshal(template)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	resp, _ = nc.Request(fmt.Sprintf(JSApiTemplateCreateT, template.Name), req, time.Second)
	stResp.Error, stResp.StreamTemplateInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &stResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if stResp.StreamTemplateInfo == nil || stResp.Error != nil {
		t.Fatalf("Did not receive correct response")
	}

	// Now grab the list of templates
	var tListResp JSApiStreamTemplateNamesResponse
	resp, err = nc.Request(JSApiTemplates, nil, time.Second)
	if err = json.Unmarshal(resp.Data, &tListResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(tListResp.Templates) != 2 {
		t.Fatalf("Expected 2 templates but got %d", len(tListResp.Templates))
	}
	sort.Strings(tListResp.Templates)
	if tListResp.Templates[0] != "kv" {
		t.Fatalf("Expected to get %q, but got %q", "kv", tListResp.Templates[0])
	}
	if tListResp.Templates[1] != "ss" {
		t.Fatalf("Expected to get %q, but got %q", "ss", tListResp.Templates[1])
	}

	// Now delete one.
	// Test bad name.
	resp, _ = nc.Request(fmt.Sprintf(JSApiTemplateDeleteT, "bob"), nil, time.Second)
	var tDeleteResp JSApiStreamTemplateDeleteResponse
	if err = json.Unmarshal(resp.Data, &tDeleteResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkNatsError(t, tDeleteResp.Error, JSStreamTemplateNotFoundErr)

	resp, _ = nc.Request(fmt.Sprintf(JSApiTemplateDeleteT, "ss"), nil, time.Second)
	tDeleteResp.Error = nil
	if err = json.Unmarshal(resp.Data, &tDeleteResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !tDeleteResp.Success || tDeleteResp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", tDeleteResp.Error)
	}

	resp, err = nc.Request(JSApiTemplates, nil, time.Second)
	tListResp.Error, tListResp.Templates = nil, nil
	if err = json.Unmarshal(resp.Data, &tListResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(tListResp.Templates) != 1 {
		t.Fatalf("Expected 1 template but got %d", len(tListResp.Templates))
	}
	if tListResp.Templates[0] != "kv" {
		t.Fatalf("Expected to get %q, but got %q", "kv", tListResp.Templates[0])
	}

	// First create a stream from the template
	sendStreamMsg(t, nc, "kv.22", "derek")
	// Last do info
	resp, err = nc.Request(fmt.Sprintf(JSApiTemplateInfoT, "kv"), nil, time.Second)
	require_NoError(t, err)
	var ti StreamTemplateInfo
	if err = json.Unmarshal(resp.Data, &ti); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(ti.Streams) != 1 {
		t.Fatalf("Expected 1 stream, got %d", len(ti.Streams))
	}
	if ti.Streams[0] != canonicalName("kv.22") {
		t.Fatalf("Expected stream with name %q, but got %q", canonicalName("kv.22"), ti.Streams[0])
	}

	// Test that we can send nil or an empty legal json for requests that take no args.
	// We know this stream does not exist, this just checking request processing.
	checkEmptyReqArg := func(arg string) {
		t.Helper()
		var req []byte
		if len(arg) > 0 {
			req = []byte(arg)
		}
		resp, err = nc.Request(fmt.Sprintf(JSApiStreamDeleteT, "foo_bar_baz"), req, time.Second)
		var dResp JSApiStreamDeleteResponse
		if err = json.Unmarshal(resp.Data, &dResp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if dResp.Error == nil || dResp.Error.Code != 404 {
			t.Fatalf("Got a bad response, expected a 404 response %+v", dResp.Error)
		}
	}

	checkEmptyReqArg("")
	checkEmptyReqArg("{}")
	checkEmptyReqArg(" {} ")
	checkEmptyReqArg(" { } ")
}

func TestJetStreamFilteredStreamNames(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Create some streams.
	var snid int
	createStream := func(subjects []string) {
		t.Helper()
		snid++
		name := fmt.Sprintf("S-%d", snid)
		sc := &StreamConfig{Name: name, Subjects: subjects}
		if _, err := s.GlobalAccount().addStream(sc); err != nil {
			t.Fatalf("Unexpected error adding stream: %v", err)
		}
	}

	createStream([]string{"foo"})                  // S1
	createStream([]string{"bar"})                  // S2
	createStream([]string{"baz"})                  // S3
	createStream([]string{"foo.*", "bar.*"})       // S4
	createStream([]string{"foo-1.22", "bar-1.33"}) // S5

	expectStreams := func(filter string, streams []string) {
		t.Helper()
		req, _ := json.Marshal(&JSApiStreamNamesRequest{Subject: filter})
		r, _ := nc.Request(JSApiStreams, req, time.Second)
		var resp JSApiStreamNamesResponse
		if err := json.Unmarshal(r.Data, &resp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(resp.Streams) != len(streams) {
			t.Fatalf("Expected %d results, got %d", len(streams), len(resp.Streams))
		}
	}

	expectStreams("foo", []string{"S1"})
	expectStreams("bar", []string{"S2"})
	expectStreams("baz", []string{"S3"})
	expectStreams("*", []string{"S1", "S2", "S3"})
	expectStreams(">", []string{"S1", "S2", "S3", "S4", "S5"})
	expectStreams("*.*", []string{"S4", "S5"})
	expectStreams("*.22", []string{"S4", "S5"})
}

func TestJetStreamUpdateStream(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   FileStorage,
				Replicas:  1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// Test basic updates. We allow changing the subjects, limits, and no_ack along with replicas(TBD w/ cluster)
			cfg := *c.mconfig

			// Can't change name.
			cfg.Name = "bar"
			if err := mset.update(&cfg); err == nil || !strings.Contains(err.Error(), "name must match") {
				t.Fatalf("Expected error trying to update name")
			}
			// Can't change max consumers for now.
			cfg = *c.mconfig
			cfg.MaxConsumers = 10
			if err := mset.update(&cfg); err == nil || !strings.Contains(err.Error(), "can not change") {
				t.Fatalf("Expected error trying to change MaxConsumers")
			}
			// Can't change storage types.
			cfg = *c.mconfig
			if cfg.Storage == FileStorage {
				cfg.Storage = MemoryStorage
			} else {
				cfg.Storage = FileStorage
			}
			if err := mset.update(&cfg); err == nil || !strings.Contains(err.Error(), "can not change") {
				t.Fatalf("Expected error trying to change Storage")
			}
			// Can't change replicas > 1 for now.
			cfg = *c.mconfig
			cfg.Replicas = 10
			if err := mset.update(&cfg); err == nil || !strings.Contains(err.Error(), "maximum replicas") {
				t.Fatalf("Expected error trying to change Replicas")
			}
			// Can't have a template set for now.
			cfg = *c.mconfig
			cfg.Template = "baz"
			if err := mset.update(&cfg); err == nil || !strings.Contains(err.Error(), "template") {
				t.Fatalf("Expected error trying to change Template owner")
			}
			// Can't change limits policy.
			cfg = *c.mconfig
			cfg.Retention = WorkQueuePolicy
			if err := mset.update(&cfg); err == nil || !strings.Contains(err.Error(), "can not change") {
				t.Fatalf("Expected error trying to change Retention")
			}

			// Now test changing limits.
			nc := clientConnectToServer(t, s)
			defer nc.Close()

			pending := uint64(100)
			for i := uint64(0); i < pending; i++ {
				sendStreamMsg(t, nc, "foo", "0123456789")
			}
			pendingBytes := mset.state().Bytes

			checkPending := func(msgs, bts uint64) {
				t.Helper()
				state := mset.state()
				if state.Msgs != msgs {
					t.Fatalf("Expected %d messages, got %d", msgs, state.Msgs)
				}
				if state.Bytes != bts {
					t.Fatalf("Expected %d bytes, got %d", bts, state.Bytes)
				}
			}
			checkPending(pending, pendingBytes)

			// Update msgs to higher.
			cfg = *c.mconfig
			cfg.MaxMsgs = int64(pending * 2)
			if err := mset.update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			if mset.config().MaxMsgs != cfg.MaxMsgs {
				t.Fatalf("Expected the change to take effect, %d vs %d", mset.config().MaxMsgs, cfg.MaxMsgs)
			}
			checkPending(pending, pendingBytes)

			// Update msgs to lower.
			cfg = *c.mconfig
			cfg.MaxMsgs = int64(pending / 2)
			if err := mset.update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			if mset.config().MaxMsgs != cfg.MaxMsgs {
				t.Fatalf("Expected the change to take effect, %d vs %d", mset.config().MaxMsgs, cfg.MaxMsgs)
			}
			checkPending(pending/2, pendingBytes/2)
			// Now do bytes.
			cfg = *c.mconfig
			cfg.MaxBytes = int64(pendingBytes / 4)
			if err := mset.update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			if mset.config().MaxBytes != cfg.MaxBytes {
				t.Fatalf("Expected the change to take effect, %d vs %d", mset.config().MaxBytes, cfg.MaxBytes)
			}
			checkPending(pending/4, pendingBytes/4)

			// Now do age.
			cfg = *c.mconfig
			cfg.MaxAge = 100 * time.Millisecond
			if err := mset.update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			// Just wait a bit for expiration.
			time.Sleep(200 * time.Millisecond)
			if mset.config().MaxAge != cfg.MaxAge {
				t.Fatalf("Expected the change to take effect, %d vs %d", mset.config().MaxAge, cfg.MaxAge)
			}
			checkPending(0, 0)

			// Now put back to original.
			cfg = *c.mconfig
			if err := mset.update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			for i := uint64(0); i < pending; i++ {
				sendStreamMsg(t, nc, "foo", "0123456789")
			}

			// subject changes.
			// Add in a subject first.
			cfg = *c.mconfig
			cfg.Subjects = []string{"foo", "bar"}
			if err := mset.update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			// Make sure we can still send to foo.
			sendStreamMsg(t, nc, "foo", "0123456789")
			// And we can now send to bar.
			sendStreamMsg(t, nc, "bar", "0123456789")
			// Now delete both and change to baz only.
			cfg.Subjects = []string{"baz"}
			if err := mset.update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			// Make sure we do not get response acks for "foo" or "bar".
			if resp, err := nc.Request("foo", nil, 25*time.Millisecond); err == nil || resp != nil {
				t.Fatalf("Expected no response from jetstream for deleted subject: %q", "foo")
			}
			if resp, err := nc.Request("bar", nil, 25*time.Millisecond); err == nil || resp != nil {
				t.Fatalf("Expected no response from jetstream for deleted subject: %q", "bar")
			}
			// Make sure we can send to "baz"
			sendStreamMsg(t, nc, "baz", "0123456789")
			if nmsgs := mset.state().Msgs; nmsgs != pending+3 {
				t.Fatalf("Expected %d msgs, got %d", pending+3, nmsgs)
			}

			// FileStore restarts for config save.
			cfg = *c.mconfig
			if cfg.Storage == FileStorage {
				cfg.Subjects = []string{"foo", "bar"}
				cfg.MaxMsgs = 2222
				cfg.MaxBytes = 3333333
				cfg.MaxAge = 22 * time.Hour
				if err := mset.update(&cfg); err != nil {
					t.Fatalf("Unexpected error %v", err)
				}
				// Pull since certain defaults etc are set in processing.
				cfg = mset.config()

				// Restart the
				// Capture port since it was dynamic.
				u, _ := url.Parse(s.ClientURL())
				port, _ := strconv.Atoi(u.Port())

				// Stop current
				sd := s.JetStreamConfig().StoreDir
				s.Shutdown()
				// Restart.
				s = RunJetStreamServerOnPort(port, sd)
				defer s.Shutdown()

				mset, err = s.GlobalAccount().lookupStream(cfg.Name)
				if err != nil {
					t.Fatalf("Expected to find a stream for %q", cfg.Name)
				}
				restored_cfg := mset.config()
				if !reflect.DeepEqual(cfg, restored_cfg) {
					t.Fatalf("restored configuration does not match: \n%+v\n vs \n%+v", restored_cfg, cfg)
				}
			}
		})
	}
}

func TestJetStreamDeleteMsg(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   FileStorage,
				Replicas:  1,
			}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			pubTen := func() {
				t.Helper()
				for i := 0; i < 10; i++ {
					js.Publish("foo", []byte("Hello World!"))
				}
			}

			pubTen()

			state := mset.state()
			if state.Msgs != 10 {
				t.Fatalf("Expected 10 messages, got %d", state.Msgs)
			}
			bytesPerMsg := state.Bytes / 10
			if bytesPerMsg == 0 {
				t.Fatalf("Expected non-zero bytes for msg size")
			}

			deleteAndCheck := func(seq, expectedFirstSeq uint64) {
				t.Helper()
				beforeState := mset.state()
				if removed, _ := mset.deleteMsg(seq); !removed {
					t.Fatalf("Expected the delete of sequence %d to succeed", seq)
				}
				expectedState := beforeState
				expectedState.Msgs--
				expectedState.Bytes -= bytesPerMsg
				expectedState.FirstSeq = expectedFirstSeq

				sm, err := mset.getMsg(expectedFirstSeq)
				if err != nil {
					t.Fatalf("Error fetching message for seq: %d - %v", expectedFirstSeq, err)
				}
				expectedState.FirstTime = sm.Time
				expectedState.Deleted = nil
				expectedState.NumDeleted = 0

				afterState := mset.state()
				afterState.Deleted = nil
				afterState.NumDeleted = 0

				// Ignore first time in this test.
				if !reflect.DeepEqual(afterState, expectedState) {
					t.Fatalf("Stats not what we expected. Expected %+v, got %+v\n", expectedState, afterState)
				}
			}

			// Delete one from the middle
			deleteAndCheck(5, 1)
			// Now make sure sequences are updated properly.
			// Delete first msg.
			deleteAndCheck(1, 2)
			// Now last
			deleteAndCheck(10, 2)
			// Now gaps.
			deleteAndCheck(3, 2)
			deleteAndCheck(2, 4)

			mset.purge(nil)
			// Put ten more one.
			pubTen()
			deleteAndCheck(11, 12)
			deleteAndCheck(15, 12)
			deleteAndCheck(16, 12)
			deleteAndCheck(20, 12)

			// Only file storage beyond here.
			if c.mconfig.Storage == MemoryStorage {
				return
			}

			// Capture port since it was dynamic.
			u, _ := url.Parse(s.ClientURL())
			port, _ := strconv.Atoi(u.Port())
			sd := s.JetStreamConfig().StoreDir

			// Shutdown the
			s.Shutdown()

			s = RunJetStreamServerOnPort(port, sd)
			defer s.Shutdown()

			mset, err = s.GlobalAccount().lookupStream("foo")
			if err != nil {
				t.Fatalf("Expected to get the stream back")
			}

			expected := StreamState{Msgs: 6, Bytes: 6 * bytesPerMsg, FirstSeq: 12, LastSeq: 20, NumSubjects: 1}
			state = mset.state()
			state.FirstTime, state.LastTime, state.Deleted, state.NumDeleted = time.Time{}, time.Time{}, nil, 0

			if !reflect.DeepEqual(expected, state) {
				t.Fatalf("State not what we expected. Expected %+v, got %+v\n", expected, state)
			}

			// Now create an consumer and make sure we get the right sequence.
			nc = clientConnectToServer(t, s)
			defer nc.Close()

			delivery := nats.NewInbox()
			sub, _ := nc.SubscribeSync(delivery)
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery, FilterSubject: "foo"})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			expectedStoreSeq := []uint64{12, 13, 14, 17, 18, 19}

			for i := 0; i < 6; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if o.streamSeqFromReply(m.Reply) != expectedStoreSeq[i] {
					t.Fatalf("Expected store seq of %d, got %d", expectedStoreSeq[i], o.streamSeqFromReply(m.Reply))
				}
			}
		})
	}
}

// https://github.com/nats-io/jetstream/issues/396
func TestJetStreamLimitLockBug(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxMsgs:   10,
				Storage:   MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxMsgs:   10,
				Storage:   FileStorage,
				Replicas:  1,
			}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			for i := 0; i < 100; i++ {
				sendStreamMsg(t, nc, "foo", "ok")
			}

			state := mset.state()
			if state.Msgs != 10 {
				t.Fatalf("Expected 10 messages, got %d", state.Msgs)
			}
		})
	}
}

func TestJetStreamNextMsgNoInterest(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   FileStorage,
				Replicas:  1,
			}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			cfg := &StreamConfig{Name: "foo", Storage: FileStorage}
			mset, err := s.GlobalAccount().addStream(cfg)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}

			nc := clientConnectWithOldRequest(t, s)
			defer nc.Close()

			// Now create an consumer and make sure it functions properly.
			o, err := mset.addConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			nextSubj := o.requestNextMsgSubject()

			// Queue up a worker but use a short time out.
			if _, err := nc.Request(nextSubj, nil, time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Expected a timeout error and no response with acks suppressed")
			}
			// Now send a message, the worker from above will still be known but we want to make
			// sure the system detects that so we will do a request for next msg right behind it.
			nc.Publish("foo", []byte("OK"))
			if msg, err := nc.Request(nextSubj, nil, 5*time.Millisecond); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			} else {
				msg.Respond(nil) // Ack
			}
			// Now queue up 10 workers.
			for i := 0; i < 10; i++ {
				if _, err := nc.Request(nextSubj, nil, time.Microsecond); err != nats.ErrTimeout {
					t.Fatalf("Expected a timeout error and no response with acks suppressed")
				}
			}
			// Now publish ten messages.
			for i := 0; i < 10; i++ {
				nc.Publish("foo", []byte("OK"))
			}
			nc.Flush()
			for i := 0; i < 10; i++ {
				if msg, err := nc.Request(nextSubj, nil, 10*time.Millisecond); err != nil {
					t.Fatalf("Unexpected error for %d: %v", i, err)
				} else {
					msg.Respond(nil) // Ack
				}
			}
			nc.Flush()
			ostate := o.info()
			if ostate.AckFloor.Stream != 11 || ostate.NumAckPending > 0 {
				t.Fatalf("Inconsistent ack state: %+v", ostate)
			}
		})
	}
}

func TestJetStreamMsgHeaders(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   FileStorage,
				Replicas:  1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			m := nats.NewMsg("foo")
			m.Header.Add("Accept-Encoding", "json")
			m.Header.Add("Authorization", "s3cr3t")
			m.Data = []byte("Hello JetStream Headers - #1!")

			nc.PublishMsg(m)
			nc.Flush()

			checkFor(t, time.Second*2, time.Millisecond*250, func() error {
				state := mset.state()
				if state.Msgs != 1 {
					return fmt.Errorf("Expected 1 message, got %d", state.Msgs)
				}
				if state.Bytes == 0 {
					return fmt.Errorf("Expected non-zero bytes")
				}
				return nil
			})

			// Now access raw from stream.
			sm, err := mset.getMsg(1)
			if err != nil {
				t.Fatalf("Unexpected error getting stored message: %v", err)
			}
			// Calculate the []byte version of the headers.
			var b bytes.Buffer
			b.WriteString("NATS/1.0\r\n")
			http.Header(m.Header).Write(&b)
			b.WriteString("\r\n")
			hdr := b.Bytes()

			if !bytes.Equal(sm.Header, hdr) {
				t.Fatalf("Message headers do not match, %q vs %q", hdr, sm.Header)
			}
			if !bytes.Equal(sm.Data, m.Data) {
				t.Fatalf("Message data do not match, %q vs %q", m.Data, sm.Data)
			}

			// Now do consumer based.
			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			cm, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("Error getting message: %v", err)
			}
			// Check the message.
			// Check out original headers.
			if cm.Header.Get("Accept-Encoding") != "json" ||
				cm.Header.Get("Authorization") != "s3cr3t" {
				t.Fatalf("Original headers not present")
			}
			if !bytes.Equal(m.Data, cm.Data) {
				t.Fatalf("Message payloads are not the same: %q vs %q", cm.Data, m.Data)
			}
		})
	}
}

func TestJetStreamTemplateBasics(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	mcfg := &StreamConfig{
		Subjects:  []string{"kv.*"},
		Retention: LimitsPolicy,
		MaxAge:    time.Hour,
		MaxMsgs:   4,
		Storage:   MemoryStorage,
		Replicas:  1,
	}
	template := &StreamTemplateConfig{
		Name:       "kv",
		Config:     mcfg,
		MaxStreams: 4,
	}

	if _, err := acc.addStreamTemplate(template); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if templates := acc.templates(); len(templates) != 1 {
		t.Fatalf("Expected to get array of 1 template, got %d", len(templates))
	}
	if err := acc.deleteStreamTemplate("foo"); err == nil {
		t.Fatalf("Expected an error for non-existent template")
	}
	if err := acc.deleteStreamTemplate(template.Name); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if templates := acc.templates(); len(templates) != 0 {
		t.Fatalf("Expected to get array of no templates, got %d", len(templates))
	}
	// Add it back in and test basics
	if _, err := acc.addStreamTemplate(template); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Connect a client and send a message which should trigger the stream creation.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sendStreamMsg(t, nc, "kv.22", "derek")
	sendStreamMsg(t, nc, "kv.33", "cat")
	sendStreamMsg(t, nc, "kv.44", "sam")
	sendStreamMsg(t, nc, "kv.55", "meg")

	if nms := acc.numStreams(); nms != 4 {
		t.Fatalf("Expected 4 auto-created streams, got %d", nms)
	}

	// This one should fail due to max.
	if resp, err := nc.Request("kv.99", nil, 100*time.Millisecond); err == nil {
		t.Fatalf("Expected this to fail, but got %q", resp.Data)
	}

	// Now delete template and make sure the underlying streams go away too.
	if err := acc.deleteStreamTemplate(template.Name); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if nms := acc.numStreams(); nms != 0 {
		t.Fatalf("Expected no auto-created streams to remain, got %d", nms)
	}
}

func TestJetStreamTemplateFileStoreRecovery(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	mcfg := &StreamConfig{
		Subjects:  []string{"kv.*"},
		Retention: LimitsPolicy,
		MaxAge:    time.Hour,
		MaxMsgs:   50,
		Storage:   FileStorage,
		Replicas:  1,
	}
	template := &StreamTemplateConfig{
		Name:       "kv",
		Config:     mcfg,
		MaxStreams: 100,
	}

	if _, err := acc.addStreamTemplate(template); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we can not add in a stream on our own with a template owner.
	badCfg := *mcfg
	badCfg.Name = "bad"
	badCfg.Template = "kv"
	if _, err := acc.addStream(&badCfg); err == nil {
		t.Fatalf("Expected error adding stream with direct template owner")
	}

	// Connect a client and send a message which should trigger the stream creation.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	for i := 1; i <= 100; i++ {
		subj := fmt.Sprintf("kv.%d", i)
		for x := 0; x < 50; x++ {
			sendStreamMsg(t, nc, subj, "Hello")
		}
	}
	nc.Flush()

	if nms := acc.numStreams(); nms != 100 {
		t.Fatalf("Expected 100 auto-created streams, got %d", nms)
	}

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())

	restartServer := func() {
		t.Helper()
		sd := s.JetStreamConfig().StoreDir
		// Stop current
		s.Shutdown()
		// Restart.
		s = RunJetStreamServerOnPort(port, sd)
	}

	// Restart.
	restartServer()
	defer s.Shutdown()

	acc = s.GlobalAccount()
	if nms := acc.numStreams(); nms != 100 {
		t.Fatalf("Expected 100 auto-created streams, got %d", nms)
	}
	tmpl, err := acc.lookupStreamTemplate(template.Name)
	require_NoError(t, err)
	// Make sure t.delete() survives restart.
	tmpl.delete()

	// Restart.
	restartServer()
	defer s.Shutdown()

	acc = s.GlobalAccount()
	if nms := acc.numStreams(); nms != 0 {
		t.Fatalf("Expected no auto-created streams, got %d", nms)
	}
	if _, err := acc.lookupStreamTemplate(template.Name); err == nil {
		t.Fatalf("Expected to not find the template after restart")
	}
}

// This will be testing our ability to conditionally rewrite subjects for last mile
// when working with JetStream. Consumers receive messages that have their subjects
// rewritten to match the original subject. NATS routing is all subject based except
// for the last mile to the client.
func TestJetStreamSingleInstanceRemoteAccess(t *testing.T) {
	ca := createClusterWithName(t, "A", 1)
	defer shutdownCluster(ca)
	cb := createClusterWithName(t, "B", 1, ca)
	defer shutdownCluster(cb)

	// Connect our leafnode server to cluster B.
	opts := cb.opts[rand.Intn(len(cb.opts))]
	s, _ := runSolicitLeafServer(opts)
	defer s.Shutdown()

	checkLeafNodeConnected(t, s)

	if err := s.EnableJetStream(nil); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: "foo", Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 10
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "foo", "Hello World!")
	}

	// Now create a push based consumer. Connected to the non-jetstream server via a random server on cluster A.
	sl := ca.servers[rand.Intn(len(ca.servers))]
	nc2 := clientConnectToServer(t, sl)
	defer nc2.Close()

	sub, _ := nc2.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()

	// Need to wait for interest to propagate across GW.
	nc2.Flush()
	time.Sleep(25 * time.Millisecond)

	o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	checkSubPending := func(numExpected int) {
		t.Helper()
		checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != numExpected {
				return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
			}
			return nil
		})
	}
	checkSubPending(toSend)

	checkMsg := func(m *nats.Msg, err error, i int) {
		t.Helper()
		if err != nil {
			t.Fatalf("Got an error checking message: %v", err)
		}
		if m.Subject != "foo" {
			t.Fatalf("Expected original subject of %q, but got %q", "foo", m.Subject)
		}
		// Now check that reply subject exists and has a sequence as the last token.
		if seq := o.seqFromReply(m.Reply); seq != uint64(i) {
			t.Fatalf("Expected sequence of %d , got %d", i, seq)
		}
	}

	// Now check the subject to make sure its the original one.
	for i := 1; i <= toSend; i++ {
		m, err := sub.NextMsg(time.Second)
		checkMsg(m, err, i)
	}

	// Now do a pull based consumer.
	o, err = mset.addConsumer(workerModeConfig("p"))
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	nextMsg := o.requestNextMsgSubject()
	for i := 1; i <= toSend; i++ {
		m, err := nc.Request(nextMsg, nil, time.Second)
		checkMsg(m, err, i)
	}
}

func clientConnectToServerWithUP(t *testing.T, opts *Options, user, pass string) *nats.Conn {
	curl := fmt.Sprintf("nats://%s:%s@%s:%d", user, pass, opts.Host, opts.Port)
	nc, err := nats.Connect(curl, nats.Name("JS-UP-TEST"), nats.ReconnectWait(5*time.Millisecond), nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	return nc
}

func TestJetStreamCanNotEnableOnSystemAccount(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	sa := s.SystemAccount()
	if err := sa.EnableJetStream(nil); err == nil {
		t.Fatalf("Expected an error trying to enable on the system account")
	}
}

func TestJetStreamMultipleAccountsBasics(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts: {
			A: {
				jetstream: enabled
				users: [ {user: ua, password: pwd} ]
			},
			B: {
				jetstream: {max_mem: 1GB, max_store: 1TB, max_streams: 10, max_consumers: 1k}
				users: [ {user: ub, password: pwd} ]
			},
			C: {
				users: [ {user: uc, password: pwd} ]
			},
		}
	`))

	s, opts := RunServerWithConfig(conf)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}

	nca := clientConnectToServerWithUP(t, opts, "ua", "pwd")
	defer nca.Close()

	ncb := clientConnectToServerWithUP(t, opts, "ub", "pwd")
	defer ncb.Close()

	resp, err := ncb.Request(JSApiAccountInfo, nil, time.Second)
	require_NoError(t, err)
	var info JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	limits := info.Limits
	if limits.MaxStreams != 10 {
		t.Fatalf("Expected 10 for MaxStreams, got %d", limits.MaxStreams)
	}
	if limits.MaxConsumers != 1000 {
		t.Fatalf("Expected MaxConsumers of %d, got %d", 1000, limits.MaxConsumers)
	}
	gb := int64(1024 * 1024 * 1024)
	if limits.MaxMemory != gb {
		t.Fatalf("Expected MaxMemory to be 1GB, got %d", limits.MaxMemory)
	}
	if limits.MaxStore != 1024*gb {
		t.Fatalf("Expected MaxStore to be 1TB, got %d", limits.MaxStore)
	}

	ncc := clientConnectToServerWithUP(t, opts, "uc", "pwd")
	defer ncc.Close()

	expectNotEnabled := func(resp *nats.Msg, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("Unexpected error requesting enabled status: %v", err)
		}
		if resp == nil {
			t.Fatalf("No response, possible timeout?")
		}
		var iResp JSApiAccountInfoResponse
		if err := json.Unmarshal(resp.Data, &iResp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if iResp.Error == nil {
			t.Fatalf("Expected an error on not enabled account")
		}
	}

	// Check C is not enabled. We expect a negative response, not a timeout.
	expectNotEnabled(ncc.Request(JSApiAccountInfo, nil, 250*time.Millisecond))

	// Now do simple reload and check that we do the right thing. Testing enable and disable and also change in limits
	newConf := []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts: {
			A: {
				jetstream: disabled
				users: [ {user: ua, password: pwd} ]
			},
			B: {
				jetstream: {max_mem: 32GB, max_store: 512GB, max_streams: 100, max_consumers: 4k}
				users: [ {user: ub, password: pwd} ]
			},
			C: {
				jetstream: {max_mem: 1GB, max_store: 1TB, max_streams: 10, max_consumers: 1k}
				users: [ {user: uc, password: pwd} ]
			},
		}
	`)
	if err := os.WriteFile(conf, newConf, 0600); err != nil {
		t.Fatalf("Error rewriting server's config file: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatalf("Error on server reload: %v", err)
	}
	expectNotEnabled(nca.Request(JSApiAccountInfo, nil, 250*time.Millisecond))

	resp, _ = ncb.Request(JSApiAccountInfo, nil, 250*time.Millisecond)
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Error != nil {
		t.Fatalf("Expected JetStream to be enabled, got %+v", info.Error)
	}

	resp, _ = ncc.Request(JSApiAccountInfo, nil, 250*time.Millisecond)
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Error != nil {
		t.Fatalf("Expected JetStream to be enabled, got %+v", info.Error)
	}

	// Now check that limits have been updated.
	// Account B
	resp, err = ncb.Request(JSApiAccountInfo, nil, time.Second)
	require_NoError(t, err)
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	limits = info.Limits
	if limits.MaxStreams != 100 {
		t.Fatalf("Expected 100 for MaxStreams, got %d", limits.MaxStreams)
	}
	if limits.MaxConsumers != 4000 {
		t.Fatalf("Expected MaxConsumers of %d, got %d", 4000, limits.MaxConsumers)
	}
	if limits.MaxMemory != 32*gb {
		t.Fatalf("Expected MaxMemory to be 32GB, got %d", limits.MaxMemory)
	}
	if limits.MaxStore != 512*gb {
		t.Fatalf("Expected MaxStore to be 512GB, got %d", limits.MaxStore)
	}

	// Account C
	resp, err = ncc.Request(JSApiAccountInfo, nil, time.Second)
	require_NoError(t, err)
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	limits = info.Limits
	if limits.MaxStreams != 10 {
		t.Fatalf("Expected 10 for MaxStreams, got %d", limits.MaxStreams)
	}
	if limits.MaxConsumers != 1000 {
		t.Fatalf("Expected MaxConsumers of %d, got %d", 1000, limits.MaxConsumers)
	}
	if limits.MaxMemory != gb {
		t.Fatalf("Expected MaxMemory to be 1GB, got %d", limits.MaxMemory)
	}
	if limits.MaxStore != 1024*gb {
		t.Fatalf("Expected MaxStore to be 1TB, got %d", limits.MaxStore)
	}
}

func TestJetStreamServerResourcesConfig(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 2GB, max_file_store: 1TB}
	`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	gb := int64(1024 * 1024 * 1024)
	jsc := s.JetStreamConfig()
	if jsc.MaxMemory != 2*gb {
		t.Fatalf("Expected MaxMemory to be %d, got %d", 2*gb, jsc.MaxMemory)
	}
	if jsc.MaxStore != 1024*gb {
		t.Fatalf("Expected MaxStore to be %d, got %d", 1024*gb, jsc.MaxStore)
	}
}

// From 2.2.2 to 2.2.3 we fixed a bug that would not consistently place a jetstream directory
// under the store directory configured. However there were some cases where the directory was
// created that way and therefore 2.2.3 would start and not recognize the existing accounts,
// streams and consumers.
func TestJetStreamStoreDirectoryFix(t *testing.T) {
	sd := filepath.Join(os.TempDir(), "sd_test")
	defer removeDir(t, sd)

	conf := createConfFile(t, []byte(fmt.Sprintf("listen: 127.0.0.1:-1\njetstream: {store_dir: %q}\n", sd)))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.Publish("TEST", []byte("TSS")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	// Push based.
	sub, err := js.SubscribeSync("TEST", nats.Durable("dlc"))
	require_NoError(t, err)
	defer sub.Unsubscribe()

	// Now shutdown the server.
	nc.Close()
	s.Shutdown()

	// Now move stuff up from the jetstream directory etc.
	jssd := filepath.Join(sd, JetStreamStoreDir)
	fis, _ := os.ReadDir(jssd)
	// This will be accounts, move them up one directory.
	for _, fi := range fis {
		os.Rename(filepath.Join(jssd, fi.Name()), filepath.Join(sd, fi.Name()))
	}
	removeDir(t, jssd)

	// Restart our server. Make sure our assets got moved.
	s, _ = RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	var names []string
	for name := range js.StreamNames() {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(names))
	}
	names = names[:0]
	for name := range js.ConsumerNames("TEST") {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 consumer but got %d", len(names))
	}
}

func TestJetStreamPushConsumersPullError(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.Publish("TEST", []byte("TSS")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	// Push based.
	sub, err := js.SubscribeSync("TEST")
	require_NoError(t, err)
	defer sub.Unsubscribe()
	ci, err := sub.ConsumerInfo()
	require_NoError(t, err)

	// Now do a pull. Make sure we get an error.
	m, err := nc.Request(fmt.Sprintf(JSApiRequestNextT, "TEST", ci.Name), nil, time.Second)
	require_NoError(t, err)
	if m.Header.Get("Status") != "409" {
		t.Fatalf("Expected a 409 status code, got %q", m.Header.Get("Status"))
	}
}

func TestJetStreamPullConsumerMaxWaitingOfOne(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"TEST.A"}})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:    "dur",
		MaxWaiting: 1,
		AckPolicy:  nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// First check that a request can timeout (we had an issue where this was
	// not the case for MaxWaiting of 1).
	req := JSApiConsumerGetNextRequest{Batch: 1, Expires: 250 * time.Millisecond}
	reqb, _ := json.Marshal(req)
	msg, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.dur", reqb, 13000*time.Millisecond)
	require_NoError(t, err)
	if v := msg.Header.Get("Status"); v != "408" {
		t.Fatalf("Expected 408, got: %s", v)
	}

	// Now have a request waiting...
	req = JSApiConsumerGetNextRequest{Batch: 1}
	reqb, _ = json.Marshal(req)
	// Send the request, but do not block since we want then to send an extra
	// request that should be rejected.
	sub := natsSubSync(t, nc, nats.NewInbox())
	err = nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.dur", sub.Subject, reqb)
	require_NoError(t, err)

	// Send a new request, this should be rejected as a 409.
	req = JSApiConsumerGetNextRequest{Batch: 1, Expires: 250 * time.Millisecond}
	reqb, _ = json.Marshal(req)
	msg, err = nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.dur", reqb, 300*time.Millisecond)
	require_NoError(t, err)
	if v := msg.Header.Get("Status"); v != "409" {
		t.Fatalf("Expected 409, got: %s", v)
	}
	if v := msg.Header.Get("Description"); v != "Exceeded MaxWaiting" {
		t.Fatalf("Expected error about exceeded max waiting, got: %s", v)
	}
}

func TestJetStreamPullConsumerMaxWaiting(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"test.*"}})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:    "dur",
		AckPolicy:  nats.AckExplicitPolicy,
		MaxWaiting: 10,
	})
	require_NoError(t, err)

	// Cannot be updated.
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:    "dur",
		AckPolicy:  nats.AckExplicitPolicy,
		MaxWaiting: 1,
	})
	if !strings.Contains(err.Error(), "can not be updated") {
		t.Fatalf(`expected "cannot be updated" error, got %s`, err)
	}
}

////////////////////////////////////////
// Benchmark placeholders
// TODO(dlc) - move
////////////////////////////////////////

func TestJetStreamPubPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	msetConfig := StreamConfig{
		Name:     "sr22",
		Storage:  FileStorage,
		Subjects: []string{"foo"},
	}

	if _, err := acc.addStream(&msetConfig); err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 5_000_000
	numProducers := 5

	payload := []byte("Hello World")

	startCh := make(chan bool)
	var wg sync.WaitGroup

	for n := 0; n < numProducers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh
			for i := 0; i < int(toSend)/numProducers; i++ {
				nc.Publish("foo", payload)
			}
			nc.Flush()
		}()
	}

	// Wait for Go routines.
	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	close(startCh)
	wg.Wait()

	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toSend)/tt.Seconds())

	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	start = time.Now()
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()
	fmt.Printf("Took %v to restart!\n", time.Since(start))
}

func TestJetStreamPubWithAsyncResponsePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	msetConfig := StreamConfig{
		Name:     "sr33",
		Storage:  FileStorage,
		Subjects: []string{"foo"},
	}

	if _, err := acc.addStream(&msetConfig); err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 1_000_000
	payload := []byte("Hello World")

	start := time.Now()
	for i := 0; i < toSend; i++ {
		nc.PublishRequest("foo", "bar", payload)
	}
	nc.Flush()

	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toSend)/tt.Seconds())
}

func TestJetStreamPubWithSyncPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
	require_NoError(t, err)

	toSend := 1_000_000
	payload := []byte("Hello World")

	start := time.Now()
	for i := 0; i < toSend; i++ {
		js.Publish("foo", payload)
	}

	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toSend)/tt.Seconds())
}

func TestJetStreamConsumerPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	msetConfig := StreamConfig{
		Name:     "sr22",
		Storage:  MemoryStorage,
		Subjects: []string{"foo"},
	}

	mset, err := acc.addStream(&msetConfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	payload := []byte("Hello World")

	toStore := 2000000
	for i := 0; i < toStore; i++ {
		nc.Publish("foo", payload)
	}
	nc.Flush()

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "d",
		DeliverSubject: "d",
		AckPolicy:      AckNone,
	})
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}

	var received int
	done := make(chan bool)

	nc.Subscribe("d", func(m *nats.Msg) {
		received++
		if received >= toStore {
			done <- true
		}
	})
	start := time.Now()
	nc.Flush()

	<-done
	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
}

func TestJetStreamConsumerAckFileStorePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	msetConfig := StreamConfig{
		Name:     "sr22",
		Storage:  FileStorage,
		Subjects: []string{"foo"},
	}

	mset, err := acc.addStream(&msetConfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	payload := []byte("Hello World")

	toStore := uint64(200000)
	for i := uint64(0); i < toStore; i++ {
		nc.Publish("foo", payload)
	}
	nc.Flush()

	if msgs := mset.state().Msgs; msgs != uint64(toStore) {
		t.Fatalf("Expected %d messages, got %d", toStore, msgs)
	}

	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:        "d",
		DeliverSubject: "d",
		AckPolicy:      AckExplicit,
		AckWait:        10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}
	defer o.stop()

	var received uint64
	done := make(chan bool)

	sub, _ := nc.Subscribe("d", func(m *nats.Msg) {
		m.Respond(nil) // Ack
		received++
		if received >= toStore {
			done <- true
		}
	})
	sub.SetPendingLimits(-1, -1)

	start := time.Now()
	nc.Flush()

	<-done
	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
}

func TestJetStreamPubSubPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	msetConfig := StreamConfig{
		Name:     "MSET22",
		Storage:  FileStorage,
		Subjects: []string{"foo"},
	}

	mset, err := acc.addStream(&msetConfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	var toSend = 1_000_000
	var received int
	done := make(chan bool)

	delivery := "d"

	nc.Subscribe(delivery, func(m *nats.Msg) {
		received++
		if received >= toSend {
			done <- true
		}
	})
	nc.Flush()

	_, err = mset.addConsumer(&ConsumerConfig{
		DeliverSubject: delivery,
		AckPolicy:      AckNone,
	})
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}

	payload := []byte("Hello World")

	start := time.Now()

	for i := 0; i < toSend; i++ {
		nc.Publish("foo", payload)
	}

	<-done
	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toSend)/tt.Seconds())
}

func TestJetStreamAckExplicitMsgRemoval(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{
			Name:      "MY_STREAM",
			Storage:   MemoryStorage,
			Subjects:  []string{"foo.*"},
			Retention: InterestPolicy,
		}},
		{"FileStore", &StreamConfig{
			Name:      "MY_STREAM",
			Storage:   FileStorage,
			Subjects:  []string{"foo.*"},
			Retention: InterestPolicy,
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc1 := clientConnectToServer(t, s)
			defer nc1.Close()

			nc2 := clientConnectToServer(t, s)
			defer nc2.Close()

			// Create two durable consumers on the same subject
			sub1, _ := nc1.SubscribeSync(nats.NewInbox())
			defer sub1.Unsubscribe()
			nc1.Flush()

			o1, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "dur1",
				DeliverSubject: sub1.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      AckExplicit,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o1.delete()

			sub2, _ := nc2.SubscribeSync(nats.NewInbox())
			defer sub2.Unsubscribe()
			nc2.Flush()

			o2, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "dur2",
				DeliverSubject: sub2.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      AckExplicit,
				AckWait:        100 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o2.delete()

			// Send 2 messages
			toSend := 2
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc1, "foo.bar", fmt.Sprintf("msg%v", i+1))
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %v messages, got %d", toSend, state.Msgs)
			}

			// Receive the messages and ack them.
			subs := []*nats.Subscription{sub1, sub2}
			for _, sub := range subs {
				for i := 0; i < toSend; i++ {
					m, err := sub.NextMsg(time.Second)
					if err != nil {
						t.Fatalf("Error acking message: %v", err)
					}
					m.Respond(nil)
				}
			}
			// To make sure acks are processed for checking state after sending new ones.
			checkFor(t, time.Second, 25*time.Millisecond, func() error {
				if state = mset.state(); state.Msgs != 0 {
					return fmt.Errorf("Stream still has messages")
				}
				return nil
			})

			// Now close the 2nd subscription...
			sub2.Unsubscribe()
			nc2.Flush()

			// Send 2 more new messages
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc1, "foo.bar", fmt.Sprintf("msg%v", 2+i+1))
			}
			state = mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %v messages, got %d", toSend, state.Msgs)
			}

			// first subscription should get it and will ack it.
			for i := 0; i < toSend; i++ {
				m, err := sub1.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error getting message to ack: %v", err)
				}
				m.Respond(nil)
			}
			// For acks from m.Respond above
			nc1.Flush()

			// Now recreate the subscription for the 2nd JS consumer
			sub2, _ = nc2.SubscribeSync(nats.NewInbox())
			defer sub2.Unsubscribe()

			o2, err = mset.addConsumer(&ConsumerConfig{
				Durable:        "dur2",
				DeliverSubject: sub2.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      AckExplicit,
				AckWait:        100 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o2.delete()

			// Those messages should be redelivered to the 2nd consumer
			for i := 1; i <= toSend; i++ {
				m, err := sub2.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error receiving message %d: %v", i, err)
				}
				m.Respond(nil)

				sseq := o2.streamSeqFromReply(m.Reply)
				// Depending on timing from above we could receive stream sequences out of order but
				// we know we want 3 & 4.
				if sseq != 3 && sseq != 4 {
					t.Fatalf("Expected stream sequence of 3 or 4 but got %d", sseq)
				}
			}
		})
	}
}

// This test is in support fo clients that want to match on subject, they
// can set the filter subject always. We always store the subject so that
// should the stream later be edited to expand into more subjects the consumer
// still gets what was actually requested
func TestJetStreamConsumerFilterSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	sc := &StreamConfig{Name: "MY_STREAM", Subjects: []string{"foo"}}
	mset, err := s.GlobalAccount().addStream(sc)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	cfg := &ConsumerConfig{
		Durable:        "d",
		DeliverSubject: "A",
		AckPolicy:      AckExplicit,
		FilterSubject:  "foo",
	}

	o, err := mset.addConsumer(cfg)
	if err != nil {
		t.Fatalf("Unexpected error adding consumer: %v", err)
	}
	defer o.delete()

	if o.info().Config.FilterSubject != "foo" {
		t.Fatalf("Expected the filter to be stored")
	}

	// Now use the original cfg with updated delivery subject and make sure that works ok.
	cfg = &ConsumerConfig{
		Durable:        "d",
		DeliverSubject: "B",
		AckPolicy:      AckExplicit,
		FilterSubject:  "foo",
	}

	o, err = mset.addConsumer(cfg)
	if err != nil {
		t.Fatalf("Unexpected error adding consumer: %v", err)
	}
	defer o.delete()
}

func TestJetStreamStoredMsgsDontDisappearAfterCacheExpiration(t *testing.T) {
	sc := &StreamConfig{
		Name:      "MY_STREAM",
		Storage:   FileStorage,
		Subjects:  []string{"foo.>"},
		Retention: InterestPolicy,
	}

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStreamWithStore(sc, &FileStoreConfig{BlockSize: 128, CacheExpire: 15 * time.Millisecond})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc1 := clientConnectWithOldRequest(t, s)
	defer nc1.Close()

	// Create a durable consumers
	sub, _ := nc1.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()
	nc1.Flush()

	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:        "dur",
		DeliverSubject: sub.Subject,
		FilterSubject:  "foo.bar",
		DeliverPolicy:  DeliverNew,
		AckPolicy:      AckExplicit,
	})
	if err != nil {
		t.Fatalf("Unexpected error adding consumer: %v", err)
	}
	defer o.delete()

	nc2 := clientConnectWithOldRequest(t, s)
	defer nc2.Close()

	sendStreamMsg(t, nc2, "foo.bar", "msg1")

	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Did not get message: %v", err)
	}
	if string(msg.Data) != "msg1" {
		t.Fatalf("Unexpected message: %q", msg.Data)
	}

	nc1.Close()

	// Get the message from the stream
	getMsgSeq := func(seq uint64) {
		t.Helper()
		mreq := &JSApiMsgGetRequest{Seq: seq}
		req, err := json.Marshal(mreq)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		smsgj, err := nc2.Request(fmt.Sprintf(JSApiMsgGetT, sc.Name), req, time.Second)
		if err != nil {
			t.Fatalf("Could not retrieve stream message: %v", err)
		}
		if strings.Contains(string(smsgj.Data), "code") {
			t.Fatalf("Error: %q", smsgj.Data)
		}
	}

	getMsgSeq(1)

	time.Sleep(time.Second)

	sendStreamMsg(t, nc2, "foo.bar", "msg2")
	sendStreamMsg(t, nc2, "foo.bar", "msg3")

	getMsgSeq(1)
	getMsgSeq(2)
	getMsgSeq(3)
}

func TestJetStreamConsumerUpdateRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{
			Name:      "MY_STREAM",
			Storage:   MemoryStorage,
			Subjects:  []string{"foo.>"},
			Retention: InterestPolicy,
		}},
		{"FileStore", &StreamConfig{
			Name:      "MY_STREAM",
			Storage:   FileStorage,
			Subjects:  []string{"foo.>"},
			Retention: InterestPolicy,
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Create a durable consumer.
			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "dur22",
				DeliverSubject: sub.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      AckExplicit,
				AckWait:        100 * time.Millisecond,
				MaxDeliver:     3,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o.delete()

			// Send 20 messages
			toSend := 20
			for i := 1; i <= toSend; i++ {
				sendStreamMsg(t, nc, "foo.bar", fmt.Sprintf("msg-%v", i))
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %v messages, got %d", toSend, state.Msgs)
			}

			// Receive the messages and ack only every 4th
			for i := 0; i < toSend; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error getting message: %v", err)
				}
				seq, _, _, _, _ := replyInfo(m.Reply)
				// 4, 8, 12, 16, 20
				if seq%4 == 0 {
					m.Respond(nil)
				}
			}

			// Now close the sub and open a new one and update the consumer.
			sub.Unsubscribe()

			// Wait for it to become inactive
			checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
				if o.isActive() {
					return fmt.Errorf("Consumer still active")
				}
				return nil
			})

			// Send 20 more messages.
			for i := toSend; i < toSend*2; i++ {
				sendStreamMsg(t, nc, "foo.bar", fmt.Sprintf("msg-%v", i))
			}

			// Create new subscription.
			sub, _ = nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err = mset.addConsumer(&ConsumerConfig{
				Durable:        "dur22",
				DeliverSubject: sub.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      AckExplicit,
				AckWait:        100 * time.Millisecond,
				MaxDeliver:     3,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o.delete()

			expect := toSend + toSend - 5 // mod 4 acks
			checkFor(t, time.Second, 5*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != expect {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, expect)
				}
				return nil
			})

			for i, eseq := 0, uint64(1); i < expect; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error getting message: %v", err)
				}
				// Skip the ones we ack'd from above. We should not get them back here.
				if eseq <= uint64(toSend) && eseq%4 == 0 {
					eseq++
				}
				seq, _, dc, _, _ := replyInfo(m.Reply)
				if seq != eseq {
					t.Fatalf("Expected stream sequence of %d, got %d", eseq, seq)
				}
				if seq <= uint64(toSend) && dc != 2 {
					t.Fatalf("Expected delivery count of 2 for sequence of %d, got %d", seq, dc)
				}
				if seq > uint64(toSend) && dc != 1 {
					t.Fatalf("Expected delivery count of 1 for sequence of %d, got %d", seq, dc)
				}
				if seq > uint64(toSend) {
					m.Respond(nil) // Ack
				}
				eseq++
			}

			// We should get the second half back since we did not ack those from above.
			expect = toSend - 5
			checkFor(t, time.Second, 5*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != expect {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, expect)
				}
				return nil
			})

			for i, eseq := 0, uint64(1); i < expect; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error getting message: %v", err)
				}
				// Skip the ones we ack'd from above. We should not get them back here.
				if eseq <= uint64(toSend) && eseq%4 == 0 {
					eseq++
				}
				seq, _, dc, _, _ := replyInfo(m.Reply)
				if seq != eseq {
					t.Fatalf("Expected stream sequence of %d, got %d", eseq, seq)
				}
				if dc != 3 {
					t.Fatalf("Expected delivery count of 3 for sequence of %d, got %d", seq, dc)
				}
				eseq++
			}
		})
	}
}

func TestJetStreamConsumerMaxAckPending(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  MemoryStorage,
			Subjects: []string{"foo.*"},
		}},
		{"FileStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  FileStorage,
			Subjects: []string{"foo.*"},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Do error scenarios.
			_, err = mset.addConsumer(&ConsumerConfig{
				Durable:        "d22",
				DeliverSubject: nats.NewInbox(),
				AckPolicy:      AckNone,
				MaxAckPending:  1,
			})
			if err == nil {
				t.Fatalf("Expected error, MaxAckPending only applicable to ack != AckNone")
			}

			// Queue up 100 messages.
			toSend := 100
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.bar", fmt.Sprintf("MSG: %d", i+1))
			}

			// Limit to 33
			maxAckPending := 33

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "d22",
				DeliverSubject: nats.NewInbox(),
				AckPolicy:      AckExplicit,
				MaxAckPending:  maxAckPending,
			})
			require_NoError(t, err)

			defer o.delete()

			sub, _ := nc.SubscribeSync(o.info().Config.DeliverSubject)
			defer sub.Unsubscribe()

			checkSubPending := func(numExpected int) {
				t.Helper()
				checkFor(t, time.Second, 20*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != numExpected {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
					}
					return nil
				})
			}

			checkSubPending(maxAckPending)
			// We hit the limit, double check we stayed there.
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxAckPending {
				t.Fatalf("Too many messages received: %d vs %d", nmsgs, maxAckPending)
			}

			// Now ack them all.
			for i := 0; i < maxAckPending; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error receiving message %d: %v", i, err)
				}
				m.Respond(nil)
			}
			checkSubPending(maxAckPending)

			o.stop()
			mset.purge(nil)

			// Now test a consumer that is live while we publish messages to the stream.
			o, err = mset.addConsumer(&ConsumerConfig{
				Durable:        "d33",
				DeliverSubject: nats.NewInbox(),
				AckPolicy:      AckExplicit,
				MaxAckPending:  maxAckPending,
			})
			require_NoError(t, err)

			defer o.delete()

			sub, _ = nc.SubscribeSync(o.info().Config.DeliverSubject)
			defer sub.Unsubscribe()
			nc.Flush()

			checkSubPending(0)

			// Now stream more then maxAckPending.
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.baz", fmt.Sprintf("MSG: %d", i+1))
			}
			checkSubPending(maxAckPending)
			// We hit the limit, double check we stayed there.
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxAckPending {
				t.Fatalf("Too many messages received: %d vs %d", nmsgs, maxAckPending)
			}
		})
	}
}

func TestJetStreamPullConsumerMaxAckPending(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  MemoryStorage,
			Subjects: []string{"foo.*"},
		}},
		{"FileStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  FileStorage,
			Subjects: []string{"foo.*"},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Queue up 100 messages.
			toSend := 100
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.bar", fmt.Sprintf("MSG: %d", i+1))
			}

			// Limit to 33
			maxAckPending := 33

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:       "d22",
				AckPolicy:     AckExplicit,
				MaxAckPending: maxAckPending,
			})
			require_NoError(t, err)

			defer o.delete()

			getSubj := o.requestNextMsgSubject()

			var toAck []*nats.Msg

			for i := 0; i < maxAckPending; i++ {
				if m, err := nc.Request(getSubj, nil, time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				} else {
					toAck = append(toAck, m)
				}
			}

			// Now ack them all.
			for _, m := range toAck {
				m.Respond(nil)
			}

			// Now do batch above the max.
			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			checkSubPending := func(numExpected int) {
				t.Helper()
				checkFor(t, time.Second, 10*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != numExpected {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
					}
					return nil
				})
			}

			req := &JSApiConsumerGetNextRequest{Batch: maxAckPending}
			jreq, _ := json.Marshal(req)
			nc.PublishRequest(getSubj, sub.Subject, jreq)

			checkSubPending(maxAckPending)
			// We hit the limit, double check we stayed there.
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxAckPending {
				t.Fatalf("Too many messages received: %d vs %d", nmsgs, maxAckPending)
			}
		})
	}
}

func TestJetStreamPullConsumerMaxAckPendingRedeliveries(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  MemoryStorage,
			Subjects: []string{"foo.*"},
		}},
		{"FileStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  FileStorage,
			Subjects: []string{"foo.*"},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Queue up 10 messages.
			toSend := 10
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.bar", fmt.Sprintf("MSG: %d", i+1))
			}

			// Limit to 1
			maxAckPending := 1
			ackWait := 20 * time.Millisecond
			expSeq := uint64(4)

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:       "d22",
				DeliverPolicy: DeliverByStartSequence,
				OptStartSeq:   expSeq,
				AckPolicy:     AckExplicit,
				AckWait:       ackWait,
				MaxAckPending: maxAckPending,
			})
			require_NoError(t, err)

			defer o.delete()

			getSubj := o.requestNextMsgSubject()
			delivery := uint64(1)

			getNext := func() {
				t.Helper()
				m, err := nc.Request(getSubj, nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				sseq, dseq, dcount, _, pending := replyInfo(m.Reply)
				if sseq != expSeq {
					t.Fatalf("Expected stream sequence of %d, got %d", expSeq, sseq)
				}
				if dseq != delivery {
					t.Fatalf("Expected consumer sequence of %d, got %d", delivery, dseq)
				}
				if dcount != delivery {
					t.Fatalf("Expected delivery count of %d, got %d", delivery, dcount)
				}
				if pending != uint64(toSend)-expSeq {
					t.Fatalf("Expected pending to be %d, got %d", uint64(toSend)-expSeq, pending)
				}
				delivery++
			}

			getNext()
			getNext()
			getNext()
			getNext()
			getNext()
		})
	}
}

func TestJetStreamDeliveryAfterServerRestart(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	s := RunServer(&opts)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{
		Name:      "MY_STREAM",
		Storage:   FileStorage,
		Subjects:  []string{"foo.>"},
		Retention: InterestPolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	inbox := nats.NewInbox()
	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:        "dur",
		DeliverSubject: inbox,
		DeliverPolicy:  DeliverNew,
		AckPolicy:      AckExplicit,
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o.delete()

	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	nc.Flush()

	// Send 1 message
	sendStreamMsg(t, nc, "foo.bar", "msg1")

	// Make sure we receive it and ack it.
	msg, err := sub.NextMsg(250 * time.Millisecond)
	if err != nil {
		t.Fatalf("Did not get message: %v", err)
	}
	// Ack it!
	msg.Respond(nil)
	nc.Flush()

	// Shutdown client and server
	nc.Close()

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()

	opts.Port = -1
	opts.StoreDir = dir
	s = RunServer(&opts)
	defer s.Shutdown()

	// Lookup stream.
	mset, err = s.GlobalAccount().lookupStream("MY_STREAM")
	if err != nil {
		t.Fatalf("Error looking up stream: %v", err)
	}

	// Update consumer's deliver subject with new inbox
	inbox = nats.NewInbox()
	o, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "dur",
		DeliverSubject: inbox,
		DeliverPolicy:  DeliverNew,
		AckPolicy:      AckExplicit,
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o.delete()

	nc = clientConnectToServer(t, s)
	defer nc.Close()

	// Send 2nd message
	sendStreamMsg(t, nc, "foo.bar", "msg2")

	// Start sub on new inbox
	sub, err = nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	nc.Flush()

	// Should receive message 2.
	if _, err := sub.NextMsg(500 * time.Millisecond); err != nil {
		t.Fatalf("Did not get message: %v", err)
	}
}

// This is for the basics of importing the ability to send to a stream and consume
// from a consumer that is pull based on push based on a well known delivery subject.
func TestJetStreamAccountImportBasics(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [
					# This is for sending into a stream from other accounts.
					{ service: "ORDERS.*" }
					# This is for accessing a pull based consumer.
					{ service: "$JS.API.CONSUMER.MSG.NEXT.*.*" }
					# This is streaming to a delivery subject for a push based consumer.
					{ stream: "deliver.ORDERS" }
					# This is to ack received messages. This is a service to ack acks..
					{ service: "$JS.ACK.ORDERS.*.>" }
				]
			},
			IU: {
				users: [ {user: rip, password: bar} ]
				imports [
					{ service: { subject: "ORDERS.*", account: JS }, to: "my.orders.$1" }
					{ service: { subject: "$JS.API.CONSUMER.MSG.NEXT.ORDERS.d", account: JS }, to: "nxt.msg" }
					{ stream:  { subject: "deliver.ORDERS", account: JS }, to: "d" }
					{ service: { subject: "$JS.ACK.ORDERS.*.>", account: JS } }
				]
			},
		}
	`))

	s, _ := RunServerWithConfig(conf)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	acc, err := s.LookupAccount("JS")
	if err != nil {
		t.Fatalf("Unexpected error looking up account: %v", err)
	}

	mset, err := acc.addStream(&StreamConfig{Name: "ORDERS", Subjects: []string{"ORDERS.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	// This should be the rip user, the one that imports some JS.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Simple publish to a stream.
	pubAck := sendStreamMsg(t, nc, "my.orders.foo", "ORDERS-1")
	if pubAck.Stream != "ORDERS" || pubAck.Sequence != 1 {
		t.Fatalf("Bad pubAck received: %+v", pubAck)
	}
	if msgs := mset.state().Msgs; msgs != 1 {
		t.Fatalf("Expected 1 message, got %d", msgs)
	}

	total := 2
	for i := 2; i <= total; i++ {
		sendStreamMsg(t, nc, "my.orders.bar", fmt.Sprintf("ORDERS-%d", i))
	}
	if msgs := mset.state().Msgs; msgs != uint64(total) {
		t.Fatalf("Expected %d messages, got %d", total, msgs)
	}

	// Now test access to a pull based consumer, e.g. workqueue.
	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:   "d",
		AckPolicy: AckExplicit,
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o.delete()

	// We mapped the next message request, "$JS.API.CONSUMER.MSG.NEXT.ORDERS.d" -> "nxt.msg"
	m, err := nc.Request("nxt.msg", nil, time.Second)
	require_NoError(t, err)
	if string(m.Data) != "ORDERS-1" {
		t.Fatalf("Expected to receive %q, got %q", "ORDERS-1", m.Data)
	}

	// Now test access to a push based consumer
	o, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "p",
		DeliverSubject: "deliver.ORDERS",
		AckPolicy:      AckExplicit,
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o.delete()

	// We remapped from above, deliver.ORDERS -> d
	sub, _ := nc.SubscribeSync("d")
	defer sub.Unsubscribe()

	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != total {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, total)
		}
		return nil
	})

	m, _ = sub.NextMsg(time.Second)
	// Make sure we remapped subject correctly across the account boundary.
	if m.Subject != "ORDERS.foo" {
		t.Fatalf("Expected subject of %q, got %q", "ORDERS.foo", m.Subject)
	}
	// Now make sure we can ack messages correctly.
	m.Respond(AckAck)
	nc.Flush()

	if info := o.info(); info.AckFloor.Consumer != 1 {
		t.Fatalf("Did not receive the ack properly")
	}

	// Grab second one now.
	m, _ = sub.NextMsg(time.Second)
	// Make sure we remapped subject correctly across the account boundary.
	if m.Subject != "ORDERS.bar" {
		t.Fatalf("Expected subject of %q, got %q", "ORDERS.bar", m.Subject)
	}
	// Now make sure we can ack messages and get back an ack as well.
	resp, _ := nc.Request(m.Reply, nil, 100*time.Millisecond)
	if resp == nil {
		t.Fatalf("No response, possible timeout?")
	}
	if info := o.info(); info.AckFloor.Consumer != 2 {
		t.Fatalf("Did not receive the ack properly")
	}
}

// This tests whether we are able to aggregate all JetStream advisory events
// from all accounts into a single account. Config for this test uses
// service imports and exports as that allows for gathering all events
// without having to know the account name and without separate entries
// for each account in aggregate account config.
// This test fails as it is not receiving the api audit event ($JS.EVENT.ADVISORY.API).
func TestJetStreamAccountImportJSAdvisoriesAsService(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen=127.0.0.1:-1
		no_auth_user: pp
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts {
			JS {
				jetstream: enabled
				users: [ {user: pp, password: foo} ]
				imports [
					{ service: { account: AGG, subject: '$JS.EVENT.ADVISORY.ACC.JS.>' }, to: '$JS.EVENT.ADVISORY.>' }
				]
			}
			AGG {
				users: [ {user: agg, password: foo} ]
				exports: [
					{ service: '$JS.EVENT.ADVISORY.ACC.*.>', response: Singleton, account_token_position: 5 }
				]
			}
		}
	`))

	s, _ := RunServerWithConfig(conf)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	// This should be the pp user, one which manages JetStream assets
	ncJS, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error during connect: %v", err)
	}
	defer ncJS.Close()

	// This is the agg user, which should aggregate all JS advisory events.
	ncAgg, err := nats.Connect(s.ClientURL(), nats.UserInfo("agg", "foo"))
	if err != nil {
		t.Fatalf("Unexpected error during connect: %v", err)
	}
	defer ncAgg.Close()

	js, err := ncJS.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// user from JS account should receive events on $JS.EVENT.ADVISORY.> subject
	subJS, err := ncJS.SubscribeSync("$JS.EVENT.ADVISORY.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer subJS.Unsubscribe()

	// user from AGG account should receive events on mapped $JS.EVENT.ADVISORY.ACC.JS.> subject (with account name)
	subAgg, err := ncAgg.SubscribeSync("$JS.EVENT.ADVISORY.ACC.JS.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// add stream using JS account
	// this should trigger 2 events:
	// - an action event on $JS.EVENT.ADVISORY.STREAM.CREATED.ORDERS
	// - an api audit event on $JS.EVENT.ADVISORY.API
	_, err = js.AddStream(&nats.StreamConfig{Name: "ORDERS", Subjects: []string{"ORDERS.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	gotEvents := map[string]int{}
	for i := 0; i < 2; i++ {
		msg, err := subJS.NextMsg(time.Second * 2)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		gotEvents[msg.Subject]++
	}
	if c := gotEvents["$JS.EVENT.ADVISORY.STREAM.CREATED.ORDERS"]; c != 1 {
		t.Fatalf("Should have received one advisory from $JS.EVENT.ADVISORY.STREAM.CREATED.ORDERS but got %d", c)
	}
	if c := gotEvents["$JS.EVENT.ADVISORY.API"]; c != 1 {
		t.Fatalf("Should have received one advisory from $JS.EVENT.ADVISORY.API but got %d", c)
	}

	// same set of events should be received by AGG account
	// on subjects containing account name (ACC.JS)
	gotEvents = map[string]int{}
	for i := 0; i < 2; i++ {
		msg, err := subAgg.NextMsg(time.Second * 2)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		gotEvents[msg.Subject]++
	}
	if c := gotEvents["$JS.EVENT.ADVISORY.ACC.JS.STREAM.CREATED.ORDERS"]; c != 1 {
		t.Fatalf("Should have received one advisory from $JS.EVENT.ADVISORY.ACC.JS.STREAM.CREATED.ORDERS but got %d", c)
	}
	if c := gotEvents["$JS.EVENT.ADVISORY.ACC.JS.API"]; c != 1 {
		t.Fatalf("Should have received one advisory from $JS.EVENT.ADVISORY.ACC.JS.API but got %d", c)
	}
}

// This tests whether we are able to aggregate all JetStream advisory events
// from all accounts into a single account. Config for this test uses
// stream imports and exports as that allows for gathering all events
// as long as there is a separate stream import entry for each account
// in aggregate account config.
func TestJetStreamAccountImportJSAdvisoriesAsStream(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen=127.0.0.1:-1
		no_auth_user: pp
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts {
			JS {
				jetstream: enabled
				users: [ {user: pp, password: foo} ]
				exports [
					{ stream: '$JS.EVENT.ADVISORY.>' }
				]
			}
			AGG {
				users: [ {user: agg, password: foo} ]
				imports: [
					{ stream: { account: JS, subject: '$JS.EVENT.ADVISORY.>' }, to: '$JS.EVENT.ADVISORY.ACC.JS.>' }
				]
			}
		}
	`))

	s, _ := RunServerWithConfig(conf)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	// This should be the pp user, one which manages JetStream assets
	ncJS, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error during connect: %v", err)
	}
	defer ncJS.Close()

	// This is the agg user, which should aggregate all JS advisory events.
	ncAgg, err := nats.Connect(s.ClientURL(), nats.UserInfo("agg", "foo"))
	if err != nil {
		t.Fatalf("Unexpected error during connect: %v", err)
	}
	defer ncAgg.Close()

	js, err := ncJS.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// user from JS account should receive events on $JS.EVENT.ADVISORY.> subject
	subJS, err := ncJS.SubscribeSync("$JS.EVENT.ADVISORY.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer subJS.Unsubscribe()

	// user from AGG account should receive events on mapped $JS.EVENT.ADVISORY.ACC.JS.> subject (with account name)
	subAgg, err := ncAgg.SubscribeSync("$JS.EVENT.ADVISORY.ACC.JS.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// add stream using JS account
	// this should trigger 2 events:
	// - an action event on $JS.EVENT.ADVISORY.STREAM.CREATED.ORDERS
	// - an api audit event on $JS.EVENT.ADVISORY.API
	_, err = js.AddStream(&nats.StreamConfig{Name: "ORDERS", Subjects: []string{"ORDERS.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	msg, err := subJS.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if msg.Subject != "$JS.EVENT.ADVISORY.STREAM.CREATED.ORDERS" {
		t.Fatalf("Unexpected subject: %q", msg.Subject)
	}
	msg, err = subJS.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if msg.Subject != "$JS.EVENT.ADVISORY.API" {
		t.Fatalf("Unexpected subject: %q", msg.Subject)
	}

	// same set of events should be received by AGG account
	// on subjects containing account name (ACC.JS)
	msg, err = subAgg.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if msg.Subject != "$JS.EVENT.ADVISORY.ACC.JS.STREAM.CREATED.ORDERS" {
		t.Fatalf("Unexpected subject: %q", msg.Subject)
	}

	// when using stream instead of service, we get all events
	msg, err = subAgg.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if msg.Subject != "$JS.EVENT.ADVISORY.ACC.JS.API" {
		t.Fatalf("Unexpected subject: %q", msg.Subject)
	}
}

// This is for importing all of JetStream into another account for admin purposes.
func TestJetStreamAccountImportAll(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [ { service: "$JS.API.>" } ]
			},
			IU: {
				users: [ {user: rip, password: bar} ]
				imports [ { service: { subject: "$JS.API.>", account: JS }, to: "jsapi.>"} ]
			},
		}
	`))

	s, _ := RunServerWithConfig(conf)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	acc, err := s.LookupAccount("JS")
	if err != nil {
		t.Fatalf("Unexpected error looking up account: %v", err)
	}

	mset, err := acc.addStream(&StreamConfig{Name: "ORDERS", Subjects: []string{"ORDERS.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	// This should be the rip user, the one that imports all of JS.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	mapSubj := func(subject string) string {
		return strings.Replace(subject, "$JS.API.", "jsapi.", 1)
	}

	// This will get the current information about usage and limits for this account.
	resp, err := nc.Request(mapSubj(JSApiAccountInfo), nil, time.Second)
	require_NoError(t, err)
	var info JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Error != nil {
		t.Fatalf("Unexpected error: %+v", info.Error)
	}
	// Lookup streams.
	resp, err = nc.Request(mapSubj(JSApiStreams), nil, time.Second)
	require_NoError(t, err)
	var namesResponse JSApiStreamNamesResponse
	if err = json.Unmarshal(resp.Data, &namesResponse); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if namesResponse.Error != nil {
		t.Fatalf("Unexpected error: %+v", namesResponse.Error)
	}
}

// https://github.com/nats-io/nats-server/issues/1736
func TestJetStreamServerReload(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB }
		accounts: {
			A: { users: [ {user: ua, password: pwd} ] },
			B: {
				jetstream: {max_mem: 1GB, max_store: 1TB, max_streams: 10, max_consumers: 1k}
				users: [ {user: ub, password: pwd} ]
			},
			SYS: { users: [ {user: uc, password: pwd} ] },
		}
		no_auth_user: ub
		system_account: SYS
	`))

	s, _ := RunServerWithConfig(conf)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	checkJSAccount := func() {
		t.Helper()
		resp, err := nc.Request(JSApiAccountInfo, nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var info JSApiAccountInfoResponse
		if err := json.Unmarshal(resp.Data, &info); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	checkJSAccount()

	acc, err := s.LookupAccount("B")
	if err != nil {
		t.Fatalf("Unexpected error looking up account: %v", err)
	}
	mset, err := acc.addStream(&StreamConfig{Name: "22"})
	require_NoError(t, err)

	toSend := 10
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "22", fmt.Sprintf("MSG: %d", i+1))
	}
	if msgs := mset.state().Msgs; msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, msgs)
	}

	if err := s.Reload(); err != nil {
		t.Fatalf("Error on server reload: %v", err)
	}

	// Wait to get reconnected.
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if !nc.IsConnected() {
			return fmt.Errorf("Not connected")
		}
		return nil
	})

	checkJSAccount()
	sendStreamMsg(t, nc, "22", "MSG: 22")
}

func TestJetStreamConfigReloadWithGlobalAccount(t *testing.T) {
	template := `
		listen: 127.0.0.1:-1
		authorization {
			users [
				{user: anonymous}
				{user: user1, password: %s}
			]
		}
		no_auth_user: anonymous
		jetstream: enabled
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, "pwd")))

	s, _ := RunServerWithConfig(conf)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	checkJSAccount := func() {
		t.Helper()
		if _, err := js.AccountInfo(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	checkJSAccount()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "foo"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("foo", []byte(fmt.Sprintf("MSG: %d", i+1))); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	si, err := js.StreamInfo("foo")
	require_NoError(t, err)
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs after restart, got %d", toSend, si.State.Msgs)
	}

	if err := os.WriteFile(conf, []byte(fmt.Sprintf(template, "pwd2")), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}

	if err := s.Reload(); err != nil {
		t.Fatalf("Error during config reload: %v", err)
	}

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// Try to add a new stream to the global account
	if _, err := js.AddStream(&nats.StreamConfig{Name: "bar"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkJSAccount()
}

// Test that we properly enforce per subject msg limits.
func TestJetStreamMaxMsgsPerSubject(t *testing.T) {
	const maxPer = 5
	msc := StreamConfig{
		Name:       "TEST",
		Subjects:   []string{"foo", "bar", "baz.*"},
		Storage:    MemoryStorage,
		MaxMsgsPer: maxPer,
	}
	fsc := msc
	fsc.Storage = FileStorage

	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &msc},
		{"FileStore", &fsc},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// Client for API requests.
			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			pubAndCheck := func(subj string, num int, expectedNumMsgs uint64) {
				t.Helper()
				for i := 0; i < num; i++ {
					if _, err = js.Publish(subj, []byte("TSLA")); err != nil {
						t.Fatalf("Unexpected publish error: %v", err)
					}
				}
				si, err := js.StreamInfo("TEST")
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if si.State.Msgs != expectedNumMsgs {
					t.Fatalf("Expected %d msgs, got %d", expectedNumMsgs, si.State.Msgs)
				}
			}

			pubAndCheck("foo", 1, 1)
			pubAndCheck("foo", 4, 5)
			// Now make sure our per subject limits kick in..
			pubAndCheck("foo", 2, 5)
			pubAndCheck("baz.22", 5, 10)
			pubAndCheck("baz.33", 5, 15)
			// We are maxed so totals should be same no matter what we add here.
			pubAndCheck("baz.22", 5, 15)
			pubAndCheck("baz.33", 5, 15)

			// Now purge and make sure all is still good.
			mset.purge(nil)
			pubAndCheck("foo", 1, 1)
			pubAndCheck("foo", 4, 5)
			pubAndCheck("baz.22", 5, 10)
			pubAndCheck("baz.33", 5, 15)
		})
	}
}

func TestJetStreamGetLastMsgBySubject(t *testing.T) {
	for _, st := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(st.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "JSC", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			cfg := StreamConfig{
				Name:       "KV",
				Subjects:   []string{"kv.>"},
				Storage:    st,
				Replicas:   2,
				MaxMsgsPer: 20,
			}

			req, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Do manually for now.
			nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			si, err := js.StreamInfo("KV")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si == nil || si.Config.Name != "KV" {
				t.Fatalf("StreamInfo is not correct %+v", si)
			}

			for i := 0; i < 1000; i++ {
				msg := []byte(fmt.Sprintf("VAL-%d", i+1))
				js.PublishAsync("kv.foo", msg)
				js.PublishAsync("kv.bar", msg)
				js.PublishAsync("kv.baz", msg)
			}
			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}

			// Check that if both set that errors.
			b, _ := json.Marshal(JSApiMsgGetRequest{LastFor: "kv.foo", Seq: 950})
			rmsg, err := nc.Request(fmt.Sprintf(JSApiMsgGetT, "KV"), b, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var resp JSApiMsgGetResponse
			err = json.Unmarshal(rmsg.Data, &resp)
			if err != nil {
				t.Fatalf("Could not parse stream message: %v", err)
			}
			if resp.Error == nil {
				t.Fatalf("Expected an error when both are set, got %+v", resp.Error)
			}

			// Need to do stream GetMsg by hand for now until Go client support lands.
			getLast := func(subject string) *StoredMsg {
				t.Helper()
				req := &JSApiMsgGetRequest{LastFor: subject}
				b, _ := json.Marshal(req)
				rmsg, err := nc.Request(fmt.Sprintf(JSApiMsgGetT, "KV"), b, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				var resp JSApiMsgGetResponse
				err = json.Unmarshal(rmsg.Data, &resp)
				if err != nil {
					t.Fatalf("Could not parse stream message: %v", err)
				}
				if resp.Message == nil || resp.Error != nil {
					t.Fatalf("Did not receive correct response: %+v", resp.Error)
				}
				return resp.Message
			}
			// Do basic checks.
			basicCheck := func(subject string, expectedSeq uint64) {
				sm := getLast(subject)
				if sm == nil {
					t.Fatalf("Expected a message but got none")
				} else if string(sm.Data) != "VAL-1000" {
					t.Fatalf("Wrong message payload, wanted %q but got %q", "VAL-1000", sm.Data)
				} else if sm.Sequence != expectedSeq {
					t.Fatalf("Wrong message sequence, wanted %d but got %d", expectedSeq, sm.Sequence)
				} else if !subjectIsSubsetMatch(sm.Subject, subject) {
					t.Fatalf("Wrong subject, wanted %q but got %q", subject, sm.Subject)
				}
			}

			basicCheck("kv.foo", 2998)
			basicCheck("kv.bar", 2999)
			basicCheck("kv.baz", 3000)
			basicCheck("kv.*", 3000)
			basicCheck(">", 3000)
		})
	}
}

// https://github.com/nats-io/nats-server/issues/2329
func TestJetStreamGetLastMsgBySubjectAfterUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	sc := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	}
	if _, err := js.AddStream(sc); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Now Update and add in other subjects.
	sc.Subjects = append(sc.Subjects, "bar", "baz")
	if _, err := js.UpdateStream(sc); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js.Publish("foo", []byte("OK1")) // 1
	js.Publish("bar", []byte("OK1")) // 2
	js.Publish("foo", []byte("OK2")) // 3
	js.Publish("bar", []byte("OK2")) // 4

	// Need to do stream GetMsg by hand for now until Go client support lands.
	getLast := func(subject string) *StoredMsg {
		t.Helper()
		req := &JSApiMsgGetRequest{LastFor: subject}
		b, _ := json.Marshal(req)
		rmsg, err := nc.Request(fmt.Sprintf(JSApiMsgGetT, "TEST"), b, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var resp JSApiMsgGetResponse
		err = json.Unmarshal(rmsg.Data, &resp)
		if err != nil {
			t.Fatalf("Could not parse stream message: %v", err)
		}
		if resp.Message == nil || resp.Error != nil {
			t.Fatalf("Did not receive correct response: %+v", resp.Error)
		}
		return resp.Message
	}
	// Do basic checks.
	basicCheck := func(subject string, expectedSeq uint64) {
		sm := getLast(subject)
		if sm == nil {
			t.Fatalf("Expected a message but got none")
		} else if sm.Sequence != expectedSeq {
			t.Fatalf("Wrong message sequence, wanted %d but got %d", expectedSeq, sm.Sequence)
		} else if !subjectIsSubsetMatch(sm.Subject, subject) {
			t.Fatalf("Wrong subject, wanted %q but got %q", subject, sm.Subject)
		}
	}

	basicCheck("foo", 3)
	basicCheck("bar", 4)
}

func TestJetStreamLastSequenceBySubject(t *testing.T) {
	for _, st := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(st.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "JSC", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			cfg := StreamConfig{
				Name:       "KV",
				Subjects:   []string{"kv.>"},
				Storage:    st,
				Replicas:   3,
				MaxMsgsPer: 1,
			}

			req, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Do manually for now.
			m, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			require_NoError(t, err)
			si, err := js.StreamInfo("KV")
			if err != nil {
				t.Fatalf("Unexpected error: %v, respmsg: %q", err, string(m.Data))
			}
			if si == nil || si.Config.Name != "KV" {
				t.Fatalf("StreamInfo is not correct %+v", si)
			}

			js.PublishAsync("kv.foo", []byte("1"))
			js.PublishAsync("kv.bar", []byte("2"))
			js.PublishAsync("kv.baz", []byte("3"))

			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(time.Second):
				t.Fatalf("Did not receive completion signal")
			}

			// Now make sure we get an error if the last sequence is not correct per subject.
			pubAndCheck := func(subj, seq string, ok bool) {
				t.Helper()
				m := nats.NewMsg(subj)
				m.Data = []byte("HELLO")
				m.Header.Set(JSExpectedLastSubjSeq, seq)
				_, err := js.PublishMsg(m)
				if ok && err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if !ok && err == nil {
					t.Fatalf("Expected to get an error and got none")
				}
			}

			pubAndCheck("kv.foo", "1", true)  // So last is now 4.
			pubAndCheck("kv.foo", "1", false) // This should fail.
			pubAndCheck("kv.bar", "2", true)
			pubAndCheck("kv.bar", "5", true)
			pubAndCheck("kv.xxx", "5", false)
		})
	}
}

func TestJetStreamFilteredConsumersWithWiderFilter(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz", "N.*"},
	})
	require_NoError(t, err)

	// Add in some messages.
	js.Publish("foo", []byte("OK"))
	js.Publish("bar", []byte("OK"))
	js.Publish("baz", []byte("OK"))
	for i := 0; i < 12; i++ {
		js.Publish(fmt.Sprintf("N.%d", i+1), []byte("OK"))
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)

		if si.State.Msgs != 15 {
			return fmt.Errorf("Expected 15 msgs, got state: %+v", si.State)
		}
		return nil
	})

	checkWider := func(subj string, numExpected int) {
		sub, err := js.SubscribeSync(subj)
		require_NoError(t, err)

		defer sub.Unsubscribe()
		checkSubsPending(t, sub, numExpected)
	}

	checkWider("*", 3)
	checkWider("N.*", 12)
	checkWider("*.*", 12)
	checkWider("N.>", 12)
	checkWider(">", 15)
}

func TestJetStreamMirrorAndSourcesFilteredConsumers(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz.*"},
	})
	require_NoError(t, err)

	// Create Mirror now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:   "M",
		Mirror: &nats.StreamSource{Name: "TEST"},
	})
	require_NoError(t, err)

	dsubj := nats.NewInbox()
	nc.SubscribeSync(dsubj)
	nc.Flush()

	createConsumer := func(sn, fs string) {
		t.Helper()
		_, err = js.AddConsumer(sn, &nats.ConsumerConfig{DeliverSubject: dsubj, FilterSubject: fs})
		require_NoError(t, err)

	}

	createConsumer("M", "foo")
	createConsumer("M", "bar")
	createConsumer("M", "baz.foo")

	// Now do some sources.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "O1", Subjects: []string{"foo.*"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "O2", Subjects: []string{"bar.*"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create Mirror now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:    "S",
		Sources: []*nats.StreamSource{{Name: "O1"}, {Name: "O2"}},
	})
	require_NoError(t, err)

	createConsumer("S", "foo.1")
	createConsumer("S", "bar.1")

	// Chaining
	// Create Mirror now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:   "M2",
		Mirror: &nats.StreamSource{Name: "M"},
	})
	require_NoError(t, err)

	createConsumer("M2", "foo")
	createConsumer("M2", "bar")
	createConsumer("M2", "baz.foo")
}

func TestJetStreamMirrorBasics(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	createStream := func(cfg *nats.StreamConfig) (*nats.StreamInfo, error) {
		return js.AddStream(cfg)
	}

	createStreamOk := func(cfg *nats.StreamConfig) {
		t.Helper()
		if _, err := createStream(cfg); err != nil {
			t.Fatalf("Expected no error, got %+v", err)
		}
	}

	// Test we get right config errors etc.
	cfg := &nats.StreamConfig{
		Name:     "M1",
		Subjects: []string{"foo", "bar", "baz"},
		Mirror:   &nats.StreamSource{Name: "S1"},
	}
	_, err := createStream(cfg)
	if err == nil || !strings.Contains(err.Error(), "stream mirrors can not") {
		t.Fatalf("Expected error, got %+v", err)
	}

	// Clear subjects.
	cfg.Subjects = nil

	// Mirrored
	scfg := &nats.StreamConfig{
		Name:     "S1",
		Subjects: []string{"foo", "bar", "baz"},
	}

	// Create mirrored stream
	createStreamOk(scfg)

	// Now create our mirror stream.
	createStreamOk(cfg)

	// For now wait for the consumer state to register.
	time.Sleep(250 * time.Millisecond)

	// Send 100 messages.
	for i := 0; i < 100; i++ {
		if _, err := js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Faster timeout since we loop below checking for condition.
	js2, err := nc.JetStream(nats.MaxWait(250 * time.Millisecond))
	require_NoError(t, err)

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		si, err := js2.StreamInfo("M1")
		require_NoError(t, err)

		if si.State.Msgs != 100 {
			return fmt.Errorf("Expected 100 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// Purge the mirrored stream.
	if err := js.PurgeStream("S1"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
	// Send 50 more msgs now.
	for i := 0; i < 50; i++ {
		if _, err := js.Publish("bar", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	cfg = &nats.StreamConfig{
		Name:    "M2",
		Storage: nats.FileStorage,
		Mirror:  &nats.StreamSource{Name: "S1"},
	}

	// Now create our second mirror stream.
	createStreamOk(cfg)

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		si, err := js2.StreamInfo("M2")
		require_NoError(t, err)

		if si.State.Msgs != 50 {
			return fmt.Errorf("Expected 50 msgs, got state: %+v", si.State)
		}
		if si.State.FirstSeq != 101 {
			return fmt.Errorf("Expected start seq of 101, got state: %+v", si.State)
		}
		return nil
	})

	// Send 100 more msgs now. Should be 150 total, 101 first.
	for i := 0; i < 100; i++ {
		if _, err := js.Publish("baz", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	cfg = &nats.StreamConfig{
		Name:   "M3",
		Mirror: &nats.StreamSource{Name: "S1", OptStartSeq: 150},
	}

	createStreamOk(cfg)

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		si, err := js2.StreamInfo("M3")
		require_NoError(t, err)

		if si.State.Msgs != 101 {
			return fmt.Errorf("Expected 101 msgs, got state: %+v", si.State)
		}
		if si.State.FirstSeq != 150 {
			return fmt.Errorf("Expected start seq of 150, got state: %+v", si.State)
		}
		return nil
	})

	// Make sure setting time works ok.
	start := time.Now().UTC().Add(-2 * time.Hour)
	cfg = &nats.StreamConfig{
		Name:   "M4",
		Mirror: &nats.StreamSource{Name: "S1", OptStartTime: &start},
	}
	createStreamOk(cfg)

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		si, err := js2.StreamInfo("M4")
		require_NoError(t, err)

		if si.State.Msgs != 150 {
			return fmt.Errorf("Expected 150 msgs, got state: %+v", si.State)
		}
		if si.State.FirstSeq != 101 {
			return fmt.Errorf("Expected start seq of 101, got state: %+v", si.State)
		}
		return nil
	})

	// Test subject filtering and transformation
	createStreamServerStreamConfig := func(cfg *StreamConfig, errToCheck uint16) {
		t.Helper()
		req, err := json.Marshal(cfg)
		require_NoError(t, err)

		rm, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
		require_NoError(t, err)

		var resp JSApiStreamCreateResponse
		if err := json.Unmarshal(rm.Data, &resp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if errToCheck == 0 {
			if resp.Error != nil {
				t.Fatalf("Unexpected error: %+v", resp.Error)
			}
		} else {
			if resp.Error.ErrCode != errToCheck {
				t.Fatalf("Expected error %+v, got: %+v", errToCheck, resp.Error)
			}
		}
	}

	// check for errors
	createStreamServerStreamConfig(&StreamConfig{
		Name:    "MBAD",
		Storage: FileStorage,
		Mirror:  &StreamSource{Name: "S1", FilterSubject: "foo", SubjectTransforms: []SubjectTransformConfig{{Source: "foo", Destination: "foo3"}}},
	}, ApiErrors[JSMirrorMultipleFiltersNotAllowed].ErrCode)

	createStreamServerStreamConfig(&StreamConfig{
		Name:    "MBAD",
		Storage: FileStorage,
		Mirror:  &StreamSource{Name: "S1", SubjectTransforms: []SubjectTransformConfig{{Source: ".*.", Destination: "foo3"}}},
	}, ApiErrors[JSMirrorInvalidSubjectFilter].ErrCode)

	createStreamServerStreamConfig(&StreamConfig{
		Name:    "MBAD",
		Storage: FileStorage,
		Mirror:  &StreamSource{Name: "S1", SubjectTransforms: []SubjectTransformConfig{{Source: "*", Destination: "{{wildcard(2)}}"}}},
	}, ApiErrors[JSStreamCreateErrF].ErrCode)

	createStreamServerStreamConfig(&StreamConfig{
		Name:    "MBAD",
		Storage: FileStorage,
		Mirror:  &StreamSource{Name: "S1", SubjectTransforms: []SubjectTransformConfig{{Source: "foo", Destination: ""}, {Source: "foo", Destination: "bar"}}},
	}, ApiErrors[JSMirrorOverlappingSubjectFilters].ErrCode)

	createStreamServerStreamConfig(&StreamConfig{
		Name:    "M5",
		Storage: FileStorage,
		Mirror:  &StreamSource{Name: "S1", FilterSubject: "foo", SubjectTransformDest: "foo2"},
	}, 0)

	createStreamServerStreamConfig(&StreamConfig{
		Name:    "M6",
		Storage: FileStorage,
		Mirror:  &StreamSource{Name: "S1", SubjectTransforms: []SubjectTransformConfig{{Source: "bar", Destination: "bar2"}, {Source: "baz", Destination: "baz2"}}},
	}, 0)

	// Send 100 messages on foo (there should already be 50 messages on bar and 100 on baz in the stream)
	for i := 0; i < 100; i++ {
		if _, err := js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	var f = func(streamName string, subject string, subjectNumMsgs uint64, streamNumMsg uint64, firstSeq uint64, lastSeq uint64) func() error {
		return func() error {
			si, err := js2.StreamInfo(streamName, &nats.StreamInfoRequest{SubjectsFilter: ">"})
			require_NoError(t, err)
			if ss, ok := si.State.Subjects[subject]; !ok {
				t.Log("Expected messages with the transformed subject")
			} else {
				if ss != subjectNumMsgs {
					t.Fatalf("Expected %d messages on the transformed subject but got %d", subjectNumMsgs, ss)
				}
			}
			if si.State.Msgs != streamNumMsg {
				return fmt.Errorf("Expected %d stream messages, got state: %+v", streamNumMsg, si.State)
			}
			if si.State.FirstSeq != firstSeq || si.State.LastSeq != lastSeq {
				return fmt.Errorf("Expected first sequence=%d and last sequence=%d, but got state: %+v", firstSeq, lastSeq, si.State)
			}
			return nil
		}
	}

	checkFor(t, 2*time.Second, 100*time.Millisecond, f("M5", "foo2", 100, 100, 251, 350))
	checkFor(t, 2*time.Second, 100*time.Millisecond, f("M6", "bar2", 50, 150, 101, 250))
	checkFor(t, 2*time.Second, 100*time.Millisecond, f("M6", "baz2", 100, 150, 101, 250))

}

func TestJetStreamMirrorUpdatePreventsSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "ORIGINAL"})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{Name: "MIRROR", Mirror: &nats.StreamSource{Name: "ORIGINAL"}})
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{Name: "MIRROR", Mirror: &nats.StreamSource{Name: "ORIGINAL"}, Subjects: []string{"x"}})
	if err == nil || err.Error() != "nats: stream mirrors can not contain subjects" {
		t.Fatalf("Expected to not be able to put subjects on a stream, got: %+v", err)
	}
}

func TestJetStreamSourceBasics(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	createStream := func(cfg *StreamConfig) {
		t.Helper()
		req, err := json.Marshal(cfg)
		require_NoError(t, err)

		rm, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
		require_NoError(t, err)

		var resp JSApiStreamCreateResponse
		if err := json.Unmarshal(rm.Data, &resp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if resp.Error != nil {
			t.Fatalf("Unexpected error: %+v", resp.Error)
		}
	}

	if _, err := js.AddStream(&nats.StreamConfig{Name: "test", Sources: []*nats.StreamSource{{Name: ""}}}); err.Error() == "source stream name is invalid" {
		t.Fatal("Expected a source stream name is invalid error")
	}

	for _, sname := range []string{"foo", "bar", "baz"} {
		if _, err := js.AddStream(&nats.StreamConfig{Name: sname}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	sendBatch := func(subject string, n int) {
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}
	// Populate each one.
	sendBatch("foo", 10)
	sendBatch("bar", 15)
	sendBatch("baz", 25)

	cfg := &StreamConfig{
		Name:    "MS",
		Storage: FileStorage,
		Sources: []*StreamSource{
			{Name: "foo", SubjectTransformDest: "foo2.>"},
			{Name: "bar"},
			{Name: "baz"},
		},
	}

	createStream(cfg)

	// Faster timeout since we loop below checking for condition.
	js2, err := nc.JetStream(nats.MaxWait(250 * time.Millisecond))
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("MS")
		require_NoError(t, err)

		if si.State.Msgs != 50 {
			return fmt.Errorf("Expected 50 msgs, got state: %+v", si.State)
		}
		return nil
	})

	ss, err := js.SubscribeSync("foo2.foo", nats.BindStream("MS"))
	require_NoError(t, err)
	// we must have at least one message on the transformed subject name (ie no timeout)
	_, err = ss.NextMsg(time.Millisecond)
	require_NoError(t, err)
	ss.Drain()

	// Test Source Updates
	ncfg := &nats.StreamConfig{
		Name: "MS",
		Sources: []*nats.StreamSource{
			// Keep foo, bar, remove baz, add dlc
			{Name: "foo"},
			{Name: "bar"},
			{Name: "dlc"},
		},
	}
	if _, err := js.UpdateStream(ncfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Test optional start times, filtered subjects etc.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"dlc", "rip", "jnm"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("dlc", 20)
	sendBatch("rip", 20)
	sendBatch("dlc", 10)
	sendBatch("jnm", 10)

	cfg = &StreamConfig{
		Name:    "FMS",
		Storage: FileStorage,
		Sources: []*StreamSource{
			{Name: "TEST", OptStartSeq: 26},
		},
	}
	createStream(cfg)
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("FMS")
		require_NoError(t, err)
		if si.State.Msgs != 35 {
			return fmt.Errorf("Expected 35 msgs, got state: %+v", si.State)
		}
		return nil
	})
	// Double check first starting.
	m, err := js.GetMsg("FMS", 1)
	require_NoError(t, err)
	if shdr := m.Header.Get(JSStreamSource); shdr == _EMPTY_ {
		t.Fatalf("Expected a header, got none")
	} else if _, _, sseq := streamAndSeq(shdr); sseq != 26 {
		t.Fatalf("Expected header sequence of 26, got %d", sseq)
	}

	// Test Filters
	cfg = &StreamConfig{
		Name:    "FMS2",
		Storage: FileStorage,
		Sources: []*StreamSource{
			{Name: "TEST", OptStartSeq: 11, FilterSubject: "dlc", SubjectTransformDest: "dlc2"},
		},
	}
	createStream(cfg)
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("FMS2")
		require_NoError(t, err)
		if si.State.Msgs != 20 {
			return fmt.Errorf("Expected 20 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// Double check first starting.
	if m, err = js.GetMsg("FMS2", 1); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if shdr := m.Header.Get(JSStreamSource); shdr == _EMPTY_ {
		t.Fatalf("Expected a header, got none")
	} else if _, _, sseq := streamAndSeq(shdr); sseq != 11 {
		t.Fatalf("Expected header sequence of 11, got %d", sseq)
	}
	if m.Subject != "dlc2" {
		t.Fatalf("Expected transformed subject dlc2, but got %s instead", m.Subject)
	}

	// Test Filters
	cfg = &StreamConfig{
		Name:    "FMS3",
		Storage: FileStorage,
		Sources: []*StreamSource{
			{Name: "TEST", SubjectTransforms: []SubjectTransformConfig{{Source: "dlc", Destination: "dlc2"}, {Source: "rip", Destination: ""}}},
		},
	}
	createStream(cfg)
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("FMS3")
		require_NoError(t, err)
		if si.State.Msgs != 50 {
			return fmt.Errorf("Expected 50 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// Double check first message
	if m, err = js.GetMsg("FMS3", 1); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if shdr := m.Header.Get(JSStreamSource); shdr == _EMPTY_ {
		t.Fatalf("Expected a header, got none")
	} else if m.Subject != "dlc2" {
		t.Fatalf("Expected subject 'dlc2' and got %s", m.Subject)
	}

	// Double check first message with the other subject
	if m, err = js.GetMsg("FMS3", 21); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if shdr := m.Header.Get(JSStreamSource); shdr == _EMPTY_ {
		t.Fatalf("Expected a header, got none")
	} else if m.Subject != "rip" {
		t.Fatalf("Expected subject 'rip' and got %s", m.Subject)
	}

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("FMS3")
		require_NoError(t, err)
		if si.State.Subjects["jnm"] != 0 {
			return fmt.Errorf("Unexpected messages from the source found")
		}
		return nil
	})
}

func TestJetStreamInputTransform(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	createStream := func(cfg *StreamConfig) {
		t.Helper()
		req, err := json.Marshal(cfg)
		require_NoError(t, err)

		rm, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
		require_NoError(t, err)

		var resp JSApiStreamCreateResponse
		if err := json.Unmarshal(rm.Data, &resp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if resp.Error != nil {
			t.Fatalf("Unexpected error: %+v", resp.Error)
		}
	}

	createStream(&StreamConfig{Name: "T1", Subjects: []string{"foo"}, SubjectTransform: &SubjectTransformConfig{Source: ">", Destination: "transformed.>"}, Storage: MemoryStorage})

	// publish a message
	if _, err := js.Publish("foo", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	m, err := js.GetMsg("T1", 1)
	require_NoError(t, err)

	if m.Subject != "transformed.foo" {
		t.Fatalf("Expected message subject transformed.foo, got %s", m.Subject)
	}
}

func TestJetStreamOperatorAccounts(t *testing.T) {
	s, _ := RunServerWithConfig("./configs/js-op.conf")
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s, nats.UserCredentials("./configs/one.creds"))
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 100
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("TEST", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Close our user for account one.
	nc.Close()

	// Restart the server.
	s.Shutdown()
	s, _ = RunServerWithConfig("./configs/js-op.conf")
	defer s.Shutdown()

	jsz, err := s.Jsz(nil)
	require_NoError(t, err)

	if jsz.Streams != 1 {
		t.Fatalf("Expected jsz to report our stream on restart")
	}
	if jsz.Messages != uint64(toSend) {
		t.Fatalf("Expected jsz to report our %d messages on restart, got %d", toSend, jsz.Messages)
	}
}

func TestJetStreamServerDomainBadConfig(t *testing.T) {
	shouldFail := func(domain string) {
		t.Helper()
		opts := DefaultTestOptions
		opts.JetStreamDomain = domain
		if err := validateOptions(&opts); err == nil || !strings.Contains(err.Error(), "invalid domain name") {
			t.Fatalf("Expected bad domain error, got %v", err)
		}
	}

	shouldFail("HU..B")
	shouldFail("HU B")
	shouldFail(" ")
	shouldFail("\t")
	shouldFail("CORE.")
	shouldFail(".CORE")
	shouldFail("C.*.O. RE")
	shouldFail("C.ORE")
}

func TestJetStreamServerDomainConfig(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {domain: "HUB"}
	`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}

	config := s.JetStreamConfig()
	if config != nil {
		defer removeDir(t, config.StoreDir)
	}
	if config.Domain != "HUB" {
		t.Fatalf("Expected %q as domain name, got %q", "HUB", config.Domain)
	}
}

func TestJetStreamServerDomainConfigButDisabled(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {domain: "HUB", enabled: false}
	`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream NOT to be enabled")
	}

	opts := s.getOpts()
	if opts.JetStreamDomain != "HUB" {
		t.Fatalf("Expected %q as opts domain name, got %q", "HUB", opts.JetStreamDomain)
	}
}

func TestJetStreamDomainInPubAck(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {domain: "HUB"}
	`))

	s, _ := RunServerWithConfig(conf)
	config := s.JetStreamConfig()
	if config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Storage:  nats.MemoryStorage,
		Subjects: []string{"foo"},
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check by hand for now til it makes its way into Go client.
	am, err := nc.Request("foo", nil, time.Second)
	require_NoError(t, err)
	var pa PubAck
	json.Unmarshal(am.Data, &pa)
	if pa.Domain != "HUB" {
		t.Fatalf("Expected PubAck to have domain of %q, got %q", "HUB", pa.Domain)
	}
}

// Issue #2213
func TestJetStreamDirectConsumersBeingReported(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name: "S",
		Sources: []*nats.StreamSource{{
			Name: "TEST",
		}},
	})
	require_NoError(t, err)

	if _, err = js.Publish("foo", nil); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("S")
		if err != nil {
			return fmt.Errorf("Could not get stream info: %v", err)
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg, got state: %+v", si.State)
		}
		return nil
	})

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	// Direct consumers should not be reported
	if si.State.Consumers != 0 {
		t.Fatalf("Did not expect any consumers, got %d", si.State.Consumers)
	}

	// Now check for consumer in consumer names list.
	var names []string
	for name := range js.ConsumerNames("TEST") {
		names = append(names, name)
	}
	if len(names) != 0 {
		t.Fatalf("Expected no consumers but got %+v", names)
	}

	// Now check detailed list.
	var cis []*nats.ConsumerInfo
	for ci := range js.ConsumersInfo("TEST") {
		cis = append(cis, ci)
	}
	if len(cis) != 0 {
		t.Fatalf("Expected no consumers but got %+v", cis)
	}
}

// https://github.com/nats-io/nats-server/issues/2290
func TestJetStreamTemplatedErrorsBug(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	_, err = js.PullSubscribe("foo", "")
	if err != nil && strings.Contains(err.Error(), "{err}") {
		t.Fatalf("Error is not filled in: %v", err)
	}
}

func TestJetStreamServerEncryption(t *testing.T) {
	cases := []struct {
		name   string
		cstr   string
		cipher StoreCipher
	}{
		{"Default", _EMPTY_, ChaCha},
		{"ChaCha", ", cipher: chacha", ChaCha},
		{"AES", ", cipher: aes", AES},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tmpl := `
				server_name: S22
				listen: 127.0.0.1:-1
				jetstream: {key: $JS_KEY, store_dir: '%s' %s}
			`
			storeDir := t.TempDir()

			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, storeDir, c.cstr)))

			os.Setenv("JS_KEY", "s3cr3t!!")
			defer os.Unsetenv("JS_KEY")

			s, _ := RunServerWithConfig(conf)
			defer s.Shutdown()

			config := s.JetStreamConfig()
			if config == nil {
				t.Fatalf("Expected config but got none")
			}
			defer removeDir(t, config.StoreDir)

			// Client based API
			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			cfg := &nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo", "bar", "baz"},
			}
			if _, err := js.AddStream(cfg); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			msg := []byte("ENCRYPTED PAYLOAD!!")
			sendMsg := func(subj string) {
				t.Helper()
				if _, err := js.Publish(subj, msg); err != nil {
					t.Fatalf("Unexpected publish error: %v", err)
				}
			}
			// Send 10 msgs
			for i := 0; i < 10; i++ {
				sendMsg("foo")
			}

			// Now create a consumer.
			sub, err := js.PullSubscribe("foo", "dlc")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			for i, m := range fetchMsgs(t, sub, 10, 5*time.Second) {
				if i < 5 {
					m.AckSync()
				}
			}

			// Grab our state to compare after restart.
			si, _ := js.StreamInfo("TEST")
			ci, _ := js.ConsumerInfo("TEST", "dlc")

			// Quick check to make sure everything not just plaintext still.
			sdir := filepath.Join(config.StoreDir, "$G", "streams", "TEST")
			// Make sure we can not find any plaintext strings in the target file.
			checkFor := func(fn string, strs ...string) {
				t.Helper()
				data, err := os.ReadFile(fn)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				for _, str := range strs {
					if bytes.Contains(data, []byte(str)) {
						t.Fatalf("Found %q in body of file contents", str)
					}
				}
			}
			checkKeyFile := func(fn string) {
				t.Helper()
				if _, err := os.Stat(fn); err != nil {
					t.Fatalf("Expected a key file at %q", fn)
				}
			}

			// Check stream meta.
			checkEncrypted := func() {
				checkKeyFile(filepath.Join(sdir, JetStreamMetaFileKey))
				checkFor(filepath.Join(sdir, JetStreamMetaFile), "TEST", "foo", "bar", "baz", "max_msgs", "max_bytes")
				// Check a message block.
				checkKeyFile(filepath.Join(sdir, "msgs", "1.key"))
				checkFor(filepath.Join(sdir, "msgs", "1.blk"), "ENCRYPTED PAYLOAD!!", "foo", "bar", "baz")

				// Check consumer meta and state.
				checkKeyFile(filepath.Join(sdir, "obs", "dlc", JetStreamMetaFileKey))
				checkFor(filepath.Join(sdir, "obs", "dlc", JetStreamMetaFile), "TEST", "dlc", "foo", "bar", "baz", "max_msgs", "ack_policy")
				// Load and see if we can parse the consumer state.
				state, err := os.ReadFile(filepath.Join(sdir, "obs", "dlc", "o.dat"))
				require_NoError(t, err)

				if _, err := decodeConsumerState(state); err == nil {
					t.Fatalf("Expected decoding consumer state to fail")
				}
			}

			// Stop current
			s.Shutdown()

			checkEncrypted()

			// Restart.
			s, _ = RunServerWithConfig(conf)
			defer s.Shutdown()

			// Connect again.
			nc, js = jsClientConnect(t, s)
			defer nc.Close()

			si2, err := js.StreamInfo("TEST")
			require_NoError(t, err)

			if !reflect.DeepEqual(si, si2) {
				t.Fatalf("Stream infos did not match\n%+v\nvs\n%+v", si, si2)
			}

			ci2, _ := js.ConsumerInfo("TEST", "dlc")
			// Consumer create times can be slightly off after restore from disk.
			now := time.Now()
			ci.Created, ci2.Created = now, now
			ci.Delivered.Last, ci2.Delivered.Last = nil, nil
			ci.AckFloor.Last, ci2.AckFloor.Last = nil, nil
			// Also clusters will be different.
			ci.Cluster, ci2.Cluster = nil, nil
			if !reflect.DeepEqual(ci, ci2) {
				t.Fatalf("Consumer infos did not match\n%+v\nvs\n%+v", ci, ci2)
			}

			// Send 10 more msgs
			for i := 0; i < 10; i++ {
				sendMsg("foo")
			}
			if si, err = js.StreamInfo("TEST"); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si.State.Msgs != 20 {
				t.Fatalf("Expected 20 msgs total, got %d", si.State.Msgs)
			}

			// Now test snapshots etc.
			acc := s.GlobalAccount()
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)
			scfg := mset.config()
			sr, err := mset.snapshot(5*time.Second, false, true)
			if err != nil {
				t.Fatalf("Error getting snapshot: %v", err)
			}
			snapshot, err := io.ReadAll(sr.Reader)
			if err != nil {
				t.Fatalf("Error reading snapshot")
			}

			// Run new server w/o encryption. Make sure we can restore properly (meaning encryption was stripped etc).
			ns := RunBasicJetStreamServer(t)
			defer ns.Shutdown()

			nacc := ns.GlobalAccount()
			r := bytes.NewReader(snapshot)
			mset, err = nacc.RestoreStream(&scfg, r)
			require_NoError(t, err)
			ss := mset.store.State()
			if ss.Msgs != si.State.Msgs || ss.FirstSeq != si.State.FirstSeq || ss.LastSeq != si.State.LastSeq {
				t.Fatalf("Stream states do not match: %+v vs %+v", ss, si.State)
			}

			// Now restore to our encrypted server as well.
			if err := js.DeleteStream("TEST"); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			acc = s.GlobalAccount()
			r.Reset(snapshot)
			mset, err = acc.RestoreStream(&scfg, r)
			require_NoError(t, err)
			ss = mset.store.State()
			if ss.Msgs != si.State.Msgs || ss.FirstSeq != si.State.FirstSeq || ss.LastSeq != si.State.LastSeq {
				t.Fatalf("Stream states do not match: %+v vs %+v", ss, si.State)
			}

			// Check that all is encrypted like above since we know we need to convert since snapshots always plaintext.
			checkEncrypted()
		})
	}
}

// User report of bug.
func TestJetStreamConsumerBadNumPending(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.*"},
	})
	require_NoError(t, err)

	newOrders := func(n int) {
		// Queue up new orders.
		for i := 0; i < n; i++ {
			js.Publish("orders.created", []byte("NEW"))
		}
	}

	newOrders(10)

	// Create to subscribers.
	process := func(m *nats.Msg) {
		js.Publish("orders.approved", []byte("APPROVED"))
	}

	op, err := js.Subscribe("orders.created", process)
	require_NoError(t, err)

	defer op.Unsubscribe()

	mon, err := js.SubscribeSync("orders.*")
	require_NoError(t, err)

	defer mon.Unsubscribe()

	waitForMsgs := func(n uint64) {
		t.Helper()
		checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
			si, err := js.StreamInfo("ORDERS")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si.State.Msgs != n {
				return fmt.Errorf("Expected %d msgs, got state: %+v", n, si.State)
			}
			return nil
		})
	}

	checkForNoPending := func(sub *nats.Subscription) {
		t.Helper()
		if ci, err := sub.ConsumerInfo(); err != nil || ci == nil || ci.NumPending != 0 {
			if ci != nil && ci.NumPending != 0 {
				t.Fatalf("Bad consumer NumPending, expected 0 but got %d", ci.NumPending)
			} else {
				t.Fatalf("Bad consumer info: %+v", ci)
			}
		}
	}

	waitForMsgs(20)
	checkForNoPending(op)
	checkForNoPending(mon)

	newOrders(10)

	waitForMsgs(40)
	checkForNoPending(op)
	checkForNoPending(mon)
}

func TestJetStreamDeliverLastPerSubject(t *testing.T) {
	for _, st := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(st.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			// Client for API requests.
			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			cfg := StreamConfig{
				Name:       "KV",
				Subjects:   []string{"kv.>"},
				Storage:    st,
				MaxMsgsPer: 5,
			}

			req, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Do manually for now.
			nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			si, err := js.StreamInfo("KV")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si == nil || si.Config.Name != "KV" {
				t.Fatalf("StreamInfo is not correct %+v", si)
			}

			// Interleave them on purpose.
			for i := 1; i <= 11; i++ {
				msg := []byte(fmt.Sprintf("%d", i))
				js.PublishAsync("kv.b1.foo", msg)
				js.PublishAsync("kv.b2.foo", msg)

				js.PublishAsync("kv.b1.bar", msg)
				js.PublishAsync("kv.b2.bar", msg)

				js.PublishAsync("kv.b1.baz", msg)
				js.PublishAsync("kv.b2.baz", msg)
			}

			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(2 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}

			// Do quick check that config needs FilteredSubjects otherwise bad config.
			badReq := CreateConsumerRequest{
				Stream: "KV",
				Config: ConsumerConfig{
					DeliverSubject: "b",
					DeliverPolicy:  DeliverLastPerSubject,
				},
			}
			req, err = json.Marshal(badReq)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			resp, err := nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "KV"), req, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var ccResp JSApiConsumerCreateResponse
			if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ccResp.Error == nil || !strings.Contains(ccResp.Error.Description, "filter subject is not set") {
				t.Fatalf("Expected an error, got none")
			}

			// Now let's consume these via last per subject.
			obsReq := CreateConsumerRequest{
				Stream: "KV",
				Config: ConsumerConfig{
					DeliverSubject: "d",
					DeliverPolicy:  DeliverLastPerSubject,
					FilterSubject:  "kv.b1.*",
				},
			}
			req, err = json.Marshal(obsReq)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			resp, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "KV"), req, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			ccResp.Error = nil
			if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			sub, _ := nc.SubscribeSync("d")
			defer sub.Unsubscribe()

			// Helper to check messages are correct.
			checkNext := func(subject string, sseq uint64, v string) {
				t.Helper()
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error receiving message: %v", err)
				}
				if m.Subject != subject {
					t.Fatalf("Expected subject %q but got %q", subject, m.Subject)
				}
				meta, err := m.Metadata()
				if err != nil {
					t.Fatalf("didn't get metadata: %s", err)
				}
				if meta.Sequence.Stream != sseq {
					t.Fatalf("Expected stream seq %d but got %d", sseq, meta.Sequence.Stream)
				}
				if string(m.Data) != v {
					t.Fatalf("Expected data of %q but got %q", v, m.Data)
				}
			}

			checkSubsPending(t, sub, 3)

			// Now make sure they are what we expect.
			checkNext("kv.b1.foo", 61, "11")
			checkNext("kv.b1.bar", 63, "11")
			checkNext("kv.b1.baz", 65, "11")

			msg := []byte(fmt.Sprintf("%d", 22))
			js.Publish("kv.b1.bar", msg)
			js.Publish("kv.b2.foo", msg) // Not filtered through..

			checkSubsPending(t, sub, 1)
			checkNext("kv.b1.bar", 67, "22")
		})
	}
}

func TestJetStreamDeliverLastPerSubjectNumPending(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:              "KV",
		Subjects:          []string{"KV.>"},
		MaxMsgsPerSubject: 5,
		Replicas:          1,
	}); err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	for i := 0; i < 5; i++ {
		msg := []byte(fmt.Sprintf("msg%d", i))
		js.Publish("KV.foo", msg)
		js.Publish("KV.bar", msg)
		js.Publish("KV.baz", msg)
		js.Publish("KV.bat", msg)
	}

	// Delete some messages
	js.DeleteMsg("KV", 2)
	js.DeleteMsg("KV", 5)

	ci, err := js.AddConsumer("KV", &nats.ConsumerConfig{
		DeliverSubject: nats.NewInbox(),
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverPolicy:  nats.DeliverLastPerSubjectPolicy,
		FilterSubject:  "KV.>",
	})
	if err != nil {
		t.Fatalf("Error adding consumer: %v", err)
	}
	if ci.NumPending != 4 {
		t.Fatalf("Expected 4 pending msgs, got %v", ci.NumPending)
	}
}

// We had a report of a consumer delete crashing the server when in interest retention mode.
// This I believe is only really possible in clustered mode, but we will force the issue here.
func TestJetStreamConsumerCleanupWithRetentionPolicy(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "ORDERS",
		Subjects:  []string{"orders.*"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("orders.*")
	require_NoError(t, err)

	payload := []byte("Hello World")
	for i := 0; i < 10; i++ {
		subj := fmt.Sprintf("orders.%d", i+1)
		js.Publish(subj, payload)
	}

	checkSubsPending(t, sub, 10)

	for i := 0; i < 10; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		m.AckSync()
	}

	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}

	acc := s.GlobalAccount()
	mset, err := acc.lookupStream("ORDERS")
	require_NoError(t, err)

	o := mset.lookupConsumer(ci.Name)
	if o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	lseq := mset.lastSeq()
	o.mu.Lock()
	// Force boundary condition here.
	o.asflr = lseq + 2
	o.mu.Unlock()
	sub.Unsubscribe()

	// Make sure server still available.
	if _, err := js.StreamInfo("ORDERS"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

// Issue #2392
func TestJetStreamPurgeEffectsConsumerDelivery(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*"},
	})
	require_NoError(t, err)

	js.Publish("foo.a", []byte("show once"))

	sub, err := js.SubscribeSync("foo.*", nats.AckWait(250*time.Millisecond), nats.DeliverAll(), nats.AckExplicit())
	require_NoError(t, err)

	defer sub.Unsubscribe()

	checkSubsPending(t, sub, 1)

	// Do not ack.
	if _, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}

	// Now purge stream.
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}

	js.Publish("foo.b", []byte("show twice?"))
	// Do not ack again, should show back up.
	if _, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	// Make sure we get it back.
	if _, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
}

// Issue #2403
func TestJetStreamExpireCausesDeadlock(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.*"},
		Storage:   nats.MemoryStorage,
		MaxMsgs:   10,
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("foo.bar")
	require_NoError(t, err)

	defer sub.Unsubscribe()

	// Publish from two connections to get the write lock request wedged in between
	// having the RLock and wanting it again deeper in the stack.
	nc2, js2 := jsClientConnect(t, s)
	defer nc2.Close()

	for i := 0; i < 1000; i++ {
		js.PublishAsync("foo.bar", []byte("HELLO"))
		js2.PublishAsync("foo.bar", []byte("HELLO"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// If we deadlocked then we will not be able to get stream info.
	if _, err := js.StreamInfo("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamConsumerPendingBugWithKV(t *testing.T) {
	msc := StreamConfig{
		Name:       "KV",
		Subjects:   []string{"kv.>"},
		Storage:    MemoryStorage,
		MaxMsgsPer: 1,
	}
	fsc := msc
	fsc.Storage = FileStorage

	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &msc},
		{"FileStore", &fsc},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			// Client based API
			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			// Not in Go client under server yet.
			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			js.Publish("kv.1", []byte("1"))
			js.Publish("kv.2", []byte("2"))
			js.Publish("kv.3", []byte("3"))
			js.Publish("kv.1", []byte("4"))

			si, err := js.StreamInfo("KV")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si.State.Msgs != 3 {
				t.Fatalf("Expected 3 total msgs, got %d", si.State.Msgs)
			}

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "dlc",
				DeliverSubject: "xxx",
				DeliverPolicy:  DeliverLastPerSubject,
				FilterSubject:  ">",
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ci := o.info(); ci.NumPending != 3 {
				t.Fatalf("Expected pending of 3, got %d", ci.NumPending)
			}
		})
	}
}

// Issue #2420
func TestJetStreamDefaultMaxMsgsPer(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*"},
		Storage:  nats.MemoryStorage,
		MaxMsgs:  10,
	})
	require_NoError(t, err)

	if si.Config.MaxMsgsPerSubject != -1 {
		t.Fatalf("Expected default of -1, got %d", si.Config.MaxMsgsPerSubject)
	}
}

// Issue #2423
func TestJetStreamBadConsumerCreateErr(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*"},
		Storage:  nats.MemoryStorage,
	})
	require_NoError(t, err)

	// When adding a consumer with both deliver subject and max wait (push vs pull),
	// we got the wrong err about deliver subject having a wildcard.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "nowcerr",
		DeliverSubject: "X",
		MaxWaiting:     100,
	})
	if err == nil {
		t.Fatalf("Expected an error but got none")
	}
	if !strings.Contains(err.Error(), "push mode can not set max waiting") {
		t.Fatalf("Incorrect error returned: %v", err)
	}
}

func TestJetStreamConsumerPushBound(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Storage:  nats.MemoryStorage,
		Subjects: []string{"foo"},
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// We want to test extended consumer info for push based consumers.
	// We need to do these by hand for now.
	createConsumer := func(name, deliver string) {
		t.Helper()
		creq := CreateConsumerRequest{
			Stream: "TEST",
			Config: ConsumerConfig{
				Durable:        name,
				DeliverSubject: deliver,
				AckPolicy:      AckExplicit,
			},
		}
		req, err := json.Marshal(creq)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		resp, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", name), req, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var ccResp JSApiConsumerCreateResponse
		if err := json.Unmarshal(resp.Data, &ccResp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ccResp.ConsumerInfo == nil || ccResp.Error != nil {
			t.Fatalf("Got a bad response %+v", ccResp)
		}
	}

	consumerInfo := func(name string) *ConsumerInfo {
		t.Helper()
		resp, err := nc.Request(fmt.Sprintf(JSApiConsumerInfoT, "TEST", name), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var cinfo JSApiConsumerInfoResponse
		if err := json.Unmarshal(resp.Data, &cinfo); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if cinfo.ConsumerInfo == nil || cinfo.Error != nil {
			t.Fatalf("Got a bad response %+v", cinfo)
		}
		return cinfo.ConsumerInfo
	}

	// First create a durable push and make sure we show now active status.
	createConsumer("dlc", "d.X")
	if ci := consumerInfo("dlc"); ci.PushBound {
		t.Fatalf("Expected push bound to be false")
	}
	// Now bind the deliver subject.
	sub, _ := nc.SubscribeSync("d.X")
	nc.Flush() // Make sure it registers.
	// Check that its reported.
	if ci := consumerInfo("dlc"); !ci.PushBound {
		t.Fatalf("Expected push bound to be set")
	}
	sub.Unsubscribe()
	nc.Flush() // Make sure it registers.
	if ci := consumerInfo("dlc"); ci.PushBound {
		t.Fatalf("Expected push bound to be false")
	}

	// Now make sure we have queue groups indictated as needed.
	createConsumer("ik", "d.Z")
	// Now bind the deliver subject with a queue group.
	sub, _ = nc.QueueSubscribeSync("d.Z", "g22")
	defer sub.Unsubscribe()
	nc.Flush() // Make sure it registers.
	// Check that queue group is not reported.
	if ci := consumerInfo("ik"); ci.PushBound {
		t.Fatalf("Expected push bound to be false")
	}
	sub.Unsubscribe()
	nc.Flush() // Make sure it registers.
	if ci := consumerInfo("ik"); ci.PushBound {
		t.Fatalf("Expected push bound to be false")
	}

	// Make sure pull consumers report PushBound as false by default.
	createConsumer("rip", _EMPTY_)
	if ci := consumerInfo("rip"); ci.PushBound {
		t.Fatalf("Expected push bound to be false")
	}
}

// Got a report of memory leaking, tracked it to internal clients for consumers.
func TestJetStreamConsumerInternalClientLeak(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:    "TEST",
		Storage: nats.MemoryStorage,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ga, sa := s.GlobalAccount(), s.SystemAccount()
	ncb, nscb := ga.NumConnections(), sa.NumConnections()

	// Create 10 consumers
	for i := 0; i < 10; i++ {
		ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{DeliverSubject: "x"})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Accelerate ephemeral cleanup.
		mset, err := ga.lookupStream("TEST")
		if err != nil {
			t.Fatalf("Expected to find a stream for %q", "TEST")
		}
		o := mset.lookupConsumer(ci.Name)
		if o == nil {
			t.Fatalf("Error looking up consumer %q", ci.Name)
		}
		o.setInActiveDeleteThreshold(500 * time.Millisecond)
	}

	// Wait for them to all go away.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Consumers == 0 {
			return nil
		}
		return fmt.Errorf("Consumers still present")
	})
	// Make sure we are not leaking clients/connections.
	// Server does not see these so need to look at account.
	if nca := ga.NumConnections(); nca != ncb {
		t.Fatalf("Leaked clients in global account: %d vs %d", ncb, nca)
	}
	if nsca := sa.NumConnections(); nsca != nscb {
		t.Fatalf("Leaked clients in system account: %d vs %d", nscb, nsca)
	}
}

func TestJetStreamConsumerEventingRaceOnShutdown(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s, nats.NoReconnect())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  nats.MemoryStorage,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			if _, err := js.SubscribeSync("foo", nats.BindStream("TEST")); err != nil {
				return
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	s.Shutdown()

	wg.Wait()
}

// Got a report of streams that expire all messages while the server is down report errors when clients reconnect
// and try to send new messages.
func TestJetStreamExpireAllWhileServerDown(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:   "TEST",
		MaxAge: 250 * time.Millisecond,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	toSend := 10_000
	for i := 0; i < toSend; i++ {
		js.PublishAsync("TEST", []byte("OK"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()

	time.Sleep(300 * time.Millisecond)

	// Restart after expire.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	if si, err := js.StreamInfo("TEST"); err != nil || si.State.Msgs != 0 {
		t.Fatalf("Unexpected stream info state: %+v", si)
	}

	for i := 0; i < 10; i++ {
		if _, err := js.Publish("TEST", []byte("OK")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	if si, err := js.StreamInfo("TEST"); err != nil || si.State.Msgs != 10 {
		t.Fatalf("Unexpected stream info state: %+v", si)
	}
}

func TestJetStreamLongStreamNamesAndPubAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     strings.Repeat("ZABC", 256/4)[:255],
		Subjects: []string{"foo"},
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	js.Publish("foo", []byte("HELLO"))
}

func TestJetStreamPerSubjectPending(t *testing.T) {
	for _, st := range []nats.StorageType{nats.FileStorage, nats.MemoryStorage} {
		t.Run(st.String(), func(t *testing.T) {

			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:              "KV_X",
				Subjects:          []string{"$KV.X.>"},
				MaxMsgsPerSubject: 5,
				Storage:           st,
			})
			if err != nil {
				t.Fatalf("add stream failed: %s", err)
			}

			// the message we will care for
			_, err = js.Publish("$KV.X.x.y.z", []byte("hello world"))
			if err != nil {
				t.Fatalf("publish failed: %s", err)
			}

			// make sure there's some unrelated message after
			_, err = js.Publish("$KV.X.1", []byte("hello world"))
			if err != nil {
				t.Fatalf("publish failed: %s", err)
			}

			// we expect the wildcard filter subject to match only the one message and so pending will be 0
			sub, err := js.SubscribeSync("$KV.X.x.>", nats.DeliverLastPerSubject())
			if err != nil {
				t.Fatalf("subscribe failed: %s", err)
			}

			msg, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("next failed: %s", err)
			}

			meta, err := msg.Metadata()
			if err != nil {
				t.Fatalf("meta failed: %s", err)
			}

			// with DeliverLastPerSubject set this is never 0, but without setting that its 0 correctly
			if meta.NumPending != 0 {
				t.Fatalf("expected numpending 0 got %d", meta.NumPending)
			}
		})
	}
}

func TestJetStreamPublishExpectNoMsg(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "KV",
		Subjects:          []string{"KV.>"},
		MaxMsgsPerSubject: 5,
	})
	if err != nil {
		t.Fatalf("add stream failed: %s", err)
	}

	if _, err = js.Publish("KV.22", []byte("hello world")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// This should succeed.
	m := nats.NewMsg("KV.33")
	m.Header.Set(JSExpectedLastSubjSeq, "0")
	if _, err := js.PublishMsg(m); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// This should fail.
	m = nats.NewMsg("KV.22")
	m.Header.Set(JSExpectedLastSubjSeq, "0")
	if _, err := js.PublishMsg(m); err == nil {
		t.Fatalf("Expected error: %v", err)
	}

	if err := js.PurgeStream("KV"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}

	// This should succeed now.
	if _, err := js.PublishMsg(m); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamPullLargeBatchExpired(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("add stream failed: %s", err)
	}

	sub, err := js.PullSubscribe("foo", "dlc", nats.PullMaxWaiting(10), nats.MaxAckPending(10*50_000_000))
	if err != nil {
		t.Fatalf("Error creating pull subscriber: %v", err)
	}

	// Queue up 10 batch requests with timeout.
	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", "dlc")
	req := &JSApiConsumerGetNextRequest{Batch: 50_000_000, Expires: 100 * time.Millisecond}
	jreq, _ := json.Marshal(req)
	for i := 0; i < 10; i++ {
		nc.PublishRequest(rsubj, "bar", jreq)
	}
	nc.Flush()

	// Let them all expire.
	time.Sleep(150 * time.Millisecond)

	// Now do another and measure how long to timeout and shutdown the server.
	start := time.Now()
	sub.Fetch(1, nats.MaxWait(100*time.Millisecond))
	s.Shutdown()

	if delta := time.Since(start); delta > 200*time.Millisecond {
		t.Fatalf("Took too long to expire: %v", delta)
	}
}

func TestJetStreamNegativeDupeWindow(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// we incorrectly set MaxAge to -1 which then as a side effect sets dupe window to -1 which should fail
	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "TEST",
		Subjects:          nil,
		Retention:         nats.WorkQueuePolicy,
		MaxConsumers:      1,
		MaxMsgs:           -1,
		MaxBytes:          -1,
		Discard:           nats.DiscardNew,
		MaxAge:            -1,
		MaxMsgsPerSubject: -1,
		MaxMsgSize:        -1,
		Storage:           nats.FileStorage,
		Replicas:          1,
		NoAck:             false,
	})
	if err == nil || err.Error() != "nats: duplicates window can not be negative" {
		t.Fatalf("Expected dupe window error got: %v", err)
	}
}

// Issue #2551
func TestJetStreamMirroredConsumerFailAfterRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "S1",
		Storage:  nats.FileStorage,
		Subjects: []string{"foo", "bar", "baz"},
	})
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:    "M1",
		Storage: nats.FileStorage,
		Mirror:  &nats.StreamSource{Name: "S1"},
	})
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}

	_, err = js.AddConsumer("M1", &nats.ConsumerConfig{
		Durable:       "C1",
		FilterSubject: ">",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("consumer create failed: %s", err)
	}

	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	s.WaitForShutdown()

	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	_, err = js.StreamInfo("M1")
	if err != nil {
		t.Fatalf("%s did not exist after start: %s", "M1", err)
	}

	_, err = js.ConsumerInfo("M1", "C1")
	if err != nil {
		t.Fatalf("C1 did not exist after start: %s", err)
	}
}

func TestJetStreamDisabledLimitsEnforcementJWT(t *testing.T) {
	updateJwt := func(url string, akp nkeys.KeyPair, pubKey string, jwt string) {
		t.Helper()
		c := natsConnect(t, url, createUserCreds(t, nil, akp))
		defer c.Close()
		if msg, err := c.Request(fmt.Sprintf(accUpdateEventSubjNew, pubKey), []byte(jwt), time.Second); err != nil {
			t.Fatal("error not expected in this test", err)
		} else {
			content := make(map[string]interface{})
			if err := json.Unmarshal(msg.Data, &content); err != nil {
				t.Fatalf("%v", err)
			} else if _, ok := content["data"]; !ok {
				t.Fatalf("did not get an ok response got: %v", content)
			}
		}
	}
	// create system account
	sysKp, _ := nkeys.CreateAccount()
	sysPub, _ := sysKp.PublicKey()
	// limits to apply and check
	limits1 := jwt.JetStreamLimits{MemoryStorage: 1024, DiskStorage: 0, Streams: 1, Consumer: 2}
	akp, _ := nkeys.CreateAccount()
	aPub, _ := akp.PublicKey()
	claim := jwt.NewAccountClaims(aPub)
	claim.Limits.JetStreamLimits = limits1
	aJwt1, err := claim.Encode(oKp)
	require_NoError(t, err)
	dir := t.TempDir()
	storeDir1 := t.TempDir()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		jetstream: {store_dir: '%s'}
		operator: %s
		resolver: {
			type: full
			dir: '%s'
		}
		system_account: %s
    `, storeDir1, ojwt, dir, sysPub)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()
	updateJwt(s.ClientURL(), sysKp, aPub, aJwt1)
	c := natsConnect(t, s.ClientURL(), createUserCreds(t, nil, akp), nats.ReconnectWait(200*time.Millisecond))
	defer c.Close()
	// keep using the same connection
	js, err := c.JetStream()
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "disk",
		Storage:  nats.FileStorage,
		Subjects: []string{"disk"},
	})
	require_Error(t, err)
}

func TestJetStreamDisabledLimitsEnforcement(t *testing.T) {
	storeDir1 := t.TempDir()
	conf1 := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		accounts {
			one {
				jetstream: {
					mem: 1024
					disk: 0
					streams: 1
					consumers: 2
				}
				users [{user: one, password: password}]
			}
		}
		no_auth_user: one
	`, storeDir1)))
	s, _ := RunServerWithConfig(conf1)
	defer s.Shutdown()

	c := natsConnect(t, s.ClientURL())
	defer c.Close()
	// keep using the same connection
	js, err := c.JetStream()
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "disk",
		Storage:  nats.FileStorage,
		Subjects: []string{"disk"},
	})
	require_Error(t, err)
}

func TestJetStreamConsumerNoMsgPayload(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "S"})
	require_NoError(t, err)

	msg := nats.NewMsg("S")
	msg.Header.Set("name", "derek")
	msg.Data = bytes.Repeat([]byte("A"), 128)
	for i := 0; i < 10; i++ {
		msg.Reply = _EMPTY_ // Fixed in Go client but not in embdedded on yet.
		_, err = js.PublishMsgAsync(msg)
		require_NoError(t, err)
	}

	mset, err := s.GlobalAccount().lookupStream("S")
	require_NoError(t, err)

	// Now create our consumer with no payload option.
	_, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: "_d_", Durable: "d22", HeadersOnly: true})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("S", nats.Durable("d22"))
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		if len(m.Data) > 0 {
			t.Fatalf("Expected no payload")
		}
		if ms := m.Header.Get(JSMsgSize); ms != "128" {
			t.Fatalf("Expected a header with msg size, got %q", ms)
		}
	}
}

// Issue #2607
func TestJetStreamPurgeAndFilteredConsumers(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "S", Subjects: []string{"FOO.*"}})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = js.Publish("FOO.adam", []byte("M"))
		require_NoError(t, err)
		_, err = js.Publish("FOO.eve", []byte("F"))
		require_NoError(t, err)
	}

	ci, err := js.AddConsumer("S", &nats.ConsumerConfig{
		Durable:       "adam",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "FOO.adam",
	})
	require_NoError(t, err)
	if ci.NumPending != 10 {
		t.Fatalf("Expected NumPending to be 10, got %d", ci.NumPending)
	}

	ci, err = js.AddConsumer("S", &nats.ConsumerConfig{
		Durable:       "eve",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "FOO.eve",
	})
	require_NoError(t, err)
	if ci.NumPending != 10 {
		t.Fatalf("Expected NumPending to be 10, got %d", ci.NumPending)
	}

	// Now purge only adam.
	jr, _ := json.Marshal(&JSApiStreamPurgeRequest{Subject: "FOO.adam"})
	_, err = nc.Request(fmt.Sprintf(JSApiStreamPurgeT, "S"), jr, time.Second)
	require_NoError(t, err)

	si, err := js.StreamInfo("S")
	require_NoError(t, err)
	if si.State.Msgs != 10 {
		t.Fatalf("Expected 10 messages after purge, got %d", si.State.Msgs)
	}

	ci, err = js.ConsumerInfo("S", "eve")
	require_NoError(t, err)
	if ci.NumPending != 10 {
		t.Fatalf("Expected NumPending to be 10, got %d", ci.NumPending)
	}

	ci, err = js.ConsumerInfo("S", "adam")
	require_NoError(t, err)
	if ci.NumPending != 0 {
		t.Fatalf("Expected NumPending to be 0, got %d", ci.NumPending)
	}
	if ci.AckFloor.Stream != 20 {
		t.Fatalf("Expected AckFloor for stream to be 20, got %d", ci.AckFloor.Stream)
	}
}

// Issue #2662
func TestJetStreamLargeExpiresAndServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	maxAge := 2 * time.Second

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "S",
		Subjects: []string{"foo"},
		MaxAge:   maxAge,
	})
	require_NoError(t, err)

	start := time.Now()
	_, err = js.Publish("foo", []byte("ok"))
	require_NoError(t, err)

	// Wait total of maxAge - 1s.
	time.Sleep(maxAge - time.Since(start) - time.Second)

	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		si, err := js.StreamInfo("S")
		require_NoError(t, err)
		if si.State.Msgs != 0 {
			return fmt.Errorf("Expected no messages, got %d", si.State.Msgs)
		}
		return nil
	})

	if waited := time.Since(start); waited > maxAge+time.Second {
		t.Fatalf("Waited to long %v vs %v for messages to expire", waited, maxAge)
	}
}

// Bug that was reported showing memstore not handling max per subject of 1.
func TestJetStreamMessagePerSubjectKeepBug(t *testing.T) {
	test := func(t *testing.T, keep int64, store nats.StorageType) {
		s := RunBasicJetStreamServer(t)
		defer s.Shutdown()

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:              "TEST",
			MaxMsgsPerSubject: keep,
			Storage:           store,
		})
		require_NoError(t, err)

		for i := 0; i < 100; i++ {
			_, err = js.Publish("TEST", []byte(fmt.Sprintf("test %d", i)))
			require_NoError(t, err)
		}

		nfo, err := js.StreamInfo("TEST")
		require_NoError(t, err)

		if nfo.State.Msgs != uint64(keep) {
			t.Fatalf("Expected %d message got %d", keep, nfo.State.Msgs)
		}
	}

	t.Run("FileStore", func(t *testing.T) {
		t.Run("Keep 10", func(t *testing.T) { test(t, 10, nats.FileStorage) })
		t.Run("Keep 1", func(t *testing.T) { test(t, 1, nats.FileStorage) })
	})

	t.Run("MemStore", func(t *testing.T) {
		t.Run("Keep 10", func(t *testing.T) { test(t, 10, nats.MemoryStorage) })
		t.Run("Keep 1", func(t *testing.T) { test(t, 1, nats.MemoryStorage) })
	})
}

func TestJetStreamInvalidDeliverSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name: "TEST",
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{DeliverSubject: " x"})
	require_Error(t, err, NewJSConsumerInvalidDeliverSubjectError())
}

func TestJetStreamMemoryCorruption(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	errCh := make(chan error, 10)
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) {
		select {
		case errCh <- e:
		default:
		}
	})

	// The storage has to be MemoryStorage to show the issue
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "bucket", Storage: nats.MemoryStorage})
	require_NoError(t, err)

	w1, err := kv.WatchAll()
	require_NoError(t, err)

	w2, err := kv.WatchAll(nats.MetaOnly())
	require_NoError(t, err)

	kv.Put("key1", []byte("aaa"))
	kv.Put("key1", []byte("aab"))
	kv.Put("key2", []byte("zza"))
	kv.Put("key2", []byte("zzb"))
	kv.Delete("key1")
	kv.Delete("key2")
	kv.Put("key1", []byte("aac"))
	kv.Put("key2", []byte("zzc"))
	kv.Delete("key1")
	kv.Delete("key2")
	kv.Purge("key1")
	kv.Purge("key2")

	checkUpdates := func(updates <-chan nats.KeyValueEntry) {
		t.Helper()
		count := 0
		for {
			select {
			case <-updates:
				count++
				if count == 13 {
					return
				}
			case <-time.After(time.Second):
				t.Fatal("Did not receive all updates")
			}
		}
	}
	checkUpdates(w1.Updates())
	checkUpdates(w2.Updates())

	select {
	case e := <-errCh:
		t.Fatal(e)
	case <-time.After(250 * time.Millisecond):
		// OK
	}
}

func TestJetStreamRecoverBadStreamSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()

	f := filepath.Join(sd, "$G", "streams", "TEST")
	fs, err := newFileStore(FileStoreConfig{StoreDir: f}, StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", " baz "}, // baz has spaces
		Storage:  FileStorage,
	})
	require_NoError(t, err)
	fs.Stop()

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	if len(si.Config.Subjects) != 3 {
		t.Fatalf("Expected to recover all subjects")
	}
}

func TestJetStreamRecoverBadMirrorConfigWithSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	sd := s.JetStreamConfig().StoreDir

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "S",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	s.Shutdown()

	f := filepath.Join(sd, "$G", "streams", "M")
	fs, err := newFileStore(FileStoreConfig{StoreDir: f}, StreamConfig{
		Name:     "M",
		Subjects: []string{"foo", "bar", "baz"}, // Mirrors should not have spaces.
		Mirror:   &StreamSource{Name: "S"},
		Storage:  FileStorage,
	})
	require_NoError(t, err)
	fs.Stop()

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo("M")
	require_NoError(t, err)

	if len(si.Config.Subjects) != 0 {
		t.Fatalf("Expected to have NO subjects on mirror")
	}
}

func TestJetStreamCrossAccountsDeliverSubjectInterest(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 4GB, max_file_store: 1TB}
		accounts: {
			A: {
				jetstream: enabled
				users: [ {user: a, password: pwd} ]
				exports [
					{ stream: "_d_" }   # For the delivery subject for the consumer
				]
			},
			B: {
				users: [ {user: b, password: pwd} ]
				imports [
					{ stream: { account: A, subject: "_d_"}, to: "foo" }
				]
			},
		}
	`))

	s, _ := RunServerWithConfig(conf)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s, nats.UserInfo("a", "pwd"))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	msg, toSend := []byte("OK"), 100
	for i := 0; i < toSend; i++ {
		if _, err := js.PublishAsync("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Now create the consumer as well here manually that we will want to reference from Account B.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dlc", DeliverSubject: "_d_"})
	require_NoError(t, err)

	// Wait to see if the stream import signals to deliver messages with no real subscriber interest.
	time.Sleep(200 * time.Millisecond)

	ci, err := js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)

	// Make sure we have not delivered any messages based on the import signal alone.
	if ci.NumPending != uint64(toSend) || ci.Delivered.Consumer != 0 {
		t.Fatalf("Bad consumer info, looks like we started delivering: %+v", ci)
	}

	// Now create interest in the delivery subject through the import on account B.
	nc, _ = jsClientConnect(t, s, nats.UserInfo("b", "pwd"))
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	require_NoError(t, err)
	checkSubsPending(t, sub, toSend)

	ci, err = js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)

	// Make sure our consumer info reflects we delivered the messages.
	if ci.NumPending != 0 || ci.Delivered.Consumer != uint64(toSend) {
		t.Fatalf("Bad consumer info, looks like we did not deliver: %+v", ci)
	}
}

func TestJetStreamPullConsumerRequestCleanup(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "T", Storage: nats.MemoryStorage})
	require_NoError(t, err)

	_, err = js.AddConsumer("T", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	req := &JSApiConsumerGetNextRequest{Batch: 10, Expires: 100 * time.Millisecond}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)

	// Need interest otherwise the requests will be recycled based on that.
	_, err = nc.SubscribeSync("xx")
	require_NoError(t, err)

	// Queue up 100 requests.
	rsubj := fmt.Sprintf(JSApiRequestNextT, "T", "dlc")
	for i := 0; i < 100; i++ {
		err = nc.PublishRequest(rsubj, "xx", jreq)
		require_NoError(t, err)
	}
	// Wait to expire
	time.Sleep(200 * time.Millisecond)

	ci, err := js.ConsumerInfo("T", "dlc")
	require_NoError(t, err)

	if ci.NumWaiting != 0 {
		t.Fatalf("Expected to see no waiting requests, got %d", ci.NumWaiting)
	}
}

func TestJetStreamPullConsumerRequestMaximums(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	// Need to do this via server for now.
	acc := s.GlobalAccount()
	mset, err := acc.addStream(&StreamConfig{Name: "TEST", Storage: MemoryStorage})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:            "dlc",
		MaxRequestBatch:    10,
		MaxRequestMaxBytes: 10_000,
		MaxRequestExpires:  time.Second,
		AckPolicy:          AckExplicit,
	})
	require_NoError(t, err)

	genReq := func(b, mb int, e time.Duration) []byte {
		req := &JSApiConsumerGetNextRequest{Batch: b, Expires: e, MaxBytes: mb}
		jreq, err := json.Marshal(req)
		require_NoError(t, err)
		return jreq
	}

	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", "dlc")

	// Exceeds max batch size.
	resp, err := nc.Request(rsubj, genReq(11, 0, 100*time.Millisecond), time.Second)
	require_NoError(t, err)
	if status := resp.Header.Get("Status"); status != "409" {
		t.Fatalf("Expected a 409 status code, got %q", status)
	}

	// Exceeds max expires.
	resp, err = nc.Request(rsubj, genReq(1, 0, 10*time.Minute), time.Second)
	require_NoError(t, err)
	if status := resp.Header.Get("Status"); status != "409" {
		t.Fatalf("Expected a 409 status code, got %q", status)
	}

	// Exceeds max bytes.
	resp, err = nc.Request(rsubj, genReq(10, 10_000*2, 10*time.Minute), time.Second)
	require_NoError(t, err)
	if status := resp.Header.Get("Status"); status != "409" {
		t.Fatalf("Expected a 409 status code, got %q", status)
	}
}

func TestJetStreamEphemeralPullConsumers(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "EC", Storage: nats.MemoryStorage})
	require_NoError(t, err)

	ci, err := js.AddConsumer("EC", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	mset, err := s.GlobalAccount().lookupStream("EC")
	require_NoError(t, err)
	o := mset.lookupConsumer(ci.Name)
	if o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	err = o.setInActiveDeleteThreshold(50 * time.Millisecond)
	require_NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	// Should no longer be around.
	if o := mset.lookupConsumer(ci.Name); o != nil {
		t.Fatalf("Expected consumer to be closed and removed")
	}

	// Make sure timer keeps firing etc. and does not delete until interest is gone.
	ci, err = js.AddConsumer("EC", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	if o = mset.lookupConsumer(ci.Name); o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	err = o.setInActiveDeleteThreshold(50 * time.Millisecond)
	require_NoError(t, err)

	// Need interest otherwise the requests will be recycled based on no real interest.
	sub, err := nc.SubscribeSync("xx")
	require_NoError(t, err)

	req := &JSApiConsumerGetNextRequest{Batch: 10, Expires: 250 * time.Millisecond}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)
	rsubj := fmt.Sprintf(JSApiRequestNextT, "EC", ci.Name)
	err = nc.PublishRequest(rsubj, "xx", jreq)
	require_NoError(t, err)
	nc.Flush()

	time.Sleep(100 * time.Millisecond)
	// Should still be alive here.
	if o := mset.lookupConsumer(ci.Name); o == nil {
		t.Fatalf("Expected consumer to still be active")
	}
	// Remove interest.
	sub.Unsubscribe()
	// Make sure this EPC goes away now.
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if o := mset.lookupConsumer(ci.Name); o != nil {
			return fmt.Errorf("Consumer still present")
		}
		return nil
	})
}

func TestJetStreamEphemeralPullConsumersInactiveThresholdAndNoWait(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "ECIT", Storage: nats.MemoryStorage})
	require_NoError(t, err)

	ci, err := js.AddConsumer("ECIT", &nats.ConsumerConfig{
		AckPolicy:         nats.AckExplicitPolicy,
		InactiveThreshold: 100 * time.Millisecond,
	})
	require_NoError(t, err)

	// Send 10 no_wait requests every 25ms and consumer should still be present.
	req := &JSApiConsumerGetNextRequest{Batch: 10, NoWait: true}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)
	rsubj := fmt.Sprintf(JSApiRequestNextT, "ECIT", ci.Name)
	for i := 0; i < 10; i++ {
		err = nc.PublishRequest(rsubj, "xx", jreq)
		require_NoError(t, err)
		nc.Flush()
		time.Sleep(25 * time.Millisecond)
	}

	_, err = js.ConsumerInfo("ECIT", ci.Name)
	require_NoError(t, err)
}

func TestJetStreamPullConsumerCrossAccountExpires(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 4GB, max_file_store: 1TB}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [ { service: "$JS.API.CONSUMER.MSG.NEXT.>", response: stream } ]
			},
			IU: {
				users: [ {user: mh, password: bar} ]
				imports [ { service: { subject: "$JS.API.CONSUMER.MSG.NEXT.*.*", account: JS } }]
				# Re-export for dasiy chain test.
				exports [ { service: "$JS.API.CONSUMER.MSG.NEXT.>", response: stream } ]
			},
			IU2: {
				users: [ {user: ik, password: bar} ]
				imports [ { service: { subject: "$JS.API.CONSUMER.MSG.NEXT.*.*", account: IU } } ]
			},
		}
	`))

	s, _ := RunServerWithConfig(conf)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	// Connect to JS account and create stream, put some messages into it.
	nc, js := jsClientConnect(t, s, nats.UserInfo("dlc", "foo"))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "PC", Subjects: []string{"foo"}})
	require_NoError(t, err)

	toSend := 50
	for i := 0; i < toSend; i++ {
		_, err := js.Publish("foo", []byte("OK"))
		require_NoError(t, err)
	}

	// Now create pull consumer.
	_, err = js.AddConsumer("PC", &nats.ConsumerConfig{Durable: "PC", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// Now access from the importing account.
	nc2, _ := jsClientConnect(t, s, nats.UserInfo("mh", "bar"))
	defer nc2.Close()

	// Make sure batch request works properly with stream response.
	req := &JSApiConsumerGetNextRequest{Batch: 10}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)
	rsubj := fmt.Sprintf(JSApiRequestNextT, "PC", "PC")
	// Make sure we can get a batch correctly etc.
	// This requires response stream above in the export definition.
	sub, err := nc2.SubscribeSync("xx")
	require_NoError(t, err)
	err = nc2.PublishRequest(rsubj, "xx", jreq)
	require_NoError(t, err)
	checkSubsPending(t, sub, 10)

	// Now let's queue up a bunch of requests and then delete interest to make sure the system
	// removes those requests.

	// Purge stream
	err = js.PurgeStream("PC")
	require_NoError(t, err)

	// Queue up 10 requests
	for i := 0; i < 10; i++ {
		err = nc2.PublishRequest(rsubj, "xx", jreq)
		require_NoError(t, err)
	}
	// Since using different connection, flush to make sure processed.
	nc2.Flush()

	ci, err := js.ConsumerInfo("PC", "PC")
	require_NoError(t, err)
	if ci.NumWaiting != 10 {
		t.Fatalf("Expected to see 10 waiting requests, got %d", ci.NumWaiting)
	}

	// Now remove interest and make sure requests are removed.
	sub.Unsubscribe()
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("PC", "PC")
		require_NoError(t, err)
		if ci.NumWaiting != 0 {
			return fmt.Errorf("Requests still present")
		}
		return nil
	})

	// Now let's test that ephemerals will go away as well when interest etc is no longer around.
	ci, err = js.AddConsumer("PC", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// Set the inactivity threshold by hand for now.
	jsacc, err := s.LookupAccount("JS")
	require_NoError(t, err)
	mset, err := jsacc.lookupStream("PC")
	require_NoError(t, err)
	o := mset.lookupConsumer(ci.Name)
	if o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	err = o.setInActiveDeleteThreshold(50 * time.Millisecond)
	require_NoError(t, err)

	rsubj = fmt.Sprintf(JSApiRequestNextT, "PC", ci.Name)
	sub, err = nc2.SubscribeSync("zz")
	require_NoError(t, err)
	err = nc2.PublishRequest(rsubj, "zz", jreq)
	require_NoError(t, err)

	// Wait past inactive threshold.
	time.Sleep(100 * time.Millisecond)
	// Make sure it is still there..
	ci, err = js.ConsumerInfo("PC", ci.Name)
	require_NoError(t, err)
	if ci.NumWaiting != 1 {
		t.Fatalf("Expected to see 1 waiting request, got %d", ci.NumWaiting)
	}

	// Now release interest.
	sub.Unsubscribe()
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		_, err := js.ConsumerInfo("PC", ci.Name)
		if err == nil {
			return fmt.Errorf("Consumer still present")
		}
		return nil
	})

	// Now test daisy chained.
	toSend = 10
	for i := 0; i < toSend; i++ {
		_, err := js.Publish("foo", []byte("OK"))
		require_NoError(t, err)
	}

	ci, err = js.AddConsumer("PC", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// Set the inactivity threshold by hand for now.
	o = mset.lookupConsumer(ci.Name)
	if o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	// Make this one longer so we test request purge and ephemerals in same test.
	err = o.setInActiveDeleteThreshold(500 * time.Millisecond)
	require_NoError(t, err)

	// Now access from the importing account.
	nc3, _ := jsClientConnect(t, s, nats.UserInfo("ik", "bar"))
	defer nc3.Close()

	sub, err = nc3.SubscribeSync("yy")
	require_NoError(t, err)

	rsubj = fmt.Sprintf(JSApiRequestNextT, "PC", ci.Name)
	err = nc3.PublishRequest(rsubj, "yy", jreq)
	require_NoError(t, err)
	checkSubsPending(t, sub, 10)

	// Purge stream
	err = js.PurgeStream("PC")
	require_NoError(t, err)

	// Queue up 10 requests
	for i := 0; i < 10; i++ {
		err = nc3.PublishRequest(rsubj, "yy", jreq)
		require_NoError(t, err)
	}
	// Since using different connection, flush to make sure processed.
	nc3.Flush()

	ci, err = js.ConsumerInfo("PC", ci.Name)
	require_NoError(t, err)
	if ci.NumWaiting != 10 {
		t.Fatalf("Expected to see 10 waiting requests, got %d", ci.NumWaiting)
	}

	// Now remove interest and make sure requests are removed.
	sub.Unsubscribe()
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("PC", ci.Name)
		require_NoError(t, err)
		if ci.NumWaiting != 0 {
			return fmt.Errorf("Requests still present")
		}
		return nil
	})
	// Now make sure the ephemeral goes away too.
	// Ephemerals have jitter by default of up to 1s.
	checkFor(t, 6*time.Second, 10*time.Millisecond, func() error {
		_, err := js.ConsumerInfo("PC", ci.Name)
		if err == nil {
			return fmt.Errorf("Consumer still present")
		}
		return nil
	})
}

func TestJetStreamPullConsumerCrossAccountExpiresNoDataRace(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 4GB, max_file_store: 1TB}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [ { service: "$JS.API.CONSUMER.MSG.NEXT.>", response: stream } ]
			},
			IU: {
				jetstream: enabled
				users: [ {user: ik, password: bar} ]
				imports [ { service: { subject: "$JS.API.CONSUMER.MSG.NEXT.*.*", account: JS } }]
			},
		}
	`))

	test := func() {
		s, _ := RunServerWithConfig(conf)
		if config := s.JetStreamConfig(); config != nil {
			defer removeDir(t, config.StoreDir)
		}
		defer s.Shutdown()

		// Connect to JS account and create stream, put some messages into it.
		nc, js := jsClientConnect(t, s, nats.UserInfo("dlc", "foo"))
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{Name: "PC", Subjects: []string{"foo"}})
		require_NoError(t, err)

		toSend := 100
		for i := 0; i < toSend; i++ {
			_, err := js.Publish("foo", []byte("OK"))
			require_NoError(t, err)
		}

		// Create pull consumer.
		_, err = js.AddConsumer("PC", &nats.ConsumerConfig{Durable: "PC", AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)

		// Now access from the importing account.
		nc2, _ := jsClientConnect(t, s, nats.UserInfo("ik", "bar"))
		defer nc2.Close()

		req := &JSApiConsumerGetNextRequest{Batch: 1}
		jreq, err := json.Marshal(req)
		require_NoError(t, err)
		rsubj := fmt.Sprintf(JSApiRequestNextT, "PC", "PC")
		sub, err := nc2.SubscribeSync("xx")
		require_NoError(t, err)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			time.Sleep(5 * time.Millisecond)
			sub.Unsubscribe()
			wg.Done()
		}()
		for i := 0; i < toSend; i++ {
			nc2.PublishRequest(rsubj, "xx", jreq)
		}
		wg.Wait()
	}
	// Need to rerun this test several times to get the race (which then would possible be panic
	// such as: "fatal error: concurrent map read and map write"
	for iter := 0; iter < 10; iter++ {
		test()
	}
}

// This tests account export/import replies across a LN connection with account import/export
// on both sides of the LN.
func TestJetStreamPullConsumerCrossAccountsAndLeafNodes(t *testing.T) {
	conf := createConfFile(t, []byte(`
		server_name: SJS
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 4GB, max_file_store: 1TB, domain: JSD }
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [ { service: "$JS.API.CONSUMER.MSG.NEXT.>", response: stream } ]
			},
			IU: {
				users: [ {user: mh, password: bar} ]
				imports [ { service: { subject: "$JS.API.CONSUMER.MSG.NEXT.*.*", account: JS } }]
			},
		}
		leaf { listen: "127.0.0.1:-1" }
	`))

	s, o := RunServerWithConfig(conf)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: SLN
		listen: 127.0.0.1:-1
		accounts: {
			A: {
				users: [ {user: l, password: p} ]
				exports [ { service: "$JS.JSD.API.CONSUMER.MSG.NEXT.>", response: stream } ]
			},
			B: {
				users: [ {user: m, password: p} ]
				imports [ { service: { subject: "$JS.JSD.API.CONSUMER.MSG.NEXT.*.*", account: A } }]
			},
		}
		# bind local A to IU account on other side of LN.
		leaf { remotes [ { url: nats://mh:bar@127.0.0.1:%d; account: A } ] }
	`, o.LeafNode.Port)))

	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnectedCount(t, s, 1)

	// Connect to JS account, create stream and consumer and put in some messages.
	nc, js := jsClientConnect(t, s, nats.UserInfo("dlc", "foo"))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "PC", Subjects: []string{"foo"}})
	require_NoError(t, err)

	toSend := 10
	for i := 0; i < toSend; i++ {
		_, err := js.Publish("foo", []byte("OK"))
		require_NoError(t, err)
	}

	// Now create durable pull consumer.
	_, err = js.AddConsumer("PC", &nats.ConsumerConfig{Durable: "PC", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// Now access from the account on the leafnode, so importing on both sides and crossing a leafnode connection.
	nc2, _ := jsClientConnect(t, s2, nats.UserInfo("m", "p"))
	defer nc2.Close()

	req := &JSApiConsumerGetNextRequest{Batch: toSend, Expires: 500 * time.Millisecond}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)

	// Make sure we can get a batch correctly etc.
	// This requires response stream above in the export definition.
	sub, err := nc2.SubscribeSync("xx")
	require_NoError(t, err)

	rsubj := "$JS.JSD.API.CONSUMER.MSG.NEXT.PC.PC"
	err = nc2.PublishRequest(rsubj, "xx", jreq)
	require_NoError(t, err)
	checkSubsPending(t, sub, 10)

	// Queue up a bunch of requests.
	for i := 0; i < 10; i++ {
		err = nc2.PublishRequest(rsubj, "xx", jreq)
		require_NoError(t, err)
	}
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("PC", "PC")
		require_NoError(t, err)
		if ci.NumWaiting != 10 {
			return fmt.Errorf("Expected to see 10 waiting requests, got %d", ci.NumWaiting)
		}
		return nil
	})

	// Remove interest.
	sub.Unsubscribe()
	// Make sure requests go away eventually after they expire.
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("PC", "PC")
		require_NoError(t, err)
		if ci.NumWaiting != 0 {
			return fmt.Errorf("Expected to see no waiting requests, got %d", ci.NumWaiting)
		}
		return nil
	})
}

// This test is to explicitly test for all combinations of pull consumer behavior.
//  1. Long poll, will be used to emulate push. A request is only invalidated when batch is filled, it expires, or we lose interest.
//  2. Batch 1, will return no messages or a message. Works today.
//  3. Conditional wait, or one shot. This is what the clients do when the do a fetch().
//     They expect to wait up to a given time for any messages but will return once they have any to deliver, so parital fills.
//  4. Try, which never waits at all ever.
func TestJetStreamPullConsumersOneShotBehavior(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "dlc",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "foo",
	})
	require_NoError(t, err)

	// We will do low level requests by hand for this test as to not depend on any client impl.
	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", "dlc")

	getNext := func(batch int, expires time.Duration, noWait bool) (numMsgs int, elapsed time.Duration, hdr *nats.Header) {
		t.Helper()
		req := &JSApiConsumerGetNextRequest{Batch: batch, Expires: expires, NoWait: noWait}
		jreq, err := json.Marshal(req)
		require_NoError(t, err)
		// Create listener.
		reply, msgs := nats.NewInbox(), make(chan *nats.Msg, batch)
		sub, err := nc.ChanSubscribe(reply, msgs)
		require_NoError(t, err)
		defer sub.Unsubscribe()

		// Send request.
		start := time.Now()
		err = nc.PublishRequest(rsubj, reply, jreq)
		require_NoError(t, err)

		for {
			select {
			case m := <-msgs:
				if len(m.Data) == 0 && m.Header != nil {
					return numMsgs, time.Since(start), &m.Header
				}
				numMsgs++
				if numMsgs >= batch {
					return numMsgs, time.Since(start), nil
				}
			case <-time.After(expires + 250*time.Millisecond):
				t.Fatalf("Did not receive all the msgs in time")
			}
		}
	}

	expect := func(batch int, expires time.Duration, noWait bool, ne int, he *nats.Header, lt time.Duration, gt time.Duration) {
		t.Helper()
		n, e, h := getNext(batch, expires, noWait)
		if n != ne {
			t.Fatalf("Expected %d msgs, got %d", ne, n)
		}
		if !reflect.DeepEqual(h, he) {
			t.Fatalf("Expected %+v hdr, got %+v", he, h)
		}
		if lt > 0 && e > lt {
			t.Fatalf("Expected elapsed of %v to be less than %v", e, lt)
		}
		if gt > 0 && e < gt {
			t.Fatalf("Expected elapsed of %v to be greater than %v", e, gt)
		}
	}
	expectAfter := func(batch int, expires time.Duration, noWait bool, ne int, he *nats.Header, gt time.Duration) {
		t.Helper()
		expect(batch, expires, noWait, ne, he, 0, gt)
	}
	expectInstant := func(batch int, expires time.Duration, noWait bool, ne int, he *nats.Header) {
		t.Helper()
		expect(batch, expires, noWait, ne, he, 5*time.Millisecond, 0)
	}
	expectOK := func(batch int, expires time.Duration, noWait bool, ne int) {
		t.Helper()
		expectInstant(batch, expires, noWait, ne, nil)
	}

	noMsgs := &nats.Header{"Status": []string{"404"}, "Description": []string{"No Messages"}}
	reqTimeout := &nats.Header{"Status": []string{"408"}, "Description": []string{"Request Timeout"}, "Nats-Pending-Bytes": []string{"0"}, "Nats-Pending-Messages": []string{"1"}}

	// We are empty here, meaning no messages available.
	// Do not wait, should get noMsgs.
	expectInstant(1, 0, true, 0, noMsgs)
	// We should wait here for the full second.
	expectAfter(1, 250*time.Millisecond, false, 0, reqTimeout, 250*time.Millisecond)
	// This should also wait since no messages are available. This is the one shot scenario, or wait for at least a message if none are there.
	expectAfter(1, 500*time.Millisecond, true, 0, reqTimeout, 500*time.Millisecond)

	// Now let's put some messages into the system.
	for i := 0; i < 20; i++ {
		_, err := js.Publish("foo", []byte("HELLO"))
		require_NoError(t, err)
	}

	// Now run same 3 scenarios.
	expectOK(1, 0, true, 1)
	expectOK(5, 500*time.Millisecond, false, 5)
	expectOK(5, 500*time.Millisecond, true, 5)
}

func TestJetStreamPullConsumersMultipleRequestsExpireOutOfOrder(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "dlc",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "foo",
	})
	require_NoError(t, err)

	// We will now queue up 4 requests. All should expire but they will do so out of order.
	// We want to make sure we get them in correct order.
	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", "dlc")
	sub, err := nc.SubscribeSync("i.*")
	require_NoError(t, err)
	defer sub.Unsubscribe()

	for _, expires := range []time.Duration{200, 100, 25, 75} {
		reply := fmt.Sprintf("i.%d", expires)
		req := &JSApiConsumerGetNextRequest{Expires: expires * time.Millisecond}
		jreq, err := json.Marshal(req)
		require_NoError(t, err)
		err = nc.PublishRequest(rsubj, reply, jreq)
		require_NoError(t, err)
	}
	start := time.Now()
	checkSubsPending(t, sub, 4)
	elapsed := time.Since(start)

	if elapsed < 200*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("Expected elapsed to be close to %v, but got %v", 200*time.Millisecond, elapsed)
	}

	var rs []string
	for i := 0; i < 4; i++ {
		m, err := sub.NextMsg(0)
		require_NoError(t, err)
		rs = append(rs, m.Subject)
	}
	if expected := []string{"i.25", "i.75", "i.100", "i.200"}; !reflect.DeepEqual(rs, expected) {
		t.Fatalf("Received in wrong order, wanted %+v, got %+v", expected, rs)
	}
}

func TestJetStreamConsumerUpdateSurvival(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "X"})
	require_NoError(t, err)

	// First create a consumer with max ack pending.
	_, err = js.AddConsumer("X", &nats.ConsumerConfig{
		Durable:       "dlc",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxAckPending: 1024,
	})
	require_NoError(t, err)

	// Now do same name but pull. This will update the MaxAcKPending
	ci, err := js.UpdateConsumer("X", &nats.ConsumerConfig{
		Durable:       "dlc",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxAckPending: 22,
	})
	require_NoError(t, err)

	if ci.Config.MaxAckPending != 22 {
		t.Fatalf("Expected MaxAckPending to be 22, got %d", ci.Config.MaxAckPending)
	}

	// Make sure this survives across a restart.
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	ci, err = js.ConsumerInfo("X", "dlc")
	require_NoError(t, err)

	if ci.Config.MaxAckPending != 22 {
		t.Fatalf("Expected MaxAckPending to be 22, got %d", ci.Config.MaxAckPending)
	}
}

func TestJetStreamNakRedeliveryWithNoWait(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", []byte("NAK"))
	require_NoError(t, err)

	ccReq := &CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			Durable:    "dlc",
			AckPolicy:  AckExplicit,
			MaxDeliver: 3,
			AckWait:    time.Minute,
			BackOff:    []time.Duration{5 * time.Second, 10 * time.Second},
		},
	}
	// Do by hand for now until Go client catches up.
	req, err := json.Marshal(ccReq)
	require_NoError(t, err)
	resp, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", "dlc"), req, time.Second)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	if ccResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", ccResp.Error)
	}

	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", "dlc")
	m, err := nc.Request(rsubj, nil, time.Second)
	require_NoError(t, err)

	// NAK this message.
	delay, err := json.Marshal(&ConsumerNakOptions{Delay: 500 * time.Millisecond})
	require_NoError(t, err)
	dnak := []byte(fmt.Sprintf("%s  %s", AckNak, delay))
	m.Respond(dnak)

	// This message should come back to us after 500ms. If we do a one-shot request, with NoWait and Expires
	// this will do the right thing and we get the message.
	// What we want to test here is a true NoWait request with Expires==0 and eventually seeing the message be redelivered.
	expires := time.Now().Add(time.Second)
	for time.Now().Before(expires) {
		m, err = nc.Request(rsubj, []byte(`{"batch":1, "no_wait": true}`), time.Second)
		require_NoError(t, err)
		if len(m.Data) > 0 {
			// We got our message, so we are good.
			return
		}
		// So we do not spin.
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("Did not get the message in time")
}

// Test that we properly enforce per subject msg limits when DiscardNew is set.
// DiscardNew should only apply to stream limits, subject based limits should always be DiscardOld.
func TestJetStreamMaxMsgsPerSubjectWithDiscardNew(t *testing.T) {
	msc := StreamConfig{
		Name:       "TEST",
		Subjects:   []string{"foo", "bar", "baz", "x"},
		Discard:    DiscardNew,
		Storage:    MemoryStorage,
		MaxMsgsPer: 4,
		MaxMsgs:    10,
		MaxBytes:   500,
	}
	fsc := msc
	fsc.Storage = FileStorage

	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &msc},
		{"FileStore", &fsc},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			require_NoError(t, err)
			defer mset.delete()

			// Client for API requests.
			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			pubAndCheck := func(subj string, num int, expectedNumMsgs uint64) {
				t.Helper()
				for i := 0; i < num; i++ {
					_, err = js.Publish(subj, []byte("TSLA"))
					require_NoError(t, err)
				}
				si, err := js.StreamInfo("TEST")
				require_NoError(t, err)
				if si.State.Msgs != expectedNumMsgs {
					t.Fatalf("Expected %d msgs, got %d", expectedNumMsgs, si.State.Msgs)
				}
			}

			pubExpectErr := func(subj string, sz int) {
				t.Helper()
				_, err = js.Publish(subj, bytes.Repeat([]byte("X"), sz))
				require_Error(t, err, errors.New("nats: maximum bytes exceeded"), errors.New("nats: maximum messages exceeded"))
			}

			pubAndCheck("foo", 1, 1)
			// We should treat this as DiscardOld and only have 4 msgs after.
			pubAndCheck("foo", 4, 4)
			// Same thing here, shoud only have 4 foo and 4 bar for total of 8.
			pubAndCheck("bar", 8, 8)
			// We have 8 here, so only 2 left. If we add in a new subject when we have room it will be accepted.
			pubAndCheck("baz", 2, 10)
			// Now we are full, but makeup is foo-4 bar-4 baz-2.
			// We can add to foo and bar since they are at their max and adding new ones there stays the same in terms of total of 10.
			pubAndCheck("foo", 1, 10)
			pubAndCheck("bar", 1, 10)
			// Try to send a large message under an established subject that will exceed the 500 maximum.
			// Even though we have a bar subject and its at its maximum, the message to be dropped is not big enough, so this should err.
			pubExpectErr("bar", 300)
			// Also even though we have room bytes wise, if we introduce a new subject this should fail too on msg limit exceeded.
			pubExpectErr("x", 2)
		})
	}
}

func TestJetStreamStreamInfoSubjectsDetails(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	getInfo := func(t *testing.T, filter string) *StreamInfo {
		t.Helper()
		// Need to grab StreamInfo by hand for now.
		req, err := json.Marshal(&JSApiStreamInfoRequest{SubjectsFilter: filter})
		require_NoError(t, err)
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), req, time.Second)
		require_NoError(t, err)
		var si StreamInfo
		err = json.Unmarshal(resp.Data, &si)
		require_NoError(t, err)
		if si.State.NumSubjects != 3 {
			t.Fatalf("Expected NumSubjects to be 3, but got %d", si.State.NumSubjects)
		}
		return &si
	}

	testSubjects := func(t *testing.T, st nats.StorageType) {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"*"},
			Storage:  st,
		})
		require_NoError(t, err)
		defer js.DeleteStream("TEST")

		counts, msg := []int{22, 33, 44}, []byte("ok")
		// Now place msgs, foo-22, bar-33 and baz-44.
		for i, subj := range []string{"foo", "bar", "baz"} {
			for n := 0; n < counts[i]; n++ {
				_, err = js.Publish(subj, msg)
				require_NoError(t, err)
			}
		}

		// Test all subjects first.
		expected := map[string]uint64{"foo": 22, "bar": 33, "baz": 44}
		if si := getInfo(t, nats.AllKeys); !reflect.DeepEqual(si.State.Subjects, expected) {
			t.Fatalf("Expected subjects of %+v, but got %+v", expected, si.State.Subjects)
		}
		if si := getInfo(t, "*"); !reflect.DeepEqual(si.State.Subjects, expected) {
			t.Fatalf("Expected subjects of %+v, but got %+v", expected, si.State.Subjects)
		}
		// Filtered to 1.
		expected = map[string]uint64{"foo": 22}
		if si := getInfo(t, "foo"); !reflect.DeepEqual(si.State.Subjects, expected) {
			t.Fatalf("Expected subjects of %+v, but got %+v", expected, si.State.Subjects)
		}
	}

	t.Run("MemoryStore", func(t *testing.T) { testSubjects(t, nats.MemoryStorage) })
	t.Run("FileStore", func(t *testing.T) { testSubjects(t, nats.FileStorage) })
}

func TestJetStreamStreamInfoSubjectsDetailsWithDeleteAndPurge(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	getInfo := func(t *testing.T, filter string) *StreamInfo {
		t.Helper()
		// Need to grab StreamInfo by hand for now.
		req, err := json.Marshal(&JSApiStreamInfoRequest{SubjectsFilter: filter})
		require_NoError(t, err)
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), req, time.Second)
		require_NoError(t, err)
		var si StreamInfo
		err = json.Unmarshal(resp.Data, &si)
		require_NoError(t, err)
		return &si
	}

	checkResults := func(t *testing.T, expected map[string]uint64) {
		t.Helper()
		si := getInfo(t, nats.AllKeys)
		if !reflect.DeepEqual(si.State.Subjects, expected) {
			t.Fatalf("Expected subjects of %+v, but got %+v", expected, si.State.Subjects)
		}
		if si.State.NumSubjects != len(expected) {
			t.Fatalf("Expected NumSubjects to be %d, but got %d", len(expected), si.State.NumSubjects)
		}
	}

	testSubjects := func(t *testing.T, st nats.StorageType) {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"*"},
			Storage:  st,
		})
		require_NoError(t, err)
		defer js.DeleteStream("TEST")

		msg := []byte("ok")
		js.Publish("foo", msg) // 1
		js.Publish("foo", msg) // 2
		js.Publish("bar", msg) // 3
		js.Publish("baz", msg) // 4
		js.Publish("baz", msg) // 5
		js.Publish("bar", msg) // 6
		js.Publish("bar", msg) // 7

		checkResults(t, map[string]uint64{"foo": 2, "bar": 3, "baz": 2})

		// Now delete some messages.
		js.DeleteMsg("TEST", 6)

		checkResults(t, map[string]uint64{"foo": 2, "bar": 2, "baz": 2})

		// Delete and add right back, so no-op
		js.DeleteMsg("TEST", 5) // baz
		js.Publish("baz", msg)  // 8

		checkResults(t, map[string]uint64{"foo": 2, "bar": 2, "baz": 2})

		// Now do a purge only of bar.
		jr, _ := json.Marshal(&JSApiStreamPurgeRequest{Subject: "bar"})
		_, err = nc.Request(fmt.Sprintf(JSApiStreamPurgeT, "TEST"), jr, time.Second)
		require_NoError(t, err)

		checkResults(t, map[string]uint64{"foo": 2, "baz": 2})

		// Now purge everything
		err = js.PurgeStream("TEST")
		require_NoError(t, err)

		si := getInfo(t, nats.AllKeys)
		if len(si.State.Subjects) != 0 {
			t.Fatalf("Expected no subjects, but got %+v", si.State.Subjects)
		}
		if si.State.NumSubjects != 0 {
			t.Fatalf("Expected NumSubjects to be 0, but got %d", si.State.NumSubjects)
		}
	}

	t.Run("MemoryStore", func(t *testing.T) { testSubjects(t, nats.MemoryStorage) })
	t.Run("FileStore", func(t *testing.T) { testSubjects(t, nats.FileStorage) })
}

func TestJetStreamStreamInfoSubjectsDetailsAfterRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	getInfo := func(t *testing.T, filter string) *StreamInfo {
		t.Helper()
		// Need to grab StreamInfo by hand for now.
		req, err := json.Marshal(&JSApiStreamInfoRequest{SubjectsFilter: filter})
		require_NoError(t, err)
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), req, time.Second)
		require_NoError(t, err)
		var si StreamInfo
		err = json.Unmarshal(resp.Data, &si)
		require_NoError(t, err)
		return &si
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
	})
	require_NoError(t, err)
	defer js.DeleteStream("TEST")

	msg := []byte("ok")
	js.Publish("foo", msg) // 1
	js.Publish("foo", msg) // 2
	js.Publish("bar", msg) // 3
	js.Publish("baz", msg) // 4
	js.Publish("baz", msg) // 5

	si := getInfo(t, nats.AllKeys)
	if si.State.NumSubjects != 3 {
		t.Fatalf("Expected 3 subjects, but got %d", si.State.NumSubjects)
	}

	// Stop current
	nc.Close()
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, _ = jsClientConnect(t, s)
	defer nc.Close()

	si = getInfo(t, nats.AllKeys)
	if si.State.NumSubjects != 3 {
		t.Fatalf("Expected 3 subjects, but got %d", si.State.NumSubjects)
	}
}

// Issue #2836
func TestJetStreamInterestRetentionBug(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.>"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "c1", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	test := func(token string, fseq, msgs uint64) {
		t.Helper()
		subj := fmt.Sprintf("foo.%s", token)
		_, err = js.Publish(subj, nil)
		require_NoError(t, err)
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		if si.State.FirstSeq != fseq {
			t.Fatalf("Expected first to be %d, got %d", fseq, si.State.FirstSeq)
		}
		if si.State.Msgs != msgs {
			t.Fatalf("Expected msgs to be %d, got %d", msgs, si.State.Msgs)
		}
	}

	test("bar", 1, 1)

	// Create second filtered consumer.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "c2", FilterSubject: "foo.foo", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	test("bar", 1, 2)
}

// Under load testing for K3S and the KINE interface we saw some stalls.
// These were caused by a dynamic that would not send a second FC item with one
// pending, but when we sent the next message and got blocked, if that msg would
// exceed the outstanding FC we would become stalled.
func TestJetStreamFlowControlStall(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "FC"})
	require_NoError(t, err)

	msg := []byte(strings.Repeat("X", 32768))
	_, err = js.Publish("FC", msg)
	require_NoError(t, err)

	msg = []byte(strings.Repeat("X", 8*32768))
	_, err = js.Publish("FC", msg)
	require_NoError(t, err)
	_, err = js.Publish("FC", msg)
	require_NoError(t, err)

	sub, err := js.SubscribeSync("FC", nats.OrderedConsumer())
	require_NoError(t, err)

	checkSubsPending(t, sub, 3)
}

func TestJetStreamConsumerPendingCountWithRedeliveries(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:    "test",
		AckPolicy:  nats.AckExplicitPolicy,
		AckWait:    50 * time.Millisecond,
		MaxDeliver: 1,
	})
	require_NoError(t, err)

	// Publish 1st message
	_, err = js.Publish("foo", []byte("msg1"))
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "test")
	require_NoError(t, err)
	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	for _, m := range msgs {
		require_Equal(t, string(m.Data), "msg1")
		// Do not ack the message
	}
	// Check consumer info, pending should be 0
	ci, err := js.ConsumerInfo("TEST", "test")
	require_NoError(t, err)
	if ci.NumPending != 0 {
		t.Fatalf("Expected consumer info pending count to be 0, got %v", ci.NumPending)
	}

	// Wait for more than expiration
	time.Sleep(100 * time.Millisecond)

	// Publish 2nd message
	_, err = js.Publish("foo", []byte("msg2"))
	require_NoError(t, err)

	msgs, err = sub.Fetch(1)
	require_NoError(t, err)
	for _, m := range msgs {
		require_Equal(t, string(m.Data), "msg2")
		// Its deliver count should be 1
		meta, err := m.Metadata()
		require_NoError(t, err)
		if meta.NumDelivered != 1 {
			t.Fatalf("Expected message's deliver count to be 1, got %v", meta.NumDelivered)
		}
	}
	// Check consumer info, pending should be 0
	ci, err = js.ConsumerInfo("TEST", "test")
	require_NoError(t, err)
	if ci.NumPending != 0 {
		t.Fatalf("Expected consumer info pending count to be 0, got %v", ci.NumPending)
	}
}

func TestJetStreamPullConsumerHeartBeats(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "T", Storage: nats.MemoryStorage})
	require_NoError(t, err)

	_, err = js.AddConsumer("T", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	rsubj := fmt.Sprintf(JSApiRequestNextT, "T", "dlc")

	type tsMsg struct {
		received time.Time
		msg      *nats.Msg
	}

	doReq := func(batch int, hb, expires time.Duration, expected int) []*tsMsg {
		t.Helper()
		req := &JSApiConsumerGetNextRequest{Batch: batch, Expires: expires, Heartbeat: hb}
		jreq, err := json.Marshal(req)
		require_NoError(t, err)
		reply := nats.NewInbox()
		var msgs []*tsMsg
		var mu sync.Mutex

		sub, err := nc.Subscribe(reply, func(m *nats.Msg) {
			mu.Lock()
			msgs = append(msgs, &tsMsg{time.Now(), m})
			mu.Unlock()
		})
		require_NoError(t, err)

		err = nc.PublishRequest(rsubj, reply, jreq)
		require_NoError(t, err)
		checkFor(t, time.Second, 50*time.Millisecond, func() error {
			mu.Lock()
			nr := len(msgs)
			mu.Unlock()
			if nr >= expected {
				return nil
			}
			return fmt.Errorf("Only have seen %d of %d responses", nr, expected)
		})
		sub.Unsubscribe()
		return msgs
	}

	reqBad := nats.Header{"Status": []string{"400"}, "Description": []string{"Bad Request - heartbeat value too large"}}
	expectErr := func(msgs []*tsMsg) {
		t.Helper()
		if len(msgs) != 1 {
			t.Fatalf("Expected 1 msg, got %d", len(msgs))
		}
		if !reflect.DeepEqual(msgs[0].msg.Header, reqBad) {
			t.Fatalf("Expected %+v hdr, got %+v", reqBad, msgs[0].msg.Header)
		}
	}

	// Test errors first.
	// Setting HB with no expires.
	expectErr(doReq(1, 100*time.Millisecond, 0, 1))
	// If HB larger than 50% of expires..
	expectErr(doReq(1, 75*time.Millisecond, 100*time.Millisecond, 1))

	expectHBs := func(start time.Time, msgs []*tsMsg, expected int, hbi time.Duration) {
		t.Helper()
		if len(msgs) != expected {
			t.Fatalf("Expected %d but got %d", expected, len(msgs))
		}
		// expected -1 should be all HBs.
		for i, ts := 0, start; i < expected-1; i++ {
			tr, m := msgs[i].received, msgs[i].msg
			if m.Header.Get("Status") != "100" {
				t.Fatalf("Expected a 100 status code, got %q", m.Header.Get("Status"))
			}
			if m.Header.Get("Description") != "Idle Heartbeat" {
				t.Fatalf("Wrong description, got %q", m.Header.Get("Description"))
			}
			ts = ts.Add(hbi)
			if tr.Before(ts) {
				t.Fatalf("Received at wrong time: %v vs %v", tr, ts)
			}
		}
		// Last msg should be timeout.
		lm := msgs[len(msgs)-1].msg
		if key := lm.Header.Get("Status"); key != "408" {
			t.Fatalf("Expected 408 Request Timeout, got %s", key)
		}
	}

	// These should work. Test idle first.
	start, msgs := time.Now(), doReq(1, 50*time.Millisecond, 250*time.Millisecond, 5)
	expectHBs(start, msgs, 5, 50*time.Millisecond)

	// Now test that we do not send heartbeats while we receive traffic.
	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(50 * time.Millisecond)
			js.Publish("T", nil)
		}
	}()

	msgs = doReq(10, 75*time.Millisecond, 350*time.Millisecond, 6)
	// The first 5 should be msgs, no HBs.
	for i := 0; i < 5; i++ {
		if m := msgs[i].msg; len(m.Header) > 0 {
			t.Fatalf("Got a potential heartbeat msg when we should not have: %+v", m.Header)
		}
	}
	// Last should be timeout.
	lm := msgs[len(msgs)-1].msg
	if key := lm.Header.Get("Status"); key != "408" {
		t.Fatalf("Expected 408 Request Timeout, got %s", key)
	}
}

func TestJetStreamStorageReservedBytes(t *testing.T) {
	const systemLimit = 1024
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.JetStreamMaxMemory = systemLimit
	opts.JetStreamMaxStore = systemLimit
	opts.StoreDir = t.TempDir()
	opts.HTTPPort = -1
	s := RunServer(&opts)

	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	getJetStreamVarz := func(hc *http.Client, addr string) (JetStreamVarz, error) {
		resp, err := hc.Get(addr)
		if err != nil {
			return JetStreamVarz{}, err
		}
		defer resp.Body.Close()

		var v Varz
		if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
			return JetStreamVarz{}, err
		}

		return v.JetStream, nil
	}
	getReserved := func(hc *http.Client, addr string, st nats.StorageType) (uint64, error) {
		jsv, err := getJetStreamVarz(hc, addr)
		if err != nil {
			return 0, err
		}
		if st == nats.MemoryStorage {
			return jsv.Stats.ReservedMemory, nil
		}
		return jsv.Stats.ReservedStore, nil
	}

	varzAddr := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	hc := &http.Client{Timeout: 5 * time.Second}

	jsv, err := getJetStreamVarz(hc, varzAddr)
	require_NoError(t, err)

	if got, want := systemLimit, int(jsv.Config.MaxMemory); got != want {
		t.Fatalf("Unexpected max memory: got=%d, want=%d", got, want)
	}
	if got, want := systemLimit, int(jsv.Config.MaxStore); got != want {
		t.Fatalf("Unexpected max store: got=%d, want=%d", got, want)
	}

	cases := []struct {
		name            string
		accountLimit    int64
		storage         nats.StorageType
		createMaxBytes  int64
		updateMaxBytes  int64
		wantUpdateError bool
	}{
		{
			name:           "file reserve 66% of system limit",
			accountLimit:   -1,
			storage:        nats.FileStorage,
			createMaxBytes: int64(math.Round(float64(systemLimit) * .666)),
			updateMaxBytes: int64(math.Round(float64(systemLimit)*.666)) + 1,
		},
		{
			name:           "memory reserve 66% of system limit",
			accountLimit:   -1,
			storage:        nats.MemoryStorage,
			createMaxBytes: int64(math.Round(float64(systemLimit) * .666)),
			updateMaxBytes: int64(math.Round(float64(systemLimit)*.666)) + 1,
		},
		{
			name:            "file update past system limit",
			accountLimit:    -1,
			storage:         nats.FileStorage,
			createMaxBytes:  systemLimit,
			updateMaxBytes:  systemLimit + 1,
			wantUpdateError: true,
		},
		{
			name:            "memory update past system limit",
			accountLimit:    -1,
			storage:         nats.MemoryStorage,
			createMaxBytes:  systemLimit,
			updateMaxBytes:  systemLimit + 1,
			wantUpdateError: true,
		},
		{
			name:           "file update to system limit",
			accountLimit:   -1,
			storage:        nats.FileStorage,
			createMaxBytes: systemLimit - 1,
			updateMaxBytes: systemLimit,
		},
		{
			name:           "memory update to system limit",
			accountLimit:   -1,
			storage:        nats.MemoryStorage,
			createMaxBytes: systemLimit - 1,
			updateMaxBytes: systemLimit,
		},
		{
			name:           "file reserve 66% of account limit",
			accountLimit:   systemLimit / 2,
			storage:        nats.FileStorage,
			createMaxBytes: int64(math.Round(float64(systemLimit/2) * .666)),
			updateMaxBytes: int64(math.Round(float64(systemLimit/2)*.666)) + 1,
		},
		{
			name:           "memory reserve 66% of account limit",
			accountLimit:   systemLimit / 2,
			storage:        nats.MemoryStorage,
			createMaxBytes: int64(math.Round(float64(systemLimit/2) * .666)),
			updateMaxBytes: int64(math.Round(float64(systemLimit/2)*.666)) + 1,
		},
		{
			name:            "file update past account limit",
			accountLimit:    systemLimit / 2,
			storage:         nats.FileStorage,
			createMaxBytes:  (systemLimit / 2),
			updateMaxBytes:  (systemLimit / 2) + 1,
			wantUpdateError: true,
		},
		{
			name:            "memory update past account limit",
			accountLimit:    systemLimit / 2,
			storage:         nats.MemoryStorage,
			createMaxBytes:  (systemLimit / 2),
			updateMaxBytes:  (systemLimit / 2) + 1,
			wantUpdateError: true,
		},
		{
			name:           "file update to account limit",
			accountLimit:   systemLimit / 2,
			storage:        nats.FileStorage,
			createMaxBytes: (systemLimit / 2) - 1,
			updateMaxBytes: (systemLimit / 2),
		},
		{
			name:           "memory update to account limit",
			accountLimit:   systemLimit / 2,
			storage:        nats.MemoryStorage,
			createMaxBytes: (systemLimit / 2) - 1,
			updateMaxBytes: (systemLimit / 2),
		},
	}
	for i := 0; i < len(cases) && !t.Failed(); i++ {
		c := cases[i]
		t.Run(c.name, func(st *testing.T) {
			// Setup limits
			err = s.GlobalAccount().UpdateJetStreamLimits(map[string]JetStreamAccountLimits{
				_EMPTY_: {
					MaxMemory: c.accountLimit,
					MaxStore:  c.accountLimit,
				},
			})
			require_NoError(st, err)

			// Create initial stream
			cfg := &nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Storage:  c.storage,
				MaxBytes: c.createMaxBytes,
			}
			_, err = js.AddStream(cfg)
			require_NoError(st, err)

			// Update stream MaxBytes
			cfg.MaxBytes = c.updateMaxBytes
			info, err := js.UpdateStream(cfg)
			if c.wantUpdateError && err == nil {
				got := info.Config.MaxBytes
				st.Fatalf("Unexpected update success, newMaxBytes=%d; systemLimit=%d; accountLimit=%d",
					got, systemLimit, c.accountLimit)
			} else if !c.wantUpdateError && err != nil {
				st.Fatalf("Unexpected update error: %s", err)
			}

			if !c.wantUpdateError && err == nil {
				// If update was successful, then ensure reserved shows new
				// amount
				reserved, err := getReserved(hc, varzAddr, c.storage)
				require_NoError(st, err)
				if got, want := reserved, uint64(c.updateMaxBytes); got != want {
					st.Fatalf("Unexpected reserved: %d, want %d", got, want)
				}
			}

			// Delete stream
			err = js.DeleteStream("TEST")
			require_NoError(st, err)

			// Ensure reserved shows 0 because we've deleted the stream
			reserved, err := getReserved(hc, varzAddr, c.storage)
			require_NoError(st, err)
			if reserved != 0 {
				st.Fatalf("Unexpected reserved: %d, want 0", reserved)
			}
		})
	}
}

func TestJetStreamRecoverStreamWithDeletedMessagesNonCleanShutdown(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "T"})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		js.Publish("T", []byte("OK"))
	}

	js.DeleteMsg("T", 22)

	// Now we need a non-clean shutdown.
	// For this use case that means we do *not* write the fss file.
	sd := s.JetStreamConfig().StoreDir
	fss := filepath.Join(sd, "$G", "streams", "T", "msgs", "1.fss")

	// Stop current
	nc.Close()
	s.Shutdown()

	// Remove fss file to simulate a non-clean shutdown.
	err = os.Remove(fss)
	require_NoError(t, err)

	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// Make sure we recovered our stream
	_, err = js.StreamInfo("T")
	require_NoError(t, err)
}

func TestJetStreamRestoreBadStream(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	var rreq JSApiStreamRestoreRequest
	buf, err := os.ReadFile("../test/configs/jetstream/restore_bad_stream/backup.json")
	require_NoError(t, err)
	err = json.Unmarshal(buf, &rreq)
	require_NoError(t, err)

	data, err := os.Open("../test/configs/jetstream/restore_bad_stream/stream.tar.s2")
	require_NoError(t, err)
	defer data.Close()

	var rresp JSApiStreamRestoreResponse
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamRestoreT, rreq.Config.Name), buf, 5*time.Second)
	require_NoError(t, err)
	json.Unmarshal(msg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Error on restore: %+v", rresp.Error)
	}

	var chunk [1024]byte
	for {
		n, err := data.Read(chunk[:])
		if err == io.EOF {
			break
		}
		require_NoError(t, err)

		msg, err = nc.Request(rresp.DeliverSubject, chunk[:n], 5*time.Second)
		require_NoError(t, err)
		json.Unmarshal(msg.Data, &rresp)
		if rresp.Error != nil {
			t.Fatalf("Error on restore: %+v", rresp.Error)
		}
	}
	msg, err = nc.Request(rresp.DeliverSubject, nil, 5*time.Second)
	require_NoError(t, err)
	json.Unmarshal(msg.Data, &rresp)
	if rresp.Error == nil || !strings.Contains(rresp.Error.Description, "unexpected") {
		t.Fatalf("Expected error about unexpected content, got: %+v", rresp.Error)
	}

	dir := filepath.Join(s.JetStreamConfig().StoreDir, globalAccountName)
	f1 := filepath.Join(dir, "fail1.txt")
	f2 := filepath.Join(dir, "fail2.txt")
	for _, f := range []string{f1, f2} {
		if _, err := os.Stat(f); err == nil {
			t.Fatalf("Found file %s", f)
		}
	}
}

func TestJetStreamConsumerAckSampling(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:         "dlc",
		AckPolicy:       nats.AckExplicitPolicy,
		FilterSubject:   "foo",
		SampleFrequency: "100%",
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "dlc")
	require_NoError(t, err)

	_, err = js.Publish("foo", []byte("Hello"))
	require_NoError(t, err)

	msub, err := nc.SubscribeSync("$JS.EVENT.METRIC.>")
	require_NoError(t, err)

	for _, m := range fetchMsgs(t, sub, 1, time.Second) {
		err = m.AckSync()
		require_NoError(t, err)
	}

	m, err := msub.NextMsg(time.Second)
	require_NoError(t, err)

	var am JSConsumerAckMetric
	err = json.Unmarshal(m.Data, &am)
	require_NoError(t, err)

	if am.Stream != "TEST" || am.Consumer != "dlc" || am.ConsumerSeq != 1 {
		t.Fatalf("Not a proper ack metric: %+v", am)
	}

	// Do less than 100%
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:         "alps",
		AckPolicy:       nats.AckExplicitPolicy,
		FilterSubject:   "foo",
		SampleFrequency: "50%",
	})
	require_NoError(t, err)

	asub, err := js.PullSubscribe("foo", "alps")
	require_NoError(t, err)

	total := 500
	for i := 0; i < total; i++ {
		_, err = js.Publish("foo", []byte("Hello"))
		require_NoError(t, err)
	}

	mp := 0
	for _, m := range fetchMsgs(t, asub, total, time.Second) {
		err = m.AckSync()
		require_NoError(t, err)
		mp++
	}
	nc.Flush()

	if mp != total {
		t.Fatalf("Got only %d msgs out of %d", mp, total)
	}

	nmsgs, _, err := msub.Pending()
	require_NoError(t, err)

	// Should be ~250
	if nmsgs < 200 || nmsgs > 300 {
		t.Fatalf("Expected about 250, got %d", nmsgs)
	}
}

func TestJetStreamConsumerAckSamplingSpecifiedUsingUpdateConsumer(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "dlc",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "foo",
	})
	require_NoError(t, err)

	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:         "dlc",
		AckPolicy:       nats.AckExplicitPolicy,
		FilterSubject:   "foo",
		SampleFrequency: "100%",
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "dlc")
	require_NoError(t, err)

	_, err = js.Publish("foo", []byte("Hello"))
	require_NoError(t, err)

	msub, err := nc.SubscribeSync("$JS.EVENT.METRIC.>")
	require_NoError(t, err)

	for _, m := range fetchMsgs(t, sub, 1, time.Second) {
		err = m.AckSync()
		require_NoError(t, err)
	}

	m, err := msub.NextMsg(time.Second)
	require_NoError(t, err)

	var am JSConsumerAckMetric
	err = json.Unmarshal(m.Data, &am)
	require_NoError(t, err)

	if am.Stream != "TEST" || am.Consumer != "dlc" || am.ConsumerSeq != 1 {
		t.Fatalf("Not a proper ack metric: %+v", am)
	}
}

func TestJetStreamConsumerMaxDeliverUpdate(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	maxDeliver := 2
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "ard",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "foo",
		MaxDeliver:    maxDeliver,
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "ard")
	require_NoError(t, err)

	checkMaxDeliver := func() {
		t.Helper()
		for i := 0; i <= maxDeliver; i++ {
			msgs, err := sub.Fetch(2, nats.MaxWait(100*time.Millisecond))
			if i < maxDeliver {
				require_NoError(t, err)
				require_Len(t, 1, len(msgs))
				_ = msgs[0].Nak()
			} else {
				require_Error(t, err, nats.ErrTimeout)
			}
		}
	}

	_, err = js.Publish("foo", []byte("Hello"))
	require_NoError(t, err)
	checkMaxDeliver()

	// update maxDeliver
	maxDeliver++
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "ard",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "foo",
		MaxDeliver:    maxDeliver,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", []byte("Hello"))
	require_NoError(t, err)
	checkMaxDeliver()
}

func TestJetStreamRemoveExternalSource(t *testing.T) {
	ho := DefaultTestOptions
	ho.Port = 4000 //-1
	ho.LeafNode.Host = "127.0.0.1"
	ho.LeafNode.Port = -1
	hs := RunServer(&ho)
	defer hs.Shutdown()

	lu, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ho.LeafNode.Port))
	require_NoError(t, err)

	lo1 := DefaultTestOptions
	lo1.Port = 4111 //-1
	lo1.ServerName = "a-leaf"
	lo1.JetStream = true
	lo1.StoreDir = t.TempDir()
	lo1.JetStreamDomain = "a-leaf"
	lo1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{lu}}}
	l1 := RunServer(&lo1)
	defer l1.Shutdown()

	lo2 := DefaultTestOptions
	lo2.Port = 2111 //-1
	lo2.ServerName = "b-leaf"
	lo2.JetStream = true
	lo2.StoreDir = t.TempDir()
	lo2.JetStreamDomain = "b-leaf"
	lo2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{lu}}}
	l2 := RunServer(&lo2)
	defer l2.Shutdown()

	checkLeafNodeConnected(t, l1)
	checkLeafNodeConnected(t, l2)

	checkStreamMsgs := func(js nats.JetStreamContext, stream string, expected uint64) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			si, err := js.StreamInfo(stream)
			if err != nil {
				return err
			}
			if si.State.Msgs != expected {
				return fmt.Errorf("Expected %v messages, got %v", expected, si.State.Msgs)
			}
			return nil
		})
	}

	sendToStreamTest := func(js nats.JetStreamContext) {
		t.Helper()
		for i := 0; i < 10; i++ {
			_, err = js.Publish("test", []byte("hello"))
			require_NoError(t, err)
		}
	}

	nca, jsa := jsClientConnect(t, l1)
	defer nca.Close()
	_, err = jsa.AddStream(&nats.StreamConfig{Name: "queue", Subjects: []string{"queue"}})
	require_NoError(t, err)

	_, err = jsa.AddStream(&nats.StreamConfig{Name: "testdel", Subjects: []string{"testdel"}})
	require_NoError(t, err)

	ncb, jsb := jsClientConnect(t, l2)
	defer ncb.Close()
	_, err = jsb.AddStream(&nats.StreamConfig{Name: "test", Subjects: []string{"test"}})
	require_NoError(t, err)
	sendToStreamTest(jsb)
	checkStreamMsgs(jsb, "test", 10)

	_, err = jsb.AddStream(&nats.StreamConfig{Name: "testdelsrc1", Subjects: []string{"testdelsrc1"}})
	require_NoError(t, err)
	_, err = jsb.AddStream(&nats.StreamConfig{Name: "testdelsrc2", Subjects: []string{"testdelsrc2"}})
	require_NoError(t, err)

	// Add test as source to queue
	si, err := jsa.UpdateStream(&nats.StreamConfig{
		Name:     "queue",
		Subjects: []string{"queue"},
		Sources: []*nats.StreamSource{
			{
				Name: "test",
				External: &nats.ExternalStream{
					APIPrefix: "$JS.b-leaf.API",
				},
			},
		},
	})
	require_NoError(t, err)
	require_True(t, len(si.Config.Sources) == 1)
	checkStreamMsgs(jsa, "queue", 10)

	// add more entries to "test"
	sendToStreamTest(jsb)

	// verify entries are both in "test" and "queue"
	checkStreamMsgs(jsb, "test", 20)
	checkStreamMsgs(jsa, "queue", 20)

	// Remove source
	si, err = jsa.UpdateStream(&nats.StreamConfig{
		Name:     "queue",
		Subjects: []string{"queue"},
	})
	require_NoError(t, err)
	require_True(t, len(si.Config.Sources) == 0)

	// add more entries to "test"
	sendToStreamTest(jsb)
	// verify entries are in "test"
	checkStreamMsgs(jsb, "test", 30)

	// But they should not be in "queue". We will wait a bit before checking
	// to make sure that we are letting enough time for the sourcing to
	// incorrectly happen if there is a bug.
	time.Sleep(250 * time.Millisecond)
	checkStreamMsgs(jsa, "queue", 20)

	// Test that we delete correctly. First add source to a "testdel"
	si, err = jsa.UpdateStream(&nats.StreamConfig{
		Name:     "testdel",
		Subjects: []string{"testdel"},
		Sources: []*nats.StreamSource{
			{
				Name: "testdelsrc1",
				External: &nats.ExternalStream{
					APIPrefix: "$JS.b-leaf.API",
				},
			},
		},
	})
	require_NoError(t, err)
	require_True(t, len(si.Config.Sources) == 1)
	// Now add the second one...
	si, err = jsa.UpdateStream(&nats.StreamConfig{
		Name:     "testdel",
		Subjects: []string{"testdel"},
		Sources: []*nats.StreamSource{
			{
				Name: "testdelsrc1",
				External: &nats.ExternalStream{
					APIPrefix: "$JS.b-leaf.API",
				},
			},
			{
				Name: "testdelsrc2",
				External: &nats.ExternalStream{
					APIPrefix: "$JS.b-leaf.API",
				},
			},
		},
	})
	require_NoError(t, err)
	require_True(t, len(si.Config.Sources) == 2)
	// Now check that the stream testdel has still 2 source consumers...
	acc, err := l1.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("testdel")
	require_NoError(t, err)
	mset.mu.RLock()
	n := len(mset.sources)
	mset.mu.RUnlock()
	if n != 2 {
		t.Fatalf("Expected 2 sources, got %v", n)
	}

	// Restart leaf "a"
	nca.Close()
	l1.Shutdown()
	l1 = RunServer(&lo1)
	defer l1.Shutdown()

	// add more entries to "test"
	sendToStreamTest(jsb)
	checkStreamMsgs(jsb, "test", 40)

	nca, jsa = jsClientConnect(t, l1)
	defer nca.Close()
	time.Sleep(250 * time.Millisecond)
	checkStreamMsgs(jsa, "queue", 20)
}

func TestJetStreamAddStreamWithFilestoreFailure(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Cause failure to create stream with filestore.
	// In one of ipQueue changes, this could cause a panic, so verify that we get
	// a failure to create, but no panic.
	if _, err := s.globalAccount().addStreamWithStore(
		&StreamConfig{Name: "TEST"},
		&FileStoreConfig{BlockSize: 2 * maxBlockSize}); err == nil {
		t.Fatal("Expected failure, did not get one")
	}
}

type checkFastState struct {
	count int64
	StreamStore
}

func (s *checkFastState) FastState(state *StreamState) {
	// Keep track only when called from checkPending()
	if bytes.Contains(debug.Stack(), []byte("checkPending(")) {
		atomic.AddInt64(&s.count, 1)
	}
	s.StreamStore.FastState(state)
}

func TestJetStreamBackOffCheckPending(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	// Plug or store to see how many times we invoke FastState, which is done in checkPending
	mset.mu.Lock()
	st := &checkFastState{StreamStore: mset.store}
	mset.store = st
	mset.mu.Unlock()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sendStreamMsg(t, nc, "foo", "Hello World!")

	sub, _ := nc.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()
	nc.Flush()

	o, err := mset.addConsumer(&ConsumerConfig{
		DeliverSubject: sub.Subject,
		AckPolicy:      AckExplicit,
		MaxDeliver:     1000,
		BackOff:        []time.Duration{50 * time.Millisecond, 250 * time.Millisecond, time.Second},
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o.delete()

	// Check the first delivery and the following 2 redeliveries
	start := time.Now()
	natsNexMsg(t, sub, time.Second)
	if dur := time.Since(start); dur >= 50*time.Millisecond {
		t.Fatalf("Expected first delivery to be fast, took: %v", dur)
	}
	start = time.Now()
	natsNexMsg(t, sub, time.Second)
	if dur := time.Since(start); dur < 25*time.Millisecond || dur > 75*time.Millisecond {
		t.Fatalf("Expected first redelivery to be ~50ms, took: %v", dur)
	}
	start = time.Now()
	natsNexMsg(t, sub, time.Second)
	if dur := time.Since(start); dur < 200*time.Millisecond || dur > 300*time.Millisecond {
		t.Fatalf("Expected first redelivery to be ~250ms, took: %v", dur)
	}
	// There was a bug that would cause checkPending to be invoked based on the
	// ackWait (which in this case would be the first value of BackOff, which
	// is 50ms). So we would call checkPending() too many times.
	time.Sleep(500 * time.Millisecond)
	// Check now, it should have been invoked twice.
	if n := atomic.LoadInt64(&st.count); n != 2 {
		t.Fatalf("Expected checkPending to be invoked 2 times, was %v", n)
	}
}

func TestJetStreamCrossAccounts(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
        jetstream: enabled
        accounts: {
           A: {
               users: [ {user: a, password: a} ]
               jetstream: enabled
               exports: [
                   {service: '$JS.API.>' }
                   {service: '$KV.>'}
                   {stream: 'accI.>'}
               ]
           },
           I: {
               users: [ {user: i, password: i} ]
               imports: [
                   {service: {account: A, subject: '$JS.API.>'}, to: 'fromA.>' }
                   {service: {account: A, subject: '$KV.>'}, to: 'fromA.$KV.>' }
                   {stream: {subject: 'accI.>', account: A}}
               ]
           }
		}`))
	s, _ := RunServerWithConfig(conf)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	watchNext := func(w nats.KeyWatcher) nats.KeyValueEntry {
		t.Helper()
		select {
		case e := <-w.Updates():
			return e
		case <-time.After(time.Second):
			t.Fatal("Fail to get the next update")
		}
		return nil
	}

	nc1, js1 := jsClientConnect(t, s, nats.UserInfo("a", "a"))
	defer nc1.Close()

	kv1, err := js1.CreateKeyValue(&nats.KeyValueConfig{Bucket: "Map", History: 10})
	if err != nil {
		t.Fatalf("Error creating kv store: %v", err)
	}

	w1, err := kv1.Watch("map")
	if err != nil {
		t.Fatalf("Error creating watcher: %v", err)
	}
	if e := watchNext(w1); e != nil {
		t.Fatalf("Expected nil entry, got %+v", e)
	}

	nc2, err := nats.Connect(s.ClientURL(), nats.UserInfo("i", "i"), nats.CustomInboxPrefix("accI"))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()
	js2, err := nc2.JetStream(nats.APIPrefix("fromA"))
	if err != nil {
		t.Fatalf("Error getting jetstream context: %v", err)
	}

	kv2, err := js2.CreateKeyValue(&nats.KeyValueConfig{Bucket: "Map", History: 10})
	if err != nil {
		t.Fatalf("Error creating kv store: %v", err)
	}

	w2, err := kv2.Watch("map")
	if err != nil {
		t.Fatalf("Error creating watcher: %v", err)
	}
	if e := watchNext(w2); e != nil {
		t.Fatalf("Expected nil entry, got %+v", e)
	}

	// Do a Put from kv2
	rev, err := kv2.Put("map", []byte("value"))
	if err != nil {
		t.Fatalf("Error on put: %v", err)
	}

	// Get from kv1
	e, err := kv1.Get("map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Get from kv2
	e, err = kv2.Get("map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Watcher 1
	if e := watchNext(w1); e == nil || e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Watcher 2
	if e := watchNext(w2); e == nil || e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Try an update form kv2
	if _, err := kv2.Update("map", []byte("updated"), rev); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Get from kv1
	e, err = kv1.Get("map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Get from kv2
	e, err = kv2.Get("map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Watcher 1
	if e := watchNext(w1); e == nil || e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Watcher 2
	if e := watchNext(w2); e == nil || e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Purge from kv2
	if err := kv2.Purge("map"); err != nil {
		t.Fatalf("Error on purge: %v", err)
	}

	// Check purge ok from w1
	if e := watchNext(w1); e == nil || e.Operation() != nats.KeyValuePurge {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Check purge ok from w2
	if e := watchNext(w2); e == nil || e.Operation() != nats.KeyValuePurge {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Delete purge records from kv2
	if err := kv2.PurgeDeletes(nats.DeleteMarkersOlderThan(-1)); err != nil {
		t.Fatalf("Error on purge deletes: %v", err)
	}

	// Check all gone from js1
	if si, err := js1.StreamInfo("KV_Map"); err != nil || si == nil || si.State.Msgs != 0 {
		t.Fatalf("Error getting stream info: err=%v si=%+v", err, si)
	}

	// Delete key from kv2
	if err := kv2.Delete("map"); err != nil {
		t.Fatalf("Error on delete: %v", err)
	}

	// Check key gone from kv1
	if e, err := kv1.Get("map"); err != nats.ErrKeyNotFound || e != nil {
		t.Fatalf("Expected key not found, got err=%v e=%+v", err, e)
	}
}

func TestJetStreamInvalidRestoreRequests(t *testing.T) {
	test := func(t *testing.T, s *Server, replica int) {
		nc := natsConnect(t, s.ClientURL())
		defer nc.Close()
		// test invalid stream config in restore request
		require_fail := func(cfg StreamConfig, errDesc string) {
			t.Helper()
			rreq := &JSApiStreamRestoreRequest{
				Config: cfg,
			}
			req, err := json.Marshal(rreq)
			require_NoError(t, err)
			rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "fail"), req, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var rresp JSApiStreamRestoreResponse
			json.Unmarshal(rmsg.Data, &rresp)
			require_True(t, rresp.Error != nil)
			require_Equal(t, rresp.Error.Description, errDesc)
		}
		require_fail(StreamConfig{Name: "fail", MaxBytes: 1024, Storage: FileStorage, Replicas: 6},
			"maximum replicas is 5")
		require_fail(StreamConfig{Name: "fail", MaxBytes: 2 * 1012 * 1024, Storage: FileStorage, Replicas: replica},
			"insufficient storage resources available")
		js, err := nc.JetStream()
		require_NoError(t, err)
		_, err = js.AddStream(&nats.StreamConfig{Name: "stream", MaxBytes: 1024, Storage: nats.FileStorage, Replicas: 1})
		require_NoError(t, err)
		require_fail(StreamConfig{Name: "fail", MaxBytes: 1024, Storage: FileStorage},
			"maximum number of streams reached")
	}

	commonAccSection := `
		no_auth_user: u
		accounts {
			ONE {
				users = [ { user: "u", pass: "s3cr3t!" } ]
				jetstream: {
					max_store: 1Mb
					max_streams: 1
				}
			}
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}`

	t.Run("clustered", func(t *testing.T) {
		c := createJetStreamClusterWithTemplate(t, `
			listen: 127.0.0.1:-1
			server_name: %s
			jetstream: {
				max_mem_store: 2MB,
				max_file_store: 8MB,
				store_dir: '%s',
			}
			cluster {
				name: %s
				listen: 127.0.0.1:%d
				routes = [%s]
			}`+commonAccSection, "clust", 3)
		defer c.shutdown()
		s := c.randomServer()
		test(t, s, 3)
	})
	t.Run("single", func(t *testing.T) {
		storeDir := t.TempDir()
		conf := createConfFile(t, []byte(fmt.Sprintf(`
			listen: 127.0.0.1:-1
			jetstream: {max_mem_store: 2MB, max_file_store: 8MB, store_dir: '%s'}
			%s`, storeDir, commonAccSection)))
		s, _ := RunServerWithConfig(conf)
		defer s.Shutdown()
		test(t, s, 1)
	})
}

func TestJetStreamLimits(t *testing.T) {
	test := func(t *testing.T, s *Server) {
		nc := natsConnect(t, s.ClientURL())
		defer nc.Close()

		js, err := nc.JetStream()
		require_NoError(t, err)

		si, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
		require_NoError(t, err)
		require_True(t, si.Config.Duplicates == time.Minute)

		si, err = js.AddStream(&nats.StreamConfig{Name: "bar", Duplicates: 1500 * time.Millisecond})
		require_NoError(t, err)
		require_True(t, si.Config.Duplicates == 1500*time.Millisecond)

		_, err = js.UpdateStream(&nats.StreamConfig{Name: "bar", Duplicates: 2 * time.Minute})
		require_Error(t, err)
		require_Equal(t, err.Error(), "nats: duplicates window can not be larger then server limit of 1m0s")

		_, err = js.AddStream(&nats.StreamConfig{Name: "baz", Duplicates: 2 * time.Minute})
		require_Error(t, err)
		require_Equal(t, err.Error(), "nats: duplicates window can not be larger then server limit of 1m0s")

		ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dur1", AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
		require_True(t, ci.Config.MaxAckPending == 1000)
		require_True(t, ci.Config.MaxRequestBatch == 250)

		_, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dur2", AckPolicy: nats.AckExplicitPolicy, MaxRequestBatch: 500})
		require_Error(t, err)
		require_Equal(t, err.Error(), "nats: consumer max request batch exceeds server limit of 250")

		ci, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dur2", AckPolicy: nats.AckExplicitPolicy, MaxAckPending: 500})
		require_NoError(t, err)
		require_True(t, ci.Config.MaxAckPending == 500)
		require_True(t, ci.Config.MaxRequestBatch == 250)

		_, err = js.UpdateConsumer("foo", &nats.ConsumerConfig{Durable: "dur2", AckPolicy: nats.AckExplicitPolicy, MaxAckPending: 2000})
		require_Error(t, err)
		require_Equal(t, err.Error(), "nats: consumer max ack pending exceeds system limit of 1000")

		_, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dur3", AckPolicy: nats.AckExplicitPolicy, MaxAckPending: 2000})
		require_Error(t, err)
		require_Equal(t, err.Error(), "nats: consumer max ack pending exceeds system limit of 1000")
	}

	t.Run("clustered", func(t *testing.T) {
		tmpl := `
			listen: 127.0.0.1:-1
			server_name: %s
			jetstream: {
				max_mem_store: 2MB,
				max_file_store: 8MB,
				store_dir: '%s',
				limits: {duplicate_window: "1m", max_request_batch: 250}
			}
			cluster {
				name: %s
				listen: 127.0.0.1:%d
				routes = [%s]
			}
			no_auth_user: u
			accounts {
				ONE {
					users = [ { user: "u", pass: "s3cr3t!" } ]
					jetstream: enabled
				}
				$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
			}`
		limitsTest := func(t *testing.T, tmpl string) {
			c := createJetStreamClusterWithTemplate(t, tmpl, "clust", 3)
			defer c.shutdown()
			s := c.randomServer()
			test(t, s)
		}
		// test with max_ack_pending being defined in operator or account
		t.Run("operator", func(t *testing.T) {
			limitsTest(t, strings.Replace(tmpl, "duplicate_window", "max_ack_pending: 1000, duplicate_window", 1))
		})
		t.Run("account", func(t *testing.T) {
			limitsTest(t, strings.Replace(tmpl, "jetstream: enabled", "jetstream: {max_ack_pending: 1000}", 1))
		})
	})

	t.Run("single", func(t *testing.T) {
		tmpl := `
			listen: 127.0.0.1:-1
			jetstream: {
				max_mem_store: 2MB,
				max_file_store: 8MB,
				store_dir: '%s',
				limits: {duplicate_window: "1m", max_request_batch: 250}
			}
			no_auth_user: u
			accounts {
				ONE {
					users = [ { user: "u", pass: "s3cr3t!" } ]
					jetstream: enabled
				}
				$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
			}`
		limitsTest := func(t *testing.T, tmpl string) {
			storeDir := t.TempDir()
			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, storeDir)))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			require_True(t, opts.JetStreamLimits.Duplicates == time.Minute)
			test(t, s)
		}
		// test with max_ack_pending being defined in operator or account
		t.Run("operator", func(t *testing.T) {
			limitsTest(t, strings.Replace(tmpl, "duplicate_window", "max_ack_pending: 1000, duplicate_window", 1))
		})
		t.Run("account", func(t *testing.T) {
			limitsTest(t, strings.Replace(tmpl, "jetstream: enabled", "jetstream: {max_ack_pending: 1000}", 1))
		})
	})
}

func TestJetStreamConsumerStreamUpdate(t *testing.T) {
	test := func(t *testing.T, s *Server, replica int) {
		nc := natsConnect(t, s.ClientURL())
		defer nc.Close()
		js, err := nc.JetStream()
		require_NoError(t, err)
		_, err = js.AddStream(&nats.StreamConfig{Name: "foo", Duplicates: 1 * time.Minute, Replicas: replica})
		defer js.DeleteStream("foo")
		require_NoError(t, err)
		// Update with no change
		_, err = js.UpdateStream(&nats.StreamConfig{Name: "foo", Duplicates: 1 * time.Minute, Replicas: replica})
		require_NoError(t, err)
		// Update with change
		_, err = js.UpdateStream(&nats.StreamConfig{Description: "stream", Name: "foo", Duplicates: 1 * time.Minute, Replicas: replica})
		require_NoError(t, err)
		_, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dur1", AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
		// Update with no change
		_, err = js.UpdateConsumer("foo", &nats.ConsumerConfig{Durable: "dur1", AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
		// Update with change
		_, err = js.UpdateConsumer("foo", &nats.ConsumerConfig{Description: "consumer", Durable: "dur1", AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
	}
	t.Run("clustered", func(t *testing.T) {
		c := createJetStreamClusterWithTemplate(t, `
			listen: 127.0.0.1:-1
			server_name: %s
			jetstream: {
				max_mem_store: 2MB,
				max_file_store: 8MB,
				store_dir: '%s',
			}
			cluster {
				name: %s
				listen: 127.0.0.1:%d
				routes = [%s]
			}
			no_auth_user: u
			accounts {
				ONE {
					users = [ { user: "u", pass: "s3cr3t!" } ]
					jetstream: enabled
				}
				$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
			}`, "clust", 3)
		defer c.shutdown()
		s := c.randomServer()
		t.Run("r3", func(t *testing.T) {
			test(t, s, 3)
		})
		t.Run("r1", func(t *testing.T) {
			test(t, s, 1)
		})
	})
	t.Run("single", func(t *testing.T) {
		storeDir := t.TempDir()
		conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 2MB, max_file_store: 8MB, store_dir: '%s'}`,
			storeDir)))
		s, _ := RunServerWithConfig(conf)
		defer s.Shutdown()
		test(t, s, 1)
	})
}

func TestJetStreamImportReload(t *testing.T) {
	storeDir := t.TempDir()

	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 2MB, max_file_store: 8MB, store_dir: '%s'}
		accounts: {
			account_a: {
				users: [{user: user_a, password: pwd}]
				exports: [{stream: news.>}]
			}
			account_b: {
				users: [{user: user_b, password: pwd}]
				jetstream: enabled
				imports: [{stream: {subject: news.>, account: account_a}}]
			}
		}`, storeDir)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	ncA := natsConnect(t, s.ClientURL(), nats.UserInfo("user_a", "pwd"))
	defer ncA.Close()

	ncB := natsConnect(t, s.ClientURL(), nats.UserInfo("user_b", "pwd"))
	defer ncB.Close()

	jsB, err := ncB.JetStream()
	require_NoError(t, err)

	_, err = jsB.AddStream(&nats.StreamConfig{Name: "news", Subjects: []string{"news.>"}})
	require_NoError(t, err)

	require_NoError(t, ncA.Publish("news.article", nil))
	require_NoError(t, ncA.Flush())

	si, err := jsB.StreamInfo("news")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 1)

	// Remove exports/imports
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 2MB, max_file_store: 8MB, store_dir: '%s'}
		accounts: {
			account_a: {
				users: [{user: user_a, password: pwd}]
			}
			account_b: {
				users: [{user: user_b, password: pwd}]
				jetstream: enabled
			}
		}`, storeDir))

	require_NoError(t, ncA.Publish("news.article", nil))
	require_NoError(t, ncA.Flush())

	si, err = jsB.StreamInfo("news")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 1)
}

func TestJetStreamRecoverSealedAfterServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		js.PublishAsync("foo", []byte("OK"))
	}
	<-js.PublishAsyncComplete()

	_, err = js.UpdateStream(&nats.StreamConfig{Name: "foo", Sealed: true})
	require_NoError(t, err)

	nc.Close()

	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo("foo")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 100)
}

func TestJetStreamImportConsumerStreamSubjectRemapSingle(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 4GB, max_file_store: 1TB}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: js, password: pwd} ]
				exports [
					# This is streaming to a delivery subject for a push based consumer.
					{ stream: "deliver.*" }
					{ stream: "foo.*" }
					# This is to ack received messages. This is a service to support sync ack.
					{ service: "$JS.ACK.ORDERS.*.>" }
					# To support ordered consumers, flow control.
					{ service: "$JS.FC.>" }
				]
			},
			IM: {
				users: [ {user: im, password: pwd} ]
				imports [
					{ stream:  { account: JS, subject: "deliver.ORDERS" }, to: "d.*" }
					{ stream:  { account: JS, subject: "foo.*" }, to: "bar.*" }
					{ service: { account: JS, subject: "$JS.FC.>" }}
				]
			},
		}
	`))

	test := func(t *testing.T, queue bool) {
		s, _ := RunServerWithConfig(conf)
		if config := s.JetStreamConfig(); config != nil {
			defer removeDir(t, config.StoreDir)
		}
		defer s.Shutdown()

		nc, js := jsClientConnect(t, s, nats.UserInfo("js", "pwd"))
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "ORDERS",
			Subjects: []string{"foo"}, // The JS subject.
			Storage:  nats.MemoryStorage},
		)
		require_NoError(t, err)

		_, err = js.Publish("foo", []byte("OK"))
		require_NoError(t, err)

		queueName := ""
		if queue {
			queueName = "queue"
		}

		_, err = js.AddConsumer("ORDERS", &nats.ConsumerConfig{
			DeliverSubject: "deliver.ORDERS",
			AckPolicy:      nats.AckExplicitPolicy,
			DeliverGroup:   queueName,
		})
		require_NoError(t, err)

		nc2, err := nats.Connect(s.ClientURL(), nats.UserInfo("im", "pwd"))
		require_NoError(t, err)
		defer nc2.Close()

		var sub *nats.Subscription
		if queue {
			sub, err = nc2.QueueSubscribeSync("d.ORDERS", queueName)
			require_NoError(t, err)
		} else {
			sub, err = nc2.SubscribeSync("d.ORDERS")
			require_NoError(t, err)
		}

		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)

		if m.Subject != "foo" {
			t.Fatalf("Subject not mapped correctly across account boundary, expected %q got %q", "foo", m.Subject)
		}

		// Now do one that would kick in a transform.
		_, err = js.AddConsumer("ORDERS", &nats.ConsumerConfig{
			DeliverSubject: "foo.ORDERS",
			AckPolicy:      nats.AckExplicitPolicy,
			DeliverGroup:   queueName,
		})
		require_NoError(t, err)

		if queue {
			sub, err = nc2.QueueSubscribeSync("bar.ORDERS", queueName)
			require_NoError(t, err)
		} else {
			sub, err = nc2.SubscribeSync("bar.ORDERS")
			require_NoError(t, err)
		}
		m, err = sub.NextMsg(time.Second)
		require_NoError(t, err)

		if m.Subject != "foo" {
			t.Fatalf("Subject not mapped correctly across account boundary, expected %q got %q", "foo", m.Subject)
		}
	}

	t.Run("noqueue", func(t *testing.T) {
		test(t, false)
	})
	t.Run("queue", func(t *testing.T) {
		test(t, true)
	})
}

func TestJetStreamWorkQueueSourceRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sent := 10
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Replicas: 1,
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	for i := 0; i < sent; i++ {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 1,
		// TODO test will pass when retention commented out
		Retention: nats.WorkQueuePolicy,
		Sources:   []*nats.StreamSource{{Name: "FOO"}}})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dur", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "dur", nats.BindStream("TEST"))
	require_NoError(t, err)

	ci, err := js.ConsumerInfo("TEST", "dur")
	require_NoError(t, err)
	require_True(t, ci.NumPending == uint64(sent))

	msgs, err := sub.Fetch(sent)
	require_NoError(t, err)
	require_True(t, len(msgs) == sent)

	for i := 0; i < sent; i++ {
		err = msgs[i].AckSync()
		require_NoError(t, err)
	}

	ci, err = js.ConsumerInfo("TEST", "dur")
	require_NoError(t, err)
	require_True(t, ci.NumPending == 0)

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 0)

	// Restart server
	nc.Close()
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	time.Sleep(200 * time.Millisecond)
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		hs := s.healthz(nil)
		if hs.Status == "ok" && hs.Error == _EMPTY_ {
			return nil
		}
		return fmt.Errorf("healthz %s %s", hs.Error, hs.Status)
	})

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)

	if si.State.Msgs != 0 {
		t.Fatalf("Expected 0 messages on restart, got %d", si.State.Msgs)
	}

	ctest, err := js.ConsumerInfo("TEST", "dur")
	require_NoError(t, err)

	//TODO (mh) I have experienced in other tests that NumPending has a value of 1 post restart.
	// seems to go awary in single server setup. It's also unrelated to work queue
	// but that error seems benign.
	if ctest.NumPending != 0 {
		t.Fatalf("Expected pending of 0 but got %d", ctest.NumPending)
	}

	sub, err = js.PullSubscribe("foo", "dur", nats.BindStream("TEST"))
	require_NoError(t, err)
	_, err = sub.Fetch(1, nats.MaxWait(time.Second))
	if err != nats.ErrTimeout {
		require_NoError(t, err)
	}
}

func TestJetStreamWorkQueueSourceNamingRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "C1", Subjects: []string{"foo.*"}})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "C2", Subjects: []string{"bar.*"}})
	require_NoError(t, err)

	sendCount := 10
	for i := 0; i < sendCount; i++ {
		_, err = js.Publish(fmt.Sprintf("foo.%d", i), nil)
		require_NoError(t, err)
		_, err = js.Publish(fmt.Sprintf("bar.%d", i), nil)
		require_NoError(t, err)
	}

	// TODO Test will always pass if pending is 0
	pending := 1
	// For some yet unknown reason this failure seems to require 2 streams to source from.
	// This might possibly be timing, as the test sometimes passes
	streams := 2
	totalPending := uint64(streams * pending)
	totalMsgs := streams * sendCount
	totalNonPending := streams * (sendCount - pending)

	// TODO Test will always pass if this is named A (go returns directory names sorted)
	// A: this stream is recovered BEFORE C1/C2, tbh, I'd expect this to be the case to fail, but it isn't
	// D: this stream is recovered AFTER C1/C2, which is the case that fails (perhaps it is timing)
	srcName := "D"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      srcName,
		Retention: nats.WorkQueuePolicy,
		Sources:   []*nats.StreamSource{{Name: "C1"}, {Name: "C2"}},
	})
	require_NoError(t, err)

	// Add a consumer and consume all but totalPending messages
	_, err = js.AddConsumer(srcName, &nats.ConsumerConfig{Durable: "dur", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("", "dur", nats.BindStream(srcName))
	require_NoError(t, err)

	checkFor(t, 5*time.Second, time.Millisecond*200, func() error {
		if ci, err := js.ConsumerInfo(srcName, "dur"); err != nil {
			return err
		} else if ci.NumPending != uint64(totalMsgs) {
			return fmt.Errorf("not enough messages: %d", ci.NumPending)
		}
		return nil
	})

	// consume all but messages we want pending
	msgs, err := sub.Fetch(totalNonPending)
	require_NoError(t, err)
	require_True(t, len(msgs) == totalNonPending)

	for _, m := range msgs {
		err = m.AckSync()
		require_NoError(t, err)
	}

	ci, err := js.ConsumerInfo(srcName, "dur")
	require_NoError(t, err)
	require_True(t, ci.NumPending == totalPending)

	si, err := js.StreamInfo(srcName)
	require_NoError(t, err)
	require_True(t, si.State.Msgs == totalPending)

	// Restart server
	nc.Close()
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	time.Sleep(200 * time.Millisecond)
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		hs := s.healthz(nil)
		if hs.Status == "ok" && hs.Error == _EMPTY_ {
			return nil
		}
		return fmt.Errorf("healthz %s %s", hs.Error, hs.Status)
	})

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err = js.StreamInfo(srcName)
	require_NoError(t, err)

	if si.State.Msgs != totalPending {
		t.Fatalf("Expected 0 messages on restart, got %d", si.State.Msgs)
	}
}

func TestJetStreamDisabledHealthz(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}

	s.DisableJetStream()

	hs := s.healthz(&HealthzOptions{JSEnabledOnly: true})
	if hs.Status == "unavailable" && hs.Error == NewJSNotEnabledError().Error() {
		return
	}

	t.Fatalf("Expected healthz to return error if JetStream is disabled, got status: %s", hs.Status)
}

func TestJetStreamPullTimeout(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name: "TEST",
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "pr",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	const numMessages = 1000
	// Send messages in small intervals.
	go func() {
		for i := 0; i < numMessages; i++ {
			time.Sleep(time.Millisecond * 10)
			sendStreamMsg(t, nc, "TEST", "data")
		}
	}()

	// Prepare manual Pull Request.
	req := &JSApiConsumerGetNextRequest{Batch: 200, NoWait: false, Expires: time.Millisecond * 100}
	jreq, _ := json.Marshal(req)

	subj := fmt.Sprintf(JSApiRequestNextT, "TEST", "pr")
	reply := "_pr_"
	var got atomic.Int32
	nc.PublishRequest(subj, reply, jreq)

	// Manually subscribe to inbox subject and send new request only if we get `408 Request Timeout`.
	sub, _ := nc.Subscribe(reply, func(msg *nats.Msg) {
		if msg.Header.Get("Status") == "408" && msg.Header.Get("Description") == "Request Timeout" {
			nc.PublishRequest(subj, reply, jreq)
			nc.Flush()
		} else {
			got.Add(1)
			msg.Ack()
		}
	})
	defer sub.Unsubscribe()

	// Check if we're not stuck.
	checkFor(t, time.Second*30, time.Second*1, func() error {
		if got.Load() < int32(numMessages) {
			return fmt.Errorf("expected %d messages", numMessages)
		}
		return nil
	})
}

func TestJetStreamPullMaxBytes(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name: "TEST",
	})
	require_NoError(t, err)

	// Put in ~2MB, each ~100k
	msz, dsz := 100_000, 99_950
	total, msg := 20, []byte(strings.Repeat("Z", dsz))

	for i := 0; i < total; i++ {
		if _, err := js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "pr",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	req := &JSApiConsumerGetNextRequest{MaxBytes: 100, NoWait: true}
	jreq, _ := json.Marshal(req)

	subj := fmt.Sprintf(JSApiRequestNextT, "TEST", "pr")
	reply := "_pr_"
	sub, _ := nc.SubscribeSync(reply)
	defer sub.Unsubscribe()

	checkHeader := func(m *nats.Msg, expected *nats.Header) {
		t.Helper()
		if len(m.Data) != 0 {
			t.Fatalf("Did not expect data, got %d bytes", len(m.Data))
		}
		expectedStatus, givenStatus := expected.Get("Status"), m.Header.Get("Status")
		expectedDesc, givenDesc := expected.Get("Description"), m.Header.Get("Description")
		if expectedStatus != givenStatus || expectedDesc != givenDesc {
			t.Fatalf("expected  %s %s, got %s %s", expectedStatus, expectedDesc, givenStatus, givenDesc)
		}
	}

	// If we ask for less MaxBytes then a single message make sure we get an error.
	badReq := &nats.Header{"Status": []string{"409"}, "Description": []string{"Message Size Exceeds MaxBytes"}}

	nc.PublishRequest(subj, reply, jreq)
	m, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	checkSubsPending(t, sub, 0)
	checkHeader(m, badReq)

	// If we request a ton of max bytes make sure batch size overrides.
	req = &JSApiConsumerGetNextRequest{Batch: 1, MaxBytes: 10_000_000, NoWait: true}
	jreq, _ = json.Marshal(req)
	nc.PublishRequest(subj, reply, jreq)
	// we expect two messages, as the second one should be `Batch Completed` status.
	checkSubsPending(t, sub, 2)

	// first one is message from the stream.
	m, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_True(t, len(m.Data) == dsz)
	require_True(t, len(m.Header) == 0)
	// second one is the status.
	m, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	if v := m.Header.Get("Description"); v != "Batch Completed" {
		t.Fatalf("Expected Batch Completed, got: %s", v)
	}
	checkSubsPending(t, sub, 0)

	// Same but with batch > 1
	req = &JSApiConsumerGetNextRequest{Batch: 5, MaxBytes: 10_000_000, NoWait: true}
	jreq, _ = json.Marshal(req)
	nc.PublishRequest(subj, reply, jreq)
	// 6, not 5, as 6th is the status.
	checkSubsPending(t, sub, 6)
	for i := 0; i < 5; i++ {
		m, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		require_True(t, len(m.Data) == dsz)
		require_True(t, len(m.Header) == 0)
	}
	m, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	if v := m.Header.Get("Description"); v != "Batch Completed" {
		t.Fatalf("Expected Batch Completed, got: %s", v)
	}
	checkSubsPending(t, sub, 0)

	// Now ask for large batch but make sure we are limited by batch size.
	req = &JSApiConsumerGetNextRequest{Batch: 1_000, MaxBytes: msz * 4, NoWait: true}
	jreq, _ = json.Marshal(req)
	nc.PublishRequest(subj, reply, jreq)
	// Receive 4 messages + the 409
	checkSubsPending(t, sub, 5)
	for i := 0; i < 4; i++ {
		m, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		require_True(t, len(m.Data) == dsz)
		require_True(t, len(m.Header) == 0)
	}
	m, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	checkHeader(m, badReq)
	checkSubsPending(t, sub, 0)

	req = &JSApiConsumerGetNextRequest{Batch: 1_000, MaxBytes: msz, NoWait: true}
	jreq, _ = json.Marshal(req)
	nc.PublishRequest(subj, reply, jreq)
	// Receive 1 message + 409
	checkSubsPending(t, sub, 2)
	m, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_True(t, len(m.Data) == dsz)
	require_True(t, len(m.Header) == 0)
	m, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	checkHeader(m, badReq)
	checkSubsPending(t, sub, 0)
}

func TestJetStreamStreamRepublishCycle(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:     "RPC",
		Storage:  MemoryStorage,
		Subjects: []string{"foo.>", "bar.*", "baz"},
	}

	expectFail := func() {
		t.Helper()
		req, err := json.Marshal(cfg)
		require_NoError(t, err)
		rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
		require_NoError(t, err)
		var resp JSApiStreamCreateResponse
		err = json.Unmarshal(rmsg.Data, &resp)
		require_NoError(t, err)
		if resp.Type != JSApiStreamCreateResponseType {
			t.Fatalf("Invalid response type %s expected %s", resp.Type, JSApiStreamCreateResponseType)
		}
		if resp.Error == nil {
			t.Fatalf("Expected error but got none")
		}
		if !strings.Contains(resp.Error.Description, "republish destination forms a cycle") {
			t.Fatalf("Expected cycle error, got %q", resp.Error.Description)
		}
	}

	cfg.RePublish = &RePublish{
		Source:      "foo.>",
		Destination: "foo.>",
	}
	expectFail()

	cfg.RePublish = &RePublish{
		Source:      "bar.bar",
		Destination: "foo.bar",
	}
	expectFail()

	cfg.RePublish = &RePublish{
		Source:      "baz",
		Destination: "bar.bar",
	}
	expectFail()
}

func TestJetStreamStreamRepublishOneTokenMatch(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:     "Stream1",
		Storage:  MemoryStorage,
		Subjects: []string{"one", "four"},
		RePublish: &RePublish{
			Source:      "one",
			Destination: "uno",
			HeadersOnly: false,
		},
	}
	addStream(t, nc, cfg)

	sub, err := nc.SubscribeSync("uno")
	require_NoError(t, err)

	msg, toSend := bytes.Repeat([]byte("Z"), 512), 100
	for i := 0; i < toSend; i++ {
		js.PublishAsync("one", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkSubsPending(t, sub, toSend)
	m, err := sub.NextMsg(time.Second)
	require_NoError(t, err)

	if !(len(m.Data) > 0) {
		t.Fatalf("Expected msg data")
	}
}

func TestJetStreamStreamRepublishMultiTokenMatch(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:     "Stream1",
		Storage:  MemoryStorage,
		Subjects: []string{"one.>", "four.>"},
		RePublish: &RePublish{
			Source:      "one.two.>",
			Destination: "uno.dos.>",
			HeadersOnly: false,
		},
	}
	addStream(t, nc, cfg)

	sub, err := nc.SubscribeSync("uno.dos.>")
	require_NoError(t, err)

	msg, toSend := bytes.Repeat([]byte("Z"), 512), 100
	for i := 0; i < toSend; i++ {
		js.PublishAsync("one.two.three", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkSubsPending(t, sub, toSend)
	m, err := sub.NextMsg(time.Second)
	require_NoError(t, err)

	if !(len(m.Data) > 0) {
		t.Fatalf("Expected msg data")
	}
}

func TestJetStreamStreamRepublishAnySubjectMatch(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:     "Stream1",
		Storage:  MemoryStorage,
		Subjects: []string{"one.>", "four.>"},
		RePublish: &RePublish{
			Destination: "uno.dos.>",
			HeadersOnly: false,
		},
	}
	addStream(t, nc, cfg)

	sub, err := nc.SubscribeSync("uno.dos.>")
	require_NoError(t, err)

	msg, toSend := bytes.Repeat([]byte("Z"), 512), 100
	for i := 0; i < toSend; i++ {
		js.PublishAsync("one.two.three", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkSubsPending(t, sub, toSend)
	m, err := sub.NextMsg(time.Second)
	require_NoError(t, err)

	if !(len(m.Data) > 0) {
		t.Fatalf("Expected msg data")
	}
}

func TestJetStreamStreamRepublishMultiTokenNoMatch(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:     "Stream1",
		Storage:  MemoryStorage,
		Subjects: []string{"one.>", "four.>"},
		RePublish: &RePublish{
			Source:      "one.two.>",
			Destination: "uno.dos.>",
			HeadersOnly: true,
		},
	}
	addStream(t, nc, cfg)

	sub, err := nc.SubscribeSync("uno.dos.>")
	require_NoError(t, err)

	msg, toSend := bytes.Repeat([]byte("Z"), 512), 100
	for i := 0; i < toSend; i++ {
		js.PublishAsync("four.five.six", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkSubsPending(t, sub, 0)
	require_NoError(t, err)
}

func TestJetStreamStreamRepublishOneTokenNoMatch(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:     "Stream1",
		Storage:  MemoryStorage,
		Subjects: []string{"one", "four"},
		RePublish: &RePublish{
			Source:      "one",
			Destination: "uno",
			HeadersOnly: true,
		},
	}
	addStream(t, nc, cfg)

	sub, err := nc.SubscribeSync("uno")
	require_NoError(t, err)

	msg, toSend := bytes.Repeat([]byte("Z"), 512), 100
	for i := 0; i < toSend; i++ {
		js.PublishAsync("four", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkSubsPending(t, sub, 0)
	require_NoError(t, err)
}

func TestJetStreamStreamRepublishHeadersOnly(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:     "RPC",
		Storage:  MemoryStorage,
		Subjects: []string{"foo", "bar", "baz"},
		RePublish: &RePublish{
			Destination: "RP.>",
			HeadersOnly: true,
		},
	}
	addStream(t, nc, cfg)

	sub, err := nc.SubscribeSync("RP.>")
	require_NoError(t, err)

	msg, toSend := bytes.Repeat([]byte("Z"), 512), 100
	for i := 0; i < toSend; i++ {
		js.PublishAsync("foo", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkSubsPending(t, sub, toSend)
	m, err := sub.NextMsg(time.Second)
	require_NoError(t, err)

	if len(m.Data) > 0 {
		t.Fatalf("Expected no msg just headers, but got %d bytes", len(m.Data))
	}
	if sz := m.Header.Get(JSMsgSize); sz != "512" {
		t.Fatalf("Expected msg size hdr, got %q", sz)
	}
}

func TestJetStreamConsumerDeliverNewNotConsumingBeforeRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	inbox := nats.NewInbox()
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverSubject: inbox,
		Durable:        "dur",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverPolicy:  nats.DeliverNewPolicy,
		FilterSubject:  "foo",
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	checkCount := func(expected int) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			ci, err := js.ConsumerInfo("TEST", "dur")
			if err != nil {
				return err
			}
			if n := int(ci.NumPending); n != expected {
				return fmt.Errorf("Expected %v pending, got %v", expected, n)
			}
			return nil
		})
	}
	checkCount(10)

	time.Sleep(300 * time.Millisecond)

	// Check server restart
	nc.Close()
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	checkCount(10)

	// Make sure messages can be consumed
	sub := natsSubSync(t, nc, inbox)
	for i := 0; i < 10; i++ {
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("i=%v next msg error: %v", i, err)
		}
		msg.AckSync()
	}
	checkCount(0)
}

func TestJetStreamConsumerNumPendingWithMaxPerSubjectGreaterThanOne(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	test := func(t *testing.T, st nats.StorageType) {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"KV.*.*"},
			MaxMsgsPerSubject: 2,
			Storage:           st,
		})
		require_NoError(t, err)

		// If we allow more than one msg per subject, consumer's num pending can be off (bug in store level).
		// This requires a filtered state, simple states work ok.
		// Since we now rely on stream's filtered state when asked directly for consumer info in >=2.8.3.
		js.PublishAsync("KV.plans.foo", []byte("OK"))
		js.PublishAsync("KV.plans.bar", []byte("OK"))
		js.PublishAsync("KV.plans.baz", []byte("OK"))
		// These are required, the consumer needs to filter these out to see the bug.
		js.PublishAsync("KV.config.foo", []byte("OK"))
		js.PublishAsync("KV.config.bar", []byte("OK"))
		js.PublishAsync("KV.config.baz", []byte("OK"))

		// Double up some now.
		js.PublishAsync("KV.plans.bar", []byte("OK"))
		js.PublishAsync("KV.plans.baz", []byte("OK"))

		ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
			Durable:       "d",
			AckPolicy:     nats.AckExplicitPolicy,
			DeliverPolicy: nats.DeliverLastPerSubjectPolicy,
			FilterSubject: "KV.plans.*",
		})
		require_NoError(t, err)

		err = js.DeleteStream("TEST")
		require_NoError(t, err)

		if ci.NumPending != 3 {
			t.Fatalf("Expected 3 NumPending, but got %d", ci.NumPending)
		}
	}

	t.Run("MemoryStore", func(t *testing.T) { test(t, nats.MemoryStorage) })
	t.Run("FileStore", func(t *testing.T) { test(t, nats.FileStorage) })
}

func TestJetStreamMsgGetNoAdvisory(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		js.PublishAsync("foo", []byte("ok"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	sub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.>")
	require_NoError(t, err)

	_, err = js.GetMsg("foo", 1)
	require_NoError(t, err)

	checkSubsPending(t, sub, 0)
}

func TestJetStreamDirectMsgGet(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:        "DSMG",
		Storage:     MemoryStorage,
		Subjects:    []string{"foo", "bar", "baz"},
		MaxMsgsPer:  1,
		AllowDirect: true,
	}
	addStream(t, nc, cfg)

	sendStreamMsg(t, nc, "foo", "foo")
	sendStreamMsg(t, nc, "bar", "bar")
	sendStreamMsg(t, nc, "baz", "baz")

	getSubj := fmt.Sprintf(JSDirectMsgGetT, "DSMG")
	getMsg := func(req *JSApiMsgGetRequest) *nats.Msg {
		var b []byte
		var err error
		if req != nil {
			b, err = json.Marshal(req)
			require_NoError(t, err)
		}
		m, err := nc.Request(getSubj, b, time.Second)
		require_NoError(t, err)
		return m
	}

	m := getMsg(&JSApiMsgGetRequest{LastFor: "foo"})
	require_True(t, string(m.Data) == "foo")
	require_True(t, m.Header.Get(JSStream) == "DSMG")
	require_True(t, m.Header.Get(JSSequence) == "1")
	require_True(t, m.Header.Get(JSSubject) == "foo")
	require_True(t, m.Subject != "foo")
	require_True(t, m.Header.Get(JSTimeStamp) != _EMPTY_)

	m = getMsg(&JSApiMsgGetRequest{LastFor: "bar"})
	require_True(t, string(m.Data) == "bar")
	require_True(t, m.Header.Get(JSStream) == "DSMG")
	require_True(t, m.Header.Get(JSSequence) == "2")
	require_True(t, m.Header.Get(JSSubject) == "bar")
	require_True(t, m.Subject != "bar")
	require_True(t, m.Header.Get(JSTimeStamp) != _EMPTY_)

	m = getMsg(&JSApiMsgGetRequest{LastFor: "baz"})
	require_True(t, string(m.Data) == "baz")
	require_True(t, m.Header.Get(JSStream) == "DSMG")
	require_True(t, m.Header.Get(JSSequence) == "3")
	require_True(t, m.Header.Get(JSSubject) == "baz")
	require_True(t, m.Subject != "baz")
	require_True(t, m.Header.Get(JSTimeStamp) != _EMPTY_)

	// Test error conditions

	// Nil request
	m = getMsg(nil)
	require_True(t, len(m.Data) == 0)
	require_True(t, m.Header.Get("Status") == "408")
	require_True(t, m.Header.Get("Description") == "Empty Request")

	// Empty request
	m = getMsg(&JSApiMsgGetRequest{})
	require_True(t, len(m.Data) == 0)
	require_True(t, m.Header.Get("Status") == "408")
	require_True(t, m.Header.Get("Description") == "Empty Request")

	// Both set
	m = getMsg(&JSApiMsgGetRequest{Seq: 1, LastFor: "foo"})
	require_True(t, len(m.Data) == 0)
	require_True(t, m.Header.Get("Status") == "408")
	require_True(t, m.Header.Get("Description") == "Bad Request")

	// Not found
	m = getMsg(&JSApiMsgGetRequest{LastFor: "foobar"})
	require_True(t, len(m.Data) == 0)
	require_True(t, m.Header.Get("Status") == "404")
	require_True(t, m.Header.Get("Description") == "Message Not Found")

	m = getMsg(&JSApiMsgGetRequest{Seq: 22})
	require_True(t, len(m.Data) == 0)
	require_True(t, m.Header.Get("Status") == "404")
	require_True(t, m.Header.Get("Description") == "Message Not Found")
}

// This allows support for a get next given a sequence as a starting.
// This allows these to be chained together if needed for sparse streams.
func TestJetStreamDirectMsgGetNext(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:        "DSMG",
		Storage:     MemoryStorage,
		Subjects:    []string{"foo", "bar", "baz"},
		AllowDirect: true,
	}
	addStream(t, nc, cfg)

	sendStreamMsg(t, nc, "foo", "foo")
	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "bar", "bar")
	}
	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "baz", "baz")
	}
	sendStreamMsg(t, nc, "foo", "foo")

	getSubj := fmt.Sprintf(JSDirectMsgGetT, "DSMG")
	getMsg := func(seq uint64, subj string) *nats.Msg {
		req := []byte(fmt.Sprintf(`{"seq": %d, "next_by_subj": %q}`, seq, subj))
		m, err := nc.Request(getSubj, req, time.Second)
		require_NoError(t, err)
		return m
	}

	m := getMsg(0, "foo")
	require_True(t, m.Header.Get(JSSequence) == "1")
	require_True(t, m.Header.Get(JSSubject) == "foo")

	m = getMsg(1, "foo")
	require_True(t, m.Header.Get(JSSequence) == "1")
	require_True(t, m.Header.Get(JSSubject) == "foo")

	m = getMsg(2, "foo")
	require_True(t, m.Header.Get(JSSequence) == "22")
	require_True(t, m.Header.Get(JSSubject) == "foo")

	m = getMsg(2, "bar")
	require_True(t, m.Header.Get(JSSequence) == "2")
	require_True(t, m.Header.Get(JSSubject) == "bar")

	m = getMsg(5, "baz")
	require_True(t, m.Header.Get(JSSequence) == "12")
	require_True(t, m.Header.Get(JSSubject) == "baz")

	m = getMsg(14, "baz")
	require_True(t, m.Header.Get(JSSequence) == "14")
	require_True(t, m.Header.Get(JSSubject) == "baz")
}

func TestJetStreamConsumerAndStreamNamesWithPathSeparators(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "usr/bin"})
	require_Error(t, err, NewJSStreamNameContainsPathSeparatorsError(), nats.ErrInvalidStreamName)
	_, err = js.AddStream(&nats.StreamConfig{Name: `Documents\readme.txt`})
	require_Error(t, err, NewJSStreamNameContainsPathSeparatorsError(), nats.ErrInvalidStreamName)

	// Now consumers.
	_, err = js.AddStream(&nats.StreamConfig{Name: "T"})
	require_NoError(t, err)

	_, err = js.AddConsumer("T", &nats.ConsumerConfig{Durable: "a/b", AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err, NewJSConsumerNameContainsPathSeparatorsError(), nats.ErrInvalidConsumerName)

	_, err = js.AddConsumer("T", &nats.ConsumerConfig{Durable: `a\b`, AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err, NewJSConsumerNameContainsPathSeparatorsError(), nats.ErrInvalidConsumerName)
}

func TestJetStreamConsumerUpdateFilterSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "T", Subjects: []string{"foo", "bar", "baz"}})
	require_NoError(t, err)

	// 10 foo
	for i := 0; i < 10; i++ {
		js.PublishAsync("foo", []byte("OK"))
	}
	// 20 bar
	for i := 0; i < 20; i++ {
		js.PublishAsync("bar", []byte("OK"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	sub, err := js.PullSubscribe("foo", "d")
	require_NoError(t, err)

	// Consume 5 msgs
	msgs, err := sub.Fetch(5)
	require_NoError(t, err)
	require_True(t, len(msgs) == 5)

	// Now update to different filter subject.
	_, err = js.UpdateConsumer("T", &nats.ConsumerConfig{
		Durable:       "d",
		FilterSubject: "bar",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	sub, err = js.PullSubscribe("bar", "d")
	require_NoError(t, err)

	msgs, err = sub.Fetch(1)
	require_NoError(t, err)

	// Make sure meta and pending etc are all correct.
	m := msgs[0]
	meta, err := m.Metadata()
	require_NoError(t, err)

	if meta.Sequence.Consumer != 6 || meta.Sequence.Stream != 11 {
		t.Fatalf("Sequence incorrect %+v", meta.Sequence)
	}
	if meta.NumDelivered != 1 {
		t.Fatalf("Expected NumDelivered to be 1, got %d", meta.NumDelivered)
	}
	if meta.NumPending != 19 {
		t.Fatalf("Expected NumPending to be 19, got %d", meta.NumPending)
	}
}

// Originally pull consumers were FIFO with respect to the request, not delivery of messages.
// We have changed to have the behavior be FIFO but on an individual message basis.
// So after a message is delivered, the request, if still outstanding, effectively
// goes to the end of the queue of requests pending.
func TestJetStreamConsumerPullConsumerFIFO(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "T"})
	require_NoError(t, err)

	// Create pull consumer.
	_, err = js.AddConsumer("T", &nats.ConsumerConfig{Durable: "d", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// Simulate 10 pull requests each asking for 10 messages.
	var subs []*nats.Subscription
	for i := 0; i < 10; i++ {
		inbox := nats.NewInbox()
		sub := natsSubSync(t, nc, inbox)
		subs = append(subs, sub)
		req := &JSApiConsumerGetNextRequest{Batch: 10, Expires: 60 * time.Second}
		jreq, err := json.Marshal(req)
		require_NoError(t, err)
		err = nc.PublishRequest(fmt.Sprintf(JSApiRequestNextT, "T", "d"), inbox, jreq)
		require_NoError(t, err)
	}

	// Now send 100 messages.
	for i := 0; i < 100; i++ {
		js.PublishAsync("T", []byte("FIFO FTW!"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Wait for messages
	for index, sub := range subs {
		checkSubsPending(t, sub, 10)
		for i := 0; i < 10; i++ {
			m, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
			meta, err := m.Metadata()
			require_NoError(t, err)
			// We expect these to be FIFO per message. E.g. sub #1 = [1, 11, 21, 31, ..]
			if sseq := meta.Sequence.Stream; sseq != uint64(index+1+(10*i)) {
				t.Fatalf("Expected message #%d for sub #%d to be %d, but got %d", i+1, index+1, index+1+(10*i), sseq)
			}
		}
	}
}

// Make sure that when we reach an ack limit that we follow one shot semantics.
func TestJetStreamConsumerPullConsumerOneShotOnMaxAckLimit(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "T"})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		js.Publish("T", []byte("OK"))
	}

	sub, err := js.PullSubscribe("T", "d", nats.MaxAckPending(5))
	require_NoError(t, err)

	start := time.Now()
	msgs, err := sub.Fetch(10, nats.MaxWait(2*time.Second))
	require_NoError(t, err)

	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("Took too long, not one shot behavior: %v", elapsed)
	}

	if len(msgs) != 5 {
		t.Fatalf("Expected 5 msgs, got %d", len(msgs))
	}
}

///////////////////////////////////////////////////////////////////////////
// Simple JetStream Benchmarks
///////////////////////////////////////////////////////////////////////////

func Benchmark__JetStreamPubWithAck(b *testing.B) {
	s := RunBasicJetStreamServer(b)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: "foo"})
	if err != nil {
		b.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer nc.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nc.Request("foo", []byte("Hello World!"), 50*time.Millisecond)
	}
	b.StopTimer()

	state := mset.state()
	if int(state.Msgs) != b.N {
		b.Fatalf("Expected %d messages, got %d", b.N, state.Msgs)
	}
}

func Benchmark____JetStreamPubNoAck(b *testing.B) {
	s := RunBasicJetStreamServer(b)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: "foo"})
	if err != nil {
		b.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer nc.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := nc.Publish("foo", []byte("Hello World!")); err != nil {
			b.Fatalf("Unexpected error: %v", err)
		}
	}
	nc.Flush()
	b.StopTimer()

	state := mset.state()
	if int(state.Msgs) != b.N {
		b.Fatalf("Expected %d messages, got %d", b.N, state.Msgs)
	}
}

func Benchmark_JetStreamPubAsyncAck(b *testing.B) {
	s := RunBasicJetStreamServer(b)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: "foo"})
	if err != nil {
		b.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc, err := nats.Connect(s.ClientURL(), nats.NoReconnect())
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer nc.Close()

	// Put ack stream on its own connection.
	anc, err := nats.Connect(s.ClientURL())
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer anc.Close()

	acks := nats.NewInbox()
	sub, _ := anc.Subscribe(acks, func(m *nats.Msg) {
		// Just eat them for this test.
	})
	// set max pending to unlimited.
	sub.SetPendingLimits(-1, -1)
	defer sub.Unsubscribe()

	anc.Flush()
	runtime.GC()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := nc.PublishRequest("foo", acks, []byte("Hello World!")); err != nil {
			b.Fatalf("[%d] Unexpected error: %v", i, err)
		}
	}
	nc.Flush()
	b.StopTimer()

	state := mset.state()
	if int(state.Msgs) != b.N {
		b.Fatalf("Expected %d messages, got %d", b.N, state.Msgs)
	}
}

func Benchmark____JetStreamSubNoAck(b *testing.B) {
	if b.N < 10000 {
		return
	}

	s := RunBasicJetStreamServer(b)
	defer s.Shutdown()

	mname := "foo"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname})
	if err != nil {
		b.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc, err := nats.Connect(s.ClientURL(), nats.NoReconnect())
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer nc.Close()

	// Queue up messages.
	for i := 0; i < b.N; i++ {
		nc.Publish(mname, []byte("Hello World!"))
	}
	nc.Flush()

	state := mset.state()
	if state.Msgs != uint64(b.N) {
		b.Fatalf("Expected %d messages, got %d", b.N, state.Msgs)
	}

	total := int32(b.N)
	received := int32(0)
	done := make(chan bool)

	deliverTo := "DM"
	oname := "O"

	nc.Subscribe(deliverTo, func(m *nats.Msg) {
		// We only are done when we receive all, we could check for gaps too.
		if atomic.AddInt32(&received, 1) >= total {
			done <- true
		}
	})
	nc.Flush()

	b.ResetTimer()
	o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: deliverTo, Durable: oname, AckPolicy: AckNone})
	if err != nil {
		b.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()
	<-done
	b.StopTimer()
}

func benchJetStreamWorkersAndBatch(b *testing.B, numWorkers, batchSize int) {
	// Avoid running at too low of numbers since that chews up memory and GC.
	if b.N < numWorkers*batchSize {
		return
	}

	s := RunBasicJetStreamServer(b)
	defer s.Shutdown()

	mname := "MSET22"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname})
	if err != nil {
		b.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc, err := nats.Connect(s.ClientURL(), nats.NoReconnect())
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer nc.Close()

	// Queue up messages.
	for i := 0; i < b.N; i++ {
		nc.Publish(mname, []byte("Hello World!"))
	}
	nc.Flush()

	state := mset.state()
	if state.Msgs != uint64(b.N) {
		b.Fatalf("Expected %d messages, got %d", b.N, state.Msgs)
	}

	// Create basic work queue mode consumer.
	oname := "WQ"
	o, err := mset.addConsumer(&ConsumerConfig{Durable: oname, AckPolicy: AckExplicit})
	if err != nil {
		b.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	total := int32(b.N)
	received := int32(0)
	start := make(chan bool)
	done := make(chan bool)

	batchSizeMsg := []byte(strconv.Itoa(batchSize))
	reqNextMsgSubj := o.requestNextMsgSubject()

	for i := 0; i < numWorkers; i++ {
		nc, err := nats.Connect(s.ClientURL(), nats.NoReconnect())
		if err != nil {
			b.Fatalf("Failed to create client: %v", err)
		}
		defer nc.Close()

		deliverTo := nats.NewInbox()
		nc.Subscribe(deliverTo, func(m *nats.Msg) {
			if atomic.AddInt32(&received, 1) >= total {
				done <- true
			}
			// Ack + Next request.
			nc.PublishRequest(m.Reply, deliverTo, AckNext)
		})
		nc.Flush()
		go func() {
			<-start
			nc.PublishRequest(reqNextMsgSubj, deliverTo, batchSizeMsg)
		}()
	}

	b.ResetTimer()
	close(start)
	<-done
	b.StopTimer()
}

func Benchmark___JetStream1x1Worker(b *testing.B) {
	benchJetStreamWorkersAndBatch(b, 1, 1)
}

func Benchmark__JetStream1x1kWorker(b *testing.B) {
	benchJetStreamWorkersAndBatch(b, 1, 1024)
}

func Benchmark_JetStream10x1kWorker(b *testing.B) {
	benchJetStreamWorkersAndBatch(b, 10, 1024)
}

func Benchmark_JetStream4x512Worker(b *testing.B) {
	benchJetStreamWorkersAndBatch(b, 4, 512)
}

func TestJetStreamKVMemoryStorePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TEST", History: 1, Storage: nats.MemoryStorage})
	require_NoError(t, err)

	start := time.Now()
	for i := 0; i < 100_000; i++ {
		_, err := kv.PutString(fmt.Sprintf("foo.%d", i), "HELLO")
		require_NoError(t, err)
	}
	fmt.Printf("Took %v for first run\n", time.Since(start))

	start = time.Now()
	for i := 0; i < 100_000; i++ {
		_, err := kv.PutString(fmt.Sprintf("foo.%d", i), "HELLO WORLD")
		require_NoError(t, err)
	}
	fmt.Printf("Took %v for second run\n", time.Since(start))

	start = time.Now()
	for i := 0; i < 100_000; i++ {
		_, err := kv.Get(fmt.Sprintf("foo.%d", i))
		require_NoError(t, err)
	}
	fmt.Printf("Took %v for get\n", time.Since(start))
}

func TestJetStreamKVMemoryStoreDirectGetPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &StreamConfig{
		Name:        "TEST",
		Storage:     MemoryStorage,
		Subjects:    []string{"foo.*"},
		MaxMsgsPer:  1,
		AllowDirect: true,
	}
	addStream(t, nc, cfg)

	start := time.Now()
	for i := 0; i < 100_000; i++ {
		_, err := js.Publish(fmt.Sprintf("foo.%d", i), []byte("HELLO"))
		require_NoError(t, err)
	}
	fmt.Printf("Took %v for put\n", time.Since(start))

	getSubj := fmt.Sprintf(JSDirectMsgGetT, "TEST")

	const tmpl = "{\"last_by_subj\":%q}"

	start = time.Now()
	for i := 0; i < 100_000; i++ {
		req := []byte(fmt.Sprintf(tmpl, fmt.Sprintf("foo.%d", i)))
		_, err := nc.Request(getSubj, req, time.Second)
		require_NoError(t, err)
	}
	fmt.Printf("Took %v for get\n", time.Since(start))
}

func TestJetStreamMultiplePullPerf(t *testing.T) {
	skip(t)

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	js.AddStream(&nats.StreamConfig{Name: "mp22", Storage: nats.FileStorage})
	defer js.DeleteStream("mp22")

	n, msg := 1_000_000, []byte("OK")
	for i := 0; i < n; i++ {
		js.PublishAsync("mp22", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(10 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	si, err := js.StreamInfo("mp22")
	require_NoError(t, err)

	fmt.Printf("msgs: %d, total_bytes: %v\n", si.State.Msgs, friendlyBytes(int64(si.State.Bytes)))

	// 10 pull subscribers each asking for 100 msgs.
	_, err = js.AddConsumer("mp22", &nats.ConsumerConfig{
		Durable:       "d",
		MaxAckPending: 8_000,
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	startCh := make(chan bool)
	var wg sync.WaitGroup

	np, bs := 10, 100

	count := 0

	for i := 0; i < np; i++ {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		sub, err := js.PullSubscribe("mp22", "d")
		require_NoError(t, err)

		wg.Add(1)
		go func(sub *nats.Subscription) {
			defer wg.Done()
			<-startCh
			for i := 0; i < n/(np*bs); i++ {
				msgs, err := sub.Fetch(bs)
				if err != nil {
					t.Logf("Got error on pull: %v", err)
					return
				}
				if len(msgs) != bs {
					t.Logf("Expected %d msgs, got %d", bs, len(msgs))
					return
				}
				count += len(msgs)
				for _, m := range msgs {
					m.Ack()
				}
			}
		}(sub)
	}

	start := time.Now()
	close(startCh)
	wg.Wait()

	tt := time.Since(start)
	fmt.Printf("Took %v to receive %d msgs [%d]\n", tt, n, count)
	fmt.Printf("%.0f msgs/s\n", float64(n)/tt.Seconds())
	fmt.Printf("%.0f mb/s\n\n", float64(si.State.Bytes/(1024*1024))/tt.Seconds())
}

func TestJetStreamMirrorUpdatesNotSupported(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "SOURCE"})
	require_NoError(t, err)

	cfg := &nats.StreamConfig{
		Name:   "M",
		Mirror: &nats.StreamSource{Name: "SOURCE"},
	}
	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	cfg.Mirror = nil
	_, err = js.UpdateStream(cfg)
	require_Error(t, err, NewJSStreamMirrorNotUpdatableError())
}

func TestJetStreamMirrorFirstSeqNotSupported(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	_, err := s.gacc.addStream(&StreamConfig{Name: "SOURCE"})
	require_NoError(t, err)

	cfg := &StreamConfig{
		Name:     "M",
		Mirror:   &StreamSource{Name: "SOURCE"},
		FirstSeq: 123,
	}
	_, err = s.gacc.addStream(cfg)
	require_Error(t, err, NewJSMirrorWithFirstSeqError())
}

func TestJetStreamDirectGetBySubject(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}

		ONLYME = {
			publish = { allow = "$JS.API.DIRECT.GET.KV.vid.22.>"}
		}

		accounts: {
			A: {
				jetstream: enabled
				users: [
					{ user: admin, password: s3cr3t },
					{ user: user, password: pwd, permissions: $ONLYME},
				]
			},
		}
	`))

	s, _ := RunServerWithConfig(conf)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s, nats.UserInfo("admin", "s3cr3t"))
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:        "KV",
		Storage:     MemoryStorage,
		Subjects:    []string{"vid.*.>"},
		MaxMsgsPer:  1,
		AllowDirect: true,
	}
	addStream(t, nc, cfg)

	// Add in mirror as well.
	cfg = &StreamConfig{
		Name:         "M",
		Storage:      MemoryStorage,
		Mirror:       &StreamSource{Name: "KV"},
		MirrorDirect: true,
	}
	addStream(t, nc, cfg)

	v22 := "vid.22.speed"
	v33 := "vid.33.speed"
	_, err := js.Publish(v22, []byte("100"))
	require_NoError(t, err)
	_, err = js.Publish(v33, []byte("55"))
	require_NoError(t, err)

	// User the restricted user.
	nc, _ = jsClientConnect(t, s, nats.UserInfo("user", "pwd"))
	defer nc.Close()

	errCh := make(chan error, 10)
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) {
		select {
		case errCh <- e:
		default:
		}
	})

	getSubj := fmt.Sprintf(JSDirectGetLastBySubjectT, "KV", v22)
	m, err := nc.Request(getSubj, nil, time.Second)
	require_NoError(t, err)
	require_True(t, string(m.Data) == "100")

	// Now attempt to access vid 33 data..
	getSubj = fmt.Sprintf(JSDirectGetLastBySubjectT, "KV", v33)
	_, err = nc.Request(getSubj, nil, 200*time.Millisecond)
	require_Error(t, err) // timeout here.

	select {
	case e := <-errCh:
		if !strings.HasPrefix(e.Error(), "nats: Permissions Violation") {
			t.Fatalf("Expected a permissions violation but got %v", e)
		}
	case <-time.After(time.Second):
		t.Fatalf("Expected to get a permissions error, got none")
	}

	// Now make sure mirrors are doing right thing with new way as well.
	var sawMirror bool
	getSubj = fmt.Sprintf(JSDirectGetLastBySubjectT, "KV", v22)
	for i := 0; i < 100; i++ {
		m, err := nc.Request(getSubj, nil, time.Second)
		require_NoError(t, err)
		if shdr := m.Header.Get(JSStream); shdr == "M" {
			sawMirror = true
			break
		}
	}
	if !sawMirror {
		t.Fatalf("Expected to see the mirror respond at least once")
	}
}

func TestJetStreamProperErrorDueToOverlapSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	test := func(t *testing.T, s *Server) {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo.*"},
		})
		require_NoError(t, err)

		// Now do this by end since we want to check the error returned.
		sc := &nats.StreamConfig{
			Name:     "TEST2",
			Subjects: []string{"foo.>"},
		}
		req, _ := json.Marshal(sc)
		msg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, sc.Name), req, time.Second)
		require_NoError(t, err)

		var scResp JSApiStreamCreateResponse
		err = json.Unmarshal(msg.Data, &scResp)
		require_NoError(t, err)

		if scResp.Error == nil || !IsNatsErr(scResp.Error, JSStreamSubjectOverlapErr) {
			t.Fatalf("Did not receive correct error: %+v", scResp)
		}
	}

	t.Run("standalone", func(t *testing.T) { test(t, s) })
	t.Run("clustered", func(t *testing.T) { test(t, c.randomServer()) })
}

func TestJetStreamServerCipherConvert(t *testing.T) {
	tmpl := `
		server_name: S22
		listen: 127.0.0.1:-1
		jetstream: {key: s3cr3t, store_dir: '%s', cipher: %s}
	`
	storeDir := t.TempDir()

	// Create a stream and a consumer under one cipher, and restart the server with a new cipher.
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, storeDir, "AES")))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 1000; i++ {
		msg := []byte(fmt.Sprintf("TOP SECRET DOCUMENT #%d", i+1))
		_, err := js.Publish("foo", msg)
		require_NoError(t, err)
	}

	// Make sure consumers convert as well.
	sub, err := js.PullSubscribe("foo", "dlc")
	require_NoError(t, err)
	for _, m := range fetchMsgs(t, sub, 100, 5*time.Second) {
		m.AckSync()
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	ci, err := js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)

	// Stop current
	s.Shutdown()

	conf = createConfFile(t, []byte(fmt.Sprintf(tmpl, storeDir, "ChaCha")))

	s, _ = RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si2, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	if !reflect.DeepEqual(si, si2) {
		t.Fatalf("Stream infos did not match\n%+v\nvs\n%+v", si, si2)
	}

	ci2, err := js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)

	// Consumer create times can be slightly off after restore from disk.
	now := time.Now()
	ci.Created, ci2.Created = now, now
	ci.Delivered.Last, ci2.Delivered.Last = nil, nil
	ci.AckFloor.Last, ci2.AckFloor.Last = nil, nil
	// Also clusters will be different.
	ci.Cluster, ci2.Cluster = nil, nil

	if !reflect.DeepEqual(ci, ci2) {
		t.Fatalf("Consumer infos did not match\n%+v\nvs\n%+v", ci, ci2)
	}
}

func TestJetStreamConsumerDeliverNewMaxRedeliveriesAndServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*"},
	})
	require_NoError(t, err)

	inbox := nats.NewInbox()
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverSubject: inbox,
		Durable:        "dur",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverPolicy:  nats.DeliverNewPolicy,
		MaxDeliver:     3,
		AckWait:        250 * time.Millisecond,
		FilterSubject:  "foo.bar",
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "foo.bar", "msg")

	sub := natsSubSync(t, nc, inbox)
	for i := 0; i < 3; i++ {
		natsNexMsg(t, sub, time.Second)
	}
	// Now check that there is no more redeliveries
	if msg, err := sub.NextMsg(300 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Expected timeout, got msg=%+v err=%v", msg, err)
	}

	// Give a chance to things to be persisted
	time.Sleep(300 * time.Millisecond)

	// Check server restart
	nc.Close()
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, _ = jsClientConnect(t, s)
	defer nc.Close()

	sub = natsSubSync(t, nc, inbox)
	// We should not have messages being redelivered.
	if msg, err := sub.NextMsg(300 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Expected timeout, got msg=%+v err=%v", msg, err)
	}
}

func TestJetStreamConsumerPendingLowerThanStreamFirstSeq(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	inbox := nats.NewInbox()
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverSubject: inbox,
		Durable:        "dur",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverPolicy:  nats.DeliverAllPolicy,
	})
	require_NoError(t, err)

	sub := natsSubSync(t, nc, inbox)
	for i := 0; i < 10; i++ {
		natsNexMsg(t, sub, time.Second)
	}

	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("dur")
	require_True(t, o != nil)
	o.stop()
	mset.store.Compact(1_000_000)
	nc.Close()

	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.FirstSeq == 1_000_000)
	require_True(t, si.State.LastSeq == 999_999)

	natsSubSync(t, nc, inbox)
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "dur")
		if err != nil {
			return err
		}
		if ci.NumAckPending != 0 {
			return fmt.Errorf("NumAckPending should be 0, got %v", ci.NumAckPending)
		}
		if ci.Delivered.Stream != 999_999 {
			return fmt.Errorf("Delivered.Stream should be 999,999, got %v", ci.Delivered.Stream)
		}
		return nil
	})
}

func TestJetStreamAllowDirectAfterUpdate(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
	})
	require_NoError(t, err)
	sendStreamMsg(t, nc, "foo", "msg")

	si, err := js.UpdateStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"*"},
		AllowDirect: true,
	})
	require_NoError(t, err)
	require_True(t, si.Config.AllowDirect)

	_, err = js.GetLastMsg("TEST", "foo", nats.DirectGet(), nats.MaxWait(100*time.Millisecond))
	require_NoError(t, err)

	// Make sure turning off works too.
	si, err = js.UpdateStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"*"},
		AllowDirect: false,
	})
	require_NoError(t, err)
	require_False(t, si.Config.AllowDirect)

	_, err = js.GetLastMsg("TEST", "foo", nats.DirectGet(), nats.MaxWait(100*time.Millisecond))
	require_Error(t, err)
}

// Bug when stream's consumer config does not force filestore to track per subject information.
func TestJetStreamConsumerEOFBugNewFileStore(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.bar.*"},
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:   "M",
		Mirror: &nats.StreamSource{Name: "TEST"},
	})
	require_NoError(t, err)

	dsubj := nats.NewInbox()
	sub, err := nc.SubscribeSync(dsubj)
	require_NoError(t, err)
	nc.Flush()

	// Filter needs to be a wildcard. Need to bind to the
	_, err = js.AddConsumer("M", &nats.ConsumerConfig{DeliverSubject: dsubj, FilterSubject: "foo.>"})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		_, err := js.PublishAsync("foo.bar.baz", []byte("OK"))
		require_NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		m.Respond(nil)
	}

	// Now force an expiration.
	mset, err := s.GlobalAccount().lookupStream("M")
	require_NoError(t, err)
	mset.mu.RLock()
	store := mset.store.(*fileStore)
	mset.mu.RUnlock()
	store.mu.RLock()
	mb := store.blks[0]
	store.mu.RUnlock()
	mb.mu.Lock()
	mb.fss = nil
	mb.mu.Unlock()

	// Now send another message.
	_, err = js.PublishAsync("foo.bar.baz", []byte("OK"))
	require_NoError(t, err)

	// This will fail with the bug.
	_, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
}

func TestJetStreamSubjectBasedFilteredConsumers(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts: {
			A: {
				jetstream: enabled
				users: [ {
					user: u,
					password: p
					permissions {
						publish {
							allow: [
								'ID.>',
								'$JS.API.INFO',
								'$JS.API.STREAM.>',
								'$JS.API.CONSUMER.INFO.>',
								'$JS.API.CONSUMER.CREATE.TEST.VIN-xxx.ID.foo.>', # Only allow ID.foo.
							]
							deny: [ '$JS.API.CONSUMER.CREATE.*', '$JS.API.CONSUMER.DURABLE.CREATE.*.*']
						}
					}
				} ]
			},
		}
	`))

	s, _ := RunServerWithConfig(conf)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s, nats.UserInfo("u", "p"), nats.ErrorHandler(noOpErrHandler))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"ID.*.*"},
	})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		js.Publish(fmt.Sprintf("ID.foo.%d", i*3), nil)
		js.Publish(fmt.Sprintf("ID.bar.%d", i*3+1), nil)
		js.Publish(fmt.Sprintf("ID.baz.%d", i*3+2), nil)
	}
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 300)

	// Trying to create a consumer with non filtered API should fail.
	js, err = nc.JetStream(nats.MaxWait(200 * time.Millisecond))
	require_NoError(t, err)

	_, err = js.SubscribeSync("ID.foo.*")
	require_Error(t, err, nats.ErrTimeout, context.DeadlineExceeded)

	_, err = js.SubscribeSync("ID.foo.*", nats.Durable("dlc"))
	require_Error(t, err, nats.ErrTimeout, context.DeadlineExceeded)

	// Direct filtered should work.
	// Need to do by hand for now.
	ecSubj := fmt.Sprintf(JSApiConsumerCreateExT, "TEST", "VIN-xxx", "ID.foo.*")

	crReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			DeliverPolicy: DeliverLast,
			FilterSubject: "ID.foo.*",
			AckPolicy:     AckExplicit,
		},
	}
	req, err := json.Marshal(crReq)
	require_NoError(t, err)

	resp, err := nc.Request(ecSubj, req, 500*time.Millisecond)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	if ccResp.Error != nil {
		t.Fatalf("Unexpected error: %v", ccResp.Error)
	}
	cfg := ccResp.Config
	ci := ccResp.ConsumerInfo
	// Make sure we recognized as an ephemeral (since no durable was set) and that we have an InactiveThreshold.
	// Make sure we captured preferred ephemeral name.
	if ci.Name != "VIN-xxx" {
		t.Fatalf("Did not get correct name, expected %q got %q", "xxx", ci.Name)
	}
	if cfg.InactiveThreshold == 0 {
		t.Fatalf("Expected default inactive threshold to be set, got %v", cfg.InactiveThreshold)
	}

	// Make sure we can not use different consumer name since locked above.
	ecSubj = fmt.Sprintf(JSApiConsumerCreateExT, "TEST", "VIN-zzz", "ID.foo.*")
	_, err = nc.Request(ecSubj, req, 500*time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)

	// Now check that we error when we mismatch filtersubject.
	crReq = CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			DeliverPolicy: DeliverLast,
			FilterSubject: "ID.bar.*",
			AckPolicy:     AckExplicit,
		},
	}
	req, err = json.Marshal(crReq)
	require_NoError(t, err)

	ecSubj = fmt.Sprintf(JSApiConsumerCreateExT, "TEST", "VIN-xxx", "ID.foo.*")
	resp, err = nc.Request(ecSubj, req, 500*time.Millisecond)
	require_NoError(t, err)
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	checkNatsError(t, ccResp.Error, JSConsumerCreateFilterSubjectMismatchErr)

	// Now make sure if we change subject to match that we can not create a filtered consumer on ID.bar.>
	ecSubj = fmt.Sprintf(JSApiConsumerCreateExT, "TEST", "VIN-xxx", "ID.bar.*")
	_, err = nc.Request(ecSubj, req, 500*time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)
}

func TestJetStreamStreamSubjectsOverlap(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*", "foo.A"},
	})
	require_Error(t, err)
	require_True(t, strings.Contains(err.Error(), "overlaps"))

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*"},
	})
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*", "foo.A"},
	})
	require_Error(t, err)
	require_True(t, strings.Contains(err.Error(), "overlaps"))
}

func TestJetStreamSuppressAllowDirect(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"key.*"},
		MaxMsgsPerSubject: 1,
		AllowDirect:       true,
	})
	require_NoError(t, err)
	require_True(t, si.Config.AllowDirect)

	si, err = js.UpdateStream(&nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"key.*"},
		MaxMsgsPerSubject: 1,
		AllowDirect:       false,
	})
	require_NoError(t, err)
	require_False(t, si.Config.AllowDirect)

	sendStreamMsg(t, nc, "key.22", "msg")

	_, err = js.GetLastMsg("TEST", "foo", nats.DirectGet(), nats.MaxWait(100*time.Millisecond))
	require_Error(t, err)
}

func TestJetStreamPullConsumerNoAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"ORDERS.*"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "dlc",
		AckPolicy: nats.AckNonePolicy,
	})
	require_NoError(t, err)
}

func TestJetStreamAccountPurge(t *testing.T) {
	sysKp, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
	sysCreds := newUser(t, sysKp)
	accKp, accpub := createKey(t)
	accClaim := jwt.NewAccountClaims(accpub)
	accClaim.Limits.JetStreamLimits.DiskStorage = 1024 * 1024 * 5
	accClaim.Limits.JetStreamLimits.MemoryStorage = 1024 * 1024 * 5
	accJwt := encodeClaim(t, accClaim, accpub)
	accCreds := newUser(t, accKp)

	storeDir := t.TempDir()

	cfg := createConfFile(t, []byte(fmt.Sprintf(`
        host: 127.0.0.1
        port:-1
        server_name: S1
        operator: %s
        system_account: %s
        resolver: {
                type: full
                dir: '%s/jwt'
        }
        jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s/js'}
`, ojwt, syspub, storeDir, storeDir)))
	defer os.Remove(cfg)

	s, o := RunServerWithConfig(cfg)
	updateJwt(t, s.ClientURL(), sysCreds, sysJwt, 1)
	updateJwt(t, s.ClientURL(), sysCreds, accJwt, 1)
	defer s.Shutdown()

	inspectDirs := func(t *testing.T, accTotal int) error {
		t.Helper()
		if accTotal == 0 {
			files, err := os.ReadDir(filepath.Join(o.StoreDir, "jetstream", accpub))
			require_True(t, len(files) == accTotal || err != nil)
		} else {
			files, err := os.ReadDir(filepath.Join(o.StoreDir, "jetstream", accpub, "streams"))
			require_NoError(t, err)
			require_True(t, len(files) == accTotal)
		}
		return nil
	}

	createTestData := func() {
		nc := natsConnect(t, s.ClientURL(), nats.UserCredentials(accCreds))
		defer nc.Close()
		js, err := nc.JetStream()
		require_NoError(t, err)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "TEST1",
			Subjects: []string{"foo"},
		})
		require_NoError(t, err)
		_, err = js.AddConsumer("TEST1",
			&nats.ConsumerConfig{Durable: "DUR1",
				AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
	}

	purge := func(t *testing.T) {
		t.Helper()
		var resp JSApiAccountPurgeResponse
		ncsys := natsConnect(t, s.ClientURL(), nats.UserCredentials(sysCreds))
		defer ncsys.Close()
		m, err := ncsys.Request(fmt.Sprintf(JSApiAccountPurgeT, accpub), nil, 5*time.Second)
		require_NoError(t, err)
		err = json.Unmarshal(m.Data, &resp)
		require_NoError(t, err)
		require_True(t, resp.Initiated)
	}

	createTestData()
	inspectDirs(t, 1)
	purge(t)
	inspectDirs(t, 0)
	createTestData()
	inspectDirs(t, 1)

	s.Shutdown()
	require_NoError(t, os.Remove(storeDir+"/jwt/"+accpub+".jwt"))

	s, o = RunServerWithConfig(o.ConfigFile)
	defer s.Shutdown()
	inspectDirs(t, 1)
	purge(t)
	inspectDirs(t, 0)
}

func TestJetStreamPullConsumerLastPerSubjectRedeliveries(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>"},
	})
	require_NoError(t, err)

	for i := 0; i < 20; i++ {
		sendStreamMsg(t, nc, fmt.Sprintf("foo.%v", i), "msg")
	}

	// Create a pull sub with a maxackpending that is <= of the number of
	// messages in the stream and as much as we are going to Fetch() below.
	sub, err := js.PullSubscribe(">", "dur",
		nats.AckExplicit(),
		nats.BindStream("TEST"),
		nats.DeliverLastPerSubject(),
		nats.MaxAckPending(10),
		nats.MaxRequestBatch(10),
		nats.AckWait(250*time.Millisecond))
	require_NoError(t, err)

	// Fetch the max number of message we can get, and don't ack them.
	_, err = sub.Fetch(10, nats.MaxWait(time.Second))
	require_NoError(t, err)

	// Wait for more than redelivery time.
	time.Sleep(500 * time.Millisecond)

	// Fetch again, make sure we can get those 10 messages.
	msgs, err := sub.Fetch(10, nats.MaxWait(time.Second))
	require_NoError(t, err)
	require_True(t, len(msgs) == 10)
	// Make sure those were the first 10 messages
	for i, m := range msgs {
		if m.Subject != fmt.Sprintf("foo.%v", i) {
			t.Fatalf("Expected message for subject foo.%v, got %v", i, m.Subject)
		}
		m.Ack()
	}
}

func TestJetStreamPullConsumersTimeoutHeaders(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "dlc",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	nc.Publish("foo.foo", []byte("foo"))
	nc.Publish("foo.bar", []byte("bar"))
	nc.Publish("foo.else", []byte("baz"))
	nc.Flush()

	// We will do low level requests by hand for this test as to not depend on any client impl.
	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", "dlc")

	maxBytes := 1024
	batch := 50
	req := &JSApiConsumerGetNextRequest{Batch: batch, Expires: 100 * time.Millisecond, NoWait: false, MaxBytes: maxBytes}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)
	// Create listener.
	reply, msgs := nats.NewInbox(), make(chan *nats.Msg, batch)
	sub, err := nc.ChanSubscribe(reply, msgs)
	require_NoError(t, err)
	defer sub.Unsubscribe()

	// Send request.
	err = nc.PublishRequest(rsubj, reply, jreq)
	require_NoError(t, err)

	bytesReceived := 0
	messagesReceived := 0

	checkHeaders := func(expectedStatus, expectedDesc string, m *nats.Msg) {
		t.Helper()
		if value := m.Header.Get("Status"); value != expectedStatus {
			t.Fatalf("Expected status %q, got %q", expectedStatus, value)
		}
		if value := m.Header.Get("Description"); value != expectedDesc {
			t.Fatalf("Expected description %q, got %q", expectedDesc, value)
		}
		if value := m.Header.Get(JSPullRequestPendingMsgs); value != fmt.Sprint(batch-messagesReceived) {
			t.Fatalf("Expected %d messages, got %s", batch-messagesReceived, value)
		}
		if value := m.Header.Get(JSPullRequestPendingBytes); value != fmt.Sprint(maxBytes-bytesReceived) {
			t.Fatalf("Expected %d bytes, got %s", maxBytes-bytesReceived, value)
		}
	}

	for done := false; !done; {
		select {
		case m := <-msgs:
			if len(m.Data) == 0 && m.Header != nil {
				checkHeaders("408", "Request Timeout", m)
				done = true
			} else {
				messagesReceived += 1
				bytesReceived += (len(m.Data) + len(m.Header) + len(m.Reply) + len(m.Subject))
			}
		case <-time.After(100 + 250*time.Millisecond):
			t.Fatalf("Did not receive all the msgs in time")
		}
	}

	// Now resend the request but then shutdown the server and
	// make sure we have the same info.
	err = nc.PublishRequest(rsubj, reply, jreq)
	require_NoError(t, err)
	natsFlush(t, nc)

	s.Shutdown()

	// It is possible that the client did not receive, so let's not fail
	// on that. But if the 409 indicating the the server is shutdown
	// is received, then it should have the new headers.
	messagesReceived, bytesReceived = 0, 0
	select {
	case m := <-msgs:
		checkHeaders("409", "Server Shutdown", m)
	case <-time.After(500 * time.Millisecond):
		// we can't fail for that.
		t.Logf("Subscription did not receive the pull request response on server shutdown")
	}
}

// For issue https://github.com/nats-io/nats-server/issues/3612
// Do auto cleanup.
func TestJetStreamDanglingMessageAutoCleanup(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "dlc", nats.MaxAckPending(10))
	require_NoError(t, err)

	// Send 100 msgs
	n := 100
	for i := 0; i < n; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	// Grab and ack 10 messages.
	for _, m := range fetchMsgs(t, sub, 10, time.Second) {
		m.AckSync()
	}

	ci, err := sub.ConsumerInfo()
	require_NoError(t, err)
	require_True(t, ci.AckFloor.Stream == 10)

	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()

	// We will hand move the ackfloor to simulate dangling message condition.
	cstore := filepath.Join(sd, "$G", "streams", "TEST", "obs", "dlc", "o.dat")

	buf, err := os.ReadFile(cstore)
	require_NoError(t, err)

	state, err := decodeConsumerState(buf)
	require_NoError(t, err)

	// Update from 10 for delivered and ack to 90.
	state.Delivered.Stream, state.Delivered.Consumer = 90, 90
	state.AckFloor.Stream, state.AckFloor.Consumer = 90, 90

	err = os.WriteFile(cstore, encodeConsumerState(state), defaultFilePerms)
	require_NoError(t, err)

	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	if si.State.Msgs != 10 {
		t.Fatalf("Expected auto-cleanup to have worked but got %d msgs vs 10", si.State.Msgs)
	}
}

// Issue https://github.com/nats-io/nats-server/issues/3645
func TestJetStreamMsgIDHeaderCollision(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"ORDERS.*"},
	})
	require_NoError(t, err)

	m := nats.NewMsg("ORDERS.test")
	m.Header.Add(JSMsgId, "1")
	m.Data = []byte("ok")

	_, err = js.PublishMsg(m)
	require_NoError(t, err)

	m.Header = make(nats.Header)
	m.Header.Add("Orig-Nats-Msg-Id", "1")

	_, err = js.PublishMsg(m)
	require_NoError(t, err)

	m.Header = make(nats.Header)
	m.Header.Add("Original-Nats-Msg-Id", "1")

	_, err = js.PublishMsg(m)
	require_NoError(t, err)

	m.Header = make(nats.Header)
	m.Header.Add("Original-Nats-Msg-Id", "1")
	m.Header.Add("Really-Original-Nats-Msg-Id", "1")

	_, err = js.PublishMsg(m)
	require_NoError(t, err)

	m.Header = make(nats.Header)
	m.Header.Add("X", "Nats-Msg-Id:1")

	_, err = js.PublishMsg(m)
	require_NoError(t, err)

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	require_True(t, si.State.Msgs == 5)
}

// https://github.com/nats-io/nats-server/issues/3657
func TestJetStreamServerCrashOnPullConsumerDeleteWithInactiveThresholdAfterAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "foo", "msg")

	sub, err := js.PullSubscribe("foo", "dlc", nats.InactiveThreshold(10*time.Second))
	require_NoError(t, err)

	msgs := fetchMsgs(t, sub, 1, time.Second)
	require_True(t, len(msgs) == 1)
	msgs[0].Ack()
	err = js.DeleteConsumer("TEST", "dlc")
	require_NoError(t, err)

	// If server crashes this will fail.
	_, err = js.StreamInfo("TEST")
	require_NoError(t, err)
}

func TestJetStreamConsumerMultipleSubjectsLast(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	durable := "durable"
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Subjects: []string{"events", "data", "other"},
		Name:     "name",
	})
	if err != nil {
		t.Fatalf("error while creating stream")
	}

	sendStreamMsg(t, nc, "events", "1")
	sendStreamMsg(t, nc, "data", "2")
	sendStreamMsg(t, nc, "other", "3")
	sendStreamMsg(t, nc, "events", "4")
	sendStreamMsg(t, nc, "data", "5")
	sendStreamMsg(t, nc, "data", "6")
	sendStreamMsg(t, nc, "other", "7")
	sendStreamMsg(t, nc, "other", "8")

	// if they're not the same, expect error
	_, err = mset.addConsumer(&ConsumerConfig{
		DeliverPolicy:  DeliverLast,
		AckPolicy:      AckExplicit,
		DeliverSubject: "deliver",
		FilterSubjects: []string{"events", "data"},
		Durable:        durable,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("", nats.Bind("name", durable))
	require_NoError(t, err)

	msg, err := sub.NextMsg(time.Millisecond * 500)
	require_NoError(t, err)

	j, err := strconv.Atoi(string(msg.Data))
	require_NoError(t, err)
	expectedStreamSeq := 6
	if j != expectedStreamSeq {
		t.Fatalf("wrong sequence, expected %v got %v", expectedStreamSeq, j)
	}

	require_NoError(t, msg.AckSync())

	// check if we don't get more than we wanted
	msg, err = sub.NextMsg(time.Millisecond * 500)
	if msg != nil || err == nil {
		t.Fatalf("should not get more messages")
	}

	info, err := js.ConsumerInfo("name", durable)
	require_NoError(t, err)

	require_True(t, info.NumAckPending == 0)
	require_True(t, info.AckFloor.Stream == 8)
	require_True(t, info.AckFloor.Consumer == 1)
	require_True(t, info.NumPending == 0)
}

func TestJetStreamConsumerMultipleSubjectsLastPerSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	durable := "durable"
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Subjects: []string{"events.*", "data.>", "other"},
		Name:     "name",
	})
	if err != nil {
		t.Fatalf("error while creating stream")
	}

	sendStreamMsg(t, nc, "events.1", "bad")
	sendStreamMsg(t, nc, "events.1", "events.1")

	sendStreamMsg(t, nc, "data.1", "bad")
	sendStreamMsg(t, nc, "data.1", "bad")
	sendStreamMsg(t, nc, "data.1", "bad")
	sendStreamMsg(t, nc, "data.1", "bad")
	sendStreamMsg(t, nc, "data.1", "data.1")

	sendStreamMsg(t, nc, "events.2", "bad")
	sendStreamMsg(t, nc, "events.2", "bad")
	// this is last proper sequence,
	sendStreamMsg(t, nc, "events.2", "events.2")

	sendStreamMsg(t, nc, "other", "bad")
	sendStreamMsg(t, nc, "other", "bad")

	// if they're not the same, expect error
	_, err = mset.addConsumer(&ConsumerConfig{
		DeliverPolicy:  DeliverLastPerSubject,
		AckPolicy:      AckExplicit,
		DeliverSubject: "deliver",
		FilterSubjects: []string{"events.*", "data.>"},
		Durable:        durable,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("", nats.Bind("name", durable))
	require_NoError(t, err)

	checkMessage := func(t *testing.T, subject string, payload string, ack bool) {
		msg, err := sub.NextMsg(time.Millisecond * 500)
		require_NoError(t, err)

		if string(msg.Data) != payload {
			t.Fatalf("expected %v paylaod, got %v", payload, string(msg.Data))
		}
		if subject != msg.Subject {
			t.Fatalf("expected %v subject, got %v", subject, msg.Subject)
		}
		if ack {
			msg.AckSync()
		}
	}

	checkMessage(t, "events.1", "events.1", true)
	checkMessage(t, "data.1", "data.1", true)
	checkMessage(t, "events.2", "events.2", false)

	info, err := js.ConsumerInfo("name", durable)
	require_NoError(t, err)

	require_True(t, info.AckFloor.Consumer == 2)
	require_True(t, info.AckFloor.Stream == 9)
	require_True(t, info.Delivered.Stream == 12)
	require_True(t, info.Delivered.Consumer == 3)

	require_NoError(t, err)

}
func TestJetStreamConsumerMultipleSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	durable := "durable"
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{
		Subjects: []string{"events.>", "data.>"},
		Name:     "name",
	})
	require_NoError(t, err)

	for i := 0; i < 20; i += 2 {
		sendStreamMsg(t, nc, "events.created", fmt.Sprintf("created %v", i))
		sendStreamMsg(t, nc, "data.processed", fmt.Sprintf("processed %v", i+1))
	}

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        durable,
		DeliverSubject: "deliver",
		FilterSubjects: []string{"events.created", "data.processed"},
		AckPolicy:      AckExplicit,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("", nats.Bind("name", durable))
	require_NoError(t, err)

	for i := 0; i < 20; i++ {
		msg, err := sub.NextMsg(time.Millisecond * 500)
		require_NoError(t, err)
		require_NoError(t, msg.AckSync())
	}
	info, err := js.ConsumerInfo("name", durable)
	require_NoError(t, err)
	require_True(t, info.NumAckPending == 0)
	require_True(t, info.NumPending == 0)
	require_True(t, info.AckFloor.Consumer == 20)
	require_True(t, info.AckFloor.Stream == 20)

}

func TestJetStreamConsumerMultipleSubjectsWithEmpty(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	durable := "durable"
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Subjects: []string{"events.>"},
		Name:     "name",
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "events.created", fmt.Sprintf("%v", i))
	}

	// if they're not the same, expect error
	_, err = js.AddConsumer("name", &nats.ConsumerConfig{
		DeliverSubject: "deliver",
		FilterSubject:  "",
		Durable:        durable,
		AckPolicy:      nats.AckExplicitPolicy})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("", nats.Bind("name", durable))
	require_NoError(t, err)

	for i := 0; i < 9; i++ {
		msg, err := sub.NextMsg(time.Millisecond * 500)
		require_NoError(t, err)
		j, err := strconv.Atoi(string(msg.Data))
		require_NoError(t, err)
		if j != i {
			t.Fatalf("wrong sequence, expected %v got %v", i, j)
		}
		require_NoError(t, msg.AckSync())
	}

	info, err := js.ConsumerInfo("name", durable)
	require_NoError(t, err)
	require_True(t, info.Delivered.Stream == 10)
	require_True(t, info.Delivered.Consumer == 10)
	require_True(t, info.AckFloor.Stream == 9)
	require_True(t, info.AckFloor.Consumer == 9)
	require_True(t, info.NumAckPending == 1)

	resp := createConsumer(t, nc, "name", ConsumerConfig{
		FilterSubjects: []string{""},
		DeliverSubject: "multiple",
		Durable:        "multiple",
		AckPolicy:      AckExplicit,
	})
	require_True(t, resp.Error.ErrCode == 10139)
}

func SingleFilterConsumerCheck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	durable := "durable"
	nc, _ := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Subjects: []string{"events.>"},
		Name:     "deliver",
	})
	require_NoError(t, err)

	// if they're not the same, expect error
	_, err = mset.addConsumer(&ConsumerConfig{
		DeliverSubject: "deliver",
		FilterSubject:  "SINGLE",
		Durable:        durable,
	})
	require_Error(t, err)
}

// createConsumer is a temporary method until nats.go client supports multiple subjects.
// it is used where lowe level call on mset is not enough, as we want to test error validation.
func createConsumer(t *testing.T, nc *nats.Conn, stream string, config ConsumerConfig) JSApiConsumerCreateResponse {
	req, err := json.Marshal(&CreateConsumerRequest{Stream: stream, Config: config})
	require_NoError(t, err)

	resp, err := nc.Request(fmt.Sprintf("$JS.API.CONSUMER.DURABLE.CREATE.%s.%s", stream, config.Durable), req, time.Second*10)
	require_NoError(t, err)

	var apiResp JSApiConsumerCreateResponse
	require_NoError(t, json.Unmarshal(resp.Data, &apiResp))

	return apiResp
}

func TestJetStreamConsumerOverlappingSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	_, err := acc.addStream(&StreamConfig{
		Subjects: []string{"events.>"},
		Name:     "deliver",
	})
	require_NoError(t, err)

	resp := createConsumer(t, nc, "deliver", ConsumerConfig{
		FilterSubjects: []string{"events.one", "events.*"},
		Durable:        "name",
	})

	if resp.Error.ErrCode != 10138 {
		t.Fatalf("this should error as we have overlapping subjects, got %+v", resp.Error)
	}
}

func TestJetStreamBothFiltersSet(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	_, err := acc.addStream(&StreamConfig{
		Subjects: []string{"events.>"},
		Name:     "deliver",
	})
	require_NoError(t, err)

	resp := createConsumer(t, nc, "deliver", ConsumerConfig{
		FilterSubjects: []string{"events.one", "events.two"},
		FilterSubject:  "events.three",
		Durable:        "name",
	})
	require_True(t, resp.Error.ErrCode == 10136)
}

func TestJetStreamMultipleSubjectsPushBasic(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{
		Subjects: []string{"events", "data", "other"},
		Name:     "deliver",
	})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		FilterSubjects: []string{"events", "data"},
		Durable:        "name",
		DeliverSubject: "push",
	})
	require_NoError(t, err)

	sub, err := nc.SubscribeSync("push")
	require_NoError(t, err)

	sendStreamMsg(t, nc, "other", "10")
	sendStreamMsg(t, nc, "events", "0")
	sendStreamMsg(t, nc, "data", "1")
	sendStreamMsg(t, nc, "events", "2")
	sendStreamMsg(t, nc, "events", "3")
	sendStreamMsg(t, nc, "other", "10")
	sendStreamMsg(t, nc, "data", "4")
	sendStreamMsg(t, nc, "data", "5")

	for i := 0; i < 6; i++ {
		msg, err := sub.NextMsg(time.Second * 1)
		require_NoError(t, err)
		if fmt.Sprintf("%v", i) != string(msg.Data) {
			t.Fatalf("bad sequence. Expected %v, got %v", i, string(msg.Data))
		}
	}
	info, err := js.ConsumerInfo("deliver", "name")
	require_NoError(t, err)
	require_True(t, info.AckFloor.Consumer == 6)
	require_True(t, info.AckFloor.Stream == 8)
}
func TestJetStreamMultipleSubjectsBasic(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Subjects: []string{"events", "data", "other"},
		Name:     "deliver",
	})
	require_NoError(t, err)

	mset.addConsumer(&ConsumerConfig{
		FilterSubjects: []string{"events", "data"},
		Durable:        "name",
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "other", "10")
	sendStreamMsg(t, nc, "events", "0")
	sendStreamMsg(t, nc, "data", "1")
	sendStreamMsg(t, nc, "events", "2")
	sendStreamMsg(t, nc, "events", "3")
	sendStreamMsg(t, nc, "other", "10")
	sendStreamMsg(t, nc, "data", "4")
	sendStreamMsg(t, nc, "data", "5")

	consumer, err := js.PullSubscribe("", "name", nats.Bind("deliver", "name"))
	require_NoError(t, err)

	msg, err := consumer.Fetch(6)
	require_NoError(t, err)

	for i, msg := range msg {
		if fmt.Sprintf("%v", i) != string(msg.Data) {
			t.Fatalf("bad sequence. Expected %v, got %v", i, string(msg.Data))
		}
	}
	_, err = js.ConsumerInfo("deliver", "name")
	require_NoError(t, err)
}

func TestJetStreamKVDelete(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  "deletion",
		History: 10,
	})
	require_NoError(t, err)
	kv.Put("a", nil)
	kv.Put("a.a", nil)
	kv.Put("a.b", nil)
	kv.Put("a.b.c", nil)

	keys, err := kv.Keys()
	require_NoError(t, err)
	require_True(t, len(keys) == 4)

	info, err := js.AddConsumer("KV_deletion", &nats.ConsumerConfig{
		Name:           "keys",
		FilterSubject:  "$KV.deletion.a.*",
		DeliverPolicy:  nats.DeliverLastPerSubjectPolicy,
		DeliverSubject: "keys",
		MaxDeliver:     1,
		AckPolicy:      nats.AckNonePolicy,
		MemoryStorage:  true,
		FlowControl:    true,
		Heartbeat:      time.Second * 5,
	})
	require_NoError(t, err)
	require_True(t, info.NumPending == 2)

	sub, err := js.SubscribeSync("$KV.deletion.a.*", nats.Bind("KV_deletion", "keys"))
	require_NoError(t, err)

	_, err = sub.NextMsg(time.Second * 1)
	require_NoError(t, err)
	_, err = sub.NextMsg(time.Second * 1)
	require_NoError(t, err)
	msg, err := sub.NextMsg(time.Second * 1)
	require_True(t, msg == nil)
	require_Error(t, err)

	require_NoError(t, kv.Delete("a.a"))
	require_NoError(t, kv.Delete("a.b"))

	watcher, err := kv.WatchAll()
	require_NoError(t, err)

	updates := watcher.Updates()

	keys = []string{}
	for v := range updates {
		if v == nil {
			break
		}
		if v.Operation() == nats.KeyValueDelete {
			keys = append(keys, v.Key())
		}
	}
	require_True(t, len(keys) == 2)
}

func TestJetStreamDeliverLastPerSubjectWithKV(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "TEST",
		MaxMsgsPerSubject: 5,
		Subjects:          []string{"kv.>"},
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "kv.a", "bad")
	sendStreamMsg(t, nc, "kv.a", "bad")
	sendStreamMsg(t, nc, "kv.a", "bad")
	sendStreamMsg(t, nc, "kv.a", "a")
	sendStreamMsg(t, nc, "kv.a.b", "bad")
	sendStreamMsg(t, nc, "kv.a.b", "bad")
	sendStreamMsg(t, nc, "kv.a.b", "a.b")
	sendStreamMsg(t, nc, "kv.a.b.c", "bad")
	sendStreamMsg(t, nc, "kv.a.b.c", "bad")
	sendStreamMsg(t, nc, "kv.a.b.c", "bad")
	sendStreamMsg(t, nc, "kv.a.b.c", "a.b.c")

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name:           "CONSUMER",
		FilterSubject:  "kv.>",
		DeliverPolicy:  nats.DeliverLastPerSubjectPolicy,
		DeliverSubject: "deliver",
		MaxDeliver:     1,
		AckPolicy:      nats.AckNonePolicy,
		MemoryStorage:  true,
		FlowControl:    true,
		Heartbeat:      time.Second * 5,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("kv.>", nats.Bind("TEST", "CONSUMER"))
	require_NoError(t, err)

	for i := 1; i <= 3; i++ {
		_, err := sub.NextMsg(time.Second * 1)
		require_NoError(t, err)
	}

	msg, err := sub.NextMsg(time.Second * 1)
	if err == nil || msg != nil {
		t.Fatalf("should not get any more messages")
	}
}

func TestJetStreamConsumerMultipleSubjectsAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Subjects: []string{"events", "data", "other"},
		Name:     "deliver",
	})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		FilterSubjects: []string{"events", "data"},
		Durable:        "name",
		AckPolicy:      AckExplicit,
		Replicas:       1,
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "events", "1")
	sendStreamMsg(t, nc, "data", "2")
	sendStreamMsg(t, nc, "data", "3")
	sendStreamMsg(t, nc, "data", "4")
	sendStreamMsg(t, nc, "events", "5")
	sendStreamMsg(t, nc, "data", "6")
	sendStreamMsg(t, nc, "data", "7")

	consumer, err := js.PullSubscribe("", "name", nats.Bind("deliver", "name"))
	require_NoError(t, err)

	msg, err := consumer.Fetch(3)
	require_NoError(t, err)

	require_True(t, len(msg) == 3)

	require_NoError(t, msg[0].AckSync())
	require_NoError(t, msg[1].AckSync())

	info, err := js.ConsumerInfo("deliver", "name")
	require_NoError(t, err)

	if info.AckFloor.Consumer != 2 {
		t.Fatalf("bad consumer sequence. expected %v, got %v", 2, info.AckFloor.Consumer)
	}
	if info.AckFloor.Stream != 2 {
		t.Fatalf("bad stream sequence. expected %v, got %v", 2, info.AckFloor.Stream)
	}
	if info.NumPending != 4 {
		t.Fatalf("bad num pending. Expected %v, got %v", 2, info.NumPending)
	}

}

func TestJetStreamConsumerMultipleSubjectAndNewAPI(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	_, err := acc.addStream(&StreamConfig{
		Subjects: []string{"data", "events"},
		Name:     "deliver",
	})
	if err != nil {
		t.Fatalf("error while creating stream")
	}

	req, err := json.Marshal(&CreateConsumerRequest{Stream: "deliver", Config: ConsumerConfig{
		FilterSubjects: []string{"events", "data"},
		Name:           "name",
		Durable:        "name",
	}})
	require_NoError(t, err)

	resp, err := nc.Request(fmt.Sprintf("$JS.API.CONSUMER.CREATE.%s.%s.%s", "deliver", "name", "data.>"), req, time.Second*10)

	var apiResp JSApiConsumerCreateResponse
	json.Unmarshal(resp.Data, &apiResp)
	require_NoError(t, err)

	if apiResp.Error.ErrCode != 10137 {
		t.Fatal("this should error as multiple subject filters is incompatible with new API and didn't")
	}

}

func TestJetStreamConsumerMultipleSubjectsWithAddedMessages(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	durable := "durable"
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Subjects: []string{"events.>"},
		Name:     "deliver",
	})
	require_NoError(t, err)

	// if they're not the same, expect error
	_, err = mset.addConsumer(&ConsumerConfig{
		DeliverSubject: "deliver",
		FilterSubjects: []string{"events.created", "events.processed"},
		Durable:        durable,
		AckPolicy:      AckExplicit,
	})

	require_NoError(t, err)

	sendStreamMsg(t, nc, "events.created", "0")
	sendStreamMsg(t, nc, "events.created", "1")
	sendStreamMsg(t, nc, "events.created", "2")
	sendStreamMsg(t, nc, "events.created", "3")
	sendStreamMsg(t, nc, "events.other", "BAD")
	sendStreamMsg(t, nc, "events.processed", "4")
	sendStreamMsg(t, nc, "events.processed", "5")
	sendStreamMsg(t, nc, "events.processed", "6")
	sendStreamMsg(t, nc, "events.other", "BAD")
	sendStreamMsg(t, nc, "events.processed", "7")
	sendStreamMsg(t, nc, "events.processed", "8")

	sub, err := js.SubscribeSync("", nats.Bind("deliver", durable))
	if err != nil {
		t.Fatalf("error while subscribing to Consumer: %v", err)
	}

	for i := 0; i < 10; i++ {
		if i == 5 {
			sendStreamMsg(t, nc, "events.created", "9")
		}
		if i == 9 {
			sendStreamMsg(t, nc, "events.other", "BAD")
			sendStreamMsg(t, nc, "events.created", "11")
		}
		if i == 7 {
			sendStreamMsg(t, nc, "events.processed", "10")
		}

		msg, err := sub.NextMsg(time.Second * 1)
		require_NoError(t, err)
		j, err := strconv.Atoi(string(msg.Data))
		require_NoError(t, err)
		if j != i {
			t.Fatalf("wrong sequence, expected %v got %v", i, j)
		}
		if err := msg.AckSync(); err != nil {
			t.Fatalf("error while acking the message :%v", err)
		}

	}

	info, err := js.ConsumerInfo("deliver", durable)
	require_NoError(t, err)

	require_True(t, info.Delivered.Consumer == 12)
	require_True(t, info.Delivered.Stream == 15)
	require_True(t, info.AckFloor.Stream == 12)
	require_True(t, info.AckFloor.Consumer == 10)
}

func TestJetStreamConsumerThreeFilters(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"events", "data", "other", "ignored"},
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "ignored", "100")
	sendStreamMsg(t, nc, "events", "0")
	sendStreamMsg(t, nc, "events", "1")

	sendStreamMsg(t, nc, "data", "2")
	sendStreamMsg(t, nc, "ignored", "100")
	sendStreamMsg(t, nc, "data", "3")

	sendStreamMsg(t, nc, "other", "4")
	sendStreamMsg(t, nc, "data", "5")
	sendStreamMsg(t, nc, "other", "6")
	sendStreamMsg(t, nc, "data", "7")
	sendStreamMsg(t, nc, "ignored", "100")

	mset.addConsumer(&ConsumerConfig{
		FilterSubjects: []string{"events", "data", "other"},
		Durable:        "multi",
		AckPolicy:      AckExplicit,
	})

	consumer, err := js.PullSubscribe("", "multi", nats.Bind("TEST", "multi"))
	require_NoError(t, err)

	msgs, err := consumer.Fetch(6)
	require_NoError(t, err)
	for i, msg := range msgs {
		require_Equal(t, string(msg.Data), fmt.Sprintf("%d", i))
		require_NoError(t, msg.AckSync())
	}

	info, err := js.ConsumerInfo("TEST", "multi")
	require_NoError(t, err)
	require_True(t, info.Delivered.Stream == 8)
	require_True(t, info.Delivered.Consumer == 6)
	require_True(t, info.NumPending == 2)
	require_True(t, info.NumAckPending == 0)
	require_True(t, info.AckFloor.Consumer == 6)
	require_True(t, info.AckFloor.Stream == 8)
}

func TestJetStreamConsumerUpdateFilterSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"events", "data", "other"},
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "other", "100")
	sendStreamMsg(t, nc, "events", "0")
	sendStreamMsg(t, nc, "events", "1")
	sendStreamMsg(t, nc, "data", "2")
	sendStreamMsg(t, nc, "data", "3")
	sendStreamMsg(t, nc, "other", "4")
	sendStreamMsg(t, nc, "data", "5")

	_, err = mset.addConsumer(&ConsumerConfig{
		FilterSubjects: []string{"events", "data"},
		Durable:        "multi",
		AckPolicy:      AckExplicit,
	})
	require_NoError(t, err)

	consumer, err := js.PullSubscribe("", "multi", nats.Bind("TEST", "multi"))
	require_NoError(t, err)

	msgs, err := consumer.Fetch(3)
	require_NoError(t, err)
	for i, msg := range msgs {
		require_Equal(t, string(msg.Data), fmt.Sprintf("%d", i))
		require_NoError(t, msg.AckSync())
	}

	_, err = mset.addConsumer(&ConsumerConfig{
		FilterSubjects: []string{"events", "data", "other"},
		Durable:        "multi",
		AckPolicy:      AckExplicit,
	})
	require_NoError(t, err)

	updatedConsumer, err := js.PullSubscribe("", "multi", nats.Bind("TEST", "multi"))
	require_NoError(t, err)

	msgs, err = updatedConsumer.Fetch(3)
	require_NoError(t, err)
	for i, msg := range msgs {
		require_Equal(t, string(msg.Data), fmt.Sprintf("%d", i+3))
		require_NoError(t, msg.AckSync())
	}
}
func TestJetStreamStreamUpdateSubjectsOverlapOthers(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"TEST"},
	})
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"TEST", "foo.a"},
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST2",
		Subjects: []string{"TEST2"},
	})
	require_NoError(t, err)

	// we expect an error updating stream TEST2 with subject that overlaps that used by TEST
	// foo.a fails too, but foo.* also double-check for sophisticated overlap match
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST2",
		Subjects: []string{"TEST2", "foo.*"},
	})
	require_Error(t, err)
	require_Contains(t, err.Error(), "overlap")
}

func TestJetStreamMetaDataFailOnKernelFault(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "foo", "OK")
	}

	sd := s.JetStreamConfig().StoreDir
	sdir := filepath.Join(sd, "$G", "streams", "TEST")
	s.Shutdown()

	// Emulate if the kernel did not flush out to disk the meta information.
	// so we will zero out both meta.inf and meta.sum.
	err = os.WriteFile(filepath.Join(sdir, JetStreamMetaFile), nil, defaultFilePerms)
	require_NoError(t, err)

	err = os.WriteFile(filepath.Join(sdir, JetStreamMetaFileSum), nil, defaultFilePerms)
	require_NoError(t, err)

	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// The stream will have not been recovered. So err is normal.
	_, err = js.StreamInfo("TEST")
	require_Error(t, err)

	// Make sure we are signaled here from healthz
	hs := s.healthz(nil)
	const expected = "JetStream stream '$G > TEST' could not be recovered"
	if hs.Status != "unavailable" || hs.Error == _EMPTY_ {
		t.Fatalf("Expected healthz to return an error")
	} else if hs.Error != expected {
		t.Fatalf("Expected healthz error %q got %q", expected, hs.Error)
	}

	// If we add it back, this should recover the msgs.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	// Make sure we recovered.
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 10)

	// Now if we restart the server, meta should be correct,
	// and the stream should be restored.
	s.Shutdown()

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// Make sure we recovered the stream correctly after re-adding.
	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 10)
}

func TestJetstreamConsumerSingleTokenSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	filterSubject := "foo"
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{filterSubject},
	})
	require_NoError(t, err)

	req, err := json.Marshal(&CreateConsumerRequest{Stream: "TEST", Config: ConsumerConfig{
		FilterSubject: filterSubject,
		Name:          "name",
	}})

	if err != nil {
		t.Fatalf("failed to marshal consumer create request: %v", err)
	}

	resp, err := nc.Request(fmt.Sprintf("$JS.API.CONSUMER.CREATE.%s.%s.%s", "TEST", "name", "not_filter_subject"), req, time.Second*10)

	var apiResp ApiResponse
	json.Unmarshal(resp.Data, &apiResp)
	if err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if apiResp.Error == nil {
		t.Fatalf("expected error, got nil")
	}
	if apiResp.Error.ErrCode != 10131 {
		t.Fatalf("expected error 10131, got %v", apiResp.Error)
	}
}

// https://github.com/nats-io/nats-server/issues/3734
func TestJetStreamMsgBlkFailOnKernelFault(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		MaxBytes: 10 * 1024 * 1024, // 10MB
	})
	require_NoError(t, err)

	msgSize := 1024 * 1024 // 1MB
	msg := make([]byte, msgSize)
	crand.Read(msg)

	for i := 0; i < 20; i++ {
		_, err := js.Publish("foo", msg)
		require_NoError(t, err)
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Bytes < uint64(si.Config.MaxBytes))

	// Now emulate a kernel fault that fails to write the last blk properly.
	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	mset.mu.RLock()
	fs := mset.store.(*fileStore)
	fs.mu.RLock()
	require_True(t, len(fs.blks) > 2)
	// Here we do not grab the last one, which we handle correctly. We grab an interior one near the end.
	lmbf := fs.blks[len(fs.blks)-2].mfn
	fs.mu.RUnlock()
	mset.mu.RUnlock()

	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()

	// Zero out the last block.
	err = os.WriteFile(lmbf, nil, defaultFilePerms)
	require_NoError(t, err)

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.NumDeleted == 3)

	// Test detailed version as well.
	si, err = js.StreamInfo("TEST", &nats.StreamInfoRequest{DeletedDetails: true})
	require_NoError(t, err)
	require_True(t, si.State.NumDeleted == 3)
	if !reflect.DeepEqual(si.State.Deleted, []uint64{16, 17, 18}) {
		t.Fatalf("Expected deleted of %+v, got %+v", []uint64{16, 17, 18}, si.State.Deleted)
	}

	for i := 0; i < 20; i++ {
		_, err := js.Publish("foo", msg)
		require_NoError(t, err)
	}

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	if si.State.Bytes > uint64(si.Config.MaxBytes) {
		t.Fatalf("MaxBytes not enforced with empty interior msg blk, max %v, bytes %v",
			friendlyBytes(si.Config.MaxBytes), friendlyBytes(int64(si.State.Bytes)))
	}
}

func TestJetStreamPullConsumerBatchCompleted(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	msgSize := 128
	msg := make([]byte, msgSize)
	rand.Read(msg)

	for i := 0; i < 10; i++ {
		_, err := js.Publish("foo", msg)
		require_NoError(t, err)
	}

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "dur",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	req := JSApiConsumerGetNextRequest{Batch: 0, MaxBytes: 1024, Expires: 250 * time.Millisecond}

	reqb, _ := json.Marshal(req)
	sub := natsSubSync(t, nc, nats.NewInbox())
	err = nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.dur", sub.Subject, reqb)
	require_NoError(t, err)

	// Expect first message to arrive normally.
	_, err = sub.NextMsg(time.Second * 1)
	require_NoError(t, err)

	// Second message should be info that batch is complete, but there were pending bytes.
	pullMsg, err := sub.NextMsg(time.Second * 1)
	require_NoError(t, err)

	if v := pullMsg.Header.Get("Status"); v != "409" {
		t.Fatalf("Expected 409, got: %s", v)
	}
	if v := pullMsg.Header.Get("Description"); v != "Batch Completed" {
		t.Fatalf("Expected Batch Completed, got: %s", v)
	}
}

func TestJetStreamConsumerAndStreamMetadata(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	metadata := map[string]string{"key": "value", "_nats_created_version": "2.9.11"}
	acc := s.GlobalAccount()

	// Check stream's first.
	mset, err := acc.addStream(&StreamConfig{Name: "foo", Metadata: metadata})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	if cfg := mset.config(); !reflect.DeepEqual(metadata, cfg.Metadata) {
		t.Fatalf("Expected a metadata of %q, got %q", metadata, cfg.Metadata)
	}

	// Now consumer
	o, err := mset.addConsumer(&ConsumerConfig{
		Metadata:       metadata,
		DeliverSubject: "to",
		AckPolicy:      AckNone})
	if err != nil {
		t.Fatalf("Unexpected error adding consumer: %v", err)
	}
	if cfg := o.config(); !reflect.DeepEqual(metadata, cfg.Metadata) {
		t.Fatalf("Expected a metadata of %q, got %q", metadata, cfg.Metadata)
	}

	// Test max.
	data := make([]byte, JSMaxMetadataLen/100)
	rand.Read(data)
	bigValue := base64.StdEncoding.EncodeToString(data)

	bigMetadata := make(map[string]string, 101)
	for i := 0; i < 101; i++ {
		bigMetadata[fmt.Sprintf("key%d", i)] = bigValue
	}

	_, err = acc.addStream(&StreamConfig{Name: "bar", Metadata: bigMetadata})
	if err == nil || !strings.Contains(err.Error(), "stream metadata exceeds") {
		t.Fatalf("Expected an error but got none")
	}

	_, err = mset.addConsumer(&ConsumerConfig{
		Metadata:       bigMetadata,
		DeliverSubject: "to",
		AckPolicy:      AckNone})
	if err == nil || !strings.Contains(err.Error(), "consumer metadata exceeds") {
		t.Fatalf("Expected an error but got none")
	}
}

func TestJetStreamConsumerPurge(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "test.1", "hello")
	sendStreamMsg(t, nc, "test.2", "hello")

	sub, err := js.PullSubscribe("test.>", "consumer")
	require_NoError(t, err)

	// Purge one of the subjects.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Subject: "test.2"})
	require_NoError(t, err)

	info, err := js.ConsumerInfo("TEST", "consumer")
	require_NoError(t, err)
	require_True(t, info.NumPending == 1)

	// Expect to get message from not purged subject.
	_, err = sub.Fetch(1)
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "OTHER",
		Subjects: []string{"other.>"},
	})
	require_NoError(t, err)

	// Publish two items to two subjects.
	sendStreamMsg(t, nc, "other.1", "hello")
	sendStreamMsg(t, nc, "other.2", "hello")

	sub, err = js.PullSubscribe("other.>", "other_consumer")
	require_NoError(t, err)

	// Purge whole stream.
	err = js.PurgeStream("OTHER", &nats.StreamPurgeRequest{})
	require_NoError(t, err)

	info, err = js.ConsumerInfo("OTHER", "other_consumer")
	require_NoError(t, err)
	require_True(t, info.NumPending == 0)

	// This time expect error, as we purged whole stream,
	_, err = sub.Fetch(1)
	require_Error(t, err)

}

func TestJetStreamConsumerFilterUpdate(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>", "bar.>"},
	})
	require_NoError(t, err)

	for i := 0; i < 3; i++ {
		sendStreamMsg(t, nc, "foo.data", "OK")
	}

	sub, err := nc.SubscribeSync("deliver")
	require_NoError(t, err)

	js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "consumer",
		DeliverSubject: "deliver",
		FilterSubject:  "foo.data",
	})

	_, err = sub.NextMsg(time.Second * 1)
	require_NoError(t, err)
	_, err = sub.NextMsg(time.Second * 1)
	require_NoError(t, err)
	_, err = sub.NextMsg(time.Second * 1)
	require_NoError(t, err)

	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "consumer",
		DeliverSubject: "deliver",
		FilterSubject:  "foo.>",
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "foo.other", "data")

	// This will timeout if filters were not properly updated.
	_, err = sub.NextMsg(time.Second * 1)
	require_NoError(t, err)

	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	checkNumFilter := func(expected int) {
		t.Helper()
		mset.mu.RLock()
		nf := mset.numFilter
		mset.mu.RUnlock()
		if nf != expected {
			t.Fatalf("Expected stream's numFilter to be %d, got %d", expected, nf)
		}
	}

	checkNumFilter(1)

	// Update consumer once again, now not having any filters
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "consumer",
		DeliverSubject: "deliver",
		FilterSubject:  _EMPTY_,
	})
	require_NoError(t, err)

	// and expect that numFilter reports correctly.
	checkNumFilter(0)
}

func TestJetStreamPurgeExAndAccounting(t *testing.T) {
	cases := []struct {
		name string
		cfg  *nats.StreamConfig
	}{
		{name: "MemoryStore",
			cfg: &nats.StreamConfig{
				Name:     "TEST",
				Storage:  nats.MemoryStorage,
				Subjects: []string{"*"},
			}},
		{name: "FileStore",
			cfg: &nats.StreamConfig{
				Name:     "TEST",
				Storage:  nats.FileStorage,
				Subjects: []string{"*"},
			}},
	}
	for _, c := range cases {
		s := RunBasicJetStreamServer(t)
		defer s.Shutdown()

		// Client for API requests.
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(c.cfg)
		require_NoError(t, err)

		msg := []byte("accounting")
		for i := 0; i < 100; i++ {
			_, err = js.Publish("foo", msg)
			require_NoError(t, err)
			_, err = js.Publish("bar", msg)
			require_NoError(t, err)
		}

		info, err := js.AccountInfo()
		require_NoError(t, err)

		err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Subject: "foo"})
		require_NoError(t, err)

		ninfo, err := js.AccountInfo()
		require_NoError(t, err)

		// Make sure we did the proper accounting.
		if c.cfg.Storage == nats.MemoryStorage {
			if ninfo.Memory != info.Memory/2 {
				t.Fatalf("Accounting information incorrect for Memory: %d vs %d",
					ninfo.Memory, info.Memory/2)
			}
		} else {
			if ninfo.Store != info.Store/2 {
				t.Fatalf("Accounting information incorrect for FileStore: %d vs %d",
					ninfo.Store, info.Store/2)
			}
		}
	}
}

func TestJetStreamRollup(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	const STREAM = "S"
	const SUBJ = "S.*"

	js.AddStream(&nats.StreamConfig{
		Name:        STREAM,
		Subjects:    []string{SUBJ},
		AllowRollup: true,
	})

	for i := 1; i <= 10; i++ {
		sendStreamMsg(t, nc, "S.A", fmt.Sprintf("%v", i))
		sendStreamMsg(t, nc, "S.B", fmt.Sprintf("%v", i))
	}

	sinfo, err := js.StreamInfo(STREAM)
	require_NoError(t, err)
	require_True(t, sinfo.State.Msgs == 20)

	cinfo, err := js.AddConsumer(STREAM, &nats.ConsumerConfig{
		Durable:       "DUR-A",
		FilterSubject: "S.A",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)
	require_True(t, cinfo.NumPending == 10)

	m := nats.NewMsg("S.A")
	m.Header.Set(JSMsgRollup, JSMsgRollupSubject)

	_, err = js.PublishMsg(m)
	require_NoError(t, err)

	cinfo, err = js.ConsumerInfo("S", "DUR-A")
	require_NoError(t, err)
	require_True(t, cinfo.NumPending == 1)

	sinfo, err = js.StreamInfo(STREAM)
	require_NoError(t, err)
	require_True(t, sinfo.State.Msgs == 11)

	cinfo, err = js.AddConsumer(STREAM, &nats.ConsumerConfig{
		Durable:       "DUR-B",
		FilterSubject: "S.B",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)
	require_True(t, cinfo.NumPending == 10)
}

func TestJetStreamPartialPurgeWithAckPending(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	nmsgs := 100
	for i := 0; i < nmsgs; i++ {
		sendStreamMsg(t, nc, "foo", "OK")
	}
	sub, err := js.PullSubscribe("foo", "dlc", nats.AckWait(time.Second))
	require_NoError(t, err)

	// Queue up all for ack pending.
	_, err = sub.Fetch(nmsgs)
	require_NoError(t, err)

	keep := nmsgs / 2
	require_NoError(t, js.PurgeStream("TEST", &nats.StreamPurgeRequest{Keep: uint64(keep)}))

	// Should be able to be redelivered now.
	time.Sleep(2 * time.Second)

	ci, err := js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)
	// Make sure we calculated correctly.
	require_True(t, ci.AckFloor.Consumer == uint64(keep))
	require_True(t, ci.AckFloor.Stream == uint64(keep))
	require_True(t, ci.NumAckPending == keep)
	require_True(t, ci.NumPending == 0)

	for i := 0; i < nmsgs; i++ {
		sendStreamMsg(t, nc, "foo", "OK")
	}

	ci, err = js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)
	// Make sure we calculated correctly.
	// Top 3 will be same.
	require_True(t, ci.AckFloor.Consumer == uint64(keep))
	require_True(t, ci.AckFloor.Stream == uint64(keep))
	require_True(t, ci.NumAckPending == keep)
	require_True(t, ci.NumPending == uint64(nmsgs))
	require_True(t, ci.NumRedelivered == 0)

	msgs, err := sub.Fetch(keep)
	require_NoError(t, err)
	require_True(t, len(msgs) == keep)

	ci, err = js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)
	// Make sure we calculated correctly.
	require_True(t, ci.Delivered.Consumer == uint64(nmsgs+keep))
	require_True(t, ci.Delivered.Stream == uint64(nmsgs))
	require_True(t, ci.AckFloor.Consumer == uint64(keep))
	require_True(t, ci.AckFloor.Stream == uint64(keep))
	require_True(t, ci.NumAckPending == keep)
	require_True(t, ci.NumPending == uint64(nmsgs))
	require_True(t, ci.NumRedelivered == keep)

	// Ack all.
	for _, m := range msgs {
		m.Ack()
	}
	nc.Flush()

	ci, err = js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)
	// Same for Delivered
	require_True(t, ci.Delivered.Consumer == uint64(nmsgs+keep))
	require_True(t, ci.Delivered.Stream == uint64(nmsgs))
	require_True(t, ci.AckFloor.Consumer == uint64(nmsgs+keep))
	require_True(t, ci.AckFloor.Stream == uint64(nmsgs))
	require_True(t, ci.NumAckPending == 0)
	require_True(t, ci.NumPending == uint64(nmsgs))
	require_True(t, ci.NumRedelivered == 0)

	msgs, err = sub.Fetch(nmsgs)
	require_NoError(t, err)
	require_True(t, len(msgs) == nmsgs)

	// Ack all again
	for _, m := range msgs {
		m.Ack()
	}
	nc.Flush()

	ci, err = js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)
	// Make sure we calculated correctly.
	require_True(t, ci.Delivered.Consumer == uint64(nmsgs*2+keep))
	require_True(t, ci.Delivered.Stream == uint64(nmsgs*2))
	require_True(t, ci.AckFloor.Consumer == uint64(nmsgs*2+keep))
	require_True(t, ci.AckFloor.Stream == uint64(nmsgs*2))
	require_True(t, ci.NumAckPending == 0)
	require_True(t, ci.NumPending == 0)
	require_True(t, ci.NumRedelivered == 0)
}

func TestJetStreamPurgeWithRedeliveredPending(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	nmsgs := 100
	for i := 0; i < nmsgs; i++ {
		sendStreamMsg(t, nc, "foo", "OK")
	}
	sub, err := js.PullSubscribe("foo", "dlc", nats.AckWait(time.Second))
	require_NoError(t, err)

	// Queue up all for ack pending.
	msgs, err := sub.Fetch(nmsgs)
	require_NoError(t, err)
	require_True(t, len(msgs) == nmsgs)

	// Should be able to be redelivered now.
	time.Sleep(2 * time.Second)

	// Queue up all for ack pending again.
	msgs, err = sub.Fetch(nmsgs)
	require_NoError(t, err)
	require_True(t, len(msgs) == nmsgs)

	require_NoError(t, js.PurgeStream("TEST"))

	ci, err := js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)

	require_True(t, ci.Delivered.Consumer == uint64(2*nmsgs))
	require_True(t, ci.Delivered.Stream == uint64(nmsgs))
	require_True(t, ci.AckFloor.Consumer == uint64(2*nmsgs))
	require_True(t, ci.AckFloor.Stream == uint64(nmsgs))
	require_True(t, ci.NumAckPending == 0)
	require_True(t, ci.NumPending == 0)
	require_True(t, ci.NumRedelivered == 0)
}

func TestJetStreamConsumerAckFloorWithExpired(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		MaxAge:   2 * time.Second,
	})
	require_NoError(t, err)

	nmsgs := 100
	for i := 0; i < nmsgs; i++ {
		sendStreamMsg(t, nc, "foo", "OK")
	}
	sub, err := js.PullSubscribe("foo", "dlc", nats.AckWait(time.Second))
	require_NoError(t, err)

	// Queue up all for ack pending.
	msgs, err := sub.Fetch(nmsgs)
	require_NoError(t, err)
	require_True(t, len(msgs) == nmsgs)

	// Let all messages expire.
	time.Sleep(3 * time.Second)

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 0)

	ci, err := js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)

	require_True(t, ci.Delivered.Consumer == uint64(nmsgs))
	require_True(t, ci.Delivered.Stream == uint64(nmsgs))
	require_True(t, ci.AckFloor.Consumer == uint64(nmsgs))
	require_True(t, ci.AckFloor.Stream == uint64(nmsgs))
	require_True(t, ci.NumAckPending == 0)
	require_True(t, ci.NumPending == 0)
	require_True(t, ci.NumRedelivered == 0)
}

func TestJetStreamConsumerIsFiltered(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	acc := s.GlobalAccount()

	tests := []struct {
		name           string
		streamSubjects []string
		filters        []string
		isFiltered     bool
	}{
		{
			name:           "single_subject",
			streamSubjects: []string{"one"},
			filters:        []string{"one"},
			isFiltered:     false,
		},
		{
			name:           "single_subject_filtered",
			streamSubjects: []string{"one.>"},
			filters:        []string{"one.filter"},
			isFiltered:     true,
		},
		{
			name:           "multi_subject_non_filtered",
			streamSubjects: []string{"multi", "foo", "bar.>"},
			filters:        []string{"multi", "bar.>", "foo"},
			isFiltered:     false,
		},
		{
			name:           "multi_subject_filtered_wc",
			streamSubjects: []string{"events", "data"},
			filters:        []string{"data"},
			isFiltered:     true,
		},
		{
			name:           "multi_subject_filtered",
			streamSubjects: []string{"machines", "floors"},
			filters:        []string{"machines"},
			isFiltered:     true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mset, err := acc.addStream(&StreamConfig{
				Name:     test.name,
				Subjects: test.streamSubjects,
			})
			require_NoError(t, err)

			o, err := mset.addConsumer(&ConsumerConfig{
				FilterSubjects: test.filters,
				Durable:        test.name,
			})
			require_NoError(t, err)

			require_True(t, o.isFiltered() == test.isFiltered)
		})
	}
}

func TestJetStreamConsumerWithFormattingSymbol(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "Test%123",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "foo", "OK")
	}

	_, err = js.AddConsumer("Test%123", &nats.ConsumerConfig{
		Durable:        "Test%123",
		FilterSubject:  "foo",
		DeliverSubject: "bar",
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("foo", nats.Bind("Test%123", "Test%123"))
	require_NoError(t, err)

	_, err = sub.NextMsg(time.Second * 5)
	require_NoError(t, err)
}

func TestJetStreamStreamUpdateWithExternalSource(t *testing.T) {
	ho := DefaultTestOptions
	ho.Port = -1
	ho.LeafNode.Host = "127.0.0.1"
	ho.LeafNode.Port = -1
	ho.JetStream = true
	ho.JetStreamDomain = "hub"
	ho.StoreDir = t.TempDir()
	hs := RunServer(&ho)
	defer hs.Shutdown()

	lu, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ho.LeafNode.Port))
	require_NoError(t, err)

	lo1 := DefaultTestOptions
	lo1.Port = -1
	lo1.ServerName = "a-leaf"
	lo1.JetStream = true
	lo1.StoreDir = t.TempDir()
	lo1.JetStreamDomain = "a-leaf"
	lo1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{lu}}}
	l1 := RunServer(&lo1)
	defer l1.Shutdown()

	checkLeafNodeConnected(t, l1)

	// Test sources with `External` provided
	ncl, jsl := jsClientConnect(t, l1)
	defer ncl.Close()

	// Hub stream.
	_, err = jsl.AddStream(&nats.StreamConfig{Name: "stream", Subjects: []string{"leaf"}})
	require_NoError(t, err)

	nch, jsh := jsClientConnect(t, hs)
	defer nch.Close()

	// Leaf stream.
	// Both streams uses the same name, as we're testing if overlap does not check against itself
	// if `External` stream has the same name.
	_, err = jsh.AddStream(&nats.StreamConfig{
		Name:     "stream",
		Subjects: []string{"hub"},
	})
	require_NoError(t, err)

	// Add `Sources`.
	// This should not validate subjects overlap against itself.
	_, err = jsh.UpdateStream(&nats.StreamConfig{
		Name:     "stream",
		Subjects: []string{"hub"},
		Sources: []*nats.StreamSource{
			{
				Name:          "stream",
				FilterSubject: "leaf",
				External: &nats.ExternalStream{
					APIPrefix: "$JS.a-leaf.API",
				},
			},
		},
	})
	require_NoError(t, err)

	// Specifying not existing FilterSubject should also be fine, as we do not validate `External` stream.
	_, err = jsh.UpdateStream(&nats.StreamConfig{
		Name:     "stream",
		Subjects: []string{"hub"},
		Sources: []*nats.StreamSource{
			{
				Name:          "stream",
				FilterSubject: "foo",
				External: &nats.ExternalStream{
					APIPrefix: "$JS.a-leaf.API",
				},
			},
		},
	})
	require_NoError(t, err)
}

func TestJetStreamKVHistoryRegression(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for _, storage := range []nats.StorageType{nats.FileStorage, nats.MemoryStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			js.DeleteKeyValue("TEST")

			kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:  "TEST",
				History: 4,
				Storage: storage,
			})
			require_NoError(t, err)

			r1, err := kv.Create("foo", []byte("a"))
			require_NoError(t, err)

			_, err = kv.Update("foo", []byte("ab"), r1)
			require_NoError(t, err)

			err = kv.Delete("foo")
			require_NoError(t, err)

			_, err = kv.Create("foo", []byte("abc"))
			require_NoError(t, err)

			err = kv.Delete("foo")
			require_NoError(t, err)

			history, err := kv.History("foo")
			require_NoError(t, err)
			require_True(t, len(history) == 4)

			_, err = kv.Update("foo", []byte("abcd"), history[len(history)-1].Revision())
			require_NoError(t, err)

			err = kv.Purge("foo")
			require_NoError(t, err)

			_, err = kv.Create("foo", []byte("abcde"))
			require_NoError(t, err)

			err = kv.Purge("foo")
			require_NoError(t, err)

			history, err = kv.History("foo")
			require_NoError(t, err)
			require_True(t, len(history) == 1)
		})
	}
}

func TestJetStreamSnapshotRestoreStallAndHealthz(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.*"},
	})
	require_NoError(t, err)

	for i := 0; i < 1000; i++ {
		sendStreamMsg(t, nc, "orders.created", "new order")
	}

	hs := s.healthz(nil)
	if hs.Status != "ok" || hs.Error != _EMPTY_ {
		t.Fatalf("Expected health to be ok, got %+v", hs)
	}

	// Simulate the staging directory for restores. This is normally cleaned up
	// but since its at the root of the storage directory make sure healthz is not affected.
	snapDir := filepath.Join(s.getJetStream().config.StoreDir, snapStagingDir)
	require_NoError(t, os.MkdirAll(snapDir, defaultDirPerms))

	// Make sure healthz ok.
	hs = s.healthz(nil)
	if hs.Status != "ok" || hs.Error != _EMPTY_ {
		t.Fatalf("Expected health to be ok, got %+v", hs)
	}
}

// https://github.com/nats-io/nats-server/pull/4163
func TestJetStreamMaxBytesIgnored(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		MaxBytes: 10 * 1024 * 1024,
	})
	require_NoError(t, err)

	msg := bytes.Repeat([]byte("A"), 1024*1024)

	for i := 0; i < 10; i++ {
		_, err := js.Publish("x", msg)
		require_NoError(t, err)
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 9)

	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()

	// We will remove the idx file and truncate the blk and fss files.
	mdir := filepath.Join(sd, "$G", "streams", "TEST", "msgs")
	// Remove idx
	err = os.Remove(filepath.Join(mdir, "1.idx"))
	require_NoError(t, err)
	// Truncate fss
	err = os.WriteFile(filepath.Join(mdir, "1.fss"), nil, defaultFilePerms)
	require_NoError(t, err)
	// Truncate blk
	err = os.WriteFile(filepath.Join(mdir, "1.blk"), nil, defaultFilePerms)
	require_NoError(t, err)

	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	for i := 0; i < 10; i++ {
		_, err := js.Publish("x", msg)
		require_NoError(t, err)
	}

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Bytes <= 10*1024*1024)
}

func TestJetStreamLastSequenceBySubjectConcurrent(t *testing.T) {
	for _, st := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(st.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "JSC", 3)
			defer c.shutdown()

			nc0, js0 := jsClientConnect(t, c.randomServer())
			defer nc0.Close()

			nc1, js1 := jsClientConnect(t, c.randomServer())
			defer nc1.Close()

			cfg := StreamConfig{
				Name:     "KV",
				Subjects: []string{"kv.>"},
				Storage:  st,
				Replicas: 3,
			}

			req, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Do manually for now.
			m, err := nc0.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			require_NoError(t, err)
			si, err := js0.StreamInfo("KV")
			if err != nil {
				t.Fatalf("Unexpected error: %v, respmsg: %q", err, string(m.Data))
			}
			if si == nil || si.Config.Name != "KV" {
				t.Fatalf("StreamInfo is not correct %+v", si)
			}

			pub := func(js nats.JetStreamContext, subj, data, seq string) {
				t.Helper()
				m := nats.NewMsg(subj)
				m.Data = []byte(data)
				m.Header.Set(JSExpectedLastSubjSeq, seq)
				js.PublishMsg(m)
			}

			ready := make(chan struct{})
			wg := &sync.WaitGroup{}
			wg.Add(2)

			go func() {
				<-ready
				pub(js0, "kv.foo", "0-0", "0")
				pub(js0, "kv.foo", "0-1", "1")
				pub(js0, "kv.foo", "0-2", "2")
				wg.Done()
			}()

			go func() {
				<-ready
				pub(js1, "kv.foo", "1-0", "0")
				pub(js1, "kv.foo", "1-1", "1")
				pub(js1, "kv.foo", "1-2", "2")
				wg.Done()
			}()

			time.Sleep(50 * time.Millisecond)
			close(ready)
			wg.Wait()

			// Read the messages.
			sub, err := js0.PullSubscribe(_EMPTY_, _EMPTY_, nats.BindStream("KV"))
			require_NoError(t, err)
			msgs, err := sub.Fetch(10)
			require_NoError(t, err)
			if len(msgs) != 3 {
				t.Errorf("Expected 3 messages, got %d", len(msgs))
			}
			for i, m := range msgs {
				if m.Header.Get(JSExpectedLastSubjSeq) != fmt.Sprint(i) {
					t.Errorf("Expected %d for last sequence, got %q", i, m.Header.Get(JSExpectedLastSubjSeq))
				}
			}
		})
	}
}

func TestJetStreamServerReencryption(t *testing.T) {
	storeDir := t.TempDir()

	for i, algo := range []struct {
		from string
		to   string
	}{
		{"aes", "aes"},
		{"aes", "chacha"},
		{"chacha", "chacha"},
		{"chacha", "aes"},
	} {
		t.Run(fmt.Sprintf("%s_to_%s", algo.from, algo.to), func(t *testing.T) {
			streamName := fmt.Sprintf("TEST_%d", i)
			subjectName := fmt.Sprintf("foo_%d", i)
			expected := 30

			checkStream := func(js nats.JetStreamContext) {
				si, err := js.StreamInfo(streamName)
				if err != nil {
					t.Fatal(err)
				}

				if si.State.Msgs != uint64(expected) {
					t.Fatalf("Should be %d messages but got %d messages", expected, si.State.Msgs)
				}

				sub, err := js.PullSubscribe(subjectName, "")
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				c := 0
				for _, m := range fetchMsgs(t, sub, expected, 5*time.Second) {
					m.AckSync()
					c++
				}
				if c != expected {
					t.Fatalf("Should have read back %d messages but got %d messages", expected, c)
				}
			}

			// First off, we start up using the original encryption key and algorithm.
			// We'll create a stream and populate it with some messages.
			t.Run("setup", func(t *testing.T) {
				conf := createConfFile(t, []byte(fmt.Sprintf(`
					server_name: S22
					listen: 127.0.0.1:-1
					jetstream: {
						key: %q,
						cipher: %s,
						store_dir: %q
					}
				`, "firstencryptionkey", algo.from, storeDir)))

				s, _ := RunServerWithConfig(conf)
				defer s.Shutdown()

				nc, js := jsClientConnect(t, s)
				defer nc.Close()

				cfg := &nats.StreamConfig{
					Name:     streamName,
					Subjects: []string{subjectName},
				}
				if _, err := js.AddStream(cfg); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				for i := 0; i < expected; i++ {
					if _, err := js.Publish(subjectName, []byte("ENCRYPTED PAYLOAD!!")); err != nil {
						t.Fatalf("Unexpected publish error: %v", err)
					}
				}

				checkStream(js)
			})

			// Next up, we will restart the server, this time with both the new key
			// and algorithm and also the old key. At startup, the server will detect
			// the change in encryption key and/or algorithm and re-encrypt the stream.
			t.Run("reencrypt", func(t *testing.T) {
				conf := createConfFile(t, []byte(fmt.Sprintf(`
					server_name: S22
					listen: 127.0.0.1:-1
					jetstream: {
						key: %q,
						cipher: %s,
						prev_key: %q,
						store_dir: %q
					}
				`, "secondencryptionkey", algo.to, "firstencryptionkey", storeDir)))

				s, _ := RunServerWithConfig(conf)
				defer s.Shutdown()

				nc, js := jsClientConnect(t, s)
				defer nc.Close()

				checkStream(js)
			})

			// Finally, we'll restart the server using only the new key and algorithm.
			// At this point everything should have been re-encrypted, so we should still
			// be able to access the stream.
			t.Run("restart", func(t *testing.T) {
				conf := createConfFile(t, []byte(fmt.Sprintf(`
					server_name: S22
					listen: 127.0.0.1:-1
					jetstream: {
						key: %q,
						cipher: %s,
						store_dir: %q
					}
				`, "secondencryptionkey", algo.to, storeDir)))

				s, _ := RunServerWithConfig(conf)
				defer s.Shutdown()

				nc, js := jsClientConnect(t, s)
				defer nc.Close()

				checkStream(js)
			})
		})
	}
}

func TestJetStreamLimitsToInterestPolicy(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.leader())
	defer nc.Close()

	// This is the index of the consumer that we'll create as R1
	// instead of R3, just to prove that it blocks the stream
	// update from happening properly.
	singleReplica := 3

	streamCfg := nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Storage:   nats.MemoryStorage,
		Replicas:  3,
	}

	stream, err := js.AddStream(&streamCfg)
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		replicas := streamCfg.Replicas
		if i == singleReplica {
			// Make one of the consumers R1 so that we can check
			// that the switch to interest-based retention is also
			// turning it into an R3 consumer.
			replicas = 1
		}
		cname := fmt.Sprintf("test_%d", i)
		_, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
			Name:      cname,
			Durable:   cname,
			AckPolicy: nats.AckAllPolicy,
			Replicas:  replicas,
		})
		require_NoError(t, err)
	}

	for i := 0; i < 20; i++ {
		_, err := js.Publish("foo", []byte{1, 2, 3, 4, 5})
		require_NoError(t, err)
	}

	// Pull 10 or more messages from the stream. We will never pull
	// less than 10, which guarantees that the lowest ack floor of
	// all consumers should be 10.
	for i := 0; i < 10; i++ {
		cname := fmt.Sprintf("test_%d", i)
		count := 10 + i // At least 10 messages

		sub, err := js.PullSubscribe("foo", cname)
		require_NoError(t, err)

		msgs, err := sub.Fetch(count)
		require_NoError(t, err)
		require_Equal(t, len(msgs), count)
		require_NoError(t, msgs[len(msgs)-1].AckSync())

		// At this point the ack floor should match the count of
		// messages we received.
		info, err := js.ConsumerInfo("TEST", cname)
		require_NoError(t, err)
		require_Equal(t, info.AckFloor.Consumer, uint64(count))
	}

	// Try updating to interest-based. This should fail because
	// we have a consumer that is R1 on an R3 stream.
	streamCfg = stream.Config
	streamCfg.Retention = nats.InterestPolicy
	_, err = js.UpdateStream(&streamCfg)
	require_Error(t, err)

	// Now we'll make the R1 consumer an R3.
	cname := fmt.Sprintf("test_%d", singleReplica)
	cinfo, err := js.ConsumerInfo("TEST", cname)
	require_NoError(t, err)

	cinfo.Config.Replicas = streamCfg.Replicas
	_, _ = js.UpdateConsumer("TEST", &cinfo.Config)
	// TODO(nat): The jsConsumerCreateRequest update doesn't always
	// respond when there are no errors updating a consumer, so this
	// nearly always returns a timeout, despite actually doing what
	// it should. We'll make sure the replicas were updated by doing
	// another consumer info just to be sure.
	// require_NoError(t, err)
	c.waitOnAllCurrent()
	cinfo, err = js.ConsumerInfo("TEST", cname)
	require_NoError(t, err)
	require_Equal(t, cinfo.Config.Replicas, streamCfg.Replicas)
	require_Equal(t, len(cinfo.Cluster.Replicas), streamCfg.Replicas-1)

	// This time it should succeed.
	_, err = js.UpdateStream(&streamCfg)
	require_NoError(t, err)

	// We need to wait for all nodes to have applied the new stream
	// configuration.
	c.waitOnAllCurrent()

	// Now we should only have 10 messages left in the stream, as
	// each consumer has acked at least the first 10 messages.
	info, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, info.State.FirstSeq, 11)
	require_Equal(t, info.State.Msgs, 10)
}
