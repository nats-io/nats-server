// Copyright 2019-2020 The NATS Authors
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

package test

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/server/sysmem"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

func TestJetStreamBasicNilConfig(t *testing.T) {
	s := RunRandClientPortServer()
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
	// Check store path, should be tmpdir.
	expectedDir := filepath.Join(os.TempDir(), server.JetStreamStoreDir)
	if config.StoreDir != expectedDir {
		t.Fatalf("Expected storage directory of %q, but got %q", expectedDir, config.StoreDir)
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

func RunBasicJetStreamServer() *server.Server {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return RunServer(&opts)
}

func RunJetStreamServerOnPort(port int) *server.Server {
	opts := DefaultTestOptions
	opts.Port = port
	opts.JetStream = true
	return RunServer(&opts)
}

func clientConnectToServer(t *testing.T, s *server.Server) *nats.Conn {
	nc, err := nats.Connect(s.ClientURL(), nats.ReconnectWait(5*time.Millisecond), nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	return nc
}

func clientConnectWithOldRequest(t *testing.T, s *server.Server) *nats.Conn {
	nc, err := nats.Connect(s.ClientURL(), nats.UseOldRequestStyle())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	return nc
}

func TestJetStreamEnableAndDisableAccount(t *testing.T) {
	s := RunBasicJetStreamServer()
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
		t.Fatalf("Expected reserved memory and store to be 0, got %v and %v", server.FriendlyBytes(rm), server.FriendlyBytes(rd))
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
	acc = server.NewAccount("$BAZ")
	if err := acc.EnableJetStream(nil); err == nil {
		t.Fatalf("Expected error on enabling account that was not registered")
	}
}

func TestJetStreamAddStream(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.FileStorage,
				Replicas:  1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			nc.Publish("foo", []byte("Hello World!"))
			nc.Flush()

			state := mset.State()
			if state.Msgs != 1 {
				t.Fatalf("Expected 1 message, got %d", state.Msgs)
			}
			if state.Bytes == 0 {
				t.Fatalf("Expected non-zero bytes")
			}

			nc.Publish("foo", []byte("Hello World Again!"))
			nc.Flush()

			state = mset.State()
			if state.Msgs != 2 {
				t.Fatalf("Expected 2 messages, got %d", state.Msgs)
			}

			if err := mset.Delete(); err != nil {
				t.Fatalf("Got an error deleting the stream: %v", err)
			}
		})
	}
}

func TestJetStreamConsumerMaxDeliveries(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Queue up our work item.
			sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			maxDeliver := 5
			ackWait := 10 * time.Millisecond

			o, err := mset.AddConsumer(&server.ConsumerConfig{
				Delivery:   sub.Subject,
				DeliverAll: true,
				AckPolicy:  server.AckExplicit,
				AckWait:    ackWait,
				MaxDeliver: maxDeliver,
			})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.Delete()

			// Wait for redeliveries to pile up.
			checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxDeliver {
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

func TestJetStreamAddStreamMaxMsgSize(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &server.StreamConfig{
				Name:       "foo",
				Retention:  server.LimitsPolicy,
				MaxAge:     time.Hour,
				Storage:    server.MemoryStorage,
				MaxMsgSize: 22,
				Replicas:   1,
			}},
		{name: "FileStore",
			mconfig: &server.StreamConfig{
				Name:       "foo",
				Retention:  server.LimitsPolicy,
				MaxAge:     time.Hour,
				Storage:    server.FileStorage,
				MaxMsgSize: 22,
				Replicas:   1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			if _, err := nc.Request("foo", []byte("Hello World!"), time.Second); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			tooBig := []byte("1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ")
			resp, err := nc.Request("foo", tooBig, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if string(resp.Data) != "-ERR 'message size exceeds maximum allowed'" {
				t.Fatalf("Expected to get an error for maximum message size, got %q", resp.Data)
			}
		})
	}
}

func TestJetStreamAddStreamCanonicalNames(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	acc := s.GlobalAccount()

	expectErr := func(_ *server.Stream, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "can not contain") {
			t.Fatalf("Expected error but got none")
		}
	}

	expectErr(acc.AddStream(&server.StreamConfig{Name: "foo.bar"}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "foo.*"}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "foo.>"}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "*"}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: ">"}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "*>"}))
}

func TestJetStreamAddStreamOverlappingSubjects(t *testing.T) {
	mconfig := &server.StreamConfig{
		Name:     "ok",
		Subjects: []string{"foo", "bar", "baz.*", "foo.bar.baz.>"},
		Storage:  server.MemoryStorage,
	}

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	acc := s.GlobalAccount()
	mset, err := acc.AddStream(mconfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	expectErr := func(_ *server.Stream, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "subjects overlap") {
			t.Fatalf("Expected error but got none")
		}
	}

	// Test that any overlapping subjects will fail.
	expectErr(acc.AddStream(&server.StreamConfig{Name: "foo"}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "a", Subjects: []string{"baz", "bar"}}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "b", Subjects: []string{">"}}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "c", Subjects: []string{"baz.33"}}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "d", Subjects: []string{"*.33"}}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "e", Subjects: []string{"*.>"}}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "f", Subjects: []string{"foo.bar", "*.bar.>"}}))
}

func sendStreamMsg(t *testing.T, nc *nats.Conn, subject, msg string) {
	t.Helper()
	resp, _ := nc.Request(subject, []byte(msg), 100*time.Millisecond)
	if resp == nil {
		t.Fatalf("No response, possible timeout?")
	}
	if string(resp.Data) != server.OK {
		t.Fatalf("Expected a JetStreamPubAck, got %q", resp.Data)
	}
}

func expectOKResponse(t *testing.T, m *nats.Msg) {
	t.Helper()
	if m == nil {
		t.Fatalf("No response, possible timeout?")
	}
	if string(m.Data) != server.OK {
		t.Fatalf("Expected a JetStreamPubAck, got %q", m.Data)
	}
}

func TestJetStreamBasicAckPublish(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "foo", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "foo", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			for i := 0; i < 50; i++ {
				sendStreamMsg(t, nc, "foo.bar", "Hello World!")
			}
			state := mset.State()
			if state.Msgs != 50 {
				t.Fatalf("Expected 50 messages, got %d", state.Msgs)
			}
		})
	}
}

func TestJetStreamNoAckStream(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "foo", Storage: server.MemoryStorage, NoAck: true}},
		{"FileStore", &server.StreamConfig{Name: "foo", Storage: server.FileStorage, NoAck: true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			// We can use NoAck to suppress acks even when reply subjects are present.
			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			if _, err := nc.Request("foo", []byte("Hello World!"), 25*time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Expected a timeout error and no response with acks suppressed")
			}

			state := mset.State()
			if state.Msgs != 1 {
				t.Fatalf("Expected 1 message, got %d", state.Msgs)
			}
		})
	}
}

func TestJetStreamCreateConsumer(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "foo", Storage: server.MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &server.StreamConfig{Name: "foo", Storage: server.FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			// Check for basic errors.
			if _, err := mset.AddConsumer(nil); err == nil {
				t.Fatalf("Expected an error for no config")
			}

			// No deliver subject, meaning its in pull mode, work queue mode means it is required to
			// do explicit ack.
			if _, err := mset.AddConsumer(&server.ConsumerConfig{}); err == nil {
				t.Fatalf("Expected an error on work queue / pull mode without explicit ack mode")
			}

			// Check for delivery subject errors.

			// Literal delivery subject required.
			if _, err := mset.AddConsumer(&server.ConsumerConfig{Delivery: "foo.*"}); err == nil {
				t.Fatalf("Expected an error on bad delivery subject")
			}
			// Check for cycles
			if _, err := mset.AddConsumer(&server.ConsumerConfig{Delivery: "foo"}); err == nil {
				t.Fatalf("Expected an error on delivery subject that forms a cycle")
			}
			if _, err := mset.AddConsumer(&server.ConsumerConfig{Delivery: "bar"}); err == nil {
				t.Fatalf("Expected an error on delivery subject that forms a cycle")
			}
			if _, err := mset.AddConsumer(&server.ConsumerConfig{Delivery: "*"}); err == nil {
				t.Fatalf("Expected an error on delivery subject that forms a cycle")
			}

			// StartPosition conflicts
			if _, err := mset.AddConsumer(&server.ConsumerConfig{
				Delivery:  "A",
				StreamSeq: 1,
				StartTime: time.Now(),
			}); err == nil {
				t.Fatalf("Expected an error on start position conflicts")
			}
			if _, err := mset.AddConsumer(&server.ConsumerConfig{
				Delivery:   "A",
				StartTime:  time.Now(),
				DeliverAll: true,
			}); err == nil {
				t.Fatalf("Expected an error on start position conflicts")
			}
			if _, err := mset.AddConsumer(&server.ConsumerConfig{
				Delivery:    "A",
				DeliverAll:  true,
				DeliverLast: true,
			}); err == nil {
				t.Fatalf("Expected an error on start position conflicts")
			}

			// Non-Durables need to have subscription to delivery subject.
			delivery := nats.NewInbox()
			if _, err := mset.AddConsumer(&server.ConsumerConfig{Delivery: delivery}); err == nil {
				t.Fatalf("Expected an error on unsubscribed delivery subject")
			}

			// Pull-based consumers are required to be durable since we do not know when they should
			// be cleaned up.
			if _, err := mset.AddConsumer(&server.ConsumerConfig{
				DeliverAll: true,
				AckPolicy:  server.AckExplicit,
			}); err == nil {
				t.Fatalf("Expected an error on pull-based that is non-durable.")
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()
			sub, _ := nc.SubscribeSync(delivery)
			defer sub.Unsubscribe()
			nc.Flush()

			// Subjects can not be AckAll.
			if _, err := mset.AddConsumer(&server.ConsumerConfig{
				Delivery:      delivery,
				DeliverAll:    true,
				FilterSubject: "foo",
				AckPolicy:     server.AckAll,
			}); err == nil {
				t.Fatalf("Expected an error on partitioned consumer with ack policy of all")
			}

			// This should work..
			o, err := mset.AddConsumer(&server.ConsumerConfig{Delivery: delivery})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}

			if err := mset.DeleteConsumer(o); err != nil {
				t.Fatalf("Expected no error on delete, got %v", err)
			}
		})
	}
}

func TestJetStreamBasicDelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MSET", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "MSET", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 100
			sendSubj := "foo.bar"
			for i := 1; i <= toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, strconv.Itoa(i))
			}
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			// Now create an consumer. Use different connection.
			nc2 := clientConnectToServer(t, s)
			defer nc2.Close()

			sub, _ := nc2.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc2.Flush()

			o, err := mset.AddConsumer(&server.ConsumerConfig{Delivery: sub.Subject, DeliverAll: true})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

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
					if seq := o.SeqFromReply(m.Reply); seq != uint64(i+seqOff) {
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
			state = mset.State()
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
			o.Delete()

			// Now check for deliver last, deliver new and deliver by seq.
			o, err = mset.AddConsumer(&server.ConsumerConfig{Delivery: sub.Subject, DeliverLast: true})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			m, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("Did not get expected message, got %v", err)
			}
			// All Consumers start with sequence #1.
			if seq := o.SeqFromReply(m.Reply); seq != 1 {
				t.Fatalf("Expected sequence to be 1, but got %d", seq)
			}
			// Check that is is the last msg we sent though.
			if mseq, _ := strconv.Atoi(string(m.Data)); mseq != 200 {
				t.Fatalf("Expected messag sequence to be 200, but got %d", mseq)
			}

			checkSubEmpty()
			o.Delete()

			o, err = mset.AddConsumer(&server.ConsumerConfig{Delivery: sub.Subject}) // Default is deliver new only.
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			// Make sure we only got one message.
			if m, err := sub.NextMsg(5 * time.Millisecond); err == nil {
				t.Fatalf("Expected no msg, got %+v", m)
			}

			checkSubEmpty()
			o.Delete()

			// Now try by sequence number.
			o, err = mset.AddConsumer(&server.ConsumerConfig{Delivery: sub.Subject, StreamSeq: 101})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			checkMsgs(1)
		})
	}
}

func workerModeConfig(name string) *server.ConsumerConfig {
	return &server.ConsumerConfig{Durable: name, DeliverAll: true, AckPolicy: server.AckExplicit}
}

func TestJetStreamBasicWorkQueue(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			// Create basic work queue mode consumer.
			oname := "WQ"
			o, err := mset.AddConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			if o.NextSeq() != 1 {
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
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			getNext := func(seqno int) {
				t.Helper()
				nextMsg, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error for seq %d: %v", seqno, err)
				}
				if nextMsg.Subject != "bar" {
					t.Fatalf("Expected subject of %q, got %q", "bar", nextMsg.Subject)
				}
				if seq := o.SeqFromReply(nextMsg.Reply); seq != uint64(seqno) {
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
				nc.Request(sendSubj, []byte("Hello World!"), 100*time.Millisecond)
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
					time.Sleep(time.Millisecond)
				}
			}()

			for i := toSend + 2; i < toSend*2+2; i++ {
				getNext(i)
			}
		})
	}
}

func TestJetStreamSubjecting(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MSET", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "MSET", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 50
			subjA := "foo.A"
			subjB := "foo.B"

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subjA, "Hello World!")
				sendStreamMsg(t, nc, subjB, "Hello World!")
			}
			state := mset.State()
			if state.Msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, state.Msgs)
			}

			delivery := nats.NewInbox()
			sub, _ := nc.SubscribeSync(delivery)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.AddConsumer(&server.ConsumerConfig{Delivery: delivery, FilterSubject: subjB, DeliverAll: true})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			// Now let's check the messages
			for i := 1; i <= toSend; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				// JetStream will have the subject match the stream subject, not delivery subject.
				// We want these to only be subjB.
				if m.Subject != subjB {
					t.Fatalf("Expected original subject of %q, but got %q", subjB, m.Subject)
				}
				// Now check that reply subject exists and has a sequence as the last token.
				if seq := o.SeqFromReply(m.Reply); seq != uint64(i) {
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

func TestJetStreamWorkQueueSubjecting(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 50
			subjA := "foo.A"
			subjB := "foo.B"

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subjA, "Hello World!")
				sendStreamMsg(t, nc, subjB, "Hello World!")
			}
			state := mset.State()
			if state.Msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, state.Msgs)
			}

			oname := "WQ"
			o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: oname, FilterSubject: subjA, DeliverAll: true, AckPolicy: server.AckExplicit})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			if o.NextSeq() != 1 {
				t.Fatalf("Expected to be starting at sequence 1")
			}

			getNext := func(seqno int) {
				t.Helper()
				nextMsg, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if nextMsg.Subject != subjA {
					t.Fatalf("Expected subject of %q, got %q", subjA, nextMsg.Subject)
				}
				if seq := o.SeqFromReply(nextMsg.Reply); seq != uint64(seqno) {
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

func TestJetStreamWorkQueueAckAndNext(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			// Create basic work queue mode consumer.
			oname := "WQ"
			o, err := mset.AddConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			if o.NextSeq() != 1 {
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
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			// Kick things off.
			// For normal work queue semantics, you send requests to the subject with stream and consumer name.
			// We will do this to start it off then use ack+next to get other messages.
			nc.PublishRequest(o.RequestNextMsgSubject(), sub.Subject, nil)

			for i := 0; i < toSend; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error waiting for messages: %v", err)
				}
				nc.PublishRequest(m.Reply, sub.Subject, server.AckNext)
			}
		})
	}
}

func TestJetStreamWorkQueueRequestBatch(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			// Create basic work queue mode consumer.
			oname := "WQ"
			o, err := mset.AddConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			if o.NextSeq() != 1 {
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
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			// For normal work queue semantics, you send requests to the subject with stream and consumer name.
			// We will do this to start it off then use ack+next to get other messages.
			// Kick things off with batch size of 50.
			batchSize := 50
			nc.PublishRequest(o.RequestNextMsgSubject(), sub.Subject, []byte(strconv.Itoa(batchSize)))

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
		mconfig *server.StreamConfig
	}{
		{name: "MemoryStore", mconfig: &server.StreamConfig{
			Name:      "MWQ",
			Storage:   server.MemoryStorage,
			Subjects:  []string{"MY_WORK_QUEUE.*"},
			Retention: server.WorkQueuePolicy},
		},
		{name: "FileStore", mconfig: &server.StreamConfig{
			Name:      "MWQ",
			Storage:   server.FileStorage,
			Subjects:  []string{"MY_WORK_QUEUE.*"},
			Retention: server.WorkQueuePolicy},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			// This type of stream has restrictions which we will test here.
			// DeliverAll is only start mode allowed.
			if _, err := mset.AddConsumer(&server.ConsumerConfig{DeliverLast: true}); err == nil {
				t.Fatalf("Expected an error with anything but DeliverAll")
			}

			// We will create a non-partitioned consumer. This should succeed.
			o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "PBO", DeliverAll: true, AckPolicy: server.AckExplicit})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.Delete()

			// Now if we create another this should fail, only can have one non-partitioned.
			if _, err := mset.AddConsumer(&server.ConsumerConfig{DeliverAll: true}); err == nil {
				t.Fatalf("Expected an error on attempt for second consumer for a workqueue")
			}
			o.Delete()

			if numo := mset.NumConsumers(); numo != 0 {
				t.Fatalf("Expected to have zero consumers, got %d", numo)
			}

			// Now add in an consumer that has a partition.
			pindex := 1
			pConfig := func(pname string) *server.ConsumerConfig {
				dname := fmt.Sprintf("PPBO-%d", pindex)
				pindex += 1
				return &server.ConsumerConfig{Durable: dname, DeliverAll: true, FilterSubject: pname, AckPolicy: server.AckExplicit}
			}
			o, err = mset.AddConsumer(pConfig("MY_WORK_QUEUE.A"))
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.Delete()

			// Now creating another with separate partition should work.
			o2, err := mset.AddConsumer(pConfig("MY_WORK_QUEUE.B"))
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o2.Delete()

			// Anything that would overlap should fail though.
			if _, err := mset.AddConsumer(pConfig(">")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}
			if _, err := mset.AddConsumer(pConfig("MY_WORK_QUEUE.A")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}
			if _, err := mset.AddConsumer(pConfig("MY_WORK_QUEUE.A")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}

			o3, err := mset.AddConsumer(pConfig("MY_WORK_QUEUE.C"))
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			o.Delete()
			o2.Delete()
			o3.Delete()

			// Push based will be allowed now, including ephemerals.
			// They can not overlap etc meaning same rules as above apply.
			o4, err := mset.AddConsumer(&server.ConsumerConfig{
				Durable:    "DURABLE",
				Delivery:   "SOME.SUBJ",
				DeliverAll: true,
				AckPolicy:  server.AckExplicit,
			})
			if err != nil {
				t.Fatalf("Unexpected Error: %v", err)
			}
			defer o4.Delete()

			// Now try to create an ephemeral
			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			// This should fail at first due to conflict above.
			ephCfg := &server.ConsumerConfig{Delivery: sub.Subject, DeliverAll: true, AckPolicy: server.AckExplicit}
			if _, err := mset.AddConsumer(ephCfg); err == nil {
				t.Fatalf("Expected an error ")
			}
			// Delete of o4 should clear.
			o4.Delete()
			o5, err := mset.AddConsumer(ephCfg)
			if err != nil {
				t.Fatalf("Unexpected Error: %v", err)
			}
			defer o5.Delete()
		})
	}
}

func TestJetStreamWorkQueueAckWaitRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.MemoryStorage, Retention: server.WorkQueuePolicy}},
		{"FileStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.FileStorage, Retention: server.WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			ackWait := 100 * time.Millisecond

			o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "PBO", DeliverAll: true, AckPolicy: server.AckExplicit, AckWait: ackWait})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.Delete()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			reqNextMsgSubj := o.RequestNextMsgSubject()

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
			state = mset.State()
			if int(state.Msgs) != toSend {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			// Now consume and ack.
			for i := 1; i <= toSend; i++ {
				nc.PublishRequest(reqNextMsgSubj, sub.Subject, nil)
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error waiting for messages: %v", err)
				}
				sseq, dseq, dcount := o.ReplyInfo(m.Reply)
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
				m.Respond(nil)
			}

			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 0 {
				t.Fatalf("Did not consume all messages, still have %d", nmsgs)
			}

			// Flush acks
			nc.Flush()

			// Now check the mset as well, since we have a WorkQueue retention policy this should be empty.
			if state := mset.State(); state.Msgs != 0 {
				t.Fatalf("Expected no messages, got %d", state.Msgs)
			}
		})
	}
}

func TestJetStreamWorkQueueNakRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.MemoryStorage, Retention: server.WorkQueuePolicy}},
		{"FileStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.FileStorage, Retention: server.WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 10
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "PBO", DeliverAll: true, AckPolicy: server.AckExplicit})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.Delete()

			getMsg := func(sseq, dseq int) *nats.Msg {
				t.Helper()
				m, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, rdseq, _ := o.ReplyInfo(m.Reply)
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
			// NAK this one.
			m.Respond(server.AckNak)

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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.MemoryStorage, Retention: server.WorkQueuePolicy}},
		{"FileStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.FileStorage, Retention: server.WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 2
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			ackWait := 50 * time.Millisecond

			o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "PBO", DeliverAll: true, AckPolicy: server.AckExplicit, AckWait: ackWait})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.Delete()

			getMsg := func(sseq, dseq int) *nats.Msg {
				t.Helper()
				m, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, rdseq, _ := o.ReplyInfo(m.Reply)
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
			m.Respond(server.AckProgress)
			nc.Flush()

			// Now let's take longer than ackWait to process but signal we are working on the message.
			timeout := time.Now().Add(5 * ackWait)
			for time.Now().Before(timeout) {
				time.Sleep(ackWait / 4)
				m.Respond(server.AckProgress)
			}

			// We should get 2 here, not 1 since we have indicated we are working on it.
			m2 := getMsg(2, 3)
			time.Sleep(ackWait / 2)
			m2.Respond(server.AckProgress)

			// Now should get 1 back then 2.
			m = getMsg(1, 4)
			m.Respond(nil)

			getMsg(2, 5)
		})
	}
}

func TestJetStreamConsumerMaxDeliveryAndServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	mname := "MYS"
	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: mname, Storage: server.FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	dsubj := "D.TO"
	max := 4

	o, err := mset.AddConsumer(&server.ConsumerConfig{
		Durable:    "TO",
		Delivery:   dsubj,
		DeliverAll: true,
		AckPolicy:  server.AckExplicit,
		AckWait:    25 * time.Millisecond,
		MaxDeliver: max,
	})
	defer o.Delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sub, _ := nc.SubscribeSync(dsubj)
	defer sub.Unsubscribe()

	// Send one message.
	sendStreamMsg(t, nc, mname, "order-1")

	checkSubPending := func(numExpected int) {
		t.Helper()
		checkFor(t, 150*time.Millisecond, 10*time.Millisecond, func() error {
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != numExpected {
				return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
			}
			return nil
		})
	}

	checkNumMsgs := func(numExpected uint64) {
		t.Helper()
		mset, err = s.GlobalAccount().LookupStream(mname)
		if err != nil {
			t.Fatalf("Expected to find a stream for %q", mname)
		}
		state := mset.State()
		if state.Msgs != numExpected {
			t.Fatalf("Expected %d msgs, got %d", numExpected, state.Msgs)
		}
	}

	// Wait til we know we have max queue up.
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
		// Stop current server.
		s.Shutdown()
		// Restart.
		s = RunJetStreamServerOnPort(port)
	}

	// Restart.
	restartServer()
	defer s.Shutdown()

	checkNumMsgs(2)

	// Wait for client to be reconnected.
	checkFor(t, 2500*time.Millisecond, 5*time.Millisecond, func() error {
		if !nc.IsConnected() {
			return fmt.Errorf("Not connected")
		}
		return nil
	})

	// Once we are here send third order.
	// Send third
	sendStreamMsg(t, nc, mname, "order-3")

	checkNumMsgs(3)

	// Restart.
	restartServer()
	defer s.Shutdown()

	checkNumMsgs(3)

	// Now we should have max times three on our sub.
	checkSubPending(max * 3)
}

func TestJetStreamDeleteConsumerAndServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	sendSubj := "MYQ"
	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: sendSubj, Storage: server.FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	// Create basic work queue mode consumer.
	oname := "WQ"
	o, err := mset.AddConsumer(workerModeConfig(oname))
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}

	// Now delete and then we will restart the server.
	o.Delete()

	if numo := mset.NumConsumers(); numo != 0 {
		t.Fatalf("Expected to have zero consumers, got %d", numo)
	}

	// Stop current server.
	s.Shutdown()

	// Restart.
	s = RunBasicJetStreamServer()
	defer s.Shutdown()

	mset, err = s.GlobalAccount().LookupStream(sendSubj)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", sendSubj)
	}

	if numo := mset.NumConsumers(); numo != 0 {
		t.Fatalf("Expected to have zero consumers, got %d", numo)
	}
}

func TestJetStreamRedeliveryAfterServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	sendSubj := "MYQ"
	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: sendSubj, Storage: server.FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Now load up some messages.
	toSend := 25
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, sendSubj, "Hello World!")
	}
	state := mset.State()
	if state.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
	}

	sub, _ := nc.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()
	nc.Flush()

	o, err := mset.AddConsumer(&server.ConsumerConfig{
		Durable:    "TO",
		Delivery:   sub.Subject,
		DeliverAll: true,
		AckPolicy:  server.AckExplicit,
		AckWait:    100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer o.Delete()

	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
		}
		return nil
	})

	// Stop current server.
	s.Shutdown()

	// Restart.
	s = RunBasicJetStreamServer()
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

func TestJetStreamActiveDelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "ADS", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "ADS", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil && config.StoreDir != "" {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			sendSubj := "foo.22"
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, "Hello World!")
			}
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "to", Delivery: "d", DeliverAll: true})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "EP", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "EP", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.AddConsumer(&server.ConsumerConfig{Delivery: sub.Subject})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !o.Active() {
				t.Fatalf("Expected the consumer to be considered active")
			}
			if numo := mset.NumConsumers(); numo != 1 {
				t.Fatalf("Expected number of consumers to be 1, got %d", numo)
			}
			// Set our delete threshold to something low for testing purposes.
			o.SetInActiveDeleteThreshold(100 * time.Millisecond)

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
				if o.Active() {
					return fmt.Errorf("Expected the ephemeral consumer to be considered inactive")
				}
				return nil
			})
			// The reason for this still being 1 is that we give some time in case of a reconnect scenario.
			// We detect right away on the interest change but we wait for interest to be re-established.
			// This is in case server goes away but app is fine, we do not want to recycle those consumers.
			if numo := mset.NumConsumers(); numo != 1 {
				t.Fatalf("Expected number of consumers to be 1, got %d", numo)
			}

			// We should delete this one after the delete threshold.
			checkFor(t, time.Second, 100*time.Millisecond, func() error {
				if numo := mset.NumConsumers(); numo != 0 {
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "ET", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "ET", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			// Capture the subscription.
			delivery := sub.Subject

			o, err := mset.AddConsumer(&server.ConsumerConfig{Delivery: delivery, AckPolicy: server.AckExplicit})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !o.Active() {
				t.Fatalf("Expected the consumer to be considered active")
			}
			if numo := mset.NumConsumers(); numo != 1 {
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
				if seq := o.SeqFromReply(m.Reply); seq != uint64(seqno) {
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
					if o.Active() {
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DT", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "DT", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			dname := "d22"
			subj1 := nats.NewInbox()

			o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: dname, Delivery: subj1, AckPolicy: server.AckExplicit})
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

			checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
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
				if seq := o.SeqFromReply(m.Reply); seq != uint64(seqno) {
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

			// We should not be able to try to add an observer with the same name.
			if _, err := mset.AddConsumer(&server.ConsumerConfig{Durable: dname, Delivery: subj1, AckPolicy: server.AckExplicit}); err == nil {
				t.Fatalf("Expected and error trying to add a new durable consumer while first still active")
			}

			// Now unsubscribe and wait to become inactive
			sub.Unsubscribe()
			checkFor(t, 250*time.Millisecond, 50*time.Millisecond, func() error {
				if o.Active() {
					return fmt.Errorf("Consumer is still active")
				}
				return nil
			})

			// Now we should be able to replace the delivery subject.
			subj2 := nats.NewInbox()
			sub, _ = nc.SubscribeSync(subj2)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err = mset.AddConsumer(&server.ConsumerConfig{Durable: dname, Delivery: subj2, AckPolicy: server.AckExplicit})
			if err != nil {
				t.Fatalf("Unexpected error trying to add a new durable consumer: %v", err)
			}

			// We should get the remaining messages here.
			for i := toSend / 2; i <= toSend; i++ {
				m := getMsg(i)
				m.Respond(nil)
			}
		})
	}
}

func TestJetStreamDurableSubjectedConsumerReconnect(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DT", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "DT", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sendMsgs := func(toSend int) {
				for i := 0; i < toSend; i++ {
					var subj string
					if i%2 == 0 {
						subj = "foo.AA"
					} else {
						subj = "foo.ZZ"
					}
					if err := nc.Publish(subj, []byte("OK!")); err != nil {
						return
					}
				}
				nc.Flush()
			}

			// Send 50 msgs
			toSend := 50
			sendMsgs(toSend)

			dname := "d33"
			dsubj := nats.NewInbox()

			// Now create an consumer for foo.AA, only requesting the last one.
			o, err := mset.AddConsumer(&server.ConsumerConfig{
				Durable:       dname,
				Delivery:      dsubj,
				FilterSubject: "foo.AA",
				DeliverLast:   true,
				AckPolicy:     server.AckExplicit,
				AckWait:       100 * time.Millisecond,
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
				rsseq, roseq, dcount := o.ReplyInfo(m.Reply)
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
				_, roseq, dcount := o.ReplyInfo(m.Reply)
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

			state := mset.State()
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

func TestJetStreamRedeliverCount(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			for i := 0; i < 10; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
			if state := mset.State(); state.Msgs != 10 {
				t.Fatalf("Expected %d messages, got %d", 10, state.Msgs)
			}

			o, err := mset.AddConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			for i := uint64(1); i <= 10; i++ {
				m, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				sseq, dseq, dcount := o.ReplyInfo(m.Reply)
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
				m.Respond(server.AckNak)
			}
		})
	}
}

func TestJetStreamCanNotNakAckd(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			for i := 0; i < 10; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
			if state := mset.State(); state.Msgs != 10 {
				t.Fatalf("Expected %d messages, got %d", 10, state.Msgs)
			}

			o, err := mset.AddConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			for i := uint64(1); i <= 10; i++ {
				m, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
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
				if err := nc.Publish(fmt.Sprintf(ackReplyT, seq, seq), server.AckNak); err != nil {
					t.Fatalf("Error sending nak: %v", err)
				}
				nc.Flush()
				if _, err := nc.Request(o.RequestNextMsgSubject(), nil, 10*time.Millisecond); err != nats.ErrTimeout {
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 100 msgs
			for i := 0; i < 100; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
			if state := mset.State(); state.Msgs != 100 {
				t.Fatalf("Expected %d messages, got %d", 100, state.Msgs)
			}
			mset.Purge()
			if state := mset.State(); state.Msgs != 0 {
				t.Fatalf("Expected %d messages, got %d", 0, state.Msgs)
			}
		})
	}
}

func TestJetStreamStreamPurgeWithConsumer(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 100 msgs
			for i := 0; i < 100; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
			if state := mset.State(); state.Msgs != 100 {
				t.Fatalf("Expected %d messages, got %d", 100, state.Msgs)
			}
			// Now create an consumer and make sure it functions properly.
			o, err := mset.AddConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()
			nextSubj := o.RequestNextMsgSubject()
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
			state := o.Info().State
			if state.AckFloor.ConsumerSeq != 50 {
				t.Fatalf("Expected ack floor of 50, got %d", state.AckFloor.ConsumerSeq)
			}
			if len(state.Pending) != 25 {
				t.Fatalf("Expected len(pending) to be 25, got %d", len(state.Pending))
			}
			// Now do purge.
			mset.Purge()
			if state := mset.State(); state.Msgs != 0 {
				t.Fatalf("Expected %d messages, got %d", 0, state.Msgs)
			}
			// Now re-acquire state and check that we did the right thing.
			// Pending should be cleared, and stream sequences should have been set
			// to the total messages before purge + 1.
			state = o.Info().State
			if len(state.Pending) != 0 {
				t.Fatalf("Expected no pending, got %d", len(state.Pending))
			}
			if state.Delivered.StreamSeq != 100 {
				t.Fatalf("Expected to have setseq now at next seq of 100, got %d", state.Delivered.StreamSeq)
			}
			// Check AckFloors which should have also been adjusted.
			if state.AckFloor.StreamSeq != 100 {
				t.Fatalf("Expected ackfloor for setseq to be 100, got %d", state.AckFloor.StreamSeq)
			}
			if state.AckFloor.ConsumerSeq != 75 {
				t.Fatalf("Expected ackfloor for obsseq to be 75, got %d", state.AckFloor.ConsumerSeq)
			}
			// Also make sure we can get new messages correctly.
			nc.Request("DC", []byte("OK-22"), time.Second)
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 100 msgs
			for i := 0; i < 100; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
			if state := mset.State(); state.Msgs != 100 {
				t.Fatalf("Expected %d messages, got %d", 100, state.Msgs)
			}
			// Now create an consumer and make sure it functions properly.
			// This will test redelivery state and purge of the stream.
			wcfg := &server.ConsumerConfig{
				Durable:    "WQ",
				DeliverAll: true,
				AckPolicy:  server.AckExplicit,
				AckWait:    20 * time.Millisecond,
			}
			o, err := mset.AddConsumer(wcfg)
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()
			nextSubj := o.RequestNextMsgSubject()
			for i := 0; i < 50; i++ {
				// Do not ack these.
				if _, err := nc.Request(nextSubj, nil, time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			// Now wait to make sure we are in a redelivered state.
			time.Sleep(wcfg.AckWait * 2)
			// Now do purge.
			mset.Purge()
			if state := mset.State(); state.Msgs != 0 {
				t.Fatalf("Expected %d messages, got %d", 0, state.Msgs)
			}
			// Now get the state and check that we did the right thing.
			// Pending should be cleared, and stream sequences should have been set
			// to the total messages before purge + 1.
			state := o.Info().State
			if len(state.Pending) != 0 {
				t.Fatalf("Expected no pending, got %d", len(state.Pending))
			}
			if state.Delivered.StreamSeq != 100 {
				t.Fatalf("Expected to have setseq now at next seq of 100, got %d", state.Delivered.StreamSeq)
			}
			// Check AckFloors which should have also been adjusted.
			if state.AckFloor.StreamSeq != 100 {
				t.Fatalf("Expected ackfloor for setseq to be 100, got %d", state.AckFloor.StreamSeq)
			}
			if state.AckFloor.ConsumerSeq != 50 {
				t.Fatalf("Expected ackfloor for obsseq to be 75, got %d", state.AckFloor.ConsumerSeq)
			}
			// Also make sure we can get new messages correctly.
			nc.Request("DC", []byte("OK-22"), time.Second)
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage, Retention: server.InterestPolicy}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage, Retention: server.InterestPolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 100 msgs
			totalMsgs := 100

			for i := 0; i < totalMsgs; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()

			checkNumMsgs := func(numExpected int) {
				t.Helper()
				if state := mset.State(); state.Msgs != uint64(numExpected) {
					t.Fatalf("Expected %d messages, got %d", numExpected, state.Msgs)
				}
			}

			checkNumMsgs(totalMsgs)

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
			mset.AddConsumer(&server.ConsumerConfig{Delivery: sub1.Subject, DeliverAll: true, AckPolicy: server.AckExplicit})

			sub2 := syncSub()
			mset.AddConsumer(&server.ConsumerConfig{Delivery: sub2.Subject, DeliverAll: true, AckPolicy: server.AckAll})

			sub3 := syncSub()
			mset.AddConsumer(&server.ConsumerConfig{Delivery: sub3.Subject, DeliverAll: true, AckPolicy: server.AckNone})

			// Wait for all messsages to be pending for each sub.
			for _, sub := range []*nats.Subscription{sub1, sub2, sub3} {
				checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != totalMsgs {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, totalMsgs)
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

			// Now ack last ackall message. This should clear all of them.
			for i := 4; i <= totalMsgs; i++ {
				if m, err := sub2.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				} else if i == totalMsgs {
					m.Respond(nil)
				}
			}
			nc.Flush()

			// Should be zero now.
			checkNumMsgs(0)
		})
	}
}

func TestJetStreamConsumerReplayRate(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage, Retention: server.InterestPolicy}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage, Retention: server.InterestPolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			totalMsgs := 10

			var gaps []time.Duration
			lst := time.Now()

			for i := 0; i < totalMsgs; i++ {
				gaps = append(gaps, time.Since(lst))
				nc.Request("DC", []byte("OK!"), time.Second)
				lst = time.Now()
				// Calculate a gap between messages.
				gap := 10*time.Millisecond + time.Duration(rand.Intn(20))*time.Millisecond
				time.Sleep(gap)
			}

			if state := mset.State(); state.Msgs != uint64(totalMsgs) {
				t.Fatalf("Expected %d messages, got %d", totalMsgs, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.AddConsumer(&server.ConsumerConfig{Delivery: sub.Subject, DeliverAll: true})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.Delete()

			// Firehose/instant which is default.
			last := time.Now()
			for i := 0; i < totalMsgs; i++ {
				if _, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				now := time.Now()
				if now.Sub(last) > 5*time.Millisecond {
					t.Fatalf("Expected firehose/instant delivery, got message gap of %v", now.Sub(last))
				}
				last = now
			}

			// Now do replay rate to match original.
			o, err = mset.AddConsumer(&server.ConsumerConfig{Delivery: sub.Subject, DeliverAll: true, ReplayPolicy: server.ReplayOriginal})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.Delete()

			// Original rate messsages were received for push based consumer.
			for i := 0; i < totalMsgs; i++ {
				start := time.Now()
				if _, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				gap := time.Since(start)
				// 10ms is high but on macs time.Sleep(delay) does not sleep only delay.
				if gap < gaps[i] || gap > gaps[i]+10*time.Millisecond {
					t.Fatalf("Gap is incorrect for %d, expected %v got %v", i, gaps[i], gap)
				}
			}

			// Now create pull based.
			oc := workerModeConfig("PM")
			oc.ReplayPolicy = server.ReplayOriginal
			o, err = mset.AddConsumer(oc)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.Delete()

			for i := 0; i < totalMsgs; i++ {
				start := time.Now()
				if _, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				gap := time.Since(start)
				// 10ms is high but on macs time.Sleep(delay) does not sleep only delay.
				if gap < gaps[i] || gap > gaps[i]+10*time.Millisecond {
					t.Fatalf("Gap is incorrect for %d, expected %v got %v", i, gaps[i], gap)
				}
			}
		})
	}
}

func TestJetStreamConsumerReplayRateNoAck(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			totalMsgs := 10
			for i := 0; i < totalMsgs; i++ {
				nc.Request("DC", []byte("Hello World"), time.Second)
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			}
			if state := mset.State(); state.Msgs != uint64(totalMsgs) {
				t.Fatalf("Expected %d messages, got %d", totalMsgs, state.Msgs)
			}
			subj := "d.dc"
			o, err := mset.AddConsumer(&server.ConsumerConfig{
				Durable:      "derek",
				Delivery:     subj,
				DeliverAll:   true,
				AckPolicy:    server.AckNone,
				ReplayPolicy: server.ReplayOriginal,
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.Delete()
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage, Retention: server.InterestPolicy}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage, Retention: server.InterestPolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 2 msgs
			nc.Request("DC", []byte("OK!"), time.Second)
			time.Sleep(100 * time.Millisecond)
			nc.Request("DC", []byte("OK!"), time.Second)

			if state := mset.State(); state.Msgs != 2 {
				t.Fatalf("Expected %d messages, got %d", 2, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			// Now do replay rate to match original.
			o, err := mset.AddConsumer(&server.ConsumerConfig{Delivery: sub.Subject, DeliverAll: true, ReplayPolicy: server.ReplayOriginal})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Allow loop and deliver / replay go routine to spin up
			time.Sleep(50 * time.Millisecond)
			base := runtime.NumGoroutine()
			o.Delete()

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
	defer s.Shutdown()

	if _, _, err := s.JetStreamReservedResources(); err == nil {
		t.Fatalf("Expected error requesting jetstream reserved resources when not enabled")
	}
	// Create some accounts.
	facc, _ := s.LookupOrRegisterAccount("FOO")
	bacc, _ := s.LookupOrRegisterAccount("BAR")
	zacc, _ := s.LookupOrRegisterAccount("BAZ")

	jsconfig := &server.JetStreamConfig{MaxMemory: 1024, MaxStore: 8192}
	if err := s.EnableJetStream(jsconfig); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if rm, rd, err := s.JetStreamReservedResources(); err != nil {
		t.Fatalf("Unexpected error requesting jetstream reserved resources: %v", err)
	} else if rm != 0 || rd != 0 {
		t.Fatalf("Expected reserved memory and store to be 0, got %d and %d", rm, rd)
	}

	limits := func(mem int64, store int64) *server.JetStreamAccountLimits {
		return &server.JetStreamAccountLimits{
			MaxMemory:    mem,
			MaxStore:     store,
			MaxStreams:   -1,
			MaxConsumers: -1,
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
		t.Fatalf("Expected reserved memory and store to be 0, got %v and %v", server.FriendlyBytes(rm), server.FriendlyBytes(rd))
	}

	if err := facc.EnableJetStream(limits(24, 192)); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Test Adjust
	l := limits(jsconfig.MaxMemory, jsconfig.MaxStore)
	l.MaxStreams = 10
	l.MaxConsumers = 10

	if err := facc.UpdateJetStreamLimits(l); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	var msets []*server.Stream
	// Now test max streams and max consumers. Note max consumers is per stream.
	for i := 0; i < 10; i++ {
		mname := fmt.Sprintf("foo.%d", i)
		mset, err := facc.AddStream(&server.StreamConfig{Name: strconv.Itoa(i), Subjects: []string{mname}})
		if err != nil {
			t.Fatalf("Unexpected error adding stream: %v", err)
		}
		msets = append(msets, mset)
	}

	// This one should fail since over the limit for max number of streams.
	if _, err := facc.AddStream(&server.StreamConfig{Name: "22", Subjects: []string{"foo.22"}}); err == nil {
		t.Fatalf("Expected error adding stream over limit")
	}

	// Remove them all
	for _, mset := range msets {
		mset.Delete()
	}

	// Now try to add one with bytes limit that would exceed the account limit.
	if _, err := facc.AddStream(&server.StreamConfig{Name: "22", MaxBytes: jsconfig.MaxMemory * 2}); err == nil {
		t.Fatalf("Expected error adding stream over limit")
	}

	// Replicas can't be > 1
	if _, err := facc.AddStream(&server.StreamConfig{Name: "22", Replicas: 10}); err == nil {
		t.Fatalf("Expected error adding stream over limit")
	}

	// Test consumers limit
	mset, err := facc.AddStream(&server.StreamConfig{Name: "22", Subjects: []string{"foo.22"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	for i := 0; i < 10; i++ {
		oname := fmt.Sprintf("O:%d", i)
		_, err := mset.AddConsumer(&server.ConsumerConfig{Durable: oname, AckPolicy: server.AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// This one should fail.
	if _, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "O:22", AckPolicy: server.AckExplicit}); err == nil {
		t.Fatalf("Expected error adding consumer over the limit")
	}
}

func TestJetStreamStreamStorageTrackingAndLimits(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	gacc := s.GlobalAccount()

	al := &server.JetStreamAccountLimits{
		MaxMemory:    8192,
		MaxStore:     -1,
		MaxStreams:   -1,
		MaxConsumers: -1,
	}

	if err := gacc.UpdateJetStreamLimits(al); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	mset, err := gacc.AddStream(&server.StreamConfig{Name: "LIMITS", Retention: server.WorkQueuePolicy})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 100
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "LIMITS", "Hello World!")
	}

	state := mset.State()
	usage := gacc.JetStreamUsage()

	// Make sure these are working correctly.
	if state.Bytes != usage.Memory {
		t.Fatalf("Expected to have stream bytes match memory usage, %d vs %d", state.Bytes, usage.Memory)
	}
	if usage.Streams != 1 {
		t.Fatalf("Expected to have 1 stream, got %d", usage.Streams)
	}

	// Do second stream.
	mset2, err := gacc.AddStream(&server.StreamConfig{Name: "NUM22", Retention: server.WorkQueuePolicy})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset2.Delete()

	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "NUM22", "Hello World!")
	}

	stats2 := mset2.State()
	usage = gacc.JetStreamUsage()

	if usage.Memory != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, account is %v, stream1 is %v, stream2 is %v", usage.Memory, state.Bytes, stats2.Bytes)
	}

	// Make sure delete works.
	mset2.Delete()
	stats2 = mset2.State()
	usage = gacc.JetStreamUsage()

	if usage.Memory != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, account is %v, stream1 is %v, stream2 is %v", usage.Memory, state.Bytes, stats2.Bytes)
	}

	// Now drain the first one by consuming the messages.
	o, err := mset.AddConsumer(workerModeConfig("WQ"))
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

	for i := 0; i < toSend; i++ {
		msg, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msg.Respond(nil)
	}
	nc.Flush()

	state = mset.State()
	usage = gacc.JetStreamUsage()

	if usage.Memory != 0 {
		t.Fatalf("Expected usage memeory to be 0, got %d", usage.Memory)
	}

	// Now send twice the number of messages. Should receive an error at some point, and we will check usage against limits.
	var errSeen string
	for i := 0; i < toSend*2; i++ {
		resp, _ := nc.Request("LIMITS", []byte("The quick brown fox jumped over the..."), 50*time.Millisecond)
		if string(resp.Data) != server.OK {
			errSeen = string(resp.Data)
			break
		}
	}

	if errSeen == "" {
		t.Fatalf("Expected to see an error when exceeding the account limits")
	}

	state = mset.State()
	usage = gacc.JetStreamUsage()

	if usage.Memory > uint64(al.MaxMemory) {
		t.Fatalf("Expected memory to not exceed limit of %d, got %d", al.MaxMemory, usage.Memory)
	}
}

func TestJetStreamStreamFileStorageTrackingAndLimits(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	gacc := s.GlobalAccount()

	al := &server.JetStreamAccountLimits{
		MaxMemory:    8192,
		MaxStore:     9600,
		MaxStreams:   -1,
		MaxConsumers: -1,
	}

	if err := gacc.UpdateJetStreamLimits(al); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	mconfig := &server.StreamConfig{Name: "LIMITS", Storage: server.FileStorage, Retention: server.WorkQueuePolicy}
	mset, err := gacc.AddStream(mconfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 100
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "LIMITS", "Hello World!")
	}

	state := mset.State()
	usage := gacc.JetStreamUsage()

	// Make sure these are working correctly.
	if usage.Store != state.Bytes {
		t.Fatalf("Expected to have stream bytes match the store usage, %d vs %d", usage.Store, state.Bytes)
	}
	if usage.Streams != 1 {
		t.Fatalf("Expected to have 1 stream, got %d", usage.Streams)
	}

	// Do second stream.
	mconfig2 := &server.StreamConfig{Name: "NUM22", Storage: server.FileStorage, Retention: server.WorkQueuePolicy}
	mset2, err := gacc.AddStream(mconfig2)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset2.Delete()

	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "NUM22", "Hello World!")
	}

	stats2 := mset2.State()
	usage = gacc.JetStreamUsage()

	if usage.Store != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, usage is %v, stream1 is %v, stream2 is %v", usage.Store, state.Bytes, stats2.Bytes)
	}

	// Make sure delete works.
	mset2.Delete()
	stats2 = mset2.State()
	usage = gacc.JetStreamUsage()

	if usage.Store != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, account is %v, stream1 is %v, stream2 is %v", usage.Store, state.Bytes, stats2.Bytes)
	}

	// Now drain the first one by consuming the messages.
	o, err := mset.AddConsumer(workerModeConfig("WQ"))
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

	for i := 0; i < toSend; i++ {
		msg, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msg.Respond(nil)
	}
	nc.Flush()

	state = mset.State()
	usage = gacc.JetStreamUsage()

	if usage.Memory != 0 {
		t.Fatalf("Expected usage memeory to be 0, got %d", usage.Memory)
	}

	// Now send twice the number of messages. Should receive an error at some point, and we will check usage against limits.
	var errSeen string
	for i := 0; i < toSend*2; i++ {
		resp, _ := nc.Request("LIMITS", []byte("The quick brown fox jumped over the..."), 50*time.Millisecond)
		if string(resp.Data) != server.OK {
			errSeen = string(resp.Data)
			break
		}
	}

	if errSeen == "" {
		t.Fatalf("Expected to see an error when exceeding the account limits")
	}

	state = mset.State()
	usage = gacc.JetStreamUsage()

	if usage.Memory > uint64(al.MaxMemory) {
		t.Fatalf("Expected memory to not exceed limit of %d, got %d", al.MaxMemory, usage.Memory)
	}
}

func TestJetStreamSimpleFileStorageRecovery(t *testing.T) {
	base := runtime.NumGoroutine()

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	config := s.JetStreamConfig()
	if config == nil {
		t.Fatalf("Expected non-nil config")
	}
	defer os.RemoveAll(config.StoreDir)

	acc := s.GlobalAccount()

	type obsi struct {
		cfg server.ConsumerConfig
		ack int
	}
	type info struct {
		cfg   server.StreamConfig
		state server.StreamState
		obs   []obsi
	}
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
		msetConfig := server.StreamConfig{
			Name:     msetName,
			Storage:  server.FileStorage,
			Subjects: subjects,
			MaxMsgs:  100,
		}
		mset, err := acc.AddStream(&msetConfig)
		if err != nil {
			t.Fatalf("Unexpected error adding stream: %v", err)
		}
		defer mset.Delete()

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
			oname := fmt.Sprintf("WQ-%d", n)
			o, err := mset.AddConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Now grab some messages.
			toReceive := rand.Intn(toSend) + 1
			for r := 0; r < toReceive; r++ {
				resp, _ := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
				if resp != nil {
					resp.Respond(nil)
				}
			}
			obs = append(obs, obsi{o.Config(), toReceive})
		}
		ostate[msetName] = info{mset.Config(), mset.State(), obs}
	}
	pusage := acc.JetStreamUsage()

	// Shutdown the server. Restart and make sure things come back.
	s.Shutdown()
	time.Sleep(200 * time.Millisecond)
	delta := (runtime.NumGoroutine() - base)
	if delta > 3 {
		fmt.Printf("%d Go routines still exist post Shutdown()", delta)
		time.Sleep(30 * time.Second)
	}

	s = RunBasicJetStreamServer()
	defer s.Shutdown()

	acc = s.GlobalAccount()

	nusage := acc.JetStreamUsage()
	if nusage != pusage {
		t.Fatalf("Usage does not match after restore: %+v vs %+v", nusage, pusage)
	}

	for mname, info := range ostate {
		mset, err := acc.LookupStream(mname)
		if err != nil {
			t.Fatalf("Expected to find a stream for %q", mname)
		}
		if state := mset.State(); state != info.state {
			t.Fatalf("Stats do not match: %+v vs %+v", state, info.state)
		}
		if cfg := mset.Config(); !reflect.DeepEqual(cfg, info.cfg) {
			t.Fatalf("Configs do not match: %+v vs %+v", cfg, info.cfg)
		}
		// Consumers.
		if mset.NumConsumers() != len(info.obs) {
			t.Fatalf("Number of consumers do not match: %d vs %d", mset.NumConsumers(), len(info.obs))
		}
		for _, oi := range info.obs {
			if o := mset.LookupConsumer(oi.cfg.Durable); o != nil {
				if uint64(oi.ack+1) != o.NextSeq() {
					t.Fatalf("Consumer next seq is not correct: %d vs %d", oi.ack+1, o.NextSeq())
				}
			} else {
				t.Fatalf("Expected to get an consumer")
			}
		}
	}
}

func TestJetStreamRequestAPI(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	// Forced cleanup of all persisted state.
	config := s.JetStreamConfig()
	if config == nil {
		t.Fatalf("Expected non-nil config")
	}
	defer os.RemoveAll(config.StoreDir)

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// This will return +OK if enabled.
	resp, _ := nc.Request(server.JetStreamEnabled, nil, time.Second)
	expectOKResponse(t, resp)

	// This will get the current information about usage and limits for this account.
	resp, err := nc.Request(server.JetStreamInfo, nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var info server.JetStreamAccountStats
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now create a stream.
	msetCfg := server.StreamConfig{
		Name:     "MSET22",
		Storage:  server.FileStorage,
		Subjects: []string{"foo", "bar", "baz"},
		MaxMsgs:  100,
	}
	req, err := json.Marshal(msetCfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, _ = nc.Request(server.JetStreamCreateStream, req, time.Second)
	expectOKResponse(t, resp)

	// Now lookup info again and see that we can see the new stream.
	resp, err = nc.Request(server.JetStreamInfo, nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err = json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Streams != 1 {
		t.Fatalf("Expected to see 1 Stream, got %d", info.Streams)
	}

	// Make sure list works.
	resp, err = nc.Request(server.JetStreamListStreams, nil, time.Second)
	var names []string
	if err = json.Unmarshal(resp.Data, &names); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(names))
	}
	if names[0] != msetCfg.Name {
		t.Fatalf("Expected to get %q, but got %q", msetCfg.Name, names[0])
	}

	// Now send some messages, then we can poll for info on this stream.
	toSend := 10
	for i := 0; i < toSend; i++ {
		nc.Request("foo", []byte("WELCOME JETSTREAM"), time.Second)
	}

	resp, err = nc.Request(fmt.Sprintf(server.JetStreamStreamInfoT, msetCfg.Name), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var msi server.StreamInfo
	if err = json.Unmarshal(resp.Data, &msi); err != nil {
		log.Fatalf("Unexpected error: %v", err)
	}
	if msi.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected to get %d msgs, got %d", toSend, msi.State.Msgs)
	}

	// Looking up one that is not there should yield an error.
	resp, err = nc.Request(fmt.Sprintf(server.JetStreamStreamInfoT, "BOB"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if string(resp.Data) != "-ERR 'stream not found'" {
		t.Fatalf("Expected to get a not found error, got %q", resp.Data)
	}

	// Now create an consumer.
	delivery := nats.NewInbox()
	obsReq := server.CreateConsumerRequest{
		Stream: msetCfg.Name,
		Config: server.ConsumerConfig{Delivery: delivery, DeliverAll: true},
	}
	req, err = json.Marshal(obsReq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err = nc.Request(fmt.Sprintf(server.JetStreamCreateConsumerT, msetCfg.Name), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Since we do not have interest this should have failed.
	if !strings.HasPrefix(string(resp.Data), "-ERR 'consumer requires interest for delivery subject when ephemeral'") {
		t.Fatalf("Got wrong error response: %q", resp.Data)
	}
	// Now create subscription and make sure we get +OK
	sub, _ := nc.SubscribeSync(delivery)
	nc.Flush()

	resp, err = nc.Request(fmt.Sprintf(server.JetStreamCreateConsumerT, msetCfg.Name), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !strings.HasPrefix(string(resp.Data), server.OK) {
		t.Fatalf("Expected OK, got %q", resp.Data)
	}

	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
		}
		return nil
	})

	// Check that we get an error if the stream name in the subject does not match the config.
	resp, err = nc.Request(fmt.Sprintf(server.JetStreamCreateConsumerT, "BOB"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Since we do not have interest this should have failed.
	if !strings.HasPrefix(string(resp.Data), "-ERR 'stream name in subject does not match request'") {
		t.Fatalf("Got wrong error response: %q", resp.Data)
	}

	// Get the list of all of the obervables for our stream.
	resp, err = nc.Request(fmt.Sprintf(server.JetStreamConsumersT, msetCfg.Name), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var onames []string
	if err = json.Unmarshal(resp.Data, &onames); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(onames) != 1 {
		t.Fatalf("Expected only 1 consumer but got %d", len(onames))
	}
	// Now let's get info about our consumer.
	resp, err = nc.Request(fmt.Sprintf(server.JetStreamConsumerInfoT, msetCfg.Name, onames[0]), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var oinfo server.ConsumerInfo
	if err = json.Unmarshal(resp.Data, &oinfo); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Do some sanity checking.
	// Must match consumer.go
	const randConsumerNameLen = 6

	if len(oinfo.Name) != randConsumerNameLen {
		t.Fatalf("Expected ephemeral name, got %q", oinfo.Name)
	}
	if len(oinfo.Config.Durable) != 0 {
		t.Fatalf("Expected no durable name, but got %q", oinfo.Config.Durable)
	}
	if oinfo.Config.Delivery != delivery {
		t.Fatalf("Expected to have delivery subject of %q, got %q", delivery, oinfo.Config.Delivery)
	}
	if oinfo.State.Delivered.ConsumerSeq != 10 {
		t.Fatalf("Expected consumer delivered sequence of 10, got %d", oinfo.State.Delivered.ConsumerSeq)
	}
	if oinfo.State.AckFloor.ConsumerSeq != 10 {
		t.Fatalf("Expected ack floor to be 10, got %d", oinfo.State.AckFloor.ConsumerSeq)
	}

	// Now delete the consumer.
	resp, _ = nc.Request(fmt.Sprintf(server.JetStreamDeleteConsumerT, msetCfg.Name, onames[0]), nil, time.Second)
	expectOKResponse(t, resp)

	// Now delete a msg.
	resp, _ = nc.Request(fmt.Sprintf(server.JetStreamDeleteMsgT, msetCfg.Name), []byte("2"), time.Second)
	expectOKResponse(t, resp)

	// Now purge the stream.
	resp, _ = nc.Request(fmt.Sprintf(server.JetStreamPurgeStreamT, msetCfg.Name), nil, time.Second)
	expectOKResponse(t, resp)

	// Now delete the stream.
	resp, _ = nc.Request(fmt.Sprintf(server.JetStreamDeleteStreamT, msetCfg.Name), nil, time.Second)
	expectOKResponse(t, resp)

	// Now grab stats again.
	// This will get the current information about usage and limits for this account.
	resp, err = nc.Request(server.JetStreamInfo, nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Streams != 0 {
		t.Fatalf("Expected no remaining streams, got %d", info.Streams)
	}
}

func TestJetStreamDeleteMsg(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.FileStorage,
				Replicas:  1,
			}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			config := s.JetStreamConfig()
			if config == nil {
				t.Fatalf("Expected non-nil config")
			}
			defer os.RemoveAll(config.StoreDir)

			cfg := &server.StreamConfig{Name: "foo", Storage: server.FileStorage}
			mset, err := s.GlobalAccount().AddStream(cfg)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			pubTen := func() {
				t.Helper()
				for i := 0; i < 10; i++ {
					nc.Publish("foo", []byte("Hello World!"))
				}
				nc.Flush()
			}

			pubTen()

			state := mset.State()
			if state.Msgs != 10 {
				t.Fatalf("Expected 10 messages, got %d", state.Msgs)
			}
			bytesPerMsg := state.Bytes / 10
			if bytesPerMsg == 0 {
				t.Fatalf("Expected non-zero bytes for msg size")
			}

			deleteAndCheck := func(seq, expectedFirstSeq uint64) {
				t.Helper()
				beforeState := mset.State()
				if !mset.DeleteMsg(seq) {
					t.Fatalf("Expected the delete of sequence %d to succeed", seq)
				}
				expectedState := beforeState
				expectedState.Msgs--
				expectedState.Bytes -= bytesPerMsg
				expectedState.FirstSeq = expectedFirstSeq
				afterState := mset.State()
				if afterState != expectedState {
					t.Fatalf("Stats not what we expected. Expected %+v, got %+v\n", expectedState, afterState)
				}
			}

			// Delete one from the middle
			deleteAndCheck(5, 1)
			// Now make sure sequences are update properly.
			// Delete first msg.
			deleteAndCheck(1, 2)
			// Now last
			deleteAndCheck(10, 2)
			// Now gaps.
			deleteAndCheck(3, 2)
			deleteAndCheck(2, 4)

			mset.Purge()
			// Put ten more one.
			pubTen()
			deleteAndCheck(11, 12)
			deleteAndCheck(15, 12)
			deleteAndCheck(16, 12)
			deleteAndCheck(20, 12)

			// Shutdown the server.
			s.Shutdown()

			s = RunBasicJetStreamServer()
			defer s.Shutdown()

			mset, err = s.GlobalAccount().LookupStream("foo")
			if err != nil {
				t.Fatalf("Expected to get the stream back")
			}

			expected := server.StreamState{Msgs: 6, Bytes: 6 * bytesPerMsg, FirstSeq: 12, LastSeq: 20}
			state = mset.State()
			if state != expected {
				t.Fatalf("State not what we expected. Expected %+v, got %+v\n", expected, state)
			}

			// Now create an consumer and make sure we get the right sequence.
			nc = clientConnectToServer(t, s)
			defer nc.Close()

			delivery := nats.NewInbox()
			sub, _ := nc.SubscribeSync(delivery)
			nc.Flush()

			o, err := mset.AddConsumer(&server.ConsumerConfig{Delivery: delivery, DeliverAll: true, FilterSubject: "foo"})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			expectedStoreSeq := []uint64{12, 13, 14, 17, 18, 19}

			for i := 0; i < 6; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if o.StreamSeqFromReply(m.Reply) != expectedStoreSeq[i] {
					t.Fatalf("Expected store seq of %d, got %d", expectedStoreSeq[i], o.StreamSeqFromReply(m.Reply))
				}
			}
		})
	}
}

func TestJetStreamNextMsgNoInterest(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.FileStorage,
				Replicas:  1,
			}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			config := s.JetStreamConfig()
			if config == nil {
				t.Fatalf("Expected non-nil config")
			}
			defer os.RemoveAll(config.StoreDir)

			cfg := &server.StreamConfig{Name: "foo", Storage: server.FileStorage}
			mset, err := s.GlobalAccount().AddStream(cfg)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}

			nc := clientConnectWithOldRequest(t, s)
			defer nc.Close()

			// Now create an consumer and make sure it functions properly.
			o, err := mset.AddConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			nextSubj := o.RequestNextMsgSubject()

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
			ostate := o.Info().State
			if ostate.AckFloor.StreamSeq != 11 || len(ostate.Pending) > 0 {
				t.Fatalf("Inconsistent ack state: %+v", ostate)
			}
		})
	}
}

func TestJetStreamTemplateBasics(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	config := s.JetStreamConfig()
	if config == nil {
		t.Fatalf("Expected non-nil config")
	}
	defer os.RemoveAll(config.StoreDir)

	acc := s.GlobalAccount()

	mcfg := &server.StreamConfig{
		Subjects:  []string{"kv.*"},
		Retention: server.LimitsPolicy,
		MaxAge:    time.Hour,
		MaxMsgs:   4,
		Storage:   server.MemoryStorage,
		Replicas:  1,
	}
	template := &server.StreamTemplateConfig{
		Name:       "kv",
		Config:     mcfg,
		MaxStreams: 4,
	}

	if _, err := acc.AddStreamTemplate(template); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if templates := acc.Templates(); len(templates) != 1 {
		t.Fatalf("Expected to get array of 1 template, got %d", len(templates))
	}
	if err := acc.DeleteStreamTemplate("foo"); err == nil {
		t.Fatalf("Expected an error for non-existent template")
	}
	if err := acc.DeleteStreamTemplate(template.Name); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if templates := acc.Templates(); len(templates) != 0 {
		t.Fatalf("Expected to get array of no templates, got %d", len(templates))
	}
	// Add it back in and test basics
	if _, err := acc.AddStreamTemplate(template); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Connect a client and send a message which should trigger the stream creation.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sendStreamMsg(t, nc, "kv.22", "derek")
	sendStreamMsg(t, nc, "kv.33", "cat")
	sendStreamMsg(t, nc, "kv.44", "sam")
	sendStreamMsg(t, nc, "kv.55", "meg")

	if nms := acc.NumStreams(); nms != 4 {
		t.Fatalf("Expected 4 auto-created streams, got %d", nms)
	}

	// This one should fail due to max.
	if resp, err := nc.Request("kv.99", nil, 100*time.Millisecond); err == nil {
		t.Fatalf("Expected this to fail, but got %q", resp.Data)
	}

	// Now delete template and make sure the underlying streams go away too.
	if err := acc.DeleteStreamTemplate(template.Name); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if nms := acc.NumStreams(); nms != 0 {
		t.Fatalf("Expected no auto-created streams to remain, got %d", nms)
	}
}

func TestJetStreamTemplateFileStoreRecovery(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	config := s.JetStreamConfig()
	if config == nil {
		t.Fatalf("Expected non-nil config")
	}
	defer os.RemoveAll(config.StoreDir)

	acc := s.GlobalAccount()

	mcfg := &server.StreamConfig{
		Subjects:  []string{"kv.*"},
		Retention: server.LimitsPolicy,
		MaxAge:    time.Hour,
		MaxMsgs:   50,
		Storage:   server.FileStorage,
		Replicas:  1,
	}
	template := &server.StreamTemplateConfig{
		Name:       "kv",
		Config:     mcfg,
		MaxStreams: 100,
	}

	if _, err := acc.AddStreamTemplate(template); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we can not add in a stream on our own with a template owner.
	badCfg := *mcfg
	badCfg.Name = "bad"
	badCfg.Template = "kv"
	if _, err := acc.AddStream(&badCfg); err == nil {
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

	if nms := acc.NumStreams(); nms != 100 {
		t.Fatalf("Expected 100 auto-created streams, got %d", nms)
	}

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())

	restartServer := func() {
		t.Helper()
		// Stop current server.
		s.Shutdown()
		// Restart.
		s = RunJetStreamServerOnPort(port)
	}

	// Restart.
	restartServer()
	defer s.Shutdown()

	acc = s.GlobalAccount()
	if nms := acc.NumStreams(); nms != 100 {
		t.Fatalf("Expected 100 auto-created streams, got %d", nms)
	}
	tmpl, err := acc.LookupStreamTemplate(template.Name)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Make sure t.Delete() survives restart.
	tmpl.Delete()

	// Restart.
	restartServer()
	defer s.Shutdown()

	acc = s.GlobalAccount()
	if nms := acc.NumStreams(); nms != 0 {
		t.Fatalf("Expected no auto-created streams, got %d", nms)
	}
	if _, err := acc.LookupStreamTemplate(template.Name); err == nil {
		t.Fatalf("Expected to not find the template after restart")
	}
}

// Benchmark placeholder
func TestJetStreamPubSubPerf(t *testing.T) {
	// Uncomment to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	config := s.JetStreamConfig()
	if config == nil {
		t.Fatalf("Expected non-nil config")
	}
	defer os.RemoveAll(config.StoreDir)

	acc := s.GlobalAccount()

	msetConfig := server.StreamConfig{
		Name:     "MSET22",
		Storage:  server.FileStorage,
		Subjects: []string{"foo"},
	}

	mset, err := acc.AddStream(&msetConfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	var toSend = 1000000
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

	_, err = mset.AddConsumer(&server.ConsumerConfig{
		Delivery:   delivery,
		DeliverAll: true,
		AckPolicy:  server.AckNone,
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
