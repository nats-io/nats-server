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

package test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/server/sysmem"
	"github.com/nats-io/nats.go"
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

func clientConnectToServer(t *testing.T, s *server.Server) *nats.Conn {
	nc, err := nats.Connect(s.ClientURL())
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
	acc, _ := s.LookupOrRegisterAccount("$FOO")
	if err := s.JetStreamEnableAccount(acc, server.JetStreamAccountLimitsNoLimits); err != nil {
		t.Fatalf("Did not expect error on enabling account: %v", err)
	}
	if na := s.JetStreamNumAccounts(); na != 2 {
		t.Fatalf("Expected 2 accounts, got %d", na)
	}
	if err := s.JetStreamDisableAccount(acc); err != nil {
		t.Fatalf("Did not expect error on disabling account: %v", err)
	}
	if na := s.JetStreamNumAccounts(); na != 1 {
		t.Fatalf("Expected 1 account, got %d", na)
	}
	// We should get error if disabling something not enabled.
	acc, _ = s.LookupOrRegisterAccount("$BAR")
	if err := s.JetStreamDisableAccount(acc); err == nil {
		t.Fatalf("Expected error on disabling account that was not enabled")
	}
	// Should get an error for trying to enable a non-registered account.
	acc = server.NewAccount("$BAZ")
	if err := s.JetStreamEnableAccount(acc, server.JetStreamAccountLimitsNoLimits); err == nil {
		t.Fatalf("Expected error on enabling account that was not registered")
	}
	if err := s.JetStreamDisableAccount(s.GlobalAccount()); err != nil {
		t.Fatalf("Did not expect error on disabling account: %v", err)
	}
	if na := s.JetStreamNumAccounts(); na != 0 {
		t.Fatalf("Expected no accounts, got %d", na)
	}
}

func TestJetStreamAddMsgSet(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	mconfig := &server.MsgSetConfig{
		Name:      "foo",
		Retention: server.StreamPolicy,
		MaxAge:    time.Hour,
		Storage:   server.MemoryStorage,
		Replicas:  1,
	}

	mset, err := s.JetStreamAddMsgSet(s.GlobalAccount(), mconfig)
	if err != nil {
		t.Fatalf("Unexpected error adding message set: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	nc.Publish("foo", []byte("Hello World!"))
	nc.Flush()

	stats := mset.Stats()
	if stats.Msgs != 1 {
		t.Fatalf("Expected 1 message, got %d", stats.Msgs)
	}
	if stats.Bytes == 0 {
		t.Fatalf("Expected non-zero bytes")
	}

	nc.Publish("foo", []byte("Hello World Again!"))
	nc.Flush()

	stats = mset.Stats()
	if stats.Msgs != 2 {
		t.Fatalf("Expected 2 messages, got %d", stats.Msgs)
	}

	if err := s.JetStreamDeleteMsgSet(mset); err != nil {
		t.Fatalf("Got an error deleting the message set: %v", err)
	}
}

func expectOKResponse(t *testing.T, m *nats.Msg) {
	t.Helper()
	if m == nil {
		t.Fatalf("No response, possible timeout?")
	}
	if string(m.Data) != server.JsOK {
		t.Fatalf("Expected a JetStreamPubAck, got %q", m.Data)
	}
}

func TestJetStreamBasicAckPublish(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	mset, err := s.JetStreamAddMsgSet(s.GlobalAccount(), &server.MsgSetConfig{Name: "foo.*"})
	if err != nil {
		t.Fatalf("Unexpected error adding message set: %v", err)
	}
	defer s.JetStreamDeleteMsgSet(mset)

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	for i := 0; i < 50; i++ {
		resp, _ := nc.Request("foo.bar", []byte("Hello World!"), 50*time.Millisecond)
		expectOKResponse(t, resp)
	}
	stats := mset.Stats()
	if stats.Msgs != 50 {
		t.Fatalf("Expected 50 messages, got %d", stats.Msgs)
	}
}

func TestJetStreamRequestEnabled(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	resp, _ := nc.Request(server.JsEnabled, nil, time.Second)
	expectOKResponse(t, resp)
}

func TestJetStreamCreateObservable(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	mset, err := s.JetStreamAddMsgSet(s.GlobalAccount(), &server.MsgSetConfig{Name: "foo", Subjects: []string{"foo", "bar"}})
	if err != nil {
		t.Fatalf("Unexpected error adding message set: %v", err)
	}
	defer s.JetStreamDeleteMsgSet(mset)

	// Check for basic errors.
	if _, err := mset.AddObservable(nil); err == nil {
		t.Fatalf("Expected an error for no config")
	}

	// Check for delivery subject  errors.
	// Empty delivery subject
	if _, err := mset.AddObservable(&server.ObservableConfig{Delivery: ""}); err == nil {
		t.Fatalf("Expected an error on empty delivery subject")
	}
	// No literal delivery subject allowed.
	if _, err := mset.AddObservable(&server.ObservableConfig{Delivery: "foo.*"}); err == nil {
		t.Fatalf("Expected an error on bad delivery subject")
	}
	// Check for cycles
	if _, err := mset.AddObservable(&server.ObservableConfig{Delivery: "foo"}); err == nil {
		t.Fatalf("Expected an error on delivery subject that forms a cycle")
	}
	if _, err := mset.AddObservable(&server.ObservableConfig{Delivery: "bar"}); err == nil {
		t.Fatalf("Expected an error on delivery subject that forms a cycle")
	}
	if _, err := mset.AddObservable(&server.ObservableConfig{Delivery: "*"}); err == nil {
		t.Fatalf("Expected an error on delivery subject that forms a cycle")
	}

	// StartPosition conflicts
	if _, err := mset.AddObservable(&server.ObservableConfig{
		Delivery:  "A",
		StartSeq:  1,
		StartTime: time.Now(),
	}); err == nil {
		t.Fatalf("Expected an error on start position conflicts")
	}
	if _, err := mset.AddObservable(&server.ObservableConfig{
		Delivery:   "A",
		StartTime:  time.Now(),
		DeliverAll: true,
	}); err == nil {
		t.Fatalf("Expected an error on start position conflicts")
	}
	if _, err := mset.AddObservable(&server.ObservableConfig{
		Delivery:    "A",
		DeliverAll:  true,
		DeliverLast: true,
	}); err == nil {
		t.Fatalf("Expected an error on start position conflicts")
	}

	// Non-Durables need to have subscription to delivery subject.
	delivery := nats.NewInbox()
	if _, err := mset.AddObservable(&server.ObservableConfig{Delivery: delivery}); err == nil {
		t.Fatalf("Expected an error on unsubscribed delivery subject")
	}

	// This should work..
	nc := clientConnectToServer(t, s)
	defer nc.Close()
	sub, _ := nc.SubscribeSync(delivery)
	defer sub.Unsubscribe()
	nc.Flush()

	o, err := mset.AddObservable(&server.ObservableConfig{Delivery: delivery})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}

	if err := mset.DeleteObservable(o); err != nil {
		t.Fatalf("Expected no error on delete, got %v", err)
	}
}

func TestJetStreamBasicDelivery(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	mset, err := s.JetStreamAddMsgSet(s.GlobalAccount(), &server.MsgSetConfig{Name: "MSET", Subjects: []string{"foo.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding message set: %v", err)
	}
	defer s.JetStreamDeleteMsgSet(mset)

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 100
	sendSubj := "foo.bar"
	for i := 0; i < toSend; i++ {
		resp, _ := nc.Request(sendSubj, []byte("Hello World!"), 50*time.Millisecond)
		expectOKResponse(t, resp)
	}
	stats := mset.Stats()
	if stats.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, stats.Msgs)
	}

	// Now create an observable. Use different connection.
	nc2 := clientConnectToServer(t, s)
	defer nc2.Close()

	delivery := nats.NewInbox()
	sub, _ := nc2.SubscribeSync(delivery)
	defer sub.Unsubscribe()
	nc2.Flush()

	o, err := mset.AddObservable(&server.ObservableConfig{Delivery: delivery, DeliverAll: true})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

	// Check for our messages.
	checkMsgs := func(seqOff int) {
		t.Helper()

		checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
				return fmt.Errorf("Did not receive correct number of  messages: %d vs %d", nmsgs, toSend)
			}
			return nil
		})

		// Now let's check the messages
		for i := 0; i < toSend; i++ {
			m, _ := sub.NextMsg(time.Millisecond)
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
	for i := 0; i < toSend; i++ {
		resp, _ := nc.Request(sendSubj, []byte("Hello World!"), 50*time.Millisecond)
		expectOKResponse(t, resp)
	}
	stats = mset.Stats()
	if stats.Msgs != uint64(toSend*2) {
		t.Fatalf("Expected %d messages, got %d", toSend*2, stats.Msgs)
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
	o, err = mset.AddObservable(&server.ObservableConfig{Delivery: delivery, DeliverLast: true})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 1 {
			return fmt.Errorf("Did not receive correct number of  messages: %d vs %d", nmsgs, toSend)
		}
		return nil
	})
	m, _ := sub.NextMsg(time.Millisecond)
	if seq := o.SeqFromReply(m.Reply); seq != 200 {
		t.Fatalf("Expected sequence to be 200, but got %d", seq)
	}

	checkSubEmpty()
	o.Delete()

	o, err = mset.AddObservable(&server.ObservableConfig{Delivery: delivery}) // Default is deliver new only.
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

	if m, err := sub.NextMsg(time.Millisecond); err == nil {
		t.Fatalf("Expected no msg, got %+v", m)
	}

	checkSubEmpty()
	o.Delete()

	// Now try by sequence number.
	o, err = mset.AddObservable(&server.ObservableConfig{Delivery: delivery, StartSeq: 101})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

	checkMsgs(101)
}
