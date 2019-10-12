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
	"strconv"
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

	// No deliver subject, meaning its in pull mode, work queue mode means it is required to
	// do explicit ack.
	if _, err := mset.AddObservable(&server.ObservableConfig{}); err == nil {
		t.Fatalf("Expected an error on work queue / pull mode without explicit ack mode")
	}

	// Check for delivery subject errors.

	// Literal delivery subject required.
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

	sub, _ := nc2.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()
	nc2.Flush()

	o, err := mset.AddObservable(&server.ObservableConfig{Delivery: sub.Subject, DeliverAll: true})
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
	o, err = mset.AddObservable(&server.ObservableConfig{Delivery: sub.Subject, DeliverLast: true})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

	m, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Did not get expected message, got %v", err)
	}
	if seq := o.SeqFromReply(m.Reply); seq != 200 {
		t.Fatalf("Expected sequence to be 200, but got %d", seq)
	}

	checkSubEmpty()
	o.Delete()

	o, err = mset.AddObservable(&server.ObservableConfig{Delivery: sub.Subject}) // Default is deliver new only.
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
	o, err = mset.AddObservable(&server.ObservableConfig{Delivery: sub.Subject, StartSeq: 101})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

	checkMsgs(101)
}

func workerModeConfig(name string) *server.ObservableConfig {
	return &server.ObservableConfig{Durable: name, DeliverAll: true, AckPolicy: server.AckExplicit}
}

func TestJetStreamBasicWorkQueue(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	mname := "MY_MSG_SET"
	mset, err := s.JetStreamAddMsgSet(s.GlobalAccount(), &server.MsgSetConfig{Name: mname, Subjects: []string{"foo", "bar"}})
	if err != nil {
		t.Fatalf("Unexpected error adding message set: %v", err)
	}
	defer s.JetStreamDeleteMsgSet(mset)

	// Create basic work queue mode observable.
	oname := "WQ"
	o, err := mset.AddObservable(workerModeConfig(oname))
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
		resp, _ := nc.Request(sendSubj, []byte("Hello World!"), 50*time.Millisecond)
		expectOKResponse(t, resp)
	}
	stats := mset.Stats()
	if stats.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, stats.Msgs)
	}

	// For normal work queue semantics, you send requests to the subject with message set and observable name.
	reqMsgSubj := fmt.Sprintf("%s.%s.%s", server.JsReqPre, mname, oname)

	getNext := func(seqno int) {
		t.Helper()
		nextMsg, err := nc.Request(reqMsgSubj, nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
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
		nc.Request(sendSubj, []byte("Hello World!"), 50*time.Millisecond)
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
}

func TestJetStreamPartitioning(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	mset, err := s.JetStreamAddMsgSet(s.GlobalAccount(), &server.MsgSetConfig{Name: "MSET", Subjects: []string{"foo.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding message set: %v", err)
	}
	defer s.JetStreamDeleteMsgSet(mset)

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 50
	subjA := "foo.A"
	subjB := "foo.B"

	for i := 0; i < toSend; i++ {
		resp, _ := nc.Request(subjA, []byte("Hello World!"), 50*time.Millisecond)
		expectOKResponse(t, resp)
		resp, _ = nc.Request(subjB, []byte("Hello World!"), 50*time.Millisecond)
		expectOKResponse(t, resp)
	}
	stats := mset.Stats()
	if stats.Msgs != uint64(toSend*2) {
		t.Fatalf("Expected %d messages, got %d", toSend*2, stats.Msgs)
	}

	delivery := nats.NewInbox()
	sub, _ := nc.SubscribeSync(delivery)
	defer sub.Unsubscribe()
	nc.Flush()

	o, err := mset.AddObservable(&server.ObservableConfig{Delivery: delivery, Partition: subjB, DeliverAll: true})
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
}

func TestJetStreamWorkQueuePartitioning(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	mname := "MY_MSG_SET"
	mset, err := s.JetStreamAddMsgSet(s.GlobalAccount(), &server.MsgSetConfig{Name: mname, Subjects: []string{"foo.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding message set: %v", err)
	}
	defer s.JetStreamDeleteMsgSet(mset)

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 50
	subjA := "foo.A"
	subjB := "foo.B"

	for i := 0; i < toSend; i++ {
		resp, _ := nc.Request(subjA, []byte("Hello World!"), 50*time.Millisecond)
		expectOKResponse(t, resp)
		resp, _ = nc.Request(subjB, []byte("Hello World!"), 50*time.Millisecond)
		expectOKResponse(t, resp)
	}
	stats := mset.Stats()
	if stats.Msgs != uint64(toSend*2) {
		t.Fatalf("Expected %d messages, got %d", toSend*2, stats.Msgs)
	}

	oname := "WQ"
	o, err := mset.AddObservable(&server.ObservableConfig{Durable: oname, Partition: subjA, DeliverAll: true, AckPolicy: server.AckExplicit})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

	if o.NextSeq() != 1 {
		t.Fatalf("Expected to be starting at sequence 1")
	}

	// For normal work queue semantics, you send requests to the subject with message set and observable name.
	reqMsgSubj := fmt.Sprintf("%s.%s.%s", server.JsReqPre, mname, oname)

	getNext := func(seqno int) {
		t.Helper()
		nextMsg, err := nc.Request(reqMsgSubj, nil, time.Second)
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
}

func TestJetStreamWorkQueueAckAndNext(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	mname := "MY_MSG_SET"
	mset, err := s.JetStreamAddMsgSet(s.GlobalAccount(), &server.MsgSetConfig{Name: mname, Subjects: []string{"foo", "bar"}})
	if err != nil {
		t.Fatalf("Unexpected error adding message set: %v", err)
	}
	defer s.JetStreamDeleteMsgSet(mset)

	// Create basic work queue mode observable.
	oname := "WQ"
	o, err := mset.AddObservable(workerModeConfig(oname))
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
		resp, _ := nc.Request(sendSubj, []byte("Hello World!"), 50*time.Millisecond)
		expectOKResponse(t, resp)
	}
	stats := mset.Stats()
	if stats.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, stats.Msgs)
	}

	// For normal work queue semantics, you send requests to the subject with message set and observable name.
	// We will do this to start it off then use ack+next to get other messages.
	reqNextMsgSubj := fmt.Sprintf("%s.%s.%s", server.JsReqPre, mname, oname)

	sub, _ := nc.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()

	// Kick things off.
	nc.PublishRequest(reqNextMsgSubj, sub.Subject, nil)

	for i := 0; i < toSend; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error waiting for messages: %v", err)
		}
		nc.PublishRequest(m.Reply, sub.Subject, server.AckNext)
	}

}

func TestJetStreamWorkQueueRequestBatch(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	mname := "MY_MSG_SET"
	mset, err := s.JetStreamAddMsgSet(s.GlobalAccount(), &server.MsgSetConfig{Name: mname, Subjects: []string{"foo", "bar"}})
	if err != nil {
		t.Fatalf("Unexpected error adding message set: %v", err)
	}
	defer s.JetStreamDeleteMsgSet(mset)

	// Create basic work queue mode observable.
	oname := "WQ"
	o, err := mset.AddObservable(workerModeConfig(oname))
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
		resp, _ := nc.Request(sendSubj, []byte("Hello World!"), 50*time.Millisecond)
		expectOKResponse(t, resp)
	}
	stats := mset.Stats()
	if stats.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, stats.Msgs)
	}

	// For normal work queue semantics, you send requests to the subject with message set and observable name.
	// We will do this to start it off then use ack+next to get other messages.
	reqNextMsgSubj := fmt.Sprintf("%s.%s.%s", server.JsReqPre, mname, oname)

	sub, _ := nc.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()

	// Kick things off with batch size of 50.
	batchSize := 50
	nc.PublishRequest(reqNextMsgSubj, sub.Subject, []byte(strconv.Itoa(batchSize)))

	// We should receive batchSize with no acks or additional requests.
	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != batchSize {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, batchSize)
		}
		return nil
	})
}

func TestJetStreamBasicPushNak(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	mname := "MY_MSG_SET"
	mset, err := s.JetStreamAddMsgSet(s.GlobalAccount(), &server.MsgSetConfig{Name: mname, Subjects: []string{"foo", "bar"}})
	if err != nil {
		t.Fatalf("Unexpected error adding message set: %v", err)
	}
	defer s.JetStreamDeleteMsgSet(mset)

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Now load up some messages.
	toSend := 1000
	sendSubj := "bar"
	for i := 0; i < toSend; i++ {
		resp, _ := nc.Request(sendSubj, []byte("Hello World!"), 50*time.Millisecond)
		expectOKResponse(t, resp)
	}
	stats := mset.Stats()
	if stats.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, stats.Msgs)
	}

	// Now create a normal push based observable.
	sub, _ := nc.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()
	nc.Flush()

	startSeq := 22
	o, err := mset.AddObservable(&server.ObservableConfig{Delivery: sub.Subject, StartSeq: uint64(startSeq), AckPolicy: server.AckAll})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

	if o.NextSeq() != uint64(startSeq) {
		t.Fatalf("Expected to be starting at sequence %d, got %d", startSeq, o.NextSeq())
	}

	expected := toSend - startSeq + 1
	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != expected {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, expected)
		}
		return nil
	})

	// Which one to nak.
	nakSeq := 900

	// So we have all the messages. Whip through them all and nack one.
	// With push based, nak means restart the stream.
	for i := 0; i < expected; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		seq := o.SeqFromReply(m.Reply)
		if seq != uint64(i+startSeq) {
			t.Fatalf("Expected sequence of %d , got %d", (i + startSeq), seq)
		}
		if seq == uint64(nakSeq) {
			nc.Publish(m.Reply, server.AckNak)
		}
	}

	// We should expect since we drained the sub above to have the replayed messages again from the nak sequence.
	expected = toSend - nakSeq + 1
	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != expected {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, expected)
		}
		return nil
	})

	for i := 0; i < expected; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		seq := o.SeqFromReply(m.Reply)
		if seq != uint64(i+nakSeq) {
			t.Fatalf("Expected sequence of %d , got %d", (i + nakSeq), seq)
		}
	}
}

func TestJetStreamWorkQueueMsgSet(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	mname := "MY_WORK_QUEUE.*"
	mset, err := s.JetStreamAddMsgSet(s.GlobalAccount(), &server.MsgSetConfig{Name: mname, Retention: server.WorkQueuePolicy})
	if err != nil {
		t.Fatalf("Unexpected error adding message set: %v", err)
	}
	defer s.JetStreamDeleteMsgSet(mset)

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sub, _ := nc.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()
	nc.Flush()

	// This type of message set has restrictions which we will test here.

	// Push based not allowed.
	if _, err := mset.AddObservable(&server.ObservableConfig{Delivery: sub.Subject}); err == nil {
		t.Fatalf("Expected an error on delivery subject")
	}

	// DeliverAll is only start mode allowed.
	if _, err := mset.AddObservable(&server.ObservableConfig{DeliverLast: true}); err == nil {
		t.Fatalf("Expected an error with anything but DeliverAll")
	}

	// We will create a non-partitioned observable. This should succeed.
	o, err := mset.AddObservable(&server.ObservableConfig{DeliverAll: true, AckPolicy: server.AckExplicit})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o.Delete()

	// Now if we create another this should fail, only can have one non-partitioned.
	if _, err := mset.AddObservable(&server.ObservableConfig{DeliverAll: true}); err == nil {
		t.Fatalf("Expected an error on attempt for second observable for a workqueue")
	}

	o.Delete()

	if numo := mset.NumObservables(); numo != 0 {
		t.Fatalf("Expected to have zero observables, got %d", numo)
	}

	// Now add in an observable that has a partition.

	pConfig := func(pname string) *server.ObservableConfig {
		return &server.ObservableConfig{DeliverAll: true, Partition: pname, AckPolicy: server.AckExplicit}
	}
	o, err = mset.AddObservable(pConfig("MY_WORK_QUEUE.A"))
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o.Delete()

	// Now creating another with separate partition should work.
	o2, err := mset.AddObservable(pConfig("MY_WORK_QUEUE.B"))
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o2.Delete()

	// Anything that would overlap should fail though.
	if _, err := mset.AddObservable(pConfig(">")); err == nil {
		t.Fatalf("Expected an error on attempt for partitioned observable for a workqueue")
	}
	if _, err := mset.AddObservable(pConfig("MY_WORK_QUEUE.A")); err == nil {
		t.Fatalf("Expected an error on attempt for partitioned observable for a workqueue")
	}
	if _, err := mset.AddObservable(pConfig("MY_WORK_QUEUE.A")); err == nil {
		t.Fatalf("Expected an error on attempt for partitioned observable for a workqueue")
	}

	o3, err := mset.AddObservable(pConfig("MY_WORK_QUEUE.C"))
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o3.Delete()
}

func TestJetStreamWorkQueueAckWaitRedelivery(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	mname := "MY_WQ"
	mset, err := s.JetStreamAddMsgSet(s.GlobalAccount(), &server.MsgSetConfig{Name: mname, Retention: server.WorkQueuePolicy})
	if err != nil {
		t.Fatalf("Unexpected error adding message set: %v", err)
	}
	defer s.JetStreamDeleteMsgSet(mset)

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Now load up some messages.
	toSend := 100
	for i := 0; i < toSend; i++ {
		resp, _ := nc.Request(mname, []byte("Hello World!"), 50*time.Millisecond)
		expectOKResponse(t, resp)
	}
	stats := mset.Stats()
	if stats.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, stats.Msgs)
	}

	ackWait := 50 * time.Millisecond

	o, err := mset.AddObservable(&server.ObservableConfig{DeliverAll: true, AckPolicy: server.AckExplicit, AckWait: ackWait})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o.Delete()

	sub, _ := nc.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()

	reqNextMsgSubj := fmt.Sprintf("%s.%s.%s", server.JsReqPre, mname, o.Name())

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
	stats = mset.Stats()
	if int(stats.Msgs) != toSend {
		t.Fatalf("Expected %d messages, got %d", toSend, stats.Msgs)
	}

	// Now consume and ack.
	for i := 1; i <= toSend; i++ {
		nc.PublishRequest(reqNextMsgSubj, sub.Subject, nil)
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error waiting for messages: %v", err)
		}
		if seq := o.SeqFromReply(m.Reply); seq != uint64(i) {
			t.Fatalf("Expected sequence of %d , got %d", i, seq)
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
	if stats := mset.Stats(); stats.Msgs != 0 {
		t.Fatalf("Expected no messages, got %d", stats.Msgs)
	}
}

func TestJetStreamWorkQueueNakRedelivery(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	mname := "MY_WQ"
	mset, err := s.JetStreamAddMsgSet(s.GlobalAccount(), &server.MsgSetConfig{Name: mname, Retention: server.WorkQueuePolicy})
	if err != nil {
		t.Fatalf("Unexpected error adding message set: %v", err)
	}
	defer s.JetStreamDeleteMsgSet(mset)

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Now load up some messages.
	toSend := 10
	for i := 0; i < toSend; i++ {
		resp, _ := nc.Request(mname, []byte("Hello World!"), 50*time.Millisecond)
		expectOKResponse(t, resp)
	}
	stats := mset.Stats()
	if stats.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, stats.Msgs)
	}

	o, err := mset.AddObservable(&server.ObservableConfig{DeliverAll: true, AckPolicy: server.AckExplicit})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o.Delete()

	reqNextMsgSubj := fmt.Sprintf("%s.%s.%s", server.JsReqPre, mname, o.Name())

	getMsg := func(seqno int) *nats.Msg {
		t.Helper()
		m, err := nc.Request(reqNextMsgSubj, nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if seq := o.SeqFromReply(m.Reply); seq != uint64(seqno) {
			t.Fatalf("Expected sequence of %d , got %d", seqno, seq)
		}
		return m
	}

	for i := 1; i <= 5; i++ {
		m := getMsg(i)
		// Ack the message here.
		m.Respond(nil)
	}

	// Grab #6
	m := getMsg(6)
	// NAK this one.
	m.Respond(server.AckNak)

	// When we request again should be 6 again.
	getMsg(6)
	// Then we should get 7, 8, etc.
	getMsg(7)
	getMsg(8)
}

func TestJetStreamWorkQueueWorkingIndicator(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	mname := "MY_WQ"
	mset, err := s.JetStreamAddMsgSet(s.GlobalAccount(), &server.MsgSetConfig{Name: mname, Retention: server.WorkQueuePolicy})
	if err != nil {
		t.Fatalf("Unexpected error adding message set: %v", err)
	}
	defer s.JetStreamDeleteMsgSet(mset)

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Now load up some messages.
	toSend := 2
	for i := 0; i < toSend; i++ {
		resp, _ := nc.Request(mname, []byte("Hello World!"), 50*time.Millisecond)
		expectOKResponse(t, resp)
	}
	stats := mset.Stats()
	if stats.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, stats.Msgs)
	}

	ackWait := 50 * time.Millisecond

	o, err := mset.AddObservable(&server.ObservableConfig{DeliverAll: true, AckPolicy: server.AckExplicit, AckWait: ackWait})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o.Delete()

	reqNextMsgSubj := fmt.Sprintf("%s.%s.%s", server.JsReqPre, mname, o.Name())

	getMsg := func(seqno int) *nats.Msg {
		t.Helper()
		m, err := nc.Request(reqNextMsgSubj, nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if seq := o.SeqFromReply(m.Reply); seq != uint64(seqno) {
			t.Fatalf("Expected sequence of %d , got %d", seqno, seq)
		}
		return m
	}

	getMsg(1)
	// Now wait past ackWait
	time.Sleep(ackWait * 5)

	// We should get 1 back.
	m := getMsg(1)
	m.Respond(server.AckWork)

	// Now let's take longer than ackWait to process but signal we are working on the message.
	timeout := time.Now().Add(5 * ackWait)
	for time.Now().Before(timeout) {
		time.Sleep(ackWait / 4)
		m.Respond(server.AckWork)
	}

	// We should get 2 here, not 1 since we have indicated we are working on it.
	m2 := getMsg(2)
	time.Sleep(ackWait / 2)
	m2.Respond(server.AckWork)

	// Now should get 1 back then 2.
	m = getMsg(1)
	m.Respond(nil)

	getMsg(2)
}
