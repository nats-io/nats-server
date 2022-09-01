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

package test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

var basicMASetupContents = []byte(`
	server_name: A
	listen: 127.0.0.1:-1

	accounts: {
	    A: {
	        users: [ {user: a, password: pwd} ]
	        exports: [{service: "foo", response: stream}]
	    },
	    B: {
	        users: [{user: b, password: pwd} ]
		    imports: [{ service: { account: A, subject: "foo"}, to: "foo_request" }]
	    }
	}
`)

func TestServiceImportWithStreamed(t *testing.T) {
	conf := createConfFile(t, basicMASetupContents)

	srv, opts := RunServerWithConfig(conf)
	defer srv.Shutdown()

	// Limit max response maps here for the test.
	accB, err := srv.LookupAccount("B")
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}

	// connect and offer a service
	nc, err := nats.Connect(fmt.Sprintf("nats://a:pwd@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	nc.Subscribe("foo", func(msg *nats.Msg) {
		if err := msg.Respond([]byte("world")); err != nil {
			t.Fatalf("Error on respond: %v", err)
		}
	})
	nc.Flush()

	nc2, err := nats.Connect(fmt.Sprintf("nats://b:pwd@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	numRequests := 10
	for i := 0; i < numRequests; i++ {
		resp, err := nc2.Request("foo_request", []byte("hello"), 2*time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if resp == nil || strings.Compare("world", string(resp.Data)) != 0 {
			t.Fatal("Did not receive the correct message")
		}
	}

	// Since we are using a new client that multiplexes, until the client itself goes away
	// we will have the full number of entries, even with the tighter ResponseEntriesPruneThreshold.
	accA, err := srv.LookupAccount("A")
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}

	// These should always be the same now.
	if nre := accB.NumPendingReverseResponses(); nre != numRequests {
		t.Fatalf("Expected %d entries, got %d", numRequests, nre)
	}
	if nre := accA.NumPendingAllResponses(); nre != numRequests {
		t.Fatalf("Expected %d entries, got %d", numRequests, nre)
	}

	// Now kill of the client that was doing the requests.
	nc2.Close()

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		aNrssi := accA.NumPendingAllResponses()
		bNre := accB.NumPendingReverseResponses()
		if aNrssi != 0 || bNre != 0 {
			return fmt.Errorf("Response imports and response entries should all be 0, got %d %d", aNrssi, bNre)
		}
		return nil
	})

	// Now let's test old style request and reply that uses a new inbox each time. This should work ok..
	nc2, err = nats.Connect(fmt.Sprintf("nats://b:pwd@%s:%d", opts.Host, opts.Port), nats.UseOldRequestStyle())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	for i := 0; i < numRequests; i++ {
		resp, err := nc2.Request("foo_request", []byte("hello"), 2*time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if resp == nil || strings.Compare("world", string(resp.Data)) != 0 {
			t.Fatal("Did not receive the correct message")
		}
	}

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		aNrssi := accA.NumPendingAllResponses()
		bNre := accB.NumPendingReverseResponses()
		if aNrssi != 0 || bNre != 0 {
			return fmt.Errorf("Response imports and response entries should all be 0, got %d %d", aNrssi, bNre)
		}
		return nil
	})
}

func TestServiceImportWithStreamedResponseAndEOF(t *testing.T) {
	conf := createConfFile(t, basicMASetupContents)

	srv, opts := RunServerWithConfig(conf)
	defer srv.Shutdown()

	accA, err := srv.LookupAccount("A")
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}
	accB, err := srv.LookupAccount("B")
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}

	nc, err := nats.Connect(fmt.Sprintf("nats://a:pwd@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// We will send four responses and then and nil message signaling EOF
	nc.Subscribe("foo", func(msg *nats.Msg) {
		// Streamed response.
		msg.Respond([]byte("world-1"))
		msg.Respond([]byte("world-2"))
		msg.Respond([]byte("world-3"))
		msg.Respond([]byte("world-4"))
		msg.Respond(nil)
	})
	nc.Flush()

	// Now setup requester.
	nc2, err := nats.Connect(fmt.Sprintf("nats://b:pwd@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	numRequests := 10
	expectedResponses := 5

	for i := 0; i < numRequests; i++ {
		// Create an inbox
		reply := nats.NewInbox()
		sub, _ := nc2.SubscribeSync(reply)
		defer sub.Unsubscribe()

		if err := nc2.PublishRequest("foo_request", reply, []byte("XOXO")); err != nil {
			t.Fatalf("Error sending request: %v", err)
		}

		// Wait and make sure we get all the responses. Should be five.
		checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != expectedResponses {
				return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, expectedResponses)
			}
			return nil
		})
	}

	if nre := accA.NumPendingAllResponses(); nre != 0 {
		t.Fatalf("Expected no entries, got %d", nre)
	}
	if nre := accB.NumPendingReverseResponses(); nre != 0 {
		t.Fatalf("Expected no entries, got %d", nre)
	}
}

func TestServiceExportsResponseFiltering(t *testing.T) {
	conf := createConfFile(t, []byte(`
		server_name: A
		listen: 127.0.0.1:-1

		accounts: {
		    A: {
		        users: [ {user: a, password: pwd} ]
		        exports: [ {service: "foo"}, {service: "bar"} ]
		    },
		    B: {
		        users: [{user: b, password: pwd} ]
			    imports: [ {service: { account: A, subject: "foo"}}, {service: { account: A, subject: "bar"}, to: "baz"} ]
		    }
		}
	`))

	srv, opts := RunServerWithConfig(conf)
	defer srv.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://a:pwd@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// If we do not subscribe the system is now smart enough to not setup the response service imports.
	nc.SubscribeSync("foo")
	nc.SubscribeSync("bar")
	nc.Flush()

	nc2, err := nats.Connect(fmt.Sprintf("nats://b:pwd@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	// We don't expect responses, so just do publishes.
	// 5 for foo
	sendFoo := 5
	for i := 0; i < sendFoo; i++ {
		nc2.PublishRequest("foo", "reply", nil)
	}
	// 17 for bar
	sendBar := 17
	for i := 0; i < sendBar; i++ {
		nc2.PublishRequest("baz", "reply", nil)
	}
	nc2.Flush()

	accA, err := srv.LookupAccount("A")
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}

	sendTotal := sendFoo + sendBar
	if nre := accA.NumPendingAllResponses(); nre != sendTotal {
		t.Fatalf("Expected %d entries, got %d", sendTotal, nre)
	}

	if nre := accA.NumPendingResponses("foo"); nre != sendFoo {
		t.Fatalf("Expected %d entries, got %d", sendFoo, nre)
	}

	if nre := accA.NumPendingResponses("bar"); nre != sendBar {
		t.Fatalf("Expected %d entries, got %d", sendBar, nre)
	}
}

func TestServiceExportsAutoDirectCleanup(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts: {
		    A: {
		        users: [ {user: a, password: pwd} ]
		        exports: [ {service: "foo"} ]
		    },
		    B: {
		        users: [{user: b, password: pwd} ]
			    imports: [ {service: { account: A, subject: "foo"}} ]
		    }
		}
	`))

	srv, opts := RunServerWithConfig(conf)
	defer srv.Shutdown()

	acc, err := srv.LookupAccount("A")
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}

	// Potential resonder.
	nc, err := nats.Connect(fmt.Sprintf("nats://a:pwd@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Requestor
	nc2, err := nats.Connect(fmt.Sprintf("nats://b:pwd@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	expectNone := func() {
		t.Helper()
		if nre := acc.NumPendingAllResponses(); nre != 0 {
			t.Fatalf("Expected no entries, got %d", nre)
		}
	}

	toSend := 10

	// With no responders we should never register service import responses etc.
	for i := 0; i < toSend; i++ {
		nc2.PublishRequest("foo", "reply", nil)
	}
	nc2.Flush()
	expectNone()

	// Now register a responder.
	sub, _ := nc.Subscribe("foo", func(msg *nats.Msg) {
		msg.Respond([]byte("world"))
	})
	nc.Flush()
	defer sub.Unsubscribe()

	// With no reply subject on a request we should never register service import responses etc.
	for i := 0; i < toSend; i++ {
		nc2.Publish("foo", nil)
	}
	nc2.Flush()
	expectNone()

	// Create an old request style client.
	nc3, err := nats.Connect(fmt.Sprintf("nats://b:pwd@%s:%d", opts.Host, opts.Port), nats.UseOldRequestStyle())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc3.Close()

	// If the request loses interest before the response we should not queue up service import responses either.
	// This only works for old style requests at the moment where we can detect interest going away.
	delay := 25 * time.Millisecond
	sub.Unsubscribe()
	sub, _ = nc.Subscribe("foo", func(msg *nats.Msg) {
		time.Sleep(delay)
		msg.Respond([]byte("world"))
	})
	nc.Flush()
	defer sub.Unsubscribe()

	for i := 0; i < toSend; i++ {
		nc3.Request("foo", nil, time.Millisecond)
	}
	nc3.Flush()
	time.Sleep(time.Duration(toSend) * delay * 2)
	expectNone()
}

// In some instances we do not have a forceful trigger that signals us to clean up.
// Like a stream that does not send EOF or a responder who receives requests but does
// not answer. For these we will have an expectation of a response threshold which
// tells the system we should have seen a response by T, say 2 minutes, 30 seconds etc.
func TestServiceExportsPruningCleanup(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts: {
		    A: {
		        users: [ {user: a, password: pwd} ]
		        exports: [ {service: "foo", response: stream} ]
		    },
		    B: {
		        users: [{user: b, password: pwd} ]
			    imports: [ {service: { account: A, subject: "foo"}} ]
		    }
		}
	`))

	srv, opts := RunServerWithConfig(conf)
	defer srv.Shutdown()

	// Potential resonder.
	nc, err := nats.Connect(fmt.Sprintf("nats://a:pwd@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// We will subscribe but not answer.
	sub, _ := nc.Subscribe("foo", func(msg *nats.Msg) {})
	nc.Flush()
	defer sub.Unsubscribe()

	acc, err := srv.LookupAccount("A")
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}

	// Check on response thresholds.
	rt, err := acc.ServiceExportResponseThreshold("foo")
	if err != nil {
		t.Fatalf("Error retrieving response threshold, %v", err)
	}

	if rt != server.DEFAULT_SERVICE_EXPORT_RESPONSE_THRESHOLD {
		t.Fatalf("Expected the response threshold to be %v, got %v",
			server.DEFAULT_SERVICE_EXPORT_RESPONSE_THRESHOLD, rt)
	}
	// now set it
	newRt := 10 * time.Millisecond
	if err := acc.SetServiceExportResponseThreshold("foo", newRt); err != nil {
		t.Fatalf("Expected no error setting response threshold, got %v", err)
	}

	expectedPending := func(expected int) {
		t.Helper()
		// Caller is sleeping a bit before, but avoid flappers.
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			if nre := acc.NumPendingResponses("foo"); nre != expected {
				return fmt.Errorf("Expected %d entries, got %d", expected, nre)
			}
			return nil
		})
	}

	// Requestor
	nc2, err := nats.Connect(fmt.Sprintf("nats://b:pwd@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	toSend := 10

	// This should register and they will dangle. Make sure we clean them up.
	for i := 0; i < toSend; i++ {
		nc2.PublishRequest("foo", "reply", nil)
	}
	nc2.Flush()

	expectedPending(10)
	time.Sleep(4 * newRt)
	expectedPending(0)

	// Do it again.
	for i := 0; i < toSend; i++ {
		nc2.PublishRequest("foo", "reply", nil)
	}
	nc2.Flush()

	expectedPending(10)
	time.Sleep(4 * newRt)
	expectedPending(0)
}

func TestServiceExportsResponseThreshold(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts: {
		    A: {
		        users: [ {user: a, password: pwd} ]
		        exports: [ {service: "foo", response: stream, threshold: "1s"} ]
		    },
		}
	`))

	srv, opts := RunServerWithConfig(conf)
	defer srv.Shutdown()

	// Potential responder.
	nc, err := nats.Connect(fmt.Sprintf("nats://a:pwd@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// We will subscribe but not answer.
	sub, _ := nc.Subscribe("foo", func(msg *nats.Msg) {})
	nc.Flush()
	defer sub.Unsubscribe()

	acc, err := srv.LookupAccount("A")
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}

	// Check on response thresholds.
	rt, err := acc.ServiceExportResponseThreshold("foo")
	if err != nil {
		t.Fatalf("Error retrieving response threshold, %v", err)
	}
	if rt != 1*time.Second {
		t.Fatalf("Expected response threshold to be %v, got %v", 1*time.Second, rt)
	}
}

func TestServiceExportsResponseThresholdChunked(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts: {
		    A: {
		        users: [ {user: a, password: pwd} ]
		        exports: [ {service: "foo", response: chunked, threshold: "10ms"} ]
		    },
		    B: {
		        users: [{user: b, password: pwd} ]
			    imports: [ {service: { account: A, subject: "foo"}} ]
		    }
		}
	`))

	srv, opts := RunServerWithConfig(conf)
	defer srv.Shutdown()

	// Responder.
	nc, err := nats.Connect(fmt.Sprintf("nats://a:pwd@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	numChunks := 10

	// Respond with 5ms gaps for total response time for all chunks and EOF > 50ms.
	nc.Subscribe("foo", func(msg *nats.Msg) {
		// Streamed response.
		for i := 1; i <= numChunks; i++ {
			time.Sleep(5 * time.Millisecond)
			msg.Respond([]byte(fmt.Sprintf("chunk-%d", i)))
		}
		msg.Respond(nil)
	})
	nc.Flush()

	// Now setup requester.
	nc2, err := nats.Connect(fmt.Sprintf("nats://b:pwd@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	// Create an inbox
	reply := nats.NewInbox()
	sub, _ := nc2.SubscribeSync(reply)
	defer sub.Unsubscribe()

	if err := nc2.PublishRequest("foo", reply, nil); err != nil {
		t.Fatalf("Error sending request: %v", err)
	}

	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != numChunks+1 {
			return fmt.Errorf("Did not receive correct number of chunks: %d vs %d", nmsgs, numChunks+1)
		}
		return nil
	})
}

func TestServiceAllowResponsesPerms(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts: {
		    A: {
		        users: [ {user: a, password: pwd, permissions = {subscribe=foo, allow_responses=true}} ]
		        exports: [ {service: "foo"} ]
		    },
		    B: {
		        users: [{user: b, password: pwd} ]
			    imports: [ {service: { account: A, subject: "foo"}} ]
		    }
		}
	`))

	srv, opts := RunServerWithConfig(conf)
	defer srv.Shutdown()

	// Responder.
	nc, err := nats.Connect(fmt.Sprintf("nats://a:pwd@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	reply := []byte("Hello")
	// Respond with 5ms gaps for total response time for all chunks and EOF > 50ms.
	nc.Subscribe("foo", func(msg *nats.Msg) {
		msg.Respond(reply)
	})
	nc.Flush()

	// Now setup requester.
	nc2, err := nats.Connect(fmt.Sprintf("nats://b:pwd@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	resp, err := nc2.Request("foo", []byte("help"), time.Second)
	if err != nil {
		t.Fatalf("Error expecting response %v", err)
	}
	if !bytes.Equal(resp.Data, reply) {
		t.Fatalf("Did not get correct response, %q vs %q", resp.Data, reply)
	}
}
