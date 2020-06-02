// Copyright 2015-2019 The NATS Authors
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
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func TestMaxPayload(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/override.conf")
	defer srv.Shutdown()

	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(fmt.Sprintf("nats://%s/", endpoint))
	if err != nil {
		t.Fatalf("Could not connect to server: %v", err)
	}
	defer nc.Close()

	size := 4 * 1024 * 1024
	big := sizedBytes(size)
	err = nc.Publish("foo", big)

	if err != nats.ErrMaxPayload {
		t.Fatalf("Expected a Max Payload error")
	}

	conn, err := net.DialTimeout("tcp", endpoint, nc.Opts.Timeout)
	if err != nil {
		t.Fatalf("Could not make a raw connection to the server: %v", err)
	}
	defer conn.Close()
	info := make([]byte, 512)
	_, err = conn.Read(info)
	if err != nil {
		t.Fatalf("Expected an info message to be sent by the server: %s", err)
	}
	pub := fmt.Sprintf("PUB bar %d\r\n", size)
	_, err = conn.Write([]byte(pub))
	if err != nil {
		t.Fatalf("Could not publish event to the server: %s", err)
	}

	errMsg := make([]byte, 35)
	_, err = conn.Read(errMsg)
	if err != nil {
		t.Fatalf("Expected an error message to be sent by the server: %s", err)
	}

	if !strings.Contains(string(errMsg), "Maximum Payload Violation") {
		t.Errorf("Received wrong error message (%v)\n", string(errMsg))
	}

	// Client proactively omits sending the message so server
	// does not close the connection.
	if nc.IsClosed() {
		t.Errorf("Expected connection to not be closed.")
	}

	// On the other hand client which did not proactively omitted
	// publishing the bytes following what is suggested by server
	// in the info message has its connection closed.
	_, err = conn.Write(big)
	if err == nil && runtime.GOOS != "windows" {
		t.Errorf("Expected error due to maximum payload transgression.")
	}

	// On windows, the previous write will not fail because the connection
	// is not fully closed at this stage.
	if runtime.GOOS == "windows" {
		// Issuing a PING and not expecting the PONG.
		_, err = conn.Write([]byte("PING\r\n"))
		if err == nil {
			conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			_, err = conn.Read(big)
			if err == nil {
				t.Errorf("Expected closed connection due to maximum payload transgression.")
			}
		}
	}
}

func TestMaxPayloadOverrun(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.MaxPayload = 10000
	s := RunServer(&opts)
	defer s.Shutdown()

	// Overrun a int32
	c := createClientConn(t, "127.0.0.1", opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PUB foo 199380988\r\n")
	expect(errRe)

	// Now overrun an int64, parseSize will have returned -1,
	// so we get disconnected.
	c = createClientConn(t, "127.0.0.1", opts.Port)
	defer c.Close()

	send, _ = setupConn(t, c)
	send("PUB foo 18446744073709551615123\r\n")
	expectDisconnect(t, c)
}

func TestAsyncInfoWithSmallerMaxPayload(t *testing.T) {
	s, opts := runOperatorServer(t)
	defer s.Shutdown()

	const testMaxPayload = 522

	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	nac.Limits.Payload = testMaxPayload
	ajwt, err := nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	if err := s.AccountResolver().Store(apub, ajwt); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	nc.Flush()
	defer nc.Close()

	if mp := nc.MaxPayload(); mp != testMaxPayload {
		t.Fatalf("Expected MaxPayload of %d, got %d", testMaxPayload, mp)
	}
}
