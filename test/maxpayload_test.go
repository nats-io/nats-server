// Copyright 2015-2018 The NATS Authors
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

	"github.com/nats-io/go-nats"
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
	conn.Write([]byte(pub))
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
