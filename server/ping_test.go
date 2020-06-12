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

package server

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"
)

const PING_CLIENT_PORT = 11228

var DefaultPingOptions = Options{
	Host:         "127.0.0.1",
	Port:         PING_CLIENT_PORT,
	NoLog:        true,
	NoSigs:       true,
	PingInterval: 50 * time.Millisecond,
}

func TestPing(t *testing.T) {
	o := DefaultPingOptions
	o.DisableShortFirstPing = true
	s := RunServer(&o)
	defer s.Shutdown()

	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", PING_CLIENT_PORT))
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer c.Close()
	br := bufio.NewReader(c)
	// Wait for INFO
	br.ReadLine()
	// Send CONNECT
	c.Write([]byte("CONNECT {\"verbose\":false}\r\nPING\r\n"))
	// Wait for first PONG
	br.ReadLine()
	// Wait for PING
	start := time.Now()
	for i := 0; i < 3; i++ {
		l, _, err := br.ReadLine()
		if err != nil {
			t.Fatalf("Error: %v", err)
		}
		if string(l) != "PING" {
			t.Fatalf("Expected PING, got %q", l)
		}
		if dur := time.Since(start); dur < 25*time.Millisecond || dur > 75*time.Millisecond {
			t.Fatalf("Pings duration off: %v", dur)
		}
		c.Write([]byte(pongProto))
		start = time.Now()
	}
}
