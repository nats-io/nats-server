// Copyright 2014-2018 The NATS Authors
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
	"net"
	"strconv"
	"testing"

	"github.com/nats-io/gnatsd/server"
)

func TestResolveRandomPort(t *testing.T) {
	opts := &server.Options{Host: "127.0.0.1", Port: server.RANDOM_PORT, NoSigs: true}
	s := RunServer(opts)
	defer s.Shutdown()

	addr := s.Addr()
	_, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}

	portNum, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}

	if portNum == server.DEFAULT_PORT {
		t.Fatalf("Expected server to choose a random port\nGot: %d", server.DEFAULT_PORT)
	}

	if portNum == server.RANDOM_PORT {
		t.Fatalf("Expected server to choose a random port\nGot: %d", server.RANDOM_PORT)
	}

	if opts.Port != portNum {
		t.Fatalf("Options port (%d) should have been overridden by chosen random port (%d)",
			opts.Port, portNum)
	}
}
