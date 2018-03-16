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

import "testing"

func TestServerConfig(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/override.conf")
	defer srv.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	sinfo := checkInfoMsg(t, c)
	if sinfo.MaxPayload != opts.MaxPayload {
		t.Fatalf("Expected max_payload from server, got %d vs %d",
			opts.MaxPayload, sinfo.MaxPayload)
	}
}

func TestTLSConfig(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/tls.conf")
	defer srv.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	sinfo := checkInfoMsg(t, c)
	if !sinfo.TLSRequired {
		t.Fatal("Expected TLSRequired to be true when configured")
	}
}
