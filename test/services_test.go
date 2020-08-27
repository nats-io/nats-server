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
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

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
	defer os.Remove(conf)

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
