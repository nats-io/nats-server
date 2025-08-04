// Copyright 2025 The NATS Authors
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
	"sync"
	"testing"
)

func dummyFuzzClient(kind int) *client {
	var r *route
	var gw *gateway
	var lf *leaf

	switch kind {
	case ROUTER:
		r = &route{}
	case GATEWAY:
		gw = &gateway{outbound: false, connected: true, insim: make(map[string]*insie), outsim: &sync.Map{}}
	case LEAF:
		lf = &leaf{}
	}

	return &client{
		srv:   New(&defaultServerOptions),
		kind:  kind,
		msubs: -1,
		in: readCache{
			results: make(map[string]*SublistResult),
			pacache: make(map[string]*perAccountCache),
		},
		mpay:  MAX_PAYLOAD_SIZE,
		mcl:   MAX_CONTROL_LINE_SIZE,
		route: r,
		gw:    gw,
		leaf:  lf,
	}
}

// FuzzParser performs fuzz testing on the NATS protocol parser implementation.
// It tests the parser's ability to handle various NATS protocol messages, including
// partial (chunked) message delivery scenarios that may occur in real-world usage.
func FuzzParser(f *testing.F) {
	msgs := []string{
		"PING\r\n",
		"PONG\r\n",
		"PUB foo 33333\r\n",
		"HPUB foo INBOX.22 0 5\r\nHELLO\r",
		"HMSG $foo foo 10 8\r\nXXXhello\r",
		"MSG $foo foo 5\r\nhello\r",
		"SUB foo 1\r\nSUB foo 2\r\n",
		"UNSUB 1 5\r\n",
		"RMSG $G foo.bar | baz 11\r\nhello world\r",
		"CONNECT {\"verbose\":false,\"pedantic\":true,\"tls_required\":false}\r\n",
	}

	clientKinds := []int{
		CLIENT,
		ROUTER,
		GATEWAY,
		LEAF,
	}

	for _, ck := range clientKinds {
		for _, crp := range msgs {
			f.Add(ck, crp)
		}
	}

	f.Fuzz(func(t *testing.T, kind int, orig string) {
		c := dummyFuzzClient(kind)

		data := []byte(orig)
		half := len(data) / 2

		if err := c.parse(data[:half]); err != nil {
			return
		}

		if err := c.parse(data[half:]); err != nil {
			return
		}
	})
}
