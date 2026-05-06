// Copyright 2026 The NATS Authors
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
	"bytes"
	"runtime/debug"

	"github.com/antithesishq/antithesis-sdk-go/assert"
)

// Splice canary: detect stored stream payloads that contain bytes from
// an in-flight NATS protocol frame. Triggered by the production sample
// where an HMSG line + NATS/1.0 headers + a $JS.ACK reply subject
// appeared inside a stored stream message body.
//
// We log a warning and a stack trace at the first hit per filestore so
// that the next occurrence in a production debug build captures full
// context (subject, sequence, surrounding bytes) without spamming the
// logs if the corruption recurs across many records.

var (
	hmsgMarker = []byte("\nHMSG ")
	rmsgMarker = []byte("\nRMSG ")
	ackMarker  = []byte("$JS.ACK.")
)

func (fs *fileStore) spliceCanaryStore(subj string, hdr, msg []byte, seq uint64) {
	hit := scanForSpliceMarkers(msg)
	if hit == "" {
		hit = scanForSpliceMarkers(hdr)
	}
	if hit == "" {
		return
	}
	fs.reportSpliceCanary("store", subj, seq, hdr, msg, hit)
}

func (fs *fileStore) spliceCanaryLoad(sm *StoreMsg) {
	hit := scanForSpliceMarkers(sm.msg)
	if hit == "" {
		hit = scanForSpliceMarkers(sm.hdr)
	}
	if hit == "" {
		return
	}
	fs.reportSpliceCanary("load", sm.subj, sm.seq, sm.hdr, sm.msg, hit)
}

func scanForSpliceMarkers(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if i := bytes.Index(b, hmsgMarker); i >= 0 {
		return "HMSG"
	}
	if i := bytes.Index(b, rmsgMarker); i >= 0 {
		return "RMSG"
	}
	if i := bytes.Index(b, ackMarker); i >= 0 {
		return "JS.ACK"
	}
	return ""
}

func (fs *fileStore) reportSpliceCanary(op, subj string, seq uint64, hdr, msg []byte, marker string) {
	if !fs.spliceCanaryLogged.CompareAndSwap(false, true) {
		fs.warn("splice canary[%s] subj=%s seq=%d marker=%s (recurring)", op, subj, seq, marker)
		return
	}
	stack := debug.Stack()
	fs.warn("splice canary[%s] subj=%s seq=%d marker=%s hdr=%d msg=%d head=%q",
		op, subj, seq, marker, len(hdr), len(msg), spliceSnippet(msg))
	fs.warn("splice canary stack:\n%s", stack)
	assert.Unreachable("Stored stream payload contains in-flight protocol bytes",
		map[string]any{
			"name":    fs.cfg.Name,
			"op":      op,
			"subject": subj,
			"seq":     seq,
			"marker":  marker,
			"hdr_len": len(hdr),
			"msg_len": len(msg),
			"snippet": string(spliceSnippet(msg)),
			"stack":   string(stack),
		})
}

func spliceSnippet(b []byte) []byte {
	if len(b) <= 256 {
		return b
	}
	return b[:256]
}
