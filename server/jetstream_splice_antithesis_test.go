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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/internal/antithesis"
	"github.com/nats-io/nats.go"
)

// TestJetStreamPayloadSpliceAntithesis exercises an R3 file-backed JS
// cluster under concurrent publish + push-consumer ack and asserts via
// the Antithesis SDK that no stored stream payload contains bytes
// belonging to an in-flight NATS protocol frame.
//
// The assertion fires when a stored payload contains any of:
//   - "\nHMSG " or "\nRMSG " — a NATS server-to-server header-msg line
//   - "NATS/1.0\r\n" — the NATS headers preamble inside a payload body
//   - "$JS.ACK." — a JetStream consumer ack reply subject
//
// Each published payload is tagged with the SHA-256 of its body in a
// header; if the persisted bytes don't hash back to that digest, that
// is also flagged unreachable (catches non-protocol byte tampering).
//
// The test is self-contained, uses generic subject and header names,
// and is safe to run repeatedly. Build with -tags enable_antithesis_sdk
// to fire actual Antithesis assertions; without the tag the helpers
// print to stdout and the test still fails via t.Errorf so CI catches
// any reproduction.
func TestJetStreamPayloadSpliceAntithesis(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "SPLC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	const numStreams = 4
	streamNames := make([]string, numStreams)
	for i := 0; i < numStreams; i++ {
		streamNames[i] = fmt.Sprintf("EVENTS%d", i)
		_, err := js.AddStream(&nats.StreamConfig{
			Name:      streamNames[i],
			Subjects:  []string{fmt.Sprintf("evt%d.>", i)},
			Storage:   nats.FileStorage,
			Replicas:  3,
			Retention: nats.LimitsPolicy,
		})
		require_NoError(t, err)
		_, err = js.AddConsumer(streamNames[i], &nats.ConsumerConfig{
			Durable:        "worker",
			AckPolicy:      nats.AckExplicitPolicy,
			DeliverSubject: fmt.Sprintf("dlv.%d", i),
			DeliverGroup:   "g",
			MaxAckPending:  4096,
		})
		require_NoError(t, err)
	}

	runDuration := 2 * time.Minute
	const (
		publishers   = 16
		consumers    = 8
		minPayloadKB = 1
		maxPayloadKB = 12
	)

	ctx, cancel := context.WithTimeout(context.Background(), runDuration)
	defer cancel()

	var publishedCount atomic.Int64
	var ackedCount atomic.Int64

	pubWG := sync.WaitGroup{}
	pubWG.Add(publishers)
	for p := 0; p < publishers; p++ {
		go func(id int) {
			defer pubWG.Done()
			pnc, pjs := jsClientConnect(t, c.servers[id%len(c.servers)])
			defer pnc.Close()
			r := rand.New(rand.NewSource(int64(id)*1000 + time.Now().UnixNano()))
			seq := uint64(0)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				seq++
				stream := r.Intn(numStreams)
				key := genHexKey(r)
				subj := fmt.Sprintf("evt%d.k.%s", stream, key)
				payload := genTestPayload(r, key, id, seq, minPayloadKB, maxPayloadKB)
				sum := sha256.Sum256(payload)
				digest := hex.EncodeToString(sum[:])

				m := nats.NewMsg(subj)
				m.Data = payload
				m.Header.Set("Nats-Msg-Id", fmt.Sprintf("p%d-s%d", id, seq))
				m.Header.Set(spliceTestDigestHeader, digest)
				m.Header.Set("X-Test-Pub", fmt.Sprintf("%d", id))
				m.Header.Set("X-Test-Seq", fmt.Sprintf("%d", seq))

				if _, err := pjs.PublishMsg(m, nats.AckWait(5*time.Second)); err != nil {
					if ctx.Err() != nil {
						return
					}
					continue
				}
				publishedCount.Add(1)
			}
		}(p)
	}

	conWG := sync.WaitGroup{}
	conWG.Add(consumers * numStreams)
	for s := 0; s < numStreams; s++ {
		for ci := 0; ci < consumers; ci++ {
			go func(streamIdx, id int) {
				defer conWG.Done()
				cnc, cjs := jsClientConnect(t, c.servers[(id+streamIdx)%len(c.servers)])
				defer cnc.Close()
				sub, err := cjs.QueueSubscribeSync("", "g",
					nats.Bind(streamNames[streamIdx], "worker"), nats.ManualAck())
				if err != nil {
					return
				}
				defer sub.Unsubscribe()
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					m, err := sub.NextMsgWithContext(ctx)
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						continue
					}
					_ = m.Ack()
					ackedCount.Add(1)
				}
			}(s, ci)
		}
	}

	pubWG.Wait()
	conWG.Wait()

	t.Logf("published=%d acked=%d", publishedCount.Load(), ackedCount.Load())
	time.Sleep(2 * time.Second)

	// Verification: walk every stored message on every server and
	// fire an Antithesis Unreachable assertion if any payload either
	// contains splice-marker byte sequences or hashes to a value that
	// doesn't match the digest carried in its own header.
	for _, s := range c.servers {
		for _, sn := range streamNames {
			mset, err := s.globalAccount().lookupStream(sn)
			require_NoError(t, err)

			state := mset.state()
			var smv StoreMsg
			for seq := state.FirstSeq; seq <= state.LastSeq; seq++ {
				sm, err := mset.store.LoadMsg(seq, &smv)
				require_NoError(t, err)
				checkStoredMsgForSplice(t, s.Name(), sn, sm)
			}
		}
	}
}

const spliceTestDigestHeader = "X-Test-Sha256"

// checkStoredMsgForSplice fires an Antithesis Unreachable assertion if
// the loaded stored message looks corrupted: protocol-frame bytes inside
// the payload, or a SHA-256 mismatch against the header-carried digest.
func checkStoredMsgForSplice(t testing.TB, srv, stream string, sm *StoreMsg) {
	if sm == nil {
		return
	}

	if marker, off := scanForProtocolBytes(sm.msg); marker != "" {
		antithesis.AssertUnreachable(t, "Stored stream payload contains in-flight protocol bytes",
			map[string]any{
				"server":   srv,
				"stream":   stream,
				"subject":  sm.subj,
				"sequence": sm.seq,
				"marker":   marker,
				"offset":   off,
				"hdr_len":  len(sm.hdr),
				"msg_len":  len(sm.msg),
				"snippet":  string(spliceContextSnippet(sm.msg, off, 96)),
			})
		t.Errorf("splice in %s/%s seq=%d marker=%s offset=%d", srv, stream, sm.seq, marker, off)
	}

	expected := sliceHeader(spliceTestDigestHeader, sm.hdr)
	if len(expected) == 0 {
		// Header missing — could indicate header corruption.
		antithesis.AssertUnreachable(t, "Stored stream message missing test digest header",
			map[string]any{
				"server":   srv,
				"stream":   stream,
				"subject":  sm.subj,
				"sequence": sm.seq,
				"hdr":      string(sm.hdr),
			})
		t.Errorf("missing digest header in %s/%s seq=%d", srv, stream, sm.seq)
		return
	}
	got := sha256.Sum256(sm.msg)
	gotHex := hex.EncodeToString(got[:])
	if gotHex != string(expected) {
		antithesis.AssertUnreachable(t, "Stored stream payload SHA-256 mismatch",
			map[string]any{
				"server":   srv,
				"stream":   stream,
				"subject":  sm.subj,
				"sequence": sm.seq,
				"want_sha": string(expected),
				"got_sha":  gotHex,
				"msg_len":  len(sm.msg),
				"snippet":  string(spliceContextSnippet(sm.msg, 0, 96)),
			})
		t.Errorf("payload sha mismatch in %s/%s seq=%d", srv, stream, sm.seq)
	}
}

// scanForProtocolBytes returns the first detected splice marker name
// and its byte offset, or "" if none.
func scanForProtocolBytes(b []byte) (string, int) {
	if len(b) == 0 {
		return "", 0
	}
	for _, m := range []struct {
		name  string
		bytes []byte
	}{
		{"HMSG", []byte("\nHMSG ")},
		{"RMSG", []byte("\nRMSG ")},
		{"NATS/1.0", []byte("NATS/1.0\r\n")},
		{"JS.ACK", []byte("$JS.ACK.")},
	} {
		if i := bytes.Index(b, m.bytes); i >= 0 {
			return m.name, i
		}
	}
	return "", 0
}

func spliceContextSnippet(b []byte, off, span int) []byte {
	lo := off - span
	if lo < 0 {
		lo = 0
	}
	hi := off + span
	if hi > len(b) {
		hi = len(b)
	}
	return b[lo:hi]
}

// genHexKey returns a 16-character hex key derived from r.
func genHexKey(r *rand.Rand) string {
	var b [8]byte
	for i := range b {
		b[i] = byte(r.Intn(256))
	}
	return hex.EncodeToString(b[:])
}

// genTestPayload returns a generic-shape payload of approximately
// minKB-maxKB. Avoids any production-specific schema.
func genTestPayload(r *rand.Rand, key string, pub int, seq uint64, minKB, maxKB int) []byte {
	targetKB := minKB + r.Intn(maxKB-minKB+1)
	var b strings.Builder
	b.Grow(targetKB*1024 + 256)
	fmt.Fprintf(&b, `{"id":%q,"pub":%d,"seq":%d,"payload":"`, key, pub, seq)
	pad := strings.Repeat("a", 64)
	for b.Len() < targetKB*1024 {
		b.WriteString(pad)
	}
	b.WriteString(`"}`)
	return []byte(b.String())
}
