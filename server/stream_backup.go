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
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/s2"
)

type SnapshotConsumerState struct {
	*ConsumerConfig `json:"config"`
	*ConsumerState  `json:"state"`
}

// Create a snapshot of this stream and its consumer's state along with messages.
func CreateStreamSnapshotV2(store StreamStore, deadline time.Duration, includeConsumers bool) (*SnapshotResult, error) {
	pr, pw := net.Pipe()

	// Set a write deadline here to protect ourselves.
	if deadline > 0 {
		pw.SetWriteDeadline(time.Now().Add(deadline))
	}

	// We can add to our stream while snapshotting but not "user" delete anything.
	var state StreamState
	store.FastState(&state)

	// Stream in separate Go routine.
	errCh := make(chan string, 1)
	go streamSnapshotV2(store, &state, pw, includeConsumers, errCh)

	return &SnapshotResult{pr, state, errCh}, nil
}

// Stream our snapshot through S2 compression and tar.
func streamSnapshotV2(store StreamStore, state *StreamState, w io.WriteCloser, includeConsumers bool, errCh chan string) {
	defer close(errCh)
	defer w.Close()

	enc := s2.NewWriter(w)
	defer enc.Close()

	tw := tar.NewWriter(enc)
	defer tw.Close()

	now := time.Now()

	writeGeneric := func(name string, mod time.Time, buf []byte) error {
		hdr := &tar.Header{
			Name:    name,
			Mode:    0600,
			ModTime: mod.UTC(),
			Size:    int64(len(buf)),
			Format:  tar.FormatPAX,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := tw.Write(buf); err != nil {
			return err
		}
		return tw.Flush()
	}

	var msgw bytes.Buffer
	writeStoreMsg := func(msg *StoreMsg) error {
		msgw.Reset()
		msgw.WriteString(fmt.Sprintf("%d %d %s\r\n", len(msg.hdr), len(msg.subj), msg.subj))
		msgw.Write(msg.buf)
		return writeGeneric(
			filepath.Join("msgs", fmt.Sprintf("%d", msg.seq)),
			time.Unix(0, msg.ts),
			msgw.Bytes(),
		)
	}

	writeConsumerMsg := func(scs SnapshotConsumerState) error {
		ssj, err := json.Marshal(scs)
		if err != nil {
			return err
		}
		return writeGeneric(
			filepath.Join("consumers", scs.Name),
			now,
			ssj,
		)
	}

	// If we aren't including consumers here then make sure the consumer count
	// is set accordingly, this helps on the restore path.
	if !includeConsumers {
		state.Consumers = 0
	}

	ssj, err := json.Marshal(state)
	if err != nil {
		errCh <- err.Error()
		return
	}
	if err := writeGeneric("state.json", now, ssj); err != nil {
		errCh <- err.Error()
		return
	}

	// Do consumers first, if the stream is interest/WQ then this may be
	// important for message retention.
	if includeConsumers {
		for o := range store.Consumers() {
			config := o.GetConfig()
			state, err := o.State()
			if err != nil {
				errCh <- fmt.Sprintf("couldn't load consumer '%s' state: %s", config.Name, err)
				return
			}
			if err := writeConsumerMsg(SnapshotConsumerState{
				ConsumerConfig: config,
				ConsumerState:  state,
			}); err != nil {
				errCh <- err.Error()
				return
			}
		}
	}

	var sm StoreMsg
	for seq := state.FirstSeq - 1; seq < state.LastSeq; {
		if _, seq, err = store.LoadNextMsg(fwcs, true, seq+1, &sm); err != nil {
			errCh <- fmt.Sprintf("couldn't load next message after seq %d: %s", seq+1, err)
			return
		}
		if err = writeStoreMsg(&sm); err != nil {
			errCh <- err.Error()
			return
		}
	}
}

// RestoreStreamSnapshotV2 will restore a stream from a snapshot.
func (a *Account) RestoreStreamV2(ncfg *StreamConfig, r io.Reader) (*stream, error) {
	dec := s2.NewReader(r)
	tr := tar.NewReader(dec)

	var nstate StreamState

	// Load the stream state.
	hdr, err := tr.Next()
	if err != nil {
		return nil, err
	}
	if hdr.Name != "state.json" {
		return nil, fmt.Errorf("expected state.json first")
	}
	state, err := io.ReadAll(tr)
	if err != nil {
		return nil, fmt.Errorf("expected state.json contents")
	}
	if err := json.Unmarshal(state, &nstate); err != nil {
		return nil, fmt.Errorf("error in state.json: %w", err)
	}

	s, jsa, err := a.checkForJetStream()
	if err != nil {
		return nil, err
	}
	js := jsa.js
	if js == nil {
		return nil, NewJSNotEnabledForAccountError()
	}
	if _, err := a.lookupStream(ncfg.Name); err == nil {
		return nil, NewJSStreamNameExistRestoreFailedError()
	}

	cfg, apiErr := s.checkStreamCfg(ncfg, a, false)
	if apiErr != nil {
		return nil, apiErr
	}

	mset, err := a.addStream(&cfg)
	if err != nil {
		return nil, fmt.Errorf("error adding stream: %w", err)
	}

	// Start off at the right sequence number. This is important in particular
	// when the backup contains no messages or would restore to no interest.
	if _, err = mset.store.Compact(nstate.FirstSeq); err != nil {
		return nil, fmt.Errorf("error purging stream: %w", err)
	}

	for range nstate.Consumers {
		hdr, err := tr.Next()
		if err != nil {
			return nil, err
		}
		name, found := strings.CutPrefix(hdr.Name, "consumers/")
		if !found {
			return nil, fmt.Errorf("expected consumer, found %q", hdr.Name)
		}
		buf, err := io.ReadAll(tr)
		if err != nil {
			return nil, fmt.Errorf("failed to read consumer %q state: %w", name, err)
		}
		var consumer SnapshotConsumerState
		if err := json.Unmarshal(buf, &consumer); err != nil {
			return nil, fmt.Errorf("failed to decode consumer %q state: %w", name, err)
		}
		o, err := mset.addConsumer(consumer.ConsumerConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to add consumer %q: %w", name, err)
		}
		o.mu.Lock()
		err = o.setStoreState(consumer.ConsumerState)
		o.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("failed to set consumer %q state: %w", name, err)
		}
	}

	store := mset.store
	lseq := nstate.FirstSeq - 1
	for range nstate.Msgs {
		hdr, err := tr.Next()
		if err != nil {
			return nil, err
		}
		seqstr, found := strings.CutPrefix(hdr.Name, "msgs/")
		if !found {
			return nil, fmt.Errorf("expected message")
		}
		seq, err := strconv.ParseUint(seqstr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("expected valid sequence number: %w", err)
		}
		if seq <= lseq {
			return nil, fmt.Errorf("message sequence %d out of order", seq)
		}
		// We could have deleted messages since the last message we stored, if so
		// we should work out what the gap is and skip those sequences.
		if gap := seq - lseq - 1; gap > 0 {
			if err := store.SkipMsgs(lseq+1, gap); err != nil {
				return nil, fmt.Errorf("failed to process gap: %w", err)
			}
		}
		lseq = seq
		var hlen, slen int
		if _, err := fmt.Fscanf(tr, "%d %d", &hlen, &slen); err != nil {
			return nil, fmt.Errorf("failed to scan preamble line: %w", err)
		}
		// We can't know whether the subject contains spaces or exotic characters
		// so treat it as completely opaque by length rather than trying to scan.
		var sb strings.Builder
		n, err := io.CopyN(&sb, tr, int64(slen))
		if err != nil || n != int64(slen) {
			return nil, fmt.Errorf("failed to retrieve subject from preamble line: %w", err)
		}
		if _, err := fmt.Fscanf(tr, "\r\n"); err != nil {
			return nil, fmt.Errorf("failed to find end of preamble line: %w", err)
		}
		// At this point preamble already consumed the first line, so can continue
		// to read onward from r's current cursor position.
		buf, err := io.ReadAll(tr)
		if err != nil {
			return nil, fmt.Errorf("failed to read message sequence %d: %w", seq, err)
		}
		mhdr, msg := buf[:hlen], buf[hlen:]
		// TODO(nat): check TTL and discard new headers
		ttl, err := getMessageTTL(mhdr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse message TTL: %w", err)
		}
		if err = store.StoreRawMsg(sb.String(), mhdr, msg, seq, hdr.ModTime.UnixNano(), ttl, false); err != nil {
			return nil, fmt.Errorf("failed to store message sequence %d: %w", seq, err)
		}
	}

	if _, err := tr.Next(); err != io.EOF {
		return nil, fmt.Errorf("unexpected trailing entries")
	}

	return mset, nil
}
