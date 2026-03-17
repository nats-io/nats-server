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
	"bufio"
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
// sa is passed in when the stream is clustered, so we can find child consumer assignments.
func (js *jetStream) CreateStreamSnapshotV2(store StreamStore, deadline time.Duration, includeConsumers bool, sa *streamAssignment) (*SnapshotResult, error) {
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
	go js.streamSnapshotV2(store, &state, pw, includeConsumers, sa, errCh)

	return &SnapshotResult{pr, state, errCh}, nil
}

// Stream our snapshot through S2 compression and tar.
func (js *jetStream) streamSnapshotV2(store StreamStore, state *StreamState, w io.WriteCloser, includeConsumers bool, sa *streamAssignment, errCh chan string) {
	defer close(errCh)
	defer w.Close()

	enc := s2.NewWriter(w)
	defer enc.Close()

	tw := tar.NewWriter(enc)
	defer tw.Close()

	now := time.Now()
	clustered := js.isClustered()

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
	var consumerAssignments map[string]*consumerAssignment
	var streamState = *state
	if !includeConsumers {
		streamState.Consumers = 0
	} else if clustered {
		js.mu.RLock()
		consumerAssignments = make(map[string]*consumerAssignment, len(sa.consumers))
		for name, ca := range sa.consumers {
			consumerAssignments[name] = ca.copyGroup()
		}
		streamState.Consumers = len(consumerAssignments)
		js.mu.RUnlock()
	}

	ssj, err := json.Marshal(streamState)
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
		consumerStateFromInfo := func(ci *ConsumerInfo) *ConsumerState {
			state := &ConsumerState{
				Delivered: SequencePair{
					Consumer: ci.Delivered.Consumer,
					Stream:   ci.Delivered.Stream,
				},
				AckFloor: SequencePair{
					Consumer: ci.AckFloor.Consumer,
					Stream:   ci.AckFloor.Stream,
				},
			}
			if ci.NumAckPending <= 0 || ci.Delivered.Stream <= ci.AckFloor.Stream || ci.Delivered.Consumer <= ci.AckFloor.Consumer {
				return state
			}

			pending := uint64(ci.NumAckPending)
			if maxPending := ci.Delivered.Stream - ci.AckFloor.Stream; pending > maxPending {
				pending = maxPending
			}
			if maxPending := ci.Delivered.Consumer - ci.AckFloor.Consumer; pending > maxPending {
				pending = maxPending
			}
			if pending == 0 {
				return state
			}

			// Cluster consumer info does not include sparse pending details.
			// Approximate pending with a contiguous range above the ack floor.
			state.Pending = make(map[uint64]*Pending, int(pending))
			ts := now.UnixNano()
			for i := uint64(0); i < pending; i++ {
				state.Pending[ci.AckFloor.Stream+1+i] = &Pending{
					Sequence:  ci.AckFloor.Consumer + 1 + i,
					Timestamp: ts,
				}
			}
			return state
		}

		if clustered {
			if sa == nil {
				errCh <- "stream assignment not present in clustered mode"
				return
			}
			for _, ca := range consumerAssignments {
				ci, err := sysRequest[ConsumerInfo](js.srv, clusterConsumerInfoT, sa.Client.serviceAccount(), sa.Config.Name, ca.Name)
				if err != nil || ci == nil {
					errCh <- fmt.Sprintf("failed to get consumer state for '%s > %s'", sa.Config.Name, ca.Name)
					return
				}
				if err := writeConsumerMsg(SnapshotConsumerState{
					ConsumerConfig: ca.Config,
					ConsumerState:  consumerStateFromInfo(ci),
				}); err != nil {
					errCh <- err.Error()
					return
				}
			}
		} else {
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

	_, isClustered := jsa.jetStreamAndClustered()
	jsa.usageMu.RLock()
	selected, tier, hasTier := jsa.selectLimits(cfg.Replicas)
	jsa.usageMu.RUnlock()
	reserved := int64(0)
	if hasTier {
		if isClustered {
			js.mu.RLock()
			_, reserved = js.tieredStreamAndReservationCount(a.Name, tier, &cfg)
			js.mu.RUnlock()
		} else {
			reserved = jsa.tieredReservation(tier, &cfg)
		}
	}
	var bc int64
	js.mu.RLock()
	err = js.checkAllLimits(&selected, &cfg, reserved, bc)
	js.mu.RUnlock()
	if err != nil {
		return nil, err
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
		bc += hdr.Size
		js.mu.RLock()
		err = js.checkAllLimits(&selected, &cfg, reserved, bc)
		js.mu.RUnlock()
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
		seq, err := strconv.ParseUint(filepath.Base(seqstr), 10, 64)
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
		br := bufio.NewReader(tr)
		subj, hlen, err := parseSnapshotMessagePreamble(br)
		if err != nil {
			return nil, fmt.Errorf("failed to parse preamble line: %w", err)
		}
		buf, err := io.ReadAll(br)
		if err != nil {
			return nil, fmt.Errorf("failed to read message sequence %d: %w", seq, err)
		}
		if hlen > len(buf) {
			return nil, fmt.Errorf("failed to parse message sequence %d: invalid preamble header content", seq)
		}
		mhdr, msg := buf[:hlen], buf[hlen:]
		switch cfg.Storage {
		case MemoryStorage:
			bc += int64(memStoreMsgSize(subj, mhdr, msg))
		default:
			bc += int64(fileStoreMsgSize(subj, mhdr, msg))
		}
		js.mu.RLock()
		err = js.checkAllLimits(&selected, &cfg, reserved, bc)
		js.mu.RUnlock()
		if err != nil {
			return nil, err
		}
		// TODO(nat): check TTL and discard new headers
		ttl, err := getMessageTTL(mhdr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse message TTL: %w", err)
		}
		if err = store.StoreRawMsg(subj, mhdr, msg, seq, hdr.ModTime.UnixNano(), ttl, false); err != nil {
			return nil, fmt.Errorf("failed to store message sequence %d: %w", seq, err)
		}
	}

	if _, err := tr.Next(); err != io.EOF {
		return nil, fmt.Errorf("unexpected trailing entries")
	}

	return mset, nil
}

func parseSnapshotMessagePreamble(r io.Reader) (string, int, error) {
	var hlen, slen int
	if _, err := fmt.Fscanf(r, "%d %d", &hlen, &slen); err != nil {
		return "", 0, err
	}
	if hlen < 0 || slen < 0 {
		return "", 0, fmt.Errorf("invalid lengths")
	}

	var sep [1]byte
	if _, err := io.ReadFull(r, sep[:]); err != nil {
		return "", 0, err
	}
	if sep[0] != ' ' {
		return "", 0, fmt.Errorf("missing subject separator")
	}

	subj := make([]byte, slen)
	if _, err := io.ReadFull(r, subj); err != nil {
		return "", 0, err
	}

	var eol [2]byte
	if _, err := io.ReadFull(r, eol[:]); err != nil {
		return "", 0, err
	}
	if !bytes.Equal(eol[:], []byte("\r\n")) {
		return "", 0, fmt.Errorf("missing preamble terminator")
	}

	return string(subj), hlen, nil
}
