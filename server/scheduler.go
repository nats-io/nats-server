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
	"encoding/binary"
	"errors"
	"io"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server/thw"
)

// Error for when we try to decode a binary-encoded message schedule with an unknown version number.
var ErrMsgScheduleInvalidVersion = errors.New("msg scheduling: encoded version not known")

const (
	headerLen = 17 // 1 byte magic + 2x uint64s
)

type MsgScheduling struct {
	run       func()
	ttls      *thw.HashWheel
	timer     *time.Timer
	running   bool
	deadline  int64
	schedules map[string]*MsgSchedule
	seqToSubj map[uint64]string
	inflight  map[string]struct{}
}

type MsgSchedule struct {
	seq uint64
	ts  int64
}

func newMsgScheduling(run func()) *MsgScheduling {
	return &MsgScheduling{
		run:       run,
		ttls:      thw.NewHashWheel(),
		schedules: make(map[string]*MsgSchedule),
		seqToSubj: make(map[uint64]string),
		inflight:  make(map[string]struct{}),
	}
}

func (ms *MsgScheduling) add(seq uint64, subj string, ts int64) {
	ms.init(seq, subj, ts)
	ms.resetTimer()
}

func (ms *MsgScheduling) init(seq uint64, subj string, ts int64) {
	if sched, ok := ms.schedules[subj]; ok {
		delete(ms.seqToSubj, sched.seq)
		// Remove and add separately, since they'll have different sequences.
		ms.ttls.Remove(sched.seq, sched.ts)
		ms.ttls.Add(seq, ts)
		sched.ts, sched.seq = ts, seq
	} else {
		ms.ttls.Add(seq, ts)
		ms.schedules[subj] = &MsgSchedule{seq: seq, ts: ts}
	}
	ms.seqToSubj[seq] = subj
	delete(ms.inflight, subj)
}

func (ms *MsgScheduling) update(subj string, ts int64) {
	if sched, ok := ms.schedules[subj]; ok {
		// Remove and add separately, it's for the same sequence, but if replicated
		// this server could not know the previous timestamp yet.
		ms.ttls.Remove(sched.seq, sched.ts)
		ms.ttls.Add(sched.seq, ts)
		sched.ts = ts
		delete(ms.inflight, subj)
		ms.resetTimer()
	}
}

func (ms *MsgScheduling) markInflight(subj string) {
	if _, ok := ms.schedules[subj]; ok {
		ms.inflight[subj] = struct{}{}
	}
}

func (ms *MsgScheduling) isInflight(subj string) bool {
	_, ok := ms.inflight[subj]
	return ok
}

func (ms *MsgScheduling) remove(seq uint64) {
	if subj, ok := ms.seqToSubj[seq]; ok {
		delete(ms.seqToSubj, seq)
		delete(ms.schedules, subj)
	}
}

func (ms *MsgScheduling) removeSubject(subj string) {
	if sched, ok := ms.schedules[subj]; ok {
		ms.ttls.Remove(sched.seq, sched.ts)
		delete(ms.schedules, subj)
		delete(ms.seqToSubj, sched.seq)
	}
}

func (ms *MsgScheduling) clearInflight() {
	ms.inflight = make(map[string]struct{})
}

func (ms *MsgScheduling) resetTimer() {
	// If we're already scheduling messages, it will make sure to reset.
	// Don't trigger again, as that could result in many expire goroutines.
	if ms.running {
		return
	}

	next := ms.ttls.GetNextExpiration(math.MaxInt64)
	if next == math.MaxInt64 {
		clearTimer(&ms.timer)
		return
	}
	fireIn := time.Until(time.Unix(0, next))

	// Make sure we aren't firing too often either way, otherwise we can
	// negatively impact stream ingest performance.
	if fireIn < 250*time.Millisecond {
		fireIn = 250 * time.Millisecond
	}

	// If we want to kick the timer to run later than what was assigned before, don't reset it.
	// Otherwise, we could get in a situation where the timer is continuously reset, and it never runs.
	deadline := time.Now().UnixNano() + fireIn.Nanoseconds()
	if ms.deadline > 0 && deadline > ms.deadline {
		return
	}

	ms.deadline = deadline
	if ms.timer != nil {
		ms.timer.Reset(fireIn)
	} else {
		ms.timer = time.AfterFunc(fireIn, ms.run)
	}
}

func (ms *MsgScheduling) getScheduledMessages(loadMsg func(seq uint64, smv *StoreMsg) *StoreMsg, loadLast func(subj string, smv *StoreMsg) *StoreMsg) []*inMsg {
	var (
		smv  StoreMsg
		sm   *StoreMsg
		msgs []*inMsg
	)
	ms.ttls.ExpireTasks(func(seq uint64, ts int64) bool {
		// Need to grab the message for the specified sequence, and check
		// if it hasn't been removed in the meantime.
		sm = loadMsg(seq, &smv)
		if sm != nil {
			// If already inflight, don't duplicate a scheduled message. The stream could
			// be replicated and the scheduled message could take some time to propagate.
			subj := sm.subj
			if ms.isInflight(subj) {
				return false
			}
			// Validate the contents are correct if not, we just remove it from THW.
			pattern := bytesToString(sliceHeader(JSSchedulePattern, sm.hdr))
			if pattern == _EMPTY_ {
				ms.remove(seq)
				return true
			}
			next, repeat, ok := parseMsgSchedule(pattern, ts)
			if !ok {
				ms.remove(seq)
				return true
			}
			ttl, ok := getMessageScheduleTTL(sm.hdr)
			if !ok {
				ms.remove(seq)
				return true
			}
			target := getMessageScheduleTarget(sm.hdr)
			if target == _EMPTY_ {
				ms.remove(seq)
				return true
			}
			source := getMessageScheduleSource(sm.hdr)
			if source != _EMPTY_ {
				if sm = loadLast(source, &smv); sm == nil {
					ms.remove(seq)
					return true
				}
			}

			// Copy, as this is retrieved directly from storage, and we'll need to keep hold of this for some time.
			// And in the case of headers, we'll copy all of them, but make changes.
			hdr, msg := copyBytes(sm.hdr), copyBytes(sm.msg)

			// Strip headers specific to the schedule.
			hdr = removeHeaderIfPresent(hdr, JSSchedulePattern)
			hdr = removeHeaderIfPrefixPresent(hdr, "Nats-Schedule-")
			hdr = removeHeaderIfPrefixPresent(hdr, "Nats-Expected-")
			hdr = removeHeaderIfPresent(hdr, JSMsgId)
			hdr = removeHeaderIfPresent(hdr, JSMessageTTL)
			hdr = removeHeaderIfPresent(hdr, JSMsgRollup)

			// Add headers for the scheduled message.
			hdr = genHeader(hdr, JSScheduler, subj)
			if !repeat {
				hdr = genHeader(hdr, JSScheduleNext, JSScheduleNextPurge) // Purge the schedule message itself.
			} else {
				hdr = genHeader(hdr, JSScheduleNext, next.Format(time.RFC3339)) // Next time the schedule fires.
			}
			if ttl != _EMPTY_ {
				hdr = genHeader(hdr, JSMessageTTL, ttl)
			}
			msgs = append(msgs, &inMsg{seq: seq, subj: target, hdr: hdr, msg: msg})
			ms.markInflight(subj)
			return false
		}
		ms.remove(seq)
		return true
	})
	// THW is unordered, so must sort by sequence.
	slices.SortFunc(msgs, func(a, b *inMsg) int {
		if a.seq == b.seq {
			return 0
		} else if a.seq < b.seq {
			return -1
		} else {
			return 1
		}
	})
	return msgs
}

// encode writes out the contents of the schedule into a binary snapshot
// and returns it. The high seq number is included in the snapshot and will
// be returned on decode.
func (ms *MsgScheduling) encode(highSeq uint64) []byte {
	count := uint64(len(ms.schedules))
	b := make([]byte, 0, headerLen+(count*(2*binary.MaxVarintLen64)))
	b = append(b, 1)                                 // Magic version
	b = binary.LittleEndian.AppendUint64(b, count)   // Entry count
	b = binary.LittleEndian.AppendUint64(b, highSeq) // Stamp
	for subj, sched := range ms.schedules {
		slen := min(uint64(len(subj)), math.MaxUint16)
		b = binary.LittleEndian.AppendUint16(b, uint16(slen))
		b = append(b, subj[:slen]...)
		b = binary.AppendVarint(b, sched.ts)
		b = binary.AppendUvarint(b, sched.seq)
	}
	return b
}

// decode snapshots a binary-encoded schedule and replaces the contents of this
// schedule with them. Returns the high seq number from the snapshot.
func (ms *MsgScheduling) decode(b []byte) (uint64, error) {
	if len(b) < headerLen {
		return 0, io.ErrShortBuffer
	}
	if b[0] != 1 {
		return 0, ErrMsgScheduleInvalidVersion
	}

	count := binary.LittleEndian.Uint64(b[1:])
	stamp := binary.LittleEndian.Uint64(b[9:])
	b = b[headerLen:]
	for i := uint64(0); i < count; i++ {
		sl := int(binary.LittleEndian.Uint16(b))
		b = b[2:]
		if len(b) < sl {
			return 0, io.ErrUnexpectedEOF
		}
		subj := string(b[:sl])
		b = b[sl:]
		ts, tn := binary.Varint(b)
		if tn < 0 {
			return 0, io.ErrUnexpectedEOF
		}
		seq, vn := binary.Uvarint(b[tn:])
		if vn < 0 {
			return 0, io.ErrUnexpectedEOF
		}
		ms.init(seq, subj, ts)
		b = b[tn+vn:]
	}
	return stamp, nil
}

// parseMsgSchedule parses a message schedule pattern and returns the time
// to fire, whether it is a repeating schedule, and whether the pattern was valid.
func parseMsgSchedule(pattern string, ts int64) (time.Time, bool, bool) {
	if pattern == _EMPTY_ {
		return time.Time{}, false, true
	}
	// Exact time.
	if strings.HasPrefix(pattern, "@at ") {
		t, err := time.Parse(time.RFC3339, pattern[4:])
		return t, false, err == nil
	}
	// Repeating on a simple interval.
	if strings.HasPrefix(pattern, "@every ") {
		dur, err := time.ParseDuration(pattern[7:])
		if err != nil {
			return time.Time{}, false, false
		}
		// Only allow intervals of at least a second.
		if dur.Seconds() < 1 {
			return time.Time{}, false, false
		}
		// If this schedule would trigger multiple times, for example after a restart, skip ahead and only fire once.
		next := time.Unix(0, ts).UTC().Round(time.Second).Add(dur)
		if now := time.Now().UTC(); next.Before(now) {
			next = now.Round(time.Second).Add(dur)
		}
		return next, true, true
	}
	return time.Time{}, false, false

}
