// Copyright 2024 The NATS Authors
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

package thw

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"time"
)

// Error for when we can not locate a task for removal or updates.
var ErrTaskNotFound = errors.New("thw: task not found")

// Error for when we try to decode a binary-encoded THW with an unknown version number.
var ErrInvalidVersion = errors.New("thw: encoded version not known")

const (
	tickDuration = int64(time.Second) // Tick duration in nanoseconds.
	wheelBits    = 12                 // 2^12 = 4096 slots.
	wheelSize    = 1 << wheelBits     // Number of slots in the wheel.
	wheelMask    = wheelSize - 1      // Mask for calculating position.
	headerLen    = 17                 // 1 byte magic + 2x uint64s
)

// slot represents a single slot in the wheel.
type slot struct {
	entries map[uint64]int64 // Map of sequence to expires.
	lowest  int64            // Lowest expiration time in this slot.
}

// HashWheel represents the timing wheel.
type HashWheel struct {
	wheel  []*slot // Array of slots.
	lowest int64   // Track the lowest expiration time across all slots.
	count  uint64  // How many entries are present?
}

// NewHashWheel initializes a new HashWheel.
func NewHashWheel() *HashWheel {
	return &HashWheel{
		wheel:  make([]*slot, wheelSize),
		lowest: math.MaxInt64,
	}
}

// getPosition calculates the slot position for a given expiration time.
func (hw *HashWheel) getPosition(expires int64) int64 {
	return (expires / tickDuration) & wheelMask
}

// updateLowestExpires finds the new lowest expiration time across all slots.
func (hw *HashWheel) updateLowestExpires() {
	lowest := int64(math.MaxInt64)
	for _, s := range hw.wheel {
		if s != nil && s.lowest < lowest {
			lowest = s.lowest
		}
	}
	hw.lowest = lowest
}

// newSlot creates a new slot.
func newSlot() *slot {
	return &slot{
		entries: make(map[uint64]int64),
		lowest:  math.MaxInt64,
	}
}

// Add schedules a new timer task.
func (hw *HashWheel) Add(seq uint64, expires int64) error {
	pos := hw.getPosition(expires)
	// Initialize the slot lazily.
	if hw.wheel[pos] == nil {
		hw.wheel[pos] = newSlot()
	}
	if _, ok := hw.wheel[pos].entries[seq]; !ok {
		hw.count++
	}
	hw.wheel[pos].entries[seq] = expires

	// Update slot's lowest expiration if this is earlier.
	if expires < hw.wheel[pos].lowest {
		hw.wheel[pos].lowest = expires
		// Update global lowest if this is now the earliest.
		if expires < hw.lowest {
			hw.lowest = expires
		}
	}

	return nil
}

// Remove removes a timer task.
func (hw *HashWheel) Remove(seq uint64, expires int64) error {
	pos := hw.getPosition(expires)
	s := hw.wheel[pos]
	if s == nil {
		return ErrTaskNotFound
	}
	if _, exists := s.entries[seq]; !exists {
		return ErrTaskNotFound
	}
	delete(s.entries, seq)
	hw.count--

	// If the slot is empty, we can set it to nil to free memory.
	if len(s.entries) == 0 {
		hw.wheel[pos] = nil
	} else if expires == s.lowest {
		// Find new lowest in this slot.
		lowest := int64(math.MaxInt64)
		for _, exp := range s.entries {
			if exp < lowest {
				lowest = exp
			}
		}
		s.lowest = lowest
	}

	// If we removed the global lowest, find the new one.
	if expires == hw.lowest {
		hw.updateLowestExpires()
	}

	return nil
}

// Update updates the expiration time of an existing timer task.
func (hw *HashWheel) Update(seq uint64, oldExpires int64, newExpires int64) error {
	// Remove from old position.
	if err := hw.Remove(seq, oldExpires); err != nil {
		return err
	}
	// Add to new position.
	return hw.Add(seq, newExpires)
}

// ExpireTasks processes all expired tasks using a callback.
func (hw *HashWheel) ExpireTasks(callback func(seq uint64, expires int64)) {
	now := time.Now().UnixNano()

	// Quick return if nothing is expired.
	if hw.lowest > now {
		return
	}

	// Start from the slot containing the lowest expiration.
	startPos, exitPos := hw.getPosition(hw.lowest), hw.getPosition(now+tickDuration)
	var updateLowest bool

	for offset := int64(0); ; offset++ {
		pos := (startPos + offset) & wheelMask
		if pos == exitPos {
			if updateLowest {
				hw.updateLowestExpires()
			}
			return
		}
		// Grab our slot.
		slot := hw.wheel[pos]
		if slot == nil || slot.lowest > now {
			continue
		}

		// Track new lowest while processing expirations
		newLowest := int64(math.MaxInt64)
		for seq, expires := range slot.entries {
			if expires <= now {
				callback(seq, expires)
				delete(slot.entries, seq)
				hw.count--
				updateLowest = true
			} else if expires < newLowest {
				newLowest = expires
			}
		}

		// Nil out if we are empty.
		if len(slot.entries) == 0 {
			hw.wheel[pos] = nil
		} else {
			slot.lowest = newLowest
		}
	}
}

// GetNextExpiration returns the earliest expiration time before the given time.
// Returns math.MaxInt64 if no expirations exist before the specified time.
func (hw *HashWheel) GetNextExpiration(before int64) int64 {
	if hw.lowest < before {
		return hw.lowest
	}
	return math.MaxInt64
}

// AppendEncode writes out the contents of the THW into a binary snapshot
// and returns it. The high seq number is included in the snapshot and will
// be returned on decode.
func (hw *HashWheel) Encode(highSeq uint64) []byte {
	b := make([]byte, 0, headerLen+(hw.count*(2*binary.MaxVarintLen64)))
	b = append(b, 1)                                  // Magic version
	b = binary.LittleEndian.AppendUint64(b, hw.count) // Entry count
	b = binary.LittleEndian.AppendUint64(b, highSeq)  // Stamp
	for _, slot := range hw.wheel {
		if slot == nil || slot.entries == nil {
			continue
		}
		for v, ts := range slot.entries {
			b = binary.AppendVarint(b, ts)
			b = binary.AppendUvarint(b, v)
		}
	}
	return b
}

// Decode snapshots a binary-encoded THW and replaces the contents of this
// THW with them. Returns the high seq number from the snapshot.
func (hw *HashWheel) Decode(b []byte) (uint64, error) {
	if len(b) < headerLen {
		return 0, io.ErrShortBuffer
	}
	if b[0] != 1 {
		return 0, ErrInvalidVersion
	}
	hw.wheel = make([]*slot, wheelSize)
	hw.lowest = math.MaxInt64
	count := binary.LittleEndian.Uint64(b[1:])
	stamp := binary.LittleEndian.Uint64(b[9:])
	b = b[headerLen:]
	for i := uint64(0); i < count; i++ {
		ts, tn := binary.Varint(b)
		if tn < 0 {
			return 0, io.ErrUnexpectedEOF
		}
		v, vn := binary.Uvarint(b[tn:])
		if vn < 0 {
			return 0, io.ErrUnexpectedEOF
		}
		hw.Add(v, ts)
		b = b[tn+vn:]
	}
	return stamp, nil
}
