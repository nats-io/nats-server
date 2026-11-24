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

// Package subject provides helpers for NATS subject tokens.
package subject

import (
	"encoding/binary"
	"math/bits"
)

// Tokenize subject into the given tokens slice.
func Tokenize(tokens []string, subject string) []string {
	data := []byte(subject)
	start, offset := 0, 0
	for len(data)-offset >= 8 {
		word := binary.LittleEndian.Uint64(data[offset : offset+8])
		for mask := subjectDotMask(word); mask != 0; mask = removeFirstDot(mask) {
			end := offset + firstDotIndex(mask)
			tokens = append(tokens, subject[start:end])
			start = end + 1
		}
		offset += 8
	}
	for i := offset; i < len(subject); i++ {
		if subject[i] == '.' {
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	return append(tokens, subject[start:])
}

// TokenizeNoEmpty is like Tokenize but returns nil if subject contains an empty token.
func TokenizeNoEmpty(tokens []string, subject string) []string {
	data := []byte(subject)
	start, offset := 0, 0
	for len(data)-offset >= 8 {
		word := binary.LittleEndian.Uint64(data[offset : offset+8])
		for mask := subjectDotMask(word); mask != 0; mask = removeFirstDot(mask) {
			end := offset + firstDotIndex(mask)
			if end == start {
				return nil
			}
			tokens = append(tokens, subject[start:end])
			start = end + 1
		}
		offset += 8
	}
	for i := offset; i < len(subject); i++ {
		if subject[i] == '.' {
			if i == start {
				return nil
			}
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	if start >= len(subject) {
		return nil
	}
	return append(tokens, subject[start:])
}

// firstDotIndex returns the byte index of the first dot in a nonzero mask.
func firstDotIndex(mask uint64) int {
	return bits.TrailingZeros64(mask) / 8
}

// removeFirstDot clears the lowest set bit, which marks the first dot.
func removeFirstDot(mask uint64) uint64 {
	return mask & (mask - 1)
}

// subjectDotMask returns a mask with 0b10000000 for each '.' byte in word
// and 0b00000000 for every other byte.
func subjectDotMask(word uint64) uint64 {
	const highBits uint64 = 0x8080808080808080
	const eightDots uint64 = 0x2e2e2e2e2e2e2e2e
	// Turn each dot into a zero byte.
	x := word ^ eightDots
	// If any bit in a byte in x is set, the shifts set that byte's high bit.
	x |= x << 1
	x |= x << 2
	x |= x << 4
	return ^x & highBits
}
