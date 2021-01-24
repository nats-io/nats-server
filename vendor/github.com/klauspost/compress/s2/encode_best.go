// Copyright 2016 The Snappy-Go Authors. All rights reserved.
// Copyright (c) 2019 Klaus Post. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package s2

import (
	"fmt"
	"math/bits"
)

// encodeBlockBest encodes a non-empty src to a guaranteed-large-enough dst. It
// assumes that the varint-encoded length of the decompressed bytes has already
// been written.
//
// It also assumes that:
//	len(dst) >= MaxEncodedLen(len(src)) &&
// 	minNonLiteralBlockSize <= len(src) && len(src) <= maxBlockSize
func encodeBlockBest(dst, src []byte) (d int) {
	// Initialize the hash tables.
	const (
		// Long hash matches.
		lTableBits    = 19
		maxLTableSize = 1 << lTableBits

		// Short hash matches.
		sTableBits    = 16
		maxSTableSize = 1 << sTableBits

		inputMargin = 8 + 2
	)

	var lTable [maxLTableSize]uint64
	var sTable [maxSTableSize]uint64

	// sLimit is when to stop looking for offset/length copies. The inputMargin
	// lets us use a fast path for emitLiteral in the main loop, while we are
	// looking for copies.
	sLimit := len(src) - inputMargin
	if len(src) < minNonLiteralBlockSize {
		return 0
	}

	// Bail if we can't compress to at least this.
	dstLimit := len(src) - 5

	// nextEmit is where in src the next emitLiteral should start from.
	nextEmit := 0

	// The encoded form must start with a literal, as there are no previous
	// bytes to copy, so we start looking for hash matches at s == 1.
	s := 1
	cv := load64(src, s)

	// We search for a repeat at -1, but don't output repeats when nextEmit == 0
	repeat := 1
	const lowbitMask = 0xffffffff
	getCur := func(x uint64) int {
		return int(x & lowbitMask)
	}
	getPrev := func(x uint64) int {
		return int(x >> 32)
	}

	for {
		type match struct {
			offset int
			s      int
			length int
			rep    bool
		}
		var best match
		for {
			// Next src position to check
			nextS := s + (s-nextEmit)>>8 + 1
			if nextS > sLimit {
				goto emitRemainder
			}
			hashL := hash8(cv, lTableBits)
			hashS := hash4(cv, sTableBits)
			candidateL := lTable[hashL]
			candidateS := sTable[hashS]

			matchAt := func(offset, s int, first uint32, rep bool) match {
				if best.length != 0 && best.s-best.offset == s-offset {
					// Don't retest if we have the same offset.
					return match{offset: offset, s: s}
				}
				if load32(src, offset) != first {
					return match{offset: offset, s: s}
				}
				m := match{offset: offset, s: s, length: 4, rep: rep}
				s += 4
				for s <= sLimit {
					if diff := load64(src, s) ^ load64(src, offset+m.length); diff != 0 {
						m.length += bits.TrailingZeros64(diff) >> 3
						break
					}
					s += 8
					m.length += 8
				}
				return m
			}

			bestOf := func(a, b match) match {
				aScore := b.s - a.s + a.length
				bScore := a.s - b.s + b.length
				if !a.rep {
					// Estimate bytes needed to store offset.
					offset := a.s - a.offset
					if offset >= 65536 {
						aScore -= 5
					} else {
						aScore -= 3
					}
				}
				if !b.rep {
					// Estimate bytes needed to store offset.
					offset := b.s - b.offset
					if offset >= 65536 {
						bScore -= 5
					} else {
						bScore -= 3
					}
				}
				if aScore >= bScore {
					return a
				}
				return b
			}

			best = bestOf(matchAt(getCur(candidateL), s, uint32(cv), false), matchAt(getPrev(candidateL), s, uint32(cv), false))
			best = bestOf(best, matchAt(getCur(candidateS), s, uint32(cv), false))
			best = bestOf(best, matchAt(getPrev(candidateS), s, uint32(cv), false))

			{
				best = bestOf(best, matchAt(s-repeat+1, s+1, uint32(cv>>8), true))
				if best.length > 0 {
					// s+1
					nextShort := sTable[hash4(cv>>8, sTableBits)]
					s := s + 1
					cv := load64(src, s)
					nextLong := lTable[hash8(cv, lTableBits)]
					best = bestOf(best, matchAt(getCur(nextShort), s, uint32(cv), false))
					best = bestOf(best, matchAt(getPrev(nextShort), s, uint32(cv), false))
					best = bestOf(best, matchAt(getCur(nextLong), s, uint32(cv), false))
					best = bestOf(best, matchAt(getPrev(nextLong), s, uint32(cv), false))
					// Repeat at + 2
					best = bestOf(best, matchAt(s-repeat+1, s+1, uint32(cv>>8), true))

					// s+2
					if true {
						nextShort = sTable[hash4(cv>>8, sTableBits)]
						s++
						cv = load64(src, s)
						nextLong = lTable[hash8(cv, lTableBits)]
						best = bestOf(best, matchAt(getCur(nextShort), s, uint32(cv), false))
						best = bestOf(best, matchAt(getPrev(nextShort), s, uint32(cv), false))
						best = bestOf(best, matchAt(getCur(nextLong), s, uint32(cv), false))
						best = bestOf(best, matchAt(getPrev(nextLong), s, uint32(cv), false))
					}
				}
			}

			// Update table
			lTable[hashL] = uint64(s) | candidateL<<32
			sTable[hashS] = uint64(s) | candidateS<<32

			if best.length > 0 {
				break
			}

			cv = load64(src, nextS)
			s = nextS
		}

		// Extend backwards, not needed for repeats...
		s = best.s
		if !best.rep {
			for best.offset > 0 && s > nextEmit && src[best.offset-1] == src[s-1] {
				best.offset--
				best.length++
				s--
			}
		}
		if false && best.offset >= s {
			panic(fmt.Errorf("t %d >= s %d", best.offset, s))
		}
		// Bail if we exceed the maximum size.
		if d+(s-nextEmit) > dstLimit {
			return 0
		}

		base := s
		offset := s - best.offset

		s += best.length

		if offset > 65535 && s-base <= 5 && !best.rep {
			// Bail if the match is equal or worse to the encoding.
			s = best.s + 1
			if s >= sLimit {
				goto emitRemainder
			}
			cv = load64(src, s)
			continue
		}
		d += emitLiteral(dst[d:], src[nextEmit:base])
		if best.rep {
			if nextEmit > 0 {
				// same as `add := emitCopy(dst[d:], repeat, s-base)` but skips storing offset.
				d += emitRepeat(dst[d:], offset, best.length)
			} else {
				// First match, cannot be repeat.
				d += emitCopy(dst[d:], offset, best.length)
			}
		} else {
			d += emitCopy(dst[d:], offset, best.length)
		}
		repeat = offset

		nextEmit = s
		if s >= sLimit {
			goto emitRemainder
		}

		if d > dstLimit {
			// Do we have space for more, if not bail.
			return 0
		}
		// Fill tables...
		for i := best.s + 1; i < s; i++ {
			cv0 := load64(src, i)
			long0 := hash8(cv0, lTableBits)
			short0 := hash4(cv0, sTableBits)
			lTable[long0] = uint64(i) | lTable[long0]<<32
			sTable[short0] = uint64(i) | sTable[short0]<<32
		}
		cv = load64(src, s)
	}

emitRemainder:
	if nextEmit < len(src) {
		// Bail if we exceed the maximum size.
		if d+len(src)-nextEmit > dstLimit {
			return 0
		}
		d += emitLiteral(dst[d:], src[nextEmit:])
	}
	return d
}
