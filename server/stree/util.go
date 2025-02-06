// Copyright 2023-2025 The NATS Authors
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

package stree

// For subject matching.
const (
	pwc  = '*'
	fwc  = '>'
	tsep = '.'
)

// Determine index of common prefix. No match at all is 0, etc.
func commonPrefixLen(s1, s2 []byte) int {
	limit := min(len(s1), len(s2))
	var i int
	for ; i < limit; i++ {
		if s1[i] != s2[i] {
			break
		}
	}
	return i
}

// Helper to copy bytes.
func copyBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

type position interface{ int | uint16 }

// No pivot available.
const noPivot = byte(127)

// Can return 127 (DEL) if we have all the subject as prefixes.
// We used to use 0, but when that was in the subject would cause infinite recursion in some situations.
func pivot[N position](subject []byte, pos N) byte {
	if int(pos) >= len(subject) {
		return noPivot
	}
	return subject[pos]
}
