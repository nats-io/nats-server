// Copyright 2023-2024 The NATS Authors
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
	return min(i, maxPrefixLen)
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

// Can return 0 if we have all the subject as prefixes.
func pivot[N position](subject []byte, pos N) byte {
	if int(pos) >= len(subject) {
		return 0
	}
	return subject[pos]
}

// TODO(dlc) - Can be removed with Go 1.21 once server is on Go 1.22.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
