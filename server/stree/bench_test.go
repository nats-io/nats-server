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

package stree

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkSubjectTreeMatch(b *testing.B) {
	st := NewSubjectTree[struct{}]()

	for a := 0; a < b.N; a++ {
		for aa := 0; aa < b.N; aa++ {
			subj := fmt.Sprintf("subj.%d.%d", a, aa)
			st.Insert([]byte(subj), struct{}{})
		}
	}

	b.Logf("Running with %d subjects", b.N*b.N)
	b.Logf("---")

	for _, f := range [][]byte{
		[]byte(">"),
		[]byte("subj.>"),
		[]byte("subj.*.*"),
		[]byte("*.*.*"),
		[]byte("subj.1.*"),
		[]byte("subj.1.>"),
		[]byte("subj.*.1"),
		[]byte("*.*.1"),
	} {
		start := time.Now()
		count := 0
		st.Match(f, func(subject []byte, val *struct{}) {
			count++
		})
		b.Logf("Match %q took %s and matched %d entries", f, time.Since(start), count)
	}
	b.Logf("---")
}
