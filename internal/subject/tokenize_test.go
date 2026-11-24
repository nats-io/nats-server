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

package subject

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

func require_Tokenization(t testing.TB, subject string) {
	t.Helper()
	var buf [32]string
	want := strings.Split(subject, ".")
	if got := Tokenize(buf[:0], subject); !slices.Equal(got, want) {
		t.Fatalf("Tokenize(%q) = %q, want %q", subject, got, want)
	}
	got := TokenizeNoEmpty(buf[:0], subject)
	if slices.Contains(want, "") {
		if got != nil {
			t.Fatalf("TokenizeNoEmpty(%q) = %q, want nil", subject, got)
		}
		return
	}
	if !slices.Equal(got, want) {
		t.Fatalf("TokenizeNoEmpty(%q) = %q, want %q", subject, got, want)
	}
}

func TestTokenizeBoundaries(t *testing.T) {
	for _, subject := range []string{
		"", ".abcdefg", "abcdefg", "abcdefgh", "abcdefghi",
		"abcdefg.", "abcdefg.h", "abcdefgh.i", "abcdefg..i",
		"abcdefg.hijklmn.", "abcdefg.hijklmn.o", ".\xae../.\x00.",
		strings.Repeat("a.", 64) + "b", // 65 tokens exceed the 32-token buffer.
	} {
		t.Run(subject, func(t *testing.T) {
			require_Tokenization(t, subject)
		})
	}
}

func TestTokenizeBytePairs(t *testing.T) {
	suffix := strings.Repeat("b", 16)
	for offset := range 8 {
		prefix := strings.Repeat("a", offset+1)
		for value := range 256 {
			neighbor := string([]byte{byte(value)})
			require_Tokenization(t, prefix+"."+neighbor+suffix)
			require_Tokenization(t, prefix+neighbor+"."+suffix)
		}
	}
}

func FuzzTokenize(f *testing.F) {
	for _, subject := range []string{"", "a", "a.b", ".a", "a.", "a..b", "*", "foo.>", strings.Repeat("a.", 64) + "b", strings.Repeat("a", 129), "a.\x00.\xff"} {
		f.Add(subject)
	}
	f.Fuzz(func(t *testing.T, subject string) {
		require_Tokenization(t, subject)
	})
}

func BenchmarkTokenizeNoEmpty(b *testing.B) {
	for _, count := range []int{1, 2, 4, 8, 16, 32, 64} {
		for _, length := range []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 32, 64} {
			b.Run(fmt.Sprintf("tokens=%d/bytes=%d", count, length), func(b *testing.B) {
				token := strings.Repeat("a", length)
				subject := strings.Repeat(token+".", count-1) + token
				b.ReportAllocs()
				for b.Loop() {
					var buf [32]string
					TokenizeNoEmpty(buf[:0], subject)
				}
			})
		}
	}
}

func BenchmarkTokenizeNoEmptyMixedLengths(b *testing.B) {
	pattern := [...]string{"a", strings.Repeat("a", 36)}
	for _, count := range []int{3, 4, 16, 32, 64} {
		b.Run(fmt.Sprintf("alternating/tokens=%d", count), func(b *testing.B) {
			tokens := make([]string, count)
			for i := range tokens {
				tokens[i] = pattern[i%len(pattern)]
			}
			subject := strings.Join(tokens, ".")
			b.ReportAllocs()
			for b.Loop() {
				var buf [32]string
				TokenizeNoEmpty(buf[:0], subject)
			}
		})
	}
}
