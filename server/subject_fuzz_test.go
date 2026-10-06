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

import "testing"

// FuzzSubjectsCollide performs fuzz testing on the NATS subject collision detection logic.
// It verifies the behavior of the SubjectsCollide function which determines if two NATS
// subjects/subscriptions could potentially overlap in the NATS pub-sub system.
func FuzzSubjectsCollide(f *testing.F) {
	corpuses := []struct {
		s1 string
		s2 string
	}{
		// SubjectsCollide true
		{s1: "", s2: ""},
		{s1: "a", s2: "a"},
		{s1: "a.b.c", s2: "a.b.c"},
		{s1: "$JS.b.c", s2: "$JS.b.c"},
		{s1: "a.b.c", s2: "a.*.c"},
		{s1: "a.b.*", s2: "a.*.c"},
		{s1: "aaa.bbb.ccc", s2: "aaa.bbb.ccc"},
		{s1: "aaa.*.ccc", s2: "*.bbb.ccc"},
		{s1: "*", s2: "*"},
		{s1: "**", s2: "*"},
		{s1: "", s2: ">"},
		{s1: ">", s2: ">"},
		{s1: ">>", s2: ">"},
		{s1: "a", s2: ">"},
		{s1: "a.b.c", s2: ">"},
		{s1: "a.b.c.>", s2: "a.b.>"},
		{s1: "a.b.c.d.*", s2: "a.b.c.*.e"},
		{s1: "a.*.*.d.>", s2: "a.bbb.ccc.*.e"},

		// SubjectsCollide false
		{s1: "a", s2: ""},
		{s1: "a.b", s2: "b.a"},
		{s1: "a.bbbbb.*.d", s2: "a.b.>"},
		{s1: "a.b", s2: "a.b.c"},
		{s1: "a.b.c", s2: "a.b"},
		{s1: "a.b", s2: ""},
		{s1: "a.*.*.d.e.>", s2: "a.bbb.ccc.*.e"},
	}

	for _, crp := range corpuses {
		f.Add(crp.s1, crp.s2)
	}

	f.Fuzz(func(t *testing.T, s1, s2 string) {
		if !IsValidSubject(s1) {
			return
		}

		if !IsValidSubject(s2) {
			return
		}

		SubjectsCollide(s1, s2)
	})
}
