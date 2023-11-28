// Copyright 2016-2023 The NATS Authors
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
	"testing"
)

func checkBool(b, expected bool, t *testing.T) {
	t.Helper()
	if b != expected {
		t.Fatalf("Expected %v, but got %v\n", expected, b)
	}
}

func TestIsValidSubject(t *testing.T) {
	t.Parallel()
	checkBool(isValidSubject(""), false, t)
	checkBool(isValidSubject("."), false, t)
	checkBool(isValidSubject(".foo"), false, t)
	checkBool(isValidSubject("foo."), false, t)
	checkBool(isValidSubject("foo..bar"), false, t)
	checkBool(isValidSubject(">.bar"), false, t)
	checkBool(isValidSubject("foo.>.bar"), false, t)
	checkBool(isValidSubject("foo"), true, t)
	checkBool(isValidSubject("foo.bar.>"), true, t)
	checkBool(isValidSubject("*"), true, t)
	checkBool(isValidSubject(">"), true, t)
	checkBool(isValidSubject("foo*"), true, t)
	checkBool(isValidSubject("foo**"), true, t)
	checkBool(isValidSubject("foo.**"), true, t)
	checkBool(isValidSubject("foo*bar"), true, t)
	checkBool(isValidSubject("foo.*bar"), true, t)
	checkBool(isValidSubject("foo*.bar"), true, t)
	checkBool(isValidSubject("*bar"), true, t)
	checkBool(isValidSubject("foo>"), true, t)
	checkBool(isValidSubject("foo.>>"), true, t)
	checkBool(isValidSubject("foo>bar"), true, t)
	checkBool(isValidSubject("foo.>bar"), true, t)
	checkBool(isValidSubject("foo>.bar"), true, t)
	checkBool(isValidSubject(">bar"), true, t)
}

func TestIsLiteralSubject(t *testing.T) {
	t.Parallel()
	checkBool(isLiteralSubject("foo"), true, t)
	checkBool(isLiteralSubject("foo.bar"), true, t)
	checkBool(isLiteralSubject("foo*.bar"), true, t)
	checkBool(isLiteralSubject("*"), false, t)
	checkBool(isLiteralSubject(">"), false, t)
	checkBool(isLiteralSubject("foo.*"), false, t)
	checkBool(isLiteralSubject("foo.>"), false, t)
	checkBool(isLiteralSubject("foo.*.>"), false, t)
	checkBool(isLiteralSubject("foo.*.bar"), false, t)
	checkBool(isLiteralSubject("foo.bar.>"), false, t)
}

func TestSubjectNew(t *testing.T) {
	for _, test := range []struct {
		subject        string
		expectsSuccess bool
	}{
		{"", false},
		{"foo.bar", true},
		{"foo..bar", false},
		{"foo.*.bar", true},
	} {
		test := test
		name := fmt.Sprintf("subject.New(\"%s\")", test.subject)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, err := New(test.subject)
			if test.expectsSuccess {
				if err != nil {
					t.Fatalf("subject.New(\"%s\" failed: %v", test.subject, err)
				}
				if s == nil {
					t.Fatalf("subject.New(\"%s\" returned nil", test.subject)
				}
				checkBool(s.IsEmpty(), false, t)
			} else {
				if s != nil || err == nil {
					t.Fatalf("subject.New(\"%s\" should have failed", test.subject)
				}
			}
		})
	}
}

func TestSubjectIsSubsetMatch(t *testing.T) {
	for _, test := range []struct {
		subject string
		other   string
		result  bool
	}{
		{"foo.bar", "foo.bar", true},
		{"foo.*", ">", true},
		{"foo.*", "*.*", true},
		{"foo.*", "foo.*", true},
		{"foo.*", "foo.>", true},
		{"foo.*", "foo.bar", false},
		{"foo.>", ">", true},
		{"foo.>", "*.>", true},
		{"foo.>", "foo.>", true},
		{"foo.>", "foo.bar", false},
		{"foo.*.bar", "foo.>", true},
		{"foo.*.bar", "foo.bar.>", false},
		{"foo.bar.>", "foo.*.bar", false},
	} {
		test := test
		var name string
		if test.result {
			name = fmt.Sprintf("\"%s\" is a subset match of \"%s\")", test.other, test.subject)
		} else {
			name = fmt.Sprintf("\"%s\" is not a subset match of \"%s\")", test.other, test.subject)
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, err := New(test.subject)
			if err != nil {
				t.Fatalf("subject.New(\"%s\" failed: %v", test.subject, err)
			}
			o, err := New(test.other)
			if err != nil {
				t.Fatalf("subject.New(\"%s\" failed: %v", test.other, err)
			}
			if res := s.IsSubsetMatch(o); res != test.result {
				t.Fatalf("Subject %q is subset match of %q, should be %v, got %v",
					test.other, test.subject, test.result, res)
			}
		})
	}
}

func TestSubjectIsLiteral(t *testing.T) {
	for _, test := range []struct {
		subject string
		result  bool
	}{
		{"foo", true},
		{"foo.bar.*", false},
		{"foo.bar.>", false},
		{"*", false},
		{">", false},
		// The followings have widlcards characters but are not
		// considered as such because they are not individual tokens.
		{"foo*", true},
		{"foo**", true},
		{"foo.**", true},
		{"foo*bar", true},
		{"foo.*bar", true},
		{"foo*.bar", true},
		{"*bar", true},
		{"foo>", true},
		{"foo>>", true},
		{"foo.>>", true},
		{"foo>bar", true},
		{"foo.>bar", true},
		{"foo>.bar", true},
		{">bar", true},
	} {
		test := test
		name := fmt.Sprintf("Literal - \"%s\"", test.subject)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, err := New(test.subject)
			if err != nil {
				t.Fatalf("subject.New(\"%s\" failed: %v", test.subject, err)
			}
			if res := s.IsLiteral(); res != test.result {
				t.Fatalf("IsLiteral() for subject \"%s\", should be %v, got %v", test.subject, test.result, res)
			}
			if res := s.HasWildcard(); res != !test.result {
				t.Fatalf("HasWildcard() for subject \"%s\", should be %v, got %v", test.subject, !test.result, res)
			}
		})
	}
}

func TestEmptySubject(t *testing.T) {
	t.Parallel()
	e := Empty()
	checkBool(e.IsEmpty(), true, t)
	checkBool(e.IsLiteral(), true, t)
	checkBool(e.HasWildcard(), false, t)
	s, _ := New("foo.bar")
	checkBool(s.IsEmpty(), false, t)
	checkBool(e.IsSubsetMatch(s), false, t)
	checkBool(s.IsSubsetMatch(e), false, t)
}

func TestStackSubject(t *testing.T) {
	t.Run("Stack - empty", func(t *testing.T) {
		t.Parallel()
		if _, err := Stack(""); err == nil {
			t.Fatal("Expected Stack(\"\") to fail")
		}
	})
	t.Run("Stack - literal", func(t *testing.T) {
		t.Parallel()
		s, err := Stack("foo.bar")
		if err != nil {
			t.Fatalf("Expected no err, got %v", err)
		}
		checkBool(s.IsEmpty(), false, t)
		checkBool(s.IsLiteral(), true, t)
		checkBool(s.HasWildcard(), false, t)
	})
	t.Run("Stack - wildcard", func(t *testing.T) {
		t.Parallel()
		s, err := Stack("foo.*.bar")
		if err != nil {
			t.Fatalf("Expected no err, got %v", err)
		}
		checkBool(s.IsEmpty(), false, t)
		checkBool(s.IsLiteral(), false, t)
		checkBool(s.HasWildcard(), true, t)
		o, err := New("foo.>")
		if err != nil {
			t.Fatalf("Expected no err, got %v", err)
		}
		checkBool(s.IsSubsetMatch(o), true, t)
	})
}
