// Copyright 2019-2023 The NATS Authors
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
	"errors"
	"strings"
)

var (
	ErrInvalid = errors.New("invalid subject")
)

// Common byte variables for wildcards and token separator.
const (
	_EMPTY_ = ""

	pwc   = '*'
	pwcs  = "*"
	fwc   = '>'
	fwcs  = ">"
	tsep  = "."
	btsep = '.'
)

type Subject struct {
	subject   string
	isLiteral bool
	tokenized []string
}

var emptySubject = Subject{
	subject:   _EMPTY_,
	isLiteral: true,
}

func Empty() *Subject {
	return &emptySubject
}

func New(subject string) (*Subject, error) {
	if !isValidSubject(subject) {
		return nil, ErrInvalid
	}
	s := &Subject{
		subject:   subject,
		isLiteral: isLiteralSubject(subject),
		tokenized: tokenizeSubjectIntoSlice(nil, subject),
	}
	return s, nil
}

func Stack(subject string) (Subject, error) {
	if !isValidSubject(subject) {
		return Subject{}, ErrInvalid
	}
	tsa := [32]string{}
	s := Subject{
		subject:   subject,
		isLiteral: isLiteralSubject(subject),
		tokenized: tokenizeSubjectIntoSlice(tsa[:0], subject),
	}
	return s, nil
}

func (s *Subject) String() string {
	return s.subject
}

func (s *Subject) IsEmpty() bool {
	return s.subject == _EMPTY_
}

func (s *Subject) IsForwardWildcard() bool {
	return s.subject == fwcs
}

func (s *Subject) IsLiteral() bool {
	return s.isLiteral
}

func (s *Subject) HasWildcard() bool {
	return !s.isLiteral
}

func (s *Subject) IsSubsetMatch(other *Subject) bool {
	return isSubsetMatchTokenized(s.tokenized, other.tokenized)
}

func (s *Subject) EqualsLiteral(other string) bool {
	return s.isLiteral && s.subject == other
}

func isLiteralSubject(subject string) bool {
	for i, c := range subject {
		if c == pwc || c == fwc {
			if (i == 0 || subject[i-1] == btsep) &&
				(i+1 == len(subject) || subject[i+1] == btsep) {
				return false
			}
		}
	}
	return true
}

func isValidSubject(subject string) bool {
	if subject == _EMPTY_ {
		return false
	}
	sfwc := false
	tokens := strings.Split(subject, tsep)
	for _, t := range tokens {
		length := len(t)
		if length == 0 || sfwc {
			return false
		}
		if length > 1 {
			if strings.ContainsAny(t, "\t\n\f\r ") {
				return false
			}
			continue
		}
		switch t[0] {
		case fwc:
			sfwc = true
		case ' ', '\t', '\n', '\r', '\f':
			return false
		}
	}
	return true
}

// use similar to append. meaning, the updated slice will be returned
func tokenizeSubjectIntoSlice(tts []string, subject string) []string {
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tts = append(tts, subject[start:i])
			start = i + 1
		}
	}
	tts = append(tts, subject[start:])
	return tts
}

// This will test a subject as an array of tokens against a test subject (also encoded as array of tokens)
// and determine if the tokens are matched. Both test subject and tokens
// may contain wildcards. So foo.* is a subset match of [">", "*.*", "foo.*"],
// but not of foo.bar, etc.
func isSubsetMatchTokenized(tokens, test []string) bool {
	// Walk the target tokens
	for i, t2 := range test {
		if i >= len(tokens) {
			return false
		}
		l := len(t2)
		if l == 0 {
			return false
		}
		if t2[0] == fwc && l == 1 {
			return true
		}
		t1 := tokens[i]

		l = len(t1)
		if l == 0 || t1[0] == fwc && l == 1 {
			return false
		}

		if t1[0] == pwc && len(t1) == 1 {
			m := t2[0] == pwc && len(t2) == 1
			if !m {
				return false
			}
			if i >= len(test) {
				return true
			}
			continue
		}
		if t2[0] != pwc && strings.Compare(t1, t2) != 0 {
			return false
		}
	}
	return len(tokens) == len(test)
}
