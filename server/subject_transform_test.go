// Copyright 2023 The NATS Authors
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

import (
	"errors"
	"reflect"
	"testing"
)

func TestPlaceHolderIndex(t *testing.T) {
	testString := "$1"
	transformType, indexes, nbPartitions, _, err := indexPlaceHolders(testString)
	var position int32

	if err != nil || transformType != Wildcard || len(indexes) != 1 || indexes[0] != 1 || nbPartitions != -1 {
		t.Fatalf("Error parsing %s", testString)
	}

	testString = "{{partition(10,1,2,3)}}"

	transformType, indexes, nbPartitions, _, err = indexPlaceHolders(testString)

	if err != nil || transformType != Partition || !reflect.DeepEqual(indexes, []int{1, 2, 3}) || nbPartitions != 10 {
		t.Fatalf("Error parsing %s", testString)
	}

	testString = "{{ Partition (10,1,2,3) }}"

	transformType, indexes, nbPartitions, _, err = indexPlaceHolders(testString)

	if err != nil || transformType != Partition || !reflect.DeepEqual(indexes, []int{1, 2, 3}) || nbPartitions != 10 {
		t.Fatalf("Error parsing %s", testString)
	}

	testString = "{{wildcard(2)}}"
	transformType, indexes, nbPartitions, _, err = indexPlaceHolders(testString)

	if err != nil || transformType != Wildcard || len(indexes) != 1 || indexes[0] != 2 || nbPartitions != -1 {
		t.Fatalf("Error parsing %s", testString)
	}

	testString = "{{SplitFromLeft(2,1)}}"
	transformType, indexes, position, _, err = indexPlaceHolders(testString)

	if err != nil || transformType != SplitFromLeft || len(indexes) != 1 || indexes[0] != 2 || position != 1 {
		t.Fatalf("Error parsing %s", testString)
	}

	testString = "{{SplitFromRight(3,2)}}"
	transformType, indexes, position, _, err = indexPlaceHolders(testString)

	if err != nil || transformType != SplitFromRight || len(indexes) != 1 || indexes[0] != 3 || position != 2 {
		t.Fatalf("Error parsing %s", testString)
	}

	testString = "{{SliceFromLeft(2,2)}}"
	transformType, indexes, sliceSize, _, err := indexPlaceHolders(testString)

	if err != nil || transformType != SliceFromLeft || len(indexes) != 1 || indexes[0] != 2 || sliceSize != 2 {
		t.Fatalf("Error parsing %s", testString)
	}
}

func TestSubjectTransforms(t *testing.T) {
	shouldErr := func(src, dest string) {
		t.Helper()
		if _, err := NewSubjectTransform(src, dest); err != ErrBadSubject && !errors.Is(err, ErrInvalidMappingDestination) {
			t.Fatalf("Did not get an error for src=%q and dest=%q", src, dest)
		}
	}

	shouldErr("foo.*.*", "bar.$2") // Must place all pwcs.

	// Must be valid subjects.
	shouldErr("foo", "")
	shouldErr("foo..", "bar")

	// Wildcards are allowed in src, but must be matched by token placements on the other side.
	// e.g. foo.* -> bar.$1.
	// Need to have as many pwcs as placements on other side.
	shouldErr("foo.*", "bar.*")
	shouldErr("foo.*", "bar.$2")                   // Bad pwc token identifier
	shouldErr("foo.*", "bar.$1.>")                 // fwcs have to match.
	shouldErr("foo.>", "bar.baz")                  // fwcs have to match.
	shouldErr("foo.*.*", "bar.$2")                 // Must place all pwcs.
	shouldErr("foo.*", "foo.$foo")                 // invalid $ value
	shouldErr("foo.*", "foo.{{wildcard(2)}}")      // Mapping function being passed an out of range wildcard index
	shouldErr("foo.*", "foo.{{unimplemented(1)}}") // Mapping trying to use an unknown mapping function
	shouldErr("foo.*", "foo.{{partition(10)}}")    // Not enough arguments passed to the mapping function
	shouldErr("foo.*", "foo.{{wildcard(foo)}}")    // Invalid argument passed to the mapping function
	shouldErr("foo.*", "foo.{{wildcard()}}")       // Not enough arguments passed to the mapping function
	shouldErr("foo.*", "foo.{{wildcard(1,2)}}")    // Too many arguments passed to the mapping function
	shouldErr("foo.*", "foo.{{ wildcard5) }}")     // Bad mapping function
	shouldErr("foo.*", "foo.{{splitLeft(2,2}}")    // arg out of range

	shouldBeOK := func(src, dest string) *subjectTransform {
		t.Helper()
		tr, err := NewSubjectTransform(src, dest)
		if err != nil {
			t.Fatalf("Got an error %v for src=%q and dest=%q", err, src, dest)
		}
		return tr
	}

	shouldBeOK("foo", "bar")
	shouldBeOK("foo.*.bar.*.baz", "req.$2.$1")
	shouldBeOK("baz.>", "mybaz.>")
	shouldBeOK("*", "{{splitfromleft(1,1)}}")
	shouldBeOK("", "prefix.>")
	shouldBeOK("*.*", "{{partition(10,1,2)}}")
	shouldBeOK("foo.*.*", "foo.{{wildcard(1)}}.{{wildcard(2)}}.{{partition(5,1,2)}}")

	shouldMatch := func(src, dest, sample, expected string) {
		t.Helper()
		tr := shouldBeOK(src, dest)
		s, err := tr.Match(sample)
		if err != nil {
			t.Fatalf("Got an error %v when expecting a match for %q to %q", err, sample, expected)
		}
		if s != expected {
			t.Fatalf("Dest does not match what was expected. Got %q, expected %q", s, expected)
		}
	}

	shouldMatch("", "prefix.>", "foo", "prefix.foo")
	shouldMatch("foo", "bar", "foo", "bar")
	shouldMatch("foo.*.bar.*.baz", "req.$2.$1", "foo.A.bar.B.baz", "req.B.A")
	shouldMatch("baz.>", "my.pre.>", "baz.1.2.3", "my.pre.1.2.3")
	shouldMatch("baz.>", "foo.bar.>", "baz.1.2.3", "foo.bar.1.2.3")
	shouldMatch("*", "foo.bar.$1", "foo", "foo.bar.foo")
	shouldMatch("*", "{{splitfromleft(1,3)}}", "12345", "123.45")
	shouldMatch("*", "{{SplitFromRight(1,3)}}", "12345", "12.345")
	shouldMatch("*", "{{SliceFromLeft(1,3)}}", "1234567890", "123.456.789.0")
	shouldMatch("*", "{{SliceFromRight(1,3)}}", "1234567890", "1.234.567.890")
	shouldMatch("*", "{{split(1,-)}}", "-abc-def--ghi-", "abc.def.ghi")
	shouldMatch("*", "{{split(1,-)}}", "abc-def--ghi-", "abc.def.ghi")
	shouldMatch("*.*", "{{split(2,-)}}.{{splitfromleft(1,2)}}", "foo.-abc-def--ghij-", "abc.def.ghij.fo.o") // combo + checks split for multiple instance of deliminator and deliminator being at the start or end
}
