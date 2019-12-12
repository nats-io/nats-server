// Copyright 2019 The NATS Authors
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

package main

import (
	"testing"
)

func checkErr(t *testing.T, err error, format string, a ...interface{}) {
	if err == nil {
		return
	}

	t.Fatalf(format, a...)
}

func TestSplitString(t *testing.T) {
	for _, s := range []string{"x y", "x	y", "x  y", "x,y", "x, y"} {
		parts := splitString(s)
		if parts[0] != "x" && parts[1] != "y" {
			t.Fatalf("Expected x and y from %s, got %v", s, parts)
		}
	}

	parts := splitString("x foo.*")
	if parts[0] != "x" && parts[1] != "y" {
		t.Fatalf("Expected x and foo.* from 'x foo.*', got %v", parts)
	}
}

func TestParseDurationString(t *testing.T) {
	d, err := parseDurationString("")
	checkErr(t, err, "failed to parse empty duration: %s", err)
	if d.Nanoseconds() != 0 {
		t.Fatalf("expected 0 ns from empty duration, got %v", d)
	}

	_, err = parseDurationString("1f")
	if err.Error() != "invalid time unit f" {
		t.Fatal("expected time unit 'f' to fail but it did not")
	}

	for _, u := range []string{"d", "D"} {
		d, err = parseDurationString("1.1" + u)
		checkErr(t, err, "failed to parse 1.1%s duration: %s", u, err)
		if d.Hours() != 26 {
			t.Fatalf("expected 1 hour from 1.1%s duration, got %v", u, d)
		}
	}

	d, err = parseDurationString("1.1M")
	checkErr(t, err, "failed to parse 1.1M duration: %s", err)
	if d.Hours() != 1.1*24*30 {
		t.Fatalf("expected 30 days from 1.1M duration, got %v", d)
	}

	for _, u := range []string{"y", "Y"} {
		d, err = parseDurationString("1.1" + u)
		checkErr(t, err, "failed to parse 1.1%s duration: %s", u, err)
		if d.Hours() != 1.1*24*365 {
			t.Fatalf("expected 1.1 year from 1.1%s duration, got %v", u, d)
		}
	}

	d, err = parseDurationString("1.1h")
	checkErr(t, err, "failed to parse 1.1h duration: %s", err)
	if d.Minutes() != 66 {
		t.Fatalf("expected 1.1 hour from 1.1h duration, got %v", d)
	}
}
