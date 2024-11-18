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

package thw

import (
	"strings"
	"testing"
)

func require_NoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("require no error, but got: %v", err)
	}
}

func require_Error(t *testing.T, err error, expected ...error) {
	t.Helper()
	if err == nil {
		t.Fatalf("require error, but got none")
	}
	if len(expected) == 0 {
		return
	}
	eStr := err.Error()
	for _, e := range expected {
		if err == e || strings.Contains(eStr, e.Error()) || strings.Contains(e.Error(), eStr) {
			return
		}
	}
	t.Fatalf("Expected one of %v, got '%v'", expected, err)
}

func require_True(t *testing.T, b bool) {
	t.Helper()
	if !b {
		t.Fatalf("require true, but got false")
	}
}

func require_Equal[T comparable](t *testing.T, a, b T) {
	t.Helper()
	if a != b {
		t.Fatalf("require %T equal, but got: %v != %v", a, a, b)
	}
}
