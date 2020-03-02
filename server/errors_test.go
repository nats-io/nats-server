// Copyright 2020 The NATS Authors
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
	"strings"
	"testing"
)

func TestErrCtx(t *testing.T) {
	ctx := "Extra context information"
	e := NewErrorCtx(ErrWrongGateway, ctx)

	if e.Error() != ErrWrongGateway.Error() {
		t.Fatalf("%v and %v are supposed to be identical", e, ErrWrongGateway)
	}
	if e == ErrWrongGateway {
		t.Fatalf("%v and %v can't be compared this way", e, ErrWrongGateway)
	}
	if !ErrorIs(e, ErrWrongGateway) {
		t.Fatalf("%s and %s ", e, ErrWrongGateway)
	}
	if UnpackIfErrorCtx(ErrWrongGateway) != ErrWrongGateway.Error() {
		t.Fatalf("Error of different type should be processed unchanged")
	}
	trace := UnpackIfErrorCtx(e)
	if !strings.HasPrefix(trace, ErrWrongGateway.Error()) {
		t.Fatalf("original error needs to remain")
	}
	if !strings.HasSuffix(trace, ctx) {
		t.Fatalf("ctx needs to be added")
	}
}

func TestErrCtxWrapped(t *testing.T) {
	ctxO := "Original Ctx"
	eO := NewErrorCtx(ErrWrongGateway, ctxO)
	ctx := "Extra context information"
	e := NewErrorCtx(eO, ctx)

	if e.Error() != ErrWrongGateway.Error() {
		t.Fatalf("%v and %v are supposed to be identical", e, ErrWrongGateway)
	}
	if e == ErrWrongGateway {
		t.Fatalf("%v and %v can't be compared this way", e, ErrWrongGateway)
	}
	if !ErrorIs(e, ErrWrongGateway) {
		t.Fatalf("%s and %s ", e, ErrWrongGateway)
	}
	if UnpackIfErrorCtx(ErrWrongGateway) != ErrWrongGateway.Error() {
		t.Fatalf("Error of different type should be processed unchanged")
	}
	trace := UnpackIfErrorCtx(e)
	if !strings.HasPrefix(trace, ErrWrongGateway.Error()) {
		t.Fatalf("original error needs to remain")
	}
	if !strings.HasSuffix(trace, ctx) {
		t.Fatalf("ctx needs to be added")
	}
	if !strings.Contains(trace, ctxO) {
		t.Fatalf("Needs to contain every context")
	}
}
