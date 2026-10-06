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

package elastic

import (
	"weak"
)

func Make[T any](ptr *T) *Pointer[T] {
	return &Pointer[T]{
		weak: weak.Make(ptr),
	}
}

type Pointer[T any] struct {
	weak   weak.Pointer[T]
	strong *T
}

func (e *Pointer[T]) Set(ptr *T) {
	e.weak = weak.Make(ptr)
	if e.strong != nil {
		e.strong = ptr
	}
}

func (e *Pointer[T]) Strengthen() {
	if e == nil || e.strong != nil {
		return
	}
	e.strong = e.weak.Value()
}

func (e *Pointer[T]) Weaken() {
	if e == nil || e.strong == nil {
		return
	}
	e.strong = nil
}

func (e *Pointer[T]) Value() *T {
	if e == nil {
		return nil
	}
	if e.strong != nil {
		return e.strong
	}
	return e.weak.Value()
}
