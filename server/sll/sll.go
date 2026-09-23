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

// Package sll provides a generic singly linked list.
package sll

// List is a singly linked list of comparable values. The zero value is ready
// to use. A List is not safe for concurrent use and must not be copied while
// non-empty.
type List[T comparable] struct {
	head *node[T]
	tail *node[T]
	len  int
}

type node[T comparable] struct {
	value T
	next  *node[T]
}

// Len returns the number of values in the list.
func (l *List[T]) Len() int {
	return l.len
}

// PushBack appends a value in O(1) time.
func (l *List[T]) PushBack(value T) {
	n := &node[T]{value: value}
	if l.tail == nil {
		l.head = n
	} else {
		l.tail.next = n
	}
	l.tail = n
	l.len++
}

// PopFront removes and returns the first value in O(1) time.
// It returns the zero value and false if the list is empty.
func (l *List[T]) PopFront() (T, bool) {
	if l.head == nil {
		var zero T
		return zero, false
	}
	n := l.head
	l.head = n.next
	l.len--
	if l.head == nil {
		l.tail = nil
	}
	return n.value, true
}

// Remove removes the first occurrence of value, returning whether it was found.
// Finding the value takes O(n) time; unlinking it takes O(1).
func (l *List[T]) Remove(value T) bool {
	var prev *node[T]
	for n := l.head; n != nil; n = n.next {
		if n.value == value {
			if prev == nil {
				l.head = n.next
			} else {
				prev.next = n.next
			}
			if l.tail == n {
				l.tail = prev
			}
			l.len--
			return true
		}
		prev = n
	}
	return false
}

// Range visits values in list order until f returns false.
// The callback must not modify the list.
func (l *List[T]) Range(f func(T) bool) {
	for n := l.head; n != nil; n = n.next {
		if !f(n.value) {
			return
		}
	}
}

// Empty releases all nodes and resets the list for reuse in O(1) time.
func (l *List[T]) Empty() {
	l.head, l.tail, l.len = nil, nil, 0
}
