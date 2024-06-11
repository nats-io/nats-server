// Copyright 2023-2024 The NATS Authors
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

package stree

import (
	"bytes"
)

// Leaf node
// Order of struct fields for best memory alignment (as per govet/fieldalignment)
type leaf[T any] struct {
	value T
	// This could be the whole subject, but most likely just the suffix portion.
	// We will only store the suffix here and assume all prior prefix paths have
	// been checked once we arrive at this leafnode.
	suffix []byte
}

func newLeaf[T any](suffix []byte, value T) *leaf[T] {
	return &leaf[T]{value, copyBytes(suffix)}
}

func (n *leaf[T]) isLeaf() bool                               { return true }
func (n *leaf[T]) base() *meta                                { return nil }
func (n *leaf[T]) match(subject []byte) bool                  { return bytes.Equal(subject, n.suffix) }
func (n *leaf[T]) setSuffix(suffix []byte)                    { n.suffix = copyBytes(suffix) }
func (n *leaf[T]) isFull() bool                               { return true }
func (n *leaf[T]) matchParts(parts [][]byte) ([][]byte, bool) { return matchParts(parts, n.suffix) }
func (n *leaf[T]) iter(f func(node) bool)                     {}
func (n *leaf[T]) children() []node                           { return nil }
func (n *leaf[T]) numChildren() uint16                        { return 0 }
func (n *leaf[T]) path() []byte                               { return n.suffix }

// Not applicable to leafs and should not be called, so panic if we do.
func (n *leaf[T]) setPrefix(pre []byte)    { panic("setPrefix called on leaf") }
func (n *leaf[T]) addChild(_ byte, _ node) { panic("addChild called on leaf") }
func (n *leaf[T]) findChild(_ byte) *node  { panic("findChild called on leaf") }
func (n *leaf[T]) grow() node              { panic("grow called on leaf") }
func (n *leaf[T]) deleteChild(_ byte)      { panic("deleteChild called on leaf") }
func (n *leaf[T]) shrink() node            { panic("shrink called on leaf") }
