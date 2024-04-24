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

// Internal node interface.
type node interface {
	isLeaf() bool
	base() *meta
	setPrefix(pre []byte)
	addChild(c byte, n node)
	findChild(c byte) *node
	deleteChild(c byte)
	isFull() bool
	grow() node
	shrink() node
	matchParts(parts [][]byte) ([][]byte, bool)
	kind() string
	iter(f func(node) bool)
	children() []node
	numChildren() uint16
	path() []byte
}

type meta struct {
	prefix []byte
	size   uint16
}

func (n *meta) isLeaf() bool { return false }
func (n *meta) base() *meta  { return n }

func (n *meta) setPrefix(pre []byte) {
	n.prefix = append([]byte(nil), pre...)
}

func (n *meta) numChildren() uint16 { return n.size }
func (n *meta) path() []byte        { return n.prefix }

// Will match parts against our prefix.
func (n *meta) matchParts(parts [][]byte) ([][]byte, bool) {
	return matchParts(parts, n.prefix)
}
