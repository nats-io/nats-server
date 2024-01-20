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

// Node with 256 children
type node256 struct {
	meta
	child [256]node
}

func newNode256(prefix []byte) *node256 {
	nn := &node256{}
	nn.setPrefix(prefix)
	return nn
}

func (n *node256) isLeaf() bool { return false }
func (n *node256) base() *meta  { return &n.meta }

func (n *node256) setPrefix(pre []byte) {
	n.prefixLen = uint16(min(len(pre), maxPrefixLen))
	for i := uint16(0); i < n.prefixLen; i++ {
		n.prefix[i] = pre[i]
	}
}

func (n *node256) addChild(c byte, nn node) {
	n.child[c] = nn
	n.size++
}

func (n *node256) numChildren() uint16 { return n.size }
func (n *node256) path() []byte        { return n.prefix[:n.prefixLen] }

func (n *node256) findChild(c byte) *node {
	if n.child[c] != nil {
		return &n.child[c]
	}
	return nil
}

func (n *node256) isFull() bool { return false }
func (n *node256) grow() node   { panic("grow can not be called on node256") }

// Deletes a child from the node.
func (n *node256) deleteChild(c byte) {
	if n.child[c] != nil {
		n.child[c] = nil
		n.size--
	}
}

// Shrink if needed and return new node, otherwise return nil.
func (n *node256) shrink() node {
	if n.size > 16 {
		return nil
	}
	nn := newNode16(nil)
	for c, child := range n.child {
		if child != nil {
			nn.addChild(byte(c), n.child[c])
		}
	}
	return nn
}

// Will match parts against our prefix.
func (n *node256) matchParts(parts [][]byte) ([][]byte, bool) {
	return matchParts(parts, n.prefix[:n.prefixLen])
}

// Iterate over all children calling func f.
func (n *node256) iter(f func(node) bool) {
	for i := 0; i < 256; i++ {
		if n.child[i] != nil {
			if !f(n.child[i]) {
				return
			}
		}
	}
}

// Return our children as a slice.
func (n *node256) children() []node {
	return n.child[:256]
}
