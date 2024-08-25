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

// Node with 48 children
// Memory saving vs node256 comes from the fact that the child array is 16 bytes
// per `node` entry, so node256's 256*16=4096 vs node48's 256+(48*16)=1024
// Note that `key` is effectively 1-indexed, as 0 means no entry, so offset by 1
// Order of struct fields for best memory alignment (as per govet/fieldalignment)
type node48 struct {
	child [48]node
	meta
	key [256]byte
}

func newNode48(prefix []byte) *node48 {
	nn := &node48{}
	nn.setPrefix(prefix)
	return nn
}

func (n *node48) addChild(c byte, nn node) {
	if n.size >= 48 {
		panic("node48 full!")
	}
	n.child[n.size] = nn
	n.key[c] = byte(n.size + 1) // 1-indexed
	n.size++
}

func (n *node48) findChild(c byte) *node {
	i := n.key[c]
	if i == 0 {
		return nil
	}
	return &n.child[i-1]
}

func (n *node48) isFull() bool { return n.size >= 48 }

func (n *node48) grow() node {
	nn := newNode256(n.prefix)
	for c := 0; c < len(n.key); c++ {
		if i := n.key[byte(c)]; i > 0 {
			nn.addChild(byte(c), n.child[i-1])
		}
	}
	return nn
}

// Deletes a child from the node.
func (n *node48) deleteChild(c byte) {
	i := n.key[c]
	if i == 0 {
		return
	}
	i-- // Adjust for 1-indexing
	last := byte(n.size - 1)
	if i < last {
		n.child[i] = n.child[last]
		for ic := 0; ic < len(n.key); ic++ {
			if n.key[byte(ic)] == last+1 {
				n.key[byte(ic)] = i + 1
				break
			}
		}
	}
	n.child[last] = nil
	n.key[c] = 0
	n.size--
}

// Shrink if needed and return new node, otherwise return nil.
func (n *node48) shrink() node {
	if n.size > 16 {
		return nil
	}
	nn := newNode16(nil)
	for c := 0; c < len(n.key); c++ {
		if i := n.key[byte(c)]; i > 0 {
			nn.addChild(byte(c), n.child[i-1])
		}
	}
	return nn
}

// Iterate over all children calling func f.
func (n *node48) iter(f func(node) bool) {
	for _, c := range n.child {
		if c != nil && !f(c) {
			return
		}
	}
}

// Return our children as a slice.
func (n *node48) children() []node {
	return n.child[:n.size]
}
