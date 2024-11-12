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

// Node with 10 children
// This node size is for the particular case that a part of the subject is numeric
// in nature, i.e. it only needs to satisfy the range 0-9 without wasting bytes
// Order of struct fields for best memory alignment (as per govet/fieldalignment)
type node10 struct {
	child [10]node
	meta
	key [10]byte
}

func newNode10(prefix []byte) *node10 {
	nn := &node10{}
	nn.setPrefix(prefix)
	return nn
}

// Currently we do not keep node10 sorted or use bitfields for traversal so just add to the end.
// TODO(dlc) - We should revisit here with more detailed benchmarks.
func (n *node10) addChild(c byte, nn node) {
	if n.size >= 10 {
		panic("node10 full!")
	}
	n.key[n.size] = c
	n.child[n.size] = nn
	n.size++
}

func (n *node10) findChild(c byte) *node {
	for i := uint16(0); i < n.size; i++ {
		if n.key[i] == c {
			return &n.child[i]
		}
	}
	return nil
}

func (n *node10) isFull() bool { return n.size >= 10 }

func (n *node10) grow() node {
	nn := newNode16(n.prefix)
	for i := 0; i < 10; i++ {
		nn.addChild(n.key[i], n.child[i])
	}
	return nn
}

// Deletes a child from the node.
func (n *node10) deleteChild(c byte) {
	for i, last := uint16(0), n.size-1; i < n.size; i++ {
		if n.key[i] == c {
			// Unsorted so just swap in last one here, else nil if last.
			if i < last {
				n.key[i] = n.key[last]
				n.child[i] = n.child[last]
				n.key[last] = 0
				n.child[last] = nil
			} else {
				n.key[i] = 0
				n.child[i] = nil
			}
			n.size--
			return
		}
	}
}

// Shrink if needed and return new node, otherwise return nil.
func (n *node10) shrink() node {
	if n.size > 4 {
		return nil
	}
	nn := newNode4(nil)
	for i := uint16(0); i < n.size; i++ {
		nn.addChild(n.key[i], n.child[i])
	}
	return nn
}

// Iterate over all children calling func f.
func (n *node10) iter(f func(node) bool) {
	for i := uint16(0); i < n.size; i++ {
		if !f(n.child[i]) {
			return
		}
	}
}

// Return our children as a slice.
func (n *node10) children() []node {
	return n.child[:n.size]
}
