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

// Node with 16 children
// Order of struct fields for best memory alignment (as per govet/fieldalignment)
type node16 struct {
	child [16]node
	meta
	key [16]byte
}

func newNode16(prefix []byte) *node16 {
	nn := &node16{}
	nn.setPrefix(prefix)
	return nn
}

// Currently we do not keep node16 sorted or use bitfields for traversal so just add to the end.
// TODO(dlc) - We should revisit here with more detailed benchmarks.
func (n *node16) addChild(c byte, nn node) {
	if n.size >= 16 {
		panic("node16 full!")
	}
	n.key[n.size] = c
	n.child[n.size] = nn
	n.size++
}

func (n *node16) findChild(c byte) *node {
	for i := uint16(0); i < n.size; i++ {
		if n.key[i] == c {
			return &n.child[i]
		}
	}
	return nil
}

func (n *node16) isFull() bool { return n.size >= 16 }

func (n *node16) grow() node {
	nn := newNode48(n.prefix)
	for i := 0; i < 16; i++ {
		nn.addChild(n.key[i], n.child[i])
	}
	return nn
}

// Deletes a child from the node.
func (n *node16) deleteChild(c byte) {
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
func (n *node16) shrink() node {
	if n.size > 10 {
		return nil
	}
	nn := newNode10(nil)
	for i := uint16(0); i < n.size; i++ {
		nn.addChild(n.key[i], n.child[i])
	}
	return nn
}

// Iterate over all children calling func f.
func (n *node16) iter(f func(node) bool) {
	for i := uint16(0); i < n.size; i++ {
		if !f(n.child[i]) {
			return
		}
	}
}

// Return our children as a slice.
func (n *node16) children() []node {
	return n.child[:n.size]
}
