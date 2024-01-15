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
type node16 struct {
	meta
	children [16]node
	keys     [16]byte
}

func newNode16(prefix []byte) *node16 {
	nn := &node16{}
	nn.setPrefix(prefix)
	return nn
}

func (n *node16) isLeaf() bool { return false }
func (n *node16) base() *meta  { return &n.meta }

func (n *node16) setPrefix(pre []byte) {
	n.prefixLen = uint16(min(len(pre), maxPrefixLen))
	for i := uint16(0); i < n.prefixLen; i++ {
		n.prefix[i] = pre[i]
	}
}

// Currently we do not keep node16 sorted or use bitfields for traversal so just add to the end.
// TODO(dlc) - We should revisit here with more detailed benchmarks.
func (n *node16) addChild(c byte, nn node) {
	if n.size >= 16 {
		panic("node16 full!")
	}
	n.keys[n.size] = c
	n.children[n.size] = nn
	n.size++
}

func (n *node16) numChildren() uint16 { return n.size }
func (n *node16) path() []byte        { return n.prefix[:n.prefixLen] }

func (n *node16) findChild(c byte) *node {
	for i := uint16(0); i < n.size; i++ {
		if n.keys[i] == c {
			return &n.children[i]
		}
	}
	return nil
}

func (n *node16) isFull() bool { return n.size >= 16 }

func (n *node16) grow() node {
	nn := newNode256(n.prefix[:n.prefixLen])
	for i := 0; i < 16; i++ {
		nn.addChild(n.keys[i], n.children[i])
	}
	return nn
}

// Deletes a child from the node.
func (n *node16) deleteChild(c byte) {
	for i, last := uint16(0), n.size-1; i < n.size; i++ {
		if n.keys[i] == c {
			// Unsorted so just swap in last one here, else nil if last.
			if i < last {
				n.keys[i] = n.keys[last]
				n.children[i] = n.children[last]
			} else {
				n.keys[i] = 0
				n.children[i] = nil
			}
			n.size--
			return
		}
	}
}

// Shrink if needed and return new node, otherwise return nil.
func (n *node16) shrink() node {
	if n.size > 4 {
		return nil
	}
	nn := newNode4(nil)
	for i := uint16(0); i < n.size; i++ {
		nn.addChild(n.keys[i], n.children[i])
	}
	return nn
}

// Will match parts against our prefix.no
func (n *node16) matchParts(parts [][]byte) ([][]byte, bool) {
	return matchParts(parts, n.prefix[:n.prefixLen])
}

// Iterate over all children calling func f.
func (n *node16) iter(f func(node) bool) {
	for i := uint16(0); i < n.size; i++ {
		if !f(n.children[i]) {
			return
		}
	}
}
