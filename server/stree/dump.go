// Copyright 2024 The NATS Authors
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
	"fmt"
	"io"
	"strings"
)

// For dumping out a text representation of a tree.
func (t *SubjectTree[T]) Dump(w io.Writer) {
	t.dump(w, t.root, 0)
	fmt.Fprintln(w)
}

// Will dump out a node.
func (t *SubjectTree[T]) dump(w io.Writer, n node, depth int) {
	if n == nil {
		fmt.Fprintf(w, "EMPTY\n")
		return
	}
	if n.isLeaf() {
		leaf := n.(*leaf[T])
		fmt.Fprintf(w, "%s LEAF: Suffix: %q Value: %+v\n", dumpPre(depth), leaf.suffix, leaf.value)
		n = nil
	} else {
		// We are a node type here, grab meta portion.
		bn := n.base()
		fmt.Fprintf(w, "%s %s Prefix: %q\n", dumpPre(depth), n.kind(), bn.prefix)
		depth++
		n.iter(func(n node) bool {
			t.dump(w, n, depth)
			return true
		})
	}
}

// For individual node/leaf dumps.
func (n *leaf[T]) kind() string { return "LEAF" }
func (n *node4) kind() string   { return "NODE4" }
func (n *node10) kind() string  { return "NODE10" }
func (n *node16) kind() string  { return "NODE16" }
func (n *node48) kind() string  { return "NODE48" }
func (n *node256) kind() string { return "NODE256" }

// Calculates the indendation, etc.
func dumpPre(depth int) string {
	if depth == 0 {
		return "-- "
	} else {
		var b strings.Builder
		for i := 0; i < depth; i++ {
			b.WriteString("  ")
		}
		b.WriteString("|__ ")
		return b.String()
	}
}
