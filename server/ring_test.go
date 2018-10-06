// Copyright 2018 The NATS Authors
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

package server

import (
	"fmt"
	"reflect"
	"testing"
)

func TestRBAppendAndLenAndTotal(t *testing.T) {
	rb := newClosedRingBuffer(10)
	for i := 0; i < 5; i++ {
		rb.append(&closedClient{})
	}
	if rbl := rb.len(); rbl != 5 {
		t.Fatalf("Expected len of 5, got %d", rbl)
	}
	if rbt := rb.totalConns(); rbt != 5 {
		t.Fatalf("Expected total of 5, got %d", rbt)
	}
	for i := 0; i < 25; i++ {
		rb.append(&closedClient{})
	}
	if rbl := rb.len(); rbl != 10 {
		t.Fatalf("Expected len of 10, got %d", rbl)
	}
	if rbt := rb.totalConns(); rbt != 30 {
		t.Fatalf("Expected total of 30, got %d", rbt)
	}
}

func (cc *closedClient) String() string {
	return cc.user
}

func TestRBclosedClients(t *testing.T) {
	rb := newClosedRingBuffer(10)

	var ui int
	addConn := func() {
		ui++
		rb.append(&closedClient{user: fmt.Sprintf("%d", ui)})
	}

	max := 100
	master := make([]*closedClient, 0, max)
	for i := 1; i <= max; i++ {
		master = append(master, &closedClient{user: fmt.Sprintf("%d", i)})
	}

	testList := func(i int) {
		ccs := rb.closedClients()
		start := int(rb.totalConns()) - len(ccs)
		ms := master[start : start+len(ccs)]
		if !reflect.DeepEqual(ccs, ms) {
			t.Fatalf("test %d: List result did not match master: %+v vs %+v", i, ccs, ms)
		}
	}

	for i := 0; i < max; i++ {
		addConn()
		testList(i)
	}
}
