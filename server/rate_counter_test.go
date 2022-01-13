// Copyright 2021-2022 The NATS Authors
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
	"testing"
	"time"
)

func TestRateCounter(t *testing.T) {
	counter := newRateCounter(10)
	counter.interval = 100 * time.Millisecond

	var i int
	for i = 0; i < 10; i++ {
		if !counter.allow() {
			t.Errorf("counter should allow (iteration %d)", i)
		}
	}
	for i = 0; i < 5; i++ {
		if counter.allow() {
			t.Errorf("counter should not allow (iteration %d)", i)
		}
	}

	blocked := counter.countBlocked()
	if blocked != 5 {
		t.Errorf("Expected blocked = 5, got %d", blocked)
	}

	blocked = counter.countBlocked()
	if blocked != 0 {
		t.Errorf("Expected blocked = 0, got %d", blocked)
	}

	time.Sleep(150 * time.Millisecond)

	if !counter.allow() {
		t.Errorf("Expected true after current time window expired")
	}
}
