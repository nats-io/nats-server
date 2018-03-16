// Copyright 2012-2018 The NATS Authors
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

// +build !windows

package server

import (
	"errors"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	var (
		s       = New(DefaultOptions())
		started = make(chan error, 1)
		errC    = make(chan error, 1)
	)
	go func() {
		errC <- Run(s)
	}()
	go func() {
		if !s.ReadyForConnections(time.Second) {
			started <- errors.New("failed to start in time")
			return
		}
		s.Shutdown()
		close(started)
	}()

	select {
	case err := <-errC:
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out")
	}
	if err := <-started; err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}
