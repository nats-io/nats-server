// +build !windows
// Copyright 2012-2017 Apcera Inc. All rights reserved.

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
