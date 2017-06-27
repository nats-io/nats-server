// +build !windows
// Copyright 2012-2017 Apcera Inc. All rights reserved.

package server

import (
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	s := New(DefaultOptions())
	go func() {
		if !s.ReadyForConnections(time.Second) {
			t.Fatal("Failed to start server in time")
		}
		s.Shutdown()
	}()
	if err := Run(s); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
}
