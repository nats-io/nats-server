// Copyright 2014 Apcera Inc. All rights reserved.

package server

import (
	"testing"
)

func TestSetLogger(t *testing.T) {
	server := &Server{}
	dl := &DummyLogger{}
	server.SetLogger(dl, true, true)

	// We assert that the logger has change to the DummyLogger
	_ = log.logger.(*DummyLogger)

	if debug != 1 {
		t.Fatalf("Expected debug 1, received value %d\n", debug)
	}

	if trace != 1 {
		t.Fatalf("Expected trace 1, received value %d\n", trace)
	}

	// Make sure that we can reset to fal
	server.SetLogger(dl, false, false)
	if debug != 0 {
		t.Fatalf("Expected debug 0, got %v", debug)
	}
	if trace != 0 {
		t.Fatalf("Expected trace 0, got %v", trace)
	}
}

type DummyLogger struct{}

func (l *DummyLogger) Noticef(format string, v ...interface{}) {}
func (l *DummyLogger) Errorf(format string, v ...interface{})  {}
func (l *DummyLogger) Fatalf(format string, v ...interface{})  {}
func (l *DummyLogger) Debugf(format string, v ...interface{})  {}
func (l *DummyLogger) Tracef(format string, v ...interface{})  {}
