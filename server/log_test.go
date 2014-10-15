// Copyright 2014 Apcera Inc. All rights reserved.

package server

import (
	"testing"
)

func TestSetLogger(t *testing.T) {
	server := &Server{}
	server.SetLogger(&DummyLogger{}, true, true)

	// We assert that the logger has change to the DummyLogger
	_ = log.logger.(*DummyLogger)

	if debug != 1 {
		t.Fatalf("Expected debug 1, received value %d\n", debug)
	}

	if trace != 1 {
		t.Fatalf("Expected trace 1, received value %d\n", trace)
	}
}

type DummyLogger struct{}

func (l *DummyLogger) Notice(format string, v ...interface{}) {}
func (l *DummyLogger) Error(format string, v ...interface{})  {}
func (l *DummyLogger) Fatal(format string, v ...interface{})  {}
func (l *DummyLogger) Debug(format string, v ...interface{})  {}
func (l *DummyLogger) Trace(format string, v ...interface{})  {}
