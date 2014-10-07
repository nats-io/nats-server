// Copyright 2014 Apcera Inc. All rights reserved.

package server

import (
	"testing"
)

func TestSetLogger(t *testing.T) {
	// We assert that the default logger is the NilLogger
	_ = log.(*NilLogger)

	server := &Server{}
	server.SetLogger(&DummyLogger{})

	// We assert that the logger has change to the DummyLogger
	_ = log.(*DummyLogger)
}

type DummyLogger struct{}

func (l *DummyLogger) Log(format string, v ...interface{})   {}
func (l *DummyLogger) Fatal(format string, v ...interface{}) {}
func (l *DummyLogger) Debug(format string, v ...interface{}) {}
func (l *DummyLogger) Trace(format string, v ...interface{}) {}
