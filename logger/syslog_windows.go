// Copyright 2012-2016 Apcera Inc. All rights reserved.

// Package logger logs to the windows event log
package logger

import (
	"fmt"
	"golang.org/x/sys/windows/svc/eventlog"
	"os"
	"strings"
)

const (
	natsEventSource = "NATS-Server"
)

// SysLogger logs to the windows event logger
type SysLogger struct {
	writer *eventlog.Log
	debug  bool
	trace  bool
}

// NewSysLogger creates a log using the windows event logger
func NewSysLogger(debug, trace bool) *SysLogger {
	if err := eventlog.InstallAsEventCreate(natsEventSource, eventlog.Info|eventlog.Error|eventlog.Warning); err != nil {
		if !strings.Contains(err.Error(), "registry key already exists") {
			panic(fmt.Sprintf("could not access event log: %v", err))
		}
	}

	w, err := eventlog.Open(natsEventSource)
	if err != nil {
		panic(fmt.Sprintf("could not open event log: %v", err))
	}

	return &SysLogger{
		writer: w,
		debug:  debug,
		trace:  trace,
	}
}

// NewRemoteSysLogger creates a remote event logger
func NewRemoteSysLogger(fqn string, debug, trace bool) *SysLogger {
	w, err := eventlog.OpenRemote(fqn, natsEventSource)
	if err != nil {
		panic(fmt.Sprintf("could not open event log: %v", err))
	}

	return &SysLogger{
		writer: w,
		debug:  debug,
		trace:  trace,
	}
}

func formatMsg(tag, format string, v ...interface{}) string {
	orig := fmt.Sprintf(format, v...)
	return fmt.Sprintf("pid[%d][%s]: %s", os.Getpid(), tag, orig)
}

// Noticef logs a notice statement
func (l *SysLogger) Noticef(format string, v ...interface{}) {
	l.writer.Info(1, formatMsg("NOTICE", format, v...))
}

// Fatalf logs a fatal error
func (l *SysLogger) Fatalf(format string, v ...interface{}) {
	msg := formatMsg("FATAL", format, v...)
	l.writer.Error(5, msg)
	panic(msg)
}

// Errorf logs an error statement
func (l *SysLogger) Errorf(format string, v ...interface{}) {
	l.writer.Error(2, formatMsg("ERROR", format, v...))
}

// Debugf logs a debug statement
func (l *SysLogger) Debugf(format string, v ...interface{}) {
	if l.debug {
		l.writer.Info(3, formatMsg("DEBUG", format, v...))
	}
}

// Tracef logs a trace statement
func (l *SysLogger) Tracef(format string, v ...interface{}) {
	if l.trace {
		l.writer.Info(4, formatMsg("TRACE", format, v...))
	}
}
