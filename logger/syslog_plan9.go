// Copyright 2012-2025 The NATS Authors
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

// Package logger logs to the windows event log
package logger

import (
	"fmt"
	"os"
)

var natsEventSource = "NATS-Server"

// SetSyslogName sets the name to use for the system log event source
func SetSyslogName(name string) {
	natsEventSource = name
}

// SysLogger placeholder
type SysLogger struct {
	debug  bool
	trace  bool
}

// NewSysLogger creates a placeholder log
func NewSysLogger(debug, trace bool) *SysLogger {
	return &SysLogger{
		debug:  debug,
		trace:  trace,
	}
}

// NewRemoteSysLogger creates a placeholder remote event logger
func NewRemoteSysLogger(fqn string, debug, trace bool) *SysLogger {
	return &SysLogger{
		debug:  debug,
		trace:  trace,
	}
}

func formatMsg(tag, format string, v ...any) string {
	orig := fmt.Sprintf(format, v...)
	return fmt.Sprintf("pid[%d][%s]: %s", os.Getpid(), tag, orig)
}

// Noticef logs a notice statement
func (l *SysLogger) Noticef(format string, v ...any) {
	fmt.Fprintln(os.Stderr, formatMsg("NOTICE", format, v...))
}

// Warnf logs a warning statement
func (l *SysLogger) Warnf(format string, v ...any) {
	fmt.Fprintln(os.Stderr, formatMsg("WARN", format, v...))
}

// Fatalf logs a fatal error
func (l *SysLogger) Fatalf(format string, v ...any) {
	msg := formatMsg("FATAL", format, v...)
	fmt.Fprintln(os.Stderr, msg)
	panic(msg)
}

// Errorf logs an error statement
func (l *SysLogger) Errorf(format string, v ...any) {
	fmt.Fprintln(os.Stderr, formatMsg("ERROR", format, v...))
}

// Debugf logs a debug statement
func (l *SysLogger) Debugf(format string, v ...any) {
	if l.debug {
		fmt.Fprintln(os.Stderr, formatMsg("DEBUG", format, v...))
	}
}

// Tracef logs a trace statement
func (l *SysLogger) Tracef(format string, v ...any) {
	if l.trace {
		fmt.Fprintln(os.Stderr, formatMsg("TRACE", format, v...))
	}
}
