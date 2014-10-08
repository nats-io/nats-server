// Copyright 2012-2014 Apcera Inc. All rights reserved.
package logger

import (
	"fmt"
	"log"
	"log/syslog"
)

type SysLogger struct {
	writer *syslog.Writer
	debug  bool
	trace  bool
}

func NewSysLogger(debug, trace bool) *SysLogger {
	w, err := syslog.New(syslog.LOG_DAEMON|syslog.LOG_NOTICE, "gnatsd")
	if err != nil {
		log.Fatal("error connecting to syslog: %v", err)
	}

	return &SysLogger{
		writer: w,
		debug:  debug,
		trace:  trace,
	}
}

func NewRemoteSysLogger(network, raddr string, debug, trace bool) *SysLogger {
	w, err := syslog.Dial(network, raddr, syslog.LOG_DEBUG, "gnatsd")
	if err != nil {
		log.Fatal("error connecting to syslog: %v", err)
	}

	return &SysLogger{
		writer: w,
		debug:  debug,
		trace:  trace,
	}
}

func (l *SysLogger) Log(format string, v ...interface{}) {
	l.writer.Notice(fmt.Sprintf(format, v...))
}

func (l *SysLogger) Fatal(format string, v ...interface{}) {
	l.writer.Crit(fmt.Sprintf(format, v...))
}

func (l *SysLogger) Debug(format string, v ...interface{}) {
	if l.debug {
		l.writer.Debug(fmt.Sprintf(format, v...))
	}
}

func (l *SysLogger) Trace(format string, v ...interface{}) {
	if l.trace {
		l.writer.Info(fmt.Sprintf(format, v...))
	}
}
