// Copyright 2012-2014 Apcera Inc. All rights reserved.
package logger

import (
	"log"
	"os"
)

type Logger struct {
	logger *log.Logger
	debug  bool
	trace  bool
}

func NewStdLogger(time, debug, trace bool) *Logger {
	flags := 0
	if time {
		flags = log.LstdFlags
	}

	return &Logger{
		logger: log.New(os.Stderr, "", flags),
		debug:  debug,
		trace:  trace,
	}
}

func NewFileLogger(filename string, time, debug, trace bool) *Logger {
	fileflags := os.O_WRONLY | os.O_APPEND | os.O_CREATE
	f, err := os.OpenFile(filename, fileflags, 0660)
	if err != nil {
		log.Fatal("error opening file: %v", err)
	}

	flags := 0
	if time {
		flags = log.LstdFlags
	}

	return &Logger{
		logger: log.New(f, "", flags),
		debug:  debug,
		trace:  trace,
	}
}

func (l *Logger) Log(format string, v ...interface{}) {
	l.logger.Printf(format, v...)
}

func (l *Logger) Fatal(format string, v ...interface{}) {
	l.logger.Fatalf(format, v)
}

func (l *Logger) Debug(format string, v ...interface{}) {
	if l.debug == true {
		l.Log(format, v...)
	}
}

func (l *Logger) Trace(format string, v ...interface{}) {
	if l.trace == true {
		l.Log(format, v...)
	}
}
