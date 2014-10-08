// Copyright 2012-2014 Apcera Inc. All rights reserved.
package logger

import (
	"fmt"
	"log"
	"os"
)

type Logger struct {
	logger     *log.Logger
	debug      bool
	trace      bool
	logLabel   string
	fatalLabel string
	debugLabel string
	traceLabel string
}

func NewStdLogger(time, debug, trace, colors bool) *Logger {
	flags := 0
	if time {
		flags = log.LstdFlags
	}

	l := &Logger{
		logger: log.New(os.Stderr, "", flags),
		debug:  debug,
		trace:  trace,
	}

	if colors {
		setColoredLabelFormats(l)
	} else {
		setPlainLabelFormats(l)
	}

	return l
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

	l := &Logger{
		logger: log.New(f, "", flags),
		debug:  debug,
		trace:  trace,
	}

	setPlainLabelFormats(l)
	return l
}

func setPlainLabelFormats(l *Logger) {
	l.logLabel = "[LOG] "
	l.debugLabel = "[DBG] "
	l.fatalLabel = "[ERR] "
	l.traceLabel = "[TRA] "
}

func setColoredLabelFormats(l *Logger) {
	colorFormat := "[\x1b[%dm%s\x1b[0m] "
	l.logLabel = fmt.Sprintf(colorFormat, 32, "LOG")
	l.debugLabel = fmt.Sprintf(colorFormat, 36, "DBG")
	l.fatalLabel = fmt.Sprintf(colorFormat, 31, "ERR")
	l.traceLabel = fmt.Sprintf(colorFormat, 33, "TRA")
}

func (l *Logger) Log(format string, v ...interface{}) {
	l.logger.Printf(l.logLabel+format, v...)
}

func (l *Logger) Fatal(format string, v ...interface{}) {
	l.logger.Fatalf(l.fatalLabel+format, v)
}

func (l *Logger) Debug(format string, v ...interface{}) {
	if l.debug == true {
		l.logger.Printf(l.debugLabel+format, v...)
	}
}

func (l *Logger) Trace(format string, v ...interface{}) {
	if l.trace == true {
		l.logger.Printf(l.traceLabel+format, v...)
	}
}
