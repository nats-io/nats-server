// Copyright 2012-2014 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"sync"
	"sync/atomic"
)

var trace int32
var debug int32
var log = struct {
	logger Logger
	sync.Mutex
}{}

type Logger interface {
	Notice(format string, v ...interface{})
	Fatal(format string, v ...interface{})
	Error(format string, v ...interface{})
	Debug(format string, v ...interface{})
	Trace(format string, v ...interface{})
}

func (s *Server) SetLogger(logger Logger, d, t bool) {
	if d {
		atomic.StoreInt32(&debug, 1)
	}

	if t {
		atomic.StoreInt32(&trace, 1)
	}

	log.Lock()
	defer log.Unlock()
	log.logger = logger
}

func Notice(format string, v ...interface{}) {
	executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Notice(format, v...)
	}, format, v...)
}

func Error(format string, v ...interface{}) {
	executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Error(format, v...)
	}, format, v...)
}

func Fatal(format string, v ...interface{}) {
	executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Fatal(format, v...)
	}, format, v...)
}

func Debug(format string, v ...interface{}) {
	if debug == 0 {
		return
	}

	executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Debug(format, v...)
	}, format, v...)
}

func Trace(format string, v ...interface{}) {
	if trace == 0 {
		return
	}

	executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Trace(format, v...)
	}, format, v...)
}

func executeLogCall(f func(logger Logger, format string, v ...interface{}), format string, args ...interface{}) {
	log.Lock()
	defer log.Unlock()
	if log.logger == nil {
		return
	}

	argc := len(args)
	if argc != 0 {
		if client, ok := args[argc-1].(*Client); ok {
			args = args[:argc-1]
			format = fmt.Sprintf("%s - %s", client, format)
		}
	}

	f(log.logger, format, args...)
}
