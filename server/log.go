// Copyright 2012-2015 Apcera Inc. All rights reserved.

package server

import (
	"sync"
	"sync/atomic"
)

// Package globals for performance checks
var trace int32
var debug int32

var log = struct {
	sync.Mutex
	logger Logger
}{}

type Logger interface {
	Noticef(format string, v ...interface{})
	Fatalf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
	Debugf(format string, v ...interface{})
	Tracef(format string, v ...interface{})
}

func (s *Server) SetLogger(logger Logger, debugFlag, traceFlag bool) {
	if debugFlag {
		atomic.StoreInt32(&debug, 1)
	}

	if traceFlag {
		atomic.StoreInt32(&trace, 1)
	}

	log.Lock()
	log.logger = logger
	log.Unlock()
}

func Noticef(format string, v ...interface{}) {
	executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Noticef(format, v...)
	}, format, v...)
}

func Errorf(format string, v ...interface{}) {
	executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Errorf(format, v...)
	}, format, v...)
}

func Fatalf(format string, v ...interface{}) {
	executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Fatalf(format, v...)
	}, format, v...)
}

func Debugf(format string, v ...interface{}) {
	if atomic.LoadInt32(&debug) == 0 {
		return
	}

	executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Debugf(format, v...)
	}, format, v...)
}

func Tracef(format string, v ...interface{}) {
	if atomic.LoadInt32(&trace) == 0 {
		return
	}

	executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Tracef(format, v...)
	}, format, v...)
}

func executeLogCall(f func(logger Logger, format string, v ...interface{}), format string, args ...interface{}) {
	log.Lock()
	defer log.Unlock()
	if log.logger == nil {
		return
	}

	f(log.logger, format, args...)
}
