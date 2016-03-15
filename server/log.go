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

// Logger interface of the NATS Server
type Logger interface {

	// Log a notice statement
	Noticef(format string, v ...interface{})

	// Log a fatal error
	Fatalf(format string, v ...interface{})

	// Log an error
	Errorf(format string, v ...interface{})

	// Log a debug statement
	Debugf(format string, v ...interface{})

	// Log a trace statement
	Tracef(format string, v ...interface{})
}

// SetLogger sets the logger of the server
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

// Noticef logs a notice statement
func Noticef(format string, v ...interface{}) {
	executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Noticef(format, v...)
	}, format, v...)
}

// Errorf logs an error
func Errorf(format string, v ...interface{}) {
	executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Errorf(format, v...)
	}, format, v...)
}

// Fatalf logs a fatal error
func Fatalf(format string, v ...interface{}) {
	executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Fatalf(format, v...)
	}, format, v...)
}

// Debugf logs a debug statement
func Debugf(format string, v ...interface{}) {
	if atomic.LoadInt32(&debug) == 0 {
		return
	}

	executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Debugf(format, v...)
	}, format, v...)
}

// Tracef logs a trace statement
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
