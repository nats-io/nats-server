// Copyright 2012-2017 Apcera Inc. All rights reserved.

package server

import (
	"os"
	"sync"
	"sync/atomic"

	"github.com/nats-io/gnatsd/logger"
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

// ConfigureLogger configures and sets the logger for the server.
func (s *Server) ConfigureLogger() {
	var (
		log  Logger
		opts = s.getOpts()
	)

	if opts.LogFile != "" {
		log = logger.NewFileLogger(opts.LogFile, opts.Logtime, opts.Debug, opts.Trace, true)
	} else if opts.RemoteSyslog != "" {
		log = logger.NewRemoteSysLogger(opts.RemoteSyslog, opts.Debug, opts.Trace)
	} else if opts.Syslog {
		log = logger.NewSysLogger(opts.Debug, opts.Trace)
	} else {
		colors := true
		// Check to see if stderr is being redirected and if so turn off color
		// Also turn off colors if we're running on Windows where os.Stderr.Stat() returns an invalid handle-error
		stat, err := os.Stderr.Stat()
		if err != nil || (stat.Mode()&os.ModeCharDevice) == 0 {
			colors = false
		}
		log = logger.NewStdLogger(opts.Logtime, opts.Debug, opts.Trace, colors, true)
	}

	s.SetLogger(log, opts.Debug, opts.Trace)
}

// SetLogger sets the logger of the server
func (s *Server) SetLogger(logger Logger, debugFlag, traceFlag bool) {
	if debugFlag {
		atomic.StoreInt32(&debug, 1)
	} else {
		atomic.StoreInt32(&debug, 0)
	}
	if traceFlag {
		atomic.StoreInt32(&trace, 1)
	} else {
		atomic.StoreInt32(&trace, 0)
	}

	log.Lock()
	log.logger = logger
	log.Unlock()
}

// If the logger is a file based logger, close and re-open the file.
// This allows for file rotation by 'mv'ing the file then signaling
// the process to trigger this function.
func (s *Server) ReOpenLogFile() {
	// Check to make sure this is a file logger.
	log.Lock()
	ll := log.logger
	log.Unlock()

	if ll == nil {
		Noticef("File log re-open ignored, no logger")
		return
	}
	if s.getOpts().LogFile == "" {
		Noticef("File log re-open ignored, not a file logger")
	} else {
		fileLog := logger.NewFileLogger(s.getOpts().LogFile,
			s.getOpts().Logtime, s.getOpts().Debug, s.getOpts().Trace, true)
		s.SetLogger(fileLog, s.getOpts().Debug, s.getOpts().Trace)
		Noticef("File log re-opened")
	}
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
