// Copyright 2012-2017 Apcera Inc. All rights reserved.

package server

import (
	"os"
	"sync/atomic"

	"github.com/nats-io/gnatsd/logger"
)

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
		log Logger

		// Snapshot server options.
		opts = s.getOpts()
	)

	syslog := opts.Syslog
	if isWindowsService() && opts.LogFile == "" {
		// Enable syslog if no log file is specified and we're running as a
		// Windows service so that logs are written to the Windows event log.
		syslog = true
	}

	if opts.LogFile != "" {
		log = logger.NewFileLogger(opts.LogFile, opts.Logtime, opts.Debug, opts.Trace, true)
	} else if opts.RemoteSyslog != "" {
		log = logger.NewRemoteSysLogger(opts.RemoteSyslog, opts.Debug, opts.Trace)
	} else if syslog {
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
		atomic.StoreInt32(&s.logging.debug, 1)
	} else {
		atomic.StoreInt32(&s.logging.debug, 0)
	}
	if traceFlag {
		atomic.StoreInt32(&s.logging.trace, 1)
	} else {
		atomic.StoreInt32(&s.logging.trace, 0)
	}

	s.logging.Lock()
	s.logging.logger = logger
	s.logging.Unlock()
}

// If the logger is a file based logger, close and re-open the file.
// This allows for file rotation by 'mv'ing the file then signaling
// the process to trigger this function.
func (s *Server) ReOpenLogFile() {
	// Check to make sure this is a file logger.
	s.logging.RLock()
	ll := s.logging.logger
	s.logging.RUnlock()

	if ll == nil {
		s.Noticef("File log re-open ignored, no logger")
		return
	}

	// Snapshot server options.
	opts := s.getOpts()

	if opts.LogFile == "" {
		s.Noticef("File log re-open ignored, not a file logger")
	} else {
		fileLog := logger.NewFileLogger(opts.LogFile,
			opts.Logtime, opts.Debug, opts.Trace, true)
		s.SetLogger(fileLog, opts.Debug, opts.Trace)
		s.Noticef("File log re-opened")
	}
}

// Noticef logs a notice statement
func (s *Server) Noticef(format string, v ...interface{}) {
	s.executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Noticef(format, v...)
	}, format, v...)
}

// Errorf logs an error
func (s *Server) Errorf(format string, v ...interface{}) {
	s.executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Errorf(format, v...)
	}, format, v...)
}

// Fatalf logs a fatal error
func (s *Server) Fatalf(format string, v ...interface{}) {
	s.executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Fatalf(format, v...)
	}, format, v...)
}

// Debugf logs a debug statement
func (s *Server) Debugf(format string, v ...interface{}) {
	if atomic.LoadInt32(&s.logging.debug) == 0 {
		return
	}

	s.executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Debugf(format, v...)
	}, format, v...)
}

// Tracef logs a trace statement
func (s *Server) Tracef(format string, v ...interface{}) {
	if atomic.LoadInt32(&s.logging.trace) == 0 {
		return
	}

	s.executeLogCall(func(logger Logger, format string, v ...interface{}) {
		logger.Tracef(format, v...)
	}, format, v...)
}

func (s *Server) executeLogCall(f func(logger Logger, format string, v ...interface{}), format string, args ...interface{}) {
	s.logging.RLock()
	defer s.logging.RUnlock()
	if s.logging.logger == nil {
		return
	}

	f(s.logging.logger, format, args...)
}
