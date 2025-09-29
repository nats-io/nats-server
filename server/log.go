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

package server

import (
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	srvlog "github.com/nats-io/nats-server/v2/logger"
)

// Logger interface of the NATS Server
type Logger interface {

	// Log a notice statement
	Noticef(format string, v ...any)

	// Log a warning statement
	Warnf(format string, v ...any)

	// Log a fatal error
	Fatalf(format string, v ...any)

	// Log an error
	Errorf(format string, v ...any)

	// Log a debug statement
	Debugf(format string, v ...any)

	// Log a trace statement
	Tracef(format string, v ...any)
}

// ConfigureLogger configures and sets the logger for the server.
func (s *Server) ConfigureLogger() {
	var (
		log Logger

		// Snapshot server options.
		opts = s.getOpts()
	)

	if opts.NoLog {
		return
	}

	syslog := opts.Syslog
	if isWindowsService() && opts.LogFile == "" {
		// Enable syslog if no log file is specified and we're running as a
		// Windows service so that logs are written to the Windows event log.
		syslog = true
	}

	if opts.LogFile != "" {
		log = srvlog.NewFileLogger(opts.LogFile, opts.Logtime, opts.Debug, opts.Trace, true, srvlog.LogUTC(opts.LogtimeUTC))
		if opts.LogSizeLimit > 0 {
			if l, ok := log.(*srvlog.Logger); ok {
				l.SetSizeLimit(opts.LogSizeLimit)
			}
		}
		if opts.LogMaxFiles > 0 {
			if l, ok := log.(*srvlog.Logger); ok {
				al := int(opts.LogMaxFiles)
				if int64(al) != opts.LogMaxFiles {
					// set to default (no max) on overflow
					al = 0
				}
				l.SetMaxNumFiles(al)
			}
		}
	} else if opts.RemoteSyslog != "" {
		log = srvlog.NewRemoteSysLogger(opts.RemoteSyslog, opts.Debug, opts.Trace)
	} else if syslog {
		log = srvlog.NewSysLogger(opts.Debug, opts.Trace)
	} else {
		colors := true
		// Check to see if stderr is being redirected and if so turn off color
		// Also turn off colors if we're running on Windows where os.Stderr.Stat() returns an invalid handle-error
		stat, err := os.Stderr.Stat()
		if err != nil || (stat.Mode()&os.ModeCharDevice) == 0 {
			colors = false
		}
		log = srvlog.NewStdLogger(opts.Logtime, opts.Debug, opts.Trace, colors, true, srvlog.LogUTC(opts.LogtimeUTC))
	}

	s.SetLoggerV2(log, opts.Debug, opts.Trace, opts.TraceVerbose)
}

// Returns our current logger.
func (s *Server) Logger() Logger {
	s.logging.Lock()
	defer s.logging.Unlock()
	return s.logging.logger
}

// SetLogger sets the logger of the server
func (s *Server) SetLogger(logger Logger, debugFlag, traceFlag bool) {
	s.SetLoggerV2(logger, debugFlag, traceFlag, false)
}

// SetLogger sets the logger of the server
func (s *Server) SetLoggerV2(logger Logger, debugFlag, traceFlag, sysTrace bool) {
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
	if sysTrace {
		atomic.StoreInt32(&s.logging.traceSysAcc, 1)
	} else {
		atomic.StoreInt32(&s.logging.traceSysAcc, 0)
	}
	s.logging.Lock()
	if s.logging.logger != nil {
		// Check to see if the logger implements io.Closer.  This could be a
		// logger from another process embedding the NATS server or a dummy
		// test logger that may not implement that interface.
		if l, ok := s.logging.logger.(io.Closer); ok {
			if err := l.Close(); err != nil {
				s.Errorf("Error closing logger: %v", err)
			}
		}
	}
	s.logging.logger = logger
	s.logging.Unlock()
}

// ReOpenLogFile if the logger is a file based logger, close and re-open the file.
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
		fileLog := srvlog.NewFileLogger(
			opts.LogFile, opts.Logtime,
			opts.Debug, opts.Trace, true,
			srvlog.LogUTC(opts.LogtimeUTC),
		)
		s.SetLogger(fileLog, opts.Debug, opts.Trace)
		if opts.LogSizeLimit > 0 {
			fileLog.SetSizeLimit(opts.LogSizeLimit)
		}
		s.Noticef("File log re-opened")
	}
}

// Noticef logs a notice statement
func (s *Server) Noticef(format string, v ...any) {
	s.executeLogCall(func(logger Logger, format string, v ...any) {
		logger.Noticef(format, v...)
	}, format, v...)
}

// Errorf logs an error
func (s *Server) Errorf(format string, v ...any) {
	s.executeLogCall(func(logger Logger, format string, v ...any) {
		logger.Errorf(format, v...)
	}, format, v...)
}

// Error logs an error with a scope
func (s *Server) Errors(scope any, e error) {
	s.executeLogCall(func(logger Logger, format string, v ...any) {
		logger.Errorf(format, v...)
	}, "%s - %s", scope, UnpackIfErrorCtx(e))
}

// Error logs an error with a context
func (s *Server) Errorc(ctx string, e error) {
	s.executeLogCall(func(logger Logger, format string, v ...any) {
		logger.Errorf(format, v...)
	}, "%s: %s", ctx, UnpackIfErrorCtx(e))
}

// Error logs an error with a scope and context
func (s *Server) Errorsc(scope any, ctx string, e error) {
	s.executeLogCall(func(logger Logger, format string, v ...any) {
		logger.Errorf(format, v...)
	}, "%s - %s: %s", scope, ctx, UnpackIfErrorCtx(e))
}

// Warnf logs a warning error
func (s *Server) Warnf(format string, v ...any) {
	s.executeLogCall(func(logger Logger, format string, v ...any) {
		logger.Warnf(format, v...)
	}, format, v...)
}

func (s *Server) rateLimitFormatWarnf(format string, v ...any) {
	if _, loaded := s.rateLimitLogging.LoadOrStore(format, time.Now()); loaded {
		return
	}
	statement := fmt.Sprintf(format, v...)
	s.Warnf("%s", statement)
}

func (s *Server) RateLimitWarnf(format string, v ...any) {
	statement := fmt.Sprintf(format, v...)
	if _, loaded := s.rateLimitLogging.LoadOrStore(statement, time.Now()); loaded {
		return
	}
	s.Warnf("%s", statement)
}

func (s *Server) RateLimitDebugf(format string, v ...any) {
	statement := fmt.Sprintf(format, v...)
	if _, loaded := s.rateLimitLogging.LoadOrStore(statement, time.Now()); loaded {
		return
	}
	s.Debugf("%s", statement)
}

// Fatalf logs a fatal error
func (s *Server) Fatalf(format string, v ...any) {
	if s.isShuttingDown() {
		s.Errorf(format, v)
		return
	}
	s.executeLogCall(func(logger Logger, format string, v ...any) {
		logger.Fatalf(format, v...)
	}, format, v...)
}

// Debugf logs a debug statement
func (s *Server) Debugf(format string, v ...any) {
	if atomic.LoadInt32(&s.logging.debug) == 0 {
		return
	}

	s.executeLogCall(func(logger Logger, format string, v ...any) {
		logger.Debugf(format, v...)
	}, format, v...)
}

// Tracef logs a trace statement
func (s *Server) Tracef(format string, v ...any) {
	if atomic.LoadInt32(&s.logging.trace) == 0 {
		return
	}

	s.executeLogCall(func(logger Logger, format string, v ...any) {
		logger.Tracef(format, v...)
	}, format, v...)
}

func (s *Server) executeLogCall(f func(logger Logger, format string, v ...any), format string, args ...any) {
	s.logging.RLock()
	defer s.logging.RUnlock()
	if s.logging.logger == nil {
		return
	}

	f(s.logging.logger, format, args...)
}
