// Copyright 2012-2013 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
)

// logging functionality, compatible with the original nats-server.

var trace int32
var debug int32
var nolog int32

// LogSetup will properly setup logging and the logging flags.
func LogSetup() {
	log.SetFlags(0)
	atomic.StoreInt32(&nolog, 0)
	atomic.StoreInt32(&debug, 0)
	atomic.StoreInt32(&trace, 0)
}

// LogInit parses option flags and sets up logging.
func (s *Server) LogInit() {
	// Reset
	LogSetup()

	if s.opts.Logtime {
		log.SetFlags(log.LstdFlags)
	}
	if s.opts.NoLog {
		atomic.StoreInt32(&nolog, 1)
	}
	if s.opts.LogFile != "" {
		flags := os.O_WRONLY | os.O_APPEND | os.O_CREATE
		file, err := os.OpenFile(s.opts.LogFile, flags, 0660)
		if err != nil {
			PrintAndDie(fmt.Sprintf("Error opening logfile: %q", s.opts.LogFile))
		}
		log.SetOutput(file)
	}
	if s.opts.Debug {
		Log(s.opts)
		atomic.StoreInt32(&debug, 1)
		Log("DEBUG is on")
	}
	if s.opts.Trace {
		atomic.StoreInt32(&trace, 1)
		Log("TRACE is on")
	}
}

func alreadyFormatted(s string) bool {
	return strings.HasPrefix(s, "[")
}

func logStr(v []interface{}) string {
	args := make([]string, 0, len(v))
	for _, vt := range v {
		switch t := vt.(type) {
		case string:
			if alreadyFormatted(t) {
				args = append(args, t)
			} else {
				t = strings.Replace(t, "\"", "\\\"", -1)
				args = append(args, fmt.Sprintf("\"%s\"", t))
			}
		default:
			args = append(args, fmt.Sprintf("%+v", vt))
		}
	}
	return fmt.Sprintf("[%s]", strings.Join(args, ", "))
}

func Log(v ...interface{}) {
	if nolog == 0 {
		log.Print(logStr(v))
	}
}

func Logf(format string, v ...interface{}) {
	Log(fmt.Sprintf(format, v...))
}

func Fatal(v ...interface{}) {
	log.Fatalf(logStr(v))
}

func Fatalf(format string, v ...interface{}) {
	Fatal(fmt.Sprintf(format, v...))
}

func Debug(v ...interface{}) {
	if debug > 0 {
		Log(v...)
	}
}

func Debugf(format string, v ...interface{}) {
	if debug > 0 {
		Debug(fmt.Sprintf(format, v...))
	}
}

func Trace(v ...interface{}) {
	if trace > 0 {
		Log(v...)
	}
}

func Tracef(format string, v ...interface{}) {
	if trace > 0 {
		Trace(fmt.Sprintf(format, v...))
	}
}
