// Copyright 2012 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"log"
	"strings"
)

// logging functionality, compatible with original nats-server

var trace bool
var debug bool
var nolog bool

func LogSetup() {
	log.SetFlags(0)
}

func (s *Server) LogInit() {
	if s.opts.Logtime {
		log.SetFlags(log.LstdFlags)
	}
	if s.opts.NoLog {
		nolog = true
	}
	if s.opts.Debug {
		Log(s.opts)
		debug = true
		Log("DEBUG is on")
	}
	if s.opts.Trace {
		trace = true
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
	return fmt.Sprintf("[%s]", strings.Join(args,", "))
}

func Log(v ...interface{}) {
	if !nolog {
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
	if debug {
		Log(v...)
	}
}

func Debugf(format string, v ...interface{}) {
	if debug {
		Debug(fmt.Sprintf(format, v...))
	}
}

func Trace(v ...interface{}) {
	if trace {
		Log(v...)
	}
}

func Tracef(format string, v ...interface{}) {
	if trace {
		Trace(fmt.Sprintf(format, v...))
	}
}

