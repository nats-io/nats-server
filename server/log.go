// Copyright 2012-2014 Apcera Inc. All rights reserved.

package server

var log Logger = &NilLogger{}

type Logger interface {
	Log(format string, v ...interface{})
	Fatal(format string, v ...interface{})
	Debug(format string, v ...interface{})
	Trace(format string, v ...interface{})
}

func (s *Server) SetLogger(logger Logger) {
	log = logger
}

type NilLogger struct{}

func (l *NilLogger) Log(format string, v ...interface{})   {}
func (l *NilLogger) Fatal(format string, v ...interface{}) {}
func (l *NilLogger) Debug(format string, v ...interface{}) {}
func (l *NilLogger) Trace(format string, v ...interface{}) {}
