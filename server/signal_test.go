// Copyright 2017 Apcera Inc. All rights reserved.
// +build !windows

package server

import (
	"io/ioutil"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/logger"
)

func TestSignalToReOpenLogFile(t *testing.T) {
	logFile := "test.log"
	defer os.Remove(logFile)
	defer os.Remove(logFile + ".bak")
	opts := &Options{
		Host:    "localhost",
		Port:    -1,
		NoSigs:  false,
		LogFile: logFile,
	}
	s := RunServer(opts)
	defer s.SetLogger(nil, false, false)
	defer s.Shutdown()

	// Set the file log
	fileLog := logger.NewFileLogger(s.opts.LogFile, s.opts.Logtime, s.opts.Debug, s.opts.Trace, true)
	s.SetLogger(fileLog, false, false)

	// Add a trace
	expectedStr := "This is a Notice"
	s.Noticef(expectedStr)
	buf, err := ioutil.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	if !strings.Contains(string(buf), expectedStr) {
		t.Fatalf("Expected log to contain %q, got %q", expectedStr, string(buf))
	}
	// Rename the file
	if err := os.Rename(logFile, logFile+".bak"); err != nil {
		t.Fatalf("Unable to rename file: %v", err)
	}
	// This should cause file to be reopened.
	syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
	// Wait a bit for action to be performed
	time.Sleep(500 * time.Millisecond)
	buf, err = ioutil.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	expectedStr = "File log re-opened"
	if !strings.Contains(string(buf), expectedStr) {
		t.Fatalf("Expected log to contain %q, got %q", expectedStr, string(buf))
	}
}
