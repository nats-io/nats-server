// Copyright 2012-2018 The NATS Authors
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
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/nats-io/gnatsd/logger"
)

func TestSetLogger(t *testing.T) {
	server := &Server{}
	defer server.SetLogger(nil, false, false)
	dl := &DummyLogger{}
	server.SetLogger(dl, true, true)

	// We assert that the logger has change to the DummyLogger
	_ = server.logging.logger.(*DummyLogger)

	if server.logging.debug != 1 {
		t.Fatalf("Expected debug 1, received value %d\n", server.logging.debug)
	}

	if server.logging.trace != 1 {
		t.Fatalf("Expected trace 1, received value %d\n", server.logging.trace)
	}

	// Check traces
	expectedStr := "This is a Notice"
	server.Noticef(expectedStr)
	dl.checkContent(t, expectedStr)
	expectedStr = "This is an Error"
	server.Errorf(expectedStr)
	dl.checkContent(t, expectedStr)
	expectedStr = "This is a Fatal"
	server.Fatalf(expectedStr)
	dl.checkContent(t, expectedStr)
	expectedStr = "This is a Debug"
	server.Debugf(expectedStr)
	dl.checkContent(t, expectedStr)
	expectedStr = "This is a Trace"
	server.Tracef(expectedStr)
	dl.checkContent(t, expectedStr)

	// Make sure that we can reset to fal
	server.SetLogger(dl, false, false)
	if server.logging.debug != 0 {
		t.Fatalf("Expected debug 0, got %v", server.logging.debug)
	}
	if server.logging.trace != 0 {
		t.Fatalf("Expected trace 0, got %v", server.logging.trace)
	}
	// Now, Debug and Trace should not produce anything
	dl.msg = ""
	server.Debugf("This Debug should not be traced")
	dl.checkContent(t, "")
	server.Tracef("This Trace should not be traced")
	dl.checkContent(t, "")
}

type DummyLogger struct {
	sync.Mutex
	msg string
}

func (l *DummyLogger) checkContent(t *testing.T, expectedStr string) {
	l.Lock()
	defer l.Unlock()
	if l.msg != expectedStr {
		stackFatalf(t, "Expected log to be: %v, got %v", expectedStr, l.msg)
	}
}

func (l *DummyLogger) Noticef(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	l.msg = fmt.Sprintf(format, v...)
}
func (l *DummyLogger) Errorf(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	l.msg = fmt.Sprintf(format, v...)
}
func (l *DummyLogger) Fatalf(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	l.msg = fmt.Sprintf(format, v...)
}
func (l *DummyLogger) Debugf(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	l.msg = fmt.Sprintf(format, v...)
}
func (l *DummyLogger) Tracef(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	l.msg = fmt.Sprintf(format, v...)
}

func TestReOpenLogFile(t *testing.T) {
	// We can't rename the file log when still opened on Windows, so skip
	if runtime.GOOS == "windows" {
		t.SkipNow()
	}
	s := &Server{opts: &Options{}}
	defer s.SetLogger(nil, false, false)

	// First check with no logger
	s.SetLogger(nil, false, false)
	s.ReOpenLogFile()

	// Then when LogFile is not provided.
	dl := &DummyLogger{}
	s.SetLogger(dl, false, false)
	s.ReOpenLogFile()
	dl.checkContent(t, "File log re-open ignored, not a file logger")

	// Set a File log
	s.opts.LogFile = "test.log"
	defer os.Remove(s.opts.LogFile)
	defer os.Remove(s.opts.LogFile + ".bak")
	fileLog := logger.NewFileLogger(s.opts.LogFile, s.opts.Logtime, s.opts.Debug, s.opts.Trace, true)
	s.SetLogger(fileLog, false, false)
	// Add some log
	expectedStr := "This is a Notice"
	s.Noticef(expectedStr)
	// Check content of log
	buf, err := ioutil.ReadFile(s.opts.LogFile)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	if !strings.Contains(string(buf), expectedStr) {
		t.Fatalf("Expected log to contain: %q, got %q", expectedStr, string(buf))
	}
	// Close the file and rename it
	if err := os.Rename(s.opts.LogFile, s.opts.LogFile+".bak"); err != nil {
		t.Fatalf("Unable to rename log file: %v", err)
	}
	// Now re-open LogFile
	s.ReOpenLogFile()
	// Content should indicate that we have re-opened the log
	buf, err = ioutil.ReadFile(s.opts.LogFile)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	if strings.HasSuffix(string(buf), "File log-reopened") {
		t.Fatalf("File should indicate that file log was re-opened, got: %v", string(buf))
	}
	// Make sure we can append to the log
	s.Noticef("New message")
	buf, err = ioutil.ReadFile(s.opts.LogFile)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	if strings.HasSuffix(string(buf), "New message") {
		t.Fatalf("New message was not appended after file was re-opened, got: %v", string(buf))
	}
}
