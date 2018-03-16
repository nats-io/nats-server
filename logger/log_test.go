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

package logger

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"
)

func TestStdLogger(t *testing.T) {
	logger := NewStdLogger(false, false, false, false, false)

	flags := logger.logger.Flags()
	if flags != 0 {
		t.Fatalf("Expected %q, received %q\n", 0, flags)
	}

	if logger.debug {
		t.Fatalf("Expected %t, received %t\n", false, logger.debug)
	}

	if logger.trace {
		t.Fatalf("Expected %t, received %t\n", false, logger.trace)
	}
}

func TestStdLoggerWithDebugTraceAndTime(t *testing.T) {
	logger := NewStdLogger(true, true, true, false, false)

	flags := logger.logger.Flags()
	if flags != log.LstdFlags|log.Lmicroseconds {
		t.Fatalf("Expected %d, received %d\n", log.LstdFlags, flags)
	}

	if !logger.debug {
		t.Fatalf("Expected %t, received %t\n", true, logger.debug)
	}

	if !logger.trace {
		t.Fatalf("Expected %t, received %t\n", true, logger.trace)
	}
}

func TestStdLoggerNotice(t *testing.T) {
	expectOutput(t, func() {
		logger := NewStdLogger(false, false, false, false, false)
		logger.Noticef("foo")
	}, "[INF] foo\n")
}

func TestStdLoggerNoticeWithColor(t *testing.T) {
	expectOutput(t, func() {
		logger := NewStdLogger(false, false, false, true, false)
		logger.Noticef("foo")
	}, "[\x1b[32mINF\x1b[0m] foo\n")
}

func TestStdLoggerDebug(t *testing.T) {
	expectOutput(t, func() {
		logger := NewStdLogger(false, true, false, false, false)
		logger.Debugf("foo %s", "bar")
	}, "[DBG] foo bar\n")
}

func TestStdLoggerDebugWithOutDebug(t *testing.T) {
	expectOutput(t, func() {
		logger := NewStdLogger(false, false, false, false, false)
		logger.Debugf("foo")
	}, "")
}

func TestStdLoggerTrace(t *testing.T) {
	expectOutput(t, func() {
		logger := NewStdLogger(false, false, true, false, false)
		logger.Tracef("foo")
	}, "[TRC] foo\n")
}

func TestStdLoggerTraceWithOutDebug(t *testing.T) {
	expectOutput(t, func() {
		logger := NewStdLogger(false, false, false, false, false)
		logger.Tracef("foo")
	}, "")
}

func TestFileLogger(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "_gnatsd")
	if err != nil {
		t.Fatal("Could not create tmp dir")
	}
	defer os.RemoveAll(tmpDir)

	file, err := ioutil.TempFile(tmpDir, "gnatsd:log_")
	if err != nil {
		t.Fatalf("Could not create the temp file: %v", err)
	}
	file.Close()

	logger := NewFileLogger(file.Name(), false, false, false, false)
	logger.Noticef("foo")

	buf, err := ioutil.ReadFile(file.Name())
	if err != nil {
		t.Fatalf("Could not read logfile: %v", err)
	}
	if len(buf) <= 0 {
		t.Fatal("Expected a non-zero length logfile")
	}

	if string(buf) != "[INF] foo\n" {
		t.Fatalf("Expected '%s', received '%s'\n", "[INFO] foo", string(buf))
	}

	file, err = ioutil.TempFile(tmpDir, "gnatsd:log_")
	if err != nil {
		t.Fatalf("Could not create the temp file: %v", err)
	}
	file.Close()

	logger = NewFileLogger(file.Name(), true, true, true, true)
	logger.Errorf("foo")

	buf, err = ioutil.ReadFile(file.Name())
	if err != nil {
		t.Fatalf("Could not read logfile: %v", err)
	}
	if len(buf) <= 0 {
		t.Fatal("Expected a non-zero length logfile")
	}
	str := string(buf)
	errMsg := fmt.Sprintf("Expected '%s', received '%s'\n", "[pid] <date> [ERR] foo", str)
	pidEnd := strings.Index(str, " ")
	infoStart := strings.LastIndex(str, "[ERR]")
	if pidEnd == -1 || infoStart == -1 {
		t.Fatalf("%v", errMsg)
	}
	pid := str[0:pidEnd]
	if pid[0] != '[' || pid[len(pid)-1] != ']' {
		t.Fatalf("%v", errMsg)
	}
	//TODO: Parse date.
	if !strings.HasSuffix(str, "[ERR] foo\n") {
		t.Fatalf("%v", errMsg)
	}
}

func expectOutput(t *testing.T, f func(), expected string) {
	old := os.Stderr // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stderr = w

	f()

	outC := make(chan string)
	// copy the output in a separate goroutine so printing can't block indefinitely
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		outC <- buf.String()
	}()

	os.Stderr.Close()
	os.Stderr = old // restoring the real stdout
	out := <-outC
	if out != expected {
		t.Fatalf("Expected '%s', received '%s'\n", expected, out)
	}
}
