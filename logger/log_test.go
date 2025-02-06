// Copyright 2012-2023 The NATS Authors
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
	"log"
	"os"
	"path/filepath"
	"regexp"
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
	tmpDir := t.TempDir()
	file := createFileAtDir(t, tmpDir, "nats-server:log_")
	file.Close()

	logger := NewFileLogger(file.Name(), false, false, false, false)
	defer logger.Close()
	logger.Noticef("foo")

	buf, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatalf("Could not read logfile: %v", err)
	}
	if len(buf) <= 0 {
		t.Fatal("Expected a non-zero length logfile")
	}

	if string(buf) != "[INF] foo\n" {
		t.Fatalf("Expected '%s', received '%s'\n", "[INFO] foo", string(buf))
	}

	file = createFileAtDir(t, tmpDir, "nats-server:log_")
	file.Close()

	logger = NewFileLogger(file.Name(), true, false, true, true)
	defer logger.Close()
	logger.Errorf("foo")

	buf, err = os.ReadFile(file.Name())
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

	date := str[pidEnd:infoStart]
	dateRegExp := "[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}"
	reg, err := regexp.Compile(dateRegExp)
	if err != nil {
		t.Fatalf("Compile date regexp error: %v", err)
	}
	if matched := reg.Match([]byte(date)); !matched {
		t.Fatalf("Date string '%s' does not match '%s'", date, dateRegExp)
	}

	if !strings.HasSuffix(str, "[ERR] foo\n") {
		t.Fatalf("%v", errMsg)
	}
}

func TestFileLoggerSizeLimit(t *testing.T) {
	// Create std logger
	logger := NewStdLogger(true, false, false, false, true)
	if err := logger.SetSizeLimit(1000); err == nil ||
		!strings.Contains(err.Error(), "only for file logger") {
		t.Fatalf("Expected error about being able to use only for file logger, got %v", err)
	}
	logger.Close()

	tmpDir := t.TempDir()

	file := createFileAtDir(t, tmpDir, "log_")
	file.Close()

	logger = NewFileLogger(file.Name(), true, false, false, true)
	defer logger.Close()
	logger.SetSizeLimit(1000)

	for i := 0; i < 50; i++ {
		logger.Noticef("This is a line in the log file")
	}

	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Error reading logs dir: %v", err)
	}
	if len(files) == 1 {
		t.Fatalf("Expected file to have been rotated")
	}
	lastBackup := files[len(files)-1]
	if err := logger.Close(); err != nil {
		t.Fatalf("Error closing log: %v", err)
	}
	content, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatalf("Error loading latest log: %v", err)
	}
	if !bytes.Contains(content, []byte("Rotated log")) ||
		!bytes.Contains(content, []byte(lastBackup.Name())) {
		t.Fatalf("Should be statement about rotated log and backup name, got %s", content)
	}

	tmpDir = t.TempDir()

	// Recreate logger and don't set a limit
	file = createFileAtDir(t, tmpDir, "log_")
	file.Close()
	logger = NewFileLogger(file.Name(), true, false, false, true)
	defer logger.Close()
	for i := 0; i < 50; i++ {
		logger.Noticef("This is line %d in the log file", i+1)
	}
	files, err = os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Error reading logs dir: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("Expected file to not be rotated")
	}

	// Now set a limit that is below current size
	logger.SetSizeLimit(1000)
	// Should have triggered rotation
	files, err = os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Error reading logs dir: %v", err)
	}
	if len(files) <= 1 {
		t.Fatalf("Expected file to have been rotated")
	}
	if err := logger.Close(); err != nil {
		t.Fatalf("Error closing log: %v", err)
	}
	lastBackup = files[len(files)-1]
	content, err = os.ReadFile(file.Name())
	if err != nil {
		t.Fatalf("Error loading latest log: %v", err)
	}
	if !bytes.Contains(content, []byte("Rotated log")) ||
		!bytes.Contains(content, []byte(lastBackup.Name())) {
		t.Fatalf("Should be statement about rotated log and backup name, got %s", content)
	}

	logger = NewFileLogger(file.Name(), true, false, false, true)
	defer logger.Close()
	logger.SetSizeLimit(1000)

	// Check error on rotate.
	logger.Lock()
	logger.fl.Lock()
	failClose := &fileLogFailClose{logger.fl.f, true}
	logger.fl.f = failClose
	logger.fl.Unlock()
	logger.Unlock()
	// Write a big line that will force rotation.
	// Since we fail to close the log file, we should have bumped the limit to 2000
	logger.Noticef("This is a big line: %v", make([]byte, 1000))

	// Remove the failure
	failClose.fail = false
	// Write a big line that makes rotation happen
	logger.Noticef("This is a big line: %v", make([]byte, 2000))
	// Close
	logger.Close()

	files, err = os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Error reading logs dir: %v", err)
	}
	lastBackup = files[len(files)-1]
	content, err = os.ReadFile(filepath.Join(tmpDir, lastBackup.Name()))
	if err != nil {
		t.Fatalf("Error reading backup file: %v", err)
	}
	if !bytes.Contains(content, []byte("on purpose")) || !bytes.Contains(content, []byte("size 2000")) {
		t.Fatalf("Expected error that file could not rotated and max size bumped to 2000, got %s", content)
	}
}

type fileLogFailClose struct {
	writerAndCloser
	fail bool
}

func (l *fileLogFailClose) Close() error {
	if l.fail {
		return fmt.Errorf("on purpose")
	}
	return l.writerAndCloser.Close()
}

func expectOutput(t *testing.T, f func(), expected string) {
	old := os.Stderr // keep backup of the real stderr
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

func createFileAtDir(t *testing.T, dir, prefix string) *os.File {
	t.Helper()
	f, err := os.CreateTemp(dir, prefix)
	if err != nil {
		t.Fatal(err)
	}
	return f
}
