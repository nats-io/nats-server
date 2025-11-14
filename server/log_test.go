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
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/internal/testhelper"
	"github.com/nats-io/nats-server/v2/logger"
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
	dl.CheckContent(t, expectedStr)
	expectedStr = "This is an Error"
	server.Errorf(expectedStr)
	dl.CheckContent(t, expectedStr)
	expectedStr = "This is a Fatal"
	server.Fatalf(expectedStr)
	dl.CheckContent(t, expectedStr)
	expectedStr = "This is a Debug"
	server.Debugf(expectedStr)
	dl.CheckContent(t, expectedStr)
	expectedStr = "This is a Trace"
	server.Tracef(expectedStr)
	dl.CheckContent(t, expectedStr)
	expectedStr = "This is a Warning"
	server.Tracef(expectedStr)
	dl.CheckContent(t, expectedStr)

	// Make sure that we can reset to fal
	server.SetLogger(dl, false, false)
	if server.logging.debug != 0 {
		t.Fatalf("Expected debug 0, got %v", server.logging.debug)
	}
	if server.logging.trace != 0 {
		t.Fatalf("Expected trace 0, got %v", server.logging.trace)
	}
	// Now, Debug and Trace should not produce anything
	dl.Msg = ""
	server.Debugf("This Debug should not be traced")
	dl.CheckContent(t, "")
	server.Tracef("This Trace should not be traced")
	dl.CheckContent(t, "")
}

type DummyLogger = testhelper.DummyLogger

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
	dl.CheckContent(t, "File log re-open ignored, not a file logger")

	// Set a File log
	s.opts.LogFile = filepath.Join(t.TempDir(), "test.log")
	fileLog := logger.NewFileLogger(s.opts.LogFile, s.opts.Logtime, s.opts.Debug, s.opts.Trace, true, logger.LogUTC(s.opts.LogtimeUTC))
	s.SetLogger(fileLog, false, false)
	// Add some log
	expectedStr := "This is a Notice"
	s.Noticef(expectedStr)
	// Check content of log
	buf, err := os.ReadFile(s.opts.LogFile)
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
	buf, err = os.ReadFile(s.opts.LogFile)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	if strings.HasSuffix(string(buf), "File log-reopened") {
		t.Fatalf("File should indicate that file log was re-opened, got: %v", string(buf))
	}
	// Make sure we can append to the log
	s.Noticef("New message")
	buf, err = os.ReadFile(s.opts.LogFile)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	if strings.HasSuffix(string(buf), "New message") {
		t.Fatalf("New message was not appended after file was re-opened, got: %v", string(buf))
	}
}

func TestFileLoggerSizeLimitAndReopen(t *testing.T) {
	file := createTempFile(t, "log_")
	file.Close()

	s := &Server{opts: &Options{}}
	defer s.SetLogger(nil, false, false)

	// Set a File log
	s.opts.LogFile = file.Name()
	s.opts.Logtime = true
	s.opts.LogSizeLimit = 1000
	s.ConfigureLogger()

	// Add a trace
	s.Noticef("this is a notice")

	// Do a re-open...
	s.ReOpenLogFile()

	// Content should indicate that we have re-opened the log
	buf, err := os.ReadFile(s.opts.LogFile)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	if strings.HasSuffix(string(buf), "File log-reopened") {
		t.Fatalf("File should indicate that file log was re-opened, got: %v", string(buf))
	}

	// Now make sure that the limit is still honored.
	txt := make([]byte, 800)
	for i := 0; i < len(txt); i++ {
		txt[i] = 'A'
	}
	s.Noticef(string(txt))
	for i := 0; i < len(txt); i++ {
		txt[i] = 'B'
	}
	s.Noticef(string(txt))

	buf, err = os.ReadFile(s.opts.LogFile)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	sbuf := string(buf)
	if strings.Contains(sbuf, "AAAAA") || strings.Contains(sbuf, "BBBBB") {
		t.Fatalf("Looks like file was not rotated: %s", sbuf)
	}
	if !strings.Contains(sbuf, "Rotated log, backup saved") {
		t.Fatalf("File should have been rotated, was not: %s", sbuf)
	}
}

func TestNoPasswordsFromConnectTrace(t *testing.T) {
	opts := DefaultOptions()
	opts.NoLog = false
	opts.Trace = true
	opts.Username = "derek"
	opts.Password = "s3cr3t"
	opts.PingInterval = 2 * time.Minute
	setBaselineOptions(opts)
	s := &Server{opts: opts}
	dl := testhelper.NewDummyLogger(100)
	s.SetLogger(dl, false, true)

	_ = s.logging.logger.(*DummyLogger)
	if s.logging.trace != 1 {
		t.Fatalf("Expected trace 1, received value %d\n", s.logging.trace)
	}
	defer s.SetLogger(nil, false, false)

	c, _, _ := newClientForServer(s)
	defer c.close()

	connectOp := []byte("CONNECT {\"user\":\"derek\",\"pass\":\"s3cr3t\"}\r\n")
	err := c.parse(connectOp)
	if err != nil {
		t.Fatalf("Received error: %v\n", err)
	}

	dl.CheckForProhibited(t, "password found", "s3cr3t")
}

func TestRemovePassFromTrace(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "user and pass",
			input:    "CONNECT {\"user\":\"derek\",\"pass\":\"s3cr3t\"}\r\n",
			expected: "CONNECT {\"user\":\"derek\",\"pass\":\"[REDACTED]\"}\r\n",
		},
		{
			name:     "user and pass extra space",
			input:    "CONNECT {\"user\":\"derek\",\"pass\":  \"s3cr3t\"}\r\n",
			expected: "CONNECT {\"user\":\"derek\",\"pass\":  \"[REDACTED]\"}\r\n",
		},
		{
			name:     "user and pass is empty",
			input:    "CONNECT {\"user\":\"derek\",\"pass\":\"\"}\r\n",
			expected: "CONNECT {\"user\":\"derek\",\"pass\":\"[REDACTED]\"}\r\n",
		},
		{
			name:     "user and pass is empty whitespace",
			input:    "CONNECT {\"user\":\"derek\",\"pass\":\"               \"}\r\n",
			expected: "CONNECT {\"user\":\"derek\",\"pass\":\"[REDACTED]\"}\r\n",
		},
		{
			name:     "user and pass whitespace",
			input:    "CONNECT {\"user\":\"derek\",\"pass\":    \"s3cr3t\"     }\r\n",
			expected: "CONNECT {\"user\":\"derek\",\"pass\":    \"[REDACTED]\"     }\r\n",
		},
		{
			name:     "only pass",
			input:    "CONNECT {\"pass\":\"s3cr3t\",}\r\n",
			expected: "CONNECT {\"pass\":\"[REDACTED]\",}\r\n",
		},
		{
			name:     "invalid json",
			input:    "CONNECT {pass:s3cr3t ,   password =  s3cr3t}",
			expected: "CONNECT {pass:[REDACTED],   password =  s3cr3t}",
		},
		{
			name:     "invalid json no whitespace after key",
			input:    "CONNECT {pass:s3cr3t ,   password=  s3cr3t}",
			expected: "CONNECT {pass:[REDACTED],   password=  s3cr3t}",
		},
		{
			name:     "both pass and wrong password key",
			input:    `CONNECT {"pass":"s3cr3t4", "password": "s3cr3t4"}`,
			expected: `CONNECT {"pass":"[REDACTED]", "password": "s3cr3t4"}`,
		},
		{
			name:     "invalid json",
			input:    "CONNECT {user = hello, password =  s3cr3t}",
			expected: "CONNECT {user = hello, password =  [REDACTED]}",
		},
		{
			name:     "complete connect",
			input:    "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"foo\",\"pass\":\"s3cr3t\",\"tls_required\":false,\"name\":\"APM7JU94z77YzP6WTBEiuw\"}\r\n",
			expected: "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"foo\",\"pass\":\"[REDACTED]\",\"tls_required\":false,\"name\":\"APM7JU94z77YzP6WTBEiuw\"}\r\n",
		},
		{
			name:     "invalid json with only pass key",
			input:    "CONNECT {pass:s3cr3t\r\n",
			expected: "CONNECT {pass:[REDACTED]\r\n",
		},
		{
			name:     "invalid password key also filtered",
			input:    "CONNECT {\"password\":\"s3cr3t\",}\r\n",
			expected: "CONNECT {\"password\":\"[REDACTED]\",}\r\n",
		},
		{
			name:     "single long password with whitespace",
			input:    "CONNECT {\"pass\":\"secret password which is very long\",}\r\n",
			expected: "CONNECT {\"pass\":\"[REDACTED]\",}\r\n",
		},
		{
			name:     "single long pass key is filtered",
			input:    "CONNECT {\"pass\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\"}\r\n",
			expected: "CONNECT {\"pass\":\"[REDACTED]\"}\r\n",
		},
		{
			name:     "duplicate keys only filtered once",
			input:    "CONNECT {\"pass\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"pass\":\"BBBBBBBBBBBBBBBBBBBB\",\"password\":\"CCCCCCCCCCCCCCCC\"}\r\n",
			expected: "CONNECT {\"pass\":\"[REDACTED]\",\"pass\":\"BBBBBBBBBBBBBBBBBBBB\",\"password\":\"CCCCCCCCCCCCCCCC\"}\r\n",
		},
		{
			name:     "invalid json with multiple keys only one is filtered",
			input:    "CONNECT {pass = \"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\",pass= \"BBBBBBBBBBBBBBBBBBBB\",password =\"CCCCCCCCCCCCCCCC\"}\r\n",
			expected: "CONNECT {pass = \"[REDACTED]\",pass= \"BBBBBBBBBBBBBBBBBBBB\",password =\"CCCCCCCCCCCCCCCC\"}\r\n",
		},
		{
			name:     "complete connect protocol",
			input:    "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"foo\",\"pass\":\"s3cr3t\",\"tls_required\":false,\"name\":\"APM7JU94z77YzP6WTBEiuw\"}\r\n",
			expected: "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"foo\",\"pass\":\"[REDACTED]\",\"tls_required\":false,\"name\":\"APM7JU94z77YzP6WTBEiuw\"}\r\n",
		},
		{
			name:     "user and pass are filterered",
			input:    "CONNECT {\"user\":\"s3cr3t\",\"pass\":\"s3cr3t\"}\r\n",
			expected: "CONNECT {\"user\":\"s3cr3t\",\"pass\":\"[REDACTED]\"}\r\n",
		},
		{
			name:     "complete connect using password key with user and password being the same",
			input:    "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"s3cr3t\",\"pass\":\"s3cr3t\",\"tls_required\":false,\"name\":\"...\"}\r\n",
			expected: "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"s3cr3t\",\"pass\":\"[REDACTED]\",\"tls_required\":false,\"name\":\"...\"}\r\n",
		},
		{
			name:     "complete connect with user password and name all the same",
			input:    "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"s3cr3t\",\"pass\":\"s3cr3t\",\"tls_required\":false,\"name\":\"s3cr3t\"}\r\n",
			expected: "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"s3cr3t\",\"pass\":\"[REDACTED]\",\"tls_required\":false,\"name\":\"s3cr3t\"}\r\n",
		},
		{
			name:     "complete connect extra white space at the beginning",
			input:    "CONNECT 	 {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"s3cr3t\",\"pass\":\"s3cr3t\",\"tls_required\":false,\"name\":\"foo\"}\r\n",
			expected: "CONNECT 	 {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"s3cr3t\",\"pass\":\"[REDACTED]\",\"tls_required\":false,\"name\":\"foo\"}\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output := removeSecretsFromTrace([]byte(test.input))
			if !bytes.Equal(output, []byte(test.expected)) {
				t.Errorf("\nExpected %q\n    got: %q", test.expected, string(output))
			}
		})
	}
}

func TestRemoveAuthTokenFromTrace(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "user and auth_token",
			input:    "CONNECT {\"user\":\"derek\",\"auth_token\":\"s3cr3t\"}\r\n",
			expected: "CONNECT {\"user\":\"derek\",\"auth_token\":\"[REDACTED]\"}\r\n",
		},
		{
			name:     "user and pass extra space",
			input:    "CONNECT {\"user\":\"derek\",\"auth_token\":  \"s3cr3t\"}\r\n",
			expected: "CONNECT {\"user\":\"derek\",\"auth_token\":  \"[REDACTED]\"}\r\n",
		},
		{
			name:     "user and pass is empty",
			input:    "CONNECT {\"user\":\"derek\",\"auth_token\":\"\"}\r\n",
			expected: "CONNECT {\"user\":\"derek\",\"auth_token\":\"[REDACTED]\"}\r\n",
		},
		{
			name:     "user and pass is empty whitespace",
			input:    "CONNECT {\"user\":\"derek\",\"auth_token\":\"               \"}\r\n",
			expected: "CONNECT {\"user\":\"derek\",\"auth_token\":\"[REDACTED]\"}\r\n",
		},
		{
			name:     "user and pass whitespace",
			input:    "CONNECT {\"user\":\"derek\",\"auth_token\":    \"s3cr3t\"     }\r\n",
			expected: "CONNECT {\"user\":\"derek\",\"auth_token\":    \"[REDACTED]\"     }\r\n",
		},
		{
			name:     "only pass",
			input:    "CONNECT {\"auth_token\":\"s3cr3t\",}\r\n",
			expected: "CONNECT {\"auth_token\":\"[REDACTED]\",}\r\n",
		},
		{
			name:     "invalid json",
			input:    "CONNECT {auth_token:s3cr3t ,   password =  s3cr3t}",
			expected: "CONNECT {auth_token:[REDACTED],   password =  [REDACTED]}",
		},
		{
			name:     "invalid json no whitespace after key",
			input:    "CONNECT {auth_token:s3cr3t ,   password=  s3cr3t}",
			expected: "CONNECT {auth_token:[REDACTED],   password=  [REDACTED]}",
		},
		{
			name:     "both pass and wrong password key",
			input:    `CONNECT {"auth_token":"s3cr3t4", "password": "s3cr3t4"}`,
			expected: `CONNECT {"auth_token":"[REDACTED]", "password": "[REDACTED]"}`,
		},
		{
			name:     "invalid json",
			input:    "CONNECT {user = hello, auth_token =  s3cr3t}",
			expected: "CONNECT {user = hello, auth_token =  [REDACTED]}",
		},
		{
			name:     "complete connect",
			input:    "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"auth_token\":\"s3cr3t\",\"tls_required\":false,\"name\":\"APM7JU94z77YzP6WTBEiuw\"}\r\n",
			expected: "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"auth_token\":\"[REDACTED]\",\"tls_required\":false,\"name\":\"APM7JU94z77YzP6WTBEiuw\"}\r\n",
		},
		{
			name:     "invalid json with only pass key",
			input:    "CONNECT {auth_token:s3cr3t\r\n",
			expected: "CONNECT {auth_token:[REDACTED]\r\n",
		},
		{
			name:     "invalid password key also filtered",
			input:    "CONNECT {\"auth_token\":\"s3cr3t\",}\r\n",
			expected: "CONNECT {\"auth_token\":\"[REDACTED]\",}\r\n",
		},
		{
			name:     "single long password with whitespace",
			input:    "CONNECT {\"auth_token\":\"secret password which is very long\",}\r\n",
			expected: "CONNECT {\"auth_token\":\"[REDACTED]\",}\r\n",
		},
		{
			name:     "single long pass key is filtered",
			input:    "CONNECT {\"auth_token\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\"}\r\n",
			expected: "CONNECT {\"auth_token\":\"[REDACTED]\"}\r\n",
		},
		{
			name:     "duplicate keys only filtered once",
			input:    "CONNECT {\"auth_token\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"auth_token\":\"BBB\",\"pass\":\"BBBBBBBBBBBBBBBBBBBB\",\"password\":\"CCCCCCCCCCCCCCCC\"}\r\n",
			expected: "CONNECT {\"auth_token\":\"[REDACTED]\",\"auth_token\":\"BBB\",\"pass\":\"[REDACTED]\",\"password\":\"CCCCCCCCCCCCCCCC\"}\r\n",
		},
		{
			name:     "invalid json with multiple keys only one is filtered",
			input:    "CONNECT {auth_token = \"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\",pass= \"BBBBBBBBBBBBBBBBBBBB\",password =\"CCCCCCCCCCCCCCCC\"}\r\n",
			expected: "CONNECT {auth_token = \"[REDACTED]\",pass= \"[REDACTED]\",password =\"CCCCCCCCCCCCCCCC\"}\r\n",
		},
		{
			name:     "complete connect protocol",
			input:    "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"foo\",\"auth_token\":\"s3cr3t\",\"tls_required\":false,\"name\":\"APM7JU94z77YzP6WTBEiuw\"}\r\n",
			expected: "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"foo\",\"auth_token\":\"[REDACTED]\",\"tls_required\":false,\"name\":\"APM7JU94z77YzP6WTBEiuw\"}\r\n",
		},
		{
			name:     "user and token are filterered",
			input:    "CONNECT {\"user\":\"s3cr3t\",\"auth_token\":\"s3cr3t\"}\r\n",
			expected: "CONNECT {\"user\":\"s3cr3t\",\"auth_token\":\"[REDACTED]\"}\r\n",
		},
		{
			name:     "complete connect using token key with user and token being the same",
			input:    "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"s3cr3t\",\"auth_token\":\"s3cr3t\",\"tls_required\":false,\"name\":\"...\"}\r\n",
			expected: "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"s3cr3t\",\"auth_token\":\"[REDACTED]\",\"tls_required\":false,\"name\":\"...\"}\r\n",
		},
		{
			name:     "complete connect with user, token and name all the same",
			input:    "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"s3cr3t\",\"auth_token\":\"s3cr3t\",\"tls_required\":false,\"name\":\"s3cr3t\"}\r\n",
			expected: "CONNECT {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"s3cr3t\",\"auth_token\":\"[REDACTED]\",\"tls_required\":false,\"name\":\"s3cr3t\"}\r\n",
		},
		{
			name:     "complete connect extra white space at the beginning",
			input:    "CONNECT 	 {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"s3cr3t\",\"auth_token\":\"s3cr3t\",\"tls_required\":false,\"name\":\"foo\"}\r\n",
			expected: "CONNECT 	 {\"echo\":true,\"verbose\":false,\"pedantic\":false,\"user\":\"s3cr3t\",\"auth_token\":\"[REDACTED]\",\"tls_required\":false,\"name\":\"foo\"}\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output := removeSecretsFromTrace([]byte(test.input))
			if !bytes.Equal(output, []byte(test.expected)) {
				t.Errorf("\nExpected %q\n    got: %q", test.expected, string(output))
			}
		})
	}
}
