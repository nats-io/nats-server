// Copyright 2019-2021 The NATS Authors
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

package testhelper

// These routines need to be accessible in both the server and test
// directories, and tests importing a package don't get exported symbols from
// _test.go files in the imported package, so we put them here where they can
// be used freely.

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

type DummyLogger struct {
	sync.Mutex
	Msg     string
	AllMsgs []string
}

func (l *DummyLogger) CheckContent(t *testing.T, expectedStr string) {
	t.Helper()
	l.Lock()
	defer l.Unlock()
	if l.Msg != expectedStr {
		t.Fatalf("Expected log to be: %v, got %v", expectedStr, l.Msg)
	}
}

func (l *DummyLogger) aggregate() {
	if l.AllMsgs != nil {
		l.AllMsgs = append(l.AllMsgs, l.Msg)
	}
}

func (l *DummyLogger) Noticef(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	l.Msg = fmt.Sprintf(format, v...)
	l.aggregate()
}
func (l *DummyLogger) Errorf(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	l.Msg = fmt.Sprintf(format, v...)
	l.aggregate()
}
func (l *DummyLogger) Warnf(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	l.Msg = fmt.Sprintf(format, v...)
	l.aggregate()
}
func (l *DummyLogger) Fatalf(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	l.Msg = fmt.Sprintf(format, v...)
	l.aggregate()
}
func (l *DummyLogger) Debugf(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	l.Msg = fmt.Sprintf(format, v...)
	l.aggregate()
}
func (l *DummyLogger) Tracef(format string, v ...interface{}) {
	l.Lock()
	defer l.Unlock()
	l.Msg = fmt.Sprintf(format, v...)
	l.aggregate()
}

// NewDummyLogger creates a dummy logger and allows to ask for logs to be
// retained instead of just keeping the most recent. Use retain to provide an
// initial size estimate on messages (not to provide a max capacity).
func NewDummyLogger(retain uint) *DummyLogger {
	l := &DummyLogger{}
	if retain > 0 {
		l.AllMsgs = make([]string, 0, retain)
	}
	return l
}

func (l *DummyLogger) Drain() {
	l.Lock()
	defer l.Unlock()
	if l.AllMsgs == nil {
		return
	}
	l.AllMsgs = make([]string, 0, len(l.AllMsgs))
}

func (l *DummyLogger) CheckForProhibited(t *testing.T, reason, needle string) {
	t.Helper()
	l.Lock()
	defer l.Unlock()

	if l.AllMsgs == nil {
		t.Fatal("DummyLogger.CheckForProhibited called without AllMsgs being collected")
	}

	// Collect _all_ matches, rather than have to re-test repeatedly.
	// This will particularly help with less deterministic tests with multiple matches.
	shouldFail := false
	for i := range l.AllMsgs {
		if strings.Contains(l.AllMsgs[i], needle) {
			t.Errorf("log contains %s: %v", reason, l.AllMsgs[i])
			shouldFail = true
		}
	}
	if shouldFail {
		t.FailNow()
	}
}
