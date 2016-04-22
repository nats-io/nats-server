// Copyright 2016 Apcera Inc. All rights reserved.

package test

import (
	"fmt"
	"strings"
	"testing"
)

type dummyLogger struct {
	msg string
}

func (d *dummyLogger) Fatalf(format string, args ...interface{}) {
	d.msg = fmt.Sprintf(format, args...)

}
func (d *dummyLogger) Errorf(format string, args ...interface{}) {
}

func TestStackFatal(t *testing.T) {
	d := &dummyLogger{}
	stackFatalf(d, "test stack %d", 1)
	if !strings.HasPrefix(d.msg, "test stack 1") {
		t.Fatalf("Unexpected start of stack: %v", d.msg)
	}
	if !strings.Contains(d.msg, "test_test.go") {
		t.Fatalf("Unexpected stack: %v", d.msg)
	}
}
