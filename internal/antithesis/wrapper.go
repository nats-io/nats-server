// Copyright 2022-2024 The NATS Authors
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

package antithesis

import (
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
)

// This package is a thin wrapper around the Antithesis SDK specifically for _tests_.
// In addition to firing the corresponding event to the Antithesis runtime, it:
//  - Prints a stack trace, so each failed test has a clear pointer
//  - Constructs the underlying assertions so that one assert failure per test is surfaced in the report
//    (as opposed to one failure per assert, which would be the default)
//  - Populate the data in a way that is most suitable for the report

// AssertUnreachable This is a NOOP, refer to the actual implementation documentation
func AssertUnreachable(t testing.TB, message string, details map[string]any) {
	// Always print a message
	fmt.Printf("{*} [%s] Assert Unreachable violation: %s\n", t.Name(), message)
	if details != nil && len(details) > 0 {
		fmt.Printf("{*} Details: %+v\n", details)
	}

	// Always print the stack trace
	fmt.Printf("{*} Stack trace:\n")
	debug.PrintStack()

	// N.B. as of today, message (as-is) is the unique identifier of the event
	// Therefore this will de-duplicate the same assertion failing in 2 different tests
	// But not the same assertion failing at 2 different lines of the same test
	messageWithTestName := fmt.Sprintf("[%s] %s\n", t.Name(), message)

	// Fire assertion violation event (if Antithesis is enabled)
	assert.Unreachable(messageWithTestName, details)
}

// Assert Is a standard assertion which raises a flag if the condition is false
func Assert(t testing.TB, condition bool, message string, details map[string]any) {
	// Condition is true, nothing to do
	if condition {
		return
	}

	// Always print a message
	fmt.Printf("{*} [%s] Assert violation: %s\n", t.Name(), message)
	if details != nil && len(details) > 0 {
		fmt.Printf("{*} Details: %+v\n", details)
	}

	// Always print the stack trace
	fmt.Printf("{*} Stack trace:\n")
	debug.PrintStack()

	// N.B. as of today, message (as-is) is the unique identifier of the event
	// Therefore this will de-duplicate the same assertion failing in 2 different tests
	// But not the same assertion failing at 2 different lines of the same test
	messageWithTestName := fmt.Sprintf("[%s] %s\n", t.Name(), message)

	// Fire assertion violation event (if Antithesis is enabled)
	assert.AlwaysOrUnreachable(false, messageWithTestName, details)
}
