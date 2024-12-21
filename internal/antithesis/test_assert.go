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

// This file is used iff the `enable_antithesis_sdk` build tag is present
//go:build enable_antithesis_sdk

package antithesis

import (
	"encoding/json"
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
)

// This file provides assertions utility functions suitable for use in tests.
// It is a thin wrapper around the Antithesis SDK.
//
// Notice that unlike other assertions libraries, a violation does not halt execution or fail the test.
// The effects of violating an assertion, are:
//  1. Print the violation message, prefixed by the test name in which it happened
//  2. Print the stack for the calling goroutine so each failed test has a clear trace of the failure
//  3. Invoke the underlying Antithesis assertion
//
// N.B. Enabling this module outside for tests running outside of Antithesis will enable 1 and 2 above, but not 3.
// therefore it can be useful to output additional test failure details when running tests locally or in CI.

// AssertUnreachable is used to flag code branches that should not get invoked ever.
// Example:
//
// pubAck, err := js.Publish(...)
//
//	if err != nil {
//	    antithesis.AssertUnreachable(t, "Publish failed", map[string]any{"error": err.Error()})
//	    t.Fatalf("Publish failed with error: %s", err)
//	}
func AssertUnreachable(t testing.TB, message string, details map[string]any) {
	// Always print a message
	fmt.Printf("{*} [%s] Assert Unreachable violation: %s\n", t.Name(), message)
	if details != nil && len(details) > 0 {
		fmt.Printf("{*} Details:\n")
		jsonDetails, err := json.MarshalIndent(details, "", "  ")
		if err != nil {
			panic(err)
		}
		fmt.Println(string(jsonDetails))
	}

	// Always print the stack trace
	fmt.Printf("{*} Stack trace:\n")
	debug.PrintStack()

	// N.B. as of today, message (as-is) is the unique identifier of the event
	// Therefore this will de-duplicate the same assertion failing in 2 different tests
	// But not the same assertion failing at 2 different lines of the same test
	messageWithTestName := fmt.Sprintf("[%s] %s", t.Name(), message)

	// Fire assertion violation event (if Antithesis is enabled)
	assert.Unreachable(messageWithTestName, details)
}

// Assert is used to check that some given condition is always true,
// Example:
//
//	antithesis.Assert(t, sequence > lastSequence, "Non-monotonic stream sequence number", map[string]any{
//	    "stream": streamName,
//	    "connection_id": nc.Id(),
//	    "sequence": sequence,
//	    "lastSequence": lastSequence,
//	})
func Assert(t testing.TB, condition bool, message string, details map[string]any) {
	// Condition is true, nothing to do
	if condition {
		return
	}

	// Always print a message
	fmt.Printf("{*} [%s] Assert violation: %s\n", t.Name(), message)
	if details != nil && len(details) > 0 {
		fmt.Printf("{*} Details:\n")
		jsonDetails, err := json.MarshalIndent(details, "", "  ")
		if err != nil {
			panic(err)
		}
		fmt.Println(string(jsonDetails))
	}

	// Always print the stack trace
	fmt.Printf("{*} Stack trace:\n")
	debug.PrintStack()

	// N.B. as of today, message (as-is) is the unique identifier of the event
	// Therefore this will de-duplicate the same assertion failing in 2 different tests
	// But not the same assertion failing at 2 different lines of the same test
	messageWithTestName := fmt.Sprintf("[%s] %s", t.Name(), message)

	// Fire assertion violation event (if Antithesis is enabled)
	assert.AlwaysOrUnreachable(false, messageWithTestName, details)
}
