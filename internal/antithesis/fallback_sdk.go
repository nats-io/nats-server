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

// This build flag must be set for this module to be active.
// In its absence, the NOOP version of this module will be used.
//go:build antithesis_assert

package antithesis

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"testing"
)

// This code is not included in builds nor when running `go test` unless the tag 'antithesis_assert' is present.
// However, this module is included in binaries submitted for testing to Antithesis.
// It safe to turn this module ON for local testing and debugging, in particular to activate assertions defined here.
// Similarly, this can be turned on in build pipelines.

// This module is a "lightweight" (self-contained and dependency-free) implementation Antithesis client
// (because depending on the actual SDK is not practical at this time).
// This module implements key functionality we use in just a few lines of code, without adding dependencies.
// It communicates with the Antithesis runtime via file-based protocol (a.k.a. "Fallback SDK"):
//   https://antithesis.com/docs/using_antithesis/sdk/fallback/
// It is no substitute for the full SDK. For example a `Reachable` assertion requires cataloguing to work properly.
// This module does not implement cataloguing and therefore does not offer a Reachable assertion.

// This module emits events (e.g.: 'setup completed', 'assertion failed'). These are salient inputs for the Antithesis
// runtime. By marking points of interest in the state & execution graph, we can steer exploration to areas of interest
// and flag situations that we want counterexamples for.

// IMPORTANT
// The assertions in this module do NOT  halt execution or exit or fail the test.
// A failed assertion results in:
//  - A message to stdout (test name, assertion line number, `details` map, if provided)
//  - A stack trace of the goroutine which triggered the assertion
//  - If the Antithesis environment is detected at startup, an event is emitted into a designated file

// SHARP EDGES - Things to keep in mind and be aware of
// 1. Assert does not stop execution
// If your test violates an assertion over and over, your stdout will get flooded.
//
// 2. Be wary of side effects (even when using the NOOP variant of this module!)
// Example: `Assert(t, <expression>, ...)`, the expression is always evaluated!
// Example: `Assert(t, someCondition, map[string]{ "server_name" : srv.Name())`, always creates a map and always
//   evaluates `srv.Name()`.
//
// 3. This module locks!
// When an assertion triggers, this module acquires an exclusive lock before emitting the corresponding event to file.
// This may impact your test, depending on where the assertion is placed and how often it fails.
//
// 4. You don't need this module
// If you are debugging a test, try submitting it to Antithesis without as-is. You may get valuable information without
// adding any additional assertions. Caveman debugging (`printf`) still works great in the multiverse.
// This module is useful when a test fails in multiple ways, and you want to zoom in on a specific one.
// You can think of assertions as breakpoints. Add them where you want to stop and figure out how the system reached
// a specific state.

// outputDirEnvVarName The presence of this environment variable implies we are running in Antithesis.
// It is necessary to actually emit event.
// It is ok to set this on for local testing of this module.
// It is not necessary to set this variable to turn on assertions.
const outputDirEnvVarName = "ANTITHESIS_OUTPUT_DIR"

// outputFilename Name of the output file (as per 'Fallback SDK' spec.)
const outputFilename = "sdk.jsonl"

// antithesis The static/singleton state of this module.
var antithesis struct {
	sync.Mutex
	enabled       bool
	outputFile    *os.File
	emittedIdsSet map[string]interface{}
}

// init Static initialization of the module IFF Antithesis environment is detected.
// Open a file handle where events are written to.
func init() {
	outputDir := os.Getenv(outputDirEnvVarName)

	// Fail gracefully to initialize if the environment is not set
	if outputDir == "" {
		fmt.Printf("{*} %s not set. Antithesis Fallback SDK events disabled\n", outputDirEnvVarName)
		return
	}

	// Build the output file path
	outputPath := filepath.Join(outputDir, outputFilename)

	// Create (or truncate) the file
	// Parent directory must exist.
	file, err := os.Create(outputPath)
	if err != nil {
		// If this didn't work, let's not waste time and halt
		panic(fmt.Errorf("failed to create output file %s: %w", outputPath, err))
	}

	antithesis.outputFile = file
	antithesis.emittedIdsSet = make(map[string]interface{})
	antithesis.enabled = true
	fmt.Printf("{*} Antithesis Fallback SDK enabled (%s)\n", outputPath)
}

// SetupCompleted invoke this to inform that until now the test was just setting up. And the interesting part
// starts now. This is a hint for the Antithesis exploration.
// Call this only once per launch.
// If running a single test: call this at the start of the test, or after the setup phase if there is one.
// If running multiple tests: call this in a static `init` block in the package containing the tests.
func SetupCompleted() {

	// Always print a message
	fmt.Printf("{*} Setup completed\n")

	// The rest of this method emits an event for Antithesis, and it's skipped unless
	// the Antithesis environment is detected and successfully initialized.
	if !antithesis.enabled {
		return
	}

	// https://antithesis.com/docs/using_antithesis/sdk/fallback/lifecycle/
	// Example: {"antithesis_setup": { "status": "complete", "details": null }}

	type SetupCompleteEvent struct {
		AntithesisSetup struct {
			Status  string `json:"status"`
			Details any    `json:"details"`
		} `json:"antithesis_setup"`
	}

	e := SetupCompleteEvent{}
	e.AntithesisSetup.Status = "complete"

	emitEvent("setup completed", e)
}

// AssertUnreachable emits an event if it gets invoked (logically equivalent to a constant `false` Assert).
// Place an AssertUnreachable right before calls to `t.Fail()` (or equivalent) for the test you are investigating.
// If Antithesis finds an execution that trips this assertion, the trace of such execution will be captured in the
// report.
func AssertUnreachable(t testing.TB, message string, details map[string]any) {

	// Always print a message
	fmt.Printf("{*} [%s] AssertUnreachable: %s\n", t.Name(), message)
	if details != nil && len(details) > 0 {
		fmt.Printf("{*} Details: %+v\n", details)
	}
	// And the stack trace
	debug.PrintStack()

	// The rest of this method emits an event for Antithesis, and it's skipped unless
	// the Antithesis environment is detected and successfully initialized.
	if !antithesis.enabled {
		return
	}

	// Prefix the test name to the message, this makes report and email more clear.
	// (the same assertion causing a failure in different tests are displayed separately)
	message = fmt.Sprintf("[%s] %s", t.Name(), message)

	// Gather information about where AssertUnreachable is located.
	// This is required data in the event to be emitted.
	callerFunPc, callerFile, callerLine, ok := runtime.Caller(1)
	if !ok {
		panic(fmt.Errorf("failed to determine caller"))
	}
	callerFun := runtime.FuncForPC(callerFunPc)
	if callerFun == nil {
		panic(fmt.Errorf("failed to determine caller function"))
	}

	// Choose a unique id for this assertion
	// Including assert location AND test name means Antithesis will de-duplicate two test that fail on the same
	// assertion (e.g. one located in a common utility function like `checkLeaderPresent`, or similar, called by
	// multiple tests).
	id := createAssertionId(
		t.Name(),
		callerFun.Name(),
		callerFile,
		strconv.Itoa(callerLine),
	)

	// https://antithesis.com/docs/using_antithesis/sdk/fallback/assert/
	// Example:
	//{
	//    "antithesis_assert": {
	//        "hit": true,
	//        "must_hit": false,
	//        "assert_type": "reachability",
	//        "display_type": "Unreachable",
	//        "message": "Control handles null values",
	//        "condition": false,
	//        "id": "Control handles null values",
	//        "location": {
	//            "class": "main",
	//            "function": "main",
	//            "file": "/src/glitch-grid/go/control.go",
	//            "begin_line": 34,
	//            "begin_column": 0
	//        },
	//        "details": null
	//    }
	//}

	type AssertUnreachableEvent struct {
		AntithesisAssert struct {
			Hit         bool   `json:"hit"`
			MustHit     bool   `json:"must_hit"`
			AssertType  string `json:"assert_type"`
			DisplayType string `json:"display_type"`
			Message     string `json:"message"`
			Condition   bool   `json:"condition"`
			Id          string `json:"id"`
			Location    struct {
				Class       string `json:"class"`
				Function    string `json:"function"`
				File        string `json:"file"`
				BeginLine   int    `json:"begin_line"`
				BeginColumn int    `json:"begin_column"`
			} `json:"location"`
			Details any `json:"details"`
		} `json:"antithesis_assert"`
	}

	e := AssertUnreachableEvent{}

	e.AntithesisAssert.Hit = true
	e.AntithesisAssert.MustHit = false
	e.AntithesisAssert.AssertType = "reachability"
	e.AntithesisAssert.DisplayType = "Unreachable"
	e.AntithesisAssert.Message = message
	e.AntithesisAssert.Condition = false
	e.AntithesisAssert.Id = id
	e.AntithesisAssert.Details = details

	e.AntithesisAssert.Location.Class = "test"
	e.AntithesisAssert.Location.Function = callerFun.Name()
	e.AntithesisAssert.Location.File = callerFile
	e.AntithesisAssert.Location.BeginLine = callerLine
	e.AntithesisAssert.Location.BeginColumn = 0

	emitEvent(id, e)
}

// Assert emits an event if `condition` is false.
// Place an Assert at critical junction where you expect some condition to never be false.
// If Antithesis finds an execution that trips this assertion, the trace of such execution will be captured in the
// report.
func Assert(t testing.TB, condition bool, message string, details map[string]any) {

	// Nothing to do unless the condition is false
	if condition {
		return
	}

	// Always print a message
	fmt.Printf("{*} [%s] Assert: %s\n", t.Name(), message)
	if details != nil && len(details) > 0 {
		fmt.Printf("{*} Details: %+v\n", details)
	}
	// And the stack trace
	debug.PrintStack()

	// The rest of this method emits an event for Antithesis, and it's skipped unless
	// the Antithesis environment is detected and successfully initialized.
	if !antithesis.enabled {
		return
	}

	// Prefix the test name to the message, this makes report and email more clear.
	// (the same assertion causing a failure in different tests are displayed separately)
	message = fmt.Sprintf("[%s] %s", t.Name(), message)

	// Gather information about where AssertUnreachable is located.
	// This is required data in the event to be emitted.
	callerFunPc, callerFile, callerLine, ok := runtime.Caller(1)
	if !ok {
		panic(fmt.Errorf("failed to determine caller"))
	}
	callerFun := runtime.FuncForPC(callerFunPc)
	if callerFun == nil {
		panic(fmt.Errorf("failed to determine caller function"))
	}

	// Choose a unique id for this assertion
	// Including assert location AND test name means Antithesis will de-duplicate two test that fail on the same
	// assertion (e.g. one located in a common utility function like `checkLeaderPresent`, or similar, called by
	// multiple tests).
	id := createAssertionId(
		t.Name(),
		callerFun.Name(),
		callerFile,
		strconv.Itoa(callerLine),
	)

	// https://antithesis.com/docs/using_antithesis/sdk/fallback/assert/
	// Example:
	//{
	//    "antithesis_assert": {
	//        "hit": true,
	//        "must_hit": false,
	//        "assert_type": "always",
	//        "display_type": "AlwaysOrUnreachable",
	//        "message": "X is equal to Y",
	//        "condition": false,
	//        "id": "X is equal to Y",
	//        "location": {
	//            "class": "main",
	//            "function": "main",
	//            "file": "/src/glitch-grid/go/control.go",
	//            "begin_line": 34,
	//            "begin_column": 0
	//        },
	//        "details": null
	//    }
	//}

	type AssertAlwaysOrUnreachableEvent struct {
		AntithesisAssert struct {
			Hit         bool   `json:"hit"`
			MustHit     bool   `json:"must_hit"`
			AssertType  string `json:"assert_type"`
			DisplayType string `json:"display_type"`
			Message     string `json:"message"`
			Condition   bool   `json:"condition"`
			Id          string `json:"id"`
			Location    struct {
				Class       string `json:"class"`
				Function    string `json:"function"`
				File        string `json:"file"`
				BeginLine   int    `json:"begin_line"`
				BeginColumn int    `json:"begin_column"`
			} `json:"location"`
			Details any `json:"details"`
		} `json:"antithesis_assert"`
	}

	e := AssertAlwaysOrUnreachableEvent{}

	e.AntithesisAssert.Hit = true
	e.AntithesisAssert.MustHit = false
	e.AntithesisAssert.AssertType = "always"
	e.AntithesisAssert.DisplayType = "AlwaysOrUnreachable"
	e.AntithesisAssert.Message = message
	e.AntithesisAssert.Condition = false
	e.AntithesisAssert.Id = id
	e.AntithesisAssert.Details = details

	e.AntithesisAssert.Location.Class = "test"
	e.AntithesisAssert.Location.Function = callerFun.Name()
	e.AntithesisAssert.Location.File = callerFile
	e.AntithesisAssert.Location.BeginLine = callerLine
	e.AntithesisAssert.Location.BeginColumn = 0

	emitEvent(id, e)
}

// createAssertionId creates a digest of the provided inputs to create a unique identifier for the failed assertion.
// The ID influences how Antithesis groups traces. i.e. are two errors the same or distinct?
// Example: set random ID -> will result in a flood of example for the same line of code
// Example: constant ID shared by all events -> will only produce one example per report
// Anything in between is reasonable, and matter of choice.
// Different grouping may work better for different circumstances.
func createAssertionId(inputs ...string) string {
	h := md5.New()
	total := 0
	for _, input := range inputs {
		n, err := io.WriteString(h, input)
		if err != nil {
			panic(fmt.Errorf("failed to hash input: '%s': %w", input, err))
		}
		total += n
	}
	if total == 0 {
		panic(fmt.Errorf("no inputs to hash"))
	}
	return fmt.Sprintf("%X", h.Sum(nil))
}

// Write an event to the designated file.
func emitEvent(eventId string, event any) {

	// Lock to avoid interleaved writes
	antithesis.Lock()
	defer antithesis.Unlock()

	// Each unique assertion must be emitted only once
	if _, found := antithesis.emittedIdsSet[eventId]; found {
		// This event was already emitted
		return
	}

	// Mark this event id as emitted
	antithesis.emittedIdsSet[eventId] = nil

	// Write event as JSON (single line followed by newline, i.e. JSONL)
	err := json.NewEncoder(antithesis.outputFile).Encode(event)
	if err != nil {
		panic(fmt.Errorf("failed to emit setup completed event: %w", err))
	}

	// Flush before continuing
	err = antithesis.outputFile.Sync()
	if err != nil {
		panic(fmt.Errorf("failed to flush: %w", err))
	}
}
