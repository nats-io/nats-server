package unittest

import (
	"fmt"
	"testing"
	"time"

	"github.com/apcera/logging"
	"github.com/apcera/logging/unittest"
)

func init() {
	logging.ReplacePrependFunc("default", func(l *logging.PrependLineData) string {
		return fmt.Sprintf("[%s %s]: ", l.Class, l.TimeStamp.UTC().Format(time.RFC3339Nano))
	})
}

// Set up special logging for unit testing.
var logBuffer unittest.LogBuffer = unittest.SetupBuffer()

// A list of functions that will be run on test completion. Having
// this allows us to clean up temporary directories or files after the
// test is done.
var Finalizers []func() = nil

// Ensures that a test is started from a clean state.
func StartTest(t *testing.T) {
	logBuffer.Clear()
}

// Called as a defer in a test in order to clean up after a test run. All
// tests in this module should call this function as a defer right after
// calling StartTest()
func FinishTest(t *testing.T) {
	for i := range Finalizers {
		Finalizers[len(Finalizers)-1-i]()
	}
	Finalizers = nil
	logBuffer.FinishTest(t)
}
