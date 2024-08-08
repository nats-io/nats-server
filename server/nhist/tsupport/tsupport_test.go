
/*
	Abstract:	Unit tests for test support.

	Date:		3 May 2024
	Author:		E. Scott Daniels
*/

package tsupport

import(
	"fmt"
	"os"
	"strconv"
	"testing"
)

/*
	WARNING: these tests will NOT pass; it's not intended to!
*/
func TestTsupport( t *testing.T ) {
	tsp := GetTesty( t )
	if tsp == nil {
		fmt.Fprintf( os.Stderr, "[FAIL] didnt' get testy instance" )
		t.FailNow()		// don't continue
	}

	tsp.Ensure( false, "drive the %v case\n", false )
	tsp.Ensure( true, "drive the %v case\n", true )

	_, err := strconv.Atoi( "foo" )		// expect error
	tsp.EnsureErr( err, "did not error converting %s\n", "foo" )
	tsp.EnsureNotErr( err, "errored error converting %s\n", "foo" )

	tsp.PanicOnErr( err, "this should panic\n" )
	fmt.Fprintf( os.Stderr, "this message should never print\n" )
}

/*
	The panic that could not be tested above.
*/
func TestPanic( t *testing.T ) {
	tsp := GetTesty( t )
	if tsp == nil {
		fmt.Fprintf( os.Stderr, "[FAIL] didnt' get testy instance" )
		t.FailNow()		// don't continue
	}

	tsp.PanicIf( false, "I'm feeling a bit like having an attak\n" )
}
