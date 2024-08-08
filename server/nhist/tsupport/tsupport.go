
/*
	Abstract:	Useful unit test helper structs and functions.

	Date:		3 May 2024
	Author:		E. Scott Daniels
*/

package tsupport

import (
	"fmt"
	"os"
	"testing"
)

type Testy struct {
	tThing	*testing.T
}

func GetTesty( t *testing.T ) *Testy {
	return &Testy {
		tThing:	t,
	}
}

/*
	Fail if the state is not true. (assert with a message.)
*/
func (t *Testy) Ensure( state bool, ctl string, mParams ...any ) {
	if state {		// all good, no work to be done
		return
	}

	fmt.Fprintf( os.Stderr,  "[FAIL] " + ctl, mParams... )	// pass on the message output
	t.tThing.Fail()
}

/*
	If state is true, then fail immeiately.
*/
func (t *Testy) PanicIf( state bool, ctl string, mParams ...any ) {
	if !state {		// all good, no work to be done
		return
	}

	fmt.Fprintf( os.Stderr,  "[FAIL] " + ctl, mParams... )	// pass on the message output
	t.tThing.FailNow()
}

/*
	Used when an error is NOT expected; this will fail if there is an error
*/
func (t *Testy) EnsureNotErr( err error, ctl string, mParams ...any ) {
	if err == nil {		// all good, no work to be done
		return
	}

	fMsg := fmt.Sprintf( "[FAIL] err: %s: %s", err, ctl )
	fmt.Fprintf( os.Stderr,  fMsg, mParams... )	// pass on the message output
	t.tThing.Fail()
}

/*
	Used when an error should stop all further testing.
*/
func (t *Testy) PanicOnErr( err error, ctl string, mParams ...any ) {
	if err == nil {		// all good, no work to be done
		return
	}

	fMsg := fmt.Sprintf( "[FAIL] err: %s: %s", err, ctl )
	fmt.Fprintf( os.Stderr,  fMsg, mParams... )	// pass on the message output
	t.tThing.FailNow()
}

/*
	When an error is expected, this will fail if err is not set.
*/
func (t *Testy) EnsureErr( err error, ctl string, mParams ...any ) {
	if err != nil {		// all good, no work to be done
		return
	}

	fmt.Fprintf( os.Stderr,  "[FAIL] " + ctl, mParams... )	// pass on the message output
	t.tThing.Fail()
}


