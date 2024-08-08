package nhist

import (
	"bytes"
	"flag"
	"fmt"
	"math"
	"os"
	"testing"
        tst "github.com/nats-io/nats-server/v2/server/nhist/tsupport"
)

var (
	verbose bool
)

func print(format string, args ...interface{}) {
	if verbose {
		fmt.Printf(format, args...)
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	verbose = testing.Verbose()
	os.Exit(m.Run())
}

func TestHist(T *testing.T) {
	t := tst.GetTesty(T)
	h := New()
	err := h.RecordValue(1e-140)
	t.EnsureNotErr(err, "RecordValues failed")
	for i := range 100 {
		err := h.RecordValues(math.Ldexp(1.0, i), i+1)
		t.EnsureNotErr(err, "RecordValues %d failed", i)
	}
	b, err := h.MarshalJSON()
	t.EnsureNotErr(err, "marshal failed")
	print("b = %s\n", string(b))
	hr, err := UnmarshalJSON(b)
	t.EnsureNotErr(err, "unmarshal failed")
	br, err := hr.MarshalJSON()
	t.EnsureNotErr(err, "marshal (2) failed")
	if !bytes.Equal(b, br) {
		print("br = %s\n", string(br))
		t.Ensure(false, "differing marshals")
	}
}
