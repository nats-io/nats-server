package conf

import (
	"reflect"
	"testing"

	. "github.com/apcera/gnatsd/test/unittest"
)

// Test to make sure we get what we expect.

func test(t *testing.T, data string, ex map[string]interface{}) {
	m, err := Parse(data)
	if err != nil {
		t.Fatalf("Received err: %v\n", err)
	}
	if m == nil {
		t.Fatal("Received nil map")
	}

	if !reflect.DeepEqual(m, ex) {
		t.Fatalf("Not Equal:\nReceived: '%+v'\nExpected: '%+v'\n", m, ex)
	}
}

func TestSimpleTopLevel(t *testing.T) {
	StartTest(t)
	defer FinishTest(t)

	ex := map[string]interface{}{
		"foo": "1",
		"bar": float64(2.2),
		"baz": true,
		"boo": int64(22),
	}
	test(t, "foo='1'; bar=2.2; baz=true; boo=22", ex)
}

var sample1 = `
foo  {
  host {
    ip   = '127.0.0.1'
    port = 4242
  }
  servers = [ "a.com", "b.com", "c.com"]
}
`

func TestSample1(t *testing.T) {
	StartTest(t)
	defer FinishTest(t)

	ex := map[string]interface{}{
		"foo": map[string]interface{}{
			"host": map[string]interface{}{
				"ip":   "127.0.0.1",
				"port": int64(4242),
			},
			"servers": []interface{}{"a.com", "b.com", "c.com"},
		},
	}
	test(t, sample1, ex)
}
