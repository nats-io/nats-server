package conf

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
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
	ex := map[string]interface{}{
		"foo": "1",
		"bar": float64(2.2),
		"baz": true,
		"boo": int64(22),
	}
	test(t, "foo='1'; bar=2.2; baz=true; boo=22", ex)
}

func TestBools(t *testing.T) {
	ex := map[string]interface{}{
		"foo": true,
	}
	test(t, "foo=true", ex)
	test(t, "foo=TRUE", ex)
	test(t, "foo=true", ex)
	test(t, "foo=yes", ex)
	test(t, "foo=on", ex)
}

var varSample = `
  index = 22
  foo = $index
`

func TestSimpleVariable(t *testing.T) {
	ex := map[string]interface{}{
		"index": int64(22),
		"foo":   int64(22),
	}
	test(t, varSample, ex)
}

var varNestedSample = `
  index = 22
  nest {
    index = 11
    foo = $index
  }
  bar = $index
`

func TestNestedVariable(t *testing.T) {
	ex := map[string]interface{}{
		"index": int64(22),
		"nest": map[string]interface{}{
			"index": int64(11),
			"foo":   int64(11),
		},
		"bar": int64(22),
	}
	test(t, varNestedSample, ex)
}

func TestMissingVariable(t *testing.T) {
	_, err := Parse("foo=$index")
	if err == nil {
		t.Fatalf("Expected an error for a missing variable, got none")
	}
	if !strings.HasPrefix(err.Error(), "Variable reference") {
		t.Fatalf("Wanted a variable reference err, got %q\n", err)
	}
}

func TestEnvVariable(t *testing.T) {
	ex := map[string]interface{}{
		"foo": int64(22),
	}
	evar := "__UNIQ22__"
	os.Setenv(evar, "22")
	defer os.Unsetenv(evar)
	test(t, fmt.Sprintf("foo = $%s", evar), ex)
}

func TestBcryptVariable(t *testing.T) {
	ex := map[string]interface{}{
		"password": "$2a$11$ooo",
	}
	test(t, "password: $2a$11$ooo", ex)
}

var easynum = `
k = 8k
kb = 4kb
m = 1m
mb = 2MB
g = 2g
gb = 22GB
`

func TestConvenientNumbers(t *testing.T) {
	ex := map[string]interface{}{
		"k":  int64(8 * 1000),
		"kb": int64(4 * 1024),
		"m":  int64(1000 * 1000),
		"mb": int64(2 * 1024 * 1024),
		"g":  int64(2 * 1000 * 1000 * 1000),
		"gb": int64(22 * 1024 * 1024 * 1024),
	}
	test(t, easynum, ex)
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

var cluster = `
cluster {
  port: 4244

  authorization {
    user: route_user
    password: top_secret
    timeout: 1
  }

  # Routes are actively solicited and connected to from this server.
  # Other servers can connect to us if they supply the correct credentials
  # in their routes definitions from above.

  // Test both styles of comments

  routes = [
    nats-route://foo:bar@apcera.me:4245
    nats-route://foo:bar@apcera.me:4246
  ]
}
`

func TestSample2(t *testing.T) {
	ex := map[string]interface{}{
		"cluster": map[string]interface{}{
			"port": int64(4244),
			"authorization": map[string]interface{}{
				"user":     "route_user",
				"password": "top_secret",
				"timeout":  int64(1),
			},
			"routes": []interface{}{
				"nats-route://foo:bar@apcera.me:4245",
				"nats-route://foo:bar@apcera.me:4246",
			},
		},
	}

	test(t, cluster, ex)
}

var sample3 = `
foo  {
  expr = '(true == "false")'
  text = 'This is a multi-line
text block.'
}
`

func TestSample3(t *testing.T) {
	ex := map[string]interface{}{
		"foo": map[string]interface{}{
			"expr": "(true == \"false\")",
			"text": "This is a multi-line\ntext block.",
		},
	}
	test(t, sample3, ex)
}

var sample4 = `
  array [
    { abc: 123 }
    { xyz: "word" }
  ]
`

func TestSample4(t *testing.T) {
	ex := map[string]interface{}{
		"array": []interface{}{
			map[string]interface{}{"abc": int64(123)},
			map[string]interface{}{"xyz": "word"},
		},
	}
	test(t, sample4, ex)
}

var sample5 = `
  now = 2016-05-04T18:53:41Z
  gmt = false

`

func TestSample5(t *testing.T) {
	dt, _ := time.Parse("2006-01-02T15:04:05Z", "2016-05-04T18:53:41Z")
	ex := map[string]interface{}{
		"now": dt,
		"gmt": false,
	}
	test(t, sample5, ex)
}

func TestIncludes(t *testing.T) {
	ex := map[string]interface{}{
		"listen": "127.0.0.1:4222",
		"authorization": map[string]interface{}{
			"ALICE_PASS": "$2a$10$UHR6GhotWhpLsKtVP0/i6.Nh9.fuY73cWjLoJjb2sKT8KISBcUW5q",
			"BOB_PASS":   "$2a$11$dZM98SpGeI7dCFFGSpt.JObQcix8YHml4TBUZoge9R1uxnMIln5ly",
			"users": []interface{}{
				map[string]interface{}{
					"user":     "alice",
					"password": "$2a$10$UHR6GhotWhpLsKtVP0/i6.Nh9.fuY73cWjLoJjb2sKT8KISBcUW5q"},
				map[string]interface{}{
					"user":     "bob",
					"password": "$2a$11$dZM98SpGeI7dCFFGSpt.JObQcix8YHml4TBUZoge9R1uxnMIln5ly"},
			},
			"timeout": float64(0.5),
		},
	}

	m, err := ParseFile("simple.conf")
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
