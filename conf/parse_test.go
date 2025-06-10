// Copyright 2013-2025 The NATS Authors
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

package conf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Test to make sure we get what we expect.

func test(t *testing.T, data string, ex map[string]any) {
	t.Helper()
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

	// Also test ParseWithChecks
	n, err := ParseWithChecks(data)
	if err != nil {
		t.Fatalf("Received err: %v\n", err)
	}
	if n == nil {
		t.Fatal("Received nil map")
	}
	// Compare with the original results.
	a, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(n)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, a) {
		t.Fatalf("Not Equal:\nReceived: '%+v'\nExpected: '%+v'\n", string(a), string(b))
	}
}

func TestSimpleTopLevel(t *testing.T) {
	ex := map[string]any{
		"foo": "1",
		"bar": float64(2.2),
		"baz": true,
		"boo": int64(22),
	}
	test(t, "foo='1'; bar=2.2; baz=true; boo=22", ex)
}

func TestBools(t *testing.T) {
	ex := map[string]any{
		"foo": true,
	}
	test(t, "foo=true", ex)
	test(t, "foo=TRUE", ex)
	test(t, "foo=true", ex)
	test(t, "foo=yes", ex)
	test(t, "foo=on", ex)
}

func TestSimpleVariable(t *testing.T) {
	conf := `
		index = 22
		foo = $index
	`
	ex := map[string]any{
		"index": int64(22),
		"foo":   int64(22),
	}
	test(t, conf, ex)
}

func TestNestedVariable(t *testing.T) {
	conf := `
		index = 22
		nest {
			index = 11
			foo = $index
		}
		bar = $index
	`
	ex := map[string]any{
		"index": int64(22),
		"nest": map[string]any{
			"index": int64(11),
			"foo":   int64(11),
		},
		"bar": int64(22),
	}
	test(t, conf, ex)
}

func TestVariableAsMapKey(t *testing.T) {
	conf := `
		KEY = "name"
		test {
			KEY = "first_name"
			$KEY = derek
		}
	`
	ex := map[string]any{
		"KEY": "name",
		"test": map[string]any{
			"KEY":        "first_name",
			"first_name": "derek",
		},
	}
	test(t, conf, ex)
}

// TestVariableKeyUsedFlag tests that usedVariable flag is set when variables are used as keys in pedantic mode.
// The variable needs to be in the config and not external, yhat is the token that will have the flag set.
func TestVariableKeyUsedFlag(t *testing.T) {
	config := `
	TEST_KEY = mykey
	test {
		$TEST_KEY: "value"
	}`

	// Parse in pedantic mode to get tokens with usedVariable tracking
	p, err := parse(config, "", true)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	// The variable should be marked as used since it was used as a key
	// Look for any tokens in the mapping that have usedVariable set
	var foundUsedVariable bool
	var checkTokens func(m map[string]any)
	checkTokens = func(m map[string]any) {
		for _, v := range m {
			t := v.(*token)
			if t.usedVariable {
				foundUsedVariable = true
				return
			}
			// Cleanup any other env var that is still in the map.
			if tm, ok := t.value.(map[string]any); ok {
				checkTokens(tm)
			}
		}
	}

	// Check all values in the parsed mapping
	checkTokens(p.mapping)

	if !foundUsedVariable {
		t.Error("Expected to find at least one token with usedVariable=true when variable is used as key")
	}
}

func TestMissingVariableAsMapKey(t *testing.T) {
	_, err := Parse("test { $KEY = derek }")
	if err == nil {
		t.Fatalf("Expected an error for a missing variable, got none")
	}
	if !strings.HasPrefix(err.Error(), "variable reference") {
		t.Fatalf("Wanted a variable reference err, got %q\n", err)
	}
}

func TestMissingVariableAsMapValue(t *testing.T) {
	_, err := Parse("foo=$index")
	if err == nil {
		t.Fatalf("Expected an error for a missing variable, got none")
	}
	if !strings.HasPrefix(err.Error(), "variable reference") {
		t.Fatalf("Wanted a variable reference err, got %q\n", err)
	}
}

func TestEnvVariable(t *testing.T) {
	ex := map[string]any{
		"foo": int64(22),
	}
	evar := "__UNIQ22__"
	os.Setenv(evar, "22")
	defer os.Unsetenv(evar)
	test(t, fmt.Sprintf("foo = $%s", evar), ex)
}

func TestEnvVariableString(t *testing.T) {
	ex := map[string]any{
		"foo": "xyz",
	}
	evar := "__UNIQ22__"
	os.Setenv(evar, "xyz")
	defer os.Unsetenv(evar)
	test(t, fmt.Sprintf("foo = $%s", evar), ex)
}

func TestEnvVariableStringStartingWithNumber(t *testing.T) {
	evar := "__UNIQ22__"
	os.Setenv(evar, "3xyz")
	defer os.Unsetenv(evar)

	if _, err := Parse(fmt.Sprintf("foo = $%s", evar)); err != nil {
		t.Fatalf("Expected no err but got: %v\n", err)
	}
}

func TestEnvVariableStringStartingWithNumberAndSizeUnit(t *testing.T) {
	evar := "__UNIQ22__"
	os.Setenv(evar, "3Gyz")
	defer os.Unsetenv(evar)
	ex := map[string]any{
		"foo": "3Gyz",
	}
	test(t, fmt.Sprintf("foo = $%s", evar), ex)
}

func TestEnvVariableStringStartingWithNumberUsingQuotes(t *testing.T) {
	ex := map[string]any{
		"foo": "3xyz",
	}
	evar := "__UNIQ22__"
	os.Setenv(evar, "'3xyz'")
	defer os.Unsetenv(evar)
	test(t, fmt.Sprintf("foo = $%s", evar), ex)
}

func TestBcryptVariable(t *testing.T) {
	ex := map[string]any{
		"password": "$2a$11$ooo",
	}
	test(t, "password: $2a$11$ooo", ex)
}

var easynum = `
k = 8k
kb = 4kb
ki = 3ki
kib = 4ki
m = 1m
mb = 2MB
mi = 2Mi
mib = 64MiB
g = 2g
gb = 22GB
gi = 22Gi
gib = 22GiB
tb = 22TB
ti = 22Ti
tib = 22TiB
pb = 22PB
pi = 22Pi
pib = 22PiB
`

func TestConvenientNumbers(t *testing.T) {
	ex := map[string]any{
		"k":   int64(8 * 1000),
		"kb":  int64(4 * 1024),
		"ki":  int64(3 * 1024),
		"kib": int64(4 * 1024),
		"m":   int64(1000 * 1000),
		"mb":  int64(2 * 1024 * 1024),
		"mi":  int64(2 * 1024 * 1024),
		"mib": int64(64 * 1024 * 1024),
		"g":   int64(2 * 1000 * 1000 * 1000),
		"gb":  int64(22 * 1024 * 1024 * 1024),
		"gi":  int64(22 * 1024 * 1024 * 1024),
		"gib": int64(22 * 1024 * 1024 * 1024),
		"tb":  int64(22 * 1024 * 1024 * 1024 * 1024),
		"ti":  int64(22 * 1024 * 1024 * 1024 * 1024),
		"tib": int64(22 * 1024 * 1024 * 1024 * 1024),
		"pb":  int64(22 * 1024 * 1024 * 1024 * 1024 * 1024),
		"pi":  int64(22 * 1024 * 1024 * 1024 * 1024 * 1024),
		"pib": int64(22 * 1024 * 1024 * 1024 * 1024 * 1024),
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
	ex := map[string]any{
		"foo": map[string]any{
			"host": map[string]any{
				"ip":   "127.0.0.1",
				"port": int64(4242),
			},
			"servers": []any{"a.com", "b.com", "c.com"},
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
	ex := map[string]any{
		"cluster": map[string]any{
			"port": int64(4244),
			"authorization": map[string]any{
				"user":     "route_user",
				"password": "top_secret",
				"timeout":  int64(1),
			},
			"routes": []any{
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
	ex := map[string]any{
		"foo": map[string]any{
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
	ex := map[string]any{
		"array": []any{
			map[string]any{"abc": int64(123)},
			map[string]any{"xyz": "word"},
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
	ex := map[string]any{
		"now": dt,
		"gmt": false,
	}
	test(t, sample5, ex)
}

func TestIncludes(t *testing.T) {
	ex := map[string]any{
		"listen": "127.0.0.1:4222",
		"authorization": map[string]any{
			"ALICE_PASS": "$2a$10$UHR6GhotWhpLsKtVP0/i6.Nh9.fuY73cWjLoJjb2sKT8KISBcUW5q",
			"BOB_PASS":   "$2a$11$dZM98SpGeI7dCFFGSpt.JObQcix8YHml4TBUZoge9R1uxnMIln5ly",
			"users": []any{
				map[string]any{
					"user":     "alice",
					"password": "$2a$10$UHR6GhotWhpLsKtVP0/i6.Nh9.fuY73cWjLoJjb2sKT8KISBcUW5q"},
				map[string]any{
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

var varIncludedVariablesSample = `
authorization {

  include "./includes/passwords.conf"

  CAROL_PASS: foo

  users = [
   {user: alice, password: $ALICE_PASS}
   {user: bob,   password: $BOB_PASS}
   {user: carol, password: $CAROL_PASS}
  ]
}
`

func TestIncludeVariablesWithChecks(t *testing.T) {
	p, err := parse(varIncludedVariablesSample, "", true)
	if err != nil {
		t.Fatalf("Received err: %v\n", err)
	}
	key := "authorization"
	m, ok := p.mapping[key]
	if !ok {
		t.Errorf("Expected %q to be in the config", key)
	}
	expectKeyVal := func(t *testing.T, m any, expectedKey string, expectedVal string, expectedLine, expectedPos int) {
		t.Helper()
		tk := m.(*token)
		v := tk.Value()
		vv := v.(map[string]any)
		value, ok := vv[expectedKey]
		if !ok {
			t.Errorf("Expected key %q", expectedKey)
		}
		tk, ok = value.(*token)
		if !ok {
			t.Fatalf("Expected token %v", value)
		}
		if tk.Line() != expectedLine {
			t.Errorf("Expected token to be at line %d, got: %d", expectedLine, tk.Line())
		}
		if tk.Position() != expectedPos {
			t.Errorf("Expected token to be at position %d, got: %d", expectedPos, tk.Position())
		}
		v = tk.Value()
		if v != expectedVal {
			t.Errorf("Expected %q, got: %s", expectedVal, v)
		}
	}
	expectKeyVal(t, m, "ALICE_PASS", "$2a$10$UHR6GhotWhpLsKtVP0/i6.Nh9.fuY73cWjLoJjb2sKT8KISBcUW5q", 2, 1)
	expectKeyVal(t, m, "BOB_PASS", "$2a$11$dZM98SpGeI7dCFFGSpt.JObQcix8YHml4TBUZoge9R1uxnMIln5ly", 3, 1)
	expectKeyVal(t, m, "CAROL_PASS", "foo", 6, 3)
}

func TestParserNoInfiniteLoop(t *testing.T) {
	for _, test := range []string{`A@@Føøøø?˛ø:{øøøø˙˙`, `include "9/�`} {
		if _, err := Parse(test); err == nil {
			t.Fatal("expected an error")
		} else if !strings.Contains(err.Error(), "Unexpected EOF") {
			t.Fatal("expected unexpected eof error")
		}
	}
}

func TestParseWithNoValuesAreInvalid(t *testing.T) {
	for _, test := range []struct {
		name string
		conf string
		err  string
	}{
		{
			"invalid key without values",
			`aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`,
			"config is invalid (:1:41)",
		},
		{
			"invalid untrimmed key without values",
			`              aaaaaaaaaaaaaaaaaaaaaaaaaaa`,
			"config is invalid (:1:41)",
		},
		{
			"invalid untrimmed key without values",
			`     aaaaaaaaaaaaaaaaaaaaaaaaaaa         `,
			"config is invalid (:1:41)",
		},
		{
			"invalid keys after comments",
			`
          		# with comments and no spaces to create key values
         		# is also an invalid config.
         		aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
                        `,
			"config is invalid (:5:25)",
		},
		{
			"comma separated without values are invalid",
			`
                        a,a,a,a,a,a,a,a,a,a,a
                        `,
			"config is invalid (:3:25)",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parse(test.conf, "", true); err == nil {
				t.Error("expected an error")
			} else if !strings.Contains(err.Error(), test.err) {
				t.Errorf("expected invalid conf error, got: %v", err)
			}
		})
	}
}

func TestParseWithNoValuesEmptyConfigsAreValid(t *testing.T) {
	for _, test := range []struct {
		name string
		conf string
	}{
		{
			"empty conf",
			"",
		},
		{
			"empty conf with line breaks",
			`


                        `,
		},
		{
			"just comments with no values",
			`
                        # just comments with no values
                        # is still valid.
                        `,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parse(test.conf, "", true); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestParseWithTrailingBracketsAreValid(t *testing.T) {
	for _, test := range []struct {
		name string
		conf string
	}{
		{
			"empty conf",
			"{}",
		},
		{
			"just comments with no values",
			`
                        {
                        # comments in the body
                        }
                        `,
		},
		{
			// trailing brackets accidentally can become keys,
			// this is valid since needed to support JSON like configs..
			"trailing brackets after config",
			`
                        accounts { users = [{}]}
                        }
                        `,
		},
		{
			"wrapped in brackets",
			`{
                          accounts { users = [{}]}
                        }
                        `,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parse(test.conf, "", true); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestParseWithNoValuesIncludes(t *testing.T) {
	for _, test := range []struct {
		input    string
		includes map[string]string
		err      string
		linepos  string
	}{
		{
			`# includes
			accounts {
                          foo { include 'foo.conf'}
                          bar { users = [{user = "bar"}] }
                          quux { include 'quux.conf'}
                        }
                        `,
			map[string]string{
				"foo.conf":  ``,
				"quux.conf": `?????????????`,
			},
			"error parsing include file 'quux.conf', config is invalid",
			"quux.conf:1:1",
		},
		{
			`# includes
			accounts {
                          foo { include 'foo.conf'}
                          bar { include 'bar.conf'}
                          quux { include 'quux.conf'}
                        }
                        `,
			map[string]string{
				"foo.conf": ``, // Empty configs are ok
				"bar.conf": `AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA`,
				"quux.conf": `
                                   # just some comments,
                                   # and no key values also ok.
                                `,
			},
			"error parsing include file 'bar.conf', config is invalid",
			"bar.conf:1:34",
		},
	} {
		t.Run("", func(t *testing.T) {
			sdir := t.TempDir()
			f, err := os.CreateTemp(sdir, "nats.conf-")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(f.Name(), []byte(test.input), 066); err != nil {
				t.Error(err)
			}
			if test.includes != nil {
				for includeFile, contents := range test.includes {
					inf, err := os.Create(filepath.Join(sdir, includeFile))
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(inf.Name(), []byte(contents), 066); err != nil {
						t.Error(err)
					}
				}
			}
			if _, err := parse(test.input, f.Name(), true); err == nil {
				t.Error("expected an error")
			} else if !strings.Contains(err.Error(), test.err) || !strings.Contains(err.Error(), test.linepos) {
				t.Errorf("expected invalid conf error, got: %v", err)
			}
		})
	}
}

func TestJSONParseCompat(t *testing.T) {
	for _, test := range []struct {
		name     string
		input    string
		includes map[string]string
		expected map[string]any
	}{
		{
			"JSON with nested blocks",
			`
                        {
                          "http_port": 8227,
                          "port": 4227,
                          "write_deadline": "1h",
                          "cluster": {
                            "port": 6222,
                            "routes": [
                              "nats://127.0.0.1:4222",
                              "nats://127.0.0.1:4223",
                              "nats://127.0.0.1:4224"
                            ]
                          }
                        }
                        `,
			nil,
			map[string]any{
				"http_port":      int64(8227),
				"port":           int64(4227),
				"write_deadline": "1h",
				"cluster": map[string]any{
					"port": int64(6222),
					"routes": []any{
						"nats://127.0.0.1:4222",
						"nats://127.0.0.1:4223",
						"nats://127.0.0.1:4224",
					},
				},
			},
		},
		{
			"JSON with nested blocks",
			`{
                          "jetstream": {
                            "store_dir": "/tmp/nats"
                            "max_mem": 1000000,
                          },
                          "port": 4222,
                          "server_name": "nats1"
                        }
                        `,
			nil,
			map[string]any{
				"jetstream": map[string]any{
					"store_dir": "/tmp/nats",
					"max_mem":   int64(1_000_000),
				},
				"port":        int64(4222),
				"server_name": "nats1",
			},
		},
		{
			"JSON empty object in one line",
			`{}`,
			nil,
			map[string]any{},
		},
		{
			"JSON empty object with line breaks",
			`
                        {
                        }
                        `,
			nil,
			map[string]any{},
		},
		{
			"JSON includes",
			`
                        accounts {
                          foo  { include 'foo.json'  }
                          bar  { include 'bar.json'  }
                          quux { include 'quux.json' }
                        }
                        `,
			map[string]string{
				"foo.json": `{ "users": [ {"user": "foo"} ] }`,
				"bar.json": `{
                                  "users": [ {"user": "bar"} ]
                                }`,
				"quux.json": `{}`,
			},
			map[string]any{
				"accounts": map[string]any{
					"foo": map[string]any{
						"users": []any{
							map[string]any{
								"user": "foo",
							},
						},
					},
					"bar": map[string]any{
						"users": []any{
							map[string]any{
								"user": "bar",
							},
						},
					},
					"quux": map[string]any{},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sdir := t.TempDir()
			f, err := os.CreateTemp(sdir, "nats.conf-")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(f.Name(), []byte(test.input), 066); err != nil {
				t.Error(err)
			}
			if test.includes != nil {
				for includeFile, contents := range test.includes {
					inf, err := os.Create(filepath.Join(sdir, includeFile))
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(inf.Name(), []byte(contents), 066); err != nil {
						t.Error(err)
					}
				}
			}
			m, err := ParseFile(f.Name())
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !reflect.DeepEqual(m, test.expected) {
				t.Fatalf("Not Equal:\nReceived: '%+v'\nExpected: '%+v'\n", m, test.expected)
			}
		})
	}
}

func TestBlocks(t *testing.T) {
	for _, test := range []struct {
		name     string
		input    string
		expected map[string]any
		err      string
		linepos  string
	}{
		{
			"inline block",
			`{ listen: 0.0.0.0:4222 }`,
			map[string]any{
				"listen": "0.0.0.0:4222",
			},
			"", "",
		},
		{
			"newline block",
			`{
				listen: 0.0.0.0:4222
			 }`,
			map[string]any{
				"listen": "0.0.0.0:4222",
			},
			"", "",
		},
		{
			"newline block with trailing comment",
			`
			{
				listen: 0.0.0.0:4222
			}
			# wibble
			`,
			map[string]any{
				"listen": "0.0.0.0:4222",
			},
			"", "",
		},
		{
			"nested newline blocks with trailing comment",
			`
			{
				{
					listen: 0.0.0.0:4222 // random comment
				}
				# wibble1
			}
			# wibble2
			`,
			map[string]any{
				"listen": "0.0.0.0:4222",
			},
			"", "",
		},
		{
			"top line values in block scope",
			`
			{
			  "debug":              False
			  "prof_port":          8221
			  "server_name":        "aws-useast2-natscj1-1"
			}
			`,
			map[string]any{
				"debug":       false,
				"prof_port":   int64(8221),
				"server_name": "aws-useast2-natscj1-1",
			},
			"", "",
		},
		{
			"comment in block scope after value parse",
			`
			{
			  "debug":              False
			  "server_name":        "gcp-asianortheast3-natscj1-1"

			  # Profile port specification.
			  "prof_port":          8221
			}
			`,
			map[string]any{
				"debug":       false,
				"prof_port":   int64(8221),
				"server_name": "gcp-asianortheast3-natscj1-1",
			},
			"", "",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, err := os.CreateTemp(t.TempDir(), "nats.conf-")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(f.Name(), []byte(test.input), 066); err != nil {
				t.Error(err)
			}
			if m, err := ParseFile(f.Name()); err == nil {
				if !reflect.DeepEqual(m, test.expected) {
					t.Fatalf("Not Equal:\nReceived: '%+v'\nExpected: '%+v'\n", m, test.expected)
				}
			} else if !strings.Contains(err.Error(), test.err) || !strings.Contains(err.Error(), test.linepos) {
				t.Errorf("expected invalid conf error, got: %v", err)
			} else if err != nil {
				t.Error(err)
			}
		})
	}
}

func TestParseDigest(t *testing.T) {
	for _, test := range []struct {
		input    string
		includes map[string]string
		digest   string
	}{
		{
			`foo = bar`,
			nil,
			"sha256:226e49e13d16e5e8aa0d62e58cd63361bf097d3e2b2444aa3044334628a2e8de",
		},
		{
			`# Comments and whitespace have no effect
                        foo = bar
                        `,
			nil,
			"sha256:226e49e13d16e5e8aa0d62e58cd63361bf097d3e2b2444aa3044334628a2e8de",
		},
		{
			`# Syntax changes have no effect
                        'foo': 'bar'
                        `,
			nil,
			"sha256:226e49e13d16e5e8aa0d62e58cd63361bf097d3e2b2444aa3044334628a2e8de",
		},
		{
			`# Syntax changes have no effect
                        { 'foo': 'bar' }
                        `,
			nil,
			"sha256:226e49e13d16e5e8aa0d62e58cd63361bf097d3e2b2444aa3044334628a2e8de",
		},
		{
			`# substitutions
                        BAR_USERS = { users = [ {user = "bar"} ]}
                        hello = 'world'
                        accounts {
                          QUUX_USERS = [ { user: quux }]
                          bar = $BAR_USERS
                          quux = { users = $QUUX_USERS }
                        }
                        very { nested { env { VAR = 'NESTED', quux = $VAR }}}
                        `,
			nil,
			"sha256:34f8faf3f269fe7509edc4742f20c8c4a7ad51fe21f8b361764314b533ac3ab5",
		},
		{
			`# substitutions, same as previous one without env vars.
                        hello = 'world'
                        accounts {
                          bar  = { users = [ { user = "bar" } ]}
                          quux = { users = [ { user: quux   } ]}
                        }
                        very { nested { env { quux = 'NESTED' }}}
                        `,
			nil,
			"sha256:34f8faf3f269fe7509edc4742f20c8c4a7ad51fe21f8b361764314b533ac3ab5",
		},
		{
			`# substitutions
                        BAR_USERS = { users = [ {user = "foo"} ]}
                        bar = $BAR_USERS
                        accounts {
                          users = $BAR_USERS
                        }
                        `,
			nil,
			"sha256:f5d943b4ed22b80c6199203f8a7eaa8eb68ef7b2d46ef6b1b26f05e21f8beb13",
		},
		{
			`# substitutions
                        bar = { users = [ {user = "foo"} ]}
                        accounts {
                          users = { users = [ {user = "foo"} ]}
                        }
                        `,
			nil,
			"sha256:f5d943b4ed22b80c6199203f8a7eaa8eb68ef7b2d46ef6b1b26f05e21f8beb13",
		},
		{
			`# includes
			accounts {
                          foo { include 'foo.conf'}
                          bar { users = [{user = "bar"}] }
                          quux { include 'quux.conf'}
                        }
                        `,
			map[string]string{
				"foo.conf":  ` users = [{user = "foo"}]`,
				"quux.conf": ` users = [{user = "quux"}]`,
			},
			"sha256:e72d70c91b64b0f880f86decb95ec2600cbdcf8bdcd2355fce5ebc54a84a77e9",
		},
		{
			`# includes
			accounts {
                          foo { include 'foo.conf'}
                          bar { include 'bar.conf'}
                          quux { include 'quux.conf'}
                        }
                        `,
			map[string]string{
				"foo.conf":  ` users = [{user = "foo"}]`,
				"bar.conf":  ` users = [{user = "bar"}]`,
				"quux.conf": ` users = [{user = "quux"}]`,
			},
			"sha256:e72d70c91b64b0f880f86decb95ec2600cbdcf8bdcd2355fce5ebc54a84a77e9",
		},
	} {
		t.Run("", func(t *testing.T) {
			sdir := t.TempDir()
			f, err := os.CreateTemp(sdir, "nats.conf-")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(f.Name(), []byte(test.input), 066); err != nil {
				t.Error(err)
			}
			if test.includes != nil {
				for includeFile, contents := range test.includes {
					inf, err := os.Create(filepath.Join(sdir, includeFile))
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(inf.Name(), []byte(contents), 066); err != nil {
						t.Error(err)
					}
				}
			}
			_, digest, err := ParseFileWithChecksDigest(f.Name())
			if err != nil {
				t.Fatal(err)
			}
			if digest != test.digest {
				t.Errorf("\ngot: %s\nexpected: %s", digest, test.digest)
			}
		})
	}
}
