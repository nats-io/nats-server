// Copyright 2012-2025 The NATS Authors
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

package server

import (
	"math"
	"math/rand"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestParseSize(t *testing.T) {
	if parseSize(nil) != -1 {
		t.Fatal("Should error on nil byte slice")
	}
	n := []byte("12345678")
	if pn := parseSize(n); pn != 12345678 {
		t.Fatalf("Did not parse %q correctly, res=%d", n, pn)
	}

	n = []byte("12345invalid678")
	if pn := parseSize(n); pn != -1 {
		t.Fatalf("Should error on %q, res=%d", n, pn)
	}
}

func TestParseSInt64(t *testing.T) {
	if parseInt64(nil) != -1 {
		t.Fatal("Should error on nil byte slice")
	}
	n := []byte("12345678")
	if pn := parseInt64(n); pn != 12345678 {
		t.Fatalf("Did not parse %q correctly, res=%d", n, pn)
	}

	n = []byte("12345invalid678")
	if pn := parseInt64(n); pn != -1 {
		t.Fatalf("Should error on %q, res=%d", n, pn)
	}
}

func TestParseHostPort(t *testing.T) {
	check := func(hostPort string, defaultPort int, expectedHost string, expectedPort int, expectedErr bool) {
		h, p, err := parseHostPort(hostPort, defaultPort)
		if expectedErr {
			if err == nil {
				stackFatalf(t, "Expected an error, did not get one")
			}
			// expected error, so we are done
			return
		}
		if !expectedErr && err != nil {
			stackFatalf(t, "Unexpected error: %v", err)
		}
		if expectedHost != h {
			stackFatalf(t, "Expected host %q, got %q", expectedHost, h)
		}
		if expectedPort != p {
			stackFatalf(t, "Expected port %d, got %d", expectedPort, p)
		}
	}
	check("addr:1234", 5678, "addr", 1234, false)
	check(" addr:1234 ", 5678, "addr", 1234, false)
	check(" addr : 1234 ", 5678, "addr", 1234, false)
	check("addr", 5678, "addr", 5678, false)
	check(" addr ", 5678, "addr", 5678, false)
	check("addr:-1", 5678, "addr", 5678, false)
	check(" addr:-1 ", 5678, "addr", 5678, false)
	check(" addr : -1 ", 5678, "addr", 5678, false)
	check("addr:0", 5678, "addr", 5678, false)
	check(" addr:0 ", 5678, "addr", 5678, false)
	check(" addr : 0 ", 5678, "addr", 5678, false)
	check("addr:addr", 0, "", 0, true)
	check("addr:::1234", 0, "", 0, true)
	check("", 0, "", 0, true)
}

func TestURLsAreEqual(t *testing.T) {
	check := func(t *testing.T, u1Str, u2Str string, expectedSame bool) {
		t.Helper()
		u1, err := url.Parse(u1Str)
		if err != nil {
			t.Fatalf("Error parsing url %q: %v", u1Str, err)
		}
		u2, err := url.Parse(u2Str)
		if err != nil {
			t.Fatalf("Error parsing url %q: %v", u2Str, err)
		}
		same := urlsAreEqual(u1, u2)
		if expectedSame && !same {
			t.Fatalf("Expected %v and %v to be the same, they were not", u1, u2)
		} else if !expectedSame && same {
			t.Fatalf("Expected %v and %v to be different, they were not", u1, u2)
		}
	}
	check(t, "nats://localhost:4222", "nats://localhost:4222", true)
	check(t, "nats://ivan:pwd@localhost:4222", "nats://ivan:pwd@localhost:4222", true)
	check(t, "nats://ivan@localhost:4222", "nats://ivan@localhost:4222", true)
	check(t, "nats://ivan:@localhost:4222", "nats://ivan:@localhost:4222", true)
	check(t, "nats://host1:4222", "nats://host2:4222", false)
}

func TestComma(t *testing.T) {
	type testList []struct {
		name, got, exp string
	}

	l := testList{
		{name: "0", got: comma(0), exp: "0"},
		{name: "10", got: comma(10), exp: "10"},
		{name: "100", got: comma(100), exp: "100"},
		{name: "1,000", got: comma(1000), exp: "1,000"},
		{name: "10,000", got: comma(10000), exp: "10,000"},
		{name: "100,000", got: comma(100000), exp: "100,000"},
		{name: "10,000,000", got: comma(10000000), exp: "10,000,000"},
		{name: "10,100,000", got: comma(10100000), exp: "10,100,000"},
		{name: "10,010,000", got: comma(10010000), exp: "10,010,000"},
		{name: "10,001,000", got: comma(10001000), exp: "10,001,000"},
		{name: "123,456,789", got: comma(123456789), exp: "123,456,789"},
		{name: "maxint", got: comma(9.223372e+18), exp: "9,223,372,000,000,000,000"},
		{name: "math.maxint", got: comma(math.MaxInt64), exp: "9,223,372,036,854,775,807"},
		{name: "math.minint", got: comma(math.MinInt64), exp: "-9,223,372,036,854,775,808"},
		{name: "minint", got: comma(-9.223372e+18), exp: "-9,223,372,000,000,000,000"},
		{name: "-123,456,789", got: comma(-123456789), exp: "-123,456,789"},
		{name: "-10,100,000", got: comma(-10100000), exp: "-10,100,000"},
		{name: "-10,010,000", got: comma(-10010000), exp: "-10,010,000"},
		{name: "-10,001,000", got: comma(-10001000), exp: "-10,001,000"},
		{name: "-10,000,000", got: comma(-10000000), exp: "-10,000,000"},
		{name: "-100,000", got: comma(-100000), exp: "-100,000"},
		{name: "-10,000", got: comma(-10000), exp: "-10,000"},
		{name: "-1,000", got: comma(-1000), exp: "-1,000"},
		{name: "-100", got: comma(-100), exp: "-100"},
		{name: "-10", got: comma(-10), exp: "-10"},
	}

	failed := false
	for _, test := range l {
		if test.got != test.exp {
			t.Errorf("On %v, expected '%v', but got '%v'",
				test.name, test.exp, test.got)
			failed = true
		}
	}
	if failed {
		t.FailNow()
	}
}

func TestURLRedaction(t *testing.T) {
	redactionFromTo := []struct {
		Full string
		Safe string
	}{
		{Full: "nats://foo:bar@example.org", Safe: "nats://foo:xxxxx@example.org"},
		{Full: "nats://foo@example.org", Safe: "nats://foo@example.org"},
		{Full: "nats://example.org", Safe: "nats://example.org"},
		{Full: "nats://example.org/foo?bar=1", Safe: "nats://example.org/foo?bar=1"},
	}
	var err error
	listFull := make([]*url.URL, len(redactionFromTo))
	listSafe := make([]*url.URL, len(redactionFromTo))
	for i := range redactionFromTo {
		r := redactURLString(redactionFromTo[i].Full)
		if r != redactionFromTo[i].Safe {
			t.Fatalf("Redacting URL [index %d] %q, expected %q got %q", i, redactionFromTo[i].Full, redactionFromTo[i].Safe, r)
		}
		if listFull[i], err = url.Parse(redactionFromTo[i].Full); err != nil {
			t.Fatalf("Redacting URL index %d parse Full failed: %v", i, err)
		}
		if listSafe[i], err = url.Parse(redactionFromTo[i].Safe); err != nil {
			t.Fatalf("Redacting URL index %d parse Safe failed: %v", i, err)
		}
	}
	results := redactURLList(listFull)
	if !reflect.DeepEqual(results, listSafe) {
		t.Fatalf("Redacting URL list did not compare equal, even after each URL did")
	}
}

func TestVersionAtLeast(t *testing.T) {
	for _, test := range []struct {
		version string
		major   int
		minor   int
		update  int
		result  bool
	}{
		{version: "2.0.0-beta", major: 1, minor: 9, update: 9, result: true},
		{version: "2.0.0", major: 1, minor: 99, update: 9, result: true},
		{version: "2.2.0", major: 2, minor: 1, update: 9, result: true},
		{version: "2.2.2", major: 2, minor: 2, update: 2, result: true},
		{version: "2.2.2", major: 2, minor: 2, update: 3, result: false},
		{version: "2.2.2", major: 2, minor: 3, update: 2, result: false},
		{version: "2.2.2", major: 3, minor: 2, update: 2, result: false},
		{version: "2.22.2", major: 3, minor: 0, update: 0, result: false},
		{version: "2.2.22", major: 2, minor: 3, update: 0, result: false},
		{version: "bad.version", major: 1, minor: 2, update: 3, result: false},
	} {
		t.Run(_EMPTY_, func(t *testing.T) {
			if res := versionAtLeast(test.version, test.major, test.minor, test.update); res != test.result {
				t.Fatalf("For check version %q at least %d.%d.%d result should have been %v, got %v",
					test.version, test.major, test.minor, test.update, test.result, res)
			}
		})
	}
}

func BenchmarkParseInt(b *testing.B) {
	b.SetBytes(1)
	n := "12345678"
	for i := 0; i < b.N; i++ {
		strconv.ParseInt(n, 10, 0)
	}
}

func BenchmarkParseSize(b *testing.B) {
	b.SetBytes(1)
	n := []byte("12345678")
	for i := 0; i < b.N; i++ {
		parseSize(n)
	}
}

func deferUnlock(mu *sync.Mutex) {
	mu.Lock()
	defer mu.Unlock()
	// see noDeferUnlock
	if false {
		return
	}
}

func BenchmarkDeferMutex(b *testing.B) {
	var mu sync.Mutex
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		deferUnlock(&mu)
	}
}

func noDeferUnlock(mu *sync.Mutex) {
	mu.Lock()
	// prevent staticcheck warning about empty critical section
	if false {
		return
	}
	mu.Unlock()
}

func BenchmarkNoDeferMutex(b *testing.B) {
	var mu sync.Mutex
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		noDeferUnlock(&mu)
	}
}

func createTestSub() *subscription {
	return &subscription{
		subject: []byte("foo"),
		queue:   []byte("bar"),
		sid:     []byte("22"),
	}
}

func BenchmarkArrayRand(b *testing.B) {
	b.StopTimer()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Create an array of 10 items
	subs := []*subscription{}
	for i := 0; i < 10; i++ {
		subs = append(subs, createTestSub())
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		index := r.Intn(len(subs))
		_ = subs[index]
	}
}

func BenchmarkMapRange(b *testing.B) {
	b.StopTimer()
	// Create an map of 10 items
	subs := map[int]*subscription{}
	for i := 0; i < 10; i++ {
		subs[i] = createTestSub()
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		for range subs {
			break
		}
	}
}
