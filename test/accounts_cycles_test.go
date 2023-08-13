// Copyright 2020 The NATS Authors
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

package test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestAccountCycleService(t *testing.T) {
	conf := createConfFile(t, []byte(`
		accounts {
		  A {
		    exports [ { service: help } ]
			imports [ { service { subject: help, account: B } } ]
		  }
		  B {
		    exports [ { service: help } ]
			imports [ { service { subject: help, account: A } } ]
		  }
		}
	`))

	if _, err := server.ProcessConfigFile(conf); err == nil || !strings.Contains(err.Error(), server.ErrImportFormsCycle.Error()) {
		t.Fatalf("Expected an error on cycle service import, got none")
	}

	conf = createConfFile(t, []byte(`
		accounts {
		  A {
		    exports [ { service: * } ]
			imports [ { service { subject: help, account: B } } ]
		  }
		  B {
		    exports [ { service: help } ]
			imports [ { service { subject: *, account: A } } ]
		  }
		}
	`))

	if _, err := server.ProcessConfigFile(conf); err == nil || !strings.Contains(err.Error(), server.ErrImportFormsCycle.Error()) {
		t.Fatalf("Expected an error on cycle service import, got none")
	}

	conf = createConfFile(t, []byte(`
		accounts {
		  A {
		    exports [ { service: * } ]
			imports [ { service { subject: help, account: B } } ]
		  }
		  B {
		    exports [ { service: help } ]
			imports [ { service { subject: help, account: C } } ]
		  }
		  C {
		    exports [ { service: * } ]
			imports [ { service { subject: *, account: A } } ]
		  }
		}
	`))

	if _, err := server.ProcessConfigFile(conf); err == nil || !strings.Contains(err.Error(), server.ErrImportFormsCycle.Error()) {
		t.Fatalf("Expected an error on cycle service import, got none")
	}
}

func TestAccountCycleStream(t *testing.T) {
	conf := createConfFile(t, []byte(`
		accounts {
		  A {
		    exports [ { stream: strm } ]
			imports [ { stream { subject: strm, account: B } } ]
		  }
		  B {
		    exports [ { stream: strm } ]
			imports [ { stream { subject: strm, account: A } } ]
		  }
		}
	`))
	if _, err := server.ProcessConfigFile(conf); err == nil || !strings.Contains(err.Error(), server.ErrImportFormsCycle.Error()) {
		t.Fatalf("Expected an error on cyclic import, got none")
	}
}

func TestAccountCycleStreamWithMapping(t *testing.T) {
	conf := createConfFile(t, []byte(`
		accounts {
		  A {
		    exports [ { stream: * } ]
			imports [ { stream { subject: bar, account: B } } ]
		  }
		  B {
		    exports [ { stream: bar } ]
			imports [ { stream { subject: foo, account: A }, to: bar } ]
		  }
		}
	`))
	if _, err := server.ProcessConfigFile(conf); err == nil || !strings.Contains(err.Error(), server.ErrImportFormsCycle.Error()) {
		t.Fatalf("Expected an error on cyclic import, got none")
	}
}

func TestAccountCycleNonCycleStreamWithMapping(t *testing.T) {
	conf := createConfFile(t, []byte(`
		accounts {
		  A {
		    exports [ { stream: foo } ]
			imports [ { stream { subject: bar, account: B } } ]
		  }
		  B {
		    exports [ { stream: bar } ]
			imports [ { stream { subject: baz, account: C }, to: bar } ]
		  }
		  C {
		    exports [ { stream: baz } ]
			imports [ { stream { subject: foo, account: A }, to: bar } ]
		  }
		}
	`))
	if _, err := server.ProcessConfigFile(conf); err != nil {
		t.Fatalf("Expected no error but got %s", err)
	}
}

func TestAccountCycleServiceCycleWithMapping(t *testing.T) {
	conf := createConfFile(t, []byte(`
		accounts {
		  A {
		    exports [ { service: a } ]
			imports [ { service { subject: b, account: B }, to: a } ]
		  }
		  B {
		    exports [ { service: b } ]
			imports [ { service { subject: a, account: A }, to: b } ]
		  }
		}
	`))
	if _, err := server.ProcessConfigFile(conf); err == nil || !strings.Contains(err.Error(), server.ErrImportFormsCycle.Error()) {
		t.Fatalf("Expected an error on cycle service import, got none")
	}
}

func TestAccountCycleServiceNonCycle(t *testing.T) {
	conf := createConfFile(t, []byte(`
		accounts {
		  A {
		    exports [ { service: * } ]
			imports [ { service { subject: help, account: B } } ]
		  }
		  B {
		    exports [ { service: help } ]
			imports [ { service { subject: nohelp, account: C } } ]
		  }
		  C {
		    exports [ { service: * } ]
			imports [ { service { subject: *, account: A } } ]
		  }
		}
	`))

	if _, err := server.ProcessConfigFile(conf); err != nil {
		t.Fatalf("Expected no error but got %s", err)
	}
}

func TestAccountCycleServiceNonCycleChain(t *testing.T) {
	conf := createConfFile(t, []byte(`
		accounts {
		  A {
		    exports [ { service: help } ]
			imports [ { service { subject: help, account: B } } ]
		  }
		  B {
		    exports [ { service: help } ]
			imports [ { service { subject: help, account: C } } ]
		  }
		  C {
		    exports [ { service: help } ]
			imports [ { service { subject: help, account: D } } ]
		  }
		  D {
		    exports [ { service: help } ]
		  }
		}
	`))

	if _, err := server.ProcessConfigFile(conf); err != nil {
		t.Fatalf("Expected no error but got %s", err)
	}
}

// bug: https://github.com/nats-io/nats-server/issues/1769
func TestServiceImportReplyMatchCycle(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: -1
		accounts {
		  A {
			users: [{user: d,  pass: x}]
			imports [ {service: {account: B, subject: ">" }}]
		  }
		  B {
			users: [{user: x,  pass: x}]
		    exports [ { service: ">" } ]
		  }
		}
		no_auth_user: d
	`))

	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc1 := clientConnectToServerWithUP(t, opts, "x", "x")
	defer nc1.Close()

	msg := []byte("HELLO")
	nc1.Subscribe("foo", func(m *nats.Msg) {
		m.Respond(msg)
	})

	nc2 := clientConnectToServer(t, s)
	defer nc2.Close()

	resp, err := nc2.Request("foo", nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp == nil || string(resp.Data) != string(msg) {
		t.Fatalf("Wrong or empty response")
	}
}

func TestServiceImportReplyMatchCycleMultiHops(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: -1
		accounts {
		  A {
			users: [{user: d,  pass: x}]
			imports [ {service: {account: B, subject: ">" }}]
		  }
		  B {
		    exports [ { service: ">" } ]
			imports [ {service: {account: C, subject: ">" }}]
		  }
		  C {
			users: [{user: x,  pass: x}]
		    exports [ { service: ">" } ]
		  }
		}
		no_auth_user: d
	`))

	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc1 := clientConnectToServerWithUP(t, opts, "x", "x")
	defer nc1.Close()

	msg := []byte("HELLO")
	nc1.Subscribe("foo", func(m *nats.Msg) {
		m.Respond(msg)
	})

	nc2 := clientConnectToServer(t, s)
	defer nc2.Close()

	resp, err := nc2.Request("foo", nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp == nil || string(resp.Data) != string(msg) {
		t.Fatalf("Wrong or empty response")
	}
}

// Go's stack are infinite sans memory, but not call depth. However its good to limit.
func TestAccountCycleDepthLimit(t *testing.T) {
	var last *server.Account
	chainLen := server.MaxAccountCycleSearchDepth + 1

	// Services
	for i := 1; i <= chainLen; i++ {
		acc := server.NewAccount(fmt.Sprintf("ACC-%d", i))
		if err := acc.AddServiceExport("*", nil); err != nil {
			t.Fatalf("Error adding service export to '*': %v", err)
		}
		if last != nil {
			err := acc.AddServiceImport(last, "foo", "foo")
			switch i {
			case chainLen:
				if err != server.ErrCycleSearchDepth {
					t.Fatalf("Expected last import to fail with '%v', but got '%v'", server.ErrCycleSearchDepth, err)
				}
			default:
				if err != nil {
					t.Fatalf("Error adding service import to 'foo': %v", err)
				}
			}
		}
		last = acc
	}

	last = nil

	// Streams
	for i := 1; i <= chainLen; i++ {
		acc := server.NewAccount(fmt.Sprintf("ACC-%d", i))
		if err := acc.AddStreamExport("foo", nil); err != nil {
			t.Fatalf("Error adding stream export to '*': %v", err)
		}
		if last != nil {
			err := acc.AddStreamImport(last, "foo", "")
			switch i {
			case chainLen:
				if err != server.ErrCycleSearchDepth {
					t.Fatalf("Expected last import to fail with '%v', but got '%v'", server.ErrCycleSearchDepth, err)
				}
			default:
				if err != nil {
					t.Fatalf("Error adding stream import to 'foo': %v", err)
				}
			}
		}
		last = acc
	}
}

// Test token and partition subject mapping within an account
func TestAccountSubjectMapping(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: -1
		mappings = {
    		"foo.*.*" : "foo.$1.{{wildcard(2)}}.{{partition(10,1,2)}}"
		}
	`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc1 := clientConnectToServer(t, s)
	defer nc1.Close()

	numMessages := 100
	subjectsReceived := make(chan string)

	msg := []byte("HELLO")
	sub1, err := nc1.Subscribe("foo.*.*.*", func(m *nats.Msg) {
		subjectsReceived <- m.Subject
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub1.AutoUnsubscribe(numMessages * 2)

	nc2 := clientConnectToServer(t, s)
	defer nc2.Close()

	// publish numMessages with an increasing id (should map to partition numbers with the range of 10 partitions) - twice
	for j := 0; j < 2; j++ {
		for i := 0; i < numMessages; i++ {
			err = nc2.Publish(fmt.Sprintf("foo.%d.%d", i, numMessages-i), msg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
	}

	// verify all the partition numbers are in the expected range
	partitionsReceived := make([]int, numMessages)

	for i := 0; i < numMessages; i++ {
		subject := <-subjectsReceived
		sTokens := strings.Split(subject, ".")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		t1, _ := strconv.Atoi(sTokens[1])
		t2, _ := strconv.Atoi(sTokens[2])
		partitionsReceived[i], err = strconv.Atoi(sTokens[3])
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if partitionsReceived[i] > 9 || partitionsReceived[i] < 0 || t1 != i || t2 != numMessages-i {
			t.Fatalf("Error received unexpected %d.%d to partition %d", t1, t2, partitionsReceived[i])
		}
	}

	// verify hashing is deterministic by checking it produces the same exact result twice
	for i := 0; i < numMessages; i++ {
		subject := <-subjectsReceived
		partitionNumber, err := strconv.Atoi(strings.Split(subject, ".")[3])
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if partitionsReceived[i] != partitionNumber {
			t.Fatalf("Error: same id mapped to two different partitions")
		}
	}
}

// test token subject mapping within an account
// Alice imports from Bob with subject mapping
func TestAccountImportSubjectMapping(t *testing.T) {
	conf := createConfFile(t, []byte(`
                port: -1
                accounts {
                  A {
                      users: [{user: a,  pass: x}]
                      imports [ {stream: {account: B, subject: "foo.*.*"}, to : "foo.$1.{{wildcard(2)}}"}]
                  }
                  B {
                      users: [{user: b, pass x}]
                      exports [ { stream: ">" } ]
                  }
                }
	`))

	s, opts := RunServerWithConfig(conf)

	defer s.Shutdown()
	ncA := clientConnectToServerWithUP(t, opts, "a", "x")
	defer ncA.Close()

	numMessages := 100
	subjectsReceived := make(chan string)

	msg := []byte("HELLO")
	sub1, err := ncA.Subscribe("foo.*.*", func(m *nats.Msg) {
		subjectsReceived <- m.Subject
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub1.AutoUnsubscribe(numMessages)
	ncA.Flush()

	ncB := clientConnectToServerWithUP(t, opts, "b", "x")
	defer ncB.Close()

	// publish numMessages with an increasing id

	for i := 0; i < numMessages; i++ {
		err = ncB.Publish(fmt.Sprintf("foo.%d.%d", i, numMessages-i), msg)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	for i := 0; i < numMessages; i++ {
		var subject string
		select {
		case subject = <-subjectsReceived:
		case <-time.After(1 * time.Second):
			t.Fatal("Timed out waiting for messages")
		}
		sTokens := strings.Split(subject, ".")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		t1, _ := strconv.Atoi(sTokens[1])
		t2, _ := strconv.Atoi(sTokens[2])

		if t1 != i || t2 != numMessages-i {
			t.Fatalf("Error received unexpected %d.%d", t1, t2)
		}
	}
}

func clientConnectToServer(t *testing.T, s *server.Server) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(),
		nats.Name("JS-TEST"),
		nats.ReconnectWait(5*time.Millisecond),
		nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	return nc
}

func clientConnectToServerWithUP(t *testing.T, opts *server.Options, user, pass string) *nats.Conn {
	curl := fmt.Sprintf("nats://%s:%s@%s:%d", user, pass, opts.Host, opts.Port)
	nc, err := nats.Connect(curl, nats.Name("JS-UP-TEST"), nats.ReconnectWait(5*time.Millisecond), nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	return nc
}
