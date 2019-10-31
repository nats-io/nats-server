// Copyright 2016-2018 The NATS Authors
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
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

func checkFor(t *testing.T, totalWait, sleepDur time.Duration, f func() error) {
	t.Helper()
	timeout := time.Now().Add(totalWait)
	var err error
	for time.Now().Before(timeout) {
		err = f()
		if err == nil {
			return
		}
		time.Sleep(sleepDur)
	}
	if err != nil {
		t.Fatal(err.Error())
	}
}

// Slow Proxy - really crude but works for introducing simple RTT delays.
type slowProxy struct {
	listener net.Listener
	conns    []net.Conn
}

func newSlowProxy(latency time.Duration, opts *server.Options) (*slowProxy, *server.Options) {
	saddr := net.JoinHostPort(opts.Host, strconv.Itoa(opts.Port))
	hp := net.JoinHostPort("127.0.0.1", "0")
	l, e := net.Listen("tcp", hp)
	if e != nil {
		panic(fmt.Sprintf("Error listening on port: %s, %q", hp, e))
	}
	port := l.Addr().(*net.TCPAddr).Port
	sp := &slowProxy{listener: l}
	go func() {
		client, err := l.Accept()
		if err != nil {
			return
		}
		server, err := net.DialTimeout("tcp", saddr, time.Second)
		if err != nil {
			panic("Can't connect to server")
		}
		sp.conns = append(sp.conns, client, server)
		go sp.loop(latency, client, server)
		go sp.loop(latency, server, client)
	}()
	sopts := &server.Options{Host: "127.0.0.1", Port: port}
	return sp, sopts
}

func (sp *slowProxy) loop(latency time.Duration, r, w net.Conn) {
	delay := latency / 2
	for {
		var buf [1024]byte
		n, err := r.Read(buf[:])
		if err != nil {
			return
		}
		time.Sleep(delay)
		if _, err = w.Write(buf[:n]); err != nil {
			return
		}
	}
}

func (sp *slowProxy) Stop() {
	if sp.listener != nil {
		sp.listener.Close()
		sp.listener = nil
		for _, c := range sp.conns {
			c.Close()
		}
	}
}

// Dummy Logger
type dummyLogger struct {
	sync.Mutex
	msg string
}

func (d *dummyLogger) Fatalf(format string, args ...interface{}) {
	d.Lock()
	d.msg = fmt.Sprintf(format, args...)
	d.Unlock()
}

func (d *dummyLogger) Errorf(format string, args ...interface{}) {
}

func (d *dummyLogger) Debugf(format string, args ...interface{}) {
}

func (d *dummyLogger) Tracef(format string, args ...interface{}) {
}

func (d *dummyLogger) Noticef(format string, args ...interface{}) {
}

func (d *dummyLogger) Warnf(format string, args ...interface{}) {
}

func TestStackFatal(t *testing.T) {
	d := &dummyLogger{}
	stackFatalf(d, "test stack %d", 1)
	if !strings.HasPrefix(d.msg, "test stack 1") {
		t.Fatalf("Unexpected start of stack: %v", d.msg)
	}
	if !strings.Contains(d.msg, "test_test.go") {
		t.Fatalf("Unexpected stack: %v", d.msg)
	}
}
