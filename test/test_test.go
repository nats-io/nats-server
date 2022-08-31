// Copyright 2016-2020 The NATS Authors
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
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"golang.org/x/time/rate"
)

func checkFor(t testing.TB, totalWait, sleepDur time.Duration, f func() error) {
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

// Slow Proxy - For introducing RTT and BW constraints.
type slowProxy struct {
	listener net.Listener
	conns    []net.Conn
	o        *server.Options
	u        string
}

func newSlowProxy(rtt time.Duration, up, down int, opts *server.Options) *slowProxy {
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
		go sp.loop(rtt, up, client, server)
		go sp.loop(rtt, down, server, client)
	}()
	sp.o = &server.Options{Host: "127.0.0.1", Port: port}
	sp.u = fmt.Sprintf("nats://%s:%d", sp.o.Host, sp.o.Port)
	return sp
}

func (sp *slowProxy) opts() *server.Options {
	return sp.o
}

//lint:ignore U1000 Referenced in norace_test.go
func (sp *slowProxy) clientURL() string {
	return sp.u
}

func (sp *slowProxy) loop(rtt time.Duration, tbw int, r, w net.Conn) {
	delay := rtt / 2
	const rbl = 1024
	var buf [rbl]byte
	ctx := context.Background()

	rl := rate.NewLimiter(rate.Limit(tbw), rbl)

	for fr := true; ; {
		sr := time.Now()
		n, err := r.Read(buf[:])
		if err != nil {
			return
		}
		// RTT delays
		if fr || time.Since(sr) > 2*time.Millisecond {
			fr = false
			time.Sleep(delay)
		}
		if err := rl.WaitN(ctx, n); err != nil {
			return
		}
		if _, err = w.Write(buf[:n]); err != nil {
			return
		}
	}
}

func (sp *slowProxy) stop() {
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
