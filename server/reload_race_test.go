// Copyright 2026 The NATS Authors
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
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// Regression test for https://github.com/nats-io/nats-server/issues/8499
//
// ReloadOptions() while JetStream / service-import traffic is flowing used to
// trip a data race: reloadAuthorization() -> configureAccounts() writes an
// account's service-import map (and si.se in particular) while the client
// read-loop reads the same data in processServiceImport() during deliverMsg.
//
// This test only reproduces reliably under the race detector, mirroring the
// reporter's embedded setup: JetStream + an nkey user (so that
// reloadAuthorization() actually reprocesses authorization) plus concurrent
// publishers/fetches that keep $JS.API service imports busy while options are
// reloaded in a loop. TLS is intentionally omitted (the reporter noted it is
// incidental).
func TestReloadRaceWithServiceImports(t *testing.T) {
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("nkey: %v", err)
	}
	pub, _ := kp.PublicKey()

	port := -1 // let the server pick a free port
	storeDir := t.TempDir()
	mkOpts := func() *Options {
		return &Options{
			Host:      "127.0.0.1",
			Port:      port,
			JetStream: true,
			StoreDir:  storeDir,
			NoSigs:    true,
			Nkeys:     []*NkeyUser{{Nkey: pub}},
		}
	}

	srv := New(mkOpts())
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatalf("server not ready")
	}
	defer srv.Shutdown()

	sign := func(nonce []byte) ([]byte, error) { return kp.Sign(nonce) }
	nc, err := nats.Connect(srv.ClientURL(), nats.Nkey(pub, sign), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "S", Subjects: []string{"js.>"}}); err != nil {
		t.Fatalf("add stream: %v", err)
	}
	pull, err := js.PullSubscribe("js.>", "dur")
	if err != nil {
		t.Fatalf("pull subscribe: %v", err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ { // concurrent JetStream publishers ($JS.API service imports)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = js.Publish("js.x", []byte("x"), nats.AckWait(500*time.Millisecond))
			}
		}()
	}
	wg.Add(1)
	go func() { // consumer fetch loop: delivery is the read side
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			msgs, _ := pull.Fetch(20, nats.MaxWait(200*time.Millisecond))
			for _, m := range msgs {
				_ = m.Ack()
			}
		}
	}()
	wg.Add(1)
	go func() { // ReloadOptions loop: the write side
		defer wg.Done()
		tk := time.NewTicker(5 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				if err := srv.ReloadOptions(mkOpts()); err != nil {
					t.Errorf("reload: %v", err)
					return
				}
			}
		}
	}()

	time.Sleep(3 * time.Second)
	close(stop)
	wg.Wait()
}