// Copyright 2019 The NATS Authors
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

package main

import (
	natsd "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"io/ioutil"
	"testing"
	"time"
)

func setupJSMTest(t *testing.T) (srv *natsd.Server, nc *nats.Conn, jsm *JetStreamMgmt) {
	dir, err := ioutil.TempDir("", "")
	checkErr(t, err, "could not create temporary js store: %v", err)

	srv, err = natsd.NewServer(&natsd.Options{
		Port:      -1,
		StoreDir:  dir,
		JetStream: true,
	})
	checkErr(t, err, "could not start js server: %v", err)

	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Errorf("nats server did not start")
	}

	nc, err = nats.Connect(srv.ClientURL())
	checkErr(t, err, "could not connect client to server @ %s: %v", srv.ClientURL(), err)

	jsm = NewJSM(nc, time.Second)

	sets, err := jsm.MessageSets()
	checkErr(t, err, "could not load sets: %v", err)
	if len(sets) != 0 {
		t.Fatalf("found %v message sets but it should be empty", sets)
	}

	return srv, nc, jsm
}

func setupObsTest(t *testing.T) (srv *natsd.Server, nc *nats.Conn, jsm *JetStreamMgmt) {
	srv, nc, jsm = setupJSMTest(t)

	err := jsm.MessageSetCreate(mem1MS())
	checkErr(t, err, "could not create message set: %v", err)
	mem1ShouldExist(t, jsm)

	return srv, nc, jsm
}

func mem1ShouldNotExist(t *testing.T, jsm *JetStreamMgmt) {
	known, err := jsm.IsMessageSetKnown("mem1")
	checkErr(t, err, "ms lookup failed: %v", err)
	if known {
		t.Fatal("unexpectedly found mem1 already existing")
	}
}

func msShouldExist(t *testing.T, jsm *JetStreamMgmt, set string) {
	known, err := jsm.IsMessageSetKnown(set)
	checkErr(t, err, "ms lookup failed: %v", err)
	if !known {
		t.Fatalf("%s does not exist", set)
	}
}

func mem1ShouldExist(t *testing.T, jsm *JetStreamMgmt) {
	msShouldExist(t, jsm, "mem1")
}

func obsShouldExist(t *testing.T, jsm *JetStreamMgmt, set string, obs string) {
	known, err := jsm.IsObservableKnown(set, obs)
	checkErr(t, err, "obs lookup failed: %v", err)
	if !known {
		t.Fatalf("%s does not exist", obs)
	}
}

func push1ShouldExist(t *testing.T, jsm *JetStreamMgmt, set string) {
	obsShouldExist(t, jsm, set, "push1")
}

func push1ShouldNotExist(t *testing.T, jsm *JetStreamMgmt, set string) {
	known, err := jsm.IsObservableKnown(set, "push1")
	checkErr(t, err, "obs lookup failed: %v", err)
	if known {
		t.Fatal("push1 should not exist")
	}
}

func mem1MS() *natsd.MsgSetConfig {
	return &natsd.MsgSetConfig{
		Name:     "mem1",
		Subjects: []string{"js.mem.>"},
		Storage:  natsd.MemoryStorage,
	}
}

func file1MS() *natsd.MsgSetConfig {
	return &natsd.MsgSetConfig{
		Name:     "file1",
		Subjects: []string{"js.file.>"},
		Storage:  natsd.FileStorage,
	}
}

func push1Obs() *natsd.ObservableConfig {
	return &natsd.ObservableConfig{
		Durable:    "push1",
		DeliverAll: true,
		AckPolicy:  natsd.AckExplicit,
	}
}

func TestObservableNext(t *testing.T) {
	srv, nc, jsm := setupObsTest(t)
	defer srv.Shutdown()

	err := jsm.ObservableCreate("mem1", push1Obs())
	checkErr(t, err, "could not create observable: %v", err)
	push1ShouldExist(t, jsm, "mem1")

	_, err = nc.Request("js.mem.1", []byte("hello"), time.Second)
	checkErr(t, err, "could not publish to mem1: %v", err)

	item, err := jsm.ObservableNext("mem1", "push1")
	checkErr(t, err, "could not get next: %v", err)
	if string(item.Data) != "hello" {
		t.Fatalf("got incorrect item body on next, expected 'hello': %v", string(item.Data))
	}
}

func TestObservableInfo(t *testing.T) {
	srv, _, jsm := setupObsTest(t)
	defer srv.Shutdown()

	err := jsm.ObservableCreate("mem1", push1Obs())
	checkErr(t, err, "could not create observable: %v", err)
	push1ShouldExist(t, jsm, "mem1")

	info, err := jsm.ObservableInfo("mem1", "push1")
	checkErr(t, err, "could not load observable: %v", err)

	if info.Config.Durable != "push1" {
		t.Fatalf("expected obs push1 but got %s", info.Config.Durable)
	}
}

func TestObservableDelete(t *testing.T) {
	srv, _, jsm := setupObsTest(t)
	defer srv.Shutdown()

	err := jsm.ObservableCreate("mem1", push1Obs())
	checkErr(t, err, "could not create observable: %v", err)
	push1ShouldExist(t, jsm, "mem1")

	err = jsm.ObservableDelete("mem1", "push1")
	checkErr(t, err, "could not delete observable: %v", err)
	push1ShouldNotExist(t, jsm, "mem1")
}

func TestObservables(t *testing.T) {
	srv, _, jsm := setupObsTest(t)
	defer srv.Shutdown()

	known, err := jsm.Observables("mem1")
	checkErr(t, err, "could not get observables: %v", err)
	if len(known) != 0 {
		t.Fatalf("found observables %v before any got created", known)
	}

	err = jsm.ObservableCreate("mem1", push1Obs())
	checkErr(t, err, "could not create observable: %v", err)
	push1ShouldExist(t, jsm, "mem1")

	known, err = jsm.Observables("mem1")
	checkErr(t, err, "could not get observables: %v", err)
	if len(known) != 1 {
		t.Fatalf("expected observables [push1] got %v", known)
	}

	if known[0] != "push1" {
		t.Fatalf("found observable %v but expected [push1]", known)
	}
}

func TestIsObservableKnown(t *testing.T) {
	srv, _, jsm := setupObsTest(t)
	defer srv.Shutdown()

	known, err := jsm.IsObservableKnown("mem1", "push1")
	checkErr(t, err, "could not get observables: %v", err)
	if known {
		t.Fatal("push1 observable exist before creation")
	}

	err = jsm.ObservableCreate("mem1", push1Obs())
	checkErr(t, err, "could not create observable: %v", err)
	push1ShouldExist(t, jsm, "mem1")

	known, err = jsm.IsObservableKnown("mem1", "push1")
	checkErr(t, err, "could not get observables: %v", err)
	if !known {
		t.Fatal("push1 observable does not exist after creation")
	}
}

func TestOvervableCreate(t *testing.T) {
	srv, _, jsm := setupObsTest(t)
	defer srv.Shutdown()

	err := jsm.ObservableCreate("mem1", push1Obs())
	checkErr(t, err, "could not create observable: %v", err)
	push1ShouldExist(t, jsm, "mem1")
}

func TestMessageSetInfo(t *testing.T) {
	srv, _, jsm := setupJSMTest(t)
	defer srv.Shutdown()

	err := jsm.MessageSetCreate(mem1MS())
	checkErr(t, err, "could not create message set: %v", err)
	mem1ShouldExist(t, jsm)

	err = jsm.MessageSetCreate(file1MS())
	checkErr(t, err, "could not create message set: %v", err)
	msShouldExist(t, jsm, "file1")

	info, err := jsm.MessageSetInfo("mem1")
	checkErr(t, err, "could not get message set: %v", err)
	if info.Config.Name != "mem1" {
		t.Fatalf("expected info for mem1, got %s", info.Config.Name)
	}
}

func TestMessageSetGetItem(t *testing.T) {
	srv, nc, jsm := setupJSMTest(t)
	defer srv.Shutdown()

	err := jsm.MessageSetCreate(mem1MS())
	checkErr(t, err, "could not create message set: %v", err)
	mem1ShouldExist(t, jsm)

	_, err = nc.Request("js.mem.1", []byte("hello"), time.Second)
	checkErr(t, err, "could not publish message: %v", err)

	item, err := jsm.MessageSetGetItem("mem1", 1)
	checkErr(t, err, "could not get message: %v", err)

	if string(item.Data) != "hello" {
		t.Fatalf("got incorrect data from message, expected 'hello' got: %v", string(item.Data))
	}
}

func TestMessageSetPurge(t *testing.T) {
	srv, nc, jsm := setupJSMTest(t)
	defer srv.Shutdown()

	err := jsm.MessageSetCreate(mem1MS())
	checkErr(t, err, "could not create message set: %v", err)
	mem1ShouldExist(t, jsm)

	_, err = nc.Request("js.mem.1", []byte("hello"), time.Second)
	checkErr(t, err, "could not publish message: %v", err)

	i, err := jsm.MessageSetInfo("mem1")
	checkErr(t, err, "could not get message set info: %v", err)
	if i.Stats.Msgs != 1 {
		t.Fatalf("expected 1 message but got %d", i.Stats.Msgs)
	}

	err = jsm.MessageSetPurge("mem1")
	checkErr(t, err, "could not purge message set: %v", err)

	i, err = jsm.MessageSetInfo("mem1")
	checkErr(t, err, "could not get message set info: %v", err)
	if i.Stats.Msgs != 0 {
		t.Fatalf("expected no message but got %d", i.Stats.Msgs)
	}
}

func TestMessageSetDelete(t *testing.T) {
	srv, _, jsm := setupJSMTest(t)
	defer srv.Shutdown()

	err := jsm.MessageSetCreate(mem1MS())
	checkErr(t, err, "could not create message set: %v", err)
	mem1ShouldExist(t, jsm)

	err = jsm.MessageSetDelete("mem1")
	checkErr(t, err, "could not delete message set: %v", err)
	mem1ShouldNotExist(t, jsm)
}

func TestMessageSets(t *testing.T) {
	srv, _, jsm := setupJSMTest(t)
	defer srv.Shutdown()

	sets, err := jsm.MessageSets()
	checkErr(t, err, "could not load known sets: %v", err)

	if len(sets) != 0 {
		t.Fatalf("MessageSets() reports %d but expected 0", len(sets))
	}

	err = jsm.MessageSetCreate(mem1MS())
	checkErr(t, err, "could not create message set: %v", err)

	sets, err = jsm.MessageSets()
	checkErr(t, err, "could not load known sets: %v", err)

	if len(sets) != 1 {
		t.Fatalf("MessageSets() reports %d but expected 1", len(sets))
	}

	if sets[0] != "mem1" {
		t.Fatalf("MessageSets() reported %s but expected mem1", sets[0])
	}
}

func TestIsMessageSetKnown(t *testing.T) {
	srv, _, jsm := setupJSMTest(t)
	defer srv.Shutdown()

	known, err := jsm.IsMessageSetKnown("mem1")
	checkErr(t, err, "could not load known sets: %v", err)
	if known {
		t.Fatal("reported 'testing' ms exist when it does not")
	}

	err = jsm.MessageSetCreate(mem1MS())
	checkErr(t, err, "could not create message set: %v", err)

	known, err = jsm.IsMessageSetKnown("mem1")
	checkErr(t, err, "could not load known sets: %v", err)
	if !known {
		t.Fatal("reported 'testing' ms does not exist when it does")
	}

	sets, err := jsm.MessageSets()
	checkErr(t, err, "could not load known sets: %v", err)
	if sets[0] != "mem1" && len(sets) == 1 {
		t.Fatalf("reported mem1 exist but it does not")
	}
}

func TestMessageSetCreate(t *testing.T) {
	srv, _, jsm := setupJSMTest(t)
	defer srv.Shutdown()

	err := jsm.MessageSetCreate(mem1MS())
	checkErr(t, err, "could not create message set: %v", err)

	mem1ShouldExist(t, jsm)
}
