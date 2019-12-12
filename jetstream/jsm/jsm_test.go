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
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	api "github.com/nats-io/nats-server/v2/server"
)

func runJsmCli(t *testing.T, args ...string) (output []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := fmt.Sprintf("go run $(ls *.go | grep -v _test.go) %s", strings.Join(args, " "))
	execution := exec.CommandContext(ctx, "bash", "-c", cmd)
	out, err := execution.CombinedOutput()
	if err != nil {
		t.Fatalf("jsm utility failed: %v\n%v", err, string(out))
	}

	return out
}

func TestCLIMSCreate(t *testing.T) {
	srv, _, jsm := setupJSMTest(t)
	defer srv.Shutdown()

	runJsmCli(t, fmt.Sprintf("--server='%s' ms create mem1 --subjects 'js.mem.>,js.other' --storage m  --max-msgs=-1 --max-age=-1 --max-bytes=-1 --ack", srv.ClientURL()))
	mem1ShouldExist(t, jsm)
	info, err := jsm.MessageSetInfo("mem1")
	checkErr(t, err, "could not fetch message set: %v", err)
	if len(info.Config.Subjects) != 2 {
		t.Fatalf("expected 2 subjects in the message set, got %v", info.Config.Subjects)
	}
	if info.Config.Subjects[0] != "js.mem.>" && info.Config.Subjects[1] != "js.other" {
		t.Fatalf("expects [js.mem.>, js.other] got %v", info.Config.Subjects)
	}
}

func TestCLIMSInfo(t *testing.T) {
	srv, _, jsm := setupJSMTest(t)
	defer srv.Shutdown()

	err := jsm.MessageSetCreate(mem1MS())
	checkErr(t, err, "could not create message set: %v", err)
	mem1ShouldExist(t, jsm)

	out := runJsmCli(t, fmt.Sprintf("--server='%s' ms info mem1 -j", srv.ClientURL()))

	var info api.MsgSetInfo
	err = json.Unmarshal(out, &info)
	checkErr(t, err, "could not parse cli output: %v", err)

	if info.Config.Name != "mem1" {
		t.Fatalf("expected info for mem1, got %s", info.Config.Name)

	}
}

func TestCLIMSDelete(t *testing.T) {
	srv, _, jsm := setupJSMTest(t)
	defer srv.Shutdown()

	err := jsm.MessageSetCreate(mem1MS())
	checkErr(t, err, "could not create message set: %v", err)
	mem1ShouldExist(t, jsm)

	runJsmCli(t, fmt.Sprintf("--server='%s' ms rm mem1 -f", srv.ClientURL()))
	mem1ShouldNotExist(t, jsm)
}

func TestCLIMSLs(t *testing.T) {
	srv, _, jsm := setupJSMTest(t)
	defer srv.Shutdown()

	err := jsm.MessageSetCreate(mem1MS())
	checkErr(t, err, "could not create message set: %v", err)
	mem1ShouldExist(t, jsm)

	out := runJsmCli(t, fmt.Sprintf("--server='%s' ms ls -j", srv.ClientURL()))

	list := []string{}
	err = json.Unmarshal(out, &list)
	checkErr(t, err, "could not parse cli output: %v", err)

	if len(list) != 1 {
		t.Fatalf("expected 1 ms got %v", list)
	}

	if list[0] != "mem1" {
		t.Fatalf("expected [mem1] got %v", list)
	}
}

func TestCLIMSPurge(t *testing.T) {
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

	runJsmCli(t, fmt.Sprintf("--server='%s' ms purge mem1 -f", srv.ClientURL()))
	i, err = jsm.MessageSetInfo("mem1")
	checkErr(t, err, "could not get message set info: %v", err)
	if i.Stats.Msgs != 0 {
		t.Fatalf("expected 0 messages but got %d", i.Stats.Msgs)
	}
}

func TestCLIMSGet(t *testing.T) {
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

	out := runJsmCli(t, fmt.Sprintf("--server='%s' ms get mem1 1 -j", srv.ClientURL()))
	err = json.Unmarshal(out, item)
	checkErr(t, err, "could not parse output: %v", err)
	if string(item.Data) != "hello" {
		t.Fatalf("got incorrect data from message, expected 'hello' got: %v", string(item.Data))
	}
}

func TestCLIObsInfo(t *testing.T) {
	srv, _, jsm := setupObsTest(t)
	defer srv.Shutdown()

	err := jsm.ObservableCreate("mem1", push1Obs())
	checkErr(t, err, "could not create observable: %v", err)
	push1ShouldExist(t, jsm, "mem1")

	out := runJsmCli(t, fmt.Sprintf("--server='%s' obs info mem1 push1 -j", srv.ClientURL()))
	var info api.ObservableInfo
	err = json.Unmarshal(out, &info)
	checkErr(t, err, "could not parse output: %v", err)

	if info.Config.Durable != "push1" {
		t.Fatalf("did not find into for push1 in cli output: %v", string(out))
	}
}

func TestCLIObsLs(t *testing.T) {
	srv, _, jsm := setupObsTest(t)
	defer srv.Shutdown()

	err := jsm.ObservableCreate("mem1", push1Obs())
	checkErr(t, err, "could not create observable: %v", err)
	push1ShouldExist(t, jsm, "mem1")

	out := runJsmCli(t, fmt.Sprintf("--server='%s' obs ls mem1 -j", srv.ClientURL()))
	var info []string
	err = json.Unmarshal(out, &info)
	checkErr(t, err, "could not parse output: %v", err)

	if len(info) != 1 {
		t.Fatalf("expected 1 item in output received %d", len(info))
	}

	if info[0] != "push1" {
		t.Fatalf("did not find into for push1 in cli output: %v", string(out))
	}
}

func TestCLIObsDelete(t *testing.T) {
	srv, _, jsm := setupObsTest(t)
	defer srv.Shutdown()

	err := jsm.ObservableCreate("mem1", push1Obs())
	checkErr(t, err, "could not create observable: %v", err)
	push1ShouldExist(t, jsm, "mem1")

	runJsmCli(t, fmt.Sprintf("--server='%s' obs rm mem1 push1 -f", srv.ClientURL()))

	list, err := jsm.Observables("mem1")
	checkErr(t, err, "could not check observable: %v", err)
	if len(list) != 0 {
		t.Fatalf("Expected no observables, got %v", list)
	}
}

func TestCLIObsAdd(t *testing.T) {
	srv, _, jsm := setupObsTest(t)
	defer srv.Shutdown()

	runJsmCli(t, fmt.Sprintf("--server='%s' obs add mem1 push1 --replay instant --deliver all --pull", srv.ClientURL()))
	push1ShouldExist(t, jsm, "mem1")
}

func TestCLIObsNext(t *testing.T) {
	srv, nc, jsm := setupObsTest(t)
	defer srv.Shutdown()

	err := jsm.ObservableCreate("mem1", push1Obs())
	checkErr(t, err, "could not create observable: %v", err)
	push1ShouldExist(t, jsm, "mem1")

	_, err = nc.Request("js.mem.1", []byte("hello"), time.Second)
	checkErr(t, err, "could not publish to mem1: %v", err)

	out := runJsmCli(t, fmt.Sprintf("--server='%s' obs next mem1 push1 --raw", srv.ClientURL()))

	if strings.TrimSpace(string(out)) != "hello" {
		t.Fatalf("did not receive 'hello', got: '%s'", string(out))
	}
}
