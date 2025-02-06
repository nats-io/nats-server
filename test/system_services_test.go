// Copyright 2019-2023 The NATS Authors
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
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const dbgSubs = "$SYS.DEBUG.SUBSCRIBERS"

func (sc *supercluster) selectRandomServer() *server.Options {
	ci := rand.Int31n(int32(len(sc.clusters)))
	si := rand.Int31n(int32(len(sc.clusters[ci].servers)))
	return sc.clusters[ci].opts[si]
}

func (sc *supercluster) setupSystemServicesImports(t *testing.T, account string) {
	t.Helper()
	for _, c := range sc.clusters {
		for _, s := range c.servers {
			sysAcc := s.SystemAccount()
			if sysAcc == nil {
				t.Fatalf("System account not set")
			}
			acc, err := s.LookupAccount(account)
			if err != nil {
				t.Fatalf("Error looking up account '%s': %v", account, err)
			}
			if err := acc.AddServiceImport(sysAcc, dbgSubs, dbgSubs); err != nil {
				t.Fatalf("Error adding subscribers debug service to '%s': %v", account, err)
			}
		}
	}
}

func numSubs(t *testing.T, msg *nats.Msg) int {
	t.Helper()
	if msg == nil || msg.Data == nil {
		t.Fatalf("No response")
	}
	n, err := strconv.Atoi(string(msg.Data))
	if err != nil {
		t.Fatalf("Got non-number response: %v", err)
	}
	return n
}

func checkDbgNumSubs(t *testing.T, nc *nats.Conn, subj string, expected int) {
	t.Helper()
	var n int
	for i := 0; i < 3; i++ {
		response, err := nc.Request(dbgSubs, []byte(subj), 250*time.Millisecond)
		if err != nil {
			continue
		}
		if n = numSubs(t, response); n == expected {
			return
		}
	}
	t.Fatalf("Expected %d subscribers, got %d", expected, n)
}

func TestSystemServiceSubscribers(t *testing.T) {
	numServers, numClusters := 3, 3
	sc := createSuperCluster(t, numServers, numClusters)
	defer sc.shutdown()

	sc.setupSystemServicesImports(t, "FOO")

	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
	defer nc.Close()

	checkInterest := func(expected int) {
		t.Helper()
		checkDbgNumSubs(t, nc, "foo.bar", expected)
	}

	checkInterest(0)

	// Now add in local subscribers.
	for i := 0; i < 5; i++ {
		nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
		defer nc.Close()
		nc.SubscribeSync("foo.bar")
		nc.SubscribeSync("foo.*")
		nc.Flush()
	}

	checkInterest(10)

	// Now create remote subscribers at random.
	for i := 0; i < 90; i++ {
		nc := clientConnect(t, sc.selectRandomServer(), "foo")
		defer nc.Close()
		nc.SubscribeSync("foo.bar")
		nc.Flush()
	}

	checkInterest(100)
}

// Test that we can match wildcards. So sub may be foo.bar and we ask about foo.*, that should work.
func TestSystemServiceSubscribersWildcards(t *testing.T) {
	numServers, numClusters := 3, 3
	sc := createSuperCluster(t, numServers, numClusters)
	defer sc.shutdown()

	sc.setupSystemServicesImports(t, "FOO")

	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
	defer nc.Close()

	for i := 0; i < 50; i++ {
		nc := clientConnect(t, sc.selectRandomServer(), "foo")
		defer nc.Close()
		nc.SubscribeSync(fmt.Sprintf("foo.bar.%d", i+1))
		nc.SubscribeSync(fmt.Sprintf("%d", i+1))
		nc.Flush()
	}

	checkDbgNumSubs(t, nc, "foo.bar.*", 50)

	checkDbgNumSubs(t, nc, "foo.>", 50)

	checkDbgNumSubs(t, nc, "foo.bar.22", 1)

	response, _ := nc.Request(dbgSubs, []byte("_INBOX.*.*"), time.Second)
	hasInbox := numSubs(t, response)

	checkDbgNumSubs(t, nc, ">", 100+hasInbox)
}

// Test that we can match on queue groups as well. Separate request payload with any whitespace.
func TestSystemServiceSubscribersQueueGroups(t *testing.T) {
	numServers, numClusters := 3, 3
	sc := createSuperCluster(t, numServers, numClusters)
	defer sc.shutdown()

	sc.setupSystemServicesImports(t, "FOO")

	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
	defer nc.Close()

	for i := 0; i < 10; i++ {
		nc := clientConnect(t, sc.selectRandomServer(), "foo")
		defer nc.Close()
		subj := fmt.Sprintf("foo.bar.%d", i+1)
		nc.QueueSubscribeSync(subj, "QG.11")
		nc.QueueSubscribeSync("foo.baz", "QG.33")
		nc.Flush()
	}

	for i := 0; i < 23; i++ {
		nc := clientConnect(t, sc.selectRandomServer(), "foo")
		defer nc.Close()
		subj := fmt.Sprintf("foo.bar.%d", i+1)
		nc.QueueSubscribeSync(subj, "QG.22")
		nc.QueueSubscribeSync("foo.baz", "QG.22")
		nc.Flush()
	}

	checkDbgNumSubs(t, nc, "foo.bar.*", 33)

	checkDbgNumSubs(t, nc, "foo.bar.22 QG.22", 1)

	checkDbgNumSubs(t, nc, "foo.bar.2", 2)

	checkDbgNumSubs(t, nc, "foo.baz", 33)

	checkDbgNumSubs(t, nc, "foo.baz QG.22", 23)

	// Now check qfilters work on wildcards too.
	checkDbgNumSubs(t, nc, "foo.bar.> QG.11", 10)

	checkDbgNumSubs(t, nc, "*.baz QG.22", 23)

	checkDbgNumSubs(t, nc, "foo.*.22 QG.22", 1)
}

func TestSystemServiceSubscribersLeafNodesWithoutSystem(t *testing.T) {
	numServers, numClusters := 3, 3
	sc := createSuperCluster(t, numServers, numClusters)
	defer sc.shutdown()

	sc.setupSystemServicesImports(t, "FOO")

	ci := rand.Int31n(int32(len(sc.clusters)))
	si := rand.Int31n(int32(len(sc.clusters[ci].servers)))
	s, opts := sc.clusters[ci].servers[si], sc.clusters[ci].opts[si]
	url := fmt.Sprintf("nats://%s:pass@%s:%d", "foo", opts.Host, opts.LeafNode.Port)
	ls, lopts := runSolicitLeafServerToURL(url)
	defer ls.Shutdown()

	checkLeafNodeConnected(t, s)

	// This is so we can test when the subs on a leafnode are flushed to the connected supercluster.
	fsubj := "__leaf.flush__"
	fc := clientConnect(t, opts, "foo")
	defer fc.Close()
	fc.Subscribe(fsubj, func(m *nats.Msg) {
		m.Respond(nil)
	})

	lnc := clientConnect(t, lopts, "$G")
	defer lnc.Close()

	flushLeaf := func() {
		if _, err := lnc.Request(fsubj, nil, time.Second); err != nil {
			t.Fatalf("Did not flush through to the supercluster: %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		nc := clientConnect(t, sc.selectRandomServer(), "foo")
		defer nc.Close()
		nc.SubscribeSync(fmt.Sprintf("foo.bar.%d", i+1))
		nc.QueueSubscribeSync("foo.bar.baz", "QG.22")
		nc.Flush()
	}

	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
	defer nc.Close()

	checkDbgNumSubs(t, nc, "foo.bar.*", 20)

	checkDbgNumSubs(t, nc, "foo.bar.3", 1)

	lnc.SubscribeSync("foo.bar.3")
	lnc.QueueSubscribeSync("foo.bar.baz", "QG.22")

	// We could flush here but that does not guarantee we have flushed through to the supercluster.
	flushLeaf()

	checkDbgNumSubs(t, nc, "foo.bar.3", 2)

	checkDbgNumSubs(t, nc, "foo.bar.baz QG.22", 11)

	lnc.SubscribeSync("foo.bar.3")
	lnc.QueueSubscribeSync("foo.bar.baz", "QG.22")
	flushLeaf()

	// For now we do not see all the details behind a leafnode if the leafnode is not enabled.
	checkDbgNumSubs(t, nc, "foo.bar.3", 2)

	checkDbgNumSubs(t, nc, "foo.bar.baz QG.22", 12)
}

func runSolicitLeafServerWithSystemToURL(surl string) (*server.Server, *server.Options) {
	o := DefaultTestOptions
	o.Port = -1
	fooAcc := server.NewAccount("FOO")
	o.Accounts = []*server.Account{server.NewAccount("$SYS"), fooAcc}
	o.SystemAccount = "$SYS"
	o.Users = []*server.User{
		{Username: "foo", Password: "pass", Permissions: nil, Account: fooAcc},
	}
	rurl, _ := url.Parse(surl)
	sysUrl, _ := url.Parse(strings.Replace(surl, rurl.User.Username(), "sys", -1))
	o.LeafNode.Remotes = []*server.RemoteLeafOpts{
		{
			URLs:         []*url.URL{rurl},
			LocalAccount: "FOO",
		},
		{
			URLs:         []*url.URL{sysUrl},
			LocalAccount: "$SYS",
		},
	}
	o.LeafNode.ReconnectInterval = 100 * time.Millisecond
	return RunServer(&o), &o
}

func TestSystemServiceSubscribersLeafNodesWithSystem(t *testing.T) {
	numServers, numClusters := 3, 3
	sc := createSuperCluster(t, numServers, numClusters)
	defer sc.shutdown()

	sc.setupSystemServicesImports(t, "FOO")

	ci := rand.Int31n(int32(len(sc.clusters)))
	si := rand.Int31n(int32(len(sc.clusters[ci].servers)))
	s, opts := sc.clusters[ci].servers[si], sc.clusters[ci].opts[si]
	url := fmt.Sprintf("nats://%s:pass@%s:%d", "foo", opts.Host, opts.LeafNode.Port)
	ls, lopts := runSolicitLeafServerWithSystemToURL(url)
	defer ls.Shutdown()

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if nln := s.NumLeafNodes(); nln != 2 {
			return fmt.Errorf("Expected a connected leafnode for server %q, got none", s.ID())
		}
		return nil
	})

	// This is so we can test when the subs on a leafnode are flushed to the connected supercluster.
	fsubj := "__leaf.flush__"
	fc := clientConnect(t, opts, "foo")
	defer fc.Close()
	fc.Subscribe(fsubj, func(m *nats.Msg) {
		m.Respond(nil)
	})

	lnc := clientConnect(t, lopts, "foo")
	defer lnc.Close()

	flushLeaf := func() {
		if _, err := lnc.Request(fsubj, nil, time.Second); err != nil {
			t.Fatalf("Did not flush through to the supercluster: %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		nc := clientConnect(t, sc.selectRandomServer(), "foo")
		defer nc.Close()
		nc.SubscribeSync(fmt.Sprintf("foo.bar.%d", i+1))
		nc.QueueSubscribeSync("foo.bar.baz", "QG.22")
		nc.Flush()
	}

	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
	defer nc.Close()

	checkDbgNumSubs(t, nc, "foo.bar.3", 1)

	checkDbgNumSubs(t, nc, "foo.bar.*", 20)

	lnc.SubscribeSync("foo.bar.3")
	lnc.QueueSubscribeSync("foo.bar.baz", "QG.22")
	flushLeaf()

	// Since we are doing real tracking now on the other side, this will be off by 1 since we are counting
	// the leaf and the real sub.
	checkDbgNumSubs(t, nc, "foo.bar.3", 3)

	checkDbgNumSubs(t, nc, "foo.bar.baz QG.22", 12)
}
