// Copyright 2020-2025 The NATS Authors
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

//go:build !skip_js_tests

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func TestJetStreamLeafNodeUniqueServerNameCrossJSDomain(t *testing.T) {
	name := "NOT-UNIQUE"
	test := func(t *testing.T, s *Server, sIdExpected string, srvs ...*Server) {
		ids := map[string]string{}
		for _, srv := range srvs {
			checkLeafNodeConnectedCount(t, srv, 2)
			ids[srv.ID()] = srv.opts.JetStreamDomain
		}
		// ensure that an update for every server was received
		sysNc := natsConnect(t, fmt.Sprintf("nats://admin:s3cr3t!@127.0.0.1:%d", s.opts.Port))
		defer sysNc.Close()
		sub, err := sysNc.SubscribeSync(fmt.Sprintf(serverStatsSubj, "*"))
		require_NoError(t, err)
		for {
			m, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
			tk := strings.Split(m.Subject, ".")
			if domain, ok := ids[tk[2]]; ok {
				delete(ids, tk[2])
				require_Contains(t, string(m.Data), fmt.Sprintf(`"domain":"%s"`, domain))
			}
			if len(ids) == 0 {
				break
			}
		}
		cnt := 0
		s.nodeToInfo.Range(func(key, value any) bool {
			cnt++
			require_Equal(t, value.(nodeInfo).name, name)
			require_Equal(t, value.(nodeInfo).id, sIdExpected)
			return true
		})
		require_Equal(t, cnt, 1)
	}
	tmplA := `
		listen: -1
		server_name: %s
		jetstream {
			max_mem_store: 256MB,
			max_file_store: 2GB,
			store_dir: '%s',
			domain: hub
		}
		accounts {
			JSY { users = [ { user: "y", pass: "p" } ]; jetstream: true }
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
		leaf {
			port: -1
		}
    `
	tmplL := `
		listen: -1
		server_name: %s
		jetstream {
			max_mem_store: 256MB,
			max_file_store: 2GB,
			store_dir: '%s',
			domain: %s
		}
		accounts {
			JSY { users = [ { user: "y", pass: "p" } ]; jetstream: true }
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
		leaf {
			remotes [
				{ urls: [ %s ], account: "JSY" }
				{ urls: [ %s ], account: "$SYS" }
			]
		}
    `
	t.Run("same-domain", func(t *testing.T) {
		confA := createConfFile(t, []byte(fmt.Sprintf(tmplA, name, t.TempDir())))
		sA, oA := RunServerWithConfig(confA)
		defer sA.Shutdown()
		// using same domain as sA
		confL := createConfFile(t, []byte(fmt.Sprintf(tmplL, name, t.TempDir(), "hub",
			fmt.Sprintf("nats://y:p@127.0.0.1:%d", oA.LeafNode.Port),
			fmt.Sprintf("nats://admin:s3cr3t!@127.0.0.1:%d", oA.LeafNode.Port))))
		sL, _ := RunServerWithConfig(confL)
		defer sL.Shutdown()
		// as server name uniqueness is violates, sL.ID() is the expected value
		test(t, sA, sL.ID(), sA, sL)
	})
	t.Run("different-domain", func(t *testing.T) {
		confA := createConfFile(t, []byte(fmt.Sprintf(tmplA, name, t.TempDir())))
		sA, oA := RunServerWithConfig(confA)
		defer sA.Shutdown()
		// using different domain as sA
		confL := createConfFile(t, []byte(fmt.Sprintf(tmplL, name, t.TempDir(), "spoke",
			fmt.Sprintf("nats://y:p@127.0.0.1:%d", oA.LeafNode.Port),
			fmt.Sprintf("nats://admin:s3cr3t!@127.0.0.1:%d", oA.LeafNode.Port))))
		sL, _ := RunServerWithConfig(confL)
		defer sL.Shutdown()
		checkLeafNodeConnectedCount(t, sL, 2)
		checkLeafNodeConnectedCount(t, sA, 2)
		// ensure sA contains only sA.ID
		test(t, sA, sA.ID(), sA, sL)
	})
}

func TestJetStreamLeafNodeJwtPermsAndJSDomains(t *testing.T) {
	createAcc := func(js bool) (string, string, nkeys.KeyPair) {
		kp, _ := nkeys.CreateAccount()
		aPub, _ := kp.PublicKey()
		claim := jwt.NewAccountClaims(aPub)
		if js {
			claim.Limits.JetStreamLimits = jwt.JetStreamLimits{
				MemoryStorage: 1024 * 1024,
				DiskStorage:   1024 * 1024,
				Streams:       1, Consumer: 2}
		}
		aJwt, err := claim.Encode(oKp)
		require_NoError(t, err)
		return aPub, aJwt, kp
	}
	sysPub, sysJwt, sysKp := createAcc(false)
	accPub, accJwt, accKp := createAcc(true)
	noExpiration := time.Now().Add(time.Hour)
	// create user for acc to be used in leaf node.
	lnCreds := createUserWithLimit(t, accKp, noExpiration, func(j *jwt.UserPermissionLimits) {
		j.Sub.Deny.Add("subdeny")
		j.Pub.Deny.Add("pubdeny")
	})
	unlimitedCreds := createUserWithLimit(t, accKp, noExpiration, nil)

	sysCreds := createUserWithLimit(t, sysKp, noExpiration, nil)

	tmplA := `
operator: %s
system_account: %s
resolver: MEMORY
resolver_preload: {
  %s: %s
  %s: %s
}
listen: 127.0.0.1:-1
leafnodes: {
	listen: 127.0.0.1:-1
}
jetstream :{
    domain: "cluster"
    store_dir: '%s'
    max_mem: 100Mb
    max_file: 100Mb
}
`

	tmplL := `
listen: 127.0.0.1:-1
accounts :{
    A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
    SYS:{ users:[ {user:s1,password:s1}]},
}
system_account = SYS
jetstream: {
    domain: ln1
    store_dir: '%s'
    max_mem: 50Mb
    max_file: 50Mb
}
leafnodes:{
    remotes:[{ url:nats://127.0.0.1:%d, account: A, credentials: '%s'},
			 { url:nats://127.0.0.1:%d, account: SYS, credentials: '%s'}]
}
`

	confA := createConfFile(t, []byte(fmt.Sprintf(tmplA, ojwt, sysPub,
		sysPub, sysJwt, accPub, accJwt,
		t.TempDir())))
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()

	confL := createConfFile(t, []byte(fmt.Sprintf(tmplL, t.TempDir(),
		sA.opts.LeafNode.Port, lnCreds, sA.opts.LeafNode.Port, sysCreds)))
	sL, _ := RunServerWithConfig(confL)
	defer sL.Shutdown()

	checkLeafNodeConnectedCount(t, sA, 2)
	checkLeafNodeConnectedCount(t, sL, 2)

	ncA := natsConnect(t, sA.ClientURL(), nats.UserCredentials(unlimitedCreds))
	defer ncA.Close()

	ncL := natsConnect(t, fmt.Sprintf("nats://a1:a1@127.0.0.1:%d", sL.opts.Port))
	defer ncL.Close()

	test := func(subject string, cSub, cPub *nats.Conn, remoteServerForSub *Server, accName string, pass bool) {
		t.Helper()
		sub, err := cSub.SubscribeSync(subject)
		require_NoError(t, err)
		require_NoError(t, cSub.Flush())
		// ensure the subscription made it across, or if not sent due to sub deny, make sure it could have made it.
		if remoteServerForSub == nil {
			time.Sleep(200 * time.Millisecond)
		} else {
			checkSubInterest(t, remoteServerForSub, accName, subject, time.Second)
		}
		require_NoError(t, cPub.Publish(subject, []byte("hello world")))
		require_NoError(t, cPub.Flush())
		m, err := sub.NextMsg(500 * time.Millisecond)
		if pass {
			require_NoError(t, err)
			require_True(t, m.Subject == subject)
			require_Equal(t, string(m.Data), "hello world")
		} else {
			require_True(t, err == nats.ErrTimeout)
		}
	}

	t.Run("sub-on-ln-pass", func(t *testing.T) {
		test("sub", ncL, ncA, sA, accPub, true)
	})
	t.Run("sub-on-ln-fail", func(t *testing.T) {
		test("subdeny", ncL, ncA, nil, "", false)
	})
	t.Run("pub-on-ln-pass", func(t *testing.T) {
		test("pub", ncA, ncL, sL, "A", true)
	})
	t.Run("pub-on-ln-fail", func(t *testing.T) {
		test("pubdeny", ncA, ncL, nil, "A", false)
	})
}

func TestJetStreamLeafNodeClusterExtensionWithSystemAccount(t *testing.T) {
	/*
		Topologies tested here
		same == true
		A  <-> B
		^ |\
		|   \
		|  proxy
		|     \
		LA <-> LB

		same == false
		A  <-> B
		^      ^
		|      |
		|    proxy
		|      |
		LA <-> LB

		The proxy is turned on later, such that the system account connection can be started later, in a controlled way
		This explicitly tests the system state before and after this happens.
	*/

	tmplA := `
listen: 127.0.0.1:-1
accounts :{
    A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
    SYS:{ users:[ {user:s1,password:s1}]},
}
system_account: SYS
leafnodes: {
	listen: 127.0.0.1:-1
	no_advertise: true
	authorization: {
		timeout: 0.5
	}
}
jetstream :{
    domain: "cluster"
    store_dir: '%s'
    max_mem: 100Mb
    max_file: 100Mb
}
server_name: A
cluster: {
	name: clust1
	listen: 127.0.0.1:20104
	routes=[nats-route://127.0.0.1:20105]
	no_advertise: true
}
`

	tmplB := `
listen: 127.0.0.1:-1
accounts :{
    A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
    SYS:{ users:[ {user:s1,password:s1}]},
}
system_account: SYS
leafnodes: {
	listen: 127.0.0.1:-1
	no_advertise: true
	authorization: {
		timeout: 0.5
	}
}
jetstream: {
    domain: "cluster"
    store_dir: '%s'
    max_mem: 100Mb
    max_file: 100Mb
}
server_name: B
cluster: {
	name: clust1
	listen: 127.0.0.1:20105
	routes=[nats-route://127.0.0.1:20104]
	no_advertise: true
}
`

	tmplLA := `
listen: 127.0.0.1:-1
accounts :{
    A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
    SYS:{ users:[ {user:s1,password:s1}]},
}
system_account = SYS
jetstream: {
    domain: "cluster"
    store_dir: '%s'
    max_mem: 50Mb
    max_file: 50Mb
	%s
}
server_name: LA
cluster: {
	name: clustL
	listen: 127.0.0.1:20106
	routes=[nats-route://127.0.0.1:20107]
	no_advertise: true
}
leafnodes:{
	no_advertise: true
    remotes:[{url:nats://a1:a1@127.0.0.1:%d, account: A},
		     {url:nats://s1:s1@127.0.0.1:%d, account: SYS}]
}
`

	tmplLB := `
listen: 127.0.0.1:-1
accounts :{
    A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
    SYS:{ users:[ {user:s1,password:s1}]},
}
system_account = SYS
jetstream: {
    domain: "cluster"
    store_dir: '%s'
    max_mem: 50Mb
    max_file: 50Mb
	%s
}
server_name: LB
cluster: {
	name: clustL
	listen: 127.0.0.1:20107
	routes=[nats-route://127.0.0.1:20106]
	no_advertise: true
}
leafnodes:{
	no_advertise: true
    remotes:[{url:nats://a1:a1@127.0.0.1:%d, account: A},
		     {url:nats://s1:s1@127.0.0.1:%d, account: SYS}]
}
`

	for _, testCase := range []struct {
		// which topology to pick
		same bool
		// If leaf server should be operational and form a Js cluster prior to joining.
		// In this setup this would be an error as you give the wrong hint.
		// But this should work itself out regardless
		leafFunctionPreJoin bool
	}{
		{true, true},
		{true, false},
		{false, true},
		{false, false}} {
		t.Run(fmt.Sprintf("%t-%t", testCase.same, testCase.leafFunctionPreJoin), func(t *testing.T) {
			sd1 := t.TempDir()
			confA := createConfFile(t, []byte(fmt.Sprintf(tmplA, sd1)))
			sA, _ := RunServerWithConfig(confA)
			defer sA.Shutdown()

			sd2 := t.TempDir()
			confB := createConfFile(t, []byte(fmt.Sprintf(tmplB, sd2)))
			sB, _ := RunServerWithConfig(confB)
			defer sB.Shutdown()

			checkClusterFormed(t, sA, sB)

			c := cluster{t: t, servers: []*Server{sA, sB}}
			c.waitOnLeader()

			// starting this will allow the second remote in tmplL to successfully connect.
			port := sB.opts.LeafNode.Port
			if testCase.same {
				port = sA.opts.LeafNode.Port
			}
			p := &proxyAcceptDetectFailureLate{acceptPort: port}
			defer p.close()
			lPort := p.runEx(t, true)

			hint := ""
			if testCase.leafFunctionPreJoin {
				hint = fmt.Sprintf("extension_hint: %s", strings.ToUpper(jsNoExtend))
			}

			sd3 := t.TempDir()
			// deliberately pick server sA and proxy
			confLA := createConfFile(t, []byte(fmt.Sprintf(tmplLA, sd3, hint, sA.opts.LeafNode.Port, lPort)))
			sLA, _ := RunServerWithConfig(confLA)
			defer sLA.Shutdown()

			sd4 := t.TempDir()
			// deliberately pick server sA and proxy
			confLB := createConfFile(t, []byte(fmt.Sprintf(tmplLB, sd4, hint, sA.opts.LeafNode.Port, lPort)))
			sLB, _ := RunServerWithConfig(confLB)
			defer sLB.Shutdown()

			checkClusterFormed(t, sLA, sLB)

			strmCfg := func(name, placementCluster string) *nats.StreamConfig {
				if placementCluster == "" {
					return &nats.StreamConfig{Name: name, Replicas: 1, Subjects: []string{name}}
				}
				return &nats.StreamConfig{Name: name, Replicas: 1, Subjects: []string{name},
					Placement: &nats.Placement{Cluster: placementCluster}}
			}
			// Only after the system account is fully connected can streams be placed anywhere.
			testJSFunctions := func(pass bool) {
				ncA := natsConnect(t, fmt.Sprintf("nats://a1:a1@127.0.0.1:%d", sA.opts.Port))
				defer ncA.Close()
				jsA, err := ncA.JetStream()
				require_NoError(t, err)
				_, err = jsA.AddStream(strmCfg(fmt.Sprintf("fooA1-%t", pass), ""))
				require_NoError(t, err)
				_, err = jsA.AddStream(strmCfg(fmt.Sprintf("fooA2-%t", pass), "clust1"))
				require_NoError(t, err)
				_, err = jsA.AddStream(strmCfg(fmt.Sprintf("fooA3-%t", pass), "clustL"))
				if pass {
					require_NoError(t, err)
				} else {
					require_Error(t, err)
					require_Contains(t, err.Error(), "no suitable peers for placement")
				}
				ncL := natsConnect(t, fmt.Sprintf("nats://a1:a1@127.0.0.1:%d", sLA.opts.Port))
				defer ncL.Close()
				jsL, err := ncL.JetStream()
				require_NoError(t, err)
				_, err = jsL.AddStream(strmCfg(fmt.Sprintf("fooL1-%t", pass), ""))
				require_NoError(t, err)
				_, err = jsL.AddStream(strmCfg(fmt.Sprintf("fooL2-%t", pass), "clustL"))
				require_NoError(t, err)
				_, err = jsL.AddStream(strmCfg(fmt.Sprintf("fooL3-%t", pass), "clust1"))
				if pass {
					require_NoError(t, err)
				} else {
					require_Error(t, err)
					require_Contains(t, err.Error(), "no suitable peers for placement")
				}
			}
			clusterLnCnt := func(expected int) error {
				cnt := 0
				for _, s := range c.servers {
					cnt += s.NumLeafNodes()
				}
				if cnt == expected {
					return nil
				}
				return fmt.Errorf("not enought leaf node connections, got %d needed %d", cnt, expected)
			}

			// Even though there are two remotes defined in tmplL, only one will be able to connect.
			checkFor(t, 10*time.Second, time.Second/4, func() error { return clusterLnCnt(2) })
			checkLeafNodeConnectedCount(t, sLA, 1)
			checkLeafNodeConnectedCount(t, sLB, 1)
			c.waitOnPeerCount(2)

			if testCase.leafFunctionPreJoin {
				cl := cluster{t: t, servers: []*Server{sLA, sLB}}
				cl.waitOnLeader()
				cl.waitOnPeerCount(2)
				testJSFunctions(false)
			} else {
				// In cases where the leaf nodes have to wait for the system account to connect,
				// JetStream should not be operational during that time
				ncA := natsConnect(t, fmt.Sprintf("nats://a1:a1@127.0.0.1:%d", sLA.opts.Port))
				defer ncA.Close()
				jsA, err := ncA.JetStream()
				require_NoError(t, err)
				_, err = jsA.AddStream(strmCfg("fail-false", ""))
				require_Error(t, err)
			}
			// Starting the proxy will connect the system accounts.
			// After they are connected the clusters are merged.
			// Once this happened, all streams in test can be placed anywhere in the cluster.
			// Before that only the cluster the client is connected to can be used for placement
			p.start()

			// Even though there are two remotes defined in tmplL, only one will be able to connect.
			checkFor(t, 10*time.Second, time.Second/4, func() error { return clusterLnCnt(4) })
			checkLeafNodeConnectedCount(t, sLA, 2)
			checkLeafNodeConnectedCount(t, sLB, 2)

			// The leader will reside in the main cluster only
			c.waitOnPeerCount(4)
			testJSFunctions(true)
		})
	}
}

func TestJetStreamLeafNodeClusterMixedModeExtensionWithSystemAccount(t *testing.T) {
	/*  Topology used in this test:
	CLUSTER(A <-> B <-> C (NO JS))
	      	            ^
	                    |
	                    LA
	*/

	// once every server is up, we expect these peers to be part of the JetStream meta cluster
	expectedJetStreamPeers := map[string]struct{}{
		"A":  {},
		"B":  {},
		"LA": {},
	}

	tmplA := `
listen: 127.0.0.1:-1
accounts :{
    A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
    SYS:{ users:[ {user:s1,password:s1}]},
}
system_account: SYS
leafnodes: {
	listen: 127.0.0.1:-1
	no_advertise: true
	authorization: {
		timeout: 0.5
	}
}
jetstream: { %s store_dir: '%s'; max_mem: 50Mb, max_file: 50Mb }
server_name: A
cluster: {
	name: clust1
	listen: 127.0.0.1:20114
	routes=[nats-route://127.0.0.1:20115,nats-route://127.0.0.1:20116]
	no_advertise: true
}
`

	tmplB := `
listen: 127.0.0.1:-1
accounts :{
    A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
    SYS:{ users:[ {user:s1,password:s1}]},
}
system_account: SYS
leafnodes: {
	listen: 127.0.0.1:-1
	no_advertise: true
	authorization: {
		timeout: 0.5
	}
}
jetstream: { %s store_dir: '%s'; max_mem: 50Mb, max_file: 50Mb }
server_name: B
cluster: {
	name: clust1
	listen: 127.0.0.1:20115
	routes=[nats-route://127.0.0.1:20114,nats-route://127.0.0.1:20116]
	no_advertise: true
}
`

	tmplC := `
listen: 127.0.0.1:-1
accounts :{
    A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
    SYS:{ users:[ {user:s1,password:s1}]},
}
system_account: SYS
leafnodes: {
	listen: 127.0.0.1:-1
	no_advertise: true
	authorization: {
		timeout: 0.5
	}
}
jetstream: {
	enabled: false
	%s
}
server_name: C
cluster: {
	name: clust1
	listen: 127.0.0.1:20116
	routes=[nats-route://127.0.0.1:20114,nats-route://127.0.0.1:20115]
	no_advertise: true
}
`

	tmplLA := `
listen: 127.0.0.1:-1
accounts :{
    A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
    SYS:{ users:[ {user:s1,password:s1}]},
}
system_account = SYS
# the extension hint is to simplify this test. without it present we would need a cluster of size 2
jetstream: { %s store_dir: '%s'; max_mem: 50Mb, max_file: 50Mb, extension_hint: will_extend }
server_name: LA
leafnodes:{
	no_advertise: true
    remotes:[{url:nats://a1:a1@127.0.0.1:%d, account: A},
		     {url:nats://s1:s1@127.0.0.1:%d, account: SYS}]
}
# add the cluster here so we can test placement
cluster: { name: clustL }
`
	for _, withDomain := range []bool{true, false} {
		t.Run(fmt.Sprintf("with-domain:%t", withDomain), func(t *testing.T) {
			var jsDisabledDomainString string
			var jsEnabledDomainString string
			if withDomain {
				jsEnabledDomainString = `domain: "domain", `
				jsDisabledDomainString = `domain: "domain"`
			} else {
				// in case no domain name is set, fall back to the extension hint.
				// since JS is disabled, the value of this does not clash with other uses.
				jsDisabledDomainString = "extension_hint: will_extend"
			}

			sd1 := t.TempDir()
			confA := createConfFile(t, []byte(fmt.Sprintf(tmplA, jsEnabledDomainString, sd1)))
			sA, _ := RunServerWithConfig(confA)
			defer sA.Shutdown()

			sd2 := t.TempDir()
			confB := createConfFile(t, []byte(fmt.Sprintf(tmplB, jsEnabledDomainString, sd2)))
			sB, _ := RunServerWithConfig(confB)
			defer sB.Shutdown()

			confC := createConfFile(t, []byte(fmt.Sprintf(tmplC, jsDisabledDomainString)))
			sC, _ := RunServerWithConfig(confC)
			defer sC.Shutdown()

			checkClusterFormed(t, sA, sB, sC)
			c := cluster{t: t, servers: []*Server{sA, sB, sC}}
			c.waitOnPeerCount(2)

			sd3 := t.TempDir()
			// deliberately pick server sC (no JS) to connect to
			confLA := createConfFile(t, []byte(fmt.Sprintf(tmplLA, jsEnabledDomainString, sd3, sC.opts.LeafNode.Port, sC.opts.LeafNode.Port)))
			sLA, _ := RunServerWithConfig(confLA)
			defer sLA.Shutdown()

			checkLeafNodeConnectedCount(t, sC, 2)
			checkLeafNodeConnectedCount(t, sLA, 2)
			c.waitOnPeerCount(3)
			peers := c.leader().JetStreamClusterPeers()
			for _, peer := range peers {
				if _, ok := expectedJetStreamPeers[peer]; !ok {
					t.Fatalf("Found unexpected peer %q", peer)
				}
			}

			// helper to create stream config with uniqe name and subject
			cnt := 0
			strmCfg := func(placementCluster string) *nats.StreamConfig {
				name := fmt.Sprintf("s-%d", cnt)
				cnt++
				if placementCluster == "" {
					return &nats.StreamConfig{Name: name, Replicas: 1, Subjects: []string{name}}
				}
				return &nats.StreamConfig{Name: name, Replicas: 1, Subjects: []string{name},
					Placement: &nats.Placement{Cluster: placementCluster}}
			}

			test := func(port int, expectedDefPlacement string) {
				ncA := natsConnect(t, fmt.Sprintf("nats://a1:a1@127.0.0.1:%d", port))
				defer ncA.Close()
				jsA, err := ncA.JetStream()
				require_NoError(t, err)
				si, err := jsA.AddStream(strmCfg(""))
				require_NoError(t, err)
				require_Contains(t, si.Cluster.Name, expectedDefPlacement)
				si, err = jsA.AddStream(strmCfg("clust1"))
				require_NoError(t, err)
				require_Contains(t, si.Cluster.Name, "clust1")
				si, err = jsA.AddStream(strmCfg("clustL"))
				require_NoError(t, err)
				require_Contains(t, si.Cluster.Name, "clustL")
			}

			test(sA.opts.Port, "clust1")
			test(sB.opts.Port, "clust1")
			test(sC.opts.Port, "clust1")
			test(sLA.opts.Port, "clustL")
		})
	}
}

// checkSysLeafDenied verifies that the leaf connection bound to the system
// account on s has the JS API denies merged in both directions, i.e. that
// s itself treats the connection as isolated rather than relying on the
// other end to hold back traffic.
func checkSysLeafDenied(t *testing.T, s *Server) {
	t.Helper()
	sysAcc := s.SystemAccount()
	checkFor(t, 2*time.Second, 25*time.Millisecond, func() error {
		var found, denied bool
		s.mu.RLock()
		for _, ln := range s.leafs {
			ln.mu.Lock()
			if ln.acc == sysAcc {
				found = true
				if ln.perms != nil && ln.perms.pub.deny != nil && ln.perms.sub.deny != nil {
					rp := ln.perms.pub.deny.Match(jsAllAPI)
					rs := ln.perms.sub.deny.Match(jsAllAPI)
					denied = len(rp.psubs)+len(rp.qsubs) > 0 && len(rs.psubs)+len(rs.qsubs) > 0
				}
			}
			ln.mu.Unlock()
		}
		s.mu.RUnlock()
		if !found {
			return fmt.Errorf("no system account leaf connection on %q", s.Name())
		}
		if !denied {
			return fmt.Errorf("system account leaf connection on %q is missing JS API denies", s.Name())
		}
		return nil
	})
}

// Config templates shared by the leaf node domain / system account tests
// below. Hub slots: server name, store dir, optional cluster block.
// Leaf slots: server name, JS domain, store dir, optional extension_hint
// line, optional cluster block, remotes.
const lnDomainHubTmpl = `
listen: 127.0.0.1:-1
server_name: %s
accounts {
	A { jetstream: enabled, users: [ { user: a, password: pwd } ] }
	SYS { users: [ { user: s, password: pwd } ] }
}
system_account: SYS
jetstream {
	domain: hub
	store_dir: '%s'
	max_mem: 50Mb
	max_file: 50Mb
}
%s
leafnodes {
	listen: 127.0.0.1:-1
	no_advertise: true
}
`

const lnDomainLeafTmpl = `
listen: 127.0.0.1:-1
server_name: %s
accounts {
	A { jetstream: enabled, users: [ { user: a, password: pwd } ] }
	SYS { users: [ { user: s, password: pwd } ] }
}
system_account: SYS
jetstream {
	domain: %s
	store_dir: '%s'
	max_mem: 50Mb
	max_file: 50Mb
	%s
}
%s
leafnodes {
	remotes [
		%s
	]
}
`

// Like lnDomainLeafTmpl, but the server also runs its own leafnode listener so
// further leaves can chain below it. Slots: server name, JS domain, store dir,
// optional extension_hint line, remotes.
const lnDomainLeafHubTmpl = `
listen: 127.0.0.1:-1
server_name: %s
accounts {
	A { jetstream: enabled, users: [ { user: a, password: pwd } ] }
	SYS { users: [ { user: s, password: pwd } ] }
}
system_account: SYS
jetstream {
	domain: %s
	store_dir: '%s'
	max_mem: 50Mb
	max_file: 50Mb
	%s
}
leafnodes {
	listen: 127.0.0.1:-1
	no_advertise: true
	remotes [
		%s
	]
}
`

// lnDomainClusterBlock renders the cluster block for the templates above.
func lnDomainClusterBlock(name string, listen, route int) string {
	return fmt.Sprintf(`cluster {
	name: %s
	listen: 127.0.0.1:%d
	routes = [nats-route://127.0.0.1:%d]
	no_advertise: true
}`, name, listen, route)
}

// lnDomainRemotes renders leafnode remotes pointing at lnPort, always binding
// account A and, if shareSys is set, the system account as well.
func lnDomainRemotes(lnPort int, shareSys bool) string {
	remotes := fmt.Sprintf(`{ url: "nats://a:pwd@127.0.0.1:%d", account: A }`, lnPort)
	if shareSys {
		remotes += fmt.Sprintf("\n\t\t{ url: \"nats://s:pwd@127.0.0.1:%d\", account: SYS }", lnPort)
	}
	return remotes
}

// checkSysJSAPICross checks whether subject interest and messages on
// $JS.API.> in the (shared) system account cross the leaf connection, probing
// both directions. A control subject proves the link itself propagates
// interest either way.
func checkSysJSAPICross(t *testing.T, hub, leaf *Server, expectCross bool) {
	t.Helper()
	ncLeaf := natsConnect(t, fmt.Sprintf("nats://s:pwd@127.0.0.1:%d", leaf.opts.Port))
	defer ncLeaf.Close()
	ncHub := natsConnect(t, fmt.Sprintf("nats://s:pwd@127.0.0.1:%d", hub.opts.Port))
	defer ncHub.Close()

	// Leaf-side subscriptions probe the hub->leaf direction, hub-side
	// subscriptions the leaf->hub direction.
	ctlLeaf, err := ncLeaf.SubscribeSync("sysprobe.ctl.leaf")
	require_NoError(t, err)
	probeLeaf, err := ncLeaf.SubscribeSync("$JS.API.SYSPROBE.LEAF")
	require_NoError(t, err)
	require_NoError(t, ncLeaf.Flush())
	ctlHub, err := ncHub.SubscribeSync("sysprobe.ctl.hub")
	require_NoError(t, err)
	probeHub, err := ncHub.SubscribeSync("$JS.API.SYSPROBE.HUB")
	require_NoError(t, err)
	require_NoError(t, ncHub.Flush())

	// Wait until the control subject interest crossed the connection(s).
	checkSubInterest(t, hub, "SYS", "sysprobe.ctl.leaf", 2*time.Second)
	checkSubInterest(t, leaf, "SYS", "sysprobe.ctl.hub", 2*time.Second)

	require_NoError(t, ncHub.Publish("sysprobe.ctl.leaf", nil))
	require_NoError(t, ncHub.Publish("$JS.API.SYSPROBE.LEAF", nil))
	require_NoError(t, ncHub.Flush())
	require_NoError(t, ncLeaf.Publish("sysprobe.ctl.hub", nil))
	require_NoError(t, ncLeaf.Publish("$JS.API.SYSPROBE.HUB", nil))
	require_NoError(t, ncLeaf.Flush())

	_, err = ctlLeaf.NextMsg(2 * time.Second)
	require_NoError(t, err)
	_, err = ctlHub.NextMsg(2 * time.Second)
	require_NoError(t, err)
	for direction, probe := range map[string]*nats.Subscription{"hub->leaf": probeLeaf, "leaf->hub": probeHub} {
		_, err = probe.NextMsg(500 * time.Millisecond)
		if expectCross && err != nil {
			t.Fatalf("Expected JS API traffic to cross %s, got %v", direction, err)
		} else if !expectCross && err == nil {
			t.Fatalf("Expected no JS API traffic to cross %s, but it did", direction)
		}
	}
}

// TestJetStreamLeafNodeDomainSysAccountPermutations verifies leaf node + JetStream
// behavior for the permutations of same/different JS domain and shared/not-shared
// system account between hub and leaf.
func TestJetStreamLeafNodeDomainSysAccountPermutations(t *testing.T) {
	const cport1, cport2 = 23320, 23321
	const lport1, lport2 = 23330, 23331

	startHub := func(t *testing.T) (*Server, *Server) {
		t.Helper()
		conf1 := createConfFile(t, []byte(fmt.Sprintf(lnDomainHubTmpl, "HUB1", t.TempDir(), lnDomainClusterBlock("HUB", cport1, cport2))))
		h1, _ := RunServerWithConfig(conf1)
		t.Cleanup(h1.Shutdown)
		conf2 := createConfFile(t, []byte(fmt.Sprintf(lnDomainHubTmpl, "HUB2", t.TempDir(), lnDomainClusterBlock("HUB", cport2, cport1))))
		h2, _ := RunServerWithConfig(conf2)
		t.Cleanup(h2.Shutdown)
		checkClusterFormed(t, h1, h2)
		c := cluster{t: t, servers: []*Server{h1, h2}}
		c.waitOnLeader()
		return h1, h2
	}
	startLeaf := func(t *testing.T, hub *Server, domain string, shareSys bool, hint string) *Server {
		t.Helper()
		if hint != _EMPTY_ {
			hint = fmt.Sprintf("extension_hint: %s", hint)
		}
		remotes := lnDomainRemotes(hub.opts.LeafNode.Port, shareSys)
		conf := createConfFile(t, []byte(fmt.Sprintf(lnDomainLeafTmpl, "LEAF", domain, t.TempDir(), hint, _EMPTY_, remotes)))
		s, _ := RunServerWithConfig(conf)
		t.Cleanup(s.Shutdown)
		return s
	}

	startHubSingle := func(t *testing.T) *Server {
		t.Helper()
		conf := createConfFile(t, []byte(fmt.Sprintf(lnDomainHubTmpl, "SHUB", t.TempDir(), _EMPTY_)))
		s, _ := RunServerWithConfig(conf)
		t.Cleanup(s.Shutdown)
		return s
	}
	startLeafCluster := func(t *testing.T, hub *Server, domain string, shareSys bool) (*Server, *Server) {
		t.Helper()
		remotes := lnDomainRemotes(hub.opts.LeafNode.Port, shareSys)
		conf1 := createConfFile(t, []byte(fmt.Sprintf(lnDomainLeafTmpl, "LEAF1", domain, t.TempDir(), _EMPTY_, lnDomainClusterBlock("LEAF", lport1, lport2), remotes)))
		l1, _ := RunServerWithConfig(conf1)
		t.Cleanup(l1.Shutdown)
		conf2 := createConfFile(t, []byte(fmt.Sprintf(lnDomainLeafTmpl, "LEAF2", domain, t.TempDir(), _EMPTY_, lnDomainClusterBlock("LEAF", lport2, lport1), remotes)))
		l2, _ := RunServerWithConfig(conf2)
		t.Cleanup(l2.Shutdown)
		checkClusterFormed(t, l1, l2)
		return l1, l2
	}

	// accountDomain asks the JS account info API and returns the reported domain.
	accountDomain := func(t *testing.T, nc *nats.Conn) string {
		t.Helper()
		resp, err := nc.Request(JSApiAccountInfo, nil, 2*time.Second)
		require_NoError(t, err)
		var info JSApiAccountInfoResponse
		require_NoError(t, json.Unmarshal(resp.Data, &info))
		require_True(t, info.Error == nil)
		return info.Domain
	}

	// countJSAPIResponses sends a single account info request and counts how many
	// servers answer it. More than one response means JS API cross-talk over the
	// leaf node connection.
	countJSAPIResponses := func(t *testing.T, nc *nats.Conn) int {
		t.Helper()
		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(inbox)
		require_NoError(t, err)
		defer sub.Unsubscribe()
		require_NoError(t, nc.PublishRequest(JSApiAccountInfo, inbox, nil))
		require_NoError(t, nc.Flush())
		time.Sleep(500 * time.Millisecond)
		n, _, err := sub.Pending()
		require_NoError(t, err)
		return n
	}

	metaPeerCount := func(s *Server) int {
		if js := s.getJetStream(); js != nil {
			if mg := js.getMetaGroup(); mg != nil {
				return len(mg.Peers())
			}
		}
		return 0
	}

	t.Run("different-domain-different-sysacct", func(t *testing.T) {
		h1, _ := startHub(t)
		leaf := startLeaf(t, h1, "leaf", false, _EMPTY_)
		checkLeafNodeConnectedCount(t, h1, 1)
		checkLeafNodeConnectedCount(t, leaf, 1)

		ncHub, jsHub := jsClientConnect(t, h1, nats.UserInfo("a", "pwd"))
		defer ncHub.Close()
		ncLeaf, jsLeaf := jsClientConnect(t, leaf, nats.UserInfo("a", "pwd"))
		defer ncLeaf.Close()

		// Each side serves its own domain.
		require_Equal(t, accountDomain(t, ncHub), "hub")
		require_Equal(t, accountDomain(t, ncLeaf), "leaf")

		// Both sides can create a stream with the same name: fully isolated.
		_, err := jsHub.AddStream(&nats.StreamConfig{Name: "CLASH", Subjects: []string{"hub.foo"}})
		require_NoError(t, err)
		_, err = jsLeaf.AddStream(&nats.StreamConfig{Name: "CLASH", Subjects: []string{"leaf.foo"}})
		require_NoError(t, err)

		// A stream created on the leaf is not visible from the hub.
		_, err = jsLeaf.AddStream(&nats.StreamConfig{Name: "LEAFONLY", Subjects: []string{"leaf.only"}})
		require_NoError(t, err)
		_, err = jsHub.StreamInfo("LEAFONLY")
		require_Error(t, err, nats.ErrStreamNotFound)

		// Exactly one server answers a JS API request: no cross-talk.
		require_Equal(t, countJSAPIResponses(t, ncHub), 1)
		require_Equal(t, countJSAPIResponses(t, ncLeaf), 1)

		// Hub meta cluster does not contain the leaf, leaf runs standalone.
		require_Equal(t, metaPeerCount(h1), 2)
		require_Equal(t, metaPeerCount(leaf), 0)
	})

	t.Run("same-domain-same-sysacct-extension", func(t *testing.T) {
		h1, h2 := startHub(t)
		leaf := startLeaf(t, h1, "hub", true, jsWillExtend)
		checkLeafNodeConnectedCount(t, h1, 2)
		checkLeafNodeConnectedCount(t, leaf, 2)

		// The leaf extends the hub's JS domain: it joins the meta cluster.
		c := cluster{t: t, servers: []*Server{h1, h2}}
		c.waitOnLeader()
		c.waitOnPeerCount(3)

		ncHub, jsHub := jsClientConnect(t, h1, nats.UserInfo("a", "pwd"))
		defer ncHub.Close()
		ncLeaf, jsLeaf := jsClientConnect(t, leaf, nats.UserInfo("a", "pwd"))
		defer ncLeaf.Close()

		// Both sides report the same domain.
		require_Equal(t, accountDomain(t, ncHub), "hub")
		require_Equal(t, accountDomain(t, ncLeaf), "hub")

		// A stream created via the leaf is visible from the hub: one JS domain.
		_, err := jsLeaf.AddStream(&nats.StreamConfig{Name: "EXT", Subjects: []string{"ext.foo"}})
		require_NoError(t, err)
		_, err = jsHub.StreamInfo("EXT")
		require_NoError(t, err)

		// Exactly one response to JS API requests, even with multiple servers.
		require_Equal(t, countJSAPIResponses(t, ncHub), 1)
		require_Equal(t, countJSAPIResponses(t, ncLeaf), 1)

		// System account JS API traffic is expected to cross the leaf connection.
		checkSysJSAPICross(t, h1, leaf, true)
	})

	t.Run("same-domain-different-sysacct", func(t *testing.T) {
		h1, _ := startHub(t)
		leaf := startLeaf(t, h1, "hub", false, _EMPTY_)
		checkLeafNodeConnectedCount(t, h1, 1)
		checkLeafNodeConnectedCount(t, leaf, 1)

		ncHub, jsHub := jsClientConnect(t, h1, nats.UserInfo("a", "pwd"))
		defer ncHub.Close()
		ncLeaf, jsLeaf := jsClientConnect(t, leaf, nats.UserInfo("a", "pwd"))
		defer ncLeaf.Close()

		// Both claim the same domain name, but are two independent JS instances.
		require_Equal(t, accountDomain(t, ncHub), "hub")
		require_Equal(t, accountDomain(t, ncLeaf), "hub")

		_, err := jsHub.AddStream(&nats.StreamConfig{Name: "CLASH", Subjects: []string{"hub.foo"}})
		require_NoError(t, err)
		_, err = jsLeaf.AddStream(&nats.StreamConfig{Name: "CLASH", Subjects: []string{"leaf.foo"}})
		require_NoError(t, err)

		_, err = jsLeaf.AddStream(&nats.StreamConfig{Name: "LEAFONLY", Subjects: []string{"leaf.only"}})
		require_NoError(t, err)
		_, err = jsHub.StreamInfo("LEAFONLY")
		require_Error(t, err, nats.ErrStreamNotFound)

		// Despite the same domain name, the automatic client JS API denies
		// prevent duplicate answers over the leaf connection.
		require_Equal(t, countJSAPIResponses(t, ncHub), 1)
		require_Equal(t, countJSAPIResponses(t, ncLeaf), 1)

		require_Equal(t, metaPeerCount(h1), 2)
		require_Equal(t, metaPeerCount(leaf), 0)
	})

	t.Run("different-domain-same-sysacct", func(t *testing.T) {
		h1, _ := startHub(t)
		leaf := startLeaf(t, h1, "leaf", true, _EMPTY_)
		checkLeafNodeConnectedCount(t, h1, 2)
		checkLeafNodeConnectedCount(t, leaf, 2)

		ncHub, jsHub := jsClientConnect(t, h1, nats.UserInfo("a", "pwd"))
		defer ncHub.Close()
		ncLeaf, jsLeaf := jsClientConnect(t, leaf, nats.UserInfo("a", "pwd"))
		defer ncLeaf.Close()

		// Isolated domains, each side answers with its own.
		require_Equal(t, accountDomain(t, ncHub), "hub")
		require_Equal(t, accountDomain(t, ncLeaf), "leaf")

		// Streams stay local to each domain.
		_, err := jsHub.AddStream(&nats.StreamConfig{Name: "CLASH", Subjects: []string{"hub.foo"}})
		require_NoError(t, err)
		_, err = jsLeaf.AddStream(&nats.StreamConfig{Name: "CLASH", Subjects: []string{"leaf.foo"}})
		require_NoError(t, err)
		_, err = jsLeaf.AddStream(&nats.StreamConfig{Name: "LEAFONLY", Subjects: []string{"leaf.only"}})
		require_NoError(t, err)
		_, err = jsHub.StreamInfo("LEAFONLY")
		require_Error(t, err, nats.ErrStreamNotFound)

		require_Equal(t, countJSAPIResponses(t, ncHub), 1)
		require_Equal(t, countJSAPIResponses(t, ncLeaf), 1)

		// The leaf never joins the hub's meta cluster and runs standalone.
		require_Equal(t, metaPeerCount(h1), 2)
		require_Equal(t, metaPeerCount(leaf), 0)

		// Crucially: JS API (meta) traffic on the shared system account must NOT
		// cross the leaf connection since the domains differ.
		checkSysJSAPICross(t, h1, leaf, false)

		// Cross-domain access via the domain-prefixed API works through the
		// shared user account: the hub client can address the leaf domain.
		resp, err := ncHub.Request("$JS.leaf.API.INFO", nil, 2*time.Second)
		require_NoError(t, err)
		var info JSApiAccountInfoResponse
		require_NoError(t, json.Unmarshal(resp.Data, &info))
		require_True(t, info.Error == nil)
		require_Equal(t, info.Domain, "leaf")
	})

	// Misconfiguration: the leaf shares the system account and uses the same
	// domain, but is a standalone server without "extension_hint: will_extend".
	// It then does NOT extend the hub's JS domain but runs its own JetStream
	// under the same domain name. Since a standalone JetStream server can not
	// take part in a shared meta group, both sides must treat this connection
	// as isolated and deny JS API traffic on the system account.
	t.Run("same-domain-same-sysacct-standalone-no-hint", func(t *testing.T) {
		h1, _ := startHub(t)
		leaf := startLeaf(t, h1, "hub", true, _EMPTY_)
		checkLeafNodeConnectedCount(t, h1, 2)
		checkLeafNodeConnectedCount(t, leaf, 2)

		ncHub, _ := jsClientConnect(t, h1, nats.UserInfo("a", "pwd"))
		defer ncHub.Close()
		ncLeaf, _ := jsClientConnect(t, leaf, nats.UserInfo("a", "pwd"))
		defer ncLeaf.Close()

		// The leaf did not join the meta cluster: two JS instances, one domain name.
		require_Equal(t, metaPeerCount(h1), 2)
		require_Equal(t, metaPeerCount(leaf), 0)

		// The connection is treated as isolated: JS API traffic on the shared
		// system account does NOT cross the leaf connection.
		checkSysJSAPICross(t, h1, leaf, false)

		// Both ends must have merged the denies themselves: the leaf from its
		// own local check, the hub from the CONNECT echo of the leaf's
		// standalone state.
		checkSysLeafDenied(t, leaf)
		checkSysLeafDenied(t, h1)

		// User account requests are wrapped in a service import on the local
		// server, so they still get exactly one answer.
		require_Equal(t, countJSAPIResponses(t, ncHub), 1)
		require_Equal(t, countJSAPIResponses(t, ncLeaf), 1)
	})

	// A clustered leaf has its own meta controller and CAN extend: same domain
	// plus shared system account must still merge the meta groups (and needs no
	// extension hint).
	t.Run("clustered-leaf-same-domain-same-sysacct-extension", func(t *testing.T) {
		h1, h2 := startHub(t)
		l1, l2 := startLeafCluster(t, h1, "hub", true)
		checkLeafNodeConnectedCount(t, h1, 4)
		checkLeafNodeConnectedCount(t, l1, 2)
		checkLeafNodeConnectedCount(t, l2, 2)

		// Hub cluster (2) plus leaf cluster (2) form one meta group.
		c := cluster{t: t, servers: []*Server{h1, h2}}
		c.waitOnLeader()
		c.waitOnPeerCount(4)

		ncHub, jsHub := jsClientConnect(t, h1, nats.UserInfo("a", "pwd"))
		defer ncHub.Close()
		ncLeaf, jsLeaf := jsClientConnect(t, l1, nats.UserInfo("a", "pwd"))
		defer ncLeaf.Close()

		require_Equal(t, accountDomain(t, ncHub), "hub")
		require_Equal(t, accountDomain(t, ncLeaf), "hub")

		_, err := jsLeaf.AddStream(&nats.StreamConfig{Name: "EXT", Subjects: []string{"ext.foo"}})
		require_NoError(t, err)
		_, err = jsHub.StreamInfo("EXT")
		require_NoError(t, err)

		checkSysJSAPICross(t, h1, l1, true)
	})

	// Clustered leaf with its own domain sharing the system account: isolated,
	// the leaf cluster keeps its own meta group.
	t.Run("clustered-leaf-different-domain-same-sysacct", func(t *testing.T) {
		h1, _ := startHub(t)
		l1, l2 := startLeafCluster(t, h1, "leaf", true)
		checkLeafNodeConnectedCount(t, h1, 4)

		// The leaf cluster starts in observer mode (system account remote), and
		// must recover to its own meta group once domains turn out to differ.
		cl := cluster{t: t, servers: []*Server{l1, l2}}
		cl.waitOnLeader()
		cl.waitOnPeerCount(2)

		ncHub, jsHub := jsClientConnect(t, h1, nats.UserInfo("a", "pwd"))
		defer ncHub.Close()
		ncLeaf, jsLeaf := jsClientConnect(t, l1, nats.UserInfo("a", "pwd"))
		defer ncLeaf.Close()

		require_Equal(t, accountDomain(t, ncHub), "hub")
		require_Equal(t, accountDomain(t, ncLeaf), "leaf")

		_, err := jsLeaf.AddStream(&nats.StreamConfig{Name: "LEAFONLY", Subjects: []string{"leaf.only"}})
		require_NoError(t, err)
		_, err = jsHub.StreamInfo("LEAFONLY")
		require_Error(t, err, nats.ErrStreamNotFound)

		require_Equal(t, metaPeerCount(h1), 2)

		checkSysJSAPICross(t, h1, l1, false)
	})

	// A clustered leaf pointed at a standalone JetStream hub: the hub has no
	// meta controller, so nothing can be extended even though domains match and
	// the system account is shared. The hub advertises this, the leaf cluster
	// must isolate (deny JS API on the system account), leave observer mode and
	// elect its own meta leader instead of waiting forever for extension.
	t.Run("clustered-leaf-same-domain-same-sysacct-standalone-hub", func(t *testing.T) {
		hub := startHubSingle(t)
		l1, l2 := startLeafCluster(t, hub, "hub", true)
		checkLeafNodeConnectedCount(t, hub, 4)

		cl := cluster{t: t, servers: []*Server{l1, l2}}
		cl.waitOnLeader()
		cl.waitOnPeerCount(2)

		ncHub, jsHub := jsClientConnect(t, hub, nats.UserInfo("a", "pwd"))
		defer ncHub.Close()
		ncLeaf, jsLeaf := jsClientConnect(t, l1, nats.UserInfo("a", "pwd"))
		defer ncLeaf.Close()

		// Both sides are functional and independent, one domain name.
		require_Equal(t, accountDomain(t, ncHub), "hub")
		require_Equal(t, accountDomain(t, ncLeaf), "hub")

		_, err := jsHub.AddStream(&nats.StreamConfig{Name: "CLASH", Subjects: []string{"hub.foo"}})
		require_NoError(t, err)
		_, err = jsLeaf.AddStream(&nats.StreamConfig{Name: "CLASH", Subjects: []string{"leaf.foo"}})
		require_NoError(t, err)
		_, err = jsLeaf.AddStream(&nats.StreamConfig{Name: "LEAFONLY", Subjects: []string{"leaf.only"}})
		require_NoError(t, err)
		_, err = jsHub.StreamInfo("LEAFONLY")
		require_Error(t, err, nats.ErrStreamNotFound)

		// The standalone hub has no meta group, the leaf cluster has its own.
		require_Equal(t, metaPeerCount(hub), 0)

		// JS API traffic on the shared system account must not cross.
		checkSysJSAPICross(t, hub, l1, false)

		// Both ends must have merged the denies themselves: the hub from its
		// own local check, the leaf from the hub's INFO.
		checkSysLeafDenied(t, hub)
		checkSysLeafDenied(t, l1)
	})

	// Both sides standalone, same domain, shared system account, no extension
	// hint: neither side has a meta controller, so nothing can be extended in
	// either direction. Both must isolate and stay independently functional.
	t.Run("standalone-hub-standalone-leaf-no-hint", func(t *testing.T) {
		hub := startHubSingle(t)
		leaf := startLeaf(t, hub, "hub", true, _EMPTY_)
		checkLeafNodeConnectedCount(t, hub, 2)
		checkLeafNodeConnectedCount(t, leaf, 2)

		ncHub, jsHub := jsClientConnect(t, hub, nats.UserInfo("a", "pwd"))
		defer ncHub.Close()
		ncLeaf, jsLeaf := jsClientConnect(t, leaf, nats.UserInfo("a", "pwd"))
		defer ncLeaf.Close()

		// Two independent JS instances, one domain name, no meta groups at all.
		require_Equal(t, accountDomain(t, ncHub), "hub")
		require_Equal(t, accountDomain(t, ncLeaf), "hub")
		require_Equal(t, metaPeerCount(hub), 0)
		require_Equal(t, metaPeerCount(leaf), 0)

		_, err := jsHub.AddStream(&nats.StreamConfig{Name: "CLASH", Subjects: []string{"hub.foo"}})
		require_NoError(t, err)
		_, err = jsLeaf.AddStream(&nats.StreamConfig{Name: "CLASH", Subjects: []string{"leaf.foo"}})
		require_NoError(t, err)
		_, err = jsLeaf.AddStream(&nats.StreamConfig{Name: "LEAFONLY", Subjects: []string{"leaf.only"}})
		require_NoError(t, err)
		_, err = jsHub.StreamInfo("LEAFONLY")
		require_Error(t, err, nats.ErrStreamNotFound)

		require_Equal(t, countJSAPIResponses(t, ncHub), 1)
		require_Equal(t, countJSAPIResponses(t, ncLeaf), 1)

		checkSysJSAPICross(t, hub, leaf, false)
		checkSysLeafDenied(t, hub)
		checkSysLeafDenied(t, leaf)
	})

	// A standalone leaf soliciting a standalone hub WITH will_extend: the
	// leaf runs a meta controller solely to join the hub's meta group, but
	// the hub advertises that it can never produce a meta leader. A single
	// server must not form a meta group of its own, so the leaf stays in
	// observer mode without a leader and its JetStream remains unavailable
	// until the configuration is corrected. The connection is still isolated
	// in both directions.
	t.Run("standalone-hub-standalone-leaf-will-extend", func(t *testing.T) {
		hub := startHubSingle(t)
		leaf := startLeaf(t, hub, "hub", true, jsWillExtend)
		checkLeafNodeConnectedCount(t, hub, 2)
		checkLeafNodeConnectedCount(t, leaf, 2)

		// Isolation applies on both ends; this also proves the extension
		// decision has been made on both sides before checking meta state.
		checkSysLeafDenied(t, hub)
		checkSysLeafDenied(t, leaf)
		checkSysJSAPICross(t, hub, leaf, false)

		// The leaf keeps its meta controller in observer mode, leaderless.
		mg := leaf.getJetStream().getMetaGroup()
		require_True(t, mg != nil)
		require_True(t, mg.IsObserver())
		require_Equal(t, mg.GroupLeader(), _EMPTY_)
		require_Equal(t, metaPeerCount(hub), 0)

		// The hub's JetStream works normally.
		ncHub, jsHub := jsClientConnect(t, hub, nats.UserInfo("a", "pwd"))
		defer ncHub.Close()
		require_Equal(t, accountDomain(t, ncHub), "hub")
		_, err := jsHub.AddStream(&nats.StreamConfig{Name: "HUBSTREAM", Subjects: []string{"hub.foo"}})
		require_NoError(t, err)

		// The leaf's JetStream is unavailable: no meta leader can answer.
		ncLeaf := natsConnect(t, fmt.Sprintf("nats://a:pwd@127.0.0.1:%d", leaf.opts.Port))
		defer ncLeaf.Close()
		jsLeaf, err := ncLeaf.JetStream(nats.MaxWait(time.Second))
		require_NoError(t, err)
		_, err = jsLeaf.AddStream(&nats.StreamConfig{Name: "LEAFONLY", Subjects: []string{"leaf.only"}})
		require_Error(t, err)
	})

	// A standalone will_extend server joins the upstream meta group, so it
	// must keep advertising itself as extendable on its own leafnode listener:
	// a leaf chained below it extends the domain THROUGH it and must not be
	// told to isolate. This is the reason a standalone server soliciting
	// extension reports extendable.
	t.Run("chained-leaf-through-standalone-will-extend", func(t *testing.T) {
		h1, h2 := startHub(t)
		hint := fmt.Sprintf("extension_hint: %s", jsWillExtend)

		// Middle: standalone will_extend server extending the hub's domain,
		// with its own leafnode listener.
		midConf := createConfFile(t, []byte(fmt.Sprintf(lnDomainLeafHubTmpl, "MID", "hub", t.TempDir(), hint,
			lnDomainRemotes(h1.opts.LeafNode.Port, true))))
		mid, _ := RunServerWithConfig(midConf)
		t.Cleanup(mid.Shutdown)
		checkLeafNodeConnectedCount(t, mid, 2)

		c := cluster{t: t, servers: []*Server{h1, h2}}
		c.waitOnLeader()
		c.waitOnPeerCount(3)

		// Bottom: another standalone will_extend leaf, chained below the
		// middle server. It must extend through it and join the meta group.
		bottom := startLeaf(t, mid, "hub", true, jsWillExtend)
		checkLeafNodeConnectedCount(t, bottom, 2)
		c.waitOnPeerCount(4)

		ncHub, jsHub := jsClientConnect(t, h1, nats.UserInfo("a", "pwd"))
		defer ncHub.Close()
		ncBottom, jsBottom := jsClientConnect(t, bottom, nats.UserInfo("a", "pwd"))
		defer ncBottom.Close()

		require_Equal(t, accountDomain(t, ncBottom), "hub")

		// A stream created via the bottom leaf is visible from the hub: one
		// JS domain across both hops.
		_, err := jsBottom.AddStream(&nats.StreamConfig{Name: "CHAINED", Subjects: []string{"chained.foo"}})
		require_NoError(t, err)
		_, err = jsHub.StreamInfo("CHAINED")
		require_NoError(t, err)

		// System account JS API traffic crosses both hops.
		checkSysJSAPICross(t, h1, bottom, true)
	})
}

// A leaf that boots with will_extend but NO system account remote
// (standalone JS, no meta controller). A config reload then adds the system
// account remote. Config-derived capability now claims extendable while the
// runtime is still standalone.
func TestJetStreamLeafNodeSysRemoteAddedByReloadNotExtended(t *testing.T) {
	h1Conf := createConfFile(t, []byte(fmt.Sprintf(lnDomainHubTmpl, "HUB1", t.TempDir(), lnDomainClusterBlock("HUB", 23480, 23481))))
	h1, _ := RunServerWithConfig(h1Conf)
	defer h1.Shutdown()
	h2Conf := createConfFile(t, []byte(fmt.Sprintf(lnDomainHubTmpl, "HUB2", t.TempDir(), lnDomainClusterBlock("HUB", 23481, 23480))))
	h2, _ := RunServerWithConfig(h2Conf)
	defer h2.Shutdown()
	checkClusterFormed(t, h1, h2)
	c := cluster{t: t, servers: []*Server{h1, h2}}
	c.waitOnLeader()

	sd := t.TempDir()
	hint := fmt.Sprintf("extension_hint: %s", jsWillExtend)
	lnPort := h1.opts.LeafNode.Port
	leafConf := createConfFile(t, []byte(fmt.Sprintf(lnDomainLeafTmpl, "LEAF", "hub", sd, hint, _EMPTY_, lnDomainRemotes(lnPort, false))))
	leaf, _ := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()
	checkLeafNodeConnectedCount(t, leaf, 1)

	// Boot state: standalone JS, no meta controller.
	require_True(t, leaf.getJetStream().getMetaGroup() == nil)

	// Reload: add the system account remote.
	reloadUpdateConfig(t, leaf, leafConf, fmt.Sprintf(lnDomainLeafTmpl, "LEAF", "hub", sd, hint, _EMPTY_, lnDomainRemotes(lnPort, true)))
	checkLeafNodeConnectedCount(t, leaf, 2)

	// Still no meta controller after the reload.
	require_True(t, leaf.getJetStream().getMetaGroup() == nil)

	// Check that sys account JS API traffic does NOT cross.
	checkSysJSAPICross(t, h1, leaf, false)

	// Both ends must have merged the denies themselves: the leaf from its
	// runtime check, the hub from the CONNECT advertising the leaf's runtime
	// state.
	checkSysLeafDenied(t, leaf)
	checkSysLeafDenied(t, h1)
}

// A chained middle server (standalone will_extend with its own leafnode
// listener) boots extendable and joins the hub's meta group. A config reload
// then removes the system account remote: the middle server can no longer
// take part in the shared meta group and must stop advertising extendability
// in its leafnode INFO, so a downstream leaf connecting AFTER the reload
// isolates on its own side instead of waiting in observer mode for an
// extension that can not happen.
func TestJetStreamLeafNodeSysRemoteRemovedByReloadNotExtendable(t *testing.T) {
	h1Conf := createConfFile(t, []byte(fmt.Sprintf(lnDomainHubTmpl, "HUB1", t.TempDir(), lnDomainClusterBlock("HUB", 23490, 23491))))
	h1, _ := RunServerWithConfig(h1Conf)
	defer h1.Shutdown()
	h2Conf := createConfFile(t, []byte(fmt.Sprintf(lnDomainHubTmpl, "HUB2", t.TempDir(), lnDomainClusterBlock("HUB", 23491, 23490))))
	h2, _ := RunServerWithConfig(h2Conf)
	defer h2.Shutdown()
	checkClusterFormed(t, h1, h2)
	c := cluster{t: t, servers: []*Server{h1, h2}}
	c.waitOnLeader()

	// Middle: standalone will_extend server with its own leafnode listener,
	// extending the hub's domain.
	sd := t.TempDir()
	hint := fmt.Sprintf("extension_hint: %s", jsWillExtend)
	midConf := createConfFile(t, []byte(fmt.Sprintf(lnDomainLeafHubTmpl, "MID", "hub", sd, hint, lnDomainRemotes(h1.opts.LeafNode.Port, true))))
	mid, _ := RunServerWithConfig(midConf)
	defer mid.Shutdown()
	checkLeafNodeConnectedCount(t, mid, 2)
	c.waitOnPeerCount(3)

	// Reload: remove the system account remote from the middle server.
	reloadUpdateConfig(t, mid, midConf, fmt.Sprintf(lnDomainLeafHubTmpl, "MID", "hub", sd, hint, lnDomainRemotes(h1.opts.LeafNode.Port, false)))
	checkLeafNodeConnectedCount(t, mid, 1)

	// A downstream leaf soliciting the middle server after the reload must
	// isolate on its own side, based on the refreshed INFO.
	bottomConf := createConfFile(t, []byte(fmt.Sprintf(lnDomainLeafTmpl, "LEAF", "hub", t.TempDir(), hint, _EMPTY_, lnDomainRemotes(mid.opts.LeafNode.Port, true))))
	bottom, _ := RunServerWithConfig(bottomConf)
	defer bottom.Shutdown()
	checkLeafNodeConnectedCount(t, bottom, 2)

	checkSysLeafDenied(t, mid)
	checkSysLeafDenied(t, bottom)
	checkSysJSAPICross(t, mid, bottom, false)
}

func TestJetStreamLeafNodeCredsDenies(t *testing.T) {
	tmplL := `
listen: 127.0.0.1:-1
accounts :{
    A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
    SYS:{ users:[ {user:s1,password:s1}]},
}
system_account = SYS
jetstream: {
    domain: "cluster"
    store_dir: '%s'
    max_mem: 50Mb
    max_file: 50Mb
}
leafnodes:{
    remotes:[{url:nats://a1:a1@127.0.0.1:20125, account: A, credentials: '%s' },
		     {url:nats://s1:s1@127.0.0.1:20125, account: SYS, credentials: '%s', deny_imports: foo, deny_exports: bar}]
}
`
	akp, err := nkeys.CreateAccount()
	require_NoError(t, err)
	creds := createUserWithLimit(t, akp, time.Time{}, func(pl *jwt.UserPermissionLimits) {
		pl.Pub.Deny.Add(jsAllAPI)
		pl.Sub.Deny.Add(jsAllAPI)
	})

	sd := t.TempDir()

	confL := createConfFile(t, []byte(fmt.Sprintf(tmplL, sd, creds, creds)))
	opts := LoadConfig(confL)
	sL, err := NewServer(opts)
	require_NoError(t, err)

	l := captureNoticeLogger{}
	sL.SetLogger(&l, false, false)

	go sL.Start()
	defer sL.Shutdown()

	// wait till the notices got printed
UNTIL_READY:
	for {
		<-time.After(50 * time.Millisecond)
		l.Lock()
		for _, n := range l.notices {
			if strings.Contains(n, "Server is ready") {
				l.Unlock()
				break UNTIL_READY
			}
		}
		l.Unlock()
	}

	l.Lock()
	cnt := 0
	for _, n := range l.notices {
		if strings.Contains(n, "LeafNode Remote for Account A uses credentials file") ||
			strings.Contains(n, "LeafNode Remote for System Account uses") ||
			strings.Contains(n, "Remote for System Account uses restricted export permissions") ||
			strings.Contains(n, "Remote for System Account uses restricted import permissions") {
			cnt++
		}
	}
	l.Unlock()
	require_True(t, cnt == 4)
}

func TestJetStreamLeafNodeDefaultDomainCfg(t *testing.T) {
	tmplHub := `
listen: 127.0.0.1:%d
accounts :{
    A:{ jetstream: %s, users:[ {user:a1,password:a1}]},
    SYS:{ users:[ {user:s1,password:s1}]},
}
system_account: SYS
jetstream : %s
server_name: HUB
leafnodes: {
	listen: 127.0.0.1:%d
}
%s
`

	tmplL := `
listen: 127.0.0.1:-1
accounts :{
    A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
    SYS:{ users:[ {user:s1,password:s1}]},
}
system_account: SYS
jetstream: { domain: "%s", store_dir: '%s', max_mem: 100Mb, max_file: 100Mb }
server_name: LEAF
leafnodes: {
    remotes:[{url:nats://a1:a1@127.0.0.1:%d, account: A},%s]
}
%s
`

	test := func(domain string, sysShared bool) {
		confHub := createConfFile(t, []byte(fmt.Sprintf(tmplHub, -1, "disabled", "disabled", -1, "")))
		sHub, _ := RunServerWithConfig(confHub)
		defer sHub.Shutdown()

		noDomainFix := ""
		if domain == _EMPTY_ {
			noDomainFix = `default_js_domain:{A:""}`
		}

		sys := ""
		if sysShared {
			sys = fmt.Sprintf(`{url:nats://s1:s1@127.0.0.1:%d, account: SYS}`, sHub.opts.LeafNode.Port)
		}

		sdLeaf := t.TempDir()
		confL := createConfFile(t, []byte(fmt.Sprintf(tmplL, domain, sdLeaf, sHub.opts.LeafNode.Port, sys, noDomainFix)))
		sLeaf, _ := RunServerWithConfig(confL)
		defer sLeaf.Shutdown()

		lnCnt := 1
		if sysShared {
			lnCnt++
		}

		checkLeafNodeConnectedCount(t, sHub, lnCnt)
		checkLeafNodeConnectedCount(t, sLeaf, lnCnt)

		ncA := natsConnect(t, fmt.Sprintf("nats://a1:a1@127.0.0.1:%d", sHub.opts.Port))
		defer ncA.Close()
		jsA, err := ncA.JetStream()
		require_NoError(t, err)

		_, err = jsA.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 1, Subjects: []string{"foo"}})
		require_True(t, err == nats.ErrNoResponders)

		// Add in default domain and restart server
		require_NoError(t, os.WriteFile(confHub, []byte(fmt.Sprintf(tmplHub,
			sHub.opts.Port,
			"disabled",
			"disabled",
			sHub.opts.LeafNode.Port,
			fmt.Sprintf(`default_js_domain: {A:"%s"}`, domain))), 0664))

		sHub.Shutdown()
		sHub.WaitForShutdown()
		checkLeafNodeConnectedCount(t, sLeaf, 0)
		sHubUpd1, _ := RunServerWithConfig(confHub)
		defer sHubUpd1.Shutdown()

		checkLeafNodeConnectedCount(t, sHubUpd1, lnCnt)
		checkLeafNodeConnectedCount(t, sLeaf, lnCnt)

		_, err = jsA.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 1, Subjects: []string{"foo"}})
		require_NoError(t, err)

		// Enable jetstream in hub.
		sdHub := t.TempDir()
		jsEnabled := fmt.Sprintf(`{ domain: "%s", store_dir: '%s', max_mem: 100Mb, max_file: 100Mb }`, domain, sdHub)
		require_NoError(t, os.WriteFile(confHub, []byte(fmt.Sprintf(tmplHub,
			sHubUpd1.opts.Port,
			"disabled",
			jsEnabled,
			sHubUpd1.opts.LeafNode.Port,
			fmt.Sprintf(`default_js_domain: {A:"%s"}`, domain))), 0664))

		sHubUpd1.Shutdown()
		sHubUpd1.WaitForShutdown()
		checkLeafNodeConnectedCount(t, sLeaf, 0)
		sHubUpd2, _ := RunServerWithConfig(confHub)
		defer sHubUpd2.Shutdown()

		checkLeafNodeConnectedCount(t, sHubUpd2, lnCnt)
		checkLeafNodeConnectedCount(t, sLeaf, lnCnt)

		_, err = jsA.AddStream(&nats.StreamConfig{Name: "bar", Replicas: 1, Subjects: []string{"bar"}})
		require_NoError(t, err)

		// Enable jetstream in account A of hub
		// This is a mis config, as you can't have it both ways, local jetstream but default to another one
		require_NoError(t, os.WriteFile(confHub, []byte(fmt.Sprintf(tmplHub,
			sHubUpd2.opts.Port,
			"enabled",
			jsEnabled,
			sHubUpd2.opts.LeafNode.Port,
			fmt.Sprintf(`default_js_domain: {A:"%s"}`, domain))), 0664))

		if domain != _EMPTY_ {
			// in case no domain name exists there are no additional guard rails, hence no error
			// It is the users responsibility to get this edge case right
			sHubUpd2.Shutdown()
			sHubUpd2.WaitForShutdown()
			checkLeafNodeConnectedCount(t, sLeaf, 0)
			sHubUpd3, err := NewServer(LoadConfig(confHub))
			sHubUpd3.Shutdown()

			require_Error(t, err)
			require_Contains(t, err.Error(), `default_js_domain contains account name "A" with enabled JetStream`)
		}
	}

	t.Run("with-domain-sys", func(t *testing.T) {
		test("domain", true)
	})
	t.Run("with-domain-nosys", func(t *testing.T) {
		test("domain", false)
	})
	t.Run("no-domain", func(t *testing.T) {
		test("", true)
	})
	t.Run("no-domain", func(t *testing.T) {
		test("", false)
	})
}

func TestJetStreamLeafNodeDefaultDomainJwtExplicit(t *testing.T) {
	tmplHub := `
listen: 127.0.0.1:%d
operator: %s
system_account: %s
resolver: MEM
resolver_preload: {
	%s:%s
	%s:%s
}
jetstream : disabled
server_name: HUB
leafnodes: {
	listen: 127.0.0.1:%d
}
%s
`

	tmplL := `
listen: 127.0.0.1:-1
accounts :{
    A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
    SYS:{ users:[ {user:s1,password:s1}]},
}
system_account: SYS
jetstream: { domain: "%s", store_dir: '%s', max_mem: 100Mb, max_file: 100Mb }
server_name: LEAF
leafnodes: {
    remotes:[{url:nats://127.0.0.1:%d, account: A, credentials: '%s'},
		     {url:nats://127.0.0.1:%d, account: SYS, credentials: '%s'}]
}
%s
`

	test := func(domain string) {
		noDomainFix := ""
		if domain == _EMPTY_ {
			noDomainFix = `default_js_domain:{A:""}`
		}

		sysKp, syspub := createKey(t)
		sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
		sysCreds := newUser(t, sysKp)

		aKp, aPub := createKey(t)
		aClaim := jwt.NewAccountClaims(aPub)
		aJwt := encodeClaim(t, aClaim, aPub)
		aCreds := newUser(t, aKp)

		confHub := createConfFile(t, []byte(fmt.Sprintf(tmplHub, -1, ojwt, syspub, syspub, sysJwt, aPub, aJwt, -1, "")))
		sHub, _ := RunServerWithConfig(confHub)
		defer sHub.Shutdown()

		sdLeaf := t.TempDir()
		confL := createConfFile(t, []byte(fmt.Sprintf(tmplL,
			domain,
			sdLeaf,
			sHub.opts.LeafNode.Port,
			aCreds,
			sHub.opts.LeafNode.Port,
			sysCreds,
			noDomainFix)))
		sLeaf, _ := RunServerWithConfig(confL)
		defer sLeaf.Shutdown()

		checkLeafNodeConnectedCount(t, sHub, 2)
		checkLeafNodeConnectedCount(t, sLeaf, 2)

		ncA := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", sHub.opts.Port), createUserCreds(t, nil, aKp))
		defer ncA.Close()
		jsA, err := ncA.JetStream()
		require_NoError(t, err)

		_, err = jsA.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 1, Subjects: []string{"foo"}})
		require_True(t, err == nats.ErrNoResponders)

		// Add in default domain and restart server
		require_NoError(t, os.WriteFile(confHub, []byte(fmt.Sprintf(tmplHub,
			sHub.opts.Port, ojwt, syspub, syspub, sysJwt, aPub, aJwt, sHub.opts.LeafNode.Port,
			fmt.Sprintf(`default_js_domain: {%s:"%s"}`, aPub, domain))), 0664))

		sHub.Shutdown()
		sHub.WaitForShutdown()
		checkLeafNodeConnectedCount(t, sLeaf, 0)
		sHubUpd1, _ := RunServerWithConfig(confHub)
		defer sHubUpd1.Shutdown()

		checkLeafNodeConnectedCount(t, sHubUpd1, 2)
		checkLeafNodeConnectedCount(t, sLeaf, 2)

		_, err = jsA.AddStream(&nats.StreamConfig{Name: "bar", Replicas: 1, Subjects: []string{"bar"}})
		require_NoError(t, err)
	}
	t.Run("with-domain", func(t *testing.T) {
		test("domain")
	})
	t.Run("no-domain", func(t *testing.T) {
		test("")
	})
}

func TestJetStreamLeafNodeDefaultDomainClusterBothEnds(t *testing.T) {
	// test to ensure that default domain functions when both ends of the leaf node connection are clusters
	tmplHub1 := `
listen: 127.0.0.1:-1
accounts :{
    A:{ jetstream: enabled, users:[ {user:a1,password:a1}]},
	B:{ jetstream: enabled, users:[ {user:b1,password:b1}]}
}
jetstream : { domain: "DHUB", store_dir: '%s', max_mem: 100Mb, max_file: 100Mb }
server_name: HUB1
cluster: {
	name: HUB
	listen: 127.0.0.1:20134
	routes=[nats-route://127.0.0.1:20135]
}
leafnodes: {
	listen:127.0.0.1:-1
}
`

	tmplHub2 := `
listen: 127.0.0.1:-1
accounts :{
    A:{ jetstream: enabled, users:[ {user:a1,password:a1}]},
	B:{ jetstream: enabled, users:[ {user:b1,password:b1}]}
}
jetstream : { domain: "DHUB", store_dir: '%s', max_mem: 100Mb, max_file: 100Mb }
server_name: HUB2
cluster: {
	name: HUB
	listen: 127.0.0.1:20135
	routes=[nats-route://127.0.0.1:20134]
}
leafnodes: {
	listen:127.0.0.1:-1
}
`

	tmplL1 := `
listen: 127.0.0.1:-1
accounts :{
    A:{ jetstream: enabled,  users:[ {user:a1,password:a1}]},
	B:{ jetstream: disabled, users:[ {user:b1,password:b1}]}
}
jetstream: { domain: "DLEAF", store_dir: '%s', max_mem: 100Mb, max_file: 100Mb }
server_name: LEAF1
cluster: {
	name: LEAF
	listen: 127.0.0.1:20136
	routes=[nats-route://127.0.0.1:20137]
}
leafnodes: {
    remotes:[{url:nats://a1:a1@127.0.0.1:%d, account: A},{url:nats://b1:b1@127.0.0.1:%d, account: B}]
}
default_js_domain: {B:"DHUB"}
`

	tmplL2 := `
listen: 127.0.0.1:-1
accounts :{
    A:{ jetstream: enabled,  users:[ {user:a1,password:a1}]},
	B:{ jetstream: disabled, users:[ {user:b1,password:b1}]}
}
jetstream: { domain: "DLEAF", store_dir: '%s', max_mem: 100Mb, max_file: 100Mb }
server_name: LEAF2
cluster: {
	name: LEAF
	listen: 127.0.0.1:20137
	routes=[nats-route://127.0.0.1:20136]
}
leafnodes: {
    remotes:[{url:nats://a1:a1@127.0.0.1:%d, account: A},{url:nats://b1:b1@127.0.0.1:%d, account: B}]
}
default_js_domain: {B:"DHUB"}
`

	sd1 := t.TempDir()
	confHub1 := createConfFile(t, []byte(fmt.Sprintf(tmplHub1, sd1)))
	sHub1, _ := RunServerWithConfig(confHub1)
	defer sHub1.Shutdown()

	sd2 := t.TempDir()
	confHub2 := createConfFile(t, []byte(fmt.Sprintf(tmplHub2, sd2)))
	sHub2, _ := RunServerWithConfig(confHub2)
	defer sHub2.Shutdown()

	checkClusterFormed(t, sHub1, sHub2)
	c1 := cluster{t: t, servers: []*Server{sHub1, sHub2}}
	c1.waitOnPeerCount(2)

	sd3 := t.TempDir()
	confLeaf1 := createConfFile(t, []byte(fmt.Sprintf(tmplL1, sd3, sHub1.getOpts().LeafNode.Port, sHub1.getOpts().LeafNode.Port)))
	sLeaf1, _ := RunServerWithConfig(confLeaf1)
	defer sLeaf1.Shutdown()

	sd4 := t.TempDir()
	confLeaf2 := createConfFile(t, []byte(fmt.Sprintf(tmplL2, sd4, sHub1.getOpts().LeafNode.Port, sHub1.getOpts().LeafNode.Port)))
	sLeaf2, _ := RunServerWithConfig(confLeaf2)
	defer sLeaf2.Shutdown()

	checkClusterFormed(t, sLeaf1, sLeaf2)
	c2 := cluster{t: t, servers: []*Server{sLeaf1, sLeaf2}}
	c2.waitOnPeerCount(2)

	checkLeafNodeConnectedCount(t, sHub1, 4)
	checkLeafNodeConnectedCount(t, sLeaf1, 2)
	checkLeafNodeConnectedCount(t, sLeaf2, 2)

	ncB := natsConnect(t, fmt.Sprintf("nats://b1:b1@127.0.0.1:%d", sLeaf1.getOpts().Port))
	defer ncB.Close()
	jsB1, err := ncB.JetStream()
	require_NoError(t, err)
	si, err := jsB1.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 1, Subjects: []string{"foo"}})
	require_NoError(t, err)
	require_Equal(t, si.Cluster.Name, "HUB")

	jsB2, err := ncB.JetStream(nats.Domain("DHUB"))
	require_NoError(t, err)
	si, err = jsB2.AddStream(&nats.StreamConfig{Name: "bar", Replicas: 1, Subjects: []string{"bar"}})
	require_NoError(t, err)
	require_Equal(t, si.Cluster.Name, "HUB")
}

func TestJetStreamLeafNodeSvcImportExportCycle(t *testing.T) {
	accounts := `
	accounts {
		SYS: {
			users: [{user: admin, password: admin}]
		}
		LEAF_USER: {
			users: [{user: leaf_user, password: leaf_user}]
			imports: [
				{service: {account: LEAF_INGRESS, subject: "foo"}}
				{service: {account: LEAF_INGRESS, subject: "_INBOX.>"}}
				{service: {account: LEAF_INGRESS, subject: "$JS.leaf.API.>"}, to: "JS.leaf_ingress@leaf.API.>" }
			]
			jetstream: enabled
		}
		LEAF_INGRESS: {
			users: [{user: leaf_ingress, password: leaf_ingress}]
			exports: [
				{service: "foo", accounts: [LEAF_USER]}
				{service: "_INBOX.>", accounts: [LEAF_USER]}
				{service: "$JS.leaf.API.>", response_type: "stream", accounts: [LEAF_USER]}
			]
			imports: [
			]
			jetstream: enabled
		}
	}
	system_account: SYS
	`

	hconf := createConfFile(t, []byte(fmt.Sprintf(`
	%s
	listen: "127.0.0.1:-1"
	leafnodes {
		listen: "127.0.0.1:-1"
	}
	`, accounts)))
	defer os.Remove(hconf)
	s, o := RunServerWithConfig(hconf)
	defer s.Shutdown()

	lconf := createConfFile(t, []byte(fmt.Sprintf(`
	%s
	server_name: leaf-server
	jetstream {
		store_dir: '%s'
		domain=leaf
	}

	listen: "127.0.0.1:-1"
	leafnodes {
		remotes = [
			{
				urls: ["nats-leaf://leaf_ingress:leaf_ingress@127.0.0.1:%v"]
				account: "LEAF_INGRESS"
			}
		]
	}
	`, accounts, t.TempDir(), o.LeafNode.Port)))
	defer os.Remove(lconf)
	sl, so := RunServerWithConfig(lconf)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, sl)

	nc := natsConnect(t, fmt.Sprintf("nats://leaf_user:leaf_user@127.0.0.1:%v", so.Port))
	defer nc.Close()

	js, _ := nc.JetStream(nats.APIPrefix("JS.leaf_ingress@leaf.API."))

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  nats.FileStorage,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", []byte("msg"))
	require_NoError(t, err)
}

func TestJetStreamLeafNodeJSClusterMigrateRecovery(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: hub, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "hub", _EMPTY_, 3, 12232, true)
	defer c.shutdown()

	tmpl = strings.Replace(jsClusterTemplWithLeafNode, "store_dir:", "domain: leaf, store_dir:", 1)
	lnc := c.createLeafNodesWithTemplateAndStartPort(tmpl, "leaf", 3, 23913)
	defer lnc.shutdown()

	lnc.waitOnClusterReady()
	for _, s := range lnc.servers {
		s.setJetStreamMigrateOnRemoteLeaf()
	}

	nc, _ := jsClientConnect(t, lnc.randomServer())
	defer nc.Close()

	ljs, err := nc.JetStream(nats.Domain("leaf"))
	require_NoError(t, err)

	// Create an asset in the leafnode cluster.
	si, err := ljs.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	require_Equal(t, si.Cluster.Name, "leaf")
	require_NotEqual(t, si.Cluster.Leader, noLeader)
	require_Equal(t, len(si.Cluster.Replicas), 2)

	// Count how many remotes each server in the leafnode cluster is
	// supposed to have and then take them down.
	remotes := map[*Server]int{}
	for _, s := range lnc.servers {
		s.mu.RLock()
		count := len(s.leafRemoteCfgs)
		s.mu.RUnlock()
		remotes[s] += count
		s.closeAndDisableLeafnodes()
		checkLeafNodeConnectedCount(t, s, 0)
	}

	// The Raft nodes in the leafnode cluster now need some time to
	// notice that they're no longer receiving AEs from a leader, as
	// they should have been forced into observer mode. Check that
	// this is the case.
	time.Sleep(maxElectionTimeout)
	for _, s := range lnc.servers {
		s.rnMu.RLock()
		for name, n := range s.raftNodes {
			// We don't expect the metagroup to have turned into an
			// observer but all other assets should have done.
			if name == defaultMetaGroupName {
				require_False(t, n.IsObserver())
			} else {
				require_True(t, n.IsObserver())
			}
		}
		s.rnMu.RUnlock()
	}

	// Bring the leafnode connections back up.
	for _, s := range lnc.servers {
		s.reEnableLeafnodes()
		checkLeafNodeConnectedCount(t, s, remotes[s])
	}

	// Wait for nodes to notice they are no longer in observer mode
	// and to leave observer mode.
	time.Sleep(maxElectionTimeout)
	for _, s := range lnc.servers {
		s.rnMu.RLock()
		for _, n := range s.raftNodes {
			require_False(t, n.IsObserver())
		}
		s.rnMu.RUnlock()
	}

	// Previously nodes would have left observer mode but then would
	// have failed to elect a stream leader as they were stuck on a
	// long election timer. Now this should work reliably.
	lnc.waitOnStreamLeader(globalAccountName, "TEST")
}

func TestJetStreamLeafNodeJSClusterMigrateRecoveryWithDelay(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: hub, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "hub", _EMPTY_, 3, 12232, true)
	defer c.shutdown()

	tmpl = strings.Replace(jsClusterTemplWithLeafNode, "store_dir:", "domain: leaf, store_dir:", 1)
	lnc := c.createLeafNodesWithTemplateAndStartPort(tmpl, "leaf", 3, 23913)
	defer lnc.shutdown()

	lnc.waitOnClusterReady()
	delay := 5 * time.Second
	for _, s := range lnc.servers {
		s.setJetStreamMigrateOnRemoteLeafWithDelay(delay)
	}

	nc, _ := jsClientConnect(t, lnc.randomServer())
	defer nc.Close()

	ljs, err := nc.JetStream(nats.Domain("leaf"))
	require_NoError(t, err)

	// Create an asset in the leafnode cluster.
	si, err := ljs.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	require_Equal(t, si.Cluster.Name, "leaf")
	require_NotEqual(t, si.Cluster.Leader, noLeader)
	require_Equal(t, len(si.Cluster.Replicas), 2)

	// Count how many remotes each server in the leafnode cluster is
	// supposed to have and then take them down.
	remotes := map[*Server]int{}
	for _, s := range lnc.servers {
		s.mu.RLock()
		count := len(s.leafRemoteCfgs)
		s.mu.RUnlock()
		remotes[s] += count
		s.closeAndDisableLeafnodes()
		checkLeafNodeConnectedCount(t, s, 0)
	}

	// The Raft nodes in the leafnode cluster now need some time to
	// notice that they're no longer receiving AEs from a leader, as
	// they should have been forced into observer mode. Check that
	// this is the case.
	// We expect the nodes to become observers after the delay time.
	now := time.Now()
	timeout := maxElectionTimeout + delay
	success := false
	for time.Since(now) <= timeout {
		allObservers := true
		for _, s := range lnc.servers {
			s.rnMu.RLock()
			for name, n := range s.raftNodes {
				if name == defaultMetaGroupName {
					require_False(t, n.IsObserver())
				} else if n.IsObserver() {
					// Make sure the migration delay is respected.
					require_True(t, time.Since(now) > time.Duration(float64(delay)*0.7))
				} else {
					allObservers = false
				}
			}
			s.rnMu.RUnlock()
		}
		if allObservers {
			success = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require_True(t, success)

	// Bring the leafnode connections back up.
	for _, s := range lnc.servers {
		s.reEnableLeafnodes()
		checkLeafNodeConnectedCount(t, s, remotes[s])
	}

	// Wait for nodes to notice they are no longer in observer mode
	// and to leave observer mode.
	time.Sleep(maxElectionTimeout)
	for _, s := range lnc.servers {
		s.rnMu.RLock()
		for _, n := range s.raftNodes {
			require_False(t, n.IsObserver())
		}
		s.rnMu.RUnlock()
	}

	// Make sure all delay timers in remotes are disabled
	for _, s := range lnc.servers {
		for r := range s.leafRemoteCfgs {
			r.RLock()
			ok := r.jsMigrateTimer == nil
			r.RUnlock()
			require_True(t, ok)
		}
	}

	// Previously nodes would have left observer mode but then would
	// have failed to elect a stream leader as they were stuck on a
	// long election timer. Now this should work reliably.
	lnc.waitOnStreamLeader(globalAccountName, "TEST")
}

func TestJetStreamLeafNodeJSClusterMigrateClearObserverOnRemoteRemoval(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: hub, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "hub", _EMPTY_, 3, 12232, true)
	defer c.shutdown()

	tmpl = strings.Replace(jsClusterTemplWithLeafNode, "store_dir:", "domain: leaf, store_dir:", 1)
	lnc := c.createLeafNodesWithTemplateAndStartPort(tmpl, "leaf", 3, 23913)
	defer lnc.shutdown()

	lnc.waitOnClusterReady()
	for _, s := range lnc.servers {
		s.setJetStreamMigrateOnRemoteLeaf()
	}

	nc, _ := jsClientConnect(t, lnc.randomServer())
	defer nc.Close()

	ljs, err := nc.JetStream(nats.Domain("leaf"))
	require_NoError(t, err)

	// Create an asset in the leafnode cluster.
	si, err := ljs.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	require_Equal(t, si.Cluster.Name, "leaf")

	// Take down the leafnode connections of one of the leaf servers so
	// that checkJetStreamMigrate kicks in and moves its assets' raft
	// nodes into observer mode.
	s := lnc.randomServer()
	s.closeAndDisableLeafnodes()
	checkLeafNodeConnectedCount(t, s, 0)

	checkFor(t, maxElectionTimeout, 200*time.Millisecond, func() error {
		s.rnMu.RLock()
		defer s.rnMu.RUnlock()
		for name, n := range s.raftNodes {
			// The metagroup is not expected to become an observer,
			// but all other assets should.
			if name != defaultMetaGroupName && !n.IsObserver() {
				return fmt.Errorf("expected %q to be an observer", name)
			}
		}
		return nil
	})

	// Now remove the leafnode remotes from the configuration of that
	// server and reload. Since we will never reconnect, the observer
	// state should be cleared so this server's assets can become
	// leaders again.
	content, err := os.ReadFile(s.configFile)
	require_NoError(t, err)
	re := regexp.MustCompile(`(?s)remotes \[.*?\n\t\t\]`)
	newContent := re.ReplaceAllString(string(content), "remotes [ ]")
	require_NotEqual(t, string(content), newContent)
	changeCurrentConfigContentWithNewContent(t, s.configFile, []byte(newContent))
	require_NoError(t, s.Reload())

	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		s.rnMu.RLock()
		defer s.rnMu.RUnlock()
		for name, n := range s.raftNodes {
			if n.IsObserver() {
				return fmt.Errorf("expected %q to no longer be an observer", name)
			}
		}
		return nil
	})
}

// This will test that when a mirror or source construct is setup across a leafnode/domain
// that it will recover quickly once the LN is re-established regardless
// of backoff state of the internal consumer create.
func TestJetStreamLeafNodeAndMirrorResyncAfterConnectionDown(t *testing.T) {
	tmplA := `
		listen: -1
		server_name: tcm
		jetstream {
			store_dir: '%s',
			domain: TCM
		}
		accounts {
			JS { users = [ { user: "y", pass: "p" } ]; jetstream: true }
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
		leaf { port: -1 }
    `
	confA := createConfFile(t, []byte(fmt.Sprintf(tmplA, t.TempDir())))
	sA, oA := RunServerWithConfig(confA)
	defer sA.Shutdown()

	// Create a proxy - we will use this to simulate a network down event.
	rtt, bw := 10*time.Microsecond, 10*1024*1024*1024
	proxy := newNetProxy(rtt, bw, bw, fmt.Sprintf("nats://y:p@127.0.0.1:%d", oA.LeafNode.Port))
	defer proxy.stop()

	tmplB := `
		listen: -1
		server_name: xmm
		jetstream {
			store_dir: '%s',
			domain: XMM
		}
		accounts {
			JS { users = [ { user: "y", pass: "p" } ]; jetstream: true }
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
		leaf { remotes [ { url: %s, account: "JS" } ], reconnect: "0.25s" }
    `

	confB := createConfFile(t, []byte(fmt.Sprintf(tmplB, t.TempDir(), proxy.leafURL())))
	sB, _ := RunServerWithConfig(confB)
	defer sA.Shutdown()

	// Make sure we are connected ok.
	checkLeafNodeConnectedCount(t, sA, 1)
	checkLeafNodeConnectedCount(t, sB, 1)

	// We will have 3 streams that we will test for proper syncing after
	// the network is restored.
	//
	//  1. Mirror A --> B
	//  2. Mirror A <-- B
	//  3. Source A <-> B

	// Connect to sA.
	ncA, jsA := jsClientConnect(t, sA, nats.UserInfo("y", "p"))
	defer ncA.Close()

	// Connect to sB.
	ncB, jsB := jsClientConnect(t, sB, nats.UserInfo("y", "p"))
	defer ncB.Close()

	// Add in TEST-A
	_, err := jsA.AddStream(&nats.StreamConfig{Name: "TEST-A", Subjects: []string{"foo"}})
	require_NoError(t, err)

	// Add in TEST-B
	_, err = jsB.AddStream(&nats.StreamConfig{Name: "TEST-B", Subjects: []string{"bar"}})
	require_NoError(t, err)

	// Now setup mirrors.
	_, err = jsB.AddStream(&nats.StreamConfig{
		Name: "M-A",
		Mirror: &nats.StreamSource{
			Name:     "TEST-A",
			External: &nats.ExternalStream{APIPrefix: "$JS.TCM.API"},
		},
	})
	require_NoError(t, err)

	_, err = jsA.AddStream(&nats.StreamConfig{
		Name: "M-B",
		Mirror: &nats.StreamSource{
			Name:     "TEST-B",
			External: &nats.ExternalStream{APIPrefix: "$JS.XMM.API"},
		},
	})
	require_NoError(t, err)

	// Now add in the streams that will source from one another bi-directionally.
	_, err = jsA.AddStream(&nats.StreamConfig{
		Name:     "SRC-A",
		Subjects: []string{"A.*"},
		Sources: []*nats.StreamSource{{
			Name:          "SRC-B",
			FilterSubject: "B.*",
			External:      &nats.ExternalStream{APIPrefix: "$JS.XMM.API"},
		}},
	})
	require_NoError(t, err)

	_, err = jsB.AddStream(&nats.StreamConfig{
		Name:     "SRC-B",
		Subjects: []string{"B.*"},
		Sources: []*nats.StreamSource{{
			Name:          "SRC-A",
			FilterSubject: "A.*",
			External:      &nats.ExternalStream{APIPrefix: "$JS.TCM.API"},
		}},
	})
	require_NoError(t, err)

	// Now load them up with 500 messages.
	initMsgs := 500
	for i := 0; i < initMsgs; i++ {
		// Individual Streams
		jsA.PublishAsync("foo", []byte("PAYLOAD"))
		jsB.PublishAsync("bar", []byte("PAYLOAD"))
		// Bi-directional Sources
		jsA.PublishAsync("A.foo", []byte("PAYLOAD"))
		jsB.PublishAsync("B.bar", []byte("PAYLOAD"))
	}
	select {
	case <-jsA.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
	select {
	case <-jsB.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Utility to check the number of stream msgs.
	checkStreamMsgs := func(js nats.JetStreamContext, sname string, expected int, perr error) error {
		t.Helper()
		if perr != nil {
			return perr
		}
		si, err := js.StreamInfo(sname)
		require_NoError(t, err)
		if si.State.Msgs != uint64(expected) {
			return fmt.Errorf("Expected %d msgs for %s, got state: %+v", expected, sname, si.State)
		}
		return nil
	}

	// Wait til we see all messages.
	checkFor(t, 2*time.Second, 250*time.Millisecond, func() error {
		err := checkStreamMsgs(jsA, "TEST-A", initMsgs, nil)
		err = checkStreamMsgs(jsB, "M-A", initMsgs, err)
		err = checkStreamMsgs(jsB, "TEST-B", initMsgs, err)
		err = checkStreamMsgs(jsA, "M-B", initMsgs, err)
		err = checkStreamMsgs(jsA, "SRC-A", initMsgs*2, err)
		err = checkStreamMsgs(jsB, "SRC-B", initMsgs*2, err)
		return err
	})

	// Take down proxy. This will stop any propagation of messages between TEST and M streams.
	proxy.stop()

	// Now add an additional 500 messages to originals on both sides.
	for i := 0; i < initMsgs; i++ {
		// Individual Streams
		jsA.PublishAsync("foo", []byte("PAYLOAD"))
		jsB.PublishAsync("bar", []byte("PAYLOAD"))
		// Bi-directional Sources
		jsA.PublishAsync("A.foo", []byte("PAYLOAD"))
		jsB.PublishAsync("B.bar", []byte("PAYLOAD"))
	}
	select {
	case <-jsA.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
	select {
	case <-jsB.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	cancelAndDelayConsumer := func(s *Server, stream string) {
		// Now make sure internal consumer is at max backoff.
		acc, err := s.lookupAccount("JS")
		require_NoError(t, err)
		mset, err := acc.lookupStream(stream)
		require_NoError(t, err)

		// Reset sourceInfo to have lots of failures and last attempt 2 minutes ago.
		// Lock should be held on parent stream.
		resetSourceInfo := func(si *sourceInfo) {
			// Do not reset sip here to make sure that the internal logic clears.
			si.fails = 100
			si.lreq = time.Now().Add(-2 * time.Minute)
		}

		// Force the consumer to be canceled and we simulate 100 failed attempts
		// such that the next time we will try will be a long way out.
		mset.mu.Lock()
		if mset.mirror != nil {
			resetSourceInfo(mset.mirror)
			mset.cancelSourceInfo(mset.mirror)
			mset.scheduleSetupMirrorConsumerRetry()
		} else if len(mset.sources) > 0 {
			for iname, si := range mset.sources {
				resetSourceInfo(si)
				mset.cancelSourceInfo(si)
				mset.setupSourceConsumer(iname, si.sseq+1, time.Time{})
			}
		}
		mset.mu.Unlock()
	}

	// Mirrors
	cancelAndDelayConsumer(sA, "M-B")
	cancelAndDelayConsumer(sB, "M-A")
	// Now bi-directional sourcing
	cancelAndDelayConsumer(sA, "SRC-A")
	cancelAndDelayConsumer(sB, "SRC-B")

	// Now restart the network proxy.
	proxy.start()

	// Make sure we are connected ok.
	checkLeafNodeConnectedCount(t, sA, 1)
	checkLeafNodeConnectedCount(t, sB, 1)

	// These should be good before re-sync.
	require_NoError(t, checkStreamMsgs(jsA, "TEST-A", initMsgs*2, nil))
	require_NoError(t, checkStreamMsgs(jsB, "TEST-B", initMsgs*2, nil))

	start := time.Now()
	// Wait til we see all messages.
	checkFor(t, 2*time.Minute, 50*time.Millisecond, func() error {
		err := checkStreamMsgs(jsA, "M-B", initMsgs*2, err)
		err = checkStreamMsgs(jsB, "M-A", initMsgs*2, err)
		err = checkStreamMsgs(jsA, "SRC-A", initMsgs*4, err)
		err = checkStreamMsgs(jsB, "SRC-B", initMsgs*4, err)
		return err
	})
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Expected to resync all streams <3s but got %v", elapsed)
	}
}

// This test will test a 3 node setup where we have a hub node, a gateway node, and a satellite node.
// This is specifically testing re-sync when there is not a direct Domain with JS match for the first
// hop connect LN that is signaling.
//
//		  HUB <---- GW(+JS/DOMAIN) -----> SAT1
//		   ^
//		   |
//	       +------- GW(-JS/NO DOMAIN) --> SAT2
//
// The Gateway node will solicit the satellites but will act as a LN hub.
func TestJetStreamLeafNodeAndMirrorResyncAfterLeafEstablished(t *testing.T) {
	accs := `
		accounts {
			JS { users = [ { user: "u", pass: "p" } ]; jetstream: true }
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
	`
	hubT := `
		listen: -1
		server_name: hub
		jetstream { store_dir: '%s', domain: HUB }
		%s
		leaf { port: -1 }
    `
	confA := createConfFile(t, []byte(fmt.Sprintf(hubT, t.TempDir(), accs)))
	sHub, oHub := RunServerWithConfig(confA)
	defer sHub.Shutdown()

	// We run the SAT node second to extract out info for solicitation from targeted GW.
	sat1T := `
		listen: -1
		server_name: sat1
		jetstream { store_dir: '%s', domain: SAT1 }
		%s
		leaf { port: -1 }
    `
	confB := createConfFile(t, []byte(fmt.Sprintf(sat1T, t.TempDir(), accs)))
	sSat1, oSat1 := RunServerWithConfig(confB)
	defer sSat1.Shutdown()

	sat2T := `
		listen: -1
		server_name: sat2
		jetstream { store_dir: '%s', domain: SAT2 }
		%s
		leaf { port: -1 }
    `
	confC := createConfFile(t, []byte(fmt.Sprintf(sat2T, t.TempDir(), accs)))
	sSat2, oSat2 := RunServerWithConfig(confC)
	defer sSat2.Shutdown()

	hubLeafPort := fmt.Sprintf("nats://u:p@127.0.0.1:%d", oHub.LeafNode.Port)
	sat1LeafPort := fmt.Sprintf("nats://u:p@127.0.0.1:%d", oSat1.LeafNode.Port)
	sat2LeafPort := fmt.Sprintf("nats://u:p@127.0.0.1:%d", oSat2.LeafNode.Port)

	gw1T := `
		listen: -1
		server_name: gw1
		jetstream { store_dir: '%s', domain: GW }
		%s
		leaf { remotes [ { url: %s, account: "JS" }, { url: %s, account: "JS", hub: true } ], reconnect: "0.25s" }
    `
	confD := createConfFile(t, []byte(fmt.Sprintf(gw1T, t.TempDir(), accs, hubLeafPort, sat1LeafPort)))
	sGW1, _ := RunServerWithConfig(confD)
	defer sGW1.Shutdown()

	gw2T := `
		listen: -1
		server_name: gw2
		accounts {
			JS { users = [ { user: "u", pass: "p" } ] }
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
		leaf { remotes [ { url: %s, account: "JS" }, { url: %s, account: "JS", hub: true } ], reconnect: "0.25s" }
    `
	confE := createConfFile(t, []byte(fmt.Sprintf(gw2T, hubLeafPort, sat2LeafPort)))
	sGW2, _ := RunServerWithConfig(confE)
	defer sGW2.Shutdown()

	// Make sure we are connected ok.
	checkLeafNodeConnectedCount(t, sHub, 2)
	checkLeafNodeConnectedCount(t, sSat1, 1)
	checkLeafNodeConnectedCount(t, sSat2, 1)
	checkLeafNodeConnectedCount(t, sGW1, 2)
	checkLeafNodeConnectedCount(t, sGW2, 2)

	// Let's place a muxed stream on the hub and have it source from a stream on the Satellite.
	// Connect to Hub.
	ncHub, jsHub := jsClientConnect(t, sHub, nats.UserInfo("u", "p"))
	defer ncHub.Close()

	_, err := jsHub.AddStream(&nats.StreamConfig{Name: "HUB", Subjects: []string{"H.>"}})
	require_NoError(t, err)

	// Connect to Sat1.
	ncSat1, jsSat1 := jsClientConnect(t, sSat1, nats.UserInfo("u", "p"))
	defer ncSat1.Close()

	_, err = jsSat1.AddStream(&nats.StreamConfig{
		Name:     "SAT-1",
		Subjects: []string{"S1.*"},
		Sources: []*nats.StreamSource{{
			Name:          "HUB",
			FilterSubject: "H.SAT-1.>",
			External:      &nats.ExternalStream{APIPrefix: "$JS.HUB.API"},
		}},
	})
	require_NoError(t, err)

	// Connect to Sat2.
	ncSat2, jsSat2 := jsClientConnect(t, sSat2, nats.UserInfo("u", "p"))
	defer ncSat2.Close()

	_, err = jsSat2.AddStream(&nats.StreamConfig{
		Name:     "SAT-2",
		Subjects: []string{"S2.*"},
		Sources: []*nats.StreamSource{{
			Name:          "HUB",
			FilterSubject: "H.SAT-2.>",
			External:      &nats.ExternalStream{APIPrefix: "$JS.HUB.API"},
		}},
	})
	require_NoError(t, err)

	// Put in 10 msgs each in for each satellite.
	for i := 0; i < 10; i++ {
		jsHub.Publish("H.SAT-1.foo", []byte("CMD"))
		jsHub.Publish("H.SAT-2.foo", []byte("CMD"))
	}
	// Make sure both are sync'd.
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		si, err := jsSat1.StreamInfo("SAT-1")
		require_NoError(t, err)
		if si.State.Msgs != 10 {
			return errors.New("SAT-1 Not sync'd yet")
		}
		si, err = jsSat2.StreamInfo("SAT-2")
		require_NoError(t, err)
		if si.State.Msgs != 10 {
			return errors.New("SAT-2 Not sync'd yet")
		}
		return nil
	})

	testReconnect := func(t *testing.T, delay time.Duration, expected uint64) {
		// Now disconnect Sat1 and Sat2. In 2.12 we can do this with active: false, but since this will be
		// pulled into 2.11.9 just shutdown both gateways.
		sGW1.Shutdown()
		checkLeafNodeConnectedCount(t, sSat1, 0)
		checkLeafNodeConnectedCount(t, sHub, 1)

		sGW2.Shutdown()
		checkLeafNodeConnectedCount(t, sSat2, 0)
		checkLeafNodeConnectedCount(t, sHub, 0)

		// Send 10 more messages for each while GW1 and GW2 are down.
		for i := 0; i < 10; i++ {
			jsHub.Publish("H.SAT-1.foo", []byte("CMD"))
			jsHub.Publish("H.SAT-2.foo", []byte("CMD"))
		}

		// Keep GWs down for delay.
		time.Sleep(delay)

		sGW1, _ = RunServerWithConfig(confD)
		// Make sure we are connected ok.
		checkLeafNodeConnectedCount(t, sHub, 1)
		checkLeafNodeConnectedCount(t, sSat1, 1)
		checkLeafNodeConnectedCount(t, sGW1, 2)

		sGW2, _ = RunServerWithConfig(confE)
		// Make sure we are connected ok.
		checkLeafNodeConnectedCount(t, sHub, 2)
		checkLeafNodeConnectedCount(t, sSat2, 1)
		checkLeafNodeConnectedCount(t, sGW2, 2)

		// Make sure sync'd in less than a second or two.
		checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
			si, err := jsSat1.StreamInfo("SAT-1")
			require_NoError(t, err)
			if si.State.Msgs != expected {
				return fmt.Errorf("SAT-1 not sync'd, expected %d got %d", expected, si.State.Msgs)
			}
			si, err = jsSat2.StreamInfo("SAT-2")
			require_NoError(t, err)
			if si.State.Msgs != expected {
				return fmt.Errorf("SAT-2 not sync'd, expected %d got %d", expected, si.State.Msgs)
			}
			return nil
		})
	}

	// We will test two scenarios with amount of time the GWs (link) is down.
	// 1. Just a second, we will not have detected the consumer is offline as of yet.
	// 2. Just over sourceHealthCheckInterval, meaning we detect it is down and schedule for another try.
	t.Run(fmt.Sprintf("reconnect-%v", time.Second), func(t *testing.T) {
		testReconnect(t, time.Second, 20)
	})
	t.Run(fmt.Sprintf("reconnect-%v", sourceHealthCheckInterval+time.Second), func(t *testing.T) {
		testReconnect(t, sourceHealthCheckInterval+time.Second, 30)
	})
	defer sGW1.Shutdown()
	defer sGW2.Shutdown()
}

func TestJetStreamSourceConsumerLeafReconnectStorm(t *testing.T) {
	hubT := `
		listen: -1
		server_name: hub
		jetstream { store_dir: '%s', domain: HUB }
		leaf { port: -1 }
	`
	hubConf := createConfFile(t, []byte(fmt.Sprintf(hubT, t.TempDir())))
	sHub, oHub := RunServerWithConfig(hubConf)
	defer sHub.Shutdown()
	hubLeafURL := fmt.Sprintf("nats://u:p@127.0.0.1:%d", oHub.LeafNode.Port)

	leafT := `
		listen: -1
		server_name: %s
		jetstream { store_dir: '%s', domain: %s }
		leaf { remotes [ { url: %s } ], reconnect: "0.25s" }
	`

	type leafSpec struct {
		name      string
		domain    string
		srcStream string
		conf      string
		srv       *Server
	}
	leafs := []*leafSpec{
		{name: "leaf1", domain: "L1", srcStream: "S1"},
		{name: "leaf2", domain: "L2", srcStream: "S2"},
		{name: "leaf3", domain: "L3", srcStream: "S3"},
	}
	for _, lf := range leafs {
		lf.conf = createConfFile(t, []byte(fmt.Sprintf(leafT, lf.name, t.TempDir(), lf.domain, hubLeafURL)))
		lf.srv, _ = RunServerWithConfig(lf.conf)
	}
	defer func() {
		for _, lf := range leafs {
			if lf.srv != nil {
				lf.srv.Shutdown()
			}
		}
	}()

	// All three leaves connected to the hub.
	checkLeafNodeConnectedCount(t, sHub, 3)
	for _, lf := range leafs {
		checkLeafNodeConnectedCount(t, lf.srv, 1)
	}

	// Source streams on each leaf.
	for _, lf := range leafs {
		nc, js := jsClientConnect(t, lf.srv, nats.UserInfo("u", "p"))
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     lf.srcStream,
			Subjects: []string{lf.srcStream + ".>"},
		})
		require_NoError(t, err)
		nc.Close()
	}

	// Aggregate stream on the hub sourcing from each leaf via that leaf's
	// external API prefix.
	ncHub, jsHub := jsClientConnect(t, sHub, nats.UserInfo("u", "p"))
	defer ncHub.Close()

	var sources []*nats.StreamSource
	for _, lf := range leafs {
		sources = append(sources, &nats.StreamSource{
			Name:     lf.srcStream,
			External: &nats.ExternalStream{APIPrefix: fmt.Sprintf("$JS.%s.API", lf.domain)},
		})
	}
	_, err := jsHub.AddStream(&nats.StreamConfig{Name: "AGG", Sources: sources})
	require_NoError(t, err)
	mset, err := sHub.globalAccount().lookupStream("AGG")
	require_NoError(t, err)

	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
		if got := len(mset.sources); got != len(leafs) {
			return fmt.Errorf("expected %d sources, have %d", len(leafs), got)
		}
		for _, si := range mset.sources {
			if si.cname == _EMPTY_ || si.sub == nil {
				return fmt.Errorf("source %s not established", si.name)
			}
		}
		return nil
	})

	// Force the unrelated sources into the setup-in-progress state that the
	// buggy shouldRetry latches onto. Correct scoping must ignore them;
	// buggy scoping force-cancels their healthy subs.
	type snap struct {
		cname string
		sub   *subscription
	}
	pre := map[string]snap{}
	mset.mu.Lock()
	for _, si := range mset.sources {
		pre[si.name] = snap{cname: si.cname, sub: si.sub}
		if si.name == "S1" || si.name == "S3" {
			si.sip = true
		}
	}
	mset.mu.Unlock()

	leafs[1].srv.Shutdown()
	checkLeafNodeConnectedCount(t, sHub, 2)
	leafs[1].srv, _ = RunServerWithConfig(leafs[1].conf)
	checkLeafNodeConnectedCount(t, sHub, 3)
	checkLeafNodeConnectedCount(t, leafs[1].srv, 1)

	// processLeafNodeConnect → checkInternalSyncConsumers →
	// retryDisconnectedSyncConsumers runs synchronously on the hub for the
	// new connection; give it time to do (or fail to do) any damage.
	time.Sleep(500 * time.Millisecond)

	// Pointer-equality on *subscription is timing-robust:
	// cancelSourceInfo() nils si.sub immediately, and any fresh setup
	// produces a new *subscription, so disturbance shows up regardless of
	// when we sample within the post-bounce window.
	mset.mu.RLock()
	post := map[string]snap{}
	for _, si := range mset.sources {
		post[si.name] = snap{cname: si.cname, sub: si.sub}
	}
	mset.mu.RUnlock()

	for _, name := range []string{"S1", "S3"} {
		if post[name].sub != pre[name].sub {
			t.Errorf("source %q was recreated by leaf2 reconnect (different domain): "+
				"pre.sub=%p post.sub=%p (cname pre=%q post=%q)",
				name, pre[name].sub, post[name].sub,
				pre[name].cname, post[name].cname)
		}
	}
}

func TestJetStreamSourceConsumerSetupTimerGoroutineLeak(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name: "AGG",
		Sources: []*nats.StreamSource{{
			Name:     "SRC",
			External: &nats.ExternalStream{APIPrefix: "$JS.NONEXISTENT.API"},
		}},
	})
	require_NoError(t, err)

	mset, err := s.globalAccount().lookupStream("AGG")
	require_NoError(t, err)

	// Wait for the source's sourceInfo to be present.
	var iname string
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
		for in := range mset.sources {
			iname = in
			return nil
		}
		return errors.New("no source yet")
	})

	// Let the initial setup quiesce so its inner goroutine is the
	// pre-existing baseline rather than being attributed to the leak.
	time.Sleep(300 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	// Per cycle: call retry, then wait one AfterFunc delay (100ms+rand(100ms))
	// so the body fires and parks its inner goroutine in select.
	const N = 30
	for range N {
		mset.mu.Lock()
		if si := mset.sources[iname]; si != nil {
			si.sip = true
			// bypass the 2s retry throttle in setupSourceConsumer.
			si.lreq = time.Time{}
		}
		mset.mu.Unlock()
		mset.retryDisconnectedSyncConsumers()
		time.Sleep(250 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)

	leaked := runtime.NumGoroutine() - baseline
	const tolerance = 5
	if leaked > tolerance {
		t.Fatalf("AfterFunc-driven retry leaked %d goroutines after %d "+
			"retryDisconnectedSyncConsumers cycles for a single iname "+
			"(baseline=%d). Each cycle's AfterFunc fires a fresh "+
			"trySetupSourceConsumer which spawns a new inner goroutine; "+
			"the previous inner goroutine is orphaned in select, "+
			"allowing linear ramp-up.",
			leaked, N, baseline)
	}
}
