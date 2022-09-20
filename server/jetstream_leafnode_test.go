// Copyright 2020-2022 The NATS Authors
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
// +build !skip_js_tests

package server

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func TestJetStreamLeafNodeUniqueServerNameCrossJSDomain(t *testing.T) {
	name := "NOT-UNIQUE"
	test := func(s *Server, sIdExpected string, srvs ...*Server) {
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
		s.nodeToInfo.Range(func(key, value interface{}) bool {
			cnt++
			require_Equal(t, value.(nodeInfo).name, name)
			require_Equal(t, value.(nodeInfo).id, sIdExpected)
			return true
		})
		require_True(t, cnt == 1)
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
		confA := createConfFile(t, []byte(fmt.Sprintf(tmplA, name, createDir(t, JetStreamStoreDir))))
		defer removeFile(t, confA)
		sA, oA := RunServerWithConfig(confA)
		defer sA.Shutdown()
		// using same domain as sA
		confL := createConfFile(t, []byte(fmt.Sprintf(tmplL, name, createDir(t, JetStreamStoreDir), "hub",
			fmt.Sprintf("nats://y:p@127.0.0.1:%d", oA.LeafNode.Port),
			fmt.Sprintf("nats://admin:s3cr3t!@127.0.0.1:%d", oA.LeafNode.Port))))
		defer removeFile(t, confL)
		sL, _ := RunServerWithConfig(confL)
		defer sL.Shutdown()
		// as server name uniqueness is violates, sL.ID() is the expected value
		test(sA, sL.ID(), sA, sL)
	})
	t.Run("different-domain", func(t *testing.T) {
		confA := createConfFile(t, []byte(fmt.Sprintf(tmplA, name, createDir(t, JetStreamStoreDir))))
		defer removeFile(t, confA)
		sA, oA := RunServerWithConfig(confA)
		defer sA.Shutdown()
		// using different domain as sA
		confL := createConfFile(t, []byte(fmt.Sprintf(tmplL, name, createDir(t, JetStreamStoreDir), "spoke",
			fmt.Sprintf("nats://y:p@127.0.0.1:%d", oA.LeafNode.Port),
			fmt.Sprintf("nats://admin:s3cr3t!@127.0.0.1:%d", oA.LeafNode.Port))))
		defer removeFile(t, confL)
		sL, _ := RunServerWithConfig(confL)
		defer sL.Shutdown()
		checkLeafNodeConnectedCount(t, sL, 2)
		checkLeafNodeConnectedCount(t, sA, 2)
		// ensure sA contains only sA.ID
		test(sA, sA.ID(), sA, sL)
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
	defer removeFile(t, lnCreds)
	unlimitedCreds := createUserWithLimit(t, accKp, noExpiration, nil)
	defer removeFile(t, unlimitedCreds)

	sysCreds := createUserWithLimit(t, sysKp, noExpiration, nil)
	defer removeFile(t, sysCreds)

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
		createDir(t, JetStreamStoreDir))))
	defer removeFile(t, confA)
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()

	confL := createConfFile(t, []byte(fmt.Sprintf(tmplL, createDir(t, JetStreamStoreDir),
		sA.opts.LeafNode.Port, lnCreds, sA.opts.LeafNode.Port, sysCreds)))
	defer removeFile(t, confL)
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
	listen: 127.0.0.1:50554
	routes=[nats-route://127.0.0.1:50555]
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
	listen: 127.0.0.1:50555
	routes=[nats-route://127.0.0.1:50554]
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
	listen: 127.0.0.1:50556
	routes=[nats-route://127.0.0.1:50557]
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
	listen: 127.0.0.1:50557
	routes=[nats-route://127.0.0.1:50556]
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
			sd1 := createDir(t, JetStreamStoreDir)
			defer os.RemoveAll(sd1)
			confA := createConfFile(t, []byte(fmt.Sprintf(tmplA, sd1)))
			defer removeFile(t, confA)
			sA, _ := RunServerWithConfig(confA)
			defer sA.Shutdown()

			sd2 := createDir(t, JetStreamStoreDir)
			defer os.RemoveAll(sd2)
			confB := createConfFile(t, []byte(fmt.Sprintf(tmplB, sd2)))
			defer removeFile(t, confB)
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

			sd3 := createDir(t, JetStreamStoreDir)
			defer os.RemoveAll(sd3)
			// deliberately pick server sA and proxy
			confLA := createConfFile(t, []byte(fmt.Sprintf(tmplLA, sd3, hint, sA.opts.LeafNode.Port, lPort)))
			defer removeFile(t, confLA)
			sLA, _ := RunServerWithConfig(confLA)
			defer sLA.Shutdown()

			sd4 := createDir(t, JetStreamStoreDir)
			defer os.RemoveAll(sd4)
			// deliberately pick server sA and proxy
			confLB := createConfFile(t, []byte(fmt.Sprintf(tmplLB, sd4, hint, sA.opts.LeafNode.Port, lPort)))
			defer removeFile(t, confLB)
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
					require_Contains(t, err.Error(), "insufficient resources")
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
					require_Contains(t, err.Error(), "insufficient resources")
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
	listen: 127.0.0.1:50554
	routes=[nats-route://127.0.0.1:50555,nats-route://127.0.0.1:50556]
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
	listen: 127.0.0.1:50555
	routes=[nats-route://127.0.0.1:50554,nats-route://127.0.0.1:50556]
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
	listen: 127.0.0.1:50556
	routes=[nats-route://127.0.0.1:50554,nats-route://127.0.0.1:50555]
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
			jsDisabledDomainString := _EMPTY_
			jsEnabledDomainString := _EMPTY_
			if withDomain {
				jsEnabledDomainString = `domain: "domain", `
				jsDisabledDomainString = `domain: "domain"`
			} else {
				// in case no domain name is set, fall back to the extension hint.
				// since JS is disabled, the value of this does not clash with other uses.
				jsDisabledDomainString = "extension_hint: will_extend"
			}

			sd1 := createDir(t, JetStreamStoreDir)
			defer os.RemoveAll(sd1)
			confA := createConfFile(t, []byte(fmt.Sprintf(tmplA, jsEnabledDomainString, sd1)))
			defer removeFile(t, confA)
			sA, _ := RunServerWithConfig(confA)
			defer sA.Shutdown()

			sd2 := createDir(t, JetStreamStoreDir)
			defer os.RemoveAll(sd2)
			confB := createConfFile(t, []byte(fmt.Sprintf(tmplB, jsEnabledDomainString, sd2)))
			defer removeFile(t, confB)
			sB, _ := RunServerWithConfig(confB)
			defer sB.Shutdown()

			confC := createConfFile(t, []byte(fmt.Sprintf(tmplC, jsDisabledDomainString)))
			defer removeFile(t, confC)
			sC, _ := RunServerWithConfig(confC)
			defer sC.Shutdown()

			checkClusterFormed(t, sA, sB, sC)
			c := cluster{t: t, servers: []*Server{sA, sB, sC}}
			c.waitOnPeerCount(2)

			sd3 := createDir(t, JetStreamStoreDir)
			defer os.RemoveAll(sd3)
			// deliberately pick server sC (no JS) to connect to
			confLA := createConfFile(t, []byte(fmt.Sprintf(tmplLA, jsEnabledDomainString, sd3, sC.opts.LeafNode.Port, sC.opts.LeafNode.Port)))
			defer removeFile(t, confLA)
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
    remotes:[{url:nats://a1:a1@127.0.0.1:50555, account: A, credentials: '%s' },
		     {url:nats://s1:s1@127.0.0.1:50555, account: SYS, credentials: '%s', deny_imports: foo, deny_exports: bar}]
}
`
	akp, err := nkeys.CreateAccount()
	require_NoError(t, err)
	creds := createUserWithLimit(t, akp, time.Time{}, func(pl *jwt.UserPermissionLimits) {
		pl.Pub.Deny.Add(jsAllAPI)
		pl.Sub.Deny.Add(jsAllAPI)
	})

	sd := createDir(t, JetStreamStoreDir)
	defer os.RemoveAll(sd)

	confL := createConfFile(t, []byte(fmt.Sprintf(tmplL, sd, creds, creds)))
	defer removeFile(t, confL)
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
		defer removeFile(t, confHub)
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

		sdLeaf := createDir(t, JetStreamStoreDir)
		defer os.RemoveAll(sdLeaf)
		confL := createConfFile(t, []byte(fmt.Sprintf(tmplL, domain, sdLeaf, sHub.opts.LeafNode.Port, sys, noDomainFix)))
		defer removeFile(t, confL)
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
		require_NoError(t, ioutil.WriteFile(confHub, []byte(fmt.Sprintf(tmplHub,
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
		sdHub := createDir(t, JetStreamStoreDir)
		defer os.RemoveAll(sdHub)
		jsEnabled := fmt.Sprintf(`{ domain: "%s", store_dir: '%s', max_mem: 100Mb, max_file: 100Mb }`, domain, sdHub)
		require_NoError(t, ioutil.WriteFile(confHub, []byte(fmt.Sprintf(tmplHub,
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
		require_NoError(t, ioutil.WriteFile(confHub, []byte(fmt.Sprintf(tmplHub,
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
		defer removeFile(t, sysCreds)

		aKp, aPub := createKey(t)
		aClaim := jwt.NewAccountClaims(aPub)
		aJwt := encodeClaim(t, aClaim, aPub)
		aCreds := newUser(t, aKp)
		defer removeFile(t, aCreds)

		confHub := createConfFile(t, []byte(fmt.Sprintf(tmplHub, -1, ojwt, syspub, syspub, sysJwt, aPub, aJwt, -1, "")))
		defer removeFile(t, confHub)
		sHub, _ := RunServerWithConfig(confHub)
		defer sHub.Shutdown()

		sdLeaf := createDir(t, JetStreamStoreDir)
		defer os.RemoveAll(sdLeaf)
		confL := createConfFile(t, []byte(fmt.Sprintf(tmplL,
			domain,
			sdLeaf,
			sHub.opts.LeafNode.Port,
			aCreds,
			sHub.opts.LeafNode.Port,
			sysCreds,
			noDomainFix)))
		defer removeFile(t, confL)
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
		require_NoError(t, ioutil.WriteFile(confHub, []byte(fmt.Sprintf(tmplHub,
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
	listen: 127.0.0.1:50554
	routes=[nats-route://127.0.0.1:50555]
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
	listen: 127.0.0.1:50555
	routes=[nats-route://127.0.0.1:50554]
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
	listen: 127.0.0.1:50556
	routes=[nats-route://127.0.0.1:50557]
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
	listen: 127.0.0.1:50557
	routes=[nats-route://127.0.0.1:50556]
}
leafnodes: {
    remotes:[{url:nats://a1:a1@127.0.0.1:%d, account: A},{url:nats://b1:b1@127.0.0.1:%d, account: B}]
}
default_js_domain: {B:"DHUB"}
`

	sd1 := createDir(t, JetStreamStoreDir)
	defer os.RemoveAll(sd1)
	confHub1 := createConfFile(t, []byte(fmt.Sprintf(tmplHub1, sd1)))
	defer removeFile(t, confHub1)
	sHub1, _ := RunServerWithConfig(confHub1)
	defer sHub1.Shutdown()

	sd2 := createDir(t, JetStreamStoreDir)
	defer os.RemoveAll(sd2)
	confHub2 := createConfFile(t, []byte(fmt.Sprintf(tmplHub2, sd2)))
	defer removeFile(t, confHub2)
	sHub2, _ := RunServerWithConfig(confHub2)
	defer sHub2.Shutdown()

	checkClusterFormed(t, sHub1, sHub2)
	c1 := cluster{t: t, servers: []*Server{sHub1, sHub2}}
	c1.waitOnPeerCount(2)

	sd3 := createDir(t, JetStreamStoreDir)
	defer os.RemoveAll(sd3)
	confLeaf1 := createConfFile(t, []byte(fmt.Sprintf(tmplL1, sd3, sHub1.getOpts().LeafNode.Port, sHub1.getOpts().LeafNode.Port)))
	defer removeFile(t, confLeaf1)
	sLeaf1, _ := RunServerWithConfig(confLeaf1)
	defer sLeaf1.Shutdown()

	sd4 := createDir(t, JetStreamStoreDir)
	defer os.RemoveAll(sd4)
	confLeaf2 := createConfFile(t, []byte(fmt.Sprintf(tmplL2, sd3, sHub1.getOpts().LeafNode.Port, sHub1.getOpts().LeafNode.Port)))
	defer removeFile(t, confLeaf2)
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
