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

// Do not exlude this file with the !skip_js_tests since those helpers
// are also used by MQTT.

package server

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Support functions

// Used to setup superclusters for tests.
type supercluster struct {
	t        *testing.T
	clusters []*cluster
}

func (sc *supercluster) shutdown() {
	if sc == nil {
		return
	}
	for _, c := range sc.clusters {
		shutdownCluster(c)
	}
}

func (sc *supercluster) randomServer() *Server {
	return sc.randomCluster().randomServer()
}

func (sc *supercluster) serverByName(sname string) *Server {
	for _, c := range sc.clusters {
		if s := c.serverByName(sname); s != nil {
			return s
		}
	}
	return nil
}

func (sc *supercluster) waitOnStreamLeader(account, stream string) {
	sc.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		for _, c := range sc.clusters {
			if leader := c.streamLeader(account, stream); leader != nil {
				time.Sleep(200 * time.Millisecond)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	sc.t.Fatalf("Expected a stream leader for %q %q, got none", account, stream)
}

var jsClusterAccountsTempl = `
	listen: 127.0.0.1:-1

	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: one

	accounts {
		ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
		TWO { users = [ { user: "two", pass: "p" } ]; jetstream: enabled }
		NOJS { users = [ { user: "nojs", pass: "p" } ] }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsClusterTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterMaxBytesTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: u

	accounts {
		$U {
			users = [ { user: "u", pass: "p" } ]
			jetstream: {
				max_mem:   128MB
				max_file:  18GB
				max_bytes: true // Forces streams to indicate max_bytes.
			}
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsClusterMaxBytesAccountLimitTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 4GB, store_dir: '%s'}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: u

	accounts {
		$U {
			users = [ { user: "u", pass: "p" } ]
			jetstream: {
				max_mem:   128MB
				max_file:  3GB
				max_bytes: true // Forces streams to indicate max_bytes.
			}
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsSuperClusterTempl = `
	%s
	gateway {
		name: %s
		listen: 127.0.0.1:%d
		gateways = [%s
		]
	}

	system_account: "$SYS"
`

var jsClusterLimitsTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2MB, max_file_store: 8MB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: u

	accounts {
		ONE {
			users = [ { user: "u", pass: "s3cr3t!" } ]
			jetstream: enabled
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsMixedModeGlobalAccountTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2MB, max_file_store: 8MB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsGWTempl = `%s{name: %s, urls: [%s]}`

func createJetStreamTaggedSuperCluster(t *testing.T) *supercluster {
	sc := createJetStreamSuperCluster(t, 3, 3)
	sc.waitOnPeerCount(9)

	reset := func(s *Server) {
		s.mu.Lock()
		rch := s.sys.resetCh
		s.mu.Unlock()
		if rch != nil {
			rch <- struct{}{}
		}
		s.sendStatszUpdate()
	}

	// Make first cluster AWS, US country code.
	for _, s := range sc.clusterForName("C1").servers {
		s.optsMu.Lock()
		s.opts.Tags.Add("cloud:aws")
		s.opts.Tags.Add("country:us")
		s.optsMu.Unlock()
		reset(s)
	}
	// Make second cluster GCP, UK country code.
	for _, s := range sc.clusterForName("C2").servers {
		s.optsMu.Lock()
		s.opts.Tags.Add("cloud:gcp")
		s.opts.Tags.Add("country:uk")
		s.optsMu.Unlock()
		reset(s)
	}
	// Make third cluster AZ, JP country code.
	for _, s := range sc.clusterForName("C3").servers {
		s.optsMu.Lock()
		s.opts.Tags.Add("cloud:az")
		s.opts.Tags.Add("country:jp")
		s.optsMu.Unlock()
		reset(s)
	}

	ml := sc.leader()
	js := ml.getJetStream()
	require_True(t, js != nil)
	js.mu.RLock()
	defer js.mu.RUnlock()
	cc := js.cluster
	require_True(t, cc != nil)

	// Walk and make sure all tags are registered.
	expires := time.Now().Add(10 * time.Second)
	for time.Now().Before(expires) {
		allOK := true
		for _, p := range cc.meta.Peers() {
			si, ok := ml.nodeToInfo.Load(p.ID)
			require_True(t, ok)
			ni := si.(nodeInfo)
			if len(ni.tags) == 0 {
				allOK = false
				reset(sc.serverByName(ni.name))
			}
		}
		if allOK {
			break
		}
	}

	return sc
}

func createJetStreamSuperCluster(t *testing.T, numServersPer, numClusters int) *supercluster {
	return createJetStreamSuperClusterWithTemplate(t, jsClusterTempl, numServersPer, numClusters)
}

func createJetStreamSuperClusterWithTemplate(t *testing.T, tmpl string, numServersPer, numClusters int) *supercluster {
	return createJetStreamSuperClusterWithTemplateAndModHook(t, tmpl, numServersPer, numClusters, nil)
}

func createJetStreamSuperClusterWithTemplateAndModHook(t *testing.T, tmpl string, numServersPer, numClusters int, modify modifyCb) *supercluster {
	t.Helper()
	if numServersPer < 1 {
		t.Fatalf("Number of servers must be >= 1")
	}
	if numClusters <= 1 {
		t.Fatalf("Number of clusters must be > 1")
	}

	startClusterPorts := []int{20_022, 22_022, 24_022}
	startGatewayPorts := []int{20_122, 22_122, 24_122}
	startClusterPort := startClusterPorts[rand.Intn(len(startClusterPorts))]
	startGWPort := startGatewayPorts[rand.Intn(len(startGatewayPorts))]

	// Make the GWs form faster for the tests.
	SetGatewaysSolicitDelay(10 * time.Millisecond)
	defer ResetGatewaysSolicitDelay()

	cp, gp := startClusterPort, startGWPort
	var clusters []*cluster

	var gws []string
	// Build GWs first, will be same for all servers.
	for i, port := 1, gp; i <= numClusters; i++ {
		cn := fmt.Sprintf("C%d", i)
		var urls []string
		for n := 0; n < numServersPer; n++ {
			urls = append(urls, fmt.Sprintf("nats-route://127.0.0.1:%d", port))
			port++
		}
		gws = append(gws, fmt.Sprintf(jsGWTempl, "\n\t\t\t", cn, strings.Join(urls, ",")))
	}
	gwconf := strings.Join(gws, "")

	for i := 1; i <= numClusters; i++ {
		cn := fmt.Sprintf("C%d", i)
		// Go ahead and build configurations.
		c := &cluster{servers: make([]*Server, 0, numServersPer), opts: make([]*Options, 0, numServersPer), name: cn}

		// Build out the routes that will be shared with all configs.
		var routes []string
		for port := cp; port < cp+numServersPer; port++ {
			routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", port))
		}
		routeConfig := strings.Join(routes, ",")

		for si := 0; si < numServersPer; si++ {
			storeDir := createDir(t, JetStreamStoreDir)
			sn := fmt.Sprintf("%s-S%d", cn, si+1)
			bconf := fmt.Sprintf(tmpl, sn, storeDir, cn, cp+si, routeConfig)
			conf := fmt.Sprintf(jsSuperClusterTempl, bconf, cn, gp, gwconf)
			gp++
			if modify != nil {
				conf = modify(sn, cn, storeDir, conf)
			}
			s, o := RunServerWithConfig(createConfFile(t, []byte(conf)))
			c.servers = append(c.servers, s)
			c.opts = append(c.opts, o)
		}
		checkClusterFormed(t, c.servers...)
		clusters = append(clusters, c)
		cp += numServersPer
		c.t = t
	}

	// Wait for the supercluster to be formed.
	egws := numClusters - 1
	for _, c := range clusters {
		for _, s := range c.servers {
			waitForOutboundGateways(t, s, egws, 2*time.Second)
		}
	}

	sc := &supercluster{t, clusters}
	sc.waitOnLeader()
	sc.waitOnAllCurrent()

	// Wait for all the peer nodes to be registered.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		var peers []string
		if ml := sc.leader(); ml != nil {
			peers = ml.ActivePeers()
			if len(peers) == numClusters*numServersPer {
				return nil
			}
		}
		return fmt.Errorf("Not correct number of peers, expected %d, got %d", numClusters*numServersPer, len(peers))
	})

	if sc.leader() == nil {
		sc.t.Fatalf("Expected a cluster leader, got none")
	}

	return sc
}

func (sc *supercluster) createLeafNodes(clusterName string, numServers int) *cluster {
	// Create our leafnode cluster template first.
	return sc.createLeafNodesWithDomain(clusterName, numServers, "")
}

func (sc *supercluster) createLeafNodesWithDomain(clusterName string, numServers int, domain string) *cluster {
	// Create our leafnode cluster template first.
	return sc.randomCluster().createLeafNodes(clusterName, numServers, domain)
}

func (sc *supercluster) createSingleLeafNode(extend bool) *Server {
	return sc.randomCluster().createLeafNode(extend)
}

func (sc *supercluster) leader() *Server {
	for _, c := range sc.clusters {
		if leader := c.leader(); leader != nil {
			return leader
		}
	}
	return nil
}

func (sc *supercluster) waitOnLeader() {
	sc.t.Helper()
	expires := time.Now().Add(5 * time.Second)
	for time.Now().Before(expires) {
		for _, c := range sc.clusters {
			if leader := c.leader(); leader != nil {
				time.Sleep(250 * time.Millisecond)
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	sc.t.Fatalf("Expected a cluster leader, got none")
}

func (sc *supercluster) waitOnAllCurrent() {
	for _, c := range sc.clusters {
		c.waitOnAllCurrent()
	}
}

func (sc *supercluster) clusterForName(name string) *cluster {
	for _, c := range sc.clusters {
		if c.name == name {
			return c
		}
	}
	return nil
}

func (sc *supercluster) randomCluster() *cluster {
	clusters := append(sc.clusters[:0:0], sc.clusters...)
	rand.Shuffle(len(clusters), func(i, j int) { clusters[i], clusters[j] = clusters[j], clusters[i] })
	return clusters[0]
}

func (sc *supercluster) waitOnPeerCount(n int) {
	sc.t.Helper()
	sc.waitOnLeader()
	leader := sc.leader()
	expires := time.Now().Add(20 * time.Second)
	for time.Now().Before(expires) {
		peers := leader.JetStreamClusterPeers()
		if len(peers) == n {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	sc.t.Fatalf("Expected a super cluster peer count of %d, got %d", n, len(leader.JetStreamClusterPeers()))
}

var jsClusterMirrorSourceImportsTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: dlc

	accounts {
		JS {
			jetstream: enabled
			users = [ { user: "rip", pass: "pass" } ]
			exports [
				{ service: "$JS.API.CONSUMER.>" } # To create internal consumers to mirror/source.
				{ stream: "RI.DELIVER.SYNC.>" }   # For the mirror/source consumers sending to IA via delivery subject.
				{ service: "$JS.FC.>" }
			]
		}
		IA {
			jetstream: enabled
			users = [ { user: "dlc", pass: "pass" } ]
			imports [
				{ service: { account: JS, subject: "$JS.API.CONSUMER.>"}, to: "RI.JS.API.CONSUMER.>" }
				{ stream: { account: JS, subject: "RI.DELIVER.SYNC.>"} }
				{ service: {account: JS, subject: "$JS.FC.>" }}
			]
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsClusterImportsTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: dlc

	accounts {
		JS {
			jetstream: enabled
			users = [ { user: "rip", pass: "pass" } ]
			exports [
				{ service: "$JS.API.>", response: stream }
				{ service: "TEST" } # For publishing to the stream.
				{ service: "$JS.ACK.TEST.*.>" }
			]
		}
		IA {
			users = [ { user: "dlc", pass: "pass" } ]
			imports [
				{ service: { subject: "$JS.API.>", account: JS }}
				{ service: { subject: "TEST", account: JS }}
				{ service: { subject: "$JS.ACK.TEST.*.>", account: JS }}
			]
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

func createMixedModeCluster(t *testing.T, tmpl string, clusterName, snPre string, numJsServers, numNonServers int, doJSConfig bool) *cluster {
	t.Helper()

	if clusterName == _EMPTY_ || numJsServers < 0 || numNonServers < 1 {
		t.Fatalf("Bad params")
	}

	numServers := numJsServers + numNonServers
	const startClusterPort = 23232

	// Build out the routes that will be shared with all configs.
	var routes []string
	for cp := startClusterPort; cp < startClusterPort+numServers; cp++ {
		routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", cp))
	}
	routeConfig := strings.Join(routes, ",")

	// Go ahead and build configurations and start servers.
	c := &cluster{servers: make([]*Server, 0, numServers), opts: make([]*Options, 0, numServers), name: clusterName}

	for cp := startClusterPort; cp < startClusterPort+numServers; cp++ {
		storeDir := createDir(t, JetStreamStoreDir)

		sn := fmt.Sprintf("%sS-%d", snPre, cp-startClusterPort+1)
		conf := fmt.Sprintf(tmpl, sn, storeDir, clusterName, cp, routeConfig)

		// Disable JS here.
		if cp-startClusterPort >= numJsServers {
			// We can disable by commmenting it out, meaning no JS config, or can set the config up and just set disabled.
			// e.g. jetstream: {domain: "SPOKE", enabled: false}
			if doJSConfig {
				conf = strings.Replace(conf, "jetstream: {", "jetstream: { enabled: false, ", 1)
			} else {
				conf = strings.Replace(conf, "jetstream: ", "# jetstream: ", 1)
			}
		}

		s, o := RunServerWithConfig(createConfFile(t, []byte(conf)))
		c.servers = append(c.servers, s)
		c.opts = append(c.opts, o)
	}
	c.t = t

	// Wait til we are formed and have a leader.
	c.checkClusterFormed()
	if numJsServers > 0 {
		c.waitOnPeerCount(numJsServers)
	}

	return c
}

// This will create a cluster that is explicitly configured for the routes, etc.
// and also has a defined clustername. All configs for routes and cluster name will be the same.
func createJetStreamClusterExplicit(t *testing.T, clusterName string, numServers int) *cluster {
	return createJetStreamClusterWithTemplate(t, jsClusterTempl, clusterName, numServers)
}

func createJetStreamClusterWithTemplate(t *testing.T, tmpl string, clusterName string, numServers int) *cluster {
	return createJetStreamClusterWithTemplateAndModHook(t, tmpl, clusterName, numServers, nil)
}

func createJetStreamClusterWithTemplateAndModHook(t *testing.T, tmpl string, clusterName string, numServers int, modify modifyCb) *cluster {
	startPorts := []int{7_022, 9_022, 11_022, 15_022}
	port := startPorts[rand.Intn(len(startPorts))]
	return createJetStreamClusterAndModHook(t, tmpl, clusterName, _EMPTY_, numServers, port, true, modify)
}

func createJetStreamCluster(t *testing.T, tmpl string, clusterName, snPre string, numServers int, portStart int, waitOnReady bool) *cluster {
	return createJetStreamClusterAndModHook(t, tmpl, clusterName, snPre, numServers, portStart, waitOnReady, nil)
}

type modifyCb func(serverName, clusterName, storeDir, conf string) string

func createJetStreamClusterAndModHook(t *testing.T, tmpl string, clusterName, snPre string, numServers int, portStart int, waitOnReady bool, modify modifyCb) *cluster {
	t.Helper()
	if clusterName == _EMPTY_ || numServers < 1 {
		t.Fatalf("Bad params")
	}

	// Flaky test prevention:
	// Binding a socket to IP stack port 0 will bind an ephemeral port from an OS-specific range.
	// If someone passes in to us a port spec which would cover that range, the test would be flaky.
	// Adjust these ports to be the most inclusive across the port runner OSes.
	// Linux: /proc/sys/net/ipv4/ip_local_port_range : 32768:60999
	// <https://dataplane.org/ephemeralports.html> is useful, and shows there's no safe available range without OS-specific tuning.
	// Our tests are usually run on Linux.  Folks who care about other OSes: if you can't tune your test-runner OS to match, please
	// propose a viable alternative.
	const prohibitedPortFirst = 32768
	const prohibitedPortLast = 60999
	if (portStart >= prohibitedPortFirst && portStart <= prohibitedPortLast) ||
		(portStart+numServers-1 >= prohibitedPortFirst && portStart+numServers-1 <= prohibitedPortLast) {
		t.Fatalf("test setup failure: may not specify a cluster port range which falls within %d:%d", prohibitedPortFirst, prohibitedPortLast)
	}

	// Build out the routes that will be shared with all configs.
	var routes []string
	for cp := portStart; cp < portStart+numServers; cp++ {
		routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", cp))
	}
	routeConfig := strings.Join(routes, ",")

	// Go ahead and build configurations and start servers.
	c := &cluster{servers: make([]*Server, 0, numServers), opts: make([]*Options, 0, numServers), name: clusterName}

	for cp := portStart; cp < portStart+numServers; cp++ {
		storeDir := createDir(t, JetStreamStoreDir)
		sn := fmt.Sprintf("%sS-%d", snPre, cp-portStart+1)
		conf := fmt.Sprintf(tmpl, sn, storeDir, clusterName, cp, routeConfig)
		if modify != nil {
			conf = modify(sn, clusterName, storeDir, conf)
		}
		s, o := RunServerWithConfig(createConfFile(t, []byte(conf)))
		c.servers = append(c.servers, s)
		c.opts = append(c.opts, o)
	}
	c.t = t

	// Wait til we are formed and have a leader.
	c.checkClusterFormed()
	if waitOnReady {
		c.waitOnClusterReady()
	}

	return c
}

func (c *cluster) addInNewServer() *Server {
	c.t.Helper()
	sn := fmt.Sprintf("S-%d", len(c.servers)+1)
	storeDir, _ := ioutil.TempDir(tempRoot, JetStreamStoreDir)
	seedRoute := fmt.Sprintf("nats-route://127.0.0.1:%d", c.opts[0].Cluster.Port)
	conf := fmt.Sprintf(jsClusterTempl, sn, storeDir, c.name, -1, seedRoute)
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)
	c.checkClusterFormed()
	return s
}

// This is tied to jsClusterAccountsTempl, so changes there to users needs to be reflected here.
func (c *cluster) createSingleLeafNodeNoSystemAccount() *Server {
	as := c.randomServer()
	lno := as.getOpts().LeafNode
	ln1 := fmt.Sprintf("nats://one:p@%s:%d", lno.Host, lno.Port)
	ln2 := fmt.Sprintf("nats://two:p@%s:%d", lno.Host, lno.Port)
	conf := fmt.Sprintf(jsClusterSingleLeafNodeTempl, createDir(c.t, JetStreamStoreDir), ln1, ln2)
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)

	checkLeafNodeConnectedCount(c.t, as, 2)

	return s
}

// This is tied to jsClusterAccountsTempl, so changes there to users needs to be reflected here.
func (c *cluster) createSingleLeafNodeNoSystemAccountAndEnablesJetStream() *Server {
	return c.createSingleLeafNodeNoSystemAccountAndEnablesJetStreamWithDomain(_EMPTY_, "nojs")
}

func (c *cluster) createSingleLeafNodeNoSystemAccountAndEnablesJetStreamWithDomain(domain, user string) *Server {
	tmpl := jsClusterSingleLeafNodeLikeNGSTempl
	if domain != _EMPTY_ {
		nsc := fmt.Sprintf("domain: %s, store_dir:", domain)
		tmpl = strings.Replace(jsClusterSingleLeafNodeLikeNGSTempl, "store_dir:", nsc, 1)
	}
	as := c.randomServer()
	lno := as.getOpts().LeafNode
	ln := fmt.Sprintf("nats://%s:p@%s:%d", user, lno.Host, lno.Port)
	conf := fmt.Sprintf(tmpl, createDir(c.t, JetStreamStoreDir), ln)
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)

	checkLeafNodeConnectedCount(c.t, as, 1)

	return s
}

var jsClusterSingleLeafNodeLikeNGSTempl = `
	listen: 127.0.0.1:-1
	server_name: LNJS
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	leaf { remotes [ { urls: [ %s ] } ] }
`

var jsClusterSingleLeafNodeTempl = `
	listen: 127.0.0.1:-1
	server_name: LNJS
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	leaf { remotes [
		{ urls: [ %s ], account: "JSY" }
		{ urls: [ %s ], account: "JSN" } ]
	}

	accounts {
		JSY { users = [ { user: "y", pass: "p" } ]; jetstream: true }
		JSN { users = [ { user: "n", pass: "p" } ] }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsClusterTemplWithLeafNode = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	{{leaf}}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterTemplWithLeafNodeNoJS = `
	listen: 127.0.0.1:-1
	server_name: %s

	# Need to keep below since it fills in the store dir by default so just comment out.
	# jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	{{leaf}}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterTemplWithSingleLeafNode = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	{{leaf}}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterTemplWithSingleLeafNodeNoJS = `
	listen: 127.0.0.1:-1
	server_name: %s

	# jetstream: {store_dir: '%s'}

	{{leaf}}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsLeafFrag = `
	leaf {
		remotes [
			{ urls: [ %s ] }
			{ urls: [ %s ], account: "$SYS" }
		]
	}
`

func (c *cluster) createLeafNodes(clusterName string, numServers int, domain string) *cluster {
	return c.createLeafNodesWithStartPortAndDomain(clusterName, numServers, 22111, domain)
}

func (c *cluster) createLeafNodesNoJS(clusterName string, numServers int) *cluster {
	return c.createLeafNodesWithTemplateAndStartPort(jsClusterTemplWithLeafNodeNoJS, clusterName, numServers, 21333)
}

func (c *cluster) createLeafNodesWithStartPortAndDomain(clusterName string, numServers int, portStart int, domain string) *cluster {
	if domain == _EMPTY_ {
		return c.createLeafNodesWithTemplateAndStartPort(jsClusterTemplWithLeafNode, clusterName, numServers, portStart)
	}
	tmpl := strings.Replace(jsClusterTemplWithLeafNode, "store_dir:", fmt.Sprintf(`domain: "%s", store_dir:`, domain), 1)
	return c.createLeafNodesWithTemplateAndStartPort(tmpl, clusterName, numServers, portStart)
}

func (c *cluster) createLeafNode(extend bool) *Server {
	if extend {
		return c.createLeafNodeWithTemplate("LNS",
			strings.ReplaceAll(jsClusterTemplWithSingleLeafNode, "store_dir:", " extension_hint: will_extend, store_dir:"))
	} else {
		return c.createLeafNodeWithTemplate("LNS", jsClusterTemplWithSingleLeafNode)
	}
}

func (c *cluster) createLeafNodeWithTemplate(name, template string) *Server {
	tmpl := c.createLeafSolicit(template)
	conf := fmt.Sprintf(tmpl, name, createDir(c.t, JetStreamStoreDir))
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)
	return s
}

// Helper to generate the leaf solicit configs.
func (c *cluster) createLeafSolicit(tmpl string) string {
	// Create our leafnode cluster template first.
	var lns, lnss []string
	for _, s := range c.servers {
		if s.ClusterName() != c.name {
			continue
		}
		ln := s.getOpts().LeafNode
		lns = append(lns, fmt.Sprintf("nats://%s:%d", ln.Host, ln.Port))
		lnss = append(lnss, fmt.Sprintf("nats://admin:s3cr3t!@%s:%d", ln.Host, ln.Port))
	}
	lnc := strings.Join(lns, ", ")
	lnsc := strings.Join(lnss, ", ")
	lconf := fmt.Sprintf(jsLeafFrag, lnc, lnsc)
	return strings.Replace(tmpl, "{{leaf}}", lconf, 1)
}

func (c *cluster) createLeafNodesWithTemplateMixedMode(template, clusterName string, numJsServers, numNonServers int, doJSConfig bool) *cluster {
	// Create our leafnode cluster template first.
	tmpl := c.createLeafSolicit(template)
	pre := clusterName + "-"

	lc := createMixedModeCluster(c.t, tmpl, clusterName, pre, numJsServers, numNonServers, doJSConfig)
	for _, s := range lc.servers {
		checkLeafNodeConnectedCount(c.t, s, 2)
	}
	lc.waitOnClusterReadyWithNumPeers(numJsServers)

	return lc
}

func (c *cluster) createLeafNodesWithTemplateAndStartPort(template, clusterName string, numServers int, portStart int) *cluster {
	// Create our leafnode cluster template first.
	tmpl := c.createLeafSolicit(template)
	pre := clusterName + "-"
	lc := createJetStreamCluster(c.t, tmpl, clusterName, pre, numServers, portStart, false)
	for _, s := range lc.servers {
		checkLeafNodeConnectedCount(c.t, s, 2)
	}
	return lc
}

// Will add in the mapping for the account to each server.
func (c *cluster) addSubjectMapping(account, src, dest string) {
	for _, s := range c.servers {
		if s.ClusterName() != c.name {
			continue
		}
		acc, err := s.LookupAccount(account)
		if err != nil {
			c.t.Fatalf("Unexpected error on %v: %v", s, err)
		}
		if err := acc.AddMapping(src, dest); err != nil {
			c.t.Fatalf("Error adding mapping: %v", err)
		}
	}
	// Make sure interest propagates.
	time.Sleep(200 * time.Millisecond)
}

// Adjust limits for the given account.
func (c *cluster) updateLimits(account string, newLimits map[string]JetStreamAccountLimits) {
	c.t.Helper()
	for _, s := range c.servers {
		acc, err := s.LookupAccount(account)
		if err != nil {
			c.t.Fatalf("Unexpected error: %v", err)
		}
		if err := acc.UpdateJetStreamLimits(newLimits); err != nil {
			c.t.Fatalf("Unexpected error: %v", err)
		}
	}
}

// Hack for staticcheck
var skip = func(t *testing.T) {
	t.SkipNow()
}

func jsClientConnect(t *testing.T, s *Server, opts ...nats.Option) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func jsClientConnectEx(t *testing.T, s *Server, domain string, opts ...nats.Option) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	js, err := nc.JetStream(nats.MaxWait(10*time.Second), nats.Domain(domain))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func checkSubsPending(t *testing.T, sub *nats.Subscription, numExpected int) {
	t.Helper()
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if nmsgs, _, err := sub.Pending(); err != nil || nmsgs != numExpected {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
		}
		return nil
	})
}

func fetchMsgs(t *testing.T, sub *nats.Subscription, numExpected int, totalWait time.Duration) []*nats.Msg {
	t.Helper()
	result := make([]*nats.Msg, 0, numExpected)
	for start, count, wait := time.Now(), numExpected, totalWait; len(result) != numExpected; {
		msgs, err := sub.Fetch(count, nats.MaxWait(wait))
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, msgs...)
		count -= len(msgs)
		if wait = totalWait - time.Since(start); wait < 0 {
			break
		}
	}
	if len(result) != numExpected {
		t.Fatalf("Unexpected msg count, got %d, want %d", len(result), numExpected)
	}
	return result
}

func (c *cluster) restartServer(rs *Server) *Server {
	c.t.Helper()
	index := -1
	var opts *Options
	for i, s := range c.servers {
		if s == rs {
			index = i
			break
		}
	}
	if index < 0 {
		c.t.Fatalf("Could not find server %v to restart", rs)
	}
	opts = c.opts[index]
	s, o := RunServerWithConfig(opts.ConfigFile)
	c.servers[index] = s
	c.opts[index] = o
	return s
}

func (c *cluster) checkClusterFormed() {
	c.t.Helper()
	checkClusterFormed(c.t, c.servers...)
}

func (c *cluster) waitOnPeerCount(n int) {
	c.t.Helper()
	c.waitOnLeader()
	leader := c.leader()
	for leader == nil {
		c.waitOnLeader()
		leader = c.leader()
	}
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		if peers := leader.JetStreamClusterPeers(); len(peers) == n {
			return
		}
		time.Sleep(100 * time.Millisecond)
		leader = c.leader()
		for leader == nil {
			c.waitOnLeader()
			leader = c.leader()
		}
	}
	c.t.Fatalf("Expected a cluster peer count of %d, got %d", n, len(leader.JetStreamClusterPeers()))
}

func (c *cluster) waitOnConsumerLeader(account, stream, consumer string) {
	c.t.Helper()
	expires := time.Now().Add(20 * time.Second)
	for time.Now().Before(expires) {
		if leader := c.consumerLeader(account, stream, consumer); leader != nil {
			time.Sleep(200 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("Expected a consumer leader for %q %q %q, got none", account, stream, consumer)
}

func (c *cluster) consumerLeader(account, stream, consumer string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if s.JetStreamIsConsumerLeader(account, stream, consumer) {
			return s
		}
	}
	return nil
}

func (c *cluster) randomNonConsumerLeader(account, stream, consumer string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if !s.JetStreamIsConsumerLeader(account, stream, consumer) {
			return s
		}
	}
	return nil
}

func (c *cluster) waitOnStreamLeader(account, stream string) {
	c.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		if leader := c.streamLeader(account, stream); leader != nil {
			time.Sleep(200 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("Expected a stream leader for %q %q, got none", account, stream)
}

func (c *cluster) randomNonStreamLeader(account, stream string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if s.JetStreamIsStreamAssigned(account, stream) && !s.JetStreamIsStreamLeader(account, stream) {
			return s
		}
	}
	return nil
}

func (c *cluster) randomStreamNotAssigned(account, stream string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if !s.JetStreamIsStreamAssigned(account, stream) {
			return s
		}
	}
	return nil
}

func (c *cluster) streamLeader(account, stream string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if s.JetStreamIsStreamLeader(account, stream) {
			return s
		}
	}
	return nil
}

func (c *cluster) waitOnStreamCurrent(s *Server, account, stream string) {
	c.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		if s.JetStreamIsStreamCurrent(account, stream) {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("Expected server %q to eventually be current for stream %q", s, stream)
}

func (c *cluster) waitOnServerHealthz(s *Server) {
	c.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		hs := s.healthz()
		if hs.Status == "ok" && hs.Error == _EMPTY_ {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("Expected server %q to eventually return healthz 'ok', but got %q", s, s.healthz().Error)
}

func (c *cluster) waitOnServerCurrent(s *Server) {
	c.t.Helper()
	expires := time.Now().Add(20 * time.Second)
	for time.Now().Before(expires) {
		time.Sleep(100 * time.Millisecond)
		if !s.JetStreamEnabled() || s.JetStreamIsCurrent() {
			return
		}
	}
	c.t.Fatalf("Expected server %q to eventually be current", s)
}

func (c *cluster) waitOnAllCurrent() {
	for _, cs := range c.servers {
		c.waitOnServerCurrent(cs)
	}
}

func (c *cluster) serverByName(sname string) *Server {
	for _, s := range c.servers {
		if s.Name() == sname {
			return s
		}
	}
	return nil
}

func (c *cluster) randomNonLeader() *Server {
	// range should randomize.. but..
	for _, s := range c.servers {
		if s.Running() && !s.JetStreamIsLeader() {
			return s
		}
	}
	return nil
}

func (c *cluster) leader() *Server {
	for _, s := range c.servers {
		if s.JetStreamIsLeader() {
			return s
		}
	}
	return nil
}

func (c *cluster) expectNoLeader() {
	c.t.Helper()
	expires := time.Now().Add(maxElectionTimeout)
	for time.Now().Before(expires) {
		if c.leader() == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("Expected no leader but have one")
}

func (c *cluster) waitOnLeader() {
	c.t.Helper()
	expires := time.Now().Add(40 * time.Second)
	for time.Now().Before(expires) {
		if leader := c.leader(); leader != nil {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	c.t.Fatalf("Expected a cluster leader, got none")
}

// Helper function to check that a cluster is formed
func (c *cluster) waitOnClusterReady() {
	c.t.Helper()
	c.waitOnClusterReadyWithNumPeers(len(c.servers))
}

func (c *cluster) waitOnClusterReadyWithNumPeers(numPeersExpected int) {
	c.t.Helper()
	var leader *Server
	expires := time.Now().Add(40 * time.Second)
	for time.Now().Before(expires) {
		if leader = c.leader(); leader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Now make sure we have all peers.
	for leader != nil && time.Now().Before(expires) {
		if len(leader.JetStreamClusterPeers()) == numPeersExpected {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	if leader == nil {
		c.t.Fatalf("Failed to elect a meta-leader")
	}

	peersSeen := len(leader.JetStreamClusterPeers())
	c.shutdown()
	if leader == nil {
		c.t.Fatalf("Expected a cluster leader and fully formed cluster, no leader")
	} else {
		c.t.Fatalf("Expected a fully formed cluster, only %d of %d peers seen", peersSeen, numPeersExpected)
	}
}

// Helper function to remove JetStream from a server.
func (c *cluster) removeJetStream(s *Server) {
	c.t.Helper()
	index := -1
	for i, cs := range c.servers {
		if cs == s {
			index = i
			break
		}
	}
	cf := c.opts[index].ConfigFile
	cb, _ := ioutil.ReadFile(cf)
	var sb strings.Builder
	for _, l := range strings.Split(string(cb), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "jetstream") {
			sb.WriteString(l + "\n")
		}
	}
	if err := ioutil.WriteFile(cf, []byte(sb.String()), 0644); err != nil {
		c.t.Fatalf("Error writing updated config file: %v", err)
	}
	if err := s.Reload(); err != nil {
		c.t.Fatalf("Error on server reload: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
}

func (c *cluster) stopAll() {
	c.t.Helper()
	for _, s := range c.servers {
		s.Shutdown()
	}
}

func (c *cluster) restartAll() {
	c.t.Helper()
	for i, s := range c.servers {
		if !s.Running() {
			opts := c.opts[i]
			s, o := RunServerWithConfig(opts.ConfigFile)
			c.servers[i] = s
			c.opts[i] = o
		}
	}
	c.waitOnClusterReady()
}

func (c *cluster) restartAllSamePorts() {
	c.t.Helper()
	for i, s := range c.servers {
		if !s.Running() {
			opts := c.opts[i]
			s := RunServer(opts)
			c.servers[i] = s
		}
	}
	c.waitOnClusterReady()
}

func (c *cluster) totalSubs() (total int) {
	c.t.Helper()
	for _, s := range c.servers {
		total += int(s.NumSubscriptions())
	}
	return total
}

func (c *cluster) stableTotalSubs() (total int) {
	nsubs := -1
	checkFor(c.t, 2*time.Second, 250*time.Millisecond, func() error {
		subs := c.totalSubs()
		if subs == nsubs {
			return nil
		}
		nsubs = subs
		return fmt.Errorf("Still stabilizing")
	})
	return nsubs

}
