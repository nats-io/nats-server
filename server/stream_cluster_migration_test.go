package server

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func addJetStreamClusterWithTemplateAndModHook(t *testing.T, sc *supercluster, tmpl string, numServersPer, numClusters int, modify modifyCb) *supercluster {
	t.Helper()
	if numServersPer < 1 {
		t.Fatalf("Number of servers must be >= 1")
	}
	if numClusters <= 1 {
		t.Fatalf("Number of clusters must be > 1")
	}

	startClusterPorts := []int{20_022}
	startGatewayPorts := []int{20_122}
	startClusterPort := startClusterPorts[rand.Intn(len(startClusterPorts))]
	startGWPort := startGatewayPorts[rand.Intn(len(startGatewayPorts))]

	// Make the GWs form faster for the tests.
	SetGatewaysSolicitDelay(10 * time.Millisecond)
	defer ResetGatewaysSolicitDelay()

	cp, gp := startClusterPort, startGWPort
	var gws []string

	// Build GWs first, will be same for all servers.
	for i, port := 1, gp; i <= numClusters; i++ {
		cn := fmt.Sprintf("C%d", i)
		var urls []string
		for n := 0; n < numServersPer; n++ {
			routeURL := fmt.Sprintf("nats-route://127.0.0.1:%d", port)
			urls = append(urls, routeURL)
			port++

		}
		gws = append(gws, fmt.Sprintf(jsGWTempl, "\n\t\t\t", cn, strings.Join(urls, ",")))
	}
	gwconf := strings.Join(gws, _EMPTY_)
	//	fmt.Println(gwconf)

	// Add the cluster here
	cn := fmt.Sprintf("C%d", 4)
	// Go ahead and build configurations.
	c := &cluster{servers: make([]*Server, 0, numServersPer), opts: make([]*Options, 0, numServersPer), name: cn}

	// Build out the routes that will be shared with all configs.
	var routes []string
	for port := cp; port < cp+numServersPer; port++ {
		routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", port+9))
	}
	routeConfig := strings.Join(routes, ",")

	for si := range numServersPer {
		storeDir := t.TempDir()
		sn := fmt.Sprintf("%s-S%d", cn, si+1)
		//		fmt.Println("using port", cp+si+9)
		bconf := fmt.Sprintf(tmpl, sn, storeDir, cn, cp+si+9, routeConfig)
		conf := fmt.Sprintf(jsSuperClusterTempl, bconf, cn, gp+9, gwconf)
		gp++
		if modify != nil {
			conf = modify(sn, cn, storeDir, conf)
		}
		//		fmt.Println(conf)
		s, o := RunServerWithConfig(createConfFile(t, []byte(conf)))
		c.servers = append(c.servers, s)
		c.opts = append(c.opts, o)
	}
	checkClusterFormed(t, c.servers...)
	sc.clusters = append(sc.clusters, c)
	cp += numServersPer
	c.t = t

	// Wait for the supercluster to be formed.
	egws := numClusters - 1
	for _, s := range c.servers {
		waitForOutboundGateways(t, s, egws, 10*time.Second)
	}

	//	sc := &supercluster{t, clusters, nil}
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

func TestStreamClusterMigration(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()
	sc.waitOnLeader()
	leader := sc.leader()

	fmt.Println("leader is ", leader.info.Name)
	nc, client := jsClientConnect(t, sc.leader())
	defer nc.Close()

	si, err := client.AddStream(&nats.StreamConfig{
		Name:     "test",
		Subjects: []string{"foo"},
		Replicas: 3,
		Placement: &nats.Placement{
			Tags: []string{"country:us"},
		},
	})
	require_NoError(t, err)

	fmt.Println("Stream is on cluster: ", si.Cluster.Name)
	fmt.Println(si.Cluster.Leader)
	fmt.Println(si.Cluster.Replicas[0])
	fmt.Println(si.Cluster.Replicas[1])

	sc.waitOnStreamLeader("$G", "test")

	sc = addJetStreamClusterWithTemplateAndModHook(t, sc, jsClusterTempl, 3, 4, nil)

	sc.waitOnAllCurrent()
	fmt.Println(len(leader.JetStreamClusterPeers()))

	reset := func(s *Server) {
		s.mu.Lock()
		rch := s.sys.resetCh
		s.mu.Unlock()
		if rch != nil {
			rch <- struct{}{}
		}
		s.sendStatszUpdate()
	}

	// Make the new cluster country:us
	for _, s := range sc.clusterForName("C4").servers {
		s.optsMu.Lock()
		s.opts.Tags.Add("country:us")
		s.optsMu.Unlock()
		reset(s)
	}

	sc.waitOnLeader()
	ml := sc.leader()
	require_True(t, ml != nil)
	js := ml.getJetStream()
	require_True(t, js != nil)
	js.mu.RLock()
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
	js.mu.RUnlock()

	// Peer remove all servers in cluster C1

	c1 := sc.clusterForName("C1")
	require_NotNil(t, c1)

	// Peer remove each server in C1
	for _, s := range c1.servers {
		sc.waitOnLeader()
		leader = sc.leader()

		// Connect a system client to the meta leader for administrative API calls
		sysNC, err := nats.Connect(leader.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
		require_NoError(t, err)

		peerName := s.Name()
		req := &JSApiMetaServerRemoveRequest{Server: peerName}
		jsreq, err := json.Marshal(req)
		require_NoError(t, err)

		rmsg, err := sysNC.Request(JSApiRemoveServer, jsreq, time.Second)
		require_NoError(t, err)

		var resp JSApiMetaServerRemoveResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
		require_True(t, resp.Error == nil)
		require_True(t, resp.Success)

		sysNC.Close()
	}

	sc.waitOnLeader()
	ml = sc.leader()

	sc.waitOnPeerCount(9)
	peers := ml.JetStreamClusterPeers()
	t.Log("cluster peers: ", len(peers), ml.JetStreamClusterPeers())

	// Verify C1 is no longer part of the super cluster
	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		for _, s := range c1.servers {
			if slices.Contains(ml.JetStreamClusterPeers(), s.Name()) {
				return fmt.Errorf("Server %s still in the peer list", s.Name())
			}
		}
		return nil
	})

	sc.waitOnStreamLeader("$G", "test")

	var sl *Server
	for _, c := range sc.clusters {
		if sl = c.streamLeader("$G", "test"); sl != nil {
			break
		}
	}

	if sl != nil {
		t.Log(sl.Name())
	}

	conn, client := jsClientConnect(t, sl)
	defer conn.Close()

	testSI, err := client.StreamInfo("test")
	if err != nil {
		t.Fatal("failed to get stream info:", err)
	}
	t.Log(testSI)

}
