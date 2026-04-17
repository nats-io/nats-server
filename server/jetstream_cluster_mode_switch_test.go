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

//go:build !skip_js_tests && !skip_js_cluster_tests

package server

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/highwayhash"
	"github.com/nats-io/nats.go"
)

// findDataServer returns the cluster server whose JetStream store dir lives
// inside baseDir (i.e. baseDir is the parent of JetStreamConfig().StoreDir).
func findDataServer(c *cluster, baseDir string) *Server {
	for _, srv := range c.servers {
		if filepath.Dir(srv.JetStreamConfig().StoreDir) == baseDir {
			return srv
		}
	}
	return nil
}

// triggerAndWaitForAdoption triggers checkForOrphans on the data server and
// waits until the named stream has a leader, meaning adoption completed.
func triggerAndWaitForAdoption(t *testing.T, c *cluster, dataServer *Server, acc, stream string) {
	t.Helper()
	checkFor(t, 15*time.Second, 200*time.Millisecond, func() error {
		if c.leader() == nil {
			return fmt.Errorf("no meta leader yet")
		}
		dataServer.getJetStream().checkForOrphans()
		if c.streamLeader(acc, stream) == nil {
			return fmt.Errorf("stream %s not yet adopted", stream)
		}
		return nil
	})
}

// TestJetStreamStandaloneToClusteredModeSwitch tests that a server started in
// standalone mode can be reconfigured to run in clustered mode without losing
// stream data, consumers, or message state.
func TestJetStreamStandaloneToClusteredModeSwitch(t *testing.T) {
	t.Log("Starting standalone JetStream server")
	storeDir := t.TempDir()
	t.Logf("storeDir: %s", storeDir)

	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = storeDir
	opts.ServerName = "S1"
	s := RunServer(&opts)

	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)

	req, _ := json.Marshal(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>"},
		Storage:  FileStorage,
	})
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, "TEST"), req, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiStreamCreateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	if resp.Error != nil {
		t.Fatalf("Unexpected error creating stream: %+v", resp.Error)
	}
	t.Log("Created stream TEST on standalone server")

	numMessages := 100
	for i := 0; i < numMessages; i++ {
		_, err := nc.Request(fmt.Sprintf("foo.%d", i), []byte(fmt.Sprintf("msg-%d", i)), 2*time.Second)
		require_NoError(t, err)
	}
	t.Logf("Published %d messages to TEST", numMessages)

	consReq, _ := json.Marshal(&CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			Durable:       "dur",
			AckPolicy:     AckExplicit,
			DeliverPolicy: DeliverAll,
		},
	})
	rmsg, err = nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", "dur"), consReq, 5*time.Second)
	require_NoError(t, err)
	var consResp JSApiConsumerCreateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &consResp))
	if consResp.Error != nil {
		t.Fatalf("Unexpected error creating consumer: %+v", consResp.Error)
	}
	t.Log("Created durable consumer 'dur' on TEST, shutting down standalone")

	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()

	t.Log("Promoting standalone server into 3-node cluster (S-1 reuses storeDir)")
	modify := func(serverName, clusterName, autoStoreDir, conf string) string {
		if serverName == "S-1" {
			return strings.Replace(conf, autoStoreDir, storeDir, 1)
		}
		return conf
	}
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "C1", 3, modify)
	defer c.shutdown()

	c.waitOnLeader()
	t.Logf("Cluster formed, leader: %s", c.leader().Name())

	dataServer := findDataServer(c, storeDir)
	require_NotNil(t, dataServer)
	t.Logf("Data server (has standalone data): %s, triggering orphan adoption", dataServer.Name())

	triggerAndWaitForAdoption(t, c, dataServer, "$G", "TEST")

	c.waitOnStreamLeader("$G", "TEST")
	streamLeader := c.streamLeader("$G", "TEST")
	require_NotNil(t, streamLeader)
	t.Logf("Stream TEST adopted, leader: %s", streamLeader.Name())

	nc2, err := nats.Connect(streamLeader.ClientURL())
	require_NoError(t, err)
	defer nc2.Close()

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), nil, 5*time.Second)
	require_NoError(t, err)
	var siResp JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &siResp))
	if siResp.Error != nil {
		t.Fatalf("stream info error: %+v", siResp.Error)
	}
	if siResp.State.Msgs != uint64(numMessages) {
		t.Fatalf("expected %d messages, got %d", numMessages, siResp.State.Msgs)
	}
	t.Logf("Stream TEST has %d messages, all standalone messages intact", siResp.State.Msgs)

	c.waitOnConsumerLeader("$G", "TEST", "dur")
	rmsg, err = nc2.Request(fmt.Sprintf(JSApiConsumerInfoT, "TEST", "dur"), nil, 5*time.Second)
	require_NoError(t, err)
	var ciResp JSApiConsumerInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &ciResp))
	if ciResp.Error != nil {
		t.Fatalf("consumer info error: %+v", ciResp.Error)
	}
	if ciResp.Config.Durable != "dur" {
		t.Fatalf("expected durable %q, got %q", "dur", ciResp.Config.Durable)
	}
	t.Log("Consumer 'dur' adopted successfully")

	_, err = nc2.Request("foo.new", []byte("new-message"), 2*time.Second)
	require_NoError(t, err)

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), nil, 5*time.Second)
	require_NoError(t, err)
	var siResp2 JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &siResp2))
	if siResp2.State.Msgs != uint64(numMessages+1) {
		t.Fatalf("expected %d messages after publish, got %d", numMessages+1, siResp2.State.Msgs)
	}
	t.Logf("New publish succeeded, stream now has %d messages", siResp2.State.Msgs)
}

// TestJetStreamStandaloneToClusteredNonLeaderAdoption tests that when a server with
// standalone data joins a cluster and is NOT the meta leader, streams are still
// adopted, the adoption request is automatically forwarded to the current cluster leader.
func TestJetStreamStandaloneToClusteredNonLeaderAdoption(t *testing.T) {
	t.Log("Starting standalone JetStream server")
	storeDir := t.TempDir()

	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = storeDir
	opts.ServerName = "S1"
	s := RunServer(&opts)

	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)

	req, _ := json.Marshal(&StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.>"},
		Storage:  FileStorage,
	})
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, "ORDERS"), req, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiStreamCreateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	if resp.Error != nil {
		t.Fatalf("Unexpected error creating stream: %+v", resp.Error)
	}

	numMessages := 50
	for i := 0; i < numMessages; i++ {
		_, err := nc.Request(fmt.Sprintf("orders.%d", i), []byte(fmt.Sprintf("order-%d", i)), 2*time.Second)
		require_NoError(t, err)
	}
	t.Logf("Created stream ORDERS with %d messages", numMessages)

	consReq, _ := json.Marshal(&CreateConsumerRequest{
		Stream: "ORDERS",
		Config: ConsumerConfig{
			Durable:       "processor",
			AckPolicy:     AckExplicit,
			DeliverPolicy: DeliverAll,
		},
	})
	rmsg, err = nc.Request(fmt.Sprintf(JSApiDurableCreateT, "ORDERS", "processor"), consReq, 5*time.Second)
	require_NoError(t, err)
	var consResp JSApiConsumerCreateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &consResp))
	if consResp.Error != nil {
		t.Fatalf("Unexpected error creating consumer: %+v", consResp.Error)
	}
	t.Log("Created durable consumer 'processor', shutting down standalone")

	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()

	// S-3 gets the standalone storeDir; S-1/S-2 are clean and will likely win leader election.
	t.Log("Starting 3-node cluster: S-3 has standalone data, S-1/S-2 are clean")
	modify := func(serverName, clusterName, autoStoreDir, conf string) string {
		if serverName == "S-3" {
			return strings.Replace(conf, autoStoreDir, storeDir, 1)
		}
		return conf
	}
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "NL", 3, modify)
	defer c.shutdown()

	c.waitOnLeader()
	t.Logf("Cluster formed, leader: %s", c.leader().Name())

	dataServer := findDataServer(c, storeDir)
	require_NotNil(t, dataServer)
	t.Logf("Data server (has standalone data): %s", dataServer.Name())

	// Ensure the data server is NOT the meta leader so we test the non-leader adoption path.
	if dataServer.JetStreamIsLeader() {
		t.Logf("Data server is leader, stepping down to force non-leader adoption path")
		nc2, err := nats.Connect(dataServer.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
		require_NoError(t, err)
		nc2.Request(JSApiLeaderStepDown, nil, 2*time.Second)
		nc2.Close()
		c.waitOnLeader()
		checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
			if dataServer.JetStreamIsLeader() {
				return fmt.Errorf("data server is still meta leader")
			}
			return nil
		})
	}
	t.Logf("Data server %s is a follower, adoption will be forwarded to cluster leader %s",
		dataServer.Name(), c.leader().Name())

	triggerAndWaitForAdoption(t, c, dataServer, "$G", "ORDERS")
	t.Logf("Stream ORDERS adopted, leader: %s", c.streamLeader("$G", "ORDERS").Name())

	leader := c.leader()
	require_NotNil(t, leader)
	nc2, err := nats.Connect(leader.ClientURL())
	require_NoError(t, err)
	defer nc2.Close()

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamInfoT, "ORDERS"), nil, 5*time.Second)
	require_NoError(t, err)
	var siResp JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &siResp))
	if siResp.Error != nil {
		t.Fatalf("stream info error: %+v", siResp.Error)
	}
	if siResp.State.Msgs != uint64(numMessages) {
		t.Fatalf("expected %d messages, got %d", numMessages, siResp.State.Msgs)
	}
	t.Logf("Stream ORDERS has %d messages, all standalone messages intact", siResp.State.Msgs)

	c.waitOnConsumerLeader("$G", "ORDERS", "processor")
	rmsg, err = nc2.Request(fmt.Sprintf(JSApiConsumerInfoT, "ORDERS", "processor"), nil, 5*time.Second)
	require_NoError(t, err)
	var ciResp JSApiConsumerInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &ciResp))
	if ciResp.Error != nil {
		t.Fatalf("consumer info error: %+v", ciResp.Error)
	}
	t.Log("Consumer 'processor' adopted successfully")

	_, err = nc2.Request("orders.new", []byte("new-order"), 2*time.Second)
	require_NoError(t, err)

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamInfoT, "ORDERS"), nil, 5*time.Second)
	require_NoError(t, err)
	var siResp2 JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &siResp2))
	if siResp2.State.Msgs != uint64(numMessages+1) {
		t.Fatalf("expected %d messages after publish, got %d", numMessages+1, siResp2.State.Msgs)
	}
	t.Logf("New publish succeeded, stream now has %d messages", siResp2.State.Msgs)
}

// TestJetStreamStandaloneToClusteredDuplicateStreamName tests that when a standalone
// server has a stream with the same name as one already in the cluster, the cluster
// stream takes precedence and the standalone stream data is preserved on disk so the
// operator can recover it. The PTC marker must remain until the conflict is resolved.
func TestJetStreamStandaloneToClusteredDuplicateStreamName(t *testing.T) {
	t.Log("Starting standalone server, creating stream EVENTS on standalone.>")
	storeDir := t.TempDir()

	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = storeDir
	opts.ServerName = "S1"
	s := RunServer(&opts)

	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)

	req, _ := json.Marshal(&StreamConfig{
		Name:     "EVENTS",
		Subjects: []string{"standalone.>"},
		Storage:  FileStorage,
	})
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, "EVENTS"), req, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiStreamCreateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	if resp.Error != nil {
		t.Fatalf("Unexpected error creating stream: %+v", resp.Error)
	}

	for i := 0; i < 10; i++ {
		_, err := nc.Request(fmt.Sprintf("standalone.%d", i), []byte("data"), 2*time.Second)
		require_NoError(t, err)
	}
	t.Log("Published 10 messages to standalone EVENTS, shutting down standalone")

	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()

	t.Log("Starting fresh 3-node cluster and creating a DIFFERENT EVENTS stream on cluster.>")
	c := createJetStreamCluster(t, jsClusterTempl, "DUP", _EMPTY_, 3, 22_033, true)
	defer c.shutdown()

	c.waitOnLeader()
	leader := c.leader()
	t.Logf("Cluster leader: %s", leader.Name())

	nc2, err := nats.Connect(leader.ClientURL())
	require_NoError(t, err)
	defer nc2.Close()

	clusterReq, _ := json.Marshal(&StreamConfig{
		Name:     "EVENTS",
		Subjects: []string{"cluster.>"},
		Storage:  FileStorage,
		Replicas: 3,
	})
	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamCreateT, "EVENTS"), clusterReq, 5*time.Second)
	require_NoError(t, err)
	var clusterResp JSApiStreamCreateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &clusterResp))
	if clusterResp.Error != nil {
		t.Fatalf("Unexpected error creating cluster stream: %+v", clusterResp.Error)
	}

	for i := 0; i < 20; i++ {
		_, err := nc2.Request(fmt.Sprintf("cluster.%d", i), []byte("cluster-data"), 2*time.Second)
		require_NoError(t, err)
	}
	t.Log("Published 20 messages to cluster EVENTS, cluster stream is authoritative")
	nc2.Close()

	// addInNewServer() adds a 4th node with a temp dir. We swap its storeDir to the
	// standalone storeDir so it joins the existing 3-node cluster carrying the old data.
	// Cluster after this block: S-1, S-2, S-3 (original) + S-4 (standalone-promoted, 4th node).
	t.Log("Adding a 4th server carrying the standalone storeDir to the existing 3-node cluster")
	srvNew := c.addInNewServer()
	newOpts := srvNew.getOpts().Clone()
	t.Logf("Temp 4th server %s started (temp dir), stopping to swap storeDir", srvNew.Name())
	srvNew.Shutdown()
	srvNew.WaitForShutdown()
	c.servers = c.servers[:len(c.servers)-1]
	c.opts = c.opts[:len(c.opts)-1]

	newOpts.StoreDir = storeDir
	newOpts.Port = -1
	srvNew = RunServer(newOpts)
	c.servers = append(c.servers, srvNew)
	c.opts = append(c.opts, newOpts)
	c.checkClusterFormed()

	serverNames := make([]string, len(c.servers))
	for i, srv := range c.servers {
		role := "follower"
		if srv.JetStreamIsLeader() {
			role = "META-LEADER"
		}
		serverNames[i] = fmt.Sprintf("%s(%s)", srv.Name(), role)
	}
	t.Logf("Cluster now has %d members: %v", len(c.servers), serverNames)
	t.Logf("%s is the standalone-promoted server, the cluster will detect the EVENTS name conflict and preserve the local copy on disk", srvNew.Name())

	checkFor(t, 15*time.Second, 500*time.Millisecond, func() error {
		if c.leader() == nil {
			return fmt.Errorf("no meta leader yet")
		}
		srvNew.getJetStream().checkForOrphans()
		return nil
	})
	time.Sleep(500 * time.Millisecond)

	leader = c.leader()
	require_NotNil(t, leader)
	nc3, err := nats.Connect(leader.ClientURL())
	require_NoError(t, err)
	defer nc3.Close()

	rmsg, err = nc3.Request(fmt.Sprintf(JSApiStreamInfoT, "EVENTS"), nil, 5*time.Second)
	require_NoError(t, err)
	var siResp JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &siResp))
	if siResp.Error != nil {
		t.Fatalf("stream info error: %+v", siResp.Error)
	}
	if siResp.State.Msgs != 20 {
		t.Fatalf("expected 20 cluster messages, got %d", siResp.State.Msgs)
	}
	if len(siResp.Config.Subjects) != 1 || siResp.Config.Subjects[0] != "cluster.>" {
		t.Fatalf("expected cluster subjects [cluster.>], got %v", siResp.Config.Subjects)
	}
	t.Logf("Cluster EVENTS intact: %d msgs on subject cluster.>, cluster wins", siResp.State.Msgs)

	streamDir := filepath.Join(storeDir, JetStreamStoreDir, "$G", "streams", "EVENTS")
	if _, err := os.Stat(streamDir); err != nil {
		t.Fatalf("expected standalone stream data at %s, got error: %v", streamDir, err)
	}
	blkFiles, _ := filepath.Glob(filepath.Join(streamDir, msgDir, "*.blk"))
	if len(blkFiles) == 0 {
		t.Fatalf("expected message block files in %s/%s/, but none found", streamDir, msgDir)
	}
	t.Logf("Standalone EVENTS data preserved on disk at %s (%d .blk files)", streamDir, len(blkFiles))

	sysAcc := srvNew.SystemAccount()
	require_NotNil(t, sysAcc)
	markerPath := filepath.Join(
		storeDir, JetStreamStoreDir, sysAcc.Name,
		defaultStoreDirName, defaultMetaGroupName, ptcMarkerFile,
	)
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("expected PTC marker at %s (conflict unresolved), got error: %v", markerPath, err)
	}
	t.Logf("PTC marker present at %s, conflict is unresolved, marker correctly kept", markerPath)

	t.Log("Restarting promoted server, standalone data and PTC marker must survive")
	srvNew.Shutdown()
	srvNew.WaitForShutdown()
	c.servers = c.servers[:len(c.servers)-1]
	c.opts = c.opts[:len(c.opts)-1]

	srvNew = RunServer(newOpts)
	c.servers = append(c.servers, srvNew)
	c.opts = append(c.opts, newOpts)
	c.checkClusterFormed()
	t.Logf("Server restarted: %s", srvNew.Name())

	checkFor(t, 15*time.Second, 500*time.Millisecond, func() error {
		if c.leader() == nil {
			return fmt.Errorf("no meta leader yet")
		}
		srvNew.getJetStream().checkForOrphans()
		return nil
	})
	time.Sleep(500 * time.Millisecond)

	if _, err := os.Stat(streamDir); err != nil {
		t.Fatalf("after restart, expected standalone stream data at %s, got error: %v", streamDir, err)
	}
	blkFiles, _ = filepath.Glob(filepath.Join(streamDir, msgDir, "*.blk"))
	if len(blkFiles) == 0 {
		t.Fatalf("after restart, expected message block files but none found")
	}
	t.Logf("Standalone EVENTS data still preserved after restart (%d .blk files)", len(blkFiles))

	nc4, err := nats.Connect(leader.ClientURL())
	require_NoError(t, err)
	defer nc4.Close()

	rmsg, err = nc4.Request(fmt.Sprintf(JSApiStreamInfoT, "EVENTS"), nil, 5*time.Second)
	require_NoError(t, err)
	var siResp2 JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &siResp2))
	if siResp2.State.Msgs != 20 {
		t.Fatalf("after restart, expected 20 cluster messages, got %d", siResp2.State.Msgs)
	}
	t.Logf("Cluster EVENTS still has %d messages, unaffected by restart", siResp2.State.Msgs)
}

// TestJetStreamStandaloneToClusteredPTCMarkerLifecycle tests the promoted-to-cluster
// (PTC) marker lifecycle: the marker is written when a standalone server first joins
// a cluster (first join). After adoption completes and a second check finds
// nothing pending, the marker is removed.
func TestJetStreamStandaloneToClusteredPTCMarkerLifecycle(t *testing.T) {
	t.Log("Starting standalone server, creating stream MYSTREAM")
	storeDir := t.TempDir()

	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = storeDir
	opts.ServerName = "S1"
	s := RunServer(&opts)

	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)

	req, _ := json.Marshal(&StreamConfig{
		Name:     "MYSTREAM",
		Subjects: []string{"test.>"},
		Storage:  FileStorage,
	})
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, "MYSTREAM"), req, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiStreamCreateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	if resp.Error != nil {
		t.Fatalf("Unexpected error creating stream: %+v", resp.Error)
	}

	numMessages := 10
	for i := 0; i < numMessages; i++ {
		_, err := nc.Request(fmt.Sprintf("test.%d", i), []byte("data"), 2*time.Second)
		require_NoError(t, err)
	}
	t.Logf("Created stream MYSTREAM with %d messages, shutting down standalone", numMessages)

	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()

	t.Log("Promoting to 3-node cluster (S-1 reuses storeDir)")
	modify := func(serverName, clusterName, autoStoreDir, conf string) string {
		if serverName == "S-1" {
			return strings.Replace(conf, autoStoreDir, storeDir, 1)
		}
		return conf
	}
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "PM", 3, modify)
	defer c.shutdown()

	c.waitOnLeader()
	t.Logf("Cluster formed, leader: %s", c.leader().Name())

	dataServer := findDataServer(c, storeDir)
	require_NotNil(t, dataServer)
	t.Logf("Data server: %s, first time joining a cluster, PTC marker must have been written", dataServer.Name())

	sysAcc := dataServer.SystemAccount()
	require_NotNil(t, sysAcc)
	markerPath := filepath.Join(
		storeDir, JetStreamStoreDir, sysAcc.Name,
		defaultStoreDirName, defaultMetaGroupName, ptcMarkerFile,
	)
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("expected PTC marker immediately after cluster join at %s: %v", markerPath, err)
	}
	t.Logf("PTC marker present at %s, server is in standalone-to-cluster promotion mode", markerPath)

	t.Log("Triggering orphan adoption for MYSTREAM")
	triggerAndWaitForAdoption(t, c, dataServer, "$G", "MYSTREAM")

	c.waitOnStreamLeader("$G", "MYSTREAM")
	nc2, err := nats.Connect(c.streamLeader("$G", "MYSTREAM").ClientURL())
	require_NoError(t, err)
	defer nc2.Close()

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamInfoT, "MYSTREAM"), nil, 5*time.Second)
	require_NoError(t, err)
	var siResp JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &siResp))
	if siResp.Error != nil {
		t.Fatalf("stream info error: %+v", siResp.Error)
	}
	if siResp.State.Msgs != uint64(numMessages) {
		t.Fatalf("expected %d messages, got %d", numMessages, siResp.State.Msgs)
	}
	t.Logf("Stream MYSTREAM adopted with %d messages", siResp.State.Msgs)

	// Second pass: nothing pending to marker must be removed.
	t.Log("Running second check, no orphans remain, PTC marker must be removed")
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		dataServer.getJetStream().checkForOrphans()
		if _, err := os.Stat(markerPath); err == nil {
			return fmt.Errorf("PTC marker still present; waiting for cleanup")
		}
		return nil
	})
	t.Logf("PTC marker removed, promotion fully resolved")

	streamDir := filepath.Join(storeDir, JetStreamStoreDir, "$G", "streams", "MYSTREAM")
	if _, err := os.Stat(streamDir); err != nil {
		t.Fatalf("expected stream data at %s: %v", streamDir, err)
	}
	t.Logf("Stream data still on disk at %s, no data was deleted during promotion", streamDir)
}

// renameStreamOnDisk renames a stream's directory and updates meta.inf, meta.sum,
// and per-message block checksums so the server loads it under the new name.
// The server must be stopped before calling this. Only works for unencrypted,
// uncompressed file-storage streams.
//
// storeDir is the opts.StoreDir of the standalone server.
// account is the account name (e.g. "$G").
// oldName is the current stream name, newName is the desired name.
func renameStreamOnDisk(t *testing.T, storeDir, account, oldName, newName string) {
	t.Helper()

	oldDir := filepath.Join(storeDir, JetStreamStoreDir, account, "streams", oldName)
	newDir := filepath.Join(storeDir, JetStreamStoreDir, account, "streams", newName)

	newKey := sha256.Sum256([]byte(newName))
	newHH, err := highwayhash.NewDigest64(newKey[:])
	require_NoError(t, err)

	// 1. Update meta.inf with the new stream name.
	metaFile := filepath.Join(oldDir, JetStreamMetaFile)
	buf, err := os.ReadFile(metaFile)
	require_NoError(t, err)

	var cfg FileStreamInfo
	require_NoError(t, json.Unmarshal(buf, &cfg))
	cfg.Name = newName
	newBuf, err := json.Marshal(cfg)
	require_NoError(t, err)
	require_NoError(t, os.WriteFile(metaFile, newBuf, defaultFilePerms))

	// 2. Recompute meta.sum.
	newHH.Reset()
	newHH.Write(newBuf)
	var hb [highwayhash.Size64]byte
	checksum := hex.EncodeToString(newHH.Sum(hb[:0]))
	sumFile := filepath.Join(oldDir, JetStreamMetaFileSum)
	require_NoError(t, os.WriteFile(sumFile, []byte(checksum), defaultFilePerms))

	// 3. Rewrite per-message checksums in all .blk files.
	msgsDir := filepath.Join(oldDir, msgDir)
	blkFiles, err := os.ReadDir(msgsDir)
	require_NoError(t, err)

	le := binary.LittleEndian
	const hdrSize = msgHdrSize
	const hashSize = recordHashSize
	const headerBit = uint32(1 << 31)

	for _, fi := range blkFiles {
		if !strings.HasSuffix(fi.Name(), ".blk") {
			continue
		}
		var blkIndex int
		if n, err := fmt.Sscanf(fi.Name(), "%d.blk", &blkIndex); err != nil || n != 1 {
			continue
		}

		blkPath := filepath.Join(msgsDir, fi.Name())
		data, err := os.ReadFile(blkPath)
		require_NoError(t, err)
		if len(data) == 0 {
			continue
		}

		blkKey := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", newName, blkIndex)))
		blkHH, err := highwayhash.NewDigest64(blkKey[:])
		require_NoError(t, err)

		for idx := uint32(0); idx < uint32(len(data)); {
			if idx+hdrSize > uint32(len(data)) {
				break
			}
			hdr := data[idx : idx+hdrSize]
			rl := le.Uint32(hdr[0:])
			hasHeaders := rl&headerBit != 0
			rl &^= headerBit
			if rl < uint32(hdrSize+hashSize) || idx+rl > uint32(len(data)) {
				break
			}
			dlen := int(rl) - hdrSize
			slen := int(le.Uint16(hdr[20:]))
			rec := data[idx+hdrSize : idx+rl]

			shlen := slen
			if hasHeaders {
				shlen += 4
			}
			if dlen < hashSize || shlen > (dlen-hashSize) {
				break
			}

			blkHH.Reset()
			blkHH.Write(hdr[4:20])
			blkHH.Write(rec[:slen])
			if hasHeaders {
				blkHH.Write(rec[slen+4 : dlen-hashSize])
			} else {
				blkHH.Write(rec[slen : dlen-hashSize])
			}
			copy(rec[dlen-hashSize:dlen], blkHH.Sum(hb[:0]))

			idx += rl
		}

		require_NoError(t, os.WriteFile(blkPath, data, defaultFilePerms))
	}

	// 4. Remove index.db so the server rebuilds state from block files.
	// The index contains block-level checksums keyed by the old name.
	os.Remove(filepath.Join(msgsDir, "index.db"))

	// 5. Rename the directory.
	require_NoError(t, os.Rename(oldDir, newDir))
}

// TestJetStreamStandaloneRenameStreamThenPromote tests that an operator can rename
// a stream on disk while the server is stopped, then promote to a cluster, and
// the stream appears under the new name with all messages and consumers intact.
func TestJetStreamStandaloneRenameStreamThenPromote(t *testing.T) {
	t.Log("Starting standalone server, creating stream ORDERS with consumer")
	storeDir := t.TempDir()

	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = storeDir
	opts.ServerName = "S1"
	s := RunServer(&opts)

	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)

	req, _ := json.Marshal(&StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.>"},
		Storage:  FileStorage,
	})
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, "ORDERS"), req, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiStreamCreateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	if resp.Error != nil {
		t.Fatalf("Unexpected error creating stream: %+v", resp.Error)
	}

	numMessages := 50
	for i := 0; i < numMessages; i++ {
		_, err := nc.Request(fmt.Sprintf("orders.%d", i), []byte(fmt.Sprintf("order-%d", i)), 2*time.Second)
		require_NoError(t, err)
	}
	t.Logf("Published %d messages to stream ORDERS", numMessages)

	consReq, _ := json.Marshal(&CreateConsumerRequest{
		Stream: "ORDERS",
		Config: ConsumerConfig{
			Durable:       "processor",
			AckPolicy:     AckExplicit,
			DeliverPolicy: DeliverAll,
		},
	})
	rmsg, err = nc.Request(fmt.Sprintf(JSApiDurableCreateT, "ORDERS", "processor"), consReq, 5*time.Second)
	require_NoError(t, err)
	var consResp JSApiConsumerCreateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &consResp))
	if consResp.Error != nil {
		t.Fatalf("Unexpected error creating consumer: %+v", consResp.Error)
	}
	t.Log("Created durable consumer 'processor', shutting down standalone")

	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()

	t.Log("Renaming stream on disk: ORDERS to ORDERS_V2 (operator intervention while server is stopped)")
	renameStreamOnDisk(t, storeDir, "$G", "ORDERS", "ORDERS_V2")

	oldDir := filepath.Join(storeDir, JetStreamStoreDir, "$G", "streams", "ORDERS")
	newDir := filepath.Join(storeDir, JetStreamStoreDir, "$G", "streams", "ORDERS_V2")
	if _, err := os.Stat(oldDir); !os.IsNotExist(err) {
		t.Fatalf("old stream directory should not exist: %v", err)
	}
	if _, err := os.Stat(newDir); err != nil {
		t.Fatalf("new stream directory should exist: %v", err)
	}
	t.Logf("Rename done, old dir gone, new dir at %s", newDir)

	t.Log("Promoting to 3-node cluster (S-1 reuses storeDir)")
	modify := func(serverName, clusterName, autoStoreDir, conf string) string {
		if serverName == "S-1" {
			return strings.Replace(conf, autoStoreDir, storeDir, 1)
		}
		return conf
	}
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "RN", 3, modify)
	defer c.shutdown()

	c.waitOnLeader()
	t.Logf("Cluster formed, leader: %s", c.leader().Name())

	dataServer := findDataServer(c, storeDir)
	require_NotNil(t, dataServer)
	t.Logf("Data server: %s, ORDERS_V2 is an orphan (no meta assignment), ORDERS no longer on disk", dataServer.Name())

	triggerAndWaitForAdoption(t, c, dataServer, "$G", "ORDERS_V2")
	t.Logf("ORDERS_V2 adopted, leader: %s", c.streamLeader("$G", "ORDERS_V2").Name())

	c.waitOnStreamLeader("$G", "ORDERS_V2")
	nc2, err := nats.Connect(c.streamLeader("$G", "ORDERS_V2").ClientURL())
	require_NoError(t, err)
	defer nc2.Close()

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamInfoT, "ORDERS_V2"), nil, 5*time.Second)
	require_NoError(t, err)
	var siResp JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &siResp))
	if siResp.Error != nil {
		t.Fatalf("stream info error: %+v", siResp.Error)
	}
	if siResp.State.Msgs != uint64(numMessages) {
		t.Fatalf("expected %d messages, got %d", numMessages, siResp.State.Msgs)
	}
	t.Logf("ORDERS_V2 has %d messages, all data preserved through rename + promote", siResp.State.Msgs)

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamInfoT, "ORDERS"), nil, 5*time.Second)
	require_NoError(t, err)
	var oldResp JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &oldResp))
	if oldResp.Error == nil {
		t.Fatalf("expected error for old stream name ORDERS, but got none")
	}
	t.Log("Old name ORDERS does not exist in cluster, correct")

	c.waitOnConsumerLeader("$G", "ORDERS_V2", "processor")
	rmsg, err = nc2.Request(fmt.Sprintf(JSApiConsumerInfoT, "ORDERS_V2", "processor"), nil, 5*time.Second)
	require_NoError(t, err)
	var ciResp JSApiConsumerInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &ciResp))
	if ciResp.Error != nil {
		t.Fatalf("consumer info error: %+v", ciResp.Error)
	}
	if ciResp.Config.Durable != "processor" {
		t.Fatalf("expected durable %q, got %q", "processor", ciResp.Config.Durable)
	}
	t.Log("Consumer 'processor' adopted under ORDERS_V2")

	_, err = nc2.Request("orders.new", []byte("new-order"), 2*time.Second)
	require_NoError(t, err)

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamInfoT, "ORDERS_V2"), nil, 5*time.Second)
	require_NoError(t, err)
	var siResp2 JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &siResp2))
	if siResp2.State.Msgs != uint64(numMessages+1) {
		t.Fatalf("expected %d messages after publish, got %d", numMessages+1, siResp2.State.Msgs)
	}
	t.Logf("New publish to orders.new succeeded, ORDERS_V2 now has %d messages", siResp2.State.Msgs)
}

// TestJetStreamStandaloneRenameStreamConsumerState tests that consumers created
// under the old stream name work correctly after an on-disk rename and promotion
// to cluster. Specifically verifies:
//   - Durable consumers are adopted under the renamed stream.
//   - Consumer delivery state (acked messages) is preserved.
//   - Consumers can fetch and ack new messages after adoption.
//   - Multiple consumers on the same renamed stream all work.
func TestJetStreamStandaloneRenameStreamConsumerState(t *testing.T) {
	t.Log("Starting standalone server, creating stream TASKS with 2 consumers")
	storeDir := t.TempDir()

	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = storeDir
	opts.ServerName = "S1"
	s := RunServer(&opts)

	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)
	js, err := nc.JetStream()
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TASKS",
		Subjects: []string{"tasks.>"},
		Storage:  nats.FileStorage,
	})
	require_NoError(t, err)

	numMessages := 20
	for i := 0; i < numMessages; i++ {
		_, err := js.Publish(fmt.Sprintf("tasks.%d", i), []byte(fmt.Sprintf("task-%d", i)))
		require_NoError(t, err)
	}
	t.Logf("Published %d messages (task-0 … task-19) to TASKS", numMessages)

	_, err = js.AddConsumer("TASKS", &nats.ConsumerConfig{
		Durable:       "worker",
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
	})
	require_NoError(t, err)

	sub1, err := js.PullSubscribe("tasks.>", "worker", nats.BindStream("TASKS"))
	require_NoError(t, err)
	msgs, err := sub1.Fetch(5, nats.MaxWait(5*time.Second))
	require_NoError(t, err)
	if len(msgs) != 5 {
		t.Fatalf("expected 5 messages, got %d", len(msgs))
	}
	for _, m := range msgs {
		require_NoError(t, m.Ack())
	}
	time.Sleep(500 * time.Millisecond)
	t.Log("Consumer 'worker' fetched and acked 5 messages, ack floor should be seq 5")

	_, err = js.AddConsumer("TASKS", &nats.ConsumerConfig{
		Durable:       "auditor",
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
	})
	require_NoError(t, err)
	t.Log("Consumer 'auditor' created with no acks, all 20 messages pending, shutting down")

	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()

	t.Log("Renaming stream on disk: TASKS to JOBS (operator intervention)")
	renameStreamOnDisk(t, storeDir, "$G", "TASKS", "JOBS")
	t.Log("Rename complete, consumer ack state is preserved inside the renamed directory")

	t.Log("Promoting to 3-node cluster (S-1 reuses storeDir)")
	modify := func(serverName, clusterName, autoStoreDir, conf string) string {
		if serverName == "S-1" {
			return strings.Replace(conf, autoStoreDir, storeDir, 1)
		}
		return conf
	}
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "CS", 3, modify)
	defer c.shutdown()

	c.waitOnLeader()
	t.Logf("Cluster formed, leader: %s", c.leader().Name())

	dataServer := findDataServer(c, storeDir)
	require_NotNil(t, dataServer)
	t.Logf("Data server: %s, JOBS is an orphan (was TASKS, now renamed)", dataServer.Name())

	triggerAndWaitForAdoption(t, c, dataServer, "$G", "JOBS")
	t.Logf("Stream JOBS adopted, leader: %s", c.streamLeader("$G", "JOBS").Name())

	c.waitOnStreamLeader("$G", "JOBS")
	nc2, err := nats.Connect(c.streamLeader("$G", "JOBS").ClientURL())
	require_NoError(t, err)
	defer nc2.Close()

	rmsg, err := nc2.Request(fmt.Sprintf(JSApiStreamInfoT, "JOBS"), nil, 5*time.Second)
	require_NoError(t, err)
	var siResp JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &siResp))
	if siResp.Error != nil {
		t.Fatalf("stream info error: %+v", siResp.Error)
	}
	if siResp.State.Msgs != uint64(numMessages) {
		t.Fatalf("expected %d messages, got %d", numMessages, siResp.State.Msgs)
	}
	t.Logf("Stream JOBS has %d messages, all data intact after rename + promote", siResp.State.Msgs)

	c.waitOnConsumerLeader("$G", "JOBS", "worker")
	rmsg, err = nc2.Request(fmt.Sprintf(JSApiConsumerInfoT, "JOBS", "worker"), nil, 5*time.Second)
	require_NoError(t, err)
	var workerInfo JSApiConsumerInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &workerInfo))
	if workerInfo.Error != nil {
		t.Fatalf("worker consumer info error: %+v", workerInfo.Error)
	}
	if workerInfo.AckFloor.Consumer != 5 {
		t.Fatalf("expected worker ack floor seq 5, got %d", workerInfo.AckFloor.Consumer)
	}
	if workerInfo.NumPending != uint64(numMessages-5) {
		t.Fatalf("expected worker %d pending, got %d", numMessages-5, workerInfo.NumPending)
	}
	t.Logf("Consumer 'worker': ack floor=%d, pending=%d, ack state survived promotion",
		workerInfo.AckFloor.Consumer, workerInfo.NumPending)

	c.waitOnConsumerLeader("$G", "JOBS", "auditor")
	rmsg, err = nc2.Request(fmt.Sprintf(JSApiConsumerInfoT, "JOBS", "auditor"), nil, 5*time.Second)
	require_NoError(t, err)
	var auditorInfo JSApiConsumerInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &auditorInfo))
	if auditorInfo.Error != nil {
		t.Fatalf("auditor consumer info error: %+v", auditorInfo.Error)
	}
	if auditorInfo.AckFloor.Consumer != 0 {
		t.Fatalf("expected auditor ack floor 0, got %d", auditorInfo.AckFloor.Consumer)
	}
	if auditorInfo.NumPending != uint64(numMessages) {
		t.Fatalf("expected auditor %d pending, got %d", numMessages, auditorInfo.NumPending)
	}
	t.Logf("Consumer 'auditor': ack floor=0, pending=%d, no prior acks, all messages available", auditorInfo.NumPending)

	t.Log("Fetching remaining 15 messages for 'worker' from JOBS (continuing after ack floor 5)")
	js2, err := nc2.JetStream()
	require_NoError(t, err)
	sub2, err := js2.PullSubscribe("tasks.>", "worker", nats.BindStream("JOBS"))
	require_NoError(t, err)
	msgs, err = sub2.Fetch(15, nats.MaxWait(5*time.Second))
	require_NoError(t, err)
	if len(msgs) != 15 {
		t.Fatalf("expected 15 remaining messages for worker, got %d", len(msgs))
	}
	if string(msgs[0].Data) != "task-5" {
		t.Fatalf("expected first remaining message to be 'task-5', got %q", string(msgs[0].Data))
	}
	for _, m := range msgs {
		require_NoError(t, m.Ack())
	}
	time.Sleep(500 * time.Millisecond)

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiConsumerInfoT, "JOBS", "worker"), nil, 5*time.Second)
	require_NoError(t, err)
	require_NoError(t, json.Unmarshal(rmsg.Data, &workerInfo))
	if workerInfo.NumPending != 0 {
		t.Fatalf("expected worker 0 pending after consuming all, got %d", workerInfo.NumPending)
	}
	t.Log("Consumer 'worker' fully caught up, 0 messages pending")

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamInfoT, "TASKS"), nil, 5*time.Second)
	require_NoError(t, err)
	var oldResp JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &oldResp))
	if oldResp.Error == nil {
		t.Fatalf("expected error for old stream name TASKS, but got none")
	}
	t.Log("Old name TASKS not visible in cluster, rename is clean")
}

// TestJetStreamMultipleStreamsAdopted verifies that when a standalone server with
// multiple R1 streams joins a cluster, all streams and their durable consumers are
// adopted. After adoption the PTC marker must be removed (no orphans remain).
func TestJetStreamMultipleStreamsAdopted(t *testing.T) {
	t.Log("Starting standalone server, creating 3 streams: ALPHA (10 msgs + consumer), BETA (20 msgs + consumer), GAMMA (30 msgs, no consumer)")
	storeDir := t.TempDir()

	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = storeDir
	opts.ServerName = "S1"
	s := RunServer(&opts)

	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)

	streams := []struct {
		name     string
		subject  string
		msgs     int
		consumer string
	}{
		{"ALPHA", "alpha.>", 10, "alpha-dur"},
		{"BETA", "beta.>", 20, "beta-dur"},
		{"GAMMA", "gamma.>", 30, ""},
	}

	for _, st := range streams {
		req, _ := json.Marshal(&StreamConfig{
			Name:     st.name,
			Subjects: []string{st.subject},
			Storage:  FileStorage,
		})
		rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, st.name), req, 5*time.Second)
		require_NoError(t, err)
		var resp JSApiStreamCreateResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
		if resp.Error != nil {
			t.Fatalf("create stream %s: %+v", st.name, resp.Error)
		}
		for i := 0; i < st.msgs; i++ {
			_, err := nc.Request(fmt.Sprintf("%s.%d", strings.Split(st.subject, ".")[0], i), []byte("data"), 2*time.Second)
			require_NoError(t, err)
		}
		consumerSuffix := ""
		if st.consumer != "" {
			consReq, _ := json.Marshal(&CreateConsumerRequest{
				Stream: st.name,
				Config: ConsumerConfig{
					Durable:       st.consumer,
					AckPolicy:     AckExplicit,
					DeliverPolicy: DeliverAll,
				},
			})
			rmsg, err = nc.Request(fmt.Sprintf(JSApiDurableCreateT, st.name, st.consumer), consReq, 5*time.Second)
			require_NoError(t, err)
			var cResp JSApiConsumerCreateResponse
			require_NoError(t, json.Unmarshal(rmsg.Data, &cResp))
			if cResp.Error != nil {
				t.Fatalf("create consumer %s: %+v", st.consumer, cResp.Error)
			}
			consumerSuffix = fmt.Sprintf(" + consumer '%s'", st.consumer)
		}
		t.Logf("Created stream %s: %d msgs%s", st.name, st.msgs, consumerSuffix)
	}

	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()
	t.Log("Standalone server stopped, all 3 streams are orphans on disk")

	t.Log("Promoting to 3-node cluster (S-1 reuses storeDir)")
	modify := func(serverName, clusterName, autoStoreDir, conf string) string {
		if serverName == "S-1" {
			return strings.Replace(conf, autoStoreDir, storeDir, 1)
		}
		return conf
	}
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "MS", 3, modify)
	defer c.shutdown()

	c.waitOnLeader()
	t.Logf("Cluster formed, leader: %s", c.leader().Name())

	dataServer := findDataServer(c, storeDir)
	require_NotNil(t, dataServer)
	t.Logf("Data server: %s, triggering adoption for each stream", dataServer.Name())

	for _, st := range streams {
		t.Logf("Adopting stream %s …", st.name)
		triggerAndWaitForAdoption(t, c, dataServer, "$G", st.name)
		t.Logf("Stream %s adopted, leader: %s", st.name, c.streamLeader("$G", st.name).Name())
	}

	leader := c.leader()
	require_NotNil(t, leader)
	nc2, err := nats.Connect(leader.ClientURL())
	require_NoError(t, err)
	defer nc2.Close()

	for _, st := range streams {
		c.waitOnStreamLeader("$G", st.name)
		rmsg, err := nc2.Request(fmt.Sprintf(JSApiStreamInfoT, st.name), nil, 5*time.Second)
		require_NoError(t, err)
		var si JSApiStreamInfoResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &si))
		if si.Error != nil {
			t.Fatalf("stream %s info error: %+v", st.name, si.Error)
		}
		if si.State.Msgs != uint64(st.msgs) {
			t.Fatalf("stream %s: expected %d msgs, got %d", st.name, st.msgs, si.State.Msgs)
		}
		t.Logf("Stream %s: %d/%d messages verified", st.name, si.State.Msgs, st.msgs)
		if st.consumer != "" {
			c.waitOnConsumerLeader("$G", st.name, st.consumer)
			rmsg, err = nc2.Request(fmt.Sprintf(JSApiConsumerInfoT, st.name, st.consumer), nil, 5*time.Second)
			require_NoError(t, err)
			var ci JSApiConsumerInfoResponse
			require_NoError(t, json.Unmarshal(rmsg.Data, &ci))
			if ci.Error != nil {
				t.Fatalf("consumer %s/%s info error: %+v", st.name, st.consumer, ci.Error)
			}
			t.Logf("Consumer '%s' on stream %s adopted", st.consumer, st.name)
		}
	}

	sysAcc := dataServer.SystemAccount()
	require_NotNil(t, sysAcc)
	markerPath := filepath.Join(storeDir, JetStreamStoreDir, sysAcc.Name,
		defaultStoreDirName, defaultMetaGroupName, ptcMarkerFile)

	t.Log("All streams adopted, running second check, expecting PTC marker removal")
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		dataServer.getJetStream().checkForOrphans()
		if _, err := os.Stat(markerPath); err == nil {
			return fmt.Errorf("PTC marker still present after full adoption")
		}
		return nil
	})
	t.Logf("PTC marker removed, all 3 streams fully adopted, promotion complete")
}

// TestJetStreamMixedAdoptionAndConflict verifies that when a standalone server
// has two streams and only one of them conflicts with an existing cluster stream,
// the non-conflicting stream is adopted while the conflicting one is preserved on
// disk. The PTC marker must remain as long as the conflict is unresolved.
func TestJetStreamMixedAdoptionAndConflict(t *testing.T) {
	t.Log("Starting standalone server, creating UNIQUE (5 msgs) and SHARED (5 msgs)")
	t.Log("UNIQUE will be adopted cleanly; SHARED will conflict with a pre-existing cluster stream")
	storeDir := t.TempDir()

	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = storeDir
	opts.ServerName = "S1"
	s := RunServer(&opts)

	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)

	for _, cfg := range []struct{ name, subj string }{
		{"UNIQUE", "unique.>"},
		{"SHARED", "shared.>"},
	} {
		req, _ := json.Marshal(&StreamConfig{Name: cfg.name, Subjects: []string{cfg.subj}, Storage: FileStorage})
		rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.name), req, 5*time.Second)
		require_NoError(t, err)
		var resp JSApiStreamCreateResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
		if resp.Error != nil {
			t.Fatalf("create stream %s: %+v", cfg.name, resp.Error)
		}
	}
	for i := 0; i < 5; i++ {
		nc.Request(fmt.Sprintf("unique.%d", i), []byte("u"), 2*time.Second)
		nc.Request(fmt.Sprintf("shared.%d", i), []byte("s"), 2*time.Second)
	}
	t.Log("Published 5 messages to each stream, shutting down standalone")
	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()

	t.Log("Starting fresh 3-node cluster, creating SHARED on cluster-shared.> with 15 messages")
	t.Log("This cluster SHARED will conflict with the standalone SHARED when promoted")
	c := createJetStreamCluster(t, jsClusterTempl, "MX", _EMPTY_, 3, 22_034, true)
	defer c.shutdown()
	c.waitOnLeader()
	t.Logf("Cluster leader: %s", c.leader().Name())

	nc2, err := nats.Connect(c.leader().ClientURL())
	require_NoError(t, err)
	req, _ := json.Marshal(&StreamConfig{Name: "SHARED", Subjects: []string{"cluster-shared.>"}, Storage: FileStorage, Replicas: 3})
	rmsg, err := nc2.Request(fmt.Sprintf(JSApiStreamCreateT, "SHARED"), req, 5*time.Second)
	require_NoError(t, err)
	var clusterResp JSApiStreamCreateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &clusterResp))
	if clusterResp.Error != nil {
		t.Fatalf("create cluster SHARED: %+v", clusterResp.Error)
	}
	for i := 0; i < 15; i++ {
		nc2.Request(fmt.Sprintf("cluster-shared.%d", i), []byte("cluster"), 2*time.Second)
	}
	t.Log("Cluster SHARED has 15 messages on cluster-shared.>")
	nc2.Close()

	// addInNewServer() adds a 4th node with a temp dir. We swap its storeDir so it
	// joins carrying the standalone data (UNIQUE + SHARED on disk).
	// Cluster after this block: S-1, S-2, S-3 (original) + S-4 (standalone-promoted, 4th node).
	t.Log("Adding a 4th server carrying the standalone storeDir to the existing 3-node cluster")
	srvNew := c.addInNewServer()
	newOpts := srvNew.getOpts().Clone()
	t.Logf("Temp 4th server %s started (temp dir), stopping to swap storeDir", srvNew.Name())
	srvNew.Shutdown()
	srvNew.WaitForShutdown()
	c.servers = c.servers[:len(c.servers)-1]
	c.opts = c.opts[:len(c.opts)-1]

	newOpts.StoreDir = storeDir
	newOpts.Port = -1
	srvNew = RunServer(newOpts)
	c.servers = append(c.servers, srvNew)
	c.opts = append(c.opts, newOpts)
	c.checkClusterFormed()

	serverNames := make([]string, len(c.servers))
	for i, srv := range c.servers {
		role := "follower"
		if srv.JetStreamIsLeader() {
			role = "META-LEADER"
		}
		serverNames[i] = fmt.Sprintf("%s(%s)", srv.Name(), role)
	}
	t.Logf("Cluster now has %d members: %v", len(c.servers), serverNames)
	t.Logf("%s is the standalone-promoted server, UNIQUE should be adopted, SHARED name conflict should be detected", srvNew.Name())

	t.Log("Triggering adoption, UNIQUE orphan should be proposed, SHARED conflict should be detected")
	triggerAndWaitForAdoption(t, c, srvNew, "$G", "UNIQUE")
	t.Logf("UNIQUE adopted, leader: %s", c.streamLeader("$G", "UNIQUE").Name())

	checkFor(t, 10*time.Second, 300*time.Millisecond, func() error {
		if c.leader() == nil {
			return fmt.Errorf("no meta leader")
		}
		srvNew.getJetStream().checkForOrphans()
		return nil
	})
	time.Sleep(300 * time.Millisecond)

	nc3, err := nats.Connect(c.leader().ClientURL())
	require_NoError(t, err)
	defer nc3.Close()

	c.waitOnStreamLeader("$G", "UNIQUE")
	rmsg, err = nc3.Request(fmt.Sprintf(JSApiStreamInfoT, "UNIQUE"), nil, 5*time.Second)
	require_NoError(t, err)
	var uniqueResp JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &uniqueResp))
	if uniqueResp.Error != nil {
		t.Fatalf("UNIQUE stream info error: %+v", uniqueResp.Error)
	}
	if uniqueResp.State.Msgs != 5 {
		t.Fatalf("UNIQUE: expected 5 msgs, got %d", uniqueResp.State.Msgs)
	}
	t.Logf("UNIQUE: %d messages visible in cluster, adoption succeeded", uniqueResp.State.Msgs)

	rmsg, err = nc3.Request(fmt.Sprintf(JSApiStreamInfoT, "SHARED"), nil, 5*time.Second)
	require_NoError(t, err)
	var sharedResp JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &sharedResp))
	if sharedResp.Error != nil {
		t.Fatalf("SHARED stream info error: %+v", sharedResp.Error)
	}
	if sharedResp.State.Msgs != 15 {
		t.Fatalf("SHARED: expected 15 cluster msgs, got %d", sharedResp.State.Msgs)
	}
	t.Logf("SHARED: %d messages in cluster (cluster version wins), standalone 5 msgs preserved on disk", sharedResp.State.Msgs)

	sysAcc := srvNew.SystemAccount()
	require_NotNil(t, sysAcc)
	markerPath := filepath.Join(storeDir, JetStreamStoreDir, sysAcc.Name,
		defaultStoreDirName, defaultMetaGroupName, ptcMarkerFile)
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("PTC marker must remain while SHARED conflict is unresolved: %v", err)
	}
	t.Logf("PTC marker present, SHARED name conflict is still unresolved, marker correctly kept")

	sharedDir := filepath.Join(storeDir, JetStreamStoreDir, "$G", "streams", "SHARED")
	if _, err := os.Stat(sharedDir); err != nil {
		t.Fatalf("standalone SHARED data must be preserved on disk: %v", err)
	}
	t.Logf("Standalone SHARED data still on disk at %s, operator can recover it", sharedDir)
}

// TestJetStreamEphemeralConsumerNotAdopted verifies that when a standalone server
// is promoted to cluster, ephemeral (non-durable) consumers are NOT adopted.
// only durable consumers survive the promotion. Ephemerals are transient by design.
func TestJetStreamEphemeralConsumerNotAdopted(t *testing.T) {
	t.Log("Starting standalone server, creating stream EPHTEST with a durable consumer 'keeper' and an ephemeral consumer")
	t.Log("Durable 'keeper' MUST be adopted; ephemeral MUST NOT (ephemerals are transient by design)")
	storeDir := t.TempDir()

	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = storeDir
	opts.ServerName = "S1"
	s := RunServer(&opts)

	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)
	js, err := nc.JetStream()
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "EPHTEST",
		Subjects: []string{"eph.>"},
		Storage:  nats.FileStorage,
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := js.Publish(fmt.Sprintf("eph.%d", i), []byte("data"))
		require_NoError(t, err)
	}
	t.Log("Published 10 messages to EPHTEST")

	_, err = js.AddConsumer("EPHTEST", &nats.ConsumerConfig{
		Durable:       "keeper",
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
	})
	require_NoError(t, err)
	t.Log("Created durable consumer 'keeper'")

	deliverSubj := nats.NewInbox()
	_, err = nc.SubscribeSync(deliverSubj)
	require_NoError(t, err)
	ephCfg := &nats.ConsumerConfig{
		DeliverSubject: deliverSubj,
		AckPolicy:      nats.AckNonePolicy,
		DeliverPolicy:  nats.DeliverAllPolicy,
	}
	ephInfo, err := js.AddConsumer("EPHTEST", ephCfg)
	require_NoError(t, err)
	ephName := ephInfo.Name
	t.Logf("Created ephemeral consumer (name: %s), shutting down standalone", ephName)

	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()

	t.Log("Promoting to 3-node cluster (S-1 reuses storeDir)")
	modify := func(serverName, clusterName, autoStoreDir, conf string) string {
		if serverName == "S-1" {
			return strings.Replace(conf, autoStoreDir, storeDir, 1)
		}
		return conf
	}
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "EP", 3, modify)
	defer c.shutdown()

	c.waitOnLeader()
	t.Logf("Cluster formed, leader: %s", c.leader().Name())

	dataServer := findDataServer(c, storeDir)
	require_NotNil(t, dataServer)
	t.Logf("Data server: %s, triggering orphan adoption", dataServer.Name())

	triggerAndWaitForAdoption(t, c, dataServer, "$G", "EPHTEST")
	c.waitOnStreamLeader("$G", "EPHTEST")
	t.Logf("Stream EPHTEST adopted, leader: %s", c.streamLeader("$G", "EPHTEST").Name())

	nc2, err := nats.Connect(c.streamLeader("$G", "EPHTEST").ClientURL())
	require_NoError(t, err)
	defer nc2.Close()

	c.waitOnConsumerLeader("$G", "EPHTEST", "keeper")
	rmsg, err := nc2.Request(fmt.Sprintf(JSApiConsumerInfoT, "EPHTEST", "keeper"), nil, 5*time.Second)
	require_NoError(t, err)
	var keeperInfo JSApiConsumerInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &keeperInfo))
	if keeperInfo.Error != nil {
		t.Fatalf("durable 'keeper' must be adopted, got error: %+v", keeperInfo.Error)
	}
	t.Log("Durable consumer 'keeper' adopted, as expected")

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiConsumerInfoT, "EPHTEST", ephName), nil, 5*time.Second)
	require_NoError(t, err)
	var ephInfo2 JSApiConsumerInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &ephInfo2))
	if ephInfo2.Error == nil {
		t.Fatalf("ephemeral consumer %q must NOT be adopted, but it was found in the cluster", ephName)
	}
	t.Logf("Ephemeral consumer %q NOT adopted, correctly filtered (no Durable field)", ephName)
}

// TestJetStreamConflictResolvedAfterRename tests the full operator workflow for
// resolving a name conflict:
//  1. Standalone has stream CONFLICT.
//  2. Joins cluster that already has CONFLICT to conflict detected, data preserved.
//  3. Server is stopped. Operator renames CONFLICT to CONFLICT_OLD on disk.
//  4. Server restarts to CONFLICT_OLD is an orphan and gets adopted.
//  5. Cluster's CONFLICT stream is intact, PTC marker is removed.
func TestJetStreamConflictResolvedAfterRename(t *testing.T) {
	t.Log("Starting standalone server, creating stream CONFLICT (standalone.>, 7 messages)")
	storeDir := t.TempDir()

	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = storeDir
	opts.ServerName = "S1"
	s := RunServer(&opts)

	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)

	req, _ := json.Marshal(&StreamConfig{Name: "CONFLICT", Subjects: []string{"standalone.>"}, Storage: FileStorage})
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, "CONFLICT"), req, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiStreamCreateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	if resp.Error != nil {
		t.Fatalf("create standalone CONFLICT: %+v", resp.Error)
	}
	for i := 0; i < 7; i++ {
		nc.Request(fmt.Sprintf("standalone.%d", i), []byte("standalone"), 2*time.Second)
	}
	t.Log("Created standalone CONFLICT with 7 messages, shutting down")
	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()

	t.Log("Starting fresh 3-node cluster, creating CONFLICT on cluster.> with 25 messages")
	c := createJetStreamCluster(t, jsClusterTempl, "CR", _EMPTY_, 3, 22_035, true)
	defer c.shutdown()
	c.waitOnLeader()
	t.Logf("Cluster leader: %s", c.leader().Name())

	nc2, err := nats.Connect(c.leader().ClientURL())
	require_NoError(t, err)
	clusterReq, _ := json.Marshal(&StreamConfig{Name: "CONFLICT", Subjects: []string{"cluster.>"}, Storage: FileStorage, Replicas: 3})
	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamCreateT, "CONFLICT"), clusterReq, 5*time.Second)
	require_NoError(t, err)
	var clusterResp JSApiStreamCreateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &clusterResp))
	if clusterResp.Error != nil {
		t.Fatalf("create cluster CONFLICT: %+v", clusterResp.Error)
	}
	for i := 0; i < 25; i++ {
		nc2.Request(fmt.Sprintf("cluster.%d", i), []byte("cluster"), 2*time.Second)
	}
	t.Log("Cluster CONFLICT has 25 messages on cluster.>")
	nc2.Close()

	// After this block the cluster has 4 members: S-1, S-2, S-3 (original) + S-4 (standalone).
	t.Log("Adding a 4th server to the cluster, it will carry the standalone storeDir")
	t.Log("Steps: addInNewServer (temp dir) to shutdown to restart with storeDir=standalone storeDir")
	srvNew := c.addInNewServer()
	newOpts := srvNew.getOpts().Clone()
	t.Logf("Temp 4th server started as %s (temp storeDir), stopping it now", srvNew.Name())
	srvNew.Shutdown()
	srvNew.WaitForShutdown()
	c.servers = c.servers[:len(c.servers)-1]
	c.opts = c.opts[:len(c.opts)-1]

	newOpts.StoreDir = storeDir
	newOpts.Port = -1
	srvNew = RunServer(newOpts)
	c.servers = append(c.servers, srvNew)
	c.opts = append(c.opts, newOpts)
	c.checkClusterFormed()

	// Log the full cluster membership so the reader can see all 4 nodes.
	serverNames := make([]string, len(c.servers))
	for i, srv := range c.servers {
		role := "follower"
		if srv.JetStreamIsLeader() {
			role = "META-LEADER"
		}
		serverNames[i] = fmt.Sprintf("%s(%s)", srv.Name(), role)
	}
	t.Logf("Cluster now has %d members: %v", len(c.servers), serverNames)
	t.Logf("%s is the standalone-promoted server (storeDir=%s)", srvNew.Name(), storeDir)
	t.Log("Server will receive the cluster's CONFLICT assignment, detect a local copy with the same name, and preserve it on disk instead of overwriting")

	checkFor(t, 15*time.Second, 300*time.Millisecond, func() error {
		if c.leader() == nil {
			return fmt.Errorf("no meta leader")
		}
		srvNew.getJetStream().checkForOrphans()
		return nil
	})
	time.Sleep(300 * time.Millisecond)

	sysAcc := srvNew.SystemAccount()
	require_NotNil(t, sysAcc)
	markerPath := filepath.Join(storeDir, JetStreamStoreDir, sysAcc.Name,
		defaultStoreDirName, defaultMetaGroupName, ptcMarkerFile)

	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("PTC marker must be present when conflict unresolved: %v", err)
	}
	t.Logf("PTC marker present, name conflict still unresolved, marker correctly kept")

	t.Log("Stopping promoted server, operator renames CONFLICT to CONFLICT_OLD on disk to resolve the conflict")
	srvNew.Shutdown()
	srvNew.WaitForShutdown()
	c.servers = c.servers[:len(c.servers)-1]
	c.opts = c.opts[:len(c.opts)-1]

	renameStreamOnDisk(t, storeDir, "$G", "CONFLICT", "CONFLICT_OLD")
	t.Log("On-disk rename done: CONFLICT_OLD now holds the 7 standalone messages")

	t.Log("Restarting promoted server, CONFLICT_OLD is now an orphan (no conflict), should be adopted")
	srvNew = RunServer(newOpts)
	c.servers = append(c.servers, srvNew)
	c.opts = append(c.opts, newOpts)
	c.checkClusterFormed()
	t.Logf("Restarted server: %s, PTC marker still on disk, CONFLICT directory is gone", srvNew.Name())

	triggerAndWaitForAdoption(t, c, srvNew, "$G", "CONFLICT_OLD")
	t.Logf("CONFLICT_OLD adopted, leader: %s", c.streamLeader("$G", "CONFLICT_OLD").Name())

	nc3, err := nats.Connect(c.leader().ClientURL())
	require_NoError(t, err)
	defer nc3.Close()

	c.waitOnStreamLeader("$G", "CONFLICT_OLD")
	rmsg, err = nc3.Request(fmt.Sprintf(JSApiStreamInfoT, "CONFLICT_OLD"), nil, 5*time.Second)
	require_NoError(t, err)
	var oldResp JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &oldResp))
	if oldResp.Error != nil {
		t.Fatalf("CONFLICT_OLD stream info error: %+v", oldResp.Error)
	}
	if oldResp.State.Msgs != 7 {
		t.Fatalf("CONFLICT_OLD: expected 7 standalone msgs, got %d", oldResp.State.Msgs)
	}
	t.Logf("CONFLICT_OLD: %d messages adopted, all standalone data recovered", oldResp.State.Msgs)

	rmsg, err = nc3.Request(fmt.Sprintf(JSApiStreamInfoT, "CONFLICT"), nil, 5*time.Second)
	require_NoError(t, err)
	var clusterInfo JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &clusterInfo))
	if clusterInfo.Error != nil {
		t.Fatalf("cluster CONFLICT stream info error: %+v", clusterInfo.Error)
	}
	if clusterInfo.State.Msgs != 25 {
		t.Fatalf("cluster CONFLICT: expected 25 msgs, got %d", clusterInfo.State.Msgs)
	}
	t.Logf("Cluster CONFLICT still has %d messages, unaffected throughout", clusterInfo.State.Msgs)

	t.Log("Running final check, no conflicts or orphans remain, PTC marker must be removed")
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		srvNew.getJetStream().checkForOrphans()
		if _, err := os.Stat(markerPath); err == nil {
			return fmt.Errorf("PTC marker must be removed after conflict resolved")
		}
		return nil
	})
	t.Log("PTC marker removed, promotion fully resolved")
}

// TestJetStreamAdoptedStreamScaleUp verifies that after a standalone stream is
// adopted into the cluster as R1, the operator can scale it up to R3 via a
// stream update. Messages must remain intact and replication must work.
func TestJetStreamAdoptedStreamScaleUp(t *testing.T) {
	t.Log("Starting standalone server, creating stream SCALEME (R1 by default)")
	storeDir := t.TempDir()

	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = storeDir
	opts.ServerName = "S1"
	s := RunServer(&opts)

	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)

	req, _ := json.Marshal(&StreamConfig{
		Name:     "SCALEME",
		Subjects: []string{"scale.>"},
		Storage:  FileStorage,
	})
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, "SCALEME"), req, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiStreamCreateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	if resp.Error != nil {
		t.Fatalf("create stream: %+v", resp.Error)
	}
	numMessages := 20
	for i := 0; i < numMessages; i++ {
		_, err := nc.Request(fmt.Sprintf("scale.%d", i), []byte("data"), 2*time.Second)
		require_NoError(t, err)
	}
	t.Logf("Created stream SCALEME (R1) with %d messages, shutting down", numMessages)
	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()

	t.Log("Promoting to 3-node cluster (S-1 reuses storeDir)")
	modify := func(serverName, clusterName, autoStoreDir, conf string) string {
		if serverName == "S-1" {
			return strings.Replace(conf, autoStoreDir, storeDir, 1)
		}
		return conf
	}
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "SU", 3, modify)
	defer c.shutdown()

	c.waitOnLeader()
	t.Logf("Cluster formed, leader: %s", c.leader().Name())

	dataServer := findDataServer(c, storeDir)
	require_NotNil(t, dataServer)
	t.Logf("Data server: %s, triggering orphan adoption for SCALEME", dataServer.Name())

	triggerAndWaitForAdoption(t, c, dataServer, "$G", "SCALEME")
	c.waitOnStreamLeader("$G", "SCALEME")
	t.Logf("Stream SCALEME adopted as R1, leader: %s", c.streamLeader("$G", "SCALEME").Name())

	nc2, err := nats.Connect(c.streamLeader("$G", "SCALEME").ClientURL())
	require_NoError(t, err)
	defer nc2.Close()

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamInfoT, "SCALEME"), nil, 5*time.Second)
	require_NoError(t, err)
	var si1 JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &si1))
	if si1.Error != nil {
		t.Fatalf("stream info after adoption: %+v", si1.Error)
	}
	if si1.Config.Replicas != 1 {
		t.Fatalf("expected R1 after adoption, got R%d", si1.Config.Replicas)
	}
	if si1.State.Msgs != uint64(numMessages) {
		t.Fatalf("expected %d msgs after adoption, got %d", numMessages, si1.State.Msgs)
	}
	t.Logf("Confirmed: SCALEME is R%d with %d messages", si1.Config.Replicas, si1.State.Msgs)

	wantReplicas := 3
	t.Logf("Scaling SCALEME from R%d to R%d via stream update", si1.Config.Replicas, wantReplicas)
	updateReq, _ := json.Marshal(&StreamConfig{
		Name:     "SCALEME",
		Subjects: []string{"scale.>"},
		Storage:  FileStorage,
		Replicas: wantReplicas,
	})
	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamUpdateT, "SCALEME"), updateReq, 5*time.Second)
	require_NoError(t, err)
	var updateResp JSApiStreamUpdateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &updateResp))
	if updateResp.Error != nil {
		t.Fatalf("scale up to R%d failed: %+v", wantReplicas, updateResp.Error)
	}
	if updateResp.Config.Replicas != wantReplicas {
		t.Fatalf("update response: expected replicas=%d, got %d", wantReplicas, updateResp.Config.Replicas)
	}
	t.Logf("Stream update accepted, response shows replicas=%d, waiting for all peers to become current", updateResp.Config.Replicas)

	checkFor(t, 20*time.Second, 500*time.Millisecond, func() error {
		rmsg, err := nc2.Request(fmt.Sprintf(JSApiStreamInfoT, "SCALEME"), nil, 5*time.Second)
		if err != nil {
			return err
		}
		var si JSApiStreamInfoResponse
		if err := json.Unmarshal(rmsg.Data, &si); err != nil {
			return err
		}
		if si.Error != nil {
			return fmt.Errorf("stream info: %+v", si.Error)
		}
		if si.Config.Replicas != wantReplicas {
			return fmt.Errorf("still R%d, waiting for R%d", si.Config.Replicas, wantReplicas)
		}
		if si.Cluster == nil {
			return fmt.Errorf("no cluster info yet")
		}
		currentCount := 0
		if si.Cluster.Leader != _EMPTY_ {
			currentCount++
		}
		for _, r := range si.Cluster.Replicas {
			if r.Current {
				currentCount++
			}
		}
		if currentCount < wantReplicas {
			return fmt.Errorf("only %d/%d replicas current", currentCount, wantReplicas)
		}
		return nil
	})

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamInfoT, "SCALEME"), nil, 5*time.Second)
	require_NoError(t, err)
	var siFinal JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &siFinal))
	if siFinal.Error != nil {
		t.Fatalf("stream info after scale-up: %+v", siFinal.Error)
	}
	if siFinal.Config.Replicas != wantReplicas {
		t.Fatalf("expected replicas=%d after scale-up, got %d", wantReplicas, siFinal.Config.Replicas)
	}
	if siFinal.State.Msgs != uint64(numMessages) {
		t.Fatalf("after scale-up: expected %d msgs, got %d", numMessages, siFinal.State.Msgs)
	}
	t.Logf("All %d replicas current, scale-up complete: replicas=%d, messages=%d", wantReplicas, siFinal.Config.Replicas, siFinal.State.Msgs)

	_, err = nc2.Request("scale.new", []byte("after-scaleup"), 2*time.Second)
	require_NoError(t, err)

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamInfoT, "SCALEME"), nil, 5*time.Second)
	require_NoError(t, err)
	var siAfter JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &siAfter))
	if siAfter.Config.Replicas != wantReplicas {
		t.Fatalf("after publish: expected replicas=%d, got %d", wantReplicas, siAfter.Config.Replicas)
	}
	if siAfter.State.Msgs != uint64(numMessages+1) {
		t.Fatalf("after scale-up publish: expected %d msgs, got %d", numMessages+1, siAfter.State.Msgs)
	}
	t.Logf("New publish succeeded, SCALEME: replicas=%d, messages=%d", siAfter.Config.Replicas, siAfter.State.Msgs)
}
