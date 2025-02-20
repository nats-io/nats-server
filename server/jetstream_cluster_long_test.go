// Copyright 2022-2025 The NATS Authors
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

// This test file is skipped by default to avoid accidentally running (e.g. `go test ./server`)
//go:build !skip_js_tests && include_js_long_tests

package server

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// TestLongKVPutWithServerRestarts overwrites values in a replicated KV bucket for a fixed amount of time.
// Also restarts a random server at fixed interval.
// The test fails if updates fail for a continuous interval of time, or if a server fails to restart and catch up.
func TestLongKVPutWithServerRestarts(t *testing.T) {
	// RNG Seed
	const Seed = 123456
	// Number of keys in bucket
	const NumKeys = 1000
	// Size of (random) values
	const ValueSize = 1024
	// Test duration
	const Duration = 3 * time.Minute
	// If no updates successful for this interval, then fail the test
	const MaxRetry = 5 * time.Second
	// Minimum time between put operations
	const UpdatesInterval = 1 * time.Millisecond
	// Time between progress reports to console
	const ProgressInterval = 10 * time.Second
	// Time between server restarts
	const ServerRestartInterval = 5 * time.Second

	type Parameters struct {
		numServers int
		replicas   int
		storage    nats.StorageType
	}

	test := func(t *testing.T, p Parameters) {
		rng := rand.New(rand.NewSource(Seed))

		// Create cluster
		clusterName := fmt.Sprintf("C_%d-%s", p.numServers, p.storage)
		cluster := createJetStreamClusterExplicit(t, clusterName, p.numServers)
		defer cluster.shutdown()

		// Connect to a random server but client will discover others too.
		nc, js := jsClientConnect(t, cluster.randomServer())
		defer nc.Close()

		// Create bucket
		kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:   "TEST",
			Replicas: p.replicas,
			Storage:  p.storage,
		})
		require_NoError(t, err)

		// Initialize list of keys
		keys := make([]string, NumKeys)
		for i := 0; i < NumKeys; i++ {
			keys[i] = fmt.Sprintf("key-%d", i)
		}

		// Initialize keys in bucket with an empty value
		for _, key := range keys {
			_, err := kv.Put(key, []byte{})
			require_NoError(t, err)
		}

		// Track statistics
		var stats = struct {
			start                time.Time
			lastSuccessfulUpdate time.Time
			updateOk             uint64
			updateErr            uint64
			restarts             uint64
			restartsMap          map[string]int
		}{
			start:                time.Now(),
			lastSuccessfulUpdate: time.Now(),
			restartsMap:          make(map[string]int, p.numServers),
		}
		for _, server := range cluster.servers {
			stats.restartsMap[server.Name()] = 0
		}

		// Print statistics
		printProgress := func() {

			t.Logf(
				"[%s] %d updates %d errors, %d server restarts (%v)",
				time.Since(stats.start).Round(time.Second),
				stats.updateOk,
				stats.updateErr,
				stats.restarts,
				stats.restartsMap,
			)
		}

		// Print update on completion
		defer printProgress()

		// Pick a random key and update it with a random value
		valueBuffer := make([]byte, ValueSize)
		updateRandomKey := func() error {
			key := keys[rand.Intn(NumKeys)]
			_, err := rng.Read(valueBuffer)
			require_NoError(t, err)
			_, err = kv.Put(key, valueBuffer)
			return err
		}

		// Set up timers and tickers
		endTestTimer := time.After(Duration)
		nextUpdateTicker := time.NewTicker(UpdatesInterval)
		progressTicker := time.NewTicker(ProgressInterval)
		restartServerTicker := time.NewTicker(ServerRestartInterval)

	runLoop:
		for {
			select {

			case <-endTestTimer:
				break runLoop

			case <-nextUpdateTicker.C:
				err := updateRandomKey()
				if err == nil {
					stats.updateOk++
					stats.lastSuccessfulUpdate = time.Now()
				} else {
					stats.updateErr++
					if time.Since(stats.lastSuccessfulUpdate) > MaxRetry {
						t.Fatalf("Could not successfully update for over %s", MaxRetry)
					}
				}

			case <-restartServerTicker.C:
				randomServer := cluster.servers[rng.Intn(len(cluster.servers))]
				randomServer.Shutdown()
				randomServer.WaitForShutdown()
				restartedServer := cluster.restartServer(randomServer)
				cluster.waitOnClusterReady()
				cluster.waitOnServerHealthz(restartedServer)
				cluster.waitOnAllCurrent()
				stats.restarts++
				stats.restartsMap[randomServer.Name()]++

			case <-progressTicker.C:
				printProgress()
			}
		}
	}

	testCases := []Parameters{
		{numServers: 5, replicas: 3, storage: nats.MemoryStorage},
		{numServers: 5, replicas: 3, storage: nats.FileStorage},
	}

	for _, testCase := range testCases {
		name := fmt.Sprintf("N:%d,R:%d,%s", testCase.numServers, testCase.replicas, testCase.storage)
		t.Run(
			name,
			func(t *testing.T) { test(t, testCase) },
		)
	}
}

// This is a RaftChainOfBlocks test that randomly starts and stops nodes to exercise recovery and snapshots.
func TestLongNRGChainOfBlocks(t *testing.T) {
	const (
		ClusterSize        = 3
		GroupSize          = 3
		ConvergenceTimeout = 30 * time.Second
		Duration           = 10 * time.Minute
		PrintStateInterval = 3 * time.Second
	)

	// Create cluster
	c := createJetStreamClusterExplicit(t, "Test", ClusterSize)
	defer c.shutdown()

	rg := c.createRaftGroup("ChainOfBlocks", GroupSize, newRaftChainStateMachine)
	rg.waitOnLeader()

	// Available operations
	type TestOperation string
	const (
		StopOne       TestOperation = "Stop one active node"
		StopAll                     = "Stop all active nodes"
		RestartOne                  = "Restart one stopped node"
		RestartAll                  = "Restart all stopped nodes"
		Snapshot                    = "Snapshot one active node"
		Propose                     = "Propose a value via one active node"
		ProposeLeader               = "Propose a value via leader"
		Pause                       = "Let things run undisturbed for a while"
		Check                       = "Wait for nodes to converge"
	)

	// Weighted distribution of operations, one is randomly chosen from this vector in each iteration
	opsWeighted := []TestOperation{
		StopOne,
		StopOne,
		StopOne,
		StopAll,
		StopAll,
		RestartOne,
		RestartOne,
		RestartOne,
		RestartAll,
		RestartAll,
		RestartAll,
		Snapshot,
		Snapshot,
		Propose,
		Propose,
		Propose,
		Propose,
		ProposeLeader,
		ProposeLeader,
		ProposeLeader,
		ProposeLeader,
		Pause,
		Pause,
		Pause,
		Pause,
		Check,
		Check,
		Check,
		Check,
	}

	rngSeed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(rngSeed))
	t.Logf("Seed: %d", rngSeed)

	// Chose a node from the list (and remove it)
	pickRandomNode := func(nodes []stateMachine) ([]stateMachine, stateMachine) {
		if len(nodes) == 0 {
			// Input list is empty
			return nodes, nil
		}
		// Pick random node
		i := rng.Intn(len(nodes))
		node := nodes[i]
		// Move last element in its place
		nodes[i] = nodes[len(nodes)-1]
		// Return slice excluding last element
		return nodes[:len(nodes)-1], node
	}

	// Create summary status string for all replicas
	chainStatusString := func() string {
		b := strings.Builder{}
		for _, sm := range rg {
			csm := sm.(*RCOBStateMachine)
			running, blocksCount, blockHash := csm.getCurrentHash()
			if running {
				b.WriteString(
					fmt.Sprintf(
						" [%s (%s): %d blocks, hash=%s],",
						csm.server().Name(),
						csm.node().ID(),
						blocksCount,
						blockHash,
					),
				)
			} else {
				b.WriteString(
					fmt.Sprintf(
						" [%s (%s): STOPPED],",
						csm.server().Name(),
						csm.node().ID(),
					),
				)

			}
		}
		return b.String()
	}

	// Track the highest number of blocks applied by any of the replicas
	highestBlocksCount := uint64(0)

	// Track active and stopped nodes
	activeNodes := make([]stateMachine, 0, GroupSize)
	stoppedNodes := make([]stateMachine, 0, GroupSize)

	// Initially all nodes are active
	activeNodes = append(activeNodes, rg...)

	defer func() {
		t.Logf("Final state: %s", chainStatusString())
	}()

	printStateTicker := time.NewTicker(PrintStateInterval)
	testTimer := time.NewTimer(Duration)
	start := time.Now()
	iteration := 0
	stopCooldown := 0

	for {

		iteration++
		select {
		case <-printStateTicker.C:
			t.Logf(
				"[%s] State: %s",
				time.Since(start).Round(time.Second),
				chainStatusString(),
			)
		case <-testTimer.C:
			// Test completed
			return
		default:
			// Continue
		}

		// Choose a random operation to perform in this iteration
		nextOperation := opsWeighted[rng.Intn(len(opsWeighted))]

		// If we're about to stop one or all nodes, do some sanity checks to ensure we don't
		// spam stops which would constantly interrupt the chance of making progress.
		if nextOperation == StopOne || nextOperation == StopAll {
			if stopCooldown > 0 {
				nextOperation = Propose
			} else {
				stopCooldown = 50
			}
		}
		if stopCooldown > 0 {
			stopCooldown--
		}

		if RCOBOptions.verbose {
			t.Logf("Iteration %d: %s", iteration, nextOperation)
		}

		switch nextOperation {

		case StopOne:
			// Stop an active node (if any are left active)
			var n stateMachine
			activeNodes, n = pickRandomNode(activeNodes)
			if n != nil {
				n.stop()
				stoppedNodes = append(stoppedNodes, n)
			}

		case StopAll:
			// Stop all active nodes (if any are active)
			for _, node := range activeNodes {
				node.stop()
			}
			stoppedNodes = append(stoppedNodes, activeNodes...)
			activeNodes = make([]stateMachine, 0, GroupSize)

		case RestartOne:
			// Restart a stopped node (if any are stopped)
			var n stateMachine
			stoppedNodes, n = pickRandomNode(stoppedNodes)
			if n != nil {
				n.restart()
				activeNodes = append(activeNodes, n)
			}

		case RestartAll:
			// Restart all stopped nodes (if any)
			for _, node := range stoppedNodes {
				node.restart()
			}
			activeNodes = append(activeNodes, stoppedNodes...)
			stoppedNodes = make([]stateMachine, 0, GroupSize)

		case Snapshot:
			// Choose a random active node and tell it to create a snapshot
			if len(activeNodes) > 0 {
				n := activeNodes[rng.Intn(len(activeNodes))]
				n.(*RCOBStateMachine).createSnapshot()
			}

		case Propose:
			// Make an active node propose the next block (if any nodes are active)
			if len(activeNodes) > 0 {
				n := activeNodes[rng.Intn(len(activeNodes))]
				n.(*RCOBStateMachine).proposeBlock()
			}

		case ProposeLeader:
			// Make the leader propose the next block (if a leader is active)
			leader := rg.leader()
			if leader != nil {
				leader.(*RCOBStateMachine).proposeBlock()
			}

		case Pause:
			// Noop, let things run undisturbed for a little bit
			time.Sleep(time.Duration(rng.Intn(250)) * time.Millisecond)

		case Check:
			// Restart any stopped node
			for _, node := range stoppedNodes {
				node.restart()
			}
			activeNodes = append(activeNodes, stoppedNodes...)
			stoppedNodes = make([]stateMachine, 0, GroupSize)

			// Ensure all nodes (eventually) converge
			checkFor(t, ConvergenceTimeout, 250*time.Millisecond,
				func() error {
					referenceNode := rg[0]
					// Save block count and hash of first node as reference
					_, referenceBlocksCount, referenceHash := referenceNode.(*RCOBStateMachine).getCurrentHash()

					// Compare each node against reference
					for _, n := range rg {
						sm := n.(*RCOBStateMachine)
						running, blocksCount, blockHash := sm.getCurrentHash()
						if !running {
							return fmt.Errorf(
								"node not running: %s (%s)",
								sm.server().Name(),
								sm.node().ID(),
							)
						}

						// Track the highest block delivered by any node
						if blocksCount > highestBlocksCount {
							if RCOBOptions.verbose {
								t.Logf(
									"New highest blocks count: %d (%s (%s))",
									blocksCount,
									sm.s.Name(),
									sm.n.ID(),
								)
							}
							highestBlocksCount = blocksCount
						}

						// Each replica must match the reference node

						if blocksCount != referenceBlocksCount {
							return fmt.Errorf(
								"different number of blocks %d (%s (%s) vs. %d (%s (%s))",
								blocksCount,
								sm.server().Name(),
								sm.node().ID(),
								referenceBlocksCount,
								referenceNode.server().Name(),
								referenceNode.node().ID(),
							)
						} else if blockHash != referenceHash {
							return fmt.Errorf(
								"different hash after %d blocks %s (%s (%s) vs. %s (%s (%s))",
								blocksCount,
								blockHash,
								sm.server().Name(),
								sm.node().ID(),
								referenceHash,
								referenceNode.server().Name(),
								referenceNode.node().ID(),
							)
						}
					}

					// Verify consistency check was against the highest block known
					if referenceBlocksCount < highestBlocksCount {
						return fmt.Errorf(
							"nodes converged below highest known block count: %d: %s",
							highestBlocksCount,
							chainStatusString(),
						)
					}

					// All nodes reached the same state, check passed
					return nil
				},
			)
		}
	}
}

func TestLongClusterWorkQueueMessagesNotSkipped(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	iterations := 500_000
	stream := "s1"
	subjf := "subj.>"
	consumers := map[string]string{
		"c1": "subj.c1",
		"c2": "subj.c2.*",
		"c3": "subj.c3",
	}

	sig := make(chan *nats.Msg, 900)

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       stream,
		Storage:    nats.FileStorage,
		Subjects:   []string{subjf},
		Retention:  nats.WorkQueuePolicy,
		Duplicates: time.Minute * 2,
		Replicas:   3,
		MaxAge:     time.Hour,
	})
	require_NoError(t, err)

	for name, subjf := range consumers {
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err = js.AddConsumer(stream, &nats.ConsumerConfig{
			Name:          name,
			FilterSubject: subjf,
			Replicas:      3,
			AckPolicy:     nats.AckExplicitPolicy,
			DeliverPolicy: nats.DeliverAllPolicy,
		})
		require_NoError(t, err)

		ps, err := js.PullSubscribe(subjf, "", nats.Bind(stream, name))
		require_NoError(t, err)

		go func() {
			for {
				msgs, err := ps.FetchBatch(300)
				if errors.Is(err, nats.ErrTimeout) {
					continue
				}
				if errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrSubscriptionClosed) {
					return // ... for when the test finishes
				}
				require_NoError(t, err)
				for msg := range msgs.Messages() {
					go func() {
						time.Sleep(time.Millisecond * time.Duration(rand.Int31n(100)))
						require_NoError(t, msg.Ack())
						sig <- msg
					}()
				}
			}
		}()
	}

	go func() {
		hdrs := nats.Header{}
		for i := 1; i <= iterations; i++ {
			// Pick a random consumer to hit this time (map iteration order is
			// non-deterministic, but break to do it just once).
			for _, subj := range consumers {
				hdrs.Set("Nats-Msg-Id", fmt.Sprintf("msg-%d", i))
				if strings.HasPrefix(subj, "*") {
					subj = strings.Replace(subj, "*", fmt.Sprintf("%d", i), 1)
				}
				_, err := js.PublishMsg(&nats.Msg{
					Subject: subj,
					Header:  hdrs,
				})
				require_NoError(t, err)
				break
			}
		}
	}()

	for i := 1; i <= iterations; i++ {
		if i%10000 == 0 {
			t.Logf("%d messages out of %d", i, iterations)
		}

		select {
		case <-sig:
		case <-time.After(time.Second * 2):
			si, err := js.StreamInfo(stream)
			require_NoError(t, err)
			t.Logf("Stream info: %+v", si.State)

			for name := range consumers {
				ci, err := js.ConsumerInfo(stream, name)
				require_NoError(t, err)
				t.Logf("Consumer %q info: %+v, %+v", name, ci.AckFloor, ci.Delivered)
			}

			t.Fatalf("Didn't receive message %d", i)
		}
	}
}
