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
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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

func TestLongClusterJetStreamKeyValueSync(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	for _, s := range c.servers {
		s.optsMu.Lock()
		s.opts.LameDuckDuration = 15 * time.Second
		s.opts.LameDuckGracePeriod = -15 * time.Second
		s.optsMu.Unlock()
	}
	s := c.randomNonLeader()
	connect := func(t *testing.T) (*nats.Conn, nats.JetStreamContext) {
		return jsClientConnect(t, s)
	}

	const accountName = "$G"
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	createData := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = letterBytes[rand.Intn(len(letterBytes))]
		}
		return b
	}
	getOrCreateKvStore := func(kvname string) (nats.KeyValue, error) {
		_, js := connect(t)
		kvExists := false
		existingKvnames := js.KeyValueStoreNames()
		for existingKvname := range existingKvnames {
			if existingKvname == kvname {
				kvExists = true
				break
			}
		}
		if !kvExists {
			return js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:   kvname,
				Replicas: 3,
				Storage:  nats.FileStorage,
			})
		} else {
			return js.KeyValue(kvname)
		}
	}
	abs := func(x int64) int64 {
		if x < 0 {
			return -x
		}
		return x
	}
	var counter int64
	var errorCounter int64

	checkMsgsEqual := func(t *testing.T, accountName, streamName string) error {
		// Gather all the streams replicas and compare contents.
		msets := make(map[*Server]*stream)
		for _, s := range c.servers {
			acc, err := s.LookupAccount(accountName)
			if err != nil {
				return err
			}
			mset, err := acc.lookupStream(streamName)
			if err != nil {
				return err
			}
			msets[s] = mset
		}

		str := getStreamDetails(t, c, accountName, streamName)
		if str == nil {
			return fmt.Errorf("could not get stream leader state")
		}
		state := str.State
		for seq := state.FirstSeq; seq <= state.LastSeq; seq++ {
			var msgId string
			var smv StoreMsg
			for replica, mset := range msets {
				mset.mu.RLock()
				sm, err := mset.store.LoadMsg(seq, &smv)
				mset.mu.RUnlock()
				if err != nil {
					if err == ErrStoreMsgNotFound || err == errDeletedMsg {
						// Skip these.
					} else {
						t.Logf("WRN: Error loading message (seq=%d) from stream %q on replica %q: %v", seq, streamName, replica, err)
					}
					continue
				}
				if msgId == _EMPTY_ {
					msgId = string(sm.hdr)
				} else if msgId != string(sm.hdr) {
					t.Errorf("MsgIds do not match for seq %d on stream %q: %q vs %q", seq, streamName, msgId, sm.hdr)
				}
			}
		}
		return nil
	}

	keyUpdater := func(ctx context.Context, cancel context.CancelFunc, kvname string, numKeys int) {
		kv, err := getOrCreateKvStore(kvname)
		if err != nil {
			t.Fatalf("[%s]:%v", kvname, err)
		}
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key-%d", i)
			kv.Create(key, createData(160))
		}
		lastData := make(map[string][]byte)
		revisions := make(map[string]uint64)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			r := rand.Intn(numKeys)
			key := fmt.Sprintf("key-%d", r)

			for i := 0; i < 5; i++ {
				_, err := kv.Get(key)
				if err != nil {
					atomic.AddInt64(&errorCounter, 1)
					if err == nats.ErrKeyNotFound {
						t.Logf("WRN: Key not found! [%s/%s] - [%s]", kvname, key, err)
						cancel()
					}
				}
			}

			k, err := kv.Get(key)
			if err != nil {
				atomic.AddInt64(&errorCounter, 1)
			} else {
				if revisions[key] != 0 && abs(int64(k.Revision())-int64(revisions[key])) < 2 {
					lastDataVal, ok := lastData[key]
					if ok && k.Revision() == revisions[key] && slices.Compare(lastDataVal, k.Value()) != 0 {
						t.Logf("data loss [%s/%s][rev:%d] expected:[%v] is:[%v]", kvname, key, revisions[key], string(lastDataVal), string(k.Value()))
					}
				}
				newData := createData(160)
				revisions[key], err = kv.Update(key, newData, k.Revision())
				if err != nil && err != nats.ErrTimeout {
					atomic.AddInt64(&errorCounter, 1)
				} else {
					lastData[key] = newData
				}
				atomic.AddInt64(&counter, 1)
			}
		}
	}

	streamCount := 50
	keysCount := 100
	streamPrefix := "IKV"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// The keyUpdaters will run for less time.
	kctx, kcancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer kcancel()

	var wg sync.WaitGroup
	var streams []string
	for i := 0; i < streamCount; i++ {
		streamName := fmt.Sprintf("%s-%d", streamPrefix, i)
		streams = append(streams, "KV_"+streamName)

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			keyUpdater(kctx, cancel, streamName, keysCount)
		}(i)
	}

	debug := false
	nc2, _ := jsClientConnect(t, s)
	if debug {
		go func() {
			for range time.NewTicker(5 * time.Second).C {
				select {
				case <-ctx.Done():
					return
				default:
				}
				for _, str := range streams {
					leaderSrv := c.streamLeader(accountName, str)
					if leaderSrv == nil {
						continue
					}
					streamLeader := getStreamDetails(t, c, accountName, str)
					if streamLeader == nil {
						continue
					}
					t.Logf("|------------------------------------------------------------------------------------------------------------------------|")
					lstate := streamLeader.State
					t.Logf("| %-10s | %-10s | msgs:%-10d | bytes:%-10d | deleted:%-10d | first:%-10d | last:%-10d |",
						str, leaderSrv.String()+"*", lstate.Msgs, lstate.Bytes, lstate.NumDeleted, lstate.FirstSeq, lstate.LastSeq,
					)
					for _, srv := range c.servers {
						if srv == leaderSrv {
							continue
						}
						acc, err := srv.LookupAccount(accountName)
						if err != nil {
							continue
						}
						stream, err := acc.lookupStream(str)
						if err != nil {
							t.Logf("Error looking up stream %s on %s replica", str, srv)
							continue
						}
						state := stream.state()

						unsynced := lstate.Msgs != state.Msgs || lstate.Bytes != state.Bytes ||
							lstate.NumDeleted != state.NumDeleted || lstate.FirstSeq != state.FirstSeq || lstate.LastSeq != state.LastSeq

						var result string
						if unsynced {
							result = "UNSYNCED"
						}
						t.Logf("| %-10s | %-10s | msgs:%-10d | bytes:%-10d | deleted:%-10d | first:%-10d | last:%-10d | %s",
							str, srv, state.Msgs, state.Bytes, state.NumDeleted, state.FirstSeq, state.LastSeq, result,
						)
					}
				}
				t.Logf("|------------------------------------------------------------------------------------------------------------------------| %v", nc2.ConnectedUrl())
			}
		}()
	}

	checkStreams := func(t *testing.T) {
		for _, str := range streams {
			checkFor(t, time.Minute, 500*time.Millisecond, func() error {
				return checkState(t, c, accountName, str)
			})
			checkFor(t, time.Minute, 500*time.Millisecond, func() error {
				return checkMsgsEqual(t, accountName, str)
			})
		}
	}

Loop:
	for range time.NewTicker(30 * time.Second).C {
		select {
		case <-ctx.Done():
			break Loop
		default:
		}
		rollout := func(t *testing.T) {
			for _, s := range c.servers {
				// For graceful mode
				s.lameDuckMode()
				s.WaitForShutdown()
				s = c.restartServer(s)

				hctx, hcancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer hcancel()

			Healthz:
				for range time.NewTicker(2 * time.Second).C {
					select {
					case <-hctx.Done():
					default:
					}

					status := s.healthz(nil)
					if status.StatusCode == 200 {
						break Healthz
					}
				}
				c.waitOnClusterReady()
				checkStreams(t)
			}
		}
		rollout(t)
		checkStreams(t)
	}
	wg.Wait()
	checkStreams(t)
}

func TestLongClusterCLFSOnDuplicates(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	nc2, js2 := jsClientConnect(t, c.randomServer())
	defer nc2.Close()

	streamName := "TESTW"
	_, err := js.AddStream(&nats.StreamConfig{
		Name:       streamName,
		Subjects:   []string{"foo"},
		Replicas:   3,
		Storage:    nats.FileStorage,
		MaxAge:     3 * time.Minute,
		Duplicates: 2 * time.Minute,
	})
	require_NoError(t, err)

	c.waitOnStreamLeader(globalAccountName, streamName)

	var wg sync.WaitGroup

	// The test will be successful if it runs for this long without dup issues.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	go func() {
		tick := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			case <-tick.C:
				c.streamLeader(globalAccountName, streamName).JetStreamStepdownStream(globalAccountName, streamName)
			}
		}
	}()
	wg.Add(1)

	for i := 0; i < 5; i++ {
		go func(i int) {
			var err error
			sub, err := js2.PullSubscribe("foo", fmt.Sprintf("A:%d", i))
			require_NoError(t, err)

			for {
				select {
				case <-ctx.Done():
					wg.Done()
					return
				default:
				}

				msgs, err := sub.Fetch(100, nats.MaxWait(200*time.Millisecond))
				if err != nil {
					continue
				}
				for _, msg := range msgs {
					msg.Ack()
				}
			}
		}(i)
		wg.Add(1)
	}

	// Sync producer that only does a couple of duplicates, cancel the test
	// if we get too many errors without responses.
	errCh := make(chan error, 10)
	go func() {
		// Try sync publishes normally in this state and see if it times out.
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			default:
			}

			var succeeded bool
			var failures int
			for n := 0; n < 10; n++ {
				_, err := js.Publish(
					"foo", []byte("test"),
					nats.MsgId(fmt.Sprintf("sync:checking:%d", i)),
					nats.RetryAttempts(30),
					nats.AckWait(500*time.Millisecond),
				)
				if err != nil {
					failures++
					continue
				}
				succeeded = true
			}
			if !succeeded {
				errCh <- fmt.Errorf("Too many publishes failed with timeout: failures=%d, i=%d", failures, i)
			}
		}
	}()
	wg.Add(1)

Loop:
	for n := uint64(0); true; n++ {
		select {
		case <-ctx.Done():
			break Loop
		case e := <-errCh:
			t.Error(e)
			break Loop
		default:
		}
		// Cause a lot of duplicates very fast until producer stalls.
		for i := 0; i < 128; i++ {
			msgID := nats.MsgId(fmt.Sprintf("id.%d.%d", n, i))
			js.PublishAsync("foo", []byte("test"), msgID, nats.RetryAttempts(10))
		}
	}
	cancel()
	wg.Wait()
}

func TestLongClusterJetStreamRestartThenScaleStreamReplicas(t *testing.T) {
	t.Skip("This test takes too long, need to make shorter")

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	nc2, producer := jsClientConnect(t, s)
	defer nc2.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	end := time.Now().Add(2 * time.Second)
	for time.Now().Before(end) {
		producer.Publish("foo", []byte(strings.Repeat("A", 128)))
		time.Sleep(time.Millisecond)
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		sub, err := js.PullSubscribe("foo", fmt.Sprintf("C-%d", i))
		require_NoError(t, err)

		wg.Add(1)
		go func() {
			defer wg.Done()
			for range time.NewTicker(10 * time.Millisecond).C {
				select {
				case <-ctx.Done():
					return
				default:
				}

				msgs, err := sub.Fetch(1)
				if err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, nats.ErrConnectionClosed) {
					t.Logf("Pull Error: %v", err)
				}
				for _, msg := range msgs {
					msg.Ack()
				}
			}
		}()
	}
	c.lameDuckRestartAll()
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Swap the logger to try to detect the condition after the restart.
	loggers := make([]*captureDebugLogger, 3)
	for i, srv := range c.servers {
		l := &captureDebugLogger{dbgCh: make(chan string, 10)}
		loggers[i] = l
		srv.SetLogger(l, true, false)
	}
	condition := `Direct proposal ignored, not leader (state: CLOSED)`
	errCh := make(chan error, 10)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case dl := <-loggers[0].dbgCh:
				if strings.Contains(dl, condition) {
					errCh <- errors.New(condition)
				}
			case dl := <-loggers[1].dbgCh:
				if strings.Contains(dl, condition) {
					errCh <- errors.New(condition)
				}
			case dl := <-loggers[2].dbgCh:
				if strings.Contains(dl, condition) {
					errCh <- errors.New(condition)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start publishing again for a while.
	end = time.Now().Add(2 * time.Second)
	for time.Now().Before(end) {
		producer.Publish("foo", []byte(strings.Repeat("A", 128)))
		time.Sleep(time.Millisecond)
	}

	// Try to do a stream edit back to R=1 after doing all the upgrade.
	info, _ := js.StreamInfo("TEST")
	sconfig := info.Config
	sconfig.Replicas = 1
	_, err = js.UpdateStream(&sconfig)
	require_NoError(t, err)

	// Leave running for some time after the update.
	time.Sleep(2 * time.Second)

	info, _ = js.StreamInfo("TEST")
	sconfig = info.Config
	sconfig.Replicas = 3
	_, err = js.UpdateStream(&sconfig)
	require_NoError(t, err)

	select {
	case e := <-errCh:
		t.Fatalf("Bad condition on raft node: %v", e)
	case <-time.After(2 * time.Second):
		// Done
	}

	// Stop goroutines and wait for them to exit.
	cancel()
	wg.Wait()
}

func TestLongFileStoreEnforceMsgPerSubjectLimit(t *testing.T) {
	td := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: td, BlockSize: 1024},
		StreamConfig{
			Name: "zzz", Subjects: []string{"test.>"}, Storage: FileStorage,
		},
	)
	require_NoError(t, err)
	defer fs.Stop()

	t.Logf("Starting publishes")
	for i := 0; i < 100_000; i++ {
		_, _, err := fs.StoreMsg(fmt.Sprintf("test.%06d", i), nil, nil, 0)
		require_NoError(t, err)
	}
	// Now update some of them. Leave a bit of a mess with some big gaps.
	for i := 0; i < 5_000_000; i++ {
		n := rand.Int31n(100_000)
		if n < 5000 {
			continue
		}
		_, _, err := fs.StoreMsg(fmt.Sprintf("test.%06d", n), nil, nil, 0)
		require_NoError(t, err)
	}
	t.Logf("Publish complete")

	require_NoError(t, fs.Stop())
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: td, BlockSize: 1024},
		StreamConfig{
			Name: "zzz", Subjects: []string{"test.>"}, Storage: FileStorage,
			MaxMsgsPer: 1,
		},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// Mangle the filestore state and then see how long it takes to enforce
	// the per-subject limit.
	fs.state.Msgs++
	start := time.Now()
	fs.enforceMsgPerSubjectLimit(false)
	require_LessThan(t, time.Since(start), time.Minute)
	t.Logf("Took %s", time.Since(start))
}
