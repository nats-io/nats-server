// Copyright 2022-2024 The NATS Authors
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
// +build !skip_js_tests,include_js_long_tests

package server

import (
	"fmt"
	"math/rand"
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
