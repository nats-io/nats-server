// Copyright 2022 The NATS Authors
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

//go:build !skip_js_tests && !skip_js_cluster_tests && !skip_js_cluster_tests_2
// +build !skip_js_tests,!skip_js_cluster_tests,!skip_js_cluster_tests_2

package server

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/nats-io/nats.go"
)

func BenchmarkJetStreamKV(b *testing.B) {

	const (
		verbose      = false
		kvNamePrefix = "B_"
		keyPrefix    = "K_"
		seed         = 12345
		minOps       = 1_000
	)

	runKVGet := func(b *testing.B, kvs []nats.KeyValue, keys []string) int {
		rng := rand.New(rand.NewSource(int64(seed)))
		errors := 0

		b.ResetTimer()

		for i := 1; i <= b.N; i++ {
			kv := kvs[rng.Intn(len(kvs))]
			key := keys[rng.Intn(len(keys))]
			kve, err := kv.Get(key)
			if err != nil {
				errors++
				continue
			}

			b.SetBytes(int64(len(kve.Value())))

			if verbose && i%1000 == 0 {
				b.Logf("Completed %d/%d Get ops", i, b.N)
			}
		}

		b.StopTimer()
		return errors
	}

	runKVPut := func(b *testing.B, kvs []nats.KeyValue, keys []string, valueSize int) int {
		rng := rand.New(rand.NewSource(int64(seed)))
		value := make([]byte, valueSize)
		errors := 0

		b.ResetTimer()

		for i := 1; i <= b.N; i++ {
			kv := kvs[rng.Intn(len(kvs))]
			key := keys[rng.Intn(len(keys))]
			rng.Read(value)
			_, err := kv.Put(key, value)
			if err != nil {
				errors++
				continue
			}

			b.SetBytes(int64(valueSize))

			if verbose && i%1000 == 0 {
				b.Logf("Completed %d/%d Put ops", i, b.N)
			}
		}

		b.StopTimer()
		return errors
	}

	runKVUpdate := func(b *testing.B, kvs []nats.KeyValue, keys []string, valueSize int) int {
		rng := rand.New(rand.NewSource(int64(seed)))
		value := make([]byte, valueSize)
		errors := 0

		b.ResetTimer()

		for i := 1; i <= b.N; i++ {
			kv := kvs[rng.Intn(len(kvs))]
			key := keys[rng.Intn(len(keys))]

			kve, getErr := kv.Get(key)
			if getErr != nil {
				errors++
				continue
			}

			rng.Read(value)
			_, updateErr := kv.Update(key, value, kve.Revision())
			if updateErr != nil {
				errors++
				continue
			}

			b.SetBytes(int64(valueSize))

			if verbose && i%1000 == 0 {
				b.Logf("Completed %d/%d Update ops", i, b.N)
			}
		}

		b.StopTimer()
		return errors
	}

	type WorkloadType string
	const (
		Get    WorkloadType = "GET"
		Put    WorkloadType = "PUT"
		Update WorkloadType = "CAS"
	)

	benchmarksCases := []struct {
		clusterSize int
		replicas    int
		numBuckets  int
		numKeys     int
		valueSize   int
	}{
		{1, 1, 1, 100, 100},    // 1 node, 1 bucket with 100 keys, 100B values
		{1, 1, 10, 1000, 100},  // 1 node, 10 buckets with 1000 keys, 100B values
		{3, 3, 1, 100, 100},    // 3 nodes, 1 bucket with 100 keys, 100B values
		{3, 3, 10, 1000, 100},  // 3 nodes, 10 buckets with 1000 keys, 100B values
		{3, 3, 10, 1000, 1024}, // 3 nodes, 10 buckets with 1000 keys, 1KB values
	}

	workloadCases := []WorkloadType{
		Get,
		Put,
		Update,
	}

	for _, bc := range benchmarksCases {

		bName := fmt.Sprintf(
			"N=%d,R=%d,B=%d,K=%d,ValSz=%db",
			bc.clusterSize,
			bc.replicas,
			bc.numBuckets,
			bc.numKeys,
			bc.valueSize,
		)

		b.Run(
			bName,
			func(b *testing.B) {
				for _, wc := range workloadCases {
					wName := fmt.Sprintf("%v", wc)
					b.Run(
						wName,
						func(b *testing.B) {
							// Skip short runs, benchmark gets re-executed with a larger N
							if b.N < minOps {
								b.ResetTimer()
								return
							}

							if verbose {
								b.Logf("Running %s workload %s with %d messages", wName, bName, b.N)
							}

							if verbose {
								b.Logf("Setting up %d nodes", bc.clusterSize)
							}
							var connectURL string
							if bc.clusterSize == 1 {
								s := RunBasicJetStreamServer(b)
								defer s.Shutdown()
								connectURL = s.ClientURL()
							} else {
								cl := createJetStreamClusterExplicit(b, "BENCH_KV", bc.clusterSize)
								defer cl.shutdown()
								cl.waitOnClusterReadyWithNumPeers(bc.clusterSize)
								cl.waitOnLeader()
								connectURL = cl.randomServer().ClientURL()
							}

							nc, js := jsClientConnectURL(b, connectURL)
							defer nc.Close()

							// Pre-generate all keys
							keys := make([]string, 0, bc.numKeys)
							for i := 1; i <= bc.numKeys; i++ {
								key := fmt.Sprintf("%s%d", keyPrefix, i)
								keys = append(keys, key)
							}

							// Initialize all KVs
							kvs := make([]nats.KeyValue, 0, bc.numBuckets)
							for i := 1; i <= bc.numBuckets; i++ {
								// Create bucket
								kvName := fmt.Sprintf("%s%d", kvNamePrefix, i)
								if verbose {
									b.Logf("Creating KV %s with R=%d", kvName, bc.replicas)
								}
								kvConfig := &nats.KeyValueConfig{
									Bucket:   kvName,
									Replicas: bc.replicas,
								}
								kv, err := js.CreateKeyValue(kvConfig)
								if err != nil {
									b.Fatalf("Error creating KV: %v", err)
								}
								kvs = append(kvs, kv)

								// Initialize all keys
								rng := rand.New(rand.NewSource(int64(seed * i)))
								value := make([]byte, bc.valueSize)
								for _, key := range keys {
									rng.Read(value)
									_, err := kv.Create(key, value)
									if err != nil {
										b.Fatalf("Failed to initialize %s/%s: %v", kvName, key, err)
									}
								}
							}

							// Discard time spent during setup
							// May reset again further in
							b.ResetTimer()

							var errors int

							switch wc {
							case Get:
								errors = runKVGet(b, kvs, keys)
							case Put:
								errors = runKVPut(b, kvs, keys, bc.valueSize)
							case Update:
								errors = runKVUpdate(b, kvs, keys, bc.valueSize)
							default:
								b.Fatalf("Unknown workload type: %v", wc)
							}

							// Benchmark ends here, (may have stopped earlier)
							b.StopTimer()

							b.ReportMetric(float64(errors)*100/float64(b.N), "%error")
						},
					)
				}
			},
		)
	}
}
