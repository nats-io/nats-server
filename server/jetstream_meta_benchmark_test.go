// Copyright 2024 The NATS Authors
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

package server

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats.go"
)

func BenchmarkJetStreamCreate(b *testing.B) {

	const (
		verbose        = false
		resourcePrefix = "S"
		concurrency    = 12
	)

	// Types of resource that this benchmark creates
	type ResourceType string
	const (
		Stream      ResourceType = "Stream"
		KVBucket    ResourceType = "KVBucket"
		ObjectStore ResourceType = "ObjStore"
	)

	resourceTypeCases := []ResourceType{
		Stream,
		KVBucket,
		ObjectStore,
	}

	benchmarksCases := []struct {
		clusterSize int
		replicas    int
		storage     nats.StorageType
	}{
		{1, 1, nats.MemoryStorage},
		{3, 3, nats.MemoryStorage},
		{3, 3, nats.FileStorage},
	}

	for _, bc := range benchmarksCases {
		bName := fmt.Sprintf(
			"N=%d,R=%d,storage=%s,C=%d",
			bc.clusterSize,
			bc.replicas,
			bc.storage.String(),
			concurrency,
		)

		b.Run(
			bName,
			func(b *testing.B) {
				for _, rt := range resourceTypeCases {
					//for _, bc := range benchmarksCases {
					rName := fmt.Sprintf("resource=%s", rt)
					b.Run(
						rName,
						func(b *testing.B) {

							if verbose {
								b.Logf(
									"Creating %d %s resources in cluster with %d nodes, R=%d, %s storage",
									b.N,
									string(rt),
									bc.clusterSize,
									bc.replicas,
									bc.storage,
								)
							}

							// Setup server or cluster
							_, leaderServer, shutdown, nc, _ := startJSClusterAndConnect(b, bc.clusterSize)
							defer shutdown()
							defer nc.Close()

							// All clients connect to cluster (meta) leader for lower variability
							connectURL := leaderServer.ClientURL()

							// Wait for all clients and main routine to be ready
							wgReady := sync.WaitGroup{}
							wgReady.Add(concurrency + 1)
							// Wait for all routines to complete
							wgComplete := sync.WaitGroup{}
							wgComplete.Add(concurrency)

							// Number of operations (divided amongst clients)
							opsLeft := atomic.Int64{}
							opsLeft.Store(int64(b.N))
							totalErrors := atomic.Int64{}

							// Pre-create connections and JS contexts
							for i := 1; i <= concurrency; i++ {
								nc, js := jsClientConnectURL(b, connectURL)
								defer nc.Close()
								go func(clientId int, nc *nats.Conn, js nats.JetStreamContext) {
									defer wgComplete.Done()

									// Config struct (reused and modified in place for each call)
									streamConfig := nats.StreamConfig{
										Name:     "?",
										Storage:  bc.storage,
										Replicas: bc.replicas,
									}
									kvConfig := nats.KeyValueConfig{
										Bucket:   "?",
										Storage:  bc.storage,
										Replicas: bc.replicas,
									}
									objConfig := nats.ObjectStoreConfig{
										Bucket:   "?",
										Storage:  bc.storage,
										Replicas: bc.replicas,
									}

									// Block until everyone is ready
									wgReady.Done()
									wgReady.Wait()

									errCount := int64(0)
									defer func() {
										// Roll up error count on completion
										totalErrors.Add(errCount)
									}()

									// Track per-client opCount (just for logging/debugging)
									opCount := 0
									for opsLeft.Add(-1) >= 0 {
										var err error
										// Create unique resource name
										resourceName := fmt.Sprintf("%s_%d_%d", resourcePrefix, clientId, opCount)
										switch rt {
										case Stream:
											streamConfig.Name = resourceName
											_, err = js.AddStream(&streamConfig)
										case KVBucket:
											kvConfig.Bucket = resourceName
											_, err = js.CreateKeyValue(&kvConfig)
										case ObjectStore:
											objConfig.Bucket = resourceName
											_, err = js.CreateObjectStore(&objConfig)
										}
										opCount += 1
										if err != nil {
											b.Logf("Error creating %s (%s): %s", rt, resourceName, err)
											errCount += 1
										}
									}

									if verbose {
										b.Logf("Client %d completed %d operations", clientId, opCount)
									}

								}(i, nc, js)
							}

							// Wait for all clients to be ready
							wgReady.Done()
							wgReady.Wait()

							// Start benchmark clock
							b.ResetTimer()

							wgComplete.Wait()
							b.StopTimer()

							b.ReportMetric(float64(100*(totalErrors.Load()))/float64(b.N), "%error")
						},
					)
				}
			},
		)
	}
}

func BenchmarkJetStreamCreateConsumers(b *testing.B) {

	const (
		verbose        = false
		streamName     = "S"
		consumerPrefix = "C"
		concurrency    = 12
	)

	benchmarksCases := []struct {
		clusterSize      int
		consumerReplicas int
		consumerStorage  nats.StorageType
	}{
		{1, 1, nats.MemoryStorage},
		{3, 3, nats.MemoryStorage},
		{3, 3, nats.FileStorage},
	}

	type ConsumerType string
	const (
		Ephemeral ConsumerType = "Ephemeral"
		Durable   ConsumerType = "Durable"
	)

	consumerTypeCases := []ConsumerType{
		Ephemeral,
		Durable,
	}

	for _, bc := range benchmarksCases {

		bName := fmt.Sprintf(
			"N=%d,R=%d,storage=%s,C=%d",
			bc.clusterSize,
			bc.consumerReplicas,
			bc.consumerStorage.String(),
			concurrency,
		)

		b.Run(
			bName,
			func(b *testing.B) {

				for _, ct := range consumerTypeCases {

					cName := fmt.Sprintf("Consumer=%s", ct)

					b.Run(
						cName,
						func(b *testing.B) {
							if verbose {
								b.Logf(
									"Creating %d consumers in cluster with %d nodes, R=%d, %s storage",
									b.N,
									bc.clusterSize,
									bc.consumerReplicas,
									bc.consumerStorage,
								)
							}

							// Setup server or cluster
							_, leaderServer, shutdown, nc, js := startJSClusterAndConnect(b, bc.clusterSize)
							defer shutdown()
							defer nc.Close()

							// All clients connect to cluster (meta) leader for lower variability
							connectURL := leaderServer.ClientURL()

							// Create stream
							streamConfig := nats.StreamConfig{
								Name:     streamName,
								Storage:  nats.FileStorage,
								Replicas: bc.clusterSize,
							}

							_, err := js.AddStream(&streamConfig)
							if err != nil {
								b.Fatalf("Failed to create stream: %s", err)
							}

							// Wait for all clients and main routine to be ready
							wgReady := sync.WaitGroup{}
							wgReady.Add(concurrency + 1)
							// Wait for all routines to complete
							wgComplete := sync.WaitGroup{}
							wgComplete.Add(concurrency)

							// Number of operations (divided amongst clients)
							opsLeft := atomic.Int64{}
							opsLeft.Store(int64(b.N))
							// Total number of errors
							totalErrors := atomic.Int64{}

							// Pre-create connections and JS contexts
							for i := 1; i <= concurrency; i++ {
								nc, js := jsClientConnectURL(b, connectURL)
								defer nc.Close()

								go func(clientId int, nc *nats.Conn, js nats.JetStreamContext) {
									defer wgComplete.Done()

									// Config struct (reused and modified in place for each call)
									cfg := nats.ConsumerConfig{
										Durable:       "",
										Name:          "",
										Replicas:      bc.consumerReplicas,
										MemoryStorage: bc.consumerStorage == nats.MemoryStorage,
									}

									// Block until everyone is ready
									wgReady.Done()
									wgReady.Wait()

									errCount := int64(0)
									opCount := 0
									for opsLeft.Add(-1) >= 0 {
										var err error
										// Set unique consumer name
										cfg.Name = fmt.Sprintf("%s_%d_%d", consumerPrefix, clientId, opCount)
										if ct == Durable {
											cfg.Durable = cfg.Name
										}
										_, err = js.AddConsumer(streamName, &cfg)
										if err != nil {
											b.Logf("Failed to add consumer: %s", err)
											errCount += 1
										}
										opCount += 1
									}

									if verbose {
										b.Logf("Client %d completed %d operations", clientId, opCount)
									}

									totalErrors.Add(errCount)

								}(i, nc, js)
							}

							// Wait for all clients to be ready
							wgReady.Done()
							wgReady.Wait()

							// Start benchmark clock
							b.ResetTimer()

							wgComplete.Wait()
							b.StopTimer()

							b.ReportMetric(float64(100*(totalErrors.Load()))/float64(b.N), "%error")
						},
					)
				}
			},
		)
	}
}
