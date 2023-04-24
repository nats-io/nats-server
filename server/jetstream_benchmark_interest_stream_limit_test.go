// Copyright 2023 The NATS Authors
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
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func BenchmarkJetStreamInterestStreamWithLimit(b *testing.B) {

	const (
		verbose          = true
		seed             = 12345
		publishBatchSize = 100
		messageSize      = 256
		numSubjects      = 2500
		subjectPrefix    = "S"
		numPublishers    = 4
		randomData       = true
		warmupMessages   = 1
	)

	if verbose {
		b.Logf(
			"BatchSize: %d, MsgSize: %d, Subjects: %d, Publishers: %d, Random Message: %v",
			publishBatchSize,
			messageSize,
			numSubjects,
			numPublishers,
			randomData,
		)
	}

	// Benchmark parameters: sub-benchmarks are executed for every combination of the following 3 groups
	// Unless a more restrictive filter is specified, e.g.:
	// BenchmarkJetStreamInterestStreamWithLimit/.*R=3.*/Storage=Memory/unlimited

	// Parameter: Number of nodes and number of stream replicas
	clusterAndReplicasCases := []struct {
		clusterSize int
		replicas    int
	}{
		{1, 1}, // Single node, R=1
		{3, 3}, // 3-nodes cluster, R=3
	}

	// Parameter: Stream storage type
	storageTypeCases := []nats.StorageType{
		nats.MemoryStorage,
		nats.FileStorage,
	}

	// Parameter: Stream limit configuration
	limitConfigCases := map[string]func(*nats.StreamConfig){
		"unlimited": func(config *nats.StreamConfig) {
		},
		"MaxMsg=1000": func(config *nats.StreamConfig) {
			config.MaxMsgs = 100
		},
		"MaxMsg=10": func(config *nats.StreamConfig) {
			config.MaxMsgs = 10
		},
		"MaxPerSubject=10": func(config *nats.StreamConfig) {
			config.MaxMsgsPerSubject = 10
		},
		"MaxAge=1s": func(config *nats.StreamConfig) {
			config.MaxAge = 1 * time.Second
		},
		"MaxBytes=1MB": func(config *nats.StreamConfig) {
			config.MaxBytes = 1024 * 1024
		},
	}

	// Helper: Stand up in-process single node or cluster
	setupCluster := func(b *testing.B, clusterSize int) (string, func()) {
		var connectURL string
		var shutdownFunc func()

		if clusterSize == 1 {
			s := RunBasicJetStreamServer(b)
			shutdownFunc = s.Shutdown
			connectURL = s.ClientURL()
		} else {
			cl := createJetStreamClusterExplicit(b, "BENCH_PUB", clusterSize)
			shutdownFunc = cl.shutdown
			cl.waitOnClusterReadyWithNumPeers(clusterSize)
			cl.waitOnLeader()
			connectURL = cl.randomServer().ClientURL()
			//connectURL = cl.leader().ClientURL()
		}

		return connectURL, shutdownFunc
	}

	// Helper: Create the stream
	setupStream := func(b *testing.B, connectURL string, streamConfig *nats.StreamConfig) {
		// Connect
		nc, err := nats.Connect(connectURL)
		if err != nil {
			b.Fatalf("Failed to create client: %v", err)
		}
		defer nc.Close()

		jsOpts := []nats.JSOpt{}

		js, err := nc.JetStream(jsOpts...)
		if err != nil {
			b.Fatalf("Unexpected error getting JetStream context: %v", err)
		}

		if _, err := js.AddStream(streamConfig); err != nil {
			b.Fatalf("Error creating stream: %v", err)
		}
	}

	// Context shared by publishers routines
	type PublishersContext = struct {
		readyWg      sync.WaitGroup
		completedWg  sync.WaitGroup
		messagesLeft int
		lock         sync.Mutex
		errors       int
	}

	// Helper: Publish synchronously as Goroutine
	publish := func(publisherId int, ctx *PublishersContext, js nats.JetStreamContext) {
		defer ctx.completedWg.Done()
		errors := 0
		messageBuf := make([]byte, messageSize)
		rng := rand.New(rand.NewSource(int64(seed + publisherId)))

		// Warm up: publish a few messages
		for i := 0; i < warmupMessages; i++ {
			subject := fmt.Sprintf("%s.%d", subjectPrefix, rng.Intn(numSubjects))
			if randomData {
				rng.Read(messageBuf)
			}
			_, err := js.Publish(subject, messageBuf)
			if err != nil {
				b.Logf("Warning: failed to publish warmup message: %s", err)
			}
		}

		// Signal this publisher is ready
		ctx.readyWg.Done()

		for {
			// Obtain a batch of messages to publish
			batchSize := 0
			{
				ctx.lock.Lock()
				if ctx.messagesLeft >= publishBatchSize {
					batchSize = publishBatchSize
				} else if ctx.messagesLeft < publishBatchSize {
					batchSize = ctx.messagesLeft
				}
				ctx.messagesLeft -= batchSize
				ctx.lock.Unlock()
			}

			// Nothing left to publish, terminate
			if batchSize == 0 {
				ctx.lock.Lock()
				ctx.errors += errors
				ctx.lock.Unlock()
				return
			}

			// Publish a batch of messages
			for i := 0; i < batchSize; i++ {
				subject := fmt.Sprintf("%s.%d", subjectPrefix, rng.Intn(numSubjects))
				if randomData {
					rng.Read(messageBuf)
				}
				_, err := js.Publish(subject, messageBuf)
				if err != nil {
					errors += 1
				}
			}
		}
	}

	// Benchmark matrix: (cluster and replicas) * (storage type) * (stream limit)
	for _, benchmarkCase := range clusterAndReplicasCases {
		b.Run(
			fmt.Sprintf(
				"N=%d,R=%d",
				benchmarkCase.clusterSize,
				benchmarkCase.replicas,
			),
			func(b *testing.B) {
				for _, storageType := range storageTypeCases {
					b.Run(
						fmt.Sprintf("Storage=%v", storageType),
						func(b *testing.B) {

							for limitDescription, limitConfigFunc := range limitConfigCases {
								b.Run(
									limitDescription,
									func(b *testing.B) {
										// Stop timer during setup
										b.StopTimer()
										b.ResetTimer()

										// Set per-iteration bytes to calculate throughput (a.k.a. speed)
										b.SetBytes(messageSize)

										// Print benchmark parameters
										if verbose {
											b.Logf(
												"Stream: %+v, Storage: [%v] Limit: [%s], Ops: %d",
												benchmarkCase,
												storageType,
												limitDescription,
												b.N,
											)
										}

										// Setup server or cluster
										connectURL, shutdownFunc := setupCluster(b, benchmarkCase.clusterSize)
										defer shutdownFunc()

										// Common stream configuration
										streamConfig := &nats.StreamConfig{
											Name:      "S",
											Subjects:  []string{fmt.Sprintf("%s.>", subjectPrefix)},
											Replicas:  benchmarkCase.replicas,
											Storage:   storageType,
											Discard:   DiscardOld,
											Retention: DiscardOld,
										}
										// Configure stream limit
										limitConfigFunc(streamConfig)
										// Create stream
										setupStream(b, connectURL, streamConfig)

										// Set up publishers shared context
										var pubCtx PublishersContext
										pubCtx.readyWg.Add(numPublishers)
										pubCtx.completedWg.Add(numPublishers)

										// Hold this lock until all publishers are ready
										pubCtx.lock.Lock()
										pubCtx.messagesLeft = b.N

										// Spawn publishers routines, each with its own connection and JS context
										for i := 0; i < numPublishers; i++ {
											nc, err := nats.Connect(connectURL)
											if err != nil {
												b.Fatal(err)
											}
											defer nc.Close()
											js, err := nc.JetStream()
											if err != nil {
												b.Fatal(err)
											}
											go publish(i, &pubCtx, js)
										}

										// Wait for all publishers to be ready
										pubCtx.readyWg.Wait()

										// Benchmark starts here
										b.StartTimer()

										// Unblock the publishers
										pubCtx.lock.Unlock()

										// Wait for all publishers to complete
										pubCtx.completedWg.Wait()

										// Benchmark ends here
										b.StopTimer()

										// Sanity check, publishers may have died before completing
										if pubCtx.messagesLeft != 0 {
											b.Fatalf("Some messages left: %d", pubCtx.messagesLeft)
										}

										b.ReportMetric(float64(pubCtx.errors)*100/float64(b.N), "%error")
									},
								)
							}
						},
					)
				}
			},
		)
	}
}
