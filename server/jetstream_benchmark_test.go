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

func BenchmarkJetStreamConsume(b *testing.B) {

	const (
		verbose        = false
		streamName     = "S"
		subject        = "s"
		seed           = 12345
		publishTimeout = 30 * time.Second
	)

	runSyncPushConsumer := func(b *testing.B, js nats.JetStreamContext, streamName, subject string) (int, int, int) {
		const nextMsgTimeout = 3 * time.Second

		subOpts := []nats.SubOpt{
			nats.BindStream(streamName),
		}
		sub, err := js.SubscribeSync("", subOpts...)
		if err != nil {
			b.Fatalf("Failed to subscribe: %v", err)
		}
		defer sub.Unsubscribe()

		bitset := NewBitset(uint64(b.N))
		uniqueConsumed, duplicates, errors := 0, 0, 0

		b.ResetTimer()

		for uniqueConsumed < b.N {
			msg, err := sub.NextMsg(nextMsgTimeout)
			if err != nil {
				b.Fatalf("No more messages (received: %d/%d)", uniqueConsumed, b.N)
			}

			metadata, mdErr := msg.Metadata()
			if mdErr != nil {
				errors++
				continue
			}

			ackErr := msg.Ack()
			if ackErr != nil {
				errors++
				continue
			}

			seq := metadata.Sequence.Stream

			index := seq - 1
			if bitset.get(index) {
				duplicates++
				continue
			}

			uniqueConsumed++
			bitset.set(index, true)
			b.SetBytes(int64(len(msg.Data)))

			if verbose && uniqueConsumed%1000 == 0 {
				b.Logf("Consumed: %d/%d", bitset.count(), b.N)
			}
		}

		b.StopTimer()

		return uniqueConsumed, duplicates, errors
	}

	runAsyncPushConsumer := func(b *testing.B, js nats.JetStreamContext, streamName, subject string, ordered, durable bool) (int, int, int) {
		const timeout = 3 * time.Minute
		bitset := NewBitset(uint64(b.N))
		doneCh := make(chan bool, 1)
		uniqueConsumed, duplicates, errors := 0, 0, 0

		handleMsg := func(msg *nats.Msg) {
			metadata, mdErr := msg.Metadata()
			if mdErr != nil {
				// fmt.Printf("Metadata error: %v\n", mdErr)
				errors++
				return
			}

			// Ordered defaults to AckNone policy, don't try to ACK
			if !ordered {
				ackErr := msg.Ack()
				if ackErr != nil {
					// fmt.Printf("Ack error: %v\n", ackErr)
					errors++
					return
				}
			}

			seq := metadata.Sequence.Stream

			index := seq - 1
			if bitset.get(index) {
				duplicates++
				return
			}

			uniqueConsumed++
			bitset.set(index, true)
			b.SetBytes(int64(len(msg.Data)))

			if uniqueConsumed == b.N {
				msg.Sub.Unsubscribe()
				doneCh <- true
			}
			if verbose && uniqueConsumed%1000 == 0 {
				b.Logf("Consumed %d/%d", uniqueConsumed, b.N)
			}
		}

		subOpts := []nats.SubOpt{
			nats.BindStream(streamName),
		}

		if ordered {
			subOpts = append(subOpts, nats.OrderedConsumer())
		}

		if durable {
			subOpts = append(subOpts, nats.Durable("c"))
		}

		sub, err := js.Subscribe("", handleMsg, subOpts...)
		if err != nil {
			b.Fatalf("Failed to subscribe: %v", err)
		}
		defer sub.Unsubscribe()

		b.ResetTimer()

		select {
		case <-doneCh:
			b.StopTimer()
		case <-time.After(timeout):
			b.Fatalf("Timeout, %d/%d received, %d errors", uniqueConsumed, b.N, errors)
		}

		return uniqueConsumed, duplicates, errors
	}

	runPullConsumer := func(b *testing.B, js nats.JetStreamContext, streamName, subject string, durable bool) (int, int, int) {
		const fetchMaxWait = nats.MaxWait(3 * time.Second)
		const fetchMaxMessages = 1000

		bitset := NewBitset(uint64(b.N))
		uniqueConsumed, duplicates, errors := 0, 0, 0

		subOpts := []nats.SubOpt{
			nats.BindStream(streamName),
		}

		consumerName := "" // Default ephemeral
		if durable {
			consumerName = "c" // Durable
		}

		sub, err := js.PullSubscribe("", consumerName, subOpts...)
		if err != nil {
			b.Fatalf("Failed to subscribe: %v", err)
		}
		defer sub.Unsubscribe()

		b.ResetTimer()

	fetchLoop:
		for {
			msgs, err := sub.Fetch(fetchMaxMessages, fetchMaxWait)
			if err != nil {
				b.Fatalf("Failed to fetch: %v", err)
			}

		processMsgsLoop:
			for _, msg := range msgs {
				metadata, mdErr := msg.Metadata()
				if mdErr != nil {
					errors++
					continue processMsgsLoop
				}

				ackErr := msg.Ack()
				if ackErr != nil {
					errors++
					continue processMsgsLoop
				}

				seq := metadata.Sequence.Stream

				index := seq - 1
				if bitset.get(index) {
					duplicates++
					continue processMsgsLoop
				}

				uniqueConsumed++
				bitset.set(index, true)
				b.SetBytes(int64(len(msg.Data)))

				if uniqueConsumed == b.N {
					msg.Sub.Unsubscribe()
					break fetchLoop
				}

				if verbose && uniqueConsumed%1000 == 0 {
					b.Logf("Consumed %d/%d", uniqueConsumed, b.N)
				}
			}
		}

		b.StopTimer()

		return uniqueConsumed, duplicates, errors
	}

	type ConsumerType string
	const (
		PushSync         ConsumerType = "PUSH[Sync,Ephemeral]"
		PushAsync        ConsumerType = "PUSH[Async,Ephemeral]"
		PushAsyncOrdered ConsumerType = "PUSH[Async,Ordered]"
		PushAsyncDurable ConsumerType = "PUSH[Async,Durable]"
		PullDurable      ConsumerType = "PULL[Durable]"
		PullEphemeral    ConsumerType = "PULL[Ephemeral]"
	)

	benchmarksCases := []struct {
		clusterSize int
		replicas    int
		messageSize int
		minMessages int
	}{
		{1, 1, 10, 100_000}, // Single node, 10B messages, ~1MiB minimum
		{1, 1, 1024, 1_000}, // Single node, 1KB messages, ~1MiB minimum
		{3, 3, 10, 100_000}, // Cluster, R3, 10B messages, ~1MiB minimum
		{3, 3, 1024, 1_000}, // Cluster, R3, 1KB messages, ~1MiB minimum
	}

	//Each of the cases above is run with each of the consumer types
	consumerTypes := []ConsumerType{
		PushSync,
		PushAsync,
		PushAsyncOrdered,
		PushAsyncDurable,
		PullDurable,
		PullEphemeral,
	}

	for _, bc := range benchmarksCases {

		name := fmt.Sprintf(
			"N=%d,R=%d,MsgSz=%db",
			bc.clusterSize,
			bc.replicas,
			bc.messageSize,
		)

		b.Run(
			name,
			func(b *testing.B) {

				for _, ct := range consumerTypes {
					name := fmt.Sprintf(
						"%v",
						ct,
					)
					b.Run(
						name,
						func(b *testing.B) {
							// Skip short runs, benchmark gets re-executed with a larger N
							if b.N < bc.minMessages {
								b.ResetTimer()
								return
							}

							if verbose {
								b.Logf("Running %s with %d messages", name, b.N)
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
								cl := createJetStreamClusterExplicit(b, "BENCH_PUB", bc.clusterSize)
								defer cl.shutdown()
								cl.waitOnClusterReadyWithNumPeers(bc.clusterSize)
								cl.waitOnLeader()
								connectURL = cl.randomServer().ClientURL()
							}

							nc, js := jsClientConnectURL(b, connectURL)
							defer nc.Close()

							if verbose {
								b.Logf("Creating stream with R=%d", bc.replicas)
							}
							streamConfig := &nats.StreamConfig{
								Name:     streamName,
								Subjects: []string{subject},
								Replicas: bc.replicas,
							}
							if _, err := js.AddStream(streamConfig); err != nil {
								b.Fatalf("Error creating stream: %v", err)
							}

							rng := rand.New(rand.NewSource(int64(seed)))
							message := make([]byte, bc.messageSize)
							publishedCount := 0
							for publishedCount < b.N {
								rng.Read(message)
								_, err := js.PublishAsync(subject, message)
								if err != nil {
									continue
								} else {
									publishedCount++
								}
							}

							select {
							case <-js.PublishAsyncComplete():
								if verbose {
									b.Logf("Published %d messages", b.N)
								}
							case <-time.After(publishTimeout):
								b.Fatalf("Publish timed out")
							}

							// Discard time spent during setup
							// Consumer may reset again further in
							b.ResetTimer()

							var consumed, duplicates, errors int

							const (
								ordered   = true
								unordered = false
								durable   = true
								ephemeral = false
							)

							switch ct {
							case PushSync:
								consumed, duplicates, errors = runSyncPushConsumer(b, js, streamName, subject)
							case PushAsync:
								consumed, duplicates, errors = runAsyncPushConsumer(b, js, streamName, subject, unordered, ephemeral)
							case PushAsyncOrdered:
								consumed, duplicates, errors = runAsyncPushConsumer(b, js, streamName, subject, ordered, ephemeral)
							case PushAsyncDurable:
								consumed, duplicates, errors = runAsyncPushConsumer(b, js, streamName, subject, unordered, durable)
							case PullDurable:
								consumed, duplicates, errors = runPullConsumer(b, js, streamName, subject, durable)
							case PullEphemeral:
								consumed, duplicates, errors = runPullConsumer(b, js, streamName, subject, ephemeral)
							default:
								b.Fatalf("Unknown consumer type: %v", ct)
							}

							// Benchmark ends here, (consumer may have stopped earlier)
							b.StopTimer()

							if consumed != b.N {
								b.Fatalf("Something doesn't add up: %d != %d", consumed, b.N)
							}

							b.ReportMetric(float64(duplicates)*100/float64(b.N), "%dupe")
							b.ReportMetric(float64(errors)*100/float64(b.N), "%error")
						},
					)
				}
			},
		)
	}
}

func BenchmarkJetStreamPublish(b *testing.B) {

	const (
		verbose = false
		seed    = 12345
	)

	runSyncPublisher := func(b *testing.B, js nats.JetStreamContext, messageSize int, subjects []string) (int, int) {
		published, errors := 0, 0
		rng := rand.New(rand.NewSource(int64(seed)))
		message := make([]byte, messageSize)

		b.ResetTimer()

		for i := 1; i <= b.N; i++ {
			rng.Read(message) // TODO may skip this?
			subject := subjects[rng.Intn(len(subjects))]
			_, pubErr := js.Publish(subject, message)
			if pubErr != nil {
				errors++
			} else {
				published++
				b.SetBytes(int64(messageSize))
			}

			if verbose && i%1000 == 0 {
				b.Logf("Published %d/%d, %d errors", i, b.N, errors)
			}
		}

		b.StopTimer()

		return published, errors
	}

	runAsyncPublisher := func(b *testing.B, js nats.JetStreamContext, messageSize int, subjects []string, asyncWindow int) (int, int) {
		const publishCompleteMaxWait = 30 * time.Second
		rng := rand.New(rand.NewSource(int64(seed)))
		message := make([]byte, messageSize)
		pending := make([]nats.PubAckFuture, 0, asyncWindow)
		published, errors := 0, 0

		b.ResetTimer()

		for i := 1; i <= b.N; i++ {
			rng.Read(message) // TODO may skip this?
			subject := subjects[rng.Intn(len(subjects))]
			pubAckFuture, err := js.PublishAsync(subject, message)
			if err != nil {
				errors++
				continue
			}
			pending = append(pending, pubAckFuture)

			// Regularly trim the list of pending
			if i%asyncWindow == 0 {
				newPending := make([]nats.PubAckFuture, 0, asyncWindow)
				for _, pubAckFuture := range pending {
					select {
					case <-pubAckFuture.Ok():
						published++
						b.SetBytes(int64(messageSize))
					case <-pubAckFuture.Err():
						errors++
					default:
						// This pubAck is still pending, keep it
						newPending = append(newPending, pubAckFuture)
					}
				}
				pending = newPending
			}

			if verbose && i%1000 == 0 {
				b.Logf("Published %d/%d, %d errors", i, b.N, errors)
			}
		}

		// All published, wait for completed
		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(publishCompleteMaxWait):
			b.Fatalf("Publish timed out")
		}

		// Clear whatever is left pending
		for _, pubAckFuture := range pending {
			select {
			case <-pubAckFuture.Ok():
				published++
				b.SetBytes(int64(messageSize))
			case <-pubAckFuture.Err():
				errors++
			default:
				b.Fatalf("PubAck is still pending after publish completed")
			}
		}

		b.StopTimer()

		return published, errors
	}

	type PublishType string
	const (
		Sync  PublishType = "Sync"
		Async PublishType = "Async"
	)

	benchmarksCases := []struct {
		clusterSize int
		replicas    int
		messageSize int
		numSubjects int
		minMessages int
	}{
		{1, 1, 10, 1, 100_000}, // Single node, 10B messages, ~1MB minimum
		{1, 1, 1024, 1, 1_000}, // Single node, 1KB messages, ~1MB minimum
		{3, 3, 10, 1, 100_000}, // 3-nodes cluster, R=3, 10B messages, ~1MB minimum
		{3, 3, 1024, 1, 1_000}, // 3-nodes cluster, R=3, 10B messages, ~1MB minimum
	}

	// All the cases above are run with each of the publisher cases below
	publisherCases := []struct {
		pType       PublishType
		asyncWindow int
	}{
		{Sync, -1},
		{Async, 1000},
		{Async, 4000},
		{Async, 8000},
	}

	for _, bc := range benchmarksCases {
		name := fmt.Sprintf(
			"N=%d,R=%d,MsgSz=%db,Subjs=%d",
			bc.clusterSize,
			bc.replicas,
			bc.messageSize,
			bc.numSubjects,
		)

		b.Run(
			name,
			func(b *testing.B) {

				for _, pc := range publisherCases {
					name := fmt.Sprintf("%v", pc.pType)
					if pc.pType == Async && pc.asyncWindow > 0 {
						name = fmt.Sprintf("%s[W:%d]", name, pc.asyncWindow)
					}

					b.Run(
						name,
						func(b *testing.B) {
							// Skip short runs, benchmark gets re-executed with a larger N
							if b.N < bc.minMessages {
								b.ResetTimer()
								return
							}

							subjects := make([]string, bc.numSubjects)
							for i := 0; i < bc.numSubjects; i++ {
								subjects[i] = fmt.Sprintf("s-%d", i+1)
							}

							if verbose {
								b.Logf("Running %s with %d ops", name, b.N)
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
								cl := createJetStreamClusterExplicit(b, "BENCH_PUB", bc.clusterSize)
								defer cl.shutdown()
								cl.waitOnClusterReadyWithNumPeers(bc.clusterSize)
								cl.waitOnLeader()
								connectURL = cl.randomServer().ClientURL()
							}

							nc, err := nats.Connect(connectURL)
							if err != nil {
								b.Fatalf("Failed to create client: %v", err)
							}
							defer nc.Close()

							jsOpts := []nats.JSOpt{
								nats.MaxWait(10 * time.Second),
							}

							if pc.asyncWindow > 0 && pc.pType == Async {
								jsOpts = append(jsOpts, nats.PublishAsyncMaxPending(pc.asyncWindow))
							}

							js, err := nc.JetStream(jsOpts...)
							if err != nil {
								b.Fatalf("Unexpected error getting JetStream context: %v", err)
							}

							if verbose {
								b.Logf("Creating stream with R=%d and %d input subjects", bc.replicas, bc.numSubjects)
							}
							streamConfig := &nats.StreamConfig{
								Name:     "S",
								Subjects: subjects,
								Replicas: bc.replicas,
							}
							if _, err := js.AddStream(streamConfig); err != nil {
								b.Fatalf("Error creating stream: %v", err)
							}

							if verbose {
								b.Logf("Running %v publisher with message size: %dB", pc.pType, bc.messageSize)
							}

							// Benchmark starts here
							b.ResetTimer()

							var published, errors int
							switch pc.pType {
							case Sync:
								published, errors = runSyncPublisher(b, js, bc.messageSize, subjects)
							case Async:
								published, errors = runAsyncPublisher(b, js, bc.messageSize, subjects, pc.asyncWindow)
							}

							// Benchmark ends here
							b.StopTimer()

							if published+errors != b.N {
								b.Fatalf("Something doesn't add up: %d + %d != %d", published, errors, b.N)
							}

							b.ReportMetric(float64(errors)*100/float64(b.N), "%error")
						},
					)
				}
			},
		)
	}
}

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
