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
