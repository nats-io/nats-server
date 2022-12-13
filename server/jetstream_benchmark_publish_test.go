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
