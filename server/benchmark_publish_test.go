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

package server

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats.go"
)

func BenchmarkPublish(b *testing.B) {

	const (
		verbose     = false
		seed        = 12345
		minMessages = 10_000
		subject     = "S"
		queue       = "Q"
	)

	const (
		KB = 1024
		MB = KB * KB
	)

	type SubscriberType string
	const (
		Async      SubscriberType = "Async"
		QueueAsync SubscriberType = "AsyncQueue"
		None       SubscriberType = "None"
	)

	benchmarksCases := []struct {
		messageSize int
	}{
		{0},
		{1},
		{32},
		{128},
		{512},
		{4 * KB},
		{32 * KB},
		{128 * KB},
		{512 * KB},
		{1 * MB},
	}

	// All the cases above are run for each of the subscriber cases below
	subscribersCases := []struct {
		numSubs int
		subType SubscriberType
	}{
		{0, None},
		{1, Async},
		{1, QueueAsync},
		{10, Async},
		{10, QueueAsync},
	}

	for _, bc := range benchmarksCases {
		bcName := fmt.Sprintf(
			"MsgSz=%db",
			bc.messageSize,
		)

		b.Run(
			bcName,
			func(b *testing.B) {

				for _, sc := range subscribersCases {

					scName := fmt.Sprintf("Subs=%dx%v", sc.numSubs, sc.subType)
					if sc.subType == None {
						scName = fmt.Sprintf("Subs=%v", sc.subType)
					}

					b.Run(
						scName,
						func(b *testing.B) {
							// Skip short runs, benchmark gets re-executed with a larger N
							if b.N < minMessages {
								b.ResetTimer()
								return
							}

							if verbose {
								b.Logf("Running %s/%s with %d ops", bcName, scName, b.N)
							}

							subErrors := uint64(0)
							handleSubError := func(_ *nats.Conn, _ *nats.Subscription, _ error) {
								atomic.AddUint64(&subErrors, 1)
							}

							// Start single server (no JS)
							opts := DefaultTestOptions
							opts.Port = -1
							s := RunServer(&opts)
							defer s.Shutdown()

							// Create subscribers
							for i := 0; i < sc.numSubs; i++ {
								subConn, connErr := nats.Connect(s.ClientURL(), nats.ErrorHandler(handleSubError))
								if connErr != nil {
									b.Fatalf("Failed to connect: %v", connErr)
								}
								defer subConn.Close()

								var sub *nats.Subscription
								var subErr error

								switch sc.subType {
								case None:
									// No subscription
								case Async:
									sub, subErr = subConn.Subscribe(subject, func(*nats.Msg) {})
								case QueueAsync:
									sub, subErr = subConn.QueueSubscribe(subject, queue, func(*nats.Msg) {})
								default:
									b.Fatalf("Unknow subscribers type: %v", sc.subType)
								}

								if subErr != nil {
									b.Fatalf("Failed to subscribe: %v", subErr)
								}
								defer sub.Unsubscribe()
								// Do not drop messages due to slow subscribers:
								sub.SetPendingLimits(-1, -1)
							}

							// Create publisher connection
							nc, err := nats.Connect(s.ClientURL())
							if err != nil {
								b.Fatalf("Failed to connect: %v", err)
							}
							defer nc.Close()

							rng := rand.New(rand.NewSource(int64(seed)))
							message := make([]byte, bc.messageSize)
							var published, errors int

							// Benchmark starts here
							b.ResetTimer()

							for i := 0; i < b.N; i++ {
								rng.Read(message)
								pubErr := nc.Publish(subject, message)
								if pubErr != nil {
									errors++
								} else {
									published++
									b.SetBytes(int64(bc.messageSize))
								}
							}

							// Benchmark ends here
							b.StopTimer()

							if published+errors != b.N {
								b.Fatalf("Something doesn't add up: %d + %d != %d", published, errors, b.N)
							} else if subErrors > 0 {
								b.Fatalf("Subscribers errors: %d", subErrors)
							}

							b.ReportMetric(float64(errors)*100/float64(b.N), "%error")
						},
					)
				}
			},
		)
	}
}
