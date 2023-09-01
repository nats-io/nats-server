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

package server

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func BenchmarkCoreRequestReply(b *testing.B) {
	const (
		subject = "test-subject"
	)

	messageSizes := []int64{
		1024,   // 1kb
		4096,   // 4kb
		40960,  // 40kb
		409600, // 400kb
	}

	for _, messageSize := range messageSizes {
		b.Run(fmt.Sprintf("msgSz=%db", messageSize), func(b *testing.B) {

			// Start server
			serverOpts := DefaultOptions()
			server := RunServer(serverOpts)
			defer server.Shutdown()

			clientUrl := server.ClientURL()

			// Create "echo" subscriber
			ncSub, err := nats.Connect(clientUrl)
			if err != nil {
				b.Fatal(err)
			}
			defer ncSub.Close()
			sub, err := ncSub.Subscribe(subject, func(msg *nats.Msg) {
				// Responder echoes the request payload as-is
				msg.Respond(msg.Data)
			})
			defer sub.Unsubscribe()
			if err != nil {
				b.Fatal(err)
			}

			// Create publisher
			ncPub, err := nats.Connect(clientUrl)
			if err != nil {
				b.Fatal(err)
			}
			defer ncPub.Close()

			var errors = 0

			// Create message (reused for all requests)
			messageData := make([]byte, messageSize)
			b.SetBytes(messageSize)
			rand.Read(messageData)

			// Benchmark
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ncPub.Request(subject, messageData, time.Second)
				if err != nil {
					errors++
				}
			}
			b.StopTimer()

			b.ReportMetric(float64(errors), "errors")
		})
	}
}
