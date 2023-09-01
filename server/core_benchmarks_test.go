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
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func BenchmarkRequestReplyOverEncryptedConnection(b *testing.B) {
	const (
		subject         = "test-subject"
		configsBasePath = "./configs/tls"
	)

	// default TLS client connection options
	defaultOpts := []nats.Option{}

	keyTypes := []string{
		"none",
		"ed25519",
		"rsa-1024",
		"rsa-2048",
		"rsa-4096",
	}
	payloadSzs := []int64{
		1024,   // 1kb
		4096,   // 4kb
		40960,  // 40kb
		409600, // 400kb
	}

	for _, keyType := range keyTypes {
		schemeConfig := fmt.Sprintf("%s/tls-%s.conf", configsBasePath, keyType)
		b.Run(fmt.Sprintf("keyType=%s", keyType), func(b *testing.B) {
			for _, payloadSz := range payloadSzs {
				b.Run(fmt.Sprintf("payloadSz=%db", payloadSz), func(b *testing.B) {

					// run server with tls scheme
					server, _ := RunServerWithConfig(schemeConfig)
					defer server.Shutdown()

					opts := defaultOpts
					if keyType != "none" {
						opts = append(opts, nats.Secure(&tls.Config{
							InsecureSkipVerify: true,
						}))
					}

					// default client url
					clientUrl := server.ClientURL()

					// subscriber
					ncSub, err := nats.Connect(clientUrl, opts...)
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

					// publisher
					ncPub, err := nats.Connect(clientUrl, opts...)
					if err != nil {
						b.Fatal(err)
					}
					defer ncPub.Close()

					var errors = 0

					// random bytes as payload
					b.SetBytes(payloadSz)
					payload := make([]byte, payloadSz)
					rand.Read(payload)

					// start benchmark
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						_, err := ncPub.Request(subject, payload, time.Second)
						if err != nil {
							errors++
						}
					}

					// stop benchmark
					b.StopTimer()

					b.ReportMetric(float64(errors), "errors")
				})
			}
		})
	}

}
