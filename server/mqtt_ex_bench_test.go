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

//go:build !skip_mqtt_tests

package server

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

const (
	KB = 1024
)

type mqttBenchMatrix struct {
	QOS         []int
	MessageSize []int
	Topics      []int
	Publishers  []int
	Subscribers []int
}

type mqttBenchContext struct {
	QOS         int
	MessageSize int
	Topics      int
	Publishers  int
	Subscribers int

	Host string
	Port int
}

var mqttBenchDefaultMatrix = mqttBenchMatrix{
	QOS:         []int{0, 1, 2},
	MessageSize: []int{100, 1 * KB, 10 * KB},
	Topics:      []int{100},
	Publishers:  []int{1},
	Subscribers: []int{1},
}

type MQTTBenchmarkResult struct {
	Ops   int                      `json:"ops"`
	NS    map[string]time.Duration `json:"ns"`
	Bytes int64                    `json:"bytes"`
}

func BenchmarkXMQTT(b *testing.B) {
	if mqttTestCommandPath == "" {
		b.Skip(`"mqtt-test" command is not found in $PATH.`)
	}

	bc := mqttBenchContext{}
	b.Run("Server", func(b *testing.B) {
		b.Cleanup(bc.startServer(b, false))
		bc.runAll(b)
	})

	b.Run("Cluster", func(b *testing.B) {
		b.Cleanup(bc.startCluster(b, false))
		bc.runAll(b)
	})

	b.Run("Server-no-RMSCache", func(b *testing.B) {
		b.Cleanup(bc.startServer(b, true))

		bc.benchmarkSubRet(b)
	})

	b.Run("Cluster-no-RMSCache", func(b *testing.B) {
		b.Cleanup(bc.startCluster(b, true))

		bc.benchmarkSubRet(b)
	})
}

func (bc mqttBenchContext) runAll(b *testing.B) {
	bc.benchmarkPub(b)
	bc.benchmarkPubRetained(b)
	bc.benchmarkPubSub(b)
	bc.benchmarkSubRet(b)
}

// makes a copy of bc
func (bc mqttBenchContext) benchmarkPub(b *testing.B) {
	m := mqttBenchDefaultMatrix.
		NoSubscribers().
		NoTopics()

	b.Run("PUB", func(b *testing.B) {
		m.runMatrix(b, bc, func(b *testing.B, bc *mqttBenchContext) {
			bc.runAndReport(b, "pub",
				"--qos", strconv.Itoa(bc.QOS),
				"--messages", strconv.Itoa(b.N),
				"--size", strconv.Itoa(bc.MessageSize),
				"--publishers", strconv.Itoa(bc.Publishers),
			)
		})
	})
}

// makes a copy of bc
func (bc mqttBenchContext) benchmarkPubRetained(b *testing.B) {
	// This bench is meaningless for QOS0 since the client considers the message
	// sent as soon as it's written out. It is also useless for QOS2 since the
	// flow takes a lot longer, and the difference of publishing as retained or
	// not is lost in the noise.
	m := mqttBenchDefaultMatrix.
		NoSubscribers().
		NoTopics().
		QOS1Only()

	b.Run("PUBRET", func(b *testing.B) {
		m.runMatrix(b, bc, func(b *testing.B, bc *mqttBenchContext) {
			bc.runAndReport(b, "pub", "--retain",
				"--qos", strconv.Itoa(bc.QOS),
				"--messages", strconv.Itoa(b.N),
				"--size", strconv.Itoa(bc.MessageSize),
				"--publishers", strconv.Itoa(bc.Publishers),
			)
		})
	})
}

// makes a copy of bc
func (bc mqttBenchContext) benchmarkPubSub(b *testing.B) {
	// This test uses a single built-in topic, and a built-in publisher, so no
	// reason to run it for topics and publishers.
	m := mqttBenchDefaultMatrix.
		NoTopics().
		NoPublishers()

	b.Run("PUBSUB", func(b *testing.B) {
		m.runMatrix(b, bc, func(b *testing.B, bc *mqttBenchContext) {
			bc.runAndReport(b, "pubsub",
				"--qos", strconv.Itoa(bc.QOS),
				"--messages", strconv.Itoa(b.N),
				"--size", strconv.Itoa(bc.MessageSize),
				"--subscribers", strconv.Itoa(bc.Subscribers),
			)
		})
	})
}

// makes a copy of bc
func (bc mqttBenchContext) benchmarkSubRet(b *testing.B) {
	// This test uses a built-in publisher, and it makes most sense to measure
	// the retained message delivery "overhead" on a QoS0 subscription; without
	// the extra time involved in actually subscribing.
	m := mqttBenchDefaultMatrix.
		NoPublishers().
		QOS0Only()

	b.Run("SUBRET", func(b *testing.B) {
		m.runMatrix(b, bc, func(b *testing.B, bc *mqttBenchContext) {
			bc.runAndReport(b, "subret",
				"--qos", strconv.Itoa(bc.QOS),
				"--topics", strconv.Itoa(bc.Topics), // number of retained messages
				"--subscribers", strconv.Itoa(bc.Subscribers),
				"--size", strconv.Itoa(bc.MessageSize),
				"--repeat", strconv.Itoa(b.N), // number of subscribe requests
			)
		})
	})
}

func (bc mqttBenchContext) runAndReport(b *testing.B, name string, extraArgs ...string) {
	b.Helper()
	r := mqttRunExCommandTest(b, name, mqttNewDial("", "", bc.Host, bc.Port, ""), extraArgs...)
	r.report(b)
}

func (bc *mqttBenchContext) startServer(b *testing.B, disableRMSCache bool) func() {
	b.Helper()
	b.StopTimer()
	prevDisableRMSCache := testDisableRMSCache
	testDisableRMSCache = disableRMSCache
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(b, o)

	o = s.getOpts()
	bc.Host = o.MQTT.Host
	bc.Port = o.MQTT.Port
	mqttInitTestServer(b, mqttNewDial("", "", bc.Host, bc.Port, ""))
	return func() {
		testMQTTShutdownServer(s)
		testDisableRMSCache = prevDisableRMSCache
	}
}

func (bc *mqttBenchContext) startCluster(b *testing.B, disableRMSCache bool) func() {
	b.Helper()
	b.StopTimer()
	prevDisableRMSCache := testDisableRMSCache
	testDisableRMSCache = disableRMSCache
	conf := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		mqtt {
			listen: 127.0.0.1:-1
			stream_replicas: 3
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`

	cl := createJetStreamClusterWithTemplate(b, conf, "MQTT", 3)
	o := cl.randomNonLeader().getOpts()
	bc.Host = o.MQTT.Host
	bc.Port = o.MQTT.Port
	mqttInitTestServer(b, mqttNewDial("", "", bc.Host, bc.Port, ""))
	return func() {
		cl.shutdown()
		testDisableRMSCache = prevDisableRMSCache
	}
}

func mqttBenchWrapForMatrixField(
	vFieldPtr *int,
	arr []int,
	f func(b *testing.B, bc *mqttBenchContext),
	namef func(int) string,
) func(b *testing.B, bc *mqttBenchContext) {
	if len(arr) == 0 {
		return f
	}
	return func(b *testing.B, bc *mqttBenchContext) {
		for _, value := range arr {
			*vFieldPtr = value
			b.Run(namef(value), func(b *testing.B) {
				f(b, bc)
			})
		}
	}
}

func (m mqttBenchMatrix) runMatrix(b *testing.B, bc mqttBenchContext, f func(*testing.B, *mqttBenchContext)) {
	b.Helper()
	f = mqttBenchWrapForMatrixField(&bc.MessageSize, m.MessageSize, f, func(size int) string {
		return sizeKB(size)
	})
	f = mqttBenchWrapForMatrixField(&bc.Topics, m.Topics, f, func(n int) string {
		return fmt.Sprintf("%dtopics", n)
	})
	f = mqttBenchWrapForMatrixField(&bc.Publishers, m.Publishers, f, func(n int) string {
		return fmt.Sprintf("%dpubc", n)
	})
	f = mqttBenchWrapForMatrixField(&bc.Subscribers, m.Subscribers, f, func(n int) string {
		return fmt.Sprintf("%dsubc", n)
	})
	f = mqttBenchWrapForMatrixField(&bc.QOS, m.QOS, f, func(qos int) string {
		return fmt.Sprintf("QOS%d", qos)
	})
	b.ResetTimer()
	b.StartTimer()
	f(b, &bc)
}

func (m mqttBenchMatrix) NoSubscribers() mqttBenchMatrix {
	m.Subscribers = nil
	return m
}

func (m mqttBenchMatrix) NoTopics() mqttBenchMatrix {
	m.Topics = nil
	return m
}

func (m mqttBenchMatrix) NoPublishers() mqttBenchMatrix {
	m.Publishers = nil
	return m
}

func (m mqttBenchMatrix) QOS0Only() mqttBenchMatrix {
	m.QOS = []int{0}
	return m
}

func (m mqttBenchMatrix) QOS1Only() mqttBenchMatrix {
	m.QOS = []int{1}
	return m
}

func sizeKB(size int) string {
	unit := "B"
	N := size
	if size >= KB {
		unit = "KB"
		N = (N + KB/2) / KB
	}
	return fmt.Sprintf("%d%s", N, unit)
}

func (r MQTTBenchmarkResult) report(b *testing.B) {
	// Disable the default ns metric in favor of custom X_ms/op.
	b.ReportMetric(0, "ns/op")

	// Disable MB/s since the github benchmarking action misinterprets the sign
	// of the result (treats it as less is better).
	b.SetBytes(0)
	// b.SetBytes(r.Bytes)

	for unit, ns := range r.NS {
		nsOp := float64(ns) / float64(r.Ops)
		b.ReportMetric(nsOp/1000000, unit+"_ms/op")
	}
	// Diable ReportAllocs() since it confuses the github benchmarking action
	// with the noise.
	// b.ReportAllocs()
}
