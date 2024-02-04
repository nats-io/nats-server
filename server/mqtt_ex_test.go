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
// +build !skip_mqtt_tests

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

func TestMQTTExCompliance(t *testing.T) {
	mqttPath := os.Getenv("MQTT_CLI")
	if mqttPath == "" {
		if p, err := exec.LookPath("mqtt"); err == nil {
			mqttPath = p
		}
	}
	if mqttPath == "" {
		t.Skip(`"mqtt" command is not found in $PATH nor $MQTT_CLI. See https://hivemq.github.io/mqtt-cli/docs/installation/#debian-package for installation instructions`)
	}

	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream {
			store_dir = %q
		}
		mqtt {
			listen: 127.0.0.1:-1
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	cmd := exec.Command(mqttPath, "test", "-V", "3", "-p", strconv.Itoa(o.MQTT.Port))

	output, err := cmd.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			t.Fatalf("mqtt cli exited with error: %v", exitError)
		}
	}
}

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

	// full path to mqtt-test command
	testCmdPath string
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

func BenchmarkMQTTEx(b *testing.B) {
	bc := mqttNewBenchEx(b)
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
			bc.runCommand(b, "pub",
				"--qos", strconv.Itoa(bc.QOS),
				"--n", strconv.Itoa(b.N),
				"--size", strconv.Itoa(bc.MessageSize),
				"--num-publishers", strconv.Itoa(bc.Publishers),
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
			bc.runCommand(b, "pub", "--retain",
				"--qos", strconv.Itoa(bc.QOS),
				"--n", strconv.Itoa(b.N),
				"--size", strconv.Itoa(bc.MessageSize),
				"--num-publishers", strconv.Itoa(bc.Publishers),
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
			bc.runCommand(b, "pubsub",
				"--qos", strconv.Itoa(bc.QOS),
				"--n", strconv.Itoa(b.N),
				"--size", strconv.Itoa(bc.MessageSize),
				"--num-subscribers", strconv.Itoa(bc.Subscribers),
			)
		})
	})
}

// makes a copy of bc
func (bc mqttBenchContext) benchmarkSubRet(b *testing.B) {
	// This test uses a a built-in publisher, and it makes most sense to measure
	// the retained message delivery "overhead" on a QoS0 subscription; without
	// the extra time involved in actually subscribing.
	m := mqttBenchDefaultMatrix.
		NoPublishers().
		QOS0Only()

	b.Run("SUBRET", func(b *testing.B) {
		m.runMatrix(b, bc, func(b *testing.B, bc *mqttBenchContext) {
			bc.runCommand(b, "subret",
				"--qos", strconv.Itoa(bc.QOS),
				"--n", strconv.Itoa(b.N), // number of subscribe requests
				"--num-subscribers", strconv.Itoa(bc.Subscribers),
				"--num-topics", strconv.Itoa(bc.Topics),
				"--size", strconv.Itoa(bc.MessageSize),
			)
		})
	})
}

func mqttBenchLookupCommand(b *testing.B, name string) string {
	b.Helper()
	cmd, err := exec.LookPath(name)
	if err != nil {
		b.Skipf("%q command is not found in $PATH. Please `go install github.com/nats-io/meta-nats/apps/go/mqtt/...@latest` and try again.", name)
	}
	return cmd
}

func (bc mqttBenchContext) runCommand(b *testing.B, name string, extraArgs ...string) {
	b.Helper()

	args := append([]string{
		name,
		"-q",
		"--servers", fmt.Sprintf("%s:%d", bc.Host, bc.Port),
	}, extraArgs...)

	cmd := exec.Command(bc.testCmdPath, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		b.Fatalf("Error executing %q: %v", cmd.String(), err)
	}
	defer stdout.Close()
	errbuf := bytes.Buffer{}
	cmd.Stderr = &errbuf
	if err = cmd.Start(); err != nil {
		b.Fatalf("Error executing %q: %v", cmd.String(), err)
	}
	r := &MQTTBenchmarkResult{}
	if err = json.NewDecoder(stdout).Decode(r); err != nil {
		b.Fatalf("failed to decode output of %q: %v\n\n%s", cmd.String(), err, errbuf.String())
	}
	if err = cmd.Wait(); err != nil {
		b.Fatalf("Error executing %q: %v\n\n%s", cmd.String(), err, errbuf.String())
	}

	r.report(b)
}

func (bc mqttBenchContext) initServer(b *testing.B) {
	b.Helper()
	bc.runCommand(b, "pubsub",
		"--id", "__init__",
		"--qos", "0",
		"--n", "1",
		"--size", "100",
		"--num-subscribers", "1")
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
	bc.initServer(b)
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
	bc.initServer(b)
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

func mqttNewBenchEx(b *testing.B) *mqttBenchContext {
	cmd := mqttBenchLookupCommand(b, "mqtt-test")
	return &mqttBenchContext{
		testCmdPath: cmd,
	}
}
