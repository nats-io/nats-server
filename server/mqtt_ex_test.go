// Copyright 2020-2023 The NATS Authors
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
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nuid"
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
	Kilo = 1024
	Mega = 1024 * Kilo
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
	MessageSize: []int{1, 1 * Kilo, 10 * Kilo, 100 * Kilo},
	Topics:      []int{1},
	Publishers:  []int{1, 10},
	Subscribers: []int{1, 10},
}

func BenchmarkMQTTExServer(b *testing.B) {
	b.StopTimer()
	bc := &mqttBenchContext{}
	defer bc.startServer(b)()

	bc.benchmarkPub(b)
	bc.benchmarkPubRetained(b)
	bc.benchmarkPubSub(b)
}

func BenchmarkMQTTExCluster(b *testing.B) {
	b.StopTimer()
	bc := &mqttBenchContext{}
	defer bc.startCluster(b)()

	bc.benchmarkPub(b)
	bc.benchmarkPubRetained(b)
	bc.benchmarkPubSub(b)
}

func (bc *mqttBenchContext) benchmarkPub(b *testing.B) {
	b.Run("PUB", func(b *testing.B) {
		mqttBenchDefaultMatrix.NoSubscribers().
			run(b, bc, func(b *testing.B) {
				bc.runPubNoReport(b, "mqtt-bench-"+nuid.Next(), false).report(b)
			})
	})
}

func (bc *mqttBenchContext) benchmarkPubRetained(b *testing.B) {
	b.Run("PUBRET", func(b *testing.B) {
		mqttBenchDefaultMatrix.NoSubscribers().
			run(b, bc, func(b *testing.B) {
				bc.runPubNoReport(b, "mqtt-bench-"+nuid.Next(), true).report(b)
			})
	})
}

func (bc *mqttBenchContext) benchmarkPubSub(b *testing.B) {
	b.Run("PUBSUB", func(b *testing.B) {
		mqttBenchDefaultMatrix.NoTopics().run(b, bc, bc.runPubSub)
	})
}

func (bc *mqttBenchContext) runPubNoReport(b *testing.B, topic string, retain bool) MQTTBenchmarkResult {
	pubCmd := mqttBenchLookupCommand(b, "mqtt-pub")
	pubCh := make(chan *MQTTBenchmarkResult)
	errCh := make(chan error)

	// Start the publishers.
	for i := 0; i < bc.Publishers; i++ {
		go func(i int) {
			args := []string{
				"--id", fmt.Sprintf("pub-%d-%s", i, nuid.Next()),
				"--topic", topic,
				"--qos", strconv.Itoa(bc.QOS),
				"--n", strconv.Itoa(b.N),
				"--size", strconv.Itoa(bc.MessageSize),
			}
			if retain {
				args = append(args, "--retain", "true")
			}
			pub, errlog, err := mqttBenchRunCommand(b, pubCmd, bc.Host, bc.Port, nil, args...)
			if err != nil {
				b.Logf("Error on publisher: %v\n\n%s\n", err, errlog)
				errCh <- err
			} else {
				pubCh <- pub
			}
		}(i)
	}

	// Wait for all results.
	pubT := MQTTBenchmarkResult{}
	for i := 0; i < bc.Publishers; i++ {
		select {
		case pub := <-pubCh:
			pubT.NS += pub.NS
			pubT.Ops += pub.Ops
			pubT.Bytes += pub.Bytes
			pubT.Unit = pub.Unit

		case err := <-errCh:
			b.Fatalf("Error: %v", err)
		}
	}

	pubT.NS = time.Duration(float64(pubT.NS) / float64(pubT.Ops))
	return pubT
}

func (bc *mqttBenchContext) runPubSub(b *testing.B) {
	subCmd := mqttBenchLookupCommand(b, "mqtt-sub")
	var subCh chan *MQTTBenchmarkResult
	errCh := make(chan error)

	topicPrefix := "mqtt-bench-"
	topic := topicPrefix + nuid.Next()

	// Start all subscribers, and wait for them if needed.
	var subsReady sync.WaitGroup
	subCh = make(chan *MQTTBenchmarkResult)
	subsReady.Add(bc.Subscribers)
	for i := 0; i < bc.Subscribers; i++ {
		go func(i int) {
			r, errlog, err := mqttBenchRunCommand(b, subCmd, bc.Host, bc.Port,
				func() {
					subsReady.Done()
				},
				"--id", fmt.Sprintf("sub-%d-%s", i, nuid.Next()),
				"--qos", strconv.Itoa(int(bc.QOS)),
				"--topic", topic,
				"--match-prefix", topicPrefix,
				"--n", strconv.Itoa(b.N*bc.Publishers), // Each publisher sends b.N messages.
			)
			if err != nil {
				b.Logf("Error on subscriber %d: %v\n\n%s\n", i, err, errlog)
				errCh <- err
			} else {
				subCh <- r
			}
		}(i)
	}
	subsReady.Wait()

	// Start the publishers.
	pubDone := make(chan struct{})
	go func() {
		bc.runPubNoReport(b, topic, false)
		pubDone <- struct{}{}
	}()

	// Wait for all results.
	subT := MQTTBenchmarkResult{}
	expectedResults := bc.Subscribers + 1 // 1 for the publishers.
	for i := 0; i < expectedResults; i++ {
		select {
		case <-pubDone:

		case sub := <-subCh:
			subT.NS += sub.NS
			subT.Ops += sub.Ops
			subT.Bytes += sub.Bytes
			subT.Unit = sub.Unit

		case err := <-errCh:
			b.Fatalf("Error: %v", err)
		}
	}

	subT.NS = subT.NS / time.Duration(subT.Ops)
	subT.report(b)
}

func mqttBenchLookupCommand(b *testing.B, name string) string {
	b.Helper()
	cmd, err := exec.LookPath(name)
	if err != nil {
		b.Skipf("%q command is not found in $PATH. Please `go install github.com/nats-io/meta-nats/apps/go/mqtt/...@latest` and try again.", name)
	}
	return cmd
}

func mqttBenchRunCommand(b *testing.B, name string, host string, port int, onReady func(), extraArgs ...string) (r *MQTTBenchmarkResult, errlog string, err error) {
	b.Helper()
	defer func() {
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				err = fmt.Errorf("Exited: %v\n\n%s\n", exitErr, string(exitErr.Stderr))
			}
		}
	}()

	args := append([]string{
		"--server", fmt.Sprintf("%s:%d", host, port),
	}, extraArgs...)

	cmd := exec.Command(name, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, "", err
	}
	defer stdout.Close()
	stderr := bytes.Buffer{}
	// defer fmt.Printf("<>/<> LOG: %s:\n\n%s\n\n", name, stderr.String())

	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, stderr.String(), err
	}

	// Wait for the command to output "READY\n".
	buf := make([]byte, 6)
	if _, err := stdout.Read(buf); err != nil {
		return nil, stderr.String(), fmt.Errorf("did not get a READY from %v: %v", name, err)
	}
	if string(buf) != "READY\n" {
		return nil, stderr.String(), fmt.Errorf(`unexpected output: %q, expected "READY\n"`, buf)
	}
	if onReady != nil {
		onReady()
	}

	r = &MQTTBenchmarkResult{}
	if err := json.NewDecoder(stdout).Decode(r); err != nil {
		return nil, stderr.String(), fmt.Errorf("failed to decode output of %v: %v", name, err)
	}
	if err := cmd.Wait(); err != nil {
		return nil, stderr.String(), err
	}

	return r, "", nil
}

func (bc *mqttBenchContext) initServer(b *testing.B) {
	b.Helper()
	subCmd := mqttBenchLookupCommand(b, "mqtt-sub")

	// <>/<> TODO replace with a proper retry check
	_, errlog, err := mqttBenchRunCommand(b, subCmd, bc.Host, bc.Port, nil,
		"--id", "__init__",
		"--topic", "__mqtt_init__",
		"--n", "0",
		"--qos", "0",
	)
	if err != nil {
		b.Fatalf("Error on initial connect/publish test: %v\n\n%s", err, errlog)
	}
}

func (bc *mqttBenchContext) startServer(b *testing.B) func() {
	b.Helper()
	s := testMQTTRunServer(b, testMQTTDefaultOptions())
	o := s.getOpts()
	bc.Host = o.MQTT.Host
	bc.Port = o.MQTT.Port
	bc.initServer(b)
	return func() {
		testMQTTShutdownServer(s)
	}
}

func (bc *mqttBenchContext) startCluster(b *testing.B) func() {
	b.Helper()
	cl := createJetStreamClusterWithTemplate(b,
		testMQTTGetClusterTemplaceNoLeaf(), "MQTT", int(3))
	o := cl.randomNonLeader().getOpts()
	bc.Host = o.MQTT.Host
	bc.Port = o.MQTT.Port
	bc.initServer(b)
	return cl.shutdown
}

func (m mqttBenchMatrix) runWithServer(b *testing.B, bc *mqttBenchContext, f func(*testing.B)) {
	b.StopTimer()
	defer bc.startServer(b)()
	m.run(b, bc, f)
}

func (m mqttBenchMatrix) runWithCluster(b *testing.B, bc *mqttBenchContext, f func(*testing.B)) {
	b.StopTimer()
	defer bc.startCluster(b)()
	m.run(b, bc, f)
}

func mqttBenchWrapForMatrixField(
	vFieldPtr *int,
	arr []int,
	f func(b *testing.B),
	namef func(int) string,
) func(b *testing.B) {
	if len(arr) == 0 {
		return f
	}
	return func(b *testing.B) {
		for _, value := range arr {
			*vFieldPtr = value
			b.Run(namef(value), func(b *testing.B) {
				f(b)
			})
		}
	}
}

func (m mqttBenchMatrix) run(b *testing.B, bc *mqttBenchContext, f func(*testing.B)) {
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
	f(b)
}

func (m mqttBenchMatrix) NoSubscribers() mqttBenchMatrix {
	m.Subscribers = nil
	return m
}

func (m mqttBenchMatrix) NoTopics() mqttBenchMatrix {
	m.Topics = nil
	return m
}

func sizeKB(size int) string {
	unit := "B"
	N := size
	if size >= Mega {
		unit = "MB"
		N /= Mega
	} else if size >= Kilo {
		unit = "KB"
		N /= Kilo
	}
	return fmt.Sprintf("%d%s", N, unit)
}

func (r MQTTBenchmarkResult) report(b *testing.B) {
	b.ReportMetric(float64(r.NS), "ns/op")
	b.ReportMetric(float64(r.NS)/1000000, r.Unit+"_ms/op")
	b.SetBytes(r.Bytes)
}
