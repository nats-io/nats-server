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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/nats-io/nuid"
)

type mqttDial string

type mqttTarget struct {
	singleServers []*Server
	clusters      []*cluster
	configs       []mqttTestConfig
	all           []mqttDial
}

type mqttTestConfig struct {
	name string
	pub  []mqttDial
	sub  []mqttDial
}

func TestXMQTTCompliance(t *testing.T) {
	if mqttCLICommandPath == _EMPTY_ {
		t.Skip(`"mqtt" command is not found in $PATH nor $MQTT_CLI. See https://hivemq.github.io/mqtt-cli/docs/installation/#debian-package for installation instructions`)
	}

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	o = s.getOpts()
	defer testMQTTShutdownServer(s)

	cmd := exec.Command(mqttCLICommandPath, "test", "-V", "3", "-p", strconv.Itoa(o.MQTT.Port))

	output, err := cmd.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			t.Fatalf("mqtt cli exited with error: %v", exitError)
		}
	}
}

func TestXMQTTRetainedMessages(t *testing.T) {
	if mqttTestCommandPath == _EMPTY_ {
		t.Skip(`"mqtt-test" command is not found in $PATH.`)
	}

	for _, topo := range []struct {
		name  string
		makef func(testing.TB) *mqttTarget
	}{
		{
			name:  "single server",
			makef: mqttMakeTestServer,
		},
		{
			name:  "cluster",
			makef: mqttMakeTestCluster(4, ""),
		},
	} {
		t.Run(topo.name, func(t *testing.T) {
			target := topo.makef(t)
			t.Cleanup(target.Shutdown)

			// initialize the MQTT assets by "touching" all nodes in the
			// cluster, but then reload to start with fresh server state.
			for _, dial := range target.all {
				mqttInitTestServer(t, dial)
			}

			numRMS := 100
			strNumRMS := strconv.Itoa(numRMS)
			topics := make([]string, len(target.configs))

			for i, tc := range target.configs {
				// Publish numRMS retained messages one at a time,
				// round-robin across pub nodes. Remember the topic for each
				// test config to check the subs after reload.
				topic := "subret_" + nuid.Next()
				topics[i] = topic
				iNode := 0
				for i := 0; i < numRMS; i++ {
					pubTopic := fmt.Sprintf("%s/%d", topic, i)
					dial := tc.pub[iNode%len(tc.pub)]
					mqttRunExCommandTest(t, "pub", dial,
						"--retain",
						"--topic", pubTopic,
						"--qos", "0",
						"--size", "128", // message size 128 bytes
					)
					iNode++
				}
			}

			// Check all sub nodes for retained messages
			for i, tc := range target.configs {
				for _, dial := range tc.sub {
					mqttRunExCommandTest(t, "sub", dial,
						"--retained", strNumRMS,
						"--qos", "0",
						"--topic", topics[i],
					)
				}
			}

			// Reload the target
			target.Reload(t)

			// Now check again
			for i, tc := range target.configs {
				for _, dial := range tc.sub {
					mqttRunExCommandTestRetry(t, 1, "sub", dial,
						"--retained", strNumRMS,
						"--qos", "0",
						"--topic", topics[i],
					)
				}
			}
		})
	}
}

func mqttInitTestServer(tb testing.TB, dial mqttDial) {
	tb.Helper()
	mqttRunExCommandTestRetry(tb, 5, "pub", dial)
}

func mqttNewDialForServer(s *Server, username, password string) mqttDial {
	o := s.getOpts().MQTT
	return mqttNewDial(username, password, o.Host, o.Port, s.Name())
}

func mqttNewDial(username, password, host string, port int, comment string) mqttDial {
	d := ""
	switch {
	case username != "" && password != "":
		d = fmt.Sprintf("%s:%s@%s:%d", username, password, host, port)
	case username != "":
		d = fmt.Sprintf("%s@%s:%d", username, host, port)
	default:
		d = fmt.Sprintf("%s:%d", host, port)
	}
	if comment != "" {
		d += "#" + comment
	}
	return mqttDial(d)
}

func (d mqttDial) Get() (u, p, s, c string) {
	if d == "" {
		return "", "", "127.0.0.1:1883", ""
	}
	in := string(d)
	if i := strings.LastIndex(in, "#"); i != -1 {
		c = in[i+1:]
		in = in[:i]
	}
	if i := strings.LastIndex(in, "@"); i != -1 {
		up := in[:i]
		in = in[i+1:]
		u = up
		if i := strings.Index(up, ":"); i != -1 {
			u = up[:i]
			p = up[i+1:]
		}
	}
	s = in
	return u, p, s, c
}

func (d mqttDial) Name() string {
	_, _, _, c := d.Get()
	return c
}

func (t *mqttTarget) Reload(tb testing.TB) {
	tb.Helper()

	for _, c := range t.clusters {
		c.stopAll()
		c.restartAllSamePorts()
	}

	for i, s := range t.singleServers {
		o := s.getOpts()
		s.Shutdown()
		t.singleServers[i] = testMQTTRunServer(tb, o)
	}

	for _, dial := range t.all {
		mqttInitTestServer(tb, dial)
	}
}

func (t *mqttTarget) Shutdown() {
	for _, c := range t.clusters {
		c.shutdown()
	}
	for _, s := range t.singleServers {
		testMQTTShutdownServer(s)
	}
}

func mqttMakeTestServer(tb testing.TB) *mqttTarget {
	tb.Helper()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(tb, o)
	all := []mqttDial{mqttNewDialForServer(s, "", "")}
	return &mqttTarget{
		singleServers: []*Server{s},
		all:           all,
		configs: []mqttTestConfig{
			{
				name: "single server",
				pub:  all,
				sub:  all,
			},
		},
	}
}

func mqttMakeTestCluster(size int, domain string) func(tb testing.TB) *mqttTarget {
	return func(tb testing.TB) *mqttTarget {
		tb.Helper()
		if size < 3 {
			tb.Fatal("cluster size must be at least 3")
		}

		if domain != "" {
			domain = "domain: " + domain + ", "
		}
		clusterConf := `
	listen: 127.0.0.1:-1

	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, ` + domain + `store_dir: '%s'}

	leafnodes {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	mqtt {
		listen: 127.0.0.1:-1
		stream_replicas: 3
	}

	accounts {
		ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`
		cl := createJetStreamClusterWithTemplate(tb, clusterConf, "MQTT", size)
		cl.waitOnLeader()

		all := []mqttDial{}
		for _, s := range cl.servers {
			all = append(all, mqttNewDialForServer(s, "one", "p"))
		}

		return &mqttTarget{
			clusters: []*cluster{cl},
			all:      all,
			configs: []mqttTestConfig{
				{
					name: "publish to one",
					pub: []mqttDial{
						mqttNewDialForServer(cl.randomServer(), "one", "p"),
					},
					sub: all,
				},
				{
					name: "publish to all",
					pub:  all,
					sub:  all,
				},
			},
		}
	}
}

var mqttCLICommandPath = func() string {
	p := os.Getenv("MQTT_CLI")
	if p == "" {
		p, _ = exec.LookPath("mqtt")
	}
	return p
}()

var mqttTestCommandPath = func() string {
	p, _ := exec.LookPath("mqtt-test")
	return p
}()

func mqttRunExCommandTest(tb testing.TB, subCommand string, dial mqttDial, extraArgs ...string) *MQTTBenchmarkResult {
	tb.Helper()
	return mqttRunExCommandTestRetry(tb, 1, subCommand, dial, extraArgs...)
}

func mqttRunExCommandTestRetry(tb testing.TB, n int, subCommand string, dial mqttDial, extraArgs ...string) (r *MQTTBenchmarkResult) {
	tb.Helper()
	var err error
	for i := 0; i < n; i++ {
		if r, err = mqttTryExCommandTest(tb, subCommand, dial, extraArgs...); err == nil {
			return r
		}

		if i < (n - 1) {
			tb.Logf("failed to %q %s to %q on attempt %v, will retry.", subCommand, extraArgs, dial.Name(), i)
		} else {
			tb.Fatal(err)
		}
	}
	return nil
}

func mqttTryExCommandTest(tb testing.TB, subCommand string, dial mqttDial, extraArgs ...string) (r *MQTTBenchmarkResult, err error) {
	tb.Helper()
	if mqttTestCommandPath == "" {
		tb.Skip(`"mqtt-test" command is not found in $PATH.`)
	}

	args := []string{subCommand, // "-q",
		"-s", string(dial),
	}
	args = append(args, extraArgs...)
	cmd := exec.Command(mqttTestCommandPath, args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("error executing %q: %v", cmd.String(), err)
	}
	defer stdout.Close()
	errbuf := bytes.Buffer{}
	cmd.Stderr = &errbuf
	if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("error executing %q: %v", cmd.String(), err)
	}
	out, err := io.ReadAll(stdout)
	if err != nil {
		return nil, fmt.Errorf("error executing %q: failed to read output: %v", cmd.String(), err)
	}
	if err = cmd.Wait(); err != nil {
		return nil, fmt.Errorf("error executing %q: %v\n\n%s\n\n%s", cmd.String(), err, string(out), errbuf.String())
	}

	r = &MQTTBenchmarkResult{}
	if err := json.Unmarshal(out, r); err != nil {
		tb.Fatalf("error executing %q: failed to decode output: %v\n\n%s\n\n%s", cmd.String(), err, string(out), errbuf.String())
	}
	return r, nil
}
