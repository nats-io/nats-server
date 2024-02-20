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
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/nats-io/nuid"
)

type mqttExDial string

type mqttExTarget struct {
	servers  []*Server
	clusters []*cluster
	configs  []mqttExTestConfig
	all      []mqttExDial
}

type mqttExTestConfig struct {
	name string
	pub  []mqttExDial
	sub  []mqttExDial
}

func TestMQTTExCompliance(t *testing.T) {
	if mqttexCLICommandPath == _EMPTY_ {
		t.Skip(`"mqtt" command is not found in $PATH nor $MQTT_CLI. See https://hivemq.github.io/mqtt-cli/docs/installation/#debian-package for installation instructions`)
	}

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	o = s.getOpts()
	defer testMQTTShutdownServer(s)

	cmd := exec.Command(mqttexCLICommandPath, "test", "-V", "3", "-p", strconv.Itoa(o.MQTT.Port))

	output, err := cmd.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			t.Fatalf("mqtt cli exited with error: %v", exitError)
		}
	}
}

func TestMQTTExRetainedMessages(t *testing.T) {
	if mqttexTestCommandPath == _EMPTY_ {
		t.Skip(`"mqtt-test" command is not found in $PATH.`)
	}

	for _, topo := range []struct {
		name  string
		makef func(testing.TB) *mqttExTarget
	}{
		{
			name:  "single server",
			makef: mqttExMakeServer,
		},
		{
			name:  "server with leafnode",
			makef: mqttExMakeServerWithLeafnode("HUBD", "LEAFD", true),
		},
		{
			name:  "server with leafnode no domains",
			makef: mqttExMakeServerWithLeafnode("", "", true),
		},
		{
			name:  "server with leafnode no system account",
			makef: mqttExMakeServerWithLeafnode("HUBD", "LEAFD", false),
		},
		{
			name:  "cluster",
			makef: mqttExMakeCluster(4, ""),
		},
		{
			name:  "cluster with leafnode cluster",
			makef: mqttExMakeClusterWithLeafnodeCluster("HUBD", "LEAFD", true),
		},
		{
			name:  "cluster with leafnode cluster no system account",
			makef: mqttExMakeClusterWithLeafnodeCluster("HUBD", "LEAFD", false),
		},
	} {
		t.Run(topo.name, func(t *testing.T) {
			target := topo.makef(t)
			t.Cleanup(target.Shutdown)

			// initialize the MQTT assets by "touching" all nodes in the
			// cluster, but then reload to start with fresh server state.
			for _, dial := range target.all {
				mqttExInitServer(t, dial)
			}
			target.Reload(t)

			numRMS := 100
			strNumRMS := strconv.Itoa(numRMS)
			for _, tc := range target.configs {
				t.Run(tc.name, func(t *testing.T) {
					topic := "subret_" + nuid.Next()

					// publish numRMS retained messages, one at a time,
					// round-robin across pub nodes.
					iNode := 0
					for i := 0; i < numRMS; i++ {
						pubTopic := fmt.Sprintf("%s/%d", topic, i)
						dial := tc.pub[iNode%len(tc.pub)]
						mqttexRunTest(t, "pub", dial,
							"--retain",
							"--topic", pubTopic,
							"--qos", "0",
							"--size", "128", // message size 128 bytes
						)
						iNode++
					}

					check := func(testNameFormat string) {
						for _, dial := range tc.sub {
							_, _, _, name := dial.Get()
							t.Run(fmt.Sprintf(testNameFormat, name), func(t *testing.T) {
								mqttexRunTest(t, "sub", dial,
									"--retained", strNumRMS,
									"--qos", "0",
									"--topic", topic,
								)
							})
						}
					}

					// Subscribe and receive all retained on each sub node. Then
					// reload the entire target cluster, and repeat to ensure
					// retained messages are persisted and re-delivered after
					// restart.
					check("subscribe at %s")
					target.Reload(t)
					check("subscribe after reload at %s")
				})
			}
		})
	}
}

func mqttExInitServer(tb testing.TB, dial mqttExDial) {
	tb.Helper()
	mqttexRunTest(tb, "pub", dial, "--id", "__init__")
}

func mqttExNewDialForServer(s *Server, username, password string) mqttExDial {
	o := s.getOpts().MQTT
	return mqttExNewDial(username, password, o.Host, o.Port, s.Name())
}

func mqttExNewDial(username, password, host string, port int, comment string) mqttExDial {
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
	return mqttExDial(d)
}

func (d mqttExDial) Get() (u, p, s, c string) {
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

func (t *mqttExTarget) Reload(tb testing.TB) {
	tb.Helper()

	for _, c := range t.clusters {
		c.restartAllSamePorts()
	}

	for i, s := range t.servers {
		o := s.getOpts()
		s.Shutdown()
		t.servers[i] = testMQTTRunServer(tb, o)
	}
}

func (t *mqttExTarget) Shutdown() {
	for _, c := range t.clusters {
		c.shutdown()
	}
	for _, s := range t.servers {
		testMQTTShutdownServer(s)
	}
}

func mqttExMakeServer(tb testing.TB) *mqttExTarget {
	tb.Helper()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(tb, o)
	all := []mqttExDial{mqttExNewDialForServer(s, "", "")}
	return &mqttExTarget{
		servers: []*Server{s},
		all:     all,
		configs: []mqttExTestConfig{
			{
				name: "single server",
				pub:  all,
				sub:  all,
			},
		},
	}
}

func mqttExMakeServerWithLeafnode(hubd, leafd string, connectSystemAccount bool) func(tb testing.TB) *mqttExTarget {
	return func(tb testing.TB) *mqttExTarget {
		tb.Helper()

		if hubd != "" {
			hubd = "domain: " + hubd + ", "
		}
		sconf := `
listen: 127.0.0.1:-1

server_name: HUB
jetstream: {max_mem_store: 256MB, max_file_store: 2GB, ` + hubd + `store_dir: '` + tb.TempDir() + `'}

leafnodes {
	listen: 127.0.0.1:-1
}

accounts {
	ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
	$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
}

mqtt {
	listen: 127.0.0.1:-1
}
`
		hubConf := createConfFile(tb, []byte(sconf))
		hubServer, o := RunServerWithConfig(hubConf)
		leafRemoteAddr := fmt.Sprintf("%s:%d", o.LeafNode.Host, o.LeafNode.Port)
		tb.Cleanup(func() {
			os.Remove(hubConf)
		})

		sysRemote := ""
		if connectSystemAccount {
			sysRemote = `{ url: "nats://admin:s3cr3t!@` + leafRemoteAddr + `", account: "$SYS" },` + "\n\t\t"
		}
		if leafd != "" {
			leafd = "domain: " + leafd + ", "
		}
		leafconf := `
listen: 127.0.0.1:-1

server_name: SPOKE
jetstream: {max_mem_store: 256MB, max_file_store: 2GB, ` + leafd + `store_dir: '` + tb.TempDir() + `'}

leafnodes {
	remotes = [
		` + sysRemote + `{ url: "nats://one:p@` + leafRemoteAddr + `", account: "ONE" },
	]
}

accounts {
	ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
	$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
}

mqtt {
	listen: 127.0.0.1:-1
}
`
		leafConf := createConfFile(tb, []byte(leafconf))
		leafServer, _ := RunServerWithConfig(leafConf)
		tb.Cleanup(func() {
			os.Remove(leafConf)
		})

		both := []mqttExDial{
			mqttExNewDialForServer(hubServer, "one", "p"),
			mqttExNewDialForServer(leafServer, "one", "p"),
		}
		return &mqttExTarget{
			servers: []*Server{hubServer, leafServer},
			all:     both,
			configs: []mqttExTestConfig{
				{name: "pub to all", pub: both, sub: both},
				{name: "pub to SPOKE", pub: both[1:], sub: both},
				{name: "pub to HUB", pub: both[:1], sub: both},
			},
		}
	}
}

func mqttExMakeCluster(size int, domain string) func(tb testing.TB) *mqttExTarget {
	return func(tb testing.TB) *mqttExTarget {
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

		all := []mqttExDial{}
		for _, s := range cl.servers {
			all = append(all, mqttExNewDialForServer(s, "one", "p"))
		}

		return &mqttExTarget{
			clusters: []*cluster{cl},
			all:      all,
			configs: []mqttExTestConfig{
				{
					name: "publish to one",
					pub: []mqttExDial{
						mqttExNewDialForServer(cl.randomServer(), "one", "p"),
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

func mqttExMakeClusterWithLeafnodeCluster(hubd, leafd string, connectSystemAccount bool) func(tb testing.TB) *mqttExTarget {
	return func(tb testing.TB) *mqttExTarget {
		tb.Helper()

		// Create HUB cluster.
		if hubd != "" {
			hubd = "domain: " + hubd + ", "
		}
		hubConf := `
	listen: 127.0.0.1:-1

	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, ` + hubd + `store_dir: '%s'}

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
		hub := createJetStreamClusterWithTemplate(tb, hubConf, "HUB", 3)
		hub.waitOnLeader()

		// Pick a host to connect leafnodes to
		lno := hub.randomNonLeader().getOpts().LeafNode
		leafRemoteAddr := fmt.Sprintf("%s:%d", lno.Host, lno.Port)
		hubRandom := mqttExNewDialForServer(hub.randomNonLeader(), "one", "p")
		hubAll := []mqttExDial{}
		for _, s := range hub.servers {
			hubAll = append(hubAll, mqttExNewDialForServer(s, "one", "p"))
		}

		// Create SPOKE (leafnode) cluster.
		sysRemote := ""
		if connectSystemAccount {
			sysRemote = `{ url: "nats://admin:s3cr3t!@` + leafRemoteAddr + `", account: "$SYS" },` + "\n\t\t\t"
		}
		if leafd != "" {
			leafd = "domain: " + leafd + ", "
		}
		leafConf := `
	listen: 127.0.0.1:-1

	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, ` + leafd + `store_dir: '%s'}

	leafnodes {
		remotes = [
			` + sysRemote + `{ url: "nats://one:p@` + leafRemoteAddr + `", account: "ONE" },
		]
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
		spoke := createJetStreamCluster(tb, leafConf, "SPOKE", "SPOKE-", 3, 22111, false)
		expectedConnections := 2
		if !connectSystemAccount {
			expectedConnections = 1
		}
		for _, s := range spoke.servers {
			checkLeafNodeConnectedCount(tb, s, expectedConnections)
		}
		spoke.waitOnPeerCount(3)
		spokeRandom := mqttExNewDialForServer(spoke.randomNonLeader(), "one", "p")
		spokeAll := []mqttExDial{}
		for _, s := range spoke.servers {
			spokeAll = append(spokeAll, mqttExNewDialForServer(s, "one", "p"))
		}

		all := append(hubAll, spokeAll...)

		return &mqttExTarget{
			clusters: []*cluster{hub, spoke},
			all:      all,
			configs: []mqttExTestConfig{
				{name: "publish to all", pub: all, sub: all},
				{name: "publish to all hub", pub: hubAll, sub: all},
				{name: "publish to random in hub", pub: []mqttExDial{hubRandom}, sub: all},
				{name: "publish to all spoke", pub: spokeAll, sub: all},
				{name: "publish to random in spoke", pub: []mqttExDial{spokeRandom}, sub: all},
			},
		}
	}
}

var mqttexCLICommandPath = func() string {
	p := os.Getenv("MQTT_CLI")
	if p == "" {
		p, _ = exec.LookPath("mqtt")
	}
	return p
}()

var mqttexTestCommandPath = func() string {
	p, _ := exec.LookPath("mqtt-test")
	return p
}()

func mqttexRunTest(tb testing.TB, subCommand string, dial mqttExDial, extraArgs ...string) *MQTTBenchmarkResult {
	tb.Helper()

	if mqttexTestCommandPath == "" {
		tb.Skip(`"mqtt-test" command is not found in $PATH.`)
	}

	args := []string{subCommand, // "-q",
		"-s", string(dial),
	}
	args = append(args, extraArgs...)

	cmd := exec.Command(mqttexTestCommandPath, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		tb.Fatalf("Error executing %q: %v", cmd.String(), err)
	}
	defer stdout.Close()
	errbuf := bytes.Buffer{}
	cmd.Stderr = &errbuf
	if err = cmd.Start(); err != nil {
		tb.Fatalf("Error executing %q: %v", cmd.String(), err)
	}
	out, err := io.ReadAll(stdout)
	if err != nil {
		tb.Fatalf("Error executing %q: failed to read output: %v", cmd.String(), err)
	}
	if err = cmd.Wait(); err != nil {
		tb.Fatalf("Error executing %q: %v\n\n%s\n\n%s", cmd.String(), err, string(out), errbuf.String())
	}

	r := &MQTTBenchmarkResult{}
	if err := json.Unmarshal(out, r); err != nil {
		tb.Fatalf("Error executing %q: failed to decode output: %v\n\n%s\n\n%s", cmd.String(), err, string(out), errbuf.String())
	}
	return r
}
