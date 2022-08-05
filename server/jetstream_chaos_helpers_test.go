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

//go:build js_chaos_tests
// +build js_chaos_tests

package server

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// Additional cluster helpers

func (c *cluster) waitOnClusterHealthz() {
	c.t.Helper()
	for _, cs := range c.servers {
		c.waitOnServerHealthz(cs)
	}
}

func (c *cluster) stopSubset(toStop []*Server) {
	c.t.Helper()
	for _, s := range toStop {
		s.Shutdown()
	}
}

func (c *cluster) selectRandomServers(numServers int) []*Server {
	c.t.Helper()
	if numServers > len(c.servers) {
		panic(fmt.Sprintf("Can't select %d servers in a cluster of %d", numServers, len(c.servers)))
	}
	var selectedServers []*Server
	selectedServers = append(selectedServers, c.servers...)
	rand.Shuffle(len(selectedServers), func(x, y int) {
		selectedServers[x], selectedServers[y] = selectedServers[y], selectedServers[x]
	})
	return selectedServers[0:numServers]
}

// Support functions for "chaos" testing (random injected failures)

type ChaosMonkeyController interface {
	// Launch the monkey as background routine and return
	start()
	// Stop a monkey that was previously started
	stop()
	// Run the monkey synchronously, until it is manually stopped via stopCh
	run()
}

type ClusterChaosMonkey interface {
	// Set defaults and validates the monkey parameters
	validate(t *testing.T, c *cluster)
	// Run the monkey synchronously, until it is manually stopped via stopCh
	run(t *testing.T, c *cluster, stopCh <-chan bool)
}

// Chaos Monkey Controller that acts on a cluster
type clusterChaosMonkeyController struct {
	t       *testing.T
	cluster *cluster
	wg      sync.WaitGroup
	stopCh  chan bool
	ccm     ClusterChaosMonkey
}

func createClusterChaosMonkeyController(t *testing.T, c *cluster, ccm ClusterChaosMonkey) ChaosMonkeyController {
	ccm.validate(t, c)
	return &clusterChaosMonkeyController{
		t:       t,
		cluster: c,
		stopCh:  make(chan bool, 3),
		ccm:     ccm,
	}
}

func (m *clusterChaosMonkeyController) start() {
	m.t.Logf("🐵 Starting monkey")
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.run()
	}()
}

func (m *clusterChaosMonkeyController) stop() {
	m.t.Logf("🐵 Stopping monkey")
	m.stopCh <- true
	m.wg.Wait()
	m.t.Logf("🐵 Monkey stopped")
}

func (m *clusterChaosMonkeyController) run() {
	m.ccm.run(m.t, m.cluster, m.stopCh)
}

// Cluster Chaos Monkey that selects a random subset of the nodes in a cluster (according to min/max provided),
// shuts them down for a given duration (according to min/max provided), then brings them back up.
// Then sleeps for a given time, and does it again until stopped.
type clusterBouncerChaosMonkey struct {
	minDowntime    time.Duration
	maxDowntime    time.Duration
	minDownServers int
	maxDownServers int
	pause          time.Duration
}

func (m *clusterBouncerChaosMonkey) validate(t *testing.T, c *cluster) {
	if m.minDowntime > m.maxDowntime {
		t.Fatalf("Min downtime %v cannot be larger than max downtime %v", m.minDowntime, m.maxDowntime)
	}

	if m.minDownServers > m.maxDownServers {
		t.Fatalf("Min down servers %v cannot be larger than max down servers %v", m.minDownServers, m.maxDownServers)
	}
}

func (m *clusterBouncerChaosMonkey) run(t *testing.T, c *cluster, stopCh <-chan bool) {
	for {
		// Pause between actions
		select {
		case <-stopCh:
			return
		case <-time.After(m.pause):
		}

		// Pick a random subset of servers
		numServersDown := rand.Intn(1+m.maxDownServers-m.minDownServers) + m.minDownServers
		servers := c.selectRandomServers(numServersDown)
		serverNames := []string{}
		for _, s := range servers {
			serverNames = append(serverNames, s.info.Name)
		}

		// Pick a random outage interval
		minOutageNanos := m.minDowntime.Nanoseconds()
		maxOutageNanos := m.maxDowntime.Nanoseconds()
		outageDurationNanos := rand.Int63n(1+maxOutageNanos-minOutageNanos) + minOutageNanos
		outageDuration := time.Duration(outageDurationNanos)

		// Take down selected servers
		t.Logf("🐵 Taking down %d/%d servers for %v (%v)", numServersDown, len(c.servers), outageDuration, serverNames)
		c.stopSubset(servers)

		// Wait for the "outage" duration
		select {
		case <-stopCh:
			return
		case <-time.After(outageDuration):
		}

		// Restart servers and wait for cluster to be healthy
		t.Logf("🐵 Restoring cluster")
		c.restartAllSamePorts()
		c.waitOnClusterHealthz()

		c.waitOnClusterReady()
		c.waitOnAllCurrent()
		c.waitOnLeader()
	}
}
