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
	"testing"
	"time"
)

// Bounces the entire set of nodes, then brings them back up.
// Fail if some nodes don't come back online.
func TestJetStreamChaosClusterBounce(t *testing.T) {

	const duration = 60 * time.Second
	const clusterSize = 3

	c := createJetStreamClusterExplicit(t, "R3", clusterSize)
	defer c.shutdown()

	chaos := createClusterChaosMonkeyController(
		t,
		c,
		&clusterBouncerChaosMonkey{
			minDowntime:    0 * time.Second,
			maxDowntime:    2 * time.Second,
			minDownServers: clusterSize,
			maxDownServers: clusterSize,
			pause:          3 * time.Second,
		},
	)
	chaos.start()
	defer chaos.stop()

	<-time.After(duration)
}

// Bounces a subset of the nodes, then brings them back up.
// Fails if some nodes don't come back online.
func TestJetStreamChaosClusterBounceSubset(t *testing.T) {

	const duration = 60 * time.Second
	const clusterSize = 3

	c := createJetStreamClusterExplicit(t, "R3", clusterSize)
	defer c.shutdown()

	chaos := createClusterChaosMonkeyController(
		t,
		c,
		&clusterBouncerChaosMonkey{
			minDowntime:    0 * time.Second,
			maxDowntime:    2 * time.Second,
			minDownServers: 1,
			maxDownServers: clusterSize,
			pause:          3 * time.Second,
		},
	)
	chaos.start()
	defer chaos.stop()

	<-time.After(duration)
}
