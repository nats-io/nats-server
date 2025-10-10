// Copyright 2025 The NATS Authors
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
	"testing"

	"github.com/nats-io/nats.go"
)

// TestJetStreamClusterReplicaChangeAfterPlacementUpdate verifies that when a stream's
// placement.cluster field is updated to match the current cluster (e.g., after a cluster
// rename), subsequent replica changes should be allowed and not incorrectly treated as
// move requests.
func TestJetStreamClusterReplicaChangeAfterPlacementUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create a stream with R=1 and explicit placement
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
		Placement: &nats.Placement{
			Cluster: "R3S",
		},
	})
	require_NoError(t, err)

	// Verify initial state
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.Config.Replicas, 1)
	require_NotNil(t, si.Config.Placement)
	require_Equal(t, si.Config.Placement.Cluster, "R3S")

	// Simulate scenario: placement.cluster is already set to current cluster
	// (e.g., after cluster rename or auto-update), now try to change replicas.
	// This should NOT be treated as a move request.
	si, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3, // Scale up
		Placement: &nats.Placement{
			Cluster: "R3S", // Same cluster, should not be a move
		},
	})
	require_NoError(t, err)
	require_Equal(t, si.Config.Replicas, 3)

	// Wait for the stream to be ready with 3 replicas
	c.waitOnStreamLeader(globalAccountName, "TEST")

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.Cluster.Leader != _EMPTY_)
	require_Equal(t, len(si.Cluster.Replicas), 2) // Leader + 2 replicas = 3 total

	// Now scale down - should also work
	si, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1, // Scale down
		Placement: &nats.Placement{
			Cluster: "R3S",
		},
	})
	require_NoError(t, err)
	require_Equal(t, si.Config.Replicas, 1)

	// Verify final state
	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.Cluster.Leader != _EMPTY_)
	require_Equal(t, len(si.Cluster.Replicas), 0) // Just leader, no additional replicas
}

// TestJetStreamClusterReplicaChangeWithNilPlacement verifies that replica changes
// work correctly when placement starts as nil and we add placement matching current cluster.
func TestJetStreamClusterReplicaChangeWithNilPlacement(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create stream with R=1 and NO explicit placement
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST2",
		Subjects: []string{"bar"},
		Replicas: 1,
	})
	require_NoError(t, err)

	// Scale up with placement matching current cluster (simulates updating
	// placement.cluster to match current cluster name)
	si, err := js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST2",
		Subjects: []string{"bar"},
		Replicas: 3, // Scale up
		Placement: &nats.Placement{
			Cluster: "R3S", // Adding placement that matches current cluster
		},
	})
	require_NoError(t, err)
	require_Equal(t, si.Config.Replicas, 3)

	c.waitOnStreamLeader(globalAccountName, "TEST2")

	// Verify it scaled correctly
	si, err = js.StreamInfo("TEST2")
	require_NoError(t, err)
	require_True(t, si.Cluster.Leader != _EMPTY_)
	require_Equal(t, len(si.Cluster.Replicas), 2) // Leader + 2 replicas = 3 total
}

// Note: A test for the actual cluster rename scenario (updating placement.cluster from
// old to new name after a cluster rename) is not included because it would require
// simulating cluster gossip protocol to update nodeInfo. The fix correctly handles this
// by checking the live cluster name via clusterNameForNode() rather than stale metadata.
