// Copyright 2020-2025 The NATS Authors
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

//go:build !skip_js_tests && !skip_js_cluster_tests && !skip_js_cluster_tests_2 && !skip_js_super_cluster_tests

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

func TestJetStreamSuperClusterMetaStepDown(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()
	sc.waitOnLeader()

	// Client based API
	s := sc.randomServer()
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer nc.Close()

	stepdown := func(preferred, cn string, tags []string) *JSApiLeaderStepDownResponse {
		jreq, err := json.Marshal(&JSApiLeaderStepdownRequest{
			Placement: &Placement{
				Cluster:   cn,
				Tags:      tags,
				Preferred: preferred,
			},
		})
		require_NoError(t, err)

		resp, err := nc.Request(JSApiLeaderStepDown, jreq, time.Second)
		require_NoError(t, err)

		var sdr JSApiLeaderStepDownResponse
		require_NoError(t, json.Unmarshal(resp.Data, &sdr))
		return &sdr
	}

	// Make sure we get correct errors for clusters we don't know about.
	t.Run("UnknownCluster", func(t *testing.T) {
		sdr := stepdown(_EMPTY_, "ThisClusterDoesntExist", nil)
		require_NotNil(t, sdr.Error)
		require_Equal(t, sdr.Error.Code, 400)
		require_Equal(t, ErrorIdentifier(sdr.Error.ErrCode), JSClusterNoPeersErrF)
	})

	// Make sure we get correct errors for servers we don't know about.
	t.Run("UnknownPreferredServer", func(t *testing.T) {
		sdr := stepdown("ThisServerDoesntExist", _EMPTY_, nil)
		require_NotNil(t, sdr.Error)
		require_Equal(t, sdr.Error.Code, 400)
		require_Equal(t, ErrorIdentifier(sdr.Error.ErrCode), JSClusterNoPeersErrF)
	})

	// Make sure we get correct errors for tags that don't match any servers.
	t.Run("UnknownTag", func(t *testing.T) {
		sdr := stepdown(_EMPTY_, _EMPTY_, []string{"thistag:doesntexist"})
		require_NotNil(t, sdr.Error)
		require_Equal(t, sdr.Error.Code, 400)
		require_Equal(t, ErrorIdentifier(sdr.Error.ErrCode), JSClusterNoPeersErrF)
	})

	// Make sure we get correct errors for servers that are already leader.
	t.Run("PreferredServerAlreadyLeader", func(t *testing.T) {
		ml := sc.leader()
		require_NotNil(t, ml)

		sdr := stepdown(ml.Name(), _EMPTY_, nil)
		require_NotNil(t, sdr.Error)
		require_Equal(t, sdr.Error.Code, 400)
		require_Equal(t, ErrorIdentifier(sdr.Error.ErrCode), JSClusterNoPeersErrF)
	})

	// Make sure we get correct errors for tags and bad or unavailable cluster placement.
	t.Run("PlacementByPreferredServer", func(t *testing.T) {
		ml := sc.leader()
		require_NotNil(t, ml)

		var preferredServer string
	clusters:
		for _, c := range sc.clusters {
			for _, s := range c.servers {
				if s == ml {
					continue
				}
				preferredServer = s.Name()
				break clusters
			}
		}

		sdr := stepdown(preferredServer, _EMPTY_, nil)
		require_Equal(t, sdr.Error, nil)

		sc.waitOnLeader()
		ml = sc.leader()
		require_Equal(t, ml.Name(), preferredServer)
	})

	// Influence the placement by using the cluster name.
	t.Run("PlacementByCluster", func(t *testing.T) {
		ml := sc.leader()
		require_NotNil(t, ml)

		cn := ml.ClusterName()
		var pcn string
		for _, c := range sc.clusters {
			if c.name != cn {
				pcn = c.name
				break
			}
		}

		sdr := stepdown(_EMPTY_, pcn, nil)
		require_Equal(t, sdr.Error, nil)

		sc.waitOnLeader()
		ml = sc.leader()
		require_NotNil(t, ml)
		require_Equal(t, ml.ClusterName(), pcn)
	})

	// Influence the placement by using tag names.
	t.Run("PlacementByTag", func(t *testing.T) {
		ml := sc.leader()
		require_NotNil(t, ml)

		// Work out what the tags are of the current leader, so we can pick
		// different ones.
		possibleTags := map[string]struct{}{
			"cloud:aws": {},
			"cloud:gcp": {},
			"cloud:az":  {},
		}
		// Remove the current leader's tags from the list.
		for _, tag := range ml.getOpts().Tags {
			delete(possibleTags, tag)
		}
		// Now pick the first tag as our new chosen tag.
		var chosenTag string
		for tag := range possibleTags {
			chosenTag = tag
			break
		}

		sdr := stepdown(_EMPTY_, _EMPTY_, []string{chosenTag})
		require_Equal(t, sdr.Error, nil)

		sc.waitOnLeader()
		ml = sc.leader()
		require_NotNil(t, ml)
		require_True(t, ml.getOpts().Tags.Contains(chosenTag))
	})

	// Influence the placement by using tag names, we need to match all of them.
	t.Run("PlacementByMultipleTags", func(t *testing.T) {
		ml := sc.leader()
		require_NotNil(t, ml)

		// Work out what the tags are of the current leader, so we can pick
		// different ones.
		possibleTags := map[string]struct{}{
			"cloud:aws": {},
			"cloud:gcp": {},
			"cloud:az":  {},
		}
		// Remove the current leader's tags from the list.
		for _, tag := range ml.getOpts().Tags {
			delete(possibleTags, tag)
		}
		// Now pick the first tag as our new chosen tag.
		var chosenTag string
		for tag := range possibleTags {
			chosenTag = tag
			break
		}

		sdr := stepdown(_EMPTY_, _EMPTY_, []string{chosenTag, "node:1"})
		require_Equal(t, sdr.Error, nil)

		sc.waitOnLeader()
		ml = sc.leader()
		require_NotNil(t, ml)
		require_True(t, ml.getOpts().Tags.Contains(chosenTag))
		require_True(t, ml.getOpts().Tags.Contains("node:1"))
	})

	// Influence the placement by using the cluster name and a tag.
	t.Run("PlacementByClusterAndTag", func(t *testing.T) {
		ml := sc.leader()
		require_NotNil(t, ml)

		cn := ml.ClusterName()
		var pcn string
		for _, c := range sc.clusters {
			if c.name != cn {
				pcn = c.name
				break
			}
		}

		sdr := stepdown(_EMPTY_, pcn, []string{"node:1"})
		require_Equal(t, sdr.Error, nil)

		sc.waitOnLeader()
		ml = sc.leader()
		require_NotNil(t, ml)
		require_Equal(t, ml.ClusterName(), pcn)
		require_True(t, ml.getOpts().Tags.Contains("node:1"))
	})
}

func TestJetStreamSuperClusterStreamStepDown(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()

	sc.waitOnLeader()
	s := sc.randomServer()

	nc, js := jsClientConnectNewAPI(t, s)
	defer nc.Close()
	ctx := context.Background()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "foo",
		Subjects:  []string{"foo.>"},
		Replicas:  3,
		Retention: jetstream.InterestPolicy,
		Placement: &jetstream.Placement{
			Cluster: "C1",
			Tags:    []string{"cloud:aws"},
		},
	})
	require_NoError(t, err)

	stepdown := func(preferred, cn string, tags []string) *JSApiStreamLeaderStepDownResponse {
		jreq, err := json.Marshal(&JSApiLeaderStepdownRequest{
			Placement: &Placement{
				Cluster:   cn,
				Tags:      tags,
				Preferred: preferred,
			},
		})
		require_NoError(t, err)

		resp, err := nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "foo"), jreq, time.Second)
		require_NoError(t, err)

		var sdr JSApiStreamLeaderStepDownResponse
		require_NoError(t, json.Unmarshal(resp.Data, &sdr))
		return &sdr
	}

	// Make sure we get correct errors for clusters we don't know about.
	t.Run("UnknownCluster", func(t *testing.T) {
		sdr := stepdown(_EMPTY_, "ThisClusterDoesntExist", nil)
		require_NotNil(t, sdr.Error)
		require_Equal(t, sdr.Error.Code, 400)
		require_Equal(t, ErrorIdentifier(sdr.Error.ErrCode), JSClusterNoPeersErrF)
	})

	// Make sure we get correct errors for servers we don't know about.
	t.Run("UnknownPreferredServer", func(t *testing.T) {
		sdr := stepdown("ThisServerDoesntExist", _EMPTY_, nil)
		require_NotNil(t, sdr.Error)
		require_Equal(t, sdr.Error.Code, 400)
		require_Equal(t, ErrorIdentifier(sdr.Error.ErrCode), JSClusterNoPeersErrF)
	})

	// Make sure we get correct errors for tags that don't match any servers.
	t.Run("UnknownTag", func(t *testing.T) {
		sdr := stepdown(_EMPTY_, _EMPTY_, []string{"thistag:doesntexist"})
		require_NotNil(t, sdr.Error)
		require_Equal(t, sdr.Error.Code, 400)
		require_Equal(t, ErrorIdentifier(sdr.Error.ErrCode), JSClusterNoPeersErrF)
	})

	// Make sure we get correct errors for clusters that we aren't assigned to.
	// The asset is in C1, not C2, so placement should fail.
	t.Run("NonParticipantCluster", func(t *testing.T) {
		sdr := stepdown(_EMPTY_, "C2", nil)
		require_NotNil(t, sdr.Error)
		require_Equal(t, sdr.Error.Code, 400)
		require_Equal(t, ErrorIdentifier(sdr.Error.ErrCode), JSClusterNoPeersErrF)
	})

	// Make sure we get correct errors for servers that are already leader.
	t.Run("PreferredServerAlreadyLeader", func(t *testing.T) {
		sl := sc.clusterForName("C1").streamLeader(globalAccountName, "foo")
		require_NotNil(t, sl)

		sdr := stepdown(sl.Name(), _EMPTY_, nil)
		require_NotNil(t, sdr.Error)
		require_Equal(t, sdr.Error.Code, 400)
		require_Equal(t, ErrorIdentifier(sdr.Error.ErrCode), JSClusterNoPeersErrF)
	})

	// Make sure we get correct errors for tags and bad or unavailable cluster placement.
	t.Run("PlacementByPreferredServer", func(t *testing.T) {
		c := sc.clusterForName("C1")
		sl := c.streamLeader(globalAccountName, "foo")
		require_NotNil(t, sl)

		var preferredServer string
		for _, s := range c.servers {
			if s == sl {
				continue
			}
			preferredServer = s.Name()
			break
		}

		sdr := stepdown(preferredServer, _EMPTY_, nil)
		require_Equal(t, sdr.Error, nil)

		c.waitOnStreamLeader(globalAccountName, "foo")
		sl = c.streamLeader(globalAccountName, "foo")
		require_Equal(t, sl.Name(), preferredServer)
	})

	// Influence the placement by using the cluster name. For streams this doesn't really
	// make sense, since the stream can only exist in a single cluster (the one that it
	// had its placement in), so this effectively works the same as specifying a stepdown
	// without a cluster name specified. Let's just make sure it does the right thing in
	// any case.
	t.Run("PlacementByCluster", func(t *testing.T) {
		c := sc.clusterForName("C1")
		sl := c.streamLeader(globalAccountName, "foo")
		require_NotNil(t, sl)

		sdr := stepdown(_EMPTY_, "C1", nil)
		require_Equal(t, sdr.Error, nil)

		c.waitOnStreamLeader(globalAccountName, "foo")
		sl = c.streamLeader(globalAccountName, "foo")
		require_NotNil(t, sl)
		require_Equal(t, sl.ClusterName(), "C1")
	})

	// Influence the placement by using tag names.
	t.Run("PlacementByTag", func(t *testing.T) {
		c := sc.clusterForName("C1")
		sl := c.streamLeader(globalAccountName, "foo")
		require_NotNil(t, sl)

		// Work out what the tags are of the current leader, so we can pick
		// different ones.
		possibleTags := map[string]struct{}{
			"node:1": {},
			"node:2": {},
			"node:3": {},
		}
		// Remove the current leader's tags from the list.
		for _, tag := range sl.getOpts().Tags {
			delete(possibleTags, tag)
		}
		// Now pick the first tag as our new chosen tag.
		var chosenTag string
		for tag := range possibleTags {
			chosenTag = tag
			break
		}

		sdr := stepdown(_EMPTY_, _EMPTY_, []string{chosenTag})
		require_Equal(t, sdr.Error, nil)

		c.waitOnStreamLeader(globalAccountName, "foo")
		sl = c.streamLeader(globalAccountName, "foo")
		require_NotNil(t, sl)
		require_True(t, sl.getOpts().Tags.Contains(chosenTag))
	})

	// Influence the placement by using tag names, we need to match all of them.
	t.Run("PlacementByMultipleTags", func(t *testing.T) {
		c := sc.clusterForName("C1")
		sl := c.streamLeader(globalAccountName, "foo")
		require_NotNil(t, sl)

		// Work out what the tags are of the current leader, so we can pick
		// different ones.
		possibleTags := map[string]struct{}{
			"node:1": {},
			"node:2": {},
			"node:3": {},
		}
		// Remove the current leader's tags from the list.
		for _, tag := range sl.getOpts().Tags {
			delete(possibleTags, tag)
		}
		// Now pick the first tag as our new chosen tag.
		var chosenTag string
		for tag := range possibleTags {
			chosenTag = tag
			break
		}

		sdr := stepdown(_EMPTY_, _EMPTY_, []string{chosenTag, "cloud:aws"})
		require_Equal(t, sdr.Error, nil)

		c.waitOnStreamLeader(globalAccountName, "foo")
		sl = c.streamLeader(globalAccountName, "foo")
		require_NotNil(t, sl)
		require_True(t, sl.getOpts().Tags.Contains(chosenTag))
		require_True(t, sl.getOpts().Tags.Contains("cloud:aws"))
	})

	// Influence the placement by using the cluster name and a tag. Like with
	// PlacementByCluster above, the cluster portion of this request is not really
	// doing anything, but it's useful just to ensure the API behaves properly.
	t.Run("PlacementByClusterAndTag", func(t *testing.T) {
		c := sc.clusterForName("C1")
		sl := c.streamLeader(globalAccountName, "foo")
		require_NotNil(t, sl)

		// Work out what the tags are of the current leader, so we can pick
		// different ones.
		possibleTags := map[string]struct{}{
			"node:1": {},
			"node:2": {},
			"node:3": {},
		}
		// Remove the current leader's tags from the list.
		for _, tag := range sl.getOpts().Tags {
			delete(possibleTags, tag)
		}
		// Now pick the first tag as our new chosen tag.
		var chosenTag string
		for tag := range possibleTags {
			chosenTag = tag
			break
		}

		sdr := stepdown(_EMPTY_, "C1", []string{chosenTag, "cloud:aws"})
		require_Equal(t, sdr.Error, nil)

		c.waitOnStreamLeader(globalAccountName, "foo")
		sl = c.streamLeader(globalAccountName, "foo")
		require_NotNil(t, sl)
		require_True(t, sl.getOpts().Tags.Contains(chosenTag))
		require_True(t, sl.getOpts().Tags.Contains("cloud:aws"))
		require_Equal(t, sl.ClusterName(), "C1")
	})
}

func TestJetStreamSuperClusterConsumerStepDown(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()

	sc.waitOnLeader()
	s := sc.randomServer()

	nc, js := jsClientConnectNewAPI(t, s)
	defer nc.Close()
	ctx := context.Background()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "foo",
		Subjects:  []string{"foo.>"},
		Replicas:  3,
		Retention: jetstream.InterestPolicy,
		Placement: &jetstream.Placement{
			Cluster: "C1",
			Tags:    []string{"cloud:aws"},
		},
	})
	require_NoError(t, err)

	_, err = js.CreateConsumer(ctx, "foo", jetstream.ConsumerConfig{
		Name:      "consumer",
		AckPolicy: jetstream.AckNonePolicy,
	})
	require_NoError(t, err)

	stepdown := func(preferred, cn string, tags []string) *JSApiStreamLeaderStepDownResponse {
		jreq, err := json.Marshal(&JSApiLeaderStepdownRequest{
			Placement: &Placement{
				Cluster:   cn,
				Tags:      tags,
				Preferred: preferred,
			},
		})
		require_NoError(t, err)

		resp, err := nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "foo", "consumer"), jreq, time.Second)
		require_NoError(t, err)

		var sdr JSApiStreamLeaderStepDownResponse
		require_NoError(t, json.Unmarshal(resp.Data, &sdr))
		return &sdr
	}

	// Make sure we get correct errors for clusters we don't know about.
	t.Run("UnknownCluster", func(t *testing.T) {
		sdr := stepdown(_EMPTY_, "ThisClusterDoesntExist", nil)
		require_NotNil(t, sdr.Error)
		require_Equal(t, sdr.Error.Code, 400)
		require_Equal(t, ErrorIdentifier(sdr.Error.ErrCode), JSClusterNoPeersErrF)
	})

	// Make sure we get correct errors for servers we don't know about.
	t.Run("UnknownPreferredServer", func(t *testing.T) {
		sdr := stepdown("ThisServerDoesntExist", _EMPTY_, nil)
		require_NotNil(t, sdr.Error)
		require_Equal(t, sdr.Error.Code, 400)
		require_Equal(t, ErrorIdentifier(sdr.Error.ErrCode), JSClusterNoPeersErrF)
	})

	// Make sure we get correct errors for tags that don't match any servers.
	t.Run("UnknownTag", func(t *testing.T) {
		sdr := stepdown(_EMPTY_, _EMPTY_, []string{"thistag:doesntexist"})
		require_NotNil(t, sdr.Error)
		require_Equal(t, sdr.Error.Code, 400)
		require_Equal(t, ErrorIdentifier(sdr.Error.ErrCode), JSClusterNoPeersErrF)
	})

	// Make sure we get correct errors for clusters that we aren't assigned to.
	// The asset is in C1, not C2, so placement should fail.
	t.Run("NonParticipantCluster", func(t *testing.T) {
		sdr := stepdown(_EMPTY_, "C2", nil)
		require_NotNil(t, sdr.Error)
		require_Equal(t, sdr.Error.Code, 400)
		require_Equal(t, ErrorIdentifier(sdr.Error.ErrCode), JSClusterNoPeersErrF)
	})

	// Make sure we get correct errors for servers that are already leader.
	t.Run("PreferredServerAlreadyLeader", func(t *testing.T) {
		cl := sc.clusterForName("C1").consumerLeader(globalAccountName, "foo", "consumer")
		require_NotNil(t, cl)

		sdr := stepdown(cl.Name(), _EMPTY_, nil)
		require_NotNil(t, sdr.Error)
		require_Equal(t, sdr.Error.Code, 400)
		require_Equal(t, ErrorIdentifier(sdr.Error.ErrCode), JSClusterNoPeersErrF)
	})

	// Make sure we get correct errors for tags and bad or unavailable cluster placement.
	t.Run("PlacementByPreferredServer", func(t *testing.T) {
		c := sc.clusterForName("C1")
		cl := sc.clusterForName("C1").consumerLeader(globalAccountName, "foo", "consumer")
		require_NotNil(t, cl)

		var preferredServer string
		for _, s := range c.servers {
			if s == cl {
				continue
			}
			preferredServer = s.Name()
			break
		}

		sdr := stepdown(preferredServer, _EMPTY_, nil)
		require_Equal(t, sdr.Error, nil)

		c.waitOnConsumerLeader(globalAccountName, "foo", "consumer")
		checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
			if cl = sc.clusterForName("C1").consumerLeader(globalAccountName, "foo", "consumer"); cl == nil {
				return fmt.Errorf("consumer leader is nil")
			}
			if cl.Name() != preferredServer {
				return fmt.Errorf("consumer leader %q is not preferred %q", cl.Name(), preferredServer)
			}
			return nil
		})
	})

	// Influence the placement by using the cluster name. For consumers this doesn't really
	// make sense, since the consumers can only exist in a single cluster (the one that it
	// had its placement in), so this effectively works the same as specifying a stepdown
	// without a cluster name specified. Let's just make sure it does the right thing in
	// any case.
	t.Run("PlacementByCluster", func(t *testing.T) {
		c := sc.clusterForName("C1")
		cl := sc.clusterForName("C1").consumerLeader(globalAccountName, "foo", "consumer")
		require_NotNil(t, cl)

		sdr := stepdown(_EMPTY_, "C1", nil)
		require_Equal(t, sdr.Error, nil)

		c.waitOnConsumerLeader(globalAccountName, "foo", "consumer")
		checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
			if cl = sc.clusterForName("C1").consumerLeader(globalAccountName, "foo", "consumer"); cl == nil {
				return fmt.Errorf("consumer leader is nil")
			}
			if cl.ClusterName() != "C1" {
				return fmt.Errorf("consumer leader %q is not in cluster C1", cl.Name())
			}
			return nil
		})
	})

	// Influence the placement by using tag names.
	t.Run("PlacementByTag", func(t *testing.T) {
		c := sc.clusterForName("C1")
		cl := sc.clusterForName("C1").consumerLeader(globalAccountName, "foo", "consumer")
		require_NotNil(t, cl)

		// Work out what the tags are of the current leader, so we can pick
		// different ones.
		possibleTags := map[string]struct{}{
			"node:1": {},
			"node:2": {},
			"node:3": {},
		}
		// Remove the current leader's tags from the list.
		for _, tag := range cl.getOpts().Tags {
			delete(possibleTags, tag)
		}
		// Now pick the first tag as our new chosen tag.
		var chosenTag string
		for tag := range possibleTags {
			chosenTag = tag
			break
		}

		sdr := stepdown(_EMPTY_, _EMPTY_, []string{chosenTag})
		require_Equal(t, sdr.Error, nil)

		c.waitOnConsumerLeader(globalAccountName, "foo", "consumer")
		checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
			if cl = sc.clusterForName("C1").consumerLeader(globalAccountName, "foo", "consumer"); cl == nil {
				return fmt.Errorf("consumer leader is nil")
			}
			if !cl.getOpts().Tags.Contains(chosenTag) {
				return fmt.Errorf("consumer leader %q doesn't contain tag %q", cl.Name(), chosenTag)
			}
			return nil
		})
	})

	// Influence the placement by using tag names, we need to match all of them.
	t.Run("PlacementByMultipleTags", func(t *testing.T) {
		c := sc.clusterForName("C1")
		cl := sc.clusterForName("C1").consumerLeader(globalAccountName, "foo", "consumer")
		require_NotNil(t, cl)

		// Work out what the tags are of the current leader, so we can pick
		// different ones.
		possibleTags := map[string]struct{}{
			"node:1": {},
			"node:2": {},
			"node:3": {},
		}
		// Remove the current leader's tags from the list.
		for _, tag := range cl.getOpts().Tags {
			delete(possibleTags, tag)
		}
		// Now pick the first tag as our new chosen tag.
		var chosenTag string
		for tag := range possibleTags {
			chosenTag = tag
			break
		}

		sdr := stepdown(_EMPTY_, _EMPTY_, []string{chosenTag, "cloud:aws"})
		require_Equal(t, sdr.Error, nil)

		c.waitOnConsumerLeader(globalAccountName, "foo", "consumer")
		checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
			if cl = sc.clusterForName("C1").consumerLeader(globalAccountName, "foo", "consumer"); cl == nil {
				return fmt.Errorf("consumer leader is nil")
			}
			if !cl.getOpts().Tags.Contains(chosenTag) {
				return fmt.Errorf("consumer leader %q doesn't contain tag %q", cl.Name(), chosenTag)
			}
			if !cl.getOpts().Tags.Contains("cloud:aws") {
				return fmt.Errorf("consumer leader %q isn't in cloud:aws", cl.Name())
			}
			return nil
		})
	})

	// Influence the placement by using the cluster name and a tag. Like with
	// PlacementByCluster above, the cluster portion of this request is not really
	// doing anything, but it's useful just to ensure the API behaves properly.
	t.Run("PlacementByClusterAndTag", func(t *testing.T) {
		c := sc.clusterForName("C1")
		cl := sc.clusterForName("C1").consumerLeader(globalAccountName, "foo", "consumer")
		require_NotNil(t, cl)

		// Work out what the tags are of the current leader, so we can pick
		// different ones.
		possibleTags := map[string]struct{}{
			"node:1": {},
			"node:2": {},
			"node:3": {},
		}
		// Remove the current leader's tags from the list.
		for _, tag := range cl.getOpts().Tags {
			delete(possibleTags, tag)
		}
		// Now pick the first tag as our new chosen tag.
		var chosenTag string
		for tag := range possibleTags {
			chosenTag = tag
			break
		}

		sdr := stepdown(_EMPTY_, "C1", []string{chosenTag, "cloud:aws"})
		require_Equal(t, sdr.Error, nil)

		c.waitOnConsumerLeader(globalAccountName, "foo", "consumer")
		cl = sc.clusterForName("C1").consumerLeader(globalAccountName, "foo", "consumer")

		checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
			if cl = sc.clusterForName("C1").consumerLeader(globalAccountName, "foo", "consumer"); cl == nil {
				return fmt.Errorf("consumer leader is nil")
			}
			if cl.ClusterName() != "C1" {
				return fmt.Errorf("consumer leader %q is not in cluster C1", cl.Name())
			}
			if !cl.getOpts().Tags.Contains(chosenTag) {
				return fmt.Errorf("consumer leader %q doesn't contain tag %q", cl.Name(), chosenTag)
			}
			if !cl.getOpts().Tags.Contains("cloud:aws") {
				return fmt.Errorf("consumer leader %q isn't in cloud:aws", cl.Name())
			}
			return nil
		})
	})
}

func TestJetStreamSuperClusterUniquePlacementTag(t *testing.T) {
	tmlp := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s', unique_tag: az}
		leaf {listen: 127.0.0.1:-1}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	s := createJetStreamSuperClusterWithTemplateAndModHook(t, tmlp, 5, 2,
		func(serverName, clustername, storeDir, conf string) string {
			azTag := map[string]string{
				"C1-S1": "az:same",
				"C1-S2": "az:same",
				"C1-S3": "az:same",
				"C1-S4": "az:same",
				"C1-S5": "az:same",
				"C2-S1": "az:1",
				"C2-S2": "az:2",
				"C2-S3": "az:1",
				"C2-S4": "az:2",
				"C2-S5": "az:1",
			}
			return conf + fmt.Sprintf("\nserver_tags: [cloud:%s-tag, %s]\n", clustername, azTag[serverName])
		}, nil)
	defer s.shutdown()

	inDifferentAz := func(ci *jetstream.ClusterInfo) (bool, error) {
		t.Helper()
		if len(ci.Replicas) == 0 {
			return true, nil
		}
		// if R2 (has replica, this setup does not support R3), test if the server in a cluster picked the same az,
		// as determined by modulo2 of server number which aligns with az
		dummy := 0
		srvnum1 := 0
		srvnum2 := 0
		if n, _ := fmt.Sscanf(ci.Leader, "C%d-S%d", &dummy, &srvnum1); n != 2 {
			return false, fmt.Errorf("couldn't parse leader")
		}
		if n, _ := fmt.Sscanf(ci.Replicas[0].Name, "C%d-S%d", &dummy, &srvnum2); n != 2 {
			return false, fmt.Errorf("couldn't parse replica")
		}
		return srvnum1%2 != srvnum2%2, nil
	}

	nc := natsConnect(t, s.randomServer().ClientURL())
	defer nc.Close()

	js, err := jetstream.New(nc, jetstream.WithDefaultTimeout(10*time.Second))
	require_NoError(t, err)
	ctx := context.Background()

	for i, test := range []struct {
		placement *jetstream.Placement
		replicas  int
		fail      bool
		cluster   string
	}{
		// these pass because replica count is 1
		{&jetstream.Placement{Tags: []string{"az:same"}}, 1, false, "C1"},
		{&jetstream.Placement{Tags: []string{"cloud:C1-tag", "az:same"}}, 1, false, "C1"},
		{&jetstream.Placement{Tags: []string{"cloud:C1-tag"}}, 1, false, "C1"},
		// pass because az is set, which disables the filter
		{&jetstream.Placement{Tags: []string{"az:same"}}, 2, false, "C1"},
		{&jetstream.Placement{Tags: []string{"cloud:C1-tag", "az:same"}}, 2, false, "C1"},
		// fails because this cluster only has the same az
		{&jetstream.Placement{Tags: []string{"cloud:C1-tag"}}, 2, true, ""},
		// fails because no 3 unique tags exist
		{&jetstream.Placement{Tags: []string{"cloud:C2-tag"}}, 3, true, ""},
		{nil, 3, true, ""},
		// pass because replica count is low enough
		{nil, 2, false, "C2"},
		{&jetstream.Placement{Tags: []string{"cloud:C2-tag"}}, 2, false, "C2"},
		// pass because az is provided
		{&jetstream.Placement{Tags: []string{"az:1"}}, 3, false, "C2"},
		{&jetstream.Placement{Tags: []string{"az:2"}}, 2, false, "C2"},
	} {
		name := fmt.Sprintf("test-%d", i)
		t.Run(name, func(t *testing.T) {
			si, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Replicas: test.replicas, Placement: test.placement})
			if test.fail {
				require_Error(t, err)
				require_Contains(t, err.Error(), "no suitable peers for placement", "server tag not unique")
				return
			}
			require_NoError(t, err)
			if test.cluster != _EMPTY_ {
				require_Equal(t, si.CachedInfo().Cluster.Name, test.cluster)
			}
			// skip placement test if tags call for a particular az
			if test.placement != nil && len(test.placement.Tags) > 0 {
				for _, tag := range test.placement.Tags {
					if strings.HasPrefix(tag, "az:") {
						return
					}
				}
			}
			diff, err := inDifferentAz(si.CachedInfo().Cluster)
			require_NoError(t, err)
			require_True(t, diff)
		})
	}

	t.Run("scale-up-test", func(t *testing.T) {
		// create enough streams so we hit it eventually
		for i := 0; i < 10; i++ {
			cfg := jetstream.StreamConfig{Name: fmt.Sprintf("scale-up-%d", i), Replicas: 1,
				Placement: &jetstream.Placement{Tags: []string{"cloud:C2-tag"}}}
			si, err := js.CreateStream(ctx, cfg)
			require_NoError(t, err)
			require_Equal(t, si.CachedInfo().Cluster.Name, "C2")
			s.waitOnStreamLeader(globalAccountName, cfg.Name)
			cfg.Replicas = 2
			si, err = js.UpdateStream(ctx, cfg)
			require_NoError(t, err)
			require_Equal(t, si.CachedInfo().Cluster.Name, "C2")
			checkFor(t, 10, 250*time.Millisecond, func() error {
				if sh, err := js.Stream(ctx, cfg.Name); err != nil {
					return err
				} else if info, err := sh.Info(ctx); err != nil {
					return err
				} else if diff, err := inDifferentAz(info.Cluster); err != nil {
					return err
				} else if !diff {
					return fmt.Errorf("not in different AZ")
				}
				return nil
			})
		}
	})
}

func TestJetStreamSuperClusterBasics(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// Client based API
	s := sc.randomServer()
	nc, js := jsClientConnectNewAPI(t, s)
	defer nc.Close()
	ctx := context.Background()

	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish(ctx, "TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	// Now grab info for this stream.
	si, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}
	// Check request origin placement.
	if si.Cluster.Name != s.ClusterName() {
		t.Fatalf("Expected stream to be placed in %q, but got %q", s.ClusterName(), si.Cluster.Name)
	}

	// Check consumers.
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_ = fetchJetStreamMsgs(t, cons, toSend, 5*time.Second)
	ci, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.Delivered.Consumer != uint64(toSend) || ci.NumAckPending != toSend {
		t.Fatalf("ConsumerInfo is not correct: %+v", ci)
	}

	// Now check we can place a stream.
	pcn := "C3"
	scStream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "TEST2",
		Placement: &jetstream.Placement{Cluster: pcn},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	scResp := scStream.CachedInfo()

	if scResp.Cluster.Name != pcn {
		t.Fatalf("Expected the stream to be placed in %q, got %q", pcn, scResp.Cluster.Name)
	}
}

// Test that consumer interest across gateways and superclusters is properly identitifed in a remote cluster.
func TestJetStreamSuperClusterCrossClusterConsumerInterest(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// Since we need all of the peers accounted for to add the stream wait for all to be present.
	sc.waitOnPeerCount(9)

	// Client based API - Connect to Cluster C1. Stream and consumer will live in C2.
	s := sc.clusterForName("C1").randomServer()
	nc, js := jsClientConnectNewAPI(t, s)
	defer nc.Close()
	ctx := context.Background()

	pcn := "C2"
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Replicas: 3, Placement: &jetstream.Placement{Cluster: pcn}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Pull based first.
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "dlc",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send a message.
	if _, err = js.Publish(ctx, "foo", []byte("CCI")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	fetchJetStreamMsgs(t, cons, 1, 5*time.Second)

	// Now check push based delivery.
	cons2, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "rip",
		AckPolicy: jetstream.AckNonePolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		ci, err := cons2.Info(ctx)
		if err != nil {
			return err
		}
		if ci.NumPending != 1 {
			return fmt.Errorf("expected 1 pending, got %d", ci.NumPending)
		}
		return nil
	})

	// Send another message.
	if _, err = js.Publish(ctx, "foo", []byte("CCI")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		ci, err := cons2.Info(ctx)
		if err != nil {
			return err
		}
		if ci.NumPending != 2 {
			return fmt.Errorf("expected 2 pending, got %d", ci.NumPending)
		}
		return nil
	})
}

func TestJetStreamSuperClusterPeerReassign(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// Client based API
	s := sc.randomServer()
	nc, js := jsClientConnectNewAPI(t, s)
	defer nc.Close()
	ctx := context.Background()

	pcn := "C2"

	// Create a stream in C2 that sources TEST
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "TEST",
		Placement: &jetstream.Placement{Cluster: pcn},
		Replicas:  3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish(ctx, "TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	// Now grab info for this stream.
	si, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}
	// Check request origin placement.
	if si.Cluster.Name != pcn {
		t.Fatalf("Expected stream to be placed in %q, but got %q", s.ClusterName(), si.Cluster.Name)
	}

	// Now remove a peer that is assigned to the stream.
	rc := sc.clusterForName(pcn)
	rs := rc.randomNonStreamLeader("$G", "TEST")
	rc.removeJetStream(rs)

	// Check the stream info is eventually correct.
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		si, err := stream.Info(ctx)
		if err != nil {
			return fmt.Errorf("Could not fetch stream info: %v", err)
		}
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(si.Cluster.Replicas))
		}
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
			if !strings.HasPrefix(peer.Name, pcn) {
				t.Fatalf("Stream peer reassigned to wrong cluster: %q", peer.Name)
			}
		}
		return nil
	})
}

func TestJetStreamSuperClusterInterestOnlyMode(t *testing.T) {
	GatewayDoNotForceInterestOnlyMode(true)
	defer GatewayDoNotForceInterestOnlyMode(false)

	template := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		accounts {
			one {
				jetstream: enabled
				users [{user: one, password: password}]
			}
			two {
				%s
				users [{user: two, password: password}]
			}
		}
		cluster {
			listen: 127.0.0.1:%d
			name: %s
			routes = ["nats://127.0.0.1:%d"]
		}
		gateway {
			name: %s
			listen: 127.0.0.1:%d
			gateways = [{name: %s, urls: ["nats://127.0.0.1:%d"]}]
		}
	`
	storeDir1 := t.TempDir()
	conf1 := createConfFile(t, []byte(fmt.Sprintf(template,
		"S1", storeDir1, "", 23222, "A", 23222, "A", 11222, "B", 11223)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	storeDir2 := t.TempDir()
	conf2 := createConfFile(t, []byte(fmt.Sprintf(template,
		"S2", storeDir2, "", 23223, "B", 23223, "B", 11223, "A", 11222)))
	s2, o2 := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	waitForInboundGateways(t, s1, 1, 2*time.Second)
	waitForInboundGateways(t, s2, 1, 2*time.Second)
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)

	nc1 := natsConnect(t, fmt.Sprintf("nats://two:password@127.0.0.1:%d", o1.Port))
	defer nc1.Close()
	nc1.Publish("foo", []byte("some message"))
	nc1.Flush()

	nc2 := natsConnect(t, fmt.Sprintf("nats://two:password@127.0.0.1:%d", o2.Port))
	defer nc2.Close()
	nc2.Publish("bar", []byte("some message"))
	nc2.Flush()

	checkMode := func(accName string, expectedMode GatewayInterestMode) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			servers := []*Server{s1, s2}
			for _, s := range servers {
				var gws []*client
				s.getInboundGatewayConnections(&gws)
				for _, gw := range gws {
					var mode GatewayInterestMode
					gw.mu.Lock()
					ie := gw.gw.insim[accName]
					if ie != nil {
						mode = ie.mode
					}
					gw.mu.Unlock()
					if ie == nil {
						return fmt.Errorf("Account %q not in map", accName)
					}
					if mode != expectedMode {
						return fmt.Errorf("Expected account %q mode to be %v, got: %v", accName, expectedMode, mode)
					}
				}
			}
			return nil
		})
	}

	checkMode("one", InterestOnly)
	checkMode("two", Optimistic)

	// Now change account "two" to enable JS
	changeCurrentConfigContentWithNewContent(t, conf1, []byte(fmt.Sprintf(template,
		"S1", storeDir1, "jetstream: enabled", 23222, "A", 23222, "A", 11222, "B", 11223)))
	changeCurrentConfigContentWithNewContent(t, conf2, []byte(fmt.Sprintf(template,
		"S2", storeDir2, "jetstream: enabled", 23223, "B", 23223, "B", 11223, "A", 11222)))

	if err := s1.Reload(); err != nil {
		t.Fatalf("Error on s1 reload: %v", err)
	}
	if err := s2.Reload(); err != nil {
		t.Fatalf("Error on s2 reload: %v", err)
	}

	checkMode("one", InterestOnly)
	checkMode("two", InterestOnly)
}

func TestJetStreamSuperClusterConnectionCount(t *testing.T) {
	sc := createJetStreamSuperClusterWithTemplate(t, jsClusterAccountsTempl, 3, 2)
	defer sc.shutdown()

	sysNc := natsConnect(t, sc.randomServer().ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	defer sysNc.Close()
	_, err := sysNc.Request(fmt.Sprintf(accDirectReqSubj, "ONE", "CONNS"), nil, 100*time.Millisecond)
	// this is a timeout as the server only responds when it has connections....
	// not convinced this should be that way, but also not the issue to investigate.
	require_True(t, err == nats.ErrTimeout)

	ctx := context.Background()
	for i := 1; i <= 2; i++ {
		func() {
			nc := natsConnect(t, sc.clusterForName(fmt.Sprintf("C%d", i)).randomServer().ClientURL())
			defer nc.Close()
			js, err := jetstream.New(nc, jetstream.WithDefaultTimeout(10*time.Second))
			require_NoError(t, err)
			name := fmt.Sprintf("foo%d", 1)
			_, err = js.CreateStream(ctx, jetstream.StreamConfig{
				Name:     name,
				Subjects: []string{name},
				Replicas: 3})
			require_NoError(t, err)
		}()
	}
	func() {
		nc := natsConnect(t, sc.clusterForName("C1").randomServer().ClientURL())
		defer nc.Close()
		js, err := jetstream.New(nc, jetstream.WithDefaultTimeout(10*time.Second))
		require_NoError(t, err)
		_, err = js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     "src",
			Sources:  []*jetstream.StreamSource{{Name: "foo1"}, {Name: "foo2"}},
			Replicas: 3})
		require_NoError(t, err)
	}()
	func() {
		nc := natsConnect(t, sc.clusterForName("C2").randomServer().ClientURL())
		defer nc.Close()
		js, err := jetstream.New(nc, jetstream.WithDefaultTimeout(10*time.Second))
		require_NoError(t, err)
		_, err = js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     "mir",
			Mirror:   &jetstream.StreamSource{Name: "foo2"},
			Replicas: 3})
		require_NoError(t, err)
	}()

	// There should be no active NATS CLIENT connections, but we still need
	// to wait a little bit...
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		_, err := sysNc.Request(fmt.Sprintf(accDirectReqSubj, "ONE", "CONNS"), nil, 100*time.Millisecond)
		if err != nats.ErrTimeout {
			return fmt.Errorf("Expected timeout, got %v", err)
		}
		return nil
	})
	sysNc.Close()

	s := sc.randomServer()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		acc, err := s.lookupAccount("ONE")
		if err != nil {
			t.Fatalf("Could not look up account: %v", err)
		}
		if n := acc.NumConnections(); n != 0 {
			return fmt.Errorf("Expected no connections, got %d", n)
		}
		return nil
	})
}

func TestJetStreamSuperClusterConsumersBrokenGateways(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 1, 2)
	defer sc.shutdown()

	// Client based API
	s := sc.clusterForName("C1").randomServer()
	nc, js := jsClientConnectNewAPI(t, s)
	defer nc.Close()
	ctx := context.Background()

	// This will be in C1.
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "TEST"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create a stream in C2 that sources TEST
	streamS, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "S",
		Placement: &jetstream.Placement{Cluster: "C2"},
		Sources:   []*jetstream.StreamSource{{Name: "TEST"}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Wait for direct consumer to get registered and detect interest across GW.
	time.Sleep(time.Second)

	// Send 100 msgs over 100ms in separate Go routine.
	msg, toSend, done := []byte("Hello"), 100, make(chan bool)
	go func() {
		// Send in 10 messages.
		for i := 0; i < toSend; i++ {
			if _, err = js.Publish(ctx, "TEST", msg); err != nil {
				t.Errorf("Unexpected publish error: %v", err)
			}
			time.Sleep(500 * time.Microsecond)
		}
		done <- true
	}()

	breakGW := func() {
		s.gateway.Lock()
		gw := s.gateway.out["C2"]
		s.gateway.Unlock()
		if gw != nil {
			gw.closeConnection(ClientClosed)
		}
	}

	// Wait til about half way through.
	time.Sleep(20 * time.Millisecond)
	// Now break GW connection.
	breakGW()

	// Wait for GW to reform.
	for _, c := range sc.clusters {
		for _, s := range c.servers {
			waitForOutboundGateways(t, s, 1, 2*time.Second)
		}
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not complete sending first batch of messages")
	}

	// Make sure we can deal with data loss at the end.
	checkFor(t, 20*time.Second, 250*time.Millisecond, func() error {
		si, err := streamS.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 100 {
			return fmt.Errorf("Expected to have %d messages, got %d", 100, si.State.Msgs)
		}
		return nil
	})

	// Now send 100 more. Will aos break here in the middle.
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish(ctx, "TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
		if i == 50 {
			breakGW()
		}
	}

	// Wait for GW to reform.
	for _, c := range sc.clusters {
		for _, s := range c.servers {
			waitForOutboundGateways(t, s, 1, 2*time.Second)
		}
	}

	streamTest, err := js.Stream(ctx, "TEST")
	require_NoError(t, err)
	si, err := streamTest.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != 200 {
		t.Fatalf("Expected to have %d messages, got %d", 200, si.State.Msgs)
	}

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		si, err := streamS.Info(ctx)
		if err != nil {
			return fmt.Errorf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 200 {
			return fmt.Errorf("Expected to have %d messages, got %d", 200, si.State.Msgs)
		}
		return nil
	})
}

func TestJetStreamSuperClusterLeafNodesWithSharedSystemAccountAndSameDomain(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	lnc := sc.createLeafNodes("LNC", 2)
	defer lnc.shutdown()

	// We want to make sure there is only one leader and its always in the supercluster.
	sc.waitOnLeader()

	if ml := lnc.leader(); ml != nil {
		t.Fatalf("Detected a meta-leader in the leafnode cluster: %s", ml)
	}

	// leafnodes should have been added into the overall peer count.
	sc.waitOnPeerCount(8)

	// Check here that we auto detect sharing system account as well and auto place the correct
	// deny imports and exports.
	ls := lnc.randomServer()
	if ls == nil {
		t.Fatalf("Expected a leafnode server, got none")
	}
	gacc := ls.globalAccount().GetName()

	ls.mu.Lock()
	var hasDE, hasDI bool
	for _, ln := range ls.leafs {
		ln.mu.Lock()
		if ln.leaf.remote.RemoteLeafOpts.LocalAccount == gacc {
			re := ln.perms.pub.deny.Match(jsAllAPI)
			hasDE = len(re.psubs)+len(re.qsubs) > 0
			rs := ln.perms.sub.deny.Match(jsAllAPI)
			hasDI = len(rs.psubs)+len(rs.qsubs) > 0
		}
		ln.mu.Unlock()
	}
	ls.mu.Unlock()

	if !hasDE {
		t.Fatalf("No deny export on global account")
	}
	if !hasDI {
		t.Fatalf("No deny import on global account")
	}

	// Make a stream by connecting to the leafnode cluster. Make sure placement is correct.
	// Client based API
	nc, js := jsClientConnectNewAPI(t, lnc.randomServer())
	defer nc.Close()
	ctx := context.Background()

	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	si := stream.CachedInfo()
	if si.Cluster.Name != "LNC" {
		t.Fatalf("Expected default placement to be %q, got %q", "LNC", si.Cluster.Name)
	}

	// Now make sure placement also works if we want to place in a cluster in the supercluster.
	pcn := "C2"
	stream2, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "TEST2",
		Subjects:  []string{"baz"},
		Replicas:  2,
		Placement: &jetstream.Placement{Cluster: pcn},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	si = stream2.CachedInfo()
	if si.Cluster.Name != pcn {
		t.Fatalf("Expected default placement to be %q, got %q", pcn, si.Cluster.Name)
	}
}

func TestJetStreamSuperClusterLeafNodesWithSharedSystemAccountAndDifferentDomain(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	lnc := sc.createLeafNodesWithDomain("LNC", 2, "LEAFDOMAIN")
	defer lnc.shutdown()

	// We want to make sure there is only one leader and its always in the supercluster.
	sc.waitOnLeader()
	lnc.waitOnLeader()

	// even though system account is shared, because domains differ,
	sc.waitOnPeerCount(6)
	lnc.waitOnPeerCount(2)

	// Check here that we auto detect sharing system account as well and auto place the correct
	// deny imports and exports.
	ls := lnc.randomServer()
	if ls == nil {
		t.Fatalf("Expected a leafnode server, got none")
	}
	gacc := ls.globalAccount().GetName()

	ls.mu.Lock()
	var hasDE, hasDI bool
	for _, ln := range ls.leafs {
		ln.mu.Lock()
		if ln.leaf.remote.RemoteLeafOpts.LocalAccount == gacc {
			re := ln.perms.pub.deny.Match(jsAllAPI)
			hasDE = len(re.psubs)+len(re.qsubs) > 0
			rs := ln.perms.sub.deny.Match(jsAllAPI)
			hasDI = len(rs.psubs)+len(rs.qsubs) > 0
		}
		ln.mu.Unlock()
	}
	ls.mu.Unlock()

	if !hasDE {
		t.Fatalf("No deny export on global account")
	}
	if !hasDI {
		t.Fatalf("No deny import on global account")
	}

	// Make a stream by connecting to the leafnode cluster. Make sure placement is correct.
	// Client based API
	nc, js := jsClientConnectNewAPI(t, lnc.randomServer())
	defer nc.Close()
	ctx := context.Background()

	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	si := stream.CachedInfo()
	if si.Cluster.Name != "LNC" {
		t.Fatalf("Expected default placement to be %q, got %q", "LNC", si.Cluster.Name)
	}

	// Now make sure placement does not works for cluster in different domain
	pcn := "C2"
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "TEST2",
		Subjects:  []string{"baz"},
		Replicas:  2,
		Placement: &jetstream.Placement{Cluster: pcn},
	})
	if err == nil || !strings.Contains(err.Error(), "no suitable peers for placement") {
		t.Fatalf("Expected no suitable peers for placement, got: %v", err)
	}
}

func TestJetStreamSuperClusterSingleLeafNodeWithSharedSystemAccount(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	ln := sc.createSingleLeafNode(true)
	defer ln.Shutdown()

	// We want to make sure there is only one leader and its always in the supercluster.
	sc.waitOnLeader()

	// leafnodes should have been added into the overall peer count.
	sc.waitOnPeerCount(7)

	// Now make sure we can place a stream in the leaf node.
	// First connect to the leafnode server itself.
	nc, js := jsClientConnectNewAPI(t, ln)
	defer nc.Close()
	ctx := context.Background()

	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST1",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	si := stream.CachedInfo()
	if si.Cluster.Name != "LNS" {
		t.Fatalf("Expected to be placed in leafnode with %q as cluster name, got %q", "LNS", si.Cluster.Name)
	}
	// Now check we can place on here as well but connect to the hub.
	nc, js = jsClientConnectNewAPI(t, sc.randomServer())
	defer nc.Close()

	stream2, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "TEST2",
		Subjects:  []string{"bar"},
		Placement: &jetstream.Placement{Cluster: "LNS"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	si = stream2.CachedInfo()
	if si.Cluster.Name != "LNS" {
		t.Fatalf("Expected to be placed in leafnode with %q as cluster name, got %q", "LNS", si.Cluster.Name)
	}
}

// Issue reported with superclusters and leafnodes where first few get next requests for pull subscribers
// have the wrong subject.
func TestJetStreamSuperClusterGetNextRewrite(t *testing.T) {
	sc := createJetStreamSuperClusterWithTemplate(t, jsClusterAccountsTempl, 2, 2)
	defer sc.shutdown()

	// Will connect the leafnode to cluster C1. We will then connect the "client" to cluster C2 to cross gateways.
	ln := sc.clusterForName("C1").createSingleLeafNodeNoSystemAccountAndEnablesJetStreamWithDomain("C", "nojs")
	defer ln.Shutdown()

	c2 := sc.clusterForName("C2")
	nc, js := jsClientConnectNewAPIWithDomain(t, c2.randomServer(), "C", nats.UserInfo("nojs", "p"))
	defer nc.Close()
	ctx := context.Background()

	// Create a stream and add messages.
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := js.Publish(ctx, "foo", []byte("ok")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Pull messages and make sure subject rewrite works.
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "dlc",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, m := range fetchJetStreamMsgs(t, cons, 5, time.Second) {
		if m.Subject() != "foo" {
			t.Fatalf("Expected %q as subject but got %q", "foo", m.Subject())
		}
	}
}

func TestJetStreamSuperClusterEphemeralCleanup(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	// Create a stream in cluster 0
	s := sc.clusters[0].randomServer()
	nc, js := jsClientConnectNewAPI(t, s)
	defer nc.Close()
	ctx := context.Background()

	for _, test := range []struct {
		name            string
		sourceInCluster int
		streamName      string
		sourceName      string
	}{
		{"local", 0, "TEST1", "S1"},
		{"remote", 1, "TEST2", "S2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: test.streamName, Replicas: 3}); err != nil {
				t.Fatalf("Error adding %q stream: %v", test.streamName, err)
			}
			if _, err := js.Publish(ctx, test.streamName, []byte("hello")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}

			// Now create a source for that stream, either in same or remote cluster.
			s2 := sc.clusters[test.sourceInCluster].randomServer()
			nc2, js2 := jsClientConnectNewAPI(t, s2)
			defer nc2.Close()

			sourceStream, err := js2.CreateStream(ctx, jetstream.StreamConfig{
				Name:     test.sourceName,
				Storage:  jetstream.FileStorage,
				Sources:  []*jetstream.StreamSource{{Name: test.streamName}},
				Replicas: 1,
			})
			if err != nil {
				t.Fatalf("Error adding source stream: %v", err)
			}

			// Check that TEST(n) has 1 consumer and that S(n) is created and has 1 message.
			checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
				si, err := sourceStream.Info(ctx)
				if err != nil {
					return fmt.Errorf("Could not get stream info: %v", err)
				}
				if si.State.Msgs != 1 {
					return fmt.Errorf("Expected 1 msg, got state: %+v", si.State)
				}
				return nil
			})

			// Get the consumer because we will want to artificially reduce
			// the delete threshold.
			leader := sc.clusters[0].streamLeader("$G", test.streamName)
			mset, err := leader.GlobalAccount().lookupStream(test.streamName)
			if err != nil {
				t.Fatalf("Expected to find a stream for %q, got %v", test.streamName, err)
			}
			cons := mset.getConsumers()[0]
			cons.mu.Lock()
			cons.dthresh = 1250 * time.Millisecond
			active := cons.active
			dtimerSet := cons.dtmr != nil
			deliver := cons.cfg.DeliverSubject
			cons.mu.Unlock()

			if !active || dtimerSet {
				t.Fatalf("Invalid values for active=%v dtimerSet=%v", active, dtimerSet)
			}
			// To add to the mix, let's create a local interest on the delivery subject
			// and stop it. This is to ensure that this does not stop timers that should
			// still be running and monitor the GW interest.
			sub := natsSubSync(t, nc, deliver)
			natsFlush(t, nc)
			natsUnsub(t, sub)
			natsFlush(t, nc)

			// Now remove the "S(n)" stream...
			if err := js2.DeleteStream(ctx, test.sourceName); err != nil {
				t.Fatalf("Error deleting stream: %v", err)
			}

			// Now check that the stream S(n) is really removed and that
			// the consumer is gone for stream TEST(n).
			checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
				// First, make sure that stream S(n) has disappeared.
				if _, err := js2.Stream(ctx, test.sourceName); err == nil {
					return fmt.Errorf("Stream %q should no longer exist", test.sourceName)
				}
				if ndc := mset.numDirectConsumers(); ndc != 0 {
					return fmt.Errorf("Expected %q stream to have 0 consumers, got %v", test.streamName, ndc)
				}
				return nil
			})
		})
	}
}

func TestJetStreamSuperClusterGetNextSubRace(t *testing.T) {
	sc := createJetStreamSuperClusterWithTemplate(t, jsClusterAccountsTempl, 2, 2)
	defer sc.shutdown()

	// Will connect the leafnode to cluster C1. We will then connect the "client" to cluster C2 to cross gateways.
	ln := sc.clusterForName("C1").createSingleLeafNodeNoSystemAccountAndEnablesJetStreamWithDomain("C", "nojs")
	defer ln.Shutdown()

	// Shutdown 1 of the server from C1, (the one LN is not connected to)
	for _, s := range sc.clusterForName("C1").servers {
		s.mu.Lock()
		if len(s.leafs) == 0 {
			s.mu.Unlock()
			s.Shutdown()
			break
		}
		s.mu.Unlock()
	}

	// Wait on meta leader in case shutdown of server above caused an election.
	sc.waitOnLeader()

	var c2Srv *Server
	// Take the server from C2 that has no inbound from C1.
	c2 := sc.clusterForName("C2")
	for _, s := range c2.servers {
		var gwsa [2]*client
		gws := gwsa[:0]
		s.getInboundGatewayConnections(&gws)
		if len(gws) == 0 {
			c2Srv = s
			break
		}
	}
	if c2Srv == nil {
		t.Fatalf("Both servers in C2 had an inbound GW connection!")
	}

	nc, js := jsClientConnectNewAPIWithDomain(t, c2Srv, "C", nats.UserInfo("nojs", "p"))
	defer nc.Close()
	ctx := context.Background()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo"})
	require_NoError(t, err)

	cons, err := js.CreateConsumer(ctx, "foo", jetstream.ConsumerConfig{Durable: "dur", AckPolicy: jetstream.AckExplicitPolicy})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		sendStreamMsg(t, nc, "foo", "ok")
	}

	// Wait for all messages to appear in the consumer
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		ci, err := cons.Info(ctx)
		if err != nil {
			return err
		}
		if n := ci.NumPending; n != 100 {
			return fmt.Errorf("Expected 100 msgs, got %v", n)
		}
		return nil
	})

	req := &JSApiConsumerGetNextRequest{Batch: 1, Expires: 5 * time.Second}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)
	// Create this by hand here to make sure we create the subscription
	// on the reply subject for every single request
	nextSubj := fmt.Sprintf(JSApiRequestNextT, "foo", "dur")
	nextSubj = "$JS.C.API" + strings.TrimPrefix(nextSubj, "$JS.API")
	for i := 0; i < 100; i++ {
		inbox := nats.NewInbox()
		sub := natsSubSync(t, nc, inbox)
		natsPubReq(t, nc, nextSubj, inbox, jreq)
		msg := natsNexMsg(t, sub, time.Second)
		if len(msg.Header) != 0 && string(msg.Data) != "ok" {
			t.Fatalf("Unexpected message: header=%+v data=%s", msg.Header, msg.Data)
		}
		sub.Unsubscribe()
	}
}

func TestJetStreamSuperClusterPullConsumerAndHeaders(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	c1 := sc.clusterForName("C1")
	c2 := sc.clusterForName("C2")

	nc, js := jsClientConnectNewAPI(t, c1.randomServer())
	defer nc.Close()
	ctx := context.Background()

	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "ORIGIN"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	toSend := 50
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish(ctx, "ORIGIN", []byte("ok")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	nc2, js2 := jsClientConnectNewAPI(t, c2.randomServer())
	defer nc2.Close()

	sStream, err := js2.CreateStream(ctx, jetstream.StreamConfig{
		Name:    "S",
		Sources: []*jetstream.StreamSource{{Name: "ORIGIN"}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Wait for them to be in the sourced stream.
	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		if si, _ := sStream.Info(ctx); si.State.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected %d msgs for %q, got %d", toSend, "S", si.State.Msgs)
		}
		return nil
	})

	// Now create a pull consumer for the sourced stream.
	_, err = js2.CreateConsumer(ctx, "S", jetstream.ConsumerConfig{Durable: "dlc", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now we will connect and request the next message from each server in C1 cluster and check that headers remain in place.
	for _, s := range c1.servers {
		nc, err := nats.Connect(s.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()
		m, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.S.dlc", nil, 2*time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(m.Header) != 1 {
			t.Fatalf("Expected 1 header element, got %+v", m.Header)
		}
	}
}

func TestJetStreamSuperClusterStatszActiveServers(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 2, 2)
	defer sc.shutdown()

	checkActive := func(expected int) {
		t.Helper()
		checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
			s := sc.randomServer()
			nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
			if err != nil {
				t.Fatalf("Failed to create system client: %v", err)
			}
			defer nc.Close()

			resp, err := nc.Request(serverStatsPingReqSubj, nil, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var ssm ServerStatsMsg
			if err := json.Unmarshal(resp.Data, &ssm); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ssm.Stats.ActiveServers != expected {
				return fmt.Errorf("Wanted %d, got %d", expected, ssm.Stats.ActiveServers)
			}
			return nil
		})
	}

	checkActive(4)
	c := sc.randomCluster()
	ss := c.randomServer()
	ss.Shutdown()
	checkActive(3)
	c.restartServer(ss)
	checkActive(4)
}

func TestJetStreamSuperClusterSourceAndMirrorConsumersLeaderChange(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	c1 := sc.clusterForName("C1")
	c2 := sc.clusterForName("C2")

	nc, js := jsClientConnectNewAPI(t, c1.randomServer())
	defer nc.Close()
	ctx := context.Background()

	var sources []*jetstream.StreamSource
	numStreams := 10

	for i := 1; i <= numStreams; i++ {
		name := fmt.Sprintf("O%d", i)
		sources = append(sources, &jetstream.StreamSource{Name: name})
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Place our new stream that will source all the others in different cluster.
	nc, js = jsClientConnectNewAPI(t, c2.randomServer())
	defer nc.Close()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "S",
		Replicas: 2,
		Sources:  sources,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Force leader change twice.
	nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "S"), nil, time.Second)
	c2.waitOnStreamLeader("$G", "S")
	nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "S"), nil, time.Second)
	c2.waitOnStreamLeader("$G", "S")

	// Now make sure we only have a single direct consumer on our origin streams.
	// Pick one at random.
	name := fmt.Sprintf("O%d", rand.Intn(numStreams-1)+1)
	c1.waitOnStreamLeader("$G", name)
	s := c1.streamLeader("$G", name)
	a, err := s.lookupAccount("$G")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	mset, err := a.lookupStream(name)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		if ndc := mset.numDirectConsumers(); ndc != 1 {
			return fmt.Errorf("Stream %q wanted 1 direct consumer, got %d", name, ndc)
		}
		return nil
	})

	// Now create a mirror of selected from above. Will test same scenario.
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "M",
		Replicas: 2,
		Mirror:   &jetstream.StreamSource{Name: name},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Force leader change twice.
	nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "M"), nil, time.Second)
	c2.waitOnStreamLeader("$G", "M")
	nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "M"), nil, time.Second)
	c2.waitOnStreamLeader("$G", "M")

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		if ndc := mset.numDirectConsumers(); ndc != 2 {
			return fmt.Errorf("Stream %q wanted 2 direct consumers, got %d", name, ndc)
		}
		return nil
	})
}

func TestJetStreamSuperClusterPushConsumerInterest(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	for _, test := range []struct {
		name  string
		queue string
	}{
		{"non queue", _EMPTY_},
		{"queue", "queue"},
	} {
		t.Run(test.name, func(t *testing.T) {
			testInterest := func(s *Server) {
				t.Helper()
				nc, js := jsClientConnectNewAPI(t, s)
				defer nc.Close()
				ctx := context.Background()

				stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
					Name:     "TEST",
					Subjects: []string{"foo"},
					Replicas: 3,
				})
				require_NoError(t, err)

				// Create a push-style durable consumer using a deliver subject.
				inbox := nats.NewInbox()
				ccfg := jetstream.ConsumerConfig{
					Durable:        "dur",
					DeliverSubject: inbox,
					AckPolicy:      jetstream.AckNonePolicy,
				}
				if test.queue != _EMPTY_ {
					ccfg.Durable = _EMPTY_
					ccfg.DeliverGroup = test.queue
				}
				_, err = stream.CreateOrUpdateConsumer(ctx, ccfg)
				require_NoError(t, err)

				var sub *nats.Subscription
				if test.queue != _EMPTY_ {
					sub, err = nc.QueueSubscribeSync(inbox, test.queue)
				} else {
					sub, err = nc.SubscribeSync(inbox)
				}
				require_NoError(t, err)

				js.Publish(ctx, "foo", []byte("msg1"))
				// Since the GW watcher is checking every 1sec, make sure we are
				// giving it enough time for the delivery to start.
				_, err = sub.NextMsg(2 * time.Second)
				require_NoError(t, err)
			}

			// Create the durable push consumer from cluster "0"
			testInterest(sc.clusters[0].servers[0])

			// Now "move" to a server in cluster "1"
			testInterest(sc.clusters[1].servers[0])
		})
	}
}

func TestJetStreamSuperClusterOverflowPlacement(t *testing.T) {
	sc := createJetStreamSuperClusterWithTemplate(t, jsClusterMaxBytesTempl, 3, 3)
	defer sc.shutdown()

	pcn := "C2"
	s := sc.clusterForName(pcn).randomServer()
	nc, js := jsClientConnectNewAPI(t, s)
	defer nc.Close()
	ctx := context.Background()

	// With this setup, we opted in for requiring MaxBytes, so this should error.
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "foo",
		Replicas: 3,
	})
	require_Error(t, err, NewJSStreamMaxBytesRequiredError())

	// R=2 on purpose to leave one server empty.
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "foo",
		Replicas: 2,
		MaxBytes: 2 * 1024 * 1024 * 1024,
	})
	require_NoError(t, err)

	// Now try to add another that will overflow the current cluster's reservation.
	// Since we asked explicitly for the same cluster this should fail.
	// Note this will not be testing the peer picker since the update has probably not made it to the meta leader.
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "bar",
		Replicas:  3,
		MaxBytes:  2 * 1024 * 1024 * 1024,
		Placement: &jetstream.Placement{Cluster: pcn},
	})
	require_Contains(t, err.Error(), "nats: no suitable peers for placement")
	// Now test actual overflow placement. So try again with no placement designation.
	// This will test the peer picker's logic since they are updated at this point and the meta leader
	// knows it can not place it in C2.
	sBar, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "bar",
		Replicas: 3,
		MaxBytes: 2 * 1024 * 1024 * 1024,
	})
	require_NoError(t, err)

	// Make sure we did not get place into C2.
	falt := sBar.CachedInfo().Cluster.Name
	if falt == pcn {
		t.Fatalf("Expected to be placed in another cluster besides %q, but got %q", pcn, falt)
	}

	// One more time that should spill over again to our last cluster.
	sBaz, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "baz",
		Replicas: 3,
		MaxBytes: 2 * 1024 * 1024 * 1024,
	})
	require_NoError(t, err)

	// Make sure we did not get place into C2.
	if salt := sBaz.CachedInfo().Cluster.Name; salt == pcn || salt == falt {
		t.Fatalf("Expected to be placed in last cluster besides %q or %q, but got %q", pcn, falt, salt)
	}

	// Now place a stream of R1 into C2 which should have space.
	sDlc, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "dlc",
		MaxBytes: 2 * 1024 * 1024 * 1024,
	})
	require_NoError(t, err)

	if si := sDlc.CachedInfo(); si.Cluster.Name != pcn {
		t.Fatalf("Expected to be placed in our origin cluster %q, but got %q", pcn, si.Cluster.Name)
	}
}

func TestJetStreamSuperClusterConcurrentOverflow(t *testing.T) {
	sc := createJetStreamSuperClusterWithTemplate(t, jsClusterMaxBytesTempl, 3, 3)
	defer sc.shutdown()

	pcn := "C2"

	startCh := make(chan bool)
	var wg sync.WaitGroup
	var swg sync.WaitGroup

	start := func(name string) {
		defer wg.Done()

		s := sc.clusterForName(pcn).randomServer()
		nc, js := jsClientConnectNewAPI(t, s)
		defer nc.Close()
		ctx := context.Background()

		swg.Done()
		<-startCh

		_, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     name,
			Replicas: 3,
			MaxBytes: 2 * 1024 * 1024 * 1024,
		})
		require_NoError(t, err)
	}
	wg.Add(2)
	swg.Add(2)
	go start("foo")
	go start("bar")
	swg.Wait()
	// Now start both at same time.
	close(startCh)
	wg.Wait()
}

func TestJetStreamSuperClusterStreamTagPlacement(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()

	placeOK := func(connectCluster string, tags []string, expectedCluster string) {
		t.Helper()
		nc, js := jsClientConnectNewAPI(t, sc.clusterForName(connectCluster).randomServer())
		defer nc.Close()
		ctx := context.Background()
		stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:      "TEST",
			Subjects:  []string{"foo"},
			Placement: &jetstream.Placement{Tags: tags},
		})
		require_NoError(t, err)
		if si := stream.CachedInfo(); si.Cluster.Name != expectedCluster {
			t.Fatalf("Failed to place properly in %q, got %q", expectedCluster, si.Cluster.Name)
		}
		js.DeleteStream(ctx, "TEST")
	}

	placeOK("C2", []string{"cloud:aws"}, "C1")
	placeOK("C2", []string{"country:jp"}, "C3")
	placeOK("C1", []string{"cloud:gcp", "country:uk"}, "C2")

	// Case shoud not matter.
	placeOK("C1", []string{"cloud:GCP", "country:UK"}, "C2")
	placeOK("C2", []string{"Cloud:AwS", "Country:uS"}, "C1")

	placeErr := func(connectCluster string, tags []string) {
		t.Helper()
		nc, js := jsClientConnectNewAPI(t, sc.clusterForName(connectCluster).randomServer())
		defer nc.Close()
		ctx := context.Background()
		_, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:      "TEST",
			Subjects:  []string{"foo"},
			Placement: &jetstream.Placement{Tags: tags},
		})
		require_Contains(t, err.Error(), "no suitable peers for placement", "tags not matched")
		require_Contains(t, err.Error(), tags...)
	}

	placeErr("C1", []string{"cloud:GCP", "country:US"})
	placeErr("C1", []string{"country:DN"})
	placeErr("C1", []string{"cloud:DO"})
}

func TestJetStreamSuperClusterRemovedPeersAndStreamsListAndDelete(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	pcn := "C2"
	sc.waitOnLeader()
	ml := sc.leader()
	if ml.ClusterName() == pcn {
		pcn = "C1"
	}

	// Client based API
	nc, js := jsClientConnectNewAPI(t, ml)
	defer nc.Close()
	ctx := context.Background()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "GONE",
		Replicas:  3,
		Placement: &jetstream.Placement{Cluster: pcn},
	})
	require_NoError(t, err)

	_, err = js.CreateConsumer(ctx, "GONE", jetstream.ConsumerConfig{Durable: "dlc", AckPolicy: jetstream.AckExplicitPolicy})
	require_NoError(t, err)

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "TEST",
		Replicas:  3,
		Placement: &jetstream.Placement{Cluster: ml.ClusterName()},
	})
	require_NoError(t, err)

	// Put messages in..
	num := 100
	for i := 0; i < num; i++ {
		js.PublishAsync("GONE", []byte("SLS"))
		js.PublishAsync("TEST", []byte("SLS"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	c := sc.clusterForName(pcn)
	c.shutdown()

	// Grab Stream List..
	start := time.Now()
	resp, err := nc.Request(JSApiStreamList, nil, 2*time.Second)
	require_NoError(t, err)
	if delta := time.Since(start); delta > 100*time.Millisecond {
		t.Fatalf("Stream list call took too long to return: %v", delta)
	}
	var list JSApiStreamListResponse
	err = json.Unmarshal(resp.Data, &list)
	require_NoError(t, err)

	if len(list.Missing) != 1 || list.Missing[0] != "GONE" {
		t.Fatalf("Wrong Missing: %+v", list)
	}

	// Check behavior of stream info as well. We want it to return the stream is offline and not just timeout.
	if sh, e := js.Stream(ctx, "GONE"); e == nil {
		_, err = sh.Info(ctx)
	} else {
		err = e
	}
	// FIXME(dlc) - Go client not putting nats: prefix on for stream but does for consumer.
	require_Error(t, err, NewJSStreamOfflineError(), errors.New("nats: stream is offline"))

	// Same for Consumer
	start = time.Now()
	resp, err = nc.Request("$JS.API.CONSUMER.LIST.GONE", nil, 2*time.Second)
	require_NoError(t, err)
	if delta := time.Since(start); delta > 100*time.Millisecond {
		t.Fatalf("Consumer list call took too long to return: %v", delta)
	}
	var clist JSApiConsumerListResponse
	err = json.Unmarshal(resp.Data, &clist)
	require_NoError(t, err)

	if len(clist.Missing) != 1 || clist.Missing[0] != "dlc" {
		t.Fatalf("Wrong Missing: %+v", clist)
	}

	_, err = js.Consumer(ctx, "GONE", "dlc")
	require_Error(t, err, NewJSConsumerOfflineError(), errors.New("nats: consumer is offline"))

	// Make sure delete works.
	err = js.DeleteConsumer(ctx, "GONE", "dlc")
	require_NoError(t, err)

	err = js.DeleteStream(ctx, "GONE")
	require_NoError(t, err)

	// Test it is really gone.
	_, err = js.Stream(ctx, "GONE")
	require_Error(t, err, jetstream.ErrStreamNotFound)
}

func TestJetStreamSuperClusterConsumerDeliverNewBug(t *testing.T) {
	for _, storage := range []jetstream.StorageType{jetstream.FileStorage, jetstream.MemoryStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			sc := createJetStreamSuperCluster(t, 3, 3)
			defer sc.shutdown()

			pcn := "C2"
			sc.waitOnLeader()
			ml := sc.leader()
			if ml.ClusterName() == pcn {
				pcn = "C1"
			}

			// Client based API
			nc, js := jsClientConnectNewAPI(t, ml)
			defer nc.Close()
			ctx := context.Background()

			_, err := js.CreateStream(ctx, jetstream.StreamConfig{
				Name:      "T",
				Replicas:  3,
				Placement: &jetstream.Placement{Cluster: pcn},
				Storage:   storage,
			})
			require_NoError(t, err)

			// Put messages in..
			num := 100
			for i := 0; i < num; i++ {
				js.PublishAsync("T", []byte("OK"))
			}
			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}

			cons, err := js.CreateConsumer(ctx, "T", jetstream.ConsumerConfig{
				Durable:       "d",
				AckPolicy:     jetstream.AckExplicitPolicy,
				DeliverPolicy: jetstream.DeliverNewPolicy,
			})
			require_NoError(t, err)
			ci := cons.CachedInfo()

			if ci.Delivered.Consumer != 0 || ci.Delivered.Stream != 100 {
				t.Fatalf("Incorrect consumer delivered info: %+v", ci.Delivered)
			}

			c := sc.clusterForName(pcn)
			for _, s := range c.servers {
				sd := s.JetStreamConfig().StoreDir
				s.Shutdown()
				removeDir(t, sd)
				s = c.restartServer(s)
				c.waitOnServerHealthz(s)
				c.waitOnConsumerLeader("$G", "T", "d")
			}

			c.waitOnConsumerLeader("$G", "T", "d")

			// Each server, after being caught up, needs to fully agree on stream/consumer sequences.
			// For both the in-memory consumer state, as the stored state.
			for _, s := range c.servers {
				mset, err := s.GlobalAccount().lookupStream("T")
				require_NoError(t, err)
				o := mset.lookupConsumer("d")
				require_NotNil(t, o)
				o.mu.RLock()
				defer o.mu.RUnlock()

				if o.dseq-1 != 0 || o.sseq-1 != 100 {
					t.Fatalf("Incorrect consumer delivered info: dseq=%d, sseq=%d", o.dseq-1, o.sseq-1)
				}
				state, err := o.store.BorrowState()
				require_NoError(t, err)
				if state.Delivered.Consumer != 0 || state.Delivered.Stream != 100 {
					t.Fatalf("Incorrect consumer state: consumer_seq=%d, stream_seq=%d", state.Delivered.Consumer, state.Delivered.Stream)
				}
				np, err := o.checkNumPending()
				require_NoError(t, err)
				if np != 0 {
					t.Fatalf("Did not expect NumPending, got %d", np)
				}
			}
		})
	}
}

// This will test our ability to move streams and consumers between clusters.
func TestJetStreamSuperClusterMovingStreamsAndConsumers(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()

	nc, js := jsClientConnectNewAPI(t, sc.randomServer())
	defer nc.Close()
	ctx := context.Background()

	for _, test := range []struct {
		name     string
		replicas int
	}{
		{"R1", 1},
		{"R3", 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			replicas := test.replicas

			stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
				Name:      "MOVE",
				Replicas:  replicas,
				Placement: &jetstream.Placement{Tags: []string{"cloud:aws"}},
			})
			require_NoError(t, err)
			defer js.DeleteStream(ctx, "MOVE")

			si := stream.CachedInfo()
			if si.Cluster.Name != "C1" {
				t.Fatalf("Failed to place properly in %q, got %q", "C1", si.Cluster.Name)
			}

			for i := 0; i < 1000; i++ {
				_, err := js.PublishAsync("MOVE", []byte("Moving on up"))
				require_NoError(t, err)
			}
			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}

			// Durable Push Consumer, so same R.
			dpushInbox := nats.NewInbox()
			_, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
				Durable:        "dlc",
				DeliverSubject: dpushInbox,
				AckPolicy:      jetstream.AckExplicitPolicy,
			})
			require_NoError(t, err)
			dpushSub, err := nc.SubscribeSync(dpushInbox)
			require_NoError(t, err)
			defer dpushSub.Unsubscribe()

			// Ephemeral Push Consumer, R1.
			epushInbox := nats.NewInbox()
			epushCons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
				DeliverSubject: epushInbox,
				AckPolicy:      jetstream.AckExplicitPolicy,
			})
			require_NoError(t, err)
			epushSub, err := nc.SubscribeSync(epushInbox)
			require_NoError(t, err)
			defer epushSub.Unsubscribe()

			// Durable Pull Consumer, so same R.
			dpullCons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
				Durable:   "dlc-pull",
				AckPolicy: jetstream.AckExplicitPolicy,
			})
			require_NoError(t, err)

			// TODO(dlc) - Server supports ephemeral pulls but Go client does not yet.

			siInfo, err := stream.Info(ctx)
			require_NoError(t, err)
			if siInfo.State.Consumers != 3 {
				t.Fatalf("Expected 3 attached consumers, got %d", siInfo.State.Consumers)
			}

			initialState := siInfo.State

			checkSubsPending(t, dpushSub, int(initialState.Msgs))
			checkSubsPending(t, epushSub, int(initialState.Msgs))

			// Ack 100
			toAck := 100
			ackMsg := []byte("+ACK")
			for i := 0; i < toAck; i++ {
				m, err := dpushSub.NextMsg(time.Second)
				require_NoError(t, err)
				_, err = nc.Request(m.Reply, ackMsg, time.Second)
				require_NoError(t, err)
				// Ephemeral
				m, err = epushSub.NextMsg(time.Second)
				require_NoError(t, err)
				_, err = nc.Request(m.Reply, ackMsg, time.Second)
				require_NoError(t, err)
			}

			// Do same with pull subscriber.
			for _, m := range fetchJetStreamMsgs(t, dpullCons, toAck, 5*time.Second) {
				require_NoError(t, m.DoubleAck(ctx))
			}

			// First make sure we disallow move and replica changes in same update.
			_, err = js.UpdateStream(ctx, jetstream.StreamConfig{
				Name:      "MOVE",
				Placement: &jetstream.Placement{Tags: []string{"cloud:gcp"}},
				Replicas:  replicas + 1,
			})
			require_Error(t, err, NewJSStreamMoveAndScaleError())

			// Now move to new cluster.
			stream2, err := js.UpdateStream(ctx, jetstream.StreamConfig{
				Name:      "MOVE",
				Replicas:  replicas,
				Placement: &jetstream.Placement{Tags: []string{"cloud:gcp"}},
			})
			require_NoError(t, err)

			checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
				if si := stream2.CachedInfo(); si.Cluster.Name != "C1" {
					return fmt.Errorf("Expected cluster of %q but got %q", "C1", si.Cluster.Name)
				}
				return nil
			})

			// Make sure we can not move an inflight stream and consumers, should error.
			_, err = js.UpdateStream(ctx, jetstream.StreamConfig{
				Name:      "MOVE",
				Replicas:  replicas,
				Placement: &jetstream.Placement{Tags: []string{"cloud:aws"}},
			})
			require_Contains(t, err.Error(), "stream move already in progress")

			getSI := func() (*jetstream.StreamInfo, error) {
				cctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				defer cancel()
				sh, err := js.Stream(cctx, "MOVE")
				if err != nil {
					return nil, err
				}
				return sh.Info(cctx)
			}
			checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
				si, err := getSI()
				if err != nil {
					return err
				}
				// We should see 2X peers.
				numPeers := len(si.Cluster.Replicas)
				if si.Cluster.Leader != _EMPTY_ {
					numPeers++
				}
				if numPeers != 2*replicas {
					// The move can happen very quick now, so we might already be done.
					if si.Cluster.Name == "C2" {
						return nil
					}
					return fmt.Errorf("Expected to see %d replicas, got %d", 2*replicas, numPeers)
				}
				return nil
			})

			// Expect a new leader to emerge and replicas to drop as a leader is elected.
			// We have to check fast or it might complete and we will not see intermediate steps.
			sc.waitOnStreamLeader("$G", "MOVE")
			checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
				si, err := getSI()
				if err != nil {
					return err
				}
				if len(si.Cluster.Replicas) >= 2*replicas {
					return fmt.Errorf("Expected <%d replicas, got %d", 2*replicas, len(si.Cluster.Replicas))
				}
				return nil
			})

			// Should see the cluster designation and leader switch to C2.
			// We should also shrink back down to original replica count.
			checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
				si, err := getSI()
				if err != nil {
					return err
				}
				if si.Cluster.Name != "C2" {
					return fmt.Errorf("Wrong cluster: %q", si.Cluster.Name)
				}
				if si.Cluster.Leader == _EMPTY_ {
					return fmt.Errorf("No leader yet")
				} else if !strings.HasPrefix(si.Cluster.Leader, "C2-") {
					return fmt.Errorf("Wrong leader: %q", si.Cluster.Leader)
				}
				// Now we want to see that we shrink back to original.
				if len(si.Cluster.Replicas) != replicas-1 {
					return fmt.Errorf("Expected %d replicas, got %d", replicas-1, len(si.Cluster.Replicas))
				}
				return nil
			})

			// Check moved state is same as initial state.
			streamMoved, err := js.Stream(ctx, "MOVE")
			require_NoError(t, err)
			siInfo2, err := streamMoved.Info(ctx)
			require_NoError(t, err)

			if !reflect.DeepEqual(siInfo2.State, initialState) {
				t.Fatalf("States do not match after migration:\n%+v\nvs\n%+v", siInfo2.State, initialState)
			}

			// Make sure we can still send messages.
			addN := toAck
			for i := 0; i < addN; i++ {
				_, err := js.Publish(ctx, "MOVE", []byte("Done Moved"))
				require_NoError(t, err)
			}

			siInfo3, err := streamMoved.Info(ctx)
			require_NoError(t, err)

			expectedPushMsgs := initialState.Msgs + uint64(addN)
			expectedPullMsgs := uint64(addN)

			if siInfo3.State.Msgs != expectedPushMsgs {
				t.Fatalf("Expected to be able to send new messages")
			}

			// Now check consumers, make sure the state is correct and that they transferred state and reflect the new messages.
			// We Ack'd 100 and sent another 100, so should be same.
			checkConsumer := func(cons jetstream.Consumer, sub *nats.Subscription, isPull bool) {
				t.Helper()
				checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
					ci, err := cons.Info(ctx)
					if err != nil {
						return err
					}
					var expectedMsgs uint64
					if isPull {
						expectedMsgs = expectedPullMsgs
					} else {
						expectedMsgs = expectedPushMsgs
					}

					if ci.Delivered.Consumer != expectedMsgs || ci.Delivered.Stream != expectedMsgs {
						return fmt.Errorf("Delivered for %q is not correct: %+v", ci.Name, ci.Delivered)
					}
					if ci.AckFloor.Consumer != uint64(toAck) || ci.AckFloor.Stream != uint64(toAck) {
						return fmt.Errorf("AckFloor for %q is not correct: %+v", ci.Name, ci.AckFloor)
					}
					if isPull && ci.NumAckPending != 0 {
						return fmt.Errorf("NumAckPending for %q is not correct: %v", ci.Name, ci.NumAckPending)
					} else if !isPull && ci.NumAckPending != int(initialState.Msgs) {
						return fmt.Errorf("NumAckPending for %q is not correct: %v", ci.Name, ci.NumAckPending)
					}
					// Make sure the replicas etc are back to what is expected.
					si, err := streamMoved.Info(ctx)
					if err != nil {
						return err
					}
					numExpected := si.Config.Replicas
					if ci.Config.Durable == _EMPTY_ {
						numExpected = 1
					}
					numPeers := len(ci.Cluster.Replicas)
					if ci.Cluster.Leader != _EMPTY_ {
						numPeers++
					}
					if numPeers != numExpected {
						return fmt.Errorf("Expected %d peers, got %d", numExpected, numPeers)
					}
					// If we are push check sub pending.
					if !isPull {
						checkSubsPending(t, sub, int(expectedPushMsgs)-toAck)
					}
					return nil
				})
			}

			// Refetch consumer handles (they may have been moved).
			dpushCons, err := js.Consumer(ctx, "MOVE", "dlc")
			require_NoError(t, err)
			ephName := epushCons.CachedInfo().Name
			epushCons2, err := js.Consumer(ctx, "MOVE", ephName)
			require_NoError(t, err)
			dpullCons2, err := js.Consumer(ctx, "MOVE", "dlc-pull")
			require_NoError(t, err)

			checkConsumer(dpushCons, dpushSub, false)
			checkConsumer(epushCons2, epushSub, false)
			checkConsumer(dpullCons2, nil, true)

			// Cleanup
			err = js.DeleteStream(ctx, "MOVE")
			require_NoError(t, err)
		})
	}
}

func TestJetStreamSuperClusterMovingStreamsWithMirror(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()

	nc, js := jsClientConnectNewAPI(t, sc.randomServer())
	defer nc.Close()
	ctx := context.Background()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "SOURCE",
		Subjects:  []string{"foo", "bar"},
		Replicas:  3,
		Placement: &jetstream.Placement{Tags: []string{"cloud:aws"}},
	})
	require_NoError(t, err)

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "MIRROR",
		Replicas:  1,
		Mirror:    &jetstream.StreamSource{Name: "SOURCE"},
		Placement: &jetstream.Placement{Tags: []string{"cloud:gcp"}},
	})
	require_NoError(t, err)

	done := make(chan struct{})
	exited := make(chan struct{})
	errors := make(chan error, 1)

	numNoResp := uint64(0)

	// We will run a separate routine and send at 100hz
	go func() {
		nc, js := jsClientConnectNewAPI(t, sc.randomServer())
		defer nc.Close()
		ctx := context.Background()

		defer close(exited)

		for {
			select {
			case <-done:
				return
			case <-time.After(10 * time.Millisecond):
				_, err := js.Publish(ctx, "foo", []byte("100HZ"))
				if err == nil {
				} else if err == jetstream.ErrNoStreamResponse {
					atomic.AddUint64(&numNoResp, 1)
					continue
				}
				if err != nil {
					errors <- err
					return
				}
			}
		}
	}()

	// Let it get going.
	time.Sleep(1500 * time.Millisecond)

	// Now move the source to a new cluster.
	_, err = js.UpdateStream(ctx, jetstream.StreamConfig{
		Name:      "SOURCE",
		Subjects:  []string{"foo", "bar"},
		Replicas:  3,
		Placement: &jetstream.Placement{Tags: []string{"cloud:gcp"}},
	})
	require_NoError(t, err)

	getInfo := func(name string) (*jetstream.StreamInfo, error) {
		sh, err := js.Stream(ctx, name)
		if err != nil {
			return nil, err
		}
		return sh.Info(ctx)
	}
	checkFor(t, 30*time.Second, 100*time.Millisecond, func() error {
		si, err := getInfo("SOURCE")
		if err != nil {
			return err
		}
		if si.Cluster.Name != "C2" {
			return fmt.Errorf("Wrong cluster: %q", si.Cluster.Name)
		}
		if si.Cluster.Leader == _EMPTY_ {
			return fmt.Errorf("No leader yet")
		} else if !strings.HasPrefix(si.Cluster.Leader, "C2-") {
			return fmt.Errorf("Wrong leader: %q", si.Cluster.Leader)
		}
		// Now we want to see that we shrink back to original.
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected %d replicas, got %d", 2, len(si.Cluster.Replicas))
		}
		// Let's get to 50+ msgs.
		if si.State.Msgs < 50 {
			return fmt.Errorf("Only see %d msgs", si.State.Msgs)
		}
		return nil
	})

	close(done)
	<-exited

	if nnr := atomic.LoadUint64(&numNoResp); nnr > 0 {
		if nnr > 5 {
			t.Fatalf("Expected no or very few failed message publishes, got %d", nnr)
		} else {
			t.Logf("Got a few failed publishes: %d", nnr)
		}
	}

	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		si, err := getInfo("SOURCE")
		require_NoError(t, err)
		mi, err := getInfo("MIRROR")
		require_NoError(t, err)

		if !reflect.DeepEqual(si.State, mi.State) {
			return fmt.Errorf("Expected mirror to be the same, got %+v vs %+v", mi.State, si.State)
		}
		return nil
	})

}

func TestJetStreamSuperClusterMovingStreamAndMoveBack(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()

	nc, js := jsClientConnectNewAPI(t, sc.randomServer())
	defer nc.Close()
	ctx := context.Background()

	for _, test := range []struct {
		name     string
		replicas int
	}{
		{"R1", 1},
		{"R3", 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			js.DeleteStream(ctx, "TEST")

			_, err := js.CreateStream(ctx, jetstream.StreamConfig{
				Name:      "TEST",
				Replicas:  test.replicas,
				Placement: &jetstream.Placement{Tags: []string{"cloud:aws"}},
			})
			require_NoError(t, err)

			toSend := 10_000
			for i := 0; i < toSend; i++ {
				_, err := js.Publish(ctx, "TEST", []byte("HELLO WORLD"))
				require_NoError(t, err)
			}

			_, err = js.UpdateStream(ctx, jetstream.StreamConfig{
				Name:      "TEST",
				Replicas:  test.replicas,
				Placement: &jetstream.Placement{Tags: []string{"cloud:gcp"}},
			})
			require_NoError(t, err)

			checkMove := func(cluster string) {
				t.Helper()
				sc.waitOnStreamLeader("$G", "TEST")
				checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
					sh, err := js.Stream(ctx, "TEST")
					if err != nil {
						return err
					}
					si, err := sh.Info(ctx)
					if err != nil {
						return err
					}
					if si.Cluster.Name != cluster {
						return fmt.Errorf("Wrong cluster: %q", si.Cluster.Name)
					}
					if si.Cluster.Leader == _EMPTY_ {
						return fmt.Errorf("No leader yet")
					} else if !strings.HasPrefix(si.Cluster.Leader, cluster) {
						return fmt.Errorf("Wrong leader: %q", si.Cluster.Leader)
					}
					// Now we want to see that we shrink back to original.
					if len(si.Cluster.Replicas) != test.replicas-1 {
						return fmt.Errorf("Expected %d replicas, got %d", test.replicas-1, len(si.Cluster.Replicas))
					}
					if si.State.Msgs != uint64(toSend) {
						return fmt.Errorf("Only see %d msgs", si.State.Msgs)
					}
					return nil
				})
			}

			checkMove("C2")

			// The move could be completed when looking at the stream info, but the meta leader could still
			// deny move updates for a short time while state is cleaned up.
			checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
				_, err = js.UpdateStream(ctx, jetstream.StreamConfig{
					Name:      "TEST",
					Replicas:  test.replicas,
					Placement: &jetstream.Placement{Tags: []string{"cloud:aws"}},
				})
				return err
			})

			checkMove("C1")
		})
	}
}

func TestJetStreamSuperClusterImportConsumerStreamSubjectRemap(t *testing.T) {
	template := `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, domain: HUB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts: {
		JS: {
			jetstream: enabled
			users: [ {user: js, password: pwd} ]
			exports [
				# This is streaming to a delivery subject for a push based consumer.
				{ stream: "deliver.ORDERS.*" }
			]
		},
		IM: {
			users: [ {user: im, password: pwd} ]
			imports [
				{ stream:  { account: JS, subject: "deliver.ORDERS.route" }}
				{ stream:  { account: JS, subject: "deliver.ORDERS.gateway" }}
				{ stream:  { account: JS, subject: "deliver.ORDERS.leaf1" }}
				{ stream:  { account: JS, subject: "deliver.ORDERS.leaf2" }}
			]
		},
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] },
	}
	leaf {
		listen: 127.0.0.1:-1
	}`

	test := func(t *testing.T, queue bool) {
		c := createJetStreamSuperClusterWithTemplate(t, template, 3, 2)
		defer c.shutdown()

		s := c.randomServer()
		nc, js := jsClientConnectNewAPI(t, s, nats.UserInfo("js", "pwd"))
		defer nc.Close()
		ctx := context.Background()

		_, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:      "ORDERS",
			Subjects:  []string{"foo"}, // The JS subject.
			Replicas:  3,
			Placement: &jetstream.Placement{Cluster: "C1"},
		})
		require_NoError(t, err)

		_, err = js.Publish(ctx, "foo", []byte("OK"))
		require_NoError(t, err)

		for dur, deliver := range map[string]string{
			"dur-route":   "deliver.ORDERS.route",
			"dur-gateway": "deliver.ORDERS.gateway",
			"dur-leaf-1":  "deliver.ORDERS.leaf1",
			"dur-leaf-2":  "deliver.ORDERS.leaf2",
		} {
			cfg := jetstream.ConsumerConfig{
				Durable:        dur,
				DeliverSubject: deliver,
				AckPolicy:      jetstream.AckExplicitPolicy,
			}
			if queue {
				cfg.DeliverGroup = "queue"
			}
			_, err = js.CreateConsumer(ctx, "ORDERS", cfg)
			require_NoError(t, err)
		}

		testCase := func(t *testing.T, s *Server, dSubj string) {
			nc2, err := nats.Connect(s.ClientURL(), nats.UserInfo("im", "pwd"))
			require_NoError(t, err)
			defer nc2.Close()

			var sub *nats.Subscription
			if queue {
				sub, err = nc2.QueueSubscribeSync(dSubj, "queue")
			} else {
				sub, err = nc2.SubscribeSync(dSubj)
			}
			require_NoError(t, err)

			m, err := sub.NextMsg(time.Second)
			require_NoError(t, err)

			if m.Subject != "foo" {
				t.Fatalf("Subject not mapped correctly across account boundary, expected %q got %q", "foo", m.Subject)
			}
			require_False(t, strings.Contains(m.Reply, "@"))
		}

		t.Run("route", func(t *testing.T) {
			// pick random non consumer leader so we receive via route
			s := c.clusterForName("C1").randomNonConsumerLeader("JS", "ORDERS", "dur-route")
			testCase(t, s, "deliver.ORDERS.route")
		})
		t.Run("gateway", func(t *testing.T) {
			// pick server with inbound gateway from consumer leader, so we receive from gateway and have no route in between
			scl := c.clusterForName("C1").consumerLeader("JS", "ORDERS", "dur-gateway")
			var sfound *Server
			for _, s := range c.clusterForName("C2").servers {
				s.mu.Lock()
				for _, c := range s.gateway.in {
					if c.GetName() == scl.info.ID {
						sfound = s
						break
					}
				}
				s.mu.Unlock()
				if sfound != nil {
					break
				}
			}
			testCase(t, sfound, "deliver.ORDERS.gateway")
		})
		t.Run("leaf-post-export", func(t *testing.T) {
			// create leaf node server connected post export/import
			scl := c.clusterForName("C1").consumerLeader("JS", "ORDERS", "dur-leaf-1")
			cf := createConfFile(t, []byte(fmt.Sprintf(`
			port: -1
			leafnodes {
				remotes [ { url: "nats://im:pwd@127.0.0.1:%d" } ]
			}
			authorization: {
				user: im,
				password: pwd
			}
		`, scl.getOpts().LeafNode.Port)))
			s, _ := RunServerWithConfig(cf)
			defer s.Shutdown()
			checkLeafNodeConnected(t, scl)
			testCase(t, s, "deliver.ORDERS.leaf1")
		})
		t.Run("leaf-pre-export", func(t *testing.T) {
			// create leaf node server connected pre export, perform export/import on leaf node server
			scl := c.clusterForName("C1").consumerLeader("JS", "ORDERS", "dur-leaf-2")
			cf := createConfFile(t, []byte(fmt.Sprintf(`
			port: -1
			leafnodes {
				remotes [ { url: "nats://js:pwd@127.0.0.1:%d", account: JS2 } ]
			}
			accounts: {
				JS2: {
					users: [ {user: js, password: pwd} ]
					exports [
						# This is streaming to a delivery subject for a push based consumer.
						{ stream: "deliver.ORDERS.leaf2" }
					]
				},
				IM2: {
					users: [ {user: im, password: pwd} ]
					imports [
						{ stream:  { account: JS2, subject: "deliver.ORDERS.leaf2" }}
					]
				},
			}
		`, scl.getOpts().LeafNode.Port)))
			s, _ := RunServerWithConfig(cf)
			defer s.Shutdown()
			checkLeafNodeConnected(t, scl)
			testCase(t, s, "deliver.ORDERS.leaf2")
		})
	}

	t.Run("noQueue", func(t *testing.T) {
		test(t, false)
	})
	t.Run("queue", func(t *testing.T) {
		test(t, true)
	})
}

func TestJetStreamSuperClusterMaxHaAssets(t *testing.T) {
	sc := createJetStreamSuperClusterWithTemplateAndModHook(t, `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s', limits: {max_ha_assets: 2}}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`, 3, 2,
		func(serverName, clusterName, storeDir, conf string) string {
			return conf
		}, nil)
	defer sc.shutdown()

	// speed up statsz reporting
	for _, c := range sc.clusters {
		for _, s := range c.servers {
			s.mu.Lock()
			s.sys.statsz = 10 * time.Millisecond
			s.sys.cstatsz = s.sys.statsz
			s.sys.stmr.Reset(s.sys.statsz)
			s.mu.Unlock()
		}
	}

	nc, js := jsClientConnectNewAPI(t, sc.randomServer())
	defer nc.Close()
	ctx := context.Background()

	ncSys := natsConnect(t, sc.randomServer().ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	defer ncSys.Close()
	statszSub, err := ncSys.SubscribeSync(fmt.Sprintf(serverStatsSubj, "*"))
	require_NoError(t, err)
	require_NoError(t, ncSys.Flush())

	waitStatsz := func(peers, haassets int) {
		t.Helper()
		for peersWithExactHaAssets := 0; peersWithExactHaAssets < peers; {
			m, err := statszSub.NextMsg(time.Second)
			require_NoError(t, err)
			var statsz ServerStatsMsg
			err = json.Unmarshal(m.Data, &statsz)
			require_NoError(t, err)
			if statsz.Stats.JetStream == nil {
				continue
			}
			if haassets == statsz.Stats.JetStream.Stats.HAAssets {
				peersWithExactHaAssets++
			}
		}
	}
	waitStatsz(6, 1) // counts _meta_
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "S0", Replicas: 1, Placement: &jetstream.Placement{Cluster: "C1"}})
	require_NoError(t, err)
	waitStatsz(6, 1)
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "S1", Replicas: 3, Placement: &jetstream.Placement{Cluster: "C1"}})
	require_NoError(t, err)
	waitStatsz(3, 2)
	waitStatsz(3, 1)
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "S2", Replicas: 3, Placement: &jetstream.Placement{Cluster: "C1"}})
	require_NoError(t, err)
	waitStatsz(3, 3)
	waitStatsz(3, 1)
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "S3", Replicas: 3, Placement: &jetstream.Placement{Cluster: "C1"}})
	require_Error(t, err)
	require_Contains(t, err.Error(), "nats: no suitable peers for placement")
	require_Contains(t, err.Error(), "miscellaneous issue")
	require_NoError(t, js.DeleteStream(ctx, "S1"))
	waitStatsz(3, 2)
	waitStatsz(3, 1)
	_, err = js.CreateConsumer(ctx, "S2", jetstream.ConsumerConfig{Durable: "DUR1", AckPolicy: jetstream.AckExplicitPolicy})
	require_NoError(t, err)
	waitStatsz(3, 3)
	waitStatsz(3, 1)
	_, err = js.CreateConsumer(ctx, "S2", jetstream.ConsumerConfig{Durable: "DUR2", AckPolicy: jetstream.AckExplicitPolicy})
	require_Error(t, err)
	require_Equal(t, err.Error(), "nats: insufficient resources")
	_, err = js.CreateConsumer(ctx, "S2", jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
	require_NoError(t, err)
	waitStatsz(3, 3)
	waitStatsz(3, 1)
	_, err = js.UpdateStream(ctx, jetstream.StreamConfig{Name: "S2", Replicas: 3, Description: "foobar"})
	require_NoError(t, err)
	waitStatsz(3, 3)
	waitStatsz(3, 1)
	sS4, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "S4", Replicas: 3})
	require_NoError(t, err)
	require_Equal(t, sS4.CachedInfo().Cluster.Name, "C2")
	waitStatsz(3, 3)
	waitStatsz(3, 2)
	sS5, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "S5", Replicas: 3})
	require_NoError(t, err)
	require_Equal(t, sS5.CachedInfo().Cluster.Name, "C2")
	waitStatsz(6, 3)
	_, err = js.CreateConsumer(ctx, "S4", jetstream.ConsumerConfig{Durable: "DUR2", AckPolicy: jetstream.AckExplicitPolicy})
	require_Error(t, err)
	require_Equal(t, err.Error(), "nats: insufficient resources")
	_, err = js.UpdateStream(ctx, jetstream.StreamConfig{Name: "S2", Replicas: 3, Placement: &jetstream.Placement{Cluster: "C2"}})
	require_Error(t, err)
	require_Contains(t, err.Error(), "nats: no suitable peers for placement")
	require_Contains(t, err.Error(), "miscellaneous issue")
}

func TestJetStreamSuperClusterStreamAlternates(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()

	nc, js := jsClientConnectNewAPI(t, sc.randomServer())
	defer nc.Close()
	ctx := context.Background()

	// C1
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "SOURCE",
		Subjects:  []string{"foo", "bar", "baz"},
		Replicas:  3,
		Placement: &jetstream.Placement{Tags: []string{"cloud:aws", "country:us"}},
	})
	require_NoError(t, err)

	// C2
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "MIRROR-1",
		Replicas:  1,
		Mirror:    &jetstream.StreamSource{Name: "SOURCE"},
		Placement: &jetstream.Placement{Tags: []string{"cloud:gcp", "country:uk"}},
	})
	require_NoError(t, err)

	// C3
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "MIRROR-2",
		Replicas:  2,
		Mirror:    &jetstream.StreamSource{Name: "SOURCE"},
		Placement: &jetstream.Placement{Tags: []string{"cloud:az", "country:jp"}},
	})
	require_NoError(t, err)

	// No client support yet, so do by hand.
	getStreamInfo := func(nc *nats.Conn, expected string) {
		t.Helper()
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "SOURCE"), nil, time.Second)
		require_NoError(t, err)
		var si StreamInfo
		err = json.Unmarshal(resp.Data, &si)
		require_NoError(t, err)
		require_True(t, len(si.Alternates) == 3)
		require_True(t, si.Alternates[0].Cluster == expected)
		seen := make(map[string]struct{})
		for _, alt := range si.Alternates {
			seen[alt.Cluster] = struct{}{}
		}
		require_True(t, len(seen) == 3)
	}

	// Connect to different clusters to check ordering.
	nc, _ = jsClientConnectNewAPI(t, sc.clusterForName("C1").randomServer())
	defer nc.Close()
	getStreamInfo(nc, "C1")
	nc, _ = jsClientConnectNewAPI(t, sc.clusterForName("C2").randomServer())
	defer nc.Close()
	getStreamInfo(nc, "C2")
	nc, _ = jsClientConnectNewAPI(t, sc.clusterForName("C3").randomServer())
	defer nc.Close()
	getStreamInfo(nc, "C3")
}

// We had a scenario where a consumer would not recover properly on restart due to
// the cluster state not being set properly when checking source subjects.
func TestJetStreamSuperClusterStateOnRestartPreventsConsumerRecovery(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()

	nc, js := jsClientConnectNewAPI(t, sc.randomServer())
	defer nc.Close()
	ctx := context.Background()

	// C1
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "SOURCE",
		Subjects:  []string{"foo", "bar"},
		Replicas:  3,
		Placement: &jetstream.Placement{Tags: []string{"cloud:aws", "country:us"}},
	})
	require_NoError(t, err)

	// C2
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "DS",
		Subjects:  []string{"baz"},
		Replicas:  3,
		Sources:   []*jetstream.StreamSource{{Name: "SOURCE"}},
		Placement: &jetstream.Placement{Tags: []string{"cloud:gcp", "country:uk"}},
	})
	require_NoError(t, err)

	// Bind to DS and match filter subject of SOURCE.
	_, err = js.CreateConsumer(ctx, "DS", jetstream.ConsumerConfig{
		Durable:        "dlc",
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubject:  "foo",
		DeliverSubject: "d",
	})
	require_NoError(t, err)

	// Send a few messages.
	for i := 0; i < 100; i++ {
		_, err := js.Publish(ctx, "foo", []byte("HELLO"))
		require_NoError(t, err)
	}
	sub := natsSubSync(t, nc, "d")
	natsNexMsg(t, sub, 5*time.Second)

	c := sc.clusterForName("C2")
	cl := c.consumerLeader("$G", "DS", "dlc")

	// Pull source out from underneath the downstream stream.
	err = js.DeleteStream(ctx, "SOURCE")
	require_NoError(t, err)

	cl.Shutdown()
	cl = c.restartServer(cl)
	c.waitOnServerHealthz(cl)

	// Now make sure the consumer is still on this server and has restarted properly.
	mset, err := cl.GlobalAccount().lookupStream("DS")
	require_NoError(t, err)
	if o := mset.lookupConsumer("dlc"); o == nil {
		t.Fatalf("Consumer was not properly restarted")
	}
}

// We allow mirrors to opt-in to direct get in a distributed queue group.
func TestJetStreamSuperClusterStreamDirectGetMirrorQueueGroup(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()

	nc, js := jsClientConnectNewAPI(t, sc.randomServer())
	defer nc.Close()
	ctx := context.Background()

	// C1
	// Do by hand for now.
	cfg := &StreamConfig{
		Name:        "SOURCE",
		Subjects:    []string{"kv.>"},
		MaxMsgsPer:  1,
		Placement:   &Placement{Tags: []string{"cloud:aws", "country:us"}},
		AllowDirect: true,
		Replicas:    3,
		Storage:     MemoryStorage,
	}
	addStream(t, nc, cfg)

	num := 100
	for i := 0; i < num; i++ {
		js.PublishAsync(fmt.Sprintf("kv.%d", i), []byte("VAL"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// C2
	cfg = &StreamConfig{
		Name:         "M1",
		Mirror:       &StreamSource{Name: "SOURCE"},
		Placement:    &Placement{Tags: []string{"cloud:gcp", "country:uk"}},
		MirrorDirect: true,
		Storage:      MemoryStorage,
	}
	addStream(t, nc, cfg)

	// C3 (clustered)
	cfg = &StreamConfig{
		Name:         "M2",
		Mirror:       &StreamSource{Name: "SOURCE"},
		Replicas:     3,
		Placement:    &Placement{Tags: []string{"country:jp"}},
		MirrorDirect: true,
		Storage:      MemoryStorage,
	}
	addStream(t, nc, cfg)

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		sh, err := js.Stream(ctx, "M2")
		require_NoError(t, err)
		si, err := sh.Info(ctx)
		require_NoError(t, err)
		if si.State.Msgs != uint64(num) {
			return fmt.Errorf("Expected %d msgs, got state: %d", num, si.State.Msgs)
		}
		return nil
	})

	// Since last one was an R3, check and wait for the direct subscription.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		sl := sc.clusterForName("C3").streamLeader("$G", "M2")
		if mset, err := sl.GlobalAccount().lookupStream("M2"); err == nil {
			mset.mu.RLock()
			ok := mset.mirrorDirectSub != nil
			mset.mu.RUnlock()
			if ok {
				return nil
			}
		}
		return fmt.Errorf("No dsub yet")
	})

	// Always do a direct get to the source, but check that we are getting answers from the mirrors when connected to their cluster.
	getSubj := fmt.Sprintf(JSDirectMsgGetT, "SOURCE")
	req := []byte(`{"last_by_subj":"kv.22"}`)
	getMsg := func(c *nats.Conn) *nats.Msg {
		m, err := c.Request(getSubj, req, time.Second)
		require_NoError(t, err)
		require_True(t, string(m.Data) == "VAL")
		require_True(t, m.Header.Get(JSSequence) == "23")
		require_True(t, m.Header.Get(JSSubject) == "kv.22")
		return m
	}

	// C1 -> SOURCE
	nc, _ = jsClientConnectNewAPI(t, sc.clusterForName("C1").randomServer())
	defer nc.Close()

	m := getMsg(nc)
	require_True(t, m.Header.Get(JSStream) == "SOURCE")

	// C2 -> M1
	nc, _ = jsClientConnectNewAPI(t, sc.clusterForName("C2").randomServer())
	defer nc.Close()

	m = getMsg(nc)
	require_True(t, m.Header.Get(JSStream) == "M1")

	// C3 -> M2
	nc, _ = jsClientConnectNewAPI(t, sc.clusterForName("C3").randomServer())
	defer nc.Close()

	m = getMsg(nc)
	require_True(t, m.Header.Get(JSStream) == "M2")
}

func TestJetStreamSuperClusterTagInducedMoveCancel(t *testing.T) {
	server := map[string]struct{}{}
	sc := createJetStreamSuperClusterWithTemplateAndModHook(t, jsClusterTempl, 4, 2,
		func(serverName, clusterName, storeDir, conf string) string {
			server[serverName] = struct{}{}
			return fmt.Sprintf("%s\nserver_tags: [%s]", conf, clusterName)
		}, nil)
	defer sc.shutdown()

	// Client based API
	c := sc.randomCluster()
	srv := c.randomNonLeader()
	nc, js := jsClientConnectNewAPI(t, srv)
	defer nc.Close()
	ctx := context.Background()

	cfg := jetstream.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Placement: &jetstream.Placement{Tags: []string{"C1"}},
		Replicas:  3,
	}
	sCreate, err := js.CreateStream(ctx, cfg)
	require_NoError(t, err)
	require_Equal(t, sCreate.CachedInfo().Cluster.Name, "C1")

	toSend := uint64(1_000)
	for i := uint64(0); i < toSend; i++ {
		_, err = js.Publish(ctx, "foo", nil)
		require_NoError(t, err)
	}

	ncsys, err := nats.Connect(srv.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer ncsys.Close()

	// cause a move by altering placement tags
	cfg.Placement.Tags = []string{"C2"}
	_, err = js.UpdateStream(ctx, cfg)
	require_NoError(t, err)

	// Retry in case stream is still being created and without a leader we won't receive a response
	var cancelResp JSApiStreamUpdateResponse
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		rmsg, err := ncsys.Request(fmt.Sprintf(JSApiServerStreamCancelMoveT, "$G", "TEST"), nil, 1*time.Second)
		if errors.Is(err, nats.ErrTimeout) {
			return err
		}
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(rmsg.Data, &cancelResp))
		return nil
	})
	if cancelResp.Error != nil && ErrorIdentifier(cancelResp.Error.ErrCode) == JSStreamMoveNotInProgress {
		t.Skip("This can happen with delays, when Move completed before Cancel", cancelResp.Error)
		return
	}
	require_True(t, cancelResp.Error == nil)

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		sh, err := js.Stream(ctx, "TEST")
		require_NoError(t, err)
		si, err := sh.Info(ctx)
		require_NoError(t, err)
		if si.Config.Placement != nil {
			return fmt.Errorf("expected placement to be cleared got: %+v", si.Config.Placement)
		}
		return nil
	})
}

func TestJetStreamSuperClusterMoveCancel(t *testing.T) {
	usageTickOld := usageTick
	usageTick = 250 * time.Millisecond
	defer func() {
		usageTick = usageTickOld
	}()

	server := map[string]struct{}{}
	sc := createJetStreamSuperClusterWithTemplateAndModHook(t, jsClusterTempl, 4, 2,
		func(serverName, clusterName, storeDir, conf string) string {
			server[serverName] = struct{}{}
			return fmt.Sprintf("%s\nserver_tags: [%s]", conf, serverName)
		}, nil)
	defer sc.shutdown()

	// Client based API
	c := sc.randomCluster()
	srv := c.randomNonLeader()
	nc, js := jsClientConnectNewAPI(t, srv)
	defer nc.Close()
	ctx := context.Background()

	sCreate, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	siCreate := sCreate.CachedInfo()
	streamPeerSrv := []string{siCreate.Cluster.Leader, siCreate.Cluster.Replicas[0].Name, siCreate.Cluster.Replicas[1].Name}
	// determine empty server
	for _, s := range streamPeerSrv {
		delete(server, s)
	}
	// pick left over server in same cluster as other server
	emptySrv := _EMPTY_
	for s := range server {
		// server name is prefixed with cluster name
		if strings.HasPrefix(s, c.name) {
			emptySrv = s
			break
		}
	}

	expectedPeers := map[string]struct{}{
		getHash(streamPeerSrv[0]): {},
		getHash(streamPeerSrv[1]): {},
		getHash(streamPeerSrv[2]): {},
	}

	_, err = js.CreateConsumer(ctx, "TEST", jetstream.ConsumerConfig{Durable: "DUR", AckPolicy: jetstream.AckExplicitPolicy})
	require_NoError(t, err)
	consEph, err := js.CreateConsumer(ctx, "TEST", jetstream.ConsumerConfig{InactiveThreshold: time.Hour, AckPolicy: jetstream.AckExplicitPolicy})
	require_NoError(t, err)
	ephName := consEph.CachedInfo().Name

	toSend := uint64(1_000)
	for i := uint64(0); i < toSend; i++ {
		_, err = js.Publish(ctx, "foo", nil)
		require_NoError(t, err)
	}

	serverEmpty := func(fromSrv string) error {
		if jszAfter, err := c.serverByName(fromSrv).Jsz(nil); err != nil {
			return fmt.Errorf("could not fetch JS info for server: %v", err)
		} else if jszAfter.Streams != 0 {
			return fmt.Errorf("empty server still has %d streams", jszAfter.Streams)
		} else if jszAfter.Consumers != 0 {
			return fmt.Errorf("empty server still has %d consumers", jszAfter.Consumers)
		} else if jszAfter.Bytes != 0 {
			return fmt.Errorf("empty server still has %d storage", jszAfter.Store)
		}
		return nil
	}

	checkSrvInvariant := func(s *Server, expectedPeers map[string]struct{}) error {
		js, cc := s.getJetStreamCluster()
		js.mu.Lock()
		defer js.mu.Unlock()
		if sa, ok := cc.streams["$G"]["TEST"]; !ok {
			return fmt.Errorf("stream not found")
		} else if len(sa.Group.Peers) != len(expectedPeers) {
			return fmt.Errorf("stream peer group size not %d, but %d", len(expectedPeers), len(sa.Group.Peers))
		} else if da, ok := sa.consumers["DUR"]; !ok {
			return fmt.Errorf("durable not found")
		} else if len(da.Group.Peers) != len(expectedPeers) {
			return fmt.Errorf("durable peer group size not %d, but %d", len(expectedPeers), len(da.Group.Peers))
		} else if ea, ok := sa.consumers[ephName]; !ok {
			return fmt.Errorf("ephemeral not found")
		} else if len(ea.Group.Peers) != 1 {
			return fmt.Errorf("ephemeral peer group size not 1, but %d", len(ea.Group.Peers))
		} else if _, ok := expectedPeers[ea.Group.Peers[0]]; !ok {
			return fmt.Errorf("ephemeral peer not an expected peer")
		} else {
			for _, p := range sa.Group.Peers {
				if _, ok := expectedPeers[p]; !ok {
					return fmt.Errorf("peer not expected")
				}
				found := false
				for _, dp := range da.Group.Peers {
					if p == dp {
						found = true
						break
					}
				}
				if !found {
					t.Logf("durable peer group does not match stream peer group")
				}
			}
		}
		return nil
	}

	ncsys, err := nats.Connect(srv.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer ncsys.Close()

	time.Sleep(2 * usageTick)
	aiBefore, err := js.AccountInfo(ctx)
	require_NoError(t, err)

	for _, moveFromSrv := range streamPeerSrv {
		moveReq, err := json.Marshal(&JSApiMetaServerStreamMoveRequest{Server: moveFromSrv, Tags: []string{emptySrv}})
		require_NoError(t, err)
		rmsg, err := ncsys.Request(fmt.Sprintf(JSApiServerStreamMoveT, "$G", "TEST"), moveReq, 5*time.Second)
		require_NoError(t, err)
		var moveResp JSApiStreamUpdateResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &moveResp))
		require_True(t, moveResp.Error == nil)

		rmsg, err = ncsys.Request(fmt.Sprintf(JSApiServerStreamCancelMoveT, "$G", "TEST"), nil, 5*time.Second)
		require_NoError(t, err)
		var cancelResp JSApiStreamUpdateResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &cancelResp))
		if cancelResp.Error != nil && ErrorIdentifier(cancelResp.Error.ErrCode) == JSStreamMoveNotInProgress {
			t.Skip("This can happen with delays, when Move completed before Cancel", cancelResp.Error)
			return
		}
		require_True(t, cancelResp.Error == nil)

		for _, sExpected := range streamPeerSrv {
			s := c.serverByName(sExpected)
			require_True(t, s.JetStreamIsStreamAssigned("$G", "TEST"))
			checkFor(t, 20*time.Second, 100*time.Millisecond, func() error { return checkSrvInvariant(s, expectedPeers) })
		}
		checkFor(t, 10*time.Second, 100*time.Millisecond, func() error { return serverEmpty(emptySrv) })
		checkFor(t, 3*usageTick, 100*time.Millisecond, func() error {
			if aiAfter, err := js.AccountInfo(ctx); err != nil {
				return err
			} else if aiAfter.Store != aiBefore.Store {
				return fmt.Errorf("store before %d and after %d don't match", aiBefore.Store, aiAfter.Store)
			} else {
				return nil
			}
		})
	}
}

func TestJetStreamSuperClusterDoubleStreamMove(t *testing.T) {
	server := map[string]struct{}{}
	sc := createJetStreamSuperClusterWithTemplateAndModHook(t, jsClusterTempl, 4, 2,
		func(serverName, clusterName, storeDir, conf string) string {
			server[serverName] = struct{}{}
			return fmt.Sprintf("%s\nserver_tags: [%s]", conf, serverName)
		}, nil)
	defer sc.shutdown()

	// Client based API
	c := sc.randomCluster()
	srv := c.randomNonLeader()
	nc, js := jsClientConnectNewAPI(t, srv)
	defer nc.Close()
	ctx := context.Background()

	sCreate, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	siCreate := sCreate.CachedInfo()
	srvMoveList := []string{siCreate.Cluster.Leader, siCreate.Cluster.Replicas[0].Name, siCreate.Cluster.Replicas[1].Name}
	// determine empty server
	for _, s := range srvMoveList {
		delete(server, s)
	}
	// pick left over server in same cluster as other server
	for s := range server {
		// server name is prefixed with cluster name
		if strings.HasPrefix(s, c.name) {
			srvMoveList = append(srvMoveList, s)
			break
		}
	}

	servers := []*Server{
		c.serverByName(srvMoveList[0]),
		c.serverByName(srvMoveList[1]),
		c.serverByName(srvMoveList[2]),
		c.serverByName(srvMoveList[3]), // starts out empty
	}

	_, err = js.CreateConsumer(ctx, "TEST", jetstream.ConsumerConfig{Durable: "DUR", AckPolicy: jetstream.AckExplicitPolicy})
	require_NoError(t, err)
	consEph, err := js.CreateConsumer(ctx, "TEST", jetstream.ConsumerConfig{InactiveThreshold: time.Hour, AckPolicy: jetstream.AckExplicitPolicy})
	require_NoError(t, err)
	ephName := consEph.CachedInfo().Name

	toSend := uint64(100)
	for i := uint64(0); i < toSend; i++ {
		_, err = js.Publish(ctx, "foo", nil)
		require_NoError(t, err)
	}

	ncsys, err := nats.Connect(srv.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer ncsys.Close()

	move := func(fromSrv string, toTags ...string) {
		sEmpty := c.serverByName(fromSrv)
		jszBefore, err := sEmpty.Jsz(nil)
		require_NoError(t, err)
		require_True(t, jszBefore.Streams == 1)

		moveReq, err := json.Marshal(&JSApiMetaServerStreamMoveRequest{
			Server: fromSrv, Tags: toTags})
		require_NoError(t, err)
		rmsg, err := ncsys.Request(fmt.Sprintf(JSApiServerStreamMoveT, "$G", "TEST"), moveReq, 100*time.Second)
		require_NoError(t, err)
		var moveResp JSApiStreamUpdateResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &moveResp))
		require_True(t, moveResp.Error == nil)
	}

	serverEmpty := func(fromSrv string) error {
		if jszAfter, err := c.serverByName(fromSrv).Jsz(nil); err != nil {
			return fmt.Errorf("could not fetch JS info for server: %v", err)
		} else if jszAfter.Streams != 0 {
			return fmt.Errorf("empty server still has %d streams", jszAfter.Streams)
		} else if jszAfter.Consumers != 0 {
			return fmt.Errorf("empty server still has %d consumers", jszAfter.Consumers)
		} else if jszAfter.Store != 0 {
			return fmt.Errorf("empty server still has %d storage", jszAfter.Store)
		}
		return nil
	}

	moveComplete := func(toSrv string, expectedSet ...string) error {
		eSet := map[string]int{}
		foundInExpected := false
		for i, sExpected := range expectedSet {
			eSet[sExpected] = i
			s := c.serverByName(sExpected)
			if !s.JetStreamIsStreamAssigned("$G", "TEST") {
				return fmt.Errorf("expected stream to be assigned to %s", sExpected)
			}
			// test list order invariant
			js, cc := s.getJetStreamCluster()
			sExpHash := getHash(sExpected)
			js.mu.Lock()
			if sa, ok := cc.streams["$G"]["TEST"]; !ok {
				js.mu.Unlock()
				return fmt.Errorf("stream not found in cluster")
			} else if len(sa.Group.Peers) != 3 {
				js.mu.Unlock()
				return fmt.Errorf("peers not reset")
			} else if sa.Group.Peers[i] != sExpHash {
				js.mu.Unlock()
				return fmt.Errorf("stream: expected peer %s on index %d, got %s/%s",
					sa.Group.Peers[i], i, sExpHash, sExpected)
			} else if ca, ok := sa.consumers["DUR"]; !ok {
				js.mu.Unlock()
				return fmt.Errorf("durable not found in stream")
			} else {
				if !slices.Contains(ca.Group.Peers, sExpHash) {
					js.mu.Unlock()
					return fmt.Errorf("consumer expected peer %s/%s bud didn't find in %+v",
						sExpHash, sExpected, ca.Group.Peers)
				}
				if ephA, ok := sa.consumers[ephName]; ok {
					if len(ephA.Group.Peers) != 1 {
						return fmt.Errorf("ephemeral peers not reset")
					}
					foundInExpected = foundInExpected || (ephA.Group.Peers[0] == cc.meta.ID())
				}
			}
			js.mu.Unlock()
		}
		if len(expectedSet) > 0 && !foundInExpected {
			return fmt.Errorf("ephemeral peer not expected")
		}
		for _, s := range servers {
			if jszAfter, err := c.serverByName(toSrv).Jsz(nil); err != nil {
				return fmt.Errorf("could not fetch JS info for server: %v", err)
			} else if jszAfter.Messages != toSend {
				return fmt.Errorf("messages not yet copied, got %d, expected %d", jszAfter.Messages, toSend)
			}
			nc, js := jsClientConnectNewAPI(t, s)
			defer nc.Close()
			cctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			sh, err := js.Stream(cctx, "TEST")
			if err != nil {
				return fmt.Errorf("could not fetch stream info: %v", err)
			}
			if si, err := sh.Info(cctx); err != nil {
				return fmt.Errorf("could not fetch stream info: %v", err)
			} else if len(si.Cluster.Replicas)+1 != si.Config.Replicas {
				return fmt.Errorf("not yet downsized replica should be empty has: %d != %d (%s)",
					len(si.Cluster.Replicas)+1, si.Config.Replicas, si.Cluster.Leader)
			} else if si.Cluster.Leader == _EMPTY_ {
				return fmt.Errorf("leader not found")
			} else if len(expectedSet) > 0 {
				if _, ok := eSet[si.Cluster.Leader]; !ok {
					return fmt.Errorf("leader %s not in expected set %+v", si.Cluster.Leader, eSet)
				} else if _, ok := eSet[si.Cluster.Replicas[0].Name]; !ok {
					return fmt.Errorf("leader %s not in expected set %+v", si.Cluster.Replicas[0].Name, eSet)
				} else if _, ok := eSet[si.Cluster.Replicas[1].Name]; !ok {
					return fmt.Errorf("leader %s not in expected set %+v", si.Cluster.Replicas[1].Name, eSet)
				}
			}
			nc.Close()
		}
		return nil
	}

	moveAndCheck := func(from, to string, expectedSet ...string) {
		t.Helper()
		move(from, to)
		checkFor(t, 40*time.Second, 100*time.Millisecond, func() error { return moveComplete(to, expectedSet...) })
		checkFor(t, 20*time.Second, 100*time.Millisecond, func() error { return serverEmpty(from) })
	}

	checkFor(t, 20*time.Second, 1000*time.Millisecond, func() error { return serverEmpty(srvMoveList[3]) })
	// first iteration establishes order of server 0-2 (the internal order in the server could be 1,0,2)
	moveAndCheck(srvMoveList[0], srvMoveList[3])
	moveAndCheck(srvMoveList[1], srvMoveList[0])
	moveAndCheck(srvMoveList[2], srvMoveList[1])
	moveAndCheck(srvMoveList[3], srvMoveList[2], srvMoveList[0], srvMoveList[1], srvMoveList[2])
	// second iteration iterates in order
	moveAndCheck(srvMoveList[0], srvMoveList[3], srvMoveList[1], srvMoveList[2], srvMoveList[3])
	moveAndCheck(srvMoveList[1], srvMoveList[0], srvMoveList[2], srvMoveList[3], srvMoveList[0])
	moveAndCheck(srvMoveList[2], srvMoveList[1], srvMoveList[3], srvMoveList[0], srvMoveList[1])
	moveAndCheck(srvMoveList[3], srvMoveList[2], srvMoveList[0], srvMoveList[1], srvMoveList[2])
	// iterate in the opposite direction and establish order 2-0
	moveAndCheck(srvMoveList[2], srvMoveList[3], srvMoveList[0], srvMoveList[1], srvMoveList[3])
	moveAndCheck(srvMoveList[1], srvMoveList[2], srvMoveList[0], srvMoveList[3], srvMoveList[2])
	moveAndCheck(srvMoveList[0], srvMoveList[1], srvMoveList[3], srvMoveList[2], srvMoveList[1])
	moveAndCheck(srvMoveList[3], srvMoveList[0], srvMoveList[2], srvMoveList[1], srvMoveList[0])
	// move server in the middle of list
	moveAndCheck(srvMoveList[1], srvMoveList[3], srvMoveList[2], srvMoveList[0], srvMoveList[3])
	moveAndCheck(srvMoveList[0], srvMoveList[1], srvMoveList[2], srvMoveList[3], srvMoveList[1])
	moveAndCheck(srvMoveList[3], srvMoveList[0], srvMoveList[2], srvMoveList[1], srvMoveList[0])
	// repeatedly use end
	moveAndCheck(srvMoveList[0], srvMoveList[3], srvMoveList[2], srvMoveList[1], srvMoveList[3])
	moveAndCheck(srvMoveList[3], srvMoveList[0], srvMoveList[2], srvMoveList[1], srvMoveList[0])
	moveAndCheck(srvMoveList[0], srvMoveList[3], srvMoveList[2], srvMoveList[1], srvMoveList[3])
	moveAndCheck(srvMoveList[3], srvMoveList[0], srvMoveList[2], srvMoveList[1], srvMoveList[0])
}

func TestJetStreamSuperClusterPeerEvacuationAndStreamReassignment(t *testing.T) {
	s := createJetStreamSuperClusterWithTemplateAndModHook(t, jsClusterTempl, 4, 2,
		func(serverName, clusterName, storeDir, conf string) string {
			return fmt.Sprintf("%s\nserver_tags: [cluster:%s, server:%s]", conf, clusterName, serverName)
		}, nil)
	defer s.shutdown()

	c := s.clusterForName("C1")

	// Client based API
	srv := c.randomNonLeader()
	nc, js := jsClientConnectNewAPI(t, srv)
	defer nc.Close()
	ctx := context.Background()

	test := func(t *testing.T, r int, moveTags []string, targetCluster string, testMigrateTo bool, listFrom bool) {
		stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: r,
		})
		require_NoError(t, err)
		defer js.DeleteStream(ctx, "TEST")
		si := stream.CachedInfo()
		startSet := map[string]struct{}{
			si.Cluster.Leader: {},
		}
		for _, p := range si.Cluster.Replicas {
			startSet[p.Name] = struct{}{}
		}

		_, err = js.CreateConsumer(ctx, "TEST", jetstream.ConsumerConfig{Durable: "DUR", AckPolicy: jetstream.AckExplicitPolicy})
		require_NoError(t, err)

		// Create ephemeral push consumer on "foo"
		inbox := nats.NewInbox()
		_, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			DeliverSubject: inbox,
			FilterSubject:  "foo",
			AckPolicy:      jetstream.AckNonePolicy,
		})
		require_NoError(t, err)
		sub, err := nc.SubscribeSync(inbox)
		require_NoError(t, err)

		for i := 0; i < 100; i++ {
			_, err = js.Publish(ctx, "foo", nil)
			require_NoError(t, err)
		}

		toMoveFrom := si.Cluster.Leader
		if !listFrom {
			toMoveFrom = _EMPTY_
		}
		sLdr := c.serverByName(si.Cluster.Leader)
		jszBefore, err := sLdr.Jsz(nil)
		require_NoError(t, err)
		require_True(t, jszBefore.Streams == 1)
		require_True(t, jszBefore.Consumers >= 1)
		require_True(t, jszBefore.Store != 0)

		migrateToServer := _EMPTY_
		if testMigrateTo {
			// find an empty server
			for _, s := range c.servers {
				name := s.Name()
				found := si.Cluster.Leader == name
				if !found {
					for _, r := range si.Cluster.Replicas {
						if r.Name == name {
							found = true
							break
						}
					}
				}
				if !found {
					migrateToServer = name
					break
				}
			}
			jszAfter, err := c.serverByName(migrateToServer).Jsz(nil)
			require_NoError(t, err)
			require_True(t, jszAfter.Streams == 0)

			moveTags = append(moveTags, fmt.Sprintf("server:%s", migrateToServer))
		}

		ncsys, err := nats.Connect(srv.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
		require_NoError(t, err)
		defer ncsys.Close()

		moveReq, err := json.Marshal(&JSApiMetaServerStreamMoveRequest{Server: toMoveFrom, Tags: moveTags})
		require_NoError(t, err)
		rmsg, err := ncsys.Request(fmt.Sprintf(JSApiServerStreamMoveT, "$G", "TEST"), moveReq, 100*time.Second)
		require_NoError(t, err)
		var moveResp JSApiStreamUpdateResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &moveResp))
		require_True(t, moveResp.Error == nil)

		// test move to particular server
		if testMigrateTo {
			toSrv := c.serverByName(migrateToServer)
			checkFor(t, 20*time.Second, 1000*time.Millisecond, func() error {
				jszAfter, err := toSrv.Jsz(nil)
				if err != nil {
					return fmt.Errorf("could not fetch JS info for server: %v", err)
				}
				if jszAfter.Streams != 1 {
					return fmt.Errorf("server expected to have one stream, has %d", jszAfter.Streams)
				}
				return nil
			})
		}
		// Now wait until the stream is now current.
		checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
			cctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			sh, err := js.Stream(cctx, "TEST")
			if err != nil {
				return fmt.Errorf("could not fetch stream info: %v", err)
			}
			si, err := sh.Info(cctx)
			if err != nil {
				return fmt.Errorf("could not fetch stream info: %v", err)
			}
			if si.Cluster.Leader == toMoveFrom {
				return fmt.Errorf("peer not removed yet: %+v", toMoveFrom)
			}
			if si.Cluster.Leader == _EMPTY_ {
				return fmt.Errorf("no leader yet")
			}
			if len(si.Cluster.Replicas) != r-1 {
				return fmt.Errorf("not yet downsized replica should be %d has: %d", r-1, len(si.Cluster.Replicas))
			}
			if si.Config.Replicas != r {
				return fmt.Errorf("bad replica count %d", si.Config.Replicas)
			}
			if si.Cluster.Name != targetCluster {
				return fmt.Errorf("stream expected in %s but found in %s", si.Cluster.Name, targetCluster)
			}
			sNew := s.serverByName(si.Cluster.Leader)
			if jszNew, err := sNew.Jsz(nil); err != nil {
				return err
			} else if jszNew.Streams != 1 {
				return fmt.Errorf("new leader has %d streams, not one", jszNew.Streams)
			} else if jszNew.Store != jszBefore.Store {
				return fmt.Errorf("new leader has %d storage, should have %d", jszNew.Store, jszBefore.Store)
			}
			return nil
		})
		// test draining
		checkFor(t, 20*time.Second, time.Second, func() error {
			if !listFrom {
				// when needed determine which server move moved away from
				cctx, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				sh, err := js.Stream(cctx, "TEST")
				if err != nil {
					return fmt.Errorf("could not fetch stream info: %v", err)
				}
				si, err := sh.Info(cctx)
				if err != nil {
					return fmt.Errorf("could not fetch stream info: %v", err)
				}
				for n := range startSet {
					if n != si.Cluster.Leader {
						var found bool
						for _, p := range si.Cluster.Replicas {
							if p.Name == n {
								found = true
								break
							}
						}
						if !found {
							toMoveFrom = n
						}
					}
				}
			}
			if toMoveFrom == _EMPTY_ {
				return fmt.Errorf("server to move away from not found")
			}
			sEmpty := c.serverByName(toMoveFrom)
			jszAfter, err := sEmpty.Jsz(nil)
			if err != nil {
				return fmt.Errorf("could not fetch JS info for server: %v", err)
			}
			if jszAfter.Streams != 0 {
				return fmt.Errorf("empty server still has %d streams", jszAfter.Streams)
			}
			if jszAfter.Consumers != 0 {
				return fmt.Errorf("empty server still has %d consumers", jszAfter.Consumers)
			}
			if jszAfter.Store != 0 {
				return fmt.Errorf("empty server still has %d storage", jszAfter.Store)
			}
			return nil
		})
		// consume messages from ephemeral consumer
		for i := 0; i < 100; i++ {
			_, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
		}
	}

	for i := 1; i <= 3; i++ {
		t.Run(fmt.Sprintf("r%d", i), func(t *testing.T) {
			test(t, i, nil, "C1", false, true)
		})
		t.Run(fmt.Sprintf("r%d-explicit", i), func(t *testing.T) {
			test(t, i, nil, "C1", true, true)
		})
		t.Run(fmt.Sprintf("r%d-nosrc", i), func(t *testing.T) {
			test(t, i, nil, "C1", false, false)
		})
	}

	t.Run("r3-cluster-move", func(t *testing.T) {
		test(t, 3, []string{"cluster:C2"}, "C2", false, false)
	})
	t.Run("r3-cluster-move-nosrc", func(t *testing.T) {
		test(t, 3, []string{"cluster:C2"}, "C2", false, true)
	})
}

func TestJetStreamSuperClusterMirrorInheritsAllowDirect(t *testing.T) {
	sc := createJetStreamTaggedSuperCluster(t)
	defer sc.shutdown()

	nc, js := jsClientConnectNewAPI(t, sc.randomServer())
	defer nc.Close()
	ctx := context.Background()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:              "KV",
		Subjects:          []string{"key.*"},
		Placement:         &jetstream.Placement{Tags: []string{"cloud:aws", "country:us"}},
		MaxMsgsPerSubject: 1,
		AllowDirect:       true,
	})
	require_NoError(t, err)

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "M",
		Mirror:    &jetstream.StreamSource{Name: "KV"},
		Placement: &jetstream.Placement{Tags: []string{"cloud:gcp", "country:uk"}},
	})
	require_NoError(t, err)

	// Do direct grab for now.
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "M"), nil, time.Second)
	require_NoError(t, err)
	var si StreamInfo
	err = json.Unmarshal(resp.Data, &si)
	require_NoError(t, err)

	if !si.Config.MirrorDirect {
		t.Fatalf("Expected MirrorDirect to be inherited as true")
	}
}

func TestJetStreamSuperClusterSystemLimitsPlacement(t *testing.T) {
	const largeSystemLimit = 1024
	const smallSystemLimit = 512

	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {
			max_mem_store: _MAXMEM_
			max_file_store: _MAXFILE_
			store_dir: '%s',
		}
		server_tags: [
			_TAG_
		]
		leaf {
			listen: 127.0.0.1:-1
		}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	storeCnf := func(serverName, clusterName, storeDir, conf string) string {
		switch {
		case strings.HasPrefix(serverName, "C1"):
			conf = strings.Replace(conf, "_MAXMEM_", fmt.Sprint(largeSystemLimit), 1)
			conf = strings.Replace(conf, "_MAXFILE_", fmt.Sprint(largeSystemLimit), 1)
			return strings.Replace(conf, "_TAG_", serverName, 1)
		case strings.HasPrefix(serverName, "C2"):
			conf = strings.Replace(conf, "_MAXMEM_", fmt.Sprint(smallSystemLimit), 1)
			conf = strings.Replace(conf, "_MAXFILE_", fmt.Sprint(smallSystemLimit), 1)
			return strings.Replace(conf, "_TAG_", serverName, 1)
		default:
			return conf
		}
	}

	sCluster := createJetStreamSuperClusterWithTemplateAndModHook(t, tmpl, 3, 2, storeCnf, nil)
	defer sCluster.shutdown()

	requestLeaderStepDown := func(clientURL string) error {
		nc, err := nats.Connect(clientURL, nats.UserInfo("admin", "s3cr3t!"))
		if err != nil {
			return err
		}
		defer nc.Close()

		ncResp, err := nc.Request(JSApiLeaderStepDown, nil, time.Second)
		if err != nil {
			return err
		}

		var resp JSApiLeaderStepDownResponse
		if err := json.Unmarshal(ncResp.Data, &resp); err != nil {
			return err
		}
		if resp.Error != nil {
			return resp.Error
		}
		if !resp.Success {
			return fmt.Errorf("leader step down request not successful")
		}

		return nil
	}

	// Force large cluster to be leader
	var largeLeader *Server
	err := checkForErr(15*time.Second, 500*time.Millisecond, func() error {
		// Range over cluster A, which is the large cluster.
		servers := sCluster.clusters[0].servers
		for _, s := range servers {
			if s.JetStreamIsLeader() {
				largeLeader = s
				return nil
			}
		}

		if err := requestLeaderStepDown(servers[0].ClientURL()); err != nil {
			return fmt.Errorf("failed to request leader step down: %s", err)
		}
		return fmt.Errorf("leader is not in large cluster")
	})
	if err != nil {
		t.Skipf("failed to get desired layout: %s", err)
	}

	ctx := context.Background()
	getStreams := func(jsm jetstream.JetStream) []string {
		var streams []string
		lister := jsm.StreamNames(ctx)
		for s := range lister.Name() {
			streams = append(streams, s)
		}
		return streams
	}
	nc, js := jsClientConnectNewAPI(t, largeLeader)
	defer nc.Close()

	cases := []struct {
		name           string
		storage        jetstream.StorageType
		createMaxBytes int64
		serverTag      string
		wantErr        bool
	}{
		{
			name:           "file create large stream on small cluster b0",
			storage:        jetstream.FileStorage,
			createMaxBytes: smallSystemLimit + 1,
			serverTag:      "C2-S1",
			wantErr:        true,
		},
		{
			name:           "memory create large stream on small cluster b0",
			storage:        jetstream.MemoryStorage,
			createMaxBytes: smallSystemLimit + 1,
			serverTag:      "C2-S1",
			wantErr:        true,
		},
		{
			name:           "file create large stream on small cluster b1",
			storage:        jetstream.FileStorage,
			createMaxBytes: smallSystemLimit + 1,
			serverTag:      "C2-S2",
			wantErr:        true,
		},
		{
			name:           "memory create large stream on small cluster b1",
			storage:        jetstream.MemoryStorage,
			createMaxBytes: smallSystemLimit + 1,
			serverTag:      "C2-S2",
			wantErr:        true,
		},
		{
			name:           "file create large stream on small cluster b2",
			storage:        jetstream.FileStorage,
			createMaxBytes: smallSystemLimit + 1,
			serverTag:      "C2-S3",
			wantErr:        true,
		},
		{
			name:           "memory create large stream on small cluster b2",
			storage:        jetstream.MemoryStorage,
			createMaxBytes: smallSystemLimit + 1,
			serverTag:      "C2-S3",
			wantErr:        true,
		},
		{
			name:           "file create large stream on large cluster a0",
			storage:        jetstream.FileStorage,
			createMaxBytes: smallSystemLimit + 1,
			serverTag:      "C1-S1",
		},
		{
			name:           "memory create large stream on large cluster a0",
			storage:        jetstream.MemoryStorage,
			createMaxBytes: smallSystemLimit + 1,
			serverTag:      "C1-S1",
		},
		{
			name:           "file create large stream on large cluster a1",
			storage:        jetstream.FileStorage,
			createMaxBytes: smallSystemLimit + 1,
			serverTag:      "C1-S2",
		},
		{
			name:           "memory create large stream on large cluster a1",
			storage:        jetstream.MemoryStorage,
			createMaxBytes: smallSystemLimit + 1,
			serverTag:      "C1-S2",
		},
		{
			name:           "file create large stream on large cluster a2",
			storage:        jetstream.FileStorage,
			createMaxBytes: smallSystemLimit + 1,
			serverTag:      "C1-S3",
		},
		{
			name:           "memory create large stream on large cluster a2",
			storage:        jetstream.MemoryStorage,
			createMaxBytes: smallSystemLimit + 1,
			serverTag:      "C1-S3",
		},
	}
	for i := 0; i < len(cases) && !t.Failed(); i++ {
		c := cases[i]
		t.Run(c.name, func(st *testing.T) {
			var clusterName string
			if strings.HasPrefix(c.serverTag, "a") {
				clusterName = "cluster-a"
			} else if strings.HasPrefix(c.serverTag, "b") {
				clusterName = "cluster-b"
			}

			if s := getStreams(js); len(s) != 0 {
				st.Fatalf("unexpected stream count, got=%d, want=0", len(s))
			}

			streamName := fmt.Sprintf("TEST-%s", c.serverTag)
			stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
				Name:     streamName,
				Subjects: []string{"foo"},
				Storage:  c.storage,
				MaxBytes: c.createMaxBytes,
				Placement: &jetstream.Placement{
					Cluster: clusterName,
					Tags:    []string{c.serverTag},
				},
			})
			if c.wantErr && err == nil {
				if s := getStreams(js); len(s) != 1 {
					st.Logf("unexpected stream count, got=%d, want=1, streams=%v", len(s), s)
				}

				cfg := stream.CachedInfo().Config
				st.Fatalf("unexpected success, maxBytes=%d, cluster=%s, tags=%v",
					cfg.MaxBytes, cfg.Placement.Cluster, cfg.Placement.Tags)
			} else if !c.wantErr && err != nil {
				if s := getStreams(js); len(s) != 0 {
					st.Logf("unexpected stream count, got=%d, want=0, streams=%v", len(s), s)
				}

				require_NoError(st, err)
			}

			if err == nil {
				if s := getStreams(js); len(s) != 1 {
					st.Fatalf("unexpected stream count, got=%d, want=1", len(s))
				}
			}
			// Delete regardless.
			js.DeleteStream(ctx, streamName)
		})
	}
}

func TestJetStreamSuperClusterMixedModeSwitchToInterestOnlyStaticConfig(t *testing.T) {
	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: { domain: ngs, max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		leaf: { listen: 127.0.0.1:-1 }
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
		accounts {
			ONE {
				users = [  { user: "one", pass: "pwd" } ]
				jetstream: enabled
			}
			TWO { users = [  { user: "two", pass: "pwd" } ] }
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
	`
	sc := createJetStreamSuperClusterWithTemplateAndModHook(t, tmpl, 5, 3,
		func(serverName, clusterName, storeDir, conf string) string {
			sname := serverName[strings.Index(serverName, "-")+1:]
			switch sname {
			case "S4", "S5":
				conf = strings.ReplaceAll(conf, "jetstream: { ", "#jetstream: { ")
			default:
				conf = strings.ReplaceAll(conf, "leaf: { ", "#leaf: { ")
			}
			return conf
		}, nil)
	defer sc.shutdown()

	// Connect our client to a non JS server
	c := sc.randomCluster()
	var s *Server
	for _, as := range c.servers {
		if !as.JetStreamEnabled() {
			s = as
			break
		}
	}
	if s == nil {
		t.Fatal("Did not find a non JS server!")
	}
	nc, js := jsClientConnectNewAPI(t, s, nats.UserInfo("one", "pwd"))
	defer nc.Close()
	ctx := context.Background()

	// Just create a stream and then make sure that all gateways have switched
	// to interest-only mode.
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "interest", Replicas: 3})
	require_NoError(t, err)
	si := stream.CachedInfo()

	sc.waitOnStreamLeader("ONE", "interest")

	check := func(accName string) {
		t.Helper()
		for _, c := range sc.clusters {
			for _, s := range c.servers {
				// Check only JS servers outbound GW connections
				if !s.JetStreamEnabled() {
					continue
				}
				opts := s.getOpts()
				for _, gw := range opts.Gateway.Gateways {
					if gw.Name == opts.Gateway.Name {
						continue
					}
					checkGWInterestOnlyMode(t, s, gw.Name, accName)
				}
			}
		}
	}
	// Starting v2.9.0, all accounts should be switched to interest-only mode
	check("ONE")
	check("TWO")

	var gwsa [16]*client
	gws := gwsa[:0]

	s = sc.serverByName(si.Cluster.Leader)
	// Get the GW outbound connections
	s.getOutboundGatewayConnections(&gws)
	for _, gwc := range gws {
		gwc.mu.Lock()
		gwc.nc.Close()
		gwc.mu.Unlock()
	}
	waitForOutboundGateways(t, s, 2, 5*time.Second)
	check("ONE")
	check("TWO")
}

func TestJetStreamSuperClusterMixedModeSwitchToInterestOnlyOperatorConfig(t *testing.T) {
	kp, _ := nkeys.FromSeed(oSeed)

	skp, _ := nkeys.CreateAccount()
	spub, _ := skp.PublicKey()
	nac := jwt.NewAccountClaims(spub)
	sjwt, err := nac.Encode(kp)
	require_NoError(t, err)

	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac = jwt.NewAccountClaims(apub)
	// Set some limits to enable JS.
	nac.Limits.JetStreamLimits.DiskStorage = 1024 * 1024
	nac.Limits.JetStreamLimits.Streams = 10
	ajwt, err := nac.Encode(kp)
	require_NoError(t, err)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, spub) {
			w.Write([]byte(sjwt))
		} else {
			w.Write([]byte(ajwt))
		}
	}))
	defer ts.Close()

	operator := fmt.Sprintf(`
		operator: %s
		resolver: URL("%s/ngs/v1/accounts/jwt/")
	`, ojwt, ts.URL)

	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: { domain: ngs, max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		leaf: { listen: 127.0.0.1:-1 }
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
	` + operator
	sc := createJetStreamSuperClusterWithTemplateAndModHook(t, tmpl, 5, 3,
		func(serverName, clusterName, storeDir, conf string) string {
			conf = strings.ReplaceAll(conf, "system_account: \"$SYS\"", fmt.Sprintf("system_account: \"%s\"", spub))
			sname := serverName[strings.Index(serverName, "-")+1:]
			switch sname {
			case "S4", "S5":
				conf = strings.ReplaceAll(conf, "jetstream: { ", "#jetstream: { ")
			default:
				conf = strings.ReplaceAll(conf, "leaf: { ", "#leaf: { ")
			}
			return conf
		}, nil)
	defer sc.shutdown()

	// Connect our client to a non JS server
	c := sc.randomCluster()
	var s *Server
	for _, as := range c.servers {
		if !as.JetStreamEnabled() {
			s = as
			break
		}
	}
	if s == nil {
		t.Fatal("Did not find a non JS server!")
	}
	nc, js := jsClientConnectNewAPI(t, s, createUserCreds(t, nil, akp))
	defer nc.Close()
	ctx := context.Background()

	// Prevent 'nats: JetStream not enabled for account' when creating the first stream.
	c.waitOnAccount(apub)

	// Just create a stream and then make sure that all gateways have switched
	// to interest-only mode.
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "interest", Replicas: 3})
	require_NoError(t, err)
	si := stream.CachedInfo()

	sc.waitOnStreamLeader(apub, "interest")

	check := func(s *Server) {
		opts := s.getOpts()
		for _, gw := range opts.Gateway.Gateways {
			if gw.Name == opts.Gateway.Name {
				continue
			}
			checkGWInterestOnlyModeOrNotPresent(t, s, gw.Name, apub, true)
		}
	}
	s = sc.serverByName(si.Cluster.Leader)
	check(s)

	// Let's cause a leadership change and verify that it still works.
	_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "interest"), nil, time.Second)
	require_NoError(t, err)
	sc.waitOnStreamLeader(apub, "interest")

	si, err = stream.Info(ctx)
	require_NoError(t, err)
	s = sc.serverByName(si.Cluster.Leader)
	check(s)

	var gwsa [16]*client
	gws := gwsa[:0]
	// Get the GW outbound connections
	s.getOutboundGatewayConnections(&gws)
	for _, gwc := range gws {
		gwc.mu.Lock()
		gwc.nc.Close()
		gwc.mu.Unlock()
	}
	waitForOutboundGateways(t, s, 2, 5*time.Second)
	check(s)
}

type captureGWRewriteLogger struct {
	DummyLogger
	ch chan string
}

func (l *captureGWRewriteLogger) Tracef(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, "$JS.SNAPSHOT.ACK.TEST") && strings.Contains(msg, gwReplyPrefix) {
		select {
		case l.ch <- msg:
		default:
		}
	}
}

func TestJetStreamSuperClusterGWReplyRewrite(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	nc, js := jsClientConnectNewAPI(t, sc.serverByName("C1-S1"))
	defer nc.Close()
	ctx := context.Background()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	sc.waitOnStreamLeader(globalAccountName, "TEST")

	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	nc2, _ := jsClientConnectNewAPI(t, sc.serverByName("C2-S2"))
	defer nc2.Close()

	s := sc.clusters[0].streamLeader(globalAccountName, "TEST")
	var gws []*client
	s.getOutboundGatewayConnections(&gws)
	for _, gw := range gws {
		gw.mu.Lock()
		gw.trace = true
		gw.mu.Unlock()
	}
	l := &captureGWRewriteLogger{ch: make(chan string, 1)}
	s.SetLogger(l, false, true)

	// Send a request through the gateway
	sreq := &JSApiStreamSnapshotRequest{
		DeliverSubject: nats.NewInbox(),
		ChunkSize:      512,
	}
	natsSub(t, nc2, sreq.DeliverSubject, func(m *nats.Msg) {
		m.Respond(nil)
	})
	natsFlush(t, nc2)
	req, _ := json.Marshal(sreq)
	rmsg, err := nc2.Request(fmt.Sprintf(JSApiStreamSnapshotT, "TEST"), req, time.Second)
	require_NoError(t, err)
	var resp JSApiStreamSnapshotResponse
	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	if resp.Error != nil {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}

	// Now we just want to make sure that the reply has the gateway prefix
	select {
	case <-l.ch:
	case <-time.After(10 * time.Second):
	}
}

func TestJetStreamSuperClusterGWOfflineSatus(t *testing.T) {
	orgEventsHBInterval := eventsHBInterval
	eventsHBInterval = 500 * time.Millisecond //time.Second
	defer func() { eventsHBInterval = orgEventsHBInterval }()

	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		gateway {
			name: "local"
			listen: 127.0.0.1:-1
		}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts {
			SYS {
				users [{user: sys, password: pwd}]
			}
			ONE {
				jetstream: enabled
				users [{user: one, password: pwd}]
			}
		}
		system_account=SYS
	`
	c := createJetStreamClusterWithTemplate(t, tmpl, "local", 3)
	defer c.shutdown()

	var gwURLs string
	for i, s := range c.servers {
		if i > 0 {
			gwURLs += ","
		}
		gwURLs += `"nats://` + s.GatewayAddr().String() + `"`
	}

	tmpl2 := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		gateway {
			name: "remote"
			listen: 127.0.0.1:-1
			__remote__
		}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts {
			SYS {
				users [{user: sys, password: pwd}]
			}
			ONE {
				jetstream: enabled
				users [{user: one, password: pwd}]
			}
		}
		system_account=SYS
	`
	c2 := createJetStreamClusterAndModHook(t, tmpl2, "remote", "R", 2, 16022, false,
		func(serverName, clusterName, storeDir, conf string) string {
			conf = strings.Replace(conf, "__remote__", fmt.Sprintf("gateways [ { name: 'local', urls: [%s] } ]", gwURLs), 1)
			return conf
		})
	defer c2.shutdown()

	for _, s := range c.servers {
		waitForOutboundGateways(t, s, 1, 2*time.Second)
	}
	for _, s := range c2.servers {
		waitForOutboundGateways(t, s, 1, 2*time.Second)
	}
	c.waitOnPeerCount(5)

	// Simulate going offline without sending shutdown protocol
	for _, s := range c2.servers {
		c := s.getOutboundGatewayConnection("local")
		c.setNoReconnect()
		c.mu.Lock()
		c.nc.Close()
		c.mu.Unlock()
	}
	c2.shutdown()

	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		var ok int
		for _, s := range c.servers {
			jsz, err := s.Jsz(nil)
			if err != nil {
				return err
			}
			for _, r := range jsz.Meta.Replicas {
				if r.Name == "RS-1" && r.Offline {
					ok++
				} else if r.Name == "RS-2" && r.Offline {
					ok++
				}
			}
		}
		if ok != 2 {
			return fmt.Errorf("RS-1 or RS-2 still marked as online")
		}
		return nil
	})
}

func TestJetStreamSuperClusterMovingR1Stream(t *testing.T) {
	// Make C2 have some latency.
	gwm := gwProxyMap{
		"C2": &gwProxy{
			rtt:  10 * time.Millisecond,
			up:   1 * 1024 * 1024 * 1024, // 1gbit
			down: 1 * 1024 * 1024 * 1024, // 1gbit
		},
	}
	sc := createJetStreamTaggedSuperClusterWithGWProxy(t, gwm)
	defer sc.shutdown()

	nc, js := jsClientConnectNewAPI(t, sc.clusterForName("C1").randomServer())
	defer nc.Close()
	ctx := context.Background()

	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "TEST",
	})
	require_NoError(t, err)

	toSend := 10_000
	for i := 0; i < toSend; i++ {
		_, err := js.PublishAsync("TEST", []byte("HELLO WORLD"))
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Have it move to GCP.
	_, err = js.UpdateStream(ctx, jetstream.StreamConfig{
		Name:      "TEST",
		Placement: &jetstream.Placement{Tags: []string{"cloud:gcp"}},
	})
	require_NoError(t, err)

	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		sc.waitOnStreamLeader(globalAccountName, "TEST")
		si, err := stream.Info(ctx)
		if err != nil {
			return err
		}
		if si.Cluster.Name != "C2" {
			return fmt.Errorf("Wrong cluster: %q", si.Cluster.Name)
		}
		if si.Cluster.Leader == _EMPTY_ {
			return fmt.Errorf("No leader yet")
		} else if !strings.HasPrefix(si.Cluster.Leader, "C2") {
			return fmt.Errorf("Wrong leader: %q", si.Cluster.Leader)
		}
		// Now we want to see that we shrink back to original.
		if len(si.Cluster.Replicas) != 0 {
			return fmt.Errorf("Expected 0 replicas, got %d", len(si.Cluster.Replicas))
		}
		if si.State.Msgs != uint64(toSend) {
			return fmt.Errorf("Only see %d msgs", si.State.Msgs)
		}
		return nil
	})
}

// https://github.com/nats-io/nats-server/issues/4396
func TestJetStreamSuperClusterR1StreamPeerRemove(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 1, 3)
	defer sc.shutdown()

	nc, js := jsClientConnectNewAPI(t, sc.serverByName("C1-S1"))
	defer nc.Close()
	ctx := context.Background()

	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	})
	require_NoError(t, err)

	si, err := stream.Info(ctx)
	require_NoError(t, err)

	// Call peer remove on the only peer the leader.
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), []byte(`{"peer":"`+si.Cluster.Leader+`"}`), time.Second)
	require_NoError(t, err)
	var rpr JSApiStreamRemovePeerResponse
	require_NoError(t, json.Unmarshal(resp.Data, &rpr))
	require_False(t, rpr.Success)
	require_True(t, rpr.Error.ErrCode == 10075)

	// Stream should still be in place and useable.
	_, err = stream.Info(ctx)
	require_NoError(t, err)
}

func TestJetStreamSuperClusterConsumerPauseAdvisories(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 3)
	defer sc.shutdown()

	nc, js := jsClientConnectNewAPI(t, sc.randomServer())
	defer nc.Close()
	ctx := context.Background()

	pauseReq := func(consumer string, deadline time.Time) time.Time {
		j, err := json.Marshal(JSApiConsumerPauseRequest{
			PauseUntil: deadline,
		})
		require_NoError(t, err)
		msg, err := nc.Request(fmt.Sprintf(JSApiConsumerPauseT, "TEST", consumer), j, time.Second)
		require_NoError(t, err)
		var res JSApiConsumerPauseResponse
		err = json.Unmarshal(msg.Data, &res)
		require_NoError(t, err)
		return res.PauseUntil
	}

	checkAdvisory := func(msg *nats.Msg, shouldBePaused bool, deadline time.Time) {
		t.Helper()
		var advisory JSConsumerPauseAdvisory
		require_NoError(t, json.Unmarshal(msg.Data, &advisory))
		require_Equal(t, advisory.Stream, "TEST")
		require_Equal(t, advisory.Consumer, "my_consumer")
		require_Equal(t, advisory.Paused, shouldBePaused)
		require_True(t, advisory.PauseUntil.Equal(deadline))
	}

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	ch := make(chan *nats.Msg, 10)
	_, err = nc.ChanSubscribe(JSAdvisoryConsumerPausePre+".TEST.my_consumer", ch)
	require_NoError(t, err)

	deadline := time.Now().Add(time.Second)
	jsTestPause_CreateOrUpdateConsumer(t, nc, ActionCreate, "TEST", ConsumerConfig{
		Name:       "my_consumer",
		PauseUntil: &deadline,
		Replicas:   3,
	})

	// First advisory should tell us that the consumer was paused
	// on creation.
	msg := require_ChanRead(t, ch, time.Second*2)
	checkAdvisory(msg, true, deadline)
	require_Len(t, len(ch), 0) // Should only receive one advisory.

	// The second one for the unpause.
	msg = require_ChanRead(t, ch, time.Second*2)
	checkAdvisory(msg, false, deadline)
	require_Len(t, len(ch), 0) // Should only receive one advisory.

	// Now we'll pause the consumer for a second using the API.
	deadline = time.Now().Add(time.Second)
	require_True(t, pauseReq("my_consumer", deadline).Equal(deadline))

	// Third advisory should tell us about the pause via the API.
	msg = require_ChanRead(t, ch, time.Second*2)
	checkAdvisory(msg, true, deadline)
	require_Len(t, len(ch), 0) // Should only receive one advisory.

	// Finally that should unpause.
	msg = require_ChanRead(t, ch, time.Second*2)
	checkAdvisory(msg, false, deadline)
	require_Len(t, len(ch), 0) // Should only receive one advisory.

	// Now we're going to set the deadline into the future so we can
	// see what happens when we kick leaders or restart.
	deadline = time.Now().Add(time.Hour)
	require_True(t, pauseReq("my_consumer", deadline).Equal(deadline))

	// Setting the deadline should have generated an advisory.
	msg = require_ChanRead(t, ch, time.Second*2)
	checkAdvisory(msg, true, deadline)
	require_Len(t, len(ch), 0) // Should only receive one advisory.
}

func TestJetStreamSuperClusterConsumerAckSubjectWithStreamImportProtocolError(t *testing.T) {
	template := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {domain: hub, max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		accounts {
			SYS {}
			one {
				jetstream: enabled
				users [{user: one, password: password}]
				exports [{stream: >}]
			}
			two {
				users [{user: two, password: password}]
				imports [{stream: {subject: >, account: one}}]
			}
		}
		system_account: SYS
		cluster {
			listen: 127.0.0.1:%d
			name: %s
			routes = ["nats://127.0.0.1:%d"]
		}
		gateway {
			name: %s
			listen: 127.0.0.1:%d
			gateways = [{name: %s, urls: ["nats://127.0.0.1:%d"]}]
		}
		leaf {listen: 127.0.0.1:-1}
	`
	storeDir1 := t.TempDir()
	conf1 := createConfFile(t, []byte(fmt.Sprintf(template,
		"S1", storeDir1, 23222, "A", 23222, "A", 11222, "B", 11223)))
	s1, _ := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	storeDir2 := t.TempDir()
	conf2 := createConfFile(t, []byte(fmt.Sprintf(template,
		"S2", storeDir2, 23223, "B", 23223, "B", 11223, "A", 11222)))
	s2, o2 := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	waitForInboundGateways(t, s1, 1, 2*time.Second)
	waitForInboundGateways(t, s2, 1, 2*time.Second)
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)

	meta := s2.getJetStream().getMetaGroup()
	require_NoError(t, meta.CampaignImmediately())
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if !s1.JetStreamIsLeader() && !s2.JetStreamIsLeader() {
			return fmt.Errorf("neither server is leader")
		}
		return nil
	})

	leafCfg := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes {
			remotes [ { url: "nats://one:password@127.0.0.1:%d" } ]
		}
	`, o2.LeafNode.Port)))
	leaf, _ := RunServerWithConfig(leafCfg)
	defer leaf.Shutdown()

	ncl := clientConnectToServer(t, leaf)
	defer ncl.Close()
	js, err := jetstream.NewWithDomain(ncl, "hub", jetstream.WithDefaultTimeout(10*time.Second))
	require_NoError(t, err)
	ctx := context.Background()

	checkLeafNodeConnected(t, leaf)
	cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err = js.CreateStream(cctx, jetstream.StreamConfig{Name: "TEST", Placement: &jetstream.Placement{Cluster: "A"}})
	cancel()
	require_NoError(t, err)

	for range 2 {
		_, err = js.Publish(ctx, "TEST", nil)
		require_NoError(t, err)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, "TEST", jetstream.ConsumerConfig{
		Durable:   "DURABLE",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	require_NoError(t, err)

	fetchOne := func() jetstream.Msg {
		batch, err := cons.Fetch(1, jetstream.FetchMaxWait(time.Second))
		require_NoError(t, err)
		var collected []jetstream.Msg
		for m := range batch.Messages() {
			collected = append(collected, m)
		}
		require_NoError(t, batch.Error())
		require_Len(t, len(collected), 1)
		return collected[0]
	}

	msg := fetchOne()
	require_Equal(t, msg.Subject(), "TEST")
	require_NoError(t, msg.DoubleAck(ctx))

	// Since the JetStream ACK subject can encode the publish subject into the reply subject,
	// if we subscribe on a stream import this would previously result in a protocol error.
	nc2 := natsConnect(t, fmt.Sprintf("nats://two:password@127.0.0.1:%d", o2.Port))
	defer nc2.Close()
	sub2, err := nc2.Subscribe(">", func(msg *nats.Msg) {})
	require_NoError(t, err)
	defer sub2.Drain()
	require_NoError(t, nc2.Flush())

	// Ensure we can still receive messages. Would previously timeout due to the protocol error.
	msg = fetchOne()
	require_Equal(t, msg.Subject(), "TEST")
	require_NoError(t, msg.DoubleAck(ctx))
}
