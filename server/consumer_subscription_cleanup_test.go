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
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamClusterConsumerSubscriptionCleanupOnFollowers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	getSubCount := func(srv *Server, accName string) int {
		acc, err := srv.LookupAccount(accName)
		if err != nil {
			return -1
		}
		if acc == nil || acc.sl == nil {
			return -1
		}
		return int(acc.sl.Count())
	}

	accName := "$G"

	startCounts := make(map[string]int)
	for _, srv := range c.servers {
		startCounts[srv.Name()] = getSubCount(srv, accName)
	}

	iterations := 10
	for i := 0; i < iterations; i++ {
		consName := fmt.Sprintf("C%d", i)

		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
			Durable:   consName,
			AckPolicy: nats.AckExplicitPolicy,
			Replicas:  3,
		})
		require_NoError(t, err)

		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			for _, srv := range c.servers {
				mset, err := srv.GlobalAccount().lookupStream("TEST")
				if err != nil || mset == nil {
					return fmt.Errorf("stream not found on %s", srv.Name())
				}
				cons := mset.lookupConsumer(consName)
				if cons == nil {
					return fmt.Errorf("consumer not found on %s", srv.Name())
				}
			}
			return nil
		})

		err = js.DeleteConsumer("TEST", consName)
		require_NoError(t, err)

		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			for _, srv := range c.servers {
				mset, err := srv.GlobalAccount().lookupStream("TEST")
				if err != nil || mset == nil {
					continue
				}
				cons := mset.lookupConsumer(consName)
				if cons != nil {
					return fmt.Errorf("consumer still exists on %s", srv.Name())
				}
			}
			return nil
		})
	}

	checkFor(t, 3*time.Second, 100*time.Millisecond, func() error {
		for _, srv := range c.servers {
			endCount := getSubCount(srv, accName)
			startCount := startCounts[srv.Name()]
			if endCount != startCount {
				return fmt.Errorf("server %s: subscription count not stabilized: start=%d, current=%d", srv.Name(), startCount, endCount)
			}
		}
		return nil
	})

	endCounts := make(map[string]int)
	for _, srv := range c.servers {
		endCounts[srv.Name()] = getSubCount(srv, accName)
	}

	for name, endCount := range endCounts {
		startCount := startCounts[name]
		leaked := endCount - startCount
		t.Logf("Server %s: start=%d, end=%d, leaked=%d", name, startCount, endCount, leaked)
		if leaked != 0 {
			t.Errorf("Server %s leaked %d subscriptions (expected 0)", name, leaked)
		}
	}
}

func TestJetStreamClusterConsumerSubscriptionCleanupMultipleIterations(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.>"},
		Replicas: 3,
	})
	require_NoError(t, err)

	getStreamLeader := func() *Server {
		for _, srv := range c.servers {
			mset, err := srv.GlobalAccount().lookupStream("ORDERS")
			if err != nil || mset == nil {
				continue
			}
			if mset.IsLeader() {
				return srv
			}
		}
		return nil
	}

	getFollowers := func() []*Server {
		var followers []*Server
		leader := getStreamLeader()
		if leader == nil {
			return followers
		}

		for _, srv := range c.servers {
			if srv != leader {
				mset, err := srv.GlobalAccount().lookupStream("ORDERS")
				if err == nil && mset != nil {
					followers = append(followers, srv)
				}
			}
		}
		return followers
	}

	accName := "$G"
	iterations := 20

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		leader := getStreamLeader()
		if leader == nil {
			return fmt.Errorf("no stream leader found")
		}
		followers := getFollowers()
		if len(followers) < 2 {
			return fmt.Errorf("expected at least 2 followers, got %d", len(followers))
		}
		return nil
	})

	leader := getStreamLeader()
	followers := getFollowers()
	t.Logf("Leader: %s, Followers: %d", leader.Name(), len(followers))

	getSubCountForServer := func(srv *Server) int {
		acc, err := srv.LookupAccount(accName)
		if err != nil || acc == nil || acc.sl == nil {
			return -1
		}
		return int(acc.sl.Count())
	}

	leaderStartCount := getSubCountForServer(leader)
	followerStartCounts := make(map[string]int)
	for _, f := range followers {
		followerStartCounts[f.Name()] = getSubCountForServer(f)
	}

	for iter := 0; iter < iterations; iter++ {
		consName := fmt.Sprintf("CONSUMER_%d", iter)

		_, err = js.AddConsumer("ORDERS", &nats.ConsumerConfig{
			Durable:       consName,
			FilterSubject: "orders.>",
			AckPolicy:     nats.AckExplicitPolicy,
			Replicas:      3,
		})
		require_NoError(t, err)

		checkFor(t, 2*time.Second, 25*time.Millisecond, func() error {
			for _, srv := range c.servers {
				mset, err := srv.GlobalAccount().lookupStream("ORDERS")
				if err != nil || mset == nil {
					continue
				}
				cons := mset.lookupConsumer(consName)
				if cons == nil {
					return fmt.Errorf("consumer not replicated to %s yet", srv.Name())
				}
			}
			return nil
		})

		err = js.DeleteConsumer("ORDERS", consName)
		require_NoError(t, err)

		checkFor(t, 2*time.Second, 25*time.Millisecond, func() error {
			for _, srv := range c.servers {
				mset, err := srv.GlobalAccount().lookupStream("ORDERS")
				if err != nil || mset == nil {
					continue
				}
				cons := mset.lookupConsumer(consName)
				if cons != nil {
					return fmt.Errorf("consumer still exists on %s", srv.Name())
				}
			}
			return nil
		})
	}

	checkFor(t, 3*time.Second, 100*time.Millisecond, func() error {
		leaderEndCount := getSubCountForServer(leader)
		if leaderEndCount != leaderStartCount {
			return fmt.Errorf("leader subscription count not stabilized: start=%d, current=%d", leaderStartCount, leaderEndCount)
		}
		for _, f := range followers {
			endCount := getSubCountForServer(f)
			startCount := followerStartCounts[f.Name()]
			if endCount != startCount {
				return fmt.Errorf("follower %s subscription count not stabilized: start=%d, current=%d", f.Name(), startCount, endCount)
			}
		}
		return nil
	})

	leaderEndCount := getSubCountForServer(leader)
	leaderLeak := leaderEndCount - leaderStartCount
	t.Logf("Leader: start=%d, end=%d, leaked=%d", leaderStartCount, leaderEndCount, leaderLeak)

	if leaderLeak != 0 {
		t.Errorf("Leader leaked %d subscriptions (expected 0)", leaderLeak)
	}

	for _, f := range followers {
		endCount := getSubCountForServer(f)
		startCount := followerStartCounts[f.Name()]
		leaked := endCount - startCount
		t.Logf("Follower %s: start=%d, end=%d, leaked=%d", f.Name(), startCount, endCount, leaked)
		if leaked != 0 {
			t.Errorf("Follower %s leaked %d subscriptions (expected 0)", f.Name(), leaked)
		}
	}
}

func TestJetStreamClusterConsumerSubscriptionCleanupDuringLeadershipTransition(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3T", 3)
	defer c.shutdown()

	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TRANSITION",
		Subjects: []string{"trans.>"},
		Replicas: 3,
	})
	require_NoError(t, err)

	accName := "$G"
	getSubCount := func(srv *Server) int {
		acc, err := srv.LookupAccount(accName)
		if err != nil || acc == nil || acc.sl == nil {
			return -1
		}
		return int(acc.sl.Count())
	}

	getConsumerLeader := func(consName string) *Server {
		for _, srv := range c.servers {
			mset, err := srv.GlobalAccount().lookupStream("TRANSITION")
			if err != nil || mset == nil {
				continue
			}
			cons := mset.lookupConsumer(consName)
			if cons != nil && cons.isLeader() {
				return srv
			}
		}
		return nil
	}

	startCounts := make(map[string]int)
	for _, srv := range c.servers {
		startCounts[srv.Name()] = getSubCount(srv)
	}

	iterations := 10
	for iter := 0; iter < iterations; iter++ {
		consName := fmt.Sprintf("TRANS_%d", iter)

		_, err = js.AddConsumer("TRANSITION", &nats.ConsumerConfig{
			Durable:       consName,
			FilterSubject: "trans.>",
			AckPolicy:     nats.AckExplicitPolicy,
			Replicas:      3,
		})
		require_NoError(t, err)

		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			leader := getConsumerLeader(consName)
			if leader == nil {
				return fmt.Errorf("no consumer leader found")
			}
			return nil
		})

		leader := getConsumerLeader(consName)
		if leader != nil {
			mset, _ := leader.GlobalAccount().lookupStream("TRANSITION")
			if mset != nil {
				cons := mset.lookupConsumer(consName)
				if cons != nil {
					node := cons.raftNode()
					if node != nil {
						node.StepDown()
						checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
							newLeader := getConsumerLeader(consName)
							if newLeader == nil {
								return fmt.Errorf("waiting for new leader election")
							}
							if newLeader == leader {
								return fmt.Errorf("leader has not changed yet")
							}
							return nil
						})
					}
				}
			}
		}

		err = js.DeleteConsumer("TRANSITION", consName)
		require_NoError(t, err)

		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			for _, srv := range c.servers {
				mset, err := srv.GlobalAccount().lookupStream("TRANSITION")
				if err != nil || mset == nil {
					continue
				}
				cons := mset.lookupConsumer(consName)
				if cons != nil {
					return fmt.Errorf("consumer still exists on %s", srv.Name())
				}
			}
			return nil
		})
	}

	checkFor(t, 3*time.Second, 100*time.Millisecond, func() error {
		for _, srv := range c.servers {
			endCount := getSubCount(srv)
			startCount := startCounts[srv.Name()]
			if endCount != startCount {
				return fmt.Errorf("server %s: subscription count not stabilized: start=%d, current=%d", srv.Name(), startCount, endCount)
			}
		}
		return nil
	})

	for _, srv := range c.servers {
		endCount := getSubCount(srv)
		startCount := startCounts[srv.Name()]
		leaked := endCount - startCount
		t.Logf("Server %s: start=%d, end=%d, leaked=%d", srv.Name(), startCount, endCount, leaked)
		if leaked != 0 {
			t.Errorf("Server %s leaked %d subscriptions during leadership transitions (expected 0)", srv.Name(), leaked)
		}
	}
}

func TestJetStreamClusterConsumerSubscriptionCleanupRapidCreateDelete(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3R", 3)
	defer c.shutdown()

	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "RAPID",
		Subjects: []string{"rapid.>"},
		Replicas: 3,
	})
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		for _, srv := range c.servers {
			mset, err := srv.GlobalAccount().lookupStream("RAPID")
			if err != nil || mset == nil {
				return fmt.Errorf("stream not found on %s", srv.Name())
			}
		}
		return nil
	})

	accName := "$G"
	getSubCount := func(srv *Server) int {
		acc, err := srv.LookupAccount(accName)
		if err != nil || acc == nil || acc.sl == nil {
			return -1
		}
		return int(acc.sl.Count())
	}

	startCounts := make(map[string]int)
	for _, srv := range c.servers {
		startCounts[srv.Name()] = getSubCount(srv)
	}

	iterations := 50
	for iter := 0; iter < iterations; iter++ {
		consName := fmt.Sprintf("RAPID_%d", iter)

		_, err = js.AddConsumer("RAPID", &nats.ConsumerConfig{
			Durable:       consName,
			FilterSubject: "rapid.>",
			AckPolicy:     nats.AckExplicitPolicy,
			Replicas:      3,
		})
		if err != nil {
			t.Logf("Iteration %d: AddConsumer failed (may be expected under load): %v", iter, err)
			continue
		}

		err = js.DeleteConsumer("RAPID", consName)
		if err != nil {
			t.Logf("Iteration %d: DeleteConsumer failed: %v", iter, err)
		}
	}

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, srv := range c.servers {
			endCount := getSubCount(srv)
			startCount := startCounts[srv.Name()]
			if endCount != startCount {
				return fmt.Errorf("server %s: subscription count not stabilized: start=%d, current=%d", srv.Name(), startCount, endCount)
			}
		}
		return nil
	})

	for _, srv := range c.servers {
		endCount := getSubCount(srv)
		startCount := startCounts[srv.Name()]
		leaked := endCount - startCount
		t.Logf("Server %s: start=%d, end=%d, leaked=%d", srv.Name(), startCount, endCount, leaked)
		if leaked != 0 {
			t.Errorf("Server %s leaked %d subscriptions during rapid create/delete (expected 0)", srv.Name(), leaked)
		}
	}
}
