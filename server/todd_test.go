package server

import (
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"testing"
	"time"
)

func snapRGSet(pFlag bool, banner string, osi *nats.StreamInfo) *map[string]struct{} {
	var snapSet = make(map[string]struct{})
	if pFlag {
		fmt.Println(banner)
	}
	if osi == nil {
		if pFlag {
			fmt.Printf("bonkers!\n")
		}
		return nil
	}

	snapSet[osi.Cluster.Leader] = struct{}{}
	if pFlag {
		fmt.Printf("Leader: %s\n", osi.Cluster.Leader)
	}
	for _, replica := range osi.Cluster.Replicas {
		snapSet[replica.Name] = struct{}{}
		if pFlag {
			fmt.Printf("Replica: %s\n", replica.Name)
		}
	}

	return &snapSet
}

func TestJetStreamClusterAfterPeerRemoveZeroState(t *testing.T) {
	// R3 scenario (w/messages) in a 4-node cluster. Peer remove and check for a silent zero-state condition in the
	// new peer.

	// var nm uint64
	var err error

	sc := createJetStreamClusterExplicit(t, "cl4", 4)
	defer sc.shutdown()

	sc.waitOnClusterReadyWithNumPeers(4)

	s := sc.leader()
	nc, jsc := jsClientConnect(t, s)
	defer nc.Close()

	_, err = jsc.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sc.waitOnStreamLeader(globalAccountName, "foo")

	osi, err := jsc.StreamInfo("foo")
	require_NoError(t, err)

	// make sure 0 msgs
	require_True(t, osi.State.Msgs == 0)

	// Load up 10000
	toSend := 10000
	for i := 1; i <= toSend; i++ {
		msg := []byte(fmt.Sprintf("Hello World"))
		if _, err = jsc.Publish("foo.a", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	osi, err = jsc.StreamInfo("foo")
	require_NoError(t, err)

	// make sure 10000 msgs
	require_True(t, osi.State.Msgs == uint64(toSend))

	origSet := *snapRGSet(true, "== Orig RG Set ==", osi)

	// Remove a peer and select replacement 5 times to avoid false good
	for i := 0; i < 5; i++ {
		// Remove 1 peer replica (this will be random cloud region as initial placement was randomized ordering)
		// After each successful iteration, osi will reflect the current RG peers
		toRemove := osi.Cluster.Replicas[0].Name
		if i == 0 {
			fmt.Printf("Original replaced peer: %s\n", toRemove)
		}
		var newPeer string

		resp, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "foo"), []byte(`{"peer":"`+toRemove+`"}`), time.Second)
		require_NoError(t, err)
		var rpResp JSApiStreamRemovePeerResponse
		err = json.Unmarshal(resp.Data, &rpResp)
		require_NoError(t, err)
		require_True(t, rpResp.Success)

		sc.waitOnStreamLeader(globalAccountName, "foo")

		checkFor(t, time.Second, 200*time.Millisecond, func() error {
			osi, err = jsc.StreamInfo("foo")
			require_NoError(t, err)

			if len(osi.Cluster.Replicas) != 2 {
				return fmt.Errorf("expected R3, got R%d", len(osi.Cluster.Replicas)+1)
			}
			// STREAM.PEER.REMOVE is asynchronous command; make sure remove has occurred by
			// checking that the toRemove peer is gone.
			for _, replica := range osi.Cluster.Replicas {
				if replica.Name == toRemove {
					return fmt.Errorf("expected replaced replica, old replica still present")
				}
			}

			osi, err = jsc.StreamInfo("foo")
			require_NoError(t, err)

			// make sure all msgs reported in stream after non-leader peer replacement
			require_True(t, osi.State.Msgs == uint64(toSend))

			// identify the new peer
			newSet := *snapRGSet(false, "== Replaced ==", osi)
			for peer := range newSet {
				_, ok := origSet[peer]
				if !ok {
					newPeer = peer
					break
				}
			}
			require_True(t, newPeer != "")

			// Now we want to repeatedly do step-down until we get the new peer to be RG leader
			j := 1
			for {
				resp, err := nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "foo"), []byte(`{}`), time.Second)
				require_NoError(t, err)
				var rpResp JSApiStreamLeaderStepDownResponse
				err = json.Unmarshal(resp.Data, &rpResp)
				require_NoError(t, err)
				require_True(t, rpResp.Success)

				sc.waitOnStreamLeader(globalAccountName, "foo")

				osi, err = jsc.StreamInfo("foo")
				require_NoError(t, err)

				if osi.Cluster.Leader == newPeer {
					break
				}

				j++
				require_True(t, j < 10)
			}

			// Reset the original set for the next run
			origSet = *snapRGSet(true, fmt.Sprintf("== Iter %d: New peer as RG leader ==", i), osi)

			// Put a storage test here
			osi, err = jsc.StreamInfo("foo")
			require_NoError(t, err)

			require_True(t, osi.State.Msgs == 10000)

			return nil
		})
	}
}
