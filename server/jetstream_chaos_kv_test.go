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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	chaosKvTestsClusterName = "KV_CHAOS_TEST"
	chaosKvTestsBucketName  = "KV_CHAOS_TEST_BUCKET"
	chaosKvTestsSubject     = "foo"
	chaosKvTestsDebug       = false
)

// Creates KV store (a.k.a. bucket).
func createBucketForKvChaosTest(t *testing.T, c *cluster, replicas int) {
	t.Helper()

	pubNc, pubJs := jsClientConnectCluster(t, c)
	defer pubNc.Close()

	config := nats.KeyValueConfig{
		Bucket:      chaosKvTestsBucketName,
		Replicas:    replicas,
		Description: "Test bucket",
	}

	kvs, err := pubJs.CreateKeyValue(&config)
	if err != nil {
		t.Fatalf("Error creating bucket: %v", err)
	}

	status, err := kvs.Status()
	if err != nil {
		t.Fatalf("Error retrieving bucket status: %v", err)
	}
	t.Logf("Bucket created: %s", status.Bucket())
}

// Single client performs a set of PUT on a single key.
// If PUT is successful, perform a GET on the same key.
// If GET is successful, ensure key revision and value match the most recent successful write.
func TestJetStreamChaosKvPutGet(t *testing.T) {

	const numOps = 100_000
	const clusterSize = 3
	const replicas = 3
	const key = "key"
	const staleReadsOk = true // Set to false to check for violations of 'read committed' consistency

	c := createJetStreamClusterExplicit(t, chaosKvTestsClusterName, clusterSize)
	defer c.shutdown()

	createBucketForKvChaosTest(t, c, replicas)

	chaos := createClusterChaosMonkeyController(
		t,
		c,
		&clusterBouncerChaosMonkey{
			minDowntime:    0 * time.Second,
			maxDowntime:    2 * time.Second,
			minDownServers: clusterSize, // Whole cluster outage
			maxDownServers: clusterSize,
			pause:          1 * time.Second,
		},
	)

	nc, js := jsClientConnectCluster(t, c)
	defer nc.Close()

	// Create KV bucket
	kv, err := js.KeyValue(chaosKvTestsBucketName)
	if err != nil {
		t.Fatalf("Failed to get KV store: %v", err)
	}

	// Initialize the only key
	firstRevision, err := kv.Create(key, []byte("INITIAL VALUE"))
	if err != nil {
		t.Fatalf("Failed to create key: %v", err)
	} else if firstRevision != 1 {
		t.Fatalf("Unexpected revision: %d", firstRevision)
	}

	// Start chaos
	chaos.start()
	defer chaos.stop()

	staleReadsCount := uint64(0)
	successCount := uint64(0)

	previousRevision := firstRevision

putGetLoop:
	for i := 1; i <= numOps; i++ {

		if i%1000 == 0 {
			t.Logf("Completed %d/%d PUT+GET operations", i, numOps)
		}

		// PUT a value
		putValue := fmt.Sprintf("value-%d", i)
		putRevision, err := kv.Put(key, []byte(putValue))
		if err != nil {
			t.Logf("PUT error: %v", err)
			continue putGetLoop
		}

		// Check revision is monotonically increasing
		if putRevision <= previousRevision {
			t.Fatalf("PUT produced revision %d which is not greater than the previous successful PUT revision: %d", putRevision, previousRevision)
		}

		previousRevision = putRevision

		// If PUT was successful, GET the same
		kve, err := kv.Get(key)
		if err == nats.ErrKeyNotFound {
			t.Fatalf("GET key not found, but key does exists (last PUT revision: %d)", putRevision)
		} else if err != nil {
			t.Logf("GET error: %v", err)
			continue putGetLoop
		}

		getValue := string(kve.Value())
		getRevision := kve.Revision()

		if putRevision > getRevision {
			// Stale read, violates 'read committed' consistency criteria
			if !staleReadsOk {
				t.Fatalf("PUT value %s (rev: %d) then read value %s (rev: %d)", putValue, putRevision, getValue, getRevision)
			} else {
				staleReadsCount += 1
			}
		} else if putRevision < getRevision {
			// Returned revision is higher than any ever written, this should never happen
			t.Fatalf("GET returned revision %d, but most recent expected revision is %d", getRevision, putRevision)
		} else if putValue != getValue {
			// Returned revision matches latest, but values do not, this should never happen
			t.Fatalf("GET returned revision %d with value %s, but value %s was just committed for that same revision", getRevision, getValue, putValue)
		} else {
			// Get returned the latest revision/value
			successCount += 1
			if chaosKvTestsDebug {
				t.Logf("PUT+GET %s=%s (rev: %d)", key, putValue, putRevision)
			}
		}
	}

	t.Logf("Completed %d PUT+GET cycles of which %d successful, %d GETs returned a stale value", numOps, successCount, staleReadsCount)
}

// A variant TestJetStreamChaosKvPutGet where PUT is retried until successful, and GET is retried until it returns the latest known key revision.
// This validates than a confirmed PUT value is never lost, and becomes eventually visible.
func TestJetStreamChaosKvPutGetWithRetries(t *testing.T) {

	const numOps = 10_000
	const maxRetries = 20
	const retryDelay = 100 * time.Millisecond
	const clusterSize = 3
	const replicas = 3
	const key = "key"

	c := createJetStreamClusterExplicit(t, chaosKvTestsClusterName, clusterSize)
	defer c.shutdown()

	createBucketForKvChaosTest(t, c, replicas)

	chaos := createClusterChaosMonkeyController(
		t,
		c,
		&clusterBouncerChaosMonkey{
			minDowntime:    0 * time.Second,
			maxDowntime:    2 * time.Second,
			minDownServers: clusterSize, // Whole cluster outage
			maxDownServers: clusterSize,
			pause:          1 * time.Second,
		},
	)

	nc, js := jsClientConnectCluster(t, c)
	defer nc.Close()

	kv, err := js.KeyValue(chaosKvTestsBucketName)
	if err != nil {
		t.Fatalf("Failed to get KV store: %v", err)
	}

	// Initialize key value
	firstRevision, err := kv.Create(key, []byte("INITIAL VALUE"))
	if err != nil {
		t.Fatalf("Failed to create key: %v", err)
	} else if firstRevision != 1 {
		t.Fatalf("Unexpected revision: %d", firstRevision)
	}

	// Start chaos
	chaos.start()
	defer chaos.stop()

	staleReadCount := 0
	previousRevision := firstRevision

putGetLoop:
	for i := 1; i <= numOps; i++ {

		if i%1000 == 0 {
			t.Logf("Completed %d/%d PUT+GET operations", i, numOps)
		}

		putValue := fmt.Sprintf("value-%d", i)
		putRevision := uint64(0)

		// Put new value for key, retry until successful or out of retries
	putRetryLoop:
		for r := 0; r <= maxRetries; r++ {
			var putErr error
			putRevision, putErr = kv.Put(key, []byte(putValue))
			if putErr == nil {
				break putRetryLoop
			} else if r == maxRetries {
				t.Fatalf("Failed to PUT (retried %d times): %v", maxRetries, putErr)
			} else {
				if chaosKvTestsDebug {
					t.Logf("PUT error: %v", putErr)
				}
				time.Sleep(retryDelay)
			}
		}

		// Ensure key version is monotonically increasing
		if putRevision <= previousRevision {
			t.Fatalf("Latest PUT created revision %d which is not greater than the previous revision: %d", putRevision, previousRevision)
		}
		previousRevision = putRevision

		// Read value for key, retry until successful, and validate corresponding version and value
	getRetryLoop:
		for r := 0; r <= maxRetries; r++ {
			var getErr error
			kve, getErr := kv.Get(key)
			if getErr != nil && r == maxRetries {
				t.Fatalf("Failed to GET (retried %d times): %v", maxRetries, getErr)
			} else if getErr != nil {
				if chaosKvTestsDebug {
					t.Logf("GET error: %v", getErr)
				}
				time.Sleep(retryDelay)
				continue getRetryLoop
			}

			// GET successful, check revision and value
			getValue := string(kve.Value())
			getRevision := kve.Revision()

			if putRevision == getRevision {
				if putValue != getValue {
					t.Fatalf("Unexpected value %s for revision %d, expected: %s", getValue, getRevision, putValue)
				}
				if chaosKvTestsDebug {
					t.Logf("PUT+GET %s=%s (rev: %d) (retry: %d)", key, putValue, putRevision, r)
				}
				continue putGetLoop
			} else if getRevision > putRevision {
				t.Fatalf("GET returned version that should not exist yet: %d, last created: %d", getRevision, putRevision)
			} else { // get revision < put revision
				staleReadCount += 1
				if chaosKvTestsDebug {
					t.Logf("GET got stale value: %v (rev: %d, latest: %d)", getValue, getRevision, putRevision)
				}
				time.Sleep(retryDelay)
				continue getRetryLoop
			}
		}
	}

	t.Logf("Client completed %d PUT+GET cycles, %d GET returned a stale value", numOps, staleReadCount)
}

// Multiple clients updating a finite set of keys with CAS semantics.
// TODO check that revision is never lower than last one seen
// TODO check that KeyNotFound is never returned, as keys are initialized beforehand
func TestJetStreamChaosKvCAS(t *testing.T) {
	const numOps = 10_000
	const maxRetries = 50
	const retryDelay = 300 * time.Millisecond
	const clusterSize = 3
	const replicas = 3
	const numKeys = 15
	const numClients = 5

	c := createJetStreamClusterExplicit(t, chaosKvTestsClusterName, clusterSize)
	defer c.shutdown()

	createBucketForKvChaosTest(t, c, replicas)

	chaos := createClusterChaosMonkeyController(
		t,
		c,
		&clusterBouncerChaosMonkey{
			minDowntime:    0 * time.Second,
			maxDowntime:    2 * time.Second,
			minDownServers: clusterSize, // Whole cluster outage
			maxDownServers: clusterSize,
			pause:          1 * time.Second,
		},
	)

	nc, js := jsClientConnectCluster(t, c)
	defer nc.Close()

	// Create bucket
	kv, err := js.KeyValue(chaosKvTestsBucketName)
	if err != nil {
		t.Fatalf("Failed to get KV store: %v", err)
	}

	// Create set of keys and initialize them with dummy value
	keys := make([]string, numKeys)
	for k := 0; k < numKeys; k++ {
		key := fmt.Sprintf("key-%d", k)
		keys[k] = key

		_, err := kv.Create(key, []byte("Initial value"))
		if err != nil {
			t.Fatalf("Failed to create key: %v", err)
		}
	}

	wgStart := sync.WaitGroup{}
	wgComplete := sync.WaitGroup{}

	// Client routine
	client := func(clientId int, kv nats.KeyValue) {
		defer wgComplete.Done()

		rng := rand.New(rand.NewSource(int64(clientId)))
		successfulUpdates := 0
		casRejectUpdates := 0
		otherUpdateErrors := 0

		// Map to track last known revision for each of the keys
		knownRevisions := map[string]uint64{}
		for _, key := range keys {
			knownRevisions[key] = 0
		}

		// Wait for all clients to reach this point before proceeding
		wgStart.Done()
		wgStart.Wait()

		for i := 1; i <= numOps; i++ {

			if i%1000 == 0 {
				t.Logf("Client %d completed %d/%d updates", clientId, i, numOps)
			}

			// Pick random key from the set
			key := keys[rng.Intn(numKeys)]

			// Prepare unique value to be written
			value := fmt.Sprintf("client: %d operation %d", clientId, i)

			// Try to update a key with CAS
			newRevision, updateErr := kv.Update(key, []byte(value), knownRevisions[key])
			if updateErr == nil {
				// Update successful
				knownRevisions[key] = newRevision
				successfulUpdates += 1
				if chaosKvTestsDebug {
					t.Logf("Client %d updated key %s, new revision: %d", clientId, key, newRevision)
				}
			} else if updateErr != nil && strings.Contains(fmt.Sprint(updateErr), "wrong last sequence") {
				// CAS rejected update, learn current revision for this key
				casRejectUpdates += 1

				for r := 0; r <= maxRetries; r++ {
					kve, getErr := kv.Get(key)
					if getErr == nil {
						currentRevision := kve.Revision()
						if currentRevision < knownRevisions[key] {
							// Revision number moved backward, this should never happen
							t.Fatalf("Current revision for key %s is %d, which is lower than the last known revision %d", key, currentRevision, knownRevisions[key])

						}

						knownRevisions[key] = currentRevision
						if chaosKvTestsDebug {
							t.Logf("Client %d learn key %s revision: %d", clientId, key, currentRevision)
						}
						break
					} else if r == maxRetries {
						t.Fatalf("Failed to GET (retried %d times): %v", maxRetries, getErr)
					} else {
						time.Sleep(retryDelay)
					}
				}
			} else {
				// Other update error
				otherUpdateErrors += 1
				if chaosKvTestsDebug {
					t.Logf("Client %d update error for key %s: %v", clientId, key, updateErr)
				}
				time.Sleep(retryDelay)
			}
		}
		t.Logf("Client %d done, %d kv updates, %d CAS rejected, %d other errors", clientId, successfulUpdates, casRejectUpdates, otherUpdateErrors)
	}

	// Launch all clients
	for i := 1; i <= numClients; i++ {
		cNc, cJs := jsClientConnectCluster(t, c)
		defer cNc.Close()

		cKv, err := cJs.KeyValue(chaosKvTestsBucketName)
		if err != nil {
			t.Fatalf("Failed to get KV store: %v", err)
		}

		wgStart.Add(1)
		wgComplete.Add(1)
		go client(i, cKv)
	}

	// Wait for clients to be connected and ready
	wgStart.Wait()

	// Start failures
	chaos.start()
	defer chaos.stop()

	// Wait for all clients to be done
	wgComplete.Wait()
}
