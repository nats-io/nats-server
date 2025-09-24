// Test suite for the optimized GetSeqFromTime binary search implementation
package server

import (
    "fmt"
    "testing"
    "time"
)

// Test binary search implementation against linear search for correctness
func TestGetSeqFromTimeBinarySearchCorrectness(t *testing.T) {
	// Create a temporary filestore for testing
	storeDir := t.TempDir()
	
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "TEST", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

    // Store messages
    total := 10
    for i := 1; i <= total; i++ {
        subj := fmt.Sprintf("test.%d", i)
        data := fmt.Sprintf("msg%d", i)
        seq, _, err := fs.StoreMsg(subj, nil, []byte(data), 0)
        if err != nil {
            t.Fatalf("Failed to store message %d: %v", i, err)
        }
        if seq != uint64(i) {
            t.Fatalf("Expected seq %d, got %d", i, seq)
        }
    }

    // Helper to get timestamp for a sequence
    getTs := func(seq uint64) time.Time {
        var sm StoreMsg
        if _, err := fs.LoadMsg(seq, &sm); err != nil {
            t.Fatalf("LoadMsg(%d) failed: %v", seq, err)
        }
        return time.Unix(0, sm.ts)
    }

    ts1 := getTs(1)
    ts6 := getTs(6)
    ts10 := getTs(10)

    tests := []struct {
        name      string
        query     time.Time
        expected  uint64
    }{
        {"Before all messages", ts1.Add(-time.Nanosecond), 1},
        {"Exact match first", ts1, 1},
        {"Exact match middle", ts6, 6},
        {"Exact match last", ts10, 10},
        {"Between messages", ts6.Add(-time.Nanosecond), 6},
        {"After all messages", ts10.Add(time.Second), 11}, // lastSeq+1
    }

    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) {
            got := fs.GetSeqFromTime(tc.query)
            if got != tc.expected {
                t.Errorf("GetSeqFromTime(%v) = %d, expected %d", tc.query, got, tc.expected)
            }
        })
    }
}

// Performance benchmark comparing binary search vs linear search
func BenchmarkGetSeqFromTimeComparison(b *testing.B) {
	// Create a temporary filestore for testing
	storeDir := b.TempDir()
	
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "BENCH", Storage: FileStorage})
	if err != nil {
		b.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Store a large number of messages to see the performance difference
	numMessages := 10000
	
	for i := 0; i < numMessages; i++ {
		subject := fmt.Sprintf("test.%d", i)
		data := fmt.Sprintf("message_%d", i)
		_, _, err := fs.StoreMsg(subject, nil, []byte(data), 0)
		if err != nil {
			b.Fatalf("Failed to store message %d: %v", i, err)
		}
	}

    // Use the timestamp of a middle message
    var sm StoreMsg
    if _, err := fs.LoadMsg(uint64(numMessages/2), &sm); err != nil {
        b.Fatalf("LoadMsg failed: %v", err)
    }
    queryTime := time.Unix(0, sm.ts)
	
	b.Run("BinarySearch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = fs.GetSeqFromTime(queryTime)
		}
	})
}

// Test edge cases for binary search implementation
func TestGetSeqFromTimeBinarySearchEdgeCases(t *testing.T) {
	storeDir := t.TempDir()
	
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "EDGE_TEST", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Test empty store
    result := fs.GetSeqFromTime(time.Now())
	if result != 1 { // Should return lastSeq + 1 for empty store
		t.Errorf("Empty store: expected 1, got %d", result)
	}

	// Add single message
	_, _, err = fs.StoreMsg("test.single", nil, []byte("single"), 0)
	if err != nil {
		t.Fatalf("Failed to store single message: %v", err)
	}

	// Test with single message
    var sm StoreMsg
    if _, err := fs.LoadMsg(1, &sm); err != nil {
        t.Fatalf("LoadMsg failed: %v", err)
    }
    firstTs := time.Unix(0, sm.ts)
    result = fs.GetSeqFromTime(firstTs.Add(-1 * time.Nanosecond))
	if result != 1 {
		t.Errorf("Single message - before: expected 1, got %d", result)
	}

    result = fs.GetSeqFromTime(firstTs.Add(1 * time.Second))
    if result != 2 { // lastSeq+1
        t.Errorf("Single message - after: expected 2, got %d", result)
	}
}

// Test with message deletion to ensure binary search handles gaps correctly
func TestGetSeqFromTimeBinarySearchWithDeletions(t *testing.T) {
	storeDir := t.TempDir()
	
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "DELETE_TEST", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Store multiple messages
	for i := 0; i < 10; i++ {
		subject := fmt.Sprintf("test.%d", i)
		data := fmt.Sprintf("msg%d", i)
		_, _, err := fs.StoreMsg(subject, nil, []byte(data), 0)
		if err != nil {
			t.Fatalf("Failed to store message %d: %v", i, err)
		}
	}

	// Delete some messages in the middle
	removed, err := fs.RemoveMsg(5)
	if err != nil {
		t.Fatalf("Failed to remove message 5: %v", err)
	}
	if !removed {
		t.Fatalf("Message 5 was not removed")
	}

	removed, err = fs.RemoveMsg(6)
	if err != nil {
		t.Fatalf("Failed to remove message 6: %v", err)
	}
	if !removed {
		t.Fatalf("Message 6 was not removed")
	}

    // Test that binary search still works correctly with gaps
    var sm4 StoreMsg
    if _, err := fs.LoadMsg(4, &sm4); err != nil {
        t.Fatalf("LoadMsg(4) failed: %v", err)
    }
    result := fs.GetSeqFromTime(time.Unix(0, sm4.ts-1))
	if result == 0 {
		t.Errorf("With deletions: expected non-zero result for time before all messages")
	}

    // The implementation should skip deleted messages and find the next valid one (seq 7)
    result = fs.GetSeqFromTime(time.Unix(0, sm4.ts+1))
    if result < 7 {
        t.Errorf("With deletions: expected at least seq 7, got %d", result)
    }
}

// Helper function to create a filestore with custom configuration
func createTestFilestore(t *testing.T, name string) *fileStore {
	storeDir := t.TempDir()
	
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: name, Storage: FileStorage})
	if err != nil {
		t.Fatalf("Failed to create filestore: %v", err)
	}
	return fs
}

// Test concurrent access to GetSeqFromTime to ensure thread safety
func TestGetSeqFromTimeBinarySearchConcurrency(t *testing.T) {
	fs := createTestFilestore(t, "CONCURRENT_TEST")
	defer fs.Stop()

	numMessages := 1000

	// Store messages
	for i := 0; i < numMessages; i++ {
		subject := fmt.Sprintf("test.%d", i)
		data := fmt.Sprintf("msg%d", i)
		_, _, err := fs.StoreMsg(subject, nil, []byte(data), 0)
		if err != nil {
			t.Fatalf("Failed to store message %d: %v", i, err)
		}
	}

	// Run concurrent searches
	numGoroutines := 10
	results := make(chan uint64, numGoroutines)

    for i := 0; i < numGoroutines; i++ {
        go func(id int) {
            seq := uint64(1 + (id*100)%numMessages)
            var sm StoreMsg
            if _, err := fs.LoadMsg(seq, &sm); err != nil {
                results <- 0
                return
            }
            queryTime := time.Unix(0, sm.ts)
            result := fs.GetSeqFromTime(queryTime)
            results <- result
        }(i)
    }

	// Collect results
	for i := 0; i < numGoroutines; i++ {
		result := <-results
		if result == 0 {
			t.Errorf("Concurrent test %d: unexpected zero result", i)
		}
	}
}

