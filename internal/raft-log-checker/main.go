package main

import (
	"fmt"
	"os"
	"reflect"

	"github.com/nats-io/nats-server/v2/server"
)

func checkWALEntries(seq uint64, entry1, entry2 *server.WALEntry) error {
	if seq != entry1.PrevIndex+1 {
		return fmt.Errorf("Found seq / index mismatch")
	}
	if seq != entry2.PrevIndex+1 {
		return fmt.Errorf("Found seq / index mismatch")
	}

	if entry1.Term == entry2.Term &&
		!reflect.DeepEqual(entry1.Data, entry2.Data) {
		fmt.Errorf("Found entry mismatch")
	}

	return nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: raft-log-checker <RaftLogDir> <RaftLogDir>\n")
		os.Exit(1)
	}

	storeDir1 := os.Args[1]
	storeDir2 := os.Args[2]

	wal1, err := server.NewWALReader(storeDir1)
	if err != nil {
		fmt.Println("Failed to create a WALReader: ", err)
		return
	}

	wal2, err := server.NewWALReader(storeDir2)
	if err != nil {
		fmt.Println("Failed to create a WALReader: ", err)
		return
	}

	fmt.Println("Entries in:", storeDir1)
	var state1 server.StreamState
	wal1.FastState(&state1)
	fmt.Println("First Seq: ", state1.FirstSeq)
	fmt.Println("Last Seq: ", state1.LastSeq)

	fmt.Println("Entries in:", storeDir2)
	var state2 server.StreamState
	wal2.FastState(&state2)
	fmt.Println("First Seq: ", state2.FirstSeq)
	fmt.Println("Last Seq: ", state2.LastSeq)

	fmt.Println(state2.FirstSeq)
	fmt.Println(state2.LastSeq)

	fromSeq := max(wal1.State().FirstSeq, wal2.State().FirstSeq)
	toSeq := min(wal1.State().LastSeq, wal2.State().LastSeq)

	fmt.Printf("Checking entries from: %d to: %d\n", fromSeq, toSeq)

	for seq := fromSeq; seq <= toSeq; seq++ {
		entry1, err1 := server.LoadEntry(wal2, seq)
		if err1 != nil {
			fmt.Println("Failed to load entry: ", err1)
			os.Exit(0)
		}

		entry2, err2 := server.LoadEntry(wal2, seq)
		if err2 != nil {
			fmt.Println("Failed to load entry: ", err2)
			os.Exit(0)
		}

		if err := checkWALEntries(seq, entry1, entry2); err != nil {
			fmt.Println(err)
			fmt.Println("Entry 1 is: ", entry1)
			fmt.Println("Entry 2 is: ", entry2)
			os.Exit(0)
		}
	}
}
