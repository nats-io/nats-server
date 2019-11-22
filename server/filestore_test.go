// Copyright 2019 The NATS Authors
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
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/bits"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestFileStoreBasics(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	for i := 1; i <= 5; i++ {
		if seq, err := fs.StoreMsg(subj, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		} else if seq != uint64(i) {
			t.Fatalf("Expected sequence to be %d, got %d", i, seq)
		}
	}
	stats := fs.Stats()
	if stats.Msgs != 5 {
		t.Fatalf("Expected 5 msgs, got %d", stats.Msgs)
	}
	expectedSize := 5 * fileStoreMsgSize(subj, msg)
	if stats.Bytes != expectedSize {
		t.Fatalf("Expected %d bytes, got %d", expectedSize, stats.Bytes)
	}
	nsubj, nmsg, _, err := fs.LoadMsg(2)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if nsubj != subj {
		t.Fatalf("Subjects don't match, original %q vs %q", subj, nsubj)
	}
	if !bytes.Equal(nmsg, msg) {
		t.Fatalf("Msgs don't match, original %q vs %q", msg, nmsg)
	}
	_, _, _, err = fs.LoadMsg(3)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
}

func TestFileStoreBasicWriteMsgsAndRestore(t *testing.T) {
	storeDir := filepath.Join("", JetStreamStoreDir)
	fcfg := FileStoreConfig{StoreDir: storeDir}

	if _, err := newFileStore(fcfg, MsgSetConfig{Storage: MemoryStorage}); err == nil {
		t.Fatalf("Expected an error with wrong type")
	}
	if _, err := newFileStore(fcfg, MsgSetConfig{Storage: FileStorage}); err == nil {
		t.Fatalf("Expected an error with no name")
	}

	// Make the directories to succeed in setup.
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(fcfg, MsgSetConfig{Name: "dlc", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj := "foo"

	// Write 100 msgs
	toStore := uint64(100)

	for i := uint64(1); i <= toStore; i++ {
		msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
		if seq, err := fs.StoreMsg(subj, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		} else if seq != uint64(i) {
			t.Fatalf("Expected sequence to be %d, got %d", i, seq)
		}
	}
	stats := fs.Stats()
	if stats.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}
	msg22 := []byte(fmt.Sprintf("[%08d] Hello World!", 22))
	expectedSize := toStore * fileStoreMsgSize(subj, msg22)

	if stats.Bytes != expectedSize {
		t.Fatalf("Expected %d bytes, got %d", expectedSize, stats.Bytes)
	}
	// Stop will flush to disk.
	fs.Stop()

	fs, err = newFileStore(fcfg, MsgSetConfig{Name: "dlc", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	stats = fs.Stats()
	if stats.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}
	if stats.Bytes != expectedSize {
		t.Fatalf("Expected %d bytes, got %d", expectedSize, stats.Bytes)
	}
}

func TestFileStoreMsgLimit(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage, MaxMsgs: 10})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		fs.StoreMsg(subj, msg)
	}
	stats := fs.Stats()
	if stats.Msgs != 10 {
		t.Fatalf("Expected %d msgs, got %d", 10, stats.Msgs)
	}
	if _, err := fs.StoreMsg(subj, msg); err != nil {
		t.Fatalf("Error storing msg: %v", err)
	}
	stats = fs.Stats()
	if stats.Msgs != 10 {
		t.Fatalf("Expected %d msgs, got %d", 10, stats.Msgs)
	}
	if stats.LastSeq != 11 {
		t.Fatalf("Expected the last sequence to be 11 now, but got %d", stats.LastSeq)
	}
	if stats.FirstSeq != 2 {
		t.Fatalf("Expected the first sequence to be 2 now, but got %d", stats.FirstSeq)
	}
	// Make sure we can not lookup seq 1.
	if _, _, _, err := fs.LoadMsg(1); err == nil {
		t.Fatalf("Expected error looking up seq 1 but got none")
	}
}

func TestFileStoreBytesLimit(t *testing.T) {
	subj, msg := "foo", make([]byte, 512)
	storedMsgSize := fileStoreMsgSize(subj, msg)

	toStore := uint64(1024)
	maxBytes := storedMsgSize * toStore

	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage, MaxBytes: int64(maxBytes)})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	for i := uint64(0); i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	stats := fs.Stats()
	if stats.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}
	if stats.Bytes != storedMsgSize*toStore {
		t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, stats.Bytes)
	}

	// Now send 10 more and check that bytes limit enforced.
	for i := 0; i < 10; i++ {
		if _, err := fs.StoreMsg(subj, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		}
	}
	stats = fs.Stats()
	if stats.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}
	if stats.Bytes != storedMsgSize*toStore {
		t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, stats.Bytes)
	}
	if stats.FirstSeq != 11 {
		t.Fatalf("Expected first sequence to be 11, got %d", stats.FirstSeq)
	}
	if stats.LastSeq != toStore+10 {
		t.Fatalf("Expected last sequence to be %d, got %d", toStore+10, stats.LastSeq)
	}
}

func TestFileStoreAgeLimit(t *testing.T) {
	maxAge := 10 * time.Millisecond

	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage, MaxAge: maxAge})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Store some messages. Does not really matter how many.
	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	stats := fs.Stats()
	if stats.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}
	checkExpired := func(t *testing.T) {
		t.Helper()
		checkFor(t, time.Second, maxAge, func() error {
			stats = fs.Stats()
			if stats.Msgs != 0 {
				return fmt.Errorf("Expected no msgs, got %d", stats.Msgs)
			}
			if stats.Bytes != 0 {
				return fmt.Errorf("Expected no bytes, got %d", stats.Bytes)
			}
			return nil
		})
	}
	// Let them expire
	checkExpired(t)
	// Now add some more and make sure that timer will fire again.
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	stats = fs.Stats()
	if stats.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}
	checkExpired(t)
}

func TestFileStoreTimeStamps(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	last := time.Now().UnixNano()
	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		time.Sleep(5 * time.Millisecond)
		fs.StoreMsg(subj, msg)
	}
	for seq := uint64(1); seq <= 10; seq++ {
		_, _, ts, err := fs.LoadMsg(seq)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
		// These should be different
		if ts <= last {
			t.Fatalf("Expected different timestamps, got last %v vs %v", last, ts)
		}
		last = ts
	}
}

func TestFileStorePurge(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: 64 * 1024}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", make([]byte, 8*1024)
	storedMsgSize := fileStoreMsgSize(subj, msg)

	toStore := uint64(1024)
	for i := uint64(0); i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	stats := fs.Stats()
	if stats.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}
	if stats.Bytes != storedMsgSize*toStore {
		t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, stats.Bytes)
	}

	if numBlocks := fs.numMsgBlocks(); numBlocks <= 1 {
		t.Fatalf("Expected to have more then 1 msg block, got %d", numBlocks)
	}

	fs.Purge()

	if numBlocks := fs.numMsgBlocks(); numBlocks != 1 {
		t.Fatalf("Expected to have exactly 1 empty msg block, got %d", numBlocks)
	}

	checkPurgeStats := func() {
		t.Helper()
		stats = fs.Stats()
		if stats.Msgs != 0 {
			t.Fatalf("Expected 0 msgs after purge, got %d", stats.Msgs)
		}
		if stats.Bytes != 0 {
			t.Fatalf("Expected 0 bytes after purge, got %d", stats.Bytes)
		}
		if stats.LastSeq != toStore {
			t.Fatalf("Expected LastSeq to be %d., got %d", toStore, stats.LastSeq)
		}
		if stats.FirstSeq != toStore+1 {
			t.Fatalf("Expected FirstSeq to be %d., got %d", toStore+1, stats.FirstSeq)
		}
	}
	checkPurgeStats()

	// Make sure we recover same state.
	fs.Stop()

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: 64 * 1024}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	if numBlocks := fs.numMsgBlocks(); numBlocks != 1 {
		t.Fatalf("Expected to have exactly 1 empty msg block, got %d", numBlocks)
	}

	checkPurgeStats()
}

func TestFileStoreRemovePartialRecovery(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	stats := fs.Stats()
	if stats.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}

	// Remove half
	for i := 1; i <= toStore/2; i++ {
		fs.RemoveMsg(uint64(i))
	}

	stats = fs.Stats()
	if stats.Msgs != uint64(toStore/2) {
		t.Fatalf("Expected %d msgs, got %d", toStore/2, stats.Msgs)
	}

	// Make sure we recover same state.
	fs.Stop()

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	stats2 := fs.Stats()
	if stats != stats2 {
		t.Fatalf("Expected recovered stats to be the same, got %+v vs %+v\n", stats2, stats)
	}
}

func TestFileStoreRemoveOutOfOrderRecovery(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	stats := fs.Stats()
	if stats.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}

	// Remove evens
	for i := 2; i <= toStore; i += 2 {
		if !fs.RemoveMsg(uint64(i)) {
			t.Fatalf("Expected remove to return true")
		}
	}

	stats = fs.Stats()
	if stats.Msgs != uint64(toStore/2) {
		t.Fatalf("Expected %d msgs, got %d", toStore/2, stats.Msgs)
	}

	if _, _, _, err := fs.LoadMsg(1); err != nil {
		t.Fatalf("Expected to retrieve seq 1")
	}
	for i := 2; i <= toStore; i += 2 {
		if _, _, _, err := fs.LoadMsg(uint64(i)); err == nil {
			t.Fatalf("Expected error looking up seq %d that should be deleted", i)
		}
	}

	// Make sure we recover same state.
	fs.Stop()

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	stats2 := fs.Stats()
	if stats != stats2 {
		t.Fatalf("Expected receovered stats to be the same, got %+v vs %+v\n", stats, stats2)
	}

	if _, _, _, err := fs.LoadMsg(1); err != nil {
		t.Fatalf("Expected to retrieve seq 1")
	}
	for i := 2; i <= toStore; i += 2 {
		if _, _, _, err := fs.LoadMsg(uint64(i)); err == nil {
			t.Fatalf("Expected error looking up seq %d that should be deleted", i)
		}
	}
}

func TestFileStoreAgeLimitRecovery(t *testing.T) {
	maxAge := 10 * time.Millisecond

	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage, MaxAge: maxAge})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Store some messages. Does not really matter how many.
	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	stats := fs.Stats()
	if stats.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}
	fs.Stop()
	time.Sleep(2 * maxAge)

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage, MaxAge: maxAge})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	stats = fs.Stats()
	if stats.Msgs != 0 {
		t.Fatalf("Expected no msgs, got %d", stats.Msgs)
	}
	if stats.Bytes != 0 {
		t.Fatalf("Expected no bytes, got %d", stats.Bytes)
	}
}

func TestFileStoreBitRot(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Store some messages. Does not really matter how many.
	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	stats := fs.Stats()
	if stats.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}

	if badSeqs := len(fs.checkMsgs()); badSeqs > 0 {
		t.Fatalf("Expected to have no corrupt msgs, got %d", badSeqs)
	}

	// Now twiddle some bits.
	fs.mu.Lock()
	lmb := fs.lmb
	contents, _ := ioutil.ReadFile(lmb.mfn)
	var index int
	for {
		index = rand.Intn(len(contents))
		// Reverse one byte anywhere.
		b := contents[index]
		contents[index] = bits.Reverse8(b)
		if b != contents[index] {
			break
		}
	}
	ioutil.WriteFile(lmb.mfn, contents, 0644)
	fs.mu.Unlock()

	bseqs := fs.checkMsgs()
	if badSeqs := len(bseqs); badSeqs == 0 {
		t.Fatalf("Expected to have corrupt msgs got none: changed [%d]", index)
	}

	// Make sure we can restore.
	fs.Stop()

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	if !reflect.DeepEqual(bseqs, fs.checkMsgs()) {
		t.Fatalf("Different reporting on bad msgs: %+v vs %+v", bseqs, fs.checkMsgs())
	}
}

func TestFileStoreEraseMsg(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	fs.StoreMsg(subj, msg)
	_, smsg, _, err := fs.LoadMsg(1)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if !bytes.Equal(msg, smsg) {
		t.Fatalf("Expected same msg, got %q vs %q", smsg, msg)
	}
	sm, _ := fs.msgForSeq(1)
	if !fs.EraseMsg(1) {
		t.Fatalf("Expected erase msg to return success")
	}
	if bytes.Equal(msg, smsg) {
		t.Fatalf("Expected msg to be erased")
	}

	// Now look on disk as well.
	rl := fileStoreMsgSize(subj, msg)
	buf := make([]byte, rl)
	fp, err := os.Open(path.Join(storeDir, msgDir, fmt.Sprintf(blkScan, 1)))
	if err != nil {
		t.Fatalf("Error opening msgs file: %v", err)
	}
	defer fp.Close()
	fp.ReadAt(buf, sm.off)
	nsubj, nmsg, seq, ts, err := msgFromBuf(buf)
	if err != nil {
		t.Fatalf("error reading message from block: %v", err)
	}
	if nsubj == subj {
		t.Fatalf("Expected the subjects to be different")
	}
	if seq != 0 {
		t.Fatalf("Expected seq to be 0, marking as deleted, got %d", seq)
	}
	if ts != 0 {
		t.Fatalf("Expected timestamp to be 0, got %d", ts)
	}
	if bytes.Equal(nmsg, msg) {
		t.Fatalf("Expected message body to be randomized")
	}
}

func TestFileStoreEraseAndNoIndexRecovery(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	stats := fs.Stats()
	if stats.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}

	// Erase the even messages.
	for i := 2; i <= toStore; i += 2 {
		if !fs.EraseMsg(uint64(i)) {
			t.Fatalf("Expected erase msg to return true")
		}
	}
	stats = fs.Stats()
	if stats.Msgs != uint64(toStore/2) {
		t.Fatalf("Expected %d msgs, got %d", toStore/2, stats.Msgs)
	}

	// Stop and remove the index file.
	fs.Stop()
	ifn := path.Join(storeDir, msgDir, fmt.Sprintf(indexScan, 1))
	os.Remove(ifn)

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	stats = fs.Stats()
	if stats.Msgs != uint64(toStore/2) {
		t.Fatalf("Expected %d msgs, got %d", toStore/2, stats.Msgs)
	}

	for i := 2; i <= toStore; i += 2 {
		if _, _, _, err := fs.LoadMsg(uint64(i)); err == nil {
			t.Fatalf("Expected error looking up seq %d that should be erased", i)
		}
	}
}

func TestFileStoreMeta(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	mconfig := MsgSetConfig{Name: "ZZ-22-33", Storage: FileStorage, Subjects: []string{"foo.*"}, Replicas: 22}

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, mconfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	metafile := path.Join(storeDir, JetStreamMetaFile)
	metasum := path.Join(storeDir, JetStreamMetaFileSum)

	// Test to make sure meta file and checksum are present.
	if _, err := os.Stat(metafile); os.IsNotExist(err) {
		t.Fatalf("Expected metafile %q to exist", metafile)
	}
	if _, err := os.Stat(metasum); os.IsNotExist(err) {
		t.Fatalf("Expected metafile's checksum %q to exist", metasum)
	}

	buf, err := ioutil.ReadFile(metafile)
	if err != nil {
		t.Fatalf("Error reading metafile: %v", err)
	}
	var mconfig2 MsgSetConfig
	if err := json.Unmarshal(buf, &mconfig2); err != nil {
		t.Fatalf("Error unmarshalling: %v", err)
	}
	if !reflect.DeepEqual(mconfig, mconfig2) {
		t.Fatalf("MsgSet configs not equal, got %+v vs %+v", mconfig2, mconfig)
	}
	checksum, err := ioutil.ReadFile(metasum)
	if err != nil {
		t.Fatalf("Error reading metafile checksum: %v", err)
	}
	fs.hh.Reset()
	fs.hh.Write(buf)
	mychecksum := hex.EncodeToString(fs.hh.Sum(nil))
	if mychecksum != string(checksum) {
		t.Fatalf("Checksums do not match, got %q vs %q", mychecksum, checksum)
	}

	// Now create an observable. Same deal for them.
	oconfig := ObservableConfig{
		Delivery:   "d",
		DeliverAll: true,
		Subject:    "foo",
		AckPolicy:  AckAll,
	}
	oname := "obs22"
	obs, err := fs.ObservableStore(oname, &oconfig)
	if err != nil {
		t.Fatalf("Unexepected error: %v", err)
	}

	ometafile := path.Join(storeDir, obsDir, oname, JetStreamMetaFile)
	ometasum := path.Join(storeDir, obsDir, oname, JetStreamMetaFileSum)

	// Test to make sure meta file and checksum are present.
	if _, err := os.Stat(ometafile); os.IsNotExist(err) {
		t.Fatalf("Expected observable metafile %q to exist", ometafile)
	}
	if _, err := os.Stat(ometasum); os.IsNotExist(err) {
		t.Fatalf("Expected observable metafile's checksum %q to exist", ometasum)
	}

	buf, err = ioutil.ReadFile(ometafile)
	if err != nil {
		t.Fatalf("Error reading observable metafile: %v", err)
	}

	var oconfig2 ObservableConfig
	if err := json.Unmarshal(buf, &oconfig2); err != nil {
		t.Fatalf("Error unmarshalling: %v", err)
	}
	if oconfig2 != oconfig {
		//if !reflect.DeepEqual(oconfig, oconfig2) {
		t.Fatalf("Observable configs not equal, got %+v vs %+v", oconfig2, oconfig)
	}
	checksum, err = ioutil.ReadFile(ometasum)

	if err != nil {
		t.Fatalf("Error reading observable metafile checksum: %v", err)
	}

	hh := obs.(*observableFileStore).hh
	hh.Reset()
	hh.Write(buf)
	mychecksum = hex.EncodeToString(hh.Sum(nil))
	if mychecksum != string(checksum) {
		t.Fatalf("Checksums do not match, got %q vs %q", mychecksum, checksum)
	}
}

func TestFileStoreCollapseDmap(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	subj, msg := "foo", []byte("Hello World!")
	storedMsgSize := fileStoreMsgSize(subj, msg)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir, BlockSize: 4 * storedMsgSize},
		MsgSetConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 10; i++ {
		fs.StoreMsg(subj, msg)
	}
	stats := fs.Stats()
	if stats.Msgs != 10 {
		t.Fatalf("Expected 10 msgs, got %d", stats.Msgs)
	}

	checkDmapTotal := func(total int) {
		t.Helper()
		if nde := fs.dmapEntries(); nde != total {
			t.Fatalf("Expecting only %d entries, got %d", total, nde)
		}
	}

	checkFirstSeq := func(seq uint64) {
		t.Helper()
		stats := fs.Stats()
		if stats.FirstSeq != seq {
			t.Fatalf("Expected first seq to be %d, got %d", seq, stats.FirstSeq)
		}
	}

	// Now remove some out of order, forming gaps and entries in dmaps.
	fs.RemoveMsg(2)
	checkFirstSeq(1)
	fs.RemoveMsg(4)
	checkFirstSeq(1)
	fs.RemoveMsg(8)
	checkFirstSeq(1)

	stats = fs.Stats()
	if stats.Msgs != 7 {
		t.Fatalf("Expected 7 msgs, got %d", stats.Msgs)
	}

	checkDmapTotal(3)

	// Close gaps..
	fs.RemoveMsg(1)
	checkDmapTotal(2)
	checkFirstSeq(3)

	fs.RemoveMsg(3)
	checkDmapTotal(1)
	checkFirstSeq(5)

	fs.RemoveMsg(5)
	checkDmapTotal(1)
	checkFirstSeq(6)

	fs.RemoveMsg(7)
	checkDmapTotal(2)

	fs.RemoveMsg(6)
	checkDmapTotal(0)
}

func TestFileStoreReadCache(t *testing.T) {
	subj, msg := "foo.bar", make([]byte, 1024)
	storedMsgSize := fileStoreMsgSize(subj, msg)

	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, ReadCacheExpire: 50 * time.Millisecond}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	toStore := 500
	totalBytes := uint64(toStore) * storedMsgSize

	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}

	fs.LoadMsg(1)
	if csz := fs.cacheSize(); csz != totalBytes {
		t.Fatalf("Expected all messages to be cached, got %d vs %d", csz, totalBytes)
	}
	// Should expire and be removed.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if csz := fs.cacheSize(); csz != 0 {
			return fmt.Errorf("cache size not 0, got %s", FriendlyBytes(int64(csz)))
		}
		return nil
	})
	if cls := fs.cacheLoads(); cls != 1 {
		t.Fatalf("Expected only 1 cache load, got %d", cls)
	}
	// Now make sure we do not reload cache if there is activity.
	fs.LoadMsg(1)
	timeout := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(timeout) {
		if cls := fs.cacheLoads(); cls != 2 {
			t.Fatalf("cache loads not 2, got %d", cls)
		}
		time.Sleep(5 * time.Millisecond)
		fs.LoadMsg(1) // register activity.
	}
}

func TestFileStoreObservable(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	o, err := fs.ObservableStore("obs22", &ObservableConfig{})
	if err != nil {
		t.Fatalf("Unexepected error: %v", err)
	}
	if state, err := o.State(); state != nil || err != nil {
		t.Fatalf("Unexpected state or error: %v", err)
	}
	state := &ObservableState{}
	if err := o.Update(state); err == nil {
		t.Fatalf("Exepected an error and got none")
	}

	updateAndCheck := func() {
		t.Helper()
		if err := o.Update(state); err != nil {
			t.Fatalf("Unexepected error updating state: %v", err)
		}
		s2, err := o.State()
		if err != nil {
			t.Fatalf("Unexepected error getting state: %v", err)
		}
		if !reflect.DeepEqual(state, s2) {
			t.Fatalf("State is not the same: wanted %+v got %+v", state, s2)
		}
	}

	shouldFail := func() {
		t.Helper()
		if err := o.Update(state); err == nil {
			t.Fatalf("Expected an error and got none")
		}
	}

	state.Delivered.ObsSeq = 1
	state.Delivered.SetSeq = 22
	updateAndCheck()

	state.Delivered.ObsSeq = 100
	state.Delivered.SetSeq = 122
	state.AckFloor.ObsSeq = 50
	state.AckFloor.SetSeq = 123
	// This should fail, bad state.
	shouldFail()
	// So should this.
	state.AckFloor.ObsSeq = 200
	state.AckFloor.SetSeq = 100
	shouldFail()

	// Should succeed
	state.AckFloor.ObsSeq = 50
	state.AckFloor.SetSeq = 72
	updateAndCheck()

	tn := time.Now().UnixNano()

	// We should sanity check pending here as well, so will check if a pending value is below
	// ack floor or above delivered.
	state.Pending = map[uint64]int64{70: tn}
	shouldFail()
	state.Pending = map[uint64]int64{140: tn}
	shouldFail()
	state.Pending = map[uint64]int64{72: tn} // exact on floor should fail
	shouldFail()

	// Put timestamps a second apart.
	// We will downsample to second resolution to save space. So setup our times
	// to reflect that.
	ago := time.Now().Add(-30 * time.Second).Truncate(time.Second)
	nt := func() int64 {
		ago = ago.Add(time.Second)
		return ago.UnixNano()
	}
	// Should succeed.
	state.Pending = map[uint64]int64{75: nt(), 80: nt(), 83: nt(), 90: nt(), 111: nt()}
	updateAndCheck()

	// Now do redlivery, but first with no pending.
	state.Pending = nil
	state.Redelivery = map[uint64]uint64{22: 3, 44: 8}
	updateAndCheck()

	// All together.
	state.Pending = map[uint64]int64{75: nt(), 80: nt(), 83: nt(), 90: nt(), 111: nt()}
	updateAndCheck()

	// Large one
	state.Delivered.ObsSeq = 10000
	state.Delivered.SetSeq = 10000
	state.AckFloor.ObsSeq = 100
	state.AckFloor.SetSeq = 100
	// Generate 8k pending.
	state.Pending = make(map[uint64]int64)
	for len(state.Pending) < 8192 {
		seq := uint64(rand.Intn(9890) + 101)
		if _, ok := state.Pending[seq]; !ok {
			state.Pending[seq] = nt()
		}
	}
	updateAndCheck()

	state.Pending = nil
	state.AckFloor.ObsSeq = 10000
	state.AckFloor.SetSeq = 10000
	updateAndCheck()
}

func TestFileStorePerf(t *testing.T) {
	// Uncomment to run, holding place for now.
	t.SkipNow()

	subj, msg := "foo", make([]byte, 4*1024)
	for i := 0; i < len(msg); i++ {
		msg[i] = 'D'
	}
	storedMsgSize := fileStoreMsgSize(subj, msg)

	// 10GB
	toStore := 10 * 1024 * 1024 * 1024 / storedMsgSize

	fmt.Printf("storing %d msgs of %s each, totalling %s\n",
		toStore,
		FriendlyBytes(int64(storedMsgSize)),
		FriendlyBytes(int64(toStore*storedMsgSize)),
	)

	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)
	fmt.Printf("StoreDir is %q\n", storeDir)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		MsgSetConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	start := time.Now()
	for i := 0; i < int(toStore); i++ {
		fs.StoreMsg(subj, msg)
	}
	fs.Stop()

	tt := time.Since(start)
	fmt.Printf("time to store is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
	fmt.Printf("%s per sec\n", FriendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

	fmt.Printf("Filesystem cache flush, paused 5 seconds.\n\n")
	time.Sleep(5 * time.Second)

	fmt.Printf("reading %d msgs of %s each, totalling %s\n",
		toStore,
		FriendlyBytes(int64(storedMsgSize)),
		FriendlyBytes(int64(toStore*storedMsgSize)),
	)

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: storeDir, BlockSize: 128 * 1024 * 1024},
		MsgSetConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	start = time.Now()
	for i := uint64(1); i <= toStore; i++ {
		fs.LoadMsg(i)
	}
	fs.Stop()

	tt = time.Since(start)
	fmt.Printf("time to read all back messages is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
	fmt.Printf("%s per sec\n", FriendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: storeDir, BlockSize: 128 * 1024 * 1024},
		MsgSetConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fmt.Printf("\nremoving [in order] %d msgs of %s each, totalling %s\n",
		toStore,
		FriendlyBytes(int64(storedMsgSize)),
		FriendlyBytes(int64(toStore*storedMsgSize)),
	)

	start = time.Now()
	// For reverse order.
	//for i := toStore; i > 0; i-- {
	for i := uint64(1); i <= toStore; i++ {
		fs.RemoveMsg(i)
	}
	fs.Stop()

	tt = time.Since(start)
	fmt.Printf("time to remove all messages is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
	fmt.Printf("%s per sec\n", FriendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: storeDir, BlockSize: 128 * 1024 * 1024},
		MsgSetConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	stats := fs.Stats()
	if stats.Msgs != 0 {
		t.Fatalf("Expected no msgs, got %d", stats.Msgs)
	}
	if stats.Bytes != 0 {
		t.Fatalf("Expected no bytes, got %d", stats.Bytes)
	}
}

func TestFileStoreObservablesPerf(t *testing.T) {
	// Uncomment to run, holding place for now.
	t.SkipNow()

	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)
	fmt.Printf("StoreDir is %q\n", storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Test Observables.
	o, err := fs.ObservableStore("obs22", &ObservableConfig{})
	if err != nil {
		t.Fatalf("Unexepected error: %v", err)
	}
	state := &ObservableState{}
	toStore := uint64(1000000)
	fmt.Printf("observable of %d msgs for ACK NONE\n", toStore)

	start := time.Now()
	for i := uint64(1); i <= toStore; i++ {
		state.Delivered.ObsSeq = i
		state.Delivered.SetSeq = i
		state.AckFloor.ObsSeq = i
		state.AckFloor.SetSeq = i
		if err := o.Update(state); err != nil {
			t.Fatalf("Unexepected error updating state: %v", err)
		}
	}
	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f updates/sec\n", float64(toStore)/tt.Seconds())

	// We will lag behind with pending.
	state.Pending = make(map[uint64]int64)
	lag := uint64(100)
	state.AckFloor.ObsSeq = 0
	state.AckFloor.SetSeq = 0

	fmt.Printf("\nobservable of %d msgs for ACK EXPLICIT with pending lag of %d\n", toStore, lag)

	start = time.Now()
	for i := uint64(1); i <= toStore; i++ {
		state.Delivered.ObsSeq = i
		state.Delivered.SetSeq = i
		state.Pending[i] = time.Now().UnixNano()
		if i > lag {
			ackseq := i - lag
			state.AckFloor.ObsSeq = ackseq
			state.AckFloor.SetSeq = ackseq
			delete(state.Pending, ackseq)
		}
		if err := o.Update(state); err != nil {
			t.Fatalf("Unexepected error updating state: %v", err)
		}
	}
	tt = time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f updates/sec\n", float64(toStore)/tt.Seconds())
}
