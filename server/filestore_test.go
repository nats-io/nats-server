// Copyright 2019-2024 The NATS Authors
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
	"archive/tar"
	"bytes"
	"crypto/hmac"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
)

func testFileStoreAllPermutations(t *testing.T, fn func(t *testing.T, fcfg FileStoreConfig)) {
	for _, fcfg := range []FileStoreConfig{
		{Cipher: NoCipher, Compression: NoCompression},
		{Cipher: NoCipher, Compression: S2Compression},
		{Cipher: AES, Compression: NoCompression},
		{Cipher: AES, Compression: S2Compression},
		{Cipher: ChaCha, Compression: NoCompression},
		{Cipher: ChaCha, Compression: S2Compression},
	} {
		subtestName := fmt.Sprintf("%s-%s", fcfg.Cipher, fcfg.Compression)
		t.Run(subtestName, func(t *testing.T) {
			fcfg.StoreDir = t.TempDir()
			fn(t, fcfg)
			time.Sleep(100 * time.Millisecond)
		})
	}
}

func prf(fcfg *FileStoreConfig) func(context []byte) ([]byte, error) {
	if fcfg.Cipher == NoCipher {
		return nil
	}
	return func(context []byte) ([]byte, error) {
		h := hmac.New(sha256.New, []byte("dlc22"))
		if _, err := h.Write(context); err != nil {
			return nil, err
		}
		return h.Sum(nil), nil
	}
}

func TestFileStoreBasics(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		now := time.Now().UnixNano()
		for i := 1; i <= 5; i++ {
			if seq, ts, err := fs.StoreMsg(subj, nil, msg); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			} else if seq != uint64(i) {
				t.Fatalf("Expected sequence to be %d, got %d", i, seq)
			} else if ts < now || ts > now+int64(time.Millisecond) {
				t.Fatalf("Expected timestamp to be current, got %v", ts-now)
			}
		}

		state := fs.State()
		if state.Msgs != 5 {
			t.Fatalf("Expected 5 msgs, got %d", state.Msgs)
		}
		expectedSize := 5 * fileStoreMsgSize(subj, nil, msg)
		if state.Bytes != expectedSize {
			t.Fatalf("Expected %d bytes, got %d", expectedSize, state.Bytes)
		}

		var smv StoreMsg
		sm, err := fs.LoadMsg(2, &smv)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
		if sm.subj != subj {
			t.Fatalf("Subjects don't match, original %q vs %q", subj, sm.subj)
		}
		if !bytes.Equal(sm.msg, msg) {
			t.Fatalf("Msgs don't match, original %q vs %q", msg, sm.msg)
		}
		_, err = fs.LoadMsg(3, nil)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}

		remove := func(seq, expectedMsgs uint64) {
			t.Helper()
			removed, err := fs.RemoveMsg(seq)
			if err != nil {
				t.Fatalf("Got an error on remove of %d: %v", seq, err)
			}
			if !removed {
				t.Fatalf("Expected remove to return true for %d", seq)
			}
			if state := fs.State(); state.Msgs != expectedMsgs {
				t.Fatalf("Expected %d msgs, got %d", expectedMsgs, state.Msgs)
			}
		}

		// Remove first
		remove(1, 4)
		// Remove last
		remove(5, 3)
		// Remove a middle
		remove(3, 2)
	})
}

func TestFileStoreMsgHeaders(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, hdr, msg := "foo", []byte("name:derek"), []byte("Hello World")
		elen := 22 + len(subj) + 4 + len(hdr) + len(msg) + 8
		if sz := int(fileStoreMsgSize(subj, hdr, msg)); sz != elen {
			t.Fatalf("Wrong size for stored msg with header")
		}
		fs.StoreMsg(subj, hdr, msg)
		var smv StoreMsg
		sm, err := fs.LoadMsg(1, &smv)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
		if !bytes.Equal(msg, sm.msg) {
			t.Fatalf("Expected same msg, got %q vs %q", sm.msg, msg)
		}
		if !bytes.Equal(hdr, sm.hdr) {
			t.Fatalf("Expected same hdr, got %q vs %q", sm.hdr, hdr)
		}
		if removed, _ := fs.EraseMsg(1); !removed {
			t.Fatalf("Expected erase msg to return success")
		}
	})
}

func TestFileStoreBasicWriteMsgsAndRestore(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		if _, err := newFileStore(fcfg, StreamConfig{Storage: MemoryStorage}); err == nil {
			t.Fatalf("Expected an error with wrong type")
		}
		if _, err := newFileStore(fcfg, StreamConfig{Storage: FileStorage}); err == nil {
			t.Fatalf("Expected an error with no name")
		}

		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj := "foo"

		// Write 100 msgs
		toStore := uint64(100)
		for i := uint64(1); i <= toStore; i++ {
			msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
			if seq, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			} else if seq != uint64(i) {
				t.Fatalf("Expected sequence to be %d, got %d", i, seq)
			}
		}
		state := fs.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		msg22 := []byte(fmt.Sprintf("[%08d] Hello World!", 22))
		expectedSize := toStore * fileStoreMsgSize(subj, nil, msg22)

		if state.Bytes != expectedSize {
			t.Fatalf("Expected %d bytes, got %d", expectedSize, state.Bytes)
		}
		// Stop will flush to disk.
		fs.Stop()

		// Make sure Store call after does not work.
		if _, _, err := fs.StoreMsg(subj, nil, []byte("no work")); err == nil {
			t.Fatalf("Expected an error for StoreMsg call after Stop, got none")
		}

		// Restart
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		if state.Bytes != expectedSize {
			t.Fatalf("Expected %d bytes, got %d", expectedSize, state.Bytes)
		}

		// Now write 100 more msgs
		for i := uint64(101); i <= toStore*2; i++ {
			msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
			if seq, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			} else if seq != uint64(i) {
				t.Fatalf("Expected sequence to be %d, got %d", i, seq)
			}
		}
		state = fs.State()
		if state.Msgs != toStore*2 {
			t.Fatalf("Expected %d msgs, got %d", toStore*2, state.Msgs)
		}

		// Now cycle again and make sure that last batch was stored.
		// Stop will flush to disk.
		fs.Stop()

		// Restart
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		if state.Msgs != toStore*2 {
			t.Fatalf("Expected %d msgs, got %d", toStore*2, state.Msgs)
		}
		if state.Bytes != expectedSize*2 {
			t.Fatalf("Expected %d bytes, got %d", expectedSize*2, state.Bytes)
		}

		fs.Purge()
		fs.Stop()

		// Restart
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		if state.Msgs != 0 {
			t.Fatalf("Expected %d msgs, got %d", 0, state.Msgs)
		}
		if state.Bytes != 0 {
			t.Fatalf("Expected %d bytes, got %d", 0, state.Bytes)
		}

		seq, _, err := fs.StoreMsg(subj, nil, []byte("Hello"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		fs.RemoveMsg(seq)

		fs.Stop()

		// Restart
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		require_Equal(t, state.FirstSeq, seq+1)
	})
}

func TestFileStoreSelectNextFirst(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 256
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		numMsgs := 10
		subj, msg := "zzz", []byte("Hello World")
		for i := 0; i < numMsgs; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		if state := fs.State(); state.Msgs != uint64(numMsgs) {
			t.Fatalf("Expected %d msgs, got %d", numMsgs, state.Msgs)
		}

		// Note the 256 block size is tied to the msg size below to give us 5 messages per block.
		if fmb := fs.selectMsgBlock(1); fmb.msgs != 5 {
			t.Fatalf("Expected 5 messages per block, but got %d", fmb.msgs)
		}

		// Delete 2-7, this will cross message blocks.
		for i := 2; i <= 7; i++ {
			fs.RemoveMsg(uint64(i))
		}

		if state := fs.State(); state.Msgs != 4 || state.FirstSeq != 1 {
			t.Fatalf("Expected 4 msgs, first seq of 11, got msgs of %d and first seq of %d", state.Msgs, state.FirstSeq)
		}
		// Now close the gap which will force the system to jump underlying message blocks to find the right sequence.
		fs.RemoveMsg(1)
		if state := fs.State(); state.Msgs != 3 || state.FirstSeq != 8 {
			t.Fatalf("Expected 3 msgs, first seq of 8, got msgs of %d and first seq of %d", state.Msgs, state.FirstSeq)
		}
	})
}

func TestFileStoreSkipMsg(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 256
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		numSkips := 10
		for i := 0; i < numSkips; i++ {
			fs.SkipMsg()
		}
		state := fs.State()
		if state.Msgs != 0 {
			t.Fatalf("Expected %d msgs, got %d", 0, state.Msgs)
		}
		if state.FirstSeq != uint64(numSkips+1) || state.LastSeq != uint64(numSkips) {
			t.Fatalf("Expected first to be %d and last to be %d. got first %d and last %d", numSkips+1, numSkips, state.FirstSeq, state.LastSeq)
		}

		fs.StoreMsg("zzz", nil, []byte("Hello World!"))
		fs.SkipMsg()
		fs.SkipMsg()
		fs.StoreMsg("zzz", nil, []byte("Hello World!"))
		fs.SkipMsg()

		state = fs.State()
		if state.Msgs != 2 {
			t.Fatalf("Expected %d msgs, got %d", 2, state.Msgs)
		}
		if state.FirstSeq != uint64(numSkips+1) || state.LastSeq != uint64(numSkips+5) {
			t.Fatalf("Expected first to be %d and last to be %d. got first %d and last %d", numSkips+1, numSkips+5, state.FirstSeq, state.LastSeq)
		}

		// Make sure we recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		if state.Msgs != 2 {
			t.Fatalf("Expected %d msgs, got %d", 2, state.Msgs)
		}
		if state.FirstSeq != uint64(numSkips+1) || state.LastSeq != uint64(numSkips+5) {
			t.Fatalf("Expected first to be %d and last to be %d. got first %d and last %d", numSkips+1, numSkips+5, state.FirstSeq, state.LastSeq)
		}

		var smv StoreMsg
		sm, err := fs.LoadMsg(11, &smv)
		if err != nil {
			t.Fatalf("Unexpected error looking up seq 11: %v", err)
		}
		if sm.subj != "zzz" || string(sm.msg) != "Hello World!" {
			t.Fatalf("Message did not match")
		}

		fs.SkipMsg()
		nseq, _, err := fs.StoreMsg("AAA", nil, []byte("Skip?"))
		if err != nil {
			t.Fatalf("Unexpected error looking up seq 11: %v", err)
		}
		if nseq != 17 {
			t.Fatalf("Expected seq of %d but got %d", 17, nseq)
		}

		// Make sure we recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		sm, err = fs.LoadMsg(nseq, &smv)
		if err != nil {
			t.Fatalf("Unexpected error looking up seq %d: %v", nseq, err)
		}
		if sm.subj != "AAA" || string(sm.msg) != "Skip?" {
			t.Fatalf("Message did not match")
		}
	})
}

func TestFileStoreWriteExpireWrite(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cexp := 10 * time.Millisecond
		fcfg.CacheExpire = cexp
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		toSend := 10
		for i := 0; i < toSend; i++ {
			fs.StoreMsg("zzz", nil, []byte("Hello World!"))
		}

		// Wait for write cache portion to go to zero.
		checkFor(t, time.Second, 20*time.Millisecond, func() error {
			if csz := fs.cacheSize(); csz != 0 {
				return fmt.Errorf("cache size not 0, got %s", friendlyBytes(int64(csz)))
			}
			return nil
		})

		for i := 0; i < toSend; i++ {
			fs.StoreMsg("zzz", nil, []byte("Hello World! - 22"))
		}

		if state := fs.State(); state.Msgs != uint64(toSend*2) {
			t.Fatalf("Expected %d msgs, got %d", toSend*2, state.Msgs)
		}

		// Make sure we recover same state.
		fs.Stop()

		fcfg.CacheExpire = 0
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); state.Msgs != uint64(toSend*2) {
			t.Fatalf("Expected %d msgs, got %d", toSend*2, state.Msgs)
		}

		// Now load them in and check.
		var smv StoreMsg
		for i := 1; i <= toSend*2; i++ {
			sm, err := fs.LoadMsg(uint64(i), &smv)
			if err != nil {
				t.Fatalf("Unexpected error looking up seq %d: %v", i, err)
			}
			str := "Hello World!"
			if i > toSend {
				str = "Hello World! - 22"
			}
			if sm.subj != "zzz" || string(sm.msg) != str {
				t.Fatalf("Message did not match")
			}
		}
	})
}

func TestFileStoreMsgLimit(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxMsgs: 10}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		state := fs.State()
		if state.Msgs != 10 {
			t.Fatalf("Expected %d msgs, got %d", 10, state.Msgs)
		}
		if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		}
		state = fs.State()
		if state.Msgs != 10 {
			t.Fatalf("Expected %d msgs, got %d", 10, state.Msgs)
		}
		if state.LastSeq != 11 {
			t.Fatalf("Expected the last sequence to be 11 now, but got %d", state.LastSeq)
		}
		if state.FirstSeq != 2 {
			t.Fatalf("Expected the first sequence to be 2 now, but got %d", state.FirstSeq)
		}
		// Make sure we can not lookup seq 1.
		if _, err := fs.LoadMsg(1, nil); err == nil {
			t.Fatalf("Expected error looking up seq 1 but got none")
		}
	})
}

func TestFileStoreMsgLimitBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxMsgs: 1}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		fs.StoreMsg(subj, nil, msg)
		fs.StoreMsg(subj, nil, msg)
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxMsgs: 1}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()
		fs.StoreMsg(subj, nil, msg)
	})
}

func TestFileStoreBytesLimit(t *testing.T) {
	subj, msg := "foo", make([]byte, 512)
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	toStore := uint64(1024)
	maxBytes := storedMsgSize * toStore

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxBytes: int64(maxBytes)}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := uint64(0); i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		state := fs.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		if state.Bytes != storedMsgSize*toStore {
			t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
		}

		// Now send 10 more and check that bytes limit enforced.
		for i := 0; i < 10; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			}
		}
		state = fs.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		if state.Bytes != storedMsgSize*toStore {
			t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
		}
		if state.FirstSeq != 11 {
			t.Fatalf("Expected first sequence to be 11, got %d", state.FirstSeq)
		}
		if state.LastSeq != toStore+10 {
			t.Fatalf("Expected last sequence to be %d, got %d", toStore+10, state.LastSeq)
		}
	})
}

// https://github.com/nats-io/nats-server/issues/4771
func TestFileStoreBytesLimitWithDiscardNew(t *testing.T) {
	subj, msg := "tiny", make([]byte, 7)
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	toStore := uint64(2)
	maxBytes := 100

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Storage: FileStorage, MaxBytes: int64(maxBytes), Discard: DiscardNew}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 10; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg)
			if i < int(toStore) {
				if err != nil {
					t.Fatalf("Error storing msg: %v", err)
				}
			} else if !errors.Is(err, ErrMaxBytes) {
				t.Fatalf("Storing msg should result in: %v", ErrMaxBytes)
			}
		}
		state := fs.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		if state.Bytes != storedMsgSize*toStore {
			t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
		}
	})
}

func TestFileStoreAgeLimit(t *testing.T) {
	maxAge := 250 * time.Millisecond

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		if fcfg.Compression != NoCompression {
			// TODO(nat): This test fails at the moment with compression enabled
			// because it takes longer to compress the blocks, by which time the
			// messages have expired. Need to think about a balanced age so that
			// the test doesn't take too long in non-compressed cases.
			t.SkipNow()
		}

		fcfg.BlockSize = 256
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxAge: maxAge}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store some messages. Does not really matter how many.
		subj, msg := "foo", []byte("Hello World")
		toStore := 500
		for i := 0; i < toStore; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
		state := fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		checkExpired := func(t *testing.T) {
			t.Helper()
			checkFor(t, 5*time.Second, maxAge, func() error {
				state = fs.State()
				if state.Msgs != 0 {
					return fmt.Errorf("Expected no msgs, got %d", state.Msgs)
				}
				if state.Bytes != 0 {
					return fmt.Errorf("Expected no bytes, got %d", state.Bytes)
				}
				return nil
			})
		}
		// Let them expire
		checkExpired(t)

		// Now add some more and make sure that timer will fire again.
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		state = fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		fs.RemoveMsg(502)
		fs.RemoveMsg(602)
		fs.RemoveMsg(702)
		fs.RemoveMsg(802)
		// We will measure the time to make sure expires works with interior deletes.
		start := time.Now()
		checkExpired(t)
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("Took too long to expire: %v", elapsed)
		}
	})
}

func TestFileStoreTimeStamps(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		last := time.Now().UnixNano()
		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			time.Sleep(5 * time.Millisecond)
			fs.StoreMsg(subj, nil, msg)
		}
		var smv StoreMsg
		for seq := uint64(1); seq <= 10; seq++ {
			sm, err := fs.LoadMsg(seq, &smv)
			if err != nil {
				t.Fatalf("Unexpected error looking up msg [%d]: %v", seq, err)
			}
			// These should be different
			if sm.ts <= last {
				t.Fatalf("Expected different timestamps, got last %v vs %v", last, sm.ts)
			}
			last = sm.ts
		}
	})
}

func TestFileStorePurge(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		blkSize := uint64(64 * 1024)
		fcfg.BlockSize = blkSize
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", make([]byte, 8*1024)
		storedMsgSize := fileStoreMsgSize(subj, nil, msg)

		toStore := uint64(1024)
		for i := uint64(0); i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		state := fs.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		if state.Bytes != storedMsgSize*toStore {
			t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
		}

		expectedBlocks := int(storedMsgSize * toStore / blkSize)
		if numBlocks := fs.numMsgBlocks(); numBlocks <= expectedBlocks {
			t.Fatalf("Expected to have more then %d msg blocks, got %d", blkSize, numBlocks)
		}

		fs.Purge()

		if numBlocks := fs.numMsgBlocks(); numBlocks != 1 {
			t.Fatalf("Expected to have exactly 1 empty msg block, got %d", numBlocks)
		}

		checkPurgeState := func(stored uint64) {
			t.Helper()
			state = fs.State()
			if state.Msgs != 0 {
				t.Fatalf("Expected 0 msgs after purge, got %d", state.Msgs)
			}
			if state.Bytes != 0 {
				t.Fatalf("Expected 0 bytes after purge, got %d", state.Bytes)
			}
			if state.LastSeq != stored {
				t.Fatalf("Expected LastSeq to be %d., got %d", toStore, state.LastSeq)
			}
			if state.FirstSeq != stored+1 {
				t.Fatalf("Expected FirstSeq to be %d., got %d", toStore+1, state.FirstSeq)
			}
		}
		checkPurgeState(toStore)

		// Make sure we recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if numBlocks := fs.numMsgBlocks(); numBlocks != 1 {
			t.Fatalf("Expected to have exactly 1 empty msg block, got %d", numBlocks)
		}

		checkPurgeState(toStore)

		// Now make sure we clean up any dangling purged messages.
		for i := uint64(0); i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		state = fs.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		if state.Bytes != storedMsgSize*toStore {
			t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
		}

		// We will simulate crashing before the purge directory is cleared.
		mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
		pdir := filepath.Join(fs.fcfg.StoreDir, "ptest")
		os.Rename(mdir, pdir)
		os.MkdirAll(mdir, 0755)

		fs.Purge()
		checkPurgeState(toStore * 2)

		// Make sure we recover same state.
		fs.Stop()

		purgeDir := filepath.Join(fs.fcfg.StoreDir, purgeDir)
		os.Rename(pdir, purgeDir)

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if numBlocks := fs.numMsgBlocks(); numBlocks != 1 {
			t.Fatalf("Expected to have exactly 1 empty msg block, got %d", numBlocks)
		}

		checkPurgeState(toStore * 2)

		checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
			if _, err := os.Stat(purgeDir); err == nil {
				return fmt.Errorf("purge directory still present")
			}
			return nil
		})
	})
}

func TestFileStoreCompact(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 350
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		if state := fs.State(); state.Msgs != 10 {
			t.Fatalf("Expected 10 msgs, got %d", state.Msgs)
		}
		n, err := fs.Compact(6)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if n != 5 {
			t.Fatalf("Expected to have purged 5 msgs, got %d", n)
		}
		state := fs.State()
		if state.Msgs != 5 {
			t.Fatalf("Expected 5 msgs, got %d", state.Msgs)
		}
		if state.FirstSeq != 6 {
			t.Fatalf("Expected first seq of 6, got %d", state.FirstSeq)
		}
		// Now test that compact will also reset first if seq > last
		n, err = fs.Compact(100)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if n != 5 {
			t.Fatalf("Expected to have purged 5 msgs, got %d", n)
		}
		if state = fs.State(); state.FirstSeq != 100 {
			t.Fatalf("Expected first seq of 100, got %d", state.FirstSeq)
		}

		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state = fs.State(); state.FirstSeq != 100 {
			t.Fatalf("Expected first seq of 100, got %d", state.FirstSeq)
		}
	})
}

func TestFileStoreCompactLastPlusOne(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8192
		fcfg.AsyncFlush = true

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", make([]byte, 10_000)
		for i := 0; i < 10_000; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		// The performance of this test is quite terrible with compression
		// if we have AsyncFlush = false, so we'll batch flushes instead.
		fs.mu.Lock()
		fs.checkAndFlushAllBlocks()
		fs.mu.Unlock()

		if state := fs.State(); state.Msgs != 10_000 {
			t.Fatalf("Expected 1000000 msgs, got %d", state.Msgs)
		}
		if _, err := fs.Compact(10_001); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		state := fs.State()
		if state.Msgs != 0 {
			t.Fatalf("Expected no message but got %d", state.Msgs)
		}

		fs.StoreMsg(subj, nil, msg)
		state = fs.State()
		if state.Msgs != 1 {
			t.Fatalf("Expected one message but got %d", state.Msgs)
		}
	})
}

func TestFileStoreCompactMsgCountBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		if state := fs.State(); state.Msgs != 10 {
			t.Fatalf("Expected 10 msgs, got %d", state.Msgs)
		}
		// Now delete 2,3,4.
		fs.EraseMsg(2)
		fs.EraseMsg(3)
		fs.EraseMsg(4)

		// Also delete 7,8, and 9.
		fs.RemoveMsg(7)
		fs.RemoveMsg(8)
		fs.RemoveMsg(9)

		n, err := fs.Compact(6)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// 1 & 5
		if n != 2 {
			t.Fatalf("Expected to have deleted 2 msgs, got %d", n)
		}
		if state := fs.State(); state.Msgs != 2 {
			t.Fatalf("Expected to have 2 remaining, got %d", state.Msgs)
		}
	})
}

func TestFileStoreCompactPerf(t *testing.T) {
	t.SkipNow()

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8192
		fcfg.AsyncFlush = true

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 100_000; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		if state := fs.State(); state.Msgs != 100_000 {
			t.Fatalf("Expected 1000000 msgs, got %d", state.Msgs)
		}
		start := time.Now()
		n, err := fs.Compact(90_001)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		t.Logf("Took %v to compact\n", time.Since(start))

		if n != 90_000 {
			t.Fatalf("Expected to have purged 90_000 msgs, got %d", n)
		}
		state := fs.State()
		if state.Msgs != 10_000 {
			t.Fatalf("Expected 10_000 msgs, got %d", state.Msgs)
		}
		if state.FirstSeq != 90_001 {
			t.Fatalf("Expected first seq of 90_001, got %d", state.FirstSeq)
		}
	})
}

func TestFileStoreStreamTruncate(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 350
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		tseq := uint64(50)

		subj, toStore := "foo", uint64(100)
		for i := uint64(1); i < tseq; i++ {
			_, _, err := fs.StoreMsg(subj, nil, []byte("ok"))
			require_NoError(t, err)
		}
		subj = "bar"
		for i := tseq; i <= toStore; i++ {
			_, _, err := fs.StoreMsg(subj, nil, []byte("ok"))
			require_NoError(t, err)
		}

		if state := fs.State(); state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}

		// Check that sequence has to be interior.
		if err := fs.Truncate(toStore + 1); err != ErrInvalidSequence {
			t.Fatalf("Expected err of '%v', got '%v'", ErrInvalidSequence, err)
		}

		if err := fs.Truncate(tseq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if state := fs.State(); state.Msgs != tseq {
			t.Fatalf("Expected %d msgs, got %d", tseq, state.Msgs)
		}

		// Now make sure we report properly if we have some deleted interior messages.
		fs.RemoveMsg(10)
		fs.RemoveMsg(20)
		fs.RemoveMsg(30)
		fs.RemoveMsg(40)

		tseq = uint64(25)
		if err := fs.Truncate(tseq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		state := fs.State()
		if state.Msgs != tseq-2 {
			t.Fatalf("Expected %d msgs, got %d", tseq-2, state.Msgs)
		}
		expected := []uint64{10, 20}
		if !reflect.DeepEqual(state.Deleted, expected) {
			t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
		}

		before := state

		// Make sure we can recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v", before, state)
		}

		mb := fs.getFirstBlock()
		require_True(t, mb != nil)
		require_NoError(t, mb.loadMsgs())
	})
}

func TestFileStoreRemovePartialRecovery(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		state := fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}

		// Remove half
		for i := 1; i <= toStore/2; i++ {
			fs.RemoveMsg(uint64(i))
		}

		state = fs.State()
		if state.Msgs != uint64(toStore/2) {
			t.Fatalf("Expected %d msgs, got %d", toStore/2, state.Msgs)
		}

		// Make sure we recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state2 := fs.State()
		if !reflect.DeepEqual(state2, state) {
			t.Fatalf("Expected recovered state to be the same, got %+v vs %+v\n", state2, state)
		}
	})
}

func TestFileStoreRemoveOutOfOrderRecovery(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		state := fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}

		// Remove evens
		for i := 2; i <= toStore; i += 2 {
			if removed, _ := fs.RemoveMsg(uint64(i)); !removed {
				t.Fatalf("Expected remove to return true")
			}
		}

		state = fs.State()
		if state.Msgs != uint64(toStore/2) {
			t.Fatalf("Expected %d msgs, got %d", toStore/2, state.Msgs)
		}

		var smv StoreMsg
		if _, err := fs.LoadMsg(1, &smv); err != nil {
			t.Fatalf("Expected to retrieve seq 1")
		}
		for i := 2; i <= toStore; i += 2 {
			if _, err := fs.LoadMsg(uint64(i), &smv); err == nil {
				t.Fatalf("Expected error looking up seq %d that should be deleted", i)
			}
		}

		// Make sure we recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state2 := fs.State()
		if !reflect.DeepEqual(state2, state) {
			t.Fatalf("Expected recovered states to be the same, got %+v vs %+v\n", state, state2)
		}

		if _, err := fs.LoadMsg(1, &smv); err != nil {
			t.Fatalf("Expected to retrieve seq 1")
		}
		for i := 2; i <= toStore; i += 2 {
			if _, err := fs.LoadMsg(uint64(i), nil); err == nil {
				t.Fatalf("Expected error looking up seq %d that should be deleted", i)
			}
		}
	})
}

func TestFileStoreAgeLimitRecovery(t *testing.T) {
	maxAge := 10 * time.Millisecond

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.CacheExpire = 1 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage, MaxAge: maxAge}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store some messages. Does not really matter how many.
		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		state := fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		fs.Stop()

		fcfg.CacheExpire = 0
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure they expire.
		checkFor(t, time.Second, 2*maxAge, func() error {
			t.Helper()
			state = fs.State()
			if state.Msgs != 0 {
				return fmt.Errorf("Expected no msgs, got %d", state.Msgs)
			}
			if state.Bytes != 0 {
				return fmt.Errorf("Expected no bytes, got %d", state.Bytes)
			}
			return nil
		})
	})
}

func TestFileStoreBitRot(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store some messages. Does not really matter how many.
		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		state := fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}

		if ld := fs.checkMsgs(); ld != nil && len(ld.Msgs) > 0 {
			t.Fatalf("Expected to have no corrupt msgs, got %d", len(ld.Msgs))
		}

		for i := 0; i < 10; i++ {
			// Now twiddle some bits.
			fs.mu.Lock()
			lmb := fs.lmb
			contents, err := os.ReadFile(lmb.mfn)
			require_NoError(t, err)
			require_True(t, len(contents) > 0)

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
			os.WriteFile(lmb.mfn, contents, 0644)
			fs.mu.Unlock()

			ld := fs.checkMsgs()
			if len(ld.Msgs) > 0 {
				break
			}
			// If our bitrot caused us to not be able to recover any messages we can break as well.
			if state := fs.State(); state.Msgs == 0 {
				break
			}
			// Fail the test if we have tried the 10 times and still did not
			// get any corruption report.
			if i == 9 {
				t.Fatalf("Expected to have corrupt msgs got none: changed [%d]", index)
			}
		}

		// Make sure we can restore.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// checkMsgs will repair the underlying store, so checkMsgs should be clean now.
		if ld := fs.checkMsgs(); ld != nil {
			// If we have no msgs left this will report the head msgs as lost again.
			if state := fs.State(); state.Msgs > 0 {
				t.Fatalf("Expected no errors restoring checked and fixed filestore, got %+v", ld)
			}
		}
	})
}

func TestFileStoreEraseMsg(t *testing.T) {
	// Just do no encryption, etc.
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	fs.StoreMsg(subj, nil, msg)
	fs.StoreMsg(subj, nil, msg) // To keep block from being deleted.
	var smv StoreMsg
	sm, err := fs.LoadMsg(1, &smv)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if !bytes.Equal(msg, sm.msg) {
		t.Fatalf("Expected same msg, got %q vs %q", sm.msg, msg)
	}
	if removed, _ := fs.EraseMsg(1); !removed {
		t.Fatalf("Expected erase msg to return success")
	}
	if sm2, _ := fs.msgForSeq(1, nil); sm2 != nil {
		t.Fatalf("Expected msg to be erased")
	}
	fs.checkAndFlushAllBlocks()

	// Now look on disk as well.
	rl := fileStoreMsgSize(subj, nil, msg)
	buf := make([]byte, rl)
	fp, err := os.Open(filepath.Join(fcfg.StoreDir, msgDir, fmt.Sprintf(blkScan, 1)))
	if err != nil {
		t.Fatalf("Error opening msg block file: %v", err)
	}
	defer fp.Close()

	fp.ReadAt(buf, 0)
	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()
	mb.mu.Lock()
	sm, err = mb.msgFromBuf(buf, nil, nil)
	mb.mu.Unlock()
	if err != nil {
		t.Fatalf("error reading message from block: %v", err)
	}
	if sm.subj == subj {
		t.Fatalf("Expected the subjects to be different")
	}
	if sm.seq != 0 && sm.seq&ebit == 0 {
		t.Fatalf("Expected seq to be 0, marking as deleted, got %d", sm.seq)
	}
	if sm.ts != 0 {
		t.Fatalf("Expected timestamp to be 0, got %d", sm.ts)
	}
	if bytes.Equal(sm.msg, msg) {
		t.Fatalf("Expected message body to be randomized")
	}
}

func TestFileStoreEraseAndNoIndexRecovery(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		state := fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}

		// Erase the even messages.
		for i := 2; i <= toStore; i += 2 {
			if removed, _ := fs.EraseMsg(uint64(i)); !removed {
				t.Fatalf("Expected erase msg to return true")
			}
		}
		state = fs.State()
		if state.Msgs != uint64(toStore/2) {
			t.Fatalf("Expected %d msgs, got %d", toStore/2, state.Msgs)
		}

		// Stop and remove the optional index file.
		fs.Stop()
		ifn := filepath.Join(fcfg.StoreDir, msgDir, fmt.Sprintf(indexScan, 1))
		os.Remove(ifn)

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		if state.Msgs != uint64(toStore/2) {
			t.Fatalf("Expected %d msgs, got %d", toStore/2, state.Msgs)
		}

		for i := 2; i <= toStore; i += 2 {
			if _, err := fs.LoadMsg(uint64(i), nil); err == nil {
				t.Fatalf("Expected error looking up seq %d that should be erased", i)
			}
		}
	})
}

func TestFileStoreMeta(t *testing.T) {
	// Just do no encryption, etc.
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	mconfig := StreamConfig{Name: "ZZ-22-33", Storage: FileStorage, Subjects: []string{"foo.*"}, Replicas: 22}
	fs, err := newFileStore(fcfg, mconfig)
	require_NoError(t, err)
	defer fs.Stop()

	metafile := filepath.Join(fcfg.StoreDir, JetStreamMetaFile)
	metasum := filepath.Join(fcfg.StoreDir, JetStreamMetaFileSum)

	// Test to make sure meta file and checksum are present.
	if _, err := os.Stat(metafile); os.IsNotExist(err) {
		t.Fatalf("Expected metafile %q to exist", metafile)
	}
	if _, err := os.Stat(metasum); os.IsNotExist(err) {
		t.Fatalf("Expected metafile's checksum %q to exist", metasum)
	}

	buf, err := os.ReadFile(metafile)
	if err != nil {
		t.Fatalf("Error reading metafile: %v", err)
	}
	var mconfig2 StreamConfig
	if err := json.Unmarshal(buf, &mconfig2); err != nil {
		t.Fatalf("Error unmarshalling: %v", err)
	}
	if !reflect.DeepEqual(mconfig, mconfig2) {
		t.Fatalf("Stream configs not equal, got %+v vs %+v", mconfig2, mconfig)
	}
	checksum, err := os.ReadFile(metasum)
	if err != nil {
		t.Fatalf("Error reading metafile checksum: %v", err)
	}

	fs.mu.Lock()
	fs.hh.Reset()
	fs.hh.Write(buf)
	mychecksum := hex.EncodeToString(fs.hh.Sum(nil))
	fs.mu.Unlock()

	if mychecksum != string(checksum) {
		t.Fatalf("Checksums do not match, got %q vs %q", mychecksum, checksum)
	}

	// Now create a consumer. Same deal for them.
	oconfig := ConsumerConfig{
		DeliverSubject: "d",
		FilterSubject:  "foo",
		AckPolicy:      AckAll,
	}
	oname := "obs22"
	obs, err := fs.ConsumerStore(oname, &oconfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ometafile := filepath.Join(fcfg.StoreDir, consumerDir, oname, JetStreamMetaFile)
	ometasum := filepath.Join(fcfg.StoreDir, consumerDir, oname, JetStreamMetaFileSum)

	// Test to make sure meta file and checksum are present.
	if _, err := os.Stat(ometafile); os.IsNotExist(err) {
		t.Fatalf("Expected consumer metafile %q to exist", ometafile)
	}
	if _, err := os.Stat(ometasum); os.IsNotExist(err) {
		t.Fatalf("Expected consumer metafile's checksum %q to exist", ometasum)
	}

	buf, err = os.ReadFile(ometafile)
	if err != nil {
		t.Fatalf("Error reading consumer metafile: %v", err)
	}

	var oconfig2 ConsumerConfig
	if err := json.Unmarshal(buf, &oconfig2); err != nil {
		t.Fatalf("Error unmarshalling: %v", err)
	}
	// Since we set name we will get that back now.
	oconfig.Name = oname
	if !reflect.DeepEqual(oconfig2, oconfig) {
		t.Fatalf("Consumer configs not equal, got %+v vs %+v", oconfig2, oconfig)
	}
	checksum, err = os.ReadFile(ometasum)

	if err != nil {
		t.Fatalf("Error reading consumer metafile checksum: %v", err)
	}

	hh := obs.(*consumerFileStore).hh
	hh.Reset()
	hh.Write(buf)
	mychecksum = hex.EncodeToString(hh.Sum(nil))
	if mychecksum != string(checksum) {
		t.Fatalf("Checksums do not match, got %q vs %q", mychecksum, checksum)
	}
}

func TestFileStoreWriteAndReadSameBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World!")

		for i := uint64(1); i <= 10; i++ {
			fs.StoreMsg(subj, nil, msg)
			if _, err := fs.LoadMsg(i, nil); err != nil {
				t.Fatalf("Error loading %d: %v", i, err)
			}
		}
	})
}

func TestFileStoreAndRetrieveMultiBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		subj, msg := "foo", []byte("Hello World!")
		storedMsgSize := fileStoreMsgSize(subj, nil, msg)

		fcfg.BlockSize = 4 * storedMsgSize
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 20; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		state := fs.State()
		if state.Msgs != 20 {
			t.Fatalf("Expected 20 msgs, got %d", state.Msgs)
		}
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		var smv StoreMsg
		for i := uint64(1); i <= 20; i++ {
			if _, err := fs.LoadMsg(i, &smv); err != nil {
				t.Fatalf("Error loading %d: %v", i, err)
			}
		}
	})
}

func TestFileStoreCollapseDmap(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		subj, msg := "foo", []byte("Hello World!")
		storedMsgSize := fileStoreMsgSize(subj, nil, msg)

		fcfg.BlockSize = 4 * storedMsgSize
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 10; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		state := fs.State()
		if state.Msgs != 10 {
			t.Fatalf("Expected 10 msgs, got %d", state.Msgs)
		}

		checkDmapTotal := func(total int) {
			t.Helper()
			if nde := fs.dmapEntries(); nde != total {
				t.Fatalf("Expecting only %d entries, got %d", total, nde)
			}
		}

		checkFirstSeq := func(seq uint64) {
			t.Helper()
			state := fs.State()
			if state.FirstSeq != seq {
				t.Fatalf("Expected first seq to be %d, got %d", seq, state.FirstSeq)
			}
		}

		// Now remove some out of order, forming gaps and entries in dmaps.
		fs.RemoveMsg(2)
		checkFirstSeq(1)
		fs.RemoveMsg(4)
		checkFirstSeq(1)
		fs.RemoveMsg(8)
		checkFirstSeq(1)

		state = fs.State()
		if state.Msgs != 7 {
			t.Fatalf("Expected 7 msgs, got %d", state.Msgs)
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
	})
}

func TestFileStoreReadCache(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.CacheExpire = 100 * time.Millisecond

		subj, msg := "foo.bar", make([]byte, 1024)
		storedMsgSize := fileStoreMsgSize(subj, nil, msg)

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		toStore := 500
		totalBytes := uint64(toStore) * storedMsgSize

		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg)
		}

		// Wait for cache to go to zero.
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if csz := fs.cacheSize(); csz != 0 {
				return fmt.Errorf("cache size not 0, got %s", friendlyBytes(int64(csz)))
			}
			return nil
		})

		fs.LoadMsg(1, nil)
		if csz := fs.cacheSize(); csz != totalBytes {
			t.Fatalf("Expected all messages to be cached, got %d vs %d", csz, totalBytes)
		}
		// Should expire and be removed.
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if csz := fs.cacheSize(); csz != 0 {
				return fmt.Errorf("cache size not 0, got %s", friendlyBytes(int64(csz)))
			}
			return nil
		})
		if cls := fs.cacheLoads(); cls != 1 {
			t.Fatalf("Expected only 1 cache load, got %d", cls)
		}
		// Now make sure we do not reload cache if there is activity.
		fs.LoadMsg(1, nil)
		timeout := time.Now().Add(250 * time.Millisecond)
		for time.Now().Before(timeout) {
			if cls := fs.cacheLoads(); cls != 2 {
				t.Fatalf("cache loads not 2, got %d", cls)
			}
			time.Sleep(5 * time.Millisecond)
			fs.LoadMsg(1, nil) // register activity.
		}
	})
}

func TestFileStorePartialCacheExpiration(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cexp := 10 * time.Millisecond
		fcfg.CacheExpire = cexp

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fs.StoreMsg("foo", nil, []byte("msg1"))

		// Should expire and be removed.
		time.Sleep(2 * cexp)
		fs.StoreMsg("bar", nil, []byte("msg2"))

		// Again wait for cache to expire.
		time.Sleep(2 * cexp)
		if _, err := fs.LoadMsg(1, nil); err != nil {
			t.Fatalf("Error loading message 1: %v", err)
		}
	})
}

func TestFileStorePartialIndexes(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cexp := 10 * time.Millisecond
		fcfg.CacheExpire = cexp

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		toSend := 5
		for i := 0; i < toSend; i++ {
			fs.StoreMsg("foo", nil, []byte("ok-1"))
		}

		// Now wait til the cache expires, including the index.
		fs.mu.Lock()
		mb := fs.blks[0]
		fs.mu.Unlock()

		// Force idx to expire by resetting last remove ts.
		mb.mu.Lock()
		mb.llts = mb.llts - int64(defaultCacheBufferExpiration*2)
		mb.mu.Unlock()

		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			mb.mu.Lock()
			defer mb.mu.Unlock()
			if mb.cache == nil || len(mb.cache.idx) == 0 {
				return nil
			}
			return fmt.Errorf("Index not empty")
		})

		// Create a partial cache by adding more msgs.
		for i := 0; i < toSend; i++ {
			fs.StoreMsg("foo", nil, []byte("ok-2"))
		}
		// If we now load in a message in second half if we do not
		// detect idx is a partial correctly this will panic.
		if _, err := fs.LoadMsg(8, nil); err != nil {
			t.Fatalf("Error loading %d: %v", 1, err)
		}
	})
}

func TestFileStoreSnapshot(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		subj, msg := "foo", []byte("Hello Snappy!")
		scfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}

		fs, err := newFileStoreWithCreated(fcfg, scfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		toSend := 2233
		for i := 0; i < toSend; i++ {
			fs.StoreMsg(subj, nil, msg)
		}

		// Create a few consumers.
		o1, err := fs.ConsumerStore("o22", &ConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		o2, err := fs.ConsumerStore("o33", &ConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		state := &ConsumerState{}
		state.Delivered.Consumer = 100
		state.Delivered.Stream = 100
		state.AckFloor.Consumer = 22
		state.AckFloor.Stream = 22

		if err := o1.Update(state); err != nil {
			t.Fatalf("Unexpected error updating state: %v", err)
		}
		state.AckFloor.Consumer = 33
		state.AckFloor.Stream = 33

		if err := o2.Update(state); err != nil {
			t.Fatalf("Unexpected error updating state: %v", err)
		}

		snapshot := func() []byte {
			t.Helper()
			r, err := fs.Snapshot(5*time.Second, true, true)
			if err != nil {
				t.Fatalf("Error creating snapshot")
			}
			snapshot, err := io.ReadAll(r.Reader)
			if err != nil {
				t.Fatalf("Error reading snapshot")
			}
			return snapshot
		}

		// This will unzip the snapshot and create a new filestore that will recover the state.
		// We will compare the states for this vs the original one.
		verifySnapshot := func(snap []byte) {
			t.Helper()
			r := bytes.NewReader(snap)
			tr := tar.NewReader(s2.NewReader(r))

			rstoreDir := t.TempDir()

			for {
				hdr, err := tr.Next()
				if err == io.EOF {
					break // End of archive
				}
				if err != nil {
					t.Fatalf("Error getting next entry from snapshot: %v", err)
				}
				fpath := filepath.Join(rstoreDir, filepath.Clean(hdr.Name))
				pdir := filepath.Dir(fpath)
				os.MkdirAll(pdir, 0755)
				fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0600)
				if err != nil {
					t.Fatalf("Error opening file[%s]: %v", fpath, err)
				}
				if _, err := io.Copy(fd, tr); err != nil {
					t.Fatalf("Error writing file[%s]: %v", fpath, err)
				}
				fd.Close()
			}

			fcfg.StoreDir = rstoreDir
			fsr, err := newFileStoreWithCreated(fcfg, scfg, time.Now(), prf(&fcfg), nil)
			require_NoError(t, err)
			defer fsr.Stop()
			state := fs.State()
			rstate := fsr.State()

			// FIXME(dlc)
			// Right now the upper layers in JetStream recover the consumers and do not expect
			// the lower layers to do that. So for now blank that out of our original state.
			// Will have more exhaustive tests in jetstream_test.go.
			state.Consumers = 0

			// Just check the state.
			if !reflect.DeepEqual(rstate, state) {
				t.Fatalf("Restored state does not match:\n%+v\n\n%+v", rstate, state)
			}
		}

		// Simple case first.
		snap := snapshot()
		verifySnapshot(snap)

		// Remove first 100 messages.
		for i := 1; i <= 100; i++ {
			fs.RemoveMsg(uint64(i))
		}

		snap = snapshot()
		verifySnapshot(snap)

		// Now sporadic messages inside the stream.
		total := int64(toSend - 100)
		// Delete 50 random messages.
		for i := 0; i < 50; i++ {
			seq := uint64(rand.Int63n(total) + 101)
			fs.RemoveMsg(seq)
		}

		snap = snapshot()
		verifySnapshot(snap)

		// Make sure compaction works with snapshots.
		fs.mu.RLock()
		for _, mb := range fs.blks {
			// Should not call compact on last msg block.
			if mb != fs.lmb {
				mb.mu.Lock()
				mb.compact()
				mb.mu.Unlock()
			}
		}
		fs.mu.RUnlock()

		snap = snapshot()
		verifySnapshot(snap)

		// Now check to make sure that we get the correct error when trying to delete or erase
		// a message when a snapshot is in progress and that closing the reader releases that condition.
		sr, err := fs.Snapshot(5*time.Second, false, true)
		if err != nil {
			t.Fatalf("Error creating snapshot")
		}
		if _, err := fs.RemoveMsg(122); err != ErrStoreSnapshotInProgress {
			t.Fatalf("Did not get the correct error on remove during snapshot: %v", err)
		}
		if _, err := fs.EraseMsg(122); err != ErrStoreSnapshotInProgress {
			t.Fatalf("Did not get the correct error on remove during snapshot: %v", err)
		}

		// Now make sure we can do these when we close the reader and release the snapshot condition.
		sr.Reader.Close()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if _, err := fs.RemoveMsg(122); err != nil {
				return fmt.Errorf("Got an error on remove after snapshot: %v", err)
			}
			return nil
		})

		// Make sure if we do not read properly then it will close the writer and report an error.
		sr, err = fs.Snapshot(25*time.Millisecond, false, false)
		if err != nil {
			t.Fatalf("Error creating snapshot")
		}

		// Cause snapshot to timeout.
		time.Sleep(50 * time.Millisecond)
		// Read should fail
		var buf [32]byte
		if _, err := sr.Reader.Read(buf[:]); err != io.EOF {
			t.Fatalf("Expected read to produce an error, got none")
		}
	})
}

func TestFileStoreConsumer(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		o, err := fs.ConsumerStore("obs22", &ConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if state, err := o.State(); err != nil || state.Delivered.Consumer != 0 {
			t.Fatalf("Unexpected state or error: %v", err)
		}

		state := &ConsumerState{}

		updateAndCheck := func() {
			t.Helper()
			if err := o.Update(state); err != nil {
				t.Fatalf("Unexpected error updating state: %v", err)
			}
			s2, err := o.State()
			if err != nil {
				t.Fatalf("Unexpected error getting state: %v", err)
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

		state.Delivered.Consumer = 1
		state.Delivered.Stream = 22
		updateAndCheck()

		state.Delivered.Consumer = 100
		state.Delivered.Stream = 122
		state.AckFloor.Consumer = 50
		state.AckFloor.Stream = 123
		// This should fail, bad state.
		shouldFail()
		// So should this.
		state.AckFloor.Consumer = 200
		state.AckFloor.Stream = 100
		shouldFail()

		// Should succeed
		state.AckFloor.Consumer = 50
		state.AckFloor.Stream = 72
		updateAndCheck()

		tn := time.Now().UnixNano()

		// We should sanity check pending here as well, so will check if a pending value is below
		// ack floor or above delivered.
		state.Pending = map[uint64]*Pending{70: {70, tn}}
		shouldFail()
		state.Pending = map[uint64]*Pending{140: {140, tn}}
		shouldFail()
		state.Pending = map[uint64]*Pending{72: {72, tn}} // exact on floor should fail
		shouldFail()

		// Put timestamps a second apart.
		// We will downsample to second resolution to save space. So setup our times
		// to reflect that.
		ago := time.Now().Add(-30 * time.Second).Truncate(time.Second)
		nt := func() *Pending {
			ago = ago.Add(time.Second)
			return &Pending{0, ago.UnixNano()}
		}
		// Should succeed.
		state.Pending = map[uint64]*Pending{75: nt(), 80: nt(), 83: nt(), 90: nt(), 111: nt()}
		updateAndCheck()

		// Now do redlivery, but first with no pending.
		state.Pending = nil
		state.Redelivered = map[uint64]uint64{22: 3, 44: 8}
		updateAndCheck()

		// All together.
		state.Pending = map[uint64]*Pending{75: nt(), 80: nt(), 83: nt(), 90: nt(), 111: nt()}
		updateAndCheck()

		// Large one
		state.Delivered.Consumer = 10000
		state.Delivered.Stream = 10000
		state.AckFloor.Consumer = 100
		state.AckFloor.Stream = 100
		// Generate 8k pending.
		state.Pending = make(map[uint64]*Pending)
		for len(state.Pending) < 8192 {
			seq := uint64(rand.Intn(9890) + 101)
			if _, ok := state.Pending[seq]; !ok {
				state.Pending[seq] = nt()
			}
		}
		updateAndCheck()

		state.Pending = nil
		state.AckFloor.Consumer = 10000
		state.AckFloor.Stream = 10000
		updateAndCheck()
	})
}

func TestFileStoreConsumerEncodeDecodeRedelivered(t *testing.T) {
	state := &ConsumerState{}

	state.Delivered.Consumer = 100
	state.Delivered.Stream = 100
	state.AckFloor.Consumer = 50
	state.AckFloor.Stream = 50

	state.Redelivered = map[uint64]uint64{122: 3, 144: 8}
	buf := encodeConsumerState(state)

	rstate, err := decodeConsumerState(buf)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(state, rstate) {
		t.Fatalf("States do not match: %+v vs %+v", state, rstate)
	}
}

func TestFileStoreConsumerEncodeDecodePendingBelowStreamAckFloor(t *testing.T) {
	state := &ConsumerState{}

	state.Delivered.Consumer = 1192
	state.Delivered.Stream = 10185
	state.AckFloor.Consumer = 1189
	state.AckFloor.Stream = 10815

	now := time.Now().Round(time.Second).Add(-10 * time.Second).UnixNano()
	state.Pending = map[uint64]*Pending{
		10782: {1190, now},
		10810: {1191, now + int64(time.Second)},
		10815: {1192, now + int64(2*time.Second)},
	}
	buf := encodeConsumerState(state)

	rstate, err := decodeConsumerState(buf)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(rstate.Pending) != 3 {
		t.Fatalf("Invalid pending: %v", rstate.Pending)
	}
	for k, v := range state.Pending {
		rv, ok := rstate.Pending[k]
		if !ok {
			t.Fatalf("Did not find sseq=%v", k)
		}
		if !reflect.DeepEqual(v, rv) {
			t.Fatalf("Pending for sseq=%v should be %+v, got %+v", k, v, rv)
		}
	}
	state.Pending, rstate.Pending = nil, nil
	if !reflect.DeepEqual(*state, *rstate) {
		t.Fatalf("States do not match: %+v vs %+v", state, rstate)
	}
}

func TestFileStoreWriteFailures(t *testing.T) {
	// This test should be run inside an environment where this directory
	// has a limited size.
	// E.g. Docker
	// docker run -ti --tmpfs /jswf_test:rw,size=32k --rm -v ~/Development/go/src:/go/src -w /go/src/github.com/nats-io/nats-server/ golang:1.21 /bin/bash
	tdir := filepath.Join("/", "jswf_test")
	if stat, err := os.Stat(tdir); err != nil || !stat.IsDir() {
		t.SkipNow()
	}

	storeDir := filepath.Join(tdir, JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.StoreDir = storeDir
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello Write Failures!")

		var lseq uint64
		// msz about will be ~54 bytes, so if limit is 32k trying to send 1000 will fail at some point.
		for i := 1; i <= 1000; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
				lseq = uint64(i)
				break
			}
		}
		if lseq == 0 {
			t.Fatalf("Expected to get a failure but did not")
		}

		state := fs.State()

		if state.LastSeq != lseq-1 {
			t.Fatalf("Expected last seq to be %d, got %d\n", lseq-1, state.LastSeq)
		}
		if state.Msgs != lseq-1 {
			t.Fatalf("Expected total msgs to be %d, got %d\n", lseq-1, state.Msgs)
		}
		if _, err := fs.LoadMsg(lseq, nil); err == nil {
			t.Fatalf("Expected error loading seq that failed, got none")
		}
		// Loading should still work.
		if _, err := fs.LoadMsg(1, nil); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Make sure we recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state2 := fs.State()

		// Ignore lost state.
		state.Lost, state2.Lost = nil, nil
		if !reflect.DeepEqual(state2, state) {
			t.Fatalf("Expected recovered state to be the same\n%+v\nvs\n%+v\n", state2, state)
		}

		// We should still fail here.
		for i := 1; i <= 100; i++ {
			_, _, err = fs.StoreMsg(subj, nil, msg)
			if err != nil {
				break
			}
		}
		require_Error(t, err)

		lseq = fs.State().LastSeq + 1

		// Purge should help.
		if _, err := fs.Purge(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Wait for purge to complete its out of band processing.
		time.Sleep(50 * time.Millisecond)

		// Check we will fail again in same spot.
		for i := 1; i <= 1000; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
				if i != int(lseq) {
					t.Fatalf("Expected to fail after purge about the same spot, wanted %d got %d", lseq, i)
				}
				break
			}
		}
	})
}

func TestFileStorePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.AsyncFlush = true

		subj, msg := "foo", make([]byte, 1024-33)
		for i := 0; i < len(msg); i++ {
			msg[i] = 'D'
		}
		storedMsgSize := fileStoreMsgSize(subj, nil, msg)

		// 5GB
		toStore := 5 * 1024 * 1024 * 1024 / storedMsgSize

		fmt.Printf("storing %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		start := time.Now()
		for i := 0; i < int(toStore); i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		fs.Stop()

		tt := time.Since(start)
		fmt.Printf("time to store is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

		fmt.Printf("Filesystem cache flush, paused 5 seconds.\n\n")
		time.Sleep(5 * time.Second)

		fmt.Printf("Restoring..\n")
		start = time.Now()
		fcfg.AsyncFlush = false
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fmt.Printf("time to restore is %v\n\n", time.Since(start))

		fmt.Printf("LOAD: reading %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		var smv StoreMsg
		start = time.Now()
		for i := uint64(1); i <= toStore; i++ {
			if _, err := fs.LoadMsg(i, &smv); err != nil {
				t.Fatalf("Error loading %d: %v", i, err)
			}
		}

		tt = time.Since(start)
		fmt.Printf("time to read all back messages is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

		// Do again to test skip for hash..
		fmt.Printf("\nSKIP CHECKSUM: reading %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		start = time.Now()
		for i := uint64(1); i <= toStore; i++ {
			if _, err := fs.LoadMsg(i, &smv); err != nil {
				t.Fatalf("Error loading %d: %v", i, err)
			}
		}

		tt = time.Since(start)
		fmt.Printf("time to read all back messages is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fmt.Printf("\nremoving [in order] %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
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
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state := fs.State()
		if state.Msgs != 0 {
			t.Fatalf("Expected no msgs, got %d", state.Msgs)
		}
		if state.Bytes != 0 {
			t.Fatalf("Expected no bytes, got %d", state.Bytes)
		}
	})
}

func TestFileStoreReadBackMsgPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	subj := "foo"
	msg := []byte("ABCDEFGH") // Smaller shows problems more.

	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	// Make sure we store 2 blocks.
	toStore := defaultLargeBlockSize * 2 / storedMsgSize

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fmt.Printf("storing %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		start := time.Now()
		for i := 0; i < int(toStore); i++ {
			fs.StoreMsg(subj, nil, msg)
		}

		tt := time.Since(start)
		fmt.Printf("time to store is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

		// We should not have cached here with no reads.
		// Pick something towards end of the block.
		index := defaultLargeBlockSize/storedMsgSize - 22
		start = time.Now()
		fs.LoadMsg(index, nil)
		fmt.Printf("Time to load first msg [%d] = %v\n", index, time.Since(start))

		start = time.Now()
		fs.LoadMsg(index+2, nil)
		fmt.Printf("Time to load second msg [%d] = %v\n", index+2, time.Since(start))
	})
}

// This test is testing an upper level stream with a message or byte limit.
// Even though this is 1, any limit would trigger same behavior once the limit was reached
// and we were adding and removing.
func TestFileStoreStoreLimitRemovePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	subj, msg := "foo", make([]byte, 1024-33)
	for i := 0; i < len(msg); i++ {
		msg[i] = 'D'
	}
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	// 1GB
	toStore := 1 * 1024 * 1024 * 1024 / storedMsgSize

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fs.RegisterStorageUpdates(func(md, bd int64, seq uint64, subj string) {})

		fmt.Printf("storing and removing (limit 1) %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		start := time.Now()
		for i := 0; i < int(toStore); i++ {
			seq, _, err := fs.StoreMsg(subj, nil, msg)
			if err != nil {
				t.Fatalf("Unexpected error storing message: %v", err)
			}
			if i > 0 {
				fs.RemoveMsg(seq - 1)
			}
		}
		fs.Stop()

		tt := time.Since(start)
		fmt.Printf("time to store and remove all messages is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))
	})
}

func TestFileStorePubPerfWithSmallBlkSize(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	subj, msg := "foo", make([]byte, 1024-33)
	for i := 0; i < len(msg); i++ {
		msg[i] = 'D'
	}
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	// 1GB
	toStore := 1 * 1024 * 1024 * 1024 / storedMsgSize
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fmt.Printf("storing %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		fcfg.BlockSize = FileStoreMinBlkSize

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		start := time.Now()
		for i := 0; i < int(toStore); i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		fs.Stop()

		tt := time.Since(start)
		fmt.Printf("time to store is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))
	})
}

// Saw this manifest from a restart test with max delivered set for JetStream.
func TestFileStoreConsumerRedeliveredLost(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		cfg := &ConsumerConfig{AckPolicy: AckExplicit}
		o, err := fs.ConsumerStore("o22", cfg)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		restartConsumer := func() {
			t.Helper()
			o.Stop()
			time.Sleep(20 * time.Millisecond) // Wait for all things to settle.
			o, err = fs.ConsumerStore("o22", cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Make sure we recovered Redelivered.
			state, err := o.State()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if state == nil {
				t.Fatalf("Did not recover state")
			}
			if len(state.Redelivered) == 0 {
				t.Fatalf("Did not recover redelivered")
			}
		}

		ts := time.Now().UnixNano()
		o.UpdateDelivered(1, 1, 1, ts)
		o.UpdateDelivered(2, 1, 2, ts)
		o.UpdateDelivered(3, 1, 3, ts)
		o.UpdateDelivered(4, 1, 4, ts)
		o.UpdateDelivered(5, 2, 1, ts)

		restartConsumer()

		o.UpdateDelivered(6, 2, 2, ts)
		o.UpdateDelivered(7, 3, 1, ts)

		restartConsumer()
		if state, _ := o.State(); len(state.Pending) != 3 {
			t.Fatalf("Did not recover pending correctly")
		}

		o.UpdateAcks(7, 3)
		o.UpdateAcks(6, 2)

		restartConsumer()
		o.UpdateAcks(4, 1)

		state, _ := o.State()
		if len(state.Pending) != 0 {
			t.Fatalf("Did not clear pending correctly")
		}
		if len(state.Redelivered) != 0 {
			t.Fatalf("Did not clear redelivered correctly")
		}
	})
}

func TestFileStoreConsumerFlusher(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		o, err := fs.ConsumerStore("o22", &ConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Get the underlying impl.
		oc := o.(*consumerFileStore)
		// Wait for flusher to be running.
		checkFor(t, time.Second, 20*time.Millisecond, func() error {
			if !oc.inFlusher() {
				return fmt.Errorf("Flusher not running")
			}
			return nil
		})
		// Stop and make sure the flusher goes away
		o.Stop()
		// Wait for flusher to stop.
		checkFor(t, time.Second, 20*time.Millisecond, func() error {
			if oc.inFlusher() {
				return fmt.Errorf("Flusher still running")
			}
			return nil
		})
	})
}

func TestFileStoreConsumerDeliveredUpdates(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Simple consumer, no ack policy configured.
		o, err := fs.ConsumerStore("o22", &ConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer o.Stop()

		testDelivered := func(dseq, sseq uint64) {
			t.Helper()
			ts := time.Now().UnixNano()
			if err := o.UpdateDelivered(dseq, sseq, 1, ts); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			state, err := o.State()
			if err != nil {
				t.Fatalf("Error getting state: %v", err)
			}
			if state == nil {
				t.Fatalf("No state available")
			}
			expected := SequencePair{dseq, sseq}
			if state.Delivered != expected {
				t.Fatalf("Unexpected state, wanted %+v, got %+v", expected, state.Delivered)
			}
			if state.AckFloor != expected {
				t.Fatalf("Unexpected ack floor state, wanted %+v, got %+v", expected, state.AckFloor)
			}
			if len(state.Pending) != 0 {
				t.Fatalf("Did not expect any pending, got %d pending", len(state.Pending))
			}
		}

		testDelivered(1, 100)
		testDelivered(2, 110)
		testDelivered(5, 130)

		// If we try to do an ack this should err since we are not configured with ack policy.
		if err := o.UpdateAcks(1, 100); err != ErrNoAckPolicy {
			t.Fatalf("Expected a no ack policy error on update acks, got %v", err)
		}
		// Also if we do an update with a delivery count of anything but 1 here should also give same error.
		ts := time.Now().UnixNano()
		if err := o.UpdateDelivered(5, 130, 2, ts); err != ErrNoAckPolicy {
			t.Fatalf("Expected a no ack policy error on update delivered with dc > 1, got %v", err)
		}
	})
}

func TestFileStoreConsumerDeliveredAndAckUpdates(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Simple consumer, no ack policy configured.
		o, err := fs.ConsumerStore("o22", &ConsumerConfig{AckPolicy: AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer o.Stop()

		// Track pending.
		var pending int

		testDelivered := func(dseq, sseq uint64) {
			t.Helper()
			ts := time.Now().Round(time.Second).UnixNano()
			if err := o.UpdateDelivered(dseq, sseq, 1, ts); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			pending++
			state, err := o.State()
			if err != nil {
				t.Fatalf("Error getting state: %v", err)
			}
			if state == nil {
				t.Fatalf("No state available")
			}
			expected := SequencePair{dseq, sseq}
			if state.Delivered != expected {
				t.Fatalf("Unexpected delivered state, wanted %+v, got %+v", expected, state.Delivered)
			}
			if len(state.Pending) != pending {
				t.Fatalf("Expected %d pending, got %d pending", pending, len(state.Pending))
			}
		}

		testDelivered(1, 100)
		testDelivered(2, 110)
		testDelivered(3, 130)
		testDelivered(4, 150)
		testDelivered(5, 165)

		testBadAck := func(dseq, sseq uint64) {
			t.Helper()
			if err := o.UpdateAcks(dseq, sseq); err == nil {
				t.Fatalf("Expected error but got none")
			}
		}
		testBadAck(3, 101)
		testBadAck(1, 1)

		testAck := func(dseq, sseq, dflr, sflr uint64) {
			t.Helper()
			if err := o.UpdateAcks(dseq, sseq); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			pending--
			state, err := o.State()
			if err != nil {
				t.Fatalf("Error getting state: %v", err)
			}
			if state == nil {
				t.Fatalf("No state available")
			}
			if len(state.Pending) != pending {
				t.Fatalf("Expected %d pending, got %d pending", pending, len(state.Pending))
			}
			eflr := SequencePair{dflr, sflr}
			if state.AckFloor != eflr {
				t.Fatalf("Unexpected ack floor state, wanted %+v, got %+v", eflr, state.AckFloor)
			}
		}

		testAck(1, 100, 1, 109)
		testAck(3, 130, 1, 109)
		testAck(2, 110, 3, 149) // We do not track explicit state on previous stream floors, so we take last known -1
		testAck(5, 165, 3, 149)
		testAck(4, 150, 5, 165)

		testDelivered(6, 170)
		testDelivered(7, 171)
		testDelivered(8, 172)
		testDelivered(9, 173)
		testDelivered(10, 200)

		testAck(7, 171, 5, 165)
		testAck(8, 172, 5, 165)

		state, err := o.State()
		if err != nil {
			t.Fatalf("Unexpected error getting state: %v", err)
		}
		o.Stop()

		o, err = fs.ConsumerStore("o22", &ConsumerConfig{AckPolicy: AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer o.Stop()

		nstate, err := o.State()
		if err != nil {
			t.Fatalf("Unexpected error getting state: %v", err)
		}
		if !reflect.DeepEqual(nstate, state) {
			t.Fatalf("States don't match! NEW %+v OLD %+v", nstate, state)
		}
	})
}

func TestFileStoreStreamStateDeleted(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, toStore := "foo", uint64(10)
		for i := uint64(1); i <= toStore; i++ {
			msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
			if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			}
		}
		state := fs.State()
		if len(state.Deleted) != 0 {
			t.Fatalf("Expected deleted to be empty")
		}
		// Now remove some interior messages.
		var expected []uint64
		for seq := uint64(2); seq < toStore; seq += 2 {
			fs.RemoveMsg(seq)
			expected = append(expected, seq)
		}
		state = fs.State()
		if !reflect.DeepEqual(state.Deleted, expected) {
			t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
		}
		// Now fill the gap by deleting 1 and 3
		fs.RemoveMsg(1)
		fs.RemoveMsg(3)
		expected = expected[2:]
		state = fs.State()
		if !reflect.DeepEqual(state.Deleted, expected) {
			t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
		}
		if state.FirstSeq != 5 {
			t.Fatalf("Expected first seq to be 5, got %d", state.FirstSeq)
		}
		fs.Purge()
		if state = fs.State(); len(state.Deleted) != 0 {
			t.Fatalf("Expected no deleted after purge, got %+v\n", state.Deleted)
		}
	})
}

// We have reports that sometimes under load a stream could complain about a storage directory
// not being empty.
func TestFileStoreStreamDeleteDirNotEmpty(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, toStore := "foo", uint64(10)
		for i := uint64(1); i <= toStore; i++ {
			msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
			if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			}
		}

		ready := make(chan bool)
		go func() {
			g := filepath.Join(fcfg.StoreDir, "g")
			ready <- true
			for i := 0; i < 100; i++ {
				os.WriteFile(g, []byte("OK"), defaultFilePerms)
			}
		}()

		<-ready
		if err := fs.Delete(); err != nil {
			t.Fatalf("Delete returned an error: %v", err)
		}
	})
}

func TestFileStoreConsumerPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		o, err := fs.ConsumerStore("o22", &ConsumerConfig{AckPolicy: AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Get the underlying impl.
		oc := o.(*consumerFileStore)
		// Wait for flusher to br running
		checkFor(t, time.Second, 20*time.Millisecond, func() error {
			if !oc.inFlusher() {
				return fmt.Errorf("not in flusher")
			}
			return nil
		})

		// Stop flusher for this benchmark since we will invoke directly.
		oc.mu.Lock()
		qch := oc.qch
		oc.qch = nil
		oc.mu.Unlock()
		close(qch)

		checkFor(t, time.Second, 20*time.Millisecond, func() error {
			if oc.inFlusher() {
				return fmt.Errorf("still in flusher")
			}
			return nil
		})

		toStore := uint64(1_000_000)

		start := time.Now()

		ts := start.UnixNano()

		for i := uint64(1); i <= toStore; i++ {
			if err := o.UpdateDelivered(i, i, 1, ts); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
		tt := time.Since(start)
		fmt.Printf("time to update %d is %v\n", toStore, tt)
		fmt.Printf("%.0f updates/sec\n", float64(toStore)/tt.Seconds())

		start = time.Now()
		oc.mu.Lock()
		buf, err := oc.encodeState()
		oc.mu.Unlock()
		if err != nil {
			t.Fatalf("Error encoding state: %v", err)
		}
		fmt.Printf("time to encode %d bytes is %v\n", len(buf), time.Since(start))
		start = time.Now()
		oc.writeState(buf)
		fmt.Printf("time to write is %v\n", time.Since(start))
	})
}

// Reported by Ivan.
func TestFileStoreStreamDeleteCacheBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.CacheExpire = 50 * time.Millisecond

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")

		if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := fs.EraseMsg(1); err != nil {
			t.Fatalf("Got an error on remove of %d: %v", 1, err)
		}
		time.Sleep(100 * time.Millisecond)
		if _, err := fs.LoadMsg(2, nil); err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
	})
}

// rip
func TestFileStoreStreamFailToRollBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 512

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxBytes: 300}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure we properly roll underlying blocks.
		n, msg := 200, bytes.Repeat([]byte("ABC"), 33) // ~100bytes
		for i := 0; i < n; i++ {
			if _, _, err := fs.StoreMsg("zzz", nil, msg); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		// Grab some info for introspection.
		fs.mu.RLock()
		numBlks := len(fs.blks)
		var index uint32
		var blkSize int64
		if numBlks > 0 {
			mb := fs.blks[0]
			mb.mu.RLock()
			index = mb.index
			if fi, _ := os.Stat(mb.mfn); fi != nil {
				blkSize = fi.Size()
			}
			mb.mu.RUnlock()
		}
		fs.mu.RUnlock()

		if numBlks != 1 {
			t.Fatalf("Expected only one block, got %d", numBlks)
		}
		if index < 60 {
			t.Fatalf("Expected a block index > 60, got %d", index)
		}
		if blkSize > 512 {
			t.Fatalf("Expected block to be <= 512, got %d", blkSize)
		}
	})
}

// We had a case where a consumer state had a redelivered record that had seq of 0.
// This was causing the server to panic.
func TestFileStoreBadConsumerState(t *testing.T) {
	bs := []byte("\x16\x02\x01\x01\x03\x02\x01\x98\xf4\x8a\x8a\f\x01\x03\x86\xfa\n\x01\x00\x01")
	if cs, err := decodeConsumerState(bs); err != nil || cs == nil {
		t.Fatalf("Expected to not throw error, got %v and %+v", err, cs)
	}
}

func TestFileStoreExpireMsgsOnStart(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8 * 1024
		ttl := 250 * time.Millisecond
		cfg := StreamConfig{Name: "ORDERS", Subjects: []string{"orders.*"}, Storage: FileStorage, MaxAge: ttl}
		var fs *fileStore

		startFS := func() *fileStore {
			t.Helper()
			fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
			require_NoError(t, err)
			return fs
		}

		newFS := func() *fileStore {
			t.Helper()
			if fs != nil {
				fs.Stop()
				fs = nil
			}
			removeDir(t, fcfg.StoreDir)
			return startFS()
		}

		restartFS := func(delay time.Duration) *fileStore {
			if fs != nil {
				fs.Stop()
				fs = nil
				time.Sleep(delay)
			}
			fs = startFS()
			return fs
		}

		fs = newFS()
		defer fs.Stop()

		msg := bytes.Repeat([]byte("ABC"), 33) // ~100bytes
		loadMsgs := func(n int) {
			t.Helper()
			for i := 1; i <= n; i++ {
				if _, _, err := fs.StoreMsg(fmt.Sprintf("orders.%d", i%10), nil, msg); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
		}

		checkState := func(msgs, first, last uint64) {
			t.Helper()
			if fs == nil {
				t.Fatalf("No fs")
				return
			}
			state := fs.State()
			if state.Msgs != msgs {
				t.Fatalf("Expected %d msgs, got %d", msgs, state.Msgs)
			}
			if state.FirstSeq != first {
				t.Fatalf("Expected %d as first, got %d", first, state.FirstSeq)
			}
			if state.LastSeq != last {
				t.Fatalf("Expected %d as last, got %d", last, state.LastSeq)
			}
		}

		checkNumBlks := func(expected int) {
			t.Helper()
			fs.mu.RLock()
			n := len(fs.blks)
			fs.mu.RUnlock()
			if n != expected {
				t.Fatalf("Expected %d msg blks, got %d", expected, n)
			}
		}

		// Check the filtered subject state and make sure that is tracked properly.
		checkFiltered := func(subject string, ss SimpleState) {
			t.Helper()
			fss := fs.FilteredState(1, subject)
			if fss != ss {
				t.Fatalf("Expected FilteredState of %+v, got %+v", ss, fss)
			}
		}

		// Make sure state on disk matches (e.g. writeIndexInfo properly called)
		checkBlkState := func(index int) {
			t.Helper()
			fs.mu.RLock()
			if index >= len(fs.blks) {
				t.Fatalf("Out of range, wanted %d but only %d blks", index, len(fs.blks))
			}
			fs.mu.RUnlock()
		}

		lastSeqForBlk := func(index int) uint64 {
			t.Helper()
			fs.mu.RLock()
			defer fs.mu.RUnlock()
			if len(fs.blks) == 0 {
				t.Fatalf("No blocks?")
			}
			mb := fs.blks[0]
			mb.mu.RLock()
			defer mb.mu.RUnlock()
			return mb.last.seq
		}

		// Actual testing here.

		loadMsgs(500)
		restartFS(ttl + 100*time.Millisecond)
		checkState(0, 501, 500)
		// We actually hold onto the last one now to remember our starting sequence.
		checkNumBlks(1)

		// Now check partial expires and the fss tracking state.
		// Small numbers is to keep them in one block.
		fs = newFS()
		loadMsgs(10)
		time.Sleep(100 * time.Millisecond)
		loadMsgs(10)
		checkFiltered("orders.*", SimpleState{Msgs: 20, First: 1, Last: 20})

		restartFS(ttl - 100*time.Millisecond + 25*time.Millisecond) // Just want half
		checkState(10, 11, 20)
		checkNumBlks(1)
		checkFiltered("orders.*", SimpleState{Msgs: 10, First: 11, Last: 20})
		checkFiltered("orders.5", SimpleState{Msgs: 1, First: 15, Last: 15})
		checkBlkState(0)

		fs = newFS()
		loadMsgs(5)
		time.Sleep(100 * time.Millisecond)
		loadMsgs(15)
		restartFS(ttl - 100*time.Millisecond + 25*time.Millisecond) // Just want half
		checkState(15, 6, 20)
		checkFiltered("orders.*", SimpleState{Msgs: 15, First: 6, Last: 20})
		checkFiltered("orders.5", SimpleState{Msgs: 2, First: 10, Last: 20})

		// Now we want to test that if the end of a msg block is all deletes msgs that we do the right thing.
		fs = newFS()
		loadMsgs(150)
		time.Sleep(100 * time.Millisecond)
		loadMsgs(100)

		checkNumBlks(5)

		// Now delete 10 messages from the end of the first block which we will expire on restart.
		// We will expire up to seq 100, so delete 91-100.
		lseq := lastSeqForBlk(0)
		for seq := lseq; seq > lseq-10; seq-- {
			removed, err := fs.RemoveMsg(seq)
			if err != nil || !removed {
				t.Fatalf("Error removing message: %v", err)
			}
		}
		restartFS(ttl - 100*time.Millisecond + 25*time.Millisecond) // Just want half
		checkState(100, 151, 250)
		checkNumBlks(3) // We should only have 3 blks left.
		checkBlkState(0)

		// Now make sure that we properly clean up any internal dmap entries (sparse) when expiring.
		fs = newFS()
		loadMsgs(10)
		// Remove some in sparse fashion, adding to dmap.
		fs.RemoveMsg(2)
		fs.RemoveMsg(4)
		fs.RemoveMsg(6)
		time.Sleep(100 * time.Millisecond)
		loadMsgs(10)
		restartFS(ttl - 100*time.Millisecond + 25*time.Millisecond) // Just want half
		checkState(10, 11, 20)
		checkNumBlks(1)
		checkBlkState(0)

		// Make sure expiring a block with tail deleted messages removes the message block etc.
		fs = newFS()
		loadMsgs(7)
		time.Sleep(100 * time.Millisecond)
		loadMsgs(3)
		fs.RemoveMsg(8)
		fs.RemoveMsg(9)
		fs.RemoveMsg(10)
		restartFS(ttl - 100*time.Millisecond + 25*time.Millisecond)
		checkState(0, 11, 10)

		fs.Stop()
		// Not for start per se but since we have all the test tooling here check that Compact() does right thing as well.
		fs = newFS()
		defer fs.Stop()
		loadMsgs(100)
		checkFiltered("orders.*", SimpleState{Msgs: 100, First: 1, Last: 100})
		checkFiltered("orders.5", SimpleState{Msgs: 10, First: 5, Last: 95})
		// Check that Compact keeps fss updated, does dmap etc.
		fs.Compact(51)
		checkFiltered("orders.*", SimpleState{Msgs: 50, First: 51, Last: 100})
		checkFiltered("orders.5", SimpleState{Msgs: 5, First: 55, Last: 95})
		checkBlkState(0)
	})
}

func TestFileStoreSparseCompaction(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1024 * 1024
		cfg := StreamConfig{Name: "KV", Subjects: []string{"kv.>"}, Storage: FileStorage}

		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		msg := bytes.Repeat([]byte("ABC"), 33) // ~100bytes
		loadMsgs := func(n int) {
			t.Helper()
			for i := 1; i <= n; i++ {
				if _, _, err := fs.StoreMsg(fmt.Sprintf("kv.%d", i%10), nil, msg); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
		}

		checkState := func(msgs, first, last uint64) {
			t.Helper()
			if fs == nil {
				t.Fatalf("No fs")
				return
			}
			state := fs.State()
			if state.Msgs != msgs {
				t.Fatalf("Expected %d msgs, got %d", msgs, state.Msgs)
			}
			if state.FirstSeq != first {
				t.Fatalf("Expected %d as first, got %d", first, state.FirstSeq)
			}
			if state.LastSeq != last {
				t.Fatalf("Expected %d as last, got %d", last, state.LastSeq)
			}
		}

		deleteMsgs := func(seqs ...uint64) {
			t.Helper()
			for _, seq := range seqs {
				removed, err := fs.RemoveMsg(seq)
				if err != nil || !removed {
					t.Fatalf("Got an error on remove of %d: %v", seq, err)
				}
			}
		}

		eraseMsgs := func(seqs ...uint64) {
			t.Helper()
			for _, seq := range seqs {
				removed, err := fs.EraseMsg(seq)
				if err != nil || !removed {
					t.Fatalf("Got an error on erase of %d: %v", seq, err)
				}
			}
		}

		compact := func() {
			t.Helper()
			var ssb, ssa StreamState
			fs.FastState(&ssb)
			tb, ub, _ := fs.Utilization()

			fs.mu.RLock()
			if len(fs.blks) == 0 {
				t.Fatalf("No blocks?")
			}
			mb := fs.blks[0]
			fs.mu.RUnlock()

			mb.mu.Lock()
			mb.compact()
			mb.mu.Unlock()

			fs.FastState(&ssa)
			if !reflect.DeepEqual(ssb, ssa) {
				t.Fatalf("States do not match; %+v vs %+v", ssb, ssa)
			}
			ta, ua, _ := fs.Utilization()
			if ub != ua {
				t.Fatalf("Expected used to be the same, got %d vs %d", ub, ua)
			}
			if ta >= tb {
				t.Fatalf("Expected total after to be less then before, got %d vs %d", tb, ta)
			}
		}

		// Actual testing here.
		loadMsgs(1000)
		checkState(1000, 1, 1000)

		// Now delete a few messages.
		deleteMsgs(1)
		compact()

		deleteMsgs(1000, 999, 998, 997)
		compact()

		eraseMsgs(500, 502, 504, 506, 508, 510)
		compact()
	})
}

func TestFileStoreSparseCompactionWithInteriorDeletes(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "KV", Subjects: []string{"kv.>"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 1; i <= 1000; i++ {
			if _, _, err := fs.StoreMsg(fmt.Sprintf("kv.%d", i%10), nil, []byte("OK")); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		// Now do interior deletes.
		for _, seq := range []uint64{500, 600, 700, 800} {
			removed, err := fs.RemoveMsg(seq)
			if err != nil || !removed {
				t.Fatalf("Got an error on remove of %d: %v", seq, err)
			}
		}

		_, err = fs.LoadMsg(900, nil)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Do compact by hand, make sure we can still access msgs past the interior deletes.
		fs.mu.RLock()
		lmb := fs.lmb
		lmb.dirtyCloseWithRemove(false)
		lmb.compact()
		fs.mu.RUnlock()

		if _, err = fs.LoadMsg(900, nil); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	})
}

// When messages span multiple blocks and we want to purge but keep some amount, say 1, we would remove all.
// This is because we would not break out of iterator across more message blocks.
// Issue #2622
func TestFileStorePurgeExKeepOneBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 128
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fill := bytes.Repeat([]byte("X"), 128)

		fs.StoreMsg("A", nil, []byte("META"))
		fs.StoreMsg("B", nil, fill)
		fs.StoreMsg("A", nil, []byte("META"))
		fs.StoreMsg("B", nil, fill)

		if fss := fs.FilteredState(1, "A"); fss.Msgs != 2 {
			t.Fatalf("Expected to find 2 `A` msgs, got %d", fss.Msgs)
		}

		n, err := fs.PurgeEx("A", 0, 1)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if n != 1 {
			t.Fatalf("Expected PurgeEx to remove 1 `A` msgs, got %d", n)
		}
		if fss := fs.FilteredState(1, "A"); fss.Msgs != 1 {
			t.Fatalf("Expected to find 1 `A` msgs, got %d", fss.Msgs)
		}
	})
}

func TestFileStoreFilteredPendingBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fs.StoreMsg("foo", nil, []byte("msg"))
		fs.StoreMsg("bar", nil, []byte("msg"))
		fs.StoreMsg("baz", nil, []byte("msg"))

		fs.mu.Lock()
		mb := fs.lmb
		fs.mu.Unlock()

		total, f, l := mb.filteredPending("foo", false, 3)
		if total != 0 {
			t.Fatalf("Expected total of 0 but got %d", total)
		}
		if f != 0 || l != 0 {
			t.Fatalf("Expected first and last to be 0 as well, but got %d %d", f, l)
		}
	})
}

// Test to optimize the selectMsgBlock with lots of blocks.
func TestFileStoreFetchPerf(t *testing.T) {
	// Comment out to run.
	t.SkipNow()

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8192
		fcfg.AsyncFlush = true
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Will create 25k msg blocks.
		n, subj, msg := 100_000, "zzz", bytes.Repeat([]byte("ABC"), 600)
		for i := 0; i < n; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		// Time how long it takes us to load all messages.
		var smv StoreMsg
		now := time.Now()
		for i := 0; i < n; i++ {
			_, err := fs.LoadMsg(uint64(i), &smv)
			if err != nil {
				t.Fatalf("Unexpected error looking up seq %d: %v", i, err)
			}
		}
		fmt.Printf("Elapsed to load all messages is %v\n", time.Since(now))
	})
}

// For things like raft log when we compact and have a message block that could reclaim > 50% of space for block we want to do that.
// https://github.com/nats-io/nats-server/issues/2936
func TestFileStoreCompactReclaimHeadSpace(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 4 * 1024 * 1024
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Create random bytes for payload to test for corruption vs repeated.
		msg := make([]byte, 64*1024)
		crand.Read(msg)

		// This gives us ~63 msgs in first and ~37 in second.
		n, subj := 100, "z"
		for i := 0; i < n; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg)
			require_NoError(t, err)
		}

		checkNumBlocks := func(n int) {
			t.Helper()
			fs.mu.RLock()
			defer fs.mu.RUnlock()
			if len(fs.blks) != n {
				t.Fatalf("Expected to have %d blocks, got %d", n, len(fs.blks))
			}
		}

		getBlock := func(index int) *msgBlock {
			t.Helper()
			fs.mu.RLock()
			defer fs.mu.RUnlock()
			return fs.blks[index]
		}

		// Check that we did right thing and actually reclaimed since > 50%
		checkBlock := func(mb *msgBlock) {
			t.Helper()

			mb.mu.RLock()
			nbytes, rbytes, mfn := mb.bytes, mb.rbytes, mb.mfn
			fseq, lseq := mb.first.seq, mb.last.seq
			mb.mu.RUnlock()

			// Check that sizes match as long as we are not doing compression.
			if fcfg.Compression == NoCompression {
				// Check rbytes then the actual file as well.
				if nbytes != rbytes {
					t.Fatalf("Expected to reclaim and have bytes == rbytes, got %d vs %d", nbytes, rbytes)
				}
				file, err := os.Open(mfn)
				require_NoError(t, err)
				defer file.Close()
				fi, err := file.Stat()
				require_NoError(t, err)
				if rbytes != uint64(fi.Size()) {
					t.Fatalf("Expected to rbytes == fi.Size, got %d vs %d", rbytes, fi.Size())
				}
			}

			// Make sure we can pull messages and that they are ok.
			var smv StoreMsg
			sm, err := fs.LoadMsg(fseq, &smv)
			require_NoError(t, err)
			if !bytes.Equal(sm.msg, msg) {
				t.Fatalf("Msgs don't match, original %q vs %q", msg, sm.msg)
			}
			sm, err = fs.LoadMsg(lseq, &smv)
			require_NoError(t, err)
			if !bytes.Equal(sm.msg, msg) {
				t.Fatalf("Msgs don't match, original %q vs %q", msg, sm.msg)
			}
		}

		checkNumBlocks(2)
		_, err = fs.Compact(33)
		require_NoError(t, err)

		checkNumBlocks(2)
		checkBlock(getBlock(0))
		checkBlock(getBlock(1))

		_, err = fs.Compact(85)
		require_NoError(t, err)

		checkNumBlocks(1)
		checkBlock(getBlock(0))

		// Make sure we can write.
		_, _, err = fs.StoreMsg(subj, nil, msg)
		require_NoError(t, err)

		checkNumBlocks(1)
		checkBlock(getBlock(0))

		// Stop and start again.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		checkNumBlocks(1)
		checkBlock(getBlock(0))

		// Make sure we can write.
		_, _, err = fs.StoreMsg(subj, nil, msg)
		require_NoError(t, err)
	})
}

func TestFileStoreRememberLastMsgTime(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		var fs *fileStore
		cfg := StreamConfig{Name: "TEST", Storage: FileStorage, MaxAge: 500 * time.Millisecond}

		getFS := func() *fileStore {
			t.Helper()
			fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
			require_NoError(t, err)
			return fs
		}
		restartFS := func() {
			t.Helper()
			fs.Stop()
			fs = getFS()
		}

		msg := bytes.Repeat([]byte("X"), 2*1024*1024)

		// Get first one.
		fs = getFS()
		defer fs.Stop()

		seq, ts, err := fs.StoreMsg("foo", nil, msg)
		require_NoError(t, err)
		// We will test that last msg time survives from delete, purge and expires after restart.
		removed, err := fs.RemoveMsg(seq)
		require_NoError(t, err)
		require_True(t, removed)

		lt := time.Unix(0, ts).UTC()
		require_True(t, lt == fs.State().LastTime)

		// Restart
		restartFS()

		// Test that last time survived.
		require_True(t, lt == fs.State().LastTime)

		seq, ts, err = fs.StoreMsg("foo", nil, msg)
		require_NoError(t, err)

		var smv StoreMsg
		_, err = fs.LoadMsg(seq, &smv)
		require_NoError(t, err)

		fs.Purge()

		// Restart
		restartFS()

		lt = time.Unix(0, ts).UTC()
		require_True(t, lt == fs.State().LastTime)

		_, _, err = fs.StoreMsg("foo", nil, msg)
		require_NoError(t, err)
		seq, ts, err = fs.StoreMsg("foo", nil, msg)
		require_NoError(t, err)

		require_True(t, seq == 4)

		// Wait til messages expire.
		checkFor(t, time.Second, 250*time.Millisecond, func() error {
			state := fs.State()
			if state.Msgs == 0 {
				return nil
			}
			return fmt.Errorf("Still has %d msgs", state.Msgs)
		})

		// Restart
		restartFS()

		lt = time.Unix(0, ts).UTC()
		require_True(t, lt == fs.State().LastTime)

		// Now make sure we retain the true last seq.
		_, _, err = fs.StoreMsg("foo", nil, msg)
		require_NoError(t, err)
		seq, ts, err = fs.StoreMsg("foo", nil, msg)
		require_NoError(t, err)

		require_True(t, seq == 6)
		removed, err = fs.RemoveMsg(seq)
		require_NoError(t, err)
		require_True(t, removed)

		removed, err = fs.RemoveMsg(seq - 1)
		require_NoError(t, err)
		require_True(t, removed)

		// Restart
		restartFS()

		lt = time.Unix(0, ts).UTC()
		require_True(t, lt == fs.State().LastTime)
		require_True(t, seq == 6)
	})
}

func (fs *fileStore) getFirstBlock() *msgBlock {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if len(fs.blks) == 0 {
		return nil
	}
	return fs.blks[0]
}

func TestFileStoreRebuildStateDmapAccountingBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1024 * 1024

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 100; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil)
			require_NoError(t, err)
		}
		// Delete 2-40.
		for i := 2; i <= 40; i++ {
			_, err := fs.RemoveMsg(uint64(i))
			require_NoError(t, err)
		}

		mb := fs.getFirstBlock()
		require_True(t, mb != nil)

		check := func() {
			t.Helper()
			mb.mu.RLock()
			defer mb.mu.RUnlock()
			dmapLen := uint64(mb.dmap.Size())
			if mb.msgs != (mb.last.seq-mb.first.seq+1)-dmapLen {
				t.Fatalf("Consistency check failed: %d != %d -> last %d first %d len(dmap) %d",
					mb.msgs, (mb.last.seq-mb.first.seq+1)-dmapLen, mb.last.seq, mb.first.seq, dmapLen)
			}
		}

		check()

		mb.mu.Lock()
		mb.compact()
		mb.mu.Unlock()

		// Now delete first.
		_, err = fs.RemoveMsg(1)
		require_NoError(t, err)

		mb.mu.Lock()
		_, _, err = mb.rebuildStateLocked()
		mb.mu.Unlock()
		require_NoError(t, err)

		check()
	})
}

func TestFileStorePurgeExWithSubject(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1000
		cfg := StreamConfig{Name: "TEST", Subjects: []string{"foo.>"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		payload := make([]byte, 20)
		total := 200
		for i := 0; i < total; i++ {
			_, _, err = fs.StoreMsg("foo.1", nil, payload)
			require_NoError(t, err)
		}
		_, _, err = fs.StoreMsg("foo.2", nil, []byte("xxxxxx"))
		require_NoError(t, err)

		// This should purge all.
		p, err := fs.PurgeEx("foo.1", 1, 0)
		require_NoError(t, err)
		require_True(t, int(p) == total)
		require_True(t, int(p) == total)
		require_True(t, fs.State().Msgs == 1)
		require_True(t, fs.State().FirstSeq == 201)
	})
}

// When the N.idx file is shorter than the previous write we could fail to recover the idx properly.
// For instance, with encryption and an expiring stream that has no messages, when a restart happens the decrypt will fail
// since their are extra bytes, and this could lead to a stream sequence reset to zero.
//
// NOTE: We do not use idx files anymore, but keeping test.
func TestFileStoreShortIndexWriteBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		// Encrypted mode shows, but could effect non-encrypted mode.
		cfg := StreamConfig{Name: "TEST", Storage: FileStorage, MaxAge: time.Second}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 100; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil)
			require_NoError(t, err)
		}
		// Wait til messages all go away.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if state := fs.State(); state.Msgs != 0 {
				return fmt.Errorf("Expected no msgs, got %d", state.Msgs)
			}
			return nil
		})

		if state := fs.State(); state.FirstSeq != 101 {
			t.Fatalf("Expected first sequence of 101 vs %d", state.FirstSeq)
		}

		// Now restart..
		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); state.FirstSeq != 101 || state.LastSeq != 100 {
			t.Fatalf("Expected first sequence of 101 vs %d", state.FirstSeq)
		}
	})
}

func TestFileStoreDoubleCompactWithWriteInBetweenEncryptedBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("ouch")
		for i := 0; i < 5; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		_, err = fs.Compact(5)
		require_NoError(t, err)

		if state := fs.State(); state.LastSeq != 5 {
			t.Fatalf("Expected last sequence to be 5 but got %d", state.LastSeq)
		}
		for i := 0; i < 5; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		_, err = fs.Compact(10)
		require_NoError(t, err)

		if state := fs.State(); state.LastSeq != 10 {
			t.Fatalf("Expected last sequence to be 10 but got %d", state.LastSeq)
		}
	})
}

// When we kept the empty block for tracking sequence, we needed to reset the bek
// counter when encrypted for subsequent writes to be correct. The bek in place could
// possibly still have a non-zero counter from previous writes.
// Happens when all messages expire and the are flushed and then subsequent writes occur.
func TestFileStoreEncryptedKeepIndexNeedBekResetBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		ttl := 250 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Storage: FileStorage, MaxAge: ttl}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("ouch")
		for i := 0; i < 5; i++ {
			fs.StoreMsg(subj, nil, msg)
		}

		// Want to go to 0.
		// This will leave the marker.
		checkFor(t, time.Second, ttl, func() error {
			if state := fs.State(); state.Msgs != 0 {
				return fmt.Errorf("Expected no msgs, got %d", state.Msgs)
			}
			return nil
		})

		// Now write additional messages.
		for i := 0; i < 5; i++ {
			fs.StoreMsg(subj, nil, msg)
		}

		// Make sure the buffer is cleared.
		fs.mu.RLock()
		mb := fs.lmb
		fs.mu.RUnlock()
		mb.mu.Lock()
		mb.clearCacheAndOffset()
		mb.mu.Unlock()

		// Now make sure we can read.
		var smv StoreMsg
		_, err = fs.LoadMsg(10, &smv)
		require_NoError(t, err)
	})
}

func (fs *fileStore) reportMeta() (hasPSIM, hasAnyFSS bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	hasPSIM = fs.psim != nil
	for _, mb := range fs.blks {
		mb.mu.RLock()
		hasAnyFSS = hasAnyFSS || mb.fss != nil
		mb.mu.RUnlock()
		if hasAnyFSS {
			break
		}
	}
	return hasPSIM, hasAnyFSS
}

func TestFileStoreExpireSubjectMeta(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1024
		fcfg.CacheExpire = time.Second
		fcfg.SyncInterval = time.Second
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"kv.>"}, Storage: FileStorage, MaxMsgsPer: 1}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		ns := 100
		for i := 1; i <= ns; i++ {
			subj := fmt.Sprintf("kv.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"))
			require_NoError(t, err)
		}

		// Test that on restart we do not have extensize metadata but do have correct number of subjects/keys.
		// Only thing really needed for store state / stream info.
		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		var ss StreamState
		fs.FastState(&ss)
		if ss.NumSubjects != ns {
			t.Fatalf("Expected NumSubjects of %d, got %d", ns, ss.NumSubjects)
		}

		// Make sure we clear mb fss meta
		checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
			if _, hasAnyFSS := fs.reportMeta(); hasAnyFSS {
				return fmt.Errorf("Still have mb fss state")
			}
			return nil
		})

		// LoadLast, which is what KV uses, should load meta and succeed.
		_, err = fs.LoadLastMsg("kv.22", nil)
		require_NoError(t, err)
		// Make sure we clear mb fss meta
		checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
			if _, hasAnyFSS := fs.reportMeta(); hasAnyFSS {
				return fmt.Errorf("Still have mb fss state")
			}
			return nil
		})
	})
}

func TestFileStoreMaxMsgsPerSubject(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 128
		fcfg.CacheExpire = time.Second
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"kv.>"}, Storage: FileStorage, MaxMsgsPer: 1}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		ns := 100
		for i := 1; i <= ns; i++ {
			subj := fmt.Sprintf("kv.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"))
			require_NoError(t, err)
		}

		for i := 1; i <= ns; i++ {
			subj := fmt.Sprintf("kv.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"))
			require_NoError(t, err)
		}

		if state := fs.State(); state.Msgs != 100 || state.FirstSeq != 101 || state.LastSeq != 200 || len(state.Deleted) != 0 {
			t.Fatalf("Bad state: %+v", state)
		}

		if nb := fs.numMsgBlocks(); nb != 34 {
			t.Fatalf("Expected 34 blocks, got %d", nb)
		}
	})
}

// Testing the case in https://github.com/nats-io/nats-server/issues/4247
func TestFileStoreMaxMsgsAndMaxMsgsPerSubject(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 128
		fcfg.CacheExpire = time.Second
		cfg := StreamConfig{
			Name:     "zzz",
			Subjects: []string{"kv.>"},
			Storage:  FileStorage,
			Discard:  DiscardNew, MaxMsgs: 100, // Total stream policy
			DiscardNewPer: true, MaxMsgsPer: 1, // Per-subject policy
		}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 1; i <= 101; i++ {
			subj := fmt.Sprintf("kv.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"))
			if i == 101 {
				// The 101th iteration should fail because MaxMsgs is set to
				// 100 and the policy is DiscardNew.
				require_Error(t, err)
			} else {
				require_NoError(t, err)
			}
		}

		for i := 1; i <= 100; i++ {
			subj := fmt.Sprintf("kv.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"))
			// All of these iterations should fail because MaxMsgsPer is set
			// to 1 and DiscardNewPer is set to true, forcing us to reject
			// cases where there is already a message on this subject.
			require_Error(t, err)
		}

		if state := fs.State(); state.Msgs != 100 || state.FirstSeq != 1 || state.LastSeq != 100 || len(state.Deleted) != 0 {
			// There should be 100 messages exactly, as the 101st subject
			// should have been rejected in the first loop, and any duplicates
			// on the other subjects should have been rejected in the second loop.
			t.Fatalf("Bad state: %+v", state)
		}
	})
}

func TestFileStoreSubjectStateCacheExpiration(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 32
		fcfg.CacheExpire = time.Second
		fcfg.SyncInterval = time.Second
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"kv.>"}, Storage: FileStorage, MaxMsgsPer: 2}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 1; i <= 100; i++ {
			subj := fmt.Sprintf("kv.foo.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"))
			require_NoError(t, err)
		}
		for i := 1; i <= 100; i++ {
			subj := fmt.Sprintf("kv.bar.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"))
			require_NoError(t, err)
		}

		// Make sure we clear mb fss meta before asking for SubjectState.
		checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
			if _, hasAnyFSS := fs.reportMeta(); hasAnyFSS {
				return fmt.Errorf("Still have mb fss state")
			}
			return nil
		})

		if fss := fs.SubjectsState("kv.bar.>"); len(fss) != 100 {
			t.Fatalf("Expected 100 entries but got %d", len(fss))
		}

		fss := fs.SubjectsState("kv.bar.99")
		if len(fss) != 1 {
			t.Fatalf("Expected 1 entry but got %d", len(fss))
		}
		expected := SimpleState{Msgs: 1, First: 199, Last: 199}
		if ss := fss["kv.bar.99"]; ss != expected {
			t.Fatalf("Bad subject state, expected %+v but got %+v", expected, ss)
		}

		// Now add one to end and check as well for non-wildcard.
		_, _, err = fs.StoreMsg("kv.foo.1", nil, []byte("value22"))
		require_NoError(t, err)

		if state := fs.State(); state.Msgs != 201 {
			t.Fatalf("Expected 201 msgs but got %+v", state)
		}

		fss = fs.SubjectsState("kv.foo.1")
		if len(fss) != 1 {
			t.Fatalf("Expected 1 entry but got %d", len(fss))
		}
		expected = SimpleState{Msgs: 2, First: 1, Last: 201}
		if ss := fss["kv.foo.1"]; ss != expected {
			t.Fatalf("Bad subject state, expected %+v but got %+v", expected, ss)
		}
	})
}

func TestFileStoreEncrypted(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("aes ftw")
		for i := 0; i < 50; i++ {
			fs.StoreMsg(subj, nil, msg)
		}

		o, err := fs.ConsumerStore("o22", &ConsumerConfig{})
		require_NoError(t, err)

		state := &ConsumerState{}
		state.Delivered.Consumer = 22
		state.Delivered.Stream = 22
		state.AckFloor.Consumer = 11
		state.AckFloor.Stream = 11
		err = o.Update(state)
		require_NoError(t, err)

		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Now make sure we can read.
		var smv StoreMsg
		sm, err := fs.LoadMsg(10, &smv)
		require_NoError(t, err)
		require_True(t, string(sm.msg) == "aes ftw")

		o, err = fs.ConsumerStore("o22", &ConsumerConfig{})
		require_NoError(t, err)
		rstate, err := o.State()
		require_NoError(t, err)

		if rstate.Delivered != state.Delivered || rstate.AckFloor != state.AckFloor {
			t.Fatalf("Bad recovered consumer state, expected %+v got %+v", state, rstate)
		}
	})
}

// Make sure we do not go through block loads when we know no subjects will exists, e.g. raft.
func TestFileStoreNoFSSWhenNoSubjects(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		n, msg := 100, []byte("raft state")
		for i := 0; i < n; i++ {
			_, _, err := fs.StoreMsg(_EMPTY_, nil, msg)
			require_NoError(t, err)
		}

		state := fs.State()
		require_True(t, state.Msgs == uint64(n))

		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure we did not load the block trying to generate fss.
		fs.mu.RLock()
		mb := fs.blks[0]
		fs.mu.RUnlock()

		mb.mu.Lock()
		defer mb.mu.Unlock()

		if mb.cloads > 0 {
			t.Fatalf("Expected no cache loads but got %d", mb.cloads)
		}
		if mb.fss != nil {
			t.Fatalf("Expected fss to be nil")
		}
	})
}

func TestFileStoreNoFSSBugAfterRemoveFirst(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8 * 1024 * 1024
		fcfg.CacheExpire = 200 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.bar.*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		n, msg := 100, bytes.Repeat([]byte("ZZZ"), 33) // ~100bytes
		for i := 0; i < n; i++ {
			subj := fmt.Sprintf("foo.bar.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, msg)
			require_NoError(t, err)
		}

		state := fs.State()
		require_True(t, state.Msgs == uint64(n))

		// Let fss expire.
		time.Sleep(250 * time.Millisecond)

		_, err = fs.RemoveMsg(1)
		require_NoError(t, err)

		sm, _, err := fs.LoadNextMsg("foo.>", true, 1, nil)
		require_NoError(t, err)
		require_True(t, sm.subj == "foo.bar.1")

		// Make sure mb.fss does not have the entry for foo.bar.0
		fs.mu.Lock()
		mb := fs.blks[0]
		fs.mu.Unlock()
		mb.mu.RLock()
		ss := mb.fss["foo.bar.0"]
		mb.mu.RUnlock()

		if ss != nil {
			t.Fatalf("Expected no state for %q, but got %+v\n", "foo.bar.0", ss)
		}
	})
}

// NOTE: We do not use fss files anymore, but leaving test in place.
func TestFileStoreNoFSSAfterRecover(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		n, msg := 100, []byte("no fss for you!")
		for i := 0; i < n; i++ {
			_, _, err := fs.StoreMsg(_EMPTY_, nil, msg)
			require_NoError(t, err)
		}

		state := fs.State()
		require_True(t, state.Msgs == uint64(n))

		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure we did not load the block trying to generate fss.
		fs.mu.RLock()
		mb := fs.blks[0]
		fs.mu.RUnlock()

		mb.mu.Lock()
		defer mb.mu.Unlock()

		if mb.fss != nil {
			t.Fatalf("Expected no fss post recover")
		}
	})
}

func TestFileStoreFSSCloseAndKeepOnExpireOnRecoverBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		ttl := 100 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage, MaxAge: ttl}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo", nil, nil)
		require_NoError(t, err)

		fs.Stop()
		time.Sleep(2 * ttl)
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); state.NumSubjects != 0 {
			t.Fatalf("Expected no subjects with no messages, got %d", state.NumSubjects)
		}
	})
}

func TestFileStoreExpireOnRecoverSubjectAccounting(t *testing.T) {
	const msgLen = 19
	msg := bytes.Repeat([]byte("A"), msgLen)

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 100
		ttl := 200 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxAge: ttl}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// These are in first block.
		fs.StoreMsg("A", nil, msg)
		fs.StoreMsg("B", nil, msg)
		time.Sleep(ttl / 2)
		// This one in 2nd block.
		fs.StoreMsg("C", nil, msg)

		fs.Stop()
		time.Sleep(ttl/2 + 10*time.Millisecond)
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure we take into account PSIM when throwing a whole block away.
		if state := fs.State(); state.NumSubjects != 1 {
			t.Fatalf("Expected 1 subject, got %d", state.NumSubjects)
		}
	})
}

func TestFileStoreFSSExpireNumPendingBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cexp := 100 * time.Millisecond
		fcfg.CacheExpire = cexp
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"KV.>"}, MaxMsgsPer: 1, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Let FSS meta expire.
		time.Sleep(2 * cexp)

		_, _, err = fs.StoreMsg("KV.X", nil, []byte("Y"))
		require_NoError(t, err)

		if fss := fs.FilteredState(1, "KV.X"); fss.Msgs != 1 {
			t.Fatalf("Expected only 1 msg, got %d", fss.Msgs)
		}
	})
}

// https://github.com/nats-io/nats-server/issues/3484
func TestFileStoreFilteredFirstMatchingBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.>"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo.foo", nil, []byte("A"))
		require_NoError(t, err)

		_, _, err = fs.StoreMsg("foo.foo", nil, []byte("B"))
		require_NoError(t, err)

		_, _, err = fs.StoreMsg("foo.foo", nil, []byte("C"))
		require_NoError(t, err)

		fs.mu.RLock()
		mb := fs.lmb
		fs.mu.RUnlock()

		mb.mu.Lock()
		// Simulate swapping out the fss state and reading it back in with only one subject
		// present in the block.
		if mb.fss != nil {
			mb.fss = nil
		}
		// Now load info back in.
		mb.generatePerSubjectInfo()
		mb.mu.Unlock()

		// Now add in a different subject.
		_, _, err = fs.StoreMsg("foo.bar", nil, []byte("X"))
		require_NoError(t, err)

		// Now see if a filtered load would incorrectly succeed.
		sm, _, err := fs.LoadNextMsg("foo.foo", false, 4, nil)
		if err == nil || sm != nil {
			t.Fatalf("Loaded filtered message with wrong subject, wanted %q got %q", "foo.foo", sm.subj)
		}
	})
}

func TestFileStoreOutOfSpaceRebuildState(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo", nil, []byte("A"))
		require_NoError(t, err)

		_, _, err = fs.StoreMsg("bar", nil, []byte("B"))
		require_NoError(t, err)

		// Grab state.
		state := fs.State()
		ss := fs.SubjectsState(">")

		// Set mock out of space error to trip.
		fs.mu.RLock()
		mb := fs.lmb
		fs.mu.RUnlock()

		mb.mu.Lock()
		mb.mockWriteErr = true
		mb.mu.Unlock()

		_, _, err = fs.StoreMsg("baz", nil, []byte("C"))
		require_Error(t, err, errors.New("mock write error"))

		nstate := fs.State()
		nss := fs.SubjectsState(">")

		if !reflect.DeepEqual(state, nstate) {
			t.Fatalf("State expected to be\n  %+v\nvs\n  %+v", state, nstate)
		}

		if !reflect.DeepEqual(ss, nss) {
			t.Fatalf("Subject state expected to be\n  %+v\nvs\n  %+v", ss, nss)
		}
	})
}

func TestFileStoreRebuildStateProperlyWithMaxMsgsPerSubject(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 4096
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo", "bar", "baz"}, Storage: FileStorage, MaxMsgsPer: 1}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Send one to baz at beginning.
		_, _, err = fs.StoreMsg("baz", nil, nil)
		require_NoError(t, err)

		ns := 1000
		for i := 1; i <= ns; i++ {
			_, _, err := fs.StoreMsg("foo", nil, nil)
			require_NoError(t, err)
			_, _, err = fs.StoreMsg("bar", nil, nil)
			require_NoError(t, err)
		}

		var ss StreamState
		fs.FastState(&ss)
		if ss.NumSubjects != 3 {
			t.Fatalf("Expected NumSubjects of 3, got %d", ss.NumSubjects)
		}
		if ss.Msgs != 3 {
			t.Fatalf("Expected NumMsgs of 3, got %d", ss.Msgs)
		}
	})
}

func TestFileStoreUpdateMaxMsgsPerSubject(t *testing.T) {
	cfg := StreamConfig{
		Name:       "TEST",
		Storage:    FileStorage,
		Subjects:   []string{"foo"},
		MaxMsgsPer: 10,
	}

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure this is honored on an update.
		cfg.MaxMsgsPer = 50
		err = fs.UpdateConfig(&cfg)
		require_NoError(t, err)

		numStored := 22
		for i := 0; i < numStored; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil)
			require_NoError(t, err)
		}

		ss := fs.SubjectsState("foo")["foo"]
		if ss.Msgs != uint64(numStored) {
			t.Fatalf("Expected to have %d stored, got %d", numStored, ss.Msgs)
		}

		// Now make sure we trunk if setting to lower value.
		cfg.MaxMsgsPer = 10
		err = fs.UpdateConfig(&cfg)
		require_NoError(t, err)

		ss = fs.SubjectsState("foo")["foo"]
		if ss.Msgs != 10 {
			t.Fatalf("Expected to have %d stored, got %d", 10, ss.Msgs)
		}
	})
}

func TestFileStoreBadFirstAndFailedExpireAfterRestart(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 256
		ttl := time.Second
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage, MaxAge: ttl}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// With block size of 256 and subject and message below, seq 8 starts new block.
		// Will double check and fail test if not the case since test depends on this.
		subj, msg := "foo", []byte("ZZ")
		// These are all instant and will expire after 1 sec.
		start := time.Now()
		for i := 0; i < 7; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg)
			require_NoError(t, err)
		}

		// Put two more after a delay.
		time.Sleep(500 * time.Millisecond)
		seq, _, err := fs.StoreMsg(subj, nil, msg)
		require_NoError(t, err)
		_, _, err = fs.StoreMsg(subj, nil, msg)
		require_NoError(t, err)

		// Make sure that sequence 8 is first in second block, and break test if that is not true.
		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		lmb.mu.RLock()
		first := lmb.first.seq
		lmb.mu.RUnlock()
		require_True(t, first == 8)

		// Instantly remove first one from second block.
		// On restart this will trigger expire on recover which will set fs.FirstSeq to the deleted one.
		fs.RemoveMsg(seq)

		// Stop the filstore and wait til first block expires.
		fs.Stop()
		time.Sleep(ttl - time.Since(start) + (10 * time.Millisecond))
		fs, err = newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Check that state is correct for first message which should be 9 and have a proper timestamp.
		var state StreamState
		fs.FastState(&state)
		ts := state.FirstTime
		require_True(t, state.Msgs == 1)
		require_True(t, state.FirstSeq == 9)
		require_True(t, !state.FirstTime.IsZero())

		// Wait and make sure expire timer is still working properly.
		time.Sleep(ttl)
		fs.FastState(&state)
		require_Equal(t, state.Msgs, 0)
		require_Equal(t, state.FirstSeq, 10)
		require_Equal(t, state.LastSeq, 9)
		require_Equal(t, state.LastTime, ts)
	})
}

func TestFileStoreCompactAllWithDanglingLMB(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("ZZ")
		for i := 0; i < 100; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg)
			require_NoError(t, err)
		}

		fs.RemoveMsg(100)
		purged, err := fs.Compact(100)
		require_NoError(t, err)
		require_True(t, purged == 99)

		_, _, err = fs.StoreMsg(subj, nil, msg)
		require_NoError(t, err)
	})
}

func TestFileStoreStateWithBlkFirstDeleted(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 4096
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 500
		for i := 0; i < toStore; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg)
			require_NoError(t, err)
		}

		// Delete some messages from the beginning of an interior block.
		fs.mu.RLock()
		require_True(t, len(fs.blks) > 2)
		fseq := fs.blks[1].first.seq
		fs.mu.RUnlock()

		// Now start from first seq of second blk and delete 10 msgs
		for seq := fseq; seq < fseq+10; seq++ {
			removed, err := fs.RemoveMsg(seq)
			require_NoError(t, err)
			require_True(t, removed)
		}

		// This bug was in normal detailed state. But check fast state too.
		var fstate StreamState
		fs.FastState(&fstate)
		require_True(t, fstate.NumDeleted == 10)
		state := fs.State()
		require_True(t, state.NumDeleted == 10)
	})
}

func TestFileStoreMsgBlkFailOnKernelFaultLostDataReporting(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 4096
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 500
		for i := 0; i < toStore; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg)
			require_NoError(t, err)
		}

		// We want to make sure all of the scenarios report lost data properly.
		// Will run 3 scenarios, 1st block, last block, interior block.
		// The new system does not detect byzantine behavior by default on creating the store.
		// A LoadMsg() of checkMsgs() call will be needed now.

		// First block
		fs.mu.RLock()
		require_True(t, len(fs.blks) > 0)
		mfn := fs.blks[0].mfn
		fs.mu.RUnlock()

		fs.Stop()

		require_NoError(t, os.Remove(mfn))

		// Restart.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, err = fs.LoadMsg(1, nil)
		require_Error(t, err, errNoBlkData)

		// Load will rebuild fs itself async..
		checkFor(t, time.Second, 50*time.Millisecond, func() error {
			if state := fs.State(); state.Lost != nil {
				return nil
			}
			return errors.New("no ld yet")
		})

		state := fs.State()
		require_True(t, state.FirstSeq == 94)
		require_True(t, state.Lost != nil)
		require_True(t, len(state.Lost.Msgs) == 93)

		// Last block
		fs.mu.RLock()
		require_True(t, len(fs.blks) > 0)
		require_True(t, fs.lmb != nil)
		mfn = fs.lmb.mfn
		fs.mu.RUnlock()

		fs.Stop()

		require_NoError(t, os.Remove(mfn))

		// Restart.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		require_True(t, state.FirstSeq == 94)
		require_True(t, state.LastSeq == 500)   // Make sure we do not lose last seq.
		require_True(t, state.NumDeleted == 35) // These are interiors
		require_True(t, state.Lost != nil)
		require_True(t, len(state.Lost.Msgs) == 35)

		// Interior block.
		fs.mu.RLock()
		require_True(t, len(fs.blks) > 3)
		mfn = fs.blks[len(fs.blks)-3].mfn
		fs.mu.RUnlock()

		fs.Stop()

		require_NoError(t, os.Remove(mfn))

		// Restart.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Need checkMsgs to catch interior one.
		require_True(t, fs.checkMsgs() != nil)

		state = fs.State()
		require_True(t, state.FirstSeq == 94)
		require_True(t, state.LastSeq == 500) // Make sure we do not lose last seq.
		require_True(t, state.NumDeleted == 128)
		require_True(t, state.Lost != nil)
		require_True(t, len(state.Lost.Msgs) == 93)
	})
}

func TestFileStoreAllFilteredStateWithDeleted(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1024
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 100; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg)
			require_NoError(t, err)
		}

		remove := func(seqs ...uint64) {
			for _, seq := range seqs {
				ok, err := fs.RemoveMsg(seq)
				require_NoError(t, err)
				require_True(t, ok)
			}
		}

		checkFilteredState := func(start, msgs, first, last int) {
			fss := fs.FilteredState(uint64(start), _EMPTY_)
			if fss.Msgs != uint64(msgs) {
				t.Fatalf("Expected %d msgs, got %d", msgs, fss.Msgs)
			}
			if fss.First != uint64(first) {
				t.Fatalf("Expected %d to be first, got %d", first, fss.First)
			}
			if fss.Last != uint64(last) {
				t.Fatalf("Expected %d to be last, got %d", last, fss.Last)
			}
		}

		checkFilteredState(1, 100, 1, 100)
		remove(2)
		checkFilteredState(2, 98, 3, 100)
		remove(3, 4, 5)
		checkFilteredState(2, 95, 6, 100)
		checkFilteredState(6, 95, 6, 100)
		remove(8, 10, 12, 14, 16, 18)
		checkFilteredState(7, 88, 7, 100)
	})
}

func TestFileStoreStreamTruncateResetMultiBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 128
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 1000; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg)
			require_NoError(t, err)
		}
		fs.syncBlocks()
		require_True(t, fs.numMsgBlocks() == 500)

		// Reset everything
		require_NoError(t, fs.Truncate(0))
		require_True(t, fs.numMsgBlocks() == 0)

		state := fs.State()
		require_Equal(t, state.Msgs, 0)
		require_Equal(t, state.Bytes, 0)
		require_Equal(t, state.FirstSeq, 0)
		require_Equal(t, state.LastSeq, 0)
		require_Equal(t, state.NumSubjects, 0)
		require_Equal(t, state.NumDeleted, 0)

		for i := 0; i < 1000; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg)
			require_NoError(t, err)
		}
		fs.syncBlocks()

		state = fs.State()
		require_Equal(t, state.Msgs, 1000)
		require_Equal(t, state.Bytes, 44000)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 1000)
		require_Equal(t, state.NumSubjects, 1)
		require_Equal(t, state.NumDeleted, 0)
	})
}

func TestFileStoreStreamCompactMultiBlockSubjectInfo(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 128
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 1000; i++ {
			subj := fmt.Sprintf("foo.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("Hello World"))
			require_NoError(t, err)
		}
		require_True(t, fs.numMsgBlocks() == 500)

		// Compact such that we know we throw blocks away from the beginning.
		deleted, err := fs.Compact(501)
		require_NoError(t, err)
		require_True(t, deleted == 500)
		require_True(t, fs.numMsgBlocks() == 250)

		// Make sure we adjusted for subjects etc.
		state := fs.State()
		require_True(t, state.NumSubjects == 500)
	})
}

func TestFileStoreSubjectsTotals(t *testing.T) {
	// No need for all permutations here.
	storeDir := t.TempDir()
	fcfg := FileStoreConfig{StoreDir: storeDir}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fmap := make(map[int]int)
	bmap := make(map[int]int)

	var m map[int]int
	var ft string

	for i := 0; i < 10_000; i++ {
		// Flip coin for prefix
		if rand.Intn(2) == 0 {
			ft, m = "foo", fmap
		} else {
			ft, m = "bar", bmap
		}
		dt := rand.Intn(100)
		subj := fmt.Sprintf("%s.%d", ft, dt)
		m[dt]++

		_, _, err := fs.StoreMsg(subj, nil, []byte("Hello World"))
		require_NoError(t, err)
	}

	// Now test SubjectsTotal
	for dt, total := range fmap {
		subj := fmt.Sprintf("foo.%d", dt)
		m := fs.SubjectsTotals(subj)
		if m[subj] != uint64(total) {
			t.Fatalf("Expected %q to have %d total, got %d", subj, total, m[subj])
		}
	}

	// Check fmap.
	if st := fs.SubjectsTotals("foo.*"); len(st) != len(fmap) {
		t.Fatalf("Expected %d subjects for %q, got %d", len(fmap), "foo.*", len(st))
	} else {
		expected := 0
		for _, n := range fmap {
			expected += n
		}
		received := uint64(0)
		for _, n := range st {
			received += n
		}
		if received != uint64(expected) {
			t.Fatalf("Expected %d total but got %d", expected, received)
		}
	}

	// Check bmap.
	if st := fs.SubjectsTotals("bar.*"); len(st) != len(bmap) {
		t.Fatalf("Expected %d subjects for %q, got %d", len(bmap), "bar.*", len(st))
	} else {
		expected := 0
		for _, n := range bmap {
			expected += n
		}
		received := uint64(0)
		for _, n := range st {
			received += n
		}
		if received != uint64(expected) {
			t.Fatalf("Expected %d total but got %d", expected, received)
		}
	}

	// All with pwc match.
	if st, expected := fs.SubjectsTotals("*.*"), len(bmap)+len(fmap); len(st) != expected {
		t.Fatalf("Expected %d subjects for %q, got %d", expected, "*.*", len(st))
	}
}

func TestFileStoreConsumerStoreEncodeAfterRestart(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		o, err := fs.ConsumerStore("o22", &ConsumerConfig{AckPolicy: AckExplicit})
		require_NoError(t, err)

		state := &ConsumerState{}
		state.Delivered.Consumer = 22
		state.Delivered.Stream = 22
		state.AckFloor.Consumer = 11
		state.AckFloor.Stream = 11
		err = o.Update(state)
		require_NoError(t, err)

		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		o, err = fs.ConsumerStore("o22", &ConsumerConfig{AckPolicy: AckExplicit})
		require_NoError(t, err)

		if o.(*consumerFileStore).state.Delivered != state.Delivered {
			t.Fatalf("Consumer state is wrong %+v vs %+v", o.(*consumerFileStore).state, state)
		}
		if o.(*consumerFileStore).state.AckFloor != state.AckFloor {
			t.Fatalf("Consumer state is wrong %+v vs %+v", o.(*consumerFileStore).state, state)
		}
	})
}

func TestFileStoreNumPendingLargeNumBlks(t *testing.T) {
	// No need for all permutations here.
	storeDir := t.TempDir()
	fcfg := FileStoreConfig{
		StoreDir:  storeDir,
		BlockSize: 128, // Small on purpose to create alot of blks.
	}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"zzz"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "zzz", bytes.Repeat([]byte("X"), 100)
	numMsgs := 10_000

	for i := 0; i < numMsgs; i++ {
		fs.StoreMsg(subj, nil, msg)
	}

	start := time.Now()
	total, _ := fs.NumPending(4000, "zzz", false)
	require_LessThan(t, time.Since(start), 15*time.Millisecond)
	require_Equal(t, total, 6001)

	start = time.Now()
	total, _ = fs.NumPending(6000, "zzz", false)
	require_LessThan(t, time.Since(start), 25*time.Millisecond)
	require_Equal(t, total, 4001)

	// Now delete a message in first half and second half.
	fs.RemoveMsg(1000)
	fs.RemoveMsg(9000)

	start = time.Now()
	total, _ = fs.NumPending(4000, "zzz", false)
	require_LessThan(t, time.Since(start), 50*time.Millisecond)
	require_Equal(t, total, 6000)

	start = time.Now()
	total, _ = fs.NumPending(6000, "zzz", false)
	require_LessThan(t, time.Since(start), 50*time.Millisecond)
	require_Equal(t, total, 4000)
}

func TestFileStoreSkipMsgAndNumBlocks(t *testing.T) {
	// No need for all permutations here.
	storeDir := t.TempDir()
	fcfg := FileStoreConfig{
		StoreDir:  storeDir,
		BlockSize: 128, // Small on purpose to create alot of blks.
	}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"zzz"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "zzz", bytes.Repeat([]byte("X"), 100)
	numMsgs := 10_000

	fs.StoreMsg(subj, nil, msg)
	for i := 0; i < numMsgs; i++ {
		fs.SkipMsg()
	}
	fs.StoreMsg(subj, nil, msg)
	require_True(t, fs.numMsgBlocks() == 2)
}

func TestFileStoreRestoreEncryptedWithNoKeyFuncFails(t *testing.T) {
	// No need for all permutations here.
	fcfg := FileStoreConfig{StoreDir: t.TempDir(), Cipher: AES}
	cfg := StreamConfig{Name: "zzz", Subjects: []string{"zzz"}, Storage: FileStorage}
	fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "zzz", bytes.Repeat([]byte("X"), 100)
	numMsgs := 100
	for i := 0; i < numMsgs; i++ {
		fs.StoreMsg(subj, nil, msg)
	}

	fs.Stop()

	// Make sure if we try to restore with no prf (key) that it fails.
	_, err = newFileStoreWithCreated(fcfg, cfg, time.Now(), nil, nil)
	require_Error(t, err, errNoMainKey)
}

func TestFileStoreInitialFirstSeq(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Storage: FileStorage, FirstSeq: 1000}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		seq, _, err := fs.StoreMsg("A", nil, []byte("OK"))
		require_NoError(t, err)
		if seq != 1000 {
			t.Fatalf("Message should have been sequence 1000 but was %d", seq)
		}

		seq, _, err = fs.StoreMsg("B", nil, []byte("OK"))
		require_NoError(t, err)
		if seq != 1001 {
			t.Fatalf("Message should have been sequence 1001 but was %d", seq)
		}

		var state StreamState
		fs.FastState(&state)
		switch {
		case state.Msgs != 2:
			t.Fatalf("Expected 2 messages, got %d", state.Msgs)
		case state.FirstSeq != 1000:
			t.Fatalf("Expected first seq 1000, got %d", state.FirstSeq)
		case state.LastSeq != 1001:
			t.Fatalf("Expected last seq 1001, got %d", state.LastSeq)
		}
	})
}

func TestFileStoreRecaluclateFirstForSubjBug(t *testing.T) {
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("foo", nil, nil) // 1
	fs.StoreMsg("bar", nil, nil) // 2
	fs.StoreMsg("foo", nil, nil) // 3

	// Now remove first 2..
	fs.RemoveMsg(1)
	fs.RemoveMsg(2)

	// Now grab first (and only) block.
	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()

	// Since we lazy update the first, simulate that we have not updated it as of yet.
	ss := &SimpleState{Msgs: 1, First: 1, Last: 3, firstNeedsUpdate: true}

	mb.mu.Lock()
	defer mb.mu.Unlock()

	// Flush the cache.
	mb.clearCacheAndOffset()
	// Now call with start sequence of 1, the old one
	// This will panic without the fix.
	mb.recalculateFirstForSubj("foo", 1, ss)
	// Make sure it was update properly.
	require_True(t, *ss == SimpleState{Msgs: 1, First: 3, Last: 3, firstNeedsUpdate: false})
}

func TestFileStoreKeepWithDeletedMsgsBug(t *testing.T) {
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := bytes.Repeat([]byte("A"), 19)
	for i := 0; i < 5; i++ {
		fs.StoreMsg("A", nil, msg)
		fs.StoreMsg("B", nil, msg)
	}

	n, err := fs.PurgeEx("A", 0, 0)
	require_NoError(t, err)
	require_True(t, n == 5)

	// Purge with keep.
	n, err = fs.PurgeEx(_EMPTY_, 0, 2)
	require_NoError(t, err)
	require_True(t, n == 3)
}

func TestFileStoreRestartWithExpireAndLockingBug(t *testing.T) {
	sd := t.TempDir()
	scfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
	fs, err := newFileStore(FileStoreConfig{StoreDir: sd}, scfg)
	require_NoError(t, err)
	defer fs.Stop()

	// 20 total
	msg := []byte("HELLO WORLD")
	for i := 0; i < 10; i++ {
		fs.StoreMsg("A", nil, msg)
		fs.StoreMsg("B", nil, msg)
	}
	fs.Stop()

	// Now change config underneath of so we will do expires at startup.
	scfg.MaxMsgs = 15
	scfg.MaxMsgsPer = 2
	newCfg := FileStreamInfo{Created: fs.cfg.Created, StreamConfig: scfg}

	// Replace
	fs.cfg = newCfg
	require_NoError(t, fs.writeStreamMeta())

	fs, err = newFileStore(FileStoreConfig{StoreDir: sd}, scfg)
	require_NoError(t, err)
	defer fs.Stop()
}

// Test that loads from lmb under lots of writes do not return errPartialCache.
func TestFileStoreErrPartialLoad(t *testing.T) {
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	put := func(num int) {
		for i := 0; i < num; i++ {
			fs.StoreMsg("Z", nil, []byte("ZZZZZZZZZZZZZ"))
		}
	}

	put(100)

	// Dump cache of lmb.
	clearCache := func() {
		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		lmb.mu.Lock()
		lmb.clearCache()
		lmb.mu.Unlock()
	}
	clearCache()

	qch := make(chan struct{})
	defer close(qch)

	for i := 0; i < 10; i++ {
		go func() {
			for {
				select {
				case <-qch:
					return
				default:
					put(5)
				}
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)

	var smv StoreMsg
	for i := 0; i < 10_000; i++ {
		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		lmb.mu.Lock()
		first, last := fs.lmb.first.seq, fs.lmb.last.seq
		if i%100 == 0 {
			lmb.clearCache()
		}
		lmb.mu.Unlock()

		if spread := int(last - first); spread > 0 {
			seq := first + uint64(rand.Intn(spread))
			_, err = fs.LoadMsg(seq, &smv)
			require_NoError(t, err)
		}
	}
}

func TestFileStoreErrPartialLoadOnSyncClose(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 500},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// This yields an internal record length of 50 bytes. So 10 msgs per blk.
	msgLen := 19
	msg := bytes.Repeat([]byte("A"), msgLen)

	// Load up half the block.
	for _, subj := range []string{"A", "B", "C", "D", "E"} {
		fs.StoreMsg(subj, nil, msg)
	}

	// Now simulate the sync timer closing the last block.
	fs.mu.RLock()
	lmb := fs.lmb
	fs.mu.RUnlock()
	require_True(t, lmb != nil)

	lmb.mu.Lock()
	lmb.expireCacheLocked()
	lmb.dirtyCloseWithRemove(false)
	lmb.mu.Unlock()

	fs.StoreMsg("Z", nil, msg)
	_, err = fs.LoadMsg(1, nil)
	require_NoError(t, err)
}

func TestFileStoreSyncIntervals(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir(), SyncInterval: 250 * time.Millisecond}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	checkSyncFlag := func(expected bool) {
		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		lmb.mu.RLock()
		syncNeeded := lmb.needSync
		lmb.mu.RUnlock()
		if syncNeeded != expected {
			t.Fatalf("Expected needSync to be %v", expected)
		}
	}

	checkSyncFlag(false)
	fs.StoreMsg("Z", nil, []byte("hello"))
	checkSyncFlag(true)
	time.Sleep(400 * time.Millisecond)
	checkSyncFlag(false)
	fs.Stop()

	// Now check always
	fcfg.SyncInterval = 10 * time.Second
	fcfg.SyncAlways = true
	fs, err = newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	checkSyncFlag(false)
	fs.StoreMsg("Z", nil, []byte("hello"))
	checkSyncFlag(false)
}

// https://github.com/nats-io/nats-server/issues/4529
// Run this wuth --race and you will see the unlocked access that probably caused this.
func TestFileStoreRecalcFirstSequenceBug(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"*"}, MaxMsgsPer: 2, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := bytes.Repeat([]byte("A"), 22)

	for _, subj := range []string{"A", "A", "B", "B"} {
		fs.StoreMsg(subj, nil, msg)
	}
	// Make sure the buffer is cleared.
	clearLMBCache := func() {
		fs.mu.RLock()
		mb := fs.lmb
		fs.mu.RUnlock()
		mb.mu.Lock()
		mb.clearCacheAndOffset()
		mb.mu.Unlock()
	}

	clearLMBCache()

	// Do first here.
	fs.StoreMsg("A", nil, msg)

	var wg sync.WaitGroup
	start := make(chan bool)

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1_000; i++ {
			fs.LoadLastMsg("A", nil)
			clearLMBCache()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1_000; i++ {
			fs.StoreMsg("A", nil, msg)
		}
	}()

	close(start)
	wg.Wait()
}

///////////////////////////////////////////////////////////////////////////
// New WAL based architecture tests
///////////////////////////////////////////////////////////////////////////

func TestFileStoreFullStateBasics(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 100
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		subj, msgLen, recLen := "A", 19, uint64(50)
		msgA := bytes.Repeat([]byte("A"), msgLen)
		msgZ := bytes.Repeat([]byte("Z"), msgLen)

		// Send 2 msgs and stop, check for presence of our full state file.
		fs.StoreMsg(subj, nil, msgA)
		fs.StoreMsg(subj, nil, msgZ)
		require_True(t, fs.numMsgBlocks() == 1)

		// Make sure there is a full state file after we do a stop.
		fs.Stop()

		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		if _, err := os.Stat(sfile); err != nil {
			t.Fatalf("Expected stream state file but got %v", err)
		}

		// Read it in and make sure len > 0.
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)
		require_True(t, len(buf) > 0)

		// Now make sure we recover properly.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure there are no old idx or fss files.
		matches, err := filepath.Glob(filepath.Join(fcfg.StoreDir, msgDir, "%d.fss"))
		require_NoError(t, err)
		require_True(t, len(matches) == 0)
		matches, err = filepath.Glob(filepath.Join(fcfg.StoreDir, msgDir, "%d.idx"))
		require_NoError(t, err)
		require_True(t, len(matches) == 0)

		state := fs.State()
		require_Equal(t, state.Msgs, 2)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 2)

		// Now make sure we can read in values.
		var smv StoreMsg
		sm, err := fs.LoadMsg(1, &smv)
		require_NoError(t, err)
		require_True(t, bytes.Equal(sm.msg, msgA))

		sm, err = fs.LoadMsg(2, &smv)
		require_NoError(t, err)
		require_True(t, bytes.Equal(sm.msg, msgZ))

		// Now add in 1 more here to split the lmb.
		fs.StoreMsg(subj, nil, msgZ)

		// Now stop the filestore and replace the old stream state and make sure we recover correctly.
		fs.Stop()

		// Regrab the stream state
		buf, err = os.ReadFile(sfile)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Add in one more.
		fs.StoreMsg(subj, nil, msgZ)
		fs.Stop()

		// Put old stream state back with only 3.
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		require_Equal(t, state.Msgs, 4)
		require_Equal(t, state.Bytes, 4*recLen)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 4)
		require_Equal(t, fs.numMsgBlocks(), 2)

		// Make sure we are tracking subjects correctly.
		fs.mu.RLock()
		info, _ := fs.psim.Find(stringToBytes(subj))
		psi := *info
		fs.mu.RUnlock()

		require_Equal(t, psi.total, 4)
		require_Equal(t, psi.fblk, 1)
		require_Equal(t, psi.lblk, 2)

		// Store 1 more
		fs.StoreMsg(subj, nil, msgA)
		fs.Stop()
		// Put old stream state back with only 3.
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		require_Equal(t, state.Msgs, 5)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 5)
		require_Equal(t, fs.numMsgBlocks(), 3)
		// Make sure we are tracking subjects correctly.
		fs.mu.RLock()
		info, _ = fs.psim.Find(stringToBytes(subj))
		psi = *info
		fs.mu.RUnlock()
		require_Equal(t, psi.total, 5)
		require_Equal(t, psi.fblk, 1)
		require_Equal(t, psi.lblk, 3)
	})
}

func TestFileStoreFullStatePurge(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 132 // Leave room for tombstones.
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		subj, msg := "A", bytes.Repeat([]byte("A"), 19)

		// Should be 2 per block, so 5 blocks.
		for i := 0; i < 10; i++ {
			fs.StoreMsg(subj, nil, msg)
		}
		n, err := fs.Purge()
		require_NoError(t, err)
		require_Equal(t, n, 10)
		state := fs.State()
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		// Add in more 10 more total, some B some C.
		for i := 0; i < 5; i++ {
			fs.StoreMsg("B", nil, msg)
			fs.StoreMsg("C", nil, msg)
		}

		n, err = fs.PurgeEx("B", 0, 0)
		require_NoError(t, err)
		require_Equal(t, n, 5)

		state = fs.State()
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		// Purge with keep.
		n, err = fs.PurgeEx(_EMPTY_, 0, 2)
		require_NoError(t, err)
		require_Equal(t, n, 3)

		state = fs.State()

		// Do some quick checks here, keep had a bug.
		require_Equal(t, state.Msgs, 2)
		require_Equal(t, state.FirstSeq, 18)
		require_Equal(t, state.LastSeq, 20)

		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		// Make sure we can survive a purge with no full stream state and have the correct first sequence.
		// This used to be provided by the idx file and is now tombstones and the full stream state snapshot.
		n, err = fs.Purge()
		require_NoError(t, err)
		require_Equal(t, n, 2)
		state = fs.State()
		fs.Stop()

		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		os.Remove(sfile)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}
	})
}

func TestFileStoreFullStateTestUserRemoveWAL(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 132 // Leave room for tombstones.
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		msgLen := 19
		msgA := bytes.Repeat([]byte("A"), msgLen)
		msgZ := bytes.Repeat([]byte("Z"), msgLen)

		// Store 2 msgs and delete first.
		fs.StoreMsg("A", nil, msgA)
		fs.StoreMsg("Z", nil, msgZ)
		fs.RemoveMsg(1)

		// Check we can load things properly since the block will have a tombstone now for seq 1.
		sm, err := fs.LoadMsg(2, nil)
		require_NoError(t, err)
		require_True(t, bytes.Equal(sm.msg, msgZ))

		require_Equal(t, fs.numMsgBlocks(), 1)
		state := fs.State()
		fs.Stop()

		// Grab the state from this stop.
		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Check we can load things properly since the block will have a tombstone now for seq 1.
		_, err = fs.LoadMsg(2, nil)
		require_NoError(t, err)
		_, err = fs.LoadMsg(1, nil)
		require_Error(t, err, ErrStoreMsgNotFound)

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state does not match:\n%+v\n%+v",
				state, newState)
		}
		require_True(t, !state.FirstTime.IsZero())

		// Store 2 more msgs and delete 2 & 4.
		fs.StoreMsg("A", nil, msgA)
		fs.StoreMsg("Z", nil, msgZ)
		fs.RemoveMsg(2)
		fs.RemoveMsg(4)

		state = fs.State()
		require_Equal(t, len(state.Deleted), state.NumDeleted)
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state does not match:\n%+v\n%+v",
				state, newState)
		}
		require_True(t, !state.FirstTime.IsZero())

		// Now close again and put back old stream state.
		// This will test that we can remember user deletes by placing tombstones in the lmb/wal.
		fs.Stop()
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state does not match:\n%+v\n%+v",
				state, newState)
		}
		require_True(t, !state.FirstTime.IsZero())
	})
}

func TestFileStoreFullStateTestSysRemovals(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 100
		cfg := StreamConfig{
			Name:       "zzz",
			Subjects:   []string{"*"},
			MaxMsgs:    10,
			MaxMsgsPer: 1,
			Storage:    FileStorage,
		}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		msgLen := 19
		msg := bytes.Repeat([]byte("A"), msgLen)

		for _, subj := range []string{"A", "B", "A", "B"} {
			fs.StoreMsg(subj, nil, msg)
		}

		state := fs.State()
		require_Equal(t, state.Msgs, 2)
		require_Equal(t, state.FirstSeq, 3)
		require_Equal(t, state.LastSeq, 4)
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		for _, subj := range []string{"C", "D", "E", "F", "G", "H", "I", "J"} {
			fs.StoreMsg(subj, nil, msg)
		}

		state = fs.State()
		require_Equal(t, state.Msgs, 10)
		require_Equal(t, state.FirstSeq, 3)
		require_Equal(t, state.LastSeq, 12)
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		// Goes over limit
		fs.StoreMsg("ZZZ", nil, msg)

		state = fs.State()
		require_Equal(t, state.Msgs, 10)
		require_Equal(t, state.FirstSeq, 4)
		require_Equal(t, state.LastSeq, 13)
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}
	})
}

func TestFileStoreSelectBlockWithFirstSeqRemovals(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 100
		cfg := StreamConfig{
			Name:       "zzz",
			Subjects:   []string{"*"},
			MaxMsgsPer: 1,
			Storage:    FileStorage,
		}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		msgLen := 19
		msg := bytes.Repeat([]byte("A"), msgLen)

		subjects := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz+@$^"
		// We need over 32 blocks to kick in binary search. So 32*2+1 (65) msgs to get 33 blocks.
		for i := 0; i < 32*2+1; i++ {
			subj := string(subjects[i])
			fs.StoreMsg(subj, nil, msg)
		}
		require_Equal(t, fs.numMsgBlocks(), 33)

		// Now we want to delete the first msg of each block to move the first sequence.
		// Want to do this via system removes, not user initiated moves.
		for i := 0; i < len(subjects); i += 2 {
			subj := string(subjects[i])
			fs.StoreMsg(subj, nil, msg)
		}

		var ss StreamState
		fs.FastState(&ss)

		// We want to make sure that select always returns an index and a non-nil mb.
		for seq := ss.FirstSeq; seq <= ss.LastSeq; seq++ {
			fs.mu.RLock()
			index, mb := fs.selectMsgBlockWithIndex(seq)
			fs.mu.RUnlock()
			require_True(t, index >= 0)
			require_True(t, mb != nil)
			require_Equal(t, (seq-1)/2, uint64(index))
		}
	})
}

func TestFileStoreMsgBlockHolesAndIndexing(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// Grab the message block by hand and manipulate at that level.
	mb := fs.getFirstBlock()
	writeMsg := func(subj string, seq uint64) {
		rl := fileStoreMsgSize(subj, nil, []byte(subj))
		require_NoError(t, mb.writeMsgRecord(rl, seq, subj, nil, []byte(subj), time.Now().UnixNano(), true))
		fs.rebuildState(nil)
	}
	readMsg := func(seq uint64, expectedSubj string) {
		// Clear cache so we load back in from disk and need to properly process any holes.
		ld, tombs, err := mb.rebuildState()
		require_NoError(t, err)
		require_Equal(t, ld, nil)
		require_Equal(t, len(tombs), 0)
		fs.rebuildState(nil)
		sm, _, err := mb.fetchMsg(seq, nil)
		require_NoError(t, err)
		require_Equal(t, sm.subj, expectedSubj)
		require_True(t, bytes.Equal(sm.buf[:len(expectedSubj)], []byte(expectedSubj)))
	}

	writeMsg("A", 2)
	require_Equal(t, mb.first.seq, 2)
	require_Equal(t, mb.last.seq, 2)

	writeMsg("B", 4)
	require_Equal(t, mb.first.seq, 2)
	require_Equal(t, mb.last.seq, 4)

	writeMsg("C", 12)

	readMsg(4, "B")
	require_True(t, mb.dmap.Exists(3))

	readMsg(12, "C")
	readMsg(2, "A")

	// Check that we get deleted for the right ones etc.
	checkDeleted := func(seq uint64) {
		_, _, err = mb.fetchMsg(seq, nil)
		require_Error(t, err, ErrStoreMsgNotFound, errDeletedMsg)
		mb.mu.RLock()
		shouldExist, exists := seq >= mb.first.seq, mb.dmap.Exists(seq)
		mb.mu.RUnlock()
		if shouldExist {
			require_True(t, exists)
		}
	}
	checkDeleted(1)
	checkDeleted(3)
	for seq := 5; seq < 12; seq++ {
		checkDeleted(uint64(seq))
	}
}

func TestFileStoreMsgBlockCompactionAndHoles(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(t, err)
	defer fs.Stop()

	msg := bytes.Repeat([]byte("Z"), 1024)
	for _, subj := range []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"} {
		fs.StoreMsg(subj, nil, msg)
	}
	// Leave first one but delete the rest.
	for seq := uint64(2); seq < 10; seq++ {
		fs.RemoveMsg(seq)
	}
	require_Equal(t, fs.numMsgBlocks(), 1)
	mb := fs.getFirstBlock()
	require_NotNil(t, mb)

	_, ub, _ := fs.Utilization()

	// Do compaction, should remove all excess now.
	mb.mu.Lock()
	mb.compact()
	mb.mu.Unlock()

	ta, ua, _ := fs.Utilization()
	require_Equal(t, ub, ua)
	require_Equal(t, ta, ua)
}

func TestFileStoreRemoveLastNoDoubleTombstones(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("A", nil, []byte("hello"))
	fs.mu.Lock()
	fs.removeMsgViaLimits(1)
	fs.mu.Unlock()

	require_Equal(t, fs.numMsgBlocks(), 1)
	mb := fs.getFirstBlock()
	require_NotNil(t, mb)
	mb.loadMsgs()
	rbytes, _, err := fs.Utilization()
	require_NoError(t, err)
	require_Equal(t, rbytes, emptyRecordLen)
}

func TestFileStoreFullStateMultiBlockPastWAL(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 100
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		msgLen := 19
		msgA := bytes.Repeat([]byte("A"), msgLen)
		msgZ := bytes.Repeat([]byte("Z"), msgLen)

		// Store 2 msgs
		fs.StoreMsg("A", nil, msgA)
		fs.StoreMsg("B", nil, msgZ)
		require_Equal(t, fs.numMsgBlocks(), 1)
		fs.Stop()

		// Grab the state from this stop.
		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store 6 more msgs.
		fs.StoreMsg("C", nil, msgA)
		fs.StoreMsg("D", nil, msgZ)
		fs.StoreMsg("E", nil, msgA)
		fs.StoreMsg("F", nil, msgZ)
		fs.StoreMsg("G", nil, msgA)
		fs.StoreMsg("H", nil, msgZ)
		require_Equal(t, fs.numMsgBlocks(), 4)
		state := fs.State()
		fs.Stop()

		// Put back old stream state.
		// This will test that we properly walk multiple blocks past where we snapshotted state.
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state does not match:\n%+v\n%+v",
				state, newState)
		}
		require_True(t, !state.FirstTime.IsZero())
	})
}

// This tests we can successfully recover without having to rebuild the whole stream from a mid block index.db marker
// when they updated block has a removed entry.
// TODO(dlc) - This test will force a rebuild atm, leaving here for later.
func TestFileStoreFullStateMidBlockPastWAL(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxMsgsPer: 1}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		msg := bytes.Repeat([]byte("Z"), 19)

		// Store 5 msgs
		fs.StoreMsg("A", nil, msg)
		fs.StoreMsg("B", nil, msg)
		fs.StoreMsg("C", nil, msg)
		fs.StoreMsg("D", nil, msg)
		fs.StoreMsg("E", nil, msg)
		require_Equal(t, fs.numMsgBlocks(), 1)
		fs.Stop()

		// Grab the state from this stop.
		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store 5 more messages, then remove seq 2, "B".
		fs.StoreMsg("F", nil, msg)
		fs.StoreMsg("G", nil, msg)
		fs.StoreMsg("H", nil, msg)
		fs.StoreMsg("I", nil, msg)
		fs.StoreMsg("J", nil, msg)
		fs.RemoveMsg(2)

		require_Equal(t, fs.numMsgBlocks(), 1)
		state := fs.State()
		fs.Stop()

		// Put back old stream state.
		// This will test that we properly walk multiple blocks past where we snapshotted state.
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state does not match:\n%+v\n%+v",
				state, newState)
		}
	})
}

func TestFileStoreCompactingBlocksOnSync(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1000 // 20 msgs per block.
		fcfg.SyncInterval = 100 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxMsgsPer: 1}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 20 msgs per blk.
		msg := bytes.Repeat([]byte("Z"), 19)
		subjects := "ABCDEFGHIJKLMNOPQRST"
		for _, subj := range subjects {
			fs.StoreMsg(string(subj), nil, msg)
		}
		require_Equal(t, fs.numMsgBlocks(), 1)
		total, reported, err := fs.Utilization()
		require_NoError(t, err)

		require_Equal(t, total, reported)

		// Now start removing, since we are small this should not kick in any inline logic.
		// Remove all interior messages, leave 1 and 20. So write B-S
		for i := 1; i < 19; i++ {
			fs.StoreMsg(string(subjects[i]), nil, msg)
		}
		require_Equal(t, fs.numMsgBlocks(), 2)

		blkUtil := func() (uint64, uint64) {
			fs.mu.RLock()
			fmb := fs.blks[0]
			fs.mu.RUnlock()
			fmb.mu.RLock()
			defer fmb.mu.RUnlock()
			return fmb.rbytes, fmb.bytes
		}

		total, reported = blkUtil()
		require_Equal(t, reported, 100)
		// Raw bytes will be 1000, but due to compression could be less.
		if fcfg.Compression != NoCompression {
			require_True(t, total > reported)
		} else {
			require_Equal(t, total, 1000)
		}

		// Make sure the sync interval when kicked in compacts down to rbytes == 100.
		checkFor(t, time.Second, 100*time.Millisecond, func() error {
			if total, reported := blkUtil(); total <= reported {
				return nil
			}
			return fmt.Errorf("Not compacted yet, raw %v vs reported %v",
				friendlyBytes(total), friendlyBytes(reported))
		})
	})
}

// Make sure a call to Compact() updates PSIM correctly.
func TestFileStoreCompactAndPSIMWhenDeletingBlocks(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 512},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "A", bytes.Repeat([]byte("ABC"), 33) // ~100bytes

	// Add in 10 As
	for i := 0; i < 10; i++ {
		fs.StoreMsg(subj, nil, msg)
	}
	require_Equal(t, fs.numMsgBlocks(), 4)

	// Should leave 1.
	n, err := fs.Compact(10)
	require_NoError(t, err)
	require_Equal(t, n, 9)
	require_Equal(t, fs.numMsgBlocks(), 1)

	fs.mu.RLock()
	info, _ := fs.psim.Find(stringToBytes(subj))
	psi := *info
	fs.mu.RUnlock()

	require_Equal(t, psi.total, 1)
	require_Equal(t, psi.fblk, psi.lblk)
}

func TestFileStoreTrackSubjLenForPSIM(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Place 1000 msgs with varying subjects.
	// Make sure we track the subject length properly.
	smap := make(map[string]int, 1000)
	buf := make([]byte, 10)
	for i := 0; i < 1000; i++ {
		var b strings.Builder
		// 1-6 tokens.
		numTokens := rand.Intn(6) + 1
		for i := 0; i < numTokens; i++ {
			tlen := rand.Intn(4) + 2
			tok := buf[:tlen]
			crand.Read(tok)
			b.WriteString(hex.EncodeToString(tok))
			if i != numTokens-1 {
				b.WriteString(".")
			}
		}
		subj := b.String()
		// Avoid dupes since will cause check to fail after we delete messages.
		if _, ok := smap[subj]; ok {
			continue
		}
		smap[subj] = len(subj)
		fs.StoreMsg(subj, nil, nil)
	}

	check := func() {
		t.Helper()
		var total int
		for _, slen := range smap {
			total += slen
		}
		fs.mu.RLock()
		tsl := fs.tsl
		fs.mu.RUnlock()
		require_Equal(t, tsl, total)
	}

	check()

	// Delete ~half
	var smv StoreMsg
	for i := 0; i < 500; i++ {
		seq := uint64(rand.Intn(1000) + 1)
		sm, err := fs.LoadMsg(seq, &smv)
		if err != nil {
			continue
		}
		fs.RemoveMsg(seq)
		delete(smap, sm.subj)
	}

	check()

	// Make sure we can recover same after restart.
	fs.Stop()
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	check()

	// Drain the rest through purge.
	fs.Purge()
	smap = nil
	check()
}

// This was used to make sure our estimate was correct, but not needed normally.
func TestFileStoreLargeFullStatePSIM(t *testing.T) {
	t.Skip()

	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	buf := make([]byte, 20)
	for i := 0; i < 100_000; i++ {
		var b strings.Builder
		// 1-6 tokens.
		numTokens := rand.Intn(6) + 1
		for i := 0; i < numTokens; i++ {
			tlen := rand.Intn(8) + 2
			tok := buf[:tlen]
			crand.Read(tok)
			b.WriteString(hex.EncodeToString(tok))
			if i != numTokens-1 {
				b.WriteString(".")
			}
		}
		subj := b.String()
		fs.StoreMsg(subj, nil, nil)
	}
	fs.Stop()
}

func TestFileStoreLargeFullStateMetaCleanup(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "foo.bar.baz", bytes.Repeat([]byte("ABC"), 33) // ~100bytes
	for i := 0; i < 1000; i++ {
		fs.StoreMsg(subj, nil, nil)
	}
	fs.Stop()

	mdir := filepath.Join(sd, msgDir)
	idxFile := filepath.Join(mdir, "1.idx")
	fssFile := filepath.Join(mdir, "1.fss")
	require_NoError(t, os.WriteFile(idxFile, msg, defaultFilePerms))
	require_NoError(t, os.WriteFile(fssFile, msg, defaultFilePerms))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		if _, err := os.Stat(idxFile); err == nil {
			return errors.New("idx file still exists")
		}
		if _, err := os.Stat(fssFile); err == nil {
			return errors.New("fss file still exists")
		}
		return nil
	})
}

func TestFileStoreIndexDBExistsAfterShutdown(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj := "foo.bar.baz"
	for i := 0; i < 1000; i++ {
		fs.StoreMsg(subj, nil, nil)
	}

	idxFile := filepath.Join(sd, msgDir, streamStreamStateFile)

	fs.mu.Lock()
	fs.dirty = 1
	if err := os.Remove(idxFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	fs.mu.Unlock()

	fs.Stop()

	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		if _, err := os.Stat(idxFile); err != nil {
			return fmt.Errorf("%q doesn't exist", idxFile)
		}
		return nil
	})
}

// https://github.com/nats-io/nats-server/issues/4842
func TestFileStoreSubjectCorruption(t *testing.T) {
	sd, blkSize := t.TempDir(), uint64(2*1024*1024)
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: blkSize},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	numSubjects := 100
	msgs := [][]byte{bytes.Repeat([]byte("ABC"), 333), bytes.Repeat([]byte("ABC"), 888), bytes.Repeat([]byte("ABC"), 555)}
	for i := 0; i < 10_000; i++ {
		subj := fmt.Sprintf("foo.%d", rand.Intn(numSubjects)+1)
		msg := msgs[rand.Intn(len(msgs))]
		fs.StoreMsg(subj, nil, msg)
	}
	fs.Stop()

	require_NoError(t, os.Remove(filepath.Join(sd, msgDir, streamStreamStateFile)))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: blkSize},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	for subj := range fs.SubjectsTotals(">") {
		var n int
		_, err := fmt.Sscanf(subj, "foo.%d", &n)
		require_NoError(t, err)
	}
}

// Since 2.10 we no longer have fss, and the approach for calculating NumPending would branch
// based on the old fss metadata being present. This meant that calculating NumPending in >= 2.10.x
// would load all blocks to complete. This test makes sure we do not do that anymore.
func TestFileStoreNumPendingLastBySubject(t *testing.T) {
	sd, blkSize := t.TempDir(), uint64(1024)
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: blkSize},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	numSubjects := 20
	msg := bytes.Repeat([]byte("ABC"), 25)
	for i := 1; i <= 1000; i++ {
		subj := fmt.Sprintf("foo.%d.%d", rand.Intn(numSubjects)+1, i)
		fs.StoreMsg(subj, nil, msg)
	}
	// Each block has ~8 msgs.
	require_True(t, fs.numMsgBlocks() > 100)

	calcCacheLoads := func() (cloads uint64) {
		fs.mu.RLock()
		defer fs.mu.RUnlock()
		for _, mb := range fs.blks {
			mb.mu.RLock()
			cloads += mb.cloads
			mb.mu.RUnlock()
		}
		return cloads
	}

	total, _ := fs.NumPending(0, "foo.*.*", true)
	require_Equal(t, total, 1000)
	// Make sure no blocks were loaded to calculate this as a new consumer.
	require_Equal(t, calcCacheLoads(), 0)

	checkResult := func(sseq, np uint64, filter string) {
		t.Helper()
		var checkTotal uint64
		var smv StoreMsg
		for seq := sseq; seq <= 1000; seq++ {
			sm, err := fs.LoadMsg(seq, &smv)
			require_NoError(t, err)
			if subjectIsSubsetMatch(sm.subj, filter) {
				checkTotal++
			}
		}
		require_Equal(t, np, checkTotal)
	}

	// Make sure partials work properly.
	for _, filter := range []string{"foo.10.*", "*.22.*", "*.*.222", "foo.5.999", "*.2.*"} {
		sseq := uint64(rand.Intn(250) + 200) // Between 200-450
		total, _ = fs.NumPending(sseq, filter, true)
		checkResult(sseq, total, filter)
	}
}

// We had a bug that could cause internal memory corruption of the psim keys in memory
// which could have been written to disk via index.db.
func TestFileStoreCorruptPSIMOnDisk(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("foo.bar", nil, []byte("ABC"))
	fs.StoreMsg("foo.baz", nil, []byte("XYZ"))

	// Force bad subject.
	fs.mu.Lock()
	psi, _ := fs.psim.Find(stringToBytes("foo.bar"))
	bad := make([]byte, 7)
	crand.Read(bad)
	fs.psim.Insert(bad, *psi)
	fs.psim.Delete(stringToBytes("foo.bar"))
	fs.dirty++
	fs.mu.Unlock()

	// Restart
	fs.Stop()
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	sm, err := fs.LoadLastMsg("foo.bar", nil)
	require_NoError(t, err)
	require_True(t, bytes.Equal(sm.msg, []byte("ABC")))

	sm, err = fs.LoadLastMsg("foo.baz", nil)
	require_NoError(t, err)
	require_True(t, bytes.Equal(sm.msg, []byte("XYZ")))
}

func TestFileStorePurgeExBufPool(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 1024},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := bytes.Repeat([]byte("ABC"), 33) // ~100bytes
	for i := 0; i < 1000; i++ {
		fs.StoreMsg("foo.foo", nil, msg)
		fs.StoreMsg("foo.bar", nil, msg)
	}

	p, err := fs.PurgeEx("foo.bar", 1, 0)
	require_NoError(t, err)
	require_Equal(t, p, 1000)

	// Now make sure we do not have all of the msg blocks cache's loaded.
	var loaded int
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		if mb.cacheAlreadyLoaded() {
			loaded++
		}
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()
	require_Equal(t, loaded, 1)
}

func TestFileStoreFSSMeta(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 100, CacheExpire: 200 * time.Millisecond, SyncInterval: time.Second},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// This yields an internal record length of 50 bytes. So 2 msgs per blk with subject len of 1, e.g. "A" or "Z".
	msg := bytes.Repeat([]byte("Z"), 19)

	// Should leave us with |A-Z| |Z-Z| |Z-Z| |Z-A|
	fs.StoreMsg("A", nil, msg)
	for i := 0; i < 6; i++ {
		fs.StoreMsg("Z", nil, msg)
	}
	fs.StoreMsg("A", nil, msg)

	// Let cache's expire before PurgeEx which will load them back in.
	time.Sleep(250 * time.Millisecond)

	p, err := fs.PurgeEx("A", 1, 0)
	require_NoError(t, err)
	require_Equal(t, p, 2)

	// Make sure cache is not loaded but fss state still is.
	var stillHasCache, noFSS bool
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		stillHasCache = stillHasCache || mb.cacheAlreadyLoaded()
		noFSS = noFSS || mb.fssNotLoaded()
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()

	require_False(t, stillHasCache)
	require_False(t, noFSS)

	// Let fss expire via syncInterval.
	time.Sleep(time.Second)

	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		noFSS = noFSS || mb.fssNotLoaded()
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()

	require_True(t, noFSS)
}

func TestFileStoreExpireCacheOnLinearWalk(t *testing.T) {
	sd := t.TempDir()
	expire := 250 * time.Millisecond
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, CacheExpire: expire},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// This yields an internal record length of 50 bytes.
	subj, msg := "Z", bytes.Repeat([]byte("Z"), 19)

	// Store 10 messages, so 5 blocks.
	for i := 0; i < 10; i++ {
		fs.StoreMsg(subj, nil, msg)
	}
	// Let them all expire. This way we load as we walk and can test that we expire all blocks without
	// needing to worry about last write times blocking forced expiration.
	time.Sleep(expire)

	checkNoCache := func() {
		t.Helper()
		fs.mu.RLock()
		var stillHasCache bool
		for _, mb := range fs.blks {
			mb.mu.RLock()
			stillHasCache = stillHasCache || mb.cacheAlreadyLoaded()
			mb.mu.RUnlock()
		}
		fs.mu.RUnlock()
		require_False(t, stillHasCache)
	}

	// Walk forward.
	var smv StoreMsg
	for seq := uint64(1); seq <= 10; seq++ {
		_, err := fs.LoadMsg(seq, &smv)
		require_NoError(t, err)
	}
	checkNoCache()

	// No test walking backwards. We have this scenario when we search for starting points for sourced streams.
	// Noticed some memory bloat when we have to search many blocks looking for a source that may be closer to the
	// beginning of the stream (infrequently updated sourced stream).
	for seq := uint64(10); seq >= 1; seq-- {
		_, err := fs.LoadMsg(seq, &smv)
		require_NoError(t, err)
	}
	checkNoCache()

	// Now make sure still expires properly on linear scans with deleted msgs.
	// We want to make sure we track linear updates even if message deleted.
	_, err = fs.RemoveMsg(2)
	require_NoError(t, err)
	_, err = fs.RemoveMsg(9)
	require_NoError(t, err)

	// Walk forward.
	for seq := uint64(1); seq <= 10; seq++ {
		_, err := fs.LoadMsg(seq, &smv)
		if seq == 2 || seq == 9 {
			require_Error(t, err, errDeletedMsg)
		} else {
			require_NoError(t, err)
		}
	}
	checkNoCache()
}

func TestFileStoreSkipMsgs(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 1024},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Test on empty FS first.
	// Make sure wrong starting sequence fails.
	err = fs.SkipMsgs(10, 100)
	require_Error(t, err, ErrSequenceMismatch)

	err = fs.SkipMsgs(1, 100)
	require_NoError(t, err)

	state := fs.State()
	require_Equal(t, state.FirstSeq, 101)
	require_Equal(t, state.LastSeq, 100)
	require_Equal(t, fs.numMsgBlocks(), 1)

	// Now add alot.
	err = fs.SkipMsgs(101, 100_000)
	require_NoError(t, err)
	state = fs.State()
	require_Equal(t, state.FirstSeq, 100_101)
	require_Equal(t, state.LastSeq, 100_100)
	require_Equal(t, fs.numMsgBlocks(), 1)

	// Now add in a message, and then skip to check dmap.
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 1024},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("foo", nil, nil)
	err = fs.SkipMsgs(2, 10)
	require_NoError(t, err)
	state = fs.State()
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 11)
	require_Equal(t, state.Msgs, 1)
	require_Equal(t, state.NumDeleted, 10)
	require_Equal(t, len(state.Deleted), 10)

	// Check Fast State too.
	state.Deleted = nil
	fs.FastState(&state)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 11)
	require_Equal(t, state.Msgs, 1)
	require_Equal(t, state.NumDeleted, 10)
}

func TestFileStoreOptimizeFirstLoadNextMsgWithSequenceZero(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 4096},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := bytes.Repeat([]byte("ZZZ"), 33) // ~100bytes

	for i := 0; i < 5000; i++ {
		fs.StoreMsg("foo.A", nil, msg)
	}
	// This will create alot of blocks, ~167.
	// Just used to check that we do not load these in when searching.
	// Now add in 10 for foo.bar at the end.
	for i := 0; i < 10; i++ {
		fs.StoreMsg("foo.B", nil, msg)
	}
	// The bug would not be visible on running server per se since we would have had fss loaded
	// and that sticks around a bit longer, we would use that to skip over the early blocks. So stop
	// and restart the filestore.
	fs.Stop()
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 4096},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Now fetch the next message for foo.B but set starting sequence to 0.
	_, nseq, err := fs.LoadNextMsg("foo.B", false, 0, nil)
	require_NoError(t, err)
	require_Equal(t, nseq, 5001)
	// Now check how many blks are loaded, should be only 1.
	require_Equal(t, fs.cacheLoads(), 1)
}

///////////////////////////////////////////////////////////////////////////
// Benchmarks
///////////////////////////////////////////////////////////////////////////

func Benchmark_FileStoreSelectMsgBlock(b *testing.B) {
	// We use small block size to create lots of blocks for this test.
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 128},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	if err != nil {
		b.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "A", bytes.Repeat([]byte("ABC"), 33) // ~100bytes

	// Add in a bunch of blocks.
	for i := 0; i < 1000; i++ {
		fs.StoreMsg(subj, nil, msg)
	}
	if fs.numMsgBlocks() < 1000 {
		b.Fatalf("Expected at least 1000 blocks, got %d", fs.numMsgBlocks())
	}

	fs.mu.RLock()
	defer fs.mu.RUnlock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, mb := fs.selectMsgBlockWithIndex(1)
		if mb == nil {
			b.Fatalf("Expected a non-nil mb")
		}
	}
	b.StopTimer()
}
