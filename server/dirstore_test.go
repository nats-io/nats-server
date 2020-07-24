/*
 * Copyright 2019 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package server

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

func require_True(t *testing.T, b bool) {
	t.Helper()
	if !b {
		t.Errorf("require true, but got false")
	}
}

func require_False(t *testing.T, b bool) {
	t.Helper()
	if b {
		t.Errorf("require no false, but got true")
	}
}

func require_NoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("require no error, but got: %v", err)
	}
}

func require_Error(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Errorf("require no error, but got: %v", err)
	}
}

func require_Equal(t *testing.T, a, b string) {
	t.Helper()
	if strings.Compare(a, b) != 0 {
		t.Errorf("require equal, but got: %v != %v", a, b)
	}
}

func require_NotEqual(t *testing.T, a, b [32]byte) {
	t.Helper()
	if bytes.Equal(a[:], b[:]) {
		t.Errorf("require not equal, but got: %v != %v", a, b)
	}
}

func require_Len(t *testing.T, a, b int) {
	t.Helper()
	if a != b {
		t.Errorf("require len, but got: %v != %v", a, b)
	}
}

func TestShardedDirStoreWriteAndReadonly(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)

	store, err := NewDirJWTStore(dir, true, false, nil, nil)
	require_NoError(t, err)

	expected := map[string]string{
		"one":   "alpha",
		"two":   "beta",
		"three": "gamma",
		"four":  "delta",
	}

	require_False(t, store.IsReadOnly())

	for k, v := range expected {
		store.SaveAcc(k, v)
	}

	for k, v := range expected {
		got, err := store.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}

	got, err := store.LoadAcc("random")
	require_Error(t, err)
	require_Equal(t, "", got)

	got, err = store.LoadAcc("")
	require_Error(t, err)
	require_Equal(t, "", got)

	err = store.SaveAcc("", "onetwothree")
	require_Error(t, err)
	store.Close()

	// re-use the folder for readonly mode
	store, err = NewImmutableDirJWTStore(dir, true, func(pubKey string) {}, func(err error) {})
	require_NoError(t, err)

	require_True(t, store.IsReadOnly())

	err = store.SaveAcc("five", "omega")
	require_Error(t, err)

	for k, v := range expected {
		got, err := store.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}
	store.Close()
}

func TestUnshardedDirStoreWriteAndReadonly(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)

	store, err := NewDirJWTStore(dir, false, false, nil, nil)
	require_NoError(t, err)

	expected := map[string]string{
		"one":   "alpha",
		"two":   "beta",
		"three": "gamma",
		"four":  "delta",
	}

	require_False(t, store.IsReadOnly())

	for k, v := range expected {
		store.SaveAcc(k, v)
	}

	for k, v := range expected {
		got, err := store.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}

	got, err := store.LoadAcc("random")
	require_Error(t, err)
	require_Equal(t, "", got)

	got, err = store.LoadAcc("")
	require_Error(t, err)
	require_Equal(t, "", got)

	err = store.SaveAcc("", "onetwothree")
	require_Error(t, err)
	store.Close()

	// re-use the folder for readonly mode
	store, err = NewImmutableDirJWTStore(dir, false, func(pubKey string) {}, func(err error) {})
	require_NoError(t, err)

	require_True(t, store.IsReadOnly())

	err = store.SaveAcc("five", "omega")
	require_Error(t, err)

	for k, v := range expected {
		got, err := store.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}
	store.Close()
}

func TestReadonlyRequiresDir(t *testing.T) {
	_, err := NewImmutableDirJWTStore("/a/b/c", true, func(pubKey string) {}, func(err error) {})
	require_Error(t, err)
}

func TestNoCreateRequiresDir(t *testing.T) {
	_, err := NewDirJWTStore("/a/b/c", true, false, func(pubKey string) {}, func(err error) {})
	require_Error(t, err)
}

func TestCreateMakesDir(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)

	fullPath := filepath.Join(dir, "a/b")

	_, err = os.Stat(fullPath)
	require_Error(t, err)
	require_True(t, os.IsNotExist(err))

	s, err := NewDirJWTStore(fullPath, false, true, func(pubKey string) {}, func(err error) {})
	require_NoError(t, err)
	s.Close()

	_, err = os.Stat(fullPath)
	require_NoError(t, err)
}

func TestDirStoreNotifications(t *testing.T) {

	// Skip the file notification test on travis
	if os.Getenv("TRAVIS_GO_VERSION") != "" {
		return
	}

	for _, test := range []struct {
		name    string
		sharded bool
	}{
		{"sharded", true},
		{"unsharded", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
			require_NoError(t, err)
			defer os.RemoveAll(dir)

			notified := make(chan bool, 1)
			errors := make(chan error, 10)
			wStoreState := int32(0)

			store, err := NewDirJWTStore(dir, test.sharded, false, func(pubKey string) {
				n := atomic.LoadInt32(&wStoreState)
				switch n {
				case 0:
					return
				case 1:
					if pubKey == "one" {
						notified <- true
						atomic.StoreInt32(&wStoreState, 0)
					}
				case 2:
					if pubKey == "two" {
						notified <- true
						atomic.StoreInt32(&wStoreState, 0)
					}
				}
			}, func(err error) {
				errors <- err
			})
			require_NoError(t, err)
			defer store.Close()
			require_True(t, store.IsReadOnly())

			expected := map[string]string{
				"one":   "alpha",
				"two":   "beta",
				"three": "gamma",
				"four":  "delta",
			}

			for k, v := range expected {
				require_Error(t, store.SaveAcc(k, v))
				if test.sharded {
					require_NoError(t, os.MkdirAll(fmt.Sprintf("%s/%s/", dir, k[len(k)-2:]), 0755))
					require_NoError(t, ioutil.WriteFile(fmt.Sprintf("%s/%s/%s.jwt", dir, k[len(k)-2:], k), []byte(v), 0644))
				} else {
					require_NoError(t, ioutil.WriteFile(fmt.Sprintf("%s/%s.jwt", dir, k), []byte(v), 0644))
				}
			}

			time.Sleep(time.Second)

			for k, v := range expected {
				got, err := store.LoadAcc(k)
				require_NoError(t, err)
				require_Equal(t, v, got)
			}

			atomic.StoreInt32(&wStoreState, 1)
			require_Error(t, store.SaveAcc("one", "zip"))
			if test.sharded {
				require_NoError(t, ioutil.WriteFile(fmt.Sprintf("%s/ne/%s.jwt", dir, "one"), []byte("zip"), 0644))
			} else {
				require_NoError(t, ioutil.WriteFile(fmt.Sprintf("%s/%s.jwt", dir, "one"), []byte("zip"), 0644))
			}

			check := func() {
				t.Helper()
				select {
				case <-notified:
				case e := <-errors:
					t.Fatal(e.Error())
				case <-time.After(5 * time.Second):
					t.Fatalf("Did not get notified")
				}
			}
			check()

			// re-use the folder for readonly mode
			roStoreState := int32(0)
			readOnlyStore, err := NewImmutableDirJWTStore(dir, test.sharded, func(pubKey string) {
				n := atomic.LoadInt32(&roStoreState)
				switch n {
				case 0:
					return
				case 1:
					if pubKey == "two" {
						notified <- true
						atomic.StoreInt32(&roStoreState, 0)
					}
				}
			}, func(err error) {
				errors <- err
			})
			require_NoError(t, err)
			defer readOnlyStore.Close()
			require_True(t, readOnlyStore.IsReadOnly())

			got, err := readOnlyStore.LoadAcc("one")
			require_NoError(t, err)
			require_Equal(t, "zip", got)

			atomic.StoreInt32(&roStoreState, 1)
			atomic.StoreInt32(&wStoreState, 2)

			if test.sharded {
				require_NoError(t, ioutil.WriteFile(fmt.Sprintf("%s/wo/%s.jwt", dir, "two"), []byte("zap"), 0644))
			} else {
				require_NoError(t, ioutil.WriteFile(fmt.Sprintf("%s/%s.jwt", dir, "two"), []byte("zap"), 0644))
			}

			for i := 0; i < 2; i++ {
				check()
			}
		})
	}
}

func TestShardedDirStorePackMerge(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)
	dir2, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)
	dir3, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)

	store, err := NewDirJWTStore(dir, true, false, nil, nil)
	require_NoError(t, err)

	expected := map[string]string{
		"one":   "alpha",
		"two":   "beta",
		"three": "gamma",
		"four":  "delta",
	}

	require_False(t, store.IsReadOnly())

	for k, v := range expected {
		store.SaveAcc(k, v)
	}

	for k, v := range expected {
		got, err := store.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}

	got, err := store.LoadAcc("random")
	require_Error(t, err)
	require_Equal(t, "", got)

	pack, err := store.Pack(-1)
	require_NoError(t, err)

	inc, err := NewDirJWTStore(dir2, true, false, nil, nil)
	require_NoError(t, err)

	inc.Merge(pack)

	for k, v := range expected {
		got, err := inc.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}

	got, err = inc.LoadAcc("random")
	require_Error(t, err)
	require_Equal(t, "", got)

	limitedPack, err := inc.Pack(1)
	require_NoError(t, err)

	limited, err := NewDirJWTStore(dir3, true, false, nil, nil)

	require_NoError(t, err)

	limited.Merge(limitedPack)

	count := 0
	for k, v := range expected {
		got, err := limited.LoadAcc(k)
		if err == nil {
			count++
			require_Equal(t, v, got)
		}
	}

	require_Len(t, 1, count)

	got, err = inc.LoadAcc("random")
	require_Error(t, err)
	require_Equal(t, "", got)
}

func TestShardedToUnsharedDirStorePackMerge(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)
	dir2, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)

	store, err := NewDirJWTStore(dir, true, false, nil, nil)
	require_NoError(t, err)

	expected := map[string]string{
		"one":   "alpha",
		"two":   "beta",
		"three": "gamma",
		"four":  "delta",
	}

	require_False(t, store.IsReadOnly())

	for k, v := range expected {
		store.SaveAcc(k, v)
	}

	for k, v := range expected {
		got, err := store.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}

	got, err := store.LoadAcc("random")
	require_Error(t, err)
	require_Equal(t, "", got)

	pack, err := store.Pack(-1)
	require_NoError(t, err)

	inc, err := NewDirJWTStore(dir2, false, false, nil, nil)
	require_NoError(t, err)

	inc.Merge(pack)

	for k, v := range expected {
		got, err := inc.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}

	got, err = inc.LoadAcc("random")
	require_Error(t, err)
	require_Equal(t, "", got)

	err = store.Merge("foo")
	require_Error(t, err)

	err = store.Merge("") // will skip it
	require_NoError(t, err)

	err = store.Merge("a|something") // should fail on a for sharding
	require_Error(t, err)
}

func TestMergeOnlyOnNewer(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)

	dirStore, err := NewDirJWTStore(dir, true, false, nil, nil)
	require_NoError(t, err)

	accountKey, err := nkeys.CreateAccount()
	require_NoError(t, err)

	pubKey, err := accountKey.PublicKey()
	require_NoError(t, err)

	account := jwt.NewAccountClaims(pubKey)
	account.Name = "old"
	olderJWT, err := account.Encode(accountKey)
	require_NoError(t, err)

	time.Sleep(2 * time.Second)

	account.Name = "new"
	newerJWT, err := account.Encode(accountKey)
	require_NoError(t, err)

	// Should work
	err = dirStore.SaveAcc(pubKey, olderJWT)
	require_NoError(t, err)
	fromStore, err := dirStore.LoadAcc(pubKey)
	require_NoError(t, err)
	require_Equal(t, olderJWT, fromStore)

	// should replace
	err = dirStore.saveIfNewer(pubKey, newerJWT)
	require_NoError(t, err)
	fromStore, err = dirStore.LoadAcc(pubKey)
	require_NoError(t, err)
	require_Equal(t, newerJWT, fromStore)

	// should fail
	err = dirStore.saveIfNewer(pubKey, olderJWT)
	require_NoError(t, err)
	fromStore, err = dirStore.LoadAcc(pubKey)
	require_NoError(t, err)
	require_Equal(t, newerJWT, fromStore)
}

func createTestAccount(t *testing.T, dirStore *DirJWTStore, expSec int, accKey nkeys.KeyPair) string {
	t.Helper()
	pubKey, err := accKey.PublicKey()
	require_NoError(t, err)
	account := jwt.NewAccountClaims(pubKey)
	if expSec > 0 {
		account.Expires = time.Now().Add(time.Second * time.Duration(expSec)).Unix()
	}
	jwt, err := account.Encode(accKey)
	require_NoError(t, err)
	err = dirStore.SaveAcc(pubKey, jwt)
	require_NoError(t, err)
	return jwt
}

func assertStoreSize(t *testing.T, dirStore *DirJWTStore, length int) {
	t.Helper()
	f, err := ioutil.ReadDir(dirStore.directory)
	require_NoError(t, err)
	require_Len(t, len(f), length)
	dirStore.Lock()
	require_Len(t, len(dirStore.expiration.idx), length)
	require_Len(t, dirStore.expiration.lru.Len(), length)
	require_Len(t, len(dirStore.expiration.heap), length)
	dirStore.Unlock()
}

func TestExpiration(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)

	dirStore, err := NewExpiringDirJWTStore(dir, false, false, time.Millisecond*100, 10, true, 0, nil)
	require_NoError(t, err)
	defer dirStore.Close()

	account := func(expSec int) {
		accountKey, err := nkeys.CreateAccount()
		require_NoError(t, err)
		createTestAccount(t, dirStore, expSec, accountKey)
	}

	h := dirStore.Hash()

	for i := 1; i <= 5; i++ {
		account(i * 2)
		nh := dirStore.Hash()
		require_NotEqual(t, h, nh)
		h = nh
	}
	time.Sleep(1 * time.Second)
	for i := 5; i > 0; i-- {
		f, err := ioutil.ReadDir(dir)
		require_NoError(t, err)
		require_Len(t, len(f), i)
		assertStoreSize(t, dirStore, i)

		time.Sleep(2 * time.Second)

		nh := dirStore.Hash()
		require_NotEqual(t, h, nh)
		h = nh
	}
	assertStoreSize(t, dirStore, 0)
}

func TestLimit(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)

	dirStore, err := NewExpiringDirJWTStore(dir, false, false, time.Millisecond*100, 5, true, 0, nil)
	require_NoError(t, err)
	defer dirStore.Close()

	account := func(expSec int) {
		accountKey, err := nkeys.CreateAccount()
		require_NoError(t, err)
		createTestAccount(t, dirStore, expSec, accountKey)
	}

	h := dirStore.Hash()

	accountKey, err := nkeys.CreateAccount()
	require_NoError(t, err)
	// update first account
	for i := 0; i < 10; i++ {
		createTestAccount(t, dirStore, 50, accountKey)
		assertStoreSize(t, dirStore, 1)
	}
	// new accounts
	for i := 0; i < 10; i++ {
		account(i)
		nh := dirStore.Hash()
		require_NotEqual(t, h, nh)
		h = nh
	}
	// first account should be gone now accountKey.PublicKey()
	key, _ := accountKey.PublicKey()
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, key))
	require_True(t, os.IsNotExist(err))

	// update first account
	for i := 0; i < 10; i++ {
		createTestAccount(t, dirStore, 50, accountKey)
		assertStoreSize(t, dirStore, 5)
	}
}

func TestLimitNoEvict(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)

	dirStore, err := NewExpiringDirJWTStore(dir, false, false, time.Millisecond*100, 2, false, 0, nil)
	require_NoError(t, err)
	defer dirStore.Close()

	accountKey1, err := nkeys.CreateAccount()
	require_NoError(t, err)
	pKey1, err := accountKey1.PublicKey()
	require_NoError(t, err)
	accountKey2, err := nkeys.CreateAccount()
	require_NoError(t, err)
	accountKey3, err := nkeys.CreateAccount()
	require_NoError(t, err)
	pKey3, err := accountKey3.PublicKey()
	require_NoError(t, err)

	createTestAccount(t, dirStore, 100, accountKey1)
	assertStoreSize(t, dirStore, 1)
	createTestAccount(t, dirStore, 2, accountKey2)
	assertStoreSize(t, dirStore, 2)

	hBefore := dirStore.Hash()
	// 2 jwt are already stored. third must result in an error
	pubKey, err := accountKey3.PublicKey()
	require_NoError(t, err)
	account := jwt.NewAccountClaims(pubKey)
	jwt, err := account.Encode(accountKey3)
	require_NoError(t, err)
	err = dirStore.SaveAcc(pubKey, jwt)
	require_Error(t, err)
	assertStoreSize(t, dirStore, 2)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey1))
	require_NoError(t, err)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey3))
	require_True(t, os.IsNotExist(err))
	// check that the hash did not change
	hAfter := dirStore.Hash()
	require_True(t, bytes.Equal(hBefore[:], hAfter[:]))
	// wait for expiration of account2
	time.Sleep(3 * time.Second)
	err = dirStore.SaveAcc(pubKey, jwt)
	require_NoError(t, err)
	assertStoreSize(t, dirStore, 2)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey1))
	require_NoError(t, err)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey3))
	require_NoError(t, err)
}

func TestLruLoad(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)

	dirStore, err := NewExpiringDirJWTStore(dir, false, false, time.Millisecond*100, 2, true, 0, nil)
	require_NoError(t, err)
	defer dirStore.Close()

	accountKey1, err := nkeys.CreateAccount()
	require_NoError(t, err)
	pKey1, err := accountKey1.PublicKey()
	require_NoError(t, err)
	accountKey2, err := nkeys.CreateAccount()
	require_NoError(t, err)
	accountKey3, err := nkeys.CreateAccount()
	require_NoError(t, err)
	pKey3, err := accountKey3.PublicKey()
	require_NoError(t, err)

	createTestAccount(t, dirStore, 10, accountKey1)
	assertStoreSize(t, dirStore, 1)
	createTestAccount(t, dirStore, 10, accountKey2)
	assertStoreSize(t, dirStore, 2)
	dirStore.LoadAcc(pKey1) // will reorder 1/2
	createTestAccount(t, dirStore, 10, accountKey3)
	assertStoreSize(t, dirStore, 2)

	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey1))
	require_NoError(t, err)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey3))
	require_NoError(t, err)
}

func TestLru(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)

	dirStore, err := NewExpiringDirJWTStore(dir, false, false, time.Millisecond*100, 2, true, 0, nil)
	require_NoError(t, err)
	defer dirStore.Close()

	accountKey1, err := nkeys.CreateAccount()
	require_NoError(t, err)
	pKey1, err := accountKey1.PublicKey()
	require_NoError(t, err)
	accountKey2, err := nkeys.CreateAccount()
	require_NoError(t, err)
	accountKey3, err := nkeys.CreateAccount()
	require_NoError(t, err)
	pKey3, err := accountKey3.PublicKey()
	require_NoError(t, err)

	createTestAccount(t, dirStore, 10, accountKey1)
	assertStoreSize(t, dirStore, 1)
	createTestAccount(t, dirStore, 10, accountKey2)
	assertStoreSize(t, dirStore, 2)
	createTestAccount(t, dirStore, 10, accountKey3)
	assertStoreSize(t, dirStore, 2)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey1))
	require_True(t, os.IsNotExist(err))
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey3))
	require_NoError(t, err)

	// update -> will change this keys position for eviction
	createTestAccount(t, dirStore, 10, accountKey2)
	assertStoreSize(t, dirStore, 2)
	// recreate -> will evict 3
	createTestAccount(t, dirStore, 1, accountKey1)
	assertStoreSize(t, dirStore, 2)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey3))
	require_True(t, os.IsNotExist(err))
	// let key1 expire
	time.Sleep(2 * time.Second)
	assertStoreSize(t, dirStore, 1)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey1))
	require_True(t, os.IsNotExist(err))
	// recreate key3 - no eviction
	createTestAccount(t, dirStore, 10, accountKey3)
	assertStoreSize(t, dirStore, 2)
}

func TestExpirationUpdate(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)

	dirStore, err := NewExpiringDirJWTStore(dir, false, false, time.Millisecond*100, 10, true, 0, nil)
	require_NoError(t, err)
	defer dirStore.Close()

	accountKey, err := nkeys.CreateAccount()
	require_NoError(t, err)

	h := dirStore.Hash()

	createTestAccount(t, dirStore, 0, accountKey)
	nh := dirStore.Hash()
	require_NotEqual(t, h, nh)
	h = nh

	time.Sleep(2 * time.Second)
	f, err := ioutil.ReadDir(dir)
	require_NoError(t, err)
	require_Len(t, len(f), 1)

	createTestAccount(t, dirStore, 5, accountKey)
	nh = dirStore.Hash()
	require_NotEqual(t, h, nh)
	h = nh

	time.Sleep(2 * time.Second)
	f, err = ioutil.ReadDir(dir)
	require_NoError(t, err)
	require_Len(t, len(f), 1)

	createTestAccount(t, dirStore, 0, accountKey)
	nh = dirStore.Hash()
	require_NotEqual(t, h, nh)
	h = nh

	time.Sleep(2 * time.Second)
	f, err = ioutil.ReadDir(dir)
	require_NoError(t, err)
	require_Len(t, len(f), 1)

	createTestAccount(t, dirStore, 1, accountKey)
	nh = dirStore.Hash()
	require_NotEqual(t, h, nh)
	h = nh

	time.Sleep(2 * time.Second)
	f, err = ioutil.ReadDir(dir)
	require_NoError(t, err)
	require_Len(t, len(f), 0)

	empty := [32]byte{}
	h = dirStore.Hash()
	require_Equal(t, string(h[:]), string(empty[:]))
}

func TestTTL(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)
	require_OneJWT := func() {
		t.Helper()
		f, err := ioutil.ReadDir(dir)
		require_NoError(t, err)
		require_Len(t, len(f), 1)
	}
	dirStore, err := NewExpiringDirJWTStore(dir, false, false, time.Millisecond*100, 10, true, 2*time.Second, nil)
	require_NoError(t, err)
	defer dirStore.Close()

	accountKey, err := nkeys.CreateAccount()
	require_NoError(t, err)
	pubKey, err := accountKey.PublicKey()
	require_NoError(t, err)
	jwt := createTestAccount(t, dirStore, 0, accountKey)
	require_OneJWT()
	for i := 0; i < 6; i++ {
		time.Sleep(time.Second)
		dirStore.LoadAcc(pubKey)
		require_OneJWT()
	}
	for i := 0; i < 6; i++ {
		time.Sleep(time.Second)
		dirStore.SaveAcc(pubKey, jwt)
		require_OneJWT()
	}
	for i := 0; i < 6; i++ {
		time.Sleep(time.Second)
		createTestAccount(t, dirStore, 0, accountKey)
		require_OneJWT()
	}
	time.Sleep(3 * time.Second)
	f, err := ioutil.ReadDir(dir)
	require_NoError(t, err)
	require_Len(t, len(f), 0)
}

func TestNFS(t *testing.T) {
	emptyHash := [sha256.Size]byte{}
	changeChan := make(chan struct{}, 1)
	defer close(changeChan)
	dir, err := ioutil.TempDir(os.TempDir(), "jwtstore_test")
	require_NoError(t, err)
	s, err := NewImmutableDirJWTStore(dir, false, func(pubKey string) {
		changeChan <- struct{}{}
	}, func(err error) {})
	require_NoError(t, err)
	defer s.Close()

	createAccount := func() (string, string, [sha256.Size]byte, string) {
		t.Helper()
		key1, err := nkeys.CreateAccount()
		require_NoError(t, err)
		pKey1, err := key1.PublicKey()
		require_NoError(t, err)
		acc1 := jwt.NewAccountClaims(pKey1) // self signed is fine her
		jwt1, err := acc1.Encode(key1)
		require_NoError(t, err)
		hash := sha256.Sum256([]byte(jwt1))
		return pKey1, jwt1, hash, filepath.Join(dir, pKey1+".jwt")
	}
	writeAccount := func(file string, jwt string) {
		err := ioutil.WriteFile(file, []byte(jwt), 0644)
		require_NoError(t, err)
	}
	removeAccount := func(file string) {
		err = os.Remove(file)
		require_NoError(t, err)
	}

	_, jwt1, hash, file1 := createAccount()
	writeAccount(file1, jwt1)

	<-changeChan
	newHash := s.Hash()
	require_True(t, bytes.Equal(hash[:], newHash[:]))
	removeAccount(file1)
	time.Sleep(4 * time.Second)
	newHash = s.Hash()
	require_True(t, bytes.Equal(emptyHash[:], newHash[:]))

	writeAccount(file1, jwt1)
	_, jwt2, _, file2 := createAccount()
	writeAccount(file2, jwt2)
	_, jwt3, _, file3 := createAccount()
	writeAccount(file3, jwt3)

	<-changeChan
	<-changeChan
	<-changeChan

	removeAccount(file2)
	time.Sleep(4 * time.Second)
	removeAccount(file3)
	time.Sleep(4 * time.Second)
	newHash = s.Hash()
	require_True(t, bytes.Equal(hash[:], newHash[:]))
	removeAccount(file1)
	time.Sleep(4 * time.Second)

	newHash = s.Hash()
	require_True(t, bytes.Equal(emptyHash[:], newHash[:]))
}
