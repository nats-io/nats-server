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
	"container/heap"
	"container/list"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/jwt/v2" // only used to decode, not for storage
)

const (
	extension = "jwt"
)

// validatePathExists checks that the provided path exists and is a dir if requested
func validatePathExists(path string, dir bool) (string, error) {
	if path == "" {
		return "", errors.New("path is not specified")
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("error parsing path [%s]: %v", abs, err)
	}

	var finfo os.FileInfo
	if finfo, err = os.Stat(abs); os.IsNotExist(err) {
		return "", fmt.Errorf("the path [%s] doesn't exist", abs)
	}

	mode := finfo.Mode()
	if dir && mode.IsRegular() {
		return "", fmt.Errorf("the path [%s] is not a directory", abs)
	}

	if !dir && mode.IsDir() {
		return "", fmt.Errorf("the path [%s] is not a file", abs)
	}

	return abs, nil
}

// ValidateDirPath checks that the provided path exists and is a dir
func validateDirPath(path string) (string, error) {
	return validatePathExists(path, true)
}

// JWTChanged functions are called when the store file watcher notices a JWT changed
type JWTChanged func(publicKey string)

// JWTError functions are called when the store file watcher has an error
type JWTError func(err error)

// DirJWTStore implements the JWT Store interface, keeping JWTs in an optionally sharded
// directory structure
type DirJWTStore struct {
	sync.Mutex
	directory     string
	readonly      bool
	shard         bool
	expiration    *ExpirationTracker
	changed       JWTChanged
	errorOccurred JWTError
	done          sync.WaitGroup
}

func newDir(dirPath string, create bool) (string, error) {
	fullPath, err := validateDirPath(dirPath)
	if err != nil {
		if !create {
			return "", err
		}
		if err = os.MkdirAll(dirPath, 0755); err != nil {
			return "", err
		}
		if fullPath, err = validateDirPath(dirPath); err != nil {
			return "", err
		}
	}
	return fullPath, nil
}

func NewExpiringDirJWTStore(dirPath string, shard bool, create bool, expireCheck time.Duration, limit int64,
	evictOnLimit bool, ttl time.Duration, changeNotification JWTChanged) (*DirJWTStore, error) {
	fullPath, err := newDir(dirPath, create)
	if err != nil {
		return nil, err
	}
	theStore := &DirJWTStore{
		directory: fullPath,
		shard:     shard,
		changed:   changeNotification,
	}
	if expireCheck <= 0 {
		if ttl != 0 {
			expireCheck = ttl / 2
		}
		if expireCheck == 0 || expireCheck > time.Minute {
			expireCheck = time.Minute
		}
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	theStore.startExpiring(expireCheck, limit, evictOnLimit, ttl)
	theStore.Lock()
	err = filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if strings.HasSuffix(path, extension) {
			if theJwt, err := ioutil.ReadFile(path); err == nil {
				hash := sha256.Sum256(theJwt)
				_, file := filepath.Split(path)
				theStore.expiration.Track(strings.TrimSuffix(file, "."+extension), &hash, string(theJwt))
			}
		}
		return nil
	})
	theStore.Unlock()
	if err != nil {
		theStore.Close()
		return nil, err
	}
	theStore.readonly = false
	return theStore, err
}

// NewDirJWTStore returns an empty, mutable directory-based JWT store
func NewDirJWTStore(dirPath string, shard bool, create bool, changeNotification JWTChanged, errorNotification JWTError) (*DirJWTStore, error) {
	// Set evictOnLimit to false and set a limit that can't be meet.
	// This will cause LRU to not be updated, so it can be used as a list to iterate and find deleted files
	// Set expiration check to never happen
	st, err := NewExpiringDirJWTStore(dirPath, shard, create, time.Duration(math.MaxInt64), math.MaxInt64, false, time.Duration(math.MaxInt64), nil)
	if err != nil {
		return nil, err
	}
	st.Lock()
	st.changed = changeNotification
	st.errorOccurred = errorNotification
	st.Unlock()
	if err := st.startWatching(); err != nil {
		st.Close()
		return nil, err
	} else {
		return st, nil
	}
}

// NewImmutableDirJWTStore returns an immutable store with the provided directory
func NewImmutableDirJWTStore(dirPath string, shard bool, changeNotification JWTChanged, errorNotification JWTError) (*DirJWTStore, error) {
	st, err := NewDirJWTStore(dirPath, shard, false, changeNotification, errorNotification)
	if st == nil {
		st.readonly = true
	}
	return st, err
}

// Add file to index or update it.
// Assumes lock is NOT held
func (store *DirJWTStore) addFile(path string) (bool, error) {
	store.Lock()
	defer store.Unlock()
	if !strings.HasSuffix(path, extension) {
		return false, nil
	}
	if store.expiration == nil {
		return false, nil
	}
	pubKey := strings.Replace(filepath.Base(path), ".jwt", "", -1)
	theJWT, err := ioutil.ReadFile(path)
	if err != nil {
		return false, err
	}
	hash := sha256.Sum256(theJWT)
	if i, ok := store.expiration.idx[pubKey]; ok {
		hash := sha256.Sum256(theJWT)
		if bytes.Equal(hash[:], i.Value.(*JWTItem).hash[:]) {
			return false, nil
		}
		// to modify remove first
		store.expiration.UnTrack(pubKey)
	}
	store.expiration.Track(pubKey, &hash, string(theJWT))
	return true, nil
}

func (store *DirJWTStore) startWatching() error {
	store.Lock()
	defer store.Unlock()
	// Watch the top level dir (could be sharded)
	err := filepath.Walk(store.directory, func(path string, info os.FileInfo, err error) error {
		if strings.HasSuffix(path, extension) && filepath.Dir(path) == store.directory {
			store.addFile(path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	quitError := errors.New("abort")
	errCb := store.errorOccurred
	chCb := store.changed
	wg := store.done
	quit := store.expiration.quit
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-time.After(time.Second):
			case <-quit:
				return
			}
			// check for additions or modifications
			err := filepath.Walk(store.directory, func(path string, info os.FileInfo, err error) (retErr error) {
				if err != nil {
					return err
				}
				if strings.HasSuffix(path, extension) && filepath.Dir(path) == store.directory {
					if changed, err := store.addFile(path); err != nil {
						errCb(err)
					} else if changed {
						pubKey := strings.Replace(filepath.Base(path), ".jwt", "", -1)
						chCb(pubKey)
					}
				}
				select {
				case <-time.After(time.Second):
				case <-quit:
					return quitError
				}
				return
			})
			if err != nil && err != quitError {
				errCb(err)
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-time.After(time.Second):
			case <-quit:
				return
			}
			store.Lock() // access of e is save, pop will nil out next
			for e := store.expiration.lru.Front(); e != nil; e = e.Next() {
				pubKey := e.Value.(*JWTItem).publicKey
				path := store.pathForKey(pubKey)
				if _, err := os.Stat(path); os.IsNotExist(err) {
					store.expiration.UnTrack(pubKey)
				}
				store.Unlock()
				select {
				case <-time.After(time.Second):
				case <-quit:
					return
				}
				store.Lock()
			}
			store.Unlock()
		}
	}()
	return nil
}

// Load checks the memory store and returns the matching JWT or an error
// Assumes lock is NOT held
func (store *DirJWTStore) load(publicKey string) (string, error) {
	store.Lock()
	defer store.Unlock()
	if path := store.pathForKey(publicKey); path == "" {
		return "", fmt.Errorf("invalid public key")
	} else if data, err := ioutil.ReadFile(path); err != nil {
		return "", err
	} else {
		if store.expiration != nil {
			store.expiration.UpdateTrack(publicKey)
		}
		return string(data), nil
	}
}

// Save puts the JWT in a map by public key and performs update callbacks
// Assumes lock is NOT held
func (store *DirJWTStore) save(publicKey string, theJWT string) error {
	store.Lock()
	if store.readonly {
		store.Unlock()
		return fmt.Errorf("store is read-only")
	}
	path := store.pathForKey(publicKey)
	if path == "" {
		store.Unlock()
		return fmt.Errorf("invalid public key")
	}
	dirPath := filepath.Dir(path)
	if _, err := validateDirPath(dirPath); err != nil {
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			store.Unlock()
			return err
		}
	}
	changed, err := store.write(path, publicKey, theJWT)
	cb := store.changed
	store.Unlock()
	if changed && cb != nil {
		cb(publicKey)
	}
	return err
}

func (store *DirJWTStore) LoadAcc(publicKey string) (string, error) {
	return store.load(publicKey)
}

func (store *DirJWTStore) SaveAcc(publicKey string, theJWT string) error {
	return store.save(publicKey, theJWT)
}

func (store *DirJWTStore) LoadAct(hash string) (string, error) {
	return store.load(hash)
}

func (store *DirJWTStore) SaveAct(hash string, theJWT string) error {
	return store.save(hash, theJWT)
}

// IsReadOnly returns a flag determined at creation time
func (store *DirJWTStore) IsReadOnly() bool {
	return store.readonly
}

func (store *DirJWTStore) pathForKey(publicKey string) string {
	if len(publicKey) < 2 {
		return ""
	}
	var dirPath string
	if store.shard {
		last := publicKey[len(publicKey)-2:]
		fileName := fmt.Sprintf("%s.%s", publicKey, extension)
		dirPath = filepath.Join(store.directory, last, fileName)
	} else {
		fileName := fmt.Sprintf("%s.%s", publicKey, extension)
		dirPath = filepath.Join(store.directory, fileName)
	}
	return dirPath
}

// Close is a no-op for a directory store
func (store *DirJWTStore) Close() {
	store.Lock()
	defer store.Unlock()
	if store.expiration != nil {
		store.expiration.Close()
	}
	store.done.Wait()
	store.expiration = nil
}

// Pack up to maxJWTs into a package
// TODO this can be extended and be made resumable
func (store *DirJWTStore) Pack(maxJWTs int) (string, error) {
	count := 0
	var pack []string
	if maxJWTs > 0 {
		pack = make([]string, 0, maxJWTs)
	} else {
		pack = []string{}
	}
	store.Lock()
	err := filepath.Walk(store.directory, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && strings.HasSuffix(path, extension) { // this is a JWT
			if count == maxJWTs { // won't match negative
				return nil
			}
			pubKey := filepath.Base(path)
			pubKey = pubKey[0:strings.Index(pubKey, ".")]
			if store.expiration != nil {
				if _, ok := store.expiration.idx[pubKey]; !ok {
					return nil // only include indexed files
				}
			}
			jwtBytes, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}
			if store.expiration != nil {
				claim, err := jwt.DecodeGeneric(string(jwtBytes))
				if err == nil && claim.Expires > 0 && claim.Expires < time.Now().Unix() {
					return nil
				}
			}
			pack = append(pack, fmt.Sprintf("%s|%s", pubKey, string(jwtBytes)))
			count++
		}
		return nil
	})
	store.Unlock()
	if err != nil {
		return "", err
	} else {
		return strings.Join(pack, "\n"), nil
	}
}

// Merge takes the JWTs from package and adds them to the store
// Merge is destructive in the sense that it doesn't check if the JWT
// is newer or anything like that.
func (store *DirJWTStore) Merge(pack string) error {
	newJWTs := strings.Split(pack, "\n")
	for _, line := range newJWTs {
		if line == "" { // ignore blank lines
			continue
		}
		split := strings.Split(line, "|")
		if len(split) != 2 {
			return fmt.Errorf("line in package didn't contain 2 entries: %q", line)
		}
		pubKey := split[0]
		if err := store.saveIfNewer(pubKey, split[1]); err != nil {
			return err
		}
	}
	return nil
}

// Assumes the lock is NOT held, and only updates if the jwt is new, or the one on disk is older
// returns true when the jwt changed
func (store *DirJWTStore) saveIfNewer(publicKey string, theJWT string) error {
	path := store.pathForKey(publicKey)
	if path == "" {
		return fmt.Errorf("invalid public key")
	}
	dirPath := filepath.Dir(path)
	if _, err := validateDirPath(dirPath); err != nil {
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return err
		}
	}
	if _, err := os.Stat(path); err == nil {
		if newJWT, err := jwt.DecodeGeneric(theJWT); err != nil {
			return err
		} else if existing, err := ioutil.ReadFile(path); err != nil {
			return err
		} else if existingJWT, err := jwt.DecodeGeneric(string(existing)); err != nil {
			return err
		} else if existingJWT.ID == newJWT.ID {
			return nil
		} else if existingJWT.IssuedAt > newJWT.IssuedAt {
			return nil
		}
	}
	store.Lock()
	cb := store.changed
	changed, err := store.write(path, publicKey, theJWT)
	store.Unlock()
	if err != nil {
		return err
	} else if changed && cb != nil {
		cb(publicKey)
	}
	return nil
}

func xorAssign(lVal *[sha256.Size]byte, rVal [sha256.Size]byte) {
	for i := range rVal {
		(*lVal)[i] ^= rVal[i]
	}
}

// write that keeps hash of all jwt in sync
// Assumes the lock is held
func (store *DirJWTStore) write(path string, publicKey string, theJWT string) (bool, error) {
	var newHash *[sha256.Size]byte
	if store.expiration != nil {
		h := sha256.Sum256([]byte(theJWT))
		newHash = &h
		if v, ok := store.expiration.idx[publicKey]; ok {
			store.expiration.UpdateTrack(publicKey)
			// this write is an update, move to back
			it := v.Value.(*JWTItem)
			oldHash := it.hash[:]
			if bytes.Equal(oldHash, newHash[:]) {
				return false, nil
			}
		} else if int64(store.expiration.Len()) >= store.expiration.limit {
			if !store.expiration.evictOnLimit {
				return false, errors.New("jwt store is full")
			}
			// this write is an add, pick the least recently used value for removal
			i := store.expiration.lru.Front().Value.(*JWTItem)
			if err := os.Remove(store.pathForKey(i.publicKey)); err != nil {
				return false, err
			} else {
				store.expiration.UnTrack(i.publicKey)
			}
		}
	}
	if err := ioutil.WriteFile(path, []byte(theJWT), 0644); err != nil {
		return false, err
	} else if store.expiration != nil {
		store.expiration.Track(publicKey, newHash, theJWT)
	}
	return true, nil
}

func (store *DirJWTStore) Hash() [sha256.Size]byte {
	store.Lock()
	defer store.Unlock()
	if store.expiration == nil {
		return [sha256.Size]byte{}
	}
	return store.expiration.hash
}

// An JWTItem is something managed by the priority queue
type JWTItem struct {
	index      int
	publicKey  string
	expiration int64 // consists of unix time of expiration (ttl when set or jwt expiration) in seconds
	hash       [sha256.Size]byte
}

// A ExpirationTracker implements heap.Interface and holds Items.
type ExpirationTracker struct {
	heap         []*JWTItem
	idx          map[string]*list.Element
	lru          *list.List // keep which jwt are least used
	limit        int64      // limit how many jwt are being tracked
	evictOnLimit bool       // when limit is hit, error or evict using lru
	ttl          time.Duration
	hash         [sha256.Size]byte // xor of all JWTItem.hash in idx
	quit         chan struct{}
}

func (pq *ExpirationTracker) Len() int { return len(pq.heap) }

func (q *ExpirationTracker) Less(i, j int) bool {
	pq := q.heap
	return pq[i].expiration < pq[j].expiration
}

func (q *ExpirationTracker) Swap(i, j int) {
	pq := q.heap
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (q *ExpirationTracker) Push(x interface{}) {
	n := len(q.heap)
	item := x.(*JWTItem)
	item.index = n
	q.heap = append(q.heap, item)
	q.idx[item.publicKey] = q.lru.PushBack(item)
}

func (q *ExpirationTracker) Pop() interface{} {
	old := q.heap
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.index = -1
	q.heap = old[0 : n-1]
	q.lru.Remove(q.idx[item.publicKey])
	delete(q.idx, item.publicKey)
	return item
}

func (pq *ExpirationTracker) UpdateTrack(publicKey string) {
	if e, ok := pq.idx[publicKey]; ok {
		i := e.Value.(*JWTItem)
		if pq.ttl != 0 {
			// only update expiration when set
			i.expiration = time.Now().Add(pq.ttl).Unix()
			heap.Fix(pq, i.index)
		}
		if !pq.evictOnLimit {
			pq.lru.MoveToBack(e)
		}
	}
}

func (pq *ExpirationTracker) UnTrack(publicKey string) {
	if it, ok := pq.idx[publicKey]; ok {
		xorAssign(&pq.hash, it.Value.(*JWTItem).hash)
		heap.Remove(pq, it.Value.(*JWTItem).index)
		delete(pq.idx, publicKey)
	}
}

func (pq *ExpirationTracker) Track(publicKey string, hash *[sha256.Size]byte, theJWT string) {
	if g, err := jwt.DecodeGeneric(theJWT); err == nil && g != nil {
		var exp int64
		// prioritize ttl over expiration
		if pq.ttl != 0 {
			if pq.ttl == time.Duration(math.MaxInt64) {
				exp = math.MaxInt64
			} else {
				exp = time.Now().Add(pq.ttl).Unix()
			}
		} else if g.Expires != 0 {
			exp = g.Expires
		} else {
			exp = math.MaxInt64 // default to indefinite
		}
		if e, ok := pq.idx[publicKey]; ok {
			i := e.Value.(*JWTItem)
			xorAssign(&pq.hash, i.hash) // remove old hash
			i.expiration = exp
			i.hash = *hash
			heap.Fix(pq, i.index)
		} else {
			heap.Push(pq, &JWTItem{-1, publicKey, exp, *hash})
		}
		xorAssign(&pq.hash, *hash) // add in new hash
	}
}

func (pq *ExpirationTracker) PopItem() *JWTItem {
	return heap.Pop(pq).(*JWTItem)
}

func (pq *ExpirationTracker) Close() {
	if pq == nil || pq.quit == nil {
		return
	}
	close(pq.quit)
	pq.quit = nil
}

func (store *DirJWTStore) startExpiring(reCheck time.Duration, limit int64, evictOnLimit bool, ttl time.Duration) {
	store.Lock()
	defer store.Unlock()
	wg := store.done
	quit := make(chan struct{})
	pq := &ExpirationTracker{
		make([]*JWTItem, 0, 10),
		make(map[string]*list.Element),
		list.New(),
		limit,
		evictOnLimit,
		ttl,
		[sha256.Size]byte{},
		quit,
	}
	store.expiration = pq
	wg.Add(1)
	go func() {
		t := time.NewTicker(reCheck)
		defer t.Stop()
		defer wg.Done()
		for {
			now := time.Now()
			store.Lock()
			if pq.Len() > 0 {
				if it := pq.PopItem(); it.expiration <= now.Unix() {
					path := store.pathForKey(it.publicKey)
					if err := os.Remove(path); err != nil {
						heap.Push(pq, it) // retry later
					} else {
						pq.UnTrack(it.publicKey)
						xorAssign(&pq.hash, it.hash)
						store.Unlock()
						continue // we removed an entry, check next one
					}
				} else {
					heap.Push(pq, it)
				}
			}
			store.Unlock()
			select {
			case <-t.C:
			case <-quit:
				return
			}
		}
	}()
}
