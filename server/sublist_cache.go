// Copyright 2016-2023 The NATS Authors
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
	"sync/atomic"
	"unsafe"
)

type Cache struct {
	data     [slCacheMax]atomic.Pointer[cacheEntry]
	hashFunc func(string) int
}

type cacheEntry struct {
	key   string
	value *SublistResult
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// MemHashString is the hash function used by go map, it utilizes available hardware instructions
func MemHashString(str string) int {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return int(uint64(memhash(ss.str, 0, uintptr(ss.len))) % slCacheMax)
}

func NewCache(hashFunc func(string) int) *Cache {
	return &Cache{
		hashFunc: hashFunc,
	}
}

func (c *Cache) Set(key string, value *SublistResult) {
	if c == nil {
		return
	}

	index := c.hashFunc(key)
	entry := &cacheEntry{key: key, value: value}
	c.data[index].Store(entry)
}

func (c *Cache) Get(key string) (*SublistResult, bool) {
	if c == nil {
		return nil, false
	}

	index := c.hashFunc(key)
	entry := c.data[index].Load()
	if entry != nil {
		if entry.key == key {
			return entry.value, true
		}
	}
	return nil, false
}

func (c *Cache) Delete(key string) {
	index := c.hashFunc(key)
	c.data[index].Store(nil)
}

func (c *Cache) Len() int {
	var cc int
	c.Iterate(func(key string, value *SublistResult) {
		cc++
	})
	return cc
}

func (c *Cache) Iterate(cf func(key string, value *SublistResult)) {
	if c == nil {
		return
	}

	for i := range c.data {
		p := c.data[i].Load()
		if p != nil {
			cf(p.key, p.value)
		}
	}
}
