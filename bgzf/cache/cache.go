// Copyright ©2015 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cache provides basic block cache types for the bgzf package.
package cache

import (
	"sync"

	"github.com/biogo/hts/bgzf"
)

var (
	_ Cache = (*LRU)(nil)
	_ Cache = (*FIFO)(nil)
	_ Cache = (*Random)(nil)
)

// Free attempts to drop as many blocks from c as needed allow
// n successful Put calls on c. It returns a boolean indicating
// whether n slots were made available.
func Free(n int, c Cache) bool {
	empty := c.Cap() - c.Len()
	if n <= empty {
		return true
	}
	c.Drop(n - empty)
	return c.Cap()-c.Len() >= n
}

// Cache is an extension of bgzf.Cache that allows inspection
// and manipulation of the cache.
type Cache interface {
	bgzf.Cache

	// Len returns the number of elements held by
	// the cache.
	Len() int

	// Cap returns the maximum number of elements
	// that can be held by the cache.
	Cap() int

	// Resize changes the capacity of the cache to n,
	// dropping excess blocks if n is less than the
	// number of cached blocks.
	Resize(n int)

	// Drop evicts n elements from the cache according
	// to the cache eviction policy.
	Drop(n int)
}

func insertAfter(pos, n *node) {
	n.prev = pos
	pos.next, n.next, pos.next.prev = n, pos.next, n
}

func remove(n *node, table map[int64]*node) {
	delete(table, n.b.Base())
	n.prev.next = n.next
	n.next.prev = n.prev
	n.next = nil
	n.prev = nil
}

// NewLRU returns an LRU cache with n slots. If n is less than 1
// a nil cache is returned.
func NewLRU(n int) Cache {
	if n < 1 {
		return nil
	}
	c := LRU{
		table: make(map[int64]*node, n),
		cap:   n,
	}
	c.root.next = &c.root
	c.root.prev = &c.root
	return &c
}

// LRU satisfies the Cache interface with least recently used eviction
// behavior where Unused Blocks are preferentially evicted.
type LRU struct {
	mu    sync.RWMutex
	root  node
	table map[int64]*node
	cap   int
}

type node struct {
	b bgzf.Block

	next, prev *node
}

// Len returns the number of elements held by the cache.
func (c *LRU) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.table)
}

// Cap returns the maximum number of elements that can be held by the cache.
func (c *LRU) Cap() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.cap
}

// Resize changes the capacity of the cache to n, dropping excess blocks
// if n is less than the number of cached blocks.
func (c *LRU) Resize(n int) {
	c.mu.Lock()
	if n < len(c.table) {
		c.drop(len(c.table) - n)
	}
	c.cap = n
	c.mu.Unlock()
}

// Drop evicts n elements from the cache according to the cache eviction policy.
func (c *LRU) Drop(n int) {
	c.mu.Lock()
	c.drop(n)
	c.mu.Unlock()
}

func (c *LRU) drop(n int) {
	for ; n > 0 && c.Len() > 0; n-- {
		remove(c.root.prev, c.table)
	}
}

// Get returns the Block in the Cache with the specified base or a nil Block
// if it does not exist.
func (c *LRU) Get(base int64) bgzf.Block {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, ok := c.table[base]
	if !ok {
		return nil
	}
	remove(n, c.table)
	return n.b
}

// Peek returns a boolean indicating whether a Block exists in the Cache for
// the given base offset and the expected offset for the subsequent Block in
// the BGZF stream.
func (c *LRU) Peek(base int64) (exist bool, next int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	n, exist := c.table[base]
	if !exist {
		return false, -1
	}
	next = n.b.NextBase()
	return exist, next
}

// Put inserts a Block into the Cache, returning the Block that was evicted or
// nil if no eviction was necessary and the Block was retained. Unused Blocks
// are not retained but are returned if the Cache is full.
func (c *LRU) Put(b bgzf.Block) (evicted bgzf.Block, retained bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var d bgzf.Block
	if _, ok := c.table[b.Base()]; ok {
		return b, false
	}
	used := b.Used()
	if len(c.table) == c.cap {
		if !used {
			return b, false
		}
		d = c.root.prev.b
		remove(c.root.prev, c.table)
	}
	n := &node{b: b}
	c.table[b.Base()] = n
	if used {
		insertAfter(&c.root, n)
	} else {
		insertAfter(c.root.prev, n)
	}
	return d, true
}

// NewFIFO returns a FIFO cache with n slots. If n is less than 1
// a nil cache is returned.
func NewFIFO(n int) Cache {
	if n < 1 {
		return nil
	}
	c := FIFO{
		table: make(map[int64]*node, n),
		cap:   n,
	}
	c.root.next = &c.root
	c.root.prev = &c.root
	return &c
}

// FIFO satisfies the Cache interface with first in first out eviction
// behavior where Unused Blocks are preferentially evicted.
type FIFO struct {
	mu    sync.RWMutex
	root  node
	table map[int64]*node
	cap   int
}

// Len returns the number of elements held by the cache.
func (c *FIFO) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.table)
}

// Cap returns the maximum number of elements that can be held by the cache.
func (c *FIFO) Cap() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.cap
}

// Resize changes the capacity of the cache to n, dropping excess blocks
// if n is less than the number of cached blocks.
func (c *FIFO) Resize(n int) {
	c.mu.Lock()
	if n < len(c.table) {
		c.drop(len(c.table) - n)
	}
	c.cap = n
	c.mu.Unlock()
}

// Drop evicts n elements from the cache according to the cache eviction policy.
func (c *FIFO) Drop(n int) {
	c.mu.Lock()
	c.drop(n)
	c.mu.Unlock()
}

func (c *FIFO) drop(n int) {
	for ; n > 0 && c.Len() > 0; n-- {
		remove(c.root.prev, c.table)
	}
}

// Get returns the Block in the Cache with the specified base or a nil Block
// if it does not exist.
func (c *FIFO) Get(base int64) bgzf.Block {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, ok := c.table[base]
	if !ok {
		return nil
	}
	if !n.b.Used() {
		remove(n, c.table)
	}
	return n.b
}

// Peek returns a boolean indicating whether a Block exists in the Cache for
// the given base offset and the expected offset for the subsequent Block in
// the BGZF stream.
func (c *FIFO) Peek(base int64) (exist bool, next int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	n, exist := c.table[base]
	if !exist {
		return false, -1
	}
	next = n.b.NextBase()
	return exist, next
}

// Put inserts a Block into the Cache, returning the Block that was evicted or
// nil if no eviction was necessary and the Block was retained. Unused Blocks
// are not retained but are returned if the Cache is full.
func (c *FIFO) Put(b bgzf.Block) (evicted bgzf.Block, retained bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var d bgzf.Block
	if _, ok := c.table[b.Base()]; ok {
		return b, false
	}
	used := b.Used()
	if len(c.table) == c.cap {
		if !used {
			return b, false
		}
		d = c.root.prev.b
		remove(c.root.prev, c.table)
	}
	n := &node{b: b}
	c.table[b.Base()] = n
	if used {
		insertAfter(&c.root, n)
	} else {
		insertAfter(c.root.prev, n)
	}
	return d, true
}

// NewRandom returns a random eviction cache with n slots. If n is less than 1
// a nil cache is returned.
func NewRandom(n int) Cache {
	if n < 1 {
		return nil
	}
	return &Random{
		table: make(map[int64]bgzf.Block, n),
		cap:   n,
	}
}

// Random satisfies the Cache interface with random eviction behavior
// where Unused Blocks are preferentially evicted.
type Random struct {
	mu    sync.RWMutex
	table map[int64]bgzf.Block
	cap   int
}

// Len returns the number of elements held by the cache.
func (c *Random) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.table)
}

// Cap returns the maximum number of elements that can be held by the cache.
func (c *Random) Cap() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.cap
}

// Resize changes the capacity of the cache to n, dropping excess blocks
// if n is less than the number of cached blocks.
func (c *Random) Resize(n int) {
	c.mu.Lock()
	if n < len(c.table) {
		c.drop(len(c.table) - n)
	}
	c.cap = n
	c.mu.Unlock()
}

// Drop evicts n elements from the cache according to the cache eviction policy.
func (c *Random) Drop(n int) {
	c.mu.Lock()
	c.drop(n)
	c.mu.Unlock()
}

func (c *Random) drop(n int) {
	if n < 1 {
		return
	}
	for k, b := range c.table {
		if b.Used() {
			continue
		}
		delete(c.table, k)
		if n--; n == 0 {
			return
		}
	}
	for k := range c.table {
		delete(c.table, k)
		if n--; n == 0 {
			break
		}
	}
}

// Get returns the Block in the Cache with the specified base or a nil Block
// if it does not exist.
func (c *Random) Get(base int64) bgzf.Block {
	c.mu.Lock()
	defer c.mu.Unlock()

	b, ok := c.table[base]
	if !ok {
		return nil
	}
	delete(c.table, base)
	return b
}

// Peek returns a boolean indicating whether a Block exists in the Cache for
// the given base offset and the expected offset for the subsequent Block in
// the BGZF stream.
func (c *Random) Peek(base int64) (exist bool, next int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	n, exist := c.table[base]
	if !exist {
		return false, -1
	}
	next = n.NextBase()
	return exist, next
}

// Put inserts a Block into the Cache, returning the Block that was evicted or
// nil if no eviction was necessary and the Block was retained. Unused Blocks
// are not retained but are returned if the Cache is full.
func (c *Random) Put(b bgzf.Block) (evicted bgzf.Block, retained bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var d bgzf.Block
	if _, ok := c.table[b.Base()]; ok {
		return b, false
	}
	if len(c.table) == c.cap {
		if !b.Used() {
			return b, false
		}
		for k, v := range c.table {
			if v.Used() {
				continue
			}
			delete(c.table, k)
			d = v
			goto done
		}
		for k, v := range c.table {
			delete(c.table, k)
			d = v
			break
		}
	done:
	}
	c.table[b.Base()] = b
	return d, true
}

// StatsRecorder allows a bgzf.Cache to capture cache statistics.
type StatsRecorder struct {
	bgzf.Cache

	mu    sync.RWMutex
	stats Stats
}

// Stats represents statistics of a bgzf.Cache.
type Stats struct {
	Gets      int // number of Get operations
	Misses    int // number of cache misses
	Puts      int // number of Put operations
	Retains   int // number of times a Put has resulted in Block retention
	Evictions int // number of times a Put has resulted in a Block eviction
}

// Stats returns the current statistics for the cache.
func (s *StatsRecorder) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

// Reset zeros the statistics kept by the StatsRecorder.
func (s *StatsRecorder) Reset() {
	s.mu.Lock()
	s.stats = Stats{}
	s.mu.Unlock()
}

// Get returns the Block in the underlying Cache with the specified base or a nil
// Block if it does not exist. It updates the gets and misses statistics.
func (s *StatsRecorder) Get(base int64) bgzf.Block {
	s.mu.Lock()
	s.stats.Gets++
	blk := s.Cache.Get(base)
	if blk == nil {
		s.stats.Misses++
	}
	s.mu.Unlock()
	return blk
}

// Put inserts a Block into the underlying Cache, returning the Block and eviction
// status according to the underlying cache behavior. It updates the puts, retains and
// evictions statistics.
func (s *StatsRecorder) Put(b bgzf.Block) (evicted bgzf.Block, retained bool) {
	s.mu.Lock()
	s.stats.Puts++
	blk, retained := s.Cache.Put(b)
	if retained {
		s.stats.Retains++
		if blk != nil {
			s.stats.Evictions++
		}
	}
	s.mu.Unlock()
	return blk, retained
}
