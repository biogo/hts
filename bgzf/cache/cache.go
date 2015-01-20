// Copyright ©2015 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cache provides basic block cache types for the bgzf package.
package cache

import (
	"code.google.com/p/biogo.bam/bgzf"
)

var (
	_ bgzf.Cache = (*LRU)(nil)
	_ bgzf.Cache = (*FIFO)(nil)
	_ bgzf.Cache = (*Random)(nil)
)

// NewLRU returns an LRU cache with the n slots. If n is less than 1
// a nil cache is returned.
func NewLRU(n int) *LRU {
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

// LRU satisfies the bgzf.Cache interface with least recently used eviction
// behavior.
type LRU struct {
	root  node
	table map[int64]*node
	cap   int
}

type node struct {
	b bgzf.Block

	next, prev *node
}

// Len returns the number of elements held by the cache.
func (c *LRU) Len() int { return len(c.table) }

// Cap returns the maximum number of elements that can be held by the cache.
func (c *LRU) Cap() int { return c.cap }

// Drop evicts n elements from the cache according to the cache eviction policy.
func (c *LRU) Drop(n int) {
	for ; n > 0 && c.Len() > 0; n-- {
		c.remove(c.root.prev)
	}
}

// Get returns the Block in the Cache with the specified base or a nil Block
// if it does not exist.
func (c *LRU) Get(base int64) bgzf.Block {
	n, ok := c.table[base]
	if !ok {
		return nil
	}
	c.remove(n)
	return n.b
}

// Put inserts a Block into the Cache, returning the Block that was evicted or
// nil if no eviction was necessary.
func (c *LRU) Put(b bgzf.Block) bgzf.Block {
	var d bgzf.Block
	if len(c.table) == c.cap {
		d = c.root.prev.b
		c.remove(c.root.prev)
	}
	n := &node{b: b}
	c.table[b.Base()] = n
	f := c.root.next
	c.root.next = n
	n.prev = &c.root
	n.next = f
	f.prev = n
	return d
}

func (c *LRU) remove(n *node) {
	delete(c.table, n.b.Base())
	n.prev.next = n.next
	n.next.prev = n.prev
	n.next = nil
	n.prev = nil
}

// NewLRU returns a FIFO cache with the n slots. If n is less than 1
// a nil cache is returned.
func NewFIFO(n int) *FIFO {
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

// FIFO satisfies the bgzf.Cache interface with first in first out eviction
// behavior.
type FIFO struct {
	root  node
	table map[int64]*node
	cap   int
}

// Len returns the number of elements held by the cache.
func (c *FIFO) Len() int { return len(c.table) }

// Cap returns the maximum number of elements that can be held by the cache.
func (c *FIFO) Cap() int { return c.cap }

// Drop evicts n elements from the cache according to the cache eviction policy.
func (c *FIFO) Drop(n int) {
	for ; n > 0 && c.Len() > 0; n-- {
		c.remove(c.root.prev)
	}
}

// Get returns the Block in the Cache with the specified base or a nil Block
// if it does not exist.
func (c *FIFO) Get(base int64) bgzf.Block {
	n, ok := c.table[base]
	if !ok {
		return nil
	}
	return n.b
}

// Put inserts a Block into the Cache, returning the Block that was evicted or
// nil if no eviction was necessary.
func (c *FIFO) Put(b bgzf.Block) bgzf.Block {
	var d bgzf.Block
	if _, ok := c.table[b.Base()]; ok {
		return nil
	}
	if len(c.table) == c.cap {
		d = c.root.prev.b
		c.remove(c.root.prev)
	}
	n := &node{b: b}
	c.table[b.Base()] = n
	f := c.root.next
	c.root.next = n
	n.prev = &c.root
	n.next = f
	f.prev = n
	return d
}

func (c *FIFO) remove(n *node) {
	delete(c.table, n.b.Base())
	n.prev.next = n.next
	n.next.prev = n.prev
	n.next = nil
	n.prev = nil
}

// NewLRU returns a random eviction cache with the n slots. If n is less than 1
// a nil cache is returned.
func NewRandom(n int) *Random {
	if n < 1 {
		return nil
	}
	return &Random{
		table: make(map[int64]bgzf.Block, n),
		cap:   n,
	}
}

// Random satisfies the bgzf.Cache interface with random eviction behavior.
type Random struct {
	table map[int64]bgzf.Block
	cap   int
}

// Len returns the number of elements held by the cache.
func (c *Random) Len() int { return len(c.table) }

// Cap returns the maximum number of elements that can be held by the cache.
func (c *Random) Cap() int { return c.cap }

// Drop evicts n elements from the cache according to the cache eviction policy.
func (c *Random) Drop(n int) {
	if n < 1 {
		return
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
	b, ok := c.table[base]
	if !ok {
		return nil
	}
	delete(c.table, base)
	return b
}

// Put inserts a Block into the Cache, returning the Block that was evicted or
// nil if no eviction was necessary.
func (c *Random) Put(b bgzf.Block) bgzf.Block {
	var d bgzf.Block
	if len(c.table) == c.cap {
		for k, v := range c.table {
			delete(c.table, k)
			d = v
			break
		}
	}
	c.table[b.Base()] = b
	return d
}
