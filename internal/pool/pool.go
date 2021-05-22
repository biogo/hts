// Copyright ©2021 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pool

import (
	"math/bits"
	"sync"
)

// pool contains size stratified []byte pools. Each pool element i
// returns sized matrices with a slice capped at 1<<i.
var pool [63]sync.Pool

func init() {
	for i := range pool {
		l := 1 << uint(i)
		// Real matrix pools.
		pool[i].New = func() interface{} {
			return make([]byte, l)
		}
	}
}

// GetBuffer returns a []byte of with len size and a cap that is
// less than 2*size.
func GetBuffer(size int) []byte {
	if size == 0 {
		return nil
	}
	b := pool[poolFor(uint(size))].Get().([]byte)
	return b[:size]
}

// PutBuffer replaces a used []byte into the appropriate size
// buffer pool.
func PutBuffer(buf []byte) {
	if buf == nil {
		return
	}
	pool[poolFor(uint(cap(buf)))].Put(buf[:0])
}

// poolFor returns the ceiling of base 2 log of size. It provides an index
// into a pool array to a sync.Pool that will return values able to hold
// size elements.
func poolFor(size uint) int {
	return bits.Len(size - 1)
}
