// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cram

import (
	"bytes"
	"reflect"
	"testing"
)

func TestReadEOFContainer(t *testing.T) {
	r := bytes.NewReader(cramEOFmarker)
	var got container
	err := got.readFrom(r)
	if err != nil {
		t.Errorf("failed to read container: %v\n%#v", err, got)
	}

	want := container{
		blockLen:  15,
		refID:     -1,
		start:     4542278,
		span:      0,
		nRec:      0,
		recCount:  0,
		bases:     0,
		blocks:    1,
		landmarks: []int32{0},
		crc32:     0x4fd9bd05,
		blockData: []byte{0x0, 0x1, 0x0, 0x6, 0x6, 0x1, 0x0, 0x1, 0x0, 0x1, 0x0, 0xee, 0x63, 0x1, 0x4b},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("unexpected EOF contained value:\ngot: %#v\nwant:%#v", got, want)
	}
}
