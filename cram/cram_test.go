// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cram

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestReadDefinition(t *testing.T) {
	tests := []struct {
		bytes [26]byte
		want  definition
		err   error
	}{
		{
			bytes: [26]byte{
				'C', 'R', 'A', 'M',
				3,
				0,
				's', 'h', 'a', '1', '-', '0',
			},
			want: definition{
				Magic:   [4]byte{'C', 'R', 'A', 'M'},
				Version: [2]byte{3, 0},
				ID:      [20]byte{'s', 'h', 'a', '1', '-', '0'},
			},
			err: nil,
		},
		{
			bytes: [26]byte{
				'B', 'A', 'M', 0x1,
				3,
				0,
				's', 'h', 'a', '1', '-', '0',
			},
			want: definition{
				Magic:   [4]byte{'B', 'A', 'M', 0x1},
				Version: [2]byte{3, 0},
				ID:      [20]byte{'s', 'h', 'a', '1', '-', '0'},
			},
			err: errors.New(`cram: not a cram file: magic bytes "BAM\x01"`),
		},
	}
	for _, test := range tests {
		var got definition
		err := got.readFrom(bytes.NewReader(test.bytes[:]))
		if fmt.Sprint(err) != fmt.Sprint(test.err) {
			t.Errorf("unexpected error return: got: %q want: %q", err, test.err)
		}

		if got != test.want {
			t.Errorf("unexpected cram definition value:\ngot: %#v\nwant:%#v", got, test.want)
		}
	}
}

func TestReadEOFContainer(t *testing.T) {
	var c container
	err := c.readFrom(bytes.NewReader(cramEOFmarker))
	if err != nil {
		t.Errorf("failed to read container: %v\n%#v", err, c)
	}

	wantContainer := container{
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
	if !reflect.DeepEqual(c, wantContainer) {
		t.Errorf("unexpected EOF container value:\ngot: %#v\nwant:%#v", c, wantContainer)
	}

	var b block
	err = b.readFrom(bytes.NewReader(c.blockData))
	if err != nil {
		t.Errorf("failed to read block: %v\n%#v", err, b)
	}

	wantBlock := block{
		method:         rawMethod,
		typ:            compressionHeader,
		contentID:      0,
		compressedSize: 6,
		rawSize:        6,
		blockData:      []byte{0x01, 0x00, 0x01, 0x00, 0x01, 0x00},
		crc32:          0x4b0163ee,
	}
	if !reflect.DeepEqual(b, wantBlock) {
		t.Errorf("unexpected EOF block value:\ngot: %#v\nwant:%#v", b, wantBlock)
	}
}
