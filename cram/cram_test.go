// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cram

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"

	"github.com/kortschak/utter"
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
	var b block
	err = b.readFrom(c.blockData)
	if err != nil {
		t.Errorf("failed to read block: %v\n%#v", err, b)
	}

	c.blockData = nil
	wantContainer := container{
		blockLen:  15,
		refID:     -1,
		start:     4542278,
		span:      0,
		nRec:      0,
		recCount:  0,
		bases:     0,
		blocks:    1,
		landmarks: nil,
		crc32:     0x4fd9bd05,
	}
	if !reflect.DeepEqual(c, wantContainer) {
		t.Errorf("unexpected EOF container value:\ngot: %#v\nwant:%#v", c, wantContainer)
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

func TestHasEOF(t *testing.T) {
	r, err := get(`https://github.com/samtools/htslib/blob/develop/test/ce%235b_java.cram?raw=true`)
	if err != nil {
		t.Fatalf("failed to open test file: %v", err)
	}
	hasEOF, err := HasEOF(r)
	if err != nil {
		t.Fatalf("failed to read EOF: %v", err)
	}
	if !hasEOF {
		t.Error("failed to identify known EOF block")
	}
}

func TestRead(t *testing.T) {
	utter.Config.BytesWidth = 8

	r, err := get(`https://github.com/samtools/htslib/blob/develop/test/ce%235b_java.cram?raw=true`)
	if err != nil {
		t.Fatalf("failed to open test file: %v", err)
	}

	var d definition
	err = d.readFrom(r)
	if err != nil {
		t.Fatalf("failed to read definition: %v\n%#v", err, d)
	}
	wantDefinition := definition{
		Magic: [4]uint8{
			0x43, 0x52, 0x41, 0x4d, // |CRAM|
		},
		Version: [2]byte{3, 0},
		ID: [20]byte{
			0x63, 0x65, 0x23, 0x35, 0x62, 0x2e, 0x73, 0x61, // |ce#5b.sa|
			0x6d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // |m.......|
			0x00, 0x00, 0x00, 0x00, /*                   */ // |....|
		},
	}
	t.Log(utter.Sdump(d))
	if d != wantDefinition {
		t.Errorf("unexpected cram definition:\ngot: %#v\nwant:%#v", d, wantDefinition)
	}

	for {
		var c container
		err = c.readFrom(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("failed to read container: %v\n%#v", err, c)
		}
		blockData := c.blockData
		c.blockData = nil
		t.Log(utter.Sdump(c))

		for {
			b := &block{}
			err = b.readFrom(blockData)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Errorf("failed to read block: %v\n%#v", err, b)
			}
			v, err := b.value()
			if err != nil {
				t.Errorf("failed to get value: %v", err)
			}
			t.Log(utter.Sdump(v))
		}
	}
}

func get(url string) (*bytes.Reader, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}
