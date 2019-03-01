// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package itf8

import "testing"

func TestRoundTrip(t *testing.T) {
	b := make([]byte, 6)
	for i := uint(0); i < 32; i++ {
		for off := -1; off <= 1; off++ {
			in := int32(1<<i + off)
			inn := Encode(b, in)
			wantn := Len(in)
			if wantn != inn {
				t.Errorf("disagreement in number of encoded bytes required: want=%d need=%d", wantn, inn)
			}
			out, outn, ok := Decode(b)
			if !ok {
				t.Errorf("failed to decode ITF-8 bytes: %08b", b[:inn])
			}
			if inn != outn {
				t.Errorf("disagreement in number of encoded bytes: in=%d out=%d", inn, outn)
			}
			if in != out {
				t.Errorf("disagreement in encoded value: in=%d (0x%[1]x) out=%d (0x%[2]x)\nencoding=%08b", in, out, b[:inn])
			}
		}
	}
}

func TestKnownValues(t *testing.T) {
	tests := []struct {
		bytes []byte
		want  int32
	}{
		{bytes: []byte{0xff, 0xff, 0xff, 0xff, 0x0f}, want: -1},
		{bytes: []byte{0xe0, 0x45, 0x4f, 0x46}, want: 4542278},
	}

	for _, test := range tests {
		got, n, ok := Decode(test.bytes)
		if !ok {
			t.Errorf("failed to decode ITF-8 bytes: %08b", test.bytes)
		}
		if n != len(test.bytes) {
			t.Errorf("disagreement in expected number of encoded bytes: n=%d len(b)=%d", n, len(test.bytes))
		}
		if got != test.want {
			t.Errorf("disagreement in encoded value: got=%d want=%d (0x%[2]x)", got, test.want)
		}
	}
}
