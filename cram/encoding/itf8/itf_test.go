// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package itf8

import "testing"

func TestNLO(t *testing.T) {
	for i := 0; i < 256; i++ {
		want := loopNLO(byte(i))
		got := nlo(byte(i))
		if got != want {
			t.Errorf("unexpected number of leading ones: got:%d want:%d", got, want)
		}
	}
}

func loopNLO(x byte) int {
	var n int
	for b := 0x80; b > 0; b >>= 1 {
		if x&byte(b) == 0 {
			break
		}
		n++
	}
	return n
}

func TestUint32RoundTrip(t *testing.T) {
	b := make([]byte, 6)
	for i := uint(0); i < 32; i++ {
		for off := -1; off <= 1; off++ {
			in := uint32(1<<i + off)
			inn := EncodeUint32(b, in)
			wantn := Uint32Len(in)
			if wantn != inn {
				t.Errorf("disagreement in number of encoded bytes required: want=%d need=%d", wantn, inn)
			}
			out, outn, ok := DecodeUint32(b)
			if !ok {
				t.Error("failed to decode ITF-8 bytes: %08b", b[:inn])
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

func TestInt32RoundTrip(t *testing.T) {
	b := make([]byte, 6)
	for i := uint(0); i < 32; i++ {
		for off := -1; off <= 1; off++ {
			in := int32(1<<i + off)
			inn := EncodeInt32(b, in)
			wantn := Int32Len(in)
			if wantn != inn {
				t.Errorf("disagreement in number of encoded bytes required: want=%d need=%d", wantn, inn)
			}
			out, outn, ok := DecodeInt32(b)
			if !ok {
				t.Error("failed to decode ITF-8 bytes: %08b", b[:inn])
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
		got, n, ok := DecodeInt32(test.bytes)
		if !ok {
			t.Error("failed to decode ITF-8 bytes: %08b", test.bytes)
		}
		if n != len(test.bytes) {
			t.Errorf("disagreement in expected number of encoded bytes: n=%d len(b)=%d", n, len(test.bytes))
		}
		if got != test.want {
			t.Errorf("disagreement in encoded value: got=%d want=%d (0x%[2]x)", got, test.want)
		}
	}
}
