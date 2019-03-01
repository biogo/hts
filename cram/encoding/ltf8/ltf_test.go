// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ltf8

import "testing"

func TestInt64RoundTrip(t *testing.T) {
	b := make([]byte, 10)
	for i := uint(0); i < 64; i++ {
		for off := -1; off <= 1; off++ {
			in := int64(1<<i + off)
			inn := Encode(b, in)
			wantn := Len(in)
			if wantn != inn {
				t.Errorf("disagreement in number of encoded bytes required: want=%d need=%d", wantn, inn)
			}
			out, outn, ok := Decode(b)
			if !ok {
				t.Errorf("failed to decode LTF-8 bytes: %08b", b[:inn])
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
