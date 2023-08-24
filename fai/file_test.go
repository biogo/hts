// Copyright ©2020 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fai

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestFile(t *testing.T) {
	files, err := filepath.Glob("testdata/*.fa")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, p := range files {
		f, err := os.Open(p + ".fai")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			continue
		}
		idx, err := ReadFrom(f)
		f.Close()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			continue
		}

		b, err := os.ReadFile(p + ".json")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			continue
		}

		var tests []struct {
			Seq        string
			Start, End int
			Want       string
		}
		err = json.Unmarshal(b, &tests)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			continue
		}

		f, err = os.Open(p)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			continue
		}
		m := NewFile(f, idx)
		for _, test := range tests {
			var s *Seq
			if test.Start == -1 {
				s, err = m.Seq(test.Seq)
			} else {
				s, err = m.SeqRange(test.Seq, test.Start, test.End)
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				continue
			}

			for _, bufSize := range []int{10, 100, 1000} {
				buf := make([]byte, bufSize)
				var got []byte
				for {
					n, err := s.Read(buf)
					got = append(got, buf[:n]...)
					if err != nil {
						if err == io.EOF {
							break
						}
						t.Errorf("unexpected error: %v", err)
					}
				}
				if test.Start != -1 && len(got) != test.End-test.Start {
					t.Errorf("unexpected sequence length: got:%d want:%d", len(got), test.End-test.Start)
				}
				if string(got) != test.Want {
					t.Errorf("unexpected sequence: got:%q want:%q", got, test.Want)
				}

				s.Reset()
			}
		}

		f.Close()
	}
}
