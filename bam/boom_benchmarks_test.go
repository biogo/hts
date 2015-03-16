// Copyright ©2013 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build boom

package bam

import (
	"os"
	"testing"

	"github.com/biogo/boom"
)

func BenchmarkReadBoom(b *testing.B) {
	if *file == "" {
		b.Skip("no file specified")
	}
	b.StopTimer()
	br, err := boom.OpenBAM(*file)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		for {
			_, _, err := br.Read()
			if err != nil {
				break
			}
		}
	}

	br.Close()
}

func BenchmarkWriteBoom(b *testing.B) {
	if *file == "" {
		b.Skip("no file specified")
	}
	b.StopTimer()
	br, err := boom.OpenBAM(*file)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	f, err := os.OpenFile("/dev/null", os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	bw, err := boom.OpenBAMFile(f, "bw", br.Header())
	if err != nil {
		b.Fatalf("NewWriter failed: %v", err)
	}
	r, _, err := br.Read()
	if err != nil {
		b.Fatalf("Read failed: %v", err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err = bw.Write(r)
		if err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}

	br.Close()
	f.Close()
}

func BenchmarkReadFileBoom(b *testing.B) {
	if *file == "" {
		b.Skip("no file specified")
	}

	for i := 0; i < b.N; i++ {
		br, err := boom.OpenBAM(*file)
		if err != nil {
			b.Fatalf("Open failed: %v", err)
		}
		for {
			_, _, err := br.Read()
			if err != nil {
				break
			}
		}
		br.Close()
	}
}

func BenchmarkRoundtripFileBoom(b *testing.B) {
	if *file == "" {
		b.Skip("no file specified")
	}

	for i := 0; i < b.N; i++ {
		br, err := boom.OpenBAM(*file)
		if err != nil {
			b.Fatalf("Open failed: %v", err)
		}
		f, err := os.OpenFile("/dev/null", os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			b.Fatalf("Open failed: %v", err)
		}
		bw, err := boom.OpenBAMFile(f, "bw", br.Header())
		if err != nil {
			b.Fatalf("NewWriter failed: %v", err)
		}
		for {
			r, _, err := br.Read()
			if err != nil {
				break
			}
			_, err = bw.Write(r)
			if err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
		br.Close()
		f.Close()
	}
}
