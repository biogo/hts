// Copyright ©2021 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bgzf_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/biogo/hts/bgzf"
)

func ExampleReader_ReadByte() {
	// Write Tom Sawyer into a bgzf buffer.
	var buf bytes.Buffer
	w := bgzf.NewWriter(&buf, 1)
	f, err := os.Open("testdata/Mark.Twain-Tom.Sawyer.txt")
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer f.Close()
	_, err = io.Copy(w, f)
	if err != nil {
		log.Fatalf("failed to copy file: %v", err)
	}
	err = w.Close()
	if err != nil {
		log.Fatalf("failed to close bgzf writer: %v", err)
	}

	// The text to search for.
	const line = `"It ain't any use, Huck, we're wrong again."`

	// Read the data until the line is found and output the line
	// number and bgzf.Chunk corresponding to the lines position
	// in the compressed data.
	r, err := bgzf.NewReader(&buf, 1)
	if err != nil {
		log.Fatal(err)
	}
	var n int
	for {
		n++
		b, chunk, err := readLine(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Fatal(err)
		}
		// Make sure we trim the trailing newline.
		if bytes.Equal(bytes.TrimSpace(b), []byte(line)) {
			fmt.Printf("line:%d chunk:%+v\n", n, chunk)
			break
		}
	}

	// Output:
	//
	// line:5986 chunk:{Begin:{File:112534 Block:11772} End:{File:112534 Block:11818}}
}

// readLine returns a line terminated by a '\n' and the bgzf.Chunk that contains
// the line, including the newline character. If the end of file is reached before
// a newline, the unterminated line and corresponding chunk are returned.
func readLine(r *bgzf.Reader) ([]byte, bgzf.Chunk, error) {
	tx := r.Begin()
	var (
		data []byte
		b    byte
		err  error
	)
	for {
		b, err = r.ReadByte()
		if err != nil {
			break
		}
		data = append(data, b)
		if b == '\n' {
			break
		}
	}
	chunk := tx.End()
	return data, chunk, err
}
