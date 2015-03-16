// Copyright ©2015 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam_test

import (
	"io"
	"log"
	"os"

	"github.com/biogo/hts/bam"
)

func ExampleIndex_Add() {
	// Create a BAI for the BAM read from standard in and write it to standard out.
	br, err := bam.NewReader(os.Stdin, 1)
	if err != nil {
		log.Fatalf("failed to open BAM: %v", err)
	}

	var bai bam.Index
	for {
		r, err := br.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("failed to read BAM record: %v", err)
		}
		err = bai.Add(r, br.LastChunk())
		if err != nil {
			log.Fatalf("failed to add record to BAM index: %v", err)
		}
	}

	err = bam.WriteIndex(os.Stdout, &bai)
	if err != nil {
		log.Fatalf("failed to write BAM index: %v", err)
	}
}
