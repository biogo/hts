// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This package tabulates statistics on a bam file from the sam flag.
// It replicates functionality in samtools flagstat.
package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/biogo/hts/bam"
	"github.com/biogo/hts/sam"
)

const (
	pass = iota
	fail
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Expecting a single bam argument")
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	b, err := bam.NewReader(f, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()
	b.Omit(2)

	// counts is indexed by [pass/fail][sam.Flag] where we have 12 possible sam Flags.
	var counts [2][12]uint64
	// track mates on different chromosomes.
	var mates [2]struct{ low, high uint64 }
	var qc int
	for {
		read, err := b.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if read.Flags&sam.QCFail == 0 {
			qc = pass
		} else {
			qc = fail
		}

		counts[qc][0]++
		if read.Flags&sam.Supplementary != 0 {
			counts[qc][Supplementary]++
		} else if read.Flags&sam.Secondary != 0 {
			counts[qc][Secondary]++
		} else {
			for i := uint(1); i < 12; i++ {
				if read.Flags&(1<<i) != 0 {
					counts[qc][i]++
				}
			}
		}

		const mask = sam.Secondary | sam.ProperPair | sam.Supplementary | sam.Unmapped
		if read.Flags&mask == 0 {
			if read.MateRef != read.Ref && read.MateRef != nil && read.Ref != nil {
				if read.MapQ > 4 {
					mates[qc].high++
				}
				mates[qc].low++
			}
		}
	}
	// extract counts to match output from samtools flagstat.
	fmt.Printf("%d + %d in total (QC-passed reads + QC-failed reads)\n", counts[pass][Paired], counts[fail][Paired])
	fmt.Printf("%d + %d in total secondary\n", counts[pass][Secondary], counts[fail][Secondary])
	fmt.Printf("%d + %d in total supplementary\n", counts[pass][Supplementary], counts[fail][Supplementary])
	fmt.Printf("%d + %d duplicates\n", counts[pass][Duplicate], counts[fail][Duplicate])
	fmt.Printf("%d + %d mapped\n", counts[pass][Paired]-counts[pass][Unmapped], counts[fail][Paired]-counts[fail][Unmapped])
	fmt.Printf("%d + %d read1\n", counts[pass][Read1], counts[fail][Read1])
	fmt.Printf("%d + %d read2\n", counts[pass][Read2], counts[fail][Read2])
	fmt.Printf("%d + %d properly paired\n", counts[pass][ProperPair], counts[fail][ProperPair])
	fmt.Printf("%d + %d singletons\n", counts[pass][MateUnmapped], counts[fail][MateUnmapped])
	fmt.Printf("%d + %d with mate mapped to a different chr\n", mates[pass].low, mates[fail].low)
	fmt.Printf("%d + %d with mate mapped to a different chr (mapQ >= 5)\n", mates[pass].high, mates[fail].high)
}

// The flag indexes for SAM flags. Reflects sam.Flag order.
const (
	Paired        = iota // The read is paired in sequencing, no matter whether it is mapped in a pair.
	ProperPair           // The read is mapped in a proper pair.
	Unmapped             // The read itself is unmapped; conflictive with ProperPair.
	MateUnmapped         // The mate is unmapped.
	Reverse              // The read is mapped to the reverse strand.
	MateReverse          // The mate is mapped to the reverse strand.
	Read1                // This is read1.
	Read2                // This is read2.
	Secondary            // Not primary alignment.
	QCFail               // QC failure.
	Duplicate            // Optical or PCR duplicate.
	Supplementary        // Supplementary alignment, indicates alignment is part of a chimeric alignment.
)
