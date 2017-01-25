// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This program tabulates statistics on a bam file from the sam flag.
// It replicates functionality in samtools flagstat.
package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/biogo/hts/bam"
	"github.com/biogo/hts/bgzf"
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
	ok, err := bgzf.HasEOF(f)
	if err != nil {
		log.Fatal(err)
	}
	if !ok {
		log.Println("EOF block missing")
	}

	b, err := bam.NewReader(f, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()
	b.Omit(bam.AllVariableLengthData)

	// counts is indexed by [pass/fail][sam.Flag] where we have 12 possible sam Flags.
	var counts [2][12]uint64
	// track mates on different chromosomes.
	var mates [2]struct{ allMapQ, highMapQ uint64 }
	var good, singletons, paired [2]uint64
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

		for i := Paired; i <= Supplementary; i++ {
			if read.Flags&(1<<i) != 0 {
				counts[qc][i]++
			}
		}

		const goodMask = sam.ProperPair | sam.Unmapped
		if read.Flags&goodMask == sam.ProperPair {
			good[qc]++
		}

		const mapMask = sam.MateUnmapped | sam.Unmapped
		switch read.Flags & mapMask {
		case sam.MateUnmapped:
			singletons[qc]++
		case 0:
			paired[qc]++
			if read.MateRef != read.Ref && read.MateRef != nil && read.Ref != nil {
				if read.MapQ > 4 {
					mates[qc].highMapQ++
				}
				mates[qc].allMapQ++
			}
		}
	}

	// extract counts to match output from samtools flagstat.
	fmt.Printf("%d + %d in total (QC-passed reads + QC-failed reads)\n", counts[pass][Paired], counts[fail][Paired])
	fmt.Printf("%d + %d in total secondary\n", counts[pass][Secondary], counts[fail][Secondary])
	fmt.Printf("%d + %d in total supplementary\n", counts[pass][Supplementary], counts[fail][Supplementary])
	fmt.Printf("%d + %d duplicates\n", counts[pass][Duplicate], counts[fail][Duplicate])
	mappedPass := counts[pass][Paired] - counts[pass][Unmapped]
	mappedFail := counts[fail][Paired] - counts[fail][Unmapped]
	fmt.Printf("%d + %d mapped (%s : %s)\n", mappedPass, mappedFail, percent(mappedPass, counts[pass][Paired]), percent(mappedFail, counts[fail][Paired]))
	fmt.Printf("%d + %d paired in sequencing\n", counts[pass][Paired], counts[fail][Paired])
	fmt.Printf("%d + %d read1\n", counts[pass][Read1], counts[fail][Read1])
	fmt.Printf("%d + %d read2\n", counts[pass][Read2], counts[fail][Read2])
	fmt.Printf("%d + %d properly paired (%s : %s)\n", good[pass], good[fail], percent(good[pass], counts[pass][Paired]), percent(good[fail], counts[fail][Paired]))
	fmt.Printf("%d + %d with itself and mate mapped\n", paired[pass], paired[fail])
	fmt.Printf("%d + %d singletons (%s : %s)\n", singletons[pass], singletons[fail], percent(singletons[pass], counts[pass][Paired]), percent(singletons[fail], counts[fail][Paired]))
	fmt.Printf("%d + %d with mate mapped to a different chr\n", mates[pass].allMapQ, mates[fail].allMapQ)
	fmt.Printf("%d + %d with mate mapped to a different chr (mapQ>=5)\n", mates[pass].highMapQ, mates[fail].highMapQ)
}

func percent(n, total uint64) string {
	if total == 0 {
		return "N/A"
	}
	return fmt.Sprintf("%.2f%%", 100*float64(n)/float64(total))
}

// The flag indexes for SAM flags. Reflects sam.Flag order.
const (
	Paired        uint = iota // The read is paired in sequencing, no matter whether it is mapped in a pair.
	ProperPair                // The read is mapped in a proper pair.
	Unmapped                  // The read itself is unmapped; conflictive with ProperPair.
	MateUnmapped              // The mate is unmapped.
	Reverse                   // The read is mapped to the reverse strand.
	MateReverse               // The mate is mapped to the reverse strand.
	Read1                     // This is read1.
	Read2                     // This is read2.
	Secondary                 // Not primary alignment.
	QCFail                    // QC failure.
	Duplicate                 // Optical or PCR duplicate.
	Supplementary             // Supplementary alignment, indicates alignment is part of a chimeric alignment.
)
