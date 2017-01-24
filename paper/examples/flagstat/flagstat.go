package main

import (
	"fmt"
	"io"
	"math"
	"os"

	"github.com/biogo/hts/bam"
	"github.com/biogo/hts/sam"
)

func main() {
	if len(os.Args) != 2 {
		panic("Expecting a single bam argument")
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer f.Close()
	b, err := bam.NewReader(f, 0)
	if err != nil {
		panic(err)
	}
	defer b.Close()
	b.Omit(2)

	// counts is indexed by [pass/fail][sam.Flag] where we have 12 possible sam Flags.
	var counts [2][12]uint64
	// track mates on different chromosomes.
	var mates [2][2]uint64
	var fail int
	for {
		read, err := b.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		if read.Flags&sam.QCFail == 0 {
			fail = 0
		} else {
			fail = 1
		}

		counts[fail][0]++
		if read.Flags&sam.Supplementary != 0 {
			counts[fail][11]++
		} else if read.Flags&sam.Secondary != 0 {
			counts[fail][log2(sam.Secondary)]++
		} else {
			for i := 1; i < 12; i++ {
				if read.Flags&(1<<uint(i)) != 0 {
					counts[fail][i]++
				}
			}
		}
		if read.Flags&(sam.Secondary|sam.ProperPair|sam.Supplementary|sam.Unmapped) == 0 {
			if read.MateRef != read.Ref && read.MateRef != nil && read.Ref != nil {
				if read.MapQ > 4 {
					mates[fail][1]++
				}
				mates[fail][0]++
			}
		}
	}
	// extract counts to match output from samtools flagstat.
	fmt.Printf("%d + %d in total (QC-passed reads + QC-failed reads)\n", counts[0][0], counts[1][0])
	fmt.Printf("%d + %d in total secondary\n", counts[0][log2(sam.Secondary)], counts[1][log2(sam.Secondary)])
	fmt.Printf("%d + %d in total supplementary\n", counts[0][log2(sam.Supplementary)], counts[1][log2(sam.Supplementary)])
	fmt.Printf("%d + %d duplicates\n", counts[0][log2(sam.Duplicate)], counts[1][log2(sam.Duplicate)])
	fmt.Printf("%d + %d mapped\n", counts[0][0]-counts[0][log2(sam.Unmapped)], counts[1][0]-counts[1][log2(sam.Unmapped)])
	fmt.Printf("%d + %d read1\n", counts[0][log2(sam.Read1)], counts[1][log2(sam.Read1)])
	fmt.Printf("%d + %d read2\n", counts[0][log2(sam.Read2)], counts[1][log2(sam.Read2)])
	fmt.Printf("%d + %d properly paired\n", counts[0][log2(sam.ProperPair)], counts[1][log2(sam.ProperPair)])
	fmt.Printf("%d + %d singletons\n", counts[0][log2(sam.MateUnmapped)], counts[1][log2(sam.MateUnmapped)])
	fmt.Printf("%d + %d with mate mapped to a different chr\n", mates[0][0], mates[1][0])
	fmt.Printf("%d + %d with mate mapped to a different chr\n", mates[0][1], mates[1][1])
}

func log2(s sam.Flags) int {
	return int(math.Log2(float64(s)))
}
