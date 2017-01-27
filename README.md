![bíogo](https://raw.githubusercontent.com/biogo/biogo/master/biogo.png)

#HTS

[![Build Status](https://travis-ci.org/biogo/hts.svg?branch=master)](https://travis-ci.org/biogo/hts) [![GoDoc](https://godoc.org/github.com/biogo/hts?status.svg)](https://godoc.org/github.com/biogo/hts)

##Installation

        $ go get github.com/biogo/hts/...

##Overview

SAM and BAM handling for the Go language.

bíogo/hts provides a Go native implementation of the [SAM specification](https://samtools.github.io/hts-specs/SAMv1.pdf) for SAM and BAM alignment formats commonly used for representation of high throughput genomic data, the BAI, CSI and tabix indexing formats, and the BGZF blocked compression format.
The bíogo/hts packages perform parallelized read and write operations and are able to cache recent reads according to user-specified caching methods.
The bíogo/hts APIs have been constructed to provide a consistent interface to sequence alignment data and the underlying compression system in order to aid ease of use and tool development.

##Example usage

The following code implements the equivalent of `samtools view -c -f n -F N file.bam`.

```
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/biogo/hts/bam"
	"github.com/biogo/hts/bgzf"
	"github.com/biogo/hts/sam"
)

var (
	require = flag.Int("f", 0, "required flags")
	exclude = flag.Int("F", 0, "excluded flags")
	file    = flag.String("file", "", "input file (empty for stdin)")
	conc    = flag.Int("threads", 0, "number of threads to use (0 = auto)")
	help    = flag.Bool("help", false, "display help")
)

const maxFlag = int(^sam.Flags(0))

func main() {
	flag.Parse()
	if *help {
		flag.Usage()
		os.Exit(0)
	}

	if *require > maxFlag {
		flag.Usage()
		log.Fatal("required flags (f) out of range")
	}
	reqFlag := sam.Flags(*require)

	if *exclude > maxFlag {
		flag.Usage()
		log.Fatal("excluded flags (F) out of range")
	}
	excFlag := sam.Flags(*exclude)

	var r io.Reader
	if *file == "" {
		r = os.Stdin
	} else {
		f, err := os.Open(*file)
		if err != nil {
			log.Fatalf("could not open file %q:", err)
		}
		defer f.Close()
		ok, err := bgzf.HasEOF(f)
		if err != nil {
			log.Fatalf("could not open file %q:", err)
		}
		if !ok {
			log.Printf("file %q has no bgzf magic block: may be truncated", *file)
		}
		r = f
	}

	b, err := bam.NewReader(r, *conc)
	if err != nil {
		log.Fatalf("could not read bam:", err)
	}
	defer b.Close()

	// We only need flags, so skip variable length data.
	b.Omit(bam.AllVariableLengthData)

	var n int
	for {
		rec, err := b.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("error reading bam: %v", err)
		}
		if rec.Flags&reqFlag == reqFlag && rec.Flags&excFlag == 0 {
			n++
		}
	}

	fmt.Println(n)
}
```

##Getting help

Help or similar requests are preferred on the biogo-user Google Group.

https://groups.google.com/forum/#!forum/biogo-user

##Contributing

If you find any bugs, feel free to file an issue on the github issue tracker.
Pull requests are welcome, though if they involve changes to API or addition of features, please first open a discussion at the biogo-dev Google Group.

https://groups.google.com/forum/#!forum/biogo-dev

##Citing

If you use bíogo, please cite Kortschak and Adelson "bíogo: a simple high-performance bioinformatics toolkit for the Go language", doi:[10.1101/005033](http://biorxiv.org/content/early/2014/05/12/005033).

##Library Structure and Coding Style

The coding style should be aligned with normal Go idioms as represented in the
Go core libraries.

##Copyright and License

Copyright ©2011-2013 The bíogo Authors except where otherwise noted. All rights
reserved. Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.

The bíogo logo is derived from Bitstream Charter, Copyright ©1989-1992
Bitstream Inc., Cambridge, MA.

BITSTREAM CHARTER is a registered trademark of Bitstream Inc.
