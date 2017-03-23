# Proposal: CRAM (v3) format support in GO

Last updated: March 23, 2017

## Abstract

[CRAM format](https://samtools.github.io/hts-specs/CRAMv3.pdf) encodes genomic alignments to a reference.
It has better lossless (and optional lossy) compression compared to existing BAM format. It is also
becoming more widely used. We propose the addition of a CRAM Reader to [biogo/hts](https://github.com/biogo/hts)
under `hts/cram`.

## Background

As more projects move to whole-genome sequencing across large cohorts, drive space to store alignments becomes a
concern. This is the reason for CRAM which has a much smaller footprint than BAM. This is done largely through
encoding difference to a reference rather than saving the full sequence and by integer compression schemes.

The functionality to be implemented will be driven by the specification: https://samtools.github.io/hts-specs/CRAMv3.pdf
but limited to the types observed in the wild and implemented and used in [htslib](https://samtools.github.io)

## High-Level API

The `hts/cram` Reader should match the `hts/bam` API as closely as is reasonable.

```Go

import "biogo/hts/cram"

var cr *cram.CRAM 
var err error

cr, err = cram.NewReader(rdr, io.Reader, rd int, reference *sam.Reference)
cr.Omit(bam.AllVariableLengthData)

var hdr *cram.Header = cr.Header()

var rec *sam.Record
rec, err = cr.Read()
```

The difference from the `hts/bam` API is the requirement of the `reference` argument to the constructor.
Note that extracting the sequence is costly, especially in CRAM. While the `Omit` method in `hts/bam`
provides a global level of control over if this cost is incurred, we may wish to add a finer level 
`Record.Sequence(r *sam.Reference)` method so that the user has full control over exactly when
to incur this cost.

If the `sam.Reference` must be passed to the `Sequence()` method, then the `cram.NewReader` function would not
need that value.

We will need to understand how this would work with the regional query method in `hts/bam` that gets a slice of
`[]bgzf.Chunk` to be sent to a `bam.Iterator`.

the cram.Header will be identical to extend `sam.Header`:

```Go
type Header struct {
    *sam.Header
}
```


## Internals


### Container

The `container` is the unit of compression in `CRAM`

TODO: stub out container struct.

### Codecs

The CRAM specification lists a number of codecs. However, we will limit to those that are used in htslib.github
Namely, those [are](https://github.com/biogo/hts/issues/54#issuecomment-275359197) :

+ gzip
+ bzip2
+ lzma
+ rANS
+ huffman in single-code-only mode (?)

`gzip` and `bzip2` are already available in the go standard library as `io.Reader`s; the rest
should be implemented as `io.Reader`.

### itf8

`itf8` is a central data-type in `CRAM`.

we can follow the implementation [here](https://github.com/samtools/htslib/blob/70622cffc711b0e501c431bc221a6abdbdf3a6bd/cram/cram_io.h#L111)

with a signature like:

```Go
func readITF8(r io.reader) (int64, error)
```
