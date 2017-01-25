---
title: 'bíogo/hts: high throughput sequence handling for the Go language'
tags:
  - bioinformatics
  - toolkit
  - golang
authors:
 - name: R Daniel Kortschak
   orcid: 0000-0001-8295-2301
   affiliation: 1
 - name: Brent S Pedersen
   orcid: 0000-0003-1786-2216
   affiliation: 2
 - name: David L Adelson
   orcid: 0000-0003-2404-5636
   affiliation: 1
affiliations:
 - name: School of Biological Sciences, The University of Adelaide
   index: 1
 - name: Department of Human Genetics, University of Utah
   index: 2
date: 6 January 2017
---

# Summary

bíogo/hts provides a Go native implementation of the [SAM specification](https://samtools.github.io/hts-specs/SAMv1.pdf) for SAM and BAM alignment formats (Li et al. 2009) commonly used for representation of high throughput genomic data, the BAI, CSI and tabix indexing formats, and the BGZF blocked compression format. The bíogo/hts packages perform parallelized read and write operations and are able to cache recent reads according to user-specified caching methods. The parallelisation approach used by the bíogo/hts package is influenced by the approach of the D implementation, sambamba by Tarazov et al. (2015).
The bíogo/hts APIs have been constructed to provide a consistent interface to sequence alignment data and the underlying compression system in order to aid ease of use and tool development.

# References

https://samtools.github.io/hts-specs/SAMv1.pdf

Li, H. et al. (2009) "The Sequence Alignment/Map format and SAMtools" Bioinformatics 25(16):2078-2079. doi:10.1093/bioinformatics/btp352

Tarasov, A. et al. (2015) "Sambamba: fast processing of NGS alignment formats" Bioinformatics 31(12):2032-2034. doi:10.1093/bioinformatics/btv098

