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

bíogo/hts provides a Go native implementation of the [SAM specification](https://samtools.github.io/hts-specs/SAMv1.pdf) for SAM and BAM alignment formats, the BAI, CSI and tabix indexing formats, and the BGZF blocked compression format. The bíogo/hts packages perform parallelized read and write operations and are able to cache recent reads according to user-specified caching methods.
The bíogo/hts APIs have been constructed to provide a consistent interface to sequence alignment data and the underlying compression system in order to aid ease of use and tool development.

# References

https://samtools.github.io/hts-specs/SAMv1.pdf