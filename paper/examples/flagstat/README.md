flagstat
========

This example replicates the output of [samtools](https://samtools.github.io) flagstat command.
Core-for-core the C implementation outperforms the Go implementation.

On an example BAM file the output of samtools (1.3.2-199-gec1d68e/htslib 1.3.2-199-gec1d68e) is:
```
$ time samtools flagstat 9827_2#49.bam
56463236 + 0 in total (QC-passed reads + QC-failed reads)
0 + 0 secondary
0 + 0 supplementary
269248 + 0 duplicates
55357963 + 0 mapped (98.04% : N/A)
56463236 + 0 paired in sequencing
28231618 + 0 read1
28231618 + 0 read2
54363468 + 0 properly paired (96.28% : N/A)
55062652 + 0 with itself and mate mapped
295311 + 0 singletons (0.52% : N/A)
360264 + 0 with mate mapped to a different chr
300699 + 0 with mate mapped to a different chr (mapQ>=5)

real	1m31.517s
user	1m30.268s
sys	0m1.180s
```

The following give the same flagstat output, but with reduced time.

`--input-fmt-option nthreads=2`
```
real	0m46.057s
user	1m49.684s
sys	0m4.432s
```

`--input-fmt-option nthreads=4`
```
real	0m26.816s
user	1m55.148s
sys	0m3.856s
```

`--input-fmt-option nthreads=8`
```
real	0m23.006s
user	2m10.352s
sys	0m5.648s
```

and of this command (Go 1.8) on the same file is:
```
$ go build github.com/biogo/hts/paper/examples/flagstat
$ export GOMAXPROCS=1
$ time ./flagstat 9827_2#49.bam
56463236 + 0 in total (QC-passed reads + QC-failed reads)
0 + 0 in total secondary
0 + 0 in total supplementary
269248 + 0 duplicates
55357963 + 0 mapped (98.04% : N/A)
56463236 + 0 paired in sequencing
28231618 + 0 read1
28231618 + 0 read2
54363468 + 0 properly paired (96.28% : N/A)
55062652 + 0 with itself and mate mapped
295311 + 0 singletons (0.52% : N/A)
360264 + 0 with mate mapped to a different chr
300699 + 0 with mate mapped to a different chr (mapQ >= 5)

real	5m2.323s
user	5m0.312s
sys	0m2.148s
```

The following give the same flagstat output, but with reduced time.

GOMAXPROCS=2
```
real	2m41.310s
user	5m18.948s
sys	0m2.600s
```

GOMAXPROCS=4
```
real	1m40.957s
user	6m21.232s
sys	0m3.688s
```

GOMAXPROCS=8
```
real	1m28.465s
user	9m7.480s
sys	0m8.056s
```

The file used in the benchmark was 9827_2#49.bam, available from ftp://ftp.sra.ebi.ac.uk/vol1/ERA242/ERA242167/bam/9827_2%2349.bam