flagstat
========

This example replicates most of the output of [samtools](https://samtools.github.io) flagstat command.
With a single core, the program is 20-30% slower, but the Go program becomes faster when using 2 cores.

On an example bam the output of samtools is:
```
$ time samtools flagstat $bam
4284701 + 0 in total (QC-passed reads + QC-failed reads)
0 + 0 secondary
9319 + 0 supplementary
206663 + 0 duplicates
4273403 + 0 mapped (99.74% : N/A)
4275382 + 0 paired in sequencing
2137869 + 0 read1
2137513 + 0 read2
4230981 + 0 properly paired (98.96% : N/A)
4252786 + 0 with itself and mate mapped
11298 + 0 singletons (0.26% : N/A)
16955 + 0 with mate mapped to a different chr
10934 + 0 with mate mapped to a different chr (mapQ>=5)

real    0m10.301s
user    0m10.232s
sys 0m0.052s
```

and of this command on the same bam is:
```
$ go build -o flagstat flagstat.go
$ export GOMAXPROCS=1
$ ./flagstat $bam
0 + 0 in total secondary
9319 + 0 in total supplementary
206489 + 0 duplicates
4273403 + 0 mapped
2137869 + 0 read1
2137513 + 0 read2
4230981 + 0 properly paired
11298 + 0 singletons
16955 + 0 with mate mapped to a different chr
10934 + 0 with mate mapped to a different chr

real	0m12.254s
user	0m12.168s
sys	0m0.092s
```

and with 3 processors:

```
$ go build -o flagstat flagstat.go
$ export GOMAXPROCS=3
$ ./flagstat $bam
0 + 0 in total secondary
9319 + 0 in total supplementary
206489 + 0 duplicates
4273403 + 0 mapped
2137869 + 0 read1
2137513 + 0 read2
4230981 + 0 properly paired
11298 + 0 singletons
16955 + 0 with mate mapped to a different chr
10934 + 0 with mate mapped to a different chr

real	0m12.254s
user	0m12.168s
sys	0m0.092s
```

with 3 processors, this completes in about 6.6 seconds of user time and 18 seconds of user time.
