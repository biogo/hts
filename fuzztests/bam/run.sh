set -ex
#go install grail.com/vendor/github.com/dvyukov/go-fuzz/go-fuzz-build
#go install grail.com/vendor/github.com/dvyukov/go-fuzz/go-fuzz
go-fuzz-build github.com/biogo/hts/fuzztests/bam
WORKDIR=/tmp/fuzzbam
rm -rf $WORKDIR
mkdir -p $WORKDIR
go-fuzz -bin=./fuzzbam-fuzz.zip -workdir=$WORKDIR
