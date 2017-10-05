package fuzzbam

import (
	"bytes"
	"github.com/biogo/hts/bam"
	"github.com/biogo/hts/bgzf"
)

func Fuzz(data []byte) int {
	buf := bytes.Buffer{}
	w := bgzf.NewWriter(&buf, 1)
	if n, err := w.Write(data); err != nil || n != len(data) {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}

	r, err := bam.NewReader(&buf, 1)
	if err != nil {
		return 0
	}
	for {
		_, err := r.Read()
		if err != nil {
			break
		}
	}
	return 0
}
