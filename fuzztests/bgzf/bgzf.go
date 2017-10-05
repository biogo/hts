package fuzzbgzf

import (
	"bytes"
	"github.com/biogo/hts/bgzf"
)

func Fuzz(data []byte) int {
	buf := bytes.NewBuffer(data)
	r, err := bgzf.NewReader(buf, 1)
	if err != nil {
		return 0
	}
	tmp := make([]byte, 1024)
	for {
		_, err := r.Read(tmp)
		if err != nil {
			break
		}
	}
	return 0
}
