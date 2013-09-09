// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bgzf

import (
	"code.google.com/p/biogo.bam/bgzf/egzip"

	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

const (
	BlockSize    = 0x0ff00 // Size of input data block.
	MaxBlockSize = 0x10000 // Maximum size of output block.
)

const (
	bgzfExtra = "BC\x02\x00\x00\x00"
	minFrame  = 20 + len(bgzfExtra) // Minimum bgzf header+footer length.

	// Magic EOF block.
	magicBlock = "\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00\x42\x43\x02\x00\x1b\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00"
)

var bgzfExtraPrefix = []byte(bgzfExtra[:4])

func compressBound(srcLen int) int {
	return srcLen + srcLen>>12 + srcLen>>14 + srcLen>>25 + 13 + minFrame
}

func init() {
	if compressBound(BlockSize) > MaxBlockSize {
		panic("bam: BlockSize too large")
	}
}

var (
	NewBlock         = egzip.NewBlock
	ErrClosed        = errors.New("bgzf: use of closed writer")
	ErrBlockOverflow = errors.New("bgzf: block overflow")
	ErrWrongFileType = errors.New("bgzf: file is a directory")
)

type Reader struct {
	gzip.Header
	gz  *egzip.Reader
	err error
}

func CheckEOF(f *os.File) (bool, error) {
	fi, err := f.Stat()
	if err != nil || fi.IsDir() {
		return false, ErrWrongFileType
	}

	b := make([]byte, len(magicBlock))
	_, err = f.ReadAt(b, fi.Size()-int64(len(magicBlock)))
	if err != nil {
		return false, err
	}
	for i := range b {
		if b[i] != magicBlock[i] {
			return false, nil
		}
	}
	return true, nil
}

func NewReader(r io.Reader, limited bool) (*Reader, error) {
	bg := &Reader{}
	gz, err := egzip.NewReader(r, &bg.Header)
	if err != nil {
		return nil, err
	}
	gz.BlockLimited = limited
	bg.gz = gz
	return bg, nil
}

type Offset struct {
	File  int64
	Block uint16
}

func (bg *Reader) Seek(off Offset, whence int) error {
	bg.err = bg.gz.Seek(off.File, whence)
	if bg.err != nil {
		return bg.err
	}
	if off.Block > 0 {
		_, bg.err = io.CopyN(ioutil.Discard, bg.gz, int64(off.Block))
	}
	return bg.err
}

func (bg *Reader) Close() error {
	return bg.gz.Close()
}

func (bg *Reader) Read(p []byte) (int, error) {
	if bg.err != nil {
		return 0, bg.err
	}
	n, err := bg.gz.Read(p)
	if n < len(p) && err == nil {
		var pn int
		pn, err = bg.Read(p[n:])
		n += pn
	}
	if n > 0 && err == io.EOF {
		err = nil
	}
	return n, err
}

func (bg *Reader) CurrBlockSize() (int, error) {
	if bg.err != nil {
		return -1, bg.err
	}
	i := bytes.Index(bg.Extra, bgzfExtraPrefix)
	if i+5 >= len(bg.Extra) {
		return -1, gzip.ErrHeader
	}
	return (int(bg.Extra[i+4]) | int(bg.Extra[i+5])<<8) + 1, nil
}

type Writer struct {
	gzip.Header
	w io.Writer

	queue chan *worker
	qwg   sync.WaitGroup

	pool

	wg sync.WaitGroup

	closed bool

	m   sync.Mutex
	err error
}

type pool struct {
	active  chan *worker
	waiting chan *worker
}

func NewWriter(w io.Writer, wc int) *Writer {
	return NewWriterLevel(w, gzip.DefaultCompression, wc)
}

func NewWriterLevel(w io.Writer, level, wc int) *Writer {
	if wc < 2 {
		wc = 2
	}
	bg := &Writer{
		w: w,
		pool: pool{
			active:  make(chan *worker, 1),
			waiting: make(chan *worker, wc),
		},
		queue: make(chan *worker, wc),
	}

	wp := make([]worker, wc)
	for i := range wp {
		wp[i].Header = &bg.Header
		wp[i].level = level
		wp[i].pool = bg.pool
		wp[i].flush = make(chan *worker, 1)
		wp[i].qwg = &bg.qwg
		bg.waiting <- &wp[i]
	}
	bg.active <- <-bg.waiting

	bg.wg.Add(1)
	go func() {
		defer bg.wg.Done()
		for qw := range bg.queue {
			if !writeOK(bg, <-qw.flush) {
				break
			}
		}
		if bg.err == nil {
			writeOK(bg, <-bg.active)
		}
	}()

	return bg
}

func writeOK(bg *Writer, wk *worker) bool {
	defer func() { bg.waiting <- wk }()

	if wk.err != nil {
		bg.setErr(wk.err)
		return false
	}
	if wk.buf.Len() == 0 {
		return true
	}

	_, err := io.Copy(bg.w, &wk.buf)
	bg.qwg.Done()
	if err != nil {
		bg.setErr(err)
		return false
	}
	wk.next = 0

	return true
}

type worker struct {
	*gzip.Header
	gz    *egzip.Writer
	level int

	next  int
	block [BlockSize]byte
	buf   bytes.Buffer

	flush chan *worker
	qwg   *sync.WaitGroup

	pool

	err error
}

func (wk *worker) writeBlock() {
	wk.active <- <-wk.waiting
	defer func() { wk.flush <- wk }()

	if wk.gz == nil {
		wk.gz, wk.err = egzip.NewWriterLevel(&wk.buf, wk.level)
		if wk.err != nil {
			return
		}
	} else {
		wk.gz.Reset(&wk.buf)
	}
	wk.gz.Header = gzip.Header{
		Comment: wk.Comment,
		Extra:   append([]byte(bgzfExtra), wk.Extra...),
		ModTime: wk.ModTime,
		Name:    wk.Name,
		OS:      wk.OS,
	}

	_, wk.err = wk.gz.Write(wk.block[:wk.next])
	if wk.err != nil {
		return
	}
	wk.err = wk.gz.Close()
	if wk.err != nil {
		return
	}
	wk.next = 0

	b := wk.buf.Bytes()
	i := bytes.Index(b, bgzfExtraPrefix)
	if i < 0 {
		wk.err = gzip.ErrHeader
		return
	}
	size := len(b) - 1
	if size >= MaxBlockSize {
		wk.err = ErrBlockOverflow
		return
	}
	b[i+4], b[i+5] = byte(size), byte(size>>8)
}

func (bg *Writer) Next() (int, error) {
	if bg.closed {
		return 0, ErrClosed
	}
	if err := bg.Err(); err != nil {
		return 0, err
	}

	wk := <-bg.active
	bg.active <- wk

	return wk.next, nil
}

func (bg *Writer) Write(b []byte) (int, error) {
	if bg.closed {
		return 0, ErrClosed
	}
	err := bg.Err()
	if err != nil {
		return 0, err
	}

	wk := <-bg.active
	var n int
	for ; len(b) > 0 && err == nil; err = bg.Err() {
		var _n int
		if wk.next == 0 || wk.next+len(b) <= len(wk.block) {
			_n = copy(wk.block[wk.next:], b)
			b = b[_n:]
			wk.next += _n
		}

		if wk.next == len(wk.block) || _n == 0 {
			n += wk.buf.Len()
			bg.queue <- wk
			bg.qwg.Add(1)
			go wk.writeBlock()
			wk = <-bg.active
		}
	}
	bg.active <- wk

	return n, bg.Err()
}

func (bg *Writer) Flush() error {
	if bg.closed {
		return ErrClosed
	}
	if err := bg.Err(); err != nil {
		return err
	}

	wk := <-bg.active
	if wk.next == 0 {
		bg.active <- wk
		return nil
	}

	bg.queue <- wk
	bg.qwg.Add(1)
	go wk.writeBlock()

	return bg.Err()
}

func (bg *Writer) Wait() error {
	if err := bg.Err(); err != nil {
		return err
	}
	bg.qwg.Wait()
	return bg.Err()
}

func (bg *Writer) Err() error {
	bg.m.Lock()
	defer bg.m.Unlock()
	return bg.err
}

func (bg *Writer) setErr(err error) {
	bg.m.Lock()
	defer bg.m.Unlock()
	if bg.err == nil {
		bg.err = err
	}
}

func (bg *Writer) Close() error {
	if !bg.closed {
		wk := <-bg.active
		bg.queue <- wk
		bg.qwg.Add(1)
		wk.writeBlock()
		bg.closed = true
		close(bg.queue)
		bg.wg.Wait()
		if bg.err == nil {
			_, bg.err = bg.w.Write([]byte(magicBlock))
		}
	}
	return bg.err
}
