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

	active *compressor

	queue chan *compressor
	qwg   sync.WaitGroup

	waiting chan *compressor

	wg sync.WaitGroup

	closed bool

	m   sync.Mutex
	err error
}

func NewWriter(w io.Writer, wc int) *Writer {
	return NewWriterLevel(w, gzip.DefaultCompression, wc)
}

func NewWriterLevel(w io.Writer, level, wc int) *Writer {
	if wc < 2 {
		wc = 2
	}
	bg := &Writer{
		w:       w,
		waiting: make(chan *compressor, wc),
		queue:   make(chan *compressor, wc),
	}

	c := make([]compressor, wc)
	for i := range c {
		c[i].Header = &bg.Header
		c[i].level = level
		c[i].waiting = bg.waiting
		c[i].flush = make(chan *compressor, 1)
		c[i].qwg = &bg.qwg
		bg.waiting <- &c[i]
	}
	bg.active = <-bg.waiting

	bg.wg.Add(1)
	go func() {
		defer bg.wg.Done()
		for qw := range bg.queue {
			if !writeOK(bg, <-qw.flush) {
				break
			}
		}
	}()

	return bg
}

func writeOK(bg *Writer, c *compressor) bool {
	defer func() { bg.waiting <- c }()

	if c.err != nil {
		bg.setErr(c.err)
		return false
	}
	if c.buf.Len() == 0 {
		return true
	}

	_, err := io.Copy(bg.w, &c.buf)
	bg.qwg.Done()
	if err != nil {
		bg.setErr(err)
		return false
	}
	c.next = 0

	return true
}

type compressor struct {
	*gzip.Header
	gz    *egzip.Writer
	level int

	next  int
	block [BlockSize]byte
	buf   bytes.Buffer

	flush chan *compressor
	qwg   *sync.WaitGroup

	waiting chan *compressor

	err error
}

func (c *compressor) writeBlock() {
	defer func() { c.flush <- c }()

	if c.gz == nil {
		c.gz, c.err = egzip.NewWriterLevel(&c.buf, c.level)
		if c.err != nil {
			return
		}
	} else {
		c.gz.Reset(&c.buf)
	}
	c.gz.Header = gzip.Header{
		Comment: c.Comment,
		Extra:   append([]byte(bgzfExtra), c.Extra...),
		ModTime: c.ModTime,
		Name:    c.Name,
		OS:      c.OS,
	}

	_, c.err = c.gz.Write(c.block[:c.next])
	if c.err != nil {
		return
	}
	c.err = c.gz.Close()
	if c.err != nil {
		return
	}
	c.next = 0

	b := c.buf.Bytes()
	i := bytes.Index(b, bgzfExtraPrefix)
	if i < 0 {
		c.err = gzip.ErrHeader
		return
	}
	size := len(b) - 1
	if size >= MaxBlockSize {
		c.err = ErrBlockOverflow
		return
	}
	b[i+4], b[i+5] = byte(size), byte(size>>8)
}

func (bg *Writer) Next() (int, error) {
	if bg.closed {
		return 0, ErrClosed
	}
	if err := bg.Error(); err != nil {
		return 0, err
	}

	return bg.active.next, nil
}

func (bg *Writer) Write(b []byte) (int, error) {
	if bg.closed {
		return 0, ErrClosed
	}
	err := bg.Error()
	if err != nil {
		return 0, err
	}

	c := bg.active
	var n int
	for ; len(b) > 0 && err == nil; err = bg.Error() {
		var _n int
		if c.next == 0 || c.next+len(b) <= len(c.block) {
			_n = copy(c.block[c.next:], b)
			b = b[_n:]
			c.next += _n
		}

		if c.next == len(c.block) || _n == 0 {
			n += c.buf.Len()
			bg.queue <- c
			bg.qwg.Add(1)
			go c.writeBlock()
			c = <-bg.waiting
		}
	}
	bg.active = c

	return n, bg.Error()
}

func (bg *Writer) Flush() error {
	if bg.closed {
		return ErrClosed
	}
	if err := bg.Error(); err != nil {
		return err
	}

	if bg.active.next == 0 {
		return nil
	}

	var c *compressor
	c, bg.active = bg.active, <-bg.waiting
	bg.queue <- c
	bg.qwg.Add(1)
	go c.writeBlock()

	return bg.Error()
}

func (bg *Writer) Wait() error {
	if err := bg.Error(); err != nil {
		return err
	}
	bg.qwg.Wait()
	return bg.Error()
}

func (bg *Writer) Error() error {
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
		c := bg.active
		bg.queue <- c
		bg.qwg.Add(1)
		<-bg.waiting
		c.writeBlock()
		bg.closed = true
		close(bg.queue)
		bg.wg.Wait()
		if bg.err == nil {
			_, bg.err = bg.w.Write([]byte(magicBlock))
		}
	}
	return bg.err
}
