package server

import (
	"compress/gzip"
	"io"
	"sync"

	"github.com/klauspost/compress/s2"
)

var Gzip gzipPool
var Snappy snappyPool

type gzipPool struct {
	writers sync.Pool
}

type snappyPool struct {
	writers sync.Pool
}

func (pool *gzipPool) GetWriter(dst io.Writer) *gzip.Writer {
	var writer *gzip.Writer
	if w := pool.writers.Get(); w != nil {
		writer = w.(*gzip.Writer)
		writer.Reset(dst)
	} else {
		writer = gzip.NewWriter(dst)
	}

	return writer
}

func (pool *gzipPool) PutWriter(writer *gzip.Writer) {
	writer.Close()
	pool.writers.Put(writer)
}

func (pool *snappyPool) GetWriter(dst io.Writer) *s2.Writer {
	var writer *s2.Writer
	if w := pool.writers.Get(); w != nil {
		writer = w.(*s2.Writer)
		writer.Reset(dst)
	} else {
		writer = s2.NewWriter(dst, s2.WriterSnappyCompat())
	}

	return writer
}

func (pool *snappyPool) PutWriter(writer *s2.Writer) {
	writer.Close()
	pool.writers.Put(writer)
}
