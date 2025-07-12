package server

import (
	"compress/gzip"
	"io"
	"sync"

	"github.com/klauspost/compress/s2"
)

var Gzip gzipPool

var Snappy snappyPool

var SnappyCompact = snappyPool{
	opt: []s2.WriterOption{s2.WriterSnappyCompat()},
}
var SnappyBetterCompression = snappyPool{
	opt: []s2.WriterOption{s2.WriterBetterCompression(), s2.WriterConcurrency(1)},
}
var SnappyBestCompression = snappyPool{
	opt: []s2.WriterOption{s2.WriterBestCompression(), s2.WriterConcurrency(1)},
}
var SnappyNoCompression = snappyPool{
	opt: []s2.WriterOption{s2.WriterUncompressed(), s2.WriterConcurrency(1)},
}

type gzipPool struct {
	writers sync.Pool
}

type snappyPool struct {
	writers sync.Pool
	opt     []s2.WriterOption
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
		writer = s2.NewWriter(dst, pool.opt...)
	}

	return writer
}

func (pool *snappyPool) PutWriter(writer *s2.Writer) error {
	err := writer.Close()
	pool.writers.Put(writer)

	return err
}

func (pool *snappyPool) PutWriterNoClose(writer *s2.Writer) {
	pool.writers.Put(writer)
}

func GetS2WriterByOptions(dst io.Writer, t string) (*s2.Writer, func(writer *s2.Writer) error) {
	switch t {
	case CompressionS2Uncompressed:
		return SnappyNoCompression.GetWriter(dst), SnappyNoCompression.PutWriter
	case CompressionS2Better:
		return SnappyBetterCompression.GetWriter(dst), SnappyBetterCompression.PutWriter
	case CompressionS2Best:
		return SnappyBestCompression.GetWriter(dst), SnappyBestCompression.PutWriter
	default:
		return Snappy.GetWriter(dst), Snappy.PutWriter
	}
}
