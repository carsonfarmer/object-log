package main

import (
	"fmt"
	wt "go.bytecodealliance.org/pkg/wit/types"
	"io"
	"math"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
)

// These adapters own component resources; byte layout and caching belong to WAL.
type byteWriter struct {
	s      *store
	writer *wal.ByteWriter
}

func (s *store) newByteWriter() (*byteWriter, error) {
	value, err := storeCall(s, s.recovery.WriteBytes)
	if err != nil {
		return nil, err
	}
	w := &byteWriter{s: s, writer: value}
	s.writers = append(s.writers, w)
	return w, nil
}
func (w *byteWriter) Write(p []byte) (int, error) {
	if w.writer == nil {
		return 0, io.ErrClosedPipe
	}
	if _, err := storeCall(w.s, func() wt.Result[wt.Unit, wal.Failure] { return w.writer.Write(p) }); err != nil {
		w.close()
		return 0, err
	}
	return len(p), nil
}
func (w *byteWriter) finish() (*wal.Object, error) {
	defer w.close()
	if w.writer == nil {
		return nil, io.ErrClosedPipe
	}
	return storeCall(w.s, w.writer.Finish)
}
func (w *byteWriter) close() {
	if w.writer != nil {
		w.writer.Drop()
		w.writer = nil
	}
}

type byteReader struct {
	*io.SectionReader
	s      *store
	reader *wal.ByteReader
	size   int64
}

func (s *store) openBytes(root *wal.Object) (*byteReader, error) {
	reader, err := storeCall(s, func() wt.Result[*wal.ByteReader, wal.Failure] { return s.recovery.OpenBytes(root) })
	if err != nil {
		return nil, err
	}
	size := reader.Length()
	if size > math.MaxInt64 {
		reader.Drop()
		return nil, fmt.Errorf("object size overflow")
	}
	r := &byteReader{s: s, reader: reader, size: int64(size)}
	r.SectionReader = io.NewSectionReader(r, 0, r.size)
	return r, nil
}
func (r *byteReader) Read(p []byte) (int, error) {
	if err := r.s.ctx.Err(); err != nil {
		observeRead(&r.s.failure, err)
		return 0, err
	}
	if r.reader == nil {
		return 0, io.ErrClosedPipe
	}
	return r.SectionReader.Read(p)
}

// WAL returns at most one chunk. ReaderAt must fill the entire request or
// return an error; a short chunk is not the end of the byte stream.
func (r *byteReader) ReadAt(p []byte, offset int64) (n int, err error) {
	if offset < 0 {
		return 0, fmt.Errorf("invalid byte offset")
	}
	for n < len(p) {
		if r.reader == nil {
			return n, io.ErrClosedPipe
		}
		data, err := storeCall(r.s, func() wt.Result[[]byte, wal.Failure] {
			return r.reader.ReadAt(uint64(offset+int64(n)), uint32(min(uint64(len(p)-n), math.MaxUint32)))
		})
		if err != nil {
			return n, err
		}
		if len(data) == 0 {
			return n, io.EOF
		}
		n += copy(p[n:], data)
	}
	return n, nil
}
func (r *byteReader) Close() error {
	if r.reader != nil {
		r.reader.Drop()
		r.reader = nil
	}
	return nil
}
