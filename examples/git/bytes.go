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
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	value, err := unwrap(s.recovery.WriteBytes)
	if err != nil {
		return nil, err
	}
	w := &byteWriter{s: s, writer: value}
	s.writers = append(s.writers, w)
	return w, nil
}
func (w *byteWriter) Write(p []byte) (int, error) {
	if err := w.s.ctx.Err(); err != nil {
		w.close()
		return 0, err
	}
	if w.writer == nil {
		return 0, io.ErrClosedPipe
	}
	if _, err := unwrap(func() wt.Result[wt.Unit, wal.Failure] { return w.writer.Write(p) }); err != nil {
		w.close()
		return 0, err
	}
	return len(p), nil
}
func (w *byteWriter) finish() (*wal.Object, error) {
	defer w.close()
	if err := w.s.ctx.Err(); err != nil {
		return nil, err
	}
	if w.writer == nil {
		return nil, io.ErrClosedPipe
	}
	return unwrap(w.writer.Finish)
}
func (w *byteWriter) close() {
	if w.writer != nil {
		w.writer.Drop()
		w.writer = nil
	}
}

type byteReader struct {
	s         *store
	reader    *wal.ByteReader
	pos, size int64
}

func (s *store) openBytes(root *wal.Object) (*byteReader, error) {
	if err := s.ctx.Err(); err != nil {
		observeRead(&s.failure, err)
		return nil, err
	}
	reader, err := unwrap(func() wt.Result[*wal.ByteReader, wal.Failure] { return s.recovery.OpenBytes(root) })
	observeRead(&s.failure, err)
	if err != nil {
		return nil, err
	}
	size := reader.Length()
	if size > math.MaxInt64 {
		reader.Drop()
		return nil, fmt.Errorf("object size overflow")
	}
	return &byteReader{s: s, reader: reader, size: int64(size)}, nil
}
func (r *byteReader) Read(p []byte) (int, error) {
	if err := r.s.ctx.Err(); err != nil {
		observeRead(&r.s.failure, err)
		return 0, err
	}
	if r.reader == nil {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	data, err := unwrap(func() wt.Result[[]byte, wal.Failure] {
		return r.reader.ReadAt(uint64(r.pos), uint32(min(uint64(len(p)), math.MaxUint32)))
	})
	observeRead(&r.s.failure, err)
	if err != nil {
		return 0, err
	}
	if len(data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, data)
	r.pos += int64(n)
	return n, nil
}
func (r *byteReader) Seek(offset int64, whence int) (int64, error) {
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = r.pos
	case io.SeekEnd:
		base = r.size
	default:
		return r.pos, fmt.Errorf("invalid seek origin")
	}
	if offset < -base || offset > math.MaxInt64-base {
		return r.pos, fmt.Errorf("invalid byte offset")
	}
	r.pos = base + offset
	return r.pos, nil
}
func (r *byteReader) Close() error {
	if r.reader != nil {
		r.reader.Drop()
		r.reader = nil
	}
	return nil
}
