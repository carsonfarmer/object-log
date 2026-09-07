package main

import (
	"fmt"
	"io"
	"math"

	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
)

// A seekable pack lets go-git release decoded objects instead of retaining the
// entire inflated push. Delta resolution still buffers an individual base and result.
func (s *store) LowMemoryMode() bool { return true }
func (s *store) PackfileWriter() (io.WriteCloser, error) {
	return &incomingPack{s: s}, nil
}

type incomingPack struct {
	s      *store
	chunks []*wal.Object
	buf    []byte
	size   int64
	closed bool
	err    error
}

func (p *incomingPack) flush() error {
	if len(p.buf) == 0 {
		return nil
	}
	root, err := p.s.put(p.buf)
	if err != nil {
		return err
	}
	p.chunks = append(p.chunks, root)
	p.buf = p.buf[:0]
	return nil
}
func (p *incomingPack) Write(data []byte) (n int, err error) {
	if p.closed {
		return 0, io.ErrClosedPipe
	}
	if p.err != nil {
		return 0, p.err
	}
	defer func() { p.err = err }()
	if int64(len(data)) > math.MaxInt64-p.size {
		return 0, fmt.Errorf("pack size overflow")
	}
	for len(data) > 0 {
		count := min(len(data), chunkSize-len(p.buf))
		p.buf = append(p.buf, data[:count]...)
		p.size += int64(count)
		n += count
		data = data[count:]
		if len(p.buf) == chunkSize {
			if err := p.flush(); err != nil {
				return n, err
			}
		}
	}
	return n, nil
}
func (p *incomingPack) Close() error {
	if p.closed {
		return p.err
	}
	p.closed = true
	defer func() {
		// Input pack chunks are temporary and never referenced by the published root.
		for _, root := range p.chunks {
			root.Drop()
		}
		p.chunks, p.buf = nil, nil
		if p.err != nil && p.s.failure == nil {
			p.s.failure = p.err
		}
	}()
	if p.err == nil {
		p.err = p.flush()
	}
	if p.err == nil {
		reader := &packReader{pack: p, cached: -1}
		_, p.err = packfile.NewParser(reader, packfile.WithStorage(p.s), packfile.WithObjectFormat(p.s.meta.Format)).Parse()
		if p.err == nil {
			p.err = p.s.failure
		}
	}
	return p.err
}

type packReader struct {
	pack   *incomingPack
	pos    int64
	cached int64
	buf    []byte
}

func (r *packReader) Read(out []byte) (int, error) {
	if len(out) == 0 {
		return 0, nil
	}
	if r.pos >= r.pack.size {
		return 0, io.EOF
	}
	index := r.pos / chunkSize
	if r.cached != index {
		data, err := unwrap(r.pack.s.session.Read(r.pack.chunks[index]))
		if err != nil {
			return 0, err
		}
		expected := min(int64(chunkSize), r.pack.size-index*chunkSize)
		if int64(len(data)) != expected {
			return 0, fmt.Errorf("invalid staged pack chunk length")
		}
		r.buf, r.cached = data, index
	}
	n := copy(out, r.buf[r.pos%chunkSize:])
	r.pos += int64(n)
	return n, nil
}
func (r *packReader) Seek(offset int64, whence int) (int64, error) {
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = r.pos
	case io.SeekEnd:
		base = r.pack.size
	default:
		return r.pos, fmt.Errorf("invalid seek origin")
	}
	if offset < -base || offset > math.MaxInt64-base {
		return r.pos, fmt.Errorf("invalid pack offset")
	}
	r.pos = base + offset
	return r.pos, nil
}
