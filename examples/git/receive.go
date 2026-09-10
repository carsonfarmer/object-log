package main

import (
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"io"
)

// A seekable pack lets go-git release decoded objects instead of retaining the
// entire inflated push. Delta resolution still buffers an individual base and result.
func (s *store) LowMemoryMode() bool { return true }
func (s *store) PackfileWriter() (io.WriteCloser, error) {
	writer, err := s.newByteWriter()
	if err != nil {
		return nil, err
	}
	return &incomingPack{byteWriter: writer}, nil
}

type incomingPack struct {
	*byteWriter
	closed bool
	err    error
}

func (p *incomingPack) Write(data []byte) (int, error) {
	if p.closed {
		return 0, io.ErrClosedPipe
	}
	if p.err != nil {
		return 0, p.err
	}
	n, err := p.byteWriter.Write(data)
	p.err = err
	return n, err
}
func (p *incomingPack) Close() error {
	if p.closed {
		return p.err
	}
	p.closed = true
	defer p.close()
	defer func() {
		if p.err != nil && p.s.failure == nil {
			p.s.failure = p.err
		}
	}()
	if p.err != nil {
		return p.err
	}
	root, err := p.finish()
	if err != nil {
		p.err = err
		return err
	}
	// This stream remains temporary: its root is never published.
	defer root.Drop()
	reader, err := p.s.openBytes(root)
	if err != nil {
		p.err = err
		return err
	}
	defer reader.Close()
	_, p.err = packfile.NewParser(reader, packfile.WithStorage(p.s), packfile.WithObjectFormat(p.s.meta.Format)).Parse()
	if p.err == nil {
		p.err = p.s.failure
	}
	return p.err
}
