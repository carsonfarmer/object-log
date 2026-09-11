package main

import "io"

// Incoming packs are staged as seekable WAL bytes before streaming import.
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
	p.err = importPack(p.s.ctx, reader, p.s, p.s.meta.Format, p.s.maxObjectBytes)
	if p.err == nil {
		p.err = p.s.failure
	}
	return p.err
}
