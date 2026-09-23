package main

import (
	"io"

	"github.com/go-git/go-git/v6/plumbing"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
)

// Incoming packs are staged as seekable WAL bytes before import.
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
func (p *incomingPack) Close() (err error) {
	if p.closed {
		return p.err
	}
	p.closed = true
	defer p.close()
	defer func() {
		p.err = err
		observeRead(&p.s.failure, err)
	}()
	if p.err != nil {
		return p.err
	}
	root, err := p.finish()
	if err != nil {
		return err
	}
	// This stream remains temporary: its root is never published.
	defer root.Drop()
	reader, err := p.s.openBytes(root)
	if err != nil {
		return err
	}
	defer reader.Close()
	offsets := packOffsets{}
	if err := importPack(p.s.ctx, reader, p.s, p.s.meta.Format, p.s.limits, offsets); err != nil {
		return err
	}
	remaining := p.s.limits.catalogBytes
	if err := offsets.deltas(reader, reader.size, func(id plumbing.Hash, delta *deltaMeta) error {
		key := id.String()
		item := p.s.pending[key]
		if len(item.Inline) != 0 || item.Delta != nil || int64(len(delta.Data)) >= item.StoredSize {
			return nil
		}
		cost := int64(len(delta.Data) + len(delta.Base) + 64)
		if cost > remaining {
			return nil
		}
		remaining -= cost
		if len(delta.Data) > inlineDeltaLimit {
			writer, err := p.s.newByteWriter()
			if err != nil {
				return err
			}
			for data := delta.Data; len(data) > 0; {
				part := data[:min(len(data), 1<<20)]
				if _, err := writer.Write(part); err != nil {
					return err
				}
				data = data[len(part):]
			}
			deltaRoot, err := writer.finish()
			if err != nil {
				return err
			}
			root, err := p.s.putNode(nil, []*wal.Object{item.root, deltaRoot})
			deltaRoot.Drop()
			if err != nil {
				return err
			}
			item.root = root
			delta.StoredSize = int64(len(delta.Data))
			delta.Data = nil
		}
		item.Delta = delta
		p.s.pending[key] = item
		return nil
	}); err != nil {
		return err
	}
	return p.s.failure
}
