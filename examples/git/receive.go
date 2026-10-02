package main

import (
	"io"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
)

// Incoming packs are staged as seekable WAL bytes before import.
func (s *store) PackfileWriter() (io.WriteCloser, error) {
	writer, err := s.newByteWriter()
	if err != nil {
		return nil, err
	}
	return closingWriter(s.ctx, writer, &s.failure, func() error { return s.importIncoming(writer) }), nil
}

func (s *store) importIncoming(writer *byteWriter) error {
	defer writer.close()
	if s.failure != nil {
		return s.failure
	}
	if err := s.progress.message("Checking received pack...\n"); err != nil {
		return err
	}
	root, err := writer.finish()
	if err != nil {
		return err
	}
	// This stream remains temporary: its root is never published.
	defer root.Drop()
	reader, err := s.openBytes(root)
	if err != nil {
		return err
	}
	defer reader.Close()
	offsets := packOffsets{}
	observers := []packfile.Observer{offsets, s.progress}
	if s.progress == nil {
		observers = observers[:1]
	}
	if err := importPack(s.ctx, reader, s, s.meta.Format, s.limits, observers...); err != nil {
		return err
	}
	if err := s.progress.message("Indexing deltas...\n"); err != nil {
		return err
	}
	remaining := s.limits.catalogBytes
	if err := offsets.deltas(reader, reader.size, func(id plumbing.Hash, delta *deltaMeta) error {
		key := id.String()
		item := s.pending[key]
		if item.Delta != nil || int64(len(delta.Data)) >= item.StoredSize {
			return nil
		}
		cost := int64(len(delta.Data) + len(delta.Base) + 64)
		if cost > remaining {
			return nil
		}
		remaining -= cost
		if len(delta.Data) > inlineDeltaLimit {
			writer, err := s.newByteWriter()
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
			s.owned = append(s.owned, deltaRoot)
			item.deltaRoot = deltaRoot
			delta.StoredSize = int64(len(delta.Data))
			delta.Data = nil
		}
		item.Delta = delta
		s.pending[key] = item
		return nil
	}); err != nil {
		return err
	}
	return s.failure
}
