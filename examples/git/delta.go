package main

import (
	"bytes"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/go-git/go-git/v6/plumbing"
	packutil "github.com/go-git/go-git/v6/plumbing/format/packfile/util"
	gitbinary "github.com/go-git/go-git/v6/utils/binary"
)

// Keep optional representations out of sparse catalog reads when they grow.
const inlineDeltaLimit = 64 << 10
const retainedDeltaLimit = 8 << 20

type deltaMeta struct {
	Base       string
	Size       int64
	Data       []byte
	StoredSize int64 `json:",omitempty"`
}

func (d deltaMeta) valid() bool {
	if !plumbing.IsHash(d.Base) || d.Size < 2 {
		return false
	}
	if len(d.Data) == 0 {
		return d.StoredSize > inlineDeltaLimit && d.StoredSize <= retainedDeltaLimit
	}
	return d.StoredSize == 0 && len(d.Data) <= inlineDeltaLimit
}

// The full object remains available when the receiver does not need the base.
type storedDelta struct {
	plumbing.EncodedObject
	delta   *deltaMeta
	open    func() (io.ReadCloser, error)
	failure *error
}

func (d storedDelta) Type() plumbing.ObjectType { return plumbing.REFDeltaObject }
func (d storedDelta) Size() int64               { return d.delta.Size }
func (d storedDelta) BaseHash() plumbing.Hash   { return plumbing.NewHash(d.delta.Base) }
func (d storedDelta) ActualHash() plumbing.Hash { return d.EncodedObject.Hash() }
func (d storedDelta) ActualSize() int64         { return d.EncodedObject.Size() }
func (d storedDelta) Reader() (result io.ReadCloser, err error) {
	defer func() {
		if d.failure != nil {
			observeRead(d.failure, err)
		}
	}()
	if d.delta.StoredSize == 0 {
		return zlib.NewReader(bytes.NewReader(d.delta.Data))
	}
	source, err := d.open()
	if err != nil {
		return nil, err
	}
	reader, err := zlib.NewReader(source)
	if err != nil {
		_ = source.Close()
		return nil, err
	}
	return &deltaReader{ReadCloser: reader, source: source, failure: d.failure}, nil
}

type deltaReader struct {
	io.ReadCloser
	source  io.Closer
	failure *error
}

func (r *deltaReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err != io.EOF && r.failure != nil {
		observeRead(r.failure, err)
	}
	return n, err
}

func (r *deltaReader) Close() error {
	return errors.Join(r.ReadCloser.Close(), r.source.Close())
}

type packOffsets map[int64]plumbing.Hash

func (p packOffsets) OnHeader(uint32) error                                          { return nil }
func (p packOffsets) OnInflatedObjectHeader(plumbing.ObjectType, int64, int64) error { return nil }
func (p packOffsets) OnInflatedObjectContent(id plumbing.Hash, pos int64, _ uint32, _ []byte) error {
	p[pos] = id
	return nil
}
func (p packOffsets) OnFooter(plumbing.Hash) error { return nil }

// Extract optional representations from a pack already validated by go-git.
func (p packOffsets) deltas(source io.ReadSeeker, size int64, retain func(plumbing.Hash, *deltaMeta) error) error {
	offsets := slices.Sorted(maps.Keys(p))
	for i, offset := range offsets {
		id := p[offset]
		end := size - int64(id.Size())
		if i+1 < len(offsets) {
			end = offsets[i+1]
		}
		// An object header uses at most ten bytes, followed by a base reference.
		if end-offset > retainedDeltaLimit+10+int64(id.Size()) {
			continue
		}
		if end <= offset {
			return fmt.Errorf("invalid pack offsets")
		}
		if _, err := source.Seek(offset, io.SeekStart); err != nil {
			return err
		}
		var first [1]byte
		if _, err := io.ReadFull(source, first[:]); err != nil {
			return err
		}
		kind := packutil.ObjectType(first[0])
		if !kind.IsDelta() {
			continue
		}
		data := make([]byte, end-offset-1)
		if _, err := io.ReadFull(source, data); err != nil {
			return err
		}
		r := bytes.NewReader(data)
		length, err := packutil.VariableLengthSize(first[0], r)
		if err != nil {
			return err
		}
		base := id
		if kind == plumbing.OFSDeltaObject {
			distance, err := gitbinary.ReadVariableWidthInt(r)
			if err != nil {
				return err
			}
			base = p[offset-distance]
		} else if _, err := base.ReadFrom(r); err != nil {
			return err
		}
		if r.Len() > retainedDeltaLimit {
			continue
		}
		delta := &deltaMeta{Base: base.String(), Size: int64(length), Data: data[len(data)-r.Len():]}
		if delta.Size < 2 || len(delta.Data) == 0 || base.IsZero() {
			return fmt.Errorf("invalid retained delta")
		}
		if err := retain(id, delta); err != nil {
			return err
		}
	}
	return nil
}

// Inline deltas share the leaf's allowance. External deltas live below the
// object's root and do not enlarge unrelated catalog reads.
func limitDeltas(items []objectMeta) {
	remaining := indexLeafSize * inlineObjectLimit
	for _, item := range items {
		remaining -= len(item.Inline)
	}
	for i := range items {
		if delta := items[i].Delta; delta != nil {
			if delta.StoredSize > 0 {
				continue
			}
			if len(delta.Data) > remaining {
				items[i].Delta = nil
			} else {
				remaining -= len(delta.Data)
			}
		}
	}
}
