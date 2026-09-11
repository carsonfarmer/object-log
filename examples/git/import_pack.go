package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"math"
	"sort"

	"github.com/go-git/go-git/v6/plumbing"
	format "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	packutil "github.com/go-git/go-git/v6/plumbing/format/packfile/util"
	githash "github.com/go-git/go-git/v6/plumbing/hash"
	gitbinary "github.com/go-git/go-git/v6/utils/binary"
	gitioutil "github.com/go-git/go-git/v6/utils/ioutil"
)

type packStorage interface {
	RawObjectWriter(plumbing.ObjectType, int64) (io.WriteCloser, error)
	EncodedObject(plumbing.ObjectType, plumbing.Hash) (plumbing.EncodedObject, error)
}

type packEntry struct {
	offset, content int64
	size, target    int64
	base, id        plumbing.Hash
	kind            plumbing.ObjectType
	depth           int
}

// importPack retains entry metadata, never inflated objects or delta instructions.
// The framing pass authenticates the pack before unresolved deltas are applied.
func importPack(ctx context.Context, source io.ReadSeeker, storage packStorage, objectFormat format.ObjectFormat, limits requestLimits) error {
	if limits.objectBytes <= 0 || limits.metadataBytes <= 0 || limits.packObjects <= 0 {
		return errObjectLimit
	}
	digest, err := githash.FromObjectFormat(objectFormat)
	if err != nil {
		return err
	}
	input := &packInput{reader: bufio.NewReader(source), digest: digest, ctx: ctx}
	var header [12]byte
	if _, err := io.ReadFull(input, header[:]); err != nil {
		return err
	}
	if string(header[:4]) != "PACK" || (binary.BigEndian.Uint32(header[4:8]) != 2 && binary.BigEndian.Uint32(header[4:8]) != 3) {
		return packfile.ErrMalformedPackfile
	}
	count := binary.BigEndian.Uint32(header[8:])
	if int64(count) > limits.packObjects {
		return errObjectLimit
	}
	var entries []*packEntry
	byOffset := make(map[int64][]*packEntry)
	for remaining := count; remaining > 0; remaining-- {
		entry := &packEntry{offset: input.offset}
		first, err := input.ReadByte()
		if err != nil {
			return err
		}
		entry.kind = packutil.ObjectType(first)
		size, err := packutil.VariableLengthSize(first, input)
		if err != nil {
			return err
		}
		if size > math.MaxInt64 {
			return packfile.ErrMalformedPackfile
		}
		entry.size = int64(size)
		switch entry.kind {
		case plumbing.CommitObject, plumbing.TreeObject, plumbing.BlobObject, plumbing.TagObject:
			if err := limits.checkObject(entry.kind, entry.size); err != nil {
				return err
			}
		case plumbing.OFSDeltaObject:
			distance, err := gitbinary.ReadVariableWidthInt(input)
			if err != nil {
				return err
			}
			if err := packfile.ValidateOFSDeltaBase(entry.offset, distance); err != nil {
				return err
			}
			baseOffset := entry.offset - distance
			i := sort.Search(len(entries), func(i int) bool { return entries[i].offset >= baseOffset })
			if i == len(entries) || entries[i].offset != baseOffset {
				return packfile.ErrMalformedPackfile
			}
			byOffset[baseOffset] = append(byOffset[baseOffset], entry)
		case plumbing.REFDeltaObject:
			entry.base.ResetBySize(objectFormat.Size())
			if _, err := entry.base.ReadFrom(input); err != nil {
				return err
			}
		default:
			return packfile.ErrMalformedPackfile
		}
		entry.content = input.offset
		if err := scanPackEntry(ctx, input, entry, storage, objectFormat, limits.objectBytes); err != nil {
			return err
		}
		entries = append(entries, entry)
	}
	expected := digest.Sum(nil)
	input.digest = nil
	actual := make([]byte, len(expected))
	if _, err := io.ReadFull(input, actual); err != nil {
		return err
	}
	if !bytes.Equal(expected, actual) {
		return packfile.ErrMalformedPackfile
	}
	if _, err := input.ReadByte(); err != io.EOF {
		if err != nil {
			return err
		}
		return packfile.ErrMalformedPackfile
	}
	// A ready queue visits each dependency once, including forward REF deltas.
	byHash := make(map[plumbing.Hash][]*packEntry)
	var ready []*packEntry
	unresolved := 0
	for _, entry := range entries {
		if !entry.kind.IsDelta() {
			ready = append(ready, entry)
			continue
		}
		unresolved++
		if entry.kind == plumbing.REFDeltaObject {
			byHash[entry.base] = append(byHash[entry.base], entry)
		}
	}
	// Thin packs may reference an object already in the catalog.
	for base := range byHash {
		object, err := storage.EncodedObject(plumbing.AnyObject, base)
		if errors.Is(err, plumbing.ErrObjectNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if err := limits.checkObject(object.Type(), object.Size()); err != nil {
			return err
		}
		ready = append(ready, &packEntry{id: base})
	}
	for len(ready) > 0 {
		parent := ready[0]
		ready[0] = nil
		ready = ready[1:]
		children := append(byHash[parent.id], byOffset[parent.offset]...)
		delete(byHash, parent.id)
		delete(byOffset, parent.offset)
		for _, entry := range children {
			entry.depth = parent.depth + 1
			if entry.depth > 4095 {
				return packfile.ErrMalformedPackfile
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			base, err := storage.EncodedObject(plumbing.AnyObject, parent.id)
			if err != nil {
				return err
			}
			if err := limits.checkObject(base.Type(), base.Size()); err != nil {
				return err
			}
			if err := limits.checkObject(base.Type(), entry.target); err != nil {
				return err
			}
			if _, err := source.Seek(entry.content, io.SeekStart); err != nil {
				return err
			}
			compressed, err := zlib.NewReader(bufio.NewReader(source))
			if err != nil {
				return err
			}
			trackedBase := &packBase{EncodedObject: base}
			delta, err := packfile.ReaderFromDelta(trackedBase, compressed)
			if err != nil {
				_ = compressed.Close()
				return err
			}
			entry.kind = base.Type()
			entry.id, err = writePackObject(storage, objectFormat, entry.kind, entry.target, &packDeltaReader{ReadCloser: delta, base: trackedBase})
			_ = compressed.Close()
			if err != nil {
				return err
			}
			unresolved--
			ready = append(ready, entry)
		}
	}
	if unresolved != 0 {
		return packfile.ErrReferenceDeltaNotFound
	}
	return ctx.Err()
}

func scanPackEntry(ctx context.Context, input io.Reader, entry *packEntry, storage packStorage, objectFormat format.ObjectFormat, limit int64) error {
	compressed, err := zlib.NewReader(input)
	if err != nil {
		return err
	}
	if !entry.kind.IsDelta() {
		entry.id, err = writePackObject(storage, objectFormat, entry.kind, entry.size, compressed)
		return err
	}
	defer compressed.Close()
	// Count the complete inflated instruction stream without retaining it.
	counted := &packInput{reader: bufio.NewReader(compressed), ctx: ctx}
	sourceSize, err := packutil.DecodeLEB128FromReader(counted)
	if err != nil {
		return err
	}
	targetSize, err := packutil.DecodeLEB128FromReader(counted)
	if err != nil {
		return err
	}
	if uint64(sourceSize) > uint64(limit) || uint64(targetSize) > uint64(limit) {
		return errObjectLimit
	}
	entry.target = int64(targetSize)
	// Every valid instruction consumes at most eight bytes per output byte,
	// plus the two size headers. Saturate rather than overflowing large limits.
	instructionLimit := int64(math.MaxInt64)
	if entry.target <= (math.MaxInt64-20)/8 {
		instructionLimit = entry.target*8 + 20
	}
	if entry.size > instructionLimit {
		return packfile.ErrInvalidDelta
	}
	if counted.offset > entry.size {
		return packfile.ErrInvalidDelta
	}
	_, err = io.Copy(io.Discard, io.LimitReader(counted, entry.size-counted.offset))
	if err != nil {
		return err
	}
	if counted.offset != entry.size {
		return io.ErrUnexpectedEOF
	}
	if _, err := counted.ReadByte(); err != io.EOF {
		if err != nil {
			return err
		}
		return packfile.ErrInvalidDelta
	}
	return nil
}

func writePackObject(storage packStorage, objectFormat format.ObjectFormat, kind plumbing.ObjectType, size int64, source io.ReadCloser) (plumbing.Hash, error) {
	writer, err := storage.RawObjectWriter(kind, size)
	if err != nil {
		_ = source.Close()
		return plumbing.ZeroHash, err
	}
	digest := plumbing.NewHasher(objectFormat, kind, size)
	n, err := io.Copy(io.MultiWriter(writer, digest), io.LimitReader(source, size))
	if err == nil && n != size {
		err = io.ErrUnexpectedEOF
	}
	if err == nil {
		var extra [1]byte
		if n, readErr := source.Read(extra[:]); n != 0 {
			err = packfile.ErrMalformedPackfile
		} else if readErr != io.EOF {
			err = readErr
			if err == nil {
				err = io.ErrNoProgress
			}
		}
	}
	readClosed := source.Close()
	closed := writer.Close()
	if err != nil {
		return plumbing.ZeroHash, err
	}
	if readClosed != nil {
		return plumbing.ZeroHash, readClosed
	}
	if closed != nil {
		return plumbing.ZeroHash, closed
	}
	return digest.Sum(), nil
}

// ByteReader prevents zlib from consuming the next pack entry. Hash only bytes
// consumed by framing, not bufio's read-ahead (which can include the checksum).
type packInput struct {
	reader *bufio.Reader
	digest hash.Hash
	offset int64
	ctx    context.Context
}

func (r *packInput) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(p)
	r.offset += int64(n)
	if r.digest != nil {
		_, _ = r.digest.Write(p[:n])
	}
	return n, err
}
func (r *packInput) ReadByte() (byte, error) {
	var one [1]byte
	_, err := io.ReadFull(r, one[:])
	return one[0], err
}

// The decoder signals pipe EOF before its deferred base Close. Waiting for the
// last reader's Close keeps WAL resource destruction ahead of destination Close
// and session cleanup. Earlier readers close synchronously on backward copies.
type packBase struct {
	plumbing.EncodedObject
	done chan struct{}
}

func (b *packBase) Reader() (io.ReadCloser, error) {
	reader, err := b.EncodedObject.Reader()
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	b.done = done
	return gitioutil.NewReadCloserWithCloser(reader, func() error { close(done); return nil }), nil
}

type packDeltaReader struct {
	io.ReadCloser
	base *packBase
}

func (r *packDeltaReader) Close() error {
	// Drain on a destination error to unblock the decoder's pipe writes. Terminal
	// reads synchronize all Reader calls, so no later reopen can change base.done.
	_, err := io.Copy(io.Discard, r.ReadCloser)
	closed := r.ReadCloser.Close()
	if r.base.done != nil {
		<-r.base.done
	}
	return errors.Join(err, closed)
}
