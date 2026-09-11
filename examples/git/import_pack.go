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

	"github.com/go-git/go-git/v6/plumbing"
	format "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	packutil "github.com/go-git/go-git/v6/plumbing/format/packfile/util"
	githash "github.com/go-git/go-git/v6/plumbing/hash"
	gitbinary "github.com/go-git/go-git/v6/utils/binary"
)

type packStorage interface {
	RawObjectWriter(plumbing.ObjectType, int64) (io.WriteCloser, error)
	EncodedObject(plumbing.ObjectType, plumbing.Hash) (plumbing.EncodedObject, error)
}

type packEntry struct {
	offset, content int64
	size, target    int64
	baseOffset      int64
	base, id        plumbing.Hash
	kind            plumbing.ObjectType
	depth           int
}

// importPack retains entry metadata, never inflated objects or delta instructions.
// The framing pass authenticates the pack before unresolved deltas are applied.
func importPack(ctx context.Context, source io.ReadSeeker, storage packStorage, objectFormat format.ObjectFormat, maxObjectBytes int64) error {
	if maxObjectBytes <= 0 {
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
	var entries []*packEntry
	offsets := make(map[int64]*packEntry)
	for remaining := binary.BigEndian.Uint32(header[8:]); remaining > 0; remaining-- {
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
			if entry.size > maxObjectBytes {
				return errObjectLimit
			}
		case plumbing.OFSDeltaObject:
			distance, err := gitbinary.ReadVariableWidthInt(input)
			if err != nil {
				return err
			}
			if err := packfile.ValidateOFSDeltaBase(entry.offset, distance); err != nil {
				return err
			}
			entry.baseOffset = entry.offset - distance
			if offsets[entry.baseOffset] == nil {
				return packfile.ErrMalformedPackfile
			}
		case plumbing.REFDeltaObject:
			entry.base.ResetBySize(objectFormat.Size())
			if _, err := entry.base.ReadFrom(input); err != nil {
				return err
			}
		default:
			return packfile.ErrMalformedPackfile
		}
		entry.content = input.offset
		if err := scanPackEntry(ctx, input, entry, storage, objectFormat, maxObjectBytes); err != nil {
			return err
		}
		offsets[entry.offset] = entry
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
	byOffset := make(map[int64][]*packEntry)
	var ready []*packEntry
	unresolved := 0
	for _, entry := range entries {
		if !entry.kind.IsDelta() {
			ready = append(ready, entry)
			continue
		}
		unresolved++
		if entry.kind == plumbing.OFSDeltaObject {
			byOffset[entry.baseOffset] = append(byOffset[entry.baseOffset], entry)
		} else {
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
		if object.Size() < 0 || object.Size() > maxObjectBytes {
			return errObjectLimit
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
			if base.Size() < 0 || base.Size() > maxObjectBytes {
				return errObjectLimit
			}
			if _, err := source.Seek(entry.content, io.SeekStart); err != nil {
				return err
			}
			compressed, err := zlib.NewReader(bufio.NewReader(source))
			if err != nil {
				return err
			}
			delta, err := packfile.ReaderFromDelta(base, compressed)
			if err != nil {
				_ = compressed.Close()
				return err
			}
			entry.kind = base.Type()
			entry.id, err = writePackObject(storage, objectFormat, entry.kind, entry.target, delta)
			// ReaderFromDelta owns a goroutine: finish its reads before releasing the
			// pack/base resources, even when the destination rejected a write.
			if err != nil {
				_, _ = io.Copy(io.Discard, delta)
			}
			_ = delta.Close()
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
	defer compressed.Close()
	if !entry.kind.IsDelta() {
		entry.id, err = writePackObject(storage, objectFormat, entry.kind, entry.size, compressed)
		return err
	}
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

func writePackObject(storage packStorage, objectFormat format.ObjectFormat, kind plumbing.ObjectType, size int64, source io.Reader) (plumbing.Hash, error) {
	writer, err := storage.RawObjectWriter(kind, size)
	if err != nil {
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
	closed := writer.Close()
	if err != nil {
		return plumbing.ZeroHash, err
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
