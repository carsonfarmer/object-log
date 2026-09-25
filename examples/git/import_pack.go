package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"crypto"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/go-git/go-git/v6/plumbing"
	format "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	packutil "github.com/go-git/go-git/v6/plumbing/format/packfile/util"
	githash "github.com/go-git/go-git/v6/plumbing/hash"
	"github.com/go-git/go-git/v6/plumbing/storer"
	gitbinary "github.com/go-git/go-git/v6/utils/binary"
)

func importPack(ctx context.Context, source io.ReadSeeker, storage storer.EncodedObjectStorer, objectFormat format.ObjectFormat, limits requestLimits, observers ...packfile.Observer) error {
	if objectFormat != format.SHA1 && objectFormat != format.SHA256 {
		return fmt.Errorf("invalid object format")
	}
	if limits.packObjects <= 0 {
		return errObjectLimit
	}
	input := &packSource{ReadSeeker: source, ctx: ctx}
	size, err := input.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err = input.Seek(0, io.SeekStart); err != nil {
		return err
	}
	// Check the count before go-git allocates its per-object index.
	var header [12]byte
	if _, err := io.ReadFull(input, header[:]); err != nil {
		return err
	}
	if int64(binary.BigEndian.Uint32(header[8:])) > limits.packObjects {
		return fmt.Errorf("%w: GIT_MAX_PACK_OBJECTS", errObjectLimit)
	}
	if size < int64(len(header)+objectFormat.Size()) || string(header[:4]) != "PACK" || binary.BigEndian.Uint32(header[4:8]) != packfile.VersionSupported {
		return packfile.ErrMalformedPackfile
	}
	checksum := githash.New(crypto.SHA1)
	if objectFormat == format.SHA256 {
		checksum = githash.New(crypto.SHA256)
	}
	_, _ = checksum.Write(header[:])
	// Buffer once for zlib's byte reads. The limit keeps the footer out of
	// both the scan and the checksum.
	data := &io.LimitedReader{R: input, N: size - int64(len(header)+objectFormat.Size())}
	reader := bufio.NewReaderSize(io.TeeReader(data, checksum), 32<<10)
	for range binary.BigEndian.Uint32(header[8:]) {
		if err := checkPackEntry(reader, objectFormat, limits); err != nil {
			return err
		}
	}
	if data.N != 0 || reader.Buffered() != 0 {
		return packfile.ErrMalformedPackfile
	}
	footer := make([]byte, objectFormat.Size())
	if _, err := io.ReadFull(input, footer); err != nil {
		return err
	}
	if !bytes.Equal(checksum.Sum(nil), footer) {
		return packfile.ErrMalformedPackfile
	}
	if _, err = input.Seek(0, io.SeekStart); err != nil {
		return err
	}
	parser := packfile.NewParser(input,
		packfile.WithStorage(storage), packfile.WithObjectFormat(objectFormat),
		packfile.WithScannerObservers(observers...))
	parsed, err := parser.Parse()
	if err != nil {
		return err
	}
	if parsed.Compare(checksum.Sum(nil)) != 0 {
		return packfile.ErrMalformedPackfile
	}
	return ctx.Err()
}

type packByteReader struct{ io.Reader }

func (r *packByteReader) ReadByte() (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(r, b[:])
	return b[0], err
}

func checkPackEntry(r *bufio.Reader, objectFormat format.ObjectFormat, limits requestLimits) error {
	first, err := r.ReadByte()
	if err != nil {
		return err
	}
	kind := packutil.ObjectType(first)
	if !kind.Valid() {
		return packfile.ErrMalformedPackfile
	}
	size, err := packutil.VariableLengthSize(first, r)
	if err != nil || size >= math.MaxInt64 {
		return packfile.ErrMalformedPackfile
	}
	if kind.IsDelta() {
		if kind == plumbing.REFDeltaObject {
			_, err = io.CopyN(io.Discard, r, int64(objectFormat.Size()))
		} else {
			_, err = gitbinary.ReadVariableWidthInt(r)
		}
		if err != nil {
			return err
		}
	} else if err := limits.checkObject(kind, int64(size)); err != nil {
		return err
	}
	zr, err := zlib.NewReader(r)
	if err != nil {
		return err
	}
	defer zr.Close()
	decoded := &io.LimitedReader{R: zr, N: int64(size) + 1}
	if kind.IsDelta() {
		fields := &packByteReader{Reader: decoded}
		if _, err := binary.ReadUvarint(fields); err != nil {
			return err
		}
		result, err := binary.ReadUvarint(fields)
		if err != nil {
			return err
		}
		if result > uint64(limits.objectBytes) {
			return fmt.Errorf("%w: GIT_MAX_OBJECT_BYTES", errObjectLimit)
		}
	}
	if _, err := io.Copy(io.Discard, decoded); err != nil {
		return err
	}
	if decoded.N != 1 {
		return packfile.ErrMalformedPackfile
	}
	return nil
}

type packSource struct {
	io.ReadSeeker
	ctx context.Context
}

func (r *packSource) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.ReadSeeker.Read(p)
}
