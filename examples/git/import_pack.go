package main

import (
	"context"
	"crypto"
	"encoding/binary"
	"fmt"
	"io"

	format "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	githash "github.com/go-git/go-git/v6/plumbing/hash"
	"github.com/go-git/go-git/v6/plumbing/storer"
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
	if size < int64(len(header)+objectFormat.Size()) {
		return packfile.ErrMalformedPackfile
	}
	if _, err = input.Seek(0, io.SeekStart); err != nil {
		return err
	}
	checksum := githash.New(crypto.SHA1)
	if objectFormat == format.SHA256 {
		checksum = githash.New(crypto.SHA256)
	}
	// Compare the complete stream with the parser's framed checksum. This also
	// rejects trailing bytes without depending on the parser's final seek position.
	if _, err := io.CopyN(checksum, input, size-int64(objectFormat.Size())); err != nil {
		return err
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
