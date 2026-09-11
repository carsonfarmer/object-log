package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/go-git/go-git/v6/plumbing"
	format "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

func importPack(ctx context.Context, source io.ReadSeeker, storage storer.EncodedObjectStorer, objectFormat format.ObjectFormat, limits requestLimits) error {
	if objectFormat != format.SHA1 && objectFormat != format.SHA256 {
		return fmt.Errorf("invalid object format")
	}
	if limits.objectBytes <= 0 || limits.metadataBytes <= 0 || limits.packObjects <= 0 {
		return errObjectLimit
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	size, err := source.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err = source.Seek(0, io.SeekStart); err != nil {
		return err
	}
	admission := &packAdmission{ctx: ctx, limits: limits, size: size}
	parser := packfile.NewParser(&packSource{ReadSeeker: source, ctx: ctx},
		packfile.WithStorage(storage), packfile.WithObjectFormat(objectFormat),
		packfile.WithMaxObjectSize(limits.objectBytes), packfile.WithScannerObservers(admission))
	admission.parser = parser
	_, err = parser.Parse()
	if errors.Is(err, packfile.ErrObjectTooLarge) {
		return fmt.Errorf("%w: GIT_MAX_OBJECT_BYTES", errObjectLimit)
	}
	if err != nil {
		return err
	}
	if parser.BytesRead() != size {
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

type packAdmission struct {
	ctx    context.Context
	limits requestLimits
	parser *packfile.Parser
	size   int64
}

func (o packAdmission) OnHeader(count uint32) error {
	if int64(count) > o.limits.packObjects {
		return fmt.Errorf("%w: GIT_MAX_PACK_OBJECTS", errObjectLimit)
	}
	return o.ctx.Err()
}
func (o packAdmission) OnInflatedObjectHeader(kind plumbing.ObjectType, size, _ int64) error {
	if err := o.ctx.Err(); err != nil {
		return err
	}
	// Delta callbacks follow framing; reject trailing bytes before reconstruction.
	if n := o.parser.BytesRead(); n != 0 && n != o.size {
		return packfile.ErrMalformedPackfile
	}
	return o.limits.checkObject(kind, size)
}
func (o packAdmission) OnInflatedObjectContent(plumbing.Hash, int64, uint32, []byte) error {
	return o.ctx.Err()
}
func (o packAdmission) OnFooter(plumbing.Hash) error { return o.ctx.Err() }
