package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/objfile"
	"io"
)

type rootMeta struct {
	Validated bool `json:",omitempty"`
	Format    config.ObjectFormat
	Head      string
	Refs      map[string]string
	Buckets   []string
}

func decodeRoot(data []byte, format config.ObjectFormat, children int) (rootMeta, error) {
	var meta rootMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return meta, err
	}
	if !meta.Validated || meta.Format != format || len(meta.Buckets) != children {
		return meta, fmt.Errorf("invalid repository root")
	}
	return meta, nil
}

// Keep full catalog leaves below the WAL node limit, including base64 encoding.
const inlineObjectLimit = 512

type objectMeta struct {
	ID         string
	Kind       plumbing.ObjectType
	Size       int64
	Encoding   string `json:",omitempty"`
	StoredSize int64  `json:",omitempty"`
	Inline     []byte `json:",omitempty"`
}

func (m objectMeta) validInline() bool {
	return len(m.Inline) > 0 && len(m.Inline) <= inlineObjectLimit && m.Encoding == "zlib" && int64(len(m.Inline)) == m.StoredSize
}

func (m objectMeta) readInline(format config.ObjectFormat) (io.ReadCloser, error) {
	if !m.validInline() {
		return nil, fmt.Errorf("invalid inline object")
	}
	return readLoose(io.NopCloser(bytes.NewReader(m.Inline)), format, m.Kind, m.Size, plumbing.NewHash(m.ID))
}

// Close releases both the codec and the underlying WAL chunk handles.
// Full reads verify decoded size and Git identity, not only the zlib stream.
func readLoose(source io.ReadCloser, format config.ObjectFormat, kind plumbing.ObjectType, size int64, id plumbing.Hash) (io.ReadCloser, error) {
	body, err := objfile.NewReader(source, format)
	if err == nil {
		actualKind, actualSize, headerErr := body.Header()
		err = headerErr
		if err == nil && (actualKind != kind || actualSize != size || size < 0) {
			err = fmt.Errorf("object header differs from index")
		}
	}
	if err != nil {
		if body != nil {
			_ = body.Close()
		}
		_ = source.Close()
		return nil, err
	}
	return &looseReader{body: body, source: source, remaining: size, id: id}, nil
}

type looseReader struct {
	body      *objfile.Reader
	source    io.ReadCloser
	remaining int64
	id        plumbing.Hash
	err       error
}

func (r *looseReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.err != nil {
		return 0, r.err
	}
	n, err := r.body.Read(p[:min(int64(len(p)), r.remaining)])
	r.remaining -= int64(n)
	if err == nil && r.remaining == 0 {
		var extra [1]byte
		var count int
		count, err = r.body.Read(extra[:])
		if count != 0 {
			err = fmt.Errorf("object exceeds declared size")
		}
	}
	if err == io.EOF {
		if r.remaining != 0 {
			err = io.ErrUnexpectedEOF
		} else if r.body.Hash() != r.id {
			err = fmt.Errorf("object hash differs from index")
		}
	}
	if err != nil {
		r.err = err
	}
	return n, err
}
func (r *looseReader) Close() error {
	err := errors.Join(r.body.Close(), r.source.Close())
	r.err = io.ErrClosedPipe
	return err
}
