package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/objfile"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"
	wt "go.bytecodealliance.org/pkg/wit/types"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
)

const chunkSize = 1024 * 1024

func unwrap[T any](r wt.Result[T, wal.Failure]) (T, error) {
	if r.IsOk() {
		return r.Ok(), nil
	}
	var zero T
	if r.Err().Tag() == wal.FailureExpired {
		return zero, errExpired
	}
	return zero, fmt.Errorf("wal: %s", r.Err().Other())
}

type indexed struct {
	objectMeta
	root *wal.Object
}
type rootMeta struct {
	Validated bool `json:",omitempty"`
	Format    config.ObjectFormat
	Head      string
	Refs      map[string]string
	Buckets   []string
}
type store struct {
	ctx            context.Context // Request-scoped; go-git storage methods do not accept contexts.
	maxObjectBytes int64
	owned          []*wal.Object
	storage.Storer
	failure     error
	tail        bool
	tailEntries int
	session     *wal.Session
	meta        rootMeta
	buckets     map[string]*wal.Object
	loaded      map[*wal.Object]radixNode[indexed, *wal.Object]
	pending     map[string]indexed
}

func openStore(ctx context.Context, session *wal.Session, format config.ObjectFormat, maxObjectBytes int64) (result *store, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	mem := memory.NewStorage()
	if e := mem.SetObjectFormat(format); e != nil {
		return nil, e
	}
	cfg, e := mem.Config()
	if e != nil {
		return nil, e
	}
	// A nonzero window limits object count, not bytes: go-git buffers delta
	// bases and targets. Keep full-object streaming until selection can bound bytes.
	cfg.Pack.Window = 0
	if e = mem.SetConfig(cfg); e != nil {
		return nil, e
	}
	s := &store{ctx: ctx, maxObjectBytes: maxObjectBytes, Storer: mem, session: session, buckets: map[string]*wal.Object{}, loaded: map[*wal.Object]radixNode[indexed, *wal.Object]{}, pending: map[string]indexed{}}
	defer func() {
		if result == nil {
			s.Close()
		}
	}()
	s.meta = rootMeta{Format: format, Refs: map[string]string{}}
	records, e := unwrap(session.Records())
	if e != nil {
		return nil, e
	}
	for _, record := range records {
		if !record.Snapshot {
			s.tailEntries++
		}
		s.owned = append(s.owned, record.Objects...)
	}
	if len(records) > 0 {
		last := records[len(records)-1]
		s.tail = !last.Snapshot
		if len(last.Objects) != 1 {
			return nil, fmt.Errorf("invalid root record")
		}
		root, e := s.readNode(last.Objects[0])
		if e != nil {
			return nil, e
		}
		if e = json.Unmarshal(root.Data, &s.meta); e != nil {
			return nil, e
		}
		if s.meta.Format != format || len(s.meta.Buckets) != len(root.Objects) {
			return nil, fmt.Errorf("invalid repository root")
		}
		for i, key := range s.meta.Buckets {
			s.buckets[key] = root.Objects[i]
		}
		for name, id := range s.meta.Refs {
			_ = s.Storer.SetReference(plumbing.NewHashReference(plumbing.ReferenceName(name), plumbing.NewHash(id)))
		}
	}
	head := s.meta.Head
	if head == "" {
		branch := "main"
		if len(records) == 0 && os.Getenv("WAL_DEFAULT_BRANCH") != "" {
			branch = os.Getenv("WAL_DEFAULT_BRANCH")
		}
		head = "refs/heads/" + branch
	}
	if !strings.HasPrefix(head, "refs/heads/") {
		return nil, fmt.Errorf("invalid default branch")
	}
	if e := plumbing.ReferenceName(head).Validate(); e != nil {
		return nil, e
	}
	s.meta.Head = head
	if e := s.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.ReferenceName(head))); e != nil {
		return nil, e
	}
	return s, nil
}
func (s *store) lookup(id plumbing.Hash) (indexed, error) {
	if err := s.ctx.Err(); err != nil {
		return indexed{}, err
	}
	key := id.String()
	if item, ok := s.pending[key]; ok {
		return item, nil
	}
	root, ok := s.buckets[key[:2]]
	if !ok {
		return indexed{}, plumbing.ErrObjectNotFound
	}
	item, found, err := lookupRadix(key, root, s.loadBucket)
	if err != nil {
		return indexed{}, err
	}
	if !found {
		return indexed{}, plumbing.ErrObjectNotFound
	}
	return item, nil
}
func (s *store) EncodedObject(kind plumbing.ObjectType, id plumbing.Hash) (plumbing.EncodedObject, error) {
	item, e := s.lookup(id)
	if e != nil {
		return nil, e
	}
	if kind != plumbing.AnyObject && kind != item.Kind {
		return nil, plumbing.ErrObjectNotFound
	}
	return &storedObject{s, item}, nil
}
func (s *store) HasEncodedObject(id plumbing.Hash) error { _, e := s.lookup(id); return e }
func (s *store) EncodedObjectSize(id plumbing.Hash) (int64, error) {
	v, e := s.lookup(id)
	return v.Size, e
}
func (s *store) IterEncodedObjects(kind plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	items := map[string]indexed{}
	for _, root := range s.buckets {
		if e := walkRadix(root, s.loadBucket, func(id string, item indexed) { items[id] = item }); e != nil {
			return nil, e
		}
	}
	maps.Copy(items, s.pending)
	result := make([]plumbing.EncodedObject, 0, len(items))
	for _, item := range items {
		if kind == plumbing.AnyObject || item.Kind == kind {
			result = append(result, &storedObject{s, item})
		}
	}
	return storer.NewEncodedObjectSliceIter(result), nil
}
func (s *store) RawObjectWriter(kind plumbing.ObjectType, size int64) (io.WriteCloser, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	if size > s.maxObjectBytes {
		s.failure = errObjectLimit
		return nil, errObjectLimit
	}
	if size < 0 {
		return nil, fmt.Errorf("invalid object size")
	}
	sink := &chunkWriter{s: s}
	codec := objfile.NewWriter(sink, s.meta.Format)
	if err := codec.WriteHeader(kind, size); err != nil {
		_ = codec.Close()
		return nil, err
	}
	return &objectWriter{s: s, kind: kind, size: size, codec: codec, sink: sink}, nil
}
func (s *store) SetEncodedObject(o plumbing.EncodedObject) (plumbing.Hash, error) {
	r, e := o.Reader()
	if e != nil {
		return plumbing.ZeroHash, e
	}
	defer r.Close()
	w, e := s.RawObjectWriter(o.Type(), o.Size())
	if e != nil {
		return plumbing.ZeroHash, e
	}
	if _, e = io.Copy(w, r); e != nil {
		return plumbing.ZeroHash, e
	}
	if e = w.Close(); e != nil {
		return plumbing.ZeroHash, e
	}
	return w.(*objectWriter).codec.Hash(), nil
}

type objectWriter struct {
	s             *store
	kind          plumbing.ObjectType
	size, written int64
	codec         *objfile.Writer
	sink          *chunkWriter
	closed        bool
	err           error
}

func (w *objectWriter) Write(p []byte) (int, error) {
	if err := w.s.ctx.Err(); err != nil {
		w.err = err
		return 0, err
	}
	if w.closed || int64(len(p)) > w.size-w.written {
		if w.err == nil {
			w.err = fmt.Errorf("invalid object write")
		}
		return 0, w.err
	}
	n, err := w.codec.Write(p)
	w.written += int64(n)
	if err != nil {
		w.err = err
	}
	return n, err
}
func (w *objectWriter) Close() (err error) {
	if w.closed {
		return w.err
	}
	w.closed = true
	defer func() {
		w.err = err
		if err != nil {
			w.s.failure = err
		}
	}()
	closed := w.codec.Close()
	if w.err != nil {
		return w.err
	}
	if closed != nil {
		return closed
	}
	if w.written != w.size {
		return fmt.Errorf("incomplete object")
	}
	id := w.codec.Hash().String()
	item := indexed{objectMeta: objectMeta{ID: id, Kind: w.kind, Size: w.size, Encoding: "zlib", StoredSize: w.sink.written}}
	if len(w.sink.chunks) == 0 && len(w.sink.buf) <= inlineObjectLimit {
		item.Inline = w.sink.buf
	} else {
		if len(w.sink.buf) > 0 {
			if err = w.sink.flush(); err != nil {
				return err
			}
		}
		item.root, err = w.s.putNode(nil, w.sink.chunks)
		if err != nil {
			return err
		}
	}
	w.sink.buf = nil
	w.s.pending[id] = item
	return nil
}

type chunkWriter struct {
	s       *store
	written int64
	buf     []byte
	chunks  []*wal.Object
}

func (w *chunkWriter) Write(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 {
		k := min(chunkSize-len(w.buf), len(p))
		w.buf = append(w.buf, p[:k]...)
		p = p[k:]
		w.written += int64(k)
		if len(w.buf) == chunkSize {
			if err := w.flush(); err != nil {
				return n - len(p), err
			}
		}
	}
	return n, nil
}
func (w *chunkWriter) flush() error {
	root, err := w.s.put(w.buf)
	if err != nil {
		return err
	}
	w.chunks = append(w.chunks, root)
	w.buf = nil
	return nil
}

type storedObject struct {
	s    *store
	item indexed
}

func (o *storedObject) Hash() plumbing.Hash             { return plumbing.NewHash(o.item.ID) }
func (o *storedObject) Type() plumbing.ObjectType       { return o.item.Kind }
func (o *storedObject) Size() int64                     { return o.item.Size }
func (o *storedObject) SetType(plumbing.ObjectType)     { panic("immutable object") }
func (o *storedObject) SetSize(int64)                   { panic("immutable object") }
func (o *storedObject) Writer() (io.WriteCloser, error) { return nil, fmt.Errorf("immutable object") }
func (o *storedObject) Reader() (io.ReadCloser, error) {
	if err := o.s.ctx.Err(); err != nil {
		return nil, err
	}
	if o.item.Encoding != "" && o.item.Encoding != "zlib" {
		return nil, fmt.Errorf("unknown object encoding")
	}
	if len(o.item.Inline) > 0 {
		return o.item.readInline(o.s.meta.Format)
	}
	entry, err := unwrap(o.s.session.ReadNode(o.item.root))
	o.s.observeRead(err)
	if err != nil {
		return nil, err
	}
	size := o.item.Size
	if o.item.Encoding != "" {
		size = o.item.StoredSize
	}
	source := &objectReader{s: o.s, roots: entry.Objects, remaining: size}
	if size < 0 {
		_ = source.Close()
		return nil, fmt.Errorf("invalid object size")
	}
	if o.item.Encoding == "" {
		return source, nil
	}
	return readLoose(source, o.s.meta.Format, o.item.Kind, o.item.Size, o.Hash())
}

type objectReader struct {
	s         *store
	roots     []*wal.Object
	buf       *bytes.Reader
	remaining int64
}

func (r *objectReader) Read(p []byte) (int, error) {
	if err := r.s.ctx.Err(); err != nil {
		return 0, err
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	for r.buf == nil || r.buf.Len() == 0 {
		if len(r.roots) == 0 {
			return 0, io.ErrUnexpectedEOF
		}
		b, e := unwrap(r.s.session.Read(r.roots[0]))
		r.s.observeRead(e)
		if e != nil {
			return 0, e
		}
		r.roots[0].Drop()
		r.roots = r.roots[1:]
		r.buf = bytes.NewReader(b)
	}
	n, e := r.buf.Read(p[:min(int64(len(p)), r.remaining)])
	r.remaining -= int64(n)
	return n, e
}
func (r *objectReader) Close() error {
	for _, root := range r.roots {
		root.Drop()
	}
	r.roots = nil
	r.buf = nil
	r.remaining = 0
	return nil
}
func (s *store) publish(refs map[string]string) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if s.failure != nil {
		return s.failure
	}
	changed, e := partition(s.pending, 2)
	if e != nil {
		return e
	}
	for key, updates := range changed {
		node := radixNode[indexed, *wal.Object]{}
		if root, ok := s.buckets[key]; ok {
			node, e = s.loadBucket(root)
			if e != nil {
				return e
			}
		}
		root, e := updateRadix(key, node, updates, s.loadBucket, s.saveBucket)
		if e != nil {
			return e
		}
		s.buckets[key] = root
	}
	root, e := s.stageRoot(refs)
	if e != nil {
		return e
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	candidate, e := unwrap(s.session.Prepare(nil, []*wal.Object{root}))
	if e != nil {
		return e
	}
	defer candidate.Drop()
	if e := s.ctx.Err(); e != nil {
		return e
	}
	result, e := unwrap(candidate.Publish())
	if e != nil {
		return e
	}
	switch result.Tag() {
	case wal.OutcomeCommitted:
		return nil
	case wal.OutcomePending:
		return &pendingError{token: result.Pending()}
	default:
		return fmt.Errorf("publication conflict or expired view")
	}
}

func (s *store) stageRoot(refs map[string]string) (*wal.Object, error) {
	keys := slices.AppendSeq(make([]string, 0, len(s.buckets)), maps.Keys(s.buckets))
	slices.Sort(keys)
	children := make([]*wal.Object, 0, len(keys))
	for _, key := range keys {
		children = append(children, s.buckets[key])
	}
	s.meta.Refs = refs
	s.meta.Buckets = keys
	data, e := json.Marshal(s.meta)
	if e != nil {
		return nil, e
	}
	return s.putNode(data, children)
}

func (s *store) Close() {
	for _, o := range s.owned {
		o.Drop()
	}
	s.owned = nil
}
func (s *store) readNode(root *wal.Object) (wal.Entry, error) {
	if err := s.ctx.Err(); err != nil {
		return wal.Entry{}, err
	}
	entry, e := unwrap(s.session.ReadNode(root))
	s.observeRead(e)
	if e == nil {
		s.owned = append(s.owned, entry.Objects...)
	}
	return entry, e
}
func (s *store) put(b []byte) (*wal.Object, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	o, e := unwrap(s.session.Put(b))
	if e == nil {
		s.owned = append(s.owned, o)
	}
	return o, e
}
func (s *store) putNode(b []byte, children []*wal.Object) (*wal.Object, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	o, e := unwrap(s.session.PutNode(b, children))
	if e == nil {
		s.owned = append(s.owned, o)
	}
	return o, e
}

type pendingError struct{ token []byte }

func (*pendingError) Error() string { return "publication pending" }

func (s *store) observeRead(err error) {
	if err == errExpired {
		s.failure = err
	}
}
