package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
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
		return zero, fmt.Errorf("expired view")
	}
	return zero, fmt.Errorf("wal: %s", r.Err().Other())
}

type objectMeta struct {
	ID   string
	Kind plumbing.ObjectType
	Size int64
}
type indexed struct {
	objectMeta
	root *wal.Object
}
type rootMeta struct {
	Format  config.ObjectFormat
	Refs    map[string]string
	Buckets []string
}
type store struct {
	owned []*wal.Object
	storage.Storer
	failure error
	session *wal.Session
	meta    rootMeta
	buckets map[string]*wal.Object
	loaded  map[string]map[string]indexed
	pending map[string]indexed
}

func openStore(session *wal.Session, format config.ObjectFormat) (result *store, err error) {
	mem := memory.NewStorage()
	if e := mem.SetObjectFormat(format); e != nil {
		return nil, e
	}
	s := &store{Storer: mem, session: session, buckets: map[string]*wal.Object{}, loaded: map[string]map[string]indexed{}, pending: map[string]indexed{}}
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
		s.owned = append(s.owned, record.Objects...)
	}
	if len(records) > 0 {
		last := records[len(records)-1]
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
	_ = s.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, "refs/heads/main"))
	return s, nil
}
func (s *store) bucket(key string) (map[string]indexed, error) {
	if cached, ok := s.loaded[key]; ok {
		return cached, nil
	}
	result := map[string]indexed{}
	if root, ok := s.buckets[key]; ok {
		entry, e := s.readNode(root)
		if e != nil {
			return nil, e
		}
		var items []objectMeta
		if e = json.Unmarshal(entry.Data, &items); e != nil {
			return nil, e
		}
		if len(items) != len(entry.Objects) {
			return nil, fmt.Errorf("invalid index leaf")
		}
		for i, item := range items {
			result[item.ID] = indexed{item, entry.Objects[i]}
		}
	}
	s.loaded[key] = result
	return result, nil
}
func (s *store) lookup(id plumbing.Hash) (indexed, error) {
	key := id.String()
	if item, ok := s.pending[key]; ok {
		return item, nil
	}
	bucket, e := s.bucket(key[:2])
	if e != nil {
		return indexed{}, e
	}
	item, ok := bucket[key]
	if !ok {
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
	for key := range s.buckets {
		bucket, e := s.bucket(key)
		if e != nil {
			return nil, e
		}
		for id, item := range bucket {
			items[id] = item
		}
	}
	for id, item := range s.pending {
		items[id] = item
	}
	result := make([]plumbing.EncodedObject, 0, len(items))
	for _, item := range items {
		if kind == plumbing.AnyObject || item.Kind == kind {
			result = append(result, &storedObject{s, item})
		}
	}
	return storer.NewEncodedObjectSliceIter(result), nil
}
func (s *store) RawObjectWriter(kind plumbing.ObjectType, size int64) (io.WriteCloser, error) {
	if size < 0 {
		return nil, fmt.Errorf("invalid object size")
	}
	return &objectWriter{s: s, kind: kind, size: size, hash: plumbing.NewHasher(s.meta.Format, kind, size)}, nil
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
	return w.(*objectWriter).hash.Sum(), nil
}

type objectWriter struct {
	s             *store
	kind          plumbing.ObjectType
	size, written int64
	hash          plumbing.Hasher
	buf           []byte
	chunks        []*wal.Object
	closed        bool
}

func (w *objectWriter) Write(p []byte) (int, error) {
	if w.closed || int64(len(p)) > w.size-w.written {
		return 0, fmt.Errorf("invalid object write")
	}
	n := len(p)
	_, _ = w.hash.Write(p)
	w.written += int64(n)
	for len(p) > 0 {
		k := min(chunkSize-len(w.buf), len(p))
		w.buf = append(w.buf, p[:k]...)
		p = p[k:]
		if len(w.buf) == chunkSize {
			if e := w.flush(); e != nil {
				return 0, e
			}
		}
	}
	return n, nil
}
func (w *objectWriter) flush() error {
	root, e := w.s.put(w.buf)
	if e != nil {
		return e
	}
	w.chunks = append(w.chunks, root)
	w.buf = nil
	return nil
}
func (w *objectWriter) Close() (err error) {
	defer func() {
		if err != nil {
			w.s.failure = err
		}
	}()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.written != w.size {
		return fmt.Errorf("incomplete object")
	}
	if len(w.buf) > 0 {
		if e := w.flush(); e != nil {
			return e
		}
	}
	root, e := w.s.putNode(nil, w.chunks)
	if e != nil {
		return e
	}
	id := w.hash.Sum().String()
	w.s.pending[id] = indexed{objectMeta{id, w.kind, w.size}, root}
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
	entry, e := o.s.readNode(o.item.root)
	if e != nil {
		return nil, e
	}
	return &objectReader{s: o.s, roots: entry.Objects, remaining: o.item.Size}, nil
}

type objectReader struct {
	s         *store
	roots     []*wal.Object
	buf       *bytes.Reader
	remaining int64
}

func (r *objectReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	for r.buf == nil || r.buf.Len() == 0 {
		if len(r.roots) == 0 {
			return 0, io.ErrUnexpectedEOF
		}
		b, e := unwrap(r.s.session.Read(r.roots[0]))
		if e != nil {
			return 0, e
		}
		r.roots = r.roots[1:]
		r.buf = bytes.NewReader(b)
	}
	n, e := r.buf.Read(p[:min(int64(len(p)), r.remaining)])
	r.remaining -= int64(n)
	return n, e
}
func (r *objectReader) Close() error { return nil }
func (s *store) publish(refs map[string]string) error {
	if s.failure != nil {
		return s.failure
	}
	changed := map[string]bool{}
	for id, item := range s.pending {
		key := id[:2]
		bucket, e := s.bucket(key)
		if e != nil {
			return e
		}
		bucket[id] = item
		changed[key] = true
	}
	for key := range changed {
		bucket := s.loaded[key]
		keys := make([]string, 0, len(bucket))
		for id := range bucket {
			keys = append(keys, id)
		}
		sort.Strings(keys)
		items := make([]objectMeta, 0, len(keys))
		children := make([]*wal.Object, 0, len(keys))
		for _, id := range keys {
			items = append(items, bucket[id].objectMeta)
			children = append(children, bucket[id].root)
		}
		data, e := json.Marshal(items)
		if e != nil {
			return e
		}
		root, e := s.putNode(data, children)
		if e != nil {
			return e
		}
		s.buckets[key] = root
	}
	keys := make([]string, 0, len(s.buckets))
	for key := range s.buckets {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	children := make([]*wal.Object, 0, len(keys))
	for _, key := range keys {
		children = append(children, s.buckets[key])
	}
	s.meta.Refs = refs
	s.meta.Buckets = keys
	data, e := json.Marshal(s.meta)
	if e != nil {
		return e
	}
	root, e := s.putNode(data, children)
	if e != nil {
		return e
	}
	candidate, e := unwrap(s.session.Prepare(nil, []*wal.Object{root}))
	if e != nil {
		return e
	}
	defer candidate.Drop()
	token, e := unwrap(candidate.Token())
	if e != nil {
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
		return &pendingError{token: token}
	default:
		return fmt.Errorf("publication conflict or expired view")
	}
}

func (s *store) Close() {
	for _, o := range s.owned {
		o.Drop()
	}
	s.owned = nil
}
func (s *store) readNode(root *wal.Object) (wal.Entry, error) {
	entry, e := unwrap(s.session.ReadNode(root))
	if e == nil {
		s.owned = append(s.owned, entry.Objects...)
	}
	return entry, e
}
func (s *store) put(b []byte) (*wal.Object, error) {
	o, e := unwrap(s.session.Put(b))
	if e == nil {
		s.owned = append(s.owned, o)
	}
	return o, e
}
func (s *store) putNode(b []byte, children []*wal.Object) (*wal.Object, error) {
	o, e := unwrap(s.session.PutNode(b, children))
	if e == nil {
		s.owned = append(s.owned, o)
	}
	return o, e
}

type pendingError struct{ token []byte }

func (*pendingError) Error() string { return "publication pending" }
