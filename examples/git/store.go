package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/objfile"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"
	wt "go.bytecodealliance.org/pkg/wit/types"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
)

type indexed struct {
	objectMeta
	root      *wal.Object
	deltaRoot *wal.Object
}
type store struct {
	ctx     context.Context // Request-scoped; go-git storage methods do not accept contexts.
	limits  requestLimits
	owned   []*wal.Object
	writers []*byteWriter
	storage.Storer
	failure       error
	progress      *receiveProgress
	tailEntries   uint64
	session       *wal.Session
	recovery      *wal.Recovery
	stateRoot     *wal.Object
	meta          rootMeta
	buckets       map[string]*wal.Object
	loaded        map[bucketKey]radixNode[indexed, *wal.Object]
	pending       map[string]indexed
	pendingInline int64
}

func openStore(ctx context.Context, session *wal.Session, format config.ObjectFormat, limits requestLimits) (result *store, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := &store{ctx: ctx, limits: limits, session: session, buckets: map[string]*wal.Object{}, loaded: map[bucketKey]radixNode[indexed, *wal.Object]{}, pending: map[string]indexed{}}
	defer func() {
		if result == nil {
			s.Close()
		}
	}()
	s.meta = rootMeta{Version: 1, Format: format, Refs: map[string]string{}}
	recovery, e := storeCall(s, session.Recover)
	if e != nil {
		return nil, e
	}
	s.recovery = recovery
	s.stateRoot, s.tailEntries, e = s.acceptRecovery(recovery)
	if e != nil {
		return nil, e
	}
	if s.stateRoot != nil {
		root, e := s.readNode(s.stateRoot)
		if e != nil {
			return nil, e
		}
		if s.meta, e = decodeRoot(root.Data, format, len(root.Objects)); e != nil {
			return nil, e
		}
		for i, key := range s.meta.Buckets {
			s.buckets[key] = root.Objects[i]
		}
	}
	if s.meta.Format == "" {
		s.meta.Format = config.SHA1
	}
	s.Storer = memory.NewStorage(memory.WithObjectFormat(s.meta.Format))
	cfg, _ := s.Storer.Config()
	// Reuse stored deltas without comparing object contents to generate new ones.
	cfg.Pack.Window = 1
	for name, id := range s.meta.Refs {
		if e := s.Storer.SetReference(plumbing.NewHashReference(plumbing.ReferenceName(name), plumbing.NewHash(id))); e != nil {
			return nil, e
		}
	}
	if s.stateRoot == nil {
		s.meta.Head = "refs/heads/main"
	}
	if e := s.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.ReferenceName(s.meta.Head))); e != nil {
		return nil, e
	}
	return s, nil
}
func (s *store) lookup(id plumbing.Hash) (indexed, error) {
	if err := s.ctx.Err(); err != nil {
		observeRead(&s.failure, err)
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
	item, found, err := lookupRadix(key, key[:2], root, s.loadBucket)
	if err != nil {
		return indexed{}, err
	}
	if !found {
		return indexed{}, plumbing.ErrObjectNotFound
	}
	return item, nil
}
func (s *store) EncodedObject(kind plumbing.ObjectType, id plumbing.Hash) (object plumbing.EncodedObject, err error) {
	defer func() {
		if !errors.Is(err, plumbing.ErrObjectNotFound) {
			observeRead(&s.failure, err)
		}
	}()
	item, e := s.lookup(id)
	if e != nil {
		return nil, e
	}
	if kind != plumbing.AnyObject && kind != item.Kind {
		return nil, plumbing.ErrObjectNotFound
	}
	if err := s.limits.checkObject(item.Kind, item.Size); err != nil {
		return nil, err
	}
	return &storedObject{s, item}, nil
}
func (s *store) HasEncodedObject(id plumbing.Hash) error { _, e := s.lookup(id); return e }
func (s *store) DeltaObject(kind plumbing.ObjectType, id plumbing.Hash) (plumbing.EncodedObject, error) {
	object, err := s.EncodedObject(kind, id)
	if err != nil {
		return nil, err
	}
	item := object.(*storedObject).item
	if delta := item.Delta; delta != nil {
		result := storedDelta{EncodedObject: object, delta: delta, failure: &s.failure}
		if delta.StoredSize > 0 {
			result.open = func() (io.ReadCloser, error) { return s.openDelta(item) }
		}
		return result, nil
	}
	return object, nil
}
func (s *store) EncodedObjectSize(id plumbing.Hash) (int64, error) {
	v, e := s.lookup(id)
	return v.Size, e
}
func (*store) IterEncodedObjects(plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	return nil, errors.ErrUnsupported
}
func (s *store) RawObjectWriter(kind plumbing.ObjectType, size int64) (io.WriteCloser, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.limits.checkObject(kind, size); err != nil {
		observeRead(&s.failure, err)
		return nil, err
	}
	sink := &objectSink{s: s}
	codec := objfile.NewWriter(sink, s.meta.Format)
	if err := codec.WriteHeader(kind, size); err != nil {
		_ = codec.Close()
		return nil, err
	}
	writer := &objectWriter{Writer: codec, remaining: size}
	return closingWriter(s.ctx, writer, &s.failure, func() (err error) {
		defer func() {
			if sink.writer != nil {
				sink.writer.close()
			}
		}()
		if err := errors.Join(s.failure, codec.Close()); err != nil {
			return err
		}
		if writer.remaining != 0 {
			return fmt.Errorf("incomplete object")
		}
		id := codec.Hash().String()
		replaced := len(s.pending[id].Inline)
		if sink.writer == nil && int64(len(sink.prefix)) > s.limits.inlineRemaining(s.pendingInline, replaced) {
			if err := sink.spill(); err != nil {
				return err
			}
		}
		item := indexed{objectMeta: objectMeta{ID: id, Kind: kind, Size: size, StoredSize: sink.written}}
		if sink.writer == nil {
			item.Inline = sink.prefix
		} else {
			item.root, err = sink.writer.finish()
			if err != nil {
				return err
			}
			s.owned = append(s.owned, item.root)
		}
		sink.prefix = nil
		s.pendingInline += int64(len(item.Inline) - replaced)
		s.pending[id] = item
		return nil
	}), nil
}
func (*store) SetEncodedObject(plumbing.EncodedObject) (plumbing.Hash, error) {
	return plumbing.ZeroHash, errors.ErrUnsupported
}

// Keep tiny compressed objects in the catalog without creating separate WAL objects.
type objectSink struct {
	s       *store
	written int64
	prefix  []byte
	writer  *byteWriter
}

func (w *objectSink) Write(p []byte) (int, error) {
	w.written += int64(len(p))
	if w.writer == nil {
		if len(p) <= inlineObjectLimit-len(w.prefix) {
			w.prefix = append(w.prefix, p...)
			return len(p), nil
		}
		if err := w.spill(); err != nil {
			return 0, err
		}
	}
	return w.writer.Write(p)
}

func (w *objectSink) spill() error {
	var err error
	w.writer, err = w.s.newByteWriter()
	if err == nil {
		_, err = w.writer.Write(w.prefix)
	}
	w.prefix = nil
	return err
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
func (o *storedObject) Reader() (reader io.ReadCloser, err error) {
	defer func() {
		observeRead(&o.s.failure, err)
		if err == nil {
			reader = watchedReader(reader, &o.s.failure)
		}
	}()
	if err := o.s.limits.checkObject(o.item.Kind, o.item.Size); err != nil {
		return nil, err
	}

	if err := o.s.ctx.Err(); err != nil {
		return nil, err
	}
	if len(o.item.Inline) > 0 {
		return o.item.readInline(o.s.meta.Format)
	}
	source, err := o.s.openBytes(o.item.root)
	if err != nil {
		return nil, err
	}
	size := o.item.StoredSize
	if source.size != size {
		_ = source.Close()
		return nil, fmt.Errorf("object size differs from index")
	}
	return readLoose(source, o.s.meta.Format, o.item.Kind, o.item.Size, o.Hash())
}

func (s *store) openDelta(item indexed) (io.ReadCloser, error) {
	source, err := s.openBytes(item.deltaRoot)
	if err != nil {
		return nil, err
	}
	if source.size != item.Delta.StoredSize {
		_ = source.Close()
		return nil, fmt.Errorf("delta size differs from index")
	}
	return source, nil
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
			node, e = s.loadBucket(key, root)
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
	result, e := s.publishRoot(root)
	if e != nil {
		return e
	}
	switch result.Tag() {
	case wal.OutcomeCommitted:
		return nil
	case wal.OutcomePending:
		return &pendingError{}
	default:
		return errPublicationConflict
	}
}

func (s *store) publishRoot(root *wal.Object) (wal.Outcome, error) {
	if err := s.ctx.Err(); err != nil {
		return wal.Outcome{}, err
	}
	transactionID := make([]byte, 16)
	if _, e := rand.Read(transactionID); e != nil {
		return wal.Outcome{}, e
	}
	candidate, e := storeCall(s, func() wt.Result[*wal.Candidate, wal.Failure] {
		return s.recovery.Prepare(transactionID, nil, nil, []*wal.Object{root})
	})
	if e != nil {
		return wal.Outcome{}, e
	}
	defer candidate.Drop()
	return storeCall(s, candidate.Publish)
}

func (s *store) acceptRecovery(recovery *wal.Recovery) (*wal.Object, uint64, error) {
	current, err := storeCall(s, recovery.Latest)
	if err != nil {
		return nil, 0, err
	}
	if current.Item.IsNone() {
		return nil, current.TailEntries, nil
	}
	var latest []*wal.Object
	switch item := current.Item.Some(); item.Tag() {
	case wal.HistoryItemCheckpoint:
		latest = item.Checkpoint().Objects
	case wal.HistoryItemCommit:
		latest = item.Commit().Objects
	default:
		return nil, 0, fmt.Errorf("unknown history item %d", item.Tag())
	}
	if len(latest) != 1 {
		dropObjects(latest)
		return nil, 0, fmt.Errorf("invalid root record")
	}
	s.owned = append(s.owned, latest[0])
	return latest[0], current.TailEntries, nil
}

func dropObjects(objects []*wal.Object) {
	for _, object := range objects {
		object.Drop()
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
	for _, w := range s.writers {
		w.close()
	}
	s.writers = nil
	for _, o := range s.owned {
		o.Drop()
	}
	s.owned = nil
	if s.recovery != nil {
		s.recovery.Drop()
		s.recovery = nil
	}
}
func (s *store) readNode(root *wal.Object) (wal.Entry, error) {
	entry, e := storeCall(s, func() wt.Result[wal.Entry, wal.Failure] { return s.recovery.ReadNode(root) })
	if e == nil {
		s.owned = append(s.owned, entry.Objects...)
	}
	return entry, e
}
func (s *store) putNode(b []byte, children []*wal.Object) (*wal.Object, error) {
	o, e := storeCall(s, func() wt.Result[*wal.Object, wal.Failure] { return s.recovery.PutNode(b, children) })
	if e == nil {
		s.owned = append(s.owned, o)
	}
	return o, e
}

type pendingError struct{}

func (*pendingError) Error() string { return "publication pending" }

func (s *store) LowMemoryMode() bool { return true }

// Every component operation checks the request deadline and records errors
// before go-git has an opportunity to discard them.
func storeCall[T any](s *store, call func() wt.Result[T, wal.Failure]) (value T, err error) {
	if err = s.ctx.Err(); err == nil {
		value, err = unwrap(call)
	}
	observeRead(&s.failure, err)
	return value, err
}
