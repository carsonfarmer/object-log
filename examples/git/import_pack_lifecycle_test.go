package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"

	"github.com/go-git/go-git/v6/plumbing"
	format "github.com/go-git/go-git/v6/plumbing/format/config"
)

type lifecycleStorage struct {
	base      plumbing.EncodedObject
	sink      io.WriteCloser
	openError error
}

func (s lifecycleStorage) EncodedObject(plumbing.ObjectType, plumbing.Hash) (plumbing.EncodedObject, error) {
	return s.base, nil
}
func (s lifecycleStorage) RawObjectWriter(plumbing.ObjectType, int64) (io.WriteCloser, error) {
	return s.sink, s.openError
}

type lifecycleObject struct {
	plumbing.EncodedObject
	reader io.ReadCloser
}

func (o lifecycleObject) Reader() (io.ReadCloser, error) { return o.reader, nil }

type blockedBaseClose struct {
	io.Reader
	entered, release chan struct{}
}

func (r blockedBaseClose) Close() error { close(r.entered); <-r.release; return nil }

type lifecycleSink struct {
	closed             chan struct{}
	writeErr, closeErr error
	cancel             context.CancelFunc
}

func (w lifecycleSink) Write(p []byte) (int, error) {
	if w.cancel != nil {
		w.cancel()
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return len(p), nil
}
func (w lifecycleSink) Close() error { close(w.closed); return w.closeErr }
func TestImportPackWaitsForBaseClose(t *testing.T) {
	for _, mode := range []string{"success", "write-canceled", "open-error", "close-error"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := format.SHA1
				o := plumbing.NewMemoryObject(plumbing.FromObjectFormat(f))
				o.SetType(plumbing.BlobObject)
				w, _ := o.Writer()
				_, _ = w.Write([]byte("abc"))
				_ = w.Close()
				entered, release := make(chan struct{}), make(chan struct{})
				released := false
				defer func() {
					if !released {
						close(release)
					}
				}()
				closed := make(chan struct{})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				sink := lifecycleSink{closed: closed}
				injected := errors.New("injected writer failure")
				var expected error
				s := lifecycleStorage{base: lifecycleObject{EncodedObject: o, reader: blockedBaseClose{Reader: bytes.NewReader([]byte("abc")), entered: entered, release: release}}}
				switch mode {
				case "write-canceled":
					expected = context.Canceled
					sink.writeErr = expected
					sink.cancel = cancel
				case "open-error":
					expected = injected
					s.openError = injected
				case "close-error":
					expected = injected
					sink.closeErr = injected
				}
				s.sink = sink
				packed := fixturePack(t, f, []packFixtureEntry{{kind: plumbing.REFDeltaObject, data: []byte{3, 1, 0x90, 1}, ref: o.Hash()}})
				done := make(chan error, 1)
				go func() { done <- importPack(ctx, bytes.NewReader(packed), s, f, 1024) }()
				synctest.Wait()
				select {
				case <-entered:
				default:
					t.Fatal("base Close not reached")
				}
				select {
				case err := <-done:
					t.Fatalf("import returned before base Close: %v", err)
				default:
				}
				select {
				case <-closed:
					t.Fatal("destination closed before base Close completed")
				default:
				}
				close(release)
				released = true
				synctest.Wait()
				if err := <-done; !errors.Is(err, expected) {
					t.Fatalf("got %v, want %v", err, expected)
				}
				if mode != "open-error" {
					select {
					case <-closed:
					default:
						t.Fatal("destination not closed")
					}
				}
			})
		})
	}
}

func TestImportPackDuplicateIDsWithREFAndOFSChildren(t *testing.T) {
	for _, f := range []format.ObjectFormat{format.SHA1, format.SHA256} {
		t.Run(string(f), func(t *testing.T) {
			base := []byte("abc")
			a := blobID(f, []byte("a"))
			entries := []packFixtureEntry{
				{kind: plumbing.BlobObject, data: base},
				{kind: plumbing.REFDeltaObject, data: []byte{3, 1, 0x90, 1}, ref: blobID(f, base)},
				{kind: plumbing.OFSDeltaObject, data: []byte{1, 1, 0x90, 1}, ofs: 1},
				{kind: plumbing.REFDeltaObject, data: []byte{1, 1, 1, 'b'}, ref: a},
				{kind: plumbing.OFSDeltaObject, data: []byte{1, 1, 1, 'c'}, ofs: 2},
			}
			s := newImportStorage(f)
			if err := importPack(context.Background(), bytes.NewReader(fixturePack(t, f, entries)), s, f, 1024); err != nil {
				t.Fatal(err)
			}
			for _, value := range []string{"abc", "a", "b", "c"} {
				if _, err := s.EncodedObject(plumbing.BlobObject, blobID(f, []byte(value))); err != nil {
					t.Fatalf("missing %q: %v", value, err)
				}
			}
		})
	}
}

type failedReopenObject struct {
	plumbing.EncodedObject
	calls int
}

func (o *failedReopenObject) Reader() (io.ReadCloser, error) {
	o.calls++
	if o.calls > 1 {
		return nil, io.ErrClosedPipe
	}
	return io.NopCloser(bytes.NewReader([]byte("abc"))), nil
}
func TestImportPackFailedBackwardReopen(t *testing.T) {
	f := format.SHA1
	o := plumbing.NewMemoryObject(plumbing.FromObjectFormat(f))
	o.SetType(plumbing.BlobObject)
	w, _ := o.Writer()
	_, _ = w.Write([]byte("abc"))
	_ = w.Close()
	s := lifecycleStorage{base: &failedReopenObject{EncodedObject: o}, sink: streamSink{&streamingStorage{}}}
	p := fixturePack(t, f, []packFixtureEntry{{kind: plumbing.REFDeltaObject, data: []byte{3, 2, 0x91, 2, 1, 0x90, 1}, ref: o.Hash()}})
	if err := importPack(context.Background(), bytes.NewReader(p), s, f, 1024); err == nil {
		t.Fatal("accepted failed reopen")
	}
}
