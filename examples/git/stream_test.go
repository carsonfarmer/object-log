package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/objfile"
)

type faultWriter struct {
	bytes.Buffer
	err error
}

func (w *faultWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	return w.Buffer.Write(p)
}

func TestClosingWriterFinalizesOnceAndRecordsFailure(t *testing.T) {
	for _, failureAt := range []string{"write", "close", "cancel", "none"} {
		t.Run(failureAt, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := errors.New("failed storage write")
			var failure error
			sink := &faultWriter{}
			calls := 0
			writer := closingWriter(ctx, sink, &failure, func() error {
				calls++
				if failure != nil {
					return failure
				}
				if failureAt == "close" {
					return want
				}
				return nil
			})
			if failureAt == "write" {
				sink.err = want
			}
			if failureAt == "cancel" {
				cancel()
				want = context.Canceled
			}
			_, writeErr := writer.Write([]byte("object"))
			if failureAt == "write" || failureAt == "cancel" {
				if !errors.Is(writeErr, want) || !errors.Is(failure, want) {
					t.Fatalf("write error was lost: %v %v", writeErr, failure)
				}
			}
			closeErr := writer.Close()
			if failureAt != "none" && (!errors.Is(closeErr, want) || !errors.Is(failure, want)) {
				t.Fatalf("close error was lost: %v %v", closeErr, failure)
			}
			if again := writer.Close(); again != closeErr || calls != 1 {
				t.Fatalf("finalizer retried: calls=%d first=%v second=%v", calls, closeErr, again)
			}
			if failureAt != "none" {
				sink.err = nil
				before := sink.Len()
				if n, err := writer.Write([]byte("after failure")); n != 0 || !errors.Is(err, want) || sink.Len() != before {
					t.Fatalf("write continued after failure: n=%d err=%v", n, err)
				}
			}
		})
	}
}

func TestWatchedReaderRecordsReadAndCloseFailures(t *testing.T) {
	for _, mode := range []string{"read", "close", "eof"} {
		t.Run(mode, func(t *testing.T) {
			want := errors.New("failed storage read")
			var failure error
			source := &closeProbe{Reader: bytes.NewReader(nil)}

			if mode == "close" {
				source.closeErr = want
			}
			var body io.ReadCloser = source
			if mode == "read" {
				body = &failingReadCloser{errorReader{want}, source}
			}
			reader := watchedReader(body, &failure)
			_, readErr := reader.Read(make([]byte, 1))
			closeErr := reader.Close()
			if mode == "eof" {
				if readErr != io.EOF || failure != nil || closeErr != nil {
					t.Fatalf("normal EOF became a failure: %v %v %v", readErr, closeErr, failure)
				}
			} else if !errors.Is(failure, want) {
				t.Fatalf("%s error was lost: %v", mode, failure)
			}
			if !source.closed {
				t.Fatal("underlying source leaked")
			}
		})
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

type failingReadCloser struct {
	io.Reader
	io.Closer
}

func TestClosingWriterDoesNotLoseWritesAfterClose(t *testing.T) {
	var failure error
	sink := &faultWriter{}
	calls := 0
	writer := closingWriter(t.Context(), sink, &failure, func() error {
		calls++
		sink.err = io.ErrClosedPipe
		return nil
	})
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("after close")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("closed writer accepted content: %v", err)
	}
	if err := writer.Close(); !errors.Is(err, io.ErrClosedPipe) || calls != 1 {
		t.Fatalf("lost error or repeated publication: %v calls=%d", err, calls)
	}
}

func TestNativeObjectOverflowCannotPublish(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		t.Run(format.String(), func(t *testing.T) {
			var encoded bytes.Buffer
			codec := objfile.NewWriter(&encoded, format)
			if err := codec.WriteHeader(plumbing.BlobObject, 3); err != nil {
				t.Fatal(err)
			}
			counted := &objectWriter{Writer: codec, remaining: 3}
			var failure error
			published := false
			calls := 0
			writer := closingWriter(t.Context(), counted, &failure, func() error {
				calls++
				if err := errors.Join(failure, codec.Close()); err != nil {
					return err
				}
				published = true
				return nil
			})
			if n, err := writer.Write([]byte("four")); n != 3 || !errors.Is(err, objfile.ErrOverflow) {
				t.Fatalf("native overflow was not recorded: n=%d err=%v", n, err)
			}
			for range 2 {
				if err := writer.Close(); !errors.Is(err, objfile.ErrOverflow) {
					t.Fatalf("Close lost native Write failure: %v", err)
				}
			}
			if published || calls != 1 || counted.remaining != 0 || !errors.Is(failure, objfile.ErrOverflow) {
				t.Fatalf("overflow published or finalizer retried: published=%v calls=%d remaining=%d failure=%v", published, calls, counted.remaining, failure)
			}
		})
	}
}

func TestClosingIncomingAndLooseWritersAfterClose(t *testing.T) {
	for _, kind := range []string{"incoming", "loose"} {
		t.Run(kind, func(t *testing.T) {
			var underlying io.WriteCloser
			want := io.ErrClosedPipe
			if kind == "incoming" {
				// The incoming WAL adapter likewise rejects writes after dropping its
				// resource. Use a standard stream to exercise the shared finalizer.
				reader, writer := io.Pipe()
				defer reader.Close()
				underlying = writer
			} else {
				codec := objfile.NewWriter(io.Discard, config.SHA1)
				if err := codec.WriteHeader(plumbing.BlobObject, 0); err != nil {
					t.Fatal(err)
				}
				underlying = codec
				want = objfile.ErrClosed
			}
			var failure error
			calls := 0
			writer := closingWriter(t.Context(), underlying, &failure, func() error {
				calls++
				return underlying.Close()
			})
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Write([]byte("after Close")); !errors.Is(err, want) {
				t.Fatalf("closed writer accepted data: %v", err)
			}
			if err := writer.Close(); !errors.Is(err, want) || calls != 1 {
				t.Fatalf("lost sticky error or repeated finalizer: err=%v calls=%d", err, calls)
			}
		})
	}
}
