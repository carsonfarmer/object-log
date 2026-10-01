package main

import (
	"context"
	"io"
	"sync"

	gitio "github.com/go-git/go-git/v6/utils/ioutil"
)

// go-git's pack parser can discard Close errors. Record them before returning,
// and never retry a failed finalizer or continue writing through a failed codec.
func closingWriter(ctx context.Context, writer io.Writer, failure *error, close func() error) io.WriteCloser {
	finalize := sync.OnceValue(func() error {
		err := close()
		observeRead(failure, err)
		return err
	})
	return gitio.NewWriteCloser(&failedWriter{gitio.NewContextWriter(ctx, writer), failure}, gitio.CloserFunc(func() error {
		if err := finalize(); err != nil {
			return err
		}
		return *failure
	}))
}

type failedWriter struct {
	io.Writer
	failure *error
}

func (w *failedWriter) Write(p []byte) (int, error) {
	if *w.failure != nil {
		return 0, *w.failure
	}
	n, err := w.Writer.Write(p)
	observeRead(w.failure, err)
	return n, err
}

// The stock error observer covers Read; the finalizer also records Close.
func watchedReader(reader io.ReadCloser, failure *error) io.ReadCloser {
	if failure == nil {
		return reader
	}
	return gitio.NewReadCloser(gitio.NewReaderOnError(reader, func(err error) { observeRead(failure, err) }), gitio.CloserFunc(func() error {
		err := reader.Close()
		observeRead(failure, err)
		return err
	}))
}

type objectWriter struct {
	io.Writer
	remaining int64
}

func (w *objectWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.remaining -= int64(n)
	return n, err
}
