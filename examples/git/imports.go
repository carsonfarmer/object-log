package main

import (
	"io"
	"net/http"
	"sync"

	witRuntime "go.bytecodealliance.org/pkg/wit/runtime"
)

// The synchronous component shares one upstream imported-buffer pinner.
// Hold this lock from before an allocating import through lifting its result.
// Once all results are Go values, their ordinary Go references keep them alive.
var imports sync.Mutex

func finishImports() {
	witRuntime.Unpin()
	imports.Unlock()
}

type componentBody struct {
	io.ReadCloser
	closed bool
}

func (r *componentBody) Read(p []byte) (int, error) {
	imports.Lock()
	if r.closed {
		imports.Unlock()
		return 0, io.ErrClosedPipe
	}
	defer finishImports()
	return r.ReadCloser.Read(p)
}

func (r *componentBody) Close() error {
	imports.Lock()
	if r.closed {
		imports.Unlock()
		return nil
	}
	defer finishImports()
	r.closed = true
	return r.ReadCloser.Close()
}

type componentResponse struct{ http.ResponseWriter }

func (w componentResponse) Write(p []byte) (int, error) {
	imports.Lock()
	defer finishImports()
	return w.ResponseWriter.Write(p)
}

func (w componentResponse) WriteHeader(status int) {
	imports.Lock()
	defer finishImports()
	w.ResponseWriter.WriteHeader(status)
}
