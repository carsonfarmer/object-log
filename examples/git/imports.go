package main

import (
	"fmt"
	wt "go.bytecodealliance.org/pkg/wit/types"
	"io"
	"net/http"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
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

func unwrap[T any](call func() wt.Result[T, wal.Failure]) (T, error) {
	imports.Lock()
	defer finishImports()
	r := call()
	if r.IsOk() {
		return r.Ok(), nil
	}
	var zero T
	switch r.Err().Tag() {
	case wal.FailureExpired:
		return zero, errExpired
	case wal.FailureLimit:
		return zero, fmt.Errorf("%w: %s", errObjectLimit, r.Err().Limit())
	}
	return zero, fmt.Errorf("wal: %s", r.Err().Other())
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
