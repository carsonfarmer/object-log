package main

import (
	"bytes"
	"errors"
	"io"
	"net/http"
)

var errExpired = errors.New("expired view")

// Retain only a small request prefix. Larger negotiations still work, but cannot
// be retried here. Response bodies are never buffered or restarted after output.
func retryRead(out http.ResponseWriter, request *http.Request, refresh func() error, run func(*readResponse, *http.Request) error) error {
	defer request.Body.Close()
	replay := &replayReader{Reader: request.Body}
	body := io.Reader(replay)
	for attempt := 0; ; attempt++ {
		r := request.Clone(request.Context())
		r.Body = io.NopCloser(body)
		w := &readResponse{ResponseWriter: out, header: out.Header().Clone()}
		err := run(w, r)
		if !errors.Is(err, errExpired) || w.sent || attempt != 0 || replay.overflow {
			if err == nil {
				w.commit()
			}
			return err
		}
		if err = refresh(); err != nil {
			return err
		}
		body = io.MultiReader(bytes.NewReader(replay.saved.Bytes()), request.Body)
	}
}

type replayReader struct {
	io.Reader
	saved    bytes.Buffer
	overflow bool
}

func (r *replayReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if !r.overflow {
		if r.saved.Len()+n > 1024*1024 {
			r.overflow = true
			r.saved.Reset()
		} else {
			r.saved.Write(p[:n])
		}
	}
	return n, err
}

type readResponse struct {
	http.ResponseWriter
	header  http.Header
	status  int
	sent    bool
	failure *error
}

func (w *readResponse) Header() http.Header { return w.header }
func (w *readResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *readResponse) commit() {
	if w.sent {
		return
	}
	w.sent = true
	for key := range w.ResponseWriter.Header() {
		w.ResponseWriter.Header().Del(key)
	}
	for key, values := range w.header {
		w.ResponseWriter.Header()[key] = values
	}
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.ResponseWriter.WriteHeader(w.status)
}
func (w *readResponse) Write(p []byte) (int, error) {
	if w.failure != nil && *w.failure != nil {
		return 0, *w.failure
	}
	w.commit()
	return w.ResponseWriter.Write(p)
}

// Record physical storage failures before library traversal can discard them.
// Normal logical absence and end-of-stream are handled outside this boundary.
func observeRead(failure *error, err error) {
	if *failure == nil && err != nil {
		*failure = err
	}
}
