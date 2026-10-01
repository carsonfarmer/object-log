package main

import "net/http"

type readResponse struct {
	http.ResponseWriter
	sent bool
}

func (w *readResponse) WriteHeader(status int) {
	if !w.sent {
		w.sent = true
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *readResponse) Write(p []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return w.ResponseWriter.Write(p)
}

// Record physical storage failures before library traversal can discard them.
// Normal logical absence and end-of-stream are handled outside this boundary.
func observeRead(failure *error, err error) {
	if *failure == nil && err != nil {
		*failure = err
	}
}
