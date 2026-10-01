package main

import "net/http"

type readResponse struct {
	http.ResponseWriter
	status  int
	sent    bool
	failure *error
}

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
	w.ResponseWriter.Header().Del("Connection")
	w.ResponseWriter.Header().Del("Transfer-Encoding")
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
