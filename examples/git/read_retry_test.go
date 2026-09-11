package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadRetry(t *testing.T) {
	for _, test := range []struct {
		name                 string
		size                 int
		late, corrupt, twice bool
		attempts             int
	}{
		{name: "expired", size: 20, attempts: 2},
		{name: "large request", size: 1024*1024 + 1, attempts: 1},
		{name: "output started", size: 20, late: true, attempts: 1},
		{name: "corrupt", size: 20, corrupt: true, attempts: 1},
		{name: "expires again", size: 20, twice: true, attempts: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := strings.Repeat("x", test.size)
			request := httptest.NewRequest(http.MethodPost, "/git-upload-pack", strings.NewReader(payload))
			output := httptest.NewRecorder()
			attempts, refreshes, calls := 0, 0, 0
			err := retryRead(output, request, func() error { refreshes++; calls++; return nil }, func(w *readResponse, r *http.Request) error {
				attempts++
				calls++
				got, err := io.ReadAll(r.Body)
				if err != nil || !bytes.Equal(got, []byte(payload)) {
					t.Fatal("request was not replayed")
				}
				if attempts == 1 || test.twice {
					failure := errExpired
					if test.corrupt {
						failure = errors.New("corruption")
					}
					if test.late {
						_, _ = w.Write([]byte("partial pack"))
					}
					w.failure = &failure
					w.Header().Set("X-Failed-Attempt", "yes")
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte("backend error"))
					return failure
				}
				_, err = w.Write([]byte("pack"))
				return err
			})
			if attempts != test.attempts || refreshes != attempts-1 || calls != attempts+refreshes {
				t.Fatalf("attempts/refresh/calls %d/%d/%d", attempts, refreshes, calls)
			}
			if test.name == "expired" {
				if err != nil || output.Body.String() != "pack" || output.Header().Get("X-Failed-Attempt") != "" || output.Code != 200 {
					t.Fatalf("failed retry: %v %v", err, output)
				}
			} else if err == nil {
				t.Fatal("failure hidden")
			}
			if test.late && output.Body.String() != "partial pack" {
				t.Fatal("appended after failed stream")
			}
		})
	}
}

func TestReadRetryAfterPartialRequest(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("request"))
	tries := 0
	err := retryRead(httptest.NewRecorder(), req, func() error { return nil }, func(w *readResponse, r *http.Request) error {
		tries++
		if tries == 1 {
			b := make([]byte, 3)
			_, _ = io.ReadFull(r.Body, b)
			return errExpired
		}
		b, _ := io.ReadAll(r.Body)
		if string(b) != "request" {
			t.Fatalf("replayed %q", b)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadFailureStopsOutputWithoutRetry(t *testing.T) {
	for _, started := range []bool{false, true} {
		request := httptest.NewRequest(http.MethodPost, "/git-upload-pack", strings.NewReader("request"))
		output := httptest.NewRecorder()
		failure := errors.New("configured catalog limit exceeded")
		refreshes := 0
		err := retryRead(output, request, func() error { refreshes++; return nil }, func(w *readResponse, _ *http.Request) error {
			if started {
				if _, err := w.Write([]byte("prefix")); err != nil {
					t.Fatal(err)
				}
			}
			w.failure = &failure
			// The backend may ignore a traversal error and still attempt to write a pack.
			if n, err := w.Write([]byte("incomplete pack")); n != 0 || err != failure || w.sent != started {
				t.Fatalf("write after failure: n=%d err=%v", n, err)
			}
			return failure
		})
		want := ""
		if started {
			want = "prefix"
		}
		if err != failure || refreshes != 0 || output.Body.String() != want {
			t.Fatalf("started=%v err=%v refreshes=%d body=%q", started, err, refreshes, output.Body.String())
		}
	}
}

func TestObservedStorageFailureStopsOutput(t *testing.T) {
	for _, cause := range []error{errors.New("storage quota exceeded"), io.ErrUnexpectedEOF, io.EOF, context.Canceled} {
		var failure error
		observeRead(&failure, nil)
		if failure != nil {
			t.Fatal("successful read recorded a failure")
		}
		observeRead(&failure, cause)
		observeRead(&failure, nil)
		observeRead(&failure, errExpired)
		output := httptest.NewRecorder()
		response := &readResponse{ResponseWriter: output, header: make(http.Header), failure: &failure}
		if n, err := response.Write([]byte("incomplete pack")); n != 0 || err != cause || response.sent || output.Body.Len() != 0 {
			t.Fatalf("cause=%v n=%d err=%v sent=%v", cause, n, err, response.sent)
		}
	}
}
