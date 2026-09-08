package main

import (
	"bytes"
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
