package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReadFailureStopsOutput(t *testing.T) {
	for _, test := range []struct {
		name    string
		started bool
	}{
		{name: "before output"},
		{name: "after output", started: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			output := httptest.NewRecorder()
			var failure error
			response := &readResponse{ResponseWriter: output, failure: &failure}
			if test.started {
				if _, err := response.Write([]byte("prefix")); err != nil {
					t.Fatal(err)
				}
			}
			failure = errors.New("configured catalog limit exceeded")
			// The backend may ignore a traversal error and still attempt to write a pack.
			if n, err := response.Write([]byte("incomplete pack")); n != 0 || err != failure || response.sent != test.started {
				t.Fatalf("write after failure: n=%d err=%v", n, err)
			}
			want := ""
			if test.started {
				want = "prefix"
			}
			if output.Body.String() != want {
				t.Fatalf("started=%v body=%q", test.started, output.Body.String())
			}
		})
	}
}

func TestReadFailurePreservesHTTPErrorStatus(t *testing.T) {
	output := httptest.NewRecorder()
	response := &readResponse{ResponseWriter: output}
	var failure error = &http.MaxBytesError{Limit: 16}
	stream := &readResponse{ResponseWriter: response, failure: &failure}
	// A swallowed storage failure makes go-git's HTTP adapter attempt its own 500.
	http.Error(stream, "backend error", http.StatusInternalServerError)
	if response.sent || output.Body.Len() != 0 {
		t.Fatal("backend error committed the failed read response")
	}
	http.Error(response, "Git read failed", http.StatusRequestEntityTooLarge)
	response.commit()
	if output.Code != http.StatusRequestEntityTooLarge || output.Body.String() != "Git read failed\n" {
		t.Fatalf("status=%d body=%q", output.Code, output.Body.String())
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
		response := &readResponse{ResponseWriter: output, failure: &failure}
		if n, err := response.Write([]byte("incomplete pack")); n != 0 || err != cause || response.sent || output.Body.Len() != 0 {
			t.Fatalf("cause=%v n=%d err=%v sent=%v", cause, n, err, response.sent)
		}
	}
}

func TestResponseHeadersAndTrailersSurviveReadResponse(t *testing.T) {
	output := httptest.NewRecorder()
	response := &readResponse{ResponseWriter: output}
	response.Header().Set("Trailer", "X-Wal-Calls")
	attempt := &readResponse{ResponseWriter: response}
	attempt.Header().Set("Connection", "close")
	attempt.Header().Set("Transfer-Encoding", "chunked")
	attempt.WriteHeader(http.StatusAccepted)
	if _, err := attempt.Write([]byte("pack")); err != nil {
		t.Fatal(err)
	}
	response.Header().Set("X-Wal-Calls", "7")
	response.commit()
	result := output.Result()
	defer result.Body.Close()
	if result.StatusCode != http.StatusAccepted || output.Body.String() != "pack" || result.Trailer.Get("X-Wal-Calls") != "7" {
		t.Fatalf("status=%d body=%q trailers=%v", result.StatusCode, output.Body.String(), result.Trailer)
	}
	if result.Header.Get("Connection") != "" || result.Header.Get("Transfer-Encoding") != "" {
		t.Fatalf("hop-by-hop headers leaked: %v", result.Header)
	}
}
