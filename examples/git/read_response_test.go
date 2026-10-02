package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	gitconfig "github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"
	gitio "github.com/go-git/go-git/v6/utils/ioutil"
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
			response := &componentResponse{ResponseWriter: output}
			writer := &failedWriter{response, &failure}
			if test.started {
				if _, err := writer.Write([]byte("prefix")); err != nil {
					t.Fatal(err)
				}
			}
			failure = errors.New("configured catalog limit exceeded")
			// The backend may ignore a traversal error and still attempt to write a pack.
			if n, err := writer.Write([]byte("incomplete pack")); n != 0 || err != failure || response.sent != test.started {
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
	response := &componentResponse{ResponseWriter: output}
	var failure error = &http.MaxBytesError{Limit: 16}
	writer := &failedWriter{response, &failure}
	if n, err := writer.Write([]byte("incomplete pack")); n != 0 || err != failure {
		t.Fatalf("failed payload: n=%d err=%v", n, err)
	}
	if response.sent || output.Body.Len() != 0 {
		t.Fatal("failed payload committed the response")
	}
	http.Error(response, "Git read failed", http.StatusRequestEntityTooLarge)
	response.WriteHeader(http.StatusOK)
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
		response := &componentResponse{ResponseWriter: output}
		writer := &failedWriter{response, &failure}
		if n, err := writer.Write([]byte("incomplete pack")); n != 0 || err != cause || response.sent || output.Body.Len() != 0 {
			t.Fatalf("cause=%v n=%d err=%v sent=%v", cause, n, err, response.sent)
		}
	}
}

func TestResponseHeadersAndTrailersSurviveReadResponse(t *testing.T) {
	output := httptest.NewRecorder()
	response := &componentResponse{ResponseWriter: output}
	response.Header().Set("Trailer", "X-Wal-Calls")
	response.WriteHeader(http.StatusAccepted)
	if _, err := response.Write([]byte("pack")); err != nil {
		t.Fatal(err)
	}
	response.Header().Set("X-Wal-Calls", "7")
	response.WriteHeader(http.StatusOK)
	result := output.Result()
	defer result.Body.Close()
	if result.StatusCode != http.StatusAccepted || output.Body.String() != "pack" || result.Trailer.Get("X-Wal-Calls") != "7" {
		t.Fatalf("status=%d body=%q trailers=%v", result.StatusCode, output.Body.String(), result.Trailer)
	}
}

// go-git falls back to SHA-1 if Config fails while advertising v2. The payload
// observer must still reject that success-shaped response before its first byte.
type failedAdvertisement struct {
	storage.Storer
	failure *error
	cause   error
}

func (s failedAdvertisement) Config() (*gitconfig.Config, error) {
	observeRead(s.failure, s.cause)
	return nil, s.cause
}

func TestUploadPackObservedFailureBeforeHTTPOutput(t *testing.T) {
	for _, cause := range []error{&http.MaxBytesError{Limit: 16}, io.ErrUnexpectedEOF, context.Canceled} {
		output := httptest.NewRecorder()
		response := &componentResponse{ResponseWriter: output}
		var failure error
		s := failedAdvertisement{memory.NewStorage(), &failure, cause}
		err := transport.UploadPack(t.Context(), s, nil, gitio.WriteNopCloser(&failedWriter{response, &failure}), &transport.UploadPackRequest{
			GitProtocol: "version=2", AdvertiseRefs: true, StatelessRPC: true,
		})
		if !errors.Is(err, cause) || failure != cause || response.sent || output.Body.Len() != 0 {
			t.Fatalf("failed discovery committed output: err=%v failure=%v sent=%v body=%q", err, failure, response.sent, output.Body.String())
		}
		http.Error(response, "Git read failed", operationStatus(err))
		if output.Code != operationStatus(cause) || output.Body.String() != "Git read failed\n" {
			t.Fatalf("status=%d body=%q", output.Code, output.Body.String())
		}
	}
}

type failedFetch struct {
	storage.Storer
	failure *error
	cause   error
}

func (s failedFetch) EncodedObject(plumbing.ObjectType, plumbing.Hash) (plumbing.EncodedObject, error) {
	observeRead(s.failure, s.cause)
	return nil, s.cause
}

func TestUploadPackFetchFailurePreservesHTTPStatus(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		for _, v2 := range []bool{false, true} {
			s, ids := policyFixture(t, format)
			var input bytes.Buffer
			protocol := ""
			if v2 {
				protocol = "version=2"
				request := packp.CommandRequest{Command: "fetch", Args: &packp.FetchArgs{Wants: []plumbing.Hash{ids["tip"]}, Done: true, NoProgress: true}}
				if err := request.Encode(&input); err != nil {
					t.Fatal(err)
				}
			} else {
				request := packp.UploadRequest{Wants: []plumbing.Hash{ids["tip"]}}
				if err := request.Encode(&input); err != nil {
					t.Fatal(err)
				}
				haves := packp.UploadHaves{Done: true}
				if err := haves.Encode(&input); err != nil {
					t.Fatal(err)
				}
			}
			output := httptest.NewRecorder()
			response := &componentResponse{ResponseWriter: output}
			cause := &http.MaxBytesError{Limit: 16}
			var failure error
			err := transport.UploadPack(t.Context(), failedFetch{s.Storage, &failure, cause}, io.NopCloser(&input), gitio.WriteNopCloser(&failedWriter{response, &failure}), &transport.UploadPackRequest{
				GitProtocol: protocol, StatelessRPC: true,
			})
			if !errors.Is(err, cause) || failure != cause || response.sent || output.Body.Len() != 0 {
				t.Fatalf("format=%s v2=%v err=%v failure=%v sent=%v body=%q", format, v2, err, failure, response.sent, output.Body.String())
			}
			http.Error(response, "Git read failed", operationStatus(failure))
			if output.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status=%d", output.Code)
			}
		}
	}
}
