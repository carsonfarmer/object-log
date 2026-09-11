package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
)

var errObjectLimit = errors.New("configured Git resource limit exceeded")

type requestLimits struct {
	pushBytes, negotiationBytes, objectBytes int64
	packObjects, metadataBytes, catalogBytes int64
	timeout                                  time.Duration
	readOnly                                 bool
	catalogRead                              *int64
}

func loadLimits(getenv func(string) string) (requestLimits, error) {
	limits := requestLimits{pushBytes: 2 << 30, negotiationBytes: 8 << 20, objectBytes: 1 << 30, packObjects: 1_000_000, metadataBytes: 16 << 20, catalogBytes: 64 << 20, catalogRead: new(int64), timeout: 5 * time.Minute}
	for _, setting := range []struct {
		name  string
		value *int64
	}{
		{"GIT_MAX_PUSH_BYTES", &limits.pushBytes},
		{"GIT_MAX_NEGOTIATION_BYTES", &limits.negotiationBytes},
		{"GIT_MAX_OBJECT_BYTES", &limits.objectBytes},
		{"GIT_MAX_PACK_OBJECTS", &limits.packObjects},
		{"GIT_MAX_METADATA_BYTES", &limits.metadataBytes},
		{"GIT_MAX_CATALOG_BYTES", &limits.catalogBytes},
	} {
		if text := getenv(setting.name); text != "" {
			value, err := strconv.ParseInt(text, 10, 64)
			if err != nil || value <= 0 {
				return limits, fmt.Errorf("%s must be a positive count", setting.name)
			}
			*setting.value = value
		}
	}
	if text := getenv("GIT_REQUEST_TIMEOUT"); text != "" {
		value, err := time.ParseDuration(text)
		if err != nil || value <= 0 {
			return limits, fmt.Errorf("GIT_REQUEST_TIMEOUT must be a positive duration")
		}
		limits.timeout = value
	}
	if value := getenv("GIT_READ_ONLY"); value != "" {
		var err error
		limits.readOnly, err = strconv.ParseBool(value)
		if err != nil {
			return limits, fmt.Errorf("GIT_READ_ONLY must be a boolean")
		}
	}
	return limits, nil
}

// Cancellation is cooperative: a synchronous component import already in progress
// must finish before the next check. Publication results are always inspected.
type requestBody struct {
	io.ReadCloser
	ctx context.Context
}

func (r *requestBody) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.ReadCloser.Read(p)
	if canceled := r.ctx.Err(); canceled != nil {
		return n, canceled
	}
	return n, err
}

func limitedRequest(w http.ResponseWriter, r *http.Request, limits requestLimits, push bool) (*http.Request, context.CancelFunc, error) {
	size := limits.negotiationBytes
	if push {
		size = limits.pushBytes
	}
	if r.ContentLength > size {
		return nil, nil, &http.MaxBytesError{Limit: size}
	}
	ctx, cancel := context.WithTimeout(r.Context(), limits.timeout)
	request := r.Clone(ctx)
	request.Body = &requestBody{ReadCloser: http.MaxBytesReader(w, r.Body, size), ctx: ctx}
	return request, cancel, nil
}

func operationStatus(err error) int {
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge), errors.Is(err, errObjectLimit):
		return http.StatusRequestEntityTooLarge
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return http.StatusRequestTimeout
	default:
		return http.StatusInternalServerError
	}
}

// Structured objects are decoded into Go fields; blobs remain streamed.
func (l requestLimits) checkObject(kind plumbing.ObjectType, size int64) error {
	if size < 0 {
		return fmt.Errorf("invalid object size")
	}
	if size > l.objectBytes {
		return fmt.Errorf("%w: GIT_MAX_OBJECT_BYTES", errObjectLimit)
	}
	if kind != plumbing.BlobObject && size > l.metadataBytes {
		return fmt.Errorf("%w: GIT_MAX_METADATA_BYTES", errObjectLimit)
	}
	return nil
}

// Store copies share this request counter, including after an expired-view retry.
// Charge cache misses before decoding; a rejected page leaves the total intact.
func (l requestLimits) chargeCatalog(size int) error {
	if int64(size) > l.catalogBytes-*l.catalogRead {
		return fmt.Errorf("%w: GIT_MAX_CATALOG_BYTES", errObjectLimit)
	}
	*l.catalogRead += int64(size)
	return nil
}
