package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

var errObjectLimit = errors.New("object exceeds configured size limit")

type requestLimits struct {
	pushBytes, negotiationBytes, objectBytes int64
	timeout                                  time.Duration
}

func loadLimits(getenv func(string) string) (requestLimits, error) {
	limits := requestLimits{pushBytes: 2 << 30, negotiationBytes: 8 << 20, objectBytes: 1 << 30, timeout: 5 * time.Minute}
	for _, setting := range []struct {
		name  string
		value *int64
	}{
		{"GIT_MAX_PUSH_BYTES", &limits.pushBytes},
		{"GIT_MAX_NEGOTIATION_BYTES", &limits.negotiationBytes},
		{"GIT_MAX_OBJECT_BYTES", &limits.objectBytes},
	} {
		if text := getenv(setting.name); text != "" {
			value, err := strconv.ParseInt(text, 10, 64)
			if err != nil || value <= 0 {
				return limits, fmt.Errorf("%s must be a positive byte count", setting.name)
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
