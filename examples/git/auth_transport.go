package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	clock "go.bytecodealliance.org/pkg/imports/wasi_clocks_0_2_8_monotonic_clock"
	outgoing "go.bytecodealliance.org/pkg/imports/wasi_http_0_2_8_outgoing_handler"
	types "go.bytecodealliance.org/pkg/imports/wasi_http_0_2_8_types"
	poll "go.bytecodealliance.org/pkg/imports/wasi_io_0_2_8_poll"
	streams "go.bytecodealliance.org/pkg/imports/wasi_io_0_2_8_streams"
	witRuntime "go.bytecodealliance.org/pkg/wit/runtime"
	wt "go.bytecodealliance.org/pkg/wit/types"
)

// The SDK's HTTP transport only exposes a connection timeout. This GET-only
// transport races all network waits against one WASI clock deadline, including
// a server that sends headers or body bytes slowly. It never forwards credentials.
type keyTransport struct{}

func (keyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodGet || r.Body != nil || r.URL.Scheme != "https" {
		return nil, errors.New("signing-key request must be an HTTPS GET without a body")
	}
	imports.Lock()
	defer finishImports()
	wait := authFetchTimeout
	if deadline, ok := r.Context().Deadline(); ok {
		wait = min(wait, time.Until(deadline))
	}
	if wait <= 0 || r.Context().Err() != nil {
		return nil, context.DeadlineExceeded
	}
	deadline := clock.SubscribeDuration(uint64(wait))
	defer deadline.Drop()
	ready := func(p *poll.Pollable) error {
		defer p.Drop()
		poll.Poll([]*poll.Pollable{p, deadline})
		if deadline.Ready() {
			return context.DeadlineExceeded
		}
		return r.Context().Err()
	}
	request := types.MakeOutgoingRequest(types.MakeFields())
	if request.SetMethod(types.MakeMethodGet()).IsErr() ||
		request.SetScheme(wt.Some(types.MakeSchemeHttps())).IsErr() ||
		request.SetAuthority(wt.Some(r.URL.Host)).IsErr() ||
		request.SetPathWithQuery(wt.Some(r.URL.RequestURI())).IsErr() {
		request.Drop()
		return nil, errors.New("invalid signing-key URL")
	}
	result := outgoing.Handle(request, wt.None[*types.RequestOptions]())
	if result.IsErr() {
		return nil, errAuthUnavailable
	}
	future := result.Ok()
	defer future.Drop()
	if err := ready(future.Subscribe()); err != nil {
		return nil, err
	}
	response := future.Get()
	if response.IsNone() || response.Some().IsErr() || response.Some().Ok().IsErr() {
		return nil, errAuthUnavailable
	}
	incoming := response.Some().Ok().Ok()
	defer incoming.Drop()
	consumed := incoming.Consume()
	if consumed.IsErr() {
		return nil, errAuthUnavailable
	}
	body := consumed.Ok()
	defer body.Drop()
	opened := body.Stream()
	if opened.IsErr() {
		return nil, errAuthUnavailable
	}
	stream := opened.Ok()
	defer stream.Drop()
	var content bytes.Buffer
	for content.Len() <= authJWKSBytes {
		if err := ready(stream.Subscribe()); err != nil {
			return nil, err
		}
		chunk := stream.Read(uint64(min(8192, authJWKSBytes+1-content.Len())))
		if chunk.IsErr() {
			if chunk.Err().Tag() != streams.StreamErrorClosed {
				chunk.Err().LastOperationFailed().Drop()
				return nil, errAuthUnavailable
			}
			return &http.Response{
				StatusCode: int(incoming.Status()), Request: r, Header: make(http.Header),
				Body: io.NopCloser(bytes.NewReader(content.Bytes())),
			}, nil
		}
		content.Write(chunk.Ok())
		witRuntime.Unpin()
	}
	return nil, errors.New("JWKS exceeds byte limit")
}
