package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"os"

	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
)

// The pinned upstream returns before updating refs when report-status is absent.
// Use its codecs to request an internal report, then consume it locally.
func receive(ctx context.Context, s storage.Storer, body io.ReadCloser, out io.WriteCloser, opts *transport.ReceivePackRequest) error {
	defer body.Close()
	reader := bufio.NewReader(body)
	size, _, err := pktline.PeekLine(reader)
	if err != nil {
		return err
	}
	if size == pktline.Flush {
		return nil
	}
	var request packp.UpdateRequests
	limits, err := loadLimits(os.Getenv)
	if err != nil {
		return err
	}
	if err = request.Decode(http.MaxBytesReader(nil, io.NopCloser(reader), limits.negotiationBytes)); err != nil {
		return err
	}
	if len(request.Commands) == 0 {
		return nil
	}
	silent := !request.Capabilities.Supports(capability.ReportStatus) && !request.Capabilities.Supports(capability.ReportStatusV2)
	var report bytes.Buffer
	if silent {
		request.Capabilities.Set(capability.ReportStatus)
		request.Capabilities.Delete(capability.Sideband)
		request.Capabilities.Delete(capability.Sideband64k)
		out = writeCloser{&report}
	}
	var prefix bytes.Buffer
	if err = request.Encode(&prefix); err != nil {
		return err
	}
	if err = transport.ReceivePack(ctx, s, io.NopCloser(io.MultiReader(&prefix, reader)), out, opts); err != nil {
		return err
	}
	if silent {
		var status packp.ReportStatus
		if err = status.Decode(&report); err != nil {
			return err
		}
		return status.Error()
	}
	return nil
}
