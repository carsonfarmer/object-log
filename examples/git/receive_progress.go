package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp/sideband"
)

// ReceivePack creates its hook progress writer after pack import. Use the same
// upstream mux for import, then its hook writer for validation and publication.
type receiveProgress struct {
	writer      io.Writer
	cancel      context.CancelFunc
	total, done uint32
	last        time.Time
	err         error
}

func newReceiveProgress(w io.Writer, caps *capability.List, cancel context.CancelFunc) *receiveProgress {
	if caps == nil || !caps.Supports(capability.Sideband64k) {
		return nil
	}
	if caps.Supports(capability.Quiet) || caps.Supports(capability.NoProgress) {
		return nil
	}
	return &receiveProgress{
		writer: progressChannel{mux: sideband.NewMuxer(sideband.Sideband64k, w)},
		cancel: cancel,
	}
}

type progressChannel struct{ mux *sideband.Muxer }

func (w progressChannel) Write(data []byte) (int, error) {
	return w.mux.WriteChannel(sideband.ProgressMessage, data)
}

func (p *receiveProgress) message(text string) error {
	if p == nil {
		return nil
	}
	if p.err == nil {
		_, p.err = io.WriteString(p.writer, text)
		if p.err != nil && p.cancel != nil {
			p.cancel()
		}
		p.last = time.Now()
	}
	return p.err
}

func (p *receiveProgress) OnHeader(total uint32) error {
	if p == nil {
		return nil
	}
	p.total = total
	return p.message(fmt.Sprintf("Importing objects: 0/%d\n", total))
}

func (p *receiveProgress) OnInflatedObjectHeader(plumbing.ObjectType, int64, int64) error {
	if p == nil {
		return nil
	}
	return p.err
}

func (p *receiveProgress) OnInflatedObjectContent(plumbing.Hash, int64, uint32, []byte) error {
	if p == nil {
		return nil
	}
	p.done++
	if time.Since(p.last) < time.Second {
		return p.err
	}
	return p.message(fmt.Sprintf("Importing objects: %d/%d\r", p.done, p.total))
}

func (p *receiveProgress) OnFooter(plumbing.Hash) error {
	if p == nil {
		return nil
	}
	return p.message(fmt.Sprintf("Importing objects: %d/%d\n", p.done, p.total))
}
