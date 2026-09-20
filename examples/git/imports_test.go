package main

import (
	"errors"
	wt "go.bytecodealliance.org/pkg/wit/types"
	"io"
	"net/http"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
	"strings"
	"sync"
	"testing"
	"time"
)

type heldBody struct {
	entered chan struct{}
	resume  chan struct{}
	reads   int
	closes  int
}

func (b *heldBody) Read(p []byte) (int, error) {
	b.reads++
	close(b.entered)
	<-b.resume
	p[0] = 'x'
	return 1, nil
}

func (b *heldBody) Close() error { b.closes++; return nil }

func TestComponentBodyDrainsReadBeforeClosing(t *testing.T) {
	raw := &heldBody{entered: make(chan struct{}), resume: make(chan struct{})}
	resume := sync.OnceFunc(func() { close(raw.resume) })
	t.Cleanup(resume)
	body := &componentBody{ReadCloser: raw}
	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		n, err := body.Read(buf)
		if err == nil && (n != 1 || buf[0] != 'x') {
			err = errors.New("read result changed during close")
		}
		readDone <- err
	}()
	<-raw.entered
	closeDone := make(chan error, 1)
	go func() { closeDone <- body.Close() }()
	select {
	case <-closeDone:
		t.Fatal("closed while the underlying read was active")
	case <-time.After(20 * time.Millisecond):
	}
	resume()
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if _, err := body.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("late read: %v", err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if raw.reads != 1 || raw.closes != 1 {
		t.Fatalf("underlying reads=%d closes=%d", raw.reads, raw.closes)
	}
}

func TestWALLimitKeepsClientErrorStatus(t *testing.T) {
	_, err := unwrap(func() wt.Result[wt.Unit, wal.Failure] {
		return wt.Err[wt.Unit](wal.MakeFailureLimit("publication objects"))
	})
	if !errors.Is(err, errObjectLimit) || operationStatus(err) != http.StatusRequestEntityTooLarge || !strings.Contains(err.Error(), "publication objects") {
		t.Fatalf("WAL capacity rejection lost its client error: %v", err)
	}
}
