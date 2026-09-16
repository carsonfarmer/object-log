package main

import (
	"fmt"
	"io"
	"math"

	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage"
)

// ReceivePack opens the pack writer after decoding commands and push options.
// Release their smaller read limit; the HTTP body retains the total push limit.
type receiveStore struct {
	storage.Storer
	commands *commandReader
}

func (s *receiveStore) PackfileWriter() (io.WriteCloser, error) {
	s.commands.N = math.MaxInt64
	return s.Storer.(storer.PackfileWriter).PackfileWriter()
}

type commandReader struct{ io.LimitedReader }

func (r *commandReader) Read(p []byte) (int, error) {
	if r.N == 0 {
		return 0, fmt.Errorf("%w: GIT_MAX_NEGOTIATION_BYTES", errObjectLimit)
	}
	return r.LimitedReader.Read(p)
}
