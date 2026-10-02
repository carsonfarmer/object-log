package main

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage"
)

// Decode the first command with go-git, then replay its bounded prefix so the
// ordinary receive-pack path still validates every command and the entire pack.
func receiveFormat(body io.Reader, limit int64) (config.ObjectFormat, *capability.List, io.Reader, error) {
	var prefix bytes.Buffer
	scanner := pktline.NewScanner(&commandReader{io.LimitedReader{R: io.TeeReader(body, &prefix), N: limit}})
	for scanner.Scan() {
		if scanner.Len() == pktline.Flush || bytes.IndexByte(scanner.Bytes(), 0) >= 0 {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return "", nil, nil, err
	}
	replay := io.MultiReader(bytes.NewReader(prefix.Bytes()), body)
	if prefix.Len() == 4 && scanner.Len() == pktline.Flush {
		return "", nil, replay, nil
	}
	request := &packp.UpdateRequests{}
	if err := request.Decode(io.MultiReader(bytes.NewReader(prefix.Bytes()), strings.NewReader("0000"))); err != nil {
		return "", nil, nil, err
	}
	if len(request.Commands) == 0 {
		return "", nil, replay, nil
	}
	formats := request.Capabilities.Get(capability.ObjectFormat)
	format := config.SHA1
	if request.Capabilities.Supports(capability.ObjectFormat) && len(formats) != 1 {
		return "", nil, nil, config.ErrInvalidObjectFormat
	}
	if len(formats) == 1 {
		format = config.ObjectFormat(formats[0])
	}
	if format != config.SHA1 && format != config.SHA256 {
		return "", nil, nil, config.ErrInvalidObjectFormat
	}
	command := request.Commands[0]
	if command.Old.HexSize() != format.HexSize() || command.New.HexSize() != format.HexSize() {
		return "", nil, nil, config.ErrInvalidObjectFormat
	}
	return format, &request.Capabilities, replay, nil
}

// ReceivePack opens the pack writer after decoding commands and push options.
// Release their smaller read limit; the HTTP body retains the total push limit.
type receiveStore struct {
	storage.Storer
	commands  *commandReader
	validated map[plumbing.Hash]bool
}

// PreReceive already proved these targets before publishing; do not turn a
// confirmed publication into a failed existence check by rereading its buckets.
func (s *receiveStore) HasEncodedObject(id plumbing.Hash) error {
	if s.validated[id] {
		return nil
	}
	return s.Storer.HasEncodedObject(id)
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
