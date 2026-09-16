package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage/memory"
	gitio "github.com/go-git/go-git/v6/utils/ioutil"
)

type receiveSink struct {
	*memory.Storage
	pack bytes.Buffer
}

func (s *receiveSink) PackfileWriter() (io.WriteCloser, error) {
	return gitio.WriteNopCloser(&s.pack), nil
}

func TestReceiveCommandLimit(t *testing.T) {
	refusal := errors.New("push refused by policy")
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		for _, deleted := range []bool{false, true} {
			for _, extra := range []int{-1, 0, 4096} {
				t.Run(fmt.Sprintf("%s/delete=%t/extra=%d", format, deleted, extra), func(t *testing.T) {
					old, next := strings.Repeat("0", format.HexSize()), strings.Repeat("1", format.HexSize())
					payload := bytes.Repeat([]byte("pack bytes"), 8192)
					if deleted {
						old, next = next, old
						payload = nil
					}
					var commands bytes.Buffer
					if _, err := pktline.Writef(&commands, "%s %s refs/heads/main\x00report-status object-format=%s\n", old, next, format); err != nil {
						t.Fatal(err)
					}
					if err := pktline.WriteFlush(&commands); err != nil {
						t.Fatal(err)
					}
					body := &commandReader{LimitedReader: io.LimitedReader{
						R: io.MultiReader(&commands, bytes.NewReader(payload)), N: int64(commands.Len() + extra),
					}}
					sink := &receiveSink{Storage: memory.NewStorage(memory.WithObjectFormat(format))}
					s := &receiveStore{Storer: sink, commands: body}
					hooked := false
					err := transport.ReceivePack(
						t.Context(), s, io.NopCloser(body), gitio.WriteNopCloser(io.Discard),
						&transport.ReceivePackRequest{StatelessRPC: true, Hooks: transport.ReceivePackHooks{
							PreReceive: func(context.Context, *transport.PreReceiveInfo) error {
								hooked = true
								return refusal
							},
						}},
					)
					if extra < 0 {
						if !errors.Is(err, errObjectLimit) || hooked || sink.pack.Len() != 0 {
							t.Fatalf("over limit: err=%v hook=%t bytes=%d", err, hooked, sink.pack.Len())
						}
						return
					}
					if !errors.Is(err, refusal) || !hooked || !bytes.Equal(sink.pack.Bytes(), payload) {
						t.Fatalf("boundary: err=%v hook=%t bytes=%d/%d", err, hooked, sink.pack.Len(), len(payload))
					}
				})
			}
		}
	}
}
