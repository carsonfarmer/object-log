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

func TestReceiveFormat(t *testing.T) {
	for _, tc := range []struct {
		name, caps string
		width      int
		want       config.ObjectFormat
	}{
		{"legacy SHA-1", "report-status", 40, config.SHA1},
		{"SHA-1", "object-format=sha1", 40, config.SHA1},
		{"SHA-256", "object-format=sha256", 64, config.SHA256},
		{"missing SHA-256 capability", "report-status", 64, ""},
		{"wrong width", "object-format=sha256", 40, ""},
		{"unknown format", "object-format=other", 40, ""},
		{"missing format", "object-format", 40, ""},
		{"multiple formats", "object-format=sha1 object-format=sha256", 40, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var prefix bytes.Buffer
			_, err := pktline.Writef(&prefix, "%s %s refs/heads/main\x00%s\n", strings.Repeat("0", tc.width), strings.Repeat("1", tc.width), tc.caps)
			if err != nil {
				t.Fatal(err)
			}
			packet := bytes.Clone(prefix.Bytes())
			input := append(bytes.Clone(packet), []byte("0000PACKpayload")...)
			body := bytes.NewBuffer(input)
			format, replay, err := receiveFormat(body, int64(len(packet)))
			if tc.want == "" {
				if err == nil {
					t.Fatal("invalid format accepted")
				}
				return
			}
			if err != nil || format != tc.want || body.Len() != len("0000PACKpayload") {
				t.Fatalf("format=%s error=%v unread=%d", format, err, body.Len())
			}
			data, err := io.ReadAll(replay)
			if err != nil || !bytes.Equal(data, input) {
				t.Fatalf("push body was changed: %v", err)
			}
			if _, _, err := receiveFormat(bytes.NewReader(input), int64(len(packet)-1)); !errors.Is(err, errObjectLimit) {
				t.Fatalf("negotiation limit: %v", err)
			}
		})
	}
	for _, input := range []string{"0000", string(packetForTest("shallow "+strings.Repeat("1", 40)+"\n")) + "0000"} {
		format, replay, err := receiveFormat(strings.NewReader(input), int64(len(input)))
		if err != nil || format != "" {
			t.Fatalf("no-op push: format=%s error=%v", format, err)
		}
		data, err := io.ReadAll(replay)
		if err != nil || string(data) != input {
			t.Fatalf("no-op body was changed: %v", err)
		}
	}
}

func packetForTest(text string) []byte {
	return []byte(fmt.Sprintf("%04x%s", len(text)+4, text))
}
