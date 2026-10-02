package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage/memory"
	gitio "github.com/go-git/go-git/v6/utils/ioutil"
)

type refNameSink struct{ *memory.Storage }

func (s *refNameSink) PackfileWriter() (io.WriteCloser, error) {
	return gitio.WriteNopCloser(io.Discard), nil
}

func TestReceiveRefNamesBeforePublication(t *testing.T) {
	bad := plumbing.ReferenceName("refs/heads/\u200c./review-probe")
	if err := bad.Validate(); err != nil || !bad.IsUnderRefs() || !bad.IsSafe() {
		t.Fatalf("regression name must pass ordinary ref checks: %v", err)
	}
	if out, err := exec.Command("git", "check-ref-format", bad.String()).CombinedOutput(); err != nil {
		t.Fatalf("regression name must pass Git check-ref-format: %v: %s", err, out)
	}

	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		t.Run(format.String(), func(t *testing.T) {
			zero := plumbing.NewHash(strings.Repeat("0", format.HexSize()))
			tip, err := plumbing.FromObjectFormat(format).Compute(plumbing.BlobObject, nil)
			if err != nil {
				t.Fatal(err)
			}
			published := 0
			hook := func(info *transport.PreReceiveInfo) error {
				for _, cmd := range info.Commands {
					if err := validateRefName(cmd.Name); err != nil {
						return err
					}
				}
				published++
				return nil
			}
			badCommand := &packp.Command{Name: bad, Old: zero, New: tip}
			badReply, err := receiveRefForTest(t, format, badCommand, []byte("staged pack"), hook)
			if err == nil || !strings.Contains(badReply, "ng "+bad.String()) || published != 0 {
				t.Fatalf("rejected ref reached publication: err=%v reply=%q publishes=%d", err, badReply, published)
			}
			// With no service hook, stock go-git rejects this same command after
			// PreReceive, which is the ordering the service must guard against.
			baseline, err := receiveRefForTest(t, format, badCommand, []byte("staged pack"), nil)
			if !errors.Is(err, transport.ErrFunnyRefname) || !strings.Contains(baseline, "funny refname") {
				t.Fatalf("stock receive gate changed: err=%v reply=%q", err, baseline)
			}

			for _, name := range []plumbing.ReferenceName{"refs/heads/main", "refs/stash"} {
				t.Run(name.String(), func(t *testing.T) {
					published = 0
					command := &packp.Command{Name: name, Old: zero, New: tip}
					reply, err := receiveRefForTest(t, format, command, []byte("staged pack"), hook)
					if err != nil || !strings.Contains(reply, "ok "+name.String()) || published != 1 {
						t.Fatalf("valid ref did not pass receive and publication: err=%v reply=%q publishes=%d", err, reply, published)
					}
				})
			}
		})
	}
}

func receiveRefForTest(t *testing.T, format config.ObjectFormat, command *packp.Command, pack []byte, hook func(*transport.PreReceiveInfo) error) (string, error) {
	t.Helper()
	var request, reply bytes.Buffer
	if _, err := pktline.Writef(&request, "%s %s %s\x00report-status object-format=%s\n", command.Old, command.New, command.Name, format); err != nil {
		t.Fatal(err)
	}
	if err := pktline.WriteFlush(&request); err != nil {
		t.Fatal(err)
	}
	request.Write(pack)
	sink := &refNameSink{Storage: memory.NewStorage(memory.WithObjectFormat(format))}
	object := sink.NewEncodedObject()
	object.SetType(plumbing.BlobObject)
	if _, err := sink.SetEncodedObject(object); err != nil {
		t.Fatal(err)
	}
	if !command.Old.IsZero() {
		if err := sink.SetReference(plumbing.NewHashReference(command.Name, command.Old)); err != nil {
			t.Fatal(err)
		}
	}
	var hooks transport.ReceivePackHooks
	if hook != nil {
		hooks.PreReceive = func(_ context.Context, info *transport.PreReceiveInfo) error { return hook(info) }
	}
	err := transport.ReceivePack(t.Context(), sink, io.NopCloser(&request), gitio.WriteNopCloser(&reply), &transport.ReceivePackRequest{StatelessRPC: true, Hooks: hooks})
	return reply.String(), err
}

func TestReceiveRejectsDuplicateRefsBeforePublication(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		t.Run(format.String(), func(t *testing.T) {
			var request, reply bytes.Buffer
			old, next := strings.Repeat("0", format.HexSize()), strings.Repeat("1", format.HexSize())
			for _, suffix := range []string{"\x00report-status object-format=" + format.String(), ""} {
				if _, err := pktline.Writef(&request, "%s %s refs/heads/main%s\n", old, next, suffix); err != nil {
					t.Fatal(err)
				}
			}
			if err := pktline.WriteFlush(&request); err != nil {
				t.Fatal(err)
			}
			request.WriteString("staged pack")
			published := false
			sink := &refNameSink{Storage: memory.NewStorage(memory.WithObjectFormat(format))}
			err := transport.ReceivePack(t.Context(), sink, io.NopCloser(&request), gitio.WriteNopCloser(&reply), &transport.ReceivePackRequest{
				StatelessRPC: true, Hooks: transport.ReceivePackHooks{PreReceive: func(context.Context, *transport.PreReceiveInfo) error {
					published = true
					return nil
				}},
			})
			if err == nil || published || !strings.Contains(reply.String(), "multiple updates for ref") {
				t.Fatalf("duplicate ref reached publication: err=%v published=%t reply=%q", err, published, &reply)
			}
		})
	}
}
