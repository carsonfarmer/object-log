//go:build git_native_test

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
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

// The native test file list excludes the WASI-backed store implementation.
type store struct {
	*memory.Storage
	meta    rootMeta
	pending map[string]struct{}
}

func TestValidateRejectsUnsafeRefBeforePublication(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		t.Run(format.String(), func(t *testing.T) {
			st := &store{Storage: memory.NewStorage(memory.WithObjectFormat(format)), meta: rootMeta{Format: format, Refs: map[string]string{}}}
			command := &packp.Command{
				Name: plumbing.ReferenceName("refs/heads/\u200c./review-probe"),
				Old:  plumbing.NewHash(strings.Repeat("0", format.HexSize())),
				New:  plumbing.NewHash(strings.Repeat("1", format.HexSize())),
			}
			refs, err := new(receiveStore).validate(st, []*packp.Command{command})
			if refs != nil || !errors.Is(err, plumbing.ErrInvalidReferenceName) {
				t.Fatalf("validate returned refs=%v err=%v, want invalid reference name before publication", refs, err)
			}
		})
	}
}

func TestValidateRejectsMixedCommandFormats(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		st := &store{Storage: memory.NewStorage(memory.WithObjectFormat(format)), meta: rootMeta{Format: format, Refs: map[string]string{}}}
		other := config.SHA1
		if format == other {
			other = config.SHA256
		}
		for _, ids := range [][2]config.ObjectFormat{{other, format}, {format, other}} {
			command := &packp.Command{Name: "refs/heads/main",
				Old: plumbing.NewHash(strings.Repeat("1", ids[0].HexSize())),
				New: plumbing.NewHash(strings.Repeat("0", ids[1].HexSize())),
			}
			if refs, err := new(receiveStore).validate(st, []*packp.Command{command}); err == nil || refs != nil || !strings.Contains(err.Error(), "object format") {
				t.Fatalf("mixed-format deletion accepted: refs=%v error=%v", refs, err)
			}
		}
	}
}

type publicationLookupFault struct {
	*refNameSink
	published  bool
	lateChecks int
	failure    error
}

func (s *publicationLookupFault) HasEncodedObject(id plumbing.Hash) error {
	if s.published {
		s.lateChecks++
		return s.failure
	}
	return s.refNameSink.HasEncodedObject(id)
}

func TestReceiveValidatedTargetsAfterPublication(t *testing.T) {
	storageFailure := errors.New("object-store read failed")
	publicationFailure := errors.New("publication failed")
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		for _, test := range []struct {
			name        string
			missing     bool
			publishErr  error
			cancel      bool
			lateFailure error
		}{
			{name: "storage failure after publication", lateFailure: storageFailure},
			{name: "catalog limit after publication", lateFailure: errObjectLimit},
			{name: "missing target", missing: true},
			{name: "publication refused", publishErr: publicationFailure},
			{name: "transport canceled after publication", cancel: true, lateFailure: storageFailure},
		} {
			t.Run(format.String()+"/"+test.name, func(t *testing.T) {
				st := &store{Storage: memory.NewStorage(memory.WithObjectFormat(format)), meta: rootMeta{Format: format, Refs: map[string]string{}}, pending: map[string]struct{}{}}
				var targets []plumbing.Hash
				for _, content := range []string{"existing object", "imported object"} {
					targets = append(targets, validationRaw(t, &validationStore{Storage: st.Storage}, plumbing.BlobObject, content))
				}
				st.meta.Refs["refs/tags/deleted"] = targets[0].String()
				st.pending[targets[1].String()] = struct{}{}
				if test.missing {
					delete(st.Objects, targets[1])
				}
				zero := plumbing.NewHash(strings.Repeat("0", format.HexSize()))
				commands := []*packp.Command{
					{Name: "refs/tags/existing", Old: zero, New: targets[0]},
					{Name: "refs/tags/imported", Old: zero, New: targets[1]},
					{Name: "refs/tags/deleted", Old: targets[0], New: zero},
				}
				var request, reply bytes.Buffer
				for i, cmd := range commands {
					caps := ""
					if i == 0 {
						caps = "\x00report-status object-format=" + format.String()
					}
					if _, err := pktline.Writef(&request, "%s %s %s%s\n", cmd.Old, cmd.New, cmd.Name, caps); err != nil {
						t.Fatal(err)
					}
				}
				if err := pktline.WriteFlush(&request); err != nil {
					t.Fatal(err)
				}
				request.WriteString("staged pack")
				// Objects are imported above; the sink consumes the pack. After
				// publication, model reads of a rewritten bucket failing.
				sink := &publicationLookupFault{refNameSink: &refNameSink{Storage: st.Storage}, failure: test.lateFailure}
				if err := sink.SetReference(plumbing.NewHashReference(commands[2].Name, targets[0])); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				body := &commandReader{LimitedReader: io.LimitedReader{R: &request, N: int64(request.Len())}}
				push := &receiveStore{Storer: sink, commands: body}
				err := transport.ReceivePack(ctx, push, io.NopCloser(body), gitio.WriteNopCloser(&reply), &transport.ReceivePackRequest{
					StatelessRPC: true,
					Hooks: transport.ReceivePackHooks{PreReceive: func(_ context.Context, info *transport.PreReceiveInfo) error {
						if _, err := push.validate(st, info.Commands); err != nil {
							return err
						}
						if test.publishErr != nil {
							return test.publishErr
						}
						// The service publishes before go-git updates temporary refs.
						sink.published = true
						if test.cancel {
							cancel()
						}
						return nil
					}},
				})
				if test.missing || test.publishErr != nil {
					want := test.publishErr
					if test.missing {
						want = plumbing.ErrObjectNotFound
						if len(push.validated) != 0 {
							t.Fatal("failed validation retained a receipt")
						}
					}
					if !errors.Is(err, want) || sink.published || sink.lateChecks != 0 || strings.Contains(reply.String(), "ok refs/") {
						t.Fatalf("refused push acknowledged: err=%v published=%t checks=%d reply=%q", err, sink.published, sink.lateChecks, reply.String())
					}
					for _, cmd := range commands[:2] {
						if _, err := sink.Reference(cmd.Name); !errors.Is(err, plumbing.ErrReferenceNotFound) {
							t.Fatalf("refused push updated %s: %v", cmd.Name, err)
						}
					}
					if _, err := sink.Reference(commands[2].Name); err != nil {
						t.Fatalf("refused push deleted existing ref: %v", err)
					}
					return
				}
				if test.cancel {
					if !errors.Is(err, context.Canceled) || strings.Contains(reply.String(), "ok refs/") {
						t.Fatalf("canceled transport acknowledged success: err=%v reply=%q", err, reply.String())
					}
				} else if err != nil {
					t.Fatalf("confirmed publication failed: err=%v reply=%q", err, reply.String())
				}
				if !sink.published || sink.lateChecks != 0 {
					t.Fatalf("confirmed publication reread targets: published=%t checks=%d", sink.published, sink.lateChecks)
				}
				for _, cmd := range commands {
					if !test.cancel && !strings.Contains(reply.String(), "ok "+cmd.Name.String()+"\n") {
						t.Fatalf("missing command status: %q", reply.String())
					}
					if cmd.New.IsZero() {
						if _, err := sink.Reference(cmd.Name); !errors.Is(err, plumbing.ErrReferenceNotFound) {
							t.Fatalf("deletion failed: %v", err)
						}
					} else if ref, err := sink.Reference(cmd.Name); err != nil || ref.Hash() != cmd.New {
						t.Fatalf("ref update failed: ref=%v err=%v", ref, err)
					}
				}
				if test.cancel {
					return
				}
				otherFormat := config.SHA1
				if format == otherFormat {
					otherFormat = config.SHA256
				}
				for _, unproved := range []plumbing.Hash{zero, plumbing.NewHash(strings.Repeat("2", format.HexSize())), plumbing.NewHash(strings.Repeat("2", otherFormat.HexSize()))} {
					if err := push.HasEncodedObject(unproved); !errors.Is(err, test.lateFailure) {
						t.Fatalf("unproved target suppressed underlying error: id=%s err=%v", unproved, err)
					}
				}
				if sink.lateChecks != 3 {
					t.Fatalf("unproved targets not delegated: checks=%d", sink.lateChecks)
				}
			})
		}
	}
}
