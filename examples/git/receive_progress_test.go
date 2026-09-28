package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp/sideband"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage/memory"
	gitio "github.com/go-git/go-git/v6/utils/ioutil"
)

func TestReceiveProgressNegotiation(t *testing.T) {
	for _, tc := range []struct {
		name, caps string
		progress   bool
	}{
		{name: "unnegotiated", caps: "report-status"},
		{name: "quiet", caps: "side-band-64k quiet"},
		{name: "no progress", caps: "side-band-64k no-progress"},
		{name: "sideband 64k", caps: "side-band-64k", progress: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			var caps capability.List
			capability.DecodeList([]byte(tc.caps), &caps)
			progress := newReceiveProgress(&output, &caps, nil)
			if err := progress.message("Checking received pack...\n"); err != nil {
				t.Fatal(err)
			}
			if !tc.progress {
				if output.Len() != 0 {
					t.Fatalf("unrequested progress: %q", output.String())
				}
				return
			}
			scanner := pktline.NewScanner(&output)
			if !scanner.Scan() || string(scanner.Bytes()) != "\x02Checking received pack...\n" {
				t.Fatalf("invalid progress packet: %q, %v", scanner.Bytes(), scanner.Err())
			}
			if scanner.Scan() || scanner.Err() != nil {
				t.Fatalf("progress terminated the receive response: %v", scanner.Err())
			}
		})
	}
}

func TestReceiveProgressCountsImportedDeltaObjects(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		t.Run(format.String(), func(t *testing.T) {
			var output bytes.Buffer
			progress := &receiveProgress{writer: &output}
			pack := fixturePack(t, format, []packFixtureEntry{
				{kind: plumbing.BlobObject, data: []byte("abc")},
				{kind: plumbing.OFSDeltaObject, data: []byte{3, 1, 0x90, 1}, ofs: 0},
				{kind: plumbing.REFDeltaObject, data: []byte{1, 1, 1, 'b'}, ref: blobID(format, []byte("a"))},
			})
			if err := importPack(t.Context(), bytes.NewReader(pack), newImportStorage(format), format, testPackLimits(1024), progress); err != nil {
				t.Fatal(err)
			}
			if output.String() != "Importing objects: 0/3\nImporting objects: 3/3\n" {
				t.Fatalf("inaccurate imported-object progress: %q", output.String())
			}
		})
	}
}

func TestReceiveProgressThrottlesObjectMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var output bytes.Buffer
		progress := &receiveProgress{writer: &output}
		if err := progress.OnHeader(3); err != nil {
			t.Fatal(err)
		}
		if err := progress.OnInflatedObjectContent(plumbing.ZeroHash, 0, 0, nil); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if err := progress.OnInflatedObjectContent(plumbing.ZeroHash, 0, 0, nil); err != nil {
			t.Fatal(err)
		}
		if output.String() != "Importing objects: 0/3\nImporting objects: 2/3\r" {
			t.Fatalf("progress cadence: %q", output.String())
		}
	})
}

type failedProgress struct {
	err    error
	writes int
}

func (w *failedProgress) Write([]byte) (int, error) {
	w.writes++
	return 0, w.err
}

type cancellableImportStorage struct {
	*importStorage
	ctx context.Context
}

func (s *cancellableImportStorage) RawObjectWriter(kind plumbing.ObjectType, size int64) (io.WriteCloser, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	return s.importStorage.RawObjectWriter(kind, size)
}

func TestReceiveProgressWriteFailureStopsImport(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	failure := errors.New("response closed")
	output := &failedProgress{err: failure}
	progress := &receiveProgress{writer: output, cancel: cancel}
	st := &cancellableImportStorage{importStorage: newImportStorage(config.SHA1), ctx: ctx}
	pack := fixturePack(t, config.SHA1, []packFixtureEntry{
		{kind: plumbing.BlobObject, data: []byte("abc")},
		{kind: plumbing.BlobObject, data: []byte("def")},
	})
	err := importPack(ctx, bytes.NewReader(pack), st, config.SHA1, testPackLimits(1024), progress)
	// Cancellation stops storage work even when upstream ignores callback errors.
	if err == nil || ctx.Err() != context.Canceled || st.writes != 0 {
		t.Fatalf("import continued: err=%v context=%v object writes=%d", err, ctx.Err(), st.writes)
	}
	if !errors.Is(progress.err, failure) || output.writes != 1 {
		t.Fatalf("progress write error lost: err=%v writes=%d", progress.err, output.writes)
	}
	if err := progress.message("Publishing update...\n"); !errors.Is(err, failure) || output.writes != 1 {
		t.Fatalf("progress retried a failed writer: err=%v writes=%d", err, output.writes)
	}
}

type progressPackStorage struct {
	*memory.Storage
	closePack func() error
}

func (s *progressPackStorage) PackfileWriter() (io.WriteCloser, error) {
	return gitio.NewWriteCloser(io.Discard, progressPackClose{s.closePack}), nil
}

type progressPackClose struct{ close func() error }

func (c progressPackClose) Close() error { return c.close() }

func TestReceiveProgressPreservesReportStatus(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		caps         string
		unpackError  bool
		plain        bool
	}{
		{name: "committed"},
		{name: "validation rejected", reason: "stale ref"},
		{name: "publication uncertain", reason: "publication pending"},
		{name: "pack rejected", reason: "malformed pack", unpackError: true},
		{name: "quiet committed", caps: "report-status side-band-64k quiet"},
		{name: "quiet uncertain", caps: "report-status side-band-64k quiet", reason: "publication pending"},
		{name: "plain committed", caps: "report-status", plain: true},
		{name: "plain uncertain", caps: "report-status", reason: "publication pending", plain: true},
		{name: "no progress", caps: "report-status side-band-64k no-progress", plain: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			var caps capability.List
			features := tc.caps
			if features == "" {
				features = "report-status side-band-64k"
			}
			capability.DecodeList([]byte(features), &caps)
			progress := newReceiveProgress(&output, &caps, nil)
			st := &progressPackStorage{Storage: memory.NewStorage()}
			st.closePack = func() error {
				if err := progress.message("Importing objects...\n"); err != nil {
					return err
				}
				if tc.unpackError {
					return errors.New(tc.reason)
				}
				return nil
			}
			object := plumbing.NewMemoryObject(plumbing.FromObjectFormat(config.SHA1))
			object.SetType(plumbing.BlobObject)
			writer, _ := object.Writer()
			_, _ = io.WriteString(writer, "progress test")
			_ = writer.Close()
			tip, err := st.SetEncodedObject(object)
			if err != nil {
				t.Fatal(err)
			}
			request := packetForTest(strings.Repeat("0", 40) + " " + tip.String() + " refs/tags/progress\x00" + caps.String() + "\n")
			request = append(request, []byte("0000pack bytes")...)
			hookCalled := false
			err = transport.ReceivePack(
				t.Context(),
				st,
				io.NopCloser(bytes.NewReader(request)),
				gitio.WriteNopCloser(&output),
				&transport.ReceivePackRequest{StatelessRPC: true, Hooks: transport.ReceivePackHooks{
					PreReceive: func(_ context.Context, info *transport.PreReceiveInfo) error {
						hookCalled = true
						if progress != nil {
							progress.writer = info.Progress
						}
						if err := progress.message("Publishing update...\n"); err != nil {
							return err
						}
						if tc.reason != "" {
							return errors.New(tc.reason)
						}
						return nil
					},
				}},
			)
			if (err == nil) != (tc.reason == "") || hookCalled == tc.unpackError {
				t.Fatalf("receive outcome: err=%v hooked=%t", err, hookCalled)
			}
			var messages bytes.Buffer
			reportReader := io.Reader(bytes.NewReader(output.Bytes()))
			if !tc.plain {
				if !bytes.HasSuffix(output.Bytes(), []byte("0009\x0100000000")) {
					t.Fatalf("sideband termination changed: %q", output.String())
				}
				demux := sideband.NewDemuxer(sideband.Sideband64k, reportReader)
				demux.Progress = &messages
				reportReader = demux
			}
			var report packp.ReportStatus
			if err := report.Decode(reportReader); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(messages.String(), "Importing objects") != (progress != nil) {
				t.Fatalf("unexpected import feedback: %q", messages.String())
			}
			if tc.unpackError {
				if report.UnpackStatus == "ok" || len(report.CommandStatuses) != 0 {
					t.Fatalf("pack failure reported success: %+v", report)
				}
				return
			}
			status := report.CommandStatuses[0]
			want := "ok"
			if tc.reason != "" {
				want = tc.reason
			}
			if status.Status != want || status.ReferenceName != "refs/tags/progress" {
				t.Fatalf("publication status changed: %+v", status)
			}
			ref, refErr := st.Reference("refs/tags/progress")
			if tc.reason == "" && (refErr != nil || ref.Hash() != tip) {
				t.Fatalf("committed ref missing: %v", refErr)
			}
			if tc.reason != "" && !errors.Is(refErr, plumbing.ErrReferenceNotFound) {
				t.Fatalf("rejected or uncertain update moved a ref: %v", ref)
			}
		})
	}
}

func TestReceiveProgressWriteFailurePreventsPublication(t *testing.T) {
	failure := errors.New("response closed")
	var caps capability.List
	capability.DecodeList([]byte("report-status side-band-64k"), &caps)
	output := &failedProgress{err: failure}
	progress := newReceiveProgress(output, &caps, nil)
	st := &progressPackStorage{Storage: memory.NewStorage(), closePack: func() error {
		return progress.message("Checking received pack...\n")
	}}
	request := packetForTest(strings.Repeat("0", 40) + " " + strings.Repeat("1", 40) + " refs/heads/main\x00" + caps.String() + "\n")
	request = append(request, []byte("0000pack bytes")...)
	hooked := false
	err := transport.ReceivePack(
		t.Context(),
		st,
		io.NopCloser(bytes.NewReader(request)),
		gitio.WriteNopCloser(output),
		&transport.ReceivePackRequest{StatelessRPC: true, Hooks: transport.ReceivePackHooks{
			PreReceive: func(context.Context, *transport.PreReceiveInfo) error {
				hooked = true
				return nil
			},
		}},
	)
	if !errors.Is(err, failure) || hooked {
		t.Fatalf("progress failure reached publication: error=%v hook=%t", err, hooked)
	}
}
