package tests

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/go-git/go-git/v6/storage/memory"
)

func deltaFixture(t testing.TB, st storage.Storer, size int) ([]plumbing.Hash, [][]byte) {
	t.Helper()
	data := make([]byte, size)
	_, _ = rand.New(rand.NewSource(42)).Read(data)
	var hashes []plumbing.Hash
	var contents [][]byte
	for version := 0; version < 4; version++ {
		data = bytes.Clone(data)
		copy(data[version*1024:], bytes.Repeat([]byte{byte(version)}, 1024))
		obj := st.NewEncodedObject()
		obj.SetType(plumbing.BlobObject)
		w, err := obj.Writer()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.Write(data); err != nil {
			t.Fatal(err)
		}
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
		h, err := st.SetEncodedObject(obj)
		if err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, h)
		contents = append(contents, data)
	}
	return hashes, contents
}

func TestDeltaEncoderGitCompatibility(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("Git client required")
	}
	for _, format := range []formatcfg.ObjectFormat{formatcfg.SHA1, formatcfg.SHA256} {
		for _, disk := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/filesystem=%t", format, disk), func(t *testing.T) {
				st := storage.Storer(memory.NewStorage(memory.WithObjectFormat(format)))
				if disk {
					dir := filepath.Join(t.TempDir(), "source.git")
					if out, err := exec.Command("git", "init", "--bare", "--object-format="+format.String(), dir).CombinedOutput(); err != nil {
						t.Fatalf("%s: %v", out, err)
					}
					st = filesystem.NewStorage(osfs.New(dir), cache.NewObjectLRUDefault())
				}
				if closer, ok := st.(io.Closer); ok {
					t.Cleanup(func() {
						if err := closer.Close(); err != nil {
							t.Error(err)
						}
					})
				}
				hashes, contents := deltaFixture(t, st, 4<<20)
				var fullSize int
				for _, window := range []uint{0, 2} {
					var pack bytes.Buffer
					if _, err := packfile.NewEncoder(&pack, st, false).Encode(hashes, window); err != nil {
						t.Fatal(err)
					}
					dir := filepath.Join(t.TempDir(), "check.git")
					if out, err := exec.Command("git", "init", "--bare", "--object-format="+format.String(), dir).CombinedOutput(); err != nil {
						t.Fatalf("%s: %v", out, err)
					}
					cmd := exec.Command("git", "--git-dir="+dir, "index-pack", "--strict", "--stdin")
					cmd.Stdin = bytes.NewReader(pack.Bytes())
					if out, err := cmd.CombinedOutput(); err != nil {
						t.Fatalf("window%d: %s: %v", window, out, err)
					}
					for i, h := range hashes {
						out, err := exec.Command("git", "--git-dir="+dir, "cat-file", "blob", h.String()).Output()
						if err != nil || !bytes.Equal(out, contents[i]) {
							t.Fatalf("window%d object%s: %v", window, h, err)
						}
					}
					if window == 0 {
						fullSize = pack.Len()
					} else if pack.Len() >= fullSize/2 {
						t.Fatalf("delta pack %d bytes, full pack %d", pack.Len(), fullSize)
					}
					t.Logf("window=%d pack=%d bytes", window, pack.Len())
				}
			})
		}
	}
}

type deltaByteCounter struct{ n int64 }

func (w *deltaByteCounter) Write(p []byte) (int, error) { w.n += int64(len(p)); return len(p), nil }

// BenchmarkOutgoingDeltas measures only library encoding of four related blobs.
// Inputs are resident before timing; B/op is allocation volume, not peak memory.
func BenchmarkOutgoingDeltas(b *testing.B) {
	for _, size := range []int{64 << 10, 4 << 20, 16 << 20} {
		st := memory.NewStorage()
		hashes, _ := deltaFixture(b, st, size)
		for _, window := range []uint{0, 2} {
			b.Run(fmt.Sprintf("bytes=%d/window=%d", size, window), func(b *testing.B) {
				b.ReportAllocs()
				var out deltaByteCounter
				for b.Loop() {
					out.n = 0
					if _, err := packfile.NewEncoder(&out, st, false).Encode(hashes, window); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(out.n), "pack-bytes/op")
			})
		}
	}
}
