package tests

import (
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// GIT_LARGE_OBJECT_MIB scales this same streamed fixture up to 512 MiB or more.
// Neither fixture generation nor verification retains the whole blob in memory.
func TestLargeBlob(t *testing.T) {
	endpoint := os.Getenv("GIT_PROBE_URL")
	if endpoint == "" {
		t.Skip("set GIT_PROBE_URL to a local Spin/MinIO instance")
	}
	mib := int64(16)
	if value := os.Getenv("GIT_LARGE_OBJECT_MIB"); value != "" {
		var err error
		mib, err = strconv.ParseInt(value, 10, 42)
		if err != nil || mib < 1 {
			t.Fatal("GIT_LARGE_OBJECT_MIB must be a positive integer")
		}
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			url := strings.TrimRight(endpoint, "/") + "/" + format + ".git"
			branch := fmt.Sprintf("large-%d", time.Now().UnixNano())
			git(t, nil, "init", "--object-format="+format, "-b", branch, source)
			path := filepath.Join(source, "large.bin")
			file, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			hash := sha256.New()
			size, err := io.CopyN(io.MultiWriter(file, hash), rand.New(rand.NewSource(91)), mib*1024*1024)
			closed := file.Close()
			if err != nil || closed != nil {
				t.Fatalf("write fixture: %v %v", err, closed)
			}
			want := fmt.Sprintf("%x", hash.Sum(nil))
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "large blob")
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+branch)
			clone := filepath.Join(root, "clone")
			git(t, nil, "-c", "protocol.version=2", "clone", "--single-branch", "--branch", branch, url, clone)
			git(t, nil, "-C", clone, "fsck", "--full")
			file, err = os.Open(filepath.Join(clone, "large.bin"))
			if err != nil {
				t.Fatal(err)
			}
			hash.Reset()
			gotSize, err := io.Copy(hash, file)
			closed = file.Close()
			if err != nil || closed != nil || gotSize != size || fmt.Sprintf("%x", hash.Sum(nil)) != want {
				t.Fatalf("large blob differs: size=%d want=%d errors=%v %v", gotSize, size, err, closed)
			}
			// A one-byte edit lets ordinary Git choose a thin delta when appropriate.
			file, err = os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			var edit [1]byte
			_, err = file.ReadAt(edit[:], size/2)
			if err == nil {
				edit[0] ^= 1
				_, err = file.WriteAt(edit[:], size/2)
			}
			closed = file.Close()
			if err != nil || closed != nil {
				t.Fatalf("edit fixture: %v %v", err, closed)
			}
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "small update")
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+branch)
			git(t, nil, "-C", clone, "-c", "protocol.version=2", "fetch", "origin")
			git(t, nil, "-C", clone, "fsck", "--full")
			tip := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			if got := strings.TrimSpace(string(git(t, nil, "-C", clone, "rev-parse", "origin/"+branch))); got != tip {
				t.Fatalf("fetch got %s want %s", got, tip)
			}
			git(t, nil, "-C", clone, "reset", "--hard", "origin/"+branch)
			if fileDigest(t, path) != fileDigest(t, filepath.Join(clone, "large.bin")) {
				t.Fatal("updated large blob differs")
			}
			t.Logf("pushed, cloned, edited, and fetched %d MiB blob with matching SHA-256", mib)
		})
	}
}

func fileDigest(t *testing.T, path string) string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	_, err = io.Copy(hash, file)
	closed := file.Close()
	if err != nil || closed != nil {
		t.Fatalf("hash fixture: %v %v", err, closed)
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}
