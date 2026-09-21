package tests

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Run against a fresh host with git_max_catalog_bytes=16384.
func TestColdCatalogUpdate(t *testing.T) {
	if os.Getenv("GIT_COLD_CATALOG") != "1" {
		t.Skip("set GIT_COLD_CATALOG=1 against the documented small-catalog host")
	}
	endpoint := strings.TrimRight(os.Getenv("GIT_PROBE_URL"), "/")
	if endpoint == "" {
		t.Fatal("GIT_PROBE_URL is required")
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			url := endpoint + "/" + format + ".git"
			source := filepath.Join(t.TempDir(), "source")
			git(t, nil, "init", "--object-format="+format, "-b", "cold-catalog", source)
			for i, data := range matchingBlobs(format, 1025) {
				write(t, filepath.Join(source, fmt.Sprintf("blob-%04d", i)), data)
			}
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "wide catalog")
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/cold-catalog")

			parent := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			tree := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD^{tree}")))
			var next string
			for i := range 8192 {
				next = strings.TrimSpace(string(git(t, []byte(fmt.Sprintf("candidate %d\n", i)), "-C", source, "commit-tree", tree, "-p", parent)))
				if strings.HasPrefix(next, "00") {
					break
				}
			}
			if !strings.HasPrefix(next, "00") {
				t.Fatal("could not place the update in the split catalog bucket")
			}
			git(t, nil, "-C", source, "update-ref", "refs/heads/cold-catalog", next, parent)
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/cold-catalog")
			fields := strings.Fields(string(git(t, nil, "ls-remote", url, "refs/heads/cold-catalog")))
			if len(fields) != 2 || fields[0] != next {
				t.Fatalf("cold update did not publish: %v", fields)
			}
		})
	}
}

func matchingBlobs(format string, count int) [][]byte {
	objects := make([][]byte, 0, count)
	input := make([]byte, len("blob 8\x00")+8)
	copy(input, "blob 8\x00")
	for counter := uint64(0); len(objects) < count; counter++ {
		binary.BigEndian.PutUint64(input[len(input)-8:], counter)
		matches := sha1.Sum(input)[0] == 0
		if format == "sha256" {
			matches = sha256.Sum256(input)[0] == 0
		}
		if matches {
			objects = append(objects, append([]byte(nil), input[len(input)-8:]...))
		}
	}
	return objects
}

// Run against a fresh host with wal_max_collection_objects=32.
func TestCollectionCapacity(t *testing.T) {
	if os.Getenv("GIT_COLLECTION_CAPACITY") != "1" {
		t.Skip("set GIT_COLLECTION_CAPACITY=1 against the documented small-limit host")
	}
	endpoint := strings.TrimRight(os.Getenv("GIT_PROBE_URL"), "/")
	if endpoint == "" {
		t.Fatal("GIT_PROBE_URL is required")
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			url := endpoint + "/" + format + ".git"
			source := filepath.Join(t.TempDir(), "source")
			git(t, nil, "init", "--object-format="+format, "-b", "capacity", source)
			random := rand.New(rand.NewSource(1))
			var published string
			for i := range 64 {
				data := make([]byte, 2048)
				_, _ = random.Read(data)
				write(t, filepath.Join(source, fmt.Sprintf("blob-%02d", i)), data)
				git(t, nil, "-C", source, "add", ".")
				git(t, nil, "-C", source, "commit", "-m", fmt.Sprint(i))
				tip := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
				command := gitCommand("-C", source, "push", url, "HEAD:refs/heads/capacity")
				output, err := command.CombinedOutput()
				if err == nil {
					published = tip
					continue
				}
				if published == "" || !strings.Contains(string(output), "publication objects") {
					t.Fatalf("unexpected push failure after %d publications: %v\n%s", i, err, output)
				}
				fields := strings.Fields(string(git(t, nil, "ls-remote", url, "refs/heads/capacity")))
				if len(fields) != 2 || fields[0] != published || fields[0] == tip {
					t.Fatalf("rejected update changed the ref: %v", fields)
				}
				finishMaintenance(t, url)
				clone := filepath.Join(t.TempDir(), "clone")
				git(t, nil, "clone", "--branch", "capacity", url, clone)
				git(t, nil, "-C", clone, "fsck", "--strict")
				return
			}
			t.Fatal("64 growing pushes did not reach the 32-object collection limit")
		})
	}
}

func finishMaintenance(t *testing.T, url string) {
	t.Helper()
	if finishGitMaintenance(t, url) == 0 {
		t.Fatal("maintenance did not find unpublished objects")
	}
}
