package tests

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRepeatedPushes(t *testing.T) {
	endpoint := os.Getenv("GIT_PROBE_URL")
	if endpoint == "" || os.Getenv("GIT_REPEATED_PUSHES") == "" {
		t.Skip("set GIT_PROBE_URL and GIT_REPEATED_PUSHES=1 for local repeated-push coverage")
	}
	parallel, err := strconv.Atoi(flag.Lookup("test.parallel").Value.String())
	if err != nil || parallel < 4 {
		t.Fatal("concurrent readers and writers require -parallel=4 or greater")
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			url := strings.TrimRight(endpoint, "/") + "/" + format + ".git"
			source := filepath.Join(t.TempDir(), "source")
			branch := fmt.Sprintf("repeated-%d", time.Now().UnixNano())
			git(t, nil, "init", "--object-format="+format, "-b", branch, source)
			write(t, filepath.Join(source, "value"), []byte("base"))
			binary := make([]byte, 1<<20)
			random := rand.New(rand.NewSource(1))
			_, _ = random.Read(binary)
			write(t, filepath.Join(source, "asset.bin"), binary)
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "base")
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+branch)
			reader := filepath.Join(t.TempDir(), "reader")
			git(t, nil, "init", "--bare", "--object-format="+format, reader)
			// Finish local repacking before fsck opens the resulting pack indexes.
			git(t, nil, "-C", reader, "config", "maintenance.autoDetach", "false")
			done := make(chan struct{})
			// Cross the WAL tail capacity while another ordinary client reads.
			t.Run("traffic", func(t *testing.T) {
				t.Run("writer", func(t *testing.T) {
					t.Parallel()
					defer close(done)
					samples := make([]time.Duration, 0, 256)
					for i := 0; i < 1025; i++ {
						write(t, filepath.Join(source, "value"), []byte(fmt.Sprint(i)))
						write(t, filepath.Join(source, fmt.Sprintf("note-%d.txt", i%8)),
							[]byte(fmt.Sprintf("Small text file %d\nRevision %d\n", i%8, i)))
						if i%32 == 0 {
							// Sparse edits keep most of this incompressible object unchanged.
							offset := (i / 32 * 7919) % (len(binary) - 1024)
							_, _ = random.Read(binary[offset : offset+1024])
							write(t, filepath.Join(source, "asset.bin"), binary)
						}
						if i%64 == 0 {
							write(t, filepath.Join(source, "temporary.bin"), binary[:64<<10])
						} else if i%64 == 32 {
							if err := os.Remove(filepath.Join(source, "temporary.bin")); err != nil {
								t.Fatal(err)
							}
						}
						git(t, nil, "-C", source, "add", ".")
						git(t, nil, "-C", source, "commit", "-m", fmt.Sprint(i))
						started := time.Now()
						git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+branch)
						samples = append(samples, time.Since(started))
						if len(samples) == 256 || i == 1024 {
							logDurations(t, fmt.Sprintf("pushes ending at %d", i+1), samples)
							samples = samples[:0]
						}
					}
				})
				t.Run("reader", func(t *testing.T) {
					t.Parallel()
					var samples []time.Duration
					overlapping := 0
					for {
						started := time.Now()
						git(t, nil, "-C", reader, "fetch", url, "refs/heads/"+branch+":refs/heads/"+branch)
						samples = append(samples, time.Since(started))
						git(t, nil, "-C", reader, "fsck", "--full")
						select {
						case <-done:
							logDurations(t, "concurrent fetches", samples)
							if overlapping == 0 {
								t.Fatal("no fetch completed during writes; allow at least four parallel tests")
							}
							return
						default:
							overlapping++
						}
					}
				})
			})
			if t.Failed() {
				return
			}
			cold := filepath.Join(t.TempDir(), "cold")
			git(t, nil, "clone", "--single-branch", "--branch", branch, url, cold)
			git(t, nil, "-C", cold, "fsck", "--full")
			want := string(git(t, nil, "-C", source, "rev-list", "--objects", "HEAD"))
			if got := string(git(t, nil, "-C", cold, "rev-list", "--objects", "HEAD")); got != want {
				t.Fatal("repeated pushes lost history")
			}
			files := git(t, nil, "-C", source, "ls-files", "-z")
			if !bytes.Equal(files, git(t, nil, "-C", cold, "ls-files", "-z")) {
				t.Fatal("cold clone has different files")
			}
			for _, name := range strings.Split(strings.TrimSuffix(string(files), "\x00"), "\x00") {
				want, err := os.ReadFile(filepath.Join(source, name))
				if err != nil {
					t.Fatal(err)
				}
				got, err := os.ReadFile(filepath.Join(cold, name))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("wrong latest content for %s: %v", name, err)
				}
			}
		})
	}
}

// End-to-end client latency includes negotiation, transfer and automatic cleanup.
func logDurations(t *testing.T, label string, samples []time.Duration) {
	t.Helper()
	slices.Sort(samples)
	percentile := func(p int) time.Duration { return samples[(len(samples)*p+99)/100-1] }
	t.Logf("%s: n=%d p50=%s p95=%s p99=%s max=%s", label, len(samples), percentile(50), percentile(95), percentile(99), samples[len(samples)-1])
}
