package tests

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFetchRetentionSurvivesDestructiveCollection(t *testing.T) {
	endpoint := strings.TrimRight(os.Getenv("GIT_PROBE_URL"), "/")
	if endpoint == "" {
		t.Skip("set GIT_PROBE_URL to an isolated local Spin/MinIO instance")
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			url := endpoint + "/" + format + ".git"
			source := filepath.Join(t.TempDir(), "source")
			branch, tag := "retention-"+format, "retention-tag-"+format
			git(t, nil, "init", "--object-format="+format, "-b", branch, source)
			payload := make([]byte, 32<<20)
			_, _ = rand.New(rand.NewSource(44)).Read(payload)
			write(t, filepath.Join(source, "payload.bin"), payload)
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "retained fetch")
			git(t, nil, "-C", source, "tag", "-a", tag, "-m", "retained tag")
			tip := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			git(t, nil, "-C", source, "push", "--atomic", url,
				"HEAD:refs/heads/"+branch, "refs/tags/"+tag+":refs/tags/"+tag)
			body := append(packet("command=fetch\n"), packet("object-format="+format+"\n")...)
			body = append(body, []byte("0001")...)
			body = append(body, packet("want "+tip+"\n")...)
			body = append(body, packet("done\n")...)
			body = append(body, []byte("0000")...)
			request, err := http.NewRequest(http.MethodPost, url+"/git-upload-pack", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/x-git-upload-pack-request")
			request.Header.Set("Git-Protocol", "version=2")
			if password := os.Getenv("GIT_PROBE_PASSWORD"); password != "" {
				request.SetBasicAuth("git", password)
			}
			response, err := (&http.Client{Timeout: 5 * time.Minute}).Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()

			// Headers arrive after upload-pack starts writing. Leaving the large body
			// unread pauses the response while its retention remains active.
			git(t, nil, "-C", source, "push", "--atomic", url,
				":refs/heads/"+branch, ":refs/tags/"+tag)
			result := requestGitMaintenance(t, url)
			if result.State != "retained" {
				t.Fatalf("maintenance during fetch: %+v", result)
			}

			packResponse, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != http.StatusOK {
				t.Fatalf("fetch response: status=%d error=%v", response.StatusCode, err)
			}
			target := filepath.Join(t.TempDir(), "target")
			git(t, nil, "init", "--object-format="+format, target)
			git(t, unband(t, packResponse), "-C", target, "index-pack", "--stdin")
			git(t, nil, "-C", target, "fsck", "--strict")

			result = requestGitMaintenance(t, url)
			if result.Objects == 0 {
				t.Fatalf("maintenance after fetch found no collection candidates: %+v", result)
			}
			collectDrill(t, url)
		})
	}
}

func TestFetchRetentionDoesNotRejectSingleWriter(t *testing.T) {
	endpoint := strings.TrimRight(os.Getenv("GIT_PROBE_URL"), "/")
	if endpoint == "" {
		t.Skip("set GIT_PROBE_URL to an isolated local Spin/MinIO instance")
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			url := endpoint + "/" + format + ".git"
			branch := fmt.Sprintf("retention-push-%s-%d", format, time.Now().UnixNano())
			source := filepath.Join(t.TempDir(), "source")
			reader := filepath.Join(t.TempDir(), "reader")
			git(t, nil, "init", "--object-format="+format, "-b", branch, source)
			write(t, filepath.Join(source, "value"), []byte("0"))
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "initial")
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+branch)
			git(t, nil, "init", "--bare", "--object-format="+format, reader)

			t.Run("concurrent", func(t *testing.T) {
				t.Run("writer", func(t *testing.T) {
					t.Parallel()
					for round := 1; round <= 16; round++ {
						write(t, filepath.Join(source, "value"), []byte(fmt.Sprint(round)))
						git(t, nil, "-C", source, "commit", "-am", fmt.Sprintf("update %d", round))
						git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+branch)
					}
				})
				t.Run("reader", func(t *testing.T) {
					t.Parallel()
					for range 32 {
						git(t, nil, "-C", reader, "-c", "protocol.version=2", "fetch", url,
							"+refs/heads/"+branch+":refs/heads/"+branch)
					}
				})
			})

			git(t, nil, "-C", reader, "-c", "protocol.version=2", "fetch", url,
				"+refs/heads/"+branch+":refs/heads/"+branch)
			want := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			got := strings.TrimSpace(string(git(t, nil, "-C", reader, "rev-parse", "refs/heads/"+branch)))
			if got != want {
				t.Fatalf("fetched tip %s, want %s", got, want)
			}
			git(t, nil, "-C", reader, "fsck", "--strict")
		})
	}
}
