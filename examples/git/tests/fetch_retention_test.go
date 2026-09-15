package tests

import (
	"bytes"
	"context"
	"encoding/json"
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
			type collectionReport struct {
				State   string `json:"state"`
				Objects uint64 `json:"candidate_objects"`
			}

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
			maintenance := drillRequest(t, context.Background(), url+"/maintenance", nil)
			var result collectionReport
			err = json.NewDecoder(maintenance.Body).Decode(&result)
			maintenance.Body.Close()
			if err != nil || maintenance.StatusCode != http.StatusOK || result.State != "retained" {
				t.Fatalf("maintenance during fetch: status=%d state=%q error=%v", maintenance.StatusCode, result.State, err)
			}

			packResponse, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != http.StatusOK {
				t.Fatalf("fetch response: status=%d error=%v", response.StatusCode, err)
			}
			target := filepath.Join(t.TempDir(), "target")
			git(t, nil, "init", "--object-format="+format, target)
			git(t, unband(t, packResponse), "-C", target, "index-pack", "--stdin")
			git(t, nil, "-C", target, "fsck", "--strict")

			maintenance = drillRequest(t, context.Background(), url+"/maintenance", nil)
			result = collectionReport{}
			err = json.NewDecoder(maintenance.Body).Decode(&result)
			maintenance.Body.Close()
			if err != nil || maintenance.StatusCode != http.StatusOK || result.Objects == 0 {
				t.Fatalf("maintenance after fetch: status=%d state=%q objects=%d error=%v", maintenance.StatusCode, result.State, result.Objects, err)
			}
			collectDrill(t, url)
		})
	}
}
