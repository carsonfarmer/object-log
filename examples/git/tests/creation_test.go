package tests

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRepositoryCreation(t *testing.T) {
	endpoint := os.Getenv("GIT_PROBE_URL")
	if endpoint == "" || os.Getenv("GIT_PROBE_CREATE") != "1" {
		t.Skip("set GIT_PROBE_URL and GIT_PROBE_CREATE=1 for a local service with wildcard policy")
	}
	request := func(method, url, password, body string, want int) {
		t.Helper()
		r, err := http.NewRequest(method, url, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		r.SetBasicAuth("git", password)
		r.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil || response.StatusCode != want {
			t.Fatalf("%s: HTTP %d, want %d: %s (%v)", url, response.StatusCode, want, data, err)
		}
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			name := fmt.Sprintf("created/%s-%d", format, time.Now().UnixNano())
			url := strings.TrimRight(endpoint, "/") + "/" + name
			password := os.Getenv("GIT_PROBE_PASSWORD")
			settings := `{"format":"` + format + `","default_branch":"release"}`
			request(http.MethodPost, url+"/create", "wrong-password", settings, 401)
			request(http.MethodGet, url+"/info/refs?service=git-upload-pack", password, "", 404)
			request(http.MethodGet, url+"/info/refs?service=git-receive-pack", password, "", 404)
			request(http.MethodPost, url+"/maintenance", password, "{}", 404)
			request(http.MethodPost, url+"/create", password, strings.Repeat(" ", 4097), 413)
			request(http.MethodPost, url+"/create", password, `{"log_id":"override"}`, 400)
			request(http.MethodPost, url+"/create", password, settings, 201)
			request(http.MethodPost, url+".git/create", password, settings, 409)
			other := "sha1"
			if format == other {
				other = "sha256"
			}
			request(http.MethodPost, url+"/create", password, `{"format":"`+other+`"}`, 409)
			root := t.TempDir()
			git(t, nil, "init", "--object-format="+format, "-b", "release", root)
			write(t, filepath.Join(root, "README.md"), []byte(name))
			git(t, nil, "-C", root, "add", ".")
			git(t, nil, "-C", root, "commit", "-m", "created through API")
			git(t, nil, "-C", root, "push", url, "HEAD:refs/heads/release")
			clone := filepath.Join(t.TempDir(), "clone")
			git(t, nil, "-c", "protocol.version=2", "clone", url+".git", clone)
			git(t, nil, "-C", clone, "fsck", "--full")
			if got := git(t, nil, "-C", clone, "symbolic-ref", "--short", "HEAD"); strings.TrimSpace(string(got)) != "release" {
				t.Fatal("created default branch was lost")
			}
			if got := git(t, nil, "-C", clone, "show", "HEAD:README.md"); !bytes.Equal(got, []byte(name)) {
				t.Fatal("created repository data differs")
			}
		})
	}
	t.Run("concurrent creators", func(t *testing.T) {
		url := fmt.Sprintf("%s/created/race-%d", strings.TrimRight(endpoint, "/"), time.Now().UnixNano())
		type result struct {
			format string
			status int
			err    error
		}
		results := make(chan result, 2)
		for _, format := range []string{"sha1", "sha256"} {
			go func() {
				r, err := http.NewRequest(http.MethodPost, url+"/create", strings.NewReader(`{"format":"`+format+`"}`))
				if err != nil {
					results <- result{format: format, err: err}
					return
				}
				r.SetBasicAuth("git", os.Getenv("GIT_PROBE_PASSWORD"))
				response, err := http.DefaultClient.Do(r)
				status := 0
				if err == nil {
					status = response.StatusCode
					_, _ = io.Copy(io.Discard, response.Body)
					err = response.Body.Close()
				}
				results <- result{format: format, status: status, err: err}
			}()
		}
		winner := ""
		for range 2 {
			r := <-results
			if r.err != nil || (r.status != 201 && r.status != 409) {
				t.Fatalf("concurrent creation: %+v", r)
			}
			if r.status == 201 {
				if winner != "" {
					t.Fatal("two creations committed")
				}
				winner = r.format
			}
		}
		if winner == "" {
			t.Fatal("no creation committed")
		}
		r, err := http.NewRequest(http.MethodGet, url+"/info/refs?service=git-receive-pack", nil)
		if err != nil {
			t.Fatal(err)
		}
		r.SetBasicAuth("git", os.Getenv("GIT_PROBE_PASSWORD"))
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil || response.StatusCode != 200 || !bytes.Contains(data, []byte("object-format="+winner)) {
			t.Fatal("stored format does not match winning creator")
		}
	})
}
