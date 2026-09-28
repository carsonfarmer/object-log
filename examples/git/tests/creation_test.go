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
		r.Header.Set("Content-Type", "application/x-git-receive-pack-request")
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
			read := url + "/info/refs?service=git-upload-pack"
			request(http.MethodGet, url+"/info/refs?service=git-receive-pack", "wrong-password", "", 401)
			request(http.MethodGet, read, password, "", 404)
			request(http.MethodGet, url+"/info/refs?service=git-receive-pack", password, "", 200)
			request(http.MethodPost, url+"/git-receive-pack", password, "0000", 200)
			request(http.MethodGet, read, password, "", 404)
			request(http.MethodPost, url+"/git-receive-pack", password, "invalid", 400)
			width := 40
			if format == "sha256" {
				width = 64
			}
			command := packet(strings.Repeat("0", width) + " " + strings.Repeat("1", width) + " refs/heads/release\x00report-status object-format=" + format + "\n")
			request(http.MethodPost, url+"/git-receive-pack", password, string(command)+"0000broken pack", 200)
			request(http.MethodGet, read, password, "", 404)
			root := t.TempDir()
			git(t, nil, "init", "--object-format="+format, "-b", "release", root)
			write(t, filepath.Join(root, "README.md"), []byte(name))
			git(t, nil, "-C", root, "add", ".")
			git(t, nil, "-C", root, "commit", "-m", "created by first push")
			git(t, nil, "-C", root, "push", url, "HEAD:refs/heads/release")
			clone := filepath.Join(t.TempDir(), "clone")
			git(t, nil, "-c", "protocol.version=2", "clone", url+".git", clone)
			git(t, nil, "-C", clone, "fsck", "--full")
			if got := git(t, nil, "-C", clone, "symbolic-ref", "--short", "HEAD"); strings.TrimSpace(string(got)) != "release" {
				t.Fatal("first branch was not retained as default")
			}
			if got := git(t, nil, "-C", clone, "show", "HEAD:README.md"); !bytes.Equal(got, []byte(name)) {
				t.Fatal("created repository data differs")
			}
		})
	}
	t.Run("concurrent first pushes", func(t *testing.T) {
		url := fmt.Sprintf("%s/created/race-%d", strings.TrimRight(endpoint, "/"), time.Now().UnixNano())
		type candidate struct {
			format, tip string
			body        []byte
		}
		var pushes []candidate
		for _, format := range []string{"sha1", "sha256"} {
			root := t.TempDir()
			git(t, nil, "init", "--object-format="+format, "-b", "main", root)
			write(t, filepath.Join(root, "README.md"), []byte(format))
			git(t, nil, "-C", root, "add", ".")
			git(t, nil, "-C", root, "commit", "-m", "first push")
			tip := strings.TrimSpace(string(git(t, nil, "-C", root, "rev-parse", "HEAD")))
			body := append(packet(strings.Repeat("0", len(tip))+" "+tip+" refs/heads/main\x00report-status object-format="+format+"\n"), []byte("0000")...)
			body = append(body, git(t, []byte(tip+"\n"), "-C", root, "pack-objects", "--revs", "--stdout")...)
			pushes = append(pushes, candidate{format, tip, body})
		}
		type result struct {
			candidate
			status int
			body   []byte
			err    error
		}
		results := make(chan result, 2)
		start := make(chan struct{})
		for _, push := range pushes {
			go func() {
				r := result{candidate: push}
				request, err := http.NewRequest(http.MethodPost, url+"/git-receive-pack", bytes.NewReader(push.body))
				if err == nil {
					request.SetBasicAuth("git", os.Getenv("GIT_PROBE_PASSWORD"))
					request.Header.Set("Content-Type", "application/x-git-receive-pack-request")
					<-start
					var response *http.Response
					response, err = http.DefaultClient.Do(request)
					if err == nil {
						r.status = response.StatusCode
						r.body, err = io.ReadAll(response.Body)
						response.Body.Close()
					}
				}
				r.err = err
				results <- r
			}()
		}
		close(start)
		winner := ""
		for range 2 {
			r := <-results
			if r.err != nil {
				t.Fatal(r.err)
			}
			if r.status == 200 && bytes.Contains(r.body, []byte("ok refs/heads/main")) {
				if winner != "" {
					t.Fatal("two incompatible first pushes committed")
				}
				winner = r.tip
			} else if r.status != 400 && !bytes.Contains(r.body, []byte("ng refs/heads/main")) {
				t.Fatalf("unexpected losing push: status=%d reply=%s", r.status, r.body)
			}
		}
		if winner == "" {
			t.Fatal("no first push committed")
		}
		clone := filepath.Join(t.TempDir(), "clone")
		git(t, nil, "clone", url, clone)
		git(t, nil, "-C", clone, "fsck", "--full")
		if tip := strings.TrimSpace(string(git(t, nil, "-C", clone, "rev-parse", "HEAD"))); tip != winner {
			t.Fatal("acknowledged first push was overwritten")
		}
	})
}
