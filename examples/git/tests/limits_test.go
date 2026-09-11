package tests

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Run against a fresh host with push=128KiB, negotiation=4KiB, object=64KiB.
func TestConfiguredLimits(t *testing.T) {
	if os.Getenv("GIT_PROBE_LIMITS") != "1" {
		t.Skip("set GIT_PROBE_LIMITS=1 against the documented small-limit host")
	}
	endpoint := strings.TrimRight(os.Getenv("GIT_PROBE_URL"), "/")
	if endpoint == "" {
		t.Fatal("GIT_PROBE_URL is required")
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			url := endpoint + "/" + format + ".git"
			for _, chunked := range []bool{false, true} {
				body := append(packet("command=fetch\n"), []byte("0001")...)
				body = append(body, bytes.Repeat(packet("have "+strings.Repeat("1", 40)+"\n"), 200)...)
				body = append(body, []byte("0000")...)
				var compressed bytes.Buffer
				zipped := gzip.NewWriter(&compressed)
				if _, err := zipped.Write(body); err != nil {
					t.Fatal(err)
				}
				if err := zipped.Close(); err != nil {
					t.Fatal(err)
				}
				req, err := http.NewRequest(http.MethodPost, url+"/git-upload-pack", &compressed)
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Git-Protocol", "version=2")
				req.Header.Set("Content-Encoding", "gzip")
				req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
				if chunked {
					req.ContentLength = -1
				}
				response, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || response.StatusCode != http.StatusRequestEntityTooLarge {
					t.Fatalf("expanded negotiation: status=%d body=%s err=%v", response.StatusCode, data, err)
				}
			}
			width := 40
			if format == "sha256" {
				width = 64
			}
			var commands bytes.Buffer
			for i := range 100 {
				caps := ""
				if i == 0 {
					caps = "\x00report-status object-format=" + format
				}
				commands.Write(packet(fmt.Sprintf("%s %s refs/heads/limit-%d%s\n", strings.Repeat("1", width), strings.Repeat("0", width), i, caps)))
			}
			commands.WriteString("0000")
			req, err := http.NewRequest(http.MethodPost, url+"/git-receive-pack", &commands)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/x-git-receive-pack-request")
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			data, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr != nil || response.StatusCode != http.StatusRequestEntityTooLarge {
				t.Fatalf("push negotiation: status=%d body=%s err=%v", response.StatusCode, data, readErr)
			}
			source := filepath.Join(t.TempDir(), "source")
			git(t, nil, "init", "--object-format="+format, "-b", "limits", source)
			write(t, filepath.Join(source, "large"), bytes.Repeat([]byte("x"), 100*1024))
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "oversized decoded object")
			tip := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			pack := git(t, []byte(tip+"\n"), "-C", source, "pack-objects", "--stdout", "--revs")
			if len(pack) >= 128*1024 {
				t.Fatal("fixture must fit wire limit")
			}
			command := strings.Repeat("0", len(tip)) + " " + tip + " refs/heads/limits\x00report-status atomic object-format=" + format + "\n"
			body := append(packet(command), []byte("0000")...)
			body = append(body, pack...)
			reply, _ := post(t, url+"/git-receive-pack", "git-receive-pack", body)
			if !bytes.Contains(reply, []byte("GIT_MAX_OBJECT_BYTES")) {
				t.Fatalf("missing object-limit rejection: %s", reply)
			}
			if refs := git(t, nil, "ls-remote", url, "refs/heads/limits"); len(refs) != 0 {
				t.Fatalf("rejected object published: %s", refs)
			}
		})
	}
}
