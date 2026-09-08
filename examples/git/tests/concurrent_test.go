package tests

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
)

func racePush(t *testing.T, source, url, format, branch, old string) {
	t.Helper()
	tree := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD^{tree}")))
	type result struct {
		index int
		body  []byte
		err   error
	}
	replies := make(chan result, 2)
	start := make(chan struct{})
	tips := make([]string, 2)
	for i := range tips {
		tips[i] = strings.TrimSpace(string(git(t, []byte(fmt.Sprintf("racer %d\n", i)), "-C", source, "commit-tree", tree, "-p", old)))
		first := old + " " + tips[i] + " refs/heads/" + branch + "\x00report-status atomic object-format=" + format + "\n"
		body := append(packet(first), packet(strings.Repeat("0", len(old))+" "+tips[i]+fmt.Sprintf(" refs/heads/racer-%d\n", i))...)
		body = append(body, []byte("0000")...)
		body = append(body, git(t, []byte(tips[i]+"\n^"+old+"\n"), "-C", source, "pack-objects", "--stdout", "--revs")...)
		go func(i int, body []byte) {
			<-start
			r, err := http.NewRequest("POST", url+"/git-receive-pack", bytes.NewReader(body))
			if err != nil {
				replies <- result{i, nil, err}
				return
			}
			r.Header.Set("Content-Type", "application/x-git-receive-pack-request")
			if password := os.Getenv("GIT_PROBE_PASSWORD"); password != "" {
				r.SetBasicAuth("git", password)
			}
			response, err := http.DefaultClient.Do(r)
			if err != nil {
				replies <- result{i, nil, err}
				return
			}
			data, err := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != 200 {
				err = fmt.Errorf("HTTP %d: %s", response.StatusCode, data)
			}
			replies <- result{i, data, err}
		}(i, body)
	}
	close(start)
	winner := -1
	for range tips {
		r := <-replies
		if r.err != nil {
			t.Fatal(r.err)
		}
		if bytes.Contains(r.body, []byte("ok refs/heads/"+branch+"\n")) {
			if winner != -1 {
				t.Fatal("both conflicting pushes succeeded")
			}
			winner = r.index
		}
	}
	if winner == -1 {
		t.Fatal("neither conflicting push succeeded")
	}
	refs := string(git(t, nil, "ls-remote", url))
	if !strings.Contains(refs, tips[winner]+"\trefs/heads/"+branch) || !strings.Contains(refs, fmt.Sprintf("refs/heads/racer-%d", winner)) || strings.Contains(refs, fmt.Sprintf("refs/heads/racer-%d", 1-winner)) {
		t.Fatalf("partial or incorrect atomic update: %s", refs)
	}
}
