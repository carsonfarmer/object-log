package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Run prepare, restart ordinary Spin with the same WAL prefix, then run verify.
// The state file contains independent Git expectations, never service state.
func TestFailureDrills(t *testing.T) {
	endpoint, mode, statePath := os.Getenv("GIT_PROBE_URL"), os.Getenv("GIT_FAILURE_DRILLS"), os.Getenv("GIT_DRILL_STATE")
	if endpoint == "" || mode == "" || statePath == "" {
		t.Skip("set GIT_PROBE_URL, GIT_FAILURE_DRILLS=prepare|verify, and GIT_DRILL_STATE")
	}
	if mode == "verify" {
		data, err := os.ReadFile(statePath)
		if err != nil {
			t.Fatal(err)
		}
		var repos []drillRepository
		if err = json.Unmarshal(data, &repos); err != nil {
			t.Fatal(err)
		}
		if len(repos) != 2 {
			t.Fatalf("expected both hash formats, got %d", len(repos))
		}
		for _, repo := range repos {
			if repo.URL != strings.TrimRight(endpoint, "/")+"/"+repo.Format+".git" {
				t.Fatal("state file belongs to a different endpoint")
			}
			t.Run(repo.Format, func(t *testing.T) { verifyDrill(t, repo); collectDrill(t, repo.URL); verifyDrill(t, repo) })
		}
		return
	}
	if mode != "prepare" {
		t.Fatal("GIT_FAILURE_DRILLS must be prepare or verify")
	}
	var repos []drillRepository
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			repo := drillRepository{Format: format, URL: strings.TrimRight(endpoint, "/") + "/" + format + ".git", Refs: map[string]string{}, Blobs: map[string]string{}}
			source := filepath.Join(t.TempDir(), "source")
			prefix := fmt.Sprintf("drill-%d", time.Now().UnixNano())
			git(t, nil, "init", "--object-format="+format, "-b", prefix, source)
			write(t, filepath.Join(source, "value"), []byte("accepted base"))
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "base")
			base := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			git(t, nil, "-C", source, "push", repo.URL, "HEAD:refs/heads/"+prefix)
			repo.Refs["refs/heads/"+prefix] = base
			type update struct {
				ref, tip, blob, value string
				body                  []byte
			}
			makeUpdate := func(name string) update {
				value := "payload " + name
				blob := strings.TrimSpace(string(git(t, []byte(value), "-C", source, "hash-object", "-w", "--stdin")))
				tree := strings.TrimSpace(string(git(t, []byte("100644 blob "+blob+"\tvalue\n"), "-C", source, "mktree")))
				tip := strings.TrimSpace(string(git(t, []byte(name+"\n"), "-C", source, "commit-tree", tree, "-p", base)))
				ref := "refs/heads/" + name
				zero := strings.Repeat("0", len(tip))
				body := append(packet(zero+" "+tip+" "+ref+"\x00report-status atomic object-format="+format+"\n"), packet(zero+" "+tip+" "+ref+"-mirror\n")...)
				body = append(body, []byte("0000")...)
				body = append(body, git(t, []byte(tip+"\n^"+base+"\n"), "-C", source, "pack-objects", "--stdout", "--revs", "--window=0")...)
				return update{ref, tip, blob, value, body}
			}
			var batches [2][]update
			for writer := range batches {
				for round := 0; round < 16; round++ {
					batches[writer] = append(batches[writer], makeUpdate(fmt.Sprintf("%s-w%d-%d", prefix, writer, round)))
				}
			}
			client := filepath.Join(t.TempDir(), "reader")
			git(t, nil, "init", "--bare", "--object-format="+format, client)
			git(t, nil, "-C", client, "config", "maintenance.autoDetach", "false")
			var mu sync.Mutex
			accepted, conflicts := 0, 0
			var active [4]bool
			var overlap [3]bool
			markActive := func(worker int, running bool) {
				mu.Lock()
				defer mu.Unlock()
				active[worker] = running
				if active[3] {
					for other := range overlap {
						overlap[other] = overlap[other] || active[other]
					}
				}
			}
			t.Run("concurrent", func(t *testing.T) {
				for writer := range batches {
					t.Run(fmt.Sprintf("writer-%d", writer), func(t *testing.T) {
						t.Parallel()
						for _, u := range batches[writer] {
							markActive(writer, true)
							response := drillRequest(t, context.Background(), repo.URL+"/git-receive-pack", u.body)
							data, err := io.ReadAll(response.Body)
							response.Body.Close()
							markActive(writer, false)
							if err != nil {
								t.Fatal(err)
							}
							if response.StatusCode != 200 {
								t.Fatalf("HTTP %d: %s", response.StatusCode, data)
							}
							ok1, ok2 := bytes.Contains(data, []byte("ok "+u.ref+"\n")), bytes.Contains(data, []byte("ok "+u.ref+"-mirror\n"))
							if ok1 != ok2 {
								t.Fatalf("partial atomic response: %s", data)
							}
							mu.Lock()
							if ok1 {
								accepted++
								repo.Refs[u.ref] = u.tip
								repo.Refs[u.ref+"-mirror"] = u.tip
								repo.Blobs[u.blob] = u.value
							} else {
								conflicts++
								repo.Absent = append(repo.Absent, u.ref, u.ref+"-mirror")
							}
							mu.Unlock()
							if !ok1 && (!bytes.Contains(data, []byte("ng "+u.ref+" publication conflict or expired view\n")) || !bytes.Contains(data, []byte("ng "+u.ref+"-mirror publication conflict or expired view\n"))) {
								t.Errorf("unexpected rejection: %s", data)
							}
						}
					})
				}
				t.Run("reader", func(t *testing.T) {
					t.Parallel()
					for round := 0; round < 8; round++ {
						markActive(2, true)
						git(t, nil, "-C", client, "fetch", repo.URL, "+refs/heads/"+prefix+"*:refs/heads/"+prefix+"*")
						markActive(2, false)
						git(t, nil, "-C", client, "fsck", "--full")
					}
				})
				t.Run("collector", func(t *testing.T) {
					t.Parallel()
					for round := 0; round < 8; round++ {
						markActive(3, true)
						collectDrillStep(t, repo.URL)
						markActive(3, false)
					}
				})
			})
			for worker, name := range []string{"writer-0", "writer-1", "reader"} {
				if !overlap[worker] {
					t.Errorf("collector did not overlap %s", name)
				}
			}
			// Collection is a third publisher and can legitimately fence either
			// writer; every accepted or rejected update is still checked exactly.
			if accepted == 0 {
				t.Error("no concurrent publication was acknowledged")
			}
			t.Logf("accepted %d atomic updates, rejected %d conflicts; reader completed 8 fetch/fsck cycles and collector completed 8 maintenance calls", accepted, conflicts)
			after := makeUpdate(prefix + "-after-contention")
			git(t, nil, "-C", source, "push", "--atomic", repo.URL, after.tip+":"+after.ref, after.tip+":"+after.ref+"-mirror")
			repo.Refs[after.ref], repo.Refs[after.ref+"-mirror"] = after.tip, after.tip
			repo.Blobs[after.blob] = after.value
			canceled := makeUpdate(prefix + "-canceled")
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, repo.URL+"/git-receive-pack", bytes.NewReader(canceled.body))
			if err != nil {
				t.Fatal(err)
			}
			if response, err := http.DefaultClient.Do(request); err == nil {
				response.Body.Close()
				t.Fatal("pre-canceled request was sent")
			}
			repo.Absent = append(repo.Absent, canceled.ref, canceled.ref+"-mirror")
			interrupted := makeUpdate(prefix + "-interrupted")
			ctx, cancel = context.WithCancel(context.Background())
			reader, writer := io.Pipe()
			request, err = http.NewRequestWithContext(ctx, http.MethodPost, repo.URL+"/git-receive-pack", reader)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/x-git-receive-pack-request")
			if password := os.Getenv("GIT_PROBE_PASSWORD"); password != "" {
				request.SetBasicAuth("git", password)
			}
			finished := make(chan error, 1)
			go func() {
				response, err := (&http.Client{Timeout: 5 * time.Minute}).Do(request)
				if response != nil {
					response.Body.Close()
				}
				finished <- err
			}()
			// Write returns after the transport starts consuming the body. It
			// does not claim to identify the server's admission boundary.
			_, writeErr := writer.Write(interrupted.body[:len(interrupted.body)/2])
			cancel()
			writer.CloseWithError(context.Canceled)
			requestErr := <-finished
			reader.Close()
			if writeErr != nil || requestErr == nil {
				t.Fatalf("mid-upload cancellation: write=%v request=%v", writeErr, requestErr)
			}
			repo.Absent = append(repo.Absent, interrupted.ref, interrupted.ref+"-mirror")
			truncated := makeUpdate(prefix + "-truncated")
			response := drillRequest(t, context.Background(), repo.URL+"/git-receive-pack", truncated.body[:len(truncated.body)-8])
			data, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(data, []byte("ok "+truncated.ref+"\n")) {
				t.Fatalf("truncated pack accepted: %s", data)
			}
			repo.Absent = append(repo.Absent, truncated.ref, truncated.ref+"-mirror")
			lost := makeUpdate(prefix + "-lost-response")
			response = drillRequest(t, context.Background(), repo.URL+"/git-receive-pack", lost.body)
			// Deliberately discard receive-pack's status report. A fresh ref read, not
			// a replayed push, resolves whether the publication became visible.
			response.Body.Close()
			repo.Refs[lost.ref], repo.Refs[lost.ref+"-mirror"] = lost.tip, lost.tip
			repo.Blobs[lost.blob] = lost.value
			verifyDrill(t, repo)
			collectDrill(t, repo.URL)
			verifyDrill(t, repo)
			repos = append(repos, repo)
		})
	}
	if t.Failed() {
		return
	}
	data, err := json.MarshalIndent(repos, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(statePath, data, 0600); err != nil {
		t.Fatal(err)
	}
}

type drillRepository struct {
	Format, URL string
	Refs, Blobs map[string]string
	Absent      []string
}

func drillRequest(t *testing.T, ctx context.Context, url string, body []byte) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-git-receive-pack-request")
	if password := os.Getenv("GIT_PROBE_PASSWORD"); password != "" {
		request.SetBasicAuth("git", password)
	}
	response, err := (&http.Client{Timeout: 5 * time.Minute}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func collectDrill(t *testing.T, url string) {
	t.Helper()
	for attempt := 0; attempt < 32; attempt++ {
		if collectDrillStep(t, url) == "complete" {
			return
		}
	}
	t.Fatal("maintenance did not complete in 32 requests")
}

// A concurrent collector performs one bounded step; conflicts and pending fences
// are valid outcomes. After writers stop, collectDrill still requires completion.
func collectDrillStep(t *testing.T, url string) string {
	t.Helper()
	response := drillRequest(t, context.Background(), url+"/maintenance", nil)
	var result struct {
		State string `json:"state"`
	}
	err := json.NewDecoder(response.Body).Decode(&result)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("maintenance HTTP %d: %v", response.StatusCode, err)
	}
	switch result.State {
	case "complete", "more", "conflict", "pending":
	default:
		t.Fatalf("unexpected maintenance state %q", result.State)
	}
	return result.State
}

func verifyDrill(t *testing.T, repo drillRepository) {
	t.Helper()
	cold := filepath.Join(t.TempDir(), "cold")
	git(t, nil, "clone", "--mirror", repo.URL, cold)
	git(t, nil, "-C", cold, "fsck", "--full")
	refs := string(git(t, nil, "-C", cold, "for-each-ref", "--format=%(objectname) %(refname)"))
	for ref, id := range repo.Refs {
		if !strings.Contains(refs, id+" "+ref+"\n") {
			t.Fatalf("lost acknowledged ref %s -> %s", ref, id)
		}
	}
	for _, ref := range repo.Absent {
		for _, line := range strings.Split(refs, "\n") {
			if strings.HasSuffix(line, " "+ref) {
				t.Fatalf("rejected update published %s", ref)
			}
		}
	}
	for id, value := range repo.Blobs {
		if got := git(t, nil, "-C", cold, "cat-file", "blob", id); string(got) != value {
			t.Fatalf("accepted object %s content changed", id)
		}
	}
}
