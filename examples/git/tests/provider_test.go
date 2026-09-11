package tests

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v6/plumbing/format/pktline"
)

// GIT_PROBE_URL must point to an ordinary local Spin host with a fresh MinIO prefix.
func TestWALGit(t *testing.T) {
	endpoint := os.Getenv("GIT_PROBE_URL")
	if endpoint == "" {
		t.Skip("set GIT_PROBE_URL to an isolated local Spin/MinIO instance")
	}
	branch := os.Getenv("GIT_PROBE_BRANCH")
	if branch == "" {
		branch = "main"
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			url := strings.TrimRight(endpoint, "/") + "/" + format + ".git"
			git(t, nil, "init", "--object-format="+format, "-b", branch, source)
			rng := rand.New(rand.NewSource(42))
			changed := make([]byte, 256*1024)
			untouched := make([]byte, 1024*1024)
			_, _ = rng.Read(changed)
			_, _ = rng.Read(untouched)
			write(t, filepath.Join(source, "changing.bin"), changed)
			write(t, filepath.Join(source, "untouched.bin"), untouched)
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "initial")
			old := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			base := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD:changing.bin")))
			git(t, nil, "-C", source, "push", "--atomic", url, "HEAD:refs/heads/"+branch, "HEAD:refs/heads/other")
			advertisement, err := http.NewRequest(http.MethodGet, url+"/info/refs?service=git-receive-pack", nil)
			if err != nil {
				t.Fatal(err)
			}
			advertisement.SetBasicAuth("git", os.Getenv("GIT_PROBE_PASSWORD"))
			response, err := http.DefaultClient.Do(advertisement)
			if err != nil {
				t.Fatal(err)
			}
			scanner := pktline.NewScanner(response.Body)
			previous, count := "", 0
			for scanner.Scan() {
				line := scanner.Text()
				_, name, ok := strings.Cut(line, " ")
				if !ok || strings.HasPrefix(line, "#") {
					continue
				}
				name, _, _ = strings.Cut(name, "\x00")
				name = strings.TrimSpace(name)
				if name <= previous {
					t.Errorf("advertised refs out of order: %q after %q", name, previous)
				}
				previous, count = name, count+1
			}
			response.Body.Close()
			if scanner.Err() != nil || response.StatusCode != http.StatusOK || count < 2 {
				t.Fatalf("invalid ref advertisement: status=%d refs=%d error=%v", response.StatusCode, count, scanner.Err())
			}
			// Library decoding is permissive; malformed trees must fail before publication.
			blobID, err := hex.DecodeString(base)
			if err != nil {
				t.Fatal(err)
			}
			entry := append([]byte("100644 duplicate\x00"), blobID...)
			badTree := strings.TrimSpace(string(git(t, append(entry, entry...), "-C", source, "hash-object", "--literally", "-t", "tree", "-w", "--stdin")))
			badCommit := strings.TrimSpace(string(git(t, []byte("bad tree\n"), "-C", source, "commit-tree", badTree, "-p", old)))
			badRequest := append(packet(strings.Repeat("0", len(old))+" "+badCommit+" refs/heads/bad-tree\x00report-status atomic object-format="+format+"\n"), []byte("0000")...)
			badRequest = append(badRequest, git(t, []byte(badCommit+"\n"+badTree+"\n"), "-C", source, "pack-objects", "--stdout")...)
			badReply, _ := post(t, url+"/git-receive-pack", "git-receive-pack", badRequest)
			if !bytes.Contains(badReply, []byte("ng refs/heads/bad-tree")) {
				t.Fatalf("malformed tree accepted: %s", badReply)
			}
			if refs := git(t, nil, "ls-remote", url, "refs/heads/bad-tree"); len(refs) != 0 {
				t.Fatal("malformed tree published")
			}
			// An empty receive-pack preflight is valid and must receive an empty HTTP 200.
			post(t, url+"/git-receive-pack", "git-receive-pack", []byte("0000"))
			shallowReply, _ := post(t, url+"/git-receive-pack", "git-receive-pack", append(packet("shallow "+old+"\n"), []byte("0000")...))
			if len(shallowReply) != 0 {
				t.Fatal("shallow no-op returned body")
			}
			// foo-bar sorts between a parent and child; adjacency-only collision checks miss this.
			var conflict []byte
			zero := strings.Repeat("0", len(old))
			for i, name := range []string{"refs/heads/foo", "refs/heads/foo-bar", "refs/heads/foo/bar"} {
				line := zero + " " + old + " " + name
				if i == 0 {
					line += "\x00report-status atomic object-format=" + format
				}
				conflict = append(conflict, packet(line+"\n")...)
			}
			conflict = append(conflict, []byte("0000")...)
			conflict = append(conflict, git(t, nil, "-C", source, "pack-objects", "--stdout", "--revs")...)
			rejected, _ := post(t, url+"/git-receive-pack", "git-receive-pack", conflict)
			if bytes.Contains(rejected, []byte("ok refs/heads/foo")) || !bytes.Contains(rejected, []byte("ref prefix collision")) {
				t.Fatalf("prefix batch accepted: %s", rejected)
			}
			clone := filepath.Join(root, "clone")
			git(t, nil, "-c", "protocol.version=2", "clone", url, clone)
			git(t, nil, "-C", clone, "fsck", "--full")
			if got := strings.TrimSpace(string(git(t, nil, "-C", clone, "symbolic-ref", "--short", "HEAD"))); got != branch {
				t.Fatalf("default branch %s, want %s", got, branch)
			}
			changed[len(changed)/2] ^= 1
			write(t, filepath.Join(source, "changing.bin"), changed)
			git(t, nil, "-C", source, "commit", "-am", "thin update")
			next := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			pack := git(t, []byte(next+"\n^"+old+"\n"), "-C", source, "pack-objects", "--thin", "--stdout", "--revs")
			assertThinBase(t, pack, base)
			command := old + " " + next + " refs/heads/" + branch + "\x00report-status atomic object-format=" + format + "\n"
			request := append(packet(command), []byte("0000")...)
			request = append(request, pack...)
			result, _ := post(t, url+"/git-receive-pack", "git-receive-pack", request)
			if !bytes.Contains(result, []byte("ok refs/heads/"+branch)) {
				t.Fatalf("thin push rejected: %s", result)
			}
			git(t, nil, "-C", clone, "-c", "protocol.version=2", "fetch", "origin")
			git(t, nil, "-C", clone, "fsck", "--full")
			if got := strings.TrimSpace(string(git(t, nil, "-C", clone, "rev-parse", "origin/"+branch))); got != next {
				t.Fatalf("fetch got %s want %s", got, next)
			}
			// Also exercise an ordinary Git update after the exact thin-pack protocol case.
			changed[0] ^= 1
			write(t, filepath.Join(source, "changing.bin"), changed)
			git(t, nil, "-C", source, "commit", "-am", "client update")
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+branch)
			blob := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD:changing.bin")))
			// Spin opens a fresh WAL view per request. Fetching one reachable blob must not read untouched.bin.
			body := append(packet("command=fetch\n"), packet("object-format="+format+"\n")...)
			body = append(body, []byte("0001")...)
			body = append(body, packet("want "+blob+"\n")...)
			body = append(body, packet("done\n")...)
			body = append(body, []byte("0000")...)
			result, trailer := post(t, url+"/git-upload-pack", "git-upload-pack", body)
			target := filepath.Join(root, "sparse")
			git(t, nil, "init", "--object-format="+format, target)
			git(t, unband(t, result), "-C", target, "index-pack", "--stdin")
			if got := git(t, nil, "-C", target, "cat-file", "blob", blob); !bytes.Equal(got, changed) {
				t.Fatal("sparse blob differs")
			}
			transferred, e := strconv.ParseUint(trailer.Get("X-Wal-Bytes"), 10, 64)
			if e != nil {
				t.Fatalf("missing WAL byte counter: %v %v", trailer, e)
			}
			calls, e := strconv.ParseUint(trailer.Get("X-Wal-Calls"), 10, 64)
			if e != nil || calls == 0 {
				t.Fatalf("missing WAL call counter: %v", trailer)
			}
			if transferred >= 768*1024 {
				t.Fatalf("selected 256 KiB blob required %d bytes; unrelated 1 MiB blob should remain unread", transferred)
			}
			// Ordinary tags and branch deletion must work without custom clients.
			git(t, nil, "-C", source, "tag", "lightweight")
			git(t, nil, "-C", source, "tag", "-a", "annotated", "-m", "release")
			git(t, nil, "-C", source, "push", url, "--tags")
			git(t, nil, "-C", clone, "fetch", "--tags", "origin")
			git(t, nil, "-C", clone, "fsck", "--strict")
			git(t, nil, "-C", source, "push", url, ":refs/heads/other", ":refs/tags/lightweight")
			refs := git(t, nil, "ls-remote", url)
			if bytes.Contains(refs, []byte("refs/heads/other")) || bytes.Contains(refs, []byte("refs/tags/lightweight")) {
				t.Fatal("deleted refs remain visible")
			}
			if !bytes.Contains(refs, []byte("refs/tags/annotated^{}")) {
				t.Fatal("annotated tag missing peeled target")
			}
			// Clients may omit report-status; the update must still publish.
			tip := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			silent := append(packet(zero+" "+tip+" refs/heads/silent\x00object-format="+format+"\n"), []byte("0000")...)
			silent = append(silent, git(t, nil, "-C", source, "pack-objects", "--stdout", "--revs")...)
			reply, _ := post(t, url+"/git-receive-pack", "git-receive-pack", silent)
			if len(reply) != 0 {
				t.Fatalf("unrequested status report: %q", reply)
			}
			if refs := git(t, nil, "ls-remote", url, "refs/heads/silent"); !bytes.Contains(refs, []byte(tip)) {
				t.Fatal("silent push did not publish")
			}
			racePush(t, source, url, format, branch, tip)
			t.Logf("thin pack=%d bytes, sparse fetch=%d provider bytes/%d calls", len(pack), transferred, calls)
		})
	}
}
func git(t *testing.T, input []byte, args ...string) []byte {
	t.Helper()
	return gitWithEnv(t, input, nil, args...)
}
func gitWithEnv(t *testing.T, input []byte, extraEnv []string, args ...string) []byte {
	t.Helper()
	if password := os.Getenv("GIT_PROBE_PASSWORD"); password != "" {
		args = append([]string{"-c", "http.extraHeader=Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte("git:"+password))}, args...)
	}
	cmd := exec.Command("git", args...)
	cmd.Stdin = bytes.NewReader(input)
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=Probe", "GIT_AUTHOR_EMAIL=probe@example.invalid", "GIT_COMMITTER_NAME=Probe", "GIT_COMMITTER_EMAIL=probe@example.invalid")
	cmd.Env = append(cmd.Env, extraEnv...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, e := cmd.Output()
	if e != nil {
		t.Fatalf("git %v: %v\n%s", args, e, stderr.Bytes())
	}
	return out
}
func write(t *testing.T, path string, b []byte) {
	t.Helper()
	if e := os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
}
func packet(line string) []byte { return []byte(fmt.Sprintf("%04x%s", len(line)+4, line)) }
func post(t *testing.T, url, service string, body []byte) ([]byte, http.Header) {
	t.Helper()
	logPath := os.Getenv("GIT_PROBE_LOG")
	var offset int64
	if logPath != "" {
		info, err := os.Stat(logPath)
		if err != nil {
			t.Fatal(err)
		}
		offset = info.Size()
	}
	r, e := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if e != nil {
		t.Fatal(e)
	}
	if password := os.Getenv("GIT_PROBE_PASSWORD"); password != "" {
		r.SetBasicAuth("git", password)
	}
	r.Header.Set("Content-Type", "application/x-"+service+"-request")
	r.Header.Set("Git-Protocol", "version=2")
	response, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	data, e := io.ReadAll(response.Body)
	if e != nil {
		t.Fatal(e)
	}
	if response.StatusCode != 200 {
		t.Fatalf("HTTP %d: %s", response.StatusCode, data)
	}
	trailers := response.Trailer
	if trailers.Get("X-Wal-Bytes") == "" && logPath != "" {
		// Some Spin versions discard outgoing HTTP trailers; use the same actual transport counters from this request's log line.
		prefix := "wal POST " + r.URL.Path + " calls="
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			contents, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			if int64(len(contents)) < offset {
				t.Fatal("component log rotated during test")
			}
			for _, line := range strings.Split(string(contents[offset:]), "\n") {
				if i := strings.Index(line, prefix); i >= 0 {
					var calls, count uint64
					if n, _ := fmt.Sscanf(line[i+len(prefix):], "%d bytes=%d", &calls, &count); n == 2 {
						if trailers == nil {
							trailers = make(http.Header)
						}
						trailers.Set("X-Wal-Calls", fmt.Sprint(calls))
						trailers.Set("X-Wal-Bytes", fmt.Sprint(count))
						return data, trailers
					}
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return data, trailers
}
func assertThinBase(t *testing.T, pack []byte, base string) {
	t.Helper()
	if len(pack) < 12 || string(pack[:4]) != "PACK" {
		t.Fatal("not a Git pack")
	}
	r := bytes.NewReader(pack[12:])
	found := false
	hashLen := len(base) / 2
	for count := binary.BigEndian.Uint32(pack[8:12]); count > 0; count-- {
		b, e := r.ReadByte()
		if e != nil {
			t.Fatal(e)
		}
		kind := (b >> 4) & 7
		for b&128 != 0 {
			b, e = r.ReadByte()
			if e != nil {
				t.Fatal(e)
			}
		}
		if kind == 7 {
			ref := make([]byte, hashLen)
			if _, e = io.ReadFull(r, ref); e != nil {
				t.Fatal(e)
			}
			found = found || hex.EncodeToString(ref) == base
		} else if kind == 6 {
			b, e = r.ReadByte()
			for e == nil && b&128 != 0 {
				b, e = r.ReadByte()
			}
			if e != nil {
				t.Fatal(e)
			}
		}
		z, e := zlib.NewReader(r)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = io.Copy(io.Discard, z); e != nil {
			t.Fatal(e)
		}
		if e = z.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if !found {
		t.Fatal("fixture has no REF_DELTA against the stored base")
	}
}
func unband(t *testing.T, body []byte) []byte {
	t.Helper()
	var result []byte
	inPack := false
	for len(body) > 0 {
		if len(body) < 4 {
			t.Fatal("truncated packet")
		}
		n, e := strconv.ParseUint(string(body[:4]), 16, 16)
		if e != nil {
			t.Fatal(e)
		}
		body = body[4:]
		if n <= 2 {
			continue
		}
		if n < 4 || int(n)-4 > len(body) {
			t.Fatal("invalid packet")
		}
		line := body[:int(n)-4]
		body = body[int(n)-4:]
		if string(line) == "packfile\n" {
			inPack = true
			continue
		}
		if inPack {
			if len(line) == 0 || line[0] != 1 {
				t.Fatalf("unexpected sideband %q", line)
			}
			result = append(result, line[1:]...)
		}
	}
	if !inPack {
		t.Fatalf("no packfile: %s", body)
	}
	return result
}
