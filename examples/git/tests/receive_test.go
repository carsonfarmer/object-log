package tests

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMalformedPackDoesNotPublish(t *testing.T) {
	endpoint := os.Getenv("GIT_PROBE_URL")
	if endpoint == "" {
		t.Skip("set GIT_PROBE_URL to a local Spin/MinIO instance")
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			source := t.TempDir()
			url := strings.TrimRight(endpoint, "/") + "/" + format + ".git"
			branch := fmt.Sprintf("malformed-%d", time.Now().UnixNano())
			git(t, nil, "init", "--object-format="+format, "-b", branch, source)
			write(t, filepath.Join(source, "file"), []byte("valid base"))
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "valid base")
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+branch)
			tip := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			for _, malformed := range []string{"short-object", "trailing-byte"} {
				t.Run(malformed, func(t *testing.T) {
					var pack bytes.Buffer
					pack.WriteString("PACK")
					_ = binary.Write(&pack, binary.BigEndian, uint32(2))
					_ = binary.Write(&pack, binary.BigEndian, uint32(1))
					header := byte(0x33) // Blob with three decoded bytes.
					if malformed == "short-object" {
						header = 0x34 // The writer must reject the short body on Close.
					}
					pack.WriteByte(header)
					z := zlib.NewWriter(&pack)
					_, _ = z.Write([]byte("abc"))
					if err := z.Close(); err != nil {
						t.Fatal(err)
					}
					if format == "sha256" {
						sum := sha256.Sum256(pack.Bytes())
						pack.Write(sum[:])
					} else {
						sum := sha1.Sum(pack.Bytes())
						pack.Write(sum[:])
					}
					if malformed == "trailing-byte" {
						pack.WriteByte(0)
					}
					first := "refs/heads/" + branch + "-" + malformed
					second := first + "-second"
					zero := strings.Repeat("0", len(tip))
					request := append(packet(zero+" "+tip+" "+first+"\x00report-status atomic object-format="+format+"\n"), packet(zero+" "+tip+" "+second+"\n")...)
					request = append(request, []byte("0000")...)
					result, _ := post(t, url+"/git-receive-pack", "git-receive-pack", append(request, pack.Bytes()...))
					if bytes.Contains(result, []byte("unpack ok")) || bytes.Contains(result, []byte("ok "+first)) || bytes.Contains(result, []byte("ok "+second)) {
						t.Fatalf("malformed pack acknowledged: %s", result)
					}
					if malformed == "short-object" && !bytes.Contains(result, []byte("incomplete object")) {
						t.Fatalf("expected stored writer failure: %s", result)
					}
					// Both tips already exist: connectivity cannot mask an unpack failure.
					if refs := git(t, nil, "ls-remote", url, first, second); len(refs) != 0 {
						t.Fatalf("malformed pack published refs: %s", refs)
					}
				})
			}
		})
	}
}

// Exercise a multi-chunk incoming pack without depending on the main proof's refs.
func TestManyObjects(t *testing.T) {
	endpoint := os.Getenv("GIT_PROBE_URL")
	if endpoint == "" {
		t.Skip("set GIT_PROBE_URL to a local Spin/MinIO instance")
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			url := strings.TrimRight(endpoint, "/") + "/" + format + ".git"
			branch := fmt.Sprintf("many-%d", time.Now().UnixNano())
			git(t, nil, "init", "--object-format="+format, "-b", branch, source)
			rng := rand.New(rand.NewSource(74))
			for i := range 32 {
				data := make([]byte, 64*1024)
				_, _ = rng.Read(data)
				write(t, filepath.Join(source, fmt.Sprintf("blob-%02d", i)), data)
			}
			// Mix inline objects with external chunks in the same catalog leaves.
			for i := range 512 {
				write(t, filepath.Join(source, fmt.Sprintf("small-%03d", i)), []byte(fmt.Sprintf("small object %d", i)))
			}
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "many objects")
			tip := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			pack := git(t, []byte(tip+"\n"), "-C", source, "pack-objects", "--stdout", "--revs")
			if len(pack) <= 2*1024*1024 {
				t.Fatalf("fixture must cross two chunk boundaries: %d", len(pack))
			}
			// A damaged final checksum must leave the entire publication invisible.
			badRef := "refs/heads/" + branch + "-bad"
			command := strings.Repeat("0", len(tip)) + " " + tip + " " + badRef + "\x00report-status atomic object-format=" + format + "\n"
			request := append(packet(command), []byte("0000")...)
			request = append(request, pack[:len(pack)-1]...)
			result, _ := post(t, url+"/git-receive-pack", "git-receive-pack", request)
			if bytes.Contains(result, []byte("ok "+badRef)) {
				t.Fatalf("damaged pack accepted: %s", result)
			}
			if refs := git(t, nil, "ls-remote", url, badRef); len(refs) != 0 {
				t.Fatalf("damaged pack published a ref: %s", refs)
			}
			// A valid pack containing a malformed identity must also abort all refs.
			tree := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD^{tree}")))
			malformed := strings.TrimSpace(string(git(t, []byte("tree "+tree+"\n\nmissing identities\n"), "-C", source, "hash-object", "-t", "commit", "-w", "--stdin", "--literally")))
			objects := git(t, nil, "-C", source, "rev-list", "--objects", "--no-object-names", "HEAD")
			objects = append(objects, []byte(malformed+"\n")...)
			invalidPack := git(t, objects, "-C", source, "pack-objects", "--stdout")
			badRef += "-identity"
			goodRef := badRef + "-valid"
			zero := strings.Repeat("0", len(tip))
			request = append(packet(zero+" "+malformed+" "+badRef+"\x00report-status atomic object-format="+format+"\n"), packet(zero+" "+tip+" "+goodRef+"\n")...)
			request = append(request, []byte("0000")...)
			result, _ = post(t, url+"/git-receive-pack", "git-receive-pack", append(request, invalidPack...))
			if !bytes.Contains(result, []byte("invalid author header")) {
				t.Fatalf("malformed identity was not rejected: %s", result)
			}
			if refs := git(t, nil, "ls-remote", url, badRef, goodRef); len(refs) != 0 {
				t.Fatalf("malformed identity published atomic refs: %s", refs)
			}
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+branch)
			clone := filepath.Join(root, "clone")
			git(t, nil, "-c", "protocol.version=2", "clone", "--single-branch", "--branch", branch, url, clone)
			git(t, nil, "-C", clone, "fsck", "--full")
			if got := strings.TrimSpace(string(git(t, nil, "-C", clone, "rev-parse", "HEAD"))); got != tip {
				t.Fatal("mixed catalog changed the cloned tree")
			}
			for i := range 32 {
				name := fmt.Sprintf("blob-%02d", i)
				want, err := os.ReadFile(filepath.Join(source, name))
				if err != nil {
					t.Fatal(err)
				}
				got, err := os.ReadFile(filepath.Join(clone, name))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("%s differs: %v", name, err)
				}
			}
		})
	}
}
