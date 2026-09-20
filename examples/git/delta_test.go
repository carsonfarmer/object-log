package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os/exec"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	format "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	packutil "github.com/go-git/go-git/v6/plumbing/format/packfile/util"
)

type reuseStorage struct {
	*importStorage
	deltas map[plumbing.Hash]*deltaMeta
}

func (s reuseStorage) DeltaObject(kind plumbing.ObjectType, id plumbing.Hash) (plumbing.EncodedObject, error) {
	o, err := s.EncodedObject(kind, id)
	if err == nil && s.deltas[id] != nil {
		return storedDelta{EncodedObject: o, delta: s.deltas[id]}, nil
	}
	return o, err
}

func TestRetainedDeltaRoundtrip(t *testing.T) {
	for _, f := range []format.ObjectFormat{format.SHA1, format.SHA256} {
		t.Run(f.String(), func(t *testing.T) {
			base := make([]byte, 64<<10)
			_, _ = rand.New(rand.NewSource(42)).Read(base)
			target := bytes.Clone(base)
			copy(target[1234:], []byte("small edit"))
			objects := newImportStorage(f)
			for _, content := range [][]byte{base, target} {
				w, _ := objects.RawObjectWriter(plumbing.BlobObject, int64(len(content)))
				_, _ = w.Write(content)
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
			}
			baseID, targetID := blobID(f, base), blobID(f, target)
			baseObject, _ := objects.EncodedObject(plumbing.BlobObject, baseID)
			targetObject, _ := objects.EncodedObject(plumbing.BlobObject, targetID)
			delta, err := packfile.GetDelta(baseObject, targetObject)
			if err != nil {
				t.Fatal(err)
			}
			r, _ := delta.Reader()
			instructions, err := io.ReadAll(r)
			_ = r.Close()
			if err != nil {
				t.Fatal(err)
			}
			for _, mode := range []string{"ref", "ofs", "forward-ref", "thin"} {
				t.Run(mode, func(t *testing.T) {
					entries := []packFixtureEntry{{kind: plumbing.BlobObject, data: base}, {kind: plumbing.REFDeltaObject, data: instructions, ref: baseID}}
					s := reuseStorage{importStorage: newImportStorage(f)}
					switch mode {
					case "ofs":
						entries[1].kind = plumbing.OFSDeltaObject
					case "forward-ref":
						entries[0], entries[1] = entries[1], entries[0]
					case "thin":
						_, _ = s.SetEncodedObject(baseObject)
						entries = entries[1:]
					}
					packed := fixturePack(t, f, entries)
					source := bytes.NewReader(packed)
					offsets := packOffsets{}
					if err := importPack(context.Background(), source, s, f, testPackLimits(1<<20), offsets); err != nil {
						t.Fatal(err)
					}
					s.deltas = map[plumbing.Hash]*deltaMeta{}
					err = offsets.deltas(source, int64(len(packed)), func(id plumbing.Hash, delta *deltaMeta) { s.deltas[id] = delta })
					if err != nil || len(s.deltas) != 1 {
						t.Fatalf("retained %d deltas: %v", len(s.deltas), err)
					}
					// A cold catalog read must retain the representation, without any pack handle.
					meta := objectMeta{ID: targetID.String(), Kind: plumbing.BlobObject, Size: int64(len(target)), Delta: s.deltas[targetID]}
					encoded, _ := json.Marshal(meta)
					meta = objectMeta{}
					if err := json.Unmarshal(encoded, &meta); err != nil {
						t.Fatal(err)
					}
					s.deltas = map[plumbing.Hash]*deltaMeta{targetID: meta.Delta}
					for _, hashes := range [][]plumbing.Hash{{baseID, targetID}, {targetID}} {
						var output bytes.Buffer
						selected, err := packfile.NewDeltaSelector(s).ObjectsToPack(hashes, 1)
						if err != nil {
							t.Fatal(err)
						}
						count := 0
						for _, obj := range selected {
							if obj.IsDelta() {
								count++
							}
						}
						if count != len(hashes)-1 {
							t.Fatalf("%d deltas for %d objects", count, len(hashes))
						}
						if _, err := packfile.NewEncoder(&output, s, false).Encode(hashes, 1); err != nil {
							t.Fatal(err)
						}
						dir := t.TempDir()
						for _, args := range [][]string{{"init", "--bare", "--object-format=" + f.String(), dir}, {"-C", dir, "index-pack", "--strict", "--stdin"}} {
							command := exec.Command("git", args...)
							command.Stdin = bytes.NewReader(output.Bytes())
							if out, err := command.CombinedOutput(); err != nil {
								t.Fatalf("%v: %s: %v", args, out, err)
							}
						}
						out, err := exec.Command("git", "-C", dir, "cat-file", "blob", targetID.String()).Output()
						if err != nil || !bytes.Equal(out, target) {
							t.Fatalf("target content differs: %v", err)
						}
					}
				})
			}
		})
	}
}

func TestDeltaCatalogBound(t *testing.T) {
	for _, size := range []int{inlineObjectLimit, 4096, inlineDeltaLimit} {
		for _, mixInline := range []bool{false, true} {
			t.Run(fmt.Sprintf("bytes=%d/mixed=%t", size, mixInline), func(t *testing.T) {
				meta := objectMeta{ID: strings.Repeat("f", 64), Kind: plumbing.BlobObject, Size: math.MaxInt64, Encoding: "zlib", StoredSize: math.MaxInt64, Delta: &deltaMeta{Base: strings.Repeat("f", 64), Size: math.MaxInt64, Data: make([]byte, size)}}
				leaf := struct{ Items []objectMeta }{Items: make([]objectMeta, indexLeafSize)}
				for i := range leaf.Items {
					leaf.Items[i] = meta
					if mixInline && i%2 == 0 {
						leaf.Items[i].Delta = nil
						leaf.Items[i].Inline = make([]byte, inlineObjectLimit)
					}
				}
				limitDeltas(leaf.Items)
				total := 0
				for _, item := range leaf.Items {
					total += len(item.Inline)
					if item.Delta != nil {
						total += len(item.Delta.Data)
					}
				}
				if total > indexLeafSize*inlineObjectLimit {
					t.Fatal("exceeded shared inline allowance")
				}
				data, err := json.Marshal(leaf)
				if err != nil {
					t.Fatal(err)
				}
				if len(data) >= 1<<20 {
					t.Fatalf("delta leaf metadata grew to %d bytes", len(data))
				}
			})
		}
	}
}

func TestLargeDeltaRemainsAFullObject(t *testing.T) {
	f := format.SHA256
	target := make([]byte, 70<<10)
	_, _ = rand.New(rand.NewSource(42)).Read(target)
	delta := append([]byte{0}, packutil.EncodeLEB128(uint(len(target)))...)
	for pos := 0; pos < len(target); {
		n := min(127, len(target)-pos)
		delta = append(delta, byte(n))
		delta = append(delta, target[pos:pos+n]...)
		pos += n
	}
	packed := fixturePack(t, f, []packFixtureEntry{{kind: plumbing.BlobObject}, {kind: plumbing.REFDeltaObject, data: delta, ref: blobID(f, nil)}})
	source := bytes.NewReader(packed)
	s := newImportStorage(f)
	offsets := packOffsets{}
	if err := importPack(context.Background(), source, s, f, testPackLimits(1<<20), offsets); err != nil {
		t.Fatal(err)
	}
	if err := offsets.deltas(source, int64(len(packed)), func(plumbing.Hash, *deltaMeta) { t.Fatal("retained oversized representation") }); err != nil {
		t.Fatal(err)
	}
	object, err := s.EncodedObject(plumbing.BlobObject, blobID(f, target))
	if err != nil {
		t.Fatal(err)
	}
	r, err := object.Reader()
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || !bytes.Equal(got, target) {
		t.Fatalf("full object changed: %v", err)
	}
}
