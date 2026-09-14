package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"
)

type trackingObjectStore struct {
	storage.Storer
	mu         sync.Mutex
	readers    map[plumbing.Hash]int
	readerErrs map[plumbing.Hash]error
	deltaCalls int
}

func (s *trackingObjectStore) EncodedObject(kind plumbing.ObjectType, hash plumbing.Hash) (plumbing.EncodedObject, error) {
	object, err := s.Storer.EncodedObject(kind, hash)
	if err != nil {
		return nil, err
	}
	return &trackingObject{EncodedObject: object, store: s}, nil
}

func (s *trackingObjectStore) DeltaObject(plumbing.ObjectType, plumbing.Hash) (plumbing.EncodedObject, error) {
	s.mu.Lock()
	s.deltaCalls++
	s.mu.Unlock()
	return nil, errors.New("unexpected delta object lookup")
}

func (s *trackingObjectStore) readerCount(hash plumbing.Hash) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readers[hash]
}

type trackingObject struct {
	plumbing.EncodedObject
	store *trackingObjectStore
}

func (o *trackingObject) Reader() (io.ReadCloser, error) {
	o.store.mu.Lock()
	o.store.readers[o.Hash()]++
	err := o.store.readerErrs[o.Hash()]
	o.store.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return o.EncodedObject.Reader()
}

type fixedObjects []*packfile.ObjectToPack

func (s fixedObjects) ObjectsToPack([]plumbing.Hash, uint) ([]*packfile.ObjectToPack, error) {
	return s, nil
}

func putOutgoingObject(t testing.TB, s storage.Storer, kind plumbing.ObjectType, data []byte) plumbing.Hash {
	t.Helper()
	object := s.NewEncodedObject()
	object.SetType(kind)
	w, err := object.Writer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	hash, err := s.SetEncodedObject(object)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func outgoingTestLimits(t testing.TB) requestLimits {
	t.Helper()
	limits, err := loadLimits(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	return limits
}

func TestOutgoingObjectSelectorGitDecode(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("Git client required")
	}
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		t.Run(format.String(), func(t *testing.T) {
			mem := memory.NewStorage(memory.WithObjectFormat(format))
			base := bytes.Repeat([]byte("a"), 256<<10)
			target := bytes.Clone(base)
			copy(target[4096:8192], bytes.Repeat([]byte("b"), 4096))
			large := bytes.Repeat([]byte("large"), outgoingDeltaObjectBytes/5+1)
			contents := [][]byte{base, target, large}
			hashes := make([]plumbing.Hash, 0, len(contents))
			for _, content := range contents {
				hashes = append(hashes, putOutgoingObject(t, mem, plumbing.BlobObject, content))
			}

			tracked := &trackingObjectStore{Storer: mem, readers: map[plumbing.Hash]int{}}
			selected, err := outgoingObjectSelectorFactory(outgoingTestLimits(t))(tracked).ObjectsToPack(hashes, 99)
			if err != nil {
				t.Fatal(err)
			}
			if tracked.deltaCalls != 0 {
				t.Fatalf("used backing DeltaObjectStorer %d times", tracked.deltaCalls)
			}
			if got := tracked.readerCount(hashes[2]); got != 0 {
				t.Fatalf("opened large object %d times during selection", got)
			}
			deltas, largeIsFull := 0, false
			for _, object := range selected {
				if object.IsDelta() {
					deltas++
				}
				if object.Hash() == hashes[2] {
					largeIsFull = !object.IsDelta() && object.Object == object.Original
				}
			}
			if deltas == 0 {
				t.Fatal("related small objects did not produce a delta")
			}
			if !largeIsFull {
				t.Fatal("large object did not fall back to a full object")
			}

			var pack bytes.Buffer
			if _, err := packfile.NewEncoder(&pack, tracked, false, packfile.WithObjectSelector(fixedObjects(selected))).Encode(hashes, outgoingDeltaWindow); err != nil {
				t.Fatal(err)
			}
			if got := tracked.readerCount(hashes[2]); got != 1 {
				t.Fatalf("large object reader count after encoding = %d, want 1", got)
			}
			dir := filepath.Join(t.TempDir(), "check.git")
			if output, err := exec.Command("git", "init", "--bare", "--object-format="+format.String(), dir).CombinedOutput(); err != nil {
				t.Fatalf("git init: %s: %v", output, err)
			}
			cmd := exec.Command("git", "--git-dir="+dir, "index-pack", "--strict", "--stdin")
			cmd.Stdin = bytes.NewReader(pack.Bytes())
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("git index-pack: %s: %v", output, err)
			}
			for i, hash := range hashes {
				output, err := exec.Command("git", "--git-dir="+dir, "cat-file", "blob", hash.String()).Output()
				if err != nil || !bytes.Equal(output, contents[i]) {
					t.Fatalf("git decoded object %s incorrectly: %v", hash, err)
				}
			}
		})
	}
}

func TestOutgoingObjectSelectorSharedRetryBudget(t *testing.T) {
	mem := memory.NewStorage()
	hashes := make([]plumbing.Hash, 17)
	for i := range hashes {
		data := make([]byte, outgoingDeltaObjectBytes)
		binary.LittleEndian.PutUint64(data, uint64(i+1))
		hashes[i] = putOutgoingObject(t, mem, plumbing.BlobObject, data)
	}
	failure := errors.New("candidate read failed")
	firstStore := &trackingObjectStore{Storer: mem, readers: map[plumbing.Hash]int{}, readerErrs: map[plumbing.Hash]error{}}
	for _, hash := range hashes[:10] {
		firstStore.readerErrs[hash] = failure
	}
	limits := outgoingTestLimits(t)
	first := outgoingObjectSelectorFactory(limits)(firstStore)
	if _, err := first.ObjectsToPack(hashes[:10], outgoingDeltaWindow); !errors.Is(err, failure) {
		t.Fatalf("first selection error = %v", err)
	}
	attempted := false
	for _, hash := range hashes[:10] {
		attempted = attempted || firstStore.readerCount(hash) > 0
	}
	if !attempted {
		t.Fatal("first selection failed before attempting materialization")
	}
	if got := limits.deltaBytes.Load(); got != 10<<20 {
		t.Fatalf("first selection charged %d bytes, want %d", got, 10<<20)
	}

	retryStore := &trackingObjectStore{Storer: mem, readers: map[plumbing.Hash]int{}}
	retry := outgoingObjectSelectorFactory(limits)(retryStore)
	selected, err := retry.ObjectsToPack(hashes[10:], outgoingDeltaWindow)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != len(hashes[10:]) {
		t.Fatalf("retry selected %d objects, want %d", len(selected), len(hashes[10:]))
	}
	if got := retryStore.readerCount(hashes[16]); got != 0 {
		t.Fatalf("opened object beyond candidate budget %d times", got)
	}
	if got := limits.deltaBytes.Load(); got != outgoingDeltaCandidateBytes {
		t.Fatalf("shared selectors charged %d bytes, want %d", got, outgoingDeltaCandidateBytes)
	}
	last := selected[len(selected)-1]
	if last.Hash() != hashes[16] || last.IsDelta() || last.Object != last.Original {
		t.Fatal("object beyond candidate budget did not remain a lazy full object")
	}
}

func TestOutgoingObjectSelectorTypes(t *testing.T) {
	mem := memory.NewStorage()
	treeA := bytes.Repeat([]byte("a"), 4096)
	treeB := bytes.Clone(treeA)
	treeB[2048] = 'b'
	hashes := []plumbing.Hash{
		putOutgoingObject(t, mem, plumbing.TreeObject, treeA),
		putOutgoingObject(t, mem, plumbing.TreeObject, treeB),
		putOutgoingObject(t, mem, plumbing.CommitObject, []byte("small commit")),
		putOutgoingObject(t, mem, plumbing.TagObject, []byte("small tag")),
	}
	tracked := &trackingObjectStore{Storer: mem, readers: map[plumbing.Hash]int{}}
	selected, err := outgoingObjectSelectorFactory(outgoingTestLimits(t))(tracked).ObjectsToPack(hashes, outgoingDeltaWindow)
	if err != nil {
		t.Fatal(err)
	}
	if tracked.readerCount(hashes[0]) == 0 || tracked.readerCount(hashes[1]) == 0 {
		t.Fatal("tree candidates were not considered for deltas")
	}
	if tracked.readerCount(hashes[2]) != 0 || tracked.readerCount(hashes[3]) != 0 {
		t.Fatal("metadata object payload opened during selection")
	}
	for _, object := range selected {
		if object.Hash() == hashes[2] || object.Hash() == hashes[3] {
			if object.IsDelta() || object.Object != object.Original {
				t.Fatal("metadata object did not remain a lazy full object")
			}
		}
	}
}
