package main

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type testIndex struct {
	nodes map[int]radixNode[int, int]
	reads []int
}

func (db *testIndex) load(root int) (radixNode[int, int], error) {
	db.reads = append(db.reads, root)
	n, ok := db.nodes[root]
	if !ok {
		return n, fmt.Errorf("missing node")
	}
	return n, nil
}
func (db *testIndex) save(n radixNode[int, int]) (int, error) {
	id := len(db.nodes) + 1
	db.nodes[id] = n
	return id, nil
}

func TestSamePrefixSplitsAndPreservesOldRoot(t *testing.T) {
	for _, width := range []int{40, 64} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			db := testIndex{nodes: map[int]radixNode[int, int]{}}
			values := map[string]int{}
			key := func(i int) string { return "aa" + fmt.Sprintf("%0*x", width-2, i) }
			for i := 0; i < indexLeafSize; i++ {
				values[key(i)] = i
			}
			original, err := updateRadix("aa", radixNode[int, int]{}, values, db.load, db.save)
			if err != nil {
				t.Fatal(err)
			}
			if len(db.nodes) != 1 {
				t.Fatal("unnecessary branch before leaf limit")
			}
			updates := map[string]int{}
			for i := indexLeafSize; i < 2300; i++ {
				updates[key(i)] = i
			}
			root, err := updateRadix("aa", db.nodes[original], updates, db.load, db.save)
			if err != nil {
				t.Fatal(err)
			}
			for _, node := range db.nodes {
				if len(node.Items) > indexLeafSize || len(node.Children) > 16 {
					t.Fatal("unbounded node")
				}
			}
			for i := 0; i < 2300; i++ {
				got, ok, err := lookupRadix(key(i), root, db.load)
				if err != nil || !ok || got != i {
					t.Fatalf("lookup %d: %d %v %v", i, got, ok, err)
				}
			}
			_, found, err := lookupRadix(key(indexLeafSize), original, db.load)
			if found || err != nil {
				t.Fatal("old root changed")
			}
			db.reads = nil
			_, found, err = lookupRadix(key(2299), root, db.load)
			if !found || err != nil || len(db.reads) > width {
				t.Fatalf("lookup failed or exceeded one path: %v %v %d", found, err, len(db.reads))
			}
			got := map[string]int{}
			if err = walkRadix(root, db.load, func(id string, value int) { got[id] = value }); err != nil {
				t.Fatal(err)
			}
			for id, value := range updates {
				values[id] = value
			}
			if !reflect.DeepEqual(values, got) {
				t.Fatal("walk differs from inserted objects")
			}
		})
	}
}

func TestUpdateDoesNotReadUnchangedSibling(t *testing.T) {
	db := testIndex{nodes: map[int]radixNode[int, int]{}}
	values := map[string]int{}
	for i := 0; i < 600; i++ {
		for _, prefix := range []string{"aa0", "aaf"} {
			values[prefix+fmt.Sprintf("%037x", i)] = i
		}
	}
	root, err := updateRadix("aa", radixNode[int, int]{}, values, db.load, db.save)
	if err != nil {
		t.Fatal(err)
	}
	sibling := db.nodes[root].Children["aaf"]
	db.reads = nil
	updated, err := updateRadix("aa", db.nodes[root], map[string]int{"aa0" + strings.Repeat("f", 37): 999}, db.load, db.save)
	if err != nil {
		t.Fatal(err)
	}
	if db.nodes[updated].Children["aaf"] != sibling {
		t.Fatal("unchanged sibling rewritten")
	}
	for _, read := range db.reads {
		if read == sibling {
			t.Fatal("unchanged sibling read")
		}
	}
	db.reads = nil
	_, found, err := lookupRadix("aae"+strings.Repeat("0", 37), updated, db.load)
	if err != nil || found || len(db.reads) != 1 {
		t.Fatal("absent branch should stop at index node")
	}
}

func TestPruneCatalogKeepsOnlyReachableObjectsAndPreservesOldRoot(t *testing.T) {
	db := testIndex{nodes: map[int]radixNode[int, int]{}}
	items := map[string]int{}
	keep := func(id string) bool { return id[:3] == "aa0" && id != "aa0"+fmt.Sprintf("%037x", 0) }
	for i := 0; i < 700; i++ {
		for _, prefix := range []string{"aa0", "aaf"} {
			items[prefix+fmt.Sprintf("%037x", i)] = i
		}
	}
	root, err := updateRadix("aa", radixNode[int, int]{}, items, db.load, db.save)
	if err != nil {
		t.Fatal(err)
	}
	count := len(db.nodes)
	same, present, err := filterRadix(root, func(string) bool { return true }, db.load, db.save)
	if err != nil || !present || same != root || len(db.nodes) != count {
		t.Fatal("unchanged catalog was rewritten")
	}
	calls := map[string]int{}
	filtered, present, err := filterRadix(root, func(id string) bool {
		calls[id]++
		return keep(id)
	}, db.load, db.save)
	for id := range items {
		if calls[id] != 1 {
			t.Fatal("predicate must run once per object")
		}
	}
	if err != nil || !present {
		t.Fatal(err)
	}
	if len(db.nodes) != count+2 {
		t.Fatal("prune should rewrite only changed leaf and parent")
	}
	result := map[string]int{}
	if err = walkRadix(filtered, db.load, func(id string, item int) { result[id] = item }); err != nil {
		t.Fatal(err)
	}
	expected := map[string]int{}
	for id, item := range items {
		if keep(id) {
			expected[id] = item
		}
	}
	if !reflect.DeepEqual(result, expected) {
		t.Fatal("prune changed live objects or retained dead objects")
	}
	original := map[string]int{}
	_ = walkRadix(root, db.load, func(id string, item int) { original[id] = item })
	if !reflect.DeepEqual(original, items) {
		t.Fatal("original view changed")
	}
	_, present, err = filterRadix(filtered, func(string) bool { return false }, db.load, db.save)
	if err != nil || present {
		t.Fatal("empty catalog retained a root")
	}
}

func BenchmarkPruneCatalog(b *testing.B) {
	for _, mode := range []string{"unchanged", "one-object", "half", "all"} {
		b.Run(mode, func(b *testing.B) {
			db := testIndex{nodes: map[int]radixNode[int, int]{}}
			items := map[string]int{}
			for i := 0; i < 16384; i++ {
				items[fmt.Sprintf("aa%04x%034x", i, i)] = i
			}
			root, err := updateRadix("aa", radixNode[int, int]{}, items, db.load, db.save)
			if err != nil {
				b.Fatal(err)
			}
			count := len(db.nodes)
			load := func(id int) (radixNode[int, int], error) { return db.nodes[id], nil }
			keep := func(id string) bool {
				switch mode {
				case "all":
					return false
				case "one-object":
					return id[2:6] != "0000"
				case "half":
					return id[2] < '2'
				}
				return true
			}
			b.ReportAllocs()
			for b.Loop() {
				next, present, err := filterRadix(root, keep, load, db.save)
				if err != nil || present != (mode != "all") || mode == "unchanged" && next != root {
					b.Fatal("invalid prune result", err)
				}
				for id := len(db.nodes); id > count; id-- {
					delete(db.nodes, id)
				}
			}
		})
	}
}
