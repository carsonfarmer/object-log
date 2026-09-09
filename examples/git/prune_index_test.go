package main

import (
	"fmt"
	"reflect"
	"testing"
)

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
