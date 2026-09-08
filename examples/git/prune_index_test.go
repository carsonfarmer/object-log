package main

import (
	"fmt"
	"reflect"
	"testing"
)

func TestPruneCatalogKeepsOnlyReachableObjectsAndPreservesOldRoot(t *testing.T) {
	db := testIndex{nodes: map[int]radixNode[int, int]{}}
	items := map[string]int{}
	keep := func(id string) bool { return id[:3] == "aa0" }
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
	filtered, present, err := filterRadix(root, keep, db.load, db.save)
	if err != nil || !present {
		t.Fatal(err)
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
