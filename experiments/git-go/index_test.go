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
