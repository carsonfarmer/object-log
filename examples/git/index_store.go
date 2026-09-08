package main

import (
	"encoding/json"
	"fmt"
	"maps"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
	"slices"
)

type bucketMeta struct {
	Items    []objectMeta `json:",omitempty"`
	Prefixes []string     `json:",omitempty"`
}

func (s *store) loadBucket(root *wal.Object) (radixNode[indexed, *wal.Object], error) {
	if node, ok := s.loaded[root]; ok {
		return node, nil
	}
	node := radixNode[indexed, *wal.Object]{}
	entry, err := s.readNode(root)
	if err != nil {
		return node, err
	}
	var meta bucketMeta
	// Read original experiment leaves without a separate migration.
	if len(entry.Data) > 0 && entry.Data[0] == '[' {
		err = json.Unmarshal(entry.Data, &meta.Items)
	} else {
		err = json.Unmarshal(entry.Data, &meta)
	}
	if err != nil {
		return node, err
	}
	if len(meta.Prefixes) > 0 {
		if len(meta.Items) != 0 || len(meta.Prefixes) != len(entry.Objects) || len(meta.Prefixes) > 16 {
			return node, fmt.Errorf("invalid index branch")
		}
		node.Children = map[string]*wal.Object{}
		for i, key := range meta.Prefixes {
			if _, exists := node.Children[key]; exists {
				return node, fmt.Errorf("duplicate index child")
			}
			node.Children[key] = entry.Objects[i]
		}
	} else {
		if len(meta.Items) != len(entry.Objects) || len(meta.Items) > indexLeafSize {
			return node, fmt.Errorf("invalid index leaf")
		}
		node.Items = map[string]indexed{}
		for i, item := range meta.Items {
			if _, exists := node.Items[item.ID]; exists {
				return node, fmt.Errorf("duplicate index object")
			}
			node.Items[item.ID] = indexed{item, entry.Objects[i]}
		}
	}
	s.loaded[root] = node
	return node, nil
}

func (s *store) saveBucket(node radixNode[indexed, *wal.Object]) (*wal.Object, error) {
	var meta bucketMeta
	var children []*wal.Object
	if node.Children != nil {
		meta.Prefixes = slices.Sorted(maps.Keys(node.Children))
		for _, prefix := range meta.Prefixes {
			children = append(children, node.Children[prefix])
		}
	} else {
		keys := slices.AppendSeq(make([]string, 0, len(node.Items)), maps.Keys(node.Items))
		slices.Sort(keys)
		for _, id := range keys {
			meta.Items = append(meta.Items, node.Items[id].objectMeta)
			children = append(children, node.Items[id].root)
		}
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	return s.putNode(data, children)
}
