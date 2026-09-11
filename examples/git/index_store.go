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
	if err := s.limits.chargeCatalog(len(entry.Data)); err != nil {
		observeRead(&s.failure, err)
		return node, err
	}
	var meta bucketMeta
	if err = json.Unmarshal(entry.Data, &meta); err != nil {
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
		if len(meta.Items) > indexLeafSize {
			return node, fmt.Errorf("invalid index leaf")
		}
		node.Items = map[string]indexed{}
		next := 0
		for _, item := range meta.Items {
			if _, exists := node.Items[item.ID]; exists {
				return node, fmt.Errorf("duplicate index object")
			}
			value := indexed{objectMeta: item}
			if len(item.Inline) > 0 {
				if !item.validInline() {
					return node, fmt.Errorf("invalid inline object")
				}
			} else {
				if next == len(entry.Objects) {
					return node, fmt.Errorf("missing index object")
				}
				value.root = entry.Objects[next]
				next++
			}
			node.Items[item.ID] = value
		}
		if next != len(entry.Objects) {
			return node, fmt.Errorf("extra index objects")
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
			if len(node.Items[id].Inline) == 0 {
				children = append(children, node.Items[id].root)
			}
		}
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	return s.putNode(data, children)
}
