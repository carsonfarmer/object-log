package main

import (
	"encoding/json"
	"fmt"
	"maps"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
	"slices"
)

type bucketMeta struct {
	Items           []objectMeta `json:",omitempty"`
	Prefixes        []string     `json:",omitempty"`
	ChildWALObjects []uint64     `json:",omitempty"`
}

type catalogRoot struct {
	root    *wal.Object
	objects uint64
}

func (s *store) loadBucket(value catalogRoot) (radixNode[indexed, catalogRoot], error) {
	if node, ok := s.loaded[value.root]; ok {
		return node, nil
	}
	node := radixNode[indexed, catalogRoot]{}
	entry, err := s.readNode(value.root)
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
		if len(meta.Items) != 0 || len(meta.Prefixes) != len(entry.Objects) || len(meta.Prefixes) != len(meta.ChildWALObjects) || len(meta.Prefixes) > 16 {
			return node, fmt.Errorf("invalid index branch")
		}
		node.Children = map[string]catalogRoot{}
		for i, key := range meta.Prefixes {
			if _, exists := node.Children[key]; exists {
				return node, fmt.Errorf("duplicate index child")
			}
			node.Children[key] = catalogRoot{root: entry.Objects[i], objects: meta.ChildWALObjects[i]}
		}
	} else {
		if len(meta.ChildWALObjects) != 0 || len(meta.Items) > indexLeafSize {
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
				if next == len(entry.Objects) || item.WALObjects == 0 {
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
	objects, err := nodeObjects(node)
	if err != nil || objects != value.objects {
		return node, fmt.Errorf("invalid index object count")
	}
	s.loaded[value.root] = node
	return node, nil
}

func nodeObjects(node radixNode[indexed, catalogRoot]) (uint64, error) {
	counts := make([]uint64, 0, len(node.Items)+len(node.Children))
	for _, item := range node.Items {
		if item.WALObjects > 0 {
			counts = append(counts, item.WALObjects)
		}
	}
	for _, child := range node.Children {
		counts = append(counts, child.objects)
	}
	return sumObjects(1, counts)
}

func (s *store) saveBucket(node radixNode[indexed, catalogRoot]) (catalogRoot, error) {
	var meta bucketMeta
	var children []*wal.Object
	if node.Children != nil {
		meta.Prefixes = slices.Sorted(maps.Keys(node.Children))
		for _, prefix := range meta.Prefixes {
			child := node.Children[prefix]
			children = append(children, child.root)
			meta.ChildWALObjects = append(meta.ChildWALObjects, child.objects)
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
	objects, err := nodeObjects(node)
	if err != nil {
		return catalogRoot{}, err
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return catalogRoot{}, err
	}
	root, err := s.putNode(data, children)
	if err != nil {
		return catalogRoot{}, err
	}
	return catalogRoot{root: root, objects: objects}, nil
}
