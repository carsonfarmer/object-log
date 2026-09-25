package main

import (
	"fmt"
	"maps"
)

// A leaf fits the WAL's default child-reference limit. Internal nodes split
// one hex digit at a time, so they have at most sixteen children.
const indexLeafSize = 1024

type radixNode[V, H any] struct {
	Items    map[string]V
	Children map[string]H
}

func partition[V any](items map[string]V, width int) (map[string]map[string]V, error) {
	groups := map[string]map[string]V{}
	for id, value := range items {
		if len(id) < width {
			return nil, fmt.Errorf("invalid index key")
		}
		prefix := id[:width]
		if groups[prefix] == nil {
			groups[prefix] = map[string]V{}
		}
		groups[prefix][id] = value
	}
	return groups, nil
}

func updateRadix[V, H any](prefix string, node radixNode[V, H], updates map[string]V, load func(string, H) (radixNode[V, H], error), save func(radixNode[V, H]) (H, error)) (H, error) {
	var zero H
	if node.Children == nil {
		items := make(map[string]V, len(node.Items)+len(updates))
		maps.Copy(items, node.Items)
		maps.Copy(items, updates)
		if len(items) <= indexLeafSize {
			return save(radixNode[V, H]{Items: items})
		}
		updates = items
	}
	groups, err := partition(updates, len(prefix)+1)
	if err != nil {
		return zero, err
	}
	children := make(map[string]H, len(node.Children)+len(groups))
	maps.Copy(children, node.Children)
	for key, group := range groups {
		child := radixNode[V, H]{}
		if root, ok := children[key]; ok {
			child, err = load(key, root)
			if err != nil {
				return zero, err
			}
		}
		root, err := updateRadix(key, child, group, load, save)
		if err != nil {
			return zero, err
		}
		children[key] = root
	}
	return save(radixNode[V, H]{Children: children})
}

func lookupRadix[V, H any](id, prefix string, root H, load func(string, H) (radixNode[V, H], error)) (V, bool, error) {
	var zero V
	for {
		node, err := load(prefix, root)
		if err != nil {
			return zero, false, err
		}
		if node.Children == nil {
			item, ok := node.Items[id]
			return item, ok, nil
		}
		if len(id) <= len(prefix) {
			return zero, false, nil
		}
		prefix = id[:len(prefix)+1]
		child, ok := node.Children[prefix]
		if !ok {
			return zero, false, nil
		}
		root = child
	}
}

func walkRadix[V, H any](prefix string, root H, load func(string, H) (radixNode[V, H], error), visit func(string, V)) error {
	node, err := load(prefix, root)
	if err != nil {
		return err
	}
	for id, value := range node.Items {
		visit(id, value)
	}
	for prefix, child := range node.Children {
		if err := walkRadix(prefix, child, load, visit); err != nil {
			return err
		}
	}
	return nil
}

// Filter immutable catalog nodes, retaining the original proof whenever a
// subtree is unchanged. The caller publishes the replacement root through WAL.
func filterRadix[V any, H comparable](prefix string, root H, keep func(string) bool, load func(string, H) (radixNode[V, H], error), save func(radixNode[V, H]) (H, error)) (H, bool, error) {
	var zero H
	node, err := load(prefix, root)
	if err != nil {
		return zero, false, err
	}
	if node.Children == nil {
		var items map[string]V
		changed, kept := false, false
		for id, item := range node.Items {
			if keep(id) {
				kept = true
				if changed {
					if items == nil {
						items = map[string]V{}
					}
					items[id] = item
				}
				continue
			}
			if !changed {
				changed = true
				if kept {
					items = maps.Clone(node.Items)
				}
			}
			delete(items, id)
		}
		if !changed {
			return root, true, nil
		}
		if len(items) == 0 {
			return zero, false, nil
		}
		replacement, err := save(radixNode[V, H]{Items: items})
		return replacement, err == nil, err
	}

	var children map[string]H
	for prefix, child := range node.Children {
		replacement, exists, err := filterRadix(prefix, child, keep, load, save)
		if err != nil {
			return zero, false, err
		}
		if exists && replacement == child {
			continue
		}
		if children == nil {
			children = maps.Clone(node.Children)
		}
		if exists {
			children[prefix] = replacement
		} else {
			delete(children, prefix)
		}
	}
	if children == nil {
		return root, true, nil
	}
	if len(children) == 0 {
		return zero, false, nil
	}
	replacement, err := save(radixNode[V, H]{Children: children})
	return replacement, err == nil, err
}
