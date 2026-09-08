package main

import "fmt"

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

func updateRadix[V, H any](prefix string, node radixNode[V, H], updates map[string]V, load func(H) (radixNode[V, H], error), save func(radixNode[V, H]) (H, error)) (H, error) {
	var zero H
	if node.Children == nil {
		items := make(map[string]V, len(node.Items)+len(updates))
		for id, value := range node.Items {
			items[id] = value
		}
		for id, value := range updates {
			items[id] = value
		}
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
	for key, root := range node.Children {
		children[key] = root
	}
	for key, group := range groups {
		child := radixNode[V, H]{}
		if root, ok := children[key]; ok {
			child, err = load(root)
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

func lookupRadix[V, H any](id string, root H, load func(H) (radixNode[V, H], error)) (V, bool, error) {
	var zero V
	for {
		node, err := load(root)
		if err != nil {
			return zero, false, err
		}
		if node.Children == nil {
			item, ok := node.Items[id]
			return item, ok, nil
		}
		found := false
		for prefix, child := range node.Children {
			if len(id) >= len(prefix) && id[:len(prefix)] == prefix {
				root, found = child, true
				break
			}
		}
		if !found {
			return zero, false, nil
		}
	}
}

func walkRadix[V, H any](root H, load func(H) (radixNode[V, H], error), visit func(string, V)) error {
	node, err := load(root)
	if err != nil {
		return err
	}
	for id, value := range node.Items {
		visit(id, value)
	}
	for _, child := range node.Children {
		if err := walkRadix(child, load, visit); err != nil {
			return err
		}
	}
	return nil
}
