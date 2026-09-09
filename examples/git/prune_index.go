package main

import "maps"

// Filter immutable catalog nodes, retaining the original proof whenever a
// subtree is unchanged. The caller publishes the replacement root through WAL.
func filterRadix[V any, H comparable](root H, keep func(string) bool, load func(H) (radixNode[V, H], error), save func(radixNode[V, H]) (H, error)) (H, bool, error) {
	var zero H
	node, err := load(root)
	if err != nil {
		return zero, false, err
	}
	changed := false
	next := node
	if node.Children == nil {
		kept := false
		for id, item := range node.Items {
			if keep(id) {
				kept = true
				if changed {
					if next.Items == nil {
						next.Items = make(map[string]V)
					}
					next.Items[id] = item
				}
			} else {
				if !changed {
					next.Items = nil
					if kept {
						next.Items = maps.Clone(node.Items)
					}
					changed = true
				}
				delete(next.Items, id)
			}
		}
		if len(next.Items) == 0 {
			return zero, false, nil
		}
	} else {
		for prefix, child := range node.Children {
			replacement, exists, err := filterRadix(child, keep, load, save)
			if err != nil {
				return zero, false, err
			}
			if !exists || replacement != child {
				if !changed {
					next.Children = maps.Clone(node.Children)
					changed = true
				}
				if exists {
					next.Children[prefix] = replacement
				} else {
					delete(next.Children, prefix)
				}
			}
		}
		if len(next.Children) == 0 {
			return zero, false, nil
		}
	}
	if !changed {
		return root, true, nil
	}
	replacement, err := save(next)
	return replacement, err == nil, err
}
