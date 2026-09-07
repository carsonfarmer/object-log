package main

// Filter immutable catalog nodes, retaining the original proof whenever a
// subtree is unchanged. The caller publishes the replacement root through WAL.
func filterRadix[V any, H comparable](root H, keep func(string) bool, load func(H) (radixNode[V, H], error), save func(radixNode[V, H]) (H, error)) (H, bool, error) {
	var zero H
	node, err := load(root)
	if err != nil {
		return zero, false, err
	}
	changed := false
	next := radixNode[V, H]{}
	if node.Children == nil {
		next.Items = map[string]V{}
		for id, item := range node.Items {
			if keep(id) {
				next.Items[id] = item
			} else {
				changed = true
			}
		}
		if len(next.Items) == 0 {
			return zero, false, nil
		}
	} else {
		next.Children = map[string]H{}
		for prefix, child := range node.Children {
			replacement, exists, err := filterRadix(child, keep, load, save)
			if err != nil {
				return zero, false, err
			}
			if exists {
				next.Children[prefix] = replacement
				changed = changed || replacement != child
			} else {
				changed = true
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
