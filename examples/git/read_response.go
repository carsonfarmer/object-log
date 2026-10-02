package main

// Record physical storage failures before library traversal can discard them.
// Normal logical absence and end-of-stream are handled outside this boundary.
func observeRead(failure *error, err error) {
	if *failure == nil && err != nil {
		*failure = err
	}
}
