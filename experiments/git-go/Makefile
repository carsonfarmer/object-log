WAL_WIT ?= ../wal-component/wit

.PHONY: build
build:
	mkdir -p wit/deps/wal && cp "$(WAL_WIT)/wal.wit" wit/deps/wal/
	go tool componentize-go -d wit -w client bindings --format -o bindings --pkg-name object-log-git-proof/bindings
	go tool componentize-go -d wit -w client -w bytecodealliance:pkg/wasip2 build
