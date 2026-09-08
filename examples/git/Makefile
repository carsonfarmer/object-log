WAL_WIT ?= ../wal-component/wit
ADAPTER = .adapter/target/wasm32-unknown-unknown/release/wasi_snapshot_preview1.wasm

.PHONY: build bindings
build: $(ADAPTER) bindings
	go tool componentize-go -d wit -w client -w bytecodealliance:pkg/wasip2 build --adapt $(ADAPTER)

bindings:
	mkdir -p wit/deps/wal && cp "$(WAL_WIT)/wal.wit" wit/deps/wal/
	go tool componentize-go -d wit -w client bindings --format -o bindings --pkg-name object-log-git-proof/bindings

$(ADAPTER): adapter.patch build-adapter.sh
	sh build-adapter.sh
