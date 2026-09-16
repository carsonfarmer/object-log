WAL_WIT ?= ../wal-component/wit
ADAPTER = .adapter/target/wasm32-unknown-unknown/release/wasi_snapshot_preview1.wasm
COMPONENTIZE_GO = .componentize/bin/componentize-go

.PHONY: build bindings
build: $(ADAPTER) bindings
	$(COMPONENTIZE_GO) -d wit -w client -w bytecodealliance:pkg/wasip2 build --adapt $(ADAPTER)

bindings: $(COMPONENTIZE_GO)
	mkdir -p wit/deps/wal && cp "$(WAL_WIT)/wal.wit" wit/deps/wal/
	$(COMPONENTIZE_GO) -d wit -w client bindings --format -o bindings --pkg-name object-log-git-proof/bindings

$(COMPONENTIZE_GO): Makefile
	cargo install --locked --force --root .componentize --git https://github.com/carsonfarmer/componentize-go --rev 40407276251b333fd7e0d70725f1c5e0429634b4 componentize-go

$(ADAPTER): build-adapter.sh
	sh build-adapter.sh
