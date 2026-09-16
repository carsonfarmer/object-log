WAL_WIT ?= ../wal-component/wit
ADAPTER = .adapter/target/wasm32-unknown-unknown/release/wasi_snapshot_preview1.wasm
COMPONENTIZE_GO = .componentize/bin/componentize-go

.PHONY: build bindings
build: $(ADAPTER) bindings
	$(COMPONENTIZE_GO) -d wit -w client -w bytecodealliance:pkg/wasip2 build --adapt $(ADAPTER)

bindings: $(COMPONENTIZE_GO)
	go mod download
	mkdir -p wit/deps/wal && cp "$(WAL_WIT)/wal.wit" wit/deps/wal/
	$(COMPONENTIZE_GO) -d wit -w client bindings --format -o bindings --pkg-name object-log-git-proof/bindings

$(COMPONENTIZE_GO): Makefile
	cargo install --locked --force --root .componentize --git https://github.com/bytecodealliance/componentize-go --rev 148dba505f8c6c64ad84db777cfde5e34e25098b \
		--config 'patch."https://github.com/bytecodealliance/wit-bindgen".wit-bindgen-go.git="https://github.com/carsonfarmer/wit-bindgen"' \
		--config 'patch."https://github.com/bytecodealliance/wit-bindgen".wit-bindgen-go.rev="fd8f26d9b019b853770c0446ab27530567e51c39"' componentize-go

$(ADAPTER): build-adapter.sh
	sh build-adapter.sh
