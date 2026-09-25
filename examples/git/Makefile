WAL_WIT ?= ../wal-component/wit
COMPONENTIZE_GO = .componentize/bin/componentize-go

.PHONY: build bindings
build: bindings
	$(COMPONENTIZE_GO) -d wit -w client -w bytecodealliance:pkg/wasip2 build

bindings: $(COMPONENTIZE_GO)
	go mod download
	mkdir -p wit/deps/wal && cp "$(WAL_WIT)/wal.wit" wit/deps/wal/
	$(COMPONENTIZE_GO) -d wit -w client bindings --format -o bindings --pkg-name object-log-git-proof/bindings

$(COMPONENTIZE_GO): Makefile
	cargo install --locked --force --root .componentize --git https://github.com/bytecodealliance/componentize-go --tag v0.4.3 componentize-go
