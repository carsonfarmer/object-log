.PHONY: check test bench minio-test minio-performance gc-acceptance git-check git-build git-spin-config-test wasi-credential-test git-provider-test git-qualification-tools-test

check:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	cargo test --workspace --all-features
	cargo clippy --locked -p object-log --lib --target wasm32-wasip2 -- -D warnings
	$(MAKE) git-check

test:
	cargo test --workspace --all-features

bench:
	cargo bench --workspace --all-features

minio-test:
	./scripts/test-minio.sh

minio-performance:
	CARGO_PROFILE_TEST_OPT_LEVEL=3 ./scripts/test-minio.sh performance minio_performance

gc-acceptance:
	cargo test --features test-util --test gc_acceptance memory_gc_removes_100k_objects -- --ignored --nocapture
	./scripts/test-minio.sh gc_acceptance minio_gc_removes_10001_objects

GIT_EXAMPLE_DIR = examples/git
WAL_COMPONENT = examples/wal-component/Cargo.toml
GIT_TEST_FILES = browse.go browse_test.go access.go access_test.go auth.go auth_test.go repositories.go repositories_test.go imports.go imports_test.go receive_transport.go receive_transport_test.go request_config.go request_config_test.go import_pack.go import_pack_test.go import_pack_lifecycle_test.go limits.go limits_test.go index.go index_test.go codec.go codec_test.go delta.go delta_test.go read_retry.go read_retry_test.go retention.go retention_test.go retry.go retry_test.go fetch_policy.go fetch_policy_test.go validate_objects.go validate_objects_test.go validate.go validate_refname.go validate_refname_test.go validate_native_test.go

git-check: git-qualification-tools-test
	test -z "$$(gofmt -l $(GIT_EXAMPLE_DIR)/*.go $(GIT_EXAMPLE_DIR)/tests/*.go)"
	$(MAKE) -C $(GIT_EXAMPLE_DIR) bindings
	cd $(GIT_EXAMPLE_DIR) && go test -race -tags=git_native_test $(GIT_TEST_FILES)
	# Canonical ABI bindings use uintptr conversions; type-check the complete WASI app.
	cd $(GIT_EXAMPLE_DIR) && GOOS=wasip1 GOARCH=wasm go vet -unsafeptr=false .
	cd $(GIT_EXAMPLE_DIR) && go vet -tags=git_native_test $(GIT_TEST_FILES) && go vet ./tests && go test ./tests
	cargo fmt --manifest-path $(WAL_COMPONENT) --check
	cargo test --locked --manifest-path $(WAL_COMPONENT) --lib
	cargo clippy --locked --manifest-path $(WAL_COMPONENT) --all-targets -- -D warnings
	cargo clippy --locked --manifest-path $(WAL_COMPONENT) --target wasm32-wasip2 -- -D warnings

git-qualification-tools-test:
	./examples/git/qualification/aws/test-issue-session.sh
	python3 -m unittest discover -s examples/git/qualification/aws -p 'test_*.py'

git-build:
	cargo build --locked --release --manifest-path $(WAL_COMPONENT) --target wasm32-wasip2
	$(MAKE) -C $(GIT_EXAMPLE_DIR) build
	cd $(GIT_EXAMPLE_DIR) && wac plug --plug ../wal-component/target/wasm32-wasip2/release/object_log_component.wasm main.wasm -o git.wasm

git-spin-config-test: git-build
	./scripts/test-git-spin-config.sh

wasi-credential-test:
	python3 examples/wal-component/tests/test-credentials.py

git-provider-test:
	@test -n "$(GIT_PROBE_URL)" || (echo "Set GIT_PROBE_URL to an isolated Git test service"; exit 1)
	cd $(GIT_EXAMPLE_DIR) && go test ./tests -count=1 -v -timeout 15m

# Native Spin provider is intentionally outside the portable core workspace.
SPIN_KV = integrations/spin-key-value/Cargo.toml
.PHONY: spin-kv-check spin-kv-guest-test
spin-kv-check:
	cargo fmt --manifest-path $(SPIN_KV) --check
	cargo clippy --locked --manifest-path $(SPIN_KV) --all-targets -- -D warnings
	cargo test --locked --manifest-path $(SPIN_KV)
	cargo clippy --locked -p object-log --lib --target wasm32-wasip2 -- -D warnings
	cargo clippy --locked -p object-log-kv --lib --target wasm32-wasip2 -- -D warnings

spin-kv-guest-test:
	./integrations/spin-key-value/test-guest.sh
