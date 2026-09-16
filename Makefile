.PHONY: check test bench minio-test gc-acceptance git-check git-build git-spin-config-test git-provider-test git-remote-rehearse git-remote-qualification

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

gc-acceptance:
	cargo test --features test-util --test gc_acceptance memory_gc_removes_100k_objects -- --ignored --nocapture
	./scripts/test-minio.sh gc_acceptance minio_gc_removes_10001_objects

GIT_EXAMPLE_DIR = examples/git
WAL_COMPONENT = examples/wal-component/Cargo.toml
GIT_TEST_FILES = credentials.go credentials_test.go target_id.go target_id_test.go import_pack.go import_pack_test.go import_pack_lifecycle_test.go limits.go limits_test.go index.go index_test.go prune_index.go prune_index_test.go codec.go codec_test.go read_retry.go read_retry_test.go retention.go retention_test.go maintenance_retry.go maintenance_retry_test.go store_retry.go store_retry_test.go fetch_policy.go fetch_policy_test.go validate_objects.go validate_objects_test.go

git-check:
	test -z "$$(gofmt -l $(GIT_EXAMPLE_DIR)/*.go $(GIT_EXAMPLE_DIR)/tests/*.go)"
	$(MAKE) -C $(GIT_EXAMPLE_DIR) bindings
	cd $(GIT_EXAMPLE_DIR) && go test -race $(GIT_TEST_FILES)
	# Canonical ABI bindings use uintptr conversions; type-check the complete WASI app.
	cd $(GIT_EXAMPLE_DIR) && GOOS=wasip1 GOARCH=wasm go vet -unsafeptr=false .
	cd $(GIT_EXAMPLE_DIR) && go vet $(GIT_TEST_FILES) && go vet ./tests && go test ./tests
	cargo fmt --manifest-path $(WAL_COMPONENT) --check
	cargo test --locked --manifest-path $(WAL_COMPONENT) --lib
	cargo clippy --locked --manifest-path $(WAL_COMPONENT) --all-targets -- -D warnings
	cargo clippy --locked --manifest-path $(WAL_COMPONENT) --target wasm32-wasip2 -- -D warnings

git-build:
	cargo build --locked --release --manifest-path $(WAL_COMPONENT) --target wasm32-wasip2
	$(MAKE) -C $(GIT_EXAMPLE_DIR) build
	cd $(GIT_EXAMPLE_DIR) && wac plug --plug ../wal-component/target/wasm32-wasip2/release/wal_component_probe.wasm main.wasm -o git.wasm

git-spin-config-test: git-build
	./scripts/test-git-spin-config.sh

git-provider-test:
	@test -n "$(GIT_PROBE_URL)" || (echo "Set GIT_PROBE_URL to your local Spin/MinIO service"; exit 1)
	cd $(GIT_EXAMPLE_DIR) && go test ./tests -v -timeout 15m

git-remote-rehearse:
	./examples/git/qualification/aws/test-issue-session.sh
	./scripts/test-qualify-git-remote.sh
	./scripts/qualify-git-remote.sh rehearse

REMOTE_QUALIFICATION_PHASE ?= status
git-remote-qualification:
	./scripts/qualify-git-remote.sh $(REMOTE_QUALIFICATION_PHASE)
