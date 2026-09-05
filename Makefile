.PHONY: check test bench minio-test sqlite-minio-test sqlite-recovery-acceptance staged-performance-acceptance gc-acceptance git-bench git-wasi-check git-performance-acceptance git-minio-test

check:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	cargo test --workspace --all-features
	$(MAKE) git-wasi-check git-spin-wasi-check

test:
	cargo test --workspace --all-features

bench:
	cargo bench --workspace --all-features

minio-test:
	./scripts/test-minio.sh

sqlite-minio-test:
	./scripts/test-minio.sh minio minio_sqlite_recovers_before_and_after_collection object-log-sqlite aws

sqlite-recovery-acceptance:
	cargo test -p object-log-sqlite --all-features --test recovery thousand_wal_transactions_recover_without_the_cache -- --ignored --exact --nocapture

staged-performance-acceptance:
	cargo test -p object-log-sqlite --all-features --test performance_acceptance staged_object_request_accounting -- --ignored --exact --nocapture

gc-acceptance:
	cargo test --features test-util --test gc_acceptance memory_gc_removes_100k_objects -- --ignored --nocapture
	./scripts/test-minio.sh gc_acceptance minio_gc_removes_10001_objects

git-bench:
	cargo bench -p object-log-git --bench git

git-wasi-check:
	cargo +1.97.1 check --locked -p object-log-git --lib --target wasm32-wasip2

git-performance-acceptance:
	cargo test -p object-log-git --test performance_acceptance git_request_and_byte_accounting -- --ignored --exact --nocapture

git-minio-test:
	./scripts/test-minio.sh minio minio_git_push_checkpoint_collection_and_cold_recovery object-log-git aws

.PHONY: git-shared-performance-acceptance
git-shared-performance-acceptance:
	cargo +1.97.1 test --locked --release -p object-log-git --test shared_performance -- --ignored --exact shared_git_performance_acceptance --nocapture

.PHONY: git-spin-memory-acceptance
git-spin-memory-acceptance:
	cargo +1.97.1 build --locked -p object-log-git-spin --example memory_lifecycle --target wasm32-wasip2 --release
	python3 crates/object-log-git-spin/tests/check_memory.py

.PHONY: git-spin-wasi-check git-spin-minio-test
git-spin-wasi-check:
	cargo +1.97.1 clippy --locked -p object-log-git-spin --all-targets --all-features --target wasm32-wasip2 -- -D warnings

git-spin-minio-test:
	cargo +1.97.1 build --locked -p object-log-git-spin --target wasm32-wasip2 --release
	./scripts/test-minio.sh minio spin_minio object-log-git-spin ""
.PHONY: git-spin-performance-acceptance
git-spin-performance-acceptance:
	cargo +1.97.1 build --locked -p object-log-git-spin --example memory_lifecycle --target wasm32-wasip2 --release
	python3 crates/object-log-git-spin/tests/check_performance.py

.PHONY: git-spin-operator-minio-test
git-spin-operator-minio-test:
	cargo +1.97.1 build --locked -p object-log-git-spin --target wasm32-wasip2 --release
	./scripts/test-minio.sh operator_minio operator_minio object-log-git-spin operator

.PHONY: git-spin-shallow-test
git-spin-shallow-test:
	cargo +1.97.1 build --locked -p object-log-git-spin --target wasm32-wasip2 --release
	python3 crates/object-log-git-spin/tests/check_shallow.py
