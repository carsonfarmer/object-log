.PHONY: check test bench minio-test sqlite-minio-test sqlite-recovery-acceptance staged-performance-acceptance gc-acceptance git-bench git-wasi-check git-performance-acceptance git-minio-test git-http-minio-test

check:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	cargo test --workspace --all-features
	$(MAKE) git-wasi-check

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
	cargo +1.97.1 check --locked -p object-log-git --lib --target wasm32-wasip2 --no-default-features

git-performance-acceptance:
	cargo test -p object-log-git --test performance_acceptance git_request_and_byte_accounting -- --ignored --exact --nocapture

git-minio-test:
	./scripts/test-minio.sh minio minio_git_push_checkpoint_collection_and_cold_recovery object-log-git aws

git-http-minio-test:
	./scripts/test-minio.sh loopback minio_host_pushes_and_cold_clones object-log-git-http ""

.PHONY: git-shared-performance-acceptance
git-shared-performance-acceptance:
	cargo +1.97.1 test --locked --release -p object-log-git --test shared_performance -- --ignored --exact shared_git_performance_acceptance --nocapture
