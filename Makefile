.PHONY: check test bench minio-test sqlite-minio-test sqlite-recovery-acceptance staged-performance-acceptance gc-acceptance

check:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	cargo test --workspace --all-features

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
