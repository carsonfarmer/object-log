.PHONY: check test bench minio-test gc-acceptance

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

gc-acceptance:
	cargo test --features test-util --test gc_acceptance memory_gc_removes_100k_objects -- --ignored --nocapture
	./scripts/test-minio.sh gc_acceptance minio_gc_removes_10001_objects
