.PHONY: check test bench minio-test

check:
	cargo fmt --all --check
	cargo clippy --all-targets --all-features -- -D warnings
	cargo test --all-features

test:
	cargo test --all-features

bench:
	cargo bench --all-features

minio-test:
	./scripts/test-minio.sh
