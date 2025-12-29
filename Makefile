.PHONY: build test clean fmt clippy install run check run-dev

# Build the fdf CLI binary (release)
build:
	cargo build --release
	@echo "✓ Binary built: target/release/fdf"

# Build the fdf CLI binary (debug)
build-dev:
	cargo build
	@echo "✓ Binary built: target/debug/fdf"

lint:
	cargo fmt --all -- --check

# Check all crates
check:
	cargo check --workspace

# Run tests
test:
	cargo test --workspace --release

# Clean build artifacts
clean:
	cargo clean

# Format code
fmt:
	cargo fmt --all

# Run clippy
clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

# Install the binary
install: build
	cargo install --path .

# Run example pipeline (Rust)
run:
	@echo "Running example pipeline (Rust)..."
	./target/release/fdf -c example_pipeline.yaml || ./target/debug/fdf -c example_pipeline.yaml

# Run example pipeline (Rust, dev)
run-dev:
	@echo "Running example pipeline (Rust, dev)..."
	./target/debug/fdf -c example_pipeline.yaml
