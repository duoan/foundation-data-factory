.PHONY: build test clean fmt clippy install

# Build the project
build:
	cargo build --release

# Run tests
test:
	cargo test --release

# Clean build artifacts
clean:
	cargo clean

# Format code
fmt:
	cargo fmt --all

# Run clippy
clippy:
	cargo clippy --all-targets --all-features -- -D warnings

# Install the binary
install: build
	cargo install --path .
