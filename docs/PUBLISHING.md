# Publishing to crates.io

This document describes the process of publishing new versions of the `queue_workers` library to crates.io.

## Prerequisites

1. API token from crates.io (obtain from https://crates.io/settings/tokens)
2. Cargo login configured:
   ```bash
   cargo login <your-api-token>
   ```

## Publishing Process

### 1. Pre-release Checklist

- [ ] Update version in `Cargo.toml`
- [ ] Update CHANGELOG.md
- [ ] Ensure all tests pass: `cargo test`
- [ ] Verify documentation is up to date: `cargo doc --no-deps`
- [ ] Check for any clippy warnings: `cargo clippy -- -D warnings`
- [ ] Verify the package builds without errors: `cargo build --release`

### 2. Version Update Guidelines

Follow semantic versioning (MAJOR.MINOR.PATCH):
- MAJOR: Breaking changes
- MINOR: New features, no breaking changes
- PATCH: Bug fixes, no breaking changes

Example version update in `Cargo.toml`:
```toml
[package]
name = "queue_workers"
version = "0.2.0"  # Updated from 0.1.0
```

### 3. Dry Run

Before publishing, verify the package contents:

```bash
# Check what would be included in the package
cargo package --list

# Verify the package
cargo package
```

### 4. Publishing

```bash
# Publish to crates.io
cargo publish
```

### 5. Post-publish Verification

1. Check the package page on crates.io: https://crates.io/crates/queue_workers
2. Verify documentation on docs.rs: https://docs.rs/queue_workers
3. Test installation in a new project:
   ```bash
   cargo new test-queue-workers
   cd test-queue-workers
   cargo add queue_workers
   ```

## Git Tags

After successful publication:

```bash
# Create a new tag
git tag -a v0.2.0 -m "Release version 0.2.0"

# Push the tag
git push origin v0.2.0
```

## Yanking a Release

If a published version has critical issues:

```bash
cargo yank --version 0.2.0
```

To undo a yank:

```bash
cargo yank --version 0.2.0 --undo
```
