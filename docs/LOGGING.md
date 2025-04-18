# Logging Setup Guide

This document provides detailed information about setting up logging for both development and production use of the `queue_workers` library.

## Production Setups

### env_logger Setup

Simple and lightweight logging suitable for basic applications:

```toml
[dependencies]
env_logger = "0.10"
log = "0.4"
```

```rust
use env_logger::{Builder, Env};
use log::LevelFilter;

// Basic setup
fn setup_basic_logging() {
    env_logger::init();
}

// Advanced setup
fn setup_production_logging() {
    Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .format_module_path(true)
        .format_target(false)
        .write_style(env_logger::WriteStyle::Always)
        .filter_module("queue_workers", LevelFilter::Info)
        .filter_module("redis", LevelFilter::Warn)
        .init();
}
```

### tracing Setup

Recommended for production applications with advanced observability needs:

```toml
[dependencies]
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
tracing-appender = "0.2"
```

```rust
use tracing_subscriber::{
    fmt::{self, time::UtcTime},
    EnvFilter,
    Layer,
};
use tracing_appender::rolling::{RollingFileAppender, Rotation};

fn setup_production_tracing() {
    // Set up file appender
    let file_appender = RollingFileAppender::new(
        RollingFileAppender::builder()
            .rotation(Rotation::DAILY)
            .filename_prefix("queue-workers")
            .filename_suffix("log")
            .build_in("logs")
            .expect("Failed to create log directory")
    );

    // JSON formatter for structured logging
    let json_layer = fmt::layer()
        .json()
        .with_timer(UtcTime::rfc_3339())
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .with_writer(file_appender)
        .with_filter(EnvFilter::from_default_env()
            .add_directive("queue_workers=info".parse().unwrap())
            .add_directive("redis=warn".parse().unwrap()));

    // Console formatter for human-readable output
    let console_layer = fmt::layer()
        .pretty()
        .with_timer(UtcTime::rfc_3339())
        .with_thread_names(true)
        .with_target(true)
        .with_filter(EnvFilter::from_default_env()
            .add_directive("queue_workers=debug".parse().unwrap()));

    // Combine layers
    tracing_subscriber::registry()
        .with(json_layer)
        .with(console_layer)
        .init();
}
```

## Development and Testing

### Test Configuration

```rust
// tests/common/mod.rs
use std::sync::Once;
use env_logger::Builder;
use log::LevelFilter;

static INIT: Once = Once::new();

/// Initialize test logging
pub fn init_test_logging() {
    INIT.call_once(|| {
        Builder::new()
            .filter_level(LevelFilter::Debug)
            .is_test(true)
            .init();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_logging() {
        init_test_logging();
        // Your test code here
    }
}
```

### Development Time Logging

For local development with detailed logging:

```rust
// examples/development.rs
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

fn setup_development_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("queue_workers=debug".parse().unwrap())
                .add_directive("redis=debug".parse().unwrap())
        )
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .pretty()
        .init();
}
```

### Integration Test Setup

```rust
// tests/integration/mod.rs
use tracing_subscriber::fmt::format::FmtSpan;
use std::sync::Once;

static INIT: Once = Once::new();

pub fn init_integration_test_logging() {
    INIT.call_once(|| {
        tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::DEBUG)
            .with_thread_names(true)
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .pretty()
            .init();
    });
}

#[tokio::test]
async fn test_worker_integration() {
    init_integration_test_logging();
    // Your integration test code here
}
```

## Environment Variables

### Common Environment Variables

```bash
# Basic logging level
export RUST_LOG=debug

# Detailed configuration
export RUST_LOG="queue_workers=debug,redis=warn,tower=info"

# Production settings
export RUST_LOG="queue_workers=info,redis=warn,tower=warn"

# Test settings
export RUST_LOG="queue_workers=debug,redis=debug,tower=warn"
```

### Development Script

```bash
#!/bin/bash
# scripts/dev.sh

export RUST_LOG="queue_workers=debug,redis=debug"
export RUST_BACKTRACE=1
cargo run --example development
```

## Log Output Examples

### Production JSON Format

```json
{
  "timestamp": "2024-02-20T15:30:45.123Z",
  "level": "INFO",
  "target": "queue_workers::worker",
  "thread_id": 1,
  "thread_name": "worker-1",
  "message": "Job completed successfully",
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "attempts": 1,
  "duration_ms": 234.56
}
```

### Development Format

```
2024-02-20T15:30:45.123Z DEBUG queue_workers::worker{worker_id=1 job_id=123e4567-e89b-12d3-a456-426614174000}
    at src/worker.rs:123
    thread_name = worker-1
    → new job received
    ⋮ processing job
    ← completed successfully
```

## Best Practices

1. **Production Logging**
   - Use JSON formatting for machine-readable logs
   - Include essential context (job IDs, timestamps, thread info)
   - Set appropriate log levels (INFO for normal operation)
   - Implement log rotation

2. **Development Logging**
   - Use pretty formatting for readability
   - Enable debug logging for detailed information
   - Include file and line numbers
   - Show thread information

3. **Test Logging**
   - Initialize logging once per test suite
   - Use test-specific writers
   - Enable debug logging for troubleshooting
   - Include timing information for performance analysis