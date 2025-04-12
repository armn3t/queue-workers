# Queue Workers

A Redis-backed job queue system for Rust applications with support for retries and concurrent workers.

## Features

- Redis-backed persistent job queue
- Automatic job retries with configurable attempts and delay
- Concurrent worker support
- Async/await based API
- Serializable jobs using Serde
- Type-safe job definitions

## Prerequisites

- Rust 1.86.0 or later
- Redis server (local or remote)
- Docker and Docker Compose (for development)

## Quick Start

1. Add the dependency to your `Cargo.toml`:

```toml
[dependencies]
queue_workers = "0.1.0"
```

2. Define your job:

```rust
use serde::{Serialize, Deserialize};
use async_trait::async_trait;
use queue_workers::job::Job;

#[derive(Debug, Serialize, Deserialize)]
struct EmailJob {
    id: String,
    to: String,
    subject: String,
    body: String,
}

#[async_trait]
impl Job for EmailJob {
    type Output = String;
    type Error = String;

    async fn execute(&self) -> Result<Self::Output, Self::Error> {
        // Implement your job logic here
        Ok(format!("Email sent to {}", self.to))
    }
}
```

3. Create a queue and worker:

```rust
use queue_workers::{redis_queue::RedisQueue, worker::{Worker, WorkerConfig}};
use std::time::Duration;

#[tokio::main]
async fn main() {
    // Initialize the queue
    let queue = RedisQueue::<EmailJob>::new(
        "redis://127.0.0.1:6379",
        "email_queue"
    ).expect("Failed to create queue");

    // Configure the worker
    let config = WorkerConfig {
        retry_attempts: 3,
        retry_delay: Duration::from_secs(5),
        shutdown_timeout: Duration::from_secs(30),
    };

    // Create and start the worker
    let worker = Worker::new(queue.clone(), config);
    
    // Push a job
    let job = EmailJob {
        id: "email-1".to_string(),
        to: "user@example.com".to_string(),
        subject: "Hello".to_string(),
        body: "World".to_string(),
    };
    
    queue.push(job).await.expect("Failed to push job");

    // Start processing jobs
    worker.start().await.expect("Worker failed");
}
```

## Development Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/queue_workers.git
cd queue_workers
```

2. Install development dependencies:
```bash
rustup component add rustfmt
rustup component add clippy
```

3. Set up git hooks:
```bash
chmod +x scripts/setup-git-hooks.sh
./scripts/setup-git-hooks.sh
```

4. Start Redis using Docker Compose:
```bash
docker-compose up -d redis
```

5. Run the tests:
```bash
cargo test
```

### Code Quality

This project enforces code quality through:
- Formatting using `rustfmt`
- Linting using `clippy`

To manually run the checks:
```bash
# Check formatting
cargo fmt -- --check

# Run clippy
cargo clippy -- -D warnings
```

These checks run automatically:
- As a pre-commit hook when committing changes
- In CI/CD pipeline for all pull requests

## Configuration

### Worker Configuration

The `WorkerConfig` struct allows you to customize worker behavior:

```rust
let config = WorkerConfig {
    retry_attempts: 3,        // Number of retry attempts for failed jobs
    retry_delay: Duration::from_secs(5),  // Delay between retries
    shutdown_timeout: Duration::from_secs(30),  // Graceful shutdown timeout
};
```

### Redis Configuration

The Redis queue can be configured with a Redis URL and queue name:

```rust
let queue = RedisQueue::<MyJob>::new(
    "redis://username:password@hostname:6379/0",  // Redis URL with authentication
    "my_queue_name"
).expect("Failed to create queue");
```

### Queue Types

The queue supports both FIFO (First In, First Out) and LIFO (Last In, First Out) behaviors:

```rust
use queue_workers::{redis_queue::RedisQueue, queue::QueueType};

// Create a FIFO queue (default behavior)
let fifo_queue = RedisQueue::<MyJob>::new(redis_url, "fifo_queue")?;

// Create a LIFO queue
let lifo_queue = RedisQueue::<MyJob>::with_type(
    redis_url,
    "lifo_queue",
    QueueType::LIFO
)?;
```

- FIFO: Jobs are processed in the order they were added (oldest first)
- LIFO: Jobs are processed in reverse order (newest first)

## Running Multiple Workers

You can run multiple workers processing the same queue:

```rust
let queue = RedisQueue::<EmailJob>::new(redis_url, "email_queue")?;

// Spawn multiple workers
for _ in 0..3 {
    let worker_queue = queue.clone();
    let worker = Worker::new(worker_queue, WorkerConfig::default());
    
    tokio::spawn(async move {
        worker.start().await.expect("Worker failed");
    });
}
```

## Error Handling

The library provides a custom error type `QueueWorkerError` that covers various failure scenarios:

- Redis connection issues
- Serialization errors
- Job not found
- Worker errors
- Connection timeouts

## Worker Types

The library provides two types of workers:

### Sequential Worker

Processes jobs one at a time, with retry support:

```rust
use queue_workers::{
    redis_queue::RedisQueue,
    worker::{Worker, WorkerConfig}
};

let config = WorkerConfig {
    retry_attempts: 3,
    retry_delay: Duration::from_secs(5),
    shutdown_timeout: Duration::from_secs(30),
};

let worker = Worker::new(queue.clone(), config);
worker.start().await?;
```

### Concurrent Worker

Processes multiple jobs in parallel:

```rust
use queue_workers::{
    redis_queue::RedisQueue,
    concurrent_worker::{ConcurrentWorker, ConcurrentWorkerConfig}
};

let config = ConcurrentWorkerConfig {
    max_concurrent_jobs: 5,  // Process 5 jobs simultaneously
    retry_attempts: 3,
    retry_delay: Duration::from_secs(5),
    shutdown_timeout: Duration::from_secs(30),
};

let worker = ConcurrentWorker::new(queue.clone(), config);
worker.start().await?;

// Or with shutdown support:
let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
let worker = ConcurrentWorker::new(queue.clone(), config);
worker.start_with_shutdown(shutdown_rx).await?;
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
