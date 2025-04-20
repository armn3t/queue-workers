use async_trait::async_trait;
use queue_workers::{
    concurrent_worker::{ConcurrentWorker, ConcurrentWorkerConfig},
    error::QueueWorkerError,
    job::Job,
    queue::Queue,
    redis_queue::RedisQueue,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info};
use uuid::Uuid;

mod common;
use common::init_test_logging;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct EmailJob {
    id: String,
    to: String,
    subject: String,
    attempts: u32,
    should_fail: bool,
}

#[async_trait]
impl Job for EmailJob {
    type Output = String;
    type Error = String;

    async fn execute(&self) -> Result<Self::Output, Self::Error> {
        // Simulate some work
        debug!("Executing email job {}", self.id);
        sleep(Duration::from_millis(100)).await;

        // Increment attempts (in a real implementation, this would be handled differently)
        let attempts = self.attempts + 1;

        if self.should_fail {
            Err(format!(
                "Failed to send email {} after {} attempts",
                self.id, attempts
            ))
        } else {
            Ok(format!(
                "Email {} sent to {} with subject: {}",
                self.id, self.to, self.subject
            ))
        }
    }

    fn should_retry(&self, _error: &Self::Error, attempt: u32) -> bool {
        attempt < 2 // Retry up to 2 times (3 attempts total)
    }
}

async fn setup_redis_queue(queue_name: &str) -> RedisQueue<EmailJob> {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    RedisQueue::new(&redis_url, queue_name).expect("Failed to create Redis queue")
}

#[tokio::test]
async fn test_complete_workflow() {
    init_test_logging();
    info!("Starting complete workflow test");

    let queue = setup_redis_queue("concurrent_test_complete_workflow").await;
    debug!("Redis queue initialized");

    // Create and push jobs
    let successful_job = EmailJob {
        id: "email-1".to_string(),
        to: "user@example.com".to_string(),
        subject: "Hello".to_string(),
        attempts: 0,
        should_fail: false,
    };
    debug!(job_id = %successful_job.id, "Created successful test job");

    let failing_job = EmailJob {
        id: "email-2".to_string(),
        to: "user@example.com".to_string(),
        subject: "Fail".to_string(),
        attempts: 0,
        should_fail: true,
    };

    queue
        .push(successful_job.clone())
        .await
        .expect("Failed to push successful job");
    queue
        .push(failing_job.clone())
        .await
        .expect("Failed to push failing job");

    // Create worker with configuration
    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 2,
        retry_attempts: 2,
        retry_delay: Duration::from_millis(100),
        shutdown_timeout: Duration::from_secs(1),
    };

    let worker = ConcurrentWorker::new(queue.clone(), config);

    // Run worker for a limited time
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    tokio::spawn(async move {
        sleep(Duration::from_secs(2)).await;
        info!("Sending shutdown signal");
        shutdown_tx.send(()).unwrap();
    });

    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    match queue.pop().await {
        Err(QueueWorkerError::JobNotFound(_)) => (),
        _ => panic!("Queue should be empty"),
    }

    info!("Complete workflow test finished");
}

#[tokio::test]
async fn test_concurrent_workers() {
    init_test_logging();
    info!("Starting concurrent workers test");

    let queue_name = format!("concurrent_test_workers-{}", Uuid::new_v4());
    let queue = setup_redis_queue(&queue_name).await;
    debug!("Redis queue initialized");

    // Create and push multiple jobs
    for i in 0..10 {
        let job = EmailJob {
            id: format!("email-{}", i),
            to: "user@example.com".to_string(),
            subject: format!("Test {}", i),
            attempts: 0,
            should_fail: false,
        };
        debug!(job_id = %job.id, "Pushing job to queue");
        queue.push(job).await.expect("Failed to push job");
    }

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 2,
        retry_attempts: 1,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(1),
    };

    let cloned_queue = queue.clone();
    let handle = tokio::spawn(async move {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let worker = ConcurrentWorker::new(cloned_queue, config);

        tokio::spawn(async move {
            sleep(Duration::from_secs(1)).await;
            debug!("Sending shutdown signal to worker");
            shutdown_tx.send(()).unwrap();
        });

        worker
            .start(async move {
                let _ = shutdown_rx.recv().await;
            })
            .await
            .unwrap();
    });

    handle.await.expect("Worker task failed");

    match queue.pop().await {
        Err(QueueWorkerError::JobNotFound(_)) => (),
        _ => panic!("Queue should be empty after processing"),
    }
}

#[tokio::test]
async fn test_queue_persistence() {
    init_test_logging();
    info!("Starting queue persistence test");

    let queue = setup_redis_queue("concurrent_test_queue_persistence").await;

    let job = EmailJob {
        id: "persistent-email".to_string(),
        to: "user@example.com".to_string(),
        subject: "Persistent Test".to_string(),
        attempts: 0,
        should_fail: false,
    };

    queue.push(job.clone()).await.expect("Failed to push job");

    // Create new queue instance (simulating process restart)
    let new_queue = setup_redis_queue("concurrent_test_queue_persistence").await;

    let retrieved_job = new_queue.pop().await.expect("Failed to pop job");
    assert_eq!(retrieved_job.id, "persistent-email");
    assert_eq!(retrieved_job.subject, "Persistent Test");

    info!("Queue persistence test finished");
}
