use async_trait::async_trait;
use queue_workers::{
    error::QueueWorkerError,
    job::Job,
    queue::Queue,
    redis_queue::RedisQueue,
    worker::{Worker, WorkerConfig},
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;

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
}

async fn setup_redis_queue(queue_name: &str) -> RedisQueue<EmailJob> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    RedisQueue::new(&redis_url, queue_name).expect("Failed to create Redis queue")
}

#[tokio::test]
async fn test_complete_workflow() {
    let queue = setup_redis_queue("test_complete_workflow").await;

    // Create test jobs
    let successful_job = EmailJob {
        id: "email-1".to_string(),
        to: "user@example.com".to_string(),
        subject: "Test Subject".to_string(),
        attempts: 0,
        should_fail: false,
    };

    let failing_job = EmailJob {
        id: "email-2".to_string(),
        to: "user@example.com".to_string(),
        subject: "Failing Test".to_string(),
        attempts: 0,
        should_fail: true,
    };

    // Push jobs to queue
    queue
        .push(successful_job.clone())
        .await
        .expect("Failed to push successful job");
    queue
        .push(failing_job.clone())
        .await
        .expect("Failed to push failing job");

    // Create worker with custom config
    let config = WorkerConfig {
        retry_attempts: 2,
        retry_delay: Duration::from_millis(100),
        shutdown_timeout: Duration::from_secs(1),
    };
    let worker = Worker::new(queue.clone(), config);

    // Run the worker for a limited time
    tokio::select! {
        _ = worker.start() => {},
        _ = sleep(Duration::from_secs(2)) => {},
    }

    // Try to pop more jobs - should be empty
    match queue.pop().await {
        Err(QueueWorkerError::JobNotFound(_)) => (),
        _ => panic!("Queue should be empty"),
    }
}

#[tokio::test]
async fn test_concurrent_workers() {
    let queue = setup_redis_queue("test_concurrent_workers").await;

    // Push multiple jobs
    for i in 0..5 {
        let job = EmailJob {
            id: format!("email-{}", i),
            to: "user@example.com".to_string(),
            subject: format!("Test Subject {}", i),
            attempts: 0,
            should_fail: false,
        };
        queue.push(job).await.expect("Failed to push job");
    }

    // Create multiple workers
    let worker_count = 3;
    let mut handles = Vec::new();

    for worker_id in 0..worker_count {
        let config = WorkerConfig {
            retry_attempts: 1,
            retry_delay: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(1),
        };

        let cloned_queue = queue.clone();
        let handle = tokio::spawn(async move {
            let worker = Worker::new(cloned_queue, config);

            // Run worker for a limited time
            tokio::select! {
                _ = worker.start() => {},
                _ = sleep(Duration::from_secs(1)) => {
                    println!("Worker {} timed out", worker_id);
                },
            }
        });

        handles.push(handle);
    }

    // Wait for all workers to complete
    for handle in handles {
        handle.await.expect("Worker task failed");
    }

    // Verify queue is empty
    match queue.pop().await {
        Err(QueueWorkerError::JobNotFound(_)) => (),
        _ => panic!("Queue should be empty after processing"),
    }
}

#[tokio::test]
async fn test_queue_persistence() {
    let queue = setup_redis_queue("test_queue_persistence").await;

    // Create and push a job
    let job = EmailJob {
        id: "persistent-email".to_string(),
        to: "user@example.com".to_string(),
        subject: "Persistent Test".to_string(),
        attempts: 0,
        should_fail: false,
    };

    queue.push(job.clone()).await.expect("Failed to push job");

    // Create new queue instance (simulating process restart)
    let new_queue = setup_redis_queue("test_queue_persistence").await;

    // Try to pop job from new queue instance
    let retrieved_job = new_queue.pop().await.expect("Failed to pop job");
    assert_eq!(retrieved_job.id, "persistent-email");
    assert_eq!(retrieved_job.subject, "Persistent Test");
}
