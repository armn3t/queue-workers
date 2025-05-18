use async_trait::async_trait;
use queue_workers::{
    error::QueueWorkerError,
    jobs::job::Job,
    queues::{queue::Queue, redis_queue::RedisQueue},
    workers::concurrent_worker::{ConcurrentWorker, ConcurrentWorkerConfig},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
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
    #[serde(skip)]
    completed: Option<Arc<AtomicBool>>,
    #[serde(skip)]
    started: Option<Arc<AtomicBool>>,
    #[serde(skip)]
    execution_complete_notifier: Option<Arc<Notify>>,
}

impl EmailJob {
    fn new(id: String, to: String, subject: String, should_fail: bool) -> Self {
        Self {
            id,
            to,
            subject,
            attempts: 0,
            should_fail,
            completed: None,
            started: None,
            execution_complete_notifier: None,
        }
    }

    fn with_completion_tracking(mut self) -> Self {
        self.completed = Some(Arc::new(AtomicBool::new(false)));
        self
    }

    fn with_start_tracking(mut self) -> Self {
        self.started = Some(Arc::new(AtomicBool::new(false)));
        self
    }

    fn with_execution_complete_notifier(mut self, notifier: Arc<Notify>) -> Self {
        self.execution_complete_notifier = Some(notifier);
        self
    }
}

#[async_trait]
impl Job for EmailJob {
    type Output = String;
    type Error = String;

    async fn execute(&self) -> Result<Self::Output, Self::Error> {
        // Signal that execution has started
        if let Some(started) = &self.started {
            started.store(true, Ordering::SeqCst);
        }

        // Simulate some work
        debug!("Executing email job {}", self.id);
        sleep(Duration::from_millis(100)).await;

        // Increment attempts (in a real implementation, this would be handled differently)
        let attempts = self.attempts + 1;

        let result = if self.should_fail {
            Err(format!(
                "Failed to send email {} after {} attempts",
                self.id, attempts
            ))
        } else {
            // Mark as completed if successful
            if let Some(completed) = &self.completed {
                completed.store(true, Ordering::SeqCst);
            }

            Ok(format!(
                "Email {} sent to {} with subject: {}",
                self.id, self.to, self.subject
            ))
        };

        // Notify that execution is complete
        if let Some(notifier) = &self.execution_complete_notifier {
            notifier.notify_one();
        }

        result
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
    // Create a notifier to track when jobs complete
    let completion_notifier = Arc::new(Notify::new());

    let successful_job = EmailJob::new(
        "email-1".to_string(),
        "user@example.com".to_string(),
        "Hello".to_string(),
        false,
    )
    .with_completion_tracking()
    .with_start_tracking()
    .with_execution_complete_notifier(completion_notifier.clone());
    debug!(job_id = %successful_job.id, "Created successful test job");

    let failing_job = EmailJob::new(
        "email-2".to_string(),
        "user@example.com".to_string(),
        "Fail".to_string(),
        true,
    )
    .with_start_tracking()
    .with_execution_complete_notifier(completion_notifier.clone());

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

    // Create a counter to track completed jobs
    let jobs_processed = Arc::new(AtomicBool::new(false));
    let jobs_processed_clone = jobs_processed.clone();

    // Run worker with a shutdown signal based on job completion
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    // Use a simple timeout approach instead of notifications
    tokio::spawn(async move {
        // Wait a reasonable amount of time for jobs to complete
        sleep(Duration::from_secs(3)).await;

        // Mark jobs as processed
        jobs_processed_clone.store(true, Ordering::SeqCst);

        info!("Sending shutdown signal");
        shutdown_tx.send(()).unwrap();
    });

    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    // Give some time for the worker to finish
    sleep(Duration::from_millis(500)).await;

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
        let job = EmailJob::new(
            format!("email-{}", i),
            "user@example.com".to_string(),
            format!("Test {}", i),
            false,
        );
        debug!(job_id = %job.id, "Pushing job to queue");
        queue.push(job).await.expect("Failed to push job");
    }

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 2,
        retry_attempts: 1,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(1),
    };

    // Create a clone of the queue for checking later
    let queue_check = queue.clone();

    // Use a simpler approach with a fixed timeout
    let handle = tokio::spawn(async move {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let worker = ConcurrentWorker::new(queue.clone(), config);

        // Start a task to send shutdown signal after a fixed time
        tokio::spawn(async move {
            sleep(Duration::from_secs(3)).await;
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

    // Give some time for the worker to finish
    sleep(Duration::from_millis(500)).await;

    match queue_check.pop().await {
        Err(QueueWorkerError::JobNotFound(_)) => (),
        _ => panic!("Queue should be empty after processing"),
    }
}

#[tokio::test]
async fn test_queue_persistence() {
    init_test_logging();
    info!("Starting queue persistence test");

    let queue = setup_redis_queue("concurrent_test_queue_persistence").await;

    let job = EmailJob::new(
        "persistent-email".to_string(),
        "user@example.com".to_string(),
        "Persistent Test".to_string(),
        false,
    );

    queue.push(job.clone()).await.expect("Failed to push job");

    // Create new queue instance (simulating process restart)
    let new_queue = setup_redis_queue("concurrent_test_queue_persistence").await;

    let retrieved_job = new_queue.pop().await.expect("Failed to pop job");
    assert_eq!(retrieved_job.id, "persistent-email");
    assert_eq!(retrieved_job.subject, "Persistent Test");

    info!("Queue persistence test finished");
}
