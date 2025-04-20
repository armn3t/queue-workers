use async_trait::async_trait;
use queue_workers::{
    error::QueueWorkerError,
    job::Job,
    metrics::NoopMetrics,
    queue::Queue,
    redis_queue::RedisQueue,
    worker::{Worker, WorkerConfig},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::sleep;
use tracing::{debug, info};

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

    let queue = setup_redis_queue("test_complete_workflow").await;
    debug!("Redis queue initialized");

    // Create a notifier to track when jobs complete
    let completion_notifier = Arc::new(Notify::new());

    let successful_job = EmailJob::new(
        "email-1".to_string(),
        "user@example.com".to_string(),
        "Test Subject".to_string(),
        false,
    )
    .with_completion_tracking()
    .with_start_tracking()
    .with_execution_complete_notifier(completion_notifier.clone());
    debug!(job_id = %successful_job.id, "Created successful test job");

    let failing_job = EmailJob::new(
        "email-2".to_string(),
        "user@example.com".to_string(),
        "Failing Test".to_string(),
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

    let config = WorkerConfig {
        retry_attempts: 2,
        retry_delay: Duration::from_millis(100),
        shutdown_timeout: Duration::from_secs(1),

        metrics: Arc::new(NoopMetrics),
    };
    // Create a counter to track completed jobs
    let jobs_processed = Arc::new(AtomicBool::new(false));
    let jobs_processed_clone = jobs_processed.clone();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue.clone(), config);

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

    let queue =
        setup_redis_queue(&format!("test_concurrent_workers-{}", uuid::Uuid::new_v4())).await;
    debug!("Redis queue initialized");

    for i in 0..5 {
        let job = EmailJob::new(
            format!("email-{}", i),
            "user@example.com".to_string(),
            format!("Test Subject {}", i),
            false,
        );
        debug!(job_id = %job.id, "Pushing job to queue");
        queue.push(job).await.expect("Failed to push job");
    }

    let worker_count = 3;
    let mut handles = Vec::new();

    for worker_id in 0..worker_count {
        let config = WorkerConfig {
            retry_attempts: 1,
            retry_delay: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(1),

            metrics: Arc::new(NoopMetrics),
        };

        let cloned_queue = queue.clone();
        let handle = tokio::spawn(async move {
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
            let worker = Worker::new(cloned_queue.clone(), config);

            // Spawn a task to wait a reasonable amount of time and then send shutdown signal
            tokio::spawn(async move {
                // Wait a reasonable amount of time for jobs to complete
                sleep(Duration::from_secs(2)).await;

                debug!("Sending shutdown signal to worker {}", worker_id);
                shutdown_tx.send(()).unwrap();
            });

            worker
                .start(async move {
                    // Both receiving a value and channel closure are valid shutdown signals
                    match shutdown_rx.recv().await {
                        Ok(_) => {}                                                    // Received shutdown signal
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {} // Channel closed
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {} // Missed messages, but still continue with shutdown
                    }
                })
                .await
                .unwrap();
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("Worker task failed");
    }

    // Give some time for the worker to finish
    sleep(Duration::from_millis(500)).await;

    match queue.pop().await {
        Err(QueueWorkerError::JobNotFound(_)) => (),
        _ => panic!("Queue should be empty after processing"),
    }
    info!("Concurrent workers test finished");
}

#[tokio::test]
async fn test_queue_persistence() {
    let queue = setup_redis_queue("test_queue_persistence").await;

    let job = EmailJob::new(
        "persistent-email".to_string(),
        "user@example.com".to_string(),
        "Persistent Test".to_string(),
        false,
    );

    queue.push(job.clone()).await.expect("Failed to push job");

    // Create new queue instance (simulating process restart)
    let new_queue = setup_redis_queue("test_queue_persistence").await;

    let retrieved_job = new_queue.pop().await.expect("Failed to pop job");
    assert_eq!(retrieved_job.id, "persistent-email");
    assert_eq!(retrieved_job.subject, "Persistent Test");
}
