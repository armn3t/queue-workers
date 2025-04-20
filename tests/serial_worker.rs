// use async_trait::async_trait;
use queue_workers::{
    // error::QueueWorkerError,
    // job::Job,
    // queue::Queue,
    worker::{Worker, WorkerConfig},
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

mod common;
use common::{RetryCondition, TestJob, TestQueue};

#[tokio::test]
async fn test_worker_job_suceeds_without_retries() {
    let attempts = Arc::new(Mutex::new(0));
    let job = TestJob::new().with_attempts(attempts.clone());

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 3,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    // Spawn a task to send shutdown signal after job should be complete
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown_tx.send(()).unwrap();
    });

    // Start the worker and wait for it to complete
    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    let final_attempts = *attempts.lock().await;
    assert_eq!(
        final_attempts, 1,
        "Job should only be attempted once with RetryCondition::Never"
    );
}

#[tokio::test]
async fn test_worker_job_retries_until_it_fails() {
    let attempts = Arc::new(Mutex::new(0));
    let job = TestJob::new()
        .with_attempts(attempts.clone())
        .with_should_fail(true)
        .with_retry_conditions(RetryCondition::Always);

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 3,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    // Spawn a task to send shutdown signal after job should be complete with retries
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown_tx.send(()).unwrap();
    });

    // Start the worker and wait for it to complete
    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    let final_attempts = *attempts.lock().await;
    assert_eq!(
        final_attempts, 4,
        "Job should be attempted 4 times (initial + 3 retries)"
    );
}

#[tokio::test]
async fn test_worker_job_retries_once() {
    let attempts = Arc::new(Mutex::new(0));
    let job = TestJob::new()
        .with_attempts(attempts.clone())
        .with_should_fail(true)
        .with_retry_conditions(RetryCondition::OnlyOnAttempt(1));

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 3,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    // Spawn a task to send shutdown signal after job should be complete
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        shutdown_tx.send(()).unwrap();
    });

    // Start the worker and wait for it to complete
    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    let final_attempts = *attempts.lock().await;
    assert_eq!(final_attempts, 2, "Job should only be attempted twice");
}

#[tokio::test]
async fn test_worker_job_retries_twice() {
    let attempts = Arc::new(Mutex::new(0));
    let job = TestJob::new()
        .with_attempts(attempts.clone())
        .with_should_fail(true)
        .with_retry_conditions(RetryCondition::UntilAttempt(2));

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 5, // Set higher than UntilAttempt value
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    // Spawn a task to send shutdown signal after job should be complete
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        shutdown_tx.send(()).unwrap();
    });

    // Start the worker and wait for it to complete
    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    let final_attempts = *attempts.lock().await;
    assert_eq!(
        final_attempts, 3,
        "Job should be attempted 3 times (initial + 2 retries)"
    );
}

#[tokio::test]
async fn test_worker_job_respects_worker_config_retry_limit() {
    let attempts = Arc::new(Mutex::new(0));
    let job = TestJob::new()
        .with_attempts(attempts.clone())
        .with_should_fail(true)
        .with_retry_conditions(RetryCondition::Always);

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };
    let retry_attempts = 2;
    let config = WorkerConfig {
        retry_attempts,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    // Spawn a task to send shutdown signal after job should be complete
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        shutdown_tx.send(()).unwrap();
    });

    // Start the worker and wait for it to complete
    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    let final_attempts = *attempts.lock().await;
    assert_eq!(
        final_attempts,
        retry_attempts + 1,
        "Job should be attempted 3 times (initial + 2 retries) despite Always retry condition"
    );
}

#[tokio::test]
async fn test_worker_completes_job_during_shutdown() {
    let job = TestJob::new();
    let attempts = job.attempts.clone();

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 1,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown_tx.send(()).unwrap();
    });

    worker
        .start(async move {
            // Both receiving a value and channel closure are valid shutdown signals
            match shutdown_rx.recv().await {
                Ok(_) => {}                                                    // Received shutdown signal
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {}    // Channel closed
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {} // Missed messages, but still continue with shutdown
            }
        })
        .await
        .unwrap();

    let final_attempts = *attempts.lock().await;
    assert_eq!(
        final_attempts, 1,
        "Job should have completed during shutdown grace period"
    );
}

#[tokio::test]
async fn test_worker_leaves_jobs_in_queue_on_shutdown() {
    let jobs = vec![
        TestJob::new().with_duration(Duration::from_secs(1)),
        TestJob::new().with_duration(Duration::from_secs(1)),
        TestJob::new().with_duration(Duration::from_secs(1)),
        TestJob::new().with_duration(Duration::from_secs(1)),
    ];

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(jobs)),
    };

    let config = WorkerConfig {
        retry_attempts: 1,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_millis(100), // Short timeout
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue.clone(), config);

    // Spawn a task to send shutdown signal immediately
    tokio::spawn(async move {
        // Send shutdown signal immediately to test immediate shutdown
        shutdown_tx.send(()).unwrap();
    });

    // Start the worker and wait for it to complete
    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    let remaining_jobs = queue.jobs.lock().await.len();
    assert!(
        remaining_jobs > 0,
        "Jobs should remain in queue after immediate shutdown"
    );
}

#[tokio::test]
async fn test_worker_shutdown_during_job_retry_delay() {
    let attempts = Arc::new(Mutex::new(0));
    let job = TestJob::new()
        .with_attempts(attempts.clone())
        .with_should_fail(true)
        .with_retry_conditions(RetryCondition::Always)
        .with_duration(Duration::from_millis(50));

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 3,
        retry_delay: Duration::from_secs(1), // Long delay
        shutdown_timeout: Duration::from_millis(100),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    // Start the worker and signal shutdown during retry delay
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown_tx.send(()).unwrap();
    });

    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    let final_attempts = *attempts.lock().await;
    assert_eq!(
        final_attempts, 1,
        "Job should not retry when shutdown occurs during retry delay"
    );
}

#[tokio::test]
async fn test_worker_shutdown_with_empty_queue() {
    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![])),
    };

    let config = WorkerConfig {
        retry_attempts: 3,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_millis(500),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown_tx.send(()).unwrap();
    });

    let start = std::time::Instant::now();
    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();
    let shutdown_duration = start.elapsed();

    assert!(
        shutdown_duration < Duration::from_secs(1),
        "Worker should shut down quickly with empty queue"
    );
}

#[tokio::test]
async fn test_worker_shutdown_signal_channel_closed() {
    let attempts = Arc::new(Mutex::new(0));
    let completed = Arc::new(AtomicBool::new(false));
    let job = TestJob::new()
        .with_attempts(attempts.clone())
        .with_duration(Duration::from_secs(1))
        .with_completion_flag(completed.clone());

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 3,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(3),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        drop(shutdown_tx);
    });

    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    let final_attempts = *attempts.lock().await;
    let job_completed = completed.load(Ordering::Relaxed);

    assert_eq!(final_attempts, 1, "Job should be attempted exactly once");
    assert!(job_completed, "Job should have completed successfully");
}

#[tokio::test]
async fn test_worker_graceful_shutdown_cancels_ongoing_job() {
    let job = TestJob::new()
        .with_duration(Duration::from_millis(100))
        .with_should_fail(false);
    let completed = job.completed.clone();

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 1,
        retry_delay: Duration::from_millis(5),
        shutdown_timeout: Duration::from_millis(50),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        shutdown_tx.send(()).unwrap();
    });

    let start = std::time::Instant::now();
    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();
    let shutdown_duration = start.elapsed();

    assert!(
        shutdown_duration >= Duration::from_millis(50),
        "Worker should wait for the full shutdown timeout"
    );
    assert!(
        shutdown_duration < Duration::from_secs(100),
        "Worker should not wait for the entire job duration"
    );
    assert!(
        !completed.load(Ordering::Relaxed),
        "Job should not have completed due to shutdown timeout"
    );
}

#[tokio::test]
async fn test_worker_graceful_shutdown_completes_job() {
    let job = TestJob::new()
        .with_duration(Duration::from_millis(50))
        .with_should_fail(false);
    let completed = job.completed.clone();

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 1,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_millis(100),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        shutdown_tx.send(()).unwrap();
    });

    let start = std::time::Instant::now();
    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();
    let shutdown_duration = start.elapsed();

    assert!(
        completed.load(Ordering::Relaxed),
        "Job should have completed during graceful shutdown"
    );
    assert!(
        shutdown_duration >= Duration::from_millis(50),
        "Worker should have waited for job completion"
    );
    assert!(
        shutdown_duration < Duration::from_millis(100),
        "Worker should have finished before shutdown timeout"
    );
}
