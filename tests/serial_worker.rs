// use async_trait::async_trait;
use queue_workers::{
    // error::QueueWorkerError,
    // job::Job,
    // queue::Queue,
    metrics::NoopMetrics,
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
    let completion_notifier = Arc::new(tokio::sync::Notify::new());

    let job = TestJob::new()
        .with_attempts(attempts.clone())
        .with_execution_complete_notifier(completion_notifier.clone());

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 3,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(1),
        metrics: Arc::new(NoopMetrics),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    // Spawn a task to wait for job completion and then send shutdown signal
    tokio::spawn(async move {
        // Wait for the job to complete
        completion_notifier.notified().await;
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
    let retry_notifier = Arc::new(tokio::sync::Notify::new());

    let job = TestJob::new()
        .with_attempts(attempts.clone())
        .with_should_fail(true)
        .with_retry_conditions(RetryCondition::Always)
        .with_before_retry_notifier(retry_notifier.clone());

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 3,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),
        metrics: Arc::new(NoopMetrics),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    // Spawn a task to wait for all retries and then send shutdown signal
    tokio::spawn(async move {
        // Wait for 3 retries (4 total attempts)
        for _ in 0..3 {
            retry_notifier.notified().await;
        }
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
    let retry_notifier = Arc::new(tokio::sync::Notify::new());

    let job = TestJob::new()
        .with_attempts(attempts.clone())
        .with_should_fail(true)
        .with_retry_conditions(RetryCondition::OnlyOnAttempt(1))
        .with_before_retry_notifier(retry_notifier.clone());

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 3,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),
        metrics: Arc::new(NoopMetrics),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    // Spawn a task to wait for the retry and then send shutdown signal
    tokio::spawn(async move {
        // Wait for 1 retry
        retry_notifier.notified().await;
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
    let retry_notifier = Arc::new(tokio::sync::Notify::new());

    let job = TestJob::new()
        .with_attempts(attempts.clone())
        .with_should_fail(true)
        .with_retry_conditions(RetryCondition::UntilAttempt(2))
        .with_before_retry_notifier(retry_notifier.clone());

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 5, // Set higher than UntilAttempt value
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(1),
        metrics: Arc::new(NoopMetrics),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    // Spawn a task to wait for the retries and then send shutdown signal
    tokio::spawn(async move {
        // Wait for 2 retries
        for _ in 0..2 {
            retry_notifier.notified().await;
        }
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
    let retry_notifier = Arc::new(tokio::sync::Notify::new());

    let job = TestJob::new()
        .with_attempts(attempts.clone())
        .with_should_fail(true)
        .with_retry_conditions(RetryCondition::Always)
        .with_before_retry_notifier(retry_notifier.clone());

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };
    let retry_attempts = 2;
    let config = WorkerConfig {
        retry_attempts,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),

        metrics: Arc::new(NoopMetrics),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    // Spawn a task to wait for all retries and then send shutdown signal
    tokio::spawn(async move {
        // Wait for the configured number of retries
        for _ in 0..retry_attempts {
            retry_notifier.notified().await;
        }
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
    let completion_notifier = Arc::new(tokio::sync::Notify::new());
    let job = TestJob::new().with_execution_complete_notifier(completion_notifier.clone());
    let attempts = job.attempts.clone();

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 1,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),

        metrics: Arc::new(NoopMetrics),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    // Spawn a task to send shutdown signal after job starts but before it completes
    tokio::spawn(async move {
        // Wait a short time to ensure the job has started
        tokio::time::sleep(Duration::from_millis(10)).await;
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

    // Wait for the completion notification to ensure the job actually completed
    let completed =
        tokio::time::timeout(Duration::from_millis(100), completion_notifier.notified())
            .await
            .is_ok();
    assert!(
        completed,
        "Job should have completed during shutdown grace period"
    );

    let final_attempts = *attempts.lock().await;
    assert_eq!(final_attempts, 1, "Job should have been attempted once");
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

        metrics: Arc::new(NoopMetrics),
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
    let before_retry_notifier = Arc::new(tokio::sync::Notify::new());

    let job = TestJob::new()
        .with_attempts(attempts.clone())
        .with_should_fail(true)
        .with_retry_conditions(RetryCondition::Always)
        .with_duration(Duration::from_millis(50))
        .with_before_retry_notifier(before_retry_notifier.clone());

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 3,
        retry_delay: Duration::from_secs(1), // Long delay
        shutdown_timeout: Duration::from_millis(100),

        metrics: Arc::new(NoopMetrics),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    // Spawn a task to wait for the job to fail and then send shutdown signal during retry delay
    tokio::spawn(async move {
        // Wait for the job to fail and enter retry logic
        before_retry_notifier.notified().await;
        // Send shutdown signal during retry delay
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

        metrics: Arc::new(NoopMetrics),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    // Use a channel to signal when the worker has started
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        // Wait for worker to start
        let _ = started_rx.await;
        // Then send shutdown signal immediately
        shutdown_tx.send(()).unwrap();
    });

    // Start the worker and signal that it's started
    let start = std::time::Instant::now();
    let worker_future = worker.start(async move {
        let _ = shutdown_rx.recv().await;
    });

    // Signal that the worker has started
    let _ = started_tx.send(());

    // Wait for worker to complete
    worker_future.await.unwrap();
    let shutdown_duration = start.elapsed();

    // Since we're shutting down an empty queue, it should be very fast
    assert!(
        shutdown_duration < Duration::from_secs(1),
        "Worker should shut down quickly with empty queue"
    );
}

#[tokio::test]
async fn test_worker_shutdown_signal_channel_closed() {
    let attempts = Arc::new(Mutex::new(0));
    let completed = Arc::new(AtomicBool::new(false));
    let execution_complete_notifier = Arc::new(tokio::sync::Notify::new());

    let job = TestJob::new()
        .with_attempts(attempts.clone())
        .with_duration(Duration::from_millis(100)) // Shorter duration for faster test
        .with_completion_flag(completed.clone())
        .with_execution_complete_notifier(execution_complete_notifier.clone());

    let jobs = Arc::new(Mutex::new(vec![job]));
    let started_execution = jobs.lock().await.get(0).unwrap().started_execution.clone();

    let queue = TestQueue { jobs: jobs.clone() };

    let config = WorkerConfig {
        retry_attempts: 3,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(3),

        metrics: Arc::new(NoopMetrics),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    // Spawn a task to wait for job to start and then drop the shutdown channel
    tokio::spawn(async move {
        // Wait for the job to start executing
        while !started_execution.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        drop(shutdown_tx);
    });

    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    // Wait for the completion notification to ensure the job actually completed
    let completed_in_time = tokio::time::timeout(
        Duration::from_millis(200),
        execution_complete_notifier.notified(),
    )
    .await
    .is_ok();
    assert!(completed_in_time, "Job should have completed in time");

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

    // Get a clone of the job's started_execution flag
    let started_execution = job.started_execution.clone();

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 1,
        retry_delay: Duration::from_millis(5),
        shutdown_timeout: Duration::from_millis(50), // Short timeout to ensure job is cancelled

        metrics: Arc::new(NoopMetrics),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    tokio::spawn(async move {
        // Wait for the job to start executing
        while !started_execution.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Then send shutdown signal
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

    // We should wait for the full shutdown timeout since the job is still running
    assert!(
        shutdown_duration >= Duration::from_millis(50),
        "Worker should wait for the full shutdown timeout"
    );

    // But we shouldn't wait for the entire job duration
    assert!(
        shutdown_duration < Duration::from_millis(100),
        "Worker should not wait for the entire job duration"
    );

    // The job should not have completed due to the short shutdown timeout
    assert!(
        !completed.load(Ordering::Relaxed),
        "Job should not have completed due to shutdown timeout"
    );
}

#[tokio::test]
async fn test_worker_graceful_shutdown_completes_job() {
    let completion_notifier = Arc::new(tokio::sync::Notify::new());
    let job = TestJob::new()
        .with_duration(Duration::from_millis(50))
        .with_should_fail(false)
        .with_execution_complete_notifier(completion_notifier.clone());
    let completed = job.completed.clone();

    // Get a clone of the job's started_execution flag
    let started_execution = job.started_execution.clone();

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = WorkerConfig {
        retry_attempts: 1,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_millis(100),

        metrics: Arc::new(NoopMetrics),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = Worker::new(queue, config);

    tokio::spawn(async move {
        // Wait for the job to start executing
        while !started_execution.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Then send shutdown signal
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

    // Wait for the completion notification to ensure the job actually completed
    let completed_in_time =
        tokio::time::timeout(Duration::from_millis(100), completion_notifier.notified())
            .await
            .is_ok();
    assert!(completed_in_time, "Job should have completed in time");

    assert!(
        completed.load(Ordering::Relaxed),
        "Job should have completed during graceful shutdown"
    );

    // The shutdown duration should be at least as long as the job duration
    assert!(
        shutdown_duration >= Duration::from_millis(50),
        "Worker should have waited for job completion"
    );

    // But it should be less than the shutdown timeout
    assert!(
        shutdown_duration < Duration::from_millis(100),
        "Worker should have finished before shutdown timeout"
    );
}
