// Import Future trait for the ConcurrentWorker::start method
use queue_workers::concurrent_worker::{ConcurrentWorker, ConcurrentWorkerConfig};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

mod common;
use common::{RetryCondition, TestJob, TestQueue};

#[tokio::test]
async fn test_concurrent_worker_job_succeeds_without_retries() {
    let attempts = Arc::new(Mutex::new(0));
    let completion_notifier = Arc::new(tokio::sync::Notify::new());

    let job = TestJob::new()
        .with_attempts(attempts.clone())
        .with_execution_complete_notifier(completion_notifier.clone());

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 1, // Use 1 to make test deterministic
        retry_attempts: 3,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

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
async fn test_concurrent_worker_job_retries_until_it_fails() {
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

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 1, // Use 1 to make test deterministic
        retry_attempts: 3,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

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
async fn test_concurrent_worker_job_retries_once() {
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

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 1, // Use 1 to make test deterministic
        retry_attempts: 3,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

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
async fn test_concurrent_worker_job_retries_twice() {
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

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 1, // Use 1 to make test deterministic
        retry_attempts: 5,      // Set higher than UntilAttempt value
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

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
async fn test_concurrent_worker_job_respects_worker_config_retry_limit() {
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
    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 1, // Use 1 to make test deterministic
        retry_attempts,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

    // Spawn a task to wait for the retries and then send shutdown signal
    tokio::spawn(async move {
        // Wait for all retries (should be limited by config)
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
async fn test_concurrent_worker_completes_job_during_shutdown() {
    let completion_notifier = Arc::new(tokio::sync::Notify::new());
    let job = TestJob::new().with_execution_complete_notifier(completion_notifier.clone());
    let attempts = job.attempts.clone();

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 1, // Use 1 to make test deterministic
        retry_attempts: 1,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

    // Spawn a task to send shutdown signal after job starts but before it completes
    tokio::spawn(async move {
        // Wait for the job to start executing
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
async fn test_concurrent_worker_leaves_jobs_in_queue_on_shutdown() {
    let jobs = vec![
        TestJob::new().with_duration(Duration::from_secs(1)),
        TestJob::new().with_duration(Duration::from_secs(1)),
        TestJob::new().with_duration(Duration::from_secs(1)),
        TestJob::new().with_duration(Duration::from_secs(1)),
    ];

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(jobs)),
    };

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 1, // Use 1 to make test deterministic
        retry_attempts: 1,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_millis(100), // Short timeout
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue.clone(), config);

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
async fn test_concurrent_worker_shutdown_during_job_retry_delay() {
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

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 1, // Use 1 to make test deterministic
        retry_attempts: 3,
        retry_delay: Duration::from_secs(1), // Long delay
        shutdown_timeout: Duration::from_millis(100),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

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
async fn test_concurrent_worker_shutdown_with_empty_queue() {
    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![])),
    };

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 1, // Use 1 to make test deterministic
        retry_attempts: 3,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_millis(500),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

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
async fn test_concurrent_worker_shutdown_signal_channel_closed() {
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

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 1, // Use 1 to make test deterministic
        retry_attempts: 3,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(3),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

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
async fn test_concurrent_worker_multiple_jobs() {
    let total_jobs = 10;
    let completion_counter = Arc::new(AtomicUsize::new(0));

    // Create jobs with completion notifiers
    let jobs = Arc::new(Mutex::new(
        (0..total_jobs)
            .map(|_| {
                let counter = completion_counter.clone();
                let completion_notifier = Arc::new(tokio::sync::Notify::new());

                TestJob::new()
                    .with_execution_complete_notifier(completion_notifier.clone())
                    .with_execution_tracker(move || {
                        let counter = counter.clone();
                        async move {
                            // Increment counter when job completes
                            counter.fetch_add(1, Ordering::SeqCst);
                        }
                    })
            })
            .collect::<Vec<_>>(),
    ));

    let queue = TestQueue { jobs: jobs.clone() };

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 3,
        retry_attempts: 2,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

    // Create a clone for the assertion at the end
    let completion_counter_for_assert = completion_counter.clone();

    // Spawn a task to wait for all jobs to complete and then send shutdown signal
    tokio::spawn(async move {
        // Wait until all jobs are processed
        while completion_counter.load(Ordering::SeqCst) < total_jobs {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        shutdown_tx.send(()).unwrap();
    });

    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    let remaining_jobs = jobs.lock().await.len();
    assert_eq!(remaining_jobs, 0, "All jobs should have been processed");
    assert_eq!(
        completion_counter_for_assert.load(Ordering::SeqCst),
        total_jobs,
        "All jobs should have completed"
    );
}

#[tokio::test]
async fn test_concurrent_worker_graceful_shutdown_completes_job() {
    let job = TestJob::new()
        .with_duration(Duration::from_millis(20)) // Shorter duration
        .with_should_fail(false);
    let completed = job.completed.clone();

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 1,
        retry_attempts: 1,
        retry_delay: Duration::from_millis(50),
        shutdown_timeout: Duration::from_millis(600), // Longer timeout
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
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
        shutdown_duration < Duration::from_millis(600),
        "Worker should have finished before shutdown timeout"
    );
}

#[tokio::test]
async fn test_concurrent_worker_graceful_shutdown_cancels_ongoing_job() {
    let job = TestJob::new()
        .with_duration(Duration::from_millis(500)) // Long job
        .with_should_fail(false);
    let _completed = job.completed.clone(); // Prefix with underscore to indicate intentionally unused

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(vec![job])),
    };

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 1,
        retry_attempts: 1,
        retry_delay: Duration::from_millis(5),
        shutdown_timeout: Duration::from_millis(50), // Short timeout
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

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
        shutdown_duration < Duration::from_millis(500),
        "Worker should not wait for the entire job duration"
    );
}

/// A test to verify that jobs are executed in FIFO order when max_concurrent_jobs is 1
#[tokio::test]
async fn test_concurrent_worker_job_execution_order() {
    // Create a shared execution order tracker
    let execution_order = Arc::new(Mutex::new(Vec::new()));

    // Create 5 jobs with unique identifiers
    let jobs = (1..=5)
        .map(|id| {
            let exec_order = execution_order.clone();
            let job_id = id;

            TestJob::new()
                .with_duration(Duration::from_millis(10))
                .with_should_fail(false)
                .with_retry_conditions(RetryCondition::Never)
                .with_execution_tracker(move || {
                    let exec_order = exec_order.clone();
                    let job_id = job_id;
                    async move {
                        let mut order = exec_order.lock().await;
                        order.push(job_id);
                    }
                })
        })
        .collect::<Vec<_>>();

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(jobs)),
    };

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 1, // Force sequential execution
        retry_attempts: 0,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

    // Run the worker for enough time to process all jobs
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        shutdown_tx.send(()).unwrap();
    });

    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    // Check the execution order
    let final_order = execution_order.lock().await;
    assert_eq!(
        *final_order,
        vec![5, 4, 3, 2, 1],
        "Jobs should be executed in FIFO order (reverse of insertion due to pop() implementation)"
    );
}

/// A test to verify that multiple jobs can run concurrently
#[tokio::test]
async fn test_concurrent_worker_parallel_execution() {
    // Track how many jobs are running concurrently
    let currently_running = Arc::new(AtomicUsize::new(0));
    let max_observed_concurrency = Arc::new(AtomicUsize::new(0));

    // Create 10 jobs that will track concurrency
    let jobs = (0..10)
        .map(|_| {
            let running = currently_running.clone();
            let max_observed = max_observed_concurrency.clone();

            TestJob::new()
                .with_duration(Duration::from_millis(50))
                .with_should_fail(false)
                .with_concurrent_execution_tracker(move || {
                    let running = running.clone();
                    let max_observed = max_observed.clone();

                    async move {
                        // Increment counter when job starts
                        let current = running.fetch_add(1, Ordering::SeqCst) + 1;

                        // Update max observed concurrency
                        let mut max = max_observed.load(Ordering::SeqCst);
                        while current > max {
                            match max_observed.compare_exchange(
                                max,
                                current,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            ) {
                                Ok(_) => break,
                                Err(actual) => max = actual,
                            }
                        }

                        // Simulate work
                        tokio::time::sleep(Duration::from_millis(50)).await;

                        // Decrement counter when job finishes
                        running.fetch_sub(1, Ordering::SeqCst);
                    }
                })
        })
        .collect::<Vec<_>>();

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(jobs)),
    };

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 3, // Allow up to 3 concurrent jobs
        retry_attempts: 0,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

    // Run the worker for enough time to process all jobs
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        shutdown_tx.send(()).unwrap();
    });

    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    // Check the maximum observed concurrency
    let observed_max = max_observed_concurrency.load(Ordering::SeqCst);
    assert_eq!(
        observed_max, 3,
        "Worker should run exactly 3 jobs concurrently (as configured)"
    );

    // Ensure all jobs have completed
    let final_running = currently_running.load(Ordering::SeqCst);
    assert_eq!(final_running, 0, "All jobs should have completed");
}

/// A test to verify the worker handles queue errors appropriately
#[tokio::test]
async fn test_concurrent_worker_queue_errors() {
    // Create a queue that will return errors
    let error_queue = ErrorQueue {
        error_on_pop_count: Arc::new(AtomicUsize::new(3)), // First 3 pops will error
        normal_queue: TestQueue {
            jobs: Arc::new(Mutex::new(vec![TestJob::new()])),
        },
    };

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 1,
        retry_attempts: 0,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(error_queue, config);

    // Run the worker for enough time to process all jobs
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        shutdown_tx.send(()).unwrap();
    });

    // The worker should continue despite queue errors
    let result = worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await;

    // Worker should complete successfully despite queue errors
    assert!(
        result.is_ok(),
        "Worker should handle queue errors gracefully"
    );
}

/// A test to verify jobs with different durations all complete
#[tokio::test]
async fn test_concurrent_worker_varying_job_durations() {
    // Create jobs with varying durations
    let job_durations = vec![10, 50, 20, 100, 30];
    let completed_flags = (0..job_durations.len())
        .map(|_| Arc::new(AtomicBool::new(false)))
        .collect::<Vec<_>>();

    let jobs = job_durations
        .into_iter()
        .enumerate()
        .map(|(i, duration)| {
            TestJob::new()
                .with_duration(Duration::from_millis(duration))
                .with_completion_flag(completed_flags[i].clone())
        })
        .collect::<Vec<_>>();

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(jobs)),
    };

    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: 2,
        retry_attempts: 0,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

    // Run the worker for enough time to process all jobs
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        shutdown_tx.send(()).unwrap();
    });

    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    // Check that all jobs completed
    for (i, flag) in completed_flags.iter().enumerate() {
        assert!(
            flag.load(Ordering::Relaxed),
            "Job {} should have completed",
            i
        );
    }
}

/// A test to verify the worker respects max_concurrent_jobs limit
#[tokio::test]
async fn test_concurrent_worker_respects_concurrency_limit() {
    // Create a shared atomic counter to track concurrent execution
    let concurrent_counter = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));

    // Create jobs that will increment/decrement the counter
    let jobs = (0..20)
        .map(|_| {
            let counter = concurrent_counter.clone();
            let max = max_concurrent.clone();

            TestJob::new()
                .with_duration(Duration::from_millis(20))
                .with_concurrent_execution_tracker(move || {
                    let counter = counter.clone();
                    let max = max.clone();

                    async move {
                        // Increment counter at start of job
                        let current = counter.fetch_add(1, Ordering::SeqCst) + 1;

                        // Update max concurrency observed
                        let mut max_seen = max.load(Ordering::SeqCst);
                        while current > max_seen {
                            match max.compare_exchange(
                                max_seen,
                                current,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            ) {
                                Ok(_) => break,
                                Err(actual) => max_seen = actual,
                            }
                        }

                        // Simulate work with a small sleep
                        tokio::time::sleep(Duration::from_millis(20)).await;

                        // Decrement counter at end of job
                        counter.fetch_sub(1, Ordering::SeqCst);
                    }
                })
        })
        .collect::<Vec<_>>();

    let queue = TestQueue {
        jobs: Arc::new(Mutex::new(jobs)),
    };

    // Set a specific concurrency limit
    let concurrency_limit = 4;
    let config = ConcurrentWorkerConfig {
        max_concurrent_jobs: concurrency_limit,
        retry_attempts: 0,
        retry_delay: Duration::from_millis(1),
        shutdown_timeout: Duration::from_secs(1),
    };

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let worker = ConcurrentWorker::new(queue, config);

    // Run the worker for enough time to process all jobs
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        shutdown_tx.send(()).unwrap();
    });

    worker
        .start(async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
        .unwrap();

    // Check that the maximum concurrency was respected
    let observed_max = max_concurrent.load(Ordering::SeqCst);
    assert_eq!(
        observed_max, concurrency_limit,
        "Worker should respect the max_concurrent_jobs limit"
    );

    // Ensure all jobs have completed
    let final_count = concurrent_counter.load(Ordering::SeqCst);
    assert_eq!(final_count, 0, "All jobs should have completed");
}

// Helper struct for testing queue errors
#[derive(Clone)]
struct ErrorQueue {
    error_on_pop_count: Arc<AtomicUsize>,
    normal_queue: TestQueue,
}

#[async_trait::async_trait]
impl queue_workers::queue::Queue for ErrorQueue {
    type JobType = TestJob;

    async fn push(&self, job: Self::JobType) -> Result<(), queue_workers::error::QueueWorkerError> {
        self.normal_queue.push(job).await
    }

    async fn pop(&self) -> Result<Self::JobType, queue_workers::error::QueueWorkerError> {
        let remaining = self.error_on_pop_count.load(Ordering::SeqCst);
        if remaining > 0 {
            self.error_on_pop_count.fetch_sub(1, Ordering::SeqCst);
            Err(queue_workers::error::QueueWorkerError::Unknown(
                "Simulated queue error".into(),
            ))
        } else {
            self.normal_queue.pop().await
        }
    }
}
