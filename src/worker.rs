use crate::{error::QueueWorkerError, job::Job, queue::Queue};
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

type WorkerShutdownSignal = tokio::sync::broadcast::Receiver<()>;

pub struct WorkerConfig {
    pub retry_attempts: u32,
    pub retry_delay: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            retry_attempts: 3,
            retry_delay: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

pub struct Worker<Q: Queue> {
    queue: Q,
    config: WorkerConfig,
    shutdown_signal: WorkerShutdownSignal,
    is_shutting_down: Arc<AtomicBool>,
}

impl<Q> Worker<Q>
where
    Q: Queue,
    <Q::JobType as Job>::Error: Display,
{
    pub fn new(queue: Q, config: WorkerConfig, shutdown_signal: WorkerShutdownSignal) -> Self {
        Self {
            queue,
            config,
            shutdown_signal,
            is_shutting_down: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn start(&self) -> Result<(), QueueWorkerError> {
        let mut shutdown_rx = self.shutdown_signal.resubscribe();
        loop {
            tokio::select! {
                result = shutdown_rx.recv() => {
                    match result {
                        Ok(_r) => {
                            println!("Shutdown signal received, waiting for running jobs to complete...");
                            self.is_shutting_down.store(true, Ordering::Relaxed);
                            let timeout = tokio::time::sleep(self.config.shutdown_timeout);
                            timeout.await;
                            break Ok(());
                        }
                        Err(_e) => {
                            // Log error
                            self.is_shutting_down.store(true, Ordering::Relaxed);
                            break Ok(())
                        }
                    }
                }
                result = self.handle_jobs() => {
                    match result {
                        Ok(()) => {
                            println!("Job completed")
                        }
                        Err(e) => {
                            eprintln!("Job failed: {e}")
                        }
                    }
                }
            }
        }
    }

    async fn handle_jobs(&self) -> Result<(), QueueWorkerError> {
        if self.is_shutting_down.load(Ordering::Relaxed) {
            return Ok(());
        }
        match self.queue.pop().await {
            Ok(job) => {
                let mut attempts: u32 = 0;
                let mut result = job.execute().await;
                while let Err(ref e) = result {
                    if attempts >= self.config.retry_attempts || !job.should_retry(e, attempts) {
                        break;
                    }
                    attempts = attempts.saturating_add(1);
                    tokio::time::sleep(self.config.retry_delay).await;
                    result = job.execute().await;
                }

                if let Err(e) = result {
                    let error_msg = format!(
                        "Job failed after {} attempts: {}",
                        attempts.saturating_add(1),
                        e
                    );
                    return Err(QueueWorkerError::WorkerError(error_msg));
                }
                Ok(())
            }
            Err(QueueWorkerError::JobNotFound(_)) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Clone)]
    struct TestJob {
        attempts: Arc<Mutex<u32>>,
        should_fail: bool,
        retry_conditions: RetryCondition,
        job_duration: Duration,
    }

    impl TestJob {
        fn new() -> Self {
            Self::default()
        }

        fn with_duration(mut self, duration: Duration) -> Self {
            self.job_duration = duration;
            self
        }

        fn with_should_fail(mut self, should_fail: bool) -> Self {
            self.should_fail = should_fail;
            self
        }

        fn with_retry_conditions(mut self, conditions: RetryCondition) -> Self {
            self.retry_conditions = conditions;
            self
        }

        fn with_attempts(mut self, attempts: Arc<Mutex<u32>>) -> Self {
            self.attempts = attempts;
            self
        }
    }

    impl Default for TestJob {
        fn default() -> Self {
            Self {
                attempts: Arc::new(Mutex::new(0)),
                should_fail: false,
                retry_conditions: RetryCondition::Never,
                job_duration: Duration::from_millis(10),
            }
        }
    }

    // Define different retry conditions for better testing
    #[derive(Clone)]
    enum RetryCondition {
        Never,
        Always,
        OnlyOnAttempt(u32),
        UntilAttempt(u32),
    }

    #[async_trait]
    impl Job for TestJob {
        type Output = ();
        type Error = String;

        async fn execute(&self) -> Result<Self::Output, Self::Error> {
            let mut attempts = self.attempts.lock().await;
            *attempts += 1;

            tokio::time::sleep(self.job_duration).await;

            if self.should_fail {
                Err("Job failed".to_string())
            } else {
                Ok(())
            }
        }

        fn should_retry(&self, _error: &Self::Error, attempt: u32) -> bool {
            match self.retry_conditions {
                RetryCondition::Never => false,
                RetryCondition::Always => true,
                RetryCondition::OnlyOnAttempt(n) => attempt == n - 1,
                RetryCondition::UntilAttempt(n) => attempt < n,
            }
        }
    }

    #[derive(Clone)]
    struct TestQueue {
        jobs: Arc<Mutex<Vec<TestJob>>>,
    }

    #[async_trait]
    impl Queue for TestQueue {
        type JobType = TestJob;

        async fn push(&self, job: Self::JobType) -> Result<(), QueueWorkerError> {
            let mut jobs = self.jobs.lock().await;
            jobs.push(job);
            Ok(())
        }

        async fn pop(&self) -> Result<Self::JobType, QueueWorkerError> {
            let mut jobs = self.jobs.lock().await;
            jobs.pop()
                .ok_or_else(|| QueueWorkerError::JobNotFound("Queue empty".to_string()))
        }
    }

    #[tokio::test]
    async fn test_worker_never_retry() {
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

        let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let worker = Worker::new(queue, config, shutdown_rx);

        tokio::select! {
            _ = worker.start() => {},
            _ = tokio::time::sleep(Duration::from_secs(1)) => {},
        }

        let final_attempts = *attempts.lock().await;
        assert_eq!(
            final_attempts, 1,
            "Job should only be attempted once with RetryCondition::Never"
        );
    }

    #[tokio::test]
    async fn test_worker_always_retry() {
        let attempts = Arc::new(Mutex::new(0));
        let job = TestJob::new()
            .with_attempts(attempts.clone())
            .with_should_fail(true)
            .with_retry_conditions(RetryCondition::Always);
        // let job = TestJob {
        //     attempts: attempts.clone(),
        //     should_fail: true,
        //     retry_conditions: RetryCondition::Always,
        // };

        let queue = TestQueue {
            jobs: Arc::new(Mutex::new(vec![job])),
        };

        let config = WorkerConfig {
            retry_attempts: 3,
            retry_delay: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(1),
        };

        let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let worker = Worker::new(queue, config, shutdown_rx);

        tokio::select! {
            _ = worker.start() => {},
            _ = tokio::time::sleep(Duration::from_secs(1)) => {},
        }

        let final_attempts = *attempts.lock().await;
        assert_eq!(
            final_attempts, 4,
            "Job should be attempted 4 times (initial + 3 retries)"
        );
    }

    #[tokio::test]
    async fn test_worker_retry_on_specific_attempt() {
        let attempts = Arc::new(Mutex::new(0));
        let job = TestJob::new()
            .with_attempts(attempts.clone())
            .with_should_fail(true)
            .with_retry_conditions(RetryCondition::OnlyOnAttempt(1));
        // let job = TestJob {
        // attempts: attempts.clone(),
        // should_fail: true,
        // retry_conditions: RetryCondition::OnlyOnAttempt(1), // Only retry on first attempt
        // };

        let queue = TestQueue {
            jobs: Arc::new(Mutex::new(vec![job])),
        };

        let config = WorkerConfig {
            retry_attempts: 3,
            retry_delay: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(1),
        };

        let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let worker = Worker::new(queue, config, shutdown_rx);

        tokio::select! {
            _ = worker.start() => {},
            _ = tokio::time::sleep(Duration::from_secs(1)) => {},
        }

        let final_attempts = *attempts.lock().await;
        assert_eq!(final_attempts, 2, "Job should only be attempted twice");
    }

    #[tokio::test]
    async fn test_worker_retry_until_attempt() {
        let attempts = Arc::new(Mutex::new(0));
        let job = TestJob::new()
            .with_attempts(attempts.clone())
            .with_should_fail(true)
            .with_retry_conditions(RetryCondition::UntilAttempt(2));
        // let job = TestJob {
        // attempts: attempts.clone(),
        // should_fail: true,
        // retry_conditions: RetryCondition::UntilAttempt(2), // Retry until second attempt
        // };

        let queue = TestQueue {
            jobs: Arc::new(Mutex::new(vec![job])),
        };

        let config = WorkerConfig {
            retry_attempts: 5, // Set higher than UntilAttempt value
            retry_delay: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(1),
        };

        let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let worker = Worker::new(queue, config, shutdown_rx);
        tokio::select! {
            _ = worker.start() => {},
            _ = async {
                tokio::time::sleep(Duration::from_secs(1)).await;
            } => {},
        }

        let final_attempts = *attempts.lock().await;
        assert_eq!(
            final_attempts, 3,
            "Job should be attempted 3 times (initial + 2 retries)"
        );
    }

    #[tokio::test]
    async fn test_worker_respects_config_retry_limit() {
        let attempts = Arc::new(Mutex::new(0));
        let job = TestJob::new()
            .with_attempts(attempts.clone())
            .with_should_fail(true)
            .with_retry_conditions(RetryCondition::Always);
        // let job = TestJob {
        // attempts: attempts.clone(),
        // should_fail: true,
        // retry_conditions: RetryCondition::Always,
        // };

        let queue = TestQueue {
            jobs: Arc::new(Mutex::new(vec![job])),
        };

        let config = WorkerConfig {
            retry_attempts: 2, // Set lower than previous tests
            retry_delay: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(1),
        };

        let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let worker = Worker::new(queue, config, shutdown_rx);

        tokio::select! {
            _ = worker.start() => {},
            _ = tokio::time::sleep(Duration::from_secs(1)) => {},
        }

        let final_attempts = *attempts.lock().await;
        assert_eq!(
            final_attempts, 3,
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
            retry_delay: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(1),
        };

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let worker = Worker::new(queue, config, shutdown_rx);

        // Start the worker and immediately signal shutdown
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            shutdown_tx.send(()).unwrap();
        });

        worker.start().await.unwrap();

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

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let worker = Worker::new(queue.clone(), config, shutdown_rx);

        // Start the worker and immediately signal shutdown
        tokio::select! {
            _ = worker.start() => {},
            _ = async {
                shutdown_tx.send(()).unwrap();
                tokio::time::sleep(Duration::from_secs(1)).await;
            } => {},
        }

        let remaining_jobs = queue.jobs.lock().await.len();
        // This test is not deterministic. It leaves 2 or 3 jobs in the queue
        // TODO: rewrite into a more deterministic test if possible
        assert!(
            remaining_jobs > 0,
            "Jobs should remain in queue after immediate shutdown"
        );
    }
}
