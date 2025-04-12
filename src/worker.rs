use crate::{error::QueueWorkerError, job::Job, queue::Queue};
use std::fmt::Display;
use std::time::Duration;
use tokio::time::sleep;

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
}

impl<Q> Worker<Q>
where
    Q: Queue,
    <Q::JobType as Job>::Error: Display,
{
    pub fn new(queue: Q, config: WorkerConfig) -> Self {
        Self { queue, config }
    }

    pub async fn start(&self) -> Result<(), QueueWorkerError> {
        loop {
            match self.queue.pop().await {
                Ok(job) => {
                    let mut attempts: u32 = 0;
                    let mut result = job.execute().await;

                    while let Err(ref e) = result {
                        if attempts >= self.config.retry_attempts || !job.should_retry(e, attempts) {
                            break;
                        }
                        attempts = attempts.saturating_add(1);
                        sleep(self.config.retry_delay).await;
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
                }
                Err(QueueWorkerError::JobNotFound(_)) => {
                    sleep(Duration::from_secs(1)).await;
                }
                Err(e) => {
                    return Err(e);
                }
            }
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
        let job = TestJob {
            attempts: attempts.clone(),
            should_fail: true,
            retry_conditions: RetryCondition::Never,
        };

        let queue = TestQueue {
            jobs: Arc::new(Mutex::new(vec![job])),
        };

        let config = WorkerConfig {
            retry_attempts: 3,
            retry_delay: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(1),
        };

        let worker = Worker::new(queue, config);

        tokio::select! {
            _ = worker.start() => {},
            _ = sleep(Duration::from_secs(1)) => {},
        }

        let final_attempts = *attempts.lock().await;
        assert_eq!(final_attempts, 1, "Job should only be attempted once with RetryCondition::Never");
    }

    #[tokio::test]
    async fn test_worker_always_retry() {
        let attempts = Arc::new(Mutex::new(0));
        let job = TestJob {
            attempts: attempts.clone(),
            should_fail: true,
            retry_conditions: RetryCondition::Always,
        };

        let queue = TestQueue {
            jobs: Arc::new(Mutex::new(vec![job])),
        };

        let config = WorkerConfig {
            retry_attempts: 3,
            retry_delay: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(1),
        };

        let worker = Worker::new(queue, config);

        tokio::select! {
            _ = worker.start() => {},
            _ = sleep(Duration::from_secs(1)) => {},
        }

        let final_attempts = *attempts.lock().await;
        assert_eq!(final_attempts, 4, "Job should be attempted 4 times (initial + 3 retries)");
    }

    #[tokio::test]
    async fn test_worker_retry_on_specific_attempt() {
        let attempts = Arc::new(Mutex::new(0));
        let job = TestJob {
            attempts: attempts.clone(),
            should_fail: true,
            retry_conditions: RetryCondition::OnlyOnAttempt(1), // Only retry on first attempt
        };

        let queue = TestQueue {
            jobs: Arc::new(Mutex::new(vec![job])),
        };

        let config = WorkerConfig {
            retry_attempts: 3,
            retry_delay: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(1),
        };

        let worker = Worker::new(queue, config);

        tokio::select! {
            _ = worker.start() => {},
            _ = sleep(Duration::from_secs(1)) => {},
        }

        let final_attempts = *attempts.lock().await;
        assert_eq!(final_attempts, 2, "Job should only be attempted twice");
    }

    #[tokio::test]
    async fn test_worker_retry_until_attempt() {
        let attempts = Arc::new(Mutex::new(0));
        let job = TestJob {
            attempts: attempts.clone(),
            should_fail: true,
            retry_conditions: RetryCondition::UntilAttempt(2), // Retry until second attempt
        };

        let queue = TestQueue {
            jobs: Arc::new(Mutex::new(vec![job])),
        };

        let config = WorkerConfig {
            retry_attempts: 5, // Set higher than UntilAttempt value
            retry_delay: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(1),
        };

        let worker = Worker::new(queue, config);

        tokio::select! {
            _ = worker.start() => {},
            _ = sleep(Duration::from_secs(1)) => {},
        }

        let final_attempts = *attempts.lock().await;
        assert_eq!(final_attempts, 3, "Job should be attempted 3 times (initial + 2 retries)");
    }

    #[tokio::test]
    async fn test_worker_respects_config_retry_limit() {
        let attempts = Arc::new(Mutex::new(0));
        let job = TestJob {
            attempts: attempts.clone(),
            should_fail: true,
            retry_conditions: RetryCondition::Always,
        };

        let queue = TestQueue {
            jobs: Arc::new(Mutex::new(vec![job])),
        };

        let config = WorkerConfig {
            retry_attempts: 2, // Set lower than previous tests
            retry_delay: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(1),
        };

        let worker = Worker::new(queue, config);

        tokio::select! {
            _ = worker.start() => {},
            _ = sleep(Duration::from_secs(1)) => {},
        }

        let final_attempts = *attempts.lock().await;
        assert_eq!(final_attempts, 3, "Job should be attempted 3 times (initial + 2 retries) despite Always retry condition");
    }
}
