use std::time::Duration;
use tokio::time::sleep;
use crate::{
    job::Job,
    queue::Queue,
    error::QueueWorkerError,
};

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

impl<Q: Queue> Worker<Q> {
    pub fn new(queue: Q, config: WorkerConfig) -> Self {
        Self { queue, config }
    }

    pub async fn start(&self) -> Result<(), QueueWorkerError> {
        loop {
            match self.queue.pop().await {
                Ok(job) => {
                    let mut attempts = 0;
                    let mut result = job.execute().await;

                    while result.is_err() && attempts < self.config.retry_attempts {
                        attempts += 1;
                        sleep(self.config.retry_delay).await;
                        result = job.execute().await;
                    }

                    // TODO: do something with that error
                    if let Err(_e) = result {

                        // Handle final failure
                        eprintln!("Job failed after {} attempts", attempts + 1);
                    }
                }
                Err(QueueWorkerError::JobNotFound(_)) => {
                    // Queue is empty, wait before trying again
                    sleep(Duration::from_secs(1)).await;
                }
                Err(e) => {
                    // Handle other errors (connection issues etc.)
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
            jobs.pop().ok_or_else(|| QueueWorkerError::JobNotFound("Queue empty".to_string()))
        }
    }

    #[tokio::test]
    async fn test_worker_retries() {
        let attempts = Arc::new(Mutex::new(0));
        let job = TestJob {
            attempts: attempts.clone(),
            should_fail: true,
        };

        let queue = TestQueue {
            jobs: Arc::new(Mutex::new(vec![job])),
        };

        let config = WorkerConfig {
            retry_attempts: 2,
            retry_delay: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(1),
        };

        let worker = Worker::new(queue, config);

        // Run the worker for a short time
        tokio::select! {
            _ = worker.start() => {},
            _ = sleep(Duration::from_secs(1)) => {},
        }

        let final_attempts = *attempts.lock().await;
        assert_eq!(final_attempts, 3); // Initial attempt + 2 retries
    }
}
