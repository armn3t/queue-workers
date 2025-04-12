use crate::{error::QueueWorkerError, job::Job, queue::Queue};
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::sleep;

pub struct ConcurrentWorkerConfig {
    pub max_concurrent_jobs: usize,
    pub retry_attempts: u32,
    pub retry_delay: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for ConcurrentWorkerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_jobs: num_cpus::get(),
            retry_attempts: 3,
            retry_delay: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

pub struct ConcurrentWorker<Q: Queue> {
    queue: Q,
    config: ConcurrentWorkerConfig,
}

impl<Q> ConcurrentWorker<Q>
where
    Q: Queue + Clone + 'static,
    <Q::JobType as Job>::Error: Display + Send + Sync,
    <Q::JobType as Job>::Output: Send,
{
    pub fn new(queue: Q, config: ConcurrentWorkerConfig) -> Self {
        Self { queue, config }
    }

    async fn process_job(
        queue: Q,
        retry_attempts: u32,
        retry_delay: Duration,
    ) -> Result<(), QueueWorkerError> {
        match queue.pop().await {
            Ok(job) => {
                let mut attempts: u32 = 0;
                let mut result = job.execute().await;

                while let Err(ref e) = result {
                    if attempts >= retry_attempts || !job.should_retry(e, attempts) {
                        break;
                    }
                    attempts = attempts.saturating_add(1);
                    sleep(retry_delay).await;
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
            Err(QueueWorkerError::JobNotFound(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub async fn start(&self) -> Result<(), QueueWorkerError> {
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_jobs));
        let queue = Arc::new(self.queue.clone());
        let retry_attempts = self.config.retry_attempts;
        let retry_delay = self.config.retry_delay;

        loop {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let queue = queue.clone();
            
            tokio::spawn(async move {
                let result = Self::process_job((*queue).clone(), retry_attempts, retry_delay).await;
                if let Err(e) = result {
                    eprintln!("Worker error: {}", e);
                }
                drop(permit);
            });

            sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn start_with_shutdown(
        &self,
        shutdown_signal: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), QueueWorkerError> {
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_jobs));
        let queue = Arc::new(self.queue.clone());
        let retry_attempts = self.config.retry_attempts;
        let retry_delay = self.config.retry_delay;
        let mut shutdown_rx = shutdown_signal;

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    println!("Shutdown signal received, waiting for running jobs to complete...");
                    let timeout = tokio::time::sleep(self.config.shutdown_timeout);
                    tokio::pin!(timeout);

                    loop {
                        if semaphore.available_permits() == self.config.max_concurrent_jobs {
                            break;
                        }

                        tokio::select! {
                            _ = &mut timeout => {
                                println!("Shutdown timeout reached, forcing shutdown...");
                                break;
                            }
                            _ = tokio::time::sleep(Duration::from_millis(100)) => continue,
                        }
                    }
                    return Ok(());
                }
                _ = async {
                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                    let queue = queue.clone();
                    
                    tokio::spawn(async move {
                        let result = Self::process_job((*queue).clone(), retry_attempts, retry_delay).await;
                        if let Err(e) = result {
                            eprintln!("Worker error: {}", e);
                        }
                        drop(permit);
                    });

                    sleep(Duration::from_millis(10)).await;
                } => {}
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
    use tokio::time::sleep;

    #[derive(Clone)]
    struct TestJob {
        attempts: Arc<Mutex<u32>>,
        should_fail: bool,
        retry_on_error: bool,  // New field to control retry behavior
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
            // Only retry if retry_on_error is true and we haven't exceeded 2 attempts
            self.retry_on_error && attempt < 2
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
    async fn test_concurrent_worker() {
        let total_jobs = 10;
        let jobs = Arc::new(Mutex::new(
            (0..total_jobs)
                .map(|i| TestJob {
                    attempts: Arc::new(Mutex::new(0)),
                    should_fail: i % 3 == 0,
                    retry_on_error: i % 2 == 0,  // Only retry even-numbered jobs
                })
                .collect::<Vec<_>>(),
        ));

        let queue = TestQueue {
            jobs: jobs.clone(),
        };

        let config = ConcurrentWorkerConfig {
            max_concurrent_jobs: 3,
            retry_attempts: 2,
            retry_delay: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(1),
        };

        let worker = ConcurrentWorker::new(queue, config);

        tokio::select! {
            _ = worker.start() => {},
            _ = sleep(Duration::from_secs(2)) => {},
        }

        let remaining_jobs = jobs.lock().await.len();
        assert_eq!(remaining_jobs, 0, "All jobs should have been processed");
    }

    #[tokio::test]
    async fn test_concurrent_worker_shutdown() {
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let jobs = Arc::new(Mutex::new(
            (0..100)
                .map(|_| TestJob {
                    attempts: Arc::new(Mutex::new(0)),
                    should_fail: false,
                    retry_on_error: true,
                })
                .collect::<Vec<_>>(),
        ));

        let queue = TestQueue {
            jobs: jobs.clone(),
        };

        let config = ConcurrentWorkerConfig {
            max_concurrent_jobs: 5,
            retry_attempts: 1,
            retry_delay: Duration::from_millis(50),
            shutdown_timeout: Duration::from_secs(1),
        };

        let worker = ConcurrentWorker::new(queue, config);

        tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            shutdown_tx.send(()).unwrap();
        });

        worker.start_with_shutdown(shutdown_rx).await.unwrap();

        let remaining_jobs = jobs.lock().await.len();
        assert!(remaining_jobs < 100, "Some jobs should have been processed");
    }
}
