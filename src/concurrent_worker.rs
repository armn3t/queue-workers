use crate::{error::QueueWorkerError, job::Job, queue::Queue};

use futures::stream::{FuturesUnordered, StreamExt};
use log;
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
// use tokio::sync::Semaphore;
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
    is_shutting_down: Arc<AtomicBool>,
}

impl<Q> ConcurrentWorker<Q>
where
    Q: Queue + Clone + 'static,
    <Q::JobType as Job>::Error: Display + Send + Sync,
    <Q::JobType as Job>::Output: Send,
{
    pub fn new(queue: Q, config: ConcurrentWorkerConfig) -> Self {
        Self {
            queue,
            config,
            is_shutting_down: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn process_job(&self, queue: Arc<Q>) -> Result<(), QueueWorkerError> {
        if self.is_shutting_down.load(Ordering::Relaxed) {
            log::info!("Worker is shutting down. Can't pick up any new jobs");
            return Ok(());
        }
        log::debug!("Processing new job");
        match queue.pop().await {
            Ok(job) => {
                log::debug!("Picked up new job from queue");
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
                Ok(())
            }
            Err(QueueWorkerError::JobNotFound(_)) => {
                // Backpressure for empty queue
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn start(
        &self,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), QueueWorkerError> {
        let mut shutdown = Box::pin(shutdown);
        log::info!("Starting concurrent worker...");
        // let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_jobs));
        let queue = Arc::new(self.queue.clone());

        let mut in_progress_jobs = FuturesUnordered::new();

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    log::info!("Shutdown signal received, waiting for {:?} for running jobs to complete...", self.config.shutdown_timeout);
                    self.is_shutting_down.store(true, Ordering::Relaxed);

                    if in_progress_jobs.is_empty() {
                        return Ok(());
                    }

                    let timeout = tokio::time::sleep(self.config.shutdown_timeout);
                    tokio::pin!(timeout);
                    loop {
                        tokio::select! {
                            _ = &mut timeout => {
                                log::info!("Shutdown timeout reached, forcing shutdown...");
                                return Ok(());
                            }
                            _ = in_progress_jobs.next() => {
                                if in_progress_jobs.is_empty() {
                                    log::info!("All jobs completed, shutting down...");
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
                result = in_progress_jobs.next(), if !in_progress_jobs.is_empty() => {
                    match result {
                        Some(Ok(())) => {
                            log::info!("Job executed successfully");
                        }
                        Some(Err(e)) => {
                            log::error!("Job failed to execute: {}", e);
                        }
                        None => {
                            log::info!("No jobs being executed...");
                        }
                    }
                }
                _ = async {}, if in_progress_jobs.len() < self.config.max_concurrent_jobs => {
                    let queue = queue.clone();
                    let job_future = async move {
                        self.process_job(queue.clone()).await
                    };
                    in_progress_jobs.push(job_future);
                }
            }
        }
    }
}
