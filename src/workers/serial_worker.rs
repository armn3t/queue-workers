use crate::{error::QueueWorkerError, jobs::job::Job, queues::queue::Queue};
use log;
use std::fmt::Display;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

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
    is_shutting_down: AtomicBool,
}

impl<Q> Worker<Q>
where
    Q: Queue,
    <Q::JobType as Job>::Error: Display + Send,
    <Q::JobType as Job>::Output: Send,
{
    pub fn new(queue: Q, config: WorkerConfig) -> Self {
        Self {
            queue,
            config,
            is_shutting_down: AtomicBool::new(false),
        }
    }

    pub async fn start(
        &self,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), QueueWorkerError> {
        let mut shutdown = Box::pin(shutdown);

        log::info!("Starting serial worker...");
        loop {
            let mut job_result = Box::pin(self.process_job());

            tokio::select! {
                result = &mut job_result => {
                    match result {
                      Ok(_) => {
                          log::info!("Job executed successfully");
                      }
                      Err(e) => {
                          log::error!("Job failed to execute: {e}");
                      }
                    };
                    continue;
                }
                _ = &mut shutdown => {
                    log::info!("Shutdown signal received. Waiting for {:?}s for running jobs to complete...", self.config.shutdown_timeout);
                    self.is_shutting_down.store(true, Ordering::Relaxed);
                    tokio::select! {
                        _ = &mut job_result => {
                            log::info!("All jobs completed, shutting down...");
                        }
                        _ = self.handle_shutdown() => {
                            log::info!("Shutdown timeout reached, forcing shutdown...");
                        }
                    }
                    break Ok(())
                }
            }
        }
    }

    async fn handle_shutdown(&self) -> Result<(), QueueWorkerError> {
        tokio::time::sleep(self.config.shutdown_timeout).await;
        Ok(())
    }

    async fn process_job(&self) -> Result<(), QueueWorkerError> {
        if self.is_shutting_down.load(Ordering::Relaxed) {
            log::info!("Worker is shutting down. Can't pick up any new jobs");
            return Ok(());
        }
        log::debug!("Processing new job");
        match self.queue.pop().await {
            Ok(job) => {
                log::debug!("Picked up new job from queue");
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
                // Backpressure for empty queue
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}
