use crate::{error::QueueWorkerError, job::Job, queue::Queue};
use std::fmt::Display;
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
    is_shutting_down: AtomicBool,
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
            is_shutting_down: AtomicBool::new(false),
        }
    }

    async fn handle_shutdown(&self) -> Result<(), QueueWorkerError> {
        println!("Now: {}", chrono::Utc::now());
        tokio::time::sleep(self.config.shutdown_timeout).await;
        println!("After: {}", chrono::Utc::now());
        Ok(())
    }

    pub async fn start(&self) -> Result<(), QueueWorkerError> {
        let mut shutdown_rx = self.shutdown_signal.resubscribe();

        loop {
            let job_result = self.handle_jobs();
            tokio::pin!(job_result);

            tokio::select! {
                _ = &mut job_result => {
                    continue;
                }
                result = shutdown_rx.recv() => {
                    match result {
                        Ok(_r) => {
                            println!("Shutdown signal received, waiting for running jobs to complete...");
                        }
                        Err(_e) => {
                            println!("Shutdown signal receiver closed");
                        }
                    };
                    self.is_shutting_down.store(true, Ordering::Relaxed);
                    tokio::select! {
                        _ = &mut job_result => {
                            println!("All jobs completed, shutting down...");
                        }
                        _ = self.handle_shutdown() => {
                            println!("Shutdown timeout reached, forcing shutdown...");
                        }
                    }
                    break Ok(())
                }
            }
        }
    }

    async fn handle_jobs(&self) -> Result<(), QueueWorkerError> {
        if self.is_shutting_down.load(Ordering::Relaxed) {
            println!("Shutting down...");
            return Ok(());
        }
        println!("Handling jobs...");
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
