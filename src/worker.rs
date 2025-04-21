use crate::metrics::{Metrics, NoopMetrics};
use crate::{error::QueueWorkerError, job::Job, queue::Queue};
use log;

use std::fmt::Display;
use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

pub struct WorkerConfig {
    pub retry_attempts: u32,
    pub retry_delay: Duration,
    pub shutdown_timeout: Duration,
    pub metrics: Arc<dyn Metrics>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            retry_attempts: 3,
            retry_delay: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(30),
            metrics: Arc::new(NoopMetrics),
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
        self.config
            .metrics
            .increment_counter("worker_start", 1, &[("worker_type", "serial")]);
        let start_time = Instant::now();
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
                    let uptime_secs = start_time.elapsed().as_secs();
                    self.config.metrics.record_gauge("worker_uptime_seconds", uptime_secs as f64, &[("worker_type", "serial")]);
                    self.config.metrics.increment_counter("worker_stop", 1, &[("worker_type", "serial")]);
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

        let base_labels = &[];

        match self.queue.pop().await {
            Ok(job) => {
                let job_type_label = job.job_type();
                let base_labels = &[("job_type", job_type_label)];

                let queue_pop_time = Instant::now();
                self.config
                    .metrics
                    .increment_counter("job_executing", 1, base_labels);
                log::debug!("Picked up new job from queue");

                // Record job execution start time
                let start_time = Instant::now();
                let queue_wait_ms = start_time.duration_since(queue_pop_time).as_millis() as u64;
                self.config
                    .metrics
                    .record_timing("queue_wait_time", queue_wait_ms, base_labels);

                let mut attempts: u32 = 0;
                let mut result = job.execute().await;

                while let Err(ref e) = result {
                    if attempts >= self.config.retry_attempts || !job.should_retry(e, attempts) {
                        break;
                    }
                    attempts = attempts.saturating_add(1);

                    // Record retry metrics
                    self.config
                        .metrics
                        .increment_counter("job_retry", 1, base_labels);
                    self.config.metrics.record_gauge(
                        "job_retry_count",
                        attempts as f64,
                        base_labels,
                    );

                    tokio::time::sleep(self.config.retry_delay).await;
                    result = job.execute().await;
                }

                // Calculate total execution time
                let execution_time_ms = start_time.elapsed().as_millis() as u64;

                if let Err(e) = result {
                    // Record failure metrics with status label
                    let failure_labels = &[("job_type", job_type_label), ("status", "failed")];
                    self.config
                        .metrics
                        .increment_counter("job_failed", 1, failure_labels);
                    self.config.metrics.record_timing(
                        "job_execution_time",
                        execution_time_ms,
                        failure_labels,
                    );
                    self.config.metrics.record_gauge(
                        "job_retry_attempts",
                        attempts as f64,
                        failure_labels,
                    );

                    let error_msg = format!(
                        "Job failed after {} attempts: {}",
                        attempts.saturating_add(1),
                        e
                    );
                    return Err(QueueWorkerError::WorkerError(error_msg));
                }

                // Record success metrics with status label
                let success_labels = &[("job_type", job_type_label), ("status", "success")];
                self.config
                    .metrics
                    .increment_counter("job_completed", 1, success_labels);
                self.config.metrics.record_timing(
                    "job_execution_time",
                    execution_time_ms,
                    success_labels,
                );
                if attempts > 0 {
                    self.config.metrics.record_gauge(
                        "job_retry_attempts",
                        attempts as f64,
                        success_labels,
                    );
                }

                Ok(())
            }
            Err(QueueWorkerError::JobNotFound(_)) => {
                // Record empty queue metric
                self.config
                    .metrics
                    .increment_counter("queue_empty", 1, base_labels);

                // Backpressure for empty queue
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok(())
            }
            Err(e) => {
                // Record queue error metric
                self.config
                    .metrics
                    .increment_counter("queue_error", 1, base_labels);
                Err(e)
            }
        }
    }
}
