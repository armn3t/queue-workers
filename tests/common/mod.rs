use async_trait::async_trait;
use queue_workers::{error::QueueWorkerError, job::Job, queue::Queue};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub struct TestJob {
    pub attempts: Arc<Mutex<u32>>,
    pub should_fail: bool,
    pub retry_conditions: RetryCondition,
    pub job_duration: Duration,
    pub completed: Arc<AtomicBool>,
}

impl TestJob {
    pub fn new() -> Self {
        Self {
            attempts: Arc::new(Mutex::new(0)),
            should_fail: false,
            retry_conditions: RetryCondition::Never,
            job_duration: Duration::from_millis(0),
            completed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.job_duration = duration;
        self
    }

    pub fn with_should_fail(mut self, should_fail: bool) -> Self {
        self.should_fail = should_fail;
        self
    }

    pub fn with_retry_conditions(mut self, conditions: RetryCondition) -> Self {
        self.retry_conditions = conditions;
        self
    }

    pub fn with_attempts(mut self, attempts: Arc<Mutex<u32>>) -> Self {
        self.attempts = attempts;
        self
    }

    pub fn with_completion_flag(mut self, completed: Arc<AtomicBool>) -> Self {
        self.completed = completed;
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
            completed: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Clone, Debug)]
pub enum RetryCondition {
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
        println!("Job attempt: {}", *attempts);
        tokio::time::sleep(self.job_duration).await;
        println!(
            "Job duration complete. Job should fail: {}",
            self.should_fail
        );
        if self.should_fail {
            Err("Job failed".to_string())
        } else {
            self.completed.store(true, Ordering::Relaxed);
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
pub struct TestQueue {
    pub jobs: Arc<Mutex<Vec<TestJob>>>,
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
            .ok_or_else(|| QueueWorkerError::JobNotFound("No jobs available".into()))
    }
}
